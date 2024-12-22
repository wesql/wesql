// Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited

#include "compact/compaction_job.h"
#include <algorithm>
#include "compact/range_iterator.h"
#include "compact/reuse_block_merge_iterator.h"
#include "db/db_iter.h"
#include "db/snapshot_impl.h"
#include "logger/log_module.h"
#include "monitoring/thread_status_util.h"
#include "options/options.h"
#include "storage/extent_meta_manager.h"
#include "storage/multi_version_extent_meta_layer.h"

namespace smartengine {
using namespace common;
using namespace db;
using namespace util;
using namespace monitor;
using namespace util;
using namespace memory;

namespace storage {
CompactionJob::CompactionJob(ArenaAllocator &arena)
    : arena_(arena),
      meta_snapshot_(nullptr),
      priority_layer_compacted_(false),
      task_to_run_(0),
      current_task_type_(TaskType::INVALID_TYPE_TASK),
      inited_(false),
      need_delete_compaction_(false) {}

CompactionJob::~CompactionJob() { destroy(); }

int CompactionJob::init(const CompactionContext &context,
                        const ColumnFamilyDesc &cf,
                        const Snapshot* meta_snapshot) {
  int ret = Status::kOk;
  if (!context.valid()) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "invalid context", K(ret));
  } else if (nullptr == meta_snapshot) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(ERROR, "invalid arguments", K(ret), KP(meta_snapshot));
  } else {
    context_ = context;
    // We set change_info_.task_type_ here
    //change_info_.task_type_ = context_.task_type_;
    cf_desc_ = cf;
    meta_snapshot_ = meta_snapshot;
    inited_ = true;
  }
  return ret;
}

void CompactionJob::destroy() {
  for (Compaction *compaction : compaction_tasks_) {
    destroy_compaction(compaction);
  }
  compaction_stats_.record_stats_.reset();
  compaction_stats_.perf_stats_.reset();
  inited_ = false;
  extents_list_.clear();
  merge_batch_indexes_.clear();
  compaction_tasks_.clear();
  current_task_type_ = TaskType::INVALID_TYPE_TASK;
}

void CompactionJob::destroy_compaction(Compaction *compaction) {
  if (nullptr != compaction) {
    PLACEMENT_DELETE(Compaction, arena_, compaction);
    --task_to_run_;
  }
  ThreadStatusUtil::ResetThreadStatus();
}

void CompactionJob::destroy_range_iterators(RangeIterator **iters,
                                            const int64_t num) {
  if (nullptr != iters) {
    for (int64_t i = 0; i < num; ++i) {
      if (nullptr != iters[i]) {
        MetaDataIterator *miter = dynamic_cast<MetaDataIterator *>(iters[i]);
        // no need to delete, l1 DBIter is allocated from arena;
        // meta data iterator allocated by arena, no need free memory.
        iters[i]->~RangeIterator();
      }
    }
  }
}

// Use Stream or (FPGA)MinorCompaction
int CompactionJob::prepare_minor_task(const int64_t merge_limit) {
  if (!inited_) return Status::kIncomplete;
  COMPACTION_LOG(INFO, "begin to prepare minor task", KP(this));
  int ret = 0;

  if (TaskType::INTRA_COMPACTION_TASK == context_.task_type_) {
    context_.output_level_ = storage::Compaction::L0;
  } else if (TaskType::MINOR_COMPACTION_TASK == context_.task_type_ ||
      TaskType::STREAM_COMPACTION_TASK == context_.task_type_ ||
      TaskType::MINOR_DELETE_COMPACTION_TASK == context_.task_type_) {
    context_.output_level_ = storage::Compaction::L1;
  } else {
    COMPACTION_LOG(ERROR, "prepare minor compaction wrong task_type ", K((int)context_.task_type_));
    return Status::kCorruption;
  }

  // We only need handle the oldest MAX_CHILD_NUM ways.
  int64_t all_way_size = ReuseBlockMergeIterator::MAX_CHILD_NUM;
  int64_t way_size = 0;
  CompactWay *compact_way = static_cast<CompactWay *>(arena_.alloc(sizeof(CompactWay) * all_way_size));
  for (int64_t i = 0; i < all_way_size; ++i) {
    new (compact_way + i) CompactWay();
  }
  storage::Range wide_range;
  if (FAILED(get_all_l0_range(arena_, all_way_size, compact_way, way_size,
          wide_range))) {
    COMPACTION_LOG(WARN, "scan level 0' extents for compaction failed.", K(ret));
  } else if (way_size == 0) {
    COMPACTION_LOG(INFO, "has no level 0's data need compaction");
  } else {
    SE_LOG(INFO, "minor compaction builder info",
                K(cf_desc_.column_family_id_),
                K(meta_snapshot_->get_extent_layer_version(0)->get_extent_layer_size()),
                K(context_.output_level_),
                K(wide_range));
    storage::RangeIterator *iterators[all_way_size];
    const uint64_t start_micros = context_.cf_options_->env->NowMicros();

    if (TaskType::INTRA_COMPACTION_TASK == context_.task_type_) {
      // We set force_layer_sequence the smalleset (sequence_number - 1) here.
      context_.force_layer_sequence_ = compact_way[0].sequence_number_ - 1;
      if (FAILED(build_compaction_iterators(arena_, compact_way,
              way_size /* total way size, not used*/,
              std::min(way_size, all_way_size) /* max way size */,
              iterators,
              all_way_size /* returned way size */))) {
        COMPACTION_LOG(WARN, "build compaction iterators level0 failed", K(ret));
      }
      assert(all_way_size <= way_size);
    } else {
      if (FAILED(build_plus_l1_compaction_iterators(arena_, compact_way, way_size,
              wide_range, iterators,all_way_size))) {
        // plus level 1 's iterator;
        COMPACTION_LOG(WARN, "build compaction iterators level0 and level1 failed", K(ret));
      }
    }

    if (Status::kOk == ret &&
        FAILED(build_multiple_compaction(arena_, iterators, all_way_size, merge_limit))) {
      COMPACTION_LOG(WARN, "build multiple compaction tasks failed", K(ret));
    }
    destroy_range_iterators(iterators, all_way_size);
  }

  if (Status::kOk == ret && task_to_run_ > 0) {
    current_task_type_ = context_.task_type_;
    //change_info_.task_type_ = context_.task_type_;
  }
  COMPACTION_LOG(INFO, "success to prepare minor task", KP(this), K((int)current_task_type_), K(task_to_run_));

  if (nullptr != compact_way) {
    for (int64_t i = 0; i < all_way_size; ++i) {
      compact_way[i].~CompactWay();
    }
  }

  return ret;
}

int CompactionJob::alloc_compaction_task(
    const size_t delete_percent,
    GeneralCompaction *&compaction,
    MetaDescriptorList &extents) {
  int ret = 0;
  if (nullptr == compaction) {
    compaction = ALLOC_OBJECT(GeneralCompaction, arena_, context_, cf_desc_, arena_);
  }
  if (nullptr == compaction) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "allocate memory failed", K(ret));
  } else if (FAILED(compaction->add_merge_batch(extents, 0, extents.size()))) {
    COMPACTION_LOG(WARN, "failed to add merge batch", K(ret), K(extents.size()));
  } else {
    compaction->set_delete_percent(delete_percent);
    add_compaction_task(compaction);
    compaction = nullptr;
  }
  return ret;
}

int CompactionJob::build_delete_major_self_compaction(
    const int64_t extents_limit,
    storage::RangeIterator *iterator2) {
  int ret = 0;
  const Comparator *cmp = context_.data_comparator_;
  if (nullptr == iterator2) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "iterator2 or storage_manager is null", K(ret));
    return ret;
  } else if (nullptr == cmp) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "cmp is null", K(ret));
    return ret;
  }
  MetaDescriptorList extents;
  Slice last_userkey;
  GeneralCompaction *compaction = nullptr;
  int64_t total_extents_count = 0;
  iterator2->seek_to_first();
  while (Status::kOk == ret && iterator2->valid()) {
    MetaDescriptor md = iterator2->get_meta_descriptor();
    ExtentId extent_id = ExtentId(md.block_position_.first);
    const ExtentMeta *meta = ExtentMetaManager::get_instance().get_meta(extent_id);
    int cmp_val = 1;
    if (last_userkey.size() > 0) {
      cmp_val = cmp->Compare(last_userkey, md.get_start_user_key());
    }
    if (nullptr == meta) {
      ret = Status::kAborted;
      SE_LOG(WARN, "get extent meta failed", K(ret), K(md));
    } else if (meta->num_deletes_ > 0 || 0 == cmp_val) {
      MetaDescriptor md_copy = md.deep_copy(arena_);
      extents.push_back(md_copy);
      last_userkey = md_copy.get_end_user_key();
    } else if (0 == extents.size()) {
      // do nothing
    } else if (FAILED(alloc_compaction_task(0, compaction, extents))) {
      COMPACTION_LOG(WARN, "alloc compaction task failed", K(ret));
    } else {
      total_extents_count += extents.size();
      extents.clear();
      last_userkey.clear();
    }
    if (Status::kOk == ret) {
      iterator2->next();
    }
  }
  if (Status::kOk == ret && extents.size() > 0) {
    if (FAILED(alloc_compaction_task(0, compaction, extents))) {
      COMPACTION_LOG(WARN, "alloc compaction task failed", K(ret));
    } else {
      total_extents_count += extents.size();
    }
  }
  COMPACTION_LOG(INFO, "build delete major self task", K(total_extents_count));
  return ret;
}


int CompactionJob::pick_extents(
    const int64_t level,
    storage::RangeIterator *iterator,
    MetaDescriptorList &extents) {
  int ret = 0;
  const Comparator *data_cmp = context_.data_comparator_;
  if (IS_NULL(iterator) || IS_NULL(meta_snapshot_)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "iterator2 or meta_snapshot is null", K(ret), KP(iterator));
    return ret;
  }
  ExtentLayerVersion *layer_version = meta_snapshot_->get_extent_layer_version(level);
  if (IS_NULL(data_cmp) || IS_NULL(layer_version)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "cmp is null", K(ret), KP(data_cmp));
    return ret;
  }
  int64_t level_extents = layer_version->get_total_normal_extent_count();
  level_extents = level_extents * LEVEL_EXTENTS_PERCENT / 100 + 1;
  int64_t extents_cnt_limit = level_extents > EXTENTS_CNT_LIMIT
      ? EXTENTS_CNT_LIMIT
      : level_extents;
  int64_t total_usage = 0;
  int64_t cur_extents_cnt = 0;
  int64_t min_usage_percent = 200;
  int64_t min_usage_pos = 0;
  int64_t min_usage_end_pos = 0;
  int64_t pick_extents_cnt = 0;
  autovector<int64_t> extents_usage;
  Slice last_userkey;
  iterator->seek_to_first();
  while(Status::kOk == ret && iterator->valid()) {
    MetaDescriptor md = iterator->get_meta_descriptor();
    if (context_.shutting_down_->load(std::memory_order_acquire)) {
      SE_LOG(ERROR, "process shutting down, break compaction.", K(ret));
      ret = Status::kShutdownInProgress;
      break;
    }
    const ExtentMeta *meta = ExtentMetaManager::get_instance().get_meta(ExtentId(md.block_position_.first));
    if (nullptr == meta) {
      ret = Status::kAborted;
      SE_LOG(WARN, "get extent meta failed", K(ret), K(md));
      break;
    }
    int cmp_val = 1;
    if (last_userkey.size() > 0) {
      cmp_val = data_cmp->Compare(last_userkey, md.get_end_user_key());
    }
    if (0 != cmp_val && pick_extents_cnt >= extents_cnt_limit) {
      int64_t usage_percent = total_usage * 100 / (pick_extents_cnt * EXTENT_SIZE);
      // update min_usage_percent and min_usage_pos
      if (min_usage_percent > usage_percent) {
        min_usage_percent = usage_percent;
        min_usage_pos = cur_extents_cnt - pick_extents_cnt + 1;
        min_usage_end_pos = cur_extents_cnt;
      }
      // update the sliding window
      total_usage -= extents_usage.at(cur_extents_cnt - pick_extents_cnt);
      --pick_extents_cnt;
    }
    if (0 == cmp_val || pick_extents_cnt < extents_cnt_limit) {
      extents_usage.push_back(meta->data_size_ + meta->index_block_handle_.get_size());
      total_usage += meta->data_size_ + meta->index_block_handle_.get_size();
      last_userkey = md.get_end_user_key().deep_copy(arena_);
      ++cur_extents_cnt;
      ++pick_extents_cnt;
      iterator->next();
    }
  }
  if (Status::kOk == ret) {
    // deal with the last block
    if (pick_extents_cnt >= extents_cnt_limit) {
      int64_t usage_percent = total_usage * 100 / (pick_extents_cnt * EXTENT_SIZE);
      // update min_usage_percent and min_usage_pos
      if (min_usage_percent > usage_percent) {
        min_usage_percent = usage_percent;
        min_usage_pos = cur_extents_cnt - pick_extents_cnt + 1;
        min_usage_end_pos = cur_extents_cnt;
      }
    }
    cur_extents_cnt = 1;
    iterator->seek_to_first();
    while (iterator->valid()) {
      if (context_.shutting_down_->load(std::memory_order_acquire)) {
        SE_LOG(ERROR, "process shutting down, break compaction.", K(ret));
        ret = Status::kShutdownInProgress;
        break;
      }
      if (cur_extents_cnt >= min_usage_pos) {
        extents.push_back(iterator->get_meta_descriptor().deep_copy(arena_));
      }
      if (cur_extents_cnt >= min_usage_end_pos) {
        break;
      }
      iterator->next();
      ++cur_extents_cnt;
    }
  }
  SE_LOG(INFO, "AUTO_MAJOR_SELF: pick extents for auto major self task",
      K(extents.size()), K(min_usage_percent),
      K(min_usage_pos), K(min_usage_end_pos), K(ret));
  return ret;
}

int CompactionJob::build_auto_self_compaction(
    const int64_t level,
    storage::RangeIterator *iterator) {
  int ret = 0;
  MetaDescriptorList extents;
  if (FAILED(pick_extents(level, iterator, extents))) {
    COMPACTION_LOG(WARN, "failed to pick extents", K(ret));
  } else if (extents.size() > 0) {
    GeneralCompaction *compaction = ALLOC_OBJECT(GeneralCompaction, arena_, context_, cf_desc_, arena_);
    if (nullptr == compaction) {
      ret = Status::kMemoryLimit;
      COMPACTION_LOG(WARN, "failed to alloc memory for compaction", K(ret));
    } else if (FAILED(compaction->add_merge_batch(extents, 0, extents.size()))) {
      COMPACTION_LOG(WARN, "failed to add merge batch", K(ret), K(extents.size()));
    } else {
      compaction->set_delete_percent(0); // not do reuse
      add_compaction_task(compaction);
    }
  }
  return ret;
}

int CompactionJob::prepare_minor_self_task() {
  int ret = 0;
  size_t delete_percent = 0; // not do reuse
  MetaDataSingleIterator *l1_range_iter = nullptr;
  context_.output_level_ = Compaction::L1;
  if (FAILED(build_self_iterator(1, l1_range_iter, delete_percent))) {
    COMPACTION_LOG(WARN,"build l1 compaction iterator failed", K(ret));
  } else if (FAILED(build_auto_self_compaction(1, l1_range_iter))) {
    COMPACTION_LOG(WARN, "failed to build auto self compaction", K(ret), KP(l1_range_iter));
  }
  // set task type
  if (Status::kOk == ret) {
    current_task_type_ = context_.task_type_;
    //change_info_.task_type_ = context_.task_type_;
  }
  PLACEMENT_DELETE(MetaDataSingleIterator, arena_, l1_range_iter);
  return ret;
}

int CompactionJob::prepare_major_self_task(const int64_t extents_limit,
                                           bool is_auto,
                                           const bool is_delete) {
  int ret = 0;
  MetaDataSingleIterator *l2_range_iter = nullptr;
  size_t delete_percent = 0; // not do reuse
  context_.output_level_ = Compaction::L2;
  if (!is_major_self_task(context_.task_type_)) {
    ret = Status::kCorruption;
    COMPACTION_LOG(ERROR, "prepare major self compaction wrong task_type ",
        K(ret), K((int)context_.task_type_));
  } else if (FAILED(build_self_iterator(2, l2_range_iter, delete_percent))) {
    COMPACTION_LOG(WARN,"build l2 compaction iterator failed", K(ret), K(delete_percent));
  } else if (is_auto) {
    if (FAILED(build_auto_self_compaction(2, l2_range_iter))) {
      COMPACTION_LOG(WARN, "failed to pick extents", K(ret));
    }
  } else if (is_delete) {
    if (FAILED(build_delete_major_self_compaction(extents_limit, l2_range_iter))) {
      SE_LOG(WARN, "failed to build delete major self task", K(ret), K(extents_limit));
    }
  } else if (FAILED(build_multiple_major_self_compaction(delete_percent, extents_limit, l2_range_iter))) {
    COMPACTION_LOG(WARN, "build multiple major compaction task failed", K(ret), K(delete_percent), K(extents_limit));
  } else {
  }

  // set task type
  if (Status::kOk == ret) {
    current_task_type_ = context_.task_type_;
    //change_info_.task_type_ = context_.task_type_;
  }
  PLACEMENT_DELETE(MetaDataSingleIterator, arena_, l2_range_iter);
  return ret;
}

int CompactionJob::prepare_major_task(const int64_t level1_pick_start_pos,
                                      const int64_t extents_limit,
                                      const bool has_splitted,
                                      const size_t delete_percent) {
  int ret = 0;
  MetaDataIterator *l1_range_iter = nullptr;
  MetaDataIterator *l2_range_iter = nullptr;
  Range range;
  SE_LOG(INFO, "begin to prepare major task", KP(this));

  if (is_major_task(context_.task_type_)) {
    context_.output_level_ = Compaction::L2;
  } else {
    COMPACTION_LOG(WARN, "prepare major task wrong task_type", K((int)context_.task_type_));
    return Status::kCorruption;
  }

  int64_t cf_delete_percent = context_.mutable_cf_options_->compaction_delete_percent;
  if (FAILED(build_major_l1_iterator(level1_pick_start_pos, range, l1_range_iter, delete_percent))) {
    COMPACTION_LOG(WARN, "build l1 compaction iterator failed(%d).", K(ret));
  } else if (nullptr == l1_range_iter) {
    // no need build task
  } else if (FAILED(build_major_l2_iterator(range, l2_range_iter))) {
    COMPACTION_LOG(WARN, "build l2 compaction iterator failed(%d).", K(ret));
  } else if (has_splitted) {
    if (FAILED(build_multiple_major_compaction_again(extents_limit,
        cf_delete_percent, l1_range_iter, l2_range_iter))) {
       COMPACTION_LOG(WARN, "build multiple major compaction task failed(%d).", K(ret));
    } else {
      current_task_type_ = context_.task_type_;
      //change_info_.task_type_ = context_.task_type_;
    }
  } else if (FAILED(build_multiple_major_compaction(extents_limit,
      cf_delete_percent, l1_range_iter, l2_range_iter))) {
    COMPACTION_LOG(WARN, "build multiple major compaction task failed(%d).", K(ret));
  } else {
    // SPLIT_TASK maybe set during build_multiple_major 
    if (current_task_type_ == TaskType::SPLIT_TASK) {
      //change_info_.task_type_ = TaskType::SPLIT_TASK;
    } else {
      current_task_type_ = context_.task_type_;
      //change_info_.task_type_ = context_.task_type_;
    }
  }

  PLACEMENT_DELETE(MetaDataIterator, arena_, l1_range_iter);
  PLACEMENT_DELETE(MetaDataIterator, arena_, l2_range_iter);
  return ret;
}


int CompactionJob::prepare() {
  if (!inited_) return Status::kIncomplete;

  if (TaskType::INTRA_COMPACTION_TASK == context_.task_type_) {
    context_.output_level_ = storage::Compaction::L0;
  } else if (TaskType::MINOR_COMPACTION_TASK == context_.task_type_ ||
      TaskType::STREAM_COMPACTION_TASK == context_.task_type_ ||
      TaskType::MINOR_DELETE_COMPACTION_TASK == context_.task_type_) {
    context_.output_level_ = storage::Compaction::L1;
  } else {
    COMPACTION_LOG(ERROR, "prepare compaction wrong task_type ", K((int)context_.task_type_));
    return Status::kCorruption;
  }

  int ret = 0;
  int64_t all_way_size = ReuseBlockMergeIterator::MAX_CHILD_NUM;
  int64_t way_size = 0;
  CompactWay *compact_way = new CompactWay[all_way_size];
  storage::Range wide_range;
  if (FAILED(get_all_l0_range(arena_, all_way_size, compact_way, way_size, wide_range))) {
    COMPACTION_LOG(WARN, "scan level 0' extents for compaction failed.", K(ret));
  } else if (way_size == 0) {
    COMPACTION_LOG(INFO, "has no level 0's data need compaction");
  } else {
    SE_LOG(INFO, "has ways to go compaction, all L0 has ways,",
        K(way_size),
        K(meta_snapshot_->get_extent_layer_version(0)->get_extent_layer_size()),
        K(wide_range));

    storage::RangeIterator *iterators[all_way_size];
    if (TaskType::INTRA_COMPACTION_TASK == context_.task_type_) {
      // We set force_layer_sequence the smalleset (sequence_number - 1) here.
      context_.force_layer_sequence_ = compact_way[0].sequence_number_ - 1;
      if (FAILED(build_compaction_iterators(arena_,
                                            compact_way,
                                            way_size /* total way size, not used*/,
                                            way_size /* max way size */,
                                            iterators,
                                            all_way_size /* returned way size */))) {
        COMPACTION_LOG(WARN, "build compaction iterators level0 failed", K(ret));
      }
      assert(all_way_size <= way_size);
    } else {
      if (FAILED(build_plus_l1_compaction_iterators(arena_,
                                                    compact_way,
                                                    way_size,
                                                    wide_range, iterators,
                                                    all_way_size))) {
        // plus level 1 's iterator;
        COMPACTION_LOG(WARN, "build compaction iterators level0 and level1 failed", K(ret));
      } else if (TaskType::MINOR_COMPACTION_TASK != context_.task_type_ &&
          TaskType::STREAM_COMPACTION_TASK != context_.task_type_) {
        ret = Status::kCorruption;
        COMPACTION_LOG(WARN, "build compaction wrong task_type", K((int)context_.task_type_));
      }
    }

    if (Status::kOk == ret &&
        FAILED(build_multiple_compaction(arena_, iterators, all_way_size, common::kMaxSequenceNumber))) {
      COMPACTION_LOG(WARN, "build compaction failed.", K(way_size), K(wide_range), K(ret));
    }
  }

  if (Status::kOk == ret && task_to_run_ > 0) {
    current_task_type_ = context_.task_type_;
   // change_info_.task_type_ = context_.task_type_;
    COMPACTION_LOG(INFO, "success to prepare task", KP(this), K((int)current_task_type_));
  }

  if (nullptr != compact_way) {
    delete[] compact_way;
  }

  return ret;
}

int CompactionJob::run() {
  int ret = Status::kOk;
  for (Compaction *compaction : compaction_tasks_) {
    ret = compaction->run();
    if (FAILED(ret)) {
      COMPACTION_LOG(ERROR, "run compaction failed, cleanup compact data.", K(ret));
      compaction->cleanup();
      break;
    } else {
      COMPACTION_LOG(INFO, "complete one compaction",
                     K(compaction->get_stats().record_stats_),
                     K(compaction->get_stats().perf_stats_));
      compaction_stats_.record_stats_.add(compaction->get_stats().record_stats_);
      compaction_stats_.perf_stats_.add(compaction->get_stats().perf_stats_);
    }
  }
  return ret;
}

int CompactionJob::append_change_info(const ChangeInfo &other)
{
  int ret = Status::kOk;
  if (FAILED(change_info_.merge(other))) {
    COMPACTION_LOG(WARN, "failed to merge change info", K(ret), K(change_info_), K(other));
  }
  return ret;
}

int CompactionJob::parse_meta(const table::InternalIterator *iter, ExtentMeta *&extent_meta)
{
  int ret = Status::kOk;
  Slice key_buf;
  int64_t pos = 0;

  if (nullptr == iter) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "invalid argument", K(ret), KP(iter));
  } else {
    key_buf = iter->key();
    extent_meta = reinterpret_cast<ExtentMeta*>(const_cast<char*>(key_buf.data()));
  }
  return ret;
}

int CompactionJob::get_all_l0_range(ArenaAllocator &arena,
    const int64_t all_way_size,
    CompactWay *compact_way, int64_t &way_size, Range &wide_range) {
  int ret = Status::kOk;
  ExtentLayerVersion *layer_version = meta_snapshot_->get_extent_layer_version(0);
  if (nullptr == layer_version
      || layer_version->get_total_normal_extent_count() <= 0) {
    way_size = 0;
    return ret;
  }

  common::Slice smallest_key;
  common::Slice largest_key;
  
  ExtentLayer *layer = nullptr;
  way_size = std::min((int64_t) layer_version->get_extent_layer_size(), all_way_size);
  for (int32_t i = 0; SUCCED(ret) && i < way_size && 0 == ret; i++) {
    // from old to new
    layer = layer_version->get_extent_layer(i);
    if (layer->extent_meta_arr_.size() <= 0) {
      // the sub_table has empty layer only when it has no data
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "invalid level0 layer, has no extents", K(ret), K(i), K(way_size), "extent_count", layer->extent_meta_arr_.size(), KP(layer_version));
      break;
    }
    compact_way[i].sequence_number_ = layer->sequence_number_;
    compact_way[i].extent_count_ = layer->extent_meta_arr_.size();
    ExtentMeta *start = layer->extent_meta_arr_[0];
    ExtentMeta *end = layer->extent_meta_arr_[layer->extent_meta_arr_.size() - 1];
    if (nullptr == start || nullptr == end) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "start or end is null", KP(start), K(i), K(ret));
      break;
    }
    compact_way[i].range_.start_key_ = start->smallest_key_.Encode();
    compact_way[i].range_.end_key_ = end->largest_key_.Encode();

    db::InternalKeyComparator ik_comparator(context_.data_comparator_);
    if (0 == largest_key.size() || 
        ik_comparator.Compare(compact_way[i].range_.end_key_, largest_key) > 0)
      largest_key = compact_way[i].range_.end_key_.deep_copy(arena_);
    if (0 == smallest_key.size() || 
        ik_comparator.Compare(compact_way[i].range_.start_key_, smallest_key) < 0)
      smallest_key = compact_way[i].range_.start_key_.deep_copy(arena_);
  }
  wide_range.start_key_ = smallest_key;
  wide_range.end_key_ = largest_key;

  return ret;
}

int CompactionJob::build_compaction_iterators(ArenaAllocator &arena,
                                              const CompactWay *compact_way,
                                              const int64_t total_way_size,
                                              const int64_t max_compact_way,
                                              storage::RangeIterator **iterators,
                                              int64_t &iter_size)
{
  int ret = Status::kOk;
  ExtentLayerVersion *layer_version = nullptr;
  ExtentLayer *extent_layer = nullptr;
  table::InternalIterator *meta_iter = nullptr;
  MetaDataIterator *range_iter = nullptr;
  int64_t level = 0;
  int64_t layer_index = 0;
  int64_t iter_index = 0;

  if (nullptr == compact_way) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "invalid argument", K(ret));
  } else if (max_compact_way > iter_size) {
    ret = Status::kNoSpace;
    COMPACTION_LOG(WARN, "compact way size out of range", K(ret), K(max_compact_way), K(iter_size));
  } else {
    for (int64_t index = 0; Status::kOk == ret && index < max_compact_way; ++index) {
      if (0 > compact_way[index].level_ || 2 < compact_way[index].level_) {
        ret = Status::kErrorUnexpected;
        COMPACTION_LOG(WARN, "unexpected error, invalid compact level", K(ret), K(index), "level", compact_way[index].level_);
      } else {
        level = compact_way[index].level_;
        layer_index = (0 == level) ? index : 0;
        LayerPosition layer_position(level, layer_index);
        if (FAILED(create_meta_iterator(arena,
                                        context_.internal_comparator_,
                                        meta_snapshot_,
                                        layer_position,
                                        meta_iter))) {
          COMPACTION_LOG(WARN, "fail to create meta iterator", K(ret), K(index), K(layer_position));
        } else {
          storage::MetaType type(MetaType::SSTable, MetaType::Extent, MetaType::InternalKey, level, iter_index, iter_index);
          if (nullptr == (range_iter = ALLOC_OBJECT(MetaDataIterator,
                                                     arena_,
                                                     type,
                                                     meta_iter,
                                                     compact_way[index].range_,
                                                     *context_.data_comparator_))) {
            ret = Status::kMemoryLimit;
            COMPACTION_LOG(WARN, "fail to allocate memory for range iter", K(ret), K(index), K(layer_position));
          } else {
            iterators[iter_index++] = range_iter;
            COMPACTION_LOG(INFO, "success to build compact iterator", K(index), K(layer_position), K(compact_way[index].sequence_number_), K(compact_way[index].range_));
          }
        }
      }
    }
  }

  if (Status::kOk == ret) {
    iter_size = iter_index;
  }
  return ret;    
}

int CompactionJob::build_self_iterator(const int64_t level,
                                      MetaDataSingleIterator *&range_iter,
                                      size_t delete_percent) {
  int ret = Status::kOk;

  table::InternalIterator *meta_iter = nullptr;
  LayerPosition layer_position(level, 0);

  if (FAILED(create_meta_iterator(arena_,
                                  context_.internal_comparator_,
                                  meta_snapshot_,
                                  layer_position,
                                  meta_iter))) {
    COMPACTION_LOG(WARN, "create meta iterator failed", K(ret), K(layer_position));
  } else {
    MetaType type(MetaType::SSTable, MetaType::Extent, MetaType::InternalKey, level, level, 0);
    range_iter = ALLOC_OBJECT(MetaDataSingleIterator, arena_, type, meta_iter);
  }
  return ret;
}

int CompactionJob::build_major_l2_iterator(const storage::Range &wide_range,
                                           MetaDataIterator *&range_iter) {
  int ret = Status::kOk;
  table::InternalIterator *meta_iter = nullptr;
  LayerPosition layer_position(2, 0);

  if (FAILED(create_meta_iterator(arena_,
                                  context_.internal_comparator_,
                                  meta_snapshot_,
                                  layer_position,
                                  meta_iter))) {
    COMPACTION_LOG(WARN, "l1 create meta iterator failed", K(ret));
  } else if (IS_NULL(meta_iter)) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "create meta iter failed", K(ret));
  } else {  // create l2's range iterator
    int64_t end_key_size = wide_range.end_key_.size();
    // reset SequenceNumber of L2's search end key to 0
    // for includes all user key records
    memset(const_cast<char *>(wide_range.end_key_.data()) + end_key_size - 8, 0, 8);
    MetaType type(MetaType::SSTable, MetaType::Extent, MetaType::InternalKey, 2, 2, 0);
    range_iter = ALLOC_OBJECT(MetaDataIterator, arena_, type, meta_iter, wide_range, *context_.data_comparator_);
  }
  return ret;
}

int CompactionJob::build_major_l1_iterator(const int64_t level1_pick_start_pos,
                                           storage::Range &wide_range,
                                           MetaDataIterator *&range_iter,
                                           size_t delete_percent) {
  int ret = 0;
  table::InternalIterator *meta_iter = nullptr;
  ExtentMeta *extent_meta = nullptr;
  LayerPosition layer_position(1, 0);

  if (FAILED(create_meta_iterator(arena_,
                                  context_.internal_comparator_,
                                  meta_snapshot_,
                                  layer_position,
                                  meta_iter))) {
    COMPACTION_LOG(WARN, "fail to create meta iterator", K(ret), K(layer_position));
  } else if (nullptr == meta_iter) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "create meta iter failed", K(ret));
  } else {  // get l1's range
    Slice smallest_key;
    Slice largest_key;
    meta_iter->SeekToLast();
    if (!meta_iter->Valid()) {
    } else if (FAILED(parse_meta(meta_iter, extent_meta))) {
      COMPACTION_LOG(WARN, "parse meta failed", K(ret));
    } else if (nullptr == extent_meta) {
      ret = Status::kErrorUnexpected;
      COMPACTION_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
    } else {
      largest_key = extent_meta->largest_key_.Encode().deep_copy(arena_);
      wide_range.end_key_ = largest_key;
    }
    if (Status::kOk == ret) {
      int64_t level1_extents = 0;
      meta_iter->SeekToFirst();
      if (context_.task_type_ != TaskType::MAJOR_DELETE_COMPACTION_TASK) {
      //if (!context_.delete_triggered_) {
        // normal major will pick L1 extents from level1_start_pick_pos 
        while (meta_iter->Valid()) {
          ++level1_extents;
          if (level1_extents >= level1_pick_start_pos) {
            break;
          }
          meta_iter->Next();
        }
      } else {
        // delete high major
      }
      if (!meta_iter->Valid()) {
        // do nothing
        return ret;
      } else if (FAILED(parse_meta(meta_iter, extent_meta))) {
        COMPACTION_LOG(WARN, "parse meta kv failed", K(ret));
      } else if (nullptr == extent_meta) {
        ret = Status::kErrorUnexpected;
        COMPACTION_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
      } else {
        smallest_key = extent_meta->smallest_key_.Encode().deep_copy(arena_);
        wide_range.start_key_ = smallest_key;
      }
    }
    if (Status::kOk == ret) {
      if (context_.task_type_ != TaskType::MAJOR_DELETE_COMPACTION_TASK) {
        delete_percent = 0;
      }
      storage::MetaType type(MetaType::SSTable, MetaType::Extent,
                             MetaType::InternalKey, 1, 1, 0);
      range_iter = ALLOC_OBJECT(MetaDataIterator, arena_, type, meta_iter,
                                 wide_range, *context_.data_comparator_,
                                 delete_percent);
    }
  }
  return ret;
}

int CompactionJob::build_plus_l1_compaction_iterators(ArenaAllocator &arena,
                                                      const CompactWay *compact_way,
                                                      const int64_t way_size,
                                                      const storage::Range &wide_range,
                                                      storage::RangeIterator **iterators,
                                                      int64_t &iter_size)
{
  int ret = Status::kOk;
  LayerPosition layer_position(1, 0);
  table::InternalIterator *level1_meta_iterator = nullptr;
  MetaDataIterator *level1_range_iter = nullptr;
  int64_t end_key_size = 0;
  int64_t max_compaction_way_size = ReuseBlockMergeIterator::MAX_CHILD_NUM;
  int64_t l0_way_size = std::min(max_compaction_way_size - 1, way_size);

  if (nullptr == compact_way || way_size < 0) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "invalid argument", K(ret), KP(compact_way), K(way_size));
  } else if (l0_way_size + 1 > iter_size) {
    ret = Status::kNoSpace;
    COMPACTION_LOG(WARN, "total iter size out of range", K(ret), K(l0_way_size), K(iter_size));
  } else if (FAILED(build_compaction_iterators(arena, compact_way, way_size, l0_way_size, iterators, iter_size))) {
    COMPACTION_LOG(WARN, "fail to build compaction iterators", K(ret), K(way_size), K(l0_way_size), K(iter_size));
  } else if (FAILED(create_meta_iterator(arena,
                                         context_.internal_comparator_,
                                         meta_snapshot_,
                                         layer_position,
                                         level1_meta_iterator))) {
    COMPACTION_LOG(WARN, "fail to create meta iterator", K(ret), K(layer_position));
  } else {
    end_key_size =  wide_range.end_key_.size();
    // reset SequenceNumber of L1's search end key to 0
    // for includes all user key records
    // it will be little tricky
    memset(const_cast<char *>(wide_range.end_key_.data()) + end_key_size - 8, 0, 8);
    storage::MetaType type(MetaType::SSTable, MetaType::Extent, MetaType::InternalKey, 1, l0_way_size, l0_way_size);
    if (nullptr == (level1_range_iter = ALLOC_OBJECT(MetaDataIterator,
                                                      arena_,
                                                      type,
                                                      level1_meta_iterator,
                                                      wide_range,
                                                      *context_.data_comparator_))) {
      ret = Status::kMemoryLimit;
      COMPACTION_LOG(WARN, "fail to allocate memory for level1 range iter", K(ret));
    } else {
      iterators[l0_way_size] = level1_range_iter;
      iter_size = l0_way_size + 1;
    }
  }

  return ret;
}
int CompactionJob::deal_with_split_batch(
    const int64_t merge_limit,
    const MetaDescriptorList &extents,
    SplitCompaction *&split_task) {
  int ret = 0;
  // always split l1's extent, use l2_extent's startukey as split_key
  int64_t extents_cnt = 0;
  size_t idx = 0;
  Slice l2_last_endukey;
  size_t l1_idx = 0;
  autovector<Slice> split_keys;
  MetaDescriptorList l1_extents;
  for (size_t id = 0 ; id < extents.size(); ++id) {
    if (1 == extents.at(id).type_.level_) {
      l1_extents.push_back(extents.at(id));
    }
  }
  size_t last_idx = extents.size();
  const Comparator *data_cmp = context_.data_comparator_;
  if (nullptr == data_cmp) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "data cmp is null", K(ret));
  }

  while (idx < extents.size() && Status::kOk == ret) {
    if (extents_cnt + 1> merge_limit
        && Compaction::L2 == extents.at(idx).type_.level_
        && data_cmp->Compare(l2_last_endukey, extents.at(idx).get_start_user_key()) < 0) {
      // compare to last l2 level extent's end_userkey
      // get a splitted extent
      Slice split_key = extents.at(idx).get_start_user_key();
      while (l1_idx < l1_extents.size() && Status::kOk == ret) {
        int cmp = data_cmp->Compare(l1_extents.at(l1_idx).get_start_user_key(), split_key);
        if (cmp < 0) {
          int next_cmp = data_cmp->Compare(l1_extents.at(l1_idx).get_end_user_key(), split_key);
          if ( next_cmp >= 0) {
            if (nullptr == split_task) {
              // Note make kSplit output_level to L1, and reset L2 after constructor
              CompactionContext task_context = context_;
              task_context.output_level_ = Compaction::Level::L1;
              task_context.task_type_ = TaskType::SPLIT_TASK;
              split_task = ALLOC_OBJECT(SplitCompaction, arena_, task_context, cf_desc_, arena_);
            }
            if (nullptr == split_task) {
               ret = Status::kMemoryLimit;
               COMPACTION_LOG(WARN, "split task is null.", K(ret));
            } else { // add split key to split_task
              extents_cnt = 0;
              split_task->add_split_key(split_key);
              if (last_idx != l1_idx) {
                if (FAILED(split_task->add_merge_batch(l1_extents, l1_idx, l1_idx + 1))) {
                  COMPACTION_LOG(WARN, "failed to add merge batch", K(ret), K(l1_idx));
                } else {
                  last_idx = l1_idx;
                }
              }
            }
            break;
          }
        }
        if (Status::kOk == ret) {
          ++l1_idx;
        }
      }
    }
    if (Status::kOk == ret) {
      if (Compaction::L2 == extents.at(idx).type_.level_ ) {
        l2_last_endukey = extents.at(idx).get_end_user_key();
      }
      ++idx;
      ++extents_cnt;
    }
  }
  return ret;
}

int CompactionJob::build_multiple_major_compaction_again(
                                    const int64_t merge_limit,
                                    const size_t delete_percent,
                                    storage::RangeIterator *l1_iterator,
                                    storage::RangeIterator *l2_iterator) {
  int ret = 0;
  ReuseBlockMergeIterator extent_merge_iterator(arena_,
                                                *context_.data_comparator_);
  storage::RangeIterator *iterators[2] = {l1_iterator, l2_iterator};
  extent_merge_iterator.set_children(iterators, 2);
  extent_merge_iterator.seek_to_first();
  int64_t total_extent = 0;
  bool need_split = false;
  GeneralCompaction *compaction = nullptr;
  while (Status::kOk == ret && extent_merge_iterator.valid()) {
    if (context_.shutting_down_->load(std::memory_order_acquire)) {
      COMPACTION_LOG(WARN,"process shutting down, break compaction.");
      ret = Status::kShutdownInProgress;
      return ret;
    }
    const MetaDescriptorList &extents =
        extent_merge_iterator.get_output();
    total_extent += extents.size();
    if (nullptr == compaction) {
      compaction = ALLOC_OBJECT(GeneralCompaction, arena_, context_, cf_desc_, arena_);
    }
    if (nullptr == compaction) {
      ret = Status::kMemoryLimit;
      COMPACTION_LOG(WARN, "major compaction is null", K(ret));
    } else if (FAILED(compaction->add_merge_batch(extents, 0, extents.size()))) {
      COMPACTION_LOG(WARN, "failed to add merge batch", K(ret), K(extents.size()));
    } else if ((int64_t)compaction->get_extent_size() > merge_limit) {
      compaction->set_delete_percent(delete_percent);
      add_compaction_task(compaction);
      compaction = nullptr;
    }
    if (Status::kOk == ret) {
      extent_merge_iterator.next();
    }
  }
  if (Status::kOk == ret && nullptr != compaction) {
    compaction->set_delete_percent(delete_percent);
    add_compaction_task(compaction);
    compaction = nullptr;
  }
  COMPACTION_LOG(INFO, "build major comapctions again",
      K(merge_limit), K(total_extent));
  return ret;
}

int CompactionJob::build_multiple_major_self_compaction(
    const size_t delete_percent,
    const int64_t extents_limit,
    storage::RangeIterator *l2_iterator) {
  int ret = 0;
  if (nullptr == l2_iterator) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "l2_iterator is null", K(ret));
    return ret;
  }
  l2_iterator->seek_to_first();
  GeneralCompaction *compaction = nullptr;
  int64_t total_extents = 0;
  MetaDescriptorList extents;
  while(Status::kOk == ret && l2_iterator->valid()) {
    MetaDescriptor md = l2_iterator->get_meta_descriptor().deep_copy(arena_);
    if (context_.shutting_down_->load(std::memory_order_acquire)) {
      ret = Status::kShutdownInProgress;
      COMPACTION_LOG(WARN, "process shutting down, break compaction.", K(ret));
      break;
    }
    const ExtentMeta *meta = ExtentMetaManager::get_instance().get_meta(ExtentId(md.block_position_.first));
    if (nullptr != meta && meta->num_entries_) {
      md.delete_percent_ = meta->num_deletes_ * 100 / meta->num_entries_;
    }
    COMPACTION_LOG(DEBUG, "MAJOR SELF: meta descriptor", K(md), K(extents.size()));
    if ((int64_t)extents.size() > extents_limit && extents.size()) {
      const Slice last_userkey = extents.at(extents.size() - 1).get_end_user_key();
      const Slice cur_userkey = md.get_start_user_key();
      if (0 == context_.data_comparator_->Compare(last_userkey, cur_userkey)) {
        extents.push_back(md);
      } else {
        if (nullptr == compaction) {
          compaction = ALLOC_OBJECT(GeneralCompaction, arena_, context_, cf_desc_, arena_);
        }
        if (nullptr == compaction) {
          ret = Status::kMemoryLimit;
          COMPACTION_LOG(WARN, "major compaction is null", K(ret));
        } else if (FAILED(compaction->add_merge_batch(extents, 0, extents.size()))) {
          COMPACTION_LOG(WARN, "failed to add merge batch", K(ret), K(extents.size()));
        } else {
          compaction->set_delete_percent(delete_percent);
          add_compaction_task(compaction);
          compaction = nullptr;
          total_extents += extents.size();
          extents.clear();
        }
      }
    } else {
     extents.push_back(md);
    }
    if (Status::kOk == ret) {
      l2_iterator->next();
      if (!l2_iterator->status().ok()) {
        ret = l2_iterator->status().code();
      }
    }
  }
  if (Status::kOk == ret && extents.size()) {
    if (nullptr == compaction) {
      compaction = ALLOC_OBJECT(GeneralCompaction, arena_, context_, cf_desc_, arena_);
    }
    if (nullptr == compaction) {
      ret = Status::kMemoryLimit;
      COMPACTION_LOG(WARN, "major compaction is null", K(ret));
    } else if (FAILED(compaction->add_merge_batch(extents, 0, extents.size()))) {
      COMPACTION_LOG(WARN, "failed to add merge batch", K(ret), K(extents.size()));
    } else {
      compaction->set_delete_percent(delete_percent);
      add_compaction_task(compaction);
      compaction = nullptr;
      total_extents += extents.size();
      extents.clear();
    }
  }
  COMPACTION_LOG(INFO, "MAJOR SELF: build major self comapctions",
      K(total_extents), K(delete_percent), K(extents_limit));
  return ret;
}
int CompactionJob::build_multiple_major_compaction(
    const int64_t merge_limit,
    const size_t delete_percent,
    storage::RangeIterator *l1_iterator,
    storage::RangeIterator *l2_iterator) {
  int ret = 0;
  ReuseBlockMergeIterator extent_merge_iterator(arena_,
                                                *context_.data_comparator_);
  storage::RangeIterator *iterators[2] = {l1_iterator, l2_iterator};
  extent_merge_iterator.set_children(iterators, 2);
  extent_merge_iterator.seek_to_first();
  int64_t total_extent = 0;
  SplitCompaction *split_task = nullptr;
  while (Status::kOk == ret && extent_merge_iterator.valid()) {
    if (context_.shutting_down_->load(std::memory_order_acquire)) {
      COMPACTION_LOG(WARN,
                     "process shutting down, break compaction.");
      ret = Status::kShutdownInProgress;
      return ret;
    }
    const MetaDescriptorList &extents = extent_merge_iterator.get_output();
    total_extent += extents.size();
    if ((int64_t)extents.size() > merge_limit
        && FAILED(deal_with_split_batch(merge_limit, extents, split_task))) {
      COMPACTION_LOG(WARN,
          "add extents batch failed", K(merge_limit), K(total_extent));
    }
    if (Status::kOk == ret) {
      if (nullptr == split_task) {
        BlockPosition pos;
        pos.first = extents_list_.size();
        for (size_t idx = 0; idx < extents.size(); ++idx) {
          extents_list_.push_back(extents.at(idx));
        }
        pos.second = extents_list_.size();
        merge_batch_indexes_.emplace_back(pos);
      }
      extent_merge_iterator.next();
    }
  }

  if (FAILED(ret)) {
  } else if (nullptr != split_task) {
    add_compaction_task(split_task);
    if (1 != task_to_run_) {
      ret = Status::kInvalidArgument;
      COMPACTION_LOG(WARN, "failed, split task count is invalid",
          K(ret), K(task_to_run_));
    } else {
      // Split should be handled specially.
      current_task_type_ = TaskType::SPLIT_TASK;
    }
  } else {
    storage::GeneralCompaction *compaction = nullptr;
    for (size_t idx = 0; idx < merge_batch_indexes_.size(); ++idx) {
      BlockPosition &pos = merge_batch_indexes_.at(idx);
      size_t batch_size = pos.second - pos.first;
      if (nullptr == compaction) {
        compaction = ALLOC_OBJECT(GeneralCompaction, arena_, context_, cf_desc_, arena_);
      }
      if (nullptr == compaction) {
        ret = Status::kMemoryLimit;
        COMPACTION_LOG(WARN, "major compaction is null", K(ret));
      } else if (FAILED(compaction->add_merge_batch(extents_list_, pos.first, pos.second))) {
        COMPACTION_LOG(WARN, "failed to add merge batch", K(ret), K(pos.first), K(pos.second));
      } else if ((int64_t)compaction->get_extent_size() > merge_limit) {
        add_compaction_task(compaction);
        compaction = nullptr;
      }
    }
    if (Status::kOk == ret) {
      if (nullptr != compaction) {
        add_compaction_task(compaction);
        compaction = nullptr;
      }
      if (task_to_run_ > 0) {
        current_task_type_ = context_.task_type_;
      }
    }
  }
  COMPACTION_LOG(INFO, "build major comapctions first", K(merge_limit), K(total_extent), K((int)current_task_type_));
  return ret;
}

// TODO (Zhao Dongsheng): Refactor this function, because it has multiple return points.
// Maybe Stream or Minor compaction
int CompactionJob::build_multiple_compaction(ArenaAllocator &arena,
                                             storage::RangeIterator **iterators,
                                             int64_t iter_size,
                                             const int64_t merge_limit)
{
  int ret = Status::kOk;
  int64_t total_extent = 0;
  int64_t merge_extent = 0;
  storage::Compaction *compaction = nullptr;
  ReuseBlockMergeIterator extent_merge_iterator(arena, *context_.data_comparator_);

  if (FAILED(extent_merge_iterator.set_children(iterators, iter_size))) {
    COMPACTION_LOG(WARN, "set children failed", K(ret), K(iter_size));
  } else {
    extent_merge_iterator.seek_to_first();
    ret = extent_merge_iterator.status().code();
    while (SUCCED(ret) && extent_merge_iterator.valid()) {
      if (context_.shutting_down_->load(std::memory_order_acquire)) {
        COMPACTION_LOG(INFO, "process shutting down, break compaction.");
        ret = Status::kShutdownInProgress;
        return ret;
      }
      const MetaDescriptorList &extents = extent_merge_iterator.get_output();
      total_extent += extents.size();
      if (extents.size() > 1) {
        merge_extent += extents.size();
      }
      if (nullptr == compaction) {
        compaction = ALLOC_OBJECT(GeneralCompaction, arena, context_, cf_desc_, arena_);
      }
      if (nullptr != compaction) {
        if (FAILED(compaction->add_merge_batch(extents, 0, extents.size()))) {
          COMPACTION_LOG(WARN, "fail to add merge batch", K(ret), K(extents.size()));
          return ret;
        }
      } else {
        ret = Status::kMemoryLimit;
        COMPACTION_LOG(ERROR, "cannot allocate memory for create compaction task.", K(ret), K(extents.size()));
        return ret;
      }

      if (merge_extent > merge_limit) {
        COMPACTION_LOG(INFO, "split new task merge_extent.", K(merge_extent), K(total_extent));
        add_compaction_task(compaction);
        // reset compaction for next batch;
        compaction = nullptr;
        // clear status;
        merge_extent = 0;
      }
      extent_merge_iterator.next();
      ret = extent_merge_iterator.status().code();
    }

    if (SUCCED(ret) && nullptr != compaction) {
      COMPACTION_LOG(INFO, "split last new task merge_extent", K(merge_extent), K(total_extent));
      add_compaction_task(compaction);
    }
  }

  return ret;
}

int CompactionJob::create_meta_iterator(ArenaAllocator &arena,
                                        const InternalKeyComparator *internal_comparator,
                                        const db::Snapshot *snapshot,
                                        const storage::LayerPosition &layer_position,
                                        table::InternalIterator *&iterator)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  ExtentLayerIterator *layer_iterator = nullptr;
  ExtentLayer *extent_layer = nullptr;
  iterator = nullptr;

  if (IS_NULL(internal_comparator) || IS_NULL(snapshot) || !layer_position.is_valid()) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "invalid argument", K(ret), KP(internal_comparator), KP(snapshot), K(layer_position));
  } else if (IS_NULL(extent_layer = snapshot->get_extent_layer(layer_position))) {
    ret = Status::kErrorUnexpected;
    COMPACTION_LOG(WARN, "unexpected error, fail to get extent layer", K(ret), K(layer_position));
  } else if (IS_NULL(buf = (char *)arena.alloc(sizeof(storage::ExtentLayerIterator)))) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "fail to allocate memory for buf", K(ret), "size", sizeof(storage::ExtentLayerIterator));
  } else if (IS_NULL(layer_iterator = new (buf) ExtentLayerIterator())) {
    ret = Status::kErrorUnexpected;
    COMPACTION_LOG(WARN, "unexpected error, fail to construct ExtentLayerIterator", K(ret));
  } else if (FAILED(layer_iterator->init(internal_comparator, layer_position, extent_layer))) {
    COMPACTION_LOG(WARN, "fail to init layer_iterator", K(ret));
  } else {
    iterator = layer_iterator;
  }
  return ret;
}

void CompactionJob::report_started_compaction() {
//  ThreadStatusUtil::set_subtable_id(cf_desc_.column_family_id_,
//                                    context_.cf_options_->env,
//                                    context_.enable_thread_tracking_);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION); 
}

#ifndef NDEBUG
int CompactionJob::TEST_get_all_l0_range(ArenaAllocator &arena,
    int64_t all_way_size, CompactWay *compact_way, int64_t &way_size,
    storage::Range &wide_range, bool verbose) {
  int ret = get_all_l0_range(arena, all_way_size, compact_way, way_size,
      wide_range);
  if (verbose) {
    COMPACTION_LOG(ERROR, "wide_range", K(wide_range.start_key_), K(wide_range.end_key_));
    for (int i = 0; i < way_size; i++) {
    COMPACTION_LOG(ERROR, "way, seq, extent_count, level, Range", K(i), K(compact_way[i].sequence_number_), K(compact_way[i].extent_count_), K(compact_way[i].level_), K(compact_way[i].range_.start_key_), K(compact_way[i].range_.end_key_));
    }
  }
  return ret;
}
#endif

}  // namespace storage
}  // namespace smartengine
