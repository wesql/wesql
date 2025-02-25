//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/flush_job.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <vector>

#include "compact/compaction_job.h"
#include "compact/flush_iterator.h"
#include "compact/mt_ext_compaction.h"
#include "compact/reuse_block_merge_iterator.h"
#include "db/db.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "logger/log_module.h"
#include "memtable/memtable_list.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "memory/page_arena.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "storage/storage_manager.h"
#include "storage/storage_logger.h"
#include "table/extent_table_factory.h"
#include "table/merging_iterator.h"
#include "table/extent_writer.h"
#include "util/file_name.h"
#include "util/sync_point.h"

namespace smartengine {
using namespace cache;
using namespace common;
using namespace memory;
using namespace monitor;
using namespace storage;
using namespace table;
using namespace util;

namespace db {

BaseFlush::BaseFlush(ColumnFamilyData* cfd,
                     const common::ImmutableDBOptions& db_options,
                     const common::MutableCFOptions& mutable_cf_options,
                     common::CompressionType output_compression,
                     const std::atomic<bool>* shutting_down,
                     common::SequenceNumber earliest_write_conflict_snapshot,
                     JobContext& job_context,
                     std::vector<common::SequenceNumber> &existing_snapshots,
                     util::Directory* output_file_directory,
                     const util::EnvOptions *env_options,
                     memory::ArenaAllocator &arena)
    : cfd_(cfd),
      db_options_(db_options),
      mutable_cf_options_(mutable_cf_options),
      output_compression_(output_compression),
      shutting_down_(shutting_down),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      job_context_(job_context),
      existing_snapshots_(existing_snapshots),
      output_file_directory_(output_file_directory),
      env_options_(env_options),
      arena_(arena),
      tmp_arena_(CharArena::DEFAULT_PAGE_SIZE, 0, memory::ModId::kFlush),
      pick_memtable_called_(false)
{}

BaseFlush::~BaseFlush() {}

int BaseFlush::get_memtable_stats(int64_t &memory_usage, int64_t &num_entries, int64_t &num_deletes)
{
  int ret = Status::kOk;
  MemTable *memtable = nullptr;
  memory_usage = 0;
  num_entries = 0;
  num_deletes = 0;

  for (uint32_t i = 0; SUCCED(ret) && i < mems_.size(); ++i) {
    if (IS_NULL(memtable = mems_[i])) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "memtable mustn't be nullptr", K(ret), K(i), KP(memtable));
    } else {
      memory_usage += memtable->ApproximateMemoryUsage();
      num_entries += memtable->num_entries();
      num_deletes += memtable->num_deletes();
    }
  }

  return ret;
}

int BaseFlush::build_memtable_iterators(MemtableIterators &memtable_iterators)
{
  int ret = Status::kOk;
  MemTable *memtable = nullptr;
  InternalIterator *iterator = nullptr;
  ReadOptions read_options;
  read_options.total_order_seek = true;

  se_assert(pick_memtable_called_ && !mems_.empty());
  for (uint32_t i = 0; SUCCED(ret) && i < mems_.size(); ++i) {
    if (IS_NULL(memtable = mems_[i])) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "memtable mustn't be nullptr", K(ret), K(i), KP(memtable));
    } else if (IS_NULL(iterator = create_memtable_iterator(read_options, memtable))) {
      ret = Status::kMemoryLimit;
      FLUSH_LOG(WARN, "fail to construct memtable iterator", K(ret));
    } else {
      memtable_iterators.push_back(iterator);
    }
  }

  return ret;
}

int BaseFlush::build_memtable_merge_iterator(InternalIterator *&memtable_merge_iterator)
{
  int ret = Status::kOk;
  MergeIteratorBuilder merge_iterator_builder(&cfd_->internal_comparator(), &tmp_arena_);
  MemtableIterators memtable_iterators;
  
  if (FAILED(build_memtable_iterators(memtable_iterators))) {
    FLUSH_LOG(WARN, "failed to build memtable iterators", K(ret));
  } else if (memtable_iterators.empty()) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "at least one memtable iterator", K(ret));
  } else {
    for (auto iterator : memtable_iterators) {
      merge_iterator_builder.AddIterator(iterator);
    }

    if (IS_NULL(memtable_merge_iterator = merge_iterator_builder.Finish())) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "fail to construct memtable merge iterator", K(ret));
    }
  }

  return ret;
}

int BaseFlush::flush_data(const LayerPosition &output_layer_position,
                          const bool is_flush,
                          MiniTables &mini_tables)
{
  int ret = Status::kOk;
  InternalIterator *memtable_merge_iterator = nullptr;
  FlushIterator flush_iterator;
  ExtentWriter *extent_writer = nullptr;
  /**TODO(Zhao Dongsheng): The table space id pass by MiniTables is unsuitable.*/
  mini_tables.table_space_id_ = cfd_->get_table_space_id();
  /**TODO(Zhao Dongsheng): The way of obtaining the block cache is not elegent. */
  ExtentBasedTableFactory *tmp_factory = reinterpret_cast<ExtentBasedTableFactory *>(cfd_->ioptions()->table_factory);
  ExtentWriterArgs writer_args(tmp_factory->table_options().cluster_id,
                               cfd_->get_table_space_id(),
                               tmp_factory->table_options().block_restart_interval,
                               cfd_->ioptions()->env->IsObjectStoreInited() ? storage::OBJECT_EXTENT_SPACE : storage::FILE_EXTENT_SPACE,
                               cfd_->get_table_schema(),
                               &cfd_->internal_comparator(),
                               output_layer_position,
                               tmp_factory->table_options().block_cache.get(),
                               cfd_->ioptions()->row_cache.get(),
                               output_compression_,
                               mini_tables.change_info_);

  if (!output_layer_position.is_valid()) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "invalid argument", K(ret), K(output_layer_position), K(is_flush));
  } else if (IS_NULL(extent_writer = MOD_NEW_OBJECT(ModId::kExtentWriter, ExtentWriter))) {
    ret = Status::kMemoryLimit;
    FLUSH_LOG(WARN, "fail to construct table builder", K(ret));
  } else if (FAILED(extent_writer->init(writer_args))) {
    SE_LOG(WARN, "fail to init extent writer", K(ret));
  } else if (FAILED(build_memtable_merge_iterator(memtable_merge_iterator))) {
    FLUSH_LOG(WARN, "fail to build memtable merge iterator", K(ret));
  } else if (FAILED(flush_iterator.init(memtable_merge_iterator,
                                        cfd_->internal_comparator().user_comparator(),
                                        &existing_snapshots_,
                                        earliest_write_conflict_snapshot_,
                                        mutable_cf_options_.background_disable_merge))) {
    FLUSH_LOG(WARN, "fail to init flush iterator", K(ret));
  } else {
    flush_iterator.SeekToFirst();
    while (SUCCED(ret) && flush_iterator.Valid()) {
      //TODO(Zhao Dongsheng): All flushed data should migrate to block cache.
      extent_writer->set_migrate_flag();
      if (FAILED(extent_writer->append_row(flush_iterator.key(), flush_iterator.value()))) {
        FLUSH_LOG(WARN, "fail to append kv", K(ret));
      } else {
        flush_iterator.Next();
        ret = flush_iterator.inner_ret();
      }
    }

    if (FAILED(ret) || extent_writer->is_empty()) {
      extent_writer->rollback();
    } else if (FAILED(extent_writer->finish(nullptr /*extent_infos*/))) {
      FLUSH_LOG(WARN, "fail to finish table builder", K(ret));
    } else {
      //TODO(Zhao Dongsheng): fsync the directory is needless?
      if (nullptr != output_file_directory_) {
        output_file_directory_->Fsync();
      }
    }
  }

  //TODO(Zhao Dongsheng): Add destroy function for TableBuilder
  if (IS_NOTNULL(extent_writer)) {
    MOD_DELETE_OBJECT(ExtentWriter, extent_writer);
  }

  return ret;
}

int BaseFlush::write_level0(MiniTables &mini_tables)
{
  int ret = Status::kOk;
  uint64_t bytes_written = 0;
  const uint64_t start_micros = db_options_.env->NowMicros();
  bool is_flush = (FLUSH_TASK == job_context_.task_type_);
  int32_t layer_index = is_flush ? LayerPosition::NEW_GENERATE_LAYER_INDEX
                                 : LayerPosition::INVISIBLE_LAYER_INDEX;
  LayerPosition output_layer_position(0 /**level*/, layer_index);

  if ((FLUSH_TASK != job_context_.task_type_ && DUMP_TASK != job_context_.task_type_)) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "invalid argument", K(ret), KE_(job_context_.task_type));
  } else if (FAILED(flush_data(output_layer_position, is_flush, mini_tables))) {
    FLUSH_LOG(WARN, "fail to flush data", K(ret), K(output_layer_position), K(is_flush));
  } else {
    for (auto file_meta : mini_tables.metas) {
      bytes_written += file_meta.data_size_;
      FLUSH_LOG(INFO, "generate new level 0 extent", KE_(job_context_.task_type),
          "index_id", cfd_->GetID(), K(file_meta));
    }

    if (is_flush) {
      stop_record_flush_stats(bytes_written, start_micros);
    }

    FLUSH_LOG(INFO, "success to flush level0 data", KE(job_context_.task_type_),
        "index_id", cfd_->GetID(), "job_id", job_context_.job_id, "extent_count",
        mini_tables.metas.size(), K(bytes_written));
  }

  return ret;
}

int BaseFlush::after_run_flush(MiniTables &mtables, int ret, monitor::InstrumentedMutex *mutex) {
  //get the Gratest recovery point
  MemTable* last_mem = mems_.size() > 0 ? mems_.back() : nullptr;
  RecoveryPoint recovery_point;

  // cfd_->imm() need be protected by db mutex? if not, this lock can be removed
  mutex->Lock();
  if (SUCCED(ret)) {
    if (IS_NULL(last_mem)
        || IS_NULL(mtables.change_info_)
        || IS_NULL(cfd_)) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(ERROR, "arguments must not nullptr", K(ret), KP(last_mem), KP(cfd_));
    } else if (shutting_down_->load(std::memory_order_acquire)
               || cfd_->is_bg_stopped()) {
      ret = Status::kShutdownInProgress;
      SE_LOG(WARN, "shutting down", K(ret), K(cfd_->is_bg_stopped()));
    } else if (cfd_->IsDropped()) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "Column family has been dropped!", K(ret), K(cfd_->GetID()));
    } else {
      recovery_point = last_mem->get_recovery_point();
      SE_LOG(INFO, "get_recovery_point", KP(last_mem), K(recovery_point));
    }
  }
  if (FAILED(ret)) {
    // 2nd param of file_number is unused
    if (nullptr != cfd_ && nullptr != cfd_->imm()) {
      if (is_flush_task(job_context_.task_type_)) {
        cfd_->imm()->RollbackMemtableFlush(mems_, 0);
      } else if (nullptr != last_mem){
        last_mem->set_dump_in_progress(false);
      }
      StorageLogger::get_instance().abort();
    } else {
      FLUSH_LOG(ERROR, "unexpected error, can not abort", K(ret), K(cfd_->GetID()));
    }
  } 
  mutex->Unlock();

  if (SUCCED(ret)) {
    if (TaskType::DUMP_TASK == job_context_.task_type_) {
      if (FAILED(cfd_->apply_change_info(*(mtables.change_info_), true /*write_log*/, false /*is_replay*/, &recovery_point))) {
        StorageLogger::get_instance().abort();
        FLUSH_LOG(WARN, "failed to apply change info for dump", K(ret));
      } else {
        last_mem->set_temp_min_prep_log(UINT64_MAX);
      }
      last_mem->set_dump_in_progress(false);
    } else if (FAILED(cfd_->apply_change_info(*(mtables.change_info_),
        true /*write_log*/, false /*is_replay*/, &recovery_point, 
        &mems_, &job_context_.memtables_to_free))) {
      StorageLogger::get_instance().abort();
      FLUSH_LOG(WARN, "fail to apply change info", K(ret));
    } else {
      // do nothing
    }
  }
  if (TaskType::FLUSH_LEVEL1_TASK != job_context_.task_type_) {
    // flush level1 not alloc change_info from arena_
    FREE_OBJECT(ChangeInfo, arena_, mtables.change_info_);
  }
  return ret;
}

void BaseFlush::cancel() {
//  db_mutex_->AssertHeld();
}

int BaseFlush::stop_record_flush_stats(const int64_t bytes_written,
                                       const uint64_t start_micros) {
  int ret = 0;
  auto end_micros = db_options_.env->NowMicros();
  cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_FLUSHED,
                                     bytes_written);
  cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_WRITE,
                                     bytes_written);
  upload_current_flush_stats_to_global(bytes_written, start_micros, end_micros);
  RecordFlushIOStats();
  return ret;
}

void BaseFlush::upload_current_flush_stats_to_global(uint64_t bytes_written,
                                                     uint64_t start_micros,
                                                     uint64_t end_micros) {
  if (IS_NULL(db_options_.statistics)) {
    return;
  }
  db_options_.statistics->update_global_flush_stat(bytes_written, start_micros,
                                                   end_micros);
}

// delete M0's extens already exist
int BaseFlush::delete_old_M0(const InternalKeyComparator *internal_comparator, MiniTables &mtables) {
  int ret = Status::kOk;
  LayerPosition layer_position(0, storage::LayerPosition::INVISIBLE_LAYER_INDEX);
  Snapshot *current_snapshot = nullptr;
  const ExtentLayer *dump_layer = nullptr;
  const ExtentMeta *lob_extent_meta = nullptr;
  table::InternalIterator *meta_iter = nullptr;
  MetaDataSingleIterator *range_iter = nullptr;
  if (IS_NULL(current_snapshot = cfd_->get_meta_snapshot())) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "unexpected error, current snapshot must not nullptr", K(ret));
  } else {
    dump_layer = current_snapshot->get_extent_layer(layer_position);
    if (nullptr == dump_layer || dump_layer->extent_meta_arr_.size() <= 0) {
      // do nothing
    } else {
      // delete large object extent
      for (int32_t i = 0; i < dump_layer->lob_extent_arr_.size() && SUCCED(ret); i++) {
        lob_extent_meta = dump_layer->lob_extent_arr_.at(i);
        FLUSH_LOG(INFO, "delete large object extent for dump", K(cfd_->GetID()), "extent_id", lob_extent_meta->extent_id_);
        if (FAILED(mtables.change_info_->delete_large_object_extent(lob_extent_meta->extent_id_))) {
          FLUSH_LOG(WARN, "failed to delete large object extent", K(ret), K(i));
        }
      }

      if (SUCCED(ret)) {
        if (FAILED(CompactionJob::create_meta_iterator(arena_, internal_comparator, current_snapshot, layer_position, meta_iter))) {
          FLUSH_LOG(WARN, "create meta iterator failed", K(ret));
        } else {
          MetaType type(MetaType::SSTable, MetaType::Extent, MetaType::InternalKey, 0, 0, 0);
          range_iter = PLACEMENT_NEW(MetaDataSingleIterator, tmp_arena_, type, meta_iter);
          if (IS_NULL(range_iter)) {
            ret = Status::kErrorUnexpected;
            FLUSH_LOG(WARN, "dump range iter is null", K(ret));
          } else {
            range_iter->seek_to_first();
            while (range_iter->valid() && SUCCED(ret)) {
              MetaDescriptor extent_meta = range_iter->get_meta_descriptor().deep_copy(tmp_arena_);
              FLUSH_LOG(DEBUG, "delete extent for dump", K(cfd_->GetID()), K(extent_meta));
              if (FAILED(mtables.change_info_->delete_extent(extent_meta.layer_position_, extent_meta.extent_id_))) {
                FLUSH_LOG(WARN, "failed to delete extent", K(ret), K(extent_meta));
              } else {
                range_iter->next();
              }
            }
          }
        }
      }
    }
  }

  if (nullptr != current_snapshot) {
    cfd_->release_meta_snapshot(current_snapshot);
  }
  PLACEMENT_DELETE(MetaDataSingleIterator, arena_, range_iter);
  return ret;
}

// TODO(Zhao Dongsheng) : The function fill_table_cache is useless now.
int BaseFlush::fill_table_cache(const MiniTables &mtables) {
  int ret = 0;
  // Verify that the table is usable
  // We set for_compaction to false and don't OptimizeForCompactionTableRead
  // here because this is a special case after we finish the table building
  // No matter whether use_direct_io_for_flush_and_compaction is true,
  // we will regrad this verification as user reads since the goal is
  // to cache it here for further user reads
  if (IS_NULL(db_options_.env)
      || IS_NULL(env_options_)
      || IS_NULL(cfd_)
      || IS_NULL(cfd_->table_cache())) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "invalid ptr", K(ret), KP(db_options_.env), KP(env_options_), K(cfd_->GetID()));
    return ret;
  }
  //for (size_t i = 0; i < mtables.metas.size() && SUCCED(ret); i++) {
  //  const FileMetaData* meta = &mtables.metas[i];
  //  ExtentReader *extent_reader = nullptr;
  //  std::unique_ptr<InternalIterator, memory::ptr_destruct_delete<InternalIterator>> it(
  //      cfd_->table_cache()->create_iterator(ReadOptions(),
  //                                           cfd_->internal_comparator(),
  //                                           job_context_.output_level_ /*level*/,
  //                                           meta->extent_id_,
  //                                           extent_reader));
  //  if (FAILED(it->status().code())) {
  //    FLUSH_LOG(WARN, "iterator occur error", K(ret), K(i));
  //  //TODO(Zhao Dongsheng): Depreatd parameter paranoid_file_checks's value
  //  //is false. Remove paranoid_file_checks and move this check logic to
  //  //appropriate place in the future.
  //  } else if (false) {
  //    for (it->SeekToFirst(); it->Valid(); it->Next()) {
  //    }
  //    if (FAILED(it->status().code())) {
  //      FLUSH_LOG(WARN, "iterator is invalid", K(ret));
  //    }
  //  }
  //}
  return ret;
}

void BaseFlush::RecordFlushIOStats() {
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

FlushJob::FlushJob(const std::string& dbname, ColumnFamilyData* cfd,
                   const ImmutableDBOptions& db_options,
                   JobContext& job_context,
                   Directory* output_file_directory,
                   CompressionType output_compression,
                   Statistics* stats,
                   CompactionContext &context,
                   memory::ArenaAllocator &arena)
    : BaseFlush(cfd, db_options, *context.mutable_cf_options_, output_compression,
        context.shutting_down_, context.earliest_write_conflict_snapshot_, job_context,
        context.existing_snapshots_, output_file_directory, context.env_options_, arena),
      dbname_(dbname),
      stats_(stats),
      compaction_context_(context),
      meta_snapshot_(nullptr),
      compaction_(nullptr) {
  // Update the thread status to indicate flush.
  ReportStartedFlush();
  TEST_SYNC_POINT("FlushJob::FlushJob()");
}

FlushJob::~FlushJob() {
  ThreadStatusUtil::ResetThreadStatus();
  if (nullptr != compaction_) {
    FREE_OBJECT(MtExtCompaction, arena_, compaction_);
  }
}

void FlushJob::ReportStartedFlush() {
  assert(cfd_);
  assert(cfd_->ioptions());
//  ThreadStatusUtil::set_subtable_id(cfd_->GetID(), cfd_->ioptions()->env,
//                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_FLUSH);
  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_context_.job_id);
  IOSTATS_RESET(bytes_written);
}

void FlushJob::ReportFlushInputSize(const autovector<MemTable*>& mems) {
  uint64_t input_size = 0;
  for (auto* mem : mems) {
    input_size += mem->ApproximateMemoryUsage();
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_MEMTABLES, input_size);
}

// under mutex
void FlushJob::pick_memtable() {
  assert(!pick_memtable_called_);
  pick_memtable_called_ = true;
  assert(cfd_);
  assert(cfd_->imm());
  cfd_->imm()->PickMemtablesToFlush(&mems_);
  if (mems_.empty()) {
    return;
  }

  ReportFlushInputSize(mems_);
}

int FlushJob::run(MiniTables& mtables) {
  int ret = Status::kOk;
  if (IS_NULL(cfd_)) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "cfd is null", K(ret));
  } else if (mems_.empty()) {
    FLUSH_LOG(INFO, "Nothing in memtable to flush", K(cfd_->GetID()));
  } else {
    if (TaskType::FLUSH_LEVEL1_TASK == job_context_.task_type_) {
      if (FAILED(run_mt_ext_task(mtables))) {
        FLUSH_LOG(WARN, "failed to run mt ext task", K(ret));
      } else {
        FLUSH_LOG(INFO, "success to do flush level1 job", K(cfd_->GetID()));
      } 
    } else {
      se_assert(FLUSH_TASK == job_context_.task_type_);
      if (FAILED(write_level0(mtables))) {
        FLUSH_LOG(WARN, "fail to write level0 data", K(ret));
      } else if (FAILED(fill_table_cache(mtables))) {
        FLUSH_LOG(WARN, "fail to fill table cache", K(ret));
      } else {
        FLUSH_LOG(INFO, "success to do normal flush job", K(cfd_->GetID()));
      }
    }
  }

  RecordFlushIOStats();
  return ret;
}

int FlushJob::run_mt_ext_task(MiniTables &mtables) {
  int ret = Status::kOk;
  int64_t memory_usage = 0;
  int64_t num_entries = 0;
  int64_t num_deletes = 0;
  uint64_t start_micros = db_options_.env->NowMicros();
  FLUSH_LOG(INFO, "begin to run flush to level1 task", K(mems_.size()));
  if (IS_NULL(compaction_) || IS_NULL(cfd_)) {
    ret = Status::kNotInit;
    FLUSH_LOG(WARN, "compaction or cfd is null", K(ret), K(cfd_->GetID()));
  } else if (FAILED(get_memtable_stats(memory_usage, num_entries, num_deletes))) {
    FLUSH_LOG(WARN, "failed to get memtable stats", K(ret));
  } else {
    compaction_->add_input_bytes(memory_usage);
    if (FAILED(compaction_->run())) {
      FLUSH_LOG(WARN, "failed to do met_ext_compaction", K(ret));
    } else {
      const MiniTables &mini_tables = compaction_->get_mini_tables();
      if (FAILED(fill_table_cache(mini_tables))) {
        FLUSH_LOG(WARN, "failed to fill table cache", K(ret));
      } else {
        mtables.change_info_ = &compaction_->get_change_info();
        FLUSH_LOG(INFO, "Level-0 flush info", K(cfd_->GetID()));
        if (FAILED(delete_old_M0(compaction_context_.internal_comparator_,
                                 compaction_->get_apply_mini_tables()))) {
          FLUSH_LOG(WARN, "failed to delete old m0", K(ret));
        }
      }

      FLUSH_LOG(INFO, "complete one mt_ext compaction", K(ret),
          K(cfd_->GetID()), K(job_context_.job_id),
          K(memory_usage), K(num_entries), K(num_deletes),
          K(mini_tables.metas.size()), K(compaction_->get_stats().record_stats_),
          K(compaction_->get_stats().perf_stats_));
    }
  }

  return ret;
}

int FlushJob::prepare_flush_task(MiniTables &mtables) {
  int ret = Status::kOk;
  pick_memtable(); // pick memtables

  if (TaskType::FLUSH_LEVEL1_TASK == job_context_.task_type_) {
    if (FAILED(prepare_flush_level1_task())) {
      FLUSH_LOG(WARN, "failed to prepare flush level1 task", K(ret));
    }
  } else if (IS_NULL(mtables.change_info_ = ALLOC_OBJECT(ChangeInfo, arena_))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for ChangeInfo", K(ret));
  } else {
    mtables.change_info_->task_type_ = TaskType::FLUSH_TASK;
    if (FAILED(delete_old_M0(compaction_context_.internal_comparator_, mtables))) {
      FLUSH_LOG(WARN, "failed to delete old m0", K(ret));
    }
  }
  return ret;
}

int FlushJob::prepare_flush_level1_task() {
  int ret = 0;
  compaction_context_.output_level_ = 1;
  storage::Range wide_range;
  RangeIterator *l1_iterator = nullptr;
  MemtableIterators memtable_iterators;
  if (FAILED(build_memtable_iterators(memtable_iterators))) {
    FLUSH_LOG(WARN, "failed to build memtable iterators", K(ret));
  } else if (FAILED(get_memtable_range(memtable_iterators, wide_range))) {
    FLUSH_LOG(WARN, "failed to get memtable range", K(ret), K(wide_range));
  } else if(FAILED(build_l1_iterator(wide_range, l1_iterator))) {
    FLUSH_LOG(WARN, "failed to build l1 iterator", K(ret), K(wide_range));
  } else if (FAILED(build_mt_ext_compaction(memtable_iterators, l1_iterator))) {
    FLUSH_LOG(WARN, "failed to build mt_ext_compaction", K(ret), KP(l1_iterator));
  } else {
    se_assert(nullptr != compaction_);
  }

  FREE_OBJECT(RangeIterator, arena_, l1_iterator);
  return ret;
}

InternalIterator *FlushJob::create_memtable_iterator(const ReadOptions &read_options, MemTable *memtable)
{
  se_assert(nullptr != memtable);
  return memtable->NewIterator(read_options, &tmp_arena_);
}

int FlushJob::create_meta_iterator(const LayerPosition &layer_position,
                                   table::InternalIterator *&iterator)
{
  int ret = Status::kOk;
  ExtentLayer *layer = nullptr;
  ExtentLayerIterator *layer_iterator = nullptr;
  iterator = nullptr;

  if (IS_NULL(layer = meta_snapshot_->get_extent_layer(layer_position))) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "extent layer should not nullptr", K(ret), K(layer_position));
  } else if (IS_NULL(layer_iterator = ALLOC_OBJECT(ExtentLayerIterator, arena_))) {
    ret = Status::kMemoryLimit;
    FLUSH_LOG(WARN, "alloc memory for layer_iterator failed", K(ret), K(sizeof(ExtentLayerIterator)));
  } else if (FAILED(layer_iterator->init(compaction_context_.internal_comparator_,
                                         layer_position,
                                         layer))) {
    FLUSH_LOG(WARN, "fail to init layer_iterator", K(ret), K(layer_position));
  } else {
    iterator = layer_iterator;
  }
  return ret;
}

int FlushJob::parse_meta(const table::InternalIterator *iter, ExtentMeta *&extent_meta)
{
  int ret = Status::kOk;
  Slice key_buf;
  int64_t pos = 0;
  if (nullptr == iter) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "invalid argument", K(ret), KP(iter));
  } else {
    key_buf = iter->key();
    extent_meta = reinterpret_cast<ExtentMeta*>(const_cast<char*>(key_buf.data()));
  }
  return ret;
}

int FlushJob::build_l1_iterator(storage::Range &wide_range,
    storage::RangeIterator *&iterator) {
  int ret = 0;
  table::InternalIterator *meta_iter = nullptr;
  ExtentMeta *extent_meta = nullptr;
  ExtentLayer *level1_extent_layer = nullptr;
  ExtentLayerVersion *level1_version = nullptr;
  LayerPosition layer_position(1, 0);
  if (IS_NULL(meta_snapshot_)) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "meta snapshot is not inited", K(ret));
  } else if (FAILED(create_meta_iterator(layer_position, meta_iter))) {
    FLUSH_LOG(WARN, "fail to create meta iterator", K(ret), KP(level1_extent_layer));
  } else if (IS_NULL(meta_iter)) {
    ret = Status::kMemoryLimit;
    FLUSH_LOG(WARN, "create meta iter failed", K(ret));
  } else {  // get l1's range
    Slice smallest_key;
    Slice largest_key;
    meta_iter->SeekToLast();
    if (!meta_iter->Valid()) {
      // do nothing, no data
      return ret;
    } else if (FAILED(parse_meta(meta_iter, extent_meta))) {
      FLUSH_LOG(WARN, "parse meta failed", K(ret));
    } else if (IS_NULL(extent_meta)) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "extent meta must not nullptr", K(ret));
    } else {
      largest_key = extent_meta->largest_key_.Encode().deep_copy(arena_);
      wide_range.end_key_ = largest_key;
      int64_t level1_extents = 0;
      meta_iter->SeekToFirst();
      if (!meta_iter->Valid()) {
        // do nothing, no data
        return ret;
      } else if (FAILED(parse_meta(meta_iter, extent_meta))) {
        FLUSH_LOG(WARN, "parse meta kv failed", K(ret));
      } else if (IS_NULL(extent_meta)) {
        ret = Status::kErrorUnexpected;
        FLUSH_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
      } else {
        smallest_key = extent_meta->smallest_key_.Encode().deep_copy(arena_);
        wide_range.start_key_ = smallest_key;
      }
    }

    if (SUCCED(ret)) {
      storage::MetaType type(MetaType::SSTable, MetaType::Extent,
                             MetaType::InternalKey, 1, 1, 0);
      iterator = ALLOC_OBJECT(MetaDataIterator, arena_, type, meta_iter,
          wide_range, *compaction_context_.data_comparator_);
    }
  }
  return ret;
}

int FlushJob::get_memtable_range(
    autovector<table::InternalIterator*> &memtables, storage::Range &wide_range) {
  int ret = 0;
  Slice smallest_key;
  Slice largest_key;
  int64_t way_size = (int64_t)memtables.size();
  db::InternalKeyComparator comparator(compaction_context_.data_comparator_);
  for (int64_t i = 0; i < way_size && SUCCED(ret); ++i) {
    InternalIterator *cur_iter = memtables.at(i);
    if (IS_NULL(cur_iter)) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "cur_iter is null", K(ret));
    } else {
      cur_iter->SeekToFirst();
      if (cur_iter->Valid()) { // set smallest_key
        if (0 == smallest_key.size()
            || comparator.Compare(cur_iter->key(), smallest_key) < 0) {
          smallest_key = cur_iter->key().deep_copy(arena_);
        }
      }
      cur_iter->SeekToLast();
      if (cur_iter->Valid()) { // set largest_key
        if (0 == largest_key.size()
            || comparator.Compare(cur_iter->key(), largest_key) > 0) {
          largest_key = cur_iter->key().deep_copy(arena_);
        }
      }
    }
  }
  if (SUCCED(ret)) {
    wide_range.start_key_ = smallest_key;
    wide_range.end_key_ = largest_key;
    FLUSH_LOG(INFO, "get memtable range succ!", K(smallest_key), K(largest_key), K(way_size));
  }
  return ret;
}

int FlushJob::build_mt_ext_compaction(
    autovector<table::InternalIterator*> &memtables,
    RangeIterator *l1_iterator) {
  int ret = 0;
  MetaDescriptorList extents;
  // if level1 has no data, l1_iterator will be null
  if (nullptr != l1_iterator) {
    l1_iterator->seek_to_first();
    while (l1_iterator->valid()) {
      MetaDescriptor md = l1_iterator->get_meta_descriptor().deep_copy(arena_);
      extents.push_back(md);
      l1_iterator->next();
      if (l1_iterator->valid()) { // middle
        l1_iterator->next();
      }
    }
  }
  storage::ColumnFamilyDesc cf_desc(cfd_->GetID(), cfd_->get_table_schema());
  compaction_ = ALLOC_OBJECT(MtExtCompaction, arena_, compaction_context_, cf_desc, arena_);
  MemTable *last_mem = mems_.back();
  if (IS_NULL(compaction_)) {
    ret = Status::kMemoryLimit;
    FLUSH_LOG(WARN, "failed to alloc memory for compaction", K(ret));
  } else if (FAILED(compaction_->add_mem_iterators(memtables))) {
    FLUSH_LOG(WARN, "failed to add memtable iterators", K(ret));
  } else if (IS_NULL(last_mem)) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "last mem is null", K(ret));
  } else {
    if (0 == extents.size()) {
      // level1 has no data
    } else if (FAILED(compaction_->add_merge_batch(extents, 0, extents.size()))) {
      FLUSH_LOG(WARN, "failed to add merge batch", K(ret), K(extents.size()));
    } else {
      FLUSH_LOG(INFO, "build mt_ext_task success!", K(extents.size()), K(memtables.size()));
    }
  }
  return ret;
}


}
}  // namespace smartengine
