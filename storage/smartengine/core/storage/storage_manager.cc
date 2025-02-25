/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "storage/storage_manager.h"
#include "cache/persistent_cache.h"
#include "db/db_impl.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "monitoring/query_perf_context.h"
#include "storage/extent_meta_manager.h"
#include "storage/extent_space_manager.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "table/sstable_scan_iterator.h"
#include "util/sync_point.h"

namespace smartengine
{
using namespace cache;
using namespace common;
using namespace memory;
using namespace monitor;
using namespace db;
using namespace util;
using namespace table;

namespace {

struct RecycleBackupMetaSnapshotArgs
{
  util::autovector<storage::ExtentLayerVersion *> recyle_extent_layer_versions_;

  RecycleBackupMetaSnapshotArgs(util::autovector<storage::ExtentLayerVersion *> &&recyle_extent_layer_versions) :
      recyle_extent_layer_versions_(std::move(recyle_extent_layer_versions)) {}

  ~RecycleBackupMetaSnapshotArgs() {}
};

} // namespace

namespace storage
{
StorageManager::StorageManager(const db::ColumnFamilyData *owner,
                               const util::EnvOptions &env_options,
                               const common::ImmutableCFOptions &imm_cf_options,
                               const common::MutableCFOptions &mutable_cf_options)
    : is_inited_(false),
      owner_(owner),
      env_options_(env_options),
      immutable_cfoptions_(imm_cf_options),
      mutable_cf_options_(mutable_cf_options),
      column_family_id_(0),
      bg_recycle_count_(0),
      internalkey_comparator_(nullptr),
      meta_mutex_(),
      meta_version_(1),
      current_meta_(nullptr),
      meta_snapshot_list_(),
      waiting_delete_versions_(),
      extent_stats_updated_(false),
      extent_stats_(),
      lob_extent_mgr_(nullptr)
{
}

StorageManager::~StorageManager()
{
  destroy();
}

void StorageManager::destroy()
{
  if (is_inited_) {
    if (nullptr != internalkey_comparator_) {
      MOD_DELETE_OBJECT(InternalKeyComparator, internalkey_comparator_);
      internalkey_comparator_ = nullptr;
    }

    if (nullptr != current_meta_) {
      std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
      release_meta_snapshot_unsafe(current_meta_);
      current_meta_ = nullptr;
      if (!waiting_delete_versions_.empty()) {
        for (uint32_t i = 0; i < waiting_delete_versions_.size(); ++i) {
          MOD_DELETE_OBJECT(ExtentLayerVersion, waiting_delete_versions_.at(i));
        }
      }
    }

    if (nullptr != lob_extent_mgr_) {
      MOD_DELETE_OBJECT(LargeObjectExtentMananger, lob_extent_mgr_);
      lob_extent_mgr_ = nullptr;
    }

    is_inited_ = false;
  }
}

int StorageManager::init() {
  int ret = Status::kOk;

  if (is_inited_) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "StorageManager has been inited", K(ret));
  } else if (IS_NULL(internalkey_comparator_ = MOD_NEW_OBJECT(ModId::kStorageMgr,
                                                              db::InternalKeyComparator,
                                                              immutable_cfoptions_.user_comparator))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for internal_comparator", K(ret));
  } else if (IS_NULL(lob_extent_mgr_ = MOD_NEW_OBJECT(ModId::kStorageMgr, LargeObjectExtentMananger))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for LargeObjectExtentMananger", K(ret));
  } else if (FAILED(lob_extent_mgr_->init())) {
    SE_LOG(WARN, "fail to init lob_extent_mgr_", K(ret));
  } else if (FAILED(init_extent_layer_versions(internalkey_comparator_))) {
    SE_LOG(WARN, "fail to init extent layer versions", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int StorageManager::apply(const ChangeInfo &change_info, bool for_recovery) {
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(meta_mutex_);
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (FAILED(normal_apply(change_info))) {
    SE_LOG(WARN, "fail to normal apply change info", K(ret));
  } else if (FAILED(apply_large_object(change_info))) {
    SE_LOG(WARN, "fail to apply large object", K(ret));
  } else if (FAILED(recycle_unsafe(for_recovery))) {
    SE_LOG(WARN, "fail to recycle", K(ret));
  }

  return ret;
}

int StorageManager::get(const common::Slice &key,
                          const db::Snapshot &current_meta,
                          std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)> save_value)
{
  int ret = Status::kOk;
  bool found = false;
  ExtentLayerVersion *extent_layer_version = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (UNLIKELY(key.size() < 8)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else {
    for (int64_t level = 0; SUCCED(ret) && !found && level < MAX_TIER_COUNT; ++level) {
      if (IS_NULL(extent_layer_version = current_meta.get_extent_layer_version(level))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(level), K(key));
      } else if (FAILED(extent_layer_version->get(key, save_value, found))) {
        if (Status::kNotFound != ret) {
          SE_LOG(WARN, "fail to get from extent layer version", K(ret), K(level), K(key));
        }
      }
      SE_LOG(DEBUG, "ExtentLayerVersion for get", K(level), KP(this), KP(extent_layer_version));
    }
  }

  if (SUCCED(ret) && !found) {
    ret = Status::kNotFound;
  }
  return ret;
}

int StorageManager::add_iterators(db::TableCache *table_cache,
                                  db::InternalStats *internal_stats,
                                  const common::ReadOptions &read_options,
                                  table::MergeIteratorBuilder *merge_iter_builder,
                                  const db::Snapshot *current_meta)
{
  int ret = Status::kOk;
  ExtentLayer *extent_layer = nullptr;
  ExtentLayerVersion *extent_layer_version = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (UNLIKELY(nullptr == table_cache || nullptr == merge_iter_builder || nullptr == current_meta)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(table_cache), KP(merge_iter_builder), KP(current_meta));
  } else {
    for (int64_t level = 0; Status::kOk == ret && level < storage::MAX_TIER_COUNT; ++level) {
      if(kOnlyL2 == read_options.read_level_ && level != 2){
          continue;
      }
      if(kExcludeL2 == read_options.read_level_ && level == 2){
          continue;
      }

      if (UNLIKELY(nullptr == (extent_layer_version = current_meta->get_extent_layer_version(level)))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(level));
      } else {
        for (int64_t i = 0; Status::kOk == ret && i < extent_layer_version->get_extent_layer_size(); ++i) {
          LayerPosition layer_position(level, i);
          if (UNLIKELY(nullptr == (extent_layer = extent_layer_version->get_extent_layer(i)))) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN, "unexpected error, extent layer must not nullptr", K(ret), K(level), K(i));
          } else if (FAILED(add_iterator_for_layer(layer_position,
                                                   table_cache,
                                                   internal_stats,
                                                   read_options,
                                                   merge_iter_builder,
                                                   extent_layer))) {
            SE_LOG(WARN, "fail to add iterator for layer", K(ret), K(level), K(i));
          }
        }
      }
    }
  }

  return ret;
}

int64_t StorageManager::get_current_total_extent_count() const
{
  std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
  return current_meta_->get_total_extent_count();
}
table::InternalIterator *StorageManager::get_single_level_iterator(const common::ReadOptions &read_options,
                                                                   util::Arena *arena,
                                                                   const db::Snapshot *current_meta,
                                                                   int64_t level) const
{
  int ret = Status::kOk;
  ExtentLayerVersion *extent_layer_version = nullptr;
  ExtentLayer *extent_layer = nullptr;
  table::InternalIterator *layer_iterator = nullptr;
  table::InternalIterator *level_iterater = nullptr;
  table::MergeIteratorBuilder merge_iter_builder(internalkey_comparator_, arena);

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(arena) || IS_NULL(current_meta) || level < 0 || level > 2) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(arena), KP(current_meta), K(level));
  } else if (IS_NULL(extent_layer_version = current_meta->get_extent_layer_version(level))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(level));
  } else {
    for(int64_t i = 0; Status::kOk == ret && i < extent_layer_version->get_extent_layer_size(); ++i) {
      LayerPosition layer_position(level, i);
      if (FAILED(create_extent_layer_iterator(arena, current_meta, layer_position, layer_iterator))) {
        SE_LOG(WARN, "fail to create extent layer iterator", K(ret), K(level), K(i));
      } else {
        merge_iter_builder.AddIterator(layer_iterator);
      }
    }
  }

  if (Status::kOk == ret) {
    level_iterater = merge_iter_builder.Finish();
  }

  return level_iterater;
}

int StorageManager::get_extent_layer_iterator(
    util::Arena *arena, const db::Snapshot *snapshot,
    const LayerPosition &layer_position,
    table::InternalIterator *&iterator) const
{
  return create_extent_layer_iterator(arena, snapshot, layer_position,
                                      iterator);
}

std::pair<uint64_t, uint64_t> StorageManager::get_level_fragmentation_rate_and_delete_percent(const Snapshot *current_meta, int32_t level) const {
  uint64_t fragmentation_rate = 0;
  uint64_t delete_percent = 0;
  if (level < 0 || level >= storage::MAX_TIER_COUNT) {
    SE_LOG(WARN, "level is invalid", K(level));
    return std::make_pair(fragmentation_rate, delete_percent);
  }
  if (IS_NULL(current_meta)) {
    SE_LOG(WARN, "unexpected error, current meta must be non-nullptr", KP_(current_meta));
    return std::make_pair(fragmentation_rate, delete_percent);
  }
  auto extent_stat = current_meta->get_extent_layer_version(level)->get_extent_stats();
  fragmentation_rate = ((extent_stat.disk_size_ == 0) ? 0 : 100 - extent_stat.data_size_ * 100.0 / extent_stat.disk_size_);
  delete_percent = ((extent_stat.num_entries_ == 0) ? 0 : extent_stat.num_deletes_ * 100.0 / extent_stat.num_entries_);
  return std::make_pair(fragmentation_rate, delete_percent);
}

int64_t StorageManager::approximate_size(const db::ColumnFamilyData *cfd,
                                         const Slice &start, const Slice &end,
                                         int start_level, int end_level,
                                         const db::Snapshot *sn,
                                         int64_t estimate_cost_depth)
{
  int ret = Status::kOk;
  int64_t result_size = 0;
  util::Arena arena;
  ExtentLayerVersion *extent_layer_version = nullptr;
  ExtentLayer *extent_layer = nullptr;
  table::InternalIterator *layer_iterator = nullptr;
  EstimateCostStats cost_stats;

#ifndef NDEBUG
  SE_LOG(INFO, "begin to approximate size", "subtable_id", cfd->GetID(),
      K(start_level), K(end_level), K(estimate_cost_depth));
#endif
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(cfd) || start_level > end_level || start_level < 0 || start_level > storage::MAX_TIER_COUNT || end_level < 0 || end_level > storage::MAX_TIER_COUNT) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(cfd), K(start_level), K(end_level));
  } else {
    for (int64_t level = start_level; Status::kOk == ret && level < end_level; ++level) {
      if (IS_NULL(extent_layer_version = sn->get_extent_layer_version(level))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(level));
      } else {
        for (int64_t i = 0; Status::kOk == ret && i < extent_layer_version->get_extent_layer_size(); ++i) {
          LayerPosition layer_position(level, i);
          if (FAILED(create_extent_layer_iterator(&arena, sn, layer_position, layer_iterator))) {
            SE_LOG(WARN, "fail to create extent layer iterator", K(ret), K(layer_position));
          } else {
            result_size += one_layer_approximate_size(cfd, start, end, level, layer_iterator, estimate_cost_depth, cost_stats);
            layer_iterator->~InternalIterator();
          }
        }
      }
    }
  }
#ifndef NDEBUG
  SE_LOG(INFO, "end to approximate size", "subtable_id", cfd->GetID(),
      K(start_level), K(end_level), K(estimate_cost_depth), K(cost_stats));
#endif 

  return result_size;
}

int StorageManager::approximate_key_offet(const Slice &key,
                                          const ColumnFamilyData *cfd,
                                          const ExtentId &extent_id,
                                          const int32_t level,
                                          int64_t &offset)
{
  int ret = Status::kOk;
  Cache::Handle *extent_reader_handle = nullptr;
  ExtentReader *extent_reader = nullptr;

  if (FAILED(cfd->table_cache()->find_extent_reader(cfd->internal_comparator(),
                                                    extent_id,
                                                    false /*no_io*/,
                                                    true /*skip_filters*/,
                                                    level,
                                                    false /*prefetch_index_and_filter_in_cache*/,
                                                    &extent_reader_handle))) {
    SE_LOG(WARN, "fail to find extent reader", K(ret), K(extent_id));
  } else if (IS_NULL(extent_reader = cfd->table_cache()->get_extent_reader_from_handle(extent_reader_handle))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the extent reader must not be nullptr", K(ret), K(extent_id));
  } else if (FAILED(extent_reader->approximate_key_offet(key, offset))) {
    SE_LOG(WARN, "fail to approximate key offset", K(ret));
  } else {
    // succeed
  }

  if (IS_NOTNULL(extent_reader_handle)) {
    cfd->table_cache()->release_handle(extent_reader_handle);
    extent_reader_handle = nullptr;
  }

  return ret;
}

void StorageManager::print_raw_meta()
{
  std::lock_guard<std::mutex> guard(meta_mutex_);
  print_raw_meta_unsafe();
}

void StorageManager::print_raw_meta_unsafe()
{
  int ret = Status::kOk;
  ExtentLayerVersion *current_extent_layer_version = nullptr;
  ExtentLayer *current_extent_layer = nullptr;
  ExtentMeta *extent_meta = nullptr;
  int64_t index = 0;

  fprintf(stderr, "--------------------------------------------\n");
  for (int64_t level = 0; Status::kOk == ret && level < storage::MAX_TIER_COUNT; ++level) {
    if (nullptr == (current_extent_layer_version = extent_layer_versions_[level])) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(level));
    } else {
      for (int64_t extent_layer_index = 0; Status::kOk == ret && extent_layer_index < current_extent_layer_version->get_extent_layer_size(); ++extent_layer_index) {
        if (nullptr == (current_extent_layer = current_extent_layer_version->get_extent_layer(extent_layer_index))) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, extent layer must not nullptr", K(ret), K(level), K(extent_layer_index));
        } else {
          fprintf(stderr, "----------------level = %ld, layer_index = %ld, layer_sequence = %ld, extent_count = %d-------------\n", level, extent_layer_index, current_extent_layer->sequence_number_, current_extent_layer->extent_meta_arr_.size());
          for (int64_t i = 0; Status::kOk == ret && i < current_extent_layer->extent_meta_arr_.size(); ++i) {
            db::ParsedInternalKey smallest_key("", 0, db::kTypeValue);
            db::ParsedInternalKey largest_key("", 0, db::kTypeValue);
            if (nullptr == (extent_meta = current_extent_layer->extent_meta_arr_.at(i))) {
              ret = Status::kErrorUnexpected;
              SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret), K(level), K(extent_layer_index), K(i));
            } else {
              ParseInternalKey(extent_meta->smallest_key_.Encode(), &smallest_key);
              ParseInternalKey(extent_meta->largest_key_.Encode(), &largest_key);
              fprintf(stderr, "   value[%ld]: smallest key:[%.*s], largest_key:[%.*s], extent_id[%d, %d]\n", index,
                  (int)smallest_key.user_key.size(), smallest_key.user_key.data(),
                  (int)largest_key.user_key.size(), largest_key.user_key.data(),
                  extent_meta->extent_id_.offset, extent_meta->extent_id_.file_number);

//              fprintf(stderr, "   value[%ld]: smallest key:[%s], largest_key:[%s], extent_id[%d, %d]\n", index,
//              		util::to_cstring(smallest_key.user_key), util::to_cstring(largest_key.user_key),
//                  extent_meta->extent_id_.offset, extent_meta->extent_id_.file_number);
              ++index;
            }
          }
        }
        fprintf(stderr, "-----------------------------------------------------------\n");
      }
    }
	}

}

db::SnapshotImpl *StorageManager::accquire_meta_snapshot() {
  std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
  return accquire_meta_snapshot_unsafe();
}

db::SnapshotImpl *StorageManager::accquire_backup_meta_snapshot() {
  std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
  current_meta_->backup_ref();
  return current_meta_;
}

void StorageManager::release_meta_snapshot(db::SnapshotImpl *meta_snapshot) {
  std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
  release_meta_snapshot_unsafe(meta_snapshot);
  if (waiting_delete_versions_.size() > 0) {
    RecycleArgs *args = MOD_NEW_OBJECT(ModId::kStorageMgr, RecycleArgs, this);
    util::Env::Default()->Schedule(StorageManager::async_recycle, args, Env::Priority::RECYCLE_EXTENT, nullptr);
    ++bg_recycle_count_;
  }
}

void StorageManager::release_backup_meta_snapshot(db::SnapshotImpl *meta_snapshot) {
  util::autovector<ExtentLayerVersion *> recyle_extent_layer_versions;
  destroy_backup_meta_snapshot(meta_snapshot, recyle_extent_layer_versions);
  if (recyle_extent_layer_versions.size() > 0) {
    RecycleBackupMetaSnapshotArgs *args = MOD_NEW_OBJECT(ModId::kStorageMgr,
                                                         RecycleBackupMetaSnapshotArgs,
                                                         std::move(recyle_extent_layer_versions));
    util::Env::Default()->Schedule(StorageManager::async_recycle_meta_snapshot, args,
                                   Env::Priority::RECYCLE_EXTENT, nullptr);
  }
}

int StorageManager::cleanup_backup_snapshot(MetaSnapshotSet &meta_snapshots)
{
  int ret = Status::kOk;
  SnapshotImpl *meta_snapshot = nullptr;

  for (auto sn : meta_snapshots) {
    if (IS_NULL(meta_snapshot = dynamic_cast<SnapshotImpl *>(sn))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(ERROR, "The snapshot is nullptr", K(ret));
      break;
    } else {
      StorageManager::release_backup_meta_snapshot(meta_snapshot);
    }
  }
  meta_snapshots.clear();
  return ret;
}

int StorageManager::recycle_backup_snapshot(MetaSnapshotSet &meta_snapshots)
{
  int ret = Status::kOk;
  SnapshotImpl *meta_snapshot = nullptr;
  for (auto &sn : meta_snapshots) {
    if (IS_NULL(meta_snapshot = dynamic_cast<SnapshotImpl *>(sn))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(ERROR, "The snapshot is nullptr, unexpected");
      break;
    } else {
      util::autovector<ExtentLayerVersion *> recyle_extent_layer_versions;
      StorageManager::destroy_backup_meta_snapshot(meta_snapshot, recyle_extent_layer_versions);
      if (recyle_extent_layer_versions.size() > 0) {
        if (FAILED(StorageManager::recycle_meta_snapshot(true, recyle_extent_layer_versions))) {
          SE_LOG(ERROR, "fail to recycle meta snapshot");
          break;
        }
      }
    }
  }
  return ret;
}

void StorageManager::async_recycle_meta_snapshot(void *args) {
  RecycleBackupMetaSnapshotArgs *recycle_args = reinterpret_cast<RecycleBackupMetaSnapshotArgs *>(args);
  recycle_meta_snapshot(false, recycle_args->recyle_extent_layer_versions_);
  MOD_DELETE_OBJECT(RecycleBackupMetaSnapshotArgs, recycle_args);
}

void StorageManager::async_recycle(void *args)
{
#ifndef NDEBUG
  TEST_SYNC_POINT("StorageManager::TEST_inject_async_recycle_hang");
#endif
  RecycleArgs *recycle_args = reinterpret_cast<RecycleArgs *>(args);
  recycle_args->storage_manager_->recycle();
  MOD_DELETE_OBJECT(RecycleArgs, recycle_args);
}

int StorageManager::recycle()
{
  std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
  --bg_recycle_count_;
  return recycle_unsafe(false /*recovery*/);
}

int StorageManager::serialize(char *buf, int64_t buf_length, int64_t &pos) const
{
  int ret = Status::kOk;
  int64_t size = get_serialize_size();
  int64_t version = STORAGE_MANAGER_VERSION;
  ExtentLayerVersion *extent_layer_version = nullptr;
  int64_t max_level = storage::MAX_TIER_COUNT;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(buf) || buf_length < 0 || pos >= buf_length) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_length), K(pos));
  } else {
    *((int64_t *)(buf + pos)) = size;
    pos += sizeof(size);
    *((int64_t *)(buf + pos)) = version;
    pos += sizeof(version);
    if (FAILED(util::serialize(buf, buf_length, pos, max_level))) {
      SE_LOG(WARN, "fail to serialize MAX_LEVEL", K(ret));
    } else {
      for (int64_t level = 0; SUCCED(ret) && level < max_level; ++level) {
        if (IS_NULL(extent_layer_version = extent_layer_versions_[level])) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(level));
        } else if (FAILED(extent_layer_version->serialize(buf, buf_length, pos))) {
          SE_LOG(WARN, "fail to serialize ExtentLayerVersion", K(ret), K(level));
        }
      }

      if (SUCCED(ret)) {
        if (FAILED(lob_extent_mgr_->serialize(buf, buf_length, pos))) {
          SE_LOG(WARN, "fail to serialize lob_extent_mgr_", K(ret));
        }
      }
    }
  }
  return ret;
}

int StorageManager::deserialize(const char *buf, int64_t buf_length, int64_t &pos)
{
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t version = 0;
  int64_t max_level = 0;
  int64_t current_level = 0;
  int64_t level_extent_layer_count = 0;
  ExtentLayerVersion *current_extent_layer_version = nullptr;
  ExtentLayer *current_extent_layer = nullptr;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(buf) || buf_length < 0 || pos >= buf_length) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_length), K(pos));
  } else {
    size = *((int64_t *)(buf + pos));
    pos += sizeof(size);
    version = *((int64_t *)(buf + pos));
    pos += sizeof(version);
    if (FAILED(util::deserialize(buf, buf_length, pos, max_level))) {
      SE_LOG(WARN, "fail to deserialize max level", K(ret));
    } else {
      for (int64_t level = 0; SUCCED(ret) && level < max_level; ++level) {
        if (IS_NULL(current_extent_layer_version = extent_layer_versions_[level])) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(current_level));
        } else if (FAILED(current_extent_layer_version->deserialize(buf, buf_length, pos))) {
          SE_LOG(WARN, "fail to deserialize ExtentLayerVersion", K(ret), K(level));
        }
      }

      if (SUCCED(ret)) {
        if (FAILED(lob_extent_mgr_->deserialize(buf, buf_length, pos))) {
          SE_LOG(WARN, "fail to deserialize lob_extent_mgr_", K(ret));
        }
      }
    }
  }

  if (SUCCED(ret)) {
    if (FAILED(update_current_meta_snapshot(extent_layer_versions_))) {
      SE_LOG(WARN, "fail to update current meta snapshot", K(ret));
    }
  }
  return ret;
}

int StorageManager::deserialize_and_dump(const char *buf, int64_t buf_len, int64_t &pos,
                                         char *str_buf, int64_t str_buf_len, int64_t &str_pos)
{
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t version = 0;
  int64_t max_level = 0;
  int64_t current_level = 0;
  int64_t level_extent_layer_count = 0;
  int64_t dump_layer_count = 0;
  common::SequenceNumber sequence_number = 0;
  int64_t dump_extent_layer_count = 0;
  util::autovector<ExtentId> extent_ids;
  util::autovector<ExtentId> lob_extent_ids;
  LargeObjectExtentMananger lob_mgr;

  if (IS_NULL(buf) || buf_len < 0 || pos >= buf_len
      || IS_NULL(str_buf) || str_buf_len < 0 || str_pos >= str_buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos), KP(str_buf), K(str_buf_len), K(str_pos));
  } else {
    size = *((int64_t *)(buf + pos));
    pos += sizeof(size);
    version = *((int64_t *)(buf + pos));
    pos += sizeof(version);
    if (FAILED(util::deserialize(buf, buf_len, pos, max_level))) {
      SE_LOG(WARN, "fail to deserialize max level", K(ret));
    } else {
      for (int64_t level = 0; SUCCED(ret) && level < max_level; ++level) {
        pos += 2 * sizeof(int64_t); //size and version
        if (FAILED(util::deserialize(buf, buf_len, pos, current_level))) {
          SE_LOG(WARN, "fail to deserialize current level", K(ret));
        } else if (FAILED(util::deserialize(buf, buf_len, pos, level_extent_layer_count))) {
          SE_LOG(WARN, "fail to deserialize level_extent_layer_count", K(ret));
        } else {
          util::databuff_printf(str_buf, str_buf_len, str_pos, "{");
          util::databuff_print_json_wrapped_kv(str_buf, str_buf_len, str_pos, "level", current_level);
          util::databuff_print_json_wrapped_kv(str_buf, str_buf_len, str_pos, "extent_layer_count", level_extent_layer_count);

          for (int64_t extent_layer_index = 0;
               SUCCED(ret) && extent_layer_index < level_extent_layer_count;
               ++extent_layer_index) {
            util::databuff_printf(str_buf, str_buf_len, str_pos, "{");
            util::databuff_print_json_wrapped_kv(str_buf, str_buf_len, str_pos, "layer_index", extent_layer_index);
            if (FAILED(deserialize_extent_layer(buf, buf_len, pos,
                    sequence_number, extent_ids, lob_extent_ids))) {
            } else {
              for (uint32_t i = 0; i < extent_ids.size(); ++i) {
                util::databuff_print_json_wrapped_kv(str_buf, str_buf_len, str_pos, "extent_id", extent_ids.at(i));
              }
              for (uint32_t i = 0; i < lob_extent_ids.size(); ++i) {
                util::databuff_print_json_wrapped_kv(str_buf, str_buf_len, str_pos, "lob_extent_id", lob_extent_ids.at(i));
              }
            }
            util::databuff_printf(str_buf, str_buf_len, str_pos, "}");
          }

          if (SUCCED(ret)) {
            util::databuff_printf(str_buf, str_buf_len, str_pos, "{");
            if (FAILED(util::deserialize(buf, buf_len, pos, dump_extent_layer_count))) {
              SE_LOG(WARN, "fail to deserialize dump extent layer count", K(ret));
            } else {
              util::databuff_print_json_wrapped_kv(str_buf, str_buf_len, str_pos, "dump_extent_layer_count", dump_extent_layer_count);
              if (1 == dump_extent_layer_count) {
                if (FAILED(deserialize_extent_layer(buf, buf_len, pos,
                        sequence_number, extent_ids, lob_extent_ids))) {
                  SE_LOG(WARN, "fail to deserialize dump extent layer", K(ret));
                } else {
                  for (uint32_t i = 0; i < extent_ids.size(); ++i) {
                    util::databuff_print_json_wrapped_kv(str_buf, str_buf_len, str_pos, "extent_id", extent_ids.at(i));
                  }
                  for (uint32_t i = 0; i < lob_extent_ids.size(); ++i) {
                    util::databuff_print_json_wrapped_kv(str_buf, str_buf_len, str_pos, "lob_extent_id", lob_extent_ids.at(i));
                  }
                }
              }
            }
            util::databuff_printf(str_buf, str_buf_len, str_pos, "}");
          }
          util::databuff_printf(str_buf, str_buf_len, str_pos, "}");
        }
      }

      if (SUCCED(ret)) {
        if (FAILED(lob_mgr.deserialize_and_dump(buf, buf_len, pos, str_buf, str_buf_len, str_pos))) {
          SE_LOG(WARN, "fail to deserialize and dump lob manager", K(ret));
        }
      }
    }
  }

  return ret;
}

int StorageManager::get_extent_positions(const int64_t index_id, ExtentPositionMap &extent_positions)
{
  int ret = Status::kOk;
  ExtentLayerVersion *extent_layer_version = nullptr;

  std::lock_guard<std::mutex> guard(meta_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(current_meta_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, current_meta_snapshot is nullptr", K(ret));
  } else {
     for (int64_t level = 0; SUCCED(ret) && level < storage::MAX_TIER_COUNT; ++level) {
       if (IS_NULL(extent_layer_version = current_meta_->get_extent_layer_version(level))) {
         ret = Status::kErrorUnexpected;
         SE_LOG(WARN, "unexpected error, extent layer version should not been nullptr", K(ret), K(level));
       } else if (FAILED(extent_layer_version->get_extent_positions(index_id, extent_positions))) {
         SE_LOG(WARN, "fail to get extent infos", K(ret), K(index_id), K(level));
       }
     }
  }

  return ret;
}

int64_t StorageManager::get_serialize_size() const
{
  int64_t size = 0;
  int64_t max_level = storage::MAX_TIER_COUNT;
  ExtentLayerVersion *extent_layer_version = nullptr;

  size += 2 * sizeof(int64_t); //size and version
  size += util::get_serialize_size(max_level);
  for (int64_t level = 0; level < max_level; ++level) {
    size += extent_layer_versions_[level]->get_serialize_size();
  }
  size += lob_extent_mgr_->get_serialize_size();

  return size;
}

int64_t StorageManager::to_string(char *buf, const int64_t buf_len) const
{
  int ret = Status::kOk;
  int64_t pos = 0;
  ExtentLayerVersion *extent_layer_version = nullptr;
  ExtentLayer *extent_layer = nullptr;
  util::autovector<ExtentId> extent_ids;
  int64_t level_extent_layer_count = 0;
  util::databuff_printf(buf, buf_len, pos, "{");

  for (int64_t level = 0; SUCCED(ret) && level < storage::MAX_TIER_COUNT; ++level) {
    fprintf(stderr, "--------------------level = %ld--------------------------\n", level);
    if (nullptr == (extent_layer_version = extent_layer_versions_[level])) {
      ret = Status::kErrorUnexpected;
      fprintf(stderr, "ExtentLayerVersion must not nullptr. %d, %ld\n", ret, level);
    } else {
      level_extent_layer_count = extent_layer_version->get_extent_layer_size();
      for (int64_t layer_index = 0; SUCCED(ret) && layer_index < level_extent_layer_count; ++layer_index) {
        fprintf(stderr, "----------------layer_index = %ld-------------------\n", layer_index);
        extent_ids.clear();
        if (nullptr == (extent_layer = extent_layer_version->get_extent_layer(layer_index))) {
          ret = Status::kErrorUnexpected;
          fprintf(stderr, "Extent layer must not nullptr. %d, %ld\n", ret, layer_index);
        } else if (FAILED(extent_layer->get_all_extent_ids(extent_ids))) {
          fprintf(stderr, "fail to get all extent ids. %d\n", ret);
        } else {
          for (uint32_t i = 0; i < extent_ids.size(); ++i) {
            pos += extent_ids.at(i).to_string(buf + pos, buf_len - pos);
          }
        }
      }
    }
  }
  util::databuff_printf(buf, buf_len, pos, "}");
  
  return pos;
}
int StorageManager::init_extent_layer_versions(db::InternalKeyComparator *internalkey_comparator)
{
  int ret = Status::kOk;
  ExtentLayer *extent_layer = nullptr;
  ExtentLayerVersion *extent_layer_version = nullptr;

  for (int32_t level = 0; SUCCED(ret) && level < MAX_TIER_COUNT; ++level) {
    if (IS_NULL(extent_layer = MOD_NEW_OBJECT(ModId::kStorageMgr, ExtentLayer, internalkey_comparator))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for ExtentLayer", K(ret), K(level));
    } else if (IS_NULL(extent_layer_version = MOD_NEW_OBJECT(ModId::kStorageMgr, ExtentLayerVersion,
            level, internalkey_comparator))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for ExtentLayerVersion", K(ret), K(level));
    } else if (FAILED(extent_layer_version->add_layer(extent_layer))) {
      SE_LOG(WARN, "fail to add layer", K(ret), K(level));
    } else {
      extent_layer_versions_[level] = extent_layer_version;
    }
  }

  if (SUCCED(ret)) {
    if (FAILED(update_current_meta_snapshot(extent_layer_versions_))) {
      SE_LOG(WARN, "fail to update current meta snapshot", K(ret));
    }
  }

  return ret;
}

int StorageManager::normal_apply(const ChangeInfo &change_info) {
  int ret = Status::kOk;
  ExtentLayerVersion *new_extent_layer_versions[MAX_TIER_COUNT] = {nullptr};
  int64_t level = 0;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else {
    for (auto iter = change_info.extent_change_info_.begin(); SUCCED(ret) && change_info.extent_change_info_.end() != iter; ++iter) {
      level = iter->first;
      if (level < 0 || level >= MAX_TIER_COUNT) {
        ret = Status::kInvalidArgument;
        SE_LOG(WARN, "invalid argument", K(ret), K(level));
      } else if (IS_NULL(new_extent_layer_versions[level] = MOD_NEW_OBJECT(ModId::kStorageMgr,
                                                                           ExtentLayerVersion,
                                                                           level,
                                                                           internalkey_comparator_))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for ExtentLayerVersion", K(ret), K(level));
      } else if (FAILED(build_new_version(extent_layer_versions_[level],
                                          iter->second,
                                          change_info.lob_extent_change_info_,
                                          new_extent_layer_versions[level]))) {
        SE_LOG(WARN, "fail to build new version", K(ret), K(level));
      } else {
        SE_LOG(INFO, "success to build new version", K(level));
      }
    }

    if (SUCCED(ret)) {
      if (FAILED(update_current_meta_snapshot(new_extent_layer_versions))) {
        SE_LOG(WARN, "fail to update current meta snapshot", K(ret));
      }
    }
  }

  return ret;
}

int StorageManager::apply_large_object(const ChangeInfo &change_info)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (FAILED(lob_extent_mgr_->apply(change_info, meta_version_))) {
    SE_LOG(WARN, "fail to apply change info to LargeObjectExtentMananger", K(ret), K_(meta_version));
  } else {
    SE_LOG(INFO, "success to apply change info to LargeObjectExtentMananger", K_(meta_version));
  }

  return ret;
}

int StorageManager::build_new_version(ExtentLayerVersion *old_version,
                                      const ExtentChangeArray &extent_changes,
                                      const ExtentChangeArray &lob_extent_change,
                                      ExtentLayerVersion *&new_version)
{
  int ret = Status::kOk;
  ExtentChangesMap extent_changes_per_layer;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(old_version) || IS_NULL(new_version)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(old_version), KP(new_version));
  } else if (FAILED(prepare_build_new_version(extent_changes, extent_changes_per_layer))) {
    SE_LOG(WARN, "fail to prepare build new version", K(ret));
  } else if (FAILED(actual_build_new_version(old_version, extent_changes_per_layer, lob_extent_change, new_version))){
    SE_LOG(WARN, "fail to actual build new version", K(ret));
  }

  return ret;
}

int StorageManager::prepare_build_new_version(const ExtentChangeArray &extent_changes,
                                              ExtentChangesMap &extent_changes_per_layer)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < extent_changes.size(); ++i) {
      const ExtentChange &extent_change = extent_changes.at(i);
      auto iter = extent_changes_per_layer.find(extent_change.layer_position_.layer_index_);
      if (extent_changes_per_layer.end() != iter) {
        iter->second.push_back(extent_change);
      } else {
        std::vector<ExtentChange> extent_change_vec;
        extent_change_vec.push_back(extent_change);
        if (!(extent_changes_per_layer.emplace(extent_change.layer_position_.layer_index_, extent_change_vec).second)) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, fail to push bach extent change vector", K(ret), K(extent_change));
        }
      }
    }
  }

  return ret;
}

int StorageManager::actual_build_new_version(const ExtentLayerVersion *old_version,
                                             const ExtentChangesMap &extent_changes_per_layer,
                                             const ExtentChangeArray &lob_extent_change,
                                             ExtentLayerVersion *&new_version)
{
  int ret = Status::kOk;
  ExtentLayer *old_extent_layer = nullptr;
  ExtentLayer *new_extent_layer = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(old_version) || IS_NULL(new_version)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(old_version), KP(new_version));
  } else if (FAILED(copy_on_write_accord_old_version(old_version, extent_changes_per_layer, lob_extent_change, new_version))) {
    SE_LOG(WARN, "fail to build extent layers accord to old version", K(ret));
  } else if (FAILED(build_new_extent_layer_if_need(extent_changes_per_layer, new_version))) {
    SE_LOG(WARN, "fail to build new extent layer if need", K(ret));
  }

  return ret;
}

int StorageManager::copy_on_write_accord_old_version(const ExtentLayerVersion *old_version,
                                                     const ExtentChangesMap &extent_changes_per_layer,
                                                     const ExtentChangeArray &lob_extent_change,
                                                     ExtentLayerVersion *&new_version)
{
  int ret = Status::kOk;
  const ExtentLayer *old_extent_layer = nullptr;
  ExtentLayer *new_extent_layer = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(old_version) || IS_NULL(new_version)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(old_version), KP(new_version));
  } else {
    for (int32_t layer_index = 0; SUCCED(ret) && layer_index < old_version->get_extent_layer_size(); ++layer_index) {
      if (IS_NULL(old_extent_layer = (const_cast<ExtentLayerVersion *>(old_version))->get_extent_layer(layer_index))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, fail to get extent layer", K(ret), K(layer_index));
      } else if (IS_NULL(new_extent_layer = MOD_NEW_OBJECT(ModId::kStorageMgr, ExtentLayer, internalkey_comparator_))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for ExtentLayer", K(ret));
      } else {
        *new_extent_layer = *old_extent_layer;
        auto iter = extent_changes_per_layer.find(layer_index);
        if (extent_changes_per_layer.end() != iter) {
          if (FAILED(modify_extent_layer(new_extent_layer, iter->second))) {
            SE_LOG(WARN, "fail to modify extent layer", K(ret));
          }
        }

        if (SUCCED(ret)) {
          if (0 == new_version->get_level() && new_extent_layer->is_empty()) {
            //empty ExtentLayer in level 0, no need to add to new version
            MOD_DELETE_OBJECT(ExtentLayer, new_extent_layer);
          } else if (FAILED(new_version->add_layer(new_extent_layer))) {
            SE_LOG(WARN, "fail to add layer to new version", K(ret), K(layer_index));
          }
        }
      }
    }

    //dump extent layer is a special layer in level 0
    if (SUCCED(ret) && (0 == new_version->get_level())) {
      if (FAILED(deal_dump_extent_layer_if_need(old_version, extent_changes_per_layer, lob_extent_change, new_version))) {
        SE_LOG(WARN, "fail to deal with dump extent layer", K(ret));
      }
    }
  }

  //resource clean
  if (FAILED(ret)) {
    if (nullptr != new_extent_layer) {
      MOD_DELETE_OBJECT(ExtentLayer, new_extent_layer);
    }
  }

  return ret;
}

int StorageManager::build_new_extent_layer_if_need(const ExtentChangesMap &extent_changes_per_layer,
                                                   ExtentLayerVersion *&new_version)
{
  int ret = Status::kOk;
  ExtentLayer *new_extent_layer = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(new_version)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(new_version));
  } else {
    auto iter = extent_changes_per_layer.find(LayerPosition::NEW_GENERATE_LAYER_INDEX);
    if (extent_changes_per_layer.end() != iter) {
      //only level 0 will generate new ExtentLayer, other level only modify base on the alreay exist ExtentLayer
      //so do check here, only as defense code. May remove it, when other level also generate new ExtentLayer in future
      if (0 != new_version->get_level()) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, only level 0 will generate new ExtentLayer", K(ret));
      } else if (IS_NULL(new_extent_layer = MOD_NEW_OBJECT(ModId::kStorageMgr, ExtentLayer, internalkey_comparator_))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for ExtentLayer", K(ret));
      } else if (FAILED(modify_extent_layer(new_extent_layer, iter->second))) {
        SE_LOG(WARN, "fail to modify extent layer", K(ret));
      } else if (new_extent_layer->is_empty()) {
        //empty extent layer, delete it
        MOD_DELETE_OBJECT(ExtentLayer, new_extent_layer);
      } else if (FAILED(new_version->add_layer(new_extent_layer))) {
        SE_LOG(WARN, "fail to add layer", K(ret));
      }
    }
  }

  //resource clean
  if (FAILED(ret)) {
    if (nullptr != new_extent_layer) {
      MOD_DELETE_OBJECT(ExtentLayer, new_extent_layer);
    }
  }

  return ret;
}

int StorageManager::deal_dump_extent_layer_if_need(const ExtentLayerVersion *old_version, 
                                                   const ExtentChangesMap &extent_changes_per_layer,
                                                   const ExtentChangeArray &lob_extent_change,
                                                   ExtentLayerVersion *&new_version)
{
  int ret = Status::kOk;
  ExtentLayer *old_dump_extent_layer = nullptr;
  ExtentLayer *new_dump_extent_layer = nullptr;
  ExtentMeta *extent_meta = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first");
  } else if (IS_NULL(new_version) || (0 != new_version->get_level())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(new_version));
  } else if (IS_NULL(new_dump_extent_layer = MOD_NEW_OBJECT(ModId::kStorageMgr, ExtentLayer, internalkey_comparator_))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for ExtentLayer", K(ret));
  } else {
    if (nullptr != (old_dump_extent_layer = ((const_cast<ExtentLayerVersion *>(old_version))->get_extent_layer(LayerPosition::INVISIBLE_LAYER_INDEX)))) {
      *new_dump_extent_layer = *old_dump_extent_layer;
      SE_LOG(INFO, "old dump extent layer will been deleted");
    }
    auto iter = extent_changes_per_layer.find(LayerPosition::INVISIBLE_LAYER_INDEX);
    if (extent_changes_per_layer.end() != iter) {
      if (FAILED(modify_extent_layer(new_dump_extent_layer, iter->second))) {
        SE_LOG(WARN, "fail to modify extent layer", K(ret));
      } else if (FAILED(record_lob_extent_info_to_dump_extent_layer(lob_extent_change, new_dump_extent_layer))) {
        SE_LOG(WARN, "fail to record lob extent info to dump extent layer", K(ret));
      }
    }

    if (SUCCED(ret)) {
      if (new_dump_extent_layer->is_empty()) {
        //empty dump extent layer, no need add to new version
        MOD_DELETE_OBJECT(ExtentLayer, new_dump_extent_layer);
      } else {
        new_version->dump_extent_layer_ = new_dump_extent_layer;
        SE_LOG(INFO, "success to generate new dump extent layer");
      }
    }
  }

  //resource clean
  if (FAILED(ret)) {
    if (nullptr != new_dump_extent_layer) {
      MOD_DELETE_OBJECT(ExtentLayer, new_dump_extent_layer);
    }
  }

  return ret;
}

int StorageManager::record_lob_extent_info_to_dump_extent_layer(const ExtentChangeArray &lob_extent_change, ExtentLayer *&dump_extent_layer)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(dump_extent_layer)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(dump_extent_layer));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < lob_extent_change.size(); ++i) {
      const ExtentChange &extent_change = lob_extent_change.at(i);
      if (IS_NULL(extent_meta = ExtentMetaManager::get_instance().get_meta(extent_change.extent_id_))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "fail to get extent meta", K(ret), K(extent_change));
      } else {
        if (extent_change.is_delete()) {
          if (FAILED(dump_extent_layer->remove_lob_extent(extent_meta))) {
            SE_LOG(WARN, "fail to remove lob extent", K(ret), K(extent_change));
          } else {
            SE_LOG(INFO, "success to remove lob extent", K(extent_change));
          }
        } else if (extent_change.is_add()) {
          if (dump_extent_layer->is_empty()) {
            SE_LOG(INFO, "this lob extent is not belong to dump layer", K(extent_change));
          } else if (FAILED(dump_extent_layer->add_lob_extent(extent_meta))) {
            SE_LOG(WARN, "fail to add lob extent", K(ret), K(extent_change));
          } else {
            SE_LOG(INFO, "success to add lob extent", K(extent_change));
          }
        } else {
          ret = Status::kNotSupported;
          SE_LOG(WARN, "unsupport extent change", K(ret), K(extent_change));
        }
      }
    }
  }

  return ret;
}
int StorageManager::modify_extent_layer(ExtentLayer *extent_layer,
                                        const ExtentChangeArray &extent_changes)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(extent_layer) || UNLIKELY(extent_changes.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent_layer), "extent_changes_size", extent_changes.size());
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < extent_changes.size(); ++i) {
      const ExtentChange &extent_change = extent_changes.at(i);
      if (IS_NULL(extent_meta = ExtentMetaManager::get_instance().get_meta(extent_change.extent_id_))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret), K(extent_change));
      } else {
        if (extent_change.is_delete()) {
          if (FAILED(extent_layer->remove_extent(extent_meta))) {
            SE_LOG(WARN, "fail to remove extent", K(ret), K(extent_change));
          } else {
            SE_LOG(INFO, "success to remove extent", K(extent_change));
          }
        } else if (extent_change.is_add()) {
          if (FAILED(extent_layer->add_extent(extent_meta, true))) {
            SE_LOG(WARN, "fail to add extent", K(ret));
          } else {
            SE_LOG(INFO, "success to add extent", K(ret), K(extent_change));
          }
        } else {
          ret = Status::kNotSupported;
          SE_LOG(WARN, "unsupport extent change type", K(ret), K(extent_change));
        }
      }
    }
  }

  return ret;
}

int StorageManager::add_iterator_for_layer(const LayerPosition &layer_position,
                                           db::TableCache *table_cache,
                                           db::InternalStats *internal_stats,
                                           const common::ReadOptions &read_options,
                                           table::MergeIteratorBuilder *merge_iter_builder,
                                           ExtentLayer *extent_layer)
{
  int ret = Status::kOk;
  util::Arena *arena = nullptr;
  ExtentLayerIterator *layer_iterator = nullptr;
  table::SSTableScanIterator *scan_iter = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (UNLIKELY(nullptr == table_cache
             || !layer_position.is_valid()
             || nullptr == merge_iter_builder
             || nullptr == extent_layer)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(table_cache),
        K(layer_position), KP(merge_iter_builder), KP(extent_layer));
  } else if (UNLIKELY(nullptr == (arena = merge_iter_builder->GetArena()))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, arena must not nullptr", K(ret), KP(arena));
  } else if (IS_NULL(scan_iter = PLACEMENT_NEW(table::SSTableScanIterator, *arena))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for scan iterator", K(ret));
  } else {
    ScanParam param;
    param.table_cache_ = table_cache;
    param.read_options_ = &read_options;
    param.env_options_ = &env_options_;
    param.icomparator_ = internalkey_comparator_;
    param.file_read_hist_ = (nullptr != internal_stats) ? internal_stats->GetFileReadHist(layer_position.level_) : nullptr;
    param.for_compaction_ = false;
    param.skip_filters_ = is_filter_skipped(layer_position.level_);
    param.layer_position_ = layer_position;
    param.subtable_id_ = column_family_id_;
    //param.internal_stats_ = subtable_->internal_stats();
    param.internal_stats_ = internal_stats;
    param.extent_layer_ = extent_layer;
    param.arena_ = arena;
    param.scan_add_blocks_limit_ = mutable_cf_options_.scan_add_blocks_limit;
    if (FAILED(scan_iter->init(param))) {
      SE_LOG(WARN, "failed to init scan iterator", K(ret));
    } else {
      merge_iter_builder->AddIterator(scan_iter);
    }
  }
  return ret;
}

int StorageManager::create_extent_layer_iterator(util::Arena *arena,
                                                 const db::Snapshot *snapshot,
                                                 const LayerPosition &layer_position,
                                                 table::InternalIterator *&iterator) const
{
  int ret = Status::kOk;
  ExtentLayerIterator *extent_layer_iterator = nullptr;
  ExtentLayer *extent_layer = nullptr;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (IS_NULL(arena) || IS_NULL(snapshot) || !layer_position.is_valid()) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(arena), KP(snapshot), K(layer_position));
  } else if (IS_NULL(extent_layer_iterator = PLACEMENT_NEW(ExtentLayerIterator, *arena))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for extent layer iterator", K(ret));
  } else if (IS_NULL(extent_layer = snapshot->get_extent_layer(layer_position))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, fail to get extent layer", K(ret), K(layer_position));
  } else if (FAILED(extent_layer_iterator->init(internalkey_comparator_,
                                                layer_position,
                                                extent_layer))) {
    SE_LOG(WARN, "fail to init extent layer iterator", K(ret), K(layer_position));
  } else {
    iterator = extent_layer_iterator;
  }

  return ret;
}

int64_t StorageManager::one_layer_approximate_size(const db::ColumnFamilyData *cfd,
                                                   const Slice &start,
                                                   const Slice &end,
                                                   int level,
                                                   table::InternalIterator *iter,
                                                   int64_t estimate_cost_depth,
                                                   EstimateCostStats &cost_stats)
{
  int ret = Status::kOk;
  /**variables for calculate cost size*/
  int64_t start_off = 0;              // extent's start offset in [start, end]
  int64_t end_off = MAX_EXTENT_SIZE;  // extent's end offset in [start, end]
  int64_t cost_size = 0;              // total cost size
  int64_t include_extent_count = 0;   // extent count in [start, end]
  int64_t last_extent_cost_size = 0;  // cost size of last extent in [start,
                                      // end]
  ExtentId last_extent_id;            // last extent in [start, end]
  bool recalc_last_extent = true;     // recalculate last extent's cost size
  bool reach_end = false;             // reach to range end

  /**variables for read extent*/
  Slice extent_meta_slice;                     // current extent meta buffer
  ExtentMeta *extent_meta = nullptr;           // current extent meta

  /**estimate calculate logical:
   * first extent must been opened to calculate start_off, middle extents not
   * opened and cost_size is supposed to MAX_EXTENT_SIZE(2M), and if end less
   * than last extent's largest_key, last extent will been opened to calculate
   * end_off. However, if end is larger than largest_key in current layer, last
   * extent's cost_size is not exactly, such as [start, end] is [min, max], and
   * last extent will not been opened ,because it's largest_key less than max,
   * but last extent may only has one record, and it's cost size is 10Byte not
   * MAX_EXTENT_SIZE.
   *
   * estimate end condition:
   * 1. found error during estimate, and ret is not Status::kOk.
   * 2. reach to current layer end, iter->Valid() is false.
   * 3. reach to estimate_cost_depth limit.*/
  // seek to first extent in current layer
  iter->Seek(start);
  while (SUCCED(ret) && iter->Valid() && !reach_end) {
    start_off = 0;
    end_off = MAX_EXTENT_SIZE;
    extent_meta_slice = iter->key();
    if (IS_NULL(extent_meta = reinterpret_cast<ExtentMeta *>(
                    const_cast<char *>(extent_meta_slice.data())))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
    } else if (0 == include_extent_count) {
      /**first extent should calculate start_off*/
      if (FAILED(approximate_key_offet(start, cfd, extent_meta->extent_id_, level, start_off))) {
        SE_LOG(WARN, "fail to approximate start offset", K(ret), K(*extent_meta));
      } else {
        ++cost_stats.total_open_extent_cnt_;
      }
    }

    if (SUCCED(ret)) {
      if (internalkey_comparator_->Compare(
              end, extent_meta->largest_key_.Encode()) < 0) {
        /**reach to range end.*/
        reach_end = true;
        /**end is smaller than current layer largest_key, no need recalculate
         * last extent*/
        recalc_last_extent = false;
        /**part of current extent will been read, so need calculate end_off
         * if this is first extent(include_extent_count == 0), TableReader has
         * been created. if not the fisrt extent, TableReader need create here*/
        if (0 == include_extent_count) {
          if (FAILED(approximate_key_offet(end, cfd, extent_meta->extent_id_, level, end_off))) {
            SE_LOG(WARN, "fail to approximate end offset", K(ret), K(*extent_meta));
          }
        } else if (FAILED(approximate_key_offet(end, cfd, extent_meta->extent_id_, level, end_off))) {
          SE_LOG(WARN, "fail to approximate end offset", K(ret), K(*extent_meta));
        } else {
          ++cost_stats.total_extent_cnt_;
        }
      }
    }

    if (SUCCED(ret)) {
      assert(end_off >= start_off);
      ++include_extent_count;
      last_extent_id = extent_meta->extent_id_;
      last_extent_cost_size = end_off - start_off;
      cost_size += last_extent_cost_size;
      iter->Next();
    }

    if (estimate_cost_depth && include_extent_count >= estimate_cost_depth) {
      break;
    }
  }

  if (include_extent_count > 0 && recalc_last_extent) {
    cost_stats.recalc_last_extent_ = true;
    if (1 == include_extent_count) {
      if (FAILED(approximate_key_offet(end, cfd, last_extent_id, level, end_off))) {
        SE_LOG(WARN, "fail to approximate end offset", K(ret), K(last_extent_id));
      } else {
        assert(cost_size >= (end_off - start_off));
        cost_size += (end_off - start_off) - last_extent_cost_size;
      }
    } else {
      start_off = 0;
      end_off = MAX_EXTENT_SIZE;
      if (FAILED(approximate_key_offet(end, cfd, last_extent_id, level, end_off))) {
        SE_LOG(WARN, "fail to approximate end offset", K(ret), K(last_extent_id));
      } else {
        assert(last_extent_cost_size >= (end_off - start_off));
        cost_size += (end_off - start_off) - last_extent_cost_size;
        ++cost_stats.total_open_extent_cnt_;
      }
    }
  }
  cost_stats.cost_size_ += cost_size;
  cost_stats.total_extent_cnt_ += include_extent_count;

  return cost_size;
}

int StorageManager::update_current_meta_snapshot(ExtentLayerVersion **new_extent_layer_versions) {
  int ret = Status::kOk;
  ExtentLayerVersion *new_extent_layer_version = nullptr;
  db::SnapshotImpl *new_current_meta = nullptr;

  if (IS_NULL(new_extent_layer_versions)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(new_extent_layer_versions));
  } else {
    for (int64_t level = 0; SUCCED(ret) && level < storage::MAX_TIER_COUNT; ++level) {
      if (nullptr != (new_extent_layer_version = new_extent_layer_versions[level])) {
        if (IS_NULL(extent_layer_versions_[level])) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(level));
        } else {
          extent_layer_versions_[level] = new_extent_layer_version;
          SE_LOG(INFO, "extent layer version", K(level), KP(this), KP(extent_layer_versions_[level]), "extent_layer_count", new_extent_layer_version->get_extent_layer_size());
        }
      }
    }

    if (SUCCED(ret)) {
      int64_t index_id = IS_NOTNULL(owner_) ? owner_->GetID() : -1;
      int64_t meta_snapshot_id = -1;

      if (IS_NULL(new_current_meta = MOD_NEW_OBJECT(ModId::kStorageMgr, db::SnapshotImpl))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for new current meta", K(ret));
      } else if (GlobalContext::get_env()->IsObjectStoreInited()) {
        if (FAILED(GlobalContext::get_env()->AllocMetasnapshotId(meta_snapshot_id).code())) {
          SE_LOG(WARN, "fail to alloc meta snapshot id", K(ret));
        }
      }

      if (SUCCED(ret)) {
        if (FAILED(new_current_meta->init(index_id, meta_snapshot_id, extent_layer_versions_, ++meta_version_))) {
          SE_LOG(WARN, "fail to init new current meta", K(ret));
        } else {
          if (nullptr != current_meta_) {
            release_meta_snapshot_unsafe(current_meta_);
          }
          current_meta_ = new_current_meta;
          accquire_meta_snapshot_unsafe();
          calc_extent_stats_unsafe();
        }
      }
    }
  }

  return ret;
}

int64_t StorageManager::level0_oldest_layer_sequence(const Snapshot *current_meta) const
{

  if (current_meta != nullptr && current_meta->get_extent_layer_version(0) != nullptr &&
      current_meta->get_extent_layer_version(0)->get_extent_layer_size() > 0) {
    int64_t layer_size =  current_meta->get_extent_layer_version(0)->get_extent_layer_size();
    ExtentLayer *oldest_layer = current_meta->get_extent_layer_version(0)->get_extent_layer(0);
    if (nullptr != oldest_layer) {
      return oldest_layer->sequence_number_;
    }
  }
  return kMaxSequenceNumber;
}

int StorageManager::get_level_delete_size(const db::Snapshot *current_meta,
                                          const int32_t level,
                                          int64_t &delete_size) const {
  int ret = Status::kOk;
  delete_size = 0;
  if (level < 1 || level > 2) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "level is invalid", K(level), K(ret));
    return ret;
  }

  Arena arena;
  ReadOptions ro;
  std::unique_ptr<table::InternalIterator, ptr_destruct<table::InternalIterator>> level_iter;
  level_iter.reset(get_single_level_iterator(ro, &arena, current_meta, level));
  if (nullptr == level_iter) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "level_iter is null", K(level), K(ret));
    return ret;
  }
  ExtentMeta* meta = nullptr;
  while (level_iter->Valid() && Status::kOk == ret) {
    int64_t pos = 0;
    Slice key_slice = level_iter->key();
    if (nullptr == (meta = reinterpret_cast<ExtentMeta *>(const_cast<char *>(key_slice.data())))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
    } else if (meta->num_deletes_ > 0) {
        ++delete_size;
    }
  }
  return ret;
}

// get extents usage percent of level1/2
int StorageManager::get_level_usage_percent(const Snapshot *current_meta,
                                            const int32_t level,
                                            int64_t &usage_percent,
                                            int64_t &delete_size) const {

  int ret = 0;
  usage_percent = 100;
  delete_size = 0;
  if (level < 1 || level > 2) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "level is invalid", K(level), K(ret));
    return ret;
  }

  Arena arena;
  ReadOptions ro;
  std::unique_ptr<table::InternalIterator, ptr_destruct<table::InternalIterator>> level_iter;
  level_iter.reset(get_single_level_iterator(ro, &arena, current_meta, level));
  if (nullptr == level_iter) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "level_iter is null", K(level), K(ret));
    return ret;
  }
  ExtentMeta* meta = nullptr;
  int64_t extent_size = 2 * 1024 * 1024;
  int64_t size = 0;
  int64_t total_usage = 0;

  level_iter->SeekToFirst();
  while (level_iter->Valid() && Status::kOk == ret) {
    int64_t pos = 0;
    Slice key_slice = level_iter->key();
    if (nullptr == (meta = reinterpret_cast<ExtentMeta *>(const_cast<char *>(key_slice.data())))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
    } else {
      if (meta->num_deletes_ > 0) {
        ++delete_size;
      }
      int64_t one_usage = meta->data_size_ + meta->index_block_handle_.get_size();
      total_usage += one_usage;
      ++size;
      level_iter->Next();
    }
  }
  if (Status::kOk == ret && size) {
    usage_percent = total_usage * 100 / (size * extent_size);
  }
  return ret;
}

db::SnapshotImpl *StorageManager::accquire_meta_snapshot_unsafe() {
  if (current_meta_->ref(nullptr)) {
    //first ref,add to meta snapshot list
    meta_snapshot_list_.New(current_meta_, current_meta_->GetSequenceNumber(), 0, false, 0);
  }
  return current_meta_;
}

void StorageManager::destroy_meta_snapshot(
    db::SnapshotImpl *meta_snapshot,
    util::autovector<storage::ExtentLayerVersion *> &recyle_extent_layer_versions) {
  int32_t old_ref = INT32_MAX;
  bool destroy = false;
  {
    std::unique_lock lock(meta_snapshot->destroy_mutex_);
    if (meta_snapshot->unref(&old_ref)) {
      destroy = true;
    }
    if (1 == old_ref) {
      meta_snapshot_list_.Delete(meta_snapshot);
    }
  }
  if (destroy) {
    meta_snapshot->destroy(recyle_extent_layer_versions);
    MOD_DELETE_OBJECT(SnapshotImpl, meta_snapshot);
  }
}

void StorageManager::destroy_backup_meta_snapshot(
    db::SnapshotImpl *meta_snapshot,
    util::autovector<storage::ExtentLayerVersion *> &recyle_extent_layer_versions) {
  bool destroy = false;
  {
    std::unique_lock lock(meta_snapshot->destroy_mutex_);
    destroy = meta_snapshot->backup_unref();
  }
  if (destroy) {
    meta_snapshot->destroy(recyle_extent_layer_versions);
    MOD_DELETE_OBJECT(SnapshotImpl, meta_snapshot);
  }
}

void StorageManager::release_meta_snapshot_unsafe(db::SnapshotImpl *meta_snapshot) {
  destroy_meta_snapshot(meta_snapshot, waiting_delete_versions_);
}

int StorageManager::recycle_unsafe(bool for_recovery)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (FAILED(recycle_lob_extent(for_recovery))) {
    SE_LOG(WARN, "fail to recycle large object extent", K(ret));
  } else if (FAILED(recycle_extent_layer_versions(for_recovery, waiting_delete_versions_))) {
    SE_LOG(WARN, "fail to recycle extent layer versions");
  } else {
    SE_LOG(DEBUG, "success to recycle extent layer versions");
  }

  return ret;
}

int StorageManager::recycle_meta_snapshot(bool for_recovery,
                                          util::autovector<ExtentLayerVersion *> &extent_layer_versions) {
  int ret = Status::kOk;

  if (FAILED(recycle_extent_layer_versions(for_recovery, extent_layer_versions))) {
    SE_LOG(WARN, "fail to recycle extent layer versions");
  } else {
    SE_LOG(DEBUG, "success to recycle extent layer versions");
  }

  return ret;
}

int StorageManager::recycle_extent_layer_versions(bool for_recovery,
                                                  util::autovector<ExtentLayerVersion *> &waiting_delete_versions) {
  int ret = Status::kOk;
  ExtentLayerVersion *extent_layer_version = nullptr;

  for (uint32_t i = 0; SUCCED(ret) && i < waiting_delete_versions.size(); ++i) {
    if (IS_NULL(extent_layer_version = waiting_delete_versions.at(i))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent layer version should not been nullptr", K(ret), K(i));
    } else if (FAILED(recycle_extent_layer_version(extent_layer_version, for_recovery))) {
      SE_LOG(WARN, "fail to recycle extent layer version", K(ret), KP(extent_layer_version));
    } else {
      SE_LOG(DEBUG, "success to recycle extent layer version", KP(extent_layer_version));
    }
  }

  if (SUCCED(ret)) {
    waiting_delete_versions.clear();
  }
  return ret;
}

int StorageManager::recycle_extent_layer_version(ExtentLayerVersion *extent_layer_version, bool for_recovery) {
  int ret = Status::kOk;

  if (IS_NULL(extent_layer_version)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent_layer_version));
  } else {
    //recycle the extent in normal extent layer
    for (int64_t i = 0; SUCCED(ret) && i < extent_layer_version->get_extent_layer_size(); ++i) {
      if (FAILED(recycle_extent_layer(extent_layer_version->get_extent_layer(i), for_recovery))) {
        SE_LOG(WARN, "fail to recycle extent layer", K(ret), K(i));
      } else {
        SE_LOG(INFO, "success to recycle extent layer", K(i));
      }
    }

    //recycle the extent in dump extent layer
    if (SUCCED(ret) && (nullptr != extent_layer_version->dump_extent_layer_)) {
      if (FAILED(recycle_extent_layer(extent_layer_version->dump_extent_layer_, for_recovery))) {
        SE_LOG(WARN, "fail to recycle dump extent layer", K(ret));
      }
    }

    if (SUCCED(ret)) {
      MOD_DELETE_OBJECT(ExtentLayerVersion, extent_layer_version);
    }
  }

  return ret;
}

int StorageManager::recycle_extent_layer(ExtentLayer *extent_layer, bool for_recovery) {
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  if (IS_NULL(extent_layer)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent_layer));
  } else {
    for (int64_t i = 0; SUCCED(ret) && i < extent_layer->extent_meta_arr_.size(); ++i) {
      if (IS_NULL(extent_meta = extent_layer->extent_meta_arr_.at(i))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret), K(i));
      } else if (FAILED(do_recycle_extent(extent_meta, for_recovery))) {
        SE_LOG(WARN, "fail to do recycle extent", K(ret));
      }
    }
  }

  return ret;
}

int StorageManager::recycle_lob_extent(bool for_recovery)
{
  int ret = Status::kOk;
  SnapshotImpl *oldest_snapshot = nullptr;
  common::SequenceNumber min_seq = common::kMaxSequenceNumber; 

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else {
    //meta_snapshot_list_ is empty when the subtable been dropped
    if (!meta_snapshot_list_.empty()) {
      if (IS_NULL(oldest_snapshot = meta_snapshot_list_.oldest())) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, oldest snapshot must not nullptr", K(ret));
      } else {
        min_seq = oldest_snapshot->number_;
      }
    }

    if (SUCCED(ret)) {
      if (for_recovery) {
        //for recovery, recycle the deleted lob extent in time
        min_seq = common::kMaxSequenceNumber;
      }
      if (FAILED(lob_extent_mgr_->recycle(min_seq, for_recovery))) {
        SE_LOG(WARN, "fail to recycle lob extent", K(ret));
      }
    }
  }

  return ret;
}

int StorageManager::do_recycle_extent(ExtentMeta *extent_meta, bool for_recovery) {
  int ret = Status::kOk;
  if (IS_NULL(extent_meta)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent_meta));
  } else if (extent_meta->unref()) {
    SE_LOG(INFO, "recycle the extent", K(*extent_meta));
    if (!for_recovery) {
      cache::Cache *table_cache = GlobalContext::get_cache();
      //evict from table cache
      db::TableCache::evict(table_cache, extent_meta->extent_id_.id());
      //evict from persistent cache
      if (cache::PersistentCache::get_instance().is_enabled()) {
        if (FAILED(cache::PersistentCache::get_instance().evict(extent_meta->extent_id_))) {
          SE_LOG(WARN, "fail to recycle from persistent cache", K(ret), K(*extent_meta));
          se_assert(false);
        }
      }

      if (SUCCED(ret)) {
        if (FAILED(ExtentSpaceManager::get_instance().recycle(extent_meta->table_space_id_,
                                                              extent_meta->extent_space_type_,
                                                              extent_meta->prefix_,
                                                              extent_meta->extent_id_))) {
          SE_LOG(WARN, "fail to recycle extent", K(ret));
        }
      }
    } else {
      if (FAILED(ExtentMetaManager::get_instance().recycle_meta(extent_meta->extent_id_))) {
        SE_LOG(WARN, "fail to recycle meta", K(ret));
      }
    }
  }

  return ret;
}

int StorageManager::build_new_extent_layer(util::autovector<ExtentId> &extent_ids, ExtentLayer *&extent_layer)
{
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (nullptr == extent_layer) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), "extent_ids_size", extent_ids.size(), KP(extent_layer));
  } else {
    for (uint64_t i = 0; Status::kOk == ret && i < extent_ids.size(); ++i) {
      if (IS_NULL(extent_meta = ExtentMetaManager::get_instance().get_meta(extent_ids.at(i)))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret), K(i), "extent_id", extent_ids.at(i));
      } else if (FAILED(extent_layer->add_extent(extent_meta, true))) {
        SE_LOG(WARN, "fail to add extent", K(ret), K(i), "extent_id", extent_ids.at(i));
      } else {
        SE_LOG(INFO, "success to add extent", K(*extent_meta));
      }
    }
  }

  return ret;
}

int StorageManager::recover_extent_space()
{
  int ret = Status::kOk;
  Arena arena;
  ExtentLayerVersion *current_extent_layer_version = nullptr;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else {
    for (int64_t level = 0; Status::kOk == ret && level < storage::MAX_TIER_COUNT; ++level) {
      if (IS_NULL(current_extent_layer_version = extent_layer_versions_[level])) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret), K(level));
      } else if (FAILED(current_extent_layer_version->recover_reference_extents())) {
        SE_LOG(WARN, "fail to recover reference extents", K(ret), K(level));
      }
    }

    if (SUCCED(ret)) {
      if (FAILED(lob_extent_mgr_->recover_extent_space())) {
        SE_LOG(WARN, "fail to recover lob extent space", K(ret));
      }
    }
  }

  return ret;
}

//used for handler to update statistic info
//if extent_stats has updated after last call "get_extent_stats(", return true and handler should update statistic
//or return false and handler no need to update statistic
bool StorageManager::get_extent_stats(int64_t &data_size, int64_t &num_entries, int64_t &num_deletes, int64_t &disk_size) {
  bool stats_updated = false;
  std::lock_guard<std::mutex> guard(meta_mutex_);
  if (extent_stats_updated_) {
    stats_updated = true;
    data_size = extent_stats_.data_size_;
    num_entries = extent_stats_.num_entries_;
    num_deletes = extent_stats_.num_deletes_;
    disk_size = extent_stats_.disk_size_;
    extent_stats_updated_ = false;
  }
    
  return stats_updated;
}

int StorageManager::release_extent_resource(bool for_recovery)
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageManager should been inited first", K(ret));
  } else if (1 != meta_snapshot_list_.count() || 0 != waiting_delete_versions_.size() || 0 != bg_recycle_count_) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, the StorageManager state not expected", K(ret), K_(bg_recycle_count),
        "meta_snapshot_count", meta_snapshot_list_.count(), "wait_delete_size", waiting_delete_versions_.size());
  } else {
    release_meta_snapshot_unsafe(current_meta_);
    current_meta_ = nullptr;
    if (storage::MAX_TIER_COUNT != waiting_delete_versions_.size()) {
      int64_t index_id = IS_NOTNULL(owner_) ? owner_->GetID() : -1;
      SE_LOG(INFO, "some extent layer versions may be referenced by a backup snapshot at now", K(index_id));
    }

    if (0 == waiting_delete_versions_.size()) {
      // do noting
    } else if (FAILED(recycle_unsafe(for_recovery))) {
      SE_LOG(WARN, "fail to recycle unsafe", K(ret));
    } else if (FAILED(lob_extent_mgr_->force_recycle(for_recovery))) {
      SE_LOG(WARN, "fail to force recycle lob extent", K(ret));
    } else {
      SE_LOG(INFO, "success to release extent resource");
    }
  }

  return ret;
}

int StorageManager::calc_extent_stats_unsafe()
{
  int ret = Status::kOk;
  ExtentStats old_extent_stats;

  if (IS_NULL(current_meta_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, current meta must not nullptr", K(ret), KP_(current_meta));
  } else {
    old_extent_stats = extent_stats_;
    extent_stats_.reset();
    for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
      extent_stats_.merge(current_meta_->get_extent_layer_version(level)->get_extent_stats());
    }
    if (!(old_extent_stats == extent_stats_)) {
      extent_stats_updated_ = true;
    }
    SE_LOG(INFO, "calc extent stats", K_(extent_stats_updated), K_(extent_stats));
  }
  return ret;
}

int StorageManager::deserialize_extent_layer(const char *buf, int64_t buf_len, int64_t &pos,
                                             common::SequenceNumber &sequence_number,
                                             util::autovector<ExtentId> &extent_ids,
                                             util::autovector<ExtentId> &lob_extent_ids)
{
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t version = 0;
  sequence_number = 0;
  extent_ids.clear();
  lob_extent_ids.clear();

  if (IS_NULL(buf) || buf_len < 0 || pos >= buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    size = *((int64_t *)(buf + pos));
    pos += sizeof(size);
    version = *((int64_t *)(buf + pos));
    pos += sizeof(version);
    if (FAILED(util::deserialize(buf, buf_len, pos, sequence_number))) {
      SE_LOG(WARN, "fail to deserialize sequence number", K(ret));
    } else if (FAILED(util::deserialize_v(buf, buf_len, pos, extent_ids))) {
      SE_LOG(WARN, "fail to deserialize extent ids", K(ret));
    } else if (FAILED(util::deserialize_v(buf, buf_len, pos, lob_extent_ids))) {
      SE_LOG(WARN, "fail to deserialize lob extent ids", K(ret));
    }
  }

  return ret;
}

} //namespace storage
} //namespace smartengine
