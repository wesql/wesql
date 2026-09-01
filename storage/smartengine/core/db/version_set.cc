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

#include "db/version_set.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <climits>
#include <string>

#include "backup/hotbackup_impl.h"
#include "db/db_impl.h"
#include "db/internal_stats.h"
#include "memory/base_malloc.h"
#include "memory/mod_info.h"
#include "memtable/memtable.h"
#include "objstore/objstore_snapshot.h"
#include "schema/table_schema.h"
#include "storage/extent_space_manager.h"
#include "storage/storage_log_entry.h"
#include "storage/storage_logger.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace smartengine {
using namespace cache;
using namespace common;
using namespace monitor;
using namespace schema;
using namespace storage;
using namespace table;
using namespace util;

namespace {
enum ObjType {
  kMin = 0,
  //========
  kSubTable = 1,
  kMetaSnapshot = 2,
  //========
  kMax = 3,
};

bool is_valid_obj_type(int type) { return type > ObjType::kMin && type < ObjType::kMax; }

} // namespace

namespace db {
int AllSubTable::DUMMY = 0;
void *const AllSubTable::kAllSubtableInUse = &AllSubTable::DUMMY;
void *const AllSubTable::kAllSubtableObsolete = nullptr;
AllSubTable::AllSubTable()
    : version_number_(0),
      refs_(0),
      all_sub_table_mutex_(nullptr),
      sub_table_map_()
{
}

AllSubTable::AllSubTable(SubTableMap &sub_table_map, std::mutex *mutex)
    : version_number_(0),
      refs_(0),
      all_sub_table_mutex_(mutex),
      sub_table_map_(sub_table_map)
{
}

AllSubTable::~AllSubTable()
{
}
void AllSubTable::reset()
{
  sub_table_map_.clear();
}
int AllSubTable::add_sub_table(int64_t index_id, SubTable *sub_table)
{
  int ret = Status::kOk;

  if (index_id < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret));
  } else {
    if (!(sub_table_map_.emplace(index_id, sub_table).second)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to emplace subtable", K(ret), K(index_id));
    } else {
      SE_LOG(INFO, "map add sub_table", K(index_id));
    }
  }

  return ret;
}
int AllSubTable::remove_sub_table(int64_t index_id)
{
  int ret = Status::kOk;

  if (index_id < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else {
    if (1 != sub_table_map_.erase(index_id)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to remove subtable", K(ret), K(index_id));
    }
  }

  return ret;
}

int AllSubTable::get_sub_table(int64_t index_id, SubTable *&sub_table)
{
  int ret = Status::kOk;

  if (index_id < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else {
    std::lock_guard<std::mutex> lock_guard(*all_sub_table_mutex_);
    auto iter = sub_table_map_.find(index_id);
    if (sub_table_map_.end() == iter) {
      SE_LOG(DEBUG, "sub table not exist", K(index_id));
    } else if (nullptr == (sub_table = iter->second)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "subtable must not nullptr", K(index_id));
    }
  }

  return ret;
}


AllSubTableGuard::AllSubTableGuard(GlobalContext *global_ctx) : global_ctx_(global_ctx), all_sub_table_(nullptr)
{
  global_ctx_->acquire_thread_local_all_sub_table(all_sub_table_);
  assert(nullptr != all_sub_table_);
}

AllSubTableGuard::~AllSubTableGuard()
{
  assert(nullptr != all_sub_table_);
  global_ctx_->release_thread_local_all_sub_table(all_sub_table_);
}
  
VersionSet::VersionSet(const std::string& dbname,
                       const ImmutableDBOptions* db_options,
                       const EnvOptions& storage_options, Cache* table_cache,
                       WriteBufferManager* write_buffer_manager)
    : is_inited_(false),
      global_ctx_(nullptr),
      column_family_set_(nullptr),
      env_(db_options->env),
      dbname_(dbname),
      db_options_(db_options),
      next_file_number_(2),
      last_sequence_(0),
      last_allocated_sequence_(0),
      env_options_(storage_options)
{
#ifndef NDEBUG
  write_checkpoint_failed_ = false;
#endif
}

void CloseTables(void* ptr, size_t) {
  ExtentReader* extent_reader = reinterpret_cast<ExtentReader*>(ptr);
  extent_reader->destroy();
}

VersionSet::~VersionSet() {
  // we need to delete column_family_set_ because its destructor depends on
  if (is_inited_) {
    GlobalContext::get_cache()->ApplyToAllCacheEntries(&CloseTables, false);
    column_family_set_.reset();
  }
}

int VersionSet::init(GlobalContext *global_ctx)
{
  int ret = Status::kOk;
  ColumnFamilySet *column_family_set = nullptr;

  if (is_inited_) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "VersionSet has been inited", K(ret));
  } else if (IS_NULL(global_ctx) || UNLIKELY(!global_ctx->is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(global_ctx));
  } else if (IS_NULL(column_family_set = MOD_NEW_OBJECT(memory::ModId::kVersionSet, ColumnFamilySet, global_ctx))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for column family set", K(ret));
  } else {
    global_ctx_ = global_ctx;
    column_family_set_.reset(column_family_set);
    is_inited_ = true;
  }
  return ret;
}

int VersionSet::recover_extent_space_manager()
{
  int ret = Status::kOk;

  if (FAILED(ExtentSpaceManager::get_instance().open_all_data_file())) {
    SE_LOG(ERROR, "fail to open all data file", K(ret));
  }

  // if (SUCCED(ret)) {
  //   BackupSnapshotMap &backup_snapshots = BackupSnapshotImpl::get_instance()->get_backup_snapshot_map();
  //   int64_t backup_snapshot_count = backup_snapshots.get_backup_snapshot_count();
  //   BackupSnapshotId prev_backup_id = 0;
  //   BackupSnapshotId backup_id = 0;
  //   uint64_t auto_increment_id = 0;
  //   MetaSnapshotSet *backup_snapshot = nullptr;
  //   db::Snapshot *sn = nullptr;

  //   while (SUCCED(ret) &&
  //          backup_snapshots.get_next_backup_snapshot(prev_backup_id, backup_id, auto_increment_id, backup_snapshot)) {
  //     for (auto iter = backup_snapshot->begin(); SUCCED(ret) && iter != backup_snapshot->end(); ++iter) {
  //       if (IS_NULL(sn = *iter)) {
  //         ret = Status::kCorruption;
  //         SE_LOG(WARN, "meta snapshot must not nullptr", K(ret));
  //       } else if (FAILED(sn->recover_extent_space())) {
  //         SE_LOG(WARN, "fail to recover extent space", K(ret), "index_id", sn->GetSequenceNumber());
  //       }
  //     }

  //     prev_backup_id = backup_id;
  //   }
  // }

  if (SUCCED(ret)) {
    SubTable *sub_table = nullptr;
    SubTableMap &all_sub_tables = global_ctx_->all_sub_table_->sub_table_map_;
    for (auto iter = all_sub_tables.begin(); SUCCED(ret) && iter != all_sub_tables.end(); ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (sub_table->IsDropped()) {
        // do nothing
      } else if (FAILED(sub_table->recover_extent_space())) {
        SE_LOG(WARN, "fail to recover extent space", K(ret), "index_id", sub_table->GetID());
      }
    }
  }

  if (SUCCED(ret)) {
    if (FAILED(ExtentSpaceManager::get_instance().rebuild())) {
      SE_LOG(WARN, "fail to rebuild extent_space_mgr", K(ret));
    } else {
      SE_LOG(INFO, "success to rebuild extent_space_mgr");
    }
  }

  return ret;
}

void VersionSet::MarkFileNumberUsedDuringRecovery(uint64_t number) {
  // only called during recovery which is single threaded, so this works because
  // there can't be concurrent calls
  if (next_file_number_.load(std::memory_order_relaxed) <= number) {
    next_file_number_.store(number + 1, std::memory_order_relaxed);
  }
}

// TODO(aekmekji): in CompactionJob::GenSubcompactionBoundaries(), this
// function is called repeatedly with consecutive pairs of slices. For example
// if the slice list is [a, b, c, d] this function is called with arguments
// (a,b) then (b,c) then (c,d). Knowing this, an optimization is possible where
// we avoid doing binary search for the keys b and c twice and instead somehow
// maintain state of where they first appear in the files.
uint64_t VersionSet::ApproximateSize(ColumnFamilyData* cfd,
                           const db::Snapshot *sn,
                           const common::Slice& start,
                           const common::Slice& end, int start_level,
                           int end_level,
                           int64_t estimate_cost_depth) {
  // pre-condition
  assert(cfd->internal_comparator().Compare(start, end) <= 0);
  assert(start_level <= end_level);

  return cfd->get_storage_manager()->approximate_size(cfd, start, end,
                                            start_level, end_level, sn, estimate_cost_depth);
}

// aquire all column family snapshot under global read lock
// avoid extent reuse after release the lock
int VersionSet::create_backup_snapshot(MetaSnapshotSet &meta_snapshots) {
  int ret = Status::kOk;
  Snapshot *sn = nullptr;
  AllSubTable *all_sub_table = nullptr;
  SubTable* sub_table = nullptr;

  if (FAILED(global_ctx_->acquire_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to acquire all subtable", K(ret));
  } else if (IS_NULL(all_sub_table)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, all subtable must not nullptr", K(ret));
  } else {
    SubTableMap &sub_table_map = all_sub_table->sub_table_map_;
    for (auto iter = sub_table_map.begin();
        Status::kOk == ret && iter != sub_table_map.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (sub_table->IsDropped()) {
        // do nothing
      } else {
        sn = sub_table->get_backup_meta_snapshot();
        if (sn != nullptr) {
          meta_snapshots.emplace(sn);
        } else {
          ret = Status::kCorruption;
          SE_LOG(ERROR, "meta snapshot is null", K(ret), K(sub_table->GetID()));
          break;
        }
      }
    }
  }

  // do not overwrite ret
  int tmp_ret = Status::kOk;
  if (Status::kOk !=
      (tmp_ret = global_ctx_->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to release all subtable", K(tmp_ret));
  }

  return ret;
}

int VersionSet::add_sub_table(CreateSubTableArgs &args, bool write_log, bool is_replay, ColumnFamilyData *&sub_table)
{
  int ret = Status::kOk;
  ChangeSubTableLogEntry log_entry(args.table_schema_, args.table_space_id_);

  AllSubTable *new_all_sub_table = nullptr;
  if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (write_log && FAILED(StorageLogger::get_instance().write_log(REDO_LOG_ADD_SSTABLE, log_entry))) {
    SE_LOG(WARN, "fail to write add subtable log", K(ret), K(log_entry));
  } else if (FAILED(column_family_set_->CreateColumnFamily(args, sub_table))) {
    SE_LOG(WARN, "fail to create ColumnFamiltData", K(ret), K(args));
  } else {
    if (is_replay) {
      //no need alloc new AllSubTable when recovery, optimize for many subtable
      if (FAILED(global_ctx_->all_sub_table_->add_sub_table(args.table_schema_.get_index_id(), sub_table))) {
        SE_LOG(WARN, "fail to add subtable to all subtable", K(ret), K(args));
      }
    } else {
      std::lock_guard<std::mutex> guard(global_ctx_->all_sub_table_mutex_);
      if (nullptr == (new_all_sub_table = MOD_NEW_OBJECT(
                          memory::ModId::kAllSubTable, AllSubTable,
                          global_ctx_->all_sub_table_->sub_table_map_,
                          &(global_ctx_->all_sub_table_mutex_)))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for new all sub table",
                    K(ret));
      } else if (FAILED(new_all_sub_table->add_sub_table(args.table_schema_.get_index_id(), sub_table))) {
          SE_LOG(WARN, "fail to add sub table to new all sub table", K(ret), K(args));
      } else if (FAILED(global_ctx_->install_new_all_sub_table(new_all_sub_table))) {
        SE_LOG(WARN, "fail to install new all sub table", K(ret));
      }
    }
    SE_LOG(DEBUG, "end to add subtable", K(ret), K(args));
  }

  return ret;
}

int VersionSet::remove_sub_table(ColumnFamilyData *sub_table, bool write_log, bool is_replay)
{
  int ret = Status::kOk;
  ChangeSubTableLogEntry log_entry(sub_table->get_table_schema(), sub_table->get_table_space_id());

  AllSubTable *new_all_sub_table = nullptr;
  if (IS_NULL(sub_table)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(sub_table));
  } else if (write_log && FAILED(StorageLogger::get_instance().write_log(REDO_LOG_REMOVE_SSTABLE, log_entry))) {
    SE_LOG(WARN, "fail to write remove subtable log", K(ret), K(log_entry));
  } else {
    sub_table->SetDropped();
//    TODO: yeti (#15350417)
    sub_table->set_bg_stopped(true);
    SE_LOG(INFO, "success to remove subtable", K(log_entry), K(sub_table->GetID()));
  }

  if (SUCCED(ret)) {
    int64_t index_id = sub_table->GetID();
    if (is_replay) {
      //no need alloc new AllSubTable when recovery, optimize for many subtable
      if (FAILED(global_ctx_->all_sub_table_->remove_sub_table(index_id))) {
        SE_LOG(WARN, "fail to remove subtable", K(ret), K(index_id));
      }
    } else {
      std::lock_guard<std::mutex> guard(global_ctx_->all_sub_table_mutex_);
      if (nullptr == (new_all_sub_table = MOD_NEW_OBJECT(memory::ModId::kAllSubTable, AllSubTable, global_ctx_->all_sub_table_->sub_table_map_, &(global_ctx_->all_sub_table_mutex_)))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for new all sub table", K(ret));
      } else if (FAILED(new_all_sub_table->remove_sub_table(index_id))) {
        SE_LOG(WARN, "fail to remove subtable", K(ret), K(index_id));
      } else if (FAILED(global_ctx_->install_new_all_sub_table(new_all_sub_table))) {
        SE_LOG(WARN, "fail to install new all sub table", K(ret));
      }
    }
    SE_LOG(DEBUG, "end to remove subtable", K(ret), K(index_id));
  }

  //resource clean
  if (FAILED(ret)) {
    if (nullptr != new_all_sub_table) {
      MOD_DELETE_OBJECT(AllSubTable, new_all_sub_table);
    }
  }

  return ret;
}

int VersionSet::modify_table_schema(ColumnFamilyData *sub_table, const TableSchema &table_schema)
{
  int ret = Status::kOk;

  if (IS_NULL(sub_table) || UNLIKELY(!table_schema.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(sub_table), K(table_schema));
  } else {
    se_assert(sub_table->GetID() == table_schema.get_index_id());
    ModifyTableSchemaLogEntry log_entry(table_schema);
    
    if (FAILED(StorageLogger::get_instance().write_log(REDO_LOG_MODIFY_TABLE_SCHEMA, log_entry))) {
      SE_LOG(WARN, "fail to write modify table schema log", K(ret), K(log_entry));
    } else if (FAILED(sub_table->modify_table_schema(table_schema))) {
      SE_LOG(WARN, "fail to modify table schema", K(ret), K(table_schema));
    } else {
      SE_LOG(INFO, "success to modify table schema", K(table_schema));
    }
  }

  return ret;
}

int VersionSet::write_checkpoint_block(util::WritableFile &checkpoint_writer,
                                       int64_t &meta_block_count,
                                       CheckpointBlockHeader *block_header,
                                       char *buf,
                                       int64_t &offset,
                                       void *pointer,
                                       int pointer_type) {
  int ret = Status::kOk;
  int64_t size = 0;
  const int64_t buf_size = DEFAULT_BUFFER_SIZE;
  SubTable *sub_table = nullptr;
  SnapshotImpl *meta_snapshot = nullptr;
  int64_t serialize_size = 0;

  if (block_header == nullptr || buf == nullptr || pointer == nullptr) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(block_header), KP(buf), KP(pointer));
  } else if (!is_valid_obj_type(pointer_type)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(pointer_type));
  } else {
    if (pointer_type == ObjType::kSubTable) {
      sub_table = reinterpret_cast<SubTable *>(pointer);
      serialize_size = sub_table->get_serialize_size();
    } else if (pointer_type == ObjType::kMetaSnapshot) {
      meta_snapshot = reinterpret_cast<SnapshotImpl *>(pointer);
      serialize_size = meta_snapshot->get_serialize_size();
    } else {
      assert(false);
    }

    size = serialize_size;
    if ((static_cast<int64_t>(buf_size - sizeof(CheckpointBlockHeader))) < size) {
      if (FAILED(write_big_block(checkpoint_writer, pointer, pointer_type))) {
        SE_LOG(WARN, "fail to write big block", K(ret), K(serialize_size), K(pointer_type));
      } else {
        meta_block_count++;
      }
    } else {
      if ((buf_size - offset) < size) {
        block_header->data_size_ = offset - block_header->data_offset_;
        if (FAILED(checkpoint_writer.PositionedAppend(Slice(buf, buf_size), checkpoint_writer.GetFileSize()).code())) {
          SE_LOG(WARN, "fail to append buf to checkpoint writer", K(ret), K(size));
        } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
          SE_LOG(WARN, "fail to reserve checkpoint block header", K(ret));
        } else if (ObjType::kSubTable == pointer_type && FAILED(sub_table->serialize(buf, buf_size, offset))) {
          SE_LOG(WARN, "fail to serialize subtable", K(ret));
        } else if (ObjType::kMetaSnapshot == pointer_type && FAILED(meta_snapshot->serialize(buf, buf_size, offset))) {
          SE_LOG(WARN, "fail to serialize meta snapshot", K(ret));
        } else {
          meta_block_count++;
        }
      } else {
        if (ObjType::kSubTable == pointer_type && FAILED(sub_table->serialize(buf, buf_size, offset))) {
          SE_LOG(WARN, "fail to serialize subtable", K(ret));
        } else if (ObjType::kMetaSnapshot == pointer_type && FAILED(meta_snapshot->serialize(buf, buf_size, offset))) {
          SE_LOG(WARN, "fail to serialize meta snapshot", K(ret));
        }
      }
      ++(block_header->entry_count_);
    }
  }
  return ret;
}

int VersionSet::write_subtable(util::WritableFile &checkpoint_writer,
                               int64_t &sub_table_meta_block_count,
                               CheckpointBlockHeader *block_header,
                               char *buf,
                               int64_t &offset,
                               SubTable *sub_table) {
  return write_checkpoint_block(checkpoint_writer,
                                sub_table_meta_block_count,
                                block_header,
                                buf,
                                offset,
                                sub_table,
                                ObjType::kSubTable);
}

int VersionSet::write_current_checkpoint(util::WritableFile &checkpoint_writer, CheckpointHeader &header, char *buf) {
  int ret = Status::kOk;
  ColumnFamilyData *sub_table = nullptr;
  const int64_t buf_size = DEFAULT_BUFFER_SIZE;
  AllSubTable *all_sub_table = nullptr;
  int64_t offset = 0;
  CheckpointBlockHeader *block_header = nullptr;

  if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
    SE_LOG(WARN, "fail to reserve check block header", K(ret));
  } else if (FAILED(global_ctx_->acquire_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to acquire all subtable", K(ret));
  } else if (IS_NULL(all_sub_table)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, all subtable must not be nullptr", K(ret));
  } else {
    SubTableMap &sub_table_map = all_sub_table->sub_table_map_;
    header.sub_table_count_ = sub_table_map.size();
    header.sub_table_meta_block_offset_ = checkpoint_writer.GetFileSize();
    for (auto iter = sub_table_map.begin(); SUCCED(ret) && iter != sub_table_map.end(); ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret));
      } else if (0 == sub_table->GetID()) {
        // skip default column family data
      } else {
        if (FAILED(write_subtable(checkpoint_writer,
                                  header.sub_table_meta_block_count_,
                                  block_header,
                                  buf,
                                  offset,
                                  sub_table))) {
          SE_LOG(WARN, "fail to write subtable", K(ret), "index_id", iter->first);
        }
      }
    }

    if (SUCCED(ret) && block_header->entry_count_ > 0) {
      block_header->data_size_ = offset - block_header->data_offset_;
      if (FAILED(checkpoint_writer.PositionedAppend(Slice(buf, buf_size), checkpoint_writer.GetFileSize()).code())) {
        SE_LOG(WARN, "fail to append buffer to checkpoint writer", K(ret));
      } else {
        header.sub_table_meta_block_count_++;
      }
    }
  }

  if (all_sub_table && FAILED(global_ctx_->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to release all subtable", K(ret));
  }

  return ret;
}

int VersionSet::do_checkpoint(util::WritableFile *checkpoint_writer, CheckpointHeader *header)
{
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  char *buf = nullptr;
  const int64_t buf_size = DEFAULT_BUFFER_SIZE;

#ifndef NDEBUG
  TEST_SYNC_POINT("VersionSet::do_checkpoint::inject_error");
  if (TEST_is_write_checkpoint_failed()) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to do VersionSet checkpoint", K(ret));
    return ret;
  }
#endif
  if (IS_NULL(checkpoint_writer) || IS_NULL(header)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_writer), KP(header));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(DIOHelper::DIO_ALIGN_SIZE, buf_size, memory::ModId::kVersionSet)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else if (FAILED(write_current_checkpoint(*checkpoint_writer, *header, buf))) {
    SE_LOG(WARN, "fail to write current checkpoint", K(ret));
  } else {
    SE_LOG(INFO, "success to do checkpoint", K(ret));
  }

  //release resource
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int VersionSet::load_all_sub_table(util::RandomAccessFile &checkpoint_reader,
                                   storage::CheckpointHeader &header,
                                   char *buf,
                                   int64_t buf_size) {
  int ret = Status::kOk;
  int64_t pos = 0;
  char *sub_table_buf = nullptr;
  int64_t block_index = 0;
  int64_t offset = header.sub_table_meta_block_offset_;
  CheckpointBlockHeader *block_header = nullptr;
  ColumnFamilyData *sub_table = nullptr;
  common::ColumnFamilyOptions cf_options(global_ctx_->options_);
  CreateSubTableArgs dummy_args(cf_options);
  Slice result;

  assert(buf);
  assert(buf_size == DEFAULT_BUFFER_SIZE);

  while (SUCCED(ret) && block_index < header.sub_table_meta_block_count_) {
    pos = 0;
    if (FAILED(checkpoint_reader.Read(offset, buf_size, &result, buf).code())) {
      SE_LOG(WARN, "fail to read buf", K(ret), K(buf_size));
    } else {
      block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
      pos = block_header->data_offset_;
      if (block_header->block_size_ > buf_size) {
        if (FAILED(read_big_subtable(&checkpoint_reader, block_header->block_size_, offset))) {
          SE_LOG(WARN, "fail to read big subtable", K(ret), K(*block_header));
        }
      } else {
        for (int64_t i = 0; SUCCED(ret) && i < block_header->entry_count_; ++i) {
          if (IS_NULL(sub_table =MOD_NEW_OBJECT(memory::ModId::kColumnFamilySet,
                                                ColumnFamilyData,
                                                global_ctx_->options_))) {
            ret = Status::kMemoryLimit;
            SE_LOG(WARN, "fail to allocate memory for sub table", K(ret));
          } else if (FAILED(sub_table->init(dummy_args, global_ctx_, column_family_set_.get()))) {
            SE_LOG(WARN, "fail to init subtable", K(ret));
          } else if (FAILED(sub_table->deserialize(buf, buf_size, pos))) {
            SE_LOG(WARN, "fail to deserialize subtable", K(ret));
          } else if (FAILED(column_family_set_->add_sub_table(sub_table))) {
            SE_LOG(WARN, "fail to add subtable to column family set", K(ret), "index_id", sub_table->GetID());
          } else if (FAILED(ExtentSpaceManager::get_instance().open_table_space(sub_table->get_table_space_id()))) {
            SE_LOG(WARN, "fail to create table space if not exist", K(ret), "table_space_id",
                   sub_table->get_table_space_id());
          } else if (FAILED(ExtentSpaceManager::get_instance().register_subtable(sub_table->get_table_space_id(),
                                                                                 sub_table->GetID()))) {
            SE_LOG(WARN, "fail to register subtable", K(ret), "table_space_id", sub_table->get_table_space_id(),
                   "index_id", sub_table->GetID());
          } else if (FAILED(global_ctx_->all_sub_table_->add_sub_table(sub_table->GetID(), sub_table))) {
            SE_LOG(WARN, "fail to add subtable to AllSubTable", K(ret), "index_id", sub_table->GetID());
          } else {
            SE_LOG(INFO, "success to load subtable", K(ret), "index_id", sub_table->GetID());
          }
        }
        offset += buf_size;
      }
    }
    ++block_index;
  }

  return ret;
}

int VersionSet::load_latest_backup_snapshot() {
  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  int64_t snapshot_id = 0;
  objstore::ObjStoreSnapshotOperator *snapshot_operator = nullptr;
  util::BackupSnapshotInfo backup_snapshot_info;
  if (IS_NULL(global_ctx = get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, global context must not nullptr", K(ret));
  } else if (FAILED(env_->GetLatestSnapshotId(snapshot_id).code())) {
    SE_LOG(WARN, "fail to get latest snapshot id", K(ret));
  } else if (FAILED(env_->GetSnapshotOperator(snapshot_operator).code())) {
    SE_LOG(WARN, "fail to get snapshot operator", K(ret));
  } else if (IS_NULL(snapshot_operator)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, snapshot operator must not nullptr", K(ret));
  } else if (FAILED(snapshot_operator->get_snapshot(snapshot_id, backup_snapshot_info))) {
    SE_LOG(WARN, "fail to get snapshot", K(ret));
  } else {
    for (const auto &it : backup_snapshot_info.meta_snapshot_ids_) {
      int64_t meta_snapshot_id = it.second;
      int64_t index_id = it.first;
      std::string meta_snapshot_data;
      db::SnapshotImpl *meta_snapshot = nullptr;
      if (IS_NULL(meta_snapshot = MOD_NEW_OBJECT(memory::ModId::kStorageMgr, db::SnapshotImpl))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for new current meta", K(ret));
        break;
      // TODO: parallel load meta snapshot from objstore 
      } else if (FAILED(snapshot_operator->get_meta_snapshot(meta_snapshot_id, *meta_snapshot, global_ctx->options_.comparator))) {
        SE_LOG(WARN, "fail to get meta snapshot", K(ret));
        break;
      } else {
        // todo
      }
    }
  }
  return ret;
}

int VersionSet::load_checkpoint(util::RandomAccessFile *checkpoint_reader, storage::CheckpointHeader *header) {
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;

  SE_LOG(INFO, "begin to load version set checkpoint");
  if (IS_NULL(checkpoint_reader) || IS_NULL(header)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_reader), KP(header));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(
                         memory::base_memalign(DIOHelper::DIO_ALIGN_SIZE, buf_size, memory::ModId::kVersionSet)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else if (FAILED(load_all_sub_table(*checkpoint_reader, *header, buf, buf_size))) {
    SE_LOG(WARN, "fail to load sub tables", K(ret));
  } else {
    SE_LOG(INFO, "success to load version set checkpoint");
  }

  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int VersionSet::replay(int64_t log_type, char *log_data, int64_t log_len)
{
  int ret = Status::kOk;

  if (!is_partition_log(log_type) || nullptr == log_data || 0 >= log_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(log_type), K(log_len));
  } else {
    SE_LOG(INFO, "replay one log entry", K(log_type), K(log_len));
    switch (log_type) {
      case REDO_LOG_ADD_SSTABLE:
        if (FAILED(replay_add_subtable_log(log_data, log_len))) {
          SE_LOG(WARN, "fail to replay add subtable log", K(ret), K(log_type));
        }
        break;
      case REDO_LOG_REMOVE_SSTABLE:
        if (FAILED(replay_remove_subtable_log(log_data, log_len))) {
          SE_LOG(WARN, "fail to replay remove sutable log", K(ret), K(log_type));
        }
        break;
      case REDO_LOG_MODIFY_SSTABLE:
        if (FAILED(replay_modify_subtable_log(log_data, log_len))) {
          SE_LOG(WARN, "fail to replay extent meta log", K(ret), K(log_type));
        }
        break;
      case REDO_LOG_MODIFY_TABLE_SCHEMA:
        if (FAILED(replay_modify_table_schema_log(log_data, log_len))) {
          SE_LOG(WARN, "fail to replay table schema log", K(ret));
        }
        break;
      default:
        ret = Status::kNotSupported;
        SE_LOG(WARN, "unknow log type", K(ret), K(log_type));
    }
  }

  return ret;
}

int VersionSet::write_big_block(util::WritableFile &checkpoint_writer, void *pointer, int pointer_type) {
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = 0;
  int64_t offset = 0;
  int64_t size = 0;
  CheckpointBlockHeader *block_header = nullptr;

  int64_t serialize_size = 0;
  SubTable *sub_table = nullptr;
  SnapshotImpl *meta_snapshot = nullptr;

  if (IS_NULL(pointer)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(pointer));
  } else if (IS_FALSE(is_valid_obj_type(pointer_type))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "unknow type", K(pointer_type));
  } else if (pointer_type == ObjType::kSubTable) {
    sub_table = reinterpret_cast<ColumnFamilyData *>(pointer);
    serialize_size = sub_table->get_serialize_size();
  } else if (pointer_type == ObjType::kMetaSnapshot) {
    meta_snapshot = reinterpret_cast<SnapshotImpl *>(pointer);
    serialize_size = meta_snapshot->get_serialize_size();
  }

  if (SUCCED(ret)) {
    buf_size = ((sizeof(CheckpointBlockHeader) + serialize_size + DEFAULT_BUFFER_SIZE) / DEFAULT_BUFFER_SIZE) *
               DEFAULT_BUFFER_SIZE;
    if (IS_NULL(buf = reinterpret_cast<char *>(
                    memory::base_memalign(DIOHelper::DIO_ALIGN_SIZE, buf_size, memory::ModId::kVersionSet)))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
    } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
      SE_LOG(WARN, "fail to reserve checkpoint block header", K(ret));
    } else {
      block_header->data_size_ = serialize_size;
      block_header->entry_count_ = 1;
      if (pointer_type == ObjType::kSubTable && FAILED(sub_table->serialize(buf, buf_size, offset))) {
        SE_LOG(WARN, "fail to serialize subtable", K(ret));
      } else if (pointer_type == ObjType::kMetaSnapshot && FAILED(meta_snapshot->serialize(buf, buf_size, offset))) {
        SE_LOG(WARN, "fail to serialize meta snapshot", K(ret));
      } else if (FAILED(checkpoint_writer.PositionedAppend(Slice(buf, buf_size), checkpoint_writer.GetFileSize())
                            .code())) {
        SE_LOG(WARN, "fail to append buf to checkpoint writer", K(ret));
      }
    }

    // release resource
    if (nullptr != buf) {
      memory::base_memalign_free(buf);
    }
  }
  return ret;
}

int VersionSet::write_big_subtable(util::WritableFile &checkpoint_writer, ColumnFamilyData &sub_table) {
  int ret = Status::kOk;
  if (FAILED(write_big_block(checkpoint_writer, &sub_table, ObjType::kSubTable))) {
    SE_LOG(WARN, "fail to write big subtable", K(ret), "index id", sub_table.GetID());
  }
  return ret;
}

int VersionSet::read_big_subtable(util::RandomAccessFile *checkpoint_reader,
                                   int64_t block_size,
                                   int64_t &file_offset)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = block_size;
  CheckpointBlockHeader *block_header = nullptr;
  ColumnFamilyData *sub_table = nullptr;
  char *sub_table_buf = nullptr;
  Slice result;
  int64_t pos = 0;
  common::ColumnFamilyOptions cf_options(global_ctx_->options_);
  CreateSubTableArgs dummy_args(cf_options);

  if (IS_NULL(checkpoint_reader) || block_size <=0 || file_offset <= 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_reader), K(block_size), K(file_offset));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(
      DIOHelper::DIO_ALIGN_SIZE, buf_size, memory::ModId::kVersionSet)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else if (FAILED(checkpoint_reader->Read(file_offset, buf_size, &result, buf).code())) {
    SE_LOG(WARN, "fail to read buf", K(ret));
  } else {
    block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
    pos = block_header->data_offset_;
    if (1 != block_header->entry_count_) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "unexpected entry count", K(ret), K(block_header->entry_count_));
    } else if (IS_NULL(sub_table = MOD_NEW_OBJECT(memory::ModId::kColumnFamilySet, ColumnFamilyData, global_ctx_->options_))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to allocate memory for sub table", K(ret));
    } else if (FAILED(sub_table->init(dummy_args, global_ctx_, column_family_set_.get()))) {
      SE_LOG(WARN, "fail to deserialize subtable", K(ret));
    } else if (FAILED(sub_table->deserialize(buf, buf_size, pos))) {
      SE_LOG(WARN, "fail to deserialize partition group", K(ret));
    } else if (FAILED(ExtentSpaceManager::get_instance().open_table_space(sub_table->get_table_space_id()))) {
      SE_LOG(WARN, "fail to open table space", K(ret), "table_space_id", sub_table->get_table_space_id());
    } else if (FAILED(ExtentSpaceManager::get_instance().register_subtable(sub_table->get_table_space_id(), sub_table->GetID()))) {
      SE_LOG(WARN, "fail to register subtable", K(ret), "table_space_id", sub_table->get_table_space_id(), "index_id", sub_table->GetID());
    } else if (FAILED(column_family_set_->add_sub_table(sub_table))) {
      SE_LOG(WARN, "fail to add subtable to column family set", K(ret), "index_id", sub_table->GetID());
    } else if (FAILED(global_ctx_->all_sub_table_->add_sub_table(sub_table->GetID(), sub_table))) {
      SE_LOG(WARN, "fail to add subtable to AllSubTable", K(ret), "index_id", sub_table->GetID());
    } else {
      file_offset += buf_size;
    }
  }

  //resource clean
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int VersionSet::reserve_checkpoint_block_header(char *buf,
                                                int64_t buf_size,
                                                int64_t &offset,
                                                CheckpointBlockHeader *&block_header)
{
  int ret = Status::kOk;

  if (nullptr == buf || buf_size <= 0 || offset < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(buf_size), K(offset));
  } else {
    memset(buf, 0, buf_size);
    offset = 0;
    block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
    offset += sizeof(CheckpointBlockHeader);
    block_header->type_ = 1; //partition group meta block
    block_header->block_size_ = buf_size;
    block_header->entry_count_ = 0;
    block_header->data_size_ = 0;
    block_header->data_offset_ = offset;
  }

  return ret;
}

int VersionSet::replay_add_subtable_log(const char *log_data, int64_t log_length)
{
  int ret = Status::kOk;
  ChangeSubTableLogEntry log_entry;
  ColumnFamilyData *sub_table = nullptr;
  common::ColumnFamilyOptions cf_options(global_ctx_->options_);
  int64_t pos = 0;

  if (IS_NULL(log_data) || log_length <= 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(log_data), K(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    SE_LOG(WARN, "fail to deserialize add subtable log entry", K(ret), K(log_length));
  } else {
    //TODO:yuanfeng, create extent space?
    CreateSubTableArgs args(log_entry.table_schema_, cf_options, true, log_entry.table_space_id_);
    if (FAILED(add_sub_table(args, false /*write log*/, true /*is_replay*/, sub_table))) {
      SE_LOG(WARN, "fail to add subtable", K(ret), K(log_entry), K(args));
    } else if (FAILED(ExtentSpaceManager::get_instance().open_table_space(log_entry.table_space_id_))) {
      SE_LOG(WARN, "fail to create table space if not exist", K(ret), K(log_entry));
    } else if (FAILED(ExtentSpaceManager::get_instance().register_subtable(log_entry.table_space_id_, sub_table->GetID()))) {
      SE_LOG(WARN, "fail to register subtable", K(ret), "table_space_id", log_entry.table_space_id_, "index_id", sub_table->GetID());
    } else {
      SE_LOG(INFO, "success to replay add subtable", "index_id", sub_table->GetID());
    }
  }

  return ret;
}

int VersionSet::replay_remove_subtable_log(const char *log_data, int64_t log_length)
{
  int ret= Status::kOk;
  ChangeSubTableLogEntry log_entry;
  ColumnFamilyData *sub_table = nullptr;
  int64_t pos = 0;

  if (IS_NULL(log_data) || log_length < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(log_data), K(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    SE_LOG(WARN, "fail to deserialize remove subtable log entry", K(ret));
  } else if (IS_NULL(sub_table = column_family_set_->GetColumnFamily(log_entry.table_schema_.get_index_id()))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret), K(log_entry));
  } else if (FAILED(remove_sub_table(sub_table, false /*write log*/, true /*is_replay*/))) {
    SE_LOG(WARN, "fail to remove subtable", K(ret), K(log_entry));
  } else if (FAILED(ExtentSpaceManager::get_instance().unregister_subtable(sub_table->get_table_space_id(), sub_table->GetID()))) {
    SE_LOG(WARN, "fail to unregister subtbale", K(ret), K(log_entry),
        "table_space_id", sub_table->get_table_space_id());
  } else if (FAILED(sub_table->release_resource(true /*for_recovery*/))) {
    SE_LOG(WARN, "fail to release subtable", K(ret));
  } else {
    if (sub_table->Unref()) {
      //ugly, ColumnFamilySet will been replaced by AllSubtableMap
      MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      SE_LOG(INFO, "success to replay remove subtable", K(log_entry) );
    } else {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, subtable should not ref by other", K(ret), K(log_entry));
    }
  }

  return ret;
}

int VersionSet::replay_modify_subtable_log(const char *log_data, int64_t log_length)
{
  int ret = Status::kOk;
  ChangeInfo change_info;
  ModifySubTableLogEntry log_entry(change_info);
  ColumnFamilyData *sub_table = nullptr;
  int64_t pos = 0;

  if (IS_NULL(log_data) || log_length < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(log_data), KP(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    SE_LOG(WARN, "fail to deserialize log entry", K(ret));
  } else if (IS_NULL(sub_table = column_family_set_->GetColumnFamily(log_entry.index_id_))) {
    if (column_family_set_->is_subtable_dropped(log_entry.index_id_)) {
      SE_LOG(INFO, "subtable has been dropped, no need replay any more", K(log_entry));
    } else {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret), K(log_entry));
    }
  //} else if (TaskType::DUMP_TASK == change_info.task_type_ || TaskType::SWITCH_M02L0_TASK == change_info.task_type_) {
  //  if (FAILED(sub_table->apply_change_info_for_dump(log_entry.change_info_,
  //                                                   false /*write log*/,
  //                                                   true /*is replay*/,
  //                                                   &log_entry.recovery_point_))) {
  //    SE_LOG(WARN, "failed to apply change info for dump", K(ret), K(log_entry));
  //  } else {
  //    SE_LOG(INFO, "success to apply change info for dump", K(ret), K(log_entry));
  //  }
  } else if (FAILED(sub_table->apply_change_info(log_entry.change_info_,
                                                 false /*write log*/,
                                                 true /*is replay*/,
                                                 &log_entry.recovery_point_))) {
    SE_LOG(WARN, "fail to replay apply change info", K(ret), K(log_entry));
  } else {
    SE_LOG(INFO, "success to replay apply chang info", "index_id", log_entry.index_id_);
  }

  return ret;
}

int VersionSet::replay_modify_table_schema_log(const char *log_data, int64_t log_length)
{
  int ret = Status::kOk;
  ModifyTableSchemaLogEntry log_entry;
  ColumnFamilyData *subtable = nullptr;
  int64_t pos = 0;

  if (IS_NULL(log_data) || UNLIKELY(log_length < 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(log_data), K(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    SE_LOG(WARN, "fail to deserialize table schema log entry", K(ret));
  } else if (IS_NULL(subtable = column_family_set_->GetColumnFamily(log_entry.table_schema_.get_index_id()))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "subtable must not be nullptr", K(ret), K(log_entry));
  } else if (FAILED(subtable->modify_table_schema(log_entry.table_schema_))) {
    SE_LOG(WARN, "fail to modify table schema", K(ret), K(log_entry));
  } else {
    SE_LOG(INFO, "success to replay table schema log", K(log_entry));
  }

  return ret;
}

int VersionSet::recover_M02L0() {
  int ret = Status::kOk;

  /*TODO:@yuanfeng, may restructure the subtable manager mechanism in future
   *for fix bug#31657618
   * recover_M0L0 called when recovery for move dump layer to level0, no race condition here,
   * so can use global_ctx_->all_sub_table_->sub_table_map_ directly , like recovery add/remove subtable. */
  //SubTable* sub_table_picked = nullptr;
  //AllSubTable* all_sub_table = nullptr;
  //if (FAILED(global_ctx_->acquire_thread_local_all_sub_table(all_sub_table))) {
  //  SE_LOG(WARN, "fail to acquire all sub table", K(ret));
  //} else if (nullptr == all_sub_table) {
  //  ret = Status::kErrorUnexpected;
  //  SE_LOG(ERROR, "unexpected error, all sub table must not nullptr",
  //              K(ret));
  //} else {
    SubTableMap& all_subtables = global_ctx_->all_sub_table_->sub_table_map_;
    SubTable* sub_table = nullptr;
    for (auto iter = all_subtables.begin();
         Status::kOk == ret && iter != all_subtables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (FAILED(sub_table->recover_m0_to_l0())) {
        // subtable's memtable is empty, do nothing
        SE_LOG(INFO, "failed to recover m0 to l0", K(ret), K(iter->first));
      } else {
        SE_LOG(INFO, "success to recover m0 to l0", K(ret),
                    K(iter->first));
      }
    }
  //}

  //int tmp_ret = ret;
  //if (nullptr != global_ctx_ &&
  //    FAILED(global_ctx_->release_thread_local_all_sub_table(all_sub_table))) {
  //  SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  //}

  return ret;
}

#ifndef NDEBUG
void VersionSet::TEST_inject_write_checkpoint_failed()
{
  write_checkpoint_failed_ = true;
}

bool VersionSet::TEST_is_write_checkpoint_failed()
{
  return write_checkpoint_failed_;
}
#endif

}  // namespace db
}  // namespace xegnine
