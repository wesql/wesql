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
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#pragma once
#include <atomic>
#include <limits>
#include <memory>
#include <string>

#include "db/column_family.h"
#include "db/file_number.h"
#include "env/env.h"
#include "options/db_options.h"
#include "storage/storage_manager.h"

namespace smartengine
{
namespace storage
{
struct CheckpointBlockHeader;
struct CheckpointHeader;
}

namespace db
{
class WriteBufferManager;
class ColumnFamilySet;

struct AllSubTable {
public:
  static int DUMMY;
  static void *const kAllSubtableInUse;
  static void *const kAllSubtableObsolete;

public:
  int64_t version_number_;
  std::atomic<int64_t> refs_;
  std::mutex *all_sub_table_mutex_;
  SubTableMap sub_table_map_;


  AllSubTable();
  AllSubTable(SubTableMap &sub_table_map, std::mutex *mutex);
  ~AllSubTable();
  void reset();
  void ref() {refs_.fetch_add(1, std::memory_order_relaxed); }
  bool unref() { return 1 == refs_.fetch_sub(1); }
  int add_sub_table(int64_t index_id, SubTable *sub_table);
  int remove_sub_table(int64_t index_id);
  int get_sub_table(int64_t, SubTable *&sub_table);
};

class AllSubTableGuard
{
public:
  AllSubTableGuard() = delete;
  AllSubTableGuard(const AllSubTableGuard &guard) = delete;
  AllSubTableGuard(GlobalContext *global_ctx);
  ~AllSubTableGuard();
  const SubTableMap &get_subtable_map()
  {
    return all_sub_table_->sub_table_map_;
  }
private:
  GlobalContext *global_ctx_;
  AllSubTable *all_sub_table_;
};

class VersionSet {
public:
  VersionSet(const std::string& dbname,
             const common::ImmutableDBOptions* db_options,
             const util::EnvOptions& env_options, cache::Cache* table_cache,
             WriteBufferManager* write_buffer_manager);
  ~VersionSet();

  int init(GlobalContext *global_ctx);
  int recover_extent_space_manager();

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_.increase(1); }

  // Return the last sequence number.
  uint64_t LastSequence() const {
    return last_sequence_.load(std::memory_order_acquire);
  }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_.store(s, std::memory_order_release);
  }

  uint64_t LastAllocatedSequence() const {
    return last_allocated_sequence_.load(std::memory_order_acquire);
  }

  void SetLastAllocatedSequence(uint64_t s) {
    assert(s >= last_allocated_sequence_);
    assert(s >= last_sequence_);
    last_allocated_sequence_.store(s, std::memory_order_release);
  }

  uint64_t AllocateSequence(uint64_t sequence_size) {
    return last_allocated_sequence_.fetch_add(sequence_size);
  }
  // Mark the specified file number as used.
  // REQUIRED: this is only called during single-threaded recovery
  void MarkFileNumberUsedDuringRecovery(uint64_t number);

  // Returns the minimum log number such that all
  // log numbers less than or equal to it can be deleted
  uint64_t MinLogNumber() const
  {
    uint64_t log_number = UINT64_MAX;
    uint64_t min_log_number = UINT64_MAX;
    int64_t index_id = 0;

    for (auto sub_table : *column_family_set_) {
      log_number = static_cast<uint64_t>(sub_table->get_recovery_point().log_file_number_);
      if (0 == sub_table->GetID()) {
        //default cf is only used in unittest,
        //so the normal recyle wal point need not include the default_cf's recovery point
      } else if (!sub_table->IsDropped() && min_log_number > log_number) {
        // It's safe to ignore dropped column families here:
        // cfd->IsDropped() becomes true after the drop is persisted in manifest.
        min_log_number = log_number;
        index_id = sub_table->GetID();
      }
    }

    SE_LOG(INFO, "CK_INFO: min log number", K(index_id), K(min_log_number));
  
    if (UINT64_MAX == min_log_number) {
      //reach here, indicate that only default_cf exist
      //only occur when bootstrap, because system_cf will been created after bootstrap
      //here, reset the min_log_number to zero, prevent recycle all wal files
      min_log_number = 0;
    }
  
    return min_log_number;
  }

  // Return the approximate size of data to be scanned for range [start, end)
  // in levels [start_level, end_level). If end_level == 0 it will search
  // through all non-empty levels
  uint64_t ApproximateSize(ColumnFamilyData* cfd, 
                           const db::Snapshot *sn,
                           const common::Slice& start,
                           const common::Slice& end,
                           int start_level,
                           int end_level,
                           int64_t estimate_cost_depth);

  ColumnFamilySet* GetColumnFamilySet() { return column_family_set_.get(); }
  GlobalContext *get_global_ctx() const { return global_ctx_; }

  const util::EnvOptions& env_options() { return env_options_; }

  // hot backup
  int create_backup_snapshot(MetaSnapshotSet &meta_snapshot);

  const common::ImmutableDBOptions* get_db_options() const { return db_options_; }

  int add_sub_table(CreateSubTableArgs &args, bool write_log, bool is_replay, ColumnFamilyData *&sub_table);
  int remove_sub_table(ColumnFamilyData *sub_table, bool write_log, bool is_replay);
  int modify_table_schema(ColumnFamilyData *sub_table, const schema::TableSchema &table_schema);

  int write_checkpoint_block(util::WritableFile &checkpoint_writer,
                             int64_t &meta_block_count,
                             CheckpointBlockHeader *block_header,
                             char *buf,
                             int64_t &offset,
                             void *pointer,
                             int pointer_type);
  int write_subtable(util::WritableFile &checkpoint_writer,
                     int64_t &sub_table_meta_block_count,
                     storage::CheckpointBlockHeader *block_header,
                     char *buf,
                     int64_t &offset,
                     SubTable *sub_table);
  int write_meta_snapshot(util::WritableFile &checkpoint_writer,
                          int64_t &meta_snapshot_meta_block_count,
                          CheckpointBlockHeader *block_header,
                          char *buf,
                          int64_t &offset,
                          SnapshotImpl *meta_snapshot);

  int write_backup_snapshots(util::WritableFile &checkpoint_writer, storage::CheckpointHeader &header, char *buf);
  int write_current_checkpoint(util::WritableFile &checkpoint_writer, storage::CheckpointHeader &header, char *buf);
  int do_checkpoint(util::WritableFile *checkpoint_writer, storage::CheckpointHeader *header);
  int load_all_sub_table(util::RandomAccessFile &checkpoint_reader,
                         storage::CheckpointHeader &header,
                         char *buf,
                         int64_t buf_size);
  int load_backup_snapshots(util::RandomAccessFile &checkpoint_reader,
                            storage::CheckpointHeader &header,
                            char *buf,
                            int64_t buf_size);
  int load_checkpoint(util::RandomAccessFile *checkpoint_reader, storage::CheckpointHeader *header);

  int replay_backup_snapshot_log(int64_t log_type, char *log_data, int64_t log_len);
  int replay(int64_t log_type, char *log_data, int64_t log_len);
  int recover_M02L0();

#ifndef NDEBUG
  void TEST_inject_write_checkpoint_failed();
  bool TEST_is_write_checkpoint_failed();
#endif
 private:
  int write_big_block(util::WritableFile &checkpoint_writer, void *pointer, int pointer_type);
  int write_big_meta_snapshot(util::WritableFile &checkpoint_writer, SnapshotImpl &meta_snapshot);
  int write_big_subtable(util::WritableFile &checkpoint_writer, ColumnFamilyData &sub_table);
  int read_big_subtable(util::RandomAccessFile *checkpoint_reader, int64_t block_size, int64_t &file_offset);
  int read_big_meta_snapshot(util::RandomAccessFile &checkpoint_reader,
                             int64_t block_size,
                             int64_t &file_offset,
                             MetaSnapshotSet &backup_snapshot);
  int reserve_checkpoint_block_header(char *buf,
                                      int64_t buf_size,
                                      int64_t &offset,
                                      storage::CheckpointBlockHeader *&block_header);
  int replay_add_subtable_log(const char *log, int64_t log_len);
  int replay_remove_subtable_log(const char *log, int64_t log_len);
  int replay_modify_subtable_log(const char *log_data, int64_t log_len);
  int replay_modify_table_schema_log(const char *log_data, int64_t log_len);

  int replay_accquire_snapshot_log(const char *log_data, int64_t log_length);
  int replay_release_snapshot_log(const char *log_data, int64_t log_length);

 private:
  static const int64_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; //2MB

private:

  bool is_inited_;
  GlobalContext *global_ctx_;
  std::unique_ptr<ColumnFamilySet, memory::ptr_destruct_delete<ColumnFamilySet>> column_family_set_;

  util::Env* const env_;
  const std::string dbname_;
  const common::ImmutableDBOptions* const db_options_;
  FileNumber next_file_number_;
  std::atomic<uint64_t> last_sequence_;
  std::atomic<uint64_t> last_allocated_sequence_;  // when shutdown
                                                   // allocated_sequence ==
                                                   // last_sequence_;

  // env options for all reads and writes except compactions
  const util::EnvOptions& env_options_;

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);

#ifndef NDEBUG
  bool write_checkpoint_failed_;
#endif
};
}
}  // namespace smartengine
