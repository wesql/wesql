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
#pragma once

#include <atomic>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/db.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "env/env.h"
#include "memtable/memtable_list.h"
#include "memtable/memtablerep.h"
#include "options/db_options.h"
#include "util/arena.h"
#include "util/autovector.h"

namespace smartengine {

namespace util {
class Arena;
}

namespace storage
{
class MtExtCompaction;
}

namespace db {
/**TODO(Zhao Dongsheng): The future inheritance hierrarchy should be:
 *            Base FLush
 *               /   \
 *              /     \
 *             /       \
 *    FlushToLevel0  FlushToLevel1
 *           /  \
 *          /    \
 *         /      \
 *     FullFlush PartialFlush
 * And some member-variables like memtable_merge_iter_ and output_layer_position_
 * should be placed properly.*/
class BaseFlush {
 public:
  BaseFlush(ColumnFamilyData* cfd,
            const common::ImmutableDBOptions& db_options,
            const common::MutableCFOptions& mutable_cf_options,
            common::CompressionType output_compression,
            const std::atomic<bool>* shutting_down,
            common::SequenceNumber earliest_write_conflict_snapshot,
            JobContext& job_context,
            std::vector<common::SequenceNumber> &existing_snapshots,
            util::Directory* output_file_directory,
            const util::EnvOptions *env_options,
            memory::ArenaAllocator &arena);
  virtual ~BaseFlush();
  virtual int run(MiniTables& mtables) = 0;
  virtual int prepare_flush_task(MiniTables &mtables) = 0;
  virtual void pick_memtable() = 0;
  int after_run_flush(MiniTables &mtables, int ret, monitor::InstrumentedMutex *mutex);
  void cancel();
  // for test
  void set_memtables(util::autovector<MemTable*> &mems) {
    mems_ = mems;
  }
  TaskType get_task_type() {
    return job_context_.task_type_;
  }
  util::autovector<MemTable*> &get_picked_memtable() {
    return mems_;
  }

 protected:
  typedef util::autovector<table::InternalIterator *> MemtableIterators;

  virtual table::InternalIterator *create_memtable_iterator(const common::ReadOptions &read_options,
                                                           MemTable *memtable) = 0;
  int build_memtable_iterators(MemtableIterators &memtable_iterators);
  int build_memtable_merge_iterator(table::InternalIterator *&memtable_merge_iterator);
  int get_memtable_stats(int64_t &memory_usage, int64_t &num_entries, int64_t &num_deletes);
  int flush_data(const storage::LayerPosition &output_layer_position,
                 const bool is_flush,
                 MiniTables &mini_tables);
  int write_level0(MiniTables &mini_tables);
  int fill_table_cache(const MiniTables &mini_tables);
  int delete_old_M0(const InternalKeyComparator *internal_comparator, MiniTables &mini_tables);
  int stop_record_flush_stats(const int64_t bytes_written,
                              const uint64_t start_micros);
  void upload_current_flush_stats_to_global(uint64_t bytes_written,
                                            uint64_t start_micros,
                                            uint64_t end_micros);
  void RecordFlushIOStats();


 protected:
  ColumnFamilyData* cfd_;
  const common::ImmutableDBOptions& db_options_;
  const common::MutableCFOptions& mutable_cf_options_;
  common::CompressionType output_compression_;
  const std::atomic<bool>* shutting_down_;
  common::SequenceNumber earliest_write_conflict_snapshot_;
  JobContext &job_context_;
  util::autovector<MemTable*> mems_;
  std::vector<common::SequenceNumber> existing_snapshots_;
  util::Directory* output_file_directory_;
  const util::EnvOptions *env_options_;
  memory::ArenaAllocator &arena_;
  util::Arena tmp_arena_;
  bool pick_memtable_called_;
};

class FlushJob : public BaseFlush {
 public:
  FlushJob(const std::string& dbname,
           ColumnFamilyData* cfd,
           const common::ImmutableDBOptions& db_options,
           JobContext& job_context,
           util::Directory* output_file_directory,
           common::CompressionType output_compression,
           monitor::Statistics* stats,
           storage::CompactionContext &context,
           memory::ArenaAllocator &arena);

  virtual ~FlushJob() override;

  // Require db_mutex held.
  // Once PickMemTable() is called, either Run() or Cancel() has to be call.
  virtual void pick_memtable() override;
  virtual int prepare_flush_task(MiniTables &mtables) override;
  virtual int run(MiniTables& mtables) override;
  void cancel();
  void set_meta_snapshot(const db::Snapshot *meta_snapshot) {
    meta_snapshot_ = meta_snapshot;
  }
  storage::MtExtCompaction *get_compaction() const {
    return compaction_;
  }

 protected:
  virtual table::InternalIterator *create_memtable_iterator(const common::ReadOptions &read_options,
                                                            MemTable *memtable) override;

 private:
  void ReportStartedFlush();
  void ReportFlushInputSize(const util::autovector<MemTable*>& mems);
  int prepare_flush_level1_task();
  int get_memtable_range(util::autovector<table::InternalIterator*> &memtables,
                         storage::Range &wide_range);
  int build_mt_ext_compaction(
      util::autovector<table::InternalIterator*> &memtables,
      storage::RangeIterator *l1_iterator);
  int build_l1_iterator(storage::Range &wide_range, storage::RangeIterator *&iterator);
  int create_meta_iterator(const storage::LayerPosition &layer_position,
                           table::InternalIterator *&iterator);
  int parse_meta(const table::InternalIterator *iter, storage::ExtentMeta *&extent_meta);
  int run_mt_ext_task(MiniTables &mtables);

 private:
  const std::string& dbname_;
  monitor::Statistics* stats_;
  storage::CompactionContext compaction_context_;
  const db::Snapshot* meta_snapshot_;
  storage::MtExtCompaction *compaction_;
};
}
}  // namespace smartengine
