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

#include "db/db.h"

#include <atomic>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "db/batch_group.h"
#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/flush_job.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/pipline_queue_manager.h"
#include "db/snapshot_impl.h"
#include "env/env.h"
#include "memtable/memtable_list.h"
#include "memtable/memtablerep.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "storage/extent_space_manager.h"
#include "storage/storage_manager.h"
#include "storage/storage_common.h"
#include "util/concurrent_hash_map.h"
#include "util/file_name.h"
#include "util/thread_local.h"
#include "util/heap.h"
#include "transactions/transaction.h"

namespace smartengine {
namespace storage {
class CompactionJob;
class Compaction;
class ShrinkExtentSpacesJobTest;
}

namespace util {
class TransactionImpl;
class Arena;
}

namespace table {
struct ExternalSstFileInfo;
}

namespace db {
class MemTable;
class TableCache;
class VersionSet;
class WriteCallback;
class ReplayThreadPool;
struct JobContext;

enum TrimMemFlushState {
  kFlushWaited = 0,
  kFlushDone = 1
};

struct GlobalContext
{
  // declare
  static util::Env *env_;
  static cache::Cache *cache_;

  std::string db_name_;
  common::Options options_;
  util::EnvOptions env_options_;
  WriteBufferManager *write_buf_mgr_;
  std::mutex all_sub_table_mutex_;
  std::atomic<int64_t> version_number_;
  std::unique_ptr<util::ThreadLocalPtr, memory::ptr_destruct_delete<util::ThreadLocalPtr>> local_all_sub_table_;
  AllSubTable *all_sub_table_;
  util::Directory *db_dir_;

  GlobalContext() = delete;
  GlobalContext(const std::string &db_name, const common::Options &options);
  ~GlobalContext();

  static cache::Cache *get_cache() { return GlobalContext::cache_; }
  static util::Env *get_env() { return GlobalContext::env_; }
  static void set_env(util::Env *env) { GlobalContext::env_ = env; }

  bool is_valid();
  void reset();
  int acquire_thread_local_all_sub_table(AllSubTable *&all_sub_table);
  int release_thread_local_all_sub_table(AllSubTable *all_sub_table);
  //thread unsafe, need protect by all_sub_table_mutex_
  int install_new_all_sub_table(AllSubTable *all_sub_table);
  //thread unsafe, need protect by all_sub_table_mutex_
  void reset_thread_local_all_sub_table();
};

struct RecoveryDebugInfo
{
  int64_t prepare;
  int64_t prepare_external_write_ckpt;
  int64_t prepare_create_default_subtable;

  int64_t recovery;
  int64_t recovery_slog_replay;
  int64_t recovery_wal;

  int64_t recoverywal;
  int64_t recoverywal_before;
  int64_t recoverywal_wal_files;
  int64_t recoverywal_after;
  int64_t recoverywal_file_count;
  int64_t recoverywal_slowest;

  std::string show();
};

struct Top3ModMemInfo {
  uint64_t top1 = 0;
  uint64_t top2 = 0;
  uint64_t top3 = 0;
};

struct ShrinkArgs
{
  DBImpl *db_;
  bool auto_shrink_;
  int64_t total_max_shrink_extent_count_;
  std::vector<storage::ShrinkInfo> shrink_infos_;

  ShrinkArgs()
      : db_(nullptr),
        auto_shrink_(false),
        total_max_shrink_extent_count_(0),
        shrink_infos_()
  {
  }
  ShrinkArgs(DBImpl *db, bool auto_shrink, int64_t total_max_shrink_extent_count, const std::vector<storage::ShrinkInfo> &shrink_infos)
      : db_(db),
        auto_shrink_(auto_shrink),
        total_max_shrink_extent_count_(total_max_shrink_extent_count),
        shrink_infos_(shrink_infos)
  {
  }
  ~ShrinkArgs()
  {
  }

  bool is_valid() const
  {
    return nullptr != db_ && total_max_shrink_extent_count_ > 0 && shrink_infos_.size() > 0;
  }
  DECLARE_AND_DEFINE_TO_STRING(KVP_(db), KV_(auto_shrink), KV_(total_max_shrink_extent_count))
};

class DBImpl : public DB {
 public:
  static const int32_t MAX_COMPACTION_HISTORY_CNT = 128;
  DBImpl(const common::DBOptions& options, const std::string& dbname);
  virtual ~DBImpl() override;

  // Implementations of the DB interface
  virtual common::Status Put(const common::WriteOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& value) override;

  virtual common::Status Delete(const common::WriteOptions& options,
                                ColumnFamilyHandle* column_family,
                                const common::Slice& key) override;

  virtual common::Status SingleDelete(const common::WriteOptions& options,
                                      ColumnFamilyHandle* column_family,
                                      const common::Slice& key) override;
  using DB::Write;
  virtual common::Status Write(const common::WriteOptions& options,
                               WriteBatch* updates) override;

  using DB::WriteAsync;
  virtual common::Status WriteAsync(const common::WriteOptions &options,
                                    WriteBatch *updates,
                                    common::AsyncCallback *call_back) override;

  using DB::Get;
  virtual common::Status Get(const common::ReadOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             common::PinnableSlice* value) override;

  virtual common::Status CreateColumnFamily(CreateSubTableArgs &args, ColumnFamilyHandle** handle) override;

  virtual common::Status DropColumnFamily(ColumnFamilyHandle *column_family) override;

  virtual int modify_table_schema(ColumnFamilyHandle *subtable_handle, const schema::TableSchema &table_schema) override;  

  virtual Iterator* NewIterator(const common::ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;
  virtual const Snapshot* GetSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;

  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const common::Slice& property,
                           std::string* value) override;
  using DB::GetMapProperty;
  virtual bool GetMapProperty(ColumnFamilyHandle* column_family,
                              const common::Slice& property,
                              std::map<std::string, double>* value) override;
  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const common::Slice& property,
                              uint64_t* value) override;
  using DB::GetAggregatedIntProperty;
  virtual bool GetAggregatedIntProperty(const common::Slice& property,
                                        uint64_t* aggregated_value) override;
  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(
      ColumnFamilyHandle* column_family, const Range* range, int n,
      uint64_t* sizes, uint8_t include_flags = INCLUDE_FILES) override;
  using DB::GetApproximateMemTableStats;
  virtual void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                           const Range& range,
                                           uint64_t* const count,
                                           uint64_t* const size) override;
  using DB::CompactRange;
  virtual common::Status CompactRange(ColumnFamilyHandle* column_family,
                                      const uint32_t compact_type) override;

  virtual common::Status CompactRange(const uint32_t manual_compact_type) override;

  virtual common::Status PauseBackgroundWork() override;
  virtual common::Status ContinueBackgroundWork() override;
  virtual void CancelAllBackgroundWork(bool wait) override;

  virtual common::Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;

  virtual int reset_pending_shrink(const uint64_t subtable_id) override;

  using DB::SetOptions;
  common::Status SetOptions(
      const std::unordered_map<std::string, std::string>& options_map) override;
  common::Status SetOptions(
      ColumnFamilyHandle* column_family,
      const std::unordered_map<std::string, std::string>& options_map) override;

  virtual common::Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& options_map) override;

  virtual const std::string& GetName() const override;
  virtual util::Env* GetEnv() const override;
  using DB::GetDBOptions;
  virtual common::DBOptions GetDBOptions() const override;
  using DB::Flush;
  virtual common::Status Flush(const common::FlushOptions& options,
                               ColumnFamilyHandle* column_family) override;
  virtual int sync_wal() override;

  virtual common::SequenceNumber GetLatestSequenceNumber() const override;

  virtual common::Status DisableFileDeletions() override;
  virtual common::Status EnableFileDeletions(bool force) override;
  virtual int IsFileDeletionsEnabled() const;

  virtual bool IsObjectStoreMode() const override;
  virtual bool IsFileMode() const override;

  virtual int create_backup_snapshot(BackupSnapshotId backup_id,
                                     MetaSnapshotSet &meta_snapshots,
                                     int64_t &last_manifest_file_num,
                                     uint64_t &last_manifest_file_size,
                                     uint64_t &last_wal_file_num,
                                     BinlogPosition &last_binlog_pos) override;

  virtual int record_incremental_extent_ids(const std::string &backup_tmp_dir_path,
                                            const int64_t first_manifest_file_num,
                                            const int64_t last_manifest_file_num,
                                            const uint64_t last_manifest_file_size) override;

  virtual int shrink_table_space(int32_t table_space_id) override;

  virtual int get_all_subtable(
      std::vector<smartengine::db::ColumnFamilyHandle*>& subtables) const override;

  virtual int return_all_subtable(std::vector<smartengine::db::ColumnFamilyHandle*> &subtables) override;

  virtual int get_data_file_stats(std::vector<storage::DataFileStatistics> &data_file_stats) override
  {
    return storage::ExtentSpaceManager::get_instance().get_data_file_stats(data_file_stats);
  }

  VersionSet* get_version_set() { return versions_.get(); }

  // Similar to Write() but will call the callback once on the single write
  // thread to determine whether it is safe to perform the write.
  virtual common::Status WriteWithCallback(
      const common::WriteOptions& write_options, WriteBatch* my_batch,
      WriteCallback* callback);

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into the current
  // memtables. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  //
  // If include_history=true, will also search Memtables in MemTableList
  // History.
  common::SequenceNumber GetEarliestMemTableSequenceNumber(
      SuperVersion* sv, bool include_history);

  // For a given key, check to see if there are any records for this key
  // in the memtables, including memtable history.  If cache_only is false,
  // SST files will also be checked.
  //
  // If a key is found, *found_record_for_key will be set to true and
  // *seq will be set to the stored sequence number for the latest
  // operation on this key or kMaxSequenceNumber if unknown.
  // If no key is found, *found_record_for_key will be set to false.
  //
  // Note: If cache_only=false, it is possible for *seq to be set to 0 if
  // the sequence number has been cleared from the record.  If the caller is
  // holding an active db snapshot, we know the missing sequence must be less
  // than the snapshot's sequence number (sequence numbers are only cleared
  // when there are no earlier active snapshots).
  //
  // If NotFound is returned and found_record_for_key is set to false, then no
  // record for this key was found.  If the caller is holding an active db
  // snapshot, we know that no key could have existing after this snapshot
  // (since we do not compact keys that have an earlier snapshot).
  //
  // Returns OK or NotFound on success,
  // other status on unexpected error.
  // TODO(andrewkr): this API need to be aware of range deletion operations
  common::Status GetLatestSequenceForKey(SuperVersion* sv,
                                         const common::Slice& key,
                                         bool cache_only,
                                         common::SequenceNumber* seq,
                                         bool* found_record_for_key);

  int get_latest_seq_for_uk(ColumnFamilyHandle *column_family,
                            const common::ReadOptions *read_opts,
                            const common::Slice &key,
                            common::SequenceNumber &seq);

  // Similar to GetSnapshot(), but also lets the db know that this snapshot
  // will be used for transaction write-conflict checking.  The DB can then
  // make sure not to compact any keys that would prevent a write-conflict from
  // being detected.
  const Snapshot* GetSnapshotForWriteConflictBoundary();

#ifndef NDEBUG
  // Extra methods (for testing) that are not in the public DB interface
  // Implemented in db_impl_debug.cc

  void TEST_HandleWALFull();

  bool TEST_UnableToFlushOldestLog() { return unable_to_flush_oldest_log_; }

  bool TEST_IsLogGettingFlushed() {
    return alive_log_files_.begin()->getting_flushed;
  }

  // Force current memtable contents to be flushed.
  common::Status TEST_FlushMemTable(bool wait = true,
                                    ColumnFamilyHandle* cfh = nullptr);

  // Wait for memtable compaction
  common::Status TEST_WaitForFlushMemTable(
      ColumnFamilyHandle* column_family = nullptr);

  // Wait for any compaction
  common::Status TEST_WaitForCompact();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes(
      ColumnFamilyHandle* column_family = nullptr);

  // get total level0 file size. Only for testing.
  uint64_t TEST_GetLevel0TotalSize();

  void TEST_GetFilesMetaData(ColumnFamilyHandle* column_family,
                             std::vector<std::vector<FileMetaData>>* metadata);

  void TEST_GetExtentMeta(
      ColumnFamilyHandle* column_family,
      std::vector<std::vector<storage::ExtentMeta>>* metadata);

  void TEST_LockMutex();

  void TEST_UnlockMutex();

  uint64_t TEST_LogfileNumber();

  uint64_t TEST_total_log_size() const { return total_log_size_; }

  // Return the lastest common::MutableCFOptions of of a column family
  common::Status TEST_GetLatestMutableCFOptions(
      ColumnFamilyHandle* column_family,
      common::MutableCFOptions* mutable_cf_opitons);

  cache::Cache* TEST_table_cache() { return table_cache_.get(); }

  uint64_t TEST_FindMinLogContainingOutstandingPrep();
  uint64_t TEST_FindMinPrepLogReferencedByMemTable();

  int TEST_BGCompactionsAllowed() const;
  int TEST_create_subtable(const ColumnFamilyDescriptor& cf,
                           int32_t tid,
                           ColumnFamilyHandle*& handle);
  int TEST_modify_table_schema(ColumnFamilyHandle *handle);

  //inject error in transaction pipline
  //pipline should exit 
  bool copy_log_fail_flag_ = false;
  void TEST_inject_pipline_error_copy_log_fail() {
    copy_log_fail_flag_ = true;
  }
  bool TEST_if_copy_log_fail() {
    return copy_log_fail_flag_;
  }

  void TEST_inject_pipline_error_flush_wal_fail() {
    this->pipline_global_error_flag_.store(true);
  }

  bool write_memtable_fail_flag_ = false;
  void TEST_inject_pipline_error_write_memtable_fail() {
    write_memtable_fail_flag_ = true;
  }
  bool TEST_if_write_memtable_fail() {
    return write_memtable_fail_flag_;
  }

  bool commit_fail_flag_ = false;
  void TEST_inject_pipline_error_commit_fail() {
    commit_fail_flag_ = true;
  }
  bool TEST_if_commit_fail() {
    return commit_fail_flag_;
  }

  uint64_t TEST_get_max_sequence_during_recovery() {
    return max_sequence_during_recovery_.load();
  }

  template <typename Compare>
  int TEST_pick_and_switch_subtables(const SubTableMap& all_sub_tables,
                               std::set<uint32_t>& picked_cf_ids,
                               uint64_t expected_pick_num,
                               Compare& cmp,
                               uint64_t* picked_num,
                               util::BinaryHeap<SubTable*, Compare> *picked_sub_tables) {
    std::list<SubTable*> switched_sub_tables;
    return pick_and_switch_subtables(all_sub_tables, picked_cf_ids,
                      expected_pick_num, cmp, picked_num, picked_sub_tables,
                      switched_sub_tables);
  }
  bool TEST_trigger_switch_memtable_ = false;
  bool TEST_triggered_ = false;
  bool TEST_avoid_flush_ = false;
  void TEST_inject_version_set_write_checkpoint()
  {
    versions_->TEST_inject_write_checkpoint_failed();
  }

  void TEST_schedule_shrink()
  {
    schedule_shrink();
  }

  int TEST_get_data_file_stats(const int64_t table_space_id, std::vector<storage::DataFileStatistics> &data_file_stats)
  {
    return storage::ExtentSpaceManager::get_instance().get_data_file_stats(table_space_id, data_file_stats);
  }

  bool TEST_after_create_backup_snapshot_ = false;
  void TEST_set_after_create_backup_snapshot(bool val)
  {
    TEST_after_create_backup_snapshot_ = val;
  }
  bool TEST_get_after_create_backup_snapshot() const { return TEST_after_create_backup_snapshot_; }

#endif  // NDEBUG

  // Return maximum background compaction allowed to be scheduled based on
  // compaction status.
  int BGCompactionsAllowed() const;

  int bg_dumps_allowed() const;

  //main thread for smartengine
  //in this thread  we will trigger background job when system is in idle 
  //make sure you job can be unscheduled before schedule it in main thread
  bool db_shutting_down() {
    return shutting_down_.load(std::memory_order_acquire);
  }

  bool master_thread_running() {
    return master_thread_running_.load(std::memory_order_acquire);
  }  
  void schedule_master_thread();
  static void bg_master_func_wrapper(void* db);
  void bg_master_thread_func(void);

  uint64_t MinLogNumberToKeep();

  // Returns the list of live files in 'live' and the list
  // of all files in the filesystem in 'candidate_files'.
  // If force == false and the last call was less than
  // db_options_.delete_obsolete_files_period_micros microseconds ago,
  // it will not fill up the job_context
  void FindObsoleteFiles(JobContext* job_context, bool force,
                         bool no_full_scan = false);

  // Diffs the files listed in filenames and those that do not
  // belong to live files are posibly removed. Also, removes all the
  // files in sst_delete_files and log_delete_files.
  // It is not necessary to hold the mutex when invoking this method.
  void PurgeObsoleteFiles(const JobContext& background_contet,
                          bool schedule_only = false);

  common::Status switch_major_compaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles, bool flag) override;

  common::Status disable_backgroud_merge(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;

  common::Status enable_backgroud_merge(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;


  void SchedulePurge();

  ColumnFamilyHandle* DefaultColumnFamily() const override;

  // for hot backup
  virtual int do_manual_checkpoint(int64_t &start_manifest_file_num) override;

  // const SnapshotList& snapshots() const { return snapshots_; }
  uint64_t snapshots_count() const;
  int64_t GetOldestSnapshotTime() const;
  bool snapshot_empty();
  const common::ImmutableDBOptions& immutable_db_options() const {
    return immutable_db_options_;
  }

  // Find Super version and reference it. Based on options, it might return
  // the thread local cached one.
  // Call ReturnAndCleanupSuperVersion() when it is no longer needed.
  virtual SuperVersion* GetAndRefSuperVersion(ColumnFamilyData* cfd) override;

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.
  SuperVersion* GetAndRefSuperVersion(uint32_t column_family_id);

  // Un-reference the super version and return it to thread local cache if
  // needed. If it is the last reference of the super version. Clean it up
  // after un-referencing it.
  virtual void ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd, SuperVersion* sv) override;

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread.
  void ReturnAndCleanupSuperVersion(uint32_t colun_family_id, SuperVersion* sv);

  // Returns the number of currently running flushes.
  // REQUIREMENT: mutex_ must be held when calling this function.
  int num_running_flushes() {
    mutex_.AssertHeld();
    return num_running_flushes_;
  }

  // Returns the number of currently running compactions.
  // REQUIREMENT: mutex_ must be held when calling this function.
  int num_running_compactions() {
    mutex_.AssertHeld();
    return num_running_compactions_;
  }

  // hollow transactions shell used for recovery.
  // these will then be passed to TransactionDB so that
  // locks can be reacquired before writing can resume.
  struct RecoveredTransaction {
    std::string name_;
    util::Transaction::TransactionState state_;
    uint64_t prepare_log_num_;
    common::SequenceNumber prepare_seq_;
    uint64_t commit_log_num_;
    common::SequenceNumber commit_seq_;
    WriteBatch* batch_;
    explicit RecoveredTransaction(const std::string& name,
                                  util::Transaction::TransactionState state,
                                  const uint64_t prepare_log_num,
                                  const common::SequenceNumber prepare_seq,
                                  const uint64_t commit_log_num = 0,
                                  const common::SequenceNumber commit_seq = 0,
                                  WriteBatch* batch = nullptr)
        : name_(name), state_(state), prepare_log_num_(prepare_log_num),
          prepare_seq_(prepare_seq), commit_log_num_(commit_log_num),
          commit_seq_(commit_seq), batch_(batch) {}
    ~RecoveredTransaction();
  };

  struct CommitWriteBatch {
    std::list<std::string> uncommit_transaction_list_;
    WriteBatch* batch_;
  };

  bool allow_2pc() const { return immutable_db_options_.allow_2pc; }

  void lock_recovered_transaction_mutex() {
    recovered_transaction_mutex_.lock();
  }
  void unlock_recovered_transaction_mutex() {
    recovered_transaction_mutex_.unlock();
  }
  util::ConcurrentHashMap<std::string, RecoveredTransaction*>* recovered_transactions() {
    return &recovered_transactions_;
  }
  bool insert_prepare_sequence_into_xid_map(const std::string& xid, common::SequenceNumber seq);
  bool get_prepare_sequence_from_xid_map(const std::string& xid, common::SequenceNumber *seq);
  bool insert_recovered_transaction(const std::string& name,
                                    RecoveredTransaction *transaction,
                                    bool mark_log);
  bool delete_recovered_transaction(const std::string& name, bool unmark_log);
  void delete_all_recovered_transactions();
  RecoveredTransaction *get_recovered_transaction(const std::string& name);

  void insert_commit_transaction_groups(common::SequenceNumber commit_seq,
                                        std::list<std::string> &uncommit_transaction_list,
                                        WriteBatch *write_batch) {
    assert(commit_transactions_.find(commit_seq) == commit_transactions_.end());
    CommitWriteBatch *commit_write_batch = MOD_NEW_OBJECT(memory::ModId::kRecovery, CommitWriteBatch);
    commit_write_batch->uncommit_transaction_list_.assign(uncommit_transaction_list.begin(), uncommit_transaction_list.end());
    commit_write_batch->batch_ = write_batch;
    commit_transactions_.emplace(commit_seq, commit_write_batch);
  }
  void remove_commit_transaction_groups(common::SequenceNumber commit_seq) {
    auto it = commit_transactions_.find(commit_seq);
    if (it == commit_transactions_.end()) {
      return;
    }
    commit_transactions_.erase(it);
  }
  CommitWriteBatch *get_commit_write_batch(common::SequenceNumber commit_seq) {
    auto it = commit_transactions_.find(commit_seq);
    if (it == commit_transactions_.end()) {
      return nullptr;
    } else {
      return it->second;
    }
  }

  void set_global_binlog_pos(const char *file, uint64_t offset) {
    BinlogPosition binlog_pos(file, offset);
    mutex_.Lock();

    if (global_binlog_pos_.compare(binlog_pos) < 0)
      global_binlog_pos_ = binlog_pos;

    mutex_.Unlock();
  }
  BinlogPosition *get_global_binlog_pos() { return &global_binlog_pos_; }

  void MarkLogAsHavingPrepSectionFlushed(uint64_t log);
  void MarkLogAsContainingPrepSection(uint64_t log);

  public:
  std::list<storage::CompactionJobStatsInfo*> &get_compaction_history(std::mutex **mutex,
                                               storage::CompactionJobStatsInfo **sum) override {
    *mutex = &compaction_history_mutex_;
    *sum = &compaction_sum_;
    return compaction_history_;
  }

  // rowcache related
  int get_from_row_cache(const ColumnFamilyData *cfd,
                         const common::SequenceNumber snapshot,
                         const common::Slice& key,
                         IterKey &row_cache_key,
                         common::PinnableSlice *&pinnable_val,
                         bool &done);
  int add_into_row_cache(const void* data,
                         const size_t data_size,
                         const common::SequenceNumber snapshot,
                         const db::ColumnFamilyData *cfd,
                         const uint64_t key_seq,
                         IterKey &row_cache_key);

  int parallel_recovery_write_memtable(WriteBatch &batch,
                                       uint64_t current_log_file_number,
                                       uint64_t *next_allocate_sequence);
  int update_max_sequence_and_log_number(uint64_t next_allocate_sequence,
                                         uint64_t current_log_file_number);
  bool check_if_need_switch_memtable();
  int switch_memtable_during_parallel_recovery(std::list<SubTable*>& switched_sub_tables);
  int flush_memtable_during_parallel_recovery(std::list<SubTable*>& switched_sub_tables);
  int deal_with_log_record_corrution(common::WALRecoveryMode recovery_mode,
                                     const std::string file_name,
                                     bool is_last_record,
                                     uint64_t last_record_end_pos,
                                     bool &stop_replay);

  table::InternalIterator* NewInternalIterator(const common::ReadOptions&,
                                               ColumnFamilyData* cfd,
                                               SuperVersion* super_version,
                                               util::Arena* arena);

protected:
  util::Env* const env_;
  const std::string dbname_;
  unique_ptr<VersionSet, memory::ptr_destruct_delete<VersionSet>> versions_;
  const common::DBOptions initial_db_options_;
  const common::ImmutableDBOptions immutable_db_options_;
  common::MutableDBOptions mutable_db_options_;
  std::atomic<uint64_t> stats_dump_period_sec_;
  monitor::Statistics* stats_;
  std::unordered_map<std::string, common::SequenceNumber> xid_map_;
  std::mutex recovered_transaction_mutex_;
  util::ConcurrentHashMap<std::string, RecoveredTransaction*> recovered_transactions_;
  std::unordered_map<common::SequenceNumber/*commit_seq*/, CommitWriteBatch*> commit_transactions_;
  static SnapshotImpl snapshot_;
  int bg_recycle_scheduled_;
  std::atomic<bool> master_thread_running_;

  void NewThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadsStatusDbInfo() const;

  common::Status WriteImpl(const common::WriteOptions& options,
                           WriteBatch* updates,
                           WriteCallback* callback = nullptr,
                           uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                           bool disable_memtable = false);

  common::Status WriteImplAsync(const common::WriteOptions &options,
                                WriteBatch *updates,                   /*in: */
                                common::AsyncCallback *async_callback, /*in:async call back*/
                                uint64_t *log_used,                    /*out: param, log used for this write*/
                                uint64_t log_ref,                      /*in: log_ref for this writer*/
                                bool disable_memtable /*in:wether disable memtable*/);

  uint64_t FindMinLogContainingOutstandingPrep();
  uint64_t FindMinPrepLogReferencedByMemTable();
  bool in_prep_log_ref_map(uint64_t log_number);

 private:
  friend class DB;
  friend class InternalStats;
  friend class util::TransactionImpl;
  friend struct SuperVersion;
  friend class CompactedDBImpl;
#ifndef NDEBUG
  friend class XFTransactionWriteHandler;
  friend class storage::ShrinkExtentSpacesJobTest;
#endif

  enum SwitchType {
    WAL_LIMIT = 0,
    SINGLE_WAL_LIMIT = 1,
    WRITE_BUFFER_LIMIT = 2,
    TOTAL_WRITE_BUFFER_LIMIT = 3,
    DELETE_LIMIT = 4,
    MANUAL_FLUSH = 5,
    OTHER = 6
  };
  struct WriteContext {
    util::autovector<SuperVersion*> superversions_to_free_;
    util::autovector<MemTable*> memtables_to_free_;
    SwitchType type_;
    AllSubTable *all_sub_table_;
    WriteContext() :
      superversions_to_free_(),
      memtables_to_free_(),
      type_(OTHER),
      all_sub_table_(nullptr) {}

    ~WriteContext() {
      for (auto& sv : superversions_to_free_) {
        MOD_DELETE_OBJECT(SuperVersion, sv);
      }
      for (auto& m : memtables_to_free_) {
        MemTable::async_delete_memtable(m);
      }
    }
  };

  struct PurgeFileInfo;

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  common::Status Recover(const common::ColumnFamilyOptions &cf_options);

  const common::Status CreateArchivalDirectory();

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();
  // Delete obsolete files and log status and information of file deletion
  void DeleteObsoleteFileImpl(common::Status file_deletion_status, int job_id,
                              const std::string& fname, util::FileType type,
                              uint64_t number, uint32_t path_id);

  common::Status SyncClosedLogs(JobContext* job_context);

  struct STFlushJob;
  struct STDumpJob;
  // Flush the in-memory write buffer to storage.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  common::Status FlushMemTableToOutputFile(
      STFlushJob &st_flush_job,
      bool* madeProgress,
      JobContext &job_context);

  int dump_memtable_to_outputfile(
      STDumpJob &st_dump_job,
      bool *madeProgress,
      JobContext& job_context);

  int prepare_recovery(const common::ColumnFamilyOptions &cf_options);
  int recovery();
  int create_default_subtbale(const common::ColumnFamilyOptions &cf_options);
  int recovery_wal(memory::ArenaAllocator &arena);
  int before_replay_wal_files();
  int after_replay_wal_files(memory::ArenaAllocator &arena);
  int parallel_replay_wal_files(memory::ArenaAllocator &arena);
  int finish_parallel_replay_wal_files(ReplayThreadPool &replay_thread_pool);
  int replay_wal_files(memory::ArenaAllocator &arena);
  int collect_sorted_wal_file_number(std::vector<uint64_t> &log_file_numbers);
  void update_last_sequence_during_recovery();
  int parallel_replay_one_wal_file(uint64_t file_number,
                                   uint64_t next_file_number,
                                   bool last_file,
                                   ReplayThreadPool &replay_thread_pool,
								   memory::ArenaAllocator &arena,
                                   uint64_t *read_time,
                                   uint64_t *parse_time,
                                   uint64_t *submit_time);
  int replay_one_wal_file(uint64_t file_number,
                          bool last_file,
                          common::SequenceNumber &next_allocate_sequence,
                          bool &stop_replay,
						  memory::ArenaAllocator &arena);
  int before_replay_one_wal_file(uint64_t current_log_file_number);
  int after_replay_one_wal_file(uint64_t next_log_file_number, common::SequenceNumber next_allocate_sequence);
  int consume_flush_scheduler_task(int* schedule_flush_num = nullptr);
  int recovery_switch_and_flush_memtable(ColumnFamilyData *sub_table);
  int recovery_switch_memtable(ColumnFamilyData *sub_table);
  int recovery_write_memtable(WriteBatch &write_batch, uint64_t current_log_file_number, common::SequenceNumber &next_allocate_sequence);
  int recovery_flush(ColumnFamilyData *sub_table);
  bool check_switch_memtable_now();
  template <typename Compare>
  int pick_and_switch_subtables(const SubTableMap& all_sub_tables,
                               std::set<uint32_t>& picked_cf_ids,
                               uint64_t expected_pick_num,
                               Compare& cmp,
                               uint64_t* picked_num,
                               util::BinaryHeap<SubTable*, Compare> *picked_sub_tables,
                               std::list<SubTable*>& switched_sub_tables);

  int update_wal_writer(bool force);
  int set_compaction_need_info();
  struct LogReporter : public log::Reader::Reporter {
    util::Env* env_;
    const char* fname_;
    common::Status* status_;
		LogReporter(util::Env *env, const char *file_name, common::Status *status)
				: env_(env),
					fname_(file_name),
					status_(status)
		{
		}
    virtual void Corruption(size_t bytes, const common::Status& s) override {
      __SE_LOG(WARN, "%s%s: dropping %d bytes; %s",
                     (this->status_ == nullptr ? "(ignoring error) " : ""),
                     fname_, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status_ != nullptr && this->status_->ok()) {
        *this->status_ = s;
      }
    }
  };

  int schedule_flush(WriteContext *write_context);

  int handle_single_wal_full(WriteContext* write_context);

  int trigger_switch_memtable(ColumnFamilyData *subtable2switch, WriteContext *write_context);

  int trigger_switch_or_dump(ColumnFamilyData* cfd, WriteContext *write_context);

  int switch_memtable(ColumnFamilyData *sub_table, WriteContext *write_context, bool force_create_new_log);

  int advance_recovery_point_without_flush();

  int flush_memtable(ColumnFamilyData *sub_table, const common::FlushOptions &flush_options);

  // Wait for memtable flushed
  common::Status WaitForFlushMemTable(ColumnFamilyData* cfd);

  int get_all_sub_table(AllSubTable *&all_sub_table, GlobalContext *&global_ctx);
  // REQUIRES: mutex locked
  common::Status HandleWALFull(WriteContext* write_context);

  int force_handle_wal_full(WriteContext* write_context);
  int find_subtables_to_switch(const uint64_t oldest_alive_log,
                               WriteContext* write_context,
                               bool force_switch = false);
  // REQUIRES: mutex locked
  common::Status HandleWriteBufferFull(WriteContext* write_context);

  // REQUIRES: mutex locked
  common::Status HandleTotalWriteBufferFull(WriteContext* write_context);

  // REQUIRES: mutex locked
  common::Status PreprocessWrite(const common::WriteOptions& write_options,
                                 bool need_log_sync, bool* logs_getting_syned,
                                 WriteContext* write_context);

  int build_dump_job(ColumnFamilyData *subtable, bool &do_dump);

  // todo yeti adjust list
//  int update_ck_subtable_list(SubTable *sub_table, const int64_t last_seq);

  int master_schedule_compaction(const CompactionScheduleType type);
  /**check if can schedule backgroud job(like flush, compaction, dump, gc, shrink) in
   * some common case:
   * case 1: opened_successfully_ is false, DBImpl open failed.
   * case 2: bg_work_paused_ greater than 0, stop schedule background job initiative
   * through smartengine_pause_background_work or internal logical.
   * case 3: shutdown is true, receive shutdown command.
   * case 4: bg_error_ is not Status::kOk, some background job has failed.
   * @return false if satisfy any upper case, and can't schedule background job.
   * @Note need protect by db_mutex_*/
  bool can_schedule_bg_work_common();
  void MaybeScheduleFlushOrCompaction();
  int maybe_schedule_dump();
  int maybe_schedule_gc();
  //use for timer to scheudle gc
  int schedule_gc();
  int schedule_shrink();
  void schedule_ebr();
  void SchedulePendingFlush(ColumnFamilyData* cfd);
  void SchedulePendingCompaction(ColumnFamilyData* cfd,
                                 const CompactionScheduleType type = CompactionScheduleType::NORMAL);
  void SchedulePendingPurge(std::string fname, util::FileType type,
                            uint64_t number, uint32_t path_id, int job_id);
  void schedule_pending_gc(ColumnFamilyData *sub_table);
  int schedule_shrink_if_need(const int64_t table_space_id);
  int shrink_extent_spaces(ShrinkArgs &shrink_args);
  int shrink_extent_space(const storage::ShrinkInfo &shrink_info);
  static void BGWorkCompaction(void* arg);
  static void BGWorkFlush(void* db);
  static void bg_work_dump(void* db);
  static void bg_work_gc(void *db);
  static void BGWorkPurge(void* arg);
  static void UnscheduleCallback(void* arg);
  static void bg_work_shrink(void *arg);
  static void bg_work_ebr(void *db);
  void BackgroundCallCompaction();
  void BackgroundCallFlush();
  void background_call_dump();
  void background_call_gc();
  void background_call_ebr();
  void BackgroundCallPurge();
  common::Status BackgroundCompaction(bool* madeProgress, JobContext* job_context);
  common::Status BackgroundFlush(bool* madeProgress, JobContext* job_context);

  int background_dump(bool* madeProgress, JobContext* job_context);
  int background_gc();
  // Will be called in master thread, no need external synchronization here.
  void background_pull_gauge_statistics();
  Top3ModMemInfo pull_top3_mod_mem_info();

  // helper functions for adding and removing from flush & compaction queues
  void AddToFlushQueue(ColumnFamilyData* cfd, TaskType type);
  void remove_flush_job(STFlushJob *&flush_job, bool schedule = true);
  STFlushJob* PopFirstFromFlushQueue();
  struct GCJob;

  STDumpJob* pop_front_dump_job();
  void remove_dump_job(STDumpJob *&flush_job);
  int push_back_gc_job(GCJob *gc_job);
  GCJob *pop_front_gc_job();
  int remove_gc_job(GCJob *&gc_job);
  struct CFCompactionJob;
  // scheduing new compaction task on %cfd;
  size_t compaction_job_size();
  CFCompactionJob* pop_front_compaction_job();
  void push_back_compaction_job(CFCompactionJob* cf_job);
  void add_compaction_job(ColumnFamilyData* cfd, CompactionTasksPicker::TaskInfo task_info);
  // shrink extent no need to schedule it
  void remove_compaction_job(CFCompactionJob*& cf_job, bool schedule = true);
  common::Status build_compaction_job(ColumnFamilyData *cfd,
                                      db::Snapshot *snapshot,
                                      JobContext *job_context,
                                      storage::CompactionJob *&job,
                                      CFCompactionJob &cf_job);

  common::Status run_one_compaction_task(ColumnFamilyData* cfd,
                                         JobContext* context,
                                         storage::CompactionJob* job);

  int run_one_flush_task(ColumnFamilyData *sub_table,
                         BaseFlush* flush_job,
                         JobContext& context,
                         std::vector<common::SequenceNumber> &flushed_seqs);

  bool need_snapshot_check(const TaskType task_type,
                           const Snapshot* meta_snapshot);
  void record_compaction_stats(
      const storage::Compaction::Statstics& compaction_stats);

  const Snapshot* GetSnapshotImpl(bool is_write_conflict_boundary);

  uint64_t GetMaxTotalWalSize() const;

  // table_cache_ provides its own synchronization
  std::shared_ptr<cache::Cache> table_cache_;

  // Lock over the persistent DB state.  Non-nullptr iff successfully acquired.
  util::FileLock* db_lock_;

  // State below is protected by mutex_
  mutable monitor::InstrumentedMutex mutex_;

  std::atomic<bool> shutting_down_;
  // This condition variable is signaled on these conditions:
  // * whenever bg_compaction_scheduled_ goes down to 0
  // * if AnyManualCompaction, whenever a compaction finishes, even if it hasn't
  // made any progress
  // * whenever a compaction made any progress
  // * whenever bg_flush_scheduled_ or bg_purge_scheduled_ value decreases
  // (i.e. whenever a flush is done, even if it didn't make any progress)
  // * whenever there is an error in background purge, flush or compaction
  // * whenever num_running_ingest_file_ goes to 0.
  monitor::InstrumentedCondVar bg_cv_;
  uint64_t logfile_number_;
  // TODO (Zhao Dongsheng) : the log synced is useless?
  bool log_dir_synced_;
  bool log_empty_;
  ColumnFamilyHandleImpl* default_cf_handle_;
  InternalStats* default_cf_internal_stats_;
  //unique_ptr<ColumnFamilyMemTablesImpl> column_family_memtables_;
  std::unordered_map<int64_t, int64_t> missing_subtable_during_recovery_;
  uint64_t last_check_time_during_recovery_;
  uint32_t no_switch_round_;
  port::Mutex deal_last_record_error_mutex_;
  std::atomic<uint64_t> max_sequence_during_recovery_;
  std::atomic<uint64_t> max_log_file_number_during_recovery_;
  static const uint64_t CHECK_NEED_SWITCH_DELTA = 500 * 1000; // 500 ms
  static const uint64_t MAX_NO_SWITCH_ROUND = 20;
  static const int64_t MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME = 3; // TODO options

  struct LogFileNumberSize {
    explicit LogFileNumberSize(uint64_t _number) : number(_number) {}
    void AddSize(uint64_t new_size) { size += new_size; }
    uint64_t number;
    uint64_t size = 0;
    bool getting_flushed = false;
    bool switch_flag = false;
  };
  std::deque<LogFileNumberSize> alive_log_files_;
  log::Writer *curr_log_writer_;
  std::mutex log_flush_mutex_;
  std::mutex log_sync_mutex_;
  std::condition_variable log_sync_cv_;
  std::atomic<bool> is_syncing_;
  std::atomic<uint64_t> total_log_size_;
  // If true, we have only one (default) column family. We use this to optimize
  // some code-paths
  bool single_column_family_mode_;
  bool is_snapshot_supported_;

  mutable port::Mutex snap_mutex[MAX_SNAP];
  SnapshotList snap_lists_[MAX_SNAP];

  // Class to maintain directories for all database paths other than main one.
  class Directories {
   public:
    Directories()
       :db_dir_(nullptr),
        data_dirs_(),
        wal_dir_(nullptr),
        persistent_cache_dir_(nullptr)
    {}

    common::Status SetDirectories(util::Env *env,
                                  const std::string &dbname,
                                  const std::string &wal_dir,
                                  const std::vector<common::DbPath> &data_paths,
                                  const std::string &persistent_cache_dir);

    util::Directory* GetDataDir(size_t path_id);

    util::Directory* GetWalDir() {
      if (nullptr != wal_dir_) {
        return wal_dir_.get();
      }
      return db_dir_.get();
    }

    util::Directory* GetDbDir() { return db_dir_.get(); }

   private:
    std::unique_ptr<util::Directory, memory::ptr_destruct_delete<util::Directory>> db_dir_;
    std::vector<util::Directory *> data_dirs_;
    std::unique_ptr<util::Directory, memory::ptr_destruct_delete<util::Directory>> wal_dir_;
    std::unique_ptr<util::Directory, memory::ptr_destruct_delete<util::Directory>> persistent_cache_dir_;

    common::Status CreateAndNewDirectory(
        util::Env* env, const std::string& dirname,
        util::Directory *&directory) const;
  };

  Directories directories_;

  WriteBufferManager* write_buffer_manager_;

  // for db_total_write_buffer_size
  TrimMemFlushState trim_mem_flush_waited_;
  uint64_t next_trim_time_;

  // write buffer manager of storage manager
  WriteBufferManager* storage_write_buffer_manager_;

  WriteBatch tmp_batch_;

  // MySQL Binlog Position. Protected by db mutex.
  BinlogPosition global_binlog_pos_;

  // Add for smartengine
  db::PiplineQueueManager pipline_manager_;

  std::atomic<uint64_t> pipline_parallel_worker_num_;

  // for pipline log copy and log flush
  std::atomic<bool> pipline_copy_log_busy_flag_;
  std::atomic<bool> pipline_flush_log_busy_flag_;

  std::atomic<bool> pipline_global_error_flag_;

  // for parall write memtable, before switch we need to wait all active thread
  // exit
  std::atomic<uint64_t> active_thread_num_;
  port::Mutex active_thread_mutex_;
  port::CondVar active_thread_cv_;
  std::atomic<bool> wait_active_thread_exit_flag_;
  std::atomic<bool> last_write_in_serialization_mode_;

  std::atomic<uint64_t> last_flushed_log_lsn_;

  db::BatchGroupManager batch_group_manager_;

  // Committed Version Advance sliding window
  port::Mutex version_sliding_window_mutex_;
  std::unordered_map<uint64_t, db::WriteRequest*> version_sliding_window_map_;

  int run_pipline(uint64_t thread_local_expected_seq);
  int do_copy_log_buffer_job(uint64_t thread_local_expected_seq);
  int do_flush_log_buffer_job(uint64_t thread_local_expected_seq);
  int do_write_memtable_job();
  int do_commit_job();
  void update_committed_version(db::WriteRequest* writer);
  int complete_write_job(db::WriteRequest* writer, common::Status& s);

  int clean_pipline_error();

  void increase_active_thread(bool serialization_mode = false);
  void decrease_active_thread();
  void wait_all_active_thread_exit();
  uint64_t get_active_thread_num(std::memory_order 
                                 order = std::memory_order_relaxed);
  // End of smartengine;

  // Size of the last batch group. In slowdown mode, next write needs to
  // sleep if it uses up the quota.
  uint64_t last_batch_group_size_;

  FlushScheduler flush_scheduler_;

  // SnapshotList snapshots_;

  // PurgeFileInfo is a structure to hold information of files to be deleted in
  // purge_queue_
  struct PurgeFileInfo {
    std::string fname;
    util::FileType type;
    uint64_t number;
    uint32_t path_id;
    int job_id;
    PurgeFileInfo(std::string fn, util::FileType t, uint64_t num, uint32_t pid,
                  int jid)
        : fname(fn), type(t), number(num), path_id(pid), job_id(jid) {}
  };

  // one column family compaction task associates with multiple compaction unit;
  struct CFCompactionJob {
    ColumnFamilyData* cfd_;
    Snapshot *meta_snapshot_;
    storage::CompactionJob* job_;
    CompactionTasksPicker::TaskInfo task_info_;
    bool need_check_snapshot_;
    memory::ArenaAllocator compaction_alloc_;

    void update_snapshot(Snapshot *meta_snapshot) { meta_snapshot_ = meta_snapshot; }

    CFCompactionJob(ColumnFamilyData *cfd,
                    Snapshot *s,
                    storage::CompactionJob *job,
                    CompactionTasksPicker::TaskInfo task_info,
                    const bool need_check_snapshot)
        : cfd_(cfd),
          meta_snapshot_(s),
          job_(job),
          task_info_(task_info),
          need_check_snapshot_(true),
          compaction_alloc_(memory::CharArena::DEFAULT_PAGE_SIZE, memory::ModId::kCompaction)
    {
    }
  };

  struct STFlushJob {
    STFlushJob(ColumnFamilyData *sub_table, Snapshot *s, db::TaskType type, const bool need_check_snapshot = true)
        : sub_table_(sub_table),
          meta_snapshot_(s),
          task_type_(type),
          need_check_snapshot_(need_check_snapshot),
          flush_alloc_(8 * 1024, memory::ModId::kFlush) {}

    void update_snapshot(Snapshot *meta_snapshot) { meta_snapshot_ = meta_snapshot; }

    ColumnFamilyData *get_subtable() const {
      return sub_table_;
    }
    TaskType get_task_type() const {
      return task_type_;
    }
    ColumnFamilyData *sub_table_;
    Snapshot *meta_snapshot_;
    TaskType task_type_;
    bool need_check_snapshot_;
    memory::ArenaAllocator flush_alloc_;
  };
  struct STDumpJob {
    STDumpJob()
        : sub_table_(nullptr),
          dump_mem_(nullptr),
          dump_max_seq_(0),
          dump_alloc_(8 * 1024, memory::ModId::kFlush)
    {}
    ColumnFamilyData *get_subtable() const {
      return sub_table_;
    }
    ColumnFamilyData *sub_table_;
    MemTable *dump_mem_;
    int64_t dump_max_seq_;
    memory::ArenaAllocator dump_alloc_;
  };

  struct GCJob
  {
    GCJob() : sub_table_(nullptr), env_(nullptr), dropped_time_(0) {}
    GCJob(ColumnFamilyData *sub_table, util::Env *env, int64_t dropped_time)
        : sub_table_(sub_table),
          env_(env),
          dropped_time_(dropped_time)
    {
    }
    ~GCJob() {}
    bool valid() const
    {
      return nullptr != sub_table_ && dropped_time_ > 0 && nullptr != sub_table_;
    }
    bool can_gc()
    {
      int64_t current_time = 0;
      env_->GetCurrentTime(&current_time);
      return sub_table_->can_gc() && ((current_time - dropped_time_) > GC_INTERVAL_TIME);
    }

    static const int64_t GC_INTERVAL_TIME = 15 * 60; //15 minute
    ColumnFamilyData *sub_table_;
    util::Env *env_;
    int64_t dropped_time_;

    DECLARE_AND_DEFINE_TO_STRING(KVP_(sub_table), KV_(dropped_time))
  };

  // flush_queue_ and compaction_queue_ hold column families that we need to
  // flush and compact, respectively.
  // A column family is inserted into flush_queue_ when it satisfies condition
  // cfd->imm()->IsFlushPending()
  // A column family is inserted into compaction_queue_ when it satisfied
  // condition cfd->NeedsCompaction()
  // Column families in this list are all Ref()-erenced
  // TODO(icanadi) Provide some kind of ReferencedColumnFamily class that will
  // do RAII on ColumnFamilyData
  // Column families are in this queue when they need to be flushed or
  // compacted. Consumers of these queues are flush and compaction threads. When
  // column family is put on this queue, we increase unscheduled_flushes_ and
  // unscheduled_compactions_. When these variables are bigger than zero, that
  // means we need to schedule background threads for compaction and thread.
  // Once the background threads are scheduled, we decrease unscheduled_flushes_
  // and unscheduled_compactions_. That way we keep track of number of
  // compaction and flush threads we need to schedule. This scheduling is done
  // in MaybeScheduleFlushOrCompaction()
  // invariant(column family present in flush_queue_ <==>
  // ColumnFamilyData::pending_flush_ == true)
//  std::deque<ColumnFamilyData*> flush_queue_;
  std::deque<STFlushJob *> flush_queue_;
  // dump task for checkpoint
  std::deque<STDumpJob *> dump_queue_;
  struct MemtableCleanupInfo {
    MemtableCleanupInfo(ColumnFamilyData* cfd, common::SequenceNumber seqno)
      : cfd_(cfd), first_seqno_(seqno) {}

    ColumnFamilyData* cfd_;
    common::SequenceNumber first_seqno_;
  };
  // after memtable flushed, (cfd, seqno) is put into this queue.
  // The cfd would appear in this queue multiple times with different seqno.
  // The order is maintained for trimming memtables.
  std::deque<MemtableCleanupInfo> memtable_cleanup_queue_;

  // invariant(column family present in compaction_queue_ <==>
  // ColumnFamilyData::pending_compaction_ == true)
  // It has 2 priorities: low and high.
  std::list<CFCompactionJob*> compaction_queue_[CompactionPriority::ALL];

  // A queue to store filenames of the files to be purged
  std::deque<PurgeFileInfo> purge_queue_;

  // A queue to store pointer of subtable to garbage clean
  std::deque<GCJob *> gc_queue_;

  int unscheduled_flushes_;
  int unscheduled_compactions_;
  int unscheduled_dumps_;
  int unscheduled_gc_;

  // count how many background compactions are running or have been scheduled
  int bg_compaction_scheduled_;

  // stores the number of compactions are currently running
  int num_running_compactions_;

  // number of background memtable flush jobs, submitted to the HIGH pool
  int bg_flush_scheduled_;

  // stores the number of flushes are currently running
  int num_running_flushes_;

  // number of background memtable dump jobs, submitted to the low pool
  int bg_dump_scheduled_;

  // stores the number of dumps are currently running
  int num_running_dumps_;

  // number of background obsolete file purge jobs, submitted to the HIGH pool
  int bg_purge_scheduled_;

  //store the number of gc are currently running
  int num_running_gc_;

  // number of backgroud gc jobs, submitted to the low pool
  int bg_gc_scheduled_;

  // number of background ebr jobs, submitted to the high pool
  int bg_ebr_scheduled_;

//  util::TimerService *timer_service_;
//  util::Timer *gc_timer_;
//  util::Timer *cache_purge_timer_;
//  util::Timer *shrink_timer_;
  std::atomic<bool> shrink_running_;

  //max sequence number among all recovery point after recovery sst data
  common::SequenceNumber max_seq_in_rp_;

  struct CompactionArg {
    DBImpl* db;
  };

  // Have we encountered a background error in paranoid mode?
  common::Status bg_error_;

  // shall we disable deletion of obsolete files
  // if 0 the deletion is enabled.
  // if non-zero, files will not be getting deleted
  // This enables two different threads to call
  // EnableFileDeletions() and DisableFileDeletions()
  // without any synchronization
  int disable_delete_obsolete_files_;

  // last time when DeleteObsoleteFiles with full scan was executed. Originaly
  // initialized with startup time.
  uint64_t delete_obsolete_files_last_run_;

  // last time stats were dumped to LOG
  std::atomic<uint64_t> last_stats_dump_time_microsec_;

  // Each flush or compaction gets its own job id. this counter makes sure
  // they're unique
  std::atomic<int> next_job_id_;

  // A flag indicating whether the current database has any
  // data that is not yet persisted into either WAL or SST file.
  // Used when disableWAL is true.
  std::atomic<bool> has_unpersisted_data_;

  // if an attempt was made to flush all column families that
  // the oldest log depends on but uncommited data in the oldest
  // log prevents the log from being released.
  // We must attempt to free the dependent memtables again
  // at a later time after the transaction in the oldest
  // log is fully commited.
  bool unable_to_flush_oldest_log_;

  static const int KEEP_LOG_FILE_NUM = 1000;
  // MSVC version 1800 still does not have constexpr for ::max()
  static const uint64_t kNoTimeOut = port::kMaxUint64;

  std::string db_absolute_path_;

  // The options to access storage files
  const util::EnvOptions env_options_;

  // A value of > 0 temporarily disables scheduling of background work
  int bg_work_paused_;

  // A value of > 0 temporarily disables scheduling of background compaction
  int bg_compaction_paused_;

  // Guard against multiple concurrent refitting
  bool refitting_level_;

  // Indicate DB was opened successfully
  bool opened_successfully_;

  // Map a log number to the number of not commited transactions whose
  // prepare log entry is in the log number.
  std::map<uint64_t, int64_t> not_commited_section_;
  std::mutex not_commited_mutex_;

  cache::Cache* block_cache_ = nullptr;
  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  // Background threads call this function, which is just a wrapper around
  // the InstallSuperVersion() function. Background threads carry
  // job_context which can have new_superversion already
  // allocated.
  void InstallSuperVersionAndScheduleWorkWrapper(ColumnFamilyData* cfd, JobContext* job_context);

  // All ColumnFamily state changes go through this function. Here we analyze
  // the new state and we schedule background work if we detect that the new
  // state needs flush or compaction.
  SuperVersion* InstallSuperVersionAndScheduleWork(ColumnFamilyData* cfd, SuperVersion* new_sv);

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  common::Status GetImpl(const common::ReadOptions& options,
                         ColumnFamilyHandle* column_family,
                         const common::Slice& key, common::PinnableSlice* value,
                         bool* value_found = nullptr);

  bool GetIntPropertyInternal(ColumnFamilyData* cfd,
                              const DBPropertyInfo& property_info,
                              bool is_locked, uint64_t* value);

  size_t GetWalPreallocateBlockSize(uint64_t write_buffer_size) const;

  virtual common::Status InstallSstExternal(ColumnFamilyHandle* column_family,
                                            db::MiniTables* mtables) override;

  std::vector<common::SequenceNumber> GetAll(
      common::SequenceNumber* oldest_write_conflict_snapshot = nullptr);

  virtual bool get_columnfamily_stats(ColumnFamilyHandle* column_family, int64_t &data_size,
                              int64_t &num_entries, int64_t &num_deletes, int64_t &disk_size) override;

  // for information_schema and the compaction_history table in it
  std::atomic<int64_t> compaction_sequence_;
  std::mutex compaction_history_mutex_;
  std::list<storage::CompactionJobStatsInfo*> compaction_history_;
  storage::CompactionJobStatsInfo compaction_sum_;
  RecoveryDebugInfo recovery_debug_info_;
};


extern common::Options SanitizeOptions(const std::string& db,
                                       const common::Options& src);

extern common::DBOptions SanitizeOptions(const std::string& db,
                                         const common::DBOptions& src);

extern common::CompressionType GetCompressionFlush(const common::ImmutableCFOptions& ioptions, const int64_t level);

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
} // namespace db
} // namespace smartengine
