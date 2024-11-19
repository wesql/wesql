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
#include "db/db_impl.h"
#include "memory/base_malloc.h"
#include "memory/mod_info.h"

#ifdef WITH_SMARTENGINE
#include "se_status_vars.h"
#endif

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#ifdef OS_SOLARIS
#include <alloca.h>
#endif
#ifdef ROCKSDB_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

#include <algorithm>
#include <climits>
#include <cstdio>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backup/hotbackup_impl.h"
#include "cache/row_cache.h"
#include "compact/compaction_job.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/flush_job.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_callback.h"
#include "memory/alloc_mgr.h"
#include "memtable/memtable_list.h"
#include "monitoring/query_perf_context.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "objstore/snapshot_release_lock.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "port/likely.h"
#include "storage/extent_meta_manager.h"
#include "storage/extent_space_manager.h"
#include "storage/storage_logger.h"
#include "table/extent_table_factory.h"
#include "table/merging_iterator.h"
#include "table/two_level_iterator.h"
#include "util/autovector.h"
#include "util/build_version.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/file_reader_writer.h"
#include "util/file_util.h"
#include "util/file_name.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "write_batch/write_batch_internal.h"

namespace smartengine {
using namespace cache;
using namespace common;
using namespace memory;
using namespace monitor;
using namespace storage;
using namespace table;
using namespace util;

namespace db {

void BackupSnapshotMap::clear() {
  std::unique_lock lock(mutex_);
  backup_snapshots_.clear();
  auto_increment_ids_.clear();
}

bool BackupSnapshotMap::find_backup_snapshot(BackupSnapshotId backup_id) {
  std::unique_lock lock(mutex_);
  return backup_snapshots_.find(backup_id) != backup_snapshots_.end();
}

bool BackupSnapshotMap::get_next_backup_snapshot(BackupSnapshotId prev_backup_id,
                                                 BackupSnapshotId &backup_id,
                                                 uint64_t &auto_increment_id,
                                                 MetaSnapshotSet *&meta_snapshots)
{
  std::unique_lock lock(mutex_);
  auto it = backup_snapshots_.upper_bound(prev_backup_id);
  if (it == backup_snapshots_.end()) {
    backup_id = 0;
    return false;
  }
  backup_id = it->first;
  auto_increment_id = auto_increment_ids_[backup_id];
  meta_snapshots = &it->second;
  return true;
}

bool BackupSnapshotMap::add_backup_snapshot(BackupSnapshotId backup_id,
                                            uint64_t auto_increment_id,
                                            MetaSnapshotSet &meta_snapshots)
{
  std::unique_lock lock(mutex_);
  if (backup_snapshots_.find(backup_id) != backup_snapshots_.end()) {
    // unexpected, backup snapshot already exists
    return false;
  } else if (backup_snapshots_.size() != auto_increment_ids_.size()) {
    // unexpected, backup_snapshots_ and auto_increment_ids_ should be the same size.
    return false;
  } else {
    backup_snapshots_[backup_id] = std::move(meta_snapshots);
    auto_increment_ids_[backup_id] = auto_increment_id;
    if (auto_increment_id > max_auto_increment_id_) {
      max_auto_increment_id_ = auto_increment_id;
    }
    return true;
  }
}

uint64_t BackupSnapshotMap::get_auto_increment_id(BackupSnapshotId backup_id)
{
  std::unique_lock lock(mutex_);
  auto it = auto_increment_ids_.find(backup_id);
  if (it == auto_increment_ids_.end()) {
    return UINT64_MAX;
  }
  return it->second;
}

uint64_t BackupSnapshotMap::get_max_auto_increment_id()
{
  std::unique_lock lock(mutex_);
  return max_auto_increment_id_;
}

void BackupSnapshotMap::save_auto_increment_id_for_recover(uint64_t auto_increment_id)
{
  std::unique_lock lock(mutex_);
  auto_increment_id_for_recover_ = auto_increment_id;
}

uint64_t BackupSnapshotMap::get_auto_increment_id_for_recover()
{
  std::unique_lock lock(mutex_);
  return auto_increment_id_for_recover_;
}

bool BackupSnapshotMap::remove_backup_snapshot(BackupSnapshotId backup_id, MetaSnapshotSet &to_clean, bool &existed)
{
  std::unique_lock lock(mutex_);
  if (backup_snapshots_.find(backup_id) == backup_snapshots_.end()) {
    existed = false;
    return true;
  }
  existed = true;
  if (in_use_) {
    pending_release_backups_.push_back(backup_id);
  } else {
    to_clean = std::move(backup_snapshots_[backup_id]);
    backup_snapshots_.erase(backup_id);
    auto_increment_ids_.erase(backup_id);
  }
  return !in_use_;
}

int BackupSnapshotMap::release_backup_snapshot(BackupSnapshotId backup_id)
{
  int ret = Status::kOk;
  ReleaseBackupSnapshotLogEntry log_entry(backup_id);
  MetaSnapshotSet backup_snapshot;
  bool existed = false;
  int64_t commit_seq = 0;

  BackupSnapshotMap &backup_snapshot_map = BackupSnapshotImpl::get_instance()->get_backup_snapshot_map();
  uint64_t auto_increment_id = 0;

  if (backup_id == 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "Invalid backup id", K(backup_id), K(ret));
  } else if (IS_FALSE(backup_snapshot_map.find_backup_snapshot(backup_id))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "backup snapshot id not found", K(backup_id), K(ret));
  } else if (UINT64_MAX == (auto_increment_id = backup_snapshot_map.get_auto_increment_id(backup_id))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, auto_increment_id not found", K(backup_id), K(ret));
  } else if (GlobalContext::env_->IsObjectStoreInited()) {
    objstore::ObjectStore *objstore = nullptr;
    std::string_view objstore_bucket = GlobalContext::env_->GetObjectStoreBucket();
    const std::string &cluster_objstore_id = GlobalContext::env_->GetClusterObjstoreId();
    std::string err_msg;
    uint64_t auto_increment_id_for_recover = backup_snapshot_map.get_auto_increment_id_for_recover();
    if (FAILED(GlobalContext::env_->GetObjectStore(objstore).code())) {
      SE_LOG(WARN, "fail to get object store", K(ret));
    } else if (auto_increment_id == auto_increment_id_for_recover &&
               FAILED(objstore::updateBackupStautsToReleasing(auto_increment_id,
                                                              objstore,
                                                              objstore_bucket,
                                                              cluster_objstore_id,
                                                              err_msg))) {
      SE_LOG(WARN,
             "fail to update backup status lock file of the snapshot used for recovery",
             K(backup_id),
             K(auto_increment_id_for_recover),
             K(ret),
             K(err_msg));
    } else if (auto_increment_id != auto_increment_id_for_recover &&
               FAILED(objstore::tryBackupReleasingLock(auto_increment_id,
                                                       objstore,
                                                       objstore_bucket,
                                                       cluster_objstore_id,
                                                       err_msg))) {
      SE_LOG(WARN,
             "fail to get backup releasing lock",
             K(backup_id),
             K(auto_increment_id),
             K(auto_increment_id_for_recover),
             K(ret),
             K(err_msg));
    } else {
      SE_LOG(INFO, "success to get backup releasing lock", K(backup_id), K(auto_increment_id));
    }
  }

  if (SUCCED(ret)) {
    if (FAILED(StorageLogger::get_instance().begin(storage::SeEvent::RELEASE_BACKUP_SNAPSHOT))) {
      SE_LOG(WARN, "fail to begin release backup snapshot log", K(backup_id), K(ret));
    } else if (FAILED(StorageLogger::get_instance().write_log(storage::REDO_LOG_RELEASE_BACKUP_SNAPSHOT, log_entry))) {
      SE_LOG(WARN, "fail to write release backup snapshot log", K(backup_id), K(ret));
    } else if (FAILED(StorageLogger::get_instance().commit(commit_seq))) {
      SE_LOG(WARN, "fail to commit release backup snapshot log", K(backup_id), K(ret));
    } else if (IS_FALSE(remove_backup_snapshot(backup_id, backup_snapshot, existed)) && existed) {
      // normal case, maybe background checkpoint thread is using the backup snapshot
      SE_LOG(INFO, "Backup snapshot is in use, will be deleted later", K(ret), K(backup_id));
    } else if (IS_FALSE(existed)) {
      ret = Status::kInvalidArgument;
      SE_LOG(WARN, "backup snapshot id not found", K(ret), K(backup_id));
    } else if (FAILED(StorageManager::cleanup_backup_snapshot(backup_snapshot))) {
      SE_LOG(WARN, "fail to cleanup backup snapshot", K(backup_id), K(ret));
    } else {
      SE_LOG(INFO, "success to release the backup snapshot", K(backup_id), K(ret));
    }
  }

  return ret;
}

int BackupSnapshotMap::do_pending_release()
{
  int ret = Status::kOk;
  std::vector<BackupSnapshotId> to_release;
  {
    std::unique_lock lock(mutex_);
    if (in_use_) {
      ret = Status::kErrorUnexpected;
    } else {
      to_release = std::move(pending_release_backups_);
    }
  }
  if (FAILED(ret)) {
    SE_LOG(WARN, "unexpected, backup snapshot map is in use", K(ret));
  } else {
    for (auto backup_id : to_release) {
      if (FAILED(release_backup_snapshot(backup_id))) {
        SE_LOG(WARN, "failed to release backup snapshot", K(backup_id));
        break;
      } else {
        SE_LOG(INFO, "success to release backup snapshot", K(backup_id));
      }
    }
  }
  return ret;
}

void BackupSnapshotMap::set_in_use(bool in_use) {
  std::lock_guard<std::mutex> lock(mutex_);
  in_use_ = in_use;
}

BackupSnapshotId BackupSnapshotMap::get_latest_backup_id() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (backup_snapshots_.empty()) {
    return 0;
  }
  return backup_snapshots_.rbegin()->first;
}

size_t BackupSnapshotMap::get_backup_snapshot_count() {
  std::lock_guard<std::mutex> lock(mutex_);
  return backup_snapshots_.size();
}

#ifdef WITH_STRESS_CHECK
thread_local std::unordered_map<std::string, std::string> *STRESS_CHECK_RECORDS =
    new std::unordered_map<std::string, std::string>();
#endif

const std::string kDefaultColumnFamilyName("default");
void DumpSmartEngineBuildVersion();

CompressionType GetCompressionFlush(const ImmutableCFOptions& ioptions, const int64_t level)
{
  assert( level >= 0);
  CompressionType compress_type = common::kNoCompression;
  // Compressing memtable flushes might not help unless the sequential load
  // optimization is used for leveled compaction. Otherwise the CPU and
  // latency overhead is not offset by saving much space.
  if ( static_cast<uint64_t>(level) < ioptions.compression_per_level.size()) {
    // For leveled compress when min_level_to_compress != 0.
    compress_type = ioptions.compression_per_level[level];
  }

  return compress_type;
}

void all_sub_table_unref_handle(void *ptr)
{
  AllSubTable *all_sub_table = static_cast<AllSubTable *>(ptr);
  //there may be deadlock between ThreadExit and Scrape.
  //refs is atomic variable, and all thread will reference newer version;
  //it's still safe to free memory,without all_sub_table_mutex_.
  //std::lock_guard<std::mutex> guard(*all_sub_table->all_sub_table_mutex_);
  if (all_sub_table->unref()) {
    MOD_DELETE_OBJECT(AllSubTable, all_sub_table);
  }
}

util::Env *GlobalContext::env_ = nullptr;
cache::Cache *GlobalContext::cache_ = nullptr;

GlobalContext::GlobalContext(const std::string &db_name, const common::Options &options)
    : db_name_(db_name),
      options_(options),
      env_options_(),
      write_buf_mgr_(nullptr),
      all_sub_table_mutex_(),
      version_number_(0),
      local_all_sub_table_(nullptr),
      all_sub_table_(nullptr),
      db_dir_(nullptr) {
  local_all_sub_table_.reset(MOD_NEW_OBJECT(ModId::kAllSubTable, ThreadLocalPtr, &all_sub_table_unref_handle));
  all_sub_table_ = MOD_NEW_OBJECT(ModId::kAllSubTable, AllSubTable);
  all_sub_table_->ref();
  all_sub_table_->all_sub_table_mutex_ = &all_sub_table_mutex_;
}

GlobalContext::~GlobalContext()
{
  std::lock_guard<std::mutex> guard(*all_sub_table_->all_sub_table_mutex_);
  if (all_sub_table_->unref()) {
    MOD_DELETE_OBJECT(AllSubTable, all_sub_table_);
  }
}

bool GlobalContext::is_valid() { return nullptr != GlobalContext::get_cache() && nullptr != write_buf_mgr_; }

void GlobalContext::reset()
{
  GlobalContext::env_ = nullptr;
  GlobalContext::cache_ = nullptr;
  write_buf_mgr_ = nullptr;
  all_sub_table_->reset();
  db_dir_ = nullptr;
}

int GlobalContext::acquire_thread_local_all_sub_table(AllSubTable *&all_sub_table)
{
  int ret = Status::kOk;
  AllSubTable *all_sub_table_to_delete = nullptr;
  if (AllSubTable::kAllSubtableInUse == (all_sub_table = static_cast<AllSubTable *>(local_all_sub_table_->Swap(AllSubTable::kAllSubtableInUse)))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, local all sub table state is wrong", K(ret));
    assert(false);
  } else if (AllSubTable::kAllSubtableObsolete == all_sub_table || all_sub_table->version_number_ != version_number_.load())  {
    std::lock_guard<std::mutex> guard(all_sub_table_mutex_);
    if (nullptr != all_sub_table && all_sub_table->unref()) {
      all_sub_table_to_delete = all_sub_table;
    }
    all_sub_table_->ref();
    all_sub_table = all_sub_table_;
    if (nullptr != all_sub_table_to_delete) {
      MOD_DELETE_OBJECT(AllSubTable, all_sub_table_to_delete);
    }
  }
  return ret;
}

int GlobalContext::release_thread_local_all_sub_table(AllSubTable *all_sub_table)
{
  int ret = Status::kOk;
  void *expected = AllSubTable::kAllSubtableInUse;
  if (nullptr == all_sub_table) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(all_sub_table));
  } else if (local_all_sub_table_->CompareAndSwap(static_cast<void *>(all_sub_table), expected)) {
    //success to return all sub table
  } else if (AllSubTable::kAllSubtableObsolete != expected) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, local all sub table state is wrong", K(ret));
    assert(false);
  } else {
    std::lock_guard<std::mutex> guard(all_sub_table_mutex_);
    if (all_sub_table->unref()) {
      MOD_DELETE_OBJECT(AllSubTable, all_sub_table);
    }
  }
  return ret;
}

//thread unsafe, need protect by all_sub_table_mutex_
void GlobalContext::reset_thread_local_all_sub_table()
{
  std::vector<void *> all_sub_tables;
  AllSubTable *all_sub_table = nullptr;
  local_all_sub_table_->Scrape(&all_sub_tables, AllSubTable::kAllSubtableObsolete);
  for (uint64_t i = 0; i < all_sub_tables.size(); ++i) {
    all_sub_table = static_cast<AllSubTable *>(all_sub_tables.at(i));
    if (AllSubTable::kAllSubtableInUse == all_sub_table) {
      //do nothing
    } else {
      if (all_sub_table->unref()) {
        MOD_DELETE_OBJECT(AllSubTable, all_sub_table);
      }
    }
  }
}

//thread unsafe, need protect by all_sub_table_mutex_
int GlobalContext::install_new_all_sub_table(AllSubTable *all_sub_table)
{
  int ret = Status::kOk;
  AllSubTable *old_all_sub_table = nullptr;
  if (nullptr == all_sub_table) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(all_sub_table));
  } else {
    old_all_sub_table = all_sub_table_;
    ++version_number_;
    all_sub_table_ = all_sub_table;
    all_sub_table_->version_number_ = version_number_;
    all_sub_table_->ref();
    reset_thread_local_all_sub_table();
    if (nullptr != old_all_sub_table && old_all_sub_table->unref()) {
      MOD_DELETE_OBJECT(AllSubTable, old_all_sub_table);
    }
  }
  return ret;
}

DBImpl::DBImpl(const DBOptions &options, const std::string &dbname)
    : env_(options.env),
      dbname_(dbname),
      initial_db_options_(SanitizeOptions(dbname, options)),
      immutable_db_options_(initial_db_options_),
      mutable_db_options_(initial_db_options_),
      stats_dump_period_sec_(mutable_db_options_.stats_dump_period_sec),
      stats_(immutable_db_options_.statistics.get()),
      bg_recycle_scheduled_(0),
      master_thread_running_(false),
      db_lock_(nullptr),
      mutex_(&mutable_db_options_.mutex_backtrace_threshold_ns, env_),
      shutting_down_(false),
      bg_cv_(&mutex_),
      logfile_number_(0),
      log_dir_synced_(false),
      log_empty_(true),
      default_cf_handle_(nullptr),
      missing_subtable_during_recovery_(),
      last_check_time_during_recovery_(0),
      no_switch_round_(0),
      deal_last_record_error_mutex_(false),
      max_sequence_during_recovery_(0),
      max_log_file_number_during_recovery_(0),
      curr_log_writer_(nullptr),
      log_flush_mutex_(),
      log_sync_mutex_(),
      log_sync_cv_(),
      is_syncing_(false),
      total_log_size_(0),
      is_snapshot_supported_(true),
      write_buffer_manager_(immutable_db_options_.write_buffer_manager.get()),
      trim_mem_flush_waited_(kFlushDone),
      next_trim_time_(0),
      storage_write_buffer_manager_(nullptr),
      pipline_manager_(100 * 1024),
      pipline_parallel_worker_num_(0),
      pipline_copy_log_busy_flag_(false),
      pipline_flush_log_busy_flag_(false),
      pipline_global_error_flag_(false),
      active_thread_num_(0),
      active_thread_mutex_(false),
      active_thread_cv_(&active_thread_mutex_),
      wait_active_thread_exit_flag_(false),
      last_write_in_serialization_mode_(false),
      last_flushed_log_lsn_(0),
      batch_group_manager_(options.batch_group_slot_array_size,
                           options.batch_group_max_group_size,
                           options.batch_group_max_leader_wait_time_us),
      version_sliding_window_mutex_(false),
      last_batch_group_size_(0),
      unscheduled_flushes_(0),
      unscheduled_compactions_(0),
      unscheduled_dumps_(0),
      unscheduled_gc_(0),
      bg_compaction_scheduled_(0),
      num_running_compactions_(0),
      bg_flush_scheduled_(0),
      num_running_flushes_(0),
      bg_dump_scheduled_(0),
      num_running_dumps_(0),
      bg_purge_scheduled_(0),
      num_running_gc_(0),
      bg_gc_scheduled_(0),
      bg_ebr_scheduled_(0),
      shrink_running_(false),
      max_seq_in_rp_(0),
      disable_delete_obsolete_files_(0),
      delete_obsolete_files_last_run_(env_->NowMicros()),
      last_stats_dump_time_microsec_(0),
      next_job_id_(1),
      has_unpersisted_data_(false),
      unable_to_flush_oldest_log_(false),
      env_options_(BuildDBOptions(immutable_db_options_, mutable_db_options_)),
      bg_work_paused_(0),
      bg_compaction_paused_(0),
      refitting_level_(false),
      opened_successfully_(false) {
  env_->GetAbsolutePath(dbname, &db_absolute_path_);
  env_->SetBackgroundThreads(1, Env::STATS);
  env_->SetBackgroundThreads(1, Env::MASTER);
  env_->SetBackgroundThreads(2, Env::RECYCLE_EXTENT);
  env_->SetBackgroundThreads(1, Env::SHRINK_EXTENT_SPACE);

  table_cache_ = NewLRUCache(immutable_db_options_.table_cache_size,
                             immutable_db_options_.table_cache_numshardbits);
  if (nullptr != table_cache_.get()) {
    table_cache_.get()->set_mod_id(ModId::kTableCache);
  }
  VersionSet *vs_ptr = MOD_NEW_OBJECT(ModId::kDBImpl, VersionSet, dbname_, &immutable_db_options_,
      env_options_, table_cache_.get(), write_buffer_manager_);
  versions_.reset(vs_ptr);

  //TODO(Zhao Dongsheng. opt_enable_count and opt_print_stats should be configurable)
  QueryPerfContext *ctx = get_tls_query_perf_context();
  ctx->opt_enable_count_ = true;
  ctx->opt_print_stats_ = false;

  DumpSmartEngineBuildVersion();
  FileNameUtil::dump(immutable_db_options_, dbname_);
  immutable_db_options_.Dump();
  mutable_db_options_.Dump();
}

DBImpl::~DBImpl() {
  // CancelAllBackgroundWork called with false means we just set the shutdown
  // marker. After this we do a variant of the waiting and unschedule work
  // (to consider: moving all the waiting into CancelAllBackgroundWork(true))
  CancelAllBackgroundWork(false);
//  int compactions_unscheduled = env_->UnSchedule(this, Env::Priority::LOW); // has gc task
  int flushes_unscheduled = env_->UnSchedule(this, Env::Priority::HIGH);
  int dump_unscheduled = env_->UnSchedule(this, Env::Priority::LOW);

  get_tls_query_perf_context()->shutdown();

  mutex_.Lock();
//  bg_compaction_scheduled_ -= compactions_unscheduled;
  bg_flush_scheduled_ -= flushes_unscheduled;
  bg_dump_scheduled_ -= dump_unscheduled;

  // Wait for background work to finish
  while (bg_compaction_scheduled_ || bg_flush_scheduled_ || bg_dump_scheduled_ ||
         bg_purge_scheduled_ || bg_recycle_scheduled_ || master_thread_running()) {
    TEST_SYNC_POINT("DBImpl::~DBImpl:WaitJob");
    bg_cv_.Wait();
  }
  EraseThreadsStatusDbInfo();
  flush_scheduler_.Clear();

//  if (immutable_db_options_.compaction_type == 1 && compaction_scheduler_) {
//    compaction_scheduler_->stop();
//  }

  STFlushJob *flush_job = nullptr;
  while (!flush_queue_.empty()) {
    if (nullptr != (flush_job = PopFirstFromFlushQueue())) {
      if (nullptr != flush_job->sub_table_) {
        flush_job->sub_table_->set_pending_flush(false);
        flush_job->sub_table_->set_pending_compaction(false);
      }
      remove_flush_job(flush_job, false);
    }
  }
  STDumpJob *dump_job = nullptr;
  while (!dump_queue_.empty()) {
    if (nullptr != (dump_job = pop_front_dump_job())) {
      remove_dump_job(dump_job);
    }
  }
  for (int i = 0; i < CompactionPriority::ALL; i++) {
    while (!compaction_queue_[i].empty()) {
      auto cf_job = compaction_queue_[i].front();
      compaction_queue_[i].pop_front();
      remove_compaction_job(cf_job, false);
    }
  }

  for (auto& info : compaction_history_) {
    MOD_DELETE_OBJECT(CompactionJobStatsInfo, info);
  }

  while (!gc_queue_.empty()) {
    auto gc_job = pop_front_gc_job();
    remove_gc_job(gc_job);
  }

  if (default_cf_handle_ != nullptr) {
    // we need to delete handle outside of lock because it does its own locking
    mutex_.Unlock();
//    delete default_cf_handle_;
    MOD_DELETE_OBJECT(ColumnFamilyHandleImpl, default_cf_handle_);
    mutex_.Lock();
  }

  // Clean up obsolete files due to SuperVersion release.
  // (1) Need to delete to obsolete files before closing because RepairDB()
  // scans all existing files in the file system and builds manifest file.
  // Keeping obsolete files confuses the repair process.
  // (2) Need to check if we Open()/Recover() the DB successfully before
  // deleting because if VersionSet recover fails (may be due to corrupted
  // manifest file), it is not able to identify live files correctly. As a
  // result, all "live" files can get deleted by accident. However, corrupted
  // manifest is recoverable by RepairDB().
  if (opened_successfully_) {
    JobContext job_context(next_job_id_.fetch_add(1));
    FindObsoleteFiles(&job_context, true);

    mutex_.Unlock();
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
    mutex_.Lock();
  }

  if (IS_NOTNULL(curr_log_writer_)) {
    MOD_DELETE_OBJECT(Writer, curr_log_writer_);
  }

  // Table cache may have table handles holding blocks from the block cache.
  // We need to release them before the block cache is destroyed. The block
  // cache may be destroyed inside versions_.reset(), when column family data
  // list is destroyed, so leaving handles in table cache after
  // versions_.reset() may cause issues.
  // Here we clean all unreferenced handles in table cache.
  // Now we assume all user queries have finished, so only version set itself
  // can possibly hold the blocks from block cache. After releasing unreferenced
  // handles here, only handles held by version set left and inside
  // versions_.reset(), we will release them. There, we need to make sure every
  // time a handle is released, we erase it from the cache too. By doing that,
  // we can guarantee that after versions_.reset(), table cache is empty
  // so the cache can be safely destroyed.
  table_cache_->EraseUnRefEntries();

  delete_all_recovered_transactions();

  for (auto& info : memtable_cleanup_queue_) {
    ColumnFamilyData* cfd = info.cfd_;
    if (cfd->Unref()) {
      MOD_DELETE_OBJECT(ColumnFamilyData, cfd);
    }
  }

  auto *global_ctx = (versions_ != nullptr) ? versions_->get_global_ctx() : nullptr;
  // versions need to be destroyed before table_cache since it can hold
  // references to table_cache.
  versions_.reset();
  mutex_.Unlock();
  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  StorageLogger::get_instance().destroy();
  ExtentMetaManager::get_instance().destroy();
  ExtentSpaceManager::get_instance().destroy();
  BackupSnapshotImpl::get_instance()->destroy();
  WriteExtentJobScheduler::get_instance().stop();
  if (nullptr != global_ctx) {
    MOD_DELETE_OBJECT(GlobalContext, global_ctx);
  }
  SE_LOG(SYSTEM, "Shutdown complete");
}

void DBImpl::schedule_master_thread() {
  mutex_.AssertHeld();
  assert(opened_successfully_);
  env_->Schedule(&DBImpl::bg_master_func_wrapper, this, Env::Priority::MASTER, nullptr);
  //we must update master thread stats while holding db mutex
  master_thread_running_.store(true, std::memory_order_release);
}

void DBImpl::bg_master_func_wrapper(void* db) {
  TEST_SYNC_POINT("DBImpl::bg_master_thread:start");
  reinterpret_cast<DBImpl*>(db)->bg_master_thread_func();
  TEST_SYNC_POINT("DBImpl::bg_master_thread:end");
}

//TODO:should we use master thread or use a Timer Service?
void DBImpl::bg_master_thread_func() {
  //TODO: get HD and show in mysql show processlist;
  uint64_t max_schedule_interval_in_ms = 2 * 1000; //2 seconds
  uint64_t last_schedule_ts = env_->NowMicros() / 1000;
  uint64_t current_schedule_ts = 0;

  uint64_t last_stats_dump_ts = env_->NowMicros() / 1000;
  uint64_t last_gc_ts = last_stats_dump_ts;
  uint64_t last_cache_purge_ts = last_stats_dump_ts;
  uint64_t last_auto_compaction_ts = last_stats_dump_ts;
  uint64_t last_shrink_ts = last_stats_dump_ts;
  uint64_t last_ebr_ts = last_stats_dump_ts;
  uint64_t stats_dump_period_ms = mutable_db_options_.stats_dump_period_sec * 1000;
  uint64_t cache_purge_ms = 3000; // 3s
  uint64_t gc_ms = 5 * 1000 * 60; // 5min
  uint64_t shrink_ms = 30 * 1000 * 60; // 30min
  uint64_t ebr_ms = 2 * 1000; // 2 seconds
  uint64_t last_seq = versions_.get()->LastSequence();
  uint64_t sequence_step = 256;
  uint64_t last_monitor_ts = env_->NowMicros() / 1'000;
  bool idle_flag = true;
  SE_LOG(SYSTEM, "smartengine Master Thread online");
  int ret = 0;
  while (!db_shutting_down() && bg_error_.ok()) {
    uint64_t auto_compaction_ms = mutable_db_options_.idle_tasks_schedule_time * 1000;
    //caculate wait time in this loop
    current_schedule_ts = env_->NowMicros() / 1000;
    int64_t last_schedule_time_cost = current_schedule_ts - last_schedule_ts;
    int64_t sleep_time = max_schedule_interval_in_ms - last_schedule_time_cost;
    sleep_time = sleep_time > 0 ? sleep_time : 0;
    //TODO  use condition sleep 
    env_->SleepForMicroseconds(sleep_time * 1000);
    last_schedule_ts = env_->NowMicros() / 1000;
    // (1) schedule bg stats information output
    if (env_->NowMicros() / 1000 > last_stats_dump_ts + stats_dump_period_ms) {
      last_stats_dump_ts = env_->NowMicros() / 1000;
      QueryPerfContext::async_log_stats(env_, db_absolute_path_,
          block_cache_, immutable_db_options_.row_cache.get());
    }
    if (db_shutting_down() || !bg_error_.ok()) {
      break;
    }
    // (2) schedule bg row cache purge
    if (env_->NowMicros() / 1000 > last_cache_purge_ts + cache_purge_ms) {
      last_cache_purge_ts = env_->NowMicros() / 1000;
      RowCache *row_cache = immutable_db_options_.row_cache.get();
      if (nullptr != row_cache) {
        SE_LOG(DEBUG, "BG_TASK: cache purge", K(env_->NowMicros()));
        row_cache->schedule_cache_purge(env_);
      }
    }
    // (3) schedule gc task
    if (env_->NowMicros() / 1000 > last_gc_ts + gc_ms) {
      last_gc_ts = env_->NowMicros() / 1000;
      SE_LOG(DEBUG, "BG_TASK: gc task", K(env_->NowMicros()));
      schedule_gc();
    }
    //(a) check wal quota and usage
    if (total_log_size_ > GetMaxTotalWalSize() * 1.5) {
      // do switch
      WriteContext write_context;
      SE_LOG(INFO, "BG_TASK: force handle wal full", K(env_->NowMicros()));
      if (SUCCED(ret) && FAILED(force_handle_wal_full(&write_context))) {
        SE_LOG(WARN, "failed to handle wal", K(ret), K(total_log_size_));
      }
    }
    //(b) check memory quota and usage
    AllocMgr *alloc = AllocMgr::get_instance();
    if (nullptr != alloc && alloc->check_if_memory_overflow()) {
      stats_dump_period_ms = 10 * 1000;
    } else {
      stats_dump_period_ms = mutable_db_options_.stats_dump_period_sec * 1000;
    }
    //detect system workload
    // a.check wal generate rate for write workload
    // b.check cache read request and io read for read workload
    uint64_t cur_seq = versions_.get()->LastSequence();
    if (cur_seq - last_seq > sequence_step) {
      idle_flag = false;
    }
    last_seq = cur_seq;
    if (idle_flag) {
      if (mutable_db_options_.master_thread_compaction_enabled &&
          auto_compaction_ms > 0 &&
          env_->NowMicros() / 1000 >
              last_auto_compaction_ts + auto_compaction_ms &&
          SUCCED(ret)) {
        last_auto_compaction_ts = env_->NowMicros() / 1000;
        if (FAILED(master_schedule_compaction(
                CompactionScheduleType::MASTER_IDLE))) {
          SE_LOG(WARN, "failed to schedule compaction", K(ret));
        }
        // todo do idle checkpoint
      }
    } else {
      idle_flag = true;
    }
    // (4) schedule auto compaction task
    if (mutable_db_options_.master_thread_compaction_enabled &&
        env_->NowMicros() / 1000 >
            last_auto_compaction_ts + auto_compaction_ms &&
        SUCCED(ret)) {
      last_auto_compaction_ts = env_->NowMicros() / 1000;
      SE_LOG(DEBUG, "BG_TASK: schedule compaction", K(env_->NowMicros()));
      if (FAILED(master_schedule_compaction(CompactionScheduleType::MASTER_AUTO))) {
        SE_LOG(WARN, "failed to schedule compaction", K(ret));
      }
    }
    // (5) shrink task
    if ((env_->NowMicros() / 1000) > (last_shrink_ts + mutable_db_options_.auto_shrink_schedule_interval * 1000)
        && SUCCED(ret)) {
      last_shrink_ts = env_->NowMicros() / 1000;
      SE_LOG(INFO, "BG_TASK: shrink task", K(env_->NowMicros()));
      schedule_shrink();
    }
    // (6) epoch based reclaim task
    if (env_->NowMicros() / 1000 > last_ebr_ts + ebr_ms && SUCCED(ret)) {
      last_ebr_ts = env_->NowMicros() / 1000;
      SE_LOG(INFO, "BG_TASK: ebr task", K(env_->NowMicros()));
      schedule_ebr();
    }
    // (7) update and set monitor gauge value
    // In some core uts, stats_ maybe nullptr
    if (stats_ &&
        env_->NowMicros() / 1'000 >
            last_monitor_ts + mutable_db_options_.monitor_interval_ms) {
      last_monitor_ts = env_->NowMicros() / 1'000;
      SE_LOG(INFO, "BG_TASK: monitor task", K(env_->NowMicros()),
             K(mutable_db_options_.monitor_interval_ms));
      background_pull_gauge_statistics();
    }
  }
  //we should set stat while holding mutex_
  mutex_.Lock();
  master_thread_running_.store(false, std::memory_order_release);
  bg_cv_.SignalAll();
  mutex_.Unlock();
  SE_LOG(SYSTEM, "smartengine Master Thread go offline", K(ret), K((int)bg_error_.code()));
}

Directory* DBImpl::Directories::GetDataDir(size_t path_id) {
  assert(path_id < data_dirs_.size());
  Directory* ret_dir = data_dirs_[path_id];
  if (ret_dir == nullptr) {
    // Should use db_dir_
    return db_dir_.get();
  }
  return ret_dir;
}

int DBImpl::reset_pending_shrink(const uint64_t subtable_id) {
  int ret = Status::kOk;
  SubTable *subtable = nullptr;

  InstrumentedMutexLock mutex_guard(&mutex_);
  db::AllSubTableGuard all_subtable_guard(versions_->get_global_ctx());
  const db::SubTableMap &subtable_map = all_subtable_guard.get_subtable_map();
  auto iter = subtable_map.find(subtable_id);
  if (subtable_map.end() == iter) {
    ret = Status::kNotFound;
    SE_LOG(WARN, "this subtable not exist, can't reset pending shrink",
                K(ret), K(subtable_id));
  } else if (IS_NULL(subtable = iter->second)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret),
                K(subtable_id));
  } else {
    subtable->set_pending_shrink(false);
    SE_LOG(INFO, "success to reset pending shrink", K(subtable_id));
  }

  return ret;
}

Status DBImpl::SetOptions(const std::unordered_map<std::string, std::string>& options_map) {
  Status s;
  autovector<ColumnFamilyHandle *> cf_handles;
  mutex_.Lock();

  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  SubTable *sub_table = nullptr;
  ArenaAllocator tmp_alloc(8 * 1024);
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else {
    SubTableMap &all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
    for (auto iter = all_sub_tables.begin(); Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else {
        ColumnFamilyHandle *handle = ALLOC_OBJECT(ColumnFamilyHandleImpl, tmp_alloc, sub_table, this, &mutex_);
//            new ColumnFamilyHandleImpl(sub_table, this, &mutex_);
        cf_handles.push_back(handle);
      }
    }
  }

  mutex_.Unlock();

  bool do_dump = !cf_handles.empty();
  bool do_get_dump = true;
  ColumnFamilyOptions cf_options_to_dump;
  for (auto cf_handle: cf_handles) {
    if (s.ok()) {
      s = SetOptions(cf_handle, options_map);
    }
    if (do_get_dump && s.ok()) {
      auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cf_handle)->cfd();
      cf_options_to_dump = cfd->GetLatestCFOptions();
      do_get_dump = false;
    }
    FREE_OBJECT(ColumnFamilyHandle, tmp_alloc, cf_handle);
//    delete cf_handle;
  }
  if (s.ok() && do_dump) {
    cf_options_to_dump.Dump();
  }
  return s;
}

Status DBImpl::SetOptions(
    ColumnFamilyHandle* column_family,
    const std::unordered_map<std::string, std::string>& options_map) {
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (options_map.empty()) {
    SE_LOG(WARN, "SetOptions with empty input", "index_id", cfd->get_table_schema().get_index_id());
    return Status::InvalidArgument("empty input");
  }

  MutableCFOptions new_options;
  Status s;
  {
    InstrumentedMutexLock l(&mutex_);
    s = cfd->SetOptions(options_map);
    if (s.ok()) {
      // todo object pool
      SuperVersion *sv = MOD_NEW_OBJECT(ModId::kSuperVersion, SuperVersion);
      auto* old_sv = cfd->InstallSuperVersion(sv, &mutex_);
      MOD_DELETE_OBJECT(SuperVersion, old_sv);
      this->wait_all_active_thread_exit();
    }
  }

  std::ostringstream oss;
  oss << "inputs: {";
  for (const auto& o : options_map) {
    oss << o.first << ": " << o.second;
  }
  oss << "} " << (s.ok() ? "succeeded" : "failed");
  __SE_LOG(INFO, "SetOptions() on sub table [ID=%u] %s",
                cfd->GetID(), oss.str().c_str());
  return s;
}

Status DBImpl::SetDBOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
  if (options_map.empty()) {
    SE_LOG(WARN, "SetDBOptions(), empty input.");
    return Status::InvalidArgument("empty input");
  }

  MutableDBOptions new_options;
  Status s;
  WriteContext write_context;
  {
    InstrumentedMutexLock l(&mutex_);
    s = GetMutableDBOptionsFromStrings(mutable_db_options_, options_map,
                                       &new_options);
    if (s.ok()) {
      if (new_options.max_background_compactions >
          mutable_db_options_.max_background_compactions) {
        env_->IncBackgroundThreadsIfNeeded(
            new_options.max_background_compactions, Env::Priority::LOW);
        MaybeScheduleFlushOrCompaction();
        s = WriteExtentJobScheduler::get_instance().adjust_write_thread_count(
          initial_db_options_.max_background_flushes + new_options.max_background_compactions);
      }

      mutable_db_options_ = new_options;
      stats_dump_period_sec_.store(mutable_db_options_.stats_dump_period_sec);

      this->wait_all_active_thread_exit();
      if (total_log_size_ > GetMaxTotalWalSize()) {
        Status purge_wal_status = HandleWALFull(&write_context);
        if (!purge_wal_status.ok()) {
          __SE_LOG(WARN,
                         "Unable to purge WAL files in SetDBOptions() -- %s",
                         purge_wal_status.ToString().c_str());
        }
      }
    }
  }
  SE_LOG(INFO, "SetDBOptions(), inputs:");
  for (const auto& o : options_map) {
    __SE_LOG(INFO, "%s: %s\n", o.first.c_str(),
                   o.second.c_str());
  }
  if (s.ok()) {
    SE_LOG(INFO, "SetDBOptions() succeeded");
    new_options.Dump();
  } else {
    SE_LOG(WARN, "SetDBOptions failed");
  }
  return s;
}

int DBImpl::sync_wal()
{
  int ret = Status::kOk;
  bool need_sync = true;

  QUERY_TRACE_SCOPE(TracePoint::DB_SYNC_WAL);
  std::unique_lock<std::mutex> log_sync_guard(log_sync_mutex_);
  while (is_syncing_.load()) {
    log_sync_cv_.wait(log_sync_guard);
    need_sync = false;
  }

  if (need_sync) {
    is_syncing_.store(true);
    log_sync_guard.unlock();
    ret = curr_log_writer_->file()->sync_to_disk(false);
    QUERY_COUNT(CountPoint::WAL_FILE_SYNCED);
    log_sync_guard.lock();
    is_syncing_.store(false);
    log_sync_cv_.notify_all();
  }
    
  if (FAILED(ret)) {
    SE_LOG(ERROR, "fail to sync wal", K(ret));
  }

  return ret;
}

SequenceNumber DBImpl::GetLatestSequenceNumber() const {
  return versions_->LastSequence();
}

void DBImpl::SchedulePurge() {
  mutex_.AssertHeld();
  assert(opened_successfully_);

  // Purge operations are put into High priority queue
  bg_purge_scheduled_++;
  env_->Schedule(&DBImpl::BGWorkPurge, this, Env::Priority::HIGH, nullptr);
}

void DBImpl::BackgroundCallPurge() {
  mutex_.Lock();

  // We use one single loop to clear both queues so that after existing the loop
  // both queues are empty. This is stricter than what is needed, but can make
  // it easier for us to reason the correctness.
  while (!purge_queue_.empty()) {
    auto purge_file = purge_queue_.begin();
    auto fname = purge_file->fname;
    auto type = purge_file->type;
    auto number = purge_file->number;
    auto path_id = purge_file->path_id;
    auto job_id = purge_file->job_id;
    purge_queue_.pop_front();

    mutex_.Unlock();
    Status file_deletion_status;
    DeleteObsoleteFileImpl(file_deletion_status, job_id, fname, type, number,
                           path_id);
    mutex_.Lock();
  }
  bg_purge_scheduled_--;

  bg_cv_.SignalAll();
  // IMPORTANT:there should be no code after calling SignalAll. This call may
  // signal the DB destructor that it's OK to proceed with destruction. In
  // that case, all DB variables will be dealloacated and referencing them
  // will cause trouble.
  mutex_.Unlock();
}

namespace {
struct IterState {
  IterState(DBImpl* _db, InstrumentedMutex* _mu, SuperVersion* _super_version,
            bool _background_purge)
      : db(_db),
        mu(_mu),
        super_version(_super_version),
        background_purge(_background_purge) {}

  DBImpl* db;
  InstrumentedMutex* mu;
  SuperVersion* super_version;
  bool background_purge;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  ColumnFamilyData* cfd = reinterpret_cast<ColumnFamilyData*>(arg2);

  if (state->super_version->Unref()) {
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    JobContext job_context(0);

    state->mu->Lock();
    cfd->release_meta_snapshot(state->super_version->current_meta_, 
                               state->mu);
    state->super_version->Cleanup();
    state->db->FindObsoleteFiles(&job_context, false, true);
    state->mu->Unlock();

    MOD_DELETE_OBJECT(SuperVersion, state->super_version);
    if (job_context.HaveSomethingToDelete()) {
      if (state->background_purge) {
        // PurgeObsoleteFiles here does not delete files. Instead, it adds the
        // files to be deleted to a job queue, and deletes it in a separate
        // background thread.
        state->db->PurgeObsoleteFiles(job_context, true /* schedule only */);
        state->mu->Lock();
        state->db->SchedulePurge();
        state->mu->Unlock();
      } else {
        state->db->PurgeObsoleteFiles(job_context);
      }
    }
    job_context.Clean();
  }

  delete state;
}
}  // namespace

InternalIterator* DBImpl::NewInternalIterator(const ReadOptions& read_options,
                                              ColumnFamilyData* cfd,
                                              SuperVersion* super_version,
                                              Arena* arena)
{
  InternalIterator* internal_iter;
  assert(arena != nullptr);
  // Need to create internal iterator from the arena.
  MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(), arena);

  Status s;
  if(kOnlyL2 != read_options.read_level_ ){
    // Collect iterator for mutable mem
    QUERY_TRACE_BEGIN(TracePoint::DB_ITER_ADD_MEM);
    merge_iter_builder.AddIterator(
        super_version->mem->NewIterator(read_options, arena));
    // Collect all needed child iterators for immutable memtables
    if (s.ok()) {
      super_version->imm->AddIterators(read_options, &merge_iter_builder);
    }
    QUERY_TRACE_END(); // end for DB_ITER_ADD_MEM
  }
  if (s.ok()) {
    QUERY_TRACE_SCOPE(TracePoint::DB_ITER_ADD_STORAGE);
    // Collect iterators for files in L0 - Ln
    if (read_options.read_tier != kMemtableTier) {
      // TODO(Zhao Dongsheng): storge manager should not export.
      cfd->get_storage_manager()->add_iterators(cfd->table_cache(), 
                                                cfd->internal_stats(),
                                                read_options,
                                                &merge_iter_builder,
                                                super_version->current_meta_);
    }
    internal_iter = merge_iter_builder.Finish();
    // todo new
    IterState* cleanup =
        new IterState(this, &mutex_, super_version,
                      read_options.background_purge_on_iterator_cleanup);
    // pass the cfd to return the supper version
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, cfd);
    return internal_iter;
  }
  return NewErrorInternalIterator(s);
}

ColumnFamilyHandle* DBImpl::DefaultColumnFamily() const {
  return default_cf_handle_;
}

Status DBImpl::Get(const ReadOptions& read_options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   PinnableSlice* value) {
  return GetImpl(read_options, column_family, key, value);
}

int DBImpl::get_from_row_cache(const ColumnFamilyData *cfd,
                               const SequenceNumber snapshot,
                               const Slice& key,
                               IterKey &row_cache_key,
                               PinnableSlice *&pinnable_val,
                               bool &done) {
  int ret = 0;
  RowCache *row_cache_ptr = immutable_db_options_.row_cache.get();
  if (nullptr != row_cache_ptr && nullptr != cfd) {
    uint32_t cfd_id = cfd->GetID();
    // 1. cfd id
    char buf[10];
    char* ptr = EncodeVarint32(buf, cfd_id);
    row_cache_key.TrimAppend(row_cache_key.Size(), buf, ptr - buf);
    // 2. key's data
    row_cache_key.TrimAppend(row_cache_key.Size(), key.data(), key.size());
    // 3. get value from row_cache
    cache::Cache::Handle *row_handle = nullptr;
    if (FAILED(immutable_db_options_.row_cache->lookup_row(row_cache_key.GetUserKey(), snapshot, cfd, row_handle))) {
      SE_LOG(WARN, "fail to lookup row from row cache", K(ret));
    } else if (nullptr != row_handle) {
      const RowcHandle* found_row = reinterpret_cast<const RowcHandle *>(row_handle);
      if (nullptr != found_row && found_row->seq_ <= snapshot) {
        done = true;
        int64_t handle_size = sizeof(RowcHandle) - 1 + found_row->key_length_;
        pinnable_val->PinSelf(Slice((char *)found_row + handle_size, found_row->charge_ - handle_size));
        QUERY_COUNT(CountPoint::ROW_CACHE_HIT);
      }
      row_cache_ptr->release(row_handle);
    }
    if (!done) {
      QUERY_COUNT(CountPoint::ROW_CACHE_MISS);
    }
  }
  // If row cache is not open, record neither hit nor miss.
  return ret;
}

int DBImpl::add_into_row_cache(
    const void* data,
    const size_t data_size,
    const common::SequenceNumber snapshot,
    const db::ColumnFamilyData *cfd,
    const uint64_t key_seq,
    IterKey &row_cache_key) {
  int ret = 0;
  if (nullptr != data) {
    if (FAILED(immutable_db_options_.row_cache->insert_row(
        row_cache_key.GetUserKey(), data, data_size, key_seq, cfd, snapshot))) {
      SE_LOG(WARN, "failed to insert new row",
          K(ret), K(snapshot), K(Slice((char *)data, data_size)));
    } else {
      QUERY_COUNT(CountPoint::ROW_CACHE_ADD);
    }
  }
  return ret;
}

Status DBImpl::GetImpl(const ReadOptions& read_options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       PinnableSlice* pinnable_val, bool* value_found) {
  QUERY_TRACE_SCOPE(TracePoint::GET_IMPL);
  assert(pinnable_val != nullptr);

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  // Acquire SuperVersion
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  TEST_SYNC_POINT("DBImpl::GetImpl:1");
  TEST_SYNC_POINT("DBImpl::GetImpl:2");

  SequenceNumber snapshot;
  if (read_options.snapshot != nullptr) {
    snapshot =
        reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
  } else {
    // Since we get and reference the super version before getting
    // the snapshot number, without a mutex protection, it is possible
    // that a memtable switch happened in the middle and not all the
    // data for this snapshot is available. But it will contain all
    // the data available in the super version we have, which is also
    // a valid snapshot to read from.
    // We shouldn't get snapshot before finding and referencing the
    // super versipon because a flush happening in between may compact
    // away data for the snapshot, but the snapshot is earlier than the
    // data overwriting it, so users may see wrong results.
    snapshot = versions_->LastSequence();
  }

  STRESS_CHECK_SAVE(LAST_GET_SEQUENCE, snapshot);
  TEST_SYNC_POINT("DBImpl::GetImpl:3");
  TEST_SYNC_POINT("DBImpl::GetImpl:4");

  Status s;
  int ret = Status::kOk;
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  LookupKey lkey(key, snapshot);

  bool skip_memtable = (read_options.read_tier == kPersistedTier &&
                        has_unpersisted_data_.load(std::memory_order_relaxed));
  bool done = false;
  if (!skip_memtable) {
    if (sv->mem->Get(lkey,
                     pinnable_val->GetSelf(),
                     &s,
                     read_options)) {
      done = true;
      pinnable_val->PinSelf();
      QUERY_COUNT(CountPoint::MEMTABLE_HIT);
    } else if (s.ok() && sv->imm->Get(lkey,
                                      pinnable_val->GetSelf(),
                                      &s,
                                      read_options)) {
      done = true;
      pinnable_val->PinSelf();
      QUERY_COUNT(CountPoint::MEMTABLE_HIT);
    }
    if (!done && !s.ok()) {
      return s;
    }
  }

  STRESS_CHECK_SAVE(MEMTABLE_SEARCH_DONE, done);
  if (!done) {
    QUERY_COUNT(CountPoint::MEMTABLE_MISS);
  }
  // look up in row_cache
  IterKey* row_cache_key_ptr = nullptr;
  IterKey row_cache_key;
  if (!done && immutable_db_options_.row_cache) {
    //const SeSchema *schema = cfd->get_schema();
    if (FAILED(get_from_row_cache(cfd, snapshot, key, row_cache_key, pinnable_val, done))) {
      s = Status(ret);
      SE_LOG(WARN, "failed to get row from row_cache", K(ret));
    }
  }

  if (!done) {
    SequenceNumber key_seq = kMaxSequenceNumber;
    if (FAILED(cfd->get_from_storage_manager(read_options,
                                             *sv->current_meta_,
                                             lkey,
                                             *pinnable_val,
                                             done,
                                             &key_seq))) {
      s = Status(ret);
      if (Status::kNotFound != ret) {
        SE_LOG(WARN, "fail to get from storage manager", K(ret));
      } else {
        //QUERY_COUNT(CountPoint::VALUE_NOT_FOUND);
        if (nullptr != value_found) {
          *value_found = done;
        }
      }
    }
    // insert into row cache
    if (s.ok() && immutable_db_options_.row_cache) {
      const char* val_data = nullptr;
      size_t val_size = 0;
      if (pinnable_val->IsPinned()) {
        val_data = pinnable_val->data();
        val_size = pinnable_val->size();
      } else if (nullptr != pinnable_val->GetSelf()) {
        val_data = pinnable_val->GetSelf()->data();
        val_size = pinnable_val->GetSelf()->size();
      }
      if (val_size) {
        // add into row cache
        if (FAILED(add_into_row_cache(
            val_data, val_size, snapshot, cfd, key_seq, row_cache_key))) {
          s = Status(ret);
          SE_LOG(WARN, "failed to add into row cache", K(ret));
        }
      }
    }
  }

  ReturnAndCleanupSuperVersion(cfd, sv);

  QUERY_COUNT(CountPoint::NUMBER_KEYS_READ);
  QUERY_COUNT_ADD(CountPoint::NUMBER_BYTES_READ, pinnable_val->size());
  return s;
}

Status DBImpl::CreateColumnFamily(CreateSubTableArgs &args, ColumnFamilyHandle **handle)
{
  int ret = Status::kOk;
  ColumnFamilyData *cfd = nullptr;
  int64_t dummy_commit_lsn = 0;

  if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (FAILED(StorageLogger::get_instance().begin(CREATE_INDEX))) {
    //TODO:yuanfeng fail?abort?
    SE_LOG(WARN, "fail to begin trans", K(ret));
  } else {
    if (args.create_table_space_) {
      args.table_space_id_ = ExtentSpaceManager::get_instance().allocate_table_space_id();
    }
    InstrumentedMutexLock guard(&mutex_);
    if (FAILED(versions_->add_sub_table(args, true, false /*is replay*/, cfd))) {
      SE_LOG(WARN, "fail to add sub table", K(args));
    } else if (FAILED(StorageLogger::get_instance().commit(dummy_commit_lsn))) {
      SE_LOG(WARN, "fail to commit trans", K(ret));
      abort();
    } else {
      SuperVersion *old_sv = InstallSuperVersionAndScheduleWork(cfd, nullptr);
      MOD_DELETE_OBJECT(SuperVersion, old_sv);
    }
  }

  if (SUCCED(ret)) {
    if (args.create_table_space_
        && FAILED(ExtentSpaceManager::get_instance().create_table_space(args.table_space_id_))) {
      SE_LOG(WARN, "fail to create table space", K(args));
    } else if (FAILED(ExtentSpaceManager::get_instance().register_subtable(args.table_space_id_, cfd->GetID()))) { 
      SE_LOG(WARN, "fail to regirster subtable", K(ret), K(args));
    } else if (IS_NULL(*handle = MOD_NEW_OBJECT(ModId::kDBImpl, ColumnFamilyHandleImpl, cfd, this, &mutex_))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for ColumnFamilyHandleImpl", K(ret));
    } else {
      SE_LOG(INFO, "success to create subtbale", K(args));
    }
  }

  return ret;
}

Status DBImpl::DropColumnFamily(ColumnFamilyHandle *column_family)
{
  int ret = Status::kOk;
  ColumnFamilyData *cfd = nullptr;
  int64_t dummy_commit_lsn = 0;

  if (IS_NULL(column_family)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(column_family));
  } else if (IS_NULL(cfd = (reinterpret_cast<ColumnFamilyHandleImpl *>(column_family))->cfd())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, cfd must not nullptr", K(ret));
  } else if (FAILED(StorageLogger::get_instance().begin(DROP_INDEX))) {
    //TODO:yuanfeng abort?
    SE_LOG(WARN, "fail to begin manifest trans", K(ret));
  } else {
    InstrumentedMutexLock guard(&mutex_);
    if (FAILED(versions_->remove_sub_table(cfd, true, false /*is replay*/))) {
      SE_LOG(WARN, "fail to remove subtable", K(ret), "index_id", cfd->GetID());
    } else if (FAILED(StorageLogger::get_instance().commit(dummy_commit_lsn))) {
      SE_LOG(WARN, "fail to commit trans", K(ret));
      abort();
    } else {
      schedule_pending_gc(cfd);
      SE_LOG(INFO, "success to remove subtable", K(cfd->GetID()));
    }
  }

  return ret;
}

int DBImpl::modify_table_schema(ColumnFamilyHandle *subtable_handle, const schema::TableSchema &table_schema)
{
  int ret = Status::kOk;
  int64_t dummy_commit_lsn = 0;
  ColumnFamilyData *subtable = nullptr;

  /**The subtable_handle has acquired a refrence of subtable, can use it safely.*/
  // TODO (Zhao Dongsheng) : Is it unnecessary to lock here?
  InstrumentedMutexLock guard(&mutex_);
  if (IS_NULL(subtable_handle) || UNLIKELY(!table_schema.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(subtable_handle), K(table_schema));
  } else if (IS_NULL(subtable = reinterpret_cast<ColumnFamilyHandleImpl *>(subtable_handle)->cfd())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "subtable must not been nullptr", K(ret));
  } else if (FAILED(versions_->modify_table_schema(subtable, table_schema))) {
    SE_LOG(WARN, "fail to modify table schema", K(ret), "index_id", subtable->GetID(), K(table_schema));
  } else {
    // succeed
  }

  return ret;
}

Iterator* DBImpl::NewIterator(const ReadOptions& read_options,
                              ColumnFamilyHandle* column_family) {
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_CREATE);
  if (read_options.read_tier == kPersistedTier) {
    return NewErrorIterator(Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators."));
  }
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  QUERY_TRACE_BEGIN(TracePoint::DB_ITER_NEW_OBJECT);
  SequenceNumber latest_snapshot = versions_->LastSequence();
  SuperVersion* sv = cfd->GetReferencedSuperVersion(&mutex_);

  auto snapshot =
      read_options.snapshot != nullptr
          ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                ->number_
          : latest_snapshot;
  STRESS_CHECK_SAVE(LAST_ITER_SEQUENCE, snapshot);

  // Try to generate a DB iterator tree in continuous memory area to be
  // cache friendly. Here is an example of result:
  // +-------------------------------+
  // |                               |
  // | ArenaWrappedDBIter            |
  // |  +                            |
  // |  +---> Inner Iterator   ------------+
  // |  |                            |     |
  // |  |    +-- -- -- -- -- -- -- --+     |
  // |  +--- | Arena                 |     |
  // |       |                       |     |
  // |          Allocated Memory:    |     |
  // |       |   +-------------------+     |
  // |       |   | DBIter            | <---+
  // |           |  +                |
  // |       |   |  +-> iter_  ------------+
  // |       |   |                   |     |
  // |       |   +-------------------+     |
  // |       |   | MergingIterator   | <---+
  // |           |  +                |
  // |       |   |  +->child iter1  ------------+
  // |       |   |  |                |          |
  // |           |  +->child iter2  ----------+ |
  // |       |   |  |                |        | |
  // |       |   |  +->child iter3  --------+ | |
  // |           |                   |      | | |
  // |       |   +-------------------+      | | |
  // |       |   | Iterator1         | <--------+
  // |       |   +-------------------+      | |
  // |       |   | Iterator2         | <------+
  // |       |   +-------------------+      |
  // |       |   | Iterator3         | <----+
  // |       |   +-------------------+
  // |       |                       |
  // +-------+-----------------------+
  //
  // ArenaWrappedDBIter inlines an arena area where all the iterators in
  // the iterator tree are allocated in the order of being accessed when
  // querying.
  // Laying out the iterators in the order of being accessed makes it more
  // likely that any iterator pointer is close to the iterator it points to so
  // that they are likely to be in the same cache line and/or page.
  ArenaWrappedDBIter* db_iter = NewArenaWrappedDbIterator(
      env_,
      read_options,
      *cfd->ioptions(),
      cfd->user_comparator(),
      snapshot);
  QUERY_TRACE_END();

  InternalIterator* internal_iter =
      NewInternalIterator(read_options, cfd, sv, db_iter->GetArena());
  db_iter->SetIterUnderDBIter(internal_iter);

  return db_iter;
}

const Snapshot* DBImpl::GetSnapshot() { return GetSnapshotImpl(false); }

const Snapshot* DBImpl::GetSnapshotForWriteConflictBoundary() {
  return GetSnapshotImpl(true);
}

/* Get/Release Snapshot Optimize */
/*
 * Split snapshot lists into MAX_SNAP, and one list is protected by one
 * corresponding mutex.
 *
 * For Get Snapshot, try to lock one list, if all failed, then lock the
 * first list.
 * Then try to do list.newest->ref++ if list is not empty, else create a
 * new node and insert into list as the newest node.
 * Last, unlock the list
 *
 * For Release Snapshot, do node->ref-- and check if it's 0, means nobody
 * is using this node.
 * If 0, then lock the list, and Delete this node, Unlock list, free node
 * if needed.
 *
 * Some trickery issues:
 * As release first do node->ref-- without lock protected, then after
 * decrease, ref == 0, but during this period, Get Snapshot has also get
 * this node and do node->ref++, and also release this node node->ref--.
 * At this time, two threads are both waiting lock with ref == 0, both
 * of them will DELETE node from list.
 *
 * Solution: For Get Snapshot, if node->ref++ == 1, which means some thread
 * called Release and waiting Lock to Delete node, then revert it and create
 * a new node.
 *
 */
const Snapshot* DBImpl::GetSnapshotImpl(bool is_write_conflict_boundary) {
  int64_t unix_time = 0;
  env_->GetCurrentTime(&unix_time);  // Ignore error
  SnapshotImpl *s = nullptr;
  SnapshotImpl *head = nullptr;
  const SnapshotImpl *ns = nullptr;
  int32_t old_ref = INT32_MAX;

  // returns null if the underlying memtable does not support snapshot.
  if (!is_snapshot_supported_) return nullptr;

  uint32_t idx = 0;
  while (idx < MAX_SNAP) {
    if (snap_mutex[idx].TryLock() == 0) break;
    idx++;
  }
  if (idx == MAX_SNAP) {
    idx = 0;
    snap_mutex[idx].Lock();
  }

  bool need_create = true;
  if (!snap_lists_[idx].empty()) {
    head = snap_lists_[idx].newest();
    if (head->number_ == versions_->LastSequence()) {
      head->ref(&old_ref);
      if (0 == old_ref) {
        // if old ref is 0, don't ref
        head->unref(nullptr);
      } else if (old_ref > 0) {
        ns = head;
        need_create = false;
      } else {
        assert(0);
      }
    }
  }

  if (need_create) {
    s = MOD_NEW_OBJECT(ModId::kSnapshotImpl, SnapshotImpl);
    ns = snap_lists_[idx].New(s, versions_->LastSequence(), unix_time,
                              is_write_conflict_boundary, idx);
  }

  snap_mutex[idx].Unlock();

  return ns;
}

void DBImpl::ReleaseSnapshot(const Snapshot *s) {
  int64_t n_ref = 0;
  bool destroy = false;

  SnapshotImpl* casted_s =
      const_cast<SnapshotImpl*>(reinterpret_cast<const SnapshotImpl*>(s));

  int32_t old_ref = INT32_MAX;
  {
    std::unique_lock lock(casted_s->destroy_mutex_);
    if (casted_s->unref(&old_ref)) {
      destroy = true;
    }
    assert(old_ref >= 1);

    if (1 == old_ref) {
      uint32_t pos = casted_s->pos();
      assert(pos < MAX_SNAP);
      snap_mutex[pos].Lock();
      snap_lists_[pos].Delete(casted_s);
      snap_mutex[pos].Unlock();
    }
  }

  if (destroy) {
    MOD_DELETE_OBJECT(SnapshotImpl, casted_s);
  }
}

const std::string& DBImpl::GetName() const { return dbname_; }

Env* DBImpl::GetEnv() const { return env_; }

DBOptions DBImpl::GetDBOptions() const {
  InstrumentedMutexLock l(&mutex_);
  return BuildDBOptions(immutable_db_options_, mutable_db_options_);
}

bool DBImpl::GetProperty(ColumnFamilyHandle* column_family,
                         const Slice& property, std::string* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_int) {
    uint64_t int_value;
    bool ret_value =
        GetIntPropertyInternal(cfd, *property_info, false, &int_value);
    if (ret_value) {
      *value = ToString(int_value);
    }
    return ret_value;
  } else if (property_info->handle_string) {
    InstrumentedMutexLock l(&mutex_);
    return cfd->internal_stats()->GetStringProperty(*property_info, property,
                                                    value, this);
  }
  // Shouldn't reach here since exactly one of handle_string and handle_int
  // should be non-nullptr.
  assert(false);
  return false;
}

bool DBImpl::GetMapProperty(ColumnFamilyHandle* column_family,
                            const Slice& property,
                            std::map<std::string, double>* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_map) {
    InstrumentedMutexLock l(&mutex_);
    return cfd->internal_stats()->GetMapProperty(*property_info, property,
                                                 value);
  }
  // If we reach this point it means that handle_map is not provided for the
  // requested property
  return false;
}

bool DBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, uint64_t* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    return false;
  }
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  return GetIntPropertyInternal(cfd, *property_info, false, value);
}

bool DBImpl::GetIntPropertyInternal(ColumnFamilyData* cfd,
                                    const DBPropertyInfo& property_info,
                                    bool is_locked, uint64_t* value) {
  assert(property_info.handle_int != nullptr);
  if (!property_info.need_out_of_mutex) {
    if (is_locked) {
      mutex_.AssertHeld();
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    }
  } else {
    SuperVersion* sv = nullptr;
    if (!is_locked) {
      sv = GetAndRefSuperVersion(cfd);
    } else {
      sv = cfd->GetSuperVersion();
    }

    /*
    bool ret = cfd->internal_stats()->GetIntPropertyOutOfMutex(
        property_info, sv->current, value);
    */
    if (!is_locked) {
      ReturnAndCleanupSuperVersion(cfd, sv);
    }

    //return ret;
    return true;
  }
}

bool DBImpl::GetAggregatedIntProperty(const Slice& property,
                                      uint64_t* aggregated_value) {
 int ret = Status::kOk;
  bool bool_ret = false;
  int tmp_ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  SubTable *sub_table = nullptr;
  uint64_t sum = 0;
  uint64_t value = 0;
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    bool_ret = false;
  } else if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    bool_ret = false;
    SE_LOG(WARN, "unexpected erroe, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    bool_ret = false;
    SE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    bool_ret = false;
    SE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    SubTableMap &all_sub_tables = all_sub_table->sub_table_map_;
    for (auto iter = all_sub_tables.begin(); bool_ret && iter != all_sub_tables.end(); ++iter) {                                                                                            
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kErrorUnexpected;
        bool_ret = false;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), "index_id", iter->first);
      } else if (!GetIntPropertyInternal(sub_table, *property_info, true, &value))  {
        bool_ret = false;
      } else {
        sum += value;
      }
    }
    *aggregated_value = sum;
  }

  //there will cover the error code, by design
  tmp_ret = ret;
  if (nullptr != global_ctx && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {                                                                                     
    bool_ret = false;
    SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }
  return bool_ret;
}

SuperVersion* DBImpl::GetAndRefSuperVersion(ColumnFamilyData* cfd) {
  // TODO(ljin): consider using GetReferencedSuperVersion() directly
  return cfd->GetThreadLocalSuperVersion(&mutex_);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
SuperVersion* DBImpl::GetAndRefSuperVersion(uint32_t column_family_id) {
  GlobalContext *global_ctx = nullptr;
  SubTable *sub_table = nullptr;
  SuperVersion * super_version = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    SE_LOG(WARN, "global ctx must not nullptr");
  } else {
    auto iter = global_ctx->all_sub_table_->sub_table_map_.find(column_family_id);
    if (global_ctx->all_sub_table_->sub_table_map_.end() == iter) {
      SE_LOG(DEBUG, "sub table not exist", K(column_family_id));
    } else if (nullptr == (sub_table = iter->second)) {
      SE_LOG(WARN, "subtable must not nullptr", K(column_family_id));
    } else {
      super_version = GetAndRefSuperVersion(sub_table);
    }
  }

  return super_version;
}

void DBImpl::ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd,
                                          SuperVersion* sv) {
  bool unref_sv = !cfd->ReturnThreadLocalSuperVersion(sv);

  if (unref_sv) {
    // Release SuperVersion
    if (sv->Unref()) {
      {
        InstrumentedMutexLock l(&mutex_);
        if (sv->current_meta_) {
          cfd->release_meta_snapshot(sv->current_meta_,
                                     &mutex_);
        }
        sv->Cleanup();
      }
      MOD_DELETE_OBJECT(SuperVersion, sv);
      QUERY_COUNT(CountPoint::NUMBER_SUPERVERSION_CLEANUPS);
    }
    QUERY_COUNT(CountPoint::NUMBER_SUPERVERSION_RELEASES);
  }
}

// REQUIRED: this function should only be called on the write thread.
void DBImpl::ReturnAndCleanupSuperVersion(uint32_t column_family_id,
                                          SuperVersion* sv) {
  // If SuperVersion is held, and we successfully fetched a cfd using
  // GetAndRefSuperVersion(), it must still exist.
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  SubTable *sub_table = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    auto iter = all_sub_table->sub_table_map_.find(column_family_id);
    if (global_ctx->all_sub_table_->sub_table_map_.end() == iter) {
      SE_LOG(WARN, "subtable not exist", K(column_family_id));
      assert(nullptr != sub_table);
    } else if (nullptr == (sub_table = iter->second)) {
      SE_LOG(WARN, "subtable must not nullptr", K(column_family_id));
      assert(nullptr != sub_table);
    } else {
      ReturnAndCleanupSuperVersion(sub_table, sv);
    }
  }

  //there will cover the error code, by design
  tmp_ret = ret;
  if (nullptr != global_ctx && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to releaser all sub table", K(ret), K(tmp_ret));
  }
}

// return the all subtable handles to caller to process
// COUTION: caller need to release the handles
int DBImpl::get_all_subtable(
    std::vector<smartengine::db::ColumnFamilyHandle*>& subtables) const
{
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  GlobalContext* global_ctx = nullptr;
  AllSubTable* all_sub_table = nullptr;
  ColumnFamilyHandleImpl* handle = nullptr;

  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(
                 all_sub_table))) {
    SE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, all sub table must not nullptr",
                K(ret));
  } else {
    for (auto item : all_sub_table->sub_table_map_) {
      handle =
          MOD_NEW_OBJECT(ModId::kInformationSchema, ColumnFamilyHandleImpl,
                         item.second, const_cast<DBImpl*>(this), &mutex_);
      subtables.emplace_back(
          static_cast<smartengine::db::ColumnFamilyHandle*>(handle));
    }
  }

  // there will cover the error code, by design
  tmp_ret = ret;
  if (nullptr != global_ctx &&
      FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }

  return ret;
}

int DBImpl::return_all_subtable(std::vector<smartengine::db::ColumnFamilyHandle*> &subtables) {
  // release the reference and space
  ColumnFamilyHandleImpl *tmp_ptr = nullptr;
  for (auto st_handle : subtables) {
    if (nullptr == st_handle) {
      continue;
    }
    tmp_ptr = static_cast<ColumnFamilyHandleImpl*>(st_handle);
    MOD_DELETE_OBJECT(ColumnFamilyHandleImpl, tmp_ptr);
  }

  return Status::kOk;
}

void DBImpl::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                         const Range& range,
                                         uint64_t* const count,
                                         uint64_t* const size) {
  QUERY_TRACE_SCOPE(TracePoint::DB_APPROXIMATE_MEM_SIZE);
  ColumnFamilyHandleImpl* cfh =
      reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  // Convert user_key into a corresponding internal key.
  InternalKey k1(range.start, kMaxSequenceNumber, kValueTypeForSeek);
  InternalKey k2(range.limit, kMaxSequenceNumber, kValueTypeForSeek);
  MemTable::MemTableStats memStats =
      sv->mem->ApproximateStats(k1.Encode(), k2.Encode());
  MemTable::MemTableStats immStats =
      sv->imm->ApproximateStats(k1.Encode(), k2.Encode());
  *count = memStats.count + immStats.count;
  *size = memStats.size + immStats.size;

  ReturnAndCleanupSuperVersion(cfd, sv);
}

void DBImpl::GetApproximateSizes(ColumnFamilyHandle* column_family,
                                 const Range* range, int n, uint64_t* sizes,
                                 uint8_t include_flags) {
  QUERY_TRACE_SCOPE(TracePoint::DB_APPROXIMATE_SIZE);
  assert(include_flags & DB::SizeApproximationFlags::INCLUDE_FILES ||
         include_flags & DB::SizeApproximationFlags::INCLUDE_MEMTABLES);
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    sizes[i] = 0;
    if (include_flags & DB::SizeApproximationFlags::INCLUDE_FILES) {
      sizes[i] += versions_->ApproximateSize(cfd, sv->current_meta_,
                                             k1.Encode(), k2.Encode(),
                                             0 /*start_level*/, 3 /*end_level*/,
                                             mutable_db_options_.estimate_cost_depth);
    }
    if (include_flags & DB::SizeApproximationFlags::INCLUDE_MEMTABLES) {
      sizes[i] += sv->mem->ApproximateStats(k1.Encode(), k2.Encode()).size;
      sizes[i] += sv->imm->ApproximateStats(k1.Encode(), k2.Encode()).size;
    }
  }

  ReturnAndCleanupSuperVersion(cfd, sv);
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  const ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;
  std::vector<std::string> filenames;

  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);

  FileLock* lock;
  const std::string lockname = FileNameUtil::lock_file_path(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    int64_t number = 0;
    FileType type = util::kInvalidFileType;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (Status::kOk == FileNameUtil::parse_file_name(filenames[i], number, type) &&
          type != kLockFile) {  // Lock file will be deleted at end
        Status del;
        std::string path_to_delete = dbname + "/" + filenames[i];
        if (kDataFile == type) {
          del = DeleteSSTFile(&soptions, path_to_delete, 0);
        } else {
          del = env->DeleteFile(path_to_delete);
        }
        if (result.ok() && !del.ok()) {
          result = del;
        }
      } else if (filenames[i][0] != '.') {
        std::string path_to_delete = dbname + "/" + filenames[i];
        env->DeleteFile(path_to_delete);
      }
    }

    for (size_t path_id = 0; path_id < options.db_paths.size(); path_id++) {
      const auto& db_path = options.db_paths[path_id];
      env->GetChildren(db_path.path, &filenames);
      for (size_t i = 0; i < filenames.size(); i++) {
        if (Status::kOk == FileNameUtil::parse_file_name(filenames[i], number, type) &&
            kDataFile == type) {  // Lock file will be deleted at end
          std::string table_path = db_path.path + "/" + filenames[i];
          Status del = DeleteSSTFile(&soptions, table_path,
                                     static_cast<uint32_t>(path_id));
          if (result.ok() && !del.ok()) {
            result = del;
          }
        }
      }
    }

    std::vector<std::string> walDirFiles;
    if (dbname != soptions.wal_dir) {
      env->GetChildren(soptions.wal_dir, &walDirFiles);
    }

    // Delete log files in the WAL dir
    for (const auto& file : walDirFiles) {
      if (Status::kOk == FileNameUtil::parse_file_name(file, number, type) && kWalFile == type) {
        Status del = env->DeleteFile(soptions.wal_dir + "/" + file);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }

    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
    env->DeleteDir(soptions.wal_dir);
  }
  return result;
}


#ifdef ROCKSDB_USING_THREAD_STATUS

void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::NewColumnFamilyInfo(this,
                                          cfd,
                                          std::to_string(cfd->get_table_schema().get_index_id()),
                                          cfd->ioptions()->env);
  }
}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseColumnFamilyInfo(cfd);
  }
}

void DBImpl::EraseThreadsStatusDbInfo() const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseDatabaseInfo(this);
  }
}

#else
void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* cfd) const {}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const {}

void DBImpl::EraseThreadsStatusDbInfo() const {}
#endif  // ROCKSDB_USING_THREAD_STATUS

//
// A global method that can dump out the build version
void DumpSmartEngineBuildVersion() {
#if !defined(IOS_CROSS_COMPILE)
  __SE_LOG(INFO, "Git sha %s", smartengine_build_git_sha);
  __SE_LOG(INFO, "Compile date %s", smartengine_build_compile_date);
#endif
}

SequenceNumber DBImpl::GetEarliestMemTableSequenceNumber(SuperVersion* sv,
                                                         bool include_history) {
  // Find the earliest sequence number that we know we can rely on reading
  // from the memtable without needing to check sst files.
  SequenceNumber earliest_seq =
      sv->imm->GetEarliestSequenceNumber(include_history);
  if (earliest_seq == kMaxSequenceNumber) {
    earliest_seq = sv->mem->GetEarliestSequenceNumber();
  }
  assert(sv->mem->GetEarliestSequenceNumber() >= earliest_seq);

  return earliest_seq;
}

Status DBImpl::GetLatestSequenceForKey(SuperVersion* sv, const Slice& key,
                                       bool cache_only, SequenceNumber* seq,
                                       bool* found_record_for_key) {
  QUERY_TRACE_SCOPE(TracePoint::GET_LATEST_SEQ);

  Status s;

  ReadOptions read_options;
  SequenceNumber current_seq = versions_->LastSequence();
  LookupKey lkey(key, current_seq);

  *seq = kMaxSequenceNumber;
  *found_record_for_key = false;

  // Check if there is a record for this key in the latest memtable
  sv->mem->Get(lkey, nullptr, &s, seq, read_options);

  if (!(s.ok() || s.IsNotFound())) {
    // unexpected error reading memtable.
    __SE_LOG(ERROR,
                    "Unexpected status returned from MemTable::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check immutable memtables
    *found_record_for_key = true;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->Get(lkey, nullptr, &s, seq, read_options);

  if (!(s.ok() || s.IsNotFound())) {
    // unexpected error reading memtable.
    __SE_LOG(ERROR,
                    "Unexpected status returned from MemTableList::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check memtable history
    *found_record_for_key = true;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->GetFromHistory(lkey, nullptr, &s, seq, read_options);

  if (!(s.ok() || s.IsNotFound())) {
    // unexpected error reading memtable.
    __SE_LOG(ERROR, "Unexpected status returned from MemTableList::GetFromHistory: %s\n", s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check SST files
    *found_record_for_key = true;
    return Status::OK();
  }

  // TODO(agiardullo): possible optimization: consider checking cached
  // SST files if cache_only=true?
  if (!cache_only) {
    PinnableSlice dummy_value;
    bool dummy_bool = false;
    QUERY_COUNT(CountPoint::SEARCH_LATEST_SEQ_IN_STORAGE);
    s = Status(sv->cfd_.load()->get_from_storage_manager(read_options,
                                                  *sv->current_meta_,
                                                  lkey,
                                                  dummy_value,
                                                  dummy_bool,
                                                  seq));

    if (!(s.ok() || s.IsNotFound())) {
      // unexpected error reading SST files
      __SE_LOG(ERROR,
                      "Unexpected status returned from Version::Get: %s\n",
                      s.ToString().c_str());

      return s;
    }

    if (*seq != kMaxSequenceNumber) {
      *found_record_for_key = true;
      return Status::OK();
    }
  }

  return Status::OK();
}

int DBImpl::get_latest_seq_for_uk(ColumnFamilyHandle *column_family,
                                  const ReadOptions *read_opts,
                                  const common::Slice &key,
                                  common::SequenceNumber &seq)
{
  QUERY_TRACE_SCOPE(TracePoint::GET_LATEST_UK_SEQ);
  assert(column_family != nullptr);
  assert(read_opts != nullptr);
  int ret = Status::kOk;

  Iterator *db_iter = NewIterator(*read_opts, column_family);
  db_iter->Seek(key);
  if (db_iter->Valid()) {
    const Slice &scanned_key = db_iter->key();
    if (0 == memcmp(key.data(), scanned_key.data(), std::min(key.size(), scanned_key.size()))) {
      seq = db_iter->key_seq();
      assert(seq < kMaxSequenceNumber);
    } else {
      ret = Status::kNotFound;
    }
  } else {
    ret = Status::kNotFound;
  }
//  delete db_iter;
  MOD_DELETE_OBJECT(Iterator, db_iter);
  return ret;
}


Status DBImpl::InstallSstExternal(ColumnFamilyHandle* column_family,
                                  MiniTables* mtables) {
  ColumnFamilyData* cfd;
  int ret = Status::kOk;
  int64_t dummy_log_seq = 0;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  cfd = cfh->cfd();

  if (FAILED(StorageLogger::get_instance().begin(smartengine::storage::SeEvent::CREATE_INDEX))) {
    SE_LOG(WARN, "fail to begin create index trans", K(ret));
  } else if (FAILED(cfd->apply_change_info(*(mtables->change_info_), true))) {
    SE_LOG(WARN, "fail to apply change info", K(ret));
  } else if (FAILED(StorageLogger::get_instance().commit(dummy_log_seq))) {
    SE_LOG(WARN, "fail to commit cerate index trans", K(ret));
  } else {
    mutex_.Lock();
    SuperVersion *old_sv = InstallSuperVersionAndScheduleWork(cfd, nullptr);
    mutex_.Unlock();
    if (nullptr != old_sv) {
      MOD_DELETE_OBJECT(SuperVersion, old_sv);
    }

    std::vector<FileMetaData>::iterator meta_iter = mtables->metas.begin();
    std::vector<TableProperties>::iterator props_iter = mtables->props.begin();
    assert(mtables->metas.size() == mtables->props.size());

    while (meta_iter != mtables->metas.end()) {
      meta_iter = mtables->metas.erase(meta_iter);
      props_iter = mtables->props.erase(props_iter);
    }

    assert(mtables->metas.size() == 0);
    assert(mtables->props.size() == 0);
  }

  return Status(ret);
}

// retrieve all snapshot numbers. They are sorted in ascending order.
std::vector<SequenceNumber> DBImpl::GetAll(
    SequenceNumber* oldest_write_conflict_snapshot) {
  std::vector<SequenceNumber> ret;
  SnapshotImpl* s[MAX_SNAP] = {nullptr};

  if (oldest_write_conflict_snapshot != nullptr) {
    *oldest_write_conflict_snapshot = kMaxSequenceNumber;
  }

  for (int32_t i = 0; i < MAX_SNAP; i++) {
    snap_mutex[i].Lock();
    s[i] = snap_lists_[i].list();
  }

  uint64_t min = kMaxSequenceNumber;
  int32_t min_idx = -1;

  while (1) {
    min = kMaxSequenceNumber;
    min_idx = -1;
    for (int32_t i = 0; i < MAX_SNAP; i++) {
      if (s[i]->next() != snap_lists_[i].list() &&
          s[i]->next()->number_ < min) {
        min = s[i]->next()->number_;
        min_idx = i;
      }
    }

    if (min_idx == -1) break;

    ret.push_back(min);

    if (oldest_write_conflict_snapshot != nullptr &&
        *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
        s[min_idx]->next()->is_write_conflict_boundary()) {
      // If this is the first write-conflict boundary snapshot in the list,
      // it is the oldest
      *oldest_write_conflict_snapshot = s[min_idx]->next()->number_;
    }

    s[min_idx] = s[min_idx]->next();
  }

  for (int32_t i = 0; i < MAX_SNAP; i++) {
    snap_mutex[i].Unlock();
  }

  return ret;
}

// without snap_mutex for perf in stats
uint64_t DBImpl::snapshots_count() const {
  uint64_t c = 0;
  for (int32_t i = 0; i < MAX_SNAP; i++) {
    c += snap_lists_[i].count();
  }
  return c;
}

int64_t DBImpl::GetOldestSnapshotTime() const {
  int64_t oldest_ut = INT64_MAX;
  for (int32_t i = 0; i < MAX_SNAP; i++) {
    snap_mutex[i].Lock();
    if (!snap_lists_[i].empty() &&
        snap_lists_[i].oldest()->unix_time() < oldest_ut)
      oldest_ut = snap_lists_[i].oldest()->unix_time();
    snap_mutex[i].Unlock();
  }
  return oldest_ut;
}

bool DBImpl::snapshot_empty() {
  bool is_empty = true;
  for (int32_t i = 0; i < MAX_SNAP; i++) {
    if (!snap_lists_[i].empty()) {
      is_empty = false;
      break;
    }
  }
  return is_empty;
}

int DBImpl::do_manual_checkpoint(int64_t &start_manifest_file_num)
{
  int ret = Status::kOk;
  if (FAILED(StorageLogger::get_instance().external_write_checkpoint())) {
    SE_LOG(ERROR, "Do a manual checkpoint failed", K(ret));
  } else {
    // should record the file number of checkpoint
    start_manifest_file_num = StorageLogger::get_instance().current_manifest_file_number();
  }
  return ret;
}

int DBImpl::create_backup_snapshot(BackupSnapshotId backup_id,
                                   MetaSnapshotSet &meta_snapshots,
                                   int64_t &last_manifest_file_num,
                                   uint64_t &last_manifest_file_size,
                                   uint64_t &last_wal_file_num,
                                   BinlogPosition &last_binlog_pos)
{
  int ret = Status::kOk;
  // keep create snapshot and do checkpoint atomic,
  // exclusive from apply_change_info in flush/compaction
  mutex_.Lock();
  // TODO: is it OK to switch default cfd?
  GlobalContext* global_ctx = nullptr;

  if (backup_id == 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "Invalid backup id", K(backup_id), K(ret));
  } else if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else {
    SubTable* sub_table = nullptr;
    global_ctx->all_sub_table_->get_sub_table(0, sub_table);
    WriteContext context;
    context.all_sub_table_ = global_ctx->all_sub_table_;

    db::BackupSnapshotMap &backup_snapshot_map = BackupSnapshotImpl::get_instance()->get_backup_snapshot_map();

    if (backup_snapshot_map.find_backup_snapshot(backup_id)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "backup snapshot id already exists", K(backup_id), K(ret));
    } else if (FAILED(versions_->create_backup_snapshot(meta_snapshots))) {
      SE_LOG(WARN, "Failed to create the backup snapshot", K(backup_id), K(ret));
    } else {
      if (FAILED(switch_memtable(sub_table, &context, true /* force create new wal file*/))) {
        SE_LOG(WARN, "Failed to switch memtable", K(backup_id), K(ret));
      } else {
        last_manifest_file_num = StorageLogger::get_instance().current_manifest_file_number();
        last_manifest_file_size = StorageLogger::get_instance().current_manifest_file_size();
        last_wal_file_num = logfile_number_;
        last_binlog_pos = global_binlog_pos_;

        int64_t commit_seq = 0;
        uint64_t auto_increment_id = backup_snapshot_map.get_max_auto_increment_id() + 1;
        AccquireBackupSnapshotLogEntry log_entry(backup_id, auto_increment_id);
        if (FAILED(StorageLogger::get_instance().begin(storage::SeEvent::ACCQUIRE_BACKUP_SNAPSHOT))) {
          SE_LOG(WARN, "fail to begin create backup snapshot log", K(backup_id), K(ret));
        } else if (FAILED(StorageLogger::get_instance().write_log(storage::REDO_LOG_ACCQUIRE_BACKUP_SNAPSHOT,
                                                                  log_entry))) {
          SE_LOG(WARN, "fail to write create backup snapshot log", K(backup_id), K(ret));
        } else if (FAILED(StorageLogger::get_instance().commit(commit_seq))) {
          SE_LOG(WARN, "fail to commit create backup snapshot log", K(backup_id), K(ret));
        } else if (IS_FALSE(backup_snapshot_map.add_backup_snapshot(backup_id, auto_increment_id, meta_snapshots))) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "backup snapshot id already exists", K(ret), K(backup_id));
        } else {
          SE_LOG(INFO,
                 "Create a backup snapshot",
                 K(backup_id),
                 K(ret),
                 K(last_manifest_file_num),
                 K(last_manifest_file_size),
                 K(last_wal_file_num),
                 K(last_binlog_pos.file_name_),
                 K(last_binlog_pos.offset_));
        }
      }

      if (FAILED(ret)) {
        StorageManager::cleanup_backup_snapshot(meta_snapshots);
      }
    }
  }

  mutex_.Unlock();

  return ret;
}

int DBImpl::record_incremental_extent_ids(const std::string &backup_tmp_dir_path,
                                          const int64_t first_manifest_file_num,
                                          const int64_t last_manifest_file_num,
                                          const uint64_t last_manifest_file_size)
{
  int ret = Status::kOk;
  if (FAILED(StorageLogger::get_instance().record_incremental_extent_ids(backup_tmp_dir_path,
                                                                         first_manifest_file_num,
                                                                         last_manifest_file_num,
                                                                         last_manifest_file_size))) {
    SE_LOG(WARN, "Failed to record incremental extent ids", K(ret),
        K(first_manifest_file_num), K(last_manifest_file_num), K(last_manifest_file_size));
  }
  return ret;
}

int DBImpl::shrink_table_space(int32_t table_space_id) {
  int ret = Status::kOk;
  std::vector<ShrinkInfo> shrink_infos;
  bool actual_shrink = false;
  bool expect_shrink_running = false;
  int64_t shrink_extent_count = 0;
  ShrinkCondition shrink_condition(0 /*max_free_extent_percent*/, 0 /*shrink_allocate_interval*/, INT64_MAX /*max_shrink_extent_count*/);

  SE_LOG(INFO, "begin to do shrink table space", K(table_space_id));
  if (!shrink_running_.compare_exchange_strong(expect_shrink_running, true)) {
    SE_LOG(INFO, "another shrink job is running");
  } else if (FAILED(ExtentSpaceManager::get_instance().get_shrink_infos(table_space_id,
                                                                        shrink_condition,
                                                                        shrink_infos))) {
    SE_LOG(WARN, "fail to get shrink infos", K(ret), K(table_space_id));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < shrink_infos.size(); ++i) {
      const ShrinkInfo &shrink_info = shrink_infos.at(i);
      if (FAILED(shrink_extent_space(shrink_info))) {
        SE_LOG(WARN, "fail to shrink extent space", K(ret), K(shrink_info));
      } else {
        SE_LOG(INFO, "success to shrink extent space", K(i), K(shrink_info));
      }
    }
  }
  shrink_running_.store(false);
  SE_LOG(INFO, "end to do shrink table space", K(table_space_id));

  return ret;
}

bool DBImpl::get_columnfamily_stats(ColumnFamilyHandle* column_family, int64_t &data_size,
                                    int64_t &num_entries, int64_t &num_deletes, int64_t &disk_size) {
  if (LIKELY(column_family != nullptr)) {
    ColumnFamilyData *cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();  
    assert(cfd != nullptr);
    StorageManager *storage_manager = cfd->get_storage_manager();
    assert(storage_manager != nullptr);
    return storage_manager->get_extent_stats(data_size, num_entries, num_deletes, disk_size);
  } else {
    SE_LOG(WARN, "can't get sub tables stats of empty handle\n", K(data_size));
    return false;
  }
}

Top3ModMemInfo DBImpl::pull_top3_mod_mem_info() {
  Top3ModMemInfo ret;
  // return if block cache type
  auto process_single_mod = [this](ModId::ModType mod_type,
                                   int64_t &mod_malloc_size) {
    bool is_block_cache_type = false;
    // default every mod has max quota
    int64_t quota = std::numeric_limits<int64_t>::max();
    switch (mod_type) {
      case memory::ModId::kMemtable: {
        quota = this->immutable_db_options_.db_total_write_buffer_size;
        break;
      }
      case memory::ModId::kIndexBlockCache: {
        [[fallthrough]];
      }
      case memory::ModId::kDataBlockCache: {
        is_block_cache_type = true;
        // we leave block cache size calculation to upper layer
        break;
      }
      case memory::ModId::kRowCache: {
        if (IS_NULL(this->immutable_db_options_.row_cache)) {
          SE_LOG(INFO, "row cache is nullptr, maybe it is turned off.");
        } else {
          quota = this->immutable_db_options_.row_cache->get_capacity();
        }
        break;
      }
      // this mod is related to kRowCache, we set the quoto to 1/10 kRowCache
      case memory::ModId::kCacheHashTable: {
        if (IS_NULL(this->immutable_db_options_.row_cache)) {
          SE_LOG(INFO, "row cache is nullptr, maybe it is turned off.");
        } else {
          quota = this->immutable_db_options_.row_cache->get_capacity() / 10;
        }
        break;
      }
      // these three mods have the same quota, currently we set to 500 MB
      case memory::ModId::kAIOBuffer: {
        [[fallthrough]];
      }
      case memory::ModId::kTransactionLockMgr: {
        [[fallthrough]];
      }
      case memory::ModId::kStorageLogger: {
        // 500 MB
        quota = 500 * (1 << 20);
        break;
      }
      default:
        // the quota of other mods are zero
        quota = 0;
    }
    if (!is_block_cache_type) {
      mod_malloc_size -= quota;
      if (mod_malloc_size < 0) {
        mod_malloc_size = 0;
      }
    }
    return is_block_cache_type;
  };
  int mod_cnt = memory::ModMemSet::kModMaxCnt;
  ArenaAllocator tmp_alloc(8 * 1024);
  // It is better to use unique_ptr here but for reusing exisiting api, we just
  // use raw new/delete here
  auto items = new memory::MemItemDump[mod_cnt];
  auto items_free = util::defer([&items] { delete[] items; });
  auto mod_names = new std::string[mod_cnt];
  auto mode_names_free = util::defer([&mod_names] { delete[] mod_names; });
  int mod_cnt_processed = mod_cnt - 1;
  auto items_processed = new memory::MemItemDump[mod_cnt_processed];
  auto items_processed_free =
      util::defer([&items_processed] { delete[] items_processed; });
  memory::AllocMgr::get_instance()->get_memory_usage(items, mod_names);
  // block_cache_size = size(kDataBlockCache) + size(kIndexBlockCache), and we
  // preprocess first
  for (int i = 0, processed = 0; i < mod_cnt; ++i) {
    auto mod_malloc_size = items[i].malloc_size_;
    if (process_single_mod(static_cast<ModId::ModType>(items[i].id_),
                           mod_malloc_size)) {
      // put block cache into last entry
      items_processed[mod_cnt_processed - 1].malloc_size_ +=
          items[i].malloc_size_;
      items_processed[mod_cnt_processed - 1].alloc_size_ +=
          items[i].alloc_size_;
      // reuse kDataBlockCache to represent kDataBlockCache + kIndexBlockCache
      items_processed[mod_cnt_processed - 1].id_ = ModId::kDataBlockCache;
    } else {
      items_processed[processed] = std::move(items[i]);
      items_processed[processed].malloc_size_ = mod_malloc_size;
      items_processed[processed].alloc_size_ = mod_malloc_size;
      ++processed;
    }
  }
  int64_t block_cache_size =
      (this->block_cache_ == nullptr ? 0 : this->block_cache_->GetCapacity());
  // we minus the quota of block cache
  if (items_processed[mod_cnt_processed - 1].malloc_size_ > block_cache_size) {
    items_processed[mod_cnt_processed - 1].malloc_size_ -= block_cache_size;
    items_processed[mod_cnt_processed - 1].alloc_size_ -= block_cache_size;
  } else {
    items_processed[mod_cnt_processed - 1].malloc_size_ =
        items_processed[mod_cnt_processed - 1].alloc_size_ = 0;
  }
  std::sort(items_processed, items_processed + mod_cnt_processed);
  // we know there are more than three mods
  for (int topk = 1; topk < 4; ++topk) {
    __SE_LOG(SYSTEM, "Top %d module is %s, the memory usage beyond quota is %lld MB.",
             topk, AllocMgr::get_instance()->get_mod_name_by_mod_id(items_processed[topk - 1].id_),
             items_processed[topk - 1].malloc_size_ / (1 << 20));
  }
  ret.top1 = items_processed[0].malloc_size_ / (1 << 20);
  ret.top2 = items_processed[1].malloc_size_ / (1 << 20);
  ret.top3 = items_processed[2].malloc_size_ / (1 << 20);
  return ret;
}

void DBImpl::background_pull_gauge_statistics() {
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  auto defer_release_all_sub_table = util::defer([&global_ctx, &all_sub_table] {
    if (IS_NULL(global_ctx)) {
      return;
    }
    global_ctx->release_thread_local_all_sub_table(all_sub_table);
  });
  int ret = 0;
  // 1. We get all threadlocal sub tables
  if (FAILED(get_all_sub_table(all_sub_table, global_ctx))) {
    SE_LOG(WARN, "fail to get all sub tables");
    return;
  }
  SubTableMap &all_sub_tables = all_sub_table->sub_table_map_;
  SubTable *sub_table = nullptr;
  uint64_t max_level0_layers = 0;
  uint64_t max_imm_numbers = 0;
  uint64_t max_level0_fragmentation_rate = 0;
  uint64_t max_level1_fragmentation_rate = 0;
  uint64_t max_level2_fragmentation_rate = 0;
  uint64_t max_level0_delete_percent = 0;
  uint64_t max_level1_delete_percent = 0;
  uint64_t max_level2_delete_percent = 0;
  uint64_t global_external_fragmentation_rate = 0;
  std::vector<uint64_t> subtable_size_vec;
  subtable_size_vec.reserve(all_sub_tables.size());
  // all_flush/compaction_megabytes maintainted by statistics
  // 2. Traverse all sub tables, update the gauge value we need
  for (auto iter = all_sub_tables.begin(); iter != all_sub_tables.end();
       ++iter) {
    if (IS_NULL(sub_table = iter->second) || sub_table->IsDropped()) {
      SE_LOG(WARN, "sub table is nullptr or dropped", K(iter->first));
      continue;
    }
    // we get all data we need in thread local sv to avoid mutex_ hold
    SuperVersion *sv = GetAndRefSuperVersion(sub_table);
    auto defer_return_sv = util::defer([this, &sub_table, &sv] {
      if (IS_NULL(sub_table)) {
        return;
      }
      this->ReturnAndCleanupSuperVersion(sub_table, sv);
    });
    if (IS_NULL(sv->current_meta_)) {
      SE_LOG(WARN, "unexpected error, current meta snapshot must not nullptr",
             KP(sv->current_meta_));
      continue;
    }
    // 2.1 max level0 layer size
    uint64_t level0_layer_size =
        sv->current_meta_->get_extent_layer_version(0)->get_extent_layer_size();
    max_level0_layers = std::max(max_level0_layers, level0_layer_size);
    // 2.2 max imm number
    uint64_t imm_numbers = sv->imm->get_imm_number();
    max_imm_numbers = std::max(max_imm_numbers, imm_numbers);
    // 2.3 && 2.4 max fragmentation rate and delete percent of level 0/1/2
    auto get_level_fragmentation_rate_and_delete_percent_func =
        [](const Snapshot *meta_snapshot, int32_t level) {
          uint64_t fragmentation_rate = 0;
          uint64_t delete_percent = 0;
          if (level < 0 || level >= storage::MAX_TIER_COUNT) {
            SE_LOG(WARN, "level is invalid", K(level));
            return std::make_pair(fragmentation_rate, delete_percent);
          }
          auto extent_stat = meta_snapshot->get_extent_layer_version(level)
                                 ->get_extent_stats();
          fragmentation_rate = ((extent_stat.disk_size_ == 0)
                                    ? 0
                                    : 100 - extent_stat.data_size_ * 100.0 /
                                                extent_stat.disk_size_);
          delete_percent = ((extent_stat.num_entries_ == 0)
                                ? 0
                                : extent_stat.num_deletes_ * 100.0 /
                                      extent_stat.num_entries_);
          return std::make_pair(fragmentation_rate, delete_percent);
        };
    // level 0
    auto [level0_fragmentation_rate, level0_delete_percent] =
        get_level_fragmentation_rate_and_delete_percent_func(sv->current_meta_,
                                                             0);
    max_level0_fragmentation_rate =
        std::max(max_level0_fragmentation_rate, level0_fragmentation_rate);
    max_level0_delete_percent =
        std::max(max_level0_delete_percent, level0_delete_percent);
    // level 1
    auto [level1_fragmentation_rate, level1_delete_percent] =
        get_level_fragmentation_rate_and_delete_percent_func(sv->current_meta_,
                                                             1);
    max_level1_fragmentation_rate =
        std::max(max_level1_fragmentation_rate, level1_fragmentation_rate);
    max_level1_delete_percent =
        std::max(max_level1_delete_percent, level1_delete_percent);
    // level 2
    auto [level2_fragmentation_rate, level2_delete_percent] =
        get_level_fragmentation_rate_and_delete_percent_func(sv->current_meta_,
                                                             2);
    max_level2_fragmentation_rate =
        std::max(max_level2_fragmentation_rate, level2_fragmentation_rate);
    max_level2_delete_percent =
        std::max(max_level2_delete_percent, level2_delete_percent);
    // 2.5 global flush/compaction stat already update in flush/compaction job
    // 2.6 subtable size
    auto get_subtable_size_func = [&sv] {
      uint64_t mt_size = sv->mem->data_size();
      uint64_t immt_size = sv->imm->get_data_size();
      uint64_t extent_size =
          sv->current_meta_->get_total_extent_count() * MAX_EXTENT_SIZE;
      return (mt_size + immt_size + extent_size) / (1 << 20);
    };
    subtable_size_vec.push_back(get_subtable_size_func());
  }
  // calculate subtable size top3
  int subtable_size = subtable_size_vec.size();
  std::partial_sort(subtable_size_vec.begin(),
                    subtable_size_vec.begin() + std::min(3, subtable_size),
                    subtable_size_vec.end(), std::greater<>());
  // 2.7 global external fragmentation rate
  std::vector<storage::DataFileStatistics> data_file_stats;
  get_data_file_stats(data_file_stats);
  int64_t free_extents = 0;
  int64_t total_extents = 0;
  for (const auto &stat : data_file_stats) {
    free_extents += stat.free_extent_count_;
    total_extents += stat.total_extent_count_;
  }
  global_external_fragmentation_rate =
      ((total_extents == 0) ? 0 : (1.0 * free_extents * 100) / total_extents);
  // 2.8 top3 module memory usage
  auto top3_mod_mem_info = pull_top3_mod_mem_info();
  // 3. Set value to statistics
  stats_->set_gauge_value(Gauge::MAX_LEVEL0_LAYERS, max_level0_layers);
  stats_->set_gauge_value(Gauge::MAX_IMM_NUMBERS, max_imm_numbers);
  stats_->set_gauge_value(Gauge::MAX_LEVEL0_FRAGMENTATION_RATE,
                          max_level0_fragmentation_rate);
  stats_->set_gauge_value(Gauge::MAX_LEVEL1_FRAGMENTATION_RATE,
                          max_level1_fragmentation_rate);
  stats_->set_gauge_value(monitor::Gauge::MAX_LEVEL2_FRAGMENTATION_RATE,
                          max_level2_fragmentation_rate);
  stats_->set_gauge_value(Gauge::MAX_LEVEL0_DELETE_PERCENT,
                          max_level0_delete_percent);
  stats_->set_gauge_value(Gauge::MAX_LEVEL1_DELETE_PERCENT,
                          max_level1_delete_percent);
  stats_->set_gauge_value(Gauge::MAX_LEVEL2_DELETE_PERCENT,
                          max_level2_delete_percent);
  stats_->set_gauge_value(Gauge::ALL_FLUSH_MEGABYTES,
                          stats_->get_global_flush_megabytes_written());
  stats_->set_gauge_value(Gauge::ALL_COMPACTION_MEGABYTES,
                          stats_->get_global_compaction_megabytes_written());
#define SET_TOPK_SUBTABLE_SIZE(k)                          \
  if (k <= subtable_size) {                                \
    stats_->set_gauge_value(Gauge::TOP##k##_SUBTABLE_SIZE, \
                            subtable_size_vec[k - 1]);     \
  }
  SET_TOPK_SUBTABLE_SIZE(1);
  SET_TOPK_SUBTABLE_SIZE(2);
  SET_TOPK_SUBTABLE_SIZE(3);
#undef SET_TOPK_SUBTABLE_SIZE
  stats_->set_gauge_value(Gauge::GLOBAL_EXTERNAL_FRAGMENTATION_RATE,
                          global_external_fragmentation_rate);
  stats_->set_gauge_value(Gauge::TOP1_MOD_MEM_INFO, top3_mod_mem_info.top1);
  stats_->set_gauge_value(Gauge::TOP2_MOD_MEM_INFO, top3_mod_mem_info.top2);
  stats_->set_gauge_value(Gauge::TOP3_MOD_MEM_INFO, top3_mod_mem_info.top3);
  // 4. Reset global flush/compaction stats
  stats_->reset_global_flush_stat();
  stats_->reset_global_compaction_stat();
#ifdef WITH_SMARTENGINE
  // 5. Set global_stats in handler layer
  global_stats.max_level0_layers_ =
      stats_->get_gauge_value(Gauge::MAX_LEVEL0_LAYERS);
  global_stats.max_imm_numbers_ =
      stats_->get_gauge_value(Gauge::MAX_IMM_NUMBERS);
  global_stats.max_level0_fragmentation_rate_ =
      stats_->get_gauge_value(Gauge::MAX_LEVEL0_FRAGMENTATION_RATE);
  global_stats.max_level1_fragmentation_rate_ =
      stats_->get_gauge_value(Gauge::MAX_LEVEL1_FRAGMENTATION_RATE);
  global_stats.max_level2_fragmentation_rate_ =
      stats_->get_gauge_value(Gauge::MAX_LEVEL2_FRAGMENTATION_RATE);
  global_stats.max_level0_delete_percent_ =
      stats_->get_gauge_value(Gauge::MAX_LEVEL0_DELETE_PERCENT);
  global_stats.max_level1_delete_percent_ =
      stats_->get_gauge_value(Gauge::MAX_LEVEL1_DELETE_PERCENT);
  global_stats.max_level2_delete_percent_ =
      stats_->get_gauge_value(Gauge::MAX_LEVEL2_DELETE_PERCENT);
  global_stats.all_flush_megabytes_ =
      stats_->get_gauge_value(Gauge::ALL_FLUSH_MEGABYTES);
  global_stats.all_compaction_megabytes_ =
      stats_->get_gauge_value(Gauge::ALL_COMPACTION_MEGABYTES);
  global_stats.top1_subtable_size_ =
      stats_->get_gauge_value(Gauge::TOP1_SUBTABLE_SIZE);
  global_stats.top2_subtable_size_ =
      stats_->get_gauge_value(Gauge::TOP2_SUBTABLE_SIZE);
  global_stats.top3_subtable_size_ =
      stats_->get_gauge_value(Gauge::TOP3_SUBTABLE_SIZE);
  global_stats.top1_mod_mem_info_ =
      stats_->get_gauge_value(Gauge::TOP1_MOD_MEM_INFO);
  global_stats.top2_mod_mem_info_ =
      stats_->get_gauge_value(Gauge::TOP2_MOD_MEM_INFO);
  global_stats.top3_mod_mem_info_ =
      stats_->get_gauge_value(Gauge::TOP3_MOD_MEM_INFO);
  global_stats.global_external_fragmentation_rate_ =
      stats_->get_gauge_value(Gauge::GLOBAL_EXTERNAL_FRAGMENTATION_RATE);
#endif
}

} //namespace db
} //namespace smartengine
