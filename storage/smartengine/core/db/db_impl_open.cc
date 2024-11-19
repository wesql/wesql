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

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <thread>

#include "cache/persistent_cache.h"
#include "db/debug_info.h"
#include "db/replay_task.h"
#include "db/replay_thread_pool.h"
#include "options/options_helper.h"
#include "table/extent_table_factory.h"
#include "memory/mod_info.h"
#include "util/file_reader_writer.h"
#include "util/sync_point.h"
#include "storage/extent_meta_manager.h"
#include "storage/storage_logger.h"

namespace smartengine {
using namespace common;
using namespace memory;
using namespace monitor;
using namespace storage;
using namespace table;
using namespace util;

namespace db {
Options SanitizeOptions(const std::string& dbname, const Options& src) {
  auto db_options = SanitizeOptions(dbname, DBOptions(src));
  ImmutableDBOptions immutable_db_options(db_options);
  auto cf_options =
      SanitizeOptions(immutable_db_options, ColumnFamilyOptions(src));
  return Options(db_options, cf_options);
}

DBOptions SanitizeOptions(const std::string& dbname, const DBOptions& src) {
  DBOptions result(src);
  // TODO (Zhao Dongsheng) : The 'db_write_buffer_size' has been deprecated, and only
  // 'db_total_write_buffer_size' is retained. 
  if (!result.write_buffer_manager) {
    result.write_buffer_manager.reset(new WriteBufferManager(
        result.db_total_write_buffer_size, result.db_total_write_buffer_size));
  }
  if (result.base_background_compactions == -1) {
    result.base_background_compactions = result.max_background_compactions;
  }
  if (result.base_background_compactions > result.max_background_compactions) {
    result.base_background_compactions = result.max_background_compactions;
  }

  result.env->IncBackgroundThreadsIfNeeded(src.max_background_compactions,
                                           Env::Priority::LOW);
  result.env->IncBackgroundThreadsIfNeeded(src.max_background_flushes,
                                           Env::Priority::HIGH);

  if (result.rate_limiter.get() != nullptr) {
    if (result.bytes_per_sync == 0) {
      result.bytes_per_sync = 1024 * 1024;
    }
  }

  if (result.parallel_wal_recovery &&
      result.wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
    result.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  }

  if (result.wal_dir.empty()) {
    // Use dbname as default
    result.wal_dir = dbname;
  }
  if (result.wal_dir.back() == '/') {
    result.wal_dir = result.wal_dir.substr(0, result.wal_dir.size() - 1);
  }

  if (result.db_paths.size() == 0) {
    result.db_paths.emplace_back(dbname, std::numeric_limits<uint64_t>::max());
  }

  if (result.persistent_cache_dir.empty()) {
    // Use dbname(data dir) as default persistent cache dir
    result.persistent_cache_dir = dbname;
  }
  if ('/' == result.persistent_cache_dir.back()) {
    result.persistent_cache_dir = result.persistent_cache_dir.substr(0, result.persistent_cache_dir.size() - 1);
  }

  return result;
}

namespace {

Status SanitizeOptionsByTable(const DBOptions &db_options,
                              const ColumnFamilyOptions &cf_options)
{
  return cf_options.table_factory->SanitizeOptions(db_options, cf_options);
}

static Status ValidateOptions(const DBOptions& db_options,
                              const ColumnFamilyOptions& cf_options)
{
  Status s;

  s = CheckCompressionSupported(cf_options);
  if (s.ok() && db_options.allow_concurrent_memtable_write) {
    s = CheckConcurrentWritesSupported(cf_options);
  }

  if (s.ok() && db_options.db_paths.size() > 4) {
    s = Status::NotSupported("More than four DB paths are not supported yet.");
  }

  return s;
}
}  // namespace

Status DBImpl::Directories::CreateAndNewDirectory(
    Env* env, const std::string& dirname,
    Directory *&directory) const {
  // We call CreateDirIfMissing() as the directory may already exist (if we
  // are reopening a DB), when this happens we don't want creating the
  // directory to cause an error. However, we need to check if creating the
  // directory fails or else we may get an obscure message about the lock
  // file not existing. One real-world example of this occurring is if
  // env->CreateDirIfMissing() doesn't create intermediate directories, e.g.
  // when dbname_ is "dir/db" but when "dir" doesn't exist.
  Status s = env->CreateDirIfMissing(dirname);
  if (!s.ok()) {
    return s;
  }
  return env->NewDirectory(dirname, directory);
}

Status DBImpl::Directories::SetDirectories(Env *env,
                                           const std::string &dbname,
                                           const std::string &wal_dir,
                                           const std::vector<DbPath> &data_paths,
                                           const std::string &persistent_cache_dir)
{
  util::Directory *ptr = nullptr;
  Status s = CreateAndNewDirectory(env, dbname, ptr);
  db_dir_.reset(ptr);
  if (!s.ok()) {
    return s;
  }
  if (!wal_dir.empty() && dbname != wal_dir) {
    util::Directory *wal_ptr = nullptr;
    s = CreateAndNewDirectory(env, wal_dir, wal_ptr);
    wal_dir_.reset(wal_ptr);
    if (!s.ok()) {
      return s;
    }
  }

  data_dirs_.clear();
  for (auto& p : data_paths) {
    const std::string db_path = p.path;
    if (db_path == dbname) {
      data_dirs_.emplace_back(nullptr);
    } else {
      Directory *path_directory = nullptr;
      s = CreateAndNewDirectory(env, db_path, path_directory);
      if (!s.ok()) {
        return s;
      }
      data_dirs_.emplace_back(path_directory);
    }
  }
  assert(data_dirs_.size() == data_paths.size());

  if (!persistent_cache_dir.empty() && dbname != persistent_cache_dir) {
    util::Directory *persist_cache_dir_ptr = nullptr;
    s = CreateAndNewDirectory(env, persistent_cache_dir, persist_cache_dir_ptr);
    if (!s.ok()) {
      return s;
    }
    persistent_cache_dir_.reset(persist_cache_dir_ptr);
  }

  return Status::OK();
}

Status DBImpl::Recover(const ColumnFamilyOptions &cf_options)
{
  mutex_.AssertHeld();

  int ret = Status::kOk;
  assert(db_lock_ == nullptr);

  if (FAILED(prepare_recovery(cf_options))) {
    SE_LOG(ERROR, "fail to prepae recovery", K(ret));
  } else if (FAILED(recovery())) {
    SE_LOG(ERROR, "fail to replay manifest log or wal", K(ret));
  } else {
    SE_LOG(INFO, "success to recovery smartengine log");
  }

  return ret;
}

int DBImpl::prepare_recovery(const ColumnFamilyOptions &cf_options)
{
  int ret = Status::kOk;
  const uint64_t t1 = env_->NowMicros();

  if (FAILED(directories_.SetDirectories(env_,
                                         dbname_,
                                         immutable_db_options_.wal_dir,
                                         immutable_db_options_.db_paths,
                                         immutable_db_options_.persistent_cache_dir).code())) {
    SE_LOG(INFO, "fail to set directories", K(ret), K_(dbname),
        "wal_dir", immutable_db_options_.wal_dir, "persist_cache_dir", immutable_db_options_.persistent_cache_dir);
  } else if (FAILED(env_->LockFile(FileNameUtil::lock_file_path(dbname_), &db_lock_).code())) {
    SE_LOG(INFO, "fail to lock file", K(ret), K_(dbname));
  } else {
    if (FAILED(env_->FileExists(FileNameUtil::current_file_path(dbname_)).code())) {
      if (Status::kNotFound != ret) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, when check current file exist or not", K(ret), K_(dbname));
      } else {
        //overwrite ret as design
        const uint64_t t2 = env_->NowMicros();
        if (FAILED(StorageLogger::get_instance().external_write_checkpoint())) {
          SE_LOG(WARN, "fail to external write checkpoint", K(ret), K_(dbname));
        }
        const uint64_t t3 = env_->NowMicros();
        recovery_debug_info_.prepare_external_write_ckpt = t3 - t2;
      }
    }

    // TODO(Zhao Dongsheng): Temporarily place the initialization of the persistent 
    // cache here, because it depends on the creation of persistent_cache_dir.
    // In the future, during the refactoring of the start process, move it to an
    // appropriate position.
    if (SUCCED(ret)) {
      if (FAILED(cache::PersistentCache::get_instance().init(env_,
                                                             immutable_db_options_.persistent_cache_dir,
                                                             immutable_db_options_.persistent_cache_size,
                                                             std::thread::hardware_concurrency(),
                                                             static_cast<PersistentCacheMode>(immutable_db_options_.persistent_cache_mode)))) {
        SE_LOG(WARN, "fail to init PersistentCache", K(ret), "persist_cache_dir", immutable_db_options_.persistent_cache_dir,
            "persistent_cache_size", immutable_db_options_.persistent_cache_size);
      }
    }  
  }

  if (SUCCED(ret)) {
    const uint64_t t4 = env_->NowMicros();
    if (FAILED(create_default_subtbale(cf_options))) {
      SE_LOG(WARN, "fail to create default subtable", K(ret));
    }
    const uint64_t t5 = env_->NowMicros();
    recovery_debug_info_.prepare_create_default_subtable = t5 - t4;
  }

  const uint64_t t6 = env_->NowMicros();
  recovery_debug_info_.prepare = t6 - t1;
  return ret;
}

int DBImpl::set_compaction_need_info() {
  GlobalContext* global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  int ret = 0;
  if (FAILED(get_all_sub_table(all_sub_table, global_ctx))) {
    SE_LOG(WARN, "get all subtable failed", K(ret));
  } else {
    SubTableMap& all_subtables = all_sub_table->sub_table_map_;
    SubTable *sub_table = nullptr;
    for (auto iter = all_subtables.begin();
         Status::kOk == ret && iter != all_subtables.end(); ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (sub_table->IsDropped()) {
        SE_LOG(INFO, "subtable has been dropped", K(iter->first));
      } else {
        if(FAILED(sub_table->set_compaction_check_info(&mutex_))) {
          SE_LOG(WARN, "failed to set compaction check info", K(ret));
        }
      }
    }
  }
  int tmp_ret = ret;
  if (nullptr != global_ctx &&
      FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }
  return ret;
}

int DBImpl::recovery()
{
  int ret = Status::kOk;
  ArenaAllocator recovery_arena(CharArena::DEFAULT_PAGE_SIZE, ModId::kRecovery);

  const uint64_t t1 = env_->NowMicros();
  if (FAILED(StorageLogger::get_instance().replay(recovery_arena))) {
    SE_LOG(ERROR, "fail to replay manifest log", K(ret));
  } else if (FAILED(set_compaction_need_info())){ // set compaction info
    SE_LOG(WARN, "failed to set compaction need info", K(ret));
  }
  const uint64_t t2 = env_->NowMicros();
  if (SUCCED(ret)) {
    if (FAILED(recovery_wal(recovery_arena))) {
      SE_LOG(WARN, "fail to replay wal", K(ret));
    }
  }
  const uint64_t t3 = env_->NowMicros();
  recovery_debug_info_.recovery = t3 - t1;
  recovery_debug_info_.recovery_slog_replay = t2 - t1;
  recovery_debug_info_.recovery_wal = t3 - t2;

  DebugInfoEntry entry;
  entry.key_desc_ = "breakdown recovery time consumed";
  entry.item_id_ = 0;
  entry.timestamp_ = env_->NowMicros();
  entry.debug_info_1_ = recovery_debug_info_.show();
  const std::string recovery_debug_key = "RECOVERY_PERF";
  DebugInfoStation::get_instance()->replace_entry(recovery_debug_key, entry);
  return ret;
}

int DBImpl::create_default_subtbale(const ColumnFamilyOptions &cf_options)
{
  int ret = Status::kOk;
  schema::TableSchema table_schema;
  table_schema.set_index_id(0);
  CreateSubTableArgs args(table_schema, cf_options, true, 0);
  ColumnFamilyData *sub_table = nullptr;

  if (FAILED(versions_->add_sub_table(args, false, true /*is replay*/, sub_table))) {
    SE_LOG(WARN, "fail to create default subtable", K(ret));
  } else if IS_NULL(default_cf_handle_ = MOD_NEW_OBJECT(
      ModId::kDBImpl, ColumnFamilyHandleImpl,sub_table, this, &mutex_)) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for default_cf_handle", K(ret));
  } else if (FAILED(ExtentSpaceManager::get_instance().create_table_space(args.table_space_id_))) { 
    SE_LOG(WARN, "fail to create table space", K(ret), K(args));
  } else {
    default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();
    single_column_family_mode_ = false;
  }

  return ret;
}

int DBImpl::recovery_wal(ArenaAllocator &arena)
{
  int ret = Status::kOk;

  const uint64_t t1 = env_->NowMicros();
  if (FAILED(before_replay_wal_files())) {
    SE_LOG(WARN, "fail to do something before replay wal", K(ret));
  }
  const uint64_t t2 = env_->NowMicros();
  if (SUCCED(ret)) {
    if (immutable_db_options_.parallel_wal_recovery) {
      SE_LOG(INFO, "start replaying wal files in parallel");
      ret = parallel_replay_wal_files(arena);
    } else {
      SE_LOG(INFO, "start replaying wal files");
      ret = replay_wal_files(arena);
    }
    if (FAILED(ret)) {
      SE_LOG(WARN, "fail to replay wal", K(ret));
    }
  }
  const uint64_t t3 = env_->NowMicros();
  if (SUCCED(ret)) {
    if (FAILED(after_replay_wal_files(arena))) {
      SE_LOG(WARN, "fail to do something after replay wal files", K(ret));
    }
  }
  const uint64_t t4 = env_->NowMicros();
  recovery_debug_info_.recoverywal = t4 - t1;
  recovery_debug_info_.recoverywal_before = t2 - t1;
  recovery_debug_info_.recoverywal_wal_files = t3 - t2;
  recovery_debug_info_.recoverywal_after = t4 - t3;
  return ret;
}

int DBImpl::before_replay_wal_files()
{
  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  SubTable *sub_table = nullptr;

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
        if (sub_table->get_recovery_point().seq_ > max_seq_in_rp_) {
          max_seq_in_rp_ = sub_table->get_recovery_point().seq_;
        }
      }
    }
  }

  SE_LOG(SYSTEM, "max sequence number in recovery point", K_(max_seq_in_rp));

  if (immutable_db_options_.avoid_flush_during_recovery) {
    if (nullptr == (global_ctx = versions_->get_global_ctx())) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "global ctx must not nullptr", K(ret));
    } else {
      SubTableMap& all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
      for (auto iter = all_sub_tables.begin();
           Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
        if (nullptr == (sub_table = iter->second)) {
          ret = Status::kCorruption;
          SE_LOG(WARN, "subtable must not nullptr", K(ret),
                      K(iter->first));
        } else {
          sub_table->mem()->set_no_flush(true);
        }
      }
    }
  }

  versions_->GetColumnFamilySet()->set_during_repaly_wal(true);

  return ret;
}

// this method can only be called when all replay threads are suspend or stopped
void DBImpl::update_last_sequence_during_recovery() {
  mutex_.AssertHeld();
  if (max_sequence_during_recovery_.load() > versions_->LastSequence()) {
    versions_->SetLastSequence(max_sequence_during_recovery_.load());
    versions_->SetLastAllocatedSequence(max_sequence_during_recovery_.load());
    SE_LOG(INFO, "set last sequence", K(max_sequence_during_recovery_.load()),
        "last_sequence", versions_->LastSequence());
  }
  if (max_log_file_number_during_recovery_.load() > logfile_number_) {
    logfile_number_ = max_log_file_number_during_recovery_.load();
    SE_LOG(INFO, "set current replayed logfile_number", K(logfile_number_));
  }
}

int DBImpl::parallel_replay_wal_files(ArenaAllocator &arena) {
  assert(WALRecoveryMode::kPointInTimeRecovery != immutable_db_options_.wal_recovery_mode);
  int ret = Status::kOk;
  uint64_t thread_num = 0;
  if (immutable_db_options_.parallel_recovery_thread_num == 0) {
    auto num_cpus = std::thread::hardware_concurrency();
    thread_num = num_cpus < 4 ? 2 : (num_cpus >> 1);
    thread_num = thread_num > 1024 ? 1024 : thread_num;
  } else {
    thread_num = immutable_db_options_.parallel_recovery_thread_num;
  }
  auto start_t = env_->NowMicros();
  ReplayThreadPool replay_thread_pool(thread_num, this);
  uint64_t read_time = 0;
  uint64_t parse_time = 0;
  uint64_t submit_time = 0;
  std::vector<uint64_t> log_file_numbers;
  if (FAILED(replay_thread_pool.init())) {
    SE_LOG(WARN, "fail to init replay thread pool", K(ret), K(thread_num));
  } else if (FAILED(collect_sorted_wal_file_number(log_file_numbers))) {
      SE_LOG(WARN, "fail to collect sorted wal file numer", K(ret));
  } else {
    uint64_t log_file_number = 0;
    uint64_t next_log_file_number = 0;
    bool last_file = false;
    uint64_t slowest = 0;
    uint64_t logfile_count = log_file_numbers.size();
    for (uint32_t i = 0; SUCCED(ret) && i < logfile_count; ++i) {
      const uint64_t t1 = env_->NowMicros();
      log_file_number = log_file_numbers.at(i);
      next_log_file_number = (i < (logfile_count - 1)) ?
          log_file_numbers.at(i + 1) : (log_file_number + 1);
      last_file = ((logfile_count -1) == i);
      if (FAILED(before_replay_one_wal_file(log_file_number))) {
        SE_LOG(WARN, "fail to do something before replay one wal file",
            K(ret), K(i), K(logfile_count), K(log_file_number), K(next_log_file_number));
      } else if (FAILED(parallel_replay_one_wal_file(log_file_number,
              next_log_file_number, last_file, replay_thread_pool, arena,
              &read_time, &parse_time, &submit_time))) {
        SE_LOG(WARN, "fail to replay wal file", K(ret),
            K(logfile_count), K(log_file_number), K(next_log_file_number));
        replay_thread_pool.set_error(); // replay threads will stop if error
      } else {
        SE_LOG(INFO, "success to read one wal file", K(log_file_number), K(next_log_file_number));
      }
      const uint64_t t2 = env_->NowMicros();
      if (t2 - t1 > slowest) {
        slowest = t2 - t1;
      }
    }
    auto replay_last_time = env_->NowMicros() - start_t;
    recovery_debug_info_.recoverywal_file_count = logfile_count;
    recovery_debug_info_.recoverywal_slowest = slowest;
    if (SUCCED(ret)) {
      if (FAILED(finish_parallel_replay_wal_files(replay_thread_pool))) {
        SE_LOG(ERROR, "finish_parallel_replay_wal_files failed", K(ret),
            K(max_sequence_during_recovery_), K(max_log_file_number_during_recovery_));
      }
    } else {
       SE_LOG(ERROR, "parallel_replay_wal_files failed", K(ret),
            K(max_sequence_during_recovery_), K(max_log_file_number_during_recovery_));
    }
    auto finish_parallel_replay_wal_files_t = env_->NowMicros() - start_t;
    SE_LOG(INFO, "finish parallel_replay_wal_files", K(replay_last_time), K(read_time),
              K(parse_time), K(submit_time), K(finish_parallel_replay_wal_files_t));
  }
  return ret;
}

int DBImpl::finish_parallel_replay_wal_files(ReplayThreadPool &replay_thread_pool) {
  int ret = Status::kOk;
  if (FAILED(replay_thread_pool.stop())) {
    SE_LOG(WARN, "fail to stop thread pool", K(ret));
  } else if (FAILED(replay_thread_pool.wait_for_all_threads_stopped())) {
    SE_LOG(WARN, "fail to waiting for all replay tasks done", K(ret));
    replay_thread_pool.set_error();
  } else if (FAILED(replay_thread_pool.destroy())) {
    SE_LOG(WARN, "failed to destroy replay_thread_pool", K(ret));
  } else {
    update_last_sequence_during_recovery();
    if (check_if_need_switch_memtable()) { // consume tasks in flush_scheduler_
        std::list<SubTable*> switched_sub_tables;
        if (FAILED(switch_memtable_during_parallel_recovery(switched_sub_tables))) {
          SE_LOG(WARN, "switch memtables failed", K(ret));
        } else if (FAILED(flush_memtable_during_parallel_recovery(switched_sub_tables))) {
          SE_LOG(WARN, "flush memtables failed", K(ret));
        }
    }
  }
  return ret;
}

int DBImpl::replay_wal_files(ArenaAllocator &arena)
{
  int ret = Status::kOk;
  std::vector<uint64_t> log_file_numbers;
  uint64_t log_file_number = 0;
  SequenceNumber next_allocate_sequence = kMaxSequenceNumber;
  bool stop_replay = false;
  uint64_t next_log_file_number = 0;
  uint32_t logfile_count = 0;
  bool last_file = false;
  if (FAILED(collect_sorted_wal_file_number(log_file_numbers))) {
    SE_LOG(WARN, "fail to collect sorted wal file numer", K(ret));
  } else {
    logfile_count = log_file_numbers.size();
    uint64_t slowest = 0;
    for (uint32_t i = 0; SUCCED(ret) && i < logfile_count; ++i) {
      const uint64_t t1 = env_->NowMicros();
      log_file_number = log_file_numbers.at(i);
      next_log_file_number = (i < (logfile_count - 1)) ? log_file_numbers.at(i + 1) : (log_file_number + 1);
      last_file = ((logfile_count -1) == i);
      if (FAILED(before_replay_one_wal_file(log_file_number))) {
        SE_LOG(WARN, "fail to do something before replay one wal file", K(ret), K(i), K(logfile_count), K(log_file_number),
            K(next_log_file_number), K(next_allocate_sequence));
      } else if (FAILED(replay_one_wal_file(log_file_number, last_file, next_allocate_sequence, stop_replay, arena))) {
        SE_LOG(WARN, "fail to replay wal file", K(ret), K(i), K(logfile_count), K(log_file_number), K(next_log_file_number),
            K(next_allocate_sequence));
      } else if (FAILED(after_replay_one_wal_file(next_log_file_number, next_allocate_sequence))) {
        SE_LOG(WARN, "fail to do something after replay one wal file", K(ret), K(i), K(logfile_count), K(log_file_number),
            K(next_log_file_number), K(next_allocate_sequence));
      } else {
        SE_LOG(INFO, "success to replay one wal file", K(log_file_number), K(next_log_file_number));
      }
      SE_LOG(INFO, "replay one log file", K(ret), K(log_file_number), K(next_log_file_number));
      const uint64_t t2 = env_->NowMicros();
      if (t2 - t1 > slowest) {
        slowest = t2 - t1;
      }
    }
    recovery_debug_info_.recoverywal_file_count = logfile_count;
    recovery_debug_info_.recoverywal_slowest = slowest;
  }

  return ret;
}

int DBImpl::after_replay_wal_files(ArenaAllocator &arena)
{
  int ret = Status::kOk;
  GlobalContext* global_ctx = nullptr;
  SubTable* sub_table = nullptr;

  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else {
    SubTableMap& all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
    for (auto iter = all_sub_tables.begin();
         Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else {
        sub_table->mem()->set_no_flush(false);
      }
    }
  }

  //update the sequence, if the data with max sequence has flush to sst
  if (max_seq_in_rp_ > versions_->LastSequence()) {
    versions_->SetLastSequence(max_seq_in_rp_);
    versions_->SetLastAllocatedSequence(max_seq_in_rp_);
  }

  if (FAILED(update_wal_writer(true /*force*/))) {
    SE_LOG(WARN, "fail to create new log writer", K(ret));
  } else {
    versions_->GetColumnFamilySet()->set_during_repaly_wal(false);
  }
  return ret;
}

int DBImpl::collect_sorted_wal_file_number(std::vector<uint64_t> &log_file_numbers)
{
  int ret = Status::kOk;
  std::vector<std::string> file_names;
  int64_t file_number = 0;
  FileType file_type = util::kInvalidFileType;

  if (FAILED(env_->GetChildren(immutable_db_options_.wal_dir, &file_names).code())) {
    SE_LOG(WARN, "fail to get files in wal dir", K(ret), "wal_dir", immutable_db_options_.wal_dir);
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < file_names.size(); ++i) {
      // Ignore the file that we can't recognize it.
      if ((Status::kOk == FileNameUtil::parse_file_name(file_names[i], file_number, file_type)) && kWalFile == file_type) {
        log_file_numbers.push_back(file_number);
      }
    }

    if (SUCCED(ret)) {
      std::sort(log_file_numbers.begin(), log_file_numbers.end());
    }
  }

  return ret;
}

int DBImpl::before_replay_one_wal_file(uint64_t current_log_file_number)
{
  int ret = Status::kOk;
  versions_->MarkFileNumberUsedDuringRecovery(current_log_file_number);
  alive_log_files_.push_back(LogFileNumberSize(current_log_file_number));
  return ret;
}

int DBImpl::after_replay_one_wal_file(uint64_t next_log_file_number, SequenceNumber next_allocate_sequence)
{
  int ret = Status::kOk;

  if (next_log_file_number > logfile_number_) {
    logfile_number_ = next_log_file_number;
  }
  if (kMaxSequenceNumber != next_allocate_sequence && next_allocate_sequence > versions_->LastSequence()) {
    versions_->SetLastSequence(next_allocate_sequence - 1);
    versions_->SetLastAllocatedSequence(next_allocate_sequence - 1);
    SE_LOG(INFO, "set last sequence", K(next_allocate_sequence), "last_sequence", versions_->LastSequence());
  }

  if (FAILED(consume_flush_scheduler_task())) {
    SE_LOG(WARN, "fail to consume_flush_scheduler_task", K(ret));
  }
  return ret;
}

int DBImpl::parallel_replay_one_wal_file(uint64_t file_number,
                                         uint64_t next_file_number,
                                         bool last_file,
                                         ReplayThreadPool &replay_thread_pool,
                                         ArenaAllocator &arena,
                                         uint64_t *read_time,
                                         uint64_t *parse_time,
                                         uint64_t *submit_time) {
  assert(WALRecoveryMode::kPointInTimeRecovery != immutable_db_options_.wal_recovery_mode);
  int ret = Status::kOk;
  std::string file_name = FileNameUtil::wal_file_path(immutable_db_options_.wal_dir, file_number);
  SE_LOG(INFO, "begin to read the wal file", K(file_name), "aio_read", immutable_db_options_.enable_aio_wal_reader);
  SequentialFile *file = nullptr;
  EnvOptions tmp_options = env_options_;
  tmp_options.arena = &arena;
  uint64_t file_size;
  if (FAILED(env_->NewSequentialFile(file_name, file, tmp_options).code())) {
    SE_LOG(WARN, "fail to open file", K(ret), K(file_name));
  } else if (FAILED(env_->GetFileSize(file_name, &file_size).code())) {
    SE_LOG(WARN, "fail to get file size", K(ret), K(file_name));
  } else {
    uint64_t last_record_end_pos = 0;
    bool last_record = false;
    Status status;
    LogReporter log_reporter(env_, file_name.c_str(), &status);
    auto file_reader = ALLOC_OBJECT(SequentialFileReader, arena, file, true);
    auto log_reader = ALLOC_OBJECT(log::Reader, arena, file_reader, &log_reporter,
        true/*checksum*/, 0 /*initial_offset*/, file_number, true,
        immutable_db_options_.enable_aio_wal_reader/*use aio*/, file_size);
    auto start_t = env_->NowMicros();
    uint64_t current_t = 0;
#ifndef NDEBUG
      uint64_t read_error_offset = UINT64_MAX;
      TEST_SYNC_POINT_CALLBACK("DBImpl::read_log::read_error", &read_error_offset);
#endif
    while (SUCCED(ret) && status.ok()) {
      Slice record;
      std::string scratch;
      uint32_t record_crc;
      last_record_end_pos = log_reader->get_last_record_end_pos();
      if (!log_reader->ReadRecord(&record, &scratch,
                                immutable_db_options_.wal_recovery_mode,
                                &record_crc)) {
        break; // read current log file finished
      }
#ifndef NDEBUG
      if (last_record_end_pos >= read_error_offset) {
        log_reporter.Corruption(record.size(), Status::Corruption("test error"));
        break;
      }
#endif
      current_t = env_->NowMicros();
      (*read_time) += current_t - start_t;
      start_t = current_t;
      if (WriteBatchInternal::kHeader > record.size()) {
        log_reporter.Corruption(record.size(), Status::Corruption("log record too small"));
        continue;
      } else {
        auto record_size = record.size();
        WriteBatch *replay_write_batch = nullptr;
        Status parse_status = ReplayTaskParser::parse_replay_writebatch_from_record(
                                  record, record_crc, this, &replay_write_batch,
                                  immutable_db_options_.allow_2pc);
        current_t = env_->NowMicros();
        (*parse_time) += current_t - start_t;
        start_t = current_t;
        if (!parse_status.ok()) {
          ret = parse_status.code();
          SE_LOG(ERROR, "parse and submit replay task failed", K(ret),
                      "error msg", parse_status.getState());
          log_reporter.Corruption(record_size, parse_status);
        } else if (FAILED(replay_thread_pool.build_and_submit_task(replay_write_batch,
                          file_number, last_file && log_reader->is_real_eof(),
                          last_record_end_pos, arena))) {
          SE_LOG(ERROR, "submit replay task to thread pool failed", K(ret));
        }
        current_t = env_->NowMicros();
        (*submit_time) += current_t - start_t;
        start_t = current_t;
      }
    }
    if (!status.ok()) {
      last_record = last_file && log_reader->IsEOF();
      bool stop_replay = false; // useless
      ret = deal_with_log_record_corrution(immutable_db_options_.wal_recovery_mode,
                                           file_name,
                                           last_record,
                                           last_record_end_pos,
                                           stop_replay);
    }
    FREE_OBJECT(Reader, arena, log_reader);
    FREE_OBJECT(SequentialFileReader, arena, file_reader); // file_reader won't be deleted by log_reader if use allocator
  }
  return ret;
}

int DBImpl::replay_one_wal_file(uint64_t file_number,
                                bool last_file,
                                SequenceNumber &next_allocate_sequence,
                                bool &stop_replay,
                                ArenaAllocator &arena)
{
  int ret = Status::kOk;
  Status status;
  std::string file_name = FileNameUtil::wal_file_path(immutable_db_options_.wal_dir, file_number);
  std::string scratch;
  Slice record;
  WriteBatch batch;
//  unique_ptr<SequentialFile> file;
  SequentialFile *file = nullptr;
  SequentialFileReader *file_reader = nullptr;
  log::Reader *log_reader = nullptr;
  LogReporter log_reporter(env_, file_name.c_str(), &status);
  SequenceNumber first_seq_in_write_batch = 0;
  uint64_t last_record_end_pos = 0;
  bool is_last_record = false; 

  SE_LOG(INFO, "begin replay the wal file", K(file_name));
  EnvOptions tmp_options = env_options_;
  tmp_options.arena = &arena;
  uint64_t file_size = 0;
  if (FAILED(env_->NewSequentialFile(file_name, file, tmp_options).code())) {
    SE_LOG(WARN, "fail to open file", K(ret), K(file_name));
  } else if (FAILED(env_->GetFileSize(file_name, &file_size).code())) {
    SE_LOG(WARN, "fail to get file size", K(ret), K(file_name));
  }  else {
    file_reader = ALLOC_OBJECT(SequentialFileReader, arena, file, true);
    log_reader = ALLOC_OBJECT(log::Reader, arena, file_reader, &log_reporter,
        true /*checksum*/, 0 /*initial_offset*/, file_number, true,
        immutable_db_options_.enable_aio_wal_reader, file_size);
    while (SUCCED(ret)
           && log_reader->ReadRecord(&record, &scratch, immutable_db_options_.wal_recovery_mode)
           && status.ok()) {
      last_record_end_pos = log_reader->get_last_record_end_pos();
      if (WriteBatchInternal::kHeader > record.size()) {
        //TODO:yuanfeng
        log_reporter.Corruption(record.size(), Status::Corruption("log record too small"));
        continue;
      } else if (FAILED(WriteBatchInternal::SetContents(&batch, record).code())) {
        SE_LOG(WARN, "fail to set batch contents", K(ret))	;
      } else {
        //check if continue to replay or not, when under kPointInTimeRecovery recovery mode
        if (stop_replay && WALRecoveryMode::kPointInTimeRecovery == immutable_db_options_.wal_recovery_mode) {
          if (next_allocate_sequence == WriteBatchInternal::Sequence(&batch)) {
            stop_replay = false;
          } else {
            break;
          }
        }
        if (FAILED(recovery_write_memtable(batch, file_number, next_allocate_sequence))) {
          SE_LOG(WARN, "fail to replay write memtable", K(ret), K(file_number), K(next_allocate_sequence));
        }
      }
    }

    if (!status.ok()) {
      is_last_record = last_file && log_reader->IsEOF();
      ret = deal_with_log_record_corrution(immutable_db_options_.wal_recovery_mode,
                                           file_name,
                                           is_last_record,
                                           last_record_end_pos,
                                           stop_replay);
    }
    FREE_OBJECT(SequentialFileReader, arena, file_reader);
    FREE_OBJECT(Reader, arena, log_reader);
  }
  return ret;
}

int DBImpl::consume_flush_scheduler_task(int *schedule_flush_num) {
  mutex_.AssertHeld();
  int ret = Status::kOk;
  ColumnFamilyData *sub_table = nullptr;

  while(SUCCED(ret) && (nullptr != (sub_table = flush_scheduler_.TakeNextColumnFamily()))) {
    if (!immutable_db_options_.avoid_flush_during_recovery) {
      ret = recovery_switch_and_flush_memtable(sub_table);
      if (SUCCED(ret) && schedule_flush_num != nullptr) {
        ++(*schedule_flush_num);
      }
      if (sub_table->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      }
    }
  }
  if (SUCCED(ret)) {
    flush_scheduler_.Clear();
  }

  return ret;
}

int DBImpl::recovery_switch_memtable(ColumnFamilyData *sub_table)
{
  int ret = Status::kOk;
  MemTable *new_mem = nullptr;
  WriteContext write_context;
  RecoveryPoint recovery_point(logfile_number_, versions_->LastSequence());

  if (IS_NULL(sub_table)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(sub_table));
  } else if (FAILED(sub_table->update_active_memtable(recovery_point, &(write_context.memtables_to_free_)))) {
    SE_LOG(WARN, "fail to update active memtable", K(ret), "index_id", sub_table->GetID());
  }

  return ret;
}

int DBImpl::recovery_write_memtable(WriteBatch &batch, uint64_t current_log_file_number, SequenceNumber &next_allocate_sequence)
{
  int ret = Status::kOk;
  SequenceNumber first_seq_in_write_batch = 0;

  ColumnFamilyMemTablesImpl column_family_memtables(
      versions_->get_global_ctx()->all_sub_table_->sub_table_map_,
      versions_->GetColumnFamilySet());

  if (WALRecoveryMode::kPointInTimeRecovery == immutable_db_options_.wal_recovery_mode
      && kMaxSequenceNumber != next_allocate_sequence
      && next_allocate_sequence != (first_seq_in_write_batch = WriteBatchInternal::Sequence(&batch))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, seq not continuously", K(ret), K(next_allocate_sequence), K(first_seq_in_write_batch));
  } else if (FAILED(WriteBatchInternal::InsertInto(&batch, &column_family_memtables, &flush_scheduler_, true,
          current_log_file_number, this, false /*concurrent_memtable_writes*/,
          &next_allocate_sequence, nullptr /*has_valid_write*/, &missing_subtable_during_recovery_).code())) {
    SE_LOG(WARN, "fail to replay write memtable", K(ret), K(current_log_file_number), K(next_allocate_sequence));
  }

   return ret;
}

int DBImpl::recovery_flush(ColumnFamilyData *sub_table)
{
  int ret = Status::kOk;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  STFlushJob st_flush_job(sub_table, nullptr, FLUSH_TASK, true /*need_check_snapshot*/);
 
  if (FAILED(FlushMemTableToOutputFile(st_flush_job, nullptr, job_context).code())) {
    SE_LOG(WARN, "fail to flush memtable", K(ret));
  } else {
    job_context.Clean();
  }

  return ret;
}

bool DBImpl::check_if_need_switch_memtable() {
  bool ret = false;
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("DBImpl::check_if_need_switch_memtable::trigger_switch_memtable", this);
  if (TEST_trigger_switch_memtable_) {
    return true;
  }
#endif
  if (immutable_db_options_.avoid_flush_during_recovery) {
    ret = false;
  } else if (check_switch_memtable_now()) {
    if (flush_scheduler_.size() >= MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME ||
        write_buffer_manager_->should_trim()) {
      no_switch_round_ = 0;
      ret = true;
    } else if (!flush_scheduler_.Empty()) {
      if (++no_switch_round_ >= MAX_NO_SWITCH_ROUND) {
        no_switch_round_ = 0;
        ret = true;
      }
    }
  }
  return ret;
}

// this method can only be called by recovery main thread
bool DBImpl::check_switch_memtable_now() {
  mutex_.AssertHeld();
  if (last_check_time_during_recovery_ == 0) {
    last_check_time_during_recovery_ = env_->NowMicros();
  }
  bool ret = false;
  auto current_timestamp = env_->NowMicros();
  if (current_timestamp > last_check_time_during_recovery_ + CHECK_NEED_SWITCH_DELTA) {
    last_check_time_during_recovery_ = current_timestamp;
    ret = true;
  }
  return ret;
}

int DBImpl::switch_memtable_during_parallel_recovery(std::list<SubTable*>& switched_sub_tables) {
  mutex_.AssertHeld();
  SE_LOG(INFO, "start switching memtable during recovery", "total_memory_usage",
      write_buffer_manager_->total_memory_usage());
  int ret = Status::kOk;
  if (immutable_db_options_.avoid_flush_during_recovery) {
    return ret;
  }

  // since all replay threads are suspend, we can safely update last_sequence_
  // here to advance recovery point during switching memtable
  update_last_sequence_during_recovery();

  // 1. consume flush tasks in flush_scheduler
  int schedule_switch_num = 0;
  ColumnFamilyData *sub_table = nullptr;
  while(SUCCED(ret) && (nullptr != (sub_table = flush_scheduler_.TakeNextColumnFamily()))) {
    ret = recovery_switch_memtable(sub_table);
    if (SUCCED(ret)) {
      switched_sub_tables.push_back(sub_table);
      ++schedule_switch_num;
    } else {
      if (sub_table->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      }
    }
  }
  if (FAILED(ret)) {
    SE_LOG(WARN, "fail to consume flush scheduler task", K(ret));
    return ret;
  }
  flush_scheduler_.Clear();
  if (schedule_switch_num >= MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME) {
    // we have already flush enough subtables
    SE_LOG(INFO, "finished switching", K(schedule_switch_num));
    return ret;
  }

  // 2. switch subtables with the largest memory usage
  uint64_t expected_pick_num = MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME - schedule_switch_num;
  GlobalContext *global_ctx = versions_->get_global_ctx();
  sub_table = nullptr;
  std::set<uint32_t> picked_cf_ids;
  uint64_t picked_switch_num = 0;
  if (nullptr == global_ctx || nullptr == global_ctx->all_sub_table_) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else {
    SubTableMap &all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
    ReplayThreadPool::SubtableMemoryUsageComparor mem_cmp;
    BinaryHeap<SubTable*, ReplayThreadPool::SubtableMemoryUsageComparor> max_sub_tables;
    if (FAILED(pick_and_switch_subtables(all_sub_tables, picked_cf_ids,
                                        expected_pick_num, mem_cmp,
                                        &picked_switch_num, &max_sub_tables,
                                        switched_sub_tables))) {
      SE_LOG(WARN, "fail to switch subtables with largest memory usage", K(ret));
      return ret;
    }
  }
  schedule_switch_num += picked_switch_num;
  if (schedule_switch_num >= MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME) {
    // we have already flush enough subtables
    SE_LOG(INFO, "finished switching", K(schedule_switch_num));
    return ret;
  }

  // 3. switch oldest subtale
  SubTableMap &all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
  expected_pick_num = MAX_SWITCH_NUM_DURING_RECOVERY_ONE_TIME - schedule_switch_num;
  ReplayThreadPool::SubtableSeqComparor seq_cmp;
  BinaryHeap<SubTable*, ReplayThreadPool::SubtableSeqComparor> oldest_sub_tables;
  if (FAILED(pick_and_switch_subtables(all_sub_tables, picked_cf_ids,
                                      expected_pick_num, seq_cmp,
                                      &picked_switch_num, &oldest_sub_tables,
                                      switched_sub_tables))) {
    SE_LOG(WARN, "fail to switch oldest subtables", K(ret));
    return ret;
  }
  schedule_switch_num += picked_switch_num;
  SE_LOG(INFO, "finished switching", K(schedule_switch_num));
  return ret;
}

template <typename Compare>
int DBImpl::pick_and_switch_subtables(const SubTableMap& all_sub_tables,
                                     std::set<uint32_t>& picked_cf_ids,
                                     uint64_t expected_pick_num,
                                     Compare& cmp,
                                     uint64_t *picked_num,
                                     BinaryHeap<SubTable*, Compare> *picked_sub_tables,
                                     std::list<SubTable*>& switched_sub_tables) {
  int ret = Status::kOk;
  *picked_num = 0;
  SubTable *sub_table = nullptr;
  for (auto iter = all_sub_tables.begin();
      SUCCED(ret) && iter != all_sub_tables.end(); ++iter) {
    if (UNLIKELY(nullptr == (sub_table = iter->second) || nullptr == sub_table->mem())) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "subtable must not be nullptr", K(ret), K(iter->first), KP(sub_table), K(*sub_table));
    } else {
      if (sub_table->mem()->IsEmpty()) {
        continue;
      }
      if (picked_cf_ids.find(sub_table->GetID()) != picked_cf_ids.end()) {
        continue;
      }
      if (picked_sub_tables->size() < expected_pick_num) {
        picked_sub_tables->push(sub_table);
      } else {
        if (cmp(sub_table, picked_sub_tables->top())) {
          picked_sub_tables->replace_top(sub_table);
        }
      }
    }
  }
#ifndef NDEBUG
  if (TEST_avoid_flush_) {
    return ret;
  }
#endif
  while(SUCCED(ret) && !picked_sub_tables->empty()) {
    SubTable *picked_sub_table = picked_sub_tables->top();
    picked_sub_tables->pop();
    auto sub_table_id = picked_sub_table->GetID();
    auto mem_usage = picked_sub_table->mem()->ApproximateMemoryUsage();
    auto first_seq = picked_sub_table->mem()->GetFirstSequenceNumber();
    ret = recovery_switch_memtable(picked_sub_table);
    SE_LOG(DEBUG, "switched subtable", K(sub_table_id), K(mem_usage), K(first_seq), K(ret));
    if (SUCCED(ret)) {
      picked_cf_ids.insert(sub_table_id);
      picked_sub_table->Ref();
      switched_sub_tables.push_back(picked_sub_table);
      ++(*picked_num);
    } else {
      SE_LOG(ERROR, "failed to switch memtable during recovery", K(sub_table_id), K(ret));
    }
  }
  picked_sub_tables->clear();
  return ret;
}

int DBImpl::flush_memtable_during_parallel_recovery(std::list<SubTable*>& switched_sub_tables) {
  int ret = Status::kOk;
  if (!write_buffer_manager_->should_trim()) {
    for (auto sub_table : switched_sub_tables) {
      if (sub_table->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      }
    }
    switched_sub_tables.clear();
    return ret; // no need to flush memtable during parallel recovery
  }
  for (auto sub_table : switched_sub_tables) {
    if (SUCCED(ret)) {
      ret = recovery_flush(sub_table);
    }
    if (FAILED(ret)) {
      SE_LOG(WARN, "failed to flush subtable", "sub_table_id", sub_table->GetID());
    }
    if (sub_table->Unref()) {
      MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
    }
  }
  SE_LOG(INFO, "finish flushing subtables during recovery", "schedule_flush_num",
              switched_sub_tables.size(), "total_memory_usage",
              write_buffer_manager_->total_memory_usage());
  switched_sub_tables.clear();
  return ret;
}

int DBImpl::recovery_switch_and_flush_memtable(ColumnFamilyData *sub_table) {
  int ret = Status::kOk;
  if (FAILED(recovery_switch_memtable(sub_table))) {
    SE_LOG(WARN, "fail to replay switch memtable", K(ret), "index_id", sub_table->GetID());
  } else if (FAILED(recovery_flush(sub_table))) {
    SE_LOG(WARN, "fail to recovery flush", K(ret), "index_id", sub_table->GetID());
  }
  return ret;
}

int DBImpl::parallel_recovery_write_memtable(WriteBatch &batch,
    uint64_t current_log_file_number, uint64_t* next_allocate_sequence) {
  int ret = Status::kOk;
  GlobalContext* global_ctx = nullptr;
  AllSubTable* all_sub_table = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    ColumnFamilyMemTablesImpl column_family_memtables(all_sub_table->sub_table_map_,
        versions_->GetColumnFamilySet());
    Status s = WriteBatchInternal::InsertInto(&batch, &column_family_memtables,
        &flush_scheduler_, true, current_log_file_number, this,
        true/*concurrent_memtable_writes*/, next_allocate_sequence,
        nullptr /*has_valid_write*/, &missing_subtable_during_recovery_);
    if (FAILED(s.code())) {
      SE_LOG(WARN, "fail to replay write memtable", K(ret),
          K(current_log_file_number), K(*next_allocate_sequence),
          "errormsg", s.getState());
    }
  }
  if (nullptr != global_ctx) {
    global_ctx->release_thread_local_all_sub_table(all_sub_table);
  }
  return ret;
}

int DBImpl::update_max_sequence_and_log_number(uint64_t next_allocate_sequence,
    uint64_t current_log_file_number) {
  int ret = Status::kOk;
  uint64_t current_max_sequence = next_allocate_sequence - 1;
  while(true) {
    uint64_t old_max_sequence = max_sequence_during_recovery_.load();
    if (current_max_sequence > old_max_sequence) {
      if (max_sequence_during_recovery_.compare_exchange_strong(
                old_max_sequence, current_max_sequence)) {
        break;
      }
    } else {
      break;
    }
  }
  while(true) {
    uint64_t old_logfile_number = max_log_file_number_during_recovery_.load();
    if (current_log_file_number > old_logfile_number) {
      if (max_log_file_number_during_recovery_.compare_exchange_strong(
            old_logfile_number, current_log_file_number)) {
        break;
      }
    } else {
      break;
    }
  }
  return ret;
}

int DBImpl::deal_with_log_record_corrution(WALRecoveryMode recovery_mode,
                                           const std::string file_name,
                                           bool is_last_record,
                                           uint64_t last_record_end_pos,
                                           bool &stop_replay) {
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  uint64_t origin_file_size = 0;

  //we support three recovery mode and use kAbsoluteConsistency as default mode:
  //kAbsoluteConsistency : absolute consistency mode. require all wal files are correct, recovery to the state before shutdown.
  //                       stop replay and system exit, when find incorrect wal file
  //kPointInTimeRecovery: maximize consistency mode. recovery to point-in-time consistency
  //                      stop replay and system start, when find incorrect wal file
  //kSkipAnyCorruptedRecords: maximize recovery data mode. recovery as much data as possible.
  //                          continue replay and ignore the incorrect wal file
  if (WALRecoveryMode::kAbsoluteConsistency == recovery_mode) {
    if (is_last_record) {
      util::MutexLock lock_guard(&deal_last_record_error_mutex_);
      if (FAILED(env_->GetFileSize(file_name, &origin_file_size).code())) {
        SE_LOG(WARN, "fail get file_size", K(ret), K(origin_file_size));
      } else if (origin_file_size < last_record_end_pos) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "can't truncate current file", K(ret), K(origin_file_size), K(last_record_end_pos));
      } else if (0 != (tmp_ret = truncate(file_name.c_str(), last_record_end_pos))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "fail to truncate wal file", K(tmp_ret), K(file_name), K(origin_file_size), K(last_record_end_pos),
            "errno", errno, "err_msg", strerror(errno));
      } else {
        SE_LOG(INFO, "success to truncate wal file", K(file_name), K(origin_file_size), K(last_record_end_pos));
      }
    } else {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, only last record may corruption under kAbsoluteConsistency recovery mode",
          K(ret), K(file_name), K(last_record_end_pos));
    }
  } else if (WALRecoveryMode::kPointInTimeRecovery == recovery_mode) {
    //kPointInTimeRecovery only attention consistency, don't care log record corruption
    //if the sequence is continuously with the next wal file, reset stop_replay to false, and continue to replay
    stop_replay = true;
  } else if (WALRecoveryMode::kSkipAnyCorruptedRecords == recovery_mode) {
    //kSkipAnyCorruptedRecords ignore any log record corruption
  } else {
    ret = Status::kNotSupported;
    SE_LOG(WARN, "the recovery mode is not supported", K(ret), KE(immutable_db_options_.wal_recovery_mode));
  }

  return ret;
}

int DBImpl::update_wal_writer(bool force)
{
  mutex_.AssertHeld();
  int ret = Status::kOk;
  bool create_new_log = !log_empty_ || force;

  if (create_new_log) {
    WritableFile *log_file = nullptr;
    ConcurrentDirectFileWriter *log_file_writer = nullptr;
    log::Writer *log_writer = nullptr;
    // TODO (Zhao Dongsheng) : the implementation of mutual construction between options is unclear.
    DBOptions curr_db_options = BuildDBOptions(immutable_db_options_, mutable_db_options_);
    EnvOptions opt_env_options = env_->OptimizeForLogWrite(env_options_, curr_db_options);
    uint64_t new_log_file_number = versions_->NewFileNumber();
    std::string new_log_file_name = FileNameUtil::wal_file_path(immutable_db_options_.wal_dir, new_log_file_number);

    if (FAILED(NewWritableFile(env_, new_log_file_name, log_file, opt_env_options).code())) {
      SE_LOG(WARN, "fail to create new log file", K(ret), K(new_log_file_name));
    } else {
      // TODO (Zhao Dongsheng): The preallocate is not actually working? And the parameter should be mutable_cf_options.write_buffer_size?
      log_file->SetPreallocationBlockSize(GetWalPreallocateBlockSize(32 * 1024));
      if (IS_NULL(log_file_writer = MOD_NEW_OBJECT(ModId::kLogWriter, ConcurrentDirectFileWriter, log_file, opt_env_options))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for log file writer", K(ret));
      } else if (IS_NULL(log_writer = MOD_NEW_OBJECT(ModId::kLogWriter,
                                                     log::Writer,
                                                     log_file_writer,
                                                     new_log_file_number,
                                                     false,
                                                     false))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for log writer", K(ret));
      } else {
        // TODO(Zhao Dongsheng): If it affects performance, the sync operation here needs
        // to be made asynchronous.
        std::unique_lock<std::mutex> lock_sync_guard(log_sync_mutex_);
        if (IS_NOTNULL(curr_log_writer_)) {
          curr_log_writer_->file()->sync_to_disk(false);
          MOD_DELETE_OBJECT(Writer, curr_log_writer_);
        }
        directories_.GetWalDir()->Fsync();
        log_dir_synced_ = true;
        logfile_number_ = new_log_file_number;
        log_empty_ = true;
        last_flushed_log_lsn_.store(0);
        curr_log_writer_ = log_writer;
        alive_log_files_.push_back(LogFileNumberSize(logfile_number_));
      }
    }
  }

  return ret;
}

Status DB::Open(const Options &options,
                const std::string &dbname,
                std::vector<ColumnFamilyHandle *> *handles,
                DB **dbptr)
{
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);

  Status s = SanitizeOptionsByTable(db_options, cf_options);
  if (!s.ok()) {
    return s;
  }

  s = ValidateOptions(db_options, cf_options);
  if (!s.ok()) {
    return s;
  }

  *dbptr = nullptr;
  handles->clear();

  DBImpl *impl = MOD_NEW_OBJECT(ModId::kDBImpl, DBImpl, db_options, dbname);
  s = impl->env_->CreateDirIfMissing(impl->immutable_db_options_.wal_dir);
  if (s.ok()) {
    for (auto db_path : impl->immutable_db_options_.db_paths) {
      s = impl->env_->CreateDirIfMissing(db_path.path);
      if (!s.ok()) {
        break;
      }
    }
  }

  if (!s.ok()) {
    MOD_DELETE_OBJECT(DBImpl, impl);
    return s;
  }
  AllocMgr *alloc = AllocMgr::get_instance();
  alloc->set_mod_size_limit(ModId::kMemtable, impl->immutable_db_options_.db_total_write_buffer_size);
  int64_t task_mem_limit = 128 * 1024 * 1024L;
  alloc->set_mod_size_limit(ModId::kCompaction, impl->mutable_db_options_.max_background_compactions * task_mem_limit);
  //TODO:@yuanfeng, init in the handler layer
  if (!logger::Logger::get_log().is_inited()) {
    std::string info_log_path = dbname + "/Log";
    logger::Logger::get_log().init(info_log_path.c_str(),
        logger::InfoLogLevel::INFO_LEVEL,
        256 * 1024 * 1024);
  }

  int64_t ret = Status::kOk;
  char *tmp_buf = nullptr;
  GlobalContext *gctx = nullptr;
  int64_t total_flush_compaction_thread_count = impl->initial_db_options_.max_background_flushes +
      impl->initial_db_options_.max_background_compactions;
  if (nullptr != cf_options.table_factory) {
    cache::Cache *block_cache =
        static_cast<ExtentBasedTableFactory *>(cf_options.table_factory.get())->table_options().block_cache.get();
    if (nullptr != block_cache) {
      alloc->set_mod_size_limit(ModId::kDataBlockCache, block_cache->GetCapacity());
      impl->block_cache_ = block_cache;
    }
  }
  if (IS_NULL(impl->table_cache_.get())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, table cache must not nullptr", K(ret));
  } else if (FAILED(WriteExtentJobScheduler::get_instance().start(impl->env_, total_flush_compaction_thread_count))) {
    SE_LOG(WARN, "fail to start write extent job scheduler", K(ret), K(total_flush_compaction_thread_count));
  } else if (FAILED(StorageLogger::get_instance().init(impl->env_,
                                                       dbname,
                                                       impl->env_options_,
                                                       impl->immutable_db_options_,
                                                       impl->versions_.get(),
                                                       64 * 1024 * 1024))) {
    SE_LOG(WARN, "fail to init StorageLogger", K(ret));
  } else if (FAILED(ExtentMetaManager::get_instance().init())) {
    SE_LOG(WARN, "fail to init ExtentMetaManager", K(ret)); 
  } else if (FAILED(ExtentSpaceManager::get_instance().init(impl->env_, impl->env_options_, impl->initial_db_options_))) {
    SE_LOG(WARN, "fail to init ExtentSpaceManager", K(ret));
  } else if (IS_NULL(gctx = MOD_NEW_OBJECT(ModId::kDefaultMod, GlobalContext, dbname, options))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for GlobalContext", K(ret));
  } else {
    GlobalContext::set_env(impl->env_);
    GlobalContext::cache_ = impl->table_cache_.get();
    gctx->options_ = options;
    gctx->env_options_ = impl->env_options_;
    gctx->write_buf_mgr_ = impl->write_buffer_manager_;
    gctx->reset_thread_local_all_sub_table();
    gctx->db_dir_ = impl->directories_.GetDbDir();
    if (FAILED(impl->versions_->init(gctx))) {
      SE_LOG(WARN, "fail to init versions", K(ret));
    }
  }
  s = Status(ret);
  if (!s.ok()) {
    MOD_DELETE_OBJECT(DBImpl, impl);
    return s;
  }

  if (!s.ok()) {
    MOD_DELETE_OBJECT(DBImpl, impl);
    return s;
  }
  impl->mutex_.Lock();
  s = impl->Recover(cf_options);
  if (s.ok()) {
    SubTable* sub_table = nullptr;
    for (auto iter :
         impl->versions_->get_global_ctx()->all_sub_table_->sub_table_map_) {
      handles->push_back(
          MOD_NEW_OBJECT(ModId::kColumnFamilySet, ColumnFamilyHandleImpl, iter.second, impl, &impl->mutex_));
      sub_table = iter.second;
      impl->NewThreadStatusCfInfo(sub_table);
      // no compaction schedule in recovery
      SuperVersion *old_sv = sub_table->InstallSuperVersion(MOD_NEW_OBJECT(ModId::kSuperVersion, SuperVersion), &impl->mutex_);
      MOD_DELETE_OBJECT(SuperVersion, old_sv);
    }
    impl->alive_log_files_.push_back(
        DBImpl::LogFileNumberSize(impl->logfile_number_));
    impl->DeleteObsoleteFiles();
    s = impl->directories_.GetDbDir()->Fsync();
  }
  // init BatchGroupManager;
  impl->batch_group_manager_.init();

  if (0 != impl->pipline_manager_.init_pipline_queue()) {
    SE_LOG(ERROR, "Failed to init pipline queue, probably memory limit");
    s = Status::MemoryLimit("init pipline queue failed");
    assert(s.ok());  // failed directly in dbug mode
  }

  if (s.ok()) {
    SubTable* sub_table = nullptr;
    // TODO(Zhao Dongsheng): The follow loop to set snapshot supported is useless?
    for (auto iter : impl->versions_->get_global_ctx()->all_sub_table_->sub_table_map_) {
      sub_table = iter.second;
      if (!sub_table->mem()->IsSnapshotSupported()) {
        impl->is_snapshot_supported_ = false;
      }
      if (!s.ok()) {
        break;
      }
    }
  }

  TEST_SYNC_POINT("DBImpl::Open:Opened");
  //Status persist_options_status;
  if (s.ok()) {
    // Persist Options before scheduling the compaction.
    // The WriteOptionsFile() will release and lock the mutex internally.
    //persist_options_status = impl->WriteOptionsFile();

    *dbptr = impl;
    impl->opened_successfully_ = true;
    impl->MaybeScheduleFlushOrCompaction();
    impl->schedule_master_thread();
  }
  impl->mutex_.Unlock();
  assert(impl->versions_->LastSequence() ==
         impl->versions_->LastAllocatedSequence());

  if (s.ok()) {
    SE_LOG(SYSTEM, "success to open DBImpl");
  }
  if (!s.ok()) {
    for (auto* h : *handles) {
      MOD_DELETE_OBJECT(ColumnFamilyHandle, h);
    }
    handles->clear();
    MOD_DELETE_OBJECT(DBImpl, impl);
    *dbptr = nullptr;
  }
  return s;
}

std::string RecoveryDebugInfo::show()
{
  std::stringstream ss;
  ss << "P=" << prepare << ",ewc=" << prepare_external_write_ckpt << ",cds=" << prepare_create_default_subtable << ";"
     << "R=" << recovery << ",sr=" << recovery_slog_replay << ",w=" << recovery_wal << ";"
     << "W=" << recoverywal << ",b=" << recoverywal_before << ",wf=" << recoverywal_wal_files
             << ",a=" << recoverywal_after << ",fc=" << recoverywal_file_count << ",s=" << recoverywal_slowest;
  return ss.str();
}

}
}  // namespace smartengine
