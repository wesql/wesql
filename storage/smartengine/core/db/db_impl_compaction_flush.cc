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
#include <inttypes.h>
#include <algorithm>

#include "compact/compaction_job.h"
#include "db/dump_job.h"
#include "logger/log_module.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "util/sync_point.h"
#include "util/string_util.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "storage/shrink_job.h"
#include "storage/storage_logger.h"
#include "util/ebr.h"

namespace smartengine
{
using namespace common;
using namespace memory;
using namespace monitor;
using namespace storage;
using namespace table;
using namespace util;

namespace db
{

int DBImpl::dump_memtable_to_outputfile(
    STDumpJob &st_dump_job,
    bool *madeProgress,
    JobContext& job_context)
{
  int ret = Status::kOk;
  assert(madeProgress);
  *madeProgress = false;
  mutex_.AssertHeld();
  ColumnFamilyData *sub_table = st_dump_job.sub_table_;
  SequenceNumber earliest_write_conflict_snapshot = 0;
  std::vector<SequenceNumber> snapshot_seqs = GetAll(&earliest_write_conflict_snapshot);
  assert(sub_table);
  assert(sub_table->GetLatestMutableCFOptions());
  job_context.task_type_ = DUMP_TASK;
  DumpJob *dump_job = ALLOC_OBJECT(
      DumpJob, st_dump_job.dump_alloc_,sub_table, immutable_db_options_,
      *sub_table->GetLatestMutableCFOptions(),
      GetCompressionFlush(*sub_table->ioptions(), 0),
      &shutting_down_, earliest_write_conflict_snapshot, job_context, snapshot_seqs,
      directories_.GetDataDir(0U), &env_options_, st_dump_job.dump_mem_,
      st_dump_job.dump_max_seq_, st_dump_job.dump_alloc_);
  std::vector<SequenceNumber> flushed_seqs;
  if (FAILED(run_one_flush_task(sub_table, dump_job, job_context, flushed_seqs))) {
    SE_LOG(WARN, "failed to run one flush task", K(ret));
  } else {
    InstallSuperVersionAndScheduleWorkWrapper(sub_table, &job_context);
    *madeProgress = true;
    if (nullptr != sub_table) {
      sub_table->set_pending_dump(false);
      SchedulePendingFlush(sub_table);
      MaybeScheduleFlushOrCompaction();
    }
  }

  if (bg_error_.ok()
      && Status::kShutdownInProgress == ret) {
    // do noting
  } else if (Status::kCancelTask == ret) {
    ret = Status::kOk; // by design
    SE_LOG(INFO, "just cancel the dump task", K(sub_table->GetID()));
  } else if (FAILED(ret)) {
    // if a bad error happened (not ShutdownInProgress) mark DB read-only
    SE_LOG(ERROR, "flush memtable error set bg_error_", K(ret));
    bg_error_ = Status(ret);
  }
  FREE_OBJECT(DumpJob, st_dump_job.dump_alloc_, dump_job);
  return ret;
}

int DBImpl::run_one_flush_task(ColumnFamilyData *sub_table,
                               BaseFlush* flush_job,
                               JobContext& context,
                               std::vector<SequenceNumber> &flushed_seqs) {
  mutex_.AssertHeld();
  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  objstore::ObjStoreSnapshotOperator *snapshot_operator = nullptr;
  int64_t metasnapshot_id = 0;
  SeEvent event = storage::INVALID_EVENT;
  TaskType task_type = flush_job->get_task_type();
  if (TaskType::DUMP_TASK == task_type) {
    event = storage::SeEvent::DUMP;
  } else if (is_flush_task(task_type)) {
    event = storage::SeEvent::FLUSH;
  } else {
    SE_LOG(WARN, "invalid task type", K((int)task_type));
  }
  if (storage::INVALID_EVENT != event) {
    MiniTables mtables;
    if (FAILED(flush_job->prepare_flush_task(mtables))){
      SE_LOG(WARN, "failed to prepare flush task", K(ret));
    } else {
      auto& mems = flush_job->get_picked_memtable();
      for (MemTable* m : mems) {
        flushed_seqs.push_back(m->GetFirstSequenceNumber());
      }
    }
    if (FAILED(ret)) {
      flush_job->cancel();
    } else {
      // unlock when flush task run
      mutex_.Unlock();
#ifndef NDEBUG
    TEST_SYNC_POINT_CALLBACK("DBImpl::wait_create_backup_snapshot", this);
#endif
      int64_t dummy_log_seq = 0;
      if (FAILED(StorageLogger::get_instance().begin(event))) {
        SE_LOG(WARN, "failed to begin flush event", K(ret), K((int)task_type));
      } 
      AutoThreadOperationStageUpdater stage_run(ThreadStatus::STAGE_FLUSH_RUN);
      if (SUCCED(ret) && FAILED(flush_job->run(mtables))) {
        SE_LOG(WARN, "failed to run flush task", K(ret));
      }
      if (FAILED(flush_job->after_run_flush(mtables, ret, &mutex_))) {
        SE_LOG(WARN, "failed to do func after run flush", K(ret));
      } else if (FAILED(StorageLogger::get_instance().commit(dummy_log_seq))) {
        SE_LOG(WARN, "fail to commit flush trans", K(ret));
      }
      mutex_.Lock();
    }
  }
  return ret;
}

Status DBImpl::FlushMemTableToOutputFile(
    STFlushJob &st_flush_job,
    bool* made_progress,
    JobContext& job_context) {
  mutex_.AssertHeld();
  int ret = Status::kOk;
  ColumnFamilyData *sub_table = st_flush_job.get_subtable();
  assert(sub_table->imm()->NumNotFlushed() != 0);
  SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber;

  // build context
  storage::CompactionContext context;
  context.shutting_down_ = &shutting_down_;
  context.bg_stopped_ = sub_table->bg_stopped();
  context.cf_options_ = sub_table->ioptions();
  context.mutable_cf_options_ = sub_table->GetLatestMutableCFOptions();
  context.env_options_ = sub_table->soptions();
  context.data_comparator_ = sub_table->ioptions()->user_comparator;
  context.internal_comparator_ = &sub_table->internal_comparator();
  context.table_space_id_ = sub_table->get_table_space_id();
  context.existing_snapshots_ = GetAll(&context.earliest_write_conflict_snapshot_);
  context.task_type_ = st_flush_job.get_task_type();
  context.enable_thread_tracking_ = immutable_db_options_.enable_thread_tracking;
  context.output_level_  = FLUSH_LEVEL1_TASK == st_flush_job.get_task_type() ? 1 : 0;
  job_context.task_type_ = st_flush_job.get_task_type();
  job_context.output_level_ = context.output_level_;
  FlushJob *flush_job = ALLOC_OBJECT(
      FlushJob, st_flush_job.flush_alloc_,
      dbname_, sub_table, immutable_db_options_,
      job_context, directories_.GetDataDir(0U),
      GetCompressionFlush(*sub_table->ioptions(), context.output_level_),
      stats_, context, st_flush_job.flush_alloc_);

  if (nullptr != flush_job) {
    // just for flush to level1
    flush_job->set_meta_snapshot(st_flush_job.meta_snapshot_);
  }
  std::vector<SequenceNumber> flushed_seqs;
  if (FAILED(run_one_flush_task(sub_table, flush_job, job_context, flushed_seqs))) {
    SE_LOG(WARN, "failed to run one flush task", K(ret));
  } else {
    InstallSuperVersionAndScheduleWorkWrapper(sub_table, &job_context);
    //@yuanfeng, ugly.this used to control flushjob seria in subtable
    sub_table->set_pending_flush(false);
    if (FLUSH_LEVEL1_TASK == st_flush_job.get_task_type()) {
      sub_table->set_pending_compaction(false);
      SchedulePendingCompaction(sub_table);
      MaybeScheduleFlushOrCompaction();
    }
    if (made_progress) {
      *made_progress = true;
    }

    for (SequenceNumber seq : flushed_seqs) {
      MemtableCleanupInfo cleanup_info(sub_table, seq);
      sub_table->Ref();
      memtable_cleanup_queue_.push_back(cleanup_info);
    }
    trim_mem_flush_waited_ = kFlushDone;
  }
  if (FAILED(ret)) {
    // avoid issue on closing DB with failed flushing job
    sub_table->set_pending_flush(false);
    if (FLUSH_LEVEL1_TASK == st_flush_job.get_task_type()) {
      sub_table->set_pending_compaction(false);
    }
  }
  if (bg_error_.ok()
      && Status::kShutdownInProgress == ret) {
    // do noting
  } else if (Status::kCancelTask == ret) {
    ret = Status::kOk; // by design
    SE_LOG(INFO, "just cancel the task", K(get_task_type_name(st_flush_job.get_task_type())));
  } else if (FAILED(ret)) {
    // if a bad error happened (not ShutdownInProgress) mark DB read-only
    SE_LOG(ERROR, "flush memtable error set bg_error_", K(ret));
    bg_error_ = Status(ret);
  }
  FREE_OBJECT(FlushJob, st_flush_job.flush_alloc_, flush_job);
  return Status(ret);
}

Status DBImpl::CompactRange(const uint32_t compact_type)
{
  Status s;
  int ret = 0;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    ArenaAllocator arena;
    SubTableMap &sub_table_map = all_sub_table->sub_table_map_;
    for (auto iter : sub_table_map) {
      ColumnFamilyHandleImpl *handle = ALLOC_OBJECT(ColumnFamilyHandleImpl, arena, iter.second, this, &mutex_);
      if (nullptr != handle) {
        s = CompactRange(handle, compact_type);
        static_cast<ColumnFamilyHandleImpl *>(handle)->~ColumnFamilyHandleImpl();
        if (!s.ok()) {
          break;
        }
      }
    }
  }

  s = Status(ret);
  int tmp_ret = 0;
  if (nullptr != all_sub_table
      && 0 != (tmp_ret = global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to release all sub table", K(ret));
  }
  if (s.ok() && 0 != tmp_ret) {
    s = Status(tmp_ret);
  }
  return s;
}

Status DBImpl::CompactRange(ColumnFamilyHandle* column_family, const uint32_t compact_type)
{
  Status s;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData *sub_table = nullptr;
  if ((TaskType)compact_type > TaskType::MAX_TYPE_TASK) {
    s = Status::InvalidArgument();
    SE_LOG(WARN, "Invalid task type", K(compact_type));
  } else if (bg_work_paused_ > 0) {
    // we paused the background work
    SE_LOG(WARN, "paused the background work", K(compact_type));
  } else if (nullptr == cfh || nullptr == (sub_table = cfh->cfd())) {
    s = Status::InvalidArgument();
    SE_LOG(WARN, "sub table is null", K(compact_type), KP(cfh));
  } else {
    {
      InstrumentedMutexLock l(&mutex_);
      sub_table->set_manual_compaction_type((TaskType)compact_type);
      SchedulePendingCompaction(sub_table);
      // an automatic compaction that has been scheduled might have been
      // preempted by the manual compactions. Need to schedule it back.
      MaybeScheduleFlushOrCompaction();
    }
  }
  return s;
}

Status DBImpl::PauseBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  bg_compaction_paused_++;
  while (bg_compaction_scheduled_ > 0
         || bg_flush_scheduled_ > 0
         || bg_dump_scheduled_ > 0) {
    bg_cv_.Wait();
  }
  bg_work_paused_++;
  return Status::OK();
}

Status DBImpl::ContinueBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  if (bg_work_paused_ == 0) {
    return Status::InvalidArgument();
  }
  assert(bg_work_paused_ > 0);
  assert(bg_compaction_paused_ > 0);
  bg_compaction_paused_--;
  bg_work_paused_--;
  // It's sufficient to check just bg_work_paused_ here since
  // bg_work_paused_ is always no greater than bg_compaction_paused_
  if (bg_work_paused_ == 0) {
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}

// Will lock the mutex_,  will wait for completion if wait is true
void DBImpl::CancelAllBackgroundWork(bool wait) {
  InstrumentedMutexLock l(&mutex_);

  SE_LOG(INFO, "Shutdown: canceling all background work");

  if (!shutting_down_.load(std::memory_order_acquire) &&
      has_unpersisted_data_.load(std::memory_order_relaxed) &&
      !mutable_db_options_.avoid_flush_during_shutdown) {
    int ret = Status::kOk;
    GlobalContext* global_ctx = nullptr;
    SubTable* sub_table = nullptr;
    if (nullptr == (global_ctx = versions_->get_global_ctx())) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "global ctx must not nullptr", K(ret));
    } else {
      SubTableMap& all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
      for (auto iter = all_sub_tables.begin();
           Status::kOk && all_sub_tables.end() != iter; ++iter) {
        if (nullptr == (sub_table = iter->second)) {
          ret = Status::kCorruption;
          SE_LOG(WARN, "subtable must not nullptr", K(ret),
                      K(iter->first));
        } else if (sub_table->IsDropped()) {
          // do nothing
          SE_LOG(INFO, "subtable has been dropped", "index_id",
                      iter->first);
        } else if (sub_table->mem()->IsEmpty()) {
          // do nothing
          SE_LOG(INFO, "subtable is empty", "index_id", iter->first);
        } else {
          sub_table->Ref();
          mutex_.Unlock();
          flush_memtable(sub_table, FlushOptions());
          mutex_.Lock();
          sub_table->Unref();
        }
      }
    }
  }

  shutting_down_.store(true, std::memory_order_release);
  bg_cv_.SignalAll();
  if (!wait) {
    return;
  }
  // Wait for background work to finish
  while (bg_compaction_scheduled_ || bg_flush_scheduled_ || bg_gc_scheduled_ 
         || bg_dump_scheduled_ || master_thread_running() || bg_ebr_scheduled_) {
    bg_cv_.Wait();
  }
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return flush_memtable(cfh->cfd(), flush_options);
}

int DBImpl::flush_memtable(ColumnFamilyData *sub_table, const FlushOptions &flush_options)
{
  int ret = Status::kOk;
  WriteContext write_context;
  write_context.type_ = MANUAL_FLUSH;

  mutex_.Lock();
  if (IS_NULL(sub_table)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret));
  } else if ((0 == sub_table->imm()->NumNotFlushed()) && (sub_table->mem()->IsEmpty())) {
    // there is no data in the memtables, no need to flush.
  } else if (FAILED(switch_memtable(sub_table, &write_context, false))) {
    SE_LOG(WARN, "fail to switch memtable", K(ret), "index_id", sub_table->GetID());
  } else {
    sub_table->imm()->FlushRequested();
    SchedulePendingFlush(sub_table);
    MaybeScheduleFlushOrCompaction();

    // try to advance the recovery point, ignore errors.
    advance_recovery_point_without_flush();
  }
  mutex_.Unlock();

  if (SUCCED(ret) && flush_options.wait) {
    if (FAILED(WaitForFlushMemTable(sub_table).code())) {
      SE_LOG(WARN, "fail to wait for flush memtable", K(ret));
    }
  }

  return ret;
}

Status DBImpl::WaitForFlushMemTable(ColumnFamilyData* cfd) {
  Status s;
  // Wait until the compaction completes
  InstrumentedMutexLock l(&mutex_);
  while (cfd->imm()->NumNotFlushed() > 0 && bg_error_.ok()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress();
    }
    if (cfd->IsDropped()) {
      // FlushJob cannot flush a dropped CF, if we did not break here
      // we will loop forever since cfd->imm()->NumNotFlushed() will never
      // drop to zero
      return Status::InvalidArgument("Cannot flush a dropped CF");
    }
    bg_cv_.Wait();
  }
  if (!bg_error_.ok()) {
    s = bg_error_;
  }
  return s;
}

Status DBImpl::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  Status s;
  for (auto cf_ptr : column_family_handles) {
    Status status =
        this->SetOptions(cf_ptr, {{"disable_auto_compactions", "false"}});
    if (!status.ok()) {
      s = status;
    }
  }

  return s;
}

Status DBImpl::switch_major_compaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles, bool flag) {
  Status s;
  for (auto cf_ptr : column_family_handles) {
    if (flag) {
      s = this->SetOptions(cf_ptr, {{"bottommost_level", "2"}});
    } else {
      s = this->SetOptions(cf_ptr, {{"bottommost_level", "1"}});
    }
  }

  return s;
}

Status DBImpl::disable_backgroud_merge(const std::vector<ColumnFamilyHandle*>& column_family_handlers) {
  Status s;
  for (auto cf_ptr : column_family_handlers) {
    s = this->SetOptions(cf_ptr, {{"background_disable_merge", "true"}});
  }

  return s;
}

Status DBImpl::enable_backgroud_merge(const std::vector<ColumnFamilyHandle*>& column_family_handlers) {
  Status s;
  for (auto cf_ptr : column_family_handlers) {
    s = this->SetOptions(cf_ptr, {{"background_disable_merge", "false"}});
  }

  return s;
}

int DBImpl::master_schedule_compaction(const CompactionScheduleType type) {
  int ret = 0;
  GlobalContext* global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  if (FAILED(get_all_sub_table(all_sub_table, global_ctx))) {
    SE_LOG(WARN, "get all subtable failed", K(ret));
  } else {
    SubTableMap& all_subtables = all_sub_table->sub_table_map_;
    SubTable *sub_table = nullptr;
    int64_t tasks_num_limit = 5;
    int64_t tasks_cnt = 0;
    for (auto iter = all_subtables.begin();
         Status::kOk == ret && iter != all_subtables.end(); ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (sub_table->IsDropped()) {
        SE_LOG(INFO, "subtable has been dropped", K(iter->first));
      } else if (sub_table->get_task_picker().need_do_task(type)
                 && !sub_table->pending_compaction()
                 && !sub_table->pending_shrink()) {
        mutex_.Lock();
        ++tasks_cnt;
        SchedulePendingCompaction(sub_table, type);
        mutex_.Unlock();
        if (tasks_cnt >= tasks_num_limit) {
          break;
        }
      }
    }
    if (tasks_cnt > 0) {
      mutex_.Lock();
      MaybeScheduleFlushOrCompaction();
      mutex_.Unlock();
    }
    if (tasks_cnt > 0) {
      SE_LOG(INFO, "BG_TASK: schedule compaction idle", K(tasks_cnt));
    }
  }
  int tmp_ret = ret;
  if (nullptr != global_ctx
      && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }
  return ret;
}

int DBImpl::maybe_schedule_dump() {
  int ret = Status::kOk;
  mutex_.AssertHeld();
  if (!can_schedule_bg_work_common()) {
    // can't schedule background job now
  } else {
    while (unscheduled_dumps_ > 0
        && bg_dump_scheduled_ < mutable_db_options_.max_background_dumps) {
      unscheduled_dumps_--;
      bg_dump_scheduled_++;
      env_->Schedule(&DBImpl::bg_work_dump, this, Env::Priority::LOW, this);
    }
  }
  return ret;
}

int DBImpl::schedule_gc()
{
  InstrumentedMutexLock lock_guard(&mutex_);
  return maybe_schedule_gc();
}

int DBImpl::schedule_shrink()
{
  int ret = Status::kOk;
  ShrinkArgs *shrink_args = nullptr;
  int64_t total_max_shrink_extent_count = 0;
  int64_t shrink_extent_count = 0;
  bool expect_shrink_running = false;
  bool auto_shrink_enabled = false;
  ShrinkCondition shrink_condition;

  {
    InstrumentedMutexLock guard(&mutex_);
    if (!can_schedule_bg_work_common()) {
      // can't schedule background job now
    } else {
      auto_shrink_enabled = mutable_db_options_.auto_shrink_enabled;
      total_max_shrink_extent_count = mutable_db_options_.total_max_shrink_extent_count;
      shrink_condition.max_free_extent_percent_ = mutable_db_options_.max_free_extent_percent;
      shrink_condition.shrink_allocate_interval_ = mutable_db_options_.shrink_allocate_interval;
      shrink_condition.max_shrink_extent_count_ = mutable_db_options_.max_shrink_extent_count;
    }
  }

  if (auto_shrink_enabled) {
    SE_LOG(INFO, "beign schedule shrink", "max_free_extent_percent", mutable_db_options_.max_free_extent_percent, "auto", mutable_db_options_.auto_shrink_enabled);
    if (!shrink_running_.compare_exchange_strong(expect_shrink_running, true)) {
      SE_LOG(INFO, "another shrink job is running");
    } else if (IS_NULL(shrink_args = MOD_NEW_OBJECT(ModId::kShrinkJob, ShrinkArgs))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for ShrinkArgs", K(ret));
    } else if (FAILED(ExtentSpaceManager::get_instance().get_shrink_infos(shrink_condition, shrink_args->shrink_infos_))) {
      SE_LOG(WARN, "fail to get shrink infos", K(ret));
    } else if (0 == shrink_args->shrink_infos_.size()) {
      //no need do shrink
      if (nullptr != shrink_args) {
        MOD_DELETE_OBJECT(ShrinkArgs, shrink_args);
      }
      shrink_running_.store(false);
    } else {
      shrink_args->db_ = this;
      shrink_args->auto_shrink_ = true;
      shrink_args->total_max_shrink_extent_count_ = total_max_shrink_extent_count;
      //shrink_args will been delete in bg_work_shrink
      env_->Schedule(&DBImpl::bg_work_shrink, shrink_args, Env::Priority::SHRINK_EXTENT_SPACE, this);
    }

    if (FAILED(ret)) {
      //resource clean
      if (nullptr != shrink_args) {
        MOD_DELETE_OBJECT(ShrinkArgs, shrink_args);
      }
      shrink_running_.store(false);
    }
  } else {
    SE_LOG(DEBUG, "auto shrink is off", K(auto_shrink_enabled));
  }

  if (SUCCED(ret)) {
    if (FAILED(ExtentSpaceManager::get_instance().recycle_dropped_table_space())) {
      SE_LOG(WARN, "fail to recycle dropped table space", K(ret));
    } else {
      SE_LOG(DEBUG, "success to recycle dopped table space");
    }
  }

  return ret;
}

void DBImpl::schedule_ebr() {
  InstrumentedMutexLock lock_guard(&mutex_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background ebr
  } else {
    if (bg_ebr_scheduled_ == 0) {
      bg_ebr_scheduled_ = 1;
      env_->Schedule(&DBImpl::bg_work_ebr, this, Env::Priority::HIGH, this);
    }
  }
}

int DBImpl::maybe_schedule_gc()
{
  mutex_.AssertHeld();
  int ret = Status::kOk;
  if (!can_schedule_bg_work_common()) {
    // can't schedule background now
  } else {
    if (unscheduled_gc_ > 0 && 0 == bg_gc_scheduled_) {
      //only one gc job running at most
      ++bg_gc_scheduled_;
      env_->Schedule(&DBImpl::bg_work_gc, this, Env::Priority::LOW, this);
    }
    SE_LOG(INFO, "schedule gc job", K_(unscheduled_gc), K_(bg_gc_scheduled), K_(num_running_gc));
  }
  return ret;
}


/**check if can schedule backgroud job(like flush, compaction, dump, gc, shrink) in
 * some common case:
 * case 1: opened_successfully_ is false, DBImpl open failed.
 * case 2: bg_work_paused_ greater than 0, stop schedule background job initiative
 * through smartengine_pause_background_work or internal logical.
 * case 3: shutdown is true, receive shutdown command.
 * case 4: bg_error_ is not Status::kOk, some background job has failed.
 * @return false if satisfy any upper case, and can't schedule background job.
 * @Note need protect by db_mutex_*/
bool DBImpl::can_schedule_bg_work_common()
{
  mutex_.AssertHeld();
  bool bret = true;

  if (!opened_successfully_ || bg_work_paused_ > 0
      || shutting_down_.load(std::memory_order_acquire)
      || !bg_error_.ok()) {
    bret = false;
  }

  return bret;
}

void DBImpl::MaybeScheduleFlushOrCompaction() {
  mutex_.AssertHeld();

  if (!can_schedule_bg_work_common()) {
    // can't schedule background job now
    return;
  }

  SE_LOG(INFO, "CK_INFO: flush info", K(unscheduled_flushes_), K(bg_flush_scheduled_));
  while (unscheduled_flushes_ > 0 &&
         bg_flush_scheduled_ < immutable_db_options_.max_background_flushes) {
    unscheduled_flushes_--;
    bg_flush_scheduled_++;
    env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::HIGH, this);
  }

  auto bg_compactions_allowed = BGCompactionsAllowed();

  // special case -- if max_background_flushes == 0, then schedule flush on a
  // compaction thread
  if (immutable_db_options_.max_background_flushes == 0) {
    while (unscheduled_flushes_ > 0 &&
           bg_flush_scheduled_ + bg_compaction_scheduled_ < bg_compactions_allowed) {
      unscheduled_flushes_--;
      bg_flush_scheduled_++;
      env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::LOW, this);
    }
  }

  if (bg_compaction_paused_ > 0) {
    // we paused the background compaction
    return;
  }

  while (bg_compaction_scheduled_ < bg_compactions_allowed &&
         unscheduled_compactions_ > 0) {
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    bg_compaction_scheduled_++;
    unscheduled_compactions_--;
    env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::LOW, this,
                   &DBImpl::UnscheduleCallback);
  }
}

int DBImpl::bg_dumps_allowed() const {
  return 0;/* mutable_db_options_.max_background_dumps*/;
}

int DBImpl::BGCompactionsAllowed() const {
  mutex_.AssertHeld();
  return mutable_db_options_.max_background_compactions;
}

size_t DBImpl::compaction_job_size() {
  return compaction_queue_[CompactionPriority::HIGH].size() +
    compaction_queue_[CompactionPriority::LOW].size();
}

DBImpl::CFCompactionJob* DBImpl::pop_front_compaction_job() {
  CFCompactionJob* cf_job = nullptr;
  if (!compaction_queue_[CompactionPriority::HIGH].empty()) {
    cf_job = compaction_queue_[CompactionPriority::HIGH].front();
    compaction_queue_[CompactionPriority::HIGH].pop_front();
  } else if (!compaction_queue_[CompactionPriority::LOW].empty()) {
    cf_job = compaction_queue_[CompactionPriority::LOW].front();
    compaction_queue_[CompactionPriority::LOW].pop_front();
  }
  return cf_job;
}

void DBImpl::push_back_compaction_job(CFCompactionJob* cf_job) {
  auto priority = CompactionPriority::LOW;
  if (nullptr != cf_job && nullptr != cf_job->cfd_) {
    priority = cf_job->cfd_->compaction_priority();
  }
  compaction_queue_[priority].push_back(cf_job);
}

bool DBImpl::need_snapshot_check(const TaskType task_type,
                                 const Snapshot* meta_snapshot) {
  bool need_check = false;
  std::vector<SequenceNumber> existing_snapshots;
  SequenceNumber earliest_write_conflict_snapshot;
  existing_snapshots = GetAll(&earliest_write_conflict_snapshot);
  SequenceNumber oldest_snapshot = existing_snapshots.size() > 0
      ? existing_snapshots.at(0)
      : kMaxSequenceNumber;
  if (earliest_write_conflict_snapshot < oldest_snapshot) {
    oldest_snapshot = earliest_write_conflict_snapshot;
  }
  assert(meta_snapshot);
  ExtentLayerVersion *l0_version = meta_snapshot->get_extent_layer_version(0);
  ExtentLayerVersion *l1_version = meta_snapshot->get_extent_layer_version(1);
  ExtentLayerVersion *l2_version = meta_snapshot->get_extent_layer_version(2);
  if (touch_level0_task(task_type)) {
    assert(l0_version);
    SequenceNumber max_l0_seq = l0_version->get_largest_sequence_number();
    if (max_l0_seq > oldest_snapshot) {
      need_check = true;
    }
  }
  if (!need_check && touch_level1_task(task_type)) {
    assert(l1_version);
    SequenceNumber max_l1_seq = l1_version->get_largest_sequence_number();
    if (max_l1_seq > oldest_snapshot) {
      need_check = true;
    }
  }
  if (!need_check && touch_level2_task(task_type)) {
    assert(l2_version);
    SequenceNumber max_l2_seq = l2_version->get_largest_sequence_number();
    if (max_l2_seq > oldest_snapshot) {
      need_check = true;
    }
  }
  return need_check;
}

void DBImpl::add_compaction_job(ColumnFamilyData* sub_table, CompactionTasksPicker::TaskInfo task_info) {
  mutex_.AssertHeld();
  assert(sub_table);
  assert(!sub_table->pending_compaction());
  assert(is_valid_task_type(task_info.task_type_));
  // check if compaction can do without snapshot, must get the meta_snapshot first
  Snapshot *meta_snapshot = sub_table->get_meta_snapshot(&mutex_);
  bool need_check = need_snapshot_check(task_info.task_type_, meta_snapshot);
  auto priority = sub_table->compaction_priority();
  sub_table->Ref();
  sub_table->set_pending_compaction(true);
  CFCompactionJob *cfcp_job = MOD_NEW_OBJECT(
      ModId::kCompaction, CFCompactionJob, sub_table, meta_snapshot, nullptr, task_info, need_check);
  push_back_compaction_job(cfcp_job);
  SE_LOG(INFO, "COMPACTION_CEHCK: will do compaction task", K(task_info), K(need_check));
}

void DBImpl::remove_dump_job(DBImpl::STDumpJob *&dump_job) {
  mutex_.AssertHeld();
  if (nullptr != dump_job) {
    if (IS_NULL(dump_job->sub_table_)) {
      SE_LOG(ERROR, "sutable is null");
    } else if (dump_job->sub_table_->Unref()) {
      SE_LOG(DEBUG, "flush  delete sub_table", K(dump_job->sub_table_->GetID()));
      MOD_DELETE_OBJECT(ColumnFamilyData, dump_job->sub_table_);
      dump_job->sub_table_ = nullptr;
    } else if (!dump_job->sub_table_->IsDropped()) {
      if (mutable_db_options_.dump_memtable_limit_size > 0 && !allow_2pc()) {
        versions_->GetColumnFamilySet()->insert_into_dump_list(dump_job->sub_table_);
      }
    }
    MOD_DELETE_OBJECT(STDumpJob, dump_job);
    dump_job = nullptr;
  }
}

void DBImpl::remove_flush_job(DBImpl::STFlushJob *&flush_job, bool schedule) {
  mutex_.AssertHeld();
  if (nullptr != flush_job) {
    if (nullptr != flush_job->meta_snapshot_) {
      Snapshot *s = flush_job->meta_snapshot_;
      flush_job->meta_snapshot_ = nullptr;
      flush_job->sub_table_->release_meta_snapshot(s);
    }
    assert(flush_job->sub_table_);
    if (IS_NULL(flush_job->sub_table_)) {
      SE_LOG(ERROR, "sutable is null");
    } else {
      // cancel the flag can't do dump task
      if (flush_job->sub_table_->task_canceled(TaskType::DUMP_TASK)) {
        flush_job->sub_table_->set_cancel_task_type((1LL << TaskType::DUMP_TASK), true);
      }
      if (flush_job->sub_table_->Unref()) {
        SE_LOG(DEBUG, "flush  delete sub_table", K(flush_job->sub_table_->GetID()));
        MOD_DELETE_OBJECT(ColumnFamilyData, flush_job->sub_table_);
      } else if (!flush_job->sub_table_->IsDropped()) {
        if (mutable_db_options_.dump_memtable_limit_size > 0 && !allow_2pc()) {
          versions_->GetColumnFamilySet()->insert_into_dump_list(flush_job->sub_table_);
        }
        if (schedule) {
          SchedulePendingFlush(flush_job->sub_table_); // schedule flush
        }
      }
    }
    MOD_DELETE_OBJECT(STFlushJob, flush_job);
    flush_job = nullptr;
  }
}

void DBImpl::remove_compaction_job(CFCompactionJob*& cf_job, bool schedule) {
  mutex_.AssertHeld();
  assert(nullptr != cf_job->cfd_);
  FREE_OBJECT(CompactionJob, cf_job->compaction_alloc_, cf_job->job_);
  if (nullptr != cf_job->meta_snapshot_) {
    Snapshot *s = cf_job->meta_snapshot_;
    cf_job->meta_snapshot_ = nullptr;
    cf_job->cfd_->release_meta_snapshot(s, &mutex_);
  }

#ifndef NDEBUG
  if (std::find(compaction_queue_[0].begin(), compaction_queue_[0].end(),
        cf_job) != compaction_queue_[0].end() ||
      std::find(compaction_queue_[1].begin(), compaction_queue_[1].end(),
        cf_job) != compaction_queue_[1].end()) {
    assert(false);
  }
#endif

  // Make sure clear pending flag before SchedulePendingCompaction
  cf_job->cfd_->set_pending_compaction(false);
  if (cf_job->cfd_->Unref()) {
    MOD_DELETE_OBJECT(ColumnFamilyData, cf_job->cfd_);
  } else if (!cf_job->cfd_->IsDropped()) {
      // Since this cfd would be Reference by TotalWriteBufferFul,
      // only Unref() after trimmed, we check dropped here.
    if (schedule) {
      SchedulePendingCompaction(cf_job->cfd_);
    }
  }
  MOD_DELETE_OBJECT(CFCompactionJob, cf_job);
}

void DBImpl::AddToFlushQueue(ColumnFamilyData* sub_table, TaskType type) {
  assert(!sub_table->pending_flush());
  Snapshot *meta_snapshot = sub_table->get_meta_snapshot(&mutex_);
  // here no memtables' max_seq, so just do the snapshot check
  bool need_check = true;
  sub_table->Ref();
  STFlushJob *flush_job = MOD_NEW_OBJECT(
      ModId::kFlush, STFlushJob, sub_table, meta_snapshot, type, need_check);
  flush_queue_.push_back(flush_job);
  SE_LOG(INFO, "flush task count", K(flush_queue_.size()), K(sub_table->GetID()));
  sub_table->set_pending_flush(true);
  if (TaskType::FLUSH_LEVEL1_TASK == type ) {
    sub_table->set_pending_compaction(true); // set pending_compction flag
  }
}

DBImpl::STDumpJob* DBImpl::pop_front_dump_job() {
  assert(!dump_queue_.empty());
  STDumpJob *dump_job = dump_queue_.front();
  dump_queue_.pop_front();
  if (IS_NULL(dump_job)
      || IS_NULL(dump_job->sub_table_)) {
    SE_LOG(ERROR, "dump_job or subtable is null", KP(dump_job));
  } else if (!dump_job->sub_table_->pending_dump()) {
    SE_LOG(ERROR, "pending flush flag is error", K(dump_job->sub_table_->pending_dump()));
  }
  SE_LOG(INFO, "pop front dump job", KP(dump_job));
  return dump_job;
}

int DBImpl::push_back_gc_job(GCJob *gc_job)
{
  int ret = Status::kOk;
  if (IS_NULL(gc_job) || !gc_job->valid()) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(gc_job), K(*gc_job));
  } else {
    gc_queue_.push_back(gc_job);
  }
  return ret;
}
DBImpl::GCJob *DBImpl::pop_front_gc_job()
{
  assert(!gc_queue_.empty());
  GCJob *gc_job = gc_queue_.front();
  if (gc_job->valid()) {
    gc_queue_.pop_front();
  } else {
    SE_LOG(WARN, "gc job is invalid", K(gc_job));
    gc_job = nullptr;
  }
  return gc_job;
}

int DBImpl::remove_gc_job(DBImpl::GCJob *&gc_job)
{
  mutex_.AssertHeld();
  int ret = Status::kOk;
  if (nullptr != gc_job) {
    if (IS_NULL(gc_job->sub_table_)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpect error, subtable in gc job must not nullptr", K(ret));
    } else if (gc_job->sub_table_->Unref()) {
      MOD_DELETE_OBJECT(ColumnFamilyData, gc_job->sub_table_);
    }
    MOD_DELETE_OBJECT(GCJob, gc_job);
  }
  return ret;
}
DBImpl::STFlushJob* DBImpl::PopFirstFromFlushQueue() {
  assert(!flush_queue_.empty());
  STFlushJob *flush_job = flush_queue_.front();
  flush_queue_.pop_front();
  if (IS_NULL(flush_job)
      || IS_NULL(flush_job->get_subtable())) {
    SE_LOG(ERROR, "flush_job or subtable is null", KP(flush_job));
  } else if (!flush_job->get_subtable()->pending_flush()){
    SE_LOG(WARN, "pending flush flag is error", K((int)flush_job->get_task_type()));
  }
  //sub_table->set_pending_flush(false);
  return flush_job;
}

void DBImpl::SchedulePendingFlush(ColumnFamilyData* cfd) {
  TaskType type = TaskType::INVALID_TYPE_TASK;
  if (!cfd->pending_flush()
      && cfd->imm()->IsFlushPending()
      && cfd->need_flush(type)
      && !cfd->pending_dump()
      && !cfd->pending_shrink()) {
//    if (cfd->pending_dump()) {
//      // cancel dump task, need reset after flush
//      cfd->set_cancel_task_type(1LL<<DUMP_TASK);
//    }
    AddToFlushQueue(cfd, type);
    ++unscheduled_flushes_;
  }
}

void DBImpl::SchedulePendingCompaction(ColumnFamilyData* cfd, const CompactionScheduleType skz_type) {
  TaskType type = TaskType::INVALID_TYPE_TASK;
  CompactionTasksPicker::TaskInfo task_info;
  if (!cfd->pending_compaction()
      && !cfd->pending_shrink()
      && cfd->need_compaction_v1(task_info, skz_type)) {
    add_compaction_job(cfd, task_info);
    ++unscheduled_compactions_;
  }
}

void DBImpl::SchedulePendingPurge(std::string fname, FileType type,
                                  uint64_t number, uint32_t path_id,
                                  int job_id) {
  mutex_.AssertHeld();
  PurgeFileInfo file_info(fname, type, number, path_id, job_id);
  purge_queue_.push_back(std::move(file_info));
}

void DBImpl::schedule_pending_gc(ColumnFamilyData *sub_table)
{
  mutex_.AssertHeld();
  int ret = Status::kOk;
  if (sub_table->IsDropped() && sub_table->is_bg_stopped()) {
    GCJob *gc_job = nullptr;
    int64_t dropped_time = 0;
    env_->GetCurrentTime(&dropped_time);
    if (IS_NULL(gc_job = MOD_NEW_OBJECT(ModId::kDefaultMod, GCJob, sub_table, env_, dropped_time))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for GCJob", K(ret));
    } else if (FAILED(push_back_gc_job(gc_job))) {
      SE_LOG(WARN, "fail to push back gc job", K(ret));
    } else {
      ++unscheduled_gc_;
      maybe_schedule_gc();
      SE_LOG(INFO, "success to schedule pending gc");
    }
  }
}

int DBImpl::shrink_extent_spaces(ShrinkArgs &shrink_args)
{
  int ret = Status::kOk;

#ifndef NDEBUG
  TEST_SYNC_POINT("DBImpl::shrink_extent_spaces_schedule_hang");
#endif
  if (UNLIKELY(!shrink_args.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(shrink_args));
  } else {
    int64_t total_max_shrink_extent_count = shrink_args.total_max_shrink_extent_count_;
    std::sort(shrink_args.shrink_infos_.begin(), shrink_args.shrink_infos_.end(),
        [=](ShrinkInfo &left, ShrinkInfo &right)
        {return left.shrink_extent_count_ > right.shrink_extent_count_; });
    for (uint32_t i = 0; SUCCED(ret) && i < shrink_args.shrink_infos_.size(); ++i) {
      const ShrinkInfo &shrink_info = shrink_args.shrink_infos_.at(i);
      if (shrink_args.auto_shrink_ && 0 == shrink_info.table_space_id_) {
        //not auto shrink table_space_0, because table space_0 may contain many subtable
      } else if (FAILED(shrink_args.db_->shrink_extent_space(shrink_info))) {
        SE_LOG(WARN, "fail to shrink extent space", K(ret), K(shrink_info), K(shrink_args));
      } else {
        SE_LOG(INFO, "success to shrink extent space", K(shrink_info), K(shrink_args));
        total_max_shrink_extent_count -= shrink_info.shrink_extent_count_;
        if (total_max_shrink_extent_count <= 0) {
          SE_LOG(INFO, "reach shrink limit, stop shrink");
          break;
        }
      }
    }
  }

  shrink_args.db_->shrink_running_.store(false);
#ifndef NDEBUG
  assert(Status::kOk == ret);
#endif

  return ret;
}

int DBImpl::shrink_extent_space(const ShrinkInfo &shrink_info)
{
  int ret = Status::kOk;
  storage::ShrinkJob *shrink_job = nullptr;

  if (IS_NULL(shrink_job = MOD_NEW_OBJECT(ModId::kShrinkJob, ShrinkJob))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for ShrinkJob", K(ret));
  } else if (FAILED(shrink_job->init(&mutex_,
                                     versions_->get_global_ctx(),
                                     shrink_info))) {
    SE_LOG(WARN, "fail to init shrink job", K(ret));
  } else if (FAILED(shrink_job->run())) {
        SE_LOG(WARN, "fail  to run shrink job", K(ret));
  } else {
    SE_LOG(INFO, "success to run shrink job", K(shrink_info));
  }

  //resource clean
  if (nullptr != shrink_job) {
    MOD_DELETE_OBJECT(ShrinkJob, shrink_job);
  }
  return ret;
}

void DBImpl::bg_work_dump(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  reinterpret_cast<DBImpl*>(db)->background_call_dump();
}

void DBImpl::bg_work_gc(void *db)
{
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  TEST_SYNC_POINT("DBImpl::BGWorkGC");
  (reinterpret_cast<DBImpl *>(db))->background_call_gc();
  TEST_SYNC_POINT("DBImpl::BGWorkGC::done");
}

void DBImpl::BGWorkFlush(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush");
  reinterpret_cast<DBImpl*>(db)->BackgroundCallFlush();
  TEST_SYNC_POINT("DBImpl::BGWorkFlush:done");
}

void DBImpl::BGWorkCompaction(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  TEST_SYNC_POINT("DBImpl::BGWorkCompaction");
  reinterpret_cast<DBImpl*>(ca.db)->BackgroundCallCompaction();
}

void DBImpl::BGWorkPurge(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:start");
  reinterpret_cast<DBImpl*>(db)->BackgroundCallPurge();
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:end");
}

void DBImpl::UnscheduleCallback(void* arg) {
  delete reinterpret_cast<CompactionArg*>(arg);
  TEST_SYNC_POINT("DBImpl::UnscheduleCallback");
}

void DBImpl::bg_work_shrink(void *arg)
{
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::SHRINK_EXTENT_SPACE);
  TEST_SYNC_POINT("DBImpl::bg_work_shrink");
  ShrinkArgs *shrink_args = reinterpret_cast<ShrinkArgs *>(arg);
  shrink_args->db_->shrink_extent_spaces(*shrink_args);
  TEST_SYNC_POINT("DBImpl::bg_work_shrink:done");
  MOD_DELETE_OBJECT(ShrinkArgs, shrink_args);
}

void DBImpl::bg_work_ebr(void *db)
{
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::bg_work_ebr");
  reinterpret_cast<DBImpl *>(db)->background_call_ebr();
  TEST_SYNC_POINT("DBImpl::bg_work_ebr:done");
}

int DBImpl::background_dump(bool* madeProgress, JobContext* job_context) {
  int ret = Status::kOk;
  Status status = bg_error_;
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    ret = Status::kShutdownInProgress;
    return ret;
  }
  STDumpJob *dump_job = nullptr;
  while(!dump_queue_.empty() && SUCCED(ret)) {
    STDumpJob *front_dump_job = pop_front_dump_job();
    ColumnFamilyData *sub_table = nullptr;
    if (IS_NULL(front_dump_job) || IS_NULL(sub_table = front_dump_job->sub_table_)) {
      ret =  Status::kErrorUnexpected;
      SE_LOG(ERROR, "dump job or subtable is null", KP(front_dump_job));
    } else if (sub_table->IsDropped() || sub_table->is_bg_stopped()) {
      // can't dump this CF, try next one
      remove_dump_job(front_dump_job);
      sub_table->set_pending_dump(false);
      if (sub_table->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      }
      continue;
      remove_dump_job(front_dump_job);
      SE_LOG(INFO, "dump task is canceled", K(sub_table->get_cancel_task_type()), K(sub_table->GetID()));
      continue;
    } else {
      // found a dump job!
      dump_job = front_dump_job;
      break;
    }
  }

  if (SUCCED(ret) && nullptr != dump_job && nullptr != job_context) {
    ColumnFamilyData *sub_table = dump_job->get_subtable();
    job_context->task_type_ = TaskType::DUMP_TASK;
    job_context->output_level_ = 0;
    if (FAILED(dump_memtable_to_outputfile(*dump_job, madeProgress, *job_context))) {
      SE_LOG(WARN, "failed to dump memtable to outputfile", K(ret));
    } else if (FAILED(sub_table->set_compaction_check_info(&mutex_))) {
      SE_LOG(WARN, "failed to set compaction check info", K(ret), K(sub_table->GetID()));
    }
    remove_dump_job(dump_job);
  }
  return ret;
}

int DBImpl::background_gc()
{
  mutex_.AssertHeld();
  int ret = Status::kOk;
  Status status = bg_error_;
  GCJob *gc_job = nullptr;
  int64_t index_id = 0;

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    ret = Status::kShutdownInProgress;
    SE_LOG(INFO, "smartengine is shutdown in progress", K(ret));
  } else if (gc_queue_.empty()) {
    //empty queue, do nothing
  } else if (IS_NULL(gc_job = pop_front_gc_job())) {
    ret= Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpect error, gc job should not been nullptr", K(ret));
  } else {
    if (!gc_job->can_gc()) {
      //can't gc now, push back queue
      SE_LOG(INFO, "the gc jon can't exec now", "index_id", gc_job->sub_table_->GetID());
      if (FAILED(push_back_gc_job(gc_job))) {
        SE_LOG(WARN, "fail to push back gc job", K(ret));
      }
    } else {
      mutex_.Unlock();
      index_id = gc_job->sub_table_->GetID();
      if (FAILED(gc_job->sub_table_->release_resource(false /*for_recovery*/))) {
        SE_LOG(WARN, "fail to release resource", K(ret));
      } else if (FAILED(ExtentSpaceManager::get_instance().unregister_subtable(
          gc_job->sub_table_->get_table_space_id(), index_id))) {
        SE_LOG(WARN, "fail to unregister subtable", K(ret), K(index_id),
            "table_space_id", gc_job->sub_table_->get_table_space_id());
      } else {
        SE_LOG(INFO, "success to recycle dropped subtable", K(index_id),
            "table_space_id", gc_job->sub_table_->get_table_space_id());
      }
      mutex_.Lock();
      remove_gc_job(gc_job);
      --unscheduled_gc_;
    }
  }

  return ret;
}
Status DBImpl::BackgroundFlush(bool* made_progress, JobContext* job_context)
{
  mutex_.AssertHeld();

  Status status = bg_error_;
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::ShutdownInProgress();
  }

  if (!status.ok()) {
    return status;
  }

  STFlushJob* flush_job = nullptr;
  while (!flush_queue_.empty()) {
    // This sub_table is already referenced
    STFlushJob *first_flush_job = PopFirstFromFlushQueue();

    if (IS_NULL(first_flush_job) || IS_NULL(first_flush_job->get_subtable())) {
      SE_LOG(ERROR, "first flush job or subtable id is null",
          KP(first_flush_job));
    } else {
      ColumnFamilyData *sub_table = first_flush_job->get_subtable();
      if (sub_table->IsDropped() || sub_table->is_bg_stopped()
          || !sub_table->imm()->IsFlushPending()) {
        // can't flush this CF, try next one
        sub_table->set_pending_flush(false);
        if (FLUSH_LEVEL1_TASK == first_flush_job->get_task_type()) {
          sub_table->set_pending_compaction(false);
        }
        if (sub_table->Unref()) {
          MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
        }
        continue;
      }
      // found a flush!
      flush_job = first_flush_job;
      break;
    }
  }

  if (nullptr != flush_job  && nullptr != job_context) {
    ColumnFamilyData *sub_table = flush_job->get_subtable();
    status = FlushMemTableToOutputFile(*flush_job, made_progress, *job_context);
    int ret = 0;
    if (FAILED(sub_table->set_compaction_check_info(&mutex_))) {
      SE_LOG(WARN, "failed to set compaction check info", K(ret), K(sub_table->GetID()));
      status = Status(ret);
    }
    remove_flush_job(flush_job);
  }
  return status;
}

void DBImpl::background_call_dump() {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  assert(bg_dump_scheduled_);
  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:dump");
  {
    InstrumentedMutexLock l(&mutex_);
    num_running_dumps_++; // nouse
    int ret  = background_dump(&made_progress, &job_context);
    if (FAILED(ret) && Status::kShutdownInProgress != ret) {
      // Wait a little bit before retrying background dump in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed dumps for the duration of
      // the problem.
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      SE_LOG(ERROR, "Waiting after background dump error", K(ret));
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    SE_LOG(INFO, "BEFORE FindObsoleteFiles");
    // If dump failed, we want to delete all temporary files that we might have
    // created. Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, Status::kOk != ret && Status::kShutdownInProgress != ret);
    SE_LOG(INFO, "AFTER FindObsoleteFiles");
    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToDelete()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_flush_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
      }
      mutex_.Lock();
    }

    assert(bg_dump_scheduled_ > 0);
    bg_dump_scheduled_--;
    // See if there's more work to be done
    maybe_schedule_dump();
    bg_cv_.SignalAll();
    job_context.Clean();
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

void DBImpl::background_call_gc()
{
  int ret = Status::kOk;
  assert(bg_gc_scheduled_);
  TEST_SYNC_POINT("DBImpl::background_call_gc");
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    if (0 == num_running_gc_) {
      int prev_unschedule_gc = unscheduled_gc_;
      ++num_running_gc_;
      ret = background_gc();
      if (FAILED(ret) && Status::kShutdownInProgress != ret) {
				// Wait a little bit before retrying background gc in
      	// case this is an environmental problem and we do not want to
      	// chew up resources for failed gcs for the duration of
      	// the problem.
      	bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      	mutex_.Unlock();
      	SE_LOG(ERROR, "Waiting after background gc error", K(ret));
      	env_->SleepForMicroseconds(1000000);
      	mutex_.Lock();
      }
      assert(bg_gc_scheduled_ > 0);
      --num_running_gc_;
      --bg_gc_scheduled_;
      //condition 1: unscheduled gc job more than zero
      //condition 2: prev gc job execute truly
      if (unscheduled_gc_ > 0 && prev_unschedule_gc > unscheduled_gc_) {
        maybe_schedule_gc();
      }
      if (0 == bg_gc_scheduled_) {
        // wake up CancelAllBackgroundWork called from ~DBImpl
        bg_cv_.SignalAll();
      }
    } else {
      //has other gc job running, do nothing
      SE_LOG(INFO, "has other gc job running", K_(num_running_gc), K_(bg_gc_scheduled));
    }
  }
}

void DBImpl::background_call_ebr() {
  EBR_MAYBE_RECLAIM;
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    bg_ebr_scheduled_ = 0;
    if (shutting_down_.load(std::memory_order_acquire)) {
      bg_cv_.SignalAll();
    }
  }
}

void DBImpl::BackgroundCallFlush() {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  assert(bg_flush_scheduled_);

  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:start");

  InstrumentedMutexLock l(&mutex_);
  num_running_flushes_++;

  Status s = BackgroundFlush(&made_progress, &job_context);
  if (!s.ok() && !s.IsShutdownInProgress()) {
    // Wait a little bit before retrying background flush in
    // case this is an environmental problem and we do not want to
    // chew up resources for failed flushes for the duration of
    // the problem.
    uint64_t error_cnt =
        default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
    bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
    mutex_.Unlock();
    __SE_LOG(ERROR, "Waiting after background flush error: %s "
                         "Accumulated background error counts: %" PRIu64,
                  s.ToString().c_str(), error_cnt);
    env_->SleepForMicroseconds(1000000);
    mutex_.Lock();
  }

  SE_LOG(INFO, "BEFORE FindObsoleteFiles");
  // If flush failed, we want to delete all temporary files that we might have
  // created. Thus, we force full scan in FindObsoleteFiles()
  FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress());
  SE_LOG(INFO, "AFTER FindObsoleteFiles");
  // delete unnecessary files if any, this is done outside the mutex
  if (job_context.HaveSomethingToDelete()) {
    mutex_.Unlock();
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
    mutex_.Lock();
  }

  assert(num_running_flushes_ > 0);
  num_running_flushes_--;
  bg_flush_scheduled_--;
  // See if there's more work to be done
  MaybeScheduleFlushOrCompaction();
  bg_cv_.SignalAll();
  // IMPORTANT: there should be no code after calling SignalAll. This call may
  // signal the DB destructor that it's OK to proceed with destruction. In
  // that case, all DB variables will be dealloacated and referencing them
  // will cause trouble.
  
}

void DBImpl::BackgroundCallCompaction()
{
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  TEST_SYNC_POINT("BackgroundCallCompaction:0");
  {
    InstrumentedMutexLock l(&mutex_);

    num_running_compactions_++;

    assert(bg_compaction_scheduled_);
    Status s;
    s = BackgroundCompaction(&made_progress, &job_context);
    TEST_SYNC_POINT("BackgroundCallCompaction:1");
    if (!s.ok() && !s.IsShutdownInProgress()) {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      __SE_LOG(ERROR, "Waiting after background compaction error: %s, "
                           "Accumulated background error counts: %" PRIu64,
                    s.ToString().c_str(), error_cnt);
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    // If compaction failed, we want to delete all temporary files that we might
    // have created (they might not be all recorded in job_context in case of a
    // failure). Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress());

    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToDelete()) {
      mutex_.Unlock();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
      }
      job_context.Clean();
      mutex_.Lock();
    }

    assert(num_running_compactions_ > 0);
    num_running_compactions_--;
    bg_compaction_scheduled_--;

    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();

    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();
    if (made_progress || bg_compaction_scheduled_ == 0) {
      // signal if
      // * made_progress -- need to wakeup DelayWrite
      // * bg_compaction_scheduled_ == 0 -- need to wakeup ~DBImpl
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

Status DBImpl::build_compaction_job(ColumnFamilyData *cfd,
                                    Snapshot *snapshot,
                                    JobContext *job_context,
                                    storage::CompactionJob *&job,
                                    CFCompactionJob &cf_job) {
  int ret = Status::kOk;
  job = ALLOC_OBJECT(CompactionJob, cf_job.compaction_alloc_, cf_job.compaction_alloc_);
  if (nullptr == job) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "job is null", K(ret));
    return Status(ret);
  }
  mutex_.AssertHeld();

  storage::CompactionContext context;
  context.shutting_down_ = &shutting_down_;
  context.bg_stopped_ = cfd->bg_stopped();
  context.cancel_type_ = cfd->cancel_task_type();
  context.cf_options_ = cfd->ioptions();
  context.mutable_cf_options_ = cfd->GetLatestMutableCFOptions();
  context.env_options_ = cfd->soptions();
  context.data_comparator_ = cfd->ioptions()->user_comparator;
  context.internal_comparator_ = &cfd->internal_comparator();
  context.table_space_id_ = cfd->get_table_space_id();
  context.existing_snapshots_ = GetAll(&context.earliest_write_conflict_snapshot_);
  context.task_type_ = cf_job.task_info_.task_type_;
  context.enable_thread_tracking_ = immutable_db_options_.enable_thread_tracking;
  context.need_check_snapshot_ = cf_job.need_check_snapshot_;
  storage::ColumnFamilyDesc cf_desc(cfd->GetID(), cfd->get_table_schema());
  const CompactionTasksPicker &task_picker = cfd->get_task_picker();
  CompactionTasksPicker::TaskInfo &task_info = cf_job.task_info_;
  if (FAILED(job->init(context, cf_desc, snapshot))) {
    SE_LOG(WARN,  "init compaction job failed.", K(ret));
  } else if (is_major_self_task(context.task_type_)) {
    // Case 1, MajorSelf or AutoMajorSelf or DeleteMajorSelf
    bool is_auto = (TaskType::MAJOR_AUTO_SELF_COMPACTION_TASK == task_info.task_type_);
    bool is_delete = (TaskType::DELETE_MAJOR_SELF_TASK == task_info.task_type_);
    if (FAILED(job->prepare_major_self_task(task_picker.get_major_self_limit(), is_auto, is_delete))) {
      SE_LOG(WARN, "prepare major self task failed", K(ret));
    }
  } else if (is_major_task_with_L1(task_info.task_type_)) {
    // Case 2, Major or MajorDelete
    // todo unlock mutex
    size_t delete_percent = cfd->GetLatestMutableCFOptions()->compaction_delete_percent;
    if (FAILED(job->prepare_major_task(task_info.l1_pick_pos_,
                                       task_picker.get_major_merge_limit(),
                                       task_info.need_split_,
                                       delete_percent))) {
      SE_LOG(WARN, "create compaction task failed",K(ret), K(task_info), K(delete_percent));
    } else if (TaskType::SPLIT_TASK == job->get_task_type()) {
      Status status = run_one_compaction_task(cfd, job_context, job);
      if (FAILED(status.code())) {
        SE_LOG(WARN, "run one split task failed", K(ret));
      } else { // update snapshot and prepare again
        if (nullptr != snapshot) {
          cfd->release_meta_snapshot(snapshot, &mutex_);
        }
        Snapshot *new_snapshot = cfd->get_meta_snapshot(&mutex_);
        cf_job.update_snapshot(new_snapshot);
        job->update_snapshot(new_snapshot);
        if (FAILED(job->prepare_major_task(task_info.l1_pick_pos_,
                                           task_picker.get_major_merge_limit(),
                                           true,
                                           delete_percent))) {
          SE_LOG(WARN, "failed to prepare major task again", K(ret),
              K(task_info.l1_pick_pos_), K(task_picker.get_major_merge_limit()), K(delete_percent));
        } else {
          SE_LOG(INFO, "complete split task", K(cfd->GetID()));
        }
      }
    }
  } else if (TaskType::MINOR_AUTO_SELF_TASK == task_info.task_type_) {
    if (FAILED(job->prepare_minor_self_task())) {
      SE_LOG(WARN, "prepare minor self task failed", K(ret));
    }
  } else { // Need minor or Intra-L0 compaction.only build one task for each Intra-L0 Job
    if (FAILED(job->prepare_minor_task(task_picker.get_minor_merge_limit()))) {
      SE_LOG(WARN, "create minor compaction task failed", K(ret));
    }
    SE_LOG(INFO, "build priority output_level job",
        K(cfd->GetID()),
        K((int)cfd->compaction_priority()),
        K(context.output_level_),
        K(job->get_task_size()));
  }
  if (Status::kOk != ret && nullptr != job) {
    FREE_OBJECT(CompactionJob, cf_job.compaction_alloc_, job);
  }
  return Status(ret);
}

Status DBImpl::run_one_compaction_task(ColumnFamilyData* sub_table,
                                       JobContext* job_context,
                                       storage::CompactionJob* job) {
  mutex_.AssertHeld();

  int ret = 0;
  storage::Compaction* compaction = job->get_next_task();
  // Note: compaction task maybe have been picked up by other compaction
  // threads
  if (nullptr != compaction) {
    // TODO compaction may be only 1 Event type instead of many
    SeEvent event = storage::INVALID_EVENT;
    if (is_batch_install_task(job->get_task_type())) {
      event = storage::SeEvent::MINOR_COMPACTION;
    } else if (is_major_task(job->get_task_type())) {
      event = storage::SeEvent::MAJOR_COMPACTION;
    } else if (job->get_task_type() == TaskType::SPLIT_TASK) {
      event = storage::SeEvent::SPLIT_COMPACTION;
    } else {
      SE_LOG(WARN, "invalid task_type", K(sub_table->GetID()), K((int)job->get_task_type()));
    }

    // begin tranx for compaction run
    if (storage::INVALID_EVENT != event) {
      // unlock meta allows other compaction threads run before run compaction
      mutex_.Unlock();
      if (FAILED(StorageLogger::get_instance().begin(event))) {
        SE_LOG(WARN, "fail to begin minor compaction", K(ret));
      } else {
        AutoThreadOperationStageUpdater stage_updater(ThreadStatus::STAGE_COMPACTION_RUN);
        ret = compaction->run();
        SE_LOG(INFO, "complete one compaction",
            K(get_task_type_name(job->get_task_type())),
            K((int)job->get_task_type()), K(sub_table->GetID()),
            K(ret), K(compaction->get_stats().record_stats_),
            K(compaction->get_stats().perf_stats_));
        sub_table->internal_stats()->add_compaction_stats(compaction->get_stats());
        record_compaction_stats(compaction->get_stats());
        const_cast<storage::Compaction::Statstics&>(job->get_stats()).record_stats_.add(compaction->get_stats().record_stats_);
        const_cast<storage::Compaction::Statstics&>(job->get_stats()).perf_stats_.add(compaction->get_stats().perf_stats_);
        if (stats_) {
          auto compaction_stat = compaction->get_stats().record_stats_;
          uint64_t bytes_written = (compaction_stat.total_input_extents -
                                    compaction_stat.reuse_extents) *
                                   MAX_EXTENT_SIZE;
          stats_->update_global_compaction_stat(bytes_written,
                                                compaction_stat.start_micros,
                                                compaction_stat.end_micros);
        }
      }
      // retain db mutex for change meta;
      mutex_.Lock();

      if (Status::kOk == ret && (shutting_down_.load(std::memory_order_acquire)
          || sub_table->IsDropped() || sub_table->is_bg_stopped())) {
        ret = Status::kShutdownInProgress;
      }
      // We handle shuttdown and bg_stopped under the mutex
      int64_t commit_log_seq = 0;
      if (Status::kOk != ret
          || FAILED(StorageLogger::get_instance().commit(commit_log_seq))) {
        StorageLogger::get_instance().abort();
        compaction->cleanup();
        if (Status::kShutdownInProgress == ret || Status::kCancelTask == ret) {
          SE_LOG(INFO, "has been shutting down or canceling", K(ret));
        } else {
          SE_LOG(ERROR, "compaction failed to commit, abort trans", K(ret));
        }
      } else {
        AutoThreadOperationStageUpdater stage_updater(ThreadStatus::STAGE_COMPACTION_INSTALL);
        // update Compaction task stats for information_schema
        CompactionJobStatsInfo *job_info = MOD_NEW_OBJECT(ModId::kInformationSchema, CompactionJobStatsInfo);
        job_info->subtable_id_ = sub_table->GetID();
        job_info->sequence_ = compaction_sequence_++;
        job_info->type_ = job->get_task_type();
        job_info->stats_.record_stats_.reset();
        job_info->stats_.record_stats_.add(job->get_stats().record_stats_);
        compaction_sum_.stats_.record_stats_.add(job->get_stats().record_stats_);
        job_info->stats_.perf_stats_.reset();
        job_info->stats_.perf_stats_.add(job->get_stats().perf_stats_);
        compaction_sum_.stats_.perf_stats_.add(job->get_stats().perf_stats_);
        compaction_history_.emplace_front(job_info);
        // limit the compaction list
        if (compaction_history_.size() > MAX_COMPACTION_HISTORY_CNT) {
          CompactionJobStatsInfo *del_info = *compaction_history_.rbegin();
          MOD_DELETE_OBJECT(CompactionJobStatsInfo, del_info);
          compaction_history_.pop_back();
        }

        // record the read write stats
        sub_table->internal_stats()->AddCFStats(InternalStats::BYTES_READ,
            compaction->get_stats().record_stats_.total_input_bytes);
        sub_table->internal_stats()->AddCFStats(InternalStats::BYTES_WRITE,
            compaction->get_stats().record_stats_.total_output_bytes);
        if (is_batch_install_task(job->get_task_type())) {
          // We store each task's result when Intra-L0 or L0->L1.
          if (FAILED(job->append_change_info(compaction->get_change_info()))) {
            SE_LOG(WARN, "faield to append change info", K(ret), K(sub_table->GetID()));
          } else {
            SchedulePendingFlush(sub_table);
            MaybeScheduleFlushOrCompaction();
          }
        } else {
          // begin tranx for compaction install
          mutex_.Unlock();
          int64_t dummy_log_seq = 0;
          storage::ChangeInfo& change_info = compaction->get_change_info();
          if (FAILED(StorageLogger::get_instance().begin(storage::SeEvent::MAJOR_COMPACTION))) {
            SE_LOG(WARN, "fail to begin install major compaction", K(ret));
          } else if (FAILED(sub_table->apply_change_info(change_info, true))) {
            SE_LOG(WARN, "fail to apply change info", K(ret));
          } else if (FAILED(StorageLogger::get_instance().commit(dummy_log_seq))) {
            SE_LOG(WARN, "fail to commit trans", K(ret));
          }

          mutex_.Lock();
          if (SUCCED(ret)) {
            InstallSuperVersionAndScheduleWorkWrapper(sub_table, job_context);
            SE_LOG(INFO, "install compaction result to level(2)");
          }
        }
      }
    }
    job->destroy_compaction(compaction);
  }

  return Status(ret);
}

void DBImpl::record_compaction_stats(
    const storage::Compaction::Statstics& compaction_stats) {
  // compaction IO
  IOSTATS_RESET(bytes_read);
  IOSTATS_RESET(bytes_written);
}

Status DBImpl::BackgroundCompaction(bool* made_progress, JobContext* job_context)
{
  *made_progress = false;
  mutex_.AssertHeld();
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Start");

  Status status = bg_error_;
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::ShutdownInProgress();
    return status;
  }

  // We store pending ColumnFamilys to compaction_queue_, then schedule
  // these CFs one by one.
  // The front CF can build a compaction job or run one task if possible,
  // then it is pushed to the last of queue.
  // We should also handle auto_compaction disabled and CF dropped.
  if (compaction_job_size() > 0) {
    CFCompactionJob* cf_job = pop_front_compaction_job();
    assert(nullptr != cf_job);
    auto cfd = cf_job->cfd_;
    assert(nullptr != cfd);
    assert(cfd->pending_compaction());

    // Pick up latest mutable CF Options and use it throughout the
    // compaction job
    // Compaction makes a copy of the latest MutableCFOptions. It should be used
    // throughout the compaction procedure to make sure consistency. It will
    // eventually be installed into SuperVersion
    auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
    if ((mutable_cf_options->disable_auto_compactions) || cfd->IsDropped()) {
      SE_LOG(INFO, "disable auto compaction on cfd", K(cfd->GetID()), K(mutable_cf_options->disable_auto_compactions));
      remove_compaction_job(cf_job, false);
    } else if (nullptr == cf_job->job_) {
      // Case 1, we should build compaction job when first scheduled this CF.
      status = build_compaction_job(cf_job->cfd_, cf_job->meta_snapshot_, job_context, cf_job->job_, *cf_job);
      if (!status.ok()) {
        // may be build at next round;We remove cf_job in case of memory leak;
        SE_LOG(INFO, "first schedule cfd, failed to build,maybe try again", K(cfd->GetID()), KE(status.code()));
        assert(nullptr == cf_job->job_);
        remove_compaction_job(cf_job);
        return status;
      } else if (0 == cf_job->job_->get_task_size()) {
        SE_LOG(INFO, "first schedule cfd, build empty compaction job", K(cfd->GetID()), KE(status.code()));
        remove_compaction_job(cf_job);
        return status;
      }
      const SnapshotImpl* meta_snapshot = static_cast<const SnapshotImpl*>(cf_job->meta_snapshot_);
      if (IS_NULL(meta_snapshot)) {
        SE_LOG(WARN, "meta snapshot is null", K(cfd->GetID()));
        status = Status(Status::kErrorUnexpected);
      } else {
        if (cf_job->job_->get_task_size() > 0) {
          // reschedule this job if new task created.
          push_back_compaction_job(cf_job);
          unscheduled_compactions_ += cf_job->job_->get_task_size();
        }

        // print subtable schedule info
        SE_LOG(INFO, "first schedule cfd",
            K(cfd->GetID()),
            K(meta_snapshot->GetSequenceNumber()),
            K(cf_job->job_->get_task_size()),
            K((int)cf_job->job_->get_task_type()),
            K(compaction_job_size()),
            K(unscheduled_compactions_),
            K(bg_compaction_scheduled_),
            K(num_running_compactions_));

        TEST_SYNC_POINT("DBImpl::BackgroundCompaction:AfterBuildJob");
        // Note there may be concurent problem here since release_meta_snapshot
        // would unlock and lock again.
        // We release meta snapshot after build compaction job since only 1 compaction
        // job will proceed for each CF.
        if (nullptr != cf_job->meta_snapshot_) {
          Snapshot *s = cf_job->meta_snapshot_;
          cf_job->meta_snapshot_ = nullptr;
          cf_job->cfd_->release_meta_snapshot(s, &mutex_);
          SE_LOG(DEBUG, "release meta snapshot cfd", K(cfd->GetID()));
        }
        MaybeScheduleFlushOrCompaction();
      }
    } else if (cf_job->job_->get_task_size()) {
      // Case 2, we will pick and run one compaction task when rescheduled this CF.
      // Move this cf_job to the last of compaction_queue if it isn't last task
      if (cf_job->job_->get_task_size() > 1) {
        push_back_compaction_job(cf_job);
      }
      status = run_one_compaction_task(cfd, job_context, cf_job->job_);
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction:AfterRunOneTask");
      // all compaction tasks are compeleted.
      if (nullptr != cf_job
          && nullptr != cf_job->cfd_
          && (nullptr == cf_job->job_ || cf_job->job_->all_task_completed())) {
        int ret = status.code();
        // install result only for Minor and Intra compaction
        if (SUCCED(ret) && nullptr != cf_job->job_ && is_batch_install_task(cf_job->job_->get_task_type())) {
          int64_t dummy_log_seq = 0;
          AutoThreadOperationStageUpdater stage_updater(ThreadStatus::STAGE_COMPACTION_INSTALL);
          mutex_.Unlock();
          if (FAILED(StorageLogger::get_instance().begin(storage::MINOR_COMPACTION))) {
            SE_LOG(WARN, "fail to begin minor compaction", K(ret));
          } else if (FAILED(cfd->apply_change_info(cf_job->job_->get_change_info(), true))) {
            SE_LOG(WARN, "fail to apply change info", K(ret));
          } else if (FAILED(StorageLogger::get_instance().commit(dummy_log_seq))) {
            SE_LOG(WARN, "fail to commit compaction trans", K(ret));
          }

          mutex_.Lock();
          if (SUCCED(ret)) {
            InstallSuperVersionAndScheduleWorkWrapper(cfd, job_context);
            SE_LOG(INFO, "install compaction result ", K((int)cf_job->job_->get_task_type()));
          } else {
            SE_LOG(ERROR, "install compaction result failed", K(ret));
          }
        }
        if (SUCCED(ret)) { // update info
          if(FAILED(cfd->set_compaction_check_info(&mutex_))) {
            SE_LOG(WARN, "failed to set compaction check info", K(ret));
          }
        }
        status = Status(ret);
        remove_compaction_job(cf_job);
        TEST_SYNC_POINT("DBImpl::BackgroundCompaction:AfterRemoveJob");
      }
    } else {
      // should not reach here
      SE_LOG(DEBUG, "unexpect path sub_table", K(cfd->GetID()), K(compaction_job_size()), K(cf_job->job_->get_task_size()));
    }
  }

  if (status.ok()) {
    // TODO NotifyCompactionCompleted
    // Done
  } else if (status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else if (Status::kCancelTask == status.code()) {
    SE_LOG(INFO, "just cancel the task");
  } else {
    SE_LOG(WARN, "Compaction error", K(status.ToString().c_str()));
    if (bg_error_.ok()) {
      bg_error_ = status;
      SE_LOG(WARN, "failed during BackgroundCompaction", K((int)bg_error_.code()));
    }
  }

  *made_progress = true;

  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Finish");
  return status;
}

// JobContext gets created and destructed outside of the lock --
// we
// use this convinently to:
// * malloc one SuperVersion() outside of the lock -- new_superversion
// * delete SuperVersion()s outside of the lock -- superversions_to_free
//
// However, if InstallSuperVersionAndScheduleWork() gets called twice with the
// same job_context, we can't reuse the SuperVersion() that got
// malloced because
// first call already used it. In that rare case, we take a hit and create a
// new SuperVersion() inside of the mutex. We do similar thing
// for superversion_to_free
void DBImpl::InstallSuperVersionAndScheduleWorkWrapper(ColumnFamilyData* cfd, JobContext* job_context)
{
  mutex_.AssertHeld();
  SuperVersion* old_superversion = InstallSuperVersionAndScheduleWork(cfd, job_context->new_superversion);
  job_context->new_superversion = nullptr;
  job_context->superversions_to_free.push_back(old_superversion);
}

SuperVersion* DBImpl::InstallSuperVersionAndScheduleWork(ColumnFamilyData* cfd, SuperVersion* new_sv)
{
  mutex_.AssertHeld();

  auto* old_sv = cfd->GetSuperVersion();
  auto* old = cfd->InstallSuperVersion(new_sv ? new_sv : MOD_NEW_OBJECT(ModId::kSuperVersion, SuperVersion), &mutex_);

  // Whenever we install new SuperVersion, we might need to issue new flushes or
  // compactions.
  SchedulePendingFlush(cfd);
  SchedulePendingCompaction(cfd);
  MaybeScheduleFlushOrCompaction();

  return old;
}

}  // namespace db
}  // namespace smartengine
