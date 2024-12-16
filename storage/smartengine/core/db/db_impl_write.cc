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
#include "db/log_writer.h"
#include "options/options_helper.h"
#include "util/file_reader_writer.h"
#include "util/sync_point.h"

namespace smartengine
{
using namespace common;
using namespace memory;
using namespace monitor;
using namespace util;

namespace db
{

using smartengine::db::WriteState;
using smartengine::db::WriteRequest;
using smartengine::db::JoinGroupStatus;
using smartengine::db::BatchGroupManager;

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, ColumnFamilyHandle* column_family,
                   const Slice& key, const Slice& val) {
  return DB::Put(o, column_family, key, val);
}

Status DBImpl::Delete(const WriteOptions& write_options,
                      ColumnFamilyHandle* column_family, const Slice& key) {
  return DB::Delete(write_options, column_family, key);
}

Status DBImpl::SingleDelete(const WriteOptions& write_options,
                            ColumnFamilyHandle* column_family,
                            const Slice& key) {
  return DB::SingleDelete(write_options, column_family, key);
}

Status DBImpl::Write(const WriteOptions& write_options, WriteBatch* my_batch) {
  // return WriteImpl(write_options, my_batch, nullptr, nullptr);
  return WriteImplAsync(write_options, my_batch, nullptr, nullptr, 0, false);
}

Status DBImpl::WriteAsync(const WriteOptions& write_options,
                          WriteBatch* my_batch, AsyncCallback* call_back) {
  return WriteImplAsync(write_options, my_batch, call_back, nullptr, 0, false);
}

Status DBImpl::WriteWithCallback(const WriteOptions& write_options,
                                 WriteBatch* my_batch,
                                 WriteCallback* callback) {
  return WriteImpl(write_options, my_batch, callback, nullptr);
}

Status DBImpl::WriteImpl(const WriteOptions& write_options,
                         WriteBatch* my_batch, WriteCallback* callback,
                         uint64_t* log_used, uint64_t log_ref,
                         bool disable_memtable) {
  if (my_batch == nullptr) {
    return Status::Corruption("Batch is nullptr!");
  }

  if (nullptr != callback) {
    return Status::NotSupported("write witth WriteCallback* not support");
  }
  return WriteImplAsync(write_options, my_batch, nullptr, /*AsyncCallback*/
                        log_used,                         /*log_used*/
                        log_ref,                          /*log ref*/
                        disable_memtable);
}

Status DBImpl::WriteImplAsync(const WriteOptions& write_options,
                              WriteBatch* my_batch, AsyncCallback* call_back,
                              uint64_t* log_used, uint64_t log_ref,
                              bool disable_memtable) {
  QUERY_TRACE_SCOPE(TracePoint::WRITE_ASYNC);
  QUERY_COUNT(CountPoint::WRITE_TRANSACTION_COUNT);
  if (my_batch == nullptr) {
    return Status::Corruption("Batch is nullptr!");
  }

  if (pipline_global_error_flag_.load(std::memory_order_relaxed)) {
    return Status::Incomplete("internal error in pipline, need restart db");
  }

  Status status;
  SequenceNumber thread_local_expected_seq = 0;
  WriteRequest* w_request = MOD_NEW_OBJECT(memory::ModId::kWriteRequest, WriteRequest,
      write_options, my_batch, call_back, log_ref, disable_memtable);
  w_request->start_time_us_ = env_->NowNanos();
  JoinGroupStatus join_status;

  // if there are leff then 5 concurrent threads, we use fast group
  uint64_t thread_num = this->get_active_thread_num();
  if (thread_num < 5) {
    join_status.fast_group_ = 1;
  } else if (thread_num > batch_group_manager_.get_cpu_num()){
    //too much thread,slow done to generate group
    join_status.fast_group_ = 2;
  }
  batch_group_manager_.join_batch_group(w_request, join_status);
  if (db::W_STATE_GROUP_FOLLOWER ==
      join_status.join_state_) {  // leader will free w_request
    QUERY_COUNT(CountPoint::WRITE_DONE_BY_OTHER);
    // we can call DoWriteMemtableJob() and DoCommitJob() here
    if (join_status.async_commit_) {
      return status;
    }

    assert(db::W_STATE_GROUP_FOLLOWER == join_status.join_state_ &&
           false == join_status.async_commit_);
    QUERY_TRACE_SCOPE(TracePoint::WRITE_SYNC_WAIT);
    this->batch_group_manager_.await_state(w_request,
                                           db::W_STATE_GROUP_COMPLETED);
    // set out parameter, log_used,wet got this in DoWriteMemtableJob();
    if (nullptr != log_used) {
      *log_used = w_request->log_used_;
      assert(*log_used != 0);
    }
    this->complete_write_job(w_request, status);
    return status;
  }

  assert(db::W_STATE_GROUP_LEADER == join_status.join_state_);
  assert(db::W_STATE_GROUP_LEADER == w_request->state_);
  QUERY_COUNT(CountPoint::PIPELINE_GROUP_COUNT);
  QUERY_COUNT_ADD(CountPoint::PIPLINE_GROUP_SIZE,
                  w_request->follower_vector_.size())
  uint64_t total_count = 0;
  uint64_t total_byte_size = 0;
  bool need_log = false;
  bool need_log_sync = false;
  WriteBatch* merged_batch = &(w_request->group_merged_log_batch_);
  BinlogPosition merged_binlog_pos;
  merged_batch->Clear();
  w_request->group_run_in_parallel_ =
      immutable_db_options_.allow_concurrent_memtable_write;
  for (auto writer : w_request->follower_vector_) {
    if (writer->should_write_to_memtable()) {
      total_count += WriteBatchInternal::Count(writer->batch_);
    }
    if (writer->should_write_to_wal()) {
      WriteBatchInternal::Append(merged_batch, writer->batch_,
                                 /*WAL_only*/ true);
      need_log = (need_log || !writer->disable_wal_);
      need_log_sync = (need_log_sync || writer->log_sync_);
      QUERY_COUNT(CountPoint::WRITE_WITH_WAL);
      QUERY_COUNT_ADD(CountPoint::WAL_FILE_BYTES,
                      writer->batch_->GetDataSize());
    }
    if (writer->batch_->HasBinlogPosition() &&
        merged_binlog_pos.compare(*writer->batch_->GetBinlogPosition()) < 0)
      merged_binlog_pos = *writer->batch_->GetBinlogPosition();
    total_byte_size = WriteBatchInternal::AppendedByteSize(
        total_byte_size, WriteBatchInternal::ByteSize(writer->batch_));
  }
  // we may meet empty batch when mysql startup
  // so just set total_count = 1 when total_count is 0
  total_count = total_count == 0 ? 1 : total_count;
  w_request->group_total_count_ = total_count;
  w_request->group_total_byte_size_ = total_byte_size;
  w_request->group_need_log_ = need_log;
  w_request->group_need_log_sync_ = need_log_sync;

  // caculate log crc32 checksum
  // we caculate crc32 here,all the leader can run concurrently
  if (w_request->group_need_log_) {
    Slice log_slice =
        WriteBatchInternal::Contents(&(w_request->group_merged_log_batch_));
    w_request->log_crc32_ = log::Writer::calculate_crc(log_slice);
  }

  QUERY_TRACE_BEGIN(TracePoint::WRITE_WAIT_LOCK);
  mutex_.Lock();
  QUERY_TRACE_END();
  QUERY_TRACE_BEGIN(TracePoint::WRITE_RUN_IN_MUTEX);
  WriteContext write_context;
  bool logs_getting_synced = false;  // this is useless
  status = PreprocessWrite(write_options, need_log_sync, &logs_getting_synced,
                           &write_context);
  TEST_SYNC_POINT("DBimpl::WriteImplAsync::AfterPreprocessWrite");

  this->increase_active_thread(!w_request->group_run_in_parallel_);

  if (merged_binlog_pos.valid() &&
      this->global_binlog_pos_.compare(merged_binlog_pos) < 0) {
    SE_LOG(DEBUG, "update se global binlog position", K(merged_binlog_pos.file_name_), K(merged_binlog_pos.offset_));
    this->global_binlog_pos_ = merged_binlog_pos;
  }

  uint64_t last_sequence =
      versions_->AllocateSequence(w_request->group_total_count_);
  w_request->group_first_sequence_ = last_sequence + 1;
  w_request->group_last_sequence_ =
      last_sequence + w_request->group_total_count_;
  w_request->log_writer_used_ = curr_log_writer_;
  // we will asigh to follower in DoWriteMemtableJob();
  assert(this->logfile_number_ != 0);
  w_request->log_used_ = this->logfile_number_;
  thread_local_expected_seq = w_request->group_first_sequence_;

  if (w_request->group_need_log_) {
    this->log_empty_ = false;
    WriteBatchInternal::SetSequence(merged_batch,
                                    w_request->group_first_sequence_);
  } else {
    has_unpersisted_data_.store(true, std::memory_order_relaxed);
  }

  this->pipline_manager_.add_copy_log_job(w_request);
	
  mutex_.Unlock();
  QUERY_TRACE_END();
  // threadlocal, mutex is unnecessary
  QUERY_COUNT_ADD(CountPoint::NUMBER_KEYS_WRITTEN, total_count);
  QUERY_COUNT_ADD(CountPoint::BYTES_PER_WRITE, total_byte_size);
  QUERY_COUNT(CountPoint::WRITE_DONE_BY_SELF);

  // run pipline jobs
  int error = 0;
  if (status.ok()) {
    error = this->run_pipline(thread_local_expected_seq);
  } else {
    TEST_SYNC_POINT("DBImpl::WriteImplAsync::run_pipline_error");
    error = -1;
  }

  this->decrease_active_thread();

  if (error) {  // failed we should clean up the pipline
    mutex_.Lock();
    this->wait_all_active_thread_exit();
    if (bg_error_.ok()) {
      bg_error_ = status;  // stop compaction & fail any further writes

      SE_LOG(WARN, "failed during WriteImplAsync", K((int)bg_error_.code()));
    }

    // clean pipline
    this->clean_pipline_error();
    mutex_.Unlock();
    status = common::Status::Incomplete("run pipline failed!");
  }

  if (!join_status.async_commit_) {
    QUERY_TRACE_SCOPE(TracePoint::WRITE_SYNC_WAIT);
    this->batch_group_manager_.await_state(w_request,
                                           db::W_STATE_GROUP_COMPLETED);
    if (nullptr != log_used) *log_used = w_request->log_used_;
    this->complete_write_job(w_request, status);
  }

  return status;
}

int DBImpl::clean_pipline_error() {
  int error = 0;

  int job_num = 0;
  WriteRequest* request = nullptr;

  while (this->pipline_manager_.get_copy_log_job_num() > 0) {
    job_num = 0;
    request = nullptr;
    job_num = this->pipline_manager_.pop_copy_log_job(request);
    assert(job_num != 0 && request != nullptr);
    for (auto job : request->follower_vector_) {
      job->status_ = Status::IOError("failed to write log buffer");
    }
    this->pipline_manager_.add_error_job(request);
  }

  while (this->pipline_manager_.get_flush_log_job_num() > 0) {
    job_num = 0;
    request = nullptr;
    job_num = this->pipline_manager_.pop_flush_log_job(request);
    assert(job_num != 0 && request != nullptr);
    for (auto job : request->follower_vector_) {
      job->status_ = Status::IOError("failed to flush log buffer");
    }
    this->pipline_manager_.add_error_job(request);
  }

  while (this->pipline_manager_.get_memtable_job_num() > 0) {
    job_num = 0;
    request = nullptr;
    job_num = this->pipline_manager_.pop_memtable_job(request);
    assert(job_num != 0 && request != nullptr);
    for (auto job : request->follower_vector_) {
      job->status_ = Status::Incomplete("write memtable failed");
    }
    this->pipline_manager_.add_error_job(request);
  }

  // clean reqeust in sliding_window
  for (auto ite = version_sliding_window_map_.begin();
       ite != version_sliding_window_map_.end(); ++ite) {
    assert(nullptr != ite->second);
    for (auto job : request->follower_vector_) {
      job->status_ = Status::Incomplete("failed in sliding window");
    }
    this->pipline_manager_.add_error_job(ite->second);
  }

  while (this->pipline_manager_.get_commit_job_num() > 0) {
    job_num = 0;
    request = nullptr;
    job_num = this->pipline_manager_.pop_commit_job(request);
    assert(job_num != 0 && request != nullptr);
    for (auto job : request->follower_vector_) {
      job->status_ = Status::Incomplete("failed in commit");
    }
    this->pipline_manager_.add_error_job(request);
  }

  WriteRequest* error_job = nullptr;
  job_num = 0;
  std::vector<WriteRequest*> error_job_list;
  while (true) {
    job_num = this->pipline_manager_.pop_error_job(error_job);
    if (!job_num) break;
    assert(nullptr != error_job);
    error_job_list.clear();
    std::copy(error_job->follower_vector_.begin(),
              error_job->follower_vector_.end(),
              std::back_inserter(error_job_list));
    bool async_commit = false;
    for (auto job : error_job_list) {
      async_commit = job->async_commit_;
      if (job->status_.ok())  // update error status if not set
        job->status_ = Status::Incomplete("internal error in pipline");
      BatchGroupManager::set_state(job, db::W_STATE_GROUP_COMPLETED);
      if (async_commit) {  // ASYNC mode, we complete the job
        Status s = Status::OK();
        error = this->complete_write_job(job, s);
      }
    }
  }
  return error;
}

int DBImpl::complete_write_job(WriteRequest* writer, Status& s) {
  int error = 0;
  assert(nullptr != writer);
  assert(db::W_STATE_GROUP_COMPLETED == writer->state_);
  s = writer->status_;
  if (nullptr != writer->async_callback_) {
    bool commit_succeed = s.ok() ? true : false;
    Status cb_status = writer->async_callback_->run_call_back(commit_succeed);

    if (writer->async_callback_->destroy_on_complete()) {
      delete writer->async_callback_;
    }
    if (!cb_status.ok()) {
      __SE_LOG(ERROR, "Failed to run call back sequence=%lu, error=%s", writer->sequence_, cb_status.ToString().c_str());
      error = -1;
    }
    if (s.ok()) s = cb_status;
  }
  // Measure the time in every stage
  // MeasureTime(stats_, DEMO_WATCH_TIME_NANOS, env_->NowNanos() -
  // writer->start_time_us_);
//  delete writer;
//  writer = nullptr;
  MOD_DELETE_OBJECT(WriteRequest, writer);
  return error;
}

int DBImpl::run_pipline(uint64_t thread_local_expected_seq) {
  // forget about w_request now, we run in async mode
  QUERY_COUNT_ADD(CountPoint::PIPLINE_CONCURRENT_RUNNING_WORKER_THERADS,
                  get_active_thread_num());
  int error = 0;
  int loop_count = 0;
  // if we encounter error triggeredk by previous reqeust,just return error;
  if (pipline_global_error_flag_.load()) {
    error = -1;
    return error;
  }
  while (false == pipline_global_error_flag_.load() && bg_error_.ok()) {
    loop_count++;
    //(1) copy log buffer
    if (0 != this->do_copy_log_buffer_job(thread_local_expected_seq)) {
      error = -1;
      break;
    }
    //(2) write log buffer
    if (0 != this->do_flush_log_buffer_job(thread_local_expected_seq)) {
      error = -1;
      break;
    }

    //(3) write memtable and  update global version
    if (0 != this->do_write_memtable_job()) {
      error = -1;
      break;
    }
    //(4) run callback function, do commit work
    if (0 != this->do_commit_job()) {
      error = -1;
      break;
    }

    // all the work done, we can exit
    //  a, LastSequence() > thread_loal_expected_seq means we have done memtalbe
    //  job
    //  b, commit work done mean there is no job left in commit queue
    //  c, so we can leave now
    if (versions_->LastSequence() >= thread_local_expected_seq &&
        pipline_manager_.is_commit_job_done(thread_local_expected_seq)) {
      break;
    }
    // a. there are less then 5 worker threads
    // b, and there are still job left in pipline queues, we reserve at least 4
    // thread to do the jobs
    //          one for copy log buffer
    //          one for flush log
    //          one for write memtable
    //          one for commit
    // to keep the pipline running  ,there should be at least 4 thread
    if (this->pipline_manager_.get_last_sequence_post_to_memtable_queue() <
            thread_local_expected_seq &&
        get_active_thread_num() < 5) {
      continue;
    }

    if (this->pipline_manager_.get_last_sequence_post_to_log_queue() >
            thread_local_expected_seq &&
        this->pipline_manager_.is_memtable_job_done(
            thread_local_expected_seq) &&
        this->pipline_manager_.is_commit_job_done(
            thread_local_expected_seq)) {  // all work done, exit
      break;
    }
  }

  if (error) {  // update backgroup error
    pipline_global_error_flag_.store(true);
  }

  QUERY_COUNT_ADD(CountPoint::PIPLINE_LOOP_COUNT, loop_count);
  QUERY_COUNT_ADD(CountPoint::PIPLINE_LOG_QUEUE_LENGTH,
                  pipline_manager_.get_copy_log_job_num());
  QUERY_COUNT_ADD(CountPoint::PIPLINE_MEM_QUEUE_LENGTH,
                  pipline_manager_.get_memtable_job_num());
  QUERY_COUNT_ADD(CountPoint::PIPLINE_COMMIT_QUEUE_LENGTH,
                  pipline_manager_.get_commit_job_num());
  return error;
}

int DBImpl::do_copy_log_buffer_job(uint64_t thread_local_expected_seq) {
  int error = 0;
  bool busy = this->pipline_copy_log_busy_flag_.load();
  if (busy ||
      !this->pipline_copy_log_busy_flag_.compare_exchange_strong(busy, true)) {
    return error;
  }

  assert(this->pipline_copy_log_busy_flag_.load());
  QUERY_TRACE_SCOPE(TracePoint::TIME_PER_LOG_COPY);

  const uint64_t MAX_COPY_BYTES_IN_SINGLE_LOOP = 4 * 1024 * 1024;
  uint64_t total_log_bytes = 0;
  uint64_t processeed_entry_num = 0;
  size_t job_num = 0;
  Status copy_log_status;
  uint32_t log_crc32 = 0;
  while (!error) {
    if (total_log_bytes > MAX_COPY_BYTES_IN_SINGLE_LOOP) {
      break;
    }
    WriteRequest* log_request = nullptr;
    if (!(job_num = this->pipline_manager_.pop_copy_log_job(log_request))) {
      assert(this->pipline_manager_.get_last_sequence_post_to_log_queue() >=
             thread_local_expected_seq);
      break;
    }
    assert(0 != job_num && log_request != nullptr);
    // we update current_log_writer_ in switchmemtable
    // so all of the log_requests in log_queue_ should wirte the same log_writer
    assert(curr_log_writer_ == log_request->log_writer_used_);
    assert(log_request->group_first_sequence_ >=
           this->pipline_manager_.get_last_sequence_post_to_flush_queue());

    // update seq_write_to_
    Slice log_entry =
        WriteBatchInternal::Contents(&(log_request->group_merged_log_batch_));
    log_crc32 = log_request->log_crc32_;
    assert(log_entry.size() != 0);

    // memory copy
    processeed_entry_num++;

    if (log_request->group_need_log_) {
      this->total_log_size_ += log_entry.size();
      this->alive_log_files_.back().AddSize(log_entry.size());
      copy_log_status = curr_log_writer_->AddRecord(log_entry, log_crc32);
    }

//inject error in copy log buffer
#ifndef NDEBUG
    TEST_SYNC_POINT("DBImpl::do_copy_log_buffer_job::inject_error");
    if (TEST_if_copy_log_fail()){
      copy_log_status = common::Status::Incomplete("inject error copy log");
    }
#endif

    if (!copy_log_status.ok()) {
      __SE_LOG(ERROR,
          "Fail to add wal log group_first_sequqnece=%lu,"
          "group_last_sequence_=%lu,"
          "log_file_num=%lu,log_file_offset=%lu,log_entry_size=%lu,",
          log_request->group_first_sequence_, log_request->group_last_sequence_,
          log_request->log_used_, curr_log_writer_->file()->get_file_size(),
          log_entry.size());
      assert(log_request != nullptr);
      this->pipline_manager_.add_error_job(log_request);
      error = -1;
      break;
    }
    total_log_bytes += log_entry.size();
    log_request->log_file_pos_ = curr_log_writer_->file()->get_file_size();
    this->pipline_manager_.add_flush_log_job(log_request);
  }
  assert(this->pipline_copy_log_busy_flag_.load());
  this->pipline_copy_log_busy_flag_.store(false);
  if (total_log_bytes > 0) {
    QUERY_COUNT_ADD(CountPoint::PIPELINE_COPY_LOG_SIZE, total_log_bytes);
    QUERY_COUNT_ADD(CountPoint::PIPELINE_COPY_LOG_COUNT, processeed_entry_num);
  }
  return error;
}

int DBImpl::do_flush_log_buffer_job(uint64_t thread_local_expected_seq) {
  int error = 0;

  // check this to avoid frequently flush log buffer, we can write more bytes
  // one time
  if ((curr_log_writer_->file()->get_imm_buffer_num() == 0) &&
      (this->pipline_manager_.get_last_sequence_post_to_log_queue() >
           thread_local_expected_seq ||
       this->pipline_manager_.get_memtable_job_num() != 0 ||
       this->pipline_manager_.get_copy_log_job_num() != 0 ||
       this->pipline_manager_.get_commit_job_num() != 0)) {
    return error;
  }

  if (!immutable_db_options_.parallel_flush_log) {
    bool busy = this->pipline_flush_log_busy_flag_.load();
    if (busy ||
        !this->pipline_flush_log_busy_flag_.compare_exchange_strong(busy, true)) {
      return error;
    }
  }

  assert(immutable_db_options_.parallel_flush_log || this->pipline_flush_log_busy_flag_.load());
  QUERY_TRACE_SCOPE(TracePoint::TIME_PER_LOG_WRITE);
  Status s;

  //(1)  flush one one imm log buffer
  int64_t flush_bytes = 0;
  uint64_t flush_lsn = 0;

  error = curr_log_writer_->file()->try_to_flush_one_imm_buffer();
  log_flush_mutex_.lock();
  if (!error) {
    flush_lsn = curr_log_writer_->file()->get_flush_pos();
    se_assert(flush_lsn >= this->last_flushed_log_lsn_.load());
    flush_bytes = flush_lsn - this->last_flushed_log_lsn_.load();
    this->last_flushed_log_lsn_.store(flush_lsn);
  } else {
    this->pipline_flush_log_busy_flag_.store(false);
    log_flush_mutex_.unlock();
    return error;
  }

  //(2) pop job from flush queue
  std::vector<WriteRequest*> flush_list;
  bool need_wal_sync = false;
  this->pipline_manager_.pop_flush_log_job(flush_lsn, flush_list,
                                           need_wal_sync);
  if (need_wal_sync) {
    QUERY_TRACE_SCOPE(TracePoint::WAL_FILE_SYNC);
    QUERY_COUNT(CountPoint::WAL_FILE_SYNCED)
    QUERY_COUNT(CountPoint::PIPELINE_FLUSH_LOG_SYNC_COUNT);
    // Handle sync failed !!!
    s = curr_log_writer_->file()->sync_to_disk(false);
  } else {
    QUERY_COUNT(CountPoint::PIPELINE_FLUSH_LOG_NOT_SYNC_COUNT);
  }

#ifndef NDEBUG
  //inject io error while flush log buffer
  //this will left corrupted wal log entry 
  if (curr_log_writer_->file()->get_imm_buffer_num() != 0) {
    TEST_SYNC_POINT("DBImpl::do_flush_log_buffer_job::after_flush_sync"); 
  }
#endif

  if (!s.ok()) {
    error = -1;
    this->pipline_flush_log_busy_flag_.store(false);
    // add to error job
    for (auto request : flush_list)
      this->pipline_manager_.add_error_job(request);

    return error;
  }

  //(3) push job to memtable queue
  for (auto request : flush_list) {
    // assert(request->log_file_pos_ <= flush_lsn);
    this->pipline_manager_.add_memtable_job(request);
  }

  log_flush_mutex_.unlock();
  this->pipline_flush_log_busy_flag_.store(false);

  QUERY_COUNT(CountPoint::PIPELINE_FLUSH_LOG_COUNT);
  QUERY_COUNT_ADD(CountPoint::PIPELINE_FLUSH_LOG_SIZE, flush_bytes);
  //make compiler happy
  UNUSED(flush_bytes);
  return error;
}

int DBImpl::do_write_memtable_job() {
  int error = 0;
  int tmp_ret = Status::kOk;
  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;

  WriteRequest* mem_task = nullptr;
  size_t job_num = pipline_manager_.pop_memtable_job(mem_task);
  if (0 == job_num) return error;
  QUERY_TRACE_SCOPE(TracePoint::WRITE_MEMTABLE);
  assert(1 == job_num && nullptr != mem_task);
  SequenceNumber iterator_seq = mem_task->group_first_sequence_;
  int64_t log_used = mem_task->log_used_;  // leader log_used  asign to follower
  bool serialization_mode = !mem_task->group_run_in_parallel_;
  for (auto writer : mem_task->follower_vector_) {
    writer->sequence_ = iterator_seq;
    writer->log_used_ = log_used;
    assert(writer->log_used_ != 0);
    if (!writer->should_write_to_memtable())  // skip write memtable
      continue;

    WriteBatchInternal::SetSequence(writer->batch_, writer->sequence_);
    if (nullptr == (global_ctx = versions_->get_global_ctx())) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
    } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
      SE_LOG(WARN, "fail to acquire all sub table", K(ret));
    } else if (nullptr == all_sub_table) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
    } else {
      ColumnFamilyMemTablesImpl column_family_memtables(all_sub_table->sub_table_map_, versions_->GetColumnFamilySet());

      if (serialization_mode) {
        writer->status_ = WriteBatchInternal::InsertInto(
            writer, writer->sequence_, &column_family_memtables,
            &flush_scheduler_, writer->ignore_missing_cf_,
            0 /*recovery_log_number*/, this);
      } else {
        writer->status_ = WriteBatchInternal::InsertInto(
          writer, &column_family_memtables, &this->flush_scheduler_,
          writer->ignore_missing_cf_, 0 /*log_number*/, this,
          true /*concurrent_memtable_writes*/);
      }
    }

    //there will cover the error code, by design
    tmp_ret = ret;
    if (nullptr != global_ctx && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
      SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
    }

//inject error in write memtable
#ifndef NDEBUG
    TEST_SYNC_POINT("DBImpl::do_write_memtable_job::inject_error");
    if (TEST_if_write_memtable_fail()){
      writer->status_ = common::Status::MemoryLimit("inject error in write memtable");
    }
#endif

    if (!writer->status_.ok()) {
      __SE_LOG(ERROR, "Failed to write memory Table sequence =%lu eror=%s",
          writer->sequence_, writer->status_.ToString().c_str());
      error = -1;
      break;
    }
    iterator_seq += WriteBatchInternal::Count(writer->batch_);
  }
  if (!error) {
    assert(iterator_seq == mem_task->group_last_sequence_ + 1 ||
         iterator_seq == mem_task->group_last_sequence_);
    this->update_committed_version(mem_task);
  } else {  //
    assert(mem_task != nullptr);
    this->pipline_manager_.add_error_job(mem_task);
  }
  return error;
}

void DBImpl::update_committed_version(WriteRequest* writer) {
  // StopWatchNano   demo_stop_watch(env_, true);
  MutexLock lock_guard(&version_sliding_window_mutex_);

  // should not exit before insert
  assert(version_sliding_window_map_.find(writer->group_first_sequence_) ==
         version_sliding_window_map_.end());
  std::pair<uint64_t, WriteRequest*> commit_unit(writer->group_first_sequence_,
                                                 writer);
  version_sliding_window_map_.insert(commit_unit);
  std::unordered_map<uint64_t, WriteRequest*>::iterator ite;
  WriteRequest* c_unit = nullptr;
  uint64_t expected_sequence = 0;
  uint64_t rec_sequence = 0;
  while (true) {
    expected_sequence = versions_->LastSequence() + 1;
    ite = version_sliding_window_map_.find(expected_sequence);
    if (ite == version_sliding_window_map_.end()) {
      break;
    }
    c_unit = ite->second;
    assert(c_unit->group_first_sequence_ == ite->first);
    // we record sequence first,once in job quque it would be destruct
    rec_sequence = c_unit->group_last_sequence_;
    versions_->SetLastSequence(rec_sequence);
    this->pipline_manager_.add_commit_job(c_unit);
    version_sliding_window_map_.erase(ite);
  }
}

int DBImpl::do_commit_job() {
  int error = 0;
  WriteRequest* commit_request = nullptr;

  size_t job_num = this->pipline_manager_.pop_commit_job(commit_request);
  if (0 == job_num) return error;
  QUERY_TRACE_SCOPE(TracePoint::COMMIT_JOB);
  assert(0 != job_num && commit_request != nullptr);
  std::vector<WriteRequest*> request_list;
  std::copy(commit_request->follower_vector_.begin(),
            commit_request->follower_vector_.end(),
            std::back_inserter(request_list));
  bool async_commit = false;
  Status s;
  for (auto writer : request_list) {
    async_commit = writer->async_commit_;
    BatchGroupManager::set_state(writer, db::W_STATE_GROUP_COMPLETED);
    if (async_commit) {  // ASYNC mode, we complete the job
      error = this->complete_write_job(writer, s);
    }
//inject error in commit job
#ifndef NDEBUG
    TEST_SYNC_POINT("DBImpl::do_commit_job::inject_error");
    if (TEST_if_commit_fail()){
      error = -1;
      writer->status_ = common::Status::Incomplete("inject error in commit job");
    }
#endif
    if (error) break;
  }
  return error;
}

void DBImpl::increase_active_thread(bool serialization_mode) {
  if (serialization_mode || this->last_write_in_serialization_mode_.load()) {
    this->wait_all_active_thread_exit();
    assert(false == last_write_in_serialization_mode_.load());
    assert(0 == get_active_thread_num());
    if (serialization_mode) {
      this->last_write_in_serialization_mode_.store(true);
    }
  }
  // last writer already exit
  this->active_thread_num_.fetch_add(1);
}

void DBImpl::decrease_active_thread() {
  if (this->last_write_in_serialization_mode_.load()) {
    assert(1 == get_active_thread_num());
    this->last_write_in_serialization_mode_.store(false);
  }
  if (1 == this->active_thread_num_.fetch_sub(1) &&
      this->wait_active_thread_exit_flag_.load()) {
    MutexLock lock_guard(&active_thread_mutex_);
    active_thread_cv_.Signal();
  }
}

void DBImpl::wait_all_active_thread_exit() {
  this->wait_active_thread_exit_flag_.store(true);
  MutexLock lock_guard(&active_thread_mutex_);
  while (0 < this->active_thread_num_.load()) {
    active_thread_cv_.Wait();
  }
  this->wait_active_thread_exit_flag_.store(false);
}

uint64_t DBImpl::get_active_thread_num(std::memory_order order) {
  return this->active_thread_num_.load(order);
}

Status DBImpl::PreprocessWrite(const WriteOptions& write_options,
                               bool need_log_sync, bool* logs_getting_synced,
                               WriteContext* write_context) {
  mutex_.AssertHeld();
  assert(write_context != nullptr && logs_getting_synced != nullptr);
  Status status = Status::OK();

  LogFileNumberSize last_file = alive_log_files_.back();
  uint64_t log_ize_limit = 1024 * 1024 * 1024; // 1G
  // check single wal file size
  if (!last_file.switch_flag
      && last_file.size > log_ize_limit) {
    // pick one cfd to do switch
    // todo (yeti) add single-wal switch way, don't depend on switching memtable
    write_context->type_ = SINGLE_WAL_LIMIT;
    int ret = handle_single_wal_full(write_context);
    status = Status(ret);
  }

  if (UNLIKELY(status.ok() && !single_column_family_mode_ &&
               total_log_size_ > GetMaxTotalWalSize())) {
    write_context->type_ = WAL_LIMIT;
    status = HandleWALFull(write_context);
  }

  if (UNLIKELY(status.ok() && write_buffer_manager_->should_flush())) {
    // Before a new memtable is added in switch_memtable(),
    // write_buffer_manager_->ShouldFlush() will keep returning true. If another
    // thread is writing to another DB with the same write buffer, they may also
    // be flushed. We may end up with flushing much more DBs than needed. It's
    // suboptimal but still correct.
    write_context->type_ = WRITE_BUFFER_LIMIT;
    status = HandleWriteBufferFull(write_context);
  }

  if (UNLIKELY(status.ok() && write_buffer_manager_->should_trim())) {
    write_context->type_ = TOTAL_WRITE_BUFFER_LIMIT;
    status = HandleTotalWriteBufferFull(write_context);
  }

  if (UNLIKELY(status.ok() && !bg_error_.ok())) {
    return bg_error_;
  }

  if (UNLIKELY(status.ok() && !flush_scheduler_.Empty())) {
    write_context->type_ = OTHER; // delete/write_buffer
    status = schedule_flush(write_context);
  }

  return status;
}

int DBImpl::handle_single_wal_full(WriteContext* write_context) {
  int ret = Status::kOk;
  auto oldest_alive_log = alive_log_files_.begin()->number;
  SubTable *pick_sub_table = nullptr;
  uint64_t mem_size = 0;
  GlobalContext* global_ctx = nullptr;
  if (FAILED(get_all_sub_table(write_context->all_sub_table_, global_ctx))) {
    SE_LOG(WARN, "get all subtable failed", K(oldest_alive_log));
  } else {
    SubTableMap& all_subtables = write_context->all_sub_table_->sub_table_map_;
    SubTable* sub_table = nullptr;
    for (auto iter = all_subtables.begin();
         Status::kOk == ret && iter != all_subtables.end(); ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (sub_table->IsDropped()) {
        // subtable has been dropped, do nothing
        SE_LOG(INFO, "subtable has been dropped", K(iter->first));
      } else if (sub_table->OldestLogToKeep() <= oldest_alive_log) {
        if (nullptr == pick_sub_table) {
          pick_sub_table = sub_table;
        }
        uint64_t cur_size = 0;
        if (nullptr != sub_table->mem()) {
          cur_size = sub_table->mem()->ApproximateMemoryUsage();
        }
        if (cur_size > mutable_db_options_.dump_memtable_limit_size) {
          pick_sub_table = sub_table;
          break;
        } else if (cur_size > mem_size) {
          pick_sub_table = sub_table;
          mem_size = cur_size;
        }
      }
    }
  }

  if (nullptr != pick_sub_table) {
    SE_LOG(INFO, "CK_INFO: picked this subtable to switch",
        "index_id", pick_sub_table->GetID(), "oldest_log_to_keep", pick_sub_table->OldestLogToKeep(),
        K(oldest_alive_log));
    LogFileNumberSize &last_file = alive_log_files_.back();
    last_file.switch_flag = true;
    if (FAILED(trigger_switch_memtable(pick_sub_table, write_context))) {
      SE_LOG(ERROR, "failed to trigger switch memtable", K(pick_sub_table->GetID()), K(ret));
    }
  } else {
    MaybeScheduleFlushOrCompaction();
    SE_LOG(INFO, "failed to pick one cfd, wait next", K(oldest_alive_log));
  }

  int tmp_ret = ret;
  if (nullptr != global_ctx &&
      FAILED(global_ctx->release_thread_local_all_sub_table(write_context->all_sub_table_))) {
    SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }
  // avoid imm not flushed for too long time
  alive_log_files_.begin()->getting_flushed = false;
  return ret;
}

int DBImpl::get_all_sub_table(AllSubTable *&all_sub_table, GlobalContext *&global_ctx) {
  int ret = 0;
  if (IS_NULL(global_ctx = versions_->get_global_ctx())) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (IS_NULL(all_sub_table)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  }
  return ret;
}

int DBImpl::find_subtables_to_switch(const uint64_t oldest_alive_log, WriteContext* write_context, bool force_switch) {
  mutex_.AssertHeld();
  int ret = Status::kOk;
  SubTableMap& all_subtables = write_context->all_sub_table_->sub_table_map_;
  SubTable* sub_table = nullptr;

  for (auto iter = all_subtables.begin(); SUCCED(ret) && all_subtables.end() != iter; ++iter) {
    if (IS_NULL(sub_table = iter->second)) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
    } else if (sub_table->IsDropped()) {
      // subtable has been dropped, do nothing
    } else if (0 == sub_table->mem()->GetFirstSequenceNumber() &&
               0 == sub_table->imm()->NumNotFlushed()) {
      // do nothing
    } else {
      uint64_t min_lognumber = sub_table->OldestLogMemToKeep();
      if (min_lognumber <= oldest_alive_log) {
        SE_LOG(INFO, "CK_INFO: pick this subtable to switch or dump",
            "index_id", sub_table->GetID(), K(min_lognumber), K(oldest_alive_log), K(force_switch));
        if (force_switch) {
          if (FAILED(trigger_switch_memtable(sub_table, write_context))) {
            SE_LOG(WARN, "failed to trigger switch memtable", K(ret), K(sub_table->GetID()));
          }
        } else if (FAILED(trigger_switch_or_dump(sub_table, write_context))) {
          SE_LOG(ERROR, "failed to trigger switch or dump", K(ret), K(oldest_alive_log));
        }
      }
    }
  }

  return ret;
}

int DBImpl::force_handle_wal_full(WriteContext* write_context) {
  int ret = 0;
  mutex_.Lock();
  auto oldest_alive_log = alive_log_files_.begin()->number;
  GlobalContext* global_ctx = nullptr;
  if (FAILED(get_all_sub_table(write_context->all_sub_table_, global_ctx))) {
    SE_LOG(WARN, "get all subtable failed", K(oldest_alive_log));
  } else if (FAILED(find_subtables_to_switch(oldest_alive_log, write_context, true))) {
    SE_LOG(WARN, "failed to find subtables to switch", K(oldest_alive_log));
  }
  int tmp_ret = ret;
  if (nullptr != global_ctx &&
      FAILED(global_ctx->release_thread_local_all_sub_table(write_context->all_sub_table_))) {
    SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }

  advance_recovery_point_without_flush();
  mutex_.Unlock();
  return ret;
}

Status DBImpl::HandleWALFull(WriteContext* write_context) {
  mutex_.AssertHeld();
  assert(write_context != nullptr);
  Status status;

  if (alive_log_files_.begin()->getting_flushed) {
    return status;
  }

  auto oldest_alive_log = alive_log_files_.begin()->number;
  auto oldest_log_with_uncommited_prep = FindMinLogContainingOutstandingPrep();

  if (allow_2pc() && oldest_log_with_uncommited_prep > 0 &&
      oldest_log_with_uncommited_prep <= oldest_alive_log) {
    if (unable_to_flush_oldest_log_) {
      // we already attempted to flush all column families dependent on
      // the oldest alive log but the log still contained uncommited
      // transactions.
      // the oldest alive log STILL contains uncommited transaction so there
      // is still nothing that we can do.
      return status;
    } else {
      SE_LOG(WARN, "Unable to release oldest log due to uncommited transaction");
      unable_to_flush_oldest_log_ = true;
    }
  } else {
    // we only mark this log as getting flushed if we have successfully
    // flushed all data in this log. If this log contains outstanding prepared
    // transactions then we cannot flush this log until those transactions are
    // commited.
    unable_to_flush_oldest_log_ = false;
    alive_log_files_.begin()->getting_flushed = true;
  }

  GlobalContext* global_ctx = nullptr;
  int ret = 0;
  if (FAILED(get_all_sub_table(write_context->all_sub_table_, global_ctx))) {
    SE_LOG(WARN, "get all subtable failed", K(oldest_alive_log));
    status = Status(ret);
  }

  // no need to refcount because drop is happening in write thread, so can't
  // happen while we're in the write thread
  if ((SUCCED(ret))) {
    if (FAILED(find_subtables_to_switch(oldest_alive_log, write_context))) {
      SE_LOG(WARN, "failed to find subtables to switch", K(oldest_alive_log), K(ret));
    }
  }

  // there will cover the error code, by design
  int tmp_ret = ret;
  if (nullptr != global_ctx &&
      FAILED(global_ctx->release_thread_local_all_sub_table(write_context->all_sub_table_))) {
    SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }
  maybe_schedule_dump();
  MaybeScheduleFlushOrCompaction();
  advance_recovery_point_without_flush();
  SE_LOG(INFO, "CK_INFO: find all cfds to do dump/switch",
      K(oldest_alive_log), K(GetMaxTotalWalSize()), K(total_log_size_.load()));
  return status;
}

int DBImpl::build_dump_job(ColumnFamilyData *sub_table, bool &do_dump) {
  mutex_.AssertHeld();
  int ret = Status::kOk;
  if (IS_NULL(sub_table)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "subtable is null", K(ret));
  } else if (sub_table->pending_flush() || sub_table->pending_dump() || sub_table->pending_shrink()) {
    SE_LOG(INFO, "CK_INFO: subtable is in flush queue or dump queue",
        K(sub_table->pending_flush()), K(sub_table->GetID()), K(sub_table->pending_shrink()));
  } else {
    STDumpJob *dump_job = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, STDumpJob);
    if (IS_NULL(dump_job)) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "alloc memory for dump job failed", K(ret));
    } else {
      LogFileNumberSize file_number = alive_log_files_.back();
      uint64_t old_value = UINT64_MAX;
      if (allow_2pc()) {
        sub_table->mem()->set_log_containing_prepsec(file_number.number, old_value);
      } 
      const RecoveryPoint &last_rp = sub_table->mem()->get_recovery_point();
      if (last_rp.log_file_number_ == (int64_t)file_number.number || in_prep_log_ref_map(old_value)) {
        // not do dump,just wait next
        sub_table->mem()->RefLogContainingPrepSection(old_value);
        MOD_DELETE_OBJECT(STDumpJob, dump_job);
        alive_log_files_.begin()->getting_flushed = false;
      } else {
        sub_table->mem()->set_temp_min_prep_log(old_value); // invalid after dump
        SequenceNumber mem_last_seq = versions_->LastSequence();
        dump_job->sub_table_ = sub_table;
        dump_job->dump_max_seq_ = mem_last_seq;
        dump_job->dump_mem_ = sub_table->mem();

        RecoveryPoint rp;
        rp.seq_ = mem_last_seq;
        rp.log_file_number_ = file_number.number;
        sub_table->mem()->set_recovery_point(rp);
        sub_table->Ref();
        sub_table->set_pending_dump(true);
        dump_queue_.push_back(dump_job);
        SE_LOG(INFO, "CK_INFO: dump job info", K(sub_table->GetID()), K(mem_last_seq),
            K(rp), K(file_number.number), K(file_number.size), K(dump_queue_.size()));
        ++unscheduled_dumps_;
        do_dump = true;
      }
    }
  }
  return ret;
}

int DBImpl::trigger_switch_or_dump(ColumnFamilyData* cfd, WriteContext *write_context)
{
  mutex_.AssertHeld();
  int ret = Status::kOk;

  assert(write_context->all_sub_table_ != nullptr);
  if (nullptr != cfd) {
    uint64_t cur_mem_size = cfd->mem()->ApproximateMemoryUsage();
    if (cur_mem_size >= mutable_db_options_.dump_memtable_limit_size
        || cfd->imm()->NumNotFlushed() > 0) {
      if (FAILED(trigger_switch_memtable(cfd, write_context))) {
        SE_LOG(WARN, "failed to trigger switch memtable", K(ret), K(cfd->GetID()));
      }
    } else {
      bool do_dump = false;
      if (FAILED(build_dump_job(cfd, do_dump))) {
        SE_LOG(WARN, "failed to build dump job", K(ret));
      } else if (!do_dump) {
        if (FAILED(trigger_switch_memtable(cfd, write_context))) {
          SE_LOG(WARN, "failed to trigger switch memtable", K(ret), K(cfd->GetID()));
        }
      }
    }
  }
  return ret;
}

int DBImpl::trigger_switch_memtable(ColumnFamilyData *cf2switch, WriteContext *write_context) {
  int ret = Status::kOk;
  if (nullptr != cf2switch) {
    if (FAILED(switch_memtable(cf2switch, write_context, false))) {
      SE_LOG(ERROR, "Fail to switch memtable", K(ret), K(cf2switch->GetID()));
    } else {
      cf2switch->imm()->FlushRequested();
      SchedulePendingFlush(cf2switch);
    }
  } else {
    SE_LOG(WARN, "CK_INFO: trigger_switch_memtable pick a null cf to switch", KP(cf2switch));
  }
  return ret;
}

// depend on how long it takes, we need to consider if it's suitable in the
// write path or put it background
Status DBImpl::HandleTotalWriteBufferFull(WriteContext* write_context) {
  mutex_.AssertHeld();
  assert(write_context != nullptr);
  size_t trim_num = 0;
  size_t trim_size = 0;
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;

  if (trim_mem_flush_waited_ == kFlushDone) {
    // try to recycle some flushed memtables in order

    uint64_t start_time = env_->NowNanos();
    if (start_time < next_trim_time_) return Status(ret);

    SuperVersion *new_superversion = MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    if (new_superversion == nullptr) {
      SE_LOG(INFO, "Cannot allocate memory for superversion");
      return Status::kMemoryLimit;
    }

    size_t old_num = write_context->memtables_to_free_.size();
    while (!memtable_cleanup_queue_.empty() && trim_num == 0) {
      MemtableCleanupInfo& info = memtable_cleanup_queue_.front();
      ColumnFamilyData* cfd = info.cfd_;
      SequenceNumber seqno = info.first_seqno_;
      memtable_cleanup_queue_.pop_front();

      if (!cfd->IsDropped()) {
        // trim_num returns positive even when memtable is still referenced.
        // trim_num returns 0 if the older memtable is already trimmed.
        trim_num = cfd->imm()->TrimOlderThan(
            &write_context->memtables_to_free_, seqno);
        if (trim_num > 0) {
          write_context->superversions_to_free_.push_back(
              InstallSuperVersionAndScheduleWork(cfd, new_superversion));
        }
      }

      if (cfd->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, cfd);
      }
    }
    size_t new_num = write_context->memtables_to_free_.size();

    if (trim_num > 0) {
      for (size_t n = old_num; n < new_num; ++n) {
        MemTable* m = write_context->memtables_to_free_[n];
        trim_size += m->ApproximateMemoryAllocated();
      }
    } else {
      MOD_DELETE_OBJECT(SuperVersion, new_superversion);
    }

    uint64_t end_time = env_->NowNanos();
    __SE_LOG(INFO,
        "Trim history memtables: %" PRIu64 " / %" PRIu64 ", "
        "cleanup queue: %" PRIu64 ", "
        "trimmed num: %" PRIu64 ", size: %" PRIu64 ", time: %" PRIu64 ".\n",
        write_buffer_manager_->total_memory_usage(),
        write_buffer_manager_->total_buffer_size(),
        memtable_cleanup_queue_.size(),
        trim_num, trim_size, end_time - start_time);

    // memtable destruction is not done yet but on the way, stop trimming
    // for a while. for every 64MB to trim, one millisecond to quiesce
    if (trim_size > 0) {
      uint64_t period = trim_size / 64;
      if (end_time + period > next_trim_time_) {
        next_trim_time_ = end_time + period;
      }
    }

  } else if (flush_queue_.empty()) {
    // switch the oldest memtable in order to free its memory,
    // copied from HandleWriteBufferFull
    SubTable* sub_table_picked = nullptr;
    SequenceNumber seq_num_for_cf_picked = kMaxSequenceNumber;

    uint64_t start_time = env_->NowNanos();
    GlobalContext *global_ctx = nullptr;
    AllSubTable *all_sub_table = nullptr; 
    if (nullptr == (global_ctx = versions_->get_global_ctx())) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "global ctx must not nullptr", K(ret));
    } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
      SE_LOG(WARN, "fail to acquire all sub table", K(ret));
    } else if (nullptr == all_sub_table) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
    } else {
      SubTableMap &all_subtables = all_sub_table->sub_table_map_;
      SubTable *sub_table = nullptr;
      uint64_t seq = 0;
      for (auto iter = all_subtables.begin(); Status::kOk == ret && iter != all_subtables.end(); ++iter) {
        if (nullptr == (sub_table = iter->second)) {
          ret = Status::kCorruption;
          SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
        } else if (sub_table->IsDropped()) {
          //subtable has been dropped, do nothing
          SE_LOG(INFO, "subtable has been dropped", K(iter->first));
        } else if (sub_table->mem()->IsEmpty()) {
          //subtable's memtable is empty, do nothing
          SE_LOG(DEBUG, "subtable's memtable is empty", K(iter->first));
        } else {
          // We only consider active mem table, hoping immutable memtable is
          // already in the process of flushing.
          seq = sub_table->mem()->GetCreationSeq();
          if (nullptr == sub_table_picked || seq < seq_num_for_cf_picked) {
            sub_table_picked = sub_table;
            seq_num_for_cf_picked = seq;
          }
        }
      }
    }
    if (sub_table_picked != nullptr) {
      write_context->all_sub_table_ = all_sub_table;
      if(FAILED(switch_memtable(sub_table_picked, write_context, false))) {
        SE_LOG(WARN, "fail to switch memtable", K(ret));
      } else {
        sub_table_picked->imm()->FlushRequested();
        SchedulePendingFlush(sub_table_picked);
        MaybeScheduleFlushOrCompaction();
      }
    }
    uint64_t passed_time = env_->NowNanos() - start_time;

    __SE_LOG(INFO,
        "Switch/flush memtables: %" PRIu64 " / %" PRIu64 ","
        "time: %" PRIu64 ".\n",
        write_buffer_manager_->total_memory_usage(),
        write_buffer_manager_->total_buffer_size(),
        passed_time);
    //there will cover the error code, by design
    tmp_ret = ret;
    if (nullptr != global_ctx && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
      SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
    }
  } else {
    // flushing is ongoing, nothing to do here. do throttling if necessary?
  }

  advance_recovery_point_without_flush();

  if (trim_num == 0) {
    trim_mem_flush_waited_ = kFlushWaited;
  }

  return Status(ret);
}

Status DBImpl::HandleWriteBufferFull(WriteContext* write_context) {
  mutex_.AssertHeld();
  assert(write_context != nullptr);
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;

  // Before a new memtable is added in switch_memtable(),
  // write_buffer_manager_->ShouldFlush() will keep returning true. If another
  // thread is writing to another DB with the same write buffer, they may also
  // be flushed. We may end up with flushing much more DBs than needed. It's
  // suboptimal but still correct.
  __SE_LOG(INFO,
      "Flushing sub table with largest mem table size. Write buffer is "
      "using %" PRIu64 " bytes out of a total of %" PRIu64 ".",
      write_buffer_manager_->memory_usage(),
      write_buffer_manager_->buffer_size());
  // no need to refcount because drop is happening in write thread, so can't
  // happen while we're in the write thread
  SubTable* sub_table_picked = nullptr;
  SequenceNumber seq_num_for_cf_picked = kMaxSequenceNumber;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    SubTableMap &all_subtables = all_sub_table->sub_table_map_;
    SubTable *sub_table = nullptr;
    uint64_t seq = 0;
    for (auto iter = all_subtables.begin(); Status::kOk == ret && iter != all_subtables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else if (sub_table->IsDropped()) {
        //subtable has been dropped, do nothing
        SE_LOG(INFO, "subtable has been dropped", K(iter->first));
      } else if (sub_table->mem()->IsEmpty()) {
        //subtable's memtable is empty, do nothing
        SE_LOG(DEBUG, "subtable's memtable is empty", K(iter->first));
      } else {
        // We only consider active mem table, hoping immutable memtable is
        // already in the process of flushing.
        seq = sub_table->mem()->GetCreationSeq();
        if (nullptr == sub_table_picked || seq < seq_num_for_cf_picked) {
          sub_table_picked = sub_table;
          seq_num_for_cf_picked = seq;
        }
      }
    }

    if (sub_table_picked != nullptr) {
      write_context->all_sub_table_ = all_sub_table;
      if (FAILED(switch_memtable(sub_table_picked, write_context, false))) {
        SE_LOG(WARN, "fail to switch memtable", K(ret));
      } else {
        sub_table_picked->imm()->FlushRequested();
        SchedulePendingFlush(sub_table_picked);
        MaybeScheduleFlushOrCompaction();
      }
    }
  }
  
  //there will cover the error code, by design
  tmp_ret = ret;
  if (nullptr != global_ctx && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }

  advance_recovery_point_without_flush();

  return Status(ret);
}

uint64_t DBImpl::GetMaxTotalWalSize() const {
  se_assert(mutable_db_options_.max_total_wal_size > 0);
  return mutable_db_options_.max_total_wal_size;
}

int DBImpl::schedule_flush(WriteContext *write_context)
{
  mutex_.AssertHeld();
  int ret = Status::kOk;
  ColumnFamilyData *sub_table = nullptr;

  if (IS_NULL(write_context)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(write_context));
  } else {
    while (SUCCED(ret) && IS_NOTNULL(sub_table = flush_scheduler_.TakeNextColumnFamily())) {
      if (FAILED(switch_memtable(sub_table, write_context, false))) {
        SE_LOG(WARN, "fail to switch memtable", K(ret), "index_id", sub_table->GetID());
      } else if (sub_table->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, sub_table);
      }
    }
  }

  // try to advance recovery point, ignore error.
  advance_recovery_point_without_flush();

  return ret;
}

// TODO (Zhao Dongsheng): Empty memtable needn't switch.
int DBImpl::switch_memtable(ColumnFamilyData *sub_table, WriteContext *write_context, bool force_create_new_log)
{
  mutex_.AssertHeld();
  int ret = Status::kOk;
  SequenceNumber seq = 0;
  RecoveryPoint recovery_point;
  SuperVersion *new_super_version = nullptr;
  SuperVersion *retired_super_version = nullptr;

  // The switch memtable operation acts as a barrier in the write process and requires waiting for
  // all tasks in the transaction commit pipeline to complete.
  this->wait_all_active_thread_exit();
  seq = versions_->LastSequence();

  if (!bg_error_.ok() || pipline_global_error_flag_.load()) {
    if (bg_error_.ok()) {
      bg_error_ = Status::Corruption("pipeline error while switch memtable");
    }
    ret = Status::kCorruption;
    SE_LOG(ERROR, "found bg_error when switch memtable", K(ret), KE(bg_error_.code()), K_(pipline_global_error_flag));
  } else if (UNLIKELY(!version_sliding_window_map_.empty()) ||
             UNLIKELY(0 != pipline_manager_.get_copy_log_job_num()) ||
             UNLIKELY(0 != pipline_manager_.get_flush_log_job_num()) ||
             UNLIKELY(0 != pipline_manager_.get_memtable_job_num()) ||
             UNLIKELY(0 != pipline_manager_.get_commit_job_num()) ||
             UNLIKELY(versions_->LastSequence() != versions_->LastAllocatedSequence())) {
    ret = Status::kCorruption;
    SE_LOG(ERROR, "the pipeline status is corrupted.", K(ret),
        K(version_sliding_window_map_.size()),
        "copy_log_job_num", pipline_manager_.get_copy_log_job_num(),
        "flush_log_job_num", pipline_manager_.get_flush_log_job_num(),
        "write_memtable_job_num", pipline_manager_.get_memtable_job_num(),
        "commit_job_num", pipline_manager_.get_commit_job_num(),
        "last_sequence", versions_->LastSequence(), "last_allocated_sequence", versions_->LastAllocatedSequence());
  } else if (FAILED(update_wal_writer(force_create_new_log))) {
    SE_LOG(WARN, "fail to update wal writer", K(ret));
  } else {
    SE_LOG(INFO, "success to switch memtable", "index_id", sub_table->GetID(),
       K_(logfile_number), "num_imm_unflushed", sub_table->imm()->NumNotFlushed(),
       "num_imm_flushed", sub_table->imm()->NumFlushed(),
       "num_entries", sub_table->mem()->num_entries(), KE(write_context->type_));
    recovery_point.log_file_number_ = logfile_number_;
    recovery_point.seq_ = seq;
    if (FAILED(sub_table->update_active_memtable(recovery_point, &(write_context->memtables_to_free_)))) {
      SE_LOG(WARN, "fail to update active memtable", K(ret), K(recovery_point));
    } else if (IS_NULL(new_super_version = MOD_NEW_OBJECT(ModId::kSuperVersion, SuperVersion))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for new super version", K(ret));
    } else {
      // (Zhao Dongsheng) The 'db_mutex' will be unlocked and locked
      // at an extreme point in 'InstallSuperVersionAndScheduleWork',
      // so this assert condition needs to be placed before it.
      se_assert((seq == versions_->LastAllocatedSequence()) && (seq == versions_->LastSequence()));

      if (IS_NOTNULL(retired_super_version = InstallSuperVersionAndScheduleWork(sub_table, new_super_version))) {
        write_context->superversions_to_free_.push_back(retired_super_version);
      }
    }
  }


  return ret;
}

int DBImpl::advance_recovery_point_without_flush()
{
  mutex_.AssertHeld();
  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  ColumnFamilyData *sub_table = nullptr;
  RecoveryPoint recovery_point;

  this->wait_all_active_thread_exit();
  recovery_point.log_file_number_ = logfile_number_;
  recovery_point.seq_ = versions_->LastSequence();
  if (FAILED(get_all_sub_table(all_sub_table, global_ctx))) {
    SE_LOG(WARN, "get all subtale failed", K(ret));
  } else {
    SubTableMap &all_subtables = all_sub_table->sub_table_map_;
    for (auto iter = all_subtables.begin(); SUCCED(ret) && all_subtables.end() != iter; ++iter) {
      if (IS_NULL(sub_table = iter->second)) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, the subtable must not be nullptr", K(ret), "index_id", sub_table->GetID());
      } else if ((0 == sub_table->mem()->GetFirstSequenceNumber()) &&
                 (0 == sub_table->imm()->NumNotFlushed())) {
        sub_table->set_recovery_point(recovery_point);
        // TODO (Zhao Dongsheng) : need insert into dump list here?
        versions_->GetColumnFamilySet()->insert_into_dump_list(sub_table);
        sub_table->mem()->SetCreationSeq(recovery_point.seq_);
      }
    }
  }

  int tmp_ret = ret;
  if (IS_NOTNULL(global_ctx) && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    SE_LOG(WARN, "fail to release all subtables", K(ret));
  }

  se_assert(pipline_global_error_flag_.load() || 
            ((versions_->LastAllocatedSequence() == versions_->LastSequence()) &&
            (recovery_point.seq_ == versions_->LastSequence())));

  return ret;  
}

size_t DBImpl::GetWalPreallocateBlockSize(uint64_t write_buffer_size) const {
  mutex_.AssertHeld();
  size_t bsize = write_buffer_size / 10 + write_buffer_size;
  // Some users might set very high write_buffer_size and rely on
  // max_total_wal_size or other parameters to control the WAL size.
  if (mutable_db_options_.max_total_wal_size > 0) {
    bsize = std::min<size_t>(bsize, mutable_db_options_.max_total_wal_size);
  }
  if (immutable_db_options_.db_total_write_buffer_size > 0) {
    bsize = std::min<size_t>(bsize, immutable_db_options_.db_total_write_buffer_size);
  }
  if (immutable_db_options_.write_buffer_manager &&
      immutable_db_options_.write_buffer_manager->enabled()) {
    bsize = std::min<size_t>(
        bsize, immutable_db_options_.write_buffer_manager->buffer_size());
  }

  return bsize;
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& value) {
  // Pre-allocate size of write batch conservatively.
  // 8 bytes are taken by header, 4 bytes for count, 1 byte for type,
  // and we allocate 11 extra bytes for key length, as well as value length.
  WriteBatch batch(key.size() + value.size() + 24);
  batch.Put(column_family, key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, ColumnFamilyHandle* column_family,
                  const Slice& key) {
  WriteBatch batch;
  batch.Delete(column_family, key);
  return Write(opt, &batch);
}

Status DB::SingleDelete(const WriteOptions& opt,
                        ColumnFamilyHandle* column_family, const Slice& key) {
  WriteBatch batch;
  batch.SingleDelete(column_family, key);
  return Write(opt, &batch);
}

}
}  // namespace smartengine
