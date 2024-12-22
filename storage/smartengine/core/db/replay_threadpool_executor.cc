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

#include "db/replay_threadpool_executor.h"
#include <thread>
#include "util/mutexlock.h"
#include "logger/log_module.h"
#include "util/sync_point.h"
#include "util/status.h"

namespace smartengine {
using namespace common;

namespace db {
using util::FixedQueue;

template<typename Deleter>
int ReplayThreadPoolExecutor<Deleter>::initialize(size_t worker_num) {
  int ret = Status::kOk;
  if (UNLIKELY(inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ReplayThreadPoolExecutor init twice", K(ret));
  } else if (worker_num > 1024) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "ReplayThreadPoolExecutor init failed", K(ret), K(worker_num));
  } else if (FAILED(util::init_lock_free_queue(max_queue_size_,
          task_queue_buffer_, task_queue_))) {
    SE_LOG(WARN, "replay task queue init failed", K(ret));
  } else if (FAILED(init_workers(worker_num))) {
    SE_LOG(WARN, "replay task workers init failed", K(ret));
  } else {
    submit_task_num_.store(0);
    executed_task_num_.store(0);
    suspend_thread_num_.store(0);
    stopped_thread_num_.store(0);
    worker_num_.store(worker_num);
    need_suspend_.store(false);
    has_error_.store(false);
    stop_.store(false);
    inited_ = true;
  }
  return ret;
}

template<typename Deleter>
int ReplayThreadPoolExecutor<Deleter>::init_workers(size_t worker_num) {
  int ret = Status::kOk;
  for (size_t i = 0; i < worker_num; ++i) {
    worker_list_.emplace_back();
  }
  for (auto& w : worker_list_) {
    w.handle = std::thread(
        std::bind(&ReplayThreadPoolExecutor::thread_run, this, std::ref(w)));
  }
  return ret;
}

template<typename Deleter>
void ReplayThreadPoolExecutor<Deleter>::stop() {
  if (UNLIKELY(!inited_)) {
    SE_LOG(WARN, "not inited");
  } else { 
    bool expect = false;
    if (stop_.compare_exchange_strong(expect, true)) {
      for (auto& w __attribute__((unused)) : worker_list_) {
        while (!has_error_.load()) {
          auto ret = submit_task(nullptr);
          if (ret != Status::kOk && ret != Status::kOverLimit) {
            SE_LOG(WARN, "failed to submit stop stask", K(ret));
            set_error();
            break;
          } else if (ret == Status::kOverLimit) {
            std::this_thread::yield(); // queue is full, retry submit
            continue;
          } else {
            break; // submit success
          }
        }
      }
    } else {
      SE_LOG(WARN, "ReplayThreadPoolExecutor is stopping, skip it");
    }
  }
}

template<typename Deleter>
int ReplayThreadPoolExecutor<Deleter>::submit_task(util::CallbackBase *func,
                                                   memory::SimpleAllocator *arena,
                                                   util::Env *env) {
  int ret = Status::kOk;
  Task *task = nullptr;
  if (UNLIKELY(!inited_)) {
    Deleter deleter;
    deleter(func, arena);
    ret = Status::kNotInit;
  } else if (has_error_.load()) {
    Deleter deleter;
    deleter(func, arena);
    ret = Status::kCorruption;
  } else if (IS_NULL(task = MOD_NEW_OBJECT(memory::ModId::kRecovery, Task, func, arena))) {
    Deleter deleter;
    deleter(func, arena);
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for replay_threadpool task");
  } else {
    bool stop_task = (func == nullptr);
    uint64_t full_queue_time = 0;
    while(SUCCED(ret)) {
      if (UNLIKELY(full_queue_time > WAIT_QUEUE_NOT_FULL_LOG_ROUND)) {
        SE_LOG(WARN, "wait thread pool not full for too long");
      }
#ifndef NDEBUG
      int push_ret = e_OK;
      TEST_SYNC_POINT_CALLBACK("ReplayThreadPoolExecutor::submit_task::queue_full", this);
      if (queue_full_.load()) {
        push_ret = e_SIZE_OVERFLOW;
      } else {
        push_ret = task_queue_->push(task);
      }
#else
      int push_ret = task_queue_->push(task);
#endif
      if (UNLIKELY(push_ret != e_OK)) {
        if (push_ret == e_SIZE_OVERFLOW) {
#ifndef NDEBUG
          full_.fetch_add(1);
#endif
          SE_LOG(DEBUG, "submit task into task_queue failed, queue_full, will try again");
          if (++full_queue_time >= WAIT_QUEUE_SLEEP_ROUND) {
            usleep(1000);
          } else {
            std::this_thread::yield();
          }
        } else {
          ret = Status::kErrorUnexpected;
          MOD_DELETE_OBJECT(Task, task);
          break;
        }
      } else {
        full_queue_time = 0;
        if (!stop_task) {
          submit_task_num_.fetch_add(1);
        }
        break;
      }
    }
  }
  return ret;
}

template<typename Deleter>
int ReplayThreadPoolExecutor<Deleter>::destroy() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "not inited", K(ret));
  } else if (UNLIKELY(!stop_.load())) {
    ret = Status::kBusy;
    SE_LOG(WARN, "not stopped", K(ret));
  } else {
    auto tmp_list(std::move(worker_list_));
    worker_num_.store(0);
    for (auto& w : tmp_list) {
      w.handle.join();
    }
#ifndef NDEBUG
    SE_LOG(INFO, "thread pool stopped", K(submit_task_num_),
                    K(executed_task_num_), K(empty_.load()), K(full_.load()));
#else
    SE_LOG(INFO, "thread pool stopped", "submit_task_num", submit_task_num_.load(),
                    "executed_task_num", executed_task_num_.load());
#endif
    if (nullptr != task_queue_) {
      while (task_queue_->get_total() != 0) {
        Task* task = nullptr;
        task_queue_->pop(task);
        MOD_DELETE_OBJECT(Task, task);
      }
      delete task_queue_;
      task_queue_ = nullptr;
    }
    if (nullptr != task_queue_buffer_) {
      free(task_queue_buffer_);
      task_queue_buffer_ = nullptr;
    }
    inited_ = false;
  }
  return ret;
}

template<typename Deleter>
void ReplayThreadPoolExecutor<Deleter>::set_error() {
  if (UNLIKELY(!inited_)) return;
  util::MutexLock lock_guard(&replay_mutex_);
  has_error_.store(true);
  replay_cv_.SignalAll();
#ifdef NDEBUG
  abort();
#endif
}

template<typename Deleter>
size_t ReplayThreadPoolExecutor<Deleter>::get_waitting_task_num() {
  if (UNLIKELY(!inited_)) {
    SE_LOG(WARN, "not inited");
    return 0;
  } else {
    size_t waitting_task_num = 0;
    waitting_task_num += task_queue_->get_total();
    return waitting_task_num;
  }
}

template<typename Deleter>
int ReplayThreadPoolExecutor<Deleter>::suspend() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    SE_LOG(WARN, "not inited");
    ret = Status::kNotInit;
  } else if (has_error_.load()) {
    SE_LOG(WARN, "has error");
    ret = Status::kCorruption;
  } else {
    need_suspend_.store(true);
  }
  return ret;
}

template<typename Deleter>
int ReplayThreadPoolExecutor<Deleter>::resume() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    SE_LOG(WARN, "not inited");
    ret = Status::kNotInit;
  } else if (has_error_.load()) {
    SE_LOG(WARN, "has error");
    ret = Status::kCorruption;
  } else {
    bool expect = true;
    if (need_suspend_.compare_exchange_strong(expect, false)) {
      util::MutexLock lock_guard(&replay_mutex_);
      replay_cv_.SignalAll();
    } else {
      SE_LOG(INFO, "need_suspend_ is false");
    }
  }
  return ret;
}

template<typename Deleter>
void ReplayThreadPoolExecutor<Deleter>::check_and_suspend_if_need() {
  if (need_suspend_.load()) {
    util::MutexLock lock_guard(&replay_mutex_);
    suspend_thread_num_.fetch_add(1);
    while (need_suspend_.load() && !has_error_.load()) {
      replay_cv_.Wait();
    }
    suspend_thread_num_.fetch_sub(1);
  }
}

template<typename Deleter>
void ReplayThreadPoolExecutor<Deleter>::thread_run(Worker& worker) {
  Task* task = nullptr;
  uint64_t empty_queue_time = 0;
  while (!has_error_.load()) {
    check_and_suspend_if_need();
    int ret = task_queue_->pop(task);
    if (ret != e_OK && ret != e_ENTRY_NOT_EXIST) {
      util::MutexLock lock_guard(&replay_mutex_);
      has_error_.store(true);
      replay_cv_.SignalAll();
      SE_LOG(WARN, "pop task from task_queue failed", K(ret));
      continue;
    }
    if UNLIKELY((e_ENTRY_NOT_EXIST == ret)) {
      if (++empty_queue_time >= WAIT_QUEUE_SLEEP_ROUND) {
        usleep(1000);
      } else {
        std::this_thread::yield();
      }
#ifndef NDEBUG
      empty_.fetch_add(1);
#endif
      continue;
    }
    empty_queue_time = 0;
    if (task != nullptr && task->func != nullptr) {
      task->func->run();
      executed_task_num_.fetch_add(1);
      MOD_DELETE_OBJECT(Task, task);
    } else { // get a null task means current thread should exit
      MOD_DELETE_OBJECT(Task, task);
      break;
    }
  }
  stopped_thread_num_.fetch_add(1);
 // avoid recovery main thread wait for all threads suspend forever when start
 // switching memtable after threadpool stopped
  suspend_thread_num_.fetch_add(1);
}

} // namespace db
} // namespace smartengine
