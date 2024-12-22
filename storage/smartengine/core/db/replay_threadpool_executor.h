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

#pragma once
#include <atomic>
#include <list>
#include <thread>
#include "port/port_posix.h"
#include "util/callback_util.h"
#include "util/lock_free_fixed_queue.h"
#include "memory/allocator.h"

namespace smartengine {

namespace util {
class Env;
}

namespace db {

using util::FixedQueue;
class CallbackDeleter {
  public:
    void operator() (util::CallbackBase *func) {
      if (func != nullptr) {
        delete func;
      }
    }
};

template<typename Deleter = CallbackDeleter>
class ReplayThreadPoolExecutor {
public:
  struct Task {
    explicit Task(util::CallbackBase *_func,
                  memory::SimpleAllocator *_arena = nullptr,
                  std::chrono::milliseconds _to = std::chrono::milliseconds(0),
                  util::CallbackBase *_on_timeout = nullptr)
      : func(_func),
        arena(_arena),
        timeout(_to),
        on_timeout(_on_timeout),
        deleter(Deleter()) {}
    virtual ~Task() {
      if (func != nullptr) {
        deleter(func, arena);
      }
      if (on_timeout != nullptr) {
        deleter(on_timeout, arena);
      }
    }
    util::CallbackBase *func;
    memory::SimpleAllocator *arena;
    std::chrono::milliseconds timeout;
    util::CallbackBase *on_timeout;
    Deleter deleter;
  };

  explicit ReplayThreadPoolExecutor(int max_queue_size)
    : task_queue_(nullptr),
      task_queue_buffer_(nullptr),
      max_queue_size_(max_queue_size),
      inited_(false),
      stop_(false),
      submit_task_num_(0),
      executed_task_num_(0),
      worker_num_(0),
      has_error_(false),
      need_suspend_(false),
      suspend_thread_num_(0),
      stopped_thread_num_(0),
      replay_mutex_(false),
      replay_cv_(&replay_mutex_) {}

  virtual ~ReplayThreadPoolExecutor() {}
  /* stop thread pool to accept new tasks
   * after this method called, new task submitted into threadpool won't be scheduled
   * but submitted tasks will still run unless some error occurred
   * threads will exit after executing tasks left in task_queue
   * */
  void stop();

  size_t get_waitting_task_num();

  int initialize(size_t worker_num);

  /* this method will wait for 'submit_log_interval' and print warning log if thread pool is still full
   */
  int submit_task(util::CallbackBase *func,
                  memory::SimpleAllocator *arena = nullptr,
                  util::Env *env = nullptr);

  /* wait for all threads stopping and destroy current thread pool
   * this method should be called after method stop(), otherwise will return error
   * */
  int destroy();
  bool stopped() const {
    return stop_.load();
  }
  void set_error();
  bool has_error() const {
    return has_error_.load();
  }

  uint64_t get_submit_task_num() const {
    return submit_task_num_.load();
  }
  uint64_t get_executed_task_num() const {
    return executed_task_num_.load();
  }
  uint64_t get_worker_num() const {
    return worker_num_.load();
  }
  void clear_executed_task_num() {
    executed_task_num_.store(0);
  }
  int suspend();
  int resume();
  bool is_suspend() const {
    return need_suspend_.load();
  }
  int get_suspend_thread_num() const {
    return suspend_thread_num_.load();
  }
  int get_stopped_thread_num() const {
    return stopped_thread_num_.load();
  }

#ifndef NDEBUG
  void set_queue_full(bool full) {
    queue_full_.store(full);
  }
  bool queue_full() const {
    return queue_full_.load();
  }
#endif

private:
  struct Worker {
    explicit Worker() {}
    std::thread handle;
  };
  virtual void thread_run(Worker& worker);
  int init_workers(size_t worker_num);
  void check_and_suspend_if_need();

private:
  util::FixedQueue<Task>* task_queue_;
  char* task_queue_buffer_;
  int max_queue_size_;
  bool inited_;
  std::atomic_bool stop_;
  std::list<Worker> worker_list_;
  std::atomic_uint64_t submit_task_num_;
  std::atomic_uint64_t executed_task_num_;
  std::atomic_int worker_num_;
  std::atomic_bool has_error_;

  // for switching memtable
  std::atomic_bool need_suspend_;
  std::atomic_int suspend_thread_num_;
  std::atomic_int stopped_thread_num_;
  port::Mutex replay_mutex_;
  port::CondVar replay_cv_;

  // thread pool will print a warning log if wait queue not full for more than 40000 round
  static const uint64_t WAIT_QUEUE_NOT_FULL_LOG_ROUND = 40000;
  // current thread will sleep for 1ms if waitting for task queue available for more than 8 round
  static const uint64_t WAIT_QUEUE_SLEEP_ROUND = 8;

#ifndef NDEBUG
  std::atomic_bool queue_full_ {false};
  std::atomic_uint64_t empty_ {0};
  std::atomic_uint64_t full_ {0};
#endif
};
} // namespace db
} // namespace smartengine
