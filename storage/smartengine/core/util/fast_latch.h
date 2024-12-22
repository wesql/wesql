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

#include "memory/thread_local_store.h"
#include "util/macro_utils.h"
#include "util/misc_utility.h"
#include "util/status.h"

namespace smartengine
{
namespace util
{

class FastRWLatch
{
  union LatchContent
  {
    struct {
      uint32_t rd_wait_;
      uint32_t wr_wait_;
    };
    uint64_t lock_;
  };

public:
  FastRWLatch()
  {
    lc_.lock_ = 0;
  }

  int try_rdlock()
  {
    int ret = common::Status::kTryAgain;
    uint64_t lock = 0;
    bool conflict = false;
    while (true) {
      lock = lc_.lock_;
      if (common::Status::kOk != (ret = try_rdlock_low(lock, lock + 1, false, conflict))) {
        if (conflict) {
          // Do not retry, now is locked by writer or has waiter
          break;
        } else if (common::Status::kTryAgain != ret) {
          // Error happened
          break;
        } else {
          // Retry, now is locked by reader
        }
      } else {
        // Success
        ret = common::Status::kOk;
        break;
      }
      PAUSE();
    }
    return ret;
  }

  int try_wrlock()
  {
    uint64_t lock = lc_.lock_;
    bool dummy = false;
    return try_wrlock_low(lock, lock | WRITE_MASK, false, dummy);
  }

  int rdlock()
  {
    int ret = common::Status::kOk;
    int64_t i = 0;
    bool conflict = false;
    uint64_t lock = 0;

    // Try lock for MAX_SPIN_CNT times
    for (i = 0; SUCCED(ret) && i < MAX_SPIN_CNT; i++) {
      lock = lc_.lock_;
      if (SUCCED(try_rdlock_low(lock, lock + 1, false, conflict))) {
        break;
      } else if (common::Status::kTryAgain == ret) {
        // Retry, regardless of the conflict
        ret = common::Status::kOk;
      }
      PAUSE();
    }

    if (common::Status::kOk != ret) {
      // Error happened
    } else if (i < MAX_SPIN_CNT) {
      // Success
    } else if (FAILED(rd_wait())) { // Need wait until success
      SE_LOG(WARN, "Failed to rd_wait", K(ret));
    } else {
      // Success
    }
    return ret;
  }

  int wrlock()
  {
    int ret = common::Status::kOk;
    int64_t i = 0;
    bool conflict = false;
    uint64_t lock = 0;

    // Try lock for MAX_SPIN_CNT times
    for (i = 0; SUCCED(ret) && i < MAX_SPIN_CNT; i++) {
      lock = lc_.lock_;
      if (SUCCED(try_wrlock_low(lock, lock | WRITE_MASK, false, conflict))) {
        break;
      } else if (common::Status::kTryAgain == ret) {
        // Retry, regardless of the conflict
        ret = common::Status::kOk;
      }
      PAUSE();
    }

    if (common::Status::kOk != ret) {
      // Error happened
    } else if (i < MAX_SPIN_CNT) {
      // Success
    } else if (FAILED(wr_wait())) { // Need wait until success
      SE_LOG(WARN, "Failed to wr_wait", K(ret));
    } else {
      // Success
    }
    return ret;
  }

  int unlock()
  {
    int ret = common::Status::kOk;
    uint64_t lock = ATOMIC_LOAD(&lc_.lock_);
    if (0 != (lock & WRITE_MASK)) {
      // Locked by writer, remove the WRITE_MASK
      if (get_wid(lock) != memory::get_tc_tid()) {
        ret = common::Status::kErrorUnexpected;
        SE_LOG(ERROR, "Writer is not the current thread",
            K(ret), K(lock), K(get_wid(lock)), K(memory::get_tc_tid())); 
        BACKTRACE(ERROR, "Writer is not the current thread");
      } else if (0 != (WAIT_MASK & ATOMIC_SET(&lc_.lock_, 0))) {
        // Has waiters
        if (futex_wake(&lc_.rd_wait_, INT32_MAX) > 0) {
          // Waked up all readers
        } else {
          // If there is no reader waiting, try to wake up a writer
          futex_wake(&lc_.wr_wait_, 1);
        }
      }
    } else if ((lock & (~WAIT_MASK)) > 0) {
      // Locked by reader, decrease the reader count
      lock = ATOMIC_SUB_FETCH(&lc_.lock_, 1);
      if (WAIT_MASK == lock && ATOMIC_BCAS(&lc_.lock_, lock, 0)) {
        // Has waiters, try to wake up a writer
        futex_wake(&lc_.wr_wait_, 1);
      }
    } else {
      ret = common::Status::kErrorUnexpected;
      SE_LOG(ERROR, "Unexpected lock value", K(ret), K(lock));
    }
    return ret;
  }

private:
  int try_rdlock_low(const uint64_t lock,
                     const uint64_t new_lock,
                     const bool ignore_wait,
                     bool &conflict)
  {
    int ret = common::Status::kTryAgain;
    if ((0 == (lock & WRITE_MASK)) && (ignore_wait || (0 == (lock & WAIT_MASK)))) {
      if ((lock & (~WAIT_MASK)) >= MAX_READ_LOCK_CNT) {
        conflict = true;
        ret = common::Status::kErrorUnexpected;
        SE_LOG(ERROR, "Too many read locks", K(ret), K(lock));
      } else {
        conflict = false;
        // Mark a reader by adding reader count
        if (ATOMIC_BCAS(&lc_.lock_, lock, new_lock)) {
          ret = common::Status::kOk;
        }
      }
    } else {
      conflict = true;
    }
    return ret;
  }

  int try_wrlock_low(const uint64_t lock,
                     const uint64_t new_lock,
                     const bool ignore_wait,
                     bool &conflict)
  {
    int ret = common::Status::kTryAgain;
    const uint64_t uid = static_cast<uint64_t>(memory::get_tc_tid()) << 32;
    if (UNLIKELY(uid >= MAX_UID)) {
      ret = common::Status::kErrorUnexpected;
      SE_LOG(ERROR, "uid is larger than MAX_UID", K(ret), K(uid), K(memory::get_tc_tid()));
    } else if (0 == lock || (ignore_wait && (WAIT_MASK == lock))) {
      conflict = false;
      // Mark a writer by adding WRITE_MASK and the tid of current thread
      if (ATOMIC_BCAS(&lc_.lock_, lock, new_lock | uid)) {
        ret = common::Status::kOk;
      }
    } else {
      conflict = true;
    }
    return ret;
  }

  int rd_wait()
  {
    int ret = common::Status::kTryAgain;
    uint64_t lock = 0;
    bool conflict = false;
    while (true) {
      lock = lc_.lock_;
      if (SUCCED(try_rdlock_low(lock, (lock + 1) | WAIT_MASK, true, conflict))) {
        // Success
        break;
      } else if (common::Status::kTryAgain == ret) {
        // Add WAIT_MASK and go to wait
        if (conflict && ATOMIC_BCAS(&lc_.lock_, lock, (lock | WAIT_MASK))) {
          break;
        }
      } else {
        SE_LOG(WARN, "Failed to try rdlock", K(ret));
        break;
      }
      PAUSE();
    }
    // Need to wait
    while (common::Status::kTryAgain == ret) {
      futex_wait(&lc_.rd_wait_, get_rd_wait(lock | WAIT_MASK), nullptr);
      conflict = false;
      while (!conflict) {
        lock = lc_.lock_;
        if (SUCCED(try_rdlock_low(lock, (lock + 1) | WAIT_MASK, true, conflict))) {
          // Success
          break;
        } else if (common::Status::kTryAgain != ret) {
          SE_LOG(WARN, "Failed to try rdlock", K(ret));
          break;
        }
        PAUSE();
      }
    }
    return ret;
  }

  int wr_wait()
  {
    int ret = common::Status::kTryAgain;
    uint64_t lock = 0;
    bool conflict = false;
    while (true) {
      lock = lc_.lock_;
      if (SUCCED(try_wrlock_low(lock, (lock | WRITE_MASK | WAIT_MASK), true, conflict))) {
        // Success
        break;
      } else if (common::Status::kTryAgain == ret) {
        // Add WAIT_MASK and go to wait
        if (conflict && ATOMIC_BCAS(&lc_.lock_, lock, (lock | WAIT_MASK))) {
          break;
        }
      } else {
        SE_LOG(WARN, "Failed to try wrlock", K(ret));
        break;
      }
      PAUSE();
    }
    // Need to wait
    while (common::Status::kTryAgain == ret) {
      futex_wait(&lc_.wr_wait_, get_wr_wait(lock | WAIT_MASK), nullptr);
      conflict = false;
      while (!conflict) {
        lock = lc_.lock_;
        if (SUCCED(try_wrlock_low(lock, (lock | WRITE_MASK | WAIT_MASK), true, conflict))) {
          // Success
          break;
        } else if (common::Status::kTryAgain != ret) {
          SE_LOG(WARN, "Failed to try wrlock", K(ret));
          break;
        }
        PAUSE();
      }
    }
    return ret;
  }

  uint32_t get_rd_wait(const uint64_t lock)
  {
    return static_cast<uint32_t>(lock & RD_WAIT_MASK);
  }

  uint32_t get_wr_wait(const uint64_t lock)
  {
    return static_cast<uint32_t>((lock & WR_WAIT_MASK) >> 32);
  }

  int32_t get_wid(const uint64_t lock)
  {
    return static_cast<int32_t>((lock & ~(WAIT_MASK | WRITE_MASK)) >> 32);
  }

  // futex
  #define futex(...) syscall(SYS_futex, __VA_ARGS__)
  inline int futex_wait(void *p, int val, const timespec *timeout)
  {
    int ret = 0;
    if (0 != futex((int *)p, FUTEX_WAIT_PRIVATE, val, timeout, NULL, 0)) {
      ret = errno;
    }
    return ret;
  }

  inline int64_t futex_wake(void *p, int val)
  {
    return futex((int *)p, FUTEX_WAKE_PRIVATE, val, NULL, NULL, 0);
  }


private:
  static const uint64_t WRITE_MASK = (1ULL << 31) | (1ULL << 63);
  static const uint64_t WAIT_MASK = (1ULL << 30) | (1ULL << 62);
  static const uint64_t MAX_READ_LOCK_CNT = 1ULL << 29;
  static const uint64_t MAX_UID = 1ULL << 61;
  static const int64_t MAX_SPIN_CNT = 100;
  static const uint64_t RD_WAIT_MASK = ~((~0ULL) << 32);
  static const uint64_t WR_WAIT_MASK = (~0ULL) << 32;

  LatchContent lc_;
};

class FastRDGuard
{
public:
  explicit FastRDGuard(FastRWLatch *lock) : lock_(lock)
  {
    int ret = common::Status::kOk;
    if (FAILED(lock_->rdlock())) {
      SE_LOG(ERROR, "Failed to rdlock", K(ret));
      BACKTRACE(ERROR, "Failed to rdlock");
    }
  }
  ~FastRDGuard()
  {
    int ret = common::Status::kOk;
    if (FAILED(lock_->unlock())) {
      SE_LOG(ERROR, "Failed to unlock", K(ret));
      BACKTRACE(ERROR, "Failed to unlock");
    }
  }
private:
  FastRWLatch *lock_;
};

class FastWRGuard
{
public:
  explicit FastWRGuard(FastRWLatch *lock) : lock_(lock)
  {
    int ret = common::Status::kOk;
    if (FAILED(lock_->wrlock())) {
      SE_LOG(ERROR, "Failed to wrlock", K(ret));
      BACKTRACE(ERROR, "Failed to wrlock");
    }
  }
  ~FastWRGuard()
  {
    int ret = common::Status::kOk;
    if (FAILED(lock_->unlock())) {
      SE_LOG(ERROR, "Failed to unlock", K(ret));
      BACKTRACE(ERROR, "Failed to wrlock");
    }
  }
private:
  FastRWLatch *lock_;
};

} // namespace cache
} // namespace smartengine
