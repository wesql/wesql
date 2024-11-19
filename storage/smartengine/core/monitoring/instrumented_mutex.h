//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "env/env.h"
#include "monitoring/statistics.h"

namespace smartengine
{
namespace monitor
{

class InstrumentedCondVar;

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex
{
 public:
  explicit InstrumentedMutex(uint64_t *backtrace_limit_nano,
                             util::Env *env,
                             bool adaptive = false)
      : mutex_(adaptive),
        env_(env),
        start_nano_(0),
        backtrace_limit_nano_(backtrace_limit_nano)
  {}

  explicit InstrumentedMutex()
      : mutex_(false),
        env_(nullptr),
        start_nano_(0),
        backtrace_limit_nano_(nullptr)
  {}


  void Lock();
  void Unlock();

  void AssertHeld() { mutex_.AssertHeld(); }

 private:
  void LockInternal();
  friend class InstrumentedCondVar;
  port::Mutex mutex_;
  util::Env* env_;
  uint64_t start_nano_;
  uint64_t *backtrace_limit_nano_;
};

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutexLock
{
 public:
  explicit InstrumentedMutexLock(InstrumentedMutex* mutex) : mutex_(mutex) { mutex_->Lock(); }

  ~InstrumentedMutexLock() { mutex_->Unlock(); }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexLock(const InstrumentedMutexLock&) = delete;
  void operator=(const InstrumentedMutexLock&) = delete;
};

class InstrumentedCondVar
{
 public:
  explicit InstrumentedCondVar(InstrumentedMutex* instrumented_mutex)
      : cond_(&(instrumented_mutex->mutex_))
  {}

  void Wait();

  bool TimedWait(uint64_t abs_time_us);

  void Signal() { cond_.Signal(); }

  void SignalAll() { cond_.SignalAll(); }

 private:
  void WaitInternal();
  bool TimedWaitInternal(uint64_t abs_time_us);
  port::CondVar cond_;
};

}  // namespace monitor
}  // namespace smartengine
