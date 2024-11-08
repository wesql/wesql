// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>

#include "db/db.h"
#include "env/env.h"
#include "monitoring/thread_status_updater.h"

namespace smartengine {
namespace db {
class ColumnFamilyData;
}
namespace monitor {

// The static utility class for updating thread-local status.
//
// The thread-local status is updated via the thread-local cached
// pointer thread_updater_local_cache_.  During each function call,
// when ThreadStatusUtil finds thread_updater_local_cache_ is
// left uninitialized (determined by thread_updater_initialized_),
// it will tries to initialize it using the return value of
// Env::GetThreadStatusUpdater().  When thread_updater_local_cache_
// is initialized by a non-null pointer, each function call will
// then update the status of the current thread.  Otherwise,
// all function calls to ThreadStatusUtil will be no-op.
class ThreadStatusUtil {
 public:
  // Register the current thread for tracking.
  static void RegisterThread(const util::Env* env,
                             util::ThreadStatus::ThreadType thread_type);

  // Unregister the current thread.
  static void UnregisterThread();

  // Create an entry in the global ColumnFamilyInfo table for the
  // specified column family.  This function should be called only
  // when the current thread does not hold db_mutex.
  static void NewColumnFamilyInfo(const db::DB* db,
                                  const db::ColumnFamilyData* cfd,
                                  const std::string& cf_name,
                                  const util::Env* env);

  // Erase the ConstantColumnFamilyInfo that is associated with the
  // specified ColumnFamilyData.  This function should be called only
  // when the current thread does not hold db_mutex.
  static void EraseColumnFamilyInfo(const db::ColumnFamilyData* cfd);

  // Erase all ConstantColumnFamilyInfo that is associated with the
  // specified db instance.  This function should be called only when
  // the current thread does not hold db_mutex.
  static void EraseDatabaseInfo(const db::DB* db);

  // Update the thread status to indicate the current thread is doing
  // something related to the specified column family.
  static void SetColumnFamily(const db::ColumnFamilyData* cfd,
                              const util::Env* env,
                              bool enable_thread_tracking);

  static void SetThreadOperation(util::ThreadStatus::OperationType type);

  static util::ThreadStatus::OperationStage SetThreadOperationStage(
      util::ThreadStatus::OperationStage stage);

  static void SetThreadOperationProperty(int code, uint64_t value);

  static void IncreaseThreadOperationProperty(int code, uint64_t delta);

  static void SetThreadState(util::ThreadStatus::StateType type);

  static void ResetThreadStatus();

#ifndef NDEBUG
  static void TEST_SetStateDelay(const util::ThreadStatus::StateType state,
                                 int micro);
  static void TEST_StateDelay(const util::ThreadStatus::StateType state);
#endif

 protected:
  // Initialize the thread-local ThreadStatusUpdater when it finds
  // the cached value is nullptr.  Returns true if it has cached
  // a non-null pointer.
  static bool MaybeInitThreadLocalUpdater(const util::Env* env);

#ifdef ROCKSDB_USING_THREAD_STATUS
  // A boolean flag indicating whether thread_updater_local_cache_
  // is initialized.  It is set to true when an Env uses any
  // ThreadStatusUtil functions using the current thread other
  // than UnregisterThread().  It will be set to false when
  // UnregisterThread() is called.
  //
  // When this variable is set to true, thread_updater_local_cache_
  // will not be updated until this variable is again set to false
  // in UnregisterThread().
  static __thread bool thread_updater_initialized_;

  // The thread-local cached ThreadStatusUpdater that caches the
  // thread_status_updater_ of the first Env that uses any ThreadStatusUtil
  // function other than UnregisterThread().  This variable will
  // be cleared when UnregisterThread() is called.
  //
  // When this variable is set to a non-null pointer, then the status
  // of the current thread will be updated when a function of
  // ThreadStatusUtil is called.  Otherwise, all functions of
  // ThreadStatusUtil will be no-op.
  //
  // When thread_updater_initialized_ is set to true, this variable
  // will not be updated until this thread_updater_initialized_ is
  // again set to false in UnregisterThread().
  static __thread util::ThreadStatusUpdater* thread_updater_local_cache_;
#else
  static bool thread_updater_initialized_;
  static util::ThreadStatusUpdater* thread_updater_local_cache_;
#endif
};

// A helper class for updating thread state.  It will set the
// thread state according to the input parameter in its constructor
// and set the thread state to the previous state in its destructor.
class AutoThreadOperationStageUpdater {
 public:
  explicit AutoThreadOperationStageUpdater(
      util::ThreadStatus::OperationStage stage);
  ~AutoThreadOperationStageUpdater();

#ifdef ROCKSDB_USING_THREAD_STATUS
 private:
  util::ThreadStatus::OperationStage prev_stage_;
#endif
};

}  // namespace monitor
}  // namespace smartengine
