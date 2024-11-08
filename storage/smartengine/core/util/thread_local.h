//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>
#include <functional>
#include <pthread.h>
#include <vector>


#ifndef ROCKSDB_SUPPORT_THREAD_LOCAL
#if defined(OS_WIN) || defined(OS_MACOSX) || defined(IOS_CROSS_COMPILE)
#define ROCKSDB_SUPPORT_THREAD_LOCAL 0
#else
#define ROCKSDB_SUPPORT_THREAD_LOCAL 1
#endif
#endif

namespace smartengine {
namespace util {

// Cleanup function that will be called for a stored thread local
// pointer (if not NULL) when one of the following happens:
// (1) a thread terminates
// (2) a ThreadLocalPtr is destroyed
typedef void (*UnrefHandler)(void* ptr);

// ThreadLocalPtr stores only values of pointer type.  Different from
// the usual thread-local-storage, ThreadLocalPtr has the ability to
// distinguish data coming from different threads and different
// ThreadLocalPtr instances.  For example, if a regular thread_local
// variable A is declared in DBImpl, two DBImpl objects would share
// the same A.  However, a ThreadLocalPtr that is defined under the
// scope of DBImpl can avoid such confliction.  As a result, its memory
// usage would be O(# of threads * # of ThreadLocalPtr instances).
class ThreadLocalPtr {
 public:
  explicit ThreadLocalPtr(UnrefHandler handler = nullptr);

  ~ThreadLocalPtr();

  // Return the current pointer stored in thread local
  void* Get() const;

  // Set a new pointer value to the thread local storage.
  void Reset(void* ptr);

  // Atomically swap the supplied ptr and return the previous value
  void* Swap(void* ptr);

  // Atomically compare the stored value with expected. Set the new
  // pointer value to thread local only if the comparison is true.
  // Otherwise, expected returns the stored value.
  // Return true on success, false on failure
  bool CompareAndSwap(void* ptr, void*& expected);

  // Reset all thread local data to replacement, and return non-nullptr
  // data for all existing threads
  void Scrape(std::vector<void*>* ptrs, void* const replacement);

  typedef std::function<void(void*, void*)> FoldFunc;
  // Update res by applying func on each thread-local value. Holds a lock that
  // prevents unref handler from running during this call, but clients must
  // still provide external synchronization since the owning thread can
  // access the values without internal locking, e.g., via Get() and Reset().
  void Fold(FoldFunc func, void* res);

  // Add here for testing
  // Return the next available Id without claiming it
  static uint32_t TEST_PeekId();

  // Initialize the static singletons of the ThreadLocalPtr.
  //
  // If this function is not called, then the singletons will be
  // automatically initialized when they are used.
  //
  // Calling this function twice or after the singletons have been
  // initialized will be no-op.
  static void InitSingletons();

  class StaticMeta;

 private:
  static StaticMeta* Instance();

  const uint32_t id_;
};

class ThreadLocalHelper {
using DeleterVector = std::vector<std::pair<UnrefHandler, void*>>;

public:
  static ThreadLocalHelper &instance();
  ThreadLocalHelper();
  ~ThreadLocalHelper();

  int register_deleter(UnrefHandler handler, void *ptr);

private:
  static DeleterVector *get_deleters();
  static void delete_entities(void *ctx);
  static void make_key() {
    (void)pthread_key_create(&tls_key_, delete_entities);
  }

  static pthread_key_t tls_key_;
  static thread_local DeleterVector *deleters_;
};

}  // namespace util
}  // namespace smartengine
