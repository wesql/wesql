//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/concurrent_arena.h"
#include <thread>
#include "util/macro_utils.h"
#include "util/random.h"


namespace smartengine
{
using namespace port;

namespace util
{

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread uint32_t ConcurrentArena::tls_cpuid = 0;
#endif

ConcurrentArena::ConcurrentArena(size_t block_size, size_t huge_page_size,
                                 size_t mod_id)
    : shard_block_size_(block_size / 8),
      arena_(block_size, huge_page_size, mod_id),
      hold_size_(0) {
  // find a power of two >= num_cpus and >= 8
  auto num_cpus = std::thread::hardware_concurrency();
  index_mask_ = 7;
  while (index_mask_ + 1 < num_cpus) {
    index_mask_ = index_mask_ * 2 + 1;
  }
  index_mask_ = num_cpus - 1;
  shards_.reset(new Shard[index_mask_ + 1]);
  Fixup();
}

ConcurrentArena::~ConcurrentArena() {
  if (hold_size_ > 0) {
    std::unique_lock<SpinMutex> lock(arena_mutex_, std::defer_lock);
    if (index_mask_ != 0) {
      lock.lock();
    }
    memory::update_hold_size(-hold_size_, arena_.get_mod_id());
    hold_size_ = 0;
  }
}

ConcurrentArena::Shard* ConcurrentArena::Repick() {
  //int cpuid = port::PhysicalCoreID();
  int cpuid = -1;
  if (UNLIKELY(cpuid < 0)) {
    // cpu id unavailable, just pick randomly
    cpuid =
        Random::GetTLSInstance()->Uniform(static_cast<int>(index_mask_) + 1);
  }
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
  // even if we are cpu 0, use a non-zero tls_cpuid so we can tell we
  // have repicked
  tls_cpuid = cpuid | (static_cast<int>(index_mask_) + 1);
#endif
  return &shards_[cpuid & index_mask_];
}

}  // namespace util
}  // namespace smartengine
