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
#include "util/status.h"

namespace smartengine {
using namespace common;
using namespace util;

namespace util{

/*
 * RWLock used for optimistic lock coupling
 */
class OptRWLatch {
private:
  static const uint64_t OBSOLETE_MASK = 1; // mask for obsolete bit
  static const uint64_t LOCK_MASK = 2;  // mask for lock bit
  /*
   * Format for version_and_lock_:
   * | node type(3 bits) | version(59 bits) | lock(1 bit) | obsolete_flag(1 bit) |
   */
  std::atomic<uint64_t> version_and_lock_;

public:
  OptRWLatch() : version_and_lock_(0) {}
  ~OptRWLatch() {}
  uint64_t get_version_snapshot() { return version_and_lock_.load(); }
  bool is_obsolete(uint64_t version_snapshot) { return (version_snapshot & OBSOLETE_MASK) > 0; }
  bool is_hold_wrlock(uint64_t version_snapshot) { return (version_snapshot & LOCK_MASK) > 0; }
  int check_version(uint64_t version_snapshot) {
    if (UNLIKELY(version_snapshot != get_version_snapshot())) {
      return Status::kTryAgain;
    }
    return Status::kOk;
  }
  int try_rdlock(uint64_t &version_snapshot) {
    version_snapshot = get_version_snapshot();
    if (UNLIKELY(is_hold_wrlock(version_snapshot) || is_obsolete(version_snapshot))) {
      return Status::kTryAgain;
    }
    return Status::kOk;
  }
  int try_upgrade_rdlock(uint64_t version_snapshot) {
    if (UNLIKELY(!version_and_lock_.compare_exchange_strong(version_snapshot, version_snapshot + LOCK_MASK))) {
      return Status::kTryAgain;
    }
    return Status::kOk;
  }
  int try_wrlock() {
    int ret = Status::kOk;
    uint64_t version_snapshot;
    if (FAILED(try_rdlock(version_snapshot))) {
      return ret;
    } else {
      return try_upgrade_rdlock(version_snapshot);
    }
  }

  int rdunlock(uint64_t version_snapshot) { return check_version(version_snapshot); }
  void wrunlock() { version_and_lock_.fetch_add(LOCK_MASK); }
  void wrunlock_with_obsolete() { version_and_lock_.fetch_add(LOCK_MASK + OBSOLETE_MASK); }
};

} // namespace util
} // namespace smartengine
