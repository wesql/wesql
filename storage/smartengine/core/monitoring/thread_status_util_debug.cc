// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <atomic>

#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"

namespace smartengine {
using namespace common;
using namespace db;
using namespace util;

namespace monitor {

#ifndef NDEBUG
// the delay for debugging purpose.
static std::atomic<int> states_delay[ThreadStatus::NUM_STATE_TYPES];

void ThreadStatusUtil::TEST_SetStateDelay(const ThreadStatus::StateType state,
                                          int micro) {
  states_delay[state].store(micro, std::memory_order_relaxed);
}

void ThreadStatusUtil::TEST_StateDelay(const ThreadStatus::StateType state) {
  auto delay = states_delay[state].load(std::memory_order_relaxed);
  if (delay > 0) {
    Env::Default()->SleepForMicroseconds(delay);
  }
}

#endif  // !NDEBUG

}  // namespace monitor
}  // namespace smartengine
