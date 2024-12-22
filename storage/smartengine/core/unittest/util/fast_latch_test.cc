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

#include "util/fast_latch.h"

#include <pthread.h>
#include "util/random.h"
#include "util/se_constants.h"
#include "util/testharness.h"

namespace smartengine
{
using namespace common;
using namespace util;

namespace cache
{

template <typename RWLOCK>
struct RWLockTestParam
{
  RWLockTestParam()
      : cycle_(0),
        ratio_(0),
        r_load_(0),
        w_load_(0)
  {}

  RWLockTestParam(const int32_t cycle,
      const int32_t ratio,
      const int32_t r_load,
      const int32_t w_load)
      : cycle_(cycle),
        ratio_(ratio),
        r_load_(r_load),
        w_load_(w_load)
  {}
  int32_t cycle_;
  int32_t ratio_;
  int32_t r_load_;
  int32_t w_load_;
  RWLOCK lock_;
};

class PthreadRWLock
{
public:
  PthreadRWLock()
  {
    lock_ = PTHREAD_RWLOCK_INITIALIZER;
  }

  int rdlock()
  {
    pthread_rwlock_rdlock(&lock_);
    return common::Status::kOk;
  }

  int wrlock()
  {
    pthread_rwlock_wrlock(&lock_);
    return common::Status::kOk;
  }

  int unlock()
  {
    pthread_rwlock_unlock(&lock_);
    return common::Status::kOk;
  }

private:
  pthread_rwlock_t lock_;
};

class PthreadMutex
{
public:
  PthreadMutex()
  {
    pthread_mutex_init(&lock_, nullptr);
  }

  int rdlock()
  {
    pthread_mutex_lock(&lock_);
    return common::Status::kOk;
  }

  int wrlock()
  {
    pthread_mutex_lock(&lock_);
    return common::Status::kOk;
  }

  int unlock()
  {
    pthread_mutex_unlock(&lock_);
    return common::Status::kOk;
  }

private:
  pthread_mutex_t lock_;
};

int64_t k_rd = 0;
template<typename RWLOCK>
class TestRWLockContend
{
public:
  TestRWLockContend(const int64_t total_thread_cnt)
      : total_thread_cnt_(total_thread_cnt) {}

  void run(RWLockTestParam<RWLOCK> param)
  {
    auto* env = Env::Default();
    uint64_t start = env->NowMicros();
    for (int th = 0; th < total_thread_cnt_; ++th) {
      env->StartThread(stress_func, static_cast<void *>(&param));
    }
    env->WaitForJoin();
    uint64_t elapsed = env->NowMicros() - start;
    std::cout << "Elapsed: " << elapsed << std::endl;
  }

  static void stress_func(void *ptr)
  {
    RWLockTestParam<RWLOCK> &param = *static_cast<RWLockTestParam<RWLOCK> *>(ptr);
    int ret = common::Status::kOk;
    int i = 0;
    while(i < param.cycle_) {
      //if (true) { // read
      if (i % param.ratio_ != 0) { // read
        if (SUCCED(param.lock_.rdlock())) {
          for (int j = 0; j < param.r_load_; j++) {
            workload();
          }
          param.lock_.unlock();
        } else {
          SE_LOG(WARN, "rdlock failed", K(ret));
        }
      } else { // write
        if (SUCCED(param.lock_.wrlock())) {
          //SE_LOG(WARN, "wrlock success", K(ret), K(is::get_tc_tid()));
          k_rd++;
          for (int j = 0; j < param.w_load_; j++) {
            workload();
          }
          param.lock_.unlock();
        } else {
          SE_LOG(WARN, "wrlock failed", K(ret));
        }
      }
      ++i;
    }
  }

  static void workload()
  {
    Random64 rand(0);
    double a = rand.Next() * 1.0 / 1.2;
  }

private:
  int64_t total_thread_cnt_;
};

TEST(FastRWLatch, stress)
{
  k_rd = 0;
  int64_t cycles = 1000;
  int64_t th_cnt = 10;
  int64_t ratio = 100;
  TestRWLockContend<FastRWLatch> stress(th_cnt);
  stress.run(RWLockTestParam<FastRWLatch>(cycles, ratio, 50, 100));
  ASSERT_TRUE(k_rd == cycles * th_cnt / ratio);

  TestRWLockContend<PthreadRWLock> pthread_rw_stress(th_cnt);
  pthread_rw_stress.run(RWLockTestParam<PthreadRWLock>(cycles, ratio, 50, 100));

  TestRWLockContend<PthreadMutex> pthread_m_stress(th_cnt);
  pthread_m_stress.run(RWLockTestParam<PthreadMutex>(cycles, ratio, 50, 100));
}

}  // namespace cache
}  // namespace smartengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	smartengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
