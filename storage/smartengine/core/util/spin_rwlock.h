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

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include "logger/log_module.h"
#include "util/macro_utils.h"

namespace smartengine
{
namespace util
{
class SpinRWLock
{
public:
  SpinRWLock() : ref_cnt_(0), wait_write_(0){}
  ~SpinRWLock()
  {
    if (0 != ref_cnt_ || 0 != wait_write_) {
      assert(false);
    }
  }

public:
  inline bool try_rdlock()
  {
    bool bret = false;
    int64_t tmp = 0;
    while (0 <= (tmp = ref_cnt_)) {
      int64_t nv = tmp + 1;
      if (tmp ==
          (int64_t)__sync_val_compare_and_swap((uint64_t *)&ref_cnt_, tmp, nv)) {
        bret = true;
        break;
      }
    }
    return bret;
  }
  inline int rdlock()
  {
    int ret = 0;
    int64_t tmp = 0;
    while (true) {
      tmp = ref_cnt_;
      if (0 > tmp || 0 < wait_write_) {
        //MOD_LOG(COMMON, INFO, "wait for write.", K(tmp), K((int64_t)wait_write_));
        // wait for write 
        PAUSE();
        continue;
      } else {
        int64_t nv = tmp + 1;
        if (tmp ==
            (int64_t)__sync_val_compare_and_swap((uint64_t *)&ref_cnt_, tmp, nv)) {
          //MOD_LOG(COMMON, INFO, "read lock hold.", K(tmp), K((int64_t)wait_write_));
          break;
        }
        //MOD_LOG(COMMON, INFO, "read lock cas failed, retry.", K(tmp), K((int64_t)wait_write_));
        PAUSE();
      }
    }
    return ret;
  }
  inline int wrlock()
  {
    int ret = 0;
    int64_t tmp = 0;
    __sync_add_and_fetch((uint64_t *)&wait_write_, 1);
    while (true) {
      tmp = ref_cnt_;
      if (0 != tmp) {
        //MOD_LOG(COMMON, INFO, "write lock conflict, retry.", K(tmp), K((int64_t)wait_write_));
        PAUSE();
        continue;
      } else {
        int64_t nv = -1;
        if (tmp ==
            (int64_t)__sync_val_compare_and_swap((uint64_t *)&ref_cnt_, tmp, nv)) {
          //MOD_LOG(COMMON, INFO, "write lock hold, set ref_cnt = -1.", K(tmp), K((int64_t)wait_write_));
          break;
        }
        //MOD_LOG(COMMON, INFO, "write lock cas failed, retry.", K(tmp), K((int64_t)wait_write_));
        PAUSE();
      }
    }
    __sync_sub_and_fetch((uint64_t *)&wait_write_, 1);
    return ret;
  }
  inline bool try_wrlock()
  {
    bool bret = false;
    int64_t tmp = ref_cnt_;
    __sync_add_and_fetch((uint64_t *)&wait_write_, 1);
    if (0 == tmp) {
      int64_t nv = -1;
      if (tmp ==
          (int64_t)__sync_val_compare_and_swap((uint64_t *)&ref_cnt_, tmp, nv)) {
        bret = true;
      }
    }
    __sync_sub_and_fetch((uint64_t *)&wait_write_, 1);
    return bret;
  }
  inline int unlock()
  {
    int ret = 0;
    int64_t tmp = 0;
    while (true) {
      tmp = ref_cnt_;
      if (0 == tmp) {
        MOD_LOG(COMMON, ERROR, "need not unlock.", K((int64_t)ref_cnt_), K((int64_t)wait_write_));
        break;
      } else if (-1 == tmp) {
        int64_t nv = 0;
        if (tmp ==
            (int64_t)__sync_val_compare_and_swap((uint64_t *)&ref_cnt_, tmp, nv)) {
          //MOD_LOG(COMMON, INFO, "unlock wrlock ok.", K(tmp));
          break;
        }
      } else if (0 < tmp) {
        int64_t nv = tmp - 1;
        if (tmp ==
            (int64_t)__sync_val_compare_and_swap((uint64_t *)&ref_cnt_, tmp, nv)) {
          //MOD_LOG(COMMON, INFO, "unlock rdlock ok.", K(tmp));
          break;
        }
      } else {
        MOD_LOG(COMMON, ERROR, "invalid ref_cnt.", K((int64_t)ref_cnt_));
      }
    }
    return ret;
  }

private:
  volatile int64_t ref_cnt_;
  volatile int64_t wait_write_;
};

class SpinRLockGuard
{
public:
  SpinRLockGuard(SpinRWLock &lock) : lock_(lock) { lock_.rdlock(); }
  ~SpinRLockGuard() { lock_.unlock(); }
private:
  SpinRWLock &lock_;
};

class SpinWLockGuard
{
public:
  SpinWLockGuard(SpinRWLock &lock) : lock_(lock) { lock_.wrlock(); }
  ~SpinWLockGuard() { lock_.unlock(); }
private:
  SpinRWLock &lock_;
};

struct DRWLock {
  class RDLockGuard
  {
  public:
    RDLockGuard(DRWLock &rwlock) : rwlock_(rwlock) { rwlock_.rdlock(); }
    ~RDLockGuard() { rwlock_.rdunlock(); }
  private:
    DRWLock &rwlock_;
  };
  class WRLockGuard
  {
  public:
    WRLockGuard(DRWLock &rwlock) : rwlock_(rwlock) { rwlock_.wrlock(); }
    ~WRLockGuard() { rwlock_.wrunlock(); }
  private:
    DRWLock &rwlock_;
  };
  enum { N_THREAD = 4096 };
  volatile int64_t read_ref_[N_THREAD][CACHE_LINE_SIZE / sizeof(int64_t)];
  volatile int64_t thread_num_;
  volatile uint64_t write_uid_ CACHE_ALIGNED;
  pthread_key_t key_ CACHE_ALIGNED;
  DRWLock()
  {
    write_uid_ = 0;
    memset((void *)read_ref_, 0, sizeof(read_ref_));
    int sys_err = 0;
    if (0 != (sys_err = pthread_key_create(&key_, NULL))) {
      MOD_LOG(COMMON, ERROR, "pthread_key_create fail.", K(sys_err));
    }
  }
  ~DRWLock() { pthread_key_delete(key_); }
  int try_rdlock()
  {
    int err = EAGAIN;
    volatile int64_t *ref = (volatile int64_t *)pthread_getspecific(key_);
    if (NULL == ref) {
      int sys_err = 0;
      if (0 !=
          (sys_err = pthread_setspecific(
             key_,
             (void *)(ref = read_ref_[__sync_fetch_and_add(&thread_num_, 1) %
                                      N_THREAD])))) {
        MOD_LOG(COMMON, ERROR, "pthread_setspecific fail.", K(sys_err));
      }
    }
    while (EAGAIN == err && 0 == write_uid_) {
      __sync_fetch_and_add(ref, 1);
      if (0 == write_uid_) {
        err = 0;
        break;
      }
      __sync_fetch_and_sub(ref, 1);
      PAUSE();
    }
    return err;
  }
  void rdlock()
  {
    volatile int64_t *ref = (volatile int64_t *)pthread_getspecific(key_);
    if (NULL == ref) {
      int sys_err = 0;
      if (0 !=
          (sys_err = pthread_setspecific(
             key_,
             (void *)(ref = read_ref_[__sync_fetch_and_add(&thread_num_, 1) %
                                      N_THREAD])))) {
        MOD_LOG(COMMON, ERROR, "pthread_setspecific fail.", K(sys_err));
      }
    }
    while (true) {
      if (*ref > 0) {
        __sync_fetch_and_add(ref, 1);
        break;
      } else if (0 == write_uid_) {
        __sync_fetch_and_add(ref, 1);
        if (0 == write_uid_) {
          break;
        }
        __sync_fetch_and_sub(ref, 1);
      }
      PAUSE();
    }
  }
  void rdunlock()
  {
    int64_t *ref = (int64_t *)pthread_getspecific(key_);
    __sync_synchronize();
    __sync_fetch_and_sub(ref, 1);
  }
  void wrlock()
  {
    while (!__sync_bool_compare_and_swap(&write_uid_, 0, 1))
      ;
    for (int64_t i = 0; i < std::min((int64_t)thread_num_, (int64_t)N_THREAD);
         i++) {
      while (*read_ref_[i] > 0)
        ;
    }
    write_uid_ = 2;
    __sync_synchronize();
  }
  void wrunlock()
  {
    __sync_synchronize();
    write_uid_ = 0;
  }
} CACHE_ALIGNED;
} //end namespace util
}

