/************************************************************************
 *
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 ************************************************************************/

#pragma once

#include <sys/syscall.h>
#include <unistd.h>
#include <linux/futex.h>
#include <errno.h>

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(expr)   (__builtin_expect(!!(expr), !!1))
#define UNLIKELY(expr) (__builtin_expect(!!(expr), !!0))
#else
#define LIKELY(expr) (expr)
#define UNLIKELY(expr) (expr)
#endif

#define IS_FALSE(expr) (UNLIKELY(false == (expr)))

#define IS_NULL(expr) (UNLIKELY(nullptr == (expr)))
#define IS_NOTNULL(expr) (LIKELY(nullptr != (expr)))

#define SUCCED(expr) (LIKELY(smartengine::common::Status::kOk == (ret = (expr))))
#define FAILED(expr) (UNLIKELY(smartengine::common::Status::kOk != (ret = (expr))))

/**Abort execution if expr does not evaluate to nonzero.*/
#define se_assert(expr)                     \
  do {                                      \
    bool val = (expr);                      \
    if (UNLIKELY(!val)) {                   \
      abort();                              \
    }                                       \
  } while (false)


#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64U
#endif

#ifndef CACHE_ALIGNED
#define CACHE_ALIGNED __attribute__ ((aligned (CACHE_LINE_SIZE)))
#endif

#ifndef PAUSE
#if defined(__x86_64__)
#define PAUSE() asm("pause\n")
#elif defined(__aarch64__)
#define PAUSE() asm("yield\n")
#endif
#endif

// atomic operations
#define ATOMIC_LOAD(x) ({asm volatile("" ::: "memory"); *(x);})
#define ATOMIC_BCAS(val, cmpv, newv) ({__sync_bool_compare_and_swap((val), (cmpv), (newv));})
#define ATOMIC_AND_FETCH(val, andv) ({__sync_and_and_fetch((val), (andv));})
#define ATOMIC_SUB_FETCH(val, subv) ({__sync_sub_and_fetch((val), (subv));})
#define ATOMIC_FETCH_ADD(val, subv) ({__sync_fetch_and_add((val), (subv));})
#define ATOMIC_ADD_FETCH(val, subv) ({__sync_add_and_fetch((val), (subv));})
#define ATOMIC_SET(val, newv) ({__sync_lock_test_and_set((val), (newv));})
#define ATOMIC_FETCH_AND(val, andv) ({__sync_fetch_and_and((val), (andv));})
#define ATOMIC_CAS(val, cmpv, newv) ({__sync_val_compare_and_swap((val), (cmpv), (newv));})


