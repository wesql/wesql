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
#ifndef IS_UTIL_TL_STORE_H_
#define IS_UTIL_TL_STORE_H_

#include <errno.h>
#include <pthread.h>
#include <mutex>
#include <sys/syscall.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include "util/macro_utils.h"
namespace smartengine 
{
namespace memory 
{

inline int get_tc_tid() {
  static __thread int tid = -1;
  if (UNLIKELY(tid == -1)) {
    tid = static_cast<int>(syscall(__NR_gettid));
  }
  return tid;
}

class DefaultThreadStoreAlloc {
 public:
  inline void *alloc(const int64_t sz) { return ::malloc(sz); }
  inline void free(void *p) { ::free(p); }
};

template <class Type>
class DefaultInitializer {
 public:
  void operator()(void *ptr) { new (ptr) Type(); }
};

template <class Type, class Initializer = DefaultInitializer<Type>,
          class Alloc = DefaultThreadStoreAlloc>
class ThreadLocalStore {
 public:
  typedef ThreadLocalStore<Type, Initializer, Alloc> TSelf;

 public:
  struct Item {
    TSelf *self;
    int thread_id;
    Type obj;
  };

  class SyncVector {
   public:
    typedef std::vector<Item *> PtrArray;

   public:
    inline int push_back(Item *ptr);

    template <class Function>
    inline int for_each(Function &f) const;
    inline void destroy();

   private:
    PtrArray ptr_array_;
    mutable std::mutex mutex_;
  };

  template <class Function>
  class ObjPtrAdapter {
   public:
    ObjPtrAdapter(Function &f) : f_(f) {}
    void operator()(const Item *item) {
      if (nullptr != item) {
        f_(&item->obj);
      }
    }
    void operator()(Item *item) {
      if (nullptr != item) {
        f_(&item->obj);
      }
    }

   protected:
    Function &f_;
  };

 public:
  static const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;

 public:
  ThreadLocalStore(Alloc &alloc);

  ThreadLocalStore(Initializer &initializer, Alloc &alloc);

  ThreadLocalStore(Initializer &initializer);

  ThreadLocalStore();

  virtual ~ThreadLocalStore();

  static void destroy_object(Item *item);

  int32_t init();

  void destroy();

  Type *get();

  template <class Function>
  int for_each_obj_ptr(Function &f) const;

  template <class Function>
  int for_each_item_ptr(Function &f) const;

 private:
  pthread_key_t key_;
  DefaultInitializer<Type> default_initializer_;
  DefaultThreadStoreAlloc default_alloc_;
  Initializer &initializer_;
  SyncVector ptr_array_;
  Alloc &alloc_;
  bool init_;
};

template <class Type, class Initializer, class Alloc>
int ThreadLocalStore<Type, Initializer, Alloc>::SyncVector::push_back(
    Item *ptr) {
  int ret = 0;
  mutex_.lock();
  ptr_array_.push_back(ptr);
  mutex_.unlock();
  return ret;
}

template <class Type, class Initializer, class Alloc>
template <class Function>
int ThreadLocalStore<Type, Initializer, Alloc>::SyncVector::for_each(
    Function &f) const {
  int ret = 0;
  mutex_.lock();
  std::for_each(ptr_array_.begin(), ptr_array_.end(), f);
  mutex_.unlock();
  return ret;
}

template <class Type, class Initializer, class Alloc>
void ThreadLocalStore<Type, Initializer, Alloc>::SyncVector::destroy() {
  mutex_.lock();
  ptr_array_.clear();
  mutex_.unlock();
}

template <class Type, class Initializer, class Alloc>
ThreadLocalStore<Type, Initializer, Alloc>::ThreadLocalStore(Alloc &alloc)
    : key_(INVALID_THREAD_KEY),
      initializer_(default_initializer_),
      alloc_(alloc),
      init_(false) {}

template <class Type, class Initializer, class Alloc>
ThreadLocalStore<Type, Initializer, Alloc>::ThreadLocalStore(
    Initializer &initializer, Alloc &alloc)
    : key_(INVALID_THREAD_KEY),
      initializer_(initializer),
      alloc_(alloc),
      init_(false) {}

template <class Type, class Initializer, class Alloc>
ThreadLocalStore<Type, Initializer, Alloc>::ThreadLocalStore(
    Initializer &initializer)
    : key_(INVALID_THREAD_KEY),
      initializer_(initializer),
      alloc_(default_alloc_),
      init_(false) {}

template <class Type, class Initializer, class Alloc>
ThreadLocalStore<Type, Initializer, Alloc>::ThreadLocalStore()
    : key_(INVALID_THREAD_KEY),
      initializer_(default_initializer_),
      alloc_(default_alloc_),
      init_(false) {}

template <class Type, class Initializer, class Alloc>
ThreadLocalStore<Type, Initializer, Alloc>::~ThreadLocalStore() {
  destroy();
}

template <class Type, class Initializer, class Alloc>
void ThreadLocalStore<Type, Initializer, Alloc>::destroy_object(Item *item) {
  if (nullptr != item) {
    item->obj.~Type();
    item->self->alloc_.free(item);
  }
}

template <class Type, class Initializer, class Alloc>
int32_t ThreadLocalStore<Type, Initializer, Alloc>::init() {
  int32_t ret = 0;
  if (init_) {
    ret = -1;
  } else {
    if (INVALID_THREAD_KEY == key_) {
      int err = pthread_key_create(&key_, nullptr);
      if (0 != err) {
        if (errno == ENOMEM) {
          ret = -1;
        } else {
          ret = -1;
        }
      } else {
        init_ = true;
      }
    } else {
      ret = -1;
    }
  }
  return ret;
}

template <class Type, class Initializer, class Alloc>
void ThreadLocalStore<Type, Initializer, Alloc>::destroy() {
  if (init_) {
    if (INVALID_THREAD_KEY != key_) {
      // void* mem = pthread_getspecific(key_);
      // if (nullptr != mem) destroy_object(mem);
      pthread_key_delete(key_);
      key_ = INVALID_THREAD_KEY;
    }
    for_each_item_ptr(destroy_object);
    ptr_array_.destroy();
    init_ = false;
  }
}

template <class Type, class Initializer, class Alloc>
Type *ThreadLocalStore<Type, Initializer, Alloc>::get() {
  Type *ret = nullptr;
  if (UNLIKELY(!init_)) {
  } else if (INVALID_THREAD_KEY == key_) {
  } else {
    Item *item = reinterpret_cast<Item *>(pthread_getspecific(key_));
    if (nullptr == item) {
      item = reinterpret_cast<Item *>(alloc_.alloc(sizeof(Item)));
      if (nullptr != item) {
        if (0 != pthread_setspecific(key_, item)) {
          alloc_.free(item);
          item = nullptr;
        } else {
          initializer_(&item->obj);
          item->self = this;
          item->thread_id = get_tc_tid();
          ptr_array_.push_back(item);
        }
      }
    }
    if (nullptr != item) {
      ret = &item->obj;
    }
  }
  return ret;
}

template <class Type, class Initializer, class Alloc>
template <class Function>
int ThreadLocalStore<Type, Initializer, Alloc>::for_each_obj_ptr(
    Function &f) const {
  if (UNLIKELY(!init_)) {
    return -1;
  } else {
    ObjPtrAdapter<Function> opa(f);
    return ptr_array_.for_each(opa);
  }
}

template <class Type, class Initializer, class Alloc>
template <class Function>
int ThreadLocalStore<Type, Initializer, Alloc>::for_each_item_ptr(
    Function &f) const {
  if (UNLIKELY(!init_)) {
    return -1;
  } else {
    return ptr_array_.for_each(f);
  }
}

}  // end namespace memory
}  // end namespace smartengine  

#endif  // IS_UTIL_TL_STORE_H_
