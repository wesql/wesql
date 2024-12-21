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

#ifndef SMARTENGINE_UTIL_LOCK_FREE_FIXED_QUEUE_H_
#define SMARTENGINE_UTIL_LOCK_FREE_FIXED_QUEUE_H_ 

#include <cstdlib>
#include <stdint.h>
#include <assert.h>
#include "util/macro_utils.h"

#define e_OK 0
#define e_NOT_INIT -1
#define e_INIT_TWICE -2
#define e_INVALID_ARGUMENT -3
#define e_SIZE_OVERFLOW -5
#define e_ENTRY_NOT_EXIST -6
#define e_ALLOCATE_MEMORY_FAILED -7


namespace smartengine {
namespace util {

template <typename T>
class FixedQueue
{
  struct ArrayItem {
    T* volatile data;
  };
  static const int64_t ARRAY_BLOCK_SIZE = 128L * 1024L;

public:
  static const int64_t ARRAY_ITEM_SIZE = sizeof(ArrayItem);

public:
  FixedQueue();
  ~FixedQueue();

public:
  int init(const int64_t max_num, char* buf);
  void destroy();

public:
  int push(T* ptr);
  int pop(T*& ptr);
  inline int64_t get_total() const;
  inline int64_t get_free() const;
  bool inited() const { return inited_; }
  int64_t capacity() const { return max_num_; }

private:
  inline int64_t get_total_(const uint64_t consumer,
                            const uint64_t producer) const;
  inline int64_t get_free_(const uint64_t consumer,
                           const uint64_t producer) const;

private:
  bool inited_;
  int64_t max_num_;
  ArrayItem* array_;
  volatile uint64_t consumer_ CACHE_ALIGNED; 
  volatile uint64_t producer_ CACHE_ALIGNED;
} CACHE_ALIGNED;

template <typename T>
FixedQueue<T>::FixedQueue()
    : inited_(false),
      max_num_(0),
      array_(nullptr),
      consumer_(0),
      producer_(0) { }

template <typename T>
FixedQueue<T>::~FixedQueue()
{
  destroy();
}

template <typename T>
int FixedQueue<T>::init(const int64_t max_num, char* buf)
{
  int ret = e_OK;
  if (nullptr == buf) {
    ret = e_INVALID_ARGUMENT;
  } else if (inited_) {
    ret = e_INIT_TWICE;
  } else if (0 >= max_num) {
    ret = e_INVALID_ARGUMENT;
  } else {
    array_ = (ArrayItem*)buf;
    memset(array_, 0, sizeof(ArrayItem) * max_num);
    max_num_ = max_num;
    consumer_ = 0;
    producer_ = 0;
    inited_ = true;
  }
  return ret;
}

template <typename T>
void FixedQueue<T>::destroy()
{
  if (inited_) {
    array_ = nullptr;
    max_num_ = 0;
    consumer_ = 0;
    producer_ = 0;
    inited_ = false;
  }
}

template <typename T>
inline int64_t FixedQueue<T>::get_total() const
{
  return get_total_(consumer_, producer_);
}

template <typename T>
inline int64_t FixedQueue<T>::get_free() const
{
  return get_free_(consumer_, producer_);
}

template <typename T>
inline int64_t FixedQueue<T>::get_total_(const uint64_t consumer,
                                           const uint64_t producer) const
{
  return (producer - consumer);
}

template <typename T>
inline int64_t FixedQueue<T>::get_free_(const uint64_t consumer,
                                          const uint64_t producer) const
{
  return max_num_ - get_total_(consumer, producer);
}

template <typename T>
int FixedQueue<T>::push(T* ptr)
{
  int ret = e_OK;
  if (!inited_) {
    ret = e_NOT_INIT;
  } else if (nullptr == ptr) {
    ret = e_INVALID_ARGUMENT;
  } else {
    uint64_t push_p = producer_;
    uint64_t push_limit = consumer_ + max_num_;
    uint64_t old_push = 0;
    while (((old_push = push_p) < push_limit ||
            push_p < (push_limit = consumer_ + max_num_)) &&
           old_push !=
               (push_p = ATOMIC_CAS(&producer_, old_push, old_push + 1))) {
      PAUSE();
    }
    if (push_p < push_limit) {
      void** pdata = (void**)&array_[push_p % max_num_].data;
      while (nullptr != ATOMIC_CAS(pdata, nullptr, ptr)) {
        PAUSE();
      }
    } else {
      ret = e_SIZE_OVERFLOW;
    }
  }
  return ret;
}

template <typename T>
int FixedQueue<T>::pop(T*& ptr)
{
  int ret = e_OK;
  if (!inited_) {
    ret = e_NOT_INIT;
  } else {
    uint64_t pop_c = consumer_;
    uint64_t pop_limit = producer_;
    uint64_t old_pop = 0;
    while (((old_pop = pop_c) < pop_limit || pop_c < (pop_limit = producer_)) &&
           old_pop != (pop_c = ATOMIC_CAS(&consumer_, old_pop, old_pop + 1))) {
      PAUSE();
    }
    if (pop_c < pop_limit) {
      void** pdata = (void**)&array_[(pop_c % max_num_)].data;
      while (nullptr == (ptr = (T*)ATOMIC_SET(pdata, nullptr))) {
        PAUSE();
      }
    } else {
      ret = e_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

template<typename T>
int init_lock_free_queue(size_t queue_size,
                            char* &queue_buf, 
                            FixedQueue<T>* &queue) {
  std::string error_msg = "Memory Limit, init_lock_free_queue failed";
  queue = new FixedQueue<T>();
  assert(queue!= NULL);
  if (queue == NULL)
    return -1;

  queue_buf = (char*)malloc(FixedQueue<T>::ARRAY_ITEM_SIZE * queue_size);
  assert(queue_buf != NULL);

  int ret = queue->init(queue_size, queue_buf);
  assert(ret == e_OK);

  if (ret != e_OK || queue_buf == NULL || queue == NULL) {
    if(queue_buf) free(queue_buf);
    if(queue) free(queue);
    return -1;
  }
  return 0;
}


template <typename T>
class LockFreeQueue {
public:
  LockFreeQueue(uint64_t queue_size) 
               : inited_(false),
                 queue_size_(queue_size){}
  ~LockFreeQueue() {
    this->destroy();
  }
  int init() {
    int ret = 0;
    if (inited_) return ret;
    ret = init_lock_free_queue(queue_size_,
                               this->queue_buf_,
                               this->queue_);
    if (!ret) {
      this->inited_ = true;
    }
    return ret;
  }
  void destroy() {
    if (!inited_)
      return ;
    assert(0 == this->queue_->get_total());
    if (this->queue_) delete this->queue_;
    if (this->queue_buf_) free(this->queue_buf_);
    inited_ = false;
  }
  uint64_t size() {
    if (!inited_) return 0;
    return this->queue_->get_total();
  }
  int push(T* ptr) {
    if (!inited_) return 0;
    int ret = 0;
    while (true) {
      ret = this->queue_->push(ptr);
      if (ret == e_OK)
        break;
    }
    return 1;
  }
  int pop(T* &ptr) {
    if (!inited_) return 0;
    int ret = this->queue_->pop(ptr);
    if (e_OK == ret) {
      return 1;
    }
    return 0;
  }
  void clear() {
    T *ptr;
    while (this->size() != 0) {
      this->pop(ptr);
    }
  }
  
private:
  bool                            inited_;
  uint64_t                        queue_size_;
  char*                           queue_buf_;
  FixedQueue<T>*                  queue_;
};

}//end of util
}//end of smartengine

#endif  // LF_FIXED_QUEUE_H_
