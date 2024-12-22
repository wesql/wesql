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
#include <set>
#include <unordered_map>
#include "base_malloc.h"
#include "alloc_mgr.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/autovector.h"
#include "util/heap.h"

namespace smartengine
{
namespace memory
{
#define ffs_val(x) __builtin_ffsll(x)
#define clz_val(x) __builtin_clzll(x)
struct ObjNode {
  ObjNode () :
      prev_size_(0),
      size_(0),
      next_(nullptr),
      prev_(nullptr) {}
  int32_t prev_size_; // if prev node size
  int32_t size_; //...n p i
  ObjNode *next_; // next node in free list
  ObjNode *prev_; // prev node in free list
};

struct ObjClass {
  ObjClass() :
    size_(0),
    num_(0) {}
  bool operator < (const ObjClass &a) const {
    return size_ < a.size_;
  }
  int64_t size_;
  int64_t num_;
  ObjNode free_list_;
};

class CacheAllocator;
class ChunkManager;
class ChunkAllocator {
public:
  static const int64_t CHUNK_SIZE = 512 * 1024 - AllocMgr::kBlockMeta;
  static const int64_t MAX_OBJ_SIZE = 8 * 1024;
  static const int32_t LEVEL_SIZE = 2 * 1024;
  static const int32_t LAST_STEP_SIZE = 128;
  static const int32_t FIRST_STEP_SIZE = 32;
  static const int32_t MIN_OBJ_SIZE = 32;
  static const int8_t HD_SIZE = sizeof(ObjNode);
  static const int8_t MIN_NODE_SIZE = HD_SIZE + 16;/*smallest obj*/
  static const int8_t PREV_INUSE = 0x2;
  static const int8_t NEXT_INUSE = 0x1;
  static const int8_t OBJ_INUSE = 0x4;
  static const int8_t FLAG_BITS = PREV_INUSE | NEXT_INUSE | OBJ_INUSE;
  static const int8_t ALLOC_ALIGNMENT = 2 * sizeof(size_t);

  ChunkAllocator(void (*deleter)(void* value, void *handler), void *handler);
  ~ChunkAllocator();
  void init(int64_t chunk_id);
  void reset();
  void clear();
  void evict_and_reuse(const int64_t default_score);
  void *alloc(const int64_t size);
  void free(void *ptr, int64_t rsize = 0);
  void set_chunk_id(const int64_t id) {
    chunk_id_ = id;
  }
  inline void set_in_use(ObjNode *obj);
  inline int64_t get_next_valid_idx(int64_t idx) {
    int64_t first = class_num_[1];
    if (idx >= first) {
      int64_t val = (class_bm_[0] >> (idx + 1 - first)) << (idx + 1 - first);
      return val == 0 ? -1 : first + ffs_val(val) - 1;
    } else {
      int64_t val = (class_bm_[1] >> idx) << idx;
      if (0 == val) {
        return class_bm_[0] == 0 ? -1 : first + ffs_val(class_bm_[0]) - 1;
      } else {
        return ffs_val(val) - 1;
      }
    }
    return -1;
  }
  inline bool belong_same_level(int64_t x, int64_t y) {
    int64_t x_idx = x >= class_num_[1] ? (x >= class_num_[2] ? 2 : 1) : 0;
    int64_t y_idx = y >= class_num_[1] ? (y >= class_num_[2] ? 2 : 1) : 0;
    return x_idx == y_idx;
  }
  inline int64_t get_need_class_idx(int64_t size) {
    int64_t first = (size - 1) / LEVEL_SIZE;
    if (first > 2) {
      first = 2;
    }
    int64_t second = ((size - 1)  % LEVEL_SIZE) / (FIRST_STEP_SIZE << first);
    int64_t obj_idx = class_num_[first]  + second;
    return obj_idx;
  }
  inline int64_t get_need_class_size(int64_t size) {
    int64_t idx = get_need_class_idx(size);
    return objs_class_.at(idx).size_;
  }
  inline void set_bm_bit(int64_t idx) {
    if (idx >= 64) {
      class_bm_[0] |= (1LL << (idx - 64));
    } else {
      class_bm_[1] |= (1LL << idx);
    }
  }
  inline void clr_bm_bit(int64_t idx) {
    if (idx >= 64) {
      class_bm_[0] &= ~(1LL << (idx - 64));
    } else {
      class_bm_[1] &= ~(1LL << idx);
    }
  }
  inline bool belong_to_class(int64_t size) {
    int64_t idx = get_need_class_idx(size);
    return objs_class_.at(idx).size_ == size;
  }
  inline ObjClass &get_size_class(int64_t size) {
    int64_t idx = get_need_class_idx(size);
    assert(idx < (int64_t)objs_class_.size());
    return objs_class_.at(idx);
  }
  inline void set_prev_in_use(ObjNode *obj) {
    assert(obj);
    obj->size_ |= PREV_INUSE;
  }
  inline void set_prev_free(ObjNode *obj) {
    assert(obj);
    obj->size_ ^= PREV_INUSE;
  }
  inline void set_next_in_use(ObjNode *obj) {
    assert(obj);
    obj->size_ |= NEXT_INUSE;
  }
  inline void set_next_free(ObjNode *obj) {
    assert(obj);
    obj->size_ ^= NEXT_INUSE;
  }
  inline void set_obj_in_use(ObjNode *obj) {
    assert(obj);
    obj->size_ |= OBJ_INUSE;
  }
  inline void set_obj_free(ObjNode *obj) {
    assert(obj);
    obj->size_ ^= OBJ_INUSE;
  }
  inline void set_free(ObjNode *obj) {
    set_obj_free(obj);
    ObjNode *nextd = get_next_obj(obj);
    if (nullptr != nextd) {
      set_prev_free(nextd);
    }
    ObjNode *prevd = get_prev_obj(obj);
    if (nullptr != prevd) {
      set_next_free(prevd);
    }
  }
  inline void reset_next_presize(ObjNode *obj) {
    ObjNode *nextd = get_next_obj(obj);
    if (nullptr != nextd) {
      nextd->prev_size_ = get_obj_size(obj);
    }
  }
  inline bool prev_in_use(ObjNode *obj) const {
    return (obj->size_ & PREV_INUSE);
  }
  inline bool next_in_use(ObjNode *obj) const {
    return (obj->size_ & NEXT_INUSE);
  }
  inline bool obj_in_use(ObjNode *obj) const {
    return (obj->size_ & OBJ_INUSE);
  }
  inline int64_t get_obj_size(ObjNode *obj) const {
    return (obj->size_ & (~FLAG_BITS));
  }
  inline ObjNode *get_next_obj(ObjNode *obj) {
    if ((char *)obj + get_obj_size(obj) < (char *)buf_ + CHUNK_SIZE) {
      return (ObjNode *)((char *)obj + get_obj_size(obj));
    } else {
      return nullptr;
    }
  }
  inline ObjNode *get_prev_obj(ObjNode *obj) {
    if ((void *)obj != buf_) {
      return (ObjNode *)((char *)obj - obj->prev_size_);
    } else {
      return nullptr;
    }
  }
  void ref() {
    refs_.fetch_add(1);
  }
  void unref() {
    assert(refs_);
    bool last_reference = (1 == refs_.fetch_sub(1));
    if (max_free_size_ < 0
        && last_reference
        && 0 == refs_.load(std::memory_order_relaxed)) {
      reset();
    }
  }
  int64_t get_chunk_id() const {
    return chunk_id_;
  }
  int64_t get_score() const {
    return score_;
  }
  void set_score(const int64_t score) {
    score_ = score;
  }
  void hit_add() {
    access_cnt_.fetch_add(1);
  }
  void reset_access_cnt() {
    access_cnt_.store(0);
  }
  int64_t get_access_cnt() {
    return access_cnt_.load(std::memory_order_relaxed);
  }

  int64_t get_max_free_size() const {
    return max_free_size_;
  }
  void update_max_free_size() {
    if(-1 == max_free_size_) return;
    if (nullptr != last_obj_) {
      if (last_obj_->size_ > MAX_OBJ_SIZE) {
        max_free_size_ = MAX_OBJ_SIZE;
      } else {
        int64_t idx = get_need_class_idx(get_obj_size(last_obj_));
        if (objs_class_.at(idx).size_ != get_obj_size(last_obj_)) {
          max_free_size_ = objs_class_.at(idx - 1).size_;
        }
      }
    } else {
      max_free_size_ = 0;
    }
    if (0 != class_bm_[0]  || class_bm_[1]  != 0) {
      int64_t idx = class_bm_[0] ? clz_val(class_bm_[0]) + 1: class_num_[1] + clz_val(class_bm_[1]) + 1;
      idx = objs_class_.size() - idx;
      assert(idx < (int64_t)objs_class_.size());
      if (idx <  (int64_t)objs_class_.size() && objs_class_.at(idx).size_ > max_free_size_) {
        max_free_size_ = objs_class_.at(idx).size_;
      }
    }
  }
  int64_t get_total_free_size() const {
    int64_t sum = CHUNK_SIZE - allocated_;
    return sum;
  }
  bool try_lock() {
    return mutex_.try_lock();
  }
  void lock() {
    return mutex_.lock();
  }
  void unlock() {
    mutex_.unlock();
  }
  int64_t get_allocated_size() const {
    return allocated_;
  }
  int64_t get_usage() const {
    return usage_;
  }
  void set_clear_flag() {
    clear_flag_ = true;
  }
  void set_commit() {
    int64_t num = in_prep_num_.fetch_sub(1);
    if (1 == num) {
      util::MutexLock lock_guard(&prep_mutex_);
      prep_cv_.Signal();
    }
  }
  void set_abort() {
    int64_t num = in_prep_num_.fetch_sub(1);
    if (1 == num) {
      util::MutexLock lock_guard(&prep_mutex_);
      prep_cv_.Signal();
    }
  }
  bool all_committed() {
    return 0 == in_prep_num_.load(std::memory_order_relaxed);
  }
  void print_chunk_stat(std::string &stat_str) const;
private:
  ObjNode *pick_obj_from_list(const int64_t idx, ObjClass &cls);
  void remove_obj_from_list(ObjNode *obj);
  void put_obj_into_free_list(ObjNode *obj);
  ObjNode *split_from_large_node(ObjNode *&large_node, const int64_t size);
  ObjNode *try_to_merge_neighbour(ObjNode *obj);
  ObjNode *get_from_large_obj(int64_t size);

  int64_t chunk_id_;
  int64_t score_;
  std::atomic<int64_t> refs_;
  std::atomic<int64_t> access_cnt_;
  int16_t class_num_[3];
  int64_t class_bm_[2];
  mutable util::SpinMutex mutex_;
  ObjNode *last_obj_;
  void *buf_;
  int64_t max_free_size_;
  int64_t usage_;
  int64_t allocated_;
  bool clear_flag_;
  void *handler_;
  void (*deleter_)(void* value, void *handler);
  util::autovector<ObjClass, 16> objs_class_;
  // wait alloc_buf be immutable, use 2pc way
  std::atomic<int64_t> in_prep_num_;
  port::Mutex prep_mutex_;
  port::CondVar prep_cv_;
};

struct ChunkScoreCmp {
  bool operator() (const ChunkAllocator *a, const ChunkAllocator *b) {
    if (IS_NULL(a) || IS_NULL(b)) {
      // error
    } else {
      return a->get_score() > b->get_score();
    }
    return false;
  }
};

class ChunkManager {
public:
  static const int64_t EVICT_PERCENT = 3;
  static const int64_t SCORE_DECAY_RATIO = 95;
  ChunkManager();
  ~ChunkManager();
  void init(const int64_t chunk_num,
            void (*deleter)(void* value, void *handler),
            void *handler);
  int evict_one_chunk();
  void print_chunks_stat(std::string &stat_str) const;
  void *alloc(const int64_t size, ChunkAllocator *&alloc);
  void free(void *value, ChunkAllocator *alloc);
  void async_evict_chunks();
  int64_t get_allocated_size() const {
    int64_t sum = 0;
    for (int64_t i = 0; i < chunks_num_; ++i) {
      sum += chunks_[i].get_allocated_size();
    }
    return sum;
  }
  int64_t get_usage() const {
    int64_t sum = 0;
    for (int64_t i = 0; i < chunks_num_; ++i) {
      sum += chunks_[i].get_usage();
    }
    return sum;
  }
  void disown_data() const {
    for (int64_t i = 0; i < chunks_num_; ++i) {
      chunks_[i].set_clear_flag();
    }
  }
private:
  int64_t chunks_num_;
  int64_t topn_val_;
  int64_t avg_score_;
  ChunkAllocator *chunks_;
  mutable util::SpinMutex mutex_;
  util::BinaryHeap<ChunkAllocator *, ChunkScoreCmp> topn_heap_;
};
}
}
