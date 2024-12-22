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

#include <sched.h>
#include <cstdio>
#include <unistd.h>

#include "memory/allocator.h"
#include "memory/base_malloc.h"
#include "memtable/memtablerep.h"
#include "memtable/art_node.h"
#include "util/coding.h"
#include "util/macro_utils.h"

namespace smartengine {
namespace memtable {

class ART {
private:
  ARTNodeBase *root_;
  const MemTableRep::KeyComparator& cmp_;
  memory::Allocator* allocator_;
  ARTValue dummy_node_;
  std::atomic<uint64_t> memory_usage_;
  std::atomic<uint64_t> num_entries_;

  /*
   * This function is very important for ART.
   * Since we use optimistic lock coupling concurrency control method,
   * if there is a high contention workload and this thread has retried many times,
   * we should call sched_yield to relinquish the CPU time slice of this thread
   * to allow other threads to do first.
   * 
   * If we don't call sched_yield, we may suffer the performance degradation.
   */
  void yield(int count) const { 
    if (count > 3) {
      sched_yield();
    } else {
      PAUSE();
    }
  }
  void calc_prefix(common::Slice &lhs, common::Slice &rhs, uint32_t &byte_pos, uint32_t limit);
  int alloc_newnode(ARTNodeBase *&new_node, const uint8_t *prefix, uint16_t prefix_len);
  void insert_into_value_list(ARTValue *insert_artvalue, ARTValue *precursor, common::Slice &insert_key);
  ARTValue *scan_backward_for_less_than(ARTValue *start, const common::Slice &target) const;
  ARTValue *scan_backward_for_greater_than_or_equal(ARTValue *start, const common::Slice &target) const;
  ARTValue *scan_forward_for_greater_than_or_equal(ARTValue *start, const common::Slice &target) const;
  int largest_artvalue_in_subtrie(void *root, ARTValue *&largest_artvalue, 
                                  void *p_node, uint64_t p_version_snapshot) const;
  int smallest_artvalue_in_subtrie(void *root, ARTValue *&smallest_artvalue,
                                    void *p_node, uint64_t p_version_snapshot) const;
  int largest_artvalue_in_subtrie_wrap(void *root, ARTValue *&largest_artvalue) const;
  int smallest_artvalue_in_subtrie_wrap(void *root, ARTValue *&smallest_artvalue) const;
  int insert_maybe_overflow(uint8_t curr_key, ARTValue *insert_artvalue, ARTNodeBase *curr_node, uint64_t curr_version_snapshot,
                            uint8_t parent_key, ARTNodeBase *parent_node, uint64_t parent_version_snapshot);
  int split_prefix_with_normal_case(common::Slice &insert_key, uint32_t start_byte_pos, uint32_t shared_prefix, 
                                    ARTValue *insert_artvalue, ARTNodeBase *curr_node, uint64_t current_version_snapshot,
                                    uint8_t parent_key, ARTNodeBase *parent_node, uint64_t parent_version_snapshot);
  int split_prefix_with_no_differentiable_suffix(common::Slice &insert_key, uint32_t start_byte_pos, uint32_t shared_prefix, 
                                                ARTValue *insert_artvalue, ARTNodeBase *curr_node, uint64_t current_version_snapshot,
                                                uint8_t parent_key, ARTNodeBase *parent_node, uint64_t parent_version_snapshot);
  int insert_maybe_split_artvalue(common::Slice &insert_key, ARTValue *insert_artvalue, uint32_t start_byte_pos,
                                  uint8_t curr_key, ARTNodeBase *curr_node, uint64_t curr_version_snapshot,
                                  ARTNodeBase *parent_node, uint64_t parent_version_snapshot);
  int split_artvalue_with_normal_case(common::Slice &insert_key, ARTValue *insert_artvalue, uint32_t start_byte_pos, uint32_t shared_prefix, 
                                      common::Slice &split_key, ARTValue *split_artvalue, uint8_t curr_key, ARTNodeBase *curr_node);
  int split_artvalue_with_no_differentiable_suffix(common::Slice &insert_key, ARTValue *insert_artvalue, 
                                                  uint32_t start_byte_pos, uint32_t shared_prefix, 
                                                  common::Slice &split_key, ARTValue *split_artvalue, 
                                                  uint8_t curr_key, ARTNodeBase *curr_node);
  int insert_with_no_split_artvalue(common::Slice &insert_key, ARTValue *insert_artvalue, common::Slice &split_key, ARTValue *split_artvalue, 
                                    uint8_t curr_key, ARTNodeBase *curr_node);
  int insert_inner(ARTValue *insert_artvalue);
  int estimate_lower_bound_count_inner(const common::Slice &target, int64_t &count) const;
  int lower_bound_inner(const common::Slice &target, ARTValue *&smallest_successor) const;

public:
  ART(const MemTableRep::KeyComparator& cmp, memory::Allocator* allocator)
  : root_(nullptr),
    cmp_(cmp),
    allocator_(allocator),
    dummy_node_(),
    memory_usage_(0),
    num_entries_(0) {}
  /* 
   * We will use memory pool to alloc all memory of the ART(in the future),
   * so we do nothing in the destructor
   */
  ~ART() { RELEASE(root_); }
  int init();
  const MemTableRep::KeyComparator &comparator() const { return cmp_; }
  ARTValue *dummy_node() { return &dummy_node_; }
  ARTValue *allocate_art_value(int32_t kv_len) {
    char *raw = allocator_->AllocateAligned(sizeof(ARTValue) + kv_len);
    return new (raw) ARTValue();
  }
  void insert(ARTValue *art_value);
  void lower_bound(const common::Slice &target, ARTValue *&smallest_successor) const;
  void estimate_lower_bound_count(const common::Slice &target, int64_t &count) const;
  uint64_t estimate_count(const common::Slice &start_ikey, const common::Slice &end_ikey) const;
  bool contains(const char *target) const;
  size_t approximate_memory_usage() { return memory_usage_.load(); }
  static void ebr_delete(void *p) {
    ARTNodeBase *node = reinterpret_cast<ARTNodeBase *>(p);
    MOD_DELETE_OBJECT(ARTNodeBase, node);
  }
  void dump_art_struct();

  class Iterator {
  private:
    ART *art_;
    ARTValue *curr_artvalue_;
  public:
    explicit Iterator(ART *index) : art_(index), curr_artvalue_(index->dummy_node()) {} 
    void set_art(ART *index) {
      art_ = index;
      curr_artvalue_ = index->dummy_node();
    }
    const char *entry() const { return curr_artvalue_->entry(); }
    Slice key() { return curr_artvalue_->key(); }
    Slice value() { return curr_artvalue_->value(); }
    bool valid() const { return curr_artvalue_ != art_->dummy_node(); }
    void seek_to_first() { curr_artvalue_ = art_->dummy_node()->next(); }
    void seek_to_last() { curr_artvalue_ = art_->dummy_node()->prev(); }
    void next() {
      assert(valid());
      curr_artvalue_ = curr_artvalue_->next();
    }
    void prev() {
      assert(valid());
      curr_artvalue_ = curr_artvalue_->prev();
    }
    void seek(const common::Slice &target) {
      art_->lower_bound(target, curr_artvalue_);
    }
    void seek(const char *target) {
      Slice key = GetLengthPrefixedSlice(target);
      art_->lower_bound(key, curr_artvalue_);
    }
    void seek_for_prev(const common::Slice &target) {
      seek(target);
      if (!valid()) {
        seek_to_last();
      }
      while (valid() && art_->comparator()(curr_artvalue_->entry(), target) > 0) {
        prev();
      }
    }
    void seek_for_prev(const char *target) {
      Slice key = GetLengthPrefixedSlice(target);
      seek_for_prev(key);
    }
  };
};

} // namespace memtable
} // namespace smartengine
