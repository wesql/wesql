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
#include "memtable/art_node.h"
#include "logger/log_module.h"
#include "util/macro_utils.h"

namespace smartengine {

using namespace common;
using namespace util;
using namespace memory;
using namespace logger;

namespace memtable {

const Slice ARTValue::value() const {
  uint32_t key_len = 0;
  const char *key_ptr = GetVarint32Ptr(entry_, entry_ + 5, &key_len);
  return GetLengthPrefixedSlice(key_ptr + key_len);
}

//This two functions ensure that only the commited node can be accessed.
ARTValue *ARTValue::prev() const { 
  ARTValue *ret = reinterpret_cast<ARTValue *>(prev_and_commit_.load() & PTR_MASK); 
  assert(ret != nullptr);
  return ret->wait_for_commit();
}

ARTValue *ARTValue::next() const { 
  ARTValue *ret = reinterpret_cast<ARTValue *>(next_and_lock_.load() & PTR_MASK); 
  assert(ret != nullptr);
  return ret->wait_for_commit();  
}

void ARTValue::set_prev(const ARTValue *p) {
  uint64_t raw_ptr = reinterpret_cast<uint64_t>(p);
  uint64_t old = prev_and_commit_.load();
  while (true) {
    if (prev_and_commit_.compare_exchange_strong(old, (old & COMMIT_MASK) | raw_ptr)) {
      break;
    } else {
      PAUSE();
    }
  }
}

void ARTValue::set_next(const ARTValue *n) {
  uint64_t raw_ptr = reinterpret_cast<uint64_t>(n);
  uint64_t old = next_and_lock_.load();
  while (true) {
    if (next_and_lock_.compare_exchange_strong(old, (old & LOCK_MASK) | raw_ptr)) {
      break;
    } else {
      PAUSE();
    }
  }
}

void ARTValue::lock() {
  while (true) {
    uint64_t old = next_and_lock_.load();
    if (is_locked(old)) {
      PAUSE();
    } else {
      if (next_and_lock_.compare_exchange_strong(old, old | LOCK_MASK)) {
        break;
      }
    }
  }
}

ARTValue *ARTValue::wait_for_commit() {
  while (is_commit() == false) {
    PAUSE();
  }
  return this;
}

int ARTNodeBase::set_prefix(const uint8_t *prefix, uint64_t len) {
  int ret = Status::kOk;
  if (alloc_ptr_ == nullptr) {
    if (len > PREFIX_BUF_SIZE) {
      if (IS_NULL(alloc_ptr_ = reinterpret_cast<uint8_t *>(base_memalign(PREFIX_ALIGN_SIZE, len, ModId::kMemtable)))) {
        SE_LOG(ERROR, "failed to alloc memory for prefix", K(alloc_ptr_));
        ret = Status::kMemoryLimit;
        return ret;
      }
    } else {
      alloc_ptr_ = prefix_buf_;
    }
    memmove(alloc_ptr_, prefix, len);
    prefix_len_and_content_.store((len << PREFIX_LEN_OFFSET) | reinterpret_cast<uint64_t>(alloc_ptr_));
  } else {
    prefix_len_and_content_.store((len << PREFIX_LEN_OFFSET) | reinterpret_cast<uint64_t>(prefix));
  }
  return ret;
}

int ARTNodeBase::check_prefix(const uint8_t *raw_key, uint32_t &byte_pos, uint32_t user_key_len) {
  uint32_t old = byte_pos;
  int32_t i = 0;
  uint64_t prefix_len_and_content_snapshot = prefix_len_and_content_.load();
  int32_t len = prefix_len(prefix_len_and_content_snapshot);
  uint8_t *prefix_content = prefix(prefix_len_and_content_snapshot);
  while ((i = byte_pos - old) < len && byte_pos < user_key_len) {
    if (raw_key[byte_pos] == prefix_content[i]) {
      byte_pos++;
    } else if (raw_key[byte_pos] < prefix_content[i]) {
      return -1;
    } else {
      return 1;
    }
  }
  return 0;
}

int ARTNode256::init(const uint8_t *prefix, uint16_t prefix_len) {
  int ret = Status::kOk;
  if (FAILED(set_prefix(prefix, prefix_len))) {
    SE_LOG(ERROR, "failed to set prefix len", K(prefix), K(prefix_len));
  } else {
    memset(children_, 0, sizeof(void *) * FANOUT);
  }
  return ret;
}

void ARTNode256::release() {
  for (int32_t i = 0; i < FANOUT; i++) {
    if (children_[i] != nullptr && IS_ARTVALUE(children_[i]) == false) {
      RELEASE(children_[i]);
    }
  }
}

int ARTNode256::insert(uint8_t key, void *child) {
  int ret = Status::kOk;
  if (IS_NULL(child)) {
    SE_LOG(ERROR, "invalid argument", KP(child));
    ret = Status::kInvalidArgument;
  } else if (UNLIKELY(children_[key] != nullptr)) {
    SE_LOG(ERROR, "child at insert position exists", K(key), KP(child), KP(children_[key]));
    ret = Status::kInsertCheckFailed;
  } else {
    children_[key] = child;
    inc_count();
  }
  return ret;
}

int ARTNode256::update(uint8_t key, void *child) {
  int ret = Status::kOk;
  if (IS_NULL(child)) {
    SE_LOG(ERROR, "invalid argument", KP(child));
    ret = Status::kInvalidArgument;
  } else if (IS_NULL(children_[key])) {
    SE_LOG(ERROR, "child at update position dose not exist", K(key), KP(child));
    ret = Status::kErrorUnexpected;
  } else {
    children_[key] = child;
  }
  return ret;
}

void *ARTNode256::get_with_less_than_percent(uint8_t key, double &percent) {
  if (UNLIKELY(is_empty())) {
    percent = 0.0;
    return nullptr;
  } else {
    int32_t pos = 0;
    int32_t less_than_count = 0;
    while (pos < key) {
      if (children_[pos] != nullptr) {
        less_than_count++;
      }
      pos++;
    }
    percent = 1.0 * less_than_count / count();
    return children_[key];
  }
}

int ARTNode256::expand(ARTNodeBase *&new_node) {
  SE_LOG(ERROR, "ARTNode256 cannot be expanded");
  return Status::kErrorUnexpected;
}

void *ARTNode256::maximum() {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    int32_t pos = FANOUT - 1;
    while (pos >= 0 && children_[pos] == nullptr) {
      pos--;
    }
    assert(pos >= 0);
    return children_[pos];
  }
}

void *ARTNode256::minimum() {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    if (has_value_ptr()) {
      return value_ptr();
    } else {
      int32_t pos = 0;
      while (pos < FANOUT && children_[pos] == nullptr) {
        pos++;
      }
      assert(pos < FANOUT);
      return children_[pos];
    }
  }
}

void *ARTNode256::less_than(uint8_t key) {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    int32_t pos = key - 1;
    while (pos >= 0 && children_[pos] == nullptr) {
      pos--;
    }
    return pos >= 0 ? children_[pos] : value_ptr();
  }
}

void *ARTNode256::greater_than(uint8_t key) {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    int32_t pos = key + 1;
    while (pos < FANOUT && children_[pos] == nullptr) {
      pos++;
    }
    return pos < FANOUT ? children_[pos] : nullptr;
  }
}

void ARTNode256::dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) {
  if (is_empty()) {
    SE_LOG(WARN, "Inner node should not be empty when dumping struct info", K(last_node_id), K(last_allocated_node_id));
    fprintf(fp, "|- LEVEL#%ld NODEID#%ld EMPTY -|\n", level, last_node_id++);
  } else {
    fprintf(fp, "|- LEVEL#%ld NODEID#%ld COUNT#%d ", level, last_node_id++, count());
    fprintf(fp, "PREFIXLEN#%u ", prefix_len());
    fprintf(fp, "PREFIX ");
    for (uint16_t i = 0; i < prefix_len(); i++) {
      fprintf(fp, "\\%u", prefix()[i]);
    }
    fprintf(fp, " ");
    for (int32_t i = 0; i < FANOUT; i++) {
      if (children_[i] != nullptr) {
        if (IS_ARTVALUE(children_[i])) {
          fprintf(fp, "%u: v ", static_cast<uint8_t>(i));
        } else {
          fprintf(fp, "%u: %ld ", static_cast<uint8_t>(i), last_allocated_node_id++);
          bfs_queue.push(VOID2INNERNODE(children_[i]));
        }
      }
    }
    fprintf(fp, "-|\n");
  }
}

int ARTNode48::copy_to_larger(ARTNodeBase *larger) {
  int ret = Status::kOk;
  for (int32_t i = 0; i < CHILDREN_MAP_SIZE; i++) {
    if (children_map_[i] == EMPTY_FLAG) {
      // Do nothing
    } else if (FAILED(larger->insert(static_cast<uint8_t>(i), children_[children_map_[i]]))) {
      SE_LOG(ERROR, "failed to insert into larger", K(i), KP(children_[children_map_[i]]), KP(larger));
      break;
    }
  }
  larger->set_value_ptr(value_ptr());
  return ret;
}

int ARTNode48::init(const uint8_t *prefix, uint16_t prefix_len) {
  int ret = Status::kOk;
  if (FAILED(set_prefix(prefix, prefix_len))) {
    SE_LOG(ERROR, "failed to set prefix", K(prefix), K(prefix_len));
  } else {
    memset(children_map_, EMPTY_FLAG, sizeof(uint8_t) * CHILDREN_MAP_SIZE);
    memset(children_, 0, sizeof(void *) * FANOUT);
  }
  return ret;
}

void ARTNode48::release() {
  for (int32_t i = 0; i < CHILDREN_MAP_SIZE; i++) {
    if (children_map_[i] != EMPTY_FLAG && IS_ARTVALUE(children_[children_map_[i]]) == false) {
      RELEASE(children_[children_map_[i]]);
    }
  }
}

int ARTNode48::insert(uint8_t key, void *child) {
  int ret = Status::kOk;
  if (IS_NULL(child)) {
    SE_LOG(ERROR, "invalid argument", KP(child));
    ret = Status::kInvalidArgument;
  } else if (UNLIKELY(children_map_[key] != EMPTY_FLAG)) {
    SE_LOG(ERROR, "child at insert position exists", K(key), KP(child), KP(children_[children_map_[key]]));
    ret = Status::kInsertCheckFailed;
  } else {
    children_map_[key] = count();
    children_[count()] = child;
    inc_count();
  }
  return ret;
}

int ARTNode48::update(uint8_t key, void *child) {
  int ret = Status::kOk;
  if (IS_NULL(child)) {
    SE_LOG(ERROR, "invalid argument", KP(child));
    ret = Status::kInvalidArgument;
  } else if (UNLIKELY(children_map_[key] == EMPTY_FLAG)) {
    SE_LOG(ERROR, "child at update position dose not exist", K(key), KP(child));
    ret = Status::kErrorUnexpected;
  } else {
    children_[children_map_[key]] = child;
  }
  return ret;
}

void *ARTNode48::get_with_less_than_percent(uint8_t key, double &percent) {
  if (UNLIKELY(is_empty())) {
    percent = 0.0;
    return nullptr;
  } else {
    int32_t pos = 0;
    int32_t less_than_count = 0;
    while (pos < key) {
      if (children_map_[pos] != EMPTY_FLAG) {
        less_than_count++;
      }
      pos++;
    }
    percent = 1.0 * less_than_count / count();
    return children_map_[key] != EMPTY_FLAG ? children_[children_map_[key]] : nullptr; 
  }
}

int ARTNode48::expand(ARTNodeBase *&new_node) {
  int ret = Status::kOk;
  if (IS_NULL(new_node = MOD_NEW_OBJECT(ModId::kMemtable, ARTNode256))) { // TODO(nanlong.ynl): use memory pool for alloc
    SE_LOG(ERROR, "failed to alloc memory for ARTNode256");
    ret = Status::kMemoryLimit;
  } else if (FAILED(new_node->init(prefix(), prefix_len()))) {
    SE_LOG(ERROR, "failed to init ARTNode256");
  } else if (FAILED(copy_to_larger(new_node))) {
    SE_LOG(ERROR, "failed to copy to new node", K(ret), KP(new_node));
  }
  return ret;
}

void *ARTNode48::maximum() {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    int pos = CHILDREN_MAP_SIZE - 1;
    while (pos >= 0 && children_map_[pos] == EMPTY_FLAG) {
      pos--;
    }
    assert(pos >= 0);
    return children_[children_map_[pos]];
  }
}

void *ARTNode48::minimum() {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    if (has_value_ptr()) {
      return value_ptr();
    } else {
      int32_t pos = 0;
      while (pos < CHILDREN_MAP_SIZE && children_map_[pos] == EMPTY_FLAG) {
        pos++;
      }
      assert(pos < CHILDREN_MAP_SIZE);
      return children_[children_map_[pos]];
    }
  }
}

void *ARTNode48::less_than(uint8_t key) {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    int32_t pos = key - 1;
    while (pos >= 0 && children_map_[pos] == EMPTY_FLAG) {
      pos--;
    }
    return pos >= 0 ? children_[children_map_[pos]] : value_ptr();
  }
}

void *ARTNode48::greater_than(uint8_t key) {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    int32_t pos = key + 1;
    while (pos < CHILDREN_MAP_SIZE && children_map_[pos] == EMPTY_FLAG) {
      pos++;
    }
    return pos < CHILDREN_MAP_SIZE ? children_[children_map_[pos]] : nullptr;
  }
}

void ARTNode48::dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) {
  if (is_empty()) {
    SE_LOG(WARN, "Inner node should not be empty when dumping struct info", K(last_node_id), K(last_allocated_node_id));
    fprintf(fp, "|- LEVEL#%ld NODEID#%ld EMPTY -|\n", level, last_node_id++);
  } else {
    fprintf(fp, "|- LEVEL#%ld NODEID#%ld COUNT#%d ", level, last_node_id++, count());
    fprintf(fp, "PREFIXLEN#%u ", prefix_len());
    fprintf(fp, "PREFIX ");
    for (uint16_t i = 0; i < prefix_len(); i++) {
      fprintf(fp, "\\%u", prefix()[i]);
    }
    fprintf(fp, " ");
    for (int32_t i = 0; i < FANOUT; i++) {
      if (children_map_[i] != EMPTY_FLAG) {
        if (IS_ARTVALUE(children_[children_map_[i]])) {
          fprintf(fp, "%u: v ", static_cast<uint8_t>(i));
        } else {
          fprintf(fp, "%u: %ld ", static_cast<uint8_t>(i), last_allocated_node_id++);
          bfs_queue.push(VOID2INNERNODE(children_[children_map_[i]]));
        }
      }
    }
    fprintf(fp, "-|\n");
  }
}

/*
 * This function flips the sign bit of a key.
 * 
 * Because the compare instructions of the SSE2 SIMD instruction set used by ARTNode16
 * treat the compare operands as signed integers, which is totally different from memcmp,
 * we should use this function to flip the sign bit of raw bytes for keys.
 * 
 * For how to use SSE2 SIMD instruction set, please see document
 * https://software.intel.com/sites/landingpage/IntrinsicsGuide/
 */
uint8_t ARTNode16::flip_sign(uint8_t key) {
  return key ^ FLIP_MASK;
}

/*
 * Find the position of the first key which is greater than or equal to target
 */
bool ARTNode16::find_pos(uint8_t key, int32_t &pos) {
#ifdef __x86_64__
  pos = 0;
  if (is_empty()) {
    return false;
  } else {
    SIMD_REG node_key_reg = _mm_loadu_si128(reinterpret_cast<SIMD_REG *>(key_));
    SIMD_REG target_reg = _mm_set1_epi8(flip_sign(key));
    pos = _popcnt32(_mm_movemask_epi8(_mm_cmplt_epi8(node_key_reg, target_reg)) & ((1 << count()) - 1));
    return pos < count() && key_[pos] == flip_sign(key) ? true : false;
  }
#else
  pos = 0;
  while (pos < count() && key_[pos] < key) {
    pos++;
  }
  return pos < count() && key_[pos] == key ? true : false;
#endif
}

int ARTNode16::copy_to_larger(ARTNodeBase *larger) {
  int ret = Status::kOk;
  for (int32_t i = 0; i < count(); i++) {
    if (FAILED(larger->insert(flip_sign(key_[i]), children_[i]))) {
      SE_LOG(ERROR, "failed to insert into larger", K(i), K(key_[i]), KP(children_[i]), KP(larger));
      break;
    }
  }
  larger->set_value_ptr(value_ptr());
  return ret;
}

int ARTNode16::init(const uint8_t *prefix, uint16_t prefix_len) {
  int ret = Status::kOk;
  if (FAILED(set_prefix(prefix, prefix_len))) {
    SE_LOG(ERROR, "failed to set prefix", K(prefix), K(prefix_len));
  } else {
    memset(key_, 0, sizeof(uint8_t) * FANOUT);
    memset(children_, 0, sizeof(void *) * FANOUT);
  }
  return ret;
}

void ARTNode16::release() {
  for (int32_t i = 0; i < count(); i++) {
    if (IS_ARTVALUE(children_[i]) == false) {
      RELEASE(children_[i]);
    }
  }
}

int ARTNode16::insert(uint8_t key, void *child) {
  int ret = Status::kOk;
  int32_t pos = 0;
  if (IS_NULL(child)) {
    SE_LOG(ERROR, "invalid argument", KP(child));
    ret = Status::kInvalidArgument;
  } else if (UNLIKELY(find_pos(key, pos))) {
    SE_LOG(ERROR, "child at insert position exists", K(key), KP(child), K(pos), KP(children_[pos]));
    ret = Status::kInsertCheckFailed;
  } else {
    memmove(key_ + pos + 1, key_ + pos, (count() - pos) * sizeof(uint8_t));
    memmove(children_ + pos + 1, children_ + pos, (count() - pos) * sizeof(void *));
    key_[pos] = flip_sign(key);
    children_[pos] = child;
    inc_count();
  }
  return ret;
}

int ARTNode16::update(uint8_t key, void *child) {
  int ret = Status::kOk;
  int32_t pos = 0;
  if (IS_NULL(child)) {
    SE_LOG(ERROR, "invalid argument", KP(child));
    ret = Status::kInvalidArgument;
  } else if (UNLIKELY(find_pos(key, pos) == false)) {
    SE_LOG(ERROR, "child at update position dose not exist", K(key), KP(child), K(pos));
    ret = Status::kErrorUnexpected;
  } else {
    children_[pos] = child;
  }
  return ret;
}

void *ARTNode16::get(uint8_t key) {
  int32_t pos = 0;
  return find_pos(key, pos) ? children_[pos] : nullptr;
}

void *ARTNode16::get_with_less_than_percent(uint8_t key, double &percent) {
  if (UNLIKELY(is_empty())) {
    percent = 0.0;
    return nullptr;
  } else {
    int32_t pos = 0;
    bool found = find_pos(key, pos);
    percent = 1.0 * pos / count();
    return found ? children_[pos] : nullptr;
  }
}

int ARTNode16::expand(ARTNodeBase *&new_node) {
  int ret = Status::kOk;
  if (IS_NULL(new_node = MOD_NEW_OBJECT(ModId::kMemtable, ARTNode48))) { // TODO(nanlong.ynl): use memory pool for alloc
    SE_LOG(ERROR, "failed to alloc memory for ARTNode16");
    ret = Status::kMemoryLimit;
  } else if (FAILED(new_node->init(prefix(), prefix_len()))) {
    SE_LOG(ERROR, "failed to init ARTNode48");
  } else if (FAILED(copy_to_larger(new_node))) {
    SE_LOG(ERROR, "failed to copy to new node", K(ret), KP(new_node));
  }
  return ret;
}

void *ARTNode16::minimum() {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    if (has_value_ptr()) {
      return value_ptr();
    } else {
      return children_[0];
    }
  }
}

void *ARTNode16::less_than(uint8_t key) {
  int32_t pos = 0;
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    find_pos(key, pos);
    return pos > 0 ? children_[pos - 1] : value_ptr();
  }
}

void *ARTNode16::greater_than(uint8_t key) {
  int32_t pos = 0;
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    if (find_pos(key, pos)) {
      return pos < count() - 1 ? children_[pos + 1] : nullptr;
    } else {
      return pos < count() ? children_[pos] : nullptr;
    }
  }
}

void ARTNode16::dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) {
  if (is_empty()) {
    SE_LOG(WARN, "Inner node should not be empty when dumping struct info", K(last_node_id), K(last_allocated_node_id));
    fprintf(fp, "|- LEVEL#%ld NODEID#%ld EMPTY -|\n", level, last_node_id++);
  } else {
    fprintf(fp, "|- LEVEL#%ld NODEID#%ld COUNT#%d ", level, last_node_id++, count());
    fprintf(fp, "PREFIXLEN#%u ", prefix_len());
    fprintf(fp, "PREFIX ");
    for (uint16_t i = 0; i < prefix_len(); i++) {
      fprintf(fp, "\\%u", prefix()[i]);
    }
    fprintf(fp, " ");
    for (int32_t i = 0; i < count(); i++) {
      if (IS_ARTVALUE(children_[i])) {
        fprintf(fp, "%u: v ", flip_sign(key_[i]));
      } else {
        fprintf(fp, "%u: %ld ", flip_sign(key_[i]), last_allocated_node_id++);
        bfs_queue.push(VOID2INNERNODE(children_[i]));
      }
    }
    fprintf(fp, "-|\n");
  }
}

/*
 * Find the position of the first key which is greater than or equal to target
 */
bool ARTNode4::find_pos(uint8_t key, int32_t &pos) {
  pos = 0;
  while (pos < count() && key_[pos] < key) {
    pos++;
  }
  return pos < count() && key_[pos] == key ? true : false;
}

int ARTNode4::copy_to_larger(ARTNodeBase *larger) {
  int ret = Status::kOk;
  for (int32_t i = 0; i < count(); i++) {
    if (FAILED(larger->insert(key_[i], children_[i]))) {
      SE_LOG(ERROR, "failed to insert into larger", K(i), K(key_[i]), KP(children_[i]), KP(larger));
      break;
    }
  }
  larger->set_value_ptr(value_ptr());
  return ret;
}

int ARTNode4::init(const uint8_t *prefix, uint16_t prefix_len) {
  int ret = Status::kOk;
  if (FAILED(set_prefix(prefix, prefix_len))) {
    SE_LOG(ERROR, "failed to set prefix", K(prefix), K(prefix_len));
  } else {
    memset(key_, 0, sizeof(uint8_t) * FANOUT);
    memset(children_, 0, sizeof(void *) * FANOUT);
  }
  return ret;
}

void ARTNode4::release() {
  for (int32_t i = 0; i < count(); i++) {
    if (IS_ARTVALUE(children_[i]) == false) {
      RELEASE(children_[i]);
    }
  }
}

int ARTNode4::insert(uint8_t key, void *child) {
  int ret = Status::kOk;
  int32_t pos = 0;
  if (IS_NULL(child)) {
    SE_LOG(ERROR, "invalid argument", KP(child));
    ret = Status::kInvalidArgument;
  } else if (UNLIKELY(find_pos(key, pos))) {
    SE_LOG(ERROR, "child at insert position exists", K(key), KP(child), K(pos), KP(children_[pos]));
    ret = Status::kInsertCheckFailed;
  } else {
    memmove(key_ + pos + 1, key_ + pos, (count() - pos) * sizeof(uint8_t));
    memmove(children_ + pos + 1, children_ + pos, (count() - pos) * sizeof(void *));
    key_[pos] = key;
    children_[pos] = child;
    inc_count();
  }
  return ret;
}

int ARTNode4::update(uint8_t key, void *child) {
  int ret = Status::kOk;
  int32_t pos = 0;
  if (IS_NULL(child)) {
    SE_LOG(ERROR, "invalid argument", KP(child));
    ret = Status::kInvalidArgument;
  } else if (UNLIKELY(find_pos(key, pos) == false)) {
    SE_LOG(ERROR, "child at update position dose not exist", K(key), KP(child), K(pos));
    ret = Status::kErrorUnexpected;
  } else {
    children_[pos] = child;
  }
  return ret;
}

void *ARTNode4::get(uint8_t key) {
  int32_t pos = 0;
  return find_pos(key, pos) ? children_[pos] : nullptr;
}

void *ARTNode4::get_with_less_than_percent(uint8_t key, double &percent) {
  if (UNLIKELY(is_empty())) {
    percent = 0.0;
    return nullptr;
  } else {
    int32_t pos = 0;
    bool found = find_pos(key, pos);
    percent = 1.0 * pos / count();
    return found ? children_[pos] : nullptr;
  }
}

int ARTNode4::expand(ARTNodeBase *&new_node) {
  int ret = Status::kOk;
  if (IS_NULL(new_node = MOD_NEW_OBJECT(ModId::kMemtable, ARTNode16))) { // TODO(nanlong.ynl): use memory pool for alloc
    SE_LOG(ERROR, "failed to alloc memory for ARTNode16");
    ret = Status::kMemoryLimit;
  } else if (FAILED(new_node->init(prefix(), prefix_len()))) {
    SE_LOG(ERROR, "failed to init ARTNode16");
  } else if (FAILED(copy_to_larger(new_node))) {
    SE_LOG(ERROR, "failed to copy to new node", K(ret), KP(new_node));
  }
  return ret;
}

void *ARTNode4::minimum() {
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    if (has_value_ptr()) {
      return value_ptr();
    } else {
      return children_[0];
    }
  }
}

void *ARTNode4::less_than(uint8_t key) {
  int32_t pos = 0;
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    find_pos(key, pos);
    return pos > 0 ? children_[pos - 1] : value_ptr();
  }
}

void *ARTNode4::greater_than(uint8_t key) {
  int32_t pos = 0;
  if (UNLIKELY(is_empty())) {
    return nullptr;
  } else {
    if (find_pos(key, pos)) {
      return pos < count() - 1 ? children_[pos + 1] : nullptr;
    } else {
      return pos < count() ? children_[pos] : nullptr;
    }
  }
}

void ARTNode4::dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) {
  if (is_empty()) {
    SE_LOG(WARN, "Inner node should not be empty when dumping struct info", K(last_node_id), K(last_allocated_node_id));
    fprintf(fp, "|- LEVEL#%ld NODEID#%ld EMPTY -|\n", level, last_node_id++);
  } else {
    fprintf(fp, "|- LEVEL#%ld NODEID#%ld COUNT#%d ", level, last_node_id++, count());
    fprintf(fp, "PREFIXLEN#%u ", prefix_len());
    fprintf(fp, "PREFIX ");
    for (uint16_t i = 0; i < prefix_len(); i++) {
      fprintf(fp, "\\%u", prefix()[i]);
    }
    fprintf(fp, " ");
    for (int32_t i = 0; i < count(); i++) {
      if (IS_ARTVALUE(children_[i])) {
        fprintf(fp, "%u: v ", key_[i]);
      } else {
        fprintf(fp, "%u: %ld ", key_[i], last_allocated_node_id++);
        bfs_queue.push(VOID2INNERNODE(children_[i]));
      }
    }
    fprintf(fp, "-|\n");
  }
}

} // namespace memtable
} // namespace smartengine
