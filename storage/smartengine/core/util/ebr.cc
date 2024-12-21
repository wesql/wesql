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

#include "util/ebr.h"
#include "logger/log_module.h"
#include "util/status.h"

namespace smartengine {
using namespace common;
using namespace memory;
using namespace logger;

namespace util {

void LocalEBR::enter_critical_area(uint32_t global_epoch) {
  assert(EBRManager::is_epoch_active(global_epoch));
  local_epoch_.store(global_epoch);
}

void LocalEBR::leave_critical_area() {
  if (local_limbo_list_head_ != nullptr && local_limbo_list_tail_ != nullptr) {
    ebr_->add_to_limbo_list(local_limbo_list_head_, local_limbo_list_tail_, local_removed_count_);
    local_limbo_list_head_ = nullptr;
    local_limbo_list_tail_ = nullptr;
    local_removed_count_ = 0;
  }
  assert(local_limbo_list_head_ == nullptr && local_limbo_list_tail_ == nullptr);
  local_epoch_.fetch_xor(EBRManager::ACTIVE_MASK);
}

int LocalEBR::remove(void *p, EBRDeleteFunc f) {
  int ret = Status::kOk;
  EBRNode *node = nullptr;
  if (IS_NULL(node = MOD_NEW_OBJECT(ModId::kDefaultMod, EBRNode, p, f))) {
    SE_LOG(ERROR, "failed to alloc memory for ebr node", KP(node), KP(p));
    ret = Status::kMemoryLimit;
  } else if (local_limbo_list_head_ == nullptr && local_limbo_list_tail_ == nullptr) {
    local_limbo_list_head_ = node;
    local_limbo_list_tail_ = node;
  } else {
    node->set_next(local_limbo_list_head_);
    local_limbo_list_head_ = node;
  }

  if (SUCCED(ret)) {
    local_removed_count_++;
  }
  return ret;
}

void EBRManager::add_to_limbo_list(EBRNode *head, EBRNode *tail, uint64_t removed_count) {
  EBRNode *old = curr_limbo_list_.next();
  do {
    tail->set_next(old);
  } while (curr_limbo_list_.next_cas(old, head) == false);
  removed_count_.fetch_add(removed_count);
}

int EBRManager::thread_register(LocalEBR *&e) {
  int ret = Status::kOk;
  if (IS_NULL(e = new (std::nothrow) LocalEBR(this))) {
    SE_LOG(ERROR, "failed to alloc memory for local ebr", KP(e));
    ret = Status::kMemoryLimit;
  } else if (pthread_setspecific(local_epoch_, e) != 0) {
    SE_LOG(ERROR, "failed to set pthread_key for local ebr", KP(e));
    ret = Status::kErrorUnexpected;
  } else {
    SpinLockGuard guard(lock_);
    e->set_next(local_epoch_list_.next());
    e->set_prev(&local_epoch_list_);
    local_epoch_list_.next()->set_prev(e);
    local_epoch_list_.set_next(e);
  }
  return ret;
}

void EBRManager::thread_exit(void *ptr) {
  if (ptr != nullptr) {
    LocalEBR *e = reinterpret_cast<LocalEBR *>(ptr);
    if (is_epoch_active(e->get_local_epoch())) {
      e->leave_critical_area();
    }
    {
      SpinLockGuard guard(EBRMANAGER.get_lock());
      e->prev()->set_next(e->next());
      e->next()->set_prev(e->prev());
      e->set_next(nullptr);
      e->set_prev(nullptr);
    }
    pthread_setspecific(EBRMANAGER.local_epoch_thread_key(), nullptr);
    delete e;
  }
}

void EBRManager::enter_critical_area() {
  int ret = Status::kOk;
  LocalEBR *e = get_local_ebr();
  if (IS_NULL(e) && FAILED(thread_register(e))) {
    SE_LOG(ERROR, "failed to register thread into ebr manager");
    abort();
  }
  assert(e != nullptr);
  e->enter_critical_area(epoch2active(global_epoch_.load()));
}

void EBRManager::leave_critical_area() {
  LocalEBR *e = get_local_ebr();
  assert(e != nullptr);
  e->leave_critical_area();
}

int EBRManager::remove(void *p, EBRDeleteFunc f) {
  int ret = Status::kOk;
  LocalEBR *e = get_local_ebr();
  assert(e != nullptr);
  if (FAILED(e->remove(p, f))) {
    SE_LOG(ERROR, "failed to remove");
  }
  return ret;
}

bool EBRManager::maybe_forward_epoch(uint32_t &gc_epoch) {
  uint32_t global_epoch = global_epoch_.load();
  {
    SpinLockGuard guard(lock_);
    LocalEBR *e = local_epoch_list_.next();
    while (e != &local_epoch_list_) {
      uint32_t local_epoch = e->get_local_epoch();
      if (is_epoch_active(local_epoch) && epoch2active(global_epoch) != local_epoch) {
        gc_epoch = get_gc_epoch();
        return false;
      }
      e = e->next();
    }
  }
  forward_epoch();
  gc_epoch = get_gc_epoch();
  return true;
}

void EBRManager::reclaim(EBRNode *retired_list) {
  if (retired_list != nullptr) {
    uint64_t reclaimed_count = 0;
    do {
      EBRNode *curr = retired_list;
      retired_list = retired_list->next();
      curr->do_reclaim();
      MOD_DELETE_OBJECT(EBRNode, curr);
      reclaimed_count++;
    } while (retired_list != nullptr);
    removed_count_.fetch_sub(reclaimed_count);
    SE_LOG(INFO, "ebr reclaim done", K(reclaimed_count));
  }
}

void EBRManager::maybe_do_reclaim() {
  uint32_t gc_epoch = 0;
  uint32_t staging_epoch = 0;
  bool running = false;
  if (need_reclaim()) {
    SE_LOG(INFO, "ebr reclaim start");
    int loop_count = 0;
    while (loop_count < static_cast<int>(MAX_EPOCH) && maybe_forward_epoch(gc_epoch) == true) {
      staging_epoch = get_staging_epoch();
      limbo_list_[staging_epoch].set_next(curr_limbo_list_.next_exchange(nullptr));
      EBRNode *retired_list = limbo_list_[gc_epoch].next_exchange(nullptr);
      reclaim(retired_list);
      loop_count++;
    }
  }
}

} // namespace util
} // namespace smartengine
