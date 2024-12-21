//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "transactions/transaction_lock_mgr.h"

#include <algorithm>
#include <mutex>
#include <string>
#include <vector>
#include "logger/log_module.h"
#include "monitoring/query_perf_context.h"
#include "transactions/transaction_db_impl.h"
#include "transactions/transaction_db_mutex.h"
#include "util/sync_point.h"

namespace smartengine {
using namespace common;
using namespace monitor;

namespace util {
TransactionLockMgr::TransactionLockMgr(
    TransactionDB* txn_db, size_t default_num_stripes, int64_t max_num_locks,
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory)
    : txn_db_impl_(nullptr),
      default_num_stripes_(default_num_stripes),
      max_num_locks_(max_num_locks),
      mutex_factory_(mutex_factory),
      lock_map_(DEFAULT_LOCK_MAP_STRIPES_NUM, mutex_factory)
{
  txn_db_impl_ = dynamic_cast<TransactionDBImpl*>(txn_db);
  assert(txn_db_impl_);
}

TransactionLockMgr::~TransactionLockMgr() {}

// Returns true if this lock has expired and can be acquired by another
// transaction.
// If false, sets *expire_time to the expiration time of the lock according
// to Env->GetMicros() or 0 if no expiration.
bool TransactionLockMgr::IsLockExpired(const TransactionID trans_id,
                                       const LockInfo& lock_info, Env* env,
                                       uint64_t* expire_time) {
  auto now = env->NowMicros();

  bool expired =
      (lock_info.expiration_time_ > 0 && lock_info.expiration_time_ <= now);

  if (!expired && lock_info.expiration_time_ > 0) {
    // return how many microseconds until lock will be expired
    *expire_time = lock_info.expiration_time_;
  } else {
    for (auto id : lock_info.trans_ids_) {
      if (trans_id == id) {
        continue;
      }

      bool success = txn_db_impl_->TryStealingExpiredTransactionLocks(id);
      if (!success) {
        expired = false;
        break;
      }
      *expire_time = 0;
    }
  }

  return expired;
}

Status TransactionLockMgr::TryLock(TransactionImpl* trans,
                                   uint32_t index_id,
                                   const std::string& key,
                                   Env* env,
                                   bool exclusive)
{
  int ret = Status::kOk;
  LockKey lock_key(index_id, key);
  LockInfo lock_info(index_id, exclusive, trans->GetExpirationTime(), trans->GetID());
  LockMapStripe *stripe = nullptr;

  if (IS_NULL(stripe = lock_map_.get_lock_map_stripe(lock_key))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, stripe must not nullptr", K(ret), KP(trans), K(lock_key), K(lock_info));
  } else if (FAILED(AcquireWithTimeout(trans,
                                       &lock_map_,
                                       stripe,
                                       index_id,
                                       key,
                                       lock_key.hash(),
                                       env,
                                       trans->GetLockTimeout(),
                                       lock_info).code())) {
    SE_LOG(WARN, "fail to acquire lock with timeout", K(ret), KP(trans), KP(stripe), K(lock_key), K(lock_info));
  }

  return Status(ret);
}

// Helper function for TryLock().
Status TransactionLockMgr::AcquireWithTimeout(
    TransactionImpl* txn, LockMap* lock_map, LockMapStripe* stripe,
    uint32_t index_id, const std::string& key, size_t key_hash,
    Env* env, int64_t timeout, const LockInfo& lock_info) {
  Status result;
  uint64_t start_time = 0;
  uint64_t end_time = 0;

  if (timeout > 0) {
    start_time = env->NowMicros();
    end_time = start_time + timeout;
  }

  if (timeout < 0) {
    // If timeout is negative, we wait indefinitely to acquire the lock
    result = stripe->stripe_mutex_->Lock();
  } else {
    result = stripe->stripe_mutex_->TryLockFor(timeout);
  }

  if (!result.ok()) {
    // failed to acquire mutex
    return result;
  }

  // Acquire lock if we are able to
  uint64_t expire_time_hint = 0;
  autovector<TransactionID> wait_ids;
  result = AcquireLocked(lock_map, stripe, key_hash, env, lock_info,
                         &expire_time_hint, &wait_ids);

  if (!result.ok() && timeout != 0) {
    // If we weren't able to acquire the lock, we will keep retrying as long
    // as the timeout allows.
    bool timed_out = false;
    do {
      // Decide how long to wait
      int64_t cv_end_time = -1;

      // Check if held lock's expiration time is sooner than our timeout
      if (expire_time_hint > 0 &&
          (timeout < 0 || (timeout > 0 && expire_time_hint < end_time))) {
        // expiration time is sooner than our timeout
        cv_end_time = expire_time_hint;
      } else if (timeout >= 0) {
        cv_end_time = end_time;
      }

      assert(result.IsBusy() || wait_ids.size() != 0);

      // We are dependent on a transaction to finish, so perform deadlock
      // detection.
      if (wait_ids.size() != 0) {
        if (txn->IsDeadlockDetect()) {
          if (IncrementWaiters(txn, wait_ids)) {
            result = Status(Status::kDeadlock);
            stripe->stripe_mutex_->UnLock();
            return result;
          }
        }
        txn->SetWaitingTxn(wait_ids, index_id, &key);
      }

      TEST_SYNC_POINT("TransactionLockMgr::AcquireWithTimeout:WaitingTxn");
      if (cv_end_time < 0) {
        // Wait indefinitely
        result = stripe->stripe_cv_->Wait(stripe->stripe_mutex_);
      } else {
        uint64_t now = env->NowMicros();
        if (static_cast<uint64_t>(cv_end_time) > now) {
          result = stripe->stripe_cv_->WaitFor(stripe->stripe_mutex_,
                                              cv_end_time - now);
        }
      }

      if (wait_ids.size() != 0) {
        txn->ClearWaitingTxn();
        if (txn->IsDeadlockDetect()) {
          DecrementWaiters(txn, wait_ids);
        }
      }

      if (result.IsTimedOut()) {
        timed_out = true;
        // Even though we timed out, we will still make one more attempt to
        // acquire lock below (it is possible the lock expired and we
        // were never signaled).
      }

      if (result.ok() || result.IsTimedOut()) {
        result = AcquireLocked(lock_map, stripe, key_hash, env, lock_info,
                               &expire_time_hint, &wait_ids);
      }
    } while (!result.ok() && !timed_out);
  }

  stripe->stripe_mutex_->UnLock();

  return result;
}

void TransactionLockMgr::DecrementWaiters(
    const TransactionImpl* txn, const autovector<TransactionID>& wait_ids) {
  std::lock_guard<std::mutex> lock(wait_txn_map_mutex_);
  DecrementWaitersImpl(txn, wait_ids);
}

void TransactionLockMgr::DecrementWaitersImpl(
    const TransactionImpl* txn, const autovector<TransactionID>& wait_ids) {
  auto id = txn->GetID();
  assert(wait_txn_map_.Contains(id));
  wait_txn_map_.Delete(id);

  for (auto wait_id : wait_ids) {
    rev_wait_txn_map_.Get(wait_id)--;
    if (rev_wait_txn_map_.Get(wait_id) == 0) {
      rev_wait_txn_map_.Delete(wait_id);
    }
  }
}

bool TransactionLockMgr::IncrementWaiters(
    const TransactionImpl* txn, const autovector<TransactionID>& wait_ids) {
  auto id = txn->GetID();
  std::vector<TransactionID> queue(txn->GetDeadlockDetectDepth());
  std::lock_guard<std::mutex> lock(wait_txn_map_mutex_);
  assert(!wait_txn_map_.Contains(id));
  wait_txn_map_.Insert(id, wait_ids);

  for (auto wait_id : wait_ids) {
    if (rev_wait_txn_map_.Contains(wait_id)) {
      rev_wait_txn_map_.Get(wait_id)++;
    } else {
      rev_wait_txn_map_.Insert(wait_id, 1);
    }
  }

  // No deadlock if nobody is waiting on self.
  if (!rev_wait_txn_map_.Contains(id)) {
    return false;
  }

  const auto* next_ids = &wait_ids;
  for (int tail = 0, head = 0; head < txn->GetDeadlockDetectDepth(); head++) {
    int i = 0;
    if (next_ids) {
      for (; i < static_cast<int>(next_ids->size()) &&
             tail + i < txn->GetDeadlockDetectDepth();
           i++) {
        queue[tail + i] = (*next_ids)[i];
      }
      tail += i;
    }

    // No more items in the list, meaning no deadlock.
    if (tail == head) {
      return false;
    }

    auto next = queue[head];
    if (next == id) {
      DecrementWaitersImpl(txn, wait_ids);
      return true;
    } else if (!wait_txn_map_.Contains(next)) {
      next_ids = nullptr;
      continue;
    } else {
      next_ids = &wait_txn_map_.Get(next);
    }
  }

  // Wait cycle too big, just assume deadlock.
  DecrementWaitersImpl(txn, wait_ids);
  return true;
}

// Try to lock this key after we have acquired the mutex.
// Sets *expire_time to the expiration time in microseconds
//  or 0 if no expiration.
// REQUIRED:  Stripe mutex must be held.
Status TransactionLockMgr::AcquireLocked(LockMap* lock_map,
                                         LockMapStripe* stripe,
                                         const size_t key, Env* env,
                                         const LockInfo& txn_lock_info,
                                         uint64_t* expire_time,
                                         autovector<TransactionID>* txn_ids) {
  assert(txn_lock_info.trans_ids_.size() == 1);

  Status result;

  // acquire lock
  auto stripe_iter = stripe->keys_.insert({key, txn_lock_info});

  if (stripe_iter.second) {
    // Check lock limit
    if (max_num_locks_ > 0 &&
        lock_map->lock_cnt_.load(std::memory_order_acquire) >= max_num_locks_) {
      stripe->keys_.erase(stripe_iter.first);
      result = Status(Status::kLockLimit);
      return result;
    }

    // Maintain lock count if there is a limit on the number of locks
    if (max_num_locks_) {
      lock_map->lock_cnt_++;
    }
  } else {
    // insert failed with dup key, as lock already held
    LockInfo& lock_info = stripe_iter.first->second;
    assert(lock_info.trans_ids_.size() == 1 || !lock_info.exclusive_);

    if (lock_info.exclusive_ || txn_lock_info.exclusive_) {
      if (lock_info.trans_ids_.size() == 1 &&
          lock_info.trans_ids_[0] == txn_lock_info.trans_ids_[0]) {
        // The list contains one txn and we're it, so just take it.
        lock_info.exclusive_ = txn_lock_info.exclusive_;
        lock_info.expiration_time_ = txn_lock_info.expiration_time_;
      } else {
        // Check if it's expired. Skips over txn_lock_info.txn_ids[0] in case
        // it's there for a shared lock with multiple holders which was not
        // caught in the first case.
        if (IsLockExpired(txn_lock_info.trans_ids_[0], lock_info, env,
                          expire_time)) {
          // lock is expired, can steal it
          lock_info.trans_ids_ = txn_lock_info.trans_ids_;
          lock_info.exclusive_ = txn_lock_info.exclusive_;
          lock_info.expiration_time_ = txn_lock_info.expiration_time_;
          // lock_cnt does not change
        } else {
          result = Status::TimedOut();
          *txn_ids = lock_info.trans_ids_;
        }
      }
    } else {
      // We are requesting shared access to a shared lock, so just grant it.
      lock_info.trans_ids_.push_back(txn_lock_info.trans_ids_[0]);
      // Using std::max means that expiration time never goes down even when
      // a transaction is removed from the list. The correct solution would be
      // to track expiry for every transaction, but this would also work for
      // now.
      lock_info.expiration_time_ =
          std::max(lock_info.expiration_time_, txn_lock_info.expiration_time_);
    }
  }

  return result;
}

void TransactionLockMgr::UnLockKey(const TransactionImpl* txn, size_t key,
                                   LockMapStripe* stripe, LockMap* lock_map,
                                   Env* env) {
  TransactionID txn_id = txn->GetID();

  auto stripe_iter = stripe->keys_.find(key);
  if (stripe_iter != stripe->keys_.end()) {
    auto& txns = stripe_iter->second.trans_ids_;
    auto txn_it = std::find(txns.begin(), txns.end(), txn_id);
    // Found the key we locked.  unlock it.
    if (txn_it != txns.end()) {
      if (txns.size() == 1) {
        stripe->keys_.erase(stripe_iter);
      } else {
        auto last_it = txns.end() - 1;
        if (txn_it != last_it) {
          *txn_it = *last_it;
        }
        txns.pop_back();
      }

      if (max_num_locks_ > 0) {
        // Maintain lock count if there is a limit on the number of locks.
        assert(lock_map->lock_cnt_.load(std::memory_order_relaxed) > 0);
        lock_map->lock_cnt_--;
      }
    }
  } else {
    // This key is either not locked or locked by someone else.  This should
    // only happen if the unlocking transaction has expired.
    assert(txn->GetExpirationTime() > 0 &&
           txn->GetExpirationTime() < env->NowMicros());
  }
}

void TransactionLockMgr::UnLock(TransactionImpl* trans,
                                uint32_t index_id,
                                const std::string& key,
                                Env* env)
{
  int ret = Status::kOk;
  LockKey lock_key(index_id, key);
  LockMapStripe *stripe = nullptr;

  if (IS_NULL(stripe = lock_map_.get_lock_map_stripe(lock_key))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, stripe must not nullptr", K(ret), K(lock_key));
  } else {
    stripe->stripe_mutex_->Lock();
    UnLockKey(trans, lock_key.hash(), stripe, &lock_map_, env);
    stripe->stripe_mutex_->UnLock();
    // signal waiting threads to retry locking
    stripe->stripe_cv_->NotifyAll();
  }
}

void TransactionLockMgr::UnLock(const TransactionImpl* trans,
                                const TransactionKeyMap* key_map,
                                Env* env)
{
  int ret = Status::kOk;
  LockMapStripe *stripe = nullptr;

  for (auto& key_map_iter : *key_map) {
    uint32_t index_id = key_map_iter.first;
    auto& keys = key_map_iter.second;

    // for common case, single key is operated
    if (1 == keys.size()) {
      LockKey lock_key(index_id, keys.begin()->first);
      if (IS_NULL(stripe = lock_map_.get_lock_map_stripe(lock_key))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, stripe must not nullptr", K(ret), K(lock_key));
      } else {
        stripe->stripe_mutex_->Lock();
        UnLockKey(trans, lock_key.hash(), stripe, &lock_map_, env);
        stripe->stripe_mutex_->UnLock();
        //signal waiting threads to retry locking
        stripe->stripe_cv_->NotifyAll();
      }
    } else {
      // Bucket keys by lock_map_ stripe
      std::unordered_map<size_t, std::vector<size_t>> keys_by_stripe;
      for (auto& key_iter : keys) {
        LockKey lock_key(index_id, key_iter.first);
        keys_by_stripe[lock_map_.get_lock_map_stripe_index(lock_key)].push_back(lock_key.hash());
      }

      // For each stripe, grab the stripe mutex and unlock all keys in this stripe
      for (auto& stripe_iter : keys_by_stripe) {
        if (IS_NULL(stripe = lock_map_.get_lock_map_stripe(stripe_iter.first))) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, stripe must not nullptr", K(ret), "stripe_index", stripe_iter.first);
          abort();
        } else {
          stripe->stripe_mutex_->Lock();
          for (size_t key_hash : stripe_iter.second) {
            UnLockKey(trans, key_hash, stripe, &lock_map_, env);
          }
          stripe->stripe_mutex_->UnLock();
          // signal waiting threads to retry locking
          stripe->stripe_cv_->NotifyAll();
        }
      }
    }
  }
}

TransactionLockMgr::LockStatusData TransactionLockMgr::GetLockStatusData()
{
  int ret = Status::kOk;
  LockStatusData lock_status_data;
  LockMapStripe *stripe = nullptr;

  for (size_t i = 0; SUCCED(ret) && i < lock_map_.stripes_num_; ++i) {
    if (IS_NULL(stripe = lock_map_.get_lock_map_stripe(i))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, stripe must not nullptr", K(ret), K(i));
    } else {
      stripe->stripe_mutex_->Lock();
      for (auto key_iter = stripe->keys_.begin(); key_iter != stripe->keys_.end(); ++key_iter) {
        struct KeyLockInfo key_lock_info;
        key_lock_info.exclusive = key_iter->second.exclusive_;
        key_lock_info.key = std::to_string(key_iter->first);
        for (auto trans_id_iter = key_iter->second.trans_ids_.begin(); trans_id_iter != key_iter->second.trans_ids_.end(); ++trans_id_iter) {
          key_lock_info.ids.push_back(*trans_id_iter);
        }
        lock_status_data.insert({key_iter->second.index_id_, key_lock_info});
      }
    }
  }

  //Unlock everything. Unlocking order is not important
  for (size_t i = 0; SUCCED(ret) && i < lock_map_.stripes_num_; ++i) {
    lock_map_.get_lock_map_stripe(i)->stripe_mutex_->UnLock();
  }

  return lock_status_data;
}

}  //  namespace util
}  //  namespace smartengine