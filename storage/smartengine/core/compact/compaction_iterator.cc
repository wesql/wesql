//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "compact/compaction_iterator.h"

#include "storage/change_info.h"
#include "table/internal_iterator.h"

namespace smartengine {
using namespace common;
using namespace storage;
using namespace util;
using namespace table;
using namespace db;
using namespace memory;

namespace storage {

CompactionIterator::CompactionIterator(
    InternalIterator* input,
    const Comparator* cmp,
    SequenceNumber last_sequence,
    std::vector<SequenceNumber>* snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    Env* env,
    bool expect_valid_internal_key,
    ChangeInfo &change_info,
    ArenaAllocator  &arena,
    const std::atomic<bool>* shutting_down,
    const std::atomic<bool>* bg_stopped,
    const std::atomic<int64_t>* cancel_type,
    const Slice *l2_largest_key,
    const bool background_disable_merge)
    : input_(input),
      cmp_(cmp),
      snapshots_(snapshots),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      expect_valid_internal_key_(expect_valid_internal_key),
      shutting_down_(shutting_down),
      bg_stopped_(bg_stopped),
      cancel_type_(cancel_type),
      valid_(false),
      change_info_(change_info),
      l2_largest_key_(l2_largest_key),
      background_disable_merge_(background_disable_merge) {

  if (snapshots_->size() == 0) {
    // optimize for fast path if there are no snapshots
    visible_at_tip_ = true;
    earliest_snapshot_ = last_sequence;
    latest_snapshot_ = 0;
  } else {
    visible_at_tip_ = false;
    earliest_snapshot_ = snapshots_->at(0);
    latest_snapshot_ = snapshots_->back();
  }
  input_->SetPinnedItersMgr(&pinned_iters_mgr_);
}
CompactionIterator::~CompactionIterator() {
  // input_ Iteartor lifetime is longer than pinned_iters_mgr_ lifetime
  input_->SetPinnedItersMgr(nullptr);
}

bool CompactionIterator::is_canceled() {
  return (cancel_type_) && (cancel_type_->load(std::memory_order_relaxed) & (1LL << change_info_.task_type_));
}

void CompactionIterator::ResetRecordCounts() {
  iter_stats_.num_record_drop_user = 0;
  iter_stats_.num_record_drop_hidden = 0;
  iter_stats_.num_record_drop_obsolete = 0;
}

void CompactionIterator::SeekToFirst() {
  NextFromInput();
  PrepareOutput();
}

void CompactionIterator::Next() {
  // If there is a merge output, return it before continuing to process the
  // input.
  // Only advance the input iterator if there is no merge output and the
  // iterator is not already at the next record.
  if (!at_next_) {
    input_->Next();
    status_ = input_->status().code();
  }
  NextFromInput();

  if (valid_) {
    // Record that we've outputted a record for the current key.
    has_outputted_key_ = true;
  }

  PrepareOutput();
}

void CompactionIterator::record_large_objects_info(
    const Slice &large_key, const Slice &large_value) {
  UNUSED(large_key);
  UNUSED(large_value);
//  const Slice tkey = large_key.deep_copy(arena_);
//  const Slice tval = large_value.deep_copy(arena_);
//  change_info_.drop_large_objects_.emplace_back(tkey, tval);
}

void CompactionIterator::NextFromInput() {
  at_next_ = false;
  valid_ = false;

  while (!valid_
         && status_.ok()
         && input_->Valid()
         && !IsShuttingDown()
         && !is_bg_stopped()) {
    key_ = input_->key();
    value_ = input_->value();
    iter_stats_.num_input_records++;

    if (!ParseInternalKey(key_, &ikey_)) {
      // If `expect_valid_internal_key_` is false, return the corrupted key
      // and let the caller decide what to do with it.
      // TODO(noetzli): We should have a more elegant solution for this.
      if (expect_valid_internal_key_) {
        assert(!"Corrupted internal key not expected.");
        status_ = Status::Corruption("Corrupted internal key not expected.");
        break;
      }
      key_ = current_key_.SetInternalKey(key_);
      has_current_user_key_ = false;
      current_user_key_sequence_ = kMaxSequenceNumber;
      current_user_key_snapshot_ = 0;
      iter_stats_.num_input_corrupt_records++;
      valid_ = true;
      break;
    }

    // Update input statistics
    if (ikey_.type == kTypeDeletion || ikey_.type == kTypeSingleDeletion) {
      iter_stats_.num_input_deletion_records++;
    }
    iter_stats_.total_input_raw_key_bytes += key_.size();
    iter_stats_.total_input_raw_value_bytes += value_.size();

    // If need_skip is true, we should seek the input iterator
    // to internal key skip_until and continue from there.
    bool need_skip = false;
    // Points either into compaction_filter_skip_until_ or into
    // merge_helper_->compaction_filter_skip_until_.
    Slice skip_until;

    // Check whether the user key changed. After this if statement current_key_
    // is a copy of the current input key (maybe converted to a delete by the
    // compaction filter). ikey_.user_key is pointing to the copy.
    if (!has_current_user_key_ ||
        !cmp_->Equal(ikey_.user_key, current_user_key_)) {
      // First occurrence of this user key
      // Copy key for output
      key_ = current_key_.SetInternalKey(key_, &ikey_);
      current_user_key_ = ikey_.user_key;
      has_current_user_key_ = true;
      has_outputted_key_ = false;
      current_user_key_sequence_ = kMaxSequenceNumber;
      current_user_key_snapshot_ = 0;

    } else {

      // Update the current key to reflect the new sequence number/type without
      // copying the user key.
      // TODO(rven): Compaction filter does not process keys in this path
      // Need to have the compaction filter process multiple versions
      // if we have versions on both sides of a snapshot
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      key_ = current_key_.GetInternalKey();
      ikey_.user_key = current_key_.GetUserKey();
    }

    //when background_disable_merge_ is true, that means it's new subtable, and we should maintain all records before index-build ready.
    if (true == background_disable_merge_) {
       valid_ = true;
       status_ = input_->status();
       break;
    }

    // If there are no snapshots, then this kv affect visibility at tip.
    // Otherwise, search though all existing snapshots to find the earliest
    // snapshot that is affected by this kv.
    SequenceNumber last_sequence __attribute__((__unused__)) =
        current_user_key_sequence_;
    current_user_key_sequence_ = ikey_.sequence;
    SequenceNumber last_snapshot = current_user_key_snapshot_;
    SequenceNumber prev_snapshot = 0;  // 0 means no previous snapshot
    current_user_key_snapshot_ =
        visible_at_tip_
            ? earliest_snapshot_
            : findEarliestVisibleSnapshot(ikey_.sequence, &prev_snapshot);

    if (need_skip) {
      // This case is handled below.
    } else if (clear_and_output_next_key_) {
      // In the previous iteration we encountered a single delete that we could
      // not compact out.  We will keep this Put, but can drop it's data.
      // (See Optimization 3, below.)
      assert(ikey_.type == kTypeValue || kTypeValueLarge == ikey_.type);
      assert(current_user_key_snapshot_ == last_snapshot);

      if (kTypeValueLarge == ikey_.type) {
        record_large_objects_info(key_, value_);
      }
      value_.clear();
      valid_ = true;
      clear_and_output_next_key_ = false;
   } else if (ikey_.type == kTypeSingleDeletion) {
      // We can compact out a SingleDelete if:
      // 1) We encounter the corresponding PUT -OR- we know that this key
      //    doesn't appear past this output level
      // =AND=
      // 2) We've already returned a record in this snapshot -OR-
      //    there are no earlier earliest_write_conflict_snapshot.
      //
      // Rule 1 is needed for SingleDelete correctness.  Rule 2 is needed to
      // allow Transactions to do write-conflict checking (if we compacted away
      // all keys, then we wouldn't know that a write happened in this
      // snapshot).  If there is no earlier snapshot, then we know that there
      // are no active transactions that need to know about any writes.
      //
      // Optimization 3:
      // If we encounter a SingleDelete followed by a PUT and Rule 2 is NOT
      // true, then we must output a SingleDelete.  In this case, we will decide
      // to also output the PUT.  While we are compacting less by outputting the
      // PUT now, hopefully this will lead to better compaction in the future
      // when Rule 2 is later true (Ie, We are hoping we can later compact out
      // both the SingleDelete and the Put, while we couldn't if we only
      // outputted the SingleDelete now).
      // In this case, we can save space by removing the PUT's value as it will
      // never be read.
      //
      // Deletes and Merges are not supported on the same key that has a
      // SingleDelete as it is not possible to correctly do any partial
      // compaction of such a combination of operations.  The result of mixing
      // those operations for a given key is documented as being undefined.  So
      // we can choose how to handle such a combinations of operations.  We will
      // try to compact out as much as we can in these cases.
      // We will report counts on these anomalous cases.

      // The easiest way to process a SingleDelete during iteration is to peek
      // ahead at the next key.
      ParsedInternalKey next_ikey;
      input_->Next();

      // Check whether the next key exists, is not corrupt, and is the same key
      // as the single delete.
      if (input_->Valid() && ParseInternalKey(input_->key(), &next_ikey) &&
          cmp_->Equal(ikey_.user_key, next_ikey.user_key)) {
        // Check whether the next key belongs to the same snapshot as the
        // SingleDelete.
        if (prev_snapshot == 0 || next_ikey.sequence > prev_snapshot) {
          if (next_ikey.type == kTypeSingleDeletion) {
            // We encountered two SingleDeletes in a row.  This could be due to
            // unexpected user input.
            // Skip the first SingleDelete and let the next iteration decide how
            // to handle the second SingleDelete

            // First SingleDelete has been skipped since we already called
            // input_->Next().
            ++iter_stats_.num_record_drop_obsolete;
            ++iter_stats_.num_single_del_mismatch;
          } else if ((ikey_.sequence <= earliest_write_conflict_snapshot_) ||
                     has_outputted_key_) {
            // Found a matching value, we can drop the single delete and the
            // value.  It is safe to drop both records since we've already
            // outputted a key in this snapshot, or there is no earlier
            // snapshot (Rule 2 above).

            // Note: it doesn't matter whether the second key is a Put or if it
            // is an unexpected Merge or Delete.  We will compact it out
            // either way. We will maintain counts of how many mismatches
            // happened
            if (next_ikey.type != kTypeValue) {
              ++iter_stats_.num_single_del_mismatch;
            }

            ++iter_stats_.num_record_drop_hidden;
            ++iter_stats_.num_record_drop_obsolete;
            // Already called input_->Next() once.  Call it a second time to
            // skip past the second key.
            if (kTypeValueLarge == next_ikey.type) {
              record_large_objects_info(input_->key(), input_->value());
            }
            input_->Next();
          } else {
            // Found a matching value, but we cannot drop both keys since
            // there is an earlier snapshot and we need to leave behind a record
            // to know that a write happened in this snapshot (Rule 2 above).
            // Clear the value and output the SingleDelete. (The value will be
            // outputted on the next iteration.)

            // Setting valid_ to true will output the current SingleDelete
            valid_ = true;

            // Set up the Put to be outputted in the next iteration.
            // (Optimization 3).
            clear_and_output_next_key_ = true;
          }
        } else {
          // We hit the next snapshot without hitting a put, so the iterator
          // returns the single delete.
          valid_ = true;
        }
      } else {
        // We are at the end of the input, could not parse the next key, or hit
        // a different key. The iterator returns the single delete if the key
        // possibly exists beyond the current output level.  We set
        // has_current_user_key to false so that if the iterator is at the next
        // key, we do not compare it again against the previous key at the next
        // iteration. If the next key is corrupt, we return before the
        // comparison, so the value of has_current_user_key does not matter.
        has_current_user_key_ = false;
        if (nullptr != l2_largest_key_
            && ikey_.sequence <= earliest_snapshot_
            && cmp_->Compare(ikey_.user_key, *l2_largest_key_) > 0) {
          // Key doesn't exist outside of this range.
          // Can compact out this SingleDelete.
          ++iter_stats_.num_record_drop_obsolete;
          ++iter_stats_.num_single_del_fallthru;
        } else {
          // Output SingleDelete
          valid_ = true;
        }
      }

      if (valid_) {
        at_next_ = true;
      }
    } else if (last_snapshot == current_user_key_snapshot_) {
      // If the earliest snapshot is which this key is visible in
      // is the same as the visibility of a previous instance of the
      // same key, then this kv is not visible in any snapshot.
      // Hidden by an newer entry for same user key
      // TODO(noetzli): why not > ?
      //
      // Note: Dropping this key will not affect TransactionDB write-conflict
      // checking since there has already been a record returned for this key
      // in this snapshot.
      assert(last_sequence >= current_user_key_sequence_);
      ++iter_stats_.num_record_drop_hidden;  // (A)
      if (kTypeValueLarge == ikey_.type) {
        record_large_objects_info(key_, value_);
      }
      input_->Next();
    } else if (nullptr != l2_largest_key_
               && ikey_.type == kTypeDeletion
               && ikey_.sequence <= earliest_snapshot_
               && cmp_->Compare(ikey_.user_key, *l2_largest_key_) > 0) {
      // TODO(noetzli): This is the only place where we use compaction_
      // (besides the constructor). We should probably get rid of this
      // dependency and find a way to do similar filtering during flushes.
      //
      // For this user key:
      // (1) there is no data in higher levels
      // (2) data in lower levels will have larger sequence numbers
      // (3) data in layers that are being compacted here and have
      //     smaller sequence numbers will be dropped in the next
      //     few iterations of this loop (by rule (A) above).
      // Therefore this deletion marker is obsolete and can be dropped.
      //
      // Note:  Dropping this Delete will not affect TransactionDB
      // write-conflict checking since it is earlier than any snapshot.
      ++iter_stats_.num_record_drop_obsolete;
      input_->Next();
    } else if (ikey_.type == kTypeMerge) {
      // not support type merge
    } else {
      // 1. new user key -OR-
      // 2. different snapshot stripe
      valid_ = true;
    }

    if (need_skip) {
      input_->Seek(skip_until);
    }
    status_ = input_->status();
  }
  if (!valid_ && (IsShuttingDown() || is_bg_stopped())) {
    status_ = Status::ShutdownInProgress();
    SE_LOG(INFO, "has been shutted down or bg_stopped", K((int)status_.code()));
  }
  if (status_.ok() && is_canceled()) {
    status_ = Status(Status::kCancelTask);
    COMPACTION_LOG(INFO, "task has been canceled", K((int)status_.code()), K(get_task_type_name(change_info_.task_type_)));
  }
}

void CompactionIterator::PrepareOutput() {
  // Zeroing out the sequence number leads to better compression.
  // If this is the bottommost level (no files in lower levels)
  // and the earliest snapshot is larger than this seqno
  // and the userkey differs from the last userkey in compaction
  // then we can squash the seqno to zero.

  // This is safe for TransactionDB write-conflict checking since transactions
  // only care about sequence number larger than any active snapshots.
  //  if (bottommost_level_ && valid_ && ikey_.sequence < earliest_snapshot_ &&
  //      ikey_.type != kTypeMerge &&
  //      !cmp_->Equal(compaction_->GetLargestUserKey(), ikey_.user_key)) {
  //    assert(ikey_.type != kTypeDeletion && ikey_.type !=
  //    kTypeSingleDeletion);
  //    ikey_.sequence = 0;
  //    current_key_.UpdateInternalKey(0, ikey_.type);
  //  }
  // do nothing
//  const SeSchema *cur_schema = nullptr;
//  if (nullptr == input_) {
//    status_ = Status::InvalidArgument(
//        "input is not properly initialized.");
//  } else if (valid_
//             && nullptr != (cur_schema = input_->get_schema())
//             && nullptr != dst_schema_) {
//    if (nullptr == row_arena_) {
//      status_ = Status::Incomplete("row_arena is not inited");
//    } else if (cur_schema->get_schema_version() < dst_schema_->get_schema_version()) {
//      int ret = 0;
//      Slice key = key_;
//      Slice tmp_value = value_;
//      uint64_t num = util::DecodeFixed64(key.data() + key.size() - 8);
//      unsigned char c = num & 0xff;
//      ValueType type = static_cast<ValueType>(c);
//      if (kTypeValue == type && value_.size() > 0
//          && Status::kOk != (ret = FieldExtractor::get_instance()->convert_schema(
//              cur_schema, dst_schema_, value_, tmp_value, *row_arena_))) {
//        COMPACTION_LOG(WARN, "switch value failed.", K(ret),
//                       K(util::to_cstring(value_)));
//        Status tmp_status(ret);
//        status_ = tmp_status;
//      } else {
//        value_ = tmp_value;
//      }
//    }
//  }
}

inline SequenceNumber CompactionIterator::findEarliestVisibleSnapshot(
    SequenceNumber in, SequenceNumber* prev_snapshot) {
  assert(snapshots_->size());
  SequenceNumber prev __attribute__((__unused__)) = kMaxSequenceNumber;
  for (const auto cur : *snapshots_) {
    assert(prev == kMaxSequenceNumber || prev <= cur);
    if (cur >= in) {
      *prev_snapshot = prev == kMaxSequenceNumber ? 0 : prev;
      return cur;
    }
    prev = cur;
    assert(prev < kMaxSequenceNumber);
  }
  *prev_snapshot = prev;
  return kMaxSequenceNumber;
}

}  // namespace db
}  // namespace smartengine
