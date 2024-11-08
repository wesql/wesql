//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"
#include "db/pinned_iterators_manager.h"
#include "options/options.h"
#include "table/iterator_wrapper.h"

namespace smartengine {
using namespace common;
using namespace db;
using namespace util;
using namespace monitor;

namespace table {
namespace {

class TwoLevelIterator : public InternalIterator {
 public:
  explicit TwoLevelIterator(TwoLevelIteratorState* state,
                            InternalIterator* first_level_iter,
                            TracePoint point,
                            bool need_free_iter_and_state);

  virtual ~TwoLevelIterator() override
  {
    // Assert that the TwoLevelIterator is never deleted while Pinning is
    // Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
    first_level_iter_.DeleteIter(!need_free_iter_and_state_);
    second_level_iter_.DeleteIter(false);
    if (need_free_iter_and_state_) {
      MOD_DELETE_OBJECT(TwoLevelIteratorState, state_);
    } else {
      state_->~TwoLevelIteratorState();
    }
  }

  virtual void Seek(const Slice& target) override;
  virtual void SeekForPrev(const Slice& target) override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Next() override;
  virtual void Prev() override;

  virtual bool Valid() const override { return second_level_iter_.Valid(); }
  virtual Slice key() const override {
    assert(Valid());
    return second_level_iter_.key();
  }
  virtual Slice value() const override {
    assert(Valid());
    return second_level_iter_.value();
  }
  virtual Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!first_level_iter_.status().ok()) {
      return first_level_iter_.status();
    } else if (second_level_iter_.iter() != nullptr &&
               !second_level_iter_.status().ok()) {
      return second_level_iter_.status();
    } else {
      return status_;
    }
  }
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    first_level_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    if (second_level_iter_.iter()) {
      second_level_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }
  virtual bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           second_level_iter_.iter() && second_level_iter_.IsKeyPinned();
  }
  virtual bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           second_level_iter_.iter() && second_level_iter_.IsValuePinned();
  }
  virtual void set_end_key(const Slice& end_key_slice, const bool need_seek_end_key) override
  {
    end_ikey_ = end_key_slice;
    first_level_iter_.iter()->set_end_key(end_key_slice, need_seek_end_key);
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetSecondLevelIterator(InternalIterator* iter);
  void InitDataBlock();

  TwoLevelIteratorState* state_;
  IteratorWrapper first_level_iter_;
  IteratorWrapper second_level_iter_;  // May be nullptr
  TracePoint point_;
  bool need_free_iter_and_state_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  Status status_;
  // If second_level_iter is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the second_level_iter.
  std::string data_block_handle_;
  uint64_t add_blocks_;
};

TwoLevelIterator::TwoLevelIterator(TwoLevelIteratorState* state,
                                   InternalIterator* first_level_iter,
                                   TracePoint point,
                                   bool need_free_iter_and_state)
    : state_(state),
      first_level_iter_(first_level_iter),
      point_(point),
      need_free_iter_and_state_(need_free_iter_and_state),
      pinned_iters_mgr_(nullptr),
      add_blocks_(0) {}

void TwoLevelIterator::Seek(const Slice& target) {
  first_level_iter_.Seek(target);

  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.Seek(target);
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekForPrev(const Slice& target) {
  first_level_iter_.Seek(target);
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekForPrev(target);
  }
  if (!Valid()) {
    if (!first_level_iter_.Valid()) {
      first_level_iter_.SeekToLast();
      InitDataBlock();
      if (second_level_iter_.iter() != nullptr) {
        second_level_iter_.SeekForPrev(target);
      }
    }
    SkipEmptyDataBlocksBackward();
  }
}

void TwoLevelIterator::SeekToFirst() {
  first_level_iter_.SeekToFirst();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.iter()->set_end_key(end_ikey_, first_level_iter_.iter()->get_is_boundary());
    second_level_iter_.SeekToFirst();
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  first_level_iter_.SeekToLast();
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekToLast();
  }
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  second_level_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  second_level_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() &&
          !second_level_iter_.status().IsIncomplete())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    QUERY_TRACE_SCOPE(point_);
    first_level_iter_.Next();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.iter()->set_end_key(end_ikey_, first_level_iter_.iter()->get_is_boundary());
      second_level_iter_.SeekToFirst();
    }
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() &&
          !second_level_iter_.status().IsIncomplete())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Prev();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToLast();
    }
  }
}

void TwoLevelIterator::SetSecondLevelIterator(InternalIterator* iter) {
  if (second_level_iter_.iter() != nullptr) {
    SaveError(second_level_iter_.status());
  }

  if (pinned_iters_mgr_ && iter) {
    iter->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  InternalIterator* old_iter = second_level_iter_.Set(iter);
  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    pinned_iters_mgr_->PinIterator(old_iter);
  } else {
//    delete old_iter;
    MOD_DELETE_OBJECT(InternalIterator, old_iter);
  }
}

void TwoLevelIterator::InitDataBlock() {
  if (!first_level_iter_.Valid()) {
    SetSecondLevelIterator(nullptr);
  } else {
    Slice handle = first_level_iter_.value();
    if (second_level_iter_.iter() != nullptr &&
        !second_level_iter_.status().IsIncomplete() &&
        handle.compare(data_block_handle_) == 0) {
      // second_level_iter is already constructed with this iterator, so
      // no need to change anything
    } else {
      uint64_t  blocks_cnt = add_blocks_;
      InternalIterator* iter = state_->NewSecondaryIterator(handle, &blocks_cnt);
      add_blocks_ = blocks_cnt;
      data_block_handle_.assign(handle.data(), handle.size());
      SetSecondLevelIterator(iter);
    }
  }
}

}  // namespace

InternalIterator* NewTwoLevelIterator(TwoLevelIteratorState* state,
                                      InternalIterator* first_level_iter,
                                      monitor::TracePoint point,
                                      memory::SimpleAllocator* arena,
                                      bool need_free_iter_and_state) {
  if (arena == nullptr) {
    return MOD_NEW_OBJECT(memory::ModId::kDbIter, TwoLevelIterator,
        state, first_level_iter, point, need_free_iter_and_state);
  } else {
    auto mem = arena->alloc(sizeof(TwoLevelIterator));
    return new (mem)
        TwoLevelIterator(state, first_level_iter,
                         point, need_free_iter_and_state);
  }
}

}  // namespace table
}  // namespace smartengine
