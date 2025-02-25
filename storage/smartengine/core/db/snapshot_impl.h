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

#pragma once
#include "db/snapshot.h"
#include "storage/multi_version_extent_meta_layer.h"

namespace smartengine {
using namespace common;
using namespace storage;
namespace db {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
class SnapshotImpl : public Snapshot {
public:
  SnapshotImpl();
  virtual ~SnapshotImpl() override;
  int init(int64_t index_id, int64_t meta_snapshot_id, storage::ExtentLayerVersion **extent_layer_versions, common::SequenceNumber seq_num);
  virtual void destroy(util::autovector<storage::ExtentLayerVersion *> &recyle_extent_layer_versions);

  bool ref(int32_t *old_ref);
  bool unref(int32_t *old_ref);
  bool backup_ref();
  bool backup_unref();

  virtual storage::ExtentLayerVersion* get_extent_layer_version(const int64_t level) const override
  {
    return extent_layer_versions_[level];
  }
  virtual storage::ExtentLayer *get_extent_layer(const storage::LayerPosition &layer_position) const override
  {
    return extent_layer_versions_[layer_position.level_]->get_extent_layer(layer_position.layer_index_);
  }
  virtual int64_t get_total_extent_count() const override;
  virtual common::SequenceNumber GetSequenceNumber() const override {
    return number_;
  }

  inline uint32_t pos() const { return pos_; }

  inline SnapshotImpl* next() { return next_; }
  inline int64_t unix_time() { return unix_time_; }
  inline bool is_write_conflict_boundary() {
    return is_write_conflict_boundary_;
  }

  int extent_layer_versions_serialize(char *buf, int64_t buf_len, int64_t &pos) const;
  int extent_layer_versions_deserialize(char *buf, int64_t buf_len, int64_t &pos);
  int64_t extent_layer_versions_get_serialize_size() const;

  int serialize(char *buf, int64_t buf_len, int64_t &pos) const;
  int deserialize(char *buf, int64_t buf_len, int64_t &pos, const util::Comparator *cmp);
  int64_t get_serialize_size() const;

  DECLARE_TO_STRING()
private:
  static const int64_t META_SNAPSHOT_VERSION = 1;

public:
  int64_t index_id_;      // const after creation
  int64_t meta_snapshot_id_; // const after creation, for object store mode only
  SequenceNumber number_; // const after creation
  ExtentLayerVersion *extent_layer_versions_[storage::MAX_TIER_COUNT];

  std::mutex destroy_mutex_;

  std::mutex ref_mutex_;
  int32_t ref_;        // protected by ref_mutex_
  int32_t backup_ref_; // protected by ref_mutex_

private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list
  SnapshotImpl* prev_;
  SnapshotImpl* next_;

  SnapshotList* list_;  // just for sanity checks

  int64_t unix_time_;

  // Will this snapshot be used by a Transaction to do write-conflict checking?
  bool is_write_conflict_boundary_;

  // position in snaplists
  uint32_t pos_;
};

class SnapshotList {
 public:
  SnapshotList() {
    list_.prev_ = &list_;
    list_.next_ = &list_;
    list_.number_ = 0xFFFFFFFFL;  // placeholder marker, for debugging
    count_ = 0;
  }

  bool empty() const { return list_.next_ == &list_; }
  SnapshotImpl* oldest() const {
    assert(!empty());
    return list_.next_;
  }
  SnapshotImpl* newest() const {
    assert(!empty());
    return list_.prev_;
  }

  inline SnapshotImpl* list() { return &list_; }

  const SnapshotImpl* New(SnapshotImpl* s, common::SequenceNumber seq,
                          uint64_t unix_time, bool is_write_conflict_boundary,
                          uint64_t pos = 0) {
    s->number_ = seq;
    s->unix_time_ = unix_time;
    s->is_write_conflict_boundary_ = is_write_conflict_boundary;
    s->list_ = this;
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    s->pos_ = pos;
    s->ref_ = 1;
    count_++;
    return s;
  }

  // Do not responsible to free the object.
  void Delete(const SnapshotImpl* s) {
    assert(s->list_ == this);
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    count_--;
  }

#if 0
  // retrieve all snapshot numbers. They are sorted in ascending order.
  std::vector<common::SequenceNumber> GetAll(
      common::SequenceNumber* oldest_write_conflict_snapshot = nullptr) {
    std::vector<common::SequenceNumber> ret;

    if (oldest_write_conflict_snapshot != nullptr) {
      *oldest_write_conflict_snapshot = kMaxSequenceNumber;
    }

    if (empty()) {
      return ret;
    }
    SnapshotImpl* s = &list_;
    while (s->next_ != &list_) {
      ret.push_back(s->next_->number_);

      if (oldest_write_conflict_snapshot != nullptr &&
          *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
          s->next_->is_write_conflict_boundary_) {
        // If this is the first write-conflict boundary snapshot in the list,
        // it is the oldest
        *oldest_write_conflict_snapshot = s->next_->number_;
      }

      s = s->next_;
    }
    return ret;
  }
#endif

  // get the sequence number of the most recent snapshot
  common::SequenceNumber GetNewest() {
    if (empty()) {
      return 0;
    }
    return newest()->number_;
  }

#if 0
  int64_t GetOldestSnapshotTime() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->unix_time_;
    }
  }
#endif

  uint64_t count() const { return count_; }

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl list_;
  uint64_t count_;
};
}
}  // namespace smartengine
