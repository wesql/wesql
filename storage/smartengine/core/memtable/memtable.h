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
#include <atomic>
#include <string>
#include "db/db.h"
#include "db/dbformat.h"
#include "db/recovery_point.h"
#include "env/env.h"
#include "memtable/memtable_allocator.h"
#include "memtable/memtablerep.h"
#include "options/cf_options.h"
#include "util/concurrent_arena.h"
#include "util/dynamic_bloom.h"

namespace smartengine {

namespace port {
class Mutex;
}

namespace table {
class InternalIterator;
}

namespace storage
{
struct ChangeInfo;
}

namespace db {

class MemTableIterator;

struct MemTableOptions {
  explicit MemTableOptions(const common::ImmutableCFOptions& ioptions,
                           const common::MutableCFOptions& mutable_cf_options);
  size_t write_buffer_size;
  int flush_delete_percent;
  int flush_delete_percent_trigger;
  int flush_delete_record_trigger;
  monitor::Statistics* statistics;
};

// Batched counters to updated when inserting keys in one write batch.
// In post process of the write batch, these can be updated together.
// Only used in concurrent memtable insert case.
struct MemTablePostProcessInfo {
  uint64_t data_size = 0;
  uint64_t num_entries = 0;
  uint64_t num_deletes = 0;
};

// Note:  Many of the methods in this class have comments indicating that
// external synchromization is required as these methods are not thread-safe.
// It is up to higher layers of code to decide how to prevent concurrent
// invokation of these methods.  This is usually done by acquiring either
// the db mutex or the single writer thread.
//
// Some of these methods are documented to only require external
// synchronization if this memtable is immutable.  Calling MarkImmutable() is
// not sufficient to guarantee immutability.  It is up to higher layers of
// code to determine if this MemTable can still be modified by other threads.
// Eg: The Superversion stores a pointer to the current MemTable (that can
// be modified) and a separate list of the MemTables that can no longer be
// written to (aka the 'immutable memtables').
class MemTable {
 public:
  struct KeyComparator : public memtable::MemTableRep::KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    virtual int operator()(const char* prefix_len_key1,
                           const char* prefix_len_key2) const override;
    virtual int operator()(const char* prefix_len_key,
                           const common::Slice& key) const override;
  };

  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  //
  // earliest_seq should be the current common::SequenceNumber in the db such
  // that any
  // key inserted into this memtable will have an equal or larger seq number.
  // (When a db is first created, the earliest sequence number will be 0).
  // If the earliest sequence number is not known, kMaxSequenceNumber may be
  // used, but this may prevent some transactions from succeeding until the
  // first key is inserted into the memtable.
  explicit MemTable(const InternalKeyComparator& comparator,
                    const common::ImmutableCFOptions& ioptions,
                    const common::MutableCFOptions& mutable_cf_options,
                    WriteBufferManager* write_buffer_manager,
                    common::SequenceNumber earliest_seq);

  // Do not delete this MemTable unless Unref() indicates it not in use.
  ~MemTable();

  static void d(void* m) {
//    delete (MemTable *)m;
    auto ptr = (MemTable *)m;
    MOD_DELETE_OBJECT(MemTable, ptr);
  }
  static void async_delete_memtable(MemTable* m) {
    m->env_->Schedule(&d, m, util::Env::LOW);
  }

  // Increase reference count.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void Ref() { ++refs_; }

  // Drop reference count.
  // If the refcount goes to zero return this memtable, otherwise return null.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  MemTable* Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      return this;
    }
    return nullptr;
  }

  // Return an estimate of the number of bytes allocated by active memory table
  size_t ApproximateMemoryAllocated();

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  size_t ApproximateMemoryUsage();

  // This method heuristically determines if the memtable should continue to
  // host more data.
  bool ShouldScheduleFlush() const {
    return flush_state_.load(std::memory_order_relaxed) == FLUSH_REQUESTED;
  }

  // Returns true if a flush should be scheduled and the caller should
  // be the one to schedule it
  bool MarkFlushScheduled() {
    auto before = FLUSH_REQUESTED;
    return flush_state_.compare_exchange_strong(before, FLUSH_SCHEDULED,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed);
  }

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/dbformat.{h,cc} module.
  //
  // By default, it returns an iterator for prefix seek if prefix_extractor
  // is configured in Options.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        Calling ~Iterator of the iterator will destroy all the states but
  //        those allocated in arena.
  table::InternalIterator* NewIterator(const common::ReadOptions& read_options,
                                       util::Arena* arena) const;
  table::InternalIterator* NewDumpIterator(const common::ReadOptions& read_options,
                                           const common::SequenceNumber max_seq,
                                           util::Arena &arena) const;

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  //
  // REQUIRES: if allow_concurrent = false, external synchronization to prevent
  // simultaneous operations on the same MemTable.
  void Add(common::SequenceNumber seq, ValueType type, const common::Slice& key,
           const common::Slice& value, bool allow_concurrent = false,
           MemTablePostProcessInfo* post_process_info = nullptr);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // If any operation was found, its most recent sequence number
  // will be stored in *seq on success (regardless of whether true/false is
  // returned).  Otherwise, *seq will be set to kMaxSequenceNumber.
  // On success, *s may be set to OK, NotFound.  Any other
  // status returned indicates a corruption or other unexpected error.
  bool Get(LookupKey& key,
           std::string* value,
           common::Status* s,
           common::SequenceNumber* seq,
           const common::ReadOptions& read_opts);

  bool Get(LookupKey& key,
           std::string* value,
           common::Status* s,
           const common::ReadOptions& read_opts)
  {
    common::SequenceNumber seq;
    return Get(key, value, s, &seq, read_opts);
  }

  // Called when FlushJob picked this memtable;
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void update_delete_trigger();

  // Update counters and flush status after inserting a whole write batch
  // Used in concurrent memtable inserts.
  void BatchPostProcess(const MemTablePostProcessInfo& update_counters) {
    num_entries_.fetch_add(update_counters.num_entries,
                           std::memory_order_relaxed);
    data_size_.fetch_add(update_counters.data_size, std::memory_order_relaxed);
    if (update_counters.num_deletes != 0) {
      num_deletes_.fetch_add(update_counters.num_deletes,
                             std::memory_order_relaxed);
    }
    UpdateFlushState();
  }

  // Get total number of entries in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  uint64_t num_entries() const {
    return num_entries_.load(std::memory_order_relaxed);
  }

  // Get total number of deletes in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  uint64_t num_deletes() const {
    return num_deletes_.load(std::memory_order_relaxed);
  }

  uint64_t data_size() const {
    return data_size_.load(std::memory_order_relaxed);
  }

  // Returns if there is no entry inserted to the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  bool IsEmpty() const { return first_seqno_ == 0; }

  // Returns the sequence number of the first element that was inserted
  // into the memtable.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  common::SequenceNumber GetFirstSequenceNumber() {
    return first_seqno_.load(std::memory_order_relaxed);
  }

  common::SequenceNumber get_last_sequence_number() {
    return last_seqno_.load(std::memory_order_relaxed);
  }
  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  common::SequenceNumber GetEarliestSequenceNumber() {
    return earliest_seqno_.load(std::memory_order_relaxed);
  }

  // DB's latest sequence ID when the memtable is created. This number
  // may be updated to a more recent one before any key is inserted.
  common::SequenceNumber GetCreationSeq() const { return creation_seq_; }

  void SetCreationSeq(common::SequenceNumber sn) { creation_seq_ = sn; }

  void set_recovery_point(const RecoveryPoint &recovery_point)
  {
    recovery_point_ = recovery_point;
  }
  const RecoveryPoint get_recovery_point()
  {
    return recovery_point_;
  }

  // if this memtable contains data from a committed
  // two phase transaction we must take note of the
  // log which contains that data so we can know
  // when to relese that log
  void RefLogContainingPrepSection(uint64_t log);
  // if this memtable is dumped, set the new log number and keep old value
  void set_log_containing_prepsec(uint64_t log_number, uint64_t &old_value);

  uint64_t GetMinLogContainingPrepSection();

  // Notify the underlying storage that no more items will be added.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  // After MarkImmutable() is called, you should not attempt to
  // write anything to this MemTable().  (Ie. do not call Add() or Update()).
  void MarkImmutable() {
    table_->MarkReadOnly();
    allocator_.DoneAllocating();
  }

  // return true if the current memtable::MemTableRep supports snapshots.
  // inplace update prevents snapshots,
  bool IsSnapshotSupported() const { return table_->IsSnapshotSupported(); }

  struct MemTableStats {
    uint64_t size;
    uint64_t count;
  };

  MemTableStats ApproximateStats(const common::Slice& start_ikey,
                                 const common::Slice& end_ikey);

  const InternalKeyComparator& GetInternalKeyComparator() const {
    return comparator_.comparator;
  }

  const MemTableOptions* GetMemTableOptions() const { return &moptions_; }

  void set_no_flush(bool value) { no_flush_ = value; }

  bool delete_triggered() const {
    return delete_triggered_;
  }

  bool is_flush_in_progress() const {
    return flush_in_progress_;
  }

  bool is_dump_in_progress() const {
    return dump_in_progress_;
  }

  void set_dump_in_progress(const bool flag) {
    dump_in_progress_ = flag;
  }

  void set_temp_min_prep_log(const uint64_t log) {
    temp_min_prep_log_ = log;
  }

  uint64_t get_temp_min_prep_log() const {
    return temp_min_prep_log_;
  }

 private:
  enum FlushStateEnum { FLUSH_NOT_REQUESTED, FLUSH_REQUESTED, FLUSH_SCHEDULED };
  static const int64_t MEMTABLE_ARENA_BLOCK_SIZE = 32 * 1024;

  //TODO(Zhao Dongsheng): friend class should remove
  friend class MemTableIterator;
  friend class MemTableList;

  KeyComparator comparator_;
  const MemTableOptions moptions_;
  int refs_;
  const size_t kArenaBlockSize;
  util::ConcurrentArena arena_;
  memtable::MemTableAllocator allocator_;
  unique_ptr<memtable::MemTableRep> table_;

  // Total data size of all data inserted
  std::atomic<uint64_t> data_size_;
  std::atomic<uint64_t> num_entries_;
  std::atomic<uint64_t> num_deletes_;

  // These are used to manage memtable flushes to storage
  bool flush_in_progress_;  // started the flush
  bool flush_completed_;    // finished the flush
  uint64_t file_number_;    // filled up after flush is complete
  bool dump_in_progress_; // started the dump
  bool dump_completed_;  // finished the dump
  bool delete_triggered_;

  // The sequence number of the kv that was inserted first
  std::atomic<common::SequenceNumber> first_seqno_;

  // The sequence number of the kv that was inserted last
  std::atomic<common::SequenceNumber> last_seqno_;

  // The db sequence number at the time of creation or kMaxSequenceNumber
  // if not set.
  std::atomic<common::SequenceNumber> earliest_seqno_;

  common::SequenceNumber creation_seq_;

  // The log files earlier than this number can be deleted.
  uint64_t mem_next_logfile_number_;

  RecoveryPoint recovery_point_;

  // the earliest log containing a prepared section
  // which has been inserted into this memtable.
  std::atomic<uint64_t> min_prep_log_referenced_;
  uint64_t temp_min_prep_log_; // set under mutex

  std::atomic<FlushStateEnum> flush_state_;

  util::Env* env_;

  // Returns a heuristic flush decision
  bool ShouldFlushNow();

  // Updates flush_state_ using ShouldFlushNow()
  void UpdateFlushState();

  // No copying allowed
  MemTable(const MemTable&);
  MemTable& operator=(const MemTable&);
  // during recovery don't switch the memtable 
  bool no_flush_;
};

extern const char* EncodeKey(std::string* scratch, const common::Slice& target);

} // namespace db
} // namespace smartengine
