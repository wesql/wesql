//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited.
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
#include <unordered_map>
#include <vector>

#include "compact/compaction_tasks_picker.h"
#include "db/db.h"
#include "db/snapshot_impl.h"  // SnapshotList
#include "db/table_cache.h"
#include "env/env.h"
#include "memory/page_arena.h"
#include "memtable/memtable_list.h"
#include "options/cf_options.h"
#include "storage/storage_manager.h"
#include "util/thread_local.h"
#include "write_batch/write_batch_internal.h"

namespace smartengine
{
namespace monitor
{
class InstrumentedMutex;
} // namespace monitor
namespace schema
{
class TableSchema;
} // namespace schema

namespace db
{
class VersionSet;
class MemTable;
class MemTableListVersion;
class InternalStats;
class ColumnFamilyData;
class ColumnFamilySet;
struct DumpCfd;
class DBImpl;
struct GlobalContext;

// ColumnFamilyHandleImpl is the class that clients use to access different
// column families. It has non-trivial destructor, which gets called when client
// is done using the column family
class ColumnFamilyHandleImpl : public ColumnFamilyHandle {
 public:
  // create while holding the mutex
  ColumnFamilyHandleImpl(ColumnFamilyData* cfd, DBImpl* db,
                         monitor::InstrumentedMutex* mutex);
  // destroy without mutex
  virtual ~ColumnFamilyHandleImpl() override;
  virtual ColumnFamilyData* cfd() const { return cfd_; }
  virtual DBImpl* db() const { return db_; }
  virtual monitor::InstrumentedMutex* mutex() const { return mutex_; }

  virtual uint32_t GetID() const override;
  virtual common::Status GetDescriptor(ColumnFamilyDescriptor* desc) override;
  virtual const util::Comparator* GetComparator() const override;

 private:
  ColumnFamilyData* cfd_;
  DBImpl* db_;
  monitor::InstrumentedMutex* mutex_;
};

// Does not ref-count ColumnFamilyData
// We use this dummy ColumnFamilyHandleImpl because sometimes MemTableInserter
// calls DBImpl methods. When this happens, MemTableInserter need access to
// ColumnFamilyHandle (same as the client would need). In that case, we feed
// MemTableInserter dummy ColumnFamilyHandle and enable it to call DBImpl
// methods
class ColumnFamilyHandleInternal : public ColumnFamilyHandleImpl {
 public:
  ColumnFamilyHandleInternal()
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr) {}

  void SetCFD(ColumnFamilyData* _cfd) { internal_cfd_ = _cfd; }
  virtual ColumnFamilyData* cfd() const override { return internal_cfd_; }

 private:
  ColumnFamilyData* internal_cfd_;
};

// holds references to memtable, all immutable memtables and version
struct SuperVersion {
  // Accessing members of this class is not thread-safe and requires external
  // synchronization (ie db mutex held or on write thread).

  std::atomic<ColumnFamilyData *>cfd_;
  MemTable* mem;
  MemTableListVersion* imm;
  // current storage manager meta
  Snapshot *current_meta_;
  common::MutableCFOptions mutable_cf_options;
  // Version number of the current SuperVersion
  uint64_t version_number;
  // when seq in (imm_largest_seq_, l0_largest_seq_], not add into cache
  // when seq < imm_largest_seq_, no use cache
  uint64_t l0_largest_seq_;
  uint64_t imm_largest_seq_;

  monitor::InstrumentedMutex* db_mutex;

  // should be called outside the mutex
  SuperVersion() = default;
  ~SuperVersion();
  SuperVersion* Ref();
  // If Unref() returns true, Cleanup() should be called with mutex held
  // before deleting this SuperVersion.
  bool Unref();

  // call these two methods with db mutex held
  // Cleanup unrefs mem, imm and current. Also, it stores all memtables
  // that needs to be deleted in to_delete vector. Unrefing those
  // objects needs to be done in the mutex
  void Cleanup();
  void Init(ColumnFamilyData *column_family_data,
            MemTable *new_mem,
            MemTableListVersion *new_imm,
            Snapshot *current_meta = nullptr);
  int64_t get_delete_mem_count() const { return to_delete.size(); }
  // The value of dummy is not actually used. kSVInUse takes its address as a
  // mark in the thread local storage to indicate the SuperVersion is in use
  // by thread. This way, the value of kSVInUse is guaranteed to have no
  // conflict with SuperVersion object address and portable on different
  // platform.
  static int dummy;
  static void* const kSVInUse;
  static void* const kSVObsolete;

 private:
  std::atomic<uint32_t> refs;
  // We need to_delete because during Cleanup(), imm->Unref() returns
  // all memtables that we need to free through this vector. We then
  // delete all those memtables outside of mutex, during destruction
  util::autovector<MemTable*> to_delete;
};

//TODO(Zhao Dongsheng) move to util directory.
extern common::Status CheckCompressionSupported(
    const common::ColumnFamilyOptions& cf_options);

extern common::Status CheckConcurrentWritesSupported(
    const common::ColumnFamilyOptions& cf_options);

extern common::ColumnFamilyOptions SanitizeOptions(
    const common::ImmutableDBOptions& db_options,
    const common::ColumnFamilyOptions& src);

// This class keeps all the data that a column family needs.
// Most methods require DB mutex held, unless otherwise noted
class ColumnFamilyData {
public:
  static const int64_t COLUMN_FAMILY_DATA_VERSION = 1;

  ColumnFamilyData(common::Options &options);
  ~ColumnFamilyData();

  int init(const CreateSubTableArgs &args, GlobalContext *global_ctx, ColumnFamilySet *column_family_set);

  void destroy();
  // thread-safe

  int64_t GetID() const { return sub_table_meta_.table_schema_.get_index_id(); }
  int64_t get_table_space_id() const { return sub_table_meta_.table_space_id_; }
  schema::TableSchema get_table_schema() const;

  // Ref() can only be called from a context where the caller can guarantee
  // that ColumnFamilyData is alive (while holding a non-zero ref already,
  // holding a DB mutex, or as the leader in a write batch group).
  void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

  // Unref decreases the reference count, but does not handle deletion
  // when the count goes to 0.  If this method returns true then the
  // caller should delete the instance immediately, or later, by calling
  // FreeDeadColumnFamilies().  Unref() can only be called while holding
  // a DB mutex, or during single-threaded recovery.
  bool Unref() {
    int old_refs = refs_.fetch_sub(1, std::memory_order_relaxed);
    assert(old_refs > 0);
    return old_refs == 1;
  }

  void set_cancel_task_type(int64_t val, bool cancel = false) {
    if (cancel) {
      cancel_task_type_.fetch_and(val);
    } else {
      cancel_task_type_.fetch_or(val);
    }
  }
  // SetDropped() can only be called under following conditions:
  // 1) Holding a DB mutex,
  // 2) from single-threaded write thread, AND
  // 3) from single-threaded VersionSet::LogAndApply()
  // After dropping column family no other operation on that column family
  // will be executed. All the files and memory will be, however, kept around
  // until client drops the column family handle. That way, client can still
  // access data from dropped column family.
  // Column family can be dropped and still alive. In that state:
  // *) Compaction and flush is not executed on the dropped column family.
  // *) Client can continue reading from column family. Writes will fail unless
  // WriteOptions::ignore_missing_column_families is true
  // When the dropped column family is unreferenced, then we:
  // *) Remove column family from the linked list maintained by ColumnFamilySet
  // *) delete all memory associated with that column family
  // *) delete all the files associated with that column family
  void SetDropped();
  bool IsDropped() const { return dropped_.load(); }
  // stop all the background flush, compaction and recyle
  void set_bg_stopped(bool val) {
    bg_stopped_.store(val);
    set_cancel_task_type(ALL_TASK_VALUE_FOR_CANCEL);
  }
  std::atomic<bool>* bg_stopped() { return &bg_stopped_; }
  bool is_bg_stopped() { return bg_stopped_.load(); }
  bool can_gc()
  {
    return IsDropped() && is_bg_stopped()
           && !pending_flush() && !pending_compaction()
           && !pending_dump() && !pending_shrink()
           && storage_manager_.can_gc();
  }
  bool can_shrink()
  {
    return !IsDropped() && !is_bg_stopped()
           && !pending_flush() && !pending_compaction()
           && !pending_dump() && !pending_shrink()
           && storage_manager_.can_shrink();
  }
  bool can_physical_shrink()
  {
    return !IsDropped() && !is_bg_stopped()
           && !pending_flush() && !pending_compaction()
           && !pending_dump() && pending_shrink()
           && storage_manager_.can_shrink();
  }
  void print_internal_stat();

  // thread-safe
  const util::EnvOptions* soptions() const;
  const common::ImmutableCFOptions* ioptions() const { return &ioptions_; }

  // TODO (Zhao Dongsheng) : this interface is improper.
  // REQUIRES: DB mutex held
  // This returns the latest MutableCFOptions, which may be not in effect yet.
  const common::MutableCFOptions* GetLatestMutableCFOptions() const { return &mutable_cf_options_; }

  // REQUIRES: DB mutex held
  // Build ColumnFamiliesOptions with immutable options and latest mutable
  // options.
  common::ColumnFamilyOptions GetLatestCFOptions() const;

  // REQUIRES: DB mutex held
  common::Status SetOptions(
      const std::unordered_map<std::string, std::string>& options_map);

  InternalStats* internal_stats() { return internal_stats_.get(); }

  int update_active_memtable(const RecoveryPoint &recovery_point, util::autovector<MemTable *> *to_delete);
  MemTableList* imm() { return &imm_; }
  MemTable* mem() { return mem_; }

  // calculate the oldest log needed for the durability of this column family
  uint64_t OldestLogToKeep();
  // calculate the oldest log needed for the durability of this cf's active memtable
  uint64_t OldestLogMemToKeep();

  // See Memtable constructor for explanation of earliest_seq param.
  MemTable* ConstructNewMemtable(common::SequenceNumber earliest_seq);
  // TODO (Zhao Dongsheng) : the interface is improper.
  void CreateNewMemtable(common::SequenceNumber earliest_seq);

  TableCache* table_cache() const { return table_cache_.get(); }

  bool need_compaction_v1(CompactionTasksPicker::TaskInfo &task_info,
                          const CompactionScheduleType type);
  bool need_flush(db::TaskType &task_type);

  // thread-safe
  const util::Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
  // thread-safe
  const InternalKeyComparator& internal_comparator() const {
    return internal_comparator_;
  }

  SuperVersion* GetSuperVersion() { return super_version_; }
  // thread-safe
  // Return a already referenced SuperVersion to be used safely.
  SuperVersion* GetReferencedSuperVersion(monitor::InstrumentedMutex* db_mutex);
  // thread-safe
  // Get SuperVersion stored in thread local storage. If it does not exist,
  // get a reference from a current SuperVersion.
  SuperVersion* GetThreadLocalSuperVersion(
      monitor::InstrumentedMutex* db_mutex);
  // Try to return SuperVersion back to thread local storage. Retrun true on
  // success and false on failure. It fails when the thread local storage
  // contains anything other than SuperVersion::kSVInUse flag.
  bool ReturnThreadLocalSuperVersion(SuperVersion* sv);
  // thread-safe
  uint64_t GetSuperVersionNumber() const {
    return super_version_number_.load();
  }
  // will return a pointer to SuperVersion* if previous SuperVersion
  // if its reference count is zero and needs deletion or nullptr if not
  // As argument takes a pointer to allocated SuperVersion to enable
  // the clients to allocate SuperVersion outside of mutex.
  // IMPORTANT: Only call this from DBImpl::InstallSuperVersion()
  SuperVersion* InstallSuperVersion(SuperVersion* new_superversion, monitor::InstrumentedMutex* db_mutex);
  void ResetThreadLocalSuperVersions();

  // Protected by DB mutex
  void set_pending_flush(bool value) { pending_flush_ = value; }
  void set_pending_compaction(bool value) { pending_compaction_ = value; }
  void set_pending_dump(bool value) { pending_dump_ = value; }
  void set_pending_shrink(bool value) { pending_shrink_ = value; }
  bool pending_flush() { return pending_flush_; }
  bool pending_compaction() { return pending_compaction_; }
  bool pending_dump() { return pending_dump_; }
  bool pending_shrink() { return pending_shrink_; }
  CompactionPriority compaction_priority() { return compaction_priority_; }
  void set_compaction_priority(CompactionPriority pri) {
    compaction_priority_ = pri;
  }

  void set_pending_priority_l0_layer_sequence(int64_t seq) {
    task_picker_.set_pending_priority_l0_layer_sequence(seq);
  }

  void set_imm_largest_seq(common::SequenceNumber seq) {
    imm_largest_seq_ = seq;
  }

  int64_t pending_priority_l0_layer_sequence() {
    return task_picker_.pending_priority_l0_layer_sequence_;
  }
  int64_t current_priority_l0_layer_sequence() {
    return task_picker_.current_priority_l0_layer_sequence_;
  }
  bool delete_triggered_compaction() {
    return task_picker_.delete_triggered_compaction_;
  }
  // todo(yeti) compaction set check
  void set_delete_triggered_compaction(bool value) {
    task_picker_.set_delete_compaction_trigger(value);
  }

  Snapshot *get_backup_meta_snapshot(monitor::InstrumentedMutex *db_mutex = nullptr);
  // manage the storage manager meta versions
  Snapshot *get_meta_snapshot(monitor::InstrumentedMutex *db_mutex = nullptr);
  void release_meta_snapshot(Snapshot *snapshot, monitor::InstrumentedMutex *db_mutex = nullptr);

  //TODO(Zhao Dongsheng), this function is inproper.
  storage::StorageManager* get_storage_manager()
  {
    return &storage_manager_;
  }

  common::SequenceNumber get_imm_largest_seq() const {
    return imm_largest_seq_;
  }

  std::atomic<int64_t>* cancel_task_type() { return &cancel_task_type_; }
  int64_t get_cancel_task_type() { return cancel_task_type_.load(); }
  bool task_canceled(TaskType type) { return (cancel_task_type_.load() & (1LL << type)); }
  //TODO:yuanfeng may_key_exist used?
  int get_from_storage_manager(const common::ReadOptions &read_options,
                               const Snapshot &current_meta,
                               const LookupKey &key,
                               common::PinnableSlice &value,
                               bool &may_key_exist,
                               common::SequenceNumber *seq = nullptr);

  int recover_m0_to_l0();

  int write_objstore_meta_snapshot();

  int apply_change_info(storage::ChangeInfo &change_info,
                        bool write_log,
                        bool is_replay = false,
                        db::RecoveryPoint *recovery_point = nullptr,
                        const util::autovector<MemTable *> *flushed_memtables = nullptr,
                        util::autovector<MemTable *> *to_delete = nullptr);

  int modify_table_schema(const schema::TableSchema &table_schema);

  int recover_extent_space();

  void set_manual_compaction_type(const db::TaskType type) { task_picker_.set_mc_task_type(type); }

  const CompactionTasksPicker &get_task_picker() const { return task_picker_; }

  void set_recovery_point(const RecoveryPoint &recovery_point) { sub_table_meta_.recovery_point_ = recovery_point; }

  const RecoveryPoint get_recovery_point() { return sub_table_meta_.recovery_point_; }

  int release_resource(bool for_recovery);

  int64_t get_current_total_extent_count() const { return storage_manager_.get_current_total_extent_count(); }

  DumpCfd *get_dump_cfd() const { return dcfd_; }

  void set_dump_cfd(DumpCfd *dcfd) { dcfd_ = dcfd; }

  common::SequenceNumber get_range_start() const { return range_start_.load(std::memory_order_relaxed); }

  common::SequenceNumber get_range_end() const { return range_end_.load(std::memory_order_relaxed); }

  int get_extent_positions(storage::ExtentPositionMap &extent_positions)
  {
    return storage_manager_.get_extent_positions(sub_table_meta_.table_schema_.get_index_id(), extent_positions);
  }

  void set_autocheck_info(const int64_t delete_extents_size,
                           const int64_t l1_usage_percent,
                           const int64_t l2_usage_percent) {
    task_picker_.set_autocheck_info(delete_extents_size, l1_usage_percent, l2_usage_percent);
  }

  void set_level_info(const int64_t l0_num_val,
                      const int64_t l1_num_val,
                      const int64_t l2_num_val) {
    task_picker_.set_level_info(l0_num_val, l1_num_val, l2_num_val);
  }

  int set_compaction_check_info(monitor::InstrumentedMutex *mutex);
  int serialize(char *buf, int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, int64_t buf_len, int64_t &pos);
  int64_t get_serialize_size() const;
  //use for ldb tools to dump checkpoint
  int deserialize_and_dump(const char *buf,
                           int64_t buf_len,
                           int64_t &pos,
                           char *str_buf,
                           int64_t string_buf_len,
                           int64_t &str_pos);
  DECLARE_TO_STRING()

#ifndef NDEBUG
  void test_set_index_id(const int64_t index_id) { sub_table_meta_.table_schema_.set_index_id(index_id); }
#endif 

 private:
  friend class VersionSet;
  friend class ColumnFamilySet;
  int release_memtable_resource();
  template <typename type> void delete_object(type *&obj) { MOD_DELETE_OBJECT(type, obj); }

  bool is_inited_;
  bool has_release_mems_;
  uint32_t id_;
  std::string name_;
  std::atomic<int> refs_; // outstanding references to ColumnFamilyData
  std::atomic<bool> dropped_;          // true if client dropped it
  // true if client stopped all the BackGround flush, compaction and recyle
  std::atomic<bool> bg_stopped_;
  const InternalKeyComparator internal_comparator_;

  const common::ColumnFamilyOptions initial_cf_options_;
  const common::ImmutableCFOptions ioptions_;
  // TODO:yuanfeng init env options
  util::EnvOptions env_options_;
  common::MutableCFOptions mutable_cf_options_;

  std::unique_ptr<TableCache, memory::ptr_destruct_delete<TableCache>> table_cache_;

  std::unique_ptr<InternalStats, memory::ptr_destruct_delete<InternalStats>> internal_stats_;

  WriteBufferManager *write_buffer_manager_;

  MemTable *mem_;
  MemTableList imm_;
  SuperVersion *super_version_;

  // An ordinal representing the current SuperVersion. Updated by
  // InstallSuperVersion(), i.e. incremented every time super_version_
  // changes.
  std::atomic<uint64_t> super_version_number_;

  // Thread's local copy of SuperVersion pointer
  // This needs to be destructed before mutex_
  std::unique_ptr<util::ThreadLocalPtr, memory::ptr_destruct_delete<util::ThreadLocalPtr>> local_sv_;

  // pointers for a circular linked list. we use it to support iterations over
  // all column families that are alive (note: dropped column families can also
  // be alive as long as client holds a reference)
  ColumnFamilyData *next_;
  ColumnFamilyData *prev_;

  ColumnFamilySet *column_family_set_;

  // If true --> this ColumnFamily is currently present in DBImpl::flush_queue_
  bool pending_flush_;

  // If true --> this ColumnFamily is currently present in
  // DBImpl::compaction_queue_
  bool pending_compaction_;

  // If true --> this subtable is currently present in DBImpl::dump_qeueue_
  bool pending_dump_;

  // If true --> this subtable is currently present in shrink extent space
  bool pending_shrink_;

  // There are 2 compaction priorities which are high and low.
  // a) low priority is default;
  // b) delete triggered compaction is high priority.
  CompactionPriority compaction_priority_;

  // if the database was opened with 2pc enabled
  bool allow_2pc_;

  // manage the current used meta version number
  SnapshotList meta_snapshots_;
  // one storage manager for one column family
  storage::StorageManager storage_manager_;
  // before it the meta is recycled
  common::SequenceNumber imm_largest_seq_;
  memory::SimpleAllocator *allocator_;
  mutable std::mutex subtable_structure_mutex_;
  storage::SubTableMeta sub_table_meta_;
  int64_t commit_log_seq_;
  common::SequenceNumber sst_largest_seq_;
  CompactionTasksPicker task_picker_;
  DumpCfd *dcfd_;
  std::atomic<int64_t> cancel_task_type_; // needed be reset by setter, use carefully
  std::atomic<common::SequenceNumber> range_start_;
  std::atomic<common::SequenceNumber> range_end_;
};

struct DumpCfd {
  DumpCfd(ColumnFamilyData *cfd):
    cfd_(cfd),
    next_(nullptr),
    prev_(nullptr),
    mem_is_empty_(true),
    in_list_(true) {}
  void reset_list_info() {
    next_ = nullptr;
    prev_ = nullptr;
    in_list_ = false;
  }
  void remove_from_list(DumpCfd *&dump_head_cfd, DumpCfd *&dump_tail_cfd) {
    if (this == dump_head_cfd) {
      dump_head_cfd = next_;
    }
    if (this == dump_tail_cfd) {
      dump_tail_cfd = prev_;
    }
    if (nullptr != next_) {
      next_->prev_ = prev_;
    }
    if (nullptr != prev_) {
      prev_->next_ = next_;
    }
    next_ = nullptr;
    prev_ = nullptr;
    in_list_ = false;
  }
  // insert into list on the position after cur node
  void insert_into_list_after(DumpCfd *&dump_tail_cfd, DumpCfd *&cur) {
    next_ = cur->next_;
    prev_ = cur;
    if (nullptr != cur->next_) {
      cur->next_->prev_ = this;
    }
    cur->next_ = this;
    if (cur == dump_tail_cfd) {
      dump_tail_cfd = this;
    }
    in_list_ = true;
  }
  ColumnFamilyData *cfd_;
  DumpCfd *next_;
  DumpCfd *prev_;
  bool mem_is_empty_;
  bool in_list_;
};
// ColumnFamilySet has interesting thread-safety requirements
// * CreateColumnFamily() or RemoveColumnFamily() -- need to be protected by DB
// mutex AND executed in the write thread.
// CreateColumnFamily() should ONLY be called from VersionSet::LogAndApply() AND
// single-threaded write thread. It is also called during Recovery and in
// DumpManifest().
// RemoveColumnFamily() is only called from SetDropped(). DB mutex needs to be
// held and it needs to be executed from the write thread. SetDropped() also
// guarantees that it will be called only from single-threaded LogAndApply(),
// but this condition is not that important.
// * Iteration -- hold DB mutex, but you can release it in the body of
// iteration. If you release DB mutex in body, reference the column
// family before the mutex and unreference after you unlock, since the column
// family might get dropped when the DB mutex is released
// * GetDefault() -- thread safe
// * GetColumnFamily() -- either inside of DB mutex or from a write thread
// * GetNextColumnFamilyID(), GetMaxColumnFamily(), UpdateMaxColumnFamily(),
// NumberOfColumnFamilies -- inside of DB mutex
class ColumnFamilySet {
 public:
  // ColumnFamilySet supports iteration
  class iterator {
   public:
    explicit iterator(ColumnFamilyData* cfd) : current_(cfd) {}
    iterator& operator++() {
      // dropped column families might still be included in this iteration
      // (we're only removing them when client drops the last reference to the
      // column family).
      // dummy is never dead, so this will never be infinite
      do {
        current_ = current_->next_;
      } while (current_->refs_.load(std::memory_order_relaxed) == 0);
      return *this;
    }
    bool operator!=(const iterator& other) {
      return this->current_ != other.current_;
    }
    ColumnFamilyData* operator*() { return current_; }

   private:
    ColumnFamilyData* current_;
  };

  ColumnFamilySet(GlobalContext *global_ctx);
  ~ColumnFamilySet();

  ColumnFamilyData* GetDefault() const;
  // GetColumnFamily() calls return nullptr if column family is not found
  ColumnFamilyData* GetColumnFamily(uint32_t id) const;
  // this call will return the next available column family ID. it guarantees
  // that there is no column family with id greater than or equal to the
  // returned value in the current running instance or anytime in smartengine
  // instance history.
  uint32_t GetNextColumnFamilyID();
  uint32_t GetMaxColumnFamily();
  void UpdateMaxColumnFamily(uint32_t new_max_column_family);

  int CreateColumnFamily(const CreateSubTableArgs &args, ColumnFamilyData *&cfd);
  int add_sub_table(ColumnFamilyData *sub_table);
  void insert_into_dump_list(ColumnFamilyData *cfd);
  const std::unordered_map<int64_t, ColumnFamilyData*> &get_sub_table_map() { return column_family_data_; }
  iterator begin() { return iterator(dummy_cfd_->next_); }
  iterator end() { return iterator(dummy_cfd_); }

  // REQUIRES: DB mutex held
  // Don't call while iterating over ColumnFamilySet
  void FreeDeadColumnFamilies();

  // for version set write_checkpoint
  util::Directory *get_db_dir() const { return db_dir_; } 

  bool is_subtable_dropped(int64_t index_id);
  void remove_cfd_from_list(ColumnFamilyData *cfd);
  std::vector<ColumnFamilyData *> get_next_dump_cfds(const int64_t file_number,
                                                     const common::SequenceNumber dump_seq);

  //set true during recovery wal
  void set_during_repaly_wal(bool during_repaly_wal)
  {
    during_repaly_wal_ = during_repaly_wal;
  }

  bool get_during_replay_wal()
  {
    return during_repaly_wal_;
  }

 private:
  friend class ColumnFamilyData;
  // helper function that gets called from cfd destructor
  // REQUIRES: DB mutex held
  void RemoveColumnFamily(ColumnFamilyData* cfd);
  void insert_into_cfd_list(ColumnFamilyData *cfd);


  // column_family_data_ need to be protected:
  // * when mutating both conditions have to be satisfied:
  // 1. DB mutex locked
  // 2. thread currently in single-threaded write thread
  // * when reading, at least one condition needs to be satisfied:
  // 1. DB mutex locked
  // 2. accessed from a single-threaded write thread
  std::unordered_map<int64_t, ColumnFamilyData *> column_family_data_;
  // TODO(ljc): should use std::set here
  std::unordered_map<int64_t, int64_t> dropped_column_family_data_;

  uint32_t max_column_family_;
  ColumnFamilyData* dummy_cfd_;
  // We don't hold the refcount here, since default column family always exists
  // We are also not responsible for cleaning up default_cfd_cache_. This is
  // just a cache that makes common case (accessing default column family)
  // faster
  ColumnFamilyData* default_cfd_cache_;
  GlobalContext *global_ctx_;
  util::Directory *db_dir_;
  VersionSet *versions_; 
  DumpCfd* dump_head_cfd_; // for dump_list
  DumpCfd* dump_tail_cfd_; // for dump_list
  bool during_repaly_wal_; // for collect dropped subtables

  memory::ArenaAllocator arena_;
};

typedef ColumnFamilyData SubTable;
typedef std::unordered_map<int64_t, SubTable *> SubTableMap;
// We use ColumnFamilyMemTablesImpl to provide WriteBatch a way to access
// memtables of different column families (specified by ID in the write batch)
class ColumnFamilyMemTablesImpl : public ColumnFamilyMemTables {
 public:
  explicit ColumnFamilyMemTablesImpl(SubTableMap &sub_table_map, ColumnFamilySet* column_family_set)
      : column_family_set_(column_family_set), sub_table_map_(sub_table_map) {}

  // sets current_ to ColumnFamilyData with column_family_id
  // returns false if column family doesn't exist
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  bool Seek(uint32_t column_family_id) override;

  // Returns log number of the selected column family
  // REQUIRES: under a DB mutex OR from a write thread
  uint64_t GetLogNumber() const override;

  // Returns the sequence of the selected column family
  // REQUIRES: under a DB mutex OR from a write thread
  common::SequenceNumber GetSequence() const override;

  // REQUIRES: Seek() called first
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual MemTable* GetMemTable() const override;

  // Returns column family handle for the selected column family
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual ColumnFamilyHandle* GetColumnFamilyHandle() override;

  // Cannot be called while another thread is calling Seek().
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual ColumnFamilyData* current() override { return current_; }

  //judge the subtable droppen during recovery
  virtual bool is_subtable_dropped(int64_t index_id) override;

 private:
  ColumnFamilySet* column_family_set_;
  ColumnFamilyData* current_;
  ColumnFamilyHandleInternal handle_;
  SubTableMap &sub_table_map_;
};

extern uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family);

extern const util::Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family);

}  // namespace db
}  // namespace smartengine
