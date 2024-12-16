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

#include "db/column_family.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <algorithm>
#include <limits>
#include <string>

#include "compact/compaction_job.h"
#include "compact/reuse_block_merge_iterator.h"
#include "db/db_impl.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "storage/extent_space_manager.h"
#include "storage/storage_log_entry.h"
#include "storage/storage_logger.h"
#include "storage/storage_manager.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "table/get_context.h"
#include "util/autovector.h"
#include "util/compression.h"
#include "util/sync_point.h"

namespace smartengine {
using namespace common;
using namespace util;
using namespace monitor;
using namespace memtable;
using namespace cache;
using namespace table;
using namespace storage;


namespace db {

ColumnFamilyHandleImpl::ColumnFamilyHandleImpl(
    ColumnFamilyData* column_family_data, DBImpl* db_in, InstrumentedMutex* mutex_in)
    : cfd_(column_family_data), db_(db_in), mutex_(mutex_in) {
  if (cfd_ != nullptr) {
    cfd_->Ref();
  }
}

ColumnFamilyHandleImpl::~ColumnFamilyHandleImpl() {
  if (cfd_ != nullptr) {
    mutex_->Lock();
    if (cfd_->Unref()) {
      MOD_DELETE_OBJECT(ColumnFamilyData, cfd_);
    }
    mutex_->Unlock();
  }
}

uint32_t ColumnFamilyHandleImpl::GetID() const { return cfd()->GetID(); }

Status ColumnFamilyHandleImpl::GetDescriptor(ColumnFamilyDescriptor* desc) {
  // accessing mutable cf-options requires db mutex.
  InstrumentedMutexLock l(mutex_);
  *desc = ColumnFamilyDescriptor(cfd()->GetLatestCFOptions());
  return Status::OK();
}

const Comparator* ColumnFamilyHandleImpl::GetComparator() const {
  return cfd()->user_comparator();
}

Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options) {
  if (!cf_options.compression_per_level.empty()) {
    for (size_t level = 0; level < cf_options.compression_per_level.size();
         ++level) {
      if (!CompressionTypeSupported(cf_options.compression_per_level[level])) {
        SE_LOG(WARN, "the compression type is not supported",
            "compression_type", CompressionTypeToString(cf_options.compression_per_level[level]));
        return Status::InvalidArgument();
      }
    }
  }
  return Status::OK();
}

Status CheckConcurrentWritesSupported(const ColumnFamilyOptions& cf_options) {
  if (!cf_options.memtable_factory->IsInsertConcurrentlySupported()) {
    return Status::InvalidArgument(
        "Memtable doesn't concurrent writes (allow_concurrent_memtable_write)");
  }
  return Status::OK();
}

ColumnFamilyOptions SanitizeOptions(const ImmutableDBOptions& db_options,
                                    const ColumnFamilyOptions& src) {
  ColumnFamilyOptions result = src;
  size_t clamp_max = std::conditional<
      sizeof(size_t) == 4, std::integral_constant<size_t, 0xffffffff>,
      std::integral_constant<uint64_t, 64ull << 30>>::type::value;
  ClipToRange(&result.write_buffer_size, ((size_t)64) << 10, clamp_max);

  if (result.flush_delete_percent <= 0 || result.flush_delete_percent >= 100) {
    result.flush_delete_percent = 100;
  }
  if (result.compaction_delete_percent <= 0 || result.compaction_delete_percent >= 100) {
    result.compaction_delete_percent = 100;
  }
  if (result.flush_delete_percent_trigger <= 10) {
    result.flush_delete_percent_trigger = 10;
  } else if (result.flush_delete_percent_trigger >= (1<<30)) {
    result.flush_delete_percent_trigger = (1 << 30);
  }
  if (result.flush_delete_record_trigger <= 1) {
    result.flush_delete_record_trigger = 1;
  } else if (result.flush_delete_record_trigger >= (1<<30)) {
    result.flush_delete_record_trigger = (1 << 30);
  }

  if (result.min_write_buffer_number_to_merge < 1) {
    result.min_write_buffer_number_to_merge = 1;
  }

  if (result.max_write_buffer_number_to_maintain < 0) {
    result.max_write_buffer_number_to_maintain = 0;
  }

  if (result.level0_file_num_compaction_trigger == 0) {
    SE_LOG(WARN, "level0_file_num_compaction_trigger cannot be 0");
    result.level0_file_num_compaction_trigger = 1;
  }

  if (result.level0_layer_num_compaction_trigger <= 0) {
    SE_LOG(WARN, "level0_layer_num_compaction_trigger cannot be 0");
    result.level0_layer_num_compaction_trigger = std::numeric_limits<int>::max();
  }

  return result;
}

int SuperVersion::dummy = 0;
void* const SuperVersion::kSVInUse = &SuperVersion::dummy;
void* const SuperVersion::kSVObsolete = nullptr;

SuperVersion::~SuperVersion() {
  for (auto td : to_delete) {
    MemTable::async_delete_memtable(td);
  }
}

SuperVersion* SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}

bool SuperVersion::Unref() {
  // fetch_sub returns the previous value of ref
  uint32_t previous_refs = refs.fetch_sub(1);
  assert(previous_refs > 0);
  return previous_refs == 1;
}

void SuperVersion::Cleanup() {
  assert(refs.load(std::memory_order_relaxed) == 0);
  imm->Unref(&to_delete);
  MemTable* m = mem->Unref();
  if (m != nullptr) {
    to_delete.push_back(m);
  }
  current_meta_ = nullptr;
}

void SuperVersion::Init(ColumnFamilyData *cfd,
                        MemTable *new_mem,
                        MemTableListVersion *new_imm,
                        Snapshot *current_meta) {
  cfd_ = cfd;
  mem = new_mem;
  imm = new_imm;
  mem->Ref();
  imm->Ref();
  refs.store(1, std::memory_order_relaxed);
  current_meta_ = current_meta;
}

namespace {
void SuperVersionUnrefHandle(void* ptr) {
  // UnrefHandle is called when a thread exists or a ThreadLocalPtr gets
  // destroyed. When former happens, the thread shouldn't see kSVInUse.
  // When latter happens, we are in ~ColumnFamilyData(), no get should happen as
  // well.
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  if (sv->Unref()) {
    sv->db_mutex->Lock();
    sv->Cleanup();
    sv->db_mutex->Unlock();
    delete sv;
  }
}
}  // anonymous namespace

ColumnFamilyData::ColumnFamilyData(Options &options)
    : is_inited_(false),
      has_release_mems_(false),
      refs_(0),
      dropped_(false),
      bg_stopped_(false),
      internal_comparator_(options.comparator),
      initial_cf_options_(SanitizeOptions(ImmutableDBOptions(DBOptions(options)), ColumnFamilyOptions(options))),
      ioptions_(ImmutableDBOptions(DBOptions(options)), initial_cf_options_),
      env_options_(),
      mutable_cf_options_(initial_cf_options_),
      write_buffer_manager_(nullptr),
      mem_(nullptr),
      imm_(ioptions_.min_write_buffer_number_to_merge, ioptions_.max_write_buffer_number_to_maintain),
      super_version_(nullptr),
      super_version_number_(0),
      local_sv_(nullptr),
      next_(nullptr),
      prev_(nullptr),
      column_family_set_(nullptr),
      pending_flush_(false),
      pending_compaction_(false),
      pending_dump_(false),
      pending_shrink_(false),
      compaction_priority_(CompactionPriority::LOW),
      allow_2pc_(options.allow_2pc),
      meta_snapshots_(),
      storage_manager_(this, env_options_, ioptions_, mutable_cf_options_),
      imm_largest_seq_(0),
      allocator_(nullptr),
      subtable_structure_mutex_(),
      sub_table_meta_(),
      commit_log_seq_(0),
      sst_largest_seq_(0),
      task_picker_(mutable_cf_options_, 0, options.level_compaction_dynamic_level_bytes),
      dcfd_(nullptr),
      cancel_task_type_(0),
      range_start_(0),
      range_end_(0)
{
  Ref();
}

int ColumnFamilyData::init(const CreateSubTableArgs &args, GlobalContext *global_ctx, ColumnFamilySet *column_family_set)
{
  int ret = Status::kOk;
  InternalStats *internal_stats = nullptr;
  TableCache *table_cache = nullptr;
  ThreadLocalPtr *local_sv = nullptr;

  if (is_inited_) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "ColumnFamilyData has been inited", K(ret));
  } else if (UNLIKELY(!args.is_valid()) || IS_NULL(global_ctx) || UNLIKELY(!global_ctx->is_valid()) || IS_NULL(column_family_set)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(args), KP(global_ctx), KP(column_family_set));
  } else if (IS_NULL(internal_stats = MOD_NEW_OBJECT(memory::ModId::kSubTable,
                                                     InternalStats,
                                                     GlobalContext::get_env(),
                                                     this))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for internal_stats", K(ret));
  } else if (IS_NULL(table_cache = MOD_NEW_OBJECT(memory::ModId::kSubTable,
                                                  TableCache,
                                                  ioptions_,
                                                  GlobalContext::get_cache()))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for table_cache", K(ret));
  } else if (IS_NULL(local_sv = MOD_NEW_OBJECT(memory::ModId::kSubTable, ThreadLocalPtr, &SuperVersionUnrefHandle))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for local_sv", K(ret));
  } else if (FAILED(storage_manager_.init())) {
    SE_LOG(WARN, "fail to init storage manager", K(ret));
  } else {
    sub_table_meta_.table_schema_ = args.table_schema_;
    sub_table_meta_.table_space_id_ = args.table_space_id_;
    task_picker_.set_cf_id(args.table_schema_.get_index_id());
    internal_stats_.reset(internal_stats);
    table_cache_.reset(table_cache);
    local_sv_.reset(local_sv);
    write_buffer_manager_ = global_ctx->write_buf_mgr_;
    column_family_set_ = column_family_set;
    mutable_cf_options_ = MutableCFOptions(SanitizeOptions(ImmutableDBOptions(DBOptions(global_ctx->options_)), args.cf_options_));
    CreateNewMemtable(1);
    is_inited_ = true;
  }

  return ret;
}

void ColumnFamilyData::destroy()
{
  assert(!pending_dump_);
  assert(!pending_flush_);
  assert(!pending_compaction_);
  if (is_inited_) {
    if (nullptr != internal_stats_.get()) {
      InternalStats *internal_stats = internal_stats_.release();
      MOD_DELETE_OBJECT(InternalStats, internal_stats);
    }
    if (nullptr != table_cache_.get()) {
      TableCache *table_cache = table_cache_.release();
      MOD_DELETE_OBJECT(TableCache, table_cache);
    }
    if (!has_release_mems_) {
     if (nullptr != super_version_) {
        // Release SuperVersion reference kept in ThreadLocalPtr.
        // This must be done outside of mutex_ since unref handler can lock mutex.
        ResetThreadLocalSuperVersions();
        super_version_->db_mutex->Unlock();
        ThreadLocalPtr *local_sv = local_sv_.release();
        MOD_DELETE_OBJECT(ThreadLocalPtr, local_sv);
        super_version_->db_mutex->Lock();
        if (super_version_->Unref()) {
          if (nullptr != super_version_->current_meta_) {
            release_meta_snapshot(super_version_->current_meta_);
          }
          super_version_->Cleanup();
          delete_object(super_version_);
        } else {
          assert(false);
        }
      }
      if (nullptr != mem_) {
        if (mem_->Unref()) {
          delete_object(mem_);
        } else {
          assert(false);
        }
      }
    }
    is_inited_ = false;
  }

  if (!has_release_mems_) {
    autovector<MemTable *> to_delete;
    imm_.current()->Unref(&to_delete);
    for (MemTable *mem : to_delete) {
      delete_object(mem);
    }
  }
}

int ColumnFamilyData::release_resource(bool for_recovery)
{
  int ret = Status::kOk;
  util::autovector<MemTable *> memtable_to_delete;
  db::MemTable *mem = nullptr;
  int64_t total_mem_count = imm_.NumNotFlushed() + imm_.NumFlushed() + 1/*active memtable*/;
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ColumnFamilyData shoule been inited first", K(ret));
  } else {
    //step 1: recycle memtable
    if (FAILED(release_memtable_resource())) {
      SE_LOG(WARN, "fail to release memtable resource", K(ret));
    }
    //step 2: recycle extent
    if (SUCCED(ret)) {
      if (FAILED(storage_manager_.release_extent_resource(for_recovery))) {
        SE_LOG(WARN, "fail to recycle extent in StorageManager", K(ret));
      } else {
        SE_LOG(INFO, "success to release resource of dropped subtable", K_(sub_table_meta));
      }
    }
  }

  return ret;
}

// DB mutex held
ColumnFamilyData::~ColumnFamilyData()
{
  destroy();
}

schema::TableSchema ColumnFamilyData::get_table_schema() const
{
  std::lock_guard<std::mutex> guard(subtable_structure_mutex_);
  return sub_table_meta_.table_schema_;
}

void ColumnFamilyData::SetDropped() {
  // can't drop default CF
  dropped_ = true;
  // remove from column_family_set
  column_family_set_->RemoveColumnFamily(this);
  //TODO:yuanfeng adapt to AllSubTableMap
  column_family_set_->remove_cfd_from_list(this);
}

void ColumnFamilyData::print_internal_stat()
{
  SE_LOG(INFO, "subtable internal stat", K_(sub_table_meta), K_(dropped), K(bg_stopped_.load()), K_(pending_dump), K_(pending_flush), K_(pending_compaction), K_(pending_shrink));
}

ColumnFamilyOptions ColumnFamilyData::GetLatestCFOptions() const {
  return BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
}

uint64_t ColumnFamilyData::OldestLogToKeep()
{
  uint64_t oldest_log_file_number = static_cast<uint64_t>(get_recovery_point().log_file_number_);

  if (allow_2pc_) {
    uint64_t imm_prep_log = imm()->GetMinLogContainingPrepSection();
    uint64_t mem_prep_log = mem()->GetMinLogContainingPrepSection();
    uint64_t temp_prep_log = mem()->get_temp_min_prep_log();

    if (imm_prep_log > 0 && imm_prep_log < oldest_log_file_number) {
      oldest_log_file_number = imm_prep_log;
    }

    if (mem_prep_log > 0 && mem_prep_log < oldest_log_file_number) {
      oldest_log_file_number = mem_prep_log;
    }

    if (temp_prep_log > 0 && temp_prep_log < oldest_log_file_number) {
      oldest_log_file_number = temp_prep_log;
    }
  }

  return oldest_log_file_number;
}

uint64_t ColumnFamilyData::OldestLogMemToKeep()
{
  uint64_t oldest_log_file_number = imm_.get_largest_lognumber();

  if (UINT64_MAX == oldest_log_file_number) {
    oldest_log_file_number = static_cast<uint64_t>(get_recovery_point().log_file_number_);
  }

  if (allow_2pc_) {
    uint64_t min_log_prep = mem()->GetMinLogContainingPrepSection();
    if (min_log_prep > 0 && min_log_prep < oldest_log_file_number) {
      oldest_log_file_number = min_log_prep;
    }

    uint64_t temp_prep_log = mem()->get_temp_min_prep_log();
    if (temp_prep_log > 0 && temp_prep_log < oldest_log_file_number) {
      oldest_log_file_number = temp_prep_log;
    }
  }

  return oldest_log_file_number;
}

const EnvOptions* ColumnFamilyData::soptions() const {
  //TODO:yuanfeng
  return &(column_family_set_->global_ctx_->env_options_);
}

MemTable* ColumnFamilyData::ConstructNewMemtable(SequenceNumber earliest_seq)
{
  // TODO(Zhao Dongsheng) maybe use objectpool
  return MOD_NEW_OBJECT(memory::ModId::kMemtable, MemTable, internal_comparator_, ioptions_,
      mutable_cf_options_, write_buffer_manager_, earliest_seq);
}

void ColumnFamilyData::CreateNewMemtable(SequenceNumber earliest_seq)
{
  if (mem_ != nullptr) {
    auto ptr = mem_->Unref();
    MOD_DELETE_OBJECT(MemTable, ptr);
  }
  mem_ = ConstructNewMemtable(earliest_seq);
  mem_->Ref();
}

bool ColumnFamilyData::need_flush(TaskType &type) {
  bool bret = true;
  const SnapshotImpl* snapshot =
    static_cast<const SnapshotImpl*>(super_version_->current_meta_);
  int64_t l0_num = snapshot->get_extent_layer_version(0)->get_total_normal_extent_count();
  if (0 == l0_num && !pending_compaction() && !mutable_cf_options_.disable_auto_compactions) {
    int64_t size = 0;
    bool delete_trigger = false;
    int64_t cnt = 0;
    imm_.calc_flush_info(delete_trigger, size, cnt);
    int64_t size_limit = 128 * 1024 * 1024;
    if (true == delete_trigger
        || (size > size_limit && cnt < 5 && task_picker_.is_normal_tree(snapshot))) {
      type = TaskType::FLUSH_LEVEL1_TASK;
      SE_LOG(INFO, "PICK_TASK: pick flush task info", K(size), K(size_limit), K(delete_trigger), K(GetID()));
    } else {
      type = TaskType::FLUSH_TASK;
    }
  } else {
    type = TaskType::FLUSH_TASK;
  }
  SE_LOG(INFO, "PICK_TASK: pick flush task info", K(int64_t(type)), K(l0_num), K(GetID()));
  return bret;
}

bool ColumnFamilyData::need_compaction_v1(CompactionTasksPicker::TaskInfo &task_info,
                                          const CompactionScheduleType type) {
  int ret = Status::kOk;
  task_info.task_type_ = TaskType::MAX_TYPE_TASK;
  CompactionPriority compaction_priority = LOW;
  const SnapshotImpl* snapshot = static_cast<const SnapshotImpl*>(super_version_->current_meta_);
  if (CompactionScheduleType::NORMAL == type) {
    if (FAILED(task_picker_.pick_one_task(snapshot, storage_manager_, task_info, compaction_priority))) {
      SE_LOG(WARN, "failed to pick one compaction task", K(ret), K(task_info));
    }
  } else if (CompactionScheduleType::MASTER_AUTO == type) {
    if (FAILED(task_picker_.pick_auto_task(task_info))) {
      SE_LOG(WARN, "failed to pick one auto compaction task", K(ret), K(task_info));
    }
  } else if (CompactionScheduleType::MASTER_IDLE == type) {
    if (FAILED(task_picker_.pick_one_task_idle(task_info))) {
      SE_LOG(WARN, "failed to pick one auto compaction task", K(ret), K(task_info));
    }
  }
  compaction_priority_ = compaction_priority;
  return TaskType::MAX_TYPE_TASK != task_info.task_type_;
}

SuperVersion* ColumnFamilyData::GetReferencedSuperVersion(
    InstrumentedMutex* db_mutex) {
  SuperVersion* sv = nullptr;
  sv = GetThreadLocalSuperVersion(db_mutex);
  sv->Ref();
  if (!ReturnThreadLocalSuperVersion(sv)) {
    sv->Unref();
  }
  return sv;
}

SuperVersion* ColumnFamilyData::GetThreadLocalSuperVersion(
    InstrumentedMutex* db_mutex) {
  SuperVersion* sv = nullptr;
  // The SuperVersion is cached in thread local storage to avoid acquiring
  // mutex when SuperVersion does not change since the last use. When a new
  // SuperVersion is installed, the compaction or flush thread cleans up
  // cached SuperVersion in all existing thread local storage. To avoid
  // acquiring mutex for this operation, we use atomic Swap() on the thread
  // local pointer to guarantee exclusive access. If the thread local pointer
  // is being used while a new SuperVersion is installed, the cached
  // SuperVersion can become stale. In that case, the background thread would
  // have swapped in kSVObsolete. We re-check the value at when returning
  // SuperVersion back to thread local, with an atomic compare and swap.
  // The superversion will need to be released if detected to be stale.
  void* ptr = local_sv_->Swap(SuperVersion::kSVInUse);
  // Invariant:
  // (1) Scrape (always) installs kSVObsolete in ThreadLocal storage
  // (2) the Swap above (always) installs kSVInUse, ThreadLocal storage
  // should only keep kSVInUse before ReturnThreadLocalSuperVersion call
  // (if no Scrape happens).
  assert(ptr != SuperVersion::kSVInUse);
  sv = static_cast<SuperVersion*>(ptr);
  if (sv == SuperVersion::kSVObsolete ||
      sv->version_number != super_version_number_.load()) {
    QUERY_COUNT(CountPoint::NUMBER_SUPERVERSION_ACQUIRES);
    QUERY_TRACE_SCOPE(smartengine::monitor::TracePoint::GET_REF_SV);
    SuperVersion* sv_to_delete = nullptr;

    if (sv && sv->Unref()) {
      QUERY_COUNT(CountPoint::NUMBER_SUPERVERSION_CLEANUPS);
      db_mutex->Lock();
      // NOTE: underlying resources held by superversion (sst files) might
      // not be released until the next background job.
      if (sv->current_meta_) {
        release_meta_snapshot(sv->current_meta_,
                              sv->db_mutex);
      }
      sv->Cleanup();
      sv_to_delete = sv;
    } else {
      db_mutex->Lock();
    }
    sv = super_version_->Ref();
    db_mutex->Unlock();

//    delete sv_to_delete;
    MOD_DELETE_OBJECT(SuperVersion, sv_to_delete);
  }
  assert(sv != nullptr);
  return sv;
}

bool ColumnFamilyData::ReturnThreadLocalSuperVersion(SuperVersion* sv) {
  assert(sv != nullptr);
  // Put the SuperVersion back
  void* expected = SuperVersion::kSVInUse;
  if (local_sv_->CompareAndSwap(static_cast<void*>(sv), expected)) {
    // When we see kSVInUse in the ThreadLocal, we are sure ThreadLocal
    // storage has not been altered and no Scrape has happened. The
    // SuperVersion is still current.
    return true;
  } else {
    // ThreadLocal scrape happened in the process of this GetImpl call (after
    // thread local Swap() at the beginning and before CompareAndSwap()).
    // This means the SuperVersion it holds is obsolete.
    assert(expected == SuperVersion::kSVObsolete);
  }
  return false;
}

Snapshot *ColumnFamilyData::get_backup_meta_snapshot(InstrumentedMutex *db_mutex) {
  // the caller be sure mutex is locked
  if (db_mutex != nullptr) {
    db_mutex->AssertHeld();
  }
  return storage_manager_.accquire_backup_meta_snapshot();
}

Snapshot *ColumnFamilyData::get_meta_snapshot(InstrumentedMutex *db_mutex) {
  // the caller be sure mutex is locked
  if (db_mutex != nullptr) {
    db_mutex->AssertHeld();
  }
  return storage_manager_.accquire_meta_snapshot();
}

// need db mutex locked outside
// TODO(Zhao Dongsheng): Does not need to be protected by 'db_mutex'?
void ColumnFamilyData::release_meta_snapshot(Snapshot *s, monitor::InstrumentedMutex *db_mutex)
{
  if (db_mutex != nullptr) {
    db_mutex->AssertHeld();
    db_mutex->Unlock();
  }
  TEST_SYNC_POINT("ColumnFamilyData::release_meta_snapshot:Unlock");
  storage_manager_.release_meta_snapshot(reinterpret_cast<SnapshotImpl *>(s));
  if (db_mutex != nullptr) {
    db_mutex->Lock();
  }
}

int ColumnFamilyData::get_from_storage_manager(const common::ReadOptions &read_options,
                                               const Snapshot &current_meta,
                                               const LookupKey &key,
                                               common::PinnableSlice &value,
                                               bool &may_key_exist,
                                               SequenceNumber *seq)
{
  int ret = Status::kOk;
  may_key_exist = false;
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ColumnFamilyData should been inited first", K(ret));
  } else if (current_meta.GetSequenceNumber() <= 1) {
    // no disk data
    ret = Status::kNotFound;
    SE_LOG(DEBUG, "no data on disk", K(ret),
                K(current_meta.GetSequenceNumber()));
  } else {
    Slice ikey = key.internal_key();
    Slice user_key = key.user_key();
    GetContext get_context(internal_comparator_.user_comparator(),
                           GetContext::kNotFound,
                           user_key,
                           &value,
                           &may_key_exist,
                           seq);
    Arena arena;
    std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)>
      save_value = [&](const ExtentMeta *extent_meta, int32_t level, bool &found) {
        found = false;
        int func_ret = table_cache_->get(read_options,
                                         internal_comparator_,
                                         level,
                                         extent_meta->extent_id_,
                                         ikey,
                                         &get_context);
        SE_LOG(DEBUG, "get from extent", "index_id", sub_table_meta_.table_schema_.get_index_id(), K(extent_meta));
        if (Status::kOk != func_ret) {
          SE_LOG(WARN, "fail to get from table cache", K(func_ret), K(*extent_meta));
        } else {
          switch (get_context.State()) {
            case GetContext::kNotFound:
              // keep searching in other files
              break;
            case GetContext::kFound:
              found = true;
              RecordTick(ioptions_.statistics, GET_HIT_L0);
              break;
            case GetContext::kDeleted:
              found = true;
              func_ret = Status::kNotFound;
              break;
            case GetContext::kCorrupt:
            default:
              found = true;
              func_ret = Status::kCorruption;
              break;
          }
        }
        return func_ret;
      };
    ret = storage_manager_.get(ikey, current_meta, save_value);
  }

  return ret;
}

int ColumnFamilyData::recover_m0_to_l0() {
  int ret = Status::kOk;
  const ExtentLayer *dump_layer = nullptr;
  Snapshot *current_snapshot = nullptr;
  const ExtentMeta *lob_extent_meta = nullptr;
  LayerPosition layer_position(0, storage::LayerPosition::INVISIBLE_LAYER_INDEX);
  int64_t dummy_log_seq = 0;
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "SubTable shoulde been inited first", K(ret));
  } else if (IS_NULL(current_snapshot = get_meta_snapshot())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, current snapshot must not nullptr", K(ret));
  } else if ((nullptr == (dump_layer = current_snapshot->get_extent_layer(layer_position)))
             || dump_layer->extent_meta_arr_.size() <= 0) {
    // has no dump extent, do nothing
    SE_LOG(INFO, "dump layer is null or empty", KP(dump_layer), K(GetID()));
  } else { // build change_info
    memory::ArenaAllocator arena;
    ChangeInfo change_info;
    change_info.task_type_ = TaskType::SWITCH_M02L0_TASK;
    table::InternalIterator *meta_iter = nullptr;
    MetaDataSingleIterator *range_iter = nullptr;
    if (FAILED(StorageLogger::get_instance().begin(storage::SeEvent::FLUSH))) {
      SE_LOG(WARN, "failed to begin flush event", K(ret));
    } else if (FAILED(CompactionJob::create_meta_iterator(arena, &internal_comparator_, current_snapshot, layer_position, meta_iter))) {
      SE_LOG(WARN, "failed to create meta iterator");
    } else {
      LayerPosition new_layer_position(0, storage::LayerPosition::NEW_GENERATE_LAYER_INDEX);
      MetaType type(MetaType::SSTable, MetaType::Extent, MetaType::InternalKey, 0, 0, 0);
      range_iter = ALLOC_OBJECT(MetaDataSingleIterator, arena, type, meta_iter);
      if (IS_NULL(range_iter)) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "range iter is nullptr", K(ret), "index_id", sub_table_meta_.table_schema_.get_index_id());
      } else {
        range_iter->seek_to_first();
        while (range_iter->valid() && SUCCED(ret)) {
          MetaDescriptor extent_meta = range_iter->get_meta_descriptor().deep_copy(arena);
          SE_LOG(INFO, "delete extent, recovry m0 to l0", K(GetID()), K(extent_meta));
          if (FAILED(change_info.delete_extent(extent_meta.layer_position_, extent_meta.extent_id_))) {
            SE_LOG(WARN, "failed to delete extent", K(ret), K(extent_meta));
          } else if (FAILED(change_info.add_extent(new_layer_position, extent_meta.extent_id_))) {
            SE_LOG(WARN, "failed to add extent", K(ret), K(extent_meta));
          } else {
            range_iter->next();
          }
        }
      }
    }

    if (SUCCED(ret)) {
      if (FAILED(apply_change_info(change_info, true, true))) {
        SE_LOG(WARN, "failed to apply change info for dump", K(ret));
      } else if (FAILED(StorageLogger::get_instance().commit(dummy_log_seq))) {
        SE_LOG(WARN, "fail to commit trans", K(ret));
      } else {
        SE_LOG(INFO, "success to recover dump extent layer to level0", "index_id", sub_table_meta_.table_schema_.get_index_id());
      }
    } else {
      SE_LOG(WARN, "fail to recover dump extent layer to level0", K(ret), "index_id", sub_table_meta_.table_schema_.get_index_id());
    }
    PLACEMENT_DELETE(MetaDataSingleIterator, arena_, range_iter);
  }
  if (nullptr != current_snapshot) {
    release_meta_snapshot(current_snapshot);
  }
  return ret;
}

int ColumnFamilyData::apply_change_info(storage::ChangeInfo &change_info,
                                      bool write_log,
                                      bool is_replay,
                                      db::RecoveryPoint *recovery_point,
                                      const util::autovector<db::MemTable *> *flushed_memtables,
                                      util::autovector<db::MemTable *> *to_delete)
{
  int ret = Status::kOk;
  storage::ModifySubTableLogEntry log_entry(sub_table_meta_.table_schema_.get_index_id(), change_info);
  int64_t commit_log_seq = 0;
  if (nullptr != recovery_point) {
    log_entry.recovery_point_ = *recovery_point;
  }

  std::lock_guard<std::mutex> guard(subtable_structure_mutex_);
  if (write_log) {
    if (IsDropped()) {
      SE_LOG(INFO, "the subtable has been dropped, no need write log any more", K_(sub_table_meta));
      //only commit the trans, no actual modify
    } else {
      if (FAILED(StorageLogger::get_instance().write_log(REDO_LOG_MODIFY_SSTABLE, log_entry))) {
        SE_LOG(WARN, "fail to write modify subtable log", K(ret));
      }
    }
  }

  if (SUCCED(ret)) {
    if (nullptr != recovery_point && (*recovery_point) > sub_table_meta_.recovery_point_) {
      set_recovery_point(*recovery_point);
      if (is_replay) {
        mem_->set_recovery_point(*recovery_point);
      }
      SE_LOG(INFO, "set recovery point", "index_id", sub_table_meta_.table_schema_.get_index_id(), K(*recovery_point));
    }
  }

  if (SUCCED(ret)) {
    if (FAILED(storage_manager_.apply(change_info, is_replay))) {
      SE_LOG(WARN, "fail to apply change info to storage manager", K(ret));
    } else if (nullptr != flushed_memtables) {
      // update sst_largest_seq_ (for row_cache)
      SE_LOG(INFO, "ROW_CACHE:update sst_largest_seq for row cache start", K(sst_largest_seq_), K(GetID()));
      bool delete_trigger = false;
      for (size_t i = 0; i < flushed_memtables->size(); ++i) {
        MemTable *memtable = flushed_memtables->at(i);
        if (IS_NULL(memtable)) {
          ret = Status::kErrorUnexpected;
          SE_LOG(ERROR, "ROW_CACHE:memtable must not nullptr", K(ret));
          break;
        } else if (memtable->delete_triggered() && !delete_trigger) {
          delete_trigger = true;
        }
        if (sst_largest_seq_ < memtable->get_last_sequence_number()) {
          sst_largest_seq_ = memtable->get_last_sequence_number();
        }
        if (delete_trigger) {
          FLUSH_LOG(INFO, "Level-0 delete triggered with ", K(sst_largest_seq_));
          set_delete_triggered_compaction(true);
          set_pending_priority_l0_layer_sequence(sst_largest_seq_);
        }
      }
      SE_LOG(INFO, "update sst_largest_seq for row cache end", K(sst_largest_seq_), K(GetID()));
      if (SUCCED(ret) && FAILED(imm_.purge_flushed_memtable(*flushed_memtables, to_delete))) {
        SE_LOG(WARN, "fail to purge flushed memtable", K(ret));
      }
    }
    SE_LOG(INFO, "success to apply change info", "index_id", sub_table_meta_.table_schema_.get_index_id());
  }

  return ret;
}

int ColumnFamilyData::modify_table_schema(const schema::TableSchema &table_schema)
{
  std::lock_guard<std::mutex> guard(subtable_structure_mutex_);
  sub_table_meta_.table_schema_ = table_schema;

  return Status::kOk;
}

int ColumnFamilyData::recover_extent_space()
{
  int ret = Status::kOk;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ColumnFamilyData should been inited first", K(ret));
  } else if (FAILED(storage_manager_.recover_extent_space())) {
    SE_LOG(WARN, "fail to recover extent space", K(ret), K(sub_table_meta_));
  }

  return ret;
}

int ColumnFamilyData::set_compaction_check_info(monitor::InstrumentedMutex *mutex) {
  int ret = 0;
  mutex->AssertHeld();
  int64_t delete_extents_size = 0;
  int64_t l1_usage_percent = 100;
  int64_t l2_usage_percent = 100;
  Snapshot *snapshot = get_meta_snapshot(mutex);
  StorageManager *storage_manager = get_storage_manager();
  if (IS_NULL(storage_manager) || IS_NULL(snapshot)) {
    ret = Status::kErrorUnexpected;
    COMPACTION_LOG(WARN, "storage manager is null", K(GetID()));
  } else if (FAILED(storage_manager->get_level_usage_percent(snapshot, 1, l1_usage_percent, delete_extents_size/*no use*/))) {
    COMPACTION_LOG(WARN, "failed to get level1 usage_percent", K(ret), K(l1_usage_percent), K(delete_extents_size));
  } else if (FAILED(storage_manager->get_level_usage_percent(snapshot, 2, l2_usage_percent, delete_extents_size))) {
    COMPACTION_LOG(WARN, "failed to get level1 usage_percent", K(ret), K(l2_usage_percent), K(delete_extents_size));
  } else {
    set_autocheck_info(delete_extents_size, l1_usage_percent, l2_usage_percent);
    ExtentLayerVersion *l0_version = snapshot->get_extent_layer_version(0);
    ExtentLayerVersion *l1_version = snapshot->get_extent_layer_version(1);
    ExtentLayerVersion *l2_version = snapshot->get_extent_layer_version(2);
    int64_t level0_num_val = nullptr != l0_version ? l0_version->get_total_normal_extent_count() : 0;
    int64_t level1_num_val = nullptr != l1_version ? l1_version->get_total_normal_extent_count() : 0;
    int64_t level2_num_val = nullptr != l2_version ? l2_version->get_total_normal_extent_count() : 0;
    set_level_info(level0_num_val, level1_num_val, level2_num_val);
  }
  if (nullptr != snapshot) {
    release_meta_snapshot(snapshot, mutex);
  }
  return ret;
}

int ColumnFamilyData::serialize(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(subtable_structure_mutex_);
  int64_t size = get_serialize_size();
  int64_t version = COLUMN_FAMILY_DATA_VERSION;
  if (IS_NULL(buf) || buf_len < 0 || pos >= buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    *((int64_t *)(buf +pos)) = size;
    pos += sizeof(size);
    *((int64_t *)(buf + pos)) = version;
    pos += sizeof(version);
    if (FAILED(sub_table_meta_.serialize(buf, buf_len, pos))) {
      SE_LOG(WARN, "fail to serialize sub_table_meta", K(ret));
    } else if (FAILED(storage_manager_.serialize(buf, buf_len, pos))) {
      SE_LOG(WARN, "fail to serialize storage manager", K(ret));
    }
  }

  return ret;
}

int ColumnFamilyData::deserialize(const char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t version = 0;

  if (IS_NULL(buf) || buf_len < 0 || pos >= buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    size = *((int64_t *)(buf + pos));
    pos += sizeof(size);
    version = *((int64_t *)(buf + pos));
    pos += sizeof(version);
    if (FAILED(sub_table_meta_.deserialize(buf, buf_len, pos))) {
      SE_LOG(WARN, "fail to deserialize subtable meta", K(ret));
    } else if (FAILED(storage_manager_.deserialize(buf, buf_len, pos))) {
      SE_LOG(WARN, "fail to deserialize storage_manager", K(ret));
    }
  }

  return ret;
}

int ColumnFamilyData::deserialize_and_dump(const char *buf,
                                           int64_t buf_len,
                                           int64_t &pos,
                                           char *str_buf,
                                           int64_t str_buf_len,
                                           int64_t &str_pos)
{
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t version = 0;

  if (IS_NULL(buf) || buf_len < 0 || pos >= buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    size = *((int64_t *)(buf + pos));
    pos += sizeof(size);
    version = *((int64_t *)(buf + pos));
    pos += sizeof(version);
    if (FAILED(sub_table_meta_.deserialize(buf, buf_len, pos))) {
      SE_LOG(WARN, "fail to deserialize subtbale meta", K(ret));
    } else {
      str_pos = sub_table_meta_.to_string(str_buf + str_pos, str_buf_len - str_pos);
      if (FAILED(storage_manager_.deserialize_and_dump(buf, buf_len, pos, str_buf, str_buf_len , str_pos))) {
        SE_LOG(WARN, "fail to deserialize StorageManager for dump", K(ret));
      }
    }
  }

  return ret;
}

int64_t ColumnFamilyData::get_serialize_size() const
{
  int64_t size = 0;
  size += 2 * sizeof(int64_t); //size and version
  size += sub_table_meta_.get_serialize_size();
  size +=storage_manager_.get_serialize_size();
  return size;
}

DEFINE_TO_STRING(ColumnFamilyData, KV_(sub_table_meta), KV_(storage_manager))

SuperVersion* ColumnFamilyData::InstallSuperVersion(SuperVersion* new_superversion, InstrumentedMutex* db_mutex)
{
  se_assert(IS_NOTNULL(db_mutex));
  db_mutex->AssertHeld();
  new_superversion->db_mutex = db_mutex;
  new_superversion->mutable_cf_options = mutable_cf_options_;
  new_superversion->Init(this,
                         mem_,
                         imm_.current(),
                         get_meta_snapshot(db_mutex));
  SuperVersion* old_superversion = super_version_;
  super_version_ = new_superversion;
  ++super_version_number_;
  super_version_->imm_largest_seq_ = imm_largest_seq_;
  super_version_->version_number = super_version_number_;
  if (nullptr != ioptions_.row_cache) {
    // update row cache version, update range
    // [start, end] => [sst_latgest_seq, imm_largest_seq]
    // row cache rules:
    // snapshot <= start        => cann't read && cann't insert
    // snapshot in [start, end] => can read && can't insert
    // snapshot > end           => can read && can insert
    if (sst_largest_seq_ || imm_largest_seq_) {
      //ShardedCache *cache = static_cast<ShardedCache *>(ioptions_.row_cache.get());
//      ioptions_.row_cache->set_sequence(sst_largest_seq_, imm_largest_seq_, this->GetID());
      range_start_.store(sst_largest_seq_, std::memory_order_relaxed); // todo nouse
      if (imm_largest_seq_ > range_end_) {
        range_end_.store(imm_largest_seq_, std::memory_order_relaxed);
      }
      if (0 != range_start_ && range_start_ == range_end_) {
        range_end_ -= 1;
      }
      SE_LOG(INFO, "update row cache range", K(range_start_), K(range_end_), K(GetID()));
    }
  }
  // todo: yeti
//  storage_manager_.set_scan_add_blocks_limit(mutable_cf_options.scan_add_blocks_limit);
  // Reset SuperVersions cached in thread local storage
  ResetThreadLocalSuperVersions();

  if (old_superversion != nullptr && old_superversion->Unref()) {
    if (old_superversion->current_meta_) {
      release_meta_snapshot(old_superversion->current_meta_,
                            old_superversion->db_mutex);
    }
    old_superversion->Cleanup();
    return old_superversion;  // will let caller delete outside of mutex
  }
  return nullptr;
}

void ColumnFamilyData::ResetThreadLocalSuperVersions() {
  std::vector<void*> sv_ptrs;
  local_sv_->Scrape(&sv_ptrs, SuperVersion::kSVObsolete);
  for (auto ptr : sv_ptrs) {
    assert(ptr);
    if (ptr == SuperVersion::kSVInUse) {
      continue;
    }
    auto sv = static_cast<SuperVersion*>(ptr);
    if (sv->Unref()) {
      if (sv->current_meta_) {
        release_meta_snapshot(sv->current_meta_,
                              sv->db_mutex);
      }
      sv->Cleanup();
      delete sv;
    }
  }
}

Status ColumnFamilyData::SetOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
  MutableCFOptions new_mutable_cf_options;
  Status s = GetMutableOptionsFromStrings(mutable_cf_options_, options_map,
                                          &new_mutable_cf_options);
  if (s.ok()) {
    mutable_cf_options_ = new_mutable_cf_options;
  }
  return s;
}

// TODO(Zhao Dongsheng): Currently, the 'switch_memtable' is a global barrier, so there is no lock protection here.
// In the future, when reviewing the use of db_mutex, it will be necessary to determine whether subtable-level
// locks are needed for protection.
int ColumnFamilyData::update_active_memtable(const RecoveryPoint &recovery_point, autovector<MemTable *> *to_delete)
{
  int ret = Status::kOk;
  MemTable *new_memtable = nullptr;

  if (UNLIKELY(!recovery_point.is_valid()) || IS_NULL(to_delete)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalud argument", K(ret), K(recovery_point), KP(to_delete));
  } else if (IS_NULL(new_memtable = ConstructNewMemtable(recovery_point.seq_))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for new memtable", K(ret));
  } else {
    se_assert(IS_NOTNULL(mem_));
    mem_->set_recovery_point(recovery_point);
    imm_.Add(mem_, to_delete);
    imm_largest_seq_ = mem_->get_last_sequence_number();
    new_memtable->Ref();
    mem_ = new_memtable;
  }

  return ret;
}

int ColumnFamilyData::release_memtable_resource()
{
  int ret = Status::kOk;
  util::autovector<MemTable *> memtable_to_delete;
  db::MemTable *mem = nullptr;
  int64_t imm_mem_count = imm_.NumNotFlushed() + imm_.NumFlushed();
  int64_t total_mem_count = imm_mem_count + 1/*active memtable*/;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ColumnFamilyData shoule been inited first", K(ret));
  } else if (IS_NULL(mem_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, mem_ must not nullptr", K(ret), KP(mem_));
  } else {
    /*---recycle all memtable---*/
    //step 1: recycle all memtable, first release the refs on all memtables, then recycle all memtable

    //release the active memtable refs, which ref in switch memtable or CreateNewMemtable
    if (mem_->Unref()) {
      delete_object(mem_);
    }

    //release the MemtableListVersion refs, which ref in MemtableList
    imm_.current()->Unref(&memtable_to_delete);
    for (MemTable *imm : memtable_to_delete) {
      delete_object(imm);
    }
    memtable_to_delete.clear();

    if (nullptr != super_version_) {
      //release the meta_snapshit refs, which ref when Init sv acquire by get_meta_snapshot()
      release_meta_snapshot(super_version_->current_meta_);
      //reset thread local sv
      ResetThreadLocalSuperVersions();
      if (!super_version_->Unref()) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, superversion should not ref by other", K(ret)/*, K(*super_version_)*/);
      } else {
        super_version_->Cleanup();
        if (total_mem_count != super_version_->get_delete_mem_count()) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, delete memtable count not expected", K(ret),
              K(total_mem_count), "delete_mem_count", super_version_->get_delete_mem_count());
        } else {
          //recycle memtables and sv
          delete_object(super_version_);
          mem_ = nullptr;
        }
      }
    }

    if (SUCCED(ret)) {
      has_release_mems_ = true;
    }
  }

  return ret;
}

ColumnFamilySet::ColumnFamilySet(GlobalContext *global_ctx)
    : column_family_data_(),
      dropped_column_family_data_(),
      max_column_family_(0),
      dummy_cfd_(new ColumnFamilyData(global_ctx->options_)),
      default_cfd_cache_(nullptr),
      global_ctx_(global_ctx),
      db_dir_(nullptr),
      versions_(nullptr),
      dump_head_cfd_(nullptr),
      dump_tail_cfd_(nullptr),
      during_repaly_wal_(false),
      arena_(memory::CharArena::DEFAULT_PAGE_SIZE, memory::ModId::kColumnFamilySet)
{
  // initialize linked list
  dummy_cfd_->prev_ = dummy_cfd_;
  dummy_cfd_->next_ = dummy_cfd_;
}

ColumnFamilySet::~ColumnFamilySet() {
  //any way, unref and deconstruct subtable at deconstruct phrase.
  for (auto iter : column_family_data_ ) {
    SubTable *subtable = iter.second;
    if (subtable->Unref()) {
      MOD_DELETE_OBJECT(SubTable, subtable);
    }
  }

  dummy_cfd_->Unref();
  delete dummy_cfd_;
}

ColumnFamilyData* ColumnFamilySet::GetDefault() const {
  assert(default_cfd_cache_ != nullptr);
  return default_cfd_cache_;
}

ColumnFamilyData* ColumnFamilySet::GetColumnFamily(uint32_t id) const {
  auto cfd_iter = column_family_data_.find(id);
  if (cfd_iter != column_family_data_.end()) {
    return cfd_iter->second;
  } else {
    return nullptr;
  }
}

uint32_t ColumnFamilySet::GetNextColumnFamilyID() {
  return ++max_column_family_;
}

uint32_t ColumnFamilySet::GetMaxColumnFamily() { return max_column_family_; }

void ColumnFamilySet::UpdateMaxColumnFamily(uint32_t new_max_column_family) {
  max_column_family_ = std::max(new_max_column_family, max_column_family_);
}

int ColumnFamilySet::CreateColumnFamily(const CreateSubTableArgs &args, ColumnFamilyData *&cfd)
{
  int ret = Status::kOk;
  ColumnFamilyData *tmp_cfd = nullptr;

  if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (IS_NULL(tmp_cfd = MOD_NEW_OBJECT(memory::ModId::kColumnFamilySet, ColumnFamilyData, global_ctx_->options_))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for ColumnFamilyData", K(ret));
  } else if (FAILED(tmp_cfd->init(args, global_ctx_, this))) {
    SE_LOG(WARN, "fail to init cfd", K(ret));
  } else if (!(column_family_data_.emplace(args.table_schema_.get_index_id(), tmp_cfd).second)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to insert into cfd map", K(ret), K(args));
  } else {
    cfd = tmp_cfd;
    if (0 == args.table_schema_.get_index_id()) {
      default_cfd_cache_ = cfd;
    }
    insert_into_cfd_list(cfd);
    insert_into_dump_list(cfd);
    SE_LOG(INFO, "success to create subtbale", K(args));
  }

  return ret;
}

int ColumnFamilySet::add_sub_table(ColumnFamilyData *sub_table)
{
  int ret = Status::kOk;

  if (IS_NULL(sub_table)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(sub_table));
  } else if (!(column_family_data_.emplace(sub_table->GetID(), sub_table).second)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to insert into cfd map", K(ret), "index_id", sub_table->GetID());
  } else {
    if (0 == sub_table->GetID()) {
      default_cfd_cache_ = sub_table;
    }
    insert_into_cfd_list(sub_table);
    insert_into_dump_list(sub_table);
    SE_LOG(INFO, "success to create subtbale", "index_id", sub_table->GetID());
  }

  return ret;
}

// REQUIRES: DB mutex held
void ColumnFamilySet::FreeDeadColumnFamilies() {
  autovector<ColumnFamilyData*> to_delete;
  for (auto cfd = dummy_cfd_->next_; cfd != dummy_cfd_; cfd = cfd->next_) {
    if (cfd->refs_.load(std::memory_order_relaxed) == 0) {
      to_delete.push_back(cfd);
    }
  }
  for (auto cfd : to_delete) {
    // this is very rare, so it's not a problem that we do it under a mutex
    MOD_DELETE_OBJECT(ColumnFamilyData, cfd);
  }
}

// under a DB mutex AND from a write thread
void ColumnFamilySet::RemoveColumnFamily(ColumnFamilyData* cfd) {
  auto cfd_iter = column_family_data_.find(cfd->GetID());
  assert(cfd_iter != column_family_data_.end());
  column_family_data_.erase(cfd_iter);
  if (!(dropped_column_family_data_.emplace(cfd->GetID(), cfd->GetID()).second)) {
    SE_LOG(WARN, "fail to emplace back to dropped_column_family_data", "index_id", cfd->GetID());
  } else {
    auto cur = cfd->get_dump_cfd();
    if (nullptr != cur) {
      cur->remove_from_list(dump_head_cfd_, dump_tail_cfd_);
    }
  }
}

//used for recovery remove subtale, ColumnFamilySet will be replace by AllSubtableMap
void ColumnFamilySet::remove_cfd_from_list(ColumnFamilyData *cfd)
{
  auto prev = cfd->prev_;
  auto next = cfd->next_;
  prev->next_ = next;
  next->prev_ = prev;
}
void ColumnFamilySet::insert_into_cfd_list(ColumnFamilyData *cfd)
{
  auto prev = dummy_cfd_->prev_;
  cfd->next_ = dummy_cfd_;
  cfd->prev_ = prev;
  prev->next_ = cfd;
  dummy_cfd_->prev_ = cfd;
}

bool ColumnFamilySet::is_subtable_dropped(int64_t index_id)
{
  return dropped_column_family_data_.end() != dropped_column_family_data_.find(index_id);
}

void ColumnFamilySet::insert_into_dump_list(ColumnFamilyData *cfd)
{
  if (nullptr != global_ctx_ && 0 == global_ctx_->options_.dump_memtable_limit_size) {
    return ;
  } else if (IS_NULL(cfd) || cfd->IsDropped()) {
    return ;
  }
  DumpCfd *dcfd = nullptr;
  if (nullptr == cfd->get_dump_cfd()) {
    dcfd = ALLOC_OBJECT(DumpCfd, arena_, cfd);
    cfd->set_dump_cfd(dcfd);
    if (nullptr == dump_tail_cfd_) {
      assert(nullptr == dump_head_cfd_);
      dcfd->cfd_ = cfd;
      dump_tail_cfd_ = dcfd;
      dump_head_cfd_ = dcfd;
      dcfd->in_list_ = true;
      return ;
    }
  } else {
    dcfd = cfd->get_dump_cfd();
  }
  auto cur = dump_tail_cfd_;
  RecoveryPoint cur_rp;
  RecoveryPoint cfd_rp = cfd->get_recovery_point();
  bool done = false;
  while (nullptr != cur) {
    assert(cur->cfd_);
    if (!cur->cfd_->IsDropped()) {
      cur_rp = cur->cfd_->get_recovery_point();
      if (cfd_rp.log_file_number_ >= cur_rp.log_file_number_) {
        if (dcfd->in_list_) {
          dcfd->remove_from_list(dump_head_cfd_, dump_tail_cfd_);
        }
        dcfd->insert_into_list_after(dump_tail_cfd_, cur);
        done = true;
        break;
      }
    } else {
      // just move it from list
      cur->remove_from_list(dump_head_cfd_, dump_tail_cfd_);
      SE_LOG(INFO, "CK_INFO: cfd is dropped", K(cur->cfd_->GetID()));
    }
    cur = cur->prev_;
  }
  if (!done && dcfd != dump_head_cfd_) {
    if (dcfd->in_list_) {
      dcfd->remove_from_list(dump_head_cfd_, dump_tail_cfd_);
    } else {
      dcfd->reset_list_info();
    }
    if (nullptr != dump_head_cfd_) {
      dcfd->next_ = dump_head_cfd_;
      assert(dump_head_cfd_);
      dump_head_cfd_->prev_ = dcfd;
    } else {
      dump_tail_cfd_ = dcfd;
    }
    dump_head_cfd_ = dcfd;
    dcfd->in_list_ = true;
  }
  SE_LOG(DEBUG, "CK_INFO: total find count for insert", K(cfd->GetID()), K(cur_rp), K(cfd_rp));
}

std::vector<ColumnFamilyData *> ColumnFamilySet::get_next_dump_cfds(const int64_t file_number,
                                                                    const SequenceNumber dump_seq) {
  std::vector<ColumnFamilyData *> res_cfds;
  auto *cur = dump_head_cfd_;
  RecoveryPoint cur_rp;
  DumpCfd *find_head = nullptr;
  DumpCfd *find_tail = nullptr;
  while (nullptr != cur) {
    assert(cur->cfd_);
    if (cur->cfd_->IsDropped()) {
      // just move it from list
      cur->remove_from_list(dump_head_cfd_, dump_tail_cfd_);
      SE_LOG(INFO, "CK_INFO: cfd is dropped", K(cur->cfd_->GetID()));
    } else {
      cur_rp = cur->cfd_->get_recovery_point();
      if (cur_rp.log_file_number_ < file_number
          || (cur_rp.log_file_number_ == file_number && cur_rp.seq_ < dump_seq)) {
        res_cfds.push_back(cur->cfd_);
        if (nullptr == find_head) {
          find_head = cur;
        }
        find_tail = cur;
        cur->in_list_ = false;
      } else {
        break;
      }
    }
    cur = cur->next_;
  }
  if (nullptr != find_head) {
    if (dump_head_cfd_ == find_head) {
      dump_head_cfd_ = find_tail->next_;
    } else {
      assert(find_head->prev_);
      find_head->prev_->next_ = find_tail->next_;
    }
    if (dump_tail_cfd_ == find_tail) {
      dump_tail_cfd_ = find_head->prev_;
    } else {
      assert(find_tail->next_);
      find_tail->next_->prev_ = find_head->prev_;
    }
  }
  SE_LOG(DEBUG, "CK_INFO: total find count for looking up", K(res_cfds.size()),
        K(file_number), K(dump_seq), K(cur_rp));
  return res_cfds;
}
// under a DB mutex OR from a write thread
bool ColumnFamilyMemTablesImpl::Seek(uint32_t column_family_id) {
  auto iter = sub_table_map_.find(column_family_id);
  if (iter != sub_table_map_.end()) {
    current_ = iter->second;
    assert(column_family_id == current_->GetID());
  } else {
    current_ = nullptr;
  }

  handle_.SetCFD(current_);
  return current_ != nullptr;
}

uint64_t ColumnFamilyMemTablesImpl::GetLogNumber() const {
  assert(current_ != nullptr);
  return static_cast<uint64_t>(current_->get_recovery_point().log_file_number_);
}

uint64_t ColumnFamilyMemTablesImpl::GetSequence() const
{
  assert(nullptr != current_);
  return current_->get_recovery_point().seq_;
}

MemTable* ColumnFamilyMemTablesImpl::GetMemTable() const {
  assert(current_ != nullptr);
  return current_->mem();
}

ColumnFamilyHandle* ColumnFamilyMemTablesImpl::GetColumnFamilyHandle() {
  assert(current_ != nullptr);
  return &handle_;
}

bool ColumnFamilyMemTablesImpl::is_subtable_dropped(int64_t index_id)
{
  bool ret = false;
  if (column_family_set_->get_during_replay_wal()) {
    ret = column_family_set_->is_subtable_dropped(index_id);
  }

  return ret;
}

uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family) {
  uint32_t column_family_id = 0;
  if (column_family != nullptr) {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    column_family_id = cfh->GetID();
  }
  return column_family_id;
}

const Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family) {
  if (column_family != nullptr) {
    return column_family->GetComparator();
  }
  return nullptr;
}

}  // namespace db
}  // namespace smartengine
