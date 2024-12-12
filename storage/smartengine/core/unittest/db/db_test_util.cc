//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db_test_util.h"
#include "storage/storage_logger.h"
#include "table/table.h"

namespace smartengine {
using namespace util;
using namespace test;
using namespace table;
using namespace memtable;
using namespace cache;
using namespace common;

namespace db {

// Special Env used to delay background operations

SpecialEnv::SpecialEnv(Env* base)
    : EnvWrapper(base),
      rnd_(301),
      sleep_counter_(this),
      addon_time_(0),
      time_elapse_only_sleep_(false),
      no_slowdown_(false) {
  delay_sstable_sync_.store(false, std::memory_order_release);
  drop_writes_.store(false, std::memory_order_release);
  no_space_.store(false, std::memory_order_release);
  non_writable_.store(false, std::memory_order_release);
  count_random_reads_ = false;
  count_sequential_reads_ = false;
  manifest_sync_error_.store(false, std::memory_order_release);
  manifest_write_error_.store(false, std::memory_order_release);
  log_write_error_.store(false, std::memory_order_release);
  random_file_open_counter_.store(0, std::memory_order_relaxed);
  delete_count_.store(0, std::memory_order_relaxed);
  num_open_wal_file_.store(0);
  log_write_slowdown_ = 0;
  bytes_written_ = 0;
  sync_counter_ = 0;
  non_writeable_rate_ = 0;
  new_writable_count_ = 0;
  non_writable_count_ = 0;
  table_write_callback_ = nullptr;
}

DBTestBase::DBTestBase(const std::string path, bool use_objstore)
    : option_config_(kDefault),
      mem_env_(!getenv("MEM_ENV") ? nullptr : new MockEnv(Env::Default())),
      env_(new SpecialEnv(mem_env_ ? mem_env_ : Env::Default()))
{
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(4, Env::FILTER);
  dbname_ = test::TmpDir(env_) + path;
  alternative_wal_dir_ = dbname_ + "/wal";
  auto options = CurrentOptions();
  options.env = env_;
  if (env_->IsObjectStoreSupported() && use_objstore) {
    std::string_view endpoint = "";
    EXPECT_EQ(common::Status::kOk, env_->InitObjectStore("local", "test", &endpoint, false, "", "", 0).code());
  }
  auto delete_options = options;
  delete_options.wal_dir = alternative_wal_dir_;
  DestroyDB(dbname_, delete_options);
  // Destroy it for not alternative WAL dir is used.
  DestroyDB(dbname_, options);
  db_ = nullptr;
  Reopen(options);
  Random::GetTLSInstance()->Reset(0xdeadbeef);
}

DBTestBase::~DBTestBase() {
  if (env_->IsObjectStoreInited()) {
    env_->DestroyObjectStore();
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({});
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
  Options options;
  options.db_paths.emplace_back(dbname_, 0);
  options.db_paths.emplace_back(dbname_ + "_2", 0);
  options.db_paths.emplace_back(dbname_ + "_3", 0);
  options.db_paths.emplace_back(dbname_ + "_4", 0);
  options.env = env_;

  if (getenv("KEEP_DB")) {
    printf("DB is still at %s\n", dbname_.c_str());
  } else {
    DestroyDB(dbname_, options);
  }
  delete env_;
}

bool DBTestBase::ShouldSkipOptions(int option_config, int skip_mask) {
  if ((skip_mask & kSkipUniversalCompaction) &&
      (option_config == kUniversalCompaction ||
       option_config == kUniversalCompactionMultiLevel)) {
    return true;
  }
  if ((skip_mask & kSkipMergePut) && option_config == kMergePut) {
    return true;
  }
  if ((skip_mask & kSkipNoSeekToLast) &&
      (option_config == kHashLinkList || option_config == kHashSkipList)) {
    return true;
  }
  if ((skip_mask & kSkipPlainTable) &&
      (option_config == kPlainTableAllBytesPrefix ||
       option_config == kPlainTableFirstBytePrefix ||
       option_config == kPlainTableCappedPrefix ||
       option_config == kPlainTableCappedPrefixNonMmap)) {
    return true;
  }
  if ((skip_mask & kSkipHashIndex) &&
      (option_config == kBlockBasedTableWithPrefixHashIndex ||
       option_config == kBlockBasedTableWithWholeKeyHashIndex)) {
    return true;
  }
  if ((skip_mask & kSkipHashCuckoo) && (option_config == kHashCuckoo)) {
    return true;
  }
  if ((skip_mask & kSkipFIFOCompaction) && option_config == kFIFOCompaction) {
    return true;
  }
  if ((skip_mask & kSkipMmapReads) && option_config == kWalDirAndMmapReads) {
    return true;
  }
  return false;
}

// Switch to a fresh database with the next option configuration to
// test.  Return false if there are no more configurations to test.
bool DBTestBase::ChangeOptions(int skip_mask) {
  for (option_config_++; option_config_ < kEnd; option_config_++) {
    // only support concurrent insert
    if (ShouldSkipOptions(option_config_, skip_mask)) {
      continue;
    }
    break;
  }

  if (option_config_ >= kEnd) {
    Destroy(last_options_);
    return false;
  } else {
    auto options = CurrentOptions();
    DestroyAndReopen(options);
    return true;
  }
}

// Switch between different WAL settings
bool DBTestBase::ChangeWalOptions() {
  if (option_config_ == kDefault) {
    option_config_ = kDBLogDir;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    TryReopen(options);
    return true;
  } else if (option_config_ == kDBLogDir) {
    option_config_ = kWalDirAndMmapReads;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    TryReopen(options);
    return true;
  } else if (option_config_ == kWalDirAndMmapReads) {
    option_config_ = kRecycleLogFiles;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    TryReopen(options);
    return true;
  } else {
    return false;
  }
}

// Switch between different filter policy
// Jump from kDefault to kFilter to kFullFilter
bool DBTestBase::ChangeFilterOptions() {
  if (option_config_ == kDefault) {
    option_config_ = kFilter;
  } else if (option_config_ == kFilter) {
    option_config_ = kFullFilterWithNewTableReaderForCompactions;
  } else {
    return false;
  }
  Destroy(last_options_);

  auto options = CurrentOptions();
  TryReopen(options);
  return true;
}

// Return the current option configuration.
Options DBTestBase::CurrentOptions()
{
  Options options;
  options.write_buffer_size = 4090 * 4096;
  options.base_background_compactions = -1;
  options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;

  return CurrentOptions(options);
}

Options DBTestBase::CurrentOptions(const Options& defaultOptions)
{
  // this redundant copy is to minimize code change w/o having lint error.
  Options options = defaultOptions;
  BlockBasedTableOptions table_options;
  bool set_block_based_table_factory = true;
  switch (option_config_) {
    case kDBLogDir:
      //options.db_log_dir = alternative_db_log_dir_;
      break;
    case kWalDirAndMmapReads:
      options.wal_dir = alternative_wal_dir_;
      break;
    case kManifestFileSize:
      break;
    case kPerfOptions:
      break;
    case kCompressedBlockCache:
      table_options.block_cache_compressed = NewLRUCache(8 * 1024 * 1024);
      break;
    case kInfiniteMaxOpenFiles:
      //options.max_open_files = -1;
      break;
    case kxxHashChecksum: {
      // table_options.checksum = kxxHash;
      break;
    }
    case kBlockBasedTableWithIndexRestartInterval: {
      table_options.index_block_restart_interval = 8;
      break;
    }
    case kOptimizeFiltersForHits: {
      set_block_based_table_factory = true;
      break;
    }
    case kRowCache: {
      NewRowCache(32 * 1024 * 1024 /**capacity*/, options.row_cache);
      break;
    }
    case kRecycleLogFiles: {
      break;
    }
    case kConcurrentSkipList: {
      options.allow_concurrent_memtable_write = true;
      break;
    }

    default:
      break;
  }

  if (set_block_based_table_factory) {
    options.table_factory.reset(
        table::NewExtentBasedTableFactory(table_options));
  }
  options.env = env_;
  return options;
}

void DBTestBase::CreateColumnFamilies(const std::vector<std::string>& cfs,
                                      const Options& options) {
  ColumnFamilyOptions cf_opts(options);
  for (auto cf : cfs) {
    uint32_t cfi = get_next_column_family_id();
    ColumnFamilyHandle *handle = nullptr;
    ColumnFamilyDescriptor cfd(cf_opts);
    ASSERT_TRUE(0 == dbfull()->TEST_create_subtable(cfd, cfi, handle));
    ASSERT_TRUE(nullptr != handle);
    ASSERT_TRUE(cfi == handle->GetID());
    cfh_map_.emplace(cfi, handle);
  }
}

void DBTestBase::CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                                       const Options& options) {
  CreateColumnFamilies(cfs, options);
  std::vector<std::string> cfs_plus_default = cfs;
  cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
  ReopenWithColumnFamilies(cfs_plus_default, options);
}

void DBTestBase::DropColumnFamily(const int cf)
{
  auto cfh = get_column_family_handle(cf);
  dbfull()->DropColumnFamily(cfh);
  cfh_map_.erase(cfh->GetID());
}

void DBTestBase::ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const std::vector<Options>& options) {
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
}

void DBTestBase::ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const Options& options) {
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
}

Status DBTestBase::TryReopenWithColumnFamilies(
    const std::vector<std::string>& cfs, const std::vector<Options>& options) {
  Close();
  EXPECT_EQ(cfs.size(), options.size());
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(options[0], dbname_, &handles, &db_);
  if (s.ok()) {
    for (auto h : handles) {
        cfh_map_.erase(h->GetID());
        cfh_map_.emplace(h->GetID(), h);
    }
  }

  return s;
}

Status DBTestBase::TryReopenWithColumnFamilies(
    const std::vector<std::string>& cfs, const Options& options) {
  Close();
  std::vector<Options> v_opts(cfs.size(), options);
  return TryReopenWithColumnFamilies(cfs, v_opts);
}

void DBTestBase::Reopen(const Options& options, const std::string *db_name) {
  ASSERT_OK(TryReopen(options, db_name));
}

void DBTestBase::Close() {
  for (auto &cfh : cfh_map_) {
    if (cfh.second != dbfull()->DefaultColumnFamily()) {
      MOD_DELETE_OBJECT(ColumnFamilyHandle, cfh.second);
    }
  }
  cfh_map_.clear();
  MOD_DELETE_OBJECT(DB, db_);
  db_ = nullptr;
}

void DBTestBase::DestroyAndReopen(const Options& options) {
  // Destroy using last options
  Destroy(last_options_);
  ASSERT_OK(TryReopen(options));
}

void DBTestBase::Destroy(const Options& options) {
  Close();
  ASSERT_OK(DestroyDB(dbname_, options));
}

Status DBTestBase::ReadOnlyReopen(const Options& options) {
  return open_create_default_subtable(options);
}

Status DBTestBase::TryReopen(const Options& options, const std::string *db_name) {
  Close();
  last_options_.table_factory.reset();
  // Note: operator= is an unsafe approach here since it destructs shared_ptr in
  // the same order of their creation, in contrast to destructors which
  // destructs them in the opposite order of creation. One particular problme is
  // that the cache destructor might invoke callback functions that use Option
  // members such as statistics. To work around this problem, we manually call
  // destructor of table_facotry which eventually clears the block cache.
  last_options_ = options;
  last_options_.allow_concurrent_memtable_write = false;
  return open_create_default_subtable(options, db_name);
}

common::Status DBTestBase::open_create_default_subtable(const common::Options& options,
                                                        const std::string *db_name) {
  uint32_t kDefaultColumnFamilyId = 0;
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(options, db_name == nullptr ? dbname_ : *db_name, &handles, &db_);
  if (s.ok()) {
    for (auto h : handles) {
      cfh_map_.erase(h->GetID());
      cfh_map_.emplace(h->GetID(), h);
    }
    bool to_create = false;
    // always create the default column family handle
    if (s.ok()) {
      auto cfh0 = get_column_family_handle(kDefaultColumnFamilyId);
      uint32_t default_id = 0;
      auto default_cfh = dbfull()->DefaultColumnFamily();
      if ((nullptr != cfh0) && (nullptr != default_cfh)) {
        if (kDefaultColumnFamilyId != (default_id = default_cfh->GetID())) {
          SE_LOG(WARN, "Id for default sub table isn't",
                      K(kDefaultColumnFamilyId), K(default_id));
          //dbfull()->SetDefaultColumnFamily(cfh0);
        } else {
          if (cfh0 != default_cfh) {
            // They both must be attached with the default column family
            //assert(reinterpret_cast<ColumnFamilyHandleImpl*>(default_cfh)->cfd()
            //       == reinterpret_cast<ColumnFamilyHandleImpl*>(cfh0)->cfd());
//            delete cfh0;
            MOD_DELETE_OBJECT(ColumnFamilyHandle, cfh0);
            cfh_map_[kDefaultColumnFamilyId] = default_cfh;
          }
        }
      } else if (nullptr != cfh0) {
        //dbfull()->SetDefaultColumnFamily(cfh0);
      } else if (nullptr != default_cfh) {
        if (kDefaultColumnFamilyId != (default_id = default_cfh->GetID())) {
          SE_LOG(WARN, "Id for default sub table isn't",
                      K(kDefaultColumnFamilyId), K(default_id));
          to_create = true;
        } else {
          cfh_map_.emplace(kDefaultColumnFamilyId, default_cfh);
        }
      } else { // cfh0 is null and default_cfh is null
        to_create = true;
      }
    }
    if (to_create) {
      ColumnFamilyHandle *handle = nullptr;
      int ret = 0;
      // table:0 for default column family, 1+ for user's column family
      ColumnFamilyDescriptor cf{ColumnFamilyOptions(options)};
      if (FAILED(dbfull()->TEST_create_subtable(cf, kDefaultColumnFamilyId, handle))) {
        SE_LOG(ERROR, "Failed to create sub table!", K(ret));
        MOD_DELETE_OBJECT(ColumnFamilyHandle, handle);
      } else {
        cfh_map_.emplace(kDefaultColumnFamilyId, handle);
        //dbfull()->SetDefaultColumnFamily(handle);
      }
      s = Status(ret);
    }
    //assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    //delete handles[0];
  }
  return s;
}
bool DBTestBase::IsDirectIOSupported() {
  util::EnvOptions env_options;
  env_options.use_direct_writes = true;
  std::string tmp = FileNameUtil::temp_file_path(dbname_, 999);
  Status s;
  {
//    unique_ptr<WritableFile> file;
    WritableFile *file = nullptr;
    s = env_->NewWritableFile(tmp, file, env_options);
  }
  if (s.ok()) {
    s = env_->DeleteFile(tmp);
  }
  return s.ok();
}

Status DBTestBase::Flush(int cf, bool wait) {
  FlushOptions flush_options;
  flush_options.wait = wait;
  if (cf == 0) {
    return db_->Flush(flush_options);
  } else {
    return db_->Flush(flush_options, get_column_family_handle(cf));
  }
}

Status DBTestBase::CompactRange(int cf, uint32_t compact_type) {
  if (0 == cf) {
    return db_->CompactRange(compact_type);
  } else {
    return db_->CompactRange(get_column_family_handle(cf), compact_type);
  }
}

int DBTestBase::write_checkpoint()
{
  int64_t dummy_manifest_file_number = 0;
  return db_->do_manual_checkpoint(dummy_manifest_file_number);
}

void DBTestBase::schedule_shrink()
{
  (reinterpret_cast<DBImpl *>(db_))->TEST_schedule_shrink();
}

int DBTestBase::test_get_data_file_stats(const int64_t table_space_id,
    std::vector<storage::DataFileStatistics> &data_file_stats)
{
  return (reinterpret_cast<DBImpl *>(db_))->TEST_get_data_file_stats(table_space_id, data_file_stats);
}

Status DBTestBase::Put(const Slice& k, const Slice& v, WriteOptions wo) {
  return Put(0, k, v, wo);
}

Status DBTestBase::Put(int cf, const Slice& k, const Slice& v,
                       WriteOptions wo) {
  return db_->Put(wo, get_column_family_handle(cf), k, v);
}

Status DBTestBase::Delete(const std::string& k) {
  return db_->Delete(WriteOptions(), db_->DefaultColumnFamily(), k);
}

Status DBTestBase::Delete(int cf, const std::string& k) {
  return db_->Delete(WriteOptions(), get_column_family_handle(cf), k);
}

Status DBTestBase::SingleDelete(const std::string& k) {
  return db_->SingleDelete(WriteOptions(), db_->DefaultColumnFamily(), k);
}

Status DBTestBase::SingleDelete(int cf, const std::string& k) {
  return db_->SingleDelete(WriteOptions(), get_column_family_handle(cf), k);
}

std::string DBTestBase::Get(const std::string& k, const Snapshot* snapshot) {
  ReadOptions options;
  options.verify_checksums = true;
  options.snapshot = snapshot;
  std::string result;
  Status s = db_->Get(options, db_->DefaultColumnFamily(), k, &result);
  if (s.IsNotFound()) {
    result = "NOT_FOUND";
  } else if (!s.ok()) {
    result = s.ToString();
  }
  return result;
}

std::string DBTestBase::Get(int cf, const std::string& k,
                            const Snapshot* snapshot) {
  ReadOptions options;
  options.verify_checksums = true;
  options.snapshot = snapshot;
  std::string result;
  Status s = db_->Get(options, get_column_family_handle(cf), k, &result);
  if (s.IsNotFound()) {
    result = "NOT_FOUND";
  } else if (!s.ok()) {
    result = s.ToString();
  }
  return result;
}

int DBTestBase::modify_table_schema(const schema::TableSchema &table_schema)
{
  int ret = Status::kOk;
  int64_t dummy_commit_lsn = 0;

  if (FAILED(storage::StorageLogger::get_instance().begin(storage::MODIFY_INDEX))) {
    SE_LOG(WARN, "fail to start slog transaction", K(ret));
  } else {
    if (FAILED(db_->modify_table_schema(db_->DefaultColumnFamily(), table_schema))) {
      SE_LOG(WARN, "fail to modify table schema", K(ret), K(table_schema));
    } else if (FAILED(storage::StorageLogger::get_instance().commit(dummy_commit_lsn))) {
      SE_LOG(WARN, "fail to commit slog transaction", K(ret));
    }

    if (FAILED(ret)) {
      storage::StorageLogger::get_instance().abort();
    }
  }

  return ret;
}

int DBTestBase::modify_table_schema(int cf, const schema::TableSchema &table_schema)
{
  int ret = Status::kOk;
  int64_t dummy_commit_lsn = 0;

  if (FAILED(storage::StorageLogger::get_instance().begin(storage::MODIFY_INDEX))) {
    SE_LOG(WARN, "fail to start slog transaction", K(ret));
  } else {
    if (FAILED(db_->modify_table_schema(get_column_family_handle(cf), table_schema))) {
      SE_LOG(WARN, "fail to modify table schema", K(ret), K(table_schema));
    } else if (FAILED(storage::StorageLogger::get_instance().commit(dummy_commit_lsn))) {
      SE_LOG(WARN, "fail to commit slog transaction", K(ret));
    }

    if (FAILED(ret)) {
      storage::StorageLogger::get_instance().abort();
    }
  }

  return ret;
}

InternalIterator *DBTestBase::NewInternalIterator(Arena *arena, ColumnFamilyHandle *column_family)
{
  ReadOptions ro;
  ColumnFamilyData *cfd = nullptr;
  ColumnFamilyHandleImpl *cfh = nullptr;
  if (nullptr == column_family) {
    cfh = reinterpret_cast<ColumnFamilyHandleImpl *>(dbfull()->DefaultColumnFamily());
    cfd = cfh->cfd();
  } else {
    cfh = reinterpret_cast<ColumnFamilyHandleImpl *>(column_family);
    cfd = cfh->cfd();
  }

  QUERY_TRACE_BEGIN(monitor::TracePoint::DB_ITER_REF_SV);
  dbfull()->TEST_LockMutex();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  dbfull()->TEST_UnlockMutex();
  QUERY_TRACE_END();
  
  return dbfull()->NewInternalIterator(ro, cfd, super_version, arena);
}

uint64_t DBTestBase::GetNumSnapshots() {
  uint64_t int_num;
  EXPECT_TRUE(dbfull()->GetIntProperty("smartengine.num-snapshots", &int_num));
  return int_num;
}

uint64_t DBTestBase::GetTimeOldestSnapshots() {
  uint64_t int_num;
  EXPECT_TRUE(
      dbfull()->GetIntProperty("smartengine.oldest-snapshot-time", &int_num));
  return int_num;
}

// Return a string that contains all key,value pairs in order,
// formatted like "(k1->v1)(k2->v2)".
std::string DBTestBase::Contents(int cf) {
  std::vector<std::string> forward;
  std::string result;
  Iterator* iter = (cf == 0) ? db_->NewIterator(ReadOptions(), db_->DefaultColumnFamily())
                             : db_->NewIterator(ReadOptions(), get_column_family_handle(cf));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string s = IterStatus(iter);
    result.push_back('(');
    result.append(s);
    result.push_back(')');
    forward.push_back(s);
  }

  // Check reverse iteration results are the reverse of forward results
  unsigned int matched = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    EXPECT_LT(matched, forward.size());
    EXPECT_EQ(IterStatus(iter), forward[forward.size() - matched - 1]);
    matched++;
  }
  EXPECT_EQ(matched, forward.size());

  delete iter;
  return result;
}

int DBTestBase::NumTableFilesAtLevel(int level, int cf) {
  std::string property;
  if (cf == 0) {
    // default cfd
    EXPECT_TRUE(db_->GetProperty(
        "smartengine.num-files-at-level" + NumberToString(level), &property));
  } else {
    EXPECT_TRUE(db_->GetProperty(
        get_column_family_handle(cf), "smartengine.num-files-at-level" + NumberToString(level),
        &property));
  }
  return atoi(property.c_str());
}

double DBTestBase::CompressionRatioAtLevel(int level, int cf) {
  std::string property;
  if (cf == 0) {
    // default cfd
    EXPECT_TRUE(db_->GetProperty(
        "smartengine.compression-ratio-at-level" + NumberToString(level),
        &property));
  } else {
    EXPECT_TRUE(db_->GetProperty(
        get_column_family_handle(cf),
        "smartengine.compression-ratio-at-level" + NumberToString(level),
        &property));
  }
  return std::stod(property);
}

size_t DBTestBase::CountFiles() {
  std::vector<std::string> files;
  env_->GetChildren(dbname_, &files);

  std::vector<std::string> logfiles;
  if (dbname_ != last_options_.wal_dir) {
    env_->GetChildren(last_options_.wal_dir, &logfiles);
  }

  return files.size() + logfiles.size();
}

uint64_t DBTestBase::Size(const Slice& start, const Slice& limit, int cf) {
  Range r(start, limit);
  uint64_t size;
  if (cf == 0) {
    db_->GetApproximateSizes(&r, 1, &size);
  } else {
    db_->GetApproximateSizes(get_column_family_handle(1), &r, 1, &size);
  }
  return size;
}

std::string DBTestBase::DumpSSTableList() {
  std::string property;
  db_->GetProperty("smartengine.sstables", &property);
  return property;
}

void DBTestBase::GetSstFiles(std::string path,
                             std::vector<std::string>* files) {
  env_->GetChildren(path, files);

  files->erase(std::remove_if(files->begin(), files->end(),
                              [](std::string name) {
                                int64_t number;
                                FileType type;
                                return !(Status::kOk == FileNameUtil::parse_file_name(name, number, type) &&
                                         kDataFile == type);
                              }),
               files->end());
}

int DBTestBase::GetSstFileCount(std::string path) {
  std::vector<std::string> files;
  GetSstFiles(path, &files);
  return static_cast<int>(files.size());
}

// this will generate non-overlapping files since it keeps increasing key_idx
void DBTestBase::GenerateNewFile(int cf, Random* rnd, int* key_idx,
                                 bool nowait) {
  for (int i = 0; i < KNumKeysByGenerateNewFile; i++) {
    ASSERT_OK(Put(cf, Key(*key_idx), RandomString(rnd, (i == 99) ? 1 : 990)));
    (*key_idx)++;
  }
  if (!nowait) {
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
}

// this will generate non-overlapping files since it keeps increasing key_idx
void DBTestBase::GenerateNewFile(Random* rnd, int* key_idx, bool nowait) {
  for (int i = 0; i < KNumKeysByGenerateNewFile; i++) {
    ASSERT_OK(Put(Key(*key_idx), RandomString(rnd, (i == 99) ? 1 : 990)));
    (*key_idx)++;
  }
  if (!nowait) {
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
}

const int DBTestBase::kNumKeysByGenerateNewRandomFile = 51;

void DBTestBase::GenerateNewRandomFile(Random* rnd, bool nowait) {
  for (int i = 0; i < kNumKeysByGenerateNewRandomFile; i++) {
    ASSERT_OK(Put("key" + RandomString(rnd, 7), RandomString(rnd, 2000)));
  }
  ASSERT_OK(Put("key" + RandomString(rnd, 7), RandomString(rnd, 200)));
  if (!nowait) {
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
}

std::string DBTestBase::IterStatus(Iterator* iter) {
  std::string result;
  if (iter->Valid()) {
    result = iter->key().ToString() + "->" + iter->value().ToString();
  } else {
    result = "(invalid)";
  }
  return result;
}

Options DBTestBase::OptionsForLogIterTest() {
  Options options = CurrentOptions();
  return options;
}

std::string DBTestBase::DummyString(size_t len, char c) {
  return std::string(len, c);
}

void DBTestBase::VerifyIterLast(std::string expected_key, int cf) {
  Iterator* iter;
  ReadOptions ro;
  if (cf == 0) {
    iter = db_->NewIterator(ro, db_->DefaultColumnFamily());
  } else {
    iter = db_->NewIterator(ro, get_column_family_handle(cf));
  }
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), expected_key);
  delete iter;
}

void DBTestBase::CopyFile(const std::string& source,
                          const std::string& destination, uint64_t size) {
  const util::EnvOptions soptions;
//  unique_ptr<util::SequentialFile> srcfile;
  SequentialFile *srcfile = nullptr;
  ASSERT_OK(env_->NewSequentialFile(source, srcfile, soptions));
//  unique_ptr<WritableFile> destfile;
  WritableFile *destfile = nullptr;
  ASSERT_OK(env_->NewWritableFile(destination, destfile, soptions));

  if (size == 0) {
    // default argument means copy everything
    ASSERT_OK(env_->GetFileSize(source, &size));
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    uint64_t one = std::min(uint64_t(sizeof(buffer)), size);
    ASSERT_OK(srcfile->Read(one, &slice, buffer));
    ASSERT_OK(destfile->Append(slice));
    size -= slice.size();
  }
  ASSERT_OK(destfile->Close());
}

std::unordered_map<std::string, uint64_t> DBTestBase::GetAllSSTFiles(
    uint64_t* total_size) {
  std::unordered_map<std::string, uint64_t> res;

  if (total_size) {
    *total_size = 0;
  }
  std::vector<std::string> files;
  env_->GetChildren(dbname_, &files);
  for (auto& file_name : files) {
    int64_t number = 0;
    FileType type = util::kInvalidFileType;
    std::string file_path = dbname_ + "/" + file_name;
    if (Status::kOk == FileNameUtil::parse_file_name(file_name, number, type) && kDataFile == type) {
      uint64_t file_size = 0;
      env_->GetFileSize(file_path, &file_size);
      res[file_path] = file_size;
      if (total_size) {
        *total_size += file_size;
      }
    }
  }
  return res;
}

std::vector<std::uint64_t> DBTestBase::ListTableFiles(Env* env,
                                                      const std::string& path) {
  std::vector<std::string> files;
  std::vector<uint64_t> file_numbers;
  env->GetChildren(path, &files);
  int64_t number = 0;
  FileType type = util::kInvalidFileType;
  for (size_t i = 0; i < files.size(); ++i) {
    if (Status::kOk == FileNameUtil::parse_file_name(files[i], number, type)) {
      if (kDataFile == type) {
        file_numbers.push_back(number);
      }
    }
  }
  return file_numbers;
}


uint32_t DBTestBase::get_next_column_family_id() const {
  uint32_t next_id = 0;
  for (auto const &iter : cfh_map_) {
    if (iter.first > next_id) {
      next_id = iter.first;
    }
  }
  return ++next_id;
}

void DBTestBase::get_column_family_handles(
    std::vector<ColumnFamilyHandle*> &handles) const {
  handles.clear();
  for (auto const &iter : cfh_map_) {
    handles.emplace_back(iter.second);
  }
}

ColumnFamilyHandle* DBTestBase::get_column_family_handle(int64_t cf) const {
  ColumnFamilyHandle* h = nullptr;
  if (cf >= 0) {
    auto iter = cfh_map_.find((uint32_t)cf);
    if (iter != cfh_map_.end()) {
      h = iter->second;
    }
  }
  return h;
}

int test_open_db(const common::Options &options, const std::string &db_name, DB **db)
{
  int ret = Status::kOk;
  std::vector<ColumnFamilyHandle *> cf_handles;

  if (FAILED(DB::Open(options, db_name, &cf_handles, db).code())) {
    SE_LOG(WARN, "Fail to open db", K(ret));
  }

  for (auto cf_handle : cf_handles) {
    MOD_DELETE_OBJECT(ColumnFamilyHandle, cf_handle);
  }

  return ret;
}

int test_open_trans_db(const Options &options,
                       const TransactionDBOptions &trans_db_options,
                       const std::string &db_name,
                       TransactionDB **trans_db)
{
  int ret = Status::kOk;
  std::vector<ColumnFamilyHandle *> cf_handles;

  if (FAILED(TransactionDB::Open(options,
                                 trans_db_options,
                                 db_name,
                                 &cf_handles,
                                 trans_db).code())) {
    SE_LOG(WARN, "Fail to open trans db", K(ret)) ; 
  }

  for (auto cf_handle : cf_handles) {
    MOD_DELETE_OBJECT(ColumnFamilyHandle, cf_handle);
  }

  return ret;
}

}
}  // namespace smartengine
