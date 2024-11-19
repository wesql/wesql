/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "backup/hotbackup_impl.h"
#include "util/stack_trace.h"
#include "storage/extent_meta_manager.h"
#include "storage/io_extent.h"
#include "util/file_reader_writer.h"
#include "util/testharness.h"
#define private public
#define protected public
#include "db/db_test_util.h"

static const std::string test_dir = "/hotbackup_test";
namespace smartengine
{
using namespace common;
using namespace db;
using namespace storage;

namespace util
{
class HotbackupTest : public DBTestBase
{
public:
  HotbackupTest() : DBTestBase(test_dir, false)
  {
    backup_tmp_dir_path_ = db_->GetName() + BACKUP_TMP_DIR;
    backup_tmp_dir_path2_ = db_->GetName() + "/hotbackup_tmp2";
    backup_tmp_dir_path3_ = db_->GetName() + "/hotbackup_tmp3";
  }
  const char *backup_dir_ = "/backup_dir";
  const char *backup_dir2_ = "/backup_dir2";
  const char *backup_dir3_ = "/backup_dir3";
  void replay_sst_files(const std::string &backup_tmp_dir_path,
                        const std::string &extent_ids_file, 
                        const std::string &extent_file);
  void copy_sst_files(std::string backup_path);
  void save_old_current_file();
  void copy_old_current_file(std::string backup_path);
  void copy_rest_files(const std::string &backup_tmp_dir_path, std::string backup_path);
  void copy_extents(const std::string &backup_tmp_dir_path,
                    const std::string &extent_ids_file,
                    const std::string &extent_file);
  int64_t last_sst_file_num_ = 0;
  std::string backup_tmp_dir_path_;
  std::string backup_tmp_dir_path2_;
  std::string backup_tmp_dir_path3_;
};

struct RestFileChecker
{
  RestFileChecker(const uint64_t last_sst_file_num) : last_sst_file_num_(last_sst_file_num) {}
  inline bool operator()(const util::FileType &type, const uint64_t &file_num)
  {
    return (util::kDataFile == type && (file_num > last_sst_file_num_ || last_sst_file_num_ == 0)) ||
           (util::kWalFile == type) || (util::kManifestFile == type) || (util::kCheckpointFile == type) ||
           (util::kCurrentFile == type);
  }
  uint64_t last_sst_file_num_;
};

void HotbackupTest::replay_sst_files(const std::string &backup_dir,
                                     const std::string &extent_ids_file,
                                     const std::string &extent_file) {
  SequentialFile *id_reader = nullptr;
  SequentialFile *extent_reader = nullptr;
  char buf[16];
  char *extent_buf = nullptr;
  EnvOptions r_opt;
  ASSERT_OK(db_->GetEnv()->NewSequentialFile(extent_ids_file, id_reader, r_opt));
  ASSERT_OK(db_->GetEnv()->NewSequentialFile(extent_file, extent_reader, r_opt));
  std::unordered_map<int32_t, int32_t> fds_map;
  int fd = -1;
  std::string fname;
  int64_t i = 0;
  Slice id_res;
  Slice extent_res;
  ASSERT_TRUE(nullptr != (extent_buf = reinterpret_cast<char *>(memory::base_memalign(
      DIOHelper::DIO_ALIGN_SIZE, storage::MAX_EXTENT_SIZE, memory::ModId::kDefaultMod))));
  while (id_reader->Read(8, &id_res, buf).ok() && id_res.size() == 8) {
    ExtentId extent_id(*reinterpret_cast<const int64_t*>(id_res.data()));
    // read from extent file
    ASSERT_TRUE(extent_reader->Read(storage::MAX_EXTENT_SIZE, &extent_res, extent_buf).ok() 
        && extent_res.size() == storage::MAX_EXTENT_SIZE);
    // write to sst file
    auto iter = fds_map.find(extent_id.file_number);
    if (fds_map.end() == iter) {
      fname = FileNameUtil::data_file_path(backup_dir, extent_id.file_number);
      do {
        fd = open(fname.c_str(), O_WRONLY, 0644);
      } while (fd < 0 && errno == EINTR);
      ASSERT_TRUE(fd > 0) << "file: " << fname;
      fds_map[extent_id.file_number] = fd;
    } else {
      fd = iter->second;
    }
    FileIOExtent writable_extent;
    ASSERT_EQ(Status::kOk, writable_extent.init(extent_id, extent_id.id(), fd));
    ASSERT_EQ(Status::kOk, writable_extent.write(Slice(extent_buf, storage::MAX_EXTENT_SIZE), 0));
    SE_LOG(INFO, "replay extent", K(extent_id));
  }
  for (auto &iter : fds_map) {
    close(iter.second);
  }
  if (nullptr != extent_buf) {
    memory::base_memalign_free(extent_buf);
  }
}

void HotbackupTest::copy_extents(const std::string &backup_tmp_dir_path,
                                 const std::string &extent_ids_file,
                                 const std::string &extent_file)
{
  // read extent id file
  SequentialFile *reader = nullptr;
  EnvOptions r_opt;
  EnvOptions w_opt;
  w_opt.use_direct_writes = false;
  ASSERT_OK(db_->GetEnv()->NewSequentialFile(extent_ids_file, reader, r_opt));
  WritableFile *extent_writer = nullptr;
  ASSERT_OK(NewWritableFile(env_, extent_file, extent_writer, w_opt));
  char buf[16];
  char *extent_buf = nullptr;
  Slice result;
  std::unordered_map<int32_t, int32_t> fds_map;
  int fd = -1;
  std::string fname;
  ASSERT_TRUE(nullptr != (extent_buf = reinterpret_cast<char *>(memory::base_memalign(
      DIOHelper::DIO_ALIGN_SIZE, storage::MAX_EXTENT_SIZE, memory::ModId::kDefaultMod))));
  while (reader->Read(8, &result, buf).ok() && result.size() > 0) {
    ExtentId extent_id(*reinterpret_cast<const int64_t*>(result.data()));
    auto iter = fds_map.find(extent_id.file_number);
    if (fds_map.end() == iter) {
      fname = FileNameUtil::data_file_path(backup_tmp_dir_path, extent_id.file_number);
      do {
        fd = open(fname.c_str(), O_RDONLY, 0644);
      } while (fd < 0 && errno == EINTR);
      ASSERT_TRUE(fd > 0);
      fds_map[extent_id.file_number] = fd;
    } else {
      fd = iter->second;
    }
    Slice extent_result;
    FileIOExtent readable_extent;
    ASSERT_EQ(Status::kOk, readable_extent.init(extent_id, extent_id.id(), fd));
    ASSERT_EQ(Status::kOk, readable_extent.read(nullptr /*aio_handle=*/, 0, storage::MAX_EXTENT_SIZE, extent_buf, extent_result));
    ASSERT_OK(extent_writer->Append(extent_result));
    SE_LOG(INFO, "copy extent", K(extent_id));
  }
  ASSERT_OK(extent_writer->Close());
  for (auto &iter : fds_map) {
    close(iter.second);
  }
  if (nullptr != extent_buf) {
    memory::base_memalign_free(extent_buf);
  }
}

void HotbackupTest::save_old_current_file()
{
  std::vector<std::string> all_files;
  CurrentFileChecker file_checker;
  ASSERT_OK(db_->GetEnv()->GetChildren(dbname_, &all_files));
  int64_t file_num = 0;
  util::FileType type = kInvalidFileType;
  char cmd[1024];
  for (auto &file : all_files) {
    if (Status::kOk == FileNameUtil::parse_file_name(file, file_num, type) && file_checker(type, file_num)) {
      std::string file_path = dbname_ + "/" + file;
      snprintf(cmd, 1024, "cp %s %s", file_path.c_str(), (file_path + ".old").c_str());
      SE_LOG(INFO, "save current file to CURRENT.old", K(cmd));
      ASSERT_EQ(0, system(cmd));
    }
  }
}

void HotbackupTest::copy_old_current_file(std::string backup_path)
{
  std::vector<std::string> all_files;
  CurrentFileChecker file_checker;
  ASSERT_OK(db_->GetEnv()->GetChildren(dbname_, &all_files));
  int64_t file_num = 0;
  util::FileType type = kInvalidFileType;
  char cmd[1024];
  ASSERT_OK(db_->GetEnv()->CreateDirIfMissing(backup_path));
  for (auto &file : all_files) {
    if (Status::kOk == FileNameUtil::parse_file_name(file, file_num, type) && file_checker(type, file_num)) {
      std::string file_path = dbname_ + "/" + file;
      snprintf(cmd, 1024, "cp %s %s", (file_path + ".old").c_str(), backup_path.c_str());
      SE_LOG(INFO, "copy old current file to backup dir", K(cmd));
      ASSERT_EQ(0, system(cmd));
      snprintf(cmd, 1024, "mv %s %s", (backup_path + "/CURRENT.old").c_str(), (backup_path + "/CURRENT").c_str());
      SE_LOG(INFO, "rename old current file", K(cmd));
      ASSERT_EQ(0, system(cmd));
    }
  }
}

void HotbackupTest::copy_sst_files(std::string backup_path)
{
  std::vector<std::string> all_files;
  SSTFileChecker file_checker;
  ASSERT_OK(db_->GetEnv()->GetChildren(dbname_, &all_files));
  int64_t file_num = 0;
  util::FileType type = kInvalidFileType;
  char cmd[1024];
  ASSERT_OK(db_->GetEnv()->CreateDir(backup_path));
  for (auto &file : all_files) {
    if (Status::kOk == FileNameUtil::parse_file_name(file, file_num, type) && file_checker(type, file_num)) {
      std::string file_path = dbname_ + "/" + file;
      snprintf(cmd, 1024, "cp %s %s", file_path.c_str(), backup_path.c_str());
      SE_LOG(INFO, "copy sst file", K(cmd));
      ASSERT_EQ(0, system(cmd));
      if (file_num > last_sst_file_num_) {
        last_sst_file_num_ = file_num;
      }
    }
  }
}

void HotbackupTest::copy_rest_files(const std::string &backup_tmp_dir_path, std::string backup_path) {
  std::vector<std::string> all_files;
  RestFileChecker file_checker(last_sst_file_num_);
  ASSERT_OK(db_->GetEnv()->GetChildren(backup_tmp_dir_path, &all_files));
  int64_t file_num = 0;
  util::FileType type = kInvalidFileType;
  char cmd[1024];
  for (auto &file : all_files) {
    if ((Status::kOk == FileNameUtil::parse_file_name(file, file_num, type) && file_checker(type, file_num)) 
        || file == "extent_ids.inc" || file == "extent.inc") {
      std::string file_path = backup_tmp_dir_path + "/" + file;
      snprintf(cmd, 1024, "cp %s %s", file_path.c_str(), backup_path.c_str());
      SE_LOG(INFO, "copy rest sst file", K(cmd));
      ASSERT_EQ(0, system(cmd));
    }
  }
}

TEST_F(HotbackupTest, normal)
{
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BackupSnapshotId backup_id = 0;
  db::BinlogPosition binlog_pos;

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));

  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));

  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));
  backup_snapshot->unlock_instance();

  Close();
  DestroyDB(backup_tmp_dir_path_, last_options_);
}

TEST_F(HotbackupTest, incremental_extent)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BackupSnapshotId backup_id = 0;
  db::BinlogPosition binlog_pos;
  int32_t dummy_file_num = 0;
  std::string backup_path = dbname_ + backup_dir_;

  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, 
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));
  //
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);
  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path); 
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  //
  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, create_backups_and_recover) {
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BackupSnapshotId backup_id = 0;
  BackupSnapshotId backup_id2 = 0;
  BackupSnapshotId backup_id3 = 0;
  db::BinlogPosition binlog_pos;
  int32_t dummy_file_num = 0;
  std::string backup_path = dbname_ + backup_dir_;

  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));

  // create the first backup snapshot
  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));

  ASSERT_OK(Put(1, "a", "aaa"));
  ASSERT_OK(Put(1, "b", "bbb"));
  ASSERT_OK(Flush(1));

  Reopen(CurrentOptions(), &dbname_);
  ASSERT_EQ(1, backup_snapshot->get_backup_snapshot_map().get_backup_snapshot_count());

  // create the second backup snapshot
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id2, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));

  ASSERT_OK(Put(1, "c", "ccc"));
  ASSERT_OK(Put(1, "d", "ddd"));
  ASSERT_OK(Flush(1));

  Reopen(CurrentOptions(), &dbname_);
  ASSERT_EQ(2, backup_snapshot->get_backup_snapshot_map().get_backup_snapshot_count());

  // create the third backup snapshot
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id3, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));

  backup_snapshot->unlock_instance();

  ASSERT_OK(Put(1, "e", "eee"));
  ASSERT_OK(Put(1, "f", "fff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "ggg"));
  ASSERT_OK(Put(1, "h", "hhh"));
  ASSERT_OK(Flush(1));

  Reopen(CurrentOptions(), &dbname_);
  ASSERT_EQ(3, backup_snapshot->get_backup_snapshot_map().get_backup_snapshot_count());

  std::vector<BackupSnapshotId> backup_ids;
  backup_snapshot->list_backup_snapshots(backup_ids);
  ASSERT_EQ(3, backup_ids.size());
  ASSERT_TRUE(std::find(backup_ids.begin(), backup_ids.end(), backup_id) != backup_ids.end());
  ASSERT_TRUE(std::find(backup_ids.begin(), backup_ids.end(), backup_id2) != backup_ids.end());
  ASSERT_TRUE(std::find(backup_ids.begin(), backup_ids.end(), backup_id3) != backup_ids.end());
  backup_ids.clear();

  ASSERT_EQ(Status::kOk, backup_snapshot->release_old_backup_snapshot(db_, backup_id));
  ASSERT_EQ(Status::kOk, backup_snapshot->release_old_backup_snapshot(db_, backup_id2));
  ASSERT_EQ(Status::kOk, backup_snapshot->release_old_backup_snapshot(db_, backup_id3));
  backup_snapshot->list_backup_snapshots(backup_ids);
  ASSERT_EQ(0, backup_ids.size());

  Reopen(CurrentOptions(), &dbname_);
  ASSERT_EQ(0, backup_snapshot->get_backup_snapshot_map().get_backup_snapshot_count());

  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aaa", Get(1, "a"));
  ASSERT_EQ("bbb", Get(1, "b"));
  ASSERT_EQ("ccc", Get(1, "c"));
  ASSERT_EQ("ddd", Get(1, "d"));
  ASSERT_EQ("eee", Get(1, "e"));
  ASSERT_EQ("fff", Get(1, "f"));
  ASSERT_EQ("ggg", Get(1, "g"));
  ASSERT_EQ("hhh", Get(1, "h"));

  Close();
  DestroyDB(dbname_, last_options_);
}

TEST_F(HotbackupTest, delete_cf)
{
  CreateColumnFamilies({"hotbackup", "delete_cf"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BackupSnapshotId backup_id = 0;
  db::BinlogPosition binlog_pos;
  int32_t dummy_file_num = 0;
  std::string backup_path = dbname_ + backup_dir_;

  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));

  ASSERT_OK(Put(2, "a", "aa"));
  ASSERT_OK(Put(2, "b", "bb"));
  ASSERT_OK(Flush(2));
  DropColumnFamily(2);
  // tigger gc, pop_front_gc_job needs to be protected by db_mutex_
  dbfull()->TEST_LockMutex();
  DBImpl::GCJob *gc_job = dbfull()->pop_front_gc_job();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(gc_job != nullptr);
  int64_t index_id = gc_job->sub_table_->GetID();
  ASSERT_OK(gc_job->sub_table_->release_resource(false));
  ASSERT_OK(ExtentSpaceManager::get_instance().unregister_subtable(gc_job->sub_table_->get_table_space_id(), index_id));
  // delete sst file
  ExtentSpaceManager::get_instance().recycle_dropped_table_space();

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_,
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));

  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);
  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path);
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  //
  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, delete_cf_before_and_after_create_snapshot) {
  CreateColumnFamilies({"hotbackup", "delete_cf"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BackupSnapshotId backup_id = 0;
  BackupSnapshotId backup_id2 = 0;
  BinlogPosition binlog_pos;
  int32_t dummy_file_num = 0;
  std::string backup_path = dbname_ + backup_dir_;
  std::string backup_path2 = dbname_ + backup_dir2_;
  int64_t lob_size = 5 * 1024 * 1024;
  char *lob_value = new char[lob_size];
  memset(lob_value, 'l', lob_size);

  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());

  // create the first backup snapshot before delete cf
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_, backup_tmp_dir_path_.c_str()));
  copy_sst_files(backup_path);

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));
  ASSERT_OK(Put(1, "l", Slice(lob_value, lob_size)));

  ASSERT_OK(Put(2, "a", "aa"));
  ASSERT_OK(Put(2, "b", "bb"));
  ASSERT_OK(Flush(2));

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));

  // get meta snapshot of cf 2
  ColumnFamilyHandleImpl *cfh = dynamic_cast<ColumnFamilyHandleImpl *>(get_column_family_handle(2));
  SnapshotImpl *meta_snapshot = dynamic_cast<SnapshotImpl *>(cfh->cfd()->get_backup_meta_snapshot());
  cfh->cfd()->storage_manager_.release_backup_meta_snapshot(meta_snapshot);
  ASSERT_EQ(2, meta_snapshot->ref_);
  ASSERT_EQ(1, meta_snapshot->backup_ref_);

  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));

  DropColumnFamily(2);

  // tigger gc, pop_front_gc_job needs to be protected by db_mutex_
  dbfull()->TEST_LockMutex();
  DBImpl::GCJob *gc_job = dbfull()->pop_front_gc_job();
  dbfull()->TEST_UnlockMutex();
  ASSERT_TRUE(gc_job != nullptr);
  int64_t index_id = gc_job->sub_table_->GetID();
  // will unref meta snapshot ref count from 2 to 1
  ASSERT_OK(gc_job->sub_table_->release_memtable_resource());
  // will unref meta snapshot ref count from 1 to 0, but backup ref count is not 0, so the extents will not be recycled
  // at now.
  ASSERT_OK(gc_job->sub_table_->storage_manager_.release_extent_resource(false));
  ASSERT_EQ(0, meta_snapshot->ref_);
  ASSERT_EQ(1, meta_snapshot->backup_ref_);
  ASSERT_OK(ExtentSpaceManager::get_instance().unregister_subtable(gc_job->sub_table_->get_table_space_id(), index_id));
  // try to delete sst file
  ExtentSpaceManager::get_instance().recycle_dropped_table_space();

  copy_extents(backup_tmp_dir_path_, backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);
  copy_rest_files(backup_tmp_dir_path_, backup_path);
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);

  std::vector<ExtentId> extent_ids;
  auto *sn = dynamic_cast<SnapshotImpl *>(meta_snapshot);
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; level++) {
    storage::ExtentLayer *extent_layer = nullptr;
    storage::ExtentLayerVersion *extent_layer_version = sn->get_extent_layer_version(level);
    for (int64_t index = 0; index < extent_layer_version->get_extent_layer_size(); index++) {
      extent_layer = extent_layer_version->get_extent_layer(index);
      ASSERT_TRUE(extent_layer != nullptr);
      for (auto &extent_meta : extent_layer->extent_meta_arr_) {
        ASSERT_EQ(1, extent_meta->refs_);
        extent_ids.push_back(extent_meta->extent_id_);
      }
      for (auto &lob_meta : extent_layer->lob_extent_arr_) {
        ASSERT_EQ(1, lob_meta->refs_);
        extent_ids.push_back(lob_meta->extent_id_);
      }
    }
    extent_layer = extent_layer_version->dump_extent_layer_;
    if (extent_layer) {
      for (auto &extent_meta : extent_layer->extent_meta_arr_) {
        ASSERT_EQ(1, extent_meta->refs_);
        extent_ids.push_back(extent_meta->extent_id_);
      }
      for (auto &lob_meta : extent_layer->lob_extent_arr_) {
        ASSERT_EQ(1, lob_meta->refs_);
        extent_ids.push_back(lob_meta->extent_id_);
      }
    }
  }

  for (auto extent_id : extent_ids) {
    ASSERT_TRUE(nullptr != storage::ExtentMetaManager::get_instance().get_meta(extent_id));
    ASSERT_TRUE(ExtentSpaceManager::get_instance().TEST_find_extent(extent_id));
  }

  Reopen(CurrentOptions(), &dbname_);
  for (auto extent_id : extent_ids) {
    ASSERT_TRUE(nullptr != storage::ExtentMetaManager::get_instance().get_meta(extent_id));
    ASSERT_TRUE(ExtentSpaceManager::get_instance().TEST_find_extent(extent_id));
  }

  ASSERT_OK(backup_snapshot->release_current_backup_snapshot(db_));

  Reopen(CurrentOptions(), &dbname_);

  for (auto extent_id : extent_ids) {
    ASSERT_EQ(nullptr, storage::ExtentMetaManager::get_instance().get_meta(extent_id));
    ASSERT_FALSE(ExtentSpaceManager::get_instance().TEST_find_extent(extent_id));
  }

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());

  // create the second backup snapshot after delete cf
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_, backup_tmp_dir_path2_.c_str()));
  copy_sst_files(backup_path2);

  ASSERT_OK(Delete(1, "l"));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id2, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path2_, backup_tmp_dir_path2_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path2_ + BACKUP_EXTENTS_FILE);
  copy_rest_files(backup_tmp_dir_path2_, backup_path2);
  replay_sst_files(backup_path2, backup_path2 + BACKUP_EXTENT_IDS_FILE, backup_path2 + BACKUP_EXTENTS_FILE);

  for (auto extent_id : extent_ids) {
    ASSERT_EQ(nullptr, storage::ExtentMetaManager::get_instance().get_meta(extent_id));
    ASSERT_FALSE(ExtentSpaceManager::get_instance().TEST_find_extent(extent_id));
  }

  ASSERT_OK(backup_snapshot->release_current_backup_snapshot(db_));

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path);
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  ASSERT_EQ(lob_size, Get(1, "l").size());
  ASSERT_EQ("aa", Get(2, "a"));
  ASSERT_EQ("bb", Get(2, "b"));

  Close();
  DestroyDB(backup_path, last_options_);

  Reopen(CurrentOptions(), &backup_path2);
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  ASSERT_EQ("NOT_FOUND", Get(1, "l"));
  ASSERT_EQ(nullptr, get_column_family_handle(2));

  Close();
  DestroyDB(backup_path2, last_options_);
  delete[] lob_value;
}

TEST_F(HotbackupTest, large_object)
{
  CreateColumnFamilies({"large_object"}, CurrentOptions());
  int64_t lob_size = 5 * 1024 * 1024;
  std::string backup_path = dbname_ + backup_dir_;
  char *lob_value = new char[lob_size];
  memset(lob_value, '0', lob_size);

  BackupSnapshotId backup_id = 0;
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BinlogPosition binlog_pos;

  ASSERT_OK(Put(1, "1", Slice(lob_value, lob_size)));
  ASSERT_OK(Put(1, "2", Slice(lob_value, lob_size)));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);

  ASSERT_OK(Delete(1, "1"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "3", Slice(lob_value, lob_size)));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Delete(1, "3"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "4", Slice(lob_value, lob_size)));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, 
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));
  //
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path); 

  ASSERT_EQ("NOT_FOUND", Get(1, "1"));
  ASSERT_EQ(lob_size, Get(1, "2").size());
  ASSERT_EQ("NOT_FOUND", Get(1, "3"));
  ASSERT_EQ(lob_size, Get(1, "4").size());

  Close();
  DestroyDB(backup_path, last_options_);
  delete[] lob_value;
}

TEST_F(HotbackupTest, recover_from_old_current_file)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BackupSnapshotId backup_id = 0;
  BinlogPosition binlog_pos;
  int64_t dummy_file_num = 0;
  std::string backup_path = dbname_ + backup_dir_;

  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);
  save_old_current_file();

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));

  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_,
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));
  //
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);

  // use old current file to recover
  copy_old_current_file(backup_path);

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path);
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  //
  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, multi_checkpoint)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BackupSnapshotId backup_id = 0;
  BinlogPosition binlog_pos;
  int64_t dummy_file_num = 0;
  std::string backup_path = dbname_ + backup_dir_;

  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, 
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));
  //
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path); 
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  //
  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, delete_manifest_before_acquire)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  int64_t dummy_file_num = 0;
  BackupSnapshotId backup_id = 0;
  BinlogPosition binlog_pos;

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  // delete manifest file before acquire
  //ASSERT_OK(db_->GetEnv()->DeleteFile(dbname_ + "/MANIFEST-000005"));
  ASSERT_OK(db_->GetEnv()->DeleteFile(dbname_ + "/5.manifest"));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kNotFound, backup_snapshot->record_incremental_extent_ids(db_));

  backup_snapshot->unlock_instance();
}

TEST_F(HotbackupTest, checkpoint_contain_dropped_cf) {
  CreateColumnFamilies({"hotbackup", "delete_cf"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  int32_t dummy_file_num = 0;
  BackupSnapshotId backup_id = 0;
  BinlogPosition binlog_pos;
  std::string backup_path = dbname_ + backup_dir_;
  std::string str;
  str.assign(3 * 1024 * 1024, 'a');
  std::map<int, size_t> value_size_map;

  size_t lob_value_count = 20;
  size_t little_value_count = 100 + rand() % 200;
  size_t total_key_count = lob_value_count + little_value_count;

  while (value_size_map.size() < lob_value_count) {
    size_t idx = rand() % total_key_count;
    if (value_size_map.find(idx) == value_size_map.end()) {
      value_size_map.emplace(idx, 2 * 1024 * 1024 + rand() % 1024 * 1024);
    }
  }
  while (value_size_map.size() < total_key_count) {
    size_t idx = rand() % total_key_count;
    if (value_size_map.find(idx) == value_size_map.end()) {
      value_size_map.emplace(idx, rand() % 100);
    }
  }

  for (size_t i = 0; i < total_key_count; i++) {
    size_t val_size = value_size_map[i];
    ASSERT_OK(Put(1, std::to_string(i) + "del", str.substr(0, val_size)));
    ASSERT_OK(Put(2, std::to_string(i), str.substr(0, val_size)));
  }

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);

  SE_LOG(INFO, "", K(lob_value_count), K(little_value_count));

  for (size_t i = 0; i < total_key_count; i++) {
    size_t val_size = value_size_map[i];
    ASSERT_OK(Put(1, std::to_string(i), str.substr(0, val_size)));
    ASSERT_OK(Delete(1, std::to_string(i) + "del"));
  }

  ASSERT_OK(Flush(1));

  DropColumnFamily(2);

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));

  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &dbname_);
  for (size_t i = 0; i < total_key_count; i++) {
    ASSERT_EQ(Get(1, std::to_string(i)).size(), value_size_map[i]);
    ASSERT_EQ(Get(1, std::to_string(i) + "del"), "NOT_FOUND");
  }
  ASSERT_TRUE(get_column_family_handle(2) == nullptr);
  Close();
  DestroyDB(dbname_, last_options_);

  Reopen(CurrentOptions(), &backup_path);
  for (size_t i = 0; i < total_key_count; i++) {
    ASSERT_EQ(Get(1, std::to_string(i)).size(), value_size_map[i]);
    ASSERT_EQ(Get(1, std::to_string(i) + "del"), "NOT_FOUND");
  }
  ASSERT_TRUE(get_column_family_handle(2) == nullptr);

  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, delete_manifest_after_accquire) {
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  int64_t dummy_file_num = 0;
  BackupSnapshotId backup_id = 0;
  BinlogPosition binlog_pos;
  std::string backup_path = dbname_ + backup_dir_;

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  // delete manifest file after acquire
  //ASSERT_OK(db_->GetEnv()->DeleteFile(dbname_ + "/MANIFEST-000005"));
  ASSERT_OK(db_->GetEnv()->DeleteFile(dbname_ + "/5.manifest"));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));

  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path);
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));

  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, create_multiple_backup_snapshots) {
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  int64_t dummy_file_num = 0;
  BackupSnapshotId backup_id = 0;
  BinlogPosition binlog_pos;
  std::string backup_path = dbname_ + backup_dir_;

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());

  // create the first backup snapshot
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);

  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);

  // create the second backup snapshot
  std::string backup_path2 = dbname_ + backup_dir2_;
  BackupSnapshotId backup_id2 = 0;
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_, backup_tmp_dir_path2_.c_str()));
  copy_sst_files(backup_path2);

  ASSERT_OK(Put(1, "a", "aaa"));
  ASSERT_OK(Put(1, "b", "bbb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "ccc"));
  ASSERT_OK(Put(1, "d", "ddd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "eee"));
  ASSERT_OK(Put(1, "f", "fff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "ggg"));
  ASSERT_OK(Put(1, "h", "hhh"));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id2, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path2_, backup_tmp_dir_path2_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path2_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path2_, backup_path2);

  replay_sst_files(backup_path2, backup_path2 + BACKUP_EXTENT_IDS_FILE, backup_path2 + BACKUP_EXTENTS_FILE);

  // create the third backup snapshot
  std::string backup_path3 = dbname_ + backup_dir3_;
  BackupSnapshotId backup_id3 = 0;
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_, backup_tmp_dir_path3_.c_str()));
  copy_sst_files(backup_path3);

  ASSERT_OK(Put(1, "a", "aaaa"));
  ASSERT_OK(Put(1, "b", "bbbb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cccc"));
  ASSERT_OK(Put(1, "d", "dddd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "eeee"));
  ASSERT_OK(Put(1, "f", "ffff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gggg"));
  ASSERT_OK(Put(1, "h", "hhhh"));
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id3, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path3_, backup_tmp_dir_path3_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path3_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path3_, backup_path3);

  replay_sst_files(backup_path3, backup_path3 + BACKUP_EXTENT_IDS_FILE, backup_path3 + BACKUP_EXTENTS_FILE);

  ASSERT_EQ(3, backup_snapshot->get_backup_snapshot_map().get_backup_snapshot_count());
  ASSERT_TRUE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id));
  ASSERT_TRUE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id2));
  ASSERT_TRUE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id3));

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path);
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));

  ASSERT_EQ(0, backup_snapshot->get_backup_snapshot_map().get_backup_snapshot_count());
  ASSERT_FALSE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id));

  Close();
  DestroyDB(backup_path, last_options_);

  Reopen(CurrentOptions(), &backup_path2);
  ASSERT_EQ("aaa", Get(1, "a"));
  ASSERT_EQ("bbb", Get(1, "b"));
  ASSERT_EQ("ccc", Get(1, "c"));
  ASSERT_EQ("ddd", Get(1, "d"));
  ASSERT_EQ("eee", Get(1, "e"));
  ASSERT_EQ("fff", Get(1, "f"));
  ASSERT_EQ("ggg", Get(1, "g"));
  ASSERT_EQ("hhh", Get(1, "h"));

  ASSERT_EQ(1, backup_snapshot->get_backup_snapshot_map().get_backup_snapshot_count());
  ASSERT_TRUE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id));
  ASSERT_FALSE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id2));

  ASSERT_EQ(Status::kOk, backup_snapshot->release_old_backup_snapshot(db_, backup_id));

  Close();
  DestroyDB(backup_path2, last_options_);

  Reopen(CurrentOptions(), &backup_path3);
  ASSERT_EQ("aaaa", Get(1, "a"));
  ASSERT_EQ("bbbb", Get(1, "b"));
  ASSERT_EQ("cccc", Get(1, "c"));
  ASSERT_EQ("dddd", Get(1, "d"));
  ASSERT_EQ("eeee", Get(1, "e"));
  ASSERT_EQ("ffff", Get(1, "f"));
  ASSERT_EQ("gggg", Get(1, "g"));
  ASSERT_EQ("hhhh", Get(1, "h"));

  ASSERT_EQ(2, backup_snapshot->get_backup_snapshot_map().get_backup_snapshot_count());
  ASSERT_TRUE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id));
  ASSERT_TRUE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id2));
  ASSERT_FALSE(backup_snapshot->get_backup_snapshot_map().find_backup_snapshot(backup_id3));

  ASSERT_EQ(Status::kOk, backup_snapshot->release_old_backup_snapshot(db_, backup_id));
  ASSERT_EQ(Status::kOk, backup_snapshot->release_old_backup_snapshot(db_, backup_id2));

  Close();
  DestroyDB(backup_path3, last_options_);
}

TEST_F(HotbackupTest, amber_test)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  WriteOptions wo;
  wo.disableWAL = true;
  std::string backup_path = dbname_ + backup_dir_;

  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  int32_t dummy_file_num = 0;
  BackupSnapshotId backup_id = 0;
  BinlogPosition binlog_pos;

  ASSERT_OK(Put(1, "1", "11", wo));
  ASSERT_OK(Put(1, "2", "22", wo));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_, backup_path.c_str()));

  ASSERT_OK(Put(1, "a", "aa", wo));
  ASSERT_OK(Put(1, "b", "bb", wo));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));

  ASSERT_OK(Put(1, "c", "cc", wo));
  ASSERT_OK(Put(1, "d", "dd", wo));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "ee", wo));
  ASSERT_OK(Put(1, "f", "ff", wo));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg", wo));
  ASSERT_OK(Put(1, "h", "hh", wo));

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path); 

  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("NOT_FOUND", Get(1, "c"));
  ASSERT_EQ("NOT_FOUND", Get(1, "d"));
  ASSERT_EQ("NOT_FOUND", Get(1, "e"));
  ASSERT_EQ("NOT_FOUND", Get(1, "f"));
  ASSERT_EQ("NOT_FOUND", Get(1, "g"));
  ASSERT_EQ("NOT_FOUND", Get(1, "h"));

  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, lost_wal)
{
  CreateColumnFamilies({"lost_wal"}, CurrentOptions());
  BackupSnapshotImpl *backup_snapshot = BackupSnapshotImpl::get_instance();
  BackupSnapshotId backup_id = 0;
  BinlogPosition binlog_pos;
  std::string backup_path = dbname_ + backup_dir_;

  //step 1: insert base data 
  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Flush(1));

  // step 2: first step of xtrabackup, copy full sst files
  ASSERT_EQ(Status::kOk, backup_snapshot->lock_instance());
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files(backup_path);

  SyncPoint::GetInstance()->SetCallBack("DBImpl::wait_create_backup_snapshot",
      [&](void *arg) {
        DBImpl *db_impl_t = reinterpret_cast<DBImpl *>(arg);
        while (!db_impl_t->TEST_get_after_create_backup_snapshot()) {
          sleep(1);
        }
      });
  SyncPoint::GetInstance()->SetCallBack("BackupSnapshotImpl::acquire_snapshot::after_create_backup_snapshot",
      [&](void *arg) {
        DBImpl *db_impl_t = reinterpret_cast<DBImpl *>(arg);
        db_impl_t->TEST_set_after_create_backup_snapshot(true);
        db_impl_t->TEST_WaitForCompact();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  //step 3: insert incremental data
  ASSERT_OK(Put(1, "2", "22"));
  // This flush job should execute after create_backup_snapshot in accquire_backup_snapshot
  ASSERT_OK(Flush(1, false /**wait*/));


  //step 4: remaining steps of xtrabackup
  ASSERT_EQ(Status::kOk, backup_snapshot->accquire_backup_snapshot(db_, &backup_id, binlog_pos));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, 
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_, backup_path);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_current_backup_snapshot(db_));

  // step 5: prepare and restore the backup
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);

  backup_snapshot->unlock_instance();

  Reopen(CurrentOptions(), &backup_path); 

  //step 6: check data
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));

  //step 7: reset db status
  SyncPoint::GetInstance()->DisableProcessing();
  Close();
  DestroyDB(backup_path, last_options_);
}

} // namespace util
} // namespace smartengine

int main(int argc, char **argv) 
{
  std::string info_log_path = smartengine::util::test::TmpDir() + "/hotbackup_test.log";
  std::string abs_test_dir = smartengine::util::test::TmpDir() + test_dir;
  smartengine::util::test::remove_dir(abs_test_dir.c_str());
  smartengine::util::StackTrace::install_handler();
  ::testing::InitGoogleTest(&argc, argv);
	smartengine::util::test::init_logger(info_log_path.c_str(), smartengine::logger::INFO_LEVEL);
  return RUN_ALL_TESTS();
}
