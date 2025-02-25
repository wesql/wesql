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

#pragma once
#include "backup/hotbackup.h"
#include "db/db.h"
#include "logger/log_module.h"
#include "util/file_name.h"

namespace smartengine
{
namespace db
{
  class Snapshot;
}
namespace util
{

struct BackupSnapshotInfo {
  static const int64_t BACKUP_SNAPSHOT_INFO_VERSION = 1;
  BackupSnapshotId backup_id_;
  int64_t last_manifest_file_num_;
  uint64_t last_manifest_file_size_;
  uint64_t last_wal_file_num_;
  std::unordered_map<int64_t, int64_t> meta_snapshot_ids_; // index_id -> meta_snapshot_id

  DECLARE_SERIALIZATION();
  DECLARE_TO_STRING();
};

class BackupSnapshotImpl : public BackupSnapshot
{
public:
  virtual ~BackupSnapshotImpl() override;

  virtual int lock_instance() override;
  virtual int unlock_instance() override;
  virtual int lock_one_step() override;
  virtual int unlock_one_step() override;
  virtual int check_lock_status() override;

  virtual bool is_objstore_mode() const { return false; }

public:
  // Check backup job and do init
  virtual int init(db::DB *db, const char *backup_tmp_dir_path = nullptr) override;
  // Create backup tmp dir
  virtual int create_tmp_dir(db::DB *db) override;
  // Cleanup backup tmp dir
  virtual int cleanup_tmp_dir(db::DB *db) override;

  // for file mode only,
  // Do a manual checkpoint and flush memtable
  virtual int do_checkpoint(db::DB *db, const char *backup_tmp_dir_path = nullptr) override;
  // Acquire snapshots and hard-link/copy manifest files
  virtual int accquire_backup_snapshot(db::DB *db,
                                       BackupSnapshotId *backup_id,
                                       db::BinlogPosition &binlog_pos) override;

  // Parse incremental manifest files and record the modified extent ids
  virtual int record_incremental_extent_ids(db::DB *db) override;

  // Release a backup snapshot
  virtual int release_objstore_backup_snapshot(db::DB *db, BackupSnapshotId backup_id) override;
  // Release the current backup snapshot
  virtual int release_current_backup_snapshot(db::DB *db) override;
  // List all backup snapshots and return the backup ids
  virtual int list_backup_snapshots(std::vector<BackupSnapshotId> &backup_ids) override;
  // Get current backup id of the backup instance
  virtual BackupSnapshotId current_backup_id() override;

public:
  int try_exclusive_lock();
  int unlock_exclusive_lock();

private:
  BackupSnapshotImpl();

  friend class BackupSnapshotFileImpl;
  friend class BackupSnapshotObjStoreImpl;

private:
  // int link_sst_files(db::DB *db);

  template <typename CurrentFileChecker, typename DataFileChecker, typename WalFileChecker>
  int link_files(db::DB *db,
                 CurrentFileChecker *current_file_checker,
                 DataFileChecker *data_file_checker,
                 WalFileChecker *wal_file_checker);

  template <typename FileChecker>
  int link_dir_files(db::DB *db,
                     const std::string &dir_path,
                     const std::vector<std::string> &files,
                     FileChecker *file_checker);
  BackupSnapshotId generate_backup_id();
  // Cleanup the tmp dir
  int do_cleanup(db::DB *db);
  void reset();

private:
  static const int free_tid_ = -1;
  // A backup job is in process
  std::atomic<int> process_tid_;
  std::atomic<bool> instance_locked_;

  const char *backup_status_;

  // The written manifest file after do checkpoint
  int64_t first_manifest_file_num_;
  // The written manifest file used when acquiring snapshots
  int64_t last_manifest_file_num_;
  // The size of last_manifest_file
  uint64_t last_manifest_file_size_;
  // The written WAL log file after switch memtable in acquiring_snapshots
  uint64_t last_wal_file_num_;
  // Binlog position of last trx
  db::BinlogPosition last_binlog_pos_;
  // The backup snapshot id of the last time
  BackupSnapshotId last_backup_id_;
  // The backup snapshot id of the backup instance, which is milliseconds since epoch
  std::atomic<BackupSnapshotId> cur_backup_id_;
  // current backup snapshot(snapshots of all subtables)
  db::MetaSnapshotSet cur_meta_snapshots_;
  std::string backup_tmp_dir_path_;
};

class BackupSnapshotFileImpl : public BackupSnapshotImpl {
public:
  virtual ~BackupSnapshotFileImpl() override;

  static BackupSnapshotFileImpl *get_instance();

  virtual bool is_objstore_mode() const override { return false; }

private:
  BackupSnapshotFileImpl();
};

class BackupSnapshotObjStoreImpl : public BackupSnapshotImpl {
public:
  virtual ~BackupSnapshotObjStoreImpl() override;

  static BackupSnapshotObjStoreImpl *get_instance();

  virtual bool is_objstore_mode() const override { return true; }

private:
  BackupSnapshotObjStoreImpl();
};

struct SSTFileChecker
{
  inline bool operator()(const util::FileType type, const uint64_t file_num)
  {
    return util::kDataFile == type;
  }
};

struct CurrentFileChecker
{
  inline bool operator()(const util::FileType type, const uint64_t file_num) { return type == util::kCurrentFile; }
};

struct DataDirFileChecker
{
  DataDirFileChecker(const uint64_t first_manifest_file_num,
      const uint64_t last_manifest_file_num)
      : first_manifest_file_num_(first_manifest_file_num),
        last_manifest_file_num_(last_manifest_file_num)
  {}
  inline bool operator()(const util::FileType &type, const uint64_t &file_num)
  {
    // clang-format off
    return (util::kDataFile == type)
        // for the last manifest file, can't copy all contents here, but only copy specified size of the last manifest
        // file, which is matched with the size of the last manifest file when backup snapshot created, will process the
        // last manifest file later.
        || (util::kManifestFile == type && file_num < last_manifest_file_num_ && file_num >= first_manifest_file_num_) 
        || (util::kCheckpointFile == type && file_num <= last_manifest_file_num_);
    // clang-format on
  }
  uint64_t first_manifest_file_num_;
  uint64_t last_manifest_file_num_;
};

struct WalDirFileChecker
{
  WalDirFileChecker(const uint64_t last_wal_file_num) : last_wal_file_num_(last_wal_file_num)
  {}
  inline bool operator()(const util::FileType &type, const uint64_t &file_num)
  {
    return (util::kWalFile == type && file_num < last_wal_file_num_);
  }
  uint64_t last_wal_file_num_;
};

template <typename CurrentFileChecker, typename DataFileChecker, typename WalFileChecker>
int BackupSnapshotImpl::link_files(db::DB *db,
                                   CurrentFileChecker *current_file_checker,
                                   DataFileChecker *data_file_checker,
                                   WalFileChecker *wal_file_checker)
{
  int ret = common::Status::kOk;
  if (IS_NULL(db)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "db is nullptr", K(ret));
  } else {
    db->DisableFileDeletions();
    std::vector<std::string> data_files;
    std::vector<std::string> wal_files;
    if (FAILED(db->GetEnv()->GetChildren(db->GetName(), &data_files).code())) {
      SE_LOG(WARN, "Failed to get all files in data dir", K(ret));
    } else {
      if (current_file_checker && FAILED(link_dir_files(db, db->GetName(), data_files, current_file_checker))) {
        SE_LOG(WARN, "Failed to link current file", K(ret), "Dir", db->GetName());
      } else if (data_file_checker && FAILED(link_dir_files(db, db->GetName(), data_files, data_file_checker))) {
        SE_LOG(WARN, "Failed to link files in data dir", K(ret), "Dir", db->GetName());
      } else if (nullptr == wal_file_checker) {
        // skip link wal files
      } else if (FAILED(db->GetEnv()->GetChildren(db->GetDBOptions().wal_dir, &wal_files).code())) {
        SE_LOG(WARN, "Failed to get all files in wal dir", K(ret));
      } else if (FAILED(link_dir_files(db, db->GetDBOptions().wal_dir, wal_files, wal_file_checker))) {
        SE_LOG(WARN, "Failed to link files in wal dir", K(ret), "Dir", db->GetDBOptions().wal_dir);
      }
    }
    db->EnableFileDeletions(false);
  }
  return ret;
}

template<typename FileChecker>
int BackupSnapshotImpl::link_dir_files(db::DB *db, const std::string &dir_path,
    const std::vector<std::string> &files, FileChecker *file_checker)
{
  int ret = common::Status::kOk;
  if (IS_NULL(db) || IS_NULL(file_checker)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "db or file_checker is nullptr", K(ret), KP(db), KP(file_checker));
  } else {
    int64_t file_num = 0;
    util::FileType type = kInvalidFileType;
    for (size_t i = 0; SUCCED(ret) && i < files.size(); i++) {
      // Ignore the file if we can't recognize it.
      if ((common::Status::kOk == FileNameUtil::parse_file_name(files[i], file_num, type)) &&
          file_checker->operator()(type, file_num)) {
        std::string file_path = dir_path + "/" + files[i];
        std::string link_file_path = backup_tmp_dir_path_ + "/" + files[i];
        if (SUCCED(db->GetEnv()->FileExists(link_file_path).code())) {
          // skip existing file
          SE_LOG(DEBUG, "File already exist", K(ret), K(file_path), K(link_file_path));
        } else if (ret != common::Status::kNotFound) {
          SE_LOG(WARN, "IO error when checking file", K(ret), K(link_file_path));
        } else if (FAILED(db->GetEnv()->FileExists(file_path).code())) {
          if (ret == common::Status::kNotFound) {
            // this file was deleted, skip, overwrite ret
            ret = common::Status::kOk;
          } else {
            SE_LOG(WARN, "IO error when checking file", K(ret), K(file_path));
          }
        } else if (FAILED(db->GetEnv()->LinkFile(file_path, link_file_path).code())) {
          SE_LOG(WARN, "Failed to link file", K(ret), K(file_path), K(link_file_path));
        } else {
          SE_LOG(INFO, "Success to link file", K(ret), K(file_path), K(link_file_path));
        }
      }
    }
  }
  return ret;
}

} // namespace util
} // namespace smartengine
