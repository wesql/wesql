/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SMARTENGINE_INCLUDE_STORAGE_LOG_ENTRY_H_
#define SMARTENGINE_INCLUDE_STORAGE_LOG_ENTRY_H_

#include "db/recovery_point.h"
#include "storage/storage_meta_struct.h"
#include "util/serialization.h"
#include "util/to_string.h"

namespace smartengine
{
namespace storage
{
struct ChangeInfo;

enum SeEvent
{
  INVALID_EVENT = 0,
  FLUSH = 1,
  MAJOR_COMPACTION = 2,
  MINOR_COMPACTION = 3,
  SPLIT_COMPACTION = 4,
  CREATE_INDEX = 5,
  DROP_INDEX = 6,
  MODIFY_INDEX = 7,
  DUMP = 8,
  SHRINK_EXTENT_SPACE = 9,
};

enum ManifestRedoLogType {
  REDO_LOG_BEGIN = 0,
  REDO_LOG_COMMIT = 1,
  REDO_LOG_ADD_SSTABLE = 2,
  REDO_LOG_REMOVE_SSTABLE = 3,
  REDO_LOG_MODIFY_SSTABLE = 4,
  REDO_LOG_MODIFY_EXTENT_META = 5,
  REDO_LOG_MODIFY_TABLE_SCHEMA = 6,
};

bool is_trans_log(int64_t log_type);
bool is_partition_log(int64_t log_type);
bool is_extent_log(int64_t log_type);

//for compatibility, the variables in this struct must not been deleted or moved.
//new variables should only been added at the end.
struct LogHeader
{
  static const int64_t LOG_HEADER_VERSION = 1;
  static const int16_t MAGIC_NUMBER = 0x1234;

  int16_t magic_number_;
  int64_t data_checksum_;
  enum SeEvent event_;
  int64_t log_id_;
  int64_t log_length_;

  LogHeader();
  ~LogHeader();
  bool is_valid();
  DECLARE_COMPACTIPLE_SERIALIZATION(LOG_HEADER_VERSION)
  DECLARE_TO_STRING()
};

//for compatibility, the variables in this struct must not been deleted or moved.
//new variables should only been added at the end.
struct ManifestLogEntryHeader
{
  static const int64_t LOG_ENTRY_HEADER_VERSION = 1;  

  int64_t trans_id_;
  int64_t log_entry_seq_;
  int64_t log_entry_type_;
  int64_t log_entry_length_;

  ManifestLogEntryHeader();
  ~ManifestLogEntryHeader();
  bool is_valid();
  DECLARE_COMPACTIPLE_SERIALIZATION(LOG_ENTRY_HEADER_VERSION)
  DECLARE_TO_STRING()
};

struct ManifestLogEntry
{
  ManifestLogEntry() {}
  virtual ~ManifestLogEntry() {}
  DECLARE_PURE_VIRTUAL_SERIALIZATION();
};

//log entry of add or remove SubTable
//for compatibility, the variables in this struct must not been deleted or moved.
//new variables should only been added at the end.
struct ChangeSubTableLogEntry : public ManifestLogEntry
{
  static const int64_t CHANGE_SUB_TABLE_LOG_ENTRY_VERSION = 1;
  schema::TableSchema table_schema_;
  int64_t table_space_id_;

  ChangeSubTableLogEntry();
  ChangeSubTableLogEntry(const schema::TableSchema &table_schema, int64_t table_space_id);
  virtual ~ChangeSubTableLogEntry() override;
  DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE(CHANGE_SUB_TABLE_LOG_ENTRY_VERSION)
  DECLARE_TO_STRING()
};

//log entry of modify the exist SubTable
//for compatibility, the variables in this struct must not been deleted or moved.
//new variables should only been added at the end.
struct ModifySubTableLogEntry : public ManifestLogEntry
{
  static const int64_t MODIFY_SUBTABLE_LOG_ENTRY_VERSION = 1;

  int64_t index_id_;
  ChangeInfo &change_info_;
  db::RecoveryPoint recovery_point_;

  ModifySubTableLogEntry(ChangeInfo &change_info);
  ModifySubTableLogEntry(int64_t index_id, ChangeInfo &change_info);
  virtual ~ModifySubTableLogEntry() override;
  DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE(MODIFY_SUBTABLE_LOG_ENTRY_VERSION)
  DECLARE_TO_STRING()
};

struct ModifyTableSchemaLogEntry : public ManifestLogEntry
{
  static const int64_t MODIFY_TABLE_SCHEMA_LOG_ENTRY_VERSION = 1;

  schema::TableSchema table_schema_;

  ModifyTableSchemaLogEntry();
  ModifyTableSchemaLogEntry(const schema::TableSchema &table_schema);
  virtual ~ModifyTableSchemaLogEntry() override;

  DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE(MODIFY_TABLE_SCHEMA_LOG_ENTRY_VERSION)
  DECLARE_TO_STRING()
};

//log entry of modify the Extent
//for compatibility, the variables in this struct must not been deleted or moved.
//new variables should only been added at the end.
struct ModifyExtentMetaLogEntry : public ManifestLogEntry
{
  static const int64_t MODIFY_EXTENT_META_LOG_ENTRY_VERSION = 1;

  ExtentMeta extent_meta_;
  ModifyExtentMetaLogEntry();
  ModifyExtentMetaLogEntry(const ExtentMeta &extent_meta);
  virtual ~ModifyExtentMetaLogEntry() override;
  DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE(MODIFY_EXTENT_META_LOG_ENTRY_VERSION)
  DECLARE_TO_STRING()
};

struct AccquireBackupSnapshotLogEntry : public ManifestLogEntry
{
  static const int64_t ACQUIRE_SNAPSHOT_LOG_ENTRY_VERSION = 1;

  uint64_t backup_id_;
  uint64_t auto_increment_id_;

  AccquireBackupSnapshotLogEntry(BackupSnapshotId backup_id, uint64_t auto_increment_id);
  virtual ~AccquireBackupSnapshotLogEntry() override;
  DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE(ACQUIRE_SNAPSHOT_LOG_ENTRY_VERSION)
  DECLARE_TO_STRING()
};

struct ReleaseBackupSnapshotLogEntry : public ManifestLogEntry
{
  static const int64_t RELEASE_SNAPSHOT_LOG_ENTRY_VERSION = 1;

  uint64_t backup_id_;

  explicit ReleaseBackupSnapshotLogEntry(BackupSnapshotId backup_id);
  virtual ~ReleaseBackupSnapshotLogEntry() override;
  DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE(RELEASE_SNAPSHOT_LOG_ENTRY_VERSION)
  DECLARE_TO_STRING()
};

} // namespace storage
} // namespace smartengine
#endif
