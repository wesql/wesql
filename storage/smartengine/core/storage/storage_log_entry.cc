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

#include "storage/storage_log_entry.h"
#include "storage/change_info.h"

namespace smartengine
{
namespace storage
{
bool is_trans_log(int64_t log_type)
{
  return REDO_LOG_BEGIN == log_type || REDO_LOG_COMMIT == log_type;
}

bool is_partition_log(int64_t log_type)
{
  return REDO_LOG_ADD_SSTABLE == log_type
         || REDO_LOG_REMOVE_SSTABLE == log_type
         || REDO_LOG_MODIFY_SSTABLE == log_type
         || REDO_LOG_MODIFY_TABLE_SCHEMA == log_type;;
}

bool is_extent_log(int64_t log_type)
{
  return REDO_LOG_MODIFY_EXTENT_META == log_type;
}

LogHeader::LogHeader()
    : magic_number_(MAGIC_NUMBER),
      data_checksum_(0),
      event_(INVALID_EVENT),
      log_id_(0),
      log_length_(0)
{
}
LogHeader::~LogHeader()
{
}
bool LogHeader::is_valid()
{
  return MAGIC_NUMBER == magic_number_ && data_checksum_ > 0 && log_id_ >= 0 && log_length_ >=0;
}
DEFINE_COMPACTIPLE_SERIALIZATION(LogHeader, magic_number_, data_checksum_, log_id_, log_length_)

DEFINE_TO_STRING(LogHeader, KV_(magic_number), KV_(data_checksum), KV_(log_id), KV_(log_length))


ManifestLogEntryHeader::ManifestLogEntryHeader()
    : trans_id_(0),
      log_entry_seq_(0),
      log_entry_type_(0),
      log_entry_length_(0)
{
}
ManifestLogEntryHeader::~ManifestLogEntryHeader()
{
}
bool ManifestLogEntryHeader::is_valid()
{
  return trans_id_ >= 0 && log_entry_seq_ >= 0 && log_entry_length_ >= 0;
}
DEFINE_COMPACTIPLE_SERIALIZATION(ManifestLogEntryHeader, trans_id_, log_entry_seq_, log_entry_type_, log_entry_length_)

DEFINE_TO_STRING(ManifestLogEntryHeader, KV_(trans_id), KV_(log_entry_seq), KV_(log_entry_type), KV_(log_entry_length))

ChangeSubTableLogEntry::ChangeSubTableLogEntry()
    : table_schema_(),
      table_space_id_(0)
{
}
ChangeSubTableLogEntry::ChangeSubTableLogEntry(const schema::TableSchema &table_schema, int64_t table_space_id)
    : table_schema_(table_schema),
      table_space_id_(table_space_id)
{
}
ChangeSubTableLogEntry::~ChangeSubTableLogEntry()
{
}
DEFINE_COMPACTIPLE_SERIALIZATION(ChangeSubTableLogEntry, table_schema_, table_space_id_)

DEFINE_TO_STRING(ChangeSubTableLogEntry, KV_(table_schema), KV_(table_space_id))

ModifySubTableLogEntry::ModifySubTableLogEntry(ChangeInfo &change_info)
    : index_id_(-1),
      change_info_(change_info),
      recovery_point_()
{
}
ModifySubTableLogEntry::ModifySubTableLogEntry(int64_t index_id, ChangeInfo &change_info)
    : index_id_(index_id),
      change_info_(change_info),
      recovery_point_()
{
}
ModifySubTableLogEntry::~ModifySubTableLogEntry()
{
}

DEFINE_COMPACTIPLE_SERIALIZATION(ModifySubTableLogEntry, index_id_, change_info_, recovery_point_)

DEFINE_TO_STRING(ModifySubTableLogEntry, KV_(index_id), KV_(recovery_point), KV_(change_info))

ModifyTableSchemaLogEntry::ModifyTableSchemaLogEntry() : table_schema_() {}

ModifyTableSchemaLogEntry::ModifyTableSchemaLogEntry(const schema::TableSchema &table_schema) 
    : table_schema_(table_schema)
{}

ModifyTableSchemaLogEntry::~ModifyTableSchemaLogEntry()
{}

DEFINE_COMPACTIPLE_SERIALIZATION(ModifyTableSchemaLogEntry, table_schema_)

DEFINE_TO_STRING(ModifyTableSchemaLogEntry, KV_(table_schema))

ModifyExtentMetaLogEntry::ModifyExtentMetaLogEntry()
    : extent_meta_()
{
}
ModifyExtentMetaLogEntry::ModifyExtentMetaLogEntry(const ExtentMeta &extent_meta)
    : extent_meta_(extent_meta)
{
}
ModifyExtentMetaLogEntry::~ModifyExtentMetaLogEntry()
{
}

DEFINE_COMPACTIPLE_SERIALIZATION(ModifyExtentMetaLogEntry, extent_meta_)

DEFINE_TO_STRING(ModifyExtentMetaLogEntry, KV_(extent_meta))

AccquireBackupSnapshotLogEntry::AccquireBackupSnapshotLogEntry(BackupSnapshotId backup_id, uint64_t auto_increment_id)
    : backup_id_(backup_id), auto_increment_id_(auto_increment_id)
{
}
AccquireBackupSnapshotLogEntry::~AccquireBackupSnapshotLogEntry()
{
}
DEFINE_COMPACTIPLE_SERIALIZATION(AccquireBackupSnapshotLogEntry, backup_id_, auto_increment_id_)

DEFINE_TO_STRING(AccquireBackupSnapshotLogEntry, KV_(backup_id), KV_(auto_increment_id))

ReleaseBackupSnapshotLogEntry::ReleaseBackupSnapshotLogEntry(BackupSnapshotId backup_id)
    : backup_id_(backup_id)
{
}
ReleaseBackupSnapshotLogEntry::~ReleaseBackupSnapshotLogEntry()
{
}
DEFINE_COMPACTIPLE_SERIALIZATION(ReleaseBackupSnapshotLogEntry, backup_id_)

DEFINE_TO_STRING(ReleaseBackupSnapshotLogEntry, KV_(backup_id))

} //namespace storage
} //namespace smartengine
