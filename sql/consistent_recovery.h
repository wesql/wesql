/*
  Portions Copyright (c) 2024, ApeCloud Inc Holding Limited
  Portions Copyright (c) 2009, 2023, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef CONSISTENT_RECOVERY_INCLUDED
#define CONSISTENT_RECOVERY_INCLUDED
#include "my_dbug.h"
#include "my_sys.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/psi/mysql_file.h"
#include "mysqld_error.h"
#include "objstore.h"

#define CONSISTENT_SNAPSHOT_RECOVERY_FILE "#status_snapshot_recovery"
#define CONSISTENT_SNAPSHOT_RECOVERY_STAGE_NONE 0
#define CONSISTENT_SNAPSHOT_RECOVERY_STAGE_BEGIN 1
#define CONSISTENT_SNAPSHOT_RECOVERY_STAGE_DATA_READY 2
#define CONSISTENT_SNAPSHOT_RECOVERY_STAGE_END 3

#define CONSISTENT_RECOVERY_SMARTENGINE_OBJECTSTORE_ROOT_PATH "smartengine/v1"

// smartengine, innodb, binlog no recovery order dependency.
#define CONSISTENT_RECOVERY_NO 0
#define CONSISTENT_RECOVERY_INNODB 1
#define CONSISTENT_RECOVERY_SMARTENGINE 2
#define CONSISTENT_RECOVERY_BINLOG 4
#define CONSISTENT_RECOVERY_SMARTENGINE_EXTENT 8

typedef struct Consistent_snapshot_recovery_status {
  int m_recovery_status;
  uint64_t m_end_binlog_pos;
  uint64_t m_end_consensus_index;
  char m_apply_stop_timestamp[MAX_DATETIME_FULL_WIDTH + 4];
} Consistent_snapshot_recovery_status;

/**
 * @brief The consistent recovery class.
 * Build recovery from consistent snapshot.
 */
class Consistent_recovery {
 public:
  Consistent_recovery();
  ~Consistent_recovery() {}
  // recovery consistent snapshot
  int recovery_consistent_snapshot(int flags);
  bool recovery_consistent_snapshot_finish();
  // read consistent snapshot file
  bool read_consistent_snapshot_file();
  // recovery mysql innodb
  bool recovery_mysql_innodb();
  // recovery smartengine
  bool recovery_smartengine();
  // recovery binlog
  bool recovery_binlog(const char *binlog_index_name, const char *bin_log_name);
  bool recovery_smartengine_objectstore_data();
  int get_last_persistent_binlog_consensus_index();
  int read_consistent_snapshot_recovery_status(
      Consistent_snapshot_recovery_status &recovery_status);
  int write_consistent_snapshot_recovery_status(
      Consistent_snapshot_recovery_status &recovery_status);
  int consistent_snapshot_consensus_recovery_finish();

 private:
  int init_objstore_in_initialize();
  int init_objstore_in_recovery();
  int init_consistent_snapshot_recovery_context();
  int open_binlog_index_file(IO_CACHE *index_file, const char *index_file_name,
                             enum cache_type type);
  int add_line_to_index_file(IO_CACHE *index_file, const char *log_name);
  int close_binlog_index_file(IO_CACHE *index_file);
  int merge_slice_to_binlog_file(const char *to_binlog_file,
                                 std::string mysql_binlog_name,
                                 my_off_t &mysql_binlog_end_pos);
  int fetch_last_persistent_snapshot_index_file(std::string &last_index);
  int fetch_last_persistent_binlog_index_file(std::string &last_binlog_index);
  enum Consistent_recovery_state {
    CONSISTENT_RECOVERY_STATE_NONE = 0,
    CONSISTENT_RECOVERY_STATE_SNAPSHOT_FILE = 1,
    CONSISTENT_RECOVERY_STATE_MYSQL_INNODB = 2,
    CONSISTENT_RECOVERY_STATE_SE = 3,
    CONSISTENT_RECOVERY_STATE_BINLOG = 4,
    CONSISTENT_RECOVERY_STATE_SST = 5,
    CONSISTENT_RECOVERY_STATE_END
  };
  enum Consistent_recovery_type {
    CONSISTENT_RECOVERY_NONE = 0,
    CONSISTENT_RECOVERY_REBULD = 1,
    CONSISTENT_RECOVERY_PITR = 2,
    CONSISTENT_RECOVERY_CLONE = 3
  };
  Consistent_recovery_type m_recovery_type;
  Consistent_recovery_state m_state;
  objstore::ObjectStore *recovery_objstore;
  objstore::ObjectStore *init_destination_objstore;
  char m_smartengine_objstore_dir[FN_REFLEN + 1];
  char m_init_destination_objstore_bucket[FN_REFLEN + 1];
  char m_objstore_bucket[FN_REFLEN + 1];
  char m_binlog_archive_dir[FN_REFLEN + 1];
  // Binlog file required for recovering consistent snapshot to a consistent
  // state
  char m_binlog_file[FN_REFLEN + 1];
  // End binlog position for recovering the consistent snapshot to a consistent
  // state Only used when consensus replication is disabled.
  uint64_t m_mysql_binlog_pos;
  // End consensus index for recovering the consistent snapshot to a consistent
  // state
  uint64_t m_consensus_index;
  uint64_t m_se_snapshot_id;
  char m_mysql_binlog_end_file[FN_REFLEN + 1];
  my_off_t m_mysql_binlog_end_pos;
  char m_snapshot_end_binlog_file[FN_REFLEN + 1];
  char m_binlog_index_keyid[FN_REFLEN + 1];
  IO_CACHE m_binlog_index_file;
  char m_binlog_index_file_name[FN_REFLEN + 1];

  char m_consistent_snapshot_archive_dir[FN_REFLEN + 1];
  char m_mysql_archive_recovery_dir[FN_REFLEN + 1];
  char m_mysql_archive_recovery_data_dir[FN_REFLEN + 1];
  char m_mysql_archive_recovery_binlog_dir[FN_REFLEN + 1];
  char m_mysql_innodb_clone_dir[FN_REFLEN + 1];

  char m_mysql_clone_keyid[FN_REFLEN + 1];
  char m_mysql_clone_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_mysql_clone_index_file;

  char m_se_backup_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_se_backup_index_file;
  uint m_se_backup_index;
  char m_se_snapshot_dir[FN_REFLEN + 1];
  char m_se_backup_keyid[FN_REFLEN + 1];
  char m_mysql_binlog_index_file_name[FN_REFLEN + 1];
  char m_consistent_snapshot_local_time
      [iso8601_size];  // MAX_DATETIME_FULL_WIDTH
};

extern Consistent_recovery consistent_recovery;
#endif
