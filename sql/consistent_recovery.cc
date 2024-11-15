/*
 * Portions Copyright (c) 2024, ApeCloud Inc Holding Limited
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

#include "sql/consistent_recovery.h"

#include <fstream>
#include <sstream>
#include <string>
#include "my_io.h"

#include "mf_wcomp.h"  // wild_one, wild_many
#include "mysql/plugin.h"
#include "sql/basic_ostream.h"
#include "sql/binlog.h"
#include "sql/binlog_archive.h"
#include "sql/consistent_archive.h"
#include "sql/debug_sync.h"
#include "sql/derror.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/log.h"
#include "sql/mysqld.h"
#include "sql/rpl_replica.h"
#include "sql/sql_plugin.h"
#include "sql/sql_show.h"
#include "sql/sys_vars_shared.h"

Consistent_recovery consistent_recovery;
static int copy_directory(const std::string &from, const std::string &to);
static bool remove_file(const std::string &file);
static void root_directory(std::string in, std::string *out, std::string *left);
static void recursive_create_dir(const std::string &dir,
                                 const std::string &root);
static int recursive_chmod(const std::string &from);
static int convert_str_to_datetime(const char *str, ulong &my_time);
static bool is_number(const char *str, ulonglong *res, bool allow_wildcards);
static const char *rpl_make_log_name(PSI_memory_key key, const char *opt,
                                     const char *def, const char *ext);
static int compare_log_name(const char *log_1, const char *log_2);

Consistent_recovery::Consistent_recovery()
    : m_recovery_type(CONSISTENT_RECOVERY_NONE),
      m_state(CONSISTENT_RECOVERY_STATE_NONE),
      recovery_objstore(nullptr),
      init_destination_objstore(nullptr),
      m_mysql_binlog_pos(0),
      m_consensus_index(0),
      m_se_snapshot_id(0),
      m_binlog_index_file(),
      m_mysql_clone_index_file(),
      m_se_backup_index_file(),
      m_se_backup_index(0) {
  m_objstore_bucket[0] = '\0';
  m_smartengine_objstore_dir[0] = '\0';
  m_init_destination_objstore_bucket[0] = '\0';
  m_binlog_archive_dir[0] = '\0';
  m_binlog_file[0] = '\0';
  m_mysql_binlog_end_file[0] = '\0';
  m_snapshot_end_binlog_file[0] = '\0';
  m_binlog_index_keyid[0] = '\0';
  m_binlog_index_file_name[0] = '\0';
  m_consistent_snapshot_archive_dir[0] = '\0';
  m_mysql_archive_recovery_dir[0] = '\0';
  m_mysql_archive_recovery_data_dir[0] = '\0';
  m_mysql_archive_recovery_binlog_dir[0] = '\0';
  m_mysql_innodb_clone_dir[0] = '\0';
  m_mysql_clone_keyid[0] = '\0';
  m_mysql_clone_index_file_name[0] = '\0';
  m_se_backup_index_file_name[0] = '\0';
  m_se_snapshot_dir[0] = '\0';
  m_se_backup_keyid[0] = '\0';
  m_mysql_binlog_index_file_name[0] = '\0';
  m_consistent_snapshot_local_time[0] = '\0';
}

/**
 * @brief Recovery database from consistent snapshot.
 * @note only support data source from object store.
 * @return int
 */
int Consistent_recovery::recovery_consistent_snapshot(int flags) {
  DBUG_TRACE;
  std::string err_msg;

  if (!opt_recovery_from_objstore && !opt_initialize_from_objstore) return 0;

  if (!opt_serverless) return 0;
  // Initialize database from consistent snapshot for PITR or clone.
  // If opt_recovery_consistent_snapshot_timestamp is set, recover to the
  // specified timestamp. Otherwise, recover to the latest consistent snapshot.
  my_setwd(mysql_real_data_home, MYF(0));
  if (opt_initialize && opt_initialize_from_objstore) {
    m_recovery_type = CONSISTENT_RECOVERY_PITR;
    err_msg.assign("Initialize database from source object store snapshot ");
    err_msg.append(" provider=");
    err_msg.append(opt_initialize_objstore_provider);
    err_msg.append(" region=");
    err_msg.append(opt_initialize_objstore_region);
    if (opt_initialize_objstore_endpoint) {
      err_msg.append(" endpoint=");
      err_msg.append(opt_initialize_objstore_endpoint);
    }
    err_msg.append(" bucket=");
    err_msg.append(opt_initialize_objstore_bucket);
    err_msg.append(" repo_objectsotre_id=");
    err_msg.append(opt_initialize_repo_objstore_id);
    err_msg.append(" branch_objectsotre_id=");
    err_msg.append(opt_initialize_branch_objstore_id);

    if (opt_recovery_consistent_snapshot_timestamp) {
      err_msg.append(" timestamp=");
      err_msg.append(opt_recovery_consistent_snapshot_timestamp);
    }

    err_msg.append(" to provider=");
    err_msg.append(opt_objstore_provider);
    err_msg.append(" region=");
    err_msg.append(opt_objstore_region);
    if (opt_objstore_endpoint) {
      err_msg.append(" endpoint=");
      err_msg.append(opt_objstore_endpoint);
    }
    err_msg.append(" bucket=");
    err_msg.append(opt_objstore_bucket);
    err_msg.append(" repo_objectsotre_id=");
    err_msg.append(opt_repo_objstore_id);
    err_msg.append(" branch_objectsotre_id=");
    err_msg.append(opt_branch_objstore_id);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // Init source object store.
    {
      objstore::init_objstore_provider(opt_initialize_objstore_provider);

      std::string_view endpoint(
          opt_initialize_objstore_endpoint
              ? std::string_view(opt_initialize_objstore_endpoint)
              : "");
      std::string obj_error_msg;
      recovery_objstore = objstore::create_source_object_store(
          std::string_view(opt_initialize_objstore_provider),
          std::string_view(opt_initialize_objstore_region),
          opt_initialize_objstore_endpoint ? &endpoint : nullptr,
          opt_initialize_objstore_use_https, obj_error_msg);
      if (recovery_objstore == nullptr) {
        err_msg.assign("Failed to create object store instance");
        if (!obj_error_msg.empty()) {
          err_msg.append(": ");
          err_msg.append(obj_error_msg);
        }
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        return 1;
      }
      if (opt_initialize_objstore_bucket) {
        strmake(m_objstore_bucket, opt_initialize_objstore_bucket,
                sizeof(m_objstore_bucket) - 1);
      } else {
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
               "Init database from object store, must set source"
               "--initialize_objectstore_bucket");
        return 1;
      }
    }
    if (opt_initialize_repo_objstore_id) {
      std::string binlog_objectstore_path(opt_initialize_repo_objstore_id);
      binlog_objectstore_path.append(FN_DIRSEP);
      binlog_objectstore_path.append(opt_initialize_branch_objstore_id);
      binlog_objectstore_path.append(FN_DIRSEP);
      binlog_objectstore_path.append(BINLOG_ARCHIVE_SUBDIR);
      binlog_objectstore_path.append(FN_DIRSEP);
      strmake(m_binlog_archive_dir, binlog_objectstore_path.c_str(),
              sizeof(m_binlog_archive_dir) - 1);

      std::string snapshot_objectstore_path(opt_initialize_repo_objstore_id);
      snapshot_objectstore_path.append(FN_DIRSEP);
      snapshot_objectstore_path.append(opt_initialize_branch_objstore_id);
      snapshot_objectstore_path.append(FN_DIRSEP);
      snapshot_objectstore_path.append(CONSISTENT_ARCHIVE_SUBDIR);
      snapshot_objectstore_path.append(FN_DIRSEP);
      strmake(m_consistent_snapshot_archive_dir,
              snapshot_objectstore_path.c_str(),
              sizeof(m_consistent_snapshot_archive_dir) - 1);
    } else {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "Init database from object store, must set "
             "--initialize_repo_objectstore_id");
      return 1;
    }

    // Init smartengine destination objec store.
    if (opt_initialize_smartengine_objectstore_data) {
      err_msg.assign("Initialize smartengine object store data ");
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());

      objstore::init_objstore_provider(opt_objstore_provider);
      std::string_view endpoint(
          opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");
      std::string obj_error_msg;
      init_destination_objstore = objstore::create_object_store(
          std::string_view(opt_objstore_provider),
          std::string_view(opt_objstore_region),
          opt_objstore_endpoint ? &endpoint : nullptr, opt_objstore_use_https,
          obj_error_msg);
      if (init_destination_objstore == nullptr) {
        err_msg.assign("Failed to create destination object store");
        if (!obj_error_msg.empty()) {
          err_msg.append(": ");
          err_msg.append(obj_error_msg);
        }
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        return 1;
      }
      if (opt_objstore_bucket) {
        strmake(m_init_destination_objstore_bucket, opt_objstore_bucket,
                sizeof(m_init_destination_objstore_bucket) - 1);
      } else {
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
               "Init database smartengine data from object store, must set "
               "--objectstore_bucket");
        return 1;
      }

      if (opt_repo_objstore_id) {
        std::string se_objectstore_path(opt_repo_objstore_id);
        se_objectstore_path.append(FN_DIRSEP);
        se_objectstore_path.append(opt_branch_objstore_id);
        se_objectstore_path.append(FN_DIRSEP);
        se_objectstore_path.append(
            CONSISTENT_RECOVERY_SMARTENGINE_OBJECTSTORE_ROOT_PATH);
        se_objectstore_path.append(FN_DIRSEP);
        strmake(m_smartengine_objstore_dir, se_objectstore_path.c_str(),
                sizeof(m_smartengine_objstore_dir) - 1);
      } else {
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
               "Init database smartengine data from object store, must set "
               "--repo_objectstore_id");
        return 1;
      }
    }
  } else if (!opt_initialize && opt_recovery_from_objstore) {
    // Crash recovery database from consistent snapshot.
    m_recovery_type = CONSISTENT_RECOVERY_REBULD;
    if (opt_recovery_consistent_snapshot_timestamp) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "Crash recovery snapshot can't set "
             "--recovery_snapshot_timestamp");
      return 1;
    }
    err_msg.assign("Crash recovery database from object store");
    err_msg.append(" provider=");
    err_msg.append(opt_objstore_provider);
    err_msg.append(" region=");
    err_msg.append(opt_objstore_region);
    if (opt_objstore_endpoint) {
      err_msg.append(" endpoint=");
      err_msg.append(opt_objstore_endpoint);
    }
    err_msg.append(" bucket=");
    err_msg.append(opt_objstore_bucket);
    err_msg.append(" repo_objectsotre_id=");
    err_msg.append(opt_repo_objstore_id);
    err_msg.append(" branch_objectsotre_id=");
    err_msg.append(opt_branch_objstore_id);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());

    objstore::init_objstore_provider(opt_objstore_provider);

    std::string_view endpoint(
        opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");
    std::string obj_error_msg;
    recovery_objstore = objstore::create_object_store(
        std::string_view(opt_objstore_provider),
        std::string_view(opt_objstore_region),
        opt_objstore_endpoint ? &endpoint : nullptr, opt_objstore_use_https,
        obj_error_msg);
    if (recovery_objstore == nullptr) {
      err_msg.assign("Failed to create object store instance");
      if (!obj_error_msg.empty()) {
        err_msg.append(": ");
        err_msg.append(obj_error_msg);
      }
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }
    if (opt_objstore_bucket) {
      strmake(m_objstore_bucket, opt_objstore_bucket,
              sizeof(m_objstore_bucket) - 1);
    } else {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "Crash recovery from object store, must set --objectstore_bucket");
      return 1;
    }

    if (opt_repo_objstore_id) {
      std::string binlog_objectstore_path(opt_repo_objstore_id);
      binlog_objectstore_path.append(FN_DIRSEP);
      binlog_objectstore_path.append(opt_branch_objstore_id);
      binlog_objectstore_path.append(FN_DIRSEP);
      binlog_objectstore_path.append(BINLOG_ARCHIVE_SUBDIR);
      binlog_objectstore_path.append(FN_DIRSEP);
      strmake(m_binlog_archive_dir, binlog_objectstore_path.c_str(),
              sizeof(m_binlog_archive_dir) - 1);

      std::string snapshot_objectstore_path(opt_repo_objstore_id);
      snapshot_objectstore_path.append(FN_DIRSEP);
      snapshot_objectstore_path.append(opt_branch_objstore_id);
      snapshot_objectstore_path.append(FN_DIRSEP);
      snapshot_objectstore_path.append(CONSISTENT_ARCHIVE_SUBDIR);
      snapshot_objectstore_path.append(FN_DIRSEP);
      strmake(m_consistent_snapshot_archive_dir,
              snapshot_objectstore_path.c_str(),
              sizeof(m_consistent_snapshot_archive_dir) - 1);
    } else {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "Crash recovery from object store, must set "
             "--repo_objectstore_id");
      return 1;
    }
  }
  if (!opt_recovery_consistent_snapshot_tmpdir) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Init or recovery db from object store, must set "
           "--recovery_snapshot_tmpdir.");
    return 1;
  }

  convert_dirname(mysql_real_data_home, mysql_real_data_home, NullS);

  if (test_if_hard_path(opt_recovery_consistent_snapshot_tmpdir)) {
    strmake(m_mysql_archive_recovery_dir,
            opt_recovery_consistent_snapshot_tmpdir,
            sizeof(m_mysql_archive_recovery_dir) - 1);
  } else {
    strxnmov(m_mysql_archive_recovery_dir,
             sizeof(m_mysql_archive_recovery_dir) - 1, mysql_real_data_home,
             opt_recovery_consistent_snapshot_tmpdir, NullS);
  }
  // create new tmp recovery dir.
  convert_dirname(m_mysql_archive_recovery_dir, m_mysql_archive_recovery_dir,
                  NullS);
  if (remove_file(m_mysql_archive_recovery_dir)) {
    err_msg.assign("Failed to create tmp recovery dir: ");
    err_msg.append(m_mysql_archive_recovery_dir);
    err_msg.append(" error=");
    err_msg.append(std::to_string(my_errno()));
    LogErr(WARNING_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }
  if (my_mkdir(m_mysql_archive_recovery_dir, my_umask_dir, MYF(0))) {
    err_msg.assign("Failed to create tmp recovery dir: ");
    err_msg.append(m_mysql_archive_recovery_dir);
    err_msg.append(" error=");
    err_msg.append(std::to_string(my_errno()));
    LogErr(WARNING_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }

  // create new tmp recovery snapshot dir.
  strmake(
      strmake(m_mysql_archive_recovery_data_dir, m_mysql_archive_recovery_dir,
              sizeof(m_mysql_archive_recovery_data_dir) -
                  CONSISTENT_ARCHIVE_SUBDIR_LEN - 1),
      STRING_WITH_LEN(CONSISTENT_ARCHIVE_SUBDIR));

  convert_dirname(m_mysql_archive_recovery_data_dir,
                  m_mysql_archive_recovery_data_dir, NullS);
  if (my_mkdir(m_mysql_archive_recovery_data_dir, my_umask_dir, MYF(0))) {
    err_msg.assign("Failed to create tmp recovery snapshot dir: ");
    err_msg.append(m_mysql_archive_recovery_data_dir);
    err_msg.append(" error=");
    err_msg.append(std::to_string(my_errno()));
    LogErr(WARNING_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }
  // create new recovery binlog dir.
  strmake(
      strmake(m_mysql_archive_recovery_binlog_dir, m_mysql_archive_recovery_dir,
              sizeof(m_mysql_archive_recovery_binlog_dir) -
                  BINLOG_ARCHIVE_SUBDIR_LEN - 1),
      STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  convert_dirname(m_mysql_archive_recovery_binlog_dir,
                  m_mysql_archive_recovery_binlog_dir, NullS);
  if (my_mkdir(m_mysql_archive_recovery_binlog_dir, my_umask_dir, MYF(0))) {
    err_msg.assign("Failed to create tmp recovery binlog dir: ");
    err_msg.append(m_mysql_archive_recovery_binlog_dir);
    err_msg.append(" error=");
    err_msg.append(std::to_string(my_errno()));
    LogErr(WARNING_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }

  Consistent_snapshot_recovery_status recovery_status{};
  // If recovery status file exists, the recovery process may be not completed.
  // Need check the recovery status file.
  // If recovery status is STAGE_DATA_READY, set global variables.
  if ((read_consistent_snapshot_recovery_status(recovery_status) == 0) &&
      (recovery_status.m_recovery_status >=
       CONSISTENT_SNAPSHOT_RECOVERY_STAGE_DATA_READY)) {
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Recovery data exists already, skip pull data from object store.");
    m_state = CONSISTENT_RECOVERY_STATE_END;
    // For consensus archive recovery.
    consistent_recovery_consensus_recovery = true;
    consistent_recovery_snapshot_end_binlog_position =
        recovery_status.m_end_binlog_pos;
    consistent_recovery_snasphot_end_consensus_index =
        recovery_status.m_end_consensus_index;
    strncpy(consistent_recovery_apply_stop_timestamp,
            recovery_status.m_apply_stop_timestamp,
            sizeof(consistent_recovery_apply_stop_timestamp) - 1);
    return 0;
  }

  memset(&recovery_status, 0, sizeof(recovery_status));
  recovery_status.m_recovery_status = CONSISTENT_SNAPSHOT_RECOVERY_STAGE_BEGIN;
  if (write_consistent_snapshot_recovery_status(recovery_status)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to write snapshot recovery status.");
    return 1;
  }

  m_state = CONSISTENT_RECOVERY_STATE_NONE;
  // read consistent snapshot file from object store.
  if (read_consistent_snapshot_file()) return 1;
  // recovery mysql innodb to mysql data dir
  if (flags & CONSISTENT_RECOVERY_INNODB)
    if (recovery_mysql_innodb()) return 1;
  // recovery smartengine to smartengine data dir
  if (flags & CONSISTENT_RECOVERY_SMARTENGINE)
    if (recovery_smartengine()) return 1;
  // recovery binlog to mysql
  if (flags & CONSISTENT_RECOVERY_BINLOG)
    if (recovery_binlog(opt_binlog_index_name, nullptr)) return 1;
  if (flags & CONSISTENT_RECOVERY_SMARTENGINE_EXTENT)
    if (recovery_smartengine_objectstore_data()) return 1;
  return 0;
}

/**
 * @brief Fetch consistent snapshot information from object store.
 *
 * @return true
 * @return false
 */
bool Consistent_recovery::read_consistent_snapshot_file() {
  DBUG_TRACE;
  ulong recovery_ts = 0;
  assert (m_state == CONSISTENT_RECOVERY_STATE_NONE);

  // If opt_recovery_consistent_snapshot_timestamp is set, PITR instance.
  if (opt_initialize && opt_recovery_consistent_snapshot_timestamp) {
    if (convert_str_to_datetime(opt_recovery_consistent_snapshot_timestamp,
                                recovery_ts)) {
      std::string err_msg;
      err_msg.assign(
          "Failed to convert recovery_consistent_snapshot_timestamp to "
          "seconds: ");
      err_msg.append(opt_recovery_consistent_snapshot_timestamp);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
  }

  std::string consistent_file_keyid;
  consistent_file_keyid.assign(m_consistent_snapshot_archive_dir);
  consistent_file_keyid.append(CONSISTENT_SNAPSHOT_INDEX_FILE);
  std::string consistent_file_name;
  consistent_file_name.assign(m_mysql_archive_recovery_dir);
  consistent_file_name.append(CONSISTENT_SNAPSHOT_INDEX_FILE);
  // Delete the local consistent snapshot file if it exists.
  remove_file(consistent_file_name);
  objstore::Status ss = recovery_objstore->get_object_to_file(
      std::string_view(m_objstore_bucket), consistent_file_keyid,
      consistent_file_name);
  if (!ss.is_succ()) {
    std::string err_msg;
    err_msg.assign("Failed to download snapshot index file: ");
    err_msg.append(" key=");
    err_msg.append(consistent_file_keyid);
    err_msg.append(" file=");
    err_msg.append(consistent_file_name);
    err_msg.append(" error=");
    err_msg.append(ss.error_message());
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  if (my_chmod(consistent_file_name.c_str(),
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
               MYF(MY_WME))) {
    std::string err_msg;
    err_msg.assign("Failed to chmod snapshot index file: ");
    err_msg.append(consistent_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  std::ifstream consistent_file;
  consistent_file.open(consistent_file_name, std::ifstream::in);
  if (!consistent_file.is_open()) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to open snapshot index file");
    return true;
  }

  std::string file_line;
  bool found = false;
  // Find the snapshot whose timestamp is smaller than or equal to the
  // specified recovery timestamp and is the closest to this recovery
  // timestamp.
  while (std::getline(consistent_file, file_line)) {
    char snapshot_info[4 * FN_REFLEN] = {0};
    strmake(snapshot_info, file_line.c_str(), sizeof(snapshot_info) - 1);
    std::string in_str;
    in_str.assign(snapshot_info);
    size_t idx = in_str.find("|");
    std::string ts = in_str.substr(0, idx);
    std::string left_string = in_str.substr(idx + 1);

    idx = left_string.find("|");
    std::string innodb_name = left_string.substr(0, idx);
    left_string = left_string.substr(idx + 1);

    idx = left_string.find("|");
    std::string se_name = left_string.substr(0, idx);
    left_string = left_string.substr(idx + 1);

    idx = left_string.find("|");
    std::string binlog_name = left_string.substr(0, idx);
    left_string = left_string.substr(idx + 1);

    idx = left_string.find("|");
    std::string binlog_pos = left_string.substr(0, idx);
    left_string = left_string.substr(idx + 1);

    idx = left_string.find("|");
    std::string consensus_index = left_string.substr(0, idx);
    std::string se_snapshot_id = left_string.substr(idx + 1);

    ulong snapshot_ts = 0;
    if (convert_str_to_datetime(ts.c_str(), snapshot_ts)) {
      std::string err_msg;
      err_msg.assign(
          "Failed to convert snapshot timestamp to "
          "seconds: ");
      err_msg.append(opt_recovery_consistent_snapshot_timestamp);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    // set snapshot recovery end binlog file.
    // recovery binlog between recovery snapshot and next snapshot.
    // If recovery_ts==0, recover to latest consistent snapshot
    if (recovery_ts > 0 && snapshot_ts > recovery_ts) {
      strncpy(m_snapshot_end_binlog_file, binlog_name.c_str(),
              sizeof(m_snapshot_end_binlog_file) - 1);
      break;
    }

    strncpy(m_consistent_snapshot_local_time, ts.c_str(),
            sizeof(m_consistent_snapshot_local_time) - 1);
    strncpy(m_mysql_clone_keyid, innodb_name.c_str(),
            sizeof(m_mysql_clone_keyid) - 1);
    strncpy(m_se_backup_keyid, se_name.c_str(), sizeof(m_se_backup_keyid) - 1);
    strncpy(m_binlog_file, binlog_name.c_str(), sizeof(m_binlog_file) - 1);
    m_mysql_binlog_pos = std::stoull(binlog_pos);
    m_consensus_index = std::stoull(consensus_index);
    m_se_snapshot_id = std::stoull(se_snapshot_id);
    found = true;
  }
  consistent_file.close();
  if (!found) {
    std::string err_msg;
    err_msg.assign("Failed to find snapshot file: ");
    err_msg.append(
        "The set recovery timestamp is too small, and no snapshot meets "
        "the requirements ");
    if (opt_recovery_consistent_snapshot_timestamp) {
      err_msg.append(opt_recovery_consistent_snapshot_timestamp);
    }
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  std::string err_msg;
  err_msg.assign("Recovery snapshot: ");
  err_msg.append(m_consistent_snapshot_local_time);
  err_msg.append(" ");
  err_msg.append(m_mysql_clone_keyid);
  err_msg.append(" ");
  err_msg.append(m_se_backup_keyid);
  err_msg.append(" ");
  err_msg.append(m_binlog_file);
  err_msg.append(" ");
  err_msg.append(std::to_string(m_mysql_binlog_pos));
  err_msg.append(" ");
  err_msg.append(std::to_string(m_consensus_index));
  err_msg.append(" ");
  err_msg.append(std::to_string(m_se_snapshot_id));
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  // Keep consistent snapshot file for diagnosis, until the next recovery.
  // remove_file(consistent_file_name);
  m_state = CONSISTENT_RECOVERY_STATE_SNAPSHOT_FILE;
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
         "recover persistent snapshot index file finish.");
  return false;
}

/**
 * @brief Read m_mysql_clone_keyid to temp archive dir from object store.
 *        Then cp archived mysql innodb file to mysql data dir.
 *
 * @return true
 * @return false
 */
bool Consistent_recovery::recovery_mysql_innodb() {
  std::string err_msg;
  std::string innodb_log_group_home{};
  // no recovery
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE ||
      m_state == CONSISTENT_RECOVERY_STATE_END)
    return false;

  String innodb_log_dir{};
  auto f = [&innodb_log_dir](const System_variable_tracker &, sys_var *var) {
    char val_buf[1024];
    SHOW_VAR show;
    show.type = SHOW_SYS;
    show.value = pointer_cast<char *>(var);
    show.name = var->name.str;

    mysql_mutex_lock(&LOCK_global_system_variables);
    size_t val_length;
    const char *value =
        get_one_variable(nullptr, &show, OPT_GLOBAL, SHOW_SYS, nullptr, nullptr,
                         val_buf, &val_length);
    if (val_length != 0) {
      innodb_log_dir.copy(value, val_length, nullptr);
    }
    mysql_mutex_unlock(&LOCK_global_system_variables);
  };

  System_variable_tracker sv =
      System_variable_tracker::make_tracker("INNODB_LOG_GROUP_HOME_DIR");
  mysql_mutex_lock(&LOCK_plugin);
  if (sv.access_system_variable(nullptr, f)) {
    mysql_mutex_unlock(&LOCK_plugin);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to get smartengine_data_dir parameter");
    return true;
  }

  if (!test_if_hard_path(innodb_log_dir.c_ptr())) {
    if (strcmp(innodb_log_dir.c_ptr(), "./") != 0) {
      char redo_path[FN_REFLEN + 1] = {0};
      (void)fn_format(redo_path, innodb_log_dir.c_ptr(), mysql_real_data_home,
                      "", MY_REPLACE_DIR | MY_UNPACK_FILENAME);
      innodb_log_group_home.assign(redo_path);
    }
  } else {
    innodb_log_group_home.assign(innodb_log_dir.c_ptr());
  }
  mysql_mutex_unlock(&LOCK_plugin);

  err_msg.assign("recover persistent innodb objects ");
  err_msg.append(m_mysql_clone_keyid);
  err_msg.append(" to mysql datadir=");
  err_msg.append(mysql_real_data_home);
  err_msg.append(" innodb_log_group_home=");
  err_msg.append(innodb_log_group_home);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());

  std::string mysql_clone_basename;
  mysql_clone_basename.assign(m_mysql_clone_keyid +
                              dirname_length(m_mysql_clone_keyid));
  size_t idx = mysql_clone_basename.find(".");
  // Check if with ".tar" or ".tar.gz" extension.
  if (idx == std::string::npos) {  // innodb_archive_000001/
    // Directly recover to mysql data directory from object store.
    err_msg.assign("download persistent innodb objects: ");
    err_msg.append(" key=");
    err_msg.append(m_mysql_clone_keyid);
    err_msg.append(" to mysql datadir=");
    err_msg.append(mysql_real_data_home);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    objstore::Status ss = recovery_objstore->get_objects_to_dir(
        std::string_view(m_objstore_bucket), m_mysql_clone_keyid,
        mysql_real_data_home);
    if (!ss.is_succ()) {
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "download persistent innodb objects finish.");
    if (recursive_chmod(mysql_real_data_home)) {
      err_msg.assign("chmod failed: ");
      err_msg.append(mysql_real_data_home);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    // move '#innodb_redo' to innodb_log_group_home
    err_msg.assign("move innodb redo log to innodb_log_group_home: ");
    err_msg.append(innodb_log_group_home);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    if (innodb_log_group_home.length() > 0) {
      std::string innodb_redo_dir_src;
      std::string innodb_redo_dir_dest;
      innodb_redo_dir_src.assign(mysql_real_data_home);
      innodb_redo_dir_src.append(CONSISTENT_INNODB_ARCHIVE_REDO_SUBDIR);

      char redo_path[FN_REFLEN + 1] = {0};
      strmake(redo_path, innodb_log_group_home.c_str(), sizeof(redo_path) - 1);
      convert_dirname(redo_path, redo_path, NullS);
      innodb_redo_dir_dest.assign(redo_path);
      innodb_redo_dir_dest.append(CONSISTENT_INNODB_ARCHIVE_REDO_SUBDIR);
      if (copy_directory(innodb_redo_dir_src, innodb_redo_dir_dest)) {
        err_msg.assign("copy innodb redo log failed: ");
        err_msg.append(innodb_redo_dir_src);
        err_msg.append(" to ");
        err_msg.append(innodb_redo_dir_dest);
        err_msg.append(" errno=");
        err_msg.append(std::to_string(my_errno()));
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        return true;
      }
      if (recursive_chmod(innodb_redo_dir_dest)) {
        err_msg.assign("chmod failed: ");
        err_msg.append(innodb_redo_dir_dest);
        err_msg.append(" errno=");
        err_msg.append(std::to_string(my_errno()));
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        return true;
      }
      if (remove_file(innodb_redo_dir_src)) {
        err_msg.assign("remove innodb redo log failed: ");
        err_msg.append(innodb_redo_dir_src);
        err_msg.append(" errno=");
        err_msg.append(std::to_string(my_errno()));
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        return true;
      }
    }
  } else {
    std::string clone_name_ext =
        mysql_clone_basename.substr(idx);  // .tar or .tar.gz
    // innodb_archive_000001.tar.gz --> innodb_archive_000001
    mysql_clone_basename = mysql_clone_basename.substr(0, idx);
    std::string mysql_clone_file_name;
    mysql_clone_file_name.assign(m_mysql_archive_recovery_data_dir);
    mysql_clone_file_name.append(m_mysql_clone_keyid +
                                 dirname_length(m_mysql_clone_keyid));
    // delete the local clone data if it exists.
    remove_file(mysql_clone_file_name);
    // download m_mysql_clone_keyid from object store
    err_msg.assign("download innodb objects: ");
    err_msg.append(" key=");
    err_msg.append(m_mysql_clone_keyid);
    err_msg.append(" file=");
    err_msg.append(mysql_clone_file_name);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    objstore::Status ss = recovery_objstore->get_object_to_file(
        std::string_view(m_objstore_bucket), m_mysql_clone_keyid,
        mysql_clone_file_name);
    if (!ss.is_succ()) {
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "download persistent innodb objects finish.");
    if (my_chmod(
            mysql_clone_file_name.c_str(),
            USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
            MYF(MY_WME))) {
      err_msg.assign("chmod failed: ");
      err_msg.append(mysql_clone_file_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      // remove_file(mysql_clone_file_name);
      return true;
    }

    // tar -xvzf /u01/mysql_archive/clone000034.tar
    std::stringstream cmd;
    std::string clone_untar_name;
    int err = 0;
    clone_untar_name.assign(m_mysql_archive_recovery_data_dir);
    clone_untar_name.append(mysql_clone_basename);
    // Delete local expired clone data dir if it exists.
    remove_file(clone_untar_name);
    if (strcmp(clone_name_ext.c_str(), CONSISTENT_TAR_GZ_SUFFIX) == 0) {
      cmd << "tar -xzf " << mysql_clone_file_name.c_str() << " -C "
          << m_mysql_archive_recovery_data_dir;
    } else {
      assert(strcmp(clone_name_ext.c_str(), CONSISTENT_TAR_SUFFIX) == 0);
      cmd << "tar -xf " << mysql_clone_file_name.c_str() << " -C "
          << m_mysql_archive_recovery_data_dir;
    }
    err_msg.assign("untar innodb objects: ");
    err_msg.append(cmd.str());
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    if ((err = system(cmd.str().c_str())) != 0) {
      err_msg.append(" error=");
      err_msg.append(std::to_string(err));
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      // mysql_archive/clone000034.tar
      remove_file(mysql_clone_file_name);
      return true;
    }
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "untar innodb objects finish.");

    // move '#innodb_redo' to innodb_log_group_home
    err_msg.assign("move innodb redo log to innodb_log_group_home: ");
    err_msg.append(innodb_log_group_home);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    if (innodb_log_group_home.length() > 0) {
      std::string innodb_redo_dir_src;
      std::string innodb_redo_dir_dest;
      innodb_redo_dir_src.assign(clone_untar_name);
      innodb_redo_dir_src.append(CONSISTENT_INNODB_ARCHIVE_REDO_SUBDIR);

      char redo_path[FN_REFLEN + 1] = {0};
      strmake(redo_path, innodb_log_group_home.c_str(), sizeof(redo_path) - 1);
      convert_dirname(redo_path, redo_path, NullS);
      innodb_redo_dir_dest.assign(redo_path);
      innodb_redo_dir_dest.append(CONSISTENT_INNODB_ARCHIVE_REDO_SUBDIR);
      if (copy_directory(innodb_redo_dir_src, innodb_redo_dir_dest)) {
        err_msg.assign("copy innodb redo log failed: ");
        err_msg.append(innodb_redo_dir_src);
        err_msg.append(" to ");
        err_msg.append(innodb_redo_dir_dest);
        err_msg.append(" errno=");
        err_msg.append(std::to_string(my_errno()));
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        // remove mysql_archive/clone000034
        remove_file(clone_untar_name);
        // mysql_archive/clone000034.tar
        remove_file(mysql_clone_file_name);
        return true;
        return true;
      }
      if (recursive_chmod(innodb_redo_dir_dest)) {
        err_msg.assign("chmod failed: ");
        err_msg.append(innodb_redo_dir_dest);
        err_msg.append(" errno=");
        err_msg.append(std::to_string(my_errno()));
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        // remove mysql_archive/clone000034
        remove_file(clone_untar_name);
        // mysql_archive/clone000034.tar
        remove_file(mysql_clone_file_name);
        return true;
        return true;
      }
      if (remove_file(innodb_redo_dir_src)) {
        err_msg.assign("remove innodb redo log failed: ");
        err_msg.append(innodb_redo_dir_src);
        err_msg.append(" errno=");
        err_msg.append(std::to_string(my_errno()));
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        // remove mysql_archive/clone000034
        remove_file(clone_untar_name);
        // mysql_archive/clone000034.tar
        remove_file(mysql_clone_file_name);
        return true;
        return true;
      }
    }

    // cp -r /u01/mysql_archive/clone000034 mysql_real_data_home
    // scan the clone dir and copy the files and sub dir to mysql data dir
    err_msg.assign("copy innodb objects: ");
    err_msg.append(clone_untar_name);
    err_msg.append(" to mysql datadir ");
    err_msg.append(mysql_real_data_home);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    if (copy_directory(clone_untar_name, mysql_real_data_home)) {
      err_msg.append(" failed");
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      // remove mysql_archive/clone000034
      remove_file(clone_untar_name);
      // mysql_archive/clone000034.tar
      remove_file(mysql_clone_file_name);
      return true;
    }
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "copy innodb objects finish.");
    // remove mysql_archive/clone000034
    remove_file(clone_untar_name);
    // mysql_archive/clone000034.tar
    remove_file(mysql_clone_file_name);
  }
  m_state = CONSISTENT_RECOVERY_STATE_MYSQL_INNODB;
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
         "recover persistent innodb objects finish.");
  return false;
}

// read m_se_backup_keyid to temp archive dir from object store.
// smartengine plugin is loaded.
bool Consistent_recovery::recovery_smartengine() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("recovery_smartengine"));
  std::string smartengine_real_data_home;
  std::string smartengine_real_wal_home;
  std::string err_msg;
  // no recovery process
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE ||
      m_state == CONSISTENT_RECOVERY_STATE_END)
    return false;

  /* Check if the smartengine plugin is loaded. */
  plugin_ref tmp_plugin = ha_resolve_by_name_raw(
      nullptr, LEX_CSTRING{STRING_WITH_LEN("smartengine")});
  if (tmp_plugin == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "smartengine plugin is not loaded");
    return true;
  }
  plugin_unlock(nullptr, tmp_plugin);

  // Retrive smartengine option --smartengine-datadir
  String se_data_dir{};
  auto f = [&se_data_dir](const System_variable_tracker &, sys_var *var) {
    char val_buf[1024];
    SHOW_VAR show;
    show.type = SHOW_SYS;
    show.value = pointer_cast<char *>(var);
    show.name = var->name.str;

    mysql_mutex_lock(&LOCK_global_system_variables);
    size_t val_length;
    const char *value =
        get_one_variable(nullptr, &show, OPT_GLOBAL, SHOW_SYS, nullptr, nullptr,
                         val_buf, &val_length);
    if (val_length != 0) {
      se_data_dir.copy(value, val_length, nullptr);
    }
    mysql_mutex_unlock(&LOCK_global_system_variables);
  };

  System_variable_tracker sv =
      System_variable_tracker::make_tracker("SMARTENGINE_DATA_DIR");
  mysql_mutex_lock(&LOCK_plugin);
  if (sv.access_system_variable(nullptr, f)) {
    mysql_mutex_unlock(&LOCK_plugin);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to get smartengine_data_dir parameter");
    return true;
  }
  smartengine_real_data_home.assign(se_data_dir.c_ptr());
  sv = System_variable_tracker::make_tracker("SMARTENGINE_WAL_DIR");
  if (sv.access_system_variable(nullptr, f)) {
    mysql_mutex_unlock(&LOCK_plugin);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to get smartengine_wal_dir parameter");
    return true;
  }
  smartengine_real_wal_home.assign(se_data_dir.c_ptr());
  if (smartengine_real_wal_home.length() == 0) {
    smartengine_real_wal_home.assign(smartengine_real_data_home);
  }
  mysql_mutex_unlock(&LOCK_plugin);

  err_msg.assign("recover persistent smartengine wals and meta ");
  err_msg.append(m_se_backup_keyid);
  err_msg.append(" to smartengine datadir=");
  err_msg.append(smartengine_real_data_home);
  err_msg.append(" waldir=");
  err_msg.append(smartengine_real_wal_home);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());

  std::string se_backup_basename;
  se_backup_basename.assign(m_se_backup_keyid +
                            dirname_length(m_se_backup_keyid));
  size_t idx = se_backup_basename.find(".");
  if (idx == std::string::npos) {  // se_archive_000001/
    // Directly recover se_archive_000001/ to smartengine data directory.
    // download smartengine wal
    std::string se_wal;
    se_wal.assign(m_se_backup_keyid);
    se_wal.append(CONSISTENT_SE_ARCHIVE_WAL_SUBDIR);
    se_wal.append(FN_DIRSEP);
    err_msg.assign("download persistent smartengine wal dir: ");
    err_msg.append(" key=");
    err_msg.append(se_wal);
    err_msg.append(" smartengine wal dir=");
    err_msg.append(smartengine_real_wal_home);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    objstore::Status ss = recovery_objstore->get_objects_to_dir(
        std::string_view(m_objstore_bucket), se_wal, smartengine_real_wal_home);
    if (!ss.is_succ()) {
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    if (recursive_chmod(smartengine_real_wal_home)) {
      err_msg.assign("chmod failed: ");
      err_msg.append(smartengine_real_wal_home);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }

    std::string se_meta;
    se_meta.assign(m_se_backup_keyid);
    se_meta.append(CONSISTENT_SE_ARCHIVE_META_SUBDIR);
    se_meta.append(FN_DIRSEP);
    err_msg.assign("download persistent smartengine meta dir: ");
    err_msg.append(" key=");
    err_msg.append(se_meta);
    err_msg.append(" smartengine datadir=");
    err_msg.append(smartengine_real_data_home);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    ss = recovery_objstore->get_objects_to_dir(
        std::string_view(m_objstore_bucket), se_meta,
        smartengine_real_data_home);
    if (!ss.is_succ()) {
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    if (recursive_chmod(smartengine_real_data_home)) {
      err_msg.assign("chmod failed: ");
      err_msg.append(smartengine_real_data_home);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
  } else {  // se_archive_000001.tar or se_archive_000001.tar.gz
    std::string se_file_name;
    se_file_name.assign(m_mysql_archive_recovery_data_dir);
    se_file_name.append(m_se_backup_keyid + dirname_length(m_se_backup_keyid));
    std::string backup_name_ext = se_backup_basename.substr(idx);
    se_backup_basename = se_backup_basename.substr(0, idx);
    // delete the local expired data if it exists.
    remove_file(se_file_name);
    // download m_se_backup_keyid from object store
    err_msg.assign("download smartengine archive file: ");
    err_msg.append(" key=");
    err_msg.append(m_se_backup_keyid);
    err_msg.append(" file=");
    err_msg.append(se_file_name);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    objstore::Status ss = recovery_objstore->get_object_to_file(
        std::string_view(m_objstore_bucket), m_se_backup_keyid, se_file_name);
    if (!ss.is_succ()) {
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }

    if (my_chmod(
            se_file_name.c_str(),
            USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
            MYF(MY_WME))) {
      err_msg.assign("chmod failed: ");
      err_msg.append(se_file_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }

    // tar -xvzf /u01/mysql_archive/se_backup000034.tar
    int err = 0;
    std::stringstream cmd;
    std::string se_backup_untar_name;
    se_backup_untar_name.assign(m_mysql_archive_recovery_data_dir);
    se_backup_untar_name.append(se_backup_basename);
    // Delete local expired data if it exists.
    remove_file(se_backup_untar_name);
    if (strcmp(backup_name_ext.c_str(), CONSISTENT_TAR_GZ_SUFFIX) == 0) {
      cmd << "tar -xzf " << se_file_name << " -C "
          << m_mysql_archive_recovery_data_dir;
    } else {
      assert(strcmp(backup_name_ext.c_str(), CONSISTENT_TAR_SUFFIX) == 0);
      cmd << "tar -xf " << se_file_name << " -C "
          << m_mysql_archive_recovery_data_dir;
    }
    err_msg.assign("untar smartengine archive file: ");
    err_msg.append(cmd.str());
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    if ((err = system(cmd.str().c_str())) != 0) {
      err_msg.append(" error=");
      err_msg.append(std::to_string(err));
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      // mysql_archive/se_backup000034.tar
      remove_file(se_file_name);
      return true;
    }
    // cp -r /u01/mysql_archive/se_backup000034 SMARTENGINE_DATA_DIR
    // scan the se backup dir and copy the files and sub dir to smartengine data
    // dir
    std::string se_meta;
    se_meta.assign(se_backup_untar_name);
    se_meta.append(FN_DIRSEP);
    se_meta.append(CONSISTENT_SE_ARCHIVE_META_SUBDIR);
    se_meta.append(FN_DIRSEP);
    err_msg.assign("copy smartengine meta file: ");
    err_msg.append(se_meta);
    err_msg.append(" to smartengine datadir=");
    err_msg.append(smartengine_real_data_home);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    if (copy_directory(se_meta, smartengine_real_data_home)) {
      err_msg.append(" failed");
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      // remove mysql_archive/se_backup000034
      remove_file(se_backup_untar_name);
      // mysql_archive/se_backup000034.tar
      remove_file(se_file_name);
      return true;
    }
    std::string se_wal;
    se_wal.assign(se_backup_untar_name);
    se_wal.append(FN_DIRSEP);
    se_wal.append(CONSISTENT_SE_ARCHIVE_WAL_SUBDIR);
    se_wal.append(FN_DIRSEP);
    err_msg.assign("copy smartengine wal file: ");
    err_msg.append(se_wal);
    err_msg.append(" to smartengine waldir=");
    err_msg.append(smartengine_real_wal_home);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    if (copy_directory(se_wal, smartengine_real_wal_home)) {
      err_msg.append(" failed");
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      // remove mysql_archive/se_backup000034
      remove_file(se_backup_untar_name);
      // mysql_archive/se_backup000034.tar
      remove_file(se_file_name);
      return true;
    }

    // remove mysql_archive/se_backup000034
    remove_file(se_backup_untar_name);
    // mysql_archive/se_backup000034.tar
    remove_file(se_file_name);
  }
  m_state = CONSISTENT_RECOVERY_STATE_SE;
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
         "recover persistent smartengine wals and meta finish.");
  return false;
}

// TODO: Initialize the smartengine extents from object store.
// If --table_on_objstore is set, the extents will be copy from
// recovery_objectstore_bucket to objstore_bucket. Otherwise, the extents will
// be download from recovery_objectstore_bucket to mysql_real_data_home.

/**
 * @brief Recovery binlog and index file from object store.
 * @note If opt_bin_log is not set, recover_binlog() will be not called.
 * After ha_recover in open_binlog, must rotate new binlog file.
 * @return true
 * @return false
 */
bool Consistent_recovery::recovery_binlog(const char *binlog_index_name
                                          [[maybe_unused]],
                                          const char *bin_log_name
                                          [[maybe_unused]]) {
  LOG_ARCHIVED_INFO log_info;
  std::string err_msg = {};
  // if not need recovery binlog, return false
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE ||
      m_state == CONSISTENT_RECOVERY_STATE_END)
    return false;
  err_msg.assign("recovery binlog from ");
  err_msg.append(m_binlog_file);
  err_msg.append(":");
  err_msg.append(std::to_string(m_mysql_binlog_pos));
  err_msg.append(" consensus_index=");
  err_msg.append(std::to_string(m_consensus_index));
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());

  // Get mysql binlog basename and binlog index basename.
  // "The basename of the persisted binlog is always 'binlog',  whereas the
  // basename of MySQL's binlog is set through the `--log-bin` variable."
  // But the extension of the persistent binlog files and mysql binlog remains
  // consistent.
  // For example: persistent binlog.000001 <-----> mysql master-bin.000001
  char mysql_binlog_basename[FN_REFLEN];
  char mysql_binlog_index_basename[FN_REFLEN];

  if (!opt_bin_logname) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, "need enable binlog mode");
    return true;
  }

  // ref: mysql_bin_log.generate_new_name()
  fn_format(mysql_binlog_basename, opt_bin_logname, mysql_data_home, "",
            MY_UNPACK_FILENAME);
  myf opt = MY_UNPACK_FILENAME;
  // ref: mysql_bin_log.open_index_file()
  if (!binlog_index_name) {
    binlog_index_name = bin_log_name;  // Use same basename for index file
    opt = MY_UNPACK_FILENAME | MY_REPLACE_EXT;
  }
  fn_format(mysql_binlog_index_basename, binlog_index_name, mysql_data_home,
            ".index", opt);

  // Download binlog.index from object store to recover tmp dir.
  // And open objstore binlog index file.
  std::string binlog_index_file_name;
  binlog_index_file_name.assign(m_mysql_archive_recovery_binlog_dir);
  binlog_index_file_name.append(BINLOG_ARCHIVE_INDEX_LOCAL_FILE);
  strmake(m_binlog_index_file_name, binlog_index_file_name.c_str(),
          sizeof(m_binlog_index_file_name) - 1);
  std::string last_index_keyid;
  // The persisted `binlog-index.index` is based on multiple versions of the
  // consensus term, so the `binlog-index.index` with the highest consensus term
  // should be obtained as the initial one.
  {
    std::vector<objstore::ObjectMeta> objects;
    bool finished = false;
    std::string start_after;
    std::string binlog_index_prefix;
    binlog_index_prefix.assign(m_binlog_archive_dir);
    binlog_index_prefix.append(BINLOG_ARCHIVE_INDEX_FILE_BASENAME);
    do {
      std::vector<objstore::ObjectMeta> tmp_objects;
      objstore::Status ss = recovery_objstore->list_object(
          std::string_view(m_objstore_bucket), binlog_index_prefix, false,
          start_after, finished, tmp_objects);
      if (!ss.is_succ()) {
        err_msg.assign("List persistent binlog index files failed: ");
        err_msg.append(BINLOG_ARCHIVE_INDEX_FILE_BASENAME);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        return true;
      }
      objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
    } while (finished == false);
    // if no persistent binlog.index, not recovery binlog and return.
    if (objects.empty()) {
      m_state = CONSISTENT_RECOVERY_STATE_BINLOG;
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "no persistent binlog.index");
      return false;
    }
    last_index_keyid.assign((objects.back()).key);
    err_msg.assign("the last persistent binlog index is ");
    err_msg.append(last_index_keyid);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    strmake(m_binlog_index_keyid, last_index_keyid.c_str(),
            sizeof(m_binlog_index_keyid) - 1);
  }
  remove_file(m_binlog_index_file_name);
  {
    objstore::Status ss = recovery_objstore->get_object_to_file(
        std::string_view(m_objstore_bucket), last_index_keyid,
        std::string_view(m_binlog_index_file_name));
    if (!ss.is_succ()) {
      err_msg.assign("download persistent binlog index file failed: ");
      err_msg.append(last_index_keyid);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
  }

  if (my_chmod(m_binlog_index_file_name,
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
               MYF(0))) {
    err_msg.assign("Failed to chmod: ");
    err_msg.append(m_binlog_index_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  if (open_binlog_index_file(&m_binlog_index_file, m_binlog_index_file_name,
                             READ_CACHE)) {
    err_msg.assign("Failed to open persisted binlog index file: ");
    err_msg.append(m_binlog_index_file_name);
    err_msg.append(" errno=");
    err_msg.append(std::to_string(my_errno()));
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  IO_CACHE mysql_binlog_index_file;
  // Create mysql binlog index file and open.
  strmake(m_mysql_binlog_index_file_name, mysql_binlog_index_basename,
          sizeof(m_mysql_binlog_index_file_name) - 1);
  if (open_binlog_index_file(&mysql_binlog_index_file,
                             m_mysql_binlog_index_file_name, WRITE_CACHE)) {
    err_msg.assign("Failed to create mysql binlog index file: ");
    err_msg.append(m_mysql_binlog_index_file_name);
    err_msg.append(" errno=");
    err_msg.append(std::to_string(my_errno()));
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    close_binlog_index_file(&m_binlog_index_file);
    return true;
  }

  // Check if m_binlog_file exists in objstore's binlog.index
  // Starting from m_binlog_file.
  // If m_binlog_file[0] == '\0', indicate that all persistent binlog is
  // required to be recovered. m_binlog_file is from consistent snapshot.
  int error = 0;
  error = Binlog_archive::find_log_pos_common(&m_binlog_index_file, &log_info,
                                              m_binlog_file, 0);
  if (error != 0) {
    // Persistent binlog is empty.
    if (error == LOG_INFO_EOF) {
      m_state = CONSISTENT_RECOVERY_STATE_BINLOG;
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "no persistent binlog file");
      return false;
    }
    err_msg.assign("Failed to find persistent binlog file: ");
    err_msg.append(m_binlog_file);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    close_binlog_index_file(&mysql_binlog_index_file);
    close_binlog_index_file(&m_binlog_index_file);
    return true;
  }

  // All binlogs after m_binlog_file on S3 should be downloaded locally
  // for recovery, as the binlogs persisted to S3 are already committed.
  // Otherwise, these binlogs still need to be synchronized from the logger node
  // for recovery.
  do {
    // Convert to mysql binlog full name using persistent binlog name.
    char mysql_binlog_full_name[FN_REFLEN] = {0};
    std::string mysql_binlog_name;
    mysql_binlog_name.assign(mysql_binlog_basename);
    // persistent binlog.000012, extension is '.000012'
    const char *ext = fn_ext(log_info.log_file_name);
    // generate mysql binlog my-bin.000012 using persistent binlog.000012
    // extenstion.
    mysql_binlog_name.append(ext);
    fn_format(mysql_binlog_full_name, mysql_binlog_name.c_str(),
              mysql_data_home, "", 4);

    // append mysql binlog full name to mysql binlog index.
    if ((add_line_to_index_file(&mysql_binlog_index_file,
                                (char *)mysql_binlog_full_name))) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, "Failed to add index");
      close_binlog_index_file(&mysql_binlog_index_file);
      close_binlog_index_file(&m_binlog_index_file);
      return true;
    }
    // merge persistent binlog slice to mysql binlog file.
    if (merge_slice_to_binlog_file(log_info.log_file_name,
                                   mysql_binlog_name.c_str(),
                                   m_mysql_binlog_end_pos)) {
      err_msg.assign("Failed to merge binlog slice ");
      err_msg.append(log_info.log_file_name);
      err_msg.append(" to mysql binlog ");
      err_msg.append(mysql_binlog_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      close_binlog_index_file(&mysql_binlog_index_file);
      close_binlog_index_file(&m_binlog_index_file);
      return true;
    }
    // Using mysql binlog full name.
    strmake(m_mysql_binlog_end_file, mysql_binlog_full_name,
            sizeof(m_mysql_binlog_end_file) - 1);

    // Clear LOG_EVENT_BINLOG_IN_USE_F for binlog file.
    if (!update_log_file_set_flag_in_use(m_mysql_binlog_end_file, false)) {
      err_msg.assign("Failed to set last msyql binlog in_use flag: ");
      err_msg.append(m_mysql_binlog_end_file);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }

    // End binlog file download.
    // If opt_recovery_consistent_snapshot_only is set, only download the
    // binlog file specified by snapshot.
    if (opt_recovery_consistent_snapshot_only ||
        (m_snapshot_end_binlog_file[0] != '\0' &&
         strcmp(m_snapshot_end_binlog_file, mysql_binlog_full_name) == 0)) {
      break;
    }
  } while (0 == Binlog_archive::find_next_log_common(&m_binlog_index_file,
                                                     &log_info));
  close_binlog_index_file(&mysql_binlog_index_file);
  close_binlog_index_file(&m_binlog_index_file);
  remove_file(m_binlog_index_file_name);

  /*
  // init database from consistent snapshot.
  // truncate binlog file to m_mysql_binlog_pos.
  if (opt_initialize) {
    IO_CACHE_ostream file_ostream;
    std::string binlog_name;
    binlog_name.assign(m_mysql_archive_recovery_binlog_dir);
    binlog_name.append(binlog_relative_file);
    if (file_ostream.open(PSI_NOT_INSTRUMENTED, binlog_name.c_str(), MYF(0))) {
      err_msg.assign("Failed to open binlog file: ");
      err_msg.append(binlog_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    if (file_ostream.truncate(m_mysql_binlog_pos)) {
      err_msg.assign("Failed to truncate binlog file: ");
      err_msg.append(binlog_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    file_ostream.close();
  }
  */

  m_state = CONSISTENT_RECOVERY_STATE_BINLOG;
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
         "persistent binlog recovery finish");
  return false;
}
/**
 * @brief Recovery smartengine object store data from soure objct store,
 * when initialize database.
 *
 * @return true
 * @return false
 */
bool Consistent_recovery::recovery_smartengine_objectstore_data() {
  if (!opt_initialize_smartengine_objectstore_data) return false;

  // no recovery process
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE ||
      m_state == CONSISTENT_RECOVERY_STATE_END)
    return false;

  // get source all smartengine extent objects.
  std::vector<objstore::ObjectMeta> objects;
  bool finished = false;
  std::string err_msg;
  std::string start_after{};
  std::string source_se_objecstore_prefix;
  source_se_objecstore_prefix.assign(opt_initialize_repo_objstore_id);
  source_se_objecstore_prefix.append(FN_DIRSEP);
  source_se_objecstore_prefix.append(opt_initialize_branch_objstore_id);
  source_se_objecstore_prefix.append(FN_DIRSEP);
  source_se_objecstore_prefix.append(
      CONSISTENT_RECOVERY_SMARTENGINE_OBJECTSTORE_ROOT_PATH);
  source_se_objecstore_prefix.append(FN_DIRSEP);

  err_msg.assign("recovery smartengine sst from source ");
  err_msg.append(source_se_objecstore_prefix);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());

  do {
    std::vector<objstore::ObjectMeta> tmp_objects;
    objstore::Status ss = recovery_objstore->list_object(
        std::string_view(opt_initialize_objstore_bucket),
        source_se_objecstore_prefix, true, start_after, finished, tmp_objects);
    if (!ss.is_succ()) {
      err_msg.assign("list persistent object files: ");
      err_msg.append(source_se_objecstore_prefix);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
  } while (finished == false);

  for (const auto &object : objects) {
    std::string data{};
    // get object from source object store.
    objstore::Status ss = recovery_objstore->get_object(
        opt_initialize_objstore_bucket, object.key, data);
    if (!ss.is_succ()) {
      err_msg.assign("get object from source object store failed: ");
      err_msg.append("key=");
      err_msg.append(object.key);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
    // skip smartengine lease_lock object.
    if (object.key.find("lease_lock") != std::string::npos) continue;
    // replace smartengine root path
    // suffix of the object key
    std::string object_key_suffix =
        object.key.substr(source_se_objecstore_prefix.length());
    std::string destination_object_key(m_smartengine_objstore_dir);
    destination_object_key.append(object_key_suffix);

    // push object to destination object store.
    ss = init_destination_objstore->put_object(opt_objstore_bucket,
                                               destination_object_key, data);
    if (!ss.is_succ()) {
      err_msg.assign("put object to destination object store failed: ");
      err_msg.append("key=");
      err_msg.append(destination_object_key);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return true;
    }
  }
  m_state = CONSISTENT_RECOVERY_STATE_SST;
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
         "recovery smartengine sst finish");
  return false;
}

/**
 * @brief Finish consistent snapshot recovery.
 *
 * @return true
 * @return false
 */
bool Consistent_recovery::recovery_consistent_snapshot_finish() {
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE ||
      m_state == CONSISTENT_RECOVERY_STATE_END)
    return false;
  Consistent_snapshot_recovery_status recovery_status;
  memset(&recovery_status, 0, sizeof(recovery_status));
  recovery_status.m_recovery_status =
      CONSISTENT_SNAPSHOT_RECOVERY_STAGE_DATA_READY;
  recovery_status.m_end_binlog_pos = m_mysql_binlog_pos;
  recovery_status.m_end_consensus_index = m_consensus_index;
  if (opt_recovery_consistent_snapshot_timestamp)
    strmake(recovery_status.m_apply_stop_timestamp,
            opt_recovery_consistent_snapshot_timestamp,
            sizeof(recovery_status.m_apply_stop_timestamp) - 1);
  if (write_consistent_snapshot_recovery_status(recovery_status)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to write snapshot recovery status.");
    return 1;
  }
  // Write global variables for consensus archive recovery.
  consistent_recovery_consensus_recovery = true;
  consistent_recovery_snapshot_end_binlog_position =
      recovery_status.m_end_binlog_pos;
  consistent_recovery_snasphot_end_consensus_index =
      recovery_status.m_end_consensus_index;
  strncpy(consistent_recovery_apply_stop_timestamp,
          recovery_status.m_apply_stop_timestamp,
          sizeof(consistent_recovery_apply_stop_timestamp) - 1);

  if (opt_recovery_consistent_snapshot_only) {
    std::string file_name;
    std::ofstream status_file;
    file_name.assign(mysql_real_data_home);
    file_name.append(CONSISTENT_SNAPSHOT_RECOVERY_FILE);
    std::remove(file_name.c_str());
  }
  m_state = CONSISTENT_RECOVERY_STATE_END;
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
         "recover persistent snapshot data finish.");
  return false;
}

/**
 * @brief If a binlog truncation occurs after consensus recovery and apply, the
 * already persisted binlog must also be truncated. Otherwise, it will result in
 * an inconsistency between the MySQL binlog and the persisted binlog, which is
 * invalid. The last truncated MySQL binlog file returned by consensus. The
 * final binlog may contain incomplete transactions that need to be truncated,
 * but with the current binlog archive design, incomplete transactions should
 * not occur.
 * @return int
 */
int Consistent_recovery::consistent_snapshot_consensus_recovery_finish() {
  std::string file_name;
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE) return 0;
  if (m_state == CONSISTENT_RECOVERY_STATE_END) {
    // After consensus recovery finish, truncate persistent binlog
    if (opt_recovery_consistent_snapshot_only) {
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "recovey snapshot only finish.");
    } else if (!opt_initialize) {
      if (compare_log_name(
              m_mysql_binlog_end_file,
              consistent_recovery_consensus_truncated_end_binlog) != 0 ||
          m_mysql_binlog_end_pos !=
              consistent_recovery_consensus_truncated_end_position) {
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
               "recovey snapshot binlog mismatch consensus recovery "
               "binlog.");
        return 1;
      } else {
        assert(compare_log_name(
                   m_mysql_binlog_end_file,
                   consistent_recovery_consensus_truncated_end_binlog) == 0);
        assert(m_mysql_binlog_end_pos ==
               consistent_recovery_consensus_truncated_end_position);
        /*
        std::string persistent_end_binlog;
        // Convert mysql binlog to persistent binlog.
        persistent_end_binlog.assign(BINLOG_ARCHIVE_BASENAME);
        const char *ext =
        fn_ext(consistent_recovery_consensus_truncated_end_binlog);
        persistent_end_binlog.append(ext);
        truncate_binlogs_from_objstore(
            persistent_end_binlog.c_str(),
            static_cast<my_off_t>(consistent_recovery_consensus_truncated_end_position));
        */
      }
    }
    convert_dirname(mysql_real_data_home, mysql_real_data_home, NullS);
    file_name.assign(mysql_real_data_home);
    file_name.append(CONSISTENT_SNAPSHOT_RECOVERY_FILE);
    remove_file(file_name);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "recover persistent snapshot wait raft recovery finish and delete "
           "#status_snapshot_recovery file");
    return 0;
  }
  return 1;
}

int Consistent_recovery::get_last_persistent_binlog_consensus_index() {
  DBUG_TRACE;
  char binlog_archive_dir[FN_REFLEN + 1] = {0};
  char mysql_archive_recovery_dir[FN_REFLEN + 1] = {0};
  char mysql_archive_recovery_binlog_dir[FN_REFLEN + 1] = {0};
  IO_CACHE binlog_index_file{};
  std::string binlog_index_file_name{};
  std::string last_index_keyid{};
  std::string err_msg{};
  int error = 0;
  LOG_ARCHIVED_INFO log_info{};

  if (!opt_serverless) return 0;

#ifdef WESQL_CLUSTER
  if (!is_consensus_replication_log_mode()) return 0;
#endif
  objstore::init_objstore_provider(opt_objstore_provider);
  std::string_view endpoint(
      opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");
  std::string obj_error_msg;
  objstore::ObjectStore *objstore =
      objstore::create_object_store(std::string_view(opt_objstore_provider),
                                    std::string_view(opt_objstore_region),
                                    opt_objstore_endpoint ? &endpoint : nullptr,
                                    opt_objstore_use_https, obj_error_msg);
  if (objstore == nullptr) {
    err_msg.assign("Failed to create object store instance");
    if (!obj_error_msg.empty()) {
      err_msg.append(": ");
      err_msg.append(obj_error_msg);
    }
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           err_msg.c_str());
    error = 1;
    goto err;
  }

  if (opt_repo_objstore_id) {
    std::string binlog_objectstore_path(opt_repo_objstore_id);
    binlog_objectstore_path.append(FN_DIRSEP);
    binlog_objectstore_path.append(opt_branch_objstore_id);
    binlog_objectstore_path.append(FN_DIRSEP);
    binlog_objectstore_path.append(BINLOG_ARCHIVE_SUBDIR);
    binlog_objectstore_path.append(FN_DIRSEP);
    strmake(binlog_archive_dir, binlog_objectstore_path.c_str(),
            sizeof(binlog_archive_dir) - 1);
  } else {
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           "In serverless mode, must set --repo_objectstore_id");
    error = 1;
    goto err;
  }

  if (!opt_recovery_consistent_snapshot_tmpdir) {
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           "In serverless mode, must set --recovery_snapshot_tmpdir");
    error = 1;
    goto err;
  }

  convert_dirname(mysql_real_data_home, mysql_real_data_home, NullS);

  if (test_if_hard_path(opt_recovery_consistent_snapshot_tmpdir)) {
    strmake(mysql_archive_recovery_dir, opt_recovery_consistent_snapshot_tmpdir,
            sizeof(mysql_archive_recovery_dir) - 1);
  } else {
    strxnmov(mysql_archive_recovery_dir, sizeof(mysql_archive_recovery_dir) - 1,
             mysql_real_data_home, opt_recovery_consistent_snapshot_tmpdir,
             NullS);
  }
  // create new tmp recovery dir.
  convert_dirname(mysql_archive_recovery_dir, mysql_archive_recovery_dir,
                  NullS);
  remove_file(mysql_archive_recovery_dir);
  if (my_mkdir(mysql_archive_recovery_dir, my_umask_dir, MYF(0))) {
    err_msg.assign("Failed to create tmp recovery dir: ");
    err_msg.append(mysql_archive_recovery_dir);
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           err_msg.c_str());
    error = 1;
    goto err;
  }

  // create new recovery binlog dir.
  strmake(strmake(mysql_archive_recovery_binlog_dir, mysql_archive_recovery_dir,
                  sizeof(mysql_archive_recovery_binlog_dir) -
                      BINLOG_ARCHIVE_SUBDIR_LEN - 1),
          STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  convert_dirname(mysql_archive_recovery_binlog_dir,
                  mysql_archive_recovery_binlog_dir, NullS);
  remove_file(mysql_archive_recovery_binlog_dir);
  if (my_mkdir(mysql_archive_recovery_binlog_dir, my_umask_dir, MYF(0))) {
    err_msg.assign("Failed to create tmp recovery binlog dir: ");
    err_msg.append(mysql_archive_recovery_binlog_dir);
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           err_msg.c_str());
    error = 1;
    goto err;
  }

  // Download binlog.index from object store to recover tmp dir.
  // And open objstore binlog index file.
  binlog_index_file_name.assign(mysql_archive_recovery_binlog_dir);
  binlog_index_file_name.append(BINLOG_ARCHIVE_INDEX_LOCAL_FILE);
  // The persisted `binlog-index.index` is based on multiple versions of the
  // consensus term, so the `binlog-index.index` with the highest consensus term
  // should be obtained as the initial one.
  {
    std::vector<objstore::ObjectMeta> objects{};
    bool finished = false;
    std::string start_after{};
    std::string binlog_index_prefix{};
    binlog_index_prefix.assign(binlog_archive_dir);
    binlog_index_prefix.append(BINLOG_ARCHIVE_INDEX_FILE_BASENAME);
    do {
      std::vector<objstore::ObjectMeta> tmp_objects{};
      objstore::Status ss = objstore->list_object(
          std::string_view(opt_objstore_bucket), binlog_index_prefix, false,
          start_after, finished, tmp_objects);
      if (!ss.is_succ()) {
        err_msg.assign("List persistent binlog index files failed: ");
        err_msg.append(binlog_index_prefix);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL,
               ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
               err_msg.c_str());
        error = 1;
        goto err;
      }
      objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
    } while (finished == false);
    // if no persistent binlog.index, return.
    if (objects.empty()) {
      error = 0;
      goto err;
    }
    last_index_keyid.assign((objects.back()).key);
    err_msg.assign("the last persistent binlog index is ");
    err_msg.append(last_index_keyid);
    LogErr(SYSTEM_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           err_msg.c_str());
  }
  remove_file(binlog_index_file_name);
  {
    objstore::Status ss = objstore->get_object_to_file(
        std::string_view(opt_objstore_bucket), last_index_keyid,
        std::string_view(binlog_index_file_name));
    if (!ss.is_succ()) {
      err_msg.assign("download persistent binlog index file failed: ");
      err_msg.append("key=");
      err_msg.append(last_index_keyid);
      err_msg.append(" to ");
      err_msg.append(binlog_index_file_name);
      LogErr(ERROR_LEVEL,
             ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
             err_msg.c_str());
      error = 1;
      goto err;
    }
  }

  if (my_chmod(binlog_index_file_name.c_str(),
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
               MYF(0))) {
    err_msg.assign("Failed to chmod: ");
    err_msg.append(binlog_index_file_name);
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           err_msg.c_str());
    return 1;
  }

  if (open_binlog_index_file(&binlog_index_file, binlog_index_file_name.c_str(),
                             READ_CACHE)) {
    err_msg.assign("Failed to open persistent binlog-index.index");
    err_msg.append(binlog_index_file_name);
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           err_msg.c_str());
    error = 1;
    goto err;
  }

  error = Binlog_archive::find_log_pos_common(&binlog_index_file, &log_info,
                                              nullptr, 0);
  if (error != 0) {
    err_msg.assign("Failed to find binlog slice entry");
    err_msg.append(binlog_index_file_name);
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           err_msg.c_str());
    error = 1;
    goto err;
  }
  do {
  } while (0 == (error = Binlog_archive::find_next_log_common(
                     &binlog_index_file, &log_info)));
  if (error == LOG_INFO_IO) {
    err_msg.assign("Failed to find binlog slice entry");
    err_msg.append(binlog_index_file_name);
    LogErr(ERROR_LEVEL,
           ER_CONSISTENT_RECOVERY_GET_LAST_BINLOG_CONSENSUS_INDEX_LOG,
           err_msg.c_str());
    error = 1;
    goto err;
  }
  error = 0;
  consistent_recovery_snasphot_end_consensus_index =
      log_info.slice_end_consensus_index;

  err_msg.assign("last binlog end consensus index=");
  err_msg.append(
      std::to_string(consistent_recovery_snasphot_end_consensus_index));
  err_msg.append(" log slice entry=");
  err_msg.append(log_info.log_slice_name);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
err:
  close_binlog_index_file(&binlog_index_file);
  if (objstore) {
    objstore::cleanup_objstore_provider(objstore);
    objstore::destroy_object_store(objstore);
  }

  return error;
}
static bool is_number(const char *str, ulonglong *res, bool allow_wildcards) {
  int flag;
  const char *start;
  DBUG_TRACE;

  flag = 0;
  start = str;
  while (*str++ == ' ')
    ;
  if (*--str == '-' || *str == '+') str++;
  while (my_isdigit(files_charset_info, *str) ||
         (allow_wildcards && (*str == wild_many || *str == wild_one))) {
    flag = 1;
    str++;
  }
  if (*str == '.') {
    for (str++; my_isdigit(files_charset_info, *str) ||
                (allow_wildcards && (*str == wild_many || *str == wild_one));
         str++, flag = 1)
      ;
  }
  if (*str != 0 || flag == 0) return false;
  if (res) *res = atol(start);
  return true; /* Number ok */
}

/**
 * @brief Binlog slice on S3 cannot guarantee the integrity of transaction logs,
 * Consensus will truncate the incomplete transaction binlog logs after
 * recovery. Therefore, the binlog archive cannot simply start from the last
 * binlog slice on S3. Instead, the binlog on S3 should be truncated first
 * before beginning the archive process. Only the last binlog should be
 *truncated truncate_archived_binlog(log_name, pos);
 *
 * @param log_file_name_arg persistent binlog name.
 * @param log_pos persistent binlog position.
 * @param has_truncated return.
 * @return int
 */
int Consistent_recovery::truncate_binlog_slice_from_objstore(
    const char *log_file_name_arg, my_off_t log_pos, bool &has_truncated) {
  std::string err_msg;
  objstore::Status ss;
  std::vector<objstore::ObjectMeta> objects;
  std::string binlog_keyid{};
  binlog_keyid.assign(m_binlog_archive_dir);
  binlog_keyid.append(log_file_name_arg);
  binlog_keyid.append(".");

  bool finished = false;
  std::string start_after;
  // Find last binlog.000000n.slice
  do {
    std::vector<objstore::ObjectMeta> tmp_objects;
    ss = recovery_objstore->list_object(std::string_view(m_objstore_bucket),
                                        binlog_keyid, false, start_after,
                                        finished, tmp_objects);
    // The binlog present in persistent binlog-index.index should also exist in
    // object store.
    if (!ss.is_succ()) {
      err_msg.assign("list binlog slice from object store failed: ");
      err_msg.append(binlog_keyid);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }
    objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
  } while (finished == false);

  my_off_t number = 0;
  my_off_t pre_slice_number = 0;
  bool truncate_slice_end = false;
  for (const auto &object : objects) {
    if (is_number(object.key.c_str() + binlog_keyid.length(), &number, false)) {
      if (truncate_slice_end) {
        recovery_objstore->delete_object(std::string_view(opt_objstore_bucket),
                                         object.key);
      } else {
        if (number == log_pos) {
          truncate_slice_end = true;
        }
        if (number > log_pos) {
          my_off_t slice_pos;
          slice_pos = log_pos - pre_slice_number;
          truncate_slice_end = true;

          std::string binlog_slice_name;
          binlog_slice_name.assign(m_mysql_archive_recovery_binlog_dir);
          binlog_slice_name.append(log_file_name_arg);
          binlog_slice_name.append(BINLOG_ARCHIVE_SLICE_LOCAL_SUFFIX);
          // Delete the local binlog slice file if it exists.
          remove_file(binlog_slice_name);
          // download slice from object store
          ss = recovery_objstore->get_object_to_file(
              std::string_view(opt_objstore_bucket), object.key,
              binlog_slice_name);
          if (!ss.is_succ()) {
            err_msg.assign("Failed to download binlog file slice: ");
            err_msg.append(" key=");
            err_msg.append(object.key);
            err_msg.append(" file=");
            err_msg.append(binlog_slice_name);
            err_msg.append(" error=");
            err_msg.append(ss.error_message());
            LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
            return 1;
          }

          if (my_chmod(binlog_slice_name.c_str(),
                       USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE |
                           OTHERS_READ,
                       MYF(0))) {
            err_msg.assign("Failed to chmod: ");
            err_msg.append(binlog_slice_name);
            LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
            return 1;
          }

          IO_CACHE_ostream file_ostream;
          file_ostream.open(0, binlog_slice_name.c_str(), MYF(0));
          file_ostream.truncate(slice_pos);
          file_ostream.close();
          // Upload truncated binlog slice, use new end_pos.
          size_t len;
          std::string new_binlog_slice_keyid;
          char binlog_slice_ext[12] = {0};
          new_binlog_slice_keyid.assign(m_binlog_archive_dir);
          new_binlog_slice_keyid.append(log_file_name_arg);
          len = snprintf(binlog_slice_ext, sizeof(binlog_slice_ext),
                         "." BINLOG_ARCHIVE_SLICE_POSITION_EXT, log_pos);
          assert(len > 0);
          new_binlog_slice_keyid.append(binlog_slice_ext);
          ss = recovery_objstore->put_object_from_file(
              std::string_view(opt_objstore_bucket), new_binlog_slice_keyid,
              binlog_slice_name);
          if (!ss.is_succ()) {
            err_msg.assign("Failed to upload binlog file to object store: ");
            err_msg.append(new_binlog_slice_keyid);
            err_msg.append(" error=");
            err_msg.append(ss.error_message());
            LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
            return 1;
          }
          // Delete old slice
          recovery_objstore->delete_object(
              std::string_view(opt_objstore_bucket), object.key);
          err_msg.assign("truncate binlog on object store: ");
          err_msg.append(log_file_name_arg);
          err_msg.append(" to pos ");
          err_msg.append(std::to_string(log_pos));
          LogErr(INFORMATION_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
                 err_msg.c_str());
          has_truncated = true;
        }
        pre_slice_number = number;
      }
    }
  }

  return 0;
}

/**
 * @brief Download binlog slice from object store and append to mysql binlog
 * file.
 *
 * @param persistent_binlog_file source, persistent binlog file.
 * @param mysql_binlog_name destination, mysql binlog basename.
 * @return int
 */
int Consistent_recovery::merge_slice_to_binlog_file(
    const char *persistent_binlog_file, std::string mysql_binlog_name,
    my_off_t &mysql_binlog_end_pos) {
  std::streamsize binlog_size = 0;
  my_off_t slice_number = 0;
  std::string err_msg;

  remove_file(mysql_binlog_name);

  err_msg.assign("merge slice to binlog file ");
  err_msg.append(persistent_binlog_file);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  // Find the first slice of the last binlog from persistent binlog.index.
  LOG_ARCHIVED_INFO log_info;
  int error = 1;
  error = Binlog_archive::find_log_pos_common(&m_binlog_index_file, &log_info,
                                              persistent_binlog_file, 0);
  if (error != 0) {
    return 1;
  }

  do {
    std::string binlog_slice_name;
    std::string binlog_slice_keyid;
    binlog_slice_name.assign(m_mysql_archive_recovery_binlog_dir);
    binlog_slice_name.append(persistent_binlog_file);
    binlog_slice_name.append(BINLOG_ARCHIVE_SLICE_LOCAL_SUFFIX);
    binlog_slice_keyid.assign(m_binlog_archive_dir);
    binlog_slice_keyid.append(log_info.log_slice_name);
    err_msg.assign("merge slice ");
    err_msg.append(log_info.log_slice_name);
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // Delete the local binlog slice file if it exists.
    remove_file(binlog_slice_name);
    // download slice from object store
    objstore::Status ss = recovery_objstore->get_object_to_file(
        std::string_view(m_objstore_bucket), binlog_slice_keyid,
        binlog_slice_name);
    if (!ss.is_succ()) {
      err_msg.assign("download binlog slice file failed: ");
      err_msg.append(" key=");
      err_msg.append(binlog_slice_keyid);
      err_msg.append(" slice file=");
      err_msg.append(binlog_slice_name);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }

    if (my_chmod(
            binlog_slice_name.c_str(),
            USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
            MYF(0))) {
      err_msg.assign("Failed to chmod: ");
      err_msg.append(binlog_slice_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }

    // append binlog slice file to binlog file.
    std::ofstream binlog_file;
    binlog_file.open(mysql_binlog_name, std::ofstream::app);
    if (!binlog_file.is_open()) {
      err_msg.assign("open binlog file failed: ");
      err_msg.append(mysql_binlog_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }
    std::ifstream binlog_slice;
    binlog_slice.open(binlog_slice_name, std::ifstream::in);
    if (!binlog_slice.is_open()) {
      err_msg.assign("Failed to open binlog slice file: ");
      err_msg.append(binlog_slice_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }
    binlog_file << binlog_slice.rdbuf();
    binlog_slice.close();
    // seek file end and get file size.
    binlog_file.seekp(0, std::ios::end);
    binlog_size = binlog_file.tellp();
    binlog_file.close();
    slice_number = log_info.slice_end_pos;
    if (static_cast<my_off_t>(binlog_size) != slice_number) {
      err_msg.assign("Failed to get binlog slice : ");
      err_msg.append(log_info.log_slice_name);
      err_msg.append(" binlog total size = ");
      err_msg.append(std::to_string(binlog_size));
      LogErr(INFORMATION_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }

    // remove binlog slice file
    remove_file(binlog_slice_name);
  } while (!(error = Binlog_archive::find_next_log_common(&m_binlog_index_file,
                                                          &log_info, true)) &&
           compare_log_name(log_info.log_file_name, persistent_binlog_file) ==
               0);
  err_msg.assign("binlog ");
  err_msg.append(mysql_binlog_name);
  err_msg.append(" size=");
  err_msg.append(std::to_string(binlog_size));
  err_msg.append(" last slice number=");
  err_msg.append(std::to_string(slice_number));
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());

  mysql_binlog_end_pos = slice_number;

  if (my_chmod(mysql_binlog_name.c_str(),
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
               MYF(MY_WME))) {
    err_msg.assign("Failed to chmod: ");
    err_msg.append(mysql_binlog_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return 1;
  }
  return 0;
}

/**
 * @brief Write consistent snapshot recovery status to mysql data dir.
 *
 * @param recovery_status
 * @return int
 */
int Consistent_recovery::write_consistent_snapshot_recovery_status(
    Consistent_snapshot_recovery_status &recovery_status) {
  std::string file_name;
  std::ofstream status_file;
  convert_dirname(mysql_real_data_home, mysql_real_data_home, NullS);
  file_name.assign(mysql_real_data_home);
  file_name.append(CONSISTENT_SNAPSHOT_RECOVERY_FILE);
  status_file.open(file_name, std::ofstream::out | std::ofstream::trunc);
  if (!status_file.is_open()) {
    return 1;
  }
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
         "write snapshot recovery status file.");
  status_file << recovery_status.m_recovery_status << std::endl;
  status_file << recovery_status.m_end_binlog_pos << std::endl;
  status_file << recovery_status.m_end_consensus_index << std::endl;
  status_file << recovery_status.m_apply_stop_timestamp << std::endl;
  status_file.close();
  return 0;
}

/**
 * @brief Read consistent snapshot recovery status from mysql data dir.
 *
 * @param recovery_status
 * @return int
 */
int Consistent_recovery::read_consistent_snapshot_recovery_status(
    Consistent_snapshot_recovery_status &recovery_status) {
  if (!opt_recovery_from_objstore && !opt_initialize_from_objstore) return 1;

  std::string file_name;
  std::ifstream status_file;
  convert_dirname(mysql_real_data_home, mysql_real_data_home, NullS);
  file_name.assign(mysql_real_data_home);
  file_name.append(CONSISTENT_SNAPSHOT_RECOVERY_FILE);
  status_file.open(file_name, std::ifstream::in);
  if (!status_file.is_open()) {
    return 1;
  }
  std::string file_line;
  int line_number = 0;
  /* loop through the lines and extract consistent snapshot information. */
  while (std::getline(status_file, file_line)) {
    ++line_number;
    std::stringstream file_data(file_line, std::ifstream::in);
    switch (line_number) {
      case 1:
        file_data >> recovery_status.m_recovery_status;
        break;
      case 2:
        file_data >> recovery_status.m_end_binlog_pos;
        break;
      case 3:
        file_data >> recovery_status.m_end_consensus_index;
        break;
      case 4:
        strncpy(recovery_status.m_apply_stop_timestamp, file_line.c_str(),
                sizeof(recovery_status.m_apply_stop_timestamp) - 1);
        break;
      default:
        break;
    }
  }
  status_file.close();
  return 0;
}

/**
 * @brief Construct a new binlog.index file.
 *
 * @param index_file_name
 * @return int
 */
int Consistent_recovery::open_binlog_index_file(IO_CACHE *index_file,
                                                const char *index_file_name,
                                                enum cache_type type) {
  int error = false;
  File index_file_nr = -1;
  myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;

  if ((index_file_nr =
           my_open(index_file_name, O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      init_io_cache(index_file, index_file_nr, IO_SIZE, type, 0, false,
                    flags)) {
    if (index_file_nr >= 0) my_close(index_file_nr, MYF(0));
    error = 1;
    goto end;
  }

end:
  return error;
}

/**
 * @brief Add a line to binlog.index file.
 *
 * @param index_file
 * @param log_name
 * @return int
 */
int Consistent_recovery::add_line_to_index_file(IO_CACHE *index_file,
                                                const char *log_name) {
  if (my_b_write(index_file, (const uchar *)log_name, strlen(log_name)) ||
      my_b_write(index_file, pointer_cast<const uchar *>("\n"), 1) ||
      flush_io_cache(index_file) ||
      mysql_file_sync(index_file->file, MYF(MY_WME))) {
    goto err;
  }
  return 0;
err:
  return 1;
}

int Consistent_recovery::close_binlog_index_file(IO_CACHE *index_file) {
  int error = 0;

  DBUG_TRACE;

  if (my_b_inited(index_file)) {
    end_io_cache(index_file);
    error = my_close(index_file->file, MYF(0));
  }

  return error;
}

static int compare_log_name(const char *log_1, const char *log_2) {
  const char *log_1_basename = log_1 + dirname_length(log_1);
  const char *log_2_basename = log_2 + dirname_length(log_2);

  return strcmp(log_1_basename, log_2_basename);
}

/**
 * @brief Truncate all persistent binlog after log_file_name_arg/log_pos.
 *
 * @param log_file_name_arg persistent binlog file
 * @param log_pos persistent binlog postion
 * @return int
 */
int Consistent_recovery::truncate_binlogs_from_objstore(
    const char *log_file_name_arg, my_off_t log_pos) {
  DBUG_TRACE;
  std::string err_msg;
  LOG_ARCHIVED_INFO log_info;
  IO_CACHE new_index_io_cache;
  IO_CACHE index_io_cache;
  char new_index_file_name[FN_REFLEN] = {0};
  int error = 0;
  const char *to_truncate_persistent_log_name = nullptr;
  bool has_truncated = false;

  if (log_file_name_arg[0] == '\0') return 0;

  if (m_binlog_index_keyid[0] == '\0') return 0;

  // Convert mysql binlog name to persistent binlog name.
  to_truncate_persistent_log_name =
      log_file_name_arg + dirname_length(log_file_name_arg);

  std::string index_file;
  index_file.assign(m_mysql_archive_recovery_binlog_dir);
  index_file.append(BINLOG_ARCHIVE_INDEX_LOCAL_FILE);
  // Delete the local temp binlog index file if it exists.
  remove_file(index_file);
  // Download binlog.index from object store to recover tmp dir.
  objstore::Status ss = recovery_objstore->get_object_to_file(
      std::string_view(m_objstore_bucket),
      std::string_view(m_binlog_index_keyid), index_file);
  if (!ss.is_succ()) {
    err_msg.assign("download persistent binlog.index failed.");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return 1;
  }

  if (my_chmod(index_file.c_str(),
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
               MYF(0))) {
    err_msg.assign("Failed to chmod: ");
    err_msg.append(index_file);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }

  if (fn_format(new_index_file_name, BINLOG_ARCHIVE_INDEX_LOCAL_FILE,
                m_mysql_archive_recovery_binlog_dir, ".~rec~",
                MYF(MY_UNPACK_FILENAME | MY_SAFE_PATH | MY_REPLACE_EXT)) ==
      nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, "failed to fn_format");
    return 1;
  }

  remove_file(new_index_file_name);
  if (open_binlog_index_file(&new_index_io_cache, new_index_file_name,
                             WRITE_CACHE) ||
      open_binlog_index_file(&index_io_cache, index_file.c_str(), READ_CACHE)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, "open binlog.index failed");
    error = 1;
    goto err;
  }

  // Generate new persistent binlog.index until to_truncate_persistent_log_name.
  if (Binlog_archive::find_log_pos_common(&index_io_cache, &log_info, NullS,
                                          0)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to find persistent binlog.index");
    error = 1;
    goto err;
  }
  do {
    if ((add_line_to_index_file(&new_index_io_cache,
                                (char *)log_info.log_line))) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "Failed to apppend local binlog.index");
      error = 1;
      goto err;
    }
  } while (
      compare_log_name(to_truncate_persistent_log_name,
                       log_info.log_file_name) &&
      0 == Binlog_archive::find_next_log_common(&index_io_cache, &log_info));

  // truncate persistent binlog slice after log_pos.
  if (log_pos > 0) {
    truncate_binlog_slice_from_objstore(to_truncate_persistent_log_name,
                                        log_pos, has_truncated);
  }

  // Delete persistent binlog after to_truncate_persistent_log_name
  while ((error = Binlog_archive::find_next_log_common(&index_io_cache,
                                                       &log_info)) == 0) {
    // List all binlog slice.
    std::string binlog_keyid;
    std::vector<objstore::ObjectMeta> objects;
    binlog_keyid.assign(m_binlog_archive_dir);
    binlog_keyid.append(log_info.log_file_name);
    binlog_keyid.append(".");

    bool finished = false;
    std::string start_after;
    do {
      std::vector<objstore::ObjectMeta> tmp_objects;
      ss = recovery_objstore->list_object(std::string_view(m_objstore_bucket),
                                          binlog_keyid, false, start_after,
                                          finished, objects);
      if (!ss.is_succ() &&
          ss.error_code() != objstore::Errors::SE_NO_SUCH_KEY) {
        err_msg.assign("list persistent binlog slice failed: ");
        err_msg.append(binlog_keyid);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        error = 1;
        goto err;
      }
    } while (finished == false);

    // Delete all binlog slice.
    for (const auto &object : objects) {
      ss = recovery_objstore->delete_object(std::string_view(m_objstore_bucket),
                                            object.key);
      if (!ss.is_succ()) {
        err_msg.assign("delete persistent binlog slice failed: ");
        err_msg.append(object.key);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        error = 1;
        goto err;
      }
    }
    has_truncated = true;
    err_msg.assign("truncate persistent binlog file ");
    err_msg.append(binlog_keyid);
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }

  if (error == LOG_INFO_IO) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, "Failed to find index");
    goto err;
  }
  assert(error == LOG_INFO_EOF);
  error = 0;

  if (has_truncated) {
    // refresh 'binlog.index' to object store
    ss = recovery_objstore->put_object_from_file(
        std::string_view(m_objstore_bucket),
        std::string_view(m_binlog_index_keyid), new_index_file_name);
    if (!ss.is_succ()) {
      err_msg.assign(
          "refresh persistent binlog.index to object store failed: ");
      err_msg.append(m_binlog_index_keyid);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      error = 1;
      goto err;
    }
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "refresh persistent binlog.index to object store");
  }

err:
  close_binlog_index_file(&index_io_cache);
  close_binlog_index_file(&new_index_io_cache);
  return error;
}

static int copy_directory(const std::string &from, const std::string &to) {
  uint i;
  MY_DIR *a, *b;
  char from_path[FN_REFLEN + 1] = {0};
  char to_path[FN_REFLEN + 1] = {0};

  strmake(from_path, from.c_str(), sizeof(from_path) - 1);
  convert_dirname(from_path, from_path, NullS);
  strmake(to_path, to.c_str(), sizeof(to_path) - 1);
  convert_dirname(to_path, to_path, NullS);

  // Check if the source directory exists.
  if (!(a = my_dir(from_path, MYF(MY_DONT_SORT | MY_WANT_STAT)))) {
    std::string err_msg;
    err_msg.assign("Failed to open directory: ");
    err_msg.append(from_path);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return 1;
  }

  // If the destination directory does not exist, create it.
  if (!(b = my_dir(to_path, MYF(MY_DONT_SORT | MY_WANT_STAT)))) {
    if (my_mkdir(to_path, my_umask_dir, MYF(0)) < 0) {
      std::string err_msg;
      err_msg.assign("Failed to create directory: ");
      err_msg.append(to_path);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }
  } else {
    my_dirend(b);
  }

  // Iterate through the source directory.
  for (i = 0; i < (uint)a->number_off_files; i++) {
    // Skip . and ..
    if (strcmp(a->dir_entry[i].name, ".") == 0 ||
        strcmp(a->dir_entry[i].name, "..") == 0) {
      continue;
    }
    std::string tmp_from;
    std::string tmp_to;
    tmp_from.assign(from_path);
    tmp_from.append(a->dir_entry[i].name);
    tmp_to.assign(to_path);
    tmp_to.append(a->dir_entry[i].name);
    // If its a folder, iterate it. Otherwise, copy the file.
    if (MY_S_ISDIR(a->dir_entry[i].mystat->st_mode)) {
      if (copy_directory(tmp_from, tmp_to)) {
        my_dirend(a);
        return 1;
      }
    } else {
      if (my_copy(tmp_from.c_str(), tmp_to.c_str(), MYF(0)) ||
          my_chmod(
              tmp_to.c_str(),
              USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
              MYF(0))) {
        std::string err_msg;
        err_msg.assign("Failed to open directory: ");
        err_msg.append(from_path);
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        return 1;
      }
    }
  }
  my_dirend(a);
  return 0;
}

/**
 * @brief Remove file or directory.
 * @note If file is directory, remove all files and sub directories.
 *       If file or directory is not exist, return false.
 * @param file
 */
static bool remove_file(const std::string &file) {
  uint i;
  MY_DIR *dir;
  MY_STAT stat_area;

  if (!my_stat(file.c_str(), &stat_area, MYF(0))) return false;
  if (!MY_S_ISDIR(stat_area.st_mode)) {
    if (my_delete(file.c_str(), MYF(MY_WME))) return true;
    return false;
  }

  char dir_path[FN_REFLEN + 1] = {0};
  strmake(dir_path, file.c_str(), sizeof(dir_path) - 1);
  convert_dirname(dir_path, dir_path, NullS);

  // open directory.
  if (!(dir = my_dir(dir_path, MYF(MY_DONT_SORT | MY_WANT_STAT)))) {
    return true;
  }

  // Iterate through the source directory.
  for (i = 0; i < (uint)dir->number_off_files; i++) {
    // Skip . and ..
    if (strcmp(dir->dir_entry[i].name, ".") == 0 ||
        strcmp(dir->dir_entry[i].name, "..") == 0) {
      continue;
    }
    std::string tmp_sub;
    tmp_sub.assign(dir_path);
    tmp_sub.append(dir->dir_entry[i].name);
    if (remove_file(tmp_sub)) {
      return true;
    }
  }
  my_dirend(dir);

  // remove the directory.
  if (rmdir(dir_path)) {
    return true;
  }

  return false;
}

/**
 * @brief Get the root directory and left string.
 * @param in_str input string.
 * @param out_str output root directory from in_str. or empty string.
 * @param left_str output left string from in_str. or in_str.
 */
static void root_directory(std::string in_str, std::string *out_str,
                           std::string *left_str) {
  // size_t idx = in.rfind(FN_DIRSEP);
  size_t idx = in_str.find(FN_DIRSEP);
  if (idx == std::string::npos) {
    out_str->assign("");
    left_str->assign(in_str);
  } else {
    // out string include the last FN_DIRSEP.
    out_str->assign(in_str.substr(0, idx + 1));
    // left string.
    left_str->assign(in_str.substr(idx + 1));
  }
}

/**
 * @brief Create directory recursively.
 *
 * @param dir
 * @param root
 */
static void recursive_create_dir(const std::string &dir,
                                 const std::string &root) {
  std::string out;
  std::string left;
  root_directory(dir, &out, &left);
  if (out.empty()) {
    return;
  }
  std::string real_path;

  real_path.assign(root);
  real_path.append(out);
  MY_STAT stat_area;
  if (!my_stat(real_path.c_str(), &stat_area, MYF(0))) {
    if (my_mkdir(real_path.c_str(), my_umask_dir, MYF(0))) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "Failed to create binlog archive dir.");
    }
  }
  recursive_create_dir(left, real_path);
}

static int recursive_chmod(const std::string &from) {
  uint i;
  MY_DIR *a;
  char from_path[FN_REFLEN + 1] = {0};

  strmake(from_path, from.c_str(), sizeof(from_path) - 1);
  convert_dirname(from_path, from_path, NullS);

  // Check if the source directory exists.
  if (!(a = my_dir(from_path, MYF(MY_DONT_SORT | MY_WANT_STAT)))) {
    std::string err_msg;
    err_msg.assign("Failed to open directory: ");
    err_msg.append(from_path);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return 1;
  }

  if (my_chmod(from_path,
               USER_READ | USER_WRITE | USER_EXECUTE | GROUP_READ |
                   GROUP_WRITE | GROUP_EXECUTE | OTHERS_READ | OTHERS_EXECUTE,
               MYF(0))) {
    std::string err_msg;
    err_msg.assign("Failed to chmod: ");
    err_msg.append(from_path);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return 1;
  }

  // Iterate through the source directory.
  for (i = 0; i < (uint)a->number_off_files; i++) {
    // Skip . and ..
    if (strcmp(a->dir_entry[i].name, ".") == 0 ||
        strcmp(a->dir_entry[i].name, "..") == 0) {
      continue;
    }
    std::string tmp_from;
    tmp_from.assign(from_path);
    tmp_from.append(a->dir_entry[i].name);
    // If its a folder, iterate it. Otherwise, copy the file.
    if (MY_S_ISDIR(a->dir_entry[i].mystat->st_mode)) {
      if (recursive_chmod(tmp_from)) {
        my_dirend(a);
        return 1;
      }
    } else {
      if (my_chmod(
              tmp_from.c_str(),
              USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
              MYF(0))) {
        std::string err_msg;
        err_msg.assign("Failed to chmod: ");
        err_msg.append(tmp_from);
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
        return 1;
      }
    }
  }
  my_dirend(a);
  return 0;
}

/**
  Converts a datetime String value to its my_time_t representation.

  @retval 0	OK
  @retval =!0	error
*/
static int convert_str_to_datetime(const char *str, ulong &my_time) {
  MYSQL_TIME_STATUS status;
  MYSQL_TIME l_time;
  my_time_t dummy_my_timezone;
  bool dummy_in_dst_time_gap;
  DBUG_TRACE;

  /* We require a total specification (date AND time) */
  if (str_to_datetime(str, strlen(str), &l_time, 0, &status) ||
      (l_time.time_type != MYSQL_TIMESTAMP_DATETIME_TZ &&
       l_time.time_type != MYSQL_TIMESTAMP_DATETIME)) {
    return 1;
  }

  /*
    Note that Feb 30th, Apr 31st cause no error messages and are mapped to
    the next existing day, like in mysqld. Maybe this could be changed when
    mysqld is changed too (with its "strict" mode?).
  */
  my_time = static_cast<ulong>(
      my_system_gmt_sec(l_time, &dummy_my_timezone, &dummy_in_dst_time_gap));
  return 0;
}

static inline const char *rpl_make_log_name(PSI_memory_key key, const char *opt,
                                            const char *def, const char *ext) {
  DBUG_TRACE;
  DBUG_PRINT("enter", ("opt: %s, def: %s, ext: %s", (opt && opt[0]) ? opt : "",
                       def, ext));
  char buff[FN_REFLEN];
  /*
    opt[0] needs to be checked to make sure opt name is not an empty
    string, in case it is an empty string default name will be considered
  */
  const char *base = (opt && opt[0]) ? opt : def;
  unsigned int options = MY_REPLACE_EXT | MY_UNPACK_FILENAME | MY_SAFE_PATH;

  /* mysql_real_data_home_ptr may be null if no value of datadir has been
     specified through command-line or througha cnf file. If that is the
     case we make mysql_real_data_home_ptr point to mysql_real_data_home
     which, in that case holds the default path for data-dir.
  */

  if (mysql_real_data_home_ptr == nullptr)
    mysql_real_data_home_ptr = mysql_real_data_home;

  if (fn_format(buff, base, mysql_real_data_home_ptr, ext, options))
    return my_strdup(key, buff, MYF(0));
  else
    return nullptr;
}
