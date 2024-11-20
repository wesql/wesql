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

#include "sql/consistent_archive.h"

#include <fstream>
#include <sstream>
#include <string>
#include "my_io.h"

#include "lex_string.h"
#include "libbinlogevents/include/binlog_event.h"
#include "mf_wcomp.h"  // wild_one, wild_many
#include "my_dbug.h"
#include "my_dir.h"
#include "my_sys.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/plugin.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/psi/mysql_thread.h"
#include "mysqld_error.h"
#include "plugin/clone/include/clone.h"
#include "sql/binlog.h"
#include "sql/binlog_archive.h"
#include "sql/binlog_reader.h"
#include "sql/debug_sync.h"
#include "sql/derror.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/log.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"  // Global_THD_manager
#include "sql/rpl_handler.h"
#include "sql/rpl_io_monitor.h"
#include "sql/rpl_source.h"
#include "sql/sql_backup_lock.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/sys_vars_shared.h"
#include "sql/transaction.h"

/** Mysql local clone directory ,refer to clone_status.cc */
#define MYSQL_LOCAL_CLONE_FILES_DIR "#clone" FN_DIRSEP
/** Mysql local clone view clone_status persister file */
#define MYSQL_LOCAL_CLONE_VIEW_STATUS_FILE \
  MYSQL_LOCAL_CLONE_FILES_DIR "#view_status"

// mysql consistent snapshot archive object
static Consistent_archive mysql_consistent_archive;
static my_thread_handle mysql_archiver_pthd;
static bool abort_archive = false;
static mysql_mutex_t m_run_lock;
static mysql_cond_t m_run_cond;

#ifdef HAVE_PSI_INTERFACE
static PSI_thread_key THR_KEY_consistent_archive;
static PSI_mutex_key PSI_consistent_archive_lock_key;
static PSI_cond_key PSI_consistent_archive_cond_key;
static PSI_mutex_key PSI_consistent_index_lock_key;
static PSI_mutex_key PSI_mysql_innodb_clone_index_lock_key;
static PSI_mutex_key PSI_se_backup_index_lock_key;

/** The instrumentation key to use for opening the index file. */
static PSI_file_key PSI_consistent_archive_mysql_log_index_key;
static PSI_file_key PSI_consistent_archive_se_log_index_key;
static PSI_file_key PSI_consistent_snapshot_file_key;
static PSI_file_key PSI_archive_file_key;
/** The instrumentation key to use for opening a index cache file. */
static PSI_file_key PSI_consistent_archive_mysql_log_index_cache_key;
static PSI_file_key PSI_consistent_archive_se_log_index_cache_key;
static PSI_file_key PSI_consistent_snapshot_file_cache_key;

static PSI_thread_info all_consistent_archive_threads[] = {
    {&THR_KEY_consistent_archive, "archive", "ss_arch", PSI_FLAG_AUTO_SEQNUM, 0,
     PSI_DOCUMENT_ME}};

static PSI_mutex_info all_consistent_archive_mutex_info[] = {
    {&PSI_consistent_index_lock_key, "consistent_mutex", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_mysql_innodb_clone_index_lock_key, "innodb_mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_se_backup_index_lock_key, "se_mutex", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_consistent_archive_lock_key, "archive_mutex", 0, 0, PSI_DOCUMENT_ME}};

static PSI_cond_info all_consistent_archive_cond_info[] = {
    {&PSI_consistent_archive_cond_key, "archive_condition", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_file_info all_consistent_archive_files[] = {
    {&PSI_consistent_archive_mysql_log_index_key, "innodb_index", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_consistent_archive_mysql_log_index_cache_key, "innodb_index_cache", 0,
     0, PSI_DOCUMENT_ME},
    {&PSI_consistent_archive_se_log_index_key, "se_index", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_consistent_archive_se_log_index_cache_key, "se_index_cache", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_archive_file_key, "archive_file", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_consistent_snapshot_file_key, "snapshot_file", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_consistent_snapshot_file_cache_key, "snapshot_file_cache", 0, 0,
     PSI_DOCUMENT_ME}};
#endif

static void *consistent_archive_action(void *arg);
static bool remove_file(const std::string &file);
static bool copy_file(IO_CACHE *from, IO_CACHE *to, my_off_t offset);
static bool file_has_suffix(const std::string &sfx, const std::string &path);
static int compare_log_name(const char *log_1, const char *log_2);
static time_t calculate_auto_purge_lower_time_bound();
static int convert_str_to_datetime(const char *str, ulong &my_time);
static size_t convert_datetime_to_str(char *str, time_t ts);
static const char *convert_archive_progress_to_str(
    Consistent_snapshot_archive_progress archive_progress);
static int check_archive_file_version_type(std::string &archive_file_name);
static uint64_t extract_term_from_index_file(const char *index_file_name);
/**
 * @brief Creates a consistent archive object and starts the consistent snapshot
 * archive thread.
 * Called when mysqld main thread mysqld_main().
 */
int start_consistent_archive() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("start_consistent_archive"));

  if (!opt_consistent_snapshot_archive || !opt_serverless) {
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "snapshot archive not enabled");
    return 0;
  }

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
         "start snapshot archive ...");

#ifdef WESQL_CLUSTER
  // Check Logger mode.
  if (is_consensus_replication_log_mode()) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "snapshot archive cannot be enabled in logger mode");
    return 1;
  }
#endif

  if (!opt_consistent_snapshot_archive_dir) {
    opt_consistent_snapshot_archive_dir = mysql_tmpdir;
  }

  // Not set consistent archive dir
  if (opt_consistent_snapshot_archive_dir == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "snapshot archive local path not set, please set "
           "--snapshot_archive_dir");
    return 1;
  }

  if (!opt_consistent_snapshot_persistent_on_objstore) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "snapshot must set objectstore, "
           " --snapshot_archive_on_objectstore");
    return 1;
  }

  // Check if clone plugin enable.
  plugin_ref plugin{};
  plugin = my_plugin_lock_by_name(nullptr, {STRING_WITH_LEN("clone")},
                                  MYSQL_CLONE_PLUGIN);
  if (plugin == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, "clone plugin load failed");
    return true;
  }
  plugin_unlock(nullptr, plugin);

  // Check if smartengine plugin enable.
  plugin = my_plugin_lock_by_name(nullptr, {STRING_WITH_LEN("smartengine")},
                                  MYSQL_STORAGE_ENGINE_PLUGIN);
  if (plugin == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "smartengine plugin load failed");
    return true;
  }
  plugin_unlock(nullptr, plugin);

  // Check if mysql consistent archive directory exists, and directory is
  // writable.
  MY_STAT stat;
  if (!my_stat(opt_consistent_snapshot_archive_dir, &stat, MYF(0)) ||
      !MY_S_ISDIR(stat.st_mode) ||
      my_access(opt_consistent_snapshot_archive_dir, (F_OK | W_OK))) {
    std::string err_msg;
    err_msg.assign("snapshot archive local dir not exist: ");
    err_msg.append(opt_consistent_snapshot_archive_dir);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return 1;
  }
  // data home can not contain archive directory.
  if (test_if_data_home_dir(opt_consistent_snapshot_archive_dir)) {
    std::string err_msg;
    err_msg.assign(
        "snapshot archive local dir is within the current data directory: ");
    err_msg.append(opt_consistent_snapshot_archive_dir);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return 1;
  }

  // Check if archive binlog dir exists. If not exists, create it.
  char tmp_archive_data_dir[FN_REFLEN + 1] = {0};
  strmake(tmp_archive_data_dir, opt_consistent_snapshot_archive_dir,
          sizeof(tmp_archive_data_dir) - CONSISTENT_ARCHIVE_SUBDIR_LEN - 1);
  strmake(convert_dirname(tmp_archive_data_dir, tmp_archive_data_dir, NullS),
          STRING_WITH_LEN(CONSISTENT_ARCHIVE_SUBDIR));
  convert_dirname(tmp_archive_data_dir, tmp_archive_data_dir, NullS);

  // remove local archive data dir and recreate it.
  if (remove_file(tmp_archive_data_dir)) {
    std::string err_msg;
    err_msg.assign("error ");
    err_msg.append(std::to_string(my_errno()));
    err_msg.append(", failed to remove local snapshot dir ");
    err_msg.append(tmp_archive_data_dir);
    LogErr(WARNING_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return 1;
  }
  if (my_mkdir(tmp_archive_data_dir, 0777, MYF(0))) {
    std::string err_msg;
    err_msg.assign("error ");
    err_msg.append(std::to_string(my_errno()));
    err_msg.append(", failed to create local snapshot dir ");
    err_msg.append(tmp_archive_data_dir);
    LogErr(WARNING_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return 1;
  }

  // persist the snapshot to the object store.
  std::string obj_error_msg;
  std::string err_msg;
  std::string_view endpoint(
      opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");
  objstore::ObjectStore *objstore =
      objstore::create_object_store(std::string_view(opt_objstore_provider),
                                    std::string_view(opt_objstore_region),
                                    opt_objstore_endpoint ? &endpoint : nullptr,
                                    opt_objstore_use_https, obj_error_msg);
  err_msg.assign("create object store instance: ");
  err_msg.append(opt_objstore_provider);
  err_msg.append(" region=");
  err_msg.append(opt_objstore_region);
  err_msg.append(" endpoint =");
  err_msg.append(endpoint);
  err_msg.append(" bucket=");
  err_msg.append(opt_objstore_bucket);
  if (!objstore) {
    err_msg.append(" failed");
    if (!obj_error_msg.empty()) {
      err_msg.append(": ");
      err_msg.append(obj_error_msg);
    }
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return 1;
  }
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  if (strcmp(opt_objstore_provider, "local") == 0) {
    objstore->create_bucket(std::string_view(opt_objstore_bucket));
  }
  mysql_consistent_archive.set_objstore(objstore);

#ifdef HAVE_PSI_INTERFACE
  const char *category = "consistent";
  int count_thread =
      static_cast<int>(array_elements(all_consistent_archive_threads));
  mysql_thread_register(category, all_consistent_archive_threads, count_thread);
#endif

  mysql_mutex_lock(&m_run_lock);
  abort_archive = false;
  if (mysql_thread_create(THR_KEY_consistent_archive, &mysql_archiver_pthd,
                          &connection_attrib, consistent_archive_action,
                          (void *)&mysql_consistent_archive)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "create snapshot archive thread failed");
    mysql_mutex_unlock(&m_run_lock);
    return 1;
  }
  mysql_consistent_archive.thread_set_created();
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
         "snapshot archive thread created");

  while (mysql_consistent_archive.is_thread_alive_not_running()) {
    DBUG_PRINT("sleep", ("Waiting for snapshot archive thread to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_run_cond, &m_run_lock, &abstime);
  }
  mysql_mutex_unlock(&m_run_lock);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
         "start snapshot archive end");
  return 0;
}

/**
 * @brief Stops the consistent snapshot archive thread.
 * Called when mysqld main thread clean_up().
 */
void stop_consistent_archive() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("stop_consistent_archive"));

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, "stop snapshot archive ...");

  mysql_mutex_lock(&m_run_lock);
  if (mysql_consistent_archive.is_thread_dead()) {
    mysql_mutex_unlock(&m_run_lock);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "snapshot archive stopped already");
    return;
  }
  mysql_mutex_unlock(&m_run_lock);
  mysql_consistent_archive.terminate_consistent_archive_thread();
  /* Wait until the thread is terminated. */
  my_thread_join(&mysql_archiver_pthd, nullptr);
  if (mysql_consistent_archive.get_objstore()) {
    objstore::destroy_object_store(mysql_consistent_archive.get_objstore());
    mysql_consistent_archive.set_objstore(nullptr);
  }
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, "stop snapshot archive end");
}

/**
 * @brief Consistent archive thread action.
 *
 * @param arg
 * @return void*
 */
static void *consistent_archive_action(void *arg [[maybe_unused]]) {
  Consistent_archive *archiver = (Consistent_archive *)arg;
  archiver->run();
  return nullptr;
}

/**
 * @brief Constructs a Consistent_archive object.
 */
Consistent_archive::Consistent_archive()
    : m_thread(),
      m_thd(nullptr),
      m_diag_area(false),
      m_thd_state(),
      snapshot_objstore(nullptr),
      m_consensus_term(0),
      m_innodb_tar_compression_mode(CONSISTENT_SNAPSHOT_NO_TAR),
      m_se_tar_compression_mode(CONSISTENT_SNAPSHOT_NO_TAR),
      m_atomic_archive_progress(STAGE_NONE),
      m_mysql_clone_index_file(),
      m_crash_safe_mysql_clone_index_file(),
      m_purge_mysql_clone_index_file(),
      m_mysql_binlog_pos_previous_snapshot(0),
      m_mysql_binlog_pos(0),
      m_consensus_index(0),
      m_se_snapshot_id(0),
      m_se_backup_index_file(),
      m_crash_safe_se_backup_index_file(),
      m_purge_se_backup_index_file(),
      m_consistent_snapshot_index_file(),
      m_crash_safe_consistent_snapshot_index_file() {
  m_archive_dir[0] = '\0';
  m_mysql_archive_dir[0] = '\0';
  m_mysql_archive_data_dir[0] = '\0';

  m_mysql_clone_name[0] = '\0';
  m_mysql_clone_keyid[0] = '\0';
  m_mysql_innodb_clone_dir[0] = '\0';
  m_mysql_clone_index_file_name[0] = '\0';
  m_crash_safe_mysql_clone_index_file_name[0] = '\0';
  m_purge_mysql_clone_index_file_name[0] = '\0';
  m_mysql_clone_next_index_number = 0;
  m_opened_innodb_index_term = 0;

  m_binlog_file[0] = '\0';
  m_mysql_binlog_file_previous_snapshot[0] = '\0';
  m_mysql_binlog_file[0] = '\0';
  m_se_temp_backup_dir[0] = '\0';
  m_se_backup_index_file_name[0] = '\0';
  m_crash_safe_se_backup_index_file_name[0] = '\0';
  m_purge_se_backup_index_file_name[0] = '\0';
  m_se_snapshot_dir[0] = '\0';
  m_se_backup_name[0] = '\0';
  m_se_backup_keyid[0] = '\0';
  m_se_backup_next_index_number = 0;
  m_opened_se_index_term = 0;

  m_consistent_snapshot_local_time[0] = '\0';
  m_consistent_snapshot_index_file_name[0] = '\0';
  m_crash_safe_consistent_snapshot_index_file_name[0] = '\0';

  m_consistent_snapshot_archive_start_ts = 0;
  m_consistent_snapshot_archive_end_ts = 0;
  m_innodb_clone_duration = 0;
  m_innodb_archive_duration = 0;
  m_se_backup_duration = 0;
  m_se_archive_duration = 0;
  m_wait_binlog_archive_duration = 0;
  m_opened_snapshot_index_term = 0;
}

void Consistent_archive::init_pthread_object() {
#ifdef HAVE_PSI_INTERFACE
  const char *category = "consistent";
  int count_mutex =
      static_cast<int>(array_elements(all_consistent_archive_mutex_info));
  mysql_mutex_register(category, all_consistent_archive_mutex_info,
                       count_mutex);

  int count_cond =
      static_cast<int>(array_elements(all_consistent_archive_cond_info));
  mysql_cond_register(category, all_consistent_archive_cond_info, count_cond);

  int count_file =
      static_cast<int>(array_elements(all_consistent_archive_files));
  mysql_file_register(category, all_consistent_archive_files, count_file);

#endif
  mysql_mutex_init(PSI_consistent_archive_lock_key, &m_run_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_consistent_archive_cond_key, &m_run_cond);
  mysql_mutex_init(PSI_consistent_index_lock_key, &m_consistent_index_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(PSI_mysql_innodb_clone_index_lock_key,
                   &m_mysql_innodb_clone_index_lock, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(PSI_se_backup_index_lock_key, &m_se_backup_index_lock,
                   MY_MUTEX_INIT_FAST);
}

void Consistent_archive::deinit_pthread_object() {
  mysql_mutex_destroy(&m_se_backup_index_lock);
  mysql_mutex_destroy(&m_mysql_innodb_clone_index_lock);
  mysql_mutex_destroy(&m_consistent_index_lock);
  mysql_mutex_destroy(&m_run_lock);
  mysql_cond_destroy(&m_run_cond);
}

/**
 * @brief Destructs the Consistent_archive object.
 */
Consistent_archive::~Consistent_archive() {}

/**
 * @brief Returns the consistent snapshot archive instance.
 * @return Consistent_archive*
 */
Consistent_archive *Consistent_archive::get_instance() {
  return &mysql_consistent_archive;
}

mysql_mutex_t *Consistent_archive::get_consistent_archive_lock() {
  return &m_run_lock;
}
/**
 * @brief Terminate the binlog archive thread.
 *
 * @return int
 */
int Consistent_archive::terminate_consistent_archive_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("terminate_consistent_archive_thread"));

  mysql_mutex_lock(&m_run_lock);
  abort_archive = true;
  mysql_cond_broadcast(&m_run_cond);
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep", ("Waiting for snapshot archive thread to stop"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_run_cond, &m_run_lock, &abstime);
  }
  assert(m_thd_state.is_thread_dead());
  mysql_mutex_unlock(&m_run_lock);
  return 0;
}

void Consistent_archive::set_thread_context() {
  THD *thd = new THD;
  my_thread_init();
  thd->set_new_thread_id();
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  thd->thread_stack = (char *)&thd;
  thd->system_thread = SYSTEM_THREAD_BACKGROUND;
  /* No privilege check needed */
  thd->security_context()->skip_grants();
  // Global_THD_manager::get_instance()->add_thd(thd);
  m_thd = thd;
}

static bool is_number(const char *str, uint64_t *res, bool allow_wildcards) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("is_number"));
  int flag = 0;
  const char *start = str;

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
 * @brief Generate a new name for innodb archive file.
 * The naming multi-version format is innodb_{term}_{index_number}
 * or old single-version format like innodb_{index_number}.
 * The number sequence is auto-incremented and maintained in
 * m_mysql_clone_next_index_number. On first call
 * (m_mysql_clone_next_index_number = 0), it scans the index file to find the
 * last used number.
 * @return 0 on success, non-zero on error
 */
int Consistent_archive::generate_innodb_new_name() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("generate_innodb_new_name"));
  LOG_INFO log_info{};
  int error = 0;
  uint64_t number = 0;
  bool need_lock = false;
  std::string err_msg;

  if (m_mysql_clone_next_index_number == 0) {
    need_lock = true;
    mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
    // init new innodb index number from mysql clone index file.
    error = find_line_from_index(&log_info, NullS, ARCHIVE_MYSQL_INNODB);
    // If the file is empty, the starting number is set to 1.
    // Otherwise, the next index number is incremented from the last found
    // number.
    if (error == 0) {
      // For the last entry in the file, it determines the numerical part by
      // stripping file extensions (like .tar or .tar.gz) and extracting the
      // numeric suffix using is_number.
      std::string last_entry{};
      do {
        last_entry.assign(log_info.log_file_name);
      } while (!(
          error = find_next_line_from_index(&log_info, ARCHIVE_MYSQL_INNODB)));
      if (error != LOG_INFO_EOF) {
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               "find innodb archive index file failed");
        goto err;
      }
      err_msg.assign("last innodb archive file");
      err_msg.append(last_entry);
      size_t idx = last_entry.find(".");
      std::string entry{last_entry};
      // Get rid of ".tar" or ".tar.gz" extension ,if format like
      // innodb_000010_000125.tar.gz or old format like innodb_000125.tar.gz
      if (idx != std::string::npos) {
        entry = last_entry.substr(0, idx);
      } else if (entry.back() == FN_LIBCHAR) {
        // Get rid of last '/', if format like innodb_000010_000125/
        // or old format like innodb_000125/
        entry = entry.substr(0, entry.length() - 1);
      } else {
        err_msg.append(" invalid");
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               err_msg.c_str());
        error = LOG_INFO_INVALID;
        goto err;
      }
      // innodb_{term}_{index_number} or old format like innodb_{index_number}.
      std::string last_index_basename;
      last_index_basename.assign(entry.c_str() + dirname_length(entry.c_str()));
      size_t last_underscore = last_index_basename.find_last_of('_');
      if (last_underscore == std::string::npos) {
        err_msg.append(" invalid");
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               err_msg.c_str());
        error = LOG_INFO_INVALID;
        goto err;
      }
      // Extract the part after the last underscore
      std::string last_index_number =
          last_index_basename.substr(last_underscore + 1);
      if (!is_number(last_index_number.c_str(), &number, false)) {
        err_msg.append(" invalid");
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               err_msg.c_str());
        error = LOG_INFO_INVALID;
        goto err;
      }
      error = 0;
      m_mysql_clone_next_index_number = number;
      m_mysql_clone_next_index_number++;
    } else if (error == LOG_INFO_EOF) {
      // log index file is empty, the starting number is set to 1.
      m_mysql_clone_next_index_number = 1;
      error = 0;
    } else {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             "find innodb archive index file failed");
      goto err;
    }
  } else {
    m_mysql_clone_next_index_number++;
    error = 0;
  }
  snprintf(m_mysql_clone_name, sizeof(m_mysql_clone_name) - 1,
           CONSISTENT_INNODB_ARCHIVE_FORMAT,
           static_cast<my_off_t>(m_consensus_term),
           static_cast<my_off_t>(m_mysql_clone_next_index_number));
err:
  if (need_lock) {
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
  }
  return error;
}

/**
 * @brief Generate a new name for smartengine archive file.
 * The naming format is smartengine_{term}_{index_number}
 * or old format like smartengine_{index_number}
 * @return int
 */
int Consistent_archive::generate_se_new_name() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("generate_se_new_name"));
  LOG_INFO log_info{};
  int error = 1;
  uint64_t number = 0;
  bool need_lock = false;
  std::string err_msg;

  if (m_se_backup_next_index_number == 0) {
    need_lock = true;
    mysql_mutex_lock(&m_se_backup_index_lock);
    // init new smartengine index number from mysql clone index file.
    error = find_line_from_index(&log_info, NullS, ARCHIVE_SE);
    if (error == 0) {
      std::string last_entry{};
      do {
        last_entry.assign(log_info.log_file_name);
      } while (!(error = find_next_line_from_index(&log_info, ARCHIVE_SE)));
      if (error != LOG_INFO_EOF) {
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               "find smartengine archive index file failed");
        goto err;
      }
      err_msg.assign("last smartengine archive file");
      err_msg.append(last_entry);
      size_t idx = last_entry.find(".");
      std::string entry{last_entry};
      // Get rid of ".tar" or ".tar.gz" extension,
      // if new format like smartengine_000125_000010.tar.gz or
      // old format like smartengine_000010.tar.gz
      if (idx != std::string::npos) {
        entry = last_entry.substr(0, idx);
      } else if (entry.back() == FN_LIBCHAR) {
        // Get rid of last '/', if format like smartengine_000125_000010/
        // or old format like smartengine_000010/
        entry = entry.substr(0, entry.length() - 1);
      } else {
        err_msg.append(" invalid");
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               err_msg.c_str());
        error = LOG_INFO_INVALID;
        goto err;
      }
      // innodb_{term}_{index_number} or old format like innodb_{index_number}.
      std::string last_index_basename;
      last_index_basename.assign(entry.c_str() + dirname_length(entry.c_str()));
      size_t last_underscore = last_index_basename.find_last_of('_');
      if (last_underscore == std::string::npos) {
        err_msg.append(" invalid");
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               err_msg.c_str());
        error = LOG_INFO_INVALID;
        goto err;
      }
      // Extract the part after the last underscore
      std::string last_index_number =
          last_index_basename.substr(last_underscore + 1);
      if (!is_number(last_index_number.c_str(), &number, false)) {
        err_msg.append(" invalid");
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               err_msg.c_str());
        error = LOG_INFO_INVALID;
        goto err;
      }
      error = 0;
      m_se_backup_next_index_number = number;
      m_se_backup_next_index_number++;
    } else if (error == LOG_INFO_EOF) {
      // log index file is empty.
      m_se_backup_next_index_number = 1;
      error = 0;
    } else {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             "find smartengine archive index file failed");
      goto err;
    }
  } else {
    m_se_backup_next_index_number++;
    error = 0;
  }
  snprintf(m_se_backup_name, sizeof(m_se_backup_name) - 1,
           CONSISTENT_SE_ARCHIVE_FORMAT,
           static_cast<my_off_t>(m_consensus_term),
           static_cast<my_off_t>(m_se_backup_next_index_number));
err:
  if (need_lock) {
    mysql_mutex_unlock(&m_se_backup_index_lock);
  }
  return error;
}

/**
 * @brief Consistent archive thread run function.
 *
 */
void Consistent_archive::run() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("Consistent_archive::run"));
  set_thread_context();

  mysql_mutex_lock(&m_run_lock);
  m_thd_state.set_initialized();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
         "initialized");

  strmake(m_mysql_archive_dir, opt_consistent_snapshot_archive_dir,
          sizeof(m_mysql_archive_dir) - 1);
  convert_dirname(m_mysql_archive_dir, m_mysql_archive_dir, NullS);
  strmake(strmake(m_mysql_archive_data_dir, m_mysql_archive_dir,
                  sizeof(m_mysql_archive_data_dir) -
                      CONSISTENT_ARCHIVE_SUBDIR_LEN - 1),
          STRING_WITH_LEN(CONSISTENT_ARCHIVE_SUBDIR));
  convert_dirname(m_mysql_archive_data_dir, m_mysql_archive_data_dir, NullS);

  // Consistent snapshot persistent directory prefix.
  std::string snapshot_objectstore_path(opt_repo_objstore_id);
  snapshot_objectstore_path.append(FN_DIRSEP);
  snapshot_objectstore_path.append(opt_branch_objstore_id);
  snapshot_objectstore_path.append(FN_DIRSEP);
  snapshot_objectstore_path.append(CONSISTENT_ARCHIVE_SUBDIR);
  snapshot_objectstore_path.append(FN_DIRSEP);
  strmake(m_archive_dir, snapshot_objectstore_path.c_str(),
          sizeof(m_archive_dir) - 1);

  // get smartengine temp backup dir name.
  {
    // select @@SMARTENGINE_DATA_DIR
    String se_data_dir_tmp, *se_data_dir = nullptr;
    System_variable_tracker se_datadir_var_tracker =
        System_variable_tracker::make_tracker("SMARTENGINE_DATA_DIR");
    if (se_datadir_var_tracker.access_system_variable(m_thd)) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             "get smartengine parameter SMARTENGINE_DATA_DIR failed.");
      goto error;
    }
    Item_func_get_system_var *item =
        new Item_func_get_system_var(se_datadir_var_tracker, OPT_GLOBAL);
    item->fixed = true;
    se_data_dir = item->val_str(&se_data_dir_tmp);
    if (se_data_dir == nullptr) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             "get smartengine parameter SMARTENGINE_DATA_DIR failed.");
      goto error;
    }

    strmake(m_se_temp_backup_dir, se_data_dir->c_ptr(),
            sizeof(m_se_temp_backup_dir) - MYSQL_SE_TMP_BACKUP_DIR_LEN - 1);
    strmake(convert_dirname(m_se_temp_backup_dir, m_se_temp_backup_dir, NullS),
            STRING_WITH_LEN(MYSQL_SE_TMP_BACKUP_DIR));
    convert_dirname(m_se_temp_backup_dir, m_se_temp_backup_dir, NullS);
    // Free new Item_***.
    m_thd->free_items();
  }

  mysql_mutex_lock(&m_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG, "running");

  for (;;) {
    int ret = 0;
    bool abort = false;
    if (m_thd == nullptr || m_thd->killed) {
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             "m_thd->killed = true");
      break;
    }
    m_thd->clear_error();
    m_thd->get_stmt_da()->reset_diagnostics_area();

    // Check whether binlog archive is runing.
    Binlog_archive *binlog_archive = Binlog_archive::get_instance();
    mysql_mutex_t *binlog_archive_lock =
        binlog_archive->get_binlog_archive_lock();
    mysql_mutex_lock(binlog_archive_lock);
    // Wait binlog archive thread running
    if (!binlog_archive->is_thread_running()) {
      mysql_mutex_unlock(binlog_archive_lock);
      std::chrono::seconds wait_timeout = std::chrono::seconds{1};
      ret = wait_for_consistent_archive(wait_timeout, abort);
      assert(ret == 0 || is_timeout(ret));
      if (!abort) continue;

      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             "has been requested to abort.");
      break;
    }
    mysql_mutex_unlock(binlog_archive_lock);
    // Consistent snapshot has been paused.
    if (!opt_consistent_snapshot_archive) {
      std::chrono::seconds wait_timeout = std::chrono::seconds{1};
      ret = wait_for_consistent_archive(wait_timeout, abort);
      assert(ret == 0 || is_timeout(ret));
      if (!abort) continue;
      break;
    }
#ifdef WESQL_CLUSTER
    // Check whether consensus role is leader
    if (!is_consensus_replication_state_leader(m_consensus_term)) {
      std::chrono::seconds wait_timeout = std::chrono::seconds{1};
      ret = wait_for_consistent_archive(wait_timeout, abort);
      assert(ret == 0 || is_timeout(ret));
      if (!abort) continue;
      break;
    }
#endif
    // reload global options.
    m_innodb_tar_compression_mode = opt_consistent_snapshot_innodb_tar_mode;
    m_se_tar_compression_mode = opt_consistent_snapshot_se_tar_mode;

    // Every time a consistent snapshot is archived, reopens the index file.
    // Ensure that the local index files are consistent with the object store.
    mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
    close_index_file(ARCHIVE_MYSQL_INNODB);
    if (open_index_file(CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
                        CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
                        ARCHIVE_MYSQL_INNODB)) {
      mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
      goto error;
    }
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);

    mysql_mutex_lock(&m_se_backup_index_lock);
    close_index_file(ARCHIVE_SE);
    if (open_index_file(CONSISTENT_SE_ARCHIVE_INDEX_FILE,
                        CONSISTENT_SE_ARCHIVE_INDEX_FILE, ARCHIVE_SE)) {
      mysql_mutex_unlock(&m_se_backup_index_lock);
      goto error;
    }
    mysql_mutex_unlock(&m_se_backup_index_lock);

    mysql_mutex_lock(&m_consistent_index_lock);
    close_index_file(ARCHIVE_SNAPSHOT_FILE);
    if (open_index_file(CONSISTENT_SNAPSHOT_INDEX_FILE,
                        CONSISTENT_SNAPSHOT_INDEX_FILE,
                        ARCHIVE_SNAPSHOT_FILE)) {
      mysql_mutex_unlock(&m_consistent_index_lock);
      goto error;
    }
    mysql_mutex_unlock(&m_consistent_index_lock);

    // If the thread is killed, again archive last consistent snapshot, before
    // exit.
    mysql_mutex_lock(&m_run_lock);
    if (abort_archive) {
      mysql_mutex_unlock(&m_run_lock);
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             "snapshot archive again before stop thread.");
      archive_consistent_snapshot();
      break;
    }
    mysql_mutex_unlock(&m_run_lock);

    // First purge expired consistent snapshot,
    // before next archive consistent snapshot.
    // The slog generated when smartengine releases snapshot
    // is included in subsequent consistent snapshots as much as possible.
    if (opt_consistent_snapshot_expire_auto_purge) {
      char purge_time_str[iso8601_size] = {};
      auto err_val{0};
      std::string err_msg{};  // error message
      auto purge_time = calculate_auto_purge_lower_time_bound();
      convert_datetime_to_str(purge_time_str, purge_time);
      err_msg.assign("Auto purge snapshot expected to ");
      err_msg.append(purge_time_str);
      LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             err_msg.c_str());
      std::tie(err_val, err_msg) = purge_consistent_snapshot(
          purge_time_str, strlen(purge_time_str), true);
      if (err_val) {
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
               err_msg.c_str());
      }
    }

    if (archive_consistent_snapshot()) {
      m_atomic_archive_progress.store(STAGE_FAIL, std::memory_order_release);
    }
    // Wait for the next archive period.
    std::chrono::seconds timeout =
        std::chrono::seconds{opt_consistent_snapshot_archive_period};
    ret = wait_for_consistent_archive(timeout, abort);
    if (ret == 0) {
      // The current thread has been forcibly awakened.
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG,
             "forcefully wake up snapshot archive thread.");
    }
    assert(ret == 0 || is_timeout(ret));

    // Every time a consistent snapshot is archived, reopens the index file.
    // Ensure that the local index files are consistent with the object store.
    mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
    close_index_file(ARCHIVE_MYSQL_INNODB);
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);

    mysql_mutex_lock(&m_se_backup_index_lock);
    close_index_file(ARCHIVE_SE);
    mysql_mutex_unlock(&m_se_backup_index_lock);

    mysql_mutex_lock(&m_consistent_index_lock);
    close_index_file(ARCHIVE_SNAPSHOT_FILE);
    mysql_mutex_unlock(&m_consistent_index_lock);
  }

error:
  mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
  close_index_file(ARCHIVE_MYSQL_INNODB);
  mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
  mysql_mutex_lock(&m_se_backup_index_lock);
  close_index_file(ARCHIVE_SE);
  mysql_mutex_unlock(&m_se_backup_index_lock);
  mysql_mutex_lock(&m_consistent_index_lock);
  close_index_file(ARCHIVE_SNAPSHOT_FILE);
  mysql_mutex_unlock(&m_consistent_index_lock);

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG, "thread end");
  m_thd->clear_error();
  m_thd->release_resources();
  my_thread_end();
  mysql_mutex_lock(&m_run_lock);
  delete m_thd;
  m_thd = nullptr;
  abort_archive = true;
  m_thd_state.set_terminated();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);
  my_thread_exit(nullptr);
}

/**
 * @brief Wait for the consistent archive thread to start archiving.
 *
 * @param timeout
 * @return int
 */
int Consistent_archive::wait_for_consistent_archive(
    const std::chrono::seconds &timeout, bool &abort) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("wait_for_consistent_archive"));
  int error = 0;
  struct timespec ts;
  set_timespec(&ts, timeout.count());
  mysql_mutex_lock(&m_run_lock);
  if (abort_archive) {
    mysql_mutex_unlock(&m_run_lock);
    abort = true;
    return error;
  }
  error = mysql_cond_timedwait(&m_run_cond, &m_run_lock, &ts);
  mysql_mutex_unlock(&m_run_lock);
  return error;
}

/**
 * @brief Signal the consistent archive thread to start archiving.
 */
void Consistent_archive::signal_consistent_archive() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("signal_consistent_archive"));
  mysql_mutex_lock(&m_run_lock);
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);
  return;
}

/**
 * @brief Consistent archive mysql innodb, and smartengine metas and wals.
 * Refresh consistent_snapshot.index file.
 */
bool Consistent_archive::archive_consistent_snapshot() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_consistent_snapshot"));
  DBUG_EXECUTE_IF("fault_injection_persistent_snapshot", { return true; });
  THD *thd = m_thd;
  bool ret = false;
  std::string err_msg;
  int64 previous_time = 0;
  int64 current_time = 0;

  // Check whether any new binlogs have updated since the last consistent
  // snapshot.
  if (DBUG_EVALUATE_IF("force_snapshot_persistent", 0, 1) &&
      !mysql_binlog_has_updated()) {
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "persistent binlog has not changed.");
    return false;
  }

  m_atomic_archive_progress.store(STAGE_NONE, std::memory_order_release);
  memset(m_consistent_snapshot_local_time, 0,
         sizeof(m_consistent_snapshot_local_time));
  memset(m_binlog_file, 0, sizeof(m_binlog_file));
  memset(m_mysql_binlog_file, 0, sizeof(m_mysql_binlog_file));
  memset(m_mysql_clone_keyid, 0, sizeof(m_mysql_clone_keyid));
  memset(m_mysql_clone_name, 0, sizeof(m_mysql_clone_name));
  memset(m_mysql_innodb_clone_dir, 0, sizeof(m_mysql_innodb_clone_dir));
  memset(m_se_backup_keyid, 0, sizeof(m_se_backup_keyid));
  memset(m_se_backup_name, 0, sizeof(m_se_backup_name));
  memset(m_se_snapshot_dir, 0, sizeof(m_se_snapshot_dir));
  m_mysql_binlog_pos = 0;
  m_consensus_index = 0;
  m_se_snapshot_id = 0;
  m_consistent_snapshot_archive_start_ts = 0;
  m_consistent_snapshot_archive_end_ts = 0;
  m_innodb_clone_duration = 0;
  m_innodb_archive_duration = 0;
  m_se_backup_duration = 0;
  m_se_archive_duration = 0;
  m_wait_binlog_archive_duration = 0;

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, "archive snapshot.");
  /*
    Acquire shared backup lock to block concurrent backup. Acquire exclusive
    backup lock to block any concurrent DDL.
  */
  m_consistent_snapshot_archive_start_ts = time(nullptr);
  m_atomic_archive_progress.store(STAGE_ACQUIRE_BACKUP_LOCK,
                                  std::memory_order_release);
  if (DBUG_EVALUATE_IF("fault_injection_acquire_backup_lock", 1, 0) ||
      (ret = acquire_exclusive_backup_lock(
           thd, thd->variables.lock_wait_timeout, false))) {
    // MDL subsystem has to set an error in Diagnostics Area
    assert(thd->is_error());
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           thd->get_stmt_da()->message_text());
    ret = true;
    goto err;
  }

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, "acquire backup lock.");
  current_time = time(nullptr);
  // consisten archive smartengine wals and metas.
  m_atomic_archive_progress.store(STAGE_SMARTENGINE_SNAPSHOT,
                                  std::memory_order_release);
  if ((ret = archive_smartengine())) {
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "release backup lock.");
    release_backup_lock(thd);
    ret = true;
    goto err;
  }
  previous_time = current_time;
  current_time = time(nullptr);
  m_se_backup_duration = current_time - previous_time;

  err_msg.assign("smartengine backup binlog position ");
  err_msg.append(m_mysql_binlog_file);
  err_msg.append(":");
  err_msg.append(std::to_string(m_mysql_binlog_pos));
  err_msg.append(" previous backup binlog position ");
  err_msg.append(m_mysql_binlog_file_previous_snapshot);
  err_msg.append(":");
  err_msg.append(std::to_string(m_mysql_binlog_pos_previous_snapshot));
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());

  if (DBUG_EVALUATE_IF("force_snapshot_persistent", 0, 1)) {
    // Check whether smartengine hotbackup binlog had changed.
    if (compare_log_name(m_mysql_binlog_file_previous_snapshot,
                         m_mysql_binlog_file) == 0 &&
        m_mysql_binlog_pos_previous_snapshot == m_mysql_binlog_pos) {
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "smartengine backup binlog position no change.");
      LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "release backup lock.");
      release_backup_lock(thd);
      // release smartengine snapshot id.
      ret = true;
      goto err;
    }
  }

  // consistent archive mysql innodb
  m_atomic_archive_progress.store(STAGE_INNODB_CLONE,
                                  std::memory_order_release);
  if ((ret = achive_mysql_innodb())) {
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "release backup lock.");
    release_backup_lock(thd);
    ret = true;
    goto err;
  }
  previous_time = current_time;
  current_time = time(nullptr);
  m_innodb_clone_duration = current_time - previous_time;

  // Release shared backup lock
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, "release backup lock.");
  m_atomic_archive_progress.store(STAGE_RELEASE_BACKUP_LOCK,
                                  std::memory_order_release);
  release_backup_lock(thd);

  // wait binlog persistent.
  m_atomic_archive_progress.store(STAGE_WAIT_BINLOG_ARCHIVE,
                                  std::memory_order_release);
  if (archive_consistent_snapshot_binlog() == 1) {
    ret = true;
    goto err;
  }
  previous_time = current_time;
  current_time = time(nullptr);
  m_wait_binlog_archive_duration = current_time - previous_time;

  // persistent innodb clone data
  m_atomic_archive_progress.store(STAGE_INNODB_SNAPSHOT_ARCHIVE,
                                  std::memory_order_release);
  if (archive_innodb_data()) {
    ret = true;
    goto err;
  }
  previous_time = current_time;
  current_time = time(nullptr);
  m_innodb_archive_duration = current_time - previous_time;

  // persistent smartengine snapshot data
  m_atomic_archive_progress.store(STAGE_SMARTENGINE_SNAPSHOT_ARCHIVE,
                                  std::memory_order_release);
  if (archive_smartengine_wals_and_metas()) {
    ret = true;
    goto err;
  }
  previous_time = current_time;
  current_time = time(nullptr);
  m_se_archive_duration = current_time - previous_time;

  // write consistent snapshot index entry.
  m_atomic_archive_progress.store(STAGE_WRITE_CONSISTENT_SNAPSHOT_INDEX,
                                  std::memory_order_release);
  if ((ret = write_consistent_snapshot_file())) {
    ret = true;
    goto err;
  }

  strmake(m_mysql_binlog_file_previous_snapshot, m_mysql_binlog_file,
          sizeof(m_mysql_binlog_file_previous_snapshot) - 1);
  m_mysql_binlog_pos_previous_snapshot = m_mysql_binlog_pos;

  // Consistent snapshot persistent end.
  m_consistent_snapshot_archive_end_ts = time(nullptr);
  m_atomic_archive_progress.store(STAGE_END, std::memory_order_release);

  err_msg.assign("archive snapshot end: ");
  err_msg.append(m_consistent_snapshot_local_time);
  err_msg.append(" clone_keyid=");
  err_msg.append(m_mysql_clone_keyid);
  err_msg.append(" se_keyid=");
  err_msg.append(m_se_backup_keyid);
  err_msg.append(" binlog=");
  err_msg.append(m_binlog_file);
  err_msg.append(" end consensus index=");
  err_msg.append(std::to_string(m_consensus_index));
  err_msg.append(" mysql binlog=");
  err_msg.append(m_mysql_binlog_file);
  err_msg.append(" mysql end pos=");
  err_msg.append(std::to_string(m_mysql_binlog_pos));
  err_msg.append(" se snapshot=");
  err_msg.append(std::to_string(m_se_snapshot_id));
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());

err:
  archive_consistent_snapshot_cleanup(ret);
  return ret;
}

int Consistent_archive::archive_consistent_snapshot_binlog() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_consistent_snapshot_binlog"));
  DBUG_EXECUTE_IF("fault_injection_wait_persistent_binlog", { return 1; });
  THD *thd = m_thd;
  int ret = 0;
  std::string err_msg;

  // force rotate binlog, wait for binlog rotate complete
  if (mysql_bin_log.is_open() && m_mysql_binlog_file[0] != '\0') {
    int errval = 0;
    err_msg.assign("wait mysql binlog ");
    err_msg.append(m_mysql_binlog_file);
    err_msg.append(":");
    err_msg.append(std::to_string(m_mysql_binlog_pos));
    err_msg.append(" persistent");
#ifdef WESQL_CLUSTER
    uint64 consensus_index = 0;
    if (!NO_HOOK(binlog_manager)) {
      if (RUN_HOOK(
              binlog_manager, get_unique_index_from_pos,
              (m_mysql_binlog_file, m_mysql_binlog_pos, consensus_index))) {
        err_msg.append(" get consensus index using position failed.");
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG,
               err_msg.c_str());
        ret = 1;
        goto err;
      }
      // If m_mysql_binlog_pos is less than or equal to 251, the consensus_index
      // is not accurate, so it is necessary to retrieve the log/pos using
      // consensus index again.
      DBUG_EXECUTE_IF("check_consistent_snapshot_binlog_position", {
        if (consensus_index > 0) {
          char consensus_to_mysql_binlog[FN_REFLEN + 1] = {0};
          my_off_t consensus_to_log_pos = 0;
          if (RUN_HOOK(binlog_manager, get_pos_from_unique_index,
                       (consensus_index, consensus_to_mysql_binlog,
                        consensus_to_log_pos))) {
            err_msg.append(" get log position using consensus index failed: ");
            err_msg.append(std::to_string(consensus_index));
            LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG,
                   err_msg.c_str());
            ret = 1;
            goto err;
          }
          // diff mysqld binlog index entry name.
          if (compare_log_name(consensus_to_mysql_binlog,
                               m_mysql_binlog_file) != 0 ||
              consensus_to_log_pos != m_mysql_binlog_pos) {
            err_msg.append(" mysql binlog and position not match: consensus=");
            err_msg.append(std::to_string(consensus_index));
            err_msg.append(" using ");
            err_msg.append(consensus_to_mysql_binlog);
            err_msg.append(":");
            err_msg.append(std::to_string(consensus_to_log_pos));
            LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG,
                   err_msg.c_str());
          }
        }
      });
    }
    m_consensus_index = consensus_index;

    err_msg.append(" consensus_index=");
    err_msg.append(std::to_string(m_consensus_index));
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG,
           err_msg.c_str());

    // wait archive binlog complete.
    // And update mysql binlog filename to the archived binlog filename.
    if (m_consensus_index > 0) {
      errval = binlog_archive_wait_for_archive(
          thd, m_mysql_binlog_file, m_binlog_file, m_mysql_binlog_pos,
          m_consensus_index);
    } else {
      // no binlog.
      m_binlog_file[0] = '\0';
    }
#else
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG,
           err_msg.c_str());
    // wait archive binlog complete.
    // And update mysql binlog filename to the archived binlog filename.
    errval = binlog_archive_wait_for_archive(
        thd, m_mysql_binlog_file, m_binlog_file, m_mysql_binlog_pos, 0);
#endif
    if (errval != 0) {
      err_msg.append(" failed.");
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG,
             err_msg.c_str());
      ret = 1;
      goto err;
    }
    err_msg.append(" to persistent ");
    err_msg.append(m_binlog_file);
    err_msg.append(" end.");
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG,
           err_msg.c_str());
  } else {
    err_msg.assign("no persistent binlog: ");
    if (m_mysql_binlog_file[0] != '\0') {
      err_msg.append(m_mysql_binlog_file);
    }
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG,
           err_msg.c_str());
  }

err:
  return ret;
}

int Consistent_archive::archive_consistent_snapshot_cleanup(bool failed) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_consistent_snapshot_cleanup"));
  DBUG_EXECUTE_IF("fault_injection_consistent_snapshot_cleanup", { return 1; });
  THD *thd = m_thd;
  plugin_ref plugin;
  remove_file(m_mysql_innodb_clone_dir);
  plugin = my_plugin_lock_by_name(nullptr, {STRING_WITH_LEN("smartengine")},
                                  MYSQL_STORAGE_ENGINE_PLUGIN);
  if (plugin == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "smartengine plugin is not loaded");
    return 1;
  }
  handlerton *hton = plugin_data<handlerton *>(plugin);
  // If release failed, already return success, only report warning.
  hton->cleanup_tmp_backup_dir(thd);
  plugin_unlock(nullptr, plugin);

  if (failed && m_se_snapshot_id > 0) {
    release_se_snapshot(m_se_snapshot_id);
    m_se_snapshot_id = 0;
  }
  return 0;
}

/**
 * @brief Archive mysql innodb by local clone.
 * Acquired backup lock before archive innodb.
 */
bool Consistent_archive::achive_mysql_innodb() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("achive_mysql_innodb"));
  DBUG_EXECUTE_IF("fault_injection_innodb_archive", { return true; });
  THD *thd = m_thd;
  plugin_ref plugin{};
  std::string err_msg;

  // generate next innodb clone dir name
  if (generate_innodb_new_name()) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           "generate new persistet name failed.");
    return true;
  }
  strmake(strmake(m_mysql_innodb_clone_dir, m_mysql_archive_data_dir,
                  FN_REFLEN - strlen(m_mysql_clone_name)),
          m_mysql_clone_name, strlen(m_mysql_clone_name));

  Clone_handler *clone = clone_plugin_lock(thd, &plugin);
  if (clone == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           "clone plugin load failed");
    return true;
  }

  err_msg.assign("local innodb clone: ");
  err_msg.append(m_mysql_innodb_clone_dir);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
         err_msg.c_str());
  // clone mysql innodb data to local dir.
  auto err = clone->clone_local(thd, m_mysql_innodb_clone_dir);
  clone_plugin_unlock(thd, plugin);
#ifdef HAVE_PSI_STATEMENT_INTERFACE
  MYSQL_END_STATEMENT(thd->m_statement_psi, thd->get_stmt_da());
  thd->m_statement_psi = nullptr;
  thd->m_digest = nullptr;
#endif
  if (err != 0) {
    err_msg.append(" failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           err_msg.c_str());
    return true;
  }
  err_msg.append(" end");
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
         err_msg.c_str());

  return false;
}

int Consistent_archive::archive_innodb_data() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_innodb_data"));
  DBUG_EXECUTE_IF("fault_injection_persistent_innodb_snapshot", { return 1; });
  std::string err_msg;
  int err = 0;

  // tar mysql innod data dir and
  // upload innodb_archive_000001.tar.gz to s3.
  // If subsequent errors, recycle the archive file by
  // consisent_archive_purge()
  std::stringstream cmd;
  std::string clone_name;
  std::string clone_full_name;
  std::string clone_keyid;
  clone_name.assign(m_mysql_innodb_clone_dir);
  clone_keyid.assign(m_archive_dir);
  clone_full_name.assign(m_mysql_clone_name);

  if (m_innodb_tar_compression_mode != CONSISTENT_SNAPSHOT_NO_TAR) {
    if (m_innodb_tar_compression_mode == CONSISTENT_SNAPSHOT_TAR_COMMPRESSION) {
      clone_name.append(CONSISTENT_TAR_GZ_SUFFIX);
      clone_full_name.append(CONSISTENT_TAR_GZ_SUFFIX);
      cmd << "tar -czf " << clone_name << " ";
    } else {
      clone_name.append(CONSISTENT_TAR_SUFFIX);
      clone_full_name.append(CONSISTENT_TAR_SUFFIX);
      cmd << "tar -cf " << clone_name << " ";
    }
    cmd << " --absolute-names --transform 's|^" << m_mysql_archive_data_dir
        << "||' ";
    cmd << m_mysql_innodb_clone_dir;

    err_msg.assign("tar innodb clone dir begin: ");
    err_msg.append(cmd.str());
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           err_msg.c_str());
    if ((err = system(cmd.str().c_str())) != 0) {
      err_msg.append(" error=");
      err_msg.append(std::to_string(err));
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
             err_msg.c_str());
      // remove local innodb_archive_000001/ dir
      remove_file(m_mysql_innodb_clone_dir);
      return 1;
    }
    clone_keyid.append(clone_full_name);
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           "tar innodb clone dir end.");
    err_msg.assign("persistent innodb clone to object store begin: ");
    err_msg.append(" keyid=");
    err_msg.append(clone_keyid);
    err_msg.append(" file=");
    err_msg.append(clone_name);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           err_msg.c_str());
    if (my_chmod(
            clone_name.c_str(),
            USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
            MYF(0))) {
      err_msg.append(" failed to chmod: ");
      err_msg.append(clone_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
             err_msg.c_str());
      remove_file(clone_name);
      remove_file(m_mysql_innodb_clone_dir);
      return 1;
    }
    // upload innodb_archive_000001.tar.gz to object store.
    objstore::Status ss = snapshot_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), clone_keyid, clone_name);

    // remove local innodb_archive_000001.tar.gz
    remove_file(clone_name);
    // remove local innodb_archive_000001/ dir
    remove_file(m_mysql_innodb_clone_dir);

    if (DBUG_EVALUATE_IF("fault_injection_put_innodb_snapshot_to_objstore", 1,
                         0) ||
        !ss.is_succ()) {
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
             err_msg.c_str());
      return 1;
    }
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           "persistent innodb clone to object store end.");
  } else {
    assert(m_innodb_tar_compression_mode == CONSISTENT_SNAPSHOT_NO_TAR);

    clone_keyid.append(clone_full_name);
    clone_keyid.append(FN_DIRSEP);
    // If a directory with the same name already exists, it should be deleted
    // first, as it is leftover garbage data from a previous failed persistence
    // attempt.
    err_msg.assign(
        "delete persistent innodb clone same name already exists from object "
        "store:");
    err_msg.append(" keyid=");
    err_msg.append(clone_keyid);
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           err_msg.c_str());
    snapshot_objstore->delete_directory(std::string_view(opt_objstore_bucket),
                                        clone_keyid);

    // Directly persistent clone dir innodb_archive_000001/ to objstore.
    err_msg.assign("directly persistent innodb clone to object store begin: ");
    err_msg.append(" keyid=");
    err_msg.append(clone_keyid);
    err_msg.append(" dir=");
    err_msg.append(clone_name);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           err_msg.c_str());
    objstore::Status ss = snapshot_objstore->put_objects_from_dir(
        clone_name, std::string_view(opt_objstore_bucket), clone_keyid);
    remove_file(clone_name);
    if (DBUG_EVALUATE_IF("fault_injection_put_innodb_snapshot_to_objstore", 1,
                         0) ||
        !ss.is_succ()) {
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
             err_msg.c_str());
      return 1;
    }
    err_msg.assign("directly persistent innodb clone to object store end.");
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           err_msg.c_str());
  }

  strmake(m_mysql_clone_keyid, clone_keyid.c_str(),
          sizeof(m_mysql_clone_keyid) - 1);

  // append new archive name to local index file 'innodb_archive.index'.
  // If subsequent errors, recover local index file by run()->while reopen
  // index.If an error occurs during add_line_to_index, a garbage file
  // innodb_archive.000000.tar will exist on the object store.
  err_msg.assign("persistent update innodb_archhive.index ");
  err_msg.append(m_mysql_clone_keyid);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
         err_msg.c_str());
  mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
  if (DBUG_EVALUATE_IF("fault_injection_persistent_innodb_archive_index", 1,
                       0) ||
      add_line_to_index(m_mysql_clone_keyid, ARCHIVE_MYSQL_INNODB)) {
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
    err_msg.append(" failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
           err_msg.c_str());
    return 1;
  }
  mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
  err_msg.append(" end");
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG,
         err_msg.c_str());
  return 0;
}
/**
 * @brief Archive smartengine wals and metas.
 * Acquired backup lock before archive smartengine.
 */
bool Consistent_archive::archive_smartengine() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_smartengine"));
  DBUG_EXECUTE_IF("fault_injection_smartengine_archive", { return true; });
  THD *thd = m_thd;
  uint64_t backup_snapshot_id = 0;
  std::string binlog_file;
  uint64_t binlog_file_offset = 0;
  plugin_ref plugin{};

  m_se_snapshot_id = 0;

  plugin = my_plugin_lock_by_name(nullptr, {STRING_WITH_LEN("smartengine")},
                                  MYSQL_STORAGE_ENGINE_PLUGIN);
  if (plugin == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "smartengine plugin load failed");
    return true;
  }
  handlerton *hton = plugin_data<handlerton *>(plugin);

  LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
         "do smartengine checkpoint begin.");
  if (opt_consistent_snapshot_smartengine_backup_checkpoint &&
      hton->checkpoint(thd)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "do smartengine checkpoint failed.");
    plugin_unlock(nullptr, plugin);
    return true;
  }

  LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
         "do smartengine checkpoint end.");

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
         "create smartengine backup begin.");
  if (hton->create_backup_snapshot(thd, &backup_snapshot_id, binlog_file,
                                   &binlog_file_offset)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "create smartengine backup failed.");
    plugin_unlock(nullptr, plugin);
    return true;
  }
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
         "create smartengine backup end.");

  plugin_unlock(nullptr, plugin);

  // When check whether binlog is archived, will update mysql binlog filename
  // to the archived binlog filename.
  const char *file_name =
      binlog_file.c_str() + dirname_length(binlog_file.c_str());
  // make binlog index entry name.
  mysql_bin_log.make_log_name(m_mysql_binlog_file, file_name);
  m_mysql_binlog_pos = binlog_file_offset;
  m_se_snapshot_id = backup_snapshot_id;

  return false;
}

/**
 * @brief  smartengine backup consistened point,
 * that is, the instance should restore to this point.
 * SmartEngine's data include base part in sst file and incremental part in
 * memtable, so the backup consistened point include manifest file position
 * and wal file position.
 * @return true if success. or false if fail.
 */

/**
 * @brief Release smartengine snapshot.
 * @return true if success. or false if fail.
 */
bool Consistent_archive::release_se_snapshot(uint64_t backup_snapshot_id) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("release_se_snapshot"));
  THD *thd = m_thd;
  plugin_ref plugin;
  std::string err_msg;

  if (backup_snapshot_id == 0) return false;

  plugin = my_plugin_lock_by_name(nullptr, {STRING_WITH_LEN("smartengine")},
                                  MYSQL_STORAGE_ENGINE_PLUGIN);
  if (plugin == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "smartengine plugin is not loaded");
    return true;
  }
  err_msg.assign("release smartengine snapshot: ");
  err_msg.append(std::to_string(backup_snapshot_id));
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
         err_msg.c_str());
  handlerton *hton = plugin_data<handlerton *>(plugin);
  // If release failed, already return success, only report warning.
  if (hton->release_backup_snapshot(thd, backup_snapshot_id)) {
    err_msg.assign("release smartengine snapshot failed, maybe not exists: ");
    err_msg.append(std::to_string(backup_snapshot_id));
    LogErr(WARNING_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           err_msg.c_str());
  }
  plugin_unlock(nullptr, plugin);
  return false;
}

/**
 * @brief Copy file from smartengine data directory to mysql archive
 * directory.
 *
 */
bool Consistent_archive::archive_smartengine_wals_and_metas() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_smartengine_wals_and_metas"));
  DBUG_EXECUTE_IF("fault_injection_persistent_smartengine_copy_wals_and_metas",
                  { return true; });
  std::string err_msg;
  std::string se_backup_full_name;

  // generate next smartengine backup dir name
  if (generate_se_new_name()) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "generate smartengine new name failed.");
    return true;
  }
  strmake(strmake(m_se_snapshot_dir, m_mysql_archive_data_dir,
                  FN_REFLEN - strlen(m_se_backup_name)),
          m_se_backup_name, strlen(m_se_backup_name));

  se_backup_full_name.assign(m_se_backup_name);

  // If the snapshot data does not need to be tarred but is directly persisted
  // to S3, there is no need to copy it to a temporary directory.
  if (m_se_tar_compression_mode == CONSISTENT_SNAPSHOT_NO_TAR) {
    std::string se_keyid_prefix;
    // key = prefix + se_backup_name + "/"
    se_keyid_prefix.assign(m_archive_dir);
    se_keyid_prefix.append(se_backup_full_name);
    se_keyid_prefix.append(FN_DIRSEP);

    err_msg.assign(
        "delete persistent smartengine same name already exists from object "
        "store:");
    err_msg.append(" keyid=");
    err_msg.append(se_keyid_prefix);
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           err_msg.c_str());
    snapshot_objstore->delete_directory(std::string_view(opt_objstore_bucket),
                                        se_keyid_prefix);

    err_msg.assign("directly persistent smartengine to object store begin: ");
    err_msg.append(m_se_backup_name);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           err_msg.c_str());
    strmake(m_se_backup_keyid, se_keyid_prefix.c_str(),
            sizeof(m_se_backup_keyid) - 1);
    // It is necessary to persist `se_archive_000001/` as a separate key;
    // otherwise, `list_object("se_archive_")` will not be able to find
    // `se_archive_000001/`.
    objstore::Status ss = snapshot_objstore->put_object(
        std::string_view(opt_objstore_bucket),
        std::string_view(m_se_backup_keyid), std::string_view(""));

    if (!ss.is_succ()) {
      err_msg.assign(
          "persistent smartengine archive dir to object store failed: ");
      err_msg.append("key=");
      err_msg.append(m_se_backup_keyid);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      return true;
    }

    std::string se_snapshot_meta_dir;
    se_snapshot_meta_dir.assign(m_se_backup_keyid);
    se_snapshot_meta_dir.append(CONSISTENT_SE_ARCHIVE_META_SUBDIR);
    se_snapshot_meta_dir.append(FN_DIRSEP);
    ss = snapshot_objstore->put_object(std::string_view(opt_objstore_bucket),
                                       std::string_view(se_snapshot_meta_dir),
                                       std::string_view(""));
    if (!ss.is_succ()) {
      err_msg.assign(
          "persistent smartengine archive meta dir to object store failed: ");
      err_msg.append("key=");
      err_msg.append(se_snapshot_meta_dir);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      return true;
    }

    std::string se_snapshot_wal_dir;
    se_snapshot_wal_dir.assign(m_se_backup_keyid);
    se_snapshot_wal_dir.append(CONSISTENT_SE_ARCHIVE_WAL_SUBDIR);
    se_snapshot_wal_dir.append(FN_DIRSEP);
    ss = snapshot_objstore->put_object(std::string_view(opt_objstore_bucket),
                                       std::string_view(se_snapshot_wal_dir),
                                       std::string_view(""));
    if (!ss.is_succ()) {
      err_msg.assign(
          "persistent smartengine archive wal dir to object store failed: ");
      err_msg.append("key=");
      err_msg.append(se_snapshot_wal_dir);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      return true;
    }
  } else {
    std::string se_snapshot_wal_dir;
    std::string se_snapshot_meta_dir;
    char se_snapshot[FN_REFLEN + 1] = {0};
    strmake(se_snapshot, m_se_snapshot_dir, sizeof(se_snapshot) - 1);
    convert_dirname(se_snapshot, se_snapshot, NullS);

    if (my_mkdir(se_snapshot, 0777, MYF(0)) < 0) {
      err_msg.assign("Failed to create smartengine archive local directory: ");
      err_msg.append(se_snapshot);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      return true;
    }

    se_snapshot_wal_dir.assign(se_snapshot);
    se_snapshot_wal_dir.append(CONSISTENT_SE_ARCHIVE_WAL_SUBDIR);
    if (my_mkdir(se_snapshot_wal_dir.c_str(), 0777, MYF(0)) < 0) {
      err_msg.assign(
          "Failed to create smartengine archive wal local directory: ");
      err_msg.append(se_snapshot_wal_dir);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      return true;
    }
    se_snapshot_meta_dir.assign(se_snapshot);
    se_snapshot_meta_dir.append(CONSISTENT_SE_ARCHIVE_META_SUBDIR);
    if (my_mkdir(se_snapshot_meta_dir.c_str(), 0777, MYF(0)) < 0) {
      err_msg.assign(
          "Failed to create smartengine archive meta local directory: ");
      err_msg.append(se_snapshot_meta_dir);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      return true;
    }
    err_msg.assign("local copy smartengine backup begin: ");
    err_msg.append(se_snapshot);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           err_msg.c_str());
  }

  // Iterate through the smartegine backup temp directory
  // and persist the WAL and Meta to S3.
  MY_DIR *dir_info =
      my_dir(m_se_temp_backup_dir, MYF(MY_DONT_SORT | MY_WANT_STAT));
  if (!dir_info) {
    err_msg.assign("Failed to open smartengine temp backup directory: ");
    err_msg.append(m_se_temp_backup_dir);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           err_msg.c_str());
    remove_file(m_se_snapshot_dir);
    return true;
  }
  // copy files in m_se_temp_backup_dir to local m_se_snapshot_dir.
  // Or directly persistent smartengine to object store.
  for (uint i = 0; i < dir_info->number_off_files; i++) {
    FILEINFO *file = dir_info->dir_entry + i;
    char ds_source[FN_REFLEN + 1] = {0};
    std::string se_destination;
    std::string se_wal_keyid;
    std::string se_meta_keyid;
    // wal_key = prefix/wal/
    se_wal_keyid.assign(m_se_backup_keyid);
    se_wal_keyid.append(CONSISTENT_SE_ARCHIVE_WAL_SUBDIR);
    se_wal_keyid.append(FN_DIRSEP);
    // meta_key = prefix/meta/
    se_meta_keyid.assign(m_se_backup_keyid);
    se_meta_keyid.append(CONSISTENT_SE_ARCHIVE_META_SUBDIR);
    se_meta_keyid.append(FN_DIRSEP);

    // skip . and .. directories
    if (0 == strcmp(file->name, ".") || 0 == strcmp(file->name, "..")) {
      /**ignore special directories*/
      continue;
    }
    // skip subdir.
    if (MY_S_ISDIR(file->mystat->st_mode)) continue;

    // skip *.sst files
    if (file_has_suffix(MYSQL_SE_DATA_FILE_SUFFIX, file->name)) continue;

    strmake(ds_source, m_se_temp_backup_dir,
            sizeof(ds_source) - strlen(file->name) - 1);
    strmake(convert_dirname(ds_source, ds_source, NullS), file->name,
            strlen(file->name));

    // Directly persistent file to object store, not tar.
    if (m_se_tar_compression_mode == CONSISTENT_SNAPSHOT_NO_TAR) {
      std::string se_keyid;
      if (file_has_suffix(MYSQL_SE_WAL_FILE_SUFFIX, file->name)) {
        se_keyid.assign(se_wal_keyid);
      } else {
        se_keyid.assign(se_meta_keyid);
      }
      se_keyid.append(file->name);

      objstore::Status ss = snapshot_objstore->put_object_from_file(
          std::string_view(opt_objstore_bucket), se_keyid,
          std::string_view(ds_source));

      if (DBUG_EVALUATE_IF(
              "fault_injection_put_smartengine_snapshot_to_objstore", 1, 0) ||
          !ss.is_succ()) {
        err_msg.assign(
            "persistent smartengine backup file to object store failed: ");
        err_msg.append("key=");
        err_msg.append(se_keyid);
        err_msg.append(" file=");
        err_msg.append(ds_source);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
               err_msg.c_str());
        my_dirend(dir_info);
        return true;
      }
    } else {
      char ds_destination[FN_REFLEN + 1] = {0};
      // If tar smartengine snapshot need or persistent to local.
      strmake(ds_destination, m_se_snapshot_dir, sizeof(ds_destination) - 1);
      convert_dirname(ds_destination, ds_destination, NullS);

      if (file_has_suffix(MYSQL_SE_WAL_FILE_SUFFIX, file->name)) {
        se_destination.assign(ds_destination);
        se_destination.append(CONSISTENT_SE_ARCHIVE_WAL_SUBDIR);
        se_destination.append(FN_DIRSEP);
      } else {
        se_destination.assign(ds_destination);
        se_destination.append(CONSISTENT_SE_ARCHIVE_META_SUBDIR);
        se_destination.append(FN_DIRSEP);
      }
      se_destination.append(file->name);

      DBUG_PRINT("info", ("Copying file: %s to local %s", ds_source,
                          se_destination.c_str()));

      int ret = my_copy(ds_source, se_destination.c_str(),
                        MYF(MY_HOLD_ORIGINAL_MODES));
      if (ret) {
        err_msg.assign("local copy smartengine file failed: ");
        err_msg.append(ds_source);
        err_msg.append(" to ");
        err_msg.append(se_destination);
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
               err_msg.c_str());
        my_dirend(dir_info);
        // remove local garbage files
        remove_file(m_se_snapshot_dir);
        return true;
      }
    }
  }
  my_dirend(dir_info);
  if (m_se_tar_compression_mode == CONSISTENT_SNAPSHOT_NO_TAR) {
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "directly persistent smartengine backup to object store end.");
  } else {
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "local copy smartengine backup end.");
  }

  // persistent smartengine backup tar package to object store
  if (m_se_tar_compression_mode != CONSISTENT_SNAPSHOT_NO_TAR) {
    int err = 0;
    std::stringstream cmd;
    std::string se_tar_name;
    std::string se_keyid;

    se_tar_name.assign(m_se_snapshot_dir);
    // append prefix.
    se_keyid.assign(m_archive_dir);

    if (m_se_tar_compression_mode == CONSISTENT_SNAPSHOT_TAR_COMMPRESSION) {
      se_tar_name.append(CONSISTENT_TAR_GZ_SUFFIX);
      se_backup_full_name.append(CONSISTENT_TAR_GZ_SUFFIX);
      cmd << "tar -czf " << se_tar_name << " ";
    } else {
      se_tar_name.append(CONSISTENT_TAR_SUFFIX);
      se_backup_full_name.append(CONSISTENT_TAR_SUFFIX);
      cmd << "tar -cf " << se_tar_name << " ";
    }
    cmd << " --absolute-names --transform 's|^" << m_mysql_archive_data_dir
        << "||' ";
    cmd << m_se_snapshot_dir;

    err_msg.assign("tar smartengine backup begin: ");
    err_msg.append(cmd.str());
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           err_msg.c_str());
    if ((err = system(cmd.str().c_str())) != 0) {
      err_msg.assign("tar smartengine backup failed");
      err_msg.append(cmd.str());
      err_msg.append(" error=");
      err_msg.append(std::to_string(err));
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      return true;
    }
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "tar smartengine backup end.");

    se_keyid.append(se_backup_full_name);

    err_msg.assign(
        "persistent smartengine backup package to object store begin: ");
    err_msg.append(" keyid=");
    err_msg.append(se_keyid);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           err_msg.c_str());

    if (my_chmod(
            se_tar_name.c_str(),
            USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
            MYF(0))) {
      err_msg.append(" failed to chmod: ");
      err_msg.append(se_tar_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      remove_file(se_tar_name);
      remove_file(m_se_snapshot_dir);
      return true;
    }

    // upload se_backup000000.tar to s3
    objstore::Status ss = snapshot_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), se_keyid, se_tar_name);

    // remove local se_archive_000001.tar and se_archive_000001/
    remove_file(se_tar_name);
    remove_file(m_se_snapshot_dir);
    strmake(m_se_backup_keyid, se_keyid.c_str(), sizeof(m_se_backup_keyid) - 1);

    if (DBUG_EVALUATE_IF("fault_injection_put_smartengine_snapshot_to_objstore",
                         1, 0) ||
        !ss.is_succ()) {
      err_msg.assign(
          "persistent smartengine backup package to object store failed: ");
      err_msg.append("key=");
      err_msg.append(se_keyid);
      err_msg.append(" file=");
      err_msg.append(se_tar_name);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
             err_msg.c_str());
      return true;
    }
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           "persistent smartengine backup package to object store end.");
  }
  // add smartengine backup name to se_backup.index,
  // And upload se_backup.index to s3.
  err_msg.assign("persistent update smartengine.index ");
  err_msg.append(m_se_backup_keyid);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
         err_msg.c_str());
  mysql_mutex_lock(&m_se_backup_index_lock);
  if (DBUG_EVALUATE_IF("fault_injection_persistent_smartengine_index_file", 1,
                       0) ||
      add_line_to_index((const char *)m_se_backup_keyid, ARCHIVE_SE)) {
    mysql_mutex_unlock(&m_se_backup_index_lock);
    err_msg.append(" failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
           err_msg.c_str());
    return true;
  }
  mysql_mutex_unlock(&m_se_backup_index_lock);
  err_msg.append(" end");
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG,
         err_msg.c_str());
  return false;
}

/**
 * @brief Write consistent snapshot file, when archive finished.
 * Only a consistent snapshot file is written to the archive directory.
 * Instance can restore to this point. Or recycle mysql innodb clone and
 * smartengine backup before this point.
 */
bool Consistent_archive::write_consistent_snapshot_file() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("write_consistent_snapshot_file"));
  DBUG_EXECUTE_IF("fault_injection_persistent_consistent_snapshot_index",
                  { return true; });
  std::string snapshot_info;
  time_t current_time = time(nullptr);
  std::string err_msg;
  convert_datetime_to_str(m_consistent_snapshot_local_time, current_time);
  // append consistent_snapshot.index
  snapshot_info.assign(m_consistent_snapshot_local_time);
  snapshot_info.append("|");
  snapshot_info.append(m_mysql_clone_keyid);
  snapshot_info.append("|");
  snapshot_info.append(m_se_backup_keyid);
  snapshot_info.append("|");
  snapshot_info.append(m_binlog_file);
  snapshot_info.append("|");
  snapshot_info.append(std::to_string(m_mysql_binlog_pos));
  snapshot_info.append("|");
  snapshot_info.append(std::to_string(m_consensus_index));
  snapshot_info.append("|");
  snapshot_info.append(std::to_string(m_se_snapshot_id));

  err_msg.assign("persistent update index begin: ");
  err_msg.append(CONSISTENT_SNAPSHOT_INDEX_FILE);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  mysql_mutex_lock(&m_consistent_index_lock);
  if (DBUG_EVALUATE_IF("fault_injection_persistent_consistent_index_file", 1,
                       0) ||
      add_line_to_index((const char *)snapshot_info.c_str(),
                        ARCHIVE_SNAPSHOT_FILE)) {
    mysql_mutex_unlock(&m_consistent_index_lock);
    err_msg.assign("persistent update snapshot.index failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }
  mysql_mutex_unlock(&m_consistent_index_lock);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
         "persistent update index end.");
  return false;
err:
  return true;
}

/**
 * @brief Check whether any new binlogs have updated since the last consistent
 * snapshot.
 *
 * @return true , updated.
 * @return false , not updated.
 */
bool Consistent_archive::mysql_binlog_has_updated() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("mysql_binlog_pos_has_advanced"));
  DBUG_EXECUTE_IF("force_consistent_snapshot_mysql_binlog_updated",
                  { return true; });

  // If previous snapshot binlog file is empty, also considered as updated.
  if ((m_mysql_binlog_file_previous_snapshot[0] == '\0') ||
      (m_mysql_binlog_pos_previous_snapshot == 0))
    return true;
  // If mysql binlog not opened, also considered as updated.
  if (mysql_bin_log.is_open()) {
    // If previous snapshot binlog file is not active, also considered as
    // updated. If binlog file is active, but the end position is greater than
    // the previous snapshot position, also considered as updated.
    if (mysql_bin_log.get_binlog_end_pos() >
            m_mysql_binlog_pos_previous_snapshot ||
        !mysql_bin_log.is_active(m_mysql_binlog_file_previous_snapshot)) {
      return true;
    }
    return false;
  }
  return true;
}

int Consistent_archive::show_consistent_snapshot_task_info(
    Consistent_snapshot_task_info &task_info) {
  DBUG_TRACE;
  Consistent_snapshot_archive_progress archive_progress =
      m_atomic_archive_progress.load(std::memory_order_acquire);
  strmake(task_info.archive_progress,
          convert_archive_progress_to_str(archive_progress),
          sizeof(task_info.archive_progress) - 1);
  convert_datetime_to_str(task_info.consistent_snapshot_archive_start_ts,
                          m_consistent_snapshot_archive_start_ts);
  convert_datetime_to_str(task_info.consistent_snapshot_archive_end_ts,
                          m_consistent_snapshot_archive_end_ts);
  task_info.consensus_term = m_consensus_term;
  strmake(task_info.mysql_clone_name, m_mysql_clone_name,
          sizeof(task_info.mysql_clone_name) - 1);
  task_info.innodb_clone_duration = m_innodb_clone_duration;
  task_info.innodb_archive_duration = m_innodb_archive_duration;
  strmake(task_info.se_backup_name, m_se_backup_name,
          sizeof(task_info.se_backup_name) - 1);
  task_info.se_snapshot_id = m_se_snapshot_id;
  task_info.se_backup_duration = m_se_backup_duration;
  task_info.se_archive_duration = m_se_archive_duration;
  strmake(task_info.binlog_file, m_binlog_file,
          sizeof(task_info.binlog_file) - 1);
  strmake(task_info.mysql_binlog_file, m_mysql_binlog_file,
          sizeof(task_info.mysql_binlog_file) - 1);
  task_info.mysql_binlog_pos = m_mysql_binlog_pos;
  task_info.consensus_index = m_consensus_index;
  task_info.wait_binlog_archive_duration = m_wait_binlog_archive_duration;
  return 0;
}

/**
 * @brief Get the last persistent index file.
 * The persisted index file is based on multiple versions of the
 * consensus term, so the index file with the highest consensus term
 * should be obtained as the initial one.
 * @param last_binlog_index
 * @return int
 * @note Ensuring compatibility with both multi-version (snapshot.000001.index)
 * and single-version (snapshot.index) formats.
 */
int Consistent_archive::fetch_last_persistent_index_file(
    std::string &last_index, Archive_type arch_type) {
  std::vector<objstore::ObjectMeta> objects;
  bool finished = false;
  std::string start_after;
  std::string index_prefix;
  std::string old_index_keyid;
  std::string err_msg;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_prefix.assign(m_archive_dir);
    index_prefix.append(CONSISTENT_INNODB_INDEX_FILE_BASENAME);
    old_index_keyid.assign(m_archive_dir);
    old_index_keyid.append(CONSISTENT_INNODB_ARCHIVE_INDEX_FILE);
  } else if (arch_type == ARCHIVE_SE) {
    index_prefix.assign(m_archive_dir);
    index_prefix.append(CONSISTENT_SE_INDEX_FILE_BASENAME);
    old_index_keyid.assign(m_archive_dir);
    old_index_keyid.append(CONSISTENT_SE_ARCHIVE_INDEX_FILE);
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    index_prefix.assign(m_archive_dir);
    index_prefix.append(CONSISTENT_SNAPSHOT_INDEX_FILE_BASENAME);
    old_index_keyid.assign(m_archive_dir);
    old_index_keyid.append(CONSISTENT_SNAPSHOT_INDEX_FILE);
  }
  /*
   snapshot.000001.index
   snapshot.000002.index
   snapshot.000003.index
   snapshot.index
   If exists multi-version index file, should get the latest one multi-version
   index file snapshot.000003.index. Otherwis, get snapshot.index. If exists
   snapshot.index, should locate the last one in list. If only the
   single-version file (snapshot.index) exists, it is selected instead..
  */
  do {
    std::vector<objstore::ObjectMeta> tmp_objects;
    objstore::Status ss = snapshot_objstore->list_object(
        std::string_view(opt_objstore_bucket), index_prefix, false, start_after,
        finished, tmp_objects);
    if (!ss.is_succ()) {
      err_msg.assign("list persistent index file failed: ");
      err_msg.append(index_prefix);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      return 1;
    }
    objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
  } while (finished == false);
  // if no persistent binlog.index, return.
  if (objects.empty()) {
    err_msg.assign("no persistent index file found: ");
    err_msg.append(index_prefix);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return 0;
  }
  // Initially, the last_index is assigned to the key of the last object in the
  // objects vector
  last_index.assign((objects.back()).key);
  // If the last file is the single-version file (snapshot.index), and there are
  // other files available, it skips this file and assigns last_index to the
  // second-last object in the list. This ensures compatibility by preferring
  // multi-version files when available.
  if (old_index_keyid.compare(objects.back().key) == 0 && objects.size() > 1) {
    last_index.assign((objects.end() - 2)->key);
  }
  err_msg.assign("fetch last persistent index file: ");
  err_msg.append(last_index);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  return 0;
}

/**
  Open a (new) crash safe index file.

  @note
    The crash safe index file is a special file
    used for guaranteeing index file crash safe.
  @retval
    0   ok
  @retval
    1   error
*/
int Consistent_archive::open_crash_safe_index_file(Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  File file = -1;
  IO_CACHE *crash_safe_index_file;
  char *crash_safe_index_file_name;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file = &m_crash_safe_mysql_clone_index_file;
    crash_safe_index_file_name = m_crash_safe_mysql_clone_index_file_name;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file = &m_crash_safe_se_backup_index_file;
    crash_safe_index_file_name = m_crash_safe_se_backup_index_file_name;
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    crash_safe_index_file = &m_crash_safe_consistent_snapshot_index_file;
    crash_safe_index_file_name =
        m_crash_safe_consistent_snapshot_index_file_name;
  }

  if (!my_b_inited(crash_safe_index_file)) {
    myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;

    if ((file = my_open(crash_safe_index_file_name, O_RDWR | O_CREAT,
                        MYF(MY_WME))) < 0 ||
        init_io_cache(crash_safe_index_file, file, IO_SIZE, WRITE_CACHE, 0,
                      false, flags)) {
      error = 1;
    }
  }
  return error;
}

/**
  Set the name of crash safe index file.

  @retval
    0   ok
  @retval
    1   error
*/
int Consistent_archive::set_crash_safe_index_file_name(
    const char *base_file_name, Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  char *crash_safe_index_file_name;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file_name = m_crash_safe_mysql_clone_index_file_name;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file_name = m_crash_safe_se_backup_index_file_name;
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    crash_safe_index_file_name =
        m_crash_safe_consistent_snapshot_index_file_name;
  }
  if (fn_format(crash_safe_index_file_name, base_file_name,
                m_mysql_archive_data_dir, ".index_crash_safe",
                MYF(MY_UNPACK_FILENAME | MY_SAFE_PATH | MY_REPLACE_EXT |
                    MY_REPLACE_DIR)) == nullptr) {
    error = 1;
  }
  return error;
}

/**
  Close the crash safe index file.

  @note
    The crash safe file is just closed, is not deleted.
    Because it is moved to index file later on.
  @retval
    0   ok
  @retval
    1   error
*/
int Consistent_archive::close_crash_safe_index_file(Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  IO_CACHE *crash_safe_index_file;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file = &m_crash_safe_mysql_clone_index_file;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file = &m_crash_safe_se_backup_index_file;
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    crash_safe_index_file = &m_crash_safe_consistent_snapshot_index_file;
  }

  if (my_b_inited(crash_safe_index_file)) {
    end_io_cache(crash_safe_index_file);
    error = my_close(crash_safe_index_file->file, MYF(0));
  }
  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    m_crash_safe_mysql_clone_index_file = IO_CACHE();
  } else if (arch_type == ARCHIVE_SE) {
    m_crash_safe_se_backup_index_file = IO_CACHE();
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    m_crash_safe_consistent_snapshot_index_file = IO_CACHE();
  }

  return error;
}

/**
  Move crash safe index file to index file.

  First, upload it to the object store, then update the local index file.
  If the upload fails, update index will also fail. If the
  upload succeeds but the local update index fails, it will result in
  inconsistency between the local and object store indexes. Therefore,
  forcibly close the index file to allow the consistent snapshot archive
  thread to download the index from the object store again and reopen it.

  @param need_lock_index If true, LOCK_index will be acquired;
  otherwise it should already be held.

  @retval 0 ok
  @retval 1 error
*/
int Consistent_archive::move_crash_safe_index_file_to_index_file(
    Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("move_crash_safe_index_file_to_index_file"));
  int error = 0;
  File fd = -1;
  int failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  bool file_rename_status = false, file_delete_status = false;
  THD *thd = m_thd;
  std::string err_msg{};
  char *crash_safe_index_file_name;
  IO_CACHE *index_file;
  char *index_file_name;
  PSI_file_key *index_file_key;
  PSI_file_key *index_file_cache_key;
  std::string index_keyid;
  char index_keyid_mv_suffix[FN_REFLEN + 1] = {0};
  uint64_t *index_term;
  bool fault_injection_put_se_index = false,
       fault_injection_put_innodb_index = false,
       fault_injection_put_consistent_index = false;
  bool fault_injection_delete_innodb_index = false,
       fault_injection_delete_se_index = false,
       fault_injection_delete_consistent_index = false;
  bool fault_injection_reopen_innodb_index = false,
       fault_injection_reopen_se_index = false,
       fault_injection_reopen_consistent_index = false;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file_name = m_crash_safe_mysql_clone_index_file_name;
    index_file = &m_mysql_clone_index_file;
    index_file_name = m_mysql_clone_index_file_name;
    index_file_key = &PSI_consistent_archive_mysql_log_index_key;
    index_file_cache_key = &PSI_consistent_archive_mysql_log_index_cache_key;
    snprintf(index_keyid_mv_suffix, FN_REFLEN,
             CONSISTENT_INNODB_ARCHIVE_INDEX_FILE_FORMAT,
             static_cast<my_off_t>(m_consensus_term));
    index_keyid.assign(m_archive_dir);
    index_keyid.append(index_keyid_mv_suffix);
    index_term = &m_opened_innodb_index_term;
    DBUG_EXECUTE_IF("fault_injection_put_innodb_index_to_objstore",
                    { fault_injection_put_innodb_index = true; });
    DBUG_EXECUTE_IF("fault_injection_delete_innodb_index_file",
                    { fault_injection_delete_innodb_index = true; });
    DBUG_EXECUTE_IF("fault_injection_reopen_innodb_index_file",
                    { fault_injection_reopen_innodb_index = true; });
    err_msg.assign("innodb.index");
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file_name = m_crash_safe_se_backup_index_file_name;
    index_file = &m_se_backup_index_file;
    index_file_name = m_se_backup_index_file_name;
    index_file_key = &PSI_consistent_archive_se_log_index_key;
    index_file_cache_key = &PSI_consistent_archive_se_log_index_cache_key;
    snprintf(index_keyid_mv_suffix, FN_REFLEN,
             CONSISTENT_SE_ARCHIVE_INDEX_FILE_FORMAT,
             static_cast<my_off_t>(m_consensus_term));
    index_keyid.assign(m_archive_dir);
    index_keyid.append(index_keyid_mv_suffix);
    index_term = &m_opened_se_index_term;
    DBUG_EXECUTE_IF("fault_injection_put_se_index_to_objstore",
                    { fault_injection_put_se_index = true; });
    DBUG_EXECUTE_IF("fault_injection_delete_se_index_file",
                    { fault_injection_delete_se_index = true; });
    DBUG_EXECUTE_IF("fault_injection_reopen_se_index_file",
                    { fault_injection_reopen_se_index = true; });
    err_msg.assign("smartengine.index");
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    crash_safe_index_file_name =
        m_crash_safe_consistent_snapshot_index_file_name;
    index_file = &m_consistent_snapshot_index_file;
    index_file_name = m_consistent_snapshot_index_file_name;
    index_file_key = &PSI_consistent_snapshot_file_key;
    index_file_cache_key = &PSI_consistent_snapshot_file_cache_key;
    snprintf(index_keyid_mv_suffix, FN_REFLEN,
             CONSISTENT_SNAPSHOT_INDEX_FILE_FORMAT,
             static_cast<my_off_t>(m_consensus_term));
    index_keyid.assign(m_archive_dir);
    index_keyid.append(index_keyid_mv_suffix);
    index_term = &m_opened_snapshot_index_term;
    DBUG_EXECUTE_IF("fault_injection_put_consistent_index_to_objstore",
                    { fault_injection_put_consistent_index = true; });
    DBUG_EXECUTE_IF("fault_injection_delete_consistent_index_file",
                    { fault_injection_delete_consistent_index = true; });
    DBUG_EXECUTE_IF("fault_injection_reopen_consistent_index_file",
                    { fault_injection_reopen_consistent_index = true; });
    err_msg.assign("snapshot.index");
  }

  // 1. upload local crash index file to object store
  // TODO: objectstore API will retry n times.
  objstore::Status ss = snapshot_objstore->put_object_from_file(
      std::string_view(opt_objstore_bucket), index_keyid,
      crash_safe_index_file_name);
  if (fault_injection_put_innodb_index || fault_injection_put_se_index ||
      fault_injection_put_consistent_index || !ss.is_succ()) {
    err_msg.append(
        " upload to object store failed in "
        "move_crash_safe_index_file_to_index_file keyid=");
    err_msg.append(index_keyid);
    err_msg.append(" error=");
    err_msg.append(ss.error_message());
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    error = 1;
    /*
      Delete Crash safe file index file here and recover the binlog.index
      from persistent binlog.index.
     */
    mysql_file_delete(*index_file_key, crash_safe_index_file_name, MYF(0));
    goto err;
  }

  // 2. close local index file.
  if (my_b_inited(index_file)) {
    end_io_cache(index_file);
    if (mysql_file_close(index_file->file, MYF(0)) < 0) {
      error = 1;
      /*
        Delete Crash safe file index file here and recover the binlog.index
        from persistent binlog.index.
       */
      mysql_file_delete(*index_file_key, crash_safe_index_file_name, MYF(0));
      goto err;
    }

    while ((file_delete_status == false) && (failure_trials > 0)) {
      if (fault_injection_delete_innodb_index ||
          fault_injection_delete_se_index ||
          fault_injection_delete_consistent_index)
        break;
      file_delete_status =
          !(mysql_file_delete(*index_file_key, index_file_name, MYF(MY_WME)));
      --failure_trials;
      if (!file_delete_status) {
        my_sleep(1000);
        /* Clear the error before retrying. */
        if (failure_trials > 0) thd->clear_error();
      }
    }

    if (!file_delete_status) {
      err_msg.append(
          " delete failed in move_crash_safe_index_file_to_index_file");
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      error = 1;
      /*
        Delete Crash safe file index file here and recover the binlog.index
        from persistent binlog.index.
       */
      mysql_file_delete(*index_file_key, crash_safe_index_file_name, MYF(0));
      goto err;
    }
  }

  // 3. update local index file.
  failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  while ((file_rename_status == false) && (failure_trials > 0)) {
    file_rename_status =
        !(my_rename(crash_safe_index_file_name, index_file_name, MYF(MY_WME)));
    --failure_trials;
    if (!file_rename_status) {
      my_sleep(1000);
      /* Clear the error before retrying. */
      if (failure_trials > 0) thd->clear_error();
    }
  }
  if (!file_rename_status) {
    mysql_file_delete(*index_file_key, crash_safe_index_file_name, MYF(0));
    error = 1;
    goto err;
  }

  // 4. reopen local index file.
  if ((fault_injection_reopen_innodb_index || fault_injection_reopen_se_index ||
       fault_injection_reopen_consistent_index) ||
      (fd = mysql_file_open(*index_file_key, index_file_name, O_RDWR | O_CREAT,
                            MYF(MY_WME))) < 0 ||
      mysql_file_sync(fd, MYF(MY_WME)) ||
      init_io_cache_ext(index_file, fd, IO_SIZE, READ_CACHE,
                        mysql_file_seek(fd, 0L, MY_SEEK_END, MYF(0)), false,
                        MYF(MY_WME | MY_WAIT_IF_FULL), *index_file_cache_key)) {
    err_msg.append(" open failed in move_crash_safe_index_file_to_index_file");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    error = 1;
    // Close local index file and recover the binlog.index
    // from persistent binlog.index.
    if (my_b_inited(index_file)) end_io_cache(index_file);
    if (fd >= 0) mysql_file_close(fd, MYF(0));
    goto err;
  }
  *index_term = extract_term_from_index_file(index_keyid.c_str());

err:
  return error;
}

/**
  Append log file name to index file.
  First persist the archive object, then persist the index file.
  If the archive object is successfully persisted but the index file persistence
  fails, the snapshot is considered a failure, and the already persisted object
  is treated as garbage and purged.
  - To make crash safe, we copy all the content of index file
  to crash safe index file firstly and then append the log
  file name to the crash safe index file. Finally move the
  crash safe index file to index file.
  @note Must mutext held before called.
  @retval
    0   ok
  @retval
    -1   error
*/
int Consistent_archive::add_line_to_index(const char *log_name,
                                          Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("add_line_to_index"));
  IO_CACHE *crash_safe_index_file;
  std::string crash_safe_index_file_name;
  IO_CACHE *index_file = nullptr;
  std::string index_file_name;
  std::string err_msg{};

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file = &m_crash_safe_mysql_clone_index_file;
    crash_safe_index_file_name.assign(m_crash_safe_mysql_clone_index_file_name);
    index_file = &m_mysql_clone_index_file;
    index_file_name.assign(m_mysql_clone_index_file_name);
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file = &m_crash_safe_se_backup_index_file;
    crash_safe_index_file_name.assign(m_crash_safe_se_backup_index_file_name);
    index_file = &m_se_backup_index_file;
    index_file_name.assign(m_se_backup_index_file_name);
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    crash_safe_index_file = &m_crash_safe_consistent_snapshot_index_file;
    crash_safe_index_file_name.assign(
        m_crash_safe_consistent_snapshot_index_file_name);
    index_file = &m_consistent_snapshot_index_file;
    index_file_name.assign(m_consistent_snapshot_index_file_name);
  }

  err_msg.assign(" add index entry ");
  err_msg.append(log_name);
  err_msg.append(" to ");
  err_msg.append(index_file_name);
  if (!my_b_inited(index_file)) {
    err_msg.append(" ,local index file not opened");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  if (open_crash_safe_index_file(arch_type)) {
    err_msg.append(" open_crash_safe_index_file failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  if (copy_file(index_file, crash_safe_index_file, 0)) {
    err_msg.append(" copy_file failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  if (my_b_write(crash_safe_index_file, (const uchar *)log_name,
                 strlen(log_name)) ||
      my_b_write(crash_safe_index_file, pointer_cast<const uchar *>("\n"), 1) ||
      flush_io_cache(crash_safe_index_file) ||
      mysql_file_sync(crash_safe_index_file->file, MYF(MY_WME))) {
    err_msg.append(" my_b_write failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  if (close_crash_safe_index_file(arch_type)) {
    err_msg.append(" close_crash_safe_index_file failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  // If failed, recover the index file from object store
  // when restart the next consistent snapshot from run()->while{}.
  if (move_crash_safe_index_file_to_index_file(arch_type)) {
    err_msg.append(" move_crash_safe_index_file_to_index_file failed");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());

  return 0;

err:
  return -1;
}

bool Consistent_archive::open_index_file(const char *index_file_name_arg,
                                         const char *log_name,
                                         Archive_type arch_type,
                                         bool need_lock) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("open_index_file"));
  bool error = false;
  File index_file_nr = -1;
  char *crash_safe_index_file_name;
  IO_CACHE *index_file;
  char *index_file_name;
  PSI_file_key *index_file_key;
  PSI_file_key *index_file_cache_key;
  mysql_mutex_t *index_lock;
  std::string err_msg{};
  std::string index_obj_keyid;
  uint64_t *index_term;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file_name = m_crash_safe_mysql_clone_index_file_name;
    index_file = &m_mysql_clone_index_file;
    index_file_name = m_mysql_clone_index_file_name;
    index_file_key = &PSI_consistent_archive_mysql_log_index_key;
    index_file_cache_key = &PSI_consistent_archive_mysql_log_index_cache_key;
    index_lock = &m_mysql_innodb_clone_index_lock;
    index_term = &m_opened_innodb_index_term;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file_name = m_crash_safe_se_backup_index_file_name;
    index_file = &m_se_backup_index_file;
    index_file_name = m_se_backup_index_file_name;
    index_file_key = &PSI_consistent_archive_se_log_index_key;
    index_file_cache_key = &PSI_consistent_archive_se_log_index_cache_key;
    index_lock = &m_se_backup_index_lock;
    index_term = &m_opened_se_index_term;
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    crash_safe_index_file_name =
        m_crash_safe_consistent_snapshot_index_file_name;
    index_file = &m_consistent_snapshot_index_file;
    index_file_name = m_consistent_snapshot_index_file_name;
    index_file_key = &PSI_consistent_snapshot_file_key;
    index_file_cache_key = &PSI_consistent_snapshot_file_cache_key;
    index_lock = &m_consistent_index_lock;
    index_term = &m_opened_snapshot_index_term;
  }

  if (need_lock) {
    mysql_mutex_lock(index_lock);
  }
  /*
    First open of this class instance
    Create an index file that will hold all file names uses for logging.
    Add new entries to the end of it.
  */
  myf opt = MY_UNPACK_FILENAME | MY_REPLACE_DIR;

  if (my_b_inited(index_file)) goto end;

  if (!index_file_name_arg) {
    index_file_name_arg = log_name;  // Use same basename for index file
    opt = MY_UNPACK_FILENAME | MY_REPLACE_EXT | MY_REPLACE_DIR;
  }
  fn_format(index_file_name, index_file_name_arg, m_mysql_archive_data_dir,
            ".index", opt);

  if (set_crash_safe_index_file_name(index_file_name_arg, arch_type)) {
    error = true;
    goto end;
  }
  if (arch_type != ARCHIVE_SNAPSHOT_FILE &&
      set_purge_index_file_name(index_file_name_arg, arch_type)) {
    error = true;
    goto end;
  }
  /*
    We need move m_crash_safe_mysql_clone_index_file to
    m_mysql_clone_index_file if the m_mysql_clone_index_file does not exist
    and m_crash_safe_mysql_clone_index_file exists when mysqld server
    restarts.
  */
  if (my_access(index_file_name, F_OK) &&
      !my_access(crash_safe_index_file_name, F_OK) &&
      my_rename(crash_safe_index_file_name, index_file_name, MYF(MY_WME))) {
    error = true;
    goto end;
  }

  // Check if the index file exists in s3, if so, download it to local.
  if (fetch_last_persistent_index_file(index_obj_keyid, arch_type)) {
    error = true;
    goto end;
  }

  // Check if the index file exists in s3, if so, download it to local.
  // And rename it to index_file_name.
  if (!index_obj_keyid.empty()) {
    // 0 if snapshot.index
    *index_term = extract_term_from_index_file(index_obj_keyid.c_str());

    std::string index_file_name_str;
    index_file_name_str.assign(crash_safe_index_file_name);
    auto status = snapshot_objstore->get_object_to_file(
        std::string_view(opt_objstore_bucket), index_obj_keyid,
        index_file_name_str);
    if (!status.is_succ()) {
      err_msg.assign("get last persistent index failed");
      err_msg.append(index_obj_keyid);
      err_msg.append(" error=");
      err_msg.append(status.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      error = true;
      goto end;
    }

    if (my_chmod(
            crash_safe_index_file_name,
            USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
            MYF(0))) {
      err_msg.assign(" failed to chmod ");
      err_msg.append(crash_safe_index_file_name);
      err_msg.append(" errno=");
      err_msg.append(std::to_string(my_errno()));
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      error = true;
      goto end;
    }
    if (my_rename(crash_safe_index_file_name, index_file_name, MYF(MY_WME))) {
      err_msg.assign(" failed to rename ");
      err_msg.append(crash_safe_index_file_name);
      err_msg.append(" to");
      err_msg.append(index_file_name);
      err_msg.append(" errno=");
      err_msg.append(std::to_string(my_errno()));
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      error = true;
      goto end;
    }
    err_msg.assign("get last persistent index file key=");
    err_msg.append(index_obj_keyid);
    err_msg.append(" to ");
    err_msg.append(index_file_name);
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  }

  if ((index_file_nr = mysql_file_open(*index_file_key, index_file_name,
                                       O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      mysql_file_sync(index_file_nr, MYF(MY_WME)) ||
      init_io_cache_ext(index_file, index_file_nr, IO_SIZE, READ_CACHE,
                        mysql_file_seek(index_file_nr, 0L, MY_SEEK_END, MYF(0)),
                        false, MYF(MY_WME | MY_WAIT_IF_FULL),
                        *index_file_cache_key) ||
      DBUG_EVALUATE_IF("fault_injection_openning_index", 1, 0)) {
    /*
      TODO: all operations creating/deleting the index file or a log, should
      call my_sync_dir() or my_sync_dir_by_file() to be durable.
      TODO: file creation should be done with mysql_file_create()
      not mysql_file_open().
    */
    if (my_b_inited(index_file)) end_io_cache(index_file);
    if (index_file_nr >= 0) mysql_file_close(index_file_nr, MYF(0));
    error = true;
    goto end;
  }

  err_msg.assign("open index file ");
  err_msg.append(index_file_name);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());

end:
  if (need_lock) {
    mysql_mutex_unlock(index_lock);
  }
  return error;
}

IO_CACHE *Consistent_archive::get_consistent_snapshot_index_file() {
  if (open_index_file(CONSISTENT_SNAPSHOT_INDEX_FILE,
                      CONSISTENT_SNAPSHOT_INDEX_FILE, ARCHIVE_SNAPSHOT_FILE)) {
    return nullptr;
  }
  return &m_consistent_snapshot_index_file;
}

IO_CACHE *Consistent_archive::get_se_backup_index_file() {
  if (open_index_file(CONSISTENT_SE_ARCHIVE_INDEX_FILE,
                      CONSISTENT_SE_ARCHIVE_INDEX_FILE, ARCHIVE_SE)) {
    return nullptr;
  }
  return &m_se_backup_index_file;
}

IO_CACHE *Consistent_archive::get_mysql_clone_index_file() {
  if (open_index_file(CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
                      CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
                      ARCHIVE_MYSQL_INNODB)) {
    return nullptr;
  }
  return &m_mysql_clone_index_file;
}

void Consistent_archive::close_index_file(Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("close_index_file"));
  IO_CACHE *index_file;
  char *index_file_name;
  uint64_t *index_term;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_mysql_clone_index_file;
    index_file_name = m_mysql_clone_index_file_name;
    index_term = &m_opened_innodb_index_term;
  } else if (arch_type == ARCHIVE_SE) {
    index_file = &m_se_backup_index_file;
    index_file_name = m_se_backup_index_file_name;
    index_term = &m_opened_se_index_term;
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    index_file = &m_consistent_snapshot_index_file;
    index_file_name = m_consistent_snapshot_index_file_name;
    index_term = &m_opened_snapshot_index_term;
  }

  *index_term = 0;
  if (my_b_inited(index_file)) {
    std::string err_msg;
    end_io_cache(index_file);
    err_msg.assign("close index file ");
    err_msg.append(index_file_name);
    if (mysql_file_close(index_file->file, MYF(0))) {
      err_msg.append(" failed");
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      return;
    }
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  }
}

static int compare_log_name(const char *log_1, const char *log_2) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("compare_log_name"));
  const char *log_1_basename = log_1 + dirname_length(log_1);
  const char *log_2_basename = log_2 + dirname_length(log_2);

  return strcmp(log_1_basename, log_2_basename);
}

/**
 * @brief Remove the trailing '/', remove the trailing extension, and then
 * compare.
 * @param log_1
 * single-version innodb_000012.tar or innodb_000012.tar.gz or innodb_000012/
 * multi-version innodb_000001_000012.tar or innodb_000001_00012.tar.gz or
 * @param log_2
 * single-version innodb_000012.tar or innodb_000012.tar.gz or innodb_000012/
 * multi-version innodb_000001_000012.tar or innodb_000001_00012.tar.gz or
 * innodb_000001_00012/
 * @return int
 */
static int compare_log_name_without_ext(const char *log_1, const char *log_2) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("compare_log_name_without_ext"));
  std::string log_1_basename{log_1};
  std::string log_2_basename{log_2};
  // Get rid of last '/'
  if (log_1_basename.back() == FN_LIBCHAR) {
    log_1_basename = log_1_basename.substr(0, log_1_basename.length() - 1);
  }
  if (log_2_basename.back() == FN_LIBCHAR) {
    log_2_basename = log_2_basename.substr(0, log_2_basename.length() - 1);
  }
  // Get rid of extension .gz or .tar.gz
  size_t idx1 = log_1_basename.find(".");
  std::string log_1_without_ext{log_1_basename};
  if (idx1 != std::string::npos) {
    log_1_without_ext = log_1_basename.substr(0, idx1);
  }
  // Get rid of extension .tar or .tar.gz
  size_t idx2 = log_2_basename.find(".");
  std::string log_2_without_ext{log_2_basename};
  if (idx2 != std::string::npos) {
    log_2_without_ext = log_2_basename.substr(0, idx2);
  }
  // innodb_000001_000012 or innodb_000012
  // note: multi-version archive file > single-version archive file
  // innodb_{term}_{index} > innodb_{index}
  if (check_archive_file_version_type(log_1_without_ext) ==
      check_archive_file_version_type(log_2_without_ext)) {
    return log_1_without_ext.compare(log_2_without_ext);
  }
  return check_archive_file_version_type(log_1_without_ext) >
                 check_archive_file_version_type(log_2_without_ext)
             ? 1
             : -1;
}

/**
 * @brief Check if the archive file is a single-version or multi-version.
 * Single-version: innodb_000001.tar or innodb_000001.tar.gz or
 * innodb_000001/
 * Multi-version: innodb_000001_000012.tar or
 * innodb_000001_000012.tar.gz or innodb_000001_000012/
 * @param archive_file_name
 * @return int, -1 for error, 0 for single-version, 1 for multi-version.
 */
static int check_archive_file_version_type(std::string &archive_file_name) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("check_archive_file_version_type"));

  std::string basename;
  basename.assign(archive_file_name.c_str() +
                  dirname_length(archive_file_name.c_str()));
  // Get rid of last '/'
  if (basename.back() == FN_LIBCHAR) {
    basename = basename.substr(0, basename.length() - 1);
  } else {
    // Get rid of extension .tar.gz or .gz
    size_t idx1 = basename.find(".");
    if (idx1 != std::string::npos) {
      basename = basename.substr(0, idx1);
    }
  }
  // innodb_000001_000012 or innodb_000012
  size_t first_underscore = basename.find('_');
  if (first_underscore == std::string::npos) {
    return -1;
  }
  basename = basename.substr(first_underscore + 1);
  size_t second_underscore = basename.find('_');
  // single-version archive file innodb_000012
  if (second_underscore == std::string::npos) {
    return 0;
  }
  // multi-version archive_file innodb_000001_000012
  return 1;
}

/**
  Find the position in the log-index-file for the given log name.

  @param[out] linfo The found log file name will be stored here, along
  with the byte offset of the next log file name in the index file.
  @param match_name Filename to find in the index file, or NULL if we
  want to read the first entry.
  @param need_lock_index If false, this function acquires LOCK_index;
  otherwise the lock should already be held by the caller.

  @note
    On systems without the truncate function the file will end with one or
    more empty lines.  These will be ignored when reading the file.

  @retval
    0			ok
  @retval
    LOG_INFO_EOF	        End of log-index-file found
  @retval
    LOG_INFO_IO		Got IO error while reading file
*/
int Consistent_archive::find_line_from_index(LOG_INFO *linfo,
                                             const char *match_name,
                                             Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("find_line_from_index"));
  int error = 0;
  char *fname = linfo->log_file_name;
  IO_CACHE *index_file;

  fname[0] = 0;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_mysql_clone_index_file;
  } else if (arch_type == ARCHIVE_SE) {
    index_file = &m_se_backup_index_file;
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    index_file = &m_consistent_snapshot_index_file;
  }

  if (!my_b_inited(index_file)) {
    error = LOG_INFO_IO;
    return error;
  }

  /* As the file is flushed, we can't get an error here */
  my_b_seek(index_file, (my_off_t)0);

  for (;;) {
    size_t length;
    my_off_t offset = my_b_tell(index_file);

    /* If we get 0 or 1 characters, this is the end of the file */
    if ((length = my_b_gets(index_file, fname, FN_REFLEN)) <= 1) {
      /* Did not find the given entry; Return not found or error */
      error = !index_file->error ? LOG_INFO_EOF : LOG_INFO_IO;
      break;
    }

    length = strlen(fname);
    // Strips the CR+LF at the end of log name and \0-terminates it.
    if (length && fname[length - 1] == '\n') {
      fname[length - 1] = 0;
      length--;
      if (length && fname[length - 1] == '\r') {
        fname[length - 1] = 0;
        length--;
      }
    }
    if (!length) {
      error = LOG_INFO_EOF;
      break;
    }

    // if the log entry matches, null string matching anything
    if (!match_name || !compare_log_name(fname, match_name)) {
      DBUG_PRINT("info", ("Found log file entry"));
      linfo->index_file_start_offset = offset;
      linfo->index_file_offset = my_b_tell(index_file);
      break;
    }
    linfo->entry_index++;
  }

  // if (need_lock_index) mysql_mutex_unlock(&LOCK_index);
  return error;
}

/**
  Find the position in the log-index-file for the given log name.

  @param[out] linfo The filename will be stored here, along with the
  byte offset of the next filename in the index file.

  @param need_lock_index If true, LOCK_index will be acquired;
  otherwise it should already be held by the caller.

  @note
    - Before calling this function, one has to call find_line_from_index()
    to set up 'linfo'
    - Mutex needed because we need to make sure the file pointer does not move
    from under our feet

  @retval 0 ok
  @retval LOG_INFO_EOF End of log-index-file found
  @retval LOG_INFO_IO Got IO error while reading file
*/
int Consistent_archive::find_next_line_from_index(LOG_INFO *linfo,
                                                  Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("find_next_line_from_index"));
  int error = 0;
  size_t length;
  char *fname = linfo->log_file_name;
  IO_CACHE *index_file;
  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_mysql_clone_index_file;
  } else if (arch_type == ARCHIVE_SE) {
    index_file = &m_se_backup_index_file;
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    index_file = &m_consistent_snapshot_index_file;
  }

  if (!my_b_inited(index_file)) {
    error = LOG_INFO_IO;
    goto err;
  }
  /* As the file is flushed, we can't get an error here */
  my_b_seek(index_file, linfo->index_file_offset);

  linfo->index_file_start_offset = linfo->index_file_offset;
  if ((length = my_b_gets(index_file, fname, FN_REFLEN)) <= 1) {
    error = !index_file->error ? LOG_INFO_EOF : LOG_INFO_IO;
    goto err;
  }

  if (fname[0] != 0) {
    length = strlen(fname);
    // Strips the CR+LF at the end of log name and \0-terminates it.
    if (length && fname[length - 1] == '\n') {
      fname[length - 1] = 0;
      length--;
      if (length && fname[length - 1] == '\r') {
        fname[length - 1] = 0;
        length--;
      }
    }
    if (!length) {
      error = LOG_INFO_EOF;
      goto err;
    }
  }

  linfo->index_file_offset = my_b_tell(index_file);

err:
  // if (need_lock_index) mysql_mutex_unlock(&LOCK_index);
  return error;
}

/**
 * @brief Purge the pesistent consistent snapshot archive file.
 * @return std::tuple<bool, std::string>
 */
std::tuple<int, std::string> Consistent_archive::purge_consistent_snapshot(
    const char *to_purged_ts, size_t len [[maybe_unused]], bool auto_purge) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("purge_consistent_snapshot"));

  int error = 0;
  std::string err_msg{};

  DBUG_EXECUTE_IF("fault_injection_purge_consistent_snapshot", {
    error = 1;
    err_msg.assign("fault_injection_purge_consistent_snapshot");
    return std::make_tuple(error, err_msg);
  });

  LOG_INFO linfo{};
  std::string purge_innodb_end_name{};
  std::string purge_se_end_name{};
  ulong purged_end_ts = 0;
  bool all_snapshot = false;
  uint64_t pre_se_snapshot = 0;
  uint64_t cur_se_snapshot = 0;
  std::string purge_ts_str{};
  int line_num = 0;
  std::vector<uint64_t> archive_backup_ids;
  std::vector<uint64_t> se_backup_ids;
  bool only_record_backup_snapshot = false;

  /*
   * 1. release smartengint snapshot
   * 2. purge consistent_snapshot.index
   * 3. purge innodb and smartengine snapshot data.
   */
  if (native_strcasecmp(to_purged_ts, "all") == 0) {
    all_snapshot = true;
  } else {
    all_snapshot = false;
    if (convert_str_to_datetime(to_purged_ts, purged_end_ts)) {
      err_msg.assign("snapshot to_purged_ts parameter error: ");
      err_msg.append(to_purged_ts);
      error = 1;
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
      return std::make_tuple(error, err_msg);
    }
    assert(purged_end_ts > 0);
  }

  // Fetch smartengine all backup snapshot id.
  if (auto_purge) {
    show_se_backup_snapshot(m_thd, se_backup_ids);
  }

  mysql_mutex_lock(&m_consistent_index_lock);
  if (!my_b_inited(&m_consistent_snapshot_index_file)) {
    mysql_mutex_unlock(&m_consistent_index_lock);
    error = 1;
    err_msg.assign("snapshot.index not opened");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
    return std::make_tuple(error, err_msg);
  }

  /* As the file is flushed, we can't get an error here */
  my_b_seek(&m_consistent_snapshot_index_file, (my_off_t)0);

  for (;;) {
    size_t length;
    char snapshot_info[4 * FN_REFLEN] = {0};
    my_off_t offset = my_b_tell(&m_consistent_snapshot_index_file);
    line_num++;

    /* If we get 0 or 1 characters, this is the end of the file */
    if ((length = my_b_gets(&m_consistent_snapshot_index_file, snapshot_info,
                            4 * FN_REFLEN)) <= 1) {
      /* Did not find the given entry; Return not found or error */
      error =
          !m_consistent_snapshot_index_file.error ? LOG_INFO_EOF : LOG_INFO_IO;
      break;
    }

    length = strlen(snapshot_info);
    // Strips the CR+LF at the end of log name and \0-terminates it.
    if (length && snapshot_info[length - 1] == '\n') {
      snapshot_info[length - 1] = 0;
      length--;
      if (length && snapshot_info[length - 1] == '\r') {
        snapshot_info[length - 1] = 0;
        length--;
      }
    }
    if (!length) break;

    std::string in_str;
    std::string left_string;
    in_str.assign(snapshot_info);
    size_t idx = in_str.find("|");
    std::string ts_str = in_str.substr(0, idx);
    left_string = in_str.substr(idx + 1);
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

    ulong snapshot_ts;
    if (convert_str_to_datetime(ts_str.c_str(), snapshot_ts)) {
      mysql_mutex_unlock(&m_consistent_index_lock);
      err_msg.assign("snapshot timestamp error: ");
      err_msg.append(ts_str);
      error = 1;
      goto err;
    }

    // Record all consistent snapshot id
    cur_se_snapshot = std::stoull(se_snapshot_id);
    archive_backup_ids.push_back(cur_se_snapshot);
    if (only_record_backup_snapshot) {
      continue;
    }

    // Do nothing
    // If the creation time of the oldest snapshot is also later
    // than the purge time, then do not purge.
    if (line_num == 1 && !all_snapshot && snapshot_ts >= purged_end_ts) {
      break;
    }
    // release previous smartengine snapshot,
    // If release failed, report warning and continue.
    if (pre_se_snapshot > 0) {
      release_se_snapshot(pre_se_snapshot);
    }
    pre_se_snapshot = cur_se_snapshot;
    linfo.index_file_start_offset = offset;
    linfo.index_file_offset = my_b_tell(&m_consistent_snapshot_index_file);
    purge_ts_str.assign(ts_str);
    purge_innodb_end_name.assign(innodb_name);
    purge_se_end_name.assign(se_name);

    // Found upper bound.
    if (!all_snapshot && snapshot_ts >= purged_end_ts)
      only_record_backup_snapshot = true;
  }
  if (error == LOG_INFO_IO) {
    mysql_mutex_unlock(&m_consistent_index_lock);
    error = 1;
    err_msg.assign("Failed to find snapshot from snapshot.index");
    goto err;
  }
  // If consistent_snapshot.index is empty or has only lastest snapshot,
  // Do nothing.
  if (linfo.index_file_start_offset == 0) {
    mysql_mutex_unlock(&m_consistent_index_lock);
    error = 0;
    err_msg.assign("purge snapshot successfully: did nothing");
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG,
           err_msg.c_str());
    return std::make_tuple(error, err_msg);
  }
  // First, purge snapshot from snapshot.index.
  error = remove_line_from_index(&linfo, ARCHIVE_SNAPSHOT_FILE);
  if (error) {
    mysql_mutex_unlock(&m_consistent_index_lock);
    err_msg.assign("failed to remove snapshot from snapshot.index");
    goto err;
  }
  // purge snapshot index file
  purge_archive_index_garbage(m_opened_snapshot_index_term,
                              ARCHIVE_SNAPSHOT_FILE);
  mysql_mutex_unlock(&m_consistent_index_lock);

  mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
  // purge innodb archive before purge_innodb_end_name.
  error = purge_archive(purge_innodb_end_name.c_str(), ARCHIVE_MYSQL_INNODB);
  if (error) {
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
    err_msg.assign("failed to purge mysql innodb persistent file: ");
    err_msg.append(purge_innodb_end_name);
    goto err;
  }
  mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);

  mysql_mutex_lock(&m_se_backup_index_lock);
  // purge se archive before purge_se_end_name.
  error = purge_archive(purge_se_end_name.c_str(), ARCHIVE_SE);
  if (error) {
    mysql_mutex_unlock(&m_se_backup_index_lock);
    err_msg.assign("failed to purge smartengine persistent file: ");
    err_msg.append(purge_se_end_name);
    goto err;
  }
  mysql_mutex_unlock(&m_se_backup_index_lock);

  // Auto purge unreference smartengine snapshot.
  // Only during auto purge can snapshots not present in
  // consistent_snapshot.index be deleted, as auto purge and archive
  // consistent snapshot are executed sequentially in the same thread. Manual
  // purge operations cannot ensure that the thread archiving consistent
  // snapshots is not currently performing create_backup_snapshot when reading
  // list_snapshot. May create_backup_snapshot succeed but has not yet been
  // written to consistent_snapshot.index. This can lead to the newly created
  // snapshot being purged.
  if (auto_purge) {
    std::vector<uint64_t> diff1;
    std::set_difference(se_backup_ids.begin(), se_backup_ids.end(),
                        archive_backup_ids.begin(), archive_backup_ids.end(),
                        std::back_inserter(diff1));
    for (const auto &snapshot_id : diff1) {
      release_se_snapshot(snapshot_id);
    }
  }

  err_msg.assign("purge snapshot successfully, before: ");
  err_msg.append(purge_ts_str);
  err_msg.append(" ");
  err_msg.append(purge_innodb_end_name);
  err_msg.append(" ");
  err_msg.append(purge_se_end_name);
  err_msg.append(" ");
  err_msg.append(std::to_string(cur_se_snapshot));
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
  return std::make_tuple(error, err_msg);
err:
  // TODO: Directly clean up those garbage snapshot index file.
  // Because snapshot index is multi-versioned, older versions are not
  // needed, when new version is created, the old version is garbage.

  LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
  return std::make_tuple(error, err_msg);
}

int Consistent_archive::purge_archive(const char *match_name,
                                      Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("purge_archive"));
  DBUG_EXECUTE_IF("fault_injection_purge_innodb_or_smartengine_data",
                  { return 1; });
  int error = 0;
  std::string err_msg{};
  LOG_INFO log_info;
  std::string dirty_end_archive;

  // Check if exists.
  if (!match_name || strlen(match_name) == 0) {
    err_msg.assign("Failed to purge name is empty");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
    goto err;
  }

  if ((error = find_line_from_index(&log_info, match_name, arch_type))) {
    err_msg.assign("Failed to find_line_from_index ");
    err_msg.append(match_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
    goto err;
  }

  if ((error = open_purge_index_file(true, arch_type))) {
    err_msg.assign("Failed to open_purge_index_file");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
    goto err;
  }
  // From the first entry to match_name, record all index entries that need to
  // be purged in the local purge.index file.
  if ((error = find_line_from_index(&log_info, NullS, arch_type))) {
    err_msg.assign("Failed to find_line_from_index from nulls");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
    goto err;
  }
  while (compare_log_name_without_ext(match_name, log_info.log_file_name) > 0) {
    if ((error =
             register_purge_index_entry(log_info.log_file_name, arch_type))) {
      err_msg.assign("Failed to register_purge_index_entry");
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
      goto err;
    }
    // Find the first purgable archive name from the index and clean it up as
    // garbage since these garbage archive files do not exist in the index.
    dirty_end_archive.assign(log_info.log_file_name);

    if (find_next_line_from_index(&log_info, arch_type)) break;
  }

  if ((error = sync_purge_index_file(arch_type))) {
    err_msg.assign("Failed to sync_purge_index_file");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
    goto err;
  }

  /* We know how many files to delete. Update index file. */
  if ((error = remove_line_from_index(&log_info, arch_type))) {
    goto err;
  }

err:
  int error_index = 0, close_error_index = 0;
  int error_purge = 0;
  // Allow purge failures; archive files that
  // were not deleted are treated as garbage

  /* Read each entry  and delete the archive file. */
  if (!error && (error_index = purge_index_entry(arch_type))) {
    error_purge = 1;
    err_msg.assign("Failed to purge_index_entry");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  }
  // Directly clean up those garbage archive files that is is smaller than
  // match_name do not exist in the archive index.
  if (!error && !error_index && !error_purge && !dirty_end_archive.empty()) {
    purge_archive_garbage(dirty_end_archive.c_str(), arch_type);
  }

  close_error_index = close_purge_index_file(arch_type);

  /*
    Error codes from purge logs take precedence.
    Then error codes from purging the index entry.
    Finally, error codes from closing the purge index file.
  */
  error = error ? error : (error_index ? error_index : close_error_index);
  return error;
}

int Consistent_archive::purge_archive_garbage(const char *dirty_end_archive,
                                              Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("purge_archive_garbage"));
  DBUG_EXECUTE_IF("fault_injection_purge_archive_garbage", { return 1; });
  int error = 0;
  bool finished = false;
  std::string err_msg;
  std::string start_after;
  std::string archive_prefix;
  uint64_t *index_term;

  err_msg.assign(
      "clean up those garbage archive files that do not exist in the index: "
      "before ");
  err_msg.append(dirty_end_archive);
  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());

  archive_prefix.assign(m_archive_dir);
  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    archive_prefix.append(CONSISTENT_INNODB_ARCHIVE_BASENAME);
    index_term = &m_opened_innodb_index_term;
  } else if (arch_type == ARCHIVE_SE) {
    archive_prefix.append(CONSISTENT_SE_ARCHIVE_BASENAME);
    index_term = &m_opened_se_index_term;
  }
  // cleanup the garbage innodb or smartengine index file.
  purge_archive_index_garbage(*index_term, arch_type);

  std::vector<objstore::ObjectMeta> objects;
  do {
    std::vector<objstore::ObjectMeta> tmp_objects;
    // Only return the files and directories at the first-level directory.
    // Single-version format: innodb_000001.tar or
    // innodb_000001.tar.gz or innodb_000001/
    // Multi-version format: innodb_000001_000012.tar or
    // innodb_000001_000012.tar.gz or innodb_000001_000012/
    objstore::Status ss = snapshot_objstore->list_object(
        std::string_view(opt_objstore_bucket), archive_prefix, false,
        start_after, finished, tmp_objects);
    if (!ss.is_succ()) {
      error = 1;
      err_msg.assign("list persistent object files: ");
      err_msg.append(archive_prefix);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
      break;
    }
    objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
  } while (finished == false);

  if (!error) {
    std::string dirty_end_archive_keyid;
    dirty_end_archive_keyid.assign(dirty_end_archive);
    for (const auto &object : objects) {
      if (compare_log_name_without_ext(dirty_end_archive_keyid.c_str(),
                                       object.key.c_str()) > 0) {
        err_msg.assign("delete garbage archive file from object store: ");
        err_msg.append(object.key);
        LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
        objstore::Status ss{};
        if (DBUG_EVALUATE_IF("fault_injection_delete_garbage_smartengine_"
                             "snapshot_from_objstore",
                             1, 0) ||
            DBUG_EVALUATE_IF(
                "fault_injection_delete_garbage_innodb_snapshot_from_objstore",
                1, 0)) {
          ss.set_error_code(objstore::Errors::SE_IO_ERROR);
          ss.set_error_msg(
              "fault injection delete garbage snapshot from objstore");
        } else {
          if (object.key.back() == FN_LIBCHAR) {
            ss = snapshot_objstore->delete_directory(
                std::string_view(opt_objstore_bucket), object.key);
          } else {
            ss = snapshot_objstore->delete_object(
                std::string_view(opt_objstore_bucket), object.key);
          }
        }
        if (!ss.is_succ()) {
          err_msg.append(" error=");
          err_msg.append(ss.error_message());
          LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG,
                 err_msg.c_str());
          // Continue cleaning up other archive files even if an error occurs.
        }
      }
    }
  }
  return error;
}

/**
 * @brief purge the smaller than term index file.
 *
 * @param end_term
 * @return int
 */
int Consistent_archive::purge_archive_index_garbage(uint64_t end_term,
                                                    Archive_type arch_type) {
  std::vector<objstore::ObjectMeta> objects;
  bool finished = false;
  std::string start_after;
  std::string index_prefix;
  std::string old_index_keyid;
  std::string err_msg;

  if (end_term == 0) {
    return 0;
  }
  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_prefix.assign(m_archive_dir);
    index_prefix.append(CONSISTENT_INNODB_INDEX_FILE_BASENAME);
    old_index_keyid.assign(m_archive_dir);
    old_index_keyid.append(CONSISTENT_INNODB_ARCHIVE_INDEX_FILE);
  } else if (arch_type == ARCHIVE_SE) {
    index_prefix.assign(m_archive_dir);
    index_prefix.append(CONSISTENT_SE_INDEX_FILE_BASENAME);
    old_index_keyid.assign(m_archive_dir);
    old_index_keyid.append(CONSISTENT_SE_ARCHIVE_INDEX_FILE);
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    index_prefix.assign(m_archive_dir);
    index_prefix.append(CONSISTENT_SNAPSHOT_INDEX_FILE_BASENAME);
    old_index_keyid.assign(m_archive_dir);
    old_index_keyid.append(CONSISTENT_SNAPSHOT_INDEX_FILE);
  }
  /*
   snapshot.000001.index
   snapshot.000002.index
   snapshot.000003.index
   snapshot.index
  */
  do {
    std::vector<objstore::ObjectMeta> tmp_objects;
    objstore::Status ss = snapshot_objstore->list_object(
        std::string_view(opt_objstore_bucket), index_prefix, false, start_after,
        finished, tmp_objects);
    if (!ss.is_succ()) {
      err_msg.assign("list persistent index file failed: ");
      err_msg.append(index_prefix);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      return 1;
    }
    objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
  } while (finished == false);
  // if no persistent binlog.index, return.
  if (objects.size() < 2) {
    err_msg.assign("no purge persistent index file.");
    return 0;
  }
  // Cleanup snapshot.index if exists.
  if (old_index_keyid.compare(objects.back().key) == 0) {
    err_msg.assign("delete garbage index from object store: ");
    err_msg.append(objects.back().key);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    objstore::Status ss = snapshot_objstore->delete_object(
        std::string_view(opt_objstore_bucket), objects.back().key);
    if (!ss.is_succ()) {
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      // Continue cleaning up other index even if an error
      // occurs.
    }
    objects.erase(objects.end() - 1);
  }

  // snapshot.{term}.index
  for (const auto &object : objects) {
    // extract term from snapshot.{term}.index
    uint64_t consensus_term = extract_term_from_index_file(object.key.c_str());
    if (consensus_term == 0) {
      err_msg.assign("invalid index key: ");
      err_msg.append(object.key);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    }

    // If the term of the index file is smaller than the end_term,
    // delete the index file from the object store.
    if (end_term > consensus_term) {
      err_msg.assign("delete garbage index from object store: ");
      err_msg.append(object.key);
      LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      objstore::Status ss = snapshot_objstore->delete_object(
          std::string_view(opt_objstore_bucket), object.key);
      if (!ss.is_succ()) {
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
        // Continue cleaning up other binlog.index even if an error
        // occurs.
      }
    }
  }
  return 0;
}

int Consistent_archive::set_purge_index_file_name(const char *base_file_name,
                                                  Archive_type arch_type) {
  int error = 0;
  DBUG_TRACE;
  char *index_file_name;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file_name = m_purge_mysql_clone_index_file_name;
  } else {
    assert(arch_type == ARCHIVE_SE);
    index_file_name = m_purge_se_backup_index_file_name;
  }

  if (fn_format(
          index_file_name, base_file_name, m_mysql_archive_data_dir, ".~rec~",
          MYF(MY_UNPACK_FILENAME | MY_SAFE_PATH | MY_REPLACE_EXT)) == nullptr) {
    error = 1;
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to set purge index");
  }
  return error;
}

int Consistent_archive::open_purge_index_file(bool destroy,
                                              Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("open_purge_index_file"));
  int error = 0;
  File file = -1;
  char *index_file_name;
  IO_CACHE *index_file;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_purge_mysql_clone_index_file;
    index_file_name = m_purge_mysql_clone_index_file_name;
  } else {
    assert(arch_type == ARCHIVE_SE);
    index_file = &m_purge_se_backup_index_file;
    index_file_name = m_purge_se_backup_index_file_name;
  }

  if (destroy) close_purge_index_file(arch_type);

  if (!my_b_inited(index_file)) {
    myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;
    if ((file = my_open(index_file_name, O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
        init_io_cache(index_file, file, IO_SIZE,
                      (destroy ? WRITE_CACHE : READ_CACHE), 0, false, flags)) {
      error = 1;
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "failed to open purge index");
    }
  }
  return error;
}

int Consistent_archive::close_purge_index_file(Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  char *index_file_name;
  IO_CACHE *index_file;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_purge_mysql_clone_index_file;
    index_file_name = m_purge_mysql_clone_index_file_name;
  } else {
    assert(arch_type == ARCHIVE_SE);
    index_file = &m_purge_se_backup_index_file;
    index_file_name = m_purge_se_backup_index_file_name;
  }

  if (my_b_inited(index_file)) {
    end_io_cache(index_file);
    error = my_close(index_file->file, MYF(0));
  }
  my_delete(index_file_name, MYF(0));
  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    m_purge_mysql_clone_index_file = IO_CACHE();
  } else {
    assert(arch_type == ARCHIVE_SE);
    m_purge_se_backup_index_file = IO_CACHE();
  }

  return error;
}

int Consistent_archive::sync_purge_index_file(Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("sync_purge_index_file"));
  int error = 0;
  IO_CACHE *index_file;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_purge_mysql_clone_index_file;
  } else {
    assert(arch_type == ARCHIVE_SE);
    index_file = &m_purge_se_backup_index_file;
  }

  if (!my_b_inited(index_file)) {
    return LOG_INFO_IO;
  }
  if ((error = flush_io_cache(index_file)) ||
      (error = my_sync(index_file->file, MYF(MY_WME))))
    return error;

  return error;
}

int Consistent_archive::register_purge_index_entry(const char *entry,
                                                   Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("register_purge_index_entry"));
  int error = 0;
  IO_CACHE *index_file;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_purge_mysql_clone_index_file;
  } else {
    assert(arch_type == ARCHIVE_SE);
    index_file = &m_purge_se_backup_index_file;
  }

  if (!my_b_inited(index_file)) {
    return LOG_INFO_IO;
  }
  if ((error = my_b_write(index_file, (const uchar *)entry, strlen(entry))) ||
      (error = my_b_write(index_file, (const uchar *)"\n", 1)))
    return error;

  return error;
}

/**
 * @brief Delete the archive file from the object store.
 *
 * @return int
 */
int Consistent_archive::purge_index_entry(Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("purge_index_entry"));
  int error = 0;
  LOG_INFO log_info;
  LOG_INFO check_log_info;
  IO_CACHE *index_file;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_purge_mysql_clone_index_file;
  } else {
    assert(arch_type == ARCHIVE_SE);
    index_file = &m_purge_se_backup_index_file;
  }

  if (!my_b_inited(index_file)) {
    return LOG_INFO_IO;
  }

  if ((error = reinit_io_cache(index_file, READ_CACHE, 0, false, false))) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG,
           "failed to reinit_io_cache");
    goto err;
  }

  for (;;) {
    size_t length = 0;

    if ((length = my_b_gets(index_file, log_info.log_file_name, FN_REFLEN)) <=
        1) {
      if (index_file->error) {
        error = index_file->error;
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, error);
        goto err;
      }

      /* Reached EOF */
      break;
    }

    /* Get rid of the trailing '\n' */
    log_info.log_file_name[length - 1] = 0;

    // delete the archived se_backup_000001.tar or se_backup_000001 directory
    // from the object store.
    std::string archive_keyid;
    archive_keyid.assign(log_info.log_file_name);
    std::string err_msg;
    err_msg.assign("delete archive file from object store: ");
    err_msg.append(archive_keyid);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
    objstore::Status ss{};
    if (DBUG_EVALUATE_IF(
            "fault_injection_delete_smartengine_snapshot_from_objstore", 1,
            0) ||
        DBUG_EVALUATE_IF("fault_injection_delete_innodb_snapshot_from_objstore",
                         1, 0)) {
      ss.set_error_code(objstore::Errors::SE_IO_ERROR);
      ss.set_error_msg("fault injection delete snapshot from objestore");
    } else {
      if (archive_keyid.back() == FN_LIBCHAR) {
        ss = snapshot_objstore->delete_directory(
            std::string_view(opt_objstore_bucket), archive_keyid);
      } else {
        ss = snapshot_objstore->delete_object(
            std::string_view(opt_objstore_bucket), archive_keyid);
      }
    }
    if (!ss.is_succ()) {
      // If not exists, ignore it.
      if (ss.error_code() == objstore::Errors::SE_NO_SUCH_KEY) {
        err_msg.append(" ignore it, error=");
        err_msg.append(ss.error_message());
        LogErr(WARNING_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG,
               err_msg.c_str());
        continue;
      }
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_PURGE_LOG, err_msg.c_str());
      error = 1;
      goto err;
    }
  }

err:
  return error;
}

/**
 * @brief Remove line entry from index file.
 * Remove the snapshot by first removing the index, then deleting the archive
 * object. As long as the index is successfully deleted, the purge is considered
 * successful. If the archive object deletion fails, it will be treated as
 * garbage and purged in the next attempt.
 * @param log_info
 * @return int
 */
int Consistent_archive::remove_line_from_index(LOG_INFO *log_info,
                                               Archive_type arch_type) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("remove_line_from_index"));
  DBUG_EXECUTE_IF("fault_injection_remove_line_from_index", { return 1; });
  IO_CACHE *crash_safe_index_file = nullptr;
  IO_CACHE *index_file = nullptr;
  std::string index_file_name;
  std::string err_msg{};

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file = &m_crash_safe_mysql_clone_index_file;
    index_file = &m_mysql_clone_index_file;
    index_file_name.assign(m_mysql_clone_index_file_name);
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file = &m_crash_safe_se_backup_index_file;
    index_file = &m_se_backup_index_file;
    index_file_name.assign(m_se_backup_index_file_name);
  } else {
    assert(arch_type == ARCHIVE_SNAPSHOT_FILE);
    crash_safe_index_file = &m_crash_safe_consistent_snapshot_index_file;
    index_file = &m_consistent_snapshot_index_file;
    index_file_name.assign(m_consistent_snapshot_index_file_name);
  }

  if (!my_b_inited(index_file)) {
    goto err;
  }

  if (open_crash_safe_index_file(arch_type)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to open_crash_safe_index_file in remove_line_from_index.");
    goto err;
  }

  if (copy_file(index_file, crash_safe_index_file,
                log_info->index_file_start_offset)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to copy_file in remove_line_from_index.");
    goto err;
  }

  if (close_crash_safe_index_file(arch_type)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to close_crash_safe_index_file in remove_line_from_index.");
    goto err;
  }

  if (move_crash_safe_index_file_to_index_file(arch_type)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to move_crash_safe_index_file_to_index_file in "
           "remove_line_from_index.");
    goto err;
  }

  return 0;

err:
  return LOG_INFO_IO;
}

int Consistent_archive::show_innodb_persistent_files(
    std::vector<objstore::ObjectMeta> &objects) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("show_innodb_persistent_files"));
  int error = 0;
  if (snapshot_objstore != nullptr) {
    bool finished = false;
    std::string start_after;
    std::string innodb_prefix;
    innodb_prefix.assign(m_archive_dir);
    innodb_prefix.append(CONSISTENT_INNODB_ARCHIVE_BASENAME);
    do {
      std::vector<objstore::ObjectMeta> tmp_objects;
      // Only return the files and directories at the first-level directory.
      // innodb_archive_000001.tar or innodb_archive_000001.tar.gz
      // or innodb_archive_000001/
      objstore::Status ss =
          snapshot_objstore->list_object(std::string_view(opt_objstore_bucket),
                                         std::string_view(innodb_prefix), false,
                                         start_after, finished, tmp_objects);
      if (!ss.is_succ()) {
        std::string err_msg;
        if (ss.error_code() == objstore::Errors::SE_NO_SUCH_KEY) {
          error = 0;
          break;
        }
        error = 1;
        err_msg.assign("list innodb from object store failed: ");
        err_msg.append(CONSISTENT_INNODB_ARCHIVE_BASENAME);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
        break;
      }
      // Append object.
      objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
    } while (finished == false);
  }
  return error;
}

int Consistent_archive::show_se_persistent_files(
    std::vector<objstore::ObjectMeta> &objects) {
  DBUG_TRACE;
  int error = 0;
  if (snapshot_objstore != nullptr) {
    bool finished = false;
    std::string start_after;
    std::string se_prefix;
    se_prefix.assign(m_archive_dir);
    se_prefix.append(CONSISTENT_SE_ARCHIVE_BASENAME);
    do {
      std::vector<objstore::ObjectMeta> tmp_objects;
      // Only return the files and directories at the first-level directory.
      // innodb_archive_000001.tar or innodb_archive_000001.tar.gz
      // or innodb_archive_000001/
      objstore::Status ss = snapshot_objstore->list_object(
          std::string_view(opt_objstore_bucket), se_prefix, false, start_after,
          finished, tmp_objects);
      if (!ss.is_succ()) {
        std::string err_msg;
        if (ss.error_code() == objstore::Errors::SE_NO_SUCH_KEY) {
          error = 0;
          break;
        }
        error = 1;
        err_msg.assign("list smartengine from object store failed: ");
        err_msg.append(CONSISTENT_SE_ARCHIVE_BASENAME);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
        break;
      }
      // Append object to result vector.
      objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
    } while (finished == false);
  }
  return error;
}

int Consistent_archive::show_se_backup_snapshot(
    THD *thd, std::vector<uint64_t> &backup_ids) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("show_se_backup_snapshot"));
  plugin_ref plugin;

  plugin = my_plugin_lock_by_name(nullptr, {STRING_WITH_LEN("smartengine")},
                                  MYSQL_STORAGE_ENGINE_PLUGIN);
  if (plugin == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "smartengine plugin is not loaded");
    return true;
  }
  handlerton *hton = plugin_data<handlerton *>(plugin);
  // If release failed, already return success, only report warning.
  if (hton->list_backup_snapshots(thd, backup_ids)) {
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "Failed to list smartengine snapshot.");
  }
  plugin_unlock(nullptr, plugin);
  return false;
}

/**
 * @brief Extract the term value from the index file name.
 * Format like snapshot.000001.index
 */
static uint64_t extract_term_from_index_file(const char *index_file_name) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("extract_term_from_index_file"));
  uint64_t term = 0;
  size_t first_dot = 0;
  size_t second_dot = 0;
  std::string index_file;
  index_file.assign(index_file_name);
  first_dot = index_file.find('.');
  if (first_dot == std::string::npos) {
    return term;
  }
  second_dot = index_file.find('.', first_dot + 1);
  if (second_dot == std::string::npos) {
    return term;
  }
  std::string term_str = index_file.substr(first_dot + 1, second_dot);
  term = std::stoull(term_str);
  return term;
}

/**
 * @brief Remove file or directory.
 * @param file
 */
static bool remove_file(const std::string &file) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("remove_file"));
  uint i = 0;
  MY_DIR *dir = nullptr;
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
  Copy content of 'from' file from offset to 'to' file.

  - We do the copy outside of the IO_CACHE as the cache
  buffers would just make things slower and more complicated.
  In most cases the copy loop should only do one read.

  @param from          File to copy.
  @param to            File to copy to.
  @param offset        Offset in 'from' file.


  @retval
    0    ok
  @retval
    -1    error
*/
static bool copy_file(IO_CACHE *from, IO_CACHE *to, my_off_t offset) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("copy_file"));
  int bytes_read;
  uchar io_buf[IO_SIZE * 2];

  mysql_file_seek(from->file, offset, MY_SEEK_SET, MYF(0));
  while (true) {
    if ((bytes_read = (int)mysql_file_read(from->file, io_buf, sizeof(io_buf),
                                           MYF(MY_WME))) < 0)
      goto err;
    if (!bytes_read) break;  // end of file
    if (mysql_file_write(to->file, io_buf, bytes_read, MYF(MY_WME | MY_NABP)))
      goto err;
  }

  return false;

err:
  return true;
}

static bool file_has_suffix(const std::string &sfx, const std::string &path) {
  DBUG_TRACE;
  return (path.size() >= sfx.size() &&
          path.compare(path.size() - sfx.size(), sfx.size(), sfx) == 0);
}

static time_t calculate_auto_purge_lower_time_bound() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("calculate_auto_purge_lower_time_bound"));
  if (DBUG_EVALUATE_IF("reserve_all_consistent_snapshots", true, false)) {
    return static_cast<time_t>(std::numeric_limits<time_t>::min());
  }

  int64 expiration_time = 0;
  int64 current_time = time(nullptr);

  expiration_time = current_time - opt_consistent_snapshot_expire_seconds;

  // check for possible overflow conditions (4 bytes time_t)
  if (expiration_time < std::numeric_limits<time_t>::min())
    expiration_time = std::numeric_limits<time_t>::min();

  return static_cast<time_t>(expiration_time);
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

static size_t convert_datetime_to_str(char *str, time_t ts) {
  struct tm *res;
  size_t len;
  DBUG_TRACE;
  struct tm tm_tmp;
  gmtime_r(&ts, (res = &tm_tmp));
  len = snprintf(str, iso8601_size, "%04d-%02d-%02d %02d:%02d:%02d",
                 res->tm_year + 1900, res->tm_mon + 1, res->tm_mday,
                 res->tm_hour, res->tm_min, res->tm_sec);
  return len;
}

static const char *convert_archive_progress_to_str(
    Consistent_snapshot_archive_progress archive_progress) {
  const char *ret = nullptr;
  switch (archive_progress) {
    case STAGE_NONE:
      ret = "NONE";
      break;
    case STAGE_ACQUIRE_BACKUP_LOCK:
      ret = "ACQURIE_BACKUP_LOCK";
      break;
    case STAGE_INNODB_CLONE:
      ret = "INNODB SNAPSHOT";
      break;
    case STAGE_INNODB_SNAPSHOT_ARCHIVE:
      ret = "INNODB_SNAPSHOT_ARCHIVE";
      break;
    case STAGE_RELEASE_BACKUP_LOCK:
      ret = "RELEASE_BACKUP_LOCK";
      break;
    case STAGE_SMARTENGINE_SNAPSHOT:
      ret = "SMARTENGINE_SNAPSHOT";
      break;
    case STAGE_SMARTENGINE_SNAPSHOT_ARCHIVE:
      ret = "SMARTENGINE_SNAPSHOT_ARCHIVE";
      break;
    case STAGE_WAIT_BINLOG_ARCHIVE:
      ret = "WAIT_BINLOG_ARCHIVE";
      break;
    case STAGE_WRITE_CONSISTENT_SNAPSHOT_INDEX:
      ret = "WRITE_SNAPSHOT_INDEX";
      break;
    case STAGE_FAIL:
      ret = "FAIL";
      break;
    case STAGE_END:
      ret = "END";
      break;
    default:
      ret = "NONE";
      break;
  }
  return ret;
}
