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

#include "sql/binlog_archive.h"

#include <fstream>
#include <sstream>
#include <string>

#include "libbinlogevents/include/binlog_event.h"  // binary_log::max_log_event_size
#include "mf_wcomp.h"                              // wild_one, wild_many
#include "my_dbug.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/components/services/log_shared.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/psi/mysql_thread.h"
#include "objstore.h"
#include "sql/basic_istream.h"
#include "sql/basic_ostream.h"
#include "sql/binlog.h"
#include "sql/binlog_istream.h"
#include "sql/binlog_ostream.h"
#include "sql/binlog_reader.h"
#include "sql/consensus_log_event.h"
#include "sql/consistent_archive.h"
#include "sql/debug_sync.h"
#include "sql/derror.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/log.h"
#include "sql/log_event.h"
#include "sql/mdl.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/protocol_classic.h"
#include "sql/rpl_handler.h"
#include "sql/rpl_io_monitor.h"
#include "sql/rpl_source.h"
#include "sql/sql_parse.h"
#include "unsafe_string_append.h"

const float Binlog_archive::PACKET_GROW_FACTOR = 2.0;
const float Binlog_archive::PACKET_SHRINK_FACTOR = 0.5;

// Global binlog archive thread object
static Binlog_archive mysql_binlog_archive;
static my_thread_handle mysql_binlog_archive_pthd;

static mysql_mutex_t m_binlog_archive_run_lock;
static mysql_cond_t m_binlog_archive_run_cond;

#ifdef HAVE_PSI_INTERFACE
static PSI_thread_key THR_KEY_binlog_archive;
static PSI_thread_key THR_KEY_binlog_archive_worker;
static PSI_thread_key THR_KEY_binlog_archive_update_index_worker;
static PSI_mutex_key PSI_binlog_archive_lock_key;
static PSI_cond_key PSI_binlog_archive_cond_key;
static PSI_mutex_key PSI_binlog_archive_worker_lock_key;
static PSI_cond_key PSI_binlog_archive_worker_cond_key;
static PSI_mutex_key PSI_binlog_archive_update_index_worker_lock_key;
static PSI_cond_key PSI_binlog_archive_update_index_worker_cond_key;
static PSI_mutex_key PSI_binlog_index_lock_key;
static PSI_mutex_key PSI_binlog_rotate_lock_key;

static PSI_mutex_key PSI_binlog_slice_lock_key;
static PSI_cond_key PSI_binlog_slice_queue_cond_key;
static PSI_cond_key PSI_binlog_slice_map_cond_key;

/** The instrumentation key to use for opening the log file. */
static PSI_file_key PSI_binlog_purge_archive_log_file_key;
/** The instrumentation key to use for opening the log file. */
static PSI_file_key PSI_binlog_archive_log_file_slice_key;
/** The instrumentation key to use for opening the log index file. */
static PSI_file_key PSI_binlog_archive_log_index_key;
/** The instrumentation key to use for opening a log index cache file. */
static PSI_file_key PSI_binlog_archive_log_index_cache_key;

static PSI_thread_info all_binlog_archive_threads[] = {
    {&THR_KEY_binlog_archive, "archive", "bin_arch", PSI_FLAG_AUTO_SEQNUM, 0,
     PSI_DOCUMENT_ME},
    {&THR_KEY_binlog_archive_worker, "archive", "bin_arch_w",
     PSI_FLAG_AUTO_SEQNUM, 0, PSI_DOCUMENT_ME},
    {&THR_KEY_binlog_archive_update_index_worker, "archive", "bin_arch_i",
     PSI_FLAG_AUTO_SEQNUM, 0, PSI_DOCUMENT_ME}};

static PSI_mutex_info all_binlog_archive_mutex_info[] = {
    {&PSI_binlog_archive_lock_key, "binlog_archive::mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_worker_lock_key, "binlog_archive::mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_update_index_worker_lock_key, "binlog_archive::mutex",
     0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_slice_lock_key, "binlog_archive_index::mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_index_lock_key, "binlog_archive_index::mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_rotate_lock_key, "binlog_archive_rotate::mutex", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_cond_info all_binlog_archive_cond_info[] = {
    {&PSI_binlog_archive_cond_key, "binlog_archive::condition", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_worker_cond_key, "binlog_archive::condition", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_update_index_worker_cond_key,
     "binlog_archive::condition", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_slice_queue_cond_key, "binlog_archive::condition", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_slice_map_cond_key, "binlog_archive::condition", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_file_info all_binlog_archive_files[] = {
    {&PSI_binlog_purge_archive_log_file_key, "binlog_archive_file", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_file_slice_key, "binlog_archive_slice", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_index_key, "binlog_archive_index", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_index_cache_key, "binlog_archive_index_cache", 0,
     0, PSI_DOCUMENT_ME}};
#endif

static bool copy_file(IO_CACHE *from, IO_CACHE *to, my_off_t offset);
static void *mysql_binlog_archive_action(void *arg);
static int compare_log_name(const char *log_1, const char *log_2);
static bool remove_file(const std::string &file);
static void root_directory(std::string in, std::string *out, std::string *left);
static bool recursive_create_dir(const std::string &dir,
                                 const std::string &root);
static time_t calculate_auto_purge_lower_time_bound();
static THD *set_thread_context();

Binlog_archive_worker::Binlog_archive_worker(Binlog_archive *archive,
                                             int worker_id)
    : m_archive(archive),
      m_worker_id(worker_id),
      m_current_slice(),
      m_thd(nullptr),
      m_thd_state(),
      atomic_binlog_archive_worker_waiting(true) {
  mysql_mutex_init(PSI_binlog_archive_worker_lock_key, &m_worker_run_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_binlog_archive_worker_cond_key, &m_worker_run_cond);
}

Binlog_archive_worker::~Binlog_archive_worker() {
  stop();
  mysql_mutex_destroy(&m_worker_run_lock);
  mysql_cond_destroy(&m_worker_run_cond);
}

/**
 * @brief Start the binlog archive worker thread.
 * @return true if the worker thread was successfully started, false otherwise.
 */
bool Binlog_archive_worker::start() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("start"));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id, "starting");
  mysql_mutex_lock(&m_worker_run_lock);
  if (mysql_thread_create(THR_KEY_binlog_archive_worker, &m_thread,
                          &connection_attrib, thread_launcher, (void *)this)) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
           "start failed");
    mysql_mutex_unlock(&m_worker_run_lock);
    return false;
  }
  thread_set_created();
  while (is_thread_alive_not_running()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive worker thread to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_worker_run_cond, &m_worker_run_lock, &abstime);
  }
  mysql_mutex_unlock(&m_worker_run_lock);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id, "started");
  return true;
}

void Binlog_archive_worker::stop() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("stop"));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
         "terminate begin");
  mysql_mutex_lock(&m_worker_run_lock);
  if (is_thread_dead()) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
           "terminated");
    mysql_mutex_unlock(&m_worker_run_lock);
    return;
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
         "terminating binlog archive worker");
  mysql_mutex_unlock(&m_worker_run_lock);
  terminate_binlog_archive_worker_thread();
  /* Wait until the thread is terminated. */
  my_thread_join(&m_thread, nullptr);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
         "terminate end");
}

/**
 * @brief Binlog archive thread terminate worker thread.
 *
 * @return true if the worker thread was successfully terminated, false
 * otherwise.
 */
int Binlog_archive_worker::terminate_binlog_archive_worker_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("terminate_binlog_archive_worker_thread"));
  mysql_mutex_lock(&m_worker_run_lock);
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive thread to stop"));
    if (m_thd_state.is_initialized()) {
      mysql_mutex_lock(&m_thd->LOCK_thd_data);
      m_thd->awake(THD::KILL_CONNECTION);
      mysql_mutex_unlock(&m_thd->LOCK_thd_data);
    }
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_worker_run_cond, &m_worker_run_lock, &abstime);
  }
  assert(m_thd_state.is_thread_dead());
  mysql_mutex_unlock(&m_worker_run_lock);
  return 0;
}

/**
 * @brief Worker thread function for persisting binlog slices to the object
 * store.
 * @return void* Thread return value (nullptr)
 */
void *Binlog_archive_worker::worker_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("worker_thread"));
  Binlog_archive *archive = m_archive;

  m_thd = set_thread_context();

  mysql_mutex_lock(&m_worker_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_worker_run_cond);
  mysql_mutex_unlock(&m_worker_run_lock);

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
         "thread running");
  while (true) {
    uint32_t retry = 5;
    Binlog_expected_slice slice;
    bool is_slice_persisted = false;
    uint64_t slice_queue_map_term = 0;

    mysql_mutex_lock(&archive->m_slice_mutex);
    // Wait for the slice to be available.
    m_thd->ENTER_COND(&archive->m_queue_cond, &archive->m_slice_mutex, nullptr,
                      nullptr);
    set_binlog_archive_worker_waiting(true);
    while (archive->m_expected_slice_queue.empty() &&
           !unlikely(m_thd->killed)) {
      // Wait for signal from the binlog archive thread that the new expected
      // slice is added to the queue. Or the worker thread is killed.
      mysql_cond_wait(&archive->m_queue_cond, &archive->m_slice_mutex);
    }
    // When only the queue is empty and the worker thread is killed, exit.
    // If the queue is not empty, the worker thread will continue to process
    // the slice until the queue is empty.
    if (archive->m_expected_slice_queue.empty() && unlikely(m_thd->killed)) {
      mysql_mutex_unlock(&archive->m_slice_mutex);
      m_thd->EXIT_COND(nullptr);
      break;
    }
    // The worker thread is no longer waiting.
    set_binlog_archive_worker_waiting(false);
    // Get the slice from the queue.
    archive->m_expected_slice_queue.de_queue(&slice);
    slice_queue_map_term = archive->get_slice_queue_map_term();
    mysql_mutex_unlock(&archive->m_slice_mutex);
    m_thd->EXIT_COND(nullptr);

    // Signal the binlog archive thread that wait, because the slice queue
    // is full.
    mysql_cond_signal(&archive->m_queue_cond);
    m_current_slice = slice;

    // Persist the slice to the object store.
    while (retry-- > 0) {
      bool error = false;
      DBUG_EXECUTE_IF("fault_injection_put_slice_to_objstore", {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
               "fault_injection_put_slice_to_objstore");
        error = true;
      });
      if (error) {
        is_slice_persisted = false;
        break;
      }
      std::string err_msg;
      objstore::Status ss = m_archive->get_objstore()->put_object(
          std::string_view(opt_objstore_bucket), slice.m_slice_keyid,
          std::string_view(slice.slice_data_cache), false);
      err_msg.append(" put slice object ");
      err_msg.append(slice.m_slice_keyid);
      err_msg.append(" size=");
      err_msg.append(std::to_string(slice.m_slice_bytes_written));
      err_msg.append(" ");
      err_msg.append(std::to_string(slice.m_file_seq));
      err_msg.append("/");
      err_msg.append(std::to_string(slice.m_slice_seq));

      if (!ss.is_succ()) {
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
               err_msg.c_str());
        my_sleep(1000);
        is_slice_persisted = false;
        continue;
      }
      is_slice_persisted = true;
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id,
             err_msg.c_str());
      break;
    }
    // Notify the binlog archive thread that the slice has been persisted.
    // After dequeuing the slice, m_slice_mutex is unlocked. So maybe the slice
    // map is reinited, so we need to check if m_slice_queue_and_map_term
    // changed before notify the slice persisted.
    m_archive->notify_slice_persisted(slice, is_slice_persisted,
                                      slice_queue_map_term);
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_WORKER_LOG, m_worker_id, "thread end");

  mysql_mutex_lock(&m_worker_run_lock);
  delete m_thd;
  m_thd = nullptr;
  m_thd_state.set_terminated();
  mysql_cond_broadcast(&m_worker_run_cond);
  mysql_mutex_unlock(&m_worker_run_lock);
  my_thread_end();
  my_thread_exit(nullptr);
}

Binlog_archive_update_index_worker::Binlog_archive_update_index_worker(
    Binlog_archive *archive)
    : m_archive(archive),
      m_thd(nullptr),
      m_thd_state(),
      atomic_update_index_failed(false),
      atomic_update_index_waiting(true) {
  mysql_mutex_init(PSI_binlog_archive_update_index_worker_lock_key,
                   &m_worker_run_lock, MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_binlog_archive_update_index_worker_cond_key,
                  &m_worker_run_cond);
}

Binlog_archive_update_index_worker::~Binlog_archive_update_index_worker() {
  stop();
  mysql_mutex_destroy(&m_worker_run_lock);
  mysql_cond_destroy(&m_worker_run_cond);
}

/**
 * @brief Start the binlog archive worker thread.
 * @return true if the worker thread was successfully started, false otherwise.
 */
bool Binlog_archive_update_index_worker::start() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("start"));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG, "starting");
  mysql_mutex_lock(&m_worker_run_lock);
  if (mysql_thread_create(THR_KEY_binlog_archive_update_index_worker, &m_thread,
                          &connection_attrib, thread_launcher, (void *)this)) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
           "start failed");
    mysql_mutex_unlock(&m_worker_run_lock);
    return false;
  }
  thread_set_created();
  while (is_thread_alive_not_running()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive worker thread to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_worker_run_cond, &m_worker_run_lock, &abstime);
  }
  mysql_mutex_unlock(&m_worker_run_lock);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG, "started");
  return true;
}

void Binlog_archive_update_index_worker::stop() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("stop"));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
         "terminate begin");
  mysql_mutex_lock(&m_worker_run_lock);
  if (is_thread_dead()) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
           "terminated");
    mysql_mutex_unlock(&m_worker_run_lock);
    return;
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
         "terminating binlog update index worker");
  mysql_mutex_unlock(&m_worker_run_lock);
  terminate_binlog_archive_update_index_worker();
  /* Wait until the thread is terminated. */
  my_thread_join(&m_thread, nullptr);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
         "terminate end");
}

/**
 * @brief Binlog archive thread terminate worker thread.
 *
 * @return true if the worker thread was successfully terminated, false
 * otherwise.
 */
int Binlog_archive_update_index_worker::
    terminate_binlog_archive_update_index_worker() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("terminate_binlog_archive_update_index_worker"));
  mysql_mutex_lock(&m_worker_run_lock);
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep",
               ("Waiting for binlog archive update index worker to stop"));
    if (m_thd_state.is_initialized()) {
      mysql_mutex_lock(&m_thd->LOCK_thd_data);
      m_thd->awake(THD::KILL_CONNECTION);
      mysql_mutex_unlock(&m_thd->LOCK_thd_data);
    }
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_worker_run_cond, &m_worker_run_lock, &abstime);
  }
  assert(m_thd_state.is_thread_dead());
  mysql_mutex_unlock(&m_worker_run_lock);
  return 0;
}

void *Binlog_archive_update_index_worker::worker_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("worker_thread"));
  Binlog_archive *archive = m_archive;

  m_thd = set_thread_context();
  mysql_mutex_lock(&m_worker_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_worker_run_cond);
  mysql_mutex_unlock(&m_worker_run_lock);

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
         "thread running");
  while (true) {
    mysql_mutex_lock(&archive->m_slice_mutex);
    // Wait for the slice to be available.
    m_thd->ENTER_COND(&archive->m_map_cond, &archive->m_slice_mutex, nullptr,
                      nullptr);
    set_update_index_waiting(true);
    while (((archive->m_slice_status_map.empty() || is_update_index_failed()) &&
            !unlikely(m_thd->killed))) {
      // Wait for signal from the binlog archive thread that the new expected
      // slice is added to the queue. Or the worker thread is killed.
      // If the update index is failed, wait for the binlog archive thread to
      // reset the flag and clear the map.
      mysql_cond_wait(&archive->m_map_cond, &archive->m_slice_mutex);
    }
    // If update index worker is killed, need process the left slices in the
    // map until the map is empty.
    // Before the update index worker is killed, the binlog archive worker must
    // have been killed first. This is ensured by the binlog archive main
    // thread.
    if ((archive->m_slice_status_map.empty() || is_update_index_failed()) &&
        unlikely(m_thd->killed)) {
      mysql_mutex_unlock(&archive->m_slice_mutex);
      m_thd->EXIT_COND(nullptr);
      break;
    }
    set_update_index_waiting(false);
    mysql_mutex_unlock(&archive->m_slice_mutex);
    m_thd->EXIT_COND(nullptr);

    // If the update binlog index is failed, set the flag to true.
    // Binlog archive thread will check the flag every time it adds a new slice.
    if (!archive->update_index_file(true)) {
      set_update_index_failed(true);
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             "update index failed");
    }
  }

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG, "thread end");

  mysql_mutex_lock(&m_worker_run_lock);
  delete m_thd;
  m_thd = nullptr;
  m_thd_state.set_terminated();
  mysql_cond_broadcast(&m_worker_run_cond);
  mysql_mutex_unlock(&m_worker_run_lock);
  my_thread_end();
  my_thread_exit(nullptr);
}

/**
 * @brief Creates a binlog archive object and starts the binlog archive thread.
 * @return int 0 if the binlog archive thread was successfully started, 1
 * otherwise.
 */
int start_binlog_archive() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("start_binlog_archive"));
  // Check if the binlog is enabled.
  if (!opt_bin_log) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_STARTUP, "need enable binlog mode");
    return 0;
  }
  // Check if the binlog archive is enabled.
  if (!opt_binlog_archive || !opt_serverless) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_STARTUP,
           "the binlog persistent not enabled");
    return 0;
  }
  if (opt_binlog_archive_using_consensus_index) {
#ifdef WESQL_CLUSTER
    if (NO_HOOK(binlog_manager))
#endif
    {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_STARTUP,
             "must enable consensus plugin");
      return 1;
    }
  }

  if (!opt_binlog_archive_dir) {
    opt_binlog_archive_dir = mysql_tmpdir;
  }

  // Check if the mysql archive path is set.
  if (opt_binlog_archive_dir == nullptr) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_STARTUP,
           "must set binlog_archive_dir");
    return 1;
  }

  if (!opt_consistent_snapshot_persistent_on_objstore) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_STARTUP, "must set objectstore");
    return 1;
  }

  MY_STAT d_stat;
  // Check if opt_binlog_archive_dir exists.
  if (!my_stat(opt_binlog_archive_dir, &d_stat, MYF(0)) ||
      !MY_S_ISDIR(d_stat.st_mode) ||
      my_access(opt_binlog_archive_dir, (F_OK | W_OK))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_STARTUP,
           "binlog_archive_dir path not exists");
    return 1;
  }

  // data home can not include archive directory.
  if (test_if_data_home_dir(opt_binlog_archive_dir)) {
    std::string err_msg;
    err_msg.assign(
        "binlog_archive_dir is within the current data "
        "directory: ");
    err_msg.append(opt_binlog_archive_dir);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_STARTUP, err_msg.c_str());
    return 1;
  }

  // Check if archive binlog dir exists. If not exists, create it.
  // This directory is used for the local temporary storage of persisted
  // binlogs.
  char tmp_archive_binlog_dir[FN_REFLEN + 1] = {0};
  strmake(tmp_archive_binlog_dir, opt_binlog_archive_dir,
          sizeof(tmp_archive_binlog_dir) - BINLOG_ARCHIVE_SUBDIR_LEN - 1);
  strmake(
      convert_dirname(tmp_archive_binlog_dir, tmp_archive_binlog_dir, NullS),
      STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  convert_dirname(tmp_archive_binlog_dir, tmp_archive_binlog_dir, NullS);

  // remove local binlog archive dir and recreate it.
  if (remove_file(tmp_archive_binlog_dir)) {
    std::string err_msg;
    err_msg.assign("error ");
    err_msg.append(std::to_string(my_errno()));
    err_msg.append(", failed to remove archive dir ");
    err_msg.append(tmp_archive_binlog_dir);
    LogErr(WARNING_LEVEL, ER_BINLOG_ARCHIVE_STARTUP, err_msg.c_str());
  }
  if (my_mkdir(tmp_archive_binlog_dir, 0777, MYF(0))) {
    std::string err_msg;
    err_msg.assign("error ");
    err_msg.append(std::to_string(my_errno()));
    err_msg.append(", failed to create archive dir ");
    err_msg.append(tmp_archive_binlog_dir);
    LogErr(WARNING_LEVEL, ER_BINLOG_ARCHIVE_STARTUP, err_msg.c_str());
  }

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_STARTUP, "start binlog archive");

  // persist the binlog to the object store.
  std::string obj_error_msg;
  std::string err_msg;
  std::string_view endpoint(
      opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");
  objstore::ObjectStore *objstore =
      objstore::create_object_store(std::string_view(opt_objstore_provider),
                                    std::string_view(opt_objstore_region),
                                    opt_objstore_endpoint ? &endpoint : nullptr,
                                    opt_objstore_use_https, obj_error_msg);
  if (!objstore) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_CREATE_OBJECT_STORE,
           opt_objstore_provider, opt_objstore_region,
           std::string(endpoint).c_str(), opt_objstore_bucket,
           opt_objstore_use_https ? "true" : "false",
           !obj_error_msg.empty() ? obj_error_msg.c_str() : "");
    return 1;
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_CREATE_OBJECT_STORE,
         opt_objstore_provider, opt_objstore_region,
         std::string(endpoint).c_str(), opt_objstore_bucket,
         opt_objstore_use_https ? "true" : "false", "");
  if (strcmp(opt_objstore_provider, "local") == 0) {
    objstore->create_bucket(std::string_view(opt_objstore_bucket));
  }
  mysql_binlog_archive.set_objstore(objstore);

  mysql_mutex_lock(&m_binlog_archive_run_lock);
  if (mysql_thread_create(THR_KEY_binlog_archive, &mysql_binlog_archive_pthd,
                          &connection_attrib, mysql_binlog_archive_action,
                          (void *)&mysql_binlog_archive)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_STARTUP,
           "Failed to create binlog archive thread");
    mysql_mutex_unlock(&m_binlog_archive_run_lock);
    return 1;
  }

  mysql_binlog_archive.thread_set_created();

  while (mysql_binlog_archive.is_thread_alive_not_running()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive thread to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_binlog_archive_run_cond, &m_binlog_archive_run_lock,
                         &abstime);
  }
  mysql_mutex_unlock(&m_binlog_archive_run_lock);
  return 0;
}

/**
 * @brief stop the binlog archive thread.
 * @note must stop consistent snapshot thread, before stop the binlog archive
 * thread. Consistent snapshot thread depends on the binlog archive thread.
 * @return void
 */
void stop_binlog_archive() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("stop_binlog_archive"));
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "terminate begin");

  mysql_mutex_lock(&m_binlog_archive_run_lock);
  if (mysql_binlog_archive.is_thread_dead()) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "terminated");
    mysql_mutex_unlock(&m_binlog_archive_run_lock);
    return;
  }
  mysql_mutex_unlock(&m_binlog_archive_run_lock);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "terminating binlog archive");
  mysql_binlog_archive.terminate_binlog_archive_thread();
  /* Wait until the thread is terminated. */
  my_thread_join(&mysql_binlog_archive_pthd, nullptr);
  if (mysql_binlog_archive.get_objstore()) {
    objstore::destroy_object_store(mysql_binlog_archive.get_objstore());
    mysql_binlog_archive.set_objstore(nullptr);
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "terminate end");
}

/**
 * @brief Archives the MySQL binlog.
 * @param arg The binlog archive object.
 */
static void *mysql_binlog_archive_action(void *arg) {
  Binlog_archive *archive = (Binlog_archive *)arg;
  archive->run();
  return nullptr;
}

class Binlog_archive::Event_allocator {
 public:
  enum { DELEGATE_MEMORY_TO_EVENT_OBJECT = false };

  void set_archiver(Binlog_archive *archiver) { m_archiver = archiver; }
  unsigned char *allocate(size_t size) {
    my_off_t event_offset = m_archiver->m_packet.length();
    if (m_archiver->grow_packet(size)) return nullptr;

    m_archiver->m_packet.length(event_offset + size);
    return pointer_cast<unsigned char *>(m_archiver->m_packet.ptr() +
                                         event_offset);
  }

  void deallocate(unsigned char *ptr [[maybe_unused]]) {}

 private:
  Binlog_archive *m_archiver = nullptr;
};

/**
 * @brief Constructs a Binlog_archive object.
 */
Binlog_archive::Binlog_archive()
    : m_expected_slice_queue(),
      m_slice_status_map(),
      m_thd(nullptr),
      m_thd_state(),
      m_description_event(BINLOG_VERSION, ::server_version),
      binlog_objstore(nullptr),
      m_diag_area(false),
      m_half_buffer_size_req_counter(0),
      m_new_shrink_size(PACKET_MIN_SIZE),
      m_index_file(),
      m_crash_safe_index_file(),
      m_purge_index_file() {
  m_consensus_term = 0;
  m_mysql_binlog_first_file = true;
  m_binlog_archive_last_index_number = 0;
  m_event_checksum_alg = binary_log::BINLOG_CHECKSUM_ALG_OFF;
  m_mysql_binlog_previouse_consensus_index = 0;
  m_slice_create_ts = 0;
  m_binlog_previouse_consensus_index = 0;
  m_slice_end_consensus_index = 0;
  m_mysql_end_consensus_index = 0;
  m_binlog_archive_last_event_end_pos = 0;
  m_binlog_last_event_type = binary_log::UNKNOWN_EVENT;
  m_binlog_last_event_type_str = nullptr;
  m_binlog_in_transaction = false;
  m_rotate_forbidden = false;
  m_slice_bytes_written = 0;
  m_binlog_archive_write_last_event_end_pos = 0;
  m_mysql_binlog_last_event_end_pos = 0;
  m_mysql_binlog_write_last_event_end_pos = 0;
  m_mysql_binlog_start_pos = 0;
  m_mysql_archive_dir[0] = '\0';
  m_mysql_binlog_archive_dir[0] = '\0';
  m_mysql_binlog_start_file[0] = '\0';
  m_binlog_archive_dir[0] = '\0';
  m_binlog_archive_file_name[0] = '\0';
  m_mysql_binlog_file_name[0] = '\0';
  m_index_local_file_name[0] = '\0';
  m_index_file_name[0] = '\0';
  m_crash_safe_index_local_file_name[0] = '\0';
  m_purge_index_file_name[0] = '\0';

  m_update_index_worker = nullptr;
  m_workers = nullptr;
  m_slice_queue_and_map_term = 0;
  m_persisted_binlog_file_name[0] = '\0';
  m_persisted_mysql_binlog_file_name[0] = '\0';
  m_persisted_mysql_binlog_last_event_end_pos = 0;
  m_persisted_binlog_last_event_end_pos = 0;
  m_persisted_slice_end_consensus_index = 0;
  m_persisted_binlog_previouse_consensus_index = 0;
}

/**
 * @brief Destructs the Binlog_archive object.
 */
Binlog_archive::~Binlog_archive() {}

/**
 * @brief Initializes the binlog archive object.
 */
void Binlog_archive::init_pthread_object() {
#ifdef HAVE_PSI_INTERFACE
  const char *category = "archive";
  int count_thread =
      static_cast<int>(array_elements(all_binlog_archive_threads));
  mysql_thread_register(category, all_binlog_archive_threads, count_thread);

  int count_mutex =
      static_cast<int>(array_elements(all_binlog_archive_mutex_info));
  mysql_mutex_register(category, all_binlog_archive_mutex_info, count_mutex);

  int count_cond =
      static_cast<int>(array_elements(all_binlog_archive_cond_info));
  mysql_cond_register(category, all_binlog_archive_cond_info, count_cond);

  int count_file = static_cast<int>(array_elements(all_binlog_archive_files));
  mysql_file_register(category, all_binlog_archive_files, count_file);

#endif

  mysql_cond_init(PSI_binlog_archive_cond_key, &m_binlog_archive_run_cond);
  mysql_mutex_init(PSI_binlog_archive_lock_key, &m_binlog_archive_run_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(PSI_binlog_rotate_lock_key, &m_rotate_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(PSI_binlog_index_lock_key, &m_index_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(PSI_binlog_slice_lock_key, &m_slice_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_binlog_slice_queue_cond_key, &m_queue_cond);
  mysql_cond_init(PSI_binlog_slice_map_cond_key, &m_map_cond);
}

/**
 * @brief Cleans up any resources used by the binlog archive.
 */
void Binlog_archive::deinit_pthread_object() {
  mysql_cond_destroy(&m_map_cond);
  mysql_cond_destroy(&m_queue_cond);
  mysql_mutex_destroy(&m_slice_mutex);
  mysql_mutex_destroy(&m_index_lock);
  mysql_mutex_destroy(&m_rotate_lock);
  mysql_mutex_destroy(&m_binlog_archive_run_lock);
  mysql_cond_destroy(&m_binlog_archive_run_cond);
}

/**
 * @brief Returns the binlog archive instance.
 * @return Binlog_archive*
 */
Binlog_archive *Binlog_archive::get_instance() { return &mysql_binlog_archive; }

/**
 * @brief Returns the binlog archive lock.
 *
 * @return mysql_mutex_t*
 */
mysql_mutex_t *Binlog_archive::get_binlog_archive_lock() {
  return &m_binlog_archive_run_lock;
}

/**
 * @brief Terminate the binlog archive thread.
 *
 * @return int
 */
int Binlog_archive::terminate_binlog_archive_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("terminate_binlog_archive_thread"));
  mysql_mutex_lock(&m_binlog_archive_run_lock);
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive thread to stop"));
    if (m_thd_state.is_initialized()) {
      mysql_mutex_lock(&m_thd->LOCK_thd_data);
      m_thd->awake(THD::KILL_CONNECTION);
      mysql_mutex_unlock(&m_thd->LOCK_thd_data);
    }
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_binlog_archive_run_cond, &m_binlog_archive_run_lock,
                         &abstime);
  }
  assert(m_thd_state.is_thread_dead());
  mysql_mutex_unlock(&m_binlog_archive_run_lock);
  return 0;
}

/**
 * @brief Initializes the THD context.
 *
 */
static THD *set_thread_context() {
  THD *thd = new THD;
  my_thread_init();
  thd->set_new_thread_id();
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  thd->get_protocol_classic()->init_net(nullptr);
  thd->system_thread = SYSTEM_THREAD_BACKGROUND;
  /* No privilege check needed */
  thd->security_context()->skip_grants();
  // Global_THD_manager::get_instance()->add_thd(thd);
  return thd;
}

/**
 * @brief Check if a string represents a valid number.
 * @param str String to check
 * @param res Pointer to store the parsed number (if not NULL)
 * @param allow_wildcards Whether to allow wildcard chars (* and ?) in the
 * number
 * @return true if string represents a valid number
 * @return false if string is not a valid number
 * @note Handles positive/negative integers and decimals
 */
static bool is_number(const char *str, ulonglong *res, bool allow_wildcards) {
  DBUG_TRACE;
  int flag = 0;
  const char *start = str;

  // Skip leading spaces
  while (*str++ == ' ')
    ;
  // Handle optional sign
  if (*--str == '-' || *str == '+') str++;
  // Process digits and wildcards before decimal
  while (my_isdigit(files_charset_info, *str) ||
         (allow_wildcards && (*str == wild_many || *str == wild_one))) {
    flag = 1;
    str++;
  }
  // Handle decimal point and trailing digits/wildcards
  if (*str == '.') {
    for (str++; my_isdigit(files_charset_info, *str) ||
                (allow_wildcards && (*str == wild_many || *str == wild_one));
         str++, flag = 1)
      ;
  }
  // Invalid if any chars remain or no digits found
  if (*str != 0 || flag == 0) return false;
  // Store result if pointer provided
  if (res) *res = atol(start);
  return true; /* Number ok */
}

/**
 * @brief Runs the binlog archiving process.
 */
void Binlog_archive::run() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("Binlog_archive::run"));
  std::string err_msg{};
  LOG_ARCHIVED_INFO log_info{};
  int error = 1;

  m_thd = set_thread_context();

  mysql_mutex_lock(&m_binlog_archive_run_lock);
  m_thd_state.set_initialized();
  mysql_cond_broadcast(&m_binlog_archive_run_cond);
  mysql_mutex_unlock(&m_binlog_archive_run_lock);

  strmake(m_mysql_archive_dir, opt_binlog_archive_dir,
          sizeof(m_mysql_archive_dir) - 1);
  convert_dirname(m_mysql_archive_dir, m_mysql_archive_dir, NullS);
  strmake(strmake(m_mysql_binlog_archive_dir, m_mysql_archive_dir,
                  sizeof(m_mysql_binlog_archive_dir) -
                      BINLOG_ARCHIVE_SUBDIR_LEN - 1),
          STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  // if m_binlog_archive_dir dir not exists, start_binlog_archive() will create
  // it.
  convert_dirname(m_mysql_binlog_archive_dir, m_mysql_binlog_archive_dir,
                  NullS);

  // Binlog archive object directory prefix.
  std::string binlog_objectstore_path(opt_repo_objstore_id);
  binlog_objectstore_path.append(FN_DIRSEP);
  binlog_objectstore_path.append(opt_branch_objstore_id);
  binlog_objectstore_path.append(FN_DIRSEP);
  binlog_objectstore_path.append(BINLOG_ARCHIVE_SUBDIR);
  binlog_objectstore_path.append(FN_DIRSEP);
  strmake(m_binlog_archive_dir, binlog_objectstore_path.c_str(),
          sizeof(m_binlog_archive_dir) - 1);

  mysql_mutex_lock(&m_binlog_archive_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_binlog_archive_run_cond);
  mysql_mutex_unlock(&m_binlog_archive_run_lock);

  assert(!m_expected_slice_queue.inited_queue);
  m_expected_slice_queue.avail = 0;
  m_expected_slice_queue.entry = 0;
  m_expected_slice_queue.len = 0;
  m_expected_slice_queue.capacity = 4 * opt_binlog_archive_parallel_workers;
  m_expected_slice_queue.inited_queue = true;
  Binlog_expected_slice slice;
  m_expected_slice_queue.m_Q.resize(m_expected_slice_queue.capacity, slice);
  assert(m_expected_slice_queue.m_Q.size() == m_expected_slice_queue.capacity);

  err_msg.assign("Binlog archive thread running");
#ifdef WESQL_CLUSTER
  err_msg.append(is_consensus_replication_log_mode() ? " in Logger mode"
                                                     : " in Data mode");
#endif
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());

  // create binlog archive update index worker thread.
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG,
         "create binlog archive update index worker");
  m_update_index_worker = new Binlog_archive_update_index_worker(this);
  if (!m_update_index_worker->start()) goto end;

  // create binlog archive worker thread.
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "create binlog archive worker");
  m_workers = static_cast<Binlog_archive_worker **>(my_malloc(
      PSI_NOT_INSTRUMENTED,
      opt_binlog_archive_parallel_workers * sizeof(Binlog_archive_worker *),
      MYF(MY_WME)));
  if (!m_workers) goto end;
  for (ulong i = 0; i < opt_binlog_archive_parallel_workers; ++i) {
    Binlog_archive_worker *worker = new Binlog_archive_worker(this, i);
    m_workers[i] = worker;
    if (!worker->start()) goto end;
  }

  for (;;) {
    my_off_t last_binlog_slice_max_num = 0;
    uint64_t last_binlog_consensus_index = 0;
    uint64_t last_binlog_index_num = 0;
    uint64_t previous_consensus_index;
    uint64_t slice_end_consensus_index;
    my_off_t mysql_start_pos = 0;
    uint64_t consensus_term = 0;
    char mysql_start_binlog[FN_REFLEN + 1] = {0};
    char last_binlog_file_name[FN_REFLEN + 1] = {0};
    mysql_mutex_lock(&m_binlog_archive_run_lock);
    if (m_thd == nullptr || unlikely(m_thd->killed)) {
      err_msg.assign("run exit, persist end consensus index=");
      err_msg.append(std::to_string(m_slice_end_consensus_index));
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      mysql_mutex_unlock(&m_binlog_archive_run_lock);
      break;
    }
    mysql_mutex_unlock(&m_binlog_archive_run_lock);
    m_thd->clear_error();

#ifdef WESQL_CLUSTER
    // Check whether consensus role is leader and fetch leader term
    if (DBUG_EVALUATE_IF("fault_injection_binlog_archive_running", true,
                         false) ||
        !is_consensus_replication_state_leader(consensus_term)) {
      struct timespec abstime;
      set_timespec(&abstime, 1);
      mysql_mutex_lock(&m_binlog_archive_run_lock);
      // When demoted to a non-Leader role, the previous Leader role's consensus
      // term remains unchanged, making it easier to query the binlog persistent
      // view. m_consensus_term = 0;
      error = mysql_cond_timedwait(&m_binlog_archive_run_cond,
                                   &m_binlog_archive_run_lock, &abstime);
      mysql_mutex_unlock(&m_binlog_archive_run_lock);
      continue;
    }
    m_consensus_term = consensus_term;
#endif
    sprintf(m_index_file_name, "%s" BINLOG_ARCHIVE_CONSENSUS_TERM_EXT "%s",
            BINLOG_ARCHIVE_INDEX_FILE_BASENAME,
            static_cast<my_off_t>(m_consensus_term),
            BINLOG_ARCHIVE_INDEX_FILE_SUFFIX);

    mysql_mutex_lock(&m_index_lock);
    // First, close the binlog index that may have been opened by the
    // consistent_snapshot thread, as the binlog index opened by
    // consistent_snapshot may belong to an old term version. A new version of
    // the binlog index needs to be opened according to the new term.
    close_index_file();
    // open persistent binlog.index
    if (open_index_file()) {
      mysql_mutex_unlock(&m_index_lock);
      break;
    }
    // Find the last slice of the last binlog from persistent binlog.index.
    error = find_log_pos_by_name(&log_info, NullS);
    if (error == 0) {
      do {
        strmake(last_binlog_file_name, log_info.log_file_name,
                sizeof(last_binlog_file_name) - 1);
      } while (!(error = find_next_log(&log_info)));
      if (error != LOG_INFO_EOF) {
        close_index_file();
        mysql_mutex_unlock(&m_index_lock);
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
               "Find binlog from persistent binlog index failed");
        break;
      }
      my_off_t number = 0;
      is_number(last_binlog_file_name + strlen(BINLOG_ARCHIVE_BASENAME),
                &number, false);
      last_binlog_index_num = number;
      last_binlog_slice_max_num = log_info.slice_end_pos;
      previous_consensus_index = log_info.log_previous_consensus_index;
      slice_end_consensus_index = log_info.slice_end_consensus_index;
    } else if (error == LOG_INFO_EOF) {
      // log index file is empty.
      last_binlog_index_num = 0;
      last_binlog_file_name[0] = '\0';
      last_binlog_slice_max_num = BIN_LOG_HEADER_SIZE;
      previous_consensus_index = 0;
      slice_end_consensus_index = 0;
    } else {
      close_index_file();
      mysql_mutex_unlock(&m_index_lock);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed IO to find binlog from archive binlog index file");
      break;
    }
    mysql_mutex_unlock(&m_index_lock);
    mysql_start_binlog[0] = '\0';
    mysql_start_pos = BIN_LOG_HEADER_SIZE;

    // Generate mysql start binlog name and position.
    if (opt_binlog_archive_using_consensus_index) {
#ifdef WESQL_CLUSTER
      // get last consensus index from last persistent binlog
      if (last_binlog_file_name[0] != '\0' && !NO_HOOK(binlog_manager)) {
        std::string last_binlog_local_file;
        last_binlog_local_file.assign(m_mysql_binlog_archive_dir);
        last_binlog_local_file.append(last_binlog_file_name);

        err_msg.assign("local bulid persist start binlog=");
        err_msg.append(last_binlog_file_name);
        LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        last_binlog_consensus_index = slice_end_consensus_index;

        DBUG_EXECUTE_IF("check_binlog_archive_last_binlog_end_consensu_index", {
          merge_slice_to_binlog_file(last_binlog_file_name,
                                     last_binlog_local_file.c_str());
          if (RUN_HOOK(binlog_manager, get_unique_index_from_pos,
                       (last_binlog_local_file.c_str(), 0,
                        last_binlog_consensus_index))) {
            err_msg.assign(
                "get consensus index using persistent binlog failed: ");
            err_msg.append(last_binlog_file_name);
            LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
          }
          assert(slice_end_consensus_index == last_binlog_consensus_index);
        });

        if (last_binlog_consensus_index > 0) {
          // get mysql binlog and end positiion using consensus index.
          // generate mysql start binlog name and position.
          char start_file[FN_REFLEN + 1];
          my_off_t start_pos = 0;
          if (RUN_HOOK(binlog_manager, get_pos_from_unique_index,
                       (last_binlog_consensus_index, start_file, start_pos))) {
            err_msg.assign(
                "get mysql binlog and position using consensus index failed: ");
            err_msg.append(std::to_string(last_binlog_consensus_index));
            LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
            break;
          }
          // Only file name.
          strmake(mysql_start_binlog, start_file + dirname_length(start_file),
                  sizeof(mysql_start_binlog) - 1);
          mysql_start_pos = start_pos;
        } else {
          // If the persisted binlog does not contain any consensus index,
          // persistence will start from the first MySQL binlog.
          mysql_start_binlog[0] = '\0';
          mysql_start_pos = BIN_LOG_HEADER_SIZE;
        }
      }
#endif
    } else {
      // Generate mysql start binlog name and position.
      strmake(mysql_start_binlog, last_binlog_file_name,
              sizeof(mysql_start_binlog) - 1);
      mysql_start_pos = last_binlog_slice_max_num;
    }

    mysql_mutex_lock(&m_rotate_lock);
    m_mysql_binlog_start_pos = mysql_start_pos;
    strmake(m_mysql_binlog_start_file, mysql_start_binlog,
            sizeof(m_mysql_binlog_start_file) - 1);
    m_mysql_binlog_last_event_end_pos = m_mysql_binlog_start_pos;
    m_mysql_binlog_write_last_event_end_pos = m_mysql_binlog_start_pos;
    m_slice_bytes_written = 0;
    m_slice_create_ts = 0;
    m_slice_cache.clear();
    m_binlog_archive_write_last_event_end_pos = last_binlog_slice_max_num;
    m_binlog_archive_last_event_end_pos = last_binlog_slice_max_num;
    m_binlog_archive_start_consensus_index = last_binlog_consensus_index;
    m_binlog_previouse_consensus_index = previous_consensus_index;
    m_slice_end_consensus_index = slice_end_consensus_index;
    m_mysql_end_consensus_index = slice_end_consensus_index;
    m_binlog_archive_last_index_number = last_binlog_index_num;
    m_binlog_in_transaction = false;
    m_rotate_forbidden = false;
    m_mysql_binlog_first_file = true;
    // init in archive_init()
    m_mysql_binlog_file_name[0] = '\0';
    m_binlog_archive_file_name[0] = '\0';
    m_last_expected_file_seq = -1;
    m_last_expected_slice_seq = -1;

    // init last persisted binlog
    strmake(m_persisted_binlog_file_name, last_binlog_file_name,
            sizeof(m_persisted_binlog_file_name) - 1);
    strmake(m_persisted_mysql_binlog_file_name, mysql_start_binlog,
            sizeof(m_persisted_mysql_binlog_file_name) - 1);
    m_persisted_mysql_binlog_last_event_end_pos = m_mysql_binlog_start_pos;
    m_persisted_binlog_last_event_end_pos = last_binlog_slice_max_num;
    m_persisted_slice_end_consensus_index = slice_end_consensus_index;
    m_persisted_binlog_previouse_consensus_index = previous_consensus_index;
    mysql_mutex_unlock(&m_rotate_lock);

    // Before starting the binlog archive each time, the binlog slice queue and
    // map must be cleared.
    mysql_mutex_lock(&m_slice_mutex);
    assert(m_expected_slice_queue.empty());
    assert(m_slice_status_map.empty());
    m_slice_queue_and_map_term++;
    m_update_index_worker->set_update_index_failed(false);
    mysql_mutex_unlock(&m_slice_mutex);

    err_msg.assign("persistent start ");
    err_msg.append("term=");
    err_msg.append(std::to_string(m_consensus_term));
    err_msg.append(" persistent_start_binlog=");
    err_msg.append(last_binlog_file_name);
    err_msg.append(":");
    err_msg.append(std::to_string(last_binlog_slice_max_num));
    err_msg.append(" consensus_start_index=");
    err_msg.append(std::to_string(last_binlog_consensus_index));
    err_msg.append(" slice_end_index=");
    err_msg.append(std::to_string(slice_end_consensus_index));
    err_msg.append(" previous_index=");
    err_msg.append(std::to_string(previous_consensus_index));
    err_msg.append(" mysql_start_binlog=");
    err_msg.append(mysql_start_binlog);
    err_msg.append(":");
    err_msg.append(std::to_string(mysql_start_pos));
    err_msg.append(" slice_queue_map_term=");
    err_msg.append(std::to_string(m_slice_queue_and_map_term));
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    // The binlog archive only stops in the event of an error, such
    // as when the thread is killed, aborted, consensus role or term changed, or
    // if an API execution fails. In cases where the thread is killed or
    // aborted, the binlog archive thread must exit. For other errors, will
    // retry binlog archiving after waiting for a period of time.
    if (archive_binlogs()) {
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "persistent abort");
    }

    // persist last binlog slice to object store.
    // No matter what causes the binlog persistence to terminate, locally
    // cached binlog slices should be persisted to prevent data loss. If binlog
    // persistence terminates and exits while some MySQL binlogs have not been
    // persisted (for example, if MySQL generates a large amount of binlog data,
    // and binlog persistence is too slow to keep up), this could result in
    // incomplete binlog persistence. Therefore, to ensure complete binlog
    // persistence, it is essential to ensure that the local mysql binlogs are
    // still present after the cluster recovers.
    // Users can determine whether all binlogs have been fully persisted by
    // comparing the `consensus_index` from
    // `information_schema.BINLOG_PERSISTENT_TASK_INFO` with the `match_index`
    // obtained from `information_schema.wesql_cluster_global`.
    rotate_binlog_slice(0, true);
    mysql_mutex_lock(&m_rotate_lock);
    // If previous archived binlog slice is not closed, close it.
    m_slice_cache.clear();
    mysql_mutex_unlock(&m_rotate_lock);

    // Wait for all persisted binlog slices to be persisted to the object store
    // and the binlog index to be updated. And all binlog worker threads are
    // finished. And the binlog archive update index worker thread is finished.
    mysql_mutex_lock(&m_slice_mutex);
    while (m_thd != nullptr && !m_thd->killed) {
      bool all_waiting = true;
      for (ulong i = 0; i < opt_binlog_archive_parallel_workers; ++i) {
        Binlog_archive_worker *worker = m_workers[i];
        if (!worker->is_binlog_archive_worker_waiting()) {
          all_waiting = false;
          break;
        }
      }
      if (m_update_index_worker->is_update_index_failed()) {
        m_slice_status_map.clear();
        LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG,
               "update binlog index failed, clear slice status map");
      }
      if (m_expected_slice_queue.empty() && all_waiting &&
          m_slice_status_map.empty() &&
          m_update_index_worker->is_update_index_waiting()) {
        LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG,
               "all binlog archive worker and update index worker threads are "
               "finished");
        break;
      }

      struct timespec abstime;
      set_timespec(&abstime, 1);
      mysql_cond_timedwait(&m_queue_cond, &m_slice_mutex, &abstime);
    }
    mysql_mutex_unlock(&m_slice_mutex);

    mysql_mutex_lock(&m_index_lock);
    close_index_file();
    mysql_mutex_unlock(&m_index_lock);

    // If the thread is killed, exit.
    mysql_mutex_lock(&m_binlog_archive_run_lock);
    if (m_thd == nullptr || unlikely(m_thd->killed)) {
      err_msg.assign("run exit, persist end consensus index=");
      err_msg.append(std::to_string(m_slice_end_consensus_index));
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      mysql_mutex_unlock(&m_binlog_archive_run_lock);
      break;
    }
    mysql_mutex_unlock(&m_binlog_archive_run_lock);

    // Will retry binlog archiving
    // after waiting for a period of time
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_mutex_lock(&m_binlog_archive_run_lock);
    error = mysql_cond_timedwait(&m_binlog_archive_run_cond,
                                 &m_binlog_archive_run_lock, &abstime);
    mysql_mutex_unlock(&m_binlog_archive_run_lock);
  }

end:
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "stopping binlog archive worker");
  for (ulong i = 0; i < opt_binlog_archive_parallel_workers; ++i) {
    Binlog_archive_worker *worker = m_workers[i];
    delete worker;
  }
  if (m_workers) {
    my_free(m_workers);
    m_workers = nullptr;
  }

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG,
         "stopping binlog archive update index worker");
  // Finally, stop the binlog archive update index worker thread.
  // Wait for all persisted binlog slices to be persisted to the object store
  // and the binlog index to be updated.
  if (m_update_index_worker) {
    delete m_update_index_worker;
    m_update_index_worker = nullptr;
  }
  err_msg.assign("persisted end consensus index=");
  err_msg.append(std::to_string(m_persisted_slice_end_consensus_index));
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());

  // Update index worker maybe reopens the binlog index file, so close it.
  mysql_mutex_lock(&m_index_lock);
  close_index_file();
  mysql_mutex_unlock(&m_index_lock);

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "binlog archive thread end");

  // Free new Item_***.
  m_thd->free_items();
  m_thd->release_resources();
  mysql_mutex_lock(&m_binlog_archive_run_lock);
  delete m_thd;
  m_thd = nullptr;
  m_thd_state.set_terminated();
  mysql_cond_broadcast(&m_binlog_archive_run_cond);
  mysql_mutex_unlock(&m_binlog_archive_run_lock);
  my_thread_end();
  my_thread_exit(nullptr);
}

int Binlog_archive::archive_init() {
  DBUG_TRACE;
  THD *thd = m_thd;
  char index_entry_name[FN_REFLEN];
  char *name_ptr = nullptr;
  std::string err_msg;

  m_diag_area.reset_diagnostics_area();
  m_diag_area.reset_condition_info(thd);
  thd->push_diagnostics_area(&m_diag_area);
  m_mysql_linfo.thread_id = thd->thread_id();
  mysql_bin_log.register_log_info(&m_mysql_linfo);

  /* Initialize the buffer only once. */
  m_packet.mem_realloc(PACKET_MIN_SIZE);  // size of the buffer
  m_new_shrink_size = PACKET_MIN_SIZE;

  if (!mysql_bin_log.is_open()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "mysql binary log is not open");
    return 1;
  }

  // check start file
  if (m_mysql_binlog_start_file[0] != '\0') {
    mysql_bin_log.make_log_name(index_entry_name, m_mysql_binlog_start_file);
    name_ptr = index_entry_name;
  }

  if (mysql_bin_log.find_log_pos(&m_mysql_linfo, name_ptr, true)) {
    err_msg.assign("Could not find first log file name in mysql binary log: ");
    err_msg.append(name_ptr);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;
  }
  if (m_mysql_binlog_start_pos < BIN_LOG_HEADER_SIZE) {
    err_msg.assign(
        "requested source to start archive "
        "from position < 4: ");
    err_msg.append(std::to_string(m_mysql_binlog_start_pos));
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;
  }

  Binlog_read_error binlog_read_error;
  Binlog_ifile binlog_ifile(&binlog_read_error);
  if (binlog_ifile.open(m_mysql_linfo.log_file_name)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, binlog_read_error.get_str());
    return 1;
  }
  // If the start position is smaller than the file size,
  // it indicates that the file has been truncated by Consensus.
  // If a binlog has been truncated, it indicates that
  // there are incomplete transaction log in the binlog persisted to S3.
  // Therefore, we also need to truncate the binlog on S3.
  if (m_mysql_binlog_start_pos > binlog_ifile.length()) {
    err_msg.assign("requested source to start archive from ");
    err_msg.append("position ");
    err_msg.append(std::to_string(m_mysql_binlog_start_pos));
    err_msg.append("> file size ");
    err_msg.append(std::to_string(binlog_ifile.length()));
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;
  }
  mysql_mutex_lock(&m_rotate_lock);
  // init the first mysql binlog.
  strmake(m_mysql_binlog_file_name, m_mysql_linfo.log_file_name,
          sizeof(m_mysql_binlog_file_name) - 1);
  // init the last persistent binlog
  sprintf(m_binlog_archive_file_name, "%s" BINLOG_ARCHIVE_NUMBER_EXT,
          BINLOG_ARCHIVE_BASENAME,
          (ulonglong)m_binlog_archive_last_index_number);
  mysql_mutex_unlock(&m_rotate_lock);
  return 0;
}

bool Binlog_archive::consensus_leader_is_changed() {
#ifdef WESQL_CLUSTER
  // Check whether consensus role is leader
  uint64_t consensus_term = 0;
  if (!is_consensus_replication_state_leader(consensus_term)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "consensus role had changed to non-leader");
    return true;
  }
  if (m_consensus_term != consensus_term) {
    std::string err_msg;
    err_msg.assign("consensus leader term had changed ");
    err_msg.append(std::to_string(m_consensus_term));
    err_msg.append(" to ");
    err_msg.append(std::to_string(consensus_term));
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return true;
  }
#endif
  return false;
}

/**
 * @brief Archives the MySQL binlog files.
 *
 * @return int
 */
int Binlog_archive::archive_binlogs() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_binlogs"));
  std::string err_msg;
  int ret = 0;

  if (archive_init()) {
    mysql_bin_log.unregister_log_info(&m_mysql_linfo);
    m_thd->clear_error();
    m_thd->pop_diagnostics_area();
    return 1;
  }
  /* Binary event can be vary large. So set it to max allowed packet. */
  unsigned int max_event_size = binary_log::max_log_event_size;
  File_reader reader(opt_source_verify_checksum, max_event_size);
  my_off_t start_pos = m_mysql_binlog_start_pos;
  const char *log_file = m_mysql_linfo.log_file_name;
  bool is_index_file_reopened_on_binlog_disable = false;

  reader.allocator()->set_archiver(this);
  while (!m_thd->killed) {
    if (reader.open(log_file)) {
      err_msg.assign("mysql binlog open error: ");
      err_msg.append(log_file);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      ret = 1;
      break;
    }

    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG_START,
           m_mysql_linfo.log_file_name, start_pos);
    if (archive_binlog(reader, start_pos)) {
      ret = 1;
      break;
    }

    // After completing the archiving of each binlog file,
    // perform a purge binlog operation.
    if (auto_purge_logs()) {
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "auto purge binlog failed");
      ret = 1;
      break;
    }

    mysql_bin_log.lock_index();
    if (!mysql_bin_log.is_open()) {
      if (mysql_bin_log.open_index_file(mysql_bin_log.get_index_fname(),
                                        log_file, false)) {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
               "mysql binary log is not open and failed to open index file "
               "to retrieve next file");
        mysql_bin_log.unlock_index();
        ret = 1;
        break;
      }
      is_index_file_reopened_on_binlog_disable = true;
    }

    int error = mysql_bin_log.find_next_log(&m_mysql_linfo, false);
    mysql_bin_log.unlock_index();
    if (unlikely(error)) {
      if (is_index_file_reopened_on_binlog_disable)
        mysql_bin_log.close(LOG_CLOSE_INDEX, true /*need_lock_log=true*/,
                            true /*need_lock_index=true*/);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "could not find next mysql binary log");
      ret = 1;
      break;
    }

    start_pos = BIN_LOG_HEADER_SIZE;
    reader.close();
  }

  if (reader.is_open()) {
    reader.close();
  }

  mysql_bin_log.unregister_log_info(&m_mysql_linfo);
  m_thd->clear_error();
  m_thd->pop_diagnostics_area();

  return ret;
}

int Binlog_archive::archive_binlog(File_reader &reader, my_off_t start_pos) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_binlog"));

  if (unlikely(read_format_description_event(reader))) return 1;

  /*
    Maybe requesting a position which is in the middle of a file,
    so seek to the correct position.
  */
  if (reader.position() != start_pos && reader.seek(start_pos)) return 1;

  while (!m_thd->killed) {
    // Each time a larger end_pos is obtained, the consensus role is checked for
    // changes.
    auto [end_pos, code] = get_binlog_end_pos(reader);

    if (code) return 1;
    if (archive_events(reader, end_pos)) return 1;

    if (end_pos == 0) return 0;

    m_thd->killed.store(DBUG_EVALUATE_IF("simulate_kill_archive_binlog_file",
                                         THD::KILL_CONNECTION,
                                         m_thd->killed.load()));
  }
  return 1;
}

int Binlog_archive::read_format_description_event(File_reader &reader) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("read_format_description_event"));
  uchar *event_ptr = nullptr;
  uint32 event_len = 0;
  std::string err_msg;

  if (reader.read_event_data(&event_ptr, &event_len)) {
    if (reader.get_error_type() == Binlog_read_error::READ_EOF) {
      event_ptr = nullptr;
      event_len = 0;
    } else {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             Binlog_read_error(reader.get_error_type()).get_str());
      return 1;
    }
  }

  if (event_ptr == nullptr ||
      event_ptr[EVENT_TYPE_OFFSET] != binary_log::FORMAT_DESCRIPTION_EVENT) {
    err_msg.assign("Could not find format_description_event in binlog ");
    err_msg.append(m_mysql_linfo.log_file_name);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;
  }

  m_event_checksum_alg =
      Log_event_footer::get_checksum_alg((const char *)event_ptr, event_len);
  assert(m_event_checksum_alg < binary_log::BINLOG_CHECKSUM_ALG_ENUM_END ||
         m_event_checksum_alg == binary_log::BINLOG_CHECKSUM_ALG_UNDEF);

  Log_event *ev = nullptr;
  Binlog_read_error binlog_read_error = binlog_event_deserialize(
      event_ptr, event_len, &reader.format_description_event(), false, &ev);
  if (binlog_read_error.has_error()) {
    err_msg.assign(binlog_read_error.get_str());
    err_msg.append(" ");
    err_msg.append(m_mysql_linfo.log_file_name);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;
  }
  reader.set_format_description_event(
      dynamic_cast<Format_description_log_event &>(*ev));
  delete ev;

#ifdef WESQL_CLUSTER
  // Get mysql binlog previous consensus index
  uint64 previous_consensus_index = 0;
  if (!NO_HOOK(binlog_manager)) {
    if (RUN_HOOK(binlog_manager, get_unique_index_from_pos,
                 (m_mysql_linfo.log_file_name, BIN_LOG_HEADER_SIZE,
                  previous_consensus_index))) {
      err_msg.assign(
          "Could not find previouse consensus index in mysql binlog ");
      err_msg.append(m_mysql_linfo.log_file_name);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      return 1;
    }
    err_msg.assign("Found previouse consensus index in mysql binlog ");
    err_msg.append(m_mysql_linfo.log_file_name);
    err_msg.append(" previous_consensus_index=");
    err_msg.append(std::to_string(previous_consensus_index));
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
  }
  m_mysql_binlog_previouse_consensus_index = previous_consensus_index;
#endif
  return 0;
}

/**
 * @brief Archive events from the binlog file.
 *
 * @param reader
 * @param end_pos
 * @return int
 */
int Binlog_archive::archive_events(File_reader &reader, my_off_t end_pos) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_events"));
  THD *thd = m_thd;
  const char *log_file = m_mysql_linfo.log_file_name;
  my_off_t log_reader_pos = reader.position();

  // Check if the current reader position has reached the end of the file
  while (likely(log_reader_pos < end_pos) || end_pos == 0) {
    uchar *event_ptr = nullptr;
    uint32 event_len = 0;

    if (unlikely(thd->killed)) return 1;

    if (reset_transmit_packet(0)) return 1;
    size_t event_offset;
    event_offset = m_packet.length();

    // Read next event data.
    if (reader.read_event_data(&event_ptr, &event_len)) {
      if (reader.get_error_type() == Binlog_read_error::READ_EOF) {
        event_ptr = nullptr;
        event_len = 0;
      } else {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
               Binlog_read_error(reader.get_error_type()).get_str());
        return 1;
      }
    }

    if (event_ptr == nullptr) {
      if (end_pos == 0) return 0;  // Arrive the end of inactive file
      std::string err_msg;
      err_msg.assign("SYSTEM IO error log_reader_pos=");
      err_msg.append(std::to_string(log_reader_pos));
      err_msg.append(" atomic_binlog_end_pos=");
      err_msg.append(std::to_string(end_pos));
      /*
        It is reading events before end_pos of active binlog file. In theory,
        it should never return nullptr. But RESET MASTER doesn't check if
        there is any dump thread working. So it is possible that the active
        binlog file is reopened and truncated to 0 after RESET MASTER.
      */
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      return 1;
    }

    assert(log_reader_pos == reader.event_start_pos());

    assert(reinterpret_cast<char *>(event_ptr) ==
           (m_packet.ptr() + event_offset));

    DBUG_PRINT("info", ("Read event %s", Log_event::get_type_str(Log_event_type(
                                             (event_ptr)[EVENT_TYPE_OFFSET]))));

    log_reader_pos = reader.position();

    if (archive_event(reader, event_ptr, event_len, log_file, log_reader_pos))
      return 1;
  }
  return 0;
}

/**
 * @brief Write an binlog event from binlog with log_pos to the archive
 * binlog slice local cache.
 *
 * @param log_file mysql binlog of the read
 * @param log_pos mysql binlog end position of the read
 * @return int
 * @note The first event in the archive is not necessarily the first event in
 * the binlog; it could also be an event from the middle of the binlog.
 * If archiving starts from the middle of the mysql binlog, the persistence
 * position of the event may differ from its position in the local binlog, so
 * the end_log_pos of the event might need to be updated; this is limited to
 * events read from the first local binlog; subsequent binlog will maintain
 * the same persistence structure as the local binlog. This is because the
 * last persisted binlog may have come from the old leader, while the event
 * currently being prepared for persistence is from the new leader. The new
 * leader needs to continue persisting subsequent events from the located file
 * into the same persistent binlog.
 */
int Binlog_archive::archive_event(File_reader &reader, uchar *event_ptr,
                                  uint32 event_len, const char *log_file,
                                  my_off_t log_pos) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_event"));
  DBUG_EXECUTE_IF("fault_injection_archive_event", { return 1; });
  int error = 0;
  assert(log_file != nullptr);
  assert(log_pos >= BIN_LOG_HEADER_SIZE);

  Log_event_type type = (Log_event_type)event_ptr[EVENT_TYPE_OFFSET];
  uint32 len = uint4korr(event_ptr + EVENT_LEN_OFFSET);
  assert(len == event_len);
  my_off_t event_start_pos = reader.event_start_pos();
  uint32 event_end_pos = uint4korr(event_ptr + LOG_POS_OFFSET);
  assert((event_start_pos + event_len) == event_end_pos);
  assert(event_end_pos == log_pos);

  DBUG_PRINT("info",
             ("Archiving event of type %s", Log_event::get_type_str(type)));
  // An event of type `FORMAT_DESCRIPTION_EVENT` signifies the start of a new
  // binlog.
  if (type == binary_log::FORMAT_DESCRIPTION_EVENT) {
    mysql_mutex_lock(&m_rotate_lock);
    assert(m_rotate_forbidden == false);

    // When MySQL switches to a new binlog, the persistent binlog should also
    // follow suit. Before switching to a new persistent binlog, the remaining
    // events of the previous binlog need to be persisted.
    if (rotate_binlog_slice(m_mysql_binlog_write_last_event_end_pos, false) ==
        1) {
      error = 1;
      goto err_slice;
    }

    // switch next mysql binlog and generate first slice.
    if (new_binlog_slice(true, log_file, log_pos,
                         m_mysql_binlog_previouse_consensus_index)) {
      error = 1;
      goto err_slice;
    }
    // Write BINLOG_MAGIC to the binlog first slice.
    m_slice_cache.assign(BINLOG_MAGIC, BIN_LOG_HEADER_SIZE);
    m_slice_bytes_written = BIN_LOG_HEADER_SIZE;
    m_binlog_archive_write_last_event_end_pos = BIN_LOG_HEADER_SIZE;
    m_binlog_archive_last_event_end_pos = BIN_LOG_HEADER_SIZE;
    m_mysql_binlog_write_last_event_end_pos = BIN_LOG_HEADER_SIZE;
    m_mysql_binlog_last_event_end_pos = BIN_LOG_HEADER_SIZE;
    // Write Format_description_event to the binlog first slice.
    m_slice_cache.append(reinterpret_cast<const char *>(event_ptr), event_len);
    m_binlog_last_event_type = type;
    m_binlog_last_event_type_str = Log_event::get_type_str(type);
    m_slice_bytes_written += event_len;
    // Update the last archived event end of position.
    m_binlog_archive_write_last_event_end_pos = event_end_pos;
    m_mysql_binlog_write_last_event_end_pos = event_end_pos;
    mysql_mutex_unlock(&m_rotate_lock);
    goto suc;
  } else {
    switch (type) {
      case binary_log::QUERY_EVENT: {
        Query_log_event ev(reinterpret_cast<char *>(event_ptr),
                           &reader.format_description_event(),
                           binary_log::QUERY_EVENT);
        std::string query{ev.query};

        if (query == "BEGIN" || query.find("XA START") == 0) {
          assert(m_binlog_in_transaction == false);
          m_binlog_in_transaction = true;
        } else if (query == "COMMIT" || query == "ROLLBACK" ||
                   query.find("XA COMMIT") == 0 ||
                   query.find("XA ROLLBACK") == 0) {
          assert(m_binlog_in_transaction == true);
          m_binlog_in_transaction = false;
        } else if (is_atomic_ddl_event(&ev)) {
          assert(m_binlog_in_transaction == false);
          m_binlog_in_transaction = false;
        }
        break;
      }
      case binary_log::XID_EVENT: {
        assert(m_binlog_in_transaction == true);
        m_binlog_in_transaction = false;
        break;
      }
      case binary_log::XA_PREPARE_LOG_EVENT: {
        m_binlog_in_transaction = false;
        break;
      }
#ifdef WESQL_CLUSTER
      case binary_log::CONSENSUS_LOG_EVENT: {
        Consensus_log_event ev(reinterpret_cast<char *>(event_ptr), event_len,
                               &reader.format_description_event());
        m_mysql_end_consensus_index = ev.get_index();
        break;
      }
#endif
      default: {
        break;
      }
    }
  }

  mysql_mutex_lock(&m_rotate_lock);

  // If the local slice has not been created, it indicates that the archiving
  // started from the middle of the binlog. The first half of the
  // current binlog has already been persisted, and now is on
  // persisting the second half of the binlog.
  // It's to note that the subsequent events might be generated by the new
  // leader.
  if (m_slice_cache.empty()) {
    assert(log_pos > BIN_LOG_HEADER_SIZE);
    if (new_binlog_slice(false, log_file, log_pos, 0)) {
      error = 1;
      goto err_slice;
    }
  }

  m_binlog_last_event_type = type;
  m_binlog_last_event_type_str = Log_event::get_type_str(type);
  m_slice_bytes_written += event_len;
  // Record the last archived event end of position.
  m_binlog_archive_write_last_event_end_pos += event_len;
  m_mysql_binlog_write_last_event_end_pos = event_end_pos;

  // Here, the primary consideration is that the new Leader's current
  // `binlog.000004` event needs to be persisted into the `binlog.000001` file
  // generated by the old Leader. However, the structures of these two binlogs
  // might differ, so it may be necessary to adjust the `event_end_pos` of each
  // event.
  if (m_mysql_binlog_first_file) {
    if (event_end_pos != m_binlog_archive_write_last_event_end_pos) {
      int4store(event_ptr + LOG_POS_OFFSET,
                static_cast<uint32>(m_binlog_archive_write_last_event_end_pos));
      if (event_checksum_on()) {
        calc_event_checksum(event_ptr, event_len);
      }
      event_end_pos = uint4korr(event_ptr + LOG_POS_OFFSET);
    }
  } else {
    assert(m_binlog_archive_write_last_event_end_pos ==
           m_mysql_binlog_write_last_event_end_pos);
  }
  assert(m_binlog_archive_write_last_event_end_pos == event_end_pos);
  m_slice_cache.append(reinterpret_cast<const char *>(event_ptr), event_len);

  // rotate binlog slice, if the binlog slice size is too large.
  // Whenever the current position is at a transaction boundary, rotate binlog
  // slice.
  if (m_binlog_in_transaction == false &&
      !(type == binary_log::GTID_LOG_EVENT ||
#ifdef WESQL_CLUSTER
        type == binary_log::CONSENSUS_LOG_EVENT ||
#endif
        type == binary_log::ANONYMOUS_GTID_LOG_EVENT) &&
      !(type == binary_log::USER_VAR_EVENT ||
        type == binary_log::INTVAR_EVENT || type == binary_log::RAND_EVENT)) {
    // rotate allowed.
    m_rotate_forbidden = false;
    ulonglong now = my_milli_time();
    if ((m_slice_bytes_written >= opt_binlog_archive_slice_max_size) ||
        ((now - m_slice_create_ts) >= opt_binlog_archive_period)) {
      if (rotate_binlog_slice(m_mysql_binlog_write_last_event_end_pos, false) ==
          1) {
        error = 1;
        goto err_slice;
      }
    }
  } else {
    m_rotate_forbidden = true;
  }

  mysql_mutex_unlock(&m_rotate_lock);

suc:
  return 0;

err_slice:
  m_slice_cache.clear();
  mysql_mutex_unlock(&m_rotate_lock);
  return error;
}

inline void Binlog_archive::calc_event_checksum(uchar *event_ptr,
                                                size_t event_len) {
  ha_checksum crc = checksum_crc32(0L, nullptr, 0);
  crc = checksum_crc32(crc, event_ptr, event_len - BINLOG_CHECKSUM_LEN);
  int4store(event_ptr + event_len - BINLOG_CHECKSUM_LEN, crc);
}

/**
 * @brief Merge binlog slices to a single binlog file.
 *
 * @param log_name
 * @param to_binlog_file
 * @return int
 */
int Binlog_archive::merge_slice_to_binlog_file(const char *log_name,
                                               const char *to_binlog_file) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_consistent_snapshot_binlog"));
  std::streamsize binlog_size = 0;
  my_off_t slice_number = 0;
  std::string err_msg;

  remove_file(std::string{to_binlog_file});

  // Find the first slice of the last binlog from persistent binlog.index.
  LOG_ARCHIVED_INFO log_info;
  int error = 1;
  error = find_log_pos_by_name(&log_info, log_name);
  if (error != 0) {
    return 1;
  }
  do {
    std::string binlog_slice_name;
    std::string binlog_slice_keyid;
    binlog_slice_name.append(to_binlog_file);
    binlog_slice_name.append(BINLOG_ARCHIVE_SLICE_LOCAL_SUFFIX);
    binlog_slice_keyid.assign(m_binlog_archive_dir);
    binlog_slice_keyid.append(log_info.log_slice_name);
    // Delete the local binlog slice file if it exists.
    remove_file(binlog_slice_name);
    // download slice from object store
    objstore::Status ss = binlog_objstore->get_object_to_file(
        std::string_view(opt_objstore_bucket), binlog_slice_keyid,
        binlog_slice_name);
    if (!ss.is_succ()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_GET_OBJECT_TO_FILE,
             "get binlog slice failed", binlog_slice_keyid.c_str(),
             binlog_slice_name.c_str(),
             std::string(ss.error_message()).c_str());
      return 1;
    }
    if (my_chmod(
            binlog_slice_name.c_str(),
            USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
            MYF(0))) {
      err_msg.assign("Failed to chmod: ");
      err_msg.append(binlog_slice_name);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      return 1;
    }
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_GET_OBJECT_TO_FILE,
           "get binlog slice", binlog_slice_keyid.c_str(),
           binlog_slice_name.c_str(), "");
    // append binlog slice file to binlog file.
    std::ofstream binlog_file;
    binlog_file.open(to_binlog_file, std::ofstream::app);
    if (!binlog_file.is_open()) {
      err_msg.assign("Failed to open binlog file: ");
      err_msg.append(to_binlog_file);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      return 1;
    }
    std::ifstream binlog_slice;
    binlog_slice.open(binlog_slice_name, std::ifstream::in);
    if (!binlog_slice.is_open()) {
      err_msg.assign("Failed to open binlog slice file: ");
      err_msg.append(binlog_slice_name);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
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
      err_msg.assign("merge binlog slice to binlog failed: ");
      err_msg.append(log_info.log_slice_name);
      err_msg.append(" slice_end_pos=");
      err_msg.append(std::to_string(slice_number));
      err_msg.append(" binlog_total_size=");
      err_msg.append(std::to_string(binlog_size));
      LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      return 1;
    }
    // remove binlog slice file
    remove_file(binlog_slice_name);
  } while (!(error = find_next_log_slice(&log_info)) &&
           compare_log_name(log_info.log_file_name, log_name) == 0);

  err_msg.assign("merge slice to binlog ");
  err_msg.append(to_binlog_file);
  err_msg.append(" size=");
  err_msg.append(std::to_string(binlog_size));
  err_msg.append(" last slice number=");
  err_msg.append(std::to_string(slice_number));
  LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());

  if (my_chmod(to_binlog_file,
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE |
                   OTHERS_WRITE | OTHERS_READ,
               MYF(MY_WME))) {
    err_msg.assign("Failed to chmod: ");
    err_msg.append(to_binlog_file);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    // remove_file(mysql_clone_file_name);
    return 1;
  }
  return 0;
}

/**
 * @brief new binlog slice name.
 *
 * @param binlog persistent binlog file name.
 * @return int
 */
int Binlog_archive::new_persistent_binlog_slice_key(const char *binlog,
                                                    std::string &slice_name,
                                                    const my_off_t pos,
                                                    const uint64_t term) {
  DBUG_TRACE;
  char binlog_slice_ext[12] = {0};
  char binlog_slice_term_ext[22] = {0};
  slice_name.assign(binlog);

  snprintf(binlog_slice_term_ext, sizeof(binlog_slice_term_ext),
           "." BINLOG_ARCHIVE_CONSENSUS_TERM_EXT, static_cast<my_off_t>(term));
  slice_name.append(binlog_slice_term_ext);
  // Binlog max size is 1G (1,073,741,824), so the binlog slice file
  // name extension is 10 bytes.
  snprintf(binlog_slice_ext, sizeof(binlog_slice_ext),
           "." BINLOG_ARCHIVE_SLICE_POSITION_EXT, pos);
  slice_name.append(binlog_slice_ext);
  return 0;
}

/**
 * @brief Check if stop waiting for mysql binlog update.
 *
 * @param log_pos wait for mysql binlog update position
 * @return 0 if stop waiting for mysql binlog update, 1 if continue waiting,
 * -1 if wait for mysql binlog update is failed.
 */
int Binlog_archive::stop_waiting_for_mysql_binlog_update(my_off_t log_pos) {
  // Check whether consensus role is leader
  if (consensus_leader_is_changed()) {
    return -1;
  }

  if (DBUG_EVALUATE_IF(
          "force_suspend_binlog_persist_while_wait_for_mysql_binlog", true,
          false)) {
    return 1;
  }

  if (mysql_bin_log.get_binlog_end_pos() > log_pos ||
      !mysql_bin_log.is_active(m_mysql_linfo.log_file_name) || m_thd->killed) {
    return 0;
  }
  return 1;
}

/**
 * @brief Wait for new mysql binlog events.
 *
 * @param log_pos mysql binlog position
 * @return 0 if success, -1 if failed.
 */
int Binlog_archive::wait_new_mysql_binlog_events(my_off_t log_pos) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("wait_new_mysql_binlog_events"));
  int ret = 0;
  std::chrono::nanoseconds timeout =
      std::chrono::nanoseconds{1000000ULL} * opt_binlog_archive_period;

  /*
    MYSQL_BIN_LOG::binlog_end_pos is atomic. We should only acquire the
    LOCK_binlog_end_pos if we reached the end of the hot log and are going
    to wait for updates on the binary log (Binlog_sender::wait_new_event()).
  */
  if ((ret = stop_waiting_for_mysql_binlog_update(log_pos)) != 1) {
    // return 0 or -1
    return ret;
  }

  mysql_bin_log.lock_binlog_end_pos();
  m_thd->ENTER_COND(mysql_bin_log.get_log_cond(),
                    mysql_bin_log.get_binlog_end_pos_lock(), nullptr, nullptr);
  while ((ret = stop_waiting_for_mysql_binlog_update(log_pos)) == 1) {
    // wait opt_binlog_archive_period millisecond
    mysql_bin_log.wait_for_update(timeout);

    // Update index worker is failed, retry binlog archive run.
    if (m_update_index_worker->is_update_index_failed()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "check found update index failed");
      ret = -1;
      break;
    }

    // If the binlog slice has not been persisted for a long time, rotate the
    // binlog slice. For purposes of binlog slice rotation, the binlog slice
    // is considered to be persisted.
    ulonglong now = my_milli_time();
    if ((m_slice_create_ts > 0) &&
        ((ulonglong)(now - m_slice_create_ts) >= opt_binlog_archive_period)) {
      mysql_bin_log.unlock_binlog_end_pos();
      m_thd->EXIT_COND(nullptr);

      if (rotate_binlog_slice(0, true) == 1) return -1;

      mysql_bin_log.lock_binlog_end_pos();
      m_thd->ENTER_COND(mysql_bin_log.get_log_cond(),
                        mysql_bin_log.get_binlog_end_pos_lock(), nullptr,
                        nullptr);
    }
  }
  mysql_bin_log.unlock_binlog_end_pos();
  m_thd->EXIT_COND(nullptr);

  return ret;
}

/**
 * @brief Retrieves the end position of the binlog.
 *
 * This function reads the current position from the provided File_reader and
 * attempts to determine the end position of the binlog. It also checks if the
 * binlog is active or cold and adjusts the result accordingly.
 *
 * @param reader A reference to the File_reader object used to read the binlog.
 * @return A pair containing the end position of the binlog and a status code.
 *         The first element of the pair is the end position (my_off_t).
 *         The second element of the pair is a status code (int):
 *         - 1 if the binlog is active and the read position is at or beyond the
 * end position.
 *         - 0 if the binlog is active and the read position is before the end
 * position.
 *         - (0, 0) if the binlog is cold.
 */
std::pair<my_off_t, int> Binlog_archive::get_binlog_end_pos(
    File_reader &reader) {
  DBUG_TRACE;
  my_off_t read_pos = reader.position();

  std::pair<my_off_t, int> result = std::make_pair(read_pos, 1);

  if (unlikely(wait_new_mysql_binlog_events(read_pos)) != 0) return result;

  result.first = mysql_bin_log.get_binlog_end_pos();

  DBUG_PRINT("info", ("Reading file %s, seek pos %llu, end_pos is %llu",
                      m_mysql_linfo.log_file_name, read_pos, result.first));
  DBUG_PRINT("info", ("Active file is %s", mysql_bin_log.get_log_fname()));

  /* If this is a cold binlog file, we are done getting the end pos */
  if (unlikely(!mysql_bin_log.is_active(m_mysql_linfo.log_file_name))) {
    return std::make_pair(0, 0);
  }
  if (read_pos < result.first) {
    result.second = 0;
    return result;
  }
  return result;
}

/**
 * @brief Create a new binlog slice local cache.
 *
 * @param new_binlog switch next binlog
 * @param log_file mysql binlog
 * @param log_pos mysql binlog position
 * @return int
 * @note Must be called with m_rotate_lock held.
 */
int Binlog_archive::new_binlog_slice(bool new_binlog, const char *log_file,
                                     my_off_t log_pos [[maybe_unused]],
                                     uint64_t previous_consensus_index) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("new_binlog_slice"));
  int error = 0;
  std::string err_msg = {};
  err_msg.assign("new local binlog slice ");
  // Generate the next archive binlog file name.
  // Generate the first binlog slice.
  if (new_binlog) {
    m_last_expected_file_seq++;
    m_last_expected_slice_seq = -1;
    m_binlog_archive_last_index_number++;
    m_mysql_binlog_first_file = false;
    m_binlog_previouse_consensus_index = previous_consensus_index;
    // The first persisted binlog.
    if (m_mysql_end_consensus_index == 0)
      m_mysql_end_consensus_index = previous_consensus_index;
    strmake(m_mysql_binlog_file_name, log_file,
            sizeof(m_mysql_binlog_file_name) - 1);
    sprintf(m_binlog_archive_file_name, "%s" BINLOG_ARCHIVE_NUMBER_EXT,
            BINLOG_ARCHIVE_BASENAME,
            (ulonglong)m_binlog_archive_last_index_number);
    err_msg.append("new mysql binlog=");
    err_msg.append(m_mysql_binlog_file_name);
    err_msg.append("new persistent binlog=");
    err_msg.append(m_binlog_archive_file_name);
    err_msg.append(" previous_consensus_index=");
    err_msg.append(std::to_string(m_binlog_previouse_consensus_index));
    err_msg.append(" ");
  }
  // rotate new archive binlog slice.
  m_slice_cache.clear();
  m_slice_create_ts = my_milli_time();
  if (m_mysql_binlog_first_file) m_last_expected_file_seq = 0;
  m_last_expected_slice_seq++;
  LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());

  return error;
}

/*
 @param log_pos mysql binlog position of the rotate.
 @return 1, rotate success.
         0, rotate failed.
*/
int Binlog_archive::rotate_binlog_slice(my_off_t log_pos, bool need_lock) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("rotate_binlog_slice"));
  DBUG_EXECUTE_IF("fault_injection_rotate_binlog_slice", { return 1; });
  int error = 0;
  std::string err_msg{};

  if (need_lock) {
    mysql_mutex_lock(&m_rotate_lock);
  }

  DBUG_PRINT("debug", ("Binlog archive request rotate binlog slice: expected "
                       "mysql position=%lld",
                       log_pos));

  // Check if update index worker is failed, retry binlog archive run.
  if (m_update_index_worker->is_update_index_failed()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "check found update index failed");
    error = 1;
    goto end;
  }

  // msyql binlog log_pos already archived.
  if (log_pos > 0 && m_mysql_binlog_last_event_end_pos >= log_pos) {
    goto end;
  }

  if (m_mysql_binlog_last_event_end_pos ==
      m_mysql_binlog_write_last_event_end_pos) {
    goto end;
  }

  if (log_pos > 0 && m_mysql_binlog_write_last_event_end_pos < log_pos) {
    err_msg.assign("request rotate binlog slice: ");
    err_msg.append("expected mysql position=");
    err_msg.append(std::to_string(log_pos));
    err_msg.append(" rotate failed mysql_binlog_write_last_event_end_pos:");
    err_msg.append(std::to_string(m_mysql_binlog_write_last_event_end_pos));
    err_msg.append(" less than log_pos: ");
    err_msg.append(std::to_string(log_pos));
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    error = 1;
    goto end;
  }

  if (DBUG_EVALUATE_IF("force_rotate_forbidded", true, false) ||
      m_rotate_forbidden) {
    err_msg.assign("request rotate binlog slice: ");
    err_msg.append("expected mysql position=");
    err_msg.append(std::to_string(log_pos));
    err_msg.append(" rotate forbidden while in transaction");
    err_msg.append(" last mysql binlog= ");
    err_msg.append(m_mysql_binlog_file_name);
    err_msg.append(":");
    err_msg.append(std::to_string(m_mysql_binlog_write_last_event_end_pos));
    err_msg.append(" to last persistent binlog=");
    err_msg.append(m_binlog_archive_file_name);
    err_msg.append(":");
    err_msg.append(std::to_string(m_binlog_archive_write_last_event_end_pos));
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto end;
  }

  if (!m_slice_cache.empty()) {
    err_msg.assign("rotate binlog slice ");
    err_msg.append("expected mysql position=");
    err_msg.append(std::to_string(log_pos));
    err_msg.append(" mysql binlog= ");
    err_msg.append(m_mysql_binlog_file_name);
    err_msg.append(" advance ");
    err_msg.append(std::to_string(m_mysql_binlog_last_event_end_pos));
    err_msg.append(" to ");
    err_msg.append(std::to_string(m_mysql_binlog_write_last_event_end_pos));
    err_msg.append(" persistent binlog=");
    err_msg.append(m_binlog_archive_file_name);
    err_msg.append(" advance position");
    err_msg.append(std::to_string(m_binlog_archive_last_event_end_pos));
    err_msg.append(" to ");
    err_msg.append(std::to_string(m_binlog_archive_write_last_event_end_pos));
    err_msg.append(" end consensus index=");
    err_msg.append(std::to_string(m_mysql_end_consensus_index));

    // Check whether consensus role is leader
    if (consensus_leader_is_changed()) {
      error = 1;
      goto end;
    }
    // Upload the local archived binlog slice to the object store.
    std::string archived_binlog_keyid{};
    std::string binlog_slice_name{};
    archived_binlog_keyid.assign(m_binlog_archive_dir);
    new_persistent_binlog_slice_key(
        m_binlog_archive_file_name, binlog_slice_name,
        m_binlog_archive_write_last_event_end_pos, m_consensus_term);
    archived_binlog_keyid.append(binlog_slice_name);

    // write slice_cache to m_expected_slice_queue
    Binlog_expected_slice slice(archived_binlog_keyid.c_str(), m_slice_cache,
                                m_slice_bytes_written, m_last_expected_file_seq,
                                m_last_expected_slice_seq);
    SLICE_INFO slice_info;
    strmake(slice_info.log_file_name, m_binlog_archive_file_name,
            sizeof(slice_info.log_file_name) - 1);
    strmake(slice_info.log_slice_name, binlog_slice_name.c_str(),
            sizeof(slice_info.log_slice_name) - 1);
    strmake(slice_info.mysql_log_name, m_mysql_binlog_file_name,
            sizeof(slice_info.mysql_log_name) - 1);
    slice_info.log_slice_end_consensus_index = m_mysql_end_consensus_index;
    slice_info.log_slice_previous_consensus_index =
        m_binlog_previouse_consensus_index;
    slice_info.log_slice_consensus_term = m_consensus_term;
    slice_info.log_slice_end_pos = m_binlog_archive_write_last_event_end_pos;
    slice_info.mysql_end_pos = m_mysql_binlog_write_last_event_end_pos;

    // Before adding the slice to the expected slice queue, release the rotate
    // lock.
    if (need_lock) {
      mysql_mutex_unlock(&m_rotate_lock);
    }
    if (!add_slice(slice, slice_info)) {
      error = 1;
      if (need_lock) {
        mysql_mutex_lock(&m_rotate_lock);
      }
      goto end;
    }
    if (need_lock) {
      mysql_mutex_lock(&m_rotate_lock);
    }
    m_slice_end_consensus_index = m_mysql_end_consensus_index;
    m_binlog_archive_last_event_end_pos =
        m_binlog_archive_write_last_event_end_pos;
    m_mysql_binlog_last_event_end_pos = m_mysql_binlog_write_last_event_end_pos;
    m_slice_bytes_written = 0;
    // Only after the slice is successfully persisted can it be deleted; if the
    // persistence fails, it must be retried continuously until successful.
    m_slice_cache.clear();
    m_slice_create_ts = 0;
  }
  assert(log_pos == 0 || m_mysql_binlog_last_event_end_pos >= log_pos);

end:
  if (need_lock) {
    mysql_mutex_unlock(&m_rotate_lock);
  }
  return error;
}

/**
 * @brief Add a slice to the expected slice queue.
 *
 * @param slice
 */
bool Binlog_archive::add_slice(Binlog_expected_slice &slice,
                               SLICE_INFO &log_info) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("add_slice"));
  std::string err_msg;
  err_msg.assign("add slice to expected slice queue ");
  err_msg.append("file_seq=");
  err_msg.append(std::to_string(slice.m_file_seq));
  err_msg.append(" slice_seq=");
  err_msg.append(std::to_string(slice.m_slice_seq));
  err_msg.append(" log_slice_keyid=");
  err_msg.append(slice.m_slice_keyid);
  err_msg.append(" slice_size=");
  err_msg.append(std::to_string(slice.m_slice_bytes_written));
  err_msg.append(" log_slice_name=");
  err_msg.append(log_info.log_slice_name);
  err_msg.append(" consensus_end_index=");
  err_msg.append(std::to_string(log_info.log_slice_end_consensus_index));
  err_msg.append(" previous_consensus_index=");
  err_msg.append(std::to_string(log_info.log_slice_previous_consensus_index));
  err_msg.append(" end_pos=");
  err_msg.append(std::to_string(log_info.log_slice_end_pos));
  err_msg.append(" mysql_log_name=");
  err_msg.append(log_info.mysql_log_name);
  err_msg.append(" mysql_end_pos=");
  err_msg.append(std::to_string(log_info.mysql_end_pos));
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());

  DBUG_EXECUTE_IF("fault_injection_add_slice_queue", {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "fault_injection_add_slice_queue");
    return false;
  });

  mysql_mutex_lock(&m_slice_mutex);
  m_thd->ENTER_COND(&m_queue_cond, &m_slice_mutex, nullptr, nullptr);
  while (m_expected_slice_queue.full() && !m_thd->killed) {
    // Wait for signal from the binlog archive worker thread that the slice
    // is persisted and dequeued from the expected slice queue.
    // Or binlog archive thread is killed.
    mysql_cond_wait(&m_queue_cond, &m_slice_mutex);
  }
  m_expected_slice_queue.en_queue(&slice);
  m_slice_status_map[slice.m_file_seq][slice.m_slice_seq] =
      Slice_status(slice.m_file_seq, slice.m_slice_seq, log_info);
  mysql_mutex_unlock(&m_slice_mutex);
  m_thd->EXIT_COND(nullptr);
  if (m_thd->killed) {
    return false;
  }
  // Signal the binlog archive worker thread that wait, because the slice queue
  // is empty.
  mysql_cond_signal(&m_queue_cond);
  return true;
}

/**
 * @brief Notify that a slice has been persisted.
 *
 * @param slice The slice that has been persisted.
 * @param is_slice_persisted Whether the slice has been persisted successfully.
 * @param slice_queue_map_term The term of the slice queue and map.
 */
bool Binlog_archive::notify_slice_persisted(const Binlog_expected_slice &slice,
                                            bool is_slice_persisted,
                                            uint64_t slice_queue_map_term) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("notify_slice_persisted"));
  std::string err_msg;
  err_msg.assign("notify slice persisted ");
  err_msg.append("file_seq=");
  err_msg.append(std::to_string(slice.m_file_seq));
  err_msg.append(" slice_seq=");
  err_msg.append(std::to_string(slice.m_slice_seq));
  err_msg.append(" log_slice_keyid=");
  err_msg.append(slice.m_slice_keyid);
  err_msg.append(" slice_queue_mapt init term=");
  err_msg.append(std::to_string(slice_queue_map_term));

  mysql_mutex_lock(&m_slice_mutex);
  // If the slice queue and map have not been initialized or reinitialized,
  // there is no need to update the slice status map. Skip the update.
  // This is to prevent the slice status map from being updated after the
  // binlog archive reinitializes the slice queue and map.
  if (m_slice_queue_and_map_term != slice_queue_map_term) {
    mysql_mutex_unlock(&m_slice_mutex);
    err_msg.append(
        " skiped deprecated slice when slice queue and map reinitialized");
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return false;
  }
  // If the slice status map is empty or the slice status is not found in the
  // map, there is no need to update the slice status map. Skip the update.
  if (m_slice_status_map.empty() || m_slice_status_map[slice.m_file_seq].empty() ||
      m_slice_status_map[slice.m_file_seq].find(slice.m_slice_seq) ==
          m_slice_status_map[slice.m_file_seq].end()) {
    mysql_mutex_unlock(&m_slice_mutex);
    err_msg.append(" skiped deprecated slice when slice status map is empty");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return false;
  }
  auto &slice_status = m_slice_status_map[slice.m_file_seq][slice.m_slice_seq];
  slice_status.persisted =
      is_slice_persisted ? SLICE_PERSISTED : SLICE_PERSISTED_FAILED;
  err_msg.append(" slice=");
  err_msg.append(slice_status.archived_info.log_slice_name);
  err_msg.append(" consensus_end_index=");
  err_msg.append(
      std::to_string(slice_status.archived_info.log_slice_end_consensus_index));
  err_msg.append(is_slice_persisted ? " success" : " failed");
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
  mysql_mutex_unlock(&m_slice_mutex);

  mysql_cond_signal(&m_map_cond);
  return true;
}

/**
 * @brief Update the binlog index file with the persisted slices.
 *
 * @param need_slice_lock Whether to acquire the slice mutex lock.
 * @return true if the index file is successfully updated; otherwise, false.
 */
bool Binlog_archive::update_index_file(bool need_slice_lock) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("update_index_file"));
  std::vector<Slice_status> to_process;
  std::string err_msg;

  DBUG_EXECUTE_IF("fault_injection_binlog_archive_update_index_file", {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
           "fault_injection_binlog_archive_update_index_file");
    return false;
  });

  if (need_slice_lock) mysql_mutex_lock(&m_slice_mutex);
  // Process from map beginning
  auto file_it = m_slice_status_map.begin();
  while (file_it != m_slice_status_map.end()) {
    auto &slice_map = file_it->second;

    // Process slices from beginning of current file
    auto slice_it = slice_map.begin();
    while (slice_it != slice_map.end()) {
      if (slice_it->second.persisted == SLICE_PERSISTED_FAILED) {
        if (need_slice_lock) mysql_mutex_unlock(&m_slice_mutex);
        err_msg.assign(" binlog slice persisted failed, slice=");
        err_msg.append(slice_it->second.archived_info.log_slice_name);
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
               err_msg.c_str());
        return false;
      }
      if (slice_it->second.persisted == SLICE_NOT_PERSISTED) {
        // Stop at first non-persisted slice
        break;
      }
      assert(slice_it->second.persisted == SLICE_PERSISTED);
      err_msg.assign(" binlog slice persisted, slice=");
      err_msg.append(slice_it->second.archived_info.log_slice_name);
      err_msg.append(" persisted slice map file_seq=");
      err_msg.append(std::to_string(slice_it->second.file_seq));
      err_msg.append(" slice_seq=");
      err_msg.append(std::to_string(slice_it->second.slice_seq));
      LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             err_msg.c_str());
      // Add to processing batch
      to_process.push_back(slice_it->second);
      slice_it = slice_map.erase(slice_it);
    }

    if (slice_map.empty()) {
      // Remove empty file map
      file_it = m_slice_status_map.erase(file_it);
    } else {
      // Stop at first file with remaining slices
      break;
    }
  }
  if (need_slice_lock) mysql_mutex_unlock(&m_slice_mutex);

  // Batch update index with all collected slices.
  if (!to_process.empty()) {
    err_msg.assign(" batch put log index entry count=");
    err_msg.append(std::to_string(to_process.size()));
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
           err_msg.c_str());

    mysql_mutex_lock(&m_index_lock);
    if (open_index_file()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             "failed to open_index_file");
      mysql_mutex_unlock(&m_index_lock);
      return false;
    }
    if (open_crash_safe_index_file()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             "failed to open_crash_safe_index_file");
      mysql_mutex_unlock(&m_index_lock);
      return false;
    }

    if (copy_file(&m_index_file, &m_crash_safe_index_file, 0)) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             "failed to copy_file");
      mysql_mutex_unlock(&m_index_lock);
      return false;
    }

    // Batch update index with all collected slices
    for (const auto &slice : to_process) {
      std::string binlog_entry;
      binlog_entry.assign(slice.archived_info.log_slice_name);
      binlog_entry.append("|");
      binlog_entry.append(
          std::to_string(slice.archived_info.log_slice_end_consensus_index));
      binlog_entry.append("|");
      binlog_entry.append(std::to_string(
          slice.archived_info.log_slice_previous_consensus_index));

      if (slice.archived_info.log_file_name[0] == '\0') {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
               "persistent slice name is empty");
        mysql_mutex_unlock(&m_index_lock);
        return false;
      }

      if (my_b_write(&m_crash_safe_index_file,
                     reinterpret_cast<const uchar *>(binlog_entry.c_str()),
                     binlog_entry.length()) ||
          my_b_write(&m_crash_safe_index_file,
                     pointer_cast<const uchar *>("\n"), 1)) {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
               "failed to my_b_write");
        mysql_mutex_unlock(&m_index_lock);
        return false;
      }
      LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             binlog_entry.c_str());
    }
    if (flush_io_cache(&m_crash_safe_index_file) ||
        mysql_file_sync(m_crash_safe_index_file.file, MYF(MY_WME))) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             "failed to mysql_file_sync");
      mysql_mutex_unlock(&m_index_lock);
      return false;
    }

    if (close_crash_safe_index_file()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             "failed to close_crash_safe_index_file");
      mysql_mutex_unlock(&m_index_lock);
      return false;
    }

    // If failed, recover the index file from object store
    // when restart the binlog archiving from run()->while{}.
    if (move_crash_safe_index_file_to_index_file()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
             "failed to move_crash_safe_index_file_to_index_file");
      mysql_mutex_unlock(&m_index_lock);
      return false;
    }
    mysql_mutex_unlock(&m_index_lock);

    mysql_mutex_lock(&m_rotate_lock);
    Slice_status &last_slice = static_cast<Slice_status &>(to_process.back());
    strmake(m_persisted_binlog_file_name,
            last_slice.archived_info.log_file_name,
            sizeof(m_persisted_binlog_file_name) - 1);
    strmake(m_persisted_mysql_binlog_file_name,
            last_slice.archived_info.mysql_log_name,
            sizeof(m_persisted_mysql_binlog_file_name) - 1);
    m_persisted_binlog_last_event_end_pos =
        last_slice.archived_info.log_slice_end_pos;
    m_persisted_slice_end_consensus_index =
        last_slice.archived_info.log_slice_end_consensus_index;
    m_persisted_binlog_previouse_consensus_index =
        last_slice.archived_info.log_slice_previous_consensus_index;
    m_persisted_mysql_binlog_last_event_end_pos =
        last_slice.archived_info.mysql_end_pos;
    mysql_mutex_unlock(&m_rotate_lock);
    err_msg.assign("update index file success ");
    err_msg.append("last persisted slice=");
    err_msg.append(last_slice.archived_info.log_slice_name);
    err_msg.append(" end pos=");
    err_msg.append(std::to_string(m_persisted_binlog_last_event_end_pos));
    err_msg.append(" end consensus index=");
    err_msg.append(std::to_string(m_persisted_slice_end_consensus_index));
    err_msg.append(" mysql binlog=");
    err_msg.append(m_persisted_mysql_binlog_file_name);
    err_msg.append(" mysql end pos=");
    err_msg.append(std::to_string(m_persisted_mysql_binlog_last_event_end_pos));
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_UPDATE_INDEX_WORKER_LOG,
           err_msg.c_str());
  }
  return true;
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
  int bytes_read = 0;
  uchar io_buf[IO_SIZE * 2] = {0};
  DBUG_TRACE;

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
int Binlog_archive::open_crash_safe_index_file() {
  int error = 0;
  File file = -1;
  DBUG_TRACE;

  if (!my_b_inited(&m_crash_safe_index_file)) {
    myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;

    if ((file = my_open(m_crash_safe_index_local_file_name, O_RDWR | O_CREAT,
                        MYF(MY_WME))) < 0 ||
        init_io_cache(&m_crash_safe_index_file, file, IO_SIZE, WRITE_CACHE, 0,
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
int Binlog_archive::set_crash_safe_index_file_name() {
  int error = 0;
  DBUG_TRACE;
  if (fn_format(m_crash_safe_index_local_file_name,
                BINLOG_ARCHIVE_INDEX_LOCAL_FILE, m_mysql_binlog_archive_dir,
                ".index_crash_safe",
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
int Binlog_archive::close_crash_safe_index_file() {
  int error = 0;
  DBUG_TRACE;

  if (my_b_inited(&m_crash_safe_index_file)) {
    end_io_cache(&m_crash_safe_index_file);
    error = my_close(m_crash_safe_index_file.file, MYF(0));
  }
  m_crash_safe_index_file = IO_CACHE();

  return error;
}

/**
  Move crash safe index file to index file.

  @retval 0 ok
  @retval 1 error
*/
int Binlog_archive::move_crash_safe_index_file_to_index_file() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("move_crash_safe_index_file_to_index_file"));
  DBUG_EXECUTE_IF("fault_injection_binlog_archive_move_crash_index_file", {
    mysql_file_delete(key_file_binlog_index, m_crash_safe_index_local_file_name,
                      MYF(0));
    return 1;
  });
  int error = 0;
  File fd = -1;
  int failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  bool file_rename_status = false, file_delete_status = false;
  bool file_persist_status = false;
  THD *thd = m_thd;
  objstore::Status ss{};
  std::string index_keyid{};

  index_keyid.assign(m_binlog_archive_dir);
  index_keyid.append(m_index_file_name);
  // 1. persist local crash index file to s3, retry 5 times.
  failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  while ((!file_persist_status) && (failure_trials > 0)) {
    ss = binlog_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), index_keyid,
        m_crash_safe_index_local_file_name);
    file_persist_status = ss.is_succ();
    --failure_trials;
    if (!file_persist_status) {
      my_sleep(1000);
      /* Clear the error before retrying. */
      if (failure_trials > 0) thd->clear_error();
    }
  }
  if (!file_persist_status) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_PUT_OBJECT_FROM_BINLOG_INDEX_FILE,
           index_keyid.c_str(), m_index_local_file_name,
           std::string(ss.error_message()).c_str());
    error = 1;
    /*
      Delete Crash safe file index file here.
      Recover the index file from object store
      when restart the binlog archiving from run()->while{}.
     */
    mysql_file_delete(key_file_binlog_index, m_crash_safe_index_local_file_name,
                      MYF(0));
    goto err;
  }
  LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_PUT_OBJECT_FROM_BINLOG_INDEX_FILE,
         index_keyid.c_str(), m_index_local_file_name, "");

  // 2. close local index file
  if (my_b_inited(&m_index_file)) {
    end_io_cache(&m_index_file);
    if (mysql_file_close(m_index_file.file, MYF(0)) < 0) {
      /*
        Delete Crash safe index file here.
        Recover the index file from object store
        when restart the binlog archiving from run()->while{}.
       */
      mysql_file_delete(key_file_binlog_index,
                        m_crash_safe_index_local_file_name, MYF(0));
      error = 1;
      goto err;
    }

    // 3. delete local index file, retry 5 times.
    failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
    while ((file_delete_status == false) && (failure_trials > 0)) {
      file_delete_status = !(mysql_file_delete(
          key_file_binlog_index, m_index_local_file_name, MYF(MY_WME)));
      --failure_trials;
      if (!file_delete_status) {
        my_sleep(1000);
        /* Clear the error before retrying. */
        if (failure_trials > 0) thd->clear_error();
      }
    }

    if (!file_delete_status) {
      error = 1;
      /*
        Delete Crash safe file index file here.
        Recover the index file from object store
        when restart the binlog archiving from run()->while{}.
       */
      mysql_file_delete(key_file_binlog_index,
                        m_crash_safe_index_local_file_name, MYF(0));
      goto err;
    }
  }

  // 4. rename crash index file to local index file.
  failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  while ((file_rename_status == false) && (failure_trials > 0)) {
    file_rename_status = !(my_rename(m_crash_safe_index_local_file_name,
                                     m_index_local_file_name, MYF(MY_WME)));
    --failure_trials;
    if (!file_rename_status) {
      my_sleep(1000);
      /* Clear the error before retrying. */
      if (failure_trials > 0) thd->clear_error();
    }
  }
  if (!file_rename_status) {
    /*
      Delete Crash safe file index file here.
      Recover the index file from object store
      when restart the binlog archiving from run()->while{}.
     */
    mysql_file_delete(key_file_binlog_index, m_crash_safe_index_local_file_name,
                      MYF(0));
    error = 1;
    goto err;
  }

  // 5. reopen local index file
  if ((fd = mysql_file_open(key_file_binlog_index, m_index_local_file_name,
                            O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      mysql_file_sync(fd, MYF(MY_WME)) ||
      init_io_cache_ext(&m_index_file, fd, IO_SIZE, READ_CACHE,
                        mysql_file_seek(fd, 0L, MY_SEEK_END, MYF(0)), false,
                        MYF(MY_WME | MY_WAIT_IF_FULL),
                        key_file_binlog_index_cache)) {
    if (my_b_inited(&m_index_file)) end_io_cache(&m_index_file);
    if (fd >= 0) mysql_file_close(fd, MYF(0));
    error = 1;
    goto err;
  }

err:
  return error;
}

/**
  @brief Append log file name to index file.

  - To make crash safe, we copy all the content of index file
  to crash safe index file firstly and then append the log
  file name to the crash safe index file. Then upload index file to object
  store. Finally move the crash safe index file to index file.
  If failed, we will recover the index file from object store.
  @note Must m_index_lock held.
  @retval
    0   ok
  @retval
    -1   error
*/
int Binlog_archive::add_log_to_index(const uchar *log_name,
                                     size_t log_name_len) {
  DBUG_TRACE;

  DBUG_EXECUTE_IF("fail_to_write_binlog_slice_to_persistent_binlog_index",
                  { return LOG_INFO_IO; });

  if (open_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to open_index_file");
    goto err;
  }

  if (open_crash_safe_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to open_crash_safe_index_file");
    goto err;
  }

  if (copy_file(&m_index_file, &m_crash_safe_index_file, 0)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to copy_file");
    goto err;
  }

  if (my_b_write(&m_crash_safe_index_file, log_name, log_name_len) ||
      my_b_write(&m_crash_safe_index_file, pointer_cast<const uchar *>("\n"),
                 1) ||
      flush_io_cache(&m_crash_safe_index_file) ||
      mysql_file_sync(m_crash_safe_index_file.file, MYF(MY_WME))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to my_b_write");
    goto err;
  }

  if (close_crash_safe_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to close_crash_safe_index_file");
    goto err;
  }

  // If failed, recover the index file from object store
  // when restart the binlog archiving from run()->while{}.
  if (move_crash_safe_index_file_to_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to move_crash_safe_index_file_to_index_file");
    goto err;
  }

  return 0;

err:
  return -1;
}

/**
 * @brief  Open the binlog archive index file.
 *
 * @note  Mutex be called with m_index_lock held.
 * @return true, if error
 * @return false, if success
 */
bool Binlog_archive::open_index_file() {
  bool error = false;
  File index_file_nr = -1;

  /*
    First open of this class instance
    Create an index file that will hold all file names uses for logging.
    Add new entries to the end of it.
  */
  myf opt = MY_UNPACK_FILENAME | MY_REPLACE_DIR;

  if (my_b_inited(&m_index_file)) goto end;

  fn_format(m_index_local_file_name, BINLOG_ARCHIVE_INDEX_LOCAL_FILE,
            m_mysql_binlog_archive_dir, ".index", opt);

  if (set_crash_safe_index_file_name()) {
    error = true;
    goto end;
  }

  if (set_purge_index_file_name()) {
    error = true;
    goto end;
  }

  /*
    We need move m_crash_safe_index_file to m_index_file if the m_index_file
    does not exist and m_crash_safe_index_file exists when mysqld server
    restarts.
  */
  if (my_access(m_index_local_file_name, F_OK) &&
      !my_access(m_crash_safe_index_local_file_name, F_OK) &&
      my_rename(m_crash_safe_index_local_file_name, m_index_local_file_name,
                MYF(MY_WME))) {
    error = true;
    goto end;
  }

  // Check if the index file exists in s3, if so, download it to local.
  // The persisted `binlog-index.index` is based on multiple versions of
  // the consensus term, so the `binlog-index.index` with the highest consensus
  // term should be obtained as the initial one.
  // {$BINLOG_ARCHIVE_INDEX_FILE_BASENAME}{$m_consensus_term}{$BINLOG_ARCHIVE_INDEX_FILE_SUFFIX}
  // binlog-index.00000000000000001234.index
  {
    std::vector<objstore::ObjectMeta> objects;
    bool finished = false;
    std::string start_after;
    do {
      std::vector<objstore::ObjectMeta> tmp_objects;
      std::string binlog_index_prefix;
      binlog_index_prefix.assign(m_binlog_archive_dir);
      binlog_index_prefix.append(BINLOG_ARCHIVE_INDEX_FILE_BASENAME);
      objstore::Status ss = binlog_objstore->list_object(
          std::string_view(opt_objstore_bucket), binlog_index_prefix, false,
          start_after, finished, tmp_objects);
      if (!ss.is_succ()) {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LIST_OBJECT,
               "list persistent binlog-index.index failed",
               BINLOG_ARCHIVE_INDEX_FILE_BASENAME,
               std::string(ss.error_message()).c_str());
        return true;
      }
      objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
    } while (finished == false);

    if (!objects.empty()) {
      // download the last binlog-index.****.index
      std::string last_binlog_index_keyid{};
      last_binlog_index_keyid.assign((objects.back()).key);
      size_t first_dot = last_binlog_index_keyid.find('.');
      if (first_dot == std::string::npos) {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
               "invalid binlog-index.index keyid", last_binlog_index_keyid);
        error = true;
        goto end;
      }
      size_t second_dot = last_binlog_index_keyid.find('.', first_dot + 1);
      if (second_dot == std::string::npos) {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
               "invalid binlog-index.index keyid", last_binlog_index_keyid);
        error = true;
        goto end;
      }
      std::string consensus_term_str =
          last_binlog_index_keyid.substr(first_dot + 1, second_dot);
      uint64_t consensus_term = std::stoull(consensus_term_str);
      if (m_consensus_term != consensus_term) {
        LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX_TERM_CHANGED,
               consensus_term, m_consensus_term);
      }
      {
        auto status = binlog_objstore->get_object_to_file(
            std::string_view(opt_objstore_bucket), last_binlog_index_keyid,
            std::string_view(m_crash_safe_index_local_file_name));
        if (!status.is_succ()) {
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_GET_OBJECT_TO_FILE,
                 "get last persistent binlog-index.index failed",
                 last_binlog_index_keyid.c_str(), m_index_local_file_name,
                 std::string(status.error_message()).c_str());
          error = true;
          goto end;
        }
        if (my_chmod(
                m_crash_safe_index_local_file_name,
                USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
                MYF(0))) {
          std::string err_msg;
          err_msg.assign("Failed to chmod: ");
          err_msg.append(m_crash_safe_index_local_file_name);
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX, err_msg.c_str());
          error = true;
          goto end;
        }
        if (my_rename(m_crash_safe_index_local_file_name,
                      m_index_local_file_name, MYF(MY_WME))) {
          error = true;
          goto end;
        }
        LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_GET_OBJECT_TO_FILE,
               "get last persistent binlog-index.index",
               last_binlog_index_keyid.c_str(), m_index_local_file_name, "");
      }
    }
  }

  LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
         "open local binlog-index.index", m_index_local_file_name);
  if ((index_file_nr = mysql_file_open(PSI_binlog_archive_log_index_key,
                                       m_index_local_file_name,
                                       O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      mysql_file_sync(index_file_nr, MYF(MY_WME)) ||
      init_io_cache_ext(&m_index_file, index_file_nr, IO_SIZE, READ_CACHE,
                        mysql_file_seek(index_file_nr, 0L, MY_SEEK_END, MYF(0)),
                        false, MYF(MY_WME | MY_WAIT_IF_FULL),
                        PSI_binlog_archive_log_index_cache_key)) {
    /*
      TODO: all operations creating/deleting the index file or a log, should
      call my_sync_dir() or my_sync_dir_by_file() to be durable.
      TODO: file creation should be done with mysql_file_create()
      not mysql_file_open().
    */
    if (index_file_nr >= 0) {
      mysql_file_close(index_file_nr, MYF(0));
    }
    error = true;
    goto end;
  }

  atomic_log_index_state = LOG_INDEX_OPENED;

end:
  return error;
}

IO_CACHE *Binlog_archive::get_index_file() {
  // Check whether consensus role is leader
  if (consensus_leader_is_changed()) return nullptr;
  if (open_index_file()) return nullptr;
  return &m_index_file;
}

/**
 * @brief Closes the binlog archive index file.
 * @note
 *  Mutex needed because we need to make sure the file pointer does not move
    from under our feet
 * @return int
 */
void Binlog_archive::close_index_file() {
  DBUG_TRACE;

  atomic_log_index_state = LOG_INDEX_CLOSED;
  if (my_b_inited(&m_index_file)) {
    end_io_cache(&m_index_file);
    if (mysql_file_close(m_index_file.file, MYF(0)))
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed to close binlog index file");
  }
}

static int compare_log_name(const char *log_1, const char *log_2) {
  const char *log_1_basename = log_1 + dirname_length(log_1);
  const char *log_2_basename = log_2 + dirname_length(log_2);

  return strcmp(log_1_basename, log_2_basename);
}

/**
  Find the position in the log-index-file for the given log name.

  @param[out] linfo The found log file name will be stored here, along
  with the byte offset of the next log file name in the index file.
  @param log_name Filename to find in the index file, or NULL if we
  want to read the first entry.
  @param need_lock_index If false, this function acquires m_index_lock;
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
int Binlog_archive::find_log_pos_common(IO_CACHE *index_file,
                                        LOG_ARCHIVED_INFO *linfo,
                                        const char *log_name,
                                        uint64_t consensus_index,
                                        bool last_slice [[maybe_unused]]) {
  DBUG_TRACE;
  int error = 0;

  /* As the file is flushed, we can't get an error here */
  my_b_seek(index_file, (my_off_t)0);

  for (;;) {
    size_t length = 0;
    my_off_t offset = my_b_tell(index_file);

    /* If we get 0 or 1 characters, this is the end of the file */
    if ((length = my_b_gets(index_file, linfo->log_line, FN_REFLEN)) <= 1) {
      /* Did not find the given entry; Return not found or error */
      error = !index_file->error ? LOG_INFO_EOF : LOG_INFO_IO;
      break;
    }
    /* Get rid of the trailing '\n' */
    linfo->log_line[length - 1] = 0;

    /*
      {$binlog_file_name}.{$consensus_term}.{$slice_end_pos}|{$end_index}|{$previous_index}

      binlog.000001.00000000000000000000.0000000377|0
      binlog.000002.00000000000000000000.0000000295|1
      binlog.000003.00000000000000000000.0000000295|1
      binlog.000004.00000000000000000000.0000001924|1
      binlog.000004.00000000000000000000.0000014524|1
      binlog.000004.00000000000000000000.0000025541|1
      binlog.000005.00000000000000000000.0000000291|11
      binlog.000005.00000000000000000000.0000000429|11
    */
    std::string in_str;
    in_str.assign(linfo->log_line);
    size_t idx = in_str.find("|");
    std::string found_log_slice_name = in_str.substr(0, idx);
    std::string left_string = in_str.substr(idx + 1);
    idx = left_string.find("|");
    std::string found_end_consensus_index = left_string.substr(0, idx);
    linfo->slice_end_consensus_index = std::stoull(found_end_consensus_index);
    std::string found_previous_consensus_index = left_string.substr(idx + 1);
    linfo->log_previous_consensus_index =
        std::stoull(found_previous_consensus_index);

    strmake(linfo->log_slice_name, found_log_slice_name.c_str(),
            sizeof(linfo->log_slice_name) - 1);

    size_t first_dot = found_log_slice_name.find('.');
    if (first_dot == std::string::npos) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
             "Invalid log slice index entry", found_log_slice_name);
      error = LOG_INFO_IO;
      break;
    }
    size_t second_dot = found_log_slice_name.find('.', first_dot + 1);
    if (second_dot == std::string::npos) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
             "Invalid log slice index entry", found_log_slice_name);
      error = LOG_INFO_IO;
      break;
    }
    std::string file_name = found_log_slice_name.substr(0, second_dot);

    strmake(linfo->log_file_name, file_name.c_str(),
            sizeof(linfo->log_file_name) - 1);

    left_string = found_log_slice_name.substr(second_dot + 1);
    size_t third_dot = left_string.find('.');
    if (third_dot == std::string::npos) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
             "Invalid log slice index entry", found_log_slice_name);
      error = LOG_INFO_IO;
      break;
    }
    std::string term = left_string.substr(0, third_dot);
    linfo->slice_consensus_term = std::stoull(term);
    std::string end_pos = left_string.substr(third_dot + 1);
    linfo->slice_end_pos = std::stoull(end_pos);

    // if the log entry matches, null string matching anything
    if (!log_name || log_name[0] == '\0' ||
        !compare_log_name(file_name.c_str(), log_name)) {
      if (consensus_index == 0 ||
          consensus_index == linfo->log_previous_consensus_index) {
        DBUG_PRINT("info", ("Found log file entry"));
        linfo->index_file_start_offset = offset;
        linfo->index_file_offset = my_b_tell(index_file);
        break;
      }
    }
    linfo->entry_index++;
  }

  return error;
}

int Binlog_archive::find_log_pos_by_name(LOG_ARCHIVED_INFO *linfo,
                                         const char *log_name) {
  DBUG_TRACE;

  if (!my_b_inited(&m_index_file)) {
    return LOG_INFO_IO;
  }
  return find_log_pos_common(&m_index_file, linfo, log_name, 0);
}

/**
 * @brief Find the position in the log-index-file for the given log name.

  @param[out] linfo The filename will be stored here, along with the
  byte offset of the next filename in the index file.

  @param need_lock_index If true, m_index_lock will be acquired;
  otherwise it should already be held by the caller.

  @note
    - Before calling this function, one has to call find_log_pos()
    to set up 'linfo'
    - Mutex needed because we need to make sure the file pointer does not move
    from under our feet

  @retval 0 ok
  @retval LOG_INFO_EOF End of log-index-file found
  @retval LOG_INFO_IO Got IO error while reading file
*/
int Binlog_archive::find_next_log(LOG_ARCHIVED_INFO *linfo) {
  DBUG_TRACE;

  if (!my_b_inited(&m_index_file)) {
    return LOG_INFO_IO;
  }
  return find_next_log_common(&m_index_file, linfo, false);
}

/**
 * @brief
 *
 * @param index_file
 * @param linfo
 * @param found_slice
 * @return int
 */
int Binlog_archive::find_next_log_common(IO_CACHE *index_file,
                                         LOG_ARCHIVED_INFO *linfo,
                                         bool found_slice) {
  DBUG_TRACE;
  int error = 0;
  size_t length = 0;
  std::string previous_log_name;
  previous_log_name.assign(linfo->log_file_name);

  if (!my_b_inited(index_file)) {
    error = LOG_INFO_IO;
    return error;
  }
  /* As the file is flushed, we can't get an error here */
  my_b_seek(index_file, linfo->index_file_offset);

  for (;;) {
    linfo->index_file_start_offset = linfo->index_file_offset;
    if ((length = my_b_gets(index_file, linfo->log_line, FN_REFLEN)) <= 1) {
      error = !index_file->error ? LOG_INFO_EOF : LOG_INFO_IO;
      break;
    }

    /* Get rid of the trailing '\n' */
    linfo->log_line[length - 1] = 0;

    // {$binlog_file_name}.{$consensus_term}.{$slice_end_pos}|{$previouse_index}
    // binlog.000010.00000000000000001120.0000000512|454
    std::string in_str;
    in_str.assign(linfo->log_line);
    size_t idx = in_str.find("|");
    std::string found_log_slice_name = in_str.substr(0, idx);
    std::string left_string = in_str.substr(idx + 1);
    idx = left_string.find("|");
    std::string found_end_consensus_index = left_string.substr(0, idx);
    linfo->slice_end_consensus_index = std::stoull(found_end_consensus_index);
    std::string found_previous_consensus_index = left_string.substr(idx + 1);
    linfo->log_previous_consensus_index =
        std::stoull(found_previous_consensus_index);

    strmake(linfo->log_slice_name, found_log_slice_name.c_str(),
            sizeof(linfo->log_slice_name) - 1);

    size_t first_dot = found_log_slice_name.find('.');
    if (first_dot == std::string::npos) {
      error = LOG_INFO_IO;
      break;
    }
    size_t second_dot = found_log_slice_name.find('.', first_dot + 1);
    if (second_dot == std::string::npos) {
      error = LOG_INFO_IO;
      break;
    }
    std::string log_name = found_log_slice_name.substr(0, second_dot);
    strmake(linfo->log_file_name, log_name.c_str(),
            sizeof(linfo->log_file_name) - 1);
    left_string = found_log_slice_name.substr(second_dot + 1);
    size_t third_dot = left_string.find('.');
    if (third_dot == std::string::npos) {
      error = LOG_INFO_IO;
      break;
    }
    std::string term = left_string.substr(0, third_dot);
    std::string end_pos = left_string.substr(third_dot + 1);
    linfo->slice_consensus_term = std::stoull(term);
    linfo->slice_end_pos = std::stoull(end_pos);

    linfo->entry_index++;
    linfo->index_file_offset = my_b_tell(index_file);
    // Find next binlog or next slice
    if (found_slice ||
        compare_log_name(log_name.c_str(), previous_log_name.c_str()) != 0) {
      break;
    }
  }

  return error;
}

int Binlog_archive::find_next_log_slice(LOG_ARCHIVED_INFO *linfo) {
  DBUG_TRACE;
  return find_next_log_common(&m_index_file, linfo, true);
}

/**
 * @brief Get binlog archive info.
 *
 * @param persisting_consensus_index
 * @param consensus_term
 * @param persisting_mysql_binlog
 * @param persisting_mysql_binlog_pos
 * @param persisting_mysql_binlog_write_pos
 * @param persisting_binlog
 * @param persisting_binlog_pos
 * @param persisting_binlog_write_pos
 * @param persisted_binlog
 * @param persisted_binlog_pos
 * @param persisted_consensus_index
 * @param persisted_mysql_binlog
 * @param persisted_mysql_binlog_pos
 * @return int
 */
int Binlog_archive::show_binlog_archive_task_info(
    uint64_t &persisting_consensus_index, uint64_t &consensus_term,
    std::string &persisting_mysql_binlog, my_off_t &persisting_mysql_binlog_pos,
    my_off_t &persisting_mysql_binlog_write_pos, std::string &persisting_binlog,
    my_off_t &persisting_binlog_pos, my_off_t &persisting_binlog_write_pos,
    std::string &persisted_binlog, my_off_t &persisted_binlog_pos,
    uint64_t &persisted_consensus_index, std::string &persisted_mysql_binlog,
    my_off_t &persisted_mysql_binlog_pos) {
  mysql_mutex_lock(&m_rotate_lock);
  persisting_mysql_binlog_pos = m_mysql_binlog_last_event_end_pos;
  persisting_mysql_binlog_write_pos = m_mysql_binlog_write_last_event_end_pos;
  persisting_mysql_binlog.assign(m_mysql_binlog_file_name);
  persisting_binlog.assign(m_binlog_archive_file_name);
  persisting_binlog_pos = m_binlog_archive_last_event_end_pos;
  persisting_binlog_write_pos = m_binlog_archive_write_last_event_end_pos;
  consensus_term = m_consensus_term;
  persisting_consensus_index = m_slice_end_consensus_index;

  persisted_binlog.assign(m_persisted_binlog_file_name);
  persisted_binlog_pos = m_persisted_binlog_last_event_end_pos;
  persisted_consensus_index = m_persisted_slice_end_consensus_index;
  persisted_mysql_binlog.assign(m_persisted_mysql_binlog_file_name);
  persisted_mysql_binlog_pos = m_persisted_mysql_binlog_last_event_end_pos;

  mysql_mutex_unlock(&m_rotate_lock);
  return 0;
}

/**
 * @brief Get last persisted mysql binlog file and position.
 *
 */
int Binlog_archive::get_mysql_current_archive_binlog(LOG_INFO *linfo,
                                                     bool need_lock /*true*/) {
  if (need_lock) mysql_mutex_lock(&m_rotate_lock);
  strmake(linfo->log_file_name, m_persisted_mysql_binlog_file_name,
          sizeof(linfo->log_file_name) - 1);
  linfo->pos = m_persisted_mysql_binlog_last_event_end_pos;
  if (need_lock) mysql_mutex_unlock(&m_rotate_lock);
  return 1;
}

/**
 * @brief Check if the given mysql log file is archived.
 * @param log_file_name_arg mysql binlog file
 * @param persistent_log_file_name return persistent binlog name
 * @param log_pos mysql binlog position
 * @param consensus_index consensus index
 * @return 0, archive success.
 * @return 1, archive failed, can retry.
 * @return 2, archive failed.
 * @note Must be called with m_index_lock held.
 */
int Binlog_archive::binlog_is_archived(const char *log_file_name_arg,
                                       char *persistent_log_file_name,
                                       my_off_t log_pos,
                                       uint64_t consensus_index) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("binlog_is_archived"));
  int ret = 0;
  LOG_ARCHIVED_INFO log_info;
  std::string err_msg{};

  err_msg.assign("mysql binlog ");
  err_msg.append(log_file_name_arg);
  err_msg.append("/");
  err_msg.append(std::to_string(log_pos));
  err_msg.append(" consensus index=");
  err_msg.append(std::to_string(consensus_index));

  // Not acquired m_rotate_lock here.
  // Check if the requested consensus index is beyond what has been read by the
  // binlog archive thread
  if (consensus_index > m_persisted_slice_end_consensus_index) {
    err_msg.append(" has not yet been persisted");
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;  // Return 1 to indicate need to wait for archive
  }

  // Not acquired m_rotate_lock here.
  // Check if the log file is the current persisted mysql binlog file and if the
  // log position has not yet been persisted.
  if (0 == compare_log_name(m_persisted_mysql_binlog_file_name,
                            log_file_name_arg) &&
      (log_pos > 0 && m_persisted_mysql_binlog_last_event_end_pos < log_pos)) {
    err_msg.append(" persisted mysql binlog end pos=");
    err_msg.append(std::to_string(m_persisted_mysql_binlog_last_event_end_pos));
    err_msg.append(", continue to wait binlog persistence");
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;
  }

  // Acquire lock when accessing persisted file info
  mysql_mutex_lock(&m_rotate_lock);
  // Check if this log file matches the last persisted mysql binlog file
  if (m_persisted_mysql_binlog_file_name[0] != '\0' &&
      0 == compare_log_name(m_persisted_mysql_binlog_file_name,
                            log_file_name_arg)) {
    // Check if requested position is beyond what has been persisted
    if ((log_pos > 0 &&
         log_pos > m_persisted_mysql_binlog_last_event_end_pos)) {
      err_msg.append(" persisted mysql binlog end pos=");
      err_msg.append(
          std::to_string(m_persisted_mysql_binlog_last_event_end_pos));
      err_msg.append(", continue to wait binlog persistence");
      LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      ret = 1;  // Return 1 to indicate need to wait for persistence
    } else {
      err_msg.append(" has been persisted as current persisted binlog");
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      // Copy the persisted binlog filename to return to caller
      strmake(persistent_log_file_name, m_persisted_binlog_file_name,
              FN_REFLEN);
      ret = 0;  // Return 0 to indicate binlog is persisted
    }
    mysql_mutex_unlock(&m_rotate_lock);
    return ret;
  }

  // Until here, the requetes log file is not the current persisted
  // mysql binlog file.

  // Check if the requested consensus index is beyond what has been read by the
  // binlog archive thread
  if (consensus_index > m_persisted_slice_end_consensus_index) {
    err_msg.append(" persisted end consensus index=");
    err_msg.append(std::to_string(m_persisted_slice_end_consensus_index));
    err_msg.append(", continue to wait binlog persistence");
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    mysql_mutex_unlock(&m_rotate_lock);
    return 1;  // Return 1 to indicate need to wait for archive
  }
  mysql_mutex_unlock(&m_rotate_lock);

  mysql_mutex_lock(&m_index_lock);
  // Check if the binlog that needs to be archived has already been
  // persisted by inspecting the `binlog.index`.
  if (open_index_file()) {
    err_msg.append(" binlog.index open failed");
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    mysql_mutex_unlock(&m_index_lock);
    return 1;
  }
  /*
    Assuming `consensus_index` is 3, which is between the previous values of
    `binlog.000001` and `binlog.000004`, this indicates that the binlog has
    already been persisted.
    If the `consensus_index` is less than or equal to the `previous_index` of
    `binlog.000001` or greater than the `previous` of `binlog.000003`, it is
    considered not persisted.

    binlog.index:
      binlog.000001.00000000000000000002.0000001183|0
      binlog.000002.00000000000000000003.0000000251|4
      binlog.000003.00000000000000000003.0000000251|12
   */
  if (consensus_index > 0) {  // consensus mode.
    int error = 0;
    if ((error = find_log_pos_by_name(&log_info, NullS))) {
      // Return 1 to indicate need to wait for archive
      ret = 1;
    } else {
      char pre_log[FN_REFLEN + 1] = {0};
      strmake(pre_log, log_info.log_file_name, FN_REFLEN);
      // diff the first persistent binlog
      if (log_info.log_previous_consensus_index >= consensus_index) {
        // The needed persist binlog has already been purged.
        err_msg.append(", persistent binlog has already been purged");
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        mysql_mutex_unlock(&m_index_lock);
        // Return 2 to indicate binlog has been purged
        return 2;
      }
      // Return 1 to indicate need to wait for archive
      ret = 1;
      do {
        // diff next persistent binlog previous consensus index.
        if (log_info.log_previous_consensus_index >= consensus_index) {
          strmake(persistent_log_file_name, pre_log, FN_REFLEN);
          err_msg.append(", found persistent binlog from binlog.index ");
          err_msg.append(log_info.log_file_name);
          err_msg.append(" log_previous_index=");
          err_msg.append(std::to_string(log_info.log_previous_consensus_index));
          err_msg.append(" found consensus_index=");
          err_msg.append(std::to_string(consensus_index));
          LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
          // Return 0 to indicate binlog is persisted
          ret = 0;
          break;
        }
        strmake(pre_log, log_info.log_file_name, FN_REFLEN);
      } while (!(error = find_next_log(&log_info)));
    }
  } else {  // non consensus mode.
    if (find_log_pos_by_name(&log_info, log_file_name_arg)) {
      ret = 1;
    } else {
      strmake(persistent_log_file_name, log_info.log_file_name, FN_REFLEN);
      ret = 0;
    }
  }

  mysql_mutex_unlock(&m_index_lock);
  return ret;
}

/**
 * @brief Signal other threads that the binlog archive is updated.
 *
 */
void Binlog_archive::signal_archive() {
  mysql_cond_broadcast(&m_binlog_archive_run_cond);
}

/**
 * @brief Wait for the binlog file and position is archived.
 * @param log_file_name
 * @param log_pos
 * @note Must be called with m_binlog_archive_run_lock held.
 * @return 0, archive success, stop wait.
 * @return 1, archive fail, continue wait.
 * @return 2, archive fail, stop wait.
 */
int Binlog_archive::binlog_stop_waiting_for_archive(
    const char *log_file_name, char *persistent_log_file_name,
    my_off_t log_pos [[maybe_unused]],
    uint64_t consensus_index [[maybe_unused]]) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("binlog_stop_waiting_for_archive"));
  DBUG_EXECUTE_IF("fault_injection_binlog_waiting_for_archive", { return 2; });
  int error = 0;

  if (consensus_leader_is_changed()) return 2;

  error = binlog_is_archived(log_file_name, persistent_log_file_name, log_pos,
                             consensus_index);
  return error;
}

/**
 * @brief Wait for the binlog archive to finish.
 * Binlog_archive::archive_event() wakes up m_binlog_archive_run_cond when a
 * binlog slice is persisted.
 * @return int
 */
int Binlog_archive::wait_for_archive() {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&m_binlog_archive_run_lock);
  struct timespec abstime;
  set_timespec(&abstime, 1);
  return mysql_cond_timedwait(&m_binlog_archive_run_cond,
                              &m_binlog_archive_run_lock, &abstime);
}

/**
 * @brief For consistent snapshot arhcive,
 *        consistent snapshot thread wait for the binlog archive to finish.
 */
int binlog_archive_wait_for_archive(THD *thd, const char *log_file_name,
                                    char *persistent_log_file_name,
                                    my_off_t log_pos,
                                    uint64_t consensus_index) {
  DBUG_TRACE;
  int ret = 0;
  if (!opt_binlog_archive) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "Binlog archive is not enabled");
    return 0;
  }
  mysql_mutex_lock(&m_binlog_archive_run_lock);
  if (!mysql_binlog_archive.is_thread_running()) {
    mysql_mutex_unlock(&m_binlog_archive_run_lock);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Binlog archive thread is not running");
    return 1;
  }
  thd->ENTER_COND(&m_binlog_archive_run_cond, &m_binlog_archive_run_lock,
                  nullptr, nullptr);
  // Check if waited binlog file is archived.
  while ((ret = mysql_binlog_archive.binlog_stop_waiting_for_archive(
              log_file_name, persistent_log_file_name, log_pos,
              consensus_index)) == 1) {
    mysql_binlog_archive.wait_for_archive();
    // binlog archive thread maybe terminated.
    if (!mysql_binlog_archive.is_thread_running()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Binlog archive thread is not running");
      break;
    }
  }
  mysql_mutex_unlock(&m_binlog_archive_run_lock);
  thd->EXIT_COND(nullptr);
  return ret;
}

/**
 * @brief Remove file or directory.
 * @param file file or directory name.
 * @return true, if error.
 * @return false, if success or file not exists.
 */
static bool remove_file(const std::string &file) {
  DBUG_TRACE;

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
 * @brief Get the root directory and left string.
 * @param in_str input string.
 * e.g "abc/def/ghi"
 * @param out_str output root directory from in_str. or empty string.
 * e.g "abc/"
 * @param left_str output left string from in_str. or in_str.
 * e.g "def/ghi"
 */
static void root_directory(std::string in_str, std::string *out_str,
                           std::string *left_str) {
  DBUG_TRACE;
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
 * @brief Create directory recursively under the root directory.
 * e.g make -p "/u01/abc/def/ghi/"
 * @param dir e.g "adc/def/ghi/file.txt"
 * @param root e.g "/u01/"
 */
static bool recursive_create_dir(const std::string &dir,
                                 const std::string &root) {
  DBUG_TRACE;
  std::string out;
  std::string left;
  root_directory(dir, &out, &left);
  if (out.empty()) {
    return false;
  }
  std::string real_path;

  real_path.assign(root);
  real_path.append(out);
  MY_STAT stat_area;
  if (!my_stat(real_path.c_str(), &stat_area, MYF(0))) {
    if (my_mkdir(real_path.c_str(), 0777, MYF(0))) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed to create binlog archive dir");
      return true;
    }
  }
  return recursive_create_dir(left, real_path);
}

/**
 * @brief Automatically purges binary logs based on expiration settings.
 *
 * This function checks if the automatic purge option is enabled. If enabled, it
 * calculates the lower time bound for purging logs and retrieves the list of
 * binary log files. It then iterates through the list and identifies logs that
 * need to be purged based on their last modified time. If such logs are found,
 * it constructs the log name up to the second dot and calls the purge function
 * to remove the logs.
 *
 * @return int Returns 0 on successful execution.
 */
int Binlog_archive::auto_purge_logs() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("auto_purge_logs"));

  if (!opt_binlog_archive_expire_auto_purge) return 0;

  auto purge_time = calculate_auto_purge_lower_time_bound();
  std::string purge_to_log;
  bool found = false;
  std::vector<objstore::ObjectMeta> binlog_objects;
  show_binlog_persistent_files(binlog_objects);
  // purge binlog before purge_time.
  for (const auto &object : binlog_objects) {
    if (purge_time < (object.last_modified / 1000)) {
      break;
    }
    purge_to_log.assign(object.key);
    found = true;
  }
  if (found) {
    // binlog.000001.00000000000000001234.0000000345
    size_t first_dot = purge_to_log.find('.');
    if (first_dot == std::string::npos) {
      return 1;
    }
    size_t second_dot = purge_to_log.find('.', first_dot + 1);
    if (second_dot == std::string::npos) {
      return 1;
    }
    purge_to_log = purge_to_log.substr(0, second_dot);
    purge_logs(purge_to_log.c_str());
  }
  return 0;
}

/**
 * @brief Purge binary logs up to the specified log file.
 *
 * This function attempts to purge binary logs up to the specified log file
 * name. It performs several checks and operations to ensure the consistency and
 * integrity of the binary logs and their associated metadata.
 *
 * @param to_log The name of the log file up to which logs should be purged.
 * @return A tuple containing an error code and an error message. The error code
 * is 0 if the operation was successful, and non-zero otherwise.
 *
 * The function performs the following steps:
 * 1. Logs the start of the purge operation.
 * 2. Checks if consistent snapshot archiving is enabled and if the server is
 * running in serverless mode.
 *    - If the consistent snapshot thread is dead, the function returns with an
 * error.
 *    - Reads the snapshot index file and determines the oldest snapshot's
 * binlog.
 *    - Adjusts the purge log name if necessary to ensure it does not purge logs
 * required by the oldest snapshot.
 * 3. Locks the index file and opens it.
 * 4. Finds the log position by name in the index file.
 * 5. Opens the purge index file.
 * 6. Registers the purge index entry and identifies any garbage binlog slices.
 * 7. Synchronizes the purge index file.
 * 8. Removes logs from the index file.
 * 9. Purges each entry from the purge index file and deletes the corresponding
 * files.
 * 10. Cleans up garbage binlogs that do not exist in the index.
 * 11. Purges old consensus term binlog-index files.
 * 12. Closes the purge index file and unlocks the index file.
 * 13. Returns the final error code and message.
 */
std::tuple<int, std::string> Binlog_archive::purge_logs(const char *to_log) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("purge_logs"));
  int error = 0;
  std::string err_msg{};
  LOG_ARCHIVED_INFO log_info;
  char purge_log_name[FN_REFLEN + 1] = {0};
  std::string dirty_end_binlog{};
  uint64_t dirty_end_binlog_consensus_term = 0;

  err_msg.assign("purge binlog to: ");
  err_msg.append(to_log);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());

  strmake(purge_log_name, to_log, sizeof(purge_log_name) - 1);

  // Check consistent snapshot, only when start consistent snapshot.
  if (opt_consistent_snapshot_archive && opt_serverless) {
    IO_CACHE *index_file;
    char snapshot_info[4 * FN_REFLEN];
    size_t snapshot_info_len;
    Consistent_archive *consistent_snapshot_archive =
        Consistent_archive::get_instance();
    // If consistent snapshot thread is dead, can't purge binlog.
    if (consistent_snapshot_archive->is_thread_dead()) {
      return std::make_tuple(error, err_msg);
    }

    consistent_snapshot_archive->lock_consistent_snapshot_index();
    index_file =
        consistent_snapshot_archive->get_consistent_snapshot_index_file();
    if (!my_b_inited(index_file) ||
        reinit_io_cache(index_file, READ_CACHE, (my_off_t)0, false, false)) {
      consistent_snapshot_archive->unlock_consistent_snapshot_index();
      error = 1;
      err_msg.assign("snapshot.index is not opened");
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      return std::make_tuple(error, err_msg);
    }

    snapshot_info_len =
        my_b_gets(index_file, snapshot_info, sizeof(snapshot_info));
    if (snapshot_info_len > 1) {
      snapshot_info[--snapshot_info_len] = '\0';  // remove the newline
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
      std::string se_snapshot = left_string.substr(idx + 1);
      if (binlog_name.length() == 0) {
        consistent_snapshot_archive->unlock_consistent_snapshot_index();
        error = 0;
        err_msg.assign(
            "binlog is empty in the first snapshot, so purge "
            "nothing.");
        LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        return std::make_tuple(error, err_msg);
      }
      // Only purge binlog smaller than the oldest snapshot's binlog.
      if (compare_log_name(purge_log_name, binlog_name.c_str())) {
        strmake(purge_log_name, binlog_name.c_str(),
                sizeof(purge_log_name) - 1);
        err_msg.assign(
            "purge binlog smaller than the oldest snapshot's binlog: ");
        err_msg.append(binlog_name);
        LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      }
    }
    consistent_snapshot_archive->unlock_consistent_snapshot_index();
    if (index_file->error == -1) {
      error = 1;
      err_msg.assign("Failed to read snapshot.index");
      return std::make_tuple(error, err_msg);
    }
  }

  mysql_mutex_lock(&m_index_lock);
  if (open_index_file()) {
    error = 1;
    err_msg.assign("Binlog archive index file open failed");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  if ((error = find_log_pos_by_name(&log_info, purge_log_name))) {
    err_msg.assign("Failed to find log pos: ");
    err_msg.append(purge_log_name);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  if ((error = open_purge_index_file(true))) {
    err_msg.assign("Failed to open purge index");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  /*
    File name exists in index file; delete until we find this file
    or a file that is used.
  */
  if ((error = find_log_pos_by_name(&log_info, NullS))) {
    err_msg.assign("Failed to find log pos");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  while (compare_log_name(purge_log_name, log_info.log_file_name)) {
    if ((error = register_purge_index_entry(log_info.log_line))) {
      err_msg.assign("Failed to register purge");
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      goto err;
    }

    // Find the first purgable binlog slice from the index and clean it up as
    // garbage since these garbage archive files do not exist in the index.
    dirty_end_binlog.assign(log_info.log_file_name);
    dirty_end_binlog_consensus_term = log_info.slice_consensus_term;

    if (find_next_log_slice(&log_info)) break;
  }

  if ((error = sync_purge_index_file())) {
    err_msg.assign("Failed to sync purge index");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  /* We know how many files to delete. Update index file. */
  if ((error = remove_logs_from_index(&log_info))) {
    err_msg.assign("Failed to remove logs from index");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

err:
  int error_index = 0, close_error_index = 0;
  int error_purge = 0;
  /* Read each entry from m_purge_index_file and delete the file. */
  if (!error && is_inited_purge_index_file() &&
      (error_index = purge_index_entry())) {
    error_purge = 1;
    err_msg.assign("Failed to purge index entry");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
  }

  // Directly clean up those garbage binlog that is is smaller than
  // match_name or do not exist in the binlog-index.index.
  // Purge old consensus term binlog-index.index
  if (!error && !error_index && !error_purge) {
    // clean up those garbage binlog
    if (!dirty_end_binlog.empty()) {
      err_msg.assign(
          "clean up those garbage binlog that do not exist in the index: "
          "before ");
      err_msg.append(dirty_end_binlog);
      LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());

      // binlog/binlog.*, fetch all persistent binlog.
      std::string binlog_prefix;
      binlog_prefix.assign(m_binlog_archive_dir);
      binlog_prefix.append(BINLOG_ARCHIVE_BASENAME);

      std::vector<objstore::ObjectMeta> objects;
      bool finished = false;
      std::string start_after;
      do {
        std::vector<objstore::ObjectMeta> tmp_objects;
        objstore::Status ss = binlog_objstore->list_object(
            std::string_view(opt_objstore_bucket), binlog_prefix, false,
            start_after, finished, tmp_objects);
        if (!ss.is_succ()) {
          error = 1;
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LIST_OBJECT,
                 "list binlog failed", binlog_prefix.c_str(),
                 std::string(ss.error_message()).c_str());
          break;
        }
        objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
      } while (finished == false);

      // Starting from the smallest persistent binlog until the
      // `dirty_end_binlog`.
      if (!error) {
        for (const auto &object : objects) {
          size_t first_dot = object.key.find('.');
          if (first_dot == std::string::npos) {
            LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                   "invalid binlog key: ", object.key.c_str());
            continue;
          }
          size_t second_dot = object.key.find('.', first_dot + 1);
          if (second_dot == std::string::npos) {
            LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                   "invalid binlog key: ", object.key.c_str());
            continue;
          }
          std::string log_name = object.key.substr(0, second_dot);
          if (compare_log_name(dirty_end_binlog.c_str(), log_name.c_str()) >
              0) {
            err_msg.assign("delete garbage binlog from object store: ");
            err_msg.append(object.key);
            LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
            objstore::Status ss = binlog_objstore->delete_object(
                std::string_view(opt_objstore_bucket), object.key);
            if (!ss.is_succ()) {
              err_msg.append(" error=");
              err_msg.append(ss.error_message());
              LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
              // Continue cleaning up other binlog even if an error occurs.
            }
          }
        }
      }
    }
    // Purge old consensus term binlog-index.index
    /*
      binlog-index.00000000000000000011.index
      binlog-index.00000000000000000012.index
      binlog-index.00000000000000000013.index
      binlog-index.00000000000000000014.index
      binlog-index.00000000000000000015.index
    */
    if (!error && dirty_end_binlog_consensus_term > 0) {
      char index_name[FN_REFLEN] = {0};
      sprintf(index_name, "%s" BINLOG_ARCHIVE_CONSENSUS_TERM_EXT "%s",
              BINLOG_ARCHIVE_INDEX_FILE_BASENAME,
              static_cast<my_off_t>(dirty_end_binlog_consensus_term),
              BINLOG_ARCHIVE_INDEX_FILE_SUFFIX);
      err_msg.assign(
          "clean up those garbage binlog-index.index: "
          "consensus term before ");
      err_msg.append(index_name);
      LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());

      // Fetch all binlog-index.index.
      std::vector<objstore::ObjectMeta> objects;
      bool finished = false;
      std::string start_after;
      do {
        std::vector<objstore::ObjectMeta> tmp_objects;
        std::string binlog_index_prefix;
        binlog_index_prefix.assign(m_binlog_archive_dir);
        binlog_index_prefix.append(BINLOG_ARCHIVE_INDEX_FILE_BASENAME);
        objstore::Status ss = binlog_objstore->list_object(
            std::string_view(opt_objstore_bucket), binlog_index_prefix, false,
            start_after, finished, tmp_objects);
        if (!ss.is_succ()) {
          error = 1;
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LIST_OBJECT,
                 "list binlog-index.index failed",
                 BINLOG_ARCHIVE_INDEX_FILE_BASENAME,
                 std::string(ss.error_message()).c_str());
          break;
        }
        objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
      } while (finished == false);
      // Starting from the smallest persistent binlog-index.idex until the
      // `dirty_end_binlog_term`.
      if (!error) {
        for (const auto &object : objects) {
          size_t first_dot = object.key.find('.');
          if (first_dot == std::string::npos) {
            LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                   "invalid binlog-index key: ", object.key.c_str());
            continue;
          }
          size_t second_dot = object.key.find('.', first_dot + 1);
          if (second_dot == std::string::npos) {
            LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                   "invalid binlog-index key: ", object.key.c_str());
            continue;
          }
          std::string consensus_term_str =
              object.key.substr(first_dot + 1, second_dot);
          uint64_t consensus_term = std::stoull(consensus_term_str);
          if (dirty_end_binlog_consensus_term > consensus_term) {
            err_msg.assign(
                "delete garbage binlog-index.index from object store: ");
            err_msg.append(object.key);
            LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
            objstore::Status ss = binlog_objstore->delete_object(
                std::string_view(opt_objstore_bucket), object.key);
            if (!ss.is_succ()) {
              err_msg.append(" error=");
              err_msg.append(ss.error_message());
              LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
              // Continue cleaning up other binlog.index even if an error
              // occurs.
            }
          }
        }
      }
    }
  }

  close_error_index = close_purge_index_file();
  mysql_mutex_unlock(&m_index_lock);
  /*
    Error codes from purge logs take precedence.
    Then error codes from purging the index entry.
    Finally, error codes from closing the purge index file.
  */
  error = error ? error : (error_index ? error_index : close_error_index);
  return std::make_tuple(error, err_msg);
}

int Binlog_archive::set_purge_index_file_name() {
  int error = 0;
  DBUG_TRACE;
  if (fn_format(m_purge_index_file_name, BINLOG_ARCHIVE_INDEX_LOCAL_FILE,
                m_mysql_binlog_archive_dir, ".~rec~",
                MYF(MY_UNPACK_FILENAME | MY_SAFE_PATH | MY_REPLACE_EXT)) ==
      nullptr) {
    error = 1;
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to set purge index");
  }
  return error;
}

int Binlog_archive::open_purge_index_file(bool destroy) {
  DBUG_TRACE;

  int error = 0;
  File file = -1;

  if (destroy) close_purge_index_file();

  if (!my_b_inited(&m_purge_index_file)) {
    myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;
    if ((file = my_open(m_purge_index_file_name, O_RDWR | O_CREAT,
                        MYF(MY_WME))) < 0 ||
        init_io_cache(&m_purge_index_file, file, IO_SIZE,
                      (destroy ? WRITE_CACHE : READ_CACHE), 0, false, flags)) {
      error = 1;
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to open purge index");
    }
  }
  return error;
}

int Binlog_archive::close_purge_index_file() {
  DBUG_TRACE;
  int error = 0;

  if (my_b_inited(&m_purge_index_file)) {
    end_io_cache(&m_purge_index_file);
    error = my_close(m_purge_index_file.file, MYF(0));
  }
  my_delete(m_purge_index_file_name, MYF(0));
  new (&m_purge_index_file) IO_CACHE();

  return error;
}

bool Binlog_archive::is_inited_purge_index_file() {
  DBUG_TRACE;
  return my_b_inited(&m_purge_index_file);
}

int Binlog_archive::sync_purge_index_file() {
  DBUG_TRACE;
  int error = 0;

  if (!my_b_inited(&m_purge_index_file)) {
    return LOG_INFO_IO;
  }
  if ((error = flush_io_cache(&m_purge_index_file)) ||
      (error = my_sync(m_purge_index_file.file, MYF(MY_WME))))
    return error;

  return error;
}

int Binlog_archive::register_purge_index_entry(const char *entry) {
  int error = 0;
  DBUG_TRACE;

  if (!my_b_inited(&m_purge_index_file)) {
    return LOG_INFO_IO;
  }
  if ((error = my_b_write(&m_purge_index_file, (const uchar *)entry,
                          strlen(entry))) ||
      (error = my_b_write(&m_purge_index_file, (const uchar *)"\n", 1)))
    return error;

  return error;
}

/**
 * @brief Delete the binlog file from the object store.
 *
 * @param decrease_log_space
 * @return int
 */
int Binlog_archive::purge_index_entry() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("purge_index_entry"));
  DBUG_EXECUTE_IF("fault_injection_binlog_archive_purge_index_entry",
                  { return 1; });
  int error = 0;
  LOG_ARCHIVED_INFO log_info{};

  assert(my_b_inited(&m_purge_index_file));

  if ((error =
           reinit_io_cache(&m_purge_index_file, READ_CACHE, 0, false, false))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to reinit_io_cache");
    goto err;
  }

  for (;;) {
    size_t length = 0;
    char log_line[FN_REFLEN + 1] = {0};

    if ((length = my_b_gets(&m_purge_index_file, log_line, FN_REFLEN)) <= 1) {
      if (m_purge_index_file.error) {
        error = m_purge_index_file.error;
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, error);
        goto err;
      }

      /* Reached EOF */
      break;
    }

    /* Get rid of the trailing '\n' */
    log_line[length - 1] = 0;

    std::string in_str;
    in_str.assign(log_line);
    size_t idx = in_str.find("|");
    std::string found_slice_name = in_str.substr(0, idx);
    std::string found_consensus_index = in_str.substr(idx + 1);
    strmake(log_info.log_slice_name, found_slice_name.c_str(),
            sizeof(log_info.log_slice_name) - 1);

    // delete the archived binlog file from the object store.
    if (binlog_objstore) {
      // List all binlog slice.
      std::string binlog_keyid{};
      std::vector<objstore::ObjectMeta> objects;
      binlog_keyid.assign(m_binlog_archive_dir);
      binlog_keyid.append(log_info.log_slice_name);

      objstore::Status ss = binlog_objstore->delete_object(
          std::string_view(opt_objstore_bucket), binlog_keyid);
      if (!ss.is_succ()) {
        std::string err_msg;
        err_msg.assign("delete binlog slice from object store: ");
        err_msg.append(binlog_keyid);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        error = 1;
        goto err;
      }
      LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_PURGE_LOG,
             binlog_keyid.c_str());
    }
  }

err:
  return error;
}

/**
 * @brief Remove logs entry from persistent index file.
 * As long as the persistent index file is successfully updated, it is
 * considered that the log entry has been successfully removed. If the local
 * index file fails to update afterwards, it can be restored by restarting the
 * binlog archive.
 * @param log_info
 * @return int
 */
int Binlog_archive::remove_logs_from_index(LOG_ARCHIVED_INFO *log_info) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("remove_logs_from_index"));
  DBUG_EXECUTE_IF("fault_injection_remove_logs_from_index", { return 1; });

  if (!my_b_inited(&m_index_file)) {
    goto err;
  }

  // 1. update local crash index file.
  if (open_crash_safe_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to open_crash_safe_index_file in remove_logs_from_index");
    goto err;
  }

  if (copy_file(&m_index_file, &m_crash_safe_index_file,
                log_info->index_file_start_offset)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to copy_file in remove_logs_from_index");
    goto err;
  }

  if (close_crash_safe_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to close_crash_safe_index_file");
    goto err;
  }

  // Rename local crash index file to local index file.
  // Forcibly close the index file to allow the archive thread to download the
  // index from the object store again and reopen it when binlog archive retry.
  if (move_crash_safe_index_file_to_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to move_crash_safe_index_file_to_index_file");
    goto err;
  }

  return 0;

err:
  return LOG_INFO_IO;
}

/**
 * @brief Show the persistent binlog files stored in the object store.
 *
 * This function lists the binlog files stored in the object store and appends
 * their metadata to the provided vector of ObjectMeta objects.
 *
 * @param objects A reference to a vector where the metadata of the binlog files
 *                will be stored.
 * @return int Returns 0 on success, or 1 if an error occurs while listing the
 *             binlog files.
 */
int Binlog_archive::show_binlog_persistent_files(
    std::vector<objstore::ObjectMeta> &objects) {
  DBUG_TRACE;
  int error = 0;
  if (binlog_objstore != nullptr) {
    bool finished = false;
    std::string start_after;
    std::string binlog_prefix;
    binlog_prefix.assign(m_binlog_archive_dir);
    binlog_prefix.append(BINLOG_ARCHIVE_BASENAME);

    do {
      std::vector<objstore::ObjectMeta> tmp_objects;
      objstore::Status ss = binlog_objstore->list_object(
          std::string_view(opt_objstore_bucket), binlog_prefix, false,
          start_after, finished, tmp_objects);
      if (!ss.is_succ()) {
        std::string err_msg;
        if (ss.error_code() == objstore::Errors::SE_NO_SUCH_KEY) {
          error = 0;
          return error;
        }
        error = 1;
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LIST_OBJECT, "list binlog failed",
               binlog_prefix.c_str(), std::string(ss.error_message()).c_str());
        return error;
      }
      objects.insert(objects.end(), tmp_objects.begin(), tmp_objects.end());
    } while (finished == false);
  }
  return error;
}

inline int Binlog_archive::reset_transmit_packet(size_t event_len) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("event_len: %zu, m_packet->alloced_length: %zu",
                      event_len, m_packet.alloced_length()));
  assert(m_packet.alloced_length() >= PACKET_MIN_SIZE);

  m_packet.length(0);          // size of the content
  qs_append('\0', &m_packet);  // Set this as an OK packet

  /* Resizes the buffer if needed. */
  if (event_len > 0 && grow_packet(event_len)) return 1;

  DBUG_PRINT("info", ("m_packet.alloced_length: %zu (after potential "
                      "reallocation)",
                      m_packet.alloced_length()));

  return 0;
}

inline bool Binlog_archive::grow_packet(size_t extra_size) {
  DBUG_TRACE;
  size_t cur_buffer_size = m_packet.alloced_length();
  size_t cur_buffer_used = m_packet.length();
  size_t needed_buffer_size = cur_buffer_used + extra_size;

  if (extra_size > (PACKET_MAX_SIZE - cur_buffer_used))
    /*
       Not enough memory: requesting packet to be bigger than the max
       allowed - PACKET_MAX_SIZE.
    */
    return true;

  /* Grow the buffer if needed. */
  if (needed_buffer_size > cur_buffer_size) {
    size_t new_buffer_size;
    new_buffer_size =
        calc_grow_buffer_size(cur_buffer_size, needed_buffer_size);

    if (!new_buffer_size) return true;

    if (m_packet.mem_realloc(new_buffer_size)) return true;

    /*
     Calculates the new, smaller buffer, size to use the next time
     one wants to shrink the buffer.
    */
    calc_shrink_buffer_size(new_buffer_size);
  }

  return false;
}

inline bool Binlog_archive::shrink_packet() {
  DBUG_TRACE;
  bool res = false;
  size_t cur_buffer_size = m_packet.alloced_length();
  size_t buffer_used = m_packet.length();

  assert(!(cur_buffer_size < PACKET_MIN_SIZE));

  /*
     If the packet is already at the minimum size, just
     do nothing. Otherwise, check if we should shrink.
   */
  if (cur_buffer_size > PACKET_MIN_SIZE) {
    /* increment the counter if we used less than the new shrink size. */
    if (buffer_used < m_new_shrink_size) {
      m_half_buffer_size_req_counter++;

      /* Check if we should shrink the buffer. */
      if (m_half_buffer_size_req_counter == PACKET_SHRINK_COUNTER_THRESHOLD) {
        /*
         The last PACKET_SHRINK_COUNTER_THRESHOLD consecutive packets
         required less than half of the current buffer size. Lets shrink
         it to not hold more memory than we potentially need.
        */
        m_packet.shrink(m_new_shrink_size);

        /*
           Calculates the new, smaller buffer, size to use the next time
           one wants to shrink the buffer.
         */
        calc_shrink_buffer_size(m_new_shrink_size);

        /* Reset the counter. */
        m_half_buffer_size_req_counter = 0;
      }
    } else
      m_half_buffer_size_req_counter = 0;
  }
#ifndef NDEBUG
  if (res == false) {
    assert(m_new_shrink_size <= cur_buffer_size);
    assert(m_packet.alloced_length() >= PACKET_MIN_SIZE);
  }
#endif
  return res;
}

inline size_t Binlog_archive::calc_grow_buffer_size(size_t current_size,
                                                    size_t min_size) {
  /* Check that a sane minimum buffer size was requested.  */
  assert(min_size > PACKET_MIN_SIZE);
  if (min_size > PACKET_MAX_SIZE) return 0;

  /*
     Even if this overflows (PACKET_MAX_SIZE == UINT_MAX32) and
     new_size wraps around, the min_size will always be returned,
     i.e., it is a safety net.

     Also, cap new_size to PACKET_MAX_SIZE (in case
     PACKET_MAX_SIZE < UINT_MAX32).
   */
  size_t new_size = static_cast<size_t>(
      std::min(static_cast<double>(PACKET_MAX_SIZE),
               static_cast<double>(current_size * PACKET_GROW_FACTOR)));

  new_size = ALIGN_SIZE(std::max(new_size, min_size));

  return new_size;
}

void Binlog_archive::calc_shrink_buffer_size(size_t current_size) {
  size_t new_size = static_cast<size_t>(
      std::max(static_cast<double>(PACKET_MIN_SIZE),
               static_cast<double>(current_size * PACKET_SHRINK_FACTOR)));

  m_new_shrink_size = ALIGN_SIZE(new_size);
}

/**
 * @brief  the lower time bound for automatic purging of binlog archives.
 *
 * @return The calculated expiration time as a time_t value.
 */
static time_t calculate_auto_purge_lower_time_bound() {
  if (DBUG_EVALUATE_IF("expire_all_persistent_binlogs", true, false)) {
    return time(nullptr);
  }

  int64 expiration_time = 0;
  int64 current_time = time(nullptr);

  if (opt_binlog_archive_expire_seconds > 0)
    expiration_time = current_time - opt_binlog_archive_expire_seconds;

  // check for possible overflow conditions (4 bytes time_t)
  if (expiration_time < std::numeric_limits<time_t>::min())
    expiration_time = std::numeric_limits<time_t>::min();

  return static_cast<time_t>(expiration_time);
}
