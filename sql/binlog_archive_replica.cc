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

#include "sql/binlog_archive_replica.h"

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
#include "sql/changestreams/apply/replication_thread_status.h"
#include "sql/consensus_log_event.h"
#include "sql/consistent_archive.h"
#include "sql/debug_sync.h"
#include "sql/derror.h"
#include "sql/log.h"
#include "sql/log_event.h"
#include "sql/mdl.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/protocol_classic.h"
#include "sql/rpl_handler.h"
#include "sql/rpl_io_monitor.h"
#include "sql/rpl_msr.h"
#include "sql/rpl_source.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "unsafe_string_append.h"

// Global binlog archive thread object
static Binlog_archive_replica mysql_binlog_archive_replica;
static my_thread_handle mysql_binlog_archive_replica_pthd;

static mysql_mutex_t m_binlog_archive_replica_run_lock;
static mysql_cond_t m_binlog_archive_replica_run_cond;

#ifdef HAVE_PSI_INTERFACE
static PSI_thread_key THR_KEY_binlog_archive_replica;
static PSI_thread_key THR_KEY_binlog_archive_io_worker;
static PSI_thread_key THR_KEY_binlog_archive_replica_relay_worker;
static PSI_mutex_key PSI_binlog_archive_replica_lock_key;
static PSI_cond_key PSI_binlog_archive_replica_cond_key;
static PSI_mutex_key PSI_binlog_archive_replica_io_worker_lock_key;
static PSI_cond_key PSI_binlog_archive_replica_io_worker_cond_key;
static PSI_mutex_key PSI_binlog_archive_replica_relay_worker_lock_key;
static PSI_cond_key PSI_binlog_archive_replica_relay_worker_cond_key;
static PSI_mutex_key PSI_binlog_index_lock_key;

static PSI_mutex_key PSI_binlog_slice_lock_key;
static PSI_cond_key PSI_binlog_slice_queue_cond_key;
static PSI_cond_key PSI_binlog_slice_map_cond_key;

/** The instrumentation key to use for opening the log index file. */
static PSI_file_key PSI_binlog_archive_log_index_key;
/** The instrumentation key to use for opening a log index cache file. */
static PSI_file_key PSI_binlog_archive_log_index_cache_key;

// The name lenght include a terminating NUL character of thread can not exceed
// 12 characters, if flag is not PSI_FLAG_SINGLETON | PSI_FLAG_NO_SEQNUM.
static PSI_thread_info all_binlog_archive_threads[] = {
    {&THR_KEY_binlog_archive_replica, "binlog_arch_rpl", "bin_ac_rpl",
     PSI_FLAG_SINGLETON | PSI_FLAG_THREAD_SYSTEM, 0, PSI_DOCUMENT_ME},
    {&THR_KEY_binlog_archive_io_worker, "binlog_arch_rpl_io", "bin_ac_rpl_i",
     PSI_FLAG_AUTO_SEQNUM | PSI_FLAG_THREAD_SYSTEM, 0, PSI_DOCUMENT_ME},
    {&THR_KEY_binlog_archive_replica_relay_worker, "binlog_arch_rpl_relay",
     "bin_ac_rpl_r", PSI_FLAG_SINGLETON | PSI_FLAG_THREAD_SYSTEM, 0,
     PSI_DOCUMENT_ME}};

static PSI_mutex_info all_binlog_archive_mutex_info[] = {
    {&PSI_binlog_archive_replica_lock_key, "binlog_archive_replica::mutex", 0,
     0, PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_replica_io_worker_lock_key,
     "binlog_archive_replica::mutex", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_replica_relay_worker_lock_key,
     "binlog_archive_replica::mutex", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_slice_lock_key, "binlog_archive_index::mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_index_lock_key, "binlog_archive_index::mutex", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_cond_info all_binlog_archive_cond_info[] = {
    {&PSI_binlog_archive_replica_cond_key, "binlog_archive_replica::condition",
     0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_replica_io_worker_cond_key,
     "binlog_archive_replica::condition", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_replica_relay_worker_cond_key,
     "binlog_archive_replica::condition", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_slice_queue_cond_key, "binlog_archive_replica::condition", 0,
     0, PSI_DOCUMENT_ME},
    {&PSI_binlog_slice_map_cond_key, "binlog_archive_replica::condition", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_file_info all_binlog_archive_files[] = {
    {&PSI_binlog_archive_log_index_key, "binlog_archive_index", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_index_cache_key, "binlog_archive_index_cache", 0,
     0, PSI_DOCUMENT_ME}};
#endif

static void *mysql_binlog_archive_replica_action(void *arg);
static bool remove_file(const std::string &file);
static int compare_log_name(const char *log_1, const char *log_2);
static THD *set_thread_context();

/**
 * @brief
 *
 * @return int
 */
int Binlog_archive_replica::channel_create() {
  DBUG_TRACE;
  Master_info *mi = nullptr;
  std::string err_msg;
  int error = 0;

  channel_map.wrlock();
  /* Get the Master_info of the channel */
  mi = channel_map.get_mi(BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME);

  if (mi) {
    err_msg.assign("binlog archive replica channel already exists ");
    err_msg.append(BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME);
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
    goto err;
  }
  /* create a new binlog archive replica channel if doesn't exist */
  // set global relay_log_recovery=true
  // For the first creation of an mi, the locally restored latest consensus
  // index is used as the starting synchronization point with the source object
  // storage.
  err_msg.assign("create binlog archive replica new channel ");
  err_msg.append(BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME);
  if ((error = add_new_channel(&mi, BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME))) {
    err_msg.append(" failed");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, "add_new_channel failed");
    goto err;
  }
  err_msg.append(" success");
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
  if ((error = load_mi_and_rli_from_repositories(
           mi, false, SLAVE_IO | SLAVE_SQL, false, true)))
    goto err;
  // Initialize the channel master info start binlog and position with the
  // source object storage
  assert(opt_binlog_archive_replica_source_log_file);
  mysql_mutex_lock(mi->rli->relay_log.get_log_lock());
  mysql_mutex_lock(&mi->data_lock);
  mi->set_master_log_name(opt_binlog_archive_replica_source_log_file);
  mi->set_master_log_pos(opt_binlog_archive_replica_source_log_pos);
  // Flush the master info to the repository
  flush_master_info(mi, true, false);
  mysql_mutex_unlock(&mi->data_lock);
  mysql_mutex_unlock(mi->rli->relay_log.get_log_lock());

  err_msg.assign("init binlog archive replica channel master info position");
  err_msg.append(mi->get_for_channel_str());
  err_msg.append(" file=");
  err_msg.append(mi->get_master_log_name());
  err_msg.append(" pos=");
  err_msg.append(std::to_string(mi->get_master_log_pos()));
  // print the current replication position
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());

err:
  channel_map.unlock();

  return error;
}

int Binlog_archive_replica::channel_start() {
  Master_info *mi = nullptr;
  std::string err_msg;
  int error = 0;
  LEX_SLAVE_CONNECTION lex_connection;
  LEX_MASTER_INFO *lex_mi;
  lex_mi = new LEX_MASTER_INFO();

  channel_map.wrlock();
  /* Get the Master_info of the channel */
  mi = channel_map.get_mi(BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME);

  if (mi == nullptr) {
    goto err;
  }

  //clear_info(mi);
  /*
    Read the channel configuration from the repository if the channel name
    was read from the repository.
    load_mi_and_rli_from_repositories -> rli_init_info() -> init_recovery
  */
  if ((error = load_mi_and_rli_from_repositories(
           mi, false, SLAVE_IO | SLAVE_SQL, false, false)))
    goto err;
  err_msg.assign(
      "load binlog archive replica channel master info and rli info");
  err_msg.append(mi->get_for_channel_str());
  err_msg.append(" file=");
  err_msg.append(mi->get_master_log_name());
  err_msg.append(" pos=");
  err_msg.append(std::to_string(mi->get_master_log_pos()));
  // print the current replication position
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());

  lex_mi->channel = BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME;
  lex_connection.reset();

  mi->rli->opt_replica_parallel_workers = opt_mts_replica_parallel_workers;
  mi->rli->checkpoint_group = opt_mta_checkpoint_group;
  if (mts_parallel_option == MTS_PARALLEL_TYPE_DB_NAME)
    mi->rli->channel_mts_submode = MTS_PARALLEL_TYPE_DB_NAME;
  else
    mi->rli->channel_mts_submode = MTS_PARALLEL_TYPE_LOGICAL_CLOCK;
  mi->abort_slave = false;
  mi->rli->abort_slave = false;
  // Startup applier threads
  if ((error =
           start_slave(m_thd, &lex_connection, lex_mi, SLAVE_SQL, mi, false))) {
    goto err;
  }

  // Init master format description event
  mysql_mutex_lock(mi->rli->relay_log.get_log_lock());
  mysql_mutex_lock(&mi->data_lock);
  mi->set_mi_description_event(new Format_description_log_event());
  /* as we are here, we tried to allocate the event */
  if (mi->get_mi_description_event() == nullptr) {
    mysql_mutex_unlock(&mi->data_lock);
    mysql_mutex_unlock(mi->rli->relay_log.get_log_lock());
    goto err;
  }
  mi->get_mi_description_event()->common_footer->checksum_alg =
      mi->rli->relay_log.relay_log_checksum_alg;
  assert(mi->get_mi_description_event()->common_footer->checksum_alg !=
         binary_log::BINLOG_CHECKSUM_ALG_UNDEF);
  assert(mi->rli->relay_log.relay_log_checksum_alg !=
         binary_log::BINLOG_CHECKSUM_ALG_UNDEF);
  mysql_mutex_unlock(&mi->data_lock);
  mysql_mutex_unlock(mi->rli->relay_log.get_log_lock());

  m_master_info = mi;
err:
  channel_map.unlock();
  delete lex_mi;
  return error;
}

int Binlog_archive_replica::channel_stop() {
  int error = 0;
  channel_map.wrlock();
  Master_info *mi = channel_map.get_mi(BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME);

  if (mi == nullptr) {
    error = 1;
    goto err;
  }

  mi->channel_wrlock();
  lock_slave_threads(mi);
  m_thd->set_skip_readonly_check();
  error =
      terminate_slave_threads(mi, SLAVE_SQL, rpl_stop_replica_timeout, false);
  m_thd->reset_skip_readonly_check();
  unlock_slave_threads(mi);
  mi->channel_unlock();
  m_master_info = nullptr;
err:
  channel_map.unlock();
  return error;
}

/**
 * @brief Check if the binlog archive replica applier thread is running.
 *
 * @return true
 * @return false
 */
bool Binlog_archive_replica::channel_is_running() {
  DBUG_TRACE;
  bool is_running = false;

  channel_map.wrlock();
  Master_info *mi = channel_map.get_mi(BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME);
  if (mi == nullptr) {
    goto err;
  }
  mysql_mutex_lock(&mi->rli->run_lock);
  is_running = mi->rli->slave_running;
  mysql_mutex_unlock(&mi->rli->run_lock);

err:
  channel_map.unlock();
  return is_running;
}

Binlog_archive_replica_io_worker::Binlog_archive_replica_io_worker(
    Binlog_archive_replica *archive, int worker_id)
    : m_archive(archive),
      m_worker_id(worker_id),
      m_thd(nullptr),
      m_thd_state(),
      atomic_binlog_archive_replica_io_worker_waiting(true) {
  mysql_mutex_init(PSI_binlog_archive_replica_io_worker_lock_key,
                   &m_worker_run_lock, MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_binlog_archive_replica_io_worker_cond_key,
                  &m_worker_run_cond);
}

Binlog_archive_replica_io_worker::~Binlog_archive_replica_io_worker() {
  stop();
  mysql_mutex_destroy(&m_worker_run_lock);
  mysql_cond_destroy(&m_worker_run_cond);
}

/**
 * @brief Start the binlog archive replica io worker thread.
 * @return true if the worker thread was successfully started, false otherwise.
 */
bool Binlog_archive_replica_io_worker::start() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("start"));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
         "starting");
  mysql_mutex_lock(&m_worker_run_lock);
  if (mysql_thread_create(THR_KEY_binlog_archive_io_worker, &m_thread,
                          &connection_attrib, thread_launcher, (void *)this)) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
           "start failed");
    mysql_mutex_unlock(&m_worker_run_lock);
    return false;
  }
  m_thd_state.set_created();
  while (m_thd_state.is_alive_not_running()) {
    DBUG_PRINT(
        "sleep",
        ("Waiting for binlog archive replica io worker thread to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_worker_run_cond, &m_worker_run_lock, &abstime);
  }
  mysql_mutex_unlock(&m_worker_run_lock);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
         "started");
  return true;
}

void Binlog_archive_replica_io_worker::stop() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("stop"));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
         "terminate begin");
  mysql_mutex_lock(&m_worker_run_lock);
  if (m_thd_state.is_thread_dead()) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
           "terminated");
    mysql_mutex_unlock(&m_worker_run_lock);
    return;
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
         "terminating binlog archive replica io worker");
  mysql_mutex_unlock(&m_worker_run_lock);
  terminate_binlog_archive_replica_io_worker_thread();
  /* Wait until the thread is terminated. */
  my_thread_join(&m_thread, nullptr);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
         "terminate end");
}

/**
 * @brief Binlog archive thread terminate worker thread.
 *
 * @return true if the worker thread was successfully terminated, false
 * otherwise.
 */
int Binlog_archive_replica_io_worker::
    terminate_binlog_archive_replica_io_worker_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("terminate_binlog_archive_replica_io_worker_thread"));
  mysql_mutex_lock(&m_worker_run_lock);
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep",
               ("Waiting for binlog archive replica io worker to stop"));
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
 * @brief Worker thread function for pulling binlog slices from object
 * store to local.
 * @return void* Thread return value (nullptr)
 */
void *Binlog_archive_replica_io_worker::worker_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("worker_thread"));
  Binlog_archive_replica *archive = m_archive;
  std::string err_msg;

  m_thd = set_thread_context();

  mysql_mutex_lock(&m_worker_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_worker_run_cond);
  mysql_mutex_unlock(&m_worker_run_lock);

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
         "thread running");
  while (true) {
    int failure_trials =
        Binlog_archive::MAX_RETRIES_FOR_OBJECT_MANIPULATION_FAILURE;
    Binlog_expected_pull_slice slice;
    int error = 0;

    mysql_mutex_lock(&archive->m_slice_mutex);
    // Wait for the slice key to be available.
    m_thd->ENTER_COND(&archive->m_queue_cond, &archive->m_slice_mutex, nullptr,
                      nullptr);
    set_binlog_archive_replica_io_worker_waiting(true);
    while (archive->m_expected_slice_queue.empty() &&
           !unlikely(m_thd->killed)) {
      // Wait for signal from the index worker thread that the new expected
      // slice key is added to the queue. Or the worker thread is killed.
      mysql_cond_wait(&archive->m_queue_cond, &archive->m_slice_mutex);
    }
    // When the worker thread is killed, exit.
    if (unlikely(m_thd->killed)) {
      mysql_mutex_unlock(&archive->m_slice_mutex);
      m_thd->EXIT_COND(nullptr);
      break;
    }
    // The io worker thread is no longer waiting.
    set_binlog_archive_replica_io_worker_waiting(false);
    // Get the slice key from the expected queue.
    archive->m_expected_slice_queue.de_queue(&slice);
    mysql_mutex_unlock(&archive->m_slice_mutex);
    m_thd->EXIT_COND(nullptr);
    // Signal the binlog archive replica thread that maybe wait, because the
    // slice queue maybe full.
    mysql_cond_signal(&archive->m_queue_cond);

    std::string slice_data_cache;
    // pull the slice to the local cache.
    while (failure_trials-- > 0) {
      err_msg.assign(" get slice object ");
      err_msg.append(slice.m_slice_keyid);
      DBUG_EXECUTE_IF("simulate_get_slice_object_error_on_io_worker", {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
               "simulate_get_slice_object_error_on_io_worker");
        goto end;
      });
      if (unlikely(m_thd->killed)) {
        error = 1;
        break;
      }

      objstore::Status ss = m_archive->get_objstore()->get_object(
          std::string_view(opt_source_objectstore_bucket),
          std::string_view(slice.m_slice_keyid), slice_data_cache);
      err_msg.append(" size=");
      err_msg.append(std::to_string(slice_data_cache.size()));
      err_msg.append(" ");
      err_msg.append(std::to_string(slice.m_file_seq));
      err_msg.append("/");
      err_msg.append(std::to_string(slice.m_slice_seq));

      if (!ss.is_succ()) {
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
               err_msg.c_str());
        my_sleep(1000);
        continue;
      }
      error = 0;
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
             err_msg.c_str());
      break;
    }
    // If the slice pull failed, break the loop and exit the thread.
    // The binlog archive replica thread will retry create the io worker thread.
    if (error) break;

    // Notify the binlog archive relay worker that the slice has been
    // pulled.
    err_msg.assign("slice pull");
    err_msg.append(" log_slice_keyid=");
    err_msg.append(slice.m_slice_keyid);
    err_msg.append(" file_seq=");
    err_msg.append(std::to_string(slice.m_file_seq));
    err_msg.append(" slice_seq=");
    err_msg.append(std::to_string(slice.m_slice_seq));

    mysql_mutex_lock(&archive->m_slice_mutex);
    // If the slice status map is empty or the slice status is not found in the
    // map, there is no need to update the slice status map. Skip the update.
    if (archive->m_slice_status_map.empty() ||
        archive->m_slice_status_map[slice.m_file_seq].empty() ||
        archive->m_slice_status_map[slice.m_file_seq].find(slice.m_slice_seq) ==
            archive->m_slice_status_map[slice.m_file_seq].end()) {
      mysql_mutex_unlock(&archive->m_slice_mutex);
      err_msg.append(" skip deprecated slice when slice status map is empty");
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
             err_msg.c_str());
      continue;
    }

    auto &slice_status =
        archive->m_slice_status_map[slice.m_file_seq][slice.m_slice_seq];
    err_msg.append(" slice=");
    err_msg.append(slice_status.archived_info.log_slice_name);
    err_msg.append(" end_pos=");
    err_msg.append(
        std::to_string(slice_status.archived_info.log_slice_end_pos));
    err_msg.append(" success");
    slice_status.persisted = SLICE_PULL_SUCCESS;
    slice_status.archived_info.slice_data_cache = slice_data_cache;
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
           err_msg.c_str());
    mysql_mutex_unlock(&archive->m_slice_mutex);
    mysql_cond_signal(&archive->m_map_cond);
  }

end:
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_IO_WORKER, m_worker_id,
         "thread end");

  mysql_mutex_lock(&m_worker_run_lock);
  delete m_thd;
  m_thd = nullptr;
  m_thd_state.set_terminated();
  mysql_cond_broadcast(&m_worker_run_cond);
  mysql_mutex_unlock(&m_worker_run_lock);
  my_thread_end();
  my_thread_exit(nullptr);
}

Binlog_archive_replica_relay_worker::Binlog_archive_replica_relay_worker(
    Binlog_archive_replica *archive)
    : m_archive(archive), m_thd(nullptr), m_thd_state() {
  mysql_mutex_init(PSI_binlog_archive_replica_relay_worker_lock_key,
                   &m_worker_run_lock, MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_binlog_archive_replica_relay_worker_cond_key,
                  &m_worker_run_cond);
}

Binlog_archive_replica_relay_worker::~Binlog_archive_replica_relay_worker() {
  stop();
  mysql_mutex_destroy(&m_worker_run_lock);
  mysql_cond_destroy(&m_worker_run_cond);
}

/**
 * @brief Start the binlog archive worker thread.
 * @return true if the worker thread was successfully started, false otherwise.
 */
bool Binlog_archive_replica_relay_worker::start() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("start"));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER, "starting");
  mysql_mutex_lock(&m_worker_run_lock);
  if (mysql_thread_create(THR_KEY_binlog_archive_replica_relay_worker,
                          &m_thread, &connection_attrib, thread_launcher,
                          (void *)this)) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
           "start failed");
    mysql_mutex_unlock(&m_worker_run_lock);
    return false;
  }
  m_thd_state.set_created();
  while (m_thd_state.is_alive_not_running()) {
    DBUG_PRINT("sleep",
               ("Waiting for binlog archive replica relay worker to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_worker_run_cond, &m_worker_run_lock, &abstime);
  }
  mysql_mutex_unlock(&m_worker_run_lock);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER, "started");
  return true;
}

void Binlog_archive_replica_relay_worker::stop() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("stop"));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
         "terminate begin");
  mysql_mutex_lock(&m_worker_run_lock);
  if (m_thd_state.is_thread_dead()) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER, "terminated");
    mysql_mutex_unlock(&m_worker_run_lock);
    return;
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
         "terminating binlog archive replica relay worker");
  mysql_mutex_unlock(&m_worker_run_lock);
  terminate_binlog_archive_replica_relay_worker();
  /* Wait until the thread is terminated. */
  my_thread_join(&m_thread, nullptr);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER, "terminate end");
}

/**
 * @brief Binlog archive thread terminate worker thread.
 *
 * @return true if the worker thread was successfully terminated, false
 * otherwise.
 */
int Binlog_archive_replica_relay_worker::
    terminate_binlog_archive_replica_relay_worker() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("terminate_binlog_archive_replica_relay_worker"));
  mysql_mutex_lock(&m_worker_run_lock);
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep",
               ("Waiting for binlog archive replica relay worker to stop"));
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

inline void Binlog_archive_replica_relay_worker::calc_event_checksum(
    uchar *event_ptr, size_t event_len) {
  ha_checksum crc = checksum_crc32(0L, nullptr, 0);
  crc = checksum_crc32(crc, event_ptr, event_len - BINLOG_CHECKSUM_LEN);
  int4store(event_ptr + event_len - BINLOG_CHECKSUM_LEN, crc);
}

int Binlog_archive_replica_relay_worker::fake_rotate_event(
    const char *next_log_file, my_off_t log_pos, ulong server_id,
    binary_log::enum_binlog_checksum_alg event_checksum_alg,
    std::string &rotate_buf) {
  DBUG_TRACE;
  const char *p = next_log_file + dirname_length(next_log_file);
  size_t ident_len = strlen(p);
  size_t event_len =
      ident_len + LOG_EVENT_HEADER_LEN + Binary_log_event::ROTATE_HEADER_LEN +
      ((event_checksum_alg > binary_log::BINLOG_CHECKSUM_ALG_OFF &&
        event_checksum_alg < binary_log::BINLOG_CHECKSUM_ALG_ENUM_END)
           ? BINLOG_CHECKSUM_LEN
           : 0);
  rotate_buf.resize(event_len);
  uchar *header =
      reinterpret_cast<uchar *>(const_cast<char *>(rotate_buf.c_str()));
  uchar *rotate_header = header + LOG_EVENT_HEADER_LEN;
  /*
    'when' (the timestamp) is set to 0 so that slave could distinguish between
    real and fake Rotate events (if necessary)
  */
  int4store(header, 0);
  header[EVENT_TYPE_OFFSET] = binary_log::ROTATE_EVENT;
  int4store(header + SERVER_ID_OFFSET, server_id);
  int4store(header + EVENT_LEN_OFFSET, static_cast<uint32>(event_len));
  int4store(header + LOG_POS_OFFSET, 0);
  int2store(header + FLAGS_OFFSET, LOG_EVENT_ARTIFICIAL_F);

  int8store(rotate_header, log_pos);
  memcpy(rotate_header + Binary_log_event::ROTATE_HEADER_LEN, p, ident_len);

  if (event_checksum_alg > binary_log::BINLOG_CHECKSUM_ALG_OFF &&
      event_checksum_alg < binary_log::BINLOG_CHECKSUM_ALG_ENUM_END)
    calc_event_checksum(header, event_len);

  return 0;
}

/**
 * @brief
 *
 * @return void*
 */
void *Binlog_archive_replica_relay_worker::worker_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("worker_thread"));
  Binlog_archive_replica *archive = m_archive;

  m_thd = set_thread_context();
  mysql_mutex_lock(&m_worker_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_worker_run_cond);
  mysql_mutex_unlock(&m_worker_run_lock);

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
         "thread running");

  while (!unlikely(m_thd->killed)) {
    std::vector<Slice_pull_status> to_process;
    std::string err_msg;
    int error = 0;

    mysql_mutex_lock(&archive->m_slice_mutex);
    // Wait for the slice to be available.
    m_thd->ENTER_COND(&archive->m_map_cond, &archive->m_slice_mutex, nullptr,
                      nullptr);
    while (((archive->m_slice_status_map.empty() ||
             (archive->m_slice_status_map.begin())->second.empty() ||
             (((archive->m_slice_status_map.begin())->second).begin())
                     ->second.persisted == SLICE_NOT_PULL) &&
            !unlikely(m_thd->killed))) {
      // Wait for signal from the binlog archive io thread that the new expected
      // slice is added to the queue. Or the worker thread is killed.
      mysql_cond_wait(&archive->m_map_cond, &archive->m_slice_mutex);
    }
    if (unlikely(m_thd->killed)) {
      mysql_mutex_unlock(&archive->m_slice_mutex);
      m_thd->EXIT_COND(nullptr);
      break;
    }
    // Read the continuous slice objects that have been pulled, in the order of
    // add_slice() by the binlog archive replica thread.
    auto log_map = archive->m_slice_status_map.begin();
    while (log_map != archive->m_slice_status_map.end()) {
      auto &slice_map = log_map->second;

      // Process slices from beginning of current binlog file
      auto slice_it = slice_map.begin();
      while (slice_it != slice_map.end()) {
        if (slice_it->second.persisted == SLICE_NOT_PULL) {
          // Stop at first non-pull slice
          break;
        }
        assert(slice_it->second.persisted == SLICE_PULL_SUCCESS);
        err_msg.assign(" binlog slice persisted, slice=");
        err_msg.append(slice_it->second.archived_info.log_slice_name);
        err_msg.append(" persisted slice map file_seq=");
        err_msg.append(std::to_string(slice_it->second.file_seq));
        err_msg.append(" slice_seq=");
        err_msg.append(std::to_string(slice_it->second.slice_seq));
        LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
               err_msg.c_str());
        // Add to processing batch
        to_process.push_back(slice_it->second);
        slice_it = slice_map.erase(slice_it);
      }

      if (slice_map.empty()) {
        // Remove empty binlog file map, then continue to next binlog file map
        log_map = archive->m_slice_status_map.erase(log_map);
      } else {
        // Stop at first file with SLICE_NOT_PULL slice.
        break;
      }
    }
    mysql_mutex_unlock(&archive->m_slice_mutex);
    m_thd->EXIT_COND(nullptr);

    // Batch process pulled slices.
    if (!to_process.empty()) {
      for (const auto &slice : to_process) {
        my_off_t reader_size = 0;
        my_off_t cache_size =
            slice.archived_info.slice_data_cache.size();  // current slice size.
        my_off_t end_pos =
            slice.archived_info
                .log_slice_end_pos;  // The end position of the binlog where the
                                     // current slice is located.
        my_off_t start_read_pos =
            slice.archived_info
                .m_start_read_pos;  // The read start position of the binlog
                                    // where the current slice is located.
        bool first_slice =
            slice.archived_info.first_slice;  // Whether the current slice is
                                              // the first slice of the binlog.

        if (error || unlikely(m_thd->killed)) {
          error = 1;
          break;
        }

        // If it is the first slice, the binlog header needs to be skipped
        // during reading; subsequent slices are read directly from position 0.
        if (first_slice) {
          reader_size = BIN_LOG_HEADER_SIZE;
        } else if ((cache_size - (end_pos - start_read_pos)) > 0) {
          // Jump to the start_read_pos position of the slice.
          reader_size = cache_size - (end_pos - start_read_pos);
          LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
                 "jump to start_read_pos position of the slice");
        }
        err_msg.assign("relay event from ");
        err_msg.append(first_slice ? "the first slice " : "the slice ");
        err_msg.append(slice.archived_info.log_slice_name);
        err_msg.append(" start_read_pos=");
        err_msg.append(std::to_string(start_read_pos));
        err_msg.append(" end_pos=");
        err_msg.append(std::to_string(end_pos));
        err_msg.append(" size=");
        err_msg.append(std::to_string(cache_size));
        LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
               err_msg.c_str());

        // Read the slice cache data and send it to the relay log queue.
        while (likely(reader_size < cache_size)) {
          const char *event_buf;
          event_buf =
              slice.archived_info.slice_data_cache.c_str() + reader_size;
          Log_event_type type = (Log_event_type)event_buf[EVENT_TYPE_OFFSET];
          uint32 event_len = uint4korr(event_buf + EVENT_LEN_OFFSET);
          // uint32 event_pos = uint4korr(event_buf + LOG_POS_OFFSET);
          DBUG_PRINT(
              "info",
              ("binlog archive replica relay thread readed event of type %s",
               Log_event::get_type_str(type)));

          // Indicates that the first slice of the first binlog file is read,
          // Need send_format_description_event.
          if (first_slice && start_read_pos > BIN_LOG_HEADER_SIZE) {
            if (type == binary_log::FORMAT_DESCRIPTION_EVENT) {
              assert(first_slice);
              LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
                     "queue fake rotate_log_event");
              // If the starting position for reading the binlog file is in the
              // middle. Indicates that the first slice of the first binlog file
              // is read, this is binlog archive replica is running for the
              // first time. If we are skipping the beginning of the binlog file
              // based on the position asked by binlog archive replica
              // "start_read_pos > BIN_LOG_HEADER_SIZE".
              binary_log::enum_binlog_checksum_alg event_checksum_alg =
                  Log_event_footer::get_checksum_alg(event_buf, event_len);
              // We need fake rotate event to set the binlog file name an
              // position, send to the relay log queue before the format
              // description event.
              std::string rotate_buf;
              fake_rotate_event(slice.archived_info.log_file_name,
                                start_read_pos,
                                uint4korr(event_buf + SERVER_ID_OFFSET),
                                event_checksum_alg, rotate_buf);
              // Send fake rotate evet.
              QUEUE_EVENT_RESULT queue_res = queue_event_from_objstore(
                  archive->m_master_info, rotate_buf.c_str(),
                  rotate_buf.length(), true);
              if (queue_res == QUEUE_EVENT_ERROR_QUEUING) {
                LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
                       "queue fake rotate_log_event relay log error");
                error = 1;
                break;
              }

              // We need to fake the format description event to set the
              // checksum algorithm to BINLOG_CHECKSUM_ALG_OFF, send to the
              // relay log queue.
              uchar *event_ptr =
                  const_cast<uchar *>(pointer_cast<const uchar *>(event_buf));
              assert(event_ptr[LOG_POS_OFFSET] > 0);
              event_ptr[FLAGS_OFFSET] &= ~LOG_EVENT_BINLOG_IN_USE_F;
              /*
                If we are skipping the beginning of the binlog file based on the
                position asked by binlog archive replica "start_read_pos >
                BIN_LOG_HEADER_SIZE". Mark that this event with "event_pos=0",
                so the slave should not increment master's binlog position
                (mi->group_master_log_pos)
              */
              int4store(event_ptr + LOG_POS_OFFSET, 0);
              /*
                Set the 'created' field to 0 to avoid destroying
                temp tables on slave.
              */
              int4store(
                  event_ptr + LOG_EVENT_MINIMAL_HEADER_LEN + ST_CREATED_OFFSET,
                  0);
              /* fix the checksum due to latest changes in header */
              if (event_checksum_alg > binary_log::BINLOG_CHECKSUM_ALG_OFF &&
                  event_checksum_alg < binary_log::BINLOG_CHECKSUM_ALG_ENUM_END)
                calc_event_checksum(event_ptr, event_len);
              LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
                     "queue Format_description_log_event ");
              // Send Format_description_log_event
              queue_res = queue_event_from_objstore(archive->m_master_info,
                                                    event_buf, event_len, true);
              if (queue_res == QUEUE_EVENT_ERROR_QUEUING) {
                LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
                       "queue fixed Format_description_log_event error");
                error = 1;
                break;
              }
              // After reading the FORMAT_DESCRIPTION_EVENT of the first slice
              // of the first binlog, is this slice no longer needed? This
              // indicates that this slice is only used to read the
              // FORMAT_DESCRIPTION_EVENT.
              if (start_read_pos >= end_pos) {
                // Skip current slice.
                LogErr(INFORMATION_LEVEL,
                       ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
                       "skip current slice");
                break;
              } else {
                assert(cache_size == end_pos);
                // Jump to the start_read_pos position of the first slice.
                reader_size = start_read_pos;
                LogErr(INFORMATION_LEVEL,
                       ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
                       "jump to start_read_pos");
                continue;
              }
            }
          }

          DBUG_EXECUTE_IF(
              "binlog_archive_replica_crash_before_queue_event_relaylog",
              DBUG_SUICIDE(););
          QUEUE_EVENT_RESULT queue_res = queue_event_from_objstore(
              archive->m_master_info, event_buf, event_len, true);
          if (queue_res == QUEUE_EVENT_ERROR_QUEUING) {
            LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
                   "queue event error");
            error = 1;
            break;
          }
          // If the starting position for reading the binlog file is in the
          // middle, after reading the format_description_log_event, events
          // before the starting position need to be skipped, and events
          // should be read starting from the specified start_read_pos
          // position. This only occurs when reading the first binlog file;
          // subsequent binlog files are read starting from position
          // BIN_LOG_HEADER_SIZE.
          reader_size += event_len;
        }
        if (error) break;
        err_msg.assign("relay position");
        err_msg.append(archive->m_master_info->get_for_channel_str());
        err_msg.append(" file=");
        err_msg.append(archive->m_master_info->get_master_log_name());
        err_msg.append(" pos=");
        err_msg.append(
            std::to_string(archive->m_master_info->get_master_log_pos()));
        // print the current replication position
        LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER,
               err_msg.c_str());
      }
      if (error) break;
    }
  }

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA_RELAY_WORKER, "thread end");

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
 * @brief Creates a binlog archive object and starts the binlog archive
 * thread.
 * @return int 0 if the binlog archive thread was successfully started, 1
 * otherwise.
 */
int start_binlog_archive_replica() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("start_binlog_archive_replica"));

  // Check if the binlog archive replica is enabled.
  if (!opt_binlog_archive_replica || !opt_serverless) {
    return 0;
  }

  // Check if the binlog is enabled.
  if (!opt_bin_log) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, "need enable binlog mode");
    return 0;
  }

  if (!relay_log_recovery) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
           "need enable relay_log_recovery");
    return 0;
  }

  if (!opt_binlog_archive_replica_source_log_file ||
      strlen(opt_binlog_archive_replica_source_log_file) == 0) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
           "binlog archive replica channel need "
           "binlog_archive_replica_source_log_file");
    return 1;
  }

  if (!opt_binlog_archive_dir) {
    opt_binlog_archive_dir = mysql_tmpdir;
  }

  // Check if the mysql archive path is set.
  if (opt_binlog_archive_dir == nullptr) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
           "must set binlog_archive_dir");
    return 1;
  }

  MY_STAT d_stat;
  // Check if opt_binlog_archive_dir exists.
  if (!my_stat(opt_binlog_archive_dir, &d_stat, MYF(0)) ||
      !MY_S_ISDIR(d_stat.st_mode) ||
      my_access(opt_binlog_archive_dir, (F_OK | W_OK))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
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
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
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
    LogErr(WARNING_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
  }
  if (my_mkdir(tmp_archive_binlog_dir, 0777, MYF(0))) {
    std::string err_msg;
    err_msg.assign("error ");
    err_msg.append(std::to_string(my_errno()));
    err_msg.append(", failed to create archive dir ");
    err_msg.append(tmp_archive_binlog_dir);
    LogErr(WARNING_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
  }

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "start binlog archive replica");

  // replica binlog from source object store.
  std::string obj_error_msg;
  std::string err_msg;
  std::string_view endpoint(
      opt_source_objectstore_endpoint
          ? std::string_view(opt_source_objectstore_endpoint)
          : "");
  objstore::ObjectStore *objstore = objstore::create_object_store(
      std::string_view(opt_source_objectstore_provider),
      std::string_view(opt_source_objectstore_region),
      opt_source_objectstore_endpoint ? &endpoint : nullptr,
      opt_source_objectstore_use_https, obj_error_msg);
  if (!objstore) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_CREATE_OBJECT_STORE,
           opt_source_objectstore_provider, opt_source_objectstore_region,
           std::string(endpoint).c_str(), opt_source_objectstore_bucket,
           opt_source_objectstore_use_https ? "true" : "false",
           !obj_error_msg.empty() ? obj_error_msg.c_str() : "");
    return 1;
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_CREATE_OBJECT_STORE,
         opt_source_objectstore_provider, opt_source_objectstore_region,
         std::string(endpoint).c_str(), opt_source_objectstore_bucket,
         opt_source_objectstore_use_https ? "true" : "false", "");
  mysql_binlog_archive_replica.set_objstore(objstore);

  mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
  if (mysql_thread_create(
          THR_KEY_binlog_archive_replica, &mysql_binlog_archive_replica_pthd,
          &connection_attrib, mysql_binlog_archive_replica_action,
          (void *)&mysql_binlog_archive_replica)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_STARTUP,
           "Failed to create binlog archive replica thread");
    mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
    return 1;
  }

  mysql_binlog_archive_replica.thread_set_created();

  while (mysql_binlog_archive_replica.is_thread_alive_not_running()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive replica thread to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_binlog_archive_replica_run_cond,
                         &m_binlog_archive_replica_run_lock, &abstime);
  }
  mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
  return 0;
}

/**
 * @brief stop the binlog archive thread.
 * @note must stop consistent snapshot thread, before stop the binlog archive
 * thread. Consistent snapshot thread depends on the binlog archive thread.
 * @return void
 */
void stop_binlog_archive_replica() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("stop_binlog_archive_replica"));
  if (!opt_binlog_archive_replica || !opt_serverless) return;

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, "terminate begin");

  mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
  if (mysql_binlog_archive_replica.is_thread_dead()) {
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, "terminated");
    mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
    return;
  }
  mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "terminating binlog archive replica");
  mysql_binlog_archive_replica.terminate_binlog_archive_replica_thread();
  /* Wait until the thread is terminated. */
  my_thread_join(&mysql_binlog_archive_replica_pthd, nullptr);
  if (mysql_binlog_archive_replica.get_objstore()) {
    objstore::destroy_object_store(mysql_binlog_archive_replica.get_objstore());
    mysql_binlog_archive_replica.set_objstore(nullptr);
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, "terminate end");
}

/**
 * @brief Archives the MySQL binlog.
 * @param arg The binlog archive object.
 */
static void *mysql_binlog_archive_replica_action(void *arg) {
  Binlog_archive_replica *archive = (Binlog_archive_replica *)arg;
  archive->run();
  return nullptr;
}

/**
 * @brief Constructs a Binlog_archive_replica object.
 */
Binlog_archive_replica::Binlog_archive_replica()
    : m_expected_slice_queue(),
      m_slice_status_map(),
      m_thd(nullptr),
      m_thd_state(),
      binlog_objstore(nullptr),
      m_index_file() {
  m_mysql_archive_dir[0] = '\0';
  m_mysql_binlog_archive_dir[0] = '\0';
  m_binlog_archive_dir[0] = '\0';
  m_index_local_file_name[0] = '\0';
  m_relay_worker = nullptr;
  m_workers = nullptr;
  m_start_binlog_file[0] = '\0';
  m_start_binlog_pos = BIN_LOG_HEADER_SIZE;
}

/**
 * @brief Destructs the Binlog_archive_replica object.
 */
Binlog_archive_replica::~Binlog_archive_replica() {}

/**
 * @brief Initializes the binlog archive object.
 */
void Binlog_archive_replica::init_pthread_object() {
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

  mysql_cond_init(PSI_binlog_archive_replica_cond_key,
                  &m_binlog_archive_replica_run_cond);
  mysql_mutex_init(PSI_binlog_archive_replica_lock_key,
                   &m_binlog_archive_replica_run_lock, MY_MUTEX_INIT_FAST);
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
void Binlog_archive_replica::deinit_pthread_object() {
  mysql_cond_destroy(&m_map_cond);
  mysql_cond_destroy(&m_queue_cond);
  mysql_mutex_destroy(&m_slice_mutex);
  mysql_mutex_destroy(&m_index_lock);
  mysql_mutex_destroy(&m_binlog_archive_replica_run_lock);
  mysql_cond_destroy(&m_binlog_archive_replica_run_cond);
}

/**
 * @brief Returns the binlog archive instance.
 * @return Binlog_archive_replica*
 */
Binlog_archive_replica *Binlog_archive_replica::get_instance() {
  return &mysql_binlog_archive_replica;
}

/**
 * @brief Returns the binlog archive lock.
 *
 * @return mysql_mutex_t*
 */
mysql_mutex_t *Binlog_archive_replica::get_binlog_archive_lock() {
  return &m_binlog_archive_replica_run_lock;
}

/**
 * @brief Terminate the binlog archive thread.
 *
 * @return int
 */
int Binlog_archive_replica::terminate_binlog_archive_replica_thread() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("terminate_binlog_archive_replica_thread"));
  mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive replica thread to stop"));
    if (m_thd_state.is_initialized()) {
      mysql_mutex_lock(&m_thd->LOCK_thd_data);
      m_thd->awake(THD::KILL_CONNECTION);
      mysql_mutex_unlock(&m_thd->LOCK_thd_data);
    }
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_binlog_archive_replica_run_cond,
                         &m_binlog_archive_replica_run_lock, &abstime);
  }
  assert(m_thd_state.is_thread_dead());
  mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
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
 * @brief Runs the binlog archive replica process.
 */
void Binlog_archive_replica::run() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("Binlog_archive_replica::run"));
  std::string err_msg{};
  bool is_send_format_description_event = true;

  m_thd = set_thread_context();

  mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
  m_thd_state.set_initialized();
  mysql_cond_broadcast(&m_binlog_archive_replica_run_cond);
  mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);

  strmake(m_mysql_archive_dir, opt_binlog_archive_dir,
          sizeof(m_mysql_archive_dir) - 1);
  convert_dirname(m_mysql_archive_dir, m_mysql_archive_dir, NullS);
  strmake(strmake(m_mysql_binlog_archive_dir, m_mysql_archive_dir,
                  sizeof(m_mysql_binlog_archive_dir) -
                      BINLOG_ARCHIVE_SUBDIR_LEN - 1),
          STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  // if m_binlog_archive_dir dir not exists, start_binlog_archive() will
  // create it.
  convert_dirname(m_mysql_binlog_archive_dir, m_mysql_binlog_archive_dir,
                  NullS);

  // Binlog archive replica source object directory prefix.
  std::string binlog_objectstore_path(opt_source_objectstore_repo_id);
  binlog_objectstore_path.append(FN_DIRSEP);
  binlog_objectstore_path.append(opt_source_objectstore_branch_id);
  binlog_objectstore_path.append(FN_DIRSEP);
  binlog_objectstore_path.append(BINLOG_ARCHIVE_SUBDIR);
  binlog_objectstore_path.append(FN_DIRSEP);
  strmake(m_binlog_archive_dir, binlog_objectstore_path.c_str(),
          sizeof(m_binlog_archive_dir) - 1);

  mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_binlog_archive_replica_run_cond);
  mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
  err_msg.assign("Binlog archive replica thread running");
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());

  // init binlog archive replica slice queue.
  assert(!m_expected_slice_queue.inited_queue);
  m_expected_slice_queue.avail = 0;
  m_expected_slice_queue.entry = 0;
  m_expected_slice_queue.len = 0;
  m_expected_slice_queue.capacity = 4 * opt_binlog_archive_parallel_workers;
  m_expected_slice_queue.inited_queue = true;
  Binlog_expected_pull_slice slice;
  m_expected_slice_queue.m_Q.resize(m_expected_slice_queue.capacity, slice);
  assert(m_expected_slice_queue.m_Q.size() == m_expected_slice_queue.capacity);
  m_last_expected_file_seq = 0;
  m_last_expected_slice_seq = 0;

  // Create new binlog archive replica channel, if not exists.
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "create binlog archive replica channel");
  if (channel_create()) {
    err_msg.assign("failed to create binlog archive replica channel");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
    goto end;
  }

  if (start_worker_threads()) {
    goto end;
  }
  for (;;) {
    LOG_ARCHIVED_INFO log_info{};
    int error = 0;

    mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
    if (m_thd == nullptr || unlikely(m_thd->killed)) {
      err_msg.assign("killed");
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
      mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
      break;
    }
    mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);

#ifdef WESQL_CLUSTER
    // Check whether consensus role is leader
    uint64_t consensus_term = 0;
    // Check whether consensus role is leader.
    // Only the leader can apply binlog. The nunleader will report failure,
    // when sql apply binlog.
    if (DBUG_EVALUATE_IF("fault_injection_binlog_archive_replica_running", true,
                         false) ||
        !is_consensus_replication_state_leader(consensus_term)) {
      struct timespec abstime;
      set_timespec(&abstime, 1);
      mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
      error =
          mysql_cond_timedwait(&m_binlog_archive_replica_run_cond,
                               &m_binlog_archive_replica_run_lock, &abstime);
      mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
      continue;
    }
#endif

    // If any worker thread is not running, stop all worker threads and
    // reinitialize the binlog archive replica, then restart the worker
    // threads.
    if (!worker_threads_are_running()) {
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
             "restart binlog archive replica worker threads");
      if (stop_worker_threads() || !reinitiate_archive_replica() ||
          start_worker_threads()) {
        goto end;
      }
      is_send_format_description_event = true;
    }
    assert(m_master_info);
    // If start_binlog_file is empty, init start binlog file and position
    // using mi info.
    if (m_start_binlog_file[0] == '\0') {
      assert(m_master_info->get_master_log_name() &&
             m_master_info->get_master_log_name()[0] != '\0');
      strmake(m_start_binlog_file, m_master_info->get_master_log_name(),
              sizeof(m_start_binlog_file) - 1);
      m_start_binlog_pos = m_master_info->get_master_log_pos();
      err_msg.assign("init binlog replica start_binlog_file=");
      err_msg.append(m_start_binlog_file);
      err_msg.append(" star_pos=");
      err_msg.append(std::to_string(m_start_binlog_pos));
      err_msg.append(" from channel master info");
      err_msg.append(m_master_info->get_for_channel_str());
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
    }
    assert(m_start_binlog_file[0] != '\0');
    assert(m_start_binlog_pos > BIN_LOG_HEADER_SIZE);

    mysql_mutex_lock(&m_index_lock);
    // open persistent binlog.index from source object storage.
    // Fetch the latest binlog index from the source object storage.
    // The binlog archive replica thread periodically retrieves the
    // binlog.index from the source object storage to check if there are any
    // incremental binlogs slice that need to be applied.
    if (open_index_file()) {
      mysql_mutex_unlock(&m_index_lock);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
             "failed to open source binlog index file");
      goto end;
    }
    my_off_t last_slice_end_pos;
    bool binlog_updated = false;
    err_msg.assign(
        "Find more binlog slice from persistent binlog index,  "
        "start_binlog=");
    err_msg.append(m_start_binlog_file);
    err_msg.append(" start_pos=");
    err_msg.append(std::to_string(m_start_binlog_pos));
    // Find more binlog slice replication from source persistent
    // binlog.index.
    error = find_log_pos_by_name(&log_info, m_start_binlog_file);
    if (error == 0) {
      bool next_log = false;
      do {
        last_slice_end_pos = log_info.slice_end_pos;
        if (next_log || m_start_binlog_pos < last_slice_end_pos) {
          // Already more binlog slice need replica.
          binlog_updated = true;
          break;
        }
      } while (!(error = find_next_log_slice(&log_info, next_log)));

      if (error == LOG_INFO_IO) {
        close_index_file();
        mysql_mutex_unlock(&m_index_lock);
        err_msg.append(" failed");
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
        goto end;
      }
      // If not found more binlog slice need replica, wait for 1 second
      // retry.
      if (!binlog_updated) {
        if (m_start_binlog_pos == last_slice_end_pos) {
          close_index_file();
          mysql_mutex_unlock(&m_index_lock);
          // If no more binlog slice to pull in source object store, retry
          // after 1 second.
          struct timespec abstime;
          set_timespec(&abstime, opt_binlog_archive_replica_flush_period);
          mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
          error = mysql_cond_timedwait(&m_binlog_archive_replica_run_cond,
                                       &m_binlog_archive_replica_run_lock,
                                       &abstime);
          mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
          continue;
        } else {
          close_index_file();
          mysql_mutex_unlock(&m_index_lock);
          err_msg.append(" failed, binlog index position mismatch ");
          err_msg.append(" expected start_pos=");
          err_msg.append(std::to_string(m_start_binlog_pos));
          err_msg.append(", but the last slice position=");
          err_msg.append(std::to_string(last_slice_end_pos));
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
          goto end;
        }
      }
    } else {
      close_index_file();
      mysql_mutex_unlock(&m_index_lock);
      err_msg.append(", not found replica start binlog ");
      err_msg.append(
          ", maybe the binlog is purged. Need to clone database "
          "from source object store");
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
      goto end;
    }

    // Find the first slice of start binlog file from the source binlog index.
    if (!(error = find_log_pos_by_name(&log_info, m_start_binlog_file))) {
      // When startup on the first time and create new channel, need read
      // format_description_event from the the first slice of
      // start_binlog_file. So we need to add the first slice in the first
      // binlog file to the expected slice queue. Regardless of whether
      // m_start_binlog_pos > en_pos. Refer to rpl_binlog_sender.cc.
      if (is_send_format_description_event) {
        err_msg.assign("read expected the first slice of start binlog ");
        err_msg.append(m_start_binlog_file);
        err_msg.append(" start_pos=");
        err_msg.append(std::to_string(m_start_binlog_pos));
        err_msg.append(" slice_end_pos=");
        err_msg.append(std::to_string(log_info.slice_end_pos));
        LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
        if (!add_slice(log_info, true, m_start_binlog_pos)) {
          close_index_file();
          mysql_mutex_unlock(&m_index_lock);
          goto end;
        }
        is_send_format_description_event = false;
      }
      bool next_log = false;
      // Read next binlig slice of current binlog file or next new binlog
      // file.
      while (!(error = find_next_log_slice(&log_info, next_log))) {
        // Skip binlog slice if the end position of the current binlog file is
        // less equal than the start position. This indicates that the binlog
        // slice already is applied.
        if (!next_log && log_info.slice_end_pos <= m_start_binlog_pos) {
          continue;
        }
        // Add next binlog slice or the first slice of next binlog file.
        if (next_log) {
          strmake(m_start_binlog_file, log_info.log_file_name,
                  sizeof(m_start_binlog_file) - 1);
          if (!add_slice(log_info, next_log, BIN_LOG_HEADER_SIZE)) {
            close_index_file();
            mysql_mutex_unlock(&m_index_lock);
            goto end;
          }
        } else {
          if (!add_slice(log_info, next_log, m_start_binlog_pos)) {
            close_index_file();
            mysql_mutex_unlock(&m_index_lock);
            goto end;
          }
        }
        // Set start_read_pos of the next binlog slice.
        // The end position of the binlog slice, and also the start position
        // of the next binlog slice.
        m_start_binlog_pos = log_info.slice_end_pos;
        LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
      }
    } else {
      // If start_binlog is not empty, but not found in the source binlog
      // index, maybe the binlog is purged, need to init database by clone
      // from source object store.
      if (error == LOG_INFO_EOF && m_start_binlog_file[0] != '\0') {
        err_msg.assign("not found replica start binlog ");
        err_msg.append(m_start_binlog_file);
        err_msg.append(
            ", maybe the binlog is purged. Need to clone database "
            "from source object store");
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
        goto end;
      }
    }
    close_index_file();
    mysql_mutex_unlock(&m_index_lock);
    if (error == LOG_INFO_IO) {
      err_msg.assign("failed to read binlog index file");
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
      goto end;
    }
  }

end:
  stop_worker_threads();
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "binlog archive replica thread end");
  m_thd->clear_error();
  m_thd->release_resources();
  mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
  delete m_thd;
  m_thd = nullptr;
  m_thd_state.set_terminated();
  mysql_cond_broadcast(&m_binlog_archive_replica_run_cond);
  mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
  my_thread_end();
  my_thread_exit(nullptr);
}

int Binlog_archive_replica::start_worker_threads() {
  int error = 0;

  DBUG_SIGNAL_WAIT_FOR(m_thd, "pause_before_start_worker_threads",
                       "reached_pause_on_start_worker_threads",
                       "continue_start_worker_threads");
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "create binlog archive replica applier sql thread");
  // Startup sql applier thread.
  if ((error = channel_start())) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
           "failed to start binlog archive replica applier sql thread");
    goto err;
  }

  // create binlog archive replica relay worker.
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "create binlog archive replica relay thread");
  m_relay_worker = new Binlog_archive_replica_relay_worker(this);
  if (!m_relay_worker || !m_relay_worker->start()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
           "failed to start binlog archive replica relay thread");
    error = 1;
    goto err;
  }

  // create binlog archive replica io worker thread.
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "create binlog archive replica io thread");
  m_workers = static_cast<Binlog_archive_replica_io_worker **>(
      my_malloc(PSI_NOT_INSTRUMENTED,
                opt_binlog_archive_parallel_workers *
                    sizeof(Binlog_archive_replica_io_worker *),
                MYF(MY_WME)));
  if (!m_workers) {
    error = 1;
    goto err;
  }
  for (ulong i = 0; i < opt_binlog_archive_parallel_workers; ++i) {
    Binlog_archive_replica_io_worker *worker =
        new Binlog_archive_replica_io_worker(this, i);
    m_workers[i] = worker;
    if (!worker || !worker->start()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
             "failed to start binlog archive replica io thread");
      error = 1;
      goto err;
    }
  }
err:
  return error;
}

int Binlog_archive_replica::stop_worker_threads() {
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "stopping binlog archive replica relay thread");
  if (m_relay_worker) {
    delete m_relay_worker;
    m_relay_worker = nullptr;
  }
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "stopping binlog archive replica io thread");
  if (m_workers) {
    for (ulong i = 0; i < opt_binlog_archive_parallel_workers; ++i) {
      Binlog_archive_replica_io_worker *worker = m_workers[i];
      delete worker;
    }

    my_free(m_workers);
    m_workers = nullptr;
  }

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
         "stopping binlog archive replica apply sql thread");
  channel_stop();
  return 0;
}

bool Binlog_archive_replica::worker_threads_are_running() {
  if (!channel_is_running()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
           "binlog archive replica apply sql thread is not running");
    return false;
  }
  if (!m_relay_worker || m_relay_worker->is_thread_dead() || !m_workers) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
           "binlog archive replica relay thread is not running");
    return false;
  }
  for (ulong i = 0; i < opt_binlog_archive_parallel_workers; ++i) {
    if (!m_workers[i] || m_workers[i]->is_thread_dead()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
             "binlog archive replica io thread is not running");
      return false;
    }
  }
  return true;
}

bool Binlog_archive_replica::reinitiate_archive_replica() {
  mysql_mutex_lock(&m_slice_mutex);
  assert(m_expected_slice_queue.inited_queue == true);
  m_expected_slice_queue.m_Q.clear();
  m_expected_slice_queue.avail = 0;
  m_expected_slice_queue.entry = 0;
  m_expected_slice_queue.len = 0;
  Binlog_expected_pull_slice slice;
  m_expected_slice_queue.m_Q.resize(m_expected_slice_queue.capacity, slice);
  assert(m_expected_slice_queue.m_Q.size() == m_expected_slice_queue.capacity);
  assert(m_expected_slice_queue.empty());
  m_slice_status_map.clear();
  assert(m_slice_status_map.empty());
  m_last_expected_file_seq = 0;
  m_last_expected_slice_seq = 0;
  mysql_mutex_unlock(&m_slice_mutex);
  m_start_binlog_file[0] = '\0';
  m_start_binlog_pos = BIN_LOG_HEADER_SIZE;
  return true;
}
/**
 * @brief Add a slice to the expected slice queue for applying.
 *
 * @param slice the slice to be added to the expected slice queue for
 * applying.
 * @param first_slice whether the slice is the first slice in the binlog file.
 * @param start_read_pos the start position for reading the binlog file slice.
 */
bool Binlog_archive_replica::add_slice(LOG_ARCHIVED_INFO &log_info,
                                       bool first_slice,
                                       my_off_t start_read_pos) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("add_slice"));
  std::string err_msg;
  std::string binlog_slice_key;
  binlog_slice_key.assign(m_binlog_archive_dir);
  binlog_slice_key.append(log_info.log_slice_name);
  // write slice key to m_expected_slice_queue
  Binlog_expected_pull_slice expected_slice(binlog_slice_key.c_str(),
                                            m_last_expected_file_seq,
                                            m_last_expected_slice_seq++);
  SLICE_PULL_INFO slice_info;
  strmake(slice_info.log_file_name, log_info.log_file_name,
          sizeof(slice_info.log_file_name) - 1);
  strmake(slice_info.log_slice_name, binlog_slice_key.c_str(),
          sizeof(slice_info.log_slice_name) - 1);
  slice_info.log_slice_end_consensus_index = log_info.slice_end_consensus_index;
  slice_info.log_slice_previous_consensus_index =
      log_info.log_previous_consensus_index;
  slice_info.log_slice_consensus_term = log_info.slice_consensus_term;
  slice_info.log_slice_end_pos = log_info.slice_end_pos;
  slice_info.first_slice = first_slice;
  slice_info.m_start_read_pos = start_read_pos;

  err_msg.assign("add ");
  err_msg.append(first_slice ? "first slice" : "slice");
  err_msg.append(" to expected slice queue ");
  err_msg.append(" log_slice_keyid=");
  err_msg.append(slice_info.log_slice_name);
  err_msg.append(" end_pos=");
  err_msg.append(std::to_string(slice_info.log_slice_end_pos));
  err_msg.append(" start_read_pos=");
  err_msg.append(std::to_string(slice_info.m_start_read_pos));
  err_msg.append(" file_seq=");
  err_msg.append(std::to_string(expected_slice.m_file_seq));
  err_msg.append(" slice_seq=");
  err_msg.append(std::to_string(expected_slice.m_slice_seq));

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());

  mysql_mutex_lock(&m_slice_mutex);
  m_thd->ENTER_COND(&m_queue_cond, &m_slice_mutex, nullptr, nullptr);
  while (m_expected_slice_queue.full() && !m_thd->killed) {
    // Wait for signal from the binlog archive worker thread that the slice
    // is persisted and dequeued from the expected slice queue.
    // Or binlog archive thread is killed.
    mysql_cond_wait(&m_queue_cond, &m_slice_mutex);
  }
  if (m_thd->killed) {
    mysql_mutex_unlock(&m_slice_mutex);
    m_thd->EXIT_COND(nullptr);
    LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, "thread killed");
    return false;
  }
  m_expected_slice_queue.en_queue(&expected_slice);
  m_slice_status_map[expected_slice.m_file_seq][expected_slice.m_slice_seq] =
      Slice_pull_status(expected_slice.m_file_seq, expected_slice.m_slice_seq,
                        slice_info);
  mysql_mutex_unlock(&m_slice_mutex);
  m_thd->EXIT_COND(nullptr);
  // Signal the binlog archive worker thread that wait, because the slice
  // queue is empty.
  mysql_cond_signal(&m_queue_cond);
  return true;
}

/**
 * @brief Fetch persistent objects from the object store by the search key.
 *
 * @param persistent_objects result of the fetched objects.
 * @param search_key search key.
 * @param all whether to fetch all objects, otherwise fetch one page.
 * @param allow_no_search_key whether to allow no search key.
 * @return true if the fetch is successful; otherwise, false.
 */
bool Binlog_archive_replica::list_persistent_objects(
    std::vector<objstore::ObjectMeta> &persistent_objects,
    const char *search_key, bool all, bool allow_no_search_key) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("list_persistent_objects"));
  bool finished = false;
  std::string start_after;
  std::string binlog_index_prefix;
  binlog_index_prefix.assign(m_binlog_archive_dir);
  binlog_index_prefix.append(search_key);
  do {
    std::vector<objstore::ObjectMeta> tmp_objects;
    objstore::Status ss = binlog_objstore->list_object(
        std::string_view(opt_source_objectstore_bucket), binlog_index_prefix,
        false, start_after, finished, tmp_objects);
    if (!ss.is_succ()) {
      if (allow_no_search_key &&
          ss.error_code() == objstore::Errors::SE_NO_SUCH_KEY) {
        return true;
      }
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LIST_OBJECT, "failed",
             binlog_index_prefix.c_str(),
             std::string(ss.error_message()).c_str());
      return false;
    }
    persistent_objects.insert(persistent_objects.end(), tmp_objects.begin(),
                              tmp_objects.end());
  } while (all == true && finished == false);
  // sort the objects by key in lexicographical order.
  std::sort(persistent_objects.begin(), persistent_objects.end(),
            [](const objstore::ObjectMeta &a, const objstore::ObjectMeta &b) {
              return a.key < b.key;
            });
  if (allow_no_search_key == false && persistent_objects.empty()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LIST_OBJECT, "failed",
           binlog_index_prefix.c_str(), "no persistent objects found");
    return false;
  }
  DBUG_PRINT("info", ("list binlog index  %s", binlog_index_prefix.c_str()));
  return true;
}

/**
 * @brief Get the last persistent binlog index file.
 * The persisted `binlog-index.index` is based on multiple versions of the
 * consensus term, so the `binlog-index.index` with the highest consensus term
 * should be obtained as the initial one.
 * @param last_binlog_index
 * @return int
 */
int Binlog_archive_replica::fetch_last_persistent_index_file(
    std::string &last_binlog_index) {
  std::vector<objstore::ObjectMeta> objects;

  if (!list_persistent_objects(objects, BINLOG_ARCHIVE_INDEX_FILE_BASENAME,
                               true, false)) {
    return 1;
  }
  // if no persistent binlog.index, return.
  if (objects.empty()) {
    DBUG_PRINT("info", ("no persistent binlog index file %s",
                        BINLOG_ARCHIVE_INDEX_FILE_BASENAME));
    return 0;
  }
  last_binlog_index.assign((objects.back()).key);
  DBUG_PRINT("info", ("the last persistent binlog index is %s",
                      last_binlog_index.c_str()));
  return 0;
}

/**
 * @brief  Open the binlog archive index file.
 *
 * @note  Mutex be called with m_index_lock held.
 * @return true, if error
 * @return false, if success
 */
bool Binlog_archive_replica::open_index_file() {
  bool error = false;
  File index_file_nr = -1;
  std::string last_binlog_index_keyid{};

  /*
    First open of this class instance
    Create an index file that will hold all file names uses for logging.
    Add new entries to the end of it.
  */
  myf opt = MY_UNPACK_FILENAME | MY_REPLACE_DIR;

  if (my_b_inited(&m_index_file)) goto end;

  fn_format(m_index_local_file_name, BINLOG_ARCHIVE_REPLICA_INDEX_LOCAL_FILE,
            m_mysql_binlog_archive_dir, ".index", opt);

  // Check if the index file exists in s3, if so, download it to local.
  // Problem: If a new index file is generated while retrieving the last index
  // file through list_object(), the slice index obtained from reading the
  // index may not be the one that actually needs to be read. Instead, it
  // could be a slice produced by the old leader, which might conflict with
  // slices generated by the new leader. If events are relayed in this
  // scenario, it could lead to data inconsistency.
  // Solution: After obtaining the latest index through get_object(), perform
  // another list_object() call to fetch the latest index again and check if
  // it matches the index already retrieved. If they are consistent, the
  // slices within the index can be used (ensuring the currently read index is
  // the latest file and its slices are valid). Otherwise, the index needs to
  // be re-read.
  while (true) {
    if (fetch_last_persistent_index_file(last_binlog_index_keyid)) {
      return true;
    }
    assert(!last_binlog_index_keyid.empty());
    auto status = binlog_objstore->get_object_to_file(
        std::string_view(opt_source_objectstore_bucket),
        last_binlog_index_keyid, std::string_view(m_index_local_file_name));
    if (!status.is_succ()) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_GET_OBJECT_TO_FILE,
             "get last persistent binlog-index.index failed",
             last_binlog_index_keyid.c_str(), m_index_local_file_name,
             std::string(status.error_message()).c_str());
      return true;
    }
    std::string last_binlog_index_keyid_retry{};
    if (fetch_last_persistent_index_file(last_binlog_index_keyid_retry)) {
      return true;
    }
    if (last_binlog_index_keyid.compare(last_binlog_index_keyid_retry) == 0) {
      break;
    }
    mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
    if (m_thd == nullptr || unlikely(m_thd->killed)) {
      std::string err_msg;
      err_msg.assign("run exit, persist end consensus index=");
      LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_REPLICA, err_msg.c_str());
      mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
      return true;
    }
    mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_mutex_lock(&m_binlog_archive_replica_run_lock);
    error = mysql_cond_timedwait(&m_binlog_archive_replica_run_cond,
                                 &m_binlog_archive_replica_run_lock, &abstime);
    mysql_mutex_unlock(&m_binlog_archive_replica_run_lock);
  }
  if (my_chmod(m_index_local_file_name,
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE | OTHERS_READ,
               MYF(0))) {
    std::string err_msg;
    err_msg.assign("Failed to chmod: ");
    err_msg.append(m_index_local_file_name);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX, err_msg.c_str());
    error = true;
    goto end;
  }
  DBUG_PRINT("info",
             ("get last persistent binlog-index.index %s %s",
              last_binlog_index_keyid.c_str(), m_index_local_file_name));

  if ((index_file_nr = mysql_file_open(PSI_binlog_archive_log_index_key,
                                       m_index_local_file_name,
                                       O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      mysql_file_sync(index_file_nr, MYF(MY_WME)) ||
      init_io_cache_ext(&m_index_file, index_file_nr, IO_SIZE, READ_CACHE,
                        mysql_file_seek(index_file_nr, 0L, MY_SEEK_END, MYF(0)),
                        false, MYF(MY_WME | MY_WAIT_IF_FULL),
                        PSI_binlog_archive_log_index_cache_key)) {
    if (index_file_nr >= 0) {
      mysql_file_close(index_file_nr, MYF(0));
    }
    error = true;
    goto end;
  }

end:
  return error;
}

/**
 * @brief Closes the binlog archive index file.
 * @note
 *  Mutex needed because we need to make sure the file pointer does not move
    from under our feet
 * @return int
 */
void Binlog_archive_replica::close_index_file() {
  DBUG_TRACE;
  if (my_b_inited(&m_index_file)) {
    end_io_cache(&m_index_file);
    if (mysql_file_close(m_index_file.file, MYF(0)))
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
             "Failed to close binlog index file");
  }
}

/**
  Find the position in the log-index-file for the given log name.
*/
int Binlog_archive_replica::find_log_pos_common(
    IO_CACHE *index_file, LOG_ARCHIVED_INFO *linfo, const char *log_name,
    uint64_t consensus_index, bool last_slice [[maybe_unused]]) {
  DBUG_TRACE;
  int error = 0;

  DBUG_EXECUTE_IF("simulate_find_log_error_in_source_binlog_index", {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_REPLICA,
           "simulate_find_log_error_in_source_binlog_index");
    return LOG_INFO_IO;
  });

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

int Binlog_archive_replica::find_log_pos_by_name(LOG_ARCHIVED_INFO *linfo,
                                                 const char *log_name) {
  DBUG_TRACE;

  if (!my_b_inited(&m_index_file)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
           "binlog index not inited");
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
int Binlog_archive_replica::find_next_log(LOG_ARCHIVED_INFO *linfo) {
  DBUG_TRACE;
  bool next_log = false;

  if (!my_b_inited(&m_index_file)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
           "binlog index not inited");
    return LOG_INFO_IO;
  }
  return find_next_log_common(&m_index_file, linfo, next_log, false);
}

/**
 * @brief
 *
 * @param index_file
 * @param linfo
 * @param found_slice
 * @param next_log true if the next log file is found.
 * @return int
 */
int Binlog_archive_replica::find_next_log_common(IO_CACHE *index_file,
                                                 LOG_ARCHIVED_INFO *linfo,
                                                 bool &next_log,
                                                 bool found_slice) {
  DBUG_TRACE;
  int error = 0;
  size_t length = 0;
  std::string previous_log_name;
  previous_log_name.assign(linfo->log_file_name);
  next_log = false;

  if (!my_b_inited(index_file)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_OPEN_INDEX,
           "binlog index not inited");
    return LOG_INFO_IO;
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
    std::string log_name = found_log_slice_name.substr(0, second_dot);
    strmake(linfo->log_file_name, log_name.c_str(),
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
    std::string end_pos = left_string.substr(third_dot + 1);
    linfo->slice_consensus_term = std::stoull(term);
    linfo->slice_end_pos = std::stoull(end_pos);

    linfo->entry_index++;
    linfo->index_file_offset = my_b_tell(index_file);
    // Found the next log file.
    if (compare_log_name(log_name.c_str(), previous_log_name.c_str()) != 0) {
      next_log = true;
    }
    // Find next binlog or next slice
    if (found_slice || next_log == true) {
      break;
    }
  }

  return error;
}

int Binlog_archive_replica::find_next_log_slice(LOG_ARCHIVED_INFO *linfo,
                                                bool &next_log) {
  DBUG_TRACE;
  return find_next_log_common(&m_index_file, linfo, next_log, true);
}

static int compare_log_name(const char *log_1, const char *log_2) {
  const char *log_1_basename = log_1 + dirname_length(log_1);
  const char *log_2_basename = log_2 + dirname_length(log_2);

  return strcmp(log_1_basename, log_2_basename);
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
