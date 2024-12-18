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

#ifndef BINLOG_ARCHIVE_REPLICA_INCLUDED
#define BINLOG_ARCHIVE_REPLICA_INCLUDED

#include "objstore.h"
#include "sql/basic_istream.h"
#include "sql/basic_ostream.h"
#include "sql/binlog.h"
#include "sql/binlog_archive.h"
#include "sql/binlog_reader.h"
#include "sql/rpl_io_monitor.h"
#include "sql/rpl_rli_pdb.h"
#include "sql/sql_error.h"
#include "sql_string.h"

#define BINLOG_ARCHIVE_REPLICA_INDEX_LOCAL_FILE "binlog_index_relay.index"
#define BINLOG_ARCHIVE_REPLICA_CHANNEL_NAME "binlog_archive_replica"

struct SLICE_PULL_INFO {
  char log_file_name[FN_REFLEN] = {0};
  char log_slice_name[FN_REFLEN] = {0};
  char mysql_log_name[FN_REFLEN] = {0};
  uint64_t log_slice_end_consensus_index;
  uint64_t log_slice_consensus_term;
  uint64_t log_slice_previous_consensus_index;
  my_off_t log_slice_end_pos;
  my_off_t mysql_end_pos;
  int entry_index;  // used in purge_logs(), calculated in find_log_pos().
  my_off_t m_slice_bytes_written;
  std::string slice_data_cache;
  bool first_slice;
  my_off_t m_start_read_pos;
  SLICE_PULL_INFO()
      : log_slice_end_consensus_index(0),
        log_slice_consensus_term(0),
        log_slice_previous_consensus_index(0),
        log_slice_end_pos(0),
        mysql_end_pos(0),
        entry_index(0),
        m_slice_bytes_written(0),
        slice_data_cache(),
        first_slice(false),
        m_start_read_pos(0) {
    memset(log_file_name, 0, FN_REFLEN);
    memset(log_slice_name, 0, FN_REFLEN);
    memset(mysql_log_name, 0, FN_REFLEN);
  }

  // Copy constructor
  SLICE_PULL_INFO(const SLICE_PULL_INFO &other)
      : log_slice_end_consensus_index(other.log_slice_end_consensus_index),
        log_slice_consensus_term(other.log_slice_consensus_term),
        log_slice_previous_consensus_index(
            other.log_slice_previous_consensus_index),
        log_slice_end_pos(other.log_slice_end_pos),
        mysql_end_pos(other.mysql_end_pos),
        entry_index(other.entry_index),
        m_slice_bytes_written(other.m_slice_bytes_written),
        slice_data_cache(other.slice_data_cache),
        first_slice(other.first_slice),
        m_start_read_pos(other.m_start_read_pos) {
    strmake(log_file_name, other.log_file_name, FN_REFLEN);
    strmake(log_slice_name, other.log_slice_name, FN_REFLEN);
    strmake(mysql_log_name, other.mysql_log_name, FN_REFLEN);
  }

  SLICE_PULL_INFO &operator=(const SLICE_PULL_INFO &other) {
    if (this != &other) {
      strmake(log_file_name, other.log_file_name, FN_REFLEN);
      strmake(log_slice_name, other.log_slice_name, FN_REFLEN);
      strmake(mysql_log_name, other.mysql_log_name, FN_REFLEN);
      log_slice_end_consensus_index = other.log_slice_end_consensus_index;
      log_slice_consensus_term = other.log_slice_consensus_term;
      log_slice_previous_consensus_index =
          other.log_slice_previous_consensus_index;
      log_slice_end_pos = other.log_slice_end_pos;
      mysql_end_pos = other.mysql_end_pos;
      entry_index = other.entry_index;
      slice_data_cache = other.slice_data_cache;
      m_slice_bytes_written = other.m_slice_bytes_written;
      first_slice = other.first_slice;
      m_start_read_pos = other.m_start_read_pos;
    }
    return *this;
  }
};

/**
 * @brief  Binlog expected slice structure.
 *
 */
struct Binlog_expected_pull_slice {
  char m_slice_keyid[FN_REFLEN + 1];
  uint32_t m_file_seq;
  uint32_t m_slice_seq;
  // Default constructor
  Binlog_expected_pull_slice() = default;
  // Constructor
  Binlog_expected_pull_slice(const char *keyid, uint32_t file_seq,
                             uint32_t slice_seq)
      : m_file_seq(file_seq), m_slice_seq(slice_seq) {
    strmake(this->m_slice_keyid, keyid, FN_REFLEN);
  }

  // Copy constructor
  Binlog_expected_pull_slice(const Binlog_expected_pull_slice &other)
      : m_file_seq(other.m_file_seq), m_slice_seq(other.m_slice_seq) {
    strmake(m_slice_keyid, other.m_slice_keyid, FN_REFLEN);
  }

  Binlog_expected_pull_slice &operator=(
      const Binlog_expected_pull_slice &other) {
    if (this != &other) {
      strmake(this->m_slice_keyid, other.m_slice_keyid, FN_REFLEN);
      this->m_file_seq = other.m_file_seq;
      this->m_slice_seq = other.m_slice_seq;
    }
    return *this;
  }
};

enum Slice_pull_status_enum {
  SLICE_NOT_PULL,
  SLICE_PULL_SUCCESS,
  SLICE_PULL_FAILED
};
struct Slice_pull_status {
  Slice_pull_status_enum persisted;
  SLICE_PULL_INFO archived_info;
  uint32_t file_seq;
  uint32_t slice_seq;
  Slice_pull_status() : persisted(SLICE_NOT_PULL), file_seq(0), slice_seq(0) {}

  Slice_pull_status(uint32_t f_seq, uint32_t s_seq, SLICE_PULL_INFO info)
      : persisted(SLICE_NOT_PULL),
        archived_info(info),
        file_seq(f_seq),
        slice_seq(s_seq) {}
};

class Binlog_archive_replica;

/**
 * @brief Binlog archive worker class.
 * IO worker pulls the binlog slice from source object store, when binlog index
 * is updated. Multiple IO workers can be created to pull the binlog slice in
 * parallel.
 */
class Binlog_archive_replica_io_worker {
 public:
  Binlog_archive_replica_io_worker(Binlog_archive_replica *archive,
                                   int worker_id);
  ~Binlog_archive_replica_io_worker();

  bool start();
  void stop();
  static void *thread_launcher(void *arg) {
    return static_cast<Binlog_archive_replica_io_worker *>(arg)
        ->worker_thread();
  }
  void *worker_thread();
  bool is_thread_dead() {
    mysql_mutex_lock(&m_worker_run_lock);
    bool ret = m_thd_state.is_thread_dead();
    mysql_mutex_unlock(&m_worker_run_lock);
    return ret;
  }
  bool is_thread_alive() {
    mysql_mutex_lock(&m_worker_run_lock);
    bool ret = m_thd_state.is_thread_alive();
    mysql_mutex_unlock(&m_worker_run_lock);
    return ret;
  }
  bool is_thread_running() {
    mysql_mutex_lock(&m_worker_run_lock);
    bool ret = m_thd_state.is_running();
    mysql_mutex_unlock(&m_worker_run_lock);
    return ret;
  }
  int terminate_binlog_archive_replica_io_worker_thread();
  bool is_binlog_archive_replica_io_worker_waiting() const {
    return atomic_binlog_archive_replica_io_worker_waiting.load(
        std::memory_order_acquire);
  }
  void set_binlog_archive_replica_io_worker_waiting(bool waiting) {
    atomic_binlog_archive_replica_io_worker_waiting.store(
        waiting, std::memory_order_release);
  }

 private:
  Binlog_archive_replica *m_archive;
  my_thread_handle m_thread;
  int m_worker_id;
  THD *m_thd;
  /* thread state */
  thread_state m_thd_state;
  mysql_mutex_t m_worker_run_lock;
  mysql_cond_t m_worker_run_cond;
  std::atomic<bool> atomic_binlog_archive_replica_io_worker_waiting;
};

/**
 * @brief Binlog archive replica fetch index worker class.
 * Index worker fetches the binlog index from source object store periodically.
 * When the index is updated, it triggers the IO worker to pull the binlog
 * slice.
 */
class Binlog_archive_replica_relay_worker {
 public:
  Binlog_archive_replica_relay_worker(Binlog_archive_replica *archive);
  ~Binlog_archive_replica_relay_worker();

  bool start();
  void stop();
  static void *thread_launcher(void *arg) {
    return static_cast<Binlog_archive_replica_relay_worker *>(arg)
        ->worker_thread();
  }
  void *worker_thread();
  bool is_thread_dead() {
    mysql_mutex_lock(&m_worker_run_lock);
    bool ret = m_thd_state.is_thread_dead();
    mysql_mutex_unlock(&m_worker_run_lock);
    return ret;
  }
  bool is_thread_alive() {
    mysql_mutex_lock(&m_worker_run_lock);
    bool ret = m_thd_state.is_thread_alive();
    mysql_mutex_unlock(&m_worker_run_lock);
    return ret;
  }
  bool is_thread_running() {
    mysql_mutex_lock(&m_worker_run_lock);
    bool ret = m_thd_state.is_running();
    mysql_mutex_unlock(&m_worker_run_lock);
    return ret;
  }
  int terminate_binlog_archive_replica_relay_worker();

 private:
  Binlog_archive_replica *m_archive;
  my_thread_handle m_thread;
  THD *m_thd;
  /* thread state */
  thread_state m_thd_state;
  mysql_mutex_t m_worker_run_lock;
  mysql_cond_t m_worker_run_cond;
  void calc_event_checksum(uchar *event_ptr, size_t event_len);
  int fake_rotate_event(const char *next_log_file, my_off_t log_pos,
                        ulong server_id,
                        binary_log::enum_binlog_checksum_alg event_checksum_alg,
                        std::string &rotate_buf);
};

/**
 * @class Binlog_archive_replica
 * @brief The Binlog_archive_replica class is responsible for generate binary
 * relay log files.
 * Binlog_archive_replica is responsible for create index worker and IO worker.
 * Binlog_archive_replicat create replica source object store information like
 'Master_info'.
 * Add binlog_archive_replica channel by Multisource_info::add_mi() and
 * Rpl_info_factory::create_mi_and_rli_objects().
 * Refer to Replication_thread_api::initialize_channel().
 *
 * if (threads_to_start & CHANNEL_APPLIER_THREAD) {
    thread_mask |= SLAVE_SQL;
  }
  if (threads_to_start & CHANNEL_RECEIVER_THREAD) {
    thread_mask |= SLAVE_IO;
  }
 * Refer to start_slave_thread().
 * Binlog_archive_replica call read_event() to read event from binlog slice file
 cache,
 * then call queue_event() to queue event to relay log queue.
 * Index worker is responsible for fetch binlog index from source object store.
 * IO worker is responsible for fetch binlog slice from source object store.
 * Binlog_archive_replica is responsible for generate binary relay log files by
 reading
 *  event from binlog slice file.
 * Before index worker fetch binlog index, it will check object store uuid.
 */
class Binlog_archive_replica {
 public:
  /**
   * @brief Constructs a Binlog_archive_replica object.
   * @param thd A pointer to the THD object.
   */
  Binlog_archive_replica();

  /**
   * @brief Destructs the Binlog_archive_replica object.
   */
  ~Binlog_archive_replica();

  /**
   * @brief Initializes.
   */
  void init_pthread_object();
  /**
   * @brief Cleans up any resources.
   */
  void deinit_pthread_object();

  mysql_mutex_t *get_binlog_archive_lock();

  static Binlog_archive_replica *get_instance();
  /**
   * @brief Runs the binlog archiving process.
   */
  void run();
  void thread_set_created() { m_thd_state.set_created(); }
  bool is_thread_alive_not_running() const {
    return m_thd_state.is_alive_not_running();
  }
  bool is_thread_dead() const { return m_thd_state.is_thread_dead(); }
  bool is_thread_alive() const { return m_thd_state.is_thread_alive(); }
  bool is_thread_running() const { return m_thd_state.is_running(); }
  int terminate_binlog_archive_replica_thread();
  void lock_binlog_index() { mysql_mutex_lock(&m_index_lock); }
  void unlock_binlog_index() { mysql_mutex_unlock(&m_index_lock); }
  inline void set_objstore(objstore::ObjectStore *objstore) {
    binlog_objstore = objstore;
  }
  inline objstore::ObjectStore *get_objstore() { return binlog_objstore; }

  Binlog_archive_replica_relay_worker *m_relay_worker;
  Binlog_archive_replica_io_worker **m_workers;

  mysql_mutex_t m_slice_mutex;
  circular_buffer_queue<Binlog_expected_pull_slice> m_expected_slice_queue;
  mysql_cond_t m_queue_cond;
  // File and slice tracking
  std::map<uint32_t, std::map<uint32_t, Slice_pull_status>> m_slice_status_map;
  mysql_cond_t m_map_cond;
  // -- m_slice_mutex protects the above variables
  int32_t m_last_expected_file_seq{-1};  // last added slice file expected queue
  int32_t m_last_expected_slice_seq{-1};  // last added slice seq expected queue
  Master_info *m_master_info;
  char m_start_binlog_file[FN_REFLEN + 1];
  my_off_t m_start_binlog_pos;

 private:
  // the binlog archive replica THD handle.
  THD *m_thd;
  /* thread state */
  thread_state m_thd_state;
  bool add_slice(LOG_ARCHIVED_INFO &log_info, bool first_slice,
                 my_off_t start_read_pos);
  int channel_create();
  int channel_start();
  int channel_stop();
  bool channel_is_running();
  int start_worker_threads();
  int stop_worker_threads();
  bool worker_threads_are_running();
  bool reinitiate_archive_replica();
  char m_binlog_archive_file_name[FN_REFLEN + 1];
  char m_binlog_archive_dir[FN_REFLEN + 1];
  char m_mysql_archive_dir[FN_REFLEN + 1];
  char m_mysql_binlog_archive_dir[FN_REFLEN + 1];
  objstore::ObjectStore *binlog_objstore;
  int archive_init();
  int archive_cleanup();
  /* The mysql binlog file it is reading */
  LOG_INFO m_mysql_linfo;

  IO_CACHE m_index_file;
  mysql_mutex_t m_index_lock;
  char m_index_local_file_name[FN_REFLEN];
  char m_index_file_name[FN_REFLEN];
  bool list_persistent_objects(
      std::vector<objstore::ObjectMeta> &persistent_objects,
      const char *search_key, bool all, bool allow_no_search_key);
  int fetch_last_persistent_index_file(std::string &last_index_file);
  bool open_index_file();
  void close_index_file();
  int find_log_pos_by_name(LOG_ARCHIVED_INFO *linfo, const char *log_name);
  int find_next_log(LOG_ARCHIVED_INFO *linfo);
  int find_next_log_slice(LOG_ARCHIVED_INFO *linfo, bool &next_log);
  int find_next_log_common(IO_CACHE *index_file, LOG_ARCHIVED_INFO *linfo,
                           bool &next_log, bool found_slice = false);
  int find_log_pos_common(IO_CACHE *index_file, LOG_ARCHIVED_INFO *linfo,
                          const char *log_name, uint64_t consensus_index,
                          bool last_slice = false);
};

extern int start_binlog_archive_replica();
extern void stop_binlog_archive_replica();
#endif
