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

#ifndef BINLOG_ARCHIVE_INCLUDED
#define BINLOG_ARCHIVE_INCLUDED

#include "objstore.h"
#include "sql/basic_istream.h"
#include "sql/basic_ostream.h"
#include "sql/binlog.h"
#include "sql/binlog_reader.h"
#include "sql/rpl_io_monitor.h"
#include "sql/rpl_rli_pdb.h"
#include "sql/sql_error.h"
#include "sql_string.h"

#define BINLOG_ARCHIVE_SUBDIR "binlog"
#define BINLOG_ARCHIVE_SUBDIR_LEN 6
#define BINLOG_ARCHIVE_INDEX_FILE_BASENAME "binlog-index."
#define BINLOG_ARCHIVE_INDEX_FILE_SUFFIX ".index"
#define BINLOG_ARCHIVE_INDEX_LOCAL_FILE "binlog-index.index"
#define BINLOG_ARCHIVE_START_INDEX_FILE_LEN 18
#define BINLOG_ARCHIVE_BASENAME "binlog."
#define BINLOG_ARCHIVE_SLICE_LOCAL_SUFFIX ".slice"
#define BINLOG_ARCHIVE_NUMBER_EXT "%06llu"
#define BINLOG_ARCHIVE_SLICE_POSITION_EXT "%010llu"
#define BINLOG_ARCHIVE_CONSENSUS_TERM_EXT "%020llu"

class THD;

struct LOG_ARCHIVED_INFO {
  char log_line[FN_REFLEN] = {0};
  char log_file_name[FN_REFLEN] = {0};
  char log_slice_name[FN_REFLEN] = {0};
  uint64_t slice_end_consensus_index;
  uint64_t slice_consensus_term;
  uint64_t log_previous_consensus_index;
  my_off_t slice_end_pos;
  my_off_t index_file_offset, index_file_start_offset;
  my_off_t pos;
  int entry_index;  // used in purge_logs(), calculatd in find_log_pos().
  LOG_ARCHIVED_INFO()
      : slice_end_consensus_index(0),
        slice_consensus_term(0),
        log_previous_consensus_index(0),
        slice_end_pos(0),
        index_file_offset(0),
        index_file_start_offset(0),
        pos(0),
        entry_index(0) {
    memset(log_line, 0, FN_REFLEN);
    memset(log_file_name, 0, FN_REFLEN);
    memset(log_slice_name, 0, FN_REFLEN);
  }

  // Copy constructor
  LOG_ARCHIVED_INFO(const LOG_ARCHIVED_INFO &other)
      : slice_end_consensus_index(other.slice_end_consensus_index),
        slice_consensus_term(other.slice_consensus_term),
        log_previous_consensus_index(other.log_previous_consensus_index),
        slice_end_pos(other.slice_end_pos),
        index_file_offset(other.index_file_offset),
        index_file_start_offset(other.index_file_start_offset),
        pos(other.pos),
        entry_index(other.entry_index) {
    strmake(log_line, other.log_line, FN_REFLEN);
    strmake(log_file_name, other.log_file_name, FN_REFLEN);
    strmake(log_slice_name, other.log_slice_name, FN_REFLEN);
  }

  LOG_ARCHIVED_INFO &operator=(const LOG_ARCHIVED_INFO &other) {
    if (this != &other) {
      strmake(this->log_line, other.log_line, FN_REFLEN);
      strmake(this->log_file_name, other.log_file_name, FN_REFLEN);
      strmake(this->log_slice_name, other.log_slice_name, FN_REFLEN);
      this->slice_end_consensus_index = other.slice_end_consensus_index;
      this->slice_consensus_term = other.slice_consensus_term;
      this->log_previous_consensus_index = other.log_previous_consensus_index;
      this->slice_end_pos = other.slice_end_pos;
      this->index_file_offset = other.index_file_offset;
      this->index_file_start_offset = other.index_file_start_offset;
      this->pos = other.pos;
      this->entry_index = other.entry_index;
    }
    return *this;
  }
};

struct SLICE_INFO {
  char log_file_name[FN_REFLEN] = {0};
  char log_slice_name[FN_REFLEN] = {0};
  char mysql_log_name[FN_REFLEN] = {0};
  uint64_t log_slice_end_consensus_index;
  uint64_t log_slice_consensus_term;
  uint64_t log_slice_previous_consensus_index;
  my_off_t log_slice_end_pos;
  my_off_t mysql_end_pos;
  int entry_index;  // used in purge_logs(), calculated in find_log_pos().
  SLICE_INFO()
      : log_slice_end_consensus_index(0),
        log_slice_consensus_term(0),
        log_slice_previous_consensus_index(0),
        log_slice_end_pos(0),
        mysql_end_pos(0),
        entry_index(0) {
    memset(log_file_name, 0, FN_REFLEN);
    memset(log_slice_name, 0, FN_REFLEN);
    memset(mysql_log_name, 0, FN_REFLEN);
  }

  // Copy constructor
  SLICE_INFO(const SLICE_INFO &other)
      : log_slice_end_consensus_index(other.log_slice_end_consensus_index),
        log_slice_consensus_term(other.log_slice_consensus_term),
        log_slice_previous_consensus_index(
            other.log_slice_previous_consensus_index),
        log_slice_end_pos(other.log_slice_end_pos),
        mysql_end_pos(other.mysql_end_pos),
        entry_index(other.entry_index) {
    strmake(log_file_name, other.log_file_name, FN_REFLEN);
    strmake(log_slice_name, other.log_slice_name, FN_REFLEN);
    strmake(mysql_log_name, other.mysql_log_name, FN_REFLEN);
  }

  SLICE_INFO &operator=(const SLICE_INFO &other) {
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
    }
    return *this;
  }
};

/**
 * @brief  Binlog expected slice structure.
 *
 */
struct Binlog_expected_slice {
  char m_slice_keyid[FN_REFLEN + 1];
  std::string slice_data_cache;
  uint32_t m_file_seq;
  uint32_t m_slice_seq;
  my_off_t m_slice_bytes_written;
  // Default constructor
  Binlog_expected_slice() = default;
  // Constructor
  Binlog_expected_slice(const char *keyid, const std::string &slice_data_cache,
                        my_off_t slice_bytes, uint32_t file_seq,
                        uint32_t slice_seq)
      : m_file_seq(file_seq), m_slice_seq(slice_seq) {
    strmake(this->m_slice_keyid, keyid, FN_REFLEN);
    this->slice_data_cache = slice_data_cache;
    this->m_slice_bytes_written = slice_bytes;
  }

  // Copy constructor
  Binlog_expected_slice(const Binlog_expected_slice &other)
      : slice_data_cache(other.slice_data_cache),
        m_file_seq(other.m_file_seq),
        m_slice_seq(other.m_slice_seq),
        m_slice_bytes_written(other.m_slice_bytes_written) {
    strmake(m_slice_keyid, other.m_slice_keyid, FN_REFLEN);
  }

  Binlog_expected_slice &operator=(const Binlog_expected_slice &other) {
    if (this != &other) {
      strmake(this->m_slice_keyid, other.m_slice_keyid, FN_REFLEN);
      this->slice_data_cache = other.slice_data_cache;
      this->m_file_seq = other.m_file_seq;
      this->m_slice_seq = other.m_slice_seq;
      this->m_slice_bytes_written = other.m_slice_bytes_written;
    }
    return *this;
  }
};

enum Slice_status_enum {
  SLICE_NOT_PERSISTED,
  SLICE_PERSISTED,
  SLICE_PERSISTED_FAILED
};
struct Slice_status {
  Slice_status_enum persisted;
  SLICE_INFO archived_info;
  uint32_t file_seq;
  uint32_t slice_seq;
  Slice_status() : persisted(SLICE_NOT_PERSISTED), file_seq(0), slice_seq(0) {}

  Slice_status(uint32_t f_seq, uint32_t s_seq, SLICE_INFO info)
      : persisted(SLICE_NOT_PERSISTED),
        archived_info(info),
        file_seq(f_seq),
        slice_seq(s_seq) {}
};

class Binlog_archive;

/**
 * @brief Binlog archive worker class.
 *
 */
class Binlog_archive_worker {
 public:
  Binlog_archive_worker(Binlog_archive *archive, int worker_id);
  ~Binlog_archive_worker();

  bool start();
  void stop();
  static void *thread_launcher(void *arg) {
    return static_cast<Binlog_archive_worker *>(arg)->worker_thread();
  }
  void *worker_thread();
  void thread_set_created() { m_thd_state.set_created(); }
  bool is_thread_alive_not_running() const {
    return m_thd_state.is_alive_not_running();
  }
  bool is_thread_dead() const { return m_thd_state.is_thread_dead(); }
  bool is_thread_alive() const { return m_thd_state.is_thread_alive(); }
  bool is_thread_running() const { return m_thd_state.is_running(); }
  int terminate_binlog_archive_worker_thread();
  bool is_binlog_archive_worker_waiting() const {
    return atomic_binlog_archive_worker_waiting.load(std::memory_order_acquire);
  }
  void set_binlog_archive_worker_waiting(bool waiting) {
    atomic_binlog_archive_worker_waiting.store(waiting, std::memory_order_release);
  }

 private:
  Binlog_archive *m_archive;
  my_thread_handle m_thread;
  int m_worker_id;
  Binlog_expected_slice m_current_slice;
  THD *m_thd;
  /* thread state */
  thread_state m_thd_state;
  mysql_mutex_t m_worker_run_lock;
  mysql_cond_t m_worker_run_cond;
  std::atomic<bool> atomic_binlog_archive_worker_waiting;
};

/**
 * @brief Binlog archive update index worker class.
 *
 */
class Binlog_archive_update_index_worker {
 public:
  Binlog_archive_update_index_worker(Binlog_archive *archive);
  ~Binlog_archive_update_index_worker();

  bool start();
  void stop();
  static void *thread_launcher(void *arg) {
    return static_cast<Binlog_archive_update_index_worker *>(arg)
        ->worker_thread();
  }
  void *worker_thread();
  void thread_set_created() { m_thd_state.set_created(); }
  bool is_thread_alive_not_running() const {
    return m_thd_state.is_alive_not_running();
  }
  bool is_thread_dead() const { return m_thd_state.is_thread_dead(); }
  bool is_thread_alive() const { return m_thd_state.is_thread_alive(); }
  bool is_thread_running() const { return m_thd_state.is_running(); }
  int terminate_binlog_archive_update_index_worker();
  bool is_update_index_failed() const {
    return atomic_update_index_failed.load(std::memory_order_acquire);
  }
  void set_update_index_failed(bool failed) {
    atomic_update_index_failed.store(failed, std::memory_order_release);
  }
  bool is_update_index_waiting() const {
    return atomic_update_index_waiting.load(std::memory_order_acquire);
  }
  void set_update_index_waiting(bool waiting) {
    atomic_update_index_waiting.store(waiting, std::memory_order_release);
  }

 private:
  Binlog_archive *m_archive;
  my_thread_handle m_thread;
  THD *m_thd;
  /* thread state */
  thread_state m_thd_state;
  mysql_mutex_t m_worker_run_lock;
  mysql_cond_t m_worker_run_cond;
  std::atomic<bool> atomic_update_index_failed;
  std::atomic<bool> atomic_update_index_waiting;
};

/**
 * @class Binlog_archive
 * @brief The Binlog_archive class is responsible for archiving binary log
 * files.
 */
class Binlog_archive {
  class Event_allocator;
  typedef Basic_binlog_file_reader<Binlog_ifile, Binlog_event_data_istream,
                                   Binlog_event_object_istream, Event_allocator>
      File_reader;

 public:
  /**
   * @brief Constructs a Binlog_archive object.
   * @param thd A pointer to the THD object.
   */
  Binlog_archive();

  /**
   * @brief Destructs the Binlog_archive object.
   */
  ~Binlog_archive();

  /**
   * @brief Initializes.
   */
  void init_pthread_object();
  /**
   * @brief Cleans up any resources.
   */
  void deinit_pthread_object();

  mysql_mutex_t *get_binlog_archive_lock();

  static Binlog_archive *get_instance();
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
  int archive_event(File_reader &reader, uchar *event_ptr, uint32 event_len,
                    const char *log_file, my_off_t log_pos);
  int binlog_stop_waiting_for_archive(const char *log_file_name,
                                      char *persistent_log_file_name,
                                      my_off_t log_pos,
                                      uint64_t consensus_index);
  int wait_for_archive();
  void signal_archive();
  int terminate_binlog_archive_thread();
  void lock_binlog_index() { mysql_mutex_lock(&m_index_lock); }
  void unlock_binlog_index() { mysql_mutex_unlock(&m_index_lock); }
  IO_CACHE *get_index_file();
  std::tuple<int, std::string> purge_logs(const char *to_log);
  int show_binlog_persistent_files(std::vector<objstore::ObjectMeta> &objects);
  static int find_next_log_common(IO_CACHE *index_file,
                                  LOG_ARCHIVED_INFO *linfo,
                                  bool found_slice = false);
  static int find_log_pos_common(IO_CACHE *index_file, LOG_ARCHIVED_INFO *linfo,
                                 const char *log_name, uint64_t consensus_index,
                                 bool last_slice = false);
  int rotate_binlog_slice(my_off_t log_pos, bool need_lock);
  inline void set_objstore(objstore::ObjectStore *objstore) {
    binlog_objstore = objstore;
  }
  inline objstore::ObjectStore *get_objstore() { return binlog_objstore; }
  int show_binlog_archive_task_info(
      uint64_t &persisting_consensus_index, uint64_t &consensus_term,
      std::string &persisting_mysql_binlog,
      my_off_t &persisting_mysql_binlog_pos,
      my_off_t &persisting_mysql_binlog_write_pos,
      std::string &persisting_binlog, my_off_t &persisting_binlog_pos,
      my_off_t &persisting_binlog_write_pos, std::string &persisted_binlog,
      my_off_t &persisted_binlog_pos, uint64_t &persisted_consensus_index,
      std::string &persisted_mysql_binlog,
      my_off_t &persisted_mysql_binlog_pos);

  int get_mysql_current_archive_binlog(LOG_INFO *linfo, bool need_lock = true);
  uint64_t get_slice_queue_map_term() { return m_slice_queue_and_map_term; }
  Binlog_archive_update_index_worker *m_update_index_worker;
  Binlog_archive_worker **m_workers;

  mysql_mutex_t m_slice_mutex;
  uint64_t m_slice_queue_and_map_term;  // the term of the slice queue and map
                                        // last change
  circular_buffer_queue<Binlog_expected_slice> m_expected_slice_queue;
  mysql_cond_t m_queue_cond;
  // File and slice tracking
  std::map<uint32_t, std::map<uint32_t, Slice_status>> m_slice_status_map;
  mysql_cond_t m_map_cond;
  // -- m_slice_mutex protects the above variables
  int32_t m_last_expected_file_seq{-1};  // last added slice file expected queue
  int32_t m_last_expected_slice_seq{-1};  // last added slice seq expected queue

  bool notify_slice_persisted(const Binlog_expected_slice &slice,
                              bool is_slice_persisted,
                              uint64_t slice_queue_map_term);
  bool update_index_file(bool need_slice_lock);

 private:
  // the binlog archive THD handle.
  THD *m_thd;
  /* thread state */
  thread_state m_thd_state;

  bool add_slice(Binlog_expected_slice &slice, SLICE_INFO &log_info);
  char m_persisted_binlog_file_name[FN_REFLEN + 1];
  char m_persisted_mysql_binlog_file_name[FN_REFLEN + 1];
  my_off_t m_persisted_mysql_binlog_last_event_end_pos;
  my_off_t m_persisted_binlog_last_event_end_pos;  //  The last persisted binlog
                                                   //  event position persisted
                                                   //  to objstore.
  uint64 m_persisted_slice_end_consensus_index;  // end consensus index of last
                                                 // persisted binlogs.
  uint64 m_persisted_binlog_previouse_consensus_index;

  /* current archive binlog file name, copy from mysql binlog file name
    e.g.
        --log-bin=mysql-bin/binlog
      m_binlog_archive_file_name = mysql-bin/binlog.000001
        --log-bin=/u01/mysql-bin/binlog
      m_binlog_archive_file_name = /u01/mysql-bin/binlog.000001
  */
  // current archive binlog file name, copy from mysql binlog file name
  char m_binlog_archive_file_name[FN_REFLEN + 1];
  char m_binlog_archive_dir[FN_REFLEN + 1];
  char m_mysql_archive_dir[FN_REFLEN + 1];
  char m_mysql_binlog_archive_dir[FN_REFLEN + 1];
  Format_description_event m_description_event;
  objstore::ObjectStore *binlog_objstore;
  mysql_mutex_t m_rotate_lock;
  uint64_t m_consensus_term;
  my_off_t m_slice_bytes_written;
  my_off_t
      m_binlog_archive_last_event_end_pos;  //  The last binlog event position
                                            //  persisted to objstore.
  my_off_t m_binlog_archive_write_last_event_end_pos;  // The last binlog event
                                                       // position writed to
                                                       // persistent cache.
  char m_mysql_binlog_start_file[FN_REFLEN + 1];
  my_off_t m_mysql_binlog_start_pos;  // mysql binlog archive start position.
  bool m_mysql_binlog_first_file;
  char m_mysql_binlog_file_name[FN_REFLEN + 1];  // mysql binlog entry name
  my_off_t
      m_mysql_binlog_last_event_end_pos;  //  The last binlog event position
                                          //  persisted to objstore.
  my_off_t m_mysql_binlog_write_last_event_end_pos;  // The last binlog event
                                                     // position writed to
                                                     // persistent cache.
  uint64_t m_binlog_archive_last_index_number;
  binary_log::enum_binlog_checksum_alg m_event_checksum_alg;
  uint64
      m_mysql_binlog_previouse_consensus_index;  // the previous consensus index
                                                 // of current mysql binlog
  uint64 m_binlog_previouse_consensus_index;     // the previous
                                                 // consensus index
                                                 // of previous
                                                 // mysql binlog
  uint64 m_binlog_archive_start_consensus_index;
  Log_event_type m_binlog_last_event_type;
  const char *m_binlog_last_event_type_str;
  Diagnostics_area m_diag_area;
  String m_packet;
  std::string m_slice_cache;
  bool m_binlog_in_transaction;
  bool m_rotate_forbidden;
  ulonglong m_slice_create_ts;
  uint64
      m_slice_end_consensus_index;  // end consensus index of persisted binlogs.
  uint64 m_mysql_end_consensus_index;  // end consensus index of readed mysql
                                       // binlogs.
  int new_binlog_slice(bool new_binlog, const char *log_file, my_off_t log_pos,
                       uint64_t previous_consensus_index);
  int archive_init();
  int archive_cleanup();
  bool consensus_leader_is_changed();
  int archive_binlogs();
  int archive_binlog(File_reader &reader, my_off_t start_pos);
  std::pair<my_off_t, int> get_binlog_end_pos(File_reader &reader);
  int archive_events(File_reader &reader, my_off_t end_pos);
  int read_format_description_event(File_reader &reader);
  int wait_new_mysql_binlog_events(my_off_t log_pos);
  int new_persistent_binlog_slice_key(const char *binlog,
                                      std::string &slice_name,
                                      const my_off_t pos, const uint64_t term);
  int stop_waiting_for_mysql_binlog_update(my_off_t log_pos);
  int binlog_is_archived(const char *log_file_name_arg,
                         char *persistent_log_file_name, my_off_t log_pos,
                         uint64_t consensus_index);
  int merge_slice_to_binlog_file(const char *log_name,
                                 const char *to_binlog_file);
  inline bool event_checksum_on() {
    return m_event_checksum_alg > binary_log::BINLOG_CHECKSUM_ALG_OFF &&
           m_event_checksum_alg < binary_log::BINLOG_CHECKSUM_ALG_ENUM_END;
  }
  void calc_event_checksum(uchar *event_ptr, size_t event_len);
  const static uint32 PACKET_MIN_SIZE = 4096;
  const static uint32 PACKET_MAX_SIZE = UINT_MAX32;
  const static ushort PACKET_SHRINK_COUNTER_THRESHOLD = 100;
  const static float PACKET_GROW_FACTOR;
  const static float PACKET_SHRINK_FACTOR;
  /*
    Needed to be able to evaluate if buffer needs to be resized (shrunk).
  */
  ushort m_half_buffer_size_req_counter;
  /*
   * The size of the buffer next time we shrink it.
   * This variable is updated once every time we shrink or grow the buffer.
   */
  size_t m_new_shrink_size;
  int reset_transmit_packet(size_t event_len);
  void calc_shrink_buffer_size(size_t current_size);
  size_t calc_grow_buffer_size(size_t current_size, size_t min_size);
  bool shrink_packet();
  bool grow_packet(size_t extra_size);

  /* The mysql binlog file it is reading */
  LOG_INFO m_mysql_linfo;

  IO_CACHE m_index_file;
  mysql_mutex_t m_index_lock;
  char m_index_local_file_name[FN_REFLEN];
  char m_index_file_name[FN_REFLEN];
  bool open_index_file();
  void close_index_file();
  int add_log_to_index(const uchar *log_name, size_t log_name_len);
  int find_log_pos_by_name(LOG_ARCHIVED_INFO *linfo, const char *log_name);
  int find_next_log(LOG_ARCHIVED_INFO *linfo);
  int find_next_log_slice(LOG_ARCHIVED_INFO *linfo);
  int remove_logs_from_index(LOG_ARCHIVED_INFO *linfo);
  /*
    m_crash_safe_index_file is temp file used for guaranteeing
    index file crash safe when master server restarts.
  */
  IO_CACHE m_crash_safe_index_file;
  char m_crash_safe_index_local_file_name[FN_REFLEN];
  enum enum_log_state { LOG_INDEX_OPENED, LOG_INDEX_CLOSED };
  std::atomic<enum_log_state> atomic_log_index_state{LOG_INDEX_CLOSED};
  int open_crash_safe_index_file();
  int close_crash_safe_index_file();
  int move_crash_safe_index_file_to_index_file();
  int set_crash_safe_index_file_name();
  /*
    purge_file is a temp file used in purge_logs so that the index file
    can be updated before deleting files from disk, yielding better crash
    recovery. It is created on demand the first time purge_logs is called
    and then reused for subsequent calls. It is cleaned up in cleanup().
  */
  IO_CACHE m_purge_index_file;
  char m_purge_index_file_name[FN_REFLEN];
  int set_purge_index_file_name();
  int open_purge_index_file(bool destroy);
  bool is_inited_purge_index_file();
  int close_purge_index_file();
  int sync_purge_index_file();
  int register_purge_index_entry(const char *entry);
  int register_create_index_entry(const char *entry);
  int purge_index_entry();
  int auto_purge_logs();
};

extern int start_binlog_archive();
extern void stop_binlog_archive();
extern int binlog_archive_wait_for_archive(THD *thd, const char *log_file_name,
                                           char *persistent_log_file_name,
                                           my_off_t log_pos,
                                           uint64_t consensus_index);
#endif
