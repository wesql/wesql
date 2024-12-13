/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Portions Copyright (c) 2015-Present, Facebook, Inc.
   Portions Copyright (c) 2012, Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#include "se_threads.h"
#include <sql_class.h>
#include "core/db/db_impl.h"
#include "dict/se_dict_util.h"
#include "handler/se_hton.h"
#include "objstore/lease_lock.h"
#include "sql/mysqld_thd_manager.h"
#include "transactions/transaction_db_impl.h"

namespace smartengine {

extern int32_t se_shrink_table_space;

extern bool opt_purge_invalid_subtable_bg;

extern uint32_t se_flush_log_at_trx_commit;

extern common::DBOptions se_db_options;

void *SeThread::thread_func(void *const thread_ptr)
{
  assert(thread_ptr != nullptr);
  //create and attach thd to current thread
  //for async commit context switch
  //backgroun thread may do async_commit for forground threads
  //so we need to create THD and set to treads attr
  my_thread_init();
  THD* thd =  new (std::nothrow) THD;
  if (nullptr != thd) {
#ifndef NDEBUG
    set_my_thread_var_id(thd->thread_id());
#endif
    thd->thread_stack=(char*)&thd;
    thd->store_globals();
  }
  SeThread *thread = static_cast<SeThread *>(thread_ptr);
  if (!thread->m_run_once.exchange(true)) {
    thread->setname();
    thread->run();
    thread->uninit();
  }
  if (nullptr != thd) 
    delete thd;
  my_thread_end();
  return nullptr;
}

void SeThread::init(
#ifdef HAVE_PSI_INTERFACE
    my_core::PSI_mutex_key stop_bg_psi_mutex_key,
    my_core::PSI_cond_key stop_bg_psi_cond_key
#endif
    )
{
  assert(!m_run_once);
  mysql_mutex_init(stop_bg_psi_mutex_key, &m_signal_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(stop_bg_psi_cond_key, &m_signal_cond);
}

void SeThread::uninit()
{
  mysql_mutex_destroy(&m_signal_mutex);
  mysql_cond_destroy(&m_signal_cond);
}

int SeThread::create_thread(const std::string &thread_name
#ifdef HAVE_PSI_INTERFACE
                              ,
                              PSI_thread_key background_psi_thread_key
#endif
                              )
{
  // Make a copy of the name so we can return without worrying that the
  // caller will free the memory
  m_name = thread_name;

  return mysql_thread_create(background_psi_thread_key, &m_handle, nullptr,
                             thread_func, this);
}

void SeThread::signal(const bool &stop_thread)
{
  SE_MUTEX_LOCK_CHECK(m_signal_mutex);

  if (stop_thread) {
    m_stop = true;
  }

  mysql_cond_signal(&m_signal_cond);

  SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);
}

/*
  Background thread's main logic
*/
void SeBackgroundThread::run()
{
  // How many seconds to wait till flushing the WAL next time.
  const int WAKE_UP_INTERVAL = 1;

  timespec ts_next_sync;
  clock_gettime(CLOCK_REALTIME, &ts_next_sync);
  ts_next_sync.tv_sec += WAKE_UP_INTERVAL;

  for (;;) {
    // Wait until the next timeout or until we receive a signal to stop the
    // thread. Request to stop the thread should only be triggered when the
    // storage engine is being unloaded.
    SE_MUTEX_LOCK_CHECK(m_signal_mutex);
    const auto ret MY_ATTRIBUTE((__unused__)) =
        mysql_cond_timedwait(&m_signal_cond, &m_signal_mutex, &ts_next_sync);

    // Check that we receive only the expected error codes.
    assert(ret == 0 || ret == ETIMEDOUT);
    const bool local_stop = m_stop;
    const bool local_save_stats = m_save_stats;
    reset();
    // backup snapshot timer is set
    if (counter_ > 0) {
      counter_--;
      if (counter_ <= 0) {
        assert(0 == counter_);
      }
    }
    SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);

    if (local_stop) {
      // If we're here then that's because condition variable was signaled by
      // another thread and we're shutting down. Break out the loop to make
      // sure that shutdown thread can proceed.
      break;
    }

    // This path should be taken only when the timer expired.
    assert(ret == ETIMEDOUT);

    if (local_save_stats) {
      ddl_manager.persist_stats();
    }

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    // Flush the WAL.
    if (se_db && se_flush_log_at_trx_commit == 2) {
      const common::Status s = se_db->sync_wal();
      if (!s.ok()) {
        se_handle_io_error(s, SE_IO_ERROR_BG_THREAD);
      }
    }

    // Set the next timestamp for mysql_cond_timedwait() (which ends up calling
    // pthread_cond_timedwait()) to wait on.
    ts_next_sync.tv_sec = ts.tv_sec + WAKE_UP_INTERVAL;
  }

  // save remaining stats which might've left unsaved
  ddl_manager.persist_stats();
}

// purge garbaged subtables due to commit-in-the-middle
static void purge_invalid_subtables()
{
  static timespec start = {0, 0};
  if (start.tv_sec == 0) {
    clock_gettime(CLOCK_REALTIME, &start);
  } else {
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    // scan invalid subtables per DURATION_PER_RUN seconds
    if (now.tv_sec < start.tv_sec + SeDropIndexThread::purge_schedule_interval) return;

    start = now;
  }

  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  THD* thd = new THD;
  thd->thread_stack = (char *)&thd;
  thd->slave_thread = 0;
  thd->set_command(COM_DAEMON);
  thd->set_new_thread_id();
  thd->store_globals();
  thd_manager->add_thd(thd);
  thd_manager->inc_thread_running();

  // get subtable ids from engine first
  std::vector<int32_t> ids_from_engine = cf_manager.get_subtable_ids();

  // collect id of subtables from global data dictionary
  std::set<uint32_t> ids_from_dd;
  if (!SeDdHelper::get_se_subtable_ids(thd, purge_acquire_lock_timeout,
                                               ids_from_dd)) {
    // pre-defined subtables for internal usage
    ids_from_dd.insert(0);                           // default cf
    ids_from_dd.insert(DEFAULT_SYSTEM_SUBTABLE_ID);  // system cf

    // saved invalid subtable ids for last run
    static std::set<uint32_t> last_invalid_subtables;
    // collect invalid id of invalid subtables
    std::set<uint32_t> current_invalid_subtables;
    for (auto& id : ids_from_engine) {
      uint32_t subtable_id = id;
      GL_INDEX_ID gl_index_id = {.cf_id = subtable_id,
                                 .index_id = subtable_id};
      // filter out create/drop-ongoing index
      if (ids_from_dd.find(subtable_id) == ids_from_dd.end() &&
          ddl_manager.can_purge_subtable(thd, gl_index_id)) {
        current_invalid_subtables.insert(subtable_id);
      }
    }

    if (!last_invalid_subtables.empty() && !current_invalid_subtables.empty()) {
      // purge invalid subtables present during both last and this run
      auto wb = dict_manager.begin();
      auto write_batch = wb.get();
      for (auto& subtable_id : last_invalid_subtables) {
        auto it = current_invalid_subtables.find(subtable_id);
        if (it != current_invalid_subtables.end()) {
          HANDLER_LOG(WARN, "SE: going to purge trashy subtable", K(subtable_id));
          dict_manager.delete_index_info(write_batch,
                            {.cf_id=subtable_id, .index_id = subtable_id});
          auto cfh = cf_manager.get_cf(subtable_id);
          if (cfh != nullptr) {
            se_db->Flush(common::FlushOptions(), cfh);
            cf_manager.drop_cf(se_db, subtable_id);
          }
          current_invalid_subtables.erase(it);
        }
      }
      dict_manager.commit(write_batch);
    }
    last_invalid_subtables = current_invalid_subtables;
  } else {
    HANDLER_LOG(WARN, "SE: failed to collect subtable id from DD!");
  }

  thd->release_resources();
  thd_manager->remove_thd(thd);
  thd_manager->dec_thread_running();
  delete thd;
}

/*
  Drop index thread's main logic
*/
void SeDropIndexThread::run()
{
  SE_MUTEX_LOCK_CHECK(m_signal_mutex);

  for (;;) {
    // The stop flag might be set by shutdown command
    // after drop_index_thread releases signal_mutex
    // (i.e. while executing expensive Seek()). To prevent drop_index_thread
    // from entering long cond_timedwait, checking if stop flag
    // is true or not is needed, with drop_index_interrupt_mutex held.
    if (m_stop) {
      break;
    }

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += SeDropIndexThread::purge_schedule_interval;

    // enter wait
    if (m_run_) {
      m_run_ = false;
    }

    const auto ret MY_ATTRIBUTE((__unused__)) =
        mysql_cond_timedwait(&m_signal_cond, &m_signal_mutex, &ts);
    if (m_stop) {
      break;
    }
    // FIXME: shrink exent space cann't do with drop index
    if (se_shrink_table_space >= 0) {
      sql_print_information("Wait for shrink extent space finish");
      continue;
    }
    // make sure, no program error is returned
    assert(ret == 0 || ret == ETIMEDOUT);
    // may be work
    m_run_ = true;
    SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);

    if (opt_purge_invalid_subtable_bg)
      purge_invalid_subtables();

    SE_MUTEX_LOCK_CHECK(m_signal_mutex);
    m_run_ = false;
  }

  SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);
}

/*
  Renewal single data node lease lock thread's main logic
 */
void SeRenewLeaseLockThread::run()
{
  const std::string_view objstore_bucket = db::GlobalContext::env_->GetObjectStoreBucket();
  const std::string_view cluster_objstore_id = db::GlobalContext::env_->GetClusterObjstoreId();
  const uint32_t objstore_lease_lock_timeout = db::GlobalContext::env_->GetObjstoreLeaseLockTimeout();
  objstore::ObjectStore *objstore = nullptr;
  if (0 != db::GlobalContext::env_->GetObjectStore(objstore).code()) {
    sql_print_error("Failed to get object store in renewal lease lock thread");
    abort_with_stack_traces();
  } else if (objstore_bucket.empty()) {
    sql_print_error("Object store bucket is empty in renewal lease lock thread");
    abort_with_stack_traces();
  } else if (cluster_objstore_id.empty()) {
    sql_print_error("Cluster object store id is empty in renewal lease lock thread");
    abort_with_stack_traces();
  } else if (objstore == nullptr) {
    sql_print_error("Object store is nullptr in renewal lease lock thread");
    abort_with_stack_traces();
  }
  const std::chrono::seconds retry_lock_interval = std::chrono::seconds(1); // 1s
    
  objstore::LeaseLockSettings lease_lock_settings;
  lease_lock_settings.lease_timeout = std::chrono::milliseconds(objstore_lease_lock_timeout*1000);
  lease_lock_settings.renewal_timeout = std::chrono::milliseconds(lease_lock_settings.lease_timeout.count() * 3 / 4);
  lease_lock_settings.renewal_interval = std::chrono::milliseconds(lease_lock_settings.renewal_timeout.count()/2);

  SE_MUTEX_LOCK_CHECK(m_signal_mutex);
  for (;;) {
    SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);

    std::string err_msg;
    std::string important_msg;
    bool need_abort = false;
    int err = objstore::try_single_data_node_lease_lock(objstore,
                                                        objstore_bucket,
                                                        cluster_objstore_id,
                                                        lease_lock_settings,
                                                        err_msg,
                                                        important_msg,
                                                        need_abort);
    if (!important_msg.empty()) {
      sql_print_warning("%s", important_msg.c_str());
    }
    if (0 != err) {
      sql_print_warning("Failed to get single data node lease lock: %s", err_msg.c_str());
      if (need_abort) {
        abort();
      }
    }

    SE_MUTEX_LOCK_CHECK(m_signal_mutex);
    if (m_stop) {
      break;
    }

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    if (objstore::is_lease_lock_owner_node()) {
      time_t inc_sec = lease_lock_settings.renewal_interval.count() / 1000;
      long inc_nsec = ts.tv_nsec + lease_lock_settings.renewal_interval.count() % 1000 * 1000000;
      ts.tv_sec += inc_sec + inc_nsec / 1000000000;
      ts.tv_nsec = inc_nsec % 1000000000;
    } else {
      ts.tv_sec += retry_lock_interval.count();
    }

    const int ret MY_ATTRIBUTE((__unused__)) = mysql_cond_timedwait(&m_signal_cond, &m_signal_mutex, &ts);
    if (m_stop) {
      break;
    }
    // make sure, no program error is returned
    assert(ret == 0 || ret == ETIMEDOUT);
  }

  SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);
}

} //namespace smartengine
