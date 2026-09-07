/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012, Monty Program Ab

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

#include "se_transaction.h"
#include "ha_smartengine.h"
#include "se_system_vars.h"
#include "mysql/plugin.h"
#include "sql/query_options.h"
#include "se_hton.h"
#include "se_transaction_list_walker.h"
#include "dict/se_table.h"
#include "transactions/transaction_db_impl.h"
#include "objstore/remote_extent.h"

namespace smartengine
{
static const char *const ERRSTR_ROLLBACK_ONLY =
    "This transaction was rolled back and cannot be "
    "committed. Only supported operation is to roll it back, "
    "so all pending changes will be discarded. "
    "Please restart another transaction.";

void SeSnapshotNotifier::SnapshotCreated(const db::Snapshot *const snapshot)
{
  if (m_owning_tx != nullptr) {
    m_owning_tx->snapshot_created(snapshot);
  }
}

std::multiset<SeTransaction *> SeTransaction::s_tx_list;

mysql_mutex_t SeTransaction::s_tx_list_mutex;

SeTransaction::SeTransaction(THD *const thd) : m_thd(thd)
{
  SE_MUTEX_LOCK_CHECK(s_tx_list_mutex);
  s_tx_list.insert(this);
  SE_MUTEX_UNLOCK_CHECK(s_tx_list_mutex);
}

SeTransaction::~SeTransaction()
{
  SE_MUTEX_LOCK_CHECK(s_tx_list_mutex);
  s_tx_list.erase(this);
  SE_MUTEX_UNLOCK_CHECK(s_tx_list_mutex);
}

bool SeTransaction::commit()
{
  reset_writebatch_iterator();

  if (get_write_count() == 0) {
    rollback();
    return false;
  } else if (m_rollback_only) {
    /*
      Transactions marked as rollback_only are expected to be rolled back at
      prepare(). But there are some exceptions like below that prepare() is
      never called and commit() is called instead.
       1. Binlog is disabled
       2. No modification exists in binlog cache for the transaction (#195)
      In both cases, rolling back transaction is safe. Nothing is written to
      binlog.
     */
    my_printf_error(ER_UNKNOWN_ERROR, ERRSTR_ROLLBACK_ONLY, MYF(0));
    rollback();
    return true;
  } else {
    return commit_no_binlog();
  }
}

bool SeTransaction::commit_or_rollback()
{
  bool res;
  if (m_is_tx_failed) {
    rollback();
    res = false;
  } else
    res = commit();
  return res;
}

void SeTransaction::start_bulk_load(ha_smartengine *const bulk_load)
{
  /*
   If we already have an open bulk load of a table and the name doesn't
   match the current one, close out the currently running one.  This allows
   multiple bulk loads to occur on a partitioned table, but then closes
   them all out when we switch to another table.
  */
  assert(bulk_load != nullptr);

  if (!m_curr_bulk_load.empty() &&
      !bulk_load->same_table(*m_curr_bulk_load[0])) {
    const auto res = finish_bulk_load();
    SHIP_ASSERT(res == 0);
  }

  m_curr_bulk_load.push_back(bulk_load);
}

void SeTransaction::end_bulk_load(ha_smartengine *const bulk_load)
{
  for (auto it = m_curr_bulk_load.begin(); it != m_curr_bulk_load.end();
       it++) {
    if (*it == bulk_load) {
      m_curr_bulk_load.erase(it);
      return;
    }
  }

  // Should not reach here
  SHIP_ASSERT(0);
}

int SeTransaction::finish_bulk_load()
{
  int rc = 0;

  std::vector<ha_smartengine *>::iterator it;
  while ((it = m_curr_bulk_load.begin()) != m_curr_bulk_load.end()) {
    int rc2 = (*it)->finalize_bulk_load();
    if (rc2 != 0 && rc == 0) {
      rc = rc2;
    }
  }

  assert(m_curr_bulk_load.size() == 0);

  return rc;
}

bool SeTransaction::flush_batch()
{
  if (storage::remote_extent::enabled()) {
    storage::remote_extent::enter_fenced(
        "SmartEngine middle transaction flush is forbidden in remote mode");
    return true;
  }
  if (get_write_count() == 0)
    return false;

  /* Commit the current transaction */
  if (commit_no_binlog())
    return true;

  /* Start another one */
  start_tx();
  return false;
}

int SeTransaction::set_status_error(THD *const thd,
                                    const common::Status &s,
                                    const SeKeyDef &kd,
                                    SeTableDef *const tbl_def)
{
  assert(!s.ok());
  assert(tbl_def != nullptr);

  if (s.IsTimedOut()) {
    __HANDLER_LOG(WARN, "tx failed with timeout on key(%u)", kd.get_index_number());
    /*
      SQL layer has weird expectations. If we return an error when
      doing a read in DELETE IGNORE, it will ignore the error ("because it's
      an IGNORE command!) but then will fail an assert, because "error code
      was returned, but no error happened".  Do what InnoDB's
      convert_error_code_to_mysql() does: force a statement
      rollback before returning HA_ERR_LOCK_WAIT_TIMEOUT:
      */
    my_core::thd_mark_transaction_to_rollback(thd, false /*just statement*/);
    m_detailed_error.copy(timeout_message(
        "index", tbl_def->full_tablename().c_str(), kd.get_name().c_str()));

    return HA_ERR_LOCK_WAIT_TIMEOUT;
  }

  if (s.IsDeadlock()) {
    __HANDLER_LOG(WARN, "tx failed with deadlock on key(%u)", kd.get_index_number());
    my_core::thd_mark_transaction_to_rollback(thd,
                                              true /* whole transaction */);
    return HA_ERR_LOCK_DEADLOCK;
  } else if (s.IsBusy()) {
    __HANDLER_LOG(WARN, "tx failed with snapshot conflict on key(%u)", kd.get_index_number());
    global_stats.snapshot_conflict_errors_++;
    return HA_ERR_LOCK_DEADLOCK;
  }

  if (s.IsLockLimit()) {
    __HANDLER_LOG(WARN, "tx failed with too many locks on key(%u)", kd.get_index_number());
    return HA_ERR_SE_TOO_MANY_LOCKS;
  }

  if (s.IsIOError() || s.IsCorruption()) {
    se_handle_io_error(s, SE_IO_ERROR_GENERAL);
  }
  my_error(ER_INTERNAL_ERROR, MYF(0), s.ToString().c_str());
  return HA_ERR_INTERNAL_ERROR;
}

void SeTransaction::set_params(THD *thd)
{
  if (thd_tx_is_dd_trx(thd)) {
    // The attachable transaction for dd is must AC-RO-RC-NL
    // (auto-commit, read-only, read-committed, no-locks)
    assert(!my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN));
    assert(0 == get_lock_count());
    assert(thd_tx_is_read_only(thd));
    assert(ISO_READ_COMMITTED == thd_tx_isolation(thd));
    m_max_row_locks = 0;
  } else {
    m_timeout_sec = se_thd_lock_wait_timeout(thd);
    m_max_row_locks = se_thd_max_row_locks(thd);
    set_lock_timeout(m_timeout_sec);
  }
}

void SeTransaction::snapshot_created(const db::Snapshot *const snapshot)
{
  assert(snapshot != nullptr);

  m_read_opts.snapshot = snapshot;
  se_db->GetEnv()->GetCurrentTime(&m_snapshot_timestamp);
  m_is_delayed_snapshot = false;
}

db::Iterator *SeTransaction::get_iterator(
             db::ColumnFamilyHandle *const column_family,
             bool skip_bloom_filter,
             bool fill_cache,
             bool read_current,
             bool create_snapshot,
             bool exclude_l2,
             bool unique_check)
{
  // Make sure we are not doing both read_current (which implies we don't
  // want a snapshot) and create_snapshot which makes sure we create
  // a snapshot
  assert(column_family != nullptr);
  assert(!read_current || !create_snapshot);

  if (create_snapshot)
    acquire_snapshot(true);

  common::ReadOptions options = m_read_opts;

  if (skip_bloom_filter) {
    options.total_order_seek = true;
  }
  options.fill_cache = fill_cache;
  if (read_current) {
    options.snapshot = nullptr;
  }

  if(exclude_l2){
    options.read_level_ = common::kExcludeL2;
  }
  options.unique_check_ = unique_check;

  return get_iterator(options, column_family);
}

bool SeTransaction::can_prepare() const
{
  if (m_rollback_only) {
    my_printf_error(ER_UNKNOWN_ERROR, ERRSTR_ROLLBACK_ONLY, MYF(0));
    return false;
  }
  return true;
}

int SeTransaction::rollback_to_savepoint(void *const savepoint)
{
  if (has_modifications()) {
    my_printf_error(ER_UNKNOWN_ERROR,
                    "SE currently does not support ROLLBACK TO "
                    "SAVEPOINT if modifying rows.",
                    MYF(0));
    m_rollback_only = true;
    return HA_EXIT_FAILURE;
  }
  return HA_EXIT_SUCCESS;
}

void SeTransaction::init_mutex()
{
  mysql_mutex_init(key_mutex_tx_list, &s_tx_list_mutex, MY_MUTEX_INIT_FAST);
}

void SeTransaction::term_mutex()
{
  assert(s_tx_list.size() == 0);
  mysql_mutex_destroy(&s_tx_list_mutex);
}

void SeTransaction::walk_tx_list(SeTransListWalker *walker)
{
    assert(walker != nullptr);

    SE_MUTEX_LOCK_CHECK(s_tx_list_mutex);

    for (auto it : s_tx_list)
      walker->process_trans(it);

    SE_MUTEX_UNLOCK_CHECK(s_tx_list_mutex);
}

} // namespace smartengine
