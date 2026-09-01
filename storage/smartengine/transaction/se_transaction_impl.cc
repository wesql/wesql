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

#include "se_transaction_impl.h"
#include "transactions/transaction_db_impl.h"

namespace smartengine
{

SeTransactionImpl::SeTransactionImpl(THD *const thd)
    : SeTransaction(thd), m_se_tx(nullptr)
{
  // Create a notifier that can be called when a snapshot gets generated.
  m_notifier = std::make_shared<SeSnapshotNotifier>(this);
}

SeTransactionImpl::~SeTransactionImpl()
{
  if (!m_prepared) {
    rollback();
  }

  // Theoretically the notifier could outlive the SeTransactionImpl
  // (because of the shared_ptr), so let it know it can't reference
  // the transaction anymore.
  m_notifier->detach();

  // Free any transaction memory that is still hanging around.
  delete m_se_reuse_tx;
}

common::Status SeTransactionImpl::put(db::ColumnFamilyHandle *const column_family,
                                      const common::Slice &key,
                                      const common::Slice &value)
{
  if (UNLIKELY(key.size() + value.size() > SE_MAX_ROW_SIZE)) {
    return common::Status::NotSupported("row too large");
  }
  ++m_write_count;
  ++m_lock_count;
  if (m_write_count > m_max_row_locks || m_lock_count > m_max_row_locks)
    return common::Status(common::Status::kLockLimit);
  return m_se_tx->Put(column_family, key, value);
}

common::Status SeTransactionImpl::delete_key(db::ColumnFamilyHandle *const column_family,
                                             const common::Slice &key)
{
  ++m_write_count;
  ++m_lock_count;
  if (m_write_count > m_max_row_locks || m_lock_count > m_max_row_locks)
    return common::Status(common::Status::kLockLimit);
  return m_se_tx->Delete(column_family, key);
}

common::Status SeTransactionImpl::single_delete(db::ColumnFamilyHandle *const column_family,
                                                const common::Slice &key)
{
  ++m_write_count;
  ++m_lock_count;
  if (m_write_count > m_max_row_locks || m_lock_count > m_max_row_locks)
    return common::Status(common::Status::kLockLimit);
  return m_se_tx->SingleDelete(column_family, key);
}

common::Status SeTransactionImpl::get(db::ColumnFamilyHandle *const column_family,
                                      const common::Slice &key,
                                      std::string *value) const
{
  return m_se_tx->Get(m_read_opts, column_family, key, value);
}

common::Status SeTransactionImpl::get_latest(db::ColumnFamilyHandle *const column_family,
                                             const common::Slice &key,
                                             std::string *value) const
{
  common::ReadOptions read_opts = m_read_opts;
  // use latest seq to read
  read_opts.snapshot = nullptr;
  return m_se_tx->Get(read_opts, column_family, key, value);
}

common::Status SeTransactionImpl::get_for_update(db::ColumnFamilyHandle *const column_family,
                                                 const common::Slice &key,
                                                 std::string *const value,
                                                 bool exclusive,
                                                 bool only_lock)
{
  if (++m_lock_count > m_max_row_locks)
    return common::Status(common::Status::kLockLimit);

  if (only_lock) {
    return m_se_tx->TryLock(column_family, key, true /* read_only */, exclusive, false, only_lock);
  } else {
    return m_se_tx->GetForUpdate(m_read_opts, column_family, key, value,
                                      exclusive);
  }
}

common::Status SeTransactionImpl::lock_unique_key(db::ColumnFamilyHandle *const column_family,
                                                  const common::Slice &key,
                                                  const bool skip_bloom_filter,
                                                  const bool fill_cache,
                                                  const bool exclusive)
{
  if (++m_lock_count > m_max_row_locks) {
    return common::Status(common::Status::kLockLimit);
  } else {
    common::ReadOptions read_opts = m_read_opts;
    // use latest seq to read in TryLock
    read_opts.snapshot = nullptr;
    assert(column_family != nullptr);
    // copied from SeTransaction::get_iterator
    if (skip_bloom_filter) {
      read_opts.total_order_seek = true;
    }
    read_opts.fill_cache = fill_cache;
    // should not skip deleted record when checking seq validation
    read_opts.skip_del_ = false;
    return m_se_tx->TryLock(column_family,
                            key,
                            true /* read_only */,
                            exclusive,
                            false,
                            true /*lock_uk*/,
                            &read_opts);
  }
}

void SeTransactionImpl::rollback()
{
  m_write_count = 0;
  m_lock_count = 0;
  m_ddl_transaction = false;
  reset_writebatch_iterator();

  if (m_se_tx) {
    release_snapshot();
    /* This will also release all of the locks: */
    m_se_tx->Rollback();

    /* Save the transaction object to be reused */
    release_tx();

    set_tx_read_only(false);
    m_rollback_only = false;
  }
  if (m_backup_running) {
    util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_file_instance();
    backup_instance->release_current_backup_snapshot(se_db);
    se_hotbackup_name = se_backup_status[4];
    m_backup_running = false;
  }
}

void SeTransactionImpl::start_tx()
{
  util::TransactionOptions tx_opts;
  common::WriteOptions write_opts;
  tx_opts.set_snapshot = false;
  tx_opts.lock_timeout = se_convert_sec_to_ms(m_timeout_sec);
  //tx_opts.deadlock_detect = THDVAR(m_thd, deadlock_detect);
  tx_opts.deadlock_detect = se_thd_deadlock_detect(m_thd);

  write_opts.sync = se_flush_log_at_trx_commit == 1;
  //write_opts.disableWAL = THDVAR(m_thd, write_disable_wal);
  write_opts.disableWAL = se_thd_write_disable_wal(m_thd);
  write_opts.ignore_missing_column_families = false;
      // THDVAR(m_thd, write_ignore_missing_column_families);
  m_is_two_phase = se_enable_2pc;

  /*
    If m_se_reuse_tx is null this will create a new transaction object.
    Otherwise it will reuse the existing one.
  */
  m_se_tx =
      se_db->BeginTransaction(write_opts, tx_opts, m_se_reuse_tx);
  m_se_reuse_tx = nullptr;

  m_read_opts = common::ReadOptions();

  m_prepared = false;
  m_ddl_transaction = false;
  m_is_delayed_snapshot = false;
}

/*
  Start a statement inside a multi-statement transaction.

  @todo: are we sure this is called once (and not several times) per
  statement start?

  For hooking to start of statement that is its own transaction, see
  ha_smartengine::external_lock().
*/
void SeTransactionImpl::start_stmt()
{
  // Set the snapshot to delayed acquisition (SetSnapshotOnNextOperation)
  acquire_snapshot(false);
  m_se_tx->SetSavePoint();
}

/*
  This must be called when last statement is rolled back, but the transaction
  continues
*/
void SeTransactionImpl::rollback_stmt()
{
  /* TODO: here we must release the locks taken since the start_stmt() call */
  if (m_se_tx) {
    const db::Snapshot *const org_snapshot = m_se_tx->GetSnapshot();
    m_se_tx->RollbackToSavePoint();

    const db::Snapshot *const cur_snapshot = m_se_tx->GetSnapshot();
    if (org_snapshot != cur_snapshot) {
      if (org_snapshot != nullptr)
        m_snapshot_timestamp = 0;

      m_read_opts.snapshot = cur_snapshot;
      if (cur_snapshot != nullptr)
        se_db->GetEnv()->GetCurrentTime(&m_snapshot_timestamp);
      else
        m_is_delayed_snapshot = true;
    }
  }
}

void SeTransactionImpl::acquire_snapshot(bool acquire_now)
{
  if (m_read_opts.snapshot == nullptr) {
    if (is_tx_read_only()) {
      snapshot_created(se_db->GetSnapshot());
    } else if (acquire_now) {
      m_se_tx->SetSnapshot();
      snapshot_created(m_se_tx->GetSnapshot());
    } else if (!m_is_delayed_snapshot) {
      m_se_tx->SetSnapshotOnNextOperation(m_notifier);
      m_is_delayed_snapshot = true;
    }
  }
}

void SeTransactionImpl::release_snapshot()
{
  bool need_clear = m_is_delayed_snapshot;

  if (m_read_opts.snapshot != nullptr) {
    m_snapshot_timestamp = 0;
    if (is_tx_read_only()) {
      se_db->ReleaseSnapshot(m_read_opts.snapshot);
      need_clear = false;
    } else {
      need_clear = true;
    }
    m_read_opts.snapshot = nullptr;
  }

  if (need_clear && m_se_tx != nullptr)
    m_se_tx->ClearSnapshot();
}

void SeTransactionImpl::release_lock(
    db::ColumnFamilyHandle *const column_family,
    const std::string &rowkey)
{
  //if (!THDVAR(m_thd, lock_scanned_rows)) {
  if (!se_thd_lock_scanned_rows(m_thd)) {
    m_se_tx->UndoGetForUpdate(column_family, common::Slice(rowkey));
  }
}

bool SeTransactionImpl::has_modifications() const
{
  return m_se_tx->GetWriteBatch() &&
         m_se_tx->GetWriteBatch()->GetWriteBatch() &&
         m_se_tx->GetWriteBatch()->GetWriteBatch()->Count() > 0;
}

/*
  Return a WriteBatch that one can write to. The writes will skip any
  transaction locking. The writes WILL be visible to the transaction.
*/
db::WriteBatchBase *SeTransactionImpl::get_indexed_write_batch()
{
  ++m_write_count;
  return m_se_tx->GetWriteBatch();
}

void SeTransactionImpl::set_lock_timeout(int timeout_sec_arg)
{
  if (m_se_tx)
    m_se_tx->SetLockTimeout(se_convert_sec_to_ms(m_timeout_sec));
}

void SeTransactionImpl::set_sync(bool sync)
{
  m_se_tx->GetWriteOptions()->sync = sync;
}

void SeTransactionImpl::detach_from_se()
{
  m_se_tx = nullptr;
  m_se_reuse_tx = nullptr;
}

db::WriteBatchBase *SeTransactionImpl::get_write_batch()
{
  if (is_two_phase()) {
    return m_se_tx->GetCommitTimeWriteBatch();
  }
  return m_se_tx->GetWriteBatch()->GetWriteBatch();
}

db::Iterator *SeTransactionImpl::get_iterator(
    const common::ReadOptions &options,
    db::ColumnFamilyHandle *const column_family)
{
  return m_se_tx->GetIterator(options, column_family);
}

void SeTransactionImpl::release_tx()
{
  // We are done with the current active transaction object.  Preserve it
  // for later reuse.
  assert(m_se_reuse_tx == nullptr);
  m_se_reuse_tx = m_se_tx;
  m_se_tx = nullptr;
  m_prepared = false;
}

bool SeTransactionImpl::prepare(const util::TransactionName &name)
{
  common::Status s;
  s = m_se_tx->SetName(name);
  if (!s.ok()) {
    se_handle_io_error(s, SE_IO_ERROR_TX_COMMIT);
    return false;
  }

  s = m_se_tx->Prepare();
  if (!s.ok()) {
    se_handle_io_error(s, SE_IO_ERROR_TX_COMMIT);
    return false;
  }
  m_prepared = true;
  return true;
}

bool SeTransactionImpl::commit_no_binlog()
{
  bool res = false;
  release_snapshot();
   //comment by beilou, we have no async commit facility in 8.0 server
  //bool async_commit = m_thd->variables.opt_async_commit &&
  //                    !m_thd->force_sync_commit &&
  //                    (nullptr != this->get_thd()->connection);

  bool async_commit = false;

  common::AsyncCallback *cb_param = nullptr;
  if (async_commit) {
    m_se_tx->GetWriteOptions()->async_commit = async_commit;
    //assert(nullptr != this->get_thd()->connection);
    this->async_call_back.reset_thd(this->get_thd());
    cb_param = &this->async_call_back;
  } else {
    m_se_tx->GetWriteOptions()->async_commit = false;
    cb_param = nullptr;
  }

  // If the binary log is not enabled, or the transaction
  // is not written to the binary log, the file name will
  // be nullptr
  if (m_se_tx->GetWriteBatch()->GetWriteBatch()) {
    const char *file_name = nullptr;
    ulonglong offset = 0;

    thd_binlog_pos(m_thd, &file_name, &offset);
    m_se_tx->GetWriteBatch()->GetWriteBatch()->SetBinlogPosition(file_name,
                                                                 offset);
  }

  const common::Status s = m_se_tx->CommitAsync(cb_param);

  if (async_commit) {//async commit return directly
    // TODO comment by beilou, will fix async commit later
    //this->m_thd->async_commit = true;
    abort();
    return res;
  }

  if (!s.ok()) {
    se_handle_io_error(s, SE_IO_ERROR_TX_COMMIT);
    res = true;
  }

  if (m_se_tx->GetWriteBatch()->GetWriteBatch())
    m_se_tx->GetWriteBatch()->GetWriteBatch()->ClearBinlogPosition();

  /* Save the transaction object to be reused */
  release_tx();

  m_write_count = 0;
  m_lock_count = 0;
  set_tx_read_only(false);
  m_rollback_only = false;
  return res;
}

bool SeTransactionImpl::commit_no_binlog2()
{
  bool res = false;

  m_se_tx->GetWriteOptions()->async_commit = false;

  /* Save the transaction object to be reused */
  release_tx();

  m_write_count = 0;
  m_lock_count = 0;
  set_tx_read_only(false);
  m_rollback_only = false;
  return res;
}

} // namespace smartengine
