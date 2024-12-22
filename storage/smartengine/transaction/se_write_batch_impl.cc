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

#include "se_write_batch_impl.h"
#include "transactions/transaction_db_impl.h"

namespace smartengine
{
SeWritebatchImpl::SeWritebatchImpl(THD *const thd)
    : SeTransaction(thd), m_batch(nullptr)
{
  m_batch = new util::WriteBatchWithIndex(
      util::BytewiseComparator(), 0, true);
}

SeWritebatchImpl::~SeWritebatchImpl()
{
  rollback();
  delete m_batch;
}

common::Status SeWritebatchImpl::put(db::ColumnFamilyHandle *const column_family,
                                     const common::Slice &key,
                                     const common::Slice &value)
{
  if (UNLIKELY(key.size() + value.size() > SE_MAX_ROW_SIZE)) {
    return common::Status::NotSupported("row too large");
  }
  ++m_write_count;
  m_batch->Put(column_family, key, value);
  // Note Put/Delete in write batch doesn't return any error code. We simply
  // return OK here.
  return common::Status::OK();
}

common::Status SeWritebatchImpl::delete_key(db::ColumnFamilyHandle *const column_family,
                                            const common::Slice &key)
{
  ++m_write_count;
  m_batch->Delete(column_family, key);
  return common::Status::OK();
}

common::Status SeWritebatchImpl::single_delete(db::ColumnFamilyHandle *const column_family,
                                               const common::Slice &key)
{
  ++m_write_count;
  m_batch->SingleDelete(column_family, key);
  return common::Status::OK();
}

common::Status SeWritebatchImpl::get(db::ColumnFamilyHandle *const column_family,
                                     const common::Slice &key,
                                     std::string *const value) const
{
  return m_batch->GetFromBatchAndDB(se_db,
                                    m_read_opts,
                                    column_family,
                                    key,
                                    value);
}

common::Status SeWritebatchImpl::get_latest(db::ColumnFamilyHandle *const column_family,
                                            const common::Slice &key,
                                            std::string *const value) const
{
  common::ReadOptions read_opts = m_read_opts;
  // use latest seq to read
  read_opts.snapshot = nullptr;
  return m_batch->GetFromBatchAndDB(se_db,
                                    read_opts,
                                    column_family,
                                    key,
                                    value);
}

common::Status SeWritebatchImpl::get_for_update(db::ColumnFamilyHandle *const column_family,
                                                const common::Slice &key,
                                                std::string *const value,
                                                bool exclusive,
                                                bool only_lock)
{
  return get(column_family, key, value);
}

common::Status SeWritebatchImpl::lock_unique_key(db::ColumnFamilyHandle *const column_family,
                                                 const common::Slice &key,
                                                 const bool skip_bloom_filter,
                                                 const bool fill_cache,
                                                 const bool exclusive)
{
  return common::Status::NotSupported();
}

void SeWritebatchImpl::rollback()
{
  m_write_count = 0;
  m_lock_count = 0;
  release_snapshot();

  reset();
  set_tx_read_only(false);
  m_rollback_only = false;

  if (m_backup_running) {
    util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();
    backup_instance->release_current_backup_snapshot(se_db);
    se_hotbackup_name = se_backup_status[4];
    m_backup_running = false;
  }
}

void SeWritebatchImpl::start_tx()
{
  reset();
  write_opts.sync = se_flush_log_at_trx_commit == 1;
  //write_opts.disableWAL = THDVAR(m_thd, write_disable_wal);
  write_opts.disableWAL = se_thd_write_disable_wal(m_thd);
  write_opts.ignore_missing_column_families = false;
      // THDVAR(m_thd, write_ignore_missing_column_families);
}

void SeWritebatchImpl::rollback_stmt()
{
  if (m_batch)
    m_batch->RollbackToSavePoint();
}

void SeWritebatchImpl::acquire_snapshot(bool acquire_now)
{
  if (m_read_opts.snapshot == nullptr)
    snapshot_created(se_db->GetSnapshot());
}

void SeWritebatchImpl::release_snapshot()
{
  if (m_read_opts.snapshot != nullptr) {
    se_db->ReleaseSnapshot(m_read_opts.snapshot);
    m_read_opts.snapshot = nullptr;
  }
}

db::WriteBatchBase *SeWritebatchImpl::get_indexed_write_batch()
{
  ++m_write_count;
  return m_batch;
}

db::Iterator *SeWritebatchImpl::get_iterator(const common::ReadOptions &options,
                                             db::ColumnFamilyHandle *const column_family)
{
  const auto it = se_db->NewIterator(options, se_db->DefaultColumnFamily());
  return m_batch->NewIteratorWithBase(it);
}

bool SeWritebatchImpl::prepare(const util::TransactionName &name)
{
  m_prepared = true;
  return true;
}

bool SeWritebatchImpl::commit_no_binlog()
{
  bool res = false;
  release_snapshot();

  bool async_commit = false;
  //bool async_commit = m_thd->variables.opt_async_commit &&
  //                    !m_thd->force_sync_commit &&
  //                    (nullptr != this->get_thd()->connection);

  common::AsyncCallback *cb_param = nullptr;
  if (async_commit) {
    write_opts.async_commit = async_commit;
    this->async_call_back.reset_thd(this->get_thd());
    cb_param = &this->async_call_back;
  } else {
    write_opts.async_commit = false;
    cb_param = nullptr;
  }

  const common::Status s =
      se_db->GetBaseDB()->WriteAsync(write_opts,
                                   m_batch->GetWriteBatch(),
                                   cb_param);
  //TODO comment by beilou 5.7->8.0 we will fix it later
  //if (async_commit) {
  //  this->m_thd->async_commit = true;
  //  return res;
  //}

	if (!s.ok()) {
    se_handle_io_error(s, SE_IO_ERROR_TX_COMMIT);
    res = true;
  }
  reset();

  m_write_count = 0;
  set_tx_read_only(false);
  m_rollback_only = false;
  return res;
}

bool SeWritebatchImpl::commit_no_binlog2()
{
	// rpl thread will not use async commit
  reset();

  write_opts.async_commit = false;

  m_write_count = 0;
  set_tx_read_only(false);
  m_rollback_only = false;
	return false;
}

void SeWritebatchImpl::reset()
{
  m_batch->Clear();
  m_read_opts = common::ReadOptions();
  m_ddl_transaction = false;
  m_prepared = false;
}

} // namespace smartengine
