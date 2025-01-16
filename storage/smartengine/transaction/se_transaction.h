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

#ifndef SMARTENGINE_TRANSACTION_H_
#define SMARTENGINE_TRANSACTION_H_

#include "se_async_commit_callback.h"
#include <set>
#include "sql_string.h"
#include "transactions/transaction.h"
#include "write_batch/write_batch.h"

namespace smartengine
{
class ha_smartengine;
class SeKeyDef;
class SeTableDef;
class SeTransListWalker;
class SeTransaction;

namespace db
{
class WriteBatchBase;
}

/*
  This is a helper class that is passed to SE to get notifications when
  a snapshot gets created.
*/

class SeSnapshotNotifier : public util::TransactionNotifier {
public:
  SeSnapshotNotifier(const SeSnapshotNotifier &) = delete;
  SeSnapshotNotifier &operator=(const SeSnapshotNotifier &) = delete;

  explicit SeSnapshotNotifier(SeTransaction *const owning_tx)
      : m_owning_tx(owning_tx) {}

  // If the owning SeTransaction gets destructed we need to not reference
  // it anymore.
  void detach() { m_owning_tx = nullptr; }

private:
  void SnapshotCreated(const db::Snapshot *snapshot) override;

private:
  SeTransaction *m_owning_tx;
};

/**This is the base class for transactions when interacting with se.*/
class SeTransaction
{
public:
  explicit SeTransaction(THD *const thd);

  virtual ~SeTransaction();

  virtual common::Status put(db::ColumnFamilyHandle *const column_family,
                             const common::Slice &key,
                             const common::Slice &value) = 0;

  virtual common::Status delete_key(db::ColumnFamilyHandle *const column_family,
                                    const common::Slice &key) = 0;

  virtual common::Status single_delete(db::ColumnFamilyHandle *const column_family,
                                       const common::Slice &key) = 0;

  virtual common::Status get(db::ColumnFamilyHandle *const column_family,
                             const common::Slice &key,
                             std::string *value) const = 0;

  virtual common::Status get_latest(db::ColumnFamilyHandle *const column_family,
                                    const common::Slice &key,
                                    std::string *value) const = 0;

  virtual common::Status get_for_update(db::ColumnFamilyHandle *const column_family,
                                        const common::Slice &key,
                                        std::string *const value,
                                        bool exclusive,
                                        bool only_lock = false) = 0;

  virtual common::Status lock_unique_key(db::ColumnFamilyHandle *const column_family,
                                         const common::Slice &key,
                                         const bool skip_bloom_filter,
                                         const bool fill_cache,
                                         const bool exclusive) = 0;

  virtual bool prepare(const util::TransactionName &name) = 0;

  bool commit();

  bool commit2() { return commit_no_binlog2(); }

  virtual void rollback() = 0;

  bool commit_or_rollback();

  virtual void start_tx() = 0;

  virtual void start_stmt() = 0;

  virtual void rollback_stmt() = 0;

  virtual void release_lock(db::ColumnFamilyHandle *const column_family,
                            const std::string &rowkey) = 0;

  virtual void acquire_snapshot(bool acquire_now) = 0;

  virtual void release_snapshot() = 0;

  virtual bool is_tx_started() const = 0;

  /*
    for distinction between se_transaction_impl and se_writebatch_impl
    when using walk tx list
  */
  virtual bool is_writebatch_trx() const = 0;

  virtual bool has_modifications() const = 0;

  virtual db::WriteBatchBase *get_indexed_write_batch() = 0;

  virtual void set_lock_timeout(int timeout_sec_arg) = 0;

  virtual void set_sync(bool sync) = 0;

  virtual bool has_prepared() { return m_prepared; }

  virtual void detach_from_se() = 0;

  void start_bulk_load(ha_smartengine *const bulk_load);

  void end_bulk_load(ha_smartengine *const bulk_load);

  int finish_bulk_load();

  int num_ongoing_bulk_load() const { return m_curr_bulk_load.size(); }

  /*
    Flush the data accumulated so far. This assumes we're doing a bulk insert.

    @detail
      This should work like transaction commit, except that we don't
      synchronize with the binlog (there is no API that would allow to have
      binlog flush the changes accumulated so far and return its current
      position)

    @todo
      Add test coverage for what happens when somebody attempts to do bulk
      inserts while inside a multi-statement transaction.
  */
  bool flush_batch();

  int set_status_error(THD *const thd,
                       const common::Status &s,
                       const SeKeyDef &kd,
                       SeTableDef *const tbl_def);

  THD *get_thd() const { return m_thd; }

  void set_params(THD *thd);

  void add_table_in_use() { ++m_n_mysql_tables_in_use; }

  void dec_table_in_use()
  {
    assert(m_n_mysql_tables_in_use > 0);
    --m_n_mysql_tables_in_use;
  }

  int64_t get_table_in_use() { return m_n_mysql_tables_in_use; }

  ulonglong get_write_count() const { return m_write_count; }

  int get_timeout_sec() const { return m_timeout_sec; }

  ulonglong get_lock_count() const { return m_lock_count; }

  /* if commit-in-middle happened, all data in writebatch has been committed,
   * and WBWI(WriteBatchWithIterator is invalid), we should not access it
   * anymore */
  void invalid_writebatch_iterator() { m_writebatch_iter_valid_flag = false; }

  /* if transaction commit/rollback from sever layer, valid writebatch iterator
   * agagin. */
  void reset_writebatch_iterator() { m_writebatch_iter_valid_flag = true; }

  bool is_writebatch_valid() { return m_writebatch_iter_valid_flag; }

  void snapshot_created(const db::Snapshot *const snapshot);

  bool has_snapshot() const { return m_read_opts.snapshot != nullptr; }

  void set_backup_running(const bool running) { m_backup_running = running; }

  bool get_backup_running() const { return m_backup_running; }

  /*
    Return a WriteBatch that one can write to. The writes will skip any
    transaction locking. The writes will NOT be visible to the transaction.
  */
  db::WriteBatchBase *get_blind_write_batch() { return get_indexed_write_batch()->GetWriteBatch(); }

  db::Iterator *get_iterator(db::ColumnFamilyHandle *const column_family,
                             bool skip_bloom_filter,
                             bool fill_cache,
                             bool read_current = false,
                             bool create_snapshot = true,
                             bool exclude_l2 = false,
                             bool unique_check = false);

  void set_tx_failed(bool failed_arg) { m_is_tx_failed = failed_arg; }

  bool can_prepare() const;

  int rollback_to_savepoint(void *const savepoint);

  /*
    This is used by transactions started with "START TRANSACTION WITH "
    "CONSISTENT [se] SNAPSHOT". When tx_read_only is turned on,
    snapshot has to be created via DB::GetSnapshot(), not via Transaction
    API.
  */
  bool is_tx_read_only() const { return m_tx_read_only; }

  bool is_two_phase() const { return m_is_two_phase; }

  void set_tx_read_only(bool val) { m_tx_read_only = val; }

  common::ReadOptions& get_read_opts() { return m_read_opts; } 

  static void init_mutex();

  static void term_mutex();

  static void walk_tx_list(SeTransListWalker *walker);

protected:
  // This should be used only when updating binlog information.
  virtual db::WriteBatchBase *get_write_batch() = 0;

  virtual bool commit_no_binlog() = 0;

  virtual bool commit_no_binlog2() = 0;

  virtual db::Iterator *get_iterator(const common::ReadOptions &options,
                                     db::ColumnFamilyHandle *column_family) = 0;

public:
  SeAsyncCommitCallback  async_call_back;

  const char *m_mysql_log_file_name;

  my_off_t m_mysql_log_offset;

  const char *m_mysql_gtid;

  const char *m_mysql_max_gtid;

  String m_detailed_error;

  int64_t m_snapshot_timestamp = 0;

  bool m_ddl_transaction;

protected:
  const size_t SE_MAX_ROW_SIZE = 32 * 1024 * 1024;

  ulonglong m_write_count = 0;

  ulonglong m_lock_count = 0;

  bool m_is_delayed_snapshot = false;

  bool m_is_two_phase = true;

  THD *m_thd = nullptr;

  common::ReadOptions m_read_opts;

  static std::multiset<SeTransaction *> s_tx_list;

  static mysql_mutex_t s_tx_list_mutex;

  bool m_tx_read_only = false;

  int m_timeout_sec; /* Cached value of @@se_lock_wait_timeout */

  /* Maximum number of locks the transaction can have */
  ulonglong m_max_row_locks;

  bool m_is_tx_failed = false;

  bool m_rollback_only = false;

  std::shared_ptr<SeSnapshotNotifier> m_notifier;

  /*
    Tracks the number of tables in use through external_lock.
    This should not be reset during start_tx().
  */
  int64_t m_n_mysql_tables_in_use = 0;

  bool m_prepared = false;

  bool m_backup_running = false;

  bool m_writebatch_iter_valid_flag = true;

private:
  // The tables we are currently loading.  In a partitioned table this can
  // have more than one entry
  std::vector<ha_smartengine *> m_curr_bulk_load;
};

} // namespace smartengine

#endif
