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

#include "se_hton.h"
#include "binlog.h"
#include "mysql/psi/mysql_stage.h"
#include "se_status_vars.h"
#include "se_system_vars.h"
#include "transaction/se_transaction.h"
#include "mysqld_error.h"
#include "mysql/plugin.h"
#include "debug_sync.h"
#include "sql_class.h"
#include "query_options.h"
#include "ha_smartengine.h"
#include "db/db_impl.h"
#include "transaction/se_transaction_factory.h"
#include "transactions/transaction_db_impl.h"
#include "core/util/memory_stat.h"

namespace smartengine
{
handlerton *se_hton = nullptr;

const char *const se_hton_name = SMARTENGINE_NAME;

util::TransactionDB *se_db = nullptr;

SeOpenTablesMap se_open_tables;

SeBackgroundThread se_bg_thread;

SeDropIndexThread se_drop_idx_thread;

SeRenewLeaseLockThread se_renewal_objstore_lease_lock_thread;

Regex_list_handler *se_collation_exceptions;

std::shared_ptr<monitor::Statistics> se_stats;

std::shared_ptr<util::RateLimiter> se_rate_limiter;

SeDdlManager ddl_manager;

SeSubtableOptions se_cf_options_map;

SeDictionaryManager dict_manager;

SeBinlogManager binlog_manager;

SeSubtableManager cf_manager;

SeDdlLogManager ddl_log_manager;

//TODO yxian atomic
// how many inplace populate indexes
int se_inplace_populate_indexes = 0;

ulong purge_acquire_lock_timeout = 30; // seconds

bool se_initialized = false;

std::vector<std::string> se_get_open_table_names(void)
{
  return se_open_tables.get_table_names();
}

void se_queue_save_stats_request() { se_bg_thread.request_save_stats(); }

bool se_is_initialized() { return se_initialized;  }

util::TransactionDB *get_se_db() { return se_db; }

SeSubtableManager &se_get_cf_manager() { return cf_manager; }

table::BlockBasedTableOptions &se_get_table_options() { return se_tbl_options; }

SeDictionaryManager *se_get_dict_manager(void) { return &dict_manager; }

SeDdlManager *se_get_ddl_manager(void) { return &ddl_manager; }

SeBinlogManager *se_get_binlog_manager(void) { return &binlog_manager; }

void se_flush_all_memtables()
{
  const SeSubtableManager &cf_manager = se_get_cf_manager();
  std::unique_ptr<db::ColumnFamilyHandle> cf_ptr;
  for (const auto &cf_handle : cf_manager.get_all_cf()) {
    cf_ptr.reset(cf_handle);
    se_db->Flush(common::FlushOptions(), cf_handle);
  }
}

bool print_stats(THD *const thd,
                 std::string const &type,
                 std::string const &name,
                 std::string const &status,
                 stat_print_fn *stat_print)
{
  return stat_print(thd, type.c_str(), type.size(),
      name.c_str(), name.size(), status.c_str(), status.size());
}

/*
  The following is needed as an argument for mysql_stage_register,
  irrespectively of whether we're compiling with P_S or not.
*/
static PSI_stage_info stage_waiting_on_row_lock = {0, "Waiting for row lock", 0};

#ifdef HAVE_PSI_INTERFACE
PSI_thread_key se_background_psi_thread_key;
PSI_thread_key se_drop_idx_psi_thread_key;
PSI_thread_key se_renewal_objstore_lease_lock_psi_thread_key;

static PSI_stage_info *all_se_stages[] = {&stage_waiting_on_row_lock};

my_core::PSI_mutex_key se_psi_open_tbls_mutex_key, se_signal_bg_psi_mutex_key, se_signal_drop_idx_psi_mutex_key,
    se_collation_data_mutex_key, se_mem_cmp_space_mutex_key, key_mutex_tx_list, se_sysvars_psi_mutex_key,
    se_renewal_objstore_lease_lock_psi_mutex_key;

static PSI_mutex_info all_se_mutexes[] = {
    {&se_psi_open_tbls_mutex_key, "open tables", PSI_FLAG_SINGLETON},
    {&se_signal_bg_psi_mutex_key, "stop background", PSI_FLAG_SINGLETON},
    {&se_signal_drop_idx_psi_mutex_key, "signal drop index", PSI_FLAG_SINGLETON},
    {&se_collation_data_mutex_key, "collation data init", PSI_FLAG_SINGLETON},
    {&se_mem_cmp_space_mutex_key, "collation space char data init", PSI_FLAG_SINGLETON},
    {&key_mutex_tx_list, "tx_list", PSI_FLAG_SINGLETON},
    {&se_sysvars_psi_mutex_key, "setting sysvar", PSI_FLAG_SINGLETON},
    {&se_renewal_objstore_lease_lock_psi_mutex_key, "renewal objstore lease lock", PSI_FLAG_SINGLETON},
};

PSI_rwlock_key key_rwlock_collation_exception_list;
static PSI_rwlock_key key_rwlock_read_free_rpl_tables;
PSI_rwlock_key key_rwlock_skip_unique_check_tables;

static PSI_rwlock_info all_se_rwlocks[] = {
    {&key_rwlock_collation_exception_list, "collation_exception_list",
     PSI_FLAG_SINGLETON},
    {&key_rwlock_read_free_rpl_tables, "read_free_rpl_tables", PSI_FLAG_SINGLETON},
    {&key_rwlock_skip_unique_check_tables, "skip_unique_check_tables",
     PSI_FLAG_SINGLETON},
};

PSI_cond_key se_signal_bg_psi_cond_key, se_signal_drop_idx_psi_cond_key, se_renewal_objstore_lease_lock_psi_cond_key;

static PSI_cond_info all_se_conds[] = {
    {&se_signal_bg_psi_cond_key, "cond signal background", PSI_FLAG_SINGLETON},
    {&se_signal_drop_idx_psi_cond_key, "cond signal drop index", PSI_FLAG_SINGLETON},
    {&se_renewal_objstore_lease_lock_psi_cond_key, "cond signal renewal objstore lease lock", PSI_FLAG_SINGLETON},
};

static PSI_thread_info all_se_threads[] = {
    {&se_background_psi_thread_key, "background", "background", PSI_FLAG_SINGLETON},
    {&se_drop_idx_psi_thread_key, "drop index", "drop_index", PSI_FLAG_SINGLETON},
    {&se_renewal_objstore_lease_lock_psi_thread_key, "renewal lease", "renewal_lease", PSI_FLAG_SINGLETON},
};

void init_se_psi_keys()
{
  const char *const category = "se";
  int count;

  count = array_elements(all_se_mutexes);
  mysql_mutex_register(category, all_se_mutexes, count);

  count = array_elements(all_se_rwlocks);
  mysql_rwlock_register(category, all_se_rwlocks, count);

  count = array_elements(all_se_conds);
  // TODO Disabling PFS for conditions due to the bug
  // https://github.com/MySQLOnRocksDB/mysql-5.6/issues/92
  // PSI_server->register_cond(category, all_se_conds, count);

  count = array_elements(all_se_stages);
  mysql_stage_register(category, all_se_stages, count);

  count = array_elements(all_se_threads);
  mysql_thread_register(category, all_se_threads, count);
}
#endif

common::DBOptions se_init_se_db_options()
{
  common::DBOptions o;
  return o;
}

logger::InfoLogLevel get_se_log_level(ulong l)
{
  switch (l) {
    case 0 /* SYSTEM_LEVEL */:
      return logger::InfoLogLevel::SYSTEM_LEVEL;
    case 1 /* ERROR_LEVEL */:
      return logger::InfoLogLevel::ERROR_LEVEL;
    case 2 /* WARNING_LEVEL */:
      return logger::InfoLogLevel::WARN_LEVEL;
    case 3 /* INFORMATION_LEVEL */:
      return logger::InfoLogLevel::INFO_LEVEL;
    default:
      return logger::InfoLogLevel::DEBUG_LEVEL;
  }
  return logger::InfoLogLevel::INFO_LEVEL;
}

handler *se_create_handler(handlerton *const hton,
                           TABLE_SHARE *const table_arg,
                           bool partitioned,
                           MEM_ROOT *const mem_root)
{
  if (partitioned) {
    //TODO we should implement partion in the future
    my_printf_error(ER_NOT_SUPPORTED_YET,
        "Create partitioned table is not supported yet in se.", MYF(0));
    return nullptr;
  }
  return new (mem_root) ha_smartengine(hton, table_arg);
}

int se_close_connection(handlerton *const hton, THD *const thd)
{
  SeTransaction *&tx = get_tx_from_thd(thd);
  if (tx != nullptr) {
    int rc = tx->finish_bulk_load();
    if (rc != 0) {
      // NO_LINT_DEBUG
      sql_print_error("SE: Error %d finalizing last SST file while "
                      "disconnecting",
                      rc);
      abort_with_stack_traces();
    }

    delete tx;
    tx = nullptr;
  }
  return HA_EXIT_SUCCESS;
}

/**
  For a slave, prepare() updates the slave_gtid_info table which tracks the
  replication progress.
*/
//TODO disable async temporaly we will fix it in the future
int se_prepare(handlerton *const hton, THD *const thd, bool prepare_tx)
{
  SeTransaction *&tx = get_tx_from_thd(thd);
  if (!tx->can_prepare()) {
    return HA_EXIT_FAILURE;
  }
  if (DBUG_EVALUATE_IF("simulate_xa_failure_prepare_in_engine", 1, 0)) {
    return HA_EXIT_FAILURE;
  }
  if (prepare_tx ||
      (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {
    /* We were instructed to prepare the whole transaction, or
    this is an SQL statement end and autocommit is on */
    //COMMENT GTID temporarily
    /*
    std::vector<st_slave_gtid_info> slave_gtid_info;
    thd_slave_gtid_info(thd, &slave_gtid_info);
    for (const auto &it : slave_gtid_info) {
      db::WriteBatchBase *const write_batch = tx->get_blind_write_batch();
      binlog_manager.update_slave_gtid_info(it.id, it.db, it.gtid, write_batch);
    }
    */

    if (tx->is_two_phase()) {
      //if (thd->durability_property == HA_IGNORE_DURABILITY || async) {
      if (thd->durability_property == HA_IGNORE_DURABILITY) {
        tx->set_sync(false);
      }
      XID xid;
      thd_get_xid(thd, reinterpret_cast<MYSQL_XID *>(&xid));
      if (!tx->prepare(se_xid_to_string(xid))) {
        return HA_EXIT_FAILURE;
      }
      if (thd->durability_property == HA_IGNORE_DURABILITY &&
          se_flush_log_at_trx_commit == 1) {
      }
    }

    DEBUG_SYNC(thd, "se.prepared");
  }

  return HA_EXIT_SUCCESS;
}

/**
 do nothing for prepare/commit by xid
 this is needed to avoid crashes in XA scenarios
*/
xa_status_code se_commit_by_xid(handlerton *const hton, XID *const xid)
{
  DBUG_ENTER_FUNC();

  assert(hton != nullptr);
  assert(xid != nullptr);

  const auto name = se_xid_to_string(*xid);
  util::Transaction *const trx = se_db->GetTransactionByName(name);
  if (trx == nullptr) {
    DBUG_RETURN(XAER_NOTA);
  }
  const common::Status s = trx->Commit();
  if (!s.ok()) {
    DBUG_RETURN(XAER_RMERR);
  }
  delete trx;
  DBUG_RETURN(XA_OK);
}

int se_savepoint(handlerton *const hton, THD *const thd, void *const savepoint)
{
  return HA_EXIT_SUCCESS;
}

bool se_rollback_to_savepoint_can_release_mdl(handlerton *const hton, THD *const thd)
{
  return true;
}

xa_status_code se_rollback_by_xid(handlerton *const hton, XID *const xid)
{
  DBUG_ENTER_FUNC();

  assert(hton != nullptr);
  assert(xid != nullptr);

  const auto name = se_xid_to_string(*xid);

  util::Transaction *const trx = se_db->GetTransactionByName(name);

  if (trx == nullptr) {
    DBUG_RETURN(XAER_NOTA);
  }
  const common::Status s = trx->Rollback();

  if (!s.ok()) {
    DBUG_RETURN(XAER_RMERR);
  }
  delete trx;
  DBUG_RETURN(XA_OK);
}

int se_recover_tc(handlerton *hton, Xa_state_list &xa_list)
{
  DBUG_ENTER_FUNC();

  assert(hton != nullptr);

  std::vector<util::Transaction *> trans_list;
  se_db->GetAllPreparedTransactions(&trans_list);

  XID id;
  for (auto &trans : trans_list) {
    auto name = trans->GetName();
    se_xid_from_string(name, &id);
    xa_list.add(id, enum_ha_recover_xa_state::PREPARED_IN_SE);
  }

  DBUG_RETURN(XA_OK);
}

/**
  Reading last committed binary log info from SE system row.
  The info is needed for crash safe slave/master to work.
*/
int se_recover(handlerton *hton,
               XA_recover_txn *txn_list,
               uint len,
               MEM_ROOT *mem_root)
{
  if (len == 0 || txn_list == nullptr) {
    return HA_EXIT_SUCCESS;
  }

  std::vector<util::Transaction *> trans_list;
  se_db->GetAllPreparedTransactions(&trans_list);

  uint count = 0;
  for (auto &trans : trans_list) {
    if (count >= len) {
      break;
    }
    auto name = trans->GetName();
    se_xid_from_string(name, &(txn_list[count].id));

    txn_list[count].mod_tables = new (mem_root) List<st_handler_tablename>();
    if (!txn_list[count].mod_tables) break;

    count++;
  }
  return count;
}

int se_commit(handlerton *const hton, THD *const thd, bool commit_tx)
{
  DBUG_ENTER_FUNC();

  assert(hton != nullptr);
  assert(thd != nullptr);

  /* note: h->external_lock(F_UNLCK) is called after this function is called) */
  SeTransaction *&tx = get_tx_from_thd(thd);

  if (tx != nullptr) {
    if (commit_tx || (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT |
                                                          OPTION_BEGIN))) {
      /*
        We get here
         - For a COMMIT statement that finishes a multi-statement transaction
         - For a statement that has its own transaction
      */
      bool ret = tx->commit();
      //TODO we will fix async commit later
      //if (thd->async_commit) {
      //  thd->async_commit = true;
      //  DBUG_RETURN(HA_EXIT_SUCCESS);
      //}

      // TODO: Leader thread put all followers to async queue

      if (ret)
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    } else {
      /*
        We get here when committing a statement within a transaction.

        We don't need to do anything here. tx->start_stmt() will notify
        SeTransactionImpl that another statement has started.
      */
      tx->set_tx_failed(false);
    }

    if (thd_tx_isolation(thd) <= ISO_READ_COMMITTED) {
      // For READ_COMMITTED, we release any existing snapshot so that we will
      // see any changes that occurred since the last statement.
      tx->release_snapshot();
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int se_rollback(handlerton *const hton, THD *const thd, bool rollback_tx)
{
  SeTransaction *&tx = get_tx_from_thd(thd);

  if (tx != nullptr) {
    if (rollback_tx) {
      /*
        We get here, when
        - ROLLBACK statement is issued.

        Discard the changes made by the transaction
      */
      tx->rollback();
    } else {
      /*
        We get here when
        - a statement with AUTOCOMMIT=1 is being rolled back (because of some
          error)
        - a statement inside a transaction is rolled back
      */

      tx->rollback_stmt();
      tx->set_tx_failed(true);
    }

    if (thd_tx_isolation(thd) <= ISO_READ_COMMITTED) {
      // For READ_COMMITTED, we release any existing snapshot so that we will
      // see any changes that occurred since the last statement.
      tx->release_snapshot();
    }
  }
  return HA_EXIT_SUCCESS;
}

/*
  This is called for SHOW ENGINE se STATUS|LOGS|etc.

  For now, produce info about live files (which gives an imprecise idea about
  what column families are there)
*/
bool se_show_status(handlerton *const hton,
                    THD *const thd,
                    stat_print_fn *const stat_print,
                    enum ha_stat_type stat_type)
{
  bool res = false;
  if (stat_type == HA_ENGINE_STATUS) {
    std::string str;

    /* Per DB stats */
    if (se_db->GetProperty("smartengine.dbstats", &str)) {
      res |= print_stats(thd, "DBSTATS", SE_STATUS_VAR_PREFIX, str, stat_print);
    }

    /* Memory Statistics */
    std::vector<db::DB *> dbs;
    std::unordered_set<const cache::Cache *> cache_set;
    size_t kDefaultInternalCacheSize = 8 * 1024 * 1024;
    char buf[100];

    dbs.push_back(se_db);
    cache_set.insert(se_tbl_options.block_cache.get());
    std::unique_ptr<db::ColumnFamilyHandle> cf_ptr;
    for (const auto &cf_handle : cf_manager.get_all_cf()) {
      cf_ptr.reset(cf_handle);
      db::ColumnFamilyDescriptor cf_desc;
      cf_handle->GetDescriptor(&cf_desc);
      auto *const table_factory = cf_desc.options.table_factory.get();
      if (table_factory != nullptr) {
        std::string tf_name = table_factory->Name();
        if (tf_name.find("BlockBasedTable") != std::string::npos) {
          const table::BlockBasedTableOptions *const bbt_opt =
              reinterpret_cast<table::BlockBasedTableOptions *>(
                  table_factory->GetOptions());
          if (bbt_opt != nullptr) {
            if (bbt_opt->block_cache.get() != nullptr) {
              cache_set.insert(bbt_opt->block_cache.get());
            }
            cache_set.insert(bbt_opt->block_cache_compressed.get());
          }
        }
      }
    }
    std::map<util::MemoryStat::MemoryStatType, uint64_t> temp_stat_by_type;
    util::MemoryStat::GetApproximateMemoryStatByType(dbs, temp_stat_by_type);
    str.clear();
    snprintf(buf, sizeof(buf), "\nActiveMemTableTotalNumber: %lu",
            temp_stat_by_type[util::MemoryStat::kActiveMemTableTotalNumber]);
    str.append(buf);
    snprintf(buf, sizeof(buf), "\nActiveMemTableTotalMemoryAllocated: %lu",
            temp_stat_by_type[util::MemoryStat::kActiveMemTableTotalMemoryAllocated]);
    str.append(buf);
    snprintf(buf, sizeof(buf), "\nActiveMemTableTotalUsed: %lu",
            temp_stat_by_type[util::MemoryStat::kActiveMemTableTotalMemoryUsed]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nUnflushedImmTableTotalNumber: %lu",
            temp_stat_by_type[util::MemoryStat::kUnflushedImmTableTotalNumber]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nUnflushedImmTableTotalMemoryAllocated: %lu",
            temp_stat_by_type[util::MemoryStat::kUnflushedImmTableTotalMemoryAllocated]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nUnflushedImmTableTotalMemoryUsed: %lu",
            temp_stat_by_type[util::MemoryStat::kUnflushedImmTableTotalMemoryUsed]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nTableReaderTotalNumber: %lu",
            temp_stat_by_type[util::MemoryStat::kTableReaderTotalNumber]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nTableReaderTotalMemoryUsed: %lu",
            temp_stat_by_type[util::MemoryStat::kTableReaderTotalMemoryUsed]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nBlockCacheTotalPinnedMemory: %lu",
            temp_stat_by_type[util::MemoryStat::kBlockCacheTotalPinnedMemory]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nBlockCacheTotalMemoryUsed: %lu",
            temp_stat_by_type[util::MemoryStat::kBlockCacheTotalMemoryUsed]);
    str.append(buf);
    snprintf(buf, sizeof(buf), "\nDBTotalMemoryAllocated: %lu",
            temp_stat_by_type[util::MemoryStat::kDBTotalMemoryAllocated]);
    str.append(buf);
    res |= print_stats(thd, "Memory_Stats", SE_STATUS_VAR_PREFIX, str, stat_print);
  }

  return res;
}

/*
    Supporting START TRANSACTION WITH CONSISTENT [se] SNAPSHOT

    Features:
    1. Supporting START TRANSACTION WITH CONSISTENT SNAPSHOT
    2. Getting current binlog position in addition to #1.

    The second feature is done by START TRANSACTION WITH
    CONSISTENT se SNAPSHOT. This is Facebook's extension, and
    it works like existing START TRANSACTION WITH CONSISTENT INNODB SNAPSHOT.

    - When not setting engine, START TRANSACTION WITH CONSISTENT SNAPSHOT
    takes both InnoDB and se snapshots, and both InnoDB and se
    participate in transaction. When executing COMMIT, both InnoDB and
    se modifications are committed. Remember that XA is not supported yet,
    so mixing engines is not recommended anyway.

    - When setting engine, START TRANSACTION WITH CONSISTENT.. takes
    snapshot for the specified engine only. But it starts both
    InnoDB and se transactions.
*/
int se_start_tx_and_assign_read_view(handlerton *const hton, THD *const thd)
{

  ulong const tx_isolation = thd_tx_isolation(thd);

  if (tx_isolation != ISO_REPEATABLE_READ) {
    // here just a warning, do not return error
    push_warning_printf(thd, Sql_condition::SL_WARNING, HA_ERR_UNSUPPORTED,
                        "Only REPEATABLE READ isolation level is supported "
                        "for START TRANSACTION WITH CONSISTENT SNAPSHOT "
                        "in SE Storage Engine. "
                        "Snapshot has not been taken");
  }

  SeTransaction *const tx = get_or_create_tx(thd);
  assert(!tx->has_snapshot());
  tx->set_tx_read_only(true);
  se_register_tx(hton, thd, tx);
  tx->acquire_snapshot(true);

  monitor::QueryPerfContext::opt_print_slow_ = se_query_trace_print_slow;
  monitor::QueryPerfContext::opt_threshold_time_ = se_query_trace_threshold_time;
  return HA_EXIT_SUCCESS;
}

int se_rollback_to_savepoint(handlerton *const hton,
                             THD *const thd,
                             void *const savepoint)
{
  SeTransaction *&tx = get_tx_from_thd(thd);
  return tx->rollback_to_savepoint(savepoint);
}

/**
  Called by hton->flush_logs after MySQL group commit prepares a set of
  transactions.
*/
bool se_flush_wal(handlerton *const hton, bool binlog_group_flush)
{
  assert(se_db != nullptr);
  if (binlog_group_flush && se_flush_log_at_trx_commit == 0) {
    return HA_EXIT_SUCCESS;
  }
  global_stats.wal_group_syncs_++;
  if (Status::kOk != se_db->sync_wal()) {
    return HA_EXIT_FAILURE;
  }
  return HA_EXIT_SUCCESS;
}

/*
 Copy from innodb.
 This function checks if the given db.tablename is a system table
 supported by Innodb and is used as an initializer for the data member
 is_supported_system_table of se storage engine handlerton.
 Currently we support only plugin, servers,  help- and time_zone- related
 system tables in se. Please don't add any SE-specific system tables here.

 @param db                            database name to check.
 @param table_name                    table name to check.
 @param is_sql_layer_system_table     if the supplied db.table_name is a SQL
                                      layer system table.
*/
bool se_is_supported_system_table(const char *db,
                                  const char *table_name,
                                  bool is_sql_layer_system_table)
{
  static const char* supported_system_tables[]= {
#ifdef WESQL_CLUSTER
                                                  "consensus_info",
                                                  "consensus_applier_info",
                                                  "consensus_applier_worker",
#endif
                                                  "slave_relay_log_info",
                                                  "slave_master_info",
                                                  "slave_worker_info",
                                                  "gtid_executed",
                                                  "help_topic",
                                                  "help_category",
                                                  "help_relation",
                                                  "help_keyword",
                                                  "plugin",
                                                  "servers",
                                                  "time_zone",
                                                  "time_zone_leap_second",
                                                  "time_zone_name",
                                                  "time_zone_transition",
                                                  "time_zone_transition_type",
                                                  (const char *)NULL };

  if (!is_sql_layer_system_table)
    return false;

  for (unsigned i= 0; supported_system_tables[i] != NULL; ++i)
  {
    if (!strcmp(table_name, supported_system_tables[i]))
      return true;
  }

  return false;
}

void se_post_recover() { ddl_log_manager.recover(); }

void se_post_engine_recover() {
  util::TransactionDBImpl *txn_db_impl;
  LOG_INFO log_info;

  txn_db_impl = dynamic_cast<util::TransactionDBImpl *>(se_db);

  if (mysql_bin_log.is_open()) {
    mysql_bin_log.get_current_log(&log_info);
    size_t dir_len = dirname_length(log_info.log_file_name);
    SE_LOG(INFO, "initialize se global binlog position", K(log_info.log_file_name), K((uint64_t)log_info.pos));
    txn_db_impl->GetDBImpl()->set_global_binlog_pos(log_info.log_file_name + dir_len, log_info.pos);
  }
}

void se_post_ddl(THD *thd) { ddl_log_manager.post_ddl(thd); }

int se_checkpoint(THD *thd) {
  int ret = Status::kOk;
  util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();
  if (FAILED(backup_instance->lock_one_step())) {
    SE_LOG(WARN, "fail to accquire exclusive lock", K(ret));
  } else if (FAILED(backup_instance->do_checkpoint(se_db))) {
    my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to do checkpoint for backup", MYF(0));
  } else {
    backup_instance->set_backup_status(se_backup_status[0]);
  }

  if (Status::kInitTwice != ret) {
    backup_instance->unlock_one_step();
  }

  if (FAILED(ret)) {
    ret = HA_ERR_INTERNAL_ERROR;
  }
  return ret;
}

int se_create_backup_snapshot(THD *thd,
                              uint64_t *backup_snapshot_id,
                              std::string &binlog_file,
                              uint64_t *binlog_file_offset)
{
  int ret = 0;
  const char* backup_status = nullptr;
  util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();
  db::BinlogPosition binlog_pos;

  if (IS_NULL(backup_snapshot_id)) {
    ret = Status::kInvalidArgument;
    my_printf_error(ER_UNKNOWN_ERROR, "SE: backup_snapshot_id is null\n", MYF(0));
  } else if (IS_NULL(binlog_file_offset)) {
    ret = Status::kInvalidArgument;
    my_printf_error(ER_UNKNOWN_ERROR, "SE: binlog_file_offset is null\n", MYF(0));
  } else if (FAILED(backup_instance->lock_one_step())) {
    SE_LOG(WARN, "fail to accquire exclusive lock", K(ret));
    // TODO(ljc):
    // 1. add an argument in handler interface to judge whether we need do checkpoint.
    // 2. combine se_checkpoint and se_create_backup_snapshot to one handler interface.
  } else {
    if (SUCCED(backup_instance->get_backup_status(backup_status)) && backup_status != se_backup_status[0]) {
      // server layer can accuire backup snapshot without doing checkpoint, if so, we should init backup snapshot here
      if (FAILED(backup_instance->init(se_db))) {
        SE_LOG(WARN, "Failed to init backup snapshot", K(ret));
      } else if (FAILED(backup_instance->create_tmp_dir(se_db))) {
        SE_LOG(WARN, "Failed to create tmp dir for backup", K(ret));
      }
    }
    if (SUCCED(ret)) {
      if (FAILED(backup_instance->accquire_backup_snapshot(se_db, backup_snapshot_id, binlog_pos))) {
        my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to acquire snapshots for backup", MYF(0));
      } else {
        binlog_file = binlog_pos.file_name_;
        *binlog_file_offset = binlog_pos.offset_;
        backup_instance->set_backup_status(se_backup_status[1]);
      }
    }
  }

  if (Status::kInitTwice != ret) {
    backup_instance->unlock_one_step();
  }

  if (FAILED(ret)) {
    ret = HA_ERR_INTERNAL_ERROR;
  }
  return ret;
}

int se_incremental_backup(THD *thd) {
  int ret = 0;
  const char *backup_status = nullptr;
  util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();

  if (FAILED(backup_instance->lock_one_step())) {
    SE_LOG(WARN, "fail to accquire exclusive lock", K(ret));
  } else if (SUCCED(backup_instance->get_backup_status(backup_status)) &&
             0 != strcasecmp(backup_status, se_backup_status[1])) {
    ret = Status::kInvalidArgument;
    my_printf_error(ER_UNKNOWN_ERROR,
                    "SE: should execute command: %s before this command\n",
                    MYF(0),
                    se_backup_status[1]);
  } else if (FAILED(backup_instance->record_incremental_extent_ids(se_db))) {
    my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to record incremental extent ids for backup", MYF(0));
  } else {
    backup_instance->set_backup_status(se_backup_status[2]);
  }

  if (Status::kInitTwice != ret) {
    backup_instance->unlock_one_step();
  }

  if (FAILED(ret)) {
    ret = HA_ERR_INTERNAL_ERROR;
  }
  return ret;
}

int se_cleanup_tmp_backup_dir(THD *thd)
{
  int ret = 0;

  util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();
  if (FAILED(backup_instance->lock_one_step())) {
    SE_LOG(WARN, "fail to accquire exclusive lock", K(ret));
  } else if (FAILED(backup_instance->cleanup_tmp_dir(se_db))) {
    my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to cleanup tmp dir for backup", MYF(0));
  } else {
    // do nothing
  }

  if (Status::kInitTwice != ret) {
    backup_instance->unlock_one_step();
  }
  return ret;
}

int release_current_backup_snapshot(THD *thd, uint64_t backup_snapshot_id) {
  int ret = Status::kOk;
  util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();

  if (backup_snapshot_id == 0) {
    ret = Status::kInvalidArgument;
    my_printf_error(ER_UNKNOWN_ERROR, "SE: backup_snapshot_id is 0\n", MYF(0));
  } else if (backup_snapshot_id == backup_instance->current_backup_id()) {
    if (FAILED(backup_instance->release_current_backup_snapshot(se_db))) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to release backup snapshot, backup id: %ld", MYF(0),
                      backup_snapshot_id);
    } else {
      backup_instance->set_backup_status(se_backup_status[4]);
    }
  }

  return ret;
}

int se_release_backup_snapshot(THD *thd, uint64_t backup_snapshot_id) {
  int ret = 0;
  util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();

  if (FAILED(backup_instance->lock_one_step())) {
    SE_LOG(WARN, "fail to accquire exclusive lock", K(ret));
  } else if (backup_snapshot_id == 0) {
    ret = Status::kErrorUnexpected;
    my_printf_error(ER_UNKNOWN_ERROR, "SE: backup_snapshot_id is null or 0\n", MYF(0));
  } else if (backup_snapshot_id != backup_instance->current_backup_id()) {
    // release old backup snapshot
    if (FAILED(backup_instance->release_old_backup_snapshot(se_db, backup_snapshot_id))) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to release backup snapshot, backup id: %ld", MYF(0),
                      backup_snapshot_id);
    }
  } else if (FAILED(release_current_backup_snapshot(thd, backup_snapshot_id))) {
    SE_LOG(WARN, "fail to release current backup snapshot", K(ret));
  } else {
    SE_LOG(INFO, "success to release current backup snapshot", K(backup_snapshot_id));
  }

  if (Status::kInitTwice != ret) {
    backup_instance->unlock_one_step();
  }

  if (FAILED(ret)) {
    ret = HA_ERR_INTERNAL_ERROR;
  }
  return ret;
}

int se_list_backup_snapshots(THD *thd, std::vector<uint64_t> &backup_ids) {
  util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();
  backup_instance->list_backup_snapshots(backup_ids);
  return 0;
}

} // namespace smartengine