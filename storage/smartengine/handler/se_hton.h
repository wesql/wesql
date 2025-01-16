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

#ifndef SMARTENGINE_HTON_H_
#define SMARTENGINE_HTON_H_

#include "handler.h"
#include "dict/se_binlog_manager.h"
#include "dict/se_cf_manager.h"
#include "dict/se_ddl_manager.h"
#include "dict/se_dict_manager.h"
#include "logger/log_module.h"
#include "se_cf_options.h"
#include "se_dict_struct.h"
#include "util/se_utils.h"
#include "util/se_threads.h"
#include "backup/hotbackup.h"

class handler;
struct handlerton;
struct TABLE_SHARE;
struct MEM_ROOT;
class THD;

namespace smartengine
{
namespace util
{
class TransactionDB;
}

common::DBOptions se_init_se_db_options();

logger::InfoLogLevel get_se_log_level(ulong l);

extern const char *const se_hton_name;

extern handlerton *se_hton;

extern util::TransactionDB *se_db;

extern SeOpenTablesMap se_open_tables;

extern SeBackgroundThread se_bg_thread;

extern SeDropIndexThread se_drop_idx_thread;

extern SeRenewLeaseLockThread se_renewal_objstore_lease_lock_thread;

// TODO (Zhao Dongsheng) : In the original logic, se_collation_exceptions
// was used to list the table names that would skip collation checks(as follow
// comment), and there table names were configured through the system variable
// strict_collation_exceptions. However, in the current logic for smartengine,
// the supported characters sets are predefined, making this variable deprecated,
// and it is being considered for removal in the future.
// List of table names (using regex) that are exceptions to the strict
// collation check requirement.
extern Regex_list_handler *se_collation_exceptions;

extern std::shared_ptr<monitor::Statistics> se_stats;

extern std::shared_ptr<util::RateLimiter> se_rate_limiter;

extern SeDdlManager ddl_manager;

extern SeSubtableManager cf_manager;

extern SeDdlLogManager ddl_log_manager;

extern SeSubtableOptions se_cf_options_map;

extern SeDictionaryManager dict_manager;

extern SeBinlogManager binlog_manager;

extern int se_inplace_populate_indexes;

extern ulong purge_acquire_lock_timeout;

extern bool se_initialized;

std::vector<std::string> se_get_open_table_names(void);

void se_queue_save_stats_request();

/*
  Access to singleton objects.
*/

bool se_is_initialized();

util::TransactionDB *get_se_db();

SeSubtableManager &se_get_cf_manager();

table::BlockBasedTableOptions &se_get_table_options();

SeDictionaryManager *se_get_dict_manager();

SeDdlManager *se_get_ddl_manager();

SeBinlogManager *se_get_binlog_manager();

void se_flush_all_memtables();


bool print_stats(THD *const thd,
                 std::string const &type,
                 std::string const &name,
                 std::string const &status,
                 stat_print_fn *stat_print);

///////////////////////////////////////////////////////////////////////////////////////////

/*
  The following is needed as an argument for mysql_stage_register,
  irrespectively of whether we're compiling with P_S or not.
*/

#ifdef HAVE_PSI_INTERFACE
extern my_core::PSI_mutex_key se_psi_open_tbls_mutex_key, se_signal_bg_psi_mutex_key, se_signal_drop_idx_psi_mutex_key,
    se_collation_data_mutex_key, se_mem_cmp_space_mutex_key, key_mutex_tx_list, se_sysvars_psi_mutex_key,
    se_renewal_objstore_lease_lock_psi_mutex_key;

extern PSI_cond_key se_signal_bg_psi_cond_key, se_signal_drop_idx_psi_cond_key,
    se_renewal_objstore_lease_lock_psi_cond_key;

extern PSI_rwlock_key key_rwlock_collation_exception_list;

extern PSI_thread_key se_background_psi_thread_key;
extern PSI_thread_key se_drop_idx_psi_thread_key;
extern PSI_thread_key se_renewal_objstore_lease_lock_psi_thread_key;
extern PSI_rwlock_key key_rwlock_skip_unique_check_tables;
extern void init_se_psi_keys();
#endif

handler *se_create_handler(handlerton *const hton,
                           TABLE_SHARE *const table_arg,
                           bool partitioned,
                           MEM_ROOT *const mem_root);

int se_close_connection(handlerton *const hton, THD *const thd);

/**For a slave, prepare() updates the slave_gtid_info table which tracks the replication progress.*/
int se_prepare(handlerton *const hton, THD *const thd, bool prepare_tx);

/**Do nothing for prepare/commit by xid this is needed to avoid crashes in XA scenarios*/
xa_status_code se_commit_by_xid(handlerton *const hton, XID *const xid);

xa_status_code se_rollback_by_xid(handlerton *const hton, XID *const xid);

int se_recover_tc(handlerton *hton, Xa_state_list &xa_list);

/**Reading last committed binary log info from SE system row. The info is needed for crash safe
slave/master to work.*/
int se_recover(handlerton *hton,
               XA_recover_txn *txn_list,
               uint len,
               MEM_ROOT *mem_root);

int se_commit(handlerton *const hton, THD *const thd, bool commit_tx);

int se_rollback(handlerton *const hton, THD *const thd, bool rollback_tx);

/**This is called for SHOW ENGINE se STATUS|LOGS|etc.For now, produce info
about live files (which gives an imprecise idea about what column families are there)*/

bool se_show_status(handlerton *const hton,
                    THD *const thd,
                    stat_print_fn *const stat_print,
                    enum ha_stat_type stat_type);

/**Supporting START TRANSACTION WITH CONSISTENT [se] SNAPSHOT
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
InnoDB and se transactions.*/
int se_start_tx_and_assign_read_view(handlerton *const hton, THD *const thd);

/**Dummy SAVEPOINT support. This is needed for long running transactions
like mysqldump (https://bugs.mysql.com/bug.php?id=71017). Current SAVEPOINT
does not correctly handle ROLLBACK and does not return errors. This needs
to be addressed in future versions (Issue#96).*/
int se_savepoint(handlerton *const hton, THD *const thd, void *const savepoint);

int se_rollback_to_savepoint(handlerton *const hton, THD *const thd, void *const savepoint);

bool se_rollback_to_savepoint_can_release_mdl(handlerton *const hton, THD *const thd);

/**Called by hton->flush_logs after MySQL group commit prepares a set of transactions.*/
bool se_flush_wal(handlerton *const hton, bool binlog_group_flush);

/**Copy from innodb.
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
                                  bool is_sql_layer_system_table);

void se_post_recover();
void se_post_engine_recover();

void se_post_ddl(THD *thd);

int se_checkpoint(THD *thd);

bool se_ddse_dict_init(dict_init_mode_t dict_init_mode,
                       uint version,
                       List<const dd::Object_table> *ddse_tables,
                       List<const Plugin_tablespace> *ddse_tablespaces);

bool se_dict_recover(dict_recovery_mode_t dict_recovery_mode, uint version);

bool se_dict_set_server_version();

bool se_dict_get_server_version(uint *version);

bool se_is_dict_readonly();

void se_dict_register_dd_table_id(dd::Object_id table_id);

void se_dict_cache_reset_tables_and_tablespaces();

int se_create_backup_snapshot(THD *thd,
                              uint64_t *backup_snapshot_id,
                              std::string &binlog_file,
                              uint64_t *binlog_file_offset);

int se_incremental_backup(THD *thd);

int se_cleanup_tmp_backup_dir(THD *thd);

int se_release_backup_snapshot(THD *thd, uint64_t backup_snapshot_id);

int se_list_backup_snapshots(THD *thd, std::vector<uint64_t> &backup_ids);

} // namespace smartengine

#endif // end of SMARTENGINE_HTON_H_
