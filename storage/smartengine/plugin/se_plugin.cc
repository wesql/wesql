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

#include <string_view>

#include "core/cache/row_cache.h"
#include "core/options/options_helper.h"
#include "core/util/sync_point.h"
#include "db_impl.h"
#include "dict/se_charset_info.h"
#include "dict/se_dict_util.h"
#include "ha_smartengine.h"
#include "mysql/plugin.h"
#include "mysqld.h"
#include "objstore/lease_lock.h"
#include "se_api.h"
#include "se_hton.h"
#include "se_i_s.h"
#include "se_status_vars.h"
#include "se_system_vars.h"
#include "transaction/se_transaction.h"
#include "transactions/transaction_db.h"
#include "util/rate_limiter.h"
#include "util/se_logger.h"
#include "util/se_mutex_wrapper.h"

// Internal MySQL APIs not exposed in any header.
extern "C" {
se_cb_t se_api_cb[] = { SE_API_FUNCS };
}

namespace smartengine
{
using namespace util;

/**Storage Engine initialization function, invoked when plugin is loaded.*/
static int se_init_func(void *const p)
{
  DBUG_ENTER_FUNC();

  // Validate the assumption about the size of SE_SIZEOF_HIDDEN_PK_COLUMN.
  static_assert(sizeof(longlong) == 8, "Assuming that longlong is 8 bytes.");

#ifdef HAVE_PSI_INTERFACE
  init_se_psi_keys();
#endif

  se_hton = (handlerton *)p;
  mysql_mutex_init(se_psi_open_tbls_mutex_key, &se_open_tables.m_mutex,
                   MY_MUTEX_INIT_FAST);
#ifdef HAVE_PSI_INTERFACE
  se_bg_thread.init(se_signal_bg_psi_mutex_key, se_signal_bg_psi_cond_key);
  se_drop_idx_thread.init(se_signal_drop_idx_psi_mutex_key,
                           se_signal_drop_idx_psi_cond_key);
  se_renewal_objstore_lease_lock_thread.init(se_renewal_objstore_lease_lock_psi_mutex_key,
                                             se_renewal_objstore_lease_lock_psi_cond_key);
#else
  se_bg_thread.init();
  se_drop_idx_thread.init();
#endif
  mysql_mutex_init(se_collation_data_mutex_key, &se_collation_data_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(se_mem_cmp_space_mutex_key, &se_mem_cmp_space_mutex,
                   MY_MUTEX_INIT_FAST);

#if defined(HAVE_PSI_INTERFACE)
  se_collation_exceptions =
      new Regex_list_handler(key_rwlock_collation_exception_list);
#else
  se_collation_exceptions = new Regex_list_handler();
#endif

  mysql_mutex_init(se_sysvars_psi_mutex_key, &se_sysvars_mutex,
                   MY_MUTEX_INIT_FAST);
  SeTransaction::init_mutex();

  se_hton->state = SHOW_OPTION_YES;
  se_hton->create = se_create_handler;
  se_hton->close_connection = se_close_connection;
  se_hton->prepare = se_prepare;
  se_hton->commit_by_xid = se_commit_by_xid;
  se_hton->rollback_by_xid = se_rollback_by_xid;
  se_hton->recover = se_recover;
  se_hton->recover_prepared_in_tc = se_recover_tc;
  se_hton->commit = se_commit;
  se_hton->rollback = se_rollback;
  se_hton->db_type = DB_TYPE_SMARTENGINE;
  se_hton->show_status = se_show_status;
  se_hton->start_consistent_snapshot =
      se_start_tx_and_assign_read_view;
  se_hton->savepoint_set = se_savepoint;
  se_hton->savepoint_rollback = se_rollback_to_savepoint;
  se_hton->savepoint_rollback_can_release_mdl =
      se_rollback_to_savepoint_can_release_mdl;
  se_hton->flush_logs = se_flush_wal;
  se_hton->is_supported_system_table = se_is_supported_system_table;
  se_hton->flags = HTON_TEMPORARY_NOT_SUPPORTED |
                   HTON_SUPPORTS_EXTENDED_KEYS |
                   HTON_CAN_RECREATE |
                   HTON_SUPPORTS_ATOMIC_DDL |
                   HTON_SUPPORTS_ENGINE_ATTRIBUTE;

  se_hton->post_recover = se_post_recover;
  se_hton->post_engine_recover = se_post_engine_recover;
  se_hton->post_ddl = se_post_ddl;

  se_hton->checkpoint = se_checkpoint;
  se_hton->create_backup_snapshot = se_create_backup_snapshot;
  se_hton->incremental_backup = se_incremental_backup;
  se_hton->cleanup_tmp_backup_dir = se_cleanup_tmp_backup_dir;
  se_hton->release_backup_snapshot = se_release_backup_snapshot;
  se_hton->list_backup_snapshots = se_list_backup_snapshots;

  se_hton->data = se_api_cb;

  //assert(!mysqld_embedded);

  se_stats = smartengine::monitor::CreateDBStatistics();
  se_db_options.statistics = se_stats;

  if (se_row_cache_size > 0) {
    // set row_cache
    int row_cache_ret = smartengine::common::Status::kOk;
    if (smartengine::common::Status::kOk != (row_cache_ret =
          smartengine::cache::NewRowCache(se_row_cache_size, se_db_options.row_cache))) {
      sql_print_error("SE: fail to construct row cache: error_code = %d", row_cache_ret);
      DBUG_RETURN(HA_EXIT_FAILURE);
    }
  }

  if (se_rate_limiter_bytes_per_sec != 0) {
    se_rate_limiter.reset(
        smartengine::util::NewGenericRateLimiter(se_rate_limiter_bytes_per_sec));
    se_db_options.rate_limiter = se_rate_limiter;
  }

  int log_init_ret = 0;
  auto log_level = get_se_log_level(log_error_verbosity);
  // according to setup_error_log() in sql/mysqld.cc
  // log_error_dest is set to "stderr" if log_errors_to_file is false
  // bool log_errors_to_file =
  //     !is_help_or_validate_option() && (log_error_dest != disabled_my_option))
  // for example, run exec mysqld in some MTR cases
  if ((NULL != log_error_dest) && (0 != strcmp(log_error_dest, "stderr"))) {
    // using redirected stderr, refer to open_error_log in sql/log.cc
    fsync(STDERR_FILENO);
    log_init_ret = smartengine::logger::Logger::get_log().init(STDERR_FILENO, log_level);
  } else {
    // Write SE log to a file
    std::ostringstream oss;
    oss << se_data_dir << "/Log";
    log_init_ret = smartengine::logger::Logger::get_log().init(oss.str().c_str(),
                                                      log_level,
                                                      256 * 1024 * 1024);
  }
  if (0 != log_init_ret) {
    sql_print_information("Failed to initialize SE logger at handler level,"
                          " DB:Open will init it with %s/Log", se_data_dir);
  }

  se_db_options.wal_dir = se_wal_dir;

  se_db_options.wal_recovery_mode =
      static_cast<smartengine::common::WALRecoveryMode>(se_wal_recovery_mode);
  se_db_options.parallel_wal_recovery = se_parallel_wal_recovery;
  se_db_options.parallel_recovery_thread_num = se_parallel_recovery_thread_num;

  se_db_options.persistent_cache_dir = se_persistent_cache_dir; 
  if (!se_tbl_options.no_block_cache) {
    se_tbl_options.block_cache =
        smartengine::cache::NewLRUCache(se_block_cache_size, -1, false, 0.1, 0.375,
            smartengine::memory::ModId::kDefaultBlockCache);
  }
  // Using newer ExtentBasedTable format version for reuse of block and SST.
  se_tbl_options.format_version = 3;

  // TODO (Zhao Dongsheng) : The force setting of the follow parameters
  // will cause the configuration parameters to not take affect.
  // set default value for some internal options
  se_tbl_options.cache_index_and_filter_blocks = true;
  se_tbl_options.cache_index_and_filter_blocks_with_high_priority = true;
  se_tbl_options.pin_l0_filter_and_index_blocks_in_cache = false;
  se_tbl_options.cluster_id = opt_repo_objstore_id;
  se_tbl_options.cluster_id.append("/");
  se_tbl_options.cluster_id.append(opt_branch_objstore_id);
  se_db_options.allow_concurrent_memtable_write = true;
  se_db_options.use_direct_write_for_wal = false;
  se_db_options.persistent_cache_mode = se_persistent_cache_mode;
  //se_db_options.concurrent_writable_file_buffer_num =
  //    DEFAULT_SE_CONCURRENT_WRITABLE_FILE_BUFFER_NUMBER;
  //se_db_options.concurrent_writable_file_single_buffer_size =
  //    DEFAULT_SE_CONCURRENT_WRITABLE_FILE_SINGLE_BUFFER_SIZE; // 64KB
  //se_db_options.concurrent_writable_file_buffer_switch_limit =
  //    DEFAULT_SE_CONCURRENT_WRITABLE_FILE_BUFFER_SWITCH_LIMIT; // 32 KB
  if (nullptr != se_cf_compression_per_level &&
      !smartengine::common::GetVectorCompressionType(
          std::string(se_cf_compression_per_level),
          &se_default_cf_options.compression_per_level)) {
    sql_print_error("SE: Failed to parse input compression_per_level");
    DBUG_RETURN(1);
  }
  if (nullptr != se_cf_compression_opts &&
      !smartengine::common::GetCompressionOptions(
          std::string(se_cf_compression_opts),
          &se_default_cf_options.compression_opts)) {
    sql_print_error("SE: Failed to parse input compress_opts");
    DBUG_RETURN(1);
  }

  if (!se_cf_options_map.init(se_tbl_options, se_default_cf_options)) {
    // NO_LINT_DEBUG
    sql_print_error("SE: Failed to initialize CF options map.");
    se_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  std::vector<smartengine::db::ColumnFamilyHandle *> cf_handles;
  common::Options main_opts(se_db_options, se_cf_options_map.get_defaults());
  main_opts.disable_auto_compactions = true;
  main_opts.allow_2pc = true;
  // 1 more bg thread for create extent space
  main_opts.env->SetBackgroundThreads(main_opts.max_background_flushes + 1,
                                      smartengine::util::Env::Priority::HIGH);
  // 1 more bg thread for delete extent space, 1 more for cache purge, 1 more for free memtable
  main_opts.env->SetBackgroundThreads(main_opts.max_background_compactions +
                                      main_opts.max_background_dumps + 3,
                                      smartengine::util::Env::Priority::LOW);

  if (opt_table_on_objstore && opt_serverless) {
    if (!(opt_objstore_provider && opt_objstore_region &&
          opt_objstore_bucket)) {
      sql_print_error(
          "SE: if want to use objstore, object store provider(%s), region(%s) "
          "and bucket(%s) must be set",
          !opt_objstore_provider ? "NULL" : opt_objstore_provider,
          !opt_objstore_region ? "NULL" : opt_objstore_region,
          !opt_objstore_bucket ? "NULL" : opt_objstore_bucket);
      se_open_tables.free_hash();
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    std::string_view endpoint(opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");

    std::string mtr_test_bucket_subdir = "";
    if (opt_objstore_mtr_test_bucket_dir) {
      mtr_test_bucket_subdir = opt_objstore_mtr_test_bucket_dir;
      // remove possible '/' at the beginning and end of the string
      mtr_test_bucket_subdir.erase(0, mtr_test_bucket_subdir.find_first_not_of("/"));
      mtr_test_bucket_subdir.erase(mtr_test_bucket_subdir.find_last_not_of("/") + 1);
    }

    common::Status status = main_opts.env->InitObjectStore(std::string_view(opt_objstore_provider),
                                                           std::string_view(opt_objstore_region),
                                                           opt_objstore_endpoint ? &endpoint : nullptr,
                                                           opt_objstore_use_https,
                                                           opt_objstore_bucket,
                                                           se_tbl_options.cluster_id,
                                                           mtr_test_bucket_subdir);
    if (!status.ok()) {
      std::string err_text = status.ToString();
      sql_print_error("SE: fail to create object store: %s", err_text.c_str());
      se_open_tables.free_hash();
      DBUG_RETURN(HA_EXIT_FAILURE);
    } else {
      sql_print_information(
          "SE: smartengine object store is enabled, object store provider: %s, "
          "region: %s, bucket: %s",
          opt_objstore_provider, opt_objstore_region, opt_objstore_bucket);
    }

    db::GlobalContext::set_env(main_opts.env);

    int ret = se_renewal_objstore_lease_lock_thread.create_thread(RENEW_LEASE_LOCK_THREAD_NAME
#ifdef HAVE_PSI_INTERFACE
                                                                  ,
                                                                  se_renewal_objstore_lease_lock_psi_thread_key
#endif
    );
    if (ret != 0) {
      sql_print_error("SE: Couldn't start the renewal object store lease lock thread: (errno=%d)", ret);
      se_open_tables.free_hash();
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    sql_print_warning("SE: This node is not the owner of the lease lock, waiting...");
    // retry 1 minute to get single data node lease lock.
    int retry = 60000;
    while (!objstore::is_lease_lock_owner_node() && retry > 0) {
      // if this node is not the owner of the lease lock, can't do anything but wait
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      retry--;
    }
    if (!objstore::is_lease_lock_owner_node()) {
      sql_print_error("SE: Couldn't get object store lease lock: other node may be running");
      se_open_tables.free_hash();
      DBUG_RETURN(HA_EXIT_FAILURE);
    }
    sql_print_warning("SE: This node becomes the owner of the lease lock, go on");
  }

  util::TransactionDBOptions tx_db_options;
  tx_db_options.transaction_lock_timeout = 2; // 2 seconds
  tx_db_options.custom_mutex_factory = std::make_shared<SeMutexFactory>();

  common::Status status = util::TransactionDB::Open(main_opts,
                                                    tx_db_options,
                                                    se_data_dir,
                                                    &cf_handles,
                                                    &se_db);

  if (!status.ok()) {
    std::string err_text = status.ToString();
    sql_print_error("SE: Error opening instance: %s", err_text.c_str());
    se_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }
  cf_manager.init(&se_cf_options_map, &cf_handles, &ddl_log_manager);

  if (dict_manager.init(se_db->GetBaseDB(), se_default_cf_options)) {
    // NO_LINT_DEBUG
    sql_print_error("SE: Failed to initialize data dictionary.");
    se_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  if (binlog_manager.init(&dict_manager)) {
    // NO_LINT_DEBUG
    sql_print_error("SE: Failed to initialize binlog manager.");
    se_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  THD *const thd = my_core::thd_get_current_thd();
  if (ddl_manager.init(thd, &dict_manager)) {
    // NO_LINT_DEBUG
    sql_print_error("SE: Failed to initialize DDL manager.");
    se_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  if (ddl_log_manager.init(&dict_manager, &ddl_manager, &cf_manager, &se_drop_idx_thread)) {
    sql_print_error("SE: Failed to initialize ddl_log manager.");
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  SeSstInfo::init(se_db);

  se_db->SetOptions({{"disable_auto_compactions", "false"}});

  if (!status.ok()) {
    const std::string err_text = status.ToString();
    // NO_LINT_DEBUG
    sql_print_error("SE: Error enabling compaction: %s", err_text.c_str());
    se_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  int ret = se_bg_thread.create_thread(BG_THREAD_NAME
#ifdef HAVE_PSI_INTERFACE
                                       ,
                                       se_background_psi_thread_key
#endif
  );
  if (ret != 0) {
    sql_print_error("SE: Couldn't start the background thread: (errno=%d)",
                    ret);
    se_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  ret = se_drop_idx_thread.create_thread(INDEX_THREAD_NAME
#ifdef HAVE_PSI_INTERFACE
                                          ,
                                          se_drop_idx_psi_thread_key
#endif
                                          );
  if (ret != 0) {
    sql_print_error("SE: Couldn't start the drop index thread: (errno=%d)",
                    ret);
    se_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  se_set_collation_exception_list(se_strict_collation_exceptions);

  if (se_pause_background_work) {
    se_db->PauseBackgroundWork();
  }

  // remove hard links in backup tmp dir if exists
  if (FAILED(BackupSnapshot::get_instance()->init(se_db))) {
    sql_print_error("SE: Failed to init backup instance.");
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

    // NO_LINT_DEBUG
    sql_print_information("SE: global statistics using %s indexer", STRINGIFY_ARG(SE_INDEXER));
#if defined(HAVE_SCHED_GETCPU)
  if (sched_getcpu() == -1) {
    // NO_LINT_DEBUG
    sql_print_information(
        "SE: sched_getcpu() failed - "
        "global statistics will use thread_id_indexer_t instead");
  }
#endif

  sql_print_information("SE instance opened");
  se_initialized = true;
  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**Storage Engine deinitialization function, invoked when plugin is unloaded.*/
static int se_done_func(void *const p)
{
  DBUG_ENTER_FUNC();

  int error = 0;

  // signal the drop index thread to stop
  se_drop_idx_thread.signal(true);

  // Flush all memtables for not losing data, even if WAL is disabled.
  se_flush_all_memtables();

  // Stop all se background work
  se_db->CancelAllBackgroundWork(true /*wait*/);

  // Signal the background thread to stop and to persist all stats collected
  // from background flushes and compactions. This will add more keys to a new
  // memtable, but since the memtables were just flushed, it should not trigger
  // a flush that can stall due to background threads being stopped. As long
  // as these keys are stored in a WAL file, they can be retrieved on restart.
  se_bg_thread.signal(true);

  if (opt_table_on_objstore && opt_serverless) {
    // Signal the renewal object store lease lock thread to stop.
    se_renewal_objstore_lease_lock_thread.signal(true);
    // Wait for the renewal object store lease lock thread to finish.
    se_renewal_objstore_lease_lock_thread.join();
  }

  // Wait for the background thread to finish.
  auto err = se_bg_thread.join();
  if (err != 0) {
    // We'll log the message and continue because we're shutting down and
    // continuation is the optimal strategy.
    // NO_LINT_DEBUG
    sql_print_error("SE: Couldn't stop the background thread: (errno=%d)",
                    err);
  }

  // Wait for the drop index thread to finish.
  err = se_drop_idx_thread.join();
  if (err != 0) {
    // NO_LINT_DEBUG
    sql_print_error("SE: Couldn't stop the index thread: (errno=%d)", err);
  }

  if (se_open_tables.m_hash.size()) {
    // Looks like we are getting unloaded and yet we have some open tables
    // left behind.
    error = 1;
  }

  se_open_tables.free_hash();
  mysql_mutex_destroy(&se_open_tables.m_mutex);
  mysql_mutex_destroy(&se_sysvars_mutex);

  delete se_collation_exceptions;
  mysql_mutex_destroy(&se_collation_data_mutex);
  mysql_mutex_destroy(&se_mem_cmp_space_mutex);

  SeTransaction::term_mutex();

  for (auto &it : se_collation_data) {
    delete it;
    it = nullptr;
  }

  ddl_manager.cleanup();
  binlog_manager.cleanup();
  dict_manager.cleanup();
  cf_manager.cleanup();

  util::Env *env = se_db->GetEnv();
  if (env) {
    env->DestroyObjectStore();
  }

  delete se_db;
  se_db = nullptr;

// Disown the cache data since we're shutting down.
// This results in memory leaks but it improved the shutdown time.
// Don't disown when running under valgrind
#ifndef HAVE_purify
  if (se_tbl_options.block_cache) {
    se_tbl_options.block_cache->DisownData();
    se_tbl_options.block_cache->destroy();
  }
  if (se_db_options.row_cache) {
    se_db_options.row_cache->destroy();
  }
#else /* HAVE_purify */
  if (se_tbl_options.block_cache) {
    se_tbl_options.block_cache->EraseUnRefEntries();
    se_tbl_options.block_cache->destroy();
  }
  if (se_db_options.row_cache) {
    se_db_options.row_cache->destroy();
  }
#endif

  DBUG_RETURN(error);
}

} // namespace smartengine

/**Register the storage engine plugin outside of smartengine namespace
so that mysql_declare_plugin does not get confused when it does its name generation.*/

struct st_mysql_storage_engine smartengine_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(smartengine){
    MYSQL_STORAGE_ENGINE_PLUGIN,  /**Plugin Type*/
    &smartengine_storage_engine,  /**Plugin Descriptor*/
    SMARTENGINE_NAME, /** Plugin Name*/
    "ApeCloud wesql team",  /**Plugin Author*/
    "High-compression, supports transactions",  /**Plugin Description*/
    PLUGIN_LICENSE_GPL,  /**Plugin Licence*/
    smartengine::se_init_func,  /**Plugin Entry Point */
    nullptr,  /**Plugin Check Uninstall*/
    smartengine::se_done_func,  /**Plugin Deinitializer*/
    0x0001,  /**version number (0.1)*/
    smartengine::se_show_vars_export,  /**status variables*/
    smartengine::se_system_vars_export,  /**system variables*/
    nullptr,  /**config options*/
    0,  /**flags*/
},
    smartengine::se_i_s_cfstats, smartengine::se_i_s_dbstats,
    smartengine::se_i_s_perf_context, smartengine::se_i_s_perf_context_global,
    /*smartengine::se_i_s_cfoptions,*/ smartengine::se_i_s_compact_stats,
    smartengine::se_i_s_global_info, smartengine::se_i_s_ddl,
    smartengine::se_i_s_lock_info,
    smartengine::se_i_s_trx_info, smartengine::i_s_se_tables, smartengine::i_s_se_columns,
    smartengine::se_i_s_se_compaction_task, smartengine::se_i_s_se_compaction_history,
    smartengine::se_i_s_se_mem_alloc,
    smartengine::se_i_s_se_subtable,
    smartengine::se_i_s_se_table_space,
    smartengine::se_i_s_query_trace,
    smartengine::se_i_s_se_debug_info
    mysql_declare_plugin_end;
