/*
   Portions Copyright (c) 2024, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2009, 2023, Oracle and/or its affiliates.

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

#include <cassert>
#include <sstream>

#include "consensus_applier.h"
#include "consensus_binlog.h"
#include "consensus_log_manager.h"
#include "consensus_meta.h"
#include "consensus_proc.h"
#include "consensus_state_process.h"
#include "i_s_consensus.h"
#include "observer_applier.h"
#include "observer_binlog_manager.h"
#include "observer_server_state.h"
#include "observer_trans.h"
#include "plugin.h"
#include "plugin_psi.h"
#include "plugin_variables.h"
#include "rpl_consensus.h"
#include "system_variables.h"

#include "mutex_lock.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_io.h"

#include "my_loglevel.h"
#include "sql/log.h"
#include "mysql/components/services/log_builtins.h"

#include "sql/package/package.h"

using std::string;

/*
  Variables that are only acessible inside plugin.cc.
*/
static struct plugin_consensus_local_variables lv;

/*
  Plugin modules.

  plugin.cc class pointers that are acessible on all plugin files,
  that is, are declared as extern on plugin.h.
  These pointers will be initialized on plugin_consensus_replication_init().
*/

/*
  Internal auxiliary functions signatures.
*/
static int check_if_server_properly_configured(
    const Trans_context_info &startup_pre_reqs);

bool plugin_is_consensus_replication_running() {
  return lv.consensus_replication_running;
}

static bool plugin_is_consensus_replication_applier_running() {
  return is_consensus_applier_running();
}

static bool plugin_is_consensus_replication_log_mode() {
  return opt_cluster_log_type_instance;
}

static bool plugin_is_consensus_replication_state_leader(uint64 &term) {
  Consensus_Log_System_Status status =
      Consensus_Log_System_Status::RELAY_LOG_WORKING;

  if (lv.consensus_replication_running)
    consensus_state_process.get_term_and_status(term, status);

  return status == Consensus_Log_System_Status::BINLOG_WORKING;
}

static bool plugin_show_consensus_logs(void *thd) {
  return consensus_show_logs((THD *)thd);
}

static bool plugin_show_consensus_log_events(void *thd) {
  return consensus_show_log_events((THD *)thd);
}

/*
  Plugin interface.
*/
struct st_mysql_consensus_replication consensus_replication_descriptor = {
    MYSQL_CONSENSUS_REPLICATION_INTERFACE_VERSION,
    plugin_is_consensus_replication_running,
    plugin_is_consensus_replication_applier_running,
    plugin_is_consensus_replication_log_mode,
    plugin_is_consensus_replication_state_leader,
    plugin_show_consensus_logs,
    plugin_show_consensus_log_events};

/**
  Initialize Package dbms_consensus context.
*/
static void init_consensus_package_context() {
  if (!opt_bin_log || opt_initialize) return;

  register_package<Proc, Consensus_proc_change_leader>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_add_learner>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_add_follower>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_drop_learner>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_upgrade_learner>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_downgrade_follower>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_refresh_learner_meta>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_configure_follower>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_configure_learner>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_force_promote>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_force_single_mode>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_fix_cluster_id>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_fix_matchindex>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_show_global>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_show_local>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_show_logs>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_purge_log>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_local_purge_log>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_force_purge_log>(CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_drop_prefetch_channel>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_activate_failpoint>(
      CONSENSUS_PROC_SCHEMA);
  register_package<Proc, Consensus_proc_deactivate_failpoint>(
      CONSENSUS_PROC_SCHEMA);
}

bool initialize_registry_handler() {
  bool res = false;

  lv.reg_srv = mysql_plugin_registry_acquire();
  if (!lv.reg_srv) res = true; /* purecov: inspected */

  return res;
}

bool finalize_registry_handler() {
  bool res = false;

  /* release the registry handle */
  if (lv.reg_srv && mysql_plugin_registry_release(lv.reg_srv))
    res = true; /* purecov: inspected */
  else
    lv.reg_srv = nullptr;

  return res;
}

static int initialize_plugin_and_join(Binlog_context_info *infos) {
  DBUG_TRACE;
  lv.plugin_running_lock->assert_some_wrlock();

  int error = 0;

  if (!opt_initialize) {
    if ((error = initialize_registry_handler())) goto err;
  }

  // Init and bind binlog to consensus log
  error = consensus_log_manager.init(opt_consensus_log_cache_size,
                                     opt_consensus_prefetch_cache_size,
                                     opt_consensus_start_index);
  consensus_meta.init();
  consensus_applier.init();
  consensus_state_process.init();
  consensus_state_process.set_binlog(infos->binlog);
  infos->binlog->is_consensus_log = true;

  lv.consensus_replication_running = true;

err:
  return error;
}

int start_consensus_replication() {
  if (opt_initialize) return 0;

  /* Init replica construct for consensus channel */
  if (init_consensus_replica()) return -1;

  if (consensus_state_process.start_consensus_state_change_thread()) return -1;

  /* Start consensus service */
  if (consensus_state_process.init_service()) return -1;

  if (consensus_log_manager.start_consensus_commit_advance_thread()) return -1;

  return 0;
}
void stop_consensus_replication() {
  if (opt_initialize) return;

  /* Stop consensus commit advance */
  consensus_log_manager.stop_consensus_commit_advance_thread();
  /* Stop consensus state change */
  consensus_state_process.stop_consensus_state_change_thread();
  /* Stop consensus relica */
  if (!opt_cluster_log_type_instance) end_consensus_replica();
  /* Stop consensus service */
  rpl_consensus_shutdown();
  rpl_consensus_cleanup();
}

int plugin_consensus_replication_stop() {
  DBUG_TRACE;

  Checkable_rwlock::Guard g(*lv.plugin_running_lock,
                            Checkable_rwlock::WRITE_LOCK);

  if (!plugin_is_consensus_replication_running()) return 0;

  if (!opt_initialize && !rpl_consensus_is_shutdown())
    stop_consensus_replication();

  lv.plugin_stop_lock->wrlock();
  LogPluginErr(INFORMATION_LEVEL, ER_CONSENSUS_RPL_IS_STOPPING);

  lv.consensus_replication_running = false;

  if (!opt_initialize) finalize_registry_handler();

  lv.plugin_stop_lock->unlock();
  LogPluginErr(SYSTEM_LEVEL, ER_CONSENSUS_RPL_IS_STOPPED);

  consensus_state_process.cleanup();
  consensus_applier.cleanup();
  consensus_meta.cleanup();
  consensus_log_manager.cleanup();

  return 0;
}

int plugin_consensus_replication_init(MYSQL_PLUGIN plugin_info) {
  // Struct that holds startup and runtime requirements
  Trans_context_info startup_pre_reqs;
  Binlog_context_info binlog_context;

  DBUG_TRACE;

  // Initialize plugin local variables.
  lv.init();

// Register all PSI keys at the time plugin init
#ifdef HAVE_PSI_INTERFACE
  register_all_consensus_replication_psi_keys();
#endif /* HAVE_PSI_INTERFACE */

  lv.plugin_running_lock = new Checkable_rwlock(
#ifdef HAVE_PSI_INTERFACE
      key_rwlock_plugin_running
#endif /* HAVE_PSI_INTERFACE */
  );
  lv.plugin_stop_lock = new Checkable_rwlock(
#ifdef HAVE_PSI_INTERFACE
      key_rwlock_plugin_stop
#endif /* HAVE_PSI_INTERFACE */
  );

  Checkable_rwlock::Guard g(*lv.plugin_running_lock,
                            Checkable_rwlock::WRITE_LOCK);

  lv.plugin_info_ptr = plugin_info;

  if (register_server_state_observer(&cr_server_state_observer,
                                     (void *)lv.plugin_info_ptr)) {
    /* purecov: begin inspected */
    LogPluginErr(ERROR_LEVEL,
                 ER_CONSENSUS_RPL_FAILED_TO_REGISTER_SERVER_STATE_OBSERVER);
    return 1;
    /* purecov: end */
  }

  if (register_trans_observer(&cr_trans_observer, (void *)lv.plugin_info_ptr)) {
    /* purecov: begin inspected */
    LogPluginErr(ERROR_LEVEL,
                 ER_CONSENSUS_RPL_FAILED_TO_REGISTER_TRANS_STATE_OBSERVER);
    return 1;
    /* purecov: end */
  }

  if (register_binlog_applier_observer(&cr_binlog_applier_observer,
                                       (void *)lv.plugin_info_ptr)) {
    /* purecov: begin inspected */
    LogPluginErr(ERROR_LEVEL,
                 ER_CONSENSUS_RPL_FAILED_TO_REGISTER_APPLIER_OBSERVER);
    return 1;
    /* purecov: end */
  }

  if (register_binlog_manager_observer(&cr_binlog_mananger_observer,
                                       (void *)lv.plugin_info_ptr)) {
    /* purecov: begin inspected */
    LogPluginErr(ERROR_LEVEL,
                 ER_CONSENSUS_RPL_FAILED_TO_REGISTER_BINLOG_MANAGER_OBSERVER);
    return 1;
    /* purecov: end */
  }

  init_consensus_package_context();

  cr_get_server_startup_prerequirements(startup_pre_reqs, binlog_context);

  if (check_if_server_properly_configured(startup_pre_reqs)) {
    /* purecov: begin inspected */
    return 1;
    /* purecov: end */
  }

  if (initialize_plugin_and_join(&binlog_context)) {
    /* purecov: begin inspected */
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_RPL_FAILED_TO_START_ON_BOOT);
    return 1;
    /* purecov: end */
  }

  return 0;
}

int plugin_consensus_replication_deinit(void *p) {
  DBUG_TRACE;

  // If plugin was not initialized, there is nothing to do here.
  if (lv.plugin_info_ptr == nullptr) return 0;

  int observer_unregister_error = 0;

  if (plugin_consensus_replication_stop())
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_RPL_FAILED_TO_STOP_ON_PLUGIN_UNINSTALL);

  if (unregister_server_state_observer(&cr_server_state_observer, p)) {
    LogPluginErr(ERROR_LEVEL,
                 ER_CONSENSUS_RPL_FAILED_TO_UNREGISTER_SERVER_STATE_OBSERVER);
    observer_unregister_error++;
  }

  if (unregister_trans_observer(&cr_trans_observer, p)) {
    LogPluginErr(ERROR_LEVEL,
                 ER_CONSENSUS_RPL_FAILED_TO_UNREGISTER_TRANS_STATE_OBSERVER);
    observer_unregister_error++;
  }

  if (unregister_binlog_applier_observer(&cr_binlog_applier_observer, p)) {
    LogPluginErr(ERROR_LEVEL,
                 ER_CONSENSUS_RPL_FAILED_TO_UNREGISTER_APPLIER_OBSERVER);
    observer_unregister_error++;
  }

  if (unregister_binlog_manager_observer(&cr_binlog_mananger_observer, p)) {
    LogPluginErr(ERROR_LEVEL,
                 ER_CONSENSUS_RPL_FAILED_TO_UNREGISTER_BINLOG_MANAGER_OBSERVER);
    observer_unregister_error++;
  }


  if (observer_unregister_error == 0)
    LogPluginErr(INFORMATION_LEVEL, ER_CONSENSUS_RPL_ALL_OBSERVERS_UNREGISTERED);

  delete lv.plugin_stop_lock;
  lv.plugin_stop_lock = nullptr;
  delete lv.plugin_running_lock;
  lv.plugin_running_lock = nullptr;

  lv.plugin_info_ptr = nullptr;

  return observer_unregister_error;
}

static int plugin_consensus_replication_check_uninstall(void *) {
  DBUG_TRACE;

  /* Uninstall fails */
  my_error(ER_PLUGIN_CANNOT_BE_UNINSTALLED, MYF(0), "raft_replication",
           "Plugin cannot be uninstalled.");

  return 1;
}

SERVICE_TYPE(registry) * get_plugin_registry() { return lv.reg_srv; }

/*
  This method is used to accomplish the startup validations of the plugin
  regarding system configuration.

  It currently verifies:
  - Binlog enabled
  - Binlog format
  - Gtid mode
  - LOG_REPLICA_UPDATES
  - Single primary mode configuration

  @return If the operation succeed or failed
    @retval 0 in case of success
    @retval 1 in case of failure
 */
static int check_if_server_properly_configured(
    const Trans_context_info &startup_pre_reqs) {
  DBUG_TRACE;

  if (!opt_initialize) {
    if (!startup_pre_reqs.binlog_enabled) {
      LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_RPL_BINLOG_DISABLED);
      return 1;
    }
  }

  if (startup_pre_reqs.mi_repository_type != 1)  // INFO_REPOSITORY_TABLE
  {
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_RPL_APPLIER_METADATA_REPO_MUST_BE_TABLE);
    return 1;
  }

  if (startup_pre_reqs.rli_repository_type != 1)  // INFO_REPOSITORY_TABLE
  {
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_RPL_APPLIER_METADATA_REPO_MUST_BE_TABLE);
    return 1;
  }

  return 0;
}

bool acquire_transaction_control_services() {
  bool ret = false;
  DBUG_TRACE;

  // Acquire the 'mysql_before_commit_transaction_control' service.
  if (nullptr == lv.svc_mysql_before_commit_transaction_control) {
    my_h_service h_mysql_before_commit_transaction_control = nullptr;
    if (lv.reg_srv->acquire(
            "mysql_before_commit_transaction_control",
            &h_mysql_before_commit_transaction_control) ||
        nullptr == h_mysql_before_commit_transaction_control) {
      /* purecov: begin inspected */
      lv.svc_mysql_before_commit_transaction_control = nullptr;
      ret = true;
      goto end;
      /* purecov: end */
    }
    lv.svc_mysql_before_commit_transaction_control =
        reinterpret_cast<SERVICE_TYPE_NO_CONST(
            mysql_before_commit_transaction_control) *>(
            h_mysql_before_commit_transaction_control);
  }
  // Acquire the
  // 'mysql_close_connection_of_binloggable_transaction_not_reached_commit'
  // service.
  if (nullptr ==
      lv.svc_mysql_close_connection_of_binloggable_transaction_not_reached_commit) {
    my_h_service
        h_mysql_close_connection_of_binloggable_transaction_not_reached_commit =
            nullptr;
    if (lv.reg_srv->acquire(
            "mysql_close_connection_of_binloggable_transaction_not_reached_"
            "commit",
            &h_mysql_close_connection_of_binloggable_transaction_not_reached_commit) ||
        nullptr ==
            h_mysql_close_connection_of_binloggable_transaction_not_reached_commit) {
      /* purecov: begin inspected */
      lv.svc_mysql_close_connection_of_binloggable_transaction_not_reached_commit =
          nullptr;
      ret = true;
      goto end;
      /* purecov: end */
    }
    lv.svc_mysql_close_connection_of_binloggable_transaction_not_reached_commit =
        reinterpret_cast<SERVICE_TYPE_NO_CONST(
            mysql_close_connection_of_binloggable_transaction_not_reached_commit)
                             *>(
            h_mysql_close_connection_of_binloggable_transaction_not_reached_commit);
  }
end:
  return ret;
}

bool release_transaction_control_services() {
  bool ret = false;
  DBUG_TRACE;

  // Release the 'mysql_before_commit_transaction_control' service.
  if (nullptr != lv.svc_mysql_before_commit_transaction_control) {
    my_h_service h_mysql_before_commit_transaction_control =
        reinterpret_cast<my_h_service>(
            lv.svc_mysql_before_commit_transaction_control);
    ret |= lv.reg_srv->release(h_mysql_before_commit_transaction_control) != 0;
    lv.svc_mysql_before_commit_transaction_control = nullptr;
  }
  // Release the
  // 'mysql_close_connection_of_binloggable_transaction_not_reached_commit'
  // service.
  if (nullptr !=
      lv.svc_mysql_close_connection_of_binloggable_transaction_not_reached_commit) {
    my_h_service
        h_mysql_close_connection_of_binloggable_transaction_not_reached_commit =
            reinterpret_cast<my_h_service>(
                lv.svc_mysql_close_connection_of_binloggable_transaction_not_reached_commit);
    ret |=
        lv.reg_srv->release(
            h_mysql_close_connection_of_binloggable_transaction_not_reached_commit) !=
        0;
    lv.svc_mysql_close_connection_of_binloggable_transaction_not_reached_commit =
        nullptr;
  }
  return ret;
}

static SHOW_VAR consensus_replication_status_vars[] = {
    {nullptr, nullptr, SHOW_LONG, SHOW_SCOPE_GLOBAL},
};

mysql_declare_plugin(raft_replication){
    MYSQL_REPLICATION_PLUGIN,
    &consensus_replication_descriptor,
    "raft_replication",
    PLUGIN_AUTHOR,
    "Raft Replication (1.0)", /* Plugin name with full version*/
    PLUGIN_LICENSE_GPL,
    plugin_consensus_replication_init,            /* Plugin Init */
    plugin_consensus_replication_check_uninstall, /* Plugin Check uninstall */
    plugin_consensus_replication_deinit,          /* Plugin Deinit */
    PLUGIN_VERSION,                    /* Plugin Version: major.minor */
    consensus_replication_status_vars, /* status variables */
    consensus_replication_system_vars, /* system variables */
    nullptr,                           /* config options */
    0,                                 /* flags */
},
    i_s_wesql_cluster_global, i_s_wesql_cluster_local, i_s_wesql_cluster_health,
    i_s_wesql_cluster_learner_source, i_s_wesql_cluster_prefetch_channel,
    i_s_wesql_cluster_consensus_status,
    i_s_wesql_cluster_consensus_membership_change

    mysql_declare_plugin_end;
