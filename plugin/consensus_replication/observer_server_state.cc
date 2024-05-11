/* Copyright (c) 2013, 2022, Oracle and/or its affiliates.

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

#include <stddef.h>

#include "consensus_applier.h"
#include "consensus_log_manager.h"
#include "observer_server_state.h"
#include "plugin.h"
#include "system_variables.h"

#include "sql/binlog.h"
#include "sql/rpl_rli.h"

using std::string;

/*
  DBMS lifecycle events observers.
*/
int consensus_replication_before_recovery(Server_state_param *) { return 0; }

int consensus_replication_after_engine_recovery(Server_state_param *) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  /* Create or load consensus info  */
  if (consensus_log_manager.init_consensus_info()) return -1;

  if (consensus_log_manager.update_consensus_info()) return -1;

  if (opt_bin_log && consensus_log_manager.recovery_applier_status()) return -1;

  return 0;
}

int consensus_replication_after_recovery(Server_state_param *) {
  if (!opt_bin_log || !plugin_is_consensus_replication_enabled()) {
    return 0;
  }

  /* Init replica construct for consensus channel */
  if (!opt_initialize) {
    if (init_consensus_replica()) return -1;
  }

  return 0;
}

int consensus_replication_before_handle_connection(Server_state_param *) {
  if (!opt_bin_log || !plugin_is_consensus_replication_enabled()) {
    return 0;
  }

  assert(!opt_initialize);

  if (consensus_log_manager.start_consensus_state_change_thread()) return -1;

  /* Start consensus service */
  if (consensus_log_manager.init_service()) return -1;

  /* Start consensus apply threads */
  if (!opt_cluster_log_type_instance && start_consensus_replica()) return -1;

  // purge logs after start applier thread
  MYSQL_BIN_LOG *log = &consensus_log_manager.get_relay_log_info()->relay_log;
  if (DBUG_EVALUATE_IF("expire_logs_always_at_start", false, true))
    log->auto_purge_at_server_startup();
  else if (expire_logs_days > 0 || binlog_expire_logs_seconds > 0)
    log->purge_logs_before_date(time(nullptr), true);

  return 0;
}

int consensus_replication_before_server_shutdown(Server_state_param *) {
  return 0;
}

int consensus_replication_after_dd_upgrade(Server_state_param *) { return 0; }

int consensus_replication_after_server_shutdown(Server_state_param *) {
  if (!opt_bin_log || !plugin_is_consensus_replication_enabled()) {
    return 0;
  }

  if (!opt_initialize) {
    /* Stop consensus state change */
    consensus_log_manager.stop_consensus_state_change_thread();
    /* Stop consensus relica */
    end_consensus_replica();
    /* Stop consensus service */
    rpl_consensus_shutdown();
    rpl_consensus_cleanup();
  }

  /* Close mysql_bin_log after shutdown */
  consensus_log_manager.get_binlog()->close(LOG_CLOSE_INDEX,
                                            true /*need_lock_log=true*/,
                                            true /*need_lock_index=true*/);

  return 0;
}

Server_state_observer cr_server_state_observer = {
    sizeof(Server_state_observer),

    consensus_replication_before_handle_connection,  // before the client
                                                     // connects to the server
    consensus_replication_before_recovery,         // before recovery
    consensus_replication_after_engine_recovery,   // after engine recovery
    consensus_replication_after_recovery,          // after recovery
    consensus_replication_before_server_shutdown,  // before shutdown
    consensus_replication_after_server_shutdown,   // after shutdown
    consensus_replication_after_dd_upgrade,        // after DD upgrade from 5.7
};