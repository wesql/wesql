/*
   Portions Copyright (c) 2024, ApeCloud Inc Holding Limited

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

#include <stddef.h>

#include "consensus_applier.h"
#include "consensus_binlog.h"
#include "consensus_log_manager.h"
#include "consensus_meta.h"
#include "consensus_state_process.h"
#include "observer_server_state.h"
#include "plugin.h"
#include "system_variables.h"

#include "sql/binlog.h"
#include "sql/mysqld.h"
#include "sql/rpl_rli.h"

using std::string;

/*
  DBMS lifecycle events observers.
*/
int consensus_replication_before_recovery(Server_state_param *) { return 0; }

int consensus_replication_after_engine_recovery(Server_state_param *) {
  DBUG_TRACE;

  /* If the plugin is not running, return failed. */
  if (!plugin_is_consensus_replication_running()) return 1;

  /* Create or load consensus info  */
  if (consensus_meta.init_consensus_info()) return -1;

  if (consensus_meta.update_consensus_info()) return -1;

  if (opt_bin_log && consensus_state_process.recovery_applier_status())
    return -1;

  return 0;
}

int consensus_replication_after_recovery(Server_state_param *) {
  DBUG_TRACE;

  if (!opt_bin_log) return 0;

  /* If the plugin is not running, return failed. */
  if (!plugin_is_consensus_replication_running()) return 1;

  if (start_consensus_replication()) return 1;

  if (!opt_initialize) {
    // purge logs after start consensus replication
    MYSQL_BIN_LOG *log =
        &consensus_state_process.get_relay_log_info()->relay_log;
    if (DBUG_EVALUATE_IF("expire_logs_always_at_start", false, true))
      log->auto_purge_at_server_startup();
    else if (expire_logs_days > 0 || binlog_expire_logs_seconds > 0)
      purge_consensus_logs_on_conditions(time(nullptr), 0, nullptr, true);
  }

  return 0;
}

int consensus_replication_before_handle_connection(Server_state_param *) {
  DBUG_TRACE;

  if (!opt_bin_log) return 0;

  /* If the plugin is not running, return failed. */
  if (!plugin_is_consensus_replication_running()) return 1;

  /* Start consensus apply threads */
  if (!opt_cluster_log_type_instance && start_consensus_replica()) return -1;

  if (!opt_initialize) rpl_consensus_set_ready();

  return 0;
}

int consensus_replication_before_server_shutdown(Server_state_param *) {
  return 0;
}

int consensus_replication_after_dd_upgrade(Server_state_param *) { return 0; }

int consensus_replication_after_server_shutdown(Server_state_param *) {
  DBUG_TRACE;

  if (!opt_bin_log) return 0;

  /* If the plugin is not running, return failed. */
  if (!plugin_is_consensus_replication_running()) return 1;

  stop_consensus_replication();

  /* Close mysql_bin_log after stop consensus replication service */
  consensus_state_process.get_binlog()->close(LOG_CLOSE_INDEX,
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
