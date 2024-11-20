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

#ifndef CONSENSUS_PLUGIN_INCLUDE
#define CONSENSUS_PLUGIN_INCLUDE

#include <mysql/plugin.h>
#include <mysql/plugin_consensus_replication.h>

#define PLUGIN_AUTHOR "Apecloud Corporation"
#define PLUGIN_VERSION 0x0100

class Checkable_rwlock;

/** Flag that indicates that there are observers (for performance)*/

// Plugin global methods
SERVICE_TYPE(registry) * get_plugin_registry();

bool plugin_is_consensus_replication_enabled();
bool plugin_is_consensus_replication_running();

// Plugin public methods
int plugin_consensus_replication_init(MYSQL_PLUGIN plugin_info);
int plugin_consensus_replication_deinit(void *p);
int plugin_consensus_replication_stop();

int start_consensus_replication();
void stop_consensus_replication();

bool acquire_transaction_control_services();
bool release_transaction_control_services();

#endif /* CONSENSUS_PLUGIN_INCLUDE */
