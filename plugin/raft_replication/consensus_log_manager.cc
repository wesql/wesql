/*
   Portions Copyright (c) 2024, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2018, 2021, Alibaba and/or its affiliates.

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

#include <signal.h>

#include "consensus_binlog.h"
#include "consensus_binlog_recovery.h"
#include "consensus_log_manager.h"
#include "consensus_meta.h"
#include "consensus_state_process.h"
#include "plugin.h"
#include "plugin_psi.h"
#include "rpl_consensus.h"
#include "system_variables.h"

#include "mysql/psi/mysql_file.h"
#include "mysql/thread_pool_priv.h"
#include "mysqld_error.h"

#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/log.h"

#include "sql/rpl_mi.h"
#include "sql/rpl_replica.h"
#include "sql/sql_thd_internal_api.h"  // create_internal_thd

using binary_log::checksum_crc32;
using std::max;

#define LOG_PREFIX "ML"

ConsensusLogManager consensus_log_manager;

uint64 show_fifo_cache_size(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size = !is_consensus_replication_enabled()
                    ? 0
                    : consensus_log_manager.get_fifo_cache_manager()
                          ->get_fifo_cache_size();
  *value = static_cast<long long>(size);
  return 0;
}

uint64 show_first_index_in_fifo_cache(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size = !is_consensus_replication_enabled()
                    ? 0
                    : consensus_log_manager.get_fifo_cache_manager()
                          ->get_first_index_of_fifo_cache();
  *value = static_cast<long long>(size);
  return 0;
}

uint64 show_log_count_in_fifo_cache(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size = !is_consensus_replication_enabled()
                    ? 0
                    : consensus_log_manager.get_fifo_cache_manager()
                          ->get_fifo_cache_log_count();
  *value = static_cast<long long>(size);
  return 0;
}

int ConsensusLogManager::init(uint64 max_fifo_cache_size_arg,
                              uint64 max_prefetch_cache_size_arg,
                              uint64 fake_current_index_arg) {
  DBUG_TRACE;
  cache_log = new IO_CACHE_binlog_cache_storage();
  if (!cache_log) return 1;

  mysql_mutex_init(key_mutex_ConsensusLog_commit_advance_lock,
                   &LOCK_consensuslog_commit_advance, MY_MUTEX_INIT_FAST);

  mysql_rwlock_init(key_rwlock_ConsensusLog_truncate_lock,
                    &LOCK_consensuslog_truncate);
  mysql_rwlock_init(key_rwlock_ConsensusLog_rotate_lock,
                    &LOCK_consensuslog_truncate);

  mysql_cond_init(key_COND_ConsensusLog_commit_advance,
                    &COND_consensuslog_commit_advance);

  if (cache_log->open(mysql_tmpdir, LOG_PREFIX, binlog_cache_size,
                      max_binlog_cache_size))
    return 1;

  if (cache_log->get_io_cache()->m_encryptor != nullptr) {
    delete cache_log->get_io_cache()->m_encryptor;
    cache_log->get_io_cache()->m_encryptor = nullptr;
  }

  // initialize the empty log content
  Consensus_empty_log_event eev;
  eev.common_footer->checksum_alg =
      static_cast<enum_binlog_checksum_alg>(binlog_checksum_options);
  eev.write(cache_log);
  my_off_t buf_size = cache_log->length();
  uchar *buffer = (uchar *)my_malloc(key_memory_thd_main_mem_root,
                                     (size_t)buf_size, MYF(MY_WME));
  copy_from_consensus_log_cache(cache_log, buffer, buf_size);
  int4store(buffer, 0);  // set timestamp to 0
  empty_log_event_content = std::string((char *)buffer, buf_size);
  my_free(buffer);
  cache_log->reset();

  current_index = fake_current_index_arg;

  fifo_cache_manager = new ConsensusFifoCacheManager();
  fifo_cache_manager->init(max_fifo_cache_size_arg);

  prefetch_manager = new ConsensusPreFetchManager();
  prefetch_manager->init(max_prefetch_cache_size_arg);

  log_file_index = new ConsensusLogIndex();
  log_file_index->init();

  inited = true;
  return 0;
}

int ConsensusLogManager::write_log_entry(ConsensusLogEntry &log,
                                         uint64 *consensus_index,
                                         bool with_check) {
  int error = 0;
  bool changed_in_large_trx = true;
  DBUG_TRACE;

  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());

  uint64 last_log_term = get_last_log_term();
  Relay_log_info *rli_info = consensus_state_process.get_relay_log_info();

  if (!in_large_trx)
    changed_in_large_trx =
        (log.flag & Consensus_log_event_flag::FLAG_LARGE_TRX);

  if (with_check || changed_in_large_trx)
    mysql_rwlock_wrlock(&LOCK_consensuslog_rotate);

  if (consensus_state_process.get_status() ==
      Consensus_Log_System_Status::BINLOG_WORKING) {
    if ((error = consensus_append_log_entry(
             consensus_state_process.get_binlog(), log, consensus_index,
             nullptr, with_check))) {
      goto end;
    }
    if (*consensus_index == 0) goto end;
  } else {
    Master_info *mi = rli_info->mi;
    bool enable_rotate = false;

    mysql_mutex_lock(&mi->data_lock);
    if ((error = consensus_append_log_entry(&rli_info->relay_log, log,
                                            consensus_index, rli_info,
                                            with_check))) {
      mysql_mutex_unlock(&mi->data_lock);
      goto end;
    }
    if (*consensus_index == 0) {
      mysql_mutex_unlock(&mi->data_lock);
      goto end;
    }

    enable_rotate =
        !with_check && !(log.flag & Consensus_log_event_flag::FLAG_LARGE_TRX);
    if (enable_rotate) {
      mysql_mutex_lock(rli_info->relay_log.get_log_lock());
      error = rli_info->relay_log.rotate(false, &enable_rotate);
      mysql_mutex_unlock(rli_info->relay_log.get_log_lock());

      if (error) LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_LOG_ROTATE_ERROR);

      if (!error && enable_rotate) {
        bool use_new_created_thd = false;
        if (current_thd == nullptr) {
          use_new_created_thd = true;
          current_thd = create_internal_thd();
        }
        rli_info->relay_log.auto_purge();
        if (use_new_created_thd) {
          destroy_internal_thd(current_thd);
          current_thd = nullptr;
        }
      }
    }
    mysql_mutex_unlock(&mi->data_lock);
  }

  in_large_trx = (log.flag & Consensus_log_event_flag::FLAG_LARGE_TRX);

end:
  if (!error && *consensus_index > 0) {
    if (with_check || last_log_term != log.term) {
      set_local_system_log_if_greater(with_check ? *consensus_index : 0,
                                      last_log_term != log.term);
    }
    if (last_log_term != log.term) set_last_log_term(log.term);
  }

  if (with_check || changed_in_large_trx)
    mysql_rwlock_unlock(&LOCK_consensuslog_rotate);

  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());
  return error;
}

int ConsensusLogManager::write_log_entries(std::vector<ConsensusLogEntry> &logs,
                                           uint64 *max_index) {
  int error = 0;
  bool changed_in_large_trx = false;
  bool enable_rotate = false;
  bool do_rotate = false;
  DBUG_TRACE;

  // only follower will call this function
  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());

  uint64 last_log_term = get_last_log_term();
  Relay_log_info *rli_info = consensus_state_process.get_relay_log_info();
  MYSQL_BIN_LOG *log = consensus_state_process.get_consensus_log();

  if (!in_large_trx)
    changed_in_large_trx =
        (logs.back().flag & Consensus_log_event_flag::FLAG_LARGE_TRX);

  if (changed_in_large_trx) mysql_rwlock_wrlock(&LOCK_consensuslog_rotate);

  if ((error = consensus_append_multi_log_entries(log, logs, max_index,
                                                  rli_info))) {
    goto end;
  }

  enable_rotate = consensus_state_process.get_status() !=
                      Consensus_Log_System_Status::BINLOG_WORKING &&
                  !(logs.back().flag & Consensus_log_event_flag::FLAG_LARGE_TRX);
  if (enable_rotate) {
    Master_info *mi = rli_info->mi;
    mysql_mutex_lock(&mi->data_lock);

    mysql_mutex_lock(log->get_log_lock());
    error = log->rotate(false, &do_rotate);
    mysql_mutex_unlock(log->get_log_lock());

    if (error) LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_LOG_ROTATE_ERROR);

    if (!error && do_rotate) {
      bool use_new_created_thd = false;
      if (current_thd == nullptr) {
        use_new_created_thd = true;
        current_thd = create_internal_thd();
      }
      log->auto_purge();
      if (use_new_created_thd) {
        destroy_internal_thd(current_thd);
        current_thd = nullptr;
      }
    }

    mysql_mutex_unlock(&mi->data_lock);
  }

  in_large_trx = (logs.back().flag & Consensus_log_event_flag::FLAG_LARGE_TRX);

end:
  if (changed_in_large_trx) mysql_rwlock_unlock(&LOCK_consensuslog_rotate);

  if (!error && *max_index > 0 && last_log_term != logs.back().term) {
    set_local_system_log_if_greater(0, true);
    set_last_log_term(logs.back().term);
  }

  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());
  return error;
}

int ConsensusLogManager::get_log_directly(uint64 consensus_index,
                                          uint64 *consensus_term,
                                          std::string &log_content, bool *outer,
                                          uint *flag, uint64 *checksum,
                                          bool need_content) {
  int error = 0;
  DBUG_TRACE;

  if (consensus_index == 0) {
    *consensus_term = 1;
    *outer = false;
    *flag = 0;
    return error;
  }

  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  if (consensus_get_log_entry(consensus_log_manager.get_log_file_index(),
                              consensus_index, consensus_term, log_content,
                              outer, flag, checksum, need_content))
    error = 1;
  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  return error;
}

int ConsensusLogManager::prefetch_log_directly(THD *thd, uint64 channel_id,
                                               uint64 consensus_index) {
  int error = 0;
  DBUG_TRACE;

  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  if (consensus_prefetch_log_entries(thd, channel_id, consensus_index))
    error = 1;
  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  return error;
}

int ConsensusLogManager::get_log_entry(uint64 channel_id,
                                       uint64 consensus_index,
                                       uint64 *consensus_term,
                                       std::string &log_content, bool *outer,
                                       uint *flag, uint64 *checksum,
                                       bool fast_fail) {
  int error = 0;
  DBUG_TRACE;

  if (!opt_consensus_disable_fifo_cache && consensus_index > cache_index)
    return 1;

  if (consensus_index == 0) {
    *consensus_term = 0;
    log_content = "";
    return 0;
  }

  error = fifo_cache_manager->get_log_from_cache(
      consensus_index, consensus_term, log_content, outer, flag, checksum);
  if (error == ALREADY_SWAP_OUT) {
    uint64_t last_sync_index = sync_index;
    if (consensus_index > last_sync_index) {
      LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_GET_LOG_ENTRY_UNFLUSHED_ERROR,
                   consensus_index, last_sync_index);
      return 1;
    }

    ConsensusPreFetchChannel *channel =
        prefetch_manager->get_prefetch_channel(channel_id);

    if ((error = channel->get_log_from_prefetch_cache(
             consensus_index, consensus_term, log_content, outer, flag,
             checksum))) {
      if (!fast_fail) {
        error = get_log_directly(consensus_index, consensus_term, log_content,
                                 outer, flag, checksum,
                                 channel_id == 0 ? false : true);
      }
      channel->set_prefetch_request(consensus_index);
    }

    channel->dec_ref_count();
  }

  if (error)
    LogPluginErr(INFORMATION_LEVEL, ER_CONSENSUS_LOG_ENTRY_NOT_FOUND_FROM_CACHE,
                 channel_id, consensus_index, error);
  return error;
}

uint64_t ConsensusLogManager::get_left_log_size(uint64 start_log_index,
                                                uint64 max_packet_size) {
  uint64 total_size = 0;
  total_size += fifo_cache_manager->get_log_size_from_cache(
      start_log_index, cache_index, max_packet_size);
  // do not consider prefetch cache here
  return total_size;
}

int ConsensusLogManager::get_log_position(uint64 consensus_index,
                                          bool need_lock, char *log_name,
                                          my_off_t *pos) {
  int error = 0;
  DBUG_TRACE;
  if (need_lock)
    mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  if (consensus_get_log_position(get_log_file_index(), consensus_index,
                                 log_name, pos)) {
    error = 1;
  }
  if (need_lock)
    mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());
  return error;
}

int ConsensusLogManager::get_log_end_position(uint64 consensus_index,
                                              bool need_lock, char *log_name,
                                              my_off_t *pos) {
  int error = 0;
  DBUG_TRACE;
  if (need_lock)
    mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  if (consensus_get_log_end_position(get_log_file_index(), consensus_index,
                                     log_name, pos)) {
    error = 1;
  }
  if (need_lock)
    mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());
  return error;
}

uint64 ConsensusLogManager::get_next_trx_index(uint64 consensus_index,
                                               bool need_lock) {
  uint64 retIndex = consensus_index;
  DBUG_TRACE;

  if (need_lock)
    mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  retIndex = consensus_get_trx_end_index(get_log_file_index(), consensus_index);
  if (need_lock)
    mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  if (retIndex == 0) {
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_LOG_FIND_NEXT_TRX_ERROR,
                 consensus_index);
    abort();
  }

  LogPluginErr(INFORMATION_LEVEL, ER_CONSENSUS_LOG_FIND_NEXT_TRX_LOG,
               consensus_index, retIndex + 1);
  return retIndex + 1;
}

uint64 ConsensusLogManager::get_next_index_from_position(
    const char *log_file_name, my_off_t log_pos, bool need_lock) {
  bool reached_stop_point;
  uint64 current_term;
  uint64 next_index;
  DBUG_TRACE;

  if (need_lock)
    mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());

  next_index = consensus_get_next_index(log_file_name, false, 0, log_pos,
                                        reached_stop_point, current_term);

  if (need_lock)
    mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  return next_index;
}

int ConsensusLogManager::truncate_log(uint64 consensus_index) {
  int error = 0;
  uint64 start_index = 0;
  my_off_t offset;
  std::string file_name;
  bool use_new_created_thd = false;
  DBUG_TRACE;

  LogPluginErr(SYSTEM_LEVEL, ER_CONSENSUS_LOG_MANAGER_LOG_OPS, "truncate",
               consensus_index);

  prefetch_manager->stop_prefetch_threads();

  if (current_thd == nullptr) {
    use_new_created_thd = true;
    current_thd = create_internal_thd();
  }

  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  mysql_rwlock_wrlock(&LOCK_consensuslog_truncate);

  Relay_log_info *rli_info = consensus_state_process.get_relay_log_info();
  MYSQL_BIN_LOG *log = consensus_state_process.get_consensus_log();
  mysql_mutex_lock(log->get_log_lock());

  // truncate log file
  Relay_log_info *rli = consensus_state_process.get_status() ==
                                Consensus_Log_System_Status::BINLOG_WORKING
                            ? nullptr
                            : rli_info;
  if (consensus_find_log_by_index(get_log_file_index(), consensus_index,
                                  file_name, start_index)) {
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_FIND_LOG_ERROR, consensus_index,
                 "truncating consensus log");
    offset = 0;
    error = 1;
  } else if (consensus_find_pos_by_index(get_log_file_index(),
                                         file_name.c_str(), start_index,
                                         consensus_index, &offset)) {
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_LOG_FIND_INDEX_IN_FILE_ERROR,
                 consensus_index, file_name.c_str());
    offset = 0;
    error = 1;
  } else if (log->truncate_log(file_name.c_str(), offset, rli)) {
    error = 1;
  }

  if (!error) {
    set_sync_index(consensus_index - 1);
    set_current_index(consensus_index);
    truncate_local_system_log_if_lesser(consensus_index - 1);
    log_file_index->truncate_after(file_name);
    log_file_index->truncate_pos_map_of_file(start_index, consensus_index);
    fifo_cache_manager->trunc_log_from_cache(consensus_index);
    prefetch_manager->trunc_log_from_prefetch_cache(consensus_index);
  }

  // truncate cache
  mysql_mutex_unlock(log->get_log_lock());

  // reset the previous_gtid_set_of_relaylog after truncate log
  if (!error && rli != nullptr) {
    mysql_mutex_lock(&rli->data_lock);
    error = rli->reset_previous_gtid_set_of_consensus_log();
    mysql_mutex_unlock(&rli->data_lock);
  }

  mysql_rwlock_unlock(&LOCK_consensuslog_truncate);
  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  if (use_new_created_thd) {
    destroy_internal_thd(current_thd);
    current_thd = nullptr;
  }

  if (error) abort();

  prefetch_manager->start_prefetch_threads();
  return error;
}

int ConsensusLogManager::purge_log(uint64 consensus_index) {
  int error = 0;
  std::string file_name;
  uint64 start_index;
  uint64 purge_index = 0;
  bool use_new_created_thd = false;
  DBUG_TRACE;

  if (consensus_index == 0) return 0;

  if (current_thd == nullptr) {
    use_new_created_thd = true;
    current_thd = create_internal_thd();
  }

  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());

  if (consensus_state_process.get_status() ==
      Consensus_Log_System_Status::BINLOG_WORKING) {
    // server still work as leader, so we should at least retain 1 binlog file
    // apply pos must at the last binlog file
    purge_index = consensus_index;
  } else {
    Consensus_info *consensus_info = consensus_meta.get_consensus_info();
    Consensus_applier_info *consensus_applier_info =
        consensus_meta.get_applier_info();

    uint64 start_apply_index = consensus_info->get_start_apply_index();
    if (start_apply_index == 0) {
      // apply thread already start, use slave applied index as purge index
      purge_index = consensus_applier_info->get_consensus_apply_index();
    } else {
      // apply thread not start
      purge_index = start_apply_index;
    }

    purge_index = opt_cluster_log_type_instance
                      ? consensus_index
                      : std::min(purge_index, consensus_index);
  }

  LogPluginErr(SYSTEM_LEVEL, ER_CONSENSUS_LOG_MANAGER_LOG_OPS, "purge",
               purge_index);

  Relay_log_info *rli_info = consensus_state_process.get_relay_log_info();
  MYSQL_BIN_LOG *log = consensus_state_process.get_consensus_log();
  MYSQL_BIN_LOG *binlog = nullptr;

  if (consensus_state_process.get_status() ==
      Consensus_Log_System_Status::RELAY_LOG_WORKING) {
    binlog = consensus_state_process.get_binlog();
    binlog->lock_index();
    mysql_mutex_lock(&rli_info->data_lock);
  }

  log->lock_index();
  if (consensus_find_log_by_index(get_log_file_index(), purge_index, file_name,
                                  start_index)) {
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_FIND_LOG_ERROR, consensus_index,
                 "purging consensus log");
    error = 1;
  } else if (log->purge_logs(file_name.c_str(), false /**include*/,
                             false /*need index lock*/, true /*update threads*/,
                             nullptr, true)) {
    error = 1;
  }
  log->unlock_index();

  LogPluginErr(SYSTEM_LEVEL, ER_CONSENSUS_LOG_MANAGER_LOG_FILE_OPS,
               "purge before", file_name.c_str());

  if (consensus_state_process.get_status() ==
      Consensus_Log_System_Status::RELAY_LOG_WORKING) {
    mysql_mutex_unlock(&rli_info->data_lock);

    if (!error) {
      LOG_INFO log_info;
      if ((error = binlog->find_log_pos(&log_info, file_name.c_str(), false))) {
        LogPluginErr(ERROR_LEVEL, ER_BINLOG_CANT_FIND_LOG_IN_INDEX, error);
      } else {
        binlog->reopen_log_index(false);
        binlog->adjust_linfo_offsets(log_info.index_file_start_offset);
      }
    }

    binlog->unlock_index();
  }

  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  if (use_new_created_thd) {
    destroy_internal_thd(current_thd);
    current_thd = nullptr;
  }
  return error;
}

uint64 ConsensusLogManager::get_exist_log_length() {
  // first index in log index may not be exact value
  // so we should check whether sync_index is larger
  uint64 start_index = log_file_index->get_first_index();
  uint64 end_index = sync_index;
  if (end_index >= start_index)
    return end_index - start_index;
  else
    return 0;
}

void ConsensusLogManager::set_sync_index_if_greater(uint64 sync_index_arg) {
  for (;;) {
    uint64 old = sync_index.load();
    if (old >= sync_index_arg ||
        (old < sync_index_arg &&
         sync_index.compare_exchange_weak(old, sync_index_arg)))
      break;
  }
}

bool ConsensusLogManager::advance_commit_index_if_greater(uint64 arg,
                                                          bool force) {
  bool ret = false;

  mysql_mutex_lock(&LOCK_consensuslog_commit_advance);

  if ((force && arg > 0 && arg >= commit_index) || arg > commit_index) {
    ret = true;
    commit_index = arg;

    LogPluginErr(INFORMATION_LEVEL, ER_CONSENSUS_COMMIT_INDEX_ADVANCED,
                 commit_index.load(), local_system_log_index);
  }

  if (has_pending_local_system_log && commit_index >= local_system_log_index) {
    has_pending_local_system_log = false;
  }


  mysql_mutex_unlock(&LOCK_consensuslog_commit_advance);

  return ret;
}

// if local_sytem_log_index_arg > 0, exclusive Lock_consensus_rotate_lock must
// be held  before calling this funcation
bool ConsensusLogManager::set_local_system_log_if_greater(
    uint64 local_sytem_log_index_arg, bool force_singal) {
  bool ret = false;

  mysql_mutex_lock(&LOCK_consensuslog_commit_advance);

  if (local_sytem_log_index_arg > local_system_log_index)
    local_system_log_index = local_sytem_log_index_arg;

  if (local_sytem_log_index_arg > commit_index)
    has_pending_local_system_log = true;

  if (has_pending_local_system_log || force_singal)
    mysql_cond_broadcast(&COND_consensuslog_commit_advance);

  if (local_sytem_log_index_arg > 0) {
    LogPluginErr(INFORMATION_LEVEL, ER_CONSENSUS_LOCAL_SYSTEM_LOG_WRITTED,
                 local_system_log_index, commit_index.load());
  }

  mysql_mutex_unlock(&LOCK_consensuslog_commit_advance);


  return ret;
}

void ConsensusLogManager::truncate_local_system_log_if_lesser(
    uint64 truncate_index_arg) {
  mysql_mutex_lock(&LOCK_consensuslog_commit_advance);
  if (truncate_index_arg < local_system_log_index)
    local_system_log_index = truncate_index_arg;
  mysql_mutex_unlock(&LOCK_consensuslog_commit_advance);
}

bool ConsensusLogManager::wait_for_uncommitted_local_system_log(uint64 timetout) {
  if (has_pending_local_system_log) return true;

  mysql_mutex_lock(&LOCK_consensuslog_commit_advance);
  if (!has_pending_local_system_log) {
    struct timespec abstime;
    set_timespec_nsec(&abstime, timetout * 1000 * 1000); // 200ms
    mysql_cond_timedwait(&COND_consensuslog_commit_advance,
                         &LOCK_consensuslog_commit_advance, &abstime);
  }
  mysql_mutex_unlock(&LOCK_consensuslog_commit_advance);

  return has_pending_local_system_log;
}


int ConsensusLogManager::try_advance_commit_position(uint64 timeout) {
  uint64 index = 0;
  uint64 commit_index = 0;
  uint64 term = 0;
  char log_name[FN_REFLEN];
  my_off_t pos = 0;

  DBUG_TRACE;

  if (!opt_cluster_log_type_instance && rpl_consensus_is_leader() &&
      !wait_for_uncommitted_local_system_log(timeout)) {
    return 0;
  }

  term = rpl_consensus_get_term();
  commit_index = get_commit_index();
  index = rpl_consensus_get_commit_index();
  if (index <= commit_index) {
    index = rpl_consensus_timed_wait_commit_index_update(commit_index + 1, term,
                                                         timeout);
    if (index <= commit_index) return 0;
  }

  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  MYSQL_BIN_LOG *log = consensus_state_process.get_binlog();

  if (consensus_get_log_end_position(get_log_file_index(), index, log_name,
                                     &pos)) {
    mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());
    return 0;
  }

  mysql_mutex_lock(log->get_log_lock());
  if (advance_commit_index_if_greater(index, false)) {
    if (consensus_state_process.get_status() != BINLOG_WORKING)
      (void)log->switch_and_seek_log(log_name, pos, true);
    else
      binlog_update_end_pos(log, log_name, pos);
  }
  mysql_mutex_unlock(log->get_log_lock());

  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  LogPluginErr(INFORMATION_LEVEL, ER_CONSENSUS_COMMIT_POSITION_ADVANCED,
               log_name, pos, term, index);

  return 0;
}

static void *run_consensus_commit_position_advance(void *arg) {
  THD *thd = nullptr;
  DBUG_TRACE;
  thd = new THD;
  thd->set_new_thread_id();

  if (my_thread_init()) return nullptr;

  thd->thread_stack = (char *)&thd;
  thd->store_globals();

  std::atomic<bool> *is_running = reinterpret_cast<std::atomic<bool> *>(arg);

  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started && (*is_running))
    mysql_cond_wait(&COND_server_started, &LOCK_server_started);
  mysql_mutex_unlock(&LOCK_server_started);

  LogPluginErr(SYSTEM_LEVEL, ER_CONSENSUS_THREAD_STARTED,
               "commit position advance");

  while (*is_running && !rpl_consensus_is_shutdown()) {
    consensus_log_manager.try_advance_commit_position(200);
  }

  LogPluginErr(SYSTEM_LEVEL, ER_CONSENSUS_THREAD_STOPPED,
               "commit position advance",
               rpl_consensus_is_shutdown() ? "consensus service was shutdown"
                                           : "be killed");

  thd->release_resources();
  delete thd;
  my_thread_end();
  return nullptr;
}

int ConsensusLogManager::start_consensus_commit_advance_thread() {
  DBUG_TRACE;
  consensus_commit_advance_is_running = true;
  if (mysql_thread_create(key_thread_consensus_commit_advance,
                          &consensus_commit_advance_thread_handler, nullptr,
                          run_consensus_commit_position_advance,
                          (void *)&consensus_commit_advance_is_running)) {
    consensus_commit_advance_is_running = false;
    LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_CREATE_THRERD_ERROR,
                 "run_consensus_commit_position_advance");
    abort();
  }
  return 0;
}

int ConsensusLogManager::stop_consensus_commit_advance_thread() {
  DBUG_TRACE;
  if (inited && consensus_commit_advance_is_running) {
    consensus_commit_advance_is_running = false;
    mysql_mutex_lock(&LOCK_consensuslog_commit_advance);
    mysql_cond_broadcast(&COND_consensuslog_commit_advance);
    mysql_cond_broadcast(&COND_server_started);
    mysql_mutex_unlock(&LOCK_consensuslog_commit_advance);
    my_thread_join(&consensus_commit_advance_thread_handler, nullptr);
  }
  return 0;
}

int ConsensusLogManager::cleanup() {
  DBUG_TRACE;
  if (inited) {
    fifo_cache_manager->cleanup();
    prefetch_manager->cleanup();
    log_file_index->cleanup();
    cache_log->close();

    mysql_rwlock_destroy(&LOCK_consensuslog_truncate);
    mysql_rwlock_destroy(&LOCK_consensuslog_rotate);
    mysql_mutex_destroy(&LOCK_consensuslog_commit_advance);
    mysql_cond_destroy(&COND_consensuslog_commit_advance);

    delete fifo_cache_manager;
    delete prefetch_manager;
    delete log_file_index;
    delete cache_log;
  }
  return 0;
}