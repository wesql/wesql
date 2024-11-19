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

#include "se_system_vars.h"
#include "query_options.h"
#include "se_hton.h"
#include "sql/sql_error.h"
#include "transaction/se_transaction.h"
#include "transaction/se_transaction_factory.h"
#include "transactions/transaction_db_impl.h"
#include "core/options/options.h"
#include "core/cache/row_cache.h"
#include "core/util/sync_point.h"
#include "core/util/rate_limiter.h"

namespace smartengine
{

mysql_mutex_t se_sysvars_mutex;

char *se_data_dir = nullptr;

char *se_wal_dir = nullptr;

char *se_persistent_cache_dir = nullptr;

char se_backup_status[][16] = {"checkpoint",
                               "acquire",
                               "incremental",
                               "release",
                               ""};
table::BlockBasedTableOptions se_tbl_options;

char *se_cf_compression_per_level = nullptr;

char *se_cf_compression_opts = nullptr;

char *se_hotbackup_name = nullptr;

uint32_t se_flush_log_at_trx_commit = 1;

bool se_enable_2pc = 1; //2pc is not configurable in the near future, for atomic ddl.

long long se_row_cache_size = 0;

long long se_block_cache_size = 0;

//TODO: Zhao Dongsheng
common::DBOptions se_db_options = se_init_se_db_options();

common::ColumnFamilyOptions se_default_cf_options;

uint32_t se_wal_recovery_mode = (uint32_t)common::WALRecoveryMode::kAbsoluteConsistency;

bool se_parallel_wal_recovery = 0;

uint32_t se_parallel_recovery_thread_num = 0;

bool se_pause_background_work = false;

bool se_flush_memtable_var = false;

long long se_compact_cf_id = 0;

uint32_t se_disable_online_ddl = 0;

bool se_disable_instant_ddl = false;

bool se_disable_parallel_ddl = false;

ulong se_sort_buffer_size = 0;
 
bool opt_purge_invalid_subtable_bg = true;

unsigned long se_rate_limiter_bytes_per_sec = 0;

int32_t se_shrink_table_space = -1;

long long se_pending_shrink_subtable_id = 0;

bool se_strict_collation_check = 1;

char *se_strict_collation_exceptions = nullptr;

uint64_t se_query_trace_sum = 0;

bool se_query_trace_print_slow = false;

double se_query_trace_threshold_time = 100.0;

bool se_rpl_skip_tx_api_var = false;

bool se_skip_unique_key_check_in_boost_insert = false;

static int se_hotbackup(THD *const thd MY_ATTRIBUTE((__unused__)),
                             struct SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
                             void *const save MY_ATTRIBUTE((__unused__)),
                             struct st_mysql_value *const value)
{

  char buf[STRING_BUFFER_USUAL_SIZE];
  int len = sizeof(buf);
  int ret = 0;
  const char *cmd = value->val_str(value, buf, &len);
  SeTransaction *const tx = get_or_create_tx(thd);
  util::BackupSnapshot *backup_instance = util::BackupSnapshot::get_instance();
  BackupSnapshotId backup_id = 0;
  db::BinlogPosition binlog_pos;

  if (!my_core::thd_test_options(thd, OPTION_BEGIN)) {
    ret = smartengine::common::Status::kNotSupported;
    my_printf_error(ER_UNKNOWN_ERROR, "SE: should begin a trx first", MYF(0));
  }

  if (FAILED(ret)) {
    // do nothing
  } else if (0 == strcasecmp(cmd, se_backup_status[0])) {
    if (FAILED(backup_instance->lock_instance())) {
      if (ret == common::Status::kInitTwice) {
        my_printf_error(ER_UNKNOWN_ERROR, "SE: there is another backup job running\n", MYF(0));
      } else {
        my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to lock backup instance", MYF(0));
      }
    } else {
      se_register_tx(se_hton, thd, tx);
      tx->set_backup_running(true);
      if (FAILED(backup_instance->do_checkpoint(se_db))) {
        my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to do checkpoint for backup", MYF(0));
      } else {
        se_hotbackup_name = se_backup_status[0];
      }
    }
  } else if (0 == strcasecmp(cmd, se_backup_status[1])) {
    if (0 != strcasecmp(se_hotbackup_name, se_backup_status[0])) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: should execute command: %s before this command\n", MYF(0), se_backup_status[0]);
    } else if (!tx->get_backup_running()) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: should do checkpoint first\n", MYF(0));
    } else if (FAILED(backup_instance->accquire_backup_snapshot(se_db, &backup_id, binlog_pos))) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to accquire snapshots for backup", MYF(0));
    } else {
      se_hotbackup_name = se_backup_status[1];
    }
  } else if (0 == strcasecmp(cmd, se_backup_status[2])) {
    if (0 != strcasecmp(se_hotbackup_name, se_backup_status[1])) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: should execute command: %s before this command\n", MYF(0), se_backup_status[1]);
    } else if (!tx->get_backup_running()) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: should do checkpoint first\n", MYF(0));
    } else if (FAILED(backup_instance->record_incremental_extent_ids(se_db))) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to record incremental extent ids for backup", MYF(0));
    } else {
      se_hotbackup_name = se_backup_status[2];
    }
  } else if (0 == strcasecmp(cmd, se_backup_status[3])) {
    if (0 != strcasecmp(se_hotbackup_name, se_backup_status[2])) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: should execute command: %s before this command\n", MYF(0), se_backup_status[2]);
    } else if (!tx->get_backup_running()) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: should do checkpoint first\n", MYF(0));
    } else if (FAILED(backup_instance->release_current_backup_snapshot(se_db))) {
      my_printf_error(ER_UNKNOWN_ERROR, "SE: failed to release snapshots for backup", MYF(0));
    } else {
      se_hotbackup_name = se_backup_status[4];
    }
  } else {
    my_printf_error(ER_UNKNOWN_ERROR, "SE: invalid command: %s\n", MYF(0), cmd);
    ret = smartengine::common::Status::kInvalidArgument;
  }

  if (FAILED(ret)) {
    if (ret != smartengine::common::Status::kInitTwice) {
      tx->rollback();
    }
    ret = HA_ERR_INTERNAL_ERROR;
  }
  return ret;
}

static void se_hotbackup_stub(THD *const thd,
                                   struct SYS_VAR *const var,
                                   void *const var_ptr,
                                   const void *const save)
{}

static void se_set_mutex_backtrace_threshold_ns(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const uint64_t mutex_backtrace_threshold_ns = *static_cast<const uint64_t*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetDBOptions({{
          "mutex_backtrace_threshold_ns",
          std::to_string(mutex_backtrace_threshold_ns)}});
  se_db_options.mutex_backtrace_threshold_ns = mutex_backtrace_threshold_ns;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_row_cache_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  auto value = *static_cast<const long long*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  if (se_row_cache_size > 0) {
    assert(se_db_options.row_cache != nullptr);
    se_row_cache_size = value;
    se_db_options.row_cache->set_capacity(se_row_cache_size);
  // ToDo: how about initialize a new row cache if not set at begin?
  }
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_block_cache_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  auto value = *static_cast<const long long*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_block_cache_size = value;
  if (!se_tbl_options.no_block_cache) {
    assert(se_tbl_options.block_cache != nullptr);
    se_tbl_options.block_cache->SetCapacity(se_block_cache_size);
  }
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_write_buffer_size (
  THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  auto value = *static_cast<const unsigned long*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{"write_buffer_size", std::to_string(value)}});
  se_default_cf_options.write_buffer_size = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_db_write_buffer_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  auto value = *static_cast<const unsigned long*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{"db_write_buffer_size", std::to_string(value)}});
  se_db_options.db_write_buffer_size = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_db_total_write_buffer_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  auto value = *static_cast<const unsigned long*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{"db_total_write_buffer_size", std::to_string(value)}});
  se_db_options.db_total_write_buffer_size = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_pause_background_work(
    my_core::THD *const thd MY_ATTRIBUTE((__unused__)),
    struct SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
    void *const var_ptr MY_ATTRIBUTE((__unused__)), const void *const save) {
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  const bool pause_requested = *static_cast<const bool *>(save);
  if (se_pause_background_work != pause_requested) {
    if (pause_requested) {
      se_db->PauseBackgroundWork();
    } else {
      se_db->ContinueBackgroundWork();
    }
    se_pause_background_work = pause_requested;
  }
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_dump_memtable_limit_size(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.dump_memtable_limit_size = *static_cast<const int *>(save);

  se_db->SetDBOptions({{
      "dump_memtable_limit_size",
      std::to_string(se_db_options.dump_memtable_limit_size)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static int se_flush_memtable(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    struct st_mysql_value *const value) {
  sql_print_information("SE: Manual memtable flush\n");
  DBUG_EXECUTE_IF("before_sleep_in_flush_collect_stats",
  util::SyncPoint::GetInstance()->SetCallBack("StorageManager::apply::before_stats_collect", [&](void* arg) {
                                        sleep(20);
                                        fprintf(stdout, "sleep 20s before storage manager stats collect\n");
                                        });
  util::SyncPoint::GetInstance()->EnableProcessing();
  );
  se_flush_all_memtables();
  DBUG_EXECUTE_IF("after_sleep_in_flush_collect_stats",
  util::SyncPoint::GetInstance()->DisableProcessing();
  );
  return HA_EXIT_SUCCESS;
}

static void se_flush_memtable_stub(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {}

static void se_set_flush_delete_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "flush_delete_percent",
          std::to_string(value)}});
  se_default_cf_options.flush_delete_percent = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_flush_delete_percent_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "flush_delete_percent_trigger",
          std::to_string(value)}});
  se_default_cf_options.flush_delete_percent_trigger = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_flush_delete_record_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "flush_delete_record_trigger",
          std::to_string(value)}});
  se_default_cf_options.flush_delete_record_trigger = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_disable_auto_compactions(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const bool value = *static_cast<const bool*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "disable_auto_compactions",
          value ? "true" : "false"}});
  se_default_cf_options.disable_auto_compactions = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_compaction_delete_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "compaction_delete_percent",
          std::to_string(value)}});
  se_default_cf_options.compaction_delete_percent = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_level0_file_num_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "level0_file_num_compaction_trigger",
          std::to_string(value)}});
  se_default_cf_options.level0_file_num_compaction_trigger = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_level0_layer_num_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "level0_layer_num_compaction_trigger",
          std::to_string(value)}});
  se_default_cf_options.level0_layer_num_compaction_trigger = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_level1_extents_major_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "level1_extents_major_compaction_trigger",
          std::to_string(value)}});
  se_default_cf_options.level1_extents_major_compaction_trigger = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_level2_usage_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int64_t value = *static_cast<const uint64_t*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "level2_usage_percent",
          std::to_string(value)}});
  se_default_cf_options.level2_usage_percent = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_compaction_task_extents_limit(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const int value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
       "compaction_task_extents_limit",
          std::to_string(value)}});
  se_default_cf_options.compaction_task_extents_limit = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static int se_compact_column_family(THD *const thd,
                                    struct SYS_VAR *const var,
                                    void *const var_ptr,
                                    struct st_mysql_value *const value) {
  long long mvalue = 0;
  assert(value != nullptr);

  if (value->val_int(value, &mvalue)) {
    /* The value is NULL. That is invalid. */
    __HANDLER_LOG(ERROR, "SE: Can't parse value for compact\n");
    return HA_EXIT_FAILURE;
  }

  // get cfd_id and compaction type
  uint32_t cf_id = mvalue & 0xffff;
  int32_t compact_type = mvalue >> 32;
  if (0 == cf_id) {
    if (nullptr != se_db) {
      __HANDLER_LOG(INFO, "SE: Manual compaction of all sub tables: %u", cf_id);
      se_db->CompactRange(compact_type);
    }
  } else {
    auto cfh = cf_manager.get_cf(cf_id);
    if (cfh != nullptr  && se_db != nullptr) {
      __HANDLER_LOG(INFO, "SE: Manual compaction of sub table: %u\n", cf_id);
      se_db->CompactRange(cfh, compact_type);
    }
  }
  return HA_EXIT_SUCCESS;
}

static void se_compact_column_family_stub(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {}

static void se_set_bottommost_level(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const uint64_t value = *static_cast<const int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "bottommost_level",
          std::to_string(value)}});
  se_default_cf_options.bottommost_level = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_estimate_cost_depth(THD *thd, struct SYS_VAR *const var,
                                            void *const var_ptr,
                                            const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.estimate_cost_depth = *static_cast<const int *>(save);

  se_db->SetDBOptions({{"estimate_cost_depth",
                      std::to_string(se_db_options.estimate_cost_depth)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_stats_dump_period_sec(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  assert(save != nullptr);
  const unsigned int dump_period_sec = *static_cast<const unsigned int*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetDBOptions({{
          "stats_dump_period_sec",
          std::to_string(dump_period_sec)}});
  se_db_options.stats_dump_period_sec = dump_period_sec;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

/*
  This function allows setting the rate limiter's bytes per second value
  but only if the rate limiter is turned on which has to be done at startup.
  If the rate is already 0 (turned off) or we are changing it to 0 (trying
  to turn it off) this function will push a warning to the client and do
  nothing.
  This is similar to the code in innodb_doublewrite_update (found in
  storage/innobase/handler/ha_innodb.cc).
*/
static void se_set_rate_limiter_bytes_per_sec(
    THD *thd,
    struct SYS_VAR *const var,
    void *const var_ptr,
    const void *const save) {
  const uint64_t new_val = *static_cast<const uint64_t *>(save);
  if (new_val == 0 || se_rate_limiter_bytes_per_sec == 0) {
    /*
      If a rate_limiter was not enabled at startup we can't change it nor
      can we disable it if one was created at startup
    */
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "SE: smartengine_rate_limiter_bytes_per_sec cannot "
                        "be dynamically changed to or from 0.  Do a clean "
                        "shutdown if you want to change it from or to 0.");
  } else if (new_val != se_rate_limiter_bytes_per_sec) {
    /* Apply the new value to the rate limiter and store it locally */
    assert(se_rate_limiter != nullptr);
    se_rate_limiter_bytes_per_sec = new_val;
    se_rate_limiter->SetBytesPerSecond(new_val);
  }
}

static void se_set_idle_tasks_schedule_time(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.idle_tasks_schedule_time = *static_cast<const int *>(save);

  se_db->SetDBOptions({{
      "idle_tasks_schedule_time",
      std::to_string(se_db_options.idle_tasks_schedule_time)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static int se_shrink_table_space_func(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    struct st_mysql_value *const value) {
  assert(value != nullptr);

  int ret = HA_EXIT_SUCCESS;
  char buf[FN_REFLEN];
  int len = sizeof(buf);
  const char *const value_raw = value->val_str(value, buf, &len);
  std::string value_str = std::string(value_raw);

  DBUG_EXECUTE_IF("before_shrink", sleep(2););
  se_drop_idx_thread.enter_race_condition();
  if (se_shrink_table_space >= 0) {
    my_printf_error(ER_UNKNOWN_ERROR,
                        "Other shrink extent space is running", MYF(0));
    ret = HA_ERR_INTERNAL_ERROR;
    se_drop_idx_thread.exit_race_condition();
    return ret;
  } else if (se_drop_idx_thread.is_run()) {
    my_printf_error(ER_UNKNOWN_ERROR,
                        "Background drop index is running", MYF(0));
    ret = HA_ERR_INTERNAL_ERROR;
    se_drop_idx_thread.exit_race_condition();
    return ret;
  } else if (se_inplace_populate_indexes > 0) {
    my_printf_error(ER_UNKNOWN_ERROR,
                        "Create second index is running", MYF(0));
    ret = HA_ERR_INTERNAL_ERROR;
    se_drop_idx_thread.exit_race_condition();
    return ret;
  }
  se_shrink_table_space = std::stoi(value_str);
  sql_print_information("SHRINK EXTENT SPACE set se_shrink_table_space = %d", se_shrink_table_space);
  se_drop_idx_thread.exit_race_condition();

  DBUG_EXECUTE_IF("sleep_in_shrink", sleep(5););
  if (-1 == se_shrink_table_space) {
    //do nothing
  } else if (se_db->shrink_table_space(se_shrink_table_space) != smartengine::common::Status::kOk) {
    my_printf_error(ER_UNKNOWN_ERROR,
                        "Shrink table space not finished, please check the log of SE", MYF(0));
    ret = HA_ERR_INTERNAL_ERROR;
  }

  se_drop_idx_thread.enter_race_condition();
  se_shrink_table_space = -1;
  sql_print_information("SHRINK EXTENT SPACE set se_shrink_table_space = -1");
  se_drop_idx_thread.exit_race_condition();

  return ret;
}

static void se_shrink_table_space_sub_func(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {}

static int se_reset_subtable_pending_shrink(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    struct st_mysql_value *const value) {
  long long mvalue = 0;
  uint64_t subtable_id = 0;
  assert(value != nullptr);

  if (value->val_int(value, &mvalue)) {
    /**The value is NULL. That is invalid.*/
    HANDLER_LOG(ERROR, "SE: Can't parse value for reset_pending_shrink");
    return HA_EXIT_FAILURE;
  } else {
    subtable_id = static_cast<uint64_t>(mvalue);
    se_db->reset_pending_shrink(subtable_id);
    HANDLER_LOG(INFO, "SE: success to reset pending shrink", "index_id", subtable_id);
  }

  return HA_EXIT_SUCCESS;
}

static void se_reset_subtable_pending_shrink_stub(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {}

static void se_set_auto_shrink_enabled(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.auto_shrink_enabled = *static_cast<const bool *>(save);

  se_db->SetDBOptions({{
      "auto_shrink_enabled",
      std::to_string(se_db_options.auto_shrink_enabled)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_max_free_extent_percent(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.max_free_extent_percent = *static_cast<const int *>(save);

  se_db->SetDBOptions({{
      "max_free_extent_percent",
      std::to_string(se_db_options.max_free_extent_percent)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_shrink_allocate_interval(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.shrink_allocate_interval = *static_cast<const int *>(save);

  se_db->SetDBOptions({{
      "shrink_allocate_interval",
      std::to_string(se_db_options.shrink_allocate_interval)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_max_shrink_extent_count(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.max_shrink_extent_count = *static_cast<const int *>(save);

  se_db->SetDBOptions({{
      "max_shrink_extent_count",
      std::to_string(se_db_options.max_shrink_extent_count)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_total_max_shrink_extent_count(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.total_max_shrink_extent_count = *static_cast<const int *>(save);

  se_db->SetDBOptions({{
      "total_max_shrink_extent_count",
      std::to_string(se_db_options.total_max_shrink_extent_count)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_auto_shrink_schedule_interval(THD *thd,
                                                      struct SYS_VAR *const var,
                                                      void *const var_ptr,
                                                      const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.auto_shrink_schedule_interval =
      *static_cast<const int *>(save);

  se_db->SetDBOptions(
      {{"auto_shrink_schedule_interval",
        std::to_string(se_db_options.auto_shrink_schedule_interval)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

void se_set_collation_exception_list(const char *const exception_list) {
  assert(se_collation_exceptions != nullptr);

  if (!se_collation_exceptions->set_patterns(exception_list)) {
    warn_about_bad_patterns(se_collation_exceptions,
                                     "strict_collation_exceptions");
  }
}

static void se_set_collation_exception_list(THD *const thd,
                                          struct SYS_VAR *const var,
                                          void *const var_ptr,
                                          const void *const save) {
  const char *const val = *static_cast<const char *const *>(save);

  se_set_collation_exception_list(val == nullptr ? "" : val);

  *static_cast<const char **>(var_ptr) = val;
}

static void se_set_query_trace_sum(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
     const void *save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  int64_t value = *static_cast<const int64_t*>(save);
  switch (value) {
  case 0:
    se_query_trace_sum = 0;
    smartengine::monitor::QueryPerfContext::opt_trace_sum_ = false;
    break;
  case 1:
    se_query_trace_sum = 1;
    smartengine::monitor::QueryPerfContext::opt_trace_sum_ = true;
    break;
  case 2:
    smartengine::monitor::get_tls_query_perf_context()->clear_stats();
    break;
  default:
    break;
  }

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_query_trace_print_slow(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const bool print_slow = *static_cast<const bool*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_query_trace_print_slow = print_slow;
  smartengine::monitor::QueryPerfContext::opt_print_slow_ = print_slow;
  SE_LOG(INFO, "SET xengie_query_trace_print_slow", K(print_slow));
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_query_trace_threshold_time(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const double threshold_time = *static_cast<const double*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_query_trace_threshold_time = threshold_time;
  smartengine::monitor::QueryPerfContext::opt_threshold_time_ = threshold_time;
  SE_LOG(INFO, "SET se_query_trace_threshold_time", K(threshold_time));
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}


static void se_set_scan_add_blocks_limit(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  assert(save != nullptr);
  const uint64_t value = *static_cast<const uint64_t*>(save);
  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);
  se_db->SetOptions({{
          "scan_add_blocks_limit",
          std::to_string(value)}});
  se_default_cf_options.scan_add_blocks_limit = value;
  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_master_thread_monitor_interval_ms(THD *thd,
                                                     struct SYS_VAR *const var,
                                                     void *const var_ptr,
                                                     const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.monitor_interval_ms = *static_cast<const uint64_t *>(save);

  se_db->SetDBOptions({{"monitor_interval_ms",
                        std::to_string(se_db_options.monitor_interval_ms)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static void se_set_master_thread_compaction_enabled(THD *thd,
                                                    struct SYS_VAR *const var,
                                                    void *const var_ptr,
                                                    const void *const save) {
  assert(save != nullptr);

  SE_MUTEX_LOCK_CHECK(se_sysvars_mutex);

  se_db_options.master_thread_compaction_enabled =
      *static_cast<const bool *>(save);

  se_db->SetDBOptions(
      {{"master_thread_compaction_enabled",
        std::to_string(se_db_options.master_thread_compaction_enabled)}});

  SE_MUTEX_UNLOCK_CHECK(se_sysvars_mutex);
}

static TYPELIB se_persistent_cache_mode_typelib = {
    array_elements(common::persistent_cache_mode_names) - 1,
    "se_persistent_cache_mode_typelib",
    common::persistent_cache_mode_names,
    nullptr};

uint64_t se_persistent_cache_mode = 0;

static MYSQL_SYSVAR_STR(
    data_dir, se_data_dir,
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
    "SE data directory",
    nullptr, nullptr,
    "smartengine");

static MYSQL_SYSVAR_STR(
    wal_dir, se_wal_dir, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::wal_dir for SE",
    nullptr, nullptr,
    "smartengine");

static MYSQL_SYSVAR_ULONG(
    block_size,
    se_tbl_options.block_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "BlockBasedTableOptions::block_size for SE",
    nullptr, nullptr,
    16 * 1024, 1L, LONG_MAX, 0);

static MYSQL_SYSVAR_STR(
    compression_per_level, se_cf_compression_per_level,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::compression_per_level for SE",
    nullptr, nullptr, "kNoCompression:kZSTD:kZSTD");

static MYSQL_SYSVAR_STR(
    compression_options, se_cf_compression_opts,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::compression_opts for SE",
    nullptr, nullptr, "-14:1:0");

static MYSQL_SYSVAR_STR(
    hotbackup, se_hotbackup_name,
    PLUGIN_VAR_RQCMDARG,
    "Hot Backup",
    se_hotbackup, se_hotbackup_stub, "");

static MYSQL_THDVAR_ULONG(
    lock_wait_timeout, PLUGIN_VAR_RQCMDARG,
    "Number of seconds to wait for lock",
    nullptr, nullptr,
    1, 1, 1024 * 1024 * 1024, 0);

static MYSQL_THDVAR_BOOL(
    deadlock_detect, PLUGIN_VAR_RQCMDARG,
    "Enables deadlock detection",
    nullptr, nullptr, FALSE);

static MYSQL_THDVAR_ULONG(
    max_row_locks, PLUGIN_VAR_RQCMDARG,
    "Maximum number of locks a transaction can have",
    nullptr, nullptr, 
    1024 * 1024 * 1024, 1, 1024 * 1024 * 1024, 0);

static MYSQL_THDVAR_BOOL(
    lock_scanned_rows, PLUGIN_VAR_RQCMDARG,
    "Take and hold locks on rows that are scanned but not updated",
    nullptr, nullptr, FALSE);

static MYSQL_SYSVAR_UINT(
    flush_log_at_trx_commit,
    se_flush_log_at_trx_commit,
    PLUGIN_VAR_RQCMDARG,
    "Sync on transaction commit. Similar to "
    "innodb_flush_log_at_trx_commit. 1: sync on commit, "
    "0: not sync until wal_bytes_per_sync, 2: sync per second",
    nullptr, nullptr, 1, 0, 2, 0);

static MYSQL_SYSVAR_BOOL(
    enable_2pc, se_enable_2pc,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Enable two phase commit for SE",
    nullptr, nullptr, TRUE);

static MYSQL_SYSVAR_ULONG(
    batch_group_slot_array_size,
    se_db_options.batch_group_slot_array_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::batch_group_slot_array_size for SE",
    nullptr, nullptr,
    5, 1, LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    batch_group_max_group_size,
    se_db_options.batch_group_max_group_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::batch_group_max_group_size for SE",
    nullptr, nullptr,
    15, 1, LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    batch_group_max_leader_wait_time_us,
    se_db_options.batch_group_max_leader_wait_time_us,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::batch_group_max_leader_wait_time_us for SE",
    nullptr, nullptr,
    50, 1, LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    mutex_backtrace_threshold_ns,
    se_db_options.mutex_backtrace_threshold_ns,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::mutex_backtrace_threshold_ns for SE",
    nullptr, se_set_mutex_backtrace_threshold_ns,
    1000000000UL, 0L, LONG_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(
    row_cache_size,
    se_row_cache_size,
    PLUGIN_VAR_RQCMDARG,
    "row_cache size for SE",
    nullptr, se_set_row_cache_size,
    0, 0, INT64_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(
    block_cache_size,
    se_block_cache_size,
    PLUGIN_VAR_RQCMDARG,
    "block_cache size for SE",
    nullptr, se_set_block_cache_size,
    128 * 1024 * 1024, 0, INT64_MAX, 1024);

static MYSQL_SYSVAR_ULONG(
    table_cache_size,
    se_db_options.table_cache_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::table_cache_size for SE",
    nullptr, nullptr,
    se_db_options.table_cache_size, 1 * 1024 * 1024, ULONG_MAX, 0);

static MYSQL_SYSVAR_INT(
    table_cache_numshardbits,
    se_db_options.table_cache_numshardbits,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::table_cache_numshardbits for SE",
    nullptr, nullptr,
    se_db_options.table_cache_numshardbits, 1, 15, 0);

static MYSQL_SYSVAR_ULONG(
    memtable_size,
    se_default_cf_options.write_buffer_size,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::write_buffer_size for SE",
    nullptr, se_set_write_buffer_size,
    128 * 1024 * 1024, 4096, INT64_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    db_write_buffer_size,
    se_db_options.db_write_buffer_size,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::db_write_buffer_size for SE",
    nullptr, se_set_db_write_buffer_size,
    100L << 30, 0L, LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    total_memtable_size,
    se_db_options.db_total_write_buffer_size,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::db_total_write_buffer_size for SE",
    nullptr, se_set_db_total_write_buffer_size,
    128 * 1024 * 1024, 0L, LONG_MAX, 0);

static MYSQL_SYSVAR_INT(
    max_write_buffer_number_to_maintain,
    se_default_cf_options.max_write_buffer_number_to_maintain,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::max_write_buffer_number_to_maintain for SE",
    nullptr, nullptr,
    0, 0, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    min_write_buffer_number_to_merge,
    se_default_cf_options.min_write_buffer_number_to_merge,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::min_write_buffer_number_to_merge for SE",
    nullptr, nullptr,
    1, 0, INT_MAX, 0);

static MYSQL_THDVAR_BOOL(
    write_disable_wal, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "WriteOptions::disableWAL for SE",
    nullptr, nullptr, smartengine::common::WriteOptions().disableWAL);

static MYSQL_THDVAR_BOOL(
    unsafe_for_binlog, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Allowing statement based binary logging which may break consistency",
    nullptr, nullptr, FALSE);

static MYSQL_SYSVAR_ULONG(
    total_wal_size,
    se_db_options.max_total_wal_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::max_total_wal_size for SE",
    nullptr, nullptr,
    128 * 1024 * 1024, 8 * 1024 * 1024, INT64_MAX, 0);

static MYSQL_SYSVAR_UINT(
    wal_recovery_mode,
    se_wal_recovery_mode,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::wal_recovery_mode for SE. Default is kAbsoluteConsistency",
    nullptr, nullptr,
    (uint)common::WALRecoveryMode::kAbsoluteConsistency,
    (uint)common::WALRecoveryMode::kTolerateCorruptedTailRecords,
    (uint)common::WALRecoveryMode::kSkipAnyCorruptedRecords, 0);

static MYSQL_SYSVAR_BOOL(
    parallel_wal_recovery,
    se_parallel_wal_recovery,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::parallel_wal_recovery for SE. Default is FALSE",
    nullptr, nullptr, FALSE);

static MYSQL_SYSVAR_UINT(
    parallel_recovery_thread_num,
    se_parallel_recovery_thread_num,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::parallel_recovery_thread_num for SE. Default is 0",
    nullptr, nullptr,
    0, 0, 1024, 0);

static MYSQL_SYSVAR_ULONG(
    concurrent_writable_file_buffer_num,
    se_db_options.concurrent_writable_file_buffer_num,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::concurrent_writable_file_buffer_num for SE",
    nullptr, nullptr,
    DEFAULT_SE_CONCURRENT_WRITABLE_FILE_BUFFER_NUMBER, 1, LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    concurrent_writable_file_single_buffer_size,
    se_db_options.concurrent_writable_file_single_buffer_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::concurrent_writable_file_single_buffer_size for SE",
    nullptr, nullptr,
    DEFAULT_SE_CONCURRENT_WRITABLE_FILE_SINGLE_BUFFER_SIZE, 1, LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    concurrent_writable_file_buffer_switch_limit,
    se_db_options.concurrent_writable_file_buffer_switch_limit,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::concurrent_writable_file_buffer_switch_limit for SE",
    nullptr, nullptr,
    DEFAULT_SE_CONCURRENT_WRITABLE_FILE_BUFFER_SWITCH_LIMIT, 1, LONG_MAX, 0);

static MYSQL_THDVAR_ULONG(
    bulk_load_size, PLUGIN_VAR_RQCMDARG,
    "Max #records in a batch for bulk-load mode",
    nullptr, nullptr,
    10000, 1, 1024 * 1024 * 1024, 0);

static MYSQL_SYSVAR_BOOL(
    pause_background_work, se_pause_background_work,
    PLUGIN_VAR_RQCMDARG,
    "Disable all se background operations",
    nullptr, se_set_pause_background_work, FALSE);

static MYSQL_SYSVAR_INT(
    max_background_dumps,
    se_db_options.max_background_dumps,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::max_background_dumps for SE",
    nullptr, nullptr,
    se_db_options.max_background_dumps, 1, 64, 0);

static MYSQL_SYSVAR_ULONG(
    dump_memtable_limit_size,
    se_db_options.dump_memtable_limit_size,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::dump_memtable_limit_size for SE",
    nullptr, se_set_dump_memtable_limit_size,
    0, 0, ULONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    flush_memtable,
    se_flush_memtable_var,
    PLUGIN_VAR_RQCMDARG,
    "Forces memstore flush which may block all write requests so be careful",
    se_flush_memtable, se_flush_memtable_stub,
    FALSE);

static MYSQL_SYSVAR_INT(
    flush_delete_percent,
    se_default_cf_options.flush_delete_percent,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::flush_delete_percent for SE",
    nullptr, se_set_flush_delete_percent,
    70, 0L, 100, 0);

static MYSQL_SYSVAR_INT(
    flush_delete_percent_trigger,
    se_default_cf_options.flush_delete_percent_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::flush_delete_percent_trigger for SE",
    nullptr, se_set_flush_delete_percent_trigger,
    700000, 10, 1<<30, 0);

static MYSQL_SYSVAR_INT(
    flush_delete_record_trigger,
    se_default_cf_options.flush_delete_record_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::flush_delete_record_trigger for SE",
    nullptr, se_set_flush_delete_record_trigger,
    700000, 1, 1<<30, 0);

static MYSQL_SYSVAR_INT(
    flush_threads,
    se_db_options.max_background_flushes,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::max_background_flushes for SE",
    nullptr, nullptr,
    1, 1, 1024, 0);

static MYSQL_SYSVAR_INT(
    base_background_compactions,
    se_db_options.base_background_compactions,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::base_background_compactions for SE",
    nullptr, nullptr,
    se_db_options.base_background_compactions, -1, 64, 0);

static MYSQL_SYSVAR_INT(
    compaction_threads,
    se_db_options.max_background_compactions,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::max_background_compactions for SE",
    nullptr, nullptr,
    1, 1, 1024, 0);

static MYSQL_SYSVAR_BOOL(
    disable_auto_compactions,
    se_default_cf_options.disable_auto_compactions,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::disable_auto_compaction for SE",
    nullptr, se_set_disable_auto_compactions,
    false);

static MYSQL_SYSVAR_INT(
    compaction_delete_percent,
    se_default_cf_options.compaction_delete_percent,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::compaction_delete_percent for SE",
    nullptr, se_set_compaction_delete_percent,
    50, 0L, 100, 0);

static MYSQL_SYSVAR_BOOL(
    level_compaction_dynamic_level_bytes,
    se_default_cf_options.level_compaction_dynamic_level_bytes,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::level_compaction_dynamic_level_bytes for SE",
    nullptr, nullptr, true);

static MYSQL_SYSVAR_INT(
    level0_file_num_compaction_trigger,
    se_default_cf_options.level0_file_num_compaction_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level0_file_num_compaction_trigger for SE",
    nullptr, se_set_level0_file_num_compaction_trigger,
    64, -1, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    level0_layer_num_compaction_trigger,
    se_default_cf_options.level0_layer_num_compaction_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level0_layer_num_compaction_trigger for SE",
    nullptr, se_set_level0_layer_num_compaction_trigger,
    2, -1, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    level1_extents_major_compaction_trigger,
    se_default_cf_options.level1_extents_major_compaction_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level1_extents_major_compaction_trigger for SE",
    nullptr, se_set_level1_extents_major_compaction_trigger,
    1000, -1, INT_MAX, 0);

static MYSQL_SYSVAR_LONG(
    level2_usage_percent,
    se_default_cf_options.level2_usage_percent,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level2_usage_percent for SE",
    nullptr, se_set_level2_usage_percent,
    70, 0L, 100, 0);

static MYSQL_SYSVAR_INT(
    compaction_task_extents_limit,
    se_default_cf_options.compaction_task_extents_limit,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::compaction_task_extents_limit for SE",
    nullptr, se_set_compaction_task_extents_limit,
    1000, 1, 1<<30, 0);

static MYSQL_SYSVAR_LONGLONG(
    compact, se_compact_cf_id,
    PLUGIN_VAR_RQCMDARG,
    "Compact sub table",
    se_compact_column_family,
    se_compact_column_family_stub,
    0, 0, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    bottommost_level,
    se_default_cf_options.bottommost_level,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::bottommost_level for SE",
    nullptr, se_set_bottommost_level,
    se_default_cf_options.bottommost_level, 0L, 2, 0);

static MYSQL_SYSVAR_UINT(
    disable_online_ddl,
    se_disable_online_ddl,
    PLUGIN_VAR_RQCMDARG,
    "disable online ddl feature if necessary."
    "0: not disable online ddl,"
    "1: disable online-rebuild ddl, like modify pk,"
    "2: disable all type online-ddl include rebuild/norebuild",
    nullptr, nullptr,
    0, 0, 2, 0);

static MYSQL_SYSVAR_BOOL(
    disable_instant_ddl,
    se_disable_instant_ddl,
    PLUGIN_VAR_RQCMDARG,
    "disable instant ddl feature if necessary.",
    nullptr, nullptr, FALSE);

static MYSQL_SYSVAR_BOOL(
    disable_parallel_ddl,
    se_disable_parallel_ddl,
    PLUGIN_VAR_RQCMDARG,
    "disable parall ddl feature if necessary.",
    nullptr, nullptr, false);

static MYSQL_SYSVAR_ULONG(
    sort_buffer_size,
    se_sort_buffer_size,
    PLUGIN_VAR_RQCMDARG,
    "Memory buffer size for index creation",
    NULL, NULL,
    4 * 1024 * 1024, 64 * 1024, (((ulonglong)16) << 30), 0);

static MYSQL_SYSVAR_BOOL(
    purge_invalid_subtable_bg,
    opt_purge_invalid_subtable_bg,
    PLUGIN_VAR_RQCMDARG,
    "Turn on to enable purging invalid subtable in background",
    nullptr, nullptr, true);

static MYSQL_SYSVAR_ULONG(
    estimate_cost_depth,
    se_db_options.estimate_cost_depth,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::estimate_cost_depth for SE",
    nullptr, se_set_estimate_cost_depth,
    se_db_options.estimate_cost_depth, 0, ULONG_MAX, 0);

static MYSQL_SYSVAR_UINT(
    stats_dump_period_sec,
    se_db_options.stats_dump_period_sec,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::stats_dump_period_sec for SE",
    nullptr, se_set_stats_dump_period_sec,
    se_db_options.stats_dump_period_sec, 0, INT_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    rate_limiter_bytes_per_sec,
    se_rate_limiter_bytes_per_sec,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::rate_limiter bytes_per_sec for SE",
    nullptr, se_set_rate_limiter_bytes_per_sec,
    0L, 0L, ULONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    idle_tasks_schedule_time,
    se_db_options.idle_tasks_schedule_time,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::idle_tasks_schedule_time for SE",
    nullptr, se_set_idle_tasks_schedule_time,
    se_db_options.idle_tasks_schedule_time, 0, ULONG_MAX, 0);

static MYSQL_SYSVAR_INT(
    shrink_table_space, se_shrink_table_space,
    PLUGIN_VAR_RQCMDARG,
    "Shrink se table space",
    se_shrink_table_space_func, se_shrink_table_space_sub_func,
    -1, -1, INT_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(
    reset_pending_shrink,
    se_pending_shrink_subtable_id,
    PLUGIN_VAR_RQCMDARG,
    "reset subtable's pending shrink",
    se_reset_subtable_pending_shrink, se_reset_subtable_pending_shrink_stub,
    0, 0, INT_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    auto_shrink_enabled,
    se_db_options.auto_shrink_enabled,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::auto_shrink_enabled for SE",
    nullptr, se_set_auto_shrink_enabled,
    se_db_options.auto_shrink_enabled);

static MYSQL_SYSVAR_ULONG(
    max_free_extent_percent,
    se_db_options.max_free_extent_percent,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::max_free_extent_percent for SE",
    nullptr, se_set_max_free_extent_percent,
    se_db_options.max_free_extent_percent, 0, 100, 0);

static MYSQL_SYSVAR_ULONG(
    shrink_allocate_interval,
    se_db_options.shrink_allocate_interval,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::shrink_allocate_interval for SE",
    nullptr, se_set_shrink_allocate_interval,
    se_db_options.shrink_allocate_interval, 0, ULONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    max_shrink_extent_count,
    se_db_options.max_shrink_extent_count,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::max_shrink_extent_count for SE",
    nullptr, se_set_max_shrink_extent_count,
    se_db_options.max_shrink_extent_count, 0, ULONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    total_max_shrink_extent_count,
    se_db_options.total_max_shrink_extent_count,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::total_max_shrink_extent_count for SE",
    nullptr, se_set_total_max_shrink_extent_count,
    se_db_options.total_max_shrink_extent_count, 0, ULONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    auto_shrink_schedule_interval,
    se_db_options.auto_shrink_schedule_interval,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::auto_shrink_schedule_interval for SE",
    nullptr, se_set_auto_shrink_schedule_interval,
    se_db_options.auto_shrink_schedule_interval, 0, ULONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    strict_collation_check, se_strict_collation_check,
    PLUGIN_VAR_RQCMDARG,
    "Enforce case sensitive collation for SE indexes",
    nullptr, nullptr, TRUE);

static MYSQL_SYSVAR_STR(
    strict_collation_exceptions,
    se_strict_collation_exceptions,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
    "List of tables (using regex) that are excluded "
    "from the case sensitive collation enforcement",
    nullptr, se_set_collation_exception_list, "");

static const char *query_trace_sum_ops[] = {"OFF", "ON", "RESET",  NullS};
static TYPELIB query_trace_sum_ops_typelib = {array_elements(query_trace_sum_ops) - 1,
                                     "query_trace_sum_ops_typelib", query_trace_sum_ops,
                                     nullptr};
static MYSQL_SYSVAR_ENUM(
    query_trace_sum,
    se_query_trace_sum,
    PLUGIN_VAR_RQCMDARG,
    "If record query detail in IS table for SE",
    nullptr, se_set_query_trace_sum,
    0 , &query_trace_sum_ops_typelib);

static MYSQL_SYSVAR_BOOL(
    query_trace_print_slow,
    se_query_trace_print_slow,
    PLUGIN_VAR_RQCMDARG,
    "If print slow query detail in error log for SE",
    nullptr, se_set_query_trace_print_slow, 0 /* default OFF */);

static MYSQL_SYSVAR_DOUBLE(
    query_trace_threshold_time,
    se_query_trace_threshold_time,
    PLUGIN_VAR_RQCMDARG,
    "If a query use more than this, a trace log will be printed to error log",
    NULL, se_set_query_trace_threshold_time,
    100.0, 0.0, LONG_TIMEOUT, 0.0);

static MYSQL_THDVAR_ULONG(
    parallel_read_threads, PLUGIN_VAR_RQCMDARG,
    "Number of threads to do parallel read.", NULL, NULL,
    4, 1, 256, 0);

static MYSQL_SYSVAR_ULONG(
    scan_add_blocks_limit,
    se_default_cf_options.scan_add_blocks_limit,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::scan_add_blocks_limit for SE",
    nullptr, se_set_scan_add_blocks_limit,
    se_default_cf_options.scan_add_blocks_limit, 0L, ULONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(master_thread_monitor_interval_ms,
                          se_db_options.monitor_interval_ms,
                          PLUGIN_VAR_OPCMDARG,
                          "DBOptions::master_thread_monitor_interval_ms for SE",
                          nullptr, se_set_master_thread_monitor_interval_ms,
                          /* default */ 60'000,
                          /* min */ 0L, /* max */ ULLONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(master_thread_compaction_enabled,
                         se_db_options.master_thread_compaction_enabled,
                         PLUGIN_VAR_RQCMDARG,
                         "DBOptions::master_thread_compaction_enabled for SE",
                         nullptr, se_set_master_thread_compaction_enabled,
                         se_db_options.master_thread_compaction_enabled);

static MYSQL_SYSVAR_STR(
    persistent_cache_dir,
    se_persistent_cache_dir,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::persistent_cache_diror SE",
    nullptr, nullptr, se_db_options.persistent_cache_dir.c_str());

static MYSQL_SYSVAR_ULONG(
    persistent_cache_size,
    se_db_options.persistent_cache_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::persistent_cache_size for SE",
    nullptr, nullptr,
    se_db_options.persistent_cache_size, 0, ULONG_MAX, 0);

static MYSQL_SYSVAR_ENUM(
    persistent_cache_mode,
    se_persistent_cache_mode,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "With which mode to add data into persistent cache",
    nullptr, nullptr, kReadWriteThrough,
    &se_persistent_cache_mode_typelib);

static MYSQL_SYSVAR_BOOL(
    parallel_flush_log,
    se_db_options.parallel_flush_log,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::parallel_flush_logfor SE",
    nullptr, nullptr, false);

ulong se_thd_lock_wait_timeout(THD *thd)
{
  return THDVAR(thd, lock_wait_timeout);
}

bool se_thd_deadlock_detect(THD *thd)
{
  return THDVAR(thd, deadlock_detect);
}

ulong se_thd_max_row_locks(THD *thd)
{
  return THDVAR(thd, max_row_locks);
}

bool se_thd_lock_scanned_rows(THD *thd)
{
  return THDVAR(thd, lock_scanned_rows);
}

ulong se_thd_bulk_load_size(THD *thd)
{
  return THDVAR(thd, bulk_load_size);
}

bool se_thd_write_disable_wal(THD *thd)
{
  return THDVAR(thd, write_disable_wal);
}

bool se_thd_unsafe_for_binlog(THD *thd)
{
  return THDVAR(thd, unsafe_for_binlog);
}

ulong se_thd_parallel_read_threads(THD *thd)
{
  return THDVAR(thd, parallel_read_threads);
}

static SYS_VAR *se_system_vars_internal[] = {
    MYSQL_SYSVAR(data_dir),
    MYSQL_SYSVAR(wal_dir),
    MYSQL_SYSVAR(block_size),
    MYSQL_SYSVAR(compression_per_level),
    MYSQL_SYSVAR(compression_options),
    MYSQL_SYSVAR(hotbackup),
    MYSQL_SYSVAR(lock_wait_timeout),
    MYSQL_SYSVAR(deadlock_detect),
    MYSQL_SYSVAR(max_row_locks),
    MYSQL_SYSVAR(lock_scanned_rows),
    MYSQL_SYSVAR(flush_log_at_trx_commit),
    MYSQL_SYSVAR(enable_2pc),
    MYSQL_SYSVAR(batch_group_slot_array_size),
    MYSQL_SYSVAR(batch_group_max_group_size),
    MYSQL_SYSVAR(batch_group_max_leader_wait_time_us),
    MYSQL_SYSVAR(mutex_backtrace_threshold_ns),
    MYSQL_SYSVAR(row_cache_size),
    MYSQL_SYSVAR(block_cache_size),
    MYSQL_SYSVAR(table_cache_size),
    MYSQL_SYSVAR(table_cache_numshardbits),
    MYSQL_SYSVAR(memtable_size),
    //MYSQL_SYSVAR(db_write_buffer_size),
    MYSQL_SYSVAR(total_memtable_size),
    MYSQL_SYSVAR(max_write_buffer_number_to_maintain),
    MYSQL_SYSVAR(min_write_buffer_number_to_merge),
    MYSQL_SYSVAR(write_disable_wal),
    MYSQL_SYSVAR(unsafe_for_binlog),
    MYSQL_SYSVAR(total_wal_size),
    MYSQL_SYSVAR(wal_recovery_mode),
    MYSQL_SYSVAR(parallel_wal_recovery),
    MYSQL_SYSVAR(parallel_recovery_thread_num),
    MYSQL_SYSVAR(concurrent_writable_file_buffer_num),
    MYSQL_SYSVAR(concurrent_writable_file_single_buffer_size),
    MYSQL_SYSVAR(concurrent_writable_file_buffer_switch_limit),
    MYSQL_SYSVAR(bulk_load_size),
    MYSQL_SYSVAR(pause_background_work),
    MYSQL_SYSVAR(max_background_dumps),
    MYSQL_SYSVAR(dump_memtable_limit_size),
    MYSQL_SYSVAR(flush_memtable),
    MYSQL_SYSVAR(flush_delete_percent),
    MYSQL_SYSVAR(flush_delete_percent_trigger),
    MYSQL_SYSVAR(flush_delete_record_trigger),
    MYSQL_SYSVAR(flush_threads),
    //MYSQL_SYSVAR(base_background_compactions),
    MYSQL_SYSVAR(compaction_threads),
    MYSQL_SYSVAR(disable_auto_compactions),
    MYSQL_SYSVAR(compaction_delete_percent),
    MYSQL_SYSVAR(level_compaction_dynamic_level_bytes),
    MYSQL_SYSVAR(level0_file_num_compaction_trigger),
    MYSQL_SYSVAR(level0_layer_num_compaction_trigger),
    MYSQL_SYSVAR(level1_extents_major_compaction_trigger),
    MYSQL_SYSVAR(level2_usage_percent),
    MYSQL_SYSVAR(compaction_task_extents_limit),
    MYSQL_SYSVAR(compact),
    MYSQL_SYSVAR(bottommost_level),
    MYSQL_SYSVAR(disable_online_ddl),
    MYSQL_SYSVAR(disable_instant_ddl),
    MYSQL_SYSVAR(disable_parallel_ddl),
    MYSQL_SYSVAR(sort_buffer_size),
    MYSQL_SYSVAR(purge_invalid_subtable_bg),
    MYSQL_SYSVAR(estimate_cost_depth),
    MYSQL_SYSVAR(stats_dump_period_sec),
    MYSQL_SYSVAR(rate_limiter_bytes_per_sec),
    MYSQL_SYSVAR(idle_tasks_schedule_time),
    MYSQL_SYSVAR(shrink_table_space),
    MYSQL_SYSVAR(reset_pending_shrink),
    MYSQL_SYSVAR(auto_shrink_enabled),
    MYSQL_SYSVAR(max_free_extent_percent),
    MYSQL_SYSVAR(shrink_allocate_interval),
    MYSQL_SYSVAR(max_shrink_extent_count),
    MYSQL_SYSVAR(total_max_shrink_extent_count),
    MYSQL_SYSVAR(auto_shrink_schedule_interval),
    MYSQL_SYSVAR(strict_collation_check),
    MYSQL_SYSVAR(strict_collation_exceptions),
    MYSQL_SYSVAR(query_trace_sum),
    MYSQL_SYSVAR(query_trace_print_slow),
    MYSQL_SYSVAR(query_trace_threshold_time),
    MYSQL_SYSVAR(parallel_read_threads),
    MYSQL_SYSVAR(scan_add_blocks_limit),
    MYSQL_SYSVAR(master_thread_monitor_interval_ms),
    MYSQL_SYSVAR(master_thread_compaction_enabled),
    MYSQL_SYSVAR(persistent_cache_dir),
    MYSQL_SYSVAR(persistent_cache_size),
    MYSQL_SYSVAR(persistent_cache_mode),
    MYSQL_SYSVAR(parallel_flush_log),
    nullptr};

SYS_VAR **se_system_vars_export = se_system_vars_internal;

} // namespace smartengine
