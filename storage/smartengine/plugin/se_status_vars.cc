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

#include "se_status_vars.h"
#include "m_string.h"
#include "mysql/status_var.h"
#include "core/logger/log_module.h"
#include "core/monitoring/query_perf_context.h"
#include "core/util/status.h"

namespace smartengine
{
SeGlobalStats global_stats;

SeGlobalStats::SeGlobalStats()
    : rows_(),
      system_rows_(),
      snapshot_conflict_errors_(0),
      wal_group_syncs_(0)
{
}

SeStatusVars::SeStatusVars()
{
  reset();
}

void SeStatusVars::reset()
{
  block_cache_miss_ = 0;
  block_cache_hit_ = 0;
  block_cache_add_ = 0;
  block_cache_index_miss_ = 0;
  block_cache_index_hit_ = 0;
  block_cache_filter_miss_ = 0;
  block_cache_filter_hit_ = 0;
  block_cache_data_miss_ = 0;
  block_cache_data_hit_ = 0;
  row_cache_add_ = 0;
  row_cache_hit_ = 0;
  row_cache_miss_ = 0;
  memtable_hit_ = 0;
  memtable_miss_ = 0;
  number_keys_written_ = 0;
  number_keys_read_ = 0;
  number_keys_updated_ = 0;
  bytes_written_ = 0;
  bytes_read_ = 0;
  block_cachecompressed_miss_ = 0;
  block_cachecompressed_hit_ = 0;
  wal_synced_ = 0;
  wal_bytes_ = 0;
  write_self_ = 0;
  write_other_ = 0;
  write_wal_ = 0;
  number_superversion_acquires_ = 0;
  number_superversion_releases_ = 0;
  number_superversion_cleanups_ = 0;
  number_block_not_compressed_ = 0;
  snapshot_conflict_errors_ = 0;
  wal_group_syncs_ = 0;
  rows_deleted_ = 0;
  rows_inserted_ = 0;
  rows_updated_ = 0;
  rows_read_ = 0;
  system_rows_deleted_ = 0;
  system_rows_inserted_ = 0;
  system_rows_updated_ = 0;
  system_rows_read_ = 0;
  max_level0_layers_ = 0;
  max_imm_numbers_ = 0;
  max_level0_fragmentation_rate_ = 0;
  max_level1_fragmentation_rate_ = 0;
  max_level2_fragmentation_rate_ = 0;
  max_level0_delete_percent_ = 0;
  max_level1_delete_percent_ = 0;
  max_level2_delete_percent_ = 0;
  all_flush_megabytes_ = 0;
  all_compaction_megabytes_ = 0;
  top1_subtable_size_ = 0;
  top2_subtable_size_ = 0;
  top3_subtable_size_ = 0;
  top1_mod_mem_info_ = 0;
  top2_mod_mem_info_ = 0;
  top3_mod_mem_info_ = 0;
  global_external_fragmentation_rate_ = 0;
  write_transaction_count_ = 0;
  pipeline_group_count_ = 0;
  pipeline_copy_log_size_ = 0;
  pipeline_copy_log_count_ = 0;
  pipeline_flush_log_size_ = 0;
  pipeline_flush_log_count_ = 0;
}

DEFINE_TO_STRING(SeStatusVars, KV_(block_cache_miss), KV_(block_cache_hit), KV_(block_cache_add),
    KV_(block_cache_index_miss), KV_(block_cache_index_hit), KV_(block_cache_filter_miss),
    KV_(block_cache_filter_hit), KV_(row_cache_add), KV_(row_cache_hit), KV_(row_cache_miss),
    KV_(memtable_hit), KV_(memtable_miss), KV_(number_keys_written), KV_(number_keys_read),
    KV_(number_keys_updated), KV_(bytes_written), KV_(bytes_read), KV_(block_cachecompressed_miss),
    KV_(block_cachecompressed_hit), KV_(wal_synced), KV_(wal_bytes), KV_(write_self), KV_(write_other),
    KV_(write_wal), KV_(number_superversion_acquires), KV_(number_superversion_releases),
    KV_(number_superversion_cleanups), KV_(number_block_not_compressed), KV_(snapshot_conflict_errors),
    KV_(wal_group_syncs), KV_(rows_deleted), KV_(rows_inserted), KV_(rows_updated), KV_(rows_read),
    KV_(system_rows_deleted), KV_(system_rows_inserted), KV_(system_rows_updated), KV_(system_rows_read),
    KV_(max_level0_layers), KV_(max_imm_numbers),
    KV_(max_level0_fragmentation_rate), KV_(max_level1_fragmentation_rate), KV_(max_level2_fragmentation_rate),
    KV_(max_level0_delete_percent), KV_(max_level1_delete_percent), KV_(max_level2_delete_percent),
    KV_(all_flush_megabytes), KV_(all_compaction_megabytes),
    KV_(top1_subtable_size), KV_(top2_subtable_size), KV_(top3_subtable_size),
    KV_(top1_mod_mem_info), KV_(top2_mod_mem_info), KV_(top3_mod_mem_info),
    KV_(global_external_fragmentation_rate), KV_(pipeline_group_count), KV_(pipeline_copy_log_size),
    KV_(pipeline_copy_log_count), KV_(pipeline_flush_log_size), KV_(pipeline_flush_log_count))

static int se_update_query_perf_vars(SeStatusVars &status_vars)
{
  int ret = common::Status::kOk;
  monitor::QueryPerfContext *perf_ctx = nullptr;

  if (IS_NULL(perf_ctx = monitor::get_tls_query_perf_context())) {
    ret = common::Status::kErrorUnexpected;
    HANDLER_LOG(WARN, "The perf context must not been nullptr", K(ret), KP(perf_ctx));
  } else {
#define SE_ASSIGN_PERF_COUNTER(name, key) \
    status_vars.name##_ = perf_ctx->get_global_count(monitor::CountPoint::key)

    SE_ASSIGN_PERF_COUNTER(block_cache_miss, BLOCK_CACHE_MISS);
    SE_ASSIGN_PERF_COUNTER(block_cache_hit, BLOCK_CACHE_HIT);
    SE_ASSIGN_PERF_COUNTER(block_cache_add, BLOCK_CACHE_ADD);
    SE_ASSIGN_PERF_COUNTER(block_cache_index_miss, BLOCK_CACHE_INDEX_MISS);
    SE_ASSIGN_PERF_COUNTER(block_cache_index_hit, BLOCK_CACHE_INDEX_HIT);
    SE_ASSIGN_PERF_COUNTER(block_cache_filter_miss, BLOCK_CACHE_FILTER_MISS);
    SE_ASSIGN_PERF_COUNTER(block_cache_filter_hit, BLOCK_CACHE_FILTER_HIT);
    SE_ASSIGN_PERF_COUNTER(block_cache_data_miss, BLOCK_CACHE_DATA_MISS);
    SE_ASSIGN_PERF_COUNTER(block_cache_data_hit, BLOCK_CACHE_DATA_HIT);
    SE_ASSIGN_PERF_COUNTER(row_cache_add, ROW_CACHE_ADD);
    SE_ASSIGN_PERF_COUNTER(row_cache_hit, ROW_CACHE_HIT);
    SE_ASSIGN_PERF_COUNTER(row_cache_miss, ROW_CACHE_MISS);
    SE_ASSIGN_PERF_COUNTER(memtable_hit, MEMTABLE_HIT);
    SE_ASSIGN_PERF_COUNTER(memtable_miss, MEMTABLE_MISS);
    SE_ASSIGN_PERF_COUNTER(number_keys_written, NUMBER_KEYS_WRITTEN);
    SE_ASSIGN_PERF_COUNTER(number_keys_read, NUMBER_KEYS_READ);
    SE_ASSIGN_PERF_COUNTER(number_keys_updated, NUMBER_KEYS_UPDATE);
    SE_ASSIGN_PERF_COUNTER(bytes_written, BLOCK_CACHE_BYTES_WRITE);
    SE_ASSIGN_PERF_COUNTER(bytes_read, ENGINE_DISK_READ);
    SE_ASSIGN_PERF_COUNTER(block_cachecompressed_miss, BLOCK_CACHE_COMPRESSED_MISS);
    SE_ASSIGN_PERF_COUNTER(block_cachecompressed_hit, BLOCK_CACHE_COMPRESSED_HIT);
    SE_ASSIGN_PERF_COUNTER(wal_synced, WAL_FILE_SYNCED);
    SE_ASSIGN_PERF_COUNTER(wal_bytes, WAL_FILE_BYTES);
    SE_ASSIGN_PERF_COUNTER(write_self, WRITE_DONE_BY_SELF);
    SE_ASSIGN_PERF_COUNTER(write_other, WRITE_DONE_BY_OTHER);
    SE_ASSIGN_PERF_COUNTER(write_wal, WRITE_WITH_WAL);
    SE_ASSIGN_PERF_COUNTER(number_superversion_acquires, NUMBER_SUPERVERSION_ACQUIRES);
    SE_ASSIGN_PERF_COUNTER(number_superversion_releases, NUMBER_SUPERVERSION_RELEASES);
    SE_ASSIGN_PERF_COUNTER(number_superversion_cleanups, NUMBER_SUPERVERSION_CLEANUPS);
    SE_ASSIGN_PERF_COUNTER(number_block_not_compressed, NUMBER_BLOCK_NOT_COMPRESSED);
    SE_ASSIGN_PERF_COUNTER(write_transaction_count, WRITE_TRANSACTION_COUNT);
    SE_ASSIGN_PERF_COUNTER(pipeline_group_count, PIPELINE_GROUP_COUNT);
    SE_ASSIGN_PERF_COUNTER(pipeline_group_wait_timeout_count, PIPELINE_GROUP_WAIT_TIMEOUT_COUNT);
    SE_ASSIGN_PERF_COUNTER(pipeline_copy_log_size, PIPELINE_COPY_LOG_SIZE);
    SE_ASSIGN_PERF_COUNTER(pipeline_copy_log_count, PIPELINE_COPY_LOG_COUNT);
    SE_ASSIGN_PERF_COUNTER(pipeline_flush_log_size, PIPELINE_FLUSH_LOG_SIZE);
    SE_ASSIGN_PERF_COUNTER(pipeline_flush_log_count, PIPELINE_FLUSH_LOG_COUNT);
    SE_ASSIGN_PERF_COUNTER(pipeline_flush_log_sync_count, PIPELINE_FLUSH_LOG_SYNC_COUNT);
    SE_ASSIGN_PERF_COUNTER(pipeline_flush_log_not_sync_count, PIPELINE_FLUSH_LOG_NOT_SYNC_COUNT);
  }

  return ret;
}

int se_update_global_statistic_vars(SeStatusVars &status_vars)
{
  int ret = common::Status::kOk;

  status_vars.rows_deleted_ = global_stats.rows_[ROWS_DELETED];
  status_vars.rows_inserted_ = global_stats.rows_[ROWS_INSERTED];
  status_vars.rows_updated_ = global_stats.rows_[ROWS_UPDATED];
  status_vars.rows_read_ = global_stats.rows_[ROWS_READ];
  status_vars.system_rows_deleted_ = global_stats.system_rows_[ROWS_DELETED];
  status_vars.system_rows_inserted_ = global_stats.system_rows_[ROWS_INSERTED];
  status_vars.system_rows_updated_ = global_stats.system_rows_[ROWS_UPDATED];
  status_vars.system_rows_read_ =  global_stats.system_rows_[ROWS_READ];
  status_vars.snapshot_conflict_errors_ = global_stats.snapshot_conflict_errors_;
  status_vars.wal_group_syncs_ = global_stats.wal_group_syncs_;
  status_vars.max_level0_layers_ = global_stats.max_level0_layers_;
  status_vars.max_imm_numbers_ = global_stats.max_imm_numbers_;
  status_vars.max_level0_fragmentation_rate_ =
      global_stats.max_level0_fragmentation_rate_;
  status_vars.max_level1_fragmentation_rate_ =
      global_stats.max_level1_fragmentation_rate_;
  status_vars.max_level2_fragmentation_rate_ =
      global_stats.max_level2_fragmentation_rate_;
  status_vars.max_level0_delete_percent_ =
      global_stats.max_level0_delete_percent_;
  status_vars.max_level1_delete_percent_ =
      global_stats.max_level1_delete_percent_;
  status_vars.max_level2_delete_percent_ =
      global_stats.max_level2_delete_percent_;
  status_vars.all_flush_megabytes_ = global_stats.all_flush_megabytes_;
  status_vars.all_compaction_megabytes_ =
      global_stats.all_compaction_megabytes_;
  status_vars.top1_subtable_size_ = global_stats.top1_subtable_size_;
  status_vars.top2_subtable_size_ = global_stats.top2_subtable_size_;
  status_vars.top3_subtable_size_ = global_stats.top3_subtable_size_;
  status_vars.top1_mod_mem_info_ = global_stats.top1_mod_mem_info_;
  status_vars.top2_mod_mem_info_ = global_stats.top2_mod_mem_info_;
  status_vars.top3_mod_mem_info_ = global_stats.top3_mod_mem_info_;
  status_vars.global_external_fragmentation_rate_ =
      global_stats.global_external_fragmentation_rate_;

  return ret;
}

int se_update_status_vars(SeStatusVars &status_vars)
{
  int ret = common::Status::kOk;

  if (FAILED(se_update_query_perf_vars(status_vars))) {
    HANDLER_LOG(WARN, "fail to update query perf variables", K(ret));
  } else if (FAILED(se_update_global_statistic_vars(status_vars))) {
    HANDLER_LOG(WARN, "fail to update global statistis variables", K(ret));
  } else {
    //succeed
  }

  return ret;
}

static SeStatusVars internal_status_vars;

#define SE_GLOBAL_LONG_STATUS_VAR(name) \
  {#name, (char *)(&internal_status_vars.name##_), SHOW_LONG, SHOW_SCOPE_GLOBAL}

static SHOW_VAR se_status_vars[] = {
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_miss),
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_hit),
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_add),
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_index_miss),
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_index_hit),
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_filter_miss),
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_filter_hit),
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_data_miss),
  SE_GLOBAL_LONG_STATUS_VAR(block_cache_data_hit),
  SE_GLOBAL_LONG_STATUS_VAR(row_cache_add),
 SE_GLOBAL_LONG_STATUS_VAR(row_cache_hit),
 SE_GLOBAL_LONG_STATUS_VAR(row_cache_miss),
 SE_GLOBAL_LONG_STATUS_VAR(memtable_hit),
 SE_GLOBAL_LONG_STATUS_VAR(memtable_miss),
 SE_GLOBAL_LONG_STATUS_VAR(number_keys_written),
 SE_GLOBAL_LONG_STATUS_VAR(number_keys_read),
 SE_GLOBAL_LONG_STATUS_VAR(number_keys_updated),
 SE_GLOBAL_LONG_STATUS_VAR(bytes_written),
 SE_GLOBAL_LONG_STATUS_VAR(bytes_read),
 SE_GLOBAL_LONG_STATUS_VAR(block_cachecompressed_miss),
 SE_GLOBAL_LONG_STATUS_VAR(block_cachecompressed_hit),
 SE_GLOBAL_LONG_STATUS_VAR(wal_synced),
 SE_GLOBAL_LONG_STATUS_VAR(wal_bytes),
 SE_GLOBAL_LONG_STATUS_VAR(write_self),
 SE_GLOBAL_LONG_STATUS_VAR(write_other),
 SE_GLOBAL_LONG_STATUS_VAR(write_wal),
 SE_GLOBAL_LONG_STATUS_VAR(number_superversion_acquires),
 SE_GLOBAL_LONG_STATUS_VAR(number_superversion_releases),
 SE_GLOBAL_LONG_STATUS_VAR(number_superversion_cleanups),
 SE_GLOBAL_LONG_STATUS_VAR(number_block_not_compressed),
 SE_GLOBAL_LONG_STATUS_VAR(snapshot_conflict_errors),
 SE_GLOBAL_LONG_STATUS_VAR(wal_group_syncs),
 SE_GLOBAL_LONG_STATUS_VAR(rows_deleted),
 SE_GLOBAL_LONG_STATUS_VAR(rows_inserted),
 SE_GLOBAL_LONG_STATUS_VAR(rows_updated),
 SE_GLOBAL_LONG_STATUS_VAR(rows_read),
 SE_GLOBAL_LONG_STATUS_VAR(system_rows_deleted),
 SE_GLOBAL_LONG_STATUS_VAR(system_rows_inserted),
 SE_GLOBAL_LONG_STATUS_VAR(system_rows_updated),
 SE_GLOBAL_LONG_STATUS_VAR(system_rows_read),
 SE_GLOBAL_LONG_STATUS_VAR(max_level0_layers),
 SE_GLOBAL_LONG_STATUS_VAR(max_imm_numbers),
 SE_GLOBAL_LONG_STATUS_VAR(max_level0_fragmentation_rate),
 SE_GLOBAL_LONG_STATUS_VAR(max_level1_fragmentation_rate),
 SE_GLOBAL_LONG_STATUS_VAR(max_level2_fragmentation_rate),
 SE_GLOBAL_LONG_STATUS_VAR(max_level0_delete_percent),
 SE_GLOBAL_LONG_STATUS_VAR(max_level1_delete_percent),
 SE_GLOBAL_LONG_STATUS_VAR(max_level2_delete_percent),
 SE_GLOBAL_LONG_STATUS_VAR(all_flush_megabytes),
 SE_GLOBAL_LONG_STATUS_VAR(all_compaction_megabytes),
 SE_GLOBAL_LONG_STATUS_VAR(top1_subtable_size),
 SE_GLOBAL_LONG_STATUS_VAR(top2_subtable_size),
 SE_GLOBAL_LONG_STATUS_VAR(top3_subtable_size),
 SE_GLOBAL_LONG_STATUS_VAR(top1_mod_mem_info),
 SE_GLOBAL_LONG_STATUS_VAR(top2_mod_mem_info),
 SE_GLOBAL_LONG_STATUS_VAR(top3_mod_mem_info),
 SE_GLOBAL_LONG_STATUS_VAR(global_external_fragmentation_rate),
 SE_GLOBAL_LONG_STATUS_VAR(write_transaction_count),
 SE_GLOBAL_LONG_STATUS_VAR(pipeline_group_count),
 SE_GLOBAL_LONG_STATUS_VAR(pipeline_group_wait_timeout_count),
 SE_GLOBAL_LONG_STATUS_VAR(pipeline_copy_log_size),
 SE_GLOBAL_LONG_STATUS_VAR(pipeline_copy_log_count),
 SE_GLOBAL_LONG_STATUS_VAR(pipeline_flush_log_size),
 SE_GLOBAL_LONG_STATUS_VAR(pipeline_flush_log_count),
 SE_GLOBAL_LONG_STATUS_VAR(pipeline_flush_log_sync_count),
 SE_GLOBAL_LONG_STATUS_VAR(pipeline_flush_log_not_sync_count),
  {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}
};

static int se_show_status_vars(THD *, SHOW_VAR *var, char *)
{
  se_update_status_vars(internal_status_vars);
  var->type = SHOW_ARRAY;
  var->value = (char *)(&se_status_vars);
  var->scope = SHOW_SCOPE_GLOBAL;
  return (0);
}

#ifdef WITH_XENGINE_COMPATIBLE_MODE
#define SE_STATUS_VAR_PREFIX "Xengine"
#else
#define SE_STATUS_VAR_PREFIX "Smartengine"
#endif

static SHOW_VAR se_status_vars_internal[] = {
  {SE_STATUS_VAR_PREFIX, (char *)se_show_status_vars, SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}
};

SHOW_VAR *se_show_vars_export = se_status_vars_internal;

} // namespace smartengine
