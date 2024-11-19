/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "options/options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "logger/log_module.h"
#include "options/db_options.h"
#include "table/extent_table_factory.h"
#include "util/compression.h"

namespace smartengine
{
using namespace util;
using namespace table;
using namespace cache;

namespace common
{

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions() {
  assert(memtable_factory.get() != nullptr);
}

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions(const Options& options)
    : min_write_buffer_number_to_merge(
          options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          options.max_write_buffer_number_to_maintain),
      compression_per_level(options.compression_per_level),
      background_disable_merge(options.background_disable_merge),
      level_compaction_dynamic_level_bytes(
          options.level_compaction_dynamic_level_bytes),
      memtable_factory(options.memtable_factory)
{
  assert(memtable_factory.get() != nullptr);
}

ColumnFamilyOptions::ColumnFamilyOptions()
{
#ifndef NDEBUG
      table_factory = std::shared_ptr<TableFactory>(new table::ExtentBasedTableFactory());
#endif
}

ColumnFamilyOptions::ColumnFamilyOptions(const Options& options)
    : AdvancedColumnFamilyOptions(options),
      comparator(options.comparator),
      write_buffer_size(options.write_buffer_size),
      flush_delete_percent(options.flush_delete_percent),
      compaction_delete_percent(options.compaction_delete_percent),
      flush_delete_percent_trigger(options.flush_delete_percent_trigger),
      flush_delete_record_trigger(options.flush_delete_record_trigger),
      compression_opts(options.compression_opts),
      level0_file_num_compaction_trigger(
          options.level0_file_num_compaction_trigger),
      level0_layer_num_compaction_trigger(
          options.level0_layer_num_compaction_trigger),
      level1_extents_major_compaction_trigger(
          options.level1_extents_major_compaction_trigger),
      level2_usage_percent(options.level2_usage_percent),
      disable_auto_compactions(options.disable_auto_compactions),
      table_factory(options.table_factory),
      scan_add_blocks_limit(options.scan_add_blocks_limit),
      bottommost_level(options.bottommost_level),
      compaction_task_extents_limit(options.compaction_task_extents_limit)
{}

DBOptions::DBOptions() {}

DBOptions::DBOptions(const Options &options)
    : env(options.env),
      rate_limiter(options.rate_limiter),
      max_total_wal_size(options.max_total_wal_size),
      statistics(options.statistics),
      monitor_interval_ms(options.monitor_interval_ms),
      db_paths(options.db_paths),
      wal_dir(options.wal_dir),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions),
      max_background_flushes(options.max_background_flushes),
      max_background_dumps(options.max_background_dumps),
      dump_memtable_limit_size(options.dump_memtable_limit_size),
      table_cache_numshardbits(options.table_cache_numshardbits),
      use_direct_reads(options.use_direct_reads),
      stats_dump_period_sec(options.stats_dump_period_sec),
      db_write_buffer_size(options.db_write_buffer_size),
      db_total_write_buffer_size(options.db_total_write_buffer_size),
      write_buffer_manager(options.write_buffer_manager),
      writable_file_max_buffer_size(options.writable_file_max_buffer_size),
      bytes_per_sync(options.bytes_per_sync),
      wal_bytes_per_sync(options.wal_bytes_per_sync),
      enable_thread_tracking(options.enable_thread_tracking),
      allow_concurrent_memtable_write(options.allow_concurrent_memtable_write),
      wal_recovery_mode(options.wal_recovery_mode),
      enable_aio_wal_reader(options.enable_aio_wal_reader),
      parallel_wal_recovery(options.parallel_wal_recovery),
      parallel_recovery_thread_num(options.parallel_recovery_thread_num),
      allow_2pc(options.allow_2pc),
      row_cache(options.row_cache),
      avoid_flush_during_recovery(options.avoid_flush_during_recovery),
      avoid_flush_during_shutdown(options.avoid_flush_during_shutdown),
      batch_group_slot_array_size(options.batch_group_slot_array_size),
      batch_group_max_group_size(options.batch_group_max_group_size),
      batch_group_max_leader_wait_time_us(
          options.batch_group_max_leader_wait_time_us),
      concurrent_writable_file_buffer_num(
          options.concurrent_writable_file_buffer_num),
      concurrent_writable_file_single_buffer_size(
          options.concurrent_writable_file_single_buffer_size),
      concurrent_writable_file_buffer_switch_limit(
          options.concurrent_writable_file_buffer_switch_limit),
      use_direct_write_for_wal(options.use_direct_write_for_wal),
      mutex_backtrace_threshold_ns(options.mutex_backtrace_threshold_ns),
      auto_shrink_enabled(options.auto_shrink_enabled),
      max_free_extent_percent(options.max_free_extent_percent),
      shrink_allocate_interval(options.shrink_allocate_interval),
      max_shrink_extent_count(options.max_shrink_extent_count),
      total_max_shrink_extent_count(options.total_max_shrink_extent_count),
      idle_tasks_schedule_time(options.idle_tasks_schedule_time),
      table_cache_size(options.table_cache_size),
      auto_shrink_schedule_interval(options.auto_shrink_schedule_interval),
      estimate_cost_depth(options.estimate_cost_depth),
      master_thread_compaction_enabled(options.master_thread_compaction_enabled),
      persistent_cache_dir(options.persistent_cache_dir),
      persistent_cache_size(options.persistent_cache_size),
      persistent_cache_mode(options.persistent_cache_mode),
      parallel_flush_log(options.parallel_flush_log)
{}

const char *persistent_cache_mode_names[kMaxPersistentCacheMode + 1] = {
    "read_write_through",
    "read_through",
    nullptr
};

void DBOptions::Dump() const {
  ImmutableDBOptions(*this).Dump();
  MutableDBOptions(*this).Dump();
}  // DBOptions::Dump

void ColumnFamilyOptions::Dump() const {
  __SE_LOG(INFO, "                       Options.comparator: %s",
                   comparator->Name());
  __SE_LOG(INFO, "                 Options.memtable_factory: %s",
                   memtable_factory->Name());
  __SE_LOG(INFO, "                    Options.table_factory: %s",
                   table_factory->Name());
  __SE_LOG(INFO, "                    table_factory options: %s",
                   table_factory->GetPrintableTableOptions().c_str());
  __SE_LOG(INFO,
                "                Options.write_buffer_size: %ld", write_buffer_size);
  __SE_LOG(INFO, "             Options.flush_delete_percent: %d",
                   flush_delete_percent);
  __SE_LOG(INFO, "        Options.compaction_delete_percent: %d",
                   compaction_delete_percent);
  __SE_LOG(INFO, "     Options.flush_delete_percent_trigger: %d",
                   flush_delete_percent_trigger);
  __SE_LOG(INFO, "      Options.flush_delete_record_trigger: %d",
                   flush_delete_record_trigger);
  if (!compression_per_level.empty()) {
    for (unsigned int i = 0; i < compression_per_level.size(); i++) {
      __SE_LOG(
          INFO, "                   Options.compression[%d]: %s", i,
          CompressionTypeToString(compression_per_level[i]).c_str());
    }
  }
  __SE_LOG(INFO, " Options.min_write_buffer_number_to_merge: %d",
                   min_write_buffer_number_to_merge);
  __SE_LOG(INFO, "Options.max_write_buffer_number_to_maintain: %d",
                   max_write_buffer_number_to_maintain);
  __SE_LOG(INFO, "     Options.compression_opts.window_bits: %d",
                   compression_opts.window_bits);
  __SE_LOG(INFO, "           Options.compression_opts.level: %d",
                   compression_opts.level);
  __SE_LOG(INFO, "        Options.compression_opts.strategy: %d",
                   compression_opts.strategy);
  __SE_LOG(INFO,
                "  Options.compression_opts.max_dict_bytes: %ld", compression_opts.max_dict_bytes);
  __SE_LOG(INFO, "Options.level0_file_num_compaction_trigger: %d",
                   level0_file_num_compaction_trigger);
  __SE_LOG(INFO, "Options.level1_extents_major_compaction_trigger: %d",
                   level1_extents_major_compaction_trigger);
  __SE_LOG(INFO, "             Options.level2_usage_percent: %ld",
                   level2_usage_percent);
  __SE_LOG(INFO, "Options.level0_layer_num_compaction_trigger: %d",
                   level0_layer_num_compaction_trigger);
  __SE_LOG(INFO, "Options.level_compaction_dynamic_level_bytes: %d",
                   level_compaction_dynamic_level_bytes);
  __SE_LOG(INFO, "         Options.disable_auto_compactions: %d",
                   disable_auto_compactions);
}

void Options::Dump() const
{
  DBOptions::Dump();
  ColumnFamilyOptions::Dump();
}

ReadOptions::ReadOptions()
    : verify_checksums(true),
      fill_cache(true),
      snapshot(nullptr),
      iterate_upper_bound(nullptr),
      read_tier(kReadAllTier),
      total_order_seek(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      max_skippable_internal_keys(0),
      skip_del_(true),
      read_level_(kAll){}

ReadOptions::ReadOptions(bool cksum, bool cache)
    : verify_checksums(cksum),
      fill_cache(cache),
      snapshot(nullptr),
      iterate_upper_bound(nullptr),
      read_tier(kReadAllTier),
      total_order_seek(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      max_skippable_internal_keys(0),
      skip_del_(true),
      read_level_(kAll){}

}  // namespace common
}  // namespace smartengine
