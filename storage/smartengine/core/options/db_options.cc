/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "options/db_options.h"
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include "logger/log_module.h"

namespace smartengine {
using namespace util;
namespace common {

ImmutableDBOptions::ImmutableDBOptions() : ImmutableDBOptions(Options()) {}

ImmutableDBOptions::ImmutableDBOptions(const DBOptions &options)
    : env(options.env),
      rate_limiter(options.rate_limiter),
      statistics(options.statistics),
      db_paths(options.db_paths),
      wal_dir(options.wal_dir),
      max_background_flushes(options.max_background_flushes),
      table_cache_numshardbits(options.table_cache_numshardbits),
      use_direct_reads(options.use_direct_reads),
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
      table_cache_size(options.table_cache_size),
      persistent_cache_dir(options.persistent_cache_dir),
      persistent_cache_size(options.persistent_cache_size),
      persistent_cache_mode(options.persistent_cache_mode),
      parallel_flush_log(options.parallel_flush_log)
{}

void ImmutableDBOptions::Dump() const {
  __SE_LOG(INFO, "                                     Options.env: %p", env);
  __SE_LOG(INFO, "                            Options.rate_limiter: %p", rate_limiter.get());
  __SE_LOG(INFO, "                              Options.statistics: %p", statistics.get());
  __SE_LOG(INFO, "                                 Options.wal_dir: %s", wal_dir.c_str());

  for (uint32_t i = 0; i < db_paths.size(); ++i) {
    __SE_LOG(INFO, "                              Options.db_path[%d]: %s", i, db_paths[i].path.c_str());
  }

  __SE_LOG(INFO, "                  Options.max_background_flushes: %d", max_background_flushes);
  __SE_LOG(INFO, "                Options.table_cache_numshardbits: %d", table_cache_numshardbits);
  __SE_LOG(INFO, "                        Options.use_direct_reads: %d", use_direct_reads);
  __SE_LOG(INFO, "                    Options.db_write_buffer_size: %ld", db_write_buffer_size);
  __SE_LOG(INFO, "              Options.db_total_write_buffer_size: %ld", db_total_write_buffer_size);
  __SE_LOG(INFO, "                    Options.write_buffer_manager: %p", write_buffer_manager.get());
  __SE_LOG(INFO, "           Options.writable_file_max_buffer_size: %ld", writable_file_max_buffer_size);
  __SE_LOG(INFO, "                          Options.bytes_per_sync: %ld", bytes_per_sync);
  __SE_LOG(INFO,"                       Options.wal_bytes_per_sync: %ld", wal_bytes_per_sync);
  __SE_LOG(INFO, "                  Options.enable_thread_tracking: %d", enable_thread_tracking);
  __SE_LOG(INFO, "         Options.allow_concurrent_memtable_write: %d", allow_concurrent_memtable_write);
  __SE_LOG(INFO, "                       Options.wal_recovery_mode: %d", wal_recovery_mode);
  __SE_LOG(INFO, "                   Options.enable_aio_wal_reader: %d", enable_aio_wal_reader);
  __SE_LOG(INFO, "                   Options.parallel_wal_recovery: %d", parallel_wal_recovery);
  __SE_LOG(INFO, "            Options.parallel_recovery_thread_num: %d", parallel_recovery_thread_num);
  __SE_LOG(INFO, "                               Options.allow_2pc: %d", parallel_recovery_thread_num);
  __SE_LOG(INFO, "                               Options.row_cache: %p", row_cache.get());
  __SE_LOG(INFO, "             Options.avoid_flush_during_recovery: %d", avoid_flush_during_recovery);
  __SE_LOG(INFO, "                        Options.table_cache_size: %d", table_cache_size);
  __SE_LOG(INFO, "                    Options.persistent_cache_dir: %s", persistent_cache_dir.c_str());
  __SE_LOG(INFO, "                   Options.persistent_cache_size: %d", persistent_cache_size);
  __SE_LOG(INFO, "                   Options.persistent_cache_mode: %s", persistent_cache_mode_names[persistent_cache_mode]);
  __SE_LOG(INFO, " .                    Options.parallel_flush_log: %d", parallel_flush_log);
}

MutableDBOptions::MutableDBOptions()
    : base_background_compactions(1),
      max_background_compactions(1),
      avoid_flush_during_shutdown(false),
      max_total_wal_size(128 * 1024 * 1024),
      delete_obsolete_files_period_micros(6ULL * 60 * 60 * 1000000),
      stats_dump_period_sec(600),
      batch_group_slot_array_size(5),
      batch_group_max_group_size(80),
      batch_group_max_leader_wait_time_us(200),
      concurrent_writable_file_buffer_num(4),
      concurrent_writable_file_single_buffer_size(4 * 1024U * 1024U),
      concurrent_writable_file_buffer_switch_limit(512 * 1024U),
      use_direct_write_for_wal(true),
      mutex_backtrace_threshold_ns(100000),
      max_background_dumps(3),
      dump_memtable_limit_size(0),
      auto_shrink_enabled(true),
      max_free_extent_percent(10),
      shrink_allocate_interval(60 * 60),
      max_shrink_extent_count(512),
      total_max_shrink_extent_count(15 * 512),
      idle_tasks_schedule_time(60),
      auto_shrink_schedule_interval(60 * 60),
      estimate_cost_depth(0),
      monitor_interval_ms(60'000),
      master_thread_compaction_enabled(true)
{}

MutableDBOptions::MutableDBOptions(const DBOptions& options)
    : base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions),
      avoid_flush_during_shutdown(options.avoid_flush_during_shutdown),
      max_total_wal_size(options.max_total_wal_size),
      delete_obsolete_files_period_micros(options.delete_obsolete_files_period_micros),
      stats_dump_period_sec(options.stats_dump_period_sec),
      batch_group_slot_array_size(options.batch_group_slot_array_size),
      batch_group_max_group_size(options.batch_group_max_group_size),
      batch_group_max_leader_wait_time_us(options.batch_group_max_leader_wait_time_us),
      concurrent_writable_file_buffer_num(options.concurrent_writable_file_buffer_num),
      concurrent_writable_file_single_buffer_size(options.concurrent_writable_file_single_buffer_size),
      concurrent_writable_file_buffer_switch_limit(options.concurrent_writable_file_buffer_switch_limit),
      use_direct_write_for_wal(options.use_direct_write_for_wal),
      mutex_backtrace_threshold_ns(options.mutex_backtrace_threshold_ns),
      max_background_dumps(options.max_background_dumps),
      dump_memtable_limit_size(options.dump_memtable_limit_size),
      auto_shrink_enabled(options.auto_shrink_enabled),
      max_free_extent_percent(options.max_free_extent_percent),
      shrink_allocate_interval(options.shrink_allocate_interval),
      max_shrink_extent_count(options.max_shrink_extent_count),
      total_max_shrink_extent_count(options.total_max_shrink_extent_count),
      idle_tasks_schedule_time(options.idle_tasks_schedule_time),
      auto_shrink_schedule_interval(options.auto_shrink_schedule_interval),
      estimate_cost_depth(options.estimate_cost_depth),
      monitor_interval_ms(options.monitor_interval_ms),
      master_thread_compaction_enabled(options.master_thread_compaction_enabled)
{}

void MutableDBOptions::Dump() const {
  __SE_LOG(INFO, "            Options.base_background_compactions: %d", base_background_compactions);
  __SE_LOG(INFO, "             Options.max_background_compactions: %d", max_background_compactions);
  __SE_LOG(INFO, "            Options.avoid_flush_during_shutdown: %d", avoid_flush_during_shutdown);
  __SE_LOG(INFO, "                     Options.max_total_wal_size: %ld", max_total_wal_size);
  __SE_LOG(INFO, "    Options.delete_obsolete_files_period_micros: %ld", delete_obsolete_files_period_micros);
  __SE_LOG(INFO, "                  Options.stats_dump_period_sec: %u", stats_dump_period_sec);
  __SE_LOG(INFO, "            Options.batch_group_slot_array_size: %u", batch_group_slot_array_size);
  __SE_LOG(INFO, "             Options.batch_group_max_group_size: %u", batch_group_max_group_size);
  __SE_LOG(INFO, "    Options.batch_group_max_leader_wait_time_us: %u", batch_group_max_leader_wait_time_us);
  __SE_LOG(INFO, "    Options.concurrent_writable_file_buffer_num: %u", concurrent_writable_file_buffer_num);
  __SE_LOG(INFO, "Options.concurrent_writable_file_single_buffer_size: %u", concurrent_writable_file_single_buffer_size);
  __SE_LOG(INFO, "Options.concurrent_writable_file_buffer_switch_limit: %u", concurrent_writable_file_buffer_switch_limit);
  __SE_LOG(INFO, "               Options.use_direct_write_for_wal: %d", use_direct_write_for_wal);
  __SE_LOG(INFO, "           Options.mutex_backtrace_threshold_ns: %d", mutex_backtrace_threshold_ns);
  __SE_LOG(INFO, "                   Options.max_background_dumps: %d", max_background_dumps);
  __SE_LOG(INFO, "               Options.dump_memtable_limit_size: %d", dump_memtable_limit_size);
  __SE_LOG(INFO, "                    Options.auto_shrink_enabled: %d", auto_shrink_enabled);
  __SE_LOG(INFO, "                Options.max_free_extent_percent: %d", max_free_extent_percent);
  __SE_LOG(INFO, "               Options.shrink_allocate_interval: %d", shrink_allocate_interval);
  __SE_LOG(INFO, "                Options.max_shrink_extent_count: %d", max_shrink_extent_count);
  __SE_LOG(INFO, "          Options.total_max_shrink_extent_count: %d", total_max_shrink_extent_count);
  __SE_LOG(INFO, "               Options.idle_tasks_schedule_time: %d", idle_tasks_schedule_time);
  __SE_LOG(INFO, "          Options.auto_shrink_schedule_interval: %d", auto_shrink_schedule_interval);
  __SE_LOG(INFO, "                    Options.estimate_cost_depth: %d", estimate_cost_depth);
  __SE_LOG(INFO, "                    Options.monitor_interval_ms: %d", monitor_interval_ms);
  __SE_LOG(INFO, "       Options.master_thread_compaction_enabled: %d", master_thread_compaction_enabled);
}

}  // namespace common
}  // namespace smartengine
