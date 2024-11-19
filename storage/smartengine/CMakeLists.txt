# TODO: Copyrights

#IF (NOT EXISTS "${CMAKE_SOURCE_DIR}/storage/smartengine/smartengine/Makefile")
#  MESSAGE(SEND_ERROR "Missing Makefile in smartengine directory. Try \"git submodule update\".")
#ENDIF()

# suppress warning of new FixedQueue<T>, since using C++17
IF (NOT WITH_SMARTENGINE)
  MESSAGE (STATUS "Not building with smartengine, you must specify -DWITH_SMARTENGINE=1|ON to cmake in order to build with smartengine.")
  RETURN ()
ELSE ()
  MESSAGE (STATUS "Building with smartengine storage engine = " ${WITH_SMARTENGINE})
ENDIF()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wno-unused-parameter -Wno-unused-variable -Wno-vla -Wno-overloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-missing-format-attribute -Wno-format-security -Wno-non-virtual-dtor -Wno-cast-qual")
set(SE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/storage/smartengine/core")
CHECK_FUNCTION_EXISTS(sched_getcpu  HAVE_SCHED_GETCPU)
IF(HAVE_SCHED_GETCPU)
  ADD_DEFINITIONS(-DHAVE_SCHED_GETCPU=1)
ENDIF()

# generate build version for smartengine.
EXECUTE_PROCESS(
  COMMAND env -i ${CMAKE_SOURCE_DIR}/storage/smartengine/util/generate_build_version.sh
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/sql
  ${CMAKE_SOURCE_DIR}/sql-common
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/storage/innobase
  ${CMAKE_SOURCE_DIR}/storage/innobase/include
  ${CMAKE_SOURCE_DIR}/storage/smartengine
  ${CMAKE_SOURCE_DIR}/storage/smartengine/handler
  ${CMAKE_SOURCE_DIR}/storage/smartengine/util
  ${CMAKE_SOURCE_DIR}/storage/smartengine/api
  ${CMAKE_SOURCE_DIR}/storage/smartengine/dict
  ${CMAKE_SOURCE_DIR}/storage/smartengine/index
  ${CMAKE_SOURCE_DIR}/storage/smartengine/plugin
  ${CMAKE_SOURCE_DIR}/storage/smartengine/transaction
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/db
)

ADD_DEFINITIONS(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -DHAVE_ZLIB)

SET(SE_SOURCES
  handler/ha_smartengine.cc 
  handler/ha_smartengine.h 
  handler/handler_alter.cc
  handler/handler_alter.h
  handler/handler_select.cc
  handler/se_hton.cc
  handler/se_hton.h
  api/se_api.cc
  api/se_api.h
  dict/se_dd_operations.cc 
  dict/se_dd_operations.h
  dict/se_cf_options.cc 
  dict/se_cf_options.h
  dict/se_cf_manager.cc 
  dict/se_cf_manager.h
  dict/se_binlog_manager.cc
  dict/se_binlog_manager.h
  dict/se_charset_info.cc
  dict/se_charset_info.h
  dict/se_ddl_manager.cc
  dict/se_ddl_manager.h
  dict/se_dict_manager.cc
  dict/se_dict_manager.h
  dict/se_dict_struct.cc
  dict/se_dict_struct.h
  dict/se_dict_util.cc
  dict/se_dict_util.h
  dict/se_field_make_unpack_info.cc
  dict/se_field_make_unpack_info.h
  dict/se_field_pack.cc
  dict/se_field_pack.h
  dict/se_field_pack_func.cc
  dict/se_field_pack_func.h
  dict/se_field_unpack_func.cc
  dict/se_field_unpack_func.h
  dict/se_field_skip_func.cc
  dict/se_field_skip_func.h
  dict/se_index.cc
  dict/se_index.h
  dict/se_table.cc
  dict/se_table.h
  dict/se_log_ddl.cc 
  dict/se_log_ddl.h
  dict/se_sst_info.cc 
  dict/se_sst_info.h
  dict/se_index_stats.cc 
  dict/se_index_stats.h
  index/se_index_merge.cc 
  index/se_index_merge.h
  index/se_parallel_ddl_ctx.cc
  index/se_parallel_ddl_ctx.h
  plugin/se_i_s.cc 
  plugin/se_i_s.h
  plugin/se_plugin.cc
  plugin/se_status_vars.cc
  plugin/se_status_vars.h
  plugin/se_system_vars.cc
  plugin/se_system_vars.h
  transaction/se_async_commit_callback.h
  transaction/se_transaction.cc
  transaction/se_transaction.h
  transaction/se_transaction_factory.cc
  transaction/se_transaction_factory.h
  transaction/se_transaction_impl.cc
  transaction/se_transaction_impl.h
  transaction/se_transaction_list_walker.cc
  transaction/se_transaction_list_walker.h
  transaction/se_write_batch_impl.cc
  transaction/se_write_batch_impl.h
  util/se_logger.cc
  util/se_logger.h
  util/se_mutex_wrapper.cc 
  util/se_mutex_wrapper.h
  util/se_utils.cc 
  util/se_utils.h 
  util/se_buff.cc
  util/se_buff.h
  util/se_threads.cc 
  util/se_threads.h
  util/se_counter.h
  util/se_comparator.h
  util/se_atomic_stat.h
  core/backup/hotbackup_impl.cc
  core/cache/cache_entry.cc
  core/cache/clock_cache.cc
  core/cache/lru_cache.cc
  core/cache/persistent_cache.cc
  core/cache/row_cache.cc
  core/cache/sharded_cache.cc
  core/compact/reuse_block_merge_iterator.cc
  core/compact/range_iterator.cc
  core/compact/compaction_job.cc
  core/compact/compaction.cc
  core/compact/flush_iterator.cc
  core/compact/split_compaction.cc
  core/compact/mt_ext_compaction.cc
  core/compact/compaction_tasks_picker.cc
  core/compact/task_type.cc
  core/compact/compaction_iterator.cc
  core/compact/new_compaction_iterator.cc
  core/compact/compaction_stats.cc
  core/db/binlog_position.cc
  core/db/column_family.cc
  core/db/db_filesnapshot.cc
  core/db/db_impl.cc
  core/db/db_impl_write.cc
  core/db/db_impl_compaction_flush.cc
  core/db/db_impl_files.cc
  core/db/db_impl_open.cc
  core/db/db_impl_debug.cc
  core/db/db_iter.cc
  core/db/dbformat.cc
  core/db/replay_task.cc
  core/db/replay_thread_pool.cc
  core/db/replay_threadpool_executor.cc
  core/db/dump_job.cc
  core/db/flush_job.cc
  core/db/flush_scheduler.cc
  core/db/internal_stats.cc
  core/db/log_reader.cc
  core/db/log_writer.cc
  core/db/recovery_point.cc
  core/db/snapshot_impl.cc
  core/db/table_cache.cc
  core/db/version_set.cc
  core/db/batch_group.cc
  core/db/pipline_queue_manager.cc
  core/env/env.cc
  core/env/env_posix.cc
  core/env/io_posix.cc
  core/env/memenv.cc
  core/logger/logger.cc
  core/memory/chunk_allocator.cc
  core/memory/alloc_mgr.cc
  core/memory/mod_info.cc
  core/memtable/memtable.cc
  core/memtable/memtable_allocator.cc
  core/memtable/memtable_list.cc
  core/memtable/skiplistrep.cc
  core/memtable/art.cc
  core/memtable/art_node.cc
  core/memtable/artrep.cc
  core/monitoring/histogram.cc
  core/monitoring/histogram_windowing.cc
  core/monitoring/instrumented_mutex.cc
  core/monitoring/iostats_context.cc
  core/monitoring/perf_level.cc
  core/monitoring/query_perf_context.cc
  core/monitoring/statistics.cc
  core/monitoring/thread_status_impl.cc
  core/monitoring/thread_status_updater.cc
  core/monitoring/thread_status_updater_debug.cc
  core/monitoring/thread_status_util.cc
  core/monitoring/thread_status_util_debug.cc
  core/options/cf_options.cc
  core/options/db_options.cc
  core/options/options.cc
  core/options/options_helper.cc
  core/port/port_posix.cc
  core/schema/column_schema.cc
  core/schema/engine_attribute.cc
  core/schema/table_schema.cc
  core/storage/change_info.cc
  core/storage/data_file.cc
  core/storage/extent_meta_manager.cc
  core/storage/extent_space_file.cc
  core/storage/extent_space_manager.cc
  core/storage/extent_space_obj.cc
  core/storage/io_extent.cc
  core/storage/large_object_extent_manager.cc
  core/storage/shrink_job.cc
  core/storage/storage_common.cc
  core/storage/storage_logger.cc
  core/storage/storage_log_entry.cc
  core/storage/storage_manager.cc
  core/storage/storage_meta_struct.cc
  core/storage/table_space.cc
  core/storage/multi_version_extent_meta_layer.cc
  core/table/block_struct.cc
  core/table/bloom_filter.cc
  core/table/column_block_iterator.cc
  core/table/column_block_writer.cc
  core/table/column_struct.cc
  core/table/column_unit.cc
  core/table/extent_table_factory.cc
  core/table/extent_reader.cc
  core/table/extent_struct.cc
  core/table/extent_writer.cc
  core/table/get_context.cc
  core/table/index_block_reader.cc
  core/table/index_block_writer.cc
  core/table/iterator.cc
  core/table/large_object.cc
  core/table/merging_iterator.cc
  core/table/parallel_read.cc
  core/table/row_block.cc
  core/table/row_block_iterator.cc
  core/table/row_block_writer.cc
  core/table/sst_file_writer.cc
  core/table/table_properties.cc
  core/table/two_level_iterator.cc
  core/table/sstable_scan_iterator.cc
  core/util/aio_wrapper.cc
  core/util/arena.cc
  core/util/build_version.cc
  core/util/coding.cc
  core/util/comparator.cc
  core/util/compress/compressor.cc
  core/util/compress/compressor_factory.cc
  core/util/compress/compressor_helper.cc
  core/util/compress/lz4_compressor.cc
  core/util/compress/zlib_compressor.cc
  core/util/compress/zstd_compressor.cc
  core/util/concurrent_arena.cc
  core/util/crc32c.cc
  core/util/dio_helper.cc
  core/util/dynamic_bloom.cc
  core/util/ebr.cc
  core/util/file_reader_writer.cc
  core/util/concurrent_direct_file_writer.cc
  core/util/file_util.cc
  core/util/file_name.cc
  core/util/hash.cc
  core/util/murmurhash.cc
  core/util/random.cc
  core/util/rate_limiter.cc
  core/util/slice.cc
  core/util/stack_trace.cc
  core/util/status.cc
  core/util/status_message.cc
  core/util/string_util.cc
  core/util/sync_point.cc
  core/util/thread_local.cc
  core/util/threadpool_imp.cc
  core/util/transaction_test_util.cc
  core/util/xxhash.cc
  core/util/memory_stat.cc
  core/util/to_string.cc
  core/util/misc_utility.cc
  core/transactions/optimistic_transaction_db_impl.cc
  core/transactions/optimistic_transaction_impl.cc
  core/transactions/transaction_base.cc
  core/transactions/transaction_db_impl.cc
  core/transactions/transaction_db_mutex_impl.cc
  core/transactions/transaction_impl.cc
  core/transactions/transaction_lock_mgr.cc
  core/transactions/transaction_util.cc
  core/write_batch/write_batch.cc
  core/write_batch/write_batch_base.cc
  core/write_batch/write_batch_with_index.cc
  core/write_batch/write_batch_with_index_internal.cc
  core/objstore/objstore_layout.cc
  core/objstore/lease_lock.cc
  core/objstore/snapshot_release_lock.cc
)

IF(WITH_FB_TSAN)
  SET(PIC_EXT "_pic")
ELSE()
  SET(PIC_EXT "")
ENDIF()

#ADD_SUBDIRECTORY(${CMAKE_SOURCE_DIR}/storage/smartengine/core/IS ${CMAKE_BINARY_DIR}/IS.build)
SET(smartengine_static_libs )
IF (NOT "$ENV{WITH_SNAPPY}" STREQUAL "")
  SET(smartengine_static_libs ${smartengine_static_libs}
  $ENV{WITH_SNAPPY}/lib/libsnappy${PIC_EXT}.a)
  ADD_DEFINITIONS(-DHAVE_SNAPPY)
ENDIF()

IF ((WITH_LZ4 STREQUAL "bundled") OR (WITH_LZ4 STREQUAL "system"))
  SET(smartengine_static_libs ${smartengine_static_libs} ext::lz4)
  ADD_DEFINITIONS(-DHAVE_LZ4)
ELSE()
  MESSAGE(FATAL_ERROR "WITH_LZ4 must be bundled or system")
ENDIF()

IF ((WITH_ZSTD STREQUAL "bundled") OR (WITH_ZSTD STREQUAL "system"))
  SET(smartengine_static_libs ${smartengine_static_libs} ext::zstd)
  ADD_DEFINITIONS(-DHAVE_ZSTD)
ELSE()
  MESSAGE(FATAL_ERROR "WITH_ZSTD must be bundled or system")
ENDIF()

IF ((WITH_ZLIB STREQUAL "bundled") OR (WITH_ZLIB STREQUAL "system"))
  SET(smartengine_static_libs ${smartengine_static_libs} ext::zlib)  
  ADD_DEFINITIONS(-DHAVE_ZLIB)
ELSE()
  MESSAGE(FATAL_ERROR "WITH_ZLIB must be bundled or system")
ENDIF()

#SET(IS_LIB ${CMAKE_BINARY_DIR}/IS.build/libis_all.a)
#SET(smartengine_static_libs ${smartengine_static_libs} ${ZLIB_LIBRARY} "-lrt" "-laio" ${IS_LIB})
SET(smartengine_static_libs ${smartengine_static_libs} ZLIB::ZLIB "-lrt" "-laio")

SET(smartengine_static_libs ${smartengine_static_libs} "myobjstore")

if (WITH_TBB)
  include_directories(${CMAKE_SOURCE_DIR}/storage/smartengine/core/third-party/tbb/include)
  SET(smartengine_static_libs ${smartengine_static_libs} ${TBB_LIBRARIES})
endif()

MYSQL_ADD_PLUGIN(smartengine ${SE_SOURCES} STORAGE_ENGINE DEFAULT STATIC_ONLY
  LINK_LIBRARIES ${smartengine_static_libs} extra::rapidjson
  #  DEPENDENCIES libis_all tbb
)

# TODO: read this file list from src.mk:TOOL_SOURCES
SET(SE_TOOL_SOURCES
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/ldb_tool.cc
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/ldb_cmd.cc
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/sst_dump_tool.cc
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/backup_check_tool.cc 
)
MYSQL_ADD_EXECUTABLE(sst_dump ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/sst_dump.cc ${SE_TOOL_SOURCES})
TARGET_LINK_LIBRARIES(sst_dump smartengine)

MYSQL_ADD_EXECUTABLE(ldb ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/ldb.cc ${SE_TOOL_SOURCES})
TARGET_LINK_LIBRARIES(ldb smartengine)

MYSQL_ADD_EXECUTABLE(mysql_ldb ${CMAKE_SOURCE_DIR}/storage/smartengine/tools/mysql_ldb.cc ${SE_TOOL_SOURCES})
TARGET_LINK_LIBRARIES(mysql_ldb smartengine)

MYSQL_ADD_EXECUTABLE(backup_check ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/backup_check.cc ${SE_TOOL_SOURCES})
TARGET_LINK_LIBRARIES(backup_check smartengine)
