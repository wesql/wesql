# Prerequisites for Windows:
#     This cmake build is for Windows 64-bit only.
#
# Prerequisites:
#     You must have at least Visual Studio 2015 Update 3. Start the Developer Command Prompt window that is a part of Visual Studio installation.
#     Run the build commands from within the Developer Command Prompt window to have paths to the compiler and runtime libraries set.
#     You must have git.exe in your %PATH% environment variable.
#
# To build Rocksdb for Windows is as easy as 1-2-3-4-5:
#
# 1. Update paths to third-party libraries in thirdparty.inc file
# 2. Create a new directory for build artifacts
#        mkdir build
#        cd build
# 3. Run cmake to generate project files for Windows, add more options to enable required third-party libraries.
#    See thirdparty.inc for more information.
#        sample command: cmake -G "Visual Studio 14 Win64" -DGFLAGS=1 -DSNAPPY=1 -DJEMALLOC=1 -DJNI=1 ..
# 4. Then build the project in debug mode (you may want to add /m[:<N>] flag to run msbuild in <N> parallel threads
#                                          or simply /m ot use all avail cores)
#        msbuild rocksdb.sln
#
#        rocksdb.sln build features exclusions of test only code in Release. If you build ALL_BUILD then everything
#        will be attempted but test only code does not build in Release mode.
#
# 5. And release mode (/m[:<N>] is also supported)
#        msbuild rocksdb.sln /p:Configuration=Release
#
# Linux:
#
# 1. Install a recent toolchain such as devtoolset-3 if you're on a older distro. C++11 required.
# 2. mkdir build; cd build
# 3. cmake ..
# 4. make -j

cmake_minimum_required(VERSION 2.6)
project(smartengine)

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")

OPTION(WITH_QUERY_TRACE "WITH_QUERY_TRACE" ON)
if (WITH_QUERY_TRACE)
  add_definitions(-DWITH_QUERY_TRACE)
endif()

OPTION(WITH_STRESS_CHECK "WITH_STRESS_CHECK" OFF)
if (WITH_STRESS_CHECK)
  add_definitions(-DWITH_STRESS_CHECK)
endif()

if(MSVC)
  include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty.inc)
else()
  option(WITH_JEMALLOC "build with JeMalloc" OFF)
  if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    # FreeBSD has jemaloc as default malloc
    # but it does not have all the jemalloc files in include/...
    set(WITH_JEMALLOC ON)
  else()
    if(WITH_JEMALLOC)
      find_package(JeMalloc REQUIRED)
      #add_definitions(-DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE)
      include_directories(${JEMALLOC_INCLUDE_DIR})
    endif()
  endif()
  option(WITH_SNAPPY "build with SNAPPY" OFF)
  if(WITH_SNAPPY)
    find_package(snappy REQUIRED)
    add_definitions(-DSNAPPY)
    include_directories(${SNAPPY_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${SNAPPY_LIBRARIES})
  endif()
endif()

if(WIN32)
  execute_process(COMMAND powershell -Command "Get-Date -format MM_dd_yyyy" OUTPUT_VARIABLE DATE)
  execute_process(COMMAND powershell -Command "Get-Date -format HH:mm:ss" OUTPUT_VARIABLE TIME)
  string(REGEX REPLACE "(..)_(..)_..(..).*" "\\1/\\2/\\3" DATE "${DATE}")
  string(REGEX REPLACE "(..):(.....).*" " \\1:\\2" TIME "${TIME}")
  set(GIT_DATE_TIME "${DATE} ${TIME}")
else()
  execute_process(COMMAND date "+%Y/%m/%d %H:%M:%S" OUTPUT_VARIABLE DATETIME)
  string(REGEX REPLACE "\n" "" DATETIME ${DATETIME})
  set(GIT_DATE_TIME "${DATETIME}")
endif()

find_package(Git)

if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  if(WIN32)
    execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} -C ${CMAKE_CURRENT_SOURCE_DIR} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} --git-dir=${CMAKE_CURRENT_SOURCE_DIR}/.git rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  endif()
else()
  set(GIT_SHA 0)
endif()

string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA "${GIT_SHA}")

if(NOT WIN32)
  execute_process(COMMAND
      "./build_tools/version.sh" "full"
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_VARIABLE ROCKSDB_VERSION
  )
  string(STRIP "${ROCKSDB_VERSION}" ROCKSDB_VERSION)
  execute_process(COMMAND
      "./build_tools/version.sh" "major"
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_VARIABLE ROCKSDB_VERSION_MAJOR
  )
  string(STRIP "${ROCKSDB_VERSION_MAJOR}" ROCKSDB_VERSION_MAJOR)
endif()

if(WIN32)
  option(WITH_AVX2 "build with AVX2" ON)
  if(WITH_AVX2)
    if(MSVC)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2")
    else()
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
    endif()
  endif()
else()
  option(WITH_SSE42 "build with SSE4.2" ON)
  if(WITH_SSE42)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
  endif()
endif()

set(BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/build_version.cc)
configure_file(util/build_version.cc.in ${BUILD_VERSION_CC} @ONLY)
add_library(build_version OBJECT ${BUILD_VERSION_CC})
target_include_directories(build_version PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/util)

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /EHsc /GS /Gd /GR /GF /fp:precise /Zc:wchar_t /Zc:forScope /errorReport:queue")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /W3 /wd4127 /wd4800 /wd4996 /wd4351")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -W -Wextra -Wall")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers")
  if(MINGW)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -D_GLIBCXX_USE_CXX11_ABI=0")
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -fno-omit-frame-pointer -DNDEBUG")
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-momit-leaf-frame-pointer" HAVE_OMIT_LEAF_FRAME_POINTER)
    if(HAVE_OMIT_LEAF_FRAME_POINTER)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -momit-leaf-frame-pointer")
    endif()
  endif()
endif()

option(FAIL_ON_WARNINGS "Treat compile warnings as errors" ON)
if(FAIL_ON_WARNINGS)
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
  else() # assume GCC
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
  endif()
endif()

option(WITH_ASAN "build with ASAN" OFF)
if(WITH_ASAN)
  add_definitions(-DROCKSDB_TSAN_RUN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -ldl -pthread -static-libasan")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -pthread -D_GLIBCXX_USE_CXX11_ABI=0")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -pthread")
  if(WITH_JEMALLOC)
    message(FATAL "ASAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_TSAN "build with TSAN" OFF)
if(WITH_TSAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread -pthread -pie")
  add_definitions(-DROCKSDB_TSAN_RUN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -pthread -fPIC")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -pthread -fPIC")
  if(WITH_JEMALLOC)
    message(FATAL "TSAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_UBSAN "build with UBSAN" OFF)
if(WITH_UBSAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined")
  if(WITH_JEMALLOC)
    message(FATAL "UBSAN does not work well with JeMalloc")
  endif()
endif()

# Used to run CI build and tests so we can run faster
set(OPTIMIZE_DEBUG_DEFAULT 0)        # Debug build is unoptimized by default use -DOPTDBG=1 to optimize

if(DEFINED OPTDBG)
   set(OPTIMIZE_DEBUG ${OPTDBG})
else()
   set(OPTIMIZE_DEBUG ${OPTIMIZE_DEBUG_DEFAULT})
endif()

if(MSVC)
  if((${OPTIMIZE_DEBUG} EQUAL 1))
    message(STATUS "Debug optimization is enabled")
    set(CMAKE_CXX_FLAGS_DEBUG "/Oxt /MDd")
  else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /Gm /MDd")
  endif()
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Oxt /Zp8 /Gm- /Gy /MD")

  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp -D_GLIBCXX_USE_CXX11_ABI=0")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Cygwin")
  add_definitions(-fno-builtin-memcmp -DCYGWIN)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES arm)
    add_definitions(-DIOS_CROSS_COMPILE)
    # no debug info for IOS, that will make our library big
    add_definitions(-DNDEBUG)
  endif()
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  add_definitions(-DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
  add_definitions(-DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
  add_definitions(-DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
  add_definitions(-DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
  add_definitions(-DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
  add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)
  if(MINGW)
    add_definitions(-D_WIN32_WINNT=_WIN32_WINNT_VISTA)
  endif()
endif()

if(NOT WIN32)
  add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)
endif()

option(WITH_FALLOCATE "build with fallocate" ON)

if(WITH_FALLOCATE)
  include(CheckCSourceCompiles)
  CHECK_C_SOURCE_COMPILES("
#include <fcntl.h>
#include <linux/falloc.h>
int main() {
 int fd = open(\"/dev/null\", 0);
 fallocate(fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, 0, 1024);
}
" HAVE_FALLOCATE)
  if(HAVE_FALLOCATE)
    add_definitions(-DROCKSDB_FALLOCATE_PRESENT)
  endif()
endif()

#include(CheckFunctionExists)
#CHECK_FUNCTION_EXISTS(malloc_usable_size HAVE_MALLOC_USABLE_SIZE)
option(HAVE_MALLOC_USABLE_SIZE "HAVE_MALLOC_USABLE_SIZE" OFF)
if(HAVE_MALLOC_USABLE_SIZE)
  add_definitions(-DROCKSDB_MALLOC_USABLE_SIZE)
endif()

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/unittest/)
include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third-party/gtest-1.8.1/fused-src)
find_package(Threads REQUIRED)

add_subdirectory(third-party/gtest-1.8.1/fused-src/gtest)

#support gflags using system's
find_package(gflags REQUIRED)
# Note: maybe need google or gflags according to system's version
add_definitions(-DGFLAGS=gflags)
#add_definitions(-DGFLAGS=google)

# import ADD_CONVENIENCE_LIBRARY from MySQL cmake util module
INCLUDE(${CMAKE_SOURCE_DIR}/../../../cmake/libutils.cmake)
# import objstore related macroes

IF(NOT WITH_SMARTENGINE)
  # import lz4 related macroes
  INCLUDE(cmake/lz4.cmake)
  MYSQL_CHECK_LZ4()
  IF (WITH_LZ4 STREQUAL "bundled")
    SET(LZ4_HOME ${CMAKE_SOURCE_DIR}/../../../extra/lz4/lz4-1.9.4/lib)
    INCLUDE_DIRECTORIES(BEFORE SYSTEM ${LZ4_HOME})
    BUILD_MYSQL_BUNDLED_LZ4(${LZ4_HOME} ${CMAKE_BINARY_DIR}/third-party/lz4)
    SET(LZ4_LIBRARY ${CMAKE_BINARY_DIR}/archive_output_directory/liblz4.a)
    ADD_DEFINITIONS(-DHAVE_LZ4)
    list(APPEND THIRDPARTY_LIBS ${LZ4_LIBRARY})
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_LZ4 must be bundled now")
  ENDIF() 

  # import zlib related macroes
  INCLUDE(cmake/zlib.cmake)
  MYSQL_CHECK_ZLIB_WITH_COMPRESS()
  IF(BUILD_BUNDLED_ZLIB)
    SET(ZLIB_HOME ${CMAKE_SOURCE_DIR}/../../../extra/zlib/zlib-1.2.13/)
    INCLUDE_DIRECTORIES(${ZLIB_HOME})
    BUILD_MYSQL_BUNDLED_ZLIB(${ZLIB_HOME} ${CMAKE_BINARY_DIR}/third-party/zlib)
    SET(ZLIB_LIBRARY ${CMAKE_BINARY_DIR}/archive_output_directory/libz.a)
    ADD_DEFINITIONS(-DHAVE_ZLIB) 
    list(APPEND THIRDPARTY_LIBS ${ZLIB_LIBRARY}) 
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_ZLIB must be bundled now")
  ENDIF()

  # import zstd related macroes
  INCLUDE(cmake/zstd.cmake)
  ENABLE_LANGUAGE(ASM)
  MYSQL_CHECK_ZSTD()
  IF (BUILD_BUNDLED_ZSTD)
    SET(ZSTD_HOME ${CMAKE_SOURCE_DIR}/../../../extra/zstd/zstd-1.5.5) 
    INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../../../include ${ZSTD_HOME}/lib)
    BUILD_MYSQL_BUNDLED_ZSTD(${ZSTD_HOME} ${CMAKE_BINARY_DIR}/third-party/zstd)
    SET(ZSTD_LIBRARY ${CMAKE_BINARY_DIR}/archive_output_directory/libzstd.a) 
    ADD_DEFINITIONS(-DHAVE_ZSTD)
    list(APPEND THIRDPARTY_LIBS ${ZSTD_LIBRARY})
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_ZSTD must be bundled now")
  ENDIF()

  INCLUDE(cmake/aws_sdk_cpp.cmake)
  INCLUDE(cmake/aliyun_oss_sdk.cmake)
  MYSQL_CHECK_OBJSTORE_S3()
  MYSQL_CHECK_OBJSTORE_ALIYUN_OSS()
  MYSQL_BUILD_OBJSTORE()
  list(APPEND THIRDPARTY_LIBS myobjstore)

ENDIF()

#for zstd compile to find my_compiler.h
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../../../include)

IF(WITH_TBB)
  MESSAGE("Building TBB...${CMAKE_CXX_FLAGS}")
  SET (TBB_LIB
    ${CMAKE_BINARY_DIR}/third-party/tbb/build/lib/libtbb.so
    ${CMAKE_BINARY_DIR}/third-party/tbb/build/lib/libtbb.so.2
    #    ${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb/build/lib/libtbbmalloc.so
    #    ${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb/build/lib/libtbbmalloc.so.2
    #    ${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb/build/lib/libtbbmalloc_proxy.so.2
  )
  EXECUTE_PROCESS(COMMAND tar -xzf ${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb.tar.gz
                   WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/third-party/)
  EXECUTE_PROCESS(COMMAND make clean 
                  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/third-party/tbb)
  EXECUTE_PROCESS(COMMAND make CFLAGS=-fPIC CXXFLAGS=-fPIC -j
                  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/third-party/tbb)
  include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb/include)
  list(APPEND THIRDPARTY_LIBS ${TBB_LIB})
ENDIF()
# Main library source code

set(SOURCES
        backup/hotbackup_impl.cc
        cache/cache_entry.cc
        cache/clock_cache.cc
        cache/lru_cache.cc
        cache/persistent_cache.cc
        cache/sharded_cache.cc
        cache/row_cache.cc
        db/binlog_position.cc
        db/column_family.cc
        db/db_filesnapshot.cc
        db/db_impl.cc
        db/db_impl_write.cc
        db/db_impl_compaction_flush.cc
        db/db_impl_files.cc
        db/db_impl_open.cc
        db/db_impl_debug.cc
        db/db_iter.cc
        db/dbformat.cc
        db/dump_job.cc
        db/flush_job.cc
        db/flush_scheduler.cc
        db/internal_stats.cc
        db/log_reader.cc
        db/log_writer.cc
        db/recovery_point.cc
        db/snapshot_impl.cc
        db/table_cache.cc
        db/version_set.cc
        db/batch_group.cc
        db/pipline_queue_manager.cc
        db/replay_task.cc
        db/replay_thread_pool.cc
        db/replay_threadpool_executor.cc
        env/env.cc
        env/memenv.cc
        logger/logger.cc
        memory/chunk_allocator.cc
        memory/mod_info.cc
        memory/alloc_mgr.cc
        memtable/memtable_allocator.cc
        memtable/skiplistrep.cc
        memtable/art.cc
        memtable/art_node.cc
        memtable/artrep.cc
        memtable/memtable.cc
        memtable/memtable_list.cc
        monitoring/histogram.cc
        monitoring/histogram_windowing.cc
        monitoring/instrumented_mutex.cc
        monitoring/iostats_context.cc
        monitoring/perf_level.cc
        monitoring/query_perf_context.cc
        monitoring/statistics.cc
        monitoring/thread_status_impl.cc
        monitoring/thread_status_updater.cc
        monitoring/thread_status_util.cc
        monitoring/thread_status_util_debug.cc
        options/cf_options.cc
        options/db_options.cc
        options/options.cc
        options/options_helper.cc
        schema/column_schema.cc
        schema/engine_attribute.cc
        schema/table_schema.cc
        storage/change_info.cc
        storage/data_file.cc
        storage/extent_meta_manager.cc
        storage/extent_space_file.cc
        storage/extent_space_manager.cc
        storage/extent_space_obj.cc
        storage/io_extent.cc
        storage/large_object_extent_manager.cc
        storage/multi_version_extent_meta_layer.cc
        storage/shrink_job.cc
        storage/storage_common.cc
        storage/storage_log_entry.cc
        storage/storage_manager.cc
        storage/storage_meta_struct.cc
        storage/storage_logger.cc
        storage/table_space.cc
        table/block_struct.cc
        table/bloom_filter.cc
        table/column_block_iterator.cc
        table/column_block_writer.cc
        table/column_struct.cc
        table/column_unit.cc
        table/extent_reader.cc
        table/extent_struct.cc
        table/extent_table_factory.cc
        table/extent_writer.cc
        table/get_context.cc
        table/index_block_reader.cc
        table/index_block_writer.cc
        table/iterator.cc
        table/large_object.cc
        table/merging_iterator.cc
        table/parallel_read.cc
        table/row_block.cc
        table/row_block_iterator.cc
        table/row_block_writer.cc
        table/sst_file_writer.cc
        table/table_properties.cc
        table/two_level_iterator.cc
        table/sstable_scan_iterator.cc
        tools/backup_check_tool.cc
        tools/db_bench_tool.cc
        tools/ldb_cmd.cc
        tools/ldb_tool.cc
        tools/sst_dump_tool.cc
        util/aio_wrapper.cc
        util/arena.cc
        util/coding.cc
        util/comparator.cc
        util/compress/compressor.cc
        util/compress/compressor_factory.cc
        util/compress/compressor_helper.cc
        util/compress/lz4_compressor.cc
        util/compress/zlib_compressor.cc
        util/compress/zstd_compressor.cc
        util/concurrent_arena.cc
        util/crc32c.cc
        util/dio_helper.cc
        util/dynamic_bloom.cc
        util/ebr.cc
        util/file_reader_writer.cc
        util/concurrent_direct_file_writer.cc
        util/file_util.cc
        util/file_name.cc
        util/hash.cc
        util/murmurhash.cc
        util/random.cc
        util/rate_limiter.cc
        util/slice.cc
        util/stack_trace.cc
        util/status.cc
        util/status_message.cc
        util/string_util.cc
        util/sync_point.cc
        util/thread_local.cc
        util/threadpool_imp.cc
        util/transaction_test_util.cc
        util/xxhash.cc
        util/to_string.cc
        util/misc_utility.cc
        util/memory_stat.cc
        objstore/objstore_layout.cc
        objstore/lease_lock.cc
        objstore/snapshot_release_lock.cc
        compact/compaction.cc
        compact/compaction_job.cc
        compact/compaction_iterator.cc
        compact/compaction_stats.cc
        compact/compaction_tasks_picker.cc
        compact/flush_iterator.cc
        compact/mt_ext_compaction.cc
        compact/new_compaction_iterator.cc
        compact/reuse_block_merge_iterator.cc
        compact/range_iterator.cc
        compact/split_compaction.cc
        compact/task_type.cc
        transactions/optimistic_transaction_db_impl.cc
        transactions/optimistic_transaction_impl.cc
        transactions/transaction_base.cc
        transactions/transaction_db_impl.cc
        transactions/transaction_db_mutex_impl.cc
        transactions/transaction_impl.cc
        transactions/transaction_lock_mgr.cc
        transactions/transaction_util.cc
        write_batch/write_batch.cc
        write_batch/write_batch_base.cc
        write_batch/write_batch_with_index.cc
        write_batch/write_batch_with_index_internal.cc
    )

if(WIN32)
  list(APPEND SOURCES
    port/win/io_win.cc
    port/win/env_win.cc
    port/win/env_default.cc
    port/win/port_win.cc
    port/win/win_thread.cc
    port/win/xpress_win.cc)
else()
  list(APPEND SOURCES
    port/port_posix.cc
    env/env_posix.cc
    env/io_posix.cc)
endif()

# add common target to build objects from files for shared/static library target
add_library(smartengine-objs OBJECT ${SOURCES})
set_property(TARGET smartengine-objs PROPERTY POSITION_INDEPENDENT_CODE 1)
set(LIB_OBJS $<TARGET_OBJECTS:smartengine-objs> $<TARGET_OBJECTS:build_version>)

if(WIN32)
  set(SYSTEM_LIBS ${SYSTEM_LIBS} Shlwapi.lib Rpcrt4.lib)
  set(SMARTENGINE_STATIC_LIB smartenginelib${ARTIFACT_SUFFIX})
  set(SMARTENGINE_IMPORT_LIB smartengine${ARTIFACT_SUFFIX})
  set(LIBS ${SMARTENGINE_STATIC_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
else()
  set(SYSTEM_LIBS ${CMAKE_THREAD_LIBS_INIT} rt aio)
  set(SMARTENGINE_STATIC_LIB smartengine${ARTIFACT_SUFFIX})
  set(SMARTENGINE_SHARED_LIB smartengine-shared)
  set(SMARTENGINE_IMPORT_LIB ${SMARTENGINE_SHARED_LIB})
  #set(LIBS ${SMARTENGINE_SHARED_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set(LIBS ${SMARTENGINE_STATIC_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

  add_library(${SMARTENGINE_SHARED_LIB} SHARED ${LIB_OBJS})
  target_link_libraries(${SMARTENGINE_SHARED_LIB}
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set_target_properties(${SMARTENGINE_SHARED_LIB} PROPERTIES
                        LINKER_LANGUAGE CXX
                        VERSION ${ROCKSDB_VERSION}
                        SOVERSION ${ROCKSDB_VERSION_MAJOR}
                        CXX_STANDARD 11
                        OUTPUT_NAME "smartengine")
endif()

add_library(${SMARTENGINE_STATIC_LIB} STATIC ${LIB_OBJS})
target_link_libraries(${SMARTENGINE_STATIC_LIB}
  ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

if(WIN32)
  add_library(${SMARTENGINE_IMPORT_LIB} SHARED ${LIB_OBJS})
  target_link_libraries(${SMARTENGINE_IMPORT_LIB}
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set_target_properties(${SMARTENGINE_IMPORT_LIB} PROPERTIES
    COMPILE_DEFINITIONS "ROCKSDB_DLL;ROCKSDB_LIBRARY_EXPORTS")
  if(MSVC)
    set_target_properties(${SMARTENGINE_STATIC_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${SMARTENGINE_STATIC_LIB}.pdb")
    set_target_properties(${SMARTENGINE_IMPORT_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${SMARTENGINE_IMPORT_LIB}.pdb")
  endif()
endif()

option(WITH_JNI "build with JNI" OFF)
if(WITH_JNI OR JNI)
  message(STATUS "JNI library is enabled")
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/java)
else()
  message(STATUS "JNI library is disabled")
endif()

set(TESTS
        unittest/backup/hotbackup_test.cc
        unittest/cache/cache_test.cc
        unittest/cache/persistent_cache_test.cc
        unittest/cache/row_cache_test.cc
        #cache/lru_cache_test.cc
        unittest/env/env_basic_test.cc
        #unittest/env/env_test.cc
        unittest/env/mock_env_test.cc
        unittest/memory/alloc_mgr_test.cc
        unittest/memory/base_malloc_test.cc
        unittest/memory/chunk_allocator_test.cc
        unittest/memory/page_arena_test.cc
        unittest/monitoring/histogram_test.cc
        unittest/monitoring/iostats_context_test.cc
        #unittest/monitoring/statistics_test.cc
        #unittest/options/options_settable_test.cc
        unittest/storage/data_file_test.cc
        unittest/storage/extent_space_test.cc
        #unittest/storage/extent_space_manager_test.cc
        unittest/storage/io_extent_test.cc
        unittest/storage/storage_common_test.cc
        #storage/storage_manager_test.cc
        unittest/storage/storage_logger_test.cc
        unittest/storage/table_space_test.cc
        #unittest/table/block_test.cc
        unittest/table/bloom_filter_test.cc
        unittest/table/merger_test.cc
        #unittest/table/extent_table_test.cc
        unittest/table/extent_writer_test.cc
        unittest/table/in_mem_extent_table_test.cc
        unittest/table/large_object_test.cc
        unittest/table/sstable_scan_iterator_test.cc
        unittest/tools/ldb_cmd_test.cc
        #unittest/tools/sst_dump_test.cc
        #unittest/tools/backup_check_test.cc
        unittest/util/arena_test.cc
        unittest/util/autovector_test.cc
        unittest/util/aio_test.cc
        unittest/util/coding_test.cc
        unittest/util/compress/compressor_test.cc
        unittest/util/crc32c_test.cc
        unittest/util/defer_test.cc
        unittest/util/dio_helper_test.cc
        unittest/util/dynamic_bloom_test.cc
        unittest/util/increment_number_allocator_test.cc
        unittest/util/file_reader_writer_test.cc
        unittest/util/filelock_test.cc
        unittest/util/file_name_test.cc
        unittest/util/heap_test.cc
        unittest/util/rate_limiter_test.cc
        unittest/util/ring_buffer_test.cc
        unittest/util/slice_test.cc
        unittest/util/thread_list_test.cc
        unittest/util/thread_local_test.cc
        unittest/util/concurrent_direct_file_writer_test.cc
        unittest/util/serialization_test.cc
        unittest/util/fast_latch_test.cc
        unittest/util/time_interval_test.cc
        unittest/util/concurrent_hash_map_test.cc
        #unittest/compact/reuse_block_merge_iterator_test.cc
        unittest/compact/compaction_test.cc
        #unittest/compact/minor_compaction_test.cc
        #unittest/compact/minor_compaction_util_test.cc
        unittest/db/bugfix_39705890_allocate_extent_failed_test.cc
        unittest/db/bugfix_25168148_test.cc
        unittest/db/db_block_cache_test.cc
        unittest/db/db_recovery_failed_test.cc
        unittest/db/db_recovery_test.cc
        unittest/db/db_transaction_pipline_test.cc
        unittest/db/db_iter_test.cc
        unittest/db/dbformat_test.cc
        unittest/db/log_test.cc
        unittest/db/perf_context_test.cc
        unittest/db/recover_no_space_test.cc
        unittest/db/shrink_job_test.cc
        #db/write_callback_test.cc
        unittest/db/parallel_recovery_test.cc
        unittest/db/2pc_parallel_recovery_test.cc
        unittest/db/replay_thread_pool_test.cc
        unittest/logger/logger_test.cc
        unittest/memtable/art_test.cc
        unittest/memtable/inlineskiplist_test.cc
        unittest/memtable/skiplist_test.cc
        unittest/schema/column_schema_test.cc
        unittest/schema/engine_attribute_test.cc
        unittest/schema/table_schema_test.cc
        unittest/table/block_struct_test.cc
        unittest/table/parallel_read_test.cc
        #unittest/transactions/optimistic_transaction_test.cc
        #unittest/transactions/transaction_test.cc
        unittest/write_batch/write_batch_test.cc
        #unittest/write_batch_with_index_test.cc
        unittest/objstore/objstore_test.cc
        unittest/objstore/objstore_bench.cc
)

set(BENCHMARKS
  cache/cache_bench.cc
  memtable/memtablerep_bench.cc
  tools/db_bench.cc)
add_library(testharness OBJECT unittest/util/testharness.cc)
add_library(testutil OBJECT unittest/util/testutil.cc)
foreach(sourcefile ${BENCHMARKS})
  get_filename_component(exename ${sourcefile} NAME_WE)
  add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile}
    $<TARGET_OBJECTS:testharness>
    $<TARGET_OBJECTS:testutil>)
  target_link_libraries(${exename}${ARTIFACT_SUFFIX} gtest ${LIBS} gflags)
endforeach(sourcefile ${BENCHMARKS})

# For test util library that is build only in DEBUG mode
# and linked to tests. Add test only code that is not #ifdefed for Release here.
set(TESTUTIL_SOURCE
    env/mock_env.cc
    monitoring/thread_status_updater_debug.cc
    unittest/db/db_test_util.cc
    unittest/util/fault_injection_test_env.cc
)
# test utilities are only build in debug
enable_testing()
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})
set(TESTUTILLIB testutillib${ARTIFACT_SUFFIX})
add_library(${TESTUTILLIB} STATIC ${TESTUTIL_SOURCE})
if(MSVC)
  set_target_properties(${TESTUTILLIB} PROPERTIES COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/testutillib${ARTIFACT_SUFFIX}.pdb")
endif()
set_target_properties(${TESTUTILLIB}
      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
      )

# Tests are excluded from Release builds
set(TEST_EXES ${TESTS})

foreach(sourcefile ${TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile}
      $<TARGET_OBJECTS:testharness>
      $<TARGET_OBJECTS:testutil>)
    set_target_properties(${exename}${ARTIFACT_SUFFIX}
      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
      )
    target_link_libraries(${exename}${ARTIFACT_SUFFIX}
      testutillib${ARTIFACT_SUFFIX} gtest ${LIBS} gflags)
    if(NOT "${exename}" MATCHES "db_sanity_test")
      add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
      add_dependencies(check ${exename}${ARTIFACT_SUFFIX})
    endif()
endforeach(sourcefile ${TEST_EXES})

# C executables must link to a shared object
#set(C_TESTS db/c_test.c)
#set(C_TEST_EXES ${C_TESTS})
#
#foreach(sourcefile ${C_TEST_EXES})
#    string(REPLACE ".c" "" exename ${sourcefile})
#    string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
#    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
#    set_target_properties(${exename}${ARTIFACT_SUFFIX}
#      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
#      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
#      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
#      )
#    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${SMARTENGINE_IMPORT_LIB} testutillib${ARTIFACT_SUFFIX} gflags)
#    add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
#    add_dependencies(check ${exename}${ARTIFACT_SUFFIX})
#endforeach(sourcefile ${C_TEST_EXES})
endif()
add_subdirectory(tools)

# Installation and packaging for Linux
if(NOT WIN32)
install(TARGETS ${SMARTENGINE_STATIC_LIB} COMPONENT devel ARCHIVE DESTINATION lib64)
install(TARGETS ${SMARTENGINE_SHARED_LIB} COMPONENT runtime DESTINATION lib64)
install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/smartengine/"
        COMPONENT devel
        DESTINATION include/smartengine)
#set(CMAKE_INSTALL_PREFIX /usr)
endif()
