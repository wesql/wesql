# Copyright (c) 2024, ApeCloud Inc Holding Limited.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# download from https://github.com/aliyun/aliyun-oss-cpp-sdk/archive/refs/tags/1.10.0.tar.gz
set(OSS_SDK_VERSION "1.10.0")
set(LOCAL_ALIYUN_OSS_SDK_ZIP "${PROJECT_SOURCE_DIR}/../../../extra/aliyun-oss-cpp-sdk-${OSS_SDK_VERSION}.tar.gz")
set(LOCAL_ALIYUN_OSS_SDK_DIR "${PROJECT_SOURCE_DIR}/../../../extra/aliyun-oss-cpp-sdk-${OSS_SDK_VERSION}")
set(ALIYUN_OSS_SDK_LIB_DIR "${CMAKE_BINARY_DIR}/lib")
set(ALIYUN_OSS_SDK_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/../../../extra/aliyun-oss-cpp-sdk-${OSS_SDK_VERSION}/sdk/include")

IF(NOT EXISTS ${LOCAL_ALIYUN_OSS_SDK_ZIP})
  MESSAGE(FATAL_ERROR "${LOCAL_ALIYUN_OSS_SDK_ZIP} not found")
ELSE()
    MESSAGE(STATUS "${LOCAL_ALIYUN_OSS_SDK_ZIP} found")
    MESSAGE(STATUS "cd ${PROJECT_SOURCE_DIR}/../../../extra && tar -xvzf aliyun-oss-cpp-sdk-${OSS_SDK_VERSION}.tar.gz")
    execute_process(COMMAND tar -xvzf ${LOCAL_ALIYUN_OSS_SDK_ZIP} WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/../../../extra RESULT_VARIABLE tar_result)
    IF (NOT tar_result EQUAL "0")
      MESSAGE(FATAL_ERROR "tar -xvzf ${LOCAL_ALIYUN_OSS_SDK_ZIP} failed")
    ENDIF()
ENDIF()

MACRO(PREPARE_OJBSTORE_ALIYUN_OSS)
  set(BUILD_SHARED_LIBS  OFF CACHE BOOL "Enable shared library")
  set(BUILD_SAMPLE OFF CACHE BOOL "Build sample")
  set(BUILD_TESTS  OFF CACHE BOOL "Build unit and perfermence tests")
  set(ENABLE_COVERAGE OFF CACHE BOOL "Flag to enable/disable building code with -fprofile-arcs and -ftest-coverage. Gcc only")
  set(ENABLE_RTTI ON CACHE BOOL   "Flag to enable/disable building code with RTTI information")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
  
  add_subdirectory(${LOCAL_ALIYUN_OSS_SDK_DIR} ${CMAKE_BINARY_DIR}/extra/aliyun-oss-cpp-sdk)
ENDMACRO()

MACRO(MYSQL_CHECK_OBJSTORE_ALIYUN_OSS)
  IF(NOT WITH_OBJSTOR_ALIYUN_OSS)
    SET(WITH_OBJSTOR_ALIYUN_OSS "bundled" CACHE STRING "By default use bundled ObjStore library")
  ENDIF()

  IF(WITH_OBJSTOR_ALIYUN_OSS STREQUAL "bundled")
    MESSAGE(STATUS "WITH_OBJSTOR_ALIYUN_OSS is bundled, download aliyun_oss_sdk and compile it")
    PREPARE_OJBSTORE_ALIYUN_OSS()

    INCLUDE_DIRECTORIES(${ALIYUN_OSS_SDK_INCLUDE_DIR})
    LINK_DIRECTORIES(${ALIYUN_OSS_SDK_LIB_DIR})
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_OBJSTOR_ALIYUN_OSS must be bundled")
  ENDIF()
ENDMACRO()
