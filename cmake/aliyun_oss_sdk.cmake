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

SET(OSS_SDK_VERSION "1.10.0")
SET(LOCAL_ALIYUN_OSS_SDK_ZIP
  "${PROJECT_SOURCE_DIR}/extra/aliyun-oss-cpp-sdk-${OSS_SDK_VERSION}.tar.gz")
SET(LOCAL_ALIYUN_OSS_SDK_DIR
  "${PROJECT_SOURCE_DIR}/extra/aliyun-oss-cpp-sdk-${OSS_SDK_VERSION}")
SET(ALIYUN_OSS_SDK_INCLUDE_DIR
  "${LOCAL_ALIYUN_OSS_SDK_DIR}/sdk/include")
SET(OBJSTORE_OSS_LIBRARIES cpp-sdk)
SET(OBJSTORE_OSS_TARGET cpp-sdk)

MACRO(PREPARE_OBJSTORE_ALIYUN_OSS)
  IF(NOT EXISTS "${LOCAL_ALIYUN_OSS_SDK_ZIP}")
    MESSAGE(FATAL_ERROR "${LOCAL_ALIYUN_OSS_SDK_ZIP} not found")
  ENDIF()

  IF(NOT EXISTS "${LOCAL_ALIYUN_OSS_SDK_DIR}/CMakeLists.txt")
    EXECUTE_PROCESS(
      COMMAND ${CMAKE_COMMAND} -E tar xzf ${LOCAL_ALIYUN_OSS_SDK_ZIP}
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/extra
      RESULT_VARIABLE OSS_UNPACK_RESULT
      OUTPUT_QUIET
      ERROR_VARIABLE OSS_UNPACK_ERROR)
    IF(NOT OSS_UNPACK_RESULT EQUAL 0)
      MESSAGE(FATAL_ERROR
        "Unable to unpack Aliyun OSS SDK: ${OSS_UNPACK_ERROR}")
    ENDIF()
  ENDIF()

  SET(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static provider SDKs" FORCE)
  SET(BUILD_SAMPLE OFF CACHE BOOL "Build Aliyun OSS samples" FORCE)
  SET(BUILD_TESTS OFF CACHE BOOL "Build Aliyun OSS tests" FORCE)
  SET(ENABLE_COVERAGE OFF CACHE BOOL "Build Aliyun OSS with coverage" FORCE)
  SET(ENABLE_RTTI ON CACHE BOOL "Build Aliyun OSS with RTTI" FORCE)

  # The SDK owns its warning policy. SYSTEM prevents those headers from
  # inheriting MySQL/WeSQL warning policy in myobjstore.
  ADD_SUBDIRECTORY(
    ${LOCAL_ALIYUN_OSS_SDK_DIR}
    ${CMAKE_BINARY_DIR}/extra/aliyun-oss-cpp-sdk
    SYSTEM)

  # Aliyun OSS 1.10.0 intentionally supports older curl releases. Keep its
  # own -Werror policy, but do not fail on curl APIs deprecated by newer curl.
  IF(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    TARGET_COMPILE_OPTIONS(cpp-sdk PRIVATE
      -Wno-error=deprecated-declarations)
  ENDIF()
ENDMACRO()

MACRO(MYSQL_CHECK_OBJSTORE_ALIYUN_OSS)
  IF(NOT DEFINED WITH_OBJSTOR_ALIYUN_OSS)
    SET(WITH_OBJSTOR_ALIYUN_OSS "bundled" CACHE STRING
      "Use the bundled Aliyun OSS SDK")
  ENDIF()
  SET_PROPERTY(CACHE WITH_OBJSTOR_ALIYUN_OSS PROPERTY STRINGS bundled)

  IF(WITH_OBJSTOR_ALIYUN_OSS STREQUAL "bundled")
    PREPARE_OBJSTORE_ALIYUN_OSS()
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_OBJSTOR_ALIYUN_OSS must be bundled")
  ENDIF()
ENDMACRO()
