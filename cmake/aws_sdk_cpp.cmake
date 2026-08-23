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

# cmake -DWITH_OBJSTOR=system|bundled
# bundled is the default.

SET(OBJSTORE_S3_LIBRARIES aws-cpp-sdk-s3 aws-cpp-sdk-core)

MACRO(SHOW_OBJSTORE_S3_INFO)
  MESSAGE(STATUS "WITH_OBJSTOR: ${WITH_OBJSTOR}")
  MESSAGE(STATUS "OBJSTORE_S3_LIBRARIES: ${OBJSTORE_S3_LIBRARIES}")
  MESSAGE(STATUS "OBJSTORE_PLATFORM_DEPS: ${OBJSTORE_PLATFORM_DEPS}")
ENDMACRO()

MACRO(PREPARE_BUNDLED_OBJSTORE_S3)
  SET(OBJSTORE_AWS_BINARY_DIR "${CMAKE_BINARY_DIR}/extra/aws-sdk-cpp")

  SET(BUILD_ONLY "s3" CACHE STRING "AWS SDK components to build" FORCE)
  SET(ENABLE_TESTING OFF CACHE BOOL "Build AWS SDK tests" FORCE)
  SET(AUTORUN_UNIT_TESTS OFF CACHE BOOL "Run AWS SDK tests" FORCE)
  SET(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static provider SDKs" FORCE)
  SET(ENABLE_ZLIB_REQUEST_COMPRESSION OFF CACHE BOOL
    "Keep the AWS SDK out of MySQL's private zlib target" FORCE)
  SET(AWS_SDK_WARNINGS_ARE_ERRORS OFF CACHE BOOL
    "Do not promote AWS SDK warnings to errors" FORCE)
  SET(AWS_WARNINGS_ARE_ERRORS OFF CACHE BOOL
    "Do not promote AWS CRT warnings to errors" FORCE)

  # The bundled release tree has no .git directory. Let the SDK use its
  # generated version header without emitting a failed Git probe.
  SET(OBJSTORE_SAVED_GIT_FOUND ${GIT_FOUND})
  SET(GIT_FOUND FALSE)

  # SYSTEM marks every AWS/CRT target created below, so their public headers
  # remain third-party headers when consumed by WeSQL targets.
  ADD_SUBDIRECTORY(
    ${PROJECT_SOURCE_DIR}/extra/aws-sdk-cpp
    ${OBJSTORE_AWS_BINARY_DIR}
    SYSTEM)

  SET(GIT_FOUND ${OBJSTORE_SAVED_GIT_FOUND})
  UNSET(OBJSTORE_SAVED_GIT_FOUND)
ENDMACRO()

MACRO(FIND_SYSTEM_OBJSTORE_S3)
  FIND_PACKAGE(AWSSDK REQUIRED COMPONENTS s3)
  SET(OBJSTORE_S3_LIBRARIES ${AWSSDK_LINK_LIBRARIES})
ENDMACRO()

MACRO(MYSQL_CHECK_OBJSTORE_S3)
  IF(NOT DEFINED WITH_OBJSTOR)
    SET(WITH_OBJSTOR "bundled" CACHE STRING
      "Use bundled or system AWS ObjectStore SDK")
  ENDIF()
  SET_PROPERTY(CACHE WITH_OBJSTOR PROPERTY STRINGS bundled system)

  FIND_PACKAGE(Threads REQUIRED)
  IF(NOT TARGET ext::curl)
    MESSAGE(FATAL_ERROR "ObjectStore requires MySQL's curl target")
  ENDIF()
  SET(OBJSTORE_PLATFORM_DEPS
    Threads::Threads
    ext::curl
    OpenSSL::SSL
    OpenSSL::Crypto)

  IF(WITH_OBJSTOR STREQUAL "bundled")
    PREPARE_BUNDLED_OBJSTORE_S3()
  ELSEIF(WITH_OBJSTOR STREQUAL "system")
    FIND_SYSTEM_OBJSTORE_S3()
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_OBJSTOR must be bundled or system")
  ENDIF()

  SHOW_OBJSTORE_S3_INFO()
ENDMACRO()
