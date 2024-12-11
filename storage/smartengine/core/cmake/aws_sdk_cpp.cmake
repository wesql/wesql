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

# cmake -DWITH_OBJSTORE=system|bundled
# bundled is the default

SET(OBJSTORE_INCLUDE_DIR "system default header")
SET(OBJSTORE_LIBRARY_PATH "system default lib path")
SET(OBJSTORE_S3_LIBRARIES "aws-cpp-sdk-s3;aws-cpp-sdk-core")
SET(OBJSTORE_OSS_LIBRARIES "alibabacloud-oss-cpp-sdk")
SET(OBJSTORE_PLATFORM_DEPS "pthread;curl")
SET(OBJSTORE_OSS_TARGET "cpp-sdk")

MACRO(SHOW_OBJSTORE_INFO)
  MESSAGE(STATUS "OBJSTORE_INCLUDE_DIR: ${OBJSTORE_INCLUDE_DIR}")
  MESSAGE(STATUS "OBJSTORE_LIBRARY_PATH: ${OBJSTORE_LIBRARY_PATH}")
  MESSAGE(STATUS "OBJSTORE_S3_LIBRARIES: ${OBJSTORE_S3_LIBRARIES}")
  MESSAGE(STATUS "OBJSTORE_OSS_LIBRARIES: ${OBJSTORE_OSS_LIBRARIES}")
  MESSAGE(STATUS "OBJSTORE_PLATFORM_DEPS: ${OBJSTORE_PLATFORM_DEPS}")
ENDMACRO()

MACRO(PREPARE_BUNDLED_OJBSTORE)
  SET(OBJSTORE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/extra/aws-sdk-cpp")

  set(BUILD_ONLY "s3" CACHE STRING "AWS sdk components to build")
  set(ENABLE_TESTING OFF  CACHE BOOL "AWS sdk building unit and integration tests")
  set(AUTORUN_UNIT_TESTS OFF CACHE BOOL "AWS sdk auto run unittests")
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build aws sdk static library")
  set(CMAKE_BUILD_TYPE Release CACHE STRING "AWS sdk build type")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-shadow -Wno-error=non-virtual-dtor -Wno-error=extra-semi -Wno-error=undef -Wno-error=cast-qual -Wno-error=overloaded-virtual")

  add_subdirectory(${PROJECT_SOURCE_DIR}/../../../extra/aws-sdk-cpp ${OBJSTORE_INSTALL_PREFIX})
  # compilation result installation is later phase, mkdir include path in advance avoid compile error.
  FILE(MAKE_DIRECTORY "${OBJSTORE_INSTALL_PREFIX}/include")
ENDMACRO()

MACRO(FIND_SYSTEM_OBJSTORE)
  FIND_PACKAGE(AWSSDK REQUIRED COMPONENTS "s3")
ENDMACRO()

MACRO (MYSQL_CHECK_OBJSTORE_S3)
  IF(NOT WITH_OBJSTOR)
    SET(WITH_OBJSTOR "bundled" CACHE STRING "By default use bundled ObjStore library")
  ENDIF()

  IF(WITH_OBJSTOR STREQUAL "bundled")
    MESSAGE(STATUS "WITH_OBJSTOR is bundled, download aws-sdk-cpp and compile it")
    PREPARE_BUNDLED_OJBSTORE()
    MESSAGE(STATUS "aws-sdk-cpp will be installed to ${OBJSTORE_INSTALL_PREFIX} in the compile phase")

    # Set the variables for the project
    SET(OBJSTORE_INCLUDE_DIR "${OBJSTORE_INSTALL_PREFIX}/include")
    SET(OBJSTORE_LIBRARY_PATH "${OBJSTORE_INSTALL_PREFIX}/lib64")
    SET(OBJSTORE_LIBRARY ${OBJSTORE_LIBRARIES})
    SET(OBJSTORE_PLATFORM_DEPS ${OBJSTORE_PLATFORM_DEPS})
    # Prepare include and ld path
    INCLUDE_DIRECTORIES(${OBJSTORE_INCLUDE_DIR})
    LINK_DIRECTORIES(${OBJSTORE_LIBRARY_PATH})
  ELSEIF(WITH_OBJSTOR STREQUAL "system")
    MESSAGE(STATUS "WITH_OBJSTOR is system, use system aws s3 lib")
    FIND_SYSTEM_OBJSTORE()
    SET(OBJSTORE_LIBRARY ${AWSSDK_LINK_LIBRARIES})
    SET(OBJSTORE_PLATFORM_DEPS ${OBJSTORE_PLATFORM_DEPS})
  ELSE()
    MESSAGE(FATAL_ERROR "WITH_OBJSTOR must be bundled or system")
  ENDIF()

  SHOW_OBJSTORE_INFO()
ENDMACRO()

MACRO (MYSQL_BUILD_OBJSTORE)
  SET(OBJSTORE_SRC "${PROJECT_SOURCE_DIR}/../../../mysys")

  SET(MY_OBJSTORE_SOURCES
    ${OBJSTORE_SRC}/objstore/local.cc
    ${OBJSTORE_SRC}/objstore/objstore.cc
    ${OBJSTORE_SRC}/objstore/objstore_lock.cc
    ${OBJSTORE_SRC}/objstore/aliyun_oss.cc
    ${OBJSTORE_SRC}/objstore/s3.cc)

  # add mysql src dir
  INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/../../../")
  # add mysql include dir
  INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/../../../include")

  ADD_CONVENIENCE_LIBRARY(myobjstore ${MY_OBJSTORE_SOURCES} COMPILE_OPTIONS "-fPIC")

  IF(NOT APPLE)
    TARGET_LINK_LIBRARIES(myobjstore_objlib
      PRIVATE
      ${OBJSTORE_S3_LIBRARIES}
      ${OBJSTORE_OSS_LIBRARIES}
      ${OBJSTORE_PLATFORM_DEPS})
    add_dependencies(myobjstore_objlib ${OBJSTORE_OSS_TARGET})
  ENDIF()
  TARGET_LINK_LIBRARIES(myobjstore
    PRIVATE
    ${OBJSTORE_S3_LIBRARIES}
    ${OBJSTORE_OSS_LIBRARIES}
    ${OBJSTORE_PLATFORM_DEPS})
  add_dependencies(myobjstore ${OBJSTORE_OSS_TARGET})

  # shut up the compile warning when encountering aws-sdk-cpp header files.
  SET_SOURCE_FILES_PROPERTIES(${OBJSTORE_SRC}/objstore/s3.cc ${OBJSTORE_SRC}/objstore/objstore.cc PROPERTIES COMPILE_FLAGS
    "-Wno-error=extra-semi -Wno-error=undef -Wno-error=cast-qual -Wno-error=overloaded-virtual")
ENDMACRO()
