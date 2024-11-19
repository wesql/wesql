//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// File names used by DB code

#pragma once
#include <stdint.h>
#include <string>

namespace smartengine
{
namespace common
{
struct ImmutableDBOptions;
} // namespace common

namespace schema
{
class TableSchema;
} // namespace schema

namespace util
{
class Env;

enum FileType
{
  kInvalidFileType,
  kCurrentFile,
  kLockFile,
  kWalFile,
  kDataFile,
  kManifestFile,
  kCheckpointFile,
  kTempFile,
  kMaxFileType
};

class FileNameUtil
{
public:
  static void dump(const common::ImmutableDBOptions &immutable_db_options, const std::string &dbname);
  static int parse_file_name(const std::string &file_name, int64_t &file_number, FileType &file_type);

  static std::string current_file_path(const std::string &dir);
  static std::string lock_file_path(const std::string &dir);
  static std::string wal_file_name(int64_t file_number);
  static std::string wal_file_path(const std::string &dir, int64_t file_number);
  static std::string data_file_name(int64_t file_number);
  static std::string data_file_path(const std::string &dir, int64_t file_number);
  static std::string manifest_file_name(int64_t file_number);
  static std::string manifest_file_path(const std::string &dir, int64_t file_number);
  static std::string checkpoint_file_name(int64_t file_number);
  static std::string checkpoint_file_path(const std::string &dir, int64_t file_number);
  static std::string temp_file_name(int64_t file_number);
  static std::string temp_file_path(const std::string &dir, int64_t file_number);

private:
  static const char *DATA_FILE_SUFFIX;
  static const char *WAL_FILE_SUFFIX;
  static const char *MANIFEST_FILE_SUFFIX;
  static const char *CHECKPOINT_FILE_SUFFIX;
  static const char *TEMP_FILE_SUFFIX;
  static const char *CURRENT_FILE_NAME;
  static const char *LOCK_FILE_NAME;
  static const char DELIMITER = '.';

private:
  static std::string file_name(int64_t file_number, const char *suffix);
  static int dump_dir(util::Env *env, const std::string &dir);

};

}  // namespace util
}  // namespace smartengine
