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

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util/file_name.h"
#include <inttypes.h>
#include <stdio.h>
#include "logger/log_module.h"
#include "options/db_options.h"
#include "schema/table_schema.h"
#include "util/se_constants.h"
#include "util/string_util.h"

namespace smartengine
{
using namespace common;

namespace util
{

const char *FileNameUtil::DATA_FILE_SUFFIX = "sst";
const char *FileNameUtil::WAL_FILE_SUFFIX = "wal";
const char *FileNameUtil::MANIFEST_FILE_SUFFIX = "manifest";
const char *FileNameUtil::CHECKPOINT_FILE_SUFFIX = "checkpoint";
const char *FileNameUtil::TEMP_FILE_SUFFIX = "tmp";
const char *FileNameUtil::CURRENT_FILE_NAME = "CURRENT";
const char *FileNameUtil::LOCK_FILE_NAME = "LOCK";

void FileNameUtil::dump(const common::ImmutableDBOptions &immutable_db_options, const std::string &db_name)
{

  __SE_LOG(SYSTEM, "SMARTENGINE FILES SUMMARY:\n");
  dump_dir(immutable_db_options.env, db_name);

  // dump files summary in directory data_dir
  for (auto data_path : immutable_db_options.db_paths) {
    if (0 != db_name.compare(data_path.path)) {
      dump_dir(immutable_db_options.env, data_path.path);
    }
  }
}

int FileNameUtil::parse_file_name(const std::string &file_name, int64_t &file_number, FileType &file_type)
{
  int ret = common::Status::kOk;
  Slice rest(file_name);
  uint64_t number = 0;

  if (UNLIKELY(file_name.empty())) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(file_name));
  } else if ("." == file_name || ".." == file_name) {
    ret = Status::kNotSupported;
  } else if (Slice(CURRENT_FILE_NAME) == rest) {
    file_number = 0;
    file_type = kCurrentFile;
  } else if (Slice(LOCK_FILE_NAME) == rest) {
    file_number = 0;
    file_type = kLockFile;
  } else if (!ConsumeDecimalNumber(&rest, &number)) {
    ret = common::Status::kErrorUnexpected;
#ifndef NDEBUG
    SE_LOG(WARN, "fail to consume decimal number", K(ret), K(file_name), K(rest));
#endif
  } else if (rest.size() <= 1 || DELIMITER != rest[0]) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "the file name format is not correct", K(ret), K(file_name), K(rest));
  } else {
    file_number = static_cast<int64_t>(number);
    rest.remove_prefix(1);
    if (Slice(WAL_FILE_SUFFIX) == rest) {
      file_type = kWalFile;
    } else if (Slice(DATA_FILE_SUFFIX) == rest) {
      file_type = kDataFile;
    } else if (Slice(MANIFEST_FILE_SUFFIX) == rest) {
      file_type = kManifestFile;
    } else if (Slice(CHECKPOINT_FILE_SUFFIX) == rest) {
      file_type = kCheckpointFile;
    } else if (Slice(TEMP_FILE_SUFFIX) == rest) {
      file_type = kTempFile;
    } else {
      ret = common::Status::kNotSupported;
      SE_LOG(WARN, "Unsupport file type", K(ret), K(file_name), K(rest));
    }
  }

  return ret;
}

std::string FileNameUtil::current_file_path(const std::string &dir)
{
  return dir + "/" + CURRENT_FILE_NAME; 
}

std::string FileNameUtil::lock_file_path(const std::string &dir)
{
  return dir + "/" + LOCK_FILE_NAME;
}

std::string FileNameUtil::wal_file_name(int64_t file_number)
{
  return file_name(file_number, WAL_FILE_SUFFIX);
}

std::string FileNameUtil::wal_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + wal_file_name(file_number);
}

std::string FileNameUtil::data_file_name(int64_t file_number)
{
  return file_name(file_number, DATA_FILE_SUFFIX);
}

std::string FileNameUtil::data_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + data_file_name(file_number);
}

std::string FileNameUtil::manifest_file_name(int64_t file_number)
{
  return file_name(file_number, MANIFEST_FILE_SUFFIX);
}

std::string FileNameUtil::manifest_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + manifest_file_name(file_number);
}

std::string FileNameUtil::checkpoint_file_name(int64_t file_number)
{
  return file_name(file_number, CHECKPOINT_FILE_SUFFIX);
}

std::string FileNameUtil::checkpoint_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + checkpoint_file_name(file_number);
}

std::string FileNameUtil::temp_file_name(int64_t file_number)
{
  return file_name(file_number, TEMP_FILE_SUFFIX);
}

std::string FileNameUtil::temp_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + temp_file_name(file_number);
}

std::string FileNameUtil::file_name(int64_t file_number, const char *suffix)
{
  assert(nullptr != suffix);
  char buf[storage::MAX_FILE_PATH_SIZE] = {0};
  snprintf(buf, sizeof(buf), "%ld%c%s", file_number, DELIMITER, suffix);
  return std::string(buf);
}

int FileNameUtil::dump_dir(util::Env *env, const std::string &dir)
{
  int ret = Status::kOk;
  int64_t file_number = 0;
  FileType file_type = kInvalidFileType;
  std::string file_name;
  std::string file_path;
  uint64_t file_size = 0;
  std::vector<std::string> files;
  int64_t sst_file_count = 0;
  int64_t wal_file_count = 0;
  int64_t manifest_file_count = 0;
  int64_t checkpoint_file_count = 0; 
  int64_t temp_file_count = 0;

  if (IS_NULL(env) || UNLIKELY(dir.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(dir));
  } else if (FAILED(env->GetChildren(dir, &files).code())) {
    if (Status::kNotFound != ret) {
      SE_LOG(WARN, "fail to read db directory", K(ret), K(dir));
    }
  } else {
    std::sort(files.begin(), files.end());
    for (uint64_t i = 0; i < files.size(); ++i) {
      file_name = files[i];
      file_path = dir + "/" + file_name;
      if (FAILED(parse_file_name(file_name, file_number, file_type))) {
        if (Status::kNotSupported != ret) {
          SE_LOG(WARN, "fail to parse file name", K(ret), K(file_name), K(file_number), KE(file_type));
        }
      } else if (FAILED(env->GetFileSize(file_path, &file_size).code())) {
        SE_LOG(WARN, "fail to get file size", K(ret), K(file_path), K(file_size));
      } else {
        switch (file_type) {
          case kDataFile:
            ++sst_file_count;
            __SE_LOG(DEBUG, "data file: %s, size: %ld Bytes", file_name.c_str(), file_size);
            break;
          case kWalFile:
            ++wal_file_count;
            __SE_LOG(DEBUG, "wal file: %s, size: %ld Bytes", file_name.c_str(), file_size);
            break;
          case kManifestFile:
            ++manifest_file_count;
            __SE_LOG(INFO, "manifest file: %s, size: %ld Bytes", file_name.c_str(), file_size);
            break;
          case kCheckpointFile:
            ++checkpoint_file_count;
            __SE_LOG(INFO, "checkpoint file: %s, size: %ld Bytes", file_name.c_str(), file_size);
            break;
          case kCurrentFile:
            __SE_LOG(INFO, "current file: %s, size: %ld Bytes", file_name.c_str(), file_size);
            break;
          case kLockFile:
            __SE_LOG(INFO, "lock file: %s, size: %ld Bytes", file_name.c_str(), file_size);
            break;
          case kTempFile:
            ++temp_file_count;
            __SE_LOG(INFO, "temp file: %s, size: %ld Bytes", file_name.c_str(), file_size);
            break;
          default:
            ret = Status::kNotSupported;
            SE_LOG(WARN, "unsupport file type", K(file_name), K(file_number), KE(file_type));
        }
      }
    }

    __SE_LOG(SYSTEM, "Files Summary in directory: %s, sst_file_count = %ld, wal_file_count = %ld, "
        "manifest_file_count = %ld, checkpoint_file_count = %ld, temp_file_count = %ld\n",
        dir.c_str(), sst_file_count, wal_file_count, manifest_file_count, checkpoint_file_count, temp_file_count);
  }

  return ret;
  
}

}  // namespace util
}  // namespace smartengine
