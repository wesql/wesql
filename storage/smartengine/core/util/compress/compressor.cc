/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "util/compress/compressor.h"
#include "logger/log_module.h"

namespace smartengine
{
using namespace common;
namespace util
{

int Compressor::compress(const char *raw_data,
                         const int64_t raw_data_size,
                         char *compress_buf,
                         const int64_t compress_buf_size,
                         int64_t &compressed_data_size)
{
  int ret = Status::kOk;
  int64_t max_compress_size = 0;

  if (IS_NULL(raw_data) || (raw_data_size <= 0)
      || (IS_NULL(compress_buf)) || (compress_buf_size <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "Invalid argument", K(ret), KP(raw_data), K(raw_data_size),
        KP(compress_buf), K(compress_buf_size));
  } else if (FAILED(get_max_compress_size(raw_data_size, max_compress_size))) {
    SE_LOG(WARN, "Failed to get max compress size", K(ret), K(raw_data_size));
  } else if (compress_buf_size < max_compress_size) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "The compress buf may not be enoguh", K(ret),
        K(raw_data_size), K(compress_buf_size), K(max_compress_size));
  } else if (FAILED(inner_compress(raw_data,
                                   raw_data_size,
                                   compress_buf,
                                   compress_buf_size,
                                   compressed_data_size))) {
    SE_LOG(WARN, "Failed to compress data", K(ret), KE_(type));
  } else {
    //succeed
  }

  return ret;
}

int Compressor::uncompress(const char *compressed_data,
                           const int64_t compressed_data_size,
                           char *raw_buf,
                           const int64_t raw_buf_size,
                           int64_t &raw_data_size)
{
  int ret = Status::kOk;

  if (IS_NULL(compressed_data) || (compressed_data_size <= 0)
      || (IS_NULL(raw_buf)) || (raw_buf_size <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "Invalid argument", K(ret), KP(compressed_data),
        K(compressed_data_size), KP(raw_buf), K(raw_buf_size));
  } else if (raw_buf_size < compressed_data_size) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "The size of raw buf must be larger than the size of compressed data",
        K(ret), K(raw_buf_size), K(compressed_data_size));
  } else if (FAILED(inner_uncompress(compressed_data,
                                     compressed_data_size,
                                     raw_buf,
                                     raw_buf_size,
                                     raw_data_size))) {
    SE_LOG(WARN, "Failed to decompress data", K(ret), KE_(type));
  } else {
    //succeed
  }

  return ret;
}

int Compressor::get_max_compress_size(const int64_t raw_data_size, int64_t &max_compress_size) const
{
  int ret = Status::kOk;

  if (raw_data_size <= 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "Invalid argument", K(ret), K(raw_data_size));
  } else {
    max_compress_size = inner_get_max_compress_size(raw_data_size);
  }

  return ret;
}

} // namespace util
} // namespace smartengine
