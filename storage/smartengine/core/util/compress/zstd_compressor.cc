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

#ifdef HAVE_ZSTD

#include "util/compress/zstd_compressor.h"
#include "logger/log_module.h"
#include "util/status.h"

namespace smartengine
{
using namespace common;

namespace util
{

ZSTDCompressor::ZSTDCompressor()
    : cctx_(nullptr),
      dctx_(nullptr)
{
  type_ = kZSTD;
}

ZSTDCompressor::~ZSTDCompressor()
{
  destroy();
}

void ZSTDCompressor::destroy()
{
  if (IS_NOTNULL(cctx_)) {
    ZSTD_freeCCtx(cctx_);
    cctx_ = nullptr;
  }

  if (IS_NOTNULL(dctx_)) {
    ZSTD_freeDCtx(dctx_);
    dctx_ = nullptr;
  }
}

int ZSTDCompressor::inner_compress(const char *raw_data,
                                     const int64_t raw_data_size,
                                     char *compress_buf,
                                     const int64_t compress_buf_size,
                                     int64_t &compressed_data_size)
{
  int ret = Status::kOk;
  size_t compressed_ret = 0;

  if (IS_NULL(cctx_) && IS_NULL(cctx_ = ZSTD_createCCtx())) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "Failed to create zstd compress context", K(ret));
  } else {
    compressed_ret = ZSTD_compressCCtx(cctx_,
                                       compress_buf,
                                       compress_buf_size,
                                       raw_data,
                                       raw_data_size,
                                       7 /**compress_level*/);

    if (ZSTD_isError(compressed_ret)) {
      ret = Status::kNotCompress;
#ifndef NDEBUG
      SE_LOG(WARN, "Failed to compress data by Zstd", K(ret), K(compressed_ret));
#endif
    } else {
      compressed_data_size = compressed_ret;
    }
  }

  return ret;
}

int ZSTDCompressor::inner_uncompress(const char *compressed_data,
                                     const int64_t compressed_data_size,
                                     char *raw_buf,
                                     const int64_t raw_buf_size,
                                     int64_t &raw_data_size)
{
  int ret = Status::kOk;
  size_t uncompressed_ret = 0;

  if (IS_NULL(dctx_) && IS_NULL(dctx_ = ZSTD_createDCtx())) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "Failed to create zstd decompress context", K(ret));
  } else {
    uncompressed_ret = ZSTD_decompressDCtx(dctx_,
                                           raw_buf,
                                           raw_buf_size,
                                           compressed_data,
                                           compressed_data_size);
    if (ZSTD_isError(uncompressed_ret)) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "Failed to uncompress data by Zstd", K(ret), K(uncompressed_ret));
    } else {
      raw_data_size = uncompressed_ret;
    }
  }

  return ret;
}

int64_t ZSTDCompressor::inner_get_max_compress_size(const int64_t raw_data_size) const
{
  return ZSTD_compressBound(raw_data_size);
}

} //namespace util
} //namespace smartengine

#endif // HAVE_ZSTD