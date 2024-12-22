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

#include "util/compress/compressor_helper.h"
#include "logger/log_module.h"
#include "options/advanced_options.h"
#include "options/options.h"
#include "util/compress/compressor_factory.h"
#include "util/compress/compressor.h"
#include "util/status.h"

namespace smartengine
{
using namespace common;
using namespace memory;

namespace util
{

CompressorHelper::CompressorHelper() : compressor_(nullptr), compress_buf_(ModId::kCompressor) {}

CompressorHelper::~CompressorHelper()
{
  reset();
}

void CompressorHelper::reset()
{
  if (IS_NOTNULL(compressor_)) {
    CompressorFactory::get_instance().free_compressor(compressor_);
  }
  compress_buf_.reuse();
}

int CompressorHelper::compress(const Slice &raw_data,
                               CompressionType expected_compress_type,
                               Slice &compressed_data,
                               CompressionType &actual_compress_type)
{
  int ret = Status::kOk;

  if (UNLIKELY(raw_data.empty()) ||
      UNLIKELY(!is_valid_compression_type(expected_compress_type))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(raw_data), KE(expected_compress_type));
  } else if (kNoCompression == expected_compress_type) {
    compressed_data.assign(raw_data.data(), raw_data.size());
    actual_compress_type = kNoCompression;
  } else if (FAILED(actual_compress(raw_data, expected_compress_type, compressed_data, actual_compress_type))) {
    SE_LOG(WARN, "fail to actually compress data", K(ret), K(raw_data), KE(expected_compress_type));
  } else {
    se_assert(compressed_data.size() <= raw_data.size());
  }

  return ret;
}

int CompressorHelper::uncompress(const common::Slice &compressed_data,
                                 common::CompressionType compress_type,
                                 char *raw_buf,
                                 int64_t raw_data_size,
                                 Slice &raw_data)
{
  int ret = Status::kOk;
  int64_t uncompress_size = 0;

  if (UNLIKELY(compressed_data.empty()) ||
      UNLIKELY(!is_valid_compression_type(compress_type)) ||
      UNLIKELY(kNoCompression == compress_type) ||
      IS_NULL(raw_buf) ||
      UNLIKELY(raw_data_size <= 0) ||
      UNLIKELY(static_cast<int64_t>(compressed_data.size()) >= raw_data_size)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(compressed_data),
        KE(compress_type), K(raw_data_size), KP(raw_buf));
  } else if (FAILED(setup_compressor(compress_type))) {
    SE_LOG(WARN, "fail to setup compressot", K(ret), KE(compress_type));
  } else if (FAILED(compressor_->uncompress(compressed_data.data(),
                                            compressed_data.size(),
                                            raw_buf,
                                            raw_data_size,
                                            uncompress_size))) {
    SE_LOG(WARN, "fail to uncompress data", K(ret));
  } else if (UNLIKELY(raw_data_size != uncompress_size)) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "the data may be corrupted", K(ret), K(raw_data_size), K(uncompress_size));
  } else {
    raw_data.assign(raw_buf, raw_data_size);
  }

  return ret;
}

int CompressorHelper::setup_compressor(common::CompressionType compress_type)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_valid_compression_type(compress_type)) || (kNoCompression == compress_type)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KE(compress_type));
  } else if (IS_NULL(compressor_) || (compress_type != compressor_->get_compress_type())) {
    // free previous compressor, and alloc new compressor.
    if (IS_NOTNULL(compressor_) && FAILED(CompressorFactory::get_instance().free_compressor(compressor_))) {
      SE_LOG(WARN, "fail to free compressor", K(ret), KP_(compressor));
    } else if (FAILED(CompressorFactory::get_instance().alloc_compressor(compress_type, compressor_))) {
      SE_LOG(WARN, "fail to alloc compressor", K(ret), KE(compress_type));
    } else {
      se_assert(IS_NOTNULL(compressor_));
    }
  }

  return ret;
}

// TODO(Zhao Dongsheng): Add some statistics for actually not compress.
int CompressorHelper::actual_compress(const Slice &raw_data,
                                      CompressionType expected_compress_type,
                                      Slice &compressed_data,
                                      CompressionType &actual_compress_type)
{
  int ret = Status::kOk;
  int64_t max_compress_size = 0;
  int64_t actual_compress_size = 0;

  compress_buf_.reuse();
  if (UNLIKELY(raw_data.empty()) ||
      UNLIKELY(!is_valid_compression_type(expected_compress_type)) ||
      UNLIKELY(kNoCompression == expected_compress_type)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(raw_data), KE(expected_compress_type));
  } else if (FAILED(setup_compressor(expected_compress_type))) {
    SE_LOG(WARN, "fail to get max compress size", K(ret), KE(expected_compress_type));
  } else if (FAILED(compressor_->get_max_compress_size(raw_data.size(), max_compress_size))) {
    SE_LOG(WARN, "fail to get max compress size", K(ret), K(raw_data));
  } else if (FAILED(compress_buf_.reserve(max_compress_size))) {
    SE_LOG(WARN, "fail to reserve compress buf", K(ret), K(max_compress_size));
  } else if (FAILED(compressor_->compress(raw_data.data(),
                                          raw_data.size(),
                                          compress_buf_.data(),
                                          compress_buf_.capacity(),
                                          actual_compress_size))) {
    if (Status::kNotCompress != ret) {
      SE_LOG(WARN, "fail to compress data", K(ret));
    } else {
      // overwrite ret here.
      ret = Status::kOk;
      compressed_data.assign(raw_data.data(), raw_data.size());
      actual_compress_type = kNoCompression;
    }
  } else if (UNLIKELY(actual_compress_size >= static_cast<int64_t>(raw_data.size()))) {
    // Abandon compression if data expands after compression.
    compressed_data.assign(raw_data.data(), raw_data.size());
    actual_compress_type = kNoCompression;
  } else {
    // actually compress
    compressed_data.assign(compress_buf_.data(), actual_compress_size);
    actual_compress_type = compressor_->get_compress_type();
  }

  return ret;
}

} // namespace util
} // namespace smartengine
