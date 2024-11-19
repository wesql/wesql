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

#include "table/large_object.h"
#include "storage/extent_space_manager.h"
#include "util/compress/compressor_helper.h"

namespace smartengine
{
using namespace common;
using namespace memory;
using namespace util;

namespace table
{

LargeValue::LargeValue() : raw_size_(0),
                           size_(0),
                           compress_type_(common::kNoCompression),
                           extents_(),
                           compressor_helper_(),
                           data_buf_(nullptr),
                           data_buf_size_(0),
                           raw_data_buf_(nullptr),
                           raw_data_buf_size_(0)
{}

LargeValue::~LargeValue()
{
  destroy();
}

void LargeValue::destroy()
{
  if (IS_NOTNULL(data_buf_)) {
    base_memalign_free(data_buf_);
    data_buf_ = nullptr;
    data_buf_size_ = 0;
  }

  if (IS_NOTNULL(raw_data_buf_)) {
    base_memalign_free(raw_data_buf_);
    raw_data_buf_ = nullptr;
    raw_data_buf_size_ = 0;
  }
}

void LargeValue::reuse()
{
  raw_size_ = 0;
  size_ = 0;
  compress_type_ = common::kNoCompression;
  extents_.clear();
}

int LargeValue::convert_to_normal_format(const Slice &large_object_value, Slice &normal_value)
{
  int ret = Status::kOk;
  storage::IOExtent *extent = nullptr;
  Slice dummy_read_result;
  char *curr_read_buf = nullptr;
  int64_t pos = 0;

  if (FAILED(this->deserialize(large_object_value.data(), large_object_value.size(), pos))) {
    SE_LOG(WARN, "fail to deserialize large object value", K(ret));
  } else if (FAILED(prepare_buf(storage::MAX_EXTENT_SIZE * extents_.size(), raw_size_, static_cast<CompressionType>(compress_type_)))) {
    SE_LOG(WARN, "fail to prepare buf for large object", K(ret));
  } else {
    // Read stored large value
    for (uint32_t i = 0; SUCCED(ret) && i < extents_.size(); ++i) {
      extent = nullptr;
      curr_read_buf = data_buf_ + i * storage::MAX_EXTENT_SIZE;
      if (FAILED(storage::ExtentSpaceManager::get_instance().get_readable_extent(extents_[i], extent))) {
        SE_LOG(WARN, "fail to get large object readable extent", K(ret), "extent_id", extents_[i]);
      } else {
        extent->set_large_object_extent();
        if (FAILED(extent->read(nullptr, 0, storage::MAX_EXTENT_SIZE, curr_read_buf, dummy_read_result))) {
          SE_LOG(WARN, "fail to read large object extent", K(ret), K(i), "extent_id", extents_[i]);
        }
        DELETE_OBJECT(ModId::kIOExtent, extent);
      }
    }

    // Uncompress large value if need.
    if (SUCCED(ret)) {
      if (kNoCompression == compress_type_) {
        assert(size_ == raw_size_);
        normal_value.assign(data_buf_, size_);
      } else if (FAILED(compressor_helper_.uncompress(Slice(data_buf_, size_),
                                                      static_cast<CompressionType>(compress_type_),
                                                      raw_data_buf_,
                                                      raw_size_,
                                                      normal_value))) {
        SE_LOG(WARN, "fail to uncompress large value", K(ret));
      }
    }
  }

  return ret;
}

int LargeValue::prepare_buf(int64_t size, int64_t raw_size, CompressionType compress_type)
{
  int ret = Status::kOk;

  // prepare data buf
  if (size > data_buf_size_) {
    if (IS_NOTNULL(data_buf_)) {
      base_memalign_free(data_buf_);
      data_buf_ = nullptr;
      data_buf_size_ = 0;
    }

    if (IS_NULL(data_buf_ = reinterpret_cast<char *>(base_memalign(DIOHelper::DIO_ALIGN_SIZE, size, ModId::kLargeObject)))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for data buf", K(ret), K(size));
    } else {
      data_buf_size_ = size;
    }
  }

  // prepare raw data buf
  if (SUCCED(ret) && (kNoCompression != compress_type)) {
    if (raw_size > raw_data_buf_size_) {
      if (IS_NOTNULL(raw_data_buf_)) {
        base_memalign_free(raw_data_buf_);
        raw_data_buf_ = nullptr;
        raw_data_buf_size_ = 0;
      }

      if (IS_NULL(raw_data_buf_ = reinterpret_cast<char *>(base_memalign(DIOHelper::DIO_ALIGN_SIZE, raw_size, ModId::kLargeObject)))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for raw data buf", K(ret), K(raw_size));
      } else {
        raw_data_buf_size_ = raw_size;
      }
    }
  }

  return ret;
}

DEFINE_COMPACTIPLE_SERIALIZATION(LargeValue, raw_size_, size_, compress_type_, extents_)

LargeObject::LargeObject() : key_(), value_() {}

LargeObject::~LargeObject() {}

DEFINE_COMPACTIPLE_SERIALIZATION(LargeObject, key_, value_)

} // namespace table
} // namespace smartengine