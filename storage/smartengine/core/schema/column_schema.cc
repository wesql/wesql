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

#include "schema/column_schema.h"
#include "logger/log_module.h"

namespace smartengine
{
using namespace common;

namespace schema
{

ColumnSchema::ColumnSchema()
    : type_(ColumnType::INVALID_COLUMN_TYPE),
      attr_(0),
      column_count_(0),
      null_bitmap_size_(0),
      original_column_count_(0),
      original_null_bitmap_size_(0),
      null_mask_(0),
      null_offset_(0),
      data_size_(0)
{}

ColumnSchema::ColumnSchema(const ColumnSchema &column_schema)
    : type_(column_schema.type_),
      attr_(column_schema.attr_),
      column_count_(column_schema.column_count_),
      null_bitmap_size_(column_schema.null_bitmap_size_),
      original_column_count_(column_schema.original_column_count_),
      original_null_bitmap_size_(column_schema.original_null_bitmap_size_),
      null_mask_(column_schema.null_mask_),
      null_offset_(column_schema.null_offset_),
      data_size_(column_schema.data_size_)
{}

ColumnSchema &ColumnSchema::operator=(const ColumnSchema &column_schema)
{
  type_ = column_schema.type_;
  attr_ = column_schema.attr_;
  column_count_ = column_schema.column_count_;
  null_bitmap_size_ = column_schema.null_bitmap_size_;
  original_column_count_ = column_schema.original_column_count_;
  original_null_bitmap_size_ = column_schema.original_null_bitmap_size_;
  null_mask_ = column_schema.null_mask_;
  null_offset_ = column_schema.null_offset_;
  data_size_ = column_schema.data_size_;

  return *this;
}

ColumnSchema::~ColumnSchema()
{}

void ColumnSchema::reset()
{
  type_ = INVALID_COLUMN_TYPE;
  attr_ = 0;
  column_count_ = 0;
  null_bitmap_size_ = 0;
  original_column_count_ = 0;
  original_null_bitmap_size_ = 0;
  null_mask_ = 0;
  null_offset_ = 0;
  data_size_ = 0;
}

bool ColumnSchema::is_valid() const
{
  bool res = (type_ > INVALID_COLUMN_TYPE) && (type_ < MAX_COLUMN_TYPE);

  if (res && is_data_column(type_)) {
    res = res && ((is_nullable() && null_mask_ != 0) ||
        (!is_nullable() && (0 == null_offset_) && (0 == null_mask_)));
  }

  if (res && (RECORD_HEADER == type_)) {
    res = res && (column_count_ > 0) && (null_bitmap_size_ >= 0);
    res = res && ((is_instant() && original_column_count_ > 0 && column_count_ > original_column_count_) ||
                  (!is_instant() && (0 == original_null_bitmap_size_) && (0 == original_column_count_)));
  }

  return res;
}

int ColumnSchema::serialize(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = Status::kOk;
  int32_t header_size = get_serialize_size();
  int32_t header_version = COLUMN_SCHEMA_VERSION;
  int8_t type_value = static_cast<int8_t>(type_);

  if (IS_NULL(buf) || UNLIKELY(buf_len <= 0) || UNLIKELY(pos >= buf_len)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, header_size))) {
    SE_LOG(WARN, "fail to encode header size", K(ret), K(header_size));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, header_version))) {
    SE_LOG(WARN, "fail to encode header version", K(ret), K(header_version));
  } else if (FAILED(util::encode_fixed_int8(buf, buf_len, pos, type_value))) {
    SE_LOG(WARN, "fail to encode type", K(ret), KE_(type));
  } else if (FAILED(util::encode_fixed_int8(buf, buf_len, pos, attr_))) {
    SE_LOG(WARN, "fail to encode attr", K(ret), K_(attr));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, column_count_))) {
    SE_LOG(WARN, "fail to encode column count", K(ret), K_(column_count));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, null_bitmap_size_))) {
    SE_LOG(WARN, "fail to encode null bitmap size", K(ret), K_(null_bitmap_size));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, original_column_count_))) {
    SE_LOG(WARN, "fail to encode origin column count", K(ret), K_(original_column_count));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, original_null_bitmap_size_))) {
    SE_LOG(WARN, "fail to encode original null bitmap size", K(ret), K_(original_null_bitmap_size));
  } else if (FAILED(util::encode_fixed_int8(buf, buf_len, pos, null_mask_))) {
    SE_LOG(WARN, "fail to encode null mask", K(ret), K_(null_mask));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, null_offset_))) {
    SE_LOG(WARN, "fail to encode null offset", K(ret), K_(null_offset));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, data_size_))) {
    SE_LOG(WARN, "fail to encode data size", K(ret), K_(data_size));
  } else {
    // succeed
  }

  return ret;
}

int ColumnSchema::deserialize(const char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = Status::kOk;
  int32_t header_size = 0;
  int32_t header_version = 0;
  int8_t type_value = 0;

  if (IS_NULL(buf) || UNLIKELY(buf_len <= 0) || UNLIKELY(pos >= buf_len)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, header_size))) {
    SE_LOG(WARN, "fail to decode header size", K(ret), K(header_size));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, header_version))) {
    SE_LOG(WARN, "fail to decode header version", K(ret), K(header_version));
  } else if (FAILED(util::decode_fixed_int8(buf, buf_len, pos, type_value))) {
    SE_LOG(WARN, "fail to decode type", K(ret), KE_(type));
  } else if (FAILED(util::decode_fixed_int8(buf, buf_len, pos, attr_))) {
    SE_LOG(WARN, "fail to decode attr", K(ret), K_(attr));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, column_count_))) {
    SE_LOG(WARN, "fail to decode column count", K(ret), K_(column_count));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, null_bitmap_size_))) {
    SE_LOG(WARN, "fail to decode null bitmap size", K(ret), K_(null_bitmap_size));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, original_column_count_))) {
    SE_LOG(WARN, "fail to decode origin column count", K(ret), K_(original_column_count));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, original_null_bitmap_size_))) {
    SE_LOG(WARN, "fail to decode original null bitmap size", K(ret), K_(original_null_bitmap_size));
  } else if (FAILED(util::decode_fixed_int8(buf, buf_len, pos, null_mask_))) {
    SE_LOG(WARN, "fail to decode null mask", K(ret), K_(null_mask));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, null_offset_))) {
    SE_LOG(WARN, "fail to decode null offset", K(ret), K_(null_offset));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, data_size_))) {
    SE_LOG(WARN, "fail to decode data size", K(ret), K_(data_size));
  } else {
    type_ = static_cast<ColumnType>(type_value);
  }

  return ret;
}
int64_t ColumnSchema::get_serialize_size() const
{
  // header size and header version
  int64_t size = sizeof(int32_t) + sizeof(int32_t);
  size += sizeof(int8_t); // type_
  size += sizeof(attr_); // attr_
  size += sizeof(column_count_); // column_count_
  size += sizeof(null_bitmap_size_); // null_bitmap_size_
  size += sizeof(original_column_count_); // original_column_count_
  size += sizeof(original_null_bitmap_size_); // original_null_bitmap_size_
  size += sizeof(null_mask_); // null_mask_
  size += sizeof(null_offset_); // null_offset_
  size += sizeof(data_size_); // data_size_

  return size;
}

DEFINE_TO_STRING(ColumnSchema, KVE_(type), KV_(attr), KV_(column_count),
    KV_(null_bitmap_size), KV_(original_null_bitmap_size), KV_(original_column_count),
    KV_(original_null_bitmap_size), KV_(null_mask), KV_(null_offset), KV_(data_size))

} // namespace schema
} // namespace smartengine