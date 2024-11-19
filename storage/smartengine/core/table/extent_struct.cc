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

#include "table/extent_struct.h"

namespace smartengine
{
using namespace common;

namespace table
{

Footer::Footer()
    : magic_numer_(FOOTER_MAGIC_NUMBER),
      checksum_type_(kCRC32c),
      extent_id_(),
      index_block_handle_()
{}

Footer::Footer(const Footer &footer)
    : magic_numer_(footer.magic_numer_),
      checksum_type_(footer.checksum_type_),
      extent_id_(footer.extent_id_),
      index_block_handle_(footer.index_block_handle_)
{}

Footer::~Footer() {}

Footer &Footer::operator=(const Footer &footer)
{
  magic_numer_ = footer.magic_numer_;
  checksum_type_ = footer.checksum_type_;
  extent_id_ = footer.extent_id_;
  index_block_handle_ = footer.index_block_handle_;

  return *this;
}

void Footer::reset()
{
  magic_numer_ = FOOTER_MAGIC_NUMBER;
  checksum_type_ = kCRC32c;
  extent_id_.reset();
  index_block_handle_.reset();
}

bool Footer::is_valid() const
{
  return FOOTER_MAGIC_NUMBER == magic_numer_ &&
         index_block_handle_.is_valid();
}

int Footer::serialize(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = Status::kOk;
  int32_t header_size = get_serialize_size();
  int32_t header_version = FOOTER_VERSION;

  if (IS_NULL(buf) || UNLIKELY(buf_len <= 0) || UNLIKELY(pos < 0) || UNLIKELY(pos >= buf_len)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, header_size))) {
    SE_LOG(WARN, "fail to encode header size", K(ret), K(header_size));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, header_version))) {
    SE_LOG(WARN, "fail to encode header version", K(ret));
  } else if (FAILED(util::encode_fixed_int64(buf, buf_len, pos, magic_numer_))) {
    SE_LOG(WARN, "fail to encode magic number", K(ret));
  } else if (FAILED(util::encode_fixed_int8(buf, buf_len, pos, checksum_type_))) {
    SE_LOG(WARN, "fail to encode checksum type", K(ret));
  } else if (FAILED(extent_id_.serialize(buf, buf_len, pos))) {
    SE_LOG(WARN, "fail to serialize extent id", K(ret));
  } else if (FAILED(index_block_handle_.serialize(buf, buf_len, pos))) {
    SE_LOG(WARN, "fail to serialize index block handle", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int Footer::deserialize(const char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = Status::kOk;
  int32_t header_size = 0;
  int32_t header_version = 0;

  if (IS_NULL(buf) || UNLIKELY(buf_len <= 0) || UNLIKELY(pos < 0) || UNLIKELY(pos >= buf_len)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, header_size))) {
    SE_LOG(WARN, "fail to decode header size", K(ret));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, header_version))) {
    SE_LOG(WARN, "fail to decode header version", K(ret));
  } else if (FAILED(util::decode_fixed_int64(buf, buf_len, pos, magic_numer_))) {
    SE_LOG(WARN, "fail to decode magic number", K(ret));
  } else if (FAILED(util::decode_fixed_int8(buf, buf_len, pos, checksum_type_))) {
    SE_LOG(WARN, "fail to decode checksum type", K(ret));
  } else if (FAILED(extent_id_.deserialize(buf, buf_len, pos))) {
    SE_LOG(WARN, "fail to deserialize extent id", K(ret));
  } else if (FAILED(index_block_handle_.deserialize(buf, buf_len, pos))) {
    SE_LOG(WARN, "fail to deserialize index block handle", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int64_t Footer::get_serialize_size() const
{
  // header size and header version
  int64_t size = sizeof(int32_t) + sizeof(int32_t);
  size += sizeof(magic_numer_);
  size += sizeof(checksum_type_);
  size += extent_id_.get_serialize_size();
  size += index_block_handle_.get_serialize_size();
  
  return size;
}

DEFINE_TO_STRING(Footer, KV_(magic_numer), KV_(checksum_type), KV_(extent_id), KV_(index_block_handle))


ExtentInfo::ExtentInfo()
    : table_space_id_(0),
      extent_space_type_(storage::FILE_EXTENT_SPACE),
      extent_id_(),
      smallest_key_(),
      largest_key_(),
      smallest_seq_(common::kMaxSequenceNumber),
      largest_seq_(0),
      raw_data_size_(0),
      data_size_(0),
      data_block_count_(0),
      row_count_(0),
      delete_row_count_(0),
      index_block_handle_()
{}

ExtentInfo::ExtentInfo(const ExtentInfo &extent_info)
    : table_space_id_(extent_info.table_space_id_),
      extent_space_type_(extent_info.extent_space_type_),
      extent_id_(extent_info.extent_id_),
      smallest_key_(extent_info.smallest_key_),
      largest_key_(extent_info.largest_key_),
      smallest_seq_(extent_info.smallest_seq_),
      largest_seq_(extent_info.largest_seq_),
      raw_data_size_(extent_info.raw_data_size_),
      data_size_(extent_info.data_size_),
      data_block_count_(extent_info.data_block_count_),
      row_count_(extent_info.row_count_),
      delete_row_count_(extent_info.delete_row_count_),
      index_block_handle_(extent_info.index_block_handle_)
{}

ExtentInfo::~ExtentInfo() {}

ExtentInfo &ExtentInfo::operator=(const ExtentInfo &extent_info)
{
  table_space_id_ = extent_info.table_space_id_;
  extent_space_type_ = extent_info.extent_space_type_;
  extent_id_ = extent_info.extent_id_;
  smallest_key_ = extent_info.smallest_key_;
  largest_key_ = extent_info.largest_key_;
  smallest_seq_ = extent_info.smallest_seq_;
  largest_seq_ = extent_info.largest_seq_;
  raw_data_size_ = extent_info.raw_data_size_;
  data_size_ = extent_info.data_size_;
  data_block_count_ = extent_info.data_block_count_;
  row_count_ = extent_info.row_count_;
  delete_row_count_ = extent_info.delete_row_count_;
  index_block_handle_ = extent_info.index_block_handle_;

  return *this;
}

void ExtentInfo::reset()
{
  table_space_id_ = 0;
  extent_space_type_ = storage::FILE_EXTENT_SPACE;
  extent_id_.reset();
  smallest_key_.Clear();
  largest_key_.Clear();
  smallest_seq_ = common::kMaxSequenceNumber;
  largest_seq_ = 0;
  raw_data_size_ = 0;
  data_size_ = 0;
  data_block_count_ = 0;
  row_count_ = 0;
  delete_row_count_ = 0;
  index_block_handle_.reset();
}

void ExtentInfo::update(const common::Slice &largest_key, const BlockInfo &block_info)
{
  if (0 == smallest_key_.size()) {
    smallest_key_.DecodeFrom(common::Slice(block_info.get_first_key()));
  }
  largest_key_.DecodeFrom(largest_key);
  smallest_seq_ = std::min(smallest_seq_, block_info.get_smallest_seq());
  largest_seq_ = std::max(largest_seq_, block_info.get_largest_seq());
  ++data_block_count_;
  row_count_ += block_info.get_row_count();
  delete_row_count_ += block_info.get_delete_row_count() + block_info.get_single_delete_row_count(); 
}

DEFINE_TO_STRING(ExtentInfo, KV_(table_space_id), KV_(extent_space_type), KV_(extent_id),
      KV_(smallest_key), KV_(largest_key), KV_(smallest_seq), KV_(largest_seq), KV_(raw_data_size),
      KV_(data_size), KV_(data_block_count), KV_(row_count), KV_(delete_row_count), KV_(index_block_handle))

} // namespace table
} // namespace smartengine