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

#include "storage/storage_meta_struct.h"
#include "logger/log_module.h"
#include "memory/modtype_define.h"
#include "storage/storage_common.h"
#include "table/extent_struct.h"
#include "table/table_properties.h"
#include "util/to_string.h"

namespace smartengine
{
using namespace util;
namespace storage
{
SubTableMeta::SubTableMeta()
    : table_schema_(),
      table_space_id_(0),
      recovery_point_()
{
}

SubTableMeta::~SubTableMeta()
{
}

void SubTableMeta::reset()
{
  table_schema_.reset();
  table_space_id_ = 0;
  recovery_point_.reset();
}

bool SubTableMeta::is_valid() const
{
  return table_schema_.is_valid() &&
         table_space_id_ >= 0 &&
         recovery_point_.is_valid();
}


DEFINE_COMPACTIPLE_SERIALIZATION(SubTableMeta, table_schema_, table_space_id_, recovery_point_)

DEFINE_TO_STRING(SubTableMeta, KV_(table_schema), KV_(table_space_id), KV_(recovery_point))

ExtentMeta::ExtentMeta()
    : attr_(0),
      smallest_key_(),
      largest_key_(),
      extent_id_(),
      smallest_seqno_(0),
      largest_seqno_(0),
      refs_(0),
      raw_data_size_(0),
      data_size_(0),
      num_data_blocks_(0),
      num_entries_(0),
      table_space_id_(0),
      extent_space_type_(FILE_EXTENT_SPACE),
      index_block_handle_(),
      table_schema_(),
      prefix_(),
      object_size_(0),
      object_sha256_()
{
}

ExtentMeta::ExtentMeta(uint8_t attr,
                       const table::ExtentInfo &extent_info,
                       const schema::TableSchema &table_schema,
                       const std::string &prefix)
    : attr_(attr),
      smallest_key_(extent_info.smallest_key_),
      largest_key_(extent_info.largest_key_),
      extent_id_(extent_info.extent_id_),
      smallest_seqno_(extent_info.smallest_seq_),
      largest_seqno_(extent_info.largest_seq_),
      refs_(0),
      raw_data_size_(extent_info.raw_data_size_),
      data_size_(extent_info.data_size_),
      num_data_blocks_(extent_info.data_block_count_),
      num_entries_(extent_info.row_count_),
      num_deletes_(extent_info.delete_row_count_),
      table_space_id_(extent_info.table_space_id_),
      extent_space_type_(extent_info.extent_space_type_),
      index_block_handle_(extent_info.index_block_handle_),
      table_schema_(table_schema),
      prefix_(prefix),
      object_size_(0),
      object_sha256_()
{}

ExtentMeta::ExtentMeta(const ExtentMeta &extent_meta)
    : attr_(extent_meta.attr_),
      smallest_key_(extent_meta.smallest_key_),
      largest_key_(extent_meta.largest_key_),
      extent_id_(extent_meta.extent_id_),
      smallest_seqno_(extent_meta.smallest_seqno_),
      largest_seqno_(extent_meta.largest_seqno_),
      refs_(0),
      raw_data_size_(extent_meta.raw_data_size_),
      data_size_(extent_meta.data_size_),
      num_data_blocks_(extent_meta.num_data_blocks_),
      num_entries_(extent_meta.num_entries_),
      num_deletes_(extent_meta.num_deletes_),
      table_space_id_(extent_meta.table_space_id_),
      extent_space_type_(extent_meta.extent_space_type_),
      index_block_handle_(extent_meta.index_block_handle_),
      table_schema_(extent_meta.table_schema_),
      prefix_(extent_meta.prefix_),
      object_size_(extent_meta.object_size_),
      object_sha256_(extent_meta.object_sha256_)
{
}

ExtentMeta::~ExtentMeta()
{
}

ExtentMeta& ExtentMeta::operator=(const ExtentMeta &extent_meta)
{
  attr_ = extent_meta.attr_;
  smallest_key_ = extent_meta.smallest_key_;
  largest_key_ = extent_meta.largest_key_;
  extent_id_ = extent_meta.extent_id_;
  smallest_seqno_ = extent_meta.smallest_seqno_;
  largest_seqno_ = extent_meta.largest_seqno_;
  raw_data_size_ = extent_meta.raw_data_size_;
  data_size_ = extent_meta.data_size_;
  num_data_blocks_ = extent_meta.num_data_blocks_;
  num_entries_ = extent_meta.num_entries_;
  num_deletes_ = extent_meta.num_deletes_;
  table_space_id_ = extent_meta.table_space_id_;
  extent_space_type_ = extent_meta.extent_space_type_;
  index_block_handle_ = extent_meta.index_block_handle_;
  table_schema_ = extent_meta.table_schema_;
  prefix_ = extent_meta.prefix_;
  object_size_ = extent_meta.object_size_;
  object_sha256_ = extent_meta.object_sha256_;

  return *this;
}

void ExtentMeta::reset()
{
  attr_ = 0;
  smallest_key_.Clear();
  largest_key_.Clear();
  extent_id_.reset();
  smallest_seqno_ = 0;
  largest_seqno_ = 0;
  refs_ = 0;
  raw_data_size_ = 0;
  data_size_ = 0;
  num_data_blocks_ = 0;
  num_entries_ = 0;
  table_space_id_ = 0;
  extent_space_type_ = FILE_EXTENT_SPACE;
  index_block_handle_.reset();
  table_schema_.reset();
  prefix_.clear();
  object_size_ = 0;
  object_sha256_.clear();
}

int ExtentMeta::deep_copy(ExtentMeta *&extent_meta) const
{
  int ret = common::Status::kOk;
  extent_meta = nullptr;
  char *tmp_buf = nullptr;
  int64_t size = 0;

  size = get_deep_copy_size();
  if (nullptr == (tmp_buf = reinterpret_cast<char *>(base_malloc(size, memory::ModId::kExtentSpaceMgr)))) {
    ret = common::Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for tmp buf", K(ret), K(size));
  } else if (nullptr == (extent_meta = new (tmp_buf) ExtentMeta())) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to constructe ExtentMeta", K(ret));
  } else {
    extent_meta->attr_ = attr_;
    extent_meta->smallest_key_ = smallest_key_;
    extent_meta->largest_key_ = largest_key_;
    extent_meta->extent_id_ = extent_id_;
    extent_meta->smallest_seqno_ = smallest_seqno_;
    extent_meta->largest_seqno_ = largest_seqno_;
    extent_meta->refs_ = refs_;
    extent_meta->raw_data_size_ = raw_data_size_;
    extent_meta->data_size_ = data_size_;
    extent_meta->num_data_blocks_ = num_data_blocks_;
    extent_meta->num_entries_ = num_entries_;
    extent_meta->num_deletes_ = num_deletes_;
    extent_meta->table_space_id_ = table_space_id_;
    extent_meta->extent_space_type_ = extent_space_type_;
    extent_meta->index_block_handle_ = index_block_handle_;
    extent_meta->table_schema_ = table_schema_;
    extent_meta->prefix_ = prefix_;
    extent_meta->object_size_ = object_size_;
    extent_meta->object_sha256_ = object_sha256_;
  }

  return ret;
}

int ExtentMeta::deep_copy(memory::SimpleAllocator &allocator, ExtentMeta *&extent_meta) const
{
  int ret = common::Status::kOk;
  char *buf = nullptr;
  int64_t size = 0;
  extent_meta = nullptr;

  size = get_deep_copy_size();
  if (nullptr == (buf = reinterpret_cast<char *>(allocator.alloc(size)))) {
    ret = common::Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for buf", K(ret));
  } else if (nullptr == (extent_meta = new (buf) ExtentMeta())) {
    ret = common::Status::kCorruption;
    SE_LOG(WARN, "fail to constructor extent meta", K(ret));
  } else {
    extent_meta->attr_ = attr_;
    extent_meta->smallest_key_ = smallest_key_;
    extent_meta->largest_key_ = largest_key_;
    extent_meta->extent_id_ = extent_id_;
    extent_meta->smallest_seqno_ = smallest_seqno_;
    extent_meta->largest_seqno_ = largest_seqno_;
    extent_meta->refs_ = refs_;
    extent_meta->raw_data_size_ = raw_data_size_;
    extent_meta->data_size_ = data_size_;
    extent_meta->num_data_blocks_ = num_data_blocks_;
    extent_meta->num_entries_ = num_entries_;
    extent_meta->num_deletes_ = num_deletes_;
    extent_meta->table_space_id_ = table_space_id_;
    extent_meta->extent_space_type_ = extent_space_type_;
    extent_meta->index_block_handle_ = index_block_handle_;
    extent_meta->table_schema_ = table_schema_;
    extent_meta->prefix_ = prefix_;
    extent_meta->object_size_ = object_size_;
    extent_meta->object_sha256_ = object_sha256_;
  }
  return ret;
}

int64_t ExtentMeta::get_deep_copy_size() const
{
  return sizeof(ExtentMeta);
}
DEFINE_COMPACTIPLE_SERIALIZATION(ExtentMeta, attr_, smallest_key_, largest_key_,
    extent_id_, smallest_seqno_, largest_seqno_, raw_data_size_, data_size_, num_data_blocks_,
    num_entries_, num_deletes_, table_space_id_, extent_space_type_, index_block_handle_,
    table_schema_, prefix_, object_size_, object_sha256_)

DEFINE_TO_STRING(ExtentMeta, KV_(attr), KV_(smallest_key), KV_(largest_key), KV_(extent_id),
    KV_(smallest_seqno), KV_(largest_seqno), KV_(refs), KV_(raw_data_size), KV_(data_size),
    KV_(num_data_blocks), KV_(num_entries), KV_(num_deletes), KV_(table_space_id),
    KV_(extent_space_type), KV_(index_block_handle), KV_(table_schema), KV_(prefix),
    KV_(object_size), KV_(object_sha256))

} //namespace storage
} //namespace smartengine
