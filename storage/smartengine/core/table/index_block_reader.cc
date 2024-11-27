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

#include "table/index_block_reader.h"
#include "logger/log_module.h"
#include "table/block_struct.h"
#include "table/row_block.h"

namespace smartengine
{
using namespace common;
using namespace db;

namespace table
{
IndexBlockReader::IndexBlockReader()
    : is_inited_(false),
      block_(nullptr),
      block_iter_()
{}

IndexBlockReader::~IndexBlockReader() {}

int IndexBlockReader::init(RowBlock *block, const InternalKeyComparator *internal_key_comparator)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "IndexBlockReader has been inited", K(ret));
  } else if (IS_NULL(block) || IS_NULL(internal_key_comparator)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(block), KP(internal_key_comparator));
  } else if (FAILED(block_iter_.setup(internal_key_comparator, block, true /*is_index_block*/))) {
    SE_LOG(WARN, "fail to setup index block iterator", K(ret));
  } else {
    block_ = block;
    is_inited_ = true;
  }

  return ret;
}

int IndexBlockReader::seek_to_first()
{
  assert(is_inited_);
  block_iter_.SeekToFirst();
  return Status::kOk;
}

int IndexBlockReader::seek(const Slice &key)
{
  assert(is_inited_);
  block_iter_.Seek(key);
  return Status::kOk;
}

int IndexBlockReader::next()
{
  assert(is_inited_);
  block_iter_.Next();
  return Status::kOk;
}

Slice IndexBlockReader::key() const
{
  return block_iter_.key();
}

Slice IndexBlockReader::value() const
{
  return block_iter_.value();
}

int IndexBlockReader::get_key(Slice &key) const
{
  assert(is_inited_);
  key = block_iter_.key();
  return Status::kOk;
}

int IndexBlockReader::get_value(bool only_critical_info, BlockInfo &block_info) const
{
  assert(is_inited_);
  int ret = Status::kOk;
  Slice value = block_iter_.value();
  int64_t pos = 0;

  if (only_critical_info) {
    if (FAILED(block_info.deserialize_critical_info(value.data(), value.size(), pos))) {
      SE_LOG(WARN, "fail to fast deserialize block info", K(ret));
    }
  } else {
    if (FAILED(block_info.deserialize(value.data(), value.size(), pos))) {
      SE_LOG(WARN, "fail to deserialize block info", K(ret));
    }
  }
  se_assert(block_info.is_valid());

  return ret;
}

bool IndexBlockReader::valid() const
{
  assert(is_inited_);
  return block_iter_.Valid();
}

}  // namespace table
}  // namespace smartengine