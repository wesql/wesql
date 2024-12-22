//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include "table/row_block.h"
#include "memory/base_malloc.h"
#include "util/coding.h"

namespace smartengine
{
using namespace common;
using namespace util;

namespace table
{

RowBlock::RowBlock(const char *data, const int64_t data_size, const CompressionType compress_type)
    : data_(data),
      size_(data_size),
      compress_type_(compress_type)
{}

RowBlock::~RowBlock()
{
  destroy();
}

void RowBlock::destroy()
{
  if (IS_NOTNULL(data_)) {
    memory::base_free(const_cast<char *>(data_));
    data_ = 0;
    size_ = 0;
    compress_type_ = common::kNoCompression;
  }
}

bool RowBlock::is_valid() const
{
  // The restats(restart array) in the block contains at least two elements with
  // type uint32_t, one is block restart, and the other is restart count.
  return IS_NOTNULL(data_) && UNLIKELY(size_ > (2 * RESTARTS_ELEMENT_SIZE));
}

int64_t RowBlock::usable_size() const
{
  return sizeof(*this) + size_;
}

void RowBlock::restarts_info(uint32_t &restarts_count, uint32_t &restarts_offset) const
{
  uint32_t restarts_size = 0;
  restarts_count = DecodeFixed32(data_ + size_ - RESTARTS_ELEMENT_SIZE);
  se_assert(restarts_count > 0);
  restarts_size = (1 + restarts_count) * RESTARTS_ELEMENT_SIZE;
  se_assert(size_ > restarts_size);
  restarts_offset = size_ - restarts_size;
}

DEFINE_TO_STRING(RowBlock, KV_(data), KV_(size), KVE_(compress_type))

}  // namespace table
}  // namespace smartengine
