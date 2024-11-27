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

#pragma once

#include "table/row_block_iterator.h"
#include "util/slice.h"

namespace smartengine
{
namespace table
{
struct BlockInfo;

class IndexBlockReader
{
public:
  IndexBlockReader();
  ~IndexBlockReader();

  int init(RowBlock *block, const db::InternalKeyComparator *internal_key_comparator);
  int seek_to_first();
  int seek(const common::Slice &key);
  int next();
  common::Slice key() const;
  common::Slice value() const;
  int get_key(common::Slice &key) const;
  int get_value(bool only_critical_info, BlockInfo &block_info) const;
  bool valid() const;

private:
  bool is_inited_;
  RowBlock *block_;
  RowBlockIterator block_iter_;
};

} // namespace table
} // namespace smartengine