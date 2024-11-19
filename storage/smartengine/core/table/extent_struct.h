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

#include "db/dbformat.h"
#include "table/block_struct.h"

namespace smartengine
{
namespace table
{
struct Footer
{
  static const int64_t FOOTER_VERSION = 1;
  // TODO (Zhao Dongsheng): 4KB?
  static const int64_t MAX_FOOTER_SIZE = 1024; // 1KB
  static const int64_t FOOTER_MAGIC_NUMBER = 0x437623A5C90E8F1A;

  int64_t magic_numer_;
  int8_t checksum_type_;
  storage::ExtentId extent_id_;
  BlockHandle index_block_handle_;

  Footer();
  Footer(const Footer &footer);
  ~Footer();
  Footer &operator=(const Footer &footer);

  void reset();
  bool is_valid() const;
  inline void set_checksum_type(const int8_t checksum_type) { checksum_type_ = checksum_type; }
  inline void set_extent_id(const storage::ExtentId &extent_id) { extent_id_ = extent_id; }
  inline storage::ExtentId get_extent_id() const { return extent_id_; }
  inline void set_index_block_handle(const BlockHandle &handle) { index_block_handle_ = handle; }
  inline BlockHandle get_index_block_handle() const { return index_block_handle_; }
  static int64_t get_max_serialize_size() { return MAX_FOOTER_SIZE; }

  DECLARE_SERIALIZATION()
  DECLARE_TO_STRING()
};

struct ExtentInfo
{
  int64_t table_space_id_;
  int32_t extent_space_type_;
  storage::ExtentId extent_id_;
  db::InternalKey smallest_key_;
  db::InternalKey largest_key_;
  common::SequenceNumber smallest_seq_;
  common::SequenceNumber largest_seq_;
  int32_t raw_data_size_;
  int32_t data_size_;
  int32_t data_block_count_;
  int32_t row_count_;
  int32_t delete_row_count_;
  BlockHandle index_block_handle_;

  ExtentInfo();
  ExtentInfo(const ExtentInfo &extent_info);
  ~ExtentInfo();
  ExtentInfo &operator=(const ExtentInfo &extent_info);

  void reset();
  void update(const common::Slice &largest_key, const BlockInfo &block_info);

  DECLARE_TO_STRING()
};

} // namespace table
} // namespace smartengine