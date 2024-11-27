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

#include "options/advanced_options.h"
#include "table/column_unit.h"
#include "util/serialization.h"
#include "util/to_string.h"
#include "util/types.h"

namespace smartengine
{
namespace storage
{
class IOExtent;
} // namespace storage
namespace util
{
struct AIOHandle;
} // namespace util
namespace table
{
struct RowBlock;

enum ChecksumType : int8_t
{
  kNoChecksum = 0x0,  // not yet supported. Will fail
  kCRC32c = 0x1,
  kxxHash = 0x2,
};

class BlockHandle
{
public:
  BlockHandle();
  BlockHandle(const BlockHandle &block_handle);
  BlockHandle &operator=(const BlockHandle &block_handle);
  ~BlockHandle();

  void reset();
  bool is_valid() const;
  inline void set_offset(int32_t offset) { offset_ = offset; }
  inline int32_t get_offset() const { return offset_; }
  inline void set_size(int32_t size) { size_ = size; }
  inline int32_t get_size() const { return size_; }
  inline void set_raw_size(int32_t raw_size) { raw_size_ = raw_size; } 
  inline int32_t get_raw_size() const { return raw_size_; }
  inline void set_checksum(uint32_t checksum) { checksum_ = checksum; }
  inline uint32_t get_checksum() const { return checksum_; }
  inline void set_compress_type(common::CompressionType compress_type) { compress_type_ = compress_type; }
  inline common::CompressionType get_compress_type() const { return compress_type_; }

  static const int64_t BLOCK_HANDLE_VERSION = 1;
  DECLARE_SERIALIZATION()
  DECLARE_TO_STRING()

private:
  int32_t offset_;
  int32_t size_;
  int32_t raw_size_;
  uint32_t checksum_;
  common::CompressionType compress_type_;
};

class BlockInfo
{
public:
  BlockInfo();
  BlockInfo(const BlockInfo &block_info);
  BlockInfo &operator=(const BlockInfo &block_info);
  ~BlockInfo();

  void reset();
  bool is_valid() const;
  inline void set_handle(BlockHandle handle) { critical_info_.set_handle(handle); }
  inline const BlockHandle &get_handle() const { return critical_info_.get_handle(); }
  inline BlockHandle &get_handle() { return critical_info_.get_handle(); }
  inline void set_columnar_format() { critical_info_.set_columnar_format(); }
  inline void set_row_format() { critical_info_.set_row_format(); }
  inline bool is_columnar_format() const { return critical_info_.is_columnar_format(); }
  inline void set_has_bloom_filter() { critical_info_.set_has_bloom_filter(); }
  inline void unset_has_bloom_filter() { critical_info_.unset_has_bloom_filter(); }
  inline bool has_bloom_filter() const { return critical_info_.has_bloom_filter(); }
  inline void set_per_key_bits(int32_t per_key_bits) { critical_info_.set_per_key_bits(per_key_bits); }
  inline int32_t get_per_key_bits() const { return critical_info_.get_per_key_bits(); }
  inline void set_probe_num(int32_t probe_num) { critical_info_.set_probe_num(probe_num); }
  inline int32_t get_probe_num() const { return critical_info_.get_probe_num(); }
  inline void set_bloom_filter(const common::Slice &bloom_filter) { critical_info_.set_bloom_filter(bloom_filter); }
  inline const common::Slice &get_bloom_filter() const { return critical_info_.get_bloom_filter(); }
  inline const std::vector<ColumnUnitInfo> &get_unit_infos() const { return critical_info_.get_unit_infos(); }
  inline std::vector<ColumnUnitInfo> &get_unit_infos() { return critical_info_.get_unit_infos(); }
  inline void set_first_key(const std::string &first_key) { first_key_ = first_key; }
  inline const std::string &get_first_key() const { return first_key_; }
  inline void inc_row_count() { critical_info_.inc_row_count(); }
  inline int32_t get_row_count() const { return critical_info_.get_row_count(); }
  inline void inc_delete_row_count() { ++delete_row_count_; }
  inline int32_t get_delete_row_count() const { return delete_row_count_; }
  inline void inc_single_delete_row_count() { ++single_delete_row_count_; }
  inline int32_t get_single_delete_row_count() const { return single_delete_row_count_; }
  inline int64_t get_delete_percent() const { return ((delete_row_count_ + single_delete_row_count_) * 100) / critical_info_.get_row_count(); }
  inline void set_smallest_seq(common::SequenceNumber smallest_seq) { smallest_seq_ = smallest_seq; }
  inline common::SequenceNumber get_smallest_seq() const { return smallest_seq_; }
  inline void set_largest_seq(common::SequenceNumber largest_seq) { largest_seq_ = largest_seq; }
  inline common::SequenceNumber get_largest_seq() const { return largest_seq_; }
  inline int64_t get_row_format_raw_size() const { return critical_info_.get_row_format_raw_size();}
  int deserialize_critical_info(const char *buf, int64_t buf_len, int64_t &pos);

  static const int64_t BLOCK_INFO_VERSION = 1;
  DECLARE_SERIALIZATION()
  DECLARE_TO_STRING()

private:
  class ReadCriticalInfo
  {
  public:
    ReadCriticalInfo();
    ReadCriticalInfo(const ReadCriticalInfo &other);
    ReadCriticalInfo &operator=(const ReadCriticalInfo &other);
    ~ReadCriticalInfo();

    void reset();
    bool is_valid() const;
    inline void set_handle(BlockHandle handle) { handle_ = handle; }
    inline const BlockHandle &get_handle() const { return handle_; }
    inline BlockHandle &get_handle() { return handle_; }
    inline void set_columnar_format() { attr_ |= FLAG_COLUMNAR_FORMAT; }
    inline void set_row_format() { attr_ &= (~FLAG_COLUMNAR_FORMAT); }
    inline bool is_columnar_format() const { return 0 != (attr_ & FLAG_COLUMNAR_FORMAT); }
    inline void set_has_bloom_filter() { attr_ |= FLAG_HAS_BLOOM_FILTER; }
    inline void unset_has_bloom_filter() { attr_ &= (~FLAG_HAS_BLOOM_FILTER); }
    inline bool has_bloom_filter() const { return 0 != (attr_ & FLAG_HAS_BLOOM_FILTER); }
    inline void inc_row_count() { ++row_count_; }
    inline void set_row_count(int32_t row_count) { row_count_ = row_count; }
    inline int32_t get_row_count() const { return row_count_; }
    inline void set_per_key_bits(int32_t per_key_bits) { per_key_bits_ = per_key_bits; }
    inline int32_t get_per_key_bits() const { return  per_key_bits_; }
    inline void set_probe_num(int32_t probe_num) { probe_num_ = probe_num; }
    inline int32_t get_probe_num() const { return probe_num_; }
    inline void set_bloom_filter(const common::Slice &bloom_filter)
    {
      bloom_filter_ = bloom_filter;
      set_has_bloom_filter();
    }
    inline const common::Slice &get_bloom_filter() const { return bloom_filter_; }
    inline const std::vector<ColumnUnitInfo> &get_unit_infos() const { return unit_infos_; }
    inline std::vector<ColumnUnitInfo> &get_unit_infos() { return unit_infos_; }
    int64_t get_row_format_raw_size() const;

    static const int32_t READ_CRITICAL_INFO_VERSION = 1;
    DECLARE_SERIALIZATION()
    DECLARE_AND_DEFINE_TO_STRING(KV_(handle), KV_(attr), KV_(per_key_bits), KV_(probe_num), KV_(unit_infos))
  private:
    static const int8_t FLAG_COLUMNAR_FORMAT = 0x01;
    static const int8_t FLAG_HAS_BLOOM_FILTER = 0x02;

  private:
    BlockHandle handle_;
    int8_t attr_;
    int32_t row_count_;
    int32_t per_key_bits_;
    int32_t probe_num_;
    common::Slice bloom_filter_;
    std::vector<ColumnUnitInfo> unit_infos_;
  };

private:
  bool only_critical_info_;
  ReadCriticalInfo critical_info_;
  std::string first_key_;
  int32_t delete_row_count_;
  int32_t single_delete_row_count_;
  common::SequenceNumber smallest_seq_;
  common::SequenceNumber largest_seq_;
};

} // namespace table
} // namespace smartengine