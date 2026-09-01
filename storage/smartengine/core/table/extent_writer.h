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

#include <unordered_map>

#include "schema/table_schema.h"
#include "table/bloom_filter.h"
#include "table/extent_struct.h"
#include "table/index_block_writer.h"
#include "table/row_block_writer.h"
#include "util/compress/compressor_helper.h"
#include "util/data_buffer.h"

namespace smartengine
{
namespace common
{
class Slice;
} // namespace common
namespace storage
{
struct ChangeInfo;
struct ExtentMeta;
class IOExtent;
class WriteExtentJob;
} // namespace storage

namespace table
{
struct LargeObject;
struct RowBlock;

struct ExtentWriterArgs
{
public:
  std::string cluster_id_;
  int64_t table_space_id_;
  int64_t block_restart_interval_;
  int32_t extent_space_type_;
  schema::TableSchema table_schema_;
  const db::InternalKeyComparator *internal_key_comparator_;
  storage::LayerPosition output_position_;
  cache::Cache *block_cache_;
  cache::RowCache *row_cache_;
  common::CompressionType compress_type_;
  storage::ChangeInfo *change_info_;

  ExtentWriterArgs();
  ExtentWriterArgs(const std::string &cluster_id,
                   const int64_t table_space_id,
                   const int64_t block_restart_interval,
                   const int32_t extent_space_type_,
                   const schema::TableSchema &table_schema,
                   const db::InternalKeyComparator *internal_key_comparator,
                   const storage::LayerPosition &output_position,
                   cache::Cache *block_cache,
                   cache::RowCache *row_cache,
                   const common::CompressionType compress_type,
                   storage::ChangeInfo *change_info);
  ~ExtentWriterArgs();

  bool is_valid() const;
  DECLARE_TO_STRING()
};

class ExtentWriter
{
public:
  ExtentWriter();
  virtual ~ExtentWriter();

  int init(const ExtentWriterArgs &args);
  void destroy();
  int append_row(const common::Slice &key, const common::Slice &value);
  int append_block(const common::Slice &block, const common::Slice &block_index, const common::Slice &last_key);
  int finish(std::vector<ExtentInfo> *extent_infos);
  int rollback();
  int mark_write_extent_finish(const storage::ExtentId &extent_id);
  void report_write_extent_error(int ret);
  void set_migrate_flag() { migrate_flag_ = true; }
  bool is_empty() const;

#ifndef NDEBUG
  int test_force_flush_data_block();
#endif
private:
  int init_data_block_writer(const ExtentWriterArgs &args);
  int init_bloom_filter_writer(const bool use_bloom_filter);
  void reuse_bloom_filter_writer();
  int append_normal_row(const common::Slice &key, const common::Slice &value);
  int append_large_row(const common::Slice &key, const common::Slice &value);
  int prepare_append_row(const common::Slice &key, const common::Slice &value);
  int inner_append_row(const common::Slice &key, const common::Slice &value);
  int prepare_append_block(const common::Slice &block,
                           const BlockInfo &block_info,
                           const common::Slice &last_key);
  int inner_append_block(const common::Slice &block,
                         const BlockInfo &block_info,
                         const common::Slice &last_key);
  int add_to_bloom_filter(const common::Slice &key);
  int build_bloom_filter(BlockInfo &block_info);
  int check_key_order(const common::Slice &key);
  int need_switch_block(const common::Slice &key, const common::Slice &value, bool &need_switch) const;
  int need_switch_extent(const common::Slice &key, const BlockInfo &block_info, bool &need_switch);
  bool is_current_extent_empty() const;
  int write_data_block();
  int write_index_block();
  int write_footer();
  int write_extent();
  int flush_extent();
  int update_block_stats(const common::Slice &key);
  int convert_to_large_object_format(const common::Slice &key,
                                     const common::Slice &value,
                                     LargeObject &large_object);
  int convert_to_large_object_key(const common::Slice &key, LargeObject &large_object);
  int convert_to_large_object_value(const common::Slice &value, LargeObject &large_object);
  int recycle_large_object_extent(LargeObject &large_object);
  int write_large_object(const LargeObject &large_object);
  int build_large_object_extent_meta(const common::Slice &lob_key,
                                     const storage::ExtentId &extent_id,
                                     const int64_t data_size,
                                     const std::string &prefix,
                                     storage::ExtentMeta &extent_meta);
  int write_extent_meta(const storage::ExtentMeta &extent_meta, bool is_large_object_extent);
  int build_allocation_prefix(std::string &prefix) const;
  int write_remote_extent_body(storage::IOExtent *extent,
                               const common::Slice &data,
                               storage::ExtentMeta &extent_meta);
  int remember_remote_extent_prefix(const storage::ExtentId &extent_id,
                                    const std::string &prefix);
  int get_recycle_prefix(const storage::ExtentId &extent_id,
                         std::string &prefix) const;
  int collect_migrating_block(const common::Slice &block,
                              const BlockHandle &block_handle,
                              const common::CompressionType &compress_type);
  int migrate_block_cache(const storage::IOExtent *extent);
  void calculate_block_checksum(const common::Slice &block, uint32_t &checksum);
  int submit_write_extent_request(storage::IOExtent *extent, const common::Slice &data);
  int add_pending_extent(const storage::ExtentId &extent_id);
  int erase_pending_extent(const storage::ExtentId &extent_id);
  int wait_pending_extents_finish();

private:
  static const int64_t SAFE_SPACE_SIZE = 1024; // size of safe space between last valid block and footer

private:
  bool is_inited_;
  int64_t index_id_;
  int64_t table_space_id_;
  int32_t extent_space_type_;
  schema::TableSchema table_schema_;
  common::CompressionType compress_type_;
  storage::LayerPosition layer_position_;
  const db::InternalKeyComparator *internal_key_comparator_;
  cache::Cache *block_cache_;
  cache::RowCache *row_cache_;
  util::CompressorHelper data_block_compressor_;
  util::CompressorHelper index_block_compressor_;
  std::string prefix_;
  std::unordered_map<int64_t, std::string> remote_extent_prefixes_;
  BlockInfo block_info_;
  ExtentInfo extent_info_;
  std::vector<ExtentInfo> writed_extent_infos_;
  std::string last_key_;
  util::BufferWriter buf_;
  Footer footer_;
  IndexBlockWriter index_block_writer_;
  BloomFilterWriter *bloom_filter_writer_;
  BlockWriter *data_block_writer_;
  storage::ChangeInfo *change_info_;
  bool migrate_flag_;
  std::unordered_map<uint32_t, RowBlock *> migrating_blocks_;
  std::vector<BlockHandle> migrate_block_handles_;
  std::mutex pending_extent_mutex_;
  std::unordered_set<int64_t> pending_extents_;
  int write_extent_ret_;
};

} //namespace table
} //namespace smartengine
