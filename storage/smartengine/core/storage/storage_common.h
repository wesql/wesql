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

#ifndef SMARTENGINE_INCLUDE_STORAGE_COMMON_H_
#define SMARTENGINE_INCLUDE_STORAGE_COMMON_H_
#include <unordered_map>
#include <set>

#include "memory/stl_adapt_allocator.h"
#include "objstore.h"
#include "options/options.h"
#include "util/autovector.h"
#include "util/to_string.h"
#include "util/se_constants.h"
#include "util/serialization.h"

namespace objstore {
class ObjectStore;
}  // namespace objstore

namespace smartengine
{
namespace storage
{
struct ExtentPosition;

// hash table of extent id to meta
typedef std::unordered_map<int64_t, ExtentPosition, std::hash<int64_t>, std::equal_to<int64_t>,
      memory::stl_adapt_allocator<std::pair<const int64_t, ExtentPosition>, memory::ModId::kStorageMgr>> ExtentPositionMap;

// hash table of sst file number to used extent ids
typedef std::unordered_map<int32_t, util::autovector<int32_t>, std::hash<int32_t>, std::equal_to<int32_t>,
      memory::stl_adapt_allocator<std::pair<const int32_t, util::autovector<int32_t>>, memory::ModId::kStorageMgr>> SSTExtentIdMap;

struct ExtentId {
  int32_t file_number;  // file number
  int32_t offset;       // extent offset in file
  ExtentId() : file_number(0), offset(0) {}
  ExtentId(const ExtentId &r) {
    file_number = r.file_number;
    offset = r.offset;
  }
  ExtentId(const int64_t eid)
  {
    set(eid);
  }

  ExtentId(int32_t fn, int32_t off) : file_number(fn), offset(off) {}

  ExtentId &operator=(const ExtentId &extent_id)
  {
    this->file_number = extent_id.file_number;
    this->offset = extent_id.offset;
    return *this;
  }

  inline int64_t id() const {
    int64_t hi = file_number;
    int64_t lo = offset;
    int64_t value = hi << 32 | lo;
    return value;
  }

  inline void set(int64_t id)
  {
    file_number = static_cast<int32_t>(id >> 32);
    offset = static_cast<int32_t>(id);
  }

  bool is_equal(const ExtentId &eid) const {
    return (this->file_number == eid.file_number) &&
           (this->offset == eid.offset);
  }

  void reset() {
    file_number = 0;
    offset = 0;
  }

  DECLARE_SERIALIZATION()
  DECLARE_TO_STRING()
};

struct LayerPosition
{
public:
  static const int64_t LAYER_POSITION_VERSION = 0;
  static const int32_t INVISIBLE_LAYER_INDEX;
  static const int32_t NEW_GENERATE_LAYER_INDEX;
public:
  int32_t level_;
  int32_t layer_index_;

  LayerPosition() : level_(0), layer_index_(0)
  {
  }
  explicit LayerPosition(const int32_t level)
      : level_(level),
        layer_index_(0)
  {
  }
  LayerPosition(const int32_t level, const int32_t layer_index)
      : level_(level),
        layer_index_(layer_index)
  {
  }
  explicit LayerPosition(const int64_t &position)
  {
    level_ = position >> 32;
    layer_index_ = static_cast<int32_t>(position);
  }
  LayerPosition(const LayerPosition &layer_position)
      : level_(layer_position.level_),
        layer_index_(layer_position.layer_index_)
  {
  }

  LayerPosition &operator=(const LayerPosition &layer_position)
  {
    this->level_ = layer_position.level_;
    this->layer_index_ = layer_position.layer_index_;
    return *this;
  }

  void reset()
  {
    level_ = 0;
    layer_index_ = 0;
  }

  bool is_valid() const
  {
    return (level_ >= 0 && level_ < storage::MAX_TIER_COUNT)
           && (layer_index_ >= 0);
  }
  int64_t position() const
  {
    return (((int64_t)level_) << 32) | layer_index_;
  }
  int32_t get_level() const { return level_; }
  int32_t get_layer_index() const { return layer_index_; }
  bool is_new_generate_layer() const { return NEW_GENERATE_LAYER_INDEX == layer_index_; }

  DECLARE_AND_DEFINE_TO_STRING(KV_(level), KV_(layer_index))
  DECLARE_COMPACTIPLE_SERIALIZATION(LAYER_POSITION_VERSION)
};

struct ExtentPosition
{
  int64_t index_id_;
  LayerPosition layer_position_;
  ExtentId extent_id_;

  ExtentPosition() : index_id_(0), layer_position_(), extent_id_() {}
  ExtentPosition(const int64_t index_id, const LayerPosition &layer_position, const ExtentId &extent_id)
      : index_id_(index_id),
        layer_position_(layer_position),
        extent_id_(extent_id)
  {}
  ExtentPosition(const ExtentPosition &) = default;
  ~ExtentPosition() {}
  void set(const int64_t index_id, const LayerPosition &layer_position, const ExtentId &extent_id)
  {
    index_id_ = index_id;
    layer_position_ = layer_position;
    extent_id_ = extent_id;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(index_id), KV_(layer_position), KV_(extent_id))
};

struct ExtentStats
{
  int64_t data_size_;
  int64_t num_entries_;
  int64_t num_deletes_;
  int64_t disk_size_;

  explicit ExtentStats() : data_size_(0),
                           num_entries_(0),
                           num_deletes_(0),
                           disk_size_(0) {}

  explicit ExtentStats(int64_t data_size,
                       int64_t num_entries,
                       int64_t num_deletes,
                       int64_t disk_size)
      : data_size_(data_size),
        num_entries_(num_entries),
        num_deletes_(num_deletes),
        disk_size_(disk_size)
  {
  }

  inline void reset()
  {
    data_size_ = 0;
    num_entries_ = 0;
    num_deletes_ = 0;
    disk_size_ = 0;
  }

  bool operator==(const ExtentStats &extent_stats)
  {
    return data_size_ == extent_stats.data_size_
           && num_entries_ == extent_stats.num_entries_
           && num_deletes_ == extent_stats.num_deletes_
           && disk_size_ == extent_stats.disk_size_;
  }

  inline void merge(const ExtentStats &extent_stats)
  {
    data_size_ += extent_stats.data_size_;
    num_entries_ += extent_stats.num_entries_;
    num_deletes_ += extent_stats.num_deletes_;
    disk_size_ += extent_stats.disk_size_;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(data_size), KV_(num_entries), KV_(num_deletes), KV_(disk_size))
};

enum ExtentSpaceType {
  FILE_EXTENT_SPACE = 0,
  OBJECT_EXTENT_SPACE = 1,
  MAX_EXTENT_SPACE
};

inline bool is_valid_extent_space_type(const int32_t extent_space_type)
{
  return extent_space_type >= FILE_EXTENT_SPACE && extent_space_type < MAX_EXTENT_SPACE;
}

struct CreateTableSpaceArgs
{
  int64_t table_space_id_;
  std::vector<common::DbPath> db_paths_;

  CreateTableSpaceArgs()
    : table_space_id_(0),
      db_paths_()
  {
  }
  CreateTableSpaceArgs(int64_t table_space_id,
                       const std::vector<common::DbPath> db_paths)
      : table_space_id_(table_space_id),
        db_paths_(db_paths)
  {
  }
  ~CreateTableSpaceArgs()
  {
  }

  bool is_valid() const
  {
    return table_space_id_ >= 0
           && db_paths_.size() > 0;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(table_space_id))
};

struct CreateExtentSpaceArgs
{
  int64_t table_space_id_;
  int32_t extent_space_type_;
  common::DbPath db_path_;

  CreateExtentSpaceArgs()
      : table_space_id_(0),
        extent_space_type_(FILE_EXTENT_SPACE),
        db_path_()
  {
  }
  CreateExtentSpaceArgs(int64_t table_space_id,
                        int32_t extent_space_type,
                        common::DbPath db_path)
      : table_space_id_(table_space_id),
        extent_space_type_(extent_space_type),
        db_path_(db_path)
  {
  }
  ~CreateExtentSpaceArgs()
  {
  }

  bool is_valid() const
  {
    return table_space_id_ >= 0 &&
           (extent_space_type_ >= FILE_EXTENT_SPACE && extent_space_type_ < MAX_EXTENT_SPACE);
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(table_space_id), KV_(extent_space_type))
};

struct CreateDataFileArgs
{
  int64_t table_space_id_;
  int32_t extent_space_type_;
  int64_t file_number_;
  std::string data_file_path_;

  CreateDataFileArgs()
      : table_space_id_(0),
        extent_space_type_(0),
        file_number_(0),
        data_file_path_()
  {
  }
  CreateDataFileArgs(int64_t table_space_id,
                     int32_t extent_space_type,
                     int64_t file_number,
                     std::string data_file_path)
      : table_space_id_(table_space_id),
        extent_space_type_(extent_space_type),
        file_number_(file_number),
        data_file_path_(data_file_path)
  {
  }
  ~CreateDataFileArgs()
  {
  }

  bool is_valid() const
  {
    return table_space_id_ >= 0 && extent_space_type_ == FILE_EXTENT_SPACE &&
           file_number_ >= 0;
  }
  DECLARE_AND_DEFINE_TO_STRING(KV_(table_space_id), KV_(extent_space_type), KV_(file_number), KV_(data_file_path))
};

struct ShrinkCondition
{
  int64_t max_free_extent_percent_;
  int64_t shrink_allocate_interval_;
  int64_t max_shrink_extent_count_;

  ShrinkCondition()
      : max_free_extent_percent_(0),
        shrink_allocate_interval_(0),
        max_shrink_extent_count_(0)
  {
  }
  ShrinkCondition(int64_t max_free_extent_percent, int64_t shrink_allocate_interval, int64_t max_shrink_extent_count)
      : max_free_extent_percent_(max_free_extent_percent),
        shrink_allocate_interval_(shrink_allocate_interval),
        max_shrink_extent_count_(max_shrink_extent_count)
  {
  }
  ShrinkCondition(const ShrinkCondition &shrink_cond)
      : max_free_extent_percent_(shrink_cond.max_free_extent_percent_),
        shrink_allocate_interval_(shrink_cond.shrink_allocate_interval_),
        max_shrink_extent_count_(shrink_cond.max_shrink_extent_count_)
  {
  }
  ~ShrinkCondition()
  {
  }

  ShrinkCondition &operator=(const ShrinkCondition &shrink_cond)
  {
    this->max_free_extent_percent_ = shrink_cond.max_free_extent_percent_;
    this->shrink_allocate_interval_ = shrink_cond.shrink_allocate_interval_;
    this->max_shrink_extent_count_ = shrink_cond.max_shrink_extent_count_;
    return *this;
  }

  bool is_valid() const
  {
    return max_free_extent_percent_ >=0 && shrink_allocate_interval_ >= 0 && max_free_extent_percent_ >= 0;
  }

  bool operator==(const ShrinkCondition &shrink_condition)
  {
    return max_free_extent_percent_ == shrink_condition.max_free_extent_percent_
           && shrink_allocate_interval_ == shrink_condition.shrink_allocate_interval_
           && max_shrink_extent_count_ == shrink_condition.max_shrink_extent_count_;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(max_free_extent_percent), KV_(shrink_allocate_interval), KV_(max_shrink_extent_count))
};

struct ShrinkInfo
{
  ShrinkCondition shrink_condition_;
  int64_t table_space_id_;
  int32_t extent_space_type_;
  std::set<int64_t> index_id_set_;
  int64_t total_need_shrink_extent_count_;
  int64_t shrink_extent_count_;

  ShrinkInfo()
      : shrink_condition_(),
        table_space_id_(0),
        extent_space_type_(0),
        index_id_set_(),
        total_need_shrink_extent_count_(0),
        shrink_extent_count_(0)
  {
  }
  ShrinkInfo(const ShrinkInfo &shrink_info)
      : shrink_condition_(shrink_info.shrink_condition_),
        table_space_id_(shrink_info.table_space_id_),
        extent_space_type_(shrink_info.extent_space_type_),
        index_id_set_(shrink_info.index_id_set_),
        total_need_shrink_extent_count_(shrink_info.total_need_shrink_extent_count_),
        shrink_extent_count_(shrink_info.shrink_extent_count_)
  {
  }
  ~ShrinkInfo()
  {
  }

  ShrinkInfo &operator=(const ShrinkInfo &shrink_info)
  {
    this->shrink_condition_ = shrink_info.shrink_condition_;
    this->table_space_id_ = shrink_info.table_space_id_;
    this->extent_space_type_ = shrink_info.extent_space_type_;
    this->index_id_set_ = shrink_info.index_id_set_;
    this->total_need_shrink_extent_count_ = shrink_info.total_need_shrink_extent_count_;
    this->shrink_extent_count_ = shrink_info.shrink_extent_count_;
    return *this;
  }

  bool is_valid() const
  {
    return shrink_condition_.is_valid()
           && table_space_id_ >= 0
           && is_valid_extent_space_type(extent_space_type_)
           && total_need_shrink_extent_count_ > 0
           && shrink_extent_count_ > 0;
  }

  bool operator==(const ShrinkInfo &shrink_info)
  {
    return shrink_condition_ == shrink_info.shrink_condition_
           && table_space_id_ == shrink_info.table_space_id_
           && extent_space_type_ == shrink_info.extent_space_type_
           && index_id_set_ == shrink_info.index_id_set_
           && total_need_shrink_extent_count_ == shrink_info.total_need_shrink_extent_count_
           && shrink_extent_count_ == shrink_info.shrink_extent_count_;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(shrink_condition), KV_(table_space_id), KV_(extent_space_type), KV_(total_need_shrink_extent_count), KV_(shrink_extent_count))
};

struct ExtentIOInfo
{
  ExtentSpaceType type_;
  ExtentId extent_id_;
  int64_t extent_size_;
  int64_t unique_id_;
  int fd_;
  ::objstore::ObjectStore *object_store_;
  std::string bucket_;
  std::string prefix_;

  ExtentIOInfo()
      : type_(FILE_EXTENT_SPACE),
        extent_id_(),
        extent_size_(0),
        unique_id_(0),
        fd_(-1),
        object_store_(nullptr),
        bucket_(),
        prefix_()
  {
  }
  ExtentIOInfo(const ExtentIOInfo &) = default;
  ~ExtentIOInfo() {}
  ExtentIOInfo &operator=(const ExtentIOInfo &) = default;

  bool is_valid() const
  {
    bool res = (FILE_EXTENT_SPACE == type_ || OBJECT_EXTENT_SPACE == type_) &&
                unique_id_ >= 0 && extent_size_ > 0;

    if (res) {
      res = (FILE_EXTENT_SPACE == type_ && fd_ >= 0) ||
            (OBJECT_EXTENT_SPACE == type_ && IS_NOTNULL(object_store_)&&
             fd_ < 0 && !bucket_.empty());
    }

    return res;
  }

  void reset()
  {
    type_ = FILE_EXTENT_SPACE;
    extent_id_.reset();
    extent_size_ = 0;
    unique_id_ = 0;
    fd_ = -1;
    object_store_ = nullptr;
    bucket_.clear();
    prefix_.clear();
  }

  void set_param(ExtentSpaceType type,
                 const ExtentId &extent_id,
                 int64_t extent_size,
                 int64_t unique_id,
                 int fd)
  {
    type_ = type;
    extent_id_ = extent_id;
    extent_size_ = extent_size;
    unique_id_ = unique_id;
    fd_ = fd;
  }

  void set_param(ExtentSpaceType type,
                 const ExtentId &extent_id,
                 int64_t extent_size,
                 int64_t unique_id,
                 int fd,
                 ::objstore::ObjectStore *object_store,
                 const std::string &bucket,
                 const std::string &prefix)
  {
    type_ = type;
    extent_id_ = extent_id;
    extent_size_ = extent_size;
    unique_id_ = unique_id;
    fd_ = fd;
    object_store_ = object_store;
    bucket_ = bucket;
    prefix_ = prefix;
  }

  DECLARE_AND_DEFINE_TO_STRING(KVE_(type), KV_(extent_id), KV_(extent_size),
      KV_(unique_id), KV_(fd), KVP_(object_store), KV_(bucket), KV_(prefix))
};

struct DataFileStatistics
{
  int64_t table_space_id_;
  int32_t extent_space_type_;
  int64_t file_number_;
  int64_t total_extent_count_;
  int64_t used_extent_count_;
  int64_t free_extent_count_;

  DataFileStatistics()
      : table_space_id_(0),
        extent_space_type_(0),
        file_number_(0),
        total_extent_count_(0),
        used_extent_count_(0),
        free_extent_count_(0)
  {}
  DataFileStatistics(const DataFileStatistics &) = default;
  DataFileStatistics(int64_t table_space_id,
                     int32_t extent_space_type,
                     int64_t file_number,
                     int64_t total_extent_count,
                     int64_t used_extent_count,
                     int64_t free_extent_count)
      : table_space_id_(table_space_id),
        extent_space_type_(extent_space_type),
        file_number_(file_number),
        total_extent_count_(total_extent_count),
        used_extent_count_(used_extent_count),
        free_extent_count_(free_extent_count)
  {
  }
  ~DataFileStatistics()
  {
  }

  void reset()
  {
    table_space_id_ = 0;
    extent_space_type_ = 0;
    file_number_ = 0;
    total_extent_count_ = 0;
    used_extent_count_ = 0;
    free_extent_count_ = 0;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(table_space_id), KV_(extent_space_type), KV_(total_extent_count),
      KV_(used_extent_count), KV_(free_extent_count))
};

struct EstimateCostStats {
  int64_t subtable_id_;
  int64_t cost_size_;
  int64_t total_extent_cnt_;
  int64_t total_open_extent_cnt_;
  bool recalc_last_extent_;

  EstimateCostStats();
  ~EstimateCostStats();

  void reset();
  DECLARE_TO_STRING()
};

inline constexpr int32_t kObjStoreFDBit = 31;
inline constexpr int64_t kMaxTableSpaceId = 1LL << kObjStoreFDBit;

inline int32_t convert_table_space_to_fd(uint64_t table_space_id) {
  assert(table_space_id < kMaxTableSpaceId);
  int32_t fd = table_space_id;
  // make fd negative, so that it can be distinguished from normal file number.
  return fd | (1 << kObjStoreFDBit);
}

inline int32_t convert_fdfn_to_table_spaceid(int32_t fdfn) {
  assert((fdfn & (1 << kObjStoreFDBit)) != 0);
  int32_t table_spaceid = fdfn & (~(1 << kObjStoreFDBit));
  return table_spaceid;
}

inline uint64_t assemble_objid(int32_t table_spaceid, int32_t offset_id) {
  assert((table_spaceid & (1 << kObjStoreFDBit)) == 0);
  assert(offset_id < INT32_MAX);
  return (static_cast<uint64_t>(offset_id) << 32) |
         static_cast<uint64_t>(table_spaceid);
}

inline uint64_t assemble_objid_by_fdfn(int32_t fdfn, int32_t offset_id) {
  return assemble_objid(convert_fdfn_to_table_spaceid(fdfn), offset_id);
}

}  // namespace storage
}  //namespace smartengine

#endif
