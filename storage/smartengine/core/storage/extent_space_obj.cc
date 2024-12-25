/*
 * Copyright (c) 2023, ApeCloud Inc Holding Limited
 *
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

#include "storage/extent_space_obj.h"

#include <cstdint>
#include <string>

#include "objstore.h"
#include "util/increment_number_allocator.h"
#include "util/status.h"
#include "util/sync_point.h"

namespace smartengine
{

using namespace common;
using namespace util;

namespace storage
{

ObjectExtentSpace::ObjectExtentSpace(util::Env *env,
                                     const util::EnvOptions &env_options,
                                     ::objstore::ObjectStore *objstore)
    : is_inited_(false),
      env_(env),
      env_options_(env_options),
      objstore_(objstore),
      extent_bucket_(),
      table_space_id_(0),
      extent_space_type_(OBJECT_EXTENT_SPACE),
      total_extent_count_(0),
      used_extent_count_(0),
      free_extent_count_(0),
      last_alloc_ts_(0),
      inused_extent_set_(),
      last_allocated_extent_offset_(0)
{
  // init field
}

ObjectExtentSpace::~ObjectExtentSpace() { destroy(); }

void ObjectExtentSpace::destroy() {
  if (is_inited_) {
    is_inited_ = false;
  }
}

int ObjectExtentSpace::create(const CreateExtentSpaceArgs &args) {
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ObjectExtentSpace has been inited", K(ret));
  } else if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (FAILED(env_->GetObjectStore(objstore_).code())) {
    SE_LOG(WARN, "fail to get object store from env", K(ret));
  } else {
    assert(args.db_path_.target_size == UINT64_MAX);
    extent_bucket_ = args.db_path_.path;
    table_space_id_ = args.table_space_id_;
    extent_space_type_ = args.extent_space_type_;
    update_last_alloc_ts();
    is_inited_ = true;
  }

  return ret;
}

int ObjectExtentSpace::remove() {
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjectExtentSpace should been inited first", K(ret));
  } else if (!is_free()) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, ObjectExtentSpace is not free", K(ret));
  } else {
    se_assert(inused_extent_set_.empty());
    // remove all the extent
    //for (int32_t ext_off : inused_extent_set_) {
    //  std::string extent_key = make_extent_key(ext_off);
    //  ::objstore::Status st =
    //      objstore_->delete_object(extent_bucket_, extent_key);
    //  if (!st.is_succ()) {
    //    ret = Status::kObjStoreError;
    //    SE_LOG(WARN, "fail to remeve extent", K(extent_bucket_), K(extent_key),
    //           K(int(st.error_code())), K(std::string(st.error_message())),
    //           K(ret));
    //    break;
    //  }
    //  assert(st.is_succ());
    //}
    //if (ret == Status::kOk) {
    //  inused_extent_set_.clear();
    //}
  }

  return ret;
}

/*
For std::set, the time consumed for various operations in some one machine is as follows:
  insert 100000 elements: 64000us
  find 100000 elements: 15000us
  std::distance(100000 elements): 980us
  std::advance(100000 elements): 950us
*/
int ObjectExtentSpace::find_available_extent_offset(int32_t &extent_offset)
{
  int ret = Status::kOk;
  std::set<int32_t> &t = inused_extent_set_;
  size_t size = t.size();
  constexpr size_t MAX_EXTENT_OFFSET = INT32_MAX;
  int32_t offset_largest_val = static_cast<int32_t>(EXTENT_OFFSET_LARGEST_VAL);
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("ObjectExtentSpace::find_available_extent_offset::change_offset_largest_val",
                           &offset_largest_val);
#endif
  extent_offset = -1;

  if (0 == size) {
    // empty
    extent_offset = 0;
  } else if (static_cast<long long>(size) < (1LL + offset_largest_val)) {
    int32_t first_value = *(t.begin());
    int32_t last_value = *(std::prev(t.end()));

    if (last_allocated_extent_offset_ < 0) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, last_allocated_extent_offset_ < 0", K(ret), K(last_allocated_extent_offset_));
    } else if (first_value < 0 || last_value > offset_largest_val) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, first_value < 0 or last_value > offset_largest_val", K(ret), K(first_value), K(last_value), K(offset_largest_val));
    } else if (first_value == 0 && last_value == offset_largest_val) {
      int32_t next_value = (last_allocated_extent_offset_ == offset_largest_val ? 0
                                                                                : last_allocated_extent_offset_ + 1);
      constexpr int32_t BIG_STEP_SIZE = 100000;
      constexpr int32_t SMALL_STEP_SIZE = 5000;

      int32_t cur_range_first_value = next_value;
      int32_t cur_range_last_value = (cur_range_first_value < offset_largest_val - BIG_STEP_SIZE
                                          ? cur_range_first_value + BIG_STEP_SIZE
                                          : offset_largest_val);
      while (SUCCED(ret)) {
        if (cur_range_last_value < 0 || cur_range_first_value > cur_range_last_value) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN,
                 "unexpected error, cur_range_first_value < 0 or > cur_range_last_value",
                 K(ret),
                 K(cur_range_first_value),
                 K(cur_range_last_value));
          break;
        }
        auto cur_first = t.find(cur_range_first_value);
        auto cur_last = t.find(cur_range_last_value);
        if (cur_first != t.end() && cur_last != t.end()) {
          long distance = std::distance(cur_first, cur_last);
          if (distance < cur_range_last_value - cur_range_first_value) {
            // there is an available extent offset at least, will find it
            break;
          } else if (distance > cur_range_last_value - cur_range_first_value) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN,
                   "unexpected error, impossible case",
                   K(ret),
                   K(cur_range_first_value),
                   K(cur_range_last_value),
                   K(distance));
          } else {
            // no available extent offset in this range
            if (cur_range_last_value == offset_largest_val) {
              cur_range_first_value = 0;
              cur_range_last_value = BIG_STEP_SIZE;
            } else {
              cur_range_first_value = cur_range_last_value + 1;
              cur_range_last_value = cur_range_first_value < offset_largest_val - BIG_STEP_SIZE
                                         ? cur_range_first_value + BIG_STEP_SIZE
                                         : offset_largest_val;
            }
          }
        } else {
          // there is an available extent offset at least, will find it
          break;
        }
      }

      if (SUCCED(ret)) {
        int32_t step = cur_range_last_value - cur_range_first_value > SMALL_STEP_SIZE
                           ? SMALL_STEP_SIZE
                           : cur_range_last_value - cur_range_first_value;
        if (step <= 0 || cur_range_last_value < 0 || cur_range_first_value > cur_range_last_value) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN,
                 "unexpected error, impossible case",
                 K(ret),
                 K(step),
                 K(cur_range_first_value),
                 K(cur_range_last_value));
        } else {
          bool found = false;
          // find the available extent offset in the range [cur_range_first_value, cur_range_last_value]
          while (step > 0 && !found) {
            auto small_step_first = t.find(cur_range_first_value);
            auto small_step_last = t.find(cur_range_first_value + step);
            if (small_step_first != t.end() && small_step_last != t.end() &&
                std::distance(small_step_first, small_step_last) == step) {
              cur_range_first_value += step;
              step = (cur_range_last_value - cur_range_first_value > SMALL_STEP_SIZE
                          ? SMALL_STEP_SIZE
                          : cur_range_last_value - cur_range_first_value);
              continue;
            }

            for (int32_t i = cur_range_first_value; i <= cur_range_first_value + step; i++) {
              if (t.find(i) == t.end()) {
                extent_offset = i;
                found = true;
                break;
              }
            }
          }

          if (step < 0) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN, "unexpected error, step < 0", K(ret), K(step));
          } else if (!found) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN, "extent offset set is not full but allocated failed", K(ret));
          }
        }
      }
    } else {
      // extent offset must be in [0, offset_large_val]
      extent_offset = (last_value != offset_largest_val) ? last_value + 1 : 0;
    }
  } else if ((1LL + offset_largest_val) == static_cast<long long>(size)) {
    // full
    ret = Status::kNoSpace;
  } else {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, extent offset set size is invalid", K(ret), K(size));
  }

  if (SUCCED(ret)) {
    if (extent_offset < 0 || extent_offset > offset_largest_val) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent offset < 0", K(ret), K(extent_offset), K(offset_largest_val));
    } else if (t.find(extent_offset) != t.end()) { // make sure the found extent_offset is not used.
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, allocated extent offset is already used", K(ret), K(extent_offset));
    } else {
      last_allocated_extent_offset_ = extent_offset;
    }
  }
  return ret;
}


int ObjectExtentSpace::allocate(const std::string prefix, ExtentIOInfo &io_info)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjectExtentSpace should been inited first", K(ret));
  } else {
    int32_t extent_offset = 0;
    ret = find_available_extent_offset(extent_offset);
    if (FAILED(ret)) {
      SE_LOG(WARN, "fail to allocate extent from obj extent space", K(ret));
    } else {
      ExtentId extent_id;
      // table_space_id_(31Bits) + extent_offset(31Bits) = ExtentID,
      // the extent_id.offset overflow is possible, it is not common case in the
      // real world. but this design does introduce a limitation ( <= 4PB) for
      // a single table.
      int32_t faked_fn = convert_table_space_to_fd(table_space_id_);
      extent_id.file_number = faked_fn;
      extent_id.offset = extent_offset;
      if (inused_extent_set_.insert(extent_id.offset).second) {
        ++total_extent_count_;
        ++used_extent_count_;
        io_info.set_param(OBJECT_EXTENT_SPACE,
                          extent_id,
                          MAX_EXTENT_SIZE,
                          UniqueIdAllocator::get_instance().alloc(),
                          faked_fn,
                          objstore_,
                          env_->GetObjectStoreBucket(),
                          prefix);
      } else {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "fail to insert extent into inused set", K(ret),
               K(table_space_id_), K(extent_id.offset));
      }
    }
  }

  return ret;
}

int ObjectExtentSpace::recycle(const std::string prefix, const ExtentId extent_id) {
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
  } else if (extent_id.file_number !=
             convert_table_space_to_fd(table_space_id_)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "unexpected error, file_num not match",
           K(extent_id.file_number), K(table_space_id_), K(ret));
  } else if (inused_extent_set_.count(extent_id.offset) == 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "unexpected error, can not find extent in inused set",
           K(extent_id.offset), K(ret));
  } else {
    std::string extent_key = prefix + make_extent_key(extent_id.offset);
    ::objstore::Status st =
        objstore_->delete_object(extent_bucket_, extent_key);
    if (!st.is_succ()) {
      ret = Status::kObjStoreError;
      SE_LOG(WARN, "fail to recyle extent", K(ret), K(extent_bucket_), K(extent_key),
             KE(st.error_code()), K(st.error_message()));
    } else {
      inused_extent_set_.erase(extent_id.offset);
      --total_extent_count_;
      --used_extent_count_;
    }
  }

  return ret;
}

int ObjectExtentSpace::reference_if_need(const std::string prefix,
                                         const ExtentId extent_id,
                                         ExtentIOInfo &io_info,
                                         bool &existed)
{
  int ret = Status::kOk;
  int32_t faked_fn = convert_table_space_to_fd(table_space_id_);

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
  } else if (extent_id.file_number != faked_fn) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "unexpected error, file_num not match",
           K(extent_id.file_number), K(table_space_id_), K(ret));
  } else if (!(inused_extent_set_.insert(extent_id.offset).second)) {
    existed = true;
    SE_LOG(DEBUG, "extent is already referenced before", K(extent_id));
  } else {
    ++total_extent_count_;
    ++used_extent_count_;
    existed = false;

    io_info.set_param(OBJECT_EXTENT_SPACE,
                      extent_id,
                      MAX_EXTENT_SIZE,
                      UniqueIdAllocator::get_instance().alloc(),
                      faked_fn,
                      objstore_,
                      env_->GetObjectStoreBucket(),
                      prefix);

    SE_LOG(DEBUG, "success to reference extent", K(extent_id));
  }
  return ret;
}

int ObjectExtentSpace::get_shrink_info_if_need(
    const ShrinkCondition &shrink_condition,
    bool &need_shrink,
    ShrinkInfo &shrink_info)
{
  UNUSED(shrink_condition);
  UNUSED(shrink_info);
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
  } else {
    // no need shrink extent space for object extent space
    need_shrink = false;
  }

  return ret;
}

int ObjectExtentSpace::move_extens_to_front(
    const int64_t move_extent_count,
    std::unordered_map<int64_t, ExtentIOInfo> &replace_map) {
  return Status::kNotSupported;
}

int ObjectExtentSpace::shrink(const int64_t shrink_extent_count) {
  return Status::kNotSupported;
}

bool ObjectExtentSpace::is_free() {
  // only used in TableSpace::remove, which checks the extent space is free
  // before removing it.
  // TODO(cnut): check whether the extent space is free.
  return inused_extent_set_.empty();
}

int ObjectExtentSpace::get_data_file_stats(
    std::vector<DataFileStatistics> &data_file_stats) {
  int ret = Status::kOk;
  DataFileStatistics data_file_statistic;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjectExtentSpace should been inited first", K(ret));
  } else {
    data_file_statistic.table_space_id_ = table_space_id_;
    data_file_statistic.extent_space_type_ = OBJECT_EXTENT_SPACE;
    data_file_statistic.file_number_ =
        convert_table_space_to_fd(table_space_id_);
    data_file_statistic.total_extent_count_ = total_extent_count_;
    data_file_statistic.used_extent_count_ = used_extent_count_;
    data_file_statistic.free_extent_count_ = free_extent_count_;
    data_file_stats.push_back(data_file_statistic);
  }

  return ret;
}

int ObjectExtentSpace::add_data_file(DataFile *data_file) {
  // recover path will use this function to add opened data file into the file
  // extent space, since for objstore, there is no file, so it's not neccesary.
  // return knotSupported to make sure the caller will not use this function.
  return Status::kNotSupported;
}

int ObjectExtentSpace::rebuild() {
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
  }

  // from the file extent implementation, this interface is used to find free
  // extent (by check whether the extent is reference or not). since we have
  // delete the extent during recycle, maybe this operation is not need, make
  // sure whether this is right or not.
  return ret;
}

std::string ObjectExtentSpace::make_extent_key(
    const int32_t extent_offset_id) const {
  return std::to_string(assemble_objid(table_space_id_, extent_offset_id));
}

}  // namespace storage
}  // namespace smartengine
