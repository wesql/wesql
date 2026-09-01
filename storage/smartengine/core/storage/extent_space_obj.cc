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
      inused_extent_set_()
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

int ObjectExtentSpace::allocate(const std::string prefix, ExtentIOInfo &io_info)
{
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjectExtentSpace should been inited first", K(ret));
  } else {
    int64_t objstore_extent_id = 0;
    if (FAILED(env_->AllocExtentId(objstore_extent_id).code())) {
      ret = Status::kObjStoreError;
      SE_LOG(WARN, "fail to allocate extent id", K(ret));
    } else {
      ExtentId extent_id;
      extent_id.set(objstore_extent_id);
      if (inused_extent_set_.insert(objstore_extent_id).second) {
        ++total_extent_count_;
        ++used_extent_count_;
        io_info.set_param(OBJECT_EXTENT_SPACE,
                          extent_id,
                          MAX_EXTENT_SIZE,
                          UniqueIdAllocator::get_instance().alloc(),
                          -1,
                          objstore_,
                          env_->GetObjectStoreBucket(),
                          prefix);
      } else {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "fail to insert extent into inused set", K(ret), K(objstore_extent_id));
      }
    }
  }
  
  return ret;
}

int ObjectExtentSpace::recycle(const std::string prefix, const ExtentId extent_id) {
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
  } else if (inused_extent_set_.count(extent_id.id()) == 0) {
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
      inused_extent_set_.erase(extent_id.id());
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
  } else if (!(inused_extent_set_.insert(extent_id.id()).second)) {
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
