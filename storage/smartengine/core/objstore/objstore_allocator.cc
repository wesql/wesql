/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
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

#include "objstore/objstore_allocator.h"
#include "logger/log_module.h"
#include "util/status.h"

namespace smartengine
{
using namespace common;

namespace objstore
{ 

ObjStoreIntAllocator::ObjStoreIntAllocator()
    : is_inited_(false),
      object_store_(nullptr),
      bucket_(),
      key_(),
      step_(0)
{
}

ObjStoreIntAllocator::~ObjStoreIntAllocator()
{
}

/**
 * @brief Creates an integer allocator in the specified object store.
 * 
 * @param object_store Pointer to the object store.
 * @param bucket The bucket name in the object store.
 * @param key The key name in the object store.
 * @param first_value The initial value to be set.
 * @return int Status::kOk for success, or other for failure.
 */
int ObjStoreIntAllocator::create(::objstore::ObjectStore *object_store,
                                 const std::string &bucket,
                                 const std::string &key,
                                 int64_t first_value)
{
  int ret = Status::kOk;
  ::objstore::Status object_status;
  std::string value_str;
  std::string etag; // empty etag

  if (IS_NULL(object_store) || UNLIKELY(bucket.empty()) || UNLIKELY(key.empty()) || UNLIKELY(first_value < 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(object_store), K(bucket), K(key), K(first_value));
  } else {
    value_str = std::to_string(first_value);
    object_status = object_store->put_object(bucket, key, value_str, etag, true /*forbid_overwrite*/);
    if (UNLIKELY(!object_status.is_succ())) {
      ret = Status::kObjStoreError;
      SE_LOG(WARN, "io error, fail to put object", K(ret), KE(object_status.error_code()),
          K(object_status.error_message()), K(bucket), K(key));
    }
  }

  return ret;
}

/**
 * @brief Removes an integer allocator from the specified object store.
 * If the object does not exist, it will still return success.
 * 
 * @param object_store Pointer to the object store.
 * @param bucket The bucket name in the object store.
 * @param key The key name in the object store.
 * @return int Status::kOk for success, or other for failure.
 */
int ObjStoreIntAllocator::remove(::objstore::ObjectStore *object_store, const std::string &bucket, const std::string &key)
{
  int ret = Status::kOk;
  ::objstore::Status object_status;

  if (IS_NULL(object_store) || UNLIKELY(bucket.empty()) || UNLIKELY(key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(object_store), K(bucket), K(key));
  } else {
    object_status = object_store->delete_object(bucket, key);
    if (UNLIKELY(!object_status.is_succ())) {
      ret = Status::kObjStoreError;
      SE_LOG(WARN, "io error, fail to delete object", K(ret), KE(object_status.error_code()), 
          K(object_status.error_message()), K(bucket), K(key));
    }
  }

  return ret;
}

/**
 * @brief Initializes the integer allocator with the specified parameters. 
 * The allocator must have been created before initialization.
 * 
 * @param object_store Pointer to the object store.
 * @param bucket The bucket name in the object store.
 * @param key The key name in the object store.
 * @param step The step value for allocation.
 * @return int Status::kOk for success, or other for failure.
 */
int ObjStoreIntAllocator::init(::objstore::ObjectStore *object_store,
                               const std::string &bucket,
                               const std::string &key,
                               int64_t step)
{
  int ret = Status::kOk;
  ::objstore::Status object_status;
  std::string value_str;
  std::string etag;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ObjStoreIntAllocator has been inited", K(ret));
  } else if (IS_NULL(object_store) || UNLIKELY(bucket.empty()) || UNLIKELY(key.empty()) || UNLIKELY(step <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(object_store), K(bucket), K(key), K(step));
  } else {
    // Check if the object exists in the object store
    object_status = object_store->get_object(bucket, key, value_str, &etag);
    if (UNLIKELY(!object_status.is_succ())) {
      ret = Status::kEntryNotExist;
      SE_LOG(WARN, "the allocator object may not exist, create it first", K(ret), K(bucket), K(key));
    } else if (UNLIKELY(value_str.empty())|| UNLIKELY(etag.empty())) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "the allocator status is unexpected", K(ret), K(bucket), K(key), K(value_str), K(etag));
    } else {
      object_store_ = object_store;
      bucket_ = bucket;
      key_ = key;
      step_ = step;

      is_inited_ = true;
    }
  }

  return ret;
}

// TODO (Zhao Dongsheng) : Optimize the allocation process by cache the last_value and etag if needed.
/**
 * @brief Allocates a range of integers, the range is [start, end).
 * 
 * @param start Reference to the start of the allocated range.
 * @param end Reference to the end of the allocated range.
 * @return int Status::kOk for success, or other for failure.
 */
int ObjStoreIntAllocator::alloc(int64_t &start, int64_t &end)
{
  int ret = Status::kOk;
  ::objstore::Status object_status;
  int64_t new_start = 0;
  int64_t new_end = 0;
  std::string value_str;
  std::string etag;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjStoreIntAllocator has not been inited", K(ret));
  } else {
    while (SUCCED(ret)) {
      new_start = 0;
      new_end = 0;
      value_str.clear();
      etag.clear();

      object_status = object_store_->get_object(bucket_, key_, value_str, &etag);
      if (UNLIKELY(!object_status.is_succ())) {
        ret = Status::kObjStoreError;
        SE_LOG(WARN, "io error, fail to get object", K(ret), KE(object_status.error_code()),
            K(object_status.error_message()), K_(bucket), K_(key));
      } else if (UNLIKELY(value_str.empty()) || UNLIKELY(etag.empty())) {
        ret = Status::kErrorUnexpected;
        SE_LOG(ERROR, "the allocator status is unexpected", K(ret), K_(bucket), K_(key), K(value_str), K(etag));
      } else {
        new_start = std::stoll(value_str);
        new_end = new_start + step_; 
        value_str = std::to_string(new_end);
        se_assert(new_start < new_end);

        object_status = object_store_->put_object(bucket_, key_, value_str, etag, false);
        if (LIKELY(object_status.is_succ())) {
          start = new_start;
          end = new_end;
          break;
        } else if (UNLIKELY(::objstore::Errors::SE_OBJECT_FORBID_OVERWRITE != object_status.error_code())) {
          ret = Status::kObjStoreError;
          SE_LOG(WARN, "io error, fail to get object", K(ret), KE(object_status.error_code()),
              K(object_status.error_message()), K_(bucket), K_(key));
        } else {        
          SE_LOG(INFO, "the object has been updated by another allocator, retry it", K(ret), K_(bucket), K_(key));
          // Overwrite ret, and retry it.
          ret = Status::kOk;
        }
      }
    }
  }

  return ret;
}

/**
 * @brief Retrieves the current value of the integer allocator.
 * 
 * @param value Reference to store the current value.
 * @return int Status::kOk for success, or other for failure.
 */
int ObjStoreIntAllocator::get_current_value(int64_t &value)
{
  int ret = Status::kOk;
  ::objstore::Status object_status;
  std::string value_str;
  std::string etag;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjStoreIntAllocator has not been inited", K(ret));
  } else {
    object_status = object_store_->get_object(bucket_, key_, value_str, &etag);
    if (UNLIKELY(!object_status.is_succ())) {
      ret = Status::kObjStoreError;
      SE_LOG(WARN, "io error, fail to get object", K(ret), KE(object_status.error_code()),
          K(object_status.error_message()), K_(bucket), K_(key));
    } else if (UNLIKELY(value_str.empty()) || UNLIKELY(etag.empty())) {
      ret = Status::kErrorUnexpected;
      SE_LOG(ERROR, "the allocator status is unexpected", K(ret), K_(bucket), K_(key), K(value_str), K(etag));
    } else {
      value = std::stoll(value_str);
    }
  }

  return ret;
}

} // namespace objstore
} // namespace smartengine