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

#pragma once

#include "objstore.h"

namespace smartengine
{
namespace objstore
{

/**
 * @brief A class to manage integer allocation in an object store atomically based on condition write.
 * Every allocator should be created first and once, then should be initialized, and the allocator can
 * allocate normally.
 */
class ObjStoreIntAllocator
{
public:
  ObjStoreIntAllocator();
  ~ObjStoreIntAllocator();

/**
 * @brief Creates an integer allocator in the specified object store.
 * 
 * @param object_store Pointer to the object store.
 * @param bucket The bucket name in the object store.
 * @param key The key name in the object store.
 * @param first_value The initial value to be set.
 * @return int Status::kOk for success, or other for failure.
 */
static int create(::objstore::ObjectStore *object_store,
                  const std::string &bucket,
                  const std::string &key,
                  int64_t first_value);

/**
 * @brief Removes an integer allocator from the specified object store.
 * If the object does not exist, it will still return success.
 * 
 * @param object_store Pointer to the object store.
 * @param bucket The bucket name in the object store.
 * @param key The key name in the object store.
 * @return int Status::kOk for success, or other for failure.
 */
static int remove(::objstore::ObjectStore *object_store, const std::string &bucket, const std::string &key);

/**
 * @brief Initializes the integer allocator with the specified parameters.
 * 
 * @param object_store Pointer to the object store.
 * @param bucket The bucket name in the object store.
 * @param key The key name in the object store.
 * @param step The step value for allocation.
 * @return int Status::kOk for success, or other for failure.
 */
int init(::objstore::ObjectStore *object_store, const std::string &bucket, const std::string &key, int64_t step);

/**
 * @brief Allocates a range of integers, the range is [start, end).
 * 
 * @param start Reference to the start of the allocated range.
 * @param end Reference to the end of the allocated range.
 * @return int Status::kOk for success, or other for failure.
 */
int alloc(int64_t &start, int64_t &end);

/**
 * @brief Retrieves the current value of the integer allocator.
 * 
 * @param value Reference to store the current value.
 * @return int Status::kOk for success, or other for failure.
 */
int get_current_value(int64_t &value);

private:
  bool is_inited_;
  ::objstore::ObjectStore *object_store_;
  std::string bucket_;
  std::string key_;
  int64_t step_;
};

} // namespace objstore
} // namespace smartengine