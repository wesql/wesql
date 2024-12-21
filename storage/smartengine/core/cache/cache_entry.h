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

#include "cache/cache.h"
#include "env/env.h"
#include "util/coding.h"

namespace smartengine
{
namespace storage
{
class IOExtent;
}

namespace cache
{
template <typename T>
struct CacheEntry
{
  T *value_;
  cache::Cache::Handle *handle_;

  CacheEntry() : value_(nullptr), handle_(nullptr) {}
  CacheEntry(T *value, cache::Cache::Handle *handle)
      : value_(value), handle_(handle)
  {}

  void release(cache::Cache *cache)
  {
    assert(IS_NOTNULL(cache));

    if (IS_NOTNULL(handle_)) {
      cache->Release(handle_);
      value_ = nullptr;
      handle_ = nullptr;
    }
  }
};

class CacheEntryKey
{
public:
  static constexpr int64_t MAX_CACHE_KEY_PREFIX_SIZE = util::kMaxVarint64Length * 3 + 1;
  static constexpr int64_t MAX_CACHE_KEY_SZIE = MAX_CACHE_KEY_PREFIX_SIZE + util::kMaxVarint64Length;

public:
  CacheEntryKey();
  ~CacheEntryKey();

  int setup(const storage::IOExtent *extent);
  // The caller needs to ensure then the key space is at least MAX_CACHE_KEY_SZIE
  int generate(const int64_t id, char *buf, common::Slice &key);

private:
  bool is_inited_;
  char key_prefix_[MAX_CACHE_KEY_PREFIX_SIZE];
  int64_t key_prefix_size_;
};

class CacheEntryHelper
{
public:
  template <typename T>
  static void delete_entry(const common::Slice &key, void *value)
  {
    T *entry_ptr = reinterpret_cast<T *>(value);
    MOD_DELETE_OBJECT(T, entry_ptr);
  }
};

} // namespace cache 
} // namespace smartengine
