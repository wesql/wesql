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

#include "cache/cache_entry.h"
#include "env/env.h"
#include "storage/storage_common.h"
#include "table/extent_struct.h"
#include <mutex>
#include <string>
namespace smartengine
{
namespace common
{
class Slice;
} // namespace common
namespace storage
{
struct ExtentId;
} // namespace storage
namespace util
{
struct AIOHandle;
} // namespace util

namespace cache
{
class PersistentCacheFile;


struct PersistentCacheInfo
{
  PersistentCacheFile *cache_file_;
  storage::ExtentId data_extent_id_;
  storage::ExtentId cache_extent_id_;

  PersistentCacheInfo();
  PersistentCacheInfo(const PersistentCacheInfo &other);
  ~PersistentCacheInfo();

  void reset();
  bool is_valid() const;
  inline PersistentCacheFile *get_cache_file() { return cache_file_; }
  inline int64_t get_offset() { return cache_extent_id_.offset * storage::MAX_EXTENT_SIZE; }
  inline int64_t get_usable_size() { return storage::MAX_EXTENT_SIZE; }
  static void delete_entry(const common::Slice &key, void *value);

  DECLARE_TO_STRING()
};

using RecoverCacheExtentFunc = std::function<int(const storage::ExtentId&, const PersistentCacheInfo&, cache::Cache::Handle **)>;
using RecoverCallbackFunc = std::function<int()>;

/**The format of the persistent cache files is similar to that of the
data files, both using fixed-length 2MB(storage::MAX_EXTENT_SIZE) as
the basic management unit.Each extent is uniquely identified by offset
0, 1, 2 ...*/
class PersistentCacheFile
{
public:
  PersistentCacheFile();
  ~PersistentCacheFile();

  int open(util::Env *env,
           const std::string &file_path,
           int64_t cache_size,
           int64_t &need_recover_extent_count);
  int recover(int64_t extent_count,
              int64_t parallel_job_count,
              const RecoverCacheExtentFunc &recover_func,
              const RecoverCallbackFunc &callback_func);
  void close();
  int alloc(const storage::ExtentId &data_extent_id, PersistentCacheInfo &cache_info);
  int recycle(PersistentCacheInfo &cache_info);
  int erase(const storage::ExtentId &data_extent_id);
  int write(const common::Slice &extent_data, const PersistentCacheInfo &cache_info);
  int read(PersistentCacheInfo &cache_info,
           util::AIOHandle *aio_handle,
           int64_t offset,
           int64_t size,
           char *buf,
           common::Slice &result);
  inline int get_fd() { return file_->get_fd(); }
  static inline std::string get_file_name()  { return DEFAULT_PERSISTENT_FILE_NAME; }

private:
  inline bool is_free_extent(const storage::ExtentId &extent_id) const { return 1 == free_space_.count(extent_id.id()); }
  inline bool exceed_capacity(const storage::ExtentId &extent_id) const { return extent_id.offset >= extent_count_; }
  int create_cache_file(util::Env *env,
                        const util::EnvOptions &env_options,
                        const std::string &file_path,
                        int64_t cache_size);
  int open_cache_file(util::Env *env,
                      const util::EnvOptions &env_options,
                      const std::string &file_path,
                      int64_t cache_size,
                      int64_t &need_recover_extent_count);
  static void async_recover(void *args);
  void close_file();
  int can_alloc(const storage::ExtentId &data_extent_id);
  int alloc(const storage::ExtentId &data_extent_id, const storage::ExtentId &cache_extent_id);
  int recover_range(int64_t start_offset, int64_t end_offset, const RecoverCacheExtentFunc &recover_func);
  int read_footer(const storage::ExtentId &cache_extent_id, char *buf, table::Footer &footer);
  int erase_by_data_extent_id(const storage::ExtentId &data_extent_id);
  int erase_by_cache_extent_id(const storage::ExtentId &data_extent_id);
  int add_to_stored_stauts(const storage::ExtentId &data_extent_id, const storage::ExtentId &cache_extent_id);

private:
  static const int32_t PERSISTENT_FILE_NUMBER = (INT32_MAX - 1);
  static const int64_t PERSISTENT_UNIQUE_ID = (INT64_MAX - 1);
  static const char *DEFAULT_PERSISTENT_FILE_NAME;
  struct RecoverArguments
  {
    PersistentCacheFile *cache_file_;
    int64_t start_offset_;
    int64_t end_offset_;
    RecoverCacheExtentFunc recover_extent_func_;
    RecoverCallbackFunc recover_callback_func_;

    RecoverArguments();
    ~RecoverArguments();

    void reset();
    bool is_valid() const;

    DECLARE_AND_DEFINE_TO_STRING(KVP_(cache_file), KV_(start_offset), KV_(end_offset))
  };

private:
  bool is_opened_;
  util::Env *env_;
  std::string file_path_;
  util::RandomRWFile *file_;
  int64_t size_;
  int64_t extent_count_;
  char *clear_footer_data_;
  std::mutex space_mutex_;
  std::set<int64_t> free_space_;
  // DataExtentId -> CacheExtentId
  std::unordered_map<int64_t, int64_t> stored_status_;
  // CacheExtentId -> DataExtentId
  std::unordered_map<int64_t, int64_t> reverse_stored_status_;
  static std::atomic<int64_t> recover_job_count_;
  static std::atomic<int64_t> recover_valid_extent_count_;
  static std::atomic<int64_t> recover_invalid_extent_count_;
};

class PersistentCache
{
public:
  static PersistentCache &get_instance();

  int init(util::Env *env,
           const std::string &cache_file_dir,
           int64_t cache_size,
           int64_t thread_count,
           common::PersistentCacheMode mode);
  void destroy();
  int insert(const storage::ExtentId &data_extent_id,
             const common::Slice &extent_data,
             bool write_process,
             cache::Cache::Handle **handle);
  int lookup(const storage::ExtentId &data_extent_id, cache::Cache::Handle *&handle);
  int evict(const storage::ExtentId &data_extent_id);
  int read_from_handle(cache::Cache::Handle *handle,
                       util::AIOHandle *aio_handle,
                       int64_t offset,
                       int64_t size,
                       char *buf,
                       common::Slice &result);
  void release_handle(cache::Cache::Handle *handle);
  PersistentCacheInfo *get_cache_info_from_handle(cache::Cache::Handle *handle);
  inline bool is_enabled() const { return is_inited_; } 

private:
  PersistentCache();
  ~PersistentCache();
  int insert_on_write_process(const storage::ExtentId &data_extent_id, const common::Slice &extent_data, Cache::Handle **handle);
  int insert_on_read_process(const storage::ExtentId &data_extent_id, const common::Slice &extent_data, Cache::Handle **handle);
  int insert_low(const storage::ExtentId &data_extent_id, const common::Slice &extent_data, Cache::Handle **handle);
  int erase(const storage::ExtentId &data_extent_id);
  void generate_cache_key(const storage::ExtentId &data_extent_id, char *buf, common::Slice &cache_key);
  int insert_cache(const storage::ExtentId &data_extent_id,
                   const PersistentCacheInfo &cache_info,
                   cache::Cache::Handle **handle);
  void evict_cache(const storage::ExtentId &data_extent_id);
  int recover_callback();
  int wait_recover_finished();

private:
  static const int64_t MAX_PERSISTENT_CACHEL_KEY_SIZE = 10;

private:
  bool is_inited_;
  std::atomic<bool> is_recovering_;
  common::PersistentCacheMode mode_;
  // TODO(Zhao Dongsheng) : don't use shared_ptr!
  std::shared_ptr<cache::Cache> cache_;
  PersistentCacheFile cache_file_;
};

} // namespace cache
} // namespace smartengine