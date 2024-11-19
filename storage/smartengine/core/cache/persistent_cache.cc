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

#include "cache/persistent_cache.h"
#include "cache/cache_entry.h"
#include "env/io_posix.h"
#include "logger/log_module.h"
#include "storage/io_extent.h"
#include "table/extent_struct.h"
#include "util/aio_wrapper.h"
#include "util/se_constants.h"
#include "util/status.h"

namespace smartengine
{
using namespace cache;
using namespace common;
using namespace memory;
using namespace storage;
using namespace table;
using namespace util;

namespace cache
{

PersistentCacheInfo::PersistentCacheInfo()
    : cache_file_(nullptr),
      data_extent_id_(),
      cache_extent_id_()
{}

PersistentCacheInfo::PersistentCacheInfo(const PersistentCacheInfo &other)
    : cache_file_(other.cache_file_),
      data_extent_id_(other.data_extent_id_),
      cache_extent_id_(other.cache_extent_id_)
{}

PersistentCacheInfo::~PersistentCacheInfo() {}

void PersistentCacheInfo::reset()
{
  cache_file_ = nullptr;
  data_extent_id_.reset();
  cache_extent_id_.reset();
}

bool PersistentCacheInfo::is_valid() const
{
  return IS_NOTNULL(cache_file_) && (data_extent_id_.offset >= 0) &&
         (cache_extent_id_.offset >= 0);
}

void PersistentCacheInfo::delete_entry(const Slice &key, void *value)
{
  assert(IS_NOTNULL(value));
  PersistentCacheInfo *cache_info = reinterpret_cast<PersistentCacheInfo *>(value);
  cache_info->cache_file_->recycle(*cache_info);
  MOD_DELETE_OBJECT(PersistentCacheInfo, cache_info);
}

DEFINE_TO_STRING(PersistentCacheInfo, KVP_(cache_file), KV_(data_extent_id), KV_(cache_extent_id))

PersistentCacheFile::RecoverArguments::RecoverArguments()
    : cache_file_(nullptr),
      start_offset_(0),
      end_offset_(0),
      recover_extent_func_(),
      recover_callback_func_()
{}

PersistentCacheFile::RecoverArguments::~RecoverArguments() {}

void PersistentCacheFile::RecoverArguments::reset()
{
  cache_file_ = nullptr;
  start_offset_ = 0;
  end_offset_ = 0;
}

bool PersistentCacheFile::RecoverArguments::is_valid() const
{
  return IS_NOTNULL(cache_file_) && start_offset_ >= 0 &&
         end_offset_ > 0 && end_offset_ > start_offset_;
}

const char *PersistentCacheFile::DEFAULT_PERSISTENT_FILE_NAME = "SMARTENGINE_PERSISTENT_CACHE";

std::atomic<int64_t> PersistentCacheFile::recover_job_count_(0);
std::atomic<int64_t> PersistentCacheFile::recover_valid_extent_count_(0);
std::atomic<int64_t> PersistentCacheFile::recover_invalid_extent_count_(0);

PersistentCacheFile::PersistentCacheFile()
    : is_opened_(false),
      env_(nullptr),
      file_path_(),
      file_(nullptr),
      size_(0),
      extent_count_(0),
      clear_footer_data_(nullptr),
      space_mutex_(),
      free_space_(),
      stored_status_(),
      reverse_stored_status_()
{}

PersistentCacheFile::~PersistentCacheFile()
{
  close();
}

int PersistentCacheFile::open(Env *env,
                              const std::string &file_path,
                              int64_t cache_size,
                              int64_t &need_recover_extent_count)
{
  int ret = Status::kOk;
  EnvOptions env_options;
  env_options.use_direct_reads = true;
  env_options.use_direct_writes = true;

  if (UNLIKELY(is_opened_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "the persistent cache file has been opened", K(ret), K(file_path));
  } else if (IS_NULL(env) || UNLIKELY(file_path.empty()) ||
             UNLIKELY(cache_size < storage::MAX_EXTENT_SIZE) || 
             UNLIKELY(0 != (cache_size % storage::MAX_EXTENT_SIZE))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(file_path), K(cache_size));
  } else if (FAILED(env->FileExists(file_path).code())) {
    // The cache file isn't exist, need create.And it will overwrite ret.
    if (Status::kNotFound != ret) {
      SE_LOG(WARN, "fail to check file exist", K(ret), K(file_path), K(cache_size));
    } else if (FAILED(create_cache_file(env, env_options, file_path, cache_size))) {
      SE_LOG(WARN, "fail to create new cache file", K(ret), K(file_path), K(cache_size)); 
    } else {
      need_recover_extent_count = 0;
      SE_LOG(INFO, "success to create new cache file", K(file_path), K(cache_size));
    }
  } else {
    // The cache file exists, need recovery the cache info. 
    if (FAILED(open_cache_file(env, env_options, file_path, cache_size, need_recover_extent_count))) {
      SE_LOG(WARN, "fail to open cache file", K(ret), K(file_path), K(cache_size));
    } else {
      SE_LOG(INFO, "success to open cache file", K(file_path), K(cache_size));
    }
  }

  if (SUCCED(ret)) {
    if (IS_NULL(clear_footer_data_ = reinterpret_cast<char *>(base_malloc(
        DIOHelper::DIO_ALIGN_SIZE, ModId::kPersistentCache)))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for clear footer data", K(ret));
    } else {
      memset(clear_footer_data_, 0, DIOHelper::DIO_ALIGN_SIZE);
      env_ = env;
      file_path_ = file_path;
      size_ = cache_size;
      extent_count_ = size_ / storage::MAX_EXTENT_SIZE;

      se_assert(free_space_.empty());
      for (int32_t offset = 0; SUCCED(ret) && offset < extent_count_; ++offset) {
        ExtentId extent_id(PERSISTENT_FILE_NUMBER, offset);
        if (!(free_space_.emplace(extent_id.id()).second)) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "fail to emplace to free space", K(ret), K(extent_id));
        }
      }

      if (SUCCED(ret)) {
        is_opened_ = true;
      }
    }
  }

  // resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(clear_footer_data_)) {
      base_free(clear_footer_data_);
      clear_footer_data_ = nullptr;
    }
  }

  return ret;
}

int PersistentCacheFile::recover(int64_t extent_count,
                                 int64_t parallel_job_count,
                                 const RecoverCacheExtentFunc &recover_extent_func,
                                 const RecoverCallbackFunc &recover_callback_func)
{
  int ret = Status::kOk;
  RecoverArguments *args = nullptr;
  int64_t single_job_size = 0;

  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCacheFile has not been opened", K(ret));
  } else if (UNLIKELY(extent_count <= 0) || UNLIKELY(parallel_job_count <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(extent_count), K(parallel_job_count));
  } else {
    recover_job_count_.store(parallel_job_count);
    single_job_size = (extent_count + parallel_job_count - 1) / parallel_job_count;
    env_->SetBackgroundThreads(parallel_job_count, Env::Priority::RECOVER_PERSISTENT_CACHE);
    for (int64_t start_offset = 0; start_offset < extent_count; start_offset += single_job_size) {
      if (IS_NULL(args = MOD_NEW_OBJECT(ModId::kPersistentCache, RecoverArguments))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for recover arguments", K(ret));
      } else {
        args->cache_file_ = this;
        args->start_offset_ = start_offset;
        args->end_offset_ = std::min(start_offset + single_job_size, extent_count);
        args->recover_extent_func_ = recover_extent_func;
        args->recover_callback_func_ = recover_callback_func;

        env_->Schedule(async_recover, args, Env::Priority::RECOVER_PERSISTENT_CACHE, nullptr);
      }
    }
    SE_LOG(INFO, "success to schedule persistent cache recover jobs", K(extent_count), K(parallel_job_count));
  }

  return ret;
}

void PersistentCacheFile::close()
{
  if (is_opened_) {
    env_ = nullptr;
    file_path_.clear();
    close_file();
    base_free(clear_footer_data_);
    clear_footer_data_ = nullptr;
    size_ = 0;
    extent_count_ = 0;
    free_space_.clear();
    stored_status_.clear();
    reverse_stored_status_.clear();
    recover_job_count_.store(0);
    recover_valid_extent_count_.store(0);
    recover_invalid_extent_count_.store(0);
    is_opened_ = false;
  }
}

// ExtentId is reused multiple times, which can lead to multiple versions of data corresponding
// to a single ExtentId.To ensure data consistency after recovering from the persistent cache,
// there are two potential approaches:
//
// 1. Allow multiple versions of data for a particular ExtentId to exist in the persistent cache.
// During recover, only the latest version is retained. However, this approach has two main issues:
// 1) A version management mechanism is required to identify different versions of the same ExtentId.
// 2) Since the timing of garbage collection and subsequent writes is unpredictable, a newer version
// of the data might be overwritten after being reclaimed, while the space occupied by old versions
// remains untouched.As a result, a system crash and recovery may only restore outdated data.
//
// 2. Ensure that PersistentCacheFile only contains the latest version of data for each ExtentId at
// all times.This approach avoids the need for a version management mechanism and prevents the risk of
// restoring outdated data after a crash, though it requires stricter control over cache content updates.
//
// After considering both approaches for maintaining data consistency, we choose the second approach:
// ensuring that PersistentCacheFile contains only the latest version of data for each ExtentId(A more
// thorough future solution would involve ensuring ExtentIds are unique to avoid versioning issues entirely.)
// The key to implementing the second approach lies in erasing old version data before writing new versions.
// There are two potential points for erasion: 
// 1) During LRU cache recycling. 
// 2) During the write operation for the new version.
// We opt to erase old data during the write operation for two main reasons:
// 1) LRU cache recycling also occurs at shutdown, when data erasion should not take place.
// 2) Performing erasion at the time of writing makes the logic more cohesive and clearer, As
// it keeps version management localized within the write operation, reducing complexity.
// 
// Before writing a new version of data for a given ExtentId, we must ensure any previous version is erased.
// The erasion occurs even if there is not enough space in PersistentCacheFile for the new versin data, in this
// case, the PersistentCacheFile will not contain any version data of this extent.Additionally, when writing any
// extent, we must clear the target location to avoid mistakenly erasing data for other ExtentIds in previous step.
// For example:
// Suppose ExtentId (1, 1) is written to position 1 in PersistentCacheFile, and later recalimed.
// ExtentId (1, 2) is then written to the same position, and subsequently, ExtentId (1, 1) attempts
// to write again. If ExtentId (1, 1) were to erase the cache for ExtentId (1, 2) mistakenly, it would
// compromise data integrity.
// An alternative approach would be to remove only the mapping of the data extent id to the cache extent id, 
// rather than performing an actual erasion. However, this is insufficient, as it introduces a failure risk:
// If the write of ExtentId (1, 2) fails, PersistentCacheFile might still retain old version data for ExtentId
// (1, 1). When writing a new version of ExtentId (1, 1) again the system would assume ExtentId (1, 1) has been
// erased, potentially resulting in PersistentCacheFile containing multiple versions of ExtentId (1,1).
// Thus, to prevent data inconsistency, it is essential to fully erase any previous data for an ExtentId before
// writing its new version.

// Forced deletion of the current data extent ID occurs in the write process to ensure that older versions are
// fully cleared before new data is written. Deleting in the read process is permitted when the previous cache
// extent which stored the data extent is freed, otherwise concurrent loading of the same data extent ID could
// lead to issues: if one process forcibly deletes the extent while another is loading it, this could result in
// read errors or data inconsistency. Therefore, deletion is restricted to the write path to maintain data integrity.
int PersistentCacheFile::alloc(const storage::ExtentId &data_extent_id, PersistentCacheInfo &cache_info)
{
  int ret = Status::kOk;
  int64_t erase_count = 0;
  ExtentId cache_extent_id;

  std::lock_guard<std::mutex> guard(space_mutex_);
  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCacheFile has not been opened", K(ret));
  } else if (FAILED(can_alloc(data_extent_id))) {
    if (Status::kNoSpace != ret) {
      SE_LOG(WARN, "fail to check if can alloc", K(ret), K(data_extent_id));
    }
  } else if (UNLIKELY(free_space_.empty())) {
    ret = Status::kNoSpace;
    SE_LOG(DEBUG, "the PersistentCacheFile has no space", K(data_extent_id));
  } else {
    cache_extent_id = ExtentId(*(free_space_.begin()));
    if (1 != (erase_count = free_space_.erase(cache_extent_id.id()))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to erase from free space", K(ret), K(data_extent_id), K(cache_extent_id), K(erase_count));
    } else if (FAILED(erase_by_cache_extent_id(cache_extent_id))) {
      SE_LOG(WARN, "fail to erase by cache extent id", K(ret), K(data_extent_id), K(cache_extent_id));
    } else if (FAILED(add_to_stored_stauts(data_extent_id, cache_extent_id))) {
      SE_LOG(WARN, "fail to add to stored status", K(ret), K(data_extent_id), K(cache_extent_id));  
    } else {
      cache_info.data_extent_id_ = data_extent_id;
      cache_info.cache_extent_id_ = cache_extent_id;
      cache_info.cache_file_ = this;
      assert(!exceed_capacity(cache_info.cache_extent_id_));
      SE_LOG(INFO, "success to alloc cache space", K(cache_extent_id));
    }
  }

  return ret;
}

int PersistentCacheFile::erase(const ExtentId &data_extent_id)
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(space_mutex_);
  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCacheFile has not been opened", K(ret));
  } else if (FAILED(erase_by_data_extent_id(data_extent_id))) {
    SE_LOG(WARN, "fail to erase from stored status", K(ret), K(data_extent_id));
  } else {
    SE_LOG(DEBUG, "success to erase cache extent", K(data_extent_id));
  }

  return ret;
}

// During crash recovery, recycling may still be  triggered if cache entries are unevenly
// distributed across shards. This imbalance can cause one of the cache shards to reach its
// limit, activating the recycle function. The recycle function then adds reclaimed entries
// to free_space.
int PersistentCacheFile::recycle(PersistentCacheInfo &cache_info)
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(space_mutex_);
  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCacheFile has not been opened", K(ret));
  } else if (UNLIKELY(!cache_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(cache_info));
  } else if (UNLIKELY(exceed_capacity(cache_info.cache_extent_id_))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "the read range exceed cache file capacity", K(ret), K(cache_info));
  } else if (!(free_space_.emplace(cache_info.cache_extent_id_.id()).second)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the free space is corrupted", K(ret), K(cache_info));
  } else {
    SE_LOG(INFO, "sucess to recycle cache exetent", K(cache_info));
  }

  return ret;
}

int PersistentCacheFile::write(const common::Slice &extent_data, const PersistentCacheInfo &cache_info)
{
  int ret = Status::kOk;
  FileIOExtent extent;

  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the cache file has not been opened", K(ret));
  } else if (UNLIKELY(extent_data.empty()) || UNLIKELY(!cache_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(extent_data), K(cache_info));
  } else if (FAILED(extent.init(cache_info.cache_extent_id_, PERSISTENT_UNIQUE_ID, cache_info.cache_file_->get_fd()))) {
    SE_LOG(WARN, "fail to init extent", K(ret), K(cache_info));
  } else if (FAILED(extent.write(extent_data, 0 /*offset*/))) {
    SE_LOG(WARN, "fail to write extent", K(ret), K(cache_info));
  } else {
    SE_LOG(INFO, "success to write extent", K(cache_info));
  }

  return ret;
}

int PersistentCacheFile::read(PersistentCacheInfo &cache_info,
                              AIOHandle *aio_handle,
                              int64_t offset,
                              int64_t size,
                              char *buf,
                              Slice &result)
{
  int ret = Status::kOk;
  FileIOExtent extent;

  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCacheFile has not been opened", K(ret));
  } else if (UNLIKELY(!cache_info.is_valid()) || UNLIKELY(offset < 0) ||
             UNLIKELY(size <= 0) || UNLIKELY(IS_NULL(buf))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret),
        K(cache_info), K(offset), K(size), KP(buf));
  } else if (UNLIKELY(exceed_capacity(cache_info.cache_extent_id_))) {
    ret = Status::kIOError;
    SE_LOG(ERROR, "the read range exceed cache file capacity", K(ret), K(cache_info));
  } else if (FAILED(extent.init(cache_info.cache_extent_id_, PERSISTENT_UNIQUE_ID, file_->get_fd()))) {
    SE_LOG(WARN, "fail to init extent", K(ret), K(cache_info));
  } else if (FAILED(extent.read(aio_handle, offset, size, buf, result))) {
    SE_LOG(WARN, "fail to read from cache file extent", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int PersistentCacheFile::create_cache_file(util::Env *env,
                                           const util::EnvOptions &env_options,
                                           const std::string &file_path,
                                           int64_t cache_size)
{
  int ret = Status::kOk;

  if (IS_NULL(env) || UNLIKELY(file_path.empty()) ||
      UNLIKELY(cache_size < storage::MAX_EXTENT_SIZE) ||
      UNLIKELY(0 != (cache_size % storage::MAX_EXTENT_SIZE))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(file_path), K(cache_size));
  } else if (SUCCED(env->FileExists(file_path).code())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the cache file must not exist", K(ret), K(file_path));
  } else if (FAILED(env->NewRandomRWFile(file_path, file_, env_options).code())) {
    SE_LOG(WARN, "fail to create cache file", K(ret), K(file_path));
  } else if (FAILED(file_->fallocate(0 /*mode*/, 0, cache_size))) {
    SE_LOG(WARN, "fail to fallocate cache file", K(ret), K(file_path), K(cache_size));
  } else {
    SE_LOG(INFO, "success to create cache file", K(file_path), K(cache_size));
  }

  // If it failed, close th file.
  if (FAILED(ret)) {
    close_file();
  }

  return ret;
}

int PersistentCacheFile::open_cache_file(util::Env *env,
                                         const util::EnvOptions &env_options,
                                         const std::string &file_path,
                                         int64_t cache_size,
                                         int64_t &need_recover_extent_count)
{
  int ret = Status::kOk;
  uint64_t curr_file_size = 0;

  if (IS_NULL(env) || UNLIKELY(file_path.empty()) ||
      UNLIKELY(cache_size < storage::MAX_EXTENT_SIZE) ||
      UNLIKELY(0 != (cache_size % storage::MAX_EXTENT_SIZE))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(file_path), K(cache_size));
  } else if (FAILED(env->FileExists(file_path).code())) {
    SE_LOG(WARN, "the cache file must exist", K(ret), K(file_path));
  } else if (FAILED(env->NewRandomRWFile(file_path, file_, env_options).code())) {
    SE_LOG(WARN, "fail to open cache file", K(ret), K(file_path));
  } else if (FAILED(env->GetFileSize(file_path, &curr_file_size).code())) {
    SE_LOG(WARN, "fail to get cache file size", K(ret), K(file_path));
  } else if (UNLIKELY(curr_file_size < storage::MAX_EXTENT_SIZE) ||
             UNLIKELY(0 != (curr_file_size % storage::MAX_EXTENT_SIZE))) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "the cache file maybe corrupted", K(ret), K(file_path), K(curr_file_size));
  } else {
    if (cache_size > static_cast<int64_t>(curr_file_size)) {
      if (FAILED(file_->fallocate(0 /*mode*/, curr_file_size, cache_size - curr_file_size))) {
        SE_LOG(WARN, "fail to extend persistent cache file", K(ret), K(curr_file_size), K(cache_size));
      } else {
        SE_LOG(INFO, "success to extend the persistent cache file", K(file_path), K(curr_file_size), K(cache_size));
      }
    } else if (cache_size < static_cast<int64_t>(curr_file_size)) {
      if (FAILED(file_->ftruncate(cache_size))) {
        SE_LOG(WARN, "fail to shrink persistent cache file", K(ret), K(curr_file_size), K(cache_size));
      } else {
        SE_LOG(INFO, "success to shrink the persistent cache file", K(file_path), K(curr_file_size), K(cache_size));
      }
    } else {
      SE_LOG(INFO, "the persistent cache file size don't change", K(cache_size));
    }

    if (SUCCED(ret)) {
      need_recover_extent_count = std::min(cache_size, static_cast<int64_t>(curr_file_size)) / storage::MAX_EXTENT_SIZE;
      SE_LOG(INFO, "success to open cache file", K(cache_size));
    }
  }
  
  // If it failed, close th file.
  if (FAILED(ret)) {
    close_file();
  }

  return ret;
}

void PersistentCacheFile::async_recover(void *args)
{
  int ret = Status::kOk;
  RecoverArguments *recover_args = nullptr;

  if (IS_NULL(recover_args = reinterpret_cast<RecoverArguments *>(args))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(args), KP(recover_args)); 
  } else if (UNLIKELY(!recover_args->is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(*recover_args));
  } else if (FAILED(recover_args->cache_file_->recover_range(recover_args->start_offset_,
                                                             recover_args->end_offset_,
                                                             recover_args->recover_extent_func_))) {
    SE_LOG(WARN, "fail to recover a range of cache extents", K(ret), K(*recover_args));
  } else {
    if (1 == (recover_job_count_.fetch_sub(1))) {
      if (FAILED(recover_args->recover_callback_func_())) {
        SE_LOG(WARN, "fail to run recover callback function", K(ret), K(*recover_args));
      } else {
        SE_LOG(INFO, "success to execute the last recovery persistent cache job", K(&recover_args),
            K_(recover_valid_extent_count), K_(recover_invalid_extent_count));
      }
    } else {
      SE_LOG(INFO, "success to execute one recovery persistent cache job", K(*recover_args));
    }
  }

  MOD_DELETE_OBJECT(RecoverArguments, recover_args);

  se_assert(SUCCED(ret));
}

void PersistentCacheFile::close_file()
{
  if (IS_NOTNULL(file_)) {
    PosixRandomRWFile *file_ptr = reinterpret_cast<PosixRandomRWFile *>(file_);
    MOD_DELETE_OBJECT(PosixRandomRWFile, file_ptr);
    file_ = nullptr;
  }
}

int PersistentCacheFile::can_alloc(const ExtentId &data_extent_id)
{
  int ret = Status::kOk;
  ExtentId cache_extent_id;

  auto iter  = stored_status_.find(data_extent_id.id());
  if (stored_status_.end() != iter) {
    cache_extent_id.set(iter->second);
    if (is_free_extent(cache_extent_id)) {
      se_assert(data_extent_id.id() == reverse_stored_status_[cache_extent_id.id()]);
      // The data extent have been recycled, but not erased because the data specific
      // data extent and cache extent have not been writed again.It should erase it here
      // for follow writing.
      if (FAILED(erase_by_data_extent_id(data_extent_id))) {
        SE_LOG(WARN, "fail to erase by data extent id", K(ret), K(data_extent_id), K(cache_extent_id));
      }
    } else {
      // The data extent have been writed into cache and haven't been recycled, there
      // is no space to store it.In the future, the upper layer could lookup from persistent
      // cache directly to increase cache hits rate in some case.
      ret = Status::kNoSpace;
    }
  }

  return ret;
}

int PersistentCacheFile::alloc(const ExtentId &data_extent_id, const ExtentId &cache_extent_id)
{
  int ret = Status::kOk;
  int64_t erase_count = 0;

  std::lock_guard<std::mutex> guard(space_mutex_);
  if (1 != (erase_count = free_space_.erase(cache_extent_id.id()))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the cache space has been alloced", K(ret), K(erase_count), K(data_extent_id), K(cache_extent_id));
  } else if (FAILED(add_to_stored_stauts(data_extent_id, cache_extent_id))) {
    SE_LOG(WARN, "fail to add to stored status", K(ret), K(data_extent_id), K(cache_extent_id));
  } else {
#ifndef NDEBUG
    SE_LOG(INFO, "success to alloc", K(data_extent_id), K(cache_extent_id));
#endif
  }

  return ret;
}

int PersistentCacheFile::recover_range(int64_t start_offset, int64_t end_offset, const RecoverCacheExtentFunc &recover_func)
{
  int ret = Status::kOk;
  char *footer_buf = nullptr;
  Footer footer;
  PersistentCacheInfo cache_info;

  if (UNLIKELY(start_offset < 0) || UNLIKELY(end_offset <= 0) || UNLIKELY(start_offset >= end_offset)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(start_offset), K(end_offset));
  } else if (IS_NULL(footer_buf = reinterpret_cast<char *>(base_malloc(Footer::get_max_serialize_size(), ModId::kPersistentCache)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for footer buf", K(ret));
  } else {
    cache_info.cache_file_ = this;
    cache_info.cache_extent_id_.file_number = PERSISTENT_FILE_NUMBER;
    for (int32_t offset = start_offset; SUCCED(ret) && offset < end_offset; ++offset) {
      footer.reset();
      memset(footer_buf, 0, Footer::get_max_serialize_size());
      cache_info.cache_extent_id_.offset = offset;
      if (FAILED(read_footer(cache_info.cache_extent_id_, footer_buf, footer))) {
        SE_LOG(WARN, "fail to read footer", K(ret), K(cache_info));
      } else if (Footer::FOOTER_MAGIC_NUMBER != footer.magic_numer_) {
        ++recover_invalid_extent_count_;
#ifndef NDEBUG
        SE_LOG(INFO, "ignore the extent", "cache_extent_id", cache_info.cache_extent_id_, K(footer));
#endif
      } else {
        cache_info.data_extent_id_ = footer.get_extent_id();
        if (FAILED(alloc(cache_info.data_extent_id_, cache_info.cache_extent_id_))) {
          SE_LOG(WARN, "fail to alloc cache file space", K(ret), K(footer), K(cache_info));
        } else if (FAILED(recover_func(cache_info.data_extent_id_, cache_info, nullptr))) {
          SE_LOG(WARN, "fail to recover extent", K(ret), K(footer), K(cache_info));
        } else {
            ++recover_valid_extent_count_;
#ifndef NDEBUG
            SE_LOG(INFO, "success to recover one extent", K(footer), K(cache_info));
#endif
        }
      }
    }
  }

  if (IS_NOTNULL(footer_buf)) {
    base_free(footer_buf);
    footer_buf = nullptr;
  }

  return ret;
}

int PersistentCacheFile::read_footer(const ExtentId &cache_extent_id, char *buf, Footer &footer)
{
  int ret = Status::kOk;
  int64_t offset = storage::MAX_EXTENT_SIZE - Footer::get_max_serialize_size();
  int64_t size = Footer::get_max_serialize_size();
  Slice result;
  int64_t pos = 0;
  FileIOExtent extent;

  if (IS_NULL(buf)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(cache_extent_id));
  } else if (FAILED(extent.init(cache_extent_id, PERSISTENT_UNIQUE_ID, file_->get_fd()))) {
    SE_LOG(WARN, "fail to init cache extent", K(ret), K(cache_extent_id));
  } else if (FAILED(extent.read(nullptr, offset, size, buf, result))) {
    SE_LOG(WARN, "fail to read footer content", K(ret), K(cache_extent_id));
  } else if (FAILED(footer.deserialize(buf, size, pos))) {
    SE_LOG(WARN, "fail to deserialize footer", K(ret), K(cache_extent_id));
  } else {
    // succeed
  }

  return ret;
}

int PersistentCacheFile::add_to_stored_stauts(const ExtentId &data_extent_id, const ExtentId &cache_extent_id)
{
  int ret = Status::kOk;

  auto iter = stored_status_.find(data_extent_id.id());
  if (stored_status_.end() != iter) {
    ret = Status::kCorruption;
    SE_LOG(ERROR, "the staled cache data has not been cleared", K(ret), K(data_extent_id),
        K(cache_extent_id), "staled_cache_extent_id", ExtentId(iter->second));
  } else if (!(stored_status_.emplace(data_extent_id.id(), cache_extent_id.id()).second)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to emplace to stored status", K(ret), K(data_extent_id), K(cache_extent_id));
  } else if (!(reverse_stored_status_.emplace(cache_extent_id.id(), data_extent_id.id()).second)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to emplace to reverse stored status", K(ret), K(data_extent_id), K(cache_extent_id));
  } else {
    SE_LOG(INFO, "success to emplace to stored status", K(data_extent_id), K(cache_extent_id));
  }

  return ret;
}

int PersistentCacheFile::erase_by_data_extent_id(const ExtentId &data_extent_id)
{
  int ret = Status::kOk;
  int64_t erase_count = 0;
  ExtentId cache_extent_id;
  FileIOExtent cache_extent;

  auto iter = stored_status_.find(data_extent_id.id());
  if (stored_status_.end() != iter) {
    cache_extent_id.set(iter->second);
    if (FAILED(cache_extent.init(cache_extent_id, PERSISTENT_UNIQUE_ID, file_->get_fd()))) {
      SE_LOG(WARN, "fail to init cache extent", K(ret), K(data_extent_id), K(cache_extent_id));
    } else if (FAILED(cache_extent.write(Slice(clear_footer_data_, DIOHelper::DIO_ALIGN_SIZE), storage::MAX_EXTENT_SIZE - DIOHelper::DIO_ALIGN_SIZE))) {
      SE_LOG(WARN, "fail to write clear cache extent data", K(ret), K(data_extent_id), K(cache_extent_id));
    } else if (1 != (erase_count = stored_status_.erase(data_extent_id.id()))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to erase from stored status", K(ret), K(data_extent_id), K(cache_extent_id));
    } else if (1 != (erase_count = reverse_stored_status_.erase(cache_extent_id.id()))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to erase from reverse stored status", K(ret), K(data_extent_id), K(cache_extent_id));
    } else {
      SE_LOG(INFO, "sucess to erase staled cache data", K(data_extent_id), K(cache_extent_id));
    }
  }

  return ret;
}

int PersistentCacheFile::erase_by_cache_extent_id(const ExtentId &cache_extent_id)
{
  int ret = Status::kOk;
  ExtentId data_extent_id;

  auto iter = reverse_stored_status_.find(cache_extent_id.id());
  if (reverse_stored_status_.end() != iter) {
    data_extent_id.set(iter->second);
    if (FAILED(erase_by_data_extent_id(data_extent_id))) {
      SE_LOG(WARN, "fail to erase by data extent id", K(ret), K(data_extent_id), K(cache_extent_id));
    } else {
#ifndef NDEBUG
      SE_LOG(INFO, "success to erase by cache extent id", K(data_extent_id), K(cache_extent_id));
#endif
    }
  }

  return ret;
}

PersistentCache::PersistentCache()
    : is_inited_(false),
      is_recovering_(false),
      mode_(kReadWriteThrough),
      cache_(nullptr),
      cache_file_()
{}

PersistentCache::~PersistentCache()
{
  destroy();
}

PersistentCache &PersistentCache::get_instance()
{
  static PersistentCache persistent_cache;
  return persistent_cache;
}

int PersistentCache::init(Env *env,
                          const std::string &cache_file_dir,
                          int64_t cache_size,
                          int64_t thread_count,
                          PersistentCacheMode mode)
{
  int ret = Status::kOk;
  int64_t actual_cache_size = ((cache_size + storage::MAX_EXTENT_SIZE -1) / storage::MAX_EXTENT_SIZE) * storage::MAX_EXTENT_SIZE;
  int64_t need_recover_extent_count = 0;
  int64_t cache_shard_bits = 0;
  int64_t cache_capacity = 0;
  std::string cache_file_path = cache_file_dir + "/" + PersistentCacheFile::get_file_name();

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "the PersistentCache has been inited", K(ret));
  } else if (UNLIKELY(IS_NULL(env)) || UNLIKELY(cache_file_dir.empty()) ||
             UNLIKELY(cache_size < 0) || UNLIKELY(thread_count <= 0) ||
             UNLIKELY(mode < common::kReadWriteThrough) || UNLIKELY(mode >= kMaxPersistentCacheMode)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(cache_file_dir), K(cache_size), K(thread_count), KE(mode));
  } else if (0 == actual_cache_size) {
    SE_LOG(INFO, "the PersistentCache is disabled");
    if (FAILED(env->FileExists(cache_file_path).code())) {
      if (Status::kNotFound != ret) {
        SE_LOG(WARN, "fail to check if the persistent cache file exists", K(ret), K(cache_file_path));
      } else {
        // Don't have staled persistent cache file, overwrite ret here.
        ret = Status::kOk;
      }
    } else if (FAILED(env->DeleteFile(cache_file_path).code())) {
      SE_LOG(WARN, "fail to delete staled persistent cache file", K(ret), K(cache_file_path));
    } else {
      SE_LOG(INFO, "success to delete staled persistent cache file", K(cache_file_path));
    }
  } else {
    // The core of PersistentCache includes two components: an LRU cache and a PersistentCacheFile.
    // The PersistentCacheFile functions as virtual memory for the in-memory cache.Therefore, the
    // actual space of the PersistentCacheFile must be greater than the capacity of the LRU cache.
    // If the LRU cache capacity exceeds that of the PersistentCacheFile, new entries may fail to 
    // allocate space from the PersistentCacheFile.In this case, data in the LRU cache will not be 
    // evicted, preventing the release of PersistentCacheFile space. This leads to inefficiend use of 
    // the PersistentCache, as the system is unable to make room for new entries by freeing space from
    // the PersistentCacheFile. 
    //
    // So we set the LRU cache capacity slightly smaller than the PersistentCacheFileby one extent.
    // This ensures that when new writes are made, there is sufficient room for allocation in the
    // PersistentCacheFile.With this adjustment, the LRU cache can evict entries effectively, freeing
    // up space in the PersistentCacheFile and maintaining a higher utilization rate for the cache.

    // Another considerration is the configuration of cache shards.For PersistentCache, 128 cache shards
    // are typically sufficient.However, if the PersistentCacheFile size if less than 128 extents, the
    // average capacity per cache shard would be less than the size of a single extent.This scenario can
    // lead to situations where a cache shard cannot store any extent, especially if the handle is nullptr.
    // Although this is a corner case, it still needs to be addressed,Therefore, when the PersistentCache
    // size is less than 1GB(A more conservative number compared to 128 * 2), we only configure a single 
    // cache shard.
    cache_shard_bits = (actual_cache_size >= (storage::MAX_EXTENT_SIZE * 512)) ? 7 : 0;
    cache_capacity = actual_cache_size - storage::MAX_EXTENT_SIZE;
    if (IS_NULL(cache_ = NewLRUCache(cache_capacity,
                                     cache_shard_bits,
                                     false /*strict_capacity_limit*/,
                                     0.0 /*high_pri_pool_ratio*/,
                                     0.0 /*old_pool_ratio*/,
                                     memory::ModId::kPersistentCache))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to init lru cache instance for persistent cache", K(ret),
          K(actual_cache_size), K(cache_capacity), K(cache_shard_bits));
    } else if (FAILED(cache_file_.open(env,
                                       cache_file_path,
                                       actual_cache_size,
                                       need_recover_extent_count))) { 
      SE_LOG(WARN, "fail to open persistent cache file", K(ret), KP(env), K(cache_file_path), K(actual_cache_size));
    } else if (need_recover_extent_count > 0) {
      is_recovering_.store(true);
      auto recover_extent_func = std::bind(&PersistentCache::insert_cache, this,
          std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
      auto recover_callback_func = std::bind(&PersistentCache::recover_callback, this);
      if (FAILED(cache_file_.recover(need_recover_extent_count,
                                      thread_count,
                                      recover_extent_func,
                                      recover_callback_func))) {
        SE_LOG(WARN, "fail to recover cache file", K(ret));
      }
    }

    if (SUCCED(ret)) {
      mode_ = mode;
      is_inited_ = true;
      SE_LOG(INFO, "success to enable PersistentCache", K(cache_file_path), K(cache_size), KE(mode));
    }
  }

  // Resource clean
  if (FAILED(ret)) {
    cache_.reset();
    cache_file_.close();
  }

  return ret;
}

void PersistentCache::destroy()
{
  if (is_inited_) {
    cache_->DisownData();
    cache_->destroy();
    cache_.reset();
    cache_file_.close();
    is_recovering_.store(false);
    is_inited_ = false;
  }
}

int PersistentCache::insert(const ExtentId &data_extent_id,
                            const Slice &extent_data,
                            bool write_process,
                            Cache::Handle **handle)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCache has not been inited", K(ret));
  } else if (UNLIKELY(extent_data.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(extent_data));
  } else if (FAILED(wait_recover_finished())) {
    SE_LOG(WARN, "fail to wait recover finished", K(ret));
  } else {
    if (write_process) {
      if (FAILED(insert_on_write_process(data_extent_id, extent_data, handle))) {
        if (Status::kNoSpace != ret) {
          SE_LOG(WARN, "fail to insert on write process", K(ret), K(data_extent_id));
        }
      } else {
#ifndef NDEBUG
        SE_LOG(INFO, "success to insert on write process", K(data_extent_id));
#endif
      }
    } else {
      if (FAILED(insert_on_read_process(data_extent_id, extent_data, handle))) {
        if (Status::kNoSpace != ret) {
          SE_LOG(WARN, "fail to insert on read process", K(ret), K(data_extent_id));
        }
      } else {
#ifndef NDEBUG
        SE_LOG(INFO, "success to insert on read process", K(data_extent_id));
#endif
      }
    }
  }

  return ret;
}

int PersistentCache::lookup(const storage::ExtentId &data_extent_id, Cache::Handle *&handle)
{
  int ret = Status::kOk;
  char cache_key_buf[MAX_PERSISTENT_CACHEL_KEY_SIZE] = {0};
  Slice cache_key;
  generate_cache_key(data_extent_id, cache_key_buf, cache_key);

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "PersistentCache has not been inited", K(ret));
  } else if (is_recovering_.load()) {
    handle = nullptr;
    ret = Status::kNotFound;
  } else {
    //se_assert(staled_entries_during_recovring_.empty());
    if (IS_NULL(handle = cache_->Lookup(cache_key))) {
      ret = Status::kNotFound;
    }
  }

  return ret;
}

int PersistentCache::evict(const storage::ExtentId &data_extent_id)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "PersistentCache has not been inited", K(ret));
  } else if (is_recovering_.load()) {
    // do nothing here.
  } else {
    evict_cache(data_extent_id);
  }

  return ret;
}
int PersistentCache::read_from_handle(Cache::Handle *handle,
                                      AIOHandle *aio_handle,
                                      int64_t offset,
                                      int64_t size,
                                      char *buf,
                                      common::Slice &result)
{
  int ret = Status::kOk;
  PersistentCacheInfo *cache_info = nullptr;
  se_assert(!is_recovering_.load());

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCache has not been inited", K(ret));
  } else if (UNLIKELY(IS_NULL(handle)) || UNLIKELY(offset < 0) ||
             UNLIKELY(size <= 0) || UNLIKELY(IS_NULL(buf))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret),
        KP(handle), K(offset), K(size), KP(buf));
  } else if (IS_NULL(cache_info = get_cache_info_from_handle(handle))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the cache info must not be nullptr", K(ret), KP(handle));
  } else if (UNLIKELY(!cache_info->is_valid())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the cache info is invalid", K(ret), K(*cache_info));
  } else if (FAILED(cache_info->cache_file_->read(*cache_info, aio_handle, offset, size, buf, result))) {
    SE_LOG(WARN, "fail to read from cache file", K(ret));
  } else {
    SE_LOG(DEBUG, "success to read from persistent cache handle", K(*cache_info), K(offset), K(size));
  }

  return ret;
}

void PersistentCache::release_handle(Cache::Handle *handle)
{
  assert(is_inited_ && !is_recovering_.load());
  cache_->Release(handle);
}

PersistentCacheInfo *PersistentCache::get_cache_info_from_handle(Cache::Handle *handle)
{
  assert(is_inited_ && !is_recovering_.load());
  return reinterpret_cast<PersistentCacheInfo *>(cache_->Value(handle));
}

// During the write process, old versions of the data extent must be erased regardless of the
// persistent cache mode.In kReadWriteThrough mode, it also attempts to write the new version
// of the data extent to the persistent cache.In kReadThrough mode, only the erasion of old
// version occurs without writing the new data to the persistent cache.
int PersistentCache::insert_on_write_process(const ExtentId &data_extent_id, const Slice &extent_data, Cache::Handle **handle)
{
  int ret = Status::kOk;

  if (UNLIKELY(extent_data.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(data_extent_id), K(extent_data));
  } else if (FAILED(erase(data_extent_id))) {
    SE_LOG(WARN, "fail to erase data extent id", K(ret), K(data_extent_id));
  } else {
    if (kReadWriteThrough == mode_) {
      if (FAILED(insert_low(data_extent_id, extent_data, handle))) {
        if (Status::kNoSpace != ret) {
          SE_LOG(WARN, "fail to insert low", K(ret), K(data_extent_id));
        }
      } else {
        SE_LOG(INFO, "success to insert low", K(data_extent_id));
      }
    } else {
      se_assert(kReadThrough == mode_);
    }
  }

  return ret;
} 

// During the read process, it should attempt to write the new version of the data extent to the persistent
// cache regardless of the persistent cache mode.
int PersistentCache::insert_on_read_process(const ExtentId &data_extent_id, const Slice &extent_data, Cache::Handle **handle)
{
  int ret = Status::kOk;

  if (UNLIKELY(extent_data.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(extent_data), K(data_extent_id));
  } else if (FAILED(insert_low(data_extent_id, extent_data, handle))) {
    if (Status::kNoSpace != ret) {
      SE_LOG(WARN, "fail to insert low", K(ret), K(data_extent_id));
    }
  } else {
    SE_LOG(INFO, "success to insert low", K(data_extent_id));
  }

  return ret;
}

int PersistentCache::insert_low(const ExtentId &data_extent_id, const Slice &extent_data, Cache::Handle **handle)
{
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  PersistentCacheInfo cache_info;

  if (FAILED(cache_file_.alloc(data_extent_id, cache_info))) {
    if (Status::kNoSpace != ret) {
      SE_LOG(WARN, "fail to alloc from cache file", K(ret), K(data_extent_id));
    }
  } else if (FAILED(cache_file_.write(extent_data, cache_info))) {
    SE_LOG(WARN, "fail to write to cache file", K(ret), K(data_extent_id), K(cache_info));
  } else if (FAILED(insert_cache(data_extent_id, cache_info, handle))) {
    SE_LOG(WARN, "fail to insert into cache", K(ret), K(data_extent_id), K(cache_info));
  } else {
#ifndef NDEBUG
    SE_LOG(INFO, "success to insert into persistent cache", K(data_extent_id), K(cache_info));
#endif
  }

  if (FAILED(ret)) {
    if (cache_info.is_valid()) {
      if (Status::kOk != (tmp_ret = cache_file_.recycle(cache_info))) {
        SE_LOG(WARN, "fail to recycle cache file space", K(tmp_ret), K(cache_info));
      }
    }
  }

  return ret;
}

int PersistentCache::erase(const storage::ExtentId &data_extent_id)
{
  se_assert(!is_recovering_);
  int ret = Status::kOk;

  if (FAILED(cache_file_.erase(data_extent_id))) {
    SE_LOG(WARN, "fail to erase from cache file", K(ret), K(data_extent_id));
  } else {
    // TODO (Zhao Dongsheng): There may left some staled entries after persistenct cache recovery.
    evict_cache(data_extent_id);
    SE_LOG(INFO, "success to erase from cache file", K(data_extent_id));
  }

  return ret;
}

void PersistentCache::generate_cache_key(const ExtentId &data_extent_id, char *buf, Slice &cache_key)
{
  char *end = util::EncodeVarint64(buf, data_extent_id.id());
  cache_key.assign(buf, end - buf);
}

int PersistentCache::insert_cache(const storage::ExtentId &data_extent_id,
                                  const PersistentCacheInfo &cache_info,
                                  Cache::Handle **handle)
{
  int ret = Status::kOk;
  Cache::Handle *tmp_handle = nullptr;
  PersistentCacheInfo *cache_value = nullptr;
  char cache_key_buf[MAX_PERSISTENT_CACHEL_KEY_SIZE] = {0};
  Slice cache_key;
  generate_cache_key(data_extent_id, cache_key_buf, cache_key);

  if (UNLIKELY(!cache_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(data_extent_id), K(cache_info));
  } else if (is_recovering_.load() && IS_NOTNULL(tmp_handle = cache_->Lookup(cache_key))) {
    // The write rules of PersistentCache ensure that only one version of data for a
    // data extent exists in the Persistent Cache at any given time.
    ret = Status::kCorruption;
    SE_LOG(ERROR, "the staled cache data has not been erased", K(ret), K(data_extent_id), K(cache_info));
  } else if (IS_NULL(cache_value = MOD_NEW_OBJECT(ModId::kPersistentCache, PersistentCacheInfo, cache_info))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for PersistentCacheInfo", K(ret), K(cache_info));
  } else if (FAILED(cache_->Insert(cache_key,
                                   cache_value,
                                   cache_value->get_usable_size(),
                                   &PersistentCacheInfo::delete_entry,
                                   handle,
                                   Cache::Priority::HIGH).code())) {
    SE_LOG(WARN, "fail to insert into persistent cache", K(ret), K(data_extent_id), K(cache_info));
  } else {
#ifndef NDEBUG
    SE_LOG(INFO, "success to insert into persistent cache", K(data_extent_id), K(cache_info));
#endif
  }

  if (FAILED(ret)) {
    if (IS_NOTNULL(cache_value)) {
      MOD_DELETE_OBJECT(PersistentCacheInfo, cache_value);
    }
  }

  return ret;
}

int PersistentCache::recover_callback()
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCache has not been inited", K(ret));
  } else if (UNLIKELY(!is_recovering_.load())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the PersistentCache needn't recover", K(ret));
  } else {
    is_recovering_.store(false);
    SE_LOG(INFO, "success to execute recovery callback");
  } 
  
  return ret;
}

void PersistentCache::evict_cache(const ExtentId &data_extent_id)
{
  char cache_key_buf[MAX_PERSISTENT_CACHEL_KEY_SIZE] = {0};
  Slice cache_key;
  generate_cache_key(data_extent_id, cache_key_buf, cache_key);
  cache_->Erase(cache_key);
}

int PersistentCache::wait_recover_finished()
{
  int ret = Status::kOk;
  // TODO(Zhao Dongsheng): The failure of inserting into the persistent cache should not
  // disrupt the write process.However, in the current implementation, persistent cache
  // recovery relies on the data in the presitent cache being up-to-date.
  while (is_recovering_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  return ret;
}

} // namespace cache
} // namespace smartengine