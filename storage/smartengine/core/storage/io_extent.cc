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

#include "storage/io_extent.h"
#include <unistd.h>
#include "cache/persistent_cache.h"
#include "table/extent_writer.h"
#include "util/common.h"
#include "util/misc_utility.h"
#include "util/sync_point.h"

namespace smartengine
{
using namespace cache;
using namespace common;
using namespace memory;
using namespace table;
using namespace util;

namespace storage
{

IOExtent::IOExtent()
    : is_inited_(false),
      extent_id_(),
      unique_id_(0),
      is_large_object_extent_(false)
{}

IOExtent::~IOExtent() {}

void IOExtent::reset()
{
  is_inited_ = false;
  extent_id_.reset();
  unique_id_ = 0;
  is_large_object_extent_ = false;
}

int64_t IOExtent::get_unique_id(char *id, const int64_t max_size) const
{
  int64_t len = std::min(static_cast<int64_t>(sizeof(unique_id_)), max_size);
  memcpy(id, (char *)(&(unique_id_)), len);
  return len;
}

FileIOExtent::FileIOExtent() : fd_(-1) {}

FileIOExtent::~FileIOExtent() {}

int FileIOExtent::init(const ExtentId &extent_id, int64_t unique_id, int fd)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "FileIOExtent has been inited", K(ret));
  } else if (UNLIKELY(unique_id < 0) || UNLIKELY(fd < 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(unique_id), K(fd));
  } else {
    extent_id_ = extent_id;
    unique_id_ = unique_id;
    fd_ = fd;

    is_inited_ = true;
  }

  return ret;
}

void FileIOExtent::reset()
{
  IOExtent::reset();
  fd_ = -1;
}

int FileIOExtent::write(const common::Slice &data, int64_t offset)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "FileIOExtent has not been inited", K(ret));
  } else if (UNLIKELY(data.empty()) || UNLIKELY(offset < 0) ||
             UNLIKELY((offset + data.size()) > storage::MAX_EXTENT_SIZE)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(offset), K(data), "size", data.size());
  } else if (UNLIKELY(!DIOHelper::is_aligned(get_base_offset() + offset)) ||
             UNLIKELY(!DIOHelper::is_aligned(data.size()))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, the writing data is not aligned", K(ret), K(offset), "size", data.size());
  } else if (DIOHelper::is_aligned(reinterpret_cast<std::uintptr_t>(data.data()))) {
    if (FAILED(direct_write(fd_, get_base_offset() + offset, data.data(), data.size()))) {
      SE_LOG(WARN, "fail to direct write file", K(ret), K_(extent_id), K_(fd), "size", data.size());
    }
  } else if (FAILED(align_to_direct_write(fd_, get_base_offset() + offset, data.data(), data.size()))) {
    SE_LOG(WARN, "fail to align to direct write", K(ret));
  }

#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("IOExtent::write_failed", &ret);
#endif

  return ret;
}

int FileIOExtent::read(AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, Slice &result)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "FileIOExtent has not been inited", K(ret));
  } else if (UNLIKELY(offset < 0) || UNLIKELY(size <= 0) || IS_NULL(buf)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(offset), K(size), KP(buf));
  } else if (UNLIKELY(offset + size > storage::MAX_EXTENT_SIZE)) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "the range to read is overflow", K(ret), K(offset), K(size));
  } else {
    if (IS_NULL(aio_handle)) {
      // Sync read
      if (FAILED(sync_read(offset, size, buf, result))) {
        SE_LOG(WARN, "fail to sync read", K(ret),
            K_(extent_id), K_(fd), K(offset), K(size));
      }
    } else {
      // Async read
      if (FAILED(async_read(aio_handle, offset, size, buf, result))) {
        SE_LOG(WARN, "fail to async read", K(result),
            K_(extent_id), K_(fd), K(offset), K(size));
      }
    }
  }

  return ret;
}

int FileIOExtent::prefetch(util::AIOHandle *aio_handle, int64_t offset, int64_t size)
{
  int ret = Status::kOk;
  AIOInfo aio_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ReadableExtent should be inited", K(ret));
  } else if (IS_NULL(aio_handle) || UNLIKELY(offset < 0) || UNLIKELY(size <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(aio_handle), K(offset), K(size));
  } else if (FAILED(fill_aio_info(offset, size, aio_info))) {
    SE_LOG(WARN, "fail to fill aio info", K(ret), K_(fd), K_(extent_id), K(offset), K(size));
  } else if (FAILED(aio_handle->prefetch(aio_info))) {
      SE_LOG(WARN, "fail to pretch", K(ret), K(aio_info));
  }

  // Prefetch failed, will try sync IO, overwrite ret here.
  if (FAILED(ret)) {
    ret = Status::kOk;
    aio_handle->aio_req_->status_ = Status::kErrorUnexpected;
    SE_LOG(WARN, "aio prefetch failed, will try sync IO!");
  }

  return ret;
}


bool FileIOExtent::is_aligned(int64_t offset, int64_t size, const char *buf) const
{
  return DIOHelper::is_aligned(offset) &&
         DIOHelper::is_aligned(size) &&
         DIOHelper::is_aligned(reinterpret_cast<std::uintptr_t>(buf));
}

int FileIOExtent::align_to_direct_write(int fd, int64_t offset, const char *buf, int64_t size)
{
  int ret = Status::kOk;
  char *aligned_buf = nullptr;

  if (IS_NULL(aligned_buf = reinterpret_cast<char *>(base_memalign(DIOHelper::DIO_ALIGN_SIZE, size, ModId::kIOExtent)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for aligned buffer", K(ret), K(size));
  } else {
    memcpy(aligned_buf, buf, size);
    if (FAILED(direct_write(fd, offset, aligned_buf, size))) {
      SE_LOG(WARN, "fail to write file", K(ret), K_(extent_id), K_(fd), K(size));
    }
  }

  if (IS_NOTNULL(aligned_buf)) {
    base_memalign_free(aligned_buf);
    aligned_buf = nullptr;
  }

  return ret;
}

int FileIOExtent::direct_write(int fd, int64_t offset, const char *buf, int64_t size)
{
  assert(fd > 0 && offset >= 0 && size > 0 && (nullptr != buf));
  assert(is_aligned(offset, size, buf));

  int ret = Status::kOk;
  int64_t write_size = 0;
  int64_t total_write_size = 0;
  const char *curr_buf = buf;
  int64_t curr_offset = offset;
  int64_t left_size = size;

  while (SUCCED(ret) && left_size > 0) {
    write_size = pwrite(fd, curr_buf, left_size, curr_offset);

    if (write_size <= 0) {
      if (EINTR == errno) {
        continue;
      } else {
        ret = Status::kIOError;
#ifndef NDEBUG
        SE_LOG(ERROR, "extent io error!", K(ret), K(errno));
#endif
      }
    } else {
      total_write_size += write_size;
      curr_buf = buf + total_write_size;
      curr_offset = offset + total_write_size;
      left_size = size - total_write_size;
    }
  }
  se_assert(size == total_write_size);

  return ret;
}

int FileIOExtent::fill_aio_info(int64_t offset, int64_t size, AIOInfo &aio_info) const
{
  int ret = Status::kOk;
  aio_info.reset();

  if (UNLIKELY((offset + size) > storage::MAX_EXTENT_SIZE)) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "extent size overflow!", K(ret), K(offset), K(size));
  } else {
    aio_info.fd_ = fd_;
    aio_info.offset_ = get_base_offset() + offset;
    aio_info.size_ = size;
  }

  return ret;
}

int FileIOExtent::sync_read(int64_t offset, int64_t size, char *buf, Slice &result)
{
  int ret = Status::kOk;
  int64_t file_offset = get_base_offset() + offset;

  if (is_aligned(offset, size, buf)) {
    if (FAILED(direct_read(fd_, file_offset, size, buf))) {
      SE_LOG(WARN, "fail to direct read", K(ret),
          K_(fd), K_(extent_id), K(offset), K(size));
    }
  } else {
    if (FAILED(align_to_direct_read(fd_, file_offset, size, buf))) {
      SE_LOG(WARN, "fail to convert to direct read", K(ret),
          K_(fd), K_(extent_id), K(offset), K(size));
    }
  }

  if (SUCCED(ret)) {
    result.assign(buf, size);
  }

  return ret;

}

int FileIOExtent::async_read(AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, Slice &result)
{
  assert(nullptr != aio_handle);
  int ret = Status::kOk;
  AIOInfo aio_info;

  if (FAILED(fill_aio_info(offset, size, aio_info))) {
    SE_LOG(WARN, "fail to fill aio info", K(ret),
        K_(fd), K_(extent_id), K(offset), K(size));
  } else if (FAILED(aio_handle->read(aio_info.offset_, aio_info.size_, &result, buf))) {
    SE_LOG(WARN, "fail to aio handle read", K(ret), K(offset), K(size), K(aio_info));
    BACKTRACE(ERROR, "aio handle read failed!");
  } else {
    // succeed
  }

  // AIO read failed, try sync read, overwrite ret here.
  if (FAILED(ret)) {
    if (FAILED(sync_read(offset, size, buf, result))) {
      SE_LOG(WARN, "fail to sync read after async read failed", K(ret), K(offset), K(size), K(aio_info));
    }
  }

  return ret;
}
int FileIOExtent::align_to_direct_read(const int fd, const int64_t offset, const int64_t size, char *buf)
{
  assert(fd > 0 && offset >= 0 && size > 0 && (nullptr != buf));
  assert(!is_aligned(offset, size, buf));

  int ret = Status::kOk;
  int64_t aligned_offset = DIOHelper::align_offset(offset);
  int64_t aligned_size = DIOHelper::align_size(offset + size - aligned_offset);
  char *aligned_buf = nullptr;

  if (IS_NULL(aligned_buf = reinterpret_cast<char *>(memory::base_memalign(
      DIOHelper::DIO_ALIGN_SIZE, aligned_size, memory::ModId::kIOExtent)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for aligned buffer", K(ret), K(aligned_size));
  } else if (FAILED(direct_read(fd, aligned_offset, aligned_size, aligned_buf))) {
    SE_LOG(WARN, "fail to direct read", K(ret), K(aligned_offset), K(aligned_size));
  } else {
    memcpy(buf, aligned_buf + (offset - aligned_offset), size);
  }

  if (IS_NOTNULL(aligned_buf)) {
    memory::base_memalign_free(aligned_buf);
    aligned_buf = nullptr;
  }

  return ret;
}

int FileIOExtent::direct_read(int fd, int64_t offset, int64_t size, char *buf)
{
  assert(fd > 0 && offset >= 0 && size > 0 && (nullptr != buf));
  assert(is_aligned(offset, size, buf));

  int ret = Status::kOk;
  int64_t total_read_size = 0;
  int64_t read_size = 0;
  char *curr_buf = buf;
  int64_t curr_offset = offset;
  int64_t left_size = size;


  while (SUCCED(ret) && left_size > 0) {
    read_size = pread(fd, curr_buf, left_size, curr_offset);
    if (read_size <= 0) {
      if (EINTR == errno) {
        continue;;
      } else {
        ret = Status::kIOError;
#ifndef NDEBUG
        SE_LOG(ERROR, "extent io error!", K(ret), K(errno));
#endif
      }
    } else {
      total_read_size += read_size;
      curr_buf = buf + total_read_size;
      curr_offset = offset + total_read_size;
      left_size = size - total_read_size;
    }
  }
  se_assert(size == total_read_size);

  return ret;
}

ObjectIOExtent::ObjectIOExtent() : object_store_(nullptr), bucket_(), prefix_() {}

ObjectIOExtent::~ObjectIOExtent() {}

int ObjectIOExtent::init(const ExtentId &extent_id,
                         int64_t unique_id,
                         ::objstore::ObjectStore *object_store,
                         const std::string &bucket,
                         const std::string &prefix)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ObjectIOExtent has been inited", K(ret));
  } else if (UNLIKELY(unique_id < 0) ||
             UNLIKELY(IS_NULL(object_store)) ||
             UNLIKELY(bucket.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(unique_id),
        KP(object_store), K(bucket));
  } else {
    extent_id_ = extent_id;
    unique_id_ = unique_id;
    object_store_ = object_store;
    bucket_ = bucket;
    prefix_ = prefix;

    is_inited_ = true;
  }

  return ret;
}

void ObjectIOExtent::reset()
{
  IOExtent::reset();
  object_store_ = nullptr;
  bucket_.clear();
  prefix_.clear();
}

// ObjectIOExtent only allows writes starting from the beginning of the extent.
int ObjectIOExtent::write(const Slice &data, int64_t offset)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjectIOExtent has not been inited", K(ret));
  } else if (UNLIKELY(data.empty()) || UNLIKELY(0 != offset) ||
             UNLIKELY(data.size() > storage::MAX_EXTENT_SIZE)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(offset), K(data), "size", data.size());
  } else if (FAILED(write_object(data.data(), data.size()))) {
    SE_LOG(WARN, "fail to write object", K(ret), K_(bucket));
  } else {
    // succeed
  }

  return ret;
}


int ObjectIOExtent::read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, Slice &result)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjectIOExtent has not been inited", K(ret));
  } else if (UNLIKELY(offset < 0) || UNLIKELY(size <= 0) || IS_NULL(buf)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(offset), K(size), KP(buf));
  } else if (UNLIKELY(offset + size > storage::MAX_EXTENT_SIZE)) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "the range to read is overflow", K(ret), K(offset), K(size));
  } else {
    if (IS_NULL(aio_handle)) {
      if (FAILED(sync_read(offset, size, buf, result))) {
        SE_LOG(WARN, "fail to sync read object", K(ret), K_(extent_id), K(offset), K(size));
      }
    } else {
      if (FAILED(async_read(aio_handle, offset, size, buf, result))) {
        SE_LOG(WARN, "fail to async read object", K(ret), K_(extent_id), K(offset), K(size));
      }
    }
  }

  return ret;
}

int ObjectIOExtent::prefetch(util::AIOHandle *aio_handle, int64_t offset, int64_t size)
{
  int ret = Status::kOk;
  AIOInfo aio_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ObjectIOExtent should be inited", K(ret));
  } else if (UNLIKELY(IS_NULL(aio_handle)) || UNLIKELY(offset < 0) || UNLIKELY(size <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(aio_handle), K(offset), K(size));
  } else if (FAILED(fill_aio_info(aio_handle, offset, size, aio_info))) {
    if (Status::kNoSpace != ret) {
      SE_LOG(WARN, "fail to fill aio info for ObjectIOExtent", K(ret), K_(extent_id),
          KP(aio_handle), K(offset), K(size));
    }
  } else if (FAILED(aio_handle->prefetch(aio_info))) {
    SE_LOG(WARN, "fail to prefetch", K(ret), K_(extent_id), K(aio_info));
  } else {
    SE_LOG(DEBUG, "success to prefetch", K(offset), K(size));
  }

  // Prefetch failed, will try sync io, overwrite ret here.
  if (FAILED(ret)) {
    if (Status::kNoSpace == ret) {
      aio_handle->aio_req_->status_ = Status::kNoSpace;
    } else {
      aio_handle->aio_req_->status_ = Status::kErrorUnexpected;
      SE_LOG(WARN, "aio prefetch object extent failed, will try sync IO!", K(ret));
    }
    ret = Status::kOk;
  }

  return ret;
}

int ObjectIOExtent::fill_aio_info(AIOHandle *aio_handle, int64_t offset, int64_t size, AIOInfo &aio_info)
{
  int ret = Status::kOk;
  PersistentCacheInfo *cache_info = nullptr;
  aio_info.reset();

  if (UNLIKELY(IS_NULL(aio_handle)) || UNLIKELY(offset < 0) || UNLIKELY(size <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(aio_handle), K(offset), K(size));
  } else if (UNLIKELY((offset + size) > storage::MAX_EXTENT_SIZE)) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "the read range is overflow", K(ret), K(offset), K(size));
  } else if (!PersistentCache::get_instance().is_enabled() || is_large_object_extent()) {
    ret = Status::kNoSpace;
  } else if (IS_NOTNULL(aio_handle->aio_req_->handle_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the persistent cache handle must be nullptr before actually prefetch", K(ret),
        K_(extent_id), K(offset), K(size));
  } else if (FAILED(load_extent(&(aio_handle->aio_req_->handle_)))) {
    if (Status::kNoSpace != ret) {
      SE_LOG(WARN, "fail to load extent", K(ret));
    }
  } else if (IS_NULL(cache_info = PersistentCache::get_instance().get_cache_info_from_handle(aio_handle->aio_req_->handle_))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the cache info must not be nullptr", K(ret), K_(extent_id));
  } else {
    aio_info.fd_ = cache_info->get_cache_file()->get_fd();
    aio_info.offset_ = cache_info->get_offset() + offset;
    aio_info.size_ = size;
  }

  return ret;
}

int ObjectIOExtent::write_object(const char *data, int64_t data_size)
{
  int ret = Status::kOk;
  // bool forbid_overwrite = true;
  bool forbid_overwrite = false;
  ::objstore::Status object_status;
  std::string object_id = prefix_ + std::to_string(assemble_objid_by_fdfn(extent_id_.file_number, extent_id_.offset));

  object_status = object_store_->put_object(bucket_, object_id, std::string_view(data, data_size), forbid_overwrite);
  if (UNLIKELY(!object_status.is_succ())) {
    // if (object_status.error_code() == ::objstore::Errors::SE_OBJECT_FORBID_OVERWRITE) {
    //   // there is maybe another wesql data node is writting an object with the same extent id.
    //   // at now, we just abort and let the user to shutdown one wesql data node.
    //   SE_LOG(FATAL,
    //          "failed to write object, an object with the same key existed, forbid overwritting extent!",
    //          K(ret),
    //          KE(object_status.error_code()),
    //          K(std::string(object_status.error_message())),
    //          K(extent_id_),
    //          K(object_id));
    //   abort();
    // }
    ret = Status::kObjStoreError;
    SE_LOG(WARN, "io error, failed to put obj", K(ret), KE((object_status.error_code())), K(std::string(object_status.error_message())),
        K_(extent_id), K_(bucket), K(object_id), K(data_size), K_(bucket));
  } else {
    if (PersistentCache::get_instance().is_enabled() && !is_large_object_extent()) {
      if (FAILED(PersistentCache::get_instance().insert(extent_id_,
                                            Slice(data, data_size),
                                            true /*write_process*/,
                                            nullptr /*handle*/))) {
        if (Status::kNoSpace != ret) {
          SE_LOG(WARN, "fail to write extent data to persistent cache", K(ret), K_(extent_id));
        } else {
          // Treat a full persistent cache as a normal condition and overwrite the ret here.
          ret = Status::kOk;
        }
      } else {
#ifndef NDEBUG
        SE_LOG(INFO, "success to write extent data to persistent cache", K_(extent_id));
#endif
      }
    }

    SE_LOG(DEBUG, "success to write object", K_(extent_id), K_(bucket), K(object_id));
  }

#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("IOExtent::write_failed", &ret);
#endif

  return ret;
}

int ObjectIOExtent::sync_read(int64_t offset, int64_t size, char *buf, Slice &result)
{
  int ret = Status::kOk;
  Cache::Handle *handle = nullptr;

  if (PersistentCache::get_instance().is_enabled()) {
    if (FAILED(PersistentCache::get_instance().lookup(extent_id_, handle))) {
      // Overwrite the ret if persistent cache miss.
      if (Status::kNotFound != ret) {
        SE_LOG(WARN, "fail to look up from persustent cache", K(ret), K_(extent_id), K(offset), K(size));
      } else if (FAILED(read_object(offset, size, buf, result))) {
        SE_LOG(WARN, "fail to read object", K(ret), K_(extent_id), K(offset), K(size));
      }
    } else if (FAILED(PersistentCache::get_instance().read_from_handle(handle,
                                                                       nullptr /*aio_handle*/,
                                                                       offset,
                                                                       size,
                                                                       buf,
                                                                       result))) {
      SE_LOG(WARN, "fail to read from persistent cache handle", K(ret), K_(extent_id), K(offset), K(size));
    } else {
      SE_LOG(DEBUG, "success to read from persistent cache", K_(extent_id), K(offset), K(size));
    }

    if (IS_NOTNULL(handle)) {
      PersistentCache::get_instance().release_handle(handle);
      handle = nullptr;
    }
  } else {
    if (FAILED(read_object(offset, size, buf, result))) {
      SE_LOG(WARN, "fail to read object", K(ret), K_(extent_id), K(offset), K(size));
    }
  }

  return ret;
}

int ObjectIOExtent::async_read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, Slice &result)
{
  assert(IS_NOTNULL(aio_handle));
  int ret = aio_handle->aio_req_->status_;

  if (Status::kNoSpace == ret) {
    // The data hasn't been prefetched into persistent cache, maybe the persistent cache
    // is disabled or the persistent cache space is exhausted.
  } else if (FAILED(PersistentCache::get_instance().read_from_handle(aio_handle->aio_req_->handle_,
                                                                     aio_handle,
                                                                     offset,
                                                                     size,
                                                                     buf,
                                                                     result))) {
    SE_LOG(WARN, "fail to read data from persistent cache", K(ret), K_(extent_id), K(offset), K(size));
  } else {
    SE_LOG(DEBUG, "success to read from persistent cache", K_(extent_id), K(offset), K(size));
  }

  // Aync read object failed, try sync read, overwrite ret here.
  if (FAILED(ret)) {
    if (FAILED(read_object(offset, size, buf, result))) {
      SE_LOG(WARN, "fail to read object", K(ret), K_(extent_id), K(offset), K(size));
    }
  }

  return ret;
}

int ObjectIOExtent::read_object(int64_t offset, int64_t size, char *buf, common::Slice &result)
{
  int ret = Status::kOk;
  ::objstore::Status object_status;
  std::string object_id = prefix_ + std::to_string(assemble_objid_by_fdfn(extent_id_.file_number, extent_id_.offset));
  std::string body;

  if (PersistentCache::get_instance().is_enabled() && !is_large_object_extent()) {
    object_status = object_store_->get_object(bucket_, object_id, 0, MAX_EXTENT_SIZE, body);
    if (UNLIKELY(!object_status.is_succ())) {
      ret = Status::kObjStoreError;
      SE_LOG(WARN, "io error, failed to get obj", K(ret), KE(object_status.error_code()),
            K(std::string(object_status.error_message())), K_(bucket), K_(extent_id), K(object_id));
    } else if (UNLIKELY(MAX_EXTENT_SIZE != body.size())) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "the data is corrupted", K(ret), "size", body.size());
    } else {
      memcpy(buf, body.data() + offset, size);
      result.assign(buf, size);

      if (FAILED(PersistentCache::get_instance().insert(extent_id_,
                                                        Slice(body.data(), body.size()),
                                                        false /*write_process*/,
                                                        nullptr /*handle*/))) {
        if (Status::kNoSpace != ret) {
          SE_LOG(WARN, "fail to insert into persistent cache", K(ret), K_(extent_id));
        } else {
          // persistent cache is full, overwrite ret here.
          ret = Status::kOk;
        }
      }
    }
  } else {
    object_status = object_store_->get_object(bucket_, object_id, offset, size, body);
    if (UNLIKELY(!object_status.is_succ())) {
      ret = Status::kObjStoreError;
      SE_LOG(WARN, "io error, failed to get obj", K(ret), KE(object_status.error_code()),
            K(std::string(object_status.error_message())), K_(bucket), K_(extent_id), K(object_id));
    } else if (UNLIKELY(size != static_cast<int64_t>(body.size()))) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "the data is corrupted", K(ret), "expected_size", size, "actual_size", body.size());
    } else {
      assert(static_cast<int64_t>(body.size()) <= size);
      memcpy(buf, body.data(), body.size());
      result.assign(buf, size);
    }
  }

  return ret;
}

int ObjectIOExtent::load_extent(cache::Cache::Handle **handle)
{
  int ret = Status::kOk;

  if (FAILED(PersistentCache::get_instance().lookup(extent_id_, *handle))) {
    if (Status::kNotFound != ret) {
      SE_LOG(WARN, "fail to lookup from persistent cache", K(ret), K_(extent_id));
    } else {
      std::string object_id = prefix_ + std::to_string(assemble_objid_by_fdfn(extent_id_.file_number, extent_id_.offset));
      std::string body;
      ::objstore::Status object_status = object_store_->get_object(bucket_, object_id, 0, MAX_EXTENT_SIZE, body);

      if (UNLIKELY(!object_status.is_succ())) {
        ret = Status::kObjStoreError;
        SE_LOG(WARN, "io error, fail to get object", K(ret), KE(object_status.error_code()),
            K(std::string(object_status.error_message())), K_(extent_id), K(object_id));
      } else if (UNLIKELY(MAX_EXTENT_SIZE != body.size())) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "the data is corrupted", K(ret), "size", body.size());
      } else if (FAILED(PersistentCache::get_instance().insert(extent_id_,
                                                               Slice(body.data(), body.size()),
                                                               false /*write_process*/,
                                                               handle))) {
        if (Status::kNoSpace != ret) {
          SE_LOG(WARN, "fail to insert into persistent cache", K(ret), K_(extent_id), K(object_id));
        }
      } else {
        SE_LOG(DEBUG, "success to load extent", K_(extent_id), KP(*handle));
      }
    }
  }

  return ret;
}

WriteExtentJob::WriteExtentJob()
    : is_inited_(false),
      writer_(nullptr),
      extent_(nullptr),
      data_(nullptr),
      data_size_(0)
{}

WriteExtentJob::~WriteExtentJob()
{
  destroy();
}

int WriteExtentJob::init(table::ExtentWriter *writer, IOExtent *extent, const common::Slice &data)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "IOExtentJob has been inited", K(ret));
  } else if (IS_NULL(writer) || IS_NULL(extent) || UNLIKELY(data.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(writer), KP(extent), K(data));
  } else if (IS_NULL(data_ = reinterpret_cast<char *>(base_memalign(DIOHelper::DIO_ALIGN_SIZE, data.size(), ModId::kIOExtent)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for data", K(ret), "size", data.size());
  } else {
    // TODO (Zhao Dongsheng): resue data_ memory if neccessary.
    memcpy(data_, data.data(), data.size());
    data_size_ = data.size();
    writer_ = writer;
    extent_ = extent;
    is_inited_ = true;
  }

  return ret;
}

void WriteExtentJob::destroy()
{
  if (IS_NOTNULL(extent_)) {
    DELETE_OBJECT(ModId::kIOExtent, extent_);
    extent_ = nullptr;
  }

  if (IS_NOTNULL(data_)) {
    base_memalign_free(data_);
    data_ = nullptr;
  }
}

int WriteExtentJob::execute()
{
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "IOExtentJob should be inited", K(ret));
  } else {
    if (FAILED(extent_->write(Slice(data_, data_size_), 0))) {
      SE_LOG(WARN, "fail to write extent data", K(ret), "extent_id", extent_->get_extent_id());
    } else {
#ifndef  NDEBUG
      SE_LOG(INFO, "success to do io extent job", "extent_id", extent_->get_extent_id());
#endif
    }
    
    // Regardless of whether the write is successful, this extent job needs to be
    // marked as finished.Additionally, the marking action is not expected to fail.
    if (Status::kOk != (tmp_ret = writer_->mark_write_extent_finish(extent_->get_extent_id()))) {
      SE_LOG(WARN, "fail to mark io complete", K(tmp_ret), "extent_id", extent_->get_extent_id());
      se_assert(false);
    }
  }

  if (FAILED(ret)) {
    writer_->report_write_extent_error(ret);
  }

  return ret;
}

WriteExtentJobScheduler::WriteExtentJobScheduler()
    : is_inited_(false),
      env_(nullptr),
      write_io_thread_count_(0),
      job_queue_mutex_(),
      job_queue_()
{}

WriteExtentJobScheduler::~WriteExtentJobScheduler() {}

WriteExtentJobScheduler &WriteExtentJobScheduler::get_instance()
{
  static WriteExtentJobScheduler scheduler;
  return scheduler;
}

int WriteExtentJobScheduler::start(util::Env *env, int64_t write_io_thread_count)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "WriteExtentJobScheduler has been inited", K(ret));
  } else if (IS_NULL(env) || UNLIKELY(write_io_thread_count <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(write_io_thread_count));
  } else {
    env_ = env;
    write_io_thread_count_ = write_io_thread_count;
    env_->SetBackgroundThreads(write_io_thread_count_, Env::WRITE_IO_THREAD);
    is_inited_ = true;
    SE_LOG(INFO, "success to start WriteExtentJobScheduler", K(write_io_thread_count));
  }

  return ret;
}

int WriteExtentJobScheduler::stop()
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "WriteExtentJobScheduler has not been inited", K(ret));
  } else {
    env_->UnSchedule(this, Env::Priority::WRITE_IO_THREAD);
    // Before WriteExtentJobScheduler exits, all flush and compaction tasks must be
    // completed or canceled.There tasks depend on completion of their submitted write
    // extent tasks.Therefore, when 'WriteExtentJobScheduler' exits, there should be no
    // unfinished write extent tasks.
    std::lock_guard<std::mutex> lock_guard(job_queue_mutex_);
    assert(job_queue_.empty());
    if (!job_queue_.empty()) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "There should be no unfinished tasks in the queue", K(ret), "job_count", job_queue_.size());
    } else {
      is_inited_ = false;
      SE_LOG(INFO, "success to stop WriteExtentJobScheduler");
    }
  }

  return ret;
}

int WriteExtentJobScheduler::submit(WriteExtentJob *job)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "WriteExtentJobScheduler should be inited", K(ret));
  } else if (IS_NULL(job)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(job));
  } else {
    push_to_job_queue(job);
    env_->Schedule(&WriteExtentJobScheduler::consume_wrapper, this, Env::Priority::WRITE_IO_THREAD, this);
  }

  return ret;
}

int WriteExtentJobScheduler::adjust_write_thread_count(int64_t thread_count)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "WriteExtentJobScheduler should be inited", K(ret));
  } else if (thread_count <= write_io_thread_count_.load()) {
    SE_LOG(INFO, "Only dynamic addition of write threads is supported",
      K(thread_count), K(write_io_thread_count_.load()));
  } else {
    env_->IncBackgroundThreadsIfNeeded(thread_count, Env::Priority::WRITE_IO_THREAD);
  }

  return ret;
}

void WriteExtentJobScheduler::consume_wrapper(void *scheduler)
{
  reinterpret_cast<WriteExtentJobScheduler *>(scheduler)->consume();
}

int WriteExtentJobScheduler::consume()
{
  int ret = Status::kOk;
  WriteExtentJob *job = nullptr;

  while (SUCCED(ret) && IS_NOTNULL(job = pop_from_job_queue())) {
    QUERY_COUNT(monitor::CountPoint::WRITE_EXTENT_COUNT);
    QUERY_TRACE_RESET();
    QUERY_TRACE_SCOPE(monitor::TracePoint::WRITE_EXTENT_TIME);
    if (FAILED(job->execute())) {
      SE_LOG(WARN, "fail to execute io extent job", K(ret));
    }

    MOD_DELETE_OBJECT(WriteExtentJob, job);
  }

  return ret;
}

void WriteExtentJobScheduler::push_to_job_queue(WriteExtentJob *job)
{
  se_assert(IS_NOTNULL(job));
  std::unique_lock<std::mutex> lock_guard(job_queue_mutex_);
  // Limit the overall memory usage by restricting the number of write extent jobs in the queue.
  while (static_cast<int64_t>(job_queue_.size()) > MAX_JOB_QUEUE_SIZE_FACTOR * write_io_thread_count_) {
    lock_guard.unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    lock_guard.lock();
  }
  job_queue_.push(job);
}

WriteExtentJob *WriteExtentJobScheduler::pop_from_job_queue()
{
  WriteExtentJob *job = nullptr;
  std::lock_guard<std::mutex> lock_guard(job_queue_mutex_);
  if (!job_queue_.empty()) {
    job = job_queue_.front();
    job_queue_.pop();
    se_assert(IS_NOTNULL(job));
  }

  return job;
}

}  // namespace storage
}  // namespace smartengine
