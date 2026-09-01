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

#include "extent_meta_manager.h"

#include <vector>

#include "objstore/remote_extent.h"
#include "storage/storage_logger.h"
#include "storage/storage_log_entry.h"
#include "util/dio_helper.h"

namespace smartengine
{
using namespace common;
using namespace util;

namespace storage
{

ExtentMetaManager::ExtentMetaManager()
    : is_inited_(false),
      meta_mutex_(),
      extent_meta_map_()
{
}

ExtentMetaManager::~ExtentMetaManager()
{
  destroy();
}

ExtentMetaManager &ExtentMetaManager::get_instance()
{
  static ExtentMetaManager extent_meta_manager;
  return extent_meta_manager;
}

void ExtentMetaManager::destroy()
{
  if (is_inited_) {
    for (auto iter = extent_meta_map_.begin(); iter != extent_meta_map_.end(); ++iter) {
      free_extent_meta(iter->second);
    }
    extent_meta_map_.clear();
    is_inited_ = false;
  }
}

// TODO(Zhao Dongsheng): the init function do nothing now
int ExtentMetaManager::init()
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ExtentMetaManager has been inited", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ExtentMetaManager::write_meta(const ExtentMeta &extent_meta, bool write_log)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta_ptr = nullptr;
  ModifyExtentMetaLogEntry log_entry(extent_meta);
  std::string validation_error;

  if (write_log && remote_extent::enabled() &&
      !remote_extent::validate_metadata(
          extent_meta, remote_extent::MetadataValidationMode::NEW_REFERENCE,
          nullptr, &validation_error)) {
    ret = Status::kCorruption;
    SE_LOG(ERROR, "reject unverified remote extent metadata", K(ret),
           K(validation_error), K(extent_meta));
    remote_extent::enter_fenced(validation_error);
  } else if (write_log && FAILED(StorageLogger::get_instance().write_log(REDO_LOG_MODIFY_EXTENT_META, log_entry))) {
    SE_LOG(WARN, "fail to write change extent meta log", K(ret));
  } else if (FAILED(extent_meta.deep_copy(extent_meta_ptr))) {
    SE_LOG(WARN, "fail to deep copy extent_meta", K(ret));
  } else if (IS_NULL(extent_meta_ptr)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, deep copy extent meta must not nullptr", K(ret), KP(extent_meta_ptr));
  } else {
    util::SpinWLockGuard guard(meta_mutex_);
    if (!(extent_meta_map_.emplace(extent_meta_ptr->extent_id_.id(), extent_meta_ptr).second)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, fail to write extent meta", K(ret), K(*extent_meta_ptr), K(write_log));
    } else {
      SE_LOG(INFO, "success to write extent meta", K(*extent_meta_ptr), K(write_log));
    }
  }

  return ret;

}

int ExtentMetaManager::recycle_meta(const ExtentId extent_id)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;
  util::SpinWLockGuard guard(meta_mutex_);
  auto iter = extent_meta_map_.find(extent_id.id());
  if (extent_meta_map_.end() == iter) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "the extent meta to recycle not exist", K(ret), K(extent_id));
  } else if (IS_NULL(extent_meta = iter->second)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "the extent meta must not nullptr", K(ret), K(extent_id));
  } else if (1 != extent_meta_map_.erase(extent_id.id())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "fail to erase extent id", K(ret), K(extent_id));
  } else {
    free_extent_meta(extent_meta);
  }
  return ret;
}

ExtentMeta *ExtentMetaManager::get_meta(const ExtentId &extent_id)
{
  ExtentMeta *extent_meta = nullptr;
  util::SpinRLockGuard r_guard(meta_mutex_);
  auto iter = extent_meta_map_.find(extent_id.id());
  if (extent_meta_map_.end() != iter) {
    extent_meta = iter->second;
  }
  return extent_meta;
}

int ExtentMetaManager::do_checkpoint(util::WritableFile *checkpoint_writer,
                                     CheckpointHeader *header)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;
  int64_t offset = 0;
  ExtentMeta * extent_meta = nullptr;
  CheckpointBlockHeader *block_header = nullptr;

  if (IS_NULL(checkpoint_writer) || IS_NULL(header)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_writer), KP(header));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(
      DIOHelper::DIO_ALIGN_SIZE, buf_size,  memory::ModId::kExtentSpaceMgr)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
    SE_LOG(WARN, "fail to reserve checkpoint block header", K(ret));
  } else {
    util::SpinRLockGuard guard(meta_mutex_);
    header->extent_count_ = extent_meta_map_.size();
    header->extent_meta_block_offset_ = checkpoint_writer->GetFileSize();
    for (auto iter = extent_meta_map_.begin(); SUCCED(ret) && iter != extent_meta_map_.end(); ++iter) {
      if (IS_NULL(extent_meta = iter->second)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "extent meta must not nullptr", K(ret));
      } else if ((buf_size - offset) < extent_meta->get_serialize_size()) {
        block_header->data_size_ = offset - block_header->data_offset_;
        if (FAILED(checkpoint_writer->PositionedAppend(Slice(buf, buf_size), checkpoint_writer->GetFileSize()).code())) {
          SE_LOG(WARN, "fail to append buffer", K(ret), K(header));
        } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
          SE_LOG(WARN, "fail to reserve checkpoint block header", K(ret));
        } else if (FAILED(extent_meta->serialize(buf, buf_size, offset))) {
            SE_LOG(WARN, "fail to serialize extent meta", K(ret));
        } else {
          ++(header->extent_meta_block_count_);
        }
      } else if (FAILED(extent_meta->serialize(buf, buf_size, offset))) {
        SE_LOG(WARN, "fail to serialize extent meta", K(ret));
      }
      ++(block_header->entry_count_);
    }

    //flush last block
    if (Status::kOk == ret && block_header->entry_count_ > 0) {
      block_header->data_size_ = offset - block_header->data_offset_;
      if (FAILED(checkpoint_writer->PositionedAppend(Slice(buf, buf_size), checkpoint_writer->GetFileSize()).code())) {
        SE_LOG(WARN, "fail to append buffer", K(ret));
      } else {
        ++(header->extent_meta_block_count_);
      }
    }
  }

  //resource clean
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int ExtentMetaManager::load_checkpoint(util::RandomAccessFile *checkpoint_reader,
                                       CheckpointHeader *header)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;
  int64_t pos = 0;
  CheckpointBlockHeader *block_header = nullptr;
  ExtentMeta *extent_meta = nullptr;
  Slice result;
  int64_t block_index = 0;
  int64_t offset = header->extent_meta_block_offset_;

  SE_LOG(INFO, "begin to load extent space manager checkpoint");
  if (IS_NULL(checkpoint_reader) || IS_NULL(header)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_reader), KP(header));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(DIOHelper::DIO_ALIGN_SIZE, buf_size, memory::ModId::kExtentSpaceMgr)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else {
    util::SpinWLockGuard guard(meta_mutex_);
    while (SUCCED(ret) && block_index < header->extent_meta_block_count_) {
      pos = 0;
      if (FAILED(checkpoint_reader->Read(offset, buf_size, &result, buf).code())) {
          SE_LOG(WARN, "fail to read buf", K(ret), K(buf_size));
      } else {
        block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
        pos = block_header->data_offset_;
        for (int64_t i = 0; SUCCED(ret) && i < block_header->entry_count_; ++i) {
          ExtentMeta extent_meta_tmp;
          if (FAILED(extent_meta_tmp.deserialize(buf, buf_size, pos))) {
            SE_LOG(WARN, "fail to deserialize extent meta", K(ret));
          } else if (FAILED(extent_meta_tmp.deep_copy(extent_meta))) {
            SE_LOG(WARN, "fail to deep copy extent meta", K(ret));
          } else if (!(extent_meta_map_.emplace(extent_meta->extent_id_.id(), extent_meta).second)) {
            ret = Status::kCorruption;
            SE_LOG(WARN, "fail to emplace extent meta", K(ret));
          }
        }
        ++block_index;
        offset += buf_size;
      }
    }

    //check checkpoint
    if (SUCCED(ret)) {
      if (header->extent_count_ != static_cast<int64_t>(extent_meta_map_.size())) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "extent meta count is unexpected", K(ret), K(*header), K(extent_meta_map_.size()));
      }
    }
  }
  SE_LOG(INFO, "success to load extent space manager checkpoint");

  //resource clean
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int ExtentMetaManager::replay(int64_t log_type, char *log_data, int64_t log_length)
{
  int ret = Status::kOk;

  if (!is_extent_log(log_type) || IS_NULL(log_data) || log_length < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(log_type), KP(log_data), K(log_length));
  } else {
    switch (log_type) {
      case REDO_LOG_MODIFY_EXTENT_META:
        if (FAILED(replay_extent_meta_log(log_data, log_length))) {
          SE_LOG(WARN, "fail to replay extent meta log", K(ret), K(log_type), K(log_length));
        }
        break;
      default:
        ret = Status::kNotSupported;
        SE_LOG(WARN, "unknow log type", K(ret), K(log_type));
    }
  }

  return ret;
}

int ExtentMetaManager::clear_free_extent_meta()
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  auto iterator = extent_meta_map_.begin();
  while (Status::kOk == ret && iterator != extent_meta_map_.end()) {
    if (IS_NULL(extent_meta = iterator->second)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "extent meta must not nullptr", K(ret), K(iterator->first));
    } else if (0 == extent_meta->refs_) {
      SE_LOG(INFO, "clear free extent meta from extent space mgr", K(iterator->first), K(iterator->second));
      iterator = extent_meta_map_.erase(iterator);
      free_extent_meta(extent_meta);
    } else {
      ++iterator;
    }
  }

  return ret;

}

int ExtentMetaManager::validate_all_remote_extents()
{
  int ret = Status::kOk;
  if (!remote_extent::enabled()) return ret;

  std::vector<ExtentMeta> metadata;
  std::string validation_error;
  {
    util::SpinRLockGuard guard(meta_mutex_);
    metadata.reserve(extent_meta_map_.size());
    for (const auto &entry : extent_meta_map_) {
      if (IS_NULL(entry.second)) {
        ret = Status::kCorruption;
        validation_error = "remote extent metadata map contains null";
        SE_LOG(ERROR, "remote extent metadata map contains null", K(ret),
               "extent_id", entry.first);
        break;
      }
      metadata.push_back(*entry.second);
    }
  }

  for (const ExtentMeta &extent_meta : metadata) {
    if (FAILED(ret)) break;
    if (!remote_extent::verify_metadata_object(extent_meta, nullptr,
                                               &validation_error)) {
      ret = Status::kCorruption;
      SE_LOG(ERROR, "remote extent metadata verification failed", K(ret),
             K(validation_error), K(extent_meta));
    }
  }
  if (FAILED(ret)) remote_extent::enter_fenced(validation_error);
  return ret;
}

void ExtentMetaManager::free_extent_meta(ExtentMeta *&extent_meta)
{
  extent_meta->~ExtentMeta();
  memory::base_free(extent_meta);
  extent_meta = nullptr;
}

int ExtentMetaManager::reserve_checkpoint_block_header(char *buf,
                                                       int64_t buf_size,
                                                       int64_t &offset,
                                                       CheckpointBlockHeader *&block_header)
{
  int ret = Status::kOk;

  if (nullptr == buf || buf_size <= 0 || offset < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(buf_size), K(offset));
  } else {
    memset(buf, 0, buf_size);
    offset = 0;
    block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
    offset += sizeof(CheckpointBlockHeader);
    block_header->type_ = 0; //extent meta block
    block_header->block_size_ = buf_size;
    block_header->data_offset_ = offset;
  }

  return ret;
}

int ExtentMetaManager::replay_extent_meta_log(char *log_data, int64_t log_length)
{
  int ret = Status::kOk;
  int64_t pos = 0;
  ModifyExtentMetaLogEntry log_entry;

  if (nullptr == log_data || log_length < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    SE_LOG(WARN, "fail to deserialize log entry", K(ret), K(log_length));
  } else if (FAILED(replay_write_meta(log_entry.extent_meta_))) {
    SE_LOG(WARN, "fail to write meta", K(ret));
  } else {
    SE_LOG(DEBUG, "success to replay extent log", K(log_entry.extent_meta_));
  }

  return ret;
}

int ExtentMetaManager::replay_write_meta(const ExtentMeta &extent_meta)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta_ptr = nullptr;

  util::SpinWLockGuard guard(meta_mutex_);
  ExtentId extent_id = extent_meta.extent_id_;
  auto iterator = extent_meta_map_.find(extent_id.id());
  if (extent_meta_map_.end() != iterator) {
    if (IS_NULL(extent_meta_ptr = iterator->second)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "extent meta ptr must not nullptr", K(ret), K(extent_id));
    } else if (1 != extent_meta_map_.erase(extent_id.id())) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to erase extent id", K(ret), K(extent_id));
    } else {
      free_extent_meta(extent_meta_ptr);
    }
  }
  if (SUCCED(ret)) {
    if (FAILED(extent_meta.deep_copy(extent_meta_ptr))) {
      SE_LOG(WARN, "fail to deep copy extent meta", K(ret));
    } else if (IS_NULL(extent_meta_ptr)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, deep copy extent meta must not nullptr", K(ret), KP(extent_meta_ptr));
    } else if (!(extent_meta_map_.emplace(extent_meta_ptr->extent_id_.id(), extent_meta_ptr).second)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to emplace extent meta", K(ret), K(*extent_meta_ptr));
    }
  }

  return ret;
}

} //namespace storage
} //namespace smartengine
