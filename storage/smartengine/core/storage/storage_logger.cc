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

#include "storage/storage_logger.h"
#include "backup/hotbackup.h"
#include "backup/hotbackup_impl.h"
#include "db/log_reader.h"
#include "db/version_set.h"
#include "logger/log_module.h"
#include "objstore/snapshot_release_lock.h"
#include "storage/extent_meta_manager.h"
#include "util/file_reader_writer.h"
#include "util/file_name.h"
#include "util/string_util.h"

namespace smartengine
{
using namespace db;
using namespace common;
using namespace util;
using namespace memory;

namespace storage
{
__thread int64_t StorageLogger::local_trans_id_ = 0;
StorageLoggerBuffer::StorageLoggerBuffer()
    : data_(nullptr),
      pos_(0),
      capacity_(0)
{
}

StorageLoggerBuffer::~StorageLoggerBuffer()
{
}

int StorageLoggerBuffer::assign(char *buf, int64_t buf_len)
{
  int ret = Status::kOk;
  if (nullptr == buf || buf_len < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument.", K(ret), K(buf_len)); //TODO:@yuanfeng print buf
  } else {
    data_ = buf;
    pos_ = 0;
    capacity_ = buf_len;
  }
  return ret;
}

int StorageLoggerBuffer::append_log(ManifestLogEntryHeader &log_entry_header, const char *log_data, const int64_t log_len)
{
  int ret = Status::kOk;

  if (!log_entry_header.is_valid() || log_len < 0 || (nullptr == log_data && log_len > 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(log_entry_header), K(log_len));
  } else {
    int64_t header_size = log_entry_header.get_serialize_size();
    if ((header_size + log_len) > remain()) {
      ret = Status::kNoSpace;
      SE_LOG(INFO, "log buffer not enough", K(ret), K_(capacity), K_(pos), K(header_size), K(log_len));
    } else if (Status::kOk != (ret = log_entry_header.serialize(data_, capacity_, pos_))) {
      SE_LOG(WARN, "fail to serialize log entry header", K(ret), K_(capacity), K_(pos));
    } else {
      memcpy(data_ + pos_, log_data, log_len);
      pos_ += log_len;
    }
  }

  return ret;
}

int StorageLoggerBuffer::append_log(ManifestLogEntryHeader &log_entry_header, ManifestLogEntry &log_entry)
{
  int ret = Status::kOk;

  if (!log_entry_header.is_valid()) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument.", K(ret), K(log_entry_header));
  } else {
    int64_t header_size = log_entry_header.get_serialize_size();
    int64_t entry_size = log_entry.get_serialize_size();
    if ((header_size + entry_size) > remain()) {
      ret = Status::kNoSpace;
      SE_LOG(INFO, "log buffer not enough", K(ret), K(header_size), K(entry_size));
    } else if (Status::kOk != (ret = log_entry_header.serialize(data_, capacity_, pos_))) {
      SE_LOG(WARN, "fail to serialize log entry header", K(ret), K_(capacity), K_(pos));
    } else if (Status::kOk != (ret = log_entry.serialize(data_, capacity_, pos_))) {
      SE_LOG(WARN, "fail to serialize log entry", K(ret), K_(capacity), K_(pos));
    }
  }

  return ret;
}

int StorageLoggerBuffer::read_log(ManifestLogEntryHeader &log_entry_header, char *&log_data, int64_t &log_len)
{
  int ret = Status::kOk;

  if (pos_ >= capacity_) {
    ret = Status::kIterEnd;
  } else if (FAILED(log_entry_header.deserialize(data_, capacity_, pos_))) {
    SE_LOG(WARN, "fail to deserialize ManifestLogEntryHeader", K(ret));
  } else {
    log_data = data_ + pos_;
    log_len = log_entry_header.log_entry_length_;
    pos_ += log_len;
  }

  return ret;
}

void CheckpointBlockHeader::reset()
{
  type_ = 0;
  block_size_ = 0;
  entry_count_ = 0;
  data_offset_ = 0;
  data_size_ = 0;
}
DEFINE_TO_STRING(CheckpointBlockHeader, KV_(type), KV_(block_size), KV_(entry_count),
                 KV_(data_offset), KV_(data_size), KV(reserve_[0]), KV(reserve_[1]))

StorageLogger::TransContext::TransContext(bool need_reused)
    : event_(INVALID_EVENT),
      log_buffer_(),
      log_count_(0),
      need_reused_(need_reused)
{
}

StorageLogger::TransContext::~TransContext()
{
}

void StorageLogger::TransContext::reuse()
{
  event_ = INVALID_EVENT;
  log_buffer_.reuse();
  log_count_ = 0;
}

int StorageLogger::TransContext::append_log(int64_t trans_id,
                                            ManifestRedoLogType log_type,
                                            const char *log_data,
                                            const int64_t log_len)
{
  int ret = Status::kOk;
  ManifestLogEntryHeader log_entry_header;

  if (trans_id < 0 || log_type < 0 || log_len < 0 || (log_len > 0 && nullptr == log_data)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument.", K(ret), K(trans_id), K(log_len));
  } else {
    log_entry_header.trans_id_ = trans_id;
    log_entry_header.log_entry_seq_ = log_count_;
    log_entry_header.log_entry_type_ = log_type;
    log_entry_header.log_entry_length_ = log_len;
    if (Status::kOk != (ret = log_buffer_.append_log(log_entry_header, log_data, log_len))) {
      SE_LOG(WARN, "fail to append log to log buffer", K(ret), K(log_entry_header), K(log_len));
    } else {
      ++log_count_;
    }
  }

  return ret;
}

int StorageLogger::TransContext::append_log(int64_t trans_id,
                                            ManifestRedoLogType log_type,
                                            ManifestLogEntry &log_entry)
{
  int ret = Status::kOk;
  ManifestLogEntryHeader log_entry_header;

  if (trans_id < 0 || log_type < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(trans_id));
  } else {
    log_entry_header.trans_id_ = trans_id;
    log_entry_header.log_entry_seq_ = log_count_;
    log_entry_header.log_entry_type_ = log_type;
    log_entry_header.log_entry_length_ = log_entry.get_serialize_size();
    if (Status::kOk != (ret = log_buffer_.append_log(log_entry_header, log_entry))) {
      SE_LOG(WARN, "fail to append log to log buffer", K(ret), K(log_entry_header));
    } else {
      ++log_count_;
    }
  }

  return ret;
}

//DEFINE_COMPACTIPLE_SERIALIZATION(CheckpointHeader, extent_count_, partition_group_count_);
DEFINE_TO_STRING(CheckpointHeader, KV_(start_log_id), KV_(extent_count), KV_(sub_table_count),
                 KV_(extent_meta_block_count), KV_(sub_table_meta_block_count), KV_(extent_meta_block_offset),
                 KV_(sub_table_meta_block_offset))

StorageLogger::StorageLogger()
    : is_inited_(false),
      env_(nullptr),
      db_name_(),
      env_options_(),
      db_options_(),
      log_file_number_(0),
      version_set_(nullptr),
      curr_manifest_log_size_(0),
      log_writer_(nullptr),
      max_manifest_log_file_size_(0),
      log_number_(0),
      global_trans_id_(0),
      map_mutex_(),
      trans_ctx_map_(),
      log_sync_mutex_(),
      log_buf_(nullptr),
      trans_ctxs_(),
      allocator_(),
      current_manifest_file_number_(0),
      trans_pool_mutex_(),
      active_trans_cnt_(0)
{
}

StorageLogger::~StorageLogger()
{
  destroy();
}

StorageLogger &StorageLogger::get_instance()
{
  static StorageLogger storage_logger;
  return storage_logger;
}

int StorageLogger::init(Env *env,
                        std::string db_name,
                        EnvOptions env_options,
                        ImmutableDBOptions db_options,
                        db::VersionSet *version_set,
                        int64_t max_manifest_log_file_size)
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;
  char *tmp_buf = nullptr;

  if (is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger has been inited.", K(ret));
  } else if (IS_NULL(env)
             || IS_NULL(version_set)
             || 0 >= max_manifest_log_file_size) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), KP(version_set), K(max_manifest_log_file_size));
  } else if (IS_NULL(tmp_buf = reinterpret_cast<char *>(allocator_.alloc(sizeof(TransContext *) * DEFAULT_TRANS_CONTEXT_COUNT)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for trans ctxs buf", K(ret));
  } else if (FAILED(trans_ctxs_.init(DEFAULT_TRANS_CONTEXT_COUNT, tmp_buf))) {
    SE_LOG(WARN, "fail to init trans_ctxs", K(ret));
  } else if (IS_NULL(log_buf_ = reinterpret_cast<char *>(allocator_.alloc(MAX_LOG_SIZE)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for log buf", K(ret));
  } else {
    for (int64_t i = 0; SUCCED(ret) && i < DEFAULT_TRANS_CONTEXT_COUNT; ++i) {
      if (FAILED(construct_trans_ctx(true /*need_reused*/, trans_ctx))) {
        SE_LOG(WARN, "fail to construct trans ctx", K(ret));
      } else if (IS_NULL(trans_ctx)) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, trans ctx must not nullptr", K(ret));
      } else if (FAILED(trans_ctxs_.push(trans_ctx))) {
        SE_LOG(WARN, "fail to push TransContext", K(ret));
      }
    }
  }

  if (Status::kOk == ret) {
    env_ = env;
    db_name_ = db_name;
    env_options_ = env_options;
    db_options_ = db_options;
    version_set_ = version_set;
    max_manifest_log_file_size_ = max_manifest_log_file_size;
    is_inited_ = true;
  }
  SE_LOG(INFO, "StorageLogger init finish", K(ret), K(log_file_number_),
              K(db_name_.c_str()), K(max_manifest_log_file_size_));
  return ret;
}

void StorageLogger::destroy()
{
  if (is_inited_) {
    TransContext *trans_ctx = nullptr;
    while (Status::kOk == trans_ctxs_.pop(trans_ctx)) {
      deconstruct_trans_context(trans_ctx);
    }
    trans_ctxs_.destroy();
    allocator_.free(log_buf_);
    destroy_log_writer(log_writer_);
    log_file_number_ = 0;
    is_inited_ = false;
  }
}

int StorageLogger::begin(enum SeEvent event)
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;
  int64_t trans_id = 0;
  ManifestLogEntryHeader header;
  bool can_write_checkpoint = false;

  std::lock_guard<std::mutex> guard(trans_pool_mutex_);
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger not been inited", K(ret));
  } else if (0 != local_trans_id_) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, may have dirty trans", K(ret), K_(local_trans_id));
  } else {
    //write checkpoint is a low priority task, should avoid influences other system task as far as possible
    //satisfy the prev condition curr_manifest_log_size_ more than max_manifest_log_file_size_ and satisfy one of the following condition, can write checkpoint
    //condition 1: has no active trans now
    //condition 2: curr_manifest_log_size_ is too large (more than FORCE_WRITE_CHECKPOINT_MULTIPLE of
    //max_manifest_log_file_size_), which may cause recovery very slow
    if (curr_manifest_log_size_.load() > max_manifest_log_file_size_) {
      SE_LOG(INFO, "check if can write checkpoint", "active_trans_cnt", get_active_trans_cnt(),
          K_(curr_manifest_log_size), K_(max_manifest_log_file_size));
      if (0 == get_active_trans_cnt())  {
        can_write_checkpoint = true;
      } else {
        if (curr_manifest_log_size_.load() > FORCE_WRITE_CHECKPOINT_MULTIPLE * max_manifest_log_file_size_) {
          wait_trans_barrier();
          can_write_checkpoint = true;
        }
      }

      if (can_write_checkpoint) {
        if (FAILED(internal_write_checkpoint())) {
          SE_LOG(WARN, "fail to internal write checkpoint", K(ret));
        }
      }
    }

    if (SUCCED(ret)) {
      SE_LOG(INFO, "begin manifest trans");
      if (FAILED(alloc_trans_ctx(trans_ctx))) {
        SE_LOG(WARN, "fail to alloc trans ctx", K(ret));
      } else if (IS_NULL(trans_ctx)) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "trans_ctx must not nullptr", K(ret));
      } else {
        trans_id = ++global_trans_id_;
        trans_ctx->event_ = event;
        if (Status::kOk != (ret = trans_ctx->append_log(trans_id,
                                                        REDO_LOG_BEGIN,
                                                        nullptr,
                                                        0))) {
          SE_LOG(WARN, "fail to append begin log", K(ret));
        } else {
          std::lock_guard<std::mutex> lock_guard(map_mutex_);
          if (!(trans_ctx_map_.emplace(std::make_pair(trans_id, trans_ctx)).second)) {
            SE_LOG(WARN, "fail to emplace trans context", K(ret), K(trans_id));
          } else {
            local_trans_id_ = trans_id;
            inc_active_trans_cnt();
            SE_LOG(INFO, "inc active trans cnt", "active_trans_cnt", active_trans_cnt_.load());
          }
        }
      }
    }
  }

  //resource clean
  if (FAILED(ret)) {
    //avoid ret overwrite
    int tmp_ret = Status::kOk;
    if (nullptr != trans_ctx) {
      std::lock_guard<std::mutex> lock_guard(map_mutex_);
      trans_ctx_map_.erase(trans_id);
    }
    if (nullptr != trans_ctx) {
      if (Status::kOk != (tmp_ret = free_trans_ctx(trans_ctx))) {
        SE_LOG(WARN, "fail to free trans ctx", K(tmp_ret));
      }
    }
    local_trans_id_ = 0;
  }

  return ret;
}

int StorageLogger::commit(int64_t &log_seq_number)
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;
  std::unordered_map<int64_t, TransContext *>::iterator trans_ctx_iterator;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited", K(ret));
  } else {
    std::lock_guard<std::mutex> lock_guard(map_mutex_);
    if (trans_ctx_map_.end() == (trans_ctx_iterator = trans_ctx_map_.find(local_trans_id_))) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "can not find trans context", K(ret), K_(local_trans_id));
    } else if (nullptr == (trans_ctx = trans_ctx_iterator->second)) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "trans context must not null", K(ret), K_(local_trans_id));
    }
  }

  if (Status::kOk == ret) {
    if (Status::kOk != (ret = trans_ctx->append_log(local_trans_id_, REDO_LOG_COMMIT, nullptr, 0))) {
      if (Status::kNoSpace != ret) {
        SE_LOG(WARN, "fail to append commit log", K(ret), K_(local_trans_id));
      } else if (Status::kOk != (ret = flush_log(*trans_ctx))) {
        SE_LOG(WARN, "fail to flush log", K(ret));
      } else if (Status::kOk != (ret = trans_ctx->append_log(local_trans_id_, REDO_LOG_COMMIT, nullptr, 0))) {
        SE_LOG(WARN, "fail to append log", K(ret), K_(local_trans_id));
      }
    }
  }

  if (Status::kOk == ret) {
    if (Status::kOk != (ret = flush_log(*trans_ctx, &log_seq_number))) {
      SE_LOG(WARN, "fail to flush log", K(ret), K_(local_trans_id));
    } else if (FAILED(free_trans_ctx(trans_ctx))) {
      SE_LOG(WARN, "fail to free trans ctx", K(ret));
    } else {
      std::lock_guard<std::mutex> lock_guard(map_mutex_);
      if (1 != trans_ctx_map_.erase(local_trans_id_)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "fail to erase trans ctx", K(ret), K_(local_trans_id));
      } else {
        local_trans_id_ = 0;
      }
    }
  }

  //anyway, decrease active trans count
  dec_active_trans_cnt();
  SE_LOG(INFO, "dec active trans cnt", "active_trans_cnt", active_trans_cnt_.load());
  return ret;
}

int StorageLogger::abort()
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;

  if (0 == local_trans_id_) {
    SE_LOG(INFO, "trans ctx has been cleaned, no need to do more thing");
  } else {
    std::lock_guard<std::mutex> lock_guard(map_mutex_);
    auto iter = trans_ctx_map_.find(local_trans_id_);
    if (trans_ctx_map_.end() == iter) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to find the trans", K(ret), K_(local_trans_id));
    } else if (nullptr == (trans_ctx = iter->second)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "trans ctx must not nullptr", K(ret), K_(local_trans_id));
    } else if (1 != trans_ctx_map_.erase(local_trans_id_)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to erase trans ctx", K(ret), K_(local_trans_id));
    } else if (FAILED(free_trans_ctx(trans_ctx))) {
      SE_LOG(WARN, "fail to free trans ctx", K(ret), K_(local_trans_id));
    } else {
      local_trans_id_ = 0;
    }
  }

  //anyway, decrease active trans count
  dec_active_trans_cnt();
  SE_LOG(INFO, "dec active trans cnt", "active_trans_cnt", active_trans_cnt_.load());

  return ret;
}

int StorageLogger::write_log(const ManifestRedoLogType log_type, ManifestLogEntry &log_entry)
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;
  std::unordered_map<int64_t, TransContext *>::iterator trans_ctx_iterator;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (log_type < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret));
  } else {
    std::lock_guard<std::mutex> guard(map_mutex_);
    if (trans_ctx_map_.end() == (trans_ctx_iterator = trans_ctx_map_.find(local_trans_id_))) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "can not find trans context", K(ret), K_(local_trans_id));
    } else if (nullptr == (trans_ctx = trans_ctx_iterator->second)){
      ret = Status::kCorruption;
      SE_LOG(WARN, "trans context must not null", K(ret), K_(local_trans_id));
    }
  }

  if (Status::kOk == ret) {
    if (Status::kOk != (ret = trans_ctx->append_log(local_trans_id_, log_type, log_entry))) {
      if (Status::kNoSpace != ret) {
        SE_LOG(WARN, "fail to append log", K(ret), K_(local_trans_id));
      } else if (Status::kOk != (ret = flush_log(*trans_ctx))) {
        SE_LOG(WARN, "fail to flush log", K(ret));
      } else if (Status::kOk != (ret = trans_ctx->append_log(local_trans_id_, log_type, log_entry))) {
        if (Status::kNoSpace != ret) {
          SE_LOG(WARN, "fail to append log", K(ret), K_(local_trans_id));
        } else if (FAILED(flush_large_log_entry(*trans_ctx, log_type, log_entry))) {
          SE_LOG(ERROR, "fail to flush large log entry", K(ret));
        }
      }
    }
  }

  return ret;
}

int StorageLogger::external_write_checkpoint()
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(trans_pool_mutex_);
  wait_trans_barrier();
  if (FAILED(internal_write_checkpoint())) {
    SE_LOG(WARN, "fail to write checkpoint", K(ret));
  }

  return ret;
}

int StorageLogger::replay(memory::ArenaAllocator &arena)
{
  int ret = Status::kOk;

  if (FAILED(load_checkpoint(arena))) {
    SE_LOG(WARN, "fail to load checkpoint", K(ret));
  } else if (FAILED(replay_after_ckpt(arena))) {
    SE_LOG(WARN, "fail to replay after checkpoint", K(ret));
  } else if (FAILED(update_log_writer(++log_file_number_))) {
    SE_LOG(WARN, "fail to create log writer", K(ret), K_(log_file_number));
  } else if (FAILED(version_set_->recover_M02L0())) {
    SE_LOG(WARN, "failed to recover m0 to l0", K(ret));
  } else if (FAILED(version_set_->recover_extent_space_manager())) {
    SE_LOG(ERROR, "fail to recover extent space manager", K(ret));
  } else {
    SE_LOG(SYSTEM, "success to replay manifest", K_(log_file_number));
  }

  return ret;
}

int StorageLogger::alloc_trans_ctx(TransContext *&trans_ctx)
{
  int ret = Status::kOk;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(trans_ctxs_.pop(trans_ctx))) {
    if (e_ENTRY_NOT_EXIST != ret) {
      SE_LOG(WARN, "fail to pop trans context", K(ret));
    } else if (FAILED(construct_trans_ctx(false /*need_reused*/, trans_ctx))) {
      SE_LOG(WARN, "fail to construc new trans ctx", K(ret));
    } else if (IS_NULL(trans_ctx)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, trans_ctx must not nullptr", K(ret));
    } else {
      SE_LOG(DEBUG, "alloc trans_ctx from system");
    }
  } else {
    SE_LOG(DEBUG, "alloc trans_ctx from trans_ctxs");
  }

  return ret;
}

int StorageLogger::free_trans_ctx(TransContext *trans_ctx)
{
  int ret = Status::kOk;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (nullptr == trans_ctx) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(trans_ctx));
  } else {
    if (trans_ctx->need_reused()) {
      trans_ctx->reuse();
      if (FAILED(trans_ctxs_.push(trans_ctx))) {
        SE_LOG(WARN, "fail to push trans_ctx", K(ret));
      } else {
        SE_LOG(DEBUG, "free trans_ctx to trans_ctxs");
      }
    } else {
      if (FAILED(deconstruct_trans_context(trans_ctx))) {
        SE_LOG(WARN, "fail to deconstruct trans context", K(ret));
      } else {
        SE_LOG(DEBUG, "free trans_ctx to system");
      }
    }
  }

  return ret;
}

int StorageLogger::construct_trans_ctx(bool need_reused, TransContext *&trans_ctx)
{
  int ret = Status::kOk;
  char *log_buf = nullptr;
  trans_ctx = nullptr;

  if (IS_NULL(trans_ctx = MOD_NEW_OBJECT(ModId::kStorageLogger, TransContext, need_reused))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for TransContext", K(ret));
  } else if (IS_NULL(log_buf = reinterpret_cast<char *>(base_malloc(DEFAULT_LOGGER_BUFFER_SIZE, ModId::kStorageLogger)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for log buf", K(ret));
  } else if (FAILED(trans_ctx->log_buffer_.assign(log_buf, DEFAULT_LOGGER_BUFFER_SIZE))) {
    SE_LOG(WARN, "fail to set TransContext buffer", K(ret));
  }

  return ret;
}

int StorageLogger::deconstruct_trans_context(TransContext *trans_ctx)
{
  int ret = Status::kOk;

  if (IS_NULL(trans_ctx)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(trans_ctx));
  } else {
    base_free(trans_ctx->log_buffer_.data());
    MOD_DELETE_OBJECT(TransContext, trans_ctx);
  }

  return ret;
}

int StorageLogger::flush_log(TransContext &trans_ctx, int64_t *log_seq_num)
{
  int ret = Status::kOk;
  int64_t pos = 0;
  LogHeader log_header;
  std::string record;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited", K(ret));
  } else {
    //this critital sections a little large, But there should not has performace problem, because log buffer include many logs
    std::lock_guard<std::mutex> guard(log_sync_mutex_);
    log_header.event_ = trans_ctx.event_;
    log_header.log_id_ = log_number_;
    log_header.log_length_ = trans_ctx.log_buffer_.length();
    if (Status::kOk != (ret = log_header.serialize(log_buf_, MAX_LOG_SIZE, pos))) {
      SE_LOG(WARN, "fail to serialize LogHeader", K(ret));
    } else {
      memcpy(log_buf_ + pos, trans_ctx.log_buffer_.data(), trans_ctx.log_buffer_.length());
      pos += trans_ctx.log_buffer_.length();

      if (Status::kOk != (ret = log_writer_->AddRecord(Slice(log_buf_, pos)).code())) {
        SE_LOG(WARN, "fail to add record", K(ret), K(pos));
      } else if (FAILED(log_writer_->sync())) {
        SE_LOG(WARN, "fail to sync manifest log", K(ret));
      } else {
        if (nullptr != log_seq_num) {
          *log_seq_num = log_number_;
        }
        ++log_number_;
        //accumulate current manifest log size
        curr_manifest_log_size_.fetch_add(pos);
        trans_ctx.log_buffer_.reuse();
      }
    }
  }

  return ret;
}

int StorageLogger::flush_large_log_entry(TransContext &trans_ctx, ManifestRedoLogType log_type, ManifestLogEntry &log_entry)
{
  int ret = Status::kOk;
  char *manifest_log_entry_buf = nullptr;
  int64_t manifest_log_entry_buf_size = 0;
  int64_t manifest_log_entry_buf_pos = 0;
  char *log_buf = nullptr;
  int64_t log_buf_size = 0;
  int64_t log_buf_pos = 0;
  LogHeader log_header;
  ManifestLogEntryHeader log_entry_header;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited", K(ret));
  } else if (log_type < 0) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret));
  } else {
    //ManifestLogEntryHeader size + ManifestLogEntry size + reserved size(512byte)
    manifest_log_entry_buf_size = sizeof(ManifestLogEntryHeader) + log_entry.get_serialize_size() + 512;
    //LogHeader size + manifest_log_entry_buf_size + reserved size(512byte)
    log_buf_size = sizeof(LogHeader) + manifest_log_entry_buf_size + 512;
    if (nullptr == (manifest_log_entry_buf = reinterpret_cast<char *>(base_malloc(manifest_log_entry_buf_size, ModId::kStorageLogger)))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for manifest_log_entry_buf", K(ret), K(manifest_log_entry_buf_size));
    } else if (nullptr == (log_buf = reinterpret_cast<char *>(base_malloc(log_buf_size, ModId::kStorageLogger)))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for log buffer", K(ret));
    } else {
      std::lock_guard<std::mutex> guard(log_sync_mutex_);
      log_entry_header.trans_id_ = local_trans_id_;
      log_entry_header.log_entry_seq_ = trans_ctx.log_count_;
      log_entry_header.log_entry_type_ = log_type;
      log_entry_header.log_entry_length_ = log_entry.get_serialize_size();
      if (FAILED(log_entry_header.serialize(manifest_log_entry_buf, manifest_log_entry_buf_size, manifest_log_entry_buf_pos))) {
        SE_LOG(WARN, "fail to serialize manifest log entry header", K(ret));
      } else if (FAILED(log_entry.serialize(manifest_log_entry_buf, manifest_log_entry_buf_size, manifest_log_entry_buf_pos))) {
        SE_LOG(WARN, "fail to serialize manifest log entry", K(ret));
      } else {
        log_header.event_ = trans_ctx.event_;
        log_header.log_id_ = log_number_;
        log_header.log_length_ = manifest_log_entry_buf_pos;
        if (FAILED(log_header.serialize(log_buf, log_buf_size, log_buf_pos))) {
          SE_LOG(WARN, "failed to serialize LogHeader", K(ret));
        } else if (log_buf_pos + manifest_log_entry_buf_pos > log_buf_size) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "log buf must enough", K(ret), K(log_buf_pos), K(manifest_log_entry_buf_pos), K(log_buf_size));
        } else {
          memcpy(log_buf + log_buf_pos, manifest_log_entry_buf, manifest_log_entry_buf_pos);
          log_buf_pos += manifest_log_entry_buf_pos;
          if (FAILED(log_writer_->AddRecord(Slice(log_buf, log_buf_pos)).code())) {
            SE_LOG(WARN, "fail to add record", K(ret), K(log_buf_pos));
          } else if (FAILED(log_writer_->sync())) {
            SE_LOG(WARN, "fail to sync manifest log", K(ret));
          } else {
            ++(trans_ctx.log_count_);
            ++log_number_;
          }
        }
      }
    }
  }

  if (nullptr != manifest_log_entry_buf) {
    base_free(manifest_log_entry_buf);
    manifest_log_entry_buf = nullptr;
  }
  if (nullptr != log_buf) {
    base_free(log_buf);
    log_buf = nullptr;
  }

  return ret;
}

int StorageLogger::load_checkpoint(memory::ArenaAllocator &arena)
{
  int ret = Status::kOk;
  std::string manifest_name;
  std::string checkpoint_name;
  uint64_t log_number = 0;
  int64_t file_offset = 0;
  char *header_buffer = nullptr;
  CheckpointHeader *header = nullptr;
  Slice result;
  util::RandomAccessFile *checkpoint_reader = nullptr;
  EnvOptions opt_env_opts = env_options_;
  opt_env_opts.use_direct_reads = false;
  opt_env_opts.arena = &arena;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (IS_NULL(header_buffer = static_cast<char *>(
                         memory::base_malloc(DEFAULT_BUFFER_SIZE, memory::ModId::kStorageLogger)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for header buffer", K(ret));
  } else {
    memset(header_buffer, 0, DEFAULT_BUFFER_SIZE);
    header = reinterpret_cast<CheckpointHeader *>(header_buffer);

    if (FAILED(parse_current_file(checkpoint_name, manifest_name, log_number))) {
      SE_LOG(WARN, "fail to parse current checkpoint file", K(ret));
    } else if (FAILED(env_->NewRandomAccessFile(db_name_ + "/" + checkpoint_name, checkpoint_reader, opt_env_opts)
                          .code())) {
      SE_LOG(WARN, "fail to create checkpoint reader", K(ret));
    } else if (FAILED(checkpoint_reader->Read(file_offset, DEFAULT_BUFFER_SIZE, &result, header_buffer).code())) {
      SE_LOG(WARN, "fail to read checkpoint header", K(ret), K(file_offset));
    } else if (FAILED(ExtentMetaManager::get_instance().load_checkpoint(checkpoint_reader, header))) {
      SE_LOG(WARN, "fail to load extent meta checkpoint", K(ret));
    } else if (FAILED(version_set_->load_checkpoint(checkpoint_reader, header))) {
      SE_LOG(WARN, "fail to load partition group meta checkpoint", K(ret));
    } else {
      SE_LOG(SYSTEM, "success to load checkpoint", K(checkpoint_name.c_str()), K(log_number));
    }
    FREE_OBJECT(RandomAccessFile, arena, checkpoint_reader);
  }
  if (header_buffer) {
    memory::base_free(header_buffer);
  }
  return ret;
}

// get the valid manifest range
int StorageLogger::manifest_file_range(int32_t &begin, int32_t &end, int64_t &end_pos) {
  int ret = Status::kOk;
  std::string manifest_name;
  if (FAILED(manifest_file_in_current(manifest_name))) {
    SE_LOG(WARN, "fail to read manifest information", K(ret));
  } else {
    manifest_name.pop_back(); // remove the \n
    begin = std::stoi(manifest_name.c_str() + 9);
    end = current_manifest_file_number_;
    end_pos = current_manifest_file_size();
  }

  return ret;
}

#ifndef NDEBUG
void StorageLogger::TEST_reset()
{
  StorageLogger::local_trans_id_ = 0;
}

int StorageLogger::set_log_writer(int64_t file_number)
{
  return update_log_writer(file_number);
}
#endif

int StorageLogger::replay_after_ckpt(memory::ArenaAllocator &arena)
{
  int ret = Status::kOk;
  Slice record;
  std::string scrath;
  log::Reader *reader = nullptr;
  LogHeader log_header;
  StorageLoggerBuffer log_buffer;
  char *log_buf = nullptr;
  ManifestLogEntryHeader log_entry_header;
  char *log_data = nullptr;
  int64_t log_len = 0;
  std::unordered_set<int64_t> commited_trans_ids;
  int64_t pos = 0;
  std::string manifest_name;
  int64_t log_file_number = 0;
  std::string start_checkpoint_name;
  std::string checkpoint_name;
  uint64_t log_number;

  SE_LOG(INFO, "begin to replay after checkpoint");
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(get_commited_trans(commited_trans_ids))) {
    SE_LOG(WARN, "fail to get commited trans", K(ret));
  } else if (FAILED(parse_current_file(start_checkpoint_name, manifest_name, log_number))) {
    SE_LOG(WARN, "fail to parse current file", K(ret));
  } else {
    log_file_number = log_file_number_;
    manifest_name = FileNameUtil::manifest_file_path(db_name_, log_file_number);
    checkpoint_name = FileNameUtil::checkpoint_file_path(db_name_, log_file_number);
    //only when file exists, do actual thing, so we not set ret
    while (Status::kOk == ret) {
      if (Status::kOk == (env_->FileExists(manifest_name).code())) {
        // need replay this manifest file
      } else if (Status::kOk == (env_->FileExists(checkpoint_name).code())) {
        SE_LOG(INFO, "skip checkpoint file and replay next manifest file", K(checkpoint_name));
        log_file_number++;
        manifest_name = FileNameUtil::manifest_file_path(db_name_, log_file_number);
        checkpoint_name = FileNameUtil::checkpoint_file_path(db_name_, log_file_number);
        continue;
      } else {
        break;
      }
      SE_LOG(SYSTEM, "replay manifest file", K(log_file_number));
      if (FAILED(create_log_reader(manifest_name, reader, arena))) {
        SE_LOG(WARN, "fail to create log reader", K(ret));
      } else {
        while (Status::kOk == ret && reader->ReadRecord(&record, &scrath)) {
          pos = 0;
          log_buf = const_cast<char *>(record.data());
          if (FAILED(log_header.deserialize(log_buf, record.size(), pos))) {
            SE_LOG(WARN, "fail to deserialize log header", K(ret));
          } else {
            log_buffer.assign(log_buf + pos, log_header.log_length_);
            while(Status::kOk == ret &&
              Status::kOk == (ret = log_buffer.read_log(log_entry_header, log_data, log_len))) {
              if (commited_trans_ids.end() == commited_trans_ids.find(log_entry_header.trans_id_)) {
                SE_LOG(WARN, "this log not commited, ignore it", K(ret), K(log_entry_header));
              } else {
                if (is_trans_log(log_entry_header.log_entry_type_)) {
                  //trans log, not need replay
                  SE_LOG(INFO, "trans log", K(log_entry_header));
                } else if (is_partition_log(log_entry_header.log_entry_type_)) {
                  if (FAILED(version_set_->replay(log_entry_header.log_entry_type_, log_data, log_len))) {
                    SE_LOG(WARN, "fail to replay partition log", K(ret), K(log_entry_header));
                  }
                } else if (is_extent_log(log_entry_header.log_entry_type_)) {
                  if (FAILED(ExtentMetaManager::get_instance().replay(log_entry_header.log_entry_type_, log_data, log_len))) {
                    SE_LOG(WARN, "fail to replay extent meta log", K(ret), K(log_entry_header));
                  }
                } else if (is_backup_snapshot_log(log_entry_header.log_entry_type_)) {
                  if (FAILED(version_set_->replay_backup_snapshot_log(log_entry_header.log_entry_type_,
                                                                      log_data,
                                                                      log_len))) {
                    SE_LOG(WARN, "fail to replay backup snapshot log", K(ret), K(log_entry_header));
                  }
                } else {
                  ret = Status::kNotSupported;
                  SE_LOG(WARN, "not support log type", K(ret), K(log_entry_header));
                }
              }
            }
            if (Status::kIterEnd != ret) {
              SE_LOG(WARN, "fail to read log", K(ret));
            } else {
              ret = Status::kOk;
              curr_manifest_log_size_.fetch_add(record.size());
            }
          }
        }
      }
      destroy_log_reader(reader, &arena);
      ++log_file_number;
      manifest_name = FileNameUtil::manifest_file_path(db_name_, log_file_number);
      checkpoint_name = FileNameUtil::checkpoint_file_path(db_name_, log_file_number);
    }
    log_file_number_ = log_file_number - 1;
  }

  if (SUCCED(ret) && env_->IsObjectStoreInited()) {
    objstore::ObjectStore *objstore = nullptr;
    std::string_view objstore_bucket = env_->GetObjectStoreBucket();
    const std::string &cluster_objstore_id = env_->GetClusterObjstoreId();
    if (FAILED(env_->GetObjectStore(objstore).code())) {
      SE_LOG(WARN, "fail to get object store", K(ret));
    } else {
      std::string err_msg;
      BackupSnapshotMap *backup_snapshots = &BackupSnapshotImpl::get_instance()->get_backup_snapshot_map();
      uint64_t auto_increment_id_for_recover = backup_snapshots->get_max_auto_increment_id() + 1;
      if (FAILED(objstore::tryBackupRecoveringLock(auto_increment_id_for_recover,
                                                   objstore,
                                                   objstore_bucket,
                                                   cluster_objstore_id,
                                                   err_msg))) {
        SE_LOG(WARN, "fail to try backup recovering lock", K(ret), K(err_msg));
      } else if (FAILED(objstore::removeObsoletedBackupStatusLockFiles(objstore,
                                                                       objstore_bucket,
                                                                       cluster_objstore_id,
                                                                       auto_increment_id_for_recover,
                                                                       err_msg))) {
        SE_LOG(WARN, "fail to remove obsoleted backup status lock files", K(ret), K(err_msg));
      } else {
        backup_snapshots->save_auto_increment_id_for_recover(auto_increment_id_for_recover);
      }
    }
  }

  SE_LOG(SYSTEM, "success to replay after checkpoint", K_(log_file_number));

  return ret;
}

int StorageLogger::internal_write_checkpoint()
{
  int ret = Status::kOk;
  CheckpointHeader *header = nullptr;
  WritableFile *checkpoint_writer = nullptr;
  int64_t checkpoint_file_number = log_file_number_++;
  int64_t manifest_file_number = log_file_number_++;
  std::string checkpoint_path = FileNameUtil::checkpoint_file_path(db_name_, checkpoint_file_number);
  char buf[MAX_FILE_PATH_SIZE];
  EnvOptions opt_env_opts = env_options_;
  opt_env_opts.use_direct_writes = true;

  char *header_buffer = nullptr;
  SE_LOG(INFO, "begin to write checkpoint");
  if (IS_NULL(header_buffer = reinterpret_cast<char *>(base_memalign(
      DIOHelper::DIO_ALIGN_SIZE, DEFAULT_BUFFER_SIZE, ModId::kDefaultMod)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for header buffer", K(ret));
  } else {
    memset(header_buffer, 0, DEFAULT_BUFFER_SIZE);
    header = reinterpret_cast<CheckpointHeader *>(header_buffer);
    if (FAILED(update_log_writer(manifest_file_number))) {
      SE_LOG(WARN, "fail to create new log writer", K(ret));
    } else if (FAILED(NewWritableFile(env_, checkpoint_path, checkpoint_writer, opt_env_opts).code())) {
      SE_LOG(WARN, "fail to create checkpoint writer", K(ret));
    } else if (FAILED(checkpoint_writer->PositionedAppend(Slice(header_buffer, DEFAULT_BUFFER_SIZE), 0).code())) {
      SE_LOG(WARN, "fail to write checkpoint header", K(ret));
    } else if (FAILED(ExtentMetaManager::get_instance().do_checkpoint(checkpoint_writer, header))) {
      SE_LOG(WARN, "fail to do extent checkpoint", K(ret));
    } else if (FAILED(version_set_->do_checkpoint(checkpoint_writer, header))) {
      SE_LOG(WARN, "fail to do partition group checkpoint", K(ret));
    } else {
      header->start_log_id_ = log_number_;
      SE_LOG(INFO, "doing the checkpoint", K(log_number_));
      if (FAILED(checkpoint_writer->PositionedAppend(Slice(header_buffer, DEFAULT_BUFFER_SIZE), 0).code())) {
        SE_LOG(WARN, "fail to rewrite checkpoint header", K(ret));
      } else if (FAILED(checkpoint_writer->Fsync().code())) {
        SE_LOG(WARN, "fail to sync the data", K(ret), K(checkpoint_file_number), K(manifest_file_number));
      } else if (FAILED(write_current_file(checkpoint_file_number, manifest_file_number))) {
        SE_LOG(WARN, "fail to write current file", K(ret), K(manifest_file_number));
      } else {
        //reset current manifest log size to zero after do checkpoint
        curr_manifest_log_size_.store(0);
      }
    }
  }
  SE_LOG(INFO, "success to do write checkpoint");

  //resource clean
  if (nullptr != header_buffer) {
    base_memalign_free(header_buffer);
    header_buffer = nullptr;
  }
  MOD_DELETE_OBJECT(WritableFile, checkpoint_writer);
  return ret;
}

int StorageLogger::update_log_writer(int64_t manifest_file_number)
{
  int ret = Status::kOk;
  db::log::Writer *log_writer = nullptr;

  if (FAILED(create_log_writer(manifest_file_number, log_writer))) {
    SE_LOG(WARN, "fail to create log writer", K(ret), K(manifest_file_number));
  } else {
    destroy_log_writer(log_writer_);
    log_writer_ = log_writer;
    current_manifest_file_number_ = manifest_file_number;
  }

  return ret;
}

int StorageLogger::write_current_file(int64_t checkpoint_file_number, int64_t manifest_file_number)
{
  int ret = Status::kOk;
  char buf[MAX_FILE_PATH_SIZE];
  std::string tmp_file = FileNameUtil::temp_file_path(db_name_, manifest_file_number);
  std::string checkpoint_name = FileNameUtil::checkpoint_file_name(checkpoint_file_number);
  std::string manifest_name = FileNameUtil::manifest_file_name(manifest_file_number);

  if (0 >= (snprintf(buf,
                     MAX_FILE_PATH_SIZE,
                     "%s:%s:%ld\n",
                     checkpoint_name.c_str(),
                     manifest_name.c_str(),
                     log_number_.load()))) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "fail to write to buf", K(ret));
  } else if (FAILED(WriteStringToFile(env_, buf, tmp_file, true).code())) {
    SE_LOG(WARN, "fail to write string to file", K(ret));
  } else if (FAILED(env_->RenameFile(tmp_file, db_name_ + "/CURRENT").code())) {
    SE_LOG(WARN, "fail to write current file", K(ret));
  } else {
    SE_LOG(INFO, "success to write current file", K(checkpoint_name), K(manifest_name), K_(log_number));
  }
  return ret;
}

int StorageLogger::parse_current_file(std::string &checkpoint_name, std::string &manifest_name, uint64_t &log_number)
{
  int ret = Status::kOk;
  std::string file_content;

  checkpoint_name.clear();
  manifest_name.clear();

  if (FAILED(ReadFileToString(env_, FileNameUtil::current_file_path(db_name_), &file_content).code())) {
    SE_LOG(WARN, "fail to read manifest information", K(ret));
  } else if ('\n' != file_content.back()) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "manifest name's last charactor not \\n", K(ret));
  } else {
    file_content.pop_back();

    std::vector<std::string> fields = StringSplit(file_content, ':');

    checkpoint_name = fields[0];
    manifest_name = fields[1];

    Slice manifest_file_name_str(manifest_name);
    Slice log_number_str(fields[2]);
    uint64_t log_file_number = 0;
    if (!ConsumeDecimalNumber(&log_number_str, &log_number)) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "fail to parse log number", K(ret));
    } else if (!ConsumeDecimalNumber(&manifest_file_name_str, &log_file_number)) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "fail to parse log file number", K(ret), K(manifest_file_name_str));
    } else {
      log_file_number_ = log_file_number;
      log_number_.store(log_number);
      SE_LOG(INFO, "parse current file", K(manifest_name), K(checkpoint_name), K(log_number),
          K_(log_file_number), K(log_number_.load()));
    }
  }
  return ret;
}

int StorageLogger::manifest_file_in_current(std::string &manifest_name)
{
  int ret = Status::kOk;
  manifest_name.clear();

  if (FAILED(ReadFileToString(env_, FileNameUtil::current_file_path(db_name_), &manifest_name).code())) {
      SE_LOG(WARN, "fail to read manifest information", K(ret));
  }

   return ret;
}

int StorageLogger::create_log_writer(int64_t manifest_file_number, db::log::Writer *&writer)
{
  int ret = Status::kOk;
  WritableFile *manifest_file = nullptr;
  util::ConcurrentDirectFileWriter *file_writer;
  char *buf = nullptr;
  std::string manifest_log_path = FileNameUtil::manifest_file_path(db_name_, manifest_file_number);
  EnvOptions opt_env_opts = env_->OptimizeForManifestWrite(env_options_);

  if (FAILED(NewWritableFile(env_, manifest_log_path, manifest_file, opt_env_opts).code())) {
    SE_LOG(WARN, "fail ro create writable file", K(ret));
  } else {
    if (IS_NULL(file_writer = MOD_NEW_OBJECT(memory::ModId::kStorageLogger,
        util::ConcurrentDirectFileWriter, manifest_file, opt_env_opts))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for ConcurrentDirectFileWriter", K(ret));
    } else {
      if (FAILED(file_writer->init_multi_buffer().code())) {
        SE_LOG(WARN, "fail to init multi buffer", K(ret));
      } else if (nullptr == (writer = MOD_NEW_OBJECT(ModId::kStorageLogger,
                                                     db::log::Writer,
                                                     file_writer,
                                                     0,
                                                     false))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to constructor writer", K(ret));
      }
    }
  }

  return ret;
}

int StorageLogger::create_log_reader(const std::string &manifest_name,
                                     db::log::Reader *&reader,
                                     memory::ArenaAllocator &arena,
                                     int64_t start)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  util::SequentialFileReader *file_reader = nullptr;
  SequentialFile *manifest_file = nullptr;
  SequentialFileReader *manifest_file_reader = nullptr;

  reader = nullptr;
  EnvOptions tmp_option = env_options_;
  tmp_option.arena = &arena;
  if (FAILED(env_->NewSequentialFile(manifest_name, manifest_file, tmp_option).code())) {
    SE_LOG(WARN, "fail to create manifest file", K(ret), K(manifest_name));
  //why use new ? because this memory contol by unique_ptr,and unique_ptr use default delete to destruct and free memory, so use new to match delete
  //why not define a deletor to unique_ptr? because this unique ptr will move to another object Reader(Reader's param use unique_ptr with default delete), and Reader used many other code
//  } else if (nullptr == (manifest_file_reader = new SequentialFileReader(manifest_file))) {
  } else if (nullptr == (manifest_file_reader = ALLOC_OBJECT(SequentialFileReader, arena, manifest_file, true))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to construct SequentialFileReader", K(ret));
  } else {
//    manifest_file_reader.reset(file_reader);
    if (nullptr == (reader = ALLOC_OBJECT(db::log::Reader,
                                          arena,
                                          manifest_file_reader,
                                          nullptr,
                                          true,
                                          start /* initial_offset */,
                                          0 /* log_number */,
                                          true /* use allocator */))) {
      SE_LOG(WARN, "fail to constructor reader", K(ret), K(manifest_name));
    } else {
      SE_LOG(INFO, "success to create log reader", K(ret), K(manifest_name));
    }
  }
  return ret;
}

void StorageLogger::destroy_log_reader(db::log::Reader *&reader, memory::SimpleAllocator *allocator)
{
  if (nullptr != allocator) {
    reader->delete_file(allocator);
    FREE_OBJECT(Reader, *allocator, reader);
  } else {
    MOD_DELETE_OBJECT(Reader, reader);
  }
}

void StorageLogger::destroy_log_writer(db::log::Writer *&writer, memory::SimpleAllocator *allocator)
{
  if (nullptr != writer) {
    writer->sync();
    if (nullptr != allocator) {
      FREE_OBJECT(Writer, *allocator, writer);
    } else {
      MOD_DELETE_OBJECT(Writer, writer);
    }
  }
}

int StorageLogger::get_commited_trans(std::unordered_set<int64_t> &commited_trans_ids)
{
  int ret = Status::kOk;
  commited_trans_ids.clear();
  std::string manifest_name;
  int64_t log_file_number = 0;
  std::string start_checkpoint_name;
  std::string checkpoint_name;
  uint64_t log_number;

  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(parse_current_file(start_checkpoint_name, manifest_name, log_number))) {
    SE_LOG(WARN, "fail to parse current file", K(ret));
  } else {
    log_file_number = log_file_number_;
    //change manifest_name from relative path to absolute path. a little ugly QAQ
    manifest_name = FileNameUtil::manifest_file_path(db_name_, log_file_number);
    checkpoint_name = FileNameUtil::checkpoint_file_path(db_name_, log_file_number);
    //only when file exists, do actual thing, so we not set ret
    while (Status::kOk == ret) {
      if (Status::kOk == (env_->FileExists(manifest_name).code())) {
        // need process this manifest file
      } else if (Status::kOk == (env_->FileExists(checkpoint_name).code())) {
        SE_LOG(INFO, "skip checkpoint file and process next manifest file", K(checkpoint_name));
        log_file_number++;
        manifest_name = FileNameUtil::manifest_file_path(db_name_, log_file_number);
        checkpoint_name = FileNameUtil::checkpoint_file_path(db_name_, log_file_number);
        continue;
      } else {
        SE_LOG(INFO,
               "finish to get commited trans, the next manifest log file or next checkpoint file is",
               K(manifest_name),
               K(checkpoint_name));
        break;
      }

      if (FAILED(get_commited_trans_from_file(manifest_name, commited_trans_ids))) {
        SE_LOG(WARN, "failed to get commited trans from file", K(ret), K(manifest_name));
      } else {
        ++log_file_number;
        manifest_name = FileNameUtil::manifest_file_path(db_name_, log_file_number);
        checkpoint_name = FileNameUtil::checkpoint_file_path(db_name_, log_file_number);
      }
    }
  }

  return ret;
}

int StorageLogger::record_incremental_extent_ids(const std::string &backup_tmp_dir_path,
                                                 const int64_t first_manifest_file_num,
                                                 const int64_t last_manifest_file_num,
                                                 const uint64_t last_manifest_file_size)
{
  int ret = Status::kOk;
  std::vector<int64_t> manifest_file_nums;
  std::unordered_set<int64_t> commited_trans_ids;

  SE_LOG(INFO, "Begin to read incremental manifest files", K(ret));
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(check_manifest_for_backup(backup_tmp_dir_path,
                                              first_manifest_file_num,
                                              last_manifest_file_num,
                                              manifest_file_nums))) {
    SE_LOG(WARN, "Failed to check manifest files for backup", K(ret));
  } else if (FAILED(get_commited_trans_for_backup(backup_tmp_dir_path, manifest_file_nums, commited_trans_ids))) {
    SE_LOG(WARN, "Failed to get commited transactions for backup", K(ret));
  } else if (FAILED(read_manifest_for_backup(backup_tmp_dir_path,
                                             backup_tmp_dir_path + util::BACKUP_EXTENT_IDS_FILE,
                                             manifest_file_nums,
                                             commited_trans_ids,
                                             last_manifest_file_size))) {
    SE_LOG(WARN, "Failed to read manifest for backup", K(ret));
  } else {
    SE_LOG(INFO, "Success to read incremental manifest files", K(ret));
  }
  return ret;
}

int StorageLogger::check_manifest_for_backup(const std::string &backup_tmp_dir_path,
                                             const int64_t first_manifest_file_num,
                                             const int64_t last_manifest_file_num,
                                             std::vector<int64_t> &manifest_file_nums)
{
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else {
    // check all manifest files exist and build hard links
    manifest_file_nums.clear();
    int64_t log_file_num = 0;
    for (log_file_num = first_manifest_file_num; SUCCED(ret) && log_file_num <= last_manifest_file_num; log_file_num++) {
      std::string manifest_path = FileNameUtil::manifest_file_path(backup_tmp_dir_path, log_file_num);
      std::string checkpoint_path = FileNameUtil::checkpoint_file_path(backup_tmp_dir_path, log_file_num);
      if (FAILED(env_->FileExists(manifest_path).code())) {
        // overwrite ret
        if (FAILED(env_->FileExists(checkpoint_path).code())) {
          SE_LOG(ERROR, "manifest file was deleted during backup", K(ret), K(manifest_path), K(checkpoint_path));
        } else {
          SE_LOG(INFO, "There was a checkpoint during backup", K(ret), K(checkpoint_path));
        }
      } else {
        manifest_file_nums.push_back(log_file_num);
        SE_LOG(INFO, "Found a manifest file", K(ret), K(manifest_path));
      }
    }
  }
  return ret;
}

int StorageLogger::get_commited_trans_for_backup(const std::string &backup_tmp_dir_path,
                                                 const std::vector<int64_t> &manifest_file_nums,
                                                 std::unordered_set<int64_t> &commited_trans_ids)
{
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else {
    std::string manifest_name;
    commited_trans_ids.clear();
    for (size_t i = 0; SUCCED(ret) && i < manifest_file_nums.size(); i++) {
      manifest_name = FileNameUtil::manifest_file_path(backup_tmp_dir_path, manifest_file_nums[i]);
      if (FAILED(env_->FileExists(manifest_name).code())) {
        SE_LOG(ERROR, "manifest file not exist", K(ret), K(manifest_name));
      } else if (FAILED(get_commited_trans_from_file(manifest_name, commited_trans_ids))) {
        SE_LOG(WARN, "failed to get commited trans from file", K(ret), K(manifest_name));
      }
    }
  }
  return ret;
}

int StorageLogger::read_manifest_for_backup(const std::string &backup_tmp_dir_path,
                                            const std::string &extent_ids_path,
                                            const std::vector<int64_t> &manifest_file_nums,
                                            const std::unordered_set<int64_t> &commited_trans_ids,
                                            const uint64_t last_manifest_file_size)
{
  // read all manifest files and record all incremental extents' id
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else {
    log::Reader *log_reader = nullptr;
    Slice record;
    std::string scrath;
    StorageLoggerBuffer log_buffer;
    LogHeader log_header;
    ManifestLogEntryHeader log_entry_header;
    char *log_data = nullptr;
    int64_t pos = 0;
    char *log_buf = nullptr;
    int64_t log_len = 0;
    //std::unordered_set<int64_t> extent_ids;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> extent_ids_map;

    // parse manifest files and record all modified extent ids
    memory::ArenaAllocator arena(8 * 1024, memory::ModId::kStorageLogger);
    for (size_t i = 0; SUCCED(ret) && i < manifest_file_nums.size(); i++) {
      // parse one manifest file
      std::string manifest_name = FileNameUtil::manifest_file_path(backup_tmp_dir_path, manifest_file_nums[i]);
      if (FAILED(env_->FileExists(manifest_name).code())) {
        SE_LOG(ERROR, "manifest file not exist", K(ret), K(manifest_name));
      } else if (FAILED(create_log_reader(manifest_name, log_reader, arena))) {
        SE_LOG(WARN, "fail to create log reader", K(ret));
      } else {
        while (SUCCED(ret) && log_reader->ReadRecord(&record, &scrath)) {
          // parse one record in manifest file
          pos = 0;
          log_buf = const_cast<char *>(record.data());
          if (i == manifest_file_nums.size() - 1 && log_reader->LastRecordOffset() >= last_manifest_file_size) {
            // skip
            break;
          } else if (FAILED(log_header.deserialize(log_buf, record.size(), pos))) {
            SE_LOG(WARN, "fail to deserialize log header", K(ret));
          } else {
            log_buffer.assign(log_buf + pos, log_header.log_length_);
            while(SUCCED(ret) && SUCCED(log_buffer.read_log(log_entry_header, log_data, log_len))) {
              // parse one log entry in record
              if (commited_trans_ids.end() == commited_trans_ids.find(log_entry_header.trans_id_)) {
                SE_LOG(DEBUG, "this log not commited, ignore it", K(ret), K(log_entry_header));
              } else if (is_partition_log(log_entry_header.log_entry_type_)) {
                if (nullptr == log_data || log_len < 0) {
                  ret = Status::kInvalidArgument;
                  SE_LOG(WARN, "invalid argument", K(ret), K(log_len));
                } else if (REDO_LOG_MODIFY_SSTABLE == log_entry_header.log_entry_type_) {
                  ChangeInfo change_info;
                  ModifySubTableLogEntry log_entry(change_info);
                  pos = 0;
                  if (FAILED(log_entry.deserialize(log_data, log_len, pos))) {
                    SE_LOG(WARN, "fail to deserialize log entry", K(ret), K(log_len), K(manifest_name));
                  } else if (FAILED(process_change_info_for_backup(log_entry.change_info_,
                                                                   extent_ids_map[log_entry.index_id_]))) {
                    SE_LOG(WARN, "Failed to process change info for backup", K(ret), K(manifest_name));
                  }
                } else if (REDO_LOG_REMOVE_SSTABLE == log_entry_header.log_entry_type_) {
                  // remove all extent ids from extent_ids_map if a table dropped
                  ChangeSubTableLogEntry log_entry;
                  pos = 0;
                  if (FAILED(log_entry.deserialize(log_data, log_len, pos))) {
                    SE_LOG(WARN, "fail to deserialize log entry", K(ret), K(log_len), K(manifest_name));
                  } else {
                    extent_ids_map.erase(log_entry.table_schema_.get_index_id());
                    SE_LOG(INFO, "Found a dropped subtable", K(log_entry.table_schema_));
                  }
                } else {
                  // skip
                }
              } else {
                // skip
              }
            }
            if (Status::kIterEnd != ret) {
              SE_LOG(WARN, "fail to read log", K(ret), K(manifest_name));
            } else {
              ret = Status::kOk;
            }
          }
        }
      }
      destroy_log_reader(log_reader, &arena);
    }
    // write all extent ids to the file
    if (SUCCED(ret)) {
      WritableFile *extent_ids_writer = nullptr;
      EnvOptions opt_env_opts = env_options_;
      opt_env_opts.use_direct_writes = false;
      if (FAILED(NewWritableFile(env_, extent_ids_path, extent_ids_writer, opt_env_opts).code())) {
        SE_LOG(WARN, "Failed to create extent ids writer", K(ret), K(extent_ids_path));
      } else {
        int64_t extent_ids_size = 0;
        for (auto iter = extent_ids_map.begin(); SUCCED(ret) && iter != extent_ids_map.end(); iter++) {
          for (int64_t extent_id : iter->second) {
            SE_LOG(INFO, "backup write incremental extent", K(ExtentId(extent_id)));
            if (FAILED(extent_ids_writer->Append(Slice(reinterpret_cast<char *>(&extent_id), sizeof(extent_id))).code())) {
              SE_LOG(WARN, "Failed to write extent id", K(ret), K(extent_id));
              break;
            }
            ++extent_ids_size;
          }
        }
        if (FAILED(ret)) {
        } else if (FAILED(extent_ids_writer->Sync().code())) {
          SE_LOG(WARN, "Failed to sync extent ids writer", K(ret));
        } else if (FAILED(extent_ids_writer->Close().code())) {
          SE_LOG(WARN, "Failed to close extent ids writer", K(ret));
        } else {
          SE_LOG(INFO, "Success to write extent_ids file", K(ret), K(extent_ids_size));
        }
      }
      MOD_DELETE_OBJECT(WritableFile, extent_ids_writer);
    }
  }
  return ret;
}

int StorageLogger::process_change_info_for_backup(ChangeInfo &change_info,
                                                  std::unordered_set<int64_t> &extent_ids)
{
  int ret = Status::kOk;
  // process change info, only records the added extent ids
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; level++) {
    // we should iterate the change_info.extent_change_info_ from level 0 to
    // level 2 in order, since there might be some reused extents
    const ExtentChangeArray &extent_changes = change_info.extent_change_info_[level];
    for (size_t i = 0; SUCCED(ret) && i < extent_changes.size(); ++i) {
      const ExtentChange ec = extent_changes.at(i);
      if (ec.is_add()) {
        extent_ids.insert(ec.extent_id_.id());
        SE_LOG(INFO, "backup insert incremental extent", K(ec));
      } else if (ec.is_delete()) {
        extent_ids.erase(ec.extent_id_.id());
        SE_LOG(INFO, "backup delete incremental extent", K(ec));
      } else {
        ret = Status::kNotSupported;
        SE_LOG(WARN, "unsupport extent change type", K(ret), K(ec));
      }
    }
  }

  // process large object change info
  for (size_t i = 0; SUCCED(ret) && i < change_info.lob_extent_change_info_.size(); ++i) {
    const ExtentChange ec = change_info.lob_extent_change_info_.at(i);
    if (ec.is_add()) {
      extent_ids.insert(ec.extent_id_.id());
      SE_LOG(INFO, "backup insert lob incremental extent", K(ec));
    } else if (ec.is_delete()) {
      extent_ids.erase(ec.extent_id_.id());
      SE_LOG(INFO, "backup delete lob incremental extent", K(ec));
    } else {
      ret = Status::kNotSupported;
      SE_LOG(WARN, "unsupport extent change type", K(ret), K(ec));
    }
  }
  return ret;
}

int StorageLogger::get_commited_trans_from_file(const std::string &manifest_name,
                                                std::unordered_set<int64_t> &commited_trans_ids)
{
  int ret = Status::kOk;
  log::Reader *log_reader = nullptr;
  Slice record;
  char *record_buf = nullptr;
  std::string scrath;
  StorageLoggerBuffer log_buffer;
  LogHeader log_header;
  ManifestLogEntryHeader log_entry_header;
  char *log_data = nullptr;
  int64_t log_length  = 0;
  std::unordered_set<int64_t> begin_trans_ids;
  int64_t pos = 0;
  memory::ArenaAllocator arena(8 * 1024, memory::ModId::kStorageLogger);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(create_log_reader(manifest_name, log_reader, arena))) {
    SE_LOG(WARN, "fail to create log reader", K(ret), K(manifest_name));
  } else {
    while (SUCCED(ret) && log_reader->ReadRecord(&record, &scrath)) {
      pos = 0;
      record_buf = const_cast<char *>(record.data());
      if (FAILED(log_header.deserialize(record_buf, record.size(), pos))) {
        SE_LOG(WARN, "fail to deserialize log header", K(ret), K(pos));
      } else {
        log_buffer.assign(record_buf + pos, log_header.log_length_);
        while (SUCCED(ret) && SUCCED(log_buffer.read_log(log_entry_header, log_data, log_length))) {
          SE_LOG(DEBUG, "read one manifest log", K(log_entry_header), K(pos));
          if (REDO_LOG_BEGIN == log_entry_header.log_entry_type_) {
            if (!(begin_trans_ids.insert(log_entry_header.trans_id_).second)) {
              ret = Status::kErrorUnexpected;
              SE_LOG(WARN, "fail to insert trans id", K(ret), K(log_entry_header));
            } else {
              if (log_entry_header.trans_id_ > global_trans_id_) {
                global_trans_id_ = log_entry_header.trans_id_;
                SE_LOG(INFO, "success to insert trans id to begin_trans_ids", K(log_entry_header));
              }
            }
          } else if (REDO_LOG_COMMIT == log_entry_header.log_entry_type_) {
            if (!(commited_trans_ids.insert(log_entry_header.trans_id_).second)) {
              ret = Status::kErrorUnexpected;
              SE_LOG(WARN, "fail to insert trans id to commited_trans_ids", K(log_entry_header));
            }
          }
        }
        if (UNLIKELY(Status::kIterEnd != ret)) {
          SE_LOG(WARN, "fail to read log", K(ret), K(manifest_name));
        } else {
          ret = Status::kOk;
        }
      }
    }
  }
  destroy_log_reader(log_reader, &arena);
  return ret;
}

void StorageLogger::wait_trans_barrier()
{
  while (active_trans_cnt_.load() > 0) {
    PAUSE();
  }
}

} // namespace storage
} // namespace smartengine
