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

#ifndef SMARTENGINE_INCLUDE_STORAGE_LOGGER_H_
#define SMARTENGINE_INCLUDE_STORAGE_LOGGER_H_
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "memory/page_arena.h"
#include "options/db_options.h"
#include "storage/storage_log_entry.h"
#include "storage/extent_space_manager.h"
#include "util/lock_free_fixed_queue.h"

namespace smartengine
{
namespace db
{
class VersionSet;
}

namespace storage
{
struct StorageLoggerBuffer
{
public:
  StorageLoggerBuffer();
  ~StorageLoggerBuffer();
  //inline int assign(char *buf, int64_t buf_len);
  int assign(char *buf, int64_t buf_len);
  inline void reuse() { pos_ = 0; }
  inline void reset()
  {
    data_ = nullptr;
    pos_ = 0;
    capacity_ = 0;
  }
  inline char *data() { return data_; }
  inline int64_t length() { return pos_; }
  inline char *current() { return data_ + pos_; }
  inline int64_t remain() { return capacity_ - pos_; }
  int append_log(ManifestLogEntryHeader &log_entry_header, const char *log_data, const int64_t log_len);
  int append_log(ManifestLogEntryHeader &log_entry_header, ManifestLogEntry &log_entry);
  int read_log(ManifestLogEntryHeader &log_entry_header, char *&log_data, int64_t &log_len);

public:
  char *data_;
  int64_t pos_;
  int64_t capacity_;
};

struct CheckpointBlockHeader
{
  static const int64_t BLOCK_HEADER_VERSION = 1;
  int64_t type_{0}; // 0: extent meta block; 1: sub table meta
  int64_t block_size_{0};
  int64_t entry_count_{0};
  int64_t data_offset_{0};
  int64_t data_size_{0};
  int64_t reserve_[2]{0, 0};

  CheckpointBlockHeader() {}
  ~CheckpointBlockHeader() {}
  void reset();
  DECLARE_TO_STRING()
};

constexpr int64_t CHECKPOINT_HEADER_SIZE = 2 * 1024 * 1024;

struct CheckpointHeader
{
  static const int64_t CHECKPOINT_HEADER_VERSION = 1;
  int64_t start_log_id_{0};
  int64_t extent_count_{0};
  int64_t sub_table_count_{0};
  int64_t extent_meta_block_count_{0};
  int64_t sub_table_meta_block_count_{0};
  int64_t extent_meta_block_offset_{0};
  int64_t sub_table_meta_block_offset_{0};
  int64_t reserve_[2]{0, 0};

  CheckpointHeader() {}
  ~CheckpointHeader() {}
  //DECLARE_COMPACTIPLE_SERIALIZATION(CHECKPOINT_HEADER_VERSION);
  DECLARE_TO_STRING()
};

static_assert(sizeof(CheckpointHeader) <= CHECKPOINT_HEADER_SIZE);

class StorageLogger
{
private:
  struct TransContext
  {
    enum SeEvent event_;
    StorageLoggerBuffer log_buffer_;
    int64_t log_count_;
    bool need_reused_;

    TransContext(bool need_reused);
    ~TransContext();
    inline void reuse();
    inline bool need_reused() { return need_reused_; }
    int append_log(int64_t trans_id, ManifestRedoLogType log_type, const char *log_data, const int64_t log_len);
    int append_log(int64_t trans_id, ManifestRedoLogType log_type, ManifestLogEntry &log_entry);
  };
public:
  static StorageLogger &get_instance();
  int init(util::Env *env,
           std::string db_name,
           util::EnvOptions env_options,
           common::ImmutableDBOptions db_options,
           db::VersionSet *version_set,
           int64_t max_manifest_log_file_size);
  void destroy();

  int begin(enum SeEvent event);
  int commit(int64_t &log_number);
  int abort();
  int write_log(const ManifestRedoLogType log_type, ManifestLogEntry &log_entry);
  //trigger write checkpoint by external condition
  //thread safe
  int external_write_checkpoint();
  int replay(memory::ArenaAllocator &arena);
  // for hotbackup
  int manifest_file_in_current(std::string &manifest_file);
  int manifest_file_range(int32_t &begin, int32_t &end, int64_t &end_pos);
  int64_t current_manifest_file_number() const { return current_manifest_file_number_; }
  uint64_t current_manifest_file_size() const { return log_writer_->file()->get_file_size(); }
  int record_incremental_extent_ids(const std::string &backup_tmp_dir_path,
                                    const int64_t first_manifest_file_num,
                                    const int64_t last_manifest_file_num,
                                    const uint64_t last_manifest_file_size);
#ifndef NDEBUG
  void TEST_reset();
  int set_log_writer(int64_t file_number);
#endif

private:
  int alloc_trans_ctx(TransContext *&trans_ctx);
  int free_trans_ctx(TransContext *trans_ctx);
  int construct_trans_ctx(bool is_reused, TransContext *&trans_ctx);
  int deconstruct_trans_context(TransContext *trans_ctx);
  int flush_log(TransContext &trans_ctx, int64_t *log_seq_num = nullptr);
  int flush_large_log_entry(TransContext &trans_ctx, ManifestRedoLogType log_type, ManifestLogEntry &log_entry);
  int load_latest_backup_snapshot();
  int load_checkpoint(memory::ArenaAllocator &arena);
  int replay_after_ckpt(memory::ArenaAllocator &arena);
  //trigger write checkpoint by internal condition
  //not thread safe, should protected by log_sync_mutex_
  int internal_write_checkpoint();
  //not thread safe, should protected by log_sync_mutex_
  int update_log_writer(int64_t manifest_file_number);
  int write_current_file(int64_t checkpoint_file_number, int64_t manifest_file_number);
  int parse_current_file(std::string &checkpoint_name, std::string &manifest_name, uint64_t &log_number);
  //not thread safe, should protected by log_sync_mutex_
  int create_log_writer(int64_t manifest_file_number, db::log::Writer *&writer);
  int create_log_reader(const std::string &manifest_name,
                        db::log::Reader *&reader,
                        memory::ArenaAllocator &arena,
                        int64_t start = 0);
  void destroy_log_reader(db::log::Reader *&reader, memory::SimpleAllocator *allocator = nullptr);
  //no thread safe, should protected by log_sync_mutex_
  void destroy_log_writer(db::log::Writer *&writer, memory::SimpleAllocator *allocator = nullptr);

  int get_commited_trans(std::unordered_set<int64_t> &commited_trans_ids);
  int get_commited_trans_from_file(const std::string &manifest_name,
                                   std::unordered_set<int64_t> &commited_trans_ids);
  // for hotbackup
  int check_manifest_for_backup(const std::string &backup_tmp_dir_path,
                                const int64_t first_manifest_file_num,
                                const int64_t last_manifest_file_num,
                                std::vector<int64_t> &manifest_file_nums);
  int get_commited_trans_for_backup(const std::string &backup_tmp_dir_path,
                                    const std::vector<int64_t> &manifest_file_nums,
                                    std::unordered_set<int64_t> &commited_trans_ids);
  int read_manifest_for_backup(const std::string &backup_tmp_dir_path,
                               const std::string &extent_ids_path,
                               const std::vector<int64_t> &manifest_file_nums,
                               const std::unordered_set<int64_t> &commited_trans_ids,
                               const uint64_t last_manifest_file_size);
  int process_change_info_for_backup(ChangeInfo &change_info,
                                     std::unordered_set<int64_t> &extent_ids);
  inline void inc_active_trans_cnt()
  {
    active_trans_cnt_.fetch_add(1);
  }
  inline void dec_active_trans_cnt()
  {
    active_trans_cnt_.fetch_sub(1);
  }
  inline int64_t get_active_trans_cnt()
  {
    return active_trans_cnt_.load();
  }
  void wait_trans_barrier();

private:
  StorageLogger();
  ~StorageLogger();

public:
  static const int64_t DEFAULT_TRANS_CONTEXT_COUNT = 4;
  static const int64_t MAX_LOG_SIZE = 2 * 1024 * 1024; //2MB
  static const int64_t DEFAULT_RESERVE_LOG_HEADER = 512; //512Byte
  static const int64_t DEFAULT_LOGGER_BUFFER_SIZE = MAX_LOG_SIZE - DEFAULT_RESERVE_LOG_HEADER;
  static const int64_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; //2MB
  static const int64_t FORCE_WRITE_CHECKPOINT_MULTIPLE = 3;

private:
  bool is_inited_;
  util::Env *env_;
  std::string db_name_;
  util::EnvOptions env_options_;
  common::ImmutableDBOptions db_options_;
  int64_t log_file_number_; //checkpoint file, manifest file
  db::VersionSet *version_set_;
  std::atomic<int64_t> curr_manifest_log_size_;
  db::log::Writer *log_writer_;
  int64_t max_manifest_log_file_size_;
  std::atomic<int64_t> log_number_;
  std::atomic<int64_t> global_trans_id_;
  static __thread int64_t local_trans_id_;
  //TODO:@yuanfeng, use thread safe map to replace map_mutex_ and trans_ctx_map_
  std::mutex map_mutex_;
  std::unordered_map<int64_t, TransContext *> trans_ctx_map_;
  std::mutex log_sync_mutex_;
  char *log_buf_;
  util::FixedQueue<TransContext> trans_ctxs_;
  memory::ArenaAllocator allocator_;
  int64_t current_manifest_file_number_; // current manifest file
  std::mutex trans_pool_mutex_;
  std::atomic<int64_t> active_trans_cnt_;
};

} // namespace storage
} // namespace smartengine
#endif
