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

#include <queue>
#include "storage/storage_common.h"
#include "util/aio_wrapper.h"

namespace smartengine
{
namespace table
{
class ExtentWriter;
} // namespace table

namespace storage
{
class IOExtent
{
public:
  IOExtent();
  virtual ~IOExtent();

  virtual void reset();
  virtual int write(const common::Slice &data, int64_t offset) = 0;
  virtual int read(util::AIOHandle *aio_handle,
                   int64_t offset,
                   int64_t size,
                   char *buf,
                   common::Slice &result) = 0;
  virtual int prefetch(util::AIOHandle *aio_handle, int64_t offset, int64_t size) = 0;
  virtual ExtentId get_extent_id() const { return extent_id_; }
  virtual int64_t get_unique_id(char *id, const int64_t max_size) const;
  inline void set_large_object_extent() { is_large_object_extent_ = true; }
  inline bool is_large_object_extent() const { return is_large_object_extent_; }

  DEFINE_PURE_VIRTUAL_CONSTRUCTOR_SIZE()
protected:
  bool is_inited_;
  ExtentId extent_id_;
  int64_t unique_id_; // for unique identify in cache
  bool is_large_object_extent_; // is large object extent
};

class FileIOExtent : public IOExtent
{
public:
  FileIOExtent();
  ~FileIOExtent() override;

  int init(const ExtentId &extent_id, int64_t unique_id, int fd);
  void reset() override; 
  int write(const common::Slice &data, int64_t offset) override;
  int read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, common::Slice &result) override;
  int prefetch(util::AIOHandle *aio_handle, int64_t offset, int64_t size) override; 

  DEFINE_OVERRIDE_CONSTRUCTOR_SIZE()
private:
  inline int64_t get_base_offset() const { return (extent_id_.offset * storage::MAX_EXTENT_SIZE); }
  // convert the offset int the extent to the offset in the data file.
  int fill_aio_info(int64_t offset, int64_t size, util::AIOInfo &aio_info) const;
  bool is_aligned(int64_t offset, int64_t size, const char *buf) const;
  int align_to_direct_write(int fd, int64_t offset, const char *buf, int64_t size);
  int direct_write(int fd, int64_t offset, const char *buf, int64_t size);
  int sync_read(int64_t offset, int64_t size, char *buf, common::Slice &result);
  int async_read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, common::Slice &result);
  int align_to_direct_read(int fd, int64_t offset, int64_t size, char *buf);
  int direct_read(int fd, int64_t offset, int64_t size, char *buf);

protected:
  int fd_;
};

class ObjectIOExtent : public IOExtent
{
public:
  ObjectIOExtent();
  virtual ~ObjectIOExtent() override;

  int init(const ExtentId &extent_id,
           int64_t unique_id,
           ::objstore::ObjectStore *object_store,
           const std::string &bucket,
           const std::string &prefix);
  void reset() override;
  int write(const common::Slice &data, int64_t offset) override;
  int read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, common::Slice &result) override;
  int prefetch(util::AIOHandle *aio_handle, int64_t offset, int64_t size) override; 

  DEFINE_OVERRIDE_CONSTRUCTOR_SIZE()
private:
  int fill_aio_info(util::AIOHandle *aio_handle, int64_t offset, int64_t size, util::AIOInfo &aio_info);
  int write_object(const char *data, int64_t data_size);
  int sync_read(int64_t offset, int64_t size, char *buf, common::Slice &result);
  int async_read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, common::Slice &result);
  int read_object(int64_t offset, int64_t size, char *buf, common::Slice &result);
  int load_extent(cache::Cache::Handle **handle);

protected:
  ::objstore::ObjectStore *object_store_;
  std::string bucket_;
  std::string prefix_;
};

class WriteExtentJob
{
public:
  WriteExtentJob();
  ~WriteExtentJob();

  int init(table::ExtentWriter *writer, IOExtent *extent, const common::Slice &data);
  void destroy();
  int execute();

private:
  bool is_inited_;
  table::ExtentWriter *writer_;
  IOExtent *extent_;
  char *data_;
  int64_t data_size_;
};

class WriteExtentJobScheduler
{
public:
  static WriteExtentJobScheduler &get_instance();

  int start(util::Env *env, int64_t write_io_thread_count);
  int stop();
  int submit(WriteExtentJob *job);
  int adjust_write_thread_count(int64_t thread_count);

private:
  static void consume_wrapper(void *scheduler);
  int consume();
  void push_to_job_queue(WriteExtentJob *job);
  WriteExtentJob *pop_from_job_queue();

private:
  WriteExtentJobScheduler();
  ~WriteExtentJobScheduler();
  
  static const int64_t MAX_JOB_QUEUE_SIZE_FACTOR = 10;

private:
  bool is_inited_;
  util::Env *env_;
  std::atomic<int64_t> write_io_thread_count_;
  std::mutex job_queue_mutex_;
  std::queue<WriteExtentJob *> job_queue_;
};

}  // namespace storage
}  // namespace smartengine
