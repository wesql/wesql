/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cache/persistent_cache.h"
#include "cache/lru_cache.h"
#include "unittest/db/db_test_util.h"
#include "util/testharness.h"
#include "util/stack_trace.h"

static const std::string test_dir = "/hotbackup_test";
namespace smartengine
{
using namespace common;
using namespace db;
using namespace memory;
using namespace table;
using namespace util;

namespace cache
{
class PersistentCacheTest : public testing::Test
{
public:
  PersistentCacheTest()
  {
    file_dir_ = test::TmpDir(Env::Default()) + test_dir;
    file_path_ = file_dir_ + "/" + PersistentCacheFile::get_file_name();
  }

  int remove_cache_file()
  {
    return Env::Default()->DeleteFile(file_path_).code();
  }

  void generate_extent_data(ExtentId data_extent_id, std::string &extent_data)
  {
    int ret = Status::kOk;
    Footer footer;
    std::string data; 
    int64_t pos = storage::MAX_EXTENT_SIZE - Footer::get_max_serialize_size();
    char *buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE));

    footer.set_extent_id(data_extent_id);
    data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE / 2); 
    memcpy(buf, data.data(), data.size());
    ret = footer.serialize(buf, storage::MAX_EXTENT_SIZE, pos);
    ASSERT_EQ(Status::kOk, ret);
    extent_data.assign(buf, storage::MAX_EXTENT_SIZE);
  }

public:
  static const int32_t PERSISTENT_FILE_NUMBER = (INT32_MAX - 1);
  std::string file_dir_;
  std::string file_path_;
};
 
TEST_F(PersistentCacheTest, persistent_cache_file_open)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;
  int64_t need_recover_extent_count = 0;

  // invalid argument
  ret = cache_file.open(nullptr, file_path_, 0, need_recover_extent_count);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.open(Env::Default(), std::string(), 0, need_recover_extent_count);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE - 1, need_recover_extent_count);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE + 1, need_recover_extent_count);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success open
  ret = cache_file.open(Env::Default(), file_path_, 2 * storage::MAX_EXTENT_SIZE, need_recover_extent_count);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(0, need_recover_extent_count);

  // repeat open
  ret = cache_file.open(Env::Default(), file_path_, 2 * storage::MAX_EXTENT_SIZE, need_recover_extent_count);
  ASSERT_EQ(Status::kInitTwice, ret);

  // close and reopen with same cache size(2 * storage::MAX_EXTENT_SIZE)
  cache_file.close();
  ret = cache_file.open(Env::Default(), file_path_, 2 * storage::MAX_EXTENT_SIZE, need_recover_extent_count);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(2, need_recover_extent_count);

  // close and reopen with larger cache size(4 * storage::MAX_EXTENT_SIZE)
  cache_file.close();
  ret = cache_file.open(Env::Default(), file_path_, 4 * storage::MAX_EXTENT_SIZE, need_recover_extent_count);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(2, need_recover_extent_count);

  // close and reopen with smaller cache size(storage::MAX_EXTENT_SIZE)
  cache_file.close();
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE, need_recover_extent_count);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(1, need_recover_extent_count);

  // close and remove the cache file
  cache_file.close();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_file_alloc)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;
  PersistentCacheInfo cache_info;
  ExtentId data_extent_id;
  int64_t need_recover_extent_count = 0;

  // not open
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kNotInit, ret);

  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 3, need_recover_extent_count);
  ASSERT_EQ(Status::kOk, ret);

  for (int64_t i = 0; i < 3; ++i) {
    data_extent_id.offset = i;
    ret = cache_file.alloc(data_extent_id, cache_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(i, cache_info.data_extent_id_.offset);
    ASSERT_EQ(i, cache_info.cache_extent_id_.offset);
  }

  // no space
  data_extent_id.offset = 4;
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kNoSpace, ret);

  // re-alloc for data extent 1
  data_extent_id.file_number = 0;
  data_extent_id.offset = 1;
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kNoSpace, ret);
  // recycle it and re-alloc
  cache_info.cache_file_ = &cache_file;
  cache_info.data_extent_id_.file_number = 0;
  cache_info.data_extent_id_.offset = 1;
  cache_info.cache_extent_id_.file_number = INT32_MAX - 1;
  cache_info.cache_extent_id_.offset = 1;
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  cache_info.reset();
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(1, cache_info.data_extent_id_.offset);
  ASSERT_EQ(1, cache_info.cache_extent_id_.offset);

  // recycle cache extent 1 which is occupied by data extent 1,
  // but not erase it, and re-alloc again for data extent 3.
  cache_info.cache_file_ = &cache_file;
  cache_info.data_extent_id_.file_number = 0;
  cache_info.data_extent_id_.offset = 1;
  cache_info.cache_extent_id_.file_number = PERSISTENT_FILE_NUMBER;
  cache_info.cache_extent_id_.offset = 1;
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  data_extent_id.file_number = 0;
  data_extent_id.offset = 3;
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(3, cache_info.data_extent_id_.offset);
  ASSERT_EQ(1, cache_info.cache_extent_id_.offset);
  // the stored_status_ of data extent id 1 has been erase by previous alloc for data extent id 3
  // , and it should be succeed in follow alloc for data extent id
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  data_extent_id.file_number = 0;
  data_extent_id.offset = 1;
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(1, cache_info.data_extent_id_.offset);
  ASSERT_EQ(1, cache_info.cache_extent_id_.offset);
  // recycle and re-alloc for data extent 1
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  cache_info.reset();
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(1, cache_info.data_extent_id_.offset);
  ASSERT_EQ(1, cache_info.cache_extent_id_.offset);

  cache_file.close();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_file_recycle)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;
  PersistentCacheInfo cache_info;
  int64_t need_recover_extent_count = 0;
  ExtentId data_extent_id;

  // not init
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kNotInit, ret);

  // open cache file
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 3, need_recover_extent_count);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(0, need_recover_extent_count);

  // invalid argument
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // alloc
  for (int64_t i = 0; i < 3; ++i) {
    data_extent_id.offset = i;
    ret = cache_file.alloc(data_extent_id, cache_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_TRUE(cache_info.is_valid());
    ASSERT_EQ(i, cache_info.data_extent_id_.offset);
    ASSERT_EQ(i, cache_info.cache_extent_id_.offset);
  }

  // no space
  data_extent_id.offset = 3;
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kNoSpace, ret);

  // recycle extents
  cache_info.cache_file_ = &cache_file;
  cache_info.data_extent_id_.file_number = 0;
  cache_info.data_extent_id_.offset = 0;
  cache_info.cache_extent_id_.file_number = INT32_MAX - 1;
  cache_info.cache_extent_id_.offset = 0;
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  cache_info.data_extent_id_.offset = 1;
  cache_info.cache_extent_id_.offset = 1;
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  // repeat recycle same extent
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kErrorUnexpected, ret);
  // recycle not exist extent
  cache_info.cache_extent_id_.offset = 10;
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // continue write
  for (int64_t i = 0; i < 2; ++i) {
    data_extent_id.offset = i;
    ret = cache_file.alloc(data_extent_id, cache_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_TRUE(cache_info.is_valid());
    ASSERT_EQ(i, cache_info.cache_extent_id_.offset);
    ASSERT_EQ(i, cache_info.cache_extent_id_.offset);
  } 

  // exhausted again
  data_extent_id.offset = 3;
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kNoSpace, ret); 

  cache_file.close();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_file_write)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;
  PersistentCacheInfo cache_info;
  ExtentId data_extent_id;
  std::string data;
  int64_t need_recover_extent_count = 0;

  // not init
  ret = cache_file.write(Slice(), cache_info);
  ASSERT_EQ(Status::kNotInit, ret);

  // open cache file
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 2, need_recover_extent_count);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(0, need_recover_extent_count);

  // invalid argument
  ret = cache_file.write(Slice(), cache_info);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success to write data
  data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
  for (int64_t i = 0; i < 2; ++i) {
    data_extent_id.offset = i;
    ret = cache_file.alloc(data_extent_id, cache_info);
    ASSERT_EQ(Status::kOk, ret);
    ret = cache_file.write(Slice(data.data(), storage::MAX_EXTENT_SIZE), cache_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_TRUE(cache_info.is_valid());
    ASSERT_EQ(i, cache_info.data_extent_id_.offset);
    ASSERT_EQ(i, cache_info.cache_extent_id_.offset);
  }

  cache_file.close();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_file_read)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;
  PersistentCacheInfo cache_info;
  std::string data;
  Slice result;
  char *buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE)); 
  ASSERT_TRUE(nullptr != buf);
  int64_t need_recover_extent_count = 0;
  ExtentId data_extent_id;

  // not open
  ret = cache_file.read(cache_info, nullptr, 0, 0, nullptr, result);
  ASSERT_EQ(Status::kNotInit, ret);

  // open cache file
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 2, need_recover_extent_count); 
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(0, need_recover_extent_count);

  // invalid argument
  ret = cache_file.read(cache_info, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  // mock valid cache info
  cache_info.cache_file_ = reinterpret_cast<PersistentCacheFile *>(1);
  cache_info.cache_extent_id_.offset = 0;
  ret = cache_file.read(cache_info, nullptr, -1, 10, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.read(cache_info, nullptr, 0, 0, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.read(cache_info, nullptr, 0, 10, nullptr, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.read(cache_info, nullptr, 0, 10, buf, result);

  // exceed capacity
  cache_info.cache_extent_id_.offset = 10;
  ret = cache_file.read(cache_info, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kIOError, ret);

  // write data
  data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
  data_extent_id.offset = 1;
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kOk, ret);
  ret = cache_file.write(Slice(data.data(), data.size()), cache_info);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(1, cache_info.data_extent_id_.offset);
  ASSERT_EQ(0, cache_info.cache_extent_id_.offset);

  // read range[10, 110]
  ret = cache_file.read(cache_info, nullptr, 10, 100, buf, result);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_TRUE(0 == memcmp(data.data() + 10, result.data(), 100));

  // write the same data extent id(0, 1), it will proactively erased previous data.
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  ret = cache_file.alloc(data_extent_id, cache_info);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(1, cache_info.data_extent_id_.offset);
  ASSERT_EQ(0, cache_info.cache_extent_id_.offset);
  // The previous data has been erased.
  ret = cache_file.read(cache_info, nullptr, storage::MAX_EXTENT_SIZE - 1024 * 4, 1024 * 4, buf, result);
  ASSERT_EQ(Status::kOk, ret);
  for (int64_t i = 0; i < 1024 * 4; ++i) {
    ASSERT_EQ('\0', result.data()[i]);
  }
  ret = cache_file.write(Slice(data.data(), data.size()), cache_info);
  ASSERT_EQ(Status::kOk, ret);

  // read range[100, 200]
  ret = cache_file.read(cache_info, nullptr, 100, 100, buf, result);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_TRUE(0 == memcmp(data.data() + 100, result.data(), 100));

  cache_file.close();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_init)
{
  int ret = Status::kOk;
  
  // invalid argument
  ret = PersistentCache::get_instance().init(nullptr, file_dir_, 0, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().init(Env::Default(), std::string(), 0, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, -1, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, 0, 0, kReadWriteThrough);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, 0, 1, (PersistentCacheMode)(2));

  // disable persistent cache
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, 0, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_FALSE(PersistentCache::get_instance().is_enabled());

  // enable persistent cache
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, storage::MAX_EXTENT_SIZE * 3, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_TRUE(PersistentCache::get_instance().is_enabled());

  // recover persistent cache
  PersistentCache::get_instance().destroy();
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, storage::MAX_EXTENT_SIZE * 3, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // insert one extent
  std::string data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
  ExtentId data_extent_id;
  data_extent_id.offset = 1;
  ret = PersistentCache::get_instance().insert(data_extent_id, Slice(data.data(), data.size()), true, nullptr);
  ASSERT_EQ(Status::kOk, ret);

  // disable persistent cache after enable persistent cache
  ret = Env::Default()->FileExists(file_path_).code();
  ASSERT_EQ(Status::kOk, ret);
  PersistentCache::get_instance().destroy();
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, 0, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_FALSE(PersistentCache::get_instance().is_enabled());
  ret = Env::Default()->FileExists(file_path_).code();
  ASSERT_EQ(Status::kNotFound, ret);

  // rollback status
  PersistentCache::get_instance().destroy();
}

TEST_F(PersistentCacheTest, persistent_cache_insert)
{
  int ret = Status::kOk;
  cache::Cache::Handle *handle = nullptr;
  std::vector<cache::Cache::Handle *> handles;
  std::string data;
  std::vector<std::string> datas;
  storage::ExtentId data_extent_id(0, 1);

  // not init
  ret = PersistentCache::get_instance().insert(data_extent_id, Slice(data.data(), data.size()), true, nullptr);
  ASSERT_EQ(Status::kNotInit, ret);

  // init persistent cache
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, storage::MAX_EXTENT_SIZE * 3, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // full fill the persistent cache and acquire the handle
  for (int64_t i = 0; i < 3; ++i) {
    handle = nullptr;
    data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
    data_extent_id.offset = i;
    ret = PersistentCache::get_instance().insert(data_extent_id, Slice(data.data(), data.size()), true, &handle);
    ASSERT_EQ(Status::kOk, ret);
    handles.push_back(handle);
    datas.push_back(data); 
  }

  // no space
  data_extent_id.offset = 3;
  ret = PersistentCache::get_instance().insert(data_extent_id, Slice(data.data(), data.size()), true, &handle);
  ASSERT_EQ(Status::kNoSpace,ret);

  // release one handle and re-insert
  PersistentCache::get_instance().release_handle(handles[0]);
  ret = PersistentCache::get_instance().insert(data_extent_id, Slice(data.data(), data.size()), true, nullptr);
  ASSERT_EQ(Status::kOk, ret);

  // release left handles
  PersistentCache::get_instance().release_handle(handles[1]);
  PersistentCache::get_instance().release_handle(handles[2]);

  // rollback status
  PersistentCache::get_instance().destroy();

  // recover the persistent cache with same cache size.
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, 3 * storage::MAX_EXTENT_SIZE, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // the persistent cache is empty after recover, because the extent footer is invalid.
  data_extent_id.offset = 4;
  ret = PersistentCache::get_instance().insert(data_extent_id, Slice(data.data(), data.size()), true, nullptr);
  ASSERT_EQ(Status::kOk, ret);

  PersistentCache::get_instance().destroy();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_read_from_handle)
{
  int ret = Status::kOk;
  cache::Cache::Handle *handle = nullptr;
  std::vector<cache::Cache::Handle *> handles;
  std::string data;
  std::vector<std::string> datas;
  storage::ExtentId data_extent_id(0, 1);
  Slice result;
  char *buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE));

  // not init
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 0, nullptr, result);
  ASSERT_EQ(Status::kNotInit, ret);

  // init persistent cache
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, storage::MAX_EXTENT_SIZE * 3, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // invalid argument
  ret = PersistentCache::get_instance().read_from_handle(nullptr, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  // mock valid handle
  handle = reinterpret_cast<cache::Cache::Handle *>(1);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, -1, 0, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 0, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, -1, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 10, nullptr, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // null cache info
  cache::LRUHandle lru_handle;
  handle = reinterpret_cast<cache::Cache::Handle *>(&lru_handle);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // invalid cache info
  PersistentCacheInfo cache_info;
  lru_handle.value = &cache_info;
  handle = reinterpret_cast<cache::Cache::Handle *>(&lru_handle);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // prepare data
  for (int64_t i = 0; i < 3; ++i) {
    handle = nullptr;
    data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
    data_extent_id.offset = i;
    ret = PersistentCache::get_instance().insert(data_extent_id, Slice(data.data(), data.size()), true, &handle);
    ASSERT_EQ(Status::kOk, ret);
    handles.push_back(handle);
    datas.push_back(data); 
  } 

  // check data
  int64_t offset = 0;
  int64_t size = 0;
  for (int64_t i = 0; i < 3; ++i) {
    handle = handles[i];
    data = datas[i];
    offset = i * 10;
    size = i + 100;
    ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, offset, size, buf, result);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, memcmp(data.data() + offset, result.data(), size));
    PersistentCache::get_instance().release_handle(handle);
  }

  // rollback status
  PersistentCache::get_instance().destroy();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_lookup_and_evict)
{
  int ret = Status::kOk;
  cache::Cache::Handle *handle = nullptr;
  std::vector<cache::Cache::Handle *> handles;
  std::string data;
  std::vector<std::string> datas;
  storage::ExtentId data_extent_id(0, 1);
  Slice result;
  char *buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE)); 

  // not init
  ret = PersistentCache::get_instance().lookup(data_extent_id, handle);
  ASSERT_EQ(Status::kNotInit, ret);
  ret = PersistentCache::get_instance().evict(data_extent_id);
  ASSERT_EQ(Status::kNotInit, ret);

  // init persistent cache
  // make cache size enough to contain three extent
  int64_t cache_size = (1 << 8) * storage::MAX_EXTENT_SIZE;
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, cache_size, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // prepare data
  for (int64_t i = 0; i < 3; ++i) {
    data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
    data_extent_id.offset = i;
    ret = PersistentCache::get_instance().insert(data_extent_id, Slice(data.data(), data.size()), true, nullptr);
    ASSERT_EQ(Status::kOk, ret);
    datas.push_back(data); 
  } 

  // lookup and check data
  for (int64_t i = 0; i < 3; ++i) {
    handle = nullptr;
    data = datas[i];
    data_extent_id.offset = i;
    ret = PersistentCache::get_instance().lookup(data_extent_id, handle);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_TRUE(nullptr != handle);
    ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 10, 200, buf, result);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, memcmp(data.data() + 10, result.data(), 200));
    PersistentCache::get_instance().release_handle(handle);
  }

  // cache miss
  data_extent_id.offset = 4;
  ret = PersistentCache::get_instance().lookup(data_extent_id, handle);
  ASSERT_EQ(Status::kNotFound, ret);

  // evict extent [0, 1]
  data_extent_id.offset = 1;
  ret = PersistentCache::get_instance().evict(data_extent_id);
  ASSERT_EQ(Status::kOk, ret);
  ret = PersistentCache::get_instance().lookup(data_extent_id, handle);
  ASSERT_EQ(Status::kNotFound, ret);

  // rollback status
  PersistentCache::get_instance().destroy();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_recover)
{
  int ret = Status::kOk;
  PersistentCacheInfo *cache_info = nullptr;
  cache::Cache::Handle *handle = nullptr;
  std::vector<std::string> extent_datas;
  std::vector<cache::Cache::Handle *> handles;
  storage::ExtentId data_extent_id;
  char *buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE));
  Slice result;

  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, 8 * storage::MAX_EXTENT_SIZE, 2, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // prepare data. insert one corrupted extent at last.
  std::string extent_data;
  for (int64_t i = 0; i < 8; ++i) {
    handle = nullptr;
    data_extent_id.offset = i;
    if (7 == i) {
      extent_data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
    } else {
      generate_extent_data(data_extent_id, extent_data);
    }
    ret = PersistentCache::get_instance().insert(data_extent_id, Slice(extent_data.data(), extent_data.size()), true, &handle);
    ASSERT_EQ(Status::kOk, ret);
    cache_info = PersistentCache::get_instance().get_cache_info_from_handle(handle);
    ASSERT_EQ(i, cache_info->data_extent_id_.offset);
    ASSERT_EQ(i, cache_info->cache_extent_id_.offset);
    handles.push_back(handle);
    extent_datas.push_back(extent_data);
  }

  // check data
  int64_t offset = 0;
  int64_t size = 0;
  for (int64_t i = 0; i < 8; ++i) {
    handle = handles[i];
    extent_data = extent_datas[i];
    offset = i * 10;
    size = i + 100;
    ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, offset, size, buf, result);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, memcmp(extent_data.data() + offset, result.data(), size));
    PersistentCache::get_instance().release_handle(handle);
  }

  // recover the persistent cache with same cache size
  PersistentCache::get_instance().destroy();
  ret = PersistentCache::get_instance().init(Env::Default(), file_dir_, 8 * storage::MAX_EXTENT_SIZE, 1, kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // check data
  for (int64_t i = 0; i < 8; ++i) {
    handle = nullptr;
    extent_data = extent_datas[i];
    data_extent_id.offset = i;
    // wait until recover finished.
    do {
      ret = PersistentCache::get_instance().lookup(data_extent_id, handle);
    } while (0 == i && Status::kNotFound == ret);

    if (7 == i) {
      ASSERT_EQ(Status::kNotFound, ret);
    } else {
      ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, storage::MAX_EXTENT_SIZE, buf, result);
      ASSERT_EQ(Status::kOk, ret);
      cache_info = PersistentCache::get_instance().get_cache_info_from_handle(handle);
      ASSERT_EQ(i, cache_info->data_extent_id_.offset);
      ASSERT_EQ(i, cache_info->cache_extent_id_.offset);
      ASSERT_EQ(0, memcmp(extent_data.data(), result.data(), storage::MAX_EXTENT_SIZE));
    }
  }

  if (IS_NOTNULL(buf)) {
    base_free(buf);
  }

  // rollback status
  PersistentCache::get_instance().destroy();
  ret = remove_cache_file();
  ASSERT_EQ(Status::kOk, ret);
}

} // namespace cache
} // namespace smartengine

int main(int argc, char **argv) 
{
  std::string info_log_path = smartengine::util::test::TmpDir() + "/persistent_cache_test.log";
  std::string abs_test_dir = smartengine::util::test::TmpDir() + test_dir;
  smartengine::util::test::remove_dir(abs_test_dir.c_str());
  smartengine::util::Env::Default()->CreateDir(abs_test_dir);
	smartengine::util::test::init_logger(info_log_path.c_str(), smartengine::logger::INFO_LEVEL);
  smartengine::util::StackTrace::install_handler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}