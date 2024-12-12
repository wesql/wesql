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

#include "util/testharness.h"
#include "util/testutil.h"
#include "cache/persistent_cache.h"
#include "storage/io_extent.h"

static const std::string test_dir = smartengine::util::test::TmpDir() + "/io_extent_test";
namespace smartengine
{
using namespace common;
using namespace util;
namespace storage
{

class IOExtentTest
{
public:
  IOExtentTest() : io_info_(), aligned_buf_(nullptr), data_() {}
  virtual ~IOExtentTest() {}

protected:
  std::string generate_random_string(int64_t length)
  {
    // use current time as seed
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 generator(seed);
    std::uniform_int_distribution<int> distribution(32, 126); // ASCII visible range

    // generate
    std::string result;
    result.reserve(length);
    for (int64_t i = 0; i < length; ++i) {
        result.push_back(static_cast<char>(distribution(generator)));
    }

    return result;
  }

  void check_read(const std::string &data,
                  IOExtent *io_extent,
                  const int64_t offset,
                  const int64_t size,
                  const int64_t aligned_buf_offset,
                  util::AIOHandle *aio_handle)
  {
    int ret = Status::kOk;
    Slice resut;
    memset(aligned_buf_, 0, 2 * MAX_EXTENT_SIZE);

    ret = io_extent->read(aio_handle, offset, size, aligned_buf_ + aligned_buf_offset, resut);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(size, resut.size());
    ret = memcmp(data.data() + offset, resut.data(), size);
    ASSERT_EQ(0, ret);
  }

protected:
  static const int64_t ALIGNED_BUF_SIZE = 2 * MAX_EXTENT_SIZE;

  ExtentIOInfo io_info_;
  char *aligned_buf_;
  std::string data_;
};

class FileIOExtentTest : public testing::Test, public IOExtentTest
{
public:
  FileIOExtentTest() {}
  ~FileIOExtentTest() override {}

  virtual void SetUp()
  {
    int ret = 0;
    // Simulate read and write operations on extent(0, 2).
    io_info_.extent_id_ = ExtentId(0, 2);
    io_info_.extent_size_ = MAX_EXTENT_SIZE;
    io_info_.unique_id_ = 1;

    // open file
    std::string file_name = test_dir + TEST_FILE_NAME;

    if (0 > (io_info_.fd_ = ::open(file_name.c_str(), O_RDWR | O_DIRECT | O_CREAT))) {
      fprintf(stderr, "Failed to open file: errno = %d (%s)\n", errno, strerror(errno));
    }
    assert(io_info_.fd_ > 0);

    // fallocate space for extents (0, 0),(0,1)
    ret = ::fallocate(io_info_.fd_, FALLOC_FL_KEEP_SIZE, 0, 3 * MAX_EXTENT_SIZE);
    assert(-1 != ret);

    // generate data
    data_ = generate_random_string(MAX_EXTENT_SIZE);

    // allocate aligned buf
    aligned_buf_ = reinterpret_cast<char *>(memory::base_memalign(DIOHelper::DIO_ALIGN_SIZE, ALIGNED_BUF_SIZE));
  }

    virtual void TearDown()
    {
      // remove file
      std::string file_name = test_dir + TEST_FILE_NAME;
      ::close(io_info_.fd_);
      ::unlink(file_name.c_str());

      io_info_.reset();
      data_.clear();
      memory::base_memalign_free(aligned_buf_);
    }

protected:
  void prepare_data()
  {
    int ret = Status::kOk;
    //WritableExtent writable_extent;
    FileIOExtent writable_extent;
    ret = writable_extent.init(io_info_.extent_id_, io_info_.unique_id_, io_info_.fd_);
    ASSERT_EQ(Status::kOk, ret);
    memcpy(aligned_buf_, data_.data(), MAX_EXTENT_SIZE);
    ret = writable_extent.write(Slice(aligned_buf_, MAX_EXTENT_SIZE), 0);
    ASSERT_EQ(Status::kOk, ret);   
  }

protected:
  static const char *TEST_FILE_NAME;
};
const char *FileIOExtentTest::TEST_FILE_NAME = "/io_extent.sst";

class ObjectIOExtentTest : public testing::Test, public IOExtentTest
{
public:
  ObjectIOExtentTest()
  {
    object_base_path_ = test_dir + "/local_object_store";
    bucket_name_ = "bucket";
  }
  ~ObjectIOExtentTest() override {}

  virtual void SetUp()
  {
    int ret = Status::kOk;
    ::objstore::Status object_status;

    ret = Env::Default()->InitObjectStore("local", object_base_path_, nullptr, false, bucket_name_, "repo/branch", 0).code();
    ASSERT_EQ(Status::kOk, ret);
    ret = Env::Default()->GetObjectStore(object_store_).code();
    ASSERT_EQ(Status::kOk, ret);

    object_status = object_store_->create_bucket(bucket_name_);
    ASSERT_TRUE(object_status.is_succ());

    io_info_.extent_id_.file_number = convert_table_space_to_fd(1);
    io_info_.extent_id_.offset = 1;

    aligned_buf_ = reinterpret_cast<char *>(memory::base_memalign(DIOHelper::DIO_ALIGN_SIZE, ALIGNED_BUF_SIZE));

    // generate data
    data_ = generate_random_string(MAX_EXTENT_SIZE);
  }

  virtual void TearDown()
  {
    ::objstore::Status object_status;
    object_status = object_store_->delete_bucket(bucket_name_);
    ASSERT_TRUE(object_status.is_succ());

    memory::base_memalign_free(aligned_buf_);

    data_.clear();
  }

  void prepare_data()
  {
    int ret = Status::kOk;
    ObjectIOExtent object_extent;
    ret = object_extent.init(io_info_.extent_id_,
                             0,
                             object_store_,
                             Env::Default()->GetObjectStoreBucket(),
                             std::string());
    ASSERT_EQ(Status::kOk, ret);
    memcpy(aligned_buf_, data_.data(), MAX_EXTENT_SIZE);
    ret = object_extent.write(Slice(aligned_buf_, MAX_EXTENT_SIZE), 0);
    ASSERT_EQ(Status::kOk, ret);   
  }

protected:
  std::string object_base_path_;
  std::string bucket_name_;
  ::objstore::ObjectStore *object_store_;
};

TEST_F(FileIOExtentTest, file_io_extent_init)
{
  int ret = Status::kOk;
  FileIOExtent file_extent;

  // invalid unique id
  ret = file_extent.init(io_info_.extent_id_, -1, 1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // invalid fd
  ret = file_extent.init(io_info_.extent_id_, 1, -1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success init
  ret = file_extent.init(io_info_.extent_id_, 1, 1);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(io_info_.extent_id_.id(), file_extent.get_extent_id().id());
  //ASSERT_EQ(1, file_extent.get_unique_id());

  // init twice
  ret = file_extent.init(io_info_.extent_id_, 1, 1);
  ASSERT_EQ(Status::kInitTwice, ret);
}

TEST_F(FileIOExtentTest, file_io_extent_write)
{
  int ret = Status::kOk;
  FileIOExtent file_extent;

  // not init
  ret = file_extent.write(Slice(), 0);
  ASSERT_EQ(Status::kNotInit, ret);

  // init
  ret = file_extent.init(io_info_.extent_id_, io_info_.unique_id_, io_info_.fd_);
  ASSERT_EQ(Status::kOk, ret);

  // empty data
  ret = file_extent.write(Slice(), 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // invalid data size
  ret = file_extent.write(Slice(data_.data(), storage::MAX_EXTENT_SIZE * 2), 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // not aligned data, success write
  char *not_aligned_addr = (aligned_buf_ + DIOHelper::DIO_ALIGN_SIZE /2);
  memcpy(not_aligned_addr, data_.data(), MAX_EXTENT_SIZE);
  ret = file_extent.write(Slice(not_aligned_addr, MAX_EXTENT_SIZE), 0);
  ASSERT_EQ(Status::kOk, ret);

  // aligned data, success write
  memcpy(aligned_buf_, data_.data(), MAX_EXTENT_SIZE);
  ret = file_extent.write(Slice(aligned_buf_, MAX_EXTENT_SIZE), 0);
  ASSERT_EQ(Status::kOk, ret);

  // check data
  check_read(data_, &file_extent, 0, MAX_EXTENT_SIZE, 0, nullptr);
}

TEST_F(FileIOExtentTest, file_io_extent_positioned_write)
{
  int ret = Status::kOk;
  FileIOExtent file_extent;
  int64_t offset = 0;
  int64_t size = 4 * 1024;

  // not init
  ret = file_extent.write(Slice(), offset);
  ASSERT_EQ(Status::kNotInit, ret);

  // init
  ret = file_extent.init(io_info_.extent_id_, io_info_.unique_id_, io_info_.fd_);
  ASSERT_EQ(Status::kOk, ret);

  // invalid argument
  // invalid data
  ret = file_extent.write(Slice(), 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  // invalid offset
  ret = file_extent.write(Slice(data_), -1);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  // exceed extent size
  ret = file_extent.write(Slice(data_.data(), size), storage::MAX_EXTENT_SIZE - 1024);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // not aligned offset 
  ret = file_extent.write(Slice(data_.data(), size), 1024);
  ASSERT_EQ(Status::kErrorUnexpected, ret);
  // not aligned size
  ret = file_extent.write(Slice(data_.data(), 1024), storage::MAX_EXTENT_SIZE - size);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // success write
  ret = file_extent.write(Slice(data_.data() + storage::MAX_EXTENT_SIZE - size, size), storage::MAX_EXTENT_SIZE - size);
  ASSERT_EQ(Status::kOk, ret);
  
  // check data
  check_read(data_, &file_extent, storage::MAX_EXTENT_SIZE - size, size, 0, nullptr);
}

TEST_F(FileIOExtentTest, file_io_extent_sync_read)
{
  int ret = Status::kOk;
  FileIOExtent file_extent;
  Slice result;

  // Prepare data
  prepare_data();

  // Not init
  ret = file_extent.read( nullptr /*aio_handle*/, 0, MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kNotInit, ret);

  // Init
  ret = file_extent.init(io_info_.extent_id_, io_info_.unique_id_, io_info_.fd_);
  ASSERT_EQ(Status::kOk, ret);

  // Invalid offset
  ret = file_extent.read( nullptr /*aio_handle*/, -1, MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid size
  ret = file_extent.read(nullptr /*aio_handle*/,0, 0, aligned_buf_, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid buffer
  ret = file_extent.read(nullptr /*aio_handle=*/, 0, MAX_EXTENT_SIZE, nullptr /*buf=*/, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // OverLimit extent size
  ret = file_extent.read(nullptr /*aio_handle*/, 0, 2 * MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kOverLimit, ret);

  // Aligned offset, size, buf
  check_read(data_, &file_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE, 0, nullptr);

  check_read(data_, &file_extent, DIOHelper::DIO_ALIGN_SIZE * 2, DIOHelper::DIO_ALIGN_SIZE * 10, 0, nullptr);

  check_read(data_, &file_extent, 0, MAX_EXTENT_SIZE, 0, nullptr);

  // Not aligned offset, and aligned size, buf
  check_read(data_, &file_extent, DIOHelper::DIO_ALIGN_SIZE / 2, DIOHelper::DIO_ALIGN_SIZE, 0, nullptr);

  check_read(data_, &file_extent,
            DIOHelper::DIO_ALIGN_SIZE * 10 + DIOHelper::DIO_ALIGN_SIZE / 10,
            DIOHelper::DIO_ALIGN_SIZE * 10,
            0,
            nullptr);

  // Not aligned size, and aligned offset, buf
  check_read(data_, &file_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE / 2, 0, nullptr);

  check_read(data_, &file_extent,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 10 + DIOHelper::DIO_ALIGN_SIZE / 10,
             0,
             nullptr);
  
  // Not aligned buf, and aligned offset, size
  check_read(data_, &file_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE / 2, nullptr);

  check_read(data_, &file_extent,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 5 + DIOHelper::DIO_ALIGN_SIZE / 5,
             nullptr);

  // Random offset, size, buf
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    check_read(data_, &file_extent, generator.generate(), generator.generate(), generator.generate(), nullptr);
  }
}

TEST_F(FileIOExtentTest, file_io_extent_async_read)
{
  int ret = Status::kOk;
  FileIOExtent file_extent;

  // Prepare data
  prepare_data();

  // Init
  ret = file_extent.init(io_info_.extent_id_, io_info_.unique_id_, io_info_.fd_);
  ASSERT_EQ(Status::kOk, ret);

  // Random async read
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    int64_t offset = generator.generate();
    int64_t size = generator.generate();
    int64_t aligned_buf_offset = generator.generate(); 
    ret = file_extent.prefetch(&aio_handle, offset, size);
    ASSERT_EQ(Status::kOk, ret);
    check_read(data_, &file_extent, offset, size, aligned_buf_offset, &aio_handle);
  }

  // Read range is not consistent with prefetch range
  for (int32_t i = 0; i < 10000; ++i) {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    ret = file_extent.prefetch(&aio_handle, generator.generate(), generator.generate());
    ASSERT_EQ(Status::kOk, ret);
    check_read(data_, &file_extent, generator.generate(), generator.generate(), generator.generate(), &aio_handle);
  }

  // Prefetch overlimit range
  {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    ret = file_extent.prefetch(&aio_handle, MAX_EXTENT_SIZE / 2, MAX_EXTENT_SIZE);
    ASSERT_EQ(Status::kOk, ret);
  }
}

TEST_F(ObjectIOExtentTest, object_io_extent_init)
{
  int ret = Status::kOk;
  ObjectIOExtent object_extent;

  // invalid argument
  ret = object_extent.init(io_info_.extent_id_,
                           -1,
                           object_store_,
                           Env::Default()->GetObjectStoreBucket(),
                           std::string());
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = object_extent.init(io_info_.extent_id_,
                           0,
                           nullptr,
                           Env::Default()->GetObjectStoreBucket(),
                           std::string());
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = object_extent.init(io_info_.extent_id_,
                           0,
                           object_store_,
                           std::string(),
                           std::string());
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success init
  ret = object_extent.init(io_info_.extent_id_,
                           0,
                           object_store_,
                           Env::Default()->GetObjectStoreBucket(),
                           std::string());
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(ObjectIOExtentTest, object_io_extent_write)
{
  int ret = Status::kOk;
  ObjectIOExtent object_extent;
  std::string data;

  // not init
  ret = object_extent.write(Slice(), 0);
  ASSERT_EQ(Status::kNotInit, ret);

  // init object extent
  ret = object_extent.init(io_info_.extent_id_,
                           0,
                           object_store_,
                           Env::Default()->GetObjectStoreBucket(),
                           std::string());
  ASSERT_EQ(Status::kOk, ret);

  // invalid argument
  ret = object_extent.write(Slice(), 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // invalid data size
  ret = object_extent.write(Slice(data.data(), storage::MAX_EXTENT_SIZE * 2), 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  // invalid data offset
  ret = object_extent.write(Slice(data.data(), storage::MAX_EXTENT_SIZE), -1);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = object_extent.write(Slice(data.data(), storage::MAX_EXTENT_SIZE -1), 1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success write
  data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
  ret = object_extent.write(Slice(data.data(), data.size()), 0);
  ASSERT_EQ(Status::kOk, ret);

  // check data
  check_read(data, &object_extent, 0, MAX_EXTENT_SIZE, 0, nullptr);
}

TEST_F(ObjectIOExtentTest, object_io_extent_sync_read)
{
  int ret = Status::kOk;
  ObjectIOExtent object_extent;
  Slice result;

  // Prepare data
  prepare_data();

  // Not init
  ret = object_extent.read( nullptr /*aio_handle*/, 0, MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kNotInit, ret);

  // Init
  ret = object_extent.init(io_info_.extent_id_,
                           0,
                           object_store_,
                           Env::Default()->GetObjectStoreBucket(),
                           std::string());
  ASSERT_EQ(Status::kOk, ret);

  // Invalid offset
  ret = object_extent.read( nullptr /*aio_handle*/, -1, MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid size
  ret = object_extent.read(nullptr /*aio_handle*/,0, 0, aligned_buf_, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid buffer
  ret = object_extent.read(nullptr /*aio_handle=*/, 0, MAX_EXTENT_SIZE, nullptr /*buf=*/, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // OverLimit extent size
  ret = object_extent.read(nullptr /*aio_handle*/, 0, 2 * MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kOverLimit, ret);

  // Aligned offset, size, buf
  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE, 0, nullptr);

  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE * 2, DIOHelper::DIO_ALIGN_SIZE * 10, 0, nullptr);

  check_read(data_, &object_extent, 0, MAX_EXTENT_SIZE, 0, nullptr);

  // Not aligned offset, and aligned size, buf
  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE / 2, DIOHelper::DIO_ALIGN_SIZE, 0, nullptr);

  check_read(data_, &object_extent,
            DIOHelper::DIO_ALIGN_SIZE * 10 + DIOHelper::DIO_ALIGN_SIZE / 10,
            DIOHelper::DIO_ALIGN_SIZE * 10,
            0,
            nullptr);

  // Not aligned size, and aligned offset, buf
  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE / 2, 0, nullptr);

  check_read(data_, &object_extent,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 10 + DIOHelper::DIO_ALIGN_SIZE / 10,
             0,
             nullptr);
  
  // Not aligned buf, and aligned offset, size
  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE / 2, nullptr);

  check_read(data_, &object_extent,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 5 + DIOHelper::DIO_ALIGN_SIZE / 5,
             nullptr);

  // Random offset, size, buf
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    check_read(data_, &object_extent, generator.generate(), generator.generate(), generator.generate(), nullptr);
  }
}

TEST_F(ObjectIOExtentTest, object_io_extent_sync_read_with_persistent_cache)
{
  int ret = Status::kOk;
  ObjectIOExtent object_extent;
  Slice result;
  int64_t need_recover_extent_count = 0;

  // init persistent cache
  ret = cache::PersistentCache::get_instance().init(Env::Default(),
                                                    test_dir,
                                                    1 * 1024 * 1024 * 1024,
                                                    1,
                                                    common::kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // Prepare data
  prepare_data();

  // Not init
  ret = object_extent.read( nullptr /*aio_handle*/, 0, MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kNotInit, ret);

  // Init
  ret = object_extent.init(io_info_.extent_id_,
                           0,
                           object_store_,
                           Env::Default()->GetObjectStoreBucket(),
                           std::string());
  ASSERT_EQ(Status::kOk, ret);

  // Invalid offset
  ret = object_extent.read( nullptr /*aio_handle*/, -1, MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid size
  ret = object_extent.read(nullptr /*aio_handle*/,0, 0, aligned_buf_, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid buffer
  ret = object_extent.read(nullptr /*aio_handle=*/, 0, MAX_EXTENT_SIZE, nullptr /*buf=*/, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // OverLimit extent size
  ret = object_extent.read(nullptr /*aio_handle*/, 0, 2 * MAX_EXTENT_SIZE, aligned_buf_, result);
  ASSERT_EQ(Status::kOverLimit, ret);

  // Aligned offset, size, buf
  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE, 0, nullptr);

  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE * 2, DIOHelper::DIO_ALIGN_SIZE * 10, 0, nullptr);

  check_read(data_, &object_extent, 0, MAX_EXTENT_SIZE, 0, nullptr);

  // Not aligned offset, and aligned size, buf
  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE / 2, DIOHelper::DIO_ALIGN_SIZE, 0, nullptr);

  check_read(data_, &object_extent,
            DIOHelper::DIO_ALIGN_SIZE * 10 + DIOHelper::DIO_ALIGN_SIZE / 10,
            DIOHelper::DIO_ALIGN_SIZE * 10,
            0,
            nullptr);

  // Not aligned size, and aligned offset, buf
  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE / 2, 0, nullptr);

  check_read(data_, &object_extent,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 10 + DIOHelper::DIO_ALIGN_SIZE / 10,
             0,
             nullptr);
  
  // Not aligned buf, and aligned offset, size
  check_read(data_, &object_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE / 2, nullptr);

  check_read(data_, &object_extent,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 5 + DIOHelper::DIO_ALIGN_SIZE / 5,
             nullptr);

  // Random offset, size, buf
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    check_read(data_, &object_extent, generator.generate(), generator.generate(), generator.generate(), nullptr);
  }

  // rollback status
  cache::PersistentCache::get_instance().destroy();
}

TEST_F(ObjectIOExtentTest, object_io_extent_async_read)
{
  int ret = Status::kOk;
  ObjectIOExtent object_extent;

  // Prepare data
  prepare_data();

  // Init
  ret = object_extent.init(io_info_.extent_id_,
                           0,
                           object_store_,
                           Env::Default()->GetObjectStoreBucket(),
                           std::string());
  ASSERT_EQ(Status::kOk, ret);

  // Random async read
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    int64_t offset = generator.generate();
    int64_t size = generator.generate();
    int64_t aligned_buf_offset = generator.generate(); 
    ret = object_extent.prefetch(&aio_handle, offset, size);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(Status::kNoSpace, aio_handle.aio_req_->status_);
    check_read(data_, &object_extent, offset, size, aligned_buf_offset, &aio_handle);
  }

  // Read range is not consistent with prefetch range
  for (int32_t i = 0; i < 10000; ++i) {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    ret = object_extent.prefetch(&aio_handle, generator.generate(), generator.generate());
    ASSERT_EQ(Status::kOk, ret);
    check_read(data_, &object_extent, generator.generate(), generator.generate(), generator.generate(), &aio_handle);
  }

  // Prefetch overlimit range
  {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    ret = object_extent.prefetch(&aio_handle, MAX_EXTENT_SIZE / 2, MAX_EXTENT_SIZE);
    ASSERT_EQ(Status::kOk, ret);
  }
}

TEST_F(ObjectIOExtentTest, object_io_extent_async_read_with_persistent_cache)
{
  int ret = Status::kOk;
  ObjectIOExtent object_extent;

  // init persistent cache
  ret = cache::PersistentCache::get_instance().init(Env::Default(),
                                                    test_dir,
                                                    1 * 1024 * 1024 * 1024,
                                                    1,
                                                    common::kReadWriteThrough);
  ASSERT_EQ(Status::kOk, ret);

  // Prepare data
  prepare_data();

  // Init
  ret = object_extent.init(io_info_.extent_id_,
                           0,
                           object_store_,
                           Env::Default()->GetObjectStoreBucket(),
                           std::string());
  ASSERT_EQ(Status::kOk, ret);

  // Random async read
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    int64_t offset = generator.generate();
    int64_t size = generator.generate();
    int64_t aligned_buf_offset = generator.generate(); 
    ret = object_extent.prefetch(&aio_handle, offset, size);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(Status::kBusy, aio_handle.aio_req_->status_);
    check_read(data_, &object_extent, offset, size, aligned_buf_offset, &aio_handle);
  }

  // Read range is not consistent with prefetch range
  for (int32_t i = 0; i < 10000; ++i) {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    ret = object_extent.prefetch(&aio_handle, generator.generate(), generator.generate());
    ASSERT_EQ(Status::kOk, ret);
    check_read(data_, &object_extent, generator.generate(), generator.generate(), generator.generate(), &aio_handle);
  }

  // Prefetch overlimit range
  {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    ret = object_extent.prefetch(&aio_handle, MAX_EXTENT_SIZE / 2, MAX_EXTENT_SIZE);
    ASSERT_EQ(Status::kOk, ret);
  }
}

} // namespace storage
} // namespace smartengine

int main(int argc, char **argv)
{
  smartengine::util::test::remove_dir(test_dir.c_str());
  smartengine::util::Env::Default()->CreateDir(test_dir);
  std::string log_path = smartengine::util::test::TmpDir() + "/io_extent_test.log";
  smartengine::logger::Logger::get_log().init(log_path.c_str(), smartengine::logger::DEBUG_LEVEL);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
