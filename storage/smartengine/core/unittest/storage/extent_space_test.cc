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

#include <cstdio>
#include <cstdlib>

#include "db/db_test_util.h"
#include "objstore.h"
#include "storage/extent_space.h"
#include "storage/extent_space_file.h"
#include "storage/extent_space_obj.h"
#include "storage/storage_common.h"
#include "util/file_name.h"
#include "util/testharness.h"

static const std::string test_dir = smartengine::util::test::TmpDir() + "/extent_space_test";
static const std::string test_local_obs_basepath = test_dir + "/local_obs";
static const std::string test_local_obs_bucket = "extent_space_test_bucket";

namespace smartengine
{
using namespace common;
using namespace util;
namespace storage
{
class ExtentSpaceTest : public testing::TestWithParam<bool> {
 public:
  ExtentSpaceTest() : env_(util::Env::Default()),
                      env_options_(),
                      extent_space_(nullptr)
  {
    use_obj_ = GetParam();
    if (use_obj_) {
      auto s = env_->InitObjectStore("local",
                                     test_local_obs_basepath /* use test_dir as the basepath */,
                                     nullptr,
                                     false,
                                     test_local_obs_bucket,
                                     "repo/branch",
                                     0);
      assert(s.ok());
      s = env_->GetObjectStore(obs_);
      assert(s.ok());
      auto ss = obs_->create_bucket(test_local_obs_bucket);
      assert(ss.is_succ());
    } else {
      obs_ = nullptr;
    }
  }
  ~ExtentSpaceTest()
  {
    if (use_obj_) {
      auto ss = obs_->delete_bucket(test_local_obs_bucket);
      assert(ss.is_succ());
      env_->DestroyObjectStore();
    }
  }

  void build_extent_space_args(CreateExtentSpaceArgs &args) {
    args.table_space_id_ = 1;
    args.extent_space_type_ = use_obj_ ? OBJECT_EXTENT_SPACE : FILE_EXTENT_SPACE;
    args.db_path_.path = use_obj_ ? test_local_obs_bucket : test_dir;
    args.db_path_.target_size = use_obj_ ? UINT64_MAX : 1024 * 1024 * 1024;
  }

protected:
  virtual void SetUp()
  {
    extent_space_ = use_obj_
                        ? dynamic_cast<ExtentSpace *>(
                              new ObjectExtentSpace(env_, env_options_, obs_))
                        : dynamic_cast<ExtentSpace *>(
                              new FileExtentSpace(env_, env_options_));
  }
  virtual void TearDown()
  {
    if (nullptr != extent_space_) {
      delete extent_space_;
      extent_space_ = nullptr;
    }
  }

protected:
  util::Env *env_;
  util::EnvOptions env_options_;
  bool use_obj_;
  objstore::ObjectStore *obs_;
  ExtentSpace *extent_space_;
  CreateExtentSpaceArgs args_;
};

INSTANTIATE_TEST_CASE_P(FileOrObjstore, ExtentSpaceTest,
                        testing::Values(false, true));

TEST_P(ExtentSpaceTest, create) {
  int ret = Status::kOk;
  CreateExtentSpaceArgs args;

  //invalid args
  args.table_space_id_ = -1;
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //success to create
  build_extent_space_args(args);
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kOk, ret);

  //dumplicate create
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kInitTwice, ret);
}

TEST_P(ExtentSpaceTest, file_extent_allocate_and_recycle) {
  if (use_obj_) {
    // skip the file extent test for objstore.
    return;
  }

  int ret = Status::kOk;
  CreateExtentSpaceArgs args;
  ExtentIOInfo io_info;
  ExtentId extent_id;

  // not init
  ret = extent_space_->allocate(std::string(), io_info);
  ASSERT_EQ(Status::kNotInit, ret);

  // success to allocate
  build_extent_space_args(args);
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  for (int64_t i = 1; Status::kOk == ret && i < 5120; ++i) {
    ret = extent_space_->allocate(std::string(), io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, io_info.extent_id_.file_number);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }

  for (int64_t i = 1; Status::kOk == ret && i < 5120; ++i) {
    ret = extent_space_->allocate(std::string(), io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(1, io_info.extent_id_.file_number);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }

  // recycle extent, which datafile not exist
  extent_id.file_number = 2;
  extent_id.offset = 1;
  ret = extent_space_->recycle(std::string(), extent_id);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // recycle extent, which offset is exceed the datafile
  extent_id.file_number = 1;
  extent_id.offset = 5122;
  ret = extent_space_->recycle(std::string(), extent_id);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success to recycle extent
  extent_id.file_number = 1;
  extent_id.offset = 1;
  ret = extent_space_->recycle(std::string(), extent_id);
  ASSERT_EQ(Status::kOk, ret);
  ExtentIOInfo new_io_info;
  ret = extent_space_->allocate(std::string(), new_io_info);
  ASSERT_EQ(1, new_io_info.extent_id_.file_number);
  ASSERT_EQ(1, new_io_info.extent_id_.offset);

  for (int32_t i = 1; Status::kOk == ret && i <= 256; ++i) {
    extent_id.file_number = 0;
    extent_id.offset = i;
    ret = extent_space_->recycle(std::string(), extent_id);
    ASSERT_EQ(Status::kOk, ret);
  }

  for (int32_t i = 1; Status::kOk == ret && i <= 256; ++i) {
    extent_id.file_number = 1;
    extent_id.offset = i;
    ret = extent_space_->recycle(std::string(), extent_id);
    ASSERT_EQ(Status::kOk, ret);
  }

  for (int32_t i = 1; Status::kOk == ret && i <= 256; ++i) {
    ret = extent_space_->allocate(std::string(), io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, io_info.extent_id_.file_number);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }

  for (int32_t i = 1; Status::kOk == ret && i <= 256; ++i) {
    ret = extent_space_->allocate(std::string(), io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(1, io_info.extent_id_.file_number);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }

  for (int32_t i = 1; Status::kOk == ret && i <= 512; ++i) {
    ret = extent_space_->allocate(std::string(), io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(2, io_info.extent_id_.file_number);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }
}

TEST_P(ExtentSpaceTest, obj_extent_allocate_and_recycle)
{
  if (!use_obj_) {
    // skip the objstore extent test for file extent.
    return;
  }

  int ret = Status::kOk;
  CreateExtentSpaceArgs args;
  ExtentIOInfo io_info;
  ExtentId extent_id;

  // not init
  ret = extent_space_->allocate(std::string(), io_info);
  ASSERT_EQ(Status::kNotInit, ret);

  // success to allocate
  build_extent_space_args(args);
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  // TODO(Zhao Dongsheng) : The ExtentId.offset_ should start from 1 in future.
  for (int64_t i = 0; i <= 5120; ++i) {
    ret = extent_space_->allocate(std::string(), io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_LT(io_info.extent_id_.file_number, 0);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }

  for (int64_t i = 1; i <= 5120; ++i) {
    ret = extent_space_->allocate(std::string(), io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_LT(io_info.extent_id_.file_number, 0);
    ASSERT_EQ(i + 5120, io_info.extent_id_.offset);
  }

  int correct_fd = io_info.extent_id_.file_number;

  // recycle extent, which datafile is a positive value.
  extent_id.file_number = 2;
  extent_id.offset = 1;
  ret = extent_space_->recycle(std::string(), extent_id);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // recycle extent, which offset is exceed the datafile
  extent_id.file_number = correct_fd;
  extent_id.offset = 5120 * 3;
  ret = extent_space_->recycle(std::string(), extent_id);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success to recycle extent and then reallocate
  extent_id.file_number = correct_fd;
  extent_id.offset = 0;
  ret = extent_space_->recycle(std::string(), extent_id);
  ASSERT_EQ(Status::kOk, ret);
  ExtentIOInfo new_io_info;
  ret = extent_space_->allocate(std::string(), new_io_info);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_LT(new_io_info.extent_id_.file_number, 0);

  int32_t next_exent_id = new_io_info.extent_id_.offset;
  for (int32_t i = 1; i <= next_exent_id; ++i) {
    extent_id.offset = i;
    ret = extent_space_->recycle(std::string(), extent_id);
    ASSERT_EQ(Status::kOk, ret);
  }

  std::vector<DataFileStatistics> data_file_stats;
  ret = extent_space_->get_data_file_stats(data_file_stats);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(data_file_stats.size(), 1);
  ASSERT_EQ(data_file_stats[0].total_extent_count_, 0);
  ASSERT_EQ(data_file_stats[0].used_extent_count_, 0);
}

TEST_P(ExtentSpaceTest, recycle) {
  int ret = Status::kOk;
  CreateExtentSpaceArgs args;
  ExtentId extent_id;

  //not init
  ret = extent_space_->recycle(std::string(), extent_id);
  ASSERT_EQ(Status::kNotInit, ret);
}

} //namespace storage
} //namespace smartengine

int main(int argc, char **argv)
{
  smartengine::util::test::remove_dir(test_dir.c_str());
  smartengine::util::Env::Default()->CreateDir(test_dir);
  std::string log_path = smartengine::util::test::TmpDir() + "/extent_space_test.log";
  smartengine::logger::Logger::get_log().init(log_path.c_str(), smartengine::logger::DEBUG_LEVEL);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
