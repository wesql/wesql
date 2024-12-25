#include "objstore/objstore_allocator.h"
#include <thread>
#include <vector>
#include "util/status.h"
#include "util/testharness.h"
#include "gtest/gtest.h"

static const std::string test_dir = smartengine::util::test::TmpDir() + "/objstore_allocator_test";

namespace smartengine
{
using namespace common;

namespace objstore
{

static const std::string S3_PROVIDER = "aws";
static const std::string S3_REGION = "cn-northwest-1";

static const std::string ALIYUN_PROVIDER = "aliyun";
static const std::string ALIYUN_ENDPOINT = "oss-cn-hangzhou.aliyuncs.com";

static const std::string MINIO_PROVIDER = "minio";
static const std::string MINIO_ENDPOINT = "http://127.0.0.1:9000";

static const std::string R2_PROVIDER = "r2";
static const std::string R2_ENDPOINT = "https://040959acaa9a8f1fb88aa8c5c884d8a9.r2.cloudflarestorage.com";

static const std::string LOCAL_PROVIDER = "local";

static const std::string bucket = "wesql-objstore-allocator-unittest";
static const std::string key = "int-allocator";

class ObjStoreIntAllocatorTest : public testing::TestWithParam<std::string>
{
public:
  ObjStoreIntAllocatorTest()
      : env_(nullptr),
        provider_(),
        region_(),
        endpoint_(nullptr),
        bucket_(),
        key_(),
        obj_store_(nullptr)
  {}

  void SetUp() override
  {
    int ret = Status::kOk;
    ::objstore::Status object_status;
    env_ = util::Env::Default();
    provider_ = GetParam(); // Use parameterized provider
    bucket_ = bucket;
    key_ = key;

    if (provider_ == S3_PROVIDER) {
      region_ = S3_REGION;
    } else if (provider_ == MINIO_PROVIDER) {
      region_ = "test";
      endpoint_ = new std::string_view(MINIO_ENDPOINT);
    } else if (provider_ == R2_PROVIDER) {
      region_ = "apac";
      endpoint_ = new std::string_view(R2_ENDPOINT);
    } else if (provider_ == ALIYUN_PROVIDER) {
      endpoint_ = new std::string_view(ALIYUN_ENDPOINT);
    } else if (provider_ == LOCAL_PROVIDER) {
      // do nothing
    } else {
      ASSERT_TRUE(false);
    }
  
    ret = env_->InitObjectStore(provider_, region_, endpoint_, false, bucket_, "repo/branch", 0).code();
    ASSERT_EQ(Status::kOk, ret);
    ret = env_->GetObjectStore(obj_store_).code();
    ASSERT_EQ(Status::kOk, ret);

    object_status = obj_store_->delete_bucket(bucket_);
    ASSERT_TRUE(object_status.is_succ() || ::objstore::SE_NO_SUCH_BUCKET == object_status.error_code());

    object_status = obj_store_->create_bucket(bucket_);
    ASSERT_TRUE(object_status.is_succ());
  }

  void TearDown() override
  {
    ::objstore::Status object_status;

    if (nullptr != obj_store_) {
      object_status = obj_store_->delete_bucket(bucket_);
      ASSERT_TRUE(object_status.is_succ());

      if (nullptr != endpoint_) {
        delete endpoint_;
      }

      env_->DestroyObjectStore();
    }
  }

protected:
  util::Env *env_;
  std::string provider_;
  std::string region_;
  std::string_view *endpoint_;
  std::string bucket_;
  std::string key_;
  ::objstore::ObjectStore *obj_store_;
};

INSTANTIATE_TEST_CASE_P(cloudProviders,
                        ObjStoreIntAllocatorTest,
                        testing::Values(
                            "aws"
                            // "r2",
                            // "minio",
                            // "aliyun",
                            // "local"
                            ));

TEST_P(ObjStoreIntAllocatorTest, create)
{
  int ret = Status::kOk;
  ObjStoreIntAllocator allocator;
  int64_t first_value = 100;

  // invalid argument
  ret = allocator.create(nullptr, bucket_, key_, first_value);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.create(obj_store_, "", key_, first_value);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.create(obj_store_, bucket_, "", first_value);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.create(obj_store_, bucket_, key_, -1);
  EXPECT_EQ(Status::kInvalidArgument, ret);

  // create succeed
  ret = allocator.create(obj_store_, bucket_, key_, first_value);
  EXPECT_EQ(Status::kOk, ret);

  // create again should fail due to forbid_overwrite
  ret = allocator.create(obj_store_, bucket_, key_, first_value);
  EXPECT_EQ(Status::kObjStoreError, ret);

  // remove the object
  ret = allocator.remove(obj_store_, bucket_, key_);
  EXPECT_EQ(Status::kOk, ret);
}

TEST_P(ObjStoreIntAllocatorTest, remove)
{
  int ret = Status::kOk;
  ObjStoreIntAllocator allocator;

  // invalid argument
  ret = allocator.remove(nullptr, bucket_, key_);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.remove(obj_store_, "", key_);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.remove(obj_store_, bucket_, "");
  EXPECT_EQ(Status::kInvalidArgument, ret);

  // create succeed
  ret = allocator.create(obj_store_, bucket_, key_, 100);
  EXPECT_EQ(Status::kOk, ret);
  // remove succeed
  ret = allocator.remove(obj_store_, bucket_, key_);
  EXPECT_EQ(Status::kOk, ret);

  // remove again should still success, regardless the object does not exist
  ret = allocator.remove(obj_store_, bucket_, key_);
  EXPECT_EQ(Status::kOk, ret);
}

TEST_P(ObjStoreIntAllocatorTest, init)
{
  int ret = Status::kOk;
  int64_t step = 100;
  ObjStoreIntAllocator allocator;
  
  // invalid argument
  ret = allocator.init(nullptr, bucket_, key_, step);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.init(obj_store_, "", key_, step);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.init(obj_store_, bucket_, "", step);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.init(obj_store_, bucket_, key_, 0);
  EXPECT_EQ(Status::kInvalidArgument, ret);
  ret = allocator.init(obj_store_, bucket_, key_, -1);
  EXPECT_EQ(Status::kInvalidArgument, ret);

  // object does not exist
  ret = allocator.init(obj_store_, bucket_, "not-exist", step);
  EXPECT_EQ(Status::kEntryNotExist, ret);

  // create succeed
  ret = allocator.create(obj_store_, bucket_, key_, 0);
  EXPECT_EQ(Status::kOk, ret);
  // init succeed
  ret = allocator.init(obj_store_, bucket_, key_, step);
  EXPECT_EQ(Status::kOk, ret);

  // init twice
  ret = allocator.init(obj_store_, bucket_, key_, step);
  EXPECT_EQ(Status::kInitTwice, ret);

  // remove the object
  ret = allocator.remove(obj_store_, bucket_, key_);
  EXPECT_EQ(Status::kOk, ret);
}

TEST_P(ObjStoreIntAllocatorTest, alloc)
{
  int ret = Status::kOk;
  int64_t step = 100;
  ObjStoreIntAllocator allocator;
  int64_t start = 0;
  int64_t end = 0;

  // create succeed
  ret = allocator.create(obj_store_, bucket_, key_, 0);
  EXPECT_EQ(Status::kOk, ret);
  // init succeed
  ret = allocator.init(obj_store_, bucket_, key_, step);
  EXPECT_EQ(Status::kOk, ret);

  // alloc succeed
  ret = allocator.alloc(start, end);
  EXPECT_EQ(Status::kOk, ret);
  EXPECT_EQ(0, start);
  EXPECT_EQ(step, end);

  // alloc succeed
  ret = allocator.alloc(start, end);
  EXPECT_EQ(Status::kOk, ret);
  EXPECT_EQ(step, start);
  EXPECT_EQ(2 * step, end);

  // remove the object
  ret = allocator.remove(obj_store_, bucket_, key_);
  EXPECT_EQ(Status::kOk, ret);
}

TEST_P(ObjStoreIntAllocatorTest, parallel_alloc)
{
  int ret = Status::kOk;
  const int64_t step = 100;
  const int64_t thread_count = 10;
  const int64_t alloc_times = 10;
  std::atomic<int64_t> newest_end(0);
  ObjStoreIntAllocator allocator;

  // create succeed
  ret = allocator.create(obj_store_, bucket_, key_, 0);
  EXPECT_EQ(Status::kOk, ret);
  // init succeed
  ret = allocator.init(obj_store_, bucket_, key_, step);
  EXPECT_EQ(Status::kOk, ret);

  auto alloc_func = [&allocator, &newest_end]() {
    int64_t start = 0;
    int64_t end = 0;
    for (int i = 0; i < alloc_times; ++i) {
      int ret = allocator.alloc(start, end);
      EXPECT_EQ(Status::kOk, ret);
      EXPECT_EQ(start + step, end);
    }

    int64_t current_end = newest_end.load();
    while (end > current_end && !newest_end.compare_exchange_weak(current_end, end)) {
      current_end = newest_end.load();
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(alloc_func);
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(thread_count * alloc_times * step, newest_end.load());

  // remove the object
  ret = allocator.remove(obj_store_, bucket_, key_);
}

TEST_P(ObjStoreIntAllocatorTest, get_current_value)
{
  int ret = Status::kOk;
  int64_t step = 100;
  ObjStoreIntAllocator allocator;
  int64_t value = 0;

  // create succeed
  ret = allocator.create(obj_store_, bucket_, key_, 0);
  EXPECT_EQ(Status::kOk, ret);
  // init succeed
  ret = allocator.init(obj_store_, bucket_, key_, step);
  EXPECT_EQ(Status::kOk, ret);

  // get current value succeed
  ret = allocator.get_current_value(value);
  EXPECT_EQ(Status::kOk, ret);
  EXPECT_EQ(0, value);

  // alloc succeed
  int64_t start = 0;
  int64_t end = 0;
  ret = allocator.alloc(start, end);
  EXPECT_EQ(Status::kOk, ret);

  // get current value succeed
  ret = allocator.get_current_value(value);
  EXPECT_EQ(Status::kOk, ret);
  EXPECT_EQ(step, value);

  // remove the object
  ret = allocator.remove(obj_store_, bucket_, key_);
  EXPECT_EQ(Status::kOk, ret);
}

} // namespace objstore
} // namespace smartengine

int main(int argc, char **argv)
{
  smartengine::util::test::remove_dir(test_dir.c_str());
  smartengine::util::Env::Default()->CreateDir(test_dir);
  std::string log_path = smartengine::util::test::TmpDir() + "/objstore_allocator_test.log";
  smartengine::logger::Logger::get_log().init(log_path.c_str(), smartengine::logger::DEBUG_LEVEL);
  testing::InitGoogleTest(&argc, argv);
  // TODO (Zhao Dongsheng): This unittest can pass only when the object store is aws now.
  // Temprarilly disable it to avoid the failure in CI.After the local object store is supported,
  // we can enable it.
  //return RUN_ALL_TESTS();
}