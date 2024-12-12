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
#include <fstream>
#include <filesystem>
#include <iostream>
#include <chrono>

#include "db/db_test_util.h"
#include "objstore.h"
#include "objstore/lease_lock.h"
#include "objstore/objstore_layout.h"
#include "storage/storage_common.h"
#include "util/file_name.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace smartengine {
namespace fs = std::filesystem;
using namespace common;
using namespace util;
namespace obj {

using ObjectMeta = ::objstore::ObjectMeta;
using Status = ::objstore::Status;

const std::string ALIYUN_PROVIDER = "aliyun";
const std::string S3_PROVIDER = "aws";
const std::string MINIO_PROVIDER = "minio";
const std::string R2_PROVIDER = "r2";
const std::string LOCAL_PROVIDER = "local";
const std::string ALIYUN_ENDPOINT = "oss-cn-hangzhou.aliyuncs.com";
const std::string MINIO_ENDPOINT = "http://127.0.0.1:9000";
const std::string R2_ENDPOINT = "https://040959acaa9a8f1fb88aa8c5c884d8a9.r2.cloudflarestorage.com";

class ObjstoreTest : public testing::TestWithParam<std::string>
{
public:
  void SetUp() override
  {
    env_ = Env::Default();
    env_options_ = EnvOptions();

    objstore::ObjectStore *obs = nullptr;

    provider_ = GetParam();

    if (provider_ == ALIYUN_PROVIDER) {
      // aliyun
      bucket_ = bucket_aliyun_;
      endpoint_ = new std::string_view(ALIYUN_ENDPOINT);
    } else if (provider_ == S3_PROVIDER) {
      // s3
      bucket_ = bucket_s3_;
    } else if (provider_ == R2_PROVIDER) {
      // Cloudflare R2
      bucket_ = bucket_r2_;
      region_ = "apac";
      endpoint_ = new std::string_view(R2_ENDPOINT);
    } else if (provider_ == MINIO_PROVIDER) {
      // minio
      bucket_ = bucket_minio_;
      region_ = "test";
      endpoint_ = new std::string_view(MINIO_ENDPOINT);
    } else if (provider_ == LOCAL_PROVIDER) {
      // local
      bucket_ = bucket_local_;
    }

    auto s = env_->InitObjectStore(provider_, region_, endpoint_, false, bucket_, "repo/branch", 0);
    ASSERT_OK(s);
    s = env_->GetObjectStore(obs);
    ASSERT_OK(s);
    obj_store_ = obs;

    Status ss;
    ss = obs->delete_bucket(bucket_);
    ASSERT_TRUE(ss.is_succ() || ss.error_code() == ::objstore::SE_NO_SUCH_BUCKET);
    if (provider_ == ALIYUN_PROVIDER) {
      // In Alibaba Cloud OSS, once a bucket is deleted, 
      // it cannot be immediately recreated with the same name for a period of time. 
      // Therefore, each time a bucket is created, it should have a unique name.
      bucket_ = bucket_aliyun_ + std::to_string(getCurrentTimeMillis());
    }
    ss = obs->create_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "set up:" << ss.error_code() << " " << ss.error_message() << std::endl;
    } else {
      std::cout << "set up: create bucket " << bucket_ << ", provider:" << provider_ << std::endl;
    }
    ASSERT_TRUE(ss.is_succ());
  }

  objstore::ObjectStore *create_objstore_client()
  {
    std::string obj_err_msg;
    ::objstore::init_objstore_provider(provider_);
    return ::objstore::create_object_store(provider_, region_, endpoint_, false, obj_err_msg);
  }

  void release_objstore_client(objstore::ObjectStore *client)
  {
    ::objstore::destroy_object_store(client);
    ::objstore::cleanup_objstore_provider(obj_store_);
  }

  void TearDown() override
  {
    Status ss;
    ss = obj_store_->delete_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "tear down:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    ASSERT_TRUE(ss.is_succ());

    env_->DestroyObjectStore();
    delete endpoint_;
  }

  Status create_bucket()
  {
    if (provider_ == ALIYUN_PROVIDER) {
      bucket_ = bucket_aliyun_ + std::to_string(getCurrentTimeMillis());
    }
    Status ss = obj_store_->create_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "create bucket:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status create_same_bucket_for_aliyun()
  {
    Status ss = obj_store_->create_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "create bucket:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status delete_bucket()
  {
    Status ss = obj_store_->delete_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "delete bucket:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status put_object(const std::string &key, const std::string &data, bool forbid_overwrite = false)
  {
    Status ss = obj_store_->put_object(bucket_, key, data, forbid_overwrite);
    if (!ss.is_succ()) {
      std::cout << "put object:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status get_object(const std::string &key, std::string &data, bool quiet = false)
  {
    Status ss = obj_store_->get_object(bucket_, key, data);
    if (!ss.is_succ() && !quiet) {
      std::cout << "get object:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status get_object_range(const std::string &key, std::string &data, size_t off, size_t len)
  {
    Status ss = obj_store_->get_object(bucket_, key, off, len, data);
    if (!ss.is_succ()) {
      std::cout << "get object range:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status delete_object(const std::string &key)
  {
    Status ss = obj_store_->delete_object(bucket_, key);
    if (!ss.is_succ()) {
      std::cout << "delete object:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status delete_objects(const std::vector<std::string_view> &keys)
  {
    Status ss = obj_store_->delete_objects(bucket_, keys);
    if (!ss.is_succ()) {
      std::cout << "delete objects:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status delete_directory(const std::string &prefix)
  {
    Status ss = obj_store_->delete_directory(bucket_, prefix);
    if (!ss.is_succ()) {
      std::cout << "delete directory:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss; 
  }

  Status list_object(const std::string &prefix,
                               bool recursive,
                               std::string &start_after,
                               bool &finished,
                               std::vector<::objstore::ObjectMeta> &objects)
  {
    Status ss = obj_store_->list_object(bucket_, prefix, recursive, start_after, finished, objects);
    if (!ss.is_succ()) {
      std::cout << "list object: " << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status put_object_from_file(const std::string &key, const std::string &data_file_path)
  {
    Status ss = obj_store_->put_object_from_file(bucket_, key, data_file_path);
    if (!ss.is_succ()) {
      std::cout << "put object from file: " << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status get_object_to_file(const std::string &key, const std::string &output_file_path)
  {
    Status ss = obj_store_->get_object_to_file(bucket_, key, output_file_path);
    if (!ss.is_succ()) {
      std::cout << "get object to file: " << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status put_objects_from_dir(const std::string &src_dir, const std::string &dst_dir)
  {
    Status ss = obj_store_->put_objects_from_dir(src_dir, bucket_, dst_dir);
    if (!ss.is_succ()) {
      std::cout << "upload directory: " << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  Status get_objects_to_dir(const std::string &src_dir, const std::string &dst_dir)
  {
    Status ss = obj_store_->get_objects_to_dir(bucket_, src_dir, dst_dir);
    if (!ss.is_succ()) {
      std::cout << "download directory: " << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss; 
  }

  static int64_t getCurrentTimeMillis() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  }

protected:
  Env *env_;
  EnvOptions env_options_;
  objstore::ObjectStore *obj_store_;

  std::string provider_;
  std::string bucket_;
  std::string bucket_local_ = "local-test";
  std::string bucket_s3_ = "wesql-s3-ut-test";
  std::string bucket_r2_ = "wesql-r2-ut-test";
  std::string bucket_minio_ = "wesql-minio-ut-test";
  std::string bucket_aliyun_ = "wesql-aliyun-ut-test";
  std::string region_ = "cn-northwest-1";
  std::string_view *endpoint_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(cloudProviders,
                        ObjstoreTest,
                        testing::Values( // clang-format off
                            // "aws"
                            // "r2"
                            // "minio"
                            // "aliyun"
                            "local"
                            )); // clang-format on

TEST_P(ObjstoreTest, reinitObjStoreApi)
{
  objstore::ObjectStore *obs = nullptr;

  ::objstore::init_objstore_provider(provider_);
  ::objstore::init_objstore_provider(provider_);
  ::objstore::init_objstore_provider(provider_);
  ::objstore::cleanup_objstore_provider(obj_store_);
  ::objstore::cleanup_objstore_provider(obj_store_);
  ::objstore::cleanup_objstore_provider(obj_store_);

  env_->GetObjectStore(obs);
  ASSERT_TRUE(obs != nullptr);

  ::objstore::cleanup_objstore_provider(obj_store_);
  ::objstore::init_objstore_provider(provider_);

  env_->DestroyObjectStore();

  env_->GetObjectStore(obs);
  ASSERT_TRUE(obs == nullptr);

  auto s = env_->InitObjectStore(provider_, region_, endpoint_, false, bucket_, "repo/branch", 0);
  ASSERT_OK(s);
  s = env_->GetObjectStore(obs);
  ASSERT_OK(s);
  obj_store_ = obs;
}

TEST_P(ObjstoreTest, noBucket)
{
  Status ss = delete_bucket();
  ASSERT_TRUE(ss.is_succ());
  std::string data;

  ss = get_object("x", data);
  if (provider_ == LOCAL_PROVIDER) {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_NO_SUCH_KEY);
  } else {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_NO_SUCH_BUCKET);
  }

  ss = put_object("x", "x");
  if (provider_ == LOCAL_PROVIDER) {
    // local object store will create the bucket automatically
    ASSERT_TRUE(ss.is_succ());
  } else {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_NO_SUCH_BUCKET);
  }

  ss = delete_object("x");
  if (provider_ == LOCAL_PROVIDER) {
    ASSERT_TRUE(ss.is_succ());
  } else {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_NO_SUCH_BUCKET);
  }

  bool finished = false;
  std::string start_after;
  std::vector<::objstore::ObjectMeta> objects;
  ss = list_object("x", true, start_after, finished, objects);
  if (provider_ == LOCAL_PROVIDER) {
    ASSERT_TRUE(ss.is_succ());
  } else {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_NO_SUCH_BUCKET);
  }

  ss = create_bucket();
  ASSERT_TRUE(ss.is_succ());
}

TEST_P(ObjstoreTest, createBucket)
{
  Status ss = delete_bucket();
  ASSERT_TRUE(ss.is_succ());
  std::string data;

  ss = create_bucket();
  ASSERT_TRUE(ss.is_succ());

  if (objstore::ObjectStore::use_s3_sdk(provider_)) {
    ss = create_bucket();
    ASSERT_EQ(ss.error_code(), ::objstore::SE_BUCKET_ALREADY_OWNED_BY_YOU);
  } else if (provider_ == ALIYUN_PROVIDER) {
    ss = create_same_bucket_for_aliyun();
    ASSERT_EQ(ss.error_code(), ::objstore::SE_BUCKET_ALREADY_EXISTS);
  }

  objstore::ObjectStore *new_client = create_objstore_client();
  ASSERT_TRUE(new_client != nullptr);

  ss = new_client->create_bucket(bucket_);
  if (objstore::ObjectStore::use_s3_sdk(provider_)) {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_BUCKET_ALREADY_OWNED_BY_YOU);
  } else if (provider_ == ALIYUN_PROVIDER) {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_BUCKET_ALREADY_EXISTS);
  }

  release_objstore_client(new_client);
}

TEST_P(ObjstoreTest, operateObject)
{
  std::string key = "test_put_object";
  std::string raw_data = "test_put_object_data";
  std::string zero_len_key = "zero_len_key";
  std::string illegal_key = "/illegal_key";
  std::string data;
  ::objstore::Status ss = put_object(key, raw_data);
  ASSERT_TRUE(ss.is_succ());

  ss = put_object(illegal_key, raw_data);
  // aliyun oss does not support key with '/' at the beginning
  ASSERT_EQ(ss.error_code(), ::objstore::SE_INVALID);

  ss = get_object(key, data);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, data);

  ss = get_object_range(key, data, 1, 3);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, "est");

  ss = get_object_range(key, data, 1, raw_data.size() - 1);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, "est_put_object_data");

  ss = get_object_range(key, data, 1, 300);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, "est_put_object_data");

  ss = get_object_range(key, data, 30, 10);
  ASSERT_TRUE(!ss.is_succ());

  ss = delete_object(key);
  ASSERT_TRUE(ss.is_succ());

  ss = get_object(key, data);
  ASSERT_EQ(ss.error_code(), ::objstore::SE_NO_SUCH_KEY);

  ss = put_object(zero_len_key, "");
  ASSERT_TRUE(ss.is_succ());

  ss = get_object(zero_len_key, data);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, "");

  ss = delete_object(zero_len_key);
  ASSERT_TRUE(ss.is_succ());
}

TEST_P(ObjstoreTest, listObject)
{
  Status ss;
  std::vector<::objstore::ObjectMeta> objects;
  bool finished = false;
  std::string start_after;
  // put 1001 objects with the same prefix
  for (int i = 0; i < 1001; i++) {
    std::string key = "test_list_object_" + std::to_string(i);
    std::string data = "test_list_object_data_" + std::to_string(i);
    ss = put_object(key, data);
    ASSERT_TRUE(ss.is_succ());
  }

  ss = list_object("test_list_object_", true, start_after, finished, objects);
  // s3 list object api returns 1000 objects at most
  if (provider_ == LOCAL_PROVIDER) {
    ASSERT_EQ(objects.size(), 1001);
    ASSERT_EQ(start_after, "");
    ASSERT_TRUE(finished);
  } else {
    ASSERT_EQ(objects.size(), 1000);
    ASSERT_FALSE(finished);
  }
  objects.clear();

  if (provider_ != LOCAL_PROVIDER) {
    ss = list_object("test_list_object_", true, start_after, finished, objects);
    // the `start_after` key("test_list_object_998") is not included.
    ASSERT_EQ(objects.size(), 1);
    ASSERT_TRUE(finished);
    ASSERT_EQ(start_after, "");
    objects.clear();
  }

  for (int i = 0; i < 1001; i++) {
    std::string key = "test_list_object_" + std::to_string(i);
    ss = delete_object(key);
    ASSERT_TRUE(ss.is_succ());
  }

  ss = put_object("empty_dir/", "");
  ASSERT_TRUE(ss.is_succ());
  ss = list_object("empty_dir", true, start_after, finished, objects);
  ASSERT_EQ(objects.size(), 1);
  ASSERT_EQ(objects[0].key, "empty_dir/");
  ss = delete_object("empty_dir/");
  ASSERT_TRUE(ss.is_succ());
}

TEST_P(ObjstoreTest, listObjectWithRecursiveOption)
{
  Status ss;
  std::vector<::objstore::ObjectMeta> objects;
  bool finished = false;
  std::string start_after;

  // NOTICE:
  // there are some different behaviors for `local` mode and `s3` and `aliyun` mode.

  // 1. for `local` mode, if a key is a directory, it should be ended with '/',
  // and a file name and a directory name should be distinguished. for example,
  // if we put a file with key "a", we can't put a directory with key "a/".
  // vice versa, if we put a directory with key "a/", we can't put a file with key "a".

  // for `s3` and `aliyun` mode, a directory key and a file key can have the same name,
  // for example, we can put a file with key "a", and then put a directory with key "a/".
  // vice versa, we can put a directory with key "a/", and then put a file with key "a".

  // 2. for `local` mode, when we delete a directory key, all the objects under the directory will be deleted.
  // for example, if we put a directory with key "a/", and then put a file with key "a/b", and then delete "a/" key,
  // the directory key "a/" and the file key "a/b" will be all deleted.

  // for `s3` and `aliyun` mode, when we delete a directory key, all the objects under the directory will not be
  // deleted. for example, if we put a directory with key "a/", and then put a file with key "a/b", and then delete "a/"
  // key, the directory key "a/" will be deleted, but the file key "a/b" will not be deleted.

  if (provider_ == LOCAL_PROVIDER || provider_ == MINIO_PROVIDER) {
    ss = put_object("a/", "");
    ASSERT_TRUE(ss.is_succ());
    ss = put_object("a/b/", "");
    ASSERT_TRUE(ss.is_succ());
    ss = put_object("a/b/c", "");
    ASSERT_TRUE(ss.is_succ());

    ss = list_object("a", true, start_after, finished, objects);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(objects.size(), 3);
    objects.clear();

    ss = list_object("a/", true, start_after, finished, objects);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(objects.size(), 3);
    objects.clear();

    ss = list_object("a", false, start_after, finished, objects);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(objects.size(), 1);
    ASSERT_EQ(objects[0].key, "a/");
    objects.clear();

    ss = list_object("a/", false, start_after, finished, objects);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(objects.size(), 2);
    ASSERT_EQ(objects[0].key, "a/");
    ASSERT_EQ(objects[1].key, "a/b/");
    objects.clear();

    ss = delete_object("a/b/c");
    ASSERT_TRUE(ss.is_succ());
    ss = delete_object("a/b/");
    ASSERT_TRUE(ss.is_succ());
    ss = delete_object("a/");
    ASSERT_TRUE(ss.is_succ());
  } else {
    ss = put_object("a", "");
    ASSERT_TRUE(ss.is_succ());
    ss = put_object("a/", "");
    ASSERT_TRUE(ss.is_succ());
    ss = put_object("a/b", "");
    ASSERT_TRUE(ss.is_succ());
    ss = put_object("a/b/", "");
    ASSERT_TRUE(ss.is_succ());
    ss = put_object("a/b/c", "");
    ASSERT_TRUE(ss.is_succ());

    ss = list_object("a", true, start_after, finished, objects);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(objects.size(), 5);
    objects.clear();

    ss = list_object("a/", true, start_after, finished, objects);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(objects.size(), 4);
    objects.clear();

    std::string expected_key;
    auto find_key = [&objects](const char *expected) {
      return std::find_if(objects.begin(), objects.end(), [expected](const auto &o) { return o.key == expected; }) != objects.end();
    };

    ss = list_object("a", false, start_after, finished, objects);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(objects.size(), 2);
    ASSERT_TRUE(find_key("a"));
    ASSERT_TRUE(find_key("a/"));
    objects.clear();

    ss = list_object("a/", false, start_after, finished, objects);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(objects.size(), 3);
    ASSERT_TRUE(find_key("a/"));
    ASSERT_TRUE(find_key("a/b"));
    ASSERT_TRUE(find_key("a/b/"));
    objects.clear();

    ss = delete_object("a");
    ASSERT_TRUE(ss.is_succ());
    ss = delete_object("a/");
    ASSERT_TRUE(ss.is_succ());
    ss = delete_object("a/b");
    ASSERT_TRUE(ss.is_succ());
    ss = delete_object("a/b/");
    ASSERT_TRUE(ss.is_succ());
    ss = delete_object("a/b/c");
    ASSERT_TRUE(ss.is_succ());
  }
}

TEST_P(ObjstoreTest, operateObjectFromFile)
{
  std::string key = "test_put_object_from_file";
  std::string data = "test_put_object_from_file_data";
  std::string data_file_path = "/tmp/test_put_object_from_file_data";
  FILE *fp = fopen(data_file_path.c_str(), "w");
  ASSERT_TRUE(fp != nullptr);
  fwrite(data.c_str(), 1, data.size(), fp);
  fclose(fp);

  Status ss = put_object_from_file(key, data_file_path);
  ASSERT_TRUE(ss.is_succ());

  std::string output_file_path = "/tmp/test_get_object_to_file_data";
  ss = get_object_to_file(key, output_file_path);
  ASSERT_TRUE(ss.is_succ());

  std::string output_data;
  fp = fopen(output_file_path.c_str(), "r");
  ASSERT_TRUE(fp != nullptr);
  char buf[1024];
  size_t read_size = fread(buf, 1, 1024, fp);
  output_data.append(buf, read_size);
  fclose(fp);
  ASSERT_EQ(data, output_data);

  ss = delete_object(key);
  ASSERT_TRUE(ss.is_succ());

  // remove the temp file
  remove(data_file_path.c_str());
  remove(output_file_path.c_str());
}

TEST_P(ObjstoreTest, noSuckKey) {
  std::string key = "test_put_object_key";
  std::string data = "test-data";
  Status ss = put_object(key, data);
  ASSERT_TRUE(ss.is_succ());

  ss = delete_object(key);
  ASSERT_TRUE(ss.is_succ());
  ss = get_object(key, data);
  ASSERT_TRUE(!ss.is_succ());
  ASSERT_EQ(ss.error_code(), ::objstore::SE_NO_SUCH_KEY);
}

std::string read_file_content(const fs::path& path) {
    std::ifstream file(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
}

void traverse_directory(const fs::path& dir, std::set<std::pair<fs::path, std::string>>& entries, const fs::path& base) {
  for (const auto& entry : fs::recursive_directory_iterator(dir)) {
    if (entry.is_regular_file()) {
      entries.emplace(fs::relative(entry.path(), base), read_file_content(entry.path()));
    } else if (entry.is_directory()) {
      entries.emplace(fs::relative(entry.path(), base), "dir");
    }
  }
}

bool compare_directories(const fs::path &dir1, const fs::path &dir2) {
  if (!fs::exists(dir1) || !fs::exists(dir2)) {
    return false;
  }

  if (!fs::is_directory(dir1) || !fs::is_directory(dir2)) {
    return false;
  }

  std::set<std::pair<fs::path, std::string>> dir1_entries;
  std::set<std::pair<fs::path, std::string>> dir2_entries; 
  traverse_directory(dir1, dir1_entries, dir1);
  traverse_directory(dir2, dir2_entries, dir2);

  return dir1_entries == dir2_entries;
}

TEST_P(ObjstoreTest, copyDir)
{
  // /tmp/test_upload_directory & /tmp/test_download_directory
  std::string upload_local_dir = "/tmp/////./../tmp/test_upload_directory//";
  std::string download_local_dir = "/tmp/.//.///test_download_directory";
  std::string remote_dir = "dir1/dir2/";
  std::vector<std::string> expect_keys;

  // upload 1200 keys totally
  fs::remove_all(upload_local_dir);
  ASSERT_TRUE(fs::create_directories(upload_local_dir));

  expect_keys.push_back(remote_dir);
  for (int i = 0; i < 5; i++) {
    std::string sub_dir = upload_local_dir + "/subdir" + std::to_string(i);
    ASSERT_TRUE(fs::create_directories(sub_dir));
    expect_keys.push_back(remote_dir + "subdir" + std::to_string(i) + "/");
    for (int j = 0; j < 10; j++) {
      std::string file_path = sub_dir + "/" + std::to_string(j);
      expect_keys.push_back(remote_dir + "subdir" + std::to_string(i) + "/" + std::to_string(j));
      std::ofstream file(file_path);
      ASSERT_TRUE(file.is_open());
      file << 1;
      file.close();
    }
  }
  std::string empty_dir = upload_local_dir + "/empty_dir";
  ASSERT_TRUE(fs::create_directories(empty_dir));
  expect_keys.push_back(remote_dir + "empty_dir/");

  Status ss = put_objects_from_dir(upload_local_dir, remote_dir);
  ASSERT_TRUE(ss.is_succ());

  std::vector<::objstore::ObjectMeta> objects;
  if (provider_ != LOCAL_PROVIDER) {
    // test if the key is correct
    std::string start_after;
    bool finished = false;
    while (!finished) {
      ss = list_object(remote_dir, true, start_after, finished, objects);
      ASSERT_TRUE(ss.is_succ());
    }
    ASSERT_EQ(expect_keys.size(), 57);
    ASSERT_EQ(objects.size(), 57);
    ASSERT_EQ(expect_keys.size(), objects.size());
    std::sort(expect_keys.begin(), expect_keys.end());
    std::sort(objects.begin(), objects.end(), [&](const ::objstore::ObjectMeta &a, ::objstore::ObjectMeta &b) { return a.key < b.key; });
    for (size_t i = 0; i < expect_keys.size(); i++) {
      ASSERT_EQ(expect_keys[i], objects[i].key);
    }
  }

  // download
  fs::remove_all(download_local_dir);

  ss = get_objects_to_dir(remote_dir, download_local_dir);
  ASSERT_TRUE(ss.is_succ());

  // same structure
  ASSERT_TRUE(compare_directories(upload_local_dir, download_local_dir));

  // clean the bucket
  for (const ::objstore::ObjectMeta &meta : objects) {
    ss = delete_object(meta.key);
    ASSERT_TRUE(ss.is_succ());
  }

  fs::remove_all(upload_local_dir);
  fs::remove_all(download_local_dir);
}

TEST_P(ObjstoreTest, deleteDir)
{
  std::string dir1 = "dir1";
  std::string dir2 = "dir2";
  std::string dir3 = "dir1dir3";
  std::string raw_data = "test_put_object_data";
  const int key_num = 100;
  Status ss;
  for (int i = 0; i < key_num; i++) {
    std::string key1 = dir1 + "/test_key_" + std::to_string(i + 1);
    std::string key2 = dir2 + "/test_key_" + std::to_string(i + 1);
    std::string key3 = dir3 + "/test_key_" + std::to_string(i + 1);
    ss = put_object(key1, raw_data);
    ASSERT_TRUE(ss.is_succ());
    ss = put_object(key2, raw_data);
    ASSERT_TRUE(ss.is_succ());
    ss = put_object(key3, raw_data);
    ASSERT_TRUE(ss.is_succ()); 
  }
  ss = delete_directory(dir1);
  ASSERT_TRUE(ss.is_succ());
  for (int i = 0; i < key_num; i++) {
    std::string key1 = dir1 + "/test_key_" + std::to_string(i + 1);
    std::string key2 = dir2 + "/test_key_" + std::to_string(i + 1);
    std::string key3 = dir3 + "/test_key_" + std::to_string(i + 1);

    std::string data;
    ss = get_object(key1, data);
    ASSERT_TRUE(!ss.is_succ());
    ASSERT_EQ(ss.error_code(), ::objstore::Errors::SE_NO_SUCH_KEY);

    ss = get_object(key2, data);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(data, raw_data);

    ss = get_object(key3, data);
    ASSERT_TRUE(ss.is_succ());
    ASSERT_EQ(data, raw_data);
  }

  ss = delete_directory(dir2);
  ASSERT_TRUE(ss.is_succ());
  ss = delete_directory(dir3);
  ASSERT_TRUE(ss.is_succ());
}

TEST_P(ObjstoreTest, CornerCase)
{
  std::vector<std::string_view> keys;
  // ========= case: delete 0 keys =========
  Status ss = delete_objects(keys);
  ASSERT_EQ(ss.error_code(), 0);

  // ========= case: delete keys contain empty string(key length = 0) =========
  keys.push_back("");
  ss = delete_objects(keys);
  if (provider_ == ALIYUN_PROVIDER) {
    ASSERT_TRUE(ss.error_message().find("MalformedXML") != std::string::npos);
    ASSERT_EQ(ss.error_code(), ::objstore::CLOUD_PROVIDER_UNRECOVERABLE_ERROR);
  } else if (provider_ == S3_PROVIDER) {
    ASSERT_TRUE(ss.error_message().find("UserKeyMustBeSpecified") != std::string::npos);
    ASSERT_EQ(ss.error_code(), ::objstore::CLOUD_PROVIDER_UNRECOVERABLE_ERROR);
  } else if (provider_ == MINIO_PROVIDER || provider_ == R2_PROVIDER) {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_SUCCESS);
  } else if (provider_ == LOCAL_PROVIDER) {
    ASSERT_EQ(ss.error_code(), ::objstore::SE_INVALID);
  }
  keys.clear();

  // ========= case: key length equal to 1024 should be invalid key =========
  std::vector<std::string> mkeys;
  std::string prefix;
  // generate a prefix with length 1019
  prefix.reserve(1019);
  for (int x = 0; x < 4; x++) {
    // the max file name length is 255 for Linux
    for (int i = 0; i < 253; i++) {
      prefix += "a";
    }
    prefix += "/";
  }
  prefix += "ooo";
  ASSERT_EQ(prefix.length(), 1019);
  std::string key_len_1024 = prefix + "aaaaa";
  ASSERT_EQ(key_len_1024.length(), 1024);
  // put object with key length 1024
  // for aliyun oss, the max key length allowed is 1023
  ss = put_object(key_len_1024, "data");
  ASSERT_EQ(ss.error_code(), ::objstore::SE_INVALID);

  // ========= case: delete more than 1000 keys with key length = 1023 =========
  const size_t prepend_len = 1023 - prefix.size();
  ASSERT_EQ(prepend_len, 4);
  // generate more than 1000 keys with length 1023
  for (int j = 0; j < 1010; j++) {
    std::string key = prefix;
    size_t my_prepend_len = prepend_len - std::to_string(j).length();
    if (my_prepend_len > 0) {
      for (size_t k = 0; k < my_prepend_len; k++) {
        key += "0";
      }
    }
    key += std::to_string(j);
    ASSERT_TRUE(key.length() == 1023);
    ss = put_object(key, "");
    ASSERT_TRUE(ss.is_succ());

    mkeys.push_back(std::move(key));
  }

  std::vector<std::string_view> mkeys_view(mkeys.begin(), mkeys.end());

  ss = delete_objects(mkeys_view);
  ASSERT_EQ(ss.error_code(), 0);

  // make sure all keys are deleted
  std::string data;
  for (const std::string &key : mkeys) {
    ss = get_object(key, data, true);
    ASSERT_EQ(ss.error_code(), ::objstore::SE_NO_SUCH_KEY);
  }
  keys.clear();
}

TEST_P(ObjstoreTest, forbidOverwrite)
{
  std::string key = "forbid_overwrite_key";

  Status ss = put_object(key, "data", true);
  ASSERT_TRUE(ss.is_succ());
  ss = delete_object(key);
  ASSERT_TRUE(ss.is_succ());

  ss = put_object(key, "data1");
  ASSERT_TRUE(ss.is_succ());

  ss = put_object(key, "data2", true);
  ASSERT_FALSE(ss.is_succ());
  ASSERT_EQ(ss.error_code(), ::objstore::Errors::SE_OBJECT_FORBID_OVERWRITE);
  std::cout << "err code:" << ss.error_code() << ", cloud err code:" << ss.cloud_provider_err_code()
            << ", err msg:" << ss.error_message() << std::endl;

  ss = delete_object(key);
  ASSERT_TRUE(ss.is_succ());
}

TEST_P(ObjstoreTest, is_first_level_sub_key)
{
  ASSERT_TRUE(::objstore::is_first_level_sub_key("a", ""));
  ASSERT_TRUE(::objstore::is_first_level_sub_key("a/", ""));
  ASSERT_FALSE(::objstore::is_first_level_sub_key("a/b", ""));

  ASSERT_TRUE(::objstore::is_first_level_sub_key("a", "a"));
  ASSERT_TRUE(::objstore::is_first_level_sub_key("ab", ""));
  ASSERT_TRUE(::objstore::is_first_level_sub_key("a/", "a"));
  ASSERT_TRUE(::objstore::is_first_level_sub_key("aa/", "a"));
  ASSERT_TRUE(::objstore::is_first_level_sub_key("ab/", ""));
  ASSERT_FALSE(::objstore::is_first_level_sub_key("a/b", "a"));
  ASSERT_FALSE(::objstore::is_first_level_sub_key("a/b/", "a"));
  ASSERT_FALSE(::objstore::is_first_level_sub_key("a/b/c", "a"));

  ASSERT_FALSE(::objstore::is_first_level_sub_key("a", "a/"));
  ASSERT_TRUE(::objstore::is_first_level_sub_key("a/", "a/"));
  ASSERT_TRUE(::objstore::is_first_level_sub_key("a/b", "a/"));
  ASSERT_TRUE(::objstore::is_first_level_sub_key("a/b/", "a/"));
  ASSERT_FALSE(::objstore::is_first_level_sub_key("a/b/c", "a/"));
}

TEST_P(ObjstoreTest, lease_lock)
{
  std::string err_msg;
  std::string important_msg;
  bool need_abort = false;
  std::string start_after;
  bool finished = false;
  std::vector<::objstore::ObjectMeta> objects;
  std::string last_lock_version;

  std::string cluster_objstore_id = "repo/branch";
  const std::string lease_lock_key = util::get_lease_lock_key(cluster_objstore_id);
  const std::string lock_version_file_prefix = util::get_lease_lock_version_file_prefix(cluster_objstore_id);

  int ret = delete_object(lease_lock_key).error_code();
  ASSERT_TRUE(ret == 0 || ret == ::objstore::SE_NO_SUCH_KEY);

  objstore::LeaseLockSettings lease_lock_settings;
  lease_lock_settings.lease_timeout = std::chrono::milliseconds(2000);
  lease_lock_settings.renewal_timeout = std::chrono::milliseconds(1500);
  lease_lock_settings.renewal_interval = std::chrono::milliseconds(750);

  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
  auto new_lease_time = now + lease_lock_settings.lease_timeout;

  // objstore is not initialized
  ret = objstore::renewal_single_data_node_lease_lock(nullptr, bucket_, cluster_objstore_id, lease_lock_settings, new_lease_time, err_msg);
  ASSERT_EQ(ret, ::objstore::SE_INVALID);
  // not lease lock owner, can not renew lease
  ret = objstore::renewal_single_data_node_lease_lock(obj_store_,
                                                      bucket_,
                                                      cluster_objstore_id,
                                                      lease_lock_settings,
                                                      new_lease_time,
                                                      err_msg);
  ASSERT_EQ(ret, ::objstore::SE_INVALID);

  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(objstore::is_lease_lock_owner_node());

  start_after = "";
  objects.clear();
  ret = list_object(lock_version_file_prefix, false, start_after, finished, objects).error_code();
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(objects.size(), 1);
  last_lock_version = objects[0].key;
  std::cout << "initial lock version: " << last_lock_version << std::endl;

  // can renew lease lock now
  ret = objstore::renewal_single_data_node_lease_lock(obj_store_,
                                                      bucket_,
                                                      cluster_objstore_id,
                                                      lease_lock_settings,
                                                      new_lease_time,
                                                      err_msg);
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(objstore::is_lease_lock_owner_node());

  // renew lease lock again
  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(objstore::is_lease_lock_owner_node());

  // no new lock version file should be created if the lease lock is renewed in renewal timeout
  start_after = "";
  objects.clear();
  ret = list_object(lock_version_file_prefix, false, start_after, finished, objects).error_code();
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(objects.size(), 1);
  ASSERT_EQ(last_lock_version, objects[0].key);

  // emulate the case that another node try to get the lease lock
  objstore::TEST_unset_lease_lock_owner();
  ASSERT_FALSE(objstore::is_lease_lock_owner_node());
  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  ASSERT_EQ(ret, ::objstore::SE_OHTER_DATA_NODE_MAYBE_RUNNING);
  // wait for a lease timeout when the lease lock is owned by another node
  std::this_thread::sleep_for(lease_lock_settings.lease_timeout + std::chrono::milliseconds(100));
  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  ASSERT_EQ(ret, 0);

  // a new lock version file should be created if the lease lock is renewed after a lease timeout
  start_after = "";
  objects.clear();
  ret = list_object(lock_version_file_prefix, false, start_after, finished, objects).error_code();
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(objects.size(), 1);
  ASSERT_NE(last_lock_version, objects[0].key);
  last_lock_version = objects[0].key;
  std::cout << "renew the lease lock after a lease timeout, new lock version created: " << last_lock_version
            << std::endl;

  // wait for a renewal timeout
  std::this_thread::sleep_for(lease_lock_settings.renewal_timeout + std::chrono::milliseconds(100));
  // should renew failed if the lease lock is not renewed in renewal timeout
  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  ASSERT_EQ(ret, ::objstore::SE_LEASE_LOCK_RENEWAL_TIMEOUT);

  std::this_thread::sleep_for(lease_lock_settings.lease_timeout -
                              lease_lock_settings.renewal_timeout);
  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  ASSERT_EQ(ret, 0);
  // test the case that a node was lease lock owner, but fail to renew, should abort this node
  SyncPoint::GetInstance()->SetCallBack("objstore::try_single_data_node_lease_lock", [&](void *arg) {
    int *ret = static_cast<int *>(arg);
    *ret = ::objstore::SE_OHTER_DATA_NODE_MAYBE_RUNNING;
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(objstore::is_lease_lock_owner_node());
  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  // std::cout << "err code:" << ret << ", err msg:" << err_msg << ", important msg:" << important_msg << std::endl;
  ASSERT_EQ(ret, ::objstore::SE_OHTER_DATA_NODE_MAYBE_RUNNING);
  ASSERT_FALSE(objstore::is_lease_lock_owner_node());
  ASSERT_TRUE(need_abort);
  SyncPoint::GetInstance()->DisableProcessing();

  // wait for a lease timeout when the lease lock is owned by me
  std::this_thread::sleep_for(lease_lock_settings.lease_timeout + std::chrono::milliseconds(100));
  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  ASSERT_EQ(ret, 0);
  // a new lock version file should be created if get lease lock success after a lease lock timeout
  start_after = "";
  objects.clear();
  ret = list_object(lock_version_file_prefix, false, start_after, finished, objects).error_code();
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(objects.size(), 1);
  ASSERT_NE(last_lock_version, objects[0].key);
  last_lock_version = objects[0].key;
  std::cout << "renew the lease lock after a lease timeout, new lock version created: " << last_lock_version
            << std::endl;

  // use a small lease time to test the case that the lease lock is expired
  now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
  new_lease_time = now + std::chrono::milliseconds(100);
  std::string new_lease_time_str = std::to_string(new_lease_time.count());
  ret = put_object(lease_lock_key, new_lease_time_str).error_code();
  ASSERT_EQ(ret, 0);
  // will for the lease lock expired
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // emulate the case that the lease lock is owned by another node
  objstore::TEST_unset_lease_lock_owner();
  new_lease_time = now + lease_lock_settings.lease_timeout;
  new_lease_time_str = std::to_string(new_lease_time.count());
  ret = put_object(lease_lock_key, new_lease_time_str).error_code();
  ASSERT_EQ(ret, 0);
  // the lease time is valid and the lease lock is owned by another node
  ret = objstore::try_single_data_node_lease_lock(obj_store_,
                                                  bucket_,
                                                  cluster_objstore_id,
                                                  lease_lock_settings,
                                                  err_msg,
                                                  important_msg,
                                                  need_abort);
  // std::cout << "err code:" << ret << ", err msg:" << err_msg << ", important msg:" << important_msg << std::endl;
  ASSERT_EQ(ret, ::objstore::SE_OHTER_DATA_NODE_MAYBE_RUNNING);
  // no new lock version file should be created if the lease lock is not renewed by me
  start_after = "";
  objects.clear();
  ret = list_object(lock_version_file_prefix, false, start_after, finished, objects).error_code();
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(objects.size(), 1);
  ASSERT_EQ(last_lock_version, objects[0].key);
  std::cout << "no new lock version created when fail to renew lease lock, last lock version is: " << last_lock_version
            << std::endl;

  // cleanup bucket
  ret = delete_object(lease_lock_key).error_code();
  ASSERT_TRUE(ret == 0);
  ret = delete_object(last_lock_version).error_code();
  ASSERT_TRUE(ret == 0);
}

} // namespace obj

} // namespace smartengine

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
