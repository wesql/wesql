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

#include "objstore.h"
#include "storage/storage_common.h"
#include "util/file_name.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace smartengine {
using namespace common;
using namespace util;
namespace obj {
class ObjstoreBench
{
public:
  void SetUp() 
  {
    env_ = Env::Default();
    env_options_ = EnvOptions();

    objstore::ObjectStore *obs = nullptr;

    std::string cluster_objstore_id = "repo/branch";
    auto s = env_->InitObjectStore(provider_, region_, endpoint_, false, bucket_, cluster_objstore_id, 0);
    if (!s.ok()) {
      std::cout << "init object store failed:" << s.ToString() << std::endl;
      std::abort();
    }
    s = env_->GetObjectStore(obs);
    if (!s.ok()) {
      std::cout << "get object store failed:" << s.ToString() << std::endl;
      // std::abort();
    }
    obj_store_ = obs;

    objstore::Status ss;
    ss = obs->delete_bucket(bucket_);
    if (!ss.is_succ() && ss.error_code() != objstore::SE_NO_SUCH_BUCKET) {
      std::cout << "delete bucket failed:" << ss.error_code() << ", cloud err code:" << ss.cloud_provider_err_code() << ", err msg:" << ss.error_message() << std::endl;
      // std::abort();
    }
    ss = obs->create_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "create bucket failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      // std::abort();
    }
  }

  objstore::ObjectStore *create_objstore_client()
  {
    std::string obj_err_msg;
    objstore::init_objstore_provider(provider_);
    return objstore::create_object_store(provider_, region_, nullptr, false, obj_err_msg);
  }

  void release_objstore_client(objstore::ObjectStore *client)
  {
    objstore::destroy_object_store(client);
    objstore::cleanup_objstore_provider(obj_store_);
  }
  void TearDown() 
  {
    objstore::Status ss;
    ss = obj_store_->delete_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "tear down: delete bucket faile:" << ss.error_code() << " " << ss.error_message() << std::endl;
      // std::abort();
    }
    env_->DestroyObjectStore();
  }
  objstore::Status create_bucket()
  {
    objstore::Status ss = obj_store_->create_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "create bucket failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }
  objstore::Status delete_bucket()
  {
    objstore::Status ss = obj_store_->delete_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "delete bucket failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }
  objstore::Status put_object(const std::string &key, const std::string &data)
  {
    objstore::Status ss = obj_store_->put_object(bucket_, key, data);
    if (!ss.is_succ()) {
      std::cout << "put object failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }
  objstore::Status get_object(const std::string &key, std::string &data)
  {
    objstore::Status ss = obj_store_->get_object(bucket_, key, data);
    if (!ss.is_succ()) {
      std::cout << "get object failed:" << ss.error_code() << ", cloud err code:" << ss.cloud_provider_err_code() << ", err msg:" << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }
  objstore::Status get_object_range(const std::string &key, std::string &data, size_t off, size_t len)
  {
    objstore::Status ss = obj_store_->get_object(bucket_, key, off, len, data);
    if (!ss.is_succ()) {
      std::cout << "get object range failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }
  objstore::Status delete_object(const std::string &key)
  {
    objstore::Status ss = obj_store_->delete_object(bucket_, key);
    if (!ss.is_succ()) {
      std::cout << "delete object failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }
  objstore::Status list_object(const std::string &prefix,
                               bool recursive,
                               std::string &start_after,
                               bool &finished,
                               std::vector<objstore::ObjectMeta> &objects)
  {
    objstore::Status ss = obj_store_->list_object(bucket_, prefix, recursive, start_after, finished, objects);
    if (!ss.is_succ()) {
      std::cout << "list object failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }
  objstore::Status put_object_from_file(const std::string &key, const std::string &data_file_path)
  {
    objstore::Status ss = obj_store_->put_object_from_file(bucket_, key, data_file_path);
    if (!ss.is_succ()) {
      std::cout << "put object from file failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }
  objstore::Status get_object_to_file(const std::string &key, const std::string &output_file_path)
  {
    objstore::Status ss = obj_store_->get_object_to_file(bucket_, key, output_file_path);
    if (!ss.is_succ()) {
      std::cout << "get object to file failed:" << ss.error_code() << " " << ss.error_message() << std::endl;
      std::abort();
    }
    return ss;
  }

protected:
  Env *env_;
  EnvOptions env_options_;
  objstore::ObjectStore *obj_store_;

// for aws
  // std::string provider_ = "aws";
  // std::string bucket_ = "ljc";
  // std::string region_ = "cn-northwest-1";
  // std::string_view* endpoint_ = nullptr;

// for aliyun
  std::string provider_ = "aliyun";
  std::string bucket_ = "wesql-mtr-tests";
  std::string region_ = "cn-hangzhou";
  std::string_view aliyun_endpoint_ = "oss-cn-hangzhou-internal.aliyuncs.com";
  std::string_view* endpoint_ = &aliyun_endpoint_;
};

} // namespace obj

} // namespace smartengine

const long obj_size = 2; // 2MB


int concurrent = 16;
long samples_count = 5000;

void prepare_data(char* data, int sample_count, smartengine::obj::ObjstoreBench &objstore_bench)
{
  long each_sample_count = sample_count / concurrent;
  std::vector<std::thread> threads;
  for (int t = 0; t < concurrent; t++) {
    std::thread th([&objstore_bench, t, &data, each_sample_count] {
      std::cout << "put object thread " << t << " start" << std::endl;
      std::string prefix = "t"+std::to_string(t)+"-";
      for (int i = 0; i < each_sample_count; i++) {
        std::string key = prefix + std::to_string(i);
        objstore_bench.put_object(key, data);
      }
    });
    threads.push_back(std::move(th));
  }

  for (auto &th : threads) {
    th.join();
  }
}

void test_put(char* data, int sample_count, smartengine::obj::ObjstoreBench &objstore_bench)
{
  long each_sample_count = sample_count / concurrent;
  std::vector<std::thread> threads;
  std::vector<int64_t> durations;
  durations.assign(concurrent, 0);

  std::cout << "============begin put object test================" << std::endl;
  long total_size = sample_count/concurrent*concurrent*obj_size; // how many MB
  std::cout << "total size: " << total_size << std::endl;

  for (int t = 0; t < concurrent; t++) {
    std::thread th([&objstore_bench, &durations, t, &data, each_sample_count] {
      // std::cout << "put object thread " << t << " start" << std::endl;
      std::string prefix = "t"+std::to_string(t)+"-";

      //  compute how many seconds it takes to get 1000 objects
      int64_t start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

      for (int i = 0; i < each_sample_count; i++) {
        std::string key = prefix + std::to_string(i);
        objstore_bench.put_object(key, data);
      }

      int64_t end = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      int64_t duration = end - start;
      durations[t] = duration;

    });
    threads.push_back(std::move(th));
  }
  for (auto &th : threads) {
    th.join();
  }

  int64_t total_duaration = 0;
  int id = 0;
  for (auto d: durations) {
    id++;
    total_duaration += d;
    std::cout << "thread "<< id << ", avg latency: " << d/each_sample_count << "ms" << ", speed: " << each_sample_count*obj_size*1000/d  << ", duration: " << d << "ms" << std::endl;
  }

  long avg_speed = total_size*1000/total_duaration;
  std::cout << "actual sample count:" << sample_count/concurrent*concurrent << std::endl;
  std::cout << "total duration: " << total_duaration << "ms" << std::endl;
  std::cout << "avg speed: " << avg_speed << "MB/s" << std::endl;
  std::cout << "total object store write bindwidth reached: " << avg_speed * concurrent << "MB/s" << std::endl;

  std::cout << "============finish put object test================" << std::endl;
}

void test_get(char* data, int sample_count, smartengine::obj::ObjstoreBench &objstore_bench)
{
  // prepare_data(data, sample_count, objstore_bench);
  std::cout << "============begin get object test================" << std::endl;

  long each_sample_count = sample_count / concurrent;
  std::vector<std::thread> threads;
  std::vector<int64_t> durations;
  durations.assign(concurrent, 0);

  long total_size = sample_count/concurrent*concurrent*obj_size; // how many MB
  std::cout << "total size: " << total_size << std::endl;

  for (int t = 0; t < concurrent; t++) {
    std::thread th([&objstore_bench, &durations, t, each_sample_count] {
      // std::cout << "get object thread " << t << " start" << std::endl;
      std::string prefix = "t"+std::to_string(t)+"-";
      std::string buf;

      //  compute how many seconds it takes to get 1000 objects
      int64_t start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

      for (int i = 0; i < each_sample_count; i++) {
        std::string key = prefix + std::to_string(i);
        objstore_bench.get_object(key, buf);
      }
      int64_t end = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
      int64_t duration = end - start;
      durations[t] = duration;
    });
    threads.push_back(std::move(th));
  }

  for (auto &th : threads) {
    th.join();
  }

  int64_t total_duaration = 0;
  int id = 0;
  for (auto d: durations) {
    id++;
    total_duaration += d;
    std::cout << "thread "<< id << ", avg latency: " << d/each_sample_count << "ms" << ", speed: " << each_sample_count*obj_size*1000/d  << ", duration: " << d << "ms" << std::endl;
  }

  long avg_speed = total_size*1000/total_duaration;
  std::cout << "total duration: " << total_duaration << "ms" << std::endl;
  std::cout << "avg speed: " << avg_speed << "MB/s" << std::endl;
  std::cout << "total object store read bindwidth reached: " << avg_speed * concurrent << "MB/s" << std::endl;

  std::cout << "============finish get object test================" << std::endl;
}

int main(int argc, char **argv) {
#ifndef NDEBUG
  return 0;
#endif
  // parse --concurrent option, if not specified, use 16 by default
  // parse --samples options, if not specified, use 5000 by default
  if (argc > 1) {
    for (int i = 1; i < argc; i++) {
      if (strcmp(argv[i], "--concurrent") == 0) {
        concurrent = atoi(argv[i+1]);
      }
      if (strcmp(argv[i], "--samples") == 0) {
        samples_count = atoi(argv[i+1]);
      }
    }
  }
  if (concurrent <= 0) {
    std::cout << "invalid concurrent value" << std::endl;
    return -1;
  }

  if (samples_count <= 0) {
    std::cout << "invalid samples value" << std::endl;
    return -1;
  }

  std::string key = "test_put_object";
  char* data = new char[2048*1024];
  memset(data, 'a', 2048*1024);

  long each_sample_count = samples_count / concurrent;

  smartengine::obj::ObjstoreBench objstore_bench;
  objstore_bench.SetUp();

  test_put(data, samples_count, objstore_bench);

  test_get(data, samples_count, objstore_bench);

  objstore_bench.TearDown();

  delete[] data;
}
