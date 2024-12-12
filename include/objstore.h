/*
   Copyright (c) 2024, ApeCloud Inc Holding Limited.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#ifndef OBJSTORE_OBJSTORE_H_INCLUDED
#define OBJSTORE_OBJSTORE_H_INCLUDED

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace objstore {
// interfaces to manipulate object like aws S3.
// this interfaces will shield the differences between different object storages
// provider, such as aws S3, aliyun OSS, MinIO, etc.

enum Errors {
  SE_SUCCESS = 0,
  // smartengine internal error codes
  SE_IO_ERROR = 1,
  SE_INVALID,
  SE_UNEXPECTED,

  // object store errors, for s3, they are all not retryable errors:
  SE_BUCKET_ALREADY_EXISTS = 101,
  SE_BUCKET_ALREADY_OWNED_BY_YOU,
  SE_INVALID_OBJECT_STATE,
  SE_NO_SUCH_BUCKET,
  SE_NO_SUCH_KEY,
  SE_NO_SUCH_UPLOAD,
  SE_OBJECT_ALREADY_IN_ACTIVE_TIER,
  SE_OBJECT_NOT_IN_ACTIVE_TIER,

  // when forbid overwrite option is true, and the object already exists
  SE_OBJECT_FORBID_OVERWRITE,

  SE_LEASE_LOCK_RENEWAL_TIMEOUT,
  // when we detected other data node may be running.
  SE_OHTER_DATA_NODE_MAYBE_RUNNING,

  // object store common errors
  SE_ACCESS_DENIED,
  SE_OBJSTORE_INVALID_ARGUMENT,

  // object store errors, for aliyun
  SE_SYMLINK_TARGET_NOT_EXIST,
  SE_TOO_MANY_BUCKETS,

  // not-retryable generic errors, for s3, like:
  // INCOMPLETE_SIGNATURE
  // INVALID_ACTION
  // INVALID_CLIENT_TOKEN_ID
  // INVALID_PARAMETER_COMBINATION
  // INVALID_PARAMETER_VALUE
  // ...
  CLOUD_PROVIDER_UNRECOVERABLE_ERROR = 201,

  // retryable generic errors,  for s3, they are:
  // INTERNAL_FAILURE
  // REQUEST_EXPIRED
  //  SERVICE_UNAVAILABLE
  // THROTTLING
  // SLOW_DOWN
  // REQUEST_TIME_TOO_SKEWED
  // REQUEST_TIMEOUT
  CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED,
};

class Status {
 public:
  Status() {}
  Status(Errors error_code, int cloud_provider_err_code,
         std::string_view error_msg)
      : error_code_(error_code),
        cloud_provider_err_code_(cloud_provider_err_code) {
          error_msg_ = std::string("cloud provider error: ") + std::to_string(cloud_provider_err_code) 
                               + ", error code: " + std::to_string(error_code)
                               + ", error message: " + std::string(error_msg);
        }
  Status(Errors error_code, const std::string &cloud_provider_err_code_str,
         std::string_view error_msg)
      : error_code_(error_code),
        cloud_provider_err_code_str_(cloud_provider_err_code_str) {
          error_msg_ = std::string("cloud provider error: ") + cloud_provider_err_code_str
                               + ", error code: " + std::to_string(error_code)
                               + ", error message: " + std::string(error_msg);
        }
  ~Status() = default;

  bool is_succ() const { return error_code_ == 0; }

  void set_error_code(Errors error_code) { error_code_ = error_code; }
  Errors error_code() const { return error_code_; }

  void set_cloud_provider_err_code(int cloud_provider_err_code) {
    cloud_provider_err_code_ = cloud_provider_err_code;
  }
  void set_cloud_provider_err_code(const std::string &cloud_provider_err_code_str) {
    cloud_provider_err_code_str_ = cloud_provider_err_code_str;
  }
  int cloud_provider_err_code() const { return cloud_provider_err_code_; }

  void set_error_msg(std::string_view error_msg) { error_msg_ = error_msg; }
  std::string_view error_message() const { return error_msg_; }

 private:
  Errors error_code_{SE_SUCCESS};
  int cloud_provider_err_code_{0};
  std::string cloud_provider_err_code_str_;
  std::string error_msg_;
};

struct ObjectMeta {
  std::string key;
  int64_t last_modified; // timestamp in milliseconds since epoch.
  long long size;        // body size
};

class ObjectStore {
 public:
  virtual ~ObjectStore() = default;

  inline static bool use_s3_sdk(const std::string_view &provider) {
    return provider == "aws" || provider == "minio" || provider == "r2";
  }

  virtual Status create_bucket(const std::string_view &bucket) = 0;

  virtual Status delete_bucket(const std::string_view &bucket) = 0;

  virtual Status put_object_from_file(
      const std::string_view &bucket, const std::string_view &key,
      const std::string_view &data_file_path) = 0;
  virtual Status get_object_to_file(
      const std::string_view &bucket, const std::string_view &key,
      const std::string_view &output_file_path) = 0;

  virtual Status put_object(const std::string_view &bucket,
                            const std::string_view &key,
                            const std::string_view &data,
                            bool forbid_overwrite = false) = 0;
  virtual Status get_object(const std::string_view &bucket,
                            const std::string_view &key, std::string &body) = 0;
  virtual Status get_object(const std::string_view &bucket,
                            const std::string_view &key, size_t off, size_t len,
                            std::string &body) = 0;
  virtual Status get_object_meta(const std::string_view &bucket,
                                 const std::string_view &key,
                                 ObjectMeta &meta) = 0;

  virtual Status list_object(const std::string_view &bucket,
                             const std::string_view &prefix, bool recursive,
                             std::string &start_after, bool &finished,
                             std::vector<ObjectMeta> &objects) = 0;

  virtual Status delete_object(const std::string_view &bucket,
                               const std::string_view &key) = 0;

  virtual Status delete_directory(const std::string_view &bucket,
                                  const std::string_view &prefix);

  virtual Status put_objects_from_dir(const std::string_view &src_dir,
                                      const std::string_view &dst_objstore_bucket,
                                      const std::string_view &dst_objstore_dir);

  virtual Status get_objects_to_dir(const std::string_view &src_objstore_bucket,
                                    const std::string_view &src_objstore_dir,
                                    const std::string_view &dst_dir);

  virtual std::string_view get_provider() const = 0;

  virtual Status delete_objects(const std::string_view &bucket,
                                const std::vector<std::string_view> &object_keys) = 0;

  bool is_valid_key(const std::string_view &key);
};

// create ObjectStore based credentials in credentials dir or environment
// variables.
ObjectStore *create_object_store(const std::string_view &provider,
                                 const std::string_view region,
                                 const std::string_view *endpoint,
                                 bool use_https, std::string &err_msg);

ObjectStore *create_source_object_store(const std::string_view &provider,
                                        const std::string_view region,
                                        const std::string_view *endpoint,
                                        bool use_https, std::string &err_msg);

ObjectStore *create_dest_object_store(const std::string_view &provider,
                                      const std::string_view region,
                                      const std::string_view *endpoint,
                                      bool use_https, std::string &err_msg);

int init_object_store(const std::string_view &provider,
                      const std::string_view &region,
                      std::string &err_msg,
                      ObjectStore *&objstore);

void cleanup_object_store(ObjectStore *&objstore);

int ensure_object_store_lock(const std::string_view &provider,
                             const std::string_view &region,
                             const std::string_view *endpoint,
                             const std::string_view &bucket_dir,
                             const std::string_view &data_uuid,
                             const std::string_view &branch_id,
                             const bool should_exist, std::string &err_msg);

void destroy_object_store(ObjectStore *obj_store);

void init_objstore_provider(const std::string_view &provider);

void cleanup_objstore_provider(ObjectStore *objstore);

char *get_src_access_key_id();

char *get_src_access_secret_key();

char *get_dest_access_key_id();

char *get_dest_access_secret_key();

int mkdir_p(std::string_view path);

int rm_f(std::string_view path);

bool is_first_level_sub_key(const std::string_view &key,
                            const std::string_view &prefix);

std::string remove_prefix(const std::string &str, const std::string &prefix);

}  // namespace objstore

#endif  // OBJSTORE_OBJSTORE_H_INCLUDED
