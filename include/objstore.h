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
#include <utility>
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

  // Exact conditional object operations preserve the provider's decision.
  SE_OBJECT_CONFLICT = 116,
  SE_OBJECT_PRECONDITION_FAILED,
  SE_CONDITIONAL_OPERATION_NOT_SUPPORTED,

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

enum class ExactObjectOutcome : uint8_t {
  FOUND,
  NOT_FOUND_404,
  TRANSIENT_UNAVAILABLE,
  PERMANENT_ERROR,
  UNSUPPORTED,
};

enum class ExactFileOutcome : uint8_t {
  APPLIED,
  NOT_FOUND_404,
  TRANSIENT_UNAVAILABLE,
  PERMANENT_ERROR,
  UNSUPPORTED,
};

inline bool is_valid_object_store_etag(std::string_view etag);

class ExactFileResult {
 public:
  static ExactFileResult applied(uint64_t size, std::string etag) {
    if (!is_valid_object_store_etag(etag)) {
      return permanent_error(Status(Errors::SE_UNEXPECTED, 0,
                                    "exact file GET returned an invalid ETag"));
    }
    return ExactFileResult(ExactFileOutcome::APPLIED, Status(), size,
                           std::move(etag));
  }

  static ExactFileResult not_found(Status status) {
    return ExactFileResult(ExactFileOutcome::NOT_FOUND_404,
                           std::move(status));
  }

  static ExactFileResult transient_unavailable(Status status) {
    return ExactFileResult(ExactFileOutcome::TRANSIENT_UNAVAILABLE,
                           std::move(status));
  }

  static ExactFileResult permanent_error(Status status) {
    return ExactFileResult(ExactFileOutcome::PERMANENT_ERROR,
                           std::move(status));
  }

  static ExactFileResult unsupported() {
    return ExactFileResult(
        ExactFileOutcome::UNSUPPORTED,
        Status(Errors::SE_CONDITIONAL_OPERATION_NOT_SUPPORTED, 0,
               "exact streaming object GET is not supported by this provider"));
  }

  ExactFileOutcome outcome() const { return outcome_; }
  bool is_applied() const { return outcome_ == ExactFileOutcome::APPLIED; }
  uint64_t size() const { return size_; }
  const std::string &etag() const { return etag_; }
  const Status &status() const { return status_; }

 private:
  ExactFileResult(ExactFileOutcome outcome, Status status, uint64_t size = 0,
                  std::string etag = {})
      : outcome_(outcome),
        size_(size),
        etag_(std::move(etag)),
        status_(std::move(status)) {}

  ExactFileOutcome outcome_;
  uint64_t size_{0};
  std::string etag_;
  Status status_;
};

inline bool is_valid_object_store_etag(std::string_view etag) {
  if (etag.empty() || etag.front() == ' ' || etag.back() == ' ') return false;
  for (const unsigned char ch : etag) {
    if (ch < 0x20 || ch == 0x7f) return false;
  }
  return true;
}

class ExactObjectResult {
 public:
  static ExactObjectResult found(std::string body, std::string etag) {
    if (!is_valid_object_store_etag(etag)) {
      return permanent_error(Status(Errors::SE_UNEXPECTED, 0,
                                    "exact GET returned an invalid ETag"));
    }
    return ExactObjectResult(ExactObjectOutcome::FOUND, Status(),
                             std::move(body), std::move(etag));
  }

  static ExactObjectResult not_found(Status status) {
    return ExactObjectResult(ExactObjectOutcome::NOT_FOUND_404,
                             std::move(status));
  }

  static ExactObjectResult transient_unavailable(Status status) {
    return ExactObjectResult(ExactObjectOutcome::TRANSIENT_UNAVAILABLE,
                             std::move(status));
  }

  static ExactObjectResult permanent_error(Status status) {
    return ExactObjectResult(ExactObjectOutcome::PERMANENT_ERROR,
                             std::move(status));
  }

  static ExactObjectResult unsupported() {
    return ExactObjectResult(
        ExactObjectOutcome::UNSUPPORTED,
        Status(Errors::SE_CONDITIONAL_OPERATION_NOT_SUPPORTED, 0,
               "exact object GET is not supported by this provider"));
  }

  ExactObjectOutcome outcome() const { return outcome_; }
  bool is_found() const { return outcome_ == ExactObjectOutcome::FOUND; }
  const std::string &body() const { return body_; }
  const std::string &etag() const { return etag_; }
  uint64_t size() const { return size_; }
  const Status &status() const { return status_; }

 private:
  ExactObjectResult(ExactObjectOutcome outcome, Status status,
                    std::string body = {}, std::string etag = {})
      : outcome_(outcome),
        body_(std::move(body)),
        etag_(std::move(etag)),
        size_(body_.size()),
        status_(std::move(status)) {}

  ExactObjectOutcome outcome_;
  std::string body_;
  std::string etag_;
  uint64_t size_;
  Status status_;
};

enum class ConditionalPutMode : uint8_t { CREATE_ONLY, MATCH_ETAG };

class ConditionalPutCondition {
 public:
  static ConditionalPutCondition create_only() {
    return ConditionalPutCondition(ConditionalPutMode::CREATE_ONLY, {});
  }

  static ConditionalPutCondition match_etag(std::string etag) {
    return ConditionalPutCondition(ConditionalPutMode::MATCH_ETAG,
                                   std::move(etag));
  }

  ConditionalPutMode mode() const { return mode_; }
  const std::string &etag() const { return etag_; }

  bool is_valid() const {
    if (mode_ == ConditionalPutMode::CREATE_ONLY) return etag_.empty();
    return is_valid_object_store_etag(etag_);
  }

 private:
  ConditionalPutCondition(ConditionalPutMode mode, std::string etag)
      : mode_(mode), etag_(std::move(etag)) {}

  ConditionalPutMode mode_;
  std::string etag_;
};

enum class ConditionalPutOutcome : uint8_t {
  APPLIED,
  CONFLICT_409,
  PRECONDITION_FAILED_412,
  TRANSPORT_UNKNOWN,
  PERMANENT_ERROR,
  UNSUPPORTED,
};

class ConditionalPutResult {
 public:
  static ConditionalPutResult applied(std::string etag = {}) {
    return ConditionalPutResult(ConditionalPutOutcome::APPLIED, Status(),
                                std::move(etag));
  }

  static ConditionalPutResult conflict_409(Status status) {
    return ConditionalPutResult(ConditionalPutOutcome::CONFLICT_409,
                                std::move(status));
  }

  static ConditionalPutResult precondition_failed_412(Status status) {
    return ConditionalPutResult(ConditionalPutOutcome::PRECONDITION_FAILED_412,
                                std::move(status));
  }

  static ConditionalPutResult transport_unknown(Status status) {
    return ConditionalPutResult(ConditionalPutOutcome::TRANSPORT_UNKNOWN,
                                std::move(status));
  }

  static ConditionalPutResult permanent_error(Status status) {
    return ConditionalPutResult(ConditionalPutOutcome::PERMANENT_ERROR,
                                std::move(status));
  }

  static ConditionalPutResult unsupported() {
    return ConditionalPutResult(
        ConditionalPutOutcome::UNSUPPORTED,
        Status(Errors::SE_CONDITIONAL_OPERATION_NOT_SUPPORTED, 0,
               "conditional object PUT is not supported by this provider"));
  }

  ConditionalPutOutcome outcome() const { return outcome_; }
  bool is_applied() const { return outcome_ == ConditionalPutOutcome::APPLIED; }
  const std::string &etag() const { return etag_; }
  const Status &status() const { return status_; }

 private:
  ConditionalPutResult(ConditionalPutOutcome outcome, Status status,
                       std::string etag = {})
      : outcome_(outcome), etag_(std::move(etag)), status_(std::move(status)) {}

  ConditionalPutOutcome outcome_;
  std::string etag_;
  Status status_;
};

struct ConditionalObjectStoreCapabilities {
  bool exact_get_with_etag{false};
  bool exact_get_to_file{false};
  bool create_only{false};
  bool compare_and_swap{false};
  bool create_from_file{false};
  bool distinct_conflict_status{false};

  bool supports_remote_commit_io() const {
    return exact_get_with_etag && exact_get_to_file && create_only && compare_and_swap &&
           create_from_file && distinct_conflict_status;
  }
};

struct ObjectMeta {
  std::string key;
  int64_t last_modified{0};  // timestamp in milliseconds since epoch.
  long long size{0};          // body size
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
  // TODO (Zhao Dongsheng): This interface is temporary to implement condition write.
  virtual Status put_object(const std::string_view &bucket __attribute__((unused)),
                            const std::string_view &key __attribute__((unused)),
                            const std::string_view &data __attribute__((unused)),
                            const std::string &etag __attribute__((unused)),
                            bool forbid_overwrite __attribute__((unused)))
  {
    return Status(Errors::SE_IO_ERROR, 0, "not implemented");
  }
  virtual Status get_object(const std::string_view &bucket,
                            const std::string_view &key, std::string &body) = 0;
  // TODO (Zhao Dongsheng): This interface is temporary to implement condition read.
  virtual Status get_object(const std::string_view &bucket __attribute__((unused)),
                            const std::string_view &key __attribute__((unused)),
                            std::string &input __attribute__((unused)),
                            std::string *etag __attribute__((unused)))
  {
    return Status(Errors::SE_IO_ERROR, 0, "not implemented");
  }
  virtual Status get_object(const std::string_view &bucket,
                            const std::string_view &key, size_t off, size_t len,
                            std::string &body) = 0;

  virtual ConditionalObjectStoreCapabilities conditional_capabilities() const {
    return {};
  }

  // Failure results never retain body or ETag from an earlier operation.
  virtual ExactObjectResult get_object_exact(const std::string_view &,
                                             const std::string_view &) {
    return ExactObjectResult::unsupported();
  }

  // Reads at most max_bytes of response data. Providers must enforce the
  // limit in their response stream rather than buffering an oversized object
  // before returning an error.
  virtual ExactObjectResult get_object_exact(const std::string_view &,
                                             const std::string_view &,
                                             uint64_t) {
    return ExactObjectResult::unsupported();
  }

  // Streams one strongly consistent full-object GET into an existing empty
  // regular file. Implementations truncate the destination on every failure.
  virtual ExactFileResult get_object_to_file_exact(
      const std::string_view &, const std::string_view &,
      const std::string_view &) {
    return ExactFileResult::unsupported();
  }

  // Streams at most max_bytes into the destination. Providers must enforce
  // the limit while receiving the response and truncate on every failure.
  // The legacy overload remains available for callers without a byte cap.
  virtual ExactFileResult get_object_to_file_exact(
      const std::string_view &, const std::string_view &,
      const std::string_view &, uint64_t) {
    return ExactFileResult::unsupported();
  }

  virtual ConditionalPutResult put_object_conditional(
      const std::string_view &, const std::string_view &,
      const std::string_view &, const ConditionalPutCondition &condition) {
    if (!condition.is_valid()) {
      return ConditionalPutResult::permanent_error(
          Status(Errors::SE_INVALID, 0, "invalid conditional PUT condition"));
    }
    return ConditionalPutResult::unsupported();
  }

  virtual ConditionalPutResult put_object_from_file_conditional(
      const std::string_view &, const std::string_view &,
      const std::string_view &, const ConditionalPutCondition &condition) {
    if (!condition.is_valid()) {
      return ConditionalPutResult::permanent_error(
          Status(Errors::SE_INVALID, 0, "invalid conditional PUT condition"));
    }
    return ConditionalPutResult::unsupported();
  }

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

void cleanup_objstore_provider(const std::string_view &provider);

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
