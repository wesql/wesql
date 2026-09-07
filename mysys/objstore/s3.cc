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

#include "mysys/objstore/s3.h"
#include "aws/s3/S3Errors.h"
#include "mysys/objstore/s3_error.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <errno.h>
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

namespace objstore {

namespace { // anonymous namespace

Errors aws_error_to_objstore_error(const Aws::S3::S3Error &aws_error) {
  if (aws_error.ShouldRetry()) {
    // if the error is retryable, and we have retried enough times, we treat it
    // as an unrecoverable error of cloud provider.
    return Errors::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED;
  }
  if ((Aws::Http::HttpResponseCode::PRECONDITION_FAILED == aws_error.GetResponseCode()) ||
      (Aws::Http::HttpResponseCode::CONFLICT == aws_error.GetResponseCode())) {
    return Errors::SE_OBJECT_FORBID_OVERWRITE;
  }
  int aws_error_code = static_cast<int>(aws_error.GetErrorType());
  switch (aws_error_code) {
    case static_cast<int>(Aws::S3::S3Errors::ACCESS_DENIED):
      return Errors::SE_ACCESS_DENIED;
    case static_cast<int>(Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS):
      return Errors::SE_BUCKET_ALREADY_EXISTS;
    case static_cast<int>(Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU):
      return Errors::SE_BUCKET_ALREADY_OWNED_BY_YOU;
    case static_cast<int>(Aws::S3::S3Errors::INVALID_OBJECT_STATE):
      return Errors::SE_INVALID_OBJECT_STATE;
    case static_cast<int>(Aws::S3::S3Errors::NO_SUCH_BUCKET):
      return Errors::SE_NO_SUCH_BUCKET;
    case static_cast<int>(Aws::S3::S3Errors::NO_SUCH_KEY):
      return Errors::SE_NO_SUCH_KEY;
    case static_cast<int>(Aws::S3::S3Errors::NO_SUCH_UPLOAD):
      return Errors::SE_NO_SUCH_UPLOAD;
    case static_cast<int>(Aws::S3::S3Errors::OBJECT_ALREADY_IN_ACTIVE_TIER):
      return Errors::SE_OBJECT_ALREADY_IN_ACTIVE_TIER;
    case static_cast<int>(Aws::S3::S3Errors::OBJECT_NOT_IN_ACTIVE_TIER):
      return Errors::SE_OBJECT_NOT_IN_ACTIVE_TIER;
    case static_cast<int>(Aws::S3::S3Errors::UNKNOWN):
      return Errors::CLOUD_PROVIDER_UNRECOVERABLE_ERROR;
    default:
      // if the error is not-retryable and not a S3 specific error, it's an
      // error of cloud provider, we treat it as an unrecoverable error.
      return Errors::CLOUD_PROVIDER_UNRECOVERABLE_ERROR;
  }
}

int aws_http_status(const Aws::S3::S3Error &error) {
  return static_cast<int>(error.GetResponseCode());
}

Status exact_operation_status(const Aws::S3::S3Error &error,
                              Errors error_code) {
  int provider_code = aws_http_status(error);
  if (provider_code == 0) {
    provider_code = static_cast<int>(error.GetErrorType());
  }
  return Status(error_code, provider_code, error.GetMessage());
}

s3_detail::ObjectErrorKind object_error_kind(const Aws::S3::S3Error &error) {
  switch (error.GetErrorType()) {
    case Aws::S3::S3Errors::NO_SUCH_KEY:
      return s3_detail::ObjectErrorKind::NO_SUCH_KEY;
    case Aws::S3::S3Errors::NO_SUCH_BUCKET:
      return s3_detail::ObjectErrorKind::NO_SUCH_BUCKET;
    default:
      return s3_detail::ObjectErrorKind::OTHER;
  }
}

ExactObjectResult exact_get_failure(const Aws::S3::S3Error &error) {
  const ExactObjectOutcome outcome = s3_detail::classify_exact_get_failure(
      aws_http_status(error), error.ShouldRetry(), object_error_kind(error));
  switch (outcome) {
    case ExactObjectOutcome::NOT_FOUND_404:
      return ExactObjectResult::not_found(
          exact_operation_status(error, Errors::SE_NO_SUCH_KEY));
    case ExactObjectOutcome::TRANSIENT_UNAVAILABLE:
      return ExactObjectResult::transient_unavailable(exact_operation_status(
          error, Errors::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED));
    case ExactObjectOutcome::PERMANENT_ERROR: {
      Errors error_code = aws_error_to_objstore_error(error);
      if (object_error_kind(error) ==
          s3_detail::ObjectErrorKind::NO_SUCH_BUCKET) {
        error_code = Errors::SE_NO_SUCH_BUCKET;
      } else if (error_code ==
                 Errors::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED) {
        error_code = Errors::CLOUD_PROVIDER_UNRECOVERABLE_ERROR;
      }
      return ExactObjectResult::permanent_error(
          exact_operation_status(error, error_code));
    }
    case ExactObjectOutcome::FOUND:
    case ExactObjectOutcome::UNSUPPORTED:
      break;
  }
  return ExactObjectResult::permanent_error(Status(
      Errors::SE_UNEXPECTED, 0, "invalid exact GET failure classification"));
}

ExactFileResult exact_file_get_failure(const Aws::S3::S3Error &error) {
  const ExactObjectResult classified = exact_get_failure(error);
  switch (classified.outcome()) {
    case ExactObjectOutcome::NOT_FOUND_404:
      return ExactFileResult::not_found(classified.status());
    case ExactObjectOutcome::TRANSIENT_UNAVAILABLE:
      return ExactFileResult::transient_unavailable(classified.status());
    case ExactObjectOutcome::PERMANENT_ERROR:
      return ExactFileResult::permanent_error(classified.status());
    case ExactObjectOutcome::UNSUPPORTED:
      return ExactFileResult::unsupported();
    case ExactObjectOutcome::FOUND:
      break;
  }
  return ExactFileResult::permanent_error(Status(
      Errors::SE_UNEXPECTED, 0, "invalid exact file GET failure classification"));
}

Status truncate_exact_file(const std::filesystem::path &path) {
  errno = 0;
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    return Status(Errors::SE_IO_ERROR, errno == 0 ? EIO : errno,
                  "unable to truncate failed exact GET destination");
  }
  output.close();
  if (!output) {
    return Status(Errors::SE_IO_ERROR, errno == 0 ? EIO : errno,
                  "unable to close truncated exact GET destination");
  }

  std::error_code filesystem_error;
  const std::uintmax_t size =
      std::filesystem::file_size(path, filesystem_error);
  if (filesystem_error || size != 0) {
    return Status(Errors::SE_IO_ERROR,
                  filesystem_error ? filesystem_error.value() : EIO,
                  "failed exact GET destination is not empty");
  }
  return Status();
}

ExactFileResult exact_file_failure_after_truncate(
    const std::filesystem::path &path, ExactFileResult failure) {
  Status truncated = truncate_exact_file(path);
  if (!truncated.is_succ()) {
    return ExactFileResult::permanent_error(std::move(truncated));
  }
  return failure;
}

ConditionalPutResult conditional_put_failure(const Aws::S3::S3Error &error) {
  const ConditionalPutOutcome outcome =
      s3_detail::classify_conditional_put_failure(aws_http_status(error),
                                                  error.ShouldRetry());
  switch (outcome) {
    case ConditionalPutOutcome::CONFLICT_409:
      return ConditionalPutResult::conflict_409(
          exact_operation_status(error, Errors::SE_OBJECT_CONFLICT));
    case ConditionalPutOutcome::PRECONDITION_FAILED_412:
      return ConditionalPutResult::precondition_failed_412(
          exact_operation_status(error, Errors::SE_OBJECT_PRECONDITION_FAILED));
    case ConditionalPutOutcome::TRANSPORT_UNKNOWN:
      return ConditionalPutResult::transport_unknown(exact_operation_status(
          error, Errors::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED));
    case ConditionalPutOutcome::PERMANENT_ERROR: {
      Errors error_code = aws_error_to_objstore_error(error);
      if (error_code == Errors::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED ||
          error_code == Errors::SE_OBJECT_FORBID_OVERWRITE) {
        error_code = Errors::CLOUD_PROVIDER_UNRECOVERABLE_ERROR;
      }
      return ConditionalPutResult::permanent_error(
          exact_operation_status(error, error_code));
    }
    case ConditionalPutOutcome::APPLIED:
    case ConditionalPutOutcome::UNSUPPORTED:
      break;
  }
  return ConditionalPutResult::permanent_error(Status(
      Errors::SE_UNEXPECTED, 0, "invalid conditional PUT classification"));
}

void set_put_condition(Aws::S3::Model::PutObjectRequest &request,
                       const ConditionalPutCondition &condition) {
  if (condition.mode() == ConditionalPutMode::CREATE_ONLY) {
    request.SetAdditionalCustomHeaderValue("If-None-Match", "*");
  } else {
    request.SetAdditionalCustomHeaderValue(
        "If-Match",
        Aws::String(condition.etag().data(), condition.etag().size()));
  }
}

constexpr size_t kExactObjectReadChunkBytes = 64 * 1024;
constexpr char kExactObjectResponseAllocationTag[] =
    "ExactObjectResponseStream";
constexpr char kExactFileResponseAllocationTag[] = "ExactFileResponseStream";

// The SDK reads error XML from the same stream used for successful objects.
// Keep a bounded readable prefix independently of the caller's payload limit.
struct ExactGetResponseState {
  static constexpr size_t kErrorBodyMaxBytes = 64 * 1024;

  void reset() {
    prefix.clear();
    bytes_received = 0;
  }

  bool complete_error_body() const { return bytes_received == prefix.size(); }

  std::string prefix;
  uint64_t bytes_received{0};
};

class ExactGetResponseStreambuf : public std::streambuf {
 public:
  explicit ExactGetResponseStreambuf(
      std::shared_ptr<ExactGetResponseState> response)
      : response_(std::move(response)) {}

 protected:
  std::streamsize record_response(const char *data, std::streamsize count) {
    const size_t captured = static_cast<size_t>(std::min<uint64_t>(
        static_cast<uint64_t>(count),
        ExactGetResponseState::kErrorBodyMaxBytes - response_->prefix.size()));
    response_->prefix.append(data, captured);
    response_->bytes_received += static_cast<uint64_t>(count);
    return static_cast<std::streamsize>(captured);
  }

  int_type underflow() override {
    if (gptr() != nullptr) return traits_type::eof();
    char *begin = response_->prefix.data();
    setg(begin, begin, begin + response_->prefix.size());
    return gptr() == egptr() ? traits_type::eof()
                            : traits_type::to_int_type(*gptr());
  }

  pos_type seekoff(off_type offset, std::ios_base::seekdir direction,
                   std::ios_base::openmode mode) override {
    if (mode == std::ios_base::out && direction == std::ios_base::cur &&
        offset == 0)
      return pos_type(response_->bytes_received);
    if (mode != std::ios_base::in) return pos_type(off_type(-1));
    const off_type size = static_cast<off_type>(response_->prefix.size());
    const off_type base = direction == std::ios_base::beg ? 0 :
        direction == std::ios_base::end ? size :
        gptr() == nullptr ? 0 : gptr() - eback();
    if (offset < -base || offset > size - base)
      return pos_type(off_type(-1));
    char *begin = response_->prefix.data();
    setg(begin, begin + base + offset, begin + size);
    return pos_type(base + offset);
  }

  pos_type seekpos(pos_type position, std::ios_base::openmode mode) override {
    return seekoff(off_type(position), std::ios_base::beg, mode);
  }

 private:
  std::shared_ptr<ExactGetResponseState> response_;
};

struct BoundedExactObjectState : ExactGetResponseState {
  explicit BoundedExactObjectState(uint64_t limit) : max_bytes(limit) {}

  void reset() {
    ExactGetResponseState::reset();
    body.clear();
    limit_exceeded = false;
    write_failed = false;
  }

  uint64_t max_bytes;
  std::string body;
  bool limit_exceeded{false};
  bool write_failed{false};
};

class BoundedExactObjectStreambuf final : public ExactGetResponseStreambuf {
 public:
  explicit BoundedExactObjectStreambuf(
      std::shared_ptr<BoundedExactObjectState> state)
      : ExactGetResponseStreambuf(state), state_(std::move(state)) {}

 protected:
  std::streamsize xsputn(const char *data, std::streamsize count) override {
    if (count <= 0) return count;

    const uint64_t remaining =
        state_->max_bytes - static_cast<uint64_t>(state_->body.size());
    const uint64_t requested = static_cast<uint64_t>(count);
    const uint64_t accepted = std::min(remaining, requested);
    uint64_t offset = 0;
    std::streamsize diagnostic_bytes = 0;
    try {
      diagnostic_bytes = record_response(data, count);
      while (offset < accepted) {
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(
            accepted - offset, kExactObjectReadChunkBytes));
        state_->body.append(data + offset, chunk);
        offset += chunk;
      }
    } catch (const std::bad_alloc &) {
      state_->write_failed = true;
      return static_cast<std::streamsize>(offset);
    }
    if (accepted != requested) state_->limit_exceeded = true;
    return std::max(static_cast<std::streamsize>(accepted), diagnostic_bytes);
  }

  int_type overflow(int_type value) override {
    if (traits_type::eq_int_type(value, traits_type::eof())) {
      return traits_type::not_eof(value);
    }
    const char byte = traits_type::to_char_type(value);
    return xsputn(&byte, 1) == 1 ? value : traits_type::eof();
  }

 private:
  std::shared_ptr<BoundedExactObjectState> state_;
};

struct BoundedExactFileState : ExactGetResponseState {
  BoundedExactFileState(std::filesystem::path destination, uint64_t limit)
      : path(std::move(destination)), max_bytes(limit) {}

  void reset() {
    ExactGetResponseState::reset();
    bytes_written = 0;
    limit_exceeded = false;
    write_failed = false;
  }

  std::filesystem::path path;
  uint64_t max_bytes;
  uint64_t bytes_written{0};
  bool limit_exceeded{false};
  bool write_failed{false};
};

class BoundedExactFileStreambuf final : public ExactGetResponseStreambuf {
 public:
  explicit BoundedExactFileStreambuf(
      std::shared_ptr<BoundedExactFileState> state)
      : ExactGetResponseStreambuf(state), state_(std::move(state)) {
    if (file_.open(state_->path,
                   std::ios_base::out | std::ios_base::binary |
                       std::ios_base::trunc) == nullptr) {
      state_->write_failed = true;
    }
  }

  ~BoundedExactFileStreambuf() override { close(); }

  bool close() {
    if (!file_.is_open()) return !state_->write_failed;
    const bool synced = file_.pubsync() == 0;
    const bool closed = file_.close() != nullptr;
    if (!synced || !closed) {
      state_->write_failed = true;
    }
    return !state_->write_failed;
  }

 protected:
  std::streamsize xsputn(const char *data, std::streamsize count) override {
    if (count <= 0) return count;
    if (!file_.is_open()) {
      state_->write_failed = true;
      return 0;
    }

    const uint64_t remaining = state_->max_bytes - state_->bytes_written;
    const uint64_t requested = static_cast<uint64_t>(count);
    const uint64_t accepted = std::min(remaining, requested);
    std::streamsize diagnostic_bytes = 0;
    try {
      diagnostic_bytes = record_response(data, count);
    } catch (const std::bad_alloc &) {
      state_->write_failed = true;
      return 0;
    }
    if (accepted != requested) state_->limit_exceeded = true;
    const std::streamsize written =
        file_.sputn(data, static_cast<std::streamsize>(accepted));
    if (written < 0 || static_cast<uint64_t>(written) != accepted) {
      state_->write_failed = true;
    }
    if (written > 0) state_->bytes_written += static_cast<uint64_t>(written);
    return state_->write_failed ? written : std::max(written, diagnostic_bytes);
  }

  int_type overflow(int_type value) override {
    if (traits_type::eq_int_type(value, traits_type::eof())) {
      return traits_type::not_eof(value);
    }
    const char byte = traits_type::to_char_type(value);
    return xsputn(&byte, 1) == 1 ? value : traits_type::eof();
  }

  int sync() override {
    if (!file_.is_open() || file_.pubsync() != 0) {
      state_->write_failed = true;
      return -1;
    }
    return 0;
  }

 private:
  std::shared_ptr<BoundedExactFileState> state_;
  std::filebuf file_;
};

}  // namespace

namespace fs = std::filesystem;

Status S3ObjectStore::create_bucket(const std::string_view &bucket) {
  Aws::S3::Model::CreateBucketRequest request;
  request.SetBucket(std::string(bucket));

  Aws::S3::Model::CreateBucketConfiguration createBucketConfig;
  createBucketConfig.SetLocationConstraint(
      Aws::S3::Model::BucketLocationConstraintMapper::
          GetBucketLocationConstraintForName(region_));
  request.SetCreateBucketConfiguration(createBucketConfig);

  Aws::S3::Model::CreateBucketOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.CreateBucket(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::delete_bucket(const std::string_view &bucket) {
  Aws::S3::Model::DeleteBucketRequest request;
  request.SetBucket(std::string(bucket));

  Aws::S3::Model::DeleteBucketOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.DeleteBucket(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::put_object_from_file(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &data_file_path) {
  Aws::S3::Model::PutObjectRequest request;
  Aws::String full_key(key);

  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
      "IOStreamAllocationTag", data_file_path.data(),
      std::ios_base::in | std::ios_base::binary);

  if (!*input_data) {
    return Status(Errors::SE_IO_ERROR, 0, "Error unable to open input file");
  }

  request.SetBody(input_data);

  Aws::S3::Model::PutObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.PutObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::get_object_to_file(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &output_file_path) {
  std::string result;
  Status status = get_object(bucket, key, result);

  std::shared_ptr<Aws::IOStream> output_stream = Aws::MakeShared<Aws::FStream>(
      "IOStreamAllocationTag", output_file_path.data(),
      std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!*output_stream) {
    return Status(Errors::SE_IO_ERROR, 0, "Error unable to open output file");
  }

  bool fail =
      output_stream->write(result.c_str(), result.length()).flush().fail();
  if (fail) {
    return Status(Errors::SE_IO_ERROR, 0,
                  "unable to write key's value into file");
  }

  return Status();
}

Status S3ObjectStore::put_object(const std::string_view &bucket,
                                 const std::string_view &key,
                                 const std::string_view &data,
                                 bool forbid_overwrite) {
  if (!is_valid_key(key)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }
  Aws::S3::Model::PutObjectRequest request;
  Aws::String full_key(key);
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));
  if (forbid_overwrite) {
    request.SetAdditionalCustomHeaderValue("If-None-Match", "*");
  }

  const std::shared_ptr<Aws::IOStream> data_stream =
      Aws::MakeShared<Aws::StringStream>("SStreamAllocationTag");
  if (!*data_stream) {
    return Status(Errors::SE_IO_ERROR, 0,
                  "unable to create data stream to hold input data");
  }

  *data_stream << data;
  if (!*data_stream) {
    return Status(Errors::SE_IO_ERROR, 0,
                  "unable to write data into data stream");
  }

  request.SetBody(data_stream);

  Aws::S3::Model::PutObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.PutObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

// TODO (Zhao Dongsheng): This interface is temporary to implement condition write.
// Most of the code is same with above put_object, so we can refactor it later.
Status S3ObjectStore::put_object(const std::string_view &bucket,
                                 const std::string_view &key,
                                 const std::string_view &data,
                                 const std::string &etag,
                                 bool forbid_overwrite) {
  if (!is_valid_key(key) || (forbid_overwrite && !etag.empty())) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }
  Aws::S3::Model::PutObjectRequest request;
  Aws::String full_key(key);
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));
  if (forbid_overwrite) {
    request.SetAdditionalCustomHeaderValue("If-None-Match", "*");
  } else if (!etag.empty()) {
    request.SetAdditionalCustomHeaderValue("If-Match", etag);
  }

  const std::shared_ptr<Aws::IOStream> data_stream =
      Aws::MakeShared<Aws::StringStream>("SStreamAllocationTag");
  if (!*data_stream) {
    return Status(Errors::SE_IO_ERROR, 0,
                  "unable to create data stream to hold input data");
  }

  *data_stream << data;
  if (!*data_stream) {
    return Status(Errors::SE_IO_ERROR, 0,
                  "unable to write data into data stream");
  }

  request.SetBody(data_stream);

  Aws::S3::Model::PutObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.PutObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::get_object(const std::string_view &bucket,
                                 const std::string_view &key,
                                 std::string &body) {
  Aws::S3::Model::GetObjectRequest request;
  Aws::String full_key(key);
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  Aws::S3::Model::GetObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.GetObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);

      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  std::ostringstream oss;
  if (outcome.GetResult().GetBody().rdbuf()->in_avail() > 0) {
    oss << outcome.GetResult().GetBody().rdbuf();
    if (!oss) {
      return Status(Errors::SE_IO_ERROR, 0,
                    "unable to read data from response stream");
    }
    body = oss.str();
  } else {
    body = "";
  }

  return Status();
}


// TODO (Zhao Dongsheng): This interface is temporary to implement condition read.
// Most of the code is same with above get_object, so we can refactor it later.
Status S3ObjectStore::get_object(const std::string_view &bucket,
                                 const std::string_view &key,
                                 std::string &body,
                                 std::string *etag) {
  Aws::S3::Model::GetObjectRequest request;
  Aws::String full_key(key);
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  Aws::S3::Model::GetObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.GetObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);

      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  std::ostringstream oss;
  if (outcome.GetResult().GetBody().rdbuf()->in_avail() > 0) {
    oss << outcome.GetResult().GetBody().rdbuf();
    if (!oss) {
      return Status(Errors::SE_IO_ERROR, 0,
                    "unable to read data from response stream");
    }
    body = oss.str();
  } else {
    body = "";
  }

  // Get ETag. Even the body is empty, we still need to get ETag.
  if (nullptr != etag) {
    *etag = outcome.GetResult().GetETag();
  }

  return Status();
}

ExactObjectResult S3ObjectStore::get_object_exact(
    const std::string_view &bucket, const std::string_view &key) {
  return get_object_exact(bucket, key, std::numeric_limits<uint64_t>::max());
}

ExactObjectResult S3ObjectStore::get_object_exact(
    const std::string_view &bucket, const std::string_view &key,
    uint64_t max_bytes) {
  if (bucket.empty() || bucket.find('\0') != std::string_view::npos ||
      !is_valid_key(key) || key.find('\0') != std::string_view::npos) {
    return ExactObjectResult::permanent_error(
        Status(Errors::SE_INVALID, EINVAL, "invalid bucket or key"));
  }
  if (max_bytes == 0) {
    return ExactObjectResult::permanent_error(Status(
        Errors::SE_INVALID, EINVAL, "exact GET byte limit must be positive"));
  }

  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(Aws::String(bucket.data(), bucket.size()));
  request.SetKey(Aws::String(key.data(), key.size()));
  const auto response_state =
      std::make_shared<BoundedExactObjectState>(max_bytes);
  request.SetResponseStreamFactory([response_state]() -> Aws::IOStream * {
    response_state->reset();
    Aws::UniquePtr<std::streambuf> buffer(
        Aws::New<BoundedExactObjectStreambuf>(
            kExactObjectResponseAllocationTag, response_state));
    return Aws::New<Aws::Utils::Stream::DefaultUnderlyingStream>(
        kExactObjectResponseAllocationTag, std::move(buffer));
  });

  Aws::S3::Model::GetObjectOutcome outcome = do_get_object(request);
  if (!outcome.IsSuccess() && response_state->complete_error_body() &&
      !response_state->write_failed &&
      aws_http_status(outcome.GetError()) >= 300) {
    return exact_get_failure(outcome.GetError());
  }
  if (response_state->limit_exceeded) {
    return ExactObjectResult::permanent_error(Status(
        Errors::SE_UNEXPECTED, EFBIG,
        "exact GET response body exceeds configured byte limit"));
  }
  if (response_state->write_failed) {
    return ExactObjectResult::permanent_error(Status(
        Errors::SE_IO_ERROR, ENOMEM,
        "unable to buffer bounded exact GET response body"));
  }
  if (!outcome.IsSuccess()) {
    if (aws_http_status(outcome.GetError()) >= 300 &&
        !response_state->complete_error_body())
      return ExactObjectResult::permanent_error(Status(
          Errors::SE_UNEXPECTED, EFBIG, "exact GET error body exceeds limit"));
    return exact_get_failure(outcome.GetError());
  }

  auto &result = outcome.GetResult();
  const long long content_length = result.GetContentLength();
  if (content_length < 0) {
    return ExactObjectResult::permanent_error(
        Status(Errors::SE_UNEXPECTED, EIO,
               "exact GET returned a negative content length"));
  }
  if (static_cast<uint64_t>(content_length) > max_bytes) {
    return ExactObjectResult::permanent_error(Status(
        Errors::SE_UNEXPECTED, EFBIG,
        "exact GET content length exceeds configured byte limit"));
  }
  if (static_cast<uint64_t>(content_length) !=
      static_cast<uint64_t>(response_state->body.size())) {
    return ExactObjectResult::permanent_error(
        Status(Errors::SE_UNEXPECTED, EIO,
               "exact GET content length does not match response body"));
  }

  return ExactObjectResult::found(
      std::move(response_state->body),
      std::string(result.GetETag().data(), result.GetETag().size()));
}

ExactFileResult S3ObjectStore::get_object_to_file_exact(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &output_file_path) {
  return get_object_to_file_exact(bucket, key, output_file_path,
                                  std::numeric_limits<uint64_t>::max());
}

ExactFileResult S3ObjectStore::get_object_to_file_exact(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &output_file_path, uint64_t max_bytes) {
  if (bucket.empty() || bucket.find('\0') != std::string_view::npos ||
      !is_valid_key(key) || key.find('\0') != std::string_view::npos ||
      output_file_path.empty() ||
      output_file_path.find('\0') != std::string_view::npos) {
    return ExactFileResult::permanent_error(
        Status(Errors::SE_INVALID, EINVAL, "invalid bucket, key, or output path"));
  }
  if (max_bytes == 0) {
    return ExactFileResult::permanent_error(Status(
        Errors::SE_INVALID, EINVAL,
        "exact file GET byte limit must be positive"));
  }

  const fs::path destination{std::string(output_file_path)};
  std::error_code filesystem_error;
  const fs::file_status status = fs::symlink_status(destination, filesystem_error);
  if (filesystem_error || !fs::is_regular_file(status) || fs::is_symlink(status) ||
      fs::file_size(destination, filesystem_error) != 0 || filesystem_error) {
    return ExactFileResult::permanent_error(Status(
        Errors::SE_INVALID, filesystem_error ? filesystem_error.value() : EINVAL,
        "exact file GET destination is not an existing empty regular file"));
  }

  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(Aws::String(bucket.data(), bucket.size()));
  request.SetKey(Aws::String(key.data(), key.size()));
  const auto response_state =
      std::make_shared<BoundedExactFileState>(destination, max_bytes);
  request.SetResponseStreamFactory([response_state]() -> Aws::IOStream * {
    response_state->reset();
    Aws::UniquePtr<std::streambuf> buffer(
        Aws::New<BoundedExactFileStreambuf>(
            kExactFileResponseAllocationTag, response_state));
    return Aws::New<Aws::Utils::Stream::DefaultUnderlyingStream>(
        kExactFileResponseAllocationTag, std::move(buffer));
  });

  Aws::S3::Model::GetObjectOutcome outcome = do_get_object(request);
  if (!outcome.IsSuccess() && response_state->complete_error_body() &&
      !response_state->write_failed &&
      aws_http_status(outcome.GetError()) >= 300) {
    return exact_file_failure_after_truncate(
        destination, exact_file_get_failure(outcome.GetError()));
  }
  if (response_state->limit_exceeded) {
    return exact_file_failure_after_truncate(
        destination, ExactFileResult::permanent_error(Status(
                         Errors::SE_UNEXPECTED, EFBIG,
                         "exact file GET response exceeds configured byte limit")));
  }
  if (response_state->write_failed) {
    return exact_file_failure_after_truncate(
        destination, ExactFileResult::permanent_error(Status(
                         Errors::SE_IO_ERROR, EIO,
                         "unable to stream bounded exact GET response file")));
  }
  if (!outcome.IsSuccess()) {
    if (aws_http_status(outcome.GetError()) >= 300 &&
        !response_state->complete_error_body())
      return exact_file_failure_after_truncate(
          destination, ExactFileResult::permanent_error(Status(
              Errors::SE_UNEXPECTED, EFBIG,
              "exact file GET error body exceeds limit")));
    return exact_file_failure_after_truncate(
        destination, exact_file_get_failure(outcome.GetError()));
  }

  auto &result = outcome.GetResult();
  Aws::IOStream &output = result.GetBody();
  output.flush();
  const bool flush_succeeded = static_cast<bool>(output);
  auto *output_buffer =
      dynamic_cast<BoundedExactFileStreambuf *>(output.rdbuf());
  const bool close_succeeded =
      output_buffer != nullptr && output_buffer->close();
  if (!flush_succeeded || !close_succeeded || response_state->write_failed) {
    return exact_file_failure_after_truncate(
        destination, ExactFileResult::permanent_error(Status(
                         Errors::SE_IO_ERROR, EIO,
                         "unable to close exact GET response file")));
  }

  const long long content_length = result.GetContentLength();
  const std::uintmax_t file_size = fs::file_size(destination, filesystem_error);
  if (content_length < 0 ||
      (content_length >= 0 &&
       static_cast<uint64_t>(content_length) > max_bytes) ||
      filesystem_error ||
      static_cast<uint64_t>(content_length) !=
          static_cast<uint64_t>(file_size) ||
      static_cast<uint64_t>(file_size) != response_state->bytes_written) {
    return exact_file_failure_after_truncate(
        destination,
        ExactFileResult::permanent_error(Status(
            Errors::SE_UNEXPECTED,
            filesystem_error ? filesystem_error.value() : EIO,
            "exact file GET content length does not match streamed bytes")));
  }

  const auto &etag = result.GetETag();
  ExactFileResult exact = ExactFileResult::applied(
      static_cast<uint64_t>(content_length),
      std::string(etag.data(), etag.size()));
  if (!exact.is_applied()) {
    return exact_file_failure_after_truncate(destination, std::move(exact));
  }
  return exact;
}

ConditionalPutResult S3ObjectStore::put_object_conditional(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &data, const ConditionalPutCondition &condition) {
  if (!condition.is_valid()) {
    return ConditionalPutResult::permanent_error(Status(
        Errors::SE_INVALID, EINVAL, "invalid conditional PUT condition"));
  }
  if (bucket.empty() || bucket.find('\0') != std::string_view::npos ||
      !is_valid_key(key) || key.find('\0') != std::string_view::npos) {
    return ConditionalPutResult::permanent_error(
        Status(Errors::SE_INVALID, EINVAL, "invalid bucket or key"));
  }
  if (data.size() >
          static_cast<size_t>(std::numeric_limits<long long>::max()) ||
      data.size() >
          static_cast<size_t>(std::numeric_limits<std::streamsize>::max())) {
    return ConditionalPutResult::permanent_error(
        Status(Errors::SE_INVALID, EOVERFLOW, "object is too large"));
  }

  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(Aws::String(bucket.data(), bucket.size()));
  request.SetKey(Aws::String(key.data(), key.size()));
  request.SetContentLength(static_cast<long long>(data.size()));
  set_put_condition(request, condition);

  const std::shared_ptr<Aws::IOStream> data_stream =
      Aws::MakeShared<Aws::StringStream>("SStreamAllocationTag");
  if (!data_stream || !*data_stream) {
    return ConditionalPutResult::permanent_error(Status(
        Errors::SE_IO_ERROR, ENOMEM, "unable to create object data stream"));
  }
  data_stream->write(data.data(), static_cast<std::streamsize>(data.size()));
  data_stream->flush();
  data_stream->clear();
  data_stream->seekg(0);
  if (!*data_stream) {
    return ConditionalPutResult::permanent_error(Status(
        Errors::SE_IO_ERROR, EIO, "unable to prepare object data stream"));
  }
  request.SetBody(data_stream);

  // One SDK invocation is one logical attempt. The SDK rewinds this seekable
  // stream for its internal transport retries; callers disambiguate the result.
  Aws::S3::Model::PutObjectOutcome outcome = do_put_object(request);
  if (!outcome.IsSuccess()) {
    return conditional_put_failure(outcome.GetError());
  }
  const auto &etag = outcome.GetResult().GetETag();
  return ConditionalPutResult::applied(std::string(etag.data(), etag.size()));
}

ConditionalPutResult S3ObjectStore::put_object_from_file_conditional(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &data_file_path,
    const ConditionalPutCondition &condition) {
  if (!condition.is_valid()) {
    return ConditionalPutResult::permanent_error(Status(
        Errors::SE_INVALID, EINVAL, "invalid conditional PUT condition"));
  }
  if (bucket.empty() || bucket.find('\0') != std::string_view::npos ||
      !is_valid_key(key) || key.find('\0') != std::string_view::npos ||
      data_file_path.find('\0') != std::string_view::npos) {
    return ConditionalPutResult::permanent_error(Status(
        Errors::SE_INVALID, EINVAL, "invalid bucket, key, or input file path"));
  }

  const fs::path input_path{std::string(data_file_path)};
  std::error_code error;
  if (!fs::is_regular_file(input_path, error)) {
    const int error_code = error ? error.value() : EINVAL;
    return ConditionalPutResult::permanent_error(Status(
        Errors::SE_IO_ERROR, error_code, "unable to open input object file"));
  }
  const std::uintmax_t file_size = fs::file_size(input_path, error);
  if (error) {
    return ConditionalPutResult::permanent_error(
        Status(Errors::SE_IO_ERROR, error.value(),
               "unable to stat input object file"));
  }
  if (file_size >
      static_cast<std::uintmax_t>(std::numeric_limits<long long>::max())) {
    return ConditionalPutResult::permanent_error(
        Status(Errors::SE_INVALID, EOVERFLOW, "object file is too large"));
  }

  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(Aws::String(bucket.data(), bucket.size()));
  request.SetKey(Aws::String(key.data(), key.size()));
  request.SetContentLength(static_cast<long long>(file_size));
  set_put_condition(request, condition);

  const std::string input_path_string = input_path.string();
  const std::shared_ptr<Aws::IOStream> input = Aws::MakeShared<Aws::FStream>(
      "IOStreamAllocationTag", input_path_string.c_str(),
      std::ios_base::in | std::ios_base::binary);
  if (!input || !*input) {
    return ConditionalPutResult::permanent_error(
        Status(Errors::SE_IO_ERROR, EIO, "unable to open input object file"));
  }
  request.SetBody(input);

  // A caller-controlled retry reopens the file and constructs a fresh request.
  Aws::S3::Model::PutObjectOutcome outcome = do_put_object(request);
  if (!outcome.IsSuccess()) {
    return conditional_put_failure(outcome.GetError());
  }
  const auto &etag = outcome.GetResult().GetETag();
  return ConditionalPutResult::applied(std::string(etag.data(), etag.size()));
}

Status S3ObjectStore::get_object(const std::string_view &bucket,
                                 const std::string_view &key, size_t off,
                                 size_t len, std::string &body) {
  Aws::S3::Model::GetObjectRequest request;
  Aws::String full_key(key);
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));
  std::string byte_range =
      "bytes=" + std::to_string(off) + "-" + std::to_string(off + len - 1);
  request.SetRange(byte_range);

  Aws::S3::Model::GetObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.GetObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  std::ostringstream oss;
  if (outcome.GetResult().GetBody().rdbuf()->in_avail() > 0) {
    oss << outcome.GetResult().GetBody().rdbuf();
    if (!oss) {
      return Status(Errors::SE_IO_ERROR, 0,
                    "unable to read data from response stream");
    }
    body = oss.str();
  } else {
    body = "";
  }
  return Status();
}

Status S3ObjectStore::get_object_meta(const std::string_view &bucket,
                                      const std::string_view &key,
                                      ObjectMeta &meta) {
  Aws::S3::Model::HeadObjectRequest request;
  Aws::String full_key(key);
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  Aws::S3::Model::HeadObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.HeadObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  meta.key = key;
  meta.last_modified = outcome.GetResult().GetLastModified().Millis();
  meta.size = outcome.GetResult().GetContentLength();

  return Status();
}

Status S3ObjectStore::list_object(const std::string_view &bucket,
                                  const std::string_view &prefix,
                                  bool recursive, std::string &start_after,
                                  bool &finished,
                                  std::vector<ObjectMeta> &objects) {
  Aws::S3::Model::ListObjectsV2Request request;
  Aws::String full_prefix(prefix);
  request.SetBucket(Aws::String(bucket));
  request.SetPrefix(full_prefix);
  if (!start_after.empty()) {
    request.SetContinuationToken(Aws::String(start_after));
  }

  Aws::S3::Model::ListObjectsV2Outcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }

      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }
  const Aws::Vector<Aws::S3::Model::Object> &s3_objects =
      outcome.GetResult().GetContents();

  for (const auto &obj : s3_objects) {
    // only list first-level sub keys
    if (!recursive && !is_first_level_sub_key(obj.GetKey(), full_prefix)) {
      continue;
    }
    ObjectMeta meta;
    meta.key = obj.GetKey();
    meta.last_modified = obj.GetLastModified().Millis();
    meta.size = obj.GetSize();
    objects.push_back(meta);
  }
  finished = !outcome.GetResult().GetIsTruncated();
  if (finished) {
    start_after = "";
  } else {
    if (!s3_objects.empty()) {
      start_after = outcome.GetResult().GetNextContinuationToken();
    } else {
      Errors err_type = Errors::CLOUD_PROVIDER_UNRECOVERABLE_ERROR;
      return Status(err_type, 0,
                    "list object returned empty objects but should not");
    }
  }

  return Status();
}

Status S3ObjectStore::delete_object(const std::string_view &bucket,
                                    const std::string_view &key) {
  Aws::S3::Model::DeleteObjectRequest request;
  Aws::String full_key(key);
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  Aws::S3::Model::DeleteObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.DeleteObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::delete_objects(
    const std::string_view &bucket,
    const std::vector<std::string_view> &object_keys) {
  Aws::String full_key;
  Aws::String common_prefix;
  Aws::Vector<Aws::S3::Model::ObjectIdentifier> object_identifiers;

  for (size_t i = 0; i < object_keys.size(); i++) {
    const std::string_view &object_key = object_keys[i];
    full_key = object_key;
    object_identifiers.emplace_back(
        Aws::S3::Model::ObjectIdentifier().WithKey(full_key));
    if (object_identifiers.size() == kDeleteObjsNumEach || i == object_keys.size() - 1) {
      Aws::S3::Model::DeleteObjectsRequest request;
      Aws::S3::Model::Delete delete_object;
      delete_object.SetObjects(std::move(object_identifiers));
      request.SetDelete(std::move(delete_object));
      request.SetBucket(Aws::String(bucket));

      int retry_times = retry_times_on_error_;
      Aws::S3::Model::DeleteObjectsOutcome outcome;
      while (true) {
        outcome = s3_client_.DeleteObjects(request);
        if (!outcome.IsSuccess()) {
          const Aws::S3::S3Error &err = outcome.GetError();
          bool should_retry = err.ShouldRetry();
          if (retry_times-- > 0 && should_retry) {
            continue;
          }
          Errors err_type = aws_error_to_objstore_error(err);
          return Status(err_type, static_cast<int>(err.GetErrorType()),
                        err.GetMessage());
        } else {
          break;
        }
      }
      object_identifiers.clear();
    }    
  }
  
  return Status();
}

void init_aws_api() {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
}

void shutdown_aws_api() {
  Aws::SDKOptions options;
  Aws::ShutdownAPI(options);
}

char *get_s3_access_key_id() {
  char *access_key_id = std::getenv("AWS_ACCESS_KEY_ID");
  if (access_key_id) {
    return access_key_id;
  }
  access_key_id = std::getenv("ACCESS_KEY_ID");
  if (access_key_id) {
    return access_key_id;
  }
  return nullptr;
}

char *get_s3_access_secret_key() {
  char *access_secret_key = std::getenv("AWS_SECRET_ACCESS_KEY");
  if (access_secret_key) {
    return access_secret_key;
  }
  access_secret_key = std::getenv("SECRET_ACCESS_KEY");
  if (access_secret_key) {
    return access_secret_key;
  }
  return nullptr;
}

S3ObjectStore *create_s3_objstore_helper(
    const std::string_view region, const std::string_view *endpoint,
    char *access_key_id, char *access_secret_key, bool use_https,
    std::string &err_msg) {
  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.region = region;
  if (endpoint != nullptr) {
    clientConfig.endpointOverride = *endpoint;
  }
  clientConfig.scheme =
      use_https ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;

  if (access_key_id && access_secret_key) {
    // if both access_key_id and access_secret_key are not empty, we use them to
    // create the client.
    Aws::String access_key_id_str(access_key_id);
    Aws::String access_secret_key_str(access_secret_key);
    Aws::Auth::AWSCredentials credentials(access_key_id_str,
                                          access_secret_key_str);
    Aws::S3::S3Client client(credentials, nullptr, clientConfig);
    return new S3ObjectStore(region, std::move(client));
  } else if (access_key_id || access_secret_key) {
    // if one of the access_key_id and access_secret_key is empty, we treat it
    // as an invalid input.
    err_msg =
        "access key id and access secret key environment variables should be "
        "both set or both empty for s3";
    return nullptr;
  } else {
    // if both access_key_id and access_secret_key are empty, will find the
    // credentials by default credential provider chain for AWS. see
    // https://github.com/aws/aws-sdk-cpp/blob/main/docs/Credentials_Providers.md
    // for details
    Aws::S3::S3Client client(clientConfig);
    return new S3ObjectStore(region, std::move(client));
  }
}

S3ObjectStore *create_s3_objstore(const std::string_view region,
                                  const std::string_view *endpoint,
                                  bool use_https, std::string &err_msg) {
  char *access_key_id = get_s3_access_key_id();
  char *access_secret_key = get_s3_access_secret_key();
  S3ObjectStore *s3_objstore =
      create_s3_objstore_helper(region, endpoint, access_key_id,
                                access_secret_key, use_https, err_msg);
  if (!s3_objstore) {
    err_msg = "failed to create s3 object store:" + err_msg;
  }
  return s3_objstore;
}

S3ObjectStore *create_source_s3_objstore(const std::string_view region,
                                         const std::string_view *endpoint,
                                         bool use_https, std::string &err_msg) {
  char *source_access_key_id = get_src_access_key_id();
  char *source_access_secret_key = get_src_access_secret_key();
  S3ObjectStore *s3_objstore = create_s3_objstore_helper(
      region, endpoint, source_access_key_id, source_access_secret_key,
      use_https, err_msg);
  if (!s3_objstore) {
    err_msg = "failed to create source s3 object store:" + err_msg;
  }
  return s3_objstore;
}

S3ObjectStore *create_dest_s3_objstore(const std::string_view region,
                                       const std::string_view *endpoint,
                                       bool use_https, std::string &err_msg) {
  char *dest_access_key_id = get_dest_access_key_id();
  char *dest_access_secret_key = get_dest_access_secret_key();
  S3ObjectStore *s3_objstore =
      create_s3_objstore_helper(region, endpoint, dest_access_key_id,
                                dest_access_secret_key, use_https, err_msg);
  if (!s3_objstore) {
    err_msg = "failed to create destination s3 object store:" + err_msg;
  }
  return s3_objstore;
}

void destroy_s3_objstore(S3ObjectStore *s3_objstore) {
  if (s3_objstore) {
    delete s3_objstore;
  }
  return;
}

}  // namespace objstore
