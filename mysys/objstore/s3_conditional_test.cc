/*
   Copyright (c) 2026, ApeCloud Inc Holding Limited.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.
*/

#include "mysys/objstore/s3.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

namespace fs = std::filesystem;

void expect(bool condition, std::string_view message) {
  if (!condition) throw std::runtime_error(std::string(message));
}

class TempFile {
 public:
  explicit TempFile(std::string_view body) {
    path_ = fs::temp_directory_path() /
            ("wesql-s3-conditional-test-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    std::ofstream output(path_, std::ios::binary | std::ios::trunc);
    output.write(body.data(), static_cast<std::streamsize>(body.size()));
    if (!output) throw std::runtime_error("failed to create test input file");
  }

  ~TempFile() {
    std::error_code error;
    fs::remove(path_, error);
  }

  const fs::path &path() const { return path_; }

 private:
  fs::path path_;
};

class TempDirectory {
 public:
  TempDirectory() {
    path_ = fs::temp_directory_path() /
            ("wesql-s3-conditional-test-dir-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    if (!fs::create_directory(path_)) {
      throw std::runtime_error("failed to create test directory");
    }
  }

  ~TempDirectory() {
    std::error_code error;
    fs::remove_all(path_, error);
  }

  const fs::path &path() const { return path_; }

 private:
  fs::path path_;
};

std::string read_file(const fs::path &path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>{input},
          std::istreambuf_iterator<char>{}};
}

bool file_is_repeated_byte(const fs::path &path, uint64_t expected_size,
                           char expected) {
  std::ifstream input(path, std::ios::binary);
  std::array<char, 8192> buffer{};
  uint64_t total = 0;
  while (input) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const std::streamsize count = input.gcount();
    if (count < 0 ||
        !std::all_of(buffer.begin(), buffer.begin() + count,
                     [expected](char value) { return value == expected; })) {
      return false;
    }
    total += static_cast<uint64_t>(count);
  }
  return input.eof() && total == expected_size;
}

class ScriptedS3ObjectStore final : public objstore::S3ObjectStore {
 public:
  ScriptedS3ObjectStore() : S3ObjectStore("us-east-1", make_client()) {}

  void put_success(std::string etag = "\"put-etag\"") {
    put_succeeds_ = true;
    put_etag_ = std::move(etag);
  }

  void put_failure(int http_status, bool retryable) {
    put_succeeds_ = false;
    put_http_status_ = http_status;
    put_retryable_ = retryable;
  }

  void get_success(std::string body, std::string etag,
                   long long content_length) {
    get_succeeds_ = true;
    get_body_ = std::move(body);
    get_etag_ = std::move(etag);
    get_content_length_ = content_length;
    get_generated_size_ = 0;
    get_failure_body_.clear();
  }

  void get_generated_success(uint64_t size, char byte, std::string etag) {
    get_succeeds_ = true;
    get_body_.clear();
    get_etag_ = std::move(etag);
    get_content_length_ = static_cast<long long>(size);
    get_generated_size_ = size;
    get_generated_byte_ = byte;
    get_failure_body_.clear();
  }

  void get_failure(int http_status, bool retryable,
                   Aws::S3::S3Errors error_type = Aws::S3::S3Errors::UNKNOWN) {
    get_succeeds_ = false;
    get_http_status_ = http_status;
    get_retryable_ = retryable;
    get_error_type_ = error_type;
    get_generated_size_ = 0;
    get_failure_body_.clear();
  }

  void get_failure_after_write(
      std::string partial_body, int http_status, bool retryable,
      Aws::S3::S3Errors error_type = Aws::S3::S3Errors::UNKNOWN) {
    get_failure(http_status, retryable, error_type);
    get_failure_body_ = std::move(partial_body);
  }

  int put_calls() const { return put_calls_; }
  int get_calls() const { return get_calls_; }
  int get_write_chunks() const { return get_write_chunks_; }
  const std::string &last_put_body() const { return last_put_body_; }
  long long last_content_length() const { return last_content_length_; }
  const Aws::Http::HeaderValueCollection &last_headers() const {
    return last_headers_;
  }

 protected:
  Aws::S3::Model::PutObjectOutcome do_put_object(
      const Aws::S3::Model::PutObjectRequest &request) override {
    ++put_calls_;
    last_headers_ = request.GetAdditionalCustomHeaders();
    last_content_length_ = request.GetContentLength();

    const std::shared_ptr<Aws::IOStream> input = request.GetBody();
    if (!input) throw std::runtime_error("conditional PUT has no body stream");
    last_put_body_.assign(std::istreambuf_iterator<char>{*input},
                          std::istreambuf_iterator<char>{});
    if (input->bad()) throw std::runtime_error("failed to consume PUT body");

    if (put_succeeds_) {
      Aws::S3::Model::PutObjectResult result;
      result.SetETag(put_etag_.c_str());
      return Aws::S3::Model::PutObjectOutcome(std::move(result));
    }
    return Aws::S3::Model::PutObjectOutcome(make_error(
        Aws::S3::S3Errors::UNKNOWN, put_http_status_, put_retryable_));
  }

  Aws::S3::Model::GetObjectOutcome do_get_object(
      const Aws::S3::Model::GetObjectRequest &request) override {
    ++get_calls_;
    get_write_chunks_ = 0;
    if (!get_succeeds_) {
      if (!get_failure_body_.empty()) {
        auto *body = request.GetResponseStreamFactory()();
        if (body == nullptr) {
          throw std::runtime_error("GET response factory returned null");
        }
        body->write(
            get_failure_body_.data(),
            static_cast<std::streamsize>(get_failure_body_.size()));
        body->flush();
        ++get_write_chunks_;
        if (!*body) {
          Aws::Delete(body);
          return Aws::S3::Model::GetObjectOutcome(
              make_error(Aws::S3::S3Errors::UNKNOWN, -1, true));
        }
        body->seekg(0);
        const std::string readable{std::istreambuf_iterator<char>{*body},
                                   std::istreambuf_iterator<char>{}};
        expect(readable == get_failure_body_.substr(0, 64 * 1024),
               "SDK cannot read the complete bounded error response");
        Aws::Delete(body);
      }
      return Aws::S3::Model::GetObjectOutcome(
          make_error(get_error_type_, get_http_status_, get_retryable_));
    }

    auto *body = request.GetResponseStreamFactory()();
    if (body == nullptr)
      throw std::runtime_error("GET response factory returned null");
    if (get_generated_size_ == 0) {
      body->write(get_body_.data(),
                  static_cast<std::streamsize>(get_body_.size()));
      ++get_write_chunks_;
    } else {
      constexpr size_t kChunkSize = 4096;
      const std::array<char, kChunkSize> chunk = [](char byte) {
        std::array<char, kChunkSize> value{};
        value.fill(byte);
        return value;
      }(get_generated_byte_);
      uint64_t remaining = get_generated_size_;
      while (remaining > 0) {
        const size_t count = static_cast<size_t>(
            std::min<uint64_t>(remaining, chunk.size()));
        body->write(chunk.data(), static_cast<std::streamsize>(count));
        remaining -= count;
        ++get_write_chunks_;
      }
    }
    body->flush();
    if (!*body) {
      Aws::Delete(body);
      return Aws::S3::Model::GetObjectOutcome(
          make_error(Aws::S3::S3Errors::UNKNOWN, -1, true));
    }

    Aws::S3::Model::GetObjectResult result;
    result.ReplaceBody(body);
    result.SetETag(get_etag_.c_str());
    result.SetContentLength(get_content_length_);
    return Aws::S3::Model::GetObjectOutcome(std::move(result));
  }

 private:
  static Aws::S3::S3Client make_client() {
    Aws::S3::S3ClientConfiguration configuration;
    configuration.region = "us-east-1";
    configuration.endpointOverride = "http://127.0.0.1:1";
    const Aws::Auth::AWSCredentials credentials("test-access-key",
                                                "test-secret-key");
    return Aws::S3::S3Client(credentials, nullptr, configuration);
  }

  static Aws::S3::S3Error make_error(Aws::S3::S3Errors error_type,
                                     int http_status, bool retryable) {
    Aws::Client::AWSError<Aws::S3::S3Errors> base_error(
        error_type, "ScriptedFailure", "scripted request failure", retryable);
    Aws::S3::S3Error error(std::move(base_error));
    error.SetResponseCode(
        static_cast<Aws::Http::HttpResponseCode>(http_status));
    return error;
  }

  bool put_succeeds_{true};
  int put_http_status_{200};
  bool put_retryable_{false};
  std::string put_etag_{"\"put-etag\""};
  int put_calls_{0};
  std::string last_put_body_;
  long long last_content_length_{0};
  Aws::Http::HeaderValueCollection last_headers_;

  bool get_succeeds_{true};
  int get_http_status_{200};
  bool get_retryable_{false};
  Aws::S3::S3Errors get_error_type_{Aws::S3::S3Errors::UNKNOWN};
  std::string get_body_;
  std::string get_etag_{"\"get-etag\""};
  long long get_content_length_{0};
  uint64_t get_generated_size_{0};
  char get_generated_byte_{0};
  std::string get_failure_body_;
  int get_calls_{0};
  int get_write_chunks_{0};
};

std::string_view header_value(const Aws::Http::HeaderValueCollection &headers,
                              std::string_view name) {
  const auto iter = headers.find(Aws::String(name.data(), name.size()));
  return iter == headers.end()
             ? std::string_view{}
             : std::string_view(iter->second.data(), iter->second.size());
}

void test_conditional_put_requests() {
  ScriptedS3ObjectStore store;
  expect(store.conditional_capabilities().supports_remote_commit_io(),
         "S3 conditional capabilities are incomplete");

  const std::string binary_body{"a\0bc", 4};
  store.put_success("\"created\"");
  auto result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::create_only());
  expect(result.outcome() == objstore::ConditionalPutOutcome::APPLIED &&
             result.etag() == "\"created\"" && store.put_calls() == 1 &&
             store.last_put_body() == binary_body &&
             store.last_content_length() == 4 &&
             header_value(store.last_headers(), "if-none-match") == "*" &&
             header_value(store.last_headers(), "if-match").empty(),
         "create-only PUT request is not exact");

  store.put_success();
  result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::match_etag("\"opaque-etag\""));
  expect(
      result.is_applied() && store.put_calls() == 2 &&
          store.last_put_body() == binary_body &&
          header_value(store.last_headers(), "if-match") == "\"opaque-etag\"" &&
          header_value(store.last_headers(), "if-none-match").empty(),
      "match-ETag PUT changed the token or body");

  const int calls_before_invalid = store.put_calls();
  result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::match_etag(""));
  expect(result.outcome() == objstore::ConditionalPutOutcome::PERMANENT_ERROR &&
             store.put_calls() == calls_before_invalid,
         "invalid condition reached the S3 client");

  store.put_failure(409, false);
  result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::create_only());
  expect(result.outcome() == objstore::ConditionalPutOutcome::CONFLICT_409,
         "HTTP 409 was not preserved");
  store.put_failure(412, false);
  result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::create_only());
  expect(result.outcome() ==
             objstore::ConditionalPutOutcome::PRECONDITION_FAILED_412,
         "HTTP 412 was not preserved");
  store.put_failure(403, false);
  result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::create_only());
  expect(result.outcome() == objstore::ConditionalPutOutcome::PERMANENT_ERROR,
         "HTTP 403 was not permanent");
  store.put_failure(403, true);
  result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::create_only());
  expect(result.outcome() == objstore::ConditionalPutOutcome::PERMANENT_ERROR,
         "SDK retryability overrode HTTP 403");

  store.put_failure(-1, true);
  const int calls_before_unknown = store.put_calls();
  result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::create_only());
  expect(
      result.outcome() == objstore::ConditionalPutOutcome::TRANSPORT_UNKNOWN &&
          store.put_calls() == calls_before_unknown + 1 &&
          store.last_put_body() == binary_body,
      "transport-unknown PUT retried or lost body bytes");
  result = store.put_object_conditional(
      "bucket", "prefix/key", binary_body,
      objstore::ConditionalPutCondition::create_only());
  expect(store.put_calls() == calls_before_unknown + 2 &&
             store.last_put_body() == binary_body,
         "caller-driven retry reused a consumed body stream");
}

void test_conditional_file_put() {
  ScriptedS3ObjectStore store;
  const std::string binary_body{"file\0body", 9};
  TempFile input(binary_body);

  store.put_failure(503, true);
  const auto condition = objstore::ConditionalPutCondition::create_only();
  auto result = store.put_object_from_file_conditional(
      "bucket", "snapshot/object", input.path().string(), condition);
  expect(
      result.outcome() == objstore::ConditionalPutOutcome::TRANSPORT_UNKNOWN &&
          store.put_calls() == 1 && store.last_put_body() == binary_body &&
          store.last_content_length() ==
              static_cast<long long>(binary_body.size()) &&
          header_value(store.last_headers(), "if-none-match") == "*",
      "conditional file PUT did not make one exact attempt");

  store.put_success();
  result = store.put_object_from_file_conditional(
      "bucket", "snapshot/object", input.path().string(), condition);
  expect(result.is_applied() && store.put_calls() == 2 &&
             store.last_put_body() == binary_body,
         "conditional file retry did not reopen the complete input");
}

void test_exact_get_results() {
  ScriptedS3ObjectStore store;

  store.get_success("", "\"empty\"", 0);
  auto result = store.get_object_exact("bucket", "head");
  expect(result.outcome() == objstore::ExactObjectOutcome::FOUND &&
             result.body().empty() && result.size() == 0 &&
             result.etag() == "\"empty\"" && store.get_calls() == 1,
         "empty exact GET was not returned as FOUND");

  const std::string binary_body{"get\0body", 8};
  store.get_success(binary_body, "\"binary\"", binary_body.size());
  result = store.get_object_exact("bucket", "head");
  expect(result.is_found() && result.body() == binary_body &&
             result.size() == binary_body.size() &&
             result.etag() == "\"binary\"",
         "exact GET was not binary-safe");

  store.get_success("body", "\"bad-length\"", 3);
  result = store.get_object_exact("bucket", "head");
  expect(result.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR &&
             result.body().empty() && result.etag().empty(),
         "content-length mismatch retained an untrusted result");

  store.get_failure(404, false, Aws::S3::S3Errors::NO_SUCH_KEY);
  result = store.get_object_exact("bucket", "head");
  expect(result.outcome() == objstore::ExactObjectOutcome::NOT_FOUND_404 &&
             result.body().empty() && result.etag().empty(),
         "HTTP 404 exact GET was not distinct");
  store.get_failure(404, false, Aws::S3::S3Errors::NO_SUCH_BUCKET);
  result = store.get_object_exact("bucket", "head");
  expect(
      result.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR &&
          result.status().error_code() == objstore::Errors::SE_NO_SUCH_BUCKET,
      "missing bucket was confused with an absent object");
  store.get_failure(-1, false);
  result = store.get_object_exact("bucket", "head");
  expect(
      result.outcome() == objstore::ExactObjectOutcome::TRANSIENT_UNAVAILABLE,
      "request-not-made exact GET was not transient");
  store.get_failure(503, true);
  result = store.get_object_exact("bucket", "head");
  expect(
      result.outcome() == objstore::ExactObjectOutcome::TRANSIENT_UNAVAILABLE,
      "retry-exhausted exact GET was not transient");
  store.get_failure(503, false, Aws::S3::S3Errors::NO_SUCH_KEY);
  result = store.get_object_exact("bucket", "head");
  expect(
      result.outcome() == objstore::ExactObjectOutcome::TRANSIENT_UNAVAILABLE,
      "SDK error enum overrode HTTP 503");
  store.get_failure(403, true, Aws::S3::S3Errors::NO_SUCH_KEY);
  result = store.get_object_exact("bucket", "head");
  expect(result.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR,
         "SDK metadata overrode HTTP 403");
  store.get_failure(403, false);
  result = store.get_object_exact("bucket", "head");
  expect(result.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR,
         "HTTP 403 exact GET was not permanent");
}

void test_bounded_exact_get_results() {
  ScriptedS3ObjectStore store;

  const int calls_before_invalid = store.get_calls();
  auto result = store.get_object_exact("bucket", "head", 0);
  expect(result.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR &&
             result.status().error_code() == objstore::Errors::SE_INVALID &&
             store.get_calls() == calls_before_invalid,
         "zero exact GET limit reached the S3 client");

  store.get_success("", "\"declared-large\"", 9);
  result = store.get_object_exact("bucket", "head", 8);
  expect(result.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR &&
             result.status().error_code() == objstore::Errors::SE_UNEXPECTED &&
             result.body().empty() && result.etag().empty(),
         "declared oversized exact GET was accepted");

  const std::string boundary{"1234\0\0\0\0", 8};
  store.get_success(boundary, "\"boundary\"", boundary.size());
  result = store.get_object_exact("bucket", "head", boundary.size());
  expect(result.is_found() && result.body() == boundary &&
             result.size() == boundary.size() &&
             result.etag() == "\"boundary\"",
         "exact GET rejected the byte-limit boundary");

  store.get_success("123456789", "\"one-over\"", 9);
  result = store.get_object_exact("bucket", "head", 8);
  expect(result.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR &&
             result.status().error_code() == objstore::Errors::SE_UNEXPECTED &&
             result.body().empty() && result.etag().empty(),
         "one-byte oversized exact GET retained response bytes");

  store.get_success("123456789", "\"stream-over\"", 8);
  result = store.get_object_exact("bucket", "head", 8);
  expect(result.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR &&
             result.body().empty() && result.etag().empty(),
         "bounded stream trusted an undersized Content-Length");
}

void test_exact_streaming_get_results() {
  ScriptedS3ObjectStore store;
  TempFile destination("");
  const std::string binary_body{"stream\0body", 11};

  store.get_success(binary_body, "\"streamed\"", binary_body.size());
  auto result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string());
  std::ifstream input(destination.path(), std::ios::binary);
  const std::string downloaded{std::istreambuf_iterator<char>{input},
                               std::istreambuf_iterator<char>{}};
  expect(result.is_applied() && result.size() == binary_body.size() &&
             result.etag() == "\"streamed\"" && downloaded == binary_body,
         "exact streaming GET did not preserve the full object");

  {
    std::ofstream reset(destination.path(), std::ios::binary | std::ios::trunc);
  }
  store.get_failure_after_write("partial-404", 404, false,
                                Aws::S3::S3Errors::NO_SUCH_KEY);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string());
  expect(result.outcome() == objstore::ExactFileOutcome::NOT_FOUND_404 &&
             fs::file_size(destination.path()) == 0,
         "exact streaming GET did not preserve 404 or truncate output");

  store.get_failure_after_write("partial-transient", 503, false);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string());
  expect(result.outcome() ==
             objstore::ExactFileOutcome::TRANSIENT_UNAVAILABLE &&
             fs::file_size(destination.path()) == 0,
         "transient streaming GET retained partial output");

  store.get_failure_after_write("partial-permanent", 403, true);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             fs::file_size(destination.path()) == 0,
         "permanent streaming GET retained partial output");

  store.get_failure_after_write("partial-missing-bucket", 404, false,
                                Aws::S3::S3Errors::NO_SUCH_BUCKET);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             result.status().error_code() ==
                 objstore::Errors::SE_NO_SUCH_BUCKET &&
             fs::file_size(destination.path()) == 0,
         "missing bucket was mapped as an absent streamed object");

  store.get_success("short", "\"bad-length\"", 99);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             fs::file_size(destination.path()) == 0,
         "stream length mismatch retained partial output");

  store.get_success("too-long", "\"bad-length\"", 2);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             fs::file_size(destination.path()) == 0,
         "oversized stream retained mismatched output");

  store.get_success("untrusted", "", 9);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             fs::file_size(destination.path()) == 0,
         "invalid streaming GET ETag retained output");
}

void test_exact_streaming_get_destination_validation() {
  ScriptedS3ObjectStore store;
  TempDirectory temp;
  const fs::path nonempty = temp.path() / "nonempty";
  {
    std::ofstream output(nonempty, std::ios::binary);
    output << "preserve";
  }

  store.get_success("body", "\"etag\"", 4);
  const int calls_before_invalid = store.get_calls();
  auto result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", nonempty.string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             result.status().error_code() == objstore::Errors::SE_INVALID &&
             store.get_calls() == calls_before_invalid &&
             read_file(nonempty) == "preserve",
         "nonempty exact GET destination was modified or sent to S3");

  const fs::path missing = temp.path() / "missing";
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", missing.string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             store.get_calls() == calls_before_invalid && !fs::exists(missing),
         "missing exact GET destination was accepted");

  const fs::path directory = temp.path() / "directory";
  fs::create_directory(directory);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", directory.string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             store.get_calls() == calls_before_invalid &&
             fs::is_directory(directory),
         "directory exact GET destination was accepted");

  const fs::path symlink_target = temp.path() / "symlink-target";
  {
    std::ofstream output(symlink_target, std::ios::binary);
  }
  const fs::path symlink = temp.path() / "symlink";
  fs::create_symlink(symlink_target, symlink);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", symlink.string());
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             store.get_calls() == calls_before_invalid &&
             fs::file_size(symlink_target) == 0,
         "symlink exact GET destination was accepted");
}

void test_exact_streaming_get_without_body_buffer() {
  ScriptedS3ObjectStore store;
  TempFile destination("");
  constexpr uint64_t kSize = 4 * 1024 * 1024 + 17;
  constexpr char kByte = static_cast<char>(0xa5);

  store.get_generated_success(kSize, kByte, "\"generated\"");
  const auto result = store.get_object_to_file_exact(
      "bucket", "snapshot/large-object", destination.path().string());
  expect(result.is_applied() && result.size() == kSize &&
             result.etag() == "\"generated\"" &&
             store.get_write_chunks() > 1 &&
             file_is_repeated_byte(destination.path(), kSize, kByte),
         "streaming GET required a whole response body or changed bytes");
}

void test_bounded_exact_streaming_get_results() {
  ScriptedS3ObjectStore store;
  TempFile destination("");

  auto result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string(), 0);
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             result.status().error_code() == objstore::Errors::SE_INVALID &&
             store.get_calls() == 0 && fs::file_size(destination.path()) == 0,
         "zero bounded exact file GET limit reached S3");

  store.get_success("12345678", "\"boundary\"", 8);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string(), 8);
  expect(result.is_applied() && result.size() == 8 &&
             read_file(destination.path()) == "12345678",
         "bounded exact file GET rejected the byte boundary");

  {
    std::ofstream reset(destination.path(), std::ios::binary | std::ios::trunc);
  }
  store.get_success("123456789", "\"one-over\"", 9);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string(), 8);
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             result.status().cloud_provider_err_code() == EFBIG &&
             fs::file_size(destination.path()) == 0,
         "one-byte oversized exact file GET retained response bytes");

  store.get_success("123456789", "\"stream-over\"", 8);
  result = store.get_object_to_file_exact(
      "bucket", "snapshot/object", destination.path().string(), 8);
  expect(result.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR &&
             result.status().cloud_provider_err_code() == EFBIG &&
             fs::file_size(destination.path()) == 0,
         "bounded exact file stream trusted an undersized Content-Length");
}

void test_bounded_error_responses() {
  ScriptedS3ObjectStore store;
  TempFile destination("");
  for (const int status : {404, 403, 503}) {
    for (const auto kind : {Aws::S3::S3Errors::NO_SUCH_KEY,
                            Aws::S3::S3Errors::NO_SUCH_BUCKET}) {
      store.get_failure_after_write(std::string(512, 'x'), status, status == 503,
                                    kind);
      const auto memory = store.get_object_exact("bucket", "key", 1);
      const auto file = store.get_object_to_file_exact(
          "bucket", "key", destination.path().string(), 1);
      const bool missing = status == 404 && kind == Aws::S3::S3Errors::NO_SUCH_KEY;
      expect((memory.outcome() == objstore::ExactObjectOutcome::NOT_FOUND_404) == missing &&
                 (file.outcome() == objstore::ExactFileOutcome::NOT_FOUND_404) == missing &&
                 !memory.is_found() && !file.is_applied() &&
                 file.status().cloud_provider_err_code() == status &&
                 fs::file_size(destination.path()) == 0,
             "bounded error body lost HTTP or bucket/key classification");
    }
  }
  for (const uint64_t payload_limit : {1, 128 * 1024}) {
    for (const size_t error_size : {64 * 1024, 64 * 1024 + 1}) {
      store.get_failure_after_write(std::string(error_size, 'x'), 404, false,
                                    Aws::S3::S3Errors::NO_SUCH_KEY);
      const auto memory = store.get_object_exact("bucket", "key", payload_limit);
      const auto file = store.get_object_to_file_exact(
          "bucket", "key", destination.path().string(), payload_limit);
      expect(memory.outcome() == (error_size == 64 * 1024
                 ? objstore::ExactObjectOutcome::NOT_FOUND_404
                 : objstore::ExactObjectOutcome::PERMANENT_ERROR) &&
                 file.outcome() == (error_size == 64 * 1024
                 ? objstore::ExactFileOutcome::NOT_FOUND_404
                 : objstore::ExactFileOutcome::PERMANENT_ERROR) &&
                 fs::file_size(destination.path()) == 0,
             "error response bound depends on the object payload limit");
    }
  }
}

void test_http_exact_get_results(const char *endpoint) {
  Aws::Client::ClientConfiguration configuration;
  configuration.region = "us-east-1";
  configuration.endpointOverride = endpoint;
  configuration.connectTimeoutMs = 2000;
  configuration.requestTimeoutMs = 5000;
  const Aws::Auth::AWSCredentials credentials("http-test", "http-test");
  Aws::S3::S3Client client(credentials, configuration,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
  objstore::S3ObjectStore store("us-east-1", std::move(client));
  TempFile destination("");
  for (const std::string key : {"missing-key", "missing-bucket", "forbidden",
                                "one-byte", "oversized"}) {
    const auto memory = store.get_object_exact("bucket", key, 1);
    const auto file = store.get_object_to_file_exact(
        "bucket", key, destination.path().string(), 1);
    if (key == "missing-key") {
      expect(memory.outcome() == objstore::ExactObjectOutcome::NOT_FOUND_404 &&
                 file.outcome() == objstore::ExactFileOutcome::NOT_FOUND_404,
             "HTTP missing key did not remain 404 under a one-byte limit");
    } else if (key == "one-byte") {
      expect(memory.is_found() && memory.body() == "x" && file.is_applied() &&
                 read_file(destination.path()) == "x",
             "HTTP exact one-byte object failed");
      std::ofstream reset(destination.path(), std::ios::binary | std::ios::trunc);
    } else {
      expect(memory.outcome() == objstore::ExactObjectOutcome::PERMANENT_ERROR &&
                 file.outcome() == objstore::ExactFileOutcome::PERMANENT_ERROR,
             "HTTP bucket/access/oversized response was not rejected");
    }
    expect(fs::file_size(destination.path()) == 0,
           "HTTP failure retained destination bytes");
  }
}

}  // namespace

int main() {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  int exit_code = 0;
  try {
    test_conditional_put_requests();
    test_conditional_file_put();
    test_exact_get_results();
    test_bounded_exact_get_results();
    test_exact_streaming_get_results();
    test_exact_streaming_get_destination_validation();
    test_exact_streaming_get_without_body_buffer();
    test_bounded_exact_streaming_get_results();
    test_bounded_error_responses();
    if (const char *endpoint = std::getenv("WESQL_S3_TEST_ENDPOINT"))
      test_http_exact_get_results(endpoint);
    std::cout << "S3 conditional ObjectStore tests passed\n";
  } catch (const std::exception &error) {
    std::cerr << "S3 conditional ObjectStore test failed: " << error.what()
              << '\n';
    exit_code = 1;
  }
  Aws::ShutdownAPI(options);
  return exit_code;
}
