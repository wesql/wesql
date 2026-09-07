/*
   Copyright (c) 2026, ApeCloud Inc Holding Limited.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.
*/

#include "mysys/objstore/local.h"
#include "mysys/objstore/provider_lifecycle.h"
#include "mysys/objstore/s3_error.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace fs = std::filesystem;
using objstore::Errors;
using objstore::ExactObjectOutcome;
using objstore::LocalObjectStore;
using objstore::ObjectMeta;
using objstore::ObjectStore;
using objstore::Status;

std::atomic<unsigned int> provider_initialize_calls{0};
std::atomic<unsigned int> provider_shutdown_calls{0};

void record_provider_initialize() {
  provider_initialize_calls.fetch_add(1, std::memory_order_relaxed);
}

void record_provider_shutdown() {
  provider_shutdown_calls.fetch_add(1, std::memory_order_relaxed);
}

template <typename T>
concept SupportsProviderCleanup = requires(T value) {
  objstore::cleanup_objstore_provider(value);
};

static_assert(!SupportsProviderCleanup<ObjectStore *>);

class TempDirectory {
 public:
  TempDirectory() {
    const auto suffix = std::chrono::steady_clock::now()
                            .time_since_epoch()
                            .count();
    path_ = fs::temp_directory_path() /
            ("wesql-objstore-local-test-" + std::to_string(suffix));
    if (!fs::create_directories(path_)) {
      throw std::runtime_error("failed to create temporary directory");
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

void write_file(const fs::path &path, std::string_view data) {
  fs::create_directories(path.parent_path());
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  output.write(data.data(), static_cast<std::streamsize>(data.size()));
  if (!output) throw std::runtime_error("failed to write test file");
}

std::string read_file(const fs::path &path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>{input},
          std::istreambuf_iterator<char>{}};
}

void expect(bool condition, std::string_view message) {
  if (!condition) throw std::runtime_error(std::string(message));
}

void expect_success(const Status &status, std::string_view operation) {
  if (!status.is_succ()) {
    throw std::runtime_error(std::string(operation) + ": " +
                             std::string(status.error_message()));
  }
}

bool contains_key(const std::vector<ObjectMeta> &objects,
                  std::string_view key) {
  for (const ObjectMeta &object : objects) {
    if (object.key == key) return true;
  }
  return false;
}

class TraversalListingStore final : public LocalObjectStore {
 public:
  using LocalObjectStore::LocalObjectStore;

  Status list_object(const std::string_view &, const std::string_view &prefix,
                     bool, std::string &, bool &finished,
                     std::vector<ObjectMeta> &objects) override {
    objects = {{std::string(prefix) + "../escaped.txt", 0, 7}};
    finished = true;
    return Status();
  }

  Status get_object_to_file(const std::string_view &, const std::string_view &,
                            const std::string_view &output_path) override {
    write_file(fs::path{std::string(output_path)}, "escaped");
    return Status();
  }
};

class FixedListingStore final : public LocalObjectStore {
 public:
  FixedListingStore(std::string_view root, std::string key)
      : LocalObjectStore(root), key_(std::move(key)) {}

  Status list_object(const std::string_view &, const std::string_view &, bool,
                     std::string &, bool &finished,
                     std::vector<ObjectMeta> &objects) override {
    objects = {{key_, 0, 7}};
    finished = true;
    return Status();
  }

  Status get_object_to_file(const std::string_view &, const std::string_view &,
                            const std::string_view &output_path) override {
    write_file(fs::path{std::string(output_path)}, "escaped");
    return Status();
  }

 private:
  std::string key_;
};

void test_conditional_protocol_contract() {
  TempDirectory temp;
  LocalObjectStore local((temp.path() / "store").string());

  const auto capabilities = local.conditional_capabilities();
  expect(!capabilities.supports_remote_commit_io(),
         "local provider claimed remote-commit conditional support");

  const auto unsupported_get = local.get_object_exact("bucket", "key");
  expect(unsupported_get.outcome() == ExactObjectOutcome::UNSUPPORTED &&
             unsupported_get.body().empty() && unsupported_get.etag().empty() &&
             unsupported_get.size() == 0 &&
             unsupported_get.status().error_code() ==
                 Errors::SE_CONDITIONAL_OPERATION_NOT_SUPPORTED,
         "unsupported exact GET retained output or returned the wrong state");

  const auto create_only = objstore::ConditionalPutCondition::create_only();
  expect(create_only.is_valid(), "create-only condition is invalid");
  const auto unsupported_put =
      local.put_object_conditional("bucket", "key", "", create_only);
  expect(unsupported_put.outcome() ==
                 objstore::ConditionalPutOutcome::UNSUPPORTED &&
             unsupported_put.etag().empty() &&
             unsupported_put.status().error_code() ==
                 Errors::SE_CONDITIONAL_OPERATION_NOT_SUPPORTED,
         "unsupported conditional PUT retained output or returned wrong state");
  const auto unsupported_file = local.put_object_from_file_conditional(
      "bucket", "key", (temp.path() / "input").string(), create_only);
  expect(unsupported_file.outcome() ==
             objstore::ConditionalPutOutcome::UNSUPPORTED,
         "local provider accepted conditional file creation");

  const auto quoted_etag =
      objstore::ConditionalPutCondition::match_etag("\"opaque-tag\"");
  expect(quoted_etag.is_valid() && quoted_etag.etag() == "\"opaque-tag\"",
         "quoted ETag was not preserved exactly");
  expect(
      !objstore::ConditionalPutCondition::match_etag("").is_valid() &&
          !objstore::ConditionalPutCondition::match_etag(" bad ").is_valid() &&
          !objstore::ConditionalPutCondition::match_etag("bad\r\ntag")
               .is_valid(),
      "invalid match-ETag condition was accepted");

  const std::string binary_body{"a\0b", 3};
  const auto found =
      objstore::ExactObjectResult::found(binary_body, "\"binary-tag\"");
  expect(found.is_found() && found.body() == binary_body && found.size() == 3 &&
             found.etag() == "\"binary-tag\"",
         "exact GET result is not binary-safe");
  const auto empty = objstore::ExactObjectResult::found("", "\"empty-tag\"");
  expect(empty.is_found() && empty.body().empty() && empty.size() == 0,
         "empty exact object was confused with a missing object");
  const auto invalid_etag = objstore::ExactObjectResult::found("stale", "");
  expect(invalid_etag.outcome() == ExactObjectOutcome::PERMANENT_ERROR &&
             invalid_etag.body().empty() && invalid_etag.etag().empty(),
         "invalid exact GET result retained an untrusted body");

  using objstore::s3_detail::ObjectErrorKind;
  expect(objstore::s3_detail::classify_exact_get_failure(
             404, false, ObjectErrorKind::OTHER) ==
             ExactObjectOutcome::NOT_FOUND_404,
         "HTTP 404 exact GET classification is wrong");
  expect(objstore::s3_detail::classify_exact_get_failure(
             404, false, ObjectErrorKind::NO_SUCH_BUCKET) ==
             ExactObjectOutcome::PERMANENT_ERROR,
         "missing bucket was confused with a missing object");
  expect(objstore::s3_detail::classify_exact_get_failure(
             -1, false, ObjectErrorKind::OTHER) ==
             ExactObjectOutcome::TRANSIENT_UNAVAILABLE,
         "request-not-made exact GET classification is wrong");
  expect(objstore::s3_detail::classify_exact_get_failure(
             503, true, ObjectErrorKind::OTHER) ==
             ExactObjectOutcome::TRANSIENT_UNAVAILABLE,
         "retry-exhausted exact GET classification is wrong");
  expect(objstore::s3_detail::classify_exact_get_failure(
             503, false, ObjectErrorKind::NO_SUCH_KEY) ==
             ExactObjectOutcome::TRANSIENT_UNAVAILABLE,
         "SDK error enum overrode a transient HTTP status");
  expect(objstore::s3_detail::classify_exact_get_failure(
             403, true, ObjectErrorKind::NO_SUCH_KEY) ==
             ExactObjectOutcome::PERMANENT_ERROR,
         "SDK metadata overrode a permanent HTTP status");
  expect(
      objstore::s3_detail::classify_conditional_put_failure(409, false) ==
              objstore::ConditionalPutOutcome::CONFLICT_409 &&
          objstore::s3_detail::classify_conditional_put_failure(412, false) ==
              objstore::ConditionalPutOutcome::PRECONDITION_FAILED_412 &&
          objstore::s3_detail::classify_conditional_put_failure(503, true) ==
              objstore::ConditionalPutOutcome::TRANSPORT_UNKNOWN &&
          objstore::s3_detail::classify_conditional_put_failure(403, false) ==
              objstore::ConditionalPutOutcome::PERMANENT_ERROR &&
          objstore::s3_detail::classify_conditional_put_failure(403, true) ==
              objstore::ConditionalPutOutcome::PERMANENT_ERROR,
      "conditional PUT failure classifications are not distinct");
}

void test_provider_lifecycle_refcount() {
  provider_initialize_calls.store(0, std::memory_order_relaxed);
  provider_shutdown_calls.store(0, std::memory_order_relaxed);
  objstore::detail::ProviderLifecycle lifecycle(record_provider_initialize,
                                                record_provider_shutdown);

  lifecycle.release();
  expect(provider_shutdown_calls.load(std::memory_order_relaxed) == 0,
         "provider lifecycle underflow invoked shutdown");

  constexpr int kThreadCount = 16;
  std::atomic<int> acquired{0};
  std::atomic<bool> release{false};
  std::vector<std::thread> workers;
  workers.reserve(kThreadCount);
  for (int index = 0; index < kThreadCount; ++index) {
    workers.emplace_back([&]() {
      lifecycle.acquire();
      acquired.fetch_add(1, std::memory_order_release);
      while (!release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      lifecycle.release();
    });
  }
  while (acquired.load(std::memory_order_acquire) != kThreadCount) {
    std::this_thread::yield();
  }
  expect(provider_initialize_calls.load(std::memory_order_relaxed) == 1,
         "concurrent provider acquires initialized the SDK more than once");
  expect(provider_shutdown_calls.load(std::memory_order_relaxed) == 0,
         "provider SDK shut down while references were active");

  release.store(true, std::memory_order_release);
  for (std::thread &worker : workers) worker.join();
  expect(provider_shutdown_calls.load(std::memory_order_relaxed) == 1,
         "last provider release did not shut down the SDK exactly once");

  lifecycle.acquire();
  lifecycle.release();
  lifecycle.release();
  expect(provider_initialize_calls.load(std::memory_order_relaxed) == 2 &&
             provider_shutdown_calls.load(std::memory_order_relaxed) == 2,
         "provider lifecycle did not support a balanced reinitialization");
}

void test_local_object_store() {
  TempDirectory temp;
  const fs::path store_root = temp.path() / "store";
  LocalObjectStore store(store_root.string());
  constexpr std::string_view bucket{"bucket"};
  std::string body;

  std::string init_error;
  objstore::ObjectStore *managed_store = nullptr;
  expect(objstore::init_object_store("local", (temp.path() / "managed").string(),
                                     init_error, managed_store) == 0 &&
             managed_store != nullptr,
         "successful object-store initialization failed");
  objstore::cleanup_object_store(managed_store);
  expect(managed_store == nullptr,
         "successful object-store cleanup did not clear the pointer");
  objstore::cleanup_object_store(managed_store);
  expect(managed_store == nullptr, "repeated object-store cleanup was unsafe");

  objstore::ObjectStore *failed_store = nullptr;
  expect(objstore::init_object_store("unsupported-provider", "unused",
                                     init_error, failed_store) == 1 &&
             failed_store == nullptr,
         "failed object-store initialization left a client");

  Status status = store.create_bucket("../escaped-bucket");
  expect(status.error_code() == Errors::SE_INVALID,
         "bucket traversal must be rejected");
  expect(!fs::exists(temp.path() / "escaped-bucket"),
         "bucket traversal escaped the store root");
  expect_success(store.create_bucket(bucket), "create bucket");

  const std::string nul_bucket{"nul-bucket\0suffix", 17};
  status = store.create_bucket(nul_bucket);
  expect(status.error_code() == Errors::SE_INVALID &&
             !fs::exists(store_root / "nul-bucket"),
         "NUL bucket created a truncated directory alias");

  const std::string nul_key{"nul-key\0suffix", 14};
  status = store.put_object(bucket, nul_key, "aliased");
  expect(status.error_code() == Errors::SE_INVALID &&
             !fs::exists(store_root / "bucket/nul-key"),
         "NUL key created a truncated file alias");

  std::string store_id_storage = "storeTRAILING";
  std::string branch_id_storage = "branchTRAILING";
  const std::string_view store_id{store_id_storage.data(), 5};
  const std::string_view branch_id{branch_id_storage.data(), 6};
  std::string lock_error;
  expect(objstore::ensure_object_store_lock(
             "local", store_root.string(), nullptr, bucket, store_id,
             branch_id, false, lock_error) == 0,
         "create bounded object-store lock failed");
  expect(fs::is_regular_file(store_root / "bucket/store/branch/data.lock") &&
             !fs::exists(store_root /
                         "bucket/storeTRAILING/branchTRAILING/data.lock"),
         "object-store lock read past string_view length");
  expect(objstore::ensure_object_store_lock(
             "local", store_root.string(), nullptr, bucket, store_id,
             branch_id, true, lock_error) == 0,
         "verify object-store lock failed");
  lock_error.clear();
  expect(objstore::ensure_object_store_lock(
             "unsupported-provider", "unused", nullptr, bucket, store_id,
             branch_id, false, lock_error) == 1,
         "failed object-store lock initialization unexpectedly succeeded");

  status = store.put_object(bucket, "../escaped-object", "bad");
  expect(status.error_code() == Errors::SE_INVALID,
         "object traversal must be rejected");
  expect(!fs::exists(store_root / "escaped-object"),
         "object traversal escaped the bucket");

  const fs::path absolute_key = temp.path() / "absolute-object";
  write_file(absolute_key, "absolute-secret");
  status = store.put_object(bucket, absolute_key.string(), "overwrite");
  expect(status.error_code() == Errors::SE_INVALID,
         "absolute object key must be rejected");
  body.clear();
  status = store.get_object(bucket, absolute_key.string(), body);
  expect(status.error_code() == Errors::SE_INVALID && body.empty(),
         "absolute object key was readable");
  status = store.delete_object(bucket, absolute_key.string());
  expect(status.error_code() == Errors::SE_INVALID &&
             read_file(absolute_key) == "absolute-secret",
         "absolute object key deleted a file outside the store");
  bool absolute_list_finished = false;
  std::string absolute_start_after;
  std::vector<ObjectMeta> absolute_objects;
  status = store.list_object(bucket, absolute_key.string(), true,
                             absolute_start_after, absolute_list_finished,
                             absolute_objects);
  expect(status.error_code() == Errors::SE_INVALID &&
             absolute_objects.empty() &&
             read_file(absolute_key) == "absolute-secret",
         "absolute list prefix accessed a file outside the store");

  expect_success(store.put_object(bucket, "repo/", ""),
                 "create object directory");
  const std::string payload{"ab\0cdef", 7};
  expect_success(store.put_object(bucket, "repo/object.bin", payload),
                 "put object");
  expect_success(store.put_object(bucket, "repo/nested/item", "nested"),
                 "put nested object");

  expect_success(store.get_object(bucket, "repo/object.bin", body),
                 "get object");
  expect(body == payload, "full object body mismatch");

  status = store.put_object(bucket, "repo/object.bin", "replacement", true);
  expect(status.error_code() == Errors::SE_OBJECT_FORBID_OVERWRITE,
         "forbid-overwrite did not reject an existing key");
  expect_success(store.get_object(bucket, "repo/object.bin", body),
                 "get object after forbid-overwrite");
  expect(body == payload, "forbid-overwrite changed object data");

  expect_success(store.get_object(bucket, "repo/object.bin", 2, 3, body),
                 "get object range");
  expect(body == std::string{"\0cd", 3}, "object range mismatch");
  expect_success(store.get_object(bucket, "repo/object.bin", 5, 100, body),
                 "get object tail range");
  expect(body == "ef", "object tail range mismatch");
  status = store.get_object(bucket, "repo/object.bin", payload.size(), 1, body);
  expect(status.error_code() == Errors::SE_INVALID,
         "range at end of object must fail");

  ObjectMeta meta;
  expect_success(store.get_object_meta(bucket, "repo/object.bin", meta),
                 "get object metadata");
  expect(meta.key == "repo/object.bin", "metadata key mismatch");
  expect(meta.size == static_cast<long long>(payload.size()),
         "metadata size mismatch");
  const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  expect(meta.last_modified > 0 && meta.last_modified <= now_ms + 1000 &&
             now_ms - meta.last_modified < 60000,
         "metadata timestamp is not Unix epoch milliseconds");

  std::string invalid_key_storage = "/badTRAILING";
  const std::string_view invalid_key{invalid_key_storage.data(), 4};
  status = store.get_object_meta(bucket, invalid_key, meta);
  expect(!status.is_succ(), "bounded invalid key unexpectedly succeeded");
  expect(status.error_message().find("TRAILING") == std::string_view::npos,
         "error message read past string_view length");

  bool finished = false;
  std::string start_after;
  std::vector<ObjectMeta> objects;
  expect_success(store.list_object(bucket, "repo/", true, start_after,
                                   finished, objects),
                 "recursive list");
  expect(finished && contains_key(objects, "repo/") &&
             contains_key(objects, "repo/object.bin") &&
             contains_key(objects, "repo/nested/") &&
             contains_key(objects, "repo/nested/item"),
         "recursive list is incomplete");

  finished = false;
  objects.clear();
  expect_success(store.list_object(bucket, "repo/", false, start_after,
                                   finished, objects),
                 "non-recursive list");
  expect(contains_key(objects, "repo/object.bin") &&
             contains_key(objects, "repo/nested/") &&
             !contains_key(objects, "repo/nested/item"),
         "non-recursive list depth is wrong");

  start_after = "repo/nested/";
  objects.clear();
  expect_success(store.list_object(bucket, "repo/", true, start_after,
                                   finished, objects),
                 "list with start-after");
  expect(!contains_key(objects, "repo/nested/"),
         "start-after key was returned");
  for (const ObjectMeta &object : objects) {
    expect(object.key > start_after, "list returned a key before start-after");
  }

  const fs::path input_file = temp.path() / "input.bin";
  const fs::path output_file = temp.path() / "output.bin";
  write_file(input_file, payload);
  expect_success(store.put_object_from_file(bucket, "files/copied.bin",
                                            input_file.string()),
                 "put object from file");
  expect_success(store.get_object_to_file(bucket, "files/copied.bin",
                                          output_file.string()),
                 "get object to file");
  expect(read_file(output_file) == payload, "copied file body mismatch");

  status = store.put_object_from_file(bucket, "files/missing.bin",
                                      (temp.path() / "missing.bin").string());
  expect(!status.is_succ(), "missing input file unexpectedly succeeded");
  expect(!fs::exists(store_root / "bucket/files/missing.bin"),
         "missing input created an object");

  write_file(output_file, "preserve");
  status = store.get_object_to_file(bucket, "files/no-such-object",
                                    output_file.string());
  expect(!status.is_succ(), "missing object unexpectedly copied");
  expect(read_file(output_file) == "preserve",
         "missing object truncated the destination file");

  expect_success(store.put_object(bucket, "blocked", "not a directory"),
                 "create blocked parent");
  status = store.put_object(bucket, "blocked/child", "data");
  expect(status.error_code() == Errors::SE_IO_ERROR,
         "parent directory error did not return a runtime status");
  status = store.put_object_from_file(bucket, "blocked/from-file",
                                      input_file.string());
  expect(status.error_code() == Errors::SE_IO_ERROR,
         "file-copy parent error did not return a runtime status");

  const fs::path upload_dir = temp.path() / "upload";
  write_file(upload_dir / "a.txt", "alpha");
  write_file(upload_dir / "sub/b.txt", "beta");
  fs::create_directories(upload_dir / "empty");
  status = store.put_objects_from_dir(upload_dir.string(), bucket,
                                      absolute_key.string());
  expect(status.error_code() == Errors::SE_INVALID &&
             read_file(absolute_key) == "absolute-secret",
         "directory upload accepted an absolute object prefix");
  expect_success(store.put_objects_from_dir(upload_dir.string(), bucket,
                                            "tree"),
                 "put directory");
  const fs::path download_dir = temp.path() / "download";
  expect_success(store.get_objects_to_dir(bucket, "tree",
                                          download_dir.string()),
                 "get directory");
  expect(read_file(download_dir / "a.txt") == "alpha" &&
             read_file(download_dir / "sub/b.txt") == "beta" &&
             fs::is_directory(download_dir / "empty"),
         "directory copy mismatch");

  const fs::path outside_dir = temp.path() / "outside";
  write_file(outside_dir / "secret.txt", "outside-secret");
  const auto expect_outside_unchanged = [&] {
    expect(read_file(outside_dir / "secret.txt") == "outside-secret" &&
               !fs::exists(outside_dir / "new.txt") &&
               !fs::exists(outside_dir / "a.txt") &&
               !fs::exists(outside_dir / "b.txt"),
           "an operation changed data outside its root");
  };
  const fs::path source_root_link = temp.path() / "source-root-link";
  fs::create_directory_symlink(outside_dir, source_root_link);
  status = store.put_objects_from_dir(source_root_link.string(), bucket,
                                      "symlink-root-source");
  expect(status.error_code() == Errors::SE_INVALID,
         "directory upload accepted a symlink as its source root");
  status = store.get_object(bucket, "symlink-root-source/secret.txt", body);
  expect(!status.is_succ(),
         "directory upload read through a symlink source root");
  expect_outside_unchanged();

  const fs::path source_link = upload_dir / "external-link";
  fs::create_symlink(outside_dir / "secret.txt", source_link);
  status = store.put_objects_from_dir(upload_dir.string(), bucket,
                                      "symlink-source");
  expect(!status.is_succ(),
         "directory upload accepted a source symlink outside its root");
  expect_outside_unchanged();
  status = store.get_object(bucket, "symlink-source/external-link", body);
  expect(!status.is_succ(),
         "directory upload read a source symlink outside its root");
  expect_outside_unchanged();
  fs::remove(source_link);

  const fs::path object_link = store_root / "bucket/link";
  fs::create_directory_symlink(outside_dir, object_link);
  status = store.put_object(bucket, "link/new.txt", "escaped");
  expect(status.error_code() == Errors::SE_INVALID,
         "put followed a symlink outside the bucket");
  expect_outside_unchanged();
  body = "unchanged";
  status = store.get_object(bucket, "link/secret.txt", body);
  expect(status.error_code() == Errors::SE_INVALID && body == "unchanged",
         "get followed a symlink outside the bucket");
  expect_outside_unchanged();
  status = store.delete_object(bucket, "link/secret.txt");
  expect(status.error_code() == Errors::SE_INVALID,
         "delete followed a symlink outside the bucket");
  expect_outside_unchanged();
  objects.clear();
  start_after.clear();
  status = store.list_object(bucket, "link/", true, start_after, finished,
                             objects);
  expect(!status.is_succ() && objects.empty(),
         "list accepted a symlink outside the bucket");
  expect_outside_unchanged();
  status = store.put_objects_from_dir(upload_dir.string(), bucket, "link");
  expect(!status.is_succ(),
         "directory upload followed a symlink outside the bucket");
  expect_outside_unchanged();
  fs::remove(object_link);

  const fs::path bucket_link = store_root / "linked-bucket";
  fs::create_directory_symlink(outside_dir, bucket_link);
  objects.clear();
  start_after.clear();
  status = store.list_object("linked-bucket", "", true, start_after,
                             finished, objects);
  expect(status.error_code() == Errors::SE_INVALID && objects.empty(),
         "list followed a bucket symlink outside the store root");
  expect_outside_unchanged();
  fs::remove(bucket_link);

  const fs::path symlink_download = temp.path() / "symlink-download";
  fs::create_directories(symlink_download);
  fs::create_directory_symlink(outside_dir, symlink_download / "sub");
  status = store.get_objects_to_dir(bucket, "tree",
                                    symlink_download.string());
  expect(!status.is_succ(),
         "directory download followed a symlink outside its target");
  expect_outside_unchanged();

  FixedListingStore absolute_listing_store(store_root.string(),
                                            absolute_key.string());
  const fs::path absolute_listing_dst = temp.path() / "absolute-download";
  status = absolute_listing_store.get_objects_to_dir(
      bucket, "", absolute_listing_dst.string());
  expect(status.error_code() == Errors::SE_INVALID &&
             !fs::exists(absolute_listing_dst / absolute_key.filename()) &&
             read_file(absolute_key) == "absolute-secret",
         "malicious absolute listing escaped the download directory");

  FixedListingStore nul_listing_store(
      store_root.string(), std::string{"nul-listing\0suffix", 18});
  const fs::path nul_listing_dst = temp.path() / "nul-listing-download";
  status = nul_listing_store.get_objects_to_dir(bucket, "",
                                                nul_listing_dst.string());
  expect(status.error_code() == Errors::SE_INVALID &&
             !fs::exists(nul_listing_dst / "nul-listing"),
         "malicious NUL listing created a truncated file alias");

  const fs::path destination_root_link = temp.path() / "destination-root-link";
  fs::create_directory_symlink(outside_dir, destination_root_link);
  status = store.get_objects_to_dir(bucket, "tree",
                                    destination_root_link.string());
  expect(status.error_code() == Errors::SE_INVALID,
         "directory download accepted a symlink destination root");
  expect_outside_unchanged();

  TraversalListingStore traversal_store(store_root.string());
  const fs::path traversal_dst = temp.path() / "traversal-download";
  status = traversal_store.get_objects_to_dir(bucket, "tree",
                                               traversal_dst.string());
  expect(status.error_code() == Errors::SE_INVALID,
         "malicious listing traversal was not rejected");
  expect(!fs::exists(temp.path() / "escaped.txt"),
         "malicious listing escaped the download directory");
  expect_outside_unchanged();

  expect_success(store.delete_object(bucket, "repo/object.bin"),
                 "delete object");
  status = store.get_object(bucket, "repo/object.bin", body);
  expect(status.error_code() == Errors::SE_NO_SUCH_KEY,
         "deleted object is still readable");
  expect_success(store.delete_directory(bucket, "repo"), "delete directory");

  objects.clear();
  start_after.clear();
  expect_success(store.list_object(bucket, "repo/", true, start_after,
                                   finished, objects),
                 "list deleted directory");
  expect(objects.empty(), "delete directory left objects behind");
  expect_success(store.delete_bucket(bucket), "delete bucket");
  status = store.list_object(bucket, "", true, start_after, finished, objects);
  expect(status.error_code() == Errors::SE_NO_SUCH_BUCKET,
         "deleted bucket is still listable");
}

}  // namespace

int main() {
  try {
    test_provider_lifecycle_refcount();
    test_conditional_protocol_contract();
    test_local_object_store();
    std::cout << "local ObjectStore tests passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "local ObjectStore test failed: " << error.what() << '\n';
    return 1;
  }
}
