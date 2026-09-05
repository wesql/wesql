/*
 * Copyright (c) 2026, ApeCloud Inc Holding Limited
 * Licensed under the Apache License, Version 2.0.
 */

#include "objstore/remote_extent.h"

#include <deque>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "schema/table_schema.h"
#include "storage/storage_meta_struct.h"
#include "storage/io_extent.h"
#include "util/testharness.h"

namespace smartengine {
namespace storage {
namespace remote_extent {
namespace {

::objstore::Status object_error(::objstore::Errors error, int provider_error,
                                const char *message) {
  return ::objstore::Status(error, provider_error, message);
}

class ScriptedIo : public ConditionalIo {
 public:
  ::objstore::ExactObjectResult get(std::string_view key) override {
    observed_get_keys.emplace_back(key);
    if (gets.empty()) return ::objstore::ExactObjectResult::unsupported();
    auto result = std::move(gets.front());
    gets.pop_front();
    return result;
  }

  ::objstore::ConditionalPutResult create(std::string_view key,
                                           std::string_view body) override {
    observed_create_keys.emplace_back(key);
    observed_create_bodies.emplace_back(body);
    if (creates.empty())
      return ::objstore::ConditionalPutResult::unsupported();
    auto result = std::move(creates.front());
    creates.pop_front();
    return result;
  }

  std::deque<::objstore::ExactObjectResult> gets;
  std::deque<::objstore::ConditionalPutResult> creates;
  std::vector<std::string> observed_get_keys;
  std::vector<std::string> observed_create_keys;
  std::vector<std::string> observed_create_bodies;
};

class FakeObjectStore : public ::objstore::ObjectStore {
 public:
  ::objstore::Status create_bucket(const std::string_view &) override {
    return {};
  }
  ::objstore::Status delete_bucket(const std::string_view &) override {
    return {};
  }
  ::objstore::Status put_object_from_file(const std::string_view &,
                                           const std::string_view &,
                                           const std::string_view &) override {
    return {};
  }
  ::objstore::Status get_object_to_file(const std::string_view &,
                                        const std::string_view &,
                                        const std::string_view &) override {
    return {};
  }
  ::objstore::Status put_object(const std::string_view &,
                                const std::string_view &key,
                                const std::string_view &data, bool) override {
    objects[std::string(key)] = std::string(data);
    return {};
  }
  ::objstore::Status get_object(const std::string_view &,
                                const std::string_view &key,
                                std::string &body) override {
    const auto found = objects.find(std::string(key));
    if (found == objects.end())
      return object_error(::objstore::SE_NO_SUCH_KEY, 404, "not found");
    body = found->second;
    return {};
  }
  ::objstore::Status get_object(const std::string_view &bucket,
                                const std::string_view &key,
                                size_t offset, size_t length,
                                std::string &body) override {
    std::string object;
    const auto status = get_object(bucket, key, object);
    if (!status.is_succ()) return status;
    if (offset > object.size())
      return object_error(::objstore::SE_INVALID, 0, "invalid range");
    body = object.substr(offset, length);
    return {};
  }
  ::objstore::ConditionalObjectStoreCapabilities conditional_capabilities()
      const override {
    return {true, true, true, true, true, true};
  }
  ::objstore::ExactObjectResult get_object_exact(
      const std::string_view &, const std::string_view &key) override {
    ++exact_get_count;
    const auto found = objects.find(std::string(key));
    if (found == objects.end()) {
      return ::objstore::ExactObjectResult::not_found(
          object_error(::objstore::SE_NO_SUCH_KEY, 404, "not found"));
    }
    return ::objstore::ExactObjectResult::found(found->second, "fake-etag");
  }
  ::objstore::ConditionalPutResult put_object_conditional(
      const std::string_view &, const std::string_view &key,
      const std::string_view &body,
      const ::objstore::ConditionalPutCondition &condition) override {
    if (!condition.is_valid()) {
      return ::objstore::ConditionalPutResult::permanent_error(
          object_error(::objstore::SE_INVALID, 0, "invalid condition"));
    }
    const auto inserted = objects.emplace(std::string(key), std::string(body));
    if (!inserted.second) {
      return ::objstore::ConditionalPutResult::precondition_failed_412(
          object_error(::objstore::SE_OBJECT_PRECONDITION_FAILED, 412,
                       "already exists"));
    }
    return ::objstore::ConditionalPutResult::applied("fake-etag");
  }
  ::objstore::Status get_object_meta(const std::string_view &,
                                     const std::string_view &,
                                     ::objstore::ObjectMeta &) override {
    return {};
  }
  ::objstore::Status list_object(const std::string_view &,
                                 const std::string_view &, bool,
                                 std::string &, bool &finished,
                                 std::vector<::objstore::ObjectMeta> &) override {
    finished = true;
    return {};
  }
  ::objstore::Status delete_object(const std::string_view &,
                                    const std::string_view &key) override {
    objects.erase(std::string(key));
    return {};
  }
  std::string_view get_provider() const override { return "fake"; }
  ::objstore::Status delete_objects(
      const std::string_view &,
      const std::vector<std::string_view> &keys) override {
    for (const auto key : keys) objects.erase(std::string(key));
    return {};
  }

  std::map<std::string, std::string> objects;
  size_t exact_get_count{0};
};

RuntimeConfig test_config(FakeObjectStore *store = nullptr) {
  return {"cluster/root", std::string(64, 'a'), 37, store, "bucket"};
}

FakeObjectStore *g_provider_store = nullptr;
bool g_provider_enabled = false;
bool g_provider_fenced = false;
size_t g_provider_fence_calls = 0;

bool provider_enabled() { return g_provider_enabled; }

bool provider_load(RuntimeConfig *config) {
  if (config == nullptr || g_provider_store == nullptr) return false;
  *config = test_config(g_provider_store);
  return true;
}

bool provider_is_fenced() { return g_provider_fenced; }

void provider_fence(std::string_view) { ++g_provider_fence_calls; }

const RuntimeProvider kTestProvider{provider_enabled, provider_load,
                                    provider_is_fenced, provider_fence};

ExtentId object_extent_id(int32_t table_space_id, int32_t offset) {
  return ExtentId(convert_table_space_to_fd(table_space_id), offset);
}

std::string build_test_prefix(std::string_view database, uint64_t index_id,
                              uint64_t allocation_seq) {
  std::string prefix;
  std::string error;
  EXPECT_TRUE(build_prefix(test_config(), database, index_id, allocation_seq,
                           &prefix, &error))
      << error;
  return prefix;
}

class RuntimeTest : public testing::Test {
 protected:
  void SetUp() override {
    std::string error;
    ASSERT_TRUE(install_test_runtime(test_config(&store), &error)) << error;
  }

  void TearDown() override { clear_test_runtime(); }

  FakeObjectStore store;
};

TEST_F(RuntimeTest, ExtentIoBindsRuntimeClientInsteadOfEngineLocalClient) {
  FakeObjectStore engine_client;
  ObjectIOExtent extent;
  const ExtentId id = object_extent_id(1, 0);
  const std::string prefix = build_test_prefix("db", 1, 0);
  ASSERT_EQ(common::Status::kOk,
            extent.init(id, 1, &engine_client, "bucket", prefix));
  const std::string body(MAX_EXTENT_SIZE, 'x');
  ASSERT_EQ(common::Status::kOk, extent.write(common::Slice(body), 0));
  std::string key;
  std::string error;
  ASSERT_TRUE(object_key(test_config(&store), prefix, id, &key, nullptr,
                          &error)) << error;
  ASSERT_EQ(1U, store.objects.size());
  EXPECT_EQ(body, store.objects.at(key));
  EXPECT_GT(store.exact_get_count, 0U);
  EXPECT_TRUE(engine_client.objects.empty());
  EXPECT_EQ(0U, engine_client.exact_get_count);
  EXPECT_FALSE(is_fenced());
}

TEST_F(RuntimeTest, ExtentIoStillRejectsBucketAndFutureEpochMismatch) {
  FakeObjectStore engine_client;
  ObjectIOExtent extent;
  const ExtentId id = object_extent_id(1, 0);
  const std::string prefix = build_test_prefix("db", 1, 0);
  EXPECT_EQ(common::Status::kCorruption,
            extent.init(id, 1, &engine_client, "other-bucket", prefix));
  EXPECT_TRUE(is_fenced());
  clear_test_runtime();
  std::string error;
  ASSERT_TRUE(install_test_runtime(test_config(&store), &error)) << error;
  auto future_config = test_config(&store);
  future_config.writer_epoch = 38;
  std::string future_prefix;
  ASSERT_TRUE(build_prefix(future_config, "db", 1, 0, &future_prefix, &error));
  EXPECT_EQ(common::Status::kCorruption,
            extent.init(id, 1, &engine_client, "bucket", future_prefix));
  EXPECT_TRUE(is_fenced());
  EXPECT_TRUE(engine_client.objects.empty());
  EXPECT_TRUE(store.objects.empty());
}

TEST_F(RuntimeTest, ExtentWriteRejectsClientChangeAfterInitialization) {
  FakeObjectStore engine_client;
  FakeObjectStore replacement_runtime_client;
  ObjectIOExtent extent;
  ASSERT_EQ(common::Status::kOk,
            extent.init(object_extent_id(1, 0), 1, &engine_client, "bucket",
                        build_test_prefix("db", 1, 0)));
  clear_test_runtime();
  std::string error;
  ASSERT_TRUE(install_test_runtime(test_config(&replacement_runtime_client),
                                   &error)) << error;
  const std::string body(MAX_EXTENT_SIZE, 'x');
  EXPECT_EQ(common::Status::kCorruption,
            extent.write(common::Slice(body), 0));
  EXPECT_TRUE(is_fenced());
  EXPECT_TRUE(store.objects.empty());
  EXPECT_TRUE(engine_client.objects.empty());
  EXPECT_TRUE(replacement_runtime_client.objects.empty());
}

TEST(RemoteExtentRuntimeProvider, DefaultsOffAndDelegatesWhenInstalled) {
  clear_runtime_provider();
  g_provider_store = nullptr;
  g_provider_enabled = false;
  g_provider_fenced = false;
  g_provider_fence_calls = 0;
  EXPECT_FALSE(enabled());

  RuntimeProvider incomplete;
  std::string error;
  EXPECT_FALSE(install_runtime_provider(incomplete, &error));
  ASSERT_TRUE(install_runtime_provider(kTestProvider, &error)) << error;
  EXPECT_FALSE(enabled());

  FakeObjectStore store;
  g_provider_store = &store;
  g_provider_enabled = true;
  EXPECT_TRUE(enabled());
  RuntimeConfig observed;
  ASSERT_TRUE(runtime_config(&observed, &error)) << error;
  EXPECT_EQ(test_config(&store), observed);

  g_provider_fenced = true;
  EXPECT_TRUE(is_fenced());
  g_provider_fenced = false;
  enter_fenced("provider fence test");
  EXPECT_EQ(1U, g_provider_fence_calls);
  EXPECT_TRUE(is_fenced());

  clear_runtime_provider();
  g_provider_store = nullptr;
  g_provider_enabled = false;
  EXPECT_FALSE(enabled());
  EXPECT_FALSE(is_fenced());
}

TEST(RemoteExtentPrefix, GoldenAsciiAndUtf8) {
  const std::string root =
      "cluster/root/smartengine/v2/extents/s" + std::string(64, 'a');
  EXPECT_EQ(root + "/e37/a9/db=74657374/idx=42/data/",
            build_test_prefix("test", 42, 9));

  const std::string non_ascii("\xe6\x95\xb0\xe6\x8d\xae", 6);
  EXPECT_EQ(root + "/e37/a10/db=e695b0e68dae/idx=7/data/",
            build_test_prefix(non_ascii, 7, 10));

  std::string key;
  std::string error;
  ASSERT_TRUE(object_key(test_config(), build_test_prefix("test", 42, 9),
                         object_extent_id(1, 2), &key, nullptr, &error))
      << error;
  EXPECT_EQ(root + "/e37/a9/db=74657374/idx=42/data/8589934593", key);
}

TEST(RemoteExtentPrefix, RejectsMalformedAndNonCanonicalForms) {
  const std::string good = build_test_prefix("test", 42, 9);
  ParsedPrefix parsed;
  std::string error;

  std::string malformed = good;
  malformed.replace(malformed.find("/e37/"), 5, "/e037/");
  EXPECT_FALSE(parse_prefix(test_config(), malformed, &parsed, &error));

  malformed = good;
  malformed.replace(malformed.find("/a9/"), 4, "/a09/");
  EXPECT_FALSE(parse_prefix(test_config(), malformed, &parsed, &error));

  malformed = good;
  malformed.replace(malformed.find("/idx=42/"), 8, "/idx=042/");
  EXPECT_FALSE(parse_prefix(test_config(), malformed, &parsed, &error));

  malformed = good;
  malformed.replace(malformed.find("db=74657374"), 11, "db=7465737G");
  EXPECT_FALSE(parse_prefix(test_config(), malformed, &parsed, &error));

  RuntimeConfig bad = test_config();
  bad.cluster_object_prefix = "cluster//root";
  std::string prefix;
  EXPECT_FALSE(build_prefix(bad, "test", 1, 1, &prefix, &error));
}

TEST(RemoteExtentWrite, AppliedAndConflictsVerifyWithoutPutRetry) {
  for (const auto &create_result : {
           ::objstore::ConditionalPutResult::applied("etag"),
           ::objstore::ConditionalPutResult::conflict_409(object_error(
               ::objstore::SE_OBJECT_CONFLICT, 409, "conflict")),
           ::objstore::ConditionalPutResult::precondition_failed_412(
               object_error(::objstore::SE_OBJECT_PRECONDITION_FAILED, 412,
                            "precondition"))}) {
    ScriptedIo io;
    io.creates.push_back(create_result);
    io.gets.push_back(::objstore::ExactObjectResult::found("body", "etag"));
    const auto result = create_and_verify(&io, "key", "body", 4, 0);
    EXPECT_TRUE(result.verified()) << result.detail;
    EXPECT_EQ(1U, io.observed_create_keys.size());
    EXPECT_EQ(1U, io.observed_get_keys.size());
  }
}

TEST(RemoteExtentWrite, UnknownThen404RetriesTheIdenticalCreate) {
  ScriptedIo io;
  io.creates.push_back(::objstore::ConditionalPutResult::transport_unknown(
      object_error(::objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED, 0,
                   "unknown")));
  io.creates.push_back(::objstore::ConditionalPutResult::applied("etag"));
  io.gets.push_back(::objstore::ExactObjectResult::not_found(
      object_error(::objstore::SE_NO_SUCH_KEY, 404, "not found")));
  io.gets.push_back(::objstore::ExactObjectResult::found("body", "etag"));

  const auto result = create_and_verify(&io, "key", "body", 8, 0);
  ASSERT_TRUE(result.verified()) << result.detail;
  ASSERT_EQ(2U, io.observed_create_bodies.size());
  EXPECT_EQ(io.observed_create_bodies[0], io.observed_create_bodies[1]);
}

TEST(RemoteExtentWrite, DivergentReadbackFencesTheWrite) {
  ScriptedIo io;
  io.creates.push_back(::objstore::ConditionalPutResult::precondition_failed_412(
      object_error(::objstore::SE_OBJECT_PRECONDITION_FAILED, 412,
                   "precondition")));
  io.gets.push_back(::objstore::ExactObjectResult::found("other", "etag"));
  const auto result = create_and_verify(&io, "key", "body", 4, 0);
  EXPECT_EQ(static_cast<int>(ImmutableWriteOutcome::FENCED),
            static_cast<int>(result.outcome));
  EXPECT_EQ(1U, io.observed_create_keys.size());
}

TEST_F(RuntimeTest, MetadataValidationAndExactBodyVerification) {
  const std::string body(MAX_EXTENT_SIZE, 'x');
  std::string digest;
  std::string error;
  ASSERT_TRUE(sha256_hex(body, &digest, &error)) << error;

  ExtentMeta meta;
  meta.attr_ = ExtentMeta::F_NORMAL_EXTENT;
  meta.extent_space_type_ = OBJECT_EXTENT_SPACE;
  meta.extent_id_ = object_extent_id(3, 4);
  meta.table_schema_.set_database_name("test");
  meta.table_schema_.set_index_id(42);
  meta.prefix_ = build_test_prefix("test", 42, 11);
  meta.object_size_ = body.size();
  meta.object_sha256_ = digest;

  std::string key;
  ASSERT_TRUE(object_key(meta.prefix_, meta.extent_id_, &key, &error)) << error;
  store.objects.emplace(key, body);
  LiveExtentRef ref;
  EXPECT_TRUE(verify_metadata_object(meta, &ref, &error, 1, 0)) << error;
  EXPECT_EQ(1U, store.exact_get_count);
  EXPECT_EQ(key, ref.key);

  ExtentMeta mismatch(meta);
  mismatch.object_size_--;
  EXPECT_FALSE(validate_metadata(mismatch,
                                 MetadataValidationMode::EXISTING_LIVE,
                                 nullptr, &error));
}

TEST(RemoteExtentMetadata, RoundTripPreservesObjectIdentity) {
  ExtentMeta meta;
  meta.attr_ = ExtentMeta::F_NORMAL_EXTENT;
  meta.extent_space_type_ = OBJECT_EXTENT_SPACE;
  meta.extent_id_ = object_extent_id(3, 4);
  meta.table_schema_.set_database_name("test");
  meta.table_schema_.set_index_id(42);
  meta.prefix_ = build_test_prefix("test", 42, 11);
  meta.object_size_ = MAX_EXTENT_SIZE;
  meta.object_sha256_ = std::string(64, 'a');

  const int64_t serialized_size = meta.get_serialize_size();
  std::vector<char> buffer(static_cast<size_t>(serialized_size));
  int64_t pos = 0;
  ASSERT_EQ(common::Status::kOk,
            meta.serialize(buffer.data(), buffer.size(), pos));
  ASSERT_EQ(serialized_size, pos);
  ExtentMeta decoded;
  pos = 0;
  ASSERT_EQ(common::Status::kOk,
            decoded.deserialize(buffer.data(), buffer.size(), pos));
  EXPECT_EQ(meta.prefix_, decoded.prefix_);
  EXPECT_EQ(meta.object_size_, decoded.object_size_);
  EXPECT_EQ(meta.object_sha256_, decoded.object_sha256_);
}

TEST_F(RuntimeTest, MismatchedLiveBodyIsRejected) {
  const std::string expected(MAX_EXTENT_SIZE, 'x');
  std::string digest;
  std::string error;
  ASSERT_TRUE(sha256_hex(expected, &digest, &error)) << error;
  ExtentMeta meta;
  meta.attr_ = ExtentMeta::F_NORMAL_EXTENT;
  meta.extent_space_type_ = OBJECT_EXTENT_SPACE;
  meta.extent_id_ = object_extent_id(5, 6);
  meta.table_schema_.set_database_name("test");
  meta.table_schema_.set_index_id(9);
  meta.prefix_ = build_test_prefix("test", 9, 12);
  meta.object_size_ = expected.size();
  meta.object_sha256_ = digest;
  std::string key;
  ASSERT_TRUE(object_key(meta.prefix_, meta.extent_id_, &key, &error)) << error;
  store.objects.emplace(key, std::string(MAX_EXTENT_SIZE, 'y'));
  EXPECT_FALSE(verify_metadata_object(meta, nullptr, &error, 1, 0));
  EXPECT_NE(std::string::npos, error.find("does not match metadata"));
}

LiveExtentRef live_ref(uint64_t epoch, uint64_t allocation,
                       uint64_t object_id, std::string key,
                       std::string digest) {
  LiveExtentRef ref;
  ref.writer_epoch = epoch;
  ref.allocation_seq = allocation;
  ref.database_name_hex = "74657374";
  ref.index_id = 42;
  ref.object_id = object_id;
  ref.key = std::move(key);
  ref.size = MAX_EXTENT_SIZE;
  ref.sha256 = std::move(digest);
  return ref;
}

TEST(RemoteExtentLiveSet, SortsDeduplicatesAndRejectsConflicts) {
  const std::string digest(64, 'a');
  const LiveExtentRef first = live_ref(3, 7, 2, "key-b", digest);
  const LiveExtentRef second = live_ref(2, 9, 1, "key-a", digest);
  std::vector<LiveExtentRef> refs{first, second, first};
  std::string error;
  ASSERT_TRUE(canonicalize_live_set(&refs, &error)) << error;
  ASSERT_EQ(2U, refs.size());
  EXPECT_EQ(2U, refs[0].writer_epoch);
  EXPECT_EQ(0U, refs[0].ordinal);
  EXPECT_EQ(1U, refs[1].ordinal);

  LiveExtentRef conflict = first;
  conflict.sha256 = std::string(64, 'b');
  refs = {first, conflict};
  EXPECT_FALSE(canonicalize_live_set(&refs, &error));

  conflict = second;
  conflict.key = first.key;
  refs = {first, conflict};
  EXPECT_FALSE(canonicalize_live_set(&refs, &error));
}

std::string read_source(const std::filesystem::path &relative_path) {
  const std::filesystem::path path =
      std::filesystem::path(SMARTENGINE_SOURCE_DIR) / relative_path;
  std::ifstream input(path);
  EXPECT_TRUE(input.good()) << path;
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

TEST(RemoteExtentStaticGuards, NoDeleteWalAndMiddleCommit) {
  const std::string recycle = read_source("storage/extent_space_obj.cc");
  const size_t remote_branch =
      recycle.find("else if (remote_extent::enabled())");
  ASSERT_NE(std::string::npos, remote_branch);
  const size_t stock_branch = recycle.find("} else {", remote_branch);
  ASSERT_NE(std::string::npos, stock_branch);
  EXPECT_EQ(std::string::npos,
            recycle.substr(remote_branch, stock_branch - remote_branch)
                .find("delete_object"));
  EXPECT_NE(std::string::npos,
            recycle.find("objstore_->delete_object(extent_bucket_, extent_key)",
                         stock_branch));

  const std::string wal = read_source("../handler/se_hton.cc");
  EXPECT_NE(std::string::npos, wal.find("validate_all_remote_extents()"));
  EXPECT_NE(std::string::npos,
            wal.find("SmartEngine WAL sync failed in remote commit mode"));
  EXPECT_NE(std::string::npos, wal.find("return HA_EXIT_FAILURE;"));

  const std::string handler = read_source("../handler/ha_smartengine.cc");
  EXPECT_NE(std::string::npos,
            handler.find("if (storage::remote_extent::enabled()) return false;"));
  const std::string transaction =
      read_source("../transaction/se_transaction.cc");
  EXPECT_NE(std::string::npos,
            transaction.find("middle transaction flush is forbidden"));
  const std::string variables = read_source("../plugin/se_system_vars.cc");
  EXPECT_NE(std::string::npos,
            variables.find("if (storage::remote_extent::enabled()) return false;"));
}

}  // namespace
}  // namespace remote_extent
}  // namespace storage
}  // namespace smartengine

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
