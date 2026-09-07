/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/materializer.h"
#include "sql/remote_commit/local_install.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <unistd.h>

namespace rc = wesql::remote_commit;

namespace {

namespace fs = std::filesystem;

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "remote commit materializer test failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

std::string hash(std::string_view bytes) {
  std::string result;
  std::string error;
  expect(rc::sha256_hex(bytes, &result, &error), "cannot hash fixture bytes");
  return result;
}

std::string read_file(const fs::path &path) {
  std::ifstream input(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

class FakePayloadIo final : public rc::PayloadIo {
 public:
  rc::PayloadReadResult download(
      std::string_view key, const fs::path &destination) override {
    reads.emplace_back(key);
    if (blocked.contains(std::string(key)))
      return {rc::PayloadReadOutcome::BLOCKED, "blocked"};
    const auto found = bodies.find(std::string(key));
    if (found == bodies.end())
      return {rc::PayloadReadOutcome::ABSENT, "absent"};
    std::ofstream output(destination, std::ios::binary | std::ios::trunc);
    if (!output)
      return {rc::PayloadReadOutcome::PERMANENT_ERROR,
              "cannot write fake download"};
    output.write(found->second.data(),
                 static_cast<std::streamsize>(found->second.size()));
    output.close();
    return {rc::PayloadReadOutcome::APPLIED, {}};
  }

  std::unordered_map<std::string, std::string> bodies;
  std::unordered_set<std::string> blocked;
  std::vector<std::string> reads;
};

class FakeBinlogValidator final : public rc::BinlogImageValidator {
 public:
  bool validate(const std::vector<fs::path> &files,
                const rc::Cursor &durable_cursor,
                std::string *error) override {
    called = true;
    observed.clear();
    for (const fs::path &file : files) observed.push_back(read_file(file));
    observed_cursor = durable_cursor;
    if (!accept && error != nullptr) *error = "fixture rejection";
    return accept;
  }

  bool accept{true};
  bool called{false};
  rc::Cursor observed_cursor;
  std::vector<std::string> observed;
};

struct Fixture {
  rc::RecoveryPlan plan;
  FakePayloadIo io;
  std::string seed;
  std::string segment;
  uint64_t total{0};
};

Fixture make_fixture() {
  Fixture result;
  const std::vector<std::pair<std::string, std::string>> objects{
      {"innodb", "data/ibdata1"},
      {"mysql-dd", "mysql/dd.ibd"},
      {"smartengine-meta", "smartengine/meta"},
      {"smartengine-wal", "smartengine/wal"},
  };
  uint64_t ordinal = 0;
  for (const auto &[component, path] : objects) {
    rc::SnapshotObject object;
    object.component = component;
    object.ordinal = 0;
    object.relative_path = path;
    object.key = "payload/" + std::to_string(ordinal++);
    const std::string body = component + "-body";
    object.size = body.size();
    object.sha256 = hash(body);
    object.format = "raw-v1";
    result.io.bodies[object.key] = body;
    result.total += body.size();
    result.plan.snapshot.objects.push_back(std::move(object));
  }

  rc::SmartengineExtentRef extent;
  extent.key = "extent/1";
  const std::string extent_body = "immutable-extent";
  extent.size = extent_body.size();
  extent.sha256 = hash(extent_body);
  result.io.bodies[extent.key] = extent_body;
  result.total += extent_body.size();
  result.plan.snapshot.smartengine_extents.push_back(std::move(extent));

  result.seed.assign(32, 's');
  result.seed[21] = static_cast<char>(0x80);
  result.plan.snapshot.binlog_seed.file = "binlog.000001";
  result.plan.snapshot.binlog_seed.cursor = {"binlog.000001", 32};
  result.plan.snapshot.binlog_seed.key = "seed/1";
  result.plan.snapshot.binlog_seed.size = result.seed.size();
  result.plan.snapshot.binlog_seed.sha256 = hash(result.seed);
  result.io.bodies[result.plan.snapshot.binlog_seed.key] = result.seed;
  result.total += result.seed.size();

  result.segment = "tail";
  rc::SegmentRef segment;
  segment.key = "segment/1";
  segment.size = result.segment.size();
  segment.sha256 = hash(result.segment);
  segment.source = {"binlog.000001", 32, 36};
  result.io.bodies[segment.key] = result.segment;
  result.total += result.segment.size();
  result.plan.replay_segments.push_back(std::move(segment));
  result.plan.head.durable_cursor = {"binlog.000001", 36};
  return result;
}

fs::path unique_root(std::string_view suffix) {
  static uint64_t sequence = 0;
  return fs::temp_directory_path() /
         ("wesql-remote-commit-materializer-" +
          std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
          std::to_string(++sequence) + "-" + std::string(suffix));
}

struct RootCleanup {
  explicit RootCleanup(fs::path value) : path(std::move(value)) {}
  ~RootCleanup() {
    std::error_code ignored;
    fs::remove_all(path, ignored);
  }
  fs::path path;
};

void test_success_and_normalization() {
  Fixture fixture = make_fixture();
  FakeBinlogValidator validator;
  rc::RecoveryMaterializer materializer(&fixture.io, &validator);
  rc::MaterializeOptions options;
  options.temp_root = unique_root("success");
  RootCleanup cleanup(options.temp_root);
  rc::MaterializedRoot root;
  const auto result = materializer.materialize(fixture.plan, options, &root);
  expect(result.ready(), result.detail.c_str());
  expect(validator.called && validator.observed.size() == 1,
         "raw binlog validator was not called exactly once");
  expect(validator.observed.front() == fixture.seed + fixture.segment,
         "validator did not observe exact remote bytes before normalization");
  const std::string materialized = read_file(root.binlog_files.front());
  expect(materialized.size() == fixture.seed.size() + fixture.segment.size(),
         "materialized binlog size changed");
  expect(static_cast<unsigned char>(materialized[21]) == 0x81,
         "active FDE in-use bit was not set while preserving other flags");
  expect(read_file(root.binlog_index) == "binlog.000001\n",
         "native binlog index changed");
  expect(root.verified_payload_bytes == fixture.total,
         "verified payload byte accounting changed");
  expect(!fs::exists(options.temp_root / ".remote-commit-download"),
         "successful materialization retained scratch payload");
}

void test_object_store_exact_file_adapter() {
  const objstore::Status absent_status(objstore::SE_NO_SUCH_KEY, 404,
                                       "not found");
  const objstore::Status transient_status(
      objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED, 503,
      "temporarily unavailable");
  const objstore::Status permanent_status(objstore::SE_IO_ERROR, 403,
                                          "forbidden");

  const fs::path root = unique_root("exact-file-adapter");
  RootCleanup cleanup(root);
  fs::create_directories(root);
  const fs::path destination = root / "payload";
  {
    std::ofstream output(destination, std::ios::binary);
    output << "body";
  }

  struct Mapping {
    objstore::ExactFileResult result;
    rc::PayloadReadOutcome expected;
  };
  const std::vector<Mapping> mappings{
      {objstore::ExactFileResult::applied(4, "\"etag\""),
       rc::PayloadReadOutcome::APPLIED},
      {objstore::ExactFileResult::not_found(absent_status),
       rc::PayloadReadOutcome::ABSENT},
      {objstore::ExactFileResult::transient_unavailable(transient_status),
       rc::PayloadReadOutcome::BLOCKED},
      {objstore::ExactFileResult::permanent_error(permanent_status),
       rc::PayloadReadOutcome::PERMANENT_ERROR},
      {objstore::ExactFileResult::unsupported(),
       rc::PayloadReadOutcome::PERMANENT_ERROR},
  };
  for (const Mapping &mapping : mappings) {
    expect(rc::ObjectStorePayloadIo::classify_exact_file_result(
               mapping.result, destination)
               .outcome == mapping.expected,
           "exact file adapter changed an ObjectStore outcome");
  }

  expect(rc::ObjectStorePayloadIo::classify_exact_file_result(
             objstore::ExactFileResult::applied(5, "\"etag\""), destination)
             .outcome == rc::PayloadReadOutcome::PERMANENT_ERROR,
         "exact file adapter accepted a streamed-size mismatch");
}

void test_blocked_and_hash_mismatch() {
  {
    Fixture fixture = make_fixture();
    fixture.io.blocked.insert(fixture.plan.snapshot.binlog_seed.key);
    FakeBinlogValidator validator;
    rc::RecoveryMaterializer materializer(&fixture.io, &validator);
    rc::MaterializeOptions options;
    options.temp_root = unique_root("blocked");
    RootCleanup cleanup(options.temp_root);
    rc::MaterializedRoot root;
    expect(materializer.materialize(fixture.plan, options, &root).outcome ==
               rc::MaterializeOutcome::BLOCKED,
           "temporary payload failure must be BLOCKED");
  }
  {
    Fixture fixture = make_fixture();
    fixture.io.bodies.at(fixture.plan.replay_segments.front().key) = "FAIL";
    FakeBinlogValidator validator;
    rc::RecoveryMaterializer materializer(&fixture.io, &validator);
    rc::MaterializeOptions options;
    options.temp_root = unique_root("hash");
    RootCleanup cleanup(options.temp_root);
    rc::MaterializedRoot root;
    expect(materializer.materialize(fixture.plan, options, &root).outcome ==
               rc::MaterializeOutcome::CORRUPT,
           "payload SHA mismatch must be CORRUPT");
  }
}

void test_cross_file_reconstruction() {
  Fixture fixture = make_fixture();
  fixture.total -= fixture.segment.size();
  fixture.io.bodies.erase(fixture.plan.replay_segments.front().key);
  fixture.plan.replay_segments.clear();

  std::string next_file(32, 'n');
  next_file[21] = static_cast<char>(0x80);
  rc::SegmentRef segment;
  segment.key = "segment/next";
  segment.size = next_file.size();
  segment.sha256 = hash(next_file);
  segment.source = {"binlog.000002", 0, 32};
  fixture.io.bodies[segment.key] = next_file;
  fixture.total += next_file.size();
  fixture.plan.replay_segments.push_back(std::move(segment));
  fixture.plan.head.durable_cursor = {"binlog.000002", 32};

  FakeBinlogValidator validator;
  rc::RecoveryMaterializer materializer(&fixture.io, &validator);
  rc::MaterializeOptions options;
  options.temp_root = unique_root("cross-file");
  RootCleanup cleanup(options.temp_root);
  rc::MaterializedRoot root;
  const auto result = materializer.materialize(fixture.plan, options, &root);
  expect(result.ready(), result.detail.c_str());
  expect(root.binlog_files.size() == 2 && validator.observed.size() == 2,
         "cross-file reconstruction did not preserve both files");
  const std::string first = read_file(root.binlog_files[0]);
  const std::string second = read_file(root.binlog_files[1]);
  expect(static_cast<unsigned char>(first[21]) == 0x80,
         "closed binlog FDE in-use bit was not cleared");
  expect(static_cast<unsigned char>(second[21]) == 0x81,
         "active cross-file FDE in-use bit was not set");
  expect(read_file(root.binlog_index) ==
             "binlog.000001\nbinlog.000002\n",
         "cross-file index order changed");
}

void test_collision_limit_and_validator_rejection() {
  {
    Fixture fixture = make_fixture();
    fixture.plan.snapshot.objects[1].relative_path =
        fixture.plan.snapshot.objects[0].relative_path;
    FakeBinlogValidator validator;
    rc::RecoveryMaterializer materializer(&fixture.io, &validator);
    rc::MaterializeOptions options;
    options.temp_root = unique_root("collision");
    RootCleanup cleanup(options.temp_root);
    rc::MaterializedRoot root;
    expect(materializer.materialize(fixture.plan, options, &root).outcome ==
               rc::MaterializeOutcome::CORRUPT,
           "cross-component path collision must be CORRUPT");
  }
  {
    Fixture fixture = make_fixture();
    fixture.plan.snapshot.objects[0].relative_path =
        rc::kLocalInstallMarkerName;
    FakeBinlogValidator validator;
    rc::RecoveryMaterializer materializer(&fixture.io, &validator);
    rc::MaterializeOptions options;
    options.temp_root = unique_root("reserved-marker");
    RootCleanup cleanup(options.temp_root);
    rc::MaterializedRoot root;
    expect(materializer.materialize(fixture.plan, options, &root).outcome ==
               rc::MaterializeOutcome::CORRUPT,
           "snapshot payload was allowed to replace the install marker");
  }
  {
    Fixture fixture = make_fixture();
    fixture.plan.snapshot.objects[0].relative_path = "mysql";
    fixture.plan.snapshot.objects[1].relative_path = "mysql/dd.ibd";
    FakeBinlogValidator validator;
    rc::RecoveryMaterializer materializer(&fixture.io, &validator);
    rc::MaterializeOptions options;
    options.temp_root = unique_root("ancestor-collision");
    RootCleanup cleanup(options.temp_root);
    rc::MaterializedRoot root;
    expect(materializer.materialize(fixture.plan, options, &root).outcome ==
               rc::MaterializeOutcome::CORRUPT,
           "ancestor payload path collision was accepted");
  }
  {
    Fixture fixture = make_fixture();
    FakeBinlogValidator validator;
    rc::RecoveryMaterializer materializer(&fixture.io, &validator);
    rc::MaterializeOptions options;
    options.temp_root = unique_root("limit");
    options.max_total_payload_bytes = fixture.total - 1;
    RootCleanup cleanup(options.temp_root);
    rc::MaterializedRoot root;
    expect(materializer.materialize(fixture.plan, options, &root).outcome ==
               rc::MaterializeOutcome::CORRUPT,
           "aggregate payload limit must fail before creating a root");
    expect(!fs::exists(options.temp_root),
           "aggregate limit failure created a temp root");
  }
  {
    Fixture fixture = make_fixture();
    FakeBinlogValidator validator;
    validator.accept = false;
    rc::RecoveryMaterializer materializer(&fixture.io, &validator);
    rc::MaterializeOptions options;
    options.temp_root = unique_root("validator");
    RootCleanup cleanup(options.temp_root);
    rc::MaterializedRoot root;
    expect(materializer.materialize(fixture.plan, options, &root).outcome ==
               rc::MaterializeOutcome::CORRUPT,
           "native binlog validator rejection must be CORRUPT");
  }
}

}  // namespace

int main() {
  test_object_store_exact_file_adapter();
  test_success_and_normalization();
  test_blocked_and_hash_mismatch();
  test_cross_file_reconstruction();
  test_collision_limit_and_validator_rejection();
  std::cout << "remote commit materializer tests passed\n";
  return EXIT_SUCCESS;
}
