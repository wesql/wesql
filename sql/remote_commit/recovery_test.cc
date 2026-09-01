/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/recovery.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace rc = wesql::remote_commit;

namespace {

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "remote commit recovery test failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

void expect_ok(bool condition, const std::string &error,
               const char *message) {
  if (!condition) {
    std::cerr << "remote commit recovery test failed: " << message << ": "
              << error << '\n';
    std::exit(EXIT_FAILURE);
  }
}

std::string hash(std::string_view bytes) {
  std::string result;
  std::string error;
  expect_ok(rc::sha256_hex(bytes, &result, &error), error, "SHA-256");
  return result;
}

objstore::Status status(objstore::Errors code, const char *message) {
  return objstore::Status(code, 0, message);
}

class FakeIo final : public rc::ConditionalIo {
 public:
  struct Stored {
    std::string body;
    std::string etag;
  };

  objstore::ExactObjectResult get(std::string_view key,
                                  uint64_t max_bytes) override {
    reads.emplace_back(key);
    read_limits.push_back(max_bytes);
    if (blocked.contains(std::string(key))) {
      return objstore::ExactObjectResult::transient_unavailable(status(
          objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED, "blocked"));
    }
    const auto found = objects.find(std::string(key));
    if (found == objects.end()) {
      return objstore::ExactObjectResult::not_found(
          status(objstore::SE_NO_SUCH_KEY, "absent"));
    }
    const auto reported = reported_sizes.find(std::string(key));
    const uint64_t reported_size =
        reported == reported_sizes.end()
            ? static_cast<uint64_t>(found->second.body.size())
            : reported->second;
    if (reported_size > max_bytes) {
      return objstore::ExactObjectResult::permanent_error(
          status(objstore::SE_UNEXPECTED, "bounded exact GET exceeded"));
    }
    return objstore::ExactObjectResult::found(found->second.body,
                                               found->second.etag);
  }

  objstore::ConditionalPutResult put(
      std::string_view, std::string_view,
      const objstore::ConditionalPutCondition &) override {
    return objstore::ConditionalPutResult::unsupported();
  }

  void store(std::string key, std::string body,
             std::string etag = "\"fixture\"") {
    objects.insert_or_assign(std::move(key),
                             Stored{std::move(body), std::move(etag)});
  }

  std::unordered_map<std::string, Stored> objects;
  std::unordered_map<std::string, uint64_t> reported_sizes;
  std::unordered_set<std::string> blocked;
  std::vector<std::string> reads;
  std::vector<uint64_t> read_limits;
};

struct ManifestObject {
  rc::TransitionManifest value;
  std::string body;
  rc::ObjectRef object;
};

struct Fixture {
  rc::StreamIdentity stream;
  rc::Writer writer1;
  rc::Writer writer2;
  rc::WriterEpoch epoch1;
  rc::WriterEpoch epoch2;
  std::string epoch1_body;
  std::string epoch2_body;
  rc::SnapshotManifest snapshot1;
  std::string snapshot1_body;
  rc::SnapshotRef snapshot1_ref;
  ManifestObject bootstrap;
  rc::Head head1;
  std::string head1_body;
  ManifestObject log;
  rc::Head head2;
  std::string head2_body;
  rc::SnapshotManifest snapshot2;
  std::string snapshot2_body;
  rc::SnapshotRef snapshot2_ref;
  ManifestObject takeover;
  rc::Head head3;
  std::string head3_body;
};

rc::SegmentTip snapshot_tip(std::string id, rc::Cursor cursor) {
  rc::SegmentTip result;
  result.kind = rc::SegmentTipKind::SNAPSHOT_ROOT;
  result.snapshot_id = std::move(id);
  result.cursor = std::move(cursor);
  return result;
}

rc::SegmentTip segment_tip(const rc::SegmentRef &segment) {
  rc::SegmentTip result;
  result.kind = rc::SegmentTipKind::SEGMENT;
  result.key = segment.key;
  result.size = segment.size;
  result.sha256 = segment.sha256;
  result.sequence = segment.sequence;
  return result;
}

rc::SnapshotManifest make_snapshot(const rc::StreamIdentity &stream,
                                   std::string id, rc::Writer writer,
                                   rc::Cursor cursor, rc::LogAnchor anchor,
                                   std::string_view variant) {
  rc::SnapshotManifest result;
  result.snapshot_id = std::move(id);
  result.writer = std::move(writer);
  result.cursor = cursor;
  result.log_anchor = std::move(anchor);
  result.server_identity.server_uuid =
      "00112233-4455-6677-8899-aabbccddeeff";
  result.deployment_fingerprints.startup_config_sha256 = hash("startup");
  result.deployment_fingerprints.server_build = "wesql-9.7-recovery-test";
  result.deployment_fingerprints.plugin_component_set_sha256 = hash("plugins");
  result.deployment_fingerprints.keyring_config_sha256 = hash("keyring");
  result.deployment_fingerprints.tls_config_sha256 = hash("tls");
  std::string error;
  expect_ok(rc::gtid_digest("", &result.gtid_executed, &error), error,
            "empty snapshot GTID");

  const std::string seed(cursor.pos, 's');
  result.binlog_seed.file = cursor.file;
  result.binlog_seed.cursor = cursor;
  result.binlog_seed.size = seed.size();
  result.binlog_seed.sha256 = hash(seed);
  expect_ok(rc::binlog_seed_object_key(
                stream, result.snapshot_id, cursor,
                result.binlog_seed.sha256, &result.binlog_seed.key, &error),
            error, "binlog seed key");

  const std::vector<std::pair<std::string, std::string>> components{
      {"innodb", "data/ibdata1"},
      {"mysql-dd", "mysql/dd.ibd"},
      {"smartengine-meta", "smartengine/meta"},
      {"smartengine-wal", "smartengine/wal"},
  };
  for (const auto &[component, path] : components) {
    rc::SnapshotObject object;
    object.component = component;
    object.relative_path = path;
    const std::string payload =
        component + "-" + std::string(variant) + "-payload";
    object.size = payload.size();
    object.sha256 = hash(payload);
    object.format = "raw-v1";
    expect_ok(rc::snapshot_object_key(stream, result.snapshot_id, object,
                                      &object.key, &error),
              error, "snapshot object key");
    result.objects.push_back(std::move(object));
  }
  return result;
}

std::string serialize_snapshot(const rc::StreamIdentity &stream,
                               const rc::SnapshotManifest &snapshot) {
  std::string body;
  std::string error;
  expect_ok(rc::serialize_snapshot_manifest(stream, snapshot, &body, &error),
            error, "serialize snapshot");
  return body;
}

rc::SnapshotRef snapshot_ref(const rc::StreamIdentity &stream,
                             const rc::SnapshotManifest &snapshot,
                             std::string_view body) {
  rc::SnapshotRef result;
  result.id = snapshot.snapshot_id;
  result.cursor = snapshot.cursor;
  result.manifest_size = body.size();
  result.manifest_sha256 = hash(body);
  std::string error;
  expect_ok(rc::snapshot_manifest_key(stream, result.id,
                                      result.manifest_sha256,
                                      &result.manifest_key, &error),
            error, "snapshot manifest key");
  return result;
}

ManifestObject finalize_manifest(const rc::StreamIdentity &stream,
                                 rc::TransitionManifest value,
                                 uint64_t base_manifest_bytes) {
  ManifestObject result;
  result.value = std::move(value);
  std::string error;
  expect_ok(rc::stabilize_transition_manifest(
                stream, base_manifest_bytes, &result.value, &result.body,
                &error),
            error, "stabilize transition manifest");
  result.object.size = result.body.size();
  result.object.sha256 = hash(result.body);
  expect_ok(rc::transition_manifest_key(
                stream, result.value.writer, result.value.generation,
                result.object.sha256, &result.object.key, &error),
            error, "transition manifest key");
  return result;
}

rc::Head make_head(const ManifestObject &manifest) {
  rc::Head result;
  result.generation = manifest.value.generation;
  result.writer = manifest.value.writer;
  result.parent = manifest.value.head_parent;
  result.manifest = manifest.object;
  result.recovery_window = manifest.value.recovery_window;
  result.segment_tip = manifest.value.segment_tip;
  result.base_cursor = manifest.value.base_cursor;
  result.durable_cursor = manifest.value.durable_cursor;
  result.snapshot = manifest.value.snapshot;
  return result;
}

std::string serialize_head(const rc::StreamIdentity &stream,
                           const rc::Head &head) {
  std::string body;
  std::string error;
  expect_ok(rc::serialize_head(stream, head, &body, &error), error,
            "serialize HEAD");
  return body;
}

rc::HeadParent parent_of(const rc::Head &head, std::string_view body,
                         std::string etag) {
  return {head.generation, std::move(etag), hash(body)};
}

rc::SegmentRef make_segment(const rc::StreamIdentity &stream,
                            const rc::Writer &writer, uint64_t sequence,
                            rc::SegmentTip previous, std::string file,
                            uint64_t start, uint64_t end) {
  rc::SegmentRef result;
  result.sequence = sequence;
  result.source = {std::move(file), start, end};
  result.size = end - start;
  result.sha256 = hash(std::string(result.size, 'b'));
  result.previous_segment = std::move(previous);
  result.transaction_count = 1;
  std::string error;
  expect_ok(rc::gtid_digest("", &result.gtid_set, &error), error,
            "segment GTID");
  expect_ok(rc::xid_digest({}, &result.xids, &error), error, "segment XID");
  expect_ok(rc::segment_object_key(stream, writer, result.source,
                                   result.sha256, &result.key, &error),
            error, "segment key");
  return result;
}

Fixture make_fixture() {
  Fixture result;
  std::string error;
  expect_ok(rc::build_stream_identity("repo-1", "branch-1", "cluster/main",
                                      &result.stream, &error),
            error, "stream identity");
  result.writer1 = {"11111111111111111111111111111111", 1};
  result.writer2 = {"22222222222222222222222222222222", 2};
  result.epoch1 = {1, result.writer1.id, 0};
  result.epoch2 = {2, result.writer2.id, 1};
  expect_ok(rc::serialize_writer_epoch(result.stream, result.epoch1,
                                       &result.epoch1_body, &error),
            error, "epoch 1");
  expect_ok(rc::serialize_writer_epoch(result.stream, result.epoch2,
                                       &result.epoch2_body, &error),
            error, "epoch 2");

  const rc::Cursor cursor1{"binlog.000001", 4};
  rc::LogAnchor empty;
  empty.kind = rc::LogAnchorKind::EMPTY_BASE;
  empty.cursor = cursor1;
  result.snapshot1 = make_snapshot(
      result.stream, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", result.writer1,
      cursor1, empty, "one");
  result.snapshot1_body = serialize_snapshot(result.stream, result.snapshot1);
  result.snapshot1_ref =
      snapshot_ref(result.stream, result.snapshot1, result.snapshot1_body);

  rc::TransitionManifest bootstrap;
  bootstrap.kind = rc::ManifestKind::BOOTSTRAP;
  bootstrap.generation = 1;
  bootstrap.writer = result.writer1;
  bootstrap.recovery_window = {1, 1, 0};
  bootstrap.segment_tip = snapshot_tip(result.snapshot1.snapshot_id, cursor1);
  bootstrap.snapshot = result.snapshot1_ref;
  bootstrap.base_cursor = cursor1;
  bootstrap.durable_cursor = cursor1;
  result.bootstrap = finalize_manifest(result.stream, std::move(bootstrap), 0);
  result.head1 = make_head(result.bootstrap);
  result.head1_body = serialize_head(result.stream, result.head1);

  rc::SegmentRef segment = make_segment(
      result.stream, result.writer1, 1, result.bootstrap.value.segment_tip,
      cursor1.file, 4, 8);
  rc::TransitionManifest log;
  log.kind = rc::ManifestKind::LOG_TRANSITION;
  log.generation = 2;
  log.writer = result.writer1;
  log.head_parent = parent_of(result.head1, result.head1_body, "\"head-1\"");
  log.previous = rc::ManifestRef{
      1, result.bootstrap.object.key, result.bootstrap.object.size,
      result.bootstrap.object.sha256};
  log.recovery_window = {2, 1, 1};
  log.segment_tip = segment_tip(segment);
  log.snapshot = result.snapshot1_ref;
  log.base_cursor = cursor1;
  log.durable_cursor = {cursor1.file, 8};
  log.segments.push_back(std::move(segment));
  result.log = finalize_manifest(result.stream, std::move(log),
                                 result.bootstrap.object.size);
  result.head2 = make_head(result.log);
  result.head2_body = serialize_head(result.stream, result.head2);

  const rc::Cursor cursor2{"binlog.000001", 8};
  rc::LogAnchor boundary;
  boundary.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  boundary.generation = result.log.value.generation;
  boundary.manifest = result.log.object;
  boundary.cursor = cursor2;
  result.snapshot2 = make_snapshot(
      result.stream, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", result.writer2,
      cursor2, boundary, "two");
  result.snapshot2_body = serialize_snapshot(result.stream, result.snapshot2);
  result.snapshot2_ref =
      snapshot_ref(result.stream, result.snapshot2, result.snapshot2_body);

  rc::TransitionManifest takeover;
  takeover.kind = rc::ManifestKind::SNAPSHOT;
  takeover.generation = 3;
  takeover.writer = result.writer2;
  takeover.head_parent =
      parent_of(result.head2, result.head2_body, "\"head-2\"");
  takeover.previous = rc::ManifestRef{
      result.log.value.generation, result.log.object.key,
      result.log.object.size, result.log.object.sha256};
  takeover.recovery_window = {2, 1, 0};
  takeover.segment_tip = result.log.value.segment_tip;
  takeover.snapshot = result.snapshot2_ref;
  takeover.base_cursor = cursor2;
  takeover.durable_cursor = cursor2;
  result.takeover = finalize_manifest(result.stream, std::move(takeover),
                                      result.log.object.size);
  result.head3 = make_head(result.takeover);
  result.head3_body = serialize_head(result.stream, result.head3);
  return result;
}

void add_common(FakeIo *io, const Fixture &fixture) {
  io->store(fixture.snapshot1_ref.manifest_key, fixture.snapshot1_body);
  io->store(fixture.bootstrap.object.key, fixture.bootstrap.body);
  io->store(fixture.log.object.key, fixture.log.body);
}

void install_log_head(FakeIo *io, const Fixture &fixture) {
  add_common(io, fixture);
  io->store(fixture.stream.remote_prefix + "/HEAD", fixture.head2_body,
            "\"head-2\"");
  io->store(fixture.stream.remote_prefix + "/WRITER_EPOCH",
            fixture.epoch1_body, "\"epoch-1\"");
}

void install_takeover_head(FakeIo *io, const Fixture &fixture) {
  add_common(io, fixture);
  io->store(fixture.snapshot2_ref.manifest_key, fixture.snapshot2_body);
  io->store(fixture.takeover.object.key, fixture.takeover.body);
  io->store(fixture.stream.remote_prefix + "/HEAD", fixture.head3_body,
            "\"head-3\"");
  io->store(fixture.stream.remote_prefix + "/WRITER_EPOCH",
            fixture.epoch2_body, "\"epoch-2\"");
}

rc::RecoveryReadResult read(FakeIo *io, const rc::StreamIdentity &stream,
                            rc::RecoveryPlan *plan) {
  rc::RecoveryChainReader reader(io, stream);
  return reader.read(plan);
}

uint64_t read_limit_for(const FakeIo &io, std::string_view key) {
  for (size_t index = 0; index < io.reads.size(); ++index) {
    if (io.reads[index] == key) return io.read_limits[index];
  }
  return 0;
}

void test_empty_routes_head_first(const Fixture &fixture) {
  {
    FakeIo io;
    rc::RecoveryPlan plan;
    const auto result = read(&io, fixture.stream, &plan);
    expect(result.outcome == rc::RecoveryReadOutcome::EMPTY,
           "absent HEAD/epoch must be EMPTY");
    expect(io.reads.size() == 2 &&
               io.reads.front() == fixture.stream.remote_prefix + "/HEAD",
           "empty routing must read HEAD before epoch");
  }
  {
    FakeIo io;
    io.store(fixture.stream.remote_prefix + "/WRITER_EPOCH",
             fixture.epoch1_body);
    rc::RecoveryPlan plan;
    const auto result = read(&io, fixture.stream, &plan);
    expect(result.outcome == rc::RecoveryReadOutcome::EMPTY,
           "absent HEAD with retained epoch must remain EMPTY");
  }
}

void test_log_chain_ready(const Fixture &fixture) {
  FakeIo io;
  install_log_head(&io, fixture);
  rc::RecoveryPlan plan;
  const auto result = read(&io, fixture.stream, &plan);
  expect(result.ready(), "BOOTSTRAP plus LOG chain must be READY");
  expect(plan.manifests.size() == 2 && plan.replay_segments.size() == 1,
         "LOG recovery plan counts changed");
  expect(plan.replay_segments.front() == fixture.log.value.segments.front(),
         "LOG recovery plan selected the wrong segment");
}

void test_oversized_metadata_is_rejected_during_read(
    const Fixture &fixture) {
  const std::string head_key = fixture.stream.remote_prefix + "/HEAD";
  const std::string epoch_key =
      fixture.stream.remote_prefix + "/WRITER_EPOCH";
  {
    FakeIo io;
    install_log_head(&io, fixture);
    io.reported_sizes[head_key] = rc::kHeadMaxBytes + 1;
    rc::RecoveryPlan plan;
    expect(read(&io, fixture.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::CORRUPT &&
               read_limit_for(io, head_key) == rc::kHeadMaxBytes,
           "oversized HEAD was not transfer-bounded");
  }
  {
    FakeIo io;
    install_log_head(&io, fixture);
    io.reported_sizes[epoch_key] = rc::kWriterEpochMaxBytes + 1;
    rc::RecoveryPlan plan;
    expect(read(&io, fixture.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::CORRUPT &&
               read_limit_for(io, epoch_key) == rc::kWriterEpochMaxBytes,
           "oversized WRITER_EPOCH was not transfer-bounded");
  }
  {
    FakeIo io;
    install_log_head(&io, fixture);
    io.reported_sizes[fixture.snapshot1_ref.manifest_key] =
        rc::kSnapshotManifestMaxBytes + 1ULL;
    rc::RecoveryPlan plan;
    expect(read(&io, fixture.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::CORRUPT &&
               read_limit_for(io, fixture.snapshot1_ref.manifest_key) ==
                   rc::kSnapshotManifestMaxBytes,
           "oversized snapshot manifest was not transfer-bounded");
  }
  {
    FakeIo io;
    install_log_head(&io, fixture);
    io.reported_sizes[fixture.log.object.key] =
        rc::kDeltaManifestMaxBytes + 1ULL;
    rc::RecoveryPlan plan;
    expect(read(&io, fixture.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::CORRUPT &&
               read_limit_for(io, fixture.log.object.key) ==
                   rc::kDeltaManifestMaxBytes,
           "oversized transition manifest was not transfer-bounded");
  }
}

void test_takeover_chain_ready(const Fixture &fixture) {
  FakeIo io;
  install_takeover_head(&io, fixture);
  rc::RecoveryPlan plan;
  const auto result = read(&io, fixture.stream, &plan);
  expect(result.ready(), "takeover SNAPSHOT chain must be READY");
  expect(plan.manifests.size() == 2 && plan.replay_segments.empty(),
         "takeover must rebase to two manifests and zero replay segments");
}

void test_missing_blocked_and_hash_mismatch(const Fixture &fixture) {
  {
    FakeIo io;
    install_log_head(&io, fixture);
    io.objects.erase(fixture.log.object.key);
    rc::RecoveryPlan plan;
    expect(read(&io, fixture.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::CORRUPT,
           "missing required manifest must be CORRUPT");
  }
  {
    FakeIo io;
    install_log_head(&io, fixture);
    io.blocked.insert(fixture.log.object.key);
    rc::RecoveryPlan plan;
    expect(read(&io, fixture.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::BLOCKED,
           "temporarily unreadable manifest must be BLOCKED");
  }
  {
    FakeIo io;
    install_log_head(&io, fixture);
    io.objects.at(fixture.log.object.key).body.push_back('x');
    rc::RecoveryPlan plan;
    expect(read(&io, fixture.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::CORRUPT,
           "manifest hash mismatch must be CORRUPT");
  }
}

void test_parent_hash_is_reconstructed(const Fixture &fixture) {
  Fixture changed = fixture;
  changed.log.value.head_parent->sha256 = hash("wrong prior HEAD");
  changed.log = finalize_manifest(changed.stream, changed.log.value,
                                  changed.bootstrap.object.size);
  changed.head2 = make_head(changed.log);
  changed.head2_body = serialize_head(changed.stream, changed.head2);

  FakeIo io;
  install_log_head(&io, changed);
  rc::RecoveryPlan plan;
  expect(read(&io, changed.stream, &plan).outcome ==
             rc::RecoveryReadOutcome::CORRUPT,
         "forged head_parent SHA must be CORRUPT");
}

void test_snapshot_component_and_generation_gap_are_corrupt(
    const Fixture &fixture) {
  {
    Fixture changed = fixture;
    changed.snapshot1.objects.pop_back();
    changed.snapshot1_body =
        serialize_snapshot(changed.stream, changed.snapshot1);
    changed.snapshot1_ref = snapshot_ref(changed.stream, changed.snapshot1,
                                         changed.snapshot1_body);
    changed.bootstrap.value.snapshot = changed.snapshot1_ref;
    changed.bootstrap =
        finalize_manifest(changed.stream, changed.bootstrap.value, 0);
    changed.head1 = make_head(changed.bootstrap);
    changed.head1_body = serialize_head(changed.stream, changed.head1);

    FakeIo io;
    io.store(changed.snapshot1_ref.manifest_key, changed.snapshot1_body);
    io.store(changed.bootstrap.object.key, changed.bootstrap.body);
    io.store(changed.stream.remote_prefix + "/HEAD", changed.head1_body,
             "\"head-1\"");
    io.store(changed.stream.remote_prefix + "/WRITER_EPOCH",
             changed.epoch1_body, "\"epoch-1\"");
    rc::RecoveryPlan plan;
    expect(read(&io, changed.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::CORRUPT,
           "snapshot missing a required component must be CORRUPT");
  }
  {
    Fixture changed = fixture;
    std::string invalid = changed.log.body;
    const std::string needle = "\"previous\":{\"generation\":1";
    const size_t offset = invalid.find(needle);
    expect(offset != std::string::npos,
           "fixture LOG previous generation was not found");
    invalid[offset + needle.size() - 1] = '0';

    rc::Head head = changed.head2;
    head.manifest.size = invalid.size();
    head.manifest.sha256 = hash(invalid);
    std::string error;
    expect_ok(rc::transition_manifest_key(
                  changed.stream, head.writer, head.generation,
                  head.manifest.sha256, &head.manifest.key, &error),
              error, "invalid generation manifest key");

    FakeIo io;
    add_common(&io, changed);
    io.store(head.manifest.key, invalid);
    io.store(changed.stream.remote_prefix + "/HEAD",
             serialize_head(changed.stream, head), "\"head-2\"");
    io.store(changed.stream.remote_prefix + "/WRITER_EPOCH",
             changed.epoch1_body, "\"epoch-1\"");
    rc::RecoveryPlan plan;
    expect(read(&io, changed.stream, &plan).outcome ==
               rc::RecoveryReadOutcome::CORRUPT,
           "manifest generation gap must be CORRUPT");
  }
}

void test_missing_anchor_is_corrupt(const Fixture &fixture) {
  Fixture changed = fixture;
  rc::LogAnchor missing;
  missing.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  missing.generation = 1;
  missing.cursor = {"binlog.000001", 4};
  const std::string absent_sha = hash("absent manifest");
  std::string error;
  std::string absent_key;
  expect_ok(rc::transition_manifest_key(changed.stream, changed.writer1, 1,
                                        absent_sha, &absent_key, &error),
            error, "absent anchor key");
  missing.manifest = rc::ObjectRef{absent_key, 1, absent_sha};
  rc::SnapshotManifest snapshot = make_snapshot(
      changed.stream, "cccccccccccccccccccccccccccccccc", changed.writer1,
      missing.cursor, missing, "missing-anchor");
  const std::string snapshot_body = serialize_snapshot(changed.stream, snapshot);
  const rc::SnapshotRef ref =
      snapshot_ref(changed.stream, snapshot, snapshot_body);

  rc::TransitionManifest transition;
  transition.kind = rc::ManifestKind::SNAPSHOT;
  transition.generation = 3;
  transition.writer = changed.writer1;
  transition.head_parent =
      parent_of(changed.head2, changed.head2_body, "\"head-2\"");
  transition.previous = rc::ManifestRef{
      changed.log.value.generation, changed.log.object.key,
      changed.log.object.size, changed.log.object.sha256};
  transition.recovery_window = {3, 1, 1};
  transition.segment_tip = changed.log.value.segment_tip;
  transition.snapshot = ref;
  transition.base_cursor = snapshot.cursor;
  transition.durable_cursor = changed.log.value.durable_cursor;
  ManifestObject object = finalize_manifest(
      changed.stream, std::move(transition),
      changed.log.object.size + changed.bootstrap.object.size);
  const rc::Head head = make_head(object);

  FakeIo io;
  add_common(&io, changed);
  io.store(ref.manifest_key, snapshot_body);
  io.store(object.object.key, object.body);
  io.store(changed.stream.remote_prefix + "/HEAD",
           serialize_head(changed.stream, head), "\"head-3\"");
  io.store(changed.stream.remote_prefix + "/WRITER_EPOCH",
           changed.epoch1_body, "\"epoch-1\"");
  rc::RecoveryPlan plan;
  expect(read(&io, changed.stream, &plan).outcome ==
             rc::RecoveryReadOutcome::CORRUPT,
         "missing exact snapshot anchor must be CORRUPT");
}

void test_takeover_fingerprint_change_is_corrupt(const Fixture &fixture) {
  Fixture changed = fixture;
  changed.snapshot2.deployment_fingerprints.startup_config_sha256 =
      hash("changed startup");
  changed.snapshot2_body =
      serialize_snapshot(changed.stream, changed.snapshot2);
  changed.snapshot2_ref =
      snapshot_ref(changed.stream, changed.snapshot2, changed.snapshot2_body);
  changed.takeover.value.snapshot = changed.snapshot2_ref;
  changed.takeover = finalize_manifest(changed.stream, changed.takeover.value,
                                      changed.log.object.size);
  changed.head3 = make_head(changed.takeover);
  changed.head3_body = serialize_head(changed.stream, changed.head3);

  FakeIo io;
  install_takeover_head(&io, changed);
  rc::RecoveryPlan plan;
  expect(read(&io, changed.stream, &plan).outcome ==
             rc::RecoveryReadOutcome::CORRUPT,
         "takeover fingerprint change must be CORRUPT");
}

void test_cross_file_replay_is_contiguous(const Fixture &fixture) {
  Fixture changed = fixture;
  const rc::SegmentRef segment = make_segment(
      changed.stream, changed.writer1, 2, changed.log.value.segment_tip,
      "binlog.000002", 0, 8);
  rc::TransitionManifest next;
  next.kind = rc::ManifestKind::LOG_TRANSITION;
  next.generation = 3;
  next.writer = changed.writer1;
  next.head_parent =
      parent_of(changed.head2, changed.head2_body, "\"head-2\"");
  next.previous = rc::ManifestRef{
      changed.log.value.generation, changed.log.object.key,
      changed.log.object.size, changed.log.object.sha256};
  next.recovery_window = {3, 1, 2};
  next.segment_tip = segment_tip(segment);
  next.snapshot = changed.snapshot1_ref;
  next.base_cursor = changed.snapshot1_ref.cursor;
  next.durable_cursor = {"binlog.000002", 8};
  next.segments.push_back(segment);
  const ManifestObject object = finalize_manifest(
      changed.stream, std::move(next),
      changed.bootstrap.object.size + changed.log.object.size);
  const rc::Head head = make_head(object);

  FakeIo io;
  add_common(&io, changed);
  io.store(object.object.key, object.body);
  io.store(changed.stream.remote_prefix + "/HEAD",
           serialize_head(changed.stream, head), "\"head-3\"");
  io.store(changed.stream.remote_prefix + "/WRITER_EPOCH",
           changed.epoch1_body, "\"epoch-1\"");
  rc::RecoveryPlan plan;
  const auto result = read(&io, changed.stream, &plan);
  expect(result.ready() && plan.replay_segments.size() == 2,
         "adjacent cross-file replay must be READY");
}

}  // namespace

int main() {
  const Fixture fixture = make_fixture();
  test_empty_routes_head_first(fixture);
  test_log_chain_ready(fixture);
  test_oversized_metadata_is_rejected_during_read(fixture);
  test_takeover_chain_ready(fixture);
  test_missing_blocked_and_hash_mismatch(fixture);
  test_parent_hash_is_reconstructed(fixture);
  test_snapshot_component_and_generation_gap_are_corrupt(fixture);
  test_missing_anchor_is_corrupt(fixture);
  test_takeover_fingerprint_change_is_corrupt(fixture);
  test_cross_file_replay_is_contiguous(fixture);
  std::cout << "remote commit recovery tests passed\n";
  return EXIT_SUCCESS;
}
