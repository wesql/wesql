/* Copyright (c) 2026, ApeCloud Inc Holding Limited.

   Standalone focused tests for the remote commit v2 protocol codec. */

#include "sql/remote_commit/protocol_codec.h"

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rc = wesql::remote_commit;

namespace {

void expect(bool condition, std::string_view message) {
  if (!condition) throw std::runtime_error(std::string(message));
}

void expect_ok(bool result, const std::string &error,
               std::string_view operation) {
  if (!result) {
    throw std::runtime_error(std::string(operation) + ": " + error);
  }
}

void expect_rejected(bool result, const std::string &error,
                     std::string_view operation) {
  if (result || error.empty()) {
    throw std::runtime_error(std::string(operation) + " was not rejected");
  }
}

std::string hash(std::string_view bytes) {
  std::string result;
  std::string error;
  expect_ok(rc::sha256_hex(bytes, &result, &error), error, "SHA-256");
  return result;
}

rc::StreamIdentity make_stream() {
  rc::StreamIdentity stream;
  std::string error;
  expect_ok(rc::build_stream_identity("repo-1", "branch_2", "cluster/main",
                                      &stream, &error),
            error, "build stream identity");
  return stream;
}

rc::SegmentTip snapshot_root(std::string id, rc::Cursor cursor) {
  rc::SegmentTip tip;
  tip.kind = rc::SegmentTipKind::SNAPSHOT_ROOT;
  tip.snapshot_id = std::move(id);
  tip.cursor = std::move(cursor);
  return tip;
}

rc::SnapshotManifest make_snapshot(const rc::StreamIdentity &stream,
                                   std::string snapshot_id, rc::Writer writer,
                                   rc::Cursor cursor, rc::LogAnchor anchor,
                                   std::string_view payload_suffix) {
  rc::SnapshotManifest snapshot;
  snapshot.snapshot_id = std::move(snapshot_id);
  snapshot.writer = std::move(writer);
  snapshot.cursor = cursor;
  snapshot.log_anchor = std::move(anchor);
  snapshot.server_identity.server_uuid =
      "00112233-4455-6677-8899-aabbccddeeff";
  snapshot.deployment_fingerprints.startup_config_sha256 = hash("startup");
  snapshot.deployment_fingerprints.server_build = "wesql-9.7-test";
  snapshot.deployment_fingerprints.plugin_component_set_sha256 =
      hash("plugins");
  snapshot.deployment_fingerprints.keyring_config_sha256 = hash("keyring");
  snapshot.deployment_fingerprints.tls_config_sha256 = hash("tls");
  std::string error;
  expect_ok(rc::gtid_digest("", &snapshot.gtid_executed, &error), error,
            "empty GTID digest");

  const std::string seed(cursor.pos, 's');
  snapshot.binlog_seed.file = cursor.file;
  snapshot.binlog_seed.cursor = cursor;
  snapshot.binlog_seed.size = cursor.pos;
  snapshot.binlog_seed.sha256 = hash(seed);
  expect_ok(rc::binlog_seed_object_key(
                stream, snapshot.snapshot_id, cursor,
                snapshot.binlog_seed.sha256, &snapshot.binlog_seed.key, &error),
            error, "derive seed key");

  rc::SnapshotObject object;
  object.component = "innodb";
  object.ordinal = 0;
  object.relative_path = "data/ibdata1";
  object.size = 7 + payload_suffix.size();
  object.sha256 = hash(std::string("object-") + std::string(payload_suffix));
  object.format = "raw-v1";
  expect_ok(rc::snapshot_object_key(stream, snapshot.snapshot_id, object,
                                    &object.key, &error),
            error, "derive snapshot object key");
  snapshot.objects.push_back(std::move(object));

  rc::SmartengineExtentRef extent;
  extent.ordinal = 0;
  extent.writer_epoch = 1;
  extent.allocation_seq = "2";
  extent.database_name_hex = "74657374";
  extent.index_id = "42";
  extent.object_id = "123456";
  extent.size = 6;
  extent.sha256 = hash("extent");
  expect_ok(rc::smartengine_extent_object_key(stream, extent, &extent.key,
                                               &error),
            error, "derive extent key");
  snapshot.smartengine_extents.push_back(std::move(extent));
  return snapshot;
}

std::string serialize_snapshot(const rc::StreamIdentity &stream,
                               const rc::SnapshotManifest &snapshot) {
  std::string body;
  std::string error;
  expect_ok(rc::serialize_snapshot_manifest(stream, snapshot, &body, &error),
            error, "serialize snapshot");
  return body;
}

std::string stabilize_bootstrap(const rc::StreamIdentity &stream,
                                rc::TransitionManifest *manifest) {
  std::string body;
  std::string error;
  expect_ok(rc::stabilize_transition_manifest(stream, 0, manifest, &body,
                                              &error),
            error, "stabilize bootstrap manifest_bytes");
  return body;
}

struct Fixture {
  rc::StreamIdentity stream;
  rc::WriterEpoch epoch1;
  rc::SnapshotManifest snapshot1;
  std::string snapshot1_body;
  rc::SnapshotRef snapshot1_ref;
  rc::TransitionManifest bootstrap;
  std::string bootstrap_body;
  std::string bootstrap_key;
  rc::Head head1;
  std::string head1_body;
  rc::TransitionManifest log;
  std::string log_body;
  std::string log_key;
  rc::SnapshotManifest snapshot2;
  std::string snapshot2_body;
  rc::SnapshotRef snapshot2_ref;
  rc::TransitionManifest snapshot_transition;
  std::string snapshot_transition_body;
  std::string snapshot_transition_key;
  rc::Head head3;
  std::string head3_body;
};

Fixture make_fixture() {
  Fixture result;
  result.stream = make_stream();
  const rc::Writer writer1{"11111111111111111111111111111111", 1};
  const rc::Cursor cursor1{"binlog.000001", 4};
  result.epoch1 = {1, writer1.id, 0};

  rc::LogAnchor empty_anchor;
  empty_anchor.kind = rc::LogAnchorKind::EMPTY_BASE;
  empty_anchor.cursor = cursor1;
  result.snapshot1 = make_snapshot(
      result.stream, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", writer1, cursor1,
      empty_anchor, "one");
  result.snapshot1_body = serialize_snapshot(result.stream, result.snapshot1);
  result.snapshot1_ref.id = result.snapshot1.snapshot_id;
  result.snapshot1_ref.cursor = cursor1;
  result.snapshot1_ref.manifest_size = result.snapshot1_body.size();
  result.snapshot1_ref.manifest_sha256 = hash(result.snapshot1_body);
  std::string error;
  expect_ok(rc::snapshot_manifest_key(
                result.stream, result.snapshot1.snapshot_id,
                result.snapshot1_ref.manifest_sha256,
                &result.snapshot1_ref.manifest_key, &error),
            error, "derive snapshot1 manifest key");

  result.bootstrap.kind = rc::ManifestKind::BOOTSTRAP;
  result.bootstrap.generation = 1;
  result.bootstrap.writer = writer1;
  result.bootstrap.recovery_window = {1, 1, 0};
  result.bootstrap.segment_tip =
      snapshot_root(result.snapshot1.snapshot_id, cursor1);
  result.bootstrap.snapshot = result.snapshot1_ref;
  result.bootstrap.base_cursor = cursor1;
  result.bootstrap.durable_cursor = cursor1;
  result.bootstrap_body = stabilize_bootstrap(result.stream, &result.bootstrap);
  const std::string bootstrap_sha = hash(result.bootstrap_body);
  expect_ok(rc::transition_manifest_key(result.stream, writer1, 1,
                                        bootstrap_sha, &result.bootstrap_key,
                                        &error),
            error, "derive bootstrap key");

  result.head1.generation = 1;
  result.head1.writer = writer1;
  result.head1.manifest =
      {result.bootstrap_key, result.bootstrap_body.size(), bootstrap_sha};
  result.head1.recovery_window = result.bootstrap.recovery_window;
  result.head1.segment_tip = result.bootstrap.segment_tip;
  result.head1.base_cursor = cursor1;
  result.head1.durable_cursor = cursor1;
  result.head1.snapshot = result.snapshot1_ref;
  expect_ok(rc::serialize_head(result.stream, result.head1, &result.head1_body,
                               &error),
            error, "serialize head1");

  rc::SegmentRef segment;
  segment.sequence = 1;
  segment.size = 4;
  segment.sha256 = hash("ABCD");
  segment.source = {cursor1.file, 4, 8};
  segment.previous_segment = result.bootstrap.segment_tip;
  segment.transaction_count = 3;
  expect_ok(rc::gtid_digest(
                "ffeeddcc-bbaa-9988-7766-554433221100:3:1-2",
                &segment.gtid_set, &error),
            error, "segment GTID digest");
  expect_ok(rc::xid_digest({10, 2, 2}, &segment.xids, &error), error,
            "segment XID digest");
  expect_ok(rc::segment_object_key(result.stream, writer1, segment.source,
                                   segment.sha256, &segment.key, &error),
            error, "derive segment key");

  const rc::HeadParent parent1{1, "\"etag-head-1\"",
                               hash(result.head1_body)};
  result.log.kind = rc::ManifestKind::LOG_TRANSITION;
  result.log.generation = 2;
  result.log.writer = writer1;
  result.log.head_parent = parent1;
  result.log.previous = rc::ManifestRef{1, result.bootstrap_key,
                                        result.bootstrap_body.size(),
                                        bootstrap_sha};
  result.log.recovery_window = {2, 5000, 1};
  result.log.segment_tip.kind = rc::SegmentTipKind::SEGMENT;
  result.log.segment_tip.key = segment.key;
  result.log.segment_tip.size = segment.size;
  result.log.segment_tip.sha256 = segment.sha256;
  result.log.segment_tip.sequence = segment.sequence;
  result.log.snapshot = result.snapshot1_ref;
  result.log.base_cursor = cursor1;
  result.log.durable_cursor = {cursor1.file, 8};
  result.log.segments.push_back(segment);
  expect_ok(rc::serialize_transition_manifest(result.stream, result.log,
                                              &result.log_body, &error),
            error, "serialize LOG manifest");
  const std::string log_sha = hash(result.log_body);
  expect_ok(rc::transition_manifest_key(result.stream, writer1, 2, log_sha,
                                        &result.log_key, &error),
            error, "derive LOG key");

  rc::Head head2;
  head2.generation = 2;
  head2.writer = writer1;
  head2.parent = parent1;
  head2.manifest = {result.log_key, result.log_body.size(), log_sha};
  head2.recovery_window = result.log.recovery_window;
  head2.segment_tip = result.log.segment_tip;
  head2.base_cursor = cursor1;
  head2.durable_cursor = result.log.durable_cursor;
  head2.snapshot = result.snapshot1_ref;
  std::string head2_body;
  expect_ok(rc::serialize_head(result.stream, head2, &head2_body, &error), error,
            "serialize head2");

  const rc::Writer writer2{"22222222222222222222222222222222", 2};
  const rc::Cursor cursor2{"binlog.000001", 8};
  rc::LogAnchor boundary;
  boundary.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  boundary.generation = 2;
  boundary.manifest = rc::ObjectRef{result.log_key, result.log_body.size(),
                                    log_sha};
  boundary.cursor = cursor2;
  result.snapshot2 = make_snapshot(
      result.stream, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", writer2, cursor2,
      boundary, "two");
  result.snapshot2_body = serialize_snapshot(result.stream, result.snapshot2);
  result.snapshot2_ref.id = result.snapshot2.snapshot_id;
  result.snapshot2_ref.cursor = cursor2;
  result.snapshot2_ref.manifest_size = result.snapshot2_body.size();
  result.snapshot2_ref.manifest_sha256 = hash(result.snapshot2_body);
  expect_ok(rc::snapshot_manifest_key(
                result.stream, result.snapshot2.snapshot_id,
                result.snapshot2_ref.manifest_sha256,
                &result.snapshot2_ref.manifest_key, &error),
            error, "derive snapshot2 manifest key");

  const rc::HeadParent parent2{2, "\"etag-head-2\"", hash(head2_body)};
  result.snapshot_transition.kind = rc::ManifestKind::SNAPSHOT;
  result.snapshot_transition.generation = 3;
  result.snapshot_transition.writer = writer2;
  result.snapshot_transition.head_parent = parent2;
  result.snapshot_transition.previous =
      rc::ManifestRef{2, result.log_key, result.log_body.size(), log_sha};
  result.snapshot_transition.recovery_window = {2, 5000, 0};
  result.snapshot_transition.segment_tip = result.log.segment_tip;
  result.snapshot_transition.snapshot = result.snapshot2_ref;
  result.snapshot_transition.base_cursor = cursor2;
  result.snapshot_transition.durable_cursor = cursor2;
  expect_ok(rc::serialize_transition_manifest(
                result.stream, result.snapshot_transition,
                &result.snapshot_transition_body, &error),
            error, "serialize SNAPSHOT transition");
  const std::string snapshot_transition_sha =
      hash(result.snapshot_transition_body);
  expect_ok(rc::transition_manifest_key(
                result.stream, writer2, 3, snapshot_transition_sha,
                &result.snapshot_transition_key, &error),
            error, "derive SNAPSHOT transition key");

  result.head3.generation = 3;
  result.head3.writer = writer2;
  result.head3.parent = parent2;
  result.head3.manifest = {result.snapshot_transition_key,
                           result.snapshot_transition_body.size(),
                           snapshot_transition_sha};
  result.head3.recovery_window = result.snapshot_transition.recovery_window;
  result.head3.segment_tip = result.snapshot_transition.segment_tip;
  result.head3.base_cursor = cursor2;
  result.head3.durable_cursor = cursor2;
  result.head3.snapshot = result.snapshot2_ref;
  expect_ok(rc::serialize_head(result.stream, result.head3, &result.head3_body,
                               &error),
            error, "serialize head3");
  return result;
}

void test_digest_vectors(const Fixture &fixture) {
  std::string error;
  expect(fixture.stream.stream_sha256 ==
             "6a4b38fba06671ae88afb0b1a86d6eff6fed2ca9ef268391de847b79dbff7a63",
         "stream identity SHA golden vector changed");
  expect(hash("") ==
             "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
         "empty SHA-256 vector changed");

  rc::GtidSetDigest empty;
  expect_ok(rc::gtid_digest("", &empty, &error), error, "empty GTID vector");
  expect(empty.canonical.empty(), "empty GTID canonical form changed");
  expect(empty.sha256 ==
             "12ae32cb1ec02d01eda3581b127c1fee3b0dc53572ed6baf239721a03d82e126",
         "empty GTID digest vector changed");

  rc::GtidSetDigest normalized;
  expect_ok(rc::gtid_digest(
                "FFEEDDCC-BBAA-9988-7766-554433221100:3:1-2,"
                "00112233-4455-6677-8899-AABBCCDDEEFF:7-8:6",
                &normalized, &error),
            error, "normalized GTID vector");
  expect(normalized.canonical ==
             "00112233-4455-6677-8899-aabbccddeeff:6-8,"
             "ffeeddcc-bbaa-9988-7766-554433221100:1-3",
         "GTID canonical normalization changed");
  expect(normalized.sha256 ==
             "125090c15317bb72b4b25a03e64080f7cb2d8f37e17545c439ac245ef7806956",
         "GTID digest vector changed");

  std::string xid_preimage;
  expect_ok(rc::xid_jcs_preimage({10, 2, 2}, &xid_preimage, &error), error,
            "XID preimage vector");
  expect(xid_preimage == "[\"2\",\"2\",\"10\"]",
         "XID numeric sorting changed");
  rc::XidDigest xids;
  expect_ok(rc::xid_digest({10, 2, 2}, &xids, &error), error,
            "XID digest vector");
  expect(xids.count == 3 &&
             xids.sha256 ==
                 "7daf668c7fe213eabe16a507e5215503a170e53bf592f5f99b1790def101d418",
         "XID digest golden vector changed");

  std::string segment_digest;
  expect_ok(rc::segment_refs_digest(fixture.log.segments, &segment_digest,
                                    &error),
            error, "segment-ref digest vector");
  expect(segment_digest ==
             "2f8f17e946ff63111bee7f2506f851e5dce2c4d3a4477a1c73659ce8109b946b",
         "segment-ref array digest changed");
}

void test_schema_golden_and_round_trip(const Fixture &fixture) {
  const auto expect_hash = [](std::string_view body, std::string_view expected,
                              std::string_view label) {
    expect(hash(body) == expected, label);
  };
  expect_hash(fixture.snapshot1_body,
              "f01635ca0ce9ce2cd8db01f03a59d086de3cce7067e72e7e7b589358db6fd1d2",
              "snapshot body/hash golden vector changed");
  expect_hash(fixture.bootstrap_body,
              "b630d1e55595595beea44e34823dace2e7697e3bb78580c5535a95cfdcb2b945",
              "BOOTSTRAP body/hash golden vector changed");
  expect_hash(fixture.log_body,
              "a0d6fed1628ec3071db3ad936027cea2d93578f8532ad1c89d015ef27b540f94",
              "LOG body/hash golden vector changed");
  expect_hash(fixture.snapshot_transition_body,
              "0b9581bce342f56072aedab6613fa9bdc162676d7e780017a8e226b319e0220f",
              "SNAPSHOT transition body/hash golden vector changed");
  expect_hash(fixture.head3_body,
              "c3764b706c9053ec3ffaf985b97bba87289da8ea48230331fc85b19f2e9c7bf1",
              "HEAD body/hash golden vector changed");

  std::string error;
  std::string epoch_body;
  expect_ok(rc::serialize_writer_epoch(fixture.stream, fixture.epoch1,
                                       &epoch_body, &error),
            error, "serialize WRITER_EPOCH");
  expect(epoch_body ==
             "{\"epoch\":1,\"format\":\"wesql.remote_commit.writer_epoch\","
             "\"previous_epoch\":0,\"stream_id\":\"r=repo-1/b=branch_2\","
             "\"version\":2,\"writer_id\":"
             "\"11111111111111111111111111111111\"}",
         "WRITER_EPOCH JCS body changed");
  rc::WriterEpoch parsed_epoch;
  expect_ok(rc::parse_writer_epoch(epoch_body, fixture.stream, &parsed_epoch,
                                   &error),
            error, "parse WRITER_EPOCH");
  expect(parsed_epoch == fixture.epoch1, "WRITER_EPOCH round trip changed");

  rc::SnapshotManifest parsed_snapshot;
  expect_ok(rc::parse_snapshot_manifest(
                fixture.snapshot1_body, fixture.stream,
                fixture.snapshot1_ref.manifest_key, &parsed_snapshot, &error),
            error, "parse snapshot");
  expect(parsed_snapshot == fixture.snapshot1, "snapshot round trip changed");
  expect_ok(rc::parse_snapshot_manifest(
                fixture.snapshot2_body, fixture.stream,
                fixture.snapshot2_ref.manifest_key, &parsed_snapshot, &error),
            error, "parse anchored snapshot");
  expect(parsed_snapshot == fixture.snapshot2,
         "anchored snapshot round trip changed");

  rc::TransitionManifest parsed_manifest;
  expect_ok(rc::parse_transition_manifest(
                fixture.bootstrap_body, fixture.stream, fixture.bootstrap_key,
                &parsed_manifest, &error),
            error, "parse BOOTSTRAP");
  expect(parsed_manifest == fixture.bootstrap, "BOOTSTRAP round trip changed");
  expect_ok(rc::parse_transition_manifest(fixture.log_body, fixture.stream,
                                          fixture.log_key, &parsed_manifest,
                                          &error),
            error, "parse LOG");
  expect(parsed_manifest == fixture.log, "LOG round trip changed");
  expect_ok(rc::parse_transition_manifest(
                fixture.snapshot_transition_body, fixture.stream,
                fixture.snapshot_transition_key, &parsed_manifest, &error),
            error, "parse SNAPSHOT transition");
  expect(parsed_manifest == fixture.snapshot_transition,
         "SNAPSHOT transition round trip changed");

  rc::Head parsed_head;
  expect_ok(rc::parse_head(fixture.head1_body, fixture.stream, &parsed_head,
                           &error),
            error, "parse generation-1 HEAD");
  expect(parsed_head == fixture.head1,
         "generation-1 HEAD round trip changed");
  expect_ok(rc::parse_head(fixture.head3_body, fixture.stream, &parsed_head,
                           &error),
            error, "parse HEAD");
  expect(parsed_head == fixture.head3, "HEAD round trip changed");
}

void test_strict_rejections(const Fixture &fixture) {
  std::string error;
  rc::WriterEpoch epoch;
  const std::string valid_epoch =
      "{\"epoch\":1,\"format\":\"wesql.remote_commit.writer_epoch\","
      "\"previous_epoch\":0,\"stream_id\":\"r=repo-1/b=branch_2\","
      "\"version\":2,\"writer_id\":"
      "\"11111111111111111111111111111111\"}";
  std::string duplicate = valid_epoch;
  duplicate.insert(1, "\"epoch\":1,");
  expect_rejected(rc::parse_writer_epoch(duplicate, fixture.stream, &epoch,
                                         &error),
                  error, "duplicate member");
  error.clear();
  std::string unknown = valid_epoch;
  unknown.insert(1, "\"unknown\":null,");
  expect_rejected(rc::parse_writer_epoch(unknown, fixture.stream, &epoch,
                                         &error),
                  error, "unknown member");
  error.clear();
  std::string wrong_type = valid_epoch;
  wrong_type.replace(wrong_type.find("\"epoch\":1"), 9,
                     "\"epoch\":\"1\"");
  expect_rejected(rc::parse_writer_epoch(wrong_type, fixture.stream, &epoch,
                                         &error),
                  error, "wrong member type");
  error.clear();
  std::string noncanonical = valid_epoch;
  noncanonical.insert(1, " ");
  expect_rejected(rc::parse_writer_epoch(noncanonical, fixture.stream, &epoch,
                                         &error),
                  error, "noncanonical whitespace");
  error.clear();
  std::string unsafe = valid_epoch;
  unsafe.replace(unsafe.find("\"epoch\":1"), 9,
                 "\"epoch\":9007199254740992");
  expect_rejected(rc::parse_writer_epoch(unsafe, fixture.stream, &epoch, &error),
                  error, "unsafe JSON integer");
  error.clear();
  std::string deeply_nested(17, '[');
  deeply_nested.append(17, ']');
  expect_rejected(rc::parse_writer_epoch(deeply_nested, fixture.stream, &epoch,
                                         &error),
                  error, "JSON depth limit");

  rc::Head bad_head = fixture.head3;
  bad_head.manifest.key.push_back('x');
  std::string body;
  error.clear();
  expect_rejected(rc::serialize_head(fixture.stream, bad_head, &body, &error),
                  error, "derived HEAD manifest key");

  rc::SnapshotManifest bad_snapshot = fixture.snapshot2;
  bad_snapshot.objects.front().ordinal = 1;
  error.clear();
  expect_rejected(rc::serialize_snapshot_manifest(
                      fixture.stream, bad_snapshot, &body, &error),
                  error, "snapshot ordinal gap");
  bad_snapshot = fixture.snapshot2;
  bad_snapshot.smartengine_extents.front().key.push_back('x');
  error.clear();
  expect_rejected(rc::serialize_snapshot_manifest(
                      fixture.stream, bad_snapshot, &body, &error),
                  error, "derived SmartEngine extent key");

  rc::TransitionManifest bad_log = fixture.log;
  bad_log.segments.front().previous_segment =
      rc::SegmentTip{rc::SegmentTipKind::SNAPSHOT_ROOT, std::nullopt,
                     std::nullopt, std::nullopt, std::nullopt,
                     std::string("cccccccccccccccccccccccccccccccc"),
                     rc::Cursor{"binlog.000001", 5}};
  error.clear();
  expect_rejected(rc::serialize_transition_manifest(fixture.stream, bad_log,
                                                    &body, &error),
                  error, "segment sequence/root mismatch");

  rc::SnapshotManifest parsed;
  error.clear();
  expect_rejected(rc::parse_snapshot_manifest(
                      fixture.snapshot2_body, fixture.stream,
                      fixture.snapshot1_ref.manifest_key, &parsed, &error),
                  error, "snapshot self-key mismatch");

  std::string canonical;
  error.clear();
  expect_rejected(rc::canonicalize_gtid_set(
                      "00112233-4455-6677-8899-aabbccddeeff:0", &canonical,
                      &error),
                  error, "zero GTID GNO");
  error.clear();
  expect_rejected(rc::canonicalize_gtid_set(
                      "00112233-4455-6677-8899-aabbccddeeff:1-"
                      "9223372036854775808",
                      &canonical, &error),
                  error, "overflow GTID GNO");
}

void print_vectors(const Fixture &fixture) {
  std::string error;
  rc::GtidSetDigest empty;
  rc::GtidSetDigest normalized;
  rc::XidDigest xids;
  std::string segments;
  expect_ok(rc::gtid_digest("", &empty, &error), error, "print empty GTID");
  expect_ok(rc::gtid_digest(
                "FFEEDDCC-BBAA-9988-7766-554433221100:3:1-2,"
                "00112233-4455-6677-8899-AABBCCDDEEFF:7-8:6",
                &normalized, &error),
            error, "print GTID");
  expect_ok(rc::xid_digest({10, 2, 2}, &xids, &error), error, "print XID");
  expect_ok(rc::segment_refs_digest(fixture.log.segments, &segments, &error),
            error, "print segment refs");
  std::cout << "stream=" << fixture.stream.stream_sha256 << '\n'
            << "empty_gtid=" << empty.sha256 << '\n'
            << "gtid=" << normalized.sha256 << '\n'
            << "xid=" << xids.sha256 << '\n'
            << "segment_refs=" << segments << '\n'
            << "snapshot1=" << hash(fixture.snapshot1_body) << '\n'
            << "bootstrap=" << hash(fixture.bootstrap_body) << '\n'
            << "log=" << hash(fixture.log_body) << '\n'
            << "snapshot_transition=" << hash(fixture.snapshot_transition_body)
            << '\n'
            << "head3=" << hash(fixture.head3_body) << '\n';
}

}  // namespace

int main(int argc, char **argv) {
  try {
    const Fixture fixture = make_fixture();
    if (argc == 2 && std::string_view(argv[1]) == "--print-vectors") {
      print_vectors(fixture);
      return 0;
    }
    test_digest_vectors(fixture);
    test_schema_golden_and_round_trip(fixture);
    test_strict_rejections(fixture);
    std::cout << "remote commit protocol codec tests passed\n";
    return 0;
  } catch (const std::exception &exception) {
    std::cerr << "remote commit protocol codec test failed: "
              << exception.what() << '\n';
    return 1;
  }
}
