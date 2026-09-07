/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/snapshot_publisher.h"
#include "sql/remote_commit/recovery.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <unistd.h>

namespace rc = wesql::remote_commit;
namespace fs = std::filesystem;

namespace {

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "snapshot publisher test failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

objstore::Status status(objstore::Errors code, const char *message) {
  return objstore::Status(code, 0, message);
}

std::string hash(std::string_view bytes) {
  std::string digest;
  std::string error;
  expect(rc::sha256_hex(bytes, &digest, &error), "cannot hash fixture");
  return digest;
}

std::string read_file(const fs::path &path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>{input},
          std::istreambuf_iterator<char>{}};
}

void write_file(const fs::path &path, std::string_view bytes) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  expect(static_cast<bool>(output), "cannot write fixture file");
}

std::string make_seed() {
  std::string seed{"\xfe\x62\x69\x6e", 4};
  std::string event(23, '\0');
  event[4] = 15;
  event[9] = 23;
  event[13] = 27;
  seed += event;
  return seed;
}

class TempDirectory {
 public:
  TempDirectory() {
    std::string pattern =
        (fs::temp_directory_path() / "wesql-snapshot-publisher-XXXXXX")
            .string();
    char *created = ::mkdtemp(pattern.data());
    expect(created != nullptr, "cannot create fixture directory");
    path_ = created;
  }

  ~TempDirectory() {
    std::error_code ignored;
    fs::remove_all(path_, ignored);
  }

  const fs::path &path() const { return path_; }

 private:
  fs::path path_;
};

class MemoryConditionalIo final : public rc::ConditionalIo {
 public:
  struct Stored {
    std::string body;
    std::string etag;
  };

  enum class SnapshotManifestMode { NORMAL, CONFLICT_SAME, CONFLICT_DIVERGENT };

  objstore::ExactObjectResult get(std::string_view key,
                                  uint64_t max_bytes) override {
    operations.push_back("GET " + std::string(key));
    if (block_snapshot_manifest_readback_key == key) {
      block_snapshot_manifest_readback_key.clear();
      return objstore::ExactObjectResult::transient_unavailable(
          status(objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED,
                 "injected snapshot manifest readback outage"));
    }
    const auto found = objects.find(std::string(key));
    if (found == objects.end()) {
      return objstore::ExactObjectResult::not_found(
          status(objstore::SE_NO_SUCH_KEY, "absent"));
    }
    if (found->second.body.size() > max_bytes) {
      return objstore::ExactObjectResult::permanent_error(
          status(objstore::SE_UNEXPECTED, "bounded exact GET exceeded"));
    }
    return objstore::ExactObjectResult::found(found->second.body,
                                               found->second.etag);
  }

  objstore::ConditionalPutResult put(
      std::string_view key, std::string_view body,
      const objstore::ConditionalPutCondition &condition) override {
    const std::string owned_key(key);
    operations.push_back("PUT " + owned_key);
    if (owned_key.ends_with("/HEAD")) ++head_put_count;

    const bool snapshot_manifest =
        owned_key.find("/snapshots/manifests/") != std::string::npos;
    if (snapshot_manifest) ++snapshot_manifest_put_count;
    if (snapshot_manifest &&
        snapshot_manifest_mode != SnapshotManifestMode::NORMAL) {
      if (!objects.contains(owned_key)) {
        const std::string stored =
            snapshot_manifest_mode == SnapshotManifestMode::CONFLICT_SAME
                ? std::string(body)
                : std::string("divergent-manifest");
        objects.emplace(owned_key,
                        Stored{stored, "\"etag-" +
                                           std::to_string(next_etag++) + "\""});
      }
      return objstore::ConditionalPutResult::conflict_409(
          status(objstore::SE_OBJECT_CONFLICT, "preexisting"));
    }

    auto found = objects.find(owned_key);
    if (condition.mode() == objstore::ConditionalPutMode::CREATE_ONLY) {
      if (found != objects.end()) {
        return objstore::ConditionalPutResult::precondition_failed_412(
            status(objstore::SE_OBJECT_PRECONDITION_FAILED, "exists"));
      }
    } else if (found == objects.end() ||
               found->second.etag != condition.etag()) {
      return objstore::ConditionalPutResult::precondition_failed_412(
          status(objstore::SE_OBJECT_PRECONDITION_FAILED, "etag mismatch"));
    }

    const std::string etag =
        "\"etag-" + std::to_string(next_etag++) + "\"";
    objects[owned_key] = {std::string(body), etag};
    if (snapshot_manifest && block_snapshot_manifest_readback_once) {
      block_snapshot_manifest_readback_once = false;
      block_snapshot_manifest_readback_key = owned_key;
    }

    if (owned_key.find("/manifests/e") != std::string::npos) {
      if (inject_head_conflict) {
        objects[head_key] = {"external-head", "\"external\""};
      }
      if (inject_epoch_loss) {
        objects[epoch_key] = {lost_epoch_body, "\"lost-epoch\""};
      }
    }
    return objstore::ConditionalPutResult::applied(etag);
  }

  std::map<std::string, Stored> objects;
  std::vector<std::string> operations;
  SnapshotManifestMode snapshot_manifest_mode{SnapshotManifestMode::NORMAL};
  bool inject_head_conflict{false};
  bool inject_epoch_loss{false};
  bool block_snapshot_manifest_readback_once{false};
  std::string block_snapshot_manifest_readback_key;
  std::string head_key;
  std::string epoch_key;
  std::string lost_epoch_body;
  size_t head_put_count{0};
  size_t snapshot_manifest_put_count{0};
  uint64_t next_etag{1};
};

class FakePayloadIo final : public rc::SnapshotPayloadIo {
 public:
  using rc::SnapshotPayloadIo::readback;

  enum class CreateMode {
    NORMAL,
    CONFLICT_SAME,
    DIVERGENT,
    UNKNOWN_APPLIED,
    UNKNOWN_ABSENT_THEN_APPLY,
  };

  rc::SnapshotPayloadCreateResult create_only_from_file(
      std::string_view key, const fs::path &source) override {
    ++create_calls;
    const std::string bytes = read_file(source);
    const rc::PayloadFingerprint intended{bytes.size(), hash(bytes)};
    const std::string owned_key(key);
    if (create_mode == CreateMode::DIVERGENT && create_calls == 1) {
      remote[owned_key] = {9, hash("different")};
      return {rc::SnapshotPayloadCreateOutcome::ALREADY_EXISTS,
              "preexisting"};
    }
    if (create_mode == CreateMode::CONFLICT_SAME) {
      remote[owned_key] = intended;
      return {rc::SnapshotPayloadCreateOutcome::ALREADY_EXISTS,
              "preexisting"};
    }
    if (create_mode == CreateMode::UNKNOWN_APPLIED && create_calls == 1) {
      remote[owned_key] = intended;
      return {rc::SnapshotPayloadCreateOutcome::TRANSPORT_UNKNOWN, "timeout"};
    }
    if (create_mode == CreateMode::UNKNOWN_ABSENT_THEN_APPLY &&
        create_calls == 1) {
      return {rc::SnapshotPayloadCreateOutcome::TRANSPORT_UNKNOWN, "timeout"};
    }
    if (remote.contains(owned_key)) {
      return {rc::SnapshotPayloadCreateOutcome::ALREADY_EXISTS,
              "preexisting"};
    }
    remote[owned_key] = intended;
    return {rc::SnapshotPayloadCreateOutcome::APPLIED, {}};
  }

  rc::SnapshotPayloadReadResult readback(std::string_view key) override {
    ++read_calls;
    if (block_read_call != 0 && read_calls == block_read_call) {
      return {rc::SnapshotPayloadReadOutcome::BLOCKED, "read unavailable",
              std::nullopt};
    }
    const auto found = remote.find(std::string(key));
    if (found == remote.end()) {
      return {rc::SnapshotPayloadReadOutcome::ABSENT, "absent", std::nullopt};
    }
    return {rc::SnapshotPayloadReadOutcome::APPLIED, {}, found->second};
  }

  std::map<std::string, rc::PayloadFingerprint> remote;
  CreateMode create_mode{CreateMode::NORMAL};
  size_t block_read_call{0};
  size_t create_calls{0};
  size_t read_calls{0};
};

class FakeExactFileReader final : public rc::SnapshotExactFileReader {
 public:
  rc::SnapshotPayloadDownloadResult download_exact(
      std::string_view bucket, std::string_view key,
      const fs::path &destination) override {
    observed_bucket.assign(bucket);
    observed_key.assign(key);
    ++calls;
    if (outcome != rc::SnapshotPayloadReadOutcome::APPLIED) {
      return {outcome, "injected exact-read failure"};
    }
    write_file(destination, body);
    return {rc::SnapshotPayloadReadOutcome::APPLIED, {}};
  }

  rc::SnapshotPayloadDownloadResult download_exact(
      std::string_view bucket, std::string_view key,
      const fs::path &destination, uint64_t max_bytes) override {
    observed_max_bytes = max_bytes;
    return download_exact(bucket, key, destination);
  }

  rc::SnapshotPayloadReadOutcome outcome{
      rc::SnapshotPayloadReadOutcome::APPLIED};
  std::string body;
  std::string observed_bucket;
  std::string observed_key;
  uint64_t observed_max_bytes{0};
  size_t calls{0};
};

struct Fixture {
  TempDirectory temporary;
  rc::StreamIdentity stream;
  std::string seed;
  rc::FixedSnapshotCut cut;

  Fixture() {
    std::string error;
    expect(rc::build_stream_identity("repo-1", "branch-1", "cluster/main",
                                     &stream, &error),
           "cannot build stream identity");
    seed = make_seed();
    const fs::path seed_path = temporary.path() / "binlog.seed";
    write_file(seed_path, seed);

    const std::vector<std::pair<std::string, std::string>> files{
        {"wal.log", "wal"},       {"ib-z.dat", "innodb-z"},
        {"dd.ibd", "mysql-dd"},  {"ib-a.dat", "innodb-a"},
        {"meta.sst", "se-meta"},
    };
    for (const auto &[name, bytes] : files) {
      write_file(temporary.path() / name, bytes);
    }

    cut.snapshot_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    cut.writer = {"11111111111111111111111111111111", 1};
    cut.proof.source = rc::FixedCutSource::EMPTY_SOURCE;
    cut.proof.public_cursor = {"binlog.000001", seed.size()};
    cut.proof.image_cursor = cut.proof.public_cursor;
    expect(rc::gtid_digest("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1",
                           &cut.proof.public_gtid, &error),
           "cannot build GTID proof");
    cut.proof.image_gtid = cut.proof.public_gtid;
    cut.proof.empty_source_scan_stable = true;
    cut.proof.internal_prepared_empty = true;
    cut.proof.external_xa_empty = true;
    cut.proof.old_tc_authority_empty = true;
    cut.proof.user_state_empty = true;
    cut.proof.legacy_live_extents_empty = true;
    cut.log_anchor.kind = rc::LogAnchorKind::EMPTY_BASE;
    cut.log_anchor.cursor = cut.proof.public_cursor;
    cut.server_identity.server_uuid =
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    cut.deployment_fingerprints = {
        hash("startup"), "wesql-test-build", hash("plugins"),
        hash("keyring"), hash("tls")};
    cut.binlog_seed_path = seed_path;
    cut.objects = {
        {"smartengine-wal", "se/wal.log", temporary.path() / "wal.log",
         "smartengine-wal-v1"},
        {"innodb", "z/ib.dat", temporary.path() / "ib-z.dat",
         "innodb-clone-v1"},
        {"mysql-dd", "mysql/dd.ibd", temporary.path() / "dd.ibd",
         "mysql-dd-v1"},
        {"innodb", "a/ib.dat", temporary.path() / "ib-a.dat",
         "innodb-clone-v1"},
        {"smartengine-meta", "se/meta.sst",
         temporary.path() / "meta.sst", "smartengine-meta-v1"},
    };

    rc::SmartengineExtentRef extent10;
    extent10.writer_epoch = 1;
    extent10.allocation_seq = "10";
    extent10.database_name_hex = "74657374";
    extent10.index_id = "2";
    extent10.object_id = "10";
    extent10.size = 10;
    extent10.sha256 = hash("extent-ten");
    expect(rc::smartengine_extent_object_key(stream, extent10, &extent10.key,
                                             &error),
           "cannot derive extent 10 key");
    rc::SmartengineExtentRef extent2 = extent10;
    extent2.allocation_seq = "2";
    extent2.object_id = "2";
    extent2.size = 9;
    extent2.sha256 = hash("extent-two");
    expect(rc::smartengine_extent_object_key(stream, extent2, &extent2.key,
                                             &error),
           "cannot derive extent 2 key");
    cut.smartengine_extents = {
        {extent10.writer_epoch, extent10.allocation_seq,
         extent10.database_name_hex, extent10.index_id, extent10.object_id,
         extent10.key, extent10.size, extent10.sha256},
        {extent2.writer_epoch, extent2.allocation_seq,
         extent2.database_name_hex, extent2.index_id, extent2.object_id,
         extent2.key, extent2.size, extent2.sha256},
    };
  }

  void seed_extents(FakePayloadIo *io) const {
    for (const rc::PinnedSmartengineExtent &extent :
         cut.smartengine_extents) {
      io->remote[extent.key] = {extent.size, extent.sha256};
    }
  }
};

struct ReadyPublisher {
  MemoryConditionalIo metadata;
  rc::HeadPublisher publisher;

  explicit ReadyPublisher(const rc::StreamIdentity &stream)
      : publisher(&metadata, stream) {
    metadata.head_key = stream.remote_prefix + "/HEAD";
    metadata.epoch_key = stream.remote_prefix + "/WRITER_EPOCH";
    expect(publisher.probe().applied(), "cannot probe empty stream");
    expect(publisher.acquire_epoch("11111111111111111111111111111111")
               .applied(),
           "cannot acquire writer epoch");
  }
};

rc::SnapshotPrepareAuthority capture_authority(
    const ReadyPublisher &ready, const rc::StreamIdentity &stream) {
  const rc::PublisherState state = ready.publisher.state();
  return {stream, state.epoch_object, state.epoch, state.head_object,
          state.head};
}

rc::SnapshotPublication sentinel_publication() {
  rc::SnapshotPublication value;
  value.snapshot_manifest.snapshot_id = "sentinel";
  value.snapshot_ref.id = "sentinel";
  value.transition.generation = 777;
  value.head.generation = 777;
  return value;
}

rc::Head append_log_transition(ReadyPublisher *ready,
                               const rc::StreamIdentity &stream) {
  const rc::Head prior = *ready->publisher.state().head;
  const rc::PublishedBytes prior_object =
      *ready->publisher.state().head_object;
  std::string error;
  const rc::HeadParent parent{prior.generation, prior_object.etag,
                              hash(prior_object.body)};
  rc::SegmentRef segment;
  segment.sequence =
      prior.segment_tip.sequence.has_value() ? *prior.segment_tip.sequence + 1
                                             : 1;
  segment.source = {prior.durable_cursor.file, prior.durable_cursor.pos,
                    prior.durable_cursor.pos + 23};
  segment.previous_segment = prior.segment_tip;
  segment.size = segment.source.end_pos - segment.source.start_pos;
  segment.sha256 = hash(std::string(segment.size, 's'));
  segment.transaction_count = 1;
  const std::string gtid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:" +
                           std::to_string(segment.sequence + 1);
  expect(rc::gtid_digest(gtid,
                         &segment.gtid_set, &error) &&
             rc::xid_digest({41 + segment.sequence}, &segment.xids, &error) &&
             rc::segment_object_key(stream, prior.writer, segment.source,
                                    segment.sha256, &segment.key, &error),
         "cannot build background LOG segment");
  rc::SegmentTip tip;
  tip.kind = rc::SegmentTipKind::SEGMENT;
  tip.key = segment.key;
  tip.size = segment.size;
  tip.sha256 = segment.sha256;
  tip.sequence = segment.sequence;

  rc::TransitionManifest transition;
  transition.kind = rc::ManifestKind::LOG_TRANSITION;
  transition.generation = prior.generation + 1;
  transition.writer = prior.writer;
  transition.head_parent = parent;
  transition.previous = rc::ManifestRef{
      prior.generation, prior.manifest.key, prior.manifest.size,
      prior.manifest.sha256};
  transition.recovery_window = prior.recovery_window;
  ++transition.recovery_window.manifest_count;
  ++transition.recovery_window.segment_count;
  transition.segment_tip = tip;
  transition.snapshot = prior.snapshot;
  transition.base_cursor = prior.base_cursor;
  transition.durable_cursor = {segment.source.file, segment.source.end_pos};
  transition.segments = {segment};
  std::string transition_body;
  expect(rc::stabilize_transition_manifest(
             stream, prior.recovery_window.manifest_bytes, &transition,
             &transition_body, &error),
         "cannot stabilize background LOG manifest");
  const std::string transition_sha = hash(transition_body);
  std::string transition_key;
  expect(rc::transition_manifest_key(stream, prior.writer,
                                     transition.generation, transition_sha,
                                     &transition_key, &error),
         "cannot derive background LOG manifest key");
  rc::Head head;
  head.generation = transition.generation;
  head.writer = transition.writer;
  head.parent = parent;
  head.manifest = {transition_key, transition_body.size(), transition_sha};
  head.recovery_window = transition.recovery_window;
  head.segment_tip = transition.segment_tip;
  head.snapshot = transition.snapshot;
  head.base_cursor = transition.base_cursor;
  head.durable_cursor = transition.durable_cursor;
  expect(ready->publisher.publish(transition, head).applied(),
         "cannot append background LOG transition");
  return head;
}

void test_bootstrap_and_canonical_builder() {
  Fixture fixture;
  ReadyPublisher ready(fixture.stream);
  FakePayloadIo payload;
  fixture.seed_extents(&payload);
  rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher);
  rc::SnapshotPublication publication;
  expect(publisher.publish(fixture.cut, &publication).applied(),
         "bootstrap publication failed");
  expect(publication.transition.kind == rc::ManifestKind::BOOTSTRAP &&
             publication.head.generation == 1 &&
             publication.head.base_cursor == fixture.cut.proof.public_cursor &&
             publication.head.durable_cursor ==
                 fixture.cut.proof.public_cursor,
         "bootstrap transition/head relation changed");
  const auto &objects = publication.snapshot_manifest.objects;
  expect(objects.size() == 5 && objects[0].component == "innodb" &&
             objects[0].relative_path == "a/ib.dat" &&
             objects[0].ordinal == 0 &&
             objects[1].relative_path == "z/ib.dat" &&
             objects[1].ordinal == 1 && objects[2].component == "mysql-dd" &&
             objects[2].ordinal == 0,
         "materialized payload order/ordinal is not canonical");
  expect(objects[0].sha256 == hash("innodb-a") &&
             objects[0].size == std::string_view("innodb-a").size(),
         "materialized payload hash/size changed");
  std::string expected_key;
  std::string error;
  expect(rc::snapshot_object_key(fixture.stream, fixture.cut.snapshot_id,
                                 objects[0], &expected_key, &error) &&
             expected_key == objects[0].key,
         "materialized payload key is not derived canonically");
  const auto &extents = publication.snapshot_manifest.smartengine_extents;
  expect(extents.size() == 2 && extents[0].allocation_seq == "2" &&
             extents[0].ordinal == 0 && extents[1].allocation_seq == "10" &&
             extents[1].ordinal == 1,
         "extent numeric order/ordinal is not canonical");
}

void test_takeover_preserves_physical_tip() {
  Fixture fixture;
  ReadyPublisher bootstrap_ready(fixture.stream);
  FakePayloadIo bootstrap_payload;
  fixture.seed_extents(&bootstrap_payload);
  rc::SnapshotPublisher bootstrap(&bootstrap_payload,
                                  &bootstrap_ready.metadata,
                                  &bootstrap_ready.publisher);
  rc::SnapshotPublication first;
  expect(bootstrap.publish(fixture.cut, &first).applied(),
         "takeover fixture bootstrap failed");

  rc::HeadPublisher takeover_head(&bootstrap_ready.metadata, fixture.stream);
  expect(takeover_head.probe().applied(), "cannot probe bootstrap HEAD");
  expect(takeover_head.acquire_epoch("22222222222222222222222222222222")
             .applied(),
         "cannot acquire takeover epoch");

  rc::FixedSnapshotCut takeover = fixture.cut;
  takeover.snapshot_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  takeover.writer = {"22222222222222222222222222222222", 2};
  takeover.proof.source = rc::FixedCutSource::RECOVERED_TAKEOVER;
  takeover.proof.empty_source_scan_stable = false;
  takeover.proof.internal_prepared_empty = false;
  takeover.proof.external_xa_empty = false;
  takeover.proof.old_tc_authority_empty = false;
  takeover.proof.user_state_empty = false;
  takeover.proof.legacy_live_extents_empty = false;
  takeover.proof.source_head_generation = first.head.generation;
  std::string error;
  expect(rc::sha256_hex(takeover_head.state().head_object->body,
                        &takeover.proof.source_head_body_sha256, &error),
         "cannot hash takeover source HEAD");
  takeover.log_anchor.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  takeover.log_anchor.generation = first.head.generation;
  takeover.log_anchor.manifest = first.head.manifest;
  takeover.log_anchor.cursor = takeover.proof.public_cursor;

  FakePayloadIo payload;
  fixture.seed_extents(&payload);
  rc::SnapshotPublisher publisher(&payload, &bootstrap_ready.metadata,
                                  &takeover_head);
  rc::SnapshotPublication publication;
  expect(publisher.publish(takeover, &publication).applied(),
         "takeover snapshot publication failed");
  expect(publication.transition.kind == rc::ManifestKind::SNAPSHOT &&
             publication.head.generation == 2 &&
             publication.head.writer == takeover.writer &&
             publication.head.segment_tip == first.head.segment_tip &&
             publication.head.durable_cursor == first.head.durable_cursor &&
             publication.head.snapshot.id == takeover.snapshot_id,
         "takeover rewrote the physical tip/durable cursor");
}

void test_background_snapshot_proves_older_anchor_ancestry() {
  Fixture fixture;
  ReadyPublisher ready(fixture.stream);
  FakePayloadIo bootstrap_payload;
  fixture.seed_extents(&bootstrap_payload);
  rc::SnapshotPublisher bootstrap(&bootstrap_payload, &ready.metadata,
                                  &ready.publisher);
  rc::SnapshotPublication first;
  expect(bootstrap.publish(fixture.cut, &first).applied(),
         "background fixture bootstrap failed");
  const rc::Head after_log = append_log_transition(&ready, fixture.stream);

  rc::FixedSnapshotCut background = fixture.cut;
  background.snapshot_id = "cccccccccccccccccccccccccccccccc";
  background.proof.source = rc::FixedCutSource::CLONE_BARRIER;
  background.proof.empty_source_scan_stable = false;
  background.proof.internal_prepared_empty = false;
  background.proof.external_xa_empty = false;
  background.proof.old_tc_authority_empty = false;
  background.proof.user_state_empty = false;
  background.proof.legacy_live_extents_empty = false;
  background.proof.source_head_generation = first.head.generation;
  std::string first_head_body;
  std::string error;
  expect(rc::serialize_head(fixture.stream, first.head, &first_head_body,
                            &error),
         "cannot serialize fixed source HEAD");
  background.proof.source_head_body_sha256 = hash(first_head_body);
  background.proof.clone_handle_id = 17;
  background.proof.redo_range_sha256 = hash("redo-range");
  background.log_anchor.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  background.log_anchor.generation = first.head.generation;
  background.log_anchor.manifest = first.head.manifest;
  background.log_anchor.cursor = background.proof.public_cursor;

  FakePayloadIo payload;
  fixture.seed_extents(&payload);
  rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher);
  rc::SnapshotPublication publication;
  expect(publisher.publish(background, &publication).applied(),
         "background snapshot did not prove older anchor ancestry");
  expect(publication.head.generation == after_log.generation + 1 &&
             publication.head.segment_tip == after_log.segment_tip &&
             publication.head.durable_cursor == after_log.durable_cursor &&
             publication.head.base_cursor ==
                 background.proof.public_cursor,
         "background snapshot lost the post-cut LOG tail");
}

void test_prepared_snapshot_recomputes_exact_retained_suffix() {
  Fixture fixture;
  ReadyPublisher ready(fixture.stream);
  FakePayloadIo bootstrap_payload;
  fixture.seed_extents(&bootstrap_payload);
  rc::SnapshotPublisher bootstrap(&bootstrap_payload, &ready.metadata,
                                  &ready.publisher);
  rc::SnapshotPublication first;
  expect(bootstrap.publish(fixture.cut, &first).applied(),
         "retained-suffix fixture bootstrap failed");

  const rc::Head anchor = append_log_transition(&ready, fixture.stream);
  rc::FixedSnapshotCut background = fixture.cut;
  background.snapshot_id = "dddddddddddddddddddddddddddddddd";
  background.proof.source = rc::FixedCutSource::CLONE_BARRIER;
  background.proof.empty_source_scan_stable = false;
  background.proof.internal_prepared_empty = false;
  background.proof.external_xa_empty = false;
  background.proof.old_tc_authority_empty = false;
  background.proof.user_state_empty = false;
  background.proof.legacy_live_extents_empty = false;
  background.proof.public_cursor = anchor.durable_cursor;
  background.proof.image_cursor = anchor.durable_cursor;
  background.proof.source_head_generation = anchor.generation;
  background.proof.source_head_body_sha256 =
      hash(ready.publisher.state().head_object->body);
  background.proof.clone_handle_id = 23;
  background.proof.redo_range_sha256 = hash("fixed-redo-range");
  background.log_anchor.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  background.log_anchor.generation = anchor.generation;
  background.log_anchor.manifest = anchor.manifest;
  background.log_anchor.cursor = anchor.durable_cursor;
  fixture.seed += fixture.seed.substr(4);
  write_file(background.binlog_seed_path, fixture.seed);
  expect(fixture.seed.size() == background.proof.public_cursor.pos,
         "fixed-cut seed does not end at the anchor cursor");

  FakePayloadIo payload;
  fixture.seed_extents(&payload);
  rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher);
  rc::PreparedSnapshotPublication prepared;
  const rc::SnapshotPrepareAuthority authority =
      capture_authority(ready, fixture.stream);
  const rc::Head after_first_tail =
      append_log_transition(&ready, fixture.stream);
  const size_t head_puts_before_prepare = ready.metadata.head_put_count;
  expect(publisher.prepare(background, authority, &prepared).applied(),
         "runtime snapshot preparation ignored its immutable authority");
  const size_t head_puts_after_prepare = ready.metadata.head_put_count;

  const rc::Head after_second_tail =
      append_log_transition(&ready, fixture.stream);
  rc::SnapshotPublication publication;
  expect(publisher.publish_prepared(prepared, &publication).applied(),
         "prepared runtime snapshot publication failed");

  const uint64_t retained_bytes =
      anchor.manifest.size + after_first_tail.manifest.size +
      after_second_tail.manifest.size + publication.head.manifest.size;
  expect(head_puts_after_prepare == head_puts_before_prepare &&
             publication.head.recovery_window.manifest_count == 4 &&
             publication.head.recovery_window.segment_count == 2 &&
             publication.head.recovery_window.manifest_bytes == retained_bytes,
         "SNAPSHOT carried prior totals instead of exact retained suffix");

  rc::RecoveryChainReader reader(&ready.metadata, fixture.stream);
  rc::RecoveryPlan plan;
  const rc::RecoveryReadResult read = reader.read(&plan);
  expect(read.ready() && plan.head == publication.head &&
             plan.replay_segments.size() == 2 && plan.manifests.size() == 4,
         "reader did not reproduce the published retained-suffix counters");
}

void test_same_writer_snapshot_supersession_requires_refix() {
  Fixture fixture;
  ReadyPublisher ready(fixture.stream);
  FakePayloadIo bootstrap_payload;
  fixture.seed_extents(&bootstrap_payload);
  rc::SnapshotPublisher bootstrap(&bootstrap_payload, &ready.metadata,
                                  &ready.publisher);
  rc::SnapshotPublication first;
  expect(bootstrap.publish(fixture.cut, &first).applied(),
         "refix fixture bootstrap failed");

  const rc::Head old_anchor = append_log_transition(&ready, fixture.stream);
  fixture.seed += fixture.seed.substr(4);
  rc::FixedSnapshotCut old_cut = fixture.cut;
  old_cut.snapshot_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
  old_cut.proof.source = rc::FixedCutSource::CLONE_BARRIER;
  old_cut.proof.empty_source_scan_stable = false;
  old_cut.proof.internal_prepared_empty = false;
  old_cut.proof.external_xa_empty = false;
  old_cut.proof.old_tc_authority_empty = false;
  old_cut.proof.user_state_empty = false;
  old_cut.proof.legacy_live_extents_empty = false;
  old_cut.proof.public_cursor = old_anchor.durable_cursor;
  old_cut.proof.image_cursor = old_anchor.durable_cursor;
  old_cut.proof.source_head_generation = old_anchor.generation;
  old_cut.proof.source_head_body_sha256 =
      hash(ready.publisher.state().head_object->body);
  old_cut.proof.clone_handle_id = 31;
  old_cut.proof.redo_range_sha256 = hash("old-fixed-redo");
  old_cut.log_anchor.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  old_cut.log_anchor.generation = old_anchor.generation;
  old_cut.log_anchor.manifest = old_anchor.manifest;
  old_cut.log_anchor.cursor = old_anchor.durable_cursor;
  write_file(old_cut.binlog_seed_path, fixture.seed);

  FakePayloadIo payload;
  fixture.seed_extents(&payload);
  rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher);
  rc::PreparedSnapshotPublication old_prepared;
  const rc::SnapshotPrepareAuthority old_authority =
      capture_authority(ready, fixture.stream);
  expect(publisher.prepare(old_cut, old_authority, &old_prepared).applied(),
         "old background snapshot preparation failed");

  const rc::Head new_anchor = append_log_transition(&ready, fixture.stream);
  fixture.seed += fixture.seed.substr(4, 23);
  rc::FixedSnapshotCut newer = old_cut;
  newer.snapshot_id = "ffffffffffffffffffffffffffffffff";
  newer.proof.public_cursor = new_anchor.durable_cursor;
  newer.proof.image_cursor = new_anchor.durable_cursor;
  newer.proof.source_head_generation = new_anchor.generation;
  newer.proof.source_head_body_sha256 =
      hash(ready.publisher.state().head_object->body);
  newer.proof.clone_handle_id = 32;
  newer.proof.redo_range_sha256 = hash("new-fixed-redo");
  newer.log_anchor.generation = new_anchor.generation;
  newer.log_anchor.manifest = new_anchor.manifest;
  newer.log_anchor.cursor = new_anchor.durable_cursor;
  write_file(newer.binlog_seed_path, fixture.seed);

  rc::SnapshotPublication newer_publication;
  expect(publisher.publish(newer, &newer_publication).applied(),
         "newer same-writer snapshot did not publish");
  const rc::Head head_before = *ready.publisher.state().head;
  const size_t head_puts_before = ready.metadata.head_put_count;
  rc::SnapshotPublication stale_output = sentinel_publication();
  const rc::SnapshotPublication sentinel = stale_output;
  const rc::PublishResult stale =
      publisher.publish_prepared(old_prepared, &stale_output);
  expect(stale.outcome == rc::PublishOutcome::REFIX_REQUIRED &&
             stale_output == sentinel &&
             *ready.publisher.state().head == head_before &&
             ready.publisher.state().lifecycle == rc::LifecycleState::RUNNING &&
             ready.metadata.head_put_count == head_puts_before,
         "same-writer supersession fenced or mutated HEAD instead of refixing");
}

void test_runtime_authority_accepts_same_writer_descendant_head() {
  Fixture fixture;
  ReadyPublisher ready(fixture.stream);
  FakePayloadIo bootstrap_payload;
  fixture.seed_extents(&bootstrap_payload);
  rc::SnapshotPublisher bootstrap(&bootstrap_payload, &ready.metadata,
                                  &ready.publisher);
  rc::SnapshotPublication first;
  expect(bootstrap.publish(fixture.cut, &first).applied(),
         "authority fixture bootstrap failed");

  const rc::Head anchor = append_log_transition(&ready, fixture.stream);
  fixture.seed += fixture.seed.substr(4);
  rc::FixedSnapshotCut cut = fixture.cut;
  cut.snapshot_id = "99999999999999999999999999999999";
  cut.proof.source = rc::FixedCutSource::CLONE_BARRIER;
  cut.proof.empty_source_scan_stable = false;
  cut.proof.internal_prepared_empty = false;
  cut.proof.external_xa_empty = false;
  cut.proof.old_tc_authority_empty = false;
  cut.proof.user_state_empty = false;
  cut.proof.legacy_live_extents_empty = false;
  cut.proof.public_cursor = anchor.durable_cursor;
  cut.proof.image_cursor = anchor.durable_cursor;
  cut.proof.source_head_generation = anchor.generation;
  cut.proof.source_head_body_sha256 =
      hash(ready.publisher.state().head_object->body);
  cut.proof.clone_handle_id = 41;
  cut.proof.redo_range_sha256 = hash("authority-redo");
  cut.log_anchor.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  cut.log_anchor.generation = anchor.generation;
  cut.log_anchor.manifest = anchor.manifest;
  cut.log_anchor.cursor = anchor.durable_cursor;
  write_file(cut.binlog_seed_path, fixture.seed);

  append_log_transition(&ready, fixture.stream);
  const rc::SnapshotPrepareAuthority late_authority =
      capture_authority(ready, fixture.stream);
  const rc::Head head_before = *ready.publisher.state().head;
  FakePayloadIo payload;
  fixture.seed_extents(&payload);
  rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher);
  rc::PreparedSnapshotPublication prepared;
  const size_t payload_creates_before = payload.create_calls;
  const rc::PublishResult result =
      publisher.prepare(cut, late_authority, &prepared);
  expect(result.applied() && prepared.writer == cut.writer &&
             prepared.snapshot_manifest.cursor == cut.proof.public_cursor &&
             prepared.source_snapshot.has_value() &&
             *prepared.source_snapshot == first.head.snapshot &&
             payload.create_calls > payload_creates_before &&
             *ready.publisher.state().head == head_before &&
             ready.publisher.state().lifecycle == rc::LifecycleState::RUNNING,
         "same-writer descendant authority did not prepare the fixed cut");

  // A refreshed authority may advance the chain, but it cannot transfer the
  // fixed cut to a different writer epoch.
  rc::SnapshotPrepareAuthority wrong_epoch = late_authority;
  wrong_epoch.epoch = rc::WriterEpoch{
      2, "22222222222222222222222222222222", 1};
  std::string wrong_epoch_body;
  std::string error;
  expect(rc::serialize_writer_epoch(fixture.stream, *wrong_epoch.epoch,
                                    &wrong_epoch_body, &error),
         "cannot serialize wrong authority epoch");
  wrong_epoch.epoch_object =
      rc::PublishedBytes{wrong_epoch_body, late_authority.epoch_object->etag};
  rc::PreparedSnapshotPublication rejected;
  rejected.writer.id = "sentinel";
  const size_t payload_creates_after = payload.create_calls;
  const rc::PublishResult wrong =
      publisher.prepare(cut, wrong_epoch, &rejected);
  expect(wrong.outcome == rc::PublishOutcome::FENCED &&
             rejected.writer.id == "sentinel" &&
             payload.create_calls == payload_creates_after &&
             *ready.publisher.state().head == head_before &&
             ready.publisher.state().lifecycle == rc::LifecycleState::RUNNING,
         "different writer epoch was not fenced before payload IO");
}

void test_duplicate_same_bytes_is_idempotent() {
  Fixture fixture;
  ReadyPublisher ready(fixture.stream);
  ready.metadata.snapshot_manifest_mode =
      MemoryConditionalIo::SnapshotManifestMode::CONFLICT_SAME;
  FakePayloadIo payload;
  payload.create_mode = FakePayloadIo::CreateMode::CONFLICT_SAME;
  fixture.seed_extents(&payload);
  rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher);
  rc::SnapshotPublication publication;
  expect(publisher.publish(fixture.cut, &publication).applied(),
         "same-byte immutable duplicates were not accepted");
}

void test_divergent_payload_and_manifest_fence_without_output() {
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    FakePayloadIo payload;
    payload.create_mode = FakePayloadIo::CreateMode::DIVERGENT;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::SnapshotPublication publication = sentinel_publication();
    const rc::SnapshotPublication before = publication;
    const rc::PublishResult published = publisher.publish(fixture.cut,
                                                          &publication);
    expect(published.outcome == rc::PublishOutcome::FENCED &&
               publication == before && ready.metadata.head_put_count == 0,
           "divergent payload changed output or attempted HEAD");
  }
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    ready.metadata.snapshot_manifest_mode =
        MemoryConditionalIo::SnapshotManifestMode::CONFLICT_DIVERGENT;
    FakePayloadIo payload;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::SnapshotPublication publication = sentinel_publication();
    const rc::SnapshotPublication before = publication;
    const rc::PublishResult published = publisher.publish(fixture.cut,
                                                          &publication);
    expect(published.outcome == rc::PublishOutcome::FENCED &&
               publication == before && ready.metadata.head_put_count == 0,
           "divergent snapshot manifest changed output or attempted HEAD");
  }
}

void test_transport_unknown_and_unavailable_readback() {
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    FakePayloadIo payload;
    payload.create_mode = FakePayloadIo::CreateMode::UNKNOWN_APPLIED;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::SnapshotPublication publication;
    expect(publisher.publish(fixture.cut, &publication).applied() &&
               payload.create_calls == fixture.cut.objects.size() + 1,
           "transport-unknown applied payload was rewritten or rejected");
  }
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    FakePayloadIo payload;
    payload.create_mode =
        FakePayloadIo::CreateMode::UNKNOWN_ABSENT_THEN_APPLY;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::SnapshotPublication publication;
    expect(publisher.publish(fixture.cut, &publication).applied() &&
               payload.create_calls == fixture.cut.objects.size() + 2,
           "transport-unknown absent readback did not retry exactly once");
  }
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    FakePayloadIo payload;
    payload.create_mode = FakePayloadIo::CreateMode::UNKNOWN_APPLIED;
    payload.block_read_call = 2;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::SnapshotPublication publication = sentinel_publication();
    const rc::SnapshotPublication before = publication;
    const rc::PublishResult published = publisher.publish(fixture.cut,
                                                          &publication);
    expect(published.outcome == rc::PublishOutcome::BLOCKED &&
               payload.create_calls == 1 && publication == before &&
               ready.metadata.head_put_count == 0,
           "unavailable readback was retried or changed HEAD/output");
  }
}

void test_prepare_resumes_without_recreating_verified_objects() {
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    FakePayloadIo payload;
    payload.block_read_call = 2;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::PreparedSnapshotPublication prepared;
    prepared.writer.id = "sentinel";
    const rc::PublishResult first = publisher.prepare(fixture.cut, &prepared);
    expect(first.outcome == rc::PublishOutcome::BLOCKED &&
               payload.create_calls == 1 && prepared.writer.id == "sentinel",
           "payload readback outage did not leave preparation resumable");

    const rc::PublishResult retried = publisher.prepare(fixture.cut, &prepared);
    expect(retried.applied() &&
               payload.create_calls == fixture.cut.objects.size() + 1 &&
               prepared.writer == fixture.cut.writer,
           "payload preparation retry recreated an already-written object");
  }
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    ready.metadata.block_snapshot_manifest_readback_once = true;
    FakePayloadIo payload;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::PreparedSnapshotPublication prepared;
    prepared.writer.id = "sentinel";
    const rc::PublishResult first = publisher.prepare(fixture.cut, &prepared);
    const size_t payload_creates = payload.create_calls;
    expect(first.outcome == rc::PublishOutcome::BLOCKED &&
               ready.metadata.snapshot_manifest_put_count == 1 &&
               prepared.writer.id == "sentinel",
           "snapshot manifest readback outage was not resumable");

    const rc::PublishResult retried = publisher.prepare(fixture.cut, &prepared);
    expect(retried.applied() && payload.create_calls == payload_creates &&
               ready.metadata.snapshot_manifest_put_count == 1 &&
               prepared.writer == fixture.cut.writer,
           "snapshot manifest retry repeated an immutable PUT");
  }
}

void test_head_conflict_and_epoch_loss_leave_orphans_only() {
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    ready.metadata.inject_head_conflict = true;
    FakePayloadIo payload;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::SnapshotPublication publication = sentinel_publication();
    const rc::SnapshotPublication before = publication;
    const rc::PublishResult published = publisher.publish(fixture.cut,
                                                          &publication);
    expect(published.outcome == rc::PublishOutcome::FENCED &&
               ready.metadata.head_put_count == 0 && publication == before,
           "HEAD conflict attempted CAS or changed output");
  }
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    rc::WriterEpoch lost{2, "22222222222222222222222222222222", 1};
    std::string error;
    expect(rc::serialize_writer_epoch(fixture.stream, lost,
                                      &ready.metadata.lost_epoch_body, &error),
           "cannot serialize lost epoch fixture");
    ready.metadata.inject_epoch_loss = true;
    FakePayloadIo payload;
    fixture.seed_extents(&payload);
    rc::SnapshotPublisher publisher(&payload, &ready.metadata,
                                    &ready.publisher);
    rc::SnapshotPublication publication = sentinel_publication();
    const rc::SnapshotPublication before = publication;
    const rc::PublishResult published = publisher.publish(fixture.cut,
                                                          &publication);
    expect(published.outcome == rc::PublishOutcome::FENCED &&
               ready.metadata.head_put_count == 0 && publication == before,
           "epoch loss attempted HEAD CAS or changed output");
  }
}

void test_invalid_cut_fails_before_remote_io() {
  Fixture fixture;
  ReadyPublisher ready(fixture.stream);
  FakePayloadIo payload;
  fixture.seed_extents(&payload);
  fixture.cut.proof.image_cursor.pos += 1;
  rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher);
  rc::SnapshotPublication publication = sentinel_publication();
  const rc::SnapshotPublication before = publication;
  const rc::PublishResult published = publisher.publish(fixture.cut,
                                                        &publication);
  expect(published.outcome == rc::PublishOutcome::PERMANENT_ERROR &&
             payload.create_calls == 0 && publication == before &&
             ready.metadata.head_put_count == 0,
         "incoherent fixed cut reached remote IO or changed output");
}

void test_payload_limits_fail_before_remote_io() {
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    FakePayloadIo payload;
    fixture.seed_extents(&payload);
    write_file(fixture.cut.objects.front().local_path,
               std::string(fixture.seed.size() + 1, 'x'));
    rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher,
                                    fixture.seed.size(), 1024);
    rc::PreparedSnapshotPublication prepared;
    prepared.writer.id = "sentinel";
    const rc::PublishResult result =
        publisher.prepare(fixture.cut, &prepared);
    expect(result.outcome == rc::PublishOutcome::PERMANENT_ERROR &&
               prepared.writer.id == "sentinel" && payload.create_calls == 0 &&
               payload.read_calls == 0 && ready.metadata.head_put_count == 0,
           "oversized snapshot object reached remote IO or changed output");
  }
  {
    Fixture fixture;
    ReadyPublisher ready(fixture.stream);
    FakePayloadIo payload;
    fixture.seed_extents(&payload);
    uint64_t aggregate = fixture.seed.size();
    for (const rc::LocalSnapshotPayload &object : fixture.cut.objects) {
      aggregate += fs::file_size(object.local_path);
    }
    for (const rc::PinnedSmartengineExtent &extent :
         fixture.cut.smartengine_extents) {
      aggregate += extent.size;
    }
    rc::SnapshotPublisher publisher(&payload, &ready.metadata, &ready.publisher,
                                    fixture.seed.size(), aggregate - 1);
    rc::PreparedSnapshotPublication prepared;
    prepared.writer.id = "sentinel";
    const rc::PublishResult result =
        publisher.prepare(fixture.cut, &prepared);
    expect(result.outcome == rc::PublishOutcome::PERMANENT_ERROR &&
               prepared.writer.id == "sentinel" && payload.create_calls == 0 &&
               payload.read_calls == 0 && ready.metadata.head_put_count == 0,
           "snapshot aggregate overflow reached remote IO or changed output");
  }
}

void test_exact_file_reader_is_stream_hashed_and_cleaned() {
  TempDirectory temporary;
  FakeExactFileReader reader;
  reader.body.assign(2 * 1024 * 1024 + 17, 'x');
  rc::ObjectStoreSnapshotPayloadIo io(nullptr, "bucket-1", &reader,
                                      temporary.path());
  const rc::SnapshotPayloadReadResult readback =
      io.readback("payload/key", reader.body.size());
  expect(readback.outcome == rc::SnapshotPayloadReadOutcome::APPLIED &&
             readback.fingerprint.has_value() &&
             readback.fingerprint->size == reader.body.size() &&
             readback.fingerprint->sha256 == hash(reader.body) &&
             reader.observed_bucket == "bucket-1" &&
             reader.observed_key == "payload/key" &&
             reader.observed_max_bytes == reader.body.size() &&
             reader.calls == 1 &&
             fs::is_empty(temporary.path()),
         "exact file reader was not stream-hashed or left scratch residue");
}

void test_object_store_exact_file_outcome_mapping() {
  TempDirectory temporary;
  const fs::path destination = temporary.path() / "exact.tmp";
  const std::string body{"streamed-body"};
  write_file(destination, body);

  struct Mapping {
    objstore::ExactFileResult result;
    rc::SnapshotPayloadReadOutcome expected;
  };
  const std::vector<Mapping> mappings{
      {objstore::ExactFileResult::applied(body.size(), "\"etag\""),
       rc::SnapshotPayloadReadOutcome::APPLIED},
      {objstore::ExactFileResult::not_found(
           status(objstore::SE_NO_SUCH_KEY, "absent")),
       rc::SnapshotPayloadReadOutcome::ABSENT},
      {objstore::ExactFileResult::transient_unavailable(status(
           objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED,
           "unavailable")),
       rc::SnapshotPayloadReadOutcome::BLOCKED},
      {objstore::ExactFileResult::permanent_error(status(
           objstore::CLOUD_PROVIDER_UNRECOVERABLE_ERROR, "permanent")),
       rc::SnapshotPayloadReadOutcome::PERMANENT_ERROR},
      {objstore::ExactFileResult::unsupported(),
       rc::SnapshotPayloadReadOutcome::PERMANENT_ERROR},
  };
  for (const Mapping &mapping : mappings) {
    const rc::SnapshotPayloadDownloadResult downloaded =
        rc::ObjectStoreSnapshotExactFileReader::classify(mapping.result,
                                                         destination);
    expect(downloaded.outcome == mapping.expected,
           "ObjectStore exact-file outcome mapping changed");
  }

  const objstore::ExactFileResult wrong_size =
      objstore::ExactFileResult::applied(body.size() + 1, "\"etag\"");
  expect(rc::ObjectStoreSnapshotExactFileReader::classify(wrong_size,
                                                          destination)
                 .outcome == rc::SnapshotPayloadReadOutcome::PERMANENT_ERROR,
         "ObjectStore exact-file size mismatch was accepted");
}

}  // namespace

int main() {
  test_bootstrap_and_canonical_builder();
  test_takeover_preserves_physical_tip();
  test_background_snapshot_proves_older_anchor_ancestry();
  test_prepared_snapshot_recomputes_exact_retained_suffix();
  test_same_writer_snapshot_supersession_requires_refix();
  test_runtime_authority_accepts_same_writer_descendant_head();
  test_duplicate_same_bytes_is_idempotent();
  test_divergent_payload_and_manifest_fence_without_output();
  test_transport_unknown_and_unavailable_readback();
  test_prepare_resumes_without_recreating_verified_objects();
  test_head_conflict_and_epoch_loss_leave_orphans_only();
  test_invalid_cut_fails_before_remote_io();
  test_payload_limits_fail_before_remote_io();
  test_exact_file_reader_is_stream_hashed_and_cleaned();
  test_object_store_exact_file_outcome_mapping();
  std::cout << "snapshot publisher tests passed\n";
  return EXIT_SUCCESS;
}
