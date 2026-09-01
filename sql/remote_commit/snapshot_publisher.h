/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_SNAPSHOT_PUBLISHER_INCLUDED
#define SQL_REMOTE_COMMIT_SNAPSHOT_PUBLISHER_INCLUDED

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "objstore.h"
#include "sql/remote_commit/protocol_codec.h"
#include "sql/remote_commit/publisher.h"

namespace wesql::remote_commit {

// Runtime snapshot payloads are bounded independently of metadata reads. The
// total cap is deliberately explicit at every production construction site.
constexpr uint64_t kSnapshotMaxObjectBytes = 4ULL * 1024 * 1024 * 1024;
constexpr uint64_t kSnapshotMaxTotalPayloadBytes = 64ULL * 1024 * 1024 * 1024;

enum class SnapshotPayloadCreateOutcome : uint8_t {
  APPLIED,
  ALREADY_EXISTS,
  TRANSPORT_UNKNOWN,
  PERMANENT_ERROR,
};

struct SnapshotPayloadCreateResult {
  SnapshotPayloadCreateOutcome outcome{
      SnapshotPayloadCreateOutcome::PERMANENT_ERROR};
  std::string detail;
};

enum class SnapshotPayloadReadOutcome : uint8_t {
  APPLIED,
  ABSENT,
  BLOCKED,
  PERMANENT_ERROR,
};

struct PayloadFingerprint {
  uint64_t size{0};
  std::string sha256;

  bool operator==(const PayloadFingerprint &) const = default;
};

struct SnapshotPayloadReadResult {
  SnapshotPayloadReadOutcome outcome{
      SnapshotPayloadReadOutcome::PERMANENT_ERROR};
  std::string detail;
  std::optional<PayloadFingerprint> fingerprint;
};

struct SnapshotPayloadDownloadResult {
  SnapshotPayloadReadOutcome outcome{
      SnapshotPayloadReadOutcome::PERMANENT_ERROR};
  std::string detail;
};

// The destination is an existing empty regular file in a caller-owned private
// directory. APPLIED means a strongly consistent full-object streaming GET;
// failure outcomes leave the destination truncated.
class SnapshotExactFileReader {
 public:
  virtual ~SnapshotExactFileReader() = default;
  virtual SnapshotPayloadDownloadResult download_exact(
      std::string_view bucket, std::string_view key,
      const std::filesystem::path &destination) = 0;
  virtual SnapshotPayloadDownloadResult download_exact(
      std::string_view bucket, std::string_view key,
      const std::filesystem::path &destination, uint64_t max_bytes) {
    (void)max_bytes;
    return download_exact(bucket, key, destination);
  }
};

class ObjectStoreSnapshotExactFileReader final
    : public SnapshotExactFileReader {
 public:
  explicit ObjectStoreSnapshotExactFileReader(
      objstore::ObjectStore *object_store)
      : object_store_(object_store) {}

  SnapshotPayloadDownloadResult download_exact(
      std::string_view bucket, std::string_view key,
      const std::filesystem::path &destination) override;
  SnapshotPayloadDownloadResult download_exact(
      std::string_view bucket, std::string_view key,
      const std::filesystem::path &destination, uint64_t max_bytes) override;

  static SnapshotPayloadDownloadResult classify(
      const objstore::ExactFileResult &result,
      const std::filesystem::path &destination);

 private:
  objstore::ObjectStore *object_store_;
};

// This boundary keeps large payloads out of the metadata ConditionalIo path.
// Implementations must issue one create-only request per create call and an
// exact, strongly consistent GET for each readback call.
class SnapshotPayloadIo {
 public:
  virtual ~SnapshotPayloadIo() = default;

  virtual SnapshotPayloadCreateResult create_only_from_file(
      std::string_view key, const std::filesystem::path &source) = 0;
  virtual SnapshotPayloadReadResult readback(std::string_view key) = 0;
  virtual SnapshotPayloadReadResult readback(std::string_view key,
                                             uint64_t max_bytes) {
    (void)max_bytes;
    return readback(key);
  }
};

class ObjectStoreSnapshotPayloadIo final : public SnapshotPayloadIo {
 public:
  ObjectStoreSnapshotPayloadIo(objstore::ObjectStore *object_store,
                               std::string bucket,
                               SnapshotExactFileReader *exact_reader,
                               std::filesystem::path scratch_directory)
      : object_store_(object_store),
        bucket_(std::move(bucket)),
        exact_reader_(exact_reader),
        scratch_directory_(std::move(scratch_directory)) {}

  SnapshotPayloadCreateResult create_only_from_file(
      std::string_view key, const std::filesystem::path &source) override;
  SnapshotPayloadReadResult readback(std::string_view key) override;
  SnapshotPayloadReadResult readback(std::string_view key,
                                     uint64_t max_bytes) override;

 private:
  objstore::ObjectStore *object_store_;
  std::string bucket_;
  SnapshotExactFileReader *exact_reader_;
  std::filesystem::path scratch_directory_;
};

enum class FixedCutSource : uint8_t {
  EMPTY_SOURCE,
  RECOVERED_TAKEOVER,
  CLONE_BARRIER,
};

// The acquisition coordinator constructs this only after the clone REDO_COPY
// cut (or offline recovered cut) and the commit-admission barrier agree. The
// lease plus InnoDB, SmartEngine, and binlog pins stay externally owned until
// publish() returns.
struct FixedCutProof {
  FixedCutSource source{FixedCutSource::EMPTY_SOURCE};
  Cursor public_cursor;
  GtidSetDigest public_gtid;
  Cursor image_cursor;
  GtidSetDigest image_gtid;
  uint64_t source_head_generation{0};
  std::string source_head_body_sha256;
  std::optional<uint64_t> clone_handle_id;
  std::optional<std::string> redo_range_sha256;
  bool empty_source_scan_stable{false};
  bool internal_prepared_empty{false};
  bool external_xa_empty{false};
  bool old_tc_authority_empty{false};
  bool user_state_empty{false};
  bool legacy_live_extents_empty{false};
};

struct LocalSnapshotPayload {
  std::string component;
  std::string relative_path;
  std::filesystem::path local_path;
  std::string format;
};

struct PinnedSmartengineExtent {
  uint64_t writer_epoch{0};
  std::string allocation_seq;
  std::string database_name_hex;
  std::string index_id;
  std::string object_id;
  std::string key;
  uint64_t size{0};
  std::string sha256;
};

struct FixedSnapshotCut {
  std::string snapshot_id;
  Writer writer;
  FixedCutProof proof;
  LogAnchor log_anchor;
  ServerIdentity server_identity;
  DeploymentFingerprints deployment_fingerprints;
  std::filesystem::path binlog_seed_path;
  std::vector<LocalSnapshotPayload> objects;
  std::vector<PinnedSmartengineExtent> smartengine_extents;
};

struct SnapshotPublication {
  SnapshotManifest snapshot_manifest;
  SnapshotRef snapshot_ref;
  TransitionManifest transition;
  Head head;

  bool operator==(const SnapshotPublication &) const = default;
};

// Immutable remote snapshot payloads prepared without owning the HEAD order
// domain. The source engine/clone leases remain caller-owned until the final
// publish_prepared() call returns.
struct PreparedSnapshotPublication {
  FixedCutProof proof;
  Writer writer;
  SnapshotManifest snapshot_manifest;
  SnapshotRef snapshot_ref;
  std::optional<SnapshotRef> source_snapshot;

  bool operator==(const PreparedSnapshotPublication &) const = default;
};

// Immutable publisher proof captured while commit admission is closed and
// drained at the REDO_COPY cut. Runtime preparation may take arbitrarily long
// and therefore must not read HeadPublisher's concurrently changing state.
// Callers may refresh this value to a same-writer descendant HEAD; the cut's
// immutable anchor is validated against that descendant's manifest chain.
struct SnapshotPrepareAuthority {
  StreamIdentity stream;
  std::optional<PublishedBytes> epoch_object;
  std::optional<WriterEpoch> epoch;
  std::optional<PublishedBytes> head_object;
  std::optional<Head> head;

  bool operator==(const SnapshotPrepareAuthority &) const = default;
};

class SnapshotPublisher {
 public:
  SnapshotPublisher(SnapshotPayloadIo *payload_io,
                    ConditionalIo *metadata_io, HeadPublisher *head_publisher,
                    uint64_t max_object_bytes =
                        kSnapshotMaxObjectBytes,
                    uint64_t max_total_payload_bytes =
                        kSnapshotMaxTotalPayloadBytes,
                    size_t logical_attempt_limit = 2)
      : payload_io_(payload_io),
        metadata_io_(metadata_io),
        metadata_store_(metadata_io, logical_attempt_limit),
        head_publisher_(head_publisher),
        max_object_bytes_(max_object_bytes),
        max_total_payload_bytes_(max_total_payload_bytes),
        logical_attempt_limit_(logical_attempt_limit) {}

  // Hashes, uploads, and exact-verifies every immutable payload and snapshot
  // manifest without advancing HEAD. On failure, prepared is unchanged;
  // already verified remote objects are intentionally left orphaned.
  PublishResult prepare(const FixedSnapshotCut &cut,
                        PreparedSnapshotPublication *prepared);

  // Runtime-only preparation from a cut-time immutable authority. This method
  // never reads or mutates HeadPublisher state and never fences it directly;
  // the coordinator owns fail-stop/refix policy for the returned outcome.
  PublishResult prepare(const FixedSnapshotCut &cut,
                        const SnapshotPrepareAuthority &authority,
                        PreparedSnapshotPublication *prepared);

  // Performs only the final ordered transition. The caller serializes this
  // call with LOG publication. It re-reads the exact current retained suffix
  // through the prepared snapshot's anchor and derives a fresh recovery_window
  // before advancing HEAD. On failure, publication is unchanged.
  PublishResult publish_prepared(const PreparedSnapshotPublication &prepared,
                                 SnapshotPublication *publication);

  // Convenience for startup/offline callers with no concurrent LOG publisher.
  // Runtime callers use prepare() and later publish_prepared() inside the
  // shared remote order-token domain.
  PublishResult publish(const FixedSnapshotCut &cut,
                        SnapshotPublication *publication);

 private:
  PublishResult prepare_with_authority(
      const FixedSnapshotCut &cut,
      const SnapshotPrepareAuthority &authority,
      PreparedSnapshotPublication *prepared, bool update_publisher_state);

  SnapshotPayloadIo *payload_io_;
  ConditionalIo *metadata_io_;
  ProtocolStore metadata_store_;
  HeadPublisher *head_publisher_;
  uint64_t max_object_bytes_;
  uint64_t max_total_payload_bytes_;
  size_t logical_attempt_limit_;
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_SNAPSHOT_PUBLISHER_INCLUDED
