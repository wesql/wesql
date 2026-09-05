/* Copyright (c) 2026, ApeCloud Inc Holding Limited.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#ifndef SQL_REMOTE_COMMIT_PROTOCOL_CODEC_H_INCLUDED
#define SQL_REMOTE_COMMIT_PROTOCOL_CODEC_H_INCLUDED

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace wesql::remote_commit {

constexpr uint64_t kJsonSafeIntegerMax = 9007199254740991ULL;
constexpr size_t kWriterEpochMaxBytes = 4 * 1024;
constexpr size_t kHeadMaxBytes = 64 * 1024;
constexpr size_t kDeltaManifestMaxBytes = 8 * 1024 * 1024;
constexpr size_t kSnapshotManifestMaxBytes = 256 * 1024 * 1024;
constexpr size_t kTransitionSnapshotManifestMaxBytes = 64 * 1024;
constexpr size_t kMaxSegmentsPerManifest = 4096;
constexpr size_t kMaxSnapshotItems = 1000000;
constexpr uint64_t kRecoveryManifestCountMax = 100000;
constexpr uint64_t kRecoveryManifestBytesMax = 512ULL * 1024 * 1024;
constexpr uint64_t kRecoverySegmentCountMax = 1000000;
constexpr size_t kMaxJsonDepth = 16;
constexpr size_t kMaxObjectKeyBytes = 1024;
constexpr size_t kMaxOrdinaryIdBytes = 128;
constexpr size_t kMaxCanonicalGtidBytes = 16 * 1024 * 1024;

struct StreamIdentity {
  std::string repo_id;
  std::string branch_id;
  std::string cluster_object_prefix;
  std::string stream_id;
  std::string remote_prefix;
  std::string extent_prefix;
  std::string stream_sha256;

  bool operator==(const StreamIdentity &) const = default;
};

struct Cursor {
  std::string file;
  uint64_t pos{0};

  bool operator==(const Cursor &) const = default;
};

struct Writer {
  std::string id;
  uint64_t epoch{0};

  bool operator==(const Writer &) const = default;
};

struct ObjectRef {
  std::string key;
  uint64_t size{0};
  std::string sha256;

  bool operator==(const ObjectRef &) const = default;
};

struct HeadParent {
  uint64_t generation{0};
  std::string etag;
  std::string sha256;

  bool operator==(const HeadParent &) const = default;
};

struct ManifestRef {
  uint64_t generation{0};
  std::string key;
  uint64_t size{0};
  std::string sha256;

  bool operator==(const ManifestRef &) const = default;
};

struct RecoveryWindow {
  uint64_t manifest_count{0};
  uint64_t manifest_bytes{0};
  uint64_t segment_count{0};

  bool operator==(const RecoveryWindow &) const = default;
};

enum class SegmentTipKind { SEGMENT, SNAPSHOT_ROOT };

struct SegmentTip {
  SegmentTipKind kind{SegmentTipKind::SNAPSHOT_ROOT};
  std::optional<std::string> key;
  std::optional<uint64_t> size;
  std::optional<std::string> sha256;
  std::optional<uint64_t> sequence;
  std::optional<std::string> snapshot_id;
  std::optional<Cursor> cursor;

  bool operator==(const SegmentTip &) const = default;
};

struct SnapshotRef {
  std::string id;
  std::string manifest_key;
  uint64_t manifest_size{0};
  std::string manifest_sha256;
  Cursor cursor;

  bool operator==(const SnapshotRef &) const = default;
};

struct WriterEpoch {
  uint64_t epoch{0};
  std::string writer_id;
  uint64_t previous_epoch{0};

  bool operator==(const WriterEpoch &) const = default;
};

struct Head {
  std::string state{"READY"};
  uint64_t generation{0};
  Writer writer;
  std::optional<HeadParent> parent;
  ObjectRef manifest;
  RecoveryWindow recovery_window;
  SegmentTip segment_tip;
  Cursor base_cursor;
  Cursor durable_cursor;
  SnapshotRef snapshot;

  bool operator==(const Head &) const = default;
};

struct GtidSetDigest {
  std::string canonical;
  std::string sha256;

  bool operator==(const GtidSetDigest &) const = default;
};

struct XidDigest {
  uint64_t count{0};
  std::string sha256;

  bool operator==(const XidDigest &) const = default;
};

struct SegmentSource {
  std::string file;
  uint64_t start_pos{0};
  uint64_t end_pos{0};

  bool operator==(const SegmentSource &) const = default;
};

struct SegmentRef {
  uint64_t sequence{0};
  std::string key;
  uint64_t size{0};
  std::string sha256;
  SegmentSource source;
  SegmentTip previous_segment;
  uint64_t transaction_count{0};
  GtidSetDigest gtid_set;
  XidDigest xids;
  bool ends_at_transaction_boundary{true};
  std::string payload_format{"native-mysql-binlog-range-v1"};
  std::string compression{"none"};

  bool operator==(const SegmentRef &) const = default;
};

enum class ManifestKind { BOOTSTRAP, LOG_TRANSITION, SNAPSHOT };

struct TransitionManifest {
  ManifestKind kind{ManifestKind::BOOTSTRAP};
  uint64_t generation{0};
  Writer writer;
  std::optional<HeadParent> head_parent;
  std::optional<ManifestRef> previous;
  RecoveryWindow recovery_window;
  SegmentTip segment_tip;
  SnapshotRef snapshot;
  Cursor base_cursor;
  Cursor durable_cursor;
  std::vector<SegmentRef> segments;

  bool operator==(const TransitionManifest &) const = default;
};

enum class LogAnchorKind { EMPTY_BASE, MANIFEST_BOUNDARY };

struct LogAnchor {
  LogAnchorKind kind{LogAnchorKind::EMPTY_BASE};
  std::optional<uint64_t> generation;
  std::optional<ObjectRef> manifest;
  Cursor cursor;

  bool operator==(const LogAnchor &) const = default;
};

struct ServerIdentity {
  std::string server_uuid;

  bool operator==(const ServerIdentity &) const = default;
};

struct DeploymentFingerprints {
  std::string startup_config_sha256;
  std::string server_build;
  std::string plugin_component_set_sha256;
  std::string keyring_config_sha256;
  std::string tls_config_sha256;

  bool operator==(const DeploymentFingerprints &) const = default;
};

struct BinlogSeed {
  std::string file;
  Cursor cursor;
  std::string key;
  uint64_t size{0};
  std::string sha256;
  std::string compression{"none"};
  std::string format{"native-mysql-binlog-prefix-v1"};
  std::string checksum{"CRC32"};

  bool operator==(const BinlogSeed &) const = default;
};

struct SnapshotObject {
  std::string component;
  uint64_t ordinal{0};
  std::string relative_path;
  std::string key;
  uint64_t size{0};
  std::string sha256;
  std::string compression{"none"};
  std::string format;

  bool operator==(const SnapshotObject &) const = default;
};

struct SmartengineExtentRef {
  uint64_t ordinal{0};
  uint64_t writer_epoch{0};
  std::string allocation_seq;
  std::string database_name_hex;
  std::string index_id;
  std::string object_id;
  std::string key;
  uint64_t size{0};
  std::string sha256;
  std::string format{"smartengine-object-extent-v2"};

  bool operator==(const SmartengineExtentRef &) const = default;
};

struct SnapshotManifest {
  std::string snapshot_id;
  Writer writer;
  Cursor cursor;
  LogAnchor log_anchor;
  ServerIdentity server_identity;
  DeploymentFingerprints deployment_fingerprints;
  GtidSetDigest gtid_executed;
  BinlogSeed binlog_seed;
  std::vector<SnapshotObject> objects;
  std::vector<SmartengineExtentRef> smartengine_extents;

  bool operator==(const SnapshotManifest &) const = default;
};

bool build_stream_identity(std::string_view repo_id,
                           std::string_view branch_id,
                           std::string_view cluster_object_prefix,
                           StreamIdentity *result, std::string *error);

bool sha256_hex(std::string_view bytes, std::string *digest,
                std::string *error);
bool canonicalize_gtid_set(std::string_view input, std::string *canonical,
                           std::string *error);
bool gtid_digest(std::string_view input, GtidSetDigest *digest,
                 std::string *error);
bool xid_jcs_preimage(const std::vector<uint64_t> &xids,
                      std::string *preimage, std::string *error);
bool xid_digest(const std::vector<uint64_t> &xids, XidDigest *digest,
                std::string *error);
bool segment_refs_digest(const std::vector<SegmentRef> &segments,
                         std::string *sha256, std::string *error);

bool transition_manifest_key(const StreamIdentity &stream, const Writer &writer,
                             uint64_t generation, std::string_view body_sha256,
                             std::string *key, std::string *error);
bool segment_object_key(const StreamIdentity &stream, const Writer &writer,
                        const SegmentSource &source,
                        std::string_view body_sha256, std::string *key,
                        std::string *error);
bool snapshot_manifest_key(const StreamIdentity &stream,
                           std::string_view snapshot_id,
                           std::string_view body_sha256, std::string *key,
                           std::string *error);
bool binlog_seed_object_key(const StreamIdentity &stream,
                            std::string_view snapshot_id,
                            const Cursor &cursor,
                            std::string_view body_sha256, std::string *key,
                            std::string *error);
bool snapshot_object_key(const StreamIdentity &stream,
                         std::string_view snapshot_id,
                         const SnapshotObject &object, std::string *key,
                         std::string *error);
bool smartengine_extent_object_key(const StreamIdentity &stream,
                                   const SmartengineExtentRef &extent,
                                   std::string *key, std::string *error);

bool serialize_writer_epoch(const StreamIdentity &stream,
                            const WriterEpoch &value, std::string *json,
                            std::string *error);
bool parse_writer_epoch(std::string_view json, const StreamIdentity &stream,
                        WriterEpoch *value, std::string *error);

bool serialize_head(const StreamIdentity &stream, const Head &value,
                    std::string *json, std::string *error);
bool parse_head(std::string_view json, const StreamIdentity &stream,
                Head *value, std::string *error);

bool serialize_transition_manifest(const StreamIdentity &stream,
                                   const TransitionManifest &value,
                                   std::string *json, std::string *error);
bool stabilize_transition_manifest(const StreamIdentity &stream,
                                   uint64_t base_manifest_bytes,
                                   TransitionManifest *value,
                                   std::string *json, std::string *error);
bool parse_transition_manifest(std::string_view json,
                               const StreamIdentity &stream,
                               std::string_view expected_object_key,
                               TransitionManifest *value,
                               std::string *error);

bool serialize_snapshot_manifest(const StreamIdentity &stream,
                                 const SnapshotManifest &value,
                                 std::string *json, std::string *error);
bool parse_snapshot_manifest(std::string_view json,
                             const StreamIdentity &stream,
                             std::string_view expected_object_key,
                             SnapshotManifest *value, std::string *error);

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_PROTOCOL_CODEC_H_INCLUDED
