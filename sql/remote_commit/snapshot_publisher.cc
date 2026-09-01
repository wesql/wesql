/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/snapshot_publisher.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <fstream>
#include <limits>
#include <map>
#include <set>
#include <tuple>
#include <utility>
#include <vector>

#include <openssl/evp.h>
#include <unistd.h>

namespace wesql::remote_commit {
namespace {

namespace fs = std::filesystem;

PublishResult result(PublishOutcome outcome, std::string detail = {}) {
  return {outcome, std::move(detail), std::nullopt};
}

std::string status_detail(const objstore::Status &status) {
  return std::string(status.error_message());
}

bool is_sha256(std::string_view value) {
  return value.size() == 64 &&
         value.find_first_not_of("0123456789abcdef") == std::string_view::npos;
}

PublishResult terminal(HeadPublisher *publisher, PublishOutcome outcome,
                       std::string detail) {
  if (publisher != nullptr) {
    if (outcome == PublishOutcome::FENCED) publisher->fence(detail);
    if (outcome == PublishOutcome::BLOCKED) publisher->mark_blocked(detail);
  }
  return result(outcome, std::move(detail));
}

bool stream_file_fingerprint(const fs::path &path, PayloadFingerprint *value,
                             std::string *error) {
  if (value == nullptr || error == nullptr) return false;
  std::error_code filesystem_error;
  if (!fs::is_regular_file(path, filesystem_error) || filesystem_error) {
    *error = "snapshot payload is not a readable regular file: " +
             path.string();
    return false;
  }
  const uintmax_t size_before = fs::file_size(path, filesystem_error);
  if (filesystem_error || size_before > kJsonSafeIntegerMax) {
    *error = "snapshot payload size is unavailable or outside the protocol "
             "limit: " +
             path.string();
    return false;
  }
  const auto time_before = fs::last_write_time(path, filesystem_error);
  if (filesystem_error) {
    *error = "snapshot payload timestamp is unavailable: " + path.string();
    return false;
  }

  std::ifstream input(path, std::ios::binary);
  if (!input) {
    *error = "cannot open snapshot payload for hashing: " + path.string();
    return false;
  }
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (context == nullptr) {
    *error = "cannot allocate snapshot SHA-256 context";
    return false;
  }
  bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1;
  std::array<char, 1024 * 1024> buffer{};
  while (ok && input) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const std::streamsize count = input.gcount();
    if (count > 0) {
      ok = EVP_DigestUpdate(context, buffer.data(),
                            static_cast<size_t>(count)) == 1;
    }
  }
  if (!input.eof()) ok = false;
  std::array<unsigned char, 32> digest{};
  unsigned int digest_length = 0;
  if (ok) {
    ok = EVP_DigestFinal_ex(context, digest.data(), &digest_length) == 1 &&
         digest_length == digest.size();
  }
  EVP_MD_CTX_free(context);
  if (!ok) {
    *error = "cannot stream snapshot payload SHA-256: " + path.string();
    return false;
  }

  const uintmax_t size_after = fs::file_size(path, filesystem_error);
  if (filesystem_error) {
    *error = "snapshot payload disappeared after hashing: " + path.string();
    return false;
  }
  const auto time_after = fs::last_write_time(path, filesystem_error);
  if (filesystem_error || size_before != size_after ||
      time_before != time_after) {
    *error = "snapshot payload changed while being hashed: " + path.string();
    return false;
  }

  static constexpr char kHex[] = "0123456789abcdef";
  value->size = static_cast<uint64_t>(size_before);
  value->sha256.clear();
  value->sha256.reserve(digest.size() * 2);
  for (const unsigned char byte : digest) {
    value->sha256.push_back(kHex[byte >> 4]);
    value->sha256.push_back(kHex[byte & 0x0f]);
  }
  return true;
}

bool account_payload_bytes(uint64_t size, uint64_t max_object_bytes,
                           uint64_t max_total_payload_bytes, uint64_t *total) {
  if (total == nullptr || size > max_object_bytes ||
      *total > max_total_payload_bytes ||
      size > max_total_payload_bytes - *total) {
    return false;
  }
  *total += size;
  return true;
}

uint32_t read_le32(const std::array<unsigned char, 19> &header,
                   size_t offset) {
  return static_cast<uint32_t>(header[offset]) |
         (static_cast<uint32_t>(header[offset + 1]) << 8) |
         (static_cast<uint32_t>(header[offset + 2]) << 16) |
         (static_cast<uint32_t>(header[offset + 3]) << 24);
}

bool validate_binlog_seed(const fs::path &path, uint64_t expected_size,
                          std::string *error) {
  constexpr std::array<unsigned char, 4> kMagic{0xfe, 0x62, 0x69, 0x6e};
  constexpr size_t kEventHeaderBytes = 19;
  constexpr uint8_t kFormatDescriptionEvent = 15;
  constexpr uint32_t kChecksumBytes = 4;
  if (expected_size < kMagic.size() + kEventHeaderBytes + kChecksumBytes) {
    *error = "binlog seed is too short for magic, FDE, and CRC32";
    return false;
  }
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    *error = "cannot open immutable binlog seed";
    return false;
  }
  std::array<unsigned char, 4> magic{};
  input.read(reinterpret_cast<char *>(magic.data()), magic.size());
  if (!input || magic != kMagic) {
    *error = "immutable binlog seed has invalid magic";
    return false;
  }
  uint64_t offset = magic.size();
  bool first = true;
  while (offset < expected_size) {
    if (expected_size - offset < kEventHeaderBytes) {
      *error = "immutable binlog seed ends in a partial event header";
      return false;
    }
    std::array<unsigned char, kEventHeaderBytes> header{};
    input.read(reinterpret_cast<char *>(header.data()), header.size());
    if (!input) {
      *error = "cannot read immutable binlog seed event header";
      return false;
    }
    const uint32_t event_size = read_le32(header, 9);
    if (event_size < kEventHeaderBytes + kChecksumBytes ||
        event_size > expected_size - offset) {
      *error = "immutable binlog seed contains an invalid event size";
      return false;
    }
    if (first && header[4] != kFormatDescriptionEvent) {
      *error = "immutable binlog seed does not begin with an FDE";
      return false;
    }
    first = false;
    input.seekg(static_cast<std::streamoff>(event_size - kEventHeaderBytes),
                std::ios::cur);
    if (!input) {
      *error = "cannot scan immutable binlog seed event";
      return false;
    }
    offset += event_size;
  }
  return !first && offset == expected_size;
}

bool parse_decimal(std::string_view text, uint64_t *value) {
  if (text.empty() || (text.size() > 1 && text.front() == '0')) return false;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), *value);
  return parsed.ec == std::errc() && parsed.ptr == text.data() + text.size();
}

bool parse_binlog_file(std::string_view file, std::string_view *basename,
                       uint64_t *sequence) {
  const size_t dot = file.rfind('.');
  if (dot == std::string_view::npos || dot == 0 || dot + 1 == file.size())
    return false;
  const std::string_view digits = file.substr(dot + 1);
  if (digits.size() < 6) return false;
  uint64_t parsed = 0;
  const auto conversion =
      std::from_chars(digits.data(), digits.data() + digits.size(), parsed);
  if (conversion.ec != std::errc() ||
      conversion.ptr != digits.data() + digits.size() ||
      parsed > kJsonSafeIntegerMax)
    return false;
  std::string canonical = std::to_string(parsed);
  if (canonical.size() < 6) canonical.insert(0, 6 - canonical.size(), '0');
  if (digits != canonical) return false;
  *basename = file.substr(0, dot);
  *sequence = parsed;
  return true;
}

bool compare_cursor(const Cursor &left, const Cursor &right, int *comparison) {
  std::string_view left_base;
  std::string_view right_base;
  uint64_t left_sequence = 0;
  uint64_t right_sequence = 0;
  if (!parse_binlog_file(left.file, &left_base, &left_sequence) ||
      !parse_binlog_file(right.file, &right_base, &right_sequence) ||
      left_base != right_base)
    return false;
  if (left_sequence != right_sequence) {
    *comparison = left_sequence < right_sequence ? -1 : 1;
  } else if (left.pos != right.pos) {
    *comparison = left.pos < right.pos ? -1 : 1;
  } else {
    *comparison = 0;
  }
  return true;
}

PublishResult verify_readback(SnapshotPayloadIo *io, std::string_view key,
                              const PayloadFingerprint &expected) {
  const SnapshotPayloadReadResult readback =
      io->readback(key, std::max<uint64_t>(expected.size, 1));
  switch (readback.outcome) {
    case SnapshotPayloadReadOutcome::APPLIED:
      if (!readback.fingerprint.has_value())
        return result(PublishOutcome::PERMANENT_ERROR,
                      "exact payload GET omitted its fingerprint");
      return *readback.fingerprint == expected
                 ? result(PublishOutcome::APPLIED)
                 : result(PublishOutcome::FENCED,
                          "immutable payload read-back has divergent bytes: " +
                              std::string(key));
    case SnapshotPayloadReadOutcome::ABSENT:
      return result(PublishOutcome::ABSENT, readback.detail);
    case SnapshotPayloadReadOutcome::BLOCKED:
      return result(PublishOutcome::BLOCKED, readback.detail);
    case SnapshotPayloadReadOutcome::PERMANENT_ERROR:
      return result(PublishOutcome::PERMANENT_ERROR, readback.detail);
  }
  return result(PublishOutcome::PERMANENT_ERROR,
                "unknown exact payload GET outcome");
}

PublishResult create_and_verify(SnapshotPayloadIo *io, std::string_view key,
                                const fs::path &source,
                                const PayloadFingerprint &expected,
                                size_t logical_attempt_limit) {
  if (logical_attempt_limit == 0) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "snapshot payload logical attempt limit must be positive");
  }
  const auto verify_source_unchanged = [&]() {
    PayloadFingerprint after;
    std::string error;
    if (!stream_file_fingerprint(source, &after, &error))
      return result(PublishOutcome::PERMANENT_ERROR, std::move(error));
    if (after != expected) {
      return result(PublishOutcome::FENCED,
                    "pinned snapshot payload changed during publication: " +
                        source.string());
    }
    return result(PublishOutcome::APPLIED);
  };

  const PublishResult existing = verify_readback(io, key, expected);
  if (existing.outcome == PublishOutcome::APPLIED)
    return verify_source_unchanged();
  if (existing.outcome != PublishOutcome::ABSENT) return existing;

  for (size_t attempt = 0; attempt < logical_attempt_limit; ++attempt) {
    const SnapshotPayloadCreateResult created =
        io->create_only_from_file(key, source);
    if (created.outcome == SnapshotPayloadCreateOutcome::PERMANENT_ERROR) {
      return result(PublishOutcome::PERMANENT_ERROR, created.detail);
    }
    const PublishResult readback = verify_readback(io, key, expected);
    if (readback.outcome == PublishOutcome::APPLIED) {
      return verify_source_unchanged();
    }
    if (readback.outcome != PublishOutcome::ABSENT) return readback;
    if (created.outcome != SnapshotPayloadCreateOutcome::TRANSPORT_UNKNOWN) {
      return result(PublishOutcome::FENCED,
                    "create-only payload disappeared during read-back: " +
                        std::string(key));
    }
    if (attempt + 1 == logical_attempt_limit) {
      return result(PublishOutcome::BLOCKED,
                    "create-only payload remains absent after an unknown "
                    "transport result: " +
                        std::string(key));
    }
  }
  return result(PublishOutcome::PERMANENT_ERROR,
                "unreachable immutable payload publication state");
}

PublishResult create_or_verify_snapshot_manifest(ProtocolStore *store,
                                                 std::string_view key,
                                                 std::string_view body) {
  const PublishResult existing =
      store->read(key, static_cast<uint64_t>(body.size()) + 1);
  if (existing.outcome == PublishOutcome::APPLIED) {
    if (!existing.object.has_value() || existing.object->body != body) {
      return result(PublishOutcome::FENCED,
                    "immutable snapshot manifest has divergent bytes");
    }
    return existing;
  }
  if (existing.outcome != PublishOutcome::ABSENT) return existing;
  return store->create_immutable(key, body);
}

struct PreparedFile {
  fs::path path;
  PayloadFingerprint fingerprint;
};

struct ExtentCandidate {
  SmartengineExtentRef ref;
  uint64_t allocation{0};
  uint64_t index{0};
  uint64_t object{0};
};

SegmentTip snapshot_root(const SnapshotRef &snapshot) {
  SegmentTip tip;
  tip.kind = SegmentTipKind::SNAPSHOT_ROOT;
  tip.snapshot_id = snapshot.id;
  tip.cursor = snapshot.cursor;
  return tip;
}

}  // namespace

SnapshotPayloadCreateResult
ObjectStoreSnapshotPayloadIo::create_only_from_file(
    std::string_view key, const fs::path &source) {
  if (object_store_ == nullptr) {
    return {SnapshotPayloadCreateOutcome::PERMANENT_ERROR,
            "null object-store snapshot payload client"};
  }
  const auto created = object_store_->put_object_from_file_conditional(
      bucket_, key, source.string(),
      objstore::ConditionalPutCondition::create_only());
  switch (created.outcome()) {
    case objstore::ConditionalPutOutcome::APPLIED:
      return {SnapshotPayloadCreateOutcome::APPLIED, {}};
    case objstore::ConditionalPutOutcome::CONFLICT_409:
    case objstore::ConditionalPutOutcome::PRECONDITION_FAILED_412:
      return {SnapshotPayloadCreateOutcome::ALREADY_EXISTS,
              status_detail(created.status())};
    case objstore::ConditionalPutOutcome::TRANSPORT_UNKNOWN:
      return {SnapshotPayloadCreateOutcome::TRANSPORT_UNKNOWN,
              status_detail(created.status())};
    case objstore::ConditionalPutOutcome::PERMANENT_ERROR:
    case objstore::ConditionalPutOutcome::UNSUPPORTED:
      return {SnapshotPayloadCreateOutcome::PERMANENT_ERROR,
              status_detail(created.status())};
  }
  return {SnapshotPayloadCreateOutcome::PERMANENT_ERROR,
          "unknown conditional payload PUT outcome"};
}

SnapshotPayloadDownloadResult
ObjectStoreSnapshotExactFileReader::download_exact(
    std::string_view bucket, std::string_view key,
    const fs::path &destination) {
  return download_exact(bucket, key, destination,
                        std::numeric_limits<uint64_t>::max());
}

SnapshotPayloadDownloadResult
ObjectStoreSnapshotExactFileReader::download_exact(
    std::string_view bucket, std::string_view key,
    const fs::path &destination, uint64_t max_bytes) {
  if (object_store_ == nullptr) {
    return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
            "null exact-file ObjectStore client"};
  }
  const objstore::ExactFileResult downloaded =
      object_store_->get_object_to_file_exact(bucket, key,
                                              destination.string(), max_bytes);
  return classify(downloaded, destination);
}

SnapshotPayloadDownloadResult ObjectStoreSnapshotExactFileReader::classify(
    const objstore::ExactFileResult &result, const fs::path &destination) {
  switch (result.outcome()) {
    case objstore::ExactFileOutcome::APPLIED: {
      std::error_code filesystem_error;
      const uintmax_t file_size = fs::file_size(destination, filesystem_error);
      if (filesystem_error || file_size != result.size()) {
        return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
                "exact-file ObjectStore size does not match streamed bytes"};
      }
      return {SnapshotPayloadReadOutcome::APPLIED, {}};
    }
    case objstore::ExactFileOutcome::NOT_FOUND_404:
      return {SnapshotPayloadReadOutcome::ABSENT,
              status_detail(result.status())};
    case objstore::ExactFileOutcome::TRANSIENT_UNAVAILABLE:
      return {SnapshotPayloadReadOutcome::BLOCKED,
              status_detail(result.status())};
    case objstore::ExactFileOutcome::PERMANENT_ERROR:
    case objstore::ExactFileOutcome::UNSUPPORTED:
      return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
              status_detail(result.status())};
  }
  return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
          "unknown exact-file ObjectStore outcome"};
}

SnapshotPayloadReadResult ObjectStoreSnapshotPayloadIo::readback(
    std::string_view key) {
  return readback(key, std::numeric_limits<uint64_t>::max());
}

SnapshotPayloadReadResult ObjectStoreSnapshotPayloadIo::readback(
    std::string_view key, uint64_t max_bytes) {
  if (exact_reader_ == nullptr || scratch_directory_.empty()) {
    return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
            "snapshot payload exact streaming reader is not configured",
            std::nullopt};
  }

  std::error_code filesystem_error;
  if (!fs::is_directory(scratch_directory_, filesystem_error) ||
      filesystem_error) {
    return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
            "snapshot readback scratch directory is unavailable",
            std::nullopt};
  }
  std::string pattern =
      (scratch_directory_ / ".wesql-snapshot-readback-XXXXXX").string();
  std::vector<char> writable(pattern.begin(), pattern.end());
  writable.push_back('\0');
  const int descriptor = ::mkstemp(writable.data());
  if (descriptor < 0) {
    return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
            "cannot create exact-readback scratch file", std::nullopt};
  }
  const fs::path destination(writable.data());
  if (::close(descriptor) != 0) {
    fs::remove(destination, filesystem_error);
    return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
            "cannot close exact-readback scratch file", std::nullopt};
  }

  const SnapshotPayloadDownloadResult downloaded =
      exact_reader_->download_exact(bucket_, key, destination, max_bytes);
  if (downloaded.outcome != SnapshotPayloadReadOutcome::APPLIED) {
    fs::remove(destination, filesystem_error);
    return {downloaded.outcome, downloaded.detail, std::nullopt};
  }
  PayloadFingerprint fingerprint;
  std::string error;
  if (!stream_file_fingerprint(destination, &fingerprint, &error)) {
    fs::remove(destination, filesystem_error);
    return {SnapshotPayloadReadOutcome::PERMANENT_ERROR, std::move(error),
            std::nullopt};
  }
  if (!fs::remove(destination, filesystem_error) || filesystem_error) {
    return {SnapshotPayloadReadOutcome::PERMANENT_ERROR,
            "cannot remove exact-readback scratch file", std::nullopt};
  }
  return {SnapshotPayloadReadOutcome::APPLIED, {}, std::move(fingerprint)};
}

PublishResult SnapshotPublisher::prepare(
    const FixedSnapshotCut &cut, PreparedSnapshotPublication *prepared) {
  if (head_publisher_ == nullptr) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "snapshot publisher is not initialized");
  }
  const PublisherState state = head_publisher_->state();
  const SnapshotPrepareAuthority authority{
      head_publisher_->stream(), state.epoch_object, state.epoch,
      state.head_object, state.head};
  return prepare_with_authority(cut, authority, prepared, true);
}

PublishResult SnapshotPublisher::prepare(
    const FixedSnapshotCut &cut, const SnapshotPrepareAuthority &authority,
    PreparedSnapshotPublication *prepared) {
  return prepare_with_authority(cut, authority, prepared, false);
}

PublishResult SnapshotPublisher::prepare_with_authority(
    const FixedSnapshotCut &cut, const SnapshotPrepareAuthority &authority,
    PreparedSnapshotPublication *prepared, bool update_publisher_state) {
  if (prepared == nullptr || payload_io_ == nullptr ||
      metadata_io_ == nullptr ||
      head_publisher_ == nullptr || max_object_bytes_ == 0 ||
      max_total_payload_bytes_ == 0 || logical_attempt_limit_ == 0) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "snapshot publisher is not initialized");
  }
  if (cut.objects.size() > kMaxSnapshotItems ||
      cut.smartengine_extents.size() >
          kMaxSnapshotItems - cut.objects.size()) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "snapshot input exceeds the combined item limit");
  }
  if (authority.stream != head_publisher_->stream()) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "snapshot prepare authority belongs to another stream");
  }
  const StreamIdentity &stream = authority.stream;
  const PublisherState initial_state{
      LifecycleState::RUNNING, authority.epoch_object, authority.epoch,
      authority.head_object, authority.head};
  WriterEpoch canonical_epoch;
  Head canonical_head;
  std::string authority_error;
  if (!initial_state.epoch_object.has_value() ||
      !initial_state.epoch.has_value() ||
      initial_state.epoch_object->body.empty() ||
      initial_state.epoch_object->etag.empty() ||
      !parse_writer_epoch(initial_state.epoch_object->body, stream,
                          &canonical_epoch, &authority_error) ||
      canonical_epoch != *initial_state.epoch ||
      initial_state.head_object.has_value() != initial_state.head.has_value() ||
      (initial_state.head.has_value() &&
       (initial_state.head_object->body.empty() ||
        initial_state.head_object->etag.empty() ||
        !parse_head(initial_state.head_object->body, stream, &canonical_head,
                    &authority_error) ||
        canonical_head != *initial_state.head))) {
    return terminal(update_publisher_state ? head_publisher_ : nullptr,
                    PublishOutcome::FENCED,
                    "snapshot prepare authority is not canonical: " +
                        authority_error);
  }
  if (
      initial_state.epoch->epoch != cut.writer.epoch ||
      initial_state.epoch->writer_id != cut.writer.id) {
    return terminal(update_publisher_state ? head_publisher_ : nullptr,
                    PublishOutcome::FENCED,
                    "fixed snapshot cut does not own WRITER_EPOCH");
  }
  if (cut.proof.public_cursor != cut.proof.image_cursor ||
      cut.proof.public_gtid != cut.proof.image_gtid) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "clone image and admission barrier describe different cuts");
  }
  if (cut.proof.source == FixedCutSource::CLONE_BARRIER) {
    if (!cut.proof.clone_handle_id.has_value() ||
        *cut.proof.clone_handle_id == 0 ||
        !cut.proof.redo_range_sha256.has_value() ||
        !is_sha256(*cut.proof.redo_range_sha256)) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "clone-barrier cut lacks immutable handle/redo identity");
    }
  } else if (cut.proof.clone_handle_id.has_value() ||
             cut.proof.redo_range_sha256.has_value()) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "non-clone cut contains clone-only identity fields");
  }

  if (cut.proof.source == FixedCutSource::EMPTY_SOURCE) {
    if (initial_state.head.has_value() || initial_state.head_object.has_value() ||
        cut.log_anchor.kind != LogAnchorKind::EMPTY_BASE ||
        cut.proof.source_head_generation != 0 ||
        !cut.proof.source_head_body_sha256.empty() ||
        !cut.proof.empty_source_scan_stable ||
        !cut.proof.internal_prepared_empty ||
        !cut.proof.external_xa_empty ||
        !cut.proof.old_tc_authority_empty || !cut.proof.user_state_empty ||
        !cut.proof.legacy_live_extents_empty) {
      return terminal(update_publisher_state ? head_publisher_ : nullptr,
                      PublishOutcome::FENCED,
                      "EMPTY_SOURCE cut is not an absent-HEAD bootstrap");
    }
  } else {
    if (cut.proof.empty_source_scan_stable ||
        cut.proof.internal_prepared_empty || cut.proof.external_xa_empty ||
        cut.proof.old_tc_authority_empty || cut.proof.user_state_empty ||
        cut.proof.legacy_live_extents_empty) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "non-bootstrap cut contains EMPTY_SOURCE-only proof");
    }
    if (!initial_state.head.has_value() ||
        !initial_state.head_object.has_value() ||
        cut.log_anchor.kind != LogAnchorKind::MANIFEST_BOUNDARY ||
        !cut.log_anchor.generation.has_value() ||
        !cut.log_anchor.manifest.has_value() ||
        cut.proof.source_head_generation != *cut.log_anchor.generation ||
        initial_state.head->generation < *cut.log_anchor.generation ||
        cut.log_anchor.cursor != cut.proof.public_cursor ||
        !is_sha256(cut.proof.source_head_body_sha256)) {
      return terminal(update_publisher_state ? head_publisher_ : nullptr,
                      PublishOutcome::FENCED,
                      "fixed cut does not bind the exact source HEAD boundary");
    }
    std::string error;
    int low = 0;
    int high = 0;
    if (!compare_cursor(initial_state.head->base_cursor,
                        cut.proof.public_cursor, &low) ||
        !compare_cursor(cut.proof.public_cursor,
                        initial_state.head->durable_cursor, &high) ||
        low > 0 || high > 0) {
      return terminal(update_publisher_state ? head_publisher_ : nullptr,
                      PublishOutcome::FENCED,
                      "current HEAD does not cover the fixed cut cursor");
    }
    if (initial_state.head->generation ==
        cut.proof.source_head_generation) {
      std::string source_head_sha;
      if (!sha256_hex(initial_state.head_object->body, &source_head_sha,
                      &error) ||
          source_head_sha != cut.proof.source_head_body_sha256) {
        return terminal(
            update_publisher_state ? head_publisher_ : nullptr,
            PublishOutcome::FENCED,
            "fixed cut source HEAD digest does not match cached HEAD");
      }
    }

    ManifestRef walk_ref{initial_state.head->generation,
                         initial_state.head->manifest.key,
                         initial_state.head->manifest.size,
                         initial_state.head->manifest.sha256};
    const uint64_t walk_limit =
        std::min<uint64_t>(initial_state.head->recovery_window.manifest_count,
                           kRecoveryManifestCountMax);
    bool first_manifest = true;
    bool found_anchor = false;
    for (uint64_t traversed = 0; traversed < walk_limit; ++traversed) {
      const PublishResult manifest_read = metadata_store_.read(walk_ref.key);
      if (!manifest_read.applied()) {
        const PublishOutcome outcome =
            manifest_read.outcome == PublishOutcome::ABSENT
                ? PublishOutcome::FENCED
                : manifest_read.outcome;
        return terminal(update_publisher_state ? head_publisher_ : nullptr,
                        outcome,
                        "cannot exact-read snapshot anchor chain: " +
                            manifest_read.detail);
      }
      std::string body_sha;
      TransitionManifest manifest;
      if (manifest_read.object->body.size() != walk_ref.size ||
          !sha256_hex(manifest_read.object->body, &body_sha, &error) ||
          body_sha != walk_ref.sha256 ||
          !parse_transition_manifest(manifest_read.object->body, stream,
                                     walk_ref.key, &manifest, &error) ||
          manifest.generation != walk_ref.generation) {
        return terminal(update_publisher_state ? head_publisher_ : nullptr,
                        PublishOutcome::FENCED,
                        "invalid immutable snapshot anchor chain: " + error);
      }
      if (first_manifest &&
          (manifest.recovery_window != initial_state.head->recovery_window ||
           manifest.segment_tip != initial_state.head->segment_tip ||
           manifest.snapshot != initial_state.head->snapshot ||
           manifest.base_cursor != initial_state.head->base_cursor ||
           manifest.durable_cursor != initial_state.head->durable_cursor)) {
        return terminal(update_publisher_state ? head_publisher_ : nullptr,
                        PublishOutcome::FENCED,
                        "current HEAD does not match its exact manifest");
      }
      first_manifest = false;
      if (walk_ref.generation == *cut.log_anchor.generation) {
        if (walk_ref.key != cut.log_anchor.manifest->key ||
            walk_ref.size != cut.log_anchor.manifest->size ||
            walk_ref.sha256 != cut.log_anchor.manifest->sha256 ||
            manifest.durable_cursor != cut.proof.public_cursor) {
          return terminal(update_publisher_state ? head_publisher_ : nullptr,
                          PublishOutcome::FENCED,
                          "fixed cut anchor is not the exact cursor boundary");
        }
        found_anchor = true;
        break;
      }
      if (walk_ref.generation < *cut.log_anchor.generation ||
          !manifest.previous.has_value()) {
        break;
      }
      walk_ref = *manifest.previous;
    }
    if (!found_anchor) {
      return terminal(update_publisher_state ? head_publisher_ : nullptr,
                      PublishOutcome::FENCED,
                      "fixed cut anchor is not an ancestor of current HEAD");
    }

    const SnapshotRef &prior_ref = initial_state.head->snapshot;
    const PublishResult prior_snapshot_read =
        metadata_store_.read(prior_ref.manifest_key);
    if (!prior_snapshot_read.applied()) {
      const PublishOutcome outcome =
          prior_snapshot_read.outcome == PublishOutcome::ABSENT
              ? PublishOutcome::FENCED
              : prior_snapshot_read.outcome;
      return terminal(update_publisher_state ? head_publisher_ : nullptr,
                      outcome,
                      "cannot exact-read prior snapshot identity: " +
                          prior_snapshot_read.detail);
    }
    std::string prior_snapshot_sha;
    SnapshotManifest prior_snapshot;
    if (prior_snapshot_read.object->body.size() != prior_ref.manifest_size ||
        !sha256_hex(prior_snapshot_read.object->body, &prior_snapshot_sha,
                    &error) ||
        prior_snapshot_sha != prior_ref.manifest_sha256 ||
        !parse_snapshot_manifest(prior_snapshot_read.object->body, stream,
                                 prior_ref.manifest_key, &prior_snapshot,
                                 &error) ||
        prior_snapshot.snapshot_id != prior_ref.id ||
        prior_snapshot.cursor != prior_ref.cursor ||
        prior_snapshot.server_identity != cut.server_identity ||
        prior_snapshot.deployment_fingerprints !=
            cut.deployment_fingerprints) {
      return terminal(update_publisher_state ? head_publisher_ : nullptr,
                      PublishOutcome::FENCED,
                      "snapshot identity/fingerprints differ from the exact "
                      "prior snapshot: " +
                          error);
    }
  }

  SnapshotManifest snapshot;
  snapshot.snapshot_id = cut.snapshot_id;
  snapshot.writer = cut.writer;
  snapshot.cursor = cut.proof.public_cursor;
  snapshot.log_anchor = cut.log_anchor;
  snapshot.server_identity = cut.server_identity;
  snapshot.deployment_fingerprints = cut.deployment_fingerprints;
  snapshot.gtid_executed = cut.proof.public_gtid;

  std::string error;
  uint64_t total_payload_bytes = 0;
  PreparedFile seed{cut.binlog_seed_path, {}};
  if (!stream_file_fingerprint(seed.path, &seed.fingerprint, &error) ||
      seed.fingerprint.size != snapshot.cursor.pos ||
      !validate_binlog_seed(seed.path, seed.fingerprint.size, &error)) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  error.empty() ? "binlog seed size does not match cut cursor"
                                : std::move(error));
  }
  if (!account_payload_bytes(seed.fingerprint.size, max_object_bytes_,
                             max_total_payload_bytes_,
                             &total_payload_bytes)) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "binlog seed exceeds snapshot payload limits");
  }
  snapshot.binlog_seed.file = snapshot.cursor.file;
  snapshot.binlog_seed.cursor = snapshot.cursor;
  snapshot.binlog_seed.size = seed.fingerprint.size;
  snapshot.binlog_seed.sha256 = seed.fingerprint.sha256;
  if (!binlog_seed_object_key(stream, snapshot.snapshot_id, snapshot.cursor,
                              snapshot.binlog_seed.sha256,
                              &snapshot.binlog_seed.key, &error)) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "cannot derive binlog seed key: " + error);
  }

  std::vector<LocalSnapshotPayload> sorted_objects = cut.objects;
  std::sort(sorted_objects.begin(), sorted_objects.end(),
            [](const LocalSnapshotPayload &left,
               const LocalSnapshotPayload &right) {
              return std::tie(left.component, left.relative_path) <
                     std::tie(right.component, right.relative_path);
            });
  std::map<std::string, uint64_t> ordinals;
  std::set<std::string> components;
  std::vector<PreparedFile> prepared_objects;
  prepared_objects.reserve(sorted_objects.size());
  snapshot.objects.reserve(sorted_objects.size());
  for (const LocalSnapshotPayload &local : sorted_objects) {
    if (!snapshot.objects.empty() &&
        snapshot.objects.back().component == local.component &&
        snapshot.objects.back().relative_path == local.relative_path) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "duplicate snapshot component/relative path");
    }
    PreparedFile prepared_file{local.local_path, {}};
    if (!stream_file_fingerprint(prepared_file.path,
                                 &prepared_file.fingerprint,
                                 &error)) {
      return result(PublishOutcome::PERMANENT_ERROR, std::move(error));
    }
    if (!account_payload_bytes(prepared_file.fingerprint.size,
                               max_object_bytes_, max_total_payload_bytes_,
                               &total_payload_bytes)) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "snapshot object aggregate exceeds payload limits");
    }
    SnapshotObject object;
    object.component = local.component;
    object.relative_path = local.relative_path;
    object.ordinal = ordinals[object.component]++;
    object.size = prepared_file.fingerprint.size;
    object.sha256 = prepared_file.fingerprint.sha256;
    object.format = local.format;
    if (!snapshot_object_key(stream, snapshot.snapshot_id, object, &object.key,
                             &error)) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "cannot derive snapshot payload key: " + error);
    }
    components.insert(object.component);
    snapshot.objects.push_back(std::move(object));
    prepared_objects.push_back(std::move(prepared_file));
  }
  static const std::set<std::string> kRequiredComponents{
      "innodb", "mysql-dd", "smartengine-meta", "smartengine-wal"};
  if (components != kRequiredComponents) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "snapshot does not contain every required component");
  }

  std::vector<ExtentCandidate> extents;
  extents.reserve(cut.smartengine_extents.size());
  for (const PinnedSmartengineExtent &input : cut.smartengine_extents) {
    if (!account_payload_bytes(input.size, max_object_bytes_,
                               max_total_payload_bytes_,
                               &total_payload_bytes)) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "SmartEngine extent aggregate exceeds payload limits");
    }
    ExtentCandidate candidate;
    candidate.ref.writer_epoch = input.writer_epoch;
    candidate.ref.allocation_seq = input.allocation_seq;
    candidate.ref.database_name_hex = input.database_name_hex;
    candidate.ref.index_id = input.index_id;
    candidate.ref.object_id = input.object_id;
    candidate.ref.key = input.key;
    candidate.ref.size = input.size;
    candidate.ref.sha256 = input.sha256;
    if (!parse_decimal(candidate.ref.allocation_seq, &candidate.allocation) ||
        !parse_decimal(candidate.ref.index_id, &candidate.index) ||
        !parse_decimal(candidate.ref.object_id, &candidate.object)) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "SmartEngine extent identity is not canonical decimal");
    }
    extents.push_back(std::move(candidate));
  }
  std::sort(extents.begin(), extents.end(),
            [](const ExtentCandidate &left, const ExtentCandidate &right) {
              return std::tie(left.ref.writer_epoch, left.allocation,
                              left.ref.database_name_hex, left.index,
                              left.object, left.ref.key) <
                     std::tie(right.ref.writer_epoch, right.allocation,
                              right.ref.database_name_hex, right.index,
                              right.object, right.ref.key);
            });
  snapshot.smartengine_extents.reserve(extents.size());
  for (size_t index = 0; index < extents.size(); ++index) {
    SmartengineExtentRef &extent = extents[index].ref;
    extent.ordinal = index;
    std::string expected_key;
    if (!smartengine_extent_object_key(stream, extent, &expected_key, &error) ||
        expected_key != extent.key) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "SmartEngine extent key is not canonical: " + error);
    }
    snapshot.smartengine_extents.push_back(extent);
  }

  std::string snapshot_body;
  if (!serialize_snapshot_manifest(stream, snapshot, &snapshot_body, &error)) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "cannot build strict snapshot manifest: " + error);
  }
  std::string snapshot_sha;
  std::string snapshot_key;
  if (!sha256_hex(snapshot_body, &snapshot_sha, &error) ||
      !snapshot_manifest_key(stream, snapshot.snapshot_id, snapshot_sha,
                             &snapshot_key, &error)) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "cannot bind snapshot manifest bytes: " + error);
  }

  for (size_t index = 0; index < snapshot.objects.size(); ++index) {
    const PublishResult uploaded = create_and_verify(
        payload_io_, snapshot.objects[index].key, prepared_objects[index].path,
        prepared_objects[index].fingerprint, logical_attempt_limit_);
    if (!uploaded.applied())
      return terminal(update_publisher_state ? head_publisher_ : nullptr,
                      uploaded.outcome, uploaded.detail);
  }
  PublishResult uploaded = create_and_verify(
      payload_io_, snapshot.binlog_seed.key, seed.path, seed.fingerprint,
      logical_attempt_limit_);
  if (!uploaded.applied())
    return terminal(update_publisher_state ? head_publisher_ : nullptr,
                    uploaded.outcome, uploaded.detail);
  for (const SmartengineExtentRef &extent : snapshot.smartengine_extents) {
    uploaded = verify_readback(
        payload_io_, extent.key, PayloadFingerprint{extent.size, extent.sha256});
    if (uploaded.outcome == PublishOutcome::ABSENT) {
      uploaded = result(PublishOutcome::FENCED,
                        "pinned SmartEngine extent is absent: " + extent.key);
    }
    if (!uploaded.applied())
      return terminal(update_publisher_state ? head_publisher_ : nullptr,
                      uploaded.outcome, uploaded.detail);
  }

  const PublishResult manifest_write = create_or_verify_snapshot_manifest(
      &metadata_store_, snapshot_key, snapshot_body);
  if (!manifest_write.applied()) {
    return terminal(update_publisher_state ? head_publisher_ : nullptr,
                    manifest_write.outcome,
                    manifest_write.detail);
  }

  SnapshotRef snapshot_ref;
  snapshot_ref.id = snapshot.snapshot_id;
  snapshot_ref.manifest_key = snapshot_key;
  snapshot_ref.manifest_size = snapshot_body.size();
  snapshot_ref.manifest_sha256 = snapshot_sha;
  snapshot_ref.cursor = snapshot.cursor;

  PreparedSnapshotPublication completed;
  completed.proof = cut.proof;
  completed.writer = cut.writer;
  completed.snapshot_manifest = std::move(snapshot);
  completed.snapshot_ref = std::move(snapshot_ref);
  if (initial_state.head.has_value())
    completed.source_snapshot = initial_state.head->snapshot;
  *prepared = std::move(completed);
  return manifest_write;
}

namespace {

bool checked_window_add(uint64_t value, uint64_t addend, uint64_t limit,
                        uint64_t *sum) {
  if (value > limit || addend > limit - value) return false;
  *sum = value + addend;
  return true;
}

bool same_object_ref(const ObjectRef &left, const ObjectRef &right) {
  return left == right;
}

ObjectRef manifest_object(const ManifestRef &reference) {
  return {reference.key, reference.size, reference.sha256};
}

PublishResult verify_prepared_snapshot(
    ProtocolStore *store, const StreamIdentity &stream,
    const PreparedSnapshotPublication &prepared) {
  const SnapshotManifest &snapshot = prepared.snapshot_manifest;
  const SnapshotRef &reference = prepared.snapshot_ref;
  if (snapshot.writer != prepared.writer ||
      snapshot.cursor != prepared.proof.public_cursor ||
      snapshot.gtid_executed != prepared.proof.public_gtid ||
      snapshot.snapshot_id != reference.id ||
      snapshot.cursor != reference.cursor) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "prepared snapshot identity differs from its fixed cut");
  }
  if (prepared.proof.public_cursor != prepared.proof.image_cursor ||
      prepared.proof.public_gtid != prepared.proof.image_gtid) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "prepared snapshot contains a split engine cut");
  }
  if (prepared.proof.source == FixedCutSource::CLONE_BARRIER) {
    if (!prepared.proof.clone_handle_id.has_value() ||
        *prepared.proof.clone_handle_id == 0 ||
        !prepared.proof.redo_range_sha256.has_value() ||
        !is_sha256(*prepared.proof.redo_range_sha256)) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "prepared clone cut lacks handle/redo identity");
    }
    if (!prepared.source_snapshot.has_value()) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "prepared clone cut lost its source snapshot identity");
    }
  } else if (prepared.proof.clone_handle_id.has_value() ||
             prepared.proof.redo_range_sha256.has_value()) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "prepared non-clone cut contains clone identity");
  }
  if (prepared.proof.source == FixedCutSource::EMPTY_SOURCE &&
      prepared.source_snapshot.has_value()) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "prepared bootstrap unexpectedly has a source snapshot");
  }

  std::string body;
  std::string body_sha;
  std::string key;
  std::string error;
  if (!serialize_snapshot_manifest(stream, snapshot, &body, &error) ||
      !sha256_hex(body, &body_sha, &error) ||
      !snapshot_manifest_key(stream, snapshot.snapshot_id, body_sha, &key,
                             &error) ||
      reference.manifest_key != key || reference.manifest_size != body.size() ||
      reference.manifest_sha256 != body_sha) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "prepared snapshot manifest binding is invalid: " + error);
  }

  const PublishResult readback = store->read(reference.manifest_key);
  if (!readback.applied() || !readback.object.has_value()) {
    return result(readback.outcome == PublishOutcome::ABSENT
                      ? PublishOutcome::FENCED
                      : readback.outcome,
                  "prepared snapshot manifest is not exact-readable: " +
                      readback.detail);
  }
  if (readback.object->body != body) {
    return result(PublishOutcome::FENCED,
                  "prepared snapshot manifest read-back changed");
  }
  return result(PublishOutcome::APPLIED);
}

PublishResult collect_retained_suffix(
    ProtocolStore *store, const StreamIdentity &stream, const Head &current,
    const PreparedSnapshotPublication &prepared, RecoveryWindow *window) {
  if (window == nullptr ||
      prepared.snapshot_manifest.log_anchor.kind !=
          LogAnchorKind::MANIFEST_BOUNDARY ||
      !prepared.snapshot_manifest.log_anchor.generation.has_value() ||
      !prepared.snapshot_manifest.log_anchor.manifest.has_value()) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "prepared runtime snapshot has no exact manifest anchor");
  }
  const LogAnchor &anchor = prepared.snapshot_manifest.log_anchor;
  if (*anchor.generation != prepared.proof.source_head_generation ||
      anchor.cursor != prepared.proof.public_cursor ||
      current.generation < *anchor.generation) {
    return result(PublishOutcome::FENCED,
                  "prepared snapshot anchor is not covered by current HEAD");
  }

  RecoveryWindow retained;
  ManifestRef next{current.generation, current.manifest.key,
                   current.manifest.size, current.manifest.sha256};
  const uint64_t walk_limit =
      std::min<uint64_t>(current.recovery_window.manifest_count,
                         kRecoveryManifestCountMax);
  bool first = true;
  for (uint64_t traversed = 0; traversed < walk_limit; ++traversed) {
    const PublishResult read = store->read(next.key);
    if (!read.applied() || !read.object.has_value()) {
      return result(read.outcome == PublishOutcome::ABSENT
                        ? PublishOutcome::FENCED
                        : read.outcome,
                    "cannot exact-read retained snapshot suffix: " +
                        read.detail);
    }

    std::string body_sha;
    std::string error;
    TransitionManifest manifest;
    if (read.object->body.size() != next.size ||
        !sha256_hex(read.object->body, &body_sha, &error) ||
        body_sha != next.sha256 ||
        !parse_transition_manifest(read.object->body, stream, next.key,
                                   &manifest, &error) ||
        manifest.generation != next.generation) {
      return result(PublishOutcome::FENCED,
                    "invalid retained snapshot suffix: " + error);
    }
    if (first &&
        (manifest.recovery_window != current.recovery_window ||
         manifest.segment_tip != current.segment_tip ||
         manifest.snapshot != current.snapshot ||
         manifest.base_cursor != current.base_cursor ||
         manifest.durable_cursor != current.durable_cursor)) {
      return result(PublishOutcome::FENCED,
                    "current HEAD differs from its exact manifest");
    }
    first = false;

    if (!checked_window_add(retained.manifest_count, 1,
                            kRecoveryManifestCountMax,
                            &retained.manifest_count) ||
        !checked_window_add(retained.manifest_bytes,
                            read.object->body.size(),
                            kRecoveryManifestBytesMax,
                            &retained.manifest_bytes)) {
      return result(PublishOutcome::REFIX_REQUIRED,
                    "fixed snapshot cut aged past a recovery-window limit");
    }
    for (const SegmentRef &segment : manifest.segments) {
      const Cursor start{segment.source.file, segment.source.start_pos};
      const Cursor end{segment.source.file, segment.source.end_pos};
      int start_order = 0;
      int end_order = 0;
      if (!compare_cursor(start, prepared.proof.public_cursor, &start_order) ||
          !compare_cursor(end, prepared.proof.public_cursor, &end_order)) {
        return result(PublishOutcome::FENCED,
                      "retained segment uses a different binlog stream");
      }
      if (end_order <= 0) continue;
      if (start_order < 0) {
        return result(PublishOutcome::FENCED,
                      "prepared snapshot cursor falls inside a segment");
      }
      if (!checked_window_add(retained.segment_count, 1,
                              kRecoverySegmentCountMax,
                              &retained.segment_count)) {
        return result(PublishOutcome::REFIX_REQUIRED,
                      "fixed snapshot cut aged past the replay-segment limit");
      }
    }

    if (next.generation == *anchor.generation) {
      if (!same_object_ref(manifest_object(next), *anchor.manifest) ||
          manifest.durable_cursor != anchor.cursor) {
        return result(PublishOutcome::FENCED,
                      "prepared snapshot anchor is not the exact cut boundary");
      }
      *window = retained;
      return result(PublishOutcome::APPLIED);
    }
    if (next.generation < *anchor.generation ||
        !manifest.previous.has_value()) {
      break;
    }
    next = *manifest.previous;
  }
  return result(PublishOutcome::REFIX_REQUIRED,
                "prepared snapshot anchor is outside the retained suffix");
}

}  // namespace

PublishResult SnapshotPublisher::publish_prepared(
    const PreparedSnapshotPublication &prepared,
    SnapshotPublication *publication) {
  if (publication == nullptr || metadata_io_ == nullptr ||
      head_publisher_ == nullptr || logical_attempt_limit_ == 0) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "snapshot publisher is not initialized");
  }
  const StreamIdentity &stream = head_publisher_->stream();
  const PublisherState publish_state = head_publisher_->state();
  if (!publish_state.epoch.has_value() ||
      publish_state.epoch->epoch != prepared.writer.epoch ||
      publish_state.epoch->writer_id != prepared.writer.id) {
    return terminal(head_publisher_, PublishOutcome::FENCED,
                    "writer epoch changed while preparing snapshot objects");
  }
  PublishResult verified =
      verify_prepared_snapshot(&metadata_store_, stream, prepared);
  if (!verified.applied())
    return terminal(head_publisher_, verified.outcome, verified.detail);

  TransitionManifest transition;
  Head head;
  uint64_t base_manifest_bytes = 0;
  if (prepared.proof.source == FixedCutSource::EMPTY_SOURCE) {
    if (publish_state.head.has_value() || publish_state.head_object.has_value() ||
        prepared.snapshot_manifest.log_anchor.kind !=
            LogAnchorKind::EMPTY_BASE ||
        prepared.proof.source_head_generation != 0 ||
        !prepared.proof.source_head_body_sha256.empty()) {
      return terminal(head_publisher_, PublishOutcome::FENCED,
                      "HEAD appeared before prepared bootstrap publication");
    }
    transition.kind = ManifestKind::BOOTSTRAP;
    transition.generation = 1;
    transition.recovery_window = {1, 0, 0};
    transition.segment_tip = snapshot_root(prepared.snapshot_ref);
    transition.durable_cursor = prepared.snapshot_ref.cursor;
  } else {
    if (!publish_state.head.has_value() ||
        !publish_state.head_object.has_value() ||
        publish_state.head->generation >= kJsonSafeIntegerMax) {
      return terminal(head_publisher_, PublishOutcome::FENCED,
                      "HEAD is not publishable for the prepared snapshot");
    }
    int low = 0;
    int high = 0;
    if (!compare_cursor(publish_state.head->base_cursor,
                        prepared.snapshot_ref.cursor, &low) ||
        !compare_cursor(prepared.snapshot_ref.cursor,
                        publish_state.head->durable_cursor, &high) ||
        high > 0) {
      return terminal(head_publisher_, PublishOutcome::FENCED,
                      "latest HEAD does not cover the prepared snapshot cursor");
    }
    if (low > 0 || !prepared.source_snapshot.has_value() ||
        publish_state.head->snapshot != *prepared.source_snapshot) {
      return result(PublishOutcome::REFIX_REQUIRED,
                    "a later same-writer snapshot superseded the fixed cut");
    }

    RecoveryWindow base_window;
    verified = collect_retained_suffix(&metadata_store_, stream,
                                       *publish_state.head, prepared,
                                       &base_window);
    if (!verified.applied())
      return terminal(head_publisher_, verified.outcome, verified.detail);
    if (base_window.manifest_count >= kRecoveryManifestCountMax) {
      return result(PublishOutcome::REFIX_REQUIRED,
                    "fixed snapshot cut cannot fit a snapshot transition");
    }

    std::string prior_sha;
    std::string error;
    if (!sha256_hex(publish_state.head_object->body, &prior_sha, &error)) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "cannot hash prior HEAD: " + error);
    }
    const HeadParent parent{publish_state.head->generation,
                            publish_state.head_object->etag, prior_sha};
    transition.kind = ManifestKind::SNAPSHOT;
    transition.generation = publish_state.head->generation + 1;
    transition.head_parent = parent;
    transition.previous = ManifestRef{
        publish_state.head->generation, publish_state.head->manifest.key,
        publish_state.head->manifest.size,
        publish_state.head->manifest.sha256};
    transition.recovery_window = base_window;
    ++transition.recovery_window.manifest_count;
    transition.segment_tip = publish_state.head->segment_tip;
    transition.durable_cursor = publish_state.head->durable_cursor;
    base_manifest_bytes = base_window.manifest_bytes;
    head.parent = parent;
  }
  transition.writer = prepared.writer;
  transition.snapshot = prepared.snapshot_ref;
  transition.base_cursor = prepared.snapshot_ref.cursor;

  std::string transition_body;
  std::string error;
  if (prepared.proof.source != FixedCutSource::EMPTY_SOURCE) {
    TransitionManifest capacity_probe = transition;
    capacity_probe.recovery_window.manifest_bytes =
        kRecoveryManifestBytesMax;
    std::string capacity_body;
    if (!serialize_transition_manifest(stream, capacity_probe, &capacity_body,
                                       &error)) {
      return result(PublishOutcome::PERMANENT_ERROR,
                    "cannot size snapshot transition: " + error);
    }
    if (capacity_body.size() >
        kRecoveryManifestBytesMax - base_manifest_bytes) {
      return result(PublishOutcome::REFIX_REQUIRED,
                    "fixed snapshot cut cannot fit the snapshot manifest");
    }
  }
  if (!stabilize_transition_manifest(stream, base_manifest_bytes, &transition,
                                     &transition_body, &error)) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "cannot stabilize snapshot transition: " + error);
  }
  std::string transition_sha;
  std::string transition_key;
  if (!sha256_hex(transition_body, &transition_sha, &error) ||
      !transition_manifest_key(stream, prepared.writer, transition.generation,
                               transition_sha, &transition_key, &error)) {
    return result(PublishOutcome::PERMANENT_ERROR,
                  "cannot bind snapshot transition bytes: " + error);
  }

  head.generation = transition.generation;
  head.writer = transition.writer;
  head.manifest = {transition_key, transition_body.size(), transition_sha};
  head.recovery_window = transition.recovery_window;
  head.segment_tip = transition.segment_tip;
  head.snapshot = transition.snapshot;
  head.base_cursor = transition.base_cursor;
  head.durable_cursor = transition.durable_cursor;

  const PublishResult published = head_publisher_->publish(transition, head);
  if (!published.applied()) return published;

  SnapshotPublication completed;
  completed.snapshot_manifest = prepared.snapshot_manifest;
  completed.snapshot_ref = prepared.snapshot_ref;
  completed.transition = std::move(transition);
  completed.head = std::move(head);
  *publication = std::move(completed);
  return published;
}

PublishResult SnapshotPublisher::publish(const FixedSnapshotCut &cut,
                                          SnapshotPublication *publication) {
  PreparedSnapshotPublication prepared;
  const PublishResult prepared_result = prepare(cut, &prepared);
  if (!prepared_result.applied()) return prepared_result;
  return publish_prepared(prepared, publication);
}

}  // namespace wesql::remote_commit
