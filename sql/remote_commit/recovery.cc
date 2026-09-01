/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/recovery.h"

#include <algorithm>
#include <charconv>
#include <limits>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace wesql::remote_commit {
namespace {

RecoveryReadResult result(RecoveryReadOutcome outcome,
                          std::string detail = {}) {
  return {outcome, std::move(detail)};
}

bool parse_binlog_file(std::string_view file, std::string_view *basename,
                       uint64_t *sequence) {
  const size_t dot = file.rfind('.');
  if (dot == std::string_view::npos || dot == 0 || dot + 1 == file.size())
    return false;
  uint64_t parsed = 0;
  const auto converted = std::from_chars(file.data() + dot + 1,
                                         file.data() + file.size(), parsed);
  if (converted.ec != std::errc() || converted.ptr != file.data() + file.size())
    return false;
  *basename = file.substr(0, dot);
  *sequence = parsed;
  return true;
}

int compare_cursor(const Cursor &left, const Cursor &right, bool *valid) {
  std::string_view left_base;
  std::string_view right_base;
  uint64_t left_sequence = 0;
  uint64_t right_sequence = 0;
  if (!parse_binlog_file(left.file, &left_base, &left_sequence) ||
      !parse_binlog_file(right.file, &right_base, &right_sequence) ||
      left_base != right_base) {
    *valid = false;
    return 0;
  }
  *valid = true;
  if (left_sequence != right_sequence)
    return left_sequence < right_sequence ? -1 : 1;
  if (left.pos != right.pos) return left.pos < right.pos ? -1 : 1;
  return 0;
}

bool object_matches(const ObjectRef &expected, std::string_view body,
                    std::string *error) {
  if (body.size() != expected.size) {
    *error = "object size does not match its immutable reference";
    return false;
  }
  std::string sha;
  if (!sha256_hex(body, &sha, error)) return false;
  if (sha != expected.sha256) {
    *error = "object SHA-256 does not match its immutable reference";
    return false;
  }
  return true;
}

bool cursor_starts_at_or_after(const Cursor &prior, const Cursor &next,
                               bool *contiguous) {
  std::string_view prior_base;
  std::string_view next_base;
  uint64_t prior_sequence = 0;
  uint64_t next_sequence = 0;
  if (!parse_binlog_file(prior.file, &prior_base, &prior_sequence) ||
      !parse_binlog_file(next.file, &next_base, &next_sequence) ||
      prior_base != next_base) {
    *contiguous = false;
    return false;
  }
  *contiguous =
      (prior_sequence == next_sequence && prior.pos == next.pos) ||
      (prior_sequence != std::numeric_limits<uint64_t>::max() &&
       next_sequence == prior_sequence + 1 && next.pos == 0);
  return true;
}

ObjectRef as_object(const ManifestRef &ref) {
  return {ref.key, ref.size, ref.sha256};
}

ManifestRef as_manifest_ref(const VerifiedManifest &manifest) {
  return {manifest.value.generation, manifest.object.key,
          manifest.object.size, manifest.object.sha256};
}

SegmentTip as_tip(const SegmentRef &segment) {
  SegmentTip tip;
  tip.kind = SegmentTipKind::SEGMENT;
  tip.key = segment.key;
  tip.size = segment.size;
  tip.sha256 = segment.sha256;
  tip.sequence = segment.sequence;
  return tip;
}

bool snapshot_bound_to_ref(const SnapshotRef &ref,
                           const SnapshotManifest &snapshot,
                           std::string_view body, std::string *error) {
  if (snapshot.snapshot_id != ref.id || snapshot.cursor != ref.cursor ||
      snapshot.binlog_seed.cursor != ref.cursor ||
      snapshot.binlog_seed.file != ref.cursor.file ||
      body.size() != ref.manifest_size) {
    *error = "snapshot body is not bound to the HEAD snapshot reference";
    return false;
  }
  std::string sha;
  if (!sha256_hex(body, &sha, error)) return false;
  if (sha != ref.manifest_sha256) {
    *error = "snapshot manifest SHA-256 does not match HEAD";
    return false;
  }
  return true;
}

bool snapshot_has_required_components(const SnapshotManifest &snapshot,
                                      std::string *error) {
  bool innodb = false;
  bool mysql_dd = false;
  bool smartengine_meta = false;
  bool smartengine_wal = false;
  for (const SnapshotObject &object : snapshot.objects) {
    innodb = innodb || object.component == "innodb";
    mysql_dd = mysql_dd || object.component == "mysql-dd";
    smartengine_meta =
        smartengine_meta || object.component == "smartengine-meta";
    smartengine_wal =
        smartengine_wal || object.component == "smartengine-wal";
  }
  if (!innodb || !mysql_dd || !smartengine_meta || !smartengine_wal) {
    *error = "snapshot is missing a required materialized component";
    return false;
  }
  return true;
}

bool reconstructed_head_sha(const StreamIdentity &stream,
                            const VerifiedManifest &manifest,
                            std::string *sha, std::string *error) {
  Head reconstructed;
  reconstructed.generation = manifest.value.generation;
  reconstructed.writer = manifest.value.writer;
  reconstructed.parent = manifest.value.head_parent;
  reconstructed.manifest = manifest.object;
  reconstructed.recovery_window = manifest.value.recovery_window;
  reconstructed.segment_tip = manifest.value.segment_tip;
  reconstructed.base_cursor = manifest.value.base_cursor;
  reconstructed.durable_cursor = manifest.value.durable_cursor;
  reconstructed.snapshot = manifest.value.snapshot;
  std::string body;
  return serialize_head(stream, reconstructed, &body, error) &&
         sha256_hex(body, sha, error);
}

bool prior_relation(const StreamIdentity &stream,
                    const VerifiedManifest &newer_manifest,
                    const VerifiedManifest &older, std::string *error) {
  const TransitionManifest &newer = newer_manifest.value;
  if (!newer.previous.has_value() ||
      *newer.previous != as_manifest_ref(older) ||
      newer.generation != older.value.generation + 1) {
    *error = "manifest previous link does not name the exact prior object";
    return false;
  }

  std::string prior_head_sha;
  if (!newer.head_parent.has_value() ||
      newer.head_parent->generation != older.value.generation ||
      !reconstructed_head_sha(stream, older, &prior_head_sha, error) ||
      newer.head_parent->sha256 != prior_head_sha) {
    *error = "manifest head_parent does not bind the reconstructed prior HEAD";
    return false;
  }

  bool valid = false;
  if (newer.kind == ManifestKind::LOG_TRANSITION) {
    const Cursor first_start{newer.segments.front().source.file,
                             newer.segments.front().source.start_pos};
    bool contiguous = false;
    if (newer.writer != older.value.writer ||
        newer.snapshot != older.value.snapshot ||
        newer.base_cursor != older.value.base_cursor ||
        newer.segments.empty() ||
        newer.segments.front().previous_segment != older.value.segment_tip ||
        !cursor_starts_at_or_after(older.value.durable_cursor, first_start,
                                   &contiguous) ||
        !contiguous ||
        compare_cursor(older.value.durable_cursor, newer.durable_cursor,
                       &valid) >= 0 ||
        !valid || newer.segment_tip != as_tip(newer.segments.back())) {
      *error = "LOG transition rewrites prior state or is not contiguous";
      return false;
    }
    return true;
  }

  if (newer.kind == ManifestKind::SNAPSHOT) {
    const int low = compare_cursor(older.value.base_cursor,
                                   newer.snapshot.cursor, &valid);
    if (!valid || low > 0) {
      *error = "SNAPSHOT cursor is before the prior base cursor";
      return false;
    }
    const int high = compare_cursor(newer.snapshot.cursor,
                                    older.value.durable_cursor, &valid);
    if (!valid || high > 0 || !newer.segments.empty() ||
        newer.durable_cursor != older.value.durable_cursor ||
        newer.segment_tip != older.value.segment_tip ||
        newer.base_cursor != newer.snapshot.cursor) {
      *error = "SNAPSHOT transition changes durable state or has an invalid cut";
      return false;
    }
    return true;
  }

  *error = "BOOTSTRAP is not the terminal manifest";
  return false;
}

bool head_matches_top(const Head &head, const VerifiedManifest &top,
                      std::string *error) {
  const TransitionManifest &manifest = top.value;
  if (head.generation != manifest.generation ||
      head.writer != manifest.writer || head.manifest != top.object ||
      head.recovery_window != manifest.recovery_window ||
      head.segment_tip != manifest.segment_tip ||
      head.snapshot != manifest.snapshot ||
      head.base_cursor != manifest.base_cursor ||
      head.durable_cursor != manifest.durable_cursor ||
      head.parent != manifest.head_parent) {
    *error = "HEAD fields diverge from its exact transition manifest";
    return false;
  }
  return true;
}

bool checked_add(uint64_t value, uint64_t addend, uint64_t limit,
                 uint64_t *result) {
  if (value > limit || addend > limit - value) return false;
  *result = value + addend;
  return true;
}

bool collect_replay_and_verify_windows(
    const SnapshotManifest &snapshot,
    const std::vector<VerifiedManifest> &manifests, size_t introduction,
    size_t anchor, std::vector<SegmentRef> *segments, std::string *error) {
  segments->clear();
  if (introduction > anchor || anchor >= manifests.size()) {
    *error = "invalid introduction/anchor range while building replay";
    return false;
  }

  uint64_t manifest_count = 0;
  uint64_t manifest_bytes = 0;
  uint64_t segment_count = 0;
  Cursor expected = snapshot.cursor;
  SegmentTip prior = manifests[anchor].value.segment_tip;

  for (size_t index = anchor + 1; index-- > 0;) {
    if (!checked_add(manifest_count, 1, kRecoveryManifestCountMax,
                     &manifest_count) ||
        !checked_add(manifest_bytes, manifests[index].body_size,
                     kRecoveryManifestBytesMax, &manifest_bytes)) {
      *error = "retained manifest counters exceed a hard limit";
      return false;
    }
    for (const SegmentRef &segment : manifests[index].value.segments) {
      bool valid = false;
      const Cursor start{segment.source.file, segment.source.start_pos};
      const Cursor end{segment.source.file, segment.source.end_pos};
      const int end_order = compare_cursor(end, snapshot.cursor, &valid);
      if (!valid) {
        *error = "segment and snapshot cursor use different binlog streams";
        return false;
      }
      if (end_order <= 0) continue;
      const int start_order = compare_cursor(start, snapshot.cursor, &valid);
      if (!valid || start_order < 0) {
        *error = "snapshot cursor falls inside a segment";
        return false;
      }
      bool contiguous = false;
      if (!cursor_starts_at_or_after(expected, start, &contiguous) ||
          !contiguous) {
        *error = "replay segments contain a cursor gap or overlap";
        return false;
      }
      if (segment.previous_segment != prior) {
        *error = "replay segment physical chain is discontinuous";
        return false;
      }
      if (!checked_add(segment_count, 1, kRecoverySegmentCountMax,
                       &segment_count)) {
        *error = "replay segment count exceeds the hard limit";
        return false;
      }
      segments->push_back(segment);
      expected = end;
      prior = as_tip(segment);
    }

    if (index <= introduction) {
      bool valid = false;
      if (compare_cursor(expected, manifests[index].value.durable_cursor,
                         &valid) != 0 ||
          !valid) {
        *error = "replay does not end at a retained manifest durable cursor";
        return false;
      }
      const RecoveryWindow recomputed{manifest_count, manifest_bytes,
                                      segment_count};
      if (recomputed != manifests[index].value.recovery_window) {
        *error =
            "manifest recovery_window does not match its retained suffix";
        return false;
      }
    }
  }
  return true;
}

}  // namespace

RecoveryReadResult RecoveryChainReader::corrupt(std::string detail) const {
  return result(RecoveryReadOutcome::CORRUPT, std::move(detail));
}

RecoveryReadResult RecoveryChainReader::read_required(
    std::string_view key, size_t max_bytes, PublishedBytes *object) {
  PublishResult read = store_.read(key, max_bytes);
  if (read.outcome == PublishOutcome::BLOCKED)
    return result(RecoveryReadOutcome::BLOCKED, read.detail);
  if (read.outcome != PublishOutcome::APPLIED || !read.object.has_value())
    return corrupt("required immutable object is absent or unreadable: " +
                   std::string(key) + ": " + read.detail);
  if (read.object->body.size() > max_bytes)
    return corrupt("object exceeds its format hard limit: " + std::string(key));
  *object = std::move(*read.object);
  return result(RecoveryReadOutcome::READY);
}

RecoveryReadResult RecoveryChainReader::read_snapshot(
    const SnapshotRef &ref, PublishedBytes *object,
    SnapshotManifest *snapshot) {
  RecoveryReadResult read =
      read_required(ref.manifest_key, kSnapshotManifestMaxBytes, object);
  if (!read.ready()) return read;
  std::string error;
  if (!parse_snapshot_manifest(object->body, stream_, ref.manifest_key,
                               snapshot, &error) ||
      !snapshot_bound_to_ref(ref, *snapshot, object->body, &error) ||
      !snapshot_has_required_components(*snapshot, &error)) {
    return corrupt("invalid snapshot manifest: " + error);
  }
  return result(RecoveryReadOutcome::READY);
}

RecoveryReadResult RecoveryChainReader::read(RecoveryPlan *plan) {
  if (plan == nullptr) return corrupt("null recovery plan result");
  *plan = {};

  const std::string head_key = stream_.remote_prefix + "/HEAD";
  PublishResult head_read = store_.read(head_key, kHeadMaxBytes);
  if (head_read.outcome == PublishOutcome::ABSENT) {
    PublishResult epoch_read = store_.read(
        stream_.remote_prefix + "/WRITER_EPOCH", kWriterEpochMaxBytes);
    if (epoch_read.outcome == PublishOutcome::BLOCKED)
      return result(RecoveryReadOutcome::BLOCKED, epoch_read.detail);
    if (epoch_read.outcome != PublishOutcome::APPLIED &&
        epoch_read.outcome != PublishOutcome::ABSENT)
      return corrupt("WRITER_EPOCH is unreadable while HEAD is absent");
    return result(RecoveryReadOutcome::EMPTY);
  }
  if (head_read.outcome == PublishOutcome::BLOCKED)
    return result(RecoveryReadOutcome::BLOCKED, head_read.detail);
  if (head_read.outcome != PublishOutcome::APPLIED ||
      !head_read.object.has_value())
    return corrupt("HEAD is unreadable: " + head_read.detail);

  plan->head_object = std::move(*head_read.object);
  std::string error;
  if (!parse_head(plan->head_object.body, stream_, &plan->head, &error))
    return corrupt("invalid HEAD: " + error);

  RecoveryReadResult epoch_result = read_required(
      stream_.remote_prefix + "/WRITER_EPOCH", kWriterEpochMaxBytes,
      &plan->epoch_object);
  if (!epoch_result.ready()) return epoch_result;
  if (!parse_writer_epoch(plan->epoch_object.body, stream_, &plan->epoch,
                          &error))
    return corrupt("invalid WRITER_EPOCH: " + error);
  if (plan->head.writer.epoch > plan->epoch.epoch ||
      (plan->head.writer.epoch == plan->epoch.epoch &&
       plan->head.writer.id != plan->epoch.writer_id))
    return corrupt("HEAD and WRITER_EPOCH have an invalid ownership relation");

  RecoveryReadResult snapshot_result = read_snapshot(
      plan->head.snapshot, &plan->snapshot_object, &plan->snapshot);
  if (!snapshot_result.ready()) return snapshot_result;
  if (plan->snapshot.writer != plan->head.writer)
    return corrupt("HEAD writer does not match its current snapshot writer");

  ObjectRef next = plan->head.manifest;
  std::unordered_set<std::string> seen;
  bool found_introduction = false;
  bool found_anchor = false;
  size_t introduction_index = 0;
  size_t anchor_index = 0;
  uint64_t manifest_bytes = 0;

  while (true) {
    if (plan->manifests.size() >= kRecoveryManifestCountMax)
      return corrupt("manifest chain exceeds the hard count limit");
    if (!seen.insert(next.key).second)
      return corrupt("manifest chain contains a cycle or duplicate key");

    PublishedBytes body;
    RecoveryReadResult object_result =
        read_required(next.key, kDeltaManifestMaxBytes, &body);
    if (!object_result.ready()) return object_result;
    if (!object_matches(next, body.body, &error))
      return corrupt("invalid manifest object: " + error);

    VerifiedManifest verified;
    verified.object = next;
    verified.body_size = body.body.size();
    if (!parse_transition_manifest(body.body, stream_, next.key,
                                   &verified.value, &error))
      return corrupt("invalid transition manifest: " + error);
    if (plan->manifests.size() >= plan->head.generation ||
        verified.value.generation !=
            plan->head.generation - plan->manifests.size())
      return corrupt("manifest generations are not strictly consecutive");

    if (manifest_bytes > kRecoveryManifestBytesMax - verified.body_size)
      return corrupt("manifest chain exceeds the hard byte limit");
    manifest_bytes += verified.body_size;
    plan->manifests.push_back(std::move(verified));

    if (plan->manifests.size() == 1 &&
        !head_matches_top(plan->head, plan->manifests.front(), &error))
      return corrupt(error);
    if (plan->manifests.size() > 1) {
      const TransitionManifest &newer =
          plan->manifests[plan->manifests.size() - 2].value;
      const VerifiedManifest &older = plan->manifests.back();
      if (!prior_relation(stream_,
                          plan->manifests[plan->manifests.size() - 2], older,
                          &error))
        return corrupt(error);
      if (newer.kind == ManifestKind::SNAPSHOT) {
        PublishedBytes prior_snapshot_object;
        SnapshotManifest prior_snapshot;
        RecoveryReadResult prior_snapshot_result = read_snapshot(
            older.value.snapshot, &prior_snapshot_object, &prior_snapshot);
        if (!prior_snapshot_result.ready()) return prior_snapshot_result;
        if (prior_snapshot.server_identity != plan->snapshot.server_identity ||
            prior_snapshot.deployment_fingerprints !=
                plan->snapshot.deployment_fingerprints) {
          return corrupt(
              "SNAPSHOT transition changes server identity or deployment "
              "fingerprints");
        }
      }
    }

    const VerifiedManifest &current = plan->manifests.back();
    const bool introduces =
        (current.value.kind == ManifestKind::BOOTSTRAP ||
         current.value.kind == ManifestKind::SNAPSHOT) &&
        current.value.snapshot == plan->head.snapshot;
    if (introduces) {
      if (found_introduction)
        return corrupt("current snapshot has multiple introducing transitions");
      found_introduction = true;
      introduction_index = plan->manifests.size() - 1;
      if (current.value.writer != plan->snapshot.writer)
        return corrupt("snapshot writer differs from its introducing transition");
      if (plan->snapshot.log_anchor.kind == LogAnchorKind::EMPTY_BASE) {
        if (current.value.kind != ManifestKind::BOOTSTRAP ||
            current.value.generation != 1)
          return corrupt("EMPTY_BASE snapshot was not introduced by BOOTSTRAP");
        found_anchor = true;
        anchor_index = introduction_index;
      }
    }

    if (found_introduction &&
        plan->snapshot.log_anchor.kind == LogAnchorKind::MANIFEST_BOUNDARY &&
        plan->snapshot.log_anchor.generation.has_value() &&
        plan->snapshot.log_anchor.manifest.has_value() &&
        current.value.generation == *plan->snapshot.log_anchor.generation &&
        current.object == *plan->snapshot.log_anchor.manifest) {
      if (found_anchor)
        return corrupt("snapshot anchor appears more than once in the chain");
      if (current.value.durable_cursor != plan->snapshot.cursor)
        return corrupt("snapshot anchor cursor differs from anchor manifest");
      found_anchor = true;
      anchor_index = plan->manifests.size() - 1;
    }

    if (found_introduction && found_anchor) break;
    if (!current.value.previous.has_value())
      return corrupt("manifest chain ended before snapshot introduction/anchor");
    next = as_object(*current.value.previous);
  }

  if (plan->snapshot.log_anchor.kind == LogAnchorKind::MANIFEST_BOUNDARY &&
      plan->manifests[introduction_index].value.writer !=
          plan->manifests[introduction_index + 1].value.writer) {
    const VerifiedManifest &candidate =
        plan->manifests[introduction_index + 1];
    if (anchor_index != introduction_index + 1 ||
        candidate.value.durable_cursor != plan->snapshot.cursor) {
      return corrupt(
          "writer-changing takeover snapshot is not anchored to the exact "
          "candidate HEAD");
    }
  }

  if (!collect_replay_and_verify_windows(
          plan->snapshot, plan->manifests, introduction_index, anchor_index,
          &plan->replay_segments, &error))
    return corrupt(error);

  const RecoveryWindow recomputed{
      static_cast<uint64_t>(plan->manifests.size()), manifest_bytes,
      static_cast<uint64_t>(plan->replay_segments.size())};
  if (recomputed != plan->head.recovery_window)
    return corrupt("HEAD recovery_window does not match the exact chain");
  return result(RecoveryReadOutcome::READY);
}

}  // namespace wesql::remote_commit
