/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/segment_sealer.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <limits>
#include <utility>

namespace wesql::remote_commit {
namespace {

constexpr std::array<unsigned char, 4> kBinlogMagic{0xfe, 0x62, 0x69, 0x6e};
constexpr size_t kEventHeaderBytes = 19;
constexpr unsigned char kFormatDescriptionEvent = 15;

PublishResult result(PublishOutcome outcome, std::string detail = {}) {
  return {outcome, std::move(detail), std::nullopt};
}

uint32_t read_le32(const unsigned char *value) {
  return static_cast<uint32_t>(value[0]) |
         (static_cast<uint32_t>(value[1]) << 8) |
         (static_cast<uint32_t>(value[2]) << 16) |
         (static_cast<uint32_t>(value[3]) << 24);
}

SegmentTip to_tip(const SegmentRef &segment) {
  SegmentTip tip;
  tip.kind = SegmentTipKind::SEGMENT;
  tip.key = segment.key;
  tip.size = segment.size;
  tip.sha256 = segment.sha256;
  tip.sequence = segment.sequence;
  tip.snapshot_id = std::nullopt;
  tip.cursor = std::nullopt;
  return tip;
}

bool valid_snapshot_root(const SegmentTip &tip) {
  return tip.kind == SegmentTipKind::SNAPSHOT_ROOT && !tip.key.has_value() &&
         !tip.size.has_value() && !tip.sha256.has_value() &&
         !tip.sequence.has_value() && tip.snapshot_id.has_value() &&
         tip.cursor.has_value();
}

bool valid_segment_tip(const SegmentTip &tip) {
  return tip.kind == SegmentTipKind::SEGMENT && tip.key.has_value() &&
         tip.size.has_value() && tip.sha256.has_value() &&
         tip.sequence.has_value() && !tip.snapshot_id.has_value() &&
         !tip.cursor.has_value();
}

}  // namespace

bool validate_native_binlog_range(std::string_view body,
                                  const SegmentSource &source,
                                  std::string *error) {
  const auto fail = [&](const char *message) {
    if (error != nullptr) *error = message;
    return false;
  };
  if (source.file.empty() || source.start_pos >= source.end_pos ||
      source.end_pos - source.start_pos != body.size())
    return fail("native range coordinates do not match body size");

  size_t offset = 0;
  if (source.start_pos == 0) {
    if (body.size() < kBinlogMagic.size() + kEventHeaderBytes)
      return fail("new binlog range is too short for magic and FDE");
    for (size_t i = 0; i < kBinlogMagic.size(); ++i) {
      if (static_cast<unsigned char>(body[i]) != kBinlogMagic[i])
        return fail("new binlog range has invalid magic");
    }
    offset = kBinlogMagic.size();
    if (static_cast<unsigned char>(body[offset + 4]) !=
        kFormatDescriptionEvent)
      return fail("new binlog range does not start with an FDE");
  }

  size_t event_count = 0;
  while (offset < body.size()) {
    if (body.size() - offset < kEventHeaderBytes)
      return fail("native range ends in a partial event header");
    const auto *header = reinterpret_cast<const unsigned char *>(
        body.data() + static_cast<std::ptrdiff_t>(offset));
    const uint32_t event_size = read_le32(header + 9);
    if (event_size < kEventHeaderBytes || event_size > body.size() - offset)
      return fail("native range contains an invalid event size");
    offset += event_size;
    ++event_count;
  }
  if (event_count == 0 || offset != body.size())
    return fail("native range is not an exact event sequence");
  return true;
}

PublishResult SegmentSealer::read_range(const NativeBinlogRange &range,
                                        std::string *body) {
  if (body == nullptr)
    return result(PublishOutcome::PERMANENT_ERROR, "null range output");
  if (range.source.start_pos >= range.source.end_pos)
    return result(PublishOutcome::PERMANENT_ERROR, "empty binlog range");
  const uint64_t length = range.source.end_pos - range.source.start_pos;
  if (length > max_segment_bytes_ || length > std::string().max_size())
    return result(PublishOutcome::PERMANENT_ERROR,
                  "binlog range exceeds the configured segment limit");

  std::error_code filesystem_error;
  const uint64_t size_before =
      std::filesystem::file_size(range.local_path, filesystem_error);
  if (filesystem_error || range.source.end_pos > size_before)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "binlog range is outside the source file");

  std::ifstream input(range.local_path, std::ios::binary);
  if (!input)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "cannot open native binlog source");
  input.seekg(static_cast<std::streamoff>(range.source.start_pos));
  if (!input)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "cannot seek native binlog source");
  body->assign(static_cast<size_t>(length), '\0');
  input.read(body->data(), static_cast<std::streamsize>(length));
  if (input.gcount() != static_cast<std::streamsize>(length) || !input)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "short read from native binlog source");

  const uint64_t size_after =
      std::filesystem::file_size(range.local_path, filesystem_error);
  if (filesystem_error || size_before != size_after)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "native binlog source changed while sealing");
  std::string validation_error;
  if (!validate_native_binlog_range(*body, range.source, &validation_error))
    return result(PublishOutcome::PERMANENT_ERROR,
                  "invalid native binlog range: " + validation_error);
  return result(PublishOutcome::APPLIED);
}

PublishResult SegmentSealer::seal(
    const Writer &writer, uint64_t first_sequence,
    const SegmentTip &previous_tip,
    const std::vector<NativeBinlogRange> &ranges, SealedSegments *sealed) {
  if (store_ == nullptr || sealed == nullptr)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "segment sealer is not initialized");
  if (ranges.empty() || first_sequence == 0 ||
      first_sequence > kJsonSafeIntegerMax ||
      (!valid_snapshot_root(previous_tip) && !valid_segment_tip(previous_tip)))
    return result(PublishOutcome::PERMANENT_ERROR,
                  "invalid segment chain input");
  if (previous_tip.kind == SegmentTipKind::SEGMENT &&
      (*previous_tip.sequence == kJsonSafeIntegerMax ||
       first_sequence != *previous_tip.sequence + 1))
    return result(PublishOutcome::PERMANENT_ERROR,
                  "segment sequence does not continue the prior tip");
  if (previous_tip.kind == SegmentTipKind::SNAPSHOT_ROOT &&
      first_sequence != 1)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "first segment after bootstrap must have sequence 1");
  if (ranges.size() - 1 > kJsonSafeIntegerMax - first_sequence)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "segment sequence exhausted");

  sealed->segments.clear();
  sealed->tip = previous_tip;
  for (size_t index = 0; index < ranges.size(); ++index) {
    const auto &range = ranges[index];
    if (index > 0) {
      const auto &prior = ranges[index - 1].source;
      const bool same_file = range.source.file == prior.file;
      if ((same_file && range.source.start_pos != prior.end_pos) ||
          (!same_file && range.source.start_pos != 0))
        return result(PublishOutcome::PERMANENT_ERROR,
                      "binlog ranges contain a gap or overlap");
    }

    std::string body;
    auto read = read_range(range, &body);
    if (!read.applied()) return read;

    SegmentRef segment;
    segment.sequence = first_sequence + index;
    segment.size = body.size();
    segment.source = range.source;
    segment.previous_segment = sealed->tip;
    segment.transaction_count = range.metadata.transaction_count;
    segment.ends_at_transaction_boundary = true;
    std::string error;
    if (!gtid_digest(range.metadata.gtid_set, &segment.gtid_set, &error) ||
        !xid_digest(range.metadata.xids, &segment.xids, &error) ||
        !sha256_hex(body, &segment.sha256, &error) ||
        !segment_object_key(stream_, writer, segment.source, segment.sha256,
                            &segment.key, &error))
      return result(PublishOutcome::PERMANENT_ERROR,
                    "cannot describe native segment: " + error);

    auto upload = store_->create_immutable(segment.key, body);
    if (!upload.applied()) return upload;
    sealed->segments.push_back(segment);
    sealed->tip = to_tip(segment);
  }
  const auto &last = sealed->segments.back().source;
  sealed->durable_cursor = {last.file, last.end_pos};
  return result(PublishOutcome::APPLIED);
}

}  // namespace wesql::remote_commit
