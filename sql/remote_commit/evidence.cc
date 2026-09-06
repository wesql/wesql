/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/evidence.h"

#include <array>
#include <charconv>
#include <string_view>

namespace wesql::remote_commit {
namespace {

bool fail(std::string *error, std::string message) {
  if (error != nullptr) *error = std::move(message);
  return false;
}

void append_string(std::string_view value, std::string *json) {
  static constexpr char kHex[] = "0123456789abcdef";
  json->push_back('"');
  for (const unsigned char ch : value) {
    switch (ch) {
      case '"':
        json->append("\\\"");
        break;
      case '\\':
        json->append("\\\\");
        break;
      case '\b':
        json->append("\\b");
        break;
      case '\t':
        json->append("\\t");
        break;
      case '\n':
        json->append("\\n");
        break;
      case '\f':
        json->append("\\f");
        break;
      case '\r':
        json->append("\\r");
        break;
      default:
        if (ch < 0x20) {
          json->append("\\u00");
          json->push_back(kHex[ch >> 4]);
          json->push_back(kHex[ch & 0x0f]);
        } else {
          json->push_back(static_cast<char>(ch));
        }
    }
  }
  json->push_back('"');
}

void append_uint(uint64_t value, std::string *json) {
  std::array<char, 32> buffer{};
  const auto converted =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  json->append(buffer.data(),
               static_cast<size_t>(converted.ptr - buffer.data()));
}

void append_cursor(const Cursor &cursor, std::string *json) {
  json->append("{\"file\":");
  append_string(cursor.file, json);
  json->append(",\"pos\":");
  append_uint(cursor.pos, json);
  json->push_back('}');
}

void append_nullable_cursor(const std::optional<Cursor> &cursor,
                            std::string *json) {
  if (cursor.has_value())
    append_cursor(*cursor, json);
  else
    json->append("null");
}

bool is_lower_hex(std::string_view value, size_t size) {
  if (value.size() != size) return false;
  for (const unsigned char ch : value) {
    if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')))
      return false;
  }
  return true;
}

bool valid_sha(std::string_view value) { return is_lower_hex(value, 64); }

bool valid_cursor(const Cursor &cursor) {
  return !cursor.file.empty() && cursor.file.size() <= 128 &&
         cursor.pos <= kJsonSafeIntegerMax;
}

bool valid_object_ref(const ObjectRef &ref) {
  return !ref.key.empty() && ref.key.size() <= kMaxObjectKeyBytes &&
         ref.size <= kJsonSafeIntegerMax && valid_sha(ref.sha256);
}

void append_object_ref(const ObjectRef &ref, std::string *json) {
  json->append("{\"key\":");
  append_string(ref.key, json);
  json->append(",\"sha256\":");
  append_string(ref.sha256, json);
  json->append(",\"size\":");
  append_uint(ref.size, json);
  json->push_back('}');
}

bool valid_status(const StatusSnapshot &status, std::string *error) {
  if (status.stream_sha256.has_value() &&
      !valid_sha(*status.stream_sha256))
    return fail(error, "status stream digest is invalid");
  if (status.writer.has_value() &&
      (status.writer->epoch == 0 ||
       status.writer->epoch > kJsonSafeIntegerMax ||
       !is_lower_hex(status.writer->id, 32)))
    return fail(error, "status writer is invalid");
  if (status.head.has_value() &&
      (status.head->generation == 0 ||
       status.head->generation > kJsonSafeIntegerMax ||
       !valid_sha(status.head->etag_sha256) ||
       !valid_sha(status.head->body_sha256)))
    return fail(error, "status HEAD is invalid");
  if (status.durable_cursor.has_value() &&
      !valid_cursor(*status.durable_cursor))
    return fail(error, "status durable cursor is invalid");
  if (status.state == LifecycleState::OFF &&
      (status.stream_sha256.has_value() || status.writer.has_value() ||
       status.head.has_value() || status.durable_cursor.has_value()))
    return fail(error, "OFF status must not retain remote state");
  return true;
}

}  // namespace

bool format_status_json(const StatusSnapshot &status, std::string *json,
                        std::string *error) {
  if (json == nullptr) return fail(error, "null status output");
  if (!valid_status(status, error)) return false;
  json->clear();
  json->append("{\"durable_cursor\":");
  append_nullable_cursor(status.durable_cursor, json);
  json->append(",\"format\":\"wesql.remote_commit.status\",\"head\":");
  if (status.head.has_value()) {
    json->append("{\"body_sha256\":");
    append_string(status.head->body_sha256, json);
    json->append(",\"etag_sha256\":");
    append_string(status.head->etag_sha256, json);
    json->append(",\"generation\":");
    append_uint(status.head->generation, json);
    json->push_back('}');
  } else {
    json->append("null");
  }
  json->append(",\"state\":");
  append_string(lifecycle_state_name(status.state), json);
  json->append(",\"stream_sha256\":");
  if (status.stream_sha256.has_value())
    append_string(*status.stream_sha256, json);
  else
    json->append("null");
  json->append(",\"version\":1,\"writer\":");
  if (status.writer.has_value()) {
    json->append("{\"epoch\":");
    append_uint(status.writer->epoch, json);
    json->append(",\"id\":");
    append_string(status.writer->id, json);
    json->push_back('}');
  } else {
    json->append("null");
  }
  json->push_back('}');
  if (json->size() >= kStatusJsonMaxBytes)
    return fail(error, "remote commit status exceeds 767 bytes");
  return true;
}

bool format_ack_ready_json(const AckReadyEvent &event, std::string *json,
                           std::string *error) {
  if (json == nullptr) return fail(error, "null ACK event output");
  if (event.stream_id.empty() || event.stream_id.size() > 101 ||
      !is_lower_hex(event.writer.id, 32) || event.writer.epoch == 0 ||
      event.writer.epoch > kJsonSafeIntegerMax ||
      !valid_cursor(event.endpoint) ||
      event.transaction_count > kJsonSafeIntegerMax ||
      !valid_sha(event.gtid_set_sha256) || !valid_sha(event.xid_sha256) ||
      event.head_generation == 0 ||
      event.head_generation > kJsonSafeIntegerMax ||
      !valid_sha(event.head_etag_sha256) ||
      !valid_sha(event.head_body_sha256) ||
      !valid_object_ref(event.manifest) ||
      event.segment_count > kMaxSegmentsPerManifest ||
      !valid_sha(event.segment_refs_sha256) ||
      !valid_object_ref({event.last_segment.key, event.last_segment.size,
                         event.last_segment.sha256}) ||
      event.last_segment.sequence == 0 ||
      event.last_segment.sequence > kJsonSafeIntegerMax ||
      !valid_cursor({event.last_segment.source.file,
                     event.last_segment.source.start_pos}) ||
      event.last_segment.source.end_pos > kJsonSafeIntegerMax ||
      event.last_segment.source.start_pos >= event.last_segment.source.end_pos)
    return fail(error, "ACK event contains invalid or unbounded fields");

  json->clear();
  json->append("{\"endpoint\":");
  append_cursor(event.endpoint, json);
  json->append(",\"format\":\"wesql.remote_commit.ack_ready\","
               "\"gtid_set_sha256\":");
  append_string(event.gtid_set_sha256, json);
  json->append(",\"head\":{\"body_sha256\":");
  append_string(event.head_body_sha256, json);
  json->append(",\"etag_sha256\":");
  append_string(event.head_etag_sha256, json);
  json->append(",\"generation\":");
  append_uint(event.head_generation, json);
  json->append("},\"last_segment\":{\"key\":");
  append_string(event.last_segment.key, json);
  json->append(",\"sequence\":");
  append_uint(event.last_segment.sequence, json);
  json->append(",\"sha256\":");
  append_string(event.last_segment.sha256, json);
  json->append(",\"size\":");
  append_uint(event.last_segment.size, json);
  json->append(",\"source\":{\"end_pos\":");
  append_uint(event.last_segment.source.end_pos, json);
  json->append(",\"file\":");
  append_string(event.last_segment.source.file, json);
  json->append(",\"start_pos\":");
  append_uint(event.last_segment.source.start_pos, json);
  json->append("}},\"manifest\":");
  append_object_ref(event.manifest, json);
  json->append(",\"segment_count\":");
  append_uint(event.segment_count, json);
  json->append(",\"segment_refs_sha256\":");
  append_string(event.segment_refs_sha256, json);
  json->append(",\"stream_id\":");
  append_string(event.stream_id, json);
  json->append(",\"transaction_count\":");
  append_uint(event.transaction_count, json);
  json->append(",\"version\":1,\"writer\":{\"epoch\":");
  append_uint(event.writer.epoch, json);
  json->append(",\"id\":");
  append_string(event.writer.id, json);
  json->append("},\"xid_sha256\":");
  append_string(event.xid_sha256, json);
  json->push_back('}');
  if (sizeof(kAckReadyPrefix) - 1 + json->size() >=
      kAckReadyEventMaxBytes)
    return fail(error, "remote commit ACK event exceeds 7167 bytes");
  return true;
}

bool format_ack_ready_log_line(const AckReadyEvent &event, std::string *line,
                               std::string *error) {
  if (line == nullptr) return fail(error, "null ACK log-line output");
  std::string json;
  if (!format_ack_ready_json(event, &json, error)) return false;
  line->assign(kAckReadyPrefix);
  line->append(json);
  return true;
}

}  // namespace wesql::remote_commit
