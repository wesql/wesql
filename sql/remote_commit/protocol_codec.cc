/* Copyright (c) 2026, ApeCloud Inc Holding Limited.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation. */

#include "sql/remote_commit/protocol_codec.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cstddef>
#include <cstring>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <utility>

#include <openssl/evp.h>
#include "my_rapidjson_size_t.h"
#include <rapidjson/error/en.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/reader.h>

namespace wesql::remote_commit {
namespace {

constexpr std::string_view kWriterEpochFormat =
    "wesql.remote_commit.writer_epoch";
constexpr std::string_view kHeadFormat = "wesql.remote_commit.head";
constexpr std::string_view kManifestFormat = "wesql.remote_commit.manifest";
constexpr std::string_view kSnapshotFormat = "wesql.remote_commit.snapshot";
constexpr std::string_view kSegmentPayloadFormat =
    "native-mysql-binlog-range-v1";
constexpr std::string_view kSeedPayloadFormat =
    "native-mysql-binlog-prefix-v1";
constexpr std::string_view kExtentFormat = "smartengine-object-extent-v2";
constexpr std::string_view kNoCompression = "none";
constexpr std::string_view kChecksumCrc32 = "CRC32";

bool fail(std::string *error, std::string message) {
  if (error != nullptr) *error = std::move(message);
  return false;
}

bool valid_utf8(std::string_view value) {
  const auto *data = reinterpret_cast<const unsigned char *>(value.data());
  size_t pos = 0;
  while (pos < value.size()) {
    const unsigned char lead = data[pos++];
    if (lead <= 0x7f) continue;

    uint32_t codepoint = 0;
    size_t continuation_count = 0;
    if (lead >= 0xc2 && lead <= 0xdf) {
      codepoint = lead & 0x1f;
      continuation_count = 1;
    } else if (lead >= 0xe0 && lead <= 0xef) {
      codepoint = lead & 0x0f;
      continuation_count = 2;
    } else if (lead >= 0xf0 && lead <= 0xf4) {
      codepoint = lead & 0x07;
      continuation_count = 3;
    } else {
      return false;
    }
    if (continuation_count > value.size() - pos) return false;
    for (size_t i = 0; i < continuation_count; ++i) {
      const unsigned char next = data[pos++];
      if ((next & 0xc0) != 0x80) return false;
      codepoint = (codepoint << 6) | (next & 0x3f);
    }
    if ((continuation_count == 2 && codepoint < 0x800) ||
        (continuation_count == 3 && codepoint < 0x10000) ||
        (codepoint >= 0xd800 && codepoint <= 0xdfff) ||
        codepoint > 0x10ffff) {
      return false;
    }
  }
  return true;
}

void append_json_string(std::string_view value, std::string *out) {
  static constexpr char kHex[] = "0123456789abcdef";
  out->push_back('"');
  for (const unsigned char ch : value) {
    switch (ch) {
      case '"':
        out->append("\\\"");
        break;
      case '\\':
        out->append("\\\\");
        break;
      case '\b':
        out->append("\\b");
        break;
      case '\t':
        out->append("\\t");
        break;
      case '\n':
        out->append("\\n");
        break;
      case '\f':
        out->append("\\f");
        break;
      case '\r':
        out->append("\\r");
        break;
      default:
        if (ch < 0x20) {
          out->append("\\u00");
          out->push_back(kHex[ch >> 4]);
          out->push_back(kHex[ch & 0x0f]);
        } else {
          out->push_back(static_cast<char>(ch));
        }
    }
  }
  out->push_back('"');
}

void append_uint(uint64_t value, std::string *out) {
  std::array<char, 32> buffer{};
  const auto result =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  out->append(buffer.data(), static_cast<size_t>(result.ptr - buffer.data()));
}

enum class JsonType { kNull, kBool, kUint, kString, kObject, kArray };

struct JsonValue {
  JsonType type{JsonType::kNull};
  bool boolean{false};
  uint64_t uint_value{0};
  std::string string_value;
  std::vector<std::pair<std::string, JsonValue>> object_values;
  std::vector<JsonValue> array_values;

  static JsonValue null_value() { return {}; }
  static JsonValue bool_value(bool value) {
    JsonValue result;
    result.type = JsonType::kBool;
    result.boolean = value;
    return result;
  }
  static JsonValue unsigned_value(uint64_t value) {
    JsonValue result;
    result.type = JsonType::kUint;
    result.uint_value = value;
    return result;
  }
  static JsonValue string(std::string value) {
    JsonValue result;
    result.type = JsonType::kString;
    result.string_value = std::move(value);
    return result;
  }
  static JsonValue object() {
    JsonValue result;
    result.type = JsonType::kObject;
    return result;
  }
  static JsonValue array() {
    JsonValue result;
    result.type = JsonType::kArray;
    return result;
  }
};

class StrictJsonHandler
    : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>,
                                          StrictJsonHandler> {
 public:
  bool Null() { return add_value(JsonValue::null_value()); }
  bool Bool(bool value) { return add_value(JsonValue::bool_value(value)); }
  bool Int(int value) {
    return value >= 0 && Uint(static_cast<unsigned>(value));
  }
  bool Uint(unsigned value) {
    return Uint64(static_cast<uint64_t>(value));
  }
  bool Int64(int64_t value) {
    return value >= 0 && Uint64(static_cast<uint64_t>(value));
  }
  bool Uint64(uint64_t value) {
    if (value > kJsonSafeIntegerMax) {
      return set_error("JSON integer exceeds the safe-integer limit");
    }
    return add_value(JsonValue::unsigned_value(value));
  }
  bool Double(double) { return set_error("floating-point JSON is forbidden"); }
  bool RawNumber(const char *, rapidjson::SizeType, bool) {
    return set_error("non-integer JSON number is forbidden");
  }
  bool String(const char *value, rapidjson::SizeType length, bool) {
    if (length > kMaxCanonicalGtidBytes) {
      return set_error("JSON string exceeds the protocol limit");
    }
    return add_value(JsonValue::string(std::string(value, length)));
  }
  bool Key(const char *value, rapidjson::SizeType length, bool) {
    if (frames_.empty() ||
        frames_.back().container.type != JsonType::kObject) {
      return set_error("JSON key outside an object");
    }
    if (length > kMaxOrdinaryIdBytes) {
      return set_error("JSON member name exceeds the protocol limit");
    }
    Frame &frame = frames_.back();
    if (frame.has_pending_key) return set_error("JSON object key has no value");
    const std::string key(value, length);
    for (const auto &member : frame.container.object_values) {
      if (member.first == key) return set_error("duplicate JSON member: " + key);
    }
    frame.pending_key = key;
    frame.has_pending_key = true;
    return true;
  }
  bool StartObject() { return start_container(JsonValue::object(), 0); }
  bool EndObject(rapidjson::SizeType member_count) {
    if (frames_.empty() ||
        frames_.back().container.type != JsonType::kObject) {
      return set_error("mismatched JSON object end");
    }
    Frame frame = std::move(frames_.back());
    frames_.pop_back();
    if (frame.has_pending_key ||
        frame.container.object_values.size() != member_count) {
      return set_error("incomplete JSON object");
    }
    return add_value(std::move(frame.container));
  }
  bool StartArray() {
    size_t limit = kMaxSnapshotItems;
    if (!frames_.empty() &&
        frames_.back().container.type == JsonType::kObject &&
        frames_.back().has_pending_key &&
        frames_.back().pending_key == "segments") {
      limit = kMaxSegmentsPerManifest;
    }
    return start_container(JsonValue::array(), limit);
  }
  bool EndArray(rapidjson::SizeType element_count) {
    if (frames_.empty() ||
        frames_.back().container.type != JsonType::kArray) {
      return set_error("mismatched JSON array end");
    }
    Frame frame = std::move(frames_.back());
    frames_.pop_back();
    if (frame.container.array_values.size() != element_count) {
      return set_error("incomplete JSON array");
    }
    return add_value(std::move(frame.container));
  }

  bool ok() const { return error_.empty(); }
  const std::string &error() const { return error_; }
  JsonValue take_root() { return std::move(root_); }
  bool has_root() const { return has_root_; }

 private:
  struct Frame {
    JsonValue container;
    std::string pending_key;
    bool has_pending_key{false};
    size_t item_limit{0};
  };

  bool set_error(std::string message) {
    if (error_.empty()) error_ = std::move(message);
    return false;
  }

  bool start_container(JsonValue container, size_t item_limit) {
    if (frames_.size() + 1 > kMaxJsonDepth) {
      return set_error("JSON nesting exceeds depth 16");
    }
    frames_.push_back(
        Frame{std::move(container), std::string(), false, item_limit});
    return true;
  }

  bool add_value(JsonValue value) {
    if (frames_.empty()) {
      if (has_root_) return set_error("multiple JSON root values");
      root_ = std::move(value);
      has_root_ = true;
      return true;
    }
    Frame &frame = frames_.back();
    if (frame.container.type == JsonType::kObject) {
      if (!frame.has_pending_key) {
        return set_error("JSON object value has no key");
      }
      frame.container.object_values.emplace_back(
          std::move(frame.pending_key), std::move(value));
      frame.pending_key.clear();
      frame.has_pending_key = false;
      return true;
    }
    if (frame.container.type == JsonType::kArray) {
      if (frame.container.array_values.size() >= frame.item_limit) {
        return set_error("JSON array exceeds the protocol item limit");
      }
      frame.container.array_values.push_back(std::move(value));
      return true;
    }
    return set_error("invalid JSON container state");
  }

  JsonValue root_;
  bool has_root_{false};
  std::vector<Frame> frames_;
  std::string error_;
};

bool parse_json(std::string_view json, size_t max_bytes, JsonValue *root,
                std::string *error) {
  if (root == nullptr) return fail(error, "null JSON result");
  if (json.size() > max_bytes) return fail(error, "JSON body exceeds size limit");
  if (json.empty()) return fail(error, "empty JSON body");

  rapidjson::MemoryStream stream(json.data(), json.size());
  rapidjson::Reader reader;
  StrictJsonHandler handler;
  const bool parsed = reader.Parse<rapidjson::kParseValidateEncodingFlag>(
      stream, handler);
  if (!parsed) {
    if (!handler.ok()) return fail(error, handler.error());
    return fail(error,
                std::string("invalid JSON at byte ") +
                    std::to_string(reader.GetErrorOffset()) + ": " +
                    rapidjson::GetParseError_En(reader.GetParseErrorCode()));
  }
  if (!handler.has_root()) return fail(error, "JSON has no root value");
  *root = handler.take_root();
  return true;
}

const JsonValue *member(const JsonValue &object, std::string_view name) {
  if (object.type != JsonType::kObject) return nullptr;
  for (const auto &entry : object.object_values) {
    if (entry.first == name) return &entry.second;
  }
  return nullptr;
}

bool exact_members(const JsonValue &object,
                   std::initializer_list<std::string_view> expected,
                   std::string_view type_name, std::string *error) {
  if (object.type != JsonType::kObject) {
    return fail(error, std::string(type_name) + " must be an object");
  }
  if (object.object_values.size() != expected.size()) {
    return fail(error, std::string(type_name) +
                           " has missing or unknown members");
  }
  for (const auto &entry : object.object_values) {
    if (std::find(expected.begin(), expected.end(), entry.first) ==
        expected.end()) {
      return fail(error, std::string(type_name) + " has unknown member " +
                             entry.first);
    }
  }
  return true;
}

bool read_string(const JsonValue &object, std::string_view name,
                 std::string *result, std::string *error) {
  const JsonValue *value = member(object, name);
  if (value == nullptr || value->type != JsonType::kString) {
    return fail(error, std::string(name) + " must be a string");
  }
  *result = value->string_value;
  return true;
}

bool read_uint(const JsonValue &object, std::string_view name, uint64_t *result,
               std::string *error) {
  const JsonValue *value = member(object, name);
  if (value == nullptr || value->type != JsonType::kUint) {
    return fail(error, std::string(name) + " must be a non-negative integer");
  }
  *result = value->uint_value;
  return true;
}

bool read_bool(const JsonValue &object, std::string_view name, bool *result,
               std::string *error) {
  const JsonValue *value = member(object, name);
  if (value == nullptr || value->type != JsonType::kBool) {
    return fail(error, std::string(name) + " must be a boolean");
  }
  *result = value->boolean;
  return true;
}

bool require_literal(const JsonValue &object, std::string_view name,
                     std::string_view expected, std::string *error) {
  std::string actual;
  if (!read_string(object, name, &actual, error)) return false;
  return actual == expected
             ? true
             : fail(error, std::string(name) + " has unsupported value");
}

bool require_version(const JsonValue &object, std::string *error) {
  uint64_t version = 0;
  if (!read_uint(object, "version", &version, error)) return false;
  return version == 2 ? true : fail(error, "version must be exactly 2");
}

bool require_stream(const JsonValue &object, const StreamIdentity &stream,
                    std::string *error) {
  std::string stream_id;
  if (!read_string(object, "stream_id", &stream_id, error)) return false;
  return stream_id == stream.stream_id
             ? true
             : fail(error, "stream_id does not match the canonical stream");
}

bool is_ascii_component(std::string_view value) {
  if (value.empty() || value.size() > 48) return false;
  const auto alpha_numeric = [](unsigned char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= '0' && ch <= '9');
  };
  if (!alpha_numeric(static_cast<unsigned char>(value.front()))) return false;
  for (const unsigned char ch : value) {
    if (!alpha_numeric(ch) && ch != '.' && ch != '_' && ch != '-') return false;
  }
  return value != "." && value != "..";
}

bool is_cluster_prefix(std::string_view value) {
  if (value.empty() || value.front() == '/' || value.back() == '/') return false;
  size_t begin = 0;
  while (begin < value.size()) {
    const size_t slash = value.find('/', begin);
    const size_t end = slash == std::string_view::npos ? value.size() : slash;
    if (!is_ascii_component(value.substr(begin, end - begin))) return false;
    if (slash == std::string_view::npos) break;
    begin = slash + 1;
  }
  return true;
}

bool is_lower_hex(std::string_view value, size_t exact_length) {
  if (value.size() != exact_length) return false;
  for (const unsigned char ch : value) {
    if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) return false;
  }
  return true;
}

bool is_sha256(std::string_view value) { return is_lower_hex(value, 64); }

bool lowercase_hex_decodes_to_utf8(std::string_view encoded,
                                   std::string *error) {
  if (encoded.empty() || encoded.size() % 2 != 0 ||
      !is_lower_hex(encoded, encoded.size())) {
    return fail(error, "hex string must be non-empty, lowercase, and even");
  }
  std::string decoded;
  decoded.reserve(encoded.size() / 2);
  for (size_t i = 0; i < encoded.size(); i += 2) {
    unsigned value = 0;
    const auto conversion = std::from_chars(encoded.data() + i,
                                            encoded.data() + i + 2, value, 16);
    if (conversion.ec != std::errc() || conversion.ptr != encoded.data() + i + 2) {
      return fail(error, "hex string contains an invalid byte");
    }
    decoded.push_back(static_cast<char>(value));
  }
  return valid_utf8(decoded)
             ? true
             : fail(error, "hex string does not decode to valid UTF-8");
}

bool parse_decimal_uint64(std::string_view value, uint64_t *result,
                          bool allow_leading_zero, std::string *error) {
  if (value.empty()) return fail(error, "empty decimal integer string");
  if (!allow_leading_zero && value.size() > 1 && value.front() == '0') {
    return fail(error, "decimal integer string has a leading zero");
  }
  uint64_t parsed = 0;
  const auto conversion =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (conversion.ec != std::errc() ||
      conversion.ptr != value.data() + value.size()) {
    return fail(error, "invalid or overflowing decimal integer string");
  }
  *result = parsed;
  return true;
}

bool parse_uuid(std::string_view value, std::array<unsigned char, 16> *bytes,
                bool require_lowercase, std::string *error) {
  if (value.size() != 36 || value[8] != '-' || value[13] != '-' ||
      value[18] != '-' || value[23] != '-') {
    return fail(error, "UUID is not in 8-4-4-4-12 form");
  }
  const auto nibble = [require_lowercase](char ch) -> int {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (!require_lowercase && ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
  };
  size_t output = 0;
  int high = -1;
  for (const char ch : value) {
    if (ch == '-') continue;
    const int current = nibble(ch);
    if (current < 0) return fail(error, "UUID contains a non-hex character");
    if (high < 0) {
      high = current;
    } else {
      (*bytes)[output++] = static_cast<unsigned char>((high << 4) | current);
      high = -1;
    }
  }
  return output == bytes->size() && high < 0;
}

std::string format_uuid(const std::array<unsigned char, 16> &bytes) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(36);
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) result.push_back('-');
    result.push_back(kHex[bytes[i] >> 4]);
    result.push_back(kHex[bytes[i] & 0x0f]);
  }
  return result;
}

bool parse_binlog_file(std::string_view file, std::string *basename,
                       uint64_t *sequence, std::string *error) {
  const size_t dot = file.rfind('.');
  if (dot == std::string_view::npos || dot == 0 || dot + 1 == file.size()) {
    return fail(error, "binlog file is missing basename or sequence");
  }
  const std::string_view base = file.substr(0, dot);
  const std::string_view digits = file.substr(dot + 1);
  if (!is_ascii_component(base)) {
    return fail(error, "binlog basename is not a canonical component");
  }
  if (digits.size() < 6) {
    return fail(error, "binlog sequence must contain at least six digits");
  }
  uint64_t parsed = 0;
  if (!parse_decimal_uint64(digits, &parsed, true, error) ||
      parsed > kJsonSafeIntegerMax) {
    return fail(error, "binlog sequence is outside the safe-integer range");
  }
  std::string canonical = std::to_string(parsed);
  if (canonical.size() < 6) canonical.insert(0, 6 - canonical.size(), '0');
  if (digits != canonical) {
    return fail(error, "binlog sequence is not minimally zero-padded");
  }
  *basename = std::string(base);
  *sequence = parsed;
  return true;
}

bool validate_cursor(const Cursor &cursor, std::string *error) {
  if (cursor.pos > kJsonSafeIntegerMax) {
    return fail(error, "cursor position exceeds the safe-integer limit");
  }
  std::string basename;
  uint64_t sequence = 0;
  return parse_binlog_file(cursor.file, &basename, &sequence, error);
}

bool cursor_order(const Cursor &left, const Cursor &right, int *comparison,
                  std::string *error) {
  std::string left_basename;
  std::string right_basename;
  uint64_t left_sequence = 0;
  uint64_t right_sequence = 0;
  if (!parse_binlog_file(left.file, &left_basename, &left_sequence, error) ||
      !parse_binlog_file(right.file, &right_basename, &right_sequence, error)) {
    return false;
  }
  if (left_basename != right_basename) {
    return fail(error, "cursor binlog basenames differ");
  }
  if (left_sequence != right_sequence) {
    *comparison = left_sequence < right_sequence ? -1 : 1;
  } else if (left.pos != right.pos) {
    *comparison = left.pos < right.pos ? -1 : 1;
  } else {
    *comparison = 0;
  }
  return true;
}

bool validate_key(std::string_view key, std::string *error) {
  if (key.empty() || key.size() > kMaxObjectKeyBytes || !valid_utf8(key) ||
      key.front() == '/' || key.back() == '/' || key.find("//") != std::string::npos ||
      key.find('\\') != std::string::npos || key.find('%') != std::string::npos) {
    return fail(error, "object key is not canonical");
  }
  size_t begin = 0;
  while (begin < key.size()) {
    const size_t slash = key.find('/', begin);
    const size_t end = slash == std::string_view::npos ? key.size() : slash;
    const std::string_view component = key.substr(begin, end - begin);
    if (component.empty() || component == "." || component == "..") {
      return fail(error, "object key contains a non-canonical component");
    }
    if (slash == std::string_view::npos) break;
    begin = slash + 1;
  }
  return true;
}

bool validate_object_ref(const ObjectRef &ref, std::string *error) {
  if (!validate_key(ref.key, error)) return false;
  if (ref.size == 0 || ref.size > kJsonSafeIntegerMax) {
    return fail(error, "object size is zero or exceeds the safe-integer limit");
  }
  return is_sha256(ref.sha256)
             ? true
             : fail(error, "object SHA-256 is not lowercase hex");
}

bool validate_writer(const Writer &writer, std::string *error) {
  if (!is_lower_hex(writer.id, 32)) {
    return fail(error, "writer id must be 32 lowercase hex characters");
  }
  if (writer.epoch == 0 || writer.epoch > kJsonSafeIntegerMax) {
    return fail(error, "writer epoch is outside the protocol range");
  }
  return true;
}

bool validate_head_parent(const HeadParent &parent, std::string *error) {
  if (parent.generation == 0 || parent.generation > kJsonSafeIntegerMax) {
    return fail(error, "parent generation is outside the protocol range");
  }
  if (parent.etag.empty() || parent.etag.size() > kMaxObjectKeyBytes ||
      !valid_utf8(parent.etag)) {
    return fail(error, "parent ETag is empty or too large");
  }
  for (const unsigned char ch : parent.etag) {
    if (ch < 0x20 || ch == 0x7f) {
      return fail(error, "parent ETag contains a control character");
    }
  }
  return is_sha256(parent.sha256)
             ? true
             : fail(error, "parent SHA-256 is not lowercase hex");
}

bool validate_recovery_window(const RecoveryWindow &window,
                              std::string *error) {
  if (window.manifest_count == 0 ||
      window.manifest_count > kRecoveryManifestCountMax ||
      window.manifest_bytes == 0 ||
      window.manifest_bytes > kRecoveryManifestBytesMax ||
      window.segment_count > kRecoverySegmentCountMax) {
    return fail(error, "recovery_window is outside its hard limits");
  }
  return true;
}

bool validate_snapshot_id(std::string_view id, std::string *error) {
  return is_lower_hex(id, 32)
             ? true
             : fail(error, "snapshot id must be 32 lowercase hex characters");
}

bool validate_relative_path(std::string_view path, std::string *error) {
  if (path.empty() || path.size() > kMaxObjectKeyBytes || !valid_utf8(path) ||
      path.front() == '/' || path.back() == '/' ||
      path.find('\\') != std::string_view::npos ||
      path.find("//") != std::string_view::npos) {
    return fail(error, "snapshot relative_path is not canonical");
  }
  size_t begin = 0;
  while (begin < path.size()) {
    const size_t slash = path.find('/', begin);
    const size_t end = slash == std::string_view::npos ? path.size() : slash;
    const std::string_view component = path.substr(begin, end - begin);
    if (component.empty() || component == "." || component == "..") {
      return fail(error, "snapshot relative_path contains dot components");
    }
    for (const unsigned char ch : component) {
      if (ch < 0x20 || ch == 0x7f) {
        return fail(error, "snapshot relative_path contains control bytes");
      }
    }
    if (slash == std::string_view::npos) break;
    begin = slash + 1;
  }
  return true;
}

bool format_padded_20(uint64_t value, std::string *result,
                      std::string *error) {
  std::string decimal = std::to_string(value);
  if (decimal.size() > 20) return fail(error, "integer does not fit width 20");
  result->assign(20 - decimal.size(), '0');
  result->append(decimal);
  return true;
}

bool validate_stream_identity(const StreamIdentity &stream,
                              std::string *error) {
  StreamIdentity rebuilt;
  if (!build_stream_identity(stream.repo_id, stream.branch_id,
                             stream.cluster_object_prefix, &rebuilt, error)) {
    return false;
  }
  return rebuilt == stream
             ? true
             : fail(error, "StreamIdentity contains inconsistent derived fields");
}

void append_cursor(const Cursor &cursor, std::string *out) {
  out->append("{\"file\":");
  append_json_string(cursor.file, out);
  out->append(",\"pos\":");
  append_uint(cursor.pos, out);
  out->push_back('}');
}

void append_writer(const Writer &writer, std::string *out) {
  out->append("{\"epoch\":");
  append_uint(writer.epoch, out);
  out->append(",\"id\":");
  append_json_string(writer.id, out);
  out->push_back('}');
}

void append_object_ref(const ObjectRef &ref, std::string *out) {
  out->append("{\"key\":");
  append_json_string(ref.key, out);
  out->append(",\"sha256\":");
  append_json_string(ref.sha256, out);
  out->append(",\"size\":");
  append_uint(ref.size, out);
  out->push_back('}');
}

void append_head_parent(const HeadParent &parent, std::string *out) {
  out->append("{\"etag\":");
  append_json_string(parent.etag, out);
  out->append(",\"generation\":");
  append_uint(parent.generation, out);
  out->append(",\"sha256\":");
  append_json_string(parent.sha256, out);
  out->push_back('}');
}

void append_manifest_ref(const ManifestRef &ref, std::string *out) {
  out->append("{\"generation\":");
  append_uint(ref.generation, out);
  out->append(",\"key\":");
  append_json_string(ref.key, out);
  out->append(",\"sha256\":");
  append_json_string(ref.sha256, out);
  out->append(",\"size\":");
  append_uint(ref.size, out);
  out->push_back('}');
}

void append_recovery_window(const RecoveryWindow &window, std::string *out) {
  out->append("{\"manifest_bytes\":");
  append_uint(window.manifest_bytes, out);
  out->append(",\"manifest_count\":");
  append_uint(window.manifest_count, out);
  out->append(",\"segment_count\":");
  append_uint(window.segment_count, out);
  out->push_back('}');
}

void append_segment_tip(const SegmentTip &tip, std::string *out) {
  out->append("{\"cursor\":");
  if (tip.cursor.has_value())
    append_cursor(*tip.cursor, out);
  else
    out->append("null");
  out->append(",\"key\":");
  if (tip.key.has_value())
    append_json_string(*tip.key, out);
  else
    out->append("null");
  out->append(",\"kind\":");
  append_json_string(tip.kind == SegmentTipKind::SEGMENT ? "SEGMENT"
                                                        : "SNAPSHOT_ROOT",
                     out);
  out->append(",\"sequence\":");
  if (tip.sequence.has_value())
    append_uint(*tip.sequence, out);
  else
    out->append("null");
  out->append(",\"sha256\":");
  if (tip.sha256.has_value())
    append_json_string(*tip.sha256, out);
  else
    out->append("null");
  out->append(",\"size\":");
  if (tip.size.has_value())
    append_uint(*tip.size, out);
  else
    out->append("null");
  out->append(",\"snapshot_id\":");
  if (tip.snapshot_id.has_value())
    append_json_string(*tip.snapshot_id, out);
  else
    out->append("null");
  out->push_back('}');
}

void append_snapshot_ref(const SnapshotRef &ref, std::string *out) {
  out->append("{\"cursor\":");
  append_cursor(ref.cursor, out);
  out->append(",\"id\":");
  append_json_string(ref.id, out);
  out->append(",\"manifest_key\":");
  append_json_string(ref.manifest_key, out);
  out->append(",\"manifest_sha256\":");
  append_json_string(ref.manifest_sha256, out);
  out->append(",\"manifest_size\":");
  append_uint(ref.manifest_size, out);
  out->push_back('}');
}

bool decode_cursor(const JsonValue &json, Cursor *cursor, std::string *error) {
  if (!exact_members(json, {"file", "pos"}, "Cursor", error) ||
      !read_string(json, "file", &cursor->file, error) ||
      !read_uint(json, "pos", &cursor->pos, error)) {
    return false;
  }
  return validate_cursor(*cursor, error);
}

bool decode_writer(const JsonValue &json, Writer *writer, std::string *error) {
  if (!exact_members(json, {"id", "epoch"}, "Writer", error) ||
      !read_string(json, "id", &writer->id, error) ||
      !read_uint(json, "epoch", &writer->epoch, error)) {
    return false;
  }
  return validate_writer(*writer, error);
}

bool decode_object_ref(const JsonValue &json, ObjectRef *ref,
                       std::string *error) {
  if (!exact_members(json, {"key", "size", "sha256"}, "ObjectRef", error) ||
      !read_string(json, "key", &ref->key, error) ||
      !read_uint(json, "size", &ref->size, error) ||
      !read_string(json, "sha256", &ref->sha256, error)) {
    return false;
  }
  return validate_object_ref(*ref, error);
}

bool decode_head_parent(const JsonValue &json, HeadParent *parent,
                        std::string *error) {
  if (!exact_members(json, {"generation", "etag", "sha256"}, "HeadParent",
                     error) ||
      !read_uint(json, "generation", &parent->generation, error) ||
      !read_string(json, "etag", &parent->etag, error) ||
      !read_string(json, "sha256", &parent->sha256, error)) {
    return false;
  }
  return validate_head_parent(*parent, error);
}

bool decode_manifest_ref(const JsonValue &json, ManifestRef *ref,
                         std::string *error) {
  if (!exact_members(json, {"generation", "key", "size", "sha256"},
                     "ManifestRef", error) ||
      !read_uint(json, "generation", &ref->generation, error) ||
      !read_string(json, "key", &ref->key, error) ||
      !read_uint(json, "size", &ref->size, error) ||
      !read_string(json, "sha256", &ref->sha256, error)) {
    return false;
  }
  ObjectRef object{ref->key, ref->size, ref->sha256};
  if (ref->generation == 0 || !validate_object_ref(object, error)) {
    return ref->generation != 0
               ? false
               : fail(error, "manifest generation must be positive");
  }
  return true;
}

bool decode_recovery_window(const JsonValue &json, RecoveryWindow *window,
                            std::string *error) {
  if (!exact_members(json,
                     {"manifest_count", "manifest_bytes", "segment_count"},
                     "RecoveryWindow", error) ||
      !read_uint(json, "manifest_count", &window->manifest_count, error) ||
      !read_uint(json, "manifest_bytes", &window->manifest_bytes, error) ||
      !read_uint(json, "segment_count", &window->segment_count, error)) {
    return false;
  }
  return validate_recovery_window(*window, error);
}

bool read_nullable_string(const JsonValue &object, std::string_view name,
                          std::optional<std::string> *result,
                          std::string *error) {
  const JsonValue *value = member(object, name);
  if (value == nullptr) return fail(error, std::string(name) + " is required");
  if (value->type == JsonType::kNull) {
    result->reset();
    return true;
  }
  if (value->type != JsonType::kString) {
    return fail(error, std::string(name) + " must be string or null");
  }
  *result = value->string_value;
  return true;
}

bool read_nullable_uint(const JsonValue &object, std::string_view name,
                        std::optional<uint64_t> *result, std::string *error) {
  const JsonValue *value = member(object, name);
  if (value == nullptr) return fail(error, std::string(name) + " is required");
  if (value->type == JsonType::kNull) {
    result->reset();
    return true;
  }
  if (value->type != JsonType::kUint) {
    return fail(error, std::string(name) + " must be integer or null");
  }
  *result = value->uint_value;
  return true;
}

bool decode_segment_tip(const JsonValue &json, SegmentTip *tip,
                        std::string *error) {
  if (!exact_members(json,
                     {"kind", "key", "size", "sha256", "sequence",
                      "snapshot_id", "cursor"},
                     "SegmentTip", error)) {
    return false;
  }
  std::string kind;
  if (!read_string(json, "kind", &kind, error) ||
      !read_nullable_string(json, "key", &tip->key, error) ||
      !read_nullable_uint(json, "size", &tip->size, error) ||
      !read_nullable_string(json, "sha256", &tip->sha256, error) ||
      !read_nullable_uint(json, "sequence", &tip->sequence, error) ||
      !read_nullable_string(json, "snapshot_id", &tip->snapshot_id, error)) {
    return false;
  }
  const JsonValue *cursor = member(json, "cursor");
  if (cursor == nullptr) return fail(error, "segment tip cursor is required");
  if (cursor->type == JsonType::kNull) {
    tip->cursor.reset();
  } else {
    Cursor parsed;
    if (!decode_cursor(*cursor, &parsed, error)) return false;
    tip->cursor = std::move(parsed);
  }

  if (kind == "SEGMENT") {
    tip->kind = SegmentTipKind::SEGMENT;
    if (!tip->key.has_value() || !tip->size.has_value() ||
        !tip->sha256.has_value() || !tip->sequence.has_value() ||
        tip->snapshot_id.has_value() || tip->cursor.has_value()) {
      return fail(error, "SEGMENT tip has an invalid union shape");
    }
    if (*tip->sequence == 0 || !validate_key(*tip->key, error) ||
        !is_sha256(*tip->sha256)) {
      return *tip->sequence == 0
                 ? fail(error, "segment sequence must be positive")
                 : is_sha256(*tip->sha256)
                       ? false
                       : fail(error, "segment tip SHA-256 is invalid");
    }
    return true;
  }
  if (kind == "SNAPSHOT_ROOT") {
    tip->kind = SegmentTipKind::SNAPSHOT_ROOT;
    if (tip->key.has_value() || tip->size.has_value() ||
        tip->sha256.has_value() || tip->sequence.has_value() ||
        !tip->snapshot_id.has_value() || !tip->cursor.has_value()) {
      return fail(error, "SNAPSHOT_ROOT tip has an invalid union shape");
    }
    return validate_snapshot_id(*tip->snapshot_id, error) &&
           validate_cursor(*tip->cursor, error);
  }
  return fail(error, "segment tip kind is unsupported");
}

bool decode_snapshot_ref(const JsonValue &json, SnapshotRef *ref,
                         std::string *error) {
  if (!exact_members(json,
                     {"id", "manifest_key", "manifest_size",
                      "manifest_sha256", "cursor"},
                     "SnapshotRef", error) ||
      !read_string(json, "id", &ref->id, error) ||
      !read_string(json, "manifest_key", &ref->manifest_key, error) ||
      !read_uint(json, "manifest_size", &ref->manifest_size, error) ||
      !read_string(json, "manifest_sha256", &ref->manifest_sha256, error)) {
    return false;
  }
  const JsonValue *cursor = member(json, "cursor");
  if (cursor == nullptr || !decode_cursor(*cursor, &ref->cursor, error)) {
    return false;
  }
  if (!validate_snapshot_id(ref->id, error) ||
      !validate_key(ref->manifest_key, error) ||
      !is_sha256(ref->manifest_sha256)) {
    return is_sha256(ref->manifest_sha256)
               ? false
               : fail(error, "snapshot manifest SHA-256 is invalid");
  }
  return ref->manifest_size > 0
             ? true
             : fail(error, "snapshot manifest size must be positive");
}

bool decode_gtid_digest(const JsonValue &json, GtidSetDigest *digest,
                        std::string *error) {
  if (!exact_members(json, {"canonical", "sha256"}, "GtidSetDigest", error) ||
      !read_string(json, "canonical", &digest->canonical, error) ||
      !read_string(json, "sha256", &digest->sha256, error)) {
    return false;
  }
  GtidSetDigest recomputed;
  if (!gtid_digest(digest->canonical, &recomputed, error)) return false;
  if (recomputed.canonical != digest->canonical ||
      recomputed.sha256 != digest->sha256) {
    return fail(error, "GTID canonical text or digest does not match");
  }
  return true;
}

bool decode_xid_digest(const JsonValue &json, XidDigest *digest,
                       std::string *error) {
  if (!exact_members(json, {"count", "sha256"}, "XidDigest", error) ||
      !read_uint(json, "count", &digest->count, error) ||
      !read_string(json, "sha256", &digest->sha256, error)) {
    return false;
  }
  return is_sha256(digest->sha256)
             ? true
             : fail(error, "XID digest is not lowercase SHA-256 hex");
}

bool decode_segment_source(const JsonValue &json, SegmentSource *source,
                           std::string *error) {
  if (!exact_members(json, {"file", "start_pos", "end_pos"},
                     "SegmentSource", error) ||
      !read_string(json, "file", &source->file, error) ||
      !read_uint(json, "start_pos", &source->start_pos, error) ||
      !read_uint(json, "end_pos", &source->end_pos, error)) {
    return false;
  }
  Cursor start{source->file, source->start_pos};
  if (!validate_cursor(start, error) || source->end_pos <= source->start_pos) {
    return source->end_pos > source->start_pos
               ? false
               : fail(error, "segment source range is empty or reversed");
  }
  return true;
}

bool decode_segment(const JsonValue &json, const StreamIdentity &stream,
                    const Writer &writer, SegmentRef *segment,
                    std::string *error) {
  if (!exact_members(json,
                     {"sequence", "key", "size", "sha256", "source",
                      "previous_segment", "transaction_count", "gtid_set",
                      "xids", "ends_at_transaction_boundary",
                      "payload_format", "compression"},
                     "SegmentRef", error) ||
      !read_uint(json, "sequence", &segment->sequence, error) ||
      !read_string(json, "key", &segment->key, error) ||
      !read_uint(json, "size", &segment->size, error) ||
      !read_string(json, "sha256", &segment->sha256, error) ||
      !read_uint(json, "transaction_count", &segment->transaction_count,
                 error) ||
      !read_bool(json, "ends_at_transaction_boundary",
                 &segment->ends_at_transaction_boundary, error) ||
      !read_string(json, "payload_format", &segment->payload_format, error) ||
      !read_string(json, "compression", &segment->compression, error)) {
    return false;
  }
  const JsonValue *source = member(json, "source");
  const JsonValue *previous = member(json, "previous_segment");
  const JsonValue *gtid = member(json, "gtid_set");
  const JsonValue *xids = member(json, "xids");
  if (source == nullptr || previous == nullptr || gtid == nullptr ||
      xids == nullptr ||
      !decode_segment_source(*source, &segment->source, error) ||
      !decode_segment_tip(*previous, &segment->previous_segment, error) ||
      !decode_gtid_digest(*gtid, &segment->gtid_set, error) ||
      !decode_xid_digest(*xids, &segment->xids, error)) {
    return false;
  }
  if (segment->sequence == 0 || !is_sha256(segment->sha256) ||
      !segment->ends_at_transaction_boundary ||
      segment->payload_format != kSegmentPayloadFormat ||
      segment->compression != kNoCompression ||
      segment->size != segment->source.end_pos - segment->source.start_pos) {
    return fail(error, "segment fields violate the native range contract");
  }
  std::string expected_key;
  if (!segment_object_key(stream, writer, segment->source, segment->sha256,
                          &expected_key, error) ||
      expected_key != segment->key) {
    return expected_key == segment->key
               ? false
               : fail(error, "segment key does not match its canonical fields");
  }
  return true;
}

bool decode_optional_parent(const JsonValue &object, std::string_view name,
                            std::optional<HeadParent> *parent,
                            std::string *error) {
  const JsonValue *json = member(object, name);
  if (json == nullptr) return fail(error, std::string(name) + " is required");
  if (json->type == JsonType::kNull) {
    parent->reset();
    return true;
  }
  HeadParent parsed;
  if (!decode_head_parent(*json, &parsed, error)) return false;
  *parent = std::move(parsed);
  return true;
}

bool decode_optional_manifest_ref(const JsonValue &object,
                                  std::string_view name,
                                  std::optional<ManifestRef> *ref,
                                  std::string *error) {
  const JsonValue *json = member(object, name);
  if (json == nullptr) return fail(error, std::string(name) + " is required");
  if (json->type == JsonType::kNull) {
    ref->reset();
    return true;
  }
  ManifestRef parsed;
  if (!decode_manifest_ref(*json, &parsed, error)) return false;
  *ref = std::move(parsed);
  return true;
}

bool decode_snapshot_object(const JsonValue &json, SnapshotObject *object,
                            std::string *error) {
  if (!exact_members(json,
                     {"component", "ordinal", "relative_path", "key", "size",
                      "sha256", "compression", "format"},
                     "SnapshotObject", error) ||
      !read_string(json, "component", &object->component, error) ||
      !read_uint(json, "ordinal", &object->ordinal, error) ||
      !read_string(json, "relative_path", &object->relative_path, error) ||
      !read_string(json, "key", &object->key, error) ||
      !read_uint(json, "size", &object->size, error) ||
      !read_string(json, "sha256", &object->sha256, error) ||
      !read_string(json, "compression", &object->compression, error) ||
      !read_string(json, "format", &object->format, error)) {
    return false;
  }
  static const std::set<std::string_view> kComponents{
      "innodb", "mysql-dd", "smartengine-meta", "smartengine-wal"};
  if (!kComponents.contains(object->component) ||
      !validate_relative_path(object->relative_path, error) ||
      !validate_key(object->key, error) || !is_sha256(object->sha256) ||
      object->compression != kNoCompression || object->format.empty() ||
      object->format.size() > kMaxOrdinaryIdBytes ||
      !valid_utf8(object->format)) {
    return kComponents.contains(object->component)
               ? false
               : fail(error, "snapshot object component is unsupported");
  }
  return true;
}

bool decode_extent(const JsonValue &json, SmartengineExtentRef *extent,
                   std::string *error) {
  if (!exact_members(json,
                     {"ordinal", "writer_epoch", "allocation_seq",
                      "database_name_hex", "index_id", "object_id", "key",
                      "size", "sha256", "format"},
                     "SmartengineExtentRef", error) ||
      !read_uint(json, "ordinal", &extent->ordinal, error) ||
      !read_uint(json, "writer_epoch", &extent->writer_epoch, error) ||
      !read_string(json, "allocation_seq", &extent->allocation_seq, error) ||
      !read_string(json, "database_name_hex", &extent->database_name_hex,
                   error) ||
      !read_string(json, "index_id", &extent->index_id, error) ||
      !read_string(json, "object_id", &extent->object_id, error) ||
      !read_string(json, "key", &extent->key, error) ||
      !read_uint(json, "size", &extent->size, error) ||
      !read_string(json, "sha256", &extent->sha256, error) ||
      !read_string(json, "format", &extent->format, error)) {
    return false;
  }
  uint64_t allocation = 0;
  uint64_t index = 0;
  uint64_t object = 0;
  if (extent->writer_epoch == 0 ||
      !parse_decimal_uint64(extent->allocation_seq, &allocation, false, error) ||
      !parse_decimal_uint64(extent->index_id, &index, false, error) ||
      !parse_decimal_uint64(extent->object_id, &object, false, error) ||
      !lowercase_hex_decodes_to_utf8(extent->database_name_hex, error) ||
      !validate_key(extent->key, error) || !is_sha256(extent->sha256) ||
      extent->format != kExtentFormat) {
    return extent->writer_epoch != 0
               ? false
               : fail(error, "extent writer_epoch must be positive");
  }
  return true;
}

void append_gtid_digest(const GtidSetDigest &digest, std::string *out) {
  out->append("{\"canonical\":");
  append_json_string(digest.canonical, out);
  out->append(",\"sha256\":");
  append_json_string(digest.sha256, out);
  out->push_back('}');
}

void append_xid_digest(const XidDigest &digest, std::string *out) {
  out->append("{\"count\":");
  append_uint(digest.count, out);
  out->append(",\"sha256\":");
  append_json_string(digest.sha256, out);
  out->push_back('}');
}

void append_segment_source(const SegmentSource &source, std::string *out) {
  out->append("{\"end_pos\":");
  append_uint(source.end_pos, out);
  out->append(",\"file\":");
  append_json_string(source.file, out);
  out->append(",\"start_pos\":");
  append_uint(source.start_pos, out);
  out->push_back('}');
}

void append_segment(const SegmentRef &segment, std::string *out) {
  out->append("{\"compression\":");
  append_json_string(segment.compression, out);
  out->append(",\"ends_at_transaction_boundary\":");
  out->append(segment.ends_at_transaction_boundary ? "true" : "false");
  out->append(",\"gtid_set\":");
  append_gtid_digest(segment.gtid_set, out);
  out->append(",\"key\":");
  append_json_string(segment.key, out);
  out->append(",\"payload_format\":");
  append_json_string(segment.payload_format, out);
  out->append(",\"previous_segment\":");
  append_segment_tip(segment.previous_segment, out);
  out->append(",\"sequence\":");
  append_uint(segment.sequence, out);
  out->append(",\"sha256\":");
  append_json_string(segment.sha256, out);
  out->append(",\"size\":");
  append_uint(segment.size, out);
  out->append(",\"source\":");
  append_segment_source(segment.source, out);
  out->append(",\"transaction_count\":");
  append_uint(segment.transaction_count, out);
  out->append(",\"xids\":");
  append_xid_digest(segment.xids, out);
  out->push_back('}');
}

void append_log_anchor(const LogAnchor &anchor, std::string *out) {
  out->append("{\"cursor\":");
  append_cursor(anchor.cursor, out);
  out->append(",\"generation\":");
  if (anchor.generation.has_value())
    append_uint(*anchor.generation, out);
  else
    out->append("null");
  out->append(",\"kind\":");
  append_json_string(anchor.kind == LogAnchorKind::EMPTY_BASE
                         ? "EMPTY_BASE"
                         : "MANIFEST_BOUNDARY",
                     out);
  out->append(",\"manifest\":");
  if (anchor.manifest.has_value())
    append_object_ref(*anchor.manifest, out);
  else
    out->append("null");
  out->push_back('}');
}

void append_deployment_fingerprints(const DeploymentFingerprints &value,
                                    std::string *out) {
  out->append("{\"keyring_config_sha256\":");
  append_json_string(value.keyring_config_sha256, out);
  out->append(",\"plugin_component_set_sha256\":");
  append_json_string(value.plugin_component_set_sha256, out);
  out->append(",\"server_build\":");
  append_json_string(value.server_build, out);
  out->append(",\"startup_config_sha256\":");
  append_json_string(value.startup_config_sha256, out);
  out->append(",\"tls_config_sha256\":");
  append_json_string(value.tls_config_sha256, out);
  out->push_back('}');
}

void append_binlog_seed(const BinlogSeed &seed, std::string *out) {
  out->append("{\"checksum\":");
  append_json_string(seed.checksum, out);
  out->append(",\"compression\":");
  append_json_string(seed.compression, out);
  out->append(",\"cursor\":");
  append_cursor(seed.cursor, out);
  out->append(",\"file\":");
  append_json_string(seed.file, out);
  out->append(",\"format\":");
  append_json_string(seed.format, out);
  out->append(",\"key\":");
  append_json_string(seed.key, out);
  out->append(",\"sha256\":");
  append_json_string(seed.sha256, out);
  out->append(",\"size\":");
  append_uint(seed.size, out);
  out->push_back('}');
}

void append_snapshot_object(const SnapshotObject &object, std::string *out) {
  out->append("{\"component\":");
  append_json_string(object.component, out);
  out->append(",\"compression\":");
  append_json_string(object.compression, out);
  out->append(",\"format\":");
  append_json_string(object.format, out);
  out->append(",\"key\":");
  append_json_string(object.key, out);
  out->append(",\"ordinal\":");
  append_uint(object.ordinal, out);
  out->append(",\"relative_path\":");
  append_json_string(object.relative_path, out);
  out->append(",\"sha256\":");
  append_json_string(object.sha256, out);
  out->append(",\"size\":");
  append_uint(object.size, out);
  out->push_back('}');
}

void append_extent(const SmartengineExtentRef &extent, std::string *out) {
  out->append("{\"allocation_seq\":");
  append_json_string(extent.allocation_seq, out);
  out->append(",\"database_name_hex\":");
  append_json_string(extent.database_name_hex, out);
  out->append(",\"format\":");
  append_json_string(extent.format, out);
  out->append(",\"index_id\":");
  append_json_string(extent.index_id, out);
  out->append(",\"key\":");
  append_json_string(extent.key, out);
  out->append(",\"object_id\":");
  append_json_string(extent.object_id, out);
  out->append(",\"ordinal\":");
  append_uint(extent.ordinal, out);
  out->append(",\"sha256\":");
  append_json_string(extent.sha256, out);
  out->append(",\"size\":");
  append_uint(extent.size, out);
  out->append(",\"writer_epoch\":");
  append_uint(extent.writer_epoch, out);
  out->push_back('}');
}

SegmentTip segment_as_tip(const SegmentRef &segment) {
  SegmentTip tip;
  tip.kind = SegmentTipKind::SEGMENT;
  tip.key = segment.key;
  tip.size = segment.size;
  tip.sha256 = segment.sha256;
  tip.sequence = segment.sequence;
  return tip;
}

bool validate_segment_tip(const SegmentTip &tip, std::string *error) {
  if (tip.kind == SegmentTipKind::SEGMENT) {
    if (!tip.key.has_value() || !tip.size.has_value() ||
        !tip.sha256.has_value() || !tip.sequence.has_value() ||
        tip.snapshot_id.has_value() || tip.cursor.has_value() ||
        *tip.size == 0 || *tip.size > kJsonSafeIntegerMax ||
        *tip.sequence == 0 || *tip.sequence > kJsonSafeIntegerMax ||
        !validate_key(*tip.key, error) ||
        !is_sha256(*tip.sha256)) {
      return fail(error, "invalid SEGMENT tip");
    }
    return true;
  }
  if (tip.key.has_value() || tip.size.has_value() || tip.sha256.has_value() ||
      tip.sequence.has_value() || !tip.snapshot_id.has_value() ||
      !tip.cursor.has_value() ||
      !validate_snapshot_id(*tip.snapshot_id, error) ||
      !validate_cursor(*tip.cursor, error)) {
    return fail(error, "invalid SNAPSHOT_ROOT tip");
  }
  return true;
}

bool validate_snapshot_ref(const StreamIdentity &stream, const SnapshotRef &ref,
                           std::string *error) {
  if (!validate_snapshot_id(ref.id, error) ||
      ref.manifest_size == 0 || ref.manifest_size > kJsonSafeIntegerMax ||
      !is_sha256(ref.manifest_sha256) || !validate_cursor(ref.cursor, error)) {
    return ref.manifest_size > 0 && ref.manifest_size <= kJsonSafeIntegerMax
               ? false
               : fail(error, "snapshot manifest size exceeds safe integer");
  }
  std::string expected;
  return snapshot_manifest_key(stream, ref.id, ref.manifest_sha256, &expected,
                               error) &&
                 expected == ref.manifest_key
             ? true
             : expected == ref.manifest_key
                   ? false
                   : fail(error, "snapshot ref manifest key is not canonical");
}

bool validate_manifest_ref_key(const StreamIdentity &stream,
                               const ManifestRef &ref, std::string *error) {
  const std::string prefix = stream.remote_prefix + "/manifests/e";
  if (!ref.key.starts_with(prefix) || !ref.key.ends_with(".json")) {
    return fail(error, "previous manifest key has the wrong namespace");
  }
  const size_t slash = ref.key.find('/', prefix.size());
  if (slash == std::string::npos || slash == prefix.size()) {
    return fail(error, "previous manifest key has no epoch");
  }
  uint64_t epoch = 0;
  if (!parse_decimal_uint64(
          std::string_view(ref.key).substr(prefix.size(), slash - prefix.size()),
          &epoch, false, error) ||
      epoch == 0 || epoch > kJsonSafeIntegerMax) {
    return fail(error, "previous manifest key epoch is invalid");
  }
  const std::string suffix =
      "g" + std::to_string(ref.generation) + "-" + ref.sha256 + ".json";
  return std::string_view(ref.key).substr(slash + 1) == suffix
             ? true
             : fail(error, "previous manifest key generation/SHA mismatch");
}

bool validate_head_value(const StreamIdentity &stream, const Head &head,
                         std::string *error) {
  if (head.state != "READY" || head.generation == 0 ||
      head.generation > kJsonSafeIntegerMax ||
      !validate_writer(head.writer, error) ||
      !validate_object_ref(head.manifest, error) ||
      !validate_recovery_window(head.recovery_window, error) ||
      !validate_segment_tip(head.segment_tip, error) ||
      !validate_cursor(head.base_cursor, error) ||
      !validate_cursor(head.durable_cursor, error) ||
      !validate_snapshot_ref(stream, head.snapshot, error)) {
    return head.state == "READY"
               ? false
               : fail(error, "HEAD state must be READY");
  }
  if ((head.generation == 1) != !head.parent.has_value()) {
    return fail(error, "HEAD parent must be null exactly at generation 1");
  }
  if (head.parent.has_value() &&
      (!validate_head_parent(*head.parent, error) ||
       head.parent->generation + 1 != head.generation)) {
    return fail(error, "HEAD parent generation is not generation-1");
  }
  std::string expected_manifest;
  if (!transition_manifest_key(stream, head.writer, head.generation,
                               head.manifest.sha256, &expected_manifest, error) ||
      expected_manifest != head.manifest.key) {
    return expected_manifest == head.manifest.key
               ? false
               : fail(error, "HEAD manifest key is not canonical");
  }
  int order = 0;
  if (!cursor_order(head.base_cursor, head.durable_cursor, &order, error) ||
      order > 0 || head.snapshot.cursor != head.base_cursor) {
    return fail(error, "HEAD cursor/snapshot ordering is invalid");
  }
  if (head.segment_tip.kind == SegmentTipKind::SNAPSHOT_ROOT &&
      *head.segment_tip.cursor != head.snapshot.cursor) {
    return fail(error, "HEAD snapshot root cursor does not match snapshot ref");
  }
  return true;
}

bool validate_segment_chain(const StreamIdentity &stream, const Writer &writer,
                            const std::vector<SegmentRef> &segments,
                            std::string *error) {
  SegmentTip prior;
  bool have_prior = false;
  std::set<std::string> keys;
  for (size_t index = 0; index < segments.size(); ++index) {
    const SegmentRef &segment = segments[index];
    if (!validate_segment_tip(segment.previous_segment, error) ||
        segment.sequence == 0 || segment.size == 0 ||
        segment.source.end_pos <= segment.source.start_pos ||
        segment.size != segment.source.end_pos - segment.source.start_pos ||
        !is_sha256(segment.sha256) ||
        !segment.ends_at_transaction_boundary ||
        segment.payload_format != kSegmentPayloadFormat ||
        segment.compression != kNoCompression) {
      return fail(error, "segment violates the native range schema");
    }
    GtidSetDigest recomputed;
    if (!gtid_digest(segment.gtid_set.canonical, &recomputed, error) ||
        recomputed != segment.gtid_set || !is_sha256(segment.xids.sha256)) {
      return fail(error, "segment GTID/XID digest is invalid");
    }
    std::string expected_key;
    if (!segment_object_key(stream, writer, segment.source, segment.sha256,
                            &expected_key, error) ||
        expected_key != segment.key || !keys.insert(segment.key).second) {
      return fail(error, "segment key is derived incorrectly or duplicated");
    }
    if (segment.previous_segment.kind == SegmentTipKind::SEGMENT) {
      if (*segment.previous_segment.sequence + 1 != segment.sequence) {
        return fail(error, "segment sequence is not previous+1");
      }
    } else if (segment.sequence != 1) {
      return fail(error, "first segment after a snapshot root must be sequence 1");
    }
    if (have_prior && segment.previous_segment != prior) {
      return fail(error, "segment previous_segment does not match prior item");
    }
    if (index > 0) {
      const SegmentRef &previous = segments[index - 1];
      std::string previous_basename;
      std::string current_basename;
      uint64_t previous_file = 0;
      uint64_t current_file = 0;
      if (!parse_binlog_file(previous.source.file, &previous_basename,
                             &previous_file, error) ||
          !parse_binlog_file(segment.source.file, &current_basename,
                             &current_file, error) ||
          previous_basename != current_basename) {
        return fail(error, "adjacent segment binlog streams differ");
      }
      if (previous_file == current_file) {
        if (segment.source.start_pos != previous.source.end_pos) {
          return fail(error, "same-file segments contain a gap or overlap");
        }
      } else if (current_file != previous_file + 1 ||
                 segment.source.start_pos != 0) {
        return fail(error, "cross-file segments are not adjacent at position 0");
      }
    }
    prior = segment_as_tip(segment);
    have_prior = true;
  }
  return true;
}

bool validate_transition_value(const StreamIdentity &stream,
                               const TransitionManifest &manifest,
                               std::string *error) {
  if (manifest.generation == 0 || manifest.generation > kJsonSafeIntegerMax ||
      !validate_writer(manifest.writer, error) ||
      !validate_recovery_window(manifest.recovery_window, error) ||
      !validate_segment_tip(manifest.segment_tip, error) ||
      !validate_snapshot_ref(stream, manifest.snapshot, error) ||
      !validate_cursor(manifest.base_cursor, error) ||
      !validate_cursor(manifest.durable_cursor, error) ||
      manifest.segments.size() > kMaxSegmentsPerManifest) {
    return false;
  }
  int base_durable_order = 0;
  if (!cursor_order(manifest.base_cursor, manifest.durable_cursor,
                    &base_durable_order, error) ||
      base_durable_order > 0 || manifest.snapshot.cursor != manifest.base_cursor) {
    return fail(error, "manifest snapshot/base/durable cursor relation is invalid");
  }

  const bool bootstrap = manifest.kind == ManifestKind::BOOTSTRAP;
  if (bootstrap) {
    if (manifest.generation != 1 || manifest.head_parent.has_value() ||
        manifest.previous.has_value() || !manifest.segments.empty() ||
        manifest.base_cursor != manifest.durable_cursor ||
        manifest.recovery_window.manifest_count != 1 ||
        manifest.recovery_window.segment_count != 0 ||
        manifest.segment_tip.kind != SegmentTipKind::SNAPSHOT_ROOT ||
        *manifest.segment_tip.snapshot_id != manifest.snapshot.id ||
        *manifest.segment_tip.cursor != manifest.snapshot.cursor) {
      return fail(error, "BOOTSTRAP manifest invariants are invalid");
    }
    return true;
  }

  if (manifest.generation <= 1 || !manifest.head_parent.has_value() ||
      !manifest.previous.has_value() ||
      !validate_head_parent(*manifest.head_parent, error) ||
      manifest.head_parent->generation + 1 != manifest.generation ||
      manifest.previous->generation + 1 != manifest.generation ||
      !validate_manifest_ref_key(stream, *manifest.previous, error)) {
    return fail(error, "non-bootstrap parent/previous relation is invalid");
  }
  if (manifest.kind == ManifestKind::LOG_TRANSITION) {
    if (manifest.segments.empty() ||
        !validate_segment_chain(stream, manifest.writer, manifest.segments,
                                error) ||
        manifest.segment_tip != segment_as_tip(manifest.segments.back())) {
      return fail(error, "LOG manifest segment chain/tip is invalid");
    }
    const SegmentTip &first_previous = manifest.segments.front().previous_segment;
    if (first_previous.kind == SegmentTipKind::SNAPSHOT_ROOT &&
        (*first_previous.cursor != manifest.snapshot.cursor ||
         manifest.segments.front().source.file != first_previous.cursor->file ||
         manifest.segments.front().source.start_pos !=
             first_previous.cursor->pos)) {
      return fail(error, "LOG root predecessor cursor is not contiguous");
    }
    const SegmentRef &last = manifest.segments.back();
    if (manifest.durable_cursor != Cursor{last.source.file, last.source.end_pos} ||
        manifest.recovery_window.segment_count < manifest.segments.size()) {
      return fail(error, "LOG durable cursor/window does not cover its segments");
    }
    return true;
  }
  if (manifest.kind == ManifestKind::SNAPSHOT) {
    return manifest.segments.empty()
               ? true
               : fail(error, "SNAPSHOT manifest must have an empty delta");
  }
  return fail(error, "unsupported transition manifest kind");
}

bool validate_snapshot_value(const StreamIdentity &stream,
                             const SnapshotManifest &snapshot,
                             std::string *error) {
  if (!validate_snapshot_id(snapshot.snapshot_id, error) ||
      !validate_writer(snapshot.writer, error) ||
      !validate_cursor(snapshot.cursor, error) ||
      !validate_cursor(snapshot.log_anchor.cursor, error) ||
      snapshot.log_anchor.cursor != snapshot.cursor ||
      snapshot.binlog_seed.cursor != snapshot.cursor ||
      snapshot.binlog_seed.file != snapshot.cursor.file ||
      snapshot.binlog_seed.size != snapshot.cursor.pos ||
      snapshot.binlog_seed.compression != kNoCompression ||
      snapshot.binlog_seed.format != kSeedPayloadFormat ||
      snapshot.binlog_seed.checksum != kChecksumCrc32 ||
      !is_sha256(snapshot.binlog_seed.sha256)) {
    return fail(error, "snapshot cursor/binlog seed relation is invalid");
  }
  if (snapshot.log_anchor.kind == LogAnchorKind::EMPTY_BASE) {
    if (snapshot.log_anchor.generation.has_value() ||
        snapshot.log_anchor.manifest.has_value()) {
      return fail(error, "EMPTY_BASE anchor must contain explicit nulls");
    }
  } else {
    if (!snapshot.log_anchor.generation.has_value() ||
        !snapshot.log_anchor.manifest.has_value() ||
        *snapshot.log_anchor.generation == 0 ||
        !validate_object_ref(*snapshot.log_anchor.manifest, error)) {
      return fail(error, "MANIFEST_BOUNDARY anchor is incomplete");
    }
    ManifestRef ref{*snapshot.log_anchor.generation,
                    snapshot.log_anchor.manifest->key,
                    snapshot.log_anchor.manifest->size,
                    snapshot.log_anchor.manifest->sha256};
    if (!validate_manifest_ref_key(stream, ref, error)) return false;
  }
  std::array<unsigned char, 16> uuid{};
  if (!parse_uuid(snapshot.server_identity.server_uuid, &uuid, true, error) ||
      !is_sha256(snapshot.deployment_fingerprints.startup_config_sha256) ||
      !is_sha256(
          snapshot.deployment_fingerprints.plugin_component_set_sha256) ||
      !is_sha256(snapshot.deployment_fingerprints.keyring_config_sha256) ||
      !is_sha256(snapshot.deployment_fingerprints.tls_config_sha256) ||
      snapshot.deployment_fingerprints.server_build.empty() ||
      snapshot.deployment_fingerprints.server_build.size() >
          kMaxOrdinaryIdBytes ||
      !valid_utf8(snapshot.deployment_fingerprints.server_build)) {
    return fail(error, "snapshot identity/fingerprint fields are invalid");
  }
  GtidSetDigest expected_gtid;
  if (!gtid_digest(snapshot.gtid_executed.canonical, &expected_gtid, error) ||
      expected_gtid != snapshot.gtid_executed) {
    return fail(error, "snapshot gtid_executed digest is invalid");
  }
  std::string expected_seed_key;
  if (!binlog_seed_object_key(stream, snapshot.snapshot_id, snapshot.cursor,
                              snapshot.binlog_seed.sha256, &expected_seed_key,
                              error) ||
      expected_seed_key != snapshot.binlog_seed.key) {
    return fail(error, "snapshot binlog seed key is not canonical");
  }
  if (snapshot.objects.size() + snapshot.smartengine_extents.size() >
      kMaxSnapshotItems) {
    return fail(error, "snapshot arrays exceed the combined item limit");
  }

  std::set<std::pair<std::string, std::string>> materialized_tuples;
  std::set<std::string> materialized_keys;
  std::string prior_component;
  std::string prior_path;
  uint64_t expected_ordinal = 0;
  for (size_t index = 0; index < snapshot.objects.size(); ++index) {
    const SnapshotObject &object = snapshot.objects[index];
    static const std::set<std::string_view> kComponents{
        "innodb", "mysql-dd", "smartengine-meta", "smartengine-wal"};
    if (!kComponents.contains(object.component) ||
        !validate_relative_path(object.relative_path, error) ||
        object.size > kJsonSafeIntegerMax || !is_sha256(object.sha256) ||
        object.compression != kNoCompression ||
        object.format.empty() || object.format.size() > kMaxOrdinaryIdBytes ||
        !valid_utf8(object.format)) {
      return fail(error, "snapshot materialized object is invalid");
    }
    if (index == 0 || object.component != prior_component) {
      if (index != 0 && object.component < prior_component) {
        return fail(error, "snapshot objects are not component-sorted");
      }
      expected_ordinal = 0;
      prior_path.clear();
    } else if (object.relative_path <= prior_path) {
      return fail(error, "snapshot paths are duplicated or out of order");
    }
    if (object.ordinal != expected_ordinal++) {
      return fail(error, "snapshot object ordinal contains a gap");
    }
    std::string expected_key;
    if (!snapshot_object_key(stream, snapshot.snapshot_id, object,
                             &expected_key, error) ||
        object.key != expected_key ||
        !materialized_tuples.emplace(object.component, object.relative_path)
             .second ||
        !materialized_keys.insert(object.key).second) {
      return fail(error, "snapshot object key/tuple is invalid or duplicated");
    }
    prior_component = object.component;
    prior_path = object.relative_path;
  }

  using ExtentOrder =
      std::tuple<uint64_t, uint64_t, std::string, uint64_t, uint64_t,
                 std::string>;
  std::optional<ExtentOrder> prior_extent;
  std::set<std::tuple<uint64_t, std::string, std::string, std::string,
                      std::string>>
      extent_tuples;
  std::set<std::string> extent_keys;
  for (size_t index = 0; index < snapshot.smartengine_extents.size(); ++index) {
    const SmartengineExtentRef &extent = snapshot.smartengine_extents[index];
    uint64_t allocation = 0;
    uint64_t index_id = 0;
    uint64_t object_id = 0;
    if (extent.ordinal != index || extent.writer_epoch == 0 ||
        extent.writer_epoch > snapshot.writer.epoch ||
        !parse_decimal_uint64(extent.allocation_seq, &allocation, false,
                              error) ||
        !parse_decimal_uint64(extent.index_id, &index_id, false, error) ||
        !parse_decimal_uint64(extent.object_id, &object_id, false, error) ||
        !lowercase_hex_decodes_to_utf8(extent.database_name_hex, error) ||
        !is_sha256(extent.sha256) || extent.size == 0 ||
        extent.size > kJsonSafeIntegerMax ||
        extent.format != kExtentFormat) {
      return fail(error, "SmartEngine extent ref is invalid");
    }
    ExtentOrder order{extent.writer_epoch, allocation,
                      extent.database_name_hex, index_id, object_id, extent.key};
    if (prior_extent.has_value() && order <= *prior_extent) {
      return fail(error, "SmartEngine extent refs are not canonically sorted");
    }
    std::string expected_key;
    if (!smartengine_extent_object_key(stream, extent, &expected_key, error) ||
        expected_key != extent.key ||
        !extent_tuples
             .emplace(extent.writer_epoch, extent.allocation_seq,
                      extent.database_name_hex, extent.index_id,
                      extent.object_id)
             .second ||
        !extent_keys.insert(extent.key).second) {
      return fail(error, "SmartEngine extent key/tuple is invalid or duplicated");
    }
    prior_extent = std::move(order);
  }
  return true;
}

bool decode_writer_epoch_value(const JsonValue &json,
                               const StreamIdentity &stream,
                               WriterEpoch *value, std::string *error) {
  if (!exact_members(json,
                     {"format", "version", "stream_id", "epoch", "writer_id",
                      "previous_epoch"},
                     "WriterEpoch", error) ||
      !require_literal(json, "format", kWriterEpochFormat, error) ||
      !require_version(json, error) || !require_stream(json, stream, error) ||
      !read_uint(json, "epoch", &value->epoch, error) ||
      !read_string(json, "writer_id", &value->writer_id, error) ||
      !read_uint(json, "previous_epoch", &value->previous_epoch, error)) {
    return false;
  }
  if (value->epoch == 0 || !is_lower_hex(value->writer_id, 32) ||
      value->previous_epoch + 1 != value->epoch) {
    return fail(error, "WRITER_EPOCH monotonic fields are invalid");
  }
  return true;
}

bool decode_head_value(const JsonValue &json, const StreamIdentity &stream,
                       Head *head, std::string *error) {
  if (!exact_members(json,
                     {"format", "version", "stream_id", "state", "generation",
                      "writer", "parent", "manifest", "recovery_window",
                      "segment_tip", "base_cursor", "durable_cursor",
                      "snapshot"},
                     "Head", error) ||
      !require_literal(json, "format", kHeadFormat, error) ||
      !require_version(json, error) || !require_stream(json, stream, error) ||
      !read_string(json, "state", &head->state, error) ||
      !read_uint(json, "generation", &head->generation, error) ||
      !decode_optional_parent(json, "parent", &head->parent, error)) {
    return false;
  }
  const JsonValue *writer = member(json, "writer");
  const JsonValue *manifest = member(json, "manifest");
  const JsonValue *window = member(json, "recovery_window");
  const JsonValue *tip = member(json, "segment_tip");
  const JsonValue *base = member(json, "base_cursor");
  const JsonValue *durable = member(json, "durable_cursor");
  const JsonValue *snapshot = member(json, "snapshot");
  return writer != nullptr && manifest != nullptr && window != nullptr &&
                 tip != nullptr && base != nullptr && durable != nullptr &&
                 snapshot != nullptr &&
                 decode_writer(*writer, &head->writer, error) &&
                 decode_object_ref(*manifest, &head->manifest, error) &&
                 decode_recovery_window(*window, &head->recovery_window,
                                        error) &&
                 decode_segment_tip(*tip, &head->segment_tip, error) &&
                 decode_cursor(*base, &head->base_cursor, error) &&
                 decode_cursor(*durable, &head->durable_cursor, error) &&
                 decode_snapshot_ref(*snapshot, &head->snapshot, error) &&
                 validate_head_value(stream, *head, error)
             ? true
             : error != nullptr && !error->empty()
                   ? false
                   : fail(error, "HEAD has a missing nested value");
}

bool decode_transition_value(const JsonValue &json,
                             const StreamIdentity &stream,
                             TransitionManifest *manifest,
                             std::string *error) {
  if (!exact_members(json,
                     {"format", "version", "stream_id", "kind", "generation",
                      "writer", "head_parent", "previous", "recovery_window",
                      "segment_tip", "snapshot", "base_cursor",
                      "durable_cursor", "segments"},
                     "TransitionManifest", error) ||
      !require_literal(json, "format", kManifestFormat, error) ||
      !require_version(json, error) || !require_stream(json, stream, error) ||
      !read_uint(json, "generation", &manifest->generation, error) ||
      !decode_optional_parent(json, "head_parent", &manifest->head_parent,
                              error) ||
      !decode_optional_manifest_ref(json, "previous", &manifest->previous,
                                    error)) {
    return false;
  }
  std::string kind;
  if (!read_string(json, "kind", &kind, error)) return false;
  if (kind == "BOOTSTRAP")
    manifest->kind = ManifestKind::BOOTSTRAP;
  else if (kind == "LOG")
    manifest->kind = ManifestKind::LOG_TRANSITION;
  else if (kind == "SNAPSHOT")
    manifest->kind = ManifestKind::SNAPSHOT;
  else
    return fail(error, "transition manifest kind is unsupported");

  const JsonValue *writer = member(json, "writer");
  const JsonValue *window = member(json, "recovery_window");
  const JsonValue *tip = member(json, "segment_tip");
  const JsonValue *snapshot = member(json, "snapshot");
  const JsonValue *base = member(json, "base_cursor");
  const JsonValue *durable = member(json, "durable_cursor");
  const JsonValue *segments = member(json, "segments");
  if (writer == nullptr || window == nullptr || tip == nullptr ||
      snapshot == nullptr || base == nullptr || durable == nullptr ||
      segments == nullptr || segments->type != JsonType::kArray ||
      !decode_writer(*writer, &manifest->writer, error) ||
      !decode_recovery_window(*window, &manifest->recovery_window, error) ||
      !decode_segment_tip(*tip, &manifest->segment_tip, error) ||
      !decode_snapshot_ref(*snapshot, &manifest->snapshot, error) ||
      !decode_cursor(*base, &manifest->base_cursor, error) ||
      !decode_cursor(*durable, &manifest->durable_cursor, error)) {
    return error != nullptr && !error->empty()
               ? false
               : fail(error, "transition manifest has an invalid nested value");
  }
  manifest->segments.clear();
  manifest->segments.reserve(segments->array_values.size());
  for (const JsonValue &segment_json : segments->array_values) {
    SegmentRef segment;
    if (!decode_segment(segment_json, stream, manifest->writer, &segment,
                        error)) {
      return false;
    }
    manifest->segments.push_back(std::move(segment));
  }
  return validate_transition_value(stream, *manifest, error);
}

bool decode_log_anchor(const JsonValue &json, LogAnchor *anchor,
                       std::string *error) {
  if (!exact_members(json, {"kind", "generation", "manifest", "cursor"},
                     "LogAnchor", error)) {
    return false;
  }
  std::string kind;
  if (!read_string(json, "kind", &kind, error)) return false;
  const JsonValue *generation = member(json, "generation");
  const JsonValue *manifest = member(json, "manifest");
  const JsonValue *cursor = member(json, "cursor");
  if (generation == nullptr || manifest == nullptr || cursor == nullptr ||
      !decode_cursor(*cursor, &anchor->cursor, error)) {
    return false;
  }
  if (generation->type == JsonType::kNull)
    anchor->generation.reset();
  else if (generation->type == JsonType::kUint)
    anchor->generation = generation->uint_value;
  else
    return fail(error, "anchor generation must be integer or null");
  if (manifest->type == JsonType::kNull) {
    anchor->manifest.reset();
  } else {
    ObjectRef parsed;
    if (!decode_object_ref(*manifest, &parsed, error)) return false;
    anchor->manifest = std::move(parsed);
  }
  if (kind == "EMPTY_BASE")
    anchor->kind = LogAnchorKind::EMPTY_BASE;
  else if (kind == "MANIFEST_BOUNDARY")
    anchor->kind = LogAnchorKind::MANIFEST_BOUNDARY;
  else
    return fail(error, "log anchor kind is unsupported");
  return true;
}

bool decode_deployment_fingerprints(const JsonValue &json,
                                    DeploymentFingerprints *fingerprints,
                                    std::string *error) {
  return exact_members(json,
                       {"startup_config_sha256", "server_build",
                        "plugin_component_set_sha256",
                        "keyring_config_sha256", "tls_config_sha256"},
                       "DeploymentFingerprints", error) &&
         read_string(json, "startup_config_sha256",
                     &fingerprints->startup_config_sha256, error) &&
         read_string(json, "server_build", &fingerprints->server_build,
                     error) &&
         read_string(json, "plugin_component_set_sha256",
                     &fingerprints->plugin_component_set_sha256, error) &&
         read_string(json, "keyring_config_sha256",
                     &fingerprints->keyring_config_sha256, error) &&
         read_string(json, "tls_config_sha256",
                     &fingerprints->tls_config_sha256, error);
}

bool decode_binlog_seed(const JsonValue &json, BinlogSeed *seed,
                        std::string *error) {
  if (!exact_members(json,
                     {"file", "cursor", "key", "size", "sha256",
                      "compression", "format", "checksum"},
                     "BinlogSeed", error) ||
      !read_string(json, "file", &seed->file, error) ||
      !read_string(json, "key", &seed->key, error) ||
      !read_uint(json, "size", &seed->size, error) ||
      !read_string(json, "sha256", &seed->sha256, error) ||
      !read_string(json, "compression", &seed->compression, error) ||
      !read_string(json, "format", &seed->format, error) ||
      !read_string(json, "checksum", &seed->checksum, error)) {
    return false;
  }
  const JsonValue *cursor = member(json, "cursor");
  return cursor != nullptr && decode_cursor(*cursor, &seed->cursor, error)
             ? true
             : fail(error, "binlog seed cursor is missing");
}

bool decode_snapshot_value(const JsonValue &json, const StreamIdentity &stream,
                           SnapshotManifest *snapshot, std::string *error) {
  if (!exact_members(json,
                     {"format", "version", "stream_id", "snapshot_id",
                      "writer", "cursor", "log_anchor", "server_identity",
                      "deployment_fingerprints", "gtid_executed", "binlog_seed",
                      "objects", "smartengine_extents"},
                     "SnapshotManifest", error) ||
      !require_literal(json, "format", kSnapshotFormat, error) ||
      !require_version(json, error) || !require_stream(json, stream, error) ||
      !read_string(json, "snapshot_id", &snapshot->snapshot_id, error)) {
    return false;
  }
  const JsonValue *writer = member(json, "writer");
  const JsonValue *cursor = member(json, "cursor");
  const JsonValue *anchor = member(json, "log_anchor");
  const JsonValue *server = member(json, "server_identity");
  const JsonValue *fingerprints = member(json, "deployment_fingerprints");
  const JsonValue *gtid = member(json, "gtid_executed");
  const JsonValue *seed = member(json, "binlog_seed");
  const JsonValue *objects = member(json, "objects");
  const JsonValue *extents = member(json, "smartengine_extents");
  if (writer == nullptr || cursor == nullptr || anchor == nullptr ||
      server == nullptr || fingerprints == nullptr || gtid == nullptr ||
      seed == nullptr || objects == nullptr || extents == nullptr ||
      objects->type != JsonType::kArray || extents->type != JsonType::kArray ||
      !decode_writer(*writer, &snapshot->writer, error) ||
      !decode_cursor(*cursor, &snapshot->cursor, error) ||
      !decode_log_anchor(*anchor, &snapshot->log_anchor, error) ||
      !exact_members(*server, {"server_uuid"}, "ServerIdentity", error) ||
      !read_string(*server, "server_uuid",
                   &snapshot->server_identity.server_uuid, error) ||
      !decode_deployment_fingerprints(
          *fingerprints, &snapshot->deployment_fingerprints, error) ||
      !decode_gtid_digest(*gtid, &snapshot->gtid_executed, error) ||
      !decode_binlog_seed(*seed, &snapshot->binlog_seed, error)) {
    return error != nullptr && !error->empty()
               ? false
               : fail(error, "snapshot manifest has an invalid nested value");
  }
  if (objects->array_values.size() + extents->array_values.size() >
      kMaxSnapshotItems) {
    return fail(error, "snapshot arrays exceed the combined item limit");
  }
  snapshot->objects.clear();
  snapshot->objects.reserve(objects->array_values.size());
  for (const JsonValue &item : objects->array_values) {
    SnapshotObject object;
    if (!decode_snapshot_object(item, &object, error)) return false;
    snapshot->objects.push_back(std::move(object));
  }
  snapshot->smartengine_extents.clear();
  snapshot->smartengine_extents.reserve(extents->array_values.size());
  for (const JsonValue &item : extents->array_values) {
    SmartengineExtentRef extent;
    if (!decode_extent(item, &extent, error)) return false;
    snapshot->smartengine_extents.push_back(std::move(extent));
  }
  return validate_snapshot_value(stream, *snapshot, error);
}

}  // namespace

bool sha256_hex(std::string_view bytes, std::string *digest,
                std::string *error) {
  if (digest == nullptr) return fail(error, "null SHA-256 result");
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (context == nullptr) return fail(error, "cannot allocate SHA-256 context");
  std::array<unsigned char, EVP_MAX_MD_SIZE> output{};
  unsigned int output_length = 0;
  const bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1 &&
                  EVP_DigestUpdate(context, bytes.data(), bytes.size()) == 1 &&
                  EVP_DigestFinal_ex(context, output.data(), &output_length) ==
                      1;
  EVP_MD_CTX_free(context);
  if (!ok || output_length != 32) {
    return fail(error, "OpenSSL SHA-256 operation failed");
  }
  static constexpr char kHex[] = "0123456789abcdef";
  digest->clear();
  digest->reserve(64);
  for (size_t i = 0; i < output_length; ++i) {
    digest->push_back(kHex[output[i] >> 4]);
    digest->push_back(kHex[output[i] & 0x0f]);
  }
  return true;
}

bool build_stream_identity(std::string_view repo_id,
                           std::string_view branch_id,
                           std::string_view cluster_object_prefix,
                           StreamIdentity *result, std::string *error) {
  if (result == nullptr) return fail(error, "null StreamIdentity result");
  if (!is_ascii_component(repo_id) || !is_ascii_component(branch_id)) {
    return fail(error, "repo and branch ids must be canonical ASCII components");
  }
  if (!is_cluster_prefix(cluster_object_prefix)) {
    return fail(error, "cluster object prefix is not canonical");
  }
  StreamIdentity built;
  built.repo_id = std::string(repo_id);
  built.branch_id = std::string(branch_id);
  built.cluster_object_prefix = std::string(cluster_object_prefix);
  built.stream_id = "r=" + built.repo_id + "/b=" + built.branch_id;
  built.remote_prefix = built.stream_id + "/remote-commit/v2";
  std::string preimage = "[\"wesql.remote_commit.stream\",2,";
  append_json_string(repo_id, &preimage);
  preimage.push_back(',');
  append_json_string(branch_id, &preimage);
  preimage.push_back(']');
  if (!sha256_hex(preimage, &built.stream_sha256, error)) return false;
  built.extent_prefix = built.cluster_object_prefix +
                        "/smartengine/v2/extents/s" + built.stream_sha256;
  if (built.stream_id.size() > 101 ||
      built.remote_prefix.size() > kMaxObjectKeyBytes ||
      built.extent_prefix.size() > kMaxObjectKeyBytes) {
    return fail(error, "derived stream namespace exceeds its size limit");
  }
  *result = std::move(built);
  return true;
}

bool transition_manifest_key(const StreamIdentity &stream, const Writer &writer,
                             uint64_t generation, std::string_view body_sha256,
                             std::string *key, std::string *error) {
  if (key == nullptr || !validate_stream_identity(stream, error) ||
      !validate_writer(writer, error) || generation == 0 ||
      generation > kJsonSafeIntegerMax || !is_sha256(body_sha256)) {
    return key == nullptr
               ? fail(error, "null transition manifest key result")
               : generation == 0 || generation > kJsonSafeIntegerMax
                     ? fail(error, "manifest generation is invalid")
                     : is_sha256(body_sha256)
                           ? false
                           : fail(error, "manifest body SHA-256 is invalid");
  }
  *key = stream.remote_prefix + "/manifests/e" +
         std::to_string(writer.epoch) + "/g" + std::to_string(generation) +
         "-" + std::string(body_sha256) + ".json";
  return validate_key(*key, error);
}

bool segment_object_key(const StreamIdentity &stream, const Writer &writer,
                        const SegmentSource &source,
                        std::string_view body_sha256, std::string *key,
                        std::string *error) {
  if (key == nullptr || !validate_stream_identity(stream, error) ||
      !validate_writer(writer, error) || !is_sha256(body_sha256) ||
      source.end_pos <= source.start_pos ||
      source.end_pos > kJsonSafeIntegerMax) {
    return fail(error, "invalid segment key input");
  }
  std::string basename;
  uint64_t file_sequence = 0;
  std::string file20;
  std::string start20;
  std::string end20;
  if (!parse_binlog_file(source.file, &basename, &file_sequence, error) ||
      !format_padded_20(file_sequence, &file20, error) ||
      !format_padded_20(source.start_pos, &start20, error) ||
      !format_padded_20(source.end_pos, &end20, error)) {
    return false;
  }
  *key = stream.remote_prefix + "/binlog/segments/e" +
         std::to_string(writer.epoch) + "/" + writer.id + "/" + file20 +
         "/" + start20 + "-" + end20 + "-" + std::string(body_sha256) +
         ".seg";
  return validate_key(*key, error);
}

bool snapshot_manifest_key(const StreamIdentity &stream,
                           std::string_view snapshot_id,
                           std::string_view body_sha256, std::string *key,
                           std::string *error) {
  if (key == nullptr || !validate_stream_identity(stream, error) ||
      !validate_snapshot_id(snapshot_id, error) || !is_sha256(body_sha256)) {
    return key == nullptr ? fail(error, "null snapshot manifest key result")
                          : fail(error, "invalid snapshot manifest key input");
  }
  *key = stream.remote_prefix + "/snapshots/manifests/" +
         std::string(snapshot_id) + "-" + std::string(body_sha256) + ".json";
  return validate_key(*key, error);
}

bool binlog_seed_object_key(const StreamIdentity &stream,
                            std::string_view snapshot_id,
                            const Cursor &cursor,
                            std::string_view body_sha256, std::string *key,
                            std::string *error) {
  if (key == nullptr || !validate_stream_identity(stream, error) ||
      !validate_snapshot_id(snapshot_id, error) ||
      !validate_cursor(cursor, error) || !is_sha256(body_sha256)) {
    return fail(error, "invalid binlog seed key input");
  }
  std::string basename;
  uint64_t file_sequence = 0;
  std::string file20;
  std::string position20;
  if (!parse_binlog_file(cursor.file, &basename, &file_sequence, error) ||
      !format_padded_20(file_sequence, &file20, error) ||
      !format_padded_20(cursor.pos, &position20, error)) {
    return false;
  }
  *key = stream.remote_prefix + "/snapshots/binlog-seeds/" +
         std::string(snapshot_id) + "/" + file20 + "-" + position20 + "-" +
         std::string(body_sha256) + ".seed";
  return validate_key(*key, error);
}

bool snapshot_object_key(const StreamIdentity &stream,
                         std::string_view snapshot_id,
                         const SnapshotObject &object, std::string *key,
                         std::string *error) {
  static const std::set<std::string_view> kComponents{
      "innodb", "mysql-dd", "smartengine-meta", "smartengine-wal"};
  if (key == nullptr || !validate_stream_identity(stream, error) ||
      !validate_snapshot_id(snapshot_id, error) ||
      !kComponents.contains(object.component) ||
      object.ordinal > kJsonSafeIntegerMax || !is_sha256(object.sha256)) {
    return fail(error, "invalid snapshot object key input");
  }
  std::string ordinal20;
  if (!format_padded_20(object.ordinal, &ordinal20, error)) return false;
  *key = stream.remote_prefix + "/snapshots/data/" +
         std::string(snapshot_id) + "/" + object.component + "/" +
         ordinal20 + "-" + object.sha256 + ".obj";
  return validate_key(*key, error);
}

bool smartengine_extent_object_key(const StreamIdentity &stream,
                                   const SmartengineExtentRef &extent,
                                   std::string *key, std::string *error) {
  uint64_t allocation = 0;
  uint64_t index = 0;
  uint64_t object = 0;
  if (key == nullptr || !validate_stream_identity(stream, error) ||
      extent.writer_epoch == 0 ||
      extent.writer_epoch > kJsonSafeIntegerMax ||
      !parse_decimal_uint64(extent.allocation_seq, &allocation, false, error) ||
      !parse_decimal_uint64(extent.index_id, &index, false, error) ||
      !parse_decimal_uint64(extent.object_id, &object, false, error) ||
      !lowercase_hex_decodes_to_utf8(extent.database_name_hex, error)) {
    return fail(error, "invalid SmartEngine extent key input");
  }
  *key = stream.extent_prefix + "/e" + std::to_string(extent.writer_epoch) +
         "/a" + extent.allocation_seq + "/db=" + extent.database_name_hex +
         "/idx=" + extent.index_id + "/data/" + extent.object_id;
  return validate_key(*key, error);
}

bool canonicalize_gtid_set(std::string_view input, std::string *canonical,
                           std::string *error) {
  if (canonical == nullptr) return fail(error, "null canonical GTID result");
  struct Interval {
    uint64_t begin;
    uint64_t end;
  };
  std::map<std::array<unsigned char, 16>, std::vector<Interval>> parsed;
  const auto trim = [](std::string_view value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front())))
      value.remove_prefix(1);
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back())))
      value.remove_suffix(1);
    return value;
  };
  input = trim(input);
  if (!input.empty()) {
    size_t sid_begin = 0;
    while (sid_begin <= input.size()) {
      const size_t comma = input.find(',', sid_begin);
      const size_t sid_end =
          comma == std::string_view::npos ? input.size() : comma;
      const std::string_view sid_entry = trim(input.substr(sid_begin,
                                                           sid_end - sid_begin));
      const size_t colon = sid_entry.find(':');
      if (sid_entry.empty() || colon == std::string_view::npos || colon == 0 ||
          colon + 1 == sid_entry.size()) {
        return fail(error, "GTID SID entry is missing intervals");
      }
      std::array<unsigned char, 16> sid{};
      if (!parse_uuid(trim(sid_entry.substr(0, colon)), &sid, false, error)) {
        return false;
      }
      size_t interval_begin = colon + 1;
      while (interval_begin <= sid_entry.size()) {
        const size_t next_colon = sid_entry.find(':', interval_begin);
        const size_t interval_end = next_colon == std::string_view::npos
                                        ? sid_entry.size()
                                        : next_colon;
        const std::string_view token = trim(sid_entry.substr(
            interval_begin, interval_end - interval_begin));
        if (token.empty()) return fail(error, "empty GTID interval");
        const size_t dash = token.find('-');
        uint64_t begin = 0;
        uint64_t end = 0;
        if (dash == std::string_view::npos) {
          if (!parse_decimal_uint64(trim(token), &begin, true, error))
            return false;
          end = begin;
        } else {
          if (token.find('-', dash + 1) != std::string_view::npos ||
              !parse_decimal_uint64(trim(token.substr(0, dash)), &begin, true,
                                    error) ||
              !parse_decimal_uint64(trim(token.substr(dash + 1)), &end, true,
                                    error)) {
            return fail(error, "invalid GTID interval range");
          }
        }
        if (begin == 0 || begin > static_cast<uint64_t>(INT64_MAX) ||
            end < begin || end > static_cast<uint64_t>(INT64_MAX)) {
          return fail(error, "GTID GNO is outside 1..INT64_MAX");
        }
        parsed[sid].push_back(Interval{begin, end});
        if (next_colon == std::string_view::npos) break;
        interval_begin = next_colon + 1;
      }
      if (comma == std::string_view::npos) break;
      sid_begin = comma + 1;
    }
  }

  canonical->clear();
  bool first_sid = true;
  for (auto &[sid, intervals] : parsed) {
    std::sort(intervals.begin(), intervals.end(), [](const Interval &left,
                                                     const Interval &right) {
      return std::tie(left.begin, left.end) < std::tie(right.begin, right.end);
    });
    std::vector<Interval> merged;
    for (const Interval interval : intervals) {
      if (merged.empty() ||
          (merged.back().end != static_cast<uint64_t>(INT64_MAX) &&
           interval.begin > merged.back().end + 1)) {
        merged.push_back(interval);
      } else {
        merged.back().end = std::max(merged.back().end, interval.end);
      }
    }
    if (!first_sid) canonical->push_back(',');
    first_sid = false;
    canonical->append(format_uuid(sid));
    for (const Interval interval : merged) {
      canonical->push_back(':');
      canonical->append(std::to_string(interval.begin));
      if (interval.begin != interval.end) {
        canonical->push_back('-');
        canonical->append(std::to_string(interval.end));
      }
    }
    if (canonical->size() > kMaxCanonicalGtidBytes) {
      return fail(error, "canonical GTID payload exceeds 16 MiB");
    }
  }
  return true;
}

bool gtid_digest(std::string_view input, GtidSetDigest *digest,
                 std::string *error) {
  if (digest == nullptr) return fail(error, "null GTID digest result");
  GtidSetDigest result;
  if (!canonicalize_gtid_set(input, &result.canonical, error)) return false;
  std::string preimage;
  append_json_string(result.canonical, &preimage);
  if (!sha256_hex(preimage, &result.sha256, error)) return false;
  *digest = std::move(result);
  return true;
}

bool xid_jcs_preimage(const std::vector<uint64_t> &xids,
                      std::string *preimage, std::string *error) {
  if (preimage == nullptr) return fail(error, "null XID preimage result");
  std::vector<uint64_t> sorted = xids;
  std::sort(sorted.begin(), sorted.end());
  preimage->clear();
  preimage->push_back('[');
  for (size_t index = 0; index < sorted.size(); ++index) {
    if (index != 0) preimage->push_back(',');
    append_json_string(std::to_string(sorted[index]), preimage);
  }
  preimage->push_back(']');
  return true;
}

bool xid_digest(const std::vector<uint64_t> &xids, XidDigest *digest,
                std::string *error) {
  if (digest == nullptr) return fail(error, "null XID digest result");
  if (xids.size() > kJsonSafeIntegerMax) {
    return fail(error, "XID count exceeds the safe-integer limit");
  }
  std::string preimage;
  XidDigest result;
  result.count = xids.size();
  if (!xid_jcs_preimage(xids, &preimage, error) ||
      !sha256_hex(preimage, &result.sha256, error)) {
    return false;
  }
  *digest = std::move(result);
  return true;
}

bool segment_refs_digest(const std::vector<SegmentRef> &segments,
                         std::string *sha256, std::string *error) {
  if (sha256 == nullptr) return fail(error, "null segment-ref digest result");
  if (segments.size() > kMaxSegmentsPerManifest) {
    return fail(error, "segment-ref array exceeds 4096 items");
  }
  std::string preimage{"["};
  for (size_t index = 0; index < segments.size(); ++index) {
    const SegmentRef &segment = segments[index];
    GtidSetDigest gtid;
    if (segment.sequence == 0 || !validate_key(segment.key, error) ||
        !is_sha256(segment.sha256) ||
        !validate_segment_tip(segment.previous_segment, error) ||
        !gtid_digest(segment.gtid_set.canonical, &gtid, error) ||
        gtid != segment.gtid_set || !is_sha256(segment.xids.sha256) ||
        !segment.ends_at_transaction_boundary ||
        segment.payload_format != kSegmentPayloadFormat ||
        segment.compression != kNoCompression ||
        segment.source.end_pos <= segment.source.start_pos ||
        segment.size != segment.source.end_pos - segment.source.start_pos) {
      return fail(error, "invalid segment-ref array item");
    }
    if (index != 0) preimage.push_back(',');
    append_segment(segment, &preimage);
  }
  preimage.push_back(']');
  return sha256_hex(preimage, sha256, error);
}

bool serialize_writer_epoch(const StreamIdentity &stream,
                            const WriterEpoch &value, std::string *json,
                            std::string *error) {
  if (json == nullptr || !validate_stream_identity(stream, error) ||
      value.epoch == 0 || value.epoch > kJsonSafeIntegerMax ||
      value.previous_epoch + 1 != value.epoch ||
      !is_lower_hex(value.writer_id, 32)) {
    return json == nullptr
               ? fail(error, "null WRITER_EPOCH JSON result")
               : fail(error, "WRITER_EPOCH fields are invalid");
  }
  json->clear();
  json->append("{\"epoch\":");
  append_uint(value.epoch, json);
  json->append(",\"format\":");
  append_json_string(kWriterEpochFormat, json);
  json->append(",\"previous_epoch\":");
  append_uint(value.previous_epoch, json);
  json->append(",\"stream_id\":");
  append_json_string(stream.stream_id, json);
  json->append(",\"version\":2,\"writer_id\":");
  append_json_string(value.writer_id, json);
  json->push_back('}');
  return json->size() <= kWriterEpochMaxBytes
             ? true
             : fail(error, "WRITER_EPOCH JSON exceeds 4 KiB");
}

bool serialize_head(const StreamIdentity &stream, const Head &value,
                    std::string *json, std::string *error) {
  if (json == nullptr) return fail(error, "null HEAD JSON result");
  if (!validate_stream_identity(stream, error) ||
      !validate_head_value(stream, value, error)) {
    return false;
  }
  json->clear();
  json->append("{\"base_cursor\":");
  append_cursor(value.base_cursor, json);
  json->append(",\"durable_cursor\":");
  append_cursor(value.durable_cursor, json);
  json->append(",\"format\":");
  append_json_string(kHeadFormat, json);
  json->append(",\"generation\":");
  append_uint(value.generation, json);
  json->append(",\"manifest\":");
  append_object_ref(value.manifest, json);
  json->append(",\"parent\":");
  if (value.parent.has_value())
    append_head_parent(*value.parent, json);
  else
    json->append("null");
  json->append(",\"recovery_window\":");
  append_recovery_window(value.recovery_window, json);
  json->append(",\"segment_tip\":");
  append_segment_tip(value.segment_tip, json);
  json->append(",\"snapshot\":");
  append_snapshot_ref(value.snapshot, json);
  json->append(",\"state\":");
  append_json_string(value.state, json);
  json->append(",\"stream_id\":");
  append_json_string(stream.stream_id, json);
  json->append(",\"version\":2,\"writer\":");
  append_writer(value.writer, json);
  json->push_back('}');
  return json->size() <= kHeadMaxBytes
             ? true
             : fail(error, "HEAD JSON exceeds 64 KiB");
}

bool serialize_transition_manifest(const StreamIdentity &stream,
                                   const TransitionManifest &value,
                                   std::string *json, std::string *error) {
  if (json == nullptr) return fail(error, "null transition JSON result");
  if (!validate_stream_identity(stream, error) ||
      !validate_transition_value(stream, value, error)) {
    return false;
  }
  const std::string_view kind = value.kind == ManifestKind::BOOTSTRAP
                                    ? "BOOTSTRAP"
                                    : value.kind == ManifestKind::LOG_TRANSITION
                                          ? "LOG"
                                          : "SNAPSHOT";
  json->clear();
  json->append("{\"base_cursor\":");
  append_cursor(value.base_cursor, json);
  json->append(",\"durable_cursor\":");
  append_cursor(value.durable_cursor, json);
  json->append(",\"format\":");
  append_json_string(kManifestFormat, json);
  json->append(",\"generation\":");
  append_uint(value.generation, json);
  json->append(",\"head_parent\":");
  if (value.head_parent.has_value())
    append_head_parent(*value.head_parent, json);
  else
    json->append("null");
  json->append(",\"kind\":");
  append_json_string(kind, json);
  json->append(",\"previous\":");
  if (value.previous.has_value())
    append_manifest_ref(*value.previous, json);
  else
    json->append("null");
  json->append(",\"recovery_window\":");
  append_recovery_window(value.recovery_window, json);
  json->append(",\"segment_tip\":");
  append_segment_tip(value.segment_tip, json);
  json->append(",\"segments\":[");
  for (size_t index = 0; index < value.segments.size(); ++index) {
    if (index != 0) json->push_back(',');
    append_segment(value.segments[index], json);
  }
  json->append("],\"snapshot\":");
  append_snapshot_ref(value.snapshot, json);
  json->append(",\"stream_id\":");
  append_json_string(stream.stream_id, json);
  json->append(",\"version\":2,\"writer\":");
  append_writer(value.writer, json);
  json->push_back('}');

  const size_t limit = value.kind == ManifestKind::LOG_TRANSITION
                           ? kDeltaManifestMaxBytes
                           : kTransitionSnapshotManifestMaxBytes;
  if (json->size() > limit) {
    return fail(error, "transition manifest JSON exceeds its kind limit");
  }
  if (value.recovery_window.manifest_bytes < json->size()) {
    return fail(error, "recovery_window manifest_bytes is smaller than body");
  }
  if (value.kind == ManifestKind::BOOTSTRAP &&
      value.recovery_window.manifest_bytes != json->size()) {
    return fail(error, "BOOTSTRAP manifest_bytes is not the body size fixed point");
  }
  return true;
}

bool stabilize_transition_manifest(const StreamIdentity &stream,
                                   uint64_t base_manifest_bytes,
                                   TransitionManifest *value,
                                   std::string *json, std::string *error) {
  if (value == nullptr || json == nullptr) {
    return fail(error, "null transition stabilization input/result");
  }
  if (base_manifest_bytes >= kRecoveryManifestBytesMax) {
    return fail(error, "base manifest byte count is outside the hard limit");
  }
  uint64_t candidate = base_manifest_bytes + 1;
  for (int attempt = 0; attempt < 4; ++attempt) {
    value->recovery_window.manifest_bytes = candidate;
    std::string rendered;
    std::string render_error;
    const bool rendered_ok = serialize_transition_manifest(
        stream, *value, &rendered, &render_error);
    if (rendered.empty()) {
      return fail(error, render_error.empty() ? "manifest rendering failed"
                                              : std::move(render_error));
    }
    if (rendered.size() >
        kRecoveryManifestBytesMax - base_manifest_bytes) {
      return fail(error, "stabilized manifest byte count exceeds 512 MiB");
    }
    const uint64_t next = base_manifest_bytes + rendered.size();
    if (next == candidate) {
      if (!rendered_ok) {
        return fail(error, render_error.empty()
                               ? "stable manifest failed validation"
                               : std::move(render_error));
      }
      *json = std::move(rendered);
      return true;
    }
    candidate = next;
  }
  return fail(error, "manifest_bytes did not stabilize within four passes");
}

bool serialize_snapshot_manifest(const StreamIdentity &stream,
                                 const SnapshotManifest &value,
                                 std::string *json, std::string *error) {
  if (json == nullptr) return fail(error, "null snapshot JSON result");
  if (!validate_stream_identity(stream, error) ||
      !validate_snapshot_value(stream, value, error)) {
    return false;
  }
  json->clear();
  json->append("{\"binlog_seed\":");
  append_binlog_seed(value.binlog_seed, json);
  json->append(",\"cursor\":");
  append_cursor(value.cursor, json);
  json->append(",\"deployment_fingerprints\":");
  append_deployment_fingerprints(value.deployment_fingerprints, json);
  json->append(",\"format\":");
  append_json_string(kSnapshotFormat, json);
  json->append(",\"gtid_executed\":");
  append_gtid_digest(value.gtid_executed, json);
  json->append(",\"log_anchor\":");
  append_log_anchor(value.log_anchor, json);
  json->append(",\"objects\":[");
  for (size_t index = 0; index < value.objects.size(); ++index) {
    if (index != 0) json->push_back(',');
    append_snapshot_object(value.objects[index], json);
  }
  json->append("],\"server_identity\":{\"server_uuid\":");
  append_json_string(value.server_identity.server_uuid, json);
  json->append("},\"smartengine_extents\":[");
  for (size_t index = 0; index < value.smartengine_extents.size(); ++index) {
    if (index != 0) json->push_back(',');
    append_extent(value.smartengine_extents[index], json);
  }
  json->append("],\"snapshot_id\":");
  append_json_string(value.snapshot_id, json);
  json->append(",\"stream_id\":");
  append_json_string(stream.stream_id, json);
  json->append(",\"version\":2,\"writer\":");
  append_writer(value.writer, json);
  json->push_back('}');
  return json->size() <= kSnapshotManifestMaxBytes
             ? true
             : fail(error, "snapshot manifest JSON exceeds 256 MiB");
}

bool parse_writer_epoch(std::string_view json, const StreamIdentity &stream,
                        WriterEpoch *value, std::string *error) {
  if (value == nullptr || !validate_stream_identity(stream, error)) {
    return value == nullptr ? fail(error, "null WRITER_EPOCH result") : false;
  }
  JsonValue root;
  WriterEpoch parsed;
  std::string canonical;
  if (!parse_json(json, kWriterEpochMaxBytes, &root, error) ||
      !decode_writer_epoch_value(root, stream, &parsed, error) ||
      !serialize_writer_epoch(stream, parsed, &canonical, error)) {
    return false;
  }
  if (canonical != json) return fail(error, "WRITER_EPOCH body is not RFC 8785 JCS");
  *value = std::move(parsed);
  return true;
}

bool parse_head(std::string_view json, const StreamIdentity &stream,
                Head *value, std::string *error) {
  if (value == nullptr || !validate_stream_identity(stream, error)) {
    return value == nullptr ? fail(error, "null HEAD result") : false;
  }
  JsonValue root;
  Head parsed;
  std::string canonical;
  if (!parse_json(json, kHeadMaxBytes, &root, error) ||
      !decode_head_value(root, stream, &parsed, error) ||
      !serialize_head(stream, parsed, &canonical, error)) {
    return false;
  }
  if (canonical != json) return fail(error, "HEAD body is not RFC 8785 JCS");
  *value = std::move(parsed);
  return true;
}

bool parse_transition_manifest(std::string_view json,
                               const StreamIdentity &stream,
                               std::string_view expected_object_key,
                               TransitionManifest *value,
                               std::string *error) {
  if (value == nullptr || !validate_stream_identity(stream, error)) {
    return value == nullptr ? fail(error, "null transition manifest result")
                            : false;
  }
  JsonValue root;
  TransitionManifest parsed;
  std::string canonical;
  if (!parse_json(json, kDeltaManifestMaxBytes, &root, error) ||
      !decode_transition_value(root, stream, &parsed, error) ||
      !serialize_transition_manifest(stream, parsed, &canonical, error)) {
    return false;
  }
  if (canonical != json) {
    return fail(error, "transition manifest body is not RFC 8785 JCS");
  }
  if (!expected_object_key.empty()) {
    std::string body_sha;
    std::string derived_key;
    if (!sha256_hex(json, &body_sha, error) ||
        !transition_manifest_key(stream, parsed.writer, parsed.generation,
                                 body_sha, &derived_key, error)) {
      return false;
    }
    if (derived_key != expected_object_key) {
      return fail(error, "transition manifest object key does not match body");
    }
  }
  *value = std::move(parsed);
  return true;
}

bool parse_snapshot_manifest(std::string_view json,
                             const StreamIdentity &stream,
                             std::string_view expected_object_key,
                             SnapshotManifest *value, std::string *error) {
  if (value == nullptr || !validate_stream_identity(stream, error)) {
    return value == nullptr ? fail(error, "null snapshot manifest result")
                            : false;
  }
  JsonValue root;
  SnapshotManifest parsed;
  std::string canonical;
  if (!parse_json(json, kSnapshotManifestMaxBytes, &root, error) ||
      !decode_snapshot_value(root, stream, &parsed, error) ||
      !serialize_snapshot_manifest(stream, parsed, &canonical, error)) {
    return false;
  }
  if (canonical != json) {
    return fail(error, "snapshot manifest body is not RFC 8785 JCS");
  }
  if (!expected_object_key.empty()) {
    std::string body_sha;
    std::string derived_key;
    if (!sha256_hex(json, &body_sha, error) ||
        !snapshot_manifest_key(stream, parsed.snapshot_id, body_sha,
                               &derived_key, error)) {
      return false;
    }
    if (derived_key != expected_object_key) {
      return fail(error, "snapshot manifest object key does not match body");
    }
  }
  *value = std::move(parsed);
  return true;
}

}  // namespace wesql::remote_commit
