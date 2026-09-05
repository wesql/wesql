/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_EVIDENCE_INCLUDED
#define SQL_REMOTE_COMMIT_EVIDENCE_INCLUDED

#include <cstdint>
#include <optional>
#include <string>

#include "sql/remote_commit/policy.h"
#include "sql/remote_commit/protocol_codec.h"

namespace wesql::remote_commit {

constexpr size_t kStatusJsonMaxBytes = 768;
constexpr size_t kAckReadyEventMaxBytes = 7168;
constexpr const char kAckReadyPrefix[] = "REMOTE_COMMIT_ACK_READY ";

struct StatusHead {
  uint64_t generation{0};
  std::string etag_sha256;
  std::string body_sha256;
};

struct StatusWriter {
  uint64_t epoch{0};
  std::string id;
};

struct StatusSnapshot {
  LifecycleState state{LifecycleState::OFF};
  std::optional<std::string> stream_sha256;
  std::optional<StatusWriter> writer;
  std::optional<StatusHead> head;
  std::optional<Cursor> durable_cursor;
};

bool format_status_json(const StatusSnapshot &status, std::string *json,
                        std::string *error);

struct AckReadyEvent {
  std::string stream_id;
  Writer writer;
  Cursor endpoint;
  uint64_t transaction_count{0};
  std::string gtid_set_sha256;
  std::string xid_sha256;
  uint64_t head_generation{0};
  std::string head_etag_sha256;
  std::string head_body_sha256;
  ObjectRef manifest;
  uint64_t segment_count{0};
  std::string segment_refs_sha256;
  SegmentRef last_segment;
};

bool format_ack_ready_json(const AckReadyEvent &event, std::string *json,
                           std::string *error);
bool format_ack_ready_log_line(const AckReadyEvent &event, std::string *line,
                               std::string *error);

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_EVIDENCE_INCLUDED
