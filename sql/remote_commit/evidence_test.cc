/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/evidence.h"

#include <cstdlib>
#include <iostream>
#include <string>

namespace rc = wesql::remote_commit;

namespace {

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "evidence test failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

std::string hex(char ch, size_t size) { return std::string(size, ch); }

void test_status() {
  std::string json;
  std::string error;
  expect(rc::format_status_json({}, &json, &error), "OFF status formats");
  expect(json ==
             "{\"durable_cursor\":null,\"format\":\"wesql.remote_commit.status\","
             "\"head\":null,\"state\":\"OFF\",\"stream_sha256\":null,"
             "\"version\":1,\"writer\":null}",
         "OFF status golden bytes");

  rc::StatusSnapshot maximum;
  maximum.state = rc::LifecycleState::RUNNING;
  maximum.stream_sha256 = hex('a', 64);
  maximum.writer = rc::StatusWriter{rc::kJsonSafeIntegerMax, hex('b', 32)};
  maximum.head = rc::StatusHead{rc::kJsonSafeIntegerMax, hex('c', 64),
                                hex('d', 64)};
  maximum.durable_cursor = rc::Cursor{std::string(128, 'f'),
                                      rc::kJsonSafeIntegerMax};
  expect(rc::format_status_json(maximum, &json, &error),
         "maximum status formats");
  expect(json.size() < rc::kStatusJsonMaxBytes,
         "maximum status cannot be truncated by SHOW_VAR");
  expect(json.find("...") == std::string::npos, "status has no ellipsis");

  maximum.state = rc::LifecycleState::OFF;
  expect(!rc::format_status_json(maximum, &json, &error),
         "OFF cannot retain stale fields");
}

void test_ack() {
  rc::AckReadyEvent event;
  event.stream_id = "r=repo/b=main";
  event.writer = {hex('a', 32), 7};
  event.endpoint = {"binlog.000007", 900};
  event.transaction_count = 3;
  event.gtid_set_sha256 = hex('b', 64);
  event.xid_sha256 = hex('c', 64);
  event.head_generation = 8;
  event.head_etag_sha256 = hex('d', 64);
  event.head_body_sha256 = hex('e', 64);
  event.manifest = {"r=repo/b=main/remote-commit/v2/manifests/m.json", 99,
                    hex('f', 64)};
  event.segment_count = 1;
  event.segment_refs_sha256 = hex('1', 64);
  event.last_segment.sequence = 4;
  event.last_segment.key =
      "r=repo/b=main/remote-commit/v2/binlog/segments/s.seg";
  event.last_segment.size = 100;
  event.last_segment.sha256 = hex('2', 64);
  event.last_segment.source = {"binlog.000007", 800, 900};

  std::string json;
  std::string line;
  std::string error;
  expect(rc::format_ack_ready_json(event, &json, &error),
         "ACK event formats");
  expect(rc::format_ack_ready_log_line(event, &line, &error),
         "ACK log line formats");
  expect(line == std::string(rc::kAckReadyPrefix) + json,
         "ACK prefix is stable");
  expect(line.size() < rc::kAckReadyEventMaxBytes,
         "ACK log line is below LOG_BUFF_MAX margin");
  expect(line.find("...") == std::string::npos, "ACK has no ellipsis");

  event.last_segment.source.end_pos = event.last_segment.source.start_pos;
  expect(!rc::format_ack_ready_json(event, &json, &error),
         "empty segment range rejected");
}

}  // namespace

int main() {
  test_status();
  test_ack();
  std::cout << "remote commit evidence tests passed\n";
  return EXIT_SUCCESS;
}
