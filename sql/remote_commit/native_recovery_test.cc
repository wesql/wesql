/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/native_recovery.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rc = wesql::remote_commit;

namespace wesql::remote_commit {

namespace {

std::string fake_digest(std::string_view value) {
  static constexpr char hex[] = "0123456789abcdef";
  std::string result(64, '0');
  for (size_t index = 0; index < value.size(); ++index) {
    const unsigned char byte = static_cast<unsigned char>(value[index]);
    result[index % result.size()] = hex[(byte + index) & 0xf];
  }
  return result;
}

}  // namespace

bool sha256_hex(std::string_view input, std::string *digest,
                std::string *error) {
  if (digest == nullptr) {
    if (error != nullptr) *error = "null digest";
    return false;
  }
  *digest = fake_digest(input);
  return true;
}

bool gtid_digest(std::string_view canonical, GtidSetDigest *digest,
                 std::string *error) {
  if (digest == nullptr || canonical.empty()) {
    if (error != nullptr) *error = "invalid GTID";
    return false;
  }
  digest->canonical.assign(canonical);
  digest->sha256 = fake_digest(std::string("gtid:") + std::string(canonical));
  return true;
}

bool xid_digest(const std::vector<uint64_t> &xids, XidDigest *digest,
                std::string *error) {
  if (digest == nullptr) {
    if (error != nullptr) *error = "null XID digest";
    return false;
  }
  std::string canonical;
  for (size_t index = 0; index < xids.size(); ++index) {
    if (index != 0) canonical.push_back(',');
    canonical.append(std::to_string(xids[index]));
  }
  digest->count = xids.size();
  digest->sha256 = fake_digest(std::string("xid:") + canonical);
  return true;
}

}  // namespace wesql::remote_commit

namespace {

[[noreturn]] void fail(std::string_view message) {
  std::cerr << "native_recovery_test: " << message << '\n';
  std::exit(1);
}

void expect(bool condition, std::string_view message) {
  if (!condition) fail(message);
}

rc::GtidSetDigest gtid(std::string_view canonical) {
  rc::GtidSetDigest result;
  std::string error;
  expect(rc::gtid_digest(canonical, &result, &error), "cannot build GTID");
  return result;
}

struct Fixture {
  rc::RecoveryPlan candidate;
  rc::MaterializedRoot materialized;
  rc::NativeRecoveryRequest request;

  Fixture() {
    candidate.head.generation = 7;
    candidate.head.durable_cursor = {"binlog.000001", 100};
    candidate.head_object.body = "exact-head-body";
    candidate.snapshot.snapshot_id = "snapshot-1";
    candidate.snapshot.cursor = {"binlog.000001", 4};
    rc::SegmentRef segment;
    segment.source = {"binlog.000001", 4, 100};
    segment.size = 96;
    segment.payload_format = "native-mysql-binlog-range-v1";
    segment.compression = "none";
    segment.ends_at_transaction_boundary = true;
    candidate.replay_segments.push_back(std::move(segment));
    request = {"r=repo/b=branch", &candidate, &materialized,
               candidate.snapshot.cursor, candidate.head.durable_cursor,
               1024 * 1024};
  }
};

rc::NativeRecoveryTransaction transaction(uint64_t end = 100) {
  return {{"binlog.000001", 4},
          {"binlog.000001", end},
          gtid("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1"),
          17,
          true};
}

class FakePreparedVerifier final : public rc::NativeRecoveryPreparedVerifier {
 public:
  bool prepared_sets_empty(std::string *error) override {
    ++calls;
    const bool empty = calls <= answers.size() ? answers[calls - 1] : true;
    if (!empty && error != nullptr) *error = "prepared set is nonempty";
    return empty;
  }

  std::vector<bool> answers{true, true};
  size_t calls{0};
};

class FakeExecutor final : public rc::NativeRecoveryExecutor {
 public:
  rc::NativeRecoveryScanResult scan(
      const rc::NativeRecoveryRequest &,
      std::vector<rc::NativeRecoveryTransaction> *output) override {
    ++scan_calls;
    if (scan_result.ready()) *output = transactions;
    return scan_result;
  }

  bool start_session(std::string *error) override {
    ++start_calls;
    if (!start_ok && error != nullptr) *error = "start failed";
    return start_ok;
  }

  bool apply_transaction(const rc::NativeRecoveryTransaction &input,
                         const rc::CommitBinding &binding,
                         std::string *error) override {
    ++apply_calls;
    applied.push_back(input);
    bindings.push_back(binding);
    if (!apply_ok && error != nullptr) *error = "apply failed";
    return apply_ok;
  }

  bool finish_session(std::string *error) override {
    ++finish_calls;
    if (!finish_ok && error != nullptr) *error = "finish failed";
    return finish_ok;
  }

  rc::NativeRecoveryScanResult scan_result{
      rc::NativeRecoveryScanOutcome::READY, {}};
  std::vector<rc::NativeRecoveryTransaction> transactions{transaction()};
  bool start_ok{true};
  bool apply_ok{true};
  bool finish_ok{true};
  size_t scan_calls{0};
  size_t start_calls{0};
  size_t apply_calls{0};
  size_t finish_calls{0};
  std::vector<rc::NativeRecoveryTransaction> applied;
  std::vector<rc::CommitBinding> bindings;
};

void happy_path() {
  Fixture fixture;
  FakeExecutor executor;
  FakePreparedVerifier prepared;
  const rc::NativeRecoveryResult result = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(result.outcome == rc::NativeRecoveryOutcome::APPLIED &&
             result.applied_transactions == 1,
         "happy path did not apply exactly one transaction");
  expect(prepared.calls == 2 && executor.scan_calls == 1 &&
             executor.start_calls == 1 && executor.apply_calls == 1 &&
             executor.finish_calls == 1,
         "happy path lifecycle count differs");
  expect(executor.bindings.size() == 1, "authorization binding is absent");
  const rc::CommitBinding &binding = executor.bindings.front();
  expect(binding.stream_id == fixture.request.stream_id &&
             binding.head_generation == fixture.candidate.head.generation &&
             binding.endpoint_file == "binlog.000001" &&
             binding.endpoint_pos == 100 &&
             binding.gtid_sha256 == executor.transactions.front().gtid.sha256,
         "authorization binding is not exact");
  rc::XidDigest expected_xid;
  std::string error;
  expect(rc::xid_digest({17}, &expected_xid, &error) &&
             binding.xid_sha256 == expected_xid.sha256,
         "authorization XID digest differs");
}

void empty_range() {
  Fixture fixture;
  fixture.candidate.head.durable_cursor = fixture.candidate.snapshot.cursor;
  fixture.candidate.replay_segments.clear();
  fixture.request.inclusive_end = fixture.request.exclusive_start;
  FakeExecutor executor;
  executor.transactions.clear();
  FakePreparedVerifier prepared;
  const rc::NativeRecoveryResult result = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(result.outcome == rc::NativeRecoveryOutcome::EMPTY &&
             prepared.calls == 2 && executor.start_calls == 0,
         "empty range entered an apply session");
}

void rejects_prepared_before_scan() {
  Fixture fixture;
  FakeExecutor executor;
  FakePreparedVerifier prepared;
  prepared.answers = {false};
  const rc::NativeRecoveryResult result = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(result.outcome ==
                 rc::NativeRecoveryOutcome::PREPARED_SET_NOT_EMPTY &&
             executor.scan_calls == 0,
         "prepared set was not rejected before scan");
}

void maps_scan_failure() {
  Fixture fixture;
  FakeExecutor executor;
  executor.scan_result = {rc::NativeRecoveryScanOutcome::INCOMPLETE_TRANSACTION,
                          "partial"};
  FakePreparedVerifier prepared;
  const rc::NativeRecoveryResult result = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(result.outcome ==
                 rc::NativeRecoveryOutcome::INCOMPLETE_TRANSACTION &&
             executor.start_calls == 0,
         "incomplete scan was not preserved");
}

void rejects_descriptor_short_of_head() {
  Fixture fixture;
  FakeExecutor executor;
  executor.transactions = {transaction(99)};
  FakePreparedVerifier prepared;
  const rc::NativeRecoveryResult result = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(result.outcome == rc::NativeRecoveryOutcome::CORRUPT &&
             executor.start_calls == 0,
         "short transaction descriptor reached apply");
}

void apply_failure_still_finishes() {
  Fixture fixture;
  FakeExecutor executor;
  executor.apply_ok = false;
  FakePreparedVerifier prepared;
  const rc::NativeRecoveryResult result = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(result.outcome == rc::NativeRecoveryOutcome::APPLY_ERROR &&
             executor.finish_calls == 1 && prepared.calls == 2,
         "apply failure skipped cleanup or prepared recheck");
}

void finish_failure_wins() {
  Fixture fixture;
  FakeExecutor executor;
  executor.apply_ok = false;
  executor.finish_ok = false;
  FakePreparedVerifier prepared;
  const rc::NativeRecoveryResult result = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(result.outcome == rc::NativeRecoveryOutcome::THD_LIFECYCLE_ERROR,
         "THD cleanup failure was hidden by apply failure");
}

void rejects_prepared_after_apply() {
  Fixture fixture;
  FakeExecutor executor;
  FakePreparedVerifier prepared;
  prepared.answers = {true, false};
  const rc::NativeRecoveryResult result = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(result.outcome ==
                 rc::NativeRecoveryOutcome::PREPARED_SET_NOT_EMPTY &&
             result.applied_transactions == 1,
         "post-apply prepared set was not rejected");
}

void rejects_segment_gap_and_null_dependencies() {
  Fixture fixture;
  fixture.candidate.replay_segments.front().source.start_pos = 5;
  fixture.candidate.replay_segments.front().size = 95;
  FakeExecutor executor;
  FakePreparedVerifier prepared;
  const rc::NativeRecoveryResult gap = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(gap.outcome == rc::NativeRecoveryOutcome::CORRUPT &&
             prepared.calls == 0,
         "segment gap reached prepared verification");
  const rc::NativeRecoveryResult null_executor =
      rc::run_bounded_native_recovery(fixture.request, nullptr, &prepared);
  expect(null_executor.outcome == rc::NativeRecoveryOutcome::CORRUPT,
         "null executor was accepted");
}

void requires_exact_next_binlog_segment() {
  Fixture fixture;
  rc::SegmentRef next = fixture.candidate.replay_segments.front();
  next.source = {"binlog.000003", 0, 50};
  next.size = 50;
  fixture.candidate.replay_segments.push_back(next);
  fixture.candidate.head.durable_cursor = {next.source.file,
                                           next.source.end_pos};
  fixture.request.inclusive_end = fixture.candidate.head.durable_cursor;

  FakeExecutor executor;
  FakePreparedVerifier prepared;
  const rc::NativeRecoveryResult skipped = rc::run_bounded_native_recovery(
      fixture.request, &executor, &prepared);
  expect(skipped.outcome == rc::NativeRecoveryOutcome::CORRUPT &&
             prepared.calls == 0 && executor.scan_calls == 0,
         "native recovery accepted a skipped binlog sequence");

  fixture.candidate.replay_segments.back().source.file = "binlog.000002";
  fixture.candidate.head.durable_cursor.file = "binlog.000002";
  fixture.request.inclusive_end.file = "binlog.000002";
  const rc::NativeRecoveryResult consecutive =
      rc::run_bounded_native_recovery(fixture.request, &executor, &prepared);
  expect(consecutive.outcome == rc::NativeRecoveryOutcome::CORRUPT &&
             prepared.calls == 1 && executor.scan_calls == 1,
         "exact next binlog did not reach the transaction proof pass");
}

}  // namespace

int main() {
  happy_path();
  empty_range();
  rejects_prepared_before_scan();
  maps_scan_failure();
  rejects_descriptor_short_of_head();
  apply_failure_still_finishes();
  finish_failure_wins();
  rejects_prepared_after_apply();
  rejects_segment_gap_and_null_dependencies();
  requires_exact_next_binlog_segment();
  std::cout << "native recovery state-machine tests passed\n";
  return 0;
}
