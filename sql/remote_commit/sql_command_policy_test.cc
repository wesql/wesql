/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/sql_command_policy.h"

#include <cstdlib>
#include <iostream>
#include <limits>

#include "sql/remote_commit/policy.h"

namespace rc = wesql::remote_commit;

namespace {

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "SQL command policy test failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

void test_command_table() {
  expect(rc::sql_command_classification_complete(),
         "classification table is complete");
  for (int value = 0; value < static_cast<int>(SQLCOM_END); ++value) {
    expect(rc::classify_sql_command(static_cast<enum_sql_command>(value)) !=
               rc::SqlCommandClass::UNCLASSIFIED,
           "valid SQL command is classified");
  }
  expect(rc::classify_sql_command(SQLCOM_END) ==
             rc::SqlCommandClass::UNCLASSIFIED,
         "SQLCOM_END is not a command");
  expect(rc::classify_sql_command(static_cast<enum_sql_command>(-1)) ==
             rc::SqlCommandClass::UNCLASSIFIED,
         "negative command value is rejected");

  expect(rc::classify_sql_command(SQLCOM_XA_START) ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "XA START is rejected before dispatch");
  expect(rc::classify_sql_command(SQLCOM_XA_END) ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "XA END is rejected before dispatch");
  expect(rc::classify_sql_command(SQLCOM_XA_PREPARE) ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "XA PREPARE is rejected before tc_log prepare");
  expect(rc::classify_sql_command(SQLCOM_XA_COMMIT) ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "XA COMMIT is rejected before dispatch");
  expect(rc::classify_sql_command(SQLCOM_XA_ROLLBACK) ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "XA ROLLBACK is rejected before dispatch");
  expect(rc::classify_sql_command(SQLCOM_XA_RECOVER) ==
             rc::SqlCommandClass::NO_DURABLE_MUTATION,
         "read-only XA RECOVER remains available");
  expect(rc::classify_sql_command(SQLCOM_ALTER_TABLESPACE) ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "tablespace DDL cannot create storage outside the managed root");
}

void test_set_assignments() {
  expect(rc::classify_set_assignment(rc::SetAssignmentScope::SESSION,
                                     "optimizer_switch") ==
             rc::SqlCommandClass::NO_DURABLE_MUTATION,
         "ordinary session variable is allowed");
  expect(rc::classify_set_assignment(rc::SetAssignmentScope::DEFAULT_SCOPE,
                                     "optimizer_switch") ==
             rc::SqlCommandClass::NO_DURABLE_MUTATION,
         "ordinary default-scope variable is allowed");
  expect(rc::classify_set_assignment(rc::SetAssignmentScope::SESSION,
                                     "SQL_LOG_BIN") ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "protected session variable is case-insensitively rejected");
  expect(rc::classify_set_assignment(rc::SetAssignmentScope::SESSION,
                                     "smartengine_write_disable_wal") ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "SmartEngine WAL bypass is rejected");
  expect(rc::classify_set_assignment(rc::SetAssignmentScope::GLOBAL,
                                     "optimizer_switch") ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "global assignment is rejected");
  expect(rc::classify_set_assignment(rc::SetAssignmentScope::PERSIST,
                                     "optimizer_switch") ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "persist assignment is rejected");
  expect(rc::classify_set_assignment(rc::SetAssignmentScope::PERSIST_ONLY,
                                     "optimizer_switch") ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "persist-only assignment is rejected");
  expect(rc::classify_set_assignment(rc::SetAssignmentScope::UNKNOWN,
                                     "optimizer_switch") ==
             rc::SqlCommandClass::LOCAL_MUTATING_REJECT,
         "unknown SET scope fails closed");
}

void test_transaction_size_boundaries() {
  const uint64_t maximum = rc::kSegmentEnvelopeBytes + 100;
  uint64_t bytes_with_envelope = 0;
  expect(rc::transaction_cache_fits_segment(60, 40, maximum,
                                            &bytes_with_envelope),
         "exact segment boundary is accepted");
  expect(bytes_with_envelope == maximum,
         "exact boundary reports the encoded total");
  expect(!rc::transaction_cache_fits_segment(60, 41, maximum),
         "one byte over the segment boundary is rejected");
  expect(!rc::transaction_cache_fits_segment(
             std::numeric_limits<uint64_t>::max(), 1,
             std::numeric_limits<uint64_t>::max()),
         "cache addition overflow is rejected");
  expect(!rc::transaction_cache_fits_segment(
             0, 0, rc::kSegmentEnvelopeBytes - 1),
         "segment smaller than the fixed envelope is rejected");

  const uint64_t largest_payload =
      std::numeric_limits<uint64_t>::max() - rc::kSegmentEnvelopeBytes;
  expect(rc::transaction_cache_fits_segment(
             largest_payload, 0, std::numeric_limits<uint64_t>::max(),
             &bytes_with_envelope),
         "largest representable exact boundary is accepted");
  expect(bytes_with_envelope == std::numeric_limits<uint64_t>::max(),
         "largest exact boundary does not overflow");
  expect(!rc::transaction_cache_fits_segment(
             largest_payload + 1, 0,
             std::numeric_limits<uint64_t>::max()),
         "envelope addition overflow is rejected");
}

}  // namespace

int main() {
  test_command_table();
  test_set_assignments();
  test_transaction_size_boundaries();
  std::cout << "SQL command policy tests passed\n";
  return EXIT_SUCCESS;
}
