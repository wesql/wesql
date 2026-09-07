/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/native_recovery.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "my_sys.h"

// Captured native events contain initial atomic CREATEs, row transactions,
// repeated IF NOT EXISTS no-ops, and another real row transaction.
int main(int argc, char **argv) {
  if (argc != 2 || my_init()) return 2;
  namespace rc = wesql::remote_commit;
  const std::filesystem::path fixture(argv[1]);
  rc::RecoveryPlan plan;
  rc::MaterializedRoot materialized;
  materialized.binlog_files.push_back(fixture);
  rc::SegmentRef segment;
  segment.source.file = fixture.filename().string();
  segment.source.start_pos = 158;
  segment.source.end_pos = 5215;
  segment.transaction_count = 11;
  std::string error;
  if (!rc::gtid_digest("ce17e2dc-aa77-11f1-b25d-4e03cffe25eb:1-11",
                       &segment.gtid_set, &error) ||
      !rc::xid_digest({14, 16, 22}, &segment.xids, &error)) {
    std::cerr << error << '\n';
    return 2;
  }
  plan.replay_segments.push_back(segment);
  rc::NativeRecoveryRequest request;
  request.candidate = &plan;
  request.materialized = &materialized;
  request.max_event_bytes = 1024 * 1024;
  std::vector<rc::NativeRecoveryTransaction> transactions;
  const auto scanned = rc::scan_native_recovery_for_test(request, &transactions);
  if (!scanned.ready() || transactions.size() != 11) {
    std::cerr << "native scan failed: " << scanned.detail << '\n';
    return 1;
  }
  bool passed = true;
  for (size_t index = 0; index < transactions.size(); ++index) {
    const auto &transaction = transactions[index];
    const bool expected = index < 6 || index == 10;
    std::cout << transaction.gtid.canonical << " durable="
              << transaction.requires_durable_authorization
              << " expected=" << expected << '\n';
    passed &= transaction.requires_durable_authorization == expected;
  }
  // Reusing the descriptor with inconsistent transaction metadata must fail.
  ++plan.replay_segments.front().transaction_count;
  const auto malformed = rc::scan_native_recovery_for_test(request, &transactions);
  passed &= !malformed.ready();
  my_end(0);
  return passed ? 0 : 1;
}
