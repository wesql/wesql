/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_NATIVE_RECOVERY_INCLUDED
#define SQL_REMOTE_COMMIT_NATIVE_RECOVERY_INCLUDED

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "sql/remote_commit/materializer.h"
#include "sql/remote_commit/policy.h"

namespace wesql::remote_commit {

enum class NativeRecoveryOutcome : uint8_t {
  APPLIED,
  EMPTY,
  NOT_RECOVERING,
  PREPARED_SET_NOT_EMPTY,
  INCOMPLETE_TRANSACTION,
  CORRUPT,
  LOCAL_IO_ERROR,
  APPLY_ERROR,
  THD_LIFECYCLE_ERROR,
};

struct NativeRecoveryResult {
  NativeRecoveryOutcome outcome{NativeRecoveryOutcome::CORRUPT};
  std::string detail;
  uint64_t applied_transactions{0};

  bool applied() const {
    return outcome == NativeRecoveryOutcome::APPLIED ||
           outcome == NativeRecoveryOutcome::EMPTY;
  }
};

struct NativeRecoveryRequest {
  std::string stream_id;
  const RecoveryPlan *candidate{nullptr};
  const MaterializedRoot *materialized{nullptr};
  Cursor exclusive_start;
  Cursor inclusive_end;
  uint32_t max_event_bytes{0};
};

// A transaction is discovered in a mutation-free first pass. Its first event
// is an assigned GTID and endpoint is the byte immediately after its terminal
// Query/Xid event. Native transactions are never allowed to span files.
struct NativeRecoveryTransaction {
  Cursor first_event;
  Cursor endpoint;
  GtidSetDigest gtid;
  std::optional<uint64_t> xid;
  bool requires_durable_authorization{false};

  bool operator==(const NativeRecoveryTransaction &) const = default;
};

enum class NativeRecoveryScanOutcome : uint8_t {
  READY,
  INCOMPLETE_TRANSACTION,
  CORRUPT,
  LOCAL_IO_ERROR,
};

struct NativeRecoveryScanResult {
  NativeRecoveryScanOutcome outcome{NativeRecoveryScanOutcome::CORRUPT};
  std::string detail;

  bool ready() const { return outcome == NativeRecoveryScanOutcome::READY; }
};

// Both internal engine-prepared and external-XA inventories must be empty at
// each call. Implementations enumerate only; they must never decide an XID.
class NativeRecoveryPreparedVerifier {
 public:
  virtual ~NativeRecoveryPreparedVerifier() = default;
  virtual bool prepared_sets_empty(std::string *error) = 0;
};

// The executor split keeps ordering and authorization policy testable without
// constructing a live server. A production executor performs a two-pass scan
// and applies events through Log_event/Relay_log_info on one dedicated THD.
class NativeRecoveryExecutor {
 public:
  virtual ~NativeRecoveryExecutor() = default;

  virtual NativeRecoveryScanResult scan(
      const NativeRecoveryRequest &request,
      std::vector<NativeRecoveryTransaction> *transactions) = 0;
  virtual bool start_session(std::string *error) = 0;
  virtual bool apply_transaction(const NativeRecoveryTransaction &transaction,
                                 const CommitBinding &binding,
                                 std::string *error) = 0;
  // Must remove any recovery authorization, roll back unfinished local work,
  // and restore Auto_THD::system_thread to SYSTEM_THREAD_BACKGROUND.
  virtual bool finish_session(std::string *error) = 0;
};

NativeRecoveryResult run_bounded_native_recovery(
    const NativeRecoveryRequest &request, NativeRecoveryExecutor *executor,
    NativeRecoveryPreparedVerifier *prepared_verifier);

// Production adapter. Startup integration supplies an enumerate-only prepared
// verifier after engines/DD have been opened on the fresh sibling root.
NativeRecoveryResult replay_bounded_native_tail(
    const NativeRecoveryRequest &request,
    NativeRecoveryPreparedVerifier *prepared_verifier);

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_NATIVE_RECOVERY_INCLUDED
