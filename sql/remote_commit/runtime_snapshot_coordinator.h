/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_COORDINATOR_INCLUDED
#define SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_COORDINATOR_INCLUDED

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "sql/remote_commit/server_hooks.h"
#include "sql/remote_commit/snapshot_publisher.h"

namespace wesql::remote_commit {

enum class RuntimeSnapshotAcquisitionOutcome : uint8_t {
  FIXED,
  BLOCKED,
  REFIX_REQUIRED,
  FENCED,
  PERMANENT_ERROR,
  SHUTDOWN,
};

// Owns the clone, SmartEngine, staging-directory, and binlog source pins that
// make a fixed cut immutable. Concrete source layers release every resource in
// their destructor. The coordinator retains this lease through preparation
// and ordered publication, including BLOCKED retries.
class RuntimeSnapshotSourceLease {
 public:
  virtual ~RuntimeSnapshotSourceLease() = default;

  RuntimeSnapshotSourceLease() = default;
  RuntimeSnapshotSourceLease(const RuntimeSnapshotSourceLease &) = delete;
  RuntimeSnapshotSourceLease &operator=(const RuntimeSnapshotSourceLease &) =
      delete;
};

struct RuntimeSnapshotAcquisition {
  uint64_t request_id{0};
  FixedSnapshotCut cut;
  RuntimeSnapshotAuthority authority;
  std::unique_ptr<RuntimeSnapshotSourceLease> source_lease;
};

struct RuntimeSnapshotAcquisitionResult {
  RuntimeSnapshotAcquisitionOutcome outcome{
      RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR};
  std::string detail;
  std::optional<RuntimeSnapshotAcquisition> acquisition;

  bool fixed() const {
    return outcome == RuntimeSnapshotAcquisitionOutcome::FIXED &&
           acquisition.has_value();
  }
};

// The production source layer runs the local clone observer, fixes the exact
// InnoDB/SmartEngine/binlog sources while admission is closed, and returns only
// immutable local artifacts plus the cut-time publisher authority. Tests use a
// fake; source-specific filesystem and engine policy does not belong here.
class RuntimeSnapshotAcquirer {
 public:
  virtual ~RuntimeSnapshotAcquirer() = default;
  virtual RuntimeSnapshotAcquisitionResult acquire(
      const RuntimeSnapshotRequest &request) = 0;
};

// Request and hard-gate ownership boundary. The bool methods use the same
// conventions as server_hooks: take() is a normal predicate, wait() returns
// true only for shutdown/cancel, and reserve returns true on error. Typed
// refresh/completion outcomes preserve retry, refix, and terminal policy.
class RuntimeSnapshotControl {
 public:
  virtual ~RuntimeSnapshotControl() = default;
  virtual bool take(RuntimeSnapshotRequest *request) = 0;
  virtual bool wait(RuntimeSnapshotRequest *request) = 0;
  virtual RuntimeSnapshotRequestResult refresh(uint64_t request_id) = 0;
  virtual bool reserve_hard_gate(uint64_t request_id,
                                 std::string *error) = 0;
  virtual RuntimeSnapshotRequestResult complete(uint64_t request_id) = 0;
  virtual void mark_terminal(uint64_t request_id,
                             RuntimeSnapshotRequestOutcome outcome,
                             std::string_view detail) = 0;
};

// SnapshotPublisher preparation and the shared LOG/SNAPSHOT order-token
// publication boundary. The prepared value is unchanged on failure.
class RuntimeSnapshotPublicationDriver {
 public:
  virtual ~RuntimeSnapshotPublicationDriver() = default;
  virtual PublishResult prepare(
      const RuntimeSnapshotAcquisition &acquisition,
      PreparedSnapshotPublication *prepared) = 0;
  virtual PublishResult publish(
      uint64_t request_id, const PreparedSnapshotPublication &prepared,
      SnapshotPublication *publication) = 0;
};

class ServerRuntimeSnapshotControl final : public RuntimeSnapshotControl {
 public:
  bool take(RuntimeSnapshotRequest *request) override;
  bool wait(RuntimeSnapshotRequest *request) override;
  RuntimeSnapshotRequestResult refresh(uint64_t request_id) override;
  bool reserve_hard_gate(uint64_t request_id, std::string *error) override;
  RuntimeSnapshotRequestResult complete(uint64_t request_id) override;
  void mark_terminal(uint64_t request_id,
                     RuntimeSnapshotRequestOutcome outcome,
                     std::string_view detail) override;
};

class ServerRuntimeSnapshotPublicationDriver final
    : public RuntimeSnapshotPublicationDriver {
 public:
  explicit ServerRuntimeSnapshotPublicationDriver(
      SnapshotPublisher *snapshot_publisher)
      : snapshot_publisher_(snapshot_publisher) {}

  PublishResult prepare(const RuntimeSnapshotAcquisition &acquisition,
                        PreparedSnapshotPublication *prepared) override;
  PublishResult publish(uint64_t request_id,
                        const PreparedSnapshotPublication &prepared,
                        SnapshotPublication *publication) override;

 private:
  SnapshotPublisher *snapshot_publisher_;
};

enum class RuntimeSnapshotCoordinatorState : uint8_t {
  UNINITIALIZED,
  READY,
  RESERVING_HARD_GATE,
  ACQUIRING,
  PREPARING,
  PUBLISHING,
  COMPLETING_REQUEST,
  BLOCKED,
  REFIX_REQUIRED,
  FENCED,
  PERMANENT_ERROR,
  SHUTDOWN,
};

enum class RuntimeSnapshotCoordinatorOutcome : uint8_t {
  READY,
  IDLE,
  PUBLISHED,
  BLOCKED,
  REFIX_REQUIRED,
  FENCED,
  PERMANENT_ERROR,
  SHUTDOWN,
};

struct RuntimeSnapshotCoordinatorResult {
  RuntimeSnapshotCoordinatorOutcome outcome{
      RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR};
  RuntimeSnapshotCoordinatorState state{
      RuntimeSnapshotCoordinatorState::UNINITIALIZED};
  uint64_t request_id{0};
  std::string detail;

  bool published() const {
    return outcome == RuntimeSnapshotCoordinatorOutcome::PUBLISHED;
  }
};

// Single-threaded coordinator. BLOCKED retains the current phase, while a
// pre-publication REFIX_REQUIRED discards the old fixed cut and reacquires
// under the same request ID. APPLIED publication is irreversible. A hard
// reservation remains owned across retryable outcomes.
class RuntimeSnapshotCoordinator {
 public:
  RuntimeSnapshotCoordinator(RuntimeSnapshotControl *control,
                             RuntimeSnapshotAcquirer *acquirer,
                             RuntimeSnapshotPublicationDriver *publication)
      : control_(control),
        acquirer_(acquirer),
        publication_(publication) {}

  RuntimeSnapshotCoordinatorResult initialize();
  RuntimeSnapshotCoordinatorResult poll();
  RuntimeSnapshotCoordinatorResult wait();
  // Server shutdown must close/drain remote commit first so destroying an
  // active source lease cannot reopen admission while this worker stops.
  void shutdown();

  RuntimeSnapshotCoordinatorState state() const { return state_; }
  const std::optional<RuntimeSnapshotRequest> &active_request() const {
    return active_request_;
  }
  const std::optional<SnapshotPublication> &last_publication() const {
    return last_publication_;
  }

 private:
  RuntimeSnapshotCoordinatorResult next(bool block_for_request);
  RuntimeSnapshotCoordinatorResult advance();
  std::optional<RuntimeSnapshotCoordinatorResult> refresh_active_request();
  std::optional<RuntimeSnapshotCoordinatorResult> reserve_hard_gate_if_needed();
  RuntimeSnapshotCoordinatorResult fail(
      RuntimeSnapshotCoordinatorOutcome outcome,
      RuntimeSnapshotCoordinatorState state, std::string detail);
  RuntimeSnapshotCoordinatorResult terminal(
      RuntimeSnapshotCoordinatorOutcome outcome,
      RuntimeSnapshotCoordinatorState state,
      RuntimeSnapshotRequestOutcome request_outcome, std::string detail);
  RuntimeSnapshotCoordinatorResult stop_active(std::string detail);
  RuntimeSnapshotCoordinatorResult current(
      RuntimeSnapshotCoordinatorOutcome outcome, std::string detail = {}) const;
  RuntimeSnapshotCoordinatorResult handle_publish_failure(
      const PublishResult &result);
  void discard_fixed_cut();
  void finish_request(SnapshotPublication publication);

  RuntimeSnapshotControl *control_{nullptr};
  RuntimeSnapshotAcquirer *acquirer_{nullptr};
  RuntimeSnapshotPublicationDriver *publication_{nullptr};
  RuntimeSnapshotCoordinatorState state_{
      RuntimeSnapshotCoordinatorState::UNINITIALIZED};
  std::optional<RuntimeSnapshotRequest> active_request_;
  std::optional<RuntimeSnapshotAcquisition> acquisition_;
  std::optional<PreparedSnapshotPublication> prepared_;
  std::optional<SnapshotPublication> applied_publication_;
  std::optional<SnapshotPublication> last_publication_;
  bool hard_gate_reserved_{false};
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_COORDINATOR_INCLUDED
