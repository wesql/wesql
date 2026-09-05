/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_SERVER_ROOT_EVIDENCE_INCLUDED
#define SQL_REMOTE_COMMIT_SERVER_ROOT_EVIDENCE_INCLUDED

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "sql/remote_commit/startup_coordinator.h"

namespace wesql::remote_commit {

// Process-local view used by the worker and installed re-exec adapters. The
// parent coordinator carries only owned StartupWorkerRequest/completion values
// and never passes live server objects across the process boundary.
struct ServerRootVerificationRequest {
  StartupCoordinatorRoute route{StartupCoordinatorRoute::TAKEOVER};
  std::filesystem::path root;
  StartupDeploymentIdentity deployment;
  bool installed{false};
  const RecoveryPlan *candidate{nullptr};
  const RecoveryPlan *published{nullptr};
};

template <typename T>
struct ServerRootObservedValue {
  bool available{false};
  T value{};
  std::string detail;
};

struct ServerReplicationInventory {
  size_t channel_count{0};
  uint64_t source_rows{0};
  uint64_t relay_rows{0};
  uint64_t worker_rows{0};

  bool operator==(const ServerReplicationInventory &) const = default;
};

struct ServerPreparedInventory {
  size_t internal_entries{0};
  size_t external_entries{0};

  bool operator==(const ServerPreparedInventory &) const = default;
};

// Input is the sorted, complete persistent schema inventory from the DD.
bool canonical_initialized_schema_inventory(
    const std::vector<std::string> &schemas, bool performance_schema_compiled);

// Raw observations are separate from policy comparison so every unavailable
// production authority remains visible and the fail-closed decisions can be
// tested without fabricating server callbacks.
struct ServerRootEvidenceObservation {
  ServerRootObservedValue<bool> opened_root_matches;
  ServerRootObservedValue<Cursor> coherent_cursor;
  ServerRootObservedValue<GtidSetDigest> executed_gtid;
  ServerRootObservedValue<std::string> server_uuid;
  ServerRootObservedValue<StartupDeploymentIdentity> declared_deployment;
  ServerRootObservedValue<bool> dd_matches;
  ServerRootObservedValue<ServerReplicationInventory> replication;
  ServerRootObservedValue<Cursor> smartengine_snapshot_cursor;
  ServerRootObservedValue<std::vector<SmartengineExtentRef>>
      smartengine_live_extents;
  ServerRootObservedValue<ServerPreparedInventory> prepared;
  ServerRootObservedValue<std::optional<LocalInstallMarker>> installed_marker;

  // These are required only on the BOOTSTRAP route. A boolean value is not a
  // proof unless its containing observation is also available.
  ServerRootObservedValue<bool> empty_source_scan_stable;
  ServerRootObservedValue<bool> old_tc_authority_empty;
  ServerRootObservedValue<bool> user_state_empty;
  ServerRootObservedValue<bool> legacy_live_extents_empty;
};

// Installs the credential-free deployment declaration parsed by the mysqld
// startup adapter. The first declaration is immutable for the process;
// repeating the exact value is idempotent and a different value is rejected.
// Runtime evidence never copies request.deployment into an observation.
bool declare_server_root_runtime_deployment(
    const StartupDeploymentIdentity &declaration, std::string *error);

// Reads the immutable process-local declaration installed above. This is the
// single production accessor used by startup adapters and evidence capture;
// it never derives or fills fields from a worker request.
bool configured_server_root_runtime_deployment(
    StartupDeploymentIdentity *declaration, std::string *error);

#ifdef WESQL_SERVER_ROOT_EVIDENCE_TEST_ONLY
void reset_server_root_runtime_deployment_for_test();
#endif

// Validates, numerically sorts, and rewrites ordinals. Duplicate extent
// identities and duplicate object keys are rejected instead of deduplicated.
bool canonicalize_server_root_extents(
    const std::vector<SmartengineExtentRef> &input,
    std::vector<SmartengineExtentRef> *canonical, std::string *error);

// Pure fail-closed comparison seam. READY means every authority required by
// this verification phase was available and matched exactly. BLOCKED means a
// required authority was unavailable; CORRUPT means an available value did not
// match. Evidence is zeroed before every comparison.
StartupStepResult compare_server_root_evidence(
    const ServerRootVerificationRequest &request,
    const ServerRootEvidenceObservation &observation,
    StartupRootEvidence *evidence);

// Worker-local ownership of one SmartEngine backup snapshot. The worker must
// build and fsync its FixedSnapshotCut artifacts, prove that the cut carries
// exactly canonical_live_extents, then call release() before clean exit. This
// type and snapshot_id are never serialized, and remote publication starts
// only after the worker has exited cleanly.
class RetainedSmartengineSnapshotEvidence {
 public:
  RetainedSmartengineSnapshotEvidence();
  ~RetainedSmartengineSnapshotEvidence();
  RetainedSmartengineSnapshotEvidence(
      RetainedSmartengineSnapshotEvidence &&other) noexcept;
  RetainedSmartengineSnapshotEvidence &operator=(
      RetainedSmartengineSnapshotEvidence &&) = delete;
  RetainedSmartengineSnapshotEvidence(
      const RetainedSmartengineSnapshotEvidence &) = delete;
  RetainedSmartengineSnapshotEvidence &operator=(
      const RetainedSmartengineSnapshotEvidence &) = delete;

  bool active() const;
  StartupStepResult release();
#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
  void arm_release_failure_for_test();
#endif

  uint64_t snapshot_id{0};
  Cursor cursor;
  std::vector<SmartengineExtentRef> canonical_live_extents;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend StartupStepResult collect_server_root_evidence(
      const ServerRootVerificationRequest &, StartupRootEvidence *,
      RetainedSmartengineSnapshotEvidence *);
  friend StartupStepResult collect_server_root_observation(
      const ServerRootVerificationRequest &, ServerRootEvidenceObservation *,
      RetainedSmartengineSnapshotEvidence *);
};

// Captures every production authority currently exposed by MySQL/SmartEngine.
// READY means capture completed; individual unavailable authorities remain
// explicit in observation for the worker adapter to supplement or fail closed.
// When returned active, retained_smartengine must be explicitly released even
// if later comparison or cut export fails.
StartupStepResult collect_server_root_observation(
    const ServerRootVerificationRequest &request,
    ServerRootEvidenceObservation *observation,
    RetainedSmartengineSnapshotEvidence *retained_smartengine = nullptr);

// Production collector over existing MySQL and SmartEngine authorities.
// Without retained_smartengine, the SmartEngine snapshot stays leased through
// comparison and is released on every return path. A successful prepublication
// worker may instead retain it for local cut export through the worker-only
// token above; every other outcome still releases it here.
StartupStepResult collect_server_root_evidence(
    const ServerRootVerificationRequest &request, StartupRootEvidence *evidence,
    RetainedSmartengineSnapshotEvidence *retained_smartengine = nullptr);

// The initialize child has no binlog or SmartEngine yet. Verify its persisted
// system state and absence of old authority without acquiring a snapshot.
StartupStepResult verify_initialized_empty_root(
    const ServerRootVerificationRequest &request,
    std::string_view configured_binlog_basename,
    const std::filesystem::path &configured_binlog_index);

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_SERVER_ROOT_EVIDENCE_INCLUDED
