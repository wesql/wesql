/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/startup_coordinator.h"

#include <filesystem>
#include <string>
#include <utility>

namespace wesql::remote_commit {

namespace fs = std::filesystem;

RecoveryReadResult DefaultStartupCoordinatorStorage::read(RecoveryPlan *plan) {
  return reader_.read(plan);
}

TargetClassification DefaultStartupCoordinatorStorage::classify(
    const fs::path &target, const LocalInstallMarker &expected_deployment,
    bool allow_managed_replace) {
  return classify_local_target(target, expected_deployment,
                               allow_managed_replace);
}

MaterializeResult DefaultStartupCoordinatorStorage::materialize(
    const RecoveryPlan &plan, const MaterializeOptions &options,
    MaterializedRoot *root) {
  return materializer_.materialize(plan, options, root);
}

InstallResult DefaultStartupCoordinatorStorage::install(
    const fs::path &temporary_root, const fs::path &target,
    const LocalInstallMarker &expected_marker, bool allow_managed_replace) {
  return install_local_root(temporary_root, target, expected_marker,
                            allow_managed_replace);
}

StartupStepResult DefaultStartupCoordinatorStorage::snapshot_stopped_root(
    const fs::path &root, StartupRootSnapshot *snapshot) {
  std::string error;
  if (!snapshot_stable_startup_root(root, snapshot, &error))
    return {StartupStepOutcome::LOCAL_IO_ERROR, std::move(error)};
  return {StartupStepOutcome::READY, {}};
}

}  // namespace wesql::remote_commit
