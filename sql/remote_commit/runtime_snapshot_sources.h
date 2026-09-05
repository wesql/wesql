/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_SOURCES_INCLUDED
#define SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_SOURCES_INCLUDED

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "sql/remote_commit/runtime_snapshot_coordinator.h"

class THD;

namespace wesql::remote_commit {

enum class RuntimeSnapshotTreeKind : uint8_t {
  CLONE,
  SMARTENGINE,
};

// Classifies and fsyncs one completed engine staging tree. Clone paths are
// rooted at the future datadir; SmartEngine paths are namespaced below
// `smartengine/`. The output is unchanged on failure.
bool finalize_runtime_snapshot_tree(
    const std::filesystem::path &root, RuntimeSnapshotTreeKind kind,
    std::vector<LocalSnapshotPayload> *objects, std::string *error);

// Production source fixation for one coordinator thread. The caller owns the
// THD, service root, and shutdown flag for the full acquirer lifetime.
class ProductionRuntimeSnapshotAcquirer final : public RuntimeSnapshotAcquirer {
 public:
  ProductionRuntimeSnapshotAcquirer(THD *thd,
                                    std::filesystem::path service_root,
                                    std::atomic<bool> *shutdown_requested);
  ~ProductionRuntimeSnapshotAcquirer() override;

  ProductionRuntimeSnapshotAcquirer(
      const ProductionRuntimeSnapshotAcquirer &) = delete;
  ProductionRuntimeSnapshotAcquirer &operator=(
      const ProductionRuntimeSnapshotAcquirer &) = delete;

  RuntimeSnapshotAcquisitionResult acquire(
      const RuntimeSnapshotRequest &request) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_SOURCES_INCLUDED
