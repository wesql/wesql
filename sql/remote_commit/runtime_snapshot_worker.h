/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_WORKER_INCLUDED
#define SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_WORKER_INCLUDED

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>

class THD;

namespace wesql::remote_commit {

// Thread-owned production composition of snapshot IO, acquisition, and the
// single-threaded coordinator. initialize() completes before the service can
// report READY. request_shutdown() interrupts retry backoff; server shutdown
// wakes a coordinator request wait.
class ProductionRuntimeSnapshotWorker {
 public:
  ProductionRuntimeSnapshotWorker(THD *thd,
                                  std::filesystem::path service_root,
                                  std::atomic<bool> *shutdown_requested);
  ~ProductionRuntimeSnapshotWorker();

  ProductionRuntimeSnapshotWorker(const ProductionRuntimeSnapshotWorker &) =
      delete;
  ProductionRuntimeSnapshotWorker &operator=(
      const ProductionRuntimeSnapshotWorker &) = delete;

  bool initialize(std::string *error);
  void run();
  void request_shutdown();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_WORKER_INCLUDED
