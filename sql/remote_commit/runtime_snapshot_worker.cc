/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/runtime_snapshot_worker.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "sql/remote_commit/runtime_snapshot_coordinator.h"
#include "sql/remote_commit/runtime_snapshot_sources.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/remote_commit/snapshot_publisher.h"

namespace fs = std::filesystem;

namespace wesql::remote_commit {
namespace {

bool set_error(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return false;
}

bool create_private_directory(const fs::path &path, std::string *error) {
  if (::mkdir(path.c_str(), 0700) != 0)
    return set_error(error,
                     "cannot create runtime snapshot readback directory: " +
                         path.string());
  const int descriptor =
      ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (descriptor < 0)
    return set_error(error,
                     "cannot open runtime snapshot service directory");
  const bool synced = ::fsync(descriptor) == 0;
  const bool closed = ::close(descriptor) == 0;
  if (!synced || !closed)
    return set_error(error,
                     "cannot fsync runtime snapshot service directory");
  return true;
}

}  // namespace

struct ProductionRuntimeSnapshotWorker::Impl {
  Impl(THD *thd_arg, fs::path service_root_arg,
       std::atomic<bool> *shutdown_requested_arg)
      : thd(thd_arg),
        service_root(std::move(service_root_arg)),
        shutdown_requested(shutdown_requested_arg) {}

  bool backoff(std::chrono::milliseconds delay) {
    std::unique_lock<std::mutex> lock(backoff_mutex);
    return backoff_condition.wait_for(lock, delay, [this] {
      return shutdown_requested->load(std::memory_order_acquire);
    });
  }

  THD *thd;
  fs::path service_root;
  std::atomic<bool> *shutdown_requested;
  std::mutex backoff_mutex;
  std::condition_variable backoff_condition;

  StartupIoRuntime runtime;
  std::unique_ptr<ObjectStoreSnapshotExactFileReader> exact_reader;
  std::unique_ptr<ObjectStoreSnapshotPayloadIo> payload_io;
  std::unique_ptr<SnapshotPublisher> snapshot_publisher;
  std::unique_ptr<ServerRuntimeSnapshotControl> control;
  std::unique_ptr<ProductionRuntimeSnapshotAcquirer> acquirer;
  std::unique_ptr<ServerRuntimeSnapshotPublicationDriver> publication;
  std::unique_ptr<RuntimeSnapshotCoordinator> coordinator;
};

ProductionRuntimeSnapshotWorker::ProductionRuntimeSnapshotWorker(
    THD *thd, fs::path service_root,
    std::atomic<bool> *shutdown_requested)
    : impl_(std::make_unique<Impl>(thd, std::move(service_root),
                                  shutdown_requested)) {}

ProductionRuntimeSnapshotWorker::~ProductionRuntimeSnapshotWorker() = default;

bool ProductionRuntimeSnapshotWorker::initialize(std::string *error) {
  if (error != nullptr) error->clear();
  if (impl_ == nullptr || impl_->thd == nullptr ||
      impl_->shutdown_requested == nullptr || impl_->service_root.empty()) {
    return set_error(error, "runtime snapshot worker is not initialized");
  }
  if (impl_->shutdown_requested->load(std::memory_order_acquire)) {
    return set_error(error, "runtime snapshot worker startup was cancelled");
  }
  if (!startup_io_runtime(&impl_->runtime) ||
      impl_->runtime.object_store == nullptr ||
      impl_->runtime.conditional_io == nullptr ||
      impl_->runtime.publisher == nullptr || impl_->runtime.bucket.empty() ||
      impl_->runtime.stream.stream_id.empty()) {
    return set_error(error, "runtime snapshot IO authority is unavailable");
  }

  const fs::path scratch = impl_->service_root / "readback";
  if (!create_private_directory(scratch, error)) return false;

  impl_->exact_reader =
      std::make_unique<ObjectStoreSnapshotExactFileReader>(
          impl_->runtime.object_store);
  impl_->payload_io = std::make_unique<ObjectStoreSnapshotPayloadIo>(
      impl_->runtime.object_store, impl_->runtime.bucket,
      impl_->exact_reader.get(), scratch);
  impl_->snapshot_publisher = std::make_unique<SnapshotPublisher>(
      impl_->payload_io.get(), impl_->runtime.conditional_io,
      impl_->runtime.publisher, maximum_segment_bytes(),
      kSnapshotMaxTotalPayloadBytes);
  impl_->control = std::make_unique<ServerRuntimeSnapshotControl>();
  impl_->acquirer = std::make_unique<ProductionRuntimeSnapshotAcquirer>(
      impl_->thd, impl_->service_root, impl_->shutdown_requested);
  impl_->publication =
      std::make_unique<ServerRuntimeSnapshotPublicationDriver>(
          impl_->snapshot_publisher.get());
  impl_->coordinator = std::make_unique<RuntimeSnapshotCoordinator>(
      impl_->control.get(), impl_->acquirer.get(), impl_->publication.get());

  const RuntimeSnapshotCoordinatorResult initialized =
      impl_->coordinator->initialize();
  if (initialized.outcome != RuntimeSnapshotCoordinatorOutcome::READY) {
    return set_error(
        error, initialized.detail.empty()
                   ? "runtime snapshot coordinator did not become ready"
                   : initialized.detail);
  }
  return true;
}

void ProductionRuntimeSnapshotWorker::run() {
  using namespace std::chrono_literals;
  constexpr auto kInitialBackoff = 25ms;
  constexpr auto kMaximumBackoff = 1000ms;

  auto delay = kInitialBackoff;
  bool block_for_request = true;
  while (!impl_->shutdown_requested->load(std::memory_order_acquire)) {
    const RuntimeSnapshotCoordinatorResult result =
        block_for_request ? impl_->coordinator->wait()
                          : impl_->coordinator->poll();
    switch (result.outcome) {
      case RuntimeSnapshotCoordinatorOutcome::READY:
      case RuntimeSnapshotCoordinatorOutcome::IDLE:
      case RuntimeSnapshotCoordinatorOutcome::PUBLISHED:
        delay = kInitialBackoff;
        block_for_request = true;
        break;
      case RuntimeSnapshotCoordinatorOutcome::BLOCKED:
      case RuntimeSnapshotCoordinatorOutcome::REFIX_REQUIRED:
        if (impl_->backoff(delay)) break;
        delay = std::min(delay * 2, kMaximumBackoff);
        block_for_request = false;
        break;
      case RuntimeSnapshotCoordinatorOutcome::SHUTDOWN:
        impl_->shutdown_requested->store(true, std::memory_order_release);
        break;
      case RuntimeSnapshotCoordinatorOutcome::FENCED:
      case RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR: {
        const std::string detail =
            result.detail.empty()
                ? "runtime snapshot coordinator stopped terminally"
                : result.detail;
        fail_stop(detail.c_str());
      }
    }
  }
  impl_->coordinator->shutdown();
}

void ProductionRuntimeSnapshotWorker::request_shutdown() {
  if (impl_ == nullptr || impl_->shutdown_requested == nullptr) return;
  impl_->shutdown_requested->store(true, std::memory_order_release);
  impl_->backoff_condition.notify_all();
}

}  // namespace wesql::remote_commit
