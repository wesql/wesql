/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/runtime_snapshot_service.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string_view>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "my_thread.h"
#include "sql/auto_thd.h"
#include "sql/remote_commit/runtime_snapshot_worker.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/sql_class.h"

namespace fs = std::filesystem;

namespace wesql::remote_commit {
namespace {

enum class ServicePhase : uint8_t {
  STOPPED,
  STARTING,
  READY,
  FAILED,
  STOPPING,
};

struct RuntimeSnapshotServiceState {
  std::mutex mutex;
  std::condition_variable condition;
  ServicePhase phase{ServicePhase::STOPPED};
  bool thread_created{false};
  my_thread_handle thread{};
  std::atomic<bool> shutdown_requested{false};
  THD *thd{nullptr};
  ProductionRuntimeSnapshotWorker *worker{nullptr};
  fs::path service_root;
  std::string error;
};

RuntimeSnapshotServiceState g_service;

bool set_error(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return true;
}

bool fsync_directory(const fs::path &path) {
  const int descriptor =
      ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (descriptor < 0) return false;
  const bool synced = ::fsync(descriptor) == 0;
  const bool closed = ::close(descriptor) == 0;
  return synced && closed;
}

bool create_service_root(const fs::path &root, std::string *error) {
  std::error_code filesystem_error;
  if (root.empty() || !root.is_absolute() || root.lexically_normal() != root ||
      root.filename().empty() ||
      fs::exists(fs::symlink_status(root, filesystem_error)) ||
      filesystem_error) {
    return set_error(error,
                     "runtime snapshot service root is not a fresh absolute path");
  }
  const fs::file_status parent =
      fs::symlink_status(root.parent_path(), filesystem_error);
  if (filesystem_error || !fs::is_directory(parent) || fs::is_symlink(parent)) {
    return set_error(error,
                     "runtime snapshot service parent is not a real directory");
  }
  if (::mkdir(root.c_str(), 0700) != 0) {
    return set_error(error, "cannot create private runtime snapshot service root");
  }
  if (!fsync_directory(root.parent_path())) {
    std::error_code cleanup_error;
    const bool removed = fs::remove(root, cleanup_error);
    const bool cleanup_synced = fsync_directory(root.parent_path());
    if (cleanup_error || !removed || !cleanup_synced) {
      return set_error(
          error,
          "cannot fsync or durably remove runtime snapshot service root");
    }
    return set_error(error,
                     "cannot fsync runtime snapshot service root creation");
  }
  return false;
}

bool remove_service_root(const fs::path &root, std::string *error) {
  if (root.empty()) return false;
  std::error_code filesystem_error;
  fs::remove_all(root, filesystem_error);
  const bool remains = fs::exists(root, filesystem_error);
  if (filesystem_error || remains || !fsync_directory(root.parent_path())) {
    return set_error(error, "cannot remove runtime snapshot service root");
  }
  return false;
}

void report_thread_failure(std::string detail) {
  std::lock_guard<std::mutex> guard(g_service.mutex);
  g_service.error = std::move(detail);
  g_service.phase = ServicePhase::FAILED;
  g_service.condition.notify_all();
}

void *runtime_snapshot_thread(void *) {
  if (my_thread_init()) {
    report_thread_failure("cannot initialize runtime snapshot system thread");
    return nullptr;
  }
  my_thread_self_setname("wesql-snapshot");

  {
    Auto_THD session;
    ProductionRuntimeSnapshotWorker worker(
        session.thd, g_service.service_root, &g_service.shutdown_requested);
    {
      std::lock_guard<std::mutex> guard(g_service.mutex);
      g_service.thd = session.thd;
      g_service.worker = &worker;
    }

    std::string error;
    if (!worker.initialize(&error)) {
      report_thread_failure(
          error.empty() ? "runtime snapshot worker initialization failed"
                        : std::move(error));
    } else {
      {
        std::lock_guard<std::mutex> guard(g_service.mutex);
        g_service.phase = ServicePhase::READY;
        g_service.condition.notify_all();
      }
      worker.run();
    }

    {
      std::lock_guard<std::mutex> guard(g_service.mutex);
      g_service.worker = nullptr;
      g_service.thd = nullptr;
      if (g_service.phase != ServicePhase::FAILED)
        g_service.phase = ServicePhase::STOPPED;
      g_service.condition.notify_all();
    }
  }

  my_thread_end();
  return nullptr;
}

void request_shutdown_locked() {
  g_service.phase = ServicePhase::STOPPING;
  g_service.shutdown_requested.store(true, std::memory_order_release);
  if (g_service.worker != nullptr) g_service.worker->request_shutdown();
  if (g_service.thd != nullptr) {
    mysql_mutex_lock(&g_service.thd->LOCK_thd_data);
    g_service.thd->awake(THD::KILL_CONNECTION);
    mysql_mutex_unlock(&g_service.thd->LOCK_thd_data);
  }
}

}  // namespace

bool start_runtime_snapshot_service(const fs::path &service_root,
                                    std::string *error) {
  if (error != nullptr) error->clear();
  std::unique_lock<std::mutex> lock(g_service.mutex);
  if (g_service.phase != ServicePhase::STOPPED || g_service.thread_created) {
    return set_error(error, "runtime snapshot service was started twice");
  }
  if (create_service_root(service_root, error)) return true;

  g_service.service_root = service_root;
  g_service.error.clear();
  g_service.shutdown_requested.store(false, std::memory_order_release);
  g_service.phase = ServicePhase::STARTING;

  my_thread_attr_t attributes;
  const int attr_error = my_thread_attr_init(&attributes);
  int create_error = attr_error;
  if (attr_error == 0) {
    create_error = my_thread_create(&g_service.thread, &attributes,
                                    runtime_snapshot_thread, nullptr);
    static_cast<void>(my_thread_attr_destroy(&attributes));
  }
  if (create_error != 0) {
    g_service.phase = ServicePhase::STOPPED;
    const fs::path failed_root = std::exchange(g_service.service_root, {});
    lock.unlock();
    std::string cleanup_error;
    if (remove_service_root(failed_root, &cleanup_error))
      return set_error(error, std::move(cleanup_error));
    return set_error(error, "cannot create runtime snapshot worker thread");
  }
  g_service.thread_created = true;
  g_service.condition.wait(lock, [] {
    return g_service.phase != ServicePhase::STARTING;
  });
  if (g_service.phase == ServicePhase::READY) return false;

  const std::string startup_error =
      g_service.error.empty() ? "runtime snapshot worker did not become ready"
                              : g_service.error;
  lock.unlock();
  static_cast<void>(my_thread_join(&g_service.thread, nullptr));
  lock.lock();
  g_service.thread_created = false;
  g_service.phase = ServicePhase::STOPPED;
  const fs::path failed_root = std::exchange(g_service.service_root, {});
  lock.unlock();
  std::string cleanup_error;
  if (remove_service_root(failed_root, &cleanup_error))
    return set_error(error, startup_error + "; " + cleanup_error);
  return set_error(error, startup_error);
}

void request_runtime_snapshot_service_shutdown() {
  std::unique_lock<std::mutex> lock(g_service.mutex);
  if (!g_service.thread_created) return;
  request_shutdown_locked();
}

void stop_runtime_snapshot_service() {
  std::unique_lock<std::mutex> lock(g_service.mutex);
  if (!g_service.thread_created) return;
  request_shutdown_locked();
  lock.unlock();

  if (my_thread_join(&g_service.thread, nullptr) != 0)
    fail_stop("cannot join runtime snapshot worker thread");

  lock.lock();
  g_service.thread_created = false;
  g_service.phase = ServicePhase::STOPPED;
  g_service.error.clear();
  const fs::path service_root = std::exchange(g_service.service_root, {});
  lock.unlock();

  std::string cleanup_error;
  if (remove_service_root(service_root, &cleanup_error))
    fail_stop(cleanup_error.c_str());
}

bool runtime_snapshot_service_ready() {
  std::lock_guard<std::mutex> guard(g_service.mutex);
  return g_service.phase == ServicePhase::READY && g_service.thread_created;
}

}  // namespace wesql::remote_commit
