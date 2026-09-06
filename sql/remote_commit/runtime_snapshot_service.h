/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_SERVICE_INCLUDED
#define SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_SERVICE_INCLUDED

#include <filesystem>
#include <string>

namespace wesql::remote_commit {

// Starts one joinable background worker and waits until every production
// dependency and the coordinator are READY. MySQL-style return: false is
// success, true is error.
bool start_runtime_snapshot_service(const std::filesystem::path &service_root,
                                    std::string *error);

// Signals cancellation and wakes the worker without waiting for it. Server
// shutdown calls this before draining commit admission so an in-flight source
// lease can unwind and release its Clone/engine pins.
void request_runtime_snapshot_service_shutdown();

// Cancels retry backoff, wakes the worker THD, joins it, and removes its private
// service tree. This is idempotent. Cleanup failure is fail-stop.
void stop_runtime_snapshot_service();

bool runtime_snapshot_service_ready();

#ifdef WESQL_TEST
bool create_runtime_snapshot_service_root_for_test(
    const std::filesystem::path &root, std::string *error);
#endif

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_RUNTIME_SNAPSHOT_SERVICE_INCLUDED
