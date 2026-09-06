/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_STARTUP_ADAPTER_INCLUDED
#define SQL_REMOTE_COMMIT_STARTUP_ADAPTER_INCLUDED

#include <cstdint>
#include <string>

#include "my_inttypes.h"

namespace wesql::remote_commit {

enum class StartupAdapterOption : uint8_t {
  MODE,
  INPUT_PATH,
  INPUT_SIZE,
  INPUT_SHA256,
  OUTPUT_PATH,
  DAEMON_PIPE_FD,
  COUNT,
};

enum class StartupAdapterAction : uint8_t {
  CONTINUE,
  EXIT_CLEAN,
  ERROR,
};

// Internal options are parsed in the early-option pass. They are intentionally
// process-local and are accepted only in an authenticated child/re-exec shape.
extern char *opt_remote_startup_mode;
extern char *opt_remote_startup_input_path;
extern ulonglong opt_remote_startup_input_size;
extern char *opt_remote_startup_input_sha256;
extern char *opt_remote_startup_output_path;
extern ulong opt_remote_startup_daemon_pipe_fd;

// Called immediately after substitute_progpath(), before load_defaults()
// rewrites argv. Children are always derived from this owned original vector.
void capture_startup_original_argv(int argc, char **argv);
void record_startup_declarative_remote_commit(bool enabled);
void note_startup_adapter_option(StartupAdapterOption option);

// Runs after the server option pass and before daemonization. It validates the
// residual argv and internal child shape but deliberately does not initialize
// the object-store runtime or spawn children, so stock daemonization never
// forks a threaded remote runtime.
bool startup_before_datadir(int remaining_argc,
                            char *const remaining_argv[],
                            std::string *error);

// Installed re-exec carries the original daemon launcher's still-open status
// pipe. mysqld adopts that process role before its stock daemonization branch.
// The second hook runs immediately after that branch and before user/engine
// initialization. NORMAL mode orchestrates and re-execs from here; workers and
// installed re-exec authenticate their proof here before stock recovery.
int startup_inherited_daemon_pipe_fd();
bool startup_after_daemonization(int daemon_pipe_fd, bool daemonized,
                                 std::string *error);

// Internal workers never create listeners. Installed re-exec defers listener
// creation until post-engine proof has succeeded; NORMAL non-remote startup is
// unaffected.
bool startup_defer_networking();
bool startup_must_initialize_security_before_admission();
bool startup_force_skip_replica_start();
bool startup_networking_ready();

// Runs after replication repositories have been loaded with receiver/applier
// start suppressed. Workers perform bounded replay/evidence capture and then
// request clean exit. Installed re-exec performs the full post-engine check and
// opens commit admission before allowing networking.
StartupAdapterAction startup_after_repositories(std::string *error);

// Called by initialize-insecure after all system schemas/views have been
// created, immediately before its normal successful unireg_abort().
bool startup_finish_bootstrap_preflight(std::string *error);

// The first hook runs while engine/plugin APIs are still alive and releases
// any retained process-local snapshot lease. The second runs after clean_up()
// has stopped engines and removed volatile runtime files; only then is the
// bounded completion/preflight proof atomically published for the parent.
bool startup_prepare_clean_exit(std::string *error);
bool startup_finalize_clean_exit(std::string *error);
#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
void startup_arm_release_failure_for_test();
#endif

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_STARTUP_ADAPTER_INCLUDED
