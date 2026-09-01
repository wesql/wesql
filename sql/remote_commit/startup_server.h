/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_STARTUP_SERVER_INCLUDED
#define SQL_REMOTE_COMMIT_STARTUP_SERVER_INCLUDED

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace wesql::remote_commit {

constexpr uint64_t kStartupProofMaxBytes = 512ULL * 1024 * 1024;
constexpr size_t kStartupRootMaxEntries = 1000000;

enum class StartupServerMode : uint8_t {
  NORMAL,
  BOOTSTRAP_PREFLIGHT,
  BOOTSTRAP_SNAPSHOT,
  TAKEOVER_RECOVERY,
  INSTALLED_REEXEC,
};

struct StartupProofReference {
  std::filesystem::path path;
  uint64_t size{0};
  std::string sha256;

  bool operator==(const StartupProofReference &) const = default;
};

struct StartupChildSpec {
  StartupServerMode mode{StartupServerMode::NORMAL};
  std::filesystem::path data_directory;
  std::filesystem::path output_path;
  std::filesystem::path pid_file;
  std::filesystem::path error_log;
  StartupProofReference input;
  bool initialize_insecure{false};
  int inherited_daemon_pipe_fd{-1};
};

struct StartupProcessResult {
  bool started{false};
  bool exited{false};
  int exit_code{-1};
  int signal_number{0};
  std::string detail;

  bool succeeded() const {
    return started && exited && exit_code == 0 && signal_number == 0;
  }
};

enum class StartupRootEntryType : uint8_t {
  DIRECTORY,
  REGULAR_FILE,
};

struct StartupRootEntry {
  std::string relative_path;
  StartupRootEntryType type{StartupRootEntryType::REGULAR_FILE};
  uint64_t size{0};
  std::string sha256;

  bool operator==(const StartupRootEntry &) const = default;
};

// Authenticated summary of one canonical, stopped-root inventory. This is the
// bounded form carried through restart proof transport; the full entry list is
// retained only while the parent compares publication/install checkpoints.
struct StartupStableRootManifest {
  uint64_t entry_count{0};
  uint64_t regular_file_count{0};
  uint64_t regular_file_bytes{0};
  std::string manifest_sha256;

  bool operator==(const StartupStableRootManifest &) const = default;
};

struct StartupRootSnapshot {
  std::vector<StartupRootEntry> entries;
  StartupStableRootManifest manifest;

  bool operator==(const StartupRootSnapshot &) const = default;
};

struct StartupInnoDBManagedPaths {
  std::string_view data_home_dir;
  std::string_view data_file_path;
  std::string_view temp_data_file_path;
  std::string_view log_group_home_dir;
  std::string_view undo_directory;
  std::string_view temp_tablespaces_dir;
  std::string_view directories;
  std::string_view doublewrite_dir;
};

struct StartupSmartengineManagedPaths {
  std::string_view data_dir;
  std::string_view wal_dir;
  std::string_view persistent_cache_dir;
  uint64_t persistent_cache_size{0};
};

struct StartupExternalSecurityOptions {
  bool auto_generate_certs{false};
  bool sha256_password_auto_generate_rsa_keys{false};
  bool caching_sha2_password_auto_generate_rsa_keys{false};
};

const char *startup_server_mode_name(StartupServerMode mode);
bool parse_startup_server_mode(std::string_view name,
                               StartupServerMode *mode);

// Reads the effective remote-commit boolean from the defaults-expanded argv.
// MySQL option aliases, explicit boolean values, negation, and ordering are
// interpreted before mysqld-auto.cnf is considered.
bool detect_declarative_remote_commit(int argc, char *const argv[],
                                      bool *enabled, std::string *error);

// Persisted state is not a declarative startup authority. A final enabled
// value is valid only when the defaults-expanded argv already enabled it.
bool validate_remote_commit_option_source(bool declarative_enabled,
                                          bool effective_enabled,
                                          std::string *error);

// Installed-root evidence must remain stable while admission is opened.
// Require every built-in TLS/RSA file generator to be disabled before TLS is
// initialized against the restored root.
bool validate_startup_external_security(
    const StartupExternalSecurityOptions &options, std::string *error);

// Remote startup owns one complete root. Before the parent creates control
// state or spawns a worker, reject plugin options that could place engine
// files outside that root. The engine-level validators are a second check over
// the values actually accepted by each plugin, before either engine opens.
bool validate_startup_managed_path_options(int argc, char *const argv[],
                                           std::string *error);
bool validate_innodb_startup_managed_paths(
    const StartupInnoDBManagedPaths &paths, std::string *error);
bool validate_smartengine_startup_managed_paths(
    const StartupSmartengineManagedPaths &paths, std::string *error);

StartupInnoDBManagedPaths innodb_startup_managed_paths_after_parse();
#ifdef WITH_SMARTENGINE
StartupSmartengineManagedPaths
smartengine_startup_managed_paths_after_parse();
#endif

// Creates one private sibling directory on the target filesystem. The caller
// retains it on failure as startup evidence and removes it after a verified
// installed-root re-exec.
bool create_startup_control_directory(
    const std::filesystem::path &target_root,
    std::filesystem::path *control_directory, std::string *error);
bool remove_startup_control_directory(
    const std::filesystem::path &control_directory, std::string *error);

// Proof payloads are typed and parsed by StartupCoordinator. This layer owns
// only exact, crash-durable transport and the bounded reference carried in
// argv; payload bytes never enter an environment variable or command line.
bool write_startup_proof(const std::filesystem::path &path,
                         std::string_view payload,
                         StartupProofReference *reference,
                         std::string *error,
                         uint64_t maximum_bytes = kStartupProofMaxBytes);
// Publishes a child output proof as a private, crash-durable file and returns
// the only reference the parent may consume. A mere configured output path is
// never evidence: callers must pass this exact absolute path/size/SHA triple
// through read_startup_proof().
bool publish_startup_output_proof(
    const std::filesystem::path &path, std::string_view payload,
    StartupProofReference *reference, std::string *error,
    uint64_t maximum_bytes = kStartupProofMaxBytes);
bool read_startup_proof(const StartupProofReference &reference,
                        std::string *payload, std::string *error,
                        uint64_t maximum_bytes = kStartupProofMaxBytes);
bool remove_startup_proof(const StartupProofReference &reference,
                          std::string *error);

// Removes stale internal mode/proof/datadir/initialize/daemon options and
// appends a single exact child specification. Worker modes also isolate
// listeners, pid, and logs. Installed re-exec preserves the deployment's
// runtime process options and carries an already-daemonized parent's pipe.
// `original` must be the argv captured at mysqld entry before option parsing
// mutates it.
bool build_startup_child_argv(const std::vector<std::string> &original,
                              const StartupChildSpec &spec,
                              std::vector<std::string> *child,
                              std::string *error);

bool resolve_current_executable(std::string_view argv0,
                                std::filesystem::path *executable,
                                std::string *error);
StartupProcessResult spawn_startup_child(
    const std::filesystem::path &executable,
    const std::vector<std::string> &arguments);
bool reexec_startup_server(const std::filesystem::path &executable,
                           const std::vector<std::string> &arguments,
                           std::string *error);

// The worker must have exited before this is called. Every directory and
// regular file is enumerated without following links; every file is streamed
// through SHA-256. Comparing snapshots on both sides of publication proves
// that the parent published and installed the same stopped root.
bool snapshot_stable_startup_root(
    const std::filesystem::path &root, StartupRootSnapshot *snapshot,
    std::string *error, size_t maximum_entries = kStartupRootMaxEntries,
    uint64_t maximum_regular_file_bytes = UINT64_MAX);

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_STARTUP_SERVER_INCLUDED
