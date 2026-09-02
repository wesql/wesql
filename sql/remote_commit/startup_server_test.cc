/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/startup_server.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include <signal.h>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;
using namespace wesql::remote_commit;

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "remote commit startup server test failed: " << message
              << '\n';
    std::exit(EXIT_FAILURE);
  }
}

struct TemporaryDirectory {
  TemporaryDirectory() {
    std::string pattern = "/tmp/wesql-startup-server-test-XXXXXX";
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    char *created = ::mkdtemp(writable.data());
    expect(created != nullptr, "cannot create temporary directory");
    path = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    fs::remove_all(path, ignored);
  }
  fs::path path;
};

void write_file(const fs::path &path, std::string_view bytes) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  expect(output.is_open(), "cannot open fixture file");
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  output.close();
  expect(output.good(), "cannot write fixture file");
}

bool has_argument(const std::vector<std::string> &arguments,
                  std::string_view expected) {
  return std::find(arguments.begin(), arguments.end(), expected) !=
         arguments.end();
}

bool has_prefix(const std::vector<std::string> &arguments,
                std::string_view prefix) {
  return std::any_of(arguments.begin(), arguments.end(),
                     [prefix](std::string_view argument) {
                       return argument.starts_with(prefix);
                     });
}

bool managed_options_valid(std::vector<std::string> arguments,
                           std::string *error) {
  std::vector<char *> argv;
  argv.reserve(arguments.size());
  for (std::string &argument : arguments) argv.push_back(argument.data());
  return validate_startup_managed_path_options(
      static_cast<int>(argv.size()), argv.data(), error);
}

bool declarative_remote_commit(std::vector<std::string> arguments,
                               bool *enabled, std::string *error) {
  std::vector<char *> argv;
  argv.reserve(arguments.size());
  for (std::string &argument : arguments) argv.push_back(argument.data());
  return detect_declarative_remote_commit(static_cast<int>(argv.size()),
                                          argv.data(), enabled, error);
}

void test_remote_commit_option_policy() {
  const auto expect_mode = [](std::vector<std::string> arguments,
                              bool expected) {
    bool enabled = !expected;
    std::string error;
    expect(declarative_remote_commit(std::move(arguments), &enabled, &error),
           error.c_str());
    expect(enabled == expected,
           "declarative remote-commit mode was detected incorrectly");
  };

  expect_mode({"mysqld"}, false);
  expect_mode({"mysqld", "--binlog_archive_remote_commit"}, true);
  expect_mode({"mysqld", "--binlog-archive-remote-commit=OFF"}, false);
  expect_mode({"mysqld", "--loose_binlog_archive_remote_commit=TrUe"},
              true);
  expect_mode({"mysqld", "--binlog-archive-remote-commit=1",
               "--skip-binlog_archive_remote_commit"},
              false);
  expect_mode({"mysqld", "--disable-binlog-archive-remote-commit=0"},
              true);
  expect_mode({"mysqld", "--enable-binlog-archive-remote-commit=0"},
              false);
  expect_mode({"mysqld", "--binlog-archive-remote-commit=OFF",
               "----args-separator----",
               "--binlog_archive_remote_commit=ON"},
              true);
  expect_mode({"mysqld", "--binlog-archive-remote-commit=ON",
               "--binlog-archive-remote-commit=invalid"},
              false);

  std::string error;
  expect(validate_remote_commit_option_source(true, true, &error),
         error.c_str());
  expect(validate_remote_commit_option_source(false, false, &error),
         error.c_str());
  expect(!validate_remote_commit_option_source(false, true, &error),
         "persisted-only remote commit passed the pre-orchestration gate");
  expect(!error.empty(), "persisted-only rejection omitted its reason");
}

void test_external_security_policy() {
  std::string error;
  expect(validate_startup_external_security({}, &error), error.c_str());

  StartupExternalSecurityOptions options;
  options.auto_generate_certs = true;
  expect(!validate_startup_external_security(options, &error),
         "accepted automatic TLS certificate generation");
  options = {};
  options.sha256_password_auto_generate_rsa_keys = true;
  expect(!validate_startup_external_security(options, &error),
         "accepted automatic sha256_password RSA generation");
  options = {};
  options.caching_sha2_password_auto_generate_rsa_keys = true;
  expect(!validate_startup_external_security(options, &error),
         "accepted automatic caching_sha2_password RSA generation");
}

void test_managed_path_options() {
  std::string error;
  const bool safe_options_valid = managed_options_valid(
      {"mysqld",
       "----args-separator----",
       "--innodb_data_home_dir=.",
       "--plugin-innodb-data-file-path",
       "ibdata1:12M:autoextend",
       "--loose-plugin-innodb-temp-data-file-path="
       "ibtmp1:12M:autoextend",
       "--innodb-log-group-home-dir=",
       "--plugin_innodb_undo_directory=.",
       "--innodb-directories=",
       "--innodb-doublewrite-dir=",
       "----persist-args-separator----",
       "--smartengine-data-dir=smartengine",
       "--loose-plugin-smartengine-wal-dir",
       "smartengine",
       "--xengine-persistent-cache-dir=",
       "--loose-plugin-xengine-persistent-cache-size=0"},
      &error);
  expect(safe_options_valid, error.c_str());

  const std::vector<std::vector<std::string>> rejected = {
      {"mysqld", "--innodb-data-home-dir=/external"},
      {"mysqld", "--innodb-undo-directory=../escape"},
      {"mysqld", "--innodb-doublewrite-dir=."},
      {"mysqld", "--innodb-temp-tablespaces-dir="},
      {"mysqld", "--smartengine-data-dir=/external"},
      {"mysqld", "--xengine-wal-dir=../escape"},
      {"mysqld", "--smartengine-persistent-cache-size=1"},
      {"mysqld", "--smartengine-data-dir=smartengine",
       "--xengine-data-dir=smartengine"},
      {"mysqld", "--innodb_data_home_dir=.",
       "--plugin-innodb-data-home-dir=."},
      {"mysqld", "--smartengine-wal-dir", "----args-separator----"},
      {"mysqld", "--smartengine-wal-dir", "--unknown-option=1"},
      {"mysqld", "--loose-loose-smartengine-data-dir=smartengine"},
      {"mysqld", "--skip-smartengine-data-dir=smartengine"},
  };
  for (const std::vector<std::string> &arguments : rejected) {
    error.clear();
    expect(!managed_options_valid(arguments, &error),
           "accepted an external or ambiguous managed-path option");
    expect(!error.empty(), "managed-path rejection omitted its reason");
  }

  const StartupInnoDBManagedPaths innodb_safe = {
      "", "ibdata1:12M:autoextend", "ibtmp1:12M:autoextend", ".", ".",
      "", "", ""};
  const bool innodb_safe_valid =
      validate_innodb_startup_managed_paths(innodb_safe, &error);
  expect(innodb_safe_valid, error.c_str());
  StartupInnoDBManagedPaths innodb_unsafe = innodb_safe;
  innodb_unsafe.temp_tablespaces_dir = "/external";
  expect(!validate_innodb_startup_managed_paths(innodb_unsafe, &error),
         "accepted an external InnoDB temporary-tablespace directory");
  innodb_unsafe = innodb_safe;
  innodb_unsafe.doublewrite_dir = ".";
  expect(!validate_innodb_startup_managed_paths(innodb_unsafe, &error),
         "accepted a nonempty InnoDB doublewrite directory");

  const StartupSmartengineManagedPaths smartengine_safe = {
      "smartengine", "smartengine", "", 0};
  const bool smartengine_safe_valid =
      validate_smartengine_startup_managed_paths(smartengine_safe, &error);
  expect(smartengine_safe_valid, error.c_str());
  StartupSmartengineManagedPaths smartengine_unsafe = smartengine_safe;
  smartengine_unsafe.persistent_cache_size = 1;
  expect(!validate_smartengine_startup_managed_paths(smartengine_unsafe,
                                                     &error),
         "accepted enabled SmartEngine persistent cache");
}

void test_modes() {
  for (const StartupServerMode expected :
       {StartupServerMode::NORMAL, StartupServerMode::BOOTSTRAP_PREFLIGHT,
        StartupServerMode::BOOTSTRAP_SNAPSHOT,
        StartupServerMode::TAKEOVER_RECOVERY,
        StartupServerMode::INSTALLED_REEXEC}) {
    StartupServerMode parsed = StartupServerMode::NORMAL;
    expect(parse_startup_server_mode(startup_server_mode_name(expected),
                                     &parsed),
           "cannot round-trip startup server mode");
    expect(parsed == expected, "startup server mode round trip changed value");
  }
  StartupServerMode ignored = StartupServerMode::NORMAL;
  expect(!parse_startup_server_mode("takeover", &ignored),
         "accepted incomplete takeover mode");
  expect(!parse_startup_server_mode("", &ignored),
         "accepted empty startup mode");
  expect(!parse_startup_server_mode("normal", nullptr),
         "accepted null startup mode output");
}

void test_canonical_startup_data_directory(const fs::path &directory) {
  const fs::path existing = directory / "existing-root";
  fs::create_directories(existing);
  const fs::path existing_expected = fs::weakly_canonical(existing);

  const std::vector<fs::path> existing_spellings = {
      existing,
      fs::path(existing.string() + "/"),
      fs::path(existing.string() + "/."),
      fs::path(existing.string() + "////"),
  };
  for (const fs::path &configured : existing_spellings) {
    fs::path resolved;
    std::string error;
    expect(canonical_startup_data_directory(configured, &resolved, &error),
           error.c_str());
    expect(resolved == existing_expected,
           "trailing-slash spelling changed an existing startup root");
  }

  const fs::path fresh = directory / "fresh-root";
  expect(!fs::exists(fresh), "fresh startup-root fixture already exists");
  const fs::path fresh_expected =
      fs::weakly_canonical(fresh.parent_path()) / fresh.filename();
  const std::vector<fs::path> fresh_spellings = {
      fresh,
      fs::path(fresh.string() + "/"),
      fs::path(fresh.string() + "/."),
      fs::path(fresh.string() + "////"),
  };
  for (const fs::path &configured : fresh_spellings) {
    fs::path resolved;
    std::string error;
    expect(canonical_startup_data_directory(configured, &resolved, &error),
           error.c_str());
    expect(resolved == fresh_expected,
           "trailing-slash spelling changed a fresh startup root");
  }

  fs::path resolved;
  std::string error;
  expect(!canonical_startup_data_directory(fs::path("/"), &resolved, &error),
         "accepted a root-only startup data directory");
  expect(!error.empty(), "root-only rejection omitted its reason");

  const fs::path missing_parent = directory / "missing-parent";
  expect(!fs::exists(missing_parent),
         "missing-parent fixture unexpectedly exists");
  expect(!canonical_startup_data_directory(missing_parent / "child", &resolved,
                                           &error),
         "accepted a startup root whose parent does not exist");
  expect(!error.empty(), "missing-parent rejection omitted its reason");
}

void test_proof_transport(const fs::path &directory) {
  std::string error;
  StartupProofReference reference;
  const fs::path proof = directory / "worker-proof.bin";
  const std::string expected("typed\0payload", 13);
  expect(publish_startup_output_proof(proof, expected, &reference, &error),
         error.c_str());
  expect(reference.path == proof, "published proof path changed");
  expect(reference.size == 13, "published proof size changed");
  expect(reference.sha256.size() == 64,
         "published proof digest has the wrong size");
  std::string payload;
  expect(read_startup_proof(reference, &payload, &error), error.c_str());
  expect(payload == expected, "proof round trip changed payload bytes");

  StartupProofReference wrong = reference;
  wrong.sha256.assign(64, '0');
  expect(!read_startup_proof(wrong, &payload, &error),
         "accepted proof with the wrong digest");
  expect(payload.empty(), "digest failure exposed proof bytes");
  wrong = reference;
  ++wrong.size;
  expect(!read_startup_proof(wrong, &payload, &error),
         "accepted proof with the wrong size");

  StartupProofReference duplicate;
  expect(!write_startup_proof(proof, "replacement", &duplicate, &error),
         "replaced an existing proof");
  expect(read_startup_proof(reference, &payload, &error), error.c_str());
  expect(payload == expected, "failed duplicate write changed proof bytes");
  expect(remove_startup_proof(reference, &error), error.c_str());
  expect(!fs::exists(proof), "removed proof remains on disk");
}

void test_control_directory(const fs::path &directory) {
  std::string error;
  fs::path control;
  expect(create_startup_control_directory(directory / "target", &control,
                                          &error),
         error.c_str());
  expect(control.parent_path() == directory,
         "control directory has the wrong parent");
  expect(control.filename().string().starts_with(".wesql-remote-startup-"),
         "control directory has the wrong prefix");
  expect(fs::is_directory(control), "control path is not a directory");
  write_file(control / "worker.err", "retained evidence\n");
  fs::create_directory(control / "snapshot-readback");
  write_file(control / "snapshot-readback" / "payload", "bytes\n");
  expect(remove_startup_control_directory(control, &error), error.c_str());
  expect(!fs::exists(control), "verified control directory was not removed");

  const fs::path quarantine =
      directory / ".wesql-remote-startup-ABC123.root";
  fs::create_directory(quarantine);
  expect(!remove_startup_control_directory(quarantine, &error),
         "control cleanup accepted a quarantined root");
  expect(fs::is_directory(quarantine), "control cleanup removed quarantine");
  fs::remove(quarantine);
}

StartupProofReference fake_reference(const fs::path &path) {
  return {path, 42, std::string(64, 'a')};
}

void test_child_argv(const fs::path &directory) {
  const std::vector<std::string> original = {
      "/opt/wesql/bin/mysqld",
      "--defaults-file=/etc/wesql.cnf",
      "--defaults_extra_file=/etc/wesql-extra.cnf",
      "--defaults-group-suffix=_remote",
      "--datadir",
      "/old/root",
      "--initialize",
      "--initialize-insecure",
      "--daemonize",
      "--loose-keep-after-daemonize=1",
      "--daemonize=ON",
      "--pid-file=/old/mysqld.pid",
      "--log-error=/old/mysqld.log",
      "--port",
      "3307",
      "--wesql-remote-startup-mode=stale",
      "--wesql-remote-startup-input=/stale",
      "--wesql-remote-startup-input-size=1",
      "--wesql-remote-startup-input-sha256=bad",
      "--wesql-remote-startup-output=/stale-output",
      "--wesql-remote-startup-daemon-pipe-fd=77",
      "--loose_wesql_remote_startup_output=/stale-output-alias",
      "--skip_networking=OFF",
      "--mysqlx=ON",
      "--loose_skip_mysqlx=OFF",
      "--bind-address=127.0.0.1",
      "--plugin-load-add=component_reference_cache",
  };
  StartupChildSpec spec;
  spec.mode = StartupServerMode::TAKEOVER_RECOVERY;
  spec.data_directory = directory / "fresh-root";
  spec.output_path = directory / "worker-evidence";
  spec.pid_file = directory / "worker.pid";
  spec.error_log = directory / "worker.log";
  spec.input = fake_reference(directory / "worker-request");

  std::vector<std::string> child;
  std::string error;
  expect(build_startup_child_argv(original, spec, &child, &error),
         error.c_str());
  expect(child.front() == original.front(), "child executable changed");
  expect(has_argument(child, "--defaults-file=/etc/wesql.cnf"),
         "child lost defaults-file");
  expect(has_argument(child,
                      "--defaults_extra_file=/etc/wesql-extra.cnf"),
         "child lost defaults-extra-file");
  expect(has_argument(child, "--defaults-group-suffix=_remote"),
         "child lost defaults group suffix");
  expect(has_argument(child,
                      "--plugin-load-add=component_reference_cache"),
         "child lost plugin-load option");
  expect(!has_argument(child, "--loose-keep-after-daemonize=1"),
         "child retained daemon-only option");
  expect(has_argument(child,
                      "--wesql-remote-startup-mode=takeover-recovery"),
         "child has the wrong startup mode");
  expect(has_argument(child, "--skip-networking"),
         "child networking is not disabled");
  expect(has_argument(child, "--skip-mysqlx"),
         "child mysqlx is not disabled");
  expect(has_argument(child, "--skip-daemonize"),
         "child daemonization is not disabled");
  expect(std::count(child.begin(), child.end(), "--skip-mysqlx") == 1,
         "child has duplicate skip-mysqlx options");
  expect(std::count(child.begin(), child.end(), "--skip-daemonize") == 1,
         "child has duplicate skip-daemonize options");
  expect(!has_argument(child, "--initialize"),
         "takeover child retained initialize option");
  expect(!has_argument(child, "--initialize-insecure"),
         "takeover child retained initialize-insecure option");
  expect(has_argument(child, "--skip-initialize"),
         "takeover child lacks initialize-off override");
  expect(has_argument(child, "--skip-initialize-insecure"),
         "takeover child lacks initialize-insecure-off override");
  expect(!has_prefix(child, "--daemonize"),
         "child retained daemonize option");
  expect(!has_prefix(child, "--datadir=/old"),
         "child retained the original data directory");
  expect(!has_prefix(child, "--wesql-remote-startup-mode=stale"),
         "child retained a stale startup mode");
  expect(!has_prefix(child, "--loose_wesql_remote_startup_output"),
         "child retained a stale startup output");
  expect(!has_prefix(child, "--bind-address"),
         "child retained bind address");
  expect(has_prefix(child, "--datadir=" + spec.data_directory.string()),
         "child lacks the requested data directory");
  expect(has_prefix(child,
                    "--wesql-remote-startup-input=" +
                        spec.input.path.string()),
         "child lacks the requested startup input");

  const std::vector<std::string> no_defaults = {
      original.front(), "--no-defaults", "--plugin-load-add=test_component"};
  expect(build_startup_child_argv(no_defaults, spec, &child, &error),
         error.c_str());
  expect(child.size() > 2 && child[1] == "--no-defaults" &&
             child[2] == "--plugin-load-add=test_component",
         "child reordered no-defaults preamble");

  spec = {};
  spec.mode = StartupServerMode::BOOTSTRAP_PREFLIGHT;
  spec.data_directory = directory / "bootstrap-root";
  spec.output_path = directory / "preflight-evidence";
  spec.pid_file = directory / "bootstrap.pid";
  spec.error_log = directory / "bootstrap.log";
  spec.initialize_insecure = true;
  expect(build_startup_child_argv(original, spec, &child, &error),
         error.c_str());
  expect(has_argument(child, "--initialize-insecure"),
         "bootstrap child lacks initialize-insecure");
  expect(has_argument(child, "--skip-initialize"),
         "bootstrap child lacks initialize-off override");
  expect(has_argument(child, "--skip-initialize-insecure"),
         "bootstrap child lacks initialize-insecure-off override");
  expect(child.back() == "--initialize-insecure",
         "bootstrap initialize-insecure is not the final override");
  expect(std::count(child.begin(), child.end(), "--skip-daemonize") == 1,
         "bootstrap child has duplicate skip-daemonize options");
  expect(!has_prefix(child, "--wesql-remote-startup-input="),
         "bootstrap child unexpectedly has startup input");

  spec.mode = StartupServerMode::INSTALLED_REEXEC;
  spec.initialize_insecure = false;
  spec.output_path.clear();
  spec.pid_file.clear();
  spec.error_log.clear();
  spec.input = fake_reference(directory / "restart-proof");
  spec.inherited_daemon_pipe_fd = 99;
  expect(build_startup_child_argv(original, spec, &child, &error),
         error.c_str());
  expect(has_argument(child,
                      "--wesql-remote-startup-mode=installed-reexec"),
         "re-exec child has the wrong startup mode");
  expect(!has_prefix(child, "--wesql-remote-startup-output="),
         "re-exec child unexpectedly has startup output");
  expect(std::count(child.begin(), child.end(), "--skip-daemonize") == 1,
         "re-exec child has duplicate skip-daemonize options");
  expect(has_argument(child, "--pid-file=/old/mysqld.pid"),
         "re-exec child lost the deployment pid file");
  expect(has_argument(child, "--log-error=/old/mysqld.log"),
         "re-exec child lost the deployment error log");
  expect(has_argument(child, "--port") && has_argument(child, "3307"),
         "re-exec child lost the deployment TCP port");
  expect(has_argument(child, "--bind-address=127.0.0.1"),
         "re-exec child lost the deployment bind address");
  expect(has_argument(child, "--skip_networking=OFF"),
         "re-exec child lost the deployment networking setting");
  expect(has_argument(child, "--wesql-remote-startup-daemon-pipe-fd=99"),
         "re-exec child lost the inherited daemon status pipe");
  expect(!has_argument(child, "--wesql-remote-startup-daemon-pipe-fd=77"),
         "re-exec child retained a stale daemon status pipe");
  expect(has_argument(child, "--skip-initialize") &&
             has_argument(child, "--skip-initialize-insecure"),
         "re-exec child lacks explicit initialize-off overrides");
  expect(!has_argument(child, "--initialize") &&
             !has_argument(child, "--initialize-insecure"),
         "re-exec child retained an initialize request");

  std::vector<std::string> print_defaults = original;
  print_defaults.push_back("--print_defaults");
  expect(!build_startup_child_argv(print_defaults, spec, &child, &error),
         "accepted print-defaults for startup child");

  std::vector<std::string> relative_defaults = original;
  relative_defaults[1] = "--defaults-file=wesql.cnf";
  expect(!build_startup_child_argv(relative_defaults, spec, &child, &error),
         "accepted a relative defaults-file for re-exec");
  relative_defaults = original;
  relative_defaults[2] = "--defaults_extra_file=conf/wesql-extra.cnf";
  expect(!build_startup_child_argv(relative_defaults, spec, &child, &error),
         "accepted a relative defaults-extra-file for re-exec");
  expect(!build_startup_child_argv(
             {original.front(), "--defaults-file", "relative.cnf"}, spec,
             &child, &error),
         "accepted a separated relative defaults-file for re-exec");
}

void test_process_helpers() {
  const StartupProcessResult success =
      spawn_startup_child("/bin/sh", {"/bin/sh", "-c", "exit 0"});
  expect(success.succeeded(), "successful child did not report success");
  const StartupProcessResult failure =
      spawn_startup_child("/bin/sh", {"/bin/sh", "-c", "exit 7"});
  expect(failure.started && failure.exited && failure.exit_code == 7,
         "failed child did not preserve its exit code");
  expect(!failure.succeeded(), "failed child reported success");
  const StartupProcessResult signaled = spawn_startup_child(
      "/bin/sh", {"/bin/sh", "-c", "kill -TERM $$"});
  expect(signaled.started && !signaled.exited &&
             signaled.signal_number == SIGTERM,
         "signaled child did not preserve its signal");

  fs::path executable;
  std::string error;
  expect(resolve_current_executable("startup_server_test", &executable,
                                    &error),
         error.c_str());
  expect(executable.is_absolute(), "resolved executable path is not absolute");
}

void test_stable_root(const fs::path &directory) {
  const fs::path root = directory / "root";
  fs::create_directories(root / "mysql" / "nested");
  write_file(root / "auto.cnf", "uuid\n");
  write_file(root / "mysql" / "nested" / "table.ibd", "table-bytes");

  StartupRootSnapshot first;
  StartupRootSnapshot second;
  std::string error;
  expect(snapshot_stable_startup_root(root, &first, &error), error.c_str());
  expect(first.entries.size() == 4, "root snapshot has the wrong entry count");
  expect(first.manifest.entry_count == 4,
         "root manifest has the wrong entry count");
  expect(first.manifest.regular_file_count == 2,
         "root manifest has the wrong regular file count");
  expect(first.manifest.regular_file_bytes == 16,
         "root manifest has the wrong byte count");
  expect(first.manifest.manifest_sha256 ==
             "2c8dfcba697cbbcca6a127052b309c76bcb69a01ca4b3c5081898092634e0720",
         "root manifest digest changed");
  expect(snapshot_stable_startup_root(root, &second, &error), error.c_str());
  expect(first == second, "stable root snapshot is not repeatable");

  write_file(root / "auto.cnf", "other-uuid\n");
  expect(snapshot_stable_startup_root(root, &second, &error), error.c_str());
  expect(first != second, "root mutation did not change the snapshot");
  expect(!snapshot_stable_startup_root(root, &second, &error, 2),
         "accepted root above the entry limit");
  expect(!snapshot_stable_startup_root(root, &second, &error,
                                       kStartupRootMaxEntries, 4),
         "accepted root above the byte limit");

  const fs::path link = root / "forbidden-link";
  fs::create_symlink(root / "auto.cnf", link);
  expect(!snapshot_stable_startup_root(root, &second, &error),
         "accepted symlink in startup root");
}

}  // namespace

int main() {
  TemporaryDirectory temporary;
  test_modes();
  test_remote_commit_option_policy();
  test_external_security_policy();
  test_managed_path_options();
  test_canonical_startup_data_directory(temporary.path);
  test_control_directory(temporary.path);
  test_proof_transport(temporary.path);
  test_child_argv(temporary.path);
  test_process_helpers();
  test_stable_root(temporary.path);
  return 0;
}
