/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/local_install.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>

#include <unistd.h>

namespace rc = wesql::remote_commit;

namespace {

namespace fs = std::filesystem;

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "remote commit local install test failed: " << message
              << '\n';
    std::exit(EXIT_FAILURE);
  }
}

rc::LocalInstallMarker marker(char digest_byte = 'a') {
  rc::LocalInstallMarker value;
  value.stream_id = "r=repository/b=branch";
  value.server_uuid = "12345678-1234-1234-1234-123456789abc";
  value.installed_head.generation = 42;
  value.installed_head.body_sha256 = std::string(64, digest_byte);
  value.installed_head.snapshot_id = "snapshot-42";
  value.installed_head.snapshot_manifest_sha256 = std::string(64, 'b');
  value.installed_head.snapshot_cursor = {"binlog.000042", 4096};
  value.config_digest = std::string(64, 'c');
  value.binary_fingerprint = std::string(64, 'd');
  return value;
}

fs::path unique_root(std::string_view suffix) {
  static uint64_t sequence = 0;
  return fs::temp_directory_path() /
         ("wesql-remote-commit-install-" +
          std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
          std::to_string(++sequence) + "-" + std::string(suffix));
}

struct RootCleanup {
  explicit RootCleanup(fs::path value) : path(std::move(value)) {}
  ~RootCleanup() {
    std::error_code ignored;
    fs::remove_all(path, ignored);
  }
  fs::path path;
};

void write_file(const fs::path &path, std::string_view body) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  output.write(body.data(), static_cast<std::streamsize>(body.size()));
  output.close();
  expect(output.good(), "cannot write test fixture");
}

void write_marker(const fs::path &root, const rc::LocalInstallMarker &value) {
  std::string json;
  std::string error;
  expect(rc::serialize_local_install_marker(value, &json, &error),
         error.c_str());
  write_file(root / rc::kLocalInstallMarkerName, json);
}

void test_marker_codec() {
  const rc::LocalInstallMarker input = marker();
  std::string json;
  std::string error;
  expect(rc::serialize_local_install_marker(input, &json, &error),
         error.c_str());
  rc::LocalInstallMarker parsed;
  expect(rc::parse_local_install_marker(json, &parsed, &error) &&
             parsed == input,
         "marker round trip changed fields");

  std::string duplicate = json;
  duplicate.insert(duplicate.size() - 1, ",\"stream_id\":\"duplicate\"");
  expect(!rc::parse_local_install_marker(duplicate, &parsed, &error),
         "duplicate marker field was accepted");
  std::string unknown = json;
  unknown.insert(unknown.size() - 1, ",\"unknown\":1");
  expect(!rc::parse_local_install_marker(unknown, &parsed, &error),
         "unknown marker field was accepted");
  expect(!rc::parse_local_install_marker(json + "x", &parsed, &error),
         "trailing marker bytes were accepted");
}

void test_target_classification() {
  const fs::path parent = unique_root("classification");
  RootCleanup cleanup(parent);
  fs::create_directories(parent);
  const fs::path target = parent / "data";
  const rc::LocalInstallMarker expected = marker();

  expect(rc::classify_local_target(target, expected, true).classification ==
             rc::TargetClass::EMPTY_TARGET,
         "absent target was not empty");
  fs::create_directory(target);
  expect(rc::classify_local_target(target, expected, true).classification ==
             rc::TargetClass::EMPTY_TARGET,
         "empty directory target was not empty");
  write_file(target / "foreign", "x");
  expect(rc::classify_local_target(target, expected, true).classification ==
             rc::TargetClass::FOREIGN_OR_CORRUPT,
         "unmarked target was accepted");
  fs::remove(target / "foreign");
  write_marker(target, expected);
  expect(rc::classify_local_target(target, expected, true).classification ==
             rc::TargetClass::MANAGED_REPLACE,
         "matching managed target was rejected");
  expect(rc::classify_local_target(target, expected, false).classification ==
             rc::TargetClass::FOREIGN_OR_CORRUPT,
         "bootstrap accepted a managed target");
  rc::LocalInstallMarker foreign = expected;
  foreign.config_digest = std::string(64, 'e');
  expect(rc::classify_local_target(target, foreign, true).classification ==
             rc::TargetClass::FOREIGN_OR_CORRUPT,
         "foreign deployment fingerprint was accepted");
}

void test_empty_target_install() {
  const fs::path parent = unique_root("empty-install");
  RootCleanup cleanup(parent);
  fs::create_directories(parent);
  const fs::path temporary = parent / "restore.tmp";
  const fs::path target = parent / "data";
  fs::create_directory(temporary);
  write_file(temporary / "payload", "durable");

  const rc::LocalInstallMarker expected = marker();
  const rc::InstallResult installed =
      rc::install_local_root(temporary, target, expected, false);
  expect(installed.installed() &&
             installed.target_class == rc::TargetClass::EMPTY_TARGET,
         installed.detail.c_str());
  expect(!fs::exists(temporary) && fs::exists(target / "payload") &&
             fs::exists(target / rc::kLocalInstallMarkerName),
         "empty target rename did not install the complete root");
}

void test_foreign_and_managed_install() {
  {
    const fs::path parent = unique_root("foreign-install");
    RootCleanup cleanup(parent);
    fs::create_directories(parent);
    const fs::path temporary = parent / "restore.tmp";
    const fs::path target = parent / "data";
    fs::create_directory(temporary);
    fs::create_directory(target);
    write_file(temporary / "new", "new");
    write_file(target / "foreign", "old");
    const rc::InstallResult refused =
        rc::install_local_root(temporary, target, marker(), true);
    expect(refused.outcome == rc::InstallOutcome::FOREIGN_OR_CORRUPT &&
               fs::exists(temporary / "new") &&
               fs::exists(target / "foreign"),
           "foreign target was mutated");
  }
  {
    const fs::path parent = unique_root("managed-install");
    RootCleanup cleanup(parent);
    fs::create_directories(parent);
    const fs::path temporary = parent / "restore.tmp";
    const fs::path target = parent / "data";
    fs::create_directory(temporary);
    fs::create_directory(target);
    write_file(temporary / "new", "new");
    write_file(target / "old", "old");
    const rc::LocalInstallMarker expected = marker();
    write_marker(target, expected);
    const rc::InstallResult result =
        rc::install_local_root(temporary, target, expected, true);
#ifdef __linux__
    expect(result.installed() && result.quarantine_path == temporary,
           result.detail.c_str());
    expect(fs::exists(target / "new") && fs::exists(temporary / "old"),
           "managed exchange did not preserve both complete roots");
#else
    expect(result.outcome == rc::InstallOutcome::UNSUPPORTED &&
               fs::exists(temporary / "new") && fs::exists(target / "old"),
           "unsupported managed exchange mutated a root");
#endif
  }
}

}  // namespace

int main() {
  test_marker_codec();
  test_target_classification();
  test_empty_target_install();
  test_foreign_and_managed_install();
  std::cout << "remote commit local install tests passed\n";
  return EXIT_SUCCESS;
}
