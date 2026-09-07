/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_LOCAL_INSTALL_INCLUDED
#define SQL_REMOTE_COMMIT_LOCAL_INSTALL_INCLUDED

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

#include "sql/remote_commit/protocol_codec.h"

namespace wesql::remote_commit {

inline constexpr char kLocalInstallMarkerName[] =
    ".wesql-remote-commit-install.json";
inline constexpr uint64_t kLocalInstallMarkerMaxBytes = 64 * 1024;

struct InstalledHeadMarker {
  uint64_t generation{0};
  std::string body_sha256;
  std::string snapshot_id;
  std::string snapshot_manifest_sha256;
  Cursor snapshot_cursor;

  bool operator==(const InstalledHeadMarker &) const = default;
};

struct LocalInstallMarker {
  std::string stream_id;
  std::string server_uuid;
  InstalledHeadMarker installed_head;
  std::string config_digest;
  std::string binary_fingerprint;

  bool operator==(const LocalInstallMarker &) const = default;
};

bool serialize_local_install_marker(const LocalInstallMarker &marker,
                                    std::string *json, std::string *error);
bool parse_local_install_marker(std::string_view json,
                                LocalInstallMarker *marker,
                                std::string *error);

enum class TargetClass : uint8_t {
  EMPTY_TARGET,
  MANAGED_REPLACE,
  FOREIGN_OR_CORRUPT,
};

struct TargetClassification {
  TargetClass classification{TargetClass::FOREIGN_OR_CORRUPT};
  std::optional<LocalInstallMarker> marker;
  std::string detail;
};

TargetClassification classify_local_target(
    const std::filesystem::path &target,
    const LocalInstallMarker &expected_deployment,
    bool allow_managed_replace);

enum class InstallOutcome : uint8_t {
  INSTALLED,
  FOREIGN_OR_CORRUPT,
  UNSUPPORTED,
  LOCAL_IO_ERROR,
};

struct InstallResult {
  InstallOutcome outcome{InstallOutcome::LOCAL_IO_ERROR};
  TargetClass target_class{TargetClass::FOREIGN_OR_CORRUPT};
  std::optional<std::filesystem::path> quarantine_path;
  std::string detail;

  bool installed() const { return outcome == InstallOutcome::INSTALLED; }
};

// The temporary root must be a fresh sibling of target. On managed replace,
// the old root remains at temporary_root and is returned as quarantine_path.
InstallResult install_local_root(
    const std::filesystem::path &temporary_root,
    const std::filesystem::path &target,
    const LocalInstallMarker &expected_marker,
    bool allow_managed_replace);

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_LOCAL_INSTALL_INCLUDED
