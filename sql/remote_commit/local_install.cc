/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/local_install.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <set>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#ifdef __linux__
#include <linux/fs.h>
#include <sys/syscall.h>
#endif

#include "my_rapidjson_size_t.h"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace wesql::remote_commit {
namespace {

namespace fs = std::filesystem;

constexpr char kMarkerFormat[] = "wesql.remote_commit.local_install";
constexpr uint64_t kMarkerVersion = 1;

bool fail_with(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return false;
}

bool valid_sha256(std::string_view value) {
  if (value.size() != 64) return false;
  for (const char byte : value) {
    if (!((byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f')))
      return false;
  }
  return true;
}

bool valid_text(std::string_view value, size_t maximum_bytes) {
  if (value.empty() || value.size() > maximum_bytes) return false;
  for (const unsigned char byte : value) {
    if (byte < 0x20 || byte == 0x7f) return false;
  }
  return true;
}

bool valid_uuid(std::string_view value) {
  if (value.size() != 36) return false;
  for (size_t index = 0; index < value.size(); ++index) {
    if (index == 8 || index == 13 || index == 18 || index == 23) {
      if (value[index] != '-') return false;
    } else if (!((value[index] >= '0' && value[index] <= '9') ||
                 (value[index] >= 'a' && value[index] <= 'f'))) {
      return false;
    }
  }
  return true;
}

bool valid_cursor(const Cursor &cursor) {
  if (!valid_text(cursor.file, 255) || cursor.pos < 4 ||
      cursor.pos > kJsonSafeIntegerMax)
    return false;
  const fs::path file(cursor.file);
  return file == file.filename() && file.lexically_normal() == file;
}

bool valid_marker(const LocalInstallMarker &marker, std::string *error) {
  if (!valid_text(marker.stream_id, kMaxObjectKeyBytes) ||
      !valid_uuid(marker.server_uuid) ||
      marker.installed_head.generation == 0 ||
      marker.installed_head.generation > kJsonSafeIntegerMax ||
      !valid_sha256(marker.installed_head.body_sha256) ||
      !valid_text(marker.installed_head.snapshot_id, kMaxOrdinaryIdBytes) ||
      !valid_sha256(marker.installed_head.snapshot_manifest_sha256) ||
      !valid_cursor(marker.installed_head.snapshot_cursor) ||
      !valid_sha256(marker.config_digest) ||
      !valid_sha256(marker.binary_fingerprint))
    return fail_with(error, "local install marker contains an invalid field");
  return true;
}

bool exact_members(const rapidjson::Value &object,
                   std::initializer_list<std::string_view> required) {
  if (!object.IsObject() || object.MemberCount() != required.size())
    return false;
  std::set<std::string_view> seen;
  for (auto member = object.MemberBegin(); member != object.MemberEnd();
       ++member) {
    const std::string_view name(member->name.GetString(),
                                member->name.GetStringLength());
    if (std::find(required.begin(), required.end(), name) == required.end() ||
        !seen.insert(name).second)
      return false;
  }
  return true;
}

bool json_string(const rapidjson::Value &object, const char *name,
                 std::string *value) {
  const auto found = object.FindMember(name);
  if (found == object.MemberEnd() || !found->value.IsString()) return false;
  value->assign(found->value.GetString(), found->value.GetStringLength());
  return true;
}

bool json_uint64(const rapidjson::Value &object, const char *name,
                 uint64_t *value) {
  const auto found = object.FindMember(name);
  if (found == object.MemberEnd() || !found->value.IsUint64() ||
      found->value.GetUint64() > kJsonSafeIntegerMax)
    return false;
  *value = found->value.GetUint64();
  return true;
}

bool write_all(int fd, std::string_view bytes, int *failure_errno) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    ssize_t written = 0;
    do {
      written = ::write(fd, bytes.data() + offset, bytes.size() - offset);
    } while (written < 0 && errno == EINTR);
    if (written <= 0) {
      *failure_errno = written == 0 ? EIO : errno;
      return false;
    }
    offset += static_cast<size_t>(written);
  }
  return true;
}

bool fsync_path(const fs::path &path, bool directory, std::string *error) {
  int flags = O_RDONLY;
#ifdef O_DIRECTORY
  if (directory) flags |= O_DIRECTORY;
#else
  (void)directory;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  const int fd = ::open(path.c_str(), flags);
  if (fd < 0)
    return fail_with(error, "cannot open path for fsync: " + path.string() +
                                ": " + std::strerror(errno));
  if (::fsync(fd) != 0) {
    const int failure_errno = errno;
    ::close(fd);
    return fail_with(error, "cannot fsync path: " + path.string() + ": " +
                                std::strerror(failure_errno));
  }
  if (::close(fd) != 0)
    return fail_with(error, "cannot close fsynced path: " + path.string() +
                                ": " + std::strerror(errno));
  return true;
}

bool sync_tree(const fs::path &root, std::string *error) {
  std::error_code filesystem_error;
  std::vector<fs::path> files;
  std::vector<fs::path> directories{root};
  for (const auto &entry : fs::recursive_directory_iterator(
           root, fs::directory_options::none, filesystem_error)) {
    if (filesystem_error) break;
    const fs::file_status status = entry.symlink_status(filesystem_error);
    if (filesystem_error) break;
    if (fs::is_symlink(status) || (!fs::is_regular_file(status) &&
                                   !fs::is_directory(status)))
      return fail_with(error,
                       "temporary root contains an unsupported file type: " +
                           entry.path().string());
    if (fs::is_regular_file(status)) files.push_back(entry.path());
    if (fs::is_directory(status)) directories.push_back(entry.path());
  }
  if (filesystem_error)
    return fail_with(error, "cannot enumerate temporary root: " +
                                filesystem_error.message());

  std::sort(files.begin(), files.end());
  for (const fs::path &file : files) {
    if (!fsync_path(file, false, error)) return false;
  }
  std::sort(directories.begin(), directories.end(),
            [](const fs::path &left, const fs::path &right) {
              const auto left_depth = std::distance(left.begin(), left.end());
              const auto right_depth =
                  std::distance(right.begin(), right.end());
              if (left_depth != right_depth) return left_depth > right_depth;
              return left.native() < right.native();
            });
  for (const fs::path &directory : directories) {
    if (!fsync_path(directory, true, error)) return false;
  }
  return true;
}

bool read_marker_file(const fs::path &root, LocalInstallMarker *marker,
                      std::string *error) {
  const fs::path marker_path = root / kLocalInstallMarkerName;
  std::error_code filesystem_error;
  const fs::file_status status = fs::symlink_status(marker_path,
                                                    filesystem_error);
  if (filesystem_error || !fs::is_regular_file(status) ||
      fs::is_symlink(status))
    return fail_with(error, "local install marker is missing or not regular");
  const uint64_t size = fs::file_size(marker_path, filesystem_error);
  if (filesystem_error || size == 0 || size > kLocalInstallMarkerMaxBytes)
    return fail_with(error, "local install marker has an invalid size");
  std::ifstream input(marker_path, std::ios::binary);
  if (!input) return fail_with(error, "cannot open local install marker");
  std::string body{std::istreambuf_iterator<char>(input),
                   std::istreambuf_iterator<char>()};
  if (input.bad() || body.size() != size)
    return fail_with(error, "cannot read complete local install marker");
  return parse_local_install_marker(body, marker, error);
}

bool write_marker_file(const fs::path &root,
                       const LocalInstallMarker &marker,
                       std::string *error) {
  std::string body;
  if (!serialize_local_install_marker(marker, &body, error)) return false;
  const fs::path temporary = root / ".wesql-remote-commit-install.tmp";
  const fs::path destination = root / kLocalInstallMarkerName;
  const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (fd < 0)
    return fail_with(error, "cannot create temporary install marker: " +
                                std::string(std::strerror(errno)));
  int failure_errno = 0;
  bool ok = write_all(fd, body, &failure_errno);
  if (ok && ::fsync(fd) != 0) {
    ok = false;
    failure_errno = errno;
  }
  if (::close(fd) != 0 && ok) {
    ok = false;
    failure_errno = errno;
  }
  if (!ok)
    return fail_with(error, "cannot durably write install marker: " +
                                std::string(std::strerror(failure_errno)));
  if (::rename(temporary.c_str(), destination.c_str()) != 0)
    return fail_with(error, "cannot publish install marker: " +
                                std::string(std::strerror(errno)));
  return fsync_path(root, true, error);
}

bool same_deployment(const LocalInstallMarker &left,
                     const LocalInstallMarker &right) {
  return left.stream_id == right.stream_id &&
         left.server_uuid == right.server_uuid &&
         left.config_digest == right.config_digest &&
         left.binary_fingerprint == right.binary_fingerprint;
}

bool same_parent_and_mount(const fs::path &temporary_root,
                           const fs::path &target, std::string *error) {
  std::error_code filesystem_error;
  const fs::path temporary_parent =
      fs::weakly_canonical(temporary_root.parent_path(), filesystem_error);
  if (filesystem_error)
    return fail_with(error, "cannot resolve temporary-root parent");
  const fs::path target_parent =
      fs::weakly_canonical(target.parent_path(), filesystem_error);
  if (filesystem_error || temporary_parent != target_parent)
    return fail_with(error,
                     "temporary root and target are not sibling paths");
  struct stat temporary_stat {};
  struct stat parent_stat {};
  if (::stat(temporary_root.c_str(), &temporary_stat) != 0 ||
      ::stat(target_parent.c_str(), &parent_stat) != 0 ||
      temporary_stat.st_dev != parent_stat.st_dev)
    return fail_with(error,
                     "temporary root and target parent are not on one mount");
  return true;
}

InstallResult install_result(InstallOutcome outcome, TargetClass target_class,
                             std::string detail = {}) {
  InstallResult result;
  result.outcome = outcome;
  result.target_class = target_class;
  result.detail = std::move(detail);
  return result;
}

}  // namespace

bool serialize_local_install_marker(const LocalInstallMarker &marker,
                                    std::string *json, std::string *error) {
  if (json == nullptr || !valid_marker(marker, error)) return false;
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  writer.Key("format");
  writer.String(kMarkerFormat);
  writer.Key("version");
  writer.Uint64(kMarkerVersion);
  writer.Key("stream_id");
  writer.String(marker.stream_id.data(), marker.stream_id.size());
  writer.Key("server_uuid");
  writer.String(marker.server_uuid.data(), marker.server_uuid.size());
  writer.Key("installed_head");
  writer.StartObject();
  writer.Key("generation");
  writer.Uint64(marker.installed_head.generation);
  writer.Key("body_sha256");
  writer.String(marker.installed_head.body_sha256.data(),
                marker.installed_head.body_sha256.size());
  writer.Key("snapshot_id");
  writer.String(marker.installed_head.snapshot_id.data(),
                marker.installed_head.snapshot_id.size());
  writer.Key("snapshot_manifest_sha256");
  writer.String(marker.installed_head.snapshot_manifest_sha256.data(),
                marker.installed_head.snapshot_manifest_sha256.size());
  writer.Key("snapshot_cursor");
  writer.StartObject();
  writer.Key("file");
  writer.String(marker.installed_head.snapshot_cursor.file.data(),
                marker.installed_head.snapshot_cursor.file.size());
  writer.Key("pos");
  writer.Uint64(marker.installed_head.snapshot_cursor.pos);
  writer.EndObject();
  writer.EndObject();
  writer.Key("config_digest");
  writer.String(marker.config_digest.data(), marker.config_digest.size());
  writer.Key("binary_fingerprint");
  writer.String(marker.binary_fingerprint.data(),
                marker.binary_fingerprint.size());
  writer.EndObject();
  if (!writer.IsComplete() || buffer.GetSize() > kLocalInstallMarkerMaxBytes)
    return fail_with(error, "cannot serialize bounded local install marker");
  json->assign(buffer.GetString(), buffer.GetSize());
  return true;
}

bool parse_local_install_marker(std::string_view json,
                                LocalInstallMarker *marker,
                                std::string *error) {
  if (marker == nullptr || json.empty() ||
      json.size() > kLocalInstallMarkerMaxBytes)
    return fail_with(error, "local install marker input is invalid");
  rapidjson::Document document;
  document.Parse(json.data(), json.size());
  if (document.HasParseError() ||
      !exact_members(document,
                     {"format", "version", "stream_id", "server_uuid",
                      "installed_head", "config_digest",
                      "binary_fingerprint"}))
    return fail_with(error, "local install marker schema is invalid");
  const auto format = document.FindMember("format");
  const auto version = document.FindMember("version");
  const auto installed = document.FindMember("installed_head");
  if (format == document.MemberEnd() || !format->value.IsString() ||
      std::string_view(format->value.GetString(),
                       format->value.GetStringLength()) != kMarkerFormat ||
      version == document.MemberEnd() || !version->value.IsUint64() ||
      version->value.GetUint64() != kMarkerVersion ||
      installed == document.MemberEnd() ||
      !exact_members(installed->value,
                     {"generation", "body_sha256", "snapshot_id",
                      "snapshot_manifest_sha256", "snapshot_cursor"}))
    return fail_with(error, "local install marker version is unsupported");
  const auto cursor = installed->value.FindMember("snapshot_cursor");
  if (cursor == installed->value.MemberEnd() ||
      !exact_members(cursor->value, {"file", "pos"}))
    return fail_with(error, "local install marker cursor is invalid");

  LocalInstallMarker parsed;
  if (!json_string(document, "stream_id", &parsed.stream_id) ||
      !json_string(document, "server_uuid", &parsed.server_uuid) ||
      !json_string(document, "config_digest", &parsed.config_digest) ||
      !json_string(document, "binary_fingerprint",
                   &parsed.binary_fingerprint) ||
      !json_uint64(installed->value, "generation",
                   &parsed.installed_head.generation) ||
      !json_string(installed->value, "body_sha256",
                   &parsed.installed_head.body_sha256) ||
      !json_string(installed->value, "snapshot_id",
                   &parsed.installed_head.snapshot_id) ||
      !json_string(installed->value, "snapshot_manifest_sha256",
                   &parsed.installed_head.snapshot_manifest_sha256) ||
      !json_string(cursor->value, "file",
                   &parsed.installed_head.snapshot_cursor.file) ||
      !json_uint64(cursor->value, "pos",
                   &parsed.installed_head.snapshot_cursor.pos) ||
      !valid_marker(parsed, error))
    return fail_with(error, "local install marker field type is invalid");
  *marker = std::move(parsed);
  return true;
}

TargetClassification classify_local_target(
    const fs::path &target, const LocalInstallMarker &expected_deployment,
    bool allow_managed_replace) {
  TargetClassification result;
  std::error_code filesystem_error;
  const fs::file_status status = fs::symlink_status(target, filesystem_error);
  if (filesystem_error == std::errc::no_such_file_or_directory ||
      status.type() == fs::file_type::not_found) {
    result.classification = TargetClass::EMPTY_TARGET;
    return result;
  }
  if (filesystem_error || !fs::is_directory(status) || fs::is_symlink(status)) {
    result.detail = "target is not a readable directory";
    return result;
  }
  if (fs::is_empty(target, filesystem_error) && !filesystem_error) {
    result.classification = TargetClass::EMPTY_TARGET;
    return result;
  }
  if (filesystem_error) {
    result.detail = "cannot classify target directory: " +
                    filesystem_error.message();
    return result;
  }
  if (!allow_managed_replace) {
    result.detail = "bootstrap refuses every non-empty target";
    return result;
  }
  LocalInstallMarker existing;
  if (!read_marker_file(target, &existing, &result.detail) ||
      !same_deployment(existing, expected_deployment)) {
    if (result.detail.empty()) result.detail = "target marker is foreign";
    return result;
  }
  result.classification = TargetClass::MANAGED_REPLACE;
  result.marker = std::move(existing);
  return result;
}

InstallResult install_local_root(const fs::path &temporary_root,
                                 const fs::path &target,
                                 const LocalInstallMarker &expected_marker,
                                 bool allow_managed_replace) {
  std::string error;
  if (!valid_marker(expected_marker, &error))
    return install_result(InstallOutcome::FOREIGN_OR_CORRUPT,
                          TargetClass::FOREIGN_OR_CORRUPT, std::move(error));
  std::error_code filesystem_error;
  const fs::file_status temporary_status =
      fs::symlink_status(temporary_root, filesystem_error);
  if (filesystem_error || !fs::is_directory(temporary_status) ||
      fs::is_symlink(temporary_status))
    return install_result(InstallOutcome::FOREIGN_OR_CORRUPT,
                          TargetClass::FOREIGN_OR_CORRUPT,
                          "temporary root is not a real directory");

  LocalInstallMarker actual_marker;
  const fs::path marker_path = temporary_root / kLocalInstallMarkerName;
  if (!fs::exists(marker_path, filesystem_error) && !filesystem_error) {
    if (!write_marker_file(temporary_root, expected_marker, &error))
      return install_result(InstallOutcome::LOCAL_IO_ERROR,
                            TargetClass::FOREIGN_OR_CORRUPT,
                            std::move(error));
  } else if (filesystem_error) {
    return install_result(InstallOutcome::LOCAL_IO_ERROR,
                          TargetClass::FOREIGN_OR_CORRUPT,
                          "cannot inspect temporary install marker");
  }
  if (!read_marker_file(temporary_root, &actual_marker, &error) ||
      actual_marker != expected_marker)
    return install_result(InstallOutcome::FOREIGN_OR_CORRUPT,
                          TargetClass::FOREIGN_OR_CORRUPT,
                          error.empty() ? "temporary marker differs from HEAD"
                                        : std::move(error));
  if (!sync_tree(temporary_root, &error) ||
      !same_parent_and_mount(temporary_root, target, &error))
    return install_result(InstallOutcome::LOCAL_IO_ERROR,
                          TargetClass::FOREIGN_OR_CORRUPT, std::move(error));

  const TargetClassification classification = classify_local_target(
      target, expected_marker, allow_managed_replace);
  if (classification.classification == TargetClass::FOREIGN_OR_CORRUPT)
    return install_result(InstallOutcome::FOREIGN_OR_CORRUPT,
                          classification.classification,
                          classification.detail);

  if (classification.classification == TargetClass::EMPTY_TARGET) {
    if (::rename(temporary_root.c_str(), target.c_str()) != 0)
      return install_result(InstallOutcome::LOCAL_IO_ERROR,
                            classification.classification,
                            "atomic empty-target rename failed: " +
                                std::string(std::strerror(errno)));
  } else {
#if defined(__linux__) && defined(SYS_renameat2) && defined(RENAME_EXCHANGE)
    if (::syscall(SYS_renameat2, AT_FDCWD, temporary_root.c_str(), AT_FDCWD,
                  target.c_str(), RENAME_EXCHANGE) != 0) {
      if (errno == ENOSYS || errno == EINVAL || errno == EOPNOTSUPP)
        return install_result(InstallOutcome::UNSUPPORTED,
                              classification.classification,
                              "RENAME_EXCHANGE is unavailable");
      return install_result(InstallOutcome::LOCAL_IO_ERROR,
                            classification.classification,
                            "managed root exchange failed: " +
                                std::string(std::strerror(errno)));
    }
#else
    return install_result(InstallOutcome::UNSUPPORTED,
                          classification.classification,
                          "RENAME_EXCHANGE is unavailable on this platform");
#endif
  }

  if (!fsync_path(target.parent_path(), true, &error))
    return install_result(InstallOutcome::LOCAL_IO_ERROR,
                          classification.classification, std::move(error));
  InstallResult installed = install_result(InstallOutcome::INSTALLED,
                                           classification.classification);
  if (classification.classification == TargetClass::MANAGED_REPLACE)
    installed.quarantine_path = temporary_root;
  return installed;
}

}  // namespace wesql::remote_commit
