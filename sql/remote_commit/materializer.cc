/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/materializer.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

#include <openssl/evp.h>
#include "sql/remote_commit/local_install.h"

namespace wesql::remote_commit {
namespace {

namespace fs = std::filesystem;

constexpr uint64_t kBinlogMagicBytes = 4;
constexpr uint64_t kEventFlagsOffset = 17;
constexpr uint16_t kBinlogInUseFlag = 0x1;

MaterializeResult result(MaterializeOutcome outcome,
                         std::string detail = {}) {
  return {outcome, std::move(detail)};
}

std::string status_detail(const objstore::Status &status) {
  return std::string(status.error_message());
}

bool checked_add(uint64_t value, uint64_t addend, uint64_t limit,
                 uint64_t *sum) {
  if (value > limit || addend > limit - value) return false;
  *sum = value + addend;
  return true;
}

bool canonical_relative_path(const fs::path &path) {
  if (path.empty() || path.is_absolute() || path.has_root_name() ||
      path.has_root_directory())
    return false;
  for (const fs::path &component : path) {
    if (component.empty() || component == "." || component == "..")
      return false;
  }
  return path.lexically_normal() == path;
}

bool path_prefix(const fs::path &prefix, const fs::path &path) {
  auto prefix_component = prefix.begin();
  auto path_component = path.begin();
  while (prefix_component != prefix.end() && path_component != path.end()) {
    if (*prefix_component != *path_component) return false;
    ++prefix_component;
    ++path_component;
  }
  return prefix_component == prefix.end();
}

bool insert_leaf_path(std::set<fs::path> *paths, const fs::path &path) {
  for (const fs::path &existing : *paths) {
    if (path_prefix(existing, path) || path_prefix(path, existing))
      return false;
  }
  paths->insert(path);
  return true;
}

bool parse_binlog_file(std::string_view file, std::string_view *basename,
                       uint64_t *sequence) {
  const size_t dot = file.rfind('.');
  if (dot == std::string_view::npos || dot == 0 || dot + 1 == file.size())
    return false;
  uint64_t parsed = 0;
  const auto converted = std::from_chars(file.data() + dot + 1,
                                         file.data() + file.size(), parsed);
  if (converted.ec != std::errc() || converted.ptr != file.data() + file.size())
    return false;
  *basename = file.substr(0, dot);
  *sequence = parsed;
  return true;
}

bool adjacent_binlog_file(std::string_view prior, std::string_view next) {
  std::string_view prior_base;
  std::string_view next_base;
  uint64_t prior_sequence = 0;
  uint64_t next_sequence = 0;
  return parse_binlog_file(prior, &prior_base, &prior_sequence) &&
         parse_binlog_file(next, &next_base, &next_sequence) &&
         prior_base == next_base &&
         prior_sequence != std::numeric_limits<uint64_t>::max() &&
         next_sequence == prior_sequence + 1;
}

bool fsync_path(const fs::path &path, bool directory, std::string *error) {
  int flags = O_RDONLY;
#ifdef O_DIRECTORY
  if (directory) flags |= O_DIRECTORY;
#else
  (void)directory;
#endif
  const int fd = ::open(path.c_str(), flags);
  if (fd < 0) {
    *error = "cannot open for fsync: " + path.string() + ": " +
             std::strerror(errno);
    return false;
  }
  const int sync_result = ::fsync(fd);
  const int saved_errno = errno;
  const int close_result = ::close(fd);
  if (sync_result != 0 || close_result != 0) {
    *error = "cannot fsync/close: " + path.string() + ": " +
             std::strerror(sync_result != 0 ? saved_errno : errno);
    return false;
  }
  return true;
}

bool sha256_file(const fs::path &path, std::string *digest,
                 std::string *error) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    *error = "cannot open downloaded object for hashing: " + path.string();
    return false;
  }
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (context == nullptr) {
    *error = "cannot allocate SHA-256 context";
    return false;
  }
  bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1;
  std::array<char, 1024 * 1024> buffer{};
  while (ok && input) {
    input.read(buffer.data(), buffer.size());
    const std::streamsize count = input.gcount();
    if (count > 0) {
      ok = EVP_DigestUpdate(context, buffer.data(),
                            static_cast<size_t>(count)) == 1;
    }
  }
  if (!input.eof()) ok = false;
  std::array<unsigned char, 32> bytes{};
  unsigned int length = 0;
  if (ok)
    ok = EVP_DigestFinal_ex(context, bytes.data(), &length) == 1 &&
         length == bytes.size();
  EVP_MD_CTX_free(context);
  if (!ok) {
    *error = "cannot stream SHA-256 for downloaded object: " + path.string();
    return false;
  }
  static constexpr char kHex[] = "0123456789abcdef";
  digest->clear();
  digest->reserve(bytes.size() * 2);
  for (const unsigned char byte : bytes) {
    digest->push_back(kHex[byte >> 4]);
    digest->push_back(kHex[byte & 0x0f]);
  }
  return true;
}

MaterializeResult download_verified(PayloadIo *io, std::string_view key,
                                    uint64_t expected_size,
                                    std::string_view expected_sha,
                                    uint64_t max_object_bytes,
                                    const fs::path &destination) {
  if (expected_size > max_object_bytes)
    return result(MaterializeOutcome::CORRUPT,
                  "payload exceeds the configured object limit: " +
                      std::string(key));
  std::error_code filesystem_error;
  if (fs::exists(destination, filesystem_error) || filesystem_error)
    return result(MaterializeOutcome::LOCAL_IO_ERROR,
                  "download destination already exists or is unreadable: " +
                      destination.string());
  if (!fs::create_directories(destination.parent_path(), filesystem_error) &&
      filesystem_error)
    return result(MaterializeOutcome::LOCAL_IO_ERROR,
                  "cannot create payload parent directory: " +
                      filesystem_error.message());

  const PayloadReadResult downloaded = io->download(key, destination);
  if (downloaded.outcome == PayloadReadOutcome::BLOCKED)
    return result(MaterializeOutcome::BLOCKED, downloaded.detail);
  if (downloaded.outcome == PayloadReadOutcome::ABSENT)
    return result(MaterializeOutcome::CORRUPT,
                  "required immutable payload is absent: " +
                      std::string(key));
  if (downloaded.outcome != PayloadReadOutcome::APPLIED)
    return result(MaterializeOutcome::CORRUPT,
                  "required immutable payload is unreadable: " +
                      std::string(key) + ": " + downloaded.detail);

  const uint64_t actual_size = fs::file_size(destination, filesystem_error);
  if (filesystem_error || actual_size != expected_size)
    return result(MaterializeOutcome::CORRUPT,
                  "downloaded payload size does not match immutable ref: " +
                      std::string(key));
  std::string actual_sha;
  std::string hash_error;
  if (!sha256_file(destination, &actual_sha, &hash_error) ||
      actual_sha != expected_sha)
    return result(MaterializeOutcome::CORRUPT,
                  hash_error.empty()
                      ? "downloaded payload SHA-256 does not match immutable "
                        "ref: " +
                            std::string(key)
                      : hash_error);
  if (!fsync_path(destination, false, &hash_error))
    return result(MaterializeOutcome::LOCAL_IO_ERROR, std::move(hash_error));
  return result(MaterializeOutcome::READY);
}

bool append_file(const fs::path &source, const fs::path &destination,
                 std::string *error) {
  const int input = ::open(source.c_str(), O_RDONLY);
  if (input < 0) {
    *error = "cannot open verified segment body: " + source.string();
    return false;
  }
  const int output = ::open(destination.c_str(), O_WRONLY | O_APPEND);
  if (output < 0) {
    const int saved_errno = errno;
    ::close(input);
    *error = "cannot open reconstructed binlog: " + destination.string() +
             ": " + std::strerror(saved_errno);
    return false;
  }
  bool ok = true;
  int failure_errno = 0;
  std::array<char, 1024 * 1024> buffer{};
  while (ok) {
    ssize_t count = 0;
    do {
      count = ::read(input, buffer.data(), buffer.size());
    } while (count < 0 && errno == EINTR);
    if (count == 0) break;
    if (count < 0) {
      ok = false;
      failure_errno = errno;
      break;
    }
    ssize_t offset = 0;
    while (offset < count) {
      ssize_t written = 0;
      do {
        written = ::write(output, buffer.data() + offset,
                          static_cast<size_t>(count - offset));
      } while (written < 0 && errno == EINTR);
      if (written <= 0) {
        ok = false;
        failure_errno = written == 0 ? EIO : errno;
        break;
      }
      offset += written;
    }
  }
  if (ok && ::fsync(output) != 0) {
    ok = false;
    failure_errno = errno;
  }
  if (::close(input) != 0 && ok) {
    ok = false;
    failure_errno = errno;
  }
  if (::close(output) != 0 && ok) {
    ok = false;
    failure_errno = errno;
  }
  if (!ok) {
    *error = "cannot append verified segment to reconstructed binlog: " +
             std::string(std::strerror(failure_errno));
  }
  return ok;
}

bool normalize_fde_flag(const fs::path &path, bool active,
                        std::string *error) {
  constexpr off_t kFlagsPosition =
      static_cast<off_t>(kBinlogMagicBytes + kEventFlagsOffset);
  const int fd = ::open(path.c_str(), O_RDWR);
  if (fd < 0) {
    *error = "cannot open binlog for FDE normalization: " + path.string();
    return false;
  }
  std::array<unsigned char, 2> encoded{};
  bool ok = ::pread(fd, encoded.data(), encoded.size(), kFlagsPosition) ==
            static_cast<ssize_t>(encoded.size());
  if (ok) {
    uint16_t flags = static_cast<uint16_t>(encoded[0]) |
                     static_cast<uint16_t>(encoded[1] << 8);
    if (active)
      flags |= kBinlogInUseFlag;
    else
      flags &= static_cast<uint16_t>(~kBinlogInUseFlag);
    encoded[0] = static_cast<unsigned char>(flags & 0xff);
    encoded[1] = static_cast<unsigned char>((flags >> 8) & 0xff);
    ok = ::pwrite(fd, encoded.data(), encoded.size(), kFlagsPosition) ==
             static_cast<ssize_t>(encoded.size()) &&
         ::fsync(fd) == 0;
  }
  const int saved_errno = errno;
  if (::close(fd) != 0) ok = false;
  if (!ok) {
    *error = "cannot normalize binlog FDE in-use bit: " + path.string() +
             ": " + std::strerror(saved_errno);
  }
  return ok;
}

bool write_index(const fs::path &path, const std::vector<fs::path> &files,
                 std::string *error) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (fd < 0) {
    *error = "cannot create reconstructed binlog index: " + path.string();
    return false;
  }
  bool ok = true;
  int failure_errno = 0;
  for (const fs::path &file : files) {
    std::string line = file.filename().string();
    line.push_back('\n');
    size_t offset = 0;
    while (offset < line.size()) {
      ssize_t written = 0;
      do {
        written = ::write(fd, line.data() + offset, line.size() - offset);
      } while (written < 0 && errno == EINTR);
      if (written <= 0) {
        ok = false;
        failure_errno = written == 0 ? EIO : errno;
        break;
      }
      offset += static_cast<size_t>(written);
    }
    if (!ok) break;
  }
  if (ok && ::fsync(fd) != 0) {
    ok = false;
    failure_errno = errno;
  }
  if (::close(fd) != 0 && ok) {
    ok = false;
    failure_errno = errno;
  }
  if (!ok) {
    *error = "cannot write reconstructed binlog index: " +
             std::string(std::strerror(failure_errno));
  }
  return ok;
}

ObjectRef payload_ref(const SmartengineExtentRef &extent) {
  return {extent.key, extent.size, extent.sha256};
}

ObjectRef payload_ref(const BinlogSeed &seed) {
  return {seed.key, seed.size, seed.sha256};
}

ObjectRef payload_ref(const SegmentRef &segment) {
  return {segment.key, segment.size, segment.sha256};
}

}  // namespace

PayloadReadResult ObjectStorePayloadIo::download(
    std::string_view key, const fs::path &destination) {
  if (object_store_ == nullptr)
    return {PayloadReadOutcome::PERMANENT_ERROR,
            "null object-store payload client"};
  const int output = open(destination.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          S_IRUSR | S_IWUSR);
  if (output < 0)
    return {PayloadReadOutcome::PERMANENT_ERROR,
            "cannot create exact payload destination: " +
                std::string(strerror(errno))};
  if (close(output) != 0)
    return {PayloadReadOutcome::PERMANENT_ERROR,
            "cannot close exact payload destination: " +
                std::string(strerror(errno))};

  const objstore::ExactFileResult downloaded =
      object_store_->get_object_to_file_exact(bucket_, key,
                                              destination.string());
  return classify_exact_file_result(downloaded, destination);
}

PayloadReadResult ObjectStorePayloadIo::classify_exact_file_result(
    const objstore::ExactFileResult &result, const fs::path &destination) {
  switch (result.outcome()) {
    case objstore::ExactFileOutcome::APPLIED:
      {
        std::error_code filesystem_error;
        const uintmax_t size = fs::file_size(destination, filesystem_error);
        if (filesystem_error || size != result.size())
          return {PayloadReadOutcome::PERMANENT_ERROR,
                  "exact payload GET size does not match streamed bytes"};
      }
      return {PayloadReadOutcome::APPLIED, {}};
    case objstore::ExactFileOutcome::NOT_FOUND_404:
      return {PayloadReadOutcome::ABSENT,
              status_detail(result.status())};
    case objstore::ExactFileOutcome::TRANSIENT_UNAVAILABLE:
      return {PayloadReadOutcome::BLOCKED,
              status_detail(result.status())};
    case objstore::ExactFileOutcome::PERMANENT_ERROR:
    case objstore::ExactFileOutcome::UNSUPPORTED:
      return {PayloadReadOutcome::PERMANENT_ERROR,
              status_detail(result.status())};
  }
  return {PayloadReadOutcome::PERMANENT_ERROR,
          "exact payload GET returned an unknown outcome"};
}

MaterializeResult RecoveryMaterializer::materialize(
    const RecoveryPlan &plan, const MaterializeOptions &options,
    MaterializedRoot *root) {
  if (root == nullptr || payload_io_ == nullptr ||
      binlog_validator_ == nullptr)
    return result(MaterializeOutcome::CORRUPT,
                  "null materializer input or validator");
  *root = {};
  if (options.max_object_bytes == 0 ||
      options.max_total_payload_bytes == 0 ||
      !canonical_relative_path(options.binlog_index_relative_path) ||
      options.temp_root.empty())
    return result(MaterializeOutcome::CORRUPT,
                  "invalid materializer limits or paths");
  if (plan.snapshot.binlog_seed.file !=
          plan.snapshot.binlog_seed.cursor.file ||
      plan.snapshot.binlog_seed.size !=
          plan.snapshot.binlog_seed.cursor.pos)
    return result(MaterializeOutcome::CORRUPT,
                  "binlog seed bytes differ from its native cursor");

  uint64_t total = 0;
  auto account = [&](uint64_t size) {
    return size <= options.max_object_bytes &&
           checked_add(total, size, options.max_total_payload_bytes, &total);
  };
  if (!account(plan.snapshot.binlog_seed.size))
    return result(MaterializeOutcome::CORRUPT,
                  "binlog seed exceeds materialization limits");
  for (const SnapshotObject &object : plan.snapshot.objects) {
    if (!account(object.size))
      return result(MaterializeOutcome::CORRUPT,
                    "snapshot payload aggregate exceeds materialization "
                    "limits");
  }
  for (const SmartengineExtentRef &extent : plan.snapshot.smartengine_extents) {
    if (!account(extent.size))
      return result(MaterializeOutcome::CORRUPT,
                    "extent payload aggregate exceeds materialization limits");
  }
  for (const SegmentRef &segment : plan.replay_segments) {
    if (segment.source.end_pos < segment.source.start_pos ||
        segment.source.end_pos - segment.source.start_pos != segment.size)
      return result(MaterializeOutcome::CORRUPT,
                    "segment bytes differ from native source coordinates");
    if (!account(segment.size))
      return result(MaterializeOutcome::CORRUPT,
                    "segment aggregate exceeds materialization limits");
  }

  std::error_code filesystem_error;
  if (fs::exists(options.temp_root, filesystem_error) || filesystem_error ||
      !fs::create_directory(options.temp_root, filesystem_error))
    return result(MaterializeOutcome::LOCAL_IO_ERROR,
                  "fresh temp root cannot be created: " +
                      filesystem_error.message());

  std::set<fs::path> destinations;
  insert_leaf_path(&destinations, fs::path(".remote-commit-download"));
  insert_leaf_path(&destinations, fs::path(kLocalInstallMarkerName));
  insert_leaf_path(&destinations,
                   fs::path(".wesql-remote-commit-install.tmp"));
  if (!insert_leaf_path(&destinations,
                        options.binlog_index_relative_path))
    return result(MaterializeOutcome::CORRUPT,
                  "binlog index collides with a reserved local path");
  for (const SnapshotObject &object : plan.snapshot.objects) {
    const fs::path relative(object.relative_path);
    if (!canonical_relative_path(relative) ||
        !insert_leaf_path(&destinations, relative))
      return result(MaterializeOutcome::CORRUPT,
                    "snapshot payload path is unsafe or duplicated across "
                    "components");
    const MaterializeResult downloaded = download_verified(
        payload_io_, object.key, object.size, object.sha256,
        options.max_object_bytes, options.temp_root / relative);
    if (!downloaded.ready()) return downloaded;
  }

  const fs::path scratch = options.temp_root / ".remote-commit-download";
  for (const SmartengineExtentRef &extent : plan.snapshot.smartengine_extents) {
    const ObjectRef ref = payload_ref(extent);
    const MaterializeResult downloaded =
        download_verified(payload_io_, ref.key, ref.size, ref.sha256,
                          options.max_object_bytes, scratch);
    if (!downloaded.ready()) return downloaded;
    fs::remove(scratch, filesystem_error);
    if (filesystem_error)
      return result(MaterializeOutcome::LOCAL_IO_ERROR,
                    "cannot remove verified extent scratch file: " +
                        filesystem_error.message());
  }

  const fs::path seed_relative(plan.snapshot.binlog_seed.file);
  if (!canonical_relative_path(seed_relative) ||
      seed_relative.has_parent_path() ||
      !insert_leaf_path(&destinations, seed_relative))
    return result(MaterializeOutcome::CORRUPT,
                  "binlog seed file name is unsafe or collides with snapshot");
  const fs::path seed_path = options.temp_root / seed_relative;
  const ObjectRef seed_ref = payload_ref(plan.snapshot.binlog_seed);
  MaterializeResult downloaded =
      download_verified(payload_io_, seed_ref.key, seed_ref.size,
                        seed_ref.sha256, options.max_object_bytes, seed_path);
  if (!downloaded.ready()) return downloaded;
  root->binlog_files.push_back(seed_path);

  std::string current_file = plan.snapshot.binlog_seed.file;
  fs::path current_path = seed_path;
  uint64_t current_size = plan.snapshot.binlog_seed.size;
  for (const SegmentRef &segment : plan.replay_segments) {
    if (segment.source.file == current_file) {
      if (segment.source.start_pos != current_size)
        return result(MaterializeOutcome::CORRUPT,
                      "same-file segment does not start at reconstructed end");
    } else {
      if (!adjacent_binlog_file(current_file, segment.source.file) ||
          segment.source.start_pos != 0)
        return result(MaterializeOutcome::CORRUPT,
                      "cross-file segment is not the adjacent file at zero");
      const fs::path next_relative(segment.source.file);
      if (!canonical_relative_path(next_relative) ||
          next_relative.has_parent_path() ||
          !insert_leaf_path(&destinations, next_relative))
        return result(MaterializeOutcome::CORRUPT,
                      "next binlog file name is unsafe or colliding");
      current_file = segment.source.file;
      current_path = options.temp_root / next_relative;
      current_size = 0;
      root->binlog_files.push_back(current_path);
      const int created =
          ::open(current_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
      if (created < 0 || ::close(created) != 0)
        return result(MaterializeOutcome::LOCAL_IO_ERROR,
                      "cannot create next reconstructed binlog file");
    }

    const ObjectRef ref = payload_ref(segment);
    uint64_t next_size = 0;
    if (!checked_add(current_size, segment.size,
                     std::numeric_limits<uint64_t>::max(), &next_size) ||
        next_size != segment.source.end_pos)
      return result(MaterializeOutcome::CORRUPT,
                    "reconstructed segment end differs from source ref");
    downloaded = download_verified(payload_io_, ref.key, ref.size, ref.sha256,
                                   options.max_object_bytes, scratch);
    if (!downloaded.ready()) return downloaded;
    std::string append_error;
    if (!append_file(scratch, current_path, &append_error))
      return result(MaterializeOutcome::LOCAL_IO_ERROR,
                    std::move(append_error));
    fs::remove(scratch, filesystem_error);
    if (filesystem_error)
      return result(MaterializeOutcome::LOCAL_IO_ERROR,
                    "cannot remove verified segment scratch file: " +
                        filesystem_error.message());
    current_size = next_size;
  }

  if (current_file != plan.head.durable_cursor.file ||
      current_size != plan.head.durable_cursor.pos)
    return result(MaterializeOutcome::CORRUPT,
                  "reconstructed binlog does not end at HEAD durable cursor");
  std::string validation_error;
  if (!binlog_validator_->validate(root->binlog_files,
                                   plan.head.durable_cursor,
                                   &validation_error))
    return result(MaterializeOutcome::CORRUPT,
                  "reconstructed binlog validation failed: " +
                      validation_error);

  for (size_t index = 0; index < root->binlog_files.size(); ++index) {
    if (!normalize_fde_flag(root->binlog_files[index],
                            index + 1 == root->binlog_files.size(),
                            &validation_error))
      return result(MaterializeOutcome::LOCAL_IO_ERROR,
                    std::move(validation_error));
  }

  root->binlog_index = options.temp_root / options.binlog_index_relative_path;
  if (!fs::create_directories(root->binlog_index.parent_path(),
                              filesystem_error) &&
      filesystem_error)
    return result(MaterializeOutcome::LOCAL_IO_ERROR,
                  "cannot create binlog index directory: " +
                      filesystem_error.message());
  if (!write_index(root->binlog_index, root->binlog_files,
                   &validation_error))
    return result(MaterializeOutcome::LOCAL_IO_ERROR,
                  std::move(validation_error));

  std::vector<fs::path> directories{options.temp_root};
  for (const auto &entry : fs::recursive_directory_iterator(
           options.temp_root, fs::directory_options::none, filesystem_error)) {
    if (filesystem_error) break;
    if (entry.is_directory()) directories.push_back(entry.path());
  }
  if (filesystem_error)
    return result(MaterializeOutcome::LOCAL_IO_ERROR,
                  "cannot enumerate materialized root directories: " +
                      filesystem_error.message());
  std::sort(directories.begin(), directories.end(),
            [](const fs::path &left, const fs::path &right) {
              const auto left_depth = std::distance(left.begin(), left.end());
              const auto right_depth =
                  std::distance(right.begin(), right.end());
              if (left_depth != right_depth) return left_depth > right_depth;
              return left.native() < right.native();
            });
  for (const fs::path &directory : directories) {
    if (!fsync_path(directory, true, &validation_error))
      return result(MaterializeOutcome::LOCAL_IO_ERROR,
                    std::move(validation_error));
  }
  if (!fsync_path(options.temp_root.parent_path(), true, &validation_error))
    return result(MaterializeOutcome::LOCAL_IO_ERROR,
                  std::move(validation_error));

  root->verified_payload_bytes = total;
  return result(MaterializeOutcome::READY);
}

}  // namespace wesql::remote_commit
