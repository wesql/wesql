/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/runtime_snapshot_sources.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <iterator>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>

#ifndef WESQL_RUNTIME_SNAPSHOT_SOURCES_TEST_ONLY
#include "my_rnd.h"
#include "mysqld_error.h"
#include "mysql/plugin_clone.h"
#include "sql/clone_handler.h"
#include "sql/handler.h"
#include "sql/remote_commit/protocol_codec.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/remote_commit/server_root_evidence.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin.h"
#include "sql/sql_plugin_ref.h"
#include "sql_string.h"
#include "storage/innobase/include/clone0api.h"
#endif

namespace fs = std::filesystem;

namespace wesql::remote_commit {
namespace {

constexpr std::string_view kInnodbFormat = "innodb-clone-v1";
constexpr std::string_view kMysqlDdFormat = "mysql-dd-v1";
constexpr std::string_view kSmartengineMetaFormat = "smartengine-meta-v1";
constexpr std::string_view kSmartengineWalFormat = "smartengine-wal-v1";

struct TreeEntry {
  std::string relative_path;
  fs::file_type type{fs::file_type::none};
  uintmax_t size{0};
  fs::file_time_type modified{};

  bool operator==(const TreeEntry &) const = default;
};

bool set_error(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return false;
}

bool first_component_is(const fs::path &relative, std::string_view value) {
  const auto first = relative.begin();
  return first != relative.end() && first->generic_string() == value;
}

bool classify_clone_file(const fs::path &relative,
                         LocalSnapshotPayload *payload,
                         std::string *error) {
  const std::string encoded = relative.generic_string();
  const std::string filename = relative.filename().string();
  const std::string extension = relative.extension().string();
  if (encoded == "auto.cnf" || encoded == "mysql.ibd" ||
      encoded == "mysql_upgrade_history" ||
      first_component_is(relative, "mysql") ||
      first_component_is(relative, "sys") ||
      first_component_is(relative, "performance_schema")) {
    payload->component = "mysql-dd";
    payload->format = std::string(kMysqlDdFormat);
  } else if (first_component_is(relative, "#innodb_redo") ||
             filename.starts_with("#ib_") ||
             filename.starts_with("ibdata") ||
             filename.starts_with("undo_") || extension == ".ibd" ||
             extension == ".ibu" || extension == ".cfg" ||
             extension == ".cfp") {
    payload->component = "innodb";
    payload->format = std::string(kInnodbFormat);
  } else {
    return set_error(error,
                     "runtime Clone tree contains an unclassified file: " +
                         encoded);
  }
  payload->relative_path = encoded;
  return true;
}

bool classify_smartengine_file(const fs::path &relative,
                               LocalSnapshotPayload *payload,
                               std::string *error) {
  const std::string encoded = relative.generic_string();
  const std::string filename = relative.filename().string();
  if (filename == "LOCK" || filename == "Log") {
    return set_error(error,
                     "runtime SmartEngine tree contains a live lock file: " +
                         encoded);
  }
  payload->relative_path = (fs::path("smartengine") / relative).generic_string();
  if (relative.extension() == ".wal") {
    payload->component = "smartengine-wal";
    payload->format = std::string(kSmartengineWalFormat);
  } else {
    payload->component = "smartengine-meta";
    payload->format = std::string(kSmartengineMetaFormat);
  }
  return true;
}

bool snapshot_tree(const fs::path &root, std::vector<TreeEntry> *entries,
                   std::vector<fs::path> *directories,
                   std::string *error) {
  std::error_code filesystem_error;
  const fs::file_status root_status = fs::symlink_status(root, filesystem_error);
  if (filesystem_error || !fs::is_directory(root_status)) {
    return set_error(error, "runtime snapshot tree root is not a directory: " +
                                root.string());
  }

  std::vector<TreeEntry> observed;
  std::vector<fs::path> observed_directories{fs::path()};
  fs::recursive_directory_iterator iterator(
      root, fs::directory_options::none, filesystem_error);
  const fs::recursive_directory_iterator end;
  if (filesystem_error) {
    return set_error(error, "cannot traverse runtime snapshot tree: " +
                                root.string());
  }
  for (; iterator != end; iterator.increment(filesystem_error)) {
    if (filesystem_error) {
      return set_error(error, "cannot traverse runtime snapshot tree: " +
                                  root.string());
    }
    const fs::path relative = iterator->path().lexically_relative(root);
    if (relative.empty() || relative.is_absolute() ||
        relative.generic_string().starts_with("../")) {
      return set_error(error,
                       "runtime snapshot tree produced an unsafe path");
    }
    const fs::file_status status =
        fs::symlink_status(iterator->path(), filesystem_error);
    if (filesystem_error) {
      return set_error(error, "cannot inspect runtime snapshot path: " +
                                  iterator->path().string());
    }
    if (fs::is_symlink(status)) {
      return set_error(error, "runtime snapshot tree contains a symlink: " +
                                  relative.generic_string());
    }
    if (fs::is_directory(status)) {
      observed_directories.push_back(relative);
      continue;
    }
    if (!fs::is_regular_file(status)) {
      return set_error(error,
                       "runtime snapshot tree contains a non-regular file: " +
                           relative.generic_string());
    }
    const uintmax_t size = fs::file_size(iterator->path(), filesystem_error);
    if (filesystem_error) {
      return set_error(error, "cannot size runtime snapshot file: " +
                                  relative.generic_string());
    }
    const fs::file_time_type modified =
        fs::last_write_time(iterator->path(), filesystem_error);
    if (filesystem_error) {
      return set_error(error, "cannot timestamp runtime snapshot file: " +
                                  relative.generic_string());
    }
    observed.push_back(
        {relative.generic_string(), status.type(), size, modified});
  }
  std::sort(observed.begin(), observed.end(),
            [](const TreeEntry &left, const TreeEntry &right) {
              return left.relative_path < right.relative_path;
            });
  std::sort(observed_directories.begin(), observed_directories.end());
  *entries = std::move(observed);
  *directories = std::move(observed_directories);
  return true;
}

bool fsync_regular_file(const fs::path &path, std::string *error) {
  int flags = O_RDONLY | O_CLOEXEC;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  const int descriptor = ::open(path.c_str(), flags);
  if (descriptor < 0) {
    return set_error(error, "cannot open runtime snapshot file for fsync: " +
                                path.string());
  }
  struct stat status {};
  const bool regular =
      ::fstat(descriptor, &status) == 0 && S_ISREG(status.st_mode);
  const bool synced = regular && ::fsync(descriptor) == 0;
  const bool closed = ::close(descriptor) == 0;
  if (!synced || !closed) {
    return set_error(error, "cannot fsync runtime snapshot file: " +
                                path.string());
  }
  return true;
}

bool fsync_directory(const fs::path &path, std::string *error) {
  int flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  const int descriptor = ::open(path.c_str(), flags);
  if (descriptor < 0) {
    return set_error(error, "cannot open runtime snapshot directory: " +
                                path.string());
  }
  const bool synced = ::fsync(descriptor) == 0;
  const bool closed = ::close(descriptor) == 0;
  if (!synced || !closed) {
    return set_error(error, "cannot fsync runtime snapshot directory: " +
                                path.string());
  }
  return true;
}

size_t path_depth(const fs::path &path) {
  return static_cast<size_t>(std::distance(path.begin(), path.end()));
}

}  // namespace

bool finalize_runtime_snapshot_tree(
    const fs::path &root, RuntimeSnapshotTreeKind kind,
    std::vector<LocalSnapshotPayload> *objects, std::string *error) {
  if (error != nullptr) error->clear();
  if (objects == nullptr)
    return set_error(error, "runtime snapshot payload output is null");

  std::vector<TreeEntry> before;
  std::vector<fs::path> directories;
  if (!snapshot_tree(root, &before, &directories, error)) return false;

  std::vector<LocalSnapshotPayload> classified;
  std::set<std::string> components;
  classified.reserve(before.size());
  for (const TreeEntry &entry : before) {
    LocalSnapshotPayload payload;
    const fs::path relative(entry.relative_path);
    const bool valid =
        kind == RuntimeSnapshotTreeKind::CLONE
            ? classify_clone_file(relative, &payload, error)
            : classify_smartengine_file(relative, &payload, error);
    if (!valid) return false;
    payload.local_path = root / relative;
    if (!fsync_regular_file(payload.local_path, error)) return false;
    components.insert(payload.component);
    classified.push_back(std::move(payload));
  }

  const std::set<std::string> required =
      kind == RuntimeSnapshotTreeKind::CLONE
          ? std::set<std::string>{"innodb", "mysql-dd"}
          : std::set<std::string>{"smartengine-meta", "smartengine-wal"};
  if (components != required) {
    return set_error(error,
                     kind == RuntimeSnapshotTreeKind::CLONE
                         ? "runtime Clone tree lacks required InnoDB/DD payloads"
                         : "runtime SmartEngine tree lacks required meta/WAL payloads");
  }

  const std::vector<fs::path> stable_directories = directories;
  std::sort(directories.begin(), directories.end(),
            [](const fs::path &left, const fs::path &right) {
              const size_t left_depth = path_depth(left);
              const size_t right_depth = path_depth(right);
              return left_depth != right_depth ? left_depth > right_depth
                                               : left > right;
            });
  for (const fs::path &relative : directories) {
    if (!fsync_directory(root / relative, error)) return false;
  }

  std::vector<TreeEntry> after;
  std::vector<fs::path> after_directories;
  if (!snapshot_tree(root, &after, &after_directories, error)) return false;
  if (after != before || after_directories != stable_directories) {
    return set_error(error,
                     "runtime snapshot tree changed while it was finalized");
  }
  *objects = std::move(classified);
  return true;
}

#ifndef WESQL_RUNTIME_SNAPSHOT_SOURCES_TEST_ONLY
namespace {

std::string random_hex(size_t byte_count) {
  std::vector<unsigned char> bytes(byte_count);
  if (bytes.empty() || my_rand_buffer(bytes.data(), bytes.size()) != 0)
    return {};
  static constexpr char kHex[] = "0123456789abcdef";
  std::string value;
  value.reserve(byte_count * 2);
  for (const unsigned char byte : bytes) {
    value.push_back(kHex[byte >> 4]);
    value.push_back(kHex[byte & 0x0f]);
  }
  return value;
}

template <typename Digest>
std::string digest_hex(const Digest &digest) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(digest.size() * 2);
  for (const unsigned char byte : digest) {
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0f]);
  }
  return result;
}

bool create_private_directory(const fs::path &path, std::string *error) {
  if (::mkdir(path.c_str(), 0700) == 0) return true;
  return set_error(error, "cannot create private runtime snapshot directory: " +
                              path.string());
}

void remove_tree_or_fail_stop(const fs::path &root) {
  if (root.empty()) return;
  std::error_code filesystem_error;
  fs::remove_all(root, filesystem_error);
  const bool remains = fs::exists(root, filesystem_error);
  if (filesystem_error || remains)
    fail_stop("cannot remove private runtime snapshot staging tree");
}

class ProductionRuntimeSnapshotSourceLease final
    : public RuntimeSnapshotSourceLease {
 public:
  ProductionRuntimeSnapshotSourceLease(THD *thd, fs::path request_root)
      : thd_(thd), request_root_(std::move(request_root)) {}

  ~ProductionRuntimeSnapshotSourceLease() override {
#ifdef WITH_SMARTENGINE
    if (smartengine_snapshot_id_ != 0) {
      if (smartengine_hton_ == nullptr ||
          smartengine_hton_->release_backup_snapshot == nullptr ||
          smartengine_hton_->release_backup_snapshot(
              thd_, smartengine_snapshot_id_) != 0) {
        fail_stop("cannot release runtime SmartEngine snapshot lease");
      }
      smartengine_snapshot_id_ = 0;
    }
#endif
    if (smartengine_plugin_ != nullptr) {
      plugin_unlock(thd_, smartengine_plugin_);
      smartengine_plugin_ = nullptr;
    }
    if (clone_plugin_ != nullptr) {
      clone_plugin_unlock(thd_, clone_plugin_);
      clone_plugin_ = nullptr;
    }
    remove_tree_or_fail_stop(request_root_);
  }

  void retain_clone_plugin(plugin_ref plugin) { clone_plugin_ = plugin; }

  void retain_smartengine(plugin_ref plugin, handlerton *hton,
                          uint64_t snapshot_id) {
    smartengine_plugin_ = plugin;
    smartengine_hton_ = hton;
    smartengine_snapshot_id_ = snapshot_id;
  }

 private:
  THD *thd_{nullptr};
  fs::path request_root_;
  plugin_ref clone_plugin_{nullptr};
  plugin_ref smartengine_plugin_{nullptr};
  handlerton *smartengine_hton_{nullptr};
  uint64_t smartengine_snapshot_id_{0};
};

struct ObserverContext {
  RuntimeSnapshotRequest request;
  THD *thd{nullptr};
  fs::path request_root;
  fs::path clone_root;
  fs::path smartengine_root;
  fs::path seed_directory;
  std::atomic<bool> *shutdown_requested{nullptr};
  std::unique_ptr<ProductionRuntimeSnapshotSourceLease> resources;
  RuntimeSnapshotAcquisitionResult result;
  bool bind_succeeded{false};
};

int observer_failure(ObserverContext *context,
                     RuntimeSnapshotAcquisitionOutcome outcome,
                     std::string detail) {
  context->result.outcome = outcome;
  context->result.detail = std::move(detail);
  return ER_INTERNAL_ERROR;
}

int bind_runtime_snapshot_request(THD *, uint64_t request_id,
                                  const unsigned char *innodb_locator,
                                  unsigned int innodb_locator_length,
                                  void *opaque) {
  auto *context = static_cast<ObserverContext *>(opaque);
  if (context == nullptr || request_id == 0 ||
      request_id != context->request.request_id) {
    return ER_INTERNAL_ERROR;
  }
  if (context->shutdown_requested->load(std::memory_order_acquire)) {
    return observer_failure(context, RuntimeSnapshotAcquisitionOutcome::SHUTDOWN,
                            "server shutdown interrupted Clone request binding");
  }
  if (!innodb_clone_bind_remote_snapshot_request(
          innodb_locator, innodb_locator_length, request_id)) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::BLOCKED,
        "InnoDB could not bind the runtime snapshot request to Clone");
  }
  context->bind_succeeded = true;
  return 0;
}

int complete_runtime_snapshot_request(THD *, uint64_t request_id,
                                      const unsigned char *innodb_locator,
                                      unsigned int innodb_locator_length,
                                      void *opaque) {
  auto *context = static_cast<ObserverContext *>(opaque);
  if (context == nullptr || !context->bind_succeeded || request_id == 0 ||
      request_id != context->request.request_id) {
    return ER_INTERNAL_ERROR;
  }
  if (context->shutdown_requested->load(std::memory_order_acquire)) {
    return observer_failure(context, RuntimeSnapshotAcquisitionOutcome::SHUTDOWN,
                            "server shutdown interrupted Clone completion");
  }

  Remote_clone_cut remote_cut;
  CloneCutBarrierLease clone_barrier;
  if (!innodb_clone_take_remote_cut(innodb_locator, innodb_locator_length,
                                    request_id, remote_cut, clone_barrier)) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::REFIX_REQUIRED,
        "InnoDB Clone did not transfer the exact completed remote cut");
  }

  const std::string gtid_sha = digest_hex(remote_cut.gtid_digest);
  const std::string redo_sha = digest_hex(remote_cut.redo_locator_digest);
  const std::string recomputed_redo_sha =
      digest_hex(innodb_clone_redo_locator_digest(remote_cut.redo_locator));
  CloneCutState cut_state{request_id,
                          remote_cut.binlog_file,
                          remote_cut.binlog_position,
                          remote_cut.gtid_executed,
                          gtid_sha,
                          remote_cut.head_generation,
                          remote_cut.head_body_sha256};

  RuntimeSnapshotAuthority authority;
  std::string error;
  if (runtime_snapshot_authority(request_id, &authority, &error)) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::REFIX_REQUIRED,
        error.empty() ? "runtime snapshot authority changed during Clone"
                      : std::move(error));
  }
  const PublisherState &publisher_state = authority.publisher_state;
  if (!publisher_state.head.has_value() ||
      !publisher_state.head_object.has_value() ||
      !publisher_state.epoch.has_value() ||
      !publisher_state.epoch_object.has_value()) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::FENCED,
        "runtime snapshot authority omitted its exact HEAD or writer epoch");
  }
  const Head &head = *publisher_state.head;
  std::string head_sha;
  GtidSetDigest gtid;
  if (!sha256_hex(publisher_state.head_object->body, &head_sha, &error) ||
      !gtid_digest(remote_cut.gtid_executed, &gtid, &error) ||
      gtid.canonical != remote_cut.gtid_executed || gtid.sha256 != gtid_sha ||
      recomputed_redo_sha != redo_sha ||
      remote_cut.request_id != request_id ||
      remote_cut.clone_handle_id == 0 ||
      remote_cut.binlog_file !=
          fs::path(remote_cut.binlog_file).filename().string() ||
      remote_cut.binlog_position == 0 ||
      remote_cut.head_generation != head.generation ||
      remote_cut.head_body_sha256 != head_sha ||
      authority.request_id != request_id || authority.writer != head.writer ||
      publisher_state.epoch->writer_id != authority.writer.id ||
      publisher_state.epoch->epoch != authority.writer.epoch ||
      Cursor{remote_cut.binlog_file, remote_cut.binlog_position} !=
          head.durable_cursor) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::FENCED,
        error.empty() ? "Clone cut differs from runtime publisher authority"
                      : std::move(error));
  }

#ifndef WITH_SMARTENGINE
  return observer_failure(
      context, RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR,
      "runtime snapshot acquisition requires SmartEngine support");
#else
  StartupDeploymentIdentity deployment;
  if (!configured_server_root_runtime_deployment(&deployment, &error) ||
      deployment.stream_id != authority.stream.stream_id) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::FENCED,
        error.empty() ? "runtime deployment identity differs from the source HEAD"
                      : std::move(error));
  }

  plugin_ref smartengine_plugin = ha_resolve_by_name_raw(
      context->thd, to_lex_cstring("smartengine"));
  if (smartengine_plugin == nullptr) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::BLOCKED,
        "SmartEngine plugin is unavailable for runtime snapshot acquisition");
  }
  handlerton *smartengine =
      plugin_data<handlerton *>(smartengine_plugin);
  if (smartengine == nullptr || smartengine->state != SHOW_OPTION_YES ||
      smartengine->create_remote_backup_snapshot == nullptr ||
      smartengine->export_backup_snapshot_live_set == nullptr ||
      smartengine->release_backup_snapshot == nullptr) {
    plugin_unlock(context->thd, smartengine_plugin);
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::BLOCKED,
        "SmartEngine runtime snapshot API is unavailable");
  }

  uint64_t smartengine_snapshot_id = 0;
  uint64_t smartengine_position = 0;
  std::string smartengine_file;
  const int create_error = smartengine->create_remote_backup_snapshot(
      context->thd, context->smartengine_root.c_str(),
      &smartengine_snapshot_id, smartengine_file, &smartengine_position);
  if (create_error != 0 || smartengine_snapshot_id == 0) {
    plugin_unlock(context->thd, smartengine_plugin);
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::BLOCKED,
        "SmartEngine could not create a remote backup snapshot");
  }
  context->resources->retain_smartengine(
      smartengine_plugin, smartengine, smartengine_snapshot_id);

  const Cursor smartengine_cursor{fs::path(smartengine_file).filename().string(),
                                  smartengine_position};
  const Cursor clone_cursor{remote_cut.binlog_file,
                            remote_cut.binlog_position};
  if (smartengine_cursor != clone_cursor) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::FENCED,
        "SmartEngine and InnoDB Clone fixed different binlog cursors");
  }

  std::vector<Smartengine_remote_extent_ref> exported;
  if (smartengine->export_backup_snapshot_live_set(
          context->thd, smartengine_snapshot_id, &exported) != 0) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR,
        "SmartEngine live extent export failed");
  }
  std::vector<SmartengineExtentRef> extents;
  extents.reserve(exported.size());
  for (const Smartengine_remote_extent_ref &source : exported) {
    extents.push_back({source.ordinal,
                       source.writer_epoch,
                       std::to_string(source.allocation_seq),
                       source.database_name_hex,
                       std::to_string(source.index_id),
                       std::to_string(source.object_id),
                       source.key,
                       source.size,
                       source.sha256,
                       "smartengine-object-extent-v2"});
  }
  std::vector<SmartengineExtentRef> canonical_extents;
  if (!canonicalize_server_root_extents(extents, &canonical_extents, &error)) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR,
        error.empty() ? "SmartEngine live extent set is invalid"
                      : std::move(error));
  }
  for (const SmartengineExtentRef &extent : canonical_extents) {
    std::string expected_key;
    if (extent.writer_epoch != authority.writer.epoch ||
        !smartengine_extent_object_key(authority.stream, extent, &expected_key,
                                       &error) ||
        expected_key != extent.key) {
      return observer_failure(
          context, RuntimeSnapshotAcquisitionOutcome::FENCED,
          error.empty()
              ? "SmartEngine live extent belongs to another writer lineage"
              : std::move(error));
    }
  }

  if (!create_private_directory(context->seed_directory, &error)) {
    return observer_failure(context,
                            RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR,
                            std::move(error));
  }
  const fs::path seed = context->seed_directory / remote_cut.binlog_file;
  if (materialize_clone_cut_binlog_seed(cut_state, clone_barrier, seed,
                                        &error)) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR,
        error.empty() ? "cannot materialize the pinned runtime binlog seed"
                      : std::move(error));
  }
  if (!fsync_directory(context->seed_directory, &error)) {
    return observer_failure(context,
                            RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR,
                            std::move(error));
  }

  // Clone, SmartEngine, and the binlog seed are now fixed. Releasing this
  // barrier reopens SOFT admission; a live HARD request remains closed by the
  // independent hard gate until ordered publication completes.
  end_clone_cut_barrier(&clone_barrier);

  std::vector<LocalSnapshotPayload> clone_objects;
  std::vector<LocalSnapshotPayload> smartengine_objects;
  if (!finalize_runtime_snapshot_tree(context->clone_root,
                                      RuntimeSnapshotTreeKind::CLONE,
                                      &clone_objects, &error) ||
      !finalize_runtime_snapshot_tree(context->smartengine_root,
                                      RuntimeSnapshotTreeKind::SMARTENGINE,
                                      &smartengine_objects, &error)) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR,
        error.empty() ? "cannot finalize runtime snapshot engine payloads"
                      : std::move(error));
  }

  const std::string snapshot_id = random_hex(16);
  if (snapshot_id.size() != 32 ||
      snapshot_id.find_first_not_of("0123456789abcdef") != std::string::npos) {
    return observer_failure(
        context, RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR,
        "cannot generate a runtime snapshot ID");
  }

  FixedSnapshotCut cut;
  cut.snapshot_id = snapshot_id;
  cut.writer = authority.writer;
  cut.proof.source = FixedCutSource::CLONE_BARRIER;
  cut.proof.public_cursor = {remote_cut.binlog_file,
                             remote_cut.binlog_position};
  cut.proof.public_gtid = gtid;
  cut.proof.image_cursor = cut.proof.public_cursor;
  cut.proof.image_gtid = gtid;
  cut.proof.source_head_generation = head.generation;
  cut.proof.source_head_body_sha256 = head_sha;
  cut.proof.clone_handle_id = remote_cut.clone_handle_id;
  cut.proof.redo_range_sha256 = redo_sha;
  cut.log_anchor.kind = LogAnchorKind::MANIFEST_BOUNDARY;
  cut.log_anchor.generation = head.generation;
  cut.log_anchor.manifest = head.manifest;
  cut.log_anchor.cursor = cut.proof.public_cursor;
  cut.server_identity.server_uuid = deployment.server_uuid;
  cut.deployment_fingerprints = deployment.fingerprints;
  cut.binlog_seed_path = seed;
  cut.objects = std::move(clone_objects);
  cut.objects.insert(cut.objects.end(),
                     std::make_move_iterator(smartengine_objects.begin()),
                     std::make_move_iterator(smartengine_objects.end()));
  cut.smartengine_extents.reserve(canonical_extents.size());
  for (const SmartengineExtentRef &extent : canonical_extents) {
    cut.smartengine_extents.push_back(
        {extent.writer_epoch, extent.allocation_seq, extent.database_name_hex,
         extent.index_id, extent.object_id, extent.key, extent.size,
         extent.sha256});
  }

  RuntimeSnapshotAcquisition acquisition;
  acquisition.request_id = request_id;
  acquisition.cut = std::move(cut);
  acquisition.authority = std::move(authority);
  acquisition.source_lease = std::move(context->resources);
  context->result.outcome = RuntimeSnapshotAcquisitionOutcome::FIXED;
  context->result.detail.clear();
  context->result.acquisition.emplace(std::move(acquisition));
#endif
  return 0;
}

}  // namespace

struct ProductionRuntimeSnapshotAcquirer::Impl {
  Impl(THD *thd_arg, fs::path service_root_arg,
       std::atomic<bool> *shutdown_requested_arg)
      : thd(thd_arg),
        service_root(std::move(service_root_arg)),
        shutdown_requested(shutdown_requested_arg) {}

  THD *thd;
  fs::path service_root;
  std::atomic<bool> *shutdown_requested;
};

ProductionRuntimeSnapshotAcquirer::ProductionRuntimeSnapshotAcquirer(
    THD *thd, fs::path service_root, std::atomic<bool> *shutdown_requested)
    : impl_(std::make_unique<Impl>(thd, std::move(service_root),
                                  shutdown_requested)) {}

ProductionRuntimeSnapshotAcquirer::~ProductionRuntimeSnapshotAcquirer() =
    default;

RuntimeSnapshotAcquisitionResult ProductionRuntimeSnapshotAcquirer::acquire(
    const RuntimeSnapshotRequest &request) {
  RuntimeSnapshotAcquisitionResult result;
  if (impl_ == nullptr || impl_->thd == nullptr ||
      impl_->shutdown_requested == nullptr || impl_->service_root.empty() ||
      request.request_id == 0) {
    result.outcome = RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR;
    result.detail = "runtime snapshot acquirer is not initialized";
    return result;
  }
  if (impl_->shutdown_requested->load(std::memory_order_acquire)) {
    result.outcome = RuntimeSnapshotAcquisitionOutcome::SHUTDOWN;
    result.detail = "server shutdown cancelled runtime snapshot acquisition";
    return result;
  }

  const std::string suffix = random_hex(16);
  if (suffix.size() != 32) {
    result.outcome = RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR;
    result.detail = "cannot generate a private runtime snapshot directory";
    return result;
  }
  ObserverContext context;
  context.request = request;
  context.thd = impl_->thd;
  context.shutdown_requested = impl_->shutdown_requested;
  context.request_root =
      impl_->service_root /
      ("request-" + std::to_string(request.request_id) + "-" + suffix);
  context.clone_root = context.request_root / "clone";
  context.smartengine_root = context.request_root / "smartengine";
  context.seed_directory = context.request_root / "binlog";

  std::string error;
  if (!create_private_directory(context.request_root, &error)) {
    result.outcome = RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR;
    result.detail = std::move(error);
    return result;
  }
  context.resources =
      std::make_unique<ProductionRuntimeSnapshotSourceLease>(
          impl_->thd, context.request_root);
  if (!fsync_directory(impl_->service_root, &error)) {
    result.outcome = RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR;
    result.detail = std::move(error);
    return result;
  }

  plugin_ref clone_plugin = nullptr;
  Clone_handler *clone = clone_plugin_lock(impl_->thd, &clone_plugin);
  if (clone == nullptr) {
    if (clone_plugin != nullptr) clone_plugin_unlock(impl_->thd, clone_plugin);
    context.result.outcome = RuntimeSnapshotAcquisitionOutcome::BLOCKED;
    context.result.detail =
        "Clone plugin is unavailable for runtime snapshot acquisition";
    return std::move(context.result);
  }
  context.resources->retain_clone_plugin(clone_plugin);

  const Mysql_clone_local_observer observer{
      request.request_id, bind_runtime_snapshot_request,
      complete_runtime_snapshot_request, &context};
  const int clone_error =
      clone->clone_local(impl_->thd, context.clone_root.c_str(), &observer);
  if (clone_error != 0) {
    context.result.acquisition.reset();
    if (impl_->shutdown_requested->load(std::memory_order_acquire)) {
      context.result.outcome = RuntimeSnapshotAcquisitionOutcome::SHUTDOWN;
      context.result.detail =
          "server shutdown interrupted local runtime Clone";
    } else if (context.result.detail.empty()) {
      context.result.outcome = RuntimeSnapshotAcquisitionOutcome::BLOCKED;
      context.result.detail = "local runtime Clone failed with error " +
                              std::to_string(clone_error);
    }
    return std::move(context.result);
  }
  if (!context.result.fixed()) {
    context.result.outcome = RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR;
    if (context.result.detail.empty())
      context.result.detail =
          "local runtime Clone completed without a fixed snapshot cut";
  }
  return std::move(context.result);
}

#endif  // WESQL_RUNTIME_SNAPSHOT_SOURCES_TEST_ONLY

}  // namespace wesql::remote_commit
