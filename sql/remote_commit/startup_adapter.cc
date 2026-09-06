/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/startup_adapter.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <openssl/evp.h>
#include <sys/stat.h>
#include <unistd.h>

#include "my_rnd.h"
#include "sql/mysqld.h"
#include "sql/remote_commit/local_install.h"
#include "sql/remote_commit/materializer.h"
#include "sql/remote_commit/native_recovery.h"
#include "sql/remote_commit/protocol_codec.h"
#include "sql/remote_commit/runtime_snapshot_service.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/remote_commit/server_root_evidence.h"
#include "sql/remote_commit/snapshot_publisher.h"
#include "sql/remote_commit/snapshot_file_classification.h"
#include "sql/remote_commit/startup_coordinator.h"
#include "sql/remote_commit/startup_server.h"

extern char *opt_binlog_index_name;

namespace wesql::remote_commit {

char *opt_remote_startup_mode = nullptr;
char *opt_remote_startup_input_path = nullptr;
ulonglong opt_remote_startup_input_size = 0;
char *opt_remote_startup_input_sha256 = nullptr;
char *opt_remote_startup_output_path = nullptr;
ulong opt_remote_startup_daemon_pipe_fd = 0;

namespace {

namespace fs = std::filesystem;

constexpr std::string_view kInnodbFormat = "innodb-clone-v1";
constexpr std::string_view kMysqlDdFormat = "mysql-dd-v1";
constexpr std::string_view kSmartengineMetaFormat = "smartengine-meta-v1";
constexpr std::string_view kSmartengineWalFormat = "smartengine-wal-v1";

enum class PendingOutput : uint8_t {
  NONE,
  BOOTSTRAP_PREFLIGHT,
  WORKER_COMPLETION,
};

struct ExternalDeployment {
  std::string configured_uuid;
  DeploymentFingerprints fingerprints;
  std::string binary_fingerprint;
};

bool fail(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return true;
}

bool lowercase_sha256(std::string_view value) {
  return value.size() == 64 &&
         std::all_of(value.begin(), value.end(), [](const char byte) {
           return (byte >= '0' && byte <= '9') ||
                  (byte >= 'a' && byte <= 'f');
         });
}

bool lowercase_uuid(std::string_view value) {
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

bool valid_text(std::string_view value, size_t maximum) {
  return !value.empty() && value.size() <= maximum &&
         std::all_of(value.begin(), value.end(), [](const unsigned char byte) {
           return byte >= 0x20 && byte != 0x7f;
         });
}

std::string configured_string(const char *value) {
  return value == nullptr ? std::string() : std::string(value);
}

bool read_external_deployment(ExternalDeployment *deployment,
                              std::string *error) {
  if (deployment == nullptr) return !fail(error, "null deployment output");
  *deployment = {};
  deployment->configured_uuid = configured_string(
      opt_binlog_archive_remote_commit_server_uuid);
  deployment->fingerprints.startup_config_sha256 = configured_string(
      opt_binlog_archive_remote_commit_startup_config_sha256);
  deployment->fingerprints.server_build =
      configured_string(opt_binlog_archive_remote_commit_server_build);
  deployment->fingerprints.plugin_component_set_sha256 = configured_string(
      opt_binlog_archive_remote_commit_plugin_component_set_sha256);
  deployment->fingerprints.keyring_config_sha256 = configured_string(
      opt_binlog_archive_remote_commit_keyring_config_sha256);
  deployment->fingerprints.tls_config_sha256 = configured_string(
      opt_binlog_archive_remote_commit_tls_config_sha256);
  deployment->binary_fingerprint = configured_string(
      opt_binlog_archive_remote_commit_binary_fingerprint);
  if ((!deployment->configured_uuid.empty() &&
       !lowercase_uuid(deployment->configured_uuid)) ||
      !lowercase_sha256(
          deployment->fingerprints.startup_config_sha256) ||
      !valid_text(deployment->fingerprints.server_build,
                  kMaxOrdinaryIdBytes) ||
      !lowercase_sha256(
          deployment->fingerprints.plugin_component_set_sha256) ||
      !lowercase_sha256(
          deployment->fingerprints.keyring_config_sha256) ||
      !lowercase_sha256(deployment->fingerprints.tls_config_sha256) ||
      !lowercase_sha256(deployment->binary_fingerprint)) {
    return !fail(error,
                 "remote startup deployment declaration is incomplete or "
                 "malformed");
  }
  return true;
}

bool same_external_deployment(const ExternalDeployment &external,
                              const StartupDeploymentIdentity &deployment,
                              bool require_configured_uuid,
                              std::string *error) {
  if ((require_configured_uuid && external.configured_uuid.empty()) ||
      (!external.configured_uuid.empty() &&
       external.configured_uuid != deployment.server_uuid) ||
      external.fingerprints != deployment.fingerprints ||
      external.binary_fingerprint != deployment.binary_fingerprint) {
    return !fail(error,
                 "startup proof differs from the external deployment "
                 "declaration");
  }
  return true;
}

StartupDeploymentIdentity make_deployment(
    const StreamIdentity &stream, const ExternalDeployment &external,
    std::string server_uuid) {
  StartupDeploymentIdentity deployment;
  deployment.stream_id = stream.stream_id;
  deployment.server_uuid = std::move(server_uuid);
  deployment.fingerprints = external.fingerprints;
  deployment.binary_fingerprint = external.binary_fingerprint;
  return deployment;
}

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

bool same_published(const PublishedBytes &left, const PublishedBytes &right) {
  return left.body == right.body && left.etag == right.etag;
}

StartupStepResult hook_failure(std::string operation) {
  const std::string detail = startup_error();
  if (!detail.empty()) operation.append(": ").append(detail);
  return {is_fenced() ? StartupStepOutcome::FENCED
                      : StartupStepOutcome::CORRUPT,
          std::move(operation)};
}

StartupStepResult map_publish_result(const PublishResult &published,
                                     std::string_view operation) {
  StartupStepOutcome outcome = StartupStepOutcome::CORRUPT;
  switch (published.outcome) {
    case PublishOutcome::APPLIED:
      return {StartupStepOutcome::READY, {}};
    case PublishOutcome::BLOCKED:
      outcome = StartupStepOutcome::BLOCKED;
      break;
    case PublishOutcome::REFIX_REQUIRED:
      outcome = StartupStepOutcome::CORRUPT;
      break;
    case PublishOutcome::FENCED:
      outcome = StartupStepOutcome::FENCED;
      break;
    case PublishOutcome::ABSENT:
    case PublishOutcome::PERMANENT_ERROR:
      outcome = StartupStepOutcome::CORRUPT;
      break;
  }
  std::string detail(operation);
  if (!published.detail.empty()) detail.append(": ").append(published.detail);
  return {outcome, std::move(detail)};
}

bool canonical_target_root(fs::path *root, std::string *error) {
  if (root == nullptr) return !fail(error, "null startup root output");
  const char *configured = mysql_real_data_home_ptr != nullptr
                               ? mysql_real_data_home_ptr
                               : mysql_real_data_home;
  if (configured == nullptr || configured[0] == '\0')
    return !fail(error, "remote startup data directory is empty");
  return canonical_startup_data_directory(fs::path(configured), root, error);
}

bool same_root(const fs::path &left, const fs::path &right) {
  std::error_code left_error;
  std::error_code right_error;
  const fs::path canonical_left = fs::weakly_canonical(left, left_error);
  const fs::path canonical_right = fs::weakly_canonical(right, right_error);
  return !left_error && !right_error && canonical_left == canonical_right;
}

bool configured_binlog_paths(fs::path *index, std::string *basename,
                             std::string *error) {
  if (index == nullptr || basename == nullptr)
    return !fail(error, "null configured binlog path output");
  fs::path configured_log;
  if (opt_bin_logname != nullptr && opt_bin_logname[0] != '\0') {
    configured_log = fs::path(opt_bin_logname);
    if (configured_log.is_absolute() || configured_log.has_parent_path())
      return !fail(error,
                   "remote startup requires the binary log inside datadir");
    *basename = configured_log.filename().string();
  } else if (log_bin_supplied) {
    *basename = std::string(glob_hostname) + "-bin";
  } else {
    *basename = "binlog";
  }
  const fs::path basename_path(*basename);
  if (basename->empty() || basename_path.has_root_path() ||
      basename_path.has_parent_path() || basename_path.filename() == "." ||
      basename_path.filename() == "..") {
    return !fail(error,
                 "remote startup binary-log basename is not a safe "
                 "data-directory filename");
  }

  if (opt_binlog_index_name != nullptr && opt_binlog_index_name[0] != '\0') {
    *index = fs::path(opt_binlog_index_name);
    if (index->has_root_path() || index->has_parent_path() ||
        index->filename() == "." || index->filename() == "..")
      return !fail(error,
                   "remote startup requires the binary-log index inside "
                   "datadir");
  } else {
    *index = fs::path(*basename + ".index");
  }
  return true;
}

bool observe_output_reference(const fs::path &path, uint64_t maximum_bytes,
                              StartupProofReference *reference,
                              std::string *error) {
  if (reference == nullptr)
    return !fail(error, "null startup output reference");
  *reference = {};
  const int descriptor =
      ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0)
    return !fail(error, "cannot open startup child output: " +
                            std::string(std::strerror(errno)));
  struct stat status {};
  if (::fstat(descriptor, &status) != 0 || !S_ISREG(status.st_mode) ||
      status.st_size <= 0 ||
      static_cast<uint64_t>(status.st_size) > maximum_bytes) {
    ::close(descriptor);
    return !fail(error, "startup child output has an invalid shape or size");
  }
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (context == nullptr) {
    ::close(descriptor);
    return !fail(error, "cannot allocate startup-output SHA-256 context");
  }
  bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1;
  std::array<unsigned char, 1024 * 1024> buffer{};
  uint64_t consumed = 0;
  while (ok) {
    const ssize_t count = ::read(descriptor, buffer.data(), buffer.size());
    if (count < 0 && errno == EINTR) continue;
    if (count < 0) {
      ok = false;
      break;
    }
    if (count == 0) break;
    consumed += static_cast<uint64_t>(count);
    ok = EVP_DigestUpdate(context, buffer.data(),
                          static_cast<size_t>(count)) == 1;
  }
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int digest_size = 0;
  if (ok)
    ok = EVP_DigestFinal_ex(context, digest.data(), &digest_size) == 1 &&
         digest_size == 32;
  EVP_MD_CTX_free(context);
  if (::close(descriptor) != 0) ok = false;
  if (!ok || consumed != static_cast<uint64_t>(status.st_size))
    return !fail(error, "cannot hash the exact startup child output");
  static constexpr char kHex[] = "0123456789abcdef";
  std::string sha(64, '0');
  for (size_t index = 0; index < digest_size; ++index) {
    sha[index * 2] = kHex[digest[index] >> 4];
    sha[index * 2 + 1] = kHex[digest[index] & 0x0f];
  }
  *reference = {path.lexically_normal(),
                static_cast<uint64_t>(status.st_size), std::move(sha)};
  return true;
}

bool read_child_output(const fs::path &path, uint64_t maximum_bytes,
                       std::string *payload, std::string *error) {
  StartupProofReference reference;
  if (!observe_output_reference(path, maximum_bytes, &reference, error) ||
      !read_startup_proof(reference, payload, error, maximum_bytes)) {
    return false;
  }
  return true;
}

class ProductionStartupOperations final : public StartupCoordinatorOperations {
 public:
  ProductionStartupOperations(StartupIoRuntime runtime, fs::path target_root,
                              fs::path control_directory)
      : runtime_(std::move(runtime)),
        target_root_(std::move(target_root)),
        control_directory_(std::move(control_directory)) {}

  StartupStepResult startup_probe(StartupHookProbe *probe) override {
    if (probe == nullptr)
      return {StartupStepOutcome::CORRUPT,
              "null coordinator startup-probe output"};
    StartupProbe observed;
    if (wesql::remote_commit::startup_probe(&observed))
      return hook_failure("remote startup HEAD probe failed");
    if (observed.route == StartupRoute::DISABLED)
      return {StartupStepOutcome::CORRUPT,
              "remote startup hook returned a disabled route"};
    probe->route = observed.route == StartupRoute::BOOTSTRAP
                       ? StartupCoordinatorRoute::BOOTSTRAP
                       : StartupCoordinatorRoute::TAKEOVER;
    probe->head_object = {observed.head_body, observed.head_etag};
    probe->head_generation = observed.head_generation;
    probe->durable_cursor = {observed.durable_file, observed.durable_pos};
    return {StartupStepOutcome::READY, {}};
  }

  StartupStepResult acquire_epoch(ExactWriterEpoch *epoch) override {
    if (epoch == nullptr)
      return {StartupStepOutcome::CORRUPT,
              "null coordinator epoch output"};
    StartupEpochProof proof;
    if (acquire_startup_epoch(&proof))
      return hook_failure("remote startup epoch acquisition failed");
    epoch->object = {proof.body, proof.etag};
    epoch->value = std::move(proof.value);
    return {StartupStepOutcome::READY, {}};
  }

  StartupStepResult publish_snapshot(
      const StartupSnapshotPublishRequest &request,
      PublishedStartupHead *published) override {
    if (published == nullptr || runtime_.object_store == nullptr ||
        runtime_.conditional_io == nullptr || runtime_.publisher == nullptr) {
      return {StartupStepOutcome::CORRUPT,
              "snapshot publication runtime is incomplete"};
    }
    if (request.route == StartupCoordinatorRoute::TAKEOVER) {
      if (request.candidate == nullptr)
        return {StartupStepOutcome::CORRUPT,
                "takeover publication lacks its selected candidate"};
      const PublishResult bound = runtime_.publisher->bind_takeover_candidate(
          request.epoch.object, request.epoch.value,
          request.candidate->head_object, request.candidate->head);
      const StartupStepResult binding =
          map_publish_result(bound, "bind takeover publication parent");
      if (!binding.ready()) return binding;
    }
    std::error_code filesystem_error;
    const fs::path scratch = control_directory_ / "snapshot-readback";
    if (!fs::create_directory(scratch, filesystem_error) || filesystem_error) {
      return {StartupStepOutcome::LOCAL_IO_ERROR,
              "cannot create snapshot exact-readback directory"};
    }
    ObjectStoreSnapshotExactFileReader exact_reader(runtime_.object_store);
    ObjectStoreSnapshotPayloadIo payload_io(
        runtime_.object_store, runtime_.bucket, &exact_reader, scratch);
    SnapshotPublisher publisher(&payload_io, runtime_.conditional_io,
                                runtime_.publisher,
                                maximum_segment_bytes(),
                                kSnapshotMaxTotalPayloadBytes);
    SnapshotPublication publication;
    const PublishResult result = publisher.publish(request.snapshot_cut,
                                                   &publication);
    const StartupStepResult mapped =
        map_publish_result(result, "remote startup snapshot publication");
    if (!mapped.ready()) return mapped;
    const PublisherState &state = runtime_.publisher->state();
    if (!state.head.has_value() || !state.head_object.has_value() ||
        *state.head != publication.head) {
      return {StartupStepOutcome::CORRUPT,
              "snapshot publisher did not retain its exact HEAD proof"};
    }
    *published = {*state.head_object, *state.head};
    return {StartupStepOutcome::READY, {}};
  }

  StartupStepResult persist_restart_proof(
      const StartupRestartProof &proof,
      StartupRestartProofReference *reference) override {
    if (reference == nullptr)
      return {StartupStepOutcome::CORRUPT,
              "null restart-proof reference output"};
    std::string body;
    std::string error;
    if (!serialize_startup_restart_proof(runtime_.stream, proof, &body,
                                         &error)) {
      return {StartupStepOutcome::CORRUPT,
              "cannot serialize restart proof: " + error};
    }
    StartupProofReference persisted;
    const fs::path path = control_directory_ / "restart-proof.json";
    if (!write_startup_proof(path, body, &persisted, &error,
                             kStartupRestartProofMaxBytes)) {
      return {StartupStepOutcome::LOCAL_IO_ERROR,
              "cannot persist restart proof: " + error};
    }
    *reference = {persisted.path, persisted.size, persisted.sha256};
    return {StartupStepOutcome::READY, {}};
  }

  StartupStepResult activate_installed_root(
      const StartupActivationRequest &request) override {
    const TargetClassification classification =
        classify_local_target(target_root_, request.marker, true);
    if (classification.classification != TargetClass::MANAGED_REPLACE ||
        !classification.marker.has_value() ||
        *classification.marker != request.marker) {
      return {StartupStepOutcome::CORRUPT,
              "installed root marker differs before stock recovery"};
    }
    fs::path configured_root;
    std::string error;
    if (!canonical_target_root(&configured_root, &error) ||
        !same_root(configured_root, target_root_)) {
      return {StartupStepOutcome::CORRUPT,
              "installed root is not the configured data directory"};
    }

    StartupEpochProof epoch;
    epoch.body = request.epoch.object.body;
    epoch.etag = request.epoch.object.etag;
    epoch.value = request.epoch.value;
    epoch.head_body = request.published.head_object.body;
    epoch.head_etag = request.published.head_object.etag;
    epoch.head_generation = request.published.head.generation;
    if (adopt_startup_epoch(epoch, StartupEpochAdoptionRole::INSTALLED_ROOT))
      return hook_failure("installed root epoch adoption failed");

    InstalledRootActivationProof activation;
    activation.head_body = request.published.head_object.body;
    activation.head_etag = request.published.head_object.etag;
    activation.marker = request.marker;
    activation.recovered_file = request.expected_root.recovered_cursor.file;
    activation.recovered_pos = request.expected_root.recovered_cursor.pos;
    activation.marker_matches = true;
    activation.root_identity_matches = true;
    if (wesql::remote_commit::activate_installed_root(activation))
      return hook_failure("installed root pre-recovery activation failed");
    return {StartupStepOutcome::READY, {}};
  }

 private:
  StartupIoRuntime runtime_;
  fs::path target_root_;
  fs::path control_directory_;
};

class ObservationPreparedVerifier final
    : public NativeRecoveryPreparedVerifier {
 public:
  explicit ObservationPreparedVerifier(ServerRootVerificationRequest request)
      : request_(std::move(request)) {}

  bool prepared_sets_empty(std::string *error) override {
    ServerRootEvidenceObservation observation;
    const StartupStepResult collected =
        collect_server_root_observation(request_, &observation);
    if (!collected.ready() || !observation.prepared.available) {
      if (error != nullptr) {
        *error = collected.detail.empty() ? observation.prepared.detail
                                          : collected.detail;
        if (error->empty())
          *error = "prepared transaction inventory is unavailable";
      }
      return false;
    }
    if (observation.prepared.value.internal_entries != 0 ||
        observation.prepared.value.external_entries != 0) {
      if (error != nullptr)
        *error = "prepared transaction inventory is not empty";
      return false;
    }
    return true;
  }

 private:
  ServerRootVerificationRequest request_;
};

constexpr size_t kStartupAdapterOptionCount =
    static_cast<size_t>(StartupAdapterOption::COUNT);

struct AdapterState {
  std::vector<std::string> original_argv;
  std::array<uint32_t, kStartupAdapterOptionCount> option_counts{};
  bool original_captured{false};
  bool original_valid{false};
  uint32_t declarative_remote_commit_records{0};
  bool declarative_remote_commit_enabled{false};
  StartupServerMode mode{StartupServerMode::NORMAL};
  bool before_datadir_called{false};
  bool after_daemonization_called{false};
  bool defer_networking{false};
  bool networking_ready{true};
  bool force_skip_replica{false};
  bool clean_exit_prepared{false};
  bool clean_exit_finalized{false};
  int inherited_daemon_pipe_fd{-1};
  PendingOutput pending_output{PendingOutput::NONE};

  fs::path executable;
  fs::path target_root;
  fs::path control_directory;
  fs::path temporary_root;
  fs::path output_path;
  fs::path configured_binlog_index;
  std::string configured_binlog_basename;
  StartupIoRuntime runtime;
  ExternalDeployment external;
  std::optional<StartupDeploymentIdentity> expected_deployment;
  std::optional<StartupWorkerRequest> worker_request;
  std::optional<RecoveryPlan> worker_candidate;
  std::optional<StartupBootstrapPreflight> pending_preflight;
  std::optional<StartupWorkerCompletion> pending_completion;
  RetainedSmartengineSnapshotEvidence retained_smartengine;

  std::unique_ptr<ObjectStorePayloadIo> payload_io;
  std::unique_ptr<NativeBinlogImageValidator> binlog_validator;
  std::unique_ptr<DefaultStartupCoordinatorStorage> storage;
  std::unique_ptr<ProductionStartupOperations> operations;
  std::unique_ptr<StartupCoordinator> coordinator;
};

#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
AdapterState &g_adapter = *new AdapterState;
#else
AdapterState g_adapter;
#endif

size_t option_index(StartupAdapterOption option) {
  return static_cast<size_t>(option);
}

bool canonical_internal_option(std::string_view argument,
                               StartupAdapterOption *option) {
  static constexpr std::array<std::pair<std::string_view,
                                        StartupAdapterOption>,
                              kStartupAdapterOptionCount>
      kOptions{{{"--wesql-remote-startup-mode", StartupAdapterOption::MODE},
                {"--wesql-remote-startup-input",
                 StartupAdapterOption::INPUT_PATH},
                {"--wesql-remote-startup-input-size",
                 StartupAdapterOption::INPUT_SIZE},
                {"--wesql-remote-startup-input-sha256",
                 StartupAdapterOption::INPUT_SHA256},
                {"--wesql-remote-startup-output",
                 StartupAdapterOption::OUTPUT_PATH},
                {"--wesql-remote-startup-daemon-pipe-fd",
                 StartupAdapterOption::DAEMON_PIPE_FD}}};
  for (const auto &[name, candidate] : kOptions) {
    if (argument.starts_with(name) && argument.size() > name.size() &&
        argument[name.size()] == '=') {
      *option = candidate;
      return true;
    }
    if (argument == name ||
        (argument.starts_with("--loose-") &&
         argument.substr(8).starts_with(name.substr(2)))) {
      *option = candidate;
      return false;
    }
  }
  return false;
}

bool validate_option_shape(std::string *error) {
  if (!g_adapter.original_captured || !g_adapter.original_valid ||
      g_adapter.original_argv.empty()) {
    return !fail(error, "mysqld original argv was not captured exactly once");
  }
  std::array<uint32_t, kStartupAdapterOptionCount> original_counts{};
  for (size_t index = 1; index < g_adapter.original_argv.size(); ++index) {
    StartupAdapterOption option;
    const std::string_view argument = g_adapter.original_argv[index];
    const std::string_view option_name =
        argument.substr(0, argument.find('='));
    if (option_name.starts_with("--") &&
        (option_name.find("wesql_remote_startup") !=
             std::string_view::npos ||
         option_name.find("wesql-remote-startup") !=
             std::string_view::npos)) {
      if (!canonical_internal_option(argument, &option))
        return !fail(error,
                     "internal startup options require canonical inline "
                     "command-line spelling");
      ++original_counts[option_index(option)];
    }
  }
  if (original_counts != g_adapter.option_counts ||
      std::any_of(g_adapter.option_counts.begin(),
                  g_adapter.option_counts.end(),
                  [](uint32_t count) { return count > 1; })) {
    return !fail(error,
                 "internal startup options are duplicated or came from a "
                 "defaults file");
  }

  const auto count = [](StartupAdapterOption option) {
    return g_adapter.option_counts[option_index(option)];
  };
  if (count(StartupAdapterOption::MODE) == 0) {
    g_adapter.mode = StartupServerMode::NORMAL;
    if (std::any_of(g_adapter.option_counts.begin(),
                    g_adapter.option_counts.end(),
                    [](uint32_t value) { return value != 0; })) {
      return !fail(error, "startup proof option is missing its internal mode");
    }
    return true;
  }
  if (opt_remote_startup_mode == nullptr ||
      !parse_startup_server_mode(opt_remote_startup_mode, &g_adapter.mode) ||
      g_adapter.mode == StartupServerMode::NORMAL) {
    return !fail(error, "internal startup mode is invalid");
  }
  const bool preflight =
      g_adapter.mode == StartupServerMode::BOOTSTRAP_PREFLIGHT;
  const bool installed =
      g_adapter.mode == StartupServerMode::INSTALLED_REEXEC;
  const uint32_t daemon_pipe_count =
      count(StartupAdapterOption::DAEMON_PIPE_FD);
  if ((preflight &&
       (count(StartupAdapterOption::INPUT_PATH) != 0 ||
        count(StartupAdapterOption::INPUT_SIZE) != 0 ||
        count(StartupAdapterOption::INPUT_SHA256) != 0 ||
        count(StartupAdapterOption::OUTPUT_PATH) != 1)) ||
      (!preflight &&
       (count(StartupAdapterOption::INPUT_PATH) != 1 ||
        count(StartupAdapterOption::INPUT_SIZE) != 1 ||
        count(StartupAdapterOption::INPUT_SHA256) != 1 ||
        count(StartupAdapterOption::OUTPUT_PATH) != (installed ? 0U : 1U)))) {
    return !fail(error, "internal startup mode has an invalid proof shape");
  }
  if (daemon_pipe_count > 1 || (!installed && daemon_pipe_count != 0) ||
      (daemon_pipe_count == 1 &&
       (opt_remote_startup_daemon_pipe_fd <= STDERR_FILENO ||
        opt_remote_startup_daemon_pipe_fd >
            static_cast<ulong>(std::numeric_limits<int>::max())))) {
    return !fail(error,
                 "internal startup mode has an invalid daemon pipe shape");
  }
  if ((!preflight &&
       (opt_remote_startup_input_path == nullptr ||
        opt_remote_startup_input_path[0] == '\0' ||
        opt_remote_startup_input_size == 0 ||
        opt_remote_startup_input_sha256 == nullptr ||
        opt_remote_startup_input_sha256[0] == '\0')) ||
      (!installed && (opt_remote_startup_output_path == nullptr ||
                      opt_remote_startup_output_path[0] == '\0'))) {
    return !fail(error, "internal startup proof value is empty");
  }
  g_adapter.inherited_daemon_pipe_fd =
      daemon_pipe_count == 0
          ? -1
          : static_cast<int>(opt_remote_startup_daemon_pipe_fd);
  return true;
}

bool input_reference(StartupProofReference *reference, std::string *error) {
  if (reference == nullptr)
    return !fail(error, "null internal input-proof reference");
  fs::path path(opt_remote_startup_input_path == nullptr
                    ? std::string()
                    : std::string(opt_remote_startup_input_path));
  if (path.empty() || !path.is_absolute() ||
      path.lexically_normal() != path ||
      !lowercase_sha256(configured_string(
          opt_remote_startup_input_sha256))) {
    return !fail(error, "internal input-proof reference is invalid");
  }
  *reference = {path, static_cast<uint64_t>(opt_remote_startup_input_size),
                opt_remote_startup_input_sha256};
  return true;
}

bool initialize_runtime(std::string *error) {
  const bool bootstrap_preflight =
      g_adapter.mode == StartupServerMode::BOOTSTRAP_PREFLIGHT && opt_initialize;
  if (wesql::remote_commit::initialize(bootstrap_preflight)) {
    const std::string detail = startup_error();
    return !fail(error,
                 detail.empty() ? "remote commit initialization failed"
                                : detail);
  }
  if (!startup_io_runtime(&g_adapter.runtime) ||
      g_adapter.runtime.object_store == nullptr ||
      g_adapter.runtime.conditional_io == nullptr ||
      g_adapter.runtime.publisher == nullptr ||
      g_adapter.runtime.stream.stream_id.empty()) {
    return !fail(error, "remote startup IO runtime is unavailable");
  }
  return true;
}

bool ensure_runtime_deployment(const StartupDeploymentIdentity &expected,
                               bool require_configured_uuid,
                               std::string *error) {
  if (!same_external_deployment(g_adapter.external, expected,
                                require_configured_uuid, error)) {
    return false;
  }
  if (server_uuid_ptr == nullptr ||
      std::string_view(server_uuid_ptr) != expected.server_uuid) {
    return !fail(error,
                 "opened root UUID differs from the authenticated startup "
                 "deployment");
  }
  if (!declare_server_root_runtime_deployment(expected, error)) return false;
  return true;
}

bool create_parent_control(std::string *error) {
  if (!create_startup_control_directory(g_adapter.target_root,
                                        &g_adapter.control_directory, error)) {
    return false;
  }
  g_adapter.temporary_root =
      startup_temporary_data_root(g_adapter.control_directory);
  std::error_code status_error;
  if (fs::exists(g_adapter.temporary_root, status_error) || status_error)
    return !fail(error,
                 "fresh remote startup temporary root already exists");
  return true;
}

bool build_and_spawn(const StartupChildSpec &spec,
                     StartupProcessResult *process, std::string *error) {
  std::vector<std::string> arguments;
  if (!build_startup_child_argv(g_adapter.original_argv, spec, &arguments,
                                error)) {
    return false;
  }
  *process = spawn_startup_child(g_adapter.executable, arguments);
  if (!process->succeeded()) {
    return !fail(error, process->detail.empty()
                            ? "startup child did not exit cleanly"
                            : process->detail);
  }
  return true;
}

bool run_bootstrap_preflight(StartupBootstrapPreflight *preflight,
                             std::string *error) {
  const fs::path output = g_adapter.control_directory / "preflight.json";
  StartupChildSpec spec;
  spec.mode = StartupServerMode::BOOTSTRAP_PREFLIGHT;
  spec.data_directory = g_adapter.temporary_root;
  spec.output_path = output;
  spec.pid_file = g_adapter.control_directory / "preflight.pid";
  spec.error_log = g_adapter.control_directory / "preflight.err";
  spec.initialize_insecure = true;
  StartupProcessResult process;
  if (!build_and_spawn(spec, &process, error)) return false;
  std::string body;
  if (!read_child_output(output, kStartupBootstrapPreflightMaxBytes, &body,
                         error) ||
      !parse_startup_bootstrap_preflight(body, preflight, error)) {
    return false;
  }
  if (!preflight->child_exited_cleanly ||
      preflight->root != g_adapter.temporary_root ||
      preflight->initialized_deployment.stream_id !=
          g_adapter.runtime.stream.stream_id ||
      !same_external_deployment(g_adapter.external,
                                preflight->initialized_deployment, false,
                                error)) {
    return !fail(error,
                 "bootstrap preflight output belongs to another root or "
                 "deployment");
  }
  StartupProofReference output_reference;
  if (!observe_output_reference(output, kStartupBootstrapPreflightMaxBytes,
                                &output_reference, error) ||
      !remove_startup_proof(output_reference, error)) {
    return false;
  }
  return true;
}

bool setup_parent_coordinator(const StartupDeploymentIdentity &deployment,
                              std::string *error) {
  if (!declare_server_root_runtime_deployment(deployment, error)) return false;
  const uint64_t segment_limit = maximum_segment_bytes();
  const uint32_t maximum_event = static_cast<uint32_t>(
      std::min<uint64_t>(segment_limit,
                         std::numeric_limits<uint32_t>::max()));
  if (maximum_event == 0)
    return !fail(error, "remote startup event bound is zero");
  g_adapter.payload_io = std::make_unique<ObjectStorePayloadIo>(
      g_adapter.runtime.object_store, g_adapter.runtime.bucket);
  g_adapter.binlog_validator =
      std::make_unique<NativeBinlogImageValidator>(maximum_event);
  g_adapter.storage = std::make_unique<DefaultStartupCoordinatorStorage>(
      g_adapter.runtime.conditional_io, g_adapter.payload_io.get(),
      g_adapter.binlog_validator.get(), g_adapter.runtime.stream);
  g_adapter.operations = std::make_unique<ProductionStartupOperations>(
      g_adapter.runtime, g_adapter.target_root, g_adapter.control_directory);
  g_adapter.coordinator = std::make_unique<StartupCoordinator>(
      g_adapter.runtime.stream, g_adapter.storage.get(),
      g_adapter.operations.get());
  return true;
}

bool exact_worker_candidate(const StartupWorkerRequest &request,
                            RecoveryPlan *candidate, std::string *error) {
  if (!request.candidate.has_value())
    return !fail(error, "takeover worker request has no candidate");
  RecoveryChainReader reader(g_adapter.runtime.conditional_io,
                             g_adapter.runtime.stream);
  const RecoveryReadResult read = reader.read(candidate);
  if (!read.ready())
    return !fail(error, "cannot exact-read worker candidate: " + read.detail);
  const StartupCandidateBoundary &expected = *request.candidate;
  if (!same_published(candidate->head_object, expected.head.object) ||
      candidate->head != expected.head.value ||
      candidate->snapshot.cursor != expected.replay_exclusive_start ||
      candidate->head.durable_cursor != expected.replay_inclusive_end ||
      !same_published(candidate->epoch_object, request.epoch.object) ||
      candidate->epoch != request.epoch.value) {
    return !fail(error,
                 "worker candidate or writer epoch changed after request "
                 "authentication");
  }
  return true;
}

bool adopt_worker_request(const StartupWorkerRequest &request,
                          std::string *error) {
  StartupEpochProof proof;
  proof.body = request.epoch.object.body;
  proof.etag = request.epoch.object.etag;
  proof.value = request.epoch.value;
  StartupEpochAdoptionRole role =
      StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT;
  if (request.route == StartupCoordinatorRoute::TAKEOVER) {
    if (!request.candidate.has_value())
      return !fail(error, "takeover worker request has no HEAD proof");
    proof.head_body = request.candidate->head.object.body;
    proof.head_etag = request.candidate->head.object.etag;
    proof.head_generation = request.candidate->head.value.generation;
    role = StartupEpochAdoptionRole::TAKEOVER_RECOVERY;
  }
  if (adopt_startup_epoch(proof, role)) {
    const std::string detail = startup_error();
    return !fail(error, detail.empty() ? "worker epoch adoption failed"
                                      : detail);
  }
  return true;
}

bool load_worker_request(std::string *error) {
  StartupProofReference reference;
  std::string body;
  if (!input_reference(&reference, error) ||
      !read_startup_proof(reference, &body, error,
                          kStartupWorkerRequestMaxBytes)) {
    return false;
  }
  StartupWorkerRequest request;
  if (!parse_startup_worker_request(g_adapter.runtime.stream, body, &request,
                                    error) ||
      request.root != g_adapter.target_root ||
      request.deployment.stream_id != g_adapter.runtime.stream.stream_id ||
      !same_external_deployment(
          g_adapter.external, request.deployment,
          request.route == StartupCoordinatorRoute::TAKEOVER, error) ||
      !adopt_worker_request(request, error)) {
    return false;
  }
  if (request.route == StartupCoordinatorRoute::TAKEOVER) {
    RecoveryPlan candidate;
    if (!exact_worker_candidate(request, &candidate, error)) return false;
    g_adapter.worker_candidate = std::move(candidate);
  }
  if (!remove_startup_proof(reference, error)) return false;
  g_adapter.expected_deployment = request.deployment;
  g_adapter.worker_request = std::move(request);
  return true;
}

bool load_restart_proof(std::string *error) {
  StartupProofReference input;
  std::string body;
  if (!input_reference(&input, error) ||
      !read_startup_proof(input, &body, error,
                          kStartupRestartProofMaxBytes)) {
    return false;
  }
  StartupRestartProof proof;
  if (!parse_startup_restart_proof(g_adapter.runtime.stream, body, &proof,
                                   error) ||
      proof.deployment.stream_id != g_adapter.runtime.stream.stream_id ||
      !same_external_deployment(
          g_adapter.external, proof.deployment,
          proof.route == StartupCoordinatorRoute::TAKEOVER, error)) {
    return false;
  }
  g_adapter.operations = std::make_unique<ProductionStartupOperations>(
      g_adapter.runtime, g_adapter.target_root,
      input.path.parent_path());
  const uint64_t segment_limit = maximum_segment_bytes();
  const uint32_t maximum_event = static_cast<uint32_t>(
      std::min<uint64_t>(segment_limit,
                         std::numeric_limits<uint32_t>::max()));
  g_adapter.payload_io = std::make_unique<ObjectStorePayloadIo>(
      g_adapter.runtime.object_store, g_adapter.runtime.bucket);
  g_adapter.binlog_validator =
      std::make_unique<NativeBinlogImageValidator>(maximum_event);
  g_adapter.storage = std::make_unique<DefaultStartupCoordinatorStorage>(
      g_adapter.runtime.conditional_io, g_adapter.payload_io.get(),
      g_adapter.binlog_validator.get(), g_adapter.runtime.stream);
  g_adapter.coordinator = std::make_unique<StartupCoordinator>(
      g_adapter.runtime.stream, g_adapter.storage.get(),
      g_adapter.operations.get());
  const StartupCoordinatorResult adopted = g_adapter.coordinator->adopt_restart_proof(
      {g_adapter.target_root, proof.deployment}, proof);
  if (!adopted.post_engine_verification_required())
    return !fail(error, adopted.detail.empty()
                            ? "installed restart proof was not activated"
                            : adopted.detail);
  if (!remove_startup_proof(input, error)) return false;
  g_adapter.control_directory = input.path.parent_path();
  g_adapter.expected_deployment = proof.deployment;
  return true;
}

bool run_parent_orchestration(std::string *error) {
  StartupProbe probe;
  if (wesql::remote_commit::startup_probe(&probe)) {
    const std::string detail = startup_error();
    return !fail(error, detail.empty() ? "HEAD-first startup probe failed"
                                      : detail);
  }
  if (probe.route == StartupRoute::DISABLED)
    return !fail(error, "remote startup probe returned a disabled route");
  if (!create_parent_control(error)) return false;

  StartupDeploymentIdentity deployment;
  std::optional<StartupBootstrapPreflight> preflight;
  if (probe.route == StartupRoute::BOOTSTRAP) {
    StartupBootstrapPreflight completed;
    if (!run_bootstrap_preflight(&completed, error)) return false;
    deployment = completed.initialized_deployment;
    preflight = std::move(completed);
  } else {
    if (g_adapter.external.configured_uuid.empty())
      return !fail(error,
                   "takeover requires an explicit deployment server UUID");
    deployment = make_deployment(g_adapter.runtime.stream, g_adapter.external,
                                 g_adapter.external.configured_uuid);
  }
  if (!setup_parent_coordinator(deployment, error)) return false;

  StartupCoordinatorOptions options;
  options.target_root = g_adapter.target_root;
  options.materialize.temp_root = g_adapter.temporary_root;
  options.materialize.binlog_index_relative_path =
      g_adapter.configured_binlog_index;
  options.materialize.max_object_bytes = maximum_segment_bytes();
  options.deployment = deployment;
  options.worker_nonce = random_hex(32);
  options.bootstrap_preflight = std::move(preflight);
  if (!lowercase_sha256(options.worker_nonce))
    return !fail(error, "cannot generate worker request nonce");
  StartupCoordinatorResult prepared =
      g_adapter.coordinator->prepare_worker(options);
  if (!prepared.worker_required())
    return !fail(error, prepared.detail.empty()
                            ? "startup coordinator did not request a worker"
                            : prepared.detail);

  std::string request_body;
  if (!serialize_startup_worker_request(g_adapter.runtime.stream,
                                        *prepared.worker_request,
                                        &request_body, error)) {
    return false;
  }
  StartupProofReference request_reference;
  const fs::path request_path =
      g_adapter.control_directory / "worker-request.json";
  if (!write_startup_proof(request_path, request_body, &request_reference,
                           error, kStartupWorkerRequestMaxBytes)) {
    return false;
  }
  const StartupServerMode worker_mode =
      prepared.worker_request->route == StartupCoordinatorRoute::BOOTSTRAP
          ? StartupServerMode::BOOTSTRAP_SNAPSHOT
          : StartupServerMode::TAKEOVER_RECOVERY;
  const fs::path completion_path =
      g_adapter.control_directory / "worker-completion.json";
  StartupChildSpec worker;
  worker.mode = worker_mode;
  worker.data_directory = g_adapter.temporary_root;
  worker.output_path = completion_path;
  worker.pid_file = g_adapter.control_directory / "worker.pid";
  worker.error_log = g_adapter.control_directory / "worker.err";
  worker.input = request_reference;
  StartupProcessResult process;
  if (!build_and_spawn(worker, &process, error)) return false;

  std::string completion_body;
  StartupWorkerCompletion completion;
  if (!read_child_output(completion_path, kStartupWorkerEvidenceMaxBytes,
                         &completion_body, error) ||
      !parse_startup_worker_completion(completion_body, &completion, error) ||
      !completion.child_exited_cleanly) {
    return !fail(error, error != nullptr && !error->empty()
                            ? *error
                            : "startup worker completion is not clean");
  }
  StartupProofReference completion_reference;
  if (!observe_output_reference(completion_path,
                                kStartupWorkerEvidenceMaxBytes,
                                &completion_reference, error) ||
      !remove_startup_proof(completion_reference, error)) {
    return false;
  }
  StartupCoordinatorResult finished =
      g_adapter.coordinator->finish_worker(completion);
  if (!finished.restart_required())
    return !fail(error, finished.detail.empty()
                            ? "startup coordinator did not require re-exec"
                            : finished.detail);

  StartupChildSpec installed;
  installed.mode = StartupServerMode::INSTALLED_REEXEC;
  installed.data_directory = g_adapter.target_root;
  installed.input = {finished.restart_proof->path,
                     finished.restart_proof->size,
                     finished.restart_proof->sha256};
  installed.inherited_daemon_pipe_fd = g_adapter.inherited_daemon_pipe_fd;
  std::vector<std::string> arguments;
  if (!build_startup_child_argv(g_adapter.original_argv, installed, &arguments,
                                error)) {
    return false;
  }
  return reexec_startup_server(g_adapter.executable, arguments, error);
}

bool validate_worker_route(std::string *error) {
  if (!g_adapter.worker_request.has_value())
    return !fail(error, "startup worker request is not retained");
  const bool bootstrap =
      g_adapter.worker_request->route == StartupCoordinatorRoute::BOOTSTRAP;
  if ((bootstrap &&
       (!may_run_startup_bootstrap_snapshot_worker() ||
        g_adapter.mode != StartupServerMode::BOOTSTRAP_SNAPSHOT)) ||
      (!bootstrap &&
       (!may_run_startup_recovery_worker() ||
        g_adapter.mode != StartupServerMode::TAKEOVER_RECOVERY))) {
    return !fail(error,
                 "startup worker mode is not authorized by the adopted "
                 "epoch and HEAD");
  }
  return true;
}

bool build_base_worker_completion(std::string *error) {
  if (!validate_worker_route(error) ||
      !g_adapter.expected_deployment.has_value() ||
      !ensure_runtime_deployment(*g_adapter.expected_deployment,
                                 g_adapter.worker_request->route ==
                                     StartupCoordinatorRoute::TAKEOVER,
                                 error)) {
    return false;
  }
  const StartupWorkerRequest &request = *g_adapter.worker_request;
  const RecoveryPlan *candidate = nullptr;
  if (request.route == StartupCoordinatorRoute::TAKEOVER) {
    if (!g_adapter.worker_candidate.has_value() ||
        !request.materialized.has_value()) {
      return !fail(error,
                   "takeover worker lost its exact candidate or materialized "
                   "root");
    }
    candidate = &*g_adapter.worker_candidate;
    ObservationPreparedVerifier verifier({
        StartupCoordinatorRoute::TAKEOVER, request.root, request.deployment,
        false, candidate, nullptr});
    NativeRecoveryRequest replay;
    replay.stream_id = g_adapter.runtime.stream.stream_id;
    replay.candidate = candidate;
    replay.materialized = &*request.materialized;
    replay.exclusive_start = request.candidate->replay_exclusive_start;
    replay.inclusive_end = request.candidate->replay_inclusive_end;
    replay.max_event_bytes = static_cast<uint32_t>(
        std::min<uint64_t>(maximum_segment_bytes(),
                           std::numeric_limits<uint32_t>::max()));
    const NativeRecoveryResult recovered =
        replay_bounded_native_tail(replay, &verifier);
    if (!recovered.applied())
      return !fail(error, "bounded native recovery failed: " +
                              recovered.detail);
  }

  ServerRootVerificationRequest verification{
      request.route, request.root, request.deployment, false, candidate,
      nullptr};
  StartupRootEvidence evidence;
  const StartupStepResult collected = collect_server_root_evidence(
      verification, &evidence, &g_adapter.retained_smartengine);
  if (!collected.ready())
    return !fail(error, "worker root evidence failed: " + collected.detail);

  const auto release_after_failure = [error]() {
    const StartupStepResult released = g_adapter.retained_smartengine.release();
    if (!released.ready() && error != nullptr) {
      if (!error->empty()) error->append("; ");
      error->append("cannot release retained SmartEngine snapshot: ")
          .append(released.detail);
    }
    return false;
  };

  FixedSnapshotCut cut;
  cut.snapshot_id = random_hex(16);
  cut.writer = {request.epoch.value.writer_id, request.epoch.value.epoch};
  cut.proof.public_cursor = evidence.recovered_cursor;
  cut.proof.public_gtid = evidence.recovered_gtid;
  cut.proof.image_cursor = evidence.recovered_cursor;
  cut.proof.image_gtid = evidence.recovered_gtid;
  cut.server_identity.server_uuid = request.deployment.server_uuid;
  cut.deployment_fingerprints = request.deployment.fingerprints;
  if (request.route == StartupCoordinatorRoute::BOOTSTRAP) {
    cut.proof.source = FixedCutSource::EMPTY_SOURCE;
    cut.proof.empty_source_scan_stable = true;
    cut.proof.internal_prepared_empty = true;
    cut.proof.external_xa_empty = true;
    cut.proof.old_tc_authority_empty = true;
    cut.proof.user_state_empty = true;
    cut.proof.legacy_live_extents_empty = true;
    cut.log_anchor.kind = LogAnchorKind::EMPTY_BASE;
    cut.log_anchor.cursor = evidence.recovered_cursor;
  } else {
    std::string head_sha;
    if (!sha256_hex(candidate->head_object.body, &head_sha, error))
      return release_after_failure();
    cut.proof.source = FixedCutSource::RECOVERED_TAKEOVER;
    cut.proof.source_head_generation = candidate->head.generation;
    cut.proof.source_head_body_sha256 = std::move(head_sha);
    cut.log_anchor.kind = LogAnchorKind::MANIFEST_BOUNDARY;
    cut.log_anchor.generation = candidate->head.generation;
    cut.log_anchor.manifest = candidate->head.manifest;
    cut.log_anchor.cursor = candidate->head.durable_cursor;
  }
  if (cut.snapshot_id.size() != 32 ||
      cut.snapshot_id.find_first_not_of("0123456789abcdef") !=
          std::string::npos) {
    fail(error, "cannot generate startup snapshot id");
    return release_after_failure();
  }
  cut.smartengine_extents.reserve(
      g_adapter.retained_smartengine.canonical_live_extents.size());
  for (const SmartengineExtentRef &extent :
       g_adapter.retained_smartengine.canonical_live_extents) {
    cut.smartengine_extents.push_back(
        {extent.writer_epoch, extent.allocation_seq,
         extent.database_name_hex, extent.index_id, extent.object_id,
         extent.key, extent.size, extent.sha256});
  }
  uint64_t extent_count = 0;
  std::string extent_digest;
  if (!startup_extent_set_digest(cut.smartengine_extents, &extent_count,
                                 &extent_digest, error)) {
    return release_after_failure();
  }
  if (extent_count != evidence.exported_extent_count ||
      extent_digest != evidence.exported_extent_set_sha256) {
    fail(error, "retained SmartEngine extent set differs from worker evidence");
    return release_after_failure();
  }
  g_adapter.pending_completion = StartupWorkerCompletion{
      false, request.request_nonce, request.request_sha256,
      std::move(evidence), std::move(cut)};
  g_adapter.pending_output = PendingOutput::WORKER_COMPLETION;
  return true;
}

bool is_numbered_binlog(std::string_view relative,
                        std::string_view basename) {
  if (!relative.starts_with(basename) || relative.size() <= basename.size() ||
      relative[basename.size()] != '.')
    return false;
  const std::string_view sequence = relative.substr(basename.size() + 1);
  return sequence.size() >= 6 &&
         std::all_of(sequence.begin(), sequence.end(), [](char byte) {
           return byte >= '0' && byte <= '9';
         });
}

bool has_prefix_component(const fs::path &path, std::string_view component) {
  const auto iterator = path.begin();
  return iterator != path.end() && iterator->generic_string() == component;
}

bool tls_or_private_file(const fs::path &relative) {
  if (relative.has_parent_path()) return false;
  const std::string name = relative.filename().string();
  const std::string extension = relative.extension().string();
  return extension == ".pem" || extension == ".key" ||
         extension == ".crt" || extension == ".p12" ||
         name == "private_key" || name == "server-key";
}

bool classify_payload(const std::string &encoded,
                      const Cursor &seed_cursor,
                      LocalSnapshotPayload *payload, bool *excluded,
                      std::string *error) {
  *excluded = false;
  const fs::path relative(encoded);
  const std::string filename = relative.filename().string();
  const std::string extension = relative.extension().string();
  if (encoded == seed_cursor.file ||
      encoded == g_adapter.configured_binlog_index.generic_string() ||
      is_numbered_binlog(encoded, g_adapter.configured_binlog_basename)) {
    *excluded = true;
    return true;
  }
  if (encoded == kLocalInstallMarkerName)
    return !fail(error, "worker root already contains an install marker");
  if (encoded == "ib_buffer_pool" || encoded == "mysqld-auto.cnf" ||
      filename.ends_with(".pid") || filename.ends_with(".err") ||
      tls_or_private_file(relative) ||
      has_prefix_component(relative, "#innodb_temp")) {
    *excluded = true;
    return true;
  }
  if (is_mysql_dictionary_snapshot_file(relative)) {
    payload->component = "mysql-dd";
    payload->format = std::string(kMysqlDdFormat);
  } else if (has_prefix_component(relative, "smartengine")) {
    if (filename == "LOCK" || filename == "Log") {
      *excluded = true;
      return true;
    }
    if (extension == ".wal") {
      payload->component = "smartengine-wal";
      payload->format = std::string(kSmartengineWalFormat);
    } else {
      payload->component = "smartengine-meta";
      payload->format = std::string(kSmartengineMetaFormat);
    }
  } else if (has_prefix_component(relative, "#innodb_redo") ||
             filename.starts_with("#ib_") ||
             filename.starts_with("ibdata") ||
             filename.starts_with("undo_") ||
             filename.starts_with("ibtmp") || extension == ".ibd" ||
             extension == ".ibu" || extension == ".cfg" ||
             extension == ".cfp") {
    if (filename.starts_with("ibtmp")) {
      *excluded = true;
      return true;
    }
    payload->component = "innodb";
    payload->format = std::string(kInnodbFormat);
  } else {
    return !fail(error,
                 "stopped worker root contains an unclassified persistent "
                 "file: " +
                     encoded);
  }
  payload->relative_path = encoded;
  payload->local_path = g_adapter.target_root / relative;
  return true;
}

bool fsync_file(const fs::path &path, std::string *error) {
  const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0)
    return !fail(error, "cannot open stopped-root payload for fsync: " +
                            path.string());
  struct stat status {};
  const bool regular =
      ::fstat(descriptor, &status) == 0 && S_ISREG(status.st_mode);
  const bool synced = regular && ::fsync(descriptor) == 0;
  const bool closed = ::close(descriptor) == 0;
  if (!synced || !closed)
    return !fail(error, "cannot fsync stopped-root payload: " + path.string());
  return true;
}

bool fsync_directories(const fs::path &root,
                       const std::set<fs::path> &directories,
                       std::string *error) {
  for (const fs::path &relative : directories) {
    const fs::path path = root / relative;
    const int descriptor =
        ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (descriptor < 0) {
      return !fail(error,
                   "cannot fsync stopped-root directory: " + path.string());
    }
    const int sync_result = ::fsync(descriptor);
    const int close_result = ::close(descriptor);
    if (sync_result != 0 || close_result != 0)
      return !fail(error,
                   "cannot fsync stopped-root directory: " + path.string());
  }
  return true;
}

bool finalize_worker_files(std::string *error) {
  if (!g_adapter.pending_completion.has_value())
    return !fail(error, "worker completion is not pending");
  StartupWorkerCompletion &completion = *g_adapter.pending_completion;
  const fs::path root = g_adapter.target_root;
  StartupRootSnapshot before;
  if (!snapshot_stable_startup_root(root, &before, error)) return false;
  std::vector<LocalSnapshotPayload> objects;
  std::set<std::string> components;
  std::set<fs::path> directories{fs::path()};
  for (const StartupRootEntry &entry : before.entries) {
    if (entry.type != StartupRootEntryType::REGULAR_FILE) continue;
    LocalSnapshotPayload payload;
    bool excluded = false;
    if (!classify_payload(entry.relative_path,
                          completion.root_evidence.recovered_cursor,
                          &payload, &excluded, error)) {
      return false;
    }
    if (excluded) continue;
    if (!fsync_file(payload.local_path, error)) return false;
    components.insert(payload.component);
    fs::path parent = fs::path(payload.relative_path).parent_path();
    while (!parent.empty()) {
      directories.insert(parent);
      parent = parent.parent_path();
    }
    objects.push_back(std::move(payload));
  }
  static const std::set<std::string> kRequired{
      "innodb", "mysql-dd", "smartengine-meta", "smartengine-wal"};
  if (components != kRequired)
    return !fail(error,
                 "stopped worker root does not contain every required "
                 "snapshot component");
  const fs::path seed =
      root / completion.root_evidence.recovered_cursor.file;
  if (!fsync_file(seed, error)) return false;
  directories.insert(seed.parent_path().lexically_relative(root));
  if (!fsync_directories(root, directories, error)) return false;
  StartupRootSnapshot after;
  if (!snapshot_stable_startup_root(root, &after, error) || after != before)
    return !fail(error,
                 "stopped worker root changed while payloads were fsynced");
  std::error_code size_error;
  const uintmax_t seed_size = fs::file_size(seed, size_error);
  if (size_error ||
      seed_size != completion.root_evidence.recovered_cursor.pos) {
    return !fail(error,
                 "stopped terminal binlog size differs from the fixed cut");
  }
  completion.snapshot_cut.binlog_seed_path = seed;
  completion.snapshot_cut.objects = std::move(objects);
  completion.child_exited_cleanly = true;
  return true;
}

}  // namespace

void capture_startup_original_argv(int argc, char **argv) {
  if (g_adapter.original_captured) {
    g_adapter.original_valid = false;
    return;
  }
  g_adapter.original_captured = true;
  g_adapter.original_valid = argc > 0 && argv != nullptr;
  if (!g_adapter.original_valid) return;
  g_adapter.original_argv.reserve(static_cast<size_t>(argc));
  for (int index = 0; index < argc; ++index) {
    if (argv[index] == nullptr) {
      g_adapter.original_valid = false;
      g_adapter.original_argv.clear();
      return;
    }
    g_adapter.original_argv.emplace_back(argv[index]);
  }
  if (g_adapter.original_argv.front().empty()) {
    g_adapter.original_valid = false;
    g_adapter.original_argv.clear();
  }
}

void record_startup_declarative_remote_commit(bool enabled) {
  if (g_adapter.declarative_remote_commit_records !=
      std::numeric_limits<uint32_t>::max()) {
    ++g_adapter.declarative_remote_commit_records;
  }
  g_adapter.declarative_remote_commit_enabled = enabled;
}

void note_startup_adapter_option(StartupAdapterOption option) {
  const size_t index = option_index(option);
  if (index >= g_adapter.option_counts.size()) {
    g_adapter.original_valid = false;
    return;
  }
  if (g_adapter.option_counts[index] != std::numeric_limits<uint32_t>::max())
    ++g_adapter.option_counts[index];
}

bool startup_before_datadir(int remaining_argc,
                            char *const remaining_argv[],
                            std::string *error) {
  if (error != nullptr) error->clear();
  if (g_adapter.before_datadir_called)
    return fail(error, "remote startup adapter may run exactly once");
  g_adapter.before_datadir_called = true;
  if (g_adapter.declarative_remote_commit_records != 1)
    return fail(error,
                "declarative remote-commit mode was not recorded exactly "
                "once");
  if (!validate_remote_commit_option_source(
          g_adapter.declarative_remote_commit_enabled, enabled(), error)) {
    // This check precedes runtime initialization, epoch acquisition, and every
    // HEAD mutation. Persisted-only enablement cannot enter orchestration.
    return true;
  }
  const bool internal_option_seen =
      std::any_of(g_adapter.option_counts.begin(),
                  g_adapter.option_counts.end(),
                  [](uint32_t count) { return count != 0; });
  if (!enabled() && !internal_option_seen) return false;
  if (!validate_option_shape(error)) return true;
  if (!enabled()) {
    if (g_adapter.mode != StartupServerMode::NORMAL)
      return fail(error,
                  "internal startup mode requires remote commit to be enabled");
    return false;
  }
#ifndef WITH_SMARTENGINE
  // Runtime snapshots require the SmartEngine lease/live-set API. Reject the
  // declarative remote mode before any object-store or child orchestration when
  // the server was built without that engine, rather than failing asynchronously
  // inside the snapshot worker.
  return fail(error,
              "remote commit runtime snapshots require WITH_SMARTENGINE");
#endif
  if (!validate_startup_managed_path_options(remaining_argc, remaining_argv,
                                             error)) {
    return true;
  }
  if (opt_validate_config) return false;
  if (!canonical_target_root(&g_adapter.target_root, error) ||
      !configured_binlog_paths(&g_adapter.configured_binlog_index,
                               &g_adapter.configured_binlog_basename, error) ||
      !resolve_current_executable(g_adapter.original_argv.front(),
                                  &g_adapter.executable, error) ||
      !read_external_deployment(&g_adapter.external, error)) {
    return true;
  }

  if (g_adapter.mode != StartupServerMode::NORMAL) {
    g_adapter.defer_networking = true;
    g_adapter.networking_ready = false;
    g_adapter.force_skip_replica = true;
    if (opt_remote_startup_output_path != nullptr)
      g_adapter.output_path = fs::path(opt_remote_startup_output_path);
  }
  return false;
}

int startup_inherited_daemon_pipe_fd() {
  return g_adapter.mode == StartupServerMode::INSTALLED_REEXEC
             ? g_adapter.inherited_daemon_pipe_fd
             : -1;
}

bool startup_after_daemonization(int daemon_pipe_fd, bool daemonized,
                                 std::string *error) {
  if (error != nullptr) error->clear();
  if (!g_adapter.before_datadir_called || opt_help || opt_validate_config)
    return false;
  if (g_adapter.after_daemonization_called)
    return fail(error,
                "remote post-daemonization adapter may run exactly once");
  g_adapter.after_daemonization_called = true;
  const bool internal_option_seen =
      std::any_of(g_adapter.option_counts.begin(),
                  g_adapter.option_counts.end(),
                  [](uint32_t count) { return count != 0; });
  if (!enabled() && !internal_option_seen) return false;
  if (!enabled())
    return fail(error,
                "internal startup mode requires remote commit to be enabled");

  if (g_adapter.inherited_daemon_pipe_fd >= 0) {
    if (!daemonized || daemon_pipe_fd != g_adapter.inherited_daemon_pipe_fd)
      return fail(error, "installed re-exec lost its daemon status pipe");
  } else if (daemonized) {
    if (g_adapter.mode != StartupServerMode::NORMAL ||
        daemon_pipe_fd <= STDERR_FILENO)
      return fail(error, "unexpected daemonized remote startup child");
    g_adapter.inherited_daemon_pipe_fd = daemon_pipe_fd;
  } else if (daemon_pipe_fd != -1) {
    return fail(error, "non-daemon remote startup retained a status pipe");
  }

  if (!initialize_runtime(error)) return true;
  if (g_adapter.mode == StartupServerMode::NORMAL)
    return !run_parent_orchestration(error);

  if (g_adapter.mode == StartupServerMode::BOOTSTRAP_PREFLIGHT) {
    if (!opt_initialize || !may_run_startup_bootstrap_worker())
      return fail(error,
                  "bootstrap preflight is not an authorized initialize child");
    g_adapter.pending_output = PendingOutput::BOOTSTRAP_PREFLIGHT;
    return false;
  }
  if (g_adapter.mode == StartupServerMode::BOOTSTRAP_SNAPSHOT ||
      g_adapter.mode == StartupServerMode::TAKEOVER_RECOVERY) {
    if (!load_worker_request(error)) return true;
    return false;
  }
  if (g_adapter.mode == StartupServerMode::INSTALLED_REEXEC) {
    if (!load_restart_proof(error)) return true;
    return false;
  }
  return fail(error, "unsupported internal startup mode");
}

bool startup_defer_networking() { return g_adapter.defer_networking; }

bool startup_must_initialize_security_before_admission() {
  return g_adapter.mode == StartupServerMode::INSTALLED_REEXEC;
}

bool startup_force_skip_replica_start() {
  return g_adapter.force_skip_replica;
}

bool startup_networking_ready() { return g_adapter.networking_ready; }

StartupAdapterAction startup_after_repositories(std::string *error) {
  if (error != nullptr) error->clear();
  if (!enabled()) return StartupAdapterAction::CONTINUE;
  if (g_adapter.mode == StartupServerMode::BOOTSTRAP_SNAPSHOT ||
      g_adapter.mode == StartupServerMode::TAKEOVER_RECOVERY) {
    if (!build_base_worker_completion(error))
      return StartupAdapterAction::ERROR;
    return StartupAdapterAction::EXIT_CLEAN;
  }
  if (g_adapter.mode == StartupServerMode::INSTALLED_REEXEC) {
    const std::optional<StartupCoordinatorRoute> route =
        g_adapter.coordinator == nullptr ? std::nullopt
                                         : g_adapter.coordinator->route();
    if (g_adapter.coordinator == nullptr ||
        !g_adapter.expected_deployment.has_value() ||
        !route.has_value() ||
        !ensure_runtime_deployment(
            *g_adapter.expected_deployment,
            *route == StartupCoordinatorRoute::TAKEOVER, error) ||
        g_adapter.coordinator->published() == nullptr) {
      if (error != nullptr && error->empty())
        *error = "installed re-exec coordinator state is incomplete";
      return StartupAdapterAction::ERROR;
    }
    ServerRootVerificationRequest verification{
        *route, g_adapter.target_root, *g_adapter.expected_deployment, true,
        nullptr, g_adapter.coordinator->published()};
    StartupRootEvidence evidence;
    const StartupStepResult collected =
        collect_server_root_evidence(verification, &evidence);
    if (!collected.ready()) {
      if (error != nullptr)
        *error = "installed root evidence failed: " + collected.detail;
      return StartupAdapterAction::ERROR;
    }
    const StartupCoordinatorResult activated =
        g_adapter.coordinator->finish_post_engine_verification(evidence);
    if (!activated.ready_for_admission()) {
      if (error != nullptr)
        *error = activated.detail.empty()
                     ? "installed root did not reach admission-ready state"
                     : activated.detail;
      return StartupAdapterAction::ERROR;
    }
    fs::path configured_root;
    if (!canonical_target_root(&configured_root, error))
      return StartupAdapterAction::ERROR;
    const StartupCoordinatorProof &verified = *activated.proof;
    const StartupRootEvidence &root = verified.installed_root;
    InstalledRootProof server_proof;
    server_proof.head_body = verified.head.object.body;
    server_proof.head_etag = verified.head.object.etag;
    server_proof.recovered_file = root.recovered_cursor.file;
    server_proof.recovered_pos = root.recovered_cursor.pos;
    server_proof.canonical_gtid = root.recovered_gtid.canonical;
    server_proof.gtid_sha256 = root.recovered_gtid.sha256;
    server_proof.marker_matches = root.marker_matches;
    server_proof.root_identity_matches =
        same_root(configured_root, g_adapter.target_root);
    server_proof.snapshot_matches = root.snapshot_matches;
    server_proof.server_uuid_matches = root.server_uuid_matches;
    server_proof.configuration_matches = root.configuration_matches;
    server_proof.gtid_matches = root.gtid_matches;
    server_proof.dd_matches = root.dd_matches;
    server_proof.repository_empty = root.repository_empty;
    server_proof.extent_live_set_matches = root.extent_live_set_matches;
    server_proof.internal_prepared_empty = root.internal_prepared_empty;
    server_proof.external_xa_empty = root.external_xa_empty;
    if (verify_installed_root_post_engine(server_proof)) {
      if (error != nullptr)
        *error = "installed root server verification failed: " + startup_error();
      return StartupAdapterAction::ERROR;
    }
    if (!remove_startup_control_directory(g_adapter.control_directory, error))
      return StartupAdapterAction::ERROR;
    const fs::path snapshot_service_root =
        g_adapter.control_directory.parent_path() /
        (g_adapter.control_directory.filename().string() +
         ".runtime-snapshots");
    g_adapter.control_directory.clear();
    if (start_runtime_snapshot_service(snapshot_service_root, error) ||
        !runtime_snapshot_service_ready()) {
      stop_runtime_snapshot_service();
      if (error != nullptr && error->empty())
        *error = "runtime snapshot service did not remain ready";
      return StartupAdapterAction::ERROR;
    }
    open_commit_admission();
    g_adapter.networking_ready = true;
    return StartupAdapterAction::CONTINUE;
  }
  return StartupAdapterAction::CONTINUE;
}

bool startup_finish_bootstrap_preflight(std::string *error) {
  if (error != nullptr) error->clear();
  if (g_adapter.mode != StartupServerMode::BOOTSTRAP_PREFLIGHT ||
      g_adapter.pending_output != PendingOutput::BOOTSTRAP_PREFLIGHT)
    return false;
  if (server_uuid_ptr == nullptr || !lowercase_uuid(server_uuid_ptr))
    return fail(error, "bootstrap did not initialize a canonical server UUID");
  const StartupDeploymentIdentity deployment = make_deployment(
      g_adapter.runtime.stream, g_adapter.external, server_uuid_ptr);
  if (!ensure_runtime_deployment(deployment, false, error)) return true;
  ServerRootVerificationRequest request{StartupCoordinatorRoute::BOOTSTRAP,
                                        g_adapter.target_root, deployment,
                                        false, nullptr, nullptr};
  const StartupStepResult compared = verify_initialized_empty_root(
      request, g_adapter.configured_binlog_basename,
      g_adapter.configured_binlog_index);
  if (!compared.ready())
    return fail(error, "bootstrap EMPTY_SOURCE evidence failed: " +
                           compared.detail);
  StartupBootstrapPreflight preflight;
  preflight.root = g_adapter.target_root;
  preflight.request_nonce = random_hex(32);
  preflight.initialized_deployment = deployment;
  preflight.dd_initialized = true;
  preflight.repository_empty = true;
  preflight.internal_prepared_empty = true;
  preflight.external_xa_empty = true;
  preflight.empty_source_scan_stable = true;
  preflight.old_tc_authority_empty = true;
  preflight.user_state_empty = true;
  preflight.legacy_live_extents_empty = true;
  if (!lowercase_sha256(preflight.request_nonce) ||
      !finalize_startup_bootstrap_preflight(&preflight, error)) {
    if (error != nullptr && !error->empty()) return true;
    return fail(error, "cannot bind bootstrap preflight request identity");
  }
  g_adapter.pending_preflight = std::move(preflight);
  return false;
}

bool startup_prepare_clean_exit(std::string *error) {
  if (error != nullptr) error->clear();
  if (g_adapter.clean_exit_prepared) return false;
  if (g_adapter.retained_smartengine.active()) {
    const StartupStepResult released = g_adapter.retained_smartengine.release();
    if (!released.ready()) {
      std::string detail = "cannot release retained SmartEngine snapshot";
      if (!released.detail.empty()) detail.append(": ").append(released.detail);
      fail_stop(detail.c_str());
    }
  }
  g_adapter.clean_exit_prepared = true;
  return false;
}

#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
void startup_arm_release_failure_for_test() {
  g_adapter.retained_smartengine.arm_release_failure_for_test();
}
#endif

bool startup_finalize_clean_exit(std::string *error) {
  if (error != nullptr) error->clear();
  if (g_adapter.clean_exit_finalized) return false;
  if (g_adapter.pending_output == PendingOutput::NONE) {
    g_adapter.clean_exit_finalized = true;
    return false;
  }
  if (!g_adapter.clean_exit_prepared)
    return fail(error,
                "startup output publication preceded live-engine cleanup");
  if (g_adapter.output_path.empty() || !g_adapter.output_path.is_absolute() ||
      g_adapter.output_path.lexically_normal() != g_adapter.output_path)
    return fail(error, "startup child output path is invalid");
  std::string body;
  uint64_t maximum = 0;
  if (g_adapter.pending_output == PendingOutput::BOOTSTRAP_PREFLIGHT) {
    if (!g_adapter.pending_preflight.has_value())
      return fail(error, "bootstrap preflight evidence is not pending");
    g_adapter.pending_preflight->child_exited_cleanly = true;
    if (!serialize_startup_bootstrap_preflight(*g_adapter.pending_preflight,
                                               &body, error)) {
      return true;
    }
    maximum = kStartupBootstrapPreflightMaxBytes;
  } else {
    if (!finalize_worker_files(error)) return true;
    if (!serialize_startup_worker_completion(*g_adapter.pending_completion,
                                             &body, error)) {
      return true;
    }
    maximum = kStartupWorkerEvidenceMaxBytes;
  }
  StartupProofReference reference;
  if (!publish_startup_output_proof(g_adapter.output_path, body, &reference,
                                    error, maximum)) {
    return true;
  }
  g_adapter.clean_exit_finalized = true;
  return false;
}

}  // namespace wesql::remote_commit
