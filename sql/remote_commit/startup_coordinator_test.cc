/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/startup_coordinator.h"
#include "sql/remote_commit/startup_adapter.h"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "my_dbug.h"

namespace rc = wesql::remote_commit;
namespace fs = std::filesystem;

#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
CODE_STATE **my_thread_var_dbug() {
  static thread_local CODE_STATE *state = nullptr;
  return &state;
}

[[noreturn]] void my_abort() { std::abort(); }

namespace wesql::remote_commit {
[[noreturn]] void fail_stop(const char *) { std::abort(); }
}  // namespace wesql::remote_commit
#endif

namespace {

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "remote commit startup coordinator test failed: " << message
              << '\n';
    std::exit(EXIT_FAILURE);
  }
}

std::string hash(std::string_view value) {
  std::string digest;
  std::string error;
  expect(rc::sha256_hex(value, &digest, &error), error.c_str());
  return digest;
}

rc::GtidSetDigest gtid(std::string_view value) {
  rc::GtidSetDigest digest;
  std::string error;
  expect(rc::gtid_digest(value, &digest, &error), error.c_str());
  return digest;
}

rc::GtidSetDigest empty_gtid() { return gtid(""); }

struct RootCleanup {
  explicit RootCleanup(fs::path value) : root(std::move(value)) {}
  ~RootCleanup() {
    std::error_code ignored;
    fs::remove_all(root, ignored);
  }
  fs::path root;
};

fs::path unique_parent(std::string_view suffix) {
  static uint64_t sequence = 0;
  fs::path root = fs::temp_directory_path() /
                  ("wesql-startup-coordinator-v2-" + std::string(suffix) +
                   "-" + std::to_string(static_cast<uint64_t>(::getpid())) +
                   "-" + std::to_string(++sequence));
  std::error_code error;
  fs::remove_all(root, error);
  error.clear();
  expect(fs::create_directory(root, error) && !error,
         "cannot create test parent");
  return root;
}

const rc::StreamIdentity &stream() {
  static const rc::StreamIdentity value = [] {
    rc::StreamIdentity built;
    std::string error;
    expect(rc::build_stream_identity("repo", "branch", "repo/branch", &built,
                                     &error),
           error.c_str());
    return built;
  }();
  return value;
}

rc::StartupDeploymentIdentity deployment() {
  rc::StartupDeploymentIdentity value;
  value.stream_id = stream().stream_id;
  value.server_uuid = "01234567-89ab-cdef-0123-456789abcdef";
  value.fingerprints.startup_config_sha256 = std::string(64, '1');
  value.fingerprints.server_build = "wesql-9.7.2-test";
  value.fingerprints.plugin_component_set_sha256 = std::string(64, '2');
  value.fingerprints.keyring_config_sha256 = std::string(64, '3');
  value.fingerprints.tls_config_sha256 = std::string(64, '4');
  value.binary_fingerprint = std::string(64, '5');
  return value;
}

rc::ExactWriterEpoch epoch(uint64_t number, std::string writer_id,
                           uint64_t previous) {
  rc::ExactWriterEpoch value;
  value.value = {number, std::move(writer_id), previous};
  std::string error;
  expect(rc::serialize_writer_epoch(stream(), value.value, &value.object.body,
                                    &error),
         error.c_str());
  value.object.etag = "epoch-etag-" + std::to_string(number);
  return value;
}

rc::SegmentTip snapshot_root(std::string id, const rc::Cursor &cursor) {
  rc::SegmentTip tip;
  tip.kind = rc::SegmentTipKind::SNAPSHOT_ROOT;
  tip.snapshot_id = std::move(id);
  tip.cursor = cursor;
  return tip;
}

struct ExactHead {
  rc::Head value;
  rc::PublishedBytes object;
};

ExactHead exact_head(uint64_t generation, const rc::Writer &writer,
                     const rc::Cursor &cursor, std::string snapshot_id,
                     std::optional<rc::HeadParent> parent) {
  ExactHead exact;
  exact.value.generation = generation;
  exact.value.writer = writer;
  exact.value.parent = std::move(parent);
  const std::string snapshot_sha = hash("snapshot-" + snapshot_id);
  exact.value.snapshot.id = std::move(snapshot_id);
  exact.value.snapshot.manifest_size = 200;
  exact.value.snapshot.manifest_sha256 = snapshot_sha;
  exact.value.snapshot.cursor = cursor;
  std::string error;
  expect(rc::snapshot_manifest_key(
             stream(), exact.value.snapshot.id, snapshot_sha,
             &exact.value.snapshot.manifest_key, &error),
         error.c_str());
  const std::string manifest_sha =
      hash("transition-" + std::to_string(generation) + writer.id);
  exact.value.manifest.size = 100;
  exact.value.manifest.sha256 = manifest_sha;
  expect(rc::transition_manifest_key(stream(), writer, generation,
                                     manifest_sha,
                                     &exact.value.manifest.key, &error),
         error.c_str());
  exact.value.recovery_window = {1, 100, 0};
  exact.value.segment_tip = snapshot_root(exact.value.snapshot.id, cursor);
  exact.value.base_cursor = cursor;
  exact.value.durable_cursor = cursor;
  expect(rc::serialize_head(stream(), exact.value, &exact.object.body, &error),
         error.c_str());
  exact.object.etag = "head-etag-" + std::to_string(generation);
  return exact;
}

rc::RecoveryPlan plan(const ExactHead &head, const rc::ExactWriterEpoch &epoch,
                      rc::ManifestKind kind,
                      std::vector<rc::PinnedSmartengineExtent> extents = {}) {
  rc::RecoveryPlan value;
  value.head_object = head.object;
  value.head = head.value;
  value.epoch_object = epoch.object;
  value.epoch = epoch.value;
  value.snapshot.snapshot_id = head.value.snapshot.id;
  value.snapshot.writer = head.value.writer;
  value.snapshot.cursor = head.value.durable_cursor;
  value.snapshot.server_identity.server_uuid = deployment().server_uuid;
  value.snapshot.deployment_fingerprints = deployment().fingerprints;
  value.snapshot.gtid_executed = empty_gtid();
  value.snapshot.binlog_seed.file = head.value.durable_cursor.file;
  value.snapshot.binlog_seed.cursor = head.value.durable_cursor;
  value.snapshot.binlog_seed.size = head.value.durable_cursor.pos;
  value.snapshot.binlog_seed.key = "seed-" + head.value.snapshot.id;
  value.snapshot.binlog_seed.sha256 = hash("seed");
  value.snapshot_object = {"snapshot-body", "snapshot-etag"};
  for (size_t index = 0; index < extents.size(); ++index) {
    const rc::PinnedSmartengineExtent &source = extents[index];
    value.snapshot.smartengine_extents.push_back(
        {index, source.writer_epoch, source.allocation_seq,
         source.database_name_hex, source.index_id, source.object_id,
         source.key, source.size, source.sha256});
  }
  rc::VerifiedManifest manifest;
  manifest.object = head.value.manifest;
  manifest.value.kind = kind;
  manifest.value.generation = head.value.generation;
  manifest.value.writer = head.value.writer;
  manifest.value.snapshot = head.value.snapshot;
  manifest.value.base_cursor = head.value.base_cursor;
  manifest.value.durable_cursor = head.value.durable_cursor;
  manifest.value.recovery_window = head.value.recovery_window;
  manifest.value.segment_tip = head.value.segment_tip;
  manifest.body_size = head.value.manifest.size;
  value.manifests.push_back(std::move(manifest));
  return value;
}

rc::PinnedSmartengineExtent extent(uint64_t writer_epoch,
                                   std::string allocation) {
  rc::PinnedSmartengineExtent value;
  value.writer_epoch = writer_epoch;
  value.allocation_seq = std::move(allocation);
  value.database_name_hex = "74657374";
  value.index_id = "42";
  value.object_id = "123";
  value.key = stream().extent_prefix + "/extent-" + value.allocation_seq;
  value.size = 17;
  value.sha256 = hash(value.key);
  return value;
}

rc::StartupRootEvidence evidence(
    const rc::Cursor &cursor, bool installed,
    const std::vector<rc::PinnedSmartengineExtent> &extents = {}) {
  rc::StartupRootEvidence value;
  value.recovered_cursor = cursor;
  value.recovered_gtid = empty_gtid();
  value.marker_matches = installed;
  value.snapshot_matches = installed;
  value.server_uuid_matches = true;
  value.configuration_matches = true;
  value.gtid_matches = true;
  value.dd_matches = true;
  value.repository_empty = true;
  value.extent_live_set_matches = true;
  std::string error;
  expect(rc::startup_extent_set_digest(
             extents, &value.exported_extent_count,
             &value.exported_extent_set_sha256, &error),
         error.c_str());
  value.internal_prepared_empty = true;
  value.external_xa_empty = true;
  return value;
}

rc::StartupBootstrapPreflight preflight(
    const fs::path &root,
    const rc::StartupDeploymentIdentity &identity = deployment()) {
  rc::StartupBootstrapPreflight value;
  value.root = root;
  value.request_nonce = hash("bootstrap-preflight-nonce");
  value.request_sha256 = hash("bootstrap-preflight-request");
  value.child_exited_cleanly = true;
  value.initialized_deployment = identity;
  value.dd_initialized = true;
  value.repository_empty = true;
  value.internal_prepared_empty = true;
  value.external_xa_empty = true;
  value.empty_source_scan_stable = true;
  value.old_tc_authority_empty = true;
  value.user_state_empty = true;
  value.legacy_live_extents_empty = true;
  std::string error;
  expect(rc::finalize_startup_bootstrap_preflight(&value, &error),
         error.c_str());
  return value;
}

rc::StartupCoordinatorOptions options(
    const fs::path &parent,
    std::optional<rc::StartupBootstrapPreflight> bootstrap = std::nullopt) {
  rc::StartupCoordinatorOptions value;
  value.target_root = parent / "data";
  value.materialize.temp_root = parent / "restore.tmp";
  value.deployment = deployment();
  value.worker_nonce = hash("worker-nonce-" + parent.string());
  value.bootstrap_preflight = std::move(bootstrap);
  return value;
}

rc::StartupActivationOptions activation_options(const fs::path &parent) {
  return {parent / "data", deployment()};
}

struct ScriptedRead {
  rc::RecoveryReadOutcome outcome{rc::RecoveryReadOutcome::CORRUPT};
  rc::RecoveryPlan plan;
  std::string detail;
};

class FakeStorage final : public rc::StartupCoordinatorStorage {
 public:
  explicit FakeStorage(std::vector<std::string> *events) : events_(events) {}

  rc::RecoveryReadResult read(rc::RecoveryPlan *result) override {
    events_->push_back("read");
    expect(read_index < reads.size(), "unexpected recovery read");
    const ScriptedRead &script = reads[read_index++];
    if (script.outcome == rc::RecoveryReadOutcome::READY)
      *result = script.plan;
    return {script.outcome, script.detail};
  }

  rc::TargetClassification classify(
      const fs::path &, const rc::LocalInstallMarker &,
      bool allow_managed_replace) override {
    events_->push_back("classify");
    classified_allow_managed = allow_managed_replace;
    return classification;
  }

  rc::MaterializeResult materialize(
      const rc::RecoveryPlan &, const rc::MaterializeOptions &materialize,
      rc::MaterializedRoot *root) override {
    events_->push_back("materialize");
    if (materialize_result.ready()) {
      std::error_code error;
      expect(fs::create_directory(materialize.temp_root, error) && !error,
             "fake materializer could not create root");
      root->binlog_files.push_back(materialize.temp_root / "binlog.000007");
      root->binlog_index = materialize.temp_root / "binlog.index";
      root->verified_payload_bytes = 91;
    }
    return materialize_result;
  }

  rc::InstallResult install(const fs::path &temporary_root,
                            const fs::path &target,
                            const rc::LocalInstallMarker &expected_marker,
                            bool allow_managed_replace) override {
    events_->push_back("install");
    installed_allow_managed = allow_managed_replace;
    installed_marker = expected_marker;
    if (install_result.installed()) {
      std::string marker_body;
      std::string marker_error;
      expect(rc::serialize_local_install_marker(expected_marker, &marker_body,
                                                &marker_error),
             marker_error.c_str());
      std::ofstream marker(temporary_root / rc::kLocalInstallMarkerName,
                           std::ios::binary | std::ios::trunc);
      marker.write(marker_body.data(),
                   static_cast<std::streamsize>(marker_body.size()));
      marker.close();
      expect(marker.good(), "fake installer could not write marker");
      std::error_code error;
      fs::rename(temporary_root, target, error);
      expect(!error, "fake installer could not move fresh root");
    }
    return install_result;
  }

  rc::StartupStepResult snapshot_stopped_root(
      const fs::path &root, rc::StartupRootSnapshot *snapshot) override {
    events_->push_back("snapshot");
    std::string error;
    if (!rc::snapshot_stable_startup_root(root, snapshot, &error))
      return {rc::StartupStepOutcome::LOCAL_IO_ERROR, std::move(error)};
    return {rc::StartupStepOutcome::READY, {}};
  }

  std::vector<ScriptedRead> reads;
  size_t read_index{0};
  rc::TargetClassification classification{rc::TargetClass::EMPTY_TARGET,
                                           std::nullopt, {}};
  rc::MaterializeResult materialize_result{rc::MaterializeOutcome::READY, {}};
  rc::InstallResult install_result{rc::InstallOutcome::INSTALLED,
                                   rc::TargetClass::EMPTY_TARGET,
                                   std::nullopt, {}};
  bool classified_allow_managed{false};
  bool installed_allow_managed{false};
  rc::LocalInstallMarker installed_marker;

 private:
  std::vector<std::string> *events_;
};

class FakeOperations final : public rc::StartupCoordinatorOperations {
 public:
  explicit FakeOperations(std::vector<std::string> *events) : events_(events) {}

  rc::StartupStepResult startup_probe(rc::StartupHookProbe *result) override {
    events_->push_back("probe");
    *result = probe;
    return probe_result;
  }

  rc::StartupStepResult acquire_epoch(rc::ExactWriterEpoch *result) override {
    events_->push_back("acquire");
    *result = acquired;
    return acquire_result;
  }

  rc::StartupStepResult publish_snapshot(
      const rc::StartupSnapshotPublishRequest &request,
      rc::PublishedStartupHead *result) override {
    events_->push_back("publish");
    published_cut_snapshot_id = request.snapshot_cut.snapshot_id;
    published_extent_digest = request.root_evidence.exported_extent_set_sha256;
    if (!mutate_root_on_publish.empty()) {
      std::ofstream mutation(mutate_root_on_publish / "publication-mutation",
                             std::ios::binary | std::ios::trunc);
      mutation << "changed";
      mutation.close();
      expect(mutation.good(), "cannot mutate fake publication root");
    }
    *result = published;
    return publish_result;
  }

  rc::StartupStepResult persist_restart_proof(
      const rc::StartupRestartProof &proof,
      rc::StartupRestartProofReference *reference) override {
    events_->push_back("persist");
    persisted_proof = proof;
    std::string body;
    std::string error;
    expect(rc::serialize_startup_restart_proof(stream(), proof, &body, &error),
           error.c_str());
    reference->path = proof_path;
    reference->size = body.size();
    reference->sha256 = hash(body);
    return persist_result;
  }

  rc::StartupStepResult activate_installed_root(
      const rc::StartupActivationRequest &request) override {
    events_->push_back("activate");
    activated_generation = request.published.head.generation;
    activated_cursor = request.expected_root.recovered_cursor;
    return activate_result;
  }

  rc::StartupHookProbe probe;
  rc::ExactWriterEpoch acquired;
  rc::PublishedStartupHead published;
  fs::path proof_path;
  std::optional<rc::StartupRestartProof> persisted_proof;
  rc::StartupStepResult probe_result{rc::StartupStepOutcome::READY, {}};
  rc::StartupStepResult acquire_result{rc::StartupStepOutcome::READY, {}};
  rc::StartupStepResult publish_result{rc::StartupStepOutcome::READY, {}};
  rc::StartupStepResult persist_result{rc::StartupStepOutcome::READY, {}};
  rc::StartupStepResult activate_result{rc::StartupStepOutcome::READY, {}};
  std::string published_cut_snapshot_id;
  std::string published_extent_digest;
  fs::path mutate_root_on_publish;
  uint64_t activated_generation{0};
  rc::Cursor activated_cursor;

 private:
  std::vector<std::string> *events_;
};

void expect_events(const std::vector<std::string> &actual,
                   const std::vector<std::string> &expected) {
  if (actual == expected) return;
  std::cerr << "event order differs\nactual:";
  for (const std::string &event : actual) std::cerr << ' ' << event;
  std::cerr << "\nexpected:";
  for (const std::string &event : expected) std::cerr << ' ' << event;
  std::cerr << '\n';
  std::exit(EXIT_FAILURE);
}

struct TakeoverFixture {
  rc::Cursor cursor{"binlog.000007", 700};
  rc::ExactWriterEpoch old_epoch{
      epoch(4, "22222222222222222222222222222222", 3)};
  rc::ExactWriterEpoch acquired{
      epoch(5, "11111111111111111111111111111111", 4)};
  ExactHead initial_head{exact_head(
      7, {old_epoch.value.writer_id, old_epoch.value.epoch}, cursor,
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      rc::HeadParent{6, "head-etag-6", hash("head-6")})};
  std::vector<rc::PinnedSmartengineExtent> old_extents{extent(4, "1")};
  std::vector<rc::PinnedSmartengineExtent> recovered_extents{extent(5, "9")};
  rc::RecoveryPlan initial{
      plan(initial_head, old_epoch, rc::ManifestKind::SNAPSHOT, old_extents)};
  rc::RecoveryPlan candidate{initial};
  ExactHead published_head;
  rc::RecoveryPlan published;

  TakeoverFixture()
      : published_head(exact_head(
            8, {acquired.value.writer_id, acquired.value.epoch}, cursor,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            rc::HeadParent{initial.head.generation, initial.head_object.etag,
                           hash(initial.head_object.body)})),
        published(plan(published_head, acquired, rc::ManifestKind::SNAPSHOT,
                       recovered_extents)) {
    candidate.epoch_object = acquired.object;
    candidate.epoch = acquired.value;
    published.snapshot.log_anchor.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
    published.snapshot.log_anchor.generation = candidate.head.generation;
    published.snapshot.log_anchor.manifest = candidate.head.manifest;
    published.snapshot.log_anchor.cursor = cursor;
    published.manifests.front().value.head_parent =
        published.head.parent;
    published.manifests.front().value.segment_tip =
        published.head.segment_tip;
  }
};

struct BootstrapFixture {
  rc::Cursor cursor{"binlog.000001", 4};
  rc::ExactWriterEpoch acquired{
      epoch(5, "11111111111111111111111111111111", 4)};
  std::vector<rc::PinnedSmartengineExtent> extents;
  ExactHead published_head{exact_head(
      1, {acquired.value.writer_id, acquired.value.epoch}, cursor,
      "cccccccccccccccccccccccccccccccc", std::nullopt)};
  rc::RecoveryPlan published{
      plan(published_head, acquired, rc::ManifestKind::BOOTSTRAP, extents)};

  BootstrapFixture() {
    published.snapshot.log_anchor.kind = rc::LogAnchorKind::EMPTY_BASE;
    published.snapshot.log_anchor.cursor = cursor;
    published.manifests.front().value.segment_tip =
        published.head.segment_tip;
  }
};

void configure_takeover(FakeStorage *storage, FakeOperations *operations,
                        const TakeoverFixture &fixture,
                        const fs::path &parent) {
  storage->reads = {
      {rc::RecoveryReadOutcome::READY, fixture.initial, {}},
      {rc::RecoveryReadOutcome::READY, fixture.candidate, {}},
      {rc::RecoveryReadOutcome::READY, fixture.published, {}},
      {rc::RecoveryReadOutcome::READY, fixture.published, {}},
  };
  operations->probe.route = rc::StartupCoordinatorRoute::TAKEOVER;
  operations->probe.head_object = fixture.initial.head_object;
  operations->probe.head_generation = fixture.initial.head.generation;
  operations->probe.durable_cursor = fixture.initial.head.durable_cursor;
  operations->acquired = fixture.acquired;
  operations->published = {fixture.published.head_object,
                           fixture.published.head};
  operations->proof_path = parent / "restart.proof";
}

void configure_bootstrap(FakeStorage *storage, FakeOperations *operations,
                         const BootstrapFixture &fixture,
                         const fs::path &parent) {
  storage->reads = {
      {rc::RecoveryReadOutcome::EMPTY, {}, {}},
      {rc::RecoveryReadOutcome::EMPTY, {}, {}},
      {rc::RecoveryReadOutcome::READY, fixture.published, {}},
      {rc::RecoveryReadOutcome::READY, fixture.published, {}},
  };
  operations->probe.route = rc::StartupCoordinatorRoute::BOOTSTRAP;
  operations->acquired = fixture.acquired;
  operations->published = {fixture.published.head_object,
                           fixture.published.head};
  operations->proof_path = parent / "restart.proof";
}

rc::FixedSnapshotCut takeover_cut(
    const TakeoverFixture &fixture, const fs::path &root,
    std::vector<rc::PinnedSmartengineExtent> extents = {}) {
  if (extents.empty()) extents = fixture.recovered_extents;
  rc::FixedSnapshotCut cut;
  cut.snapshot_id = fixture.published.snapshot.snapshot_id;
  cut.writer = {fixture.acquired.value.writer_id,
                fixture.acquired.value.epoch};
  cut.proof.source = rc::FixedCutSource::RECOVERED_TAKEOVER;
  cut.proof.public_cursor = fixture.cursor;
  cut.proof.public_gtid = empty_gtid();
  cut.proof.image_cursor = fixture.cursor;
  cut.proof.image_gtid = empty_gtid();
  cut.proof.source_head_generation = fixture.candidate.head.generation;
  cut.proof.source_head_body_sha256 = hash(fixture.candidate.head_object.body);
  cut.log_anchor.kind = rc::LogAnchorKind::MANIFEST_BOUNDARY;
  cut.log_anchor.generation = fixture.candidate.head.generation;
  cut.log_anchor.manifest = fixture.candidate.head.manifest;
  cut.log_anchor.cursor = fixture.cursor;
  cut.server_identity.server_uuid = deployment().server_uuid;
  cut.deployment_fingerprints = deployment().fingerprints;
  cut.binlog_seed_path = root / fixture.cursor.file;
  cut.smartengine_extents = std::move(extents);
  return cut;
}

rc::FixedSnapshotCut bootstrap_cut(const BootstrapFixture &fixture,
                                   const fs::path &root) {
  rc::FixedSnapshotCut cut;
  cut.snapshot_id = fixture.published.snapshot.snapshot_id;
  cut.writer = {fixture.acquired.value.writer_id,
                fixture.acquired.value.epoch};
  cut.proof.source = rc::FixedCutSource::EMPTY_SOURCE;
  cut.proof.public_cursor = fixture.cursor;
  cut.proof.public_gtid = empty_gtid();
  cut.proof.image_cursor = fixture.cursor;
  cut.proof.image_gtid = empty_gtid();
  cut.proof.empty_source_scan_stable = true;
  cut.proof.internal_prepared_empty = true;
  cut.proof.external_xa_empty = true;
  cut.proof.old_tc_authority_empty = true;
  cut.proof.user_state_empty = true;
  cut.proof.legacy_live_extents_empty = true;
  cut.log_anchor.kind = rc::LogAnchorKind::EMPTY_BASE;
  cut.log_anchor.cursor = fixture.cursor;
  cut.server_identity.server_uuid = deployment().server_uuid;
  cut.deployment_fingerprints = deployment().fingerprints;
  cut.binlog_seed_path = root / fixture.cursor.file;
  return cut;
}

rc::StartupWorkerCompletion completion(
    const rc::StartupWorkerRequest &request, rc::StartupRootEvidence root,
    rc::FixedSnapshotCut cut) {
  std::ofstream seed(cut.binlog_seed_path,
                     std::ios::binary | std::ios::trunc);
  seed << "worker-seed";
  seed.close();
  expect(seed.good(), "cannot create fake worker seed");
  return {true, request.request_nonce, request.request_sha256, std::move(root),
          std::move(cut)};
}

void configure_reexec(FakeStorage *storage, FakeOperations *operations,
                      const rc::RecoveryPlan &published,
                      const rc::StartupRestartProof &proof) {
  storage->reads = {
      {rc::RecoveryReadOutcome::READY, published, {}},
      {rc::RecoveryReadOutcome::READY, published, {}},
  };
  storage->classification = {rc::TargetClass::MANAGED_REPLACE, proof.marker,
                             {}};
  operations->proof_path = proof.marker.stream_id;
}

void test_takeover_parent_worker_reexec_lifecycle() {
  const fs::path parent = unique_parent("takeover-lifecycle");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);

  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  expect(prepared.worker_required(), prepared.detail.c_str());
  expect(prepared.worker_request->candidate.has_value() &&
             prepared.worker_request->materialized.has_value() &&
             prepared.worker_request->candidate->replay_exclusive_start ==
                 fixture.candidate.snapshot.cursor &&
             prepared.worker_request->candidate->replay_inclusive_end ==
                 fixture.candidate.head.durable_cursor,
         "takeover worker request lost its exact replay boundary");
  expect_events(events, {"probe", "read", "classify", "acquire", "read",
                         "materialize"});
  expect(fs::exists(parent / "restore.tmp") && !fs::exists(parent / "data"),
         "takeover preparation crossed the worker boundary");

  rc::StartupWorkerCompletion worker = completion(
      *prepared.worker_request,
      evidence(fixture.cursor, false, fixture.recovered_extents),
      takeover_cut(fixture, parent / "restore.tmp"));
  rc::StartupCoordinatorResult finished = coordinator.finish_worker(worker);
  expect(finished.restart_required(), finished.detail.c_str());
  expect(coordinator.state() == rc::StartupCoordinatorState::RESTART_REQUIRED &&
             operations.persisted_proof.has_value() &&
             fs::exists(parent / "data") &&
             !fs::exists(parent / "restore.tmp"),
         "takeover parent did not stop after install with a restart proof");
  expect_events(events,
                {"probe", "read", "classify", "acquire", "read",
                 "materialize", "snapshot", "publish", "snapshot", "read",
                 "install", "snapshot", "read", "persist"});
  expect(operations.published_extent_digest ==
             worker.root_evidence.exported_extent_set_sha256 &&
             fixture.candidate.snapshot.smartengine_extents.size() == 1 &&
             fixture.candidate.snapshot.smartengine_extents.front().key !=
                 worker.snapshot_cut.smartengine_extents.front().key,
         "takeover compared the export with the base snapshot live set");

  std::vector<std::string> reexec_events;
  FakeStorage reexec_storage(&reexec_events);
  FakeOperations reexec_operations(&reexec_events);
  configure_reexec(&reexec_storage, &reexec_operations, fixture.published,
                   *operations.persisted_proof);
  rc::StartupCoordinator reexec(stream(), &reexec_storage,
                                &reexec_operations);
  rc::StartupCoordinatorResult adopted = reexec.adopt_restart_proof(
      activation_options(parent), *operations.persisted_proof);
  expect(adopted.post_engine_verification_required(), adopted.detail.c_str());
  expect_events(reexec_events,
                {"read", "classify", "snapshot", "activate"});
  expect(reexec_operations.activated_generation ==
             fixture.published.head.generation,
         "re-exec activation did not receive exact published HEAD");

  rc::StartupCoordinatorResult admitted =
      reexec.finish_post_engine_verification(
          evidence(fixture.cursor, true, fixture.recovered_extents));
  expect(admitted.ready_for_admission(), admitted.detail.c_str());
  expect(admitted.proof->head.object.body ==
             fixture.published.head_object.body &&
             admitted.proof->epoch.value == fixture.acquired.value &&
             admitted.proof->installed_root.marker_matches,
         "re-exec admission proof omitted exact installed state");
  expect_events(reexec_events,
                {"read", "classify", "snapshot", "activate", "read"});
}

void test_bootstrap_preflight_worker_reexec_lifecycle() {
  const fs::path parent = unique_parent("bootstrap-lifecycle");
  RootCleanup cleanup(parent);
  BootstrapFixture fixture;
  expect(fs::create_directory(parent / "restore.tmp"),
         "cannot create bootstrap preflight root");
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_bootstrap(&storage, &operations, fixture, parent);
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorOptions startup =
      options(parent, preflight(parent / "restore.tmp"));

  rc::StartupCoordinatorResult prepared = coordinator.prepare_worker(startup);
  expect(prepared.worker_required() &&
             !prepared.worker_request->candidate.has_value() &&
             !prepared.worker_request->materialized.has_value(),
         prepared.detail.c_str());
  expect_events(events, {"probe", "read", "classify", "acquire", "read"});
  rc::StartupWorkerCompletion worker = completion(
      *prepared.worker_request, evidence(fixture.cursor, false),
      bootstrap_cut(fixture, parent / "restore.tmp"));
  rc::StartupCoordinatorResult finished = coordinator.finish_worker(worker);
  expect(finished.restart_required(), finished.detail.c_str());
  expect(!storage.classified_allow_managed &&
             !storage.installed_allow_managed,
         "bootstrap allowed managed target replacement");
  expect_events(events,
                {"probe", "read", "classify", "acquire", "read", "snapshot",
                 "publish", "snapshot", "read", "install", "snapshot",
                 "read", "persist"});

  std::vector<std::string> reexec_events;
  FakeStorage reexec_storage(&reexec_events);
  FakeOperations reexec_operations(&reexec_events);
  configure_reexec(&reexec_storage, &reexec_operations, fixture.published,
                   *operations.persisted_proof);
  rc::StartupCoordinator reexec(stream(), &reexec_storage,
                                &reexec_operations);
  rc::StartupCoordinatorResult adopted = reexec.adopt_restart_proof(
      activation_options(parent), *operations.persisted_proof);
  expect(adopted.post_engine_verification_required(), adopted.detail.c_str());
  rc::StartupCoordinatorResult admitted =
      reexec.finish_post_engine_verification(evidence(fixture.cursor, true));
  expect(admitted.ready_for_admission(), admitted.detail.c_str());
  expect_events(reexec_events,
                {"read", "classify", "snapshot", "activate", "read"});
}

void test_typed_transport_round_trips_and_rejects_tampering() {
  const fs::path parent = unique_parent("codec");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  expect(fs::create_directory(parent / "bootstrap-root"),
         "cannot create codec preflight root");
  const rc::StartupBootstrapPreflight source_preflight =
      preflight(parent / "bootstrap-root");
  std::string json;
  std::string error;
  rc::StartupBootstrapPreflight parsed_preflight;
  expect(rc::serialize_startup_bootstrap_preflight(source_preflight, &json,
                                                   &error) &&
             rc::parse_startup_bootstrap_preflight(json, &parsed_preflight,
                                                   &error) &&
             parsed_preflight.root == source_preflight.root &&
             parsed_preflight.initialized_deployment ==
                 source_preflight.initialized_deployment &&
             parsed_preflight.dd_initialized,
         error.c_str());
  std::string noncanonical = json;
  noncanonical.insert(1, " ");
  expect(!rc::parse_startup_bootstrap_preflight(
             noncanonical, &parsed_preflight, &error),
         "preflight parser accepted noncanonical JSON");
  std::string unknown_field = json;
  unknown_field.insert(1, "\"unknown\":false,");
  expect(!rc::parse_startup_bootstrap_preflight(
             unknown_field, &parsed_preflight, &error),
         "preflight parser accepted an unknown field");
  rc::StartupBootstrapPreflight forged_preflight = source_preflight;
  forged_preflight.request_sha256.assign(64, '0');
  expect(!rc::serialize_startup_bootstrap_preflight(
             forged_preflight, &json, &error),
         "preflight serializer accepted an unbound request digest");
  const rc::StartupBootstrapPreflight digest_vector =
      preflight("/tmp/wesql-preflight-vector");
  expect(digest_vector.request_sha256 ==
             "d050a7882a3c8b947b09c9e547fe2e685d185202a93815f9100572969effd5af",
         "preflight request digest vector changed");
  for (size_t mutation = 0; mutation < 3; ++mutation) {
    forged_preflight = digest_vector;
    if (mutation == 0)
      forged_preflight.request_nonce.assign(64, 'a');
    else if (mutation == 1)
      forged_preflight.root = "/tmp/wesql-preflight-vector-other";
    else
      forged_preflight.initialized_deployment.stream_id =
          "r=repo/b=other";
    expect(!rc::serialize_startup_bootstrap_preflight(
               forged_preflight, &json, &error),
           "preflight serializer accepted a request-identity mutation");
  }

  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  expect(prepared.worker_required(), prepared.detail.c_str());
  rc::StartupWorkerRequest parsed_request;
  expect(rc::serialize_startup_worker_request(
             stream(), *prepared.worker_request, &json, &error) &&
             rc::parse_startup_worker_request(stream(), json, &parsed_request,
                                              &error) &&
             parsed_request.request_sha256 ==
                 prepared.worker_request->request_sha256 &&
             parsed_request.candidate->head.object.body ==
                 prepared.worker_request->candidate->head.object.body,
         error.c_str());
  rc::StartupWorkerRequest tampered_request = *prepared.worker_request;
  tampered_request.root /= "other";
  expect(!rc::serialize_startup_worker_request(stream(), tampered_request,
                                               &json, &error),
         "worker request digest did not detect field tampering");

  rc::StartupWorkerCompletion source_completion = completion(
      *prepared.worker_request,
      evidence(fixture.cursor, false, fixture.recovered_extents),
      takeover_cut(fixture, parent / "restore.tmp"));
  rc::StartupWorkerCompletion parsed_completion;
  expect(rc::serialize_startup_worker_completion(source_completion, &json,
                                                 &error) &&
             rc::parse_startup_worker_completion(json, &parsed_completion,
                                                 &error) &&
             parsed_completion.snapshot_cut.smartengine_extents.size() == 1 &&
             parsed_completion.request_sha256 ==
                 source_completion.request_sha256,
         error.c_str());

  rc::StartupCoordinatorResult finished =
      coordinator.finish_worker(source_completion);
  expect(finished.restart_required(), finished.detail.c_str());
  rc::StartupRestartProof parsed_proof;
  expect(rc::serialize_startup_restart_proof(
             stream(), *operations.persisted_proof, &json, &error) &&
             rc::parse_startup_restart_proof(stream(), json, &parsed_proof,
                                             &error) &&
             parsed_proof.marker == operations.persisted_proof->marker &&
             parsed_proof.expected_root.exported_extent_set_sha256 ==
                 operations.persisted_proof->expected_root
                     .exported_extent_set_sha256 &&
             parsed_proof.stopped_worker_root ==
                 operations.persisted_proof->stopped_worker_root &&
             parsed_proof.installed_root ==
                 operations.persisted_proof->installed_root,
         error.c_str());
  rc::StartupRestartProof bad_manifest = *operations.persisted_proof;
  ++bad_manifest.installed_root.regular_file_bytes;
  expect(!rc::serialize_startup_restart_proof(stream(), bad_manifest, &json,
                                              &error),
         "restart proof accepted inconsistent stable-root byte counts");
  expect(rc::serialize_startup_restart_proof(
             stream(), *operations.persisted_proof, &json, &error),
         error.c_str());
  const size_t field = json.find("\"route\":\"TAKEOVER\"");
  expect(field != std::string::npos, "restart proof route field missing");
  json.replace(field, std::string("\"route\":\"TAKEOVER\"").size(),
               "\"route\":\"BOOTSTRAP\"");
  expect(!rc::parse_startup_restart_proof(stream(), json, &parsed_proof,
                                          &error),
         "restart proof parser accepted route tampering");
}

void test_bootstrap_without_clean_preflight_fails_closed() {
  const fs::path parent = unique_parent("missing-preflight");
  RootCleanup cleanup(parent);
  BootstrapFixture fixture;
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_bootstrap(&storage, &operations, fixture, parent);
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  expect(prepared.outcome == rc::StartupCoordinatorOutcome::CORRUPT,
         "bootstrap accepted missing initialize preflight");
  expect_events(events, {"probe", "read", "classify"});
}

void test_bootstrap_preflight_identity_and_authorities_fail_closed() {
  for (const bool identity_mismatch : {true, false}) {
    const fs::path parent = unique_parent(
        identity_mismatch ? "preflight-identity" : "preflight-authority");
    RootCleanup cleanup(parent);
    BootstrapFixture fixture;
    expect(fs::create_directory(parent / "restore.tmp"),
           "cannot create invalid preflight root");
    rc::StartupBootstrapPreflight invalid = preflight(parent / "restore.tmp");
    if (identity_mismatch) {
      invalid.initialized_deployment.server_uuid =
          "11234567-89ab-cdef-0123-456789abcdef";
    } else {
      invalid.repository_empty = false;
    }
    std::vector<std::string> events;
    FakeStorage storage(&events);
    FakeOperations operations(&events);
    configure_bootstrap(&storage, &operations, fixture, parent);
    rc::StartupCoordinator coordinator(stream(), &storage, &operations);
    const rc::StartupCoordinatorResult prepared =
        coordinator.prepare_worker(options(parent, std::move(invalid)));
    expect(prepared.outcome == rc::StartupCoordinatorOutcome::CORRUPT,
           "bootstrap accepted mismatched preflight identity or authority");
    expect_events(events, {"probe", "read", "classify"});
  }
}

void test_foreign_target_stops_before_epoch() {
  const fs::path parent = unique_parent("foreign-target");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  storage.classification = {rc::TargetClass::FOREIGN_OR_CORRUPT, std::nullopt,
                            "foreign marker"};
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  expect(prepared.outcome == rc::StartupCoordinatorOutcome::FOREIGN_OR_CORRUPT,
         "foreign target was not rejected");
  expect_events(events, {"probe", "read", "classify"});
  expect(!fs::exists(parent / "restore.tmp"),
         "foreign target reached materialization");
}

void test_candidate_epoch_mismatch_fences_before_materialization() {
  const fs::path parent = unique_parent("epoch-mismatch");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  storage.reads[1].plan.epoch_object.etag = "competing-etag";
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  expect(prepared.outcome == rc::StartupCoordinatorOutcome::FENCED &&
             coordinator.state() == rc::StartupCoordinatorState::FENCED,
         "candidate epoch mismatch did not fence");
  expect_events(events, {"probe", "read", "classify", "acquire", "read"});
}

void test_worker_failure_and_binding_mismatch_never_publish() {
  for (const bool binding_mismatch : {false, true}) {
    const fs::path parent = unique_parent(binding_mismatch ? "bad-binding"
                                                           : "worker-crash");
    RootCleanup cleanup(parent);
    TakeoverFixture fixture;
    std::vector<std::string> events;
    FakeStorage storage(&events);
    FakeOperations operations(&events);
    configure_takeover(&storage, &operations, fixture, parent);
    rc::StartupCoordinator coordinator(stream(), &storage, &operations);
    rc::StartupCoordinatorResult prepared =
        coordinator.prepare_worker(options(parent));
    expect(prepared.worker_required(), prepared.detail.c_str());
    rc::StartupWorkerCompletion worker = completion(
        *prepared.worker_request,
        evidence(fixture.cursor, false, fixture.recovered_extents),
        takeover_cut(fixture, parent / "restore.tmp"));
    if (binding_mismatch) {
      worker.request_sha256 = hash("another-request");
    } else {
      worker.child_exited_cleanly = false;
    }
    rc::StartupCoordinatorResult finished = coordinator.finish_worker(worker);
    expect(finished.outcome == rc::StartupCoordinatorOutcome::CORRUPT,
           "invalid worker completion was accepted");
    expect_events(events, {"probe", "read", "classify", "acquire", "read",
                           "materialize"});
    expect(!fs::exists(parent / "data"),
           "invalid worker completion reached install");
  }
}

#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
void test_release_failure_kills_worker_before_completion_or_parent_progress() {
  const fs::path parent = unique_parent("release-failure");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  const rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  expect(prepared.worker_required(), prepared.detail.c_str());
  const std::vector<std::string> before_worker = events;
  const fs::path completion_path = parent / "worker-completion.json";

  const auto run_worker = [](const fs::path &output, bool inject_failure) {
    const pid_t child = ::fork();
    expect(child >= 0, "cannot fork release-failure worker");
    if (child == 0) {
      if (inject_failure)
        DBUG_SET("+d,remote_commit_fail_smartengine_snapshot_release");
      rc::startup_arm_release_failure_for_test();
      std::string error;
      if (rc::startup_prepare_clean_exit(&error)) _exit(91);
      std::ofstream completion_file(output,
                                    std::ios::binary | std::ios::trunc);
      completion_file << "clean-worker";
      completion_file.close();
      _exit(completion_file.good() ? 0 : 92);
    }

    int status = 0;
    pid_t waited;
    do {
      waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    expect(waited == child, "cannot reap release-failure worker");
    return status;
  };

  const fs::path control_path = parent / "control-completion.json";
  const int control_status = run_worker(control_path, false);
  expect(WIFEXITED(control_status) && WEXITSTATUS(control_status) == 0 &&
             fs::is_regular_file(control_path),
         "uninjected retained lease did not release cleanly");

  const int failure_status = run_worker(completion_path, true);
  expect(WIFSIGNALED(failure_status) && WTERMSIG(failure_status) == SIGABRT,
         "release-failure worker did not fail-stop with SIGABRT");
  expect(!fs::exists(completion_path),
         "release-failure worker published a completion proof");
  expect(coordinator.state() == rc::StartupCoordinatorState::WORKER_REQUIRED,
         "parent coordinator advanced after worker death");
  expect_events(events, before_worker);
  expect(std::find(events.begin(), events.end(), "publish") == events.end() &&
             std::find(events.begin(), events.end(), "install") == events.end(),
         "worker death reached parent publication or installation");
  expect(!fs::exists(parent / "data"),
         "worker death installed a target root");
}
#endif

void test_worker_extent_or_anchor_mismatch_never_publish() {
  for (const bool bad_anchor : {false, true}) {
    const fs::path parent =
        unique_parent(bad_anchor ? "bad-worker-anchor" : "bad-worker-extents");
    RootCleanup cleanup(parent);
    TakeoverFixture fixture;
    std::vector<std::string> events;
    FakeStorage storage(&events);
    FakeOperations operations(&events);
    configure_takeover(&storage, &operations, fixture, parent);
    rc::StartupCoordinator coordinator(stream(), &storage, &operations);
    rc::StartupCoordinatorResult prepared =
        coordinator.prepare_worker(options(parent));
    rc::StartupWorkerCompletion worker = completion(
        *prepared.worker_request,
        evidence(fixture.cursor, false, fixture.recovered_extents),
        takeover_cut(fixture, parent / "restore.tmp"));
    if (bad_anchor) {
      worker.snapshot_cut.log_anchor.generation = 6;
    } else {
      worker.snapshot_cut.smartengine_extents = {extent(5, "10")};
    }
    rc::StartupCoordinatorResult finished = coordinator.finish_worker(worker);
    expect(finished.outcome == rc::StartupCoordinatorOutcome::CORRUPT,
           "mismatched worker cut was accepted");
    expect(events.back() == "snapshot",
           "mismatched worker cut reached publication");
  }
}

void test_publish_restart_required_never_installs() {
  const fs::path parent = unique_parent("publish-restart");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  operations.publish_result = {rc::StartupStepOutcome::RESTART_REQUIRED,
                               "candidate advanced"};
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  rc::StartupCoordinatorResult finished = coordinator.finish_worker(completion(
      *prepared.worker_request,
      evidence(fixture.cursor, false, fixture.recovered_extents),
      takeover_cut(fixture, parent / "restore.tmp")));
  expect(finished.outcome == rc::StartupCoordinatorOutcome::RESTART_REQUIRED &&
             !finished.restart_required(),
         "publication retry was confused with installed-root re-exec");
  expect_events(events,
                {"probe", "read", "classify", "acquire", "read",
                 "materialize", "snapshot", "publish"});
  expect(!fs::exists(parent / "data"),
         "publication restart requirement reached install");
}

void test_publication_root_mutation_never_installs() {
  const fs::path parent = unique_parent("publication-root-mutation");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  operations.mutate_root_on_publish = parent / "restore.tmp";
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  rc::StartupCoordinatorResult finished = coordinator.finish_worker(completion(
      *prepared.worker_request,
      evidence(fixture.cursor, false, fixture.recovered_extents),
      takeover_cut(fixture, parent / "restore.tmp")));
  expect(finished.outcome == rc::StartupCoordinatorOutcome::CORRUPT,
         "publication-time stopped-root mutation was accepted");
  expect_events(events,
                {"probe", "read", "classify", "acquire", "read",
                 "materialize", "snapshot", "publish", "snapshot"});
  expect(!fs::exists(parent / "data"),
         "mutated publication root reached install");
}

void test_post_install_remote_change_fences_before_proof() {
  const fs::path parent = unique_parent("post-install-change");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  storage.reads.back().plan.head_object.etag = "changed-etag";
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  rc::StartupCoordinatorResult finished = coordinator.finish_worker(completion(
      *prepared.worker_request,
      evidence(fixture.cursor, false, fixture.recovered_extents),
      takeover_cut(fixture, parent / "restore.tmp")));
  expect(finished.outcome == rc::StartupCoordinatorOutcome::FENCED,
         "post-install remote change did not fence");
  expect_events(events,
                {"probe", "read", "classify", "acquire", "read",
                 "materialize", "snapshot", "publish", "snapshot", "read",
                 "install", "snapshot", "read"});
  expect(!operations.persisted_proof.has_value(),
         "fenced install emitted a restart proof");
}

rc::StartupRestartProof complete_takeover_parent(
    const fs::path &parent, const TakeoverFixture &fixture) {
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_takeover(&storage, &operations, fixture, parent);
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupCoordinatorResult prepared =
      coordinator.prepare_worker(options(parent));
  rc::StartupCoordinatorResult finished = coordinator.finish_worker(completion(
      *prepared.worker_request,
      evidence(fixture.cursor, false, fixture.recovered_extents),
      takeover_cut(fixture, parent / "restore.tmp")));
  expect(finished.restart_required() && operations.persisted_proof.has_value(),
         finished.detail.c_str());
  return *operations.persisted_proof;
}

void test_reexec_stale_proof_never_activates() {
  const fs::path parent = unique_parent("stale-proof");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  const rc::StartupRestartProof proof =
      complete_takeover_parent(parent, fixture);
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  rc::RecoveryPlan changed = fixture.published;
  changed.head_object.etag = "new-head-etag";
  storage.reads = {{rc::RecoveryReadOutcome::READY, changed, {}}};
  storage.classification = {rc::TargetClass::MANAGED_REPLACE, proof.marker, {}};
  rc::StartupCoordinator reexec(stream(), &storage, &operations);
  rc::StartupCoordinatorResult adopted =
      reexec.adopt_restart_proof(activation_options(parent), proof);
  expect(adopted.outcome == rc::StartupCoordinatorOutcome::FENCED,
         "stale restart proof was activated");
  expect_events(events, {"read"});
}

void test_reexec_marker_and_post_engine_mismatch_fail_closed() {
  for (const bool marker_mismatch : {true, false}) {
    const fs::path parent = unique_parent(marker_mismatch
                                              ? "reexec-marker-mismatch"
                                              : "post-engine-mismatch");
    RootCleanup cleanup(parent);
    TakeoverFixture fixture;
    const rc::StartupRestartProof proof =
        complete_takeover_parent(parent, fixture);
    std::vector<std::string> events;
    FakeStorage storage(&events);
    FakeOperations operations(&events);
    configure_reexec(&storage, &operations, fixture.published, proof);
    if (marker_mismatch) {
      rc::LocalInstallMarker changed = proof.marker;
      changed.installed_head.generation--;
      storage.classification.marker = std::move(changed);
    }
    rc::StartupCoordinator reexec(stream(), &storage, &operations);
    rc::StartupCoordinatorResult adopted =
        reexec.adopt_restart_proof(activation_options(parent), proof);
    if (marker_mismatch) {
      expect(adopted.outcome ==
                 rc::StartupCoordinatorOutcome::FOREIGN_OR_CORRUPT,
             "re-exec accepted another local marker");
      expect_events(events, {"read", "classify"});
      continue;
    }
    expect(adopted.post_engine_verification_required(), adopted.detail.c_str());
    rc::StartupRootEvidence installed =
        evidence(fixture.cursor, true, fixture.recovered_extents);
    installed.gtid_matches = false;
    rc::StartupCoordinatorResult finished =
        reexec.finish_post_engine_verification(installed);
    expect(finished.outcome == rc::StartupCoordinatorOutcome::CORRUPT,
           "re-exec accepted incomplete post-engine evidence");
    expect_events(events, {"read", "classify", "snapshot", "activate"});
  }
}

void test_reexec_installed_root_manifest_mismatch_never_activates() {
  const fs::path parent = unique_parent("reexec-root-mismatch");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  const rc::StartupRestartProof proof =
      complete_takeover_parent(parent, fixture);
  std::ofstream mutation(parent / "data" / "post-proof-mutation",
                         std::ios::binary | std::ios::trunc);
  mutation << "changed";
  mutation.close();
  expect(mutation.good(), "cannot mutate installed root fixture");

  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  configure_reexec(&storage, &operations, fixture.published, proof);
  rc::StartupCoordinator reexec(stream(), &storage, &operations);
  const rc::StartupCoordinatorResult adopted =
      reexec.adopt_restart_proof(activation_options(parent), proof);
  expect(adopted.outcome ==
             rc::StartupCoordinatorOutcome::FOREIGN_OR_CORRUPT,
         "re-exec accepted another installed-root manifest");
  expect_events(events, {"read", "classify", "snapshot"});
}

void test_phase_misuse_fails_without_callbacks() {
  std::vector<std::string> events;
  FakeStorage storage(&events);
  FakeOperations operations(&events);
  rc::StartupCoordinator coordinator(stream(), &storage, &operations);
  rc::StartupWorkerCompletion completion_value;
  rc::StartupCoordinatorResult finished =
      coordinator.finish_worker(completion_value);
  expect(finished.outcome == rc::StartupCoordinatorOutcome::CORRUPT,
         "finish-before-prepare did not fail closed");
  expect_events(events, {});

  const fs::path parent = unique_parent("double-prepare");
  RootCleanup cleanup(parent);
  TakeoverFixture fixture;
  FakeStorage second_storage(&events);
  FakeOperations second_operations(&events);
  configure_takeover(&second_storage, &second_operations, fixture, parent);
  rc::StartupCoordinator second(stream(), &second_storage, &second_operations);
  rc::StartupCoordinatorResult prepared =
      second.prepare_worker(options(parent));
  expect(prepared.worker_required(), prepared.detail.c_str());
  const std::vector<std::string> before = events;
  rc::StartupCoordinatorResult duplicate =
      second.prepare_worker(options(parent));
  expect(duplicate.outcome == rc::StartupCoordinatorOutcome::CORRUPT,
         "double prepare did not fail closed");
  expect_events(events, before);
}

}  // namespace

int main() {
  test_takeover_parent_worker_reexec_lifecycle();
  test_bootstrap_preflight_worker_reexec_lifecycle();
  test_typed_transport_round_trips_and_rejects_tampering();
  test_bootstrap_without_clean_preflight_fails_closed();
  test_bootstrap_preflight_identity_and_authorities_fail_closed();
  test_foreign_target_stops_before_epoch();
  test_candidate_epoch_mismatch_fences_before_materialization();
  test_worker_failure_and_binding_mismatch_never_publish();
#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
  test_release_failure_kills_worker_before_completion_or_parent_progress();
#endif
  test_worker_extent_or_anchor_mismatch_never_publish();
  test_publish_restart_required_never_installs();
  test_publication_root_mutation_never_installs();
  test_post_install_remote_change_fences_before_proof();
  test_reexec_stale_proof_never_activates();
  test_reexec_marker_and_post_engine_mismatch_fail_closed();
  test_reexec_installed_root_manifest_mismatch_never_activates();
  test_phase_misuse_fails_without_callbacks();
  std::cout << "remote commit startup coordinator tests passed\n";
  return EXIT_SUCCESS;
}
