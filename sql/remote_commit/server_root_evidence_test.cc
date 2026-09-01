/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/server_root_evidence.h"

#include "sql/remote_commit/persistent_engine_policy.h"

#include <cstdlib>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rc = wesql::remote_commit;

namespace {

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "server root evidence test failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

std::string hash(std::string_view bytes) {
  std::string digest;
  std::string error;
  expect(rc::sha256_hex(bytes, &digest, &error), "cannot hash fixture");
  return digest;
}

rc::GtidSetDigest gtid(std::string_view text) {
  rc::GtidSetDigest digest;
  std::string error;
  expect(rc::gtid_digest(text, &digest, &error), "cannot build GTID fixture");
  return digest;
}

template <typename T>
rc::ServerRootObservedValue<T> observed(T value) {
  return {true, std::move(value), {}};
}

rc::SmartengineExtentRef extent(uint64_t allocation, uint64_t object,
                                std::string key, char digest_byte = 'a') {
  rc::SmartengineExtentRef value;
  value.writer_epoch = 7;
  value.allocation_seq = std::to_string(allocation);
  value.database_name_hex = "74657374";
  value.index_id = "5";
  value.object_id = std::to_string(object);
  value.key = std::move(key);
  value.size = object + 10;
  value.sha256 = std::string(64, digest_byte);
  return value;
}

struct Fixture {
  rc::StartupDeploymentIdentity deployment;
  rc::RecoveryPlan candidate;
  rc::RecoveryPlan published;
  rc::ServerRootEvidenceObservation observation;
  rc::LocalInstallMarker marker;

  Fixture() {
    deployment.stream_id = "r=repo/b=branch";
    deployment.server_uuid = "01234567-89ab-cdef-0123-456789abcdef";
    deployment.fingerprints = {std::string(64, '1'), "wesql-test-build",
                               std::string(64, '2'), std::string(64, '3'),
                               std::string(64, '4')};
    deployment.binary_fingerprint = std::string(64, '5');

    candidate.head.durable_cursor = {"binlog.000007", 700};
    candidate.snapshot.cursor = candidate.head.durable_cursor;
    candidate.snapshot.smartengine_extents = {
        extent(2, 20, "extent/20", 'a'),
        extent(10, 100, "extent/100", 'b')};
    candidate.snapshot.smartengine_extents[0].ordinal = 0;
    candidate.snapshot.smartengine_extents[1].ordinal = 1;

    published.head_object = {"published-head-body", "published-head-etag"};
    published.head.generation = 8;
    published.head.durable_cursor = candidate.head.durable_cursor;
    published.head.snapshot.id = "published-snapshot";
    published.head.snapshot.manifest_sha256 = std::string(64, '6');
    published.head.snapshot.cursor = candidate.head.durable_cursor;
    published.snapshot.cursor = candidate.head.durable_cursor;
    published.snapshot.server_identity.server_uuid = deployment.server_uuid;
    published.snapshot.gtid_executed =
        gtid("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3");
    published.snapshot.smartengine_extents =
        candidate.snapshot.smartengine_extents;

    marker.stream_id = deployment.stream_id;
    marker.server_uuid = deployment.server_uuid;
    marker.installed_head.generation = published.head.generation;
    marker.installed_head.body_sha256 = hash(published.head_object.body);
    marker.installed_head.snapshot_id = published.head.snapshot.id;
    marker.installed_head.snapshot_manifest_sha256 =
        published.head.snapshot.manifest_sha256;
    marker.installed_head.snapshot_cursor = published.head.snapshot.cursor;
    marker.config_digest = deployment.fingerprints.startup_config_sha256;
    marker.binary_fingerprint = deployment.binary_fingerprint;

    observation.opened_root_matches = observed(true);
    observation.coherent_cursor = observed(candidate.head.durable_cursor);
    observation.executed_gtid = observed(published.snapshot.gtid_executed);
    observation.server_uuid = observed(deployment.server_uuid);
    observation.declared_deployment = observed(deployment);
    observation.dd_matches = observed(true);
    observation.replication = observed(rc::ServerReplicationInventory{});
    observation.smartengine_snapshot_cursor =
        observed(candidate.head.durable_cursor);
    observation.smartengine_live_extents =
        observed(candidate.snapshot.smartengine_extents);
    observation.prepared = observed(rc::ServerPreparedInventory{});
    observation.installed_marker =
        observed(std::optional<rc::LocalInstallMarker>{marker});
    observation.empty_source_scan_stable = observed(true);
    observation.old_tc_authority_empty = observed(true);
    observation.user_state_empty = observed(true);
    observation.legacy_live_extents_empty = observed(true);
  }
};

void test_runtime_deployment_declaration_is_external_and_immutable() {
  rc::reset_server_root_runtime_deployment_for_test();
  rc::StartupDeploymentIdentity declaration;
  std::string error;
  expect(!rc::configured_server_root_runtime_deployment(&declaration, &error) &&
             !error.empty(),
         "missing runtime declaration was fabricated");

  Fixture fixture;
  expect(rc::declare_server_root_runtime_deployment(fixture.deployment,
                                                    &error) &&
             error.empty(),
         "valid external runtime declaration was rejected");
  expect(rc::configured_server_root_runtime_deployment(&declaration, &error) &&
             declaration == fixture.deployment && error.empty(),
         "runtime declaration accessor changed the external identity");
  expect(rc::declare_server_root_runtime_deployment(fixture.deployment,
                                                    &error),
         "idempotent runtime declaration was rejected");
  expect(!rc::configured_server_root_runtime_deployment(nullptr, &error) &&
             !error.empty(),
         "null runtime declaration output was accepted");

  rc::StartupDeploymentIdentity conflicting = fixture.deployment;
  conflicting.stream_id.append("/other");
  expect(!rc::declare_server_root_runtime_deployment(conflicting, &error) &&
             !error.empty(),
         "runtime deployment identity changed after installation");
  expect(rc::configured_server_root_runtime_deployment(&declaration, &error) &&
             declaration == fixture.deployment,
         "conflicting declaration replaced the installed identity");

  using Mutation =
      std::function<void(rc::StartupDeploymentIdentity &declaration)>;
  const std::vector<std::pair<const char *, Mutation>> malformed{
      {"empty stream", [](auto &d) { d.stream_id.clear(); }},
      {"control byte", [](auto &d) { d.fingerprints.server_build = "x\n"; }},
      {"uppercase UUID", [](auto &d) { d.server_uuid[0] = 'A'; }},
      {"short UUID", [](auto &d) { d.server_uuid.pop_back(); }},
      {"uppercase config hash",
       [](auto &d) { d.fingerprints.startup_config_sha256[0] = 'A'; }},
      {"uppercase component hash",
       [](auto &d) {
         d.fingerprints.plugin_component_set_sha256[0] = 'A';
       }},
      {"uppercase keyring hash",
       [](auto &d) { d.fingerprints.keyring_config_sha256[0] = 'A'; }},
      {"uppercase TLS hash",
       [](auto &d) { d.fingerprints.tls_config_sha256[0] = 'A'; }},
      {"uppercase binary hash",
       [](auto &d) { d.binary_fingerprint[0] = 'A'; }},
  };
  for (const auto &[name, mutate] : malformed) {
    rc::reset_server_root_runtime_deployment_for_test();
    rc::StartupDeploymentIdentity invalid = fixture.deployment;
    mutate(invalid);
    error.clear();
    expect(!rc::declare_server_root_runtime_deployment(invalid, &error) &&
               error.find("malformed") != std::string::npos,
           name);
  }
  rc::reset_server_root_runtime_deployment_for_test();
}

rc::StartupStepResult compare_takeover(
    Fixture &fixture, const rc::ServerRootEvidenceObservation &observation,
    rc::StartupRootEvidence *evidence, bool installed = false) {
  const rc::ServerRootVerificationRequest request{
      rc::StartupCoordinatorRoute::TAKEOVER, "/tmp/root", fixture.deployment,
      installed, installed ? nullptr : &fixture.candidate,
      installed ? &fixture.published : nullptr};
  return rc::compare_server_root_evidence(request, observation, evidence);
}

void expect_corrupt(Fixture &fixture,
                    rc::ServerRootEvidenceObservation observation,
                    const char *message) {
  rc::StartupRootEvidence evidence;
  expect(compare_takeover(fixture, observation, &evidence).outcome ==
             rc::StartupStepOutcome::CORRUPT,
         message);
}

void test_extent_canonicalization() {
  rc::SmartengineExtentRef ten = extent(10, 100, "extent/100", 'b');
  ten.ordinal = 99;
  rc::SmartengineExtentRef two = extent(2, 20, "extent/20", 'a');
  two.ordinal = 42;
  std::vector<rc::SmartengineExtentRef> canonical;
  std::string error;
  expect(rc::canonicalize_server_root_extents({ten, two}, &canonical, &error) &&
             canonical.size() == 2 && canonical[0].allocation_seq == "2" &&
             canonical[0].ordinal == 0 &&
             canonical[1].allocation_seq == "10" &&
             canonical[1].ordinal == 1,
         "numeric extent order and ordinal normalization failed");

  rc::SmartengineExtentRef conflicting = two;
  conflicting.key = "extent/conflict";
  expect(!rc::canonicalize_server_root_extents({two, conflicting}, &canonical,
                                                &error),
         "conflicting duplicate extent identity was accepted");
  conflicting = extent(3, 30, two.key, 'c');
  expect(!rc::canonicalize_server_root_extents({two, conflicting}, &canonical,
                                                &error),
         "duplicate extent key was accepted");
  conflicting = two;
  conflicting.allocation_seq = "02";
  expect(!rc::canonicalize_server_root_extents({conflicting}, &canonical,
                                                &error),
         "noncanonical decimal extent identity was accepted");
  conflicting = two;
  conflicting.format = "legacy-file-extent";
  expect(!rc::canonicalize_server_root_extents({conflicting}, &canonical,
                                                &error),
         "legacy extent format was accepted");
}

void test_persistent_engine_policy() {
  using rc::TableEngineScope;
  expect(rc::table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                  "InnoDB"),
         "InnoDB persistent user table was rejected");
  expect(rc::table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                  "innodb"),
         "InnoDB engine matching became case-sensitive");
#ifdef WITH_SMARTENGINE
#ifdef WITH_XENGINE_COMPATIBLE_MODE
  expect(rc::table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                  "XENGINE"),
         "immutable-extent SmartEngine compatibility name was rejected");
  expect(!rc::table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                   "SMARTENGINE"),
         "inactive SmartEngine plugin name was accepted");
#else
  expect(rc::table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                  "SMARTENGINE"),
         "immutable-extent SmartEngine was rejected");
  expect(!rc::table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                   "XENGINE"),
         "inactive SmartEngine compatibility name was accepted");
#endif
#else
  expect(!rc::table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                   "SMARTENGINE"),
         "unbuilt SmartEngine was accepted");
#endif

  for (const std::string_view engine :
       {"MyISAM", "CSV", "MEMORY", "BLACKHOLE", "third_party", ""}) {
    expect(!rc::table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                     engine),
           "unsupported persistent user table engine was accepted");
    expect(rc::table_engine_allowed(TableEngineScope::USER_TEMPORARY, engine),
           "user temporary table engine was rejected");
    expect(rc::table_engine_allowed(TableEngineScope::INTERNAL, engine),
           "internal table engine was rejected");
  }
}

void test_takeover_all_authoritative() {
  Fixture fixture;
  rc::StartupRootEvidence evidence;
  const rc::StartupStepResult result =
      compare_takeover(fixture, fixture.observation, &evidence);
  expect(result.ready(), result.detail.c_str());
  expect(evidence.recovered_cursor == fixture.candidate.head.durable_cursor &&
             evidence.recovered_gtid ==
                 fixture.published.snapshot.gtid_executed &&
             evidence.server_uuid_matches && evidence.configuration_matches &&
             evidence.gtid_matches && evidence.dd_matches &&
             evidence.repository_empty && evidence.extent_live_set_matches &&
             evidence.exported_extent_count == 2 &&
             evidence.exported_extent_set_sha256.size() == 64 &&
             evidence.internal_prepared_empty && evidence.external_xa_empty,
         "authoritative takeover evidence was not fully populated");
}

void test_every_unavailable_required_proof_blocks() {
  using Mutation =
      std::function<void(rc::ServerRootEvidenceObservation &observation)>;
  const std::vector<std::pair<const char *, Mutation>> cases{
      {"opened root", [](auto &o) { o.opened_root_matches.available = false; }},
      {"cursor", [](auto &o) { o.coherent_cursor.available = false; }},
      {"GTID", [](auto &o) { o.executed_gtid.available = false; }},
      {"UUID", [](auto &o) { o.server_uuid.available = false; }},
      {"deployment declaration",
       [](auto &o) { o.declared_deployment.available = false; }},
      {"DD", [](auto &o) { o.dd_matches.available = false; }},
      {"replication", [](auto &o) { o.replication.available = false; }},
      {"engine cursor",
       [](auto &o) { o.smartengine_snapshot_cursor.available = false; }},
      {"live set",
       [](auto &o) { o.smartengine_live_extents.available = false; }},
      {"prepared", [](auto &o) { o.prepared.available = false; }},
  };
  for (const auto &[name, mutate] : cases) {
    Fixture fixture;
    mutate(fixture.observation);
    rc::StartupRootEvidence evidence;
    const rc::StartupStepResult result =
        compare_takeover(fixture, fixture.observation, &evidence);
    expect(result.outcome == rc::StartupStepOutcome::BLOCKED, name);
  }
}

void test_available_mismatches_are_corrupt() {
  {
    Fixture fixture;
    fixture.observation.opened_root_matches.value = false;
    expect_corrupt(fixture, fixture.observation, "opened-root mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.coherent_cursor.value.pos++;
    expect_corrupt(fixture, fixture.observation, "cursor mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.executed_gtid.value.sha256[0] = 'f';
    expect_corrupt(fixture, fixture.observation, "invalid GTID digest passed");
  }
  {
    Fixture fixture;
    fixture.observation.server_uuid.value[0] = 'f';
    expect_corrupt(fixture, fixture.observation, "UUID mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.declared_deployment.value.fingerprints.server_build =
        "foreign";
    expect_corrupt(fixture, fixture.observation,
                   "configuration mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.dd_matches.value = false;
    fixture.observation.dd_matches.detail =
        "persistent user table `app`.`legacy` uses unsupported engine MyISAM";
    rc::StartupRootEvidence evidence;
    const rc::StartupStepResult result =
        compare_takeover(fixture, fixture.observation, &evidence);
    expect(result.outcome == rc::StartupStepOutcome::CORRUPT &&
               result.detail.find("unsupported engine MyISAM") !=
                   std::string::npos,
           "unsupported persistent user table DD mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.replication.value.worker_rows = 1;
    expect_corrupt(fixture, fixture.observation,
                   "replication inventory mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.prepared.value.internal_entries = 1;
    expect_corrupt(fixture, fixture.observation,
                   "internal prepared inventory mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.prepared.value.external_entries = 1;
    expect_corrupt(fixture, fixture.observation,
                   "external XA inventory mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.smartengine_snapshot_cursor.value.pos++;
    expect_corrupt(fixture, fixture.observation,
                   "SmartEngine cursor mismatch passed");
  }
  {
    Fixture fixture;
    fixture.observation.smartengine_live_extents.value.pop_back();
    rc::StartupRootEvidence evidence;
    expect(compare_takeover(fixture, fixture.observation, &evidence, true)
                   .outcome == rc::StartupStepOutcome::CORRUPT,
           "installed SmartEngine live-set mismatch passed");
  }
}

void test_full_deployment_identity_is_compared() {
  using Mutation =
      std::function<void(rc::StartupDeploymentIdentity &declaration)>;
  const std::vector<std::pair<const char *, Mutation>> cases{
      {"stream ID", [](auto &d) { d.stream_id.append("/foreign"); }},
      {"server UUID", [](auto &d) { d.server_uuid[0] = 'f'; }},
      {"startup configuration fingerprint",
       [](auto &d) { d.fingerprints.startup_config_sha256[0] = 'a'; }},
      {"server build", [](auto &d) {
         d.fingerprints.server_build.append("-foreign");
       }},
      {"plugin/component fingerprint", [](auto &d) {
         d.fingerprints.plugin_component_set_sha256[0] = 'a';
       }},
      {"keyring fingerprint",
       [](auto &d) { d.fingerprints.keyring_config_sha256[0] = 'a'; }},
      {"TLS fingerprint",
       [](auto &d) { d.fingerprints.tls_config_sha256[0] = 'a'; }},
      {"binary fingerprint",
       [](auto &d) { d.binary_fingerprint[0] = 'a'; }},
  };
  for (const auto &[name, mutate] : cases) {
    Fixture fixture;
    mutate(fixture.observation.declared_deployment.value);
    expect_corrupt(fixture, fixture.observation, name);
  }

  Fixture malformed_request;
  malformed_request.deployment.server_uuid[0] = 'A';
  rc::StartupRootEvidence evidence;
  const rc::StartupStepResult result = compare_takeover(
      malformed_request, malformed_request.observation, &evidence);
  expect(result.outcome == rc::StartupStepOutcome::CORRUPT &&
             result.detail.find("malformed deployment identity") !=
                 std::string::npos &&
             evidence.recovered_cursor.file.empty() &&
             evidence.recovered_cursor.pos == 0 &&
             !evidence.configuration_matches &&
             evidence.exported_extent_count == 0,
         "malformed request deployment identity was accepted");
}

void test_malformed_request_shape_is_rejected() {
  Fixture fixture;
  rc::StartupRootEvidence evidence;
  const auto rejected = [&](rc::ServerRootVerificationRequest request,
                            std::string_view expected_detail,
                            const char *message) {
    const rc::StartupStepResult result = rc::compare_server_root_evidence(
        request, fixture.observation, &evidence);
    expect(result.outcome == rc::StartupStepOutcome::CORRUPT &&
               result.detail.find(expected_detail) != std::string::npos &&
               evidence.recovered_cursor.file.empty() &&
               evidence.exported_extent_count == 0,
           message);
  };

  rc::ServerRootVerificationRequest request{
      rc::StartupCoordinatorRoute::TAKEOVER, "/tmp/root", fixture.deployment,
      false, &fixture.candidate, nullptr};
  request.route = static_cast<rc::StartupCoordinatorRoute>(255);
  rejected(request, "invalid route", "invalid request route was accepted");

  request = {rc::StartupCoordinatorRoute::TAKEOVER, "relative/root",
             fixture.deployment, false, &fixture.candidate, nullptr};
  rejected(request, "invalid root path",
           "relative verification root was accepted");

  request = {rc::StartupCoordinatorRoute::BOOTSTRAP, "/tmp/root",
             fixture.deployment, false, &fixture.candidate, nullptr};
  rejected(request, "inconsistent route pointers",
           "bootstrap request with a candidate was accepted");

  request = {rc::StartupCoordinatorRoute::TAKEOVER, "/tmp/root",
             fixture.deployment, true, &fixture.candidate, nullptr};
  rejected(request, "inconsistent route pointers",
           "installed request without a published HEAD was accepted");
}

void test_takeover_uses_post_replay_live_set() {
  Fixture fixture;
  fixture.candidate.snapshot.cursor.pos = 600;
  fixture.observation.smartengine_live_extents.value = {
      extent(12, 120, "extent/120", 'c')};
  rc::StartupRootEvidence evidence;
  const rc::StartupStepResult result =
      compare_takeover(fixture, fixture.observation, &evidence);
  expect(result.ready() && evidence.extent_live_set_matches &&
             evidence.exported_extent_count == 1 &&
             evidence.exported_extent_set_sha256.size() == 64,
         "takeover compared the post-replay live set with the base snapshot");
}

void test_installed_exact_marker_and_snapshot() {
  Fixture fixture;
  rc::StartupRootEvidence evidence;
  rc::StartupStepResult result =
      compare_takeover(fixture, fixture.observation, &evidence, true);
  expect(result.ready() && evidence.marker_matches &&
             evidence.snapshot_matches,
         result.detail.c_str());

  fixture.observation.installed_marker.value->installed_head.generation++;
  result = compare_takeover(fixture, fixture.observation, &evidence, true);
  expect(result.outcome == rc::StartupStepOutcome::CORRUPT &&
             !evidence.marker_matches,
         "non-exact installed marker was accepted");

  fixture = Fixture{};
  fixture.observation.executed_gtid = observed(
      gtid("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-4"));
  result = compare_takeover(fixture, fixture.observation, &evidence, true);
  expect(result.outcome == rc::StartupStepOutcome::CORRUPT &&
             !evidence.gtid_matches && !evidence.snapshot_matches,
         "valid but different installed GTID was accepted");

  fixture = Fixture{};
  fixture.observation.installed_marker.available = false;
  result = compare_takeover(fixture, fixture.observation, &evidence, true);
  expect(result.outcome == rc::StartupStepOutcome::BLOCKED &&
             !evidence.marker_matches,
         "unavailable installed marker was accepted");
}

void test_bootstrap_requires_bootstrap_authorities() {
  Fixture fixture;
  fixture.observation.smartengine_live_extents =
      observed(std::vector<rc::SmartengineExtentRef>{});
  const rc::ServerRootVerificationRequest request{
      rc::StartupCoordinatorRoute::BOOTSTRAP, "/tmp/root", fixture.deployment,
      false, nullptr, nullptr};
  rc::StartupRootEvidence evidence;
  rc::StartupStepResult result =
      rc::compare_server_root_evidence(request, fixture.observation, &evidence);
  expect(result.ready(), result.detail.c_str());

  using Mutation =
      std::function<void(rc::ServerRootEvidenceObservation &observation)>;
  const std::vector<std::pair<const char *, Mutation>> unavailable{
      {"stable EMPTY_SOURCE scan",
       [](auto &o) { o.empty_source_scan_stable.available = false; }},
      {"old transaction-coordinator authority",
       [](auto &o) { o.old_tc_authority_empty.available = false; }},
      {"bootstrap user state",
       [](auto &o) { o.user_state_empty.available = false; }},
      {"legacy SmartEngine extent boundary",
       [](auto &o) { o.legacy_live_extents_empty.available = false; }},
  };
  for (const auto &[name, mutate] : unavailable) {
    rc::ServerRootEvidenceObservation blocked = fixture.observation;
    mutate(blocked);
    result = rc::compare_server_root_evidence(request, blocked, &evidence);
    expect(result.outcome == rc::StartupStepOutcome::BLOCKED &&
               result.detail.find(name) != std::string::npos &&
               result.detail.find("unavailable") != std::string::npos,
           name);
  }

  const std::vector<std::pair<const char *, Mutation>> mismatched{
      {"unstable-paired-scan",
       [](auto &o) {
         o.empty_source_scan_stable.value = false;
         o.empty_source_scan_stable.detail = "unstable-paired-scan";
       }},
      {"old-tc-present",
       [](auto &o) {
         o.old_tc_authority_empty.value = false;
         o.old_tc_authority_empty.detail = "old-tc-present";
       }},
      {"noncanonical-user-state",
       [](auto &o) {
         o.user_state_empty.value = false;
         o.user_state_empty.detail = "noncanonical-user-state";
       }},
      {"legacy-extent-present",
       [](auto &o) {
         o.legacy_live_extents_empty.value = false;
         o.legacy_live_extents_empty.detail = "legacy-extent-present";
       }},
  };
  for (const auto &[detail, mutate] : mismatched) {
    rc::ServerRootEvidenceObservation corrupt = fixture.observation;
    mutate(corrupt);
    result = rc::compare_server_root_evidence(request, corrupt, &evidence);
    expect(result.outcome == rc::StartupStepOutcome::CORRUPT &&
               result.detail.find(detail) != std::string::npos,
           detail);
  }
}

}  // namespace

int main() {
  test_runtime_deployment_declaration_is_external_and_immutable();
  test_extent_canonicalization();
  test_persistent_engine_policy();
  test_takeover_all_authoritative();
  test_every_unavailable_required_proof_blocks();
  test_available_mismatches_are_corrupt();
  test_full_deployment_identity_is_compared();
  test_malformed_request_shape_is_rejected();
  test_takeover_uses_post_replay_live_set();
  test_installed_exact_marker_and_snapshot();
  test_bootstrap_requires_bootstrap_authorities();
  std::cout << "server root evidence tests passed\n";
  return EXIT_SUCCESS;
}
