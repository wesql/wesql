/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/server_root_evidence.h"

#include "sql/remote_commit/persistent_engine_policy.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <list>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#ifndef WESQL_SERVER_ROOT_EVIDENCE_TEST_ONLY
#include "my_dbug.h"
#include "my_sys.h"
#include "mysql_version.h"
#include "mysql/plugin.h"
#include "scope_guard.h"
#include "sql/auto_thd.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/auth_common.h"
#include "sql/auth/auth_internal.h"
#include "sql/auth/sql_auth_cache.h"
#include "sql/binlog.h"
#include "sql/binlog_index.h"
#include "sql/binlog_istream.h"
#include "sql/binlog_reader.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dd.h"
#include "sql/dd/dd_version.h"
#include "sql/dd/impl/bootstrap/bootstrap_ctx.h"
#include "sql/dd/types/schema.h"
#include "sql/dd/types/table.h"
#include "sql/handler.h"
#include "sql/log_event.h"
#include "sql/mysqld.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/rpl_gtid.h"
#include "sql/rpl_info_table_access.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_msr.h"
#include "sql/rpl_rli.h"
#include "sql/rpl_rli_pdb.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin.h"
#include "sql/tc_log.h"
#include "sql/table.h"
#include "sql/xa/recovery.h"

extern Granted_roles_graph *g_granted_roles;
extern Default_roles *g_default_roles;
#endif

namespace wesql::remote_commit {
namespace {

namespace fs = std::filesystem;

constexpr std::string_view kSmartengineExtentFormat =
    "smartengine-object-extent-v2";

struct RuntimeDeploymentRegistry {
  std::mutex mutex;
  std::optional<StartupDeploymentIdentity> declaration;
};

RuntimeDeploymentRegistry &runtime_deployment_registry() {
  static RuntimeDeploymentRegistry registry;
  return registry;
}

bool lowercase_sha256(std::string_view value) {
  if (value.size() != 64) return false;
  return std::all_of(value.begin(), value.end(), [](const char byte) {
    return (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f');
  });
}

bool valid_runtime_declaration(const StartupDeploymentIdentity &value) {
  const auto valid_text = [](std::string_view text, size_t maximum) {
    return !text.empty() && text.size() <= maximum &&
           std::all_of(text.begin(), text.end(), [](const unsigned char byte) {
             return byte >= 0x20 && byte != 0x7f;
           });
  };
  const auto valid_uuid = [](std::string_view uuid) {
    if (uuid.size() != 36) return false;
    for (size_t index = 0; index < uuid.size(); ++index) {
      if (index == 8 || index == 13 || index == 18 || index == 23) {
        if (uuid[index] != '-') return false;
      } else if (!((uuid[index] >= '0' && uuid[index] <= '9') ||
                   (uuid[index] >= 'a' && uuid[index] <= 'f'))) {
        return false;
      }
    }
    return true;
  };
  return valid_text(value.stream_id, kMaxObjectKeyBytes) &&
         valid_uuid(value.server_uuid) &&
         lowercase_sha256(value.fingerprints.startup_config_sha256) &&
         valid_text(value.fingerprints.server_build, kMaxOrdinaryIdBytes) &&
         lowercase_sha256(
             value.fingerprints.plugin_component_set_sha256) &&
         lowercase_sha256(value.fingerprints.keyring_config_sha256) &&
         lowercase_sha256(value.fingerprints.tls_config_sha256) &&
         lowercase_sha256(value.binary_fingerprint);
}

bool lowercase_even_hex(std::string_view value) {
  return !value.empty() && value.size() % 2 == 0 &&
         std::all_of(value.begin(), value.end(), [](const char byte) {
           return (byte >= '0' && byte <= '9') ||
                  (byte >= 'a' && byte <= 'f');
         });
}

bool canonical_decimal(std::string_view value, uint64_t *number) {
  if (number == nullptr || value.empty() ||
      (value.size() > 1 && value.front() == '0'))
    return false;
  uint64_t parsed = 0;
  for (const char byte : value) {
    if (byte < '0' || byte > '9') return false;
    const uint64_t digit = static_cast<uint64_t>(byte - '0');
    if (parsed > (std::numeric_limits<uint64_t>::max() - digit) / 10)
      return false;
    parsed = parsed * 10 + digit;
  }
  *number = parsed;
  return true;
}

bool valid_cursor(const Cursor &cursor) {
  return !cursor.file.empty() && cursor.pos >= 4 &&
         cursor.pos <= kJsonSafeIntegerMax;
}

bool valid_gtid(const GtidSetDigest &gtid) {
  GtidSetDigest recomputed;
  std::string error;
  return gtid.canonical.size() <= kMaxCanonicalGtidBytes &&
         gtid_digest(gtid.canonical, &recomputed, &error) &&
         recomputed == gtid;
}

std::string joined_issues(const std::vector<std::string> &issues) {
  std::string detail;
  for (const std::string &issue : issues) {
    if (!detail.empty()) detail.append("; ");
    detail.append(issue);
  }
  return detail;
}

void unavailable(std::string_view authority, std::string_view detail,
                 bool *has_unavailable, std::vector<std::string> *issues) {
  *has_unavailable = true;
  std::string issue(authority);
  issue.append(" authority unavailable");
  if (!detail.empty()) issue.append(": ").append(detail);
  issues->push_back(std::move(issue));
}

void record_mismatch(std::string_view authority, std::string_view detail,
                     bool *has_mismatch,
                     std::vector<std::string> *issues) {
  *has_mismatch = true;
  std::string issue(authority);
  issue.append(" mismatch");
  if (!detail.empty()) issue.append(": ").append(detail);
  issues->push_back(std::move(issue));
}

template <typename T>
bool require_available(const ServerRootObservedValue<T> &observed,
                       std::string_view authority, bool *has_unavailable,
                       std::vector<std::string> *issues) {
  if (observed.available) return true;
  unavailable(authority, observed.detail, has_unavailable, issues);
  return false;
}

bool build_expected_marker(const ServerRootVerificationRequest &request,
                           LocalInstallMarker *marker, std::string *error) {
  if (marker == nullptr || request.published == nullptr) {
    if (error != nullptr)
      *error = "installed verification has no published HEAD";
    return false;
  }
  std::string head_sha256;
  if (!sha256_hex(request.published->head_object.body, &head_sha256, error))
    return false;
  marker->stream_id = request.deployment.stream_id;
  marker->server_uuid =
      request.published->snapshot.server_identity.server_uuid;
  marker->installed_head.generation = request.published->head.generation;
  marker->installed_head.body_sha256 = std::move(head_sha256);
  marker->installed_head.snapshot_id = request.published->head.snapshot.id;
  marker->installed_head.snapshot_manifest_sha256 =
      request.published->head.snapshot.manifest_sha256;
  marker->installed_head.snapshot_cursor =
      request.published->head.snapshot.cursor;
  marker->config_digest =
      request.deployment.fingerprints.startup_config_sha256;
  marker->binary_fingerprint = request.deployment.binary_fingerprint;
  return true;
}

bool valid_request_shape(const ServerRootVerificationRequest &request,
                         std::string *detail) {
  if (request.route != StartupCoordinatorRoute::BOOTSTRAP &&
      request.route != StartupCoordinatorRoute::TAKEOVER) {
    *detail = "startup verification request has an invalid route";
    return false;
  }
  if (request.root.empty() || !request.root.is_absolute()) {
    *detail = "startup verification request has an invalid root path";
    return false;
  }
  const bool bootstrap =
      request.route == StartupCoordinatorRoute::BOOTSTRAP;
  const bool pointers_consistent =
      request.installed
          ? request.published != nullptr && request.candidate == nullptr
          : request.published == nullptr &&
                (bootstrap ? request.candidate == nullptr
                           : request.candidate != nullptr);
  if (!pointers_consistent) {
    *detail = "startup verification request has inconsistent route pointers";
    return false;
  }
  if (!valid_runtime_declaration(request.deployment)) {
    *detail = "startup verification request has malformed deployment identity";
    return false;
  }
  return true;
}

bool runtime_deployment_declaration(StartupDeploymentIdentity *declaration,
                                    std::string *detail) {
  if (declaration == nullptr) {
    if (detail != nullptr)
      *detail = "runtime deployment declaration output is null";
    return false;
  }
  RuntimeDeploymentRegistry &registry = runtime_deployment_registry();
  std::lock_guard<std::mutex> guard(registry.mutex);
  if (!registry.declaration.has_value()) {
    if (detail != nullptr)
      *detail = "startup adapter did not install the external deployment "
                "declaration";
    return false;
  }
  *declaration = *registry.declaration;
  if (detail != nullptr) detail->clear();
  return true;
}

}  // namespace

bool declare_server_root_runtime_deployment(
    const StartupDeploymentIdentity &declaration, std::string *error) {
  if (error != nullptr) error->clear();
  if (!valid_runtime_declaration(declaration)) {
    if (error != nullptr)
      *error = "external deployment declaration is malformed";
    return false;
  }
  RuntimeDeploymentRegistry &registry = runtime_deployment_registry();
  std::lock_guard<std::mutex> guard(registry.mutex);
  if (registry.declaration.has_value() &&
      *registry.declaration != declaration) {
    if (error != nullptr)
      *error = "external deployment declaration changed after installation";
    return false;
  }
  registry.declaration = declaration;
  return true;
}

bool configured_server_root_runtime_deployment(
    StartupDeploymentIdentity *declaration, std::string *error) {
  if (error != nullptr) error->clear();
  return runtime_deployment_declaration(declaration, error);
}

#ifdef WESQL_SERVER_ROOT_EVIDENCE_TEST_ONLY
void reset_server_root_runtime_deployment_for_test() {
  RuntimeDeploymentRegistry &registry = runtime_deployment_registry();
  std::lock_guard<std::mutex> guard(registry.mutex);
  registry.declaration.reset();
}
#endif

bool canonicalize_server_root_extents(
    const std::vector<SmartengineExtentRef> &input,
    std::vector<SmartengineExtentRef> *canonical, std::string *error) {
  if (canonical == nullptr) {
    if (error != nullptr) *error = "null canonical extent result";
    return false;
  }

  struct Candidate {
    SmartengineExtentRef ref;
    uint64_t allocation{0};
    uint64_t index{0};
    uint64_t object{0};
  };
  std::vector<Candidate> candidates;
  candidates.reserve(input.size());
  for (const SmartengineExtentRef &ref : input) {
    Candidate candidate;
    candidate.ref = ref;
    if (ref.writer_epoch == 0 || ref.writer_epoch > kJsonSafeIntegerMax ||
        !canonical_decimal(ref.allocation_seq, &candidate.allocation) ||
        !canonical_decimal(ref.index_id, &candidate.index) ||
        !canonical_decimal(ref.object_id, &candidate.object) ||
        !lowercase_even_hex(ref.database_name_hex) || ref.key.empty() ||
        ref.key.size() > kMaxObjectKeyBytes ||
        ref.key.find('\0') != std::string::npos ||
        ref.size == 0 || ref.size > kJsonSafeIntegerMax ||
        !lowercase_sha256(ref.sha256) ||
        ref.format != kSmartengineExtentFormat) {
      if (error != nullptr) *error = "SmartEngine live extent is invalid";
      return false;
    }
    candidates.push_back(std::move(candidate));
  }

  std::sort(candidates.begin(), candidates.end(),
            [](const Candidate &left, const Candidate &right) {
              return std::tie(left.ref.writer_epoch, left.allocation,
                              left.ref.database_name_hex, left.index,
                              left.object, left.ref.key) <
                     std::tie(right.ref.writer_epoch, right.allocation,
                              right.ref.database_name_hex, right.index,
                              right.object, right.ref.key);
            });

  std::set<std::tuple<uint64_t, uint64_t, std::string, uint64_t, uint64_t>>
      identities;
  std::set<std::string> keys;
  std::vector<SmartengineExtentRef> result;
  result.reserve(candidates.size());
  for (size_t ordinal = 0; ordinal < candidates.size(); ++ordinal) {
    Candidate &candidate = candidates[ordinal];
    if (!identities
             .emplace(candidate.ref.writer_epoch, candidate.allocation,
                      candidate.ref.database_name_hex, candidate.index,
                      candidate.object)
             .second) {
      if (error != nullptr)
        *error = "duplicate SmartEngine live extent identity";
      return false;
    }
    if (!keys.emplace(candidate.ref.key).second) {
      if (error != nullptr) *error = "duplicate SmartEngine live extent key";
      return false;
    }
    candidate.ref.ordinal = ordinal;
    result.push_back(std::move(candidate.ref));
  }
  *canonical = std::move(result);
  return true;
}

StartupStepResult compare_server_root_evidence(
    const ServerRootVerificationRequest &request,
    const ServerRootEvidenceObservation &observation,
    StartupRootEvidence *evidence) {
  if (evidence == nullptr)
    return {StartupStepOutcome::CORRUPT,
            "server root evidence output is null"};
  *evidence = {};

  std::string request_error;
  if (!valid_request_shape(request, &request_error))
    return {StartupStepOutcome::CORRUPT, std::move(request_error)};

  bool has_unavailable = false;
  bool has_mismatch = false;
  std::vector<std::string> issues;

  if (require_available(observation.opened_root_matches, "opened root",
                        &has_unavailable, &issues) &&
      !observation.opened_root_matches.value) {
    record_mismatch("opened root", "mysqld did not open the requested root",
                    &has_mismatch, &issues);
  }

  bool cursor_ok = false;
  if (require_available(observation.coherent_cursor, "binlog/GTID cut",
                        &has_unavailable, &issues)) {
    if (!valid_cursor(observation.coherent_cursor.value)) {
      record_mismatch("binlog/GTID cut", "observed cursor is invalid",
                      &has_mismatch, &issues);
    } else {
      cursor_ok = true;
      evidence->recovered_cursor = observation.coherent_cursor.value;
    }
  }

  const Cursor *expected_cursor = nullptr;
  if (request.installed) {
    expected_cursor = &request.published->head.durable_cursor;
  } else if (request.route == StartupCoordinatorRoute::TAKEOVER) {
    expected_cursor = &request.candidate->head.durable_cursor;
  }
  if (cursor_ok && expected_cursor != nullptr &&
      observation.coherent_cursor.value != *expected_cursor) {
    cursor_ok = false;
    record_mismatch("recovered cursor",
                    "cursor differs from the authoritative HEAD",
                    &has_mismatch, &issues);
  }

  bool gtid_ok = false;
  if (require_available(observation.executed_gtid, "executed GTID",
                        &has_unavailable, &issues)) {
    if (!valid_gtid(observation.executed_gtid.value)) {
      record_mismatch("executed GTID", "canonical digest is invalid",
                      &has_mismatch, &issues);
    } else {
      gtid_ok = true;
      evidence->recovered_gtid = observation.executed_gtid.value;
    }
  }
  if (gtid_ok && request.installed &&
      observation.executed_gtid.value !=
          request.published->snapshot.gtid_executed) {
    gtid_ok = false;
    record_mismatch("executed GTID",
                    "digest differs from the published snapshot",
                    &has_mismatch, &issues);
  }
  evidence->gtid_matches = gtid_ok;

  if (require_available(observation.server_uuid, "server UUID",
                        &has_unavailable, &issues)) {
    evidence->server_uuid_matches =
        observation.server_uuid.value == request.deployment.server_uuid;
    if (!evidence->server_uuid_matches)
      record_mismatch("server UUID",
                      "runtime UUID differs from deployment identity",
                      &has_mismatch, &issues);
  }

  if (require_available(observation.declared_deployment,
                        "external deployment declaration", &has_unavailable,
                        &issues)) {
    evidence->configuration_matches =
        observation.declared_deployment.value == request.deployment;
    if (!evidence->configuration_matches)
      record_mismatch(
          "external deployment declaration",
          "runtime declaration differs from the startup request",
          &has_mismatch, &issues);
  }

  if (require_available(observation.dd_matches, "data dictionary",
                        &has_unavailable, &issues)) {
    evidence->dd_matches = observation.dd_matches.value;
    if (!evidence->dd_matches)
      record_mismatch("data dictionary",
                      observation.dd_matches.detail.empty()
                          ? "dictionary is not ready at the current DD version"
                          : observation.dd_matches.detail,
                      &has_mismatch, &issues);
  }

  if (require_available(observation.replication, "replication inventory",
                        &has_unavailable, &issues)) {
    const ServerReplicationInventory &inventory = observation.replication.value;
    evidence->repository_empty =
        inventory.channel_count == 0 && inventory.source_rows == 0 &&
        inventory.relay_rows == 0 && inventory.worker_rows == 0;
    if (!evidence->repository_empty)
      record_mismatch("replication inventory",
                      "channel map or repository is not empty",
                      &has_mismatch, &issues);
  }

  if (require_available(observation.prepared, "prepared transaction inventory",
                        &has_unavailable, &issues)) {
    evidence->internal_prepared_empty =
        observation.prepared.value.internal_entries == 0;
    evidence->external_xa_empty =
        observation.prepared.value.external_entries == 0;
    if (!evidence->internal_prepared_empty)
      record_mismatch("internal prepared inventory", "prepared entries remain",
                      &has_mismatch, &issues);
    if (!evidence->external_xa_empty)
      record_mismatch("external XA inventory", "prepared entries remain",
                      &has_mismatch, &issues);
  }

  const std::vector<SmartengineExtentRef> *expected_extents = nullptr;
  static const std::vector<SmartengineExtentRef> kEmptyExtents;
  if (request.installed) {
    expected_extents = &request.published->snapshot.smartengine_extents;
  } else if (request.route == StartupCoordinatorRoute::BOOTSTRAP) {
    expected_extents = &kEmptyExtents;
  }

  bool engine_cursor_ok = false;
  if (require_available(observation.smartengine_snapshot_cursor,
                        "SmartEngine snapshot cursor", &has_unavailable,
                        &issues)) {
    if (cursor_ok) {
      engine_cursor_ok = observation.smartengine_snapshot_cursor.value ==
                         observation.coherent_cursor.value;
    }
    if (cursor_ok && !engine_cursor_ok)
      record_mismatch("SmartEngine snapshot cursor",
                      "engine snapshot is not at the coherent binlog/GTID cut",
                      &has_mismatch, &issues);
  }

  bool live_set_ok = false;
  if (require_available(observation.smartengine_live_extents,
                        "SmartEngine live extent set", &has_unavailable,
                        &issues)) {
    std::vector<SmartengineExtentRef> actual;
    std::string extent_error;
    if (!canonicalize_server_root_extents(
            observation.smartengine_live_extents.value, &actual,
            &extent_error)) {
      record_mismatch("SmartEngine live extent set", extent_error,
                      &has_mismatch, &issues);
    } else {
      std::vector<PinnedSmartengineExtent> pinned;
      pinned.reserve(actual.size());
      for (const SmartengineExtentRef &extent : actual) {
        pinned.push_back(PinnedSmartengineExtent{
            extent.writer_epoch, extent.allocation_seq,
            extent.database_name_hex, extent.index_id, extent.object_id,
            extent.key, extent.size, extent.sha256});
      }
      if (!startup_extent_set_digest(
              pinned, &evidence->exported_extent_count,
              &evidence->exported_extent_set_sha256, &extent_error)) {
        record_mismatch("SmartEngine live extent digest", extent_error,
                        &has_mismatch, &issues);
      } else {
        live_set_ok = true;
      }
      if (live_set_ok && expected_extents != nullptr) {
        std::vector<SmartengineExtentRef> expected;
        if (!canonicalize_server_root_extents(*expected_extents, &expected,
                                              &extent_error)) {
          live_set_ok = false;
          record_mismatch("authoritative SmartEngine extent set", extent_error,
                          &has_mismatch, &issues);
        } else if (actual != expected) {
          live_set_ok = false;
          record_mismatch(
              "SmartEngine live extent set",
              "canonical live set differs from the authoritative snapshot",
              &has_mismatch, &issues);
        }
      }
    }
  }
  evidence->extent_live_set_matches = engine_cursor_ok && live_set_ok;

  if (request.installed) {
    LocalInstallMarker expected_marker;
    std::string marker_error;
    if (!build_expected_marker(request, &expected_marker, &marker_error)) {
      record_mismatch("installed marker", marker_error, &has_mismatch, &issues);
    } else if (require_available(observation.installed_marker,
                                 "installed marker", &has_unavailable,
                                 &issues)) {
      evidence->marker_matches =
          observation.installed_marker.value.has_value() &&
          *observation.installed_marker.value == expected_marker;
      if (!evidence->marker_matches)
        record_mismatch("installed marker",
                        "marker does not bind the exact HEAD", &has_mismatch,
                        &issues);
    }
    evidence->snapshot_matches =
        cursor_ok && gtid_ok && evidence->extent_live_set_matches;
  }

  if (request.route == StartupCoordinatorRoute::BOOTSTRAP) {
    const auto require_true = [&](const ServerRootObservedValue<bool> &proof,
                                  std::string_view authority) {
      if (require_available(proof, authority, &has_unavailable, &issues) &&
          !proof.value)
        record_mismatch(
            authority,
            proof.detail.empty() ? "bootstrap proof is false" : proof.detail,
            &has_mismatch, &issues);
    };
    require_true(observation.empty_source_scan_stable,
                 "stable EMPTY_SOURCE scan");
    require_true(observation.old_tc_authority_empty,
                 "old transaction-coordinator authority");
    require_true(observation.user_state_empty, "bootstrap user state");
    require_true(observation.legacy_live_extents_empty,
                 "legacy SmartEngine extent boundary");
  }

  if (has_mismatch)
    return {StartupStepOutcome::CORRUPT, joined_issues(issues)};
  if (has_unavailable)
    return {StartupStepOutcome::BLOCKED, joined_issues(issues)};
  return {StartupStepOutcome::READY, {}};
}

#ifndef WESQL_SERVER_ROOT_EVIDENCE_TEST_ONLY
struct RetainedSmartengineSnapshotEvidence::Impl {
  Impl(handlerton *provider_arg, std::unique_ptr<Auto_THD> session_arg,
       uint64_t snapshot_id_arg)
      : provider(provider_arg),
        session(std::move(session_arg)),
        active_snapshot_id(snapshot_id_arg) {}
#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
  explicit Impl(uint64_t snapshot_id_arg)
      : active_snapshot_id(snapshot_id_arg), release_failure_test_token(true) {}
#endif

  handlerton *provider{nullptr};
  std::unique_ptr<Auto_THD> session;
  uint64_t active_snapshot_id{0};
#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
  bool release_failure_test_token{false};
#endif
};

RetainedSmartengineSnapshotEvidence::RetainedSmartengineSnapshotEvidence() =
    default;

RetainedSmartengineSnapshotEvidence::~RetainedSmartengineSnapshotEvidence() {
  (void)release();
}

RetainedSmartengineSnapshotEvidence::RetainedSmartengineSnapshotEvidence(
    RetainedSmartengineSnapshotEvidence &&other) noexcept
    : snapshot_id(other.snapshot_id),
      cursor(std::move(other.cursor)),
      canonical_live_extents(std::move(other.canonical_live_extents)),
      impl_(std::move(other.impl_)) {
  other.snapshot_id = 0;
}

bool RetainedSmartengineSnapshotEvidence::active() const {
  return impl_ != nullptr && impl_->active_snapshot_id != 0;
}

#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
void RetainedSmartengineSnapshotEvidence::arm_release_failure_for_test() {
  snapshot_id = 1;
  impl_ = std::make_unique<Impl>(snapshot_id);
}
#endif

StartupStepResult RetainedSmartengineSnapshotEvidence::release() {
  if (!active()) return {StartupStepOutcome::READY, {}};
  Impl *releasing = impl_.get();
  const uint64_t releasing_id = releasing->active_snapshot_id;
  const bool complete_token =
      releasing->provider != nullptr && releasing->session != nullptr &&
      releasing->session->thd != nullptr;
#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
  const bool release_failure_test_token = releasing->release_failure_test_token;
#else
  constexpr bool release_failure_test_token = false;
#endif
  if (!complete_token && !release_failure_test_token) {
    return {StartupStepOutcome::CORRUPT,
            "SmartEngine backup snapshot lease token is incomplete"};
  }
#ifdef WITH_SMARTENGINE
  DBUG_EXECUTE_IF("remote_commit_fail_smartengine_snapshot_release", {
    return (StartupStepResult{
        StartupStepOutcome::BLOCKED,
        "SmartEngine backup snapshot release failed by debug injection"});
  });
#ifdef WESQL_STARTUP_RELEASE_FAILURE_TEST_ONLY
  if (release_failure_test_token) {
    releasing->active_snapshot_id = 0;
    impl_.reset();
    return {StartupStepOutcome::READY, {}};
  }
#endif
  if (releasing->provider->release_backup_snapshot(releasing->session->thd,
                                                   releasing_id) != 0) {
    return {StartupStepOutcome::BLOCKED,
            "SmartEngine backup snapshot release failed"};
  }
  releasing->active_snapshot_id = 0;
  impl_.reset();
  return {StartupStepOutcome::READY, {}};
#else
  (void)releasing_id;
  return {StartupStepOutcome::CORRUPT,
          "SmartEngine snapshot lease exists in a non-SmartEngine build"};
#endif
}

namespace {

template <typename T>
void set_available(ServerRootObservedValue<T> *observation, T value) {
  observation->available = true;
  observation->value = std::move(value);
  observation->detail.clear();
}

template <typename T>
void set_unavailable(ServerRootObservedValue<T> *observation,
                     std::string detail) {
  observation->available = false;
  observation->value = {};
  observation->detail = std::move(detail);
}

void observe_opened_root(const ServerRootVerificationRequest &request,
                         ServerRootEvidenceObservation *observation) {
  const char *opened = mysql_unpacked_real_data_home[0] != '\0'
                           ? mysql_unpacked_real_data_home
                           : mysql_real_data_home_ptr;
  if (opened == nullptr || opened[0] == '\0') {
    set_unavailable(&observation->opened_root_matches,
                    "mysqld data-directory identity is empty");
    return;
  }
  std::error_code opened_error;
  std::error_code requested_error;
  const fs::path opened_root = fs::weakly_canonical(opened, opened_error);
  const fs::path requested_root =
      fs::weakly_canonical(request.root, requested_error);
  if (opened_error || requested_error) {
    set_unavailable(&observation->opened_root_matches,
                    "cannot resolve mysqld and requested data directories");
    return;
  }
  set_available(&observation->opened_root_matches,
                opened_root == requested_root);
}

bool capture_cursor(Cursor *cursor, std::string *detail) {
  if (cursor == nullptr || !mysql_bin_log.is_open()) {
    *detail = "binary log is not open";
    return false;
  }
  Log_info info;
  if (mysql_bin_log.get_current_log(&info) != 0) {
    *detail = "cannot sample the current binary log";
    return false;
  }
  cursor->file = fs::path(info.log_file_name).filename().string();
  cursor->pos = static_cast<uint64_t>(info.pos);
  if (!valid_cursor(*cursor)) {
    *detail = "current binary-log cursor is invalid";
    return false;
  }
  return true;
}

bool capture_gtid(GtidSetDigest *gtid, std::string *detail) {
  if (gtid == nullptr || global_tsid_lock == nullptr || gtid_state == nullptr) {
    *detail = "executed GTID state is not initialized";
    return false;
  }
  char *text = nullptr;
  global_tsid_lock->wrlock();
  const long length =
      gtid_state->get_executed_gtids()->to_string(&text, false, nullptr);
  global_tsid_lock->unlock();
  if (length < 0 || text == nullptr) {
    *detail = "cannot allocate the executed GTID snapshot";
    return false;
  }
  std::string digest_error;
  const bool success =
      gtid_digest(std::string_view(text, static_cast<size_t>(length)), gtid,
                  &digest_error);
  my_free(text);
  if (!success) {
    *detail = "cannot canonicalize executed GTIDs";
    if (!digest_error.empty()) detail->append(": ").append(digest_error);
  }
  return success;
}

bool capture_coherent_cut(Cursor *cursor, GtidSetDigest *gtid,
                          std::string *detail) {
  if (cursor == nullptr || gtid == nullptr || detail == nullptr) return false;
  Cursor before;
  Cursor after;
  if (!capture_cursor(&before, detail) || !capture_gtid(gtid, detail) ||
      !capture_cursor(&after, detail))
    return false;
  if (before != after) {
    *detail = "binary-log cursor moved while sampling executed GTIDs";
    return false;
  }
  *cursor = std::move(after);
  return true;
}

void observe_coherent_cut(ServerRootEvidenceObservation *observation) {
  Cursor cursor;
  GtidSetDigest gtid;
  std::string detail;
  if (!capture_coherent_cut(&cursor, &gtid, &detail)) {
    set_unavailable(&observation->coherent_cursor, detail);
    set_unavailable(&observation->executed_gtid, detail);
    return;
  }
  set_available(&observation->coherent_cursor, std::move(cursor));
  set_available(&observation->executed_gtid, std::move(gtid));
}

struct PersistentUserTableEngine {
  std::string schema;
  std::string table;
  std::string engine;

  bool operator==(const PersistentUserTableEngine &) const = default;
};

struct DataDictionaryInventory {
  bool dictionary_present{false};
  dd::bootstrap::Stage stage{dd::bootstrap::Stage::NOT_STARTED};
  uint actual_dd_version{0};
  bool schema_names_fetched{false};
  std::vector<std::string> persistent_schemas;
  bool table_engines_fetched{false};
  std::vector<PersistentUserTableEngine> persistent_user_tables;

  bool supported_table_engines() const {
    return table_engines_fetched &&
           std::all_of(persistent_user_tables.begin(),
                       persistent_user_tables.end(), [](const auto &table) {
                         return table_engine_allowed(
                             TableEngineScope::PERSISTENT_USER, table.engine);
                       });
  }

  bool healthy() const {
    return dictionary_present && stage == dd::bootstrap::Stage::FINISHED &&
           actual_dd_version == dd::DD_VERSION && schema_names_fetched &&
           supported_table_engines();
  }

  bool has_canonical_bootstrap_schemas() const {
    static const std::vector<std::string> kExpected{"mysql", "sys"};
    return healthy() && persistent_schemas == kExpected;
  }

  bool operator==(const DataDictionaryInventory &) const = default;
};

std::string data_dictionary_health_detail(
    const DataDictionaryInventory &inventory) {
  if (!inventory.dictionary_present ||
      inventory.stage != dd::bootstrap::Stage::FINISHED ||
      inventory.actual_dd_version != dd::DD_VERSION)
    return "the dictionary, bootstrap stage, or DD version is not current";
  if (!inventory.schema_names_fetched)
    return "persistent data-dictionary schemas were not enumerated";
  if (!inventory.table_engines_fetched)
    return "persistent user table engines were not enumerated";
  const auto unsupported = std::find_if(
      inventory.persistent_user_tables.begin(),
      inventory.persistent_user_tables.end(), [](const auto &table) {
        return !table_engine_allowed(TableEngineScope::PERSISTENT_USER,
                                     table.engine);
      });
  if (unsupported == inventory.persistent_user_tables.end()) return {};

  std::string detail{"persistent user table `"};
  detail.append(unsupported->schema)
      .append("`.`")
      .append(unsupported->table)
      .append("` uses unsupported engine ")
      .append(unsupported->engine.empty() ? "<unknown>" : unsupported->engine);
  return detail;
}

bool capture_data_dictionary_inventory(THD *thd,
                                       DataDictionaryInventory *inventory,
                                       std::string *detail) {
  if (thd == nullptr || inventory == nullptr || detail == nullptr) return false;
  *inventory = {};
  inventory->dictionary_present = dd::get_dictionary() != nullptr;
  const dd::bootstrap::DD_bootstrap_ctx &bootstrap =
      dd::bootstrap::DD_bootstrap_ctx::instance();
  inventory->stage = bootstrap.get_stage();
  inventory->actual_dd_version = bootstrap.get_actual_dd_version();

  if (!inventory->dictionary_present ||
      inventory->stage != dd::bootstrap::Stage::FINISHED ||
      inventory->actual_dd_version != dd::DD_VERSION) {
    return true;
  }
  if (thd->dd_client() == nullptr) {
    *detail = "data-dictionary client is not initialized";
    return false;
  }

  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  std::vector<const dd::Schema *> schemas;
  if (thd->dd_client()->fetch_global_components<dd::Schema>(&schemas)) {
    *detail = "cannot enumerate persistent data-dictionary schemas";
    return false;
  }
  std::map<dd::Object_id, std::string> schema_names_by_id;
  for (const dd::Schema *schema : schemas) {
    if (schema == nullptr) {
      *detail = "persistent data-dictionary schema inventory is invalid";
      return false;
    }
    const std::string schema_name(schema->name().data(), schema->name().size());
    if (!schema_names_by_id.emplace(schema->id(), schema_name).second) {
      *detail = "persistent data-dictionary schema inventory is invalid";
      return false;
    }
    inventory->persistent_schemas.push_back(schema_name);
  }
  std::sort(inventory->persistent_schemas.begin(),
            inventory->persistent_schemas.end());
  inventory->schema_names_fetched = true;

  std::vector<const dd::Table *> tables;
  if (thd->dd_client()->fetch_global_components<dd::Table>(&tables)) {
    *detail = "cannot enumerate persistent data-dictionary tables";
    return false;
  }
  for (const dd::Table *table : tables) {
    if (table == nullptr) {
      *detail = "persistent data-dictionary table inventory contains null";
      return false;
    }
    const auto schema = schema_names_by_id.find(table->schema_id());
    if (schema == schema_names_by_id.end()) {
      *detail = "persistent data-dictionary table has no schema authority";
      return false;
    }
    if (table->is_temporary() ||
        table->hidden() != dd::Abstract_table::HT_VISIBLE)
      continue;

    const LEX_CSTRING db{schema->second.c_str(), schema->second.size()};
    const LEX_CSTRING name{table->name().c_str(), table->name().size()};
    if (get_table_category(db, name) != TABLE_CATEGORY_USER) continue;
    inventory->persistent_user_tables.push_back({
        schema->second,
        std::string(table->name().data(), table->name().size()),
        std::string(table->engine().data(), table->engine().size())});
  }
  std::sort(inventory->persistent_user_tables.begin(),
            inventory->persistent_user_tables.end(),
            [](const auto &left, const auto &right) {
              return std::tie(left.schema, left.table, left.engine) <
                     std::tie(right.schema, right.table, right.engine);
            });
  inventory->table_engines_fetched = true;
  return true;
}

struct AccountIdentity {
  std::string user;
  std::string host;

  bool operator==(const AccountIdentity &) const = default;
};

struct AccountState {
  AccountIdentity identity;
  Access_bitmask global_access{0};
  std::string plugin;
  std::string primary_authentication;
  std::string secondary_authentication;
  bool password_expired{false};
  bool account_locked{false};
  bool is_role{false};
  int ssl_type{SSL_TYPE_NOT_SPECIFIED};
  std::string ssl_cipher;
  std::string x509_issuer;
  std::string x509_subject;
  std::array<uint, 5> resource_limits{};
  uint password_lifetime{0};
  bool use_default_password_lifetime{false};
  uint32 password_history_length{0};
  bool use_default_password_history{false};
  uint32 password_reuse_interval{0};
  bool use_default_password_reuse_interval{false};
  int password_require_current{0};
  bool password_lock_active{false};
  bool password_lock_default{false};
  bool has_mfa{false};

  bool operator==(const AccountState &) const = default;
};

bool account_less(const AccountState &left, const AccountState &right) {
  return std::tie(left.identity.user, left.identity.host) <
         std::tie(right.identity.user, right.identity.host);
}

bool canonical_bootstrap_identity(std::string_view user,
                                  std::string_view host) {
  static constexpr std::array<std::pair<std::string_view, std::string_view>, 4>
      kExpected{{{"mysql.infoschema", "localhost"},
                 {"mysql.session", "localhost"},
                 {"mysql.sys", "localhost"},
                 {"root", "localhost"}}};
  return std::find(kExpected.begin(), kExpected.end(),
                   std::pair<std::string_view, std::string_view>{user, host}) !=
         kExpected.end();
}

struct GrantReference {
  std::string kind;
  AccountIdentity owner;
  AccountIdentity target;
  std::string object;
  Access_bitmask access{0};
  Access_bitmask column_access{0};
  size_t column_count{0};
  bool with_grant{false};

  bool operator==(const GrantReference &) const = default;
};

bool grant_less(const GrantReference &left, const GrantReference &right) {
  return std::tie(left.kind, left.owner.user, left.owner.host,
                  left.target.user, left.target.host, left.object, left.access,
                  left.column_access, left.column_count, left.with_grant) <
         std::tie(right.kind, right.owner.user, right.owner.host,
                  right.target.user, right.target.host, right.object,
                  right.access, right.column_access, right.column_count,
                  right.with_grant);
}

AccountState canonical_account(std::string user, Access_bitmask access,
                               std::string authentication, bool locked) {
  AccountState account;
  account.identity = {std::move(user), "localhost"};
  account.global_access = access;
  account.plugin = "caching_sha2_password";
  account.primary_authentication = std::move(authentication);
  account.account_locked = locked;
  account.ssl_type = SSL_TYPE_NONE;
  account.use_default_password_lifetime = true;
  account.use_default_password_history = true;
  account.use_default_password_reuse_interval = true;
  account.password_require_current =
      static_cast<int>(Lex_acl_attrib_udyn::DEFAULT);
  account.password_lock_default = true;
  return account;
}

GrantReference canonical_grant(std::string kind, AccountIdentity owner,
                               std::string object, Access_bitmask access = 0,
                               bool with_grant = false,
                               AccountIdentity target = {}) {
  GrantReference grant;
  grant.kind = std::move(kind);
  grant.owner = std::move(owner);
  grant.target = std::move(target);
  grant.object = std::move(object);
  grant.access = access;
  grant.with_grant = with_grant;
  return grant;
}

struct AclInventory {
  bool initialized{false};
  bool caches_complete{false};
  std::vector<AccountState> accounts;
  std::vector<GrantReference> grants;
  std::vector<std::string> registered_dynamic_privileges;
  size_t role_edges{0};
  size_t default_roles{0};
  size_t mandatory_roles{0};
  size_t restrictions{0};

  bool canonical_initialize_insecure_state() const {
    static constexpr std::string_view kInvalidSystemPassword =
        "$A$005$THISISACOMBINATIONOFINVALIDSALTANDPASSWORDTHATMUSTNEVERBRBEUSED";
    const std::vector<AccountState> expected_accounts{
        canonical_account("mysql.infoschema", SELECT_ACL,
                          std::string(kInvalidSystemPassword), true),
        canonical_account("mysql.session", SUPER_ACL | SHUTDOWN_ACL,
                          std::string(kInvalidSystemPassword), true),
        canonical_account("mysql.sys", 0, std::string(kInvalidSystemPassword),
                          true),
        canonical_account("root", GLOBAL_ACLS, "", false)};
    if (!initialized || !caches_complete || accounts != expected_accounts ||
        role_edges != 0 || default_roles != 0 || mandatory_roles != 0 ||
        restrictions != 0)
      return false;

    const AccountIdentity infoschema{"mysql.infoschema", "localhost"};
    const AccountIdentity session{"mysql.session", "localhost"};
    const AccountIdentity mysql_sys{"mysql.sys", "localhost"};
    const AccountIdentity root{"root", "localhost"};
    std::vector<GrantReference> expected_grants{
        canonical_grant("database", session, "performance_schema", SELECT_ACL),
        canonical_grant("database", mysql_sys, "sys", TRIGGER_ACL),
        canonical_grant("proxy", root, "", 0, true, {}),
        canonical_grant("table", session, std::string("mysql\0user", 10),
                        SELECT_ACL),
        canonical_grant("table", mysql_sys,
                        std::string("sys\0sys_config", 14), SELECT_ACL)};

    const auto append_dynamic = [&](const AccountIdentity &owner,
                                    std::string_view privilege,
                                    bool with_grant) {
      expected_grants.push_back(canonical_grant(
          "dynamic", owner, std::string(privilege), 0, with_grant));
    };
    static constexpr std::array<std::string_view, 3> kInfoSchemaDynamic{
        "AUDIT_ABORT_EXEMPT", "FIREWALL_EXEMPT", "SYSTEM_USER"};
    for (const std::string_view privilege : kInfoSchemaDynamic)
      append_dynamic(infoschema, privilege, false);
    static constexpr std::array<std::string_view, 10> kSessionDynamic{
        "AUDIT_ABORT_EXEMPT",        "AUTHENTICATION_POLICY_ADMIN",
        "BACKUP_ADMIN",              "CLONE_ADMIN",
        "CONNECTION_ADMIN",          "FIREWALL_EXEMPT",
        "PERSIST_RO_VARIABLES_ADMIN", "SESSION_VARIABLES_ADMIN",
        "SYSTEM_USER",               "SYSTEM_VARIABLES_ADMIN"};
    for (const std::string_view privilege : kSessionDynamic)
      append_dynamic(session, privilege, false);
    for (const std::string_view privilege : kInfoSchemaDynamic)
      append_dynamic(mysql_sys, privilege, false);

    std::set<std::string> root_dynamic(registered_dynamic_privileges.begin(),
                                       registered_dynamic_privileges.end());
    // These are inserted by initialization even when their providers have not
    // registered them yet.
    root_dynamic.emplace("AUDIT_ABORT_EXEMPT");
    root_dynamic.emplace("FIREWALL_EXEMPT");
    for (const std::string &privilege : root_dynamic)
      append_dynamic(root, privilege, true);

    std::sort(expected_grants.begin(), expected_grants.end(), grant_less);
    if (grants != expected_grants) return false;
    for (const GrantReference &grant : grants) {
      if (!canonical_bootstrap_identity(grant.owner.user, grant.owner.host))
        return false;
    }
    return true;
  }

  bool operator==(const AclInventory &) const = default;
};

class AccountVisitor final : public ACL_USER_visitor {
 public:
  explicit AccountVisitor(std::vector<AccountState> *accounts)
      : accounts_(accounts) {}

  void visit(const ACL_USER *account) override {
    const auto copied = [](const LEX_CSTRING &value) {
      return value.str == nullptr ? std::string{}
                                  : std::string(value.str, value.length);
    };
    AccountState state;
    state.identity = {account->user == nullptr ? "" : account->user,
                      account->host.get_host()};
    state.global_access = account->access;
    state.plugin = copied(account->plugin);
    state.primary_authentication =
        copied(account->credentials[PRIMARY_CRED].m_auth_string);
    state.secondary_authentication =
        copied(account->credentials[SECOND_CRED].m_auth_string);
    state.password_expired = account->password_expired;
    state.account_locked = account->account_locked;
    state.is_role = account->is_role;
    state.ssl_type = static_cast<int>(account->ssl_type);
    state.ssl_cipher = account->ssl_cipher == nullptr ? "" : account->ssl_cipher;
    state.x509_issuer = account->x509_issuer == nullptr ? "" : account->x509_issuer;
    state.x509_subject =
        account->x509_subject == nullptr ? "" : account->x509_subject;
    state.resource_limits = {
        account->user_resource.questions, account->user_resource.updates,
        account->user_resource.conn_per_hour, account->user_resource.user_conn,
        account->user_resource.specified_limits};
    state.password_lifetime = account->password_lifetime;
    state.use_default_password_lifetime =
        account->use_default_password_lifetime;
    state.password_history_length = account->password_history_length;
    state.use_default_password_history = account->use_default_password_history;
    state.password_reuse_interval = account->password_reuse_interval;
    state.use_default_password_reuse_interval =
        account->use_default_password_reuse_interval;
    state.password_require_current =
        static_cast<int>(account->password_require_current);
    state.password_lock_active = account->password_locked_state.is_active();
    state.password_lock_default = account->password_locked_state.is_default();
    state.has_mfa = account->m_mfa != nullptr;
    accounts_->push_back(std::move(state));
  }

 private:
  std::vector<AccountState> *accounts_;
};

bool capture_acl_inventory(THD *thd, AclInventory *inventory,
                           std::string *detail) {
  if (thd == nullptr || inventory == nullptr || detail == nullptr) return false;
  *inventory = {};

  Acl_cache_lock_guard cache_lock(thd, Acl_cache_lock_mode::READ_MODE);
  if (!cache_lock.lock(false)) {
    *detail = "cannot acquire the ACL cache read lock";
    return false;
  }

  inventory->initialized = is_acl_inited();
  User_to_dynamic_privileges_map *dynamic = get_dynamic_privileges_map();
  Dynamic_privilege_register *registered = get_dynamic_privilege_register();
  inventory->caches_complete =
      acl_dbs != nullptr && acl_proxy_users != nullptr &&
      column_priv_hash != nullptr && proc_priv_hash != nullptr &&
      func_priv_hash != nullptr && library_priv_hash != nullptr &&
      dynamic != nullptr && registered != nullptr && g_granted_roles != nullptr &&
      g_default_roles != nullptr && g_mandatory_roles != nullptr &&
      acl_restrictions != nullptr;
  if (!inventory->initialized || !inventory->caches_complete) return true;

  AccountVisitor visitor(&inventory->accounts);
  acl_users_accept(&visitor);
  std::sort(inventory->accounts.begin(), inventory->accounts.end(),
            account_less);
  inventory->registered_dynamic_privileges.assign(registered->begin(),
                                                   registered->end());
  std::sort(inventory->registered_dynamic_privileges.begin(),
            inventory->registered_dynamic_privileges.end());

  for (const ACL_DB &grant : *acl_dbs) {
    inventory->grants.push_back(
        {"database",
         {grant.user == nullptr ? "" : grant.user, grant.host.get_host()},
         {}, grant.db == nullptr ? "" : grant.db, grant.access});
  }
  for (ACL_PROXY_USER &grant : *acl_proxy_users) {
    inventory->grants.push_back(
        {"proxy",
         {grant.get_user() == nullptr ? "" : grant.get_user(),
          grant.host.get_host()},
         {grant.get_proxied_user() == nullptr ? "" : grant.get_proxied_user(),
          grant.get_proxied_host() == nullptr ? "" : grant.get_proxied_host()},
         {}, 0, 0, 0, grant.get_with_grant()});
  }

  const auto append_named_grants = [&](const char *kind, const auto &grants) {
    for (const auto &entry : grants) {
      const GRANT_NAME *grant = entry.second.get();
      if (grant == nullptr) {
        inventory->caches_complete = false;
        continue;
      }
      std::string object = grant->db == nullptr ? "" : grant->db;
      object.push_back('\0');
      if (grant->tname != nullptr) object.append(grant->tname);
      inventory->grants.push_back(
          {kind,
           {grant->user == nullptr ? "" : grant->user,
            grant->host.get_host()},
           {}, std::move(object), grant->privs});
      if (const auto *table = dynamic_cast<const GRANT_TABLE *>(grant);
          table != nullptr) {
        inventory->grants.back().column_access = table->cols;
        inventory->grants.back().column_count = table->hash_columns.size();
      }
    }
  };
  append_named_grants("table", *column_priv_hash);
  append_named_grants("procedure", *proc_priv_hash);
  append_named_grants("function", *func_priv_hash);
  append_named_grants("library", *library_priv_hash);

  for (const auto &entry : *dynamic) {
    std::string privilege = entry.second.first;
    inventory->grants.push_back(
        {"dynamic", {entry.first.user(), entry.first.host()}, {},
         std::move(privilege), 0, 0, 0, entry.second.second});
  }
  std::sort(inventory->grants.begin(), inventory->grants.end(), grant_less);

  inventory->role_edges = boost::num_edges(*g_granted_roles);
  inventory->default_roles = g_default_roles->size();
  inventory->mandatory_roles = g_mandatory_roles->size();
  inventory->restrictions = acl_restrictions->size();
  return true;
}

bool count_repository_table(THD *thd, const LEX_CSTRING table_name,
                            uint field_count, uint64_t *rows,
                            std::string *detail) {
  if (thd == nullptr || table_name.str == nullptr || rows == nullptr) {
    *detail = "invalid persisted replication repository count request";
    return false;
  }

  Rpl_info_table_access access;
  Open_tables_backup backup;
  TABLE *table = nullptr;
  const bool open_error = access.open_table(
      thd, MYSQL_SCHEMA_NAME, table_name, field_count, TL_READ, &table, &backup);
  if (open_error) {
    const bool close_error = access.close_table(thd, table, &backup, true);
    *detail = "cannot open persisted replication repository " +
              std::string(table_name.str, table_name.length);
    if (close_error) detail->append(" or restore its table state");
    return false;
  }
  ulonglong count = 0;
  const bool count_error = access.count_info(table, &count);
  const bool close_error =
      access.close_table(thd, table, &backup, count_error);
  if (count_error || close_error) {
    *detail = "cannot count persisted replication repository " +
              std::string(table_name.str, table_name.length);
    return false;
  }
  *rows = static_cast<uint64_t>(count);
  return true;
}

bool capture_replication_inventory(THD *thd,
                                   ServerReplicationInventory *inventory,
                                   std::string *detail) {
  if (inventory == nullptr) {
    *detail = "replication inventory output is null";
    return false;
  }
  *inventory = {};

  // The channel-map read lock excludes every supported channel create/remove
  // path for the complete repository scan. With no mapped channel, no replica
  // thread owns a repository writer; TL_READ then gives each persisted table a
  // read lock while its exact row count is sampled.
  channel_map.rdlock();
  auto unlock_channel_map =
      create_scope_guard([] { channel_map.unlock(); });
  inventory->channel_count = channel_map.get_num_instances(true);
  if (!count_repository_table(
          thd, MI_INFO_NAME,
          static_cast<uint>(Master_info::get_number_info_mi_fields()),
          &inventory->source_rows, detail) ||
      !count_repository_table(
          thd, RLI_INFO_NAME,
          static_cast<uint>(Relay_log_info::get_number_info_rli_fields()),
          &inventory->relay_rows, detail) ||
      !count_repository_table(
          thd, WORKER_INFO_NAME,
          static_cast<uint>(Slave_worker::get_number_worker_fields()),
          &inventory->worker_rows, detail)) {
    return false;
  }
  if (inventory->channel_count != channel_map.get_num_instances(true)) {
    *detail = "replication channel map changed while its read lock was held";
    return false;
  }
  return true;
}

bool capture_prepared_inventory(ServerPreparedInventory *result) {
  if (result == nullptr) return false;
  xa::recovery::Prepared_xid_inventory inventory;
  if (xa::recovery::enumerate_prepared_transactions(&inventory)) return false;
  *result = ServerPreparedInventory{inventory.internal_entries,
                                    inventory.external_entries};
  return true;
}

void observe_prepared(ServerRootEvidenceObservation *observation) {
  ServerPreparedInventory inventory;
  if (!capture_prepared_inventory(&inventory)) {
    set_unavailable(&observation->prepared,
                    "storage-engine prepared enumeration failed");
  } else {
    set_available(&observation->prepared, std::move(inventory));
  }
}

void append_detail(std::string_view issue, std::string *detail) {
  if (detail == nullptr || issue.empty()) return;
  if (!detail->empty()) detail->append("; ");
  detail->append(issue);
}

struct OldTcInventory {
  bool tc_is_binlog{false};
  bool tc_log_absent{false};
  bool binlog_open{false};
  bool prepared_xids_drained{false};
  bool index_complete{false};
  bool exactly_one_active_log{false};
  bool executed_gtid_empty{false};
  bool seed_valid{false};
  bool previous_gtids_empty{false};
  Cursor cursor;
  std::vector<std::string> index_files;
  std::vector<int> seed_event_types;

  bool empty() const {
    return tc_is_binlog && tc_log_absent && binlog_open &&
           prepared_xids_drained && index_complete &&
           exactly_one_active_log && executed_gtid_empty && seed_valid &&
           previous_gtids_empty;
  }

  bool operator==(const OldTcInventory &) const = default;
};

bool capture_old_tc_inventory(const ServerRootVerificationRequest &request,
                              const Cursor &cursor,
                              const GtidSetDigest &executed_gtid,
                              bool prepared_xids_drained,
                              OldTcInventory *inventory,
                              std::string *detail) {
  if (inventory == nullptr || detail == nullptr) return false;
  *inventory = {};
  inventory->cursor = cursor;
  inventory->tc_is_binlog = tc_log == &mysql_bin_log;
  inventory->binlog_open = mysql_bin_log.is_open();
  inventory->prepared_xids_drained = prepared_xids_drained;
  inventory->executed_gtid_empty = executed_gtid.canonical.empty();
  if (!inventory->tc_is_binlog)
    append_detail("the active transaction coordinator is not the binary log",
                  detail);
  if (!inventory->binlog_open)
    append_detail("the binary log is not open", detail);
  if (!inventory->executed_gtid_empty)
    append_detail("the executed GTID set is not empty", detail);

  std::error_code status_error;
  const fs::file_status tc_status =
      fs::symlink_status(request.root / "tc.log", status_error);
  if (status_error) {
    append_detail("cannot inspect the legacy tc.log path", detail);
  } else {
    inventory->tc_log_absent = tc_status.type() == fs::file_type::not_found;
    if (!inventory->tc_log_absent)
      append_detail("legacy tc.log exists", detail);
  }

  auto [index_error, files] = mysql_bin_log.get_log_index();
  inventory->index_complete = index_error == LOG_INFO_EOF;
  inventory->index_files.assign(files.begin(), files.end());
  if (!inventory->index_complete)
    append_detail("cannot enumerate the complete binary-log index", detail);
  inventory->exactly_one_active_log =
      inventory->index_complete && inventory->index_files.size() == 1 &&
      mysql_bin_log.is_active(inventory->index_files.front().c_str()) &&
      fs::path(inventory->index_files.front()).filename().string() ==
          cursor.file;
  if (!inventory->exactly_one_active_log) {
    append_detail("the index does not contain exactly one active binary log",
                  detail);
    return true;
  }

  Binlog_file_reader reader(true);
  if (reader.open(inventory->index_files.front().c_str())) {
    append_detail(std::string("cannot open the active binary log: ") +
                      reader.get_error_str(),
                  detail);
    return true;
  }

  Tsid_map previous_map(nullptr);
  Gtid_set previous_gtids(&previous_map, nullptr);
  while (Log_event *raw_event = reader.read_event_object()) {
    std::unique_ptr<Log_event> event(raw_event);
    const auto event_type = event->get_type_code();
    inventory->seed_event_types.push_back(static_cast<int>(event_type));
    if (event_type == mysql::binlog::event::PREVIOUS_GTIDS_LOG_EVENT) {
      const auto *previous =
          dynamic_cast<const Previous_gtids_log_event *>(event.get());
      if (previous == nullptr || previous->add_to_set(&previous_gtids) != 0) {
        append_detail("cannot decode the Previous-GTIDs seed event", detail);
        return true;
      }
    }
  }
  if (reader.get_error_type() != Binlog_read_error::READ_EOF) {
    append_detail(std::string("cannot parse the active binary log: ") +
                      reader.get_error_str(),
                  detail);
    return true;
  }
  if (static_cast<uint64_t>(reader.position()) != cursor.pos) {
    append_detail("the parsed binary-log end differs from the sampled cursor",
                  detail);
    return true;
  }

  static const std::vector<int> kExpectedEvents{
      static_cast<int>(mysql::binlog::event::FORMAT_DESCRIPTION_EVENT),
      static_cast<int>(mysql::binlog::event::PREVIOUS_GTIDS_LOG_EVENT)};
  inventory->previous_gtids_empty = previous_gtids.is_empty();
  inventory->seed_valid =
      inventory->seed_event_types == kExpectedEvents &&
      inventory->previous_gtids_empty;
  if (!inventory->previous_gtids_empty)
    append_detail("the Previous-GTIDs seed event is not empty", detail);
  if (inventory->seed_event_types != kExpectedEvents)
    append_detail(
        "the active binary log is not exactly FDE plus Previous-GTIDs",
        detail);
  return true;
}

class TwoStageGlobalReadLock {
 public:
  explicit TwoStageGlobalReadLock(THD *thd) : thd_(thd) {}
  ~TwoStageGlobalReadLock() { release(); }

  bool acquire(std::string *detail) {
    if (thd_ == nullptr || detail == nullptr) return false;
    if (thd_->global_read_lock.lock_global_read_lock(thd_)) {
      *detail = "cannot acquire the global read lock";
      return false;
    }
    acquired_ = true;
    if (thd_->global_read_lock.make_global_read_lock_block_commit(thd_)) {
      *detail = "cannot make the global read lock block commits";
      release();
      return false;
    }
    return true;
  }

  void release() {
    if (!acquired_) return;
    thd_->global_read_lock.unlock_global_read_lock(thd_);
    acquired_ = false;
  }

 private:
  THD *thd_{nullptr};
  bool acquired_{false};
};

struct BootstrapServerSample {
  Cursor cursor;
  GtidSetDigest executed_gtid;
  std::string server_uuid;
  DataDictionaryInventory dictionary;
  ServerReplicationInventory replication;
  AclInventory acl;
  ServerPreparedInventory prepared;
  OldTcInventory old_tc;

  bool operator==(const BootstrapServerSample &) const = default;
};

bool capture_bootstrap_server_sample(
    const ServerRootVerificationRequest &request, THD *thd,
    bool prepared_xids_drained, BootstrapServerSample *sample,
    std::string *detail) {
  if (thd == nullptr || sample == nullptr || detail == nullptr) return false;
  *sample = {};
  if (!capture_coherent_cut(&sample->cursor, &sample->executed_gtid, detail))
    return false;
  if (server_uuid_ptr == nullptr || server_uuid_ptr[0] == '\0') {
    *detail = "mysqld server UUID is not initialized";
    return false;
  }
  sample->server_uuid = server_uuid_ptr;
  if (!capture_data_dictionary_inventory(thd, &sample->dictionary, detail) ||
      !capture_replication_inventory(thd, &sample->replication, detail) ||
      !capture_acl_inventory(thd, &sample->acl, detail) ||
      !capture_prepared_inventory(&sample->prepared) ||
      !capture_old_tc_inventory(request, sample->cursor,
                                sample->executed_gtid,
                                prepared_xids_drained, &sample->old_tc,
                                detail)) {
    if (detail->empty())
      *detail = "cannot capture a complete bootstrap server sample";
    return false;
  }
  return true;
}

void set_authoritative_bool(ServerRootObservedValue<bool> *observation,
                            bool value, std::string detail = {}) {
  observation->available = true;
  observation->value = value;
  observation->detail = std::move(detail);
}

void observe_data_dictionary(THD *thd,
                             ServerRootEvidenceObservation *observation) {
  DataDictionaryInventory inventory;
  std::string detail;
  if (!capture_data_dictionary_inventory(thd, &inventory, &detail)) {
    set_unavailable(&observation->dd_matches, std::move(detail));
    return;
  }
  if (!inventory.healthy()) detail = data_dictionary_health_detail(inventory);
  set_authoritative_bool(&observation->dd_matches, inventory.healthy(),
                         std::move(detail));
}

void observe_replication(THD *thd,
                         ServerRootEvidenceObservation *observation) {
  ServerReplicationInventory inventory;
  std::string detail;
  if (!capture_replication_inventory(thd, &inventory, &detail)) {
    set_unavailable(&observation->replication, std::move(detail));
    return;
  }
  set_available(&observation->replication, std::move(inventory));
}

void publish_bootstrap_sample(const BootstrapServerSample &sample,
                              std::string old_tc_detail,
                              bool stable,
                              std::string stable_detail,
                              ServerRootEvidenceObservation *observation) {
  set_available(&observation->coherent_cursor, sample.cursor);
  set_available(&observation->executed_gtid, sample.executed_gtid);
  set_available(&observation->server_uuid, sample.server_uuid);

  std::string dd_detail;
  if (!sample.dictionary.healthy())
    dd_detail = data_dictionary_health_detail(sample.dictionary);
  set_authoritative_bool(&observation->dd_matches,
                         sample.dictionary.healthy(), std::move(dd_detail));
  set_available(&observation->replication, sample.replication);
  set_available(&observation->prepared, sample.prepared);

  const bool canonical_user_state =
      sample.dictionary.has_canonical_bootstrap_schemas() &&
      sample.acl.canonical_initialize_insecure_state();
  std::string user_detail;
  if (!sample.dictionary.has_canonical_bootstrap_schemas())
    append_detail("persistent schemas are not exactly mysql and sys",
                  &user_detail);
  if (!sample.acl.canonical_initialize_insecure_state())
    append_detail(
        "accounts, grants, restrictions, or roles differ from initialization",
        &user_detail);
  set_authoritative_bool(&observation->user_state_empty,
                         canonical_user_state, std::move(user_detail));
  set_authoritative_bool(&observation->old_tc_authority_empty,
                         sample.old_tc.empty(), std::move(old_tc_detail));
  set_authoritative_bool(&observation->empty_source_scan_stable, stable,
                         std::move(stable_detail));
}

void set_bootstrap_scan_unavailable(
    std::string detail, ServerRootEvidenceObservation *observation) {
  set_unavailable(&observation->coherent_cursor, detail);
  set_unavailable(&observation->executed_gtid, detail);
  set_unavailable(&observation->dd_matches, detail);
  set_unavailable(&observation->replication, detail);
  set_unavailable(&observation->prepared, detail);
  set_unavailable(&observation->empty_source_scan_stable, detail);
  set_unavailable(&observation->old_tc_authority_empty, detail);
  set_unavailable(&observation->user_state_empty, std::move(detail));
}

void observe_installed_marker(const ServerRootVerificationRequest &request,
                              ServerRootEvidenceObservation *observation) {
  if (!request.installed) return;
  LocalInstallMarker expected;
  std::string detail;
  if (!build_expected_marker(request, &expected, &detail)) {
    set_unavailable(&observation->installed_marker, std::move(detail));
    return;
  }
  const TargetClassification classification =
      classify_local_target(request.root, expected, true);
  if (!classification.detail.empty() &&
      classification.classification == TargetClass::FOREIGN_OR_CORRUPT) {
    set_available(&observation->installed_marker,
                  std::optional<LocalInstallMarker>{});
    return;
  }
  set_available(&observation->installed_marker, classification.marker);
}

#ifdef WITH_SMARTENGINE
struct SmartengineProviderSearch {
  handlerton *provider{nullptr};
  size_t complete_count{0};
  bool partial{false};
};

bool find_smartengine_provider(THD *, plugin_ref plugin, void *argument) {
  auto *search = static_cast<SmartengineProviderSearch *>(argument);
  handlerton *hton = plugin_data<handlerton *>(plugin);
  if (hton == nullptr || hton->state != SHOW_OPTION_YES) return false;
  const bool create = hton->create_backup_snapshot != nullptr;
  const bool export_live_set =
      hton->export_backup_snapshot_live_set != nullptr;
  const bool release = hton->release_backup_snapshot != nullptr;
  if (create || export_live_set || release) {
    if (!(create && export_live_set && release)) {
      search->partial = true;
    } else {
      search->provider = hton;
      ++search->complete_count;
    }
  }
  return false;
}

struct SmartengineLease {
  handlerton *provider{nullptr};
  THD *thd{nullptr};
  uint64_t snapshot_id{0};
};

SmartengineLease observe_smartengine(ServerRootEvidenceObservation *observation,
                                    THD *thd) {
  SmartengineProviderSearch search;
  plugin_foreach(thd, find_smartengine_provider, MYSQL_STORAGE_ENGINE_PLUGIN,
                 &search);
  if (search.partial || search.complete_count != 1 ||
      search.provider == nullptr) {
    const std::string detail = search.partial
                                   ? "an active engine exposes only part of "
                                     "the required snapshot API"
                                   : "exactly one active SmartEngine snapshot "
                                     "provider is required";
    set_unavailable(&observation->smartengine_snapshot_cursor, detail);
    set_unavailable(&observation->smartengine_live_extents, detail);
    set_unavailable(&observation->legacy_live_extents_empty, detail);
    return {};
  }

  uint64_t snapshot_id = 0;
  uint64_t offset = 0;
  std::string file;
  const int create_error = search.provider->create_backup_snapshot(
      thd, &snapshot_id, file, &offset);
  SmartengineLease lease{search.provider, thd, snapshot_id};
  if (create_error != 0 || snapshot_id == 0) {
    const std::string detail = "SmartEngine backup snapshot acquisition failed";
    set_unavailable(&observation->smartengine_snapshot_cursor, detail);
    set_unavailable(&observation->smartengine_live_extents, detail);
    set_unavailable(&observation->legacy_live_extents_empty, detail);
    return lease;
  }
  const Cursor snapshot_cursor{fs::path(file).filename().string(), offset};
  if (!valid_cursor(snapshot_cursor)) {
    set_unavailable(&observation->smartengine_snapshot_cursor,
                    "SmartEngine returned an invalid snapshot cursor");
  } else {
    set_available(&observation->smartengine_snapshot_cursor, snapshot_cursor);
  }

  std::vector<Smartengine_remote_extent_ref> exported;
  if (search.provider->export_backup_snapshot_live_set(thd, snapshot_id,
                                                       &exported) != 0) {
    set_unavailable(
        &observation->smartengine_live_extents,
        "SmartEngine live-set export failed or found a legacy extent");
    set_unavailable(
        &observation->legacy_live_extents_empty,
        "SmartEngine live-set export failed or found a legacy extent");
    return lease;
  }
  std::vector<SmartengineExtentRef> extents;
  extents.reserve(exported.size());
  for (const Smartengine_remote_extent_ref &ref : exported) {
    SmartengineExtentRef extent;
    extent.ordinal = ref.ordinal;
    extent.writer_epoch = ref.writer_epoch;
    extent.allocation_seq = std::to_string(ref.allocation_seq);
    extent.database_name_hex = ref.database_name_hex;
    extent.index_id = std::to_string(ref.index_id);
    extent.object_id = std::to_string(ref.object_id);
    extent.key = ref.key;
    extent.size = ref.size;
    extent.sha256 = ref.sha256;
    extent.format = std::string(kSmartengineExtentFormat);
    extents.push_back(std::move(extent));
  }
  set_available(&observation->smartengine_live_extents, std::move(extents));
  set_available(&observation->legacy_live_extents_empty, true);
  return lease;
}
#else
void observe_smartengine_absent(ServerRootEvidenceObservation *observation) {
  if (observation->coherent_cursor.available) {
    set_available(&observation->smartengine_snapshot_cursor,
                  observation->coherent_cursor.value);
  } else {
    set_unavailable(&observation->smartengine_snapshot_cursor,
                    observation->coherent_cursor.detail);
  }
  set_available(&observation->smartengine_live_extents,
                std::vector<SmartengineExtentRef>{});
  set_available(&observation->legacy_live_extents_empty, true);
}
#endif

}  // namespace

StartupStepResult verify_initialized_empty_root(
    const ServerRootVerificationRequest &request) {
  std::string detail;
  if (!valid_request_shape(request, &detail) || request.installed ||
      request.route != StartupCoordinatorRoute::BOOTSTRAP ||
      !may_initialize_empty_root()) {
    return {StartupStepOutcome::CORRUPT,
            "empty-root initialization authority is not held: " + detail};
  }
  ServerRootEvidenceObservation opened;
  observe_opened_root(request, &opened);
  StartupDeploymentIdentity declaration;
  if (!opened.opened_root_matches.available ||
      !opened.opened_root_matches.value ||
      !configured_server_root_runtime_deployment(&declaration, &detail) ||
      declaration != request.deployment) {
    return {StartupStepOutcome::CORRUPT,
            "initialized root or deployment identity does not match"};
  }

  Auto_THD auto_thd;
  TwoStageGlobalReadLock global_lock(auto_thd.thd);
  if (!global_lock.acquire(&detail))
    return {StartupStepOutcome::BLOCKED, std::move(detail)};

  const auto capture = [&](BootstrapServerSample *sample) {
    if (!may_initialize_empty_root() || mysql_bin_log.is_open() ||
        dynamic_cast<TC_LOG_DUMMY *>(tc_log) == nullptr ||
        server_uuid_ptr == nullptr ||
        !capture_gtid(&sample->executed_gtid, &detail) ||
        !capture_data_dictionary_inventory(auto_thd.thd, &sample->dictionary,
                                            &detail) ||
        !capture_replication_inventory(auto_thd.thd, &sample->replication,
                                       &detail) ||
        !capture_acl_inventory(auto_thd.thd, &sample->acl, &detail) ||
        !capture_prepared_inventory(&sample->prepared)) {
      if (detail.empty()) detail = "initialization inventory is unavailable";
      return false;
    }
    sample->server_uuid = server_uuid_ptr;
    const std::string binlog_name =
        log_bin_basename == nullptr
            ? std::string()
            : fs::path(log_bin_basename).filename().string();
    if (binlog_name.empty()) {
      detail = "initialization binary-log basename is unavailable";
      return false;
    }
    std::error_code scan_error;
    fs::directory_iterator entry(request.root, scan_error);
    const fs::directory_iterator end;
    while (!scan_error && entry != end) {
      const std::string name = entry->path().filename().string();
      if (name == "smartengine" || name == "tc.log" ||
          name == binlog_name || name.starts_with(binlog_name + ".")) {
        detail = "initialized root contains unexpected engine or TC state";
        return false;
      }
      entry.increment(scan_error);
    }
    if (scan_error) {
      detail = "cannot inspect initialized root for old engine or TC state";
      return false;
    }
#ifdef WITH_SMARTENGINE
    SmartengineProviderSearch search;
    plugin_foreach(auto_thd.thd, find_smartengine_provider,
                   MYSQL_STORAGE_ENGINE_PLUGIN, &search);
    if (search.partial || search.complete_count != 0) {
      detail = "SmartEngine was opened before bootstrap epoch acquisition";
      return false;
    }
#endif
    return true;
  };
  BootstrapServerSample before;
  BootstrapServerSample after;
  if (!capture(&before) || !capture(&after))
    return {StartupStepOutcome::BLOCKED, std::move(detail)};
  if (!may_initialize_empty_root() || before != after)
    return {StartupStepOutcome::CORRUPT,
            "initialization authority or paired inventories changed"};
  if (after.server_uuid != request.deployment.server_uuid ||
      !after.executed_gtid.canonical.empty() ||
      !after.dictionary.has_canonical_bootstrap_schemas() ||
      !after.acl.canonical_initialize_insecure_state() ||
      after.replication != ServerReplicationInventory{} ||
      after.prepared != ServerPreparedInventory{}) {
    return {StartupStepOutcome::CORRUPT,
            "initialized DD, accounts, GTID, repositories or prepared state "
            "are not the canonical empty source"};
  }
  return {StartupStepOutcome::READY, {}};
}

StartupStepResult collect_server_root_observation(
    const ServerRootVerificationRequest &request,
    ServerRootEvidenceObservation *result,
    RetainedSmartengineSnapshotEvidence *retained_smartengine) {
  if (result == nullptr)
    return {StartupStepOutcome::CORRUPT,
            "server root observation output is null"};
  *result = {};
  if (retained_smartengine != nullptr) {
    if (retained_smartengine->active())
      return {StartupStepOutcome::CORRUPT,
              "SmartEngine snapshot output already owns an active lease"};
    retained_smartengine->snapshot_id = 0;
    retained_smartengine->cursor = {};
    retained_smartengine->canonical_live_extents.clear();
    retained_smartengine->impl_.reset();
  }

  std::string request_error;
  if (!valid_request_shape(request, &request_error))
    return {StartupStepOutcome::CORRUPT, std::move(request_error)};

  ServerRootEvidenceObservation observation;
  observe_opened_root(request, &observation);
  if (server_uuid_ptr == nullptr || server_uuid_ptr[0] == '\0') {
    set_unavailable(&observation.server_uuid,
                    "mysqld server UUID is not initialized");
  } else {
    set_available(&observation.server_uuid, std::string(server_uuid_ptr));
  }

  StartupDeploymentIdentity declaration;
  std::string declaration_detail;
  if (configured_server_root_runtime_deployment(&declaration,
                                                &declaration_detail)) {
    set_available(&observation.declared_deployment, std::move(declaration));
  } else {
    set_unavailable(&observation.declared_deployment,
                    std::move(declaration_detail));
  }

  auto auto_thd = std::make_unique<Auto_THD>();

#ifdef WITH_SMARTENGINE
  SmartengineLease lease;
#endif

  if (request.route == StartupCoordinatorRoute::BOOTSTRAP) {
    const bool authorized_before =
        may_run_startup_bootstrap_snapshot_worker();
    if (!authorized_before) {
      const std::string detail =
          "bootstrap snapshot-worker authorization is not held";
      set_bootstrap_scan_unavailable(detail, &observation);
      set_unavailable(&observation.smartengine_snapshot_cursor, detail);
      set_unavailable(&observation.smartengine_live_extents, detail);
      set_unavailable(&observation.legacy_live_extents_empty, detail);
    } else {
      TwoStageGlobalReadLock global_lock(auto_thd->thd);
      std::string lock_detail;
      if (!global_lock.acquire(&lock_detail)) {
        set_bootstrap_scan_unavailable(lock_detail, &observation);
        set_unavailable(&observation.smartengine_snapshot_cursor, lock_detail);
        set_unavailable(&observation.smartengine_live_extents, lock_detail);
        set_unavailable(&observation.legacy_live_extents_empty, lock_detail);
      } else {
        // The COMMIT MDL stage has drained pre-existing committers. Waiting
        // here proves the binlog's prepared-XID counter is at zero without
        // exposing its private counter.
        mysql_bin_log.wait_for_prep_xids();
        BootstrapServerSample before;
        BootstrapServerSample after;
        std::string before_detail;
        std::string after_detail;
        const bool before_ok = capture_bootstrap_server_sample(
            request, auto_thd->thd, true, &before, &before_detail);
#ifdef WITH_SMARTENGINE
        lease = observe_smartengine(&observation, auto_thd->thd);
#else
        if (before_ok) {
          set_available(&observation.coherent_cursor, before.cursor);
        }
        observe_smartengine_absent(&observation);
#endif
        const bool after_ok = capture_bootstrap_server_sample(
            request, auto_thd->thd, true, &after, &after_detail);
        const bool authorized_after =
            may_run_startup_bootstrap_snapshot_worker();
        global_lock.release();

        if (!before_ok || !after_ok) {
          std::string detail = !before_ok ? before_detail : after_detail;
          if (detail.empty())
            detail = "paired bootstrap server sampling failed";
          set_bootstrap_scan_unavailable(std::move(detail), &observation);
        } else {
          const bool stable = authorized_after && before == after;
          std::string stable_detail;
          if (!authorized_after)
            append_detail(
                "bootstrap snapshot-worker authorization changed during scan",
                &stable_detail);
          if (before != after)
            append_detail("paired bootstrap server samples differ",
                          &stable_detail);
          publish_bootstrap_sample(after, std::move(after_detail), stable,
                                   std::move(stable_detail), &observation);
        }
      }
    }
  } else {
    observe_coherent_cut(&observation);
    observe_data_dictionary(auto_thd->thd, &observation);
    observe_replication(auto_thd->thd, &observation);
    observe_prepared(&observation);
#ifdef WITH_SMARTENGINE
    lease = observe_smartengine(&observation, auto_thd->thd);
#else
    observe_smartengine_absent(&observation);
#endif
  }
  observe_installed_marker(request, &observation);

#ifdef WITH_SMARTENGINE
  RetainedSmartengineSnapshotEvidence lease_owner;
  if (lease.snapshot_id != 0) {
    lease_owner.snapshot_id = lease.snapshot_id;
    if (observation.smartengine_snapshot_cursor.available)
      lease_owner.cursor = observation.smartengine_snapshot_cursor.value;
    if (observation.smartengine_live_extents.available) {
      std::string canonical_error;
      if (!canonicalize_server_root_extents(
              observation.smartengine_live_extents.value,
              &lease_owner.canonical_live_extents, &canonical_error)) {
        observation.smartengine_live_extents.available = false;
        observation.smartengine_live_extents.value.clear();
        observation.smartengine_live_extents.detail =
            "SmartEngine exported a noncanonical live set: " + canonical_error;
      }
    }
    lease_owner.impl_ =
        std::make_unique<RetainedSmartengineSnapshotEvidence::Impl>(
            lease.provider, std::move(auto_thd), lease.snapshot_id);
  }
  if (retained_smartengine != nullptr && lease_owner.active()) {
    retained_smartengine->snapshot_id = lease_owner.snapshot_id;
    retained_smartengine->cursor = std::move(lease_owner.cursor);
    retained_smartengine->canonical_live_extents =
        std::move(lease_owner.canonical_live_extents);
    retained_smartengine->impl_ = std::move(lease_owner.impl_);
    *result = std::move(observation);
    return {StartupStepOutcome::READY, {}};
  }
  const StartupStepResult released = lease_owner.release();
  if (!released.ready()) {
    *result = std::move(observation);
    return released;
  }
  *result = std::move(observation);
  return {StartupStepOutcome::READY, {}};
#else
  *result = std::move(observation);
  return {StartupStepOutcome::READY, {}};
#endif
}

StartupStepResult collect_server_root_evidence(
    const ServerRootVerificationRequest &request, StartupRootEvidence *evidence,
    RetainedSmartengineSnapshotEvidence *retained_smartengine) {
  if (evidence == nullptr)
    return {StartupStepOutcome::CORRUPT,
            "server root evidence output is null"};
  *evidence = {};
  if (retained_smartengine != nullptr) {
    if (retained_smartengine->active())
      return {StartupStepOutcome::CORRUPT,
              "SmartEngine snapshot output already owns an active lease"};
    retained_smartengine->snapshot_id = 0;
    retained_smartengine->cursor = {};
    retained_smartengine->canonical_live_extents.clear();
    retained_smartengine->impl_.reset();
  }

  ServerRootEvidenceObservation observation;
  RetainedSmartengineSnapshotEvidence lease_owner;
  StartupStepResult captured =
      collect_server_root_observation(request, &observation, &lease_owner);
  if (!captured.ready()) return captured;

  StartupStepResult compared =
      compare_server_root_evidence(request, observation, evidence);
  if (compared.ready() && !request.installed &&
      retained_smartengine != nullptr && lease_owner.active()) {
    retained_smartengine->snapshot_id = lease_owner.snapshot_id;
    retained_smartengine->cursor = std::move(lease_owner.cursor);
    retained_smartengine->canonical_live_extents =
        std::move(lease_owner.canonical_live_extents);
    retained_smartengine->impl_ = std::move(lease_owner.impl_);
    return compared;
  }

  const StartupStepResult released = lease_owner.release();
  if (!released.ready()) {
    *evidence = {};
    std::string detail = released.detail;
    if (!compared.detail.empty()) detail.append("; ").append(compared.detail);
    const bool corrupt =
        compared.outcome == StartupStepOutcome::CORRUPT ||
        released.outcome == StartupStepOutcome::CORRUPT;
    return {corrupt ? StartupStepOutcome::CORRUPT
                    : StartupStepOutcome::BLOCKED,
            std::move(detail)};
  }
  return compared;
}
#endif  // WESQL_SERVER_ROOT_EVIDENCE_TEST_ONLY

}  // namespace wesql::remote_commit
