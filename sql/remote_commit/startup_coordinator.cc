/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/startup_coordinator.h"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <filesystem>
#include <initializer_list>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include "my_rapidjson_size_t.h"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace wesql::remote_commit {
namespace {

namespace fs = std::filesystem;
using JsonWriter = rapidjson::Writer<rapidjson::StringBuffer>;

constexpr char kPreflightFormat[] =
    "wesql.remote_commit.startup.bootstrap_preflight";
constexpr char kWorkerRequestFormat[] =
    "wesql.remote_commit.startup.worker_request";
constexpr char kWorkerCompletionFormat[] =
    "wesql.remote_commit.startup.worker_completion";
constexpr char kRestartProofFormat[] =
    "wesql.remote_commit.startup.restart_proof";
constexpr char kBootstrapPreflightRequestDomain[] =
    "wesql.remote_commit.bootstrap_preflight.request.v1";
constexpr uint64_t kDocumentVersion = 1;
constexpr size_t kMaxStartupPathBytes = 4096;

StartupCoordinatorResult result(
    StartupCoordinatorOutcome outcome, std::string detail = {},
    std::optional<StartupWorkerRequest> worker = std::nullopt,
    std::optional<StartupRestartProofReference> restart = std::nullopt,
    std::optional<StartupCoordinatorProof> proof = std::nullopt) {
  return {outcome, std::move(detail), std::move(worker), std::move(restart),
          std::move(proof)};
}

bool fail_with(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return false;
}

bool same_object(const PublishedBytes &left, const PublishedBytes &right) {
  return left.body == right.body && left.etag == right.etag;
}

bool same_epoch(const RecoveryPlan &plan, const ExactWriterEpoch &epoch) {
  return plan.epoch == epoch.value &&
         same_object(plan.epoch_object, epoch.object);
}

bool valid_sha256(std::string_view value) {
  return value.size() == 64 &&
         value.find_first_not_of("0123456789abcdef") == std::string_view::npos;
}

bool valid_text(std::string_view value, size_t maximum_bytes,
                bool allow_empty = false) {
  if ((!allow_empty && value.empty()) || value.size() > maximum_bytes)
    return false;
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
  return valid_text(cursor.file, 255) && cursor.pos >= 4 &&
         cursor.pos <= kJsonSafeIntegerMax;
}

bool valid_gtid_digest(const GtidSetDigest &value) {
  GtidSetDigest recomputed;
  std::string error;
  return value.canonical.size() <= kMaxCanonicalGtidBytes &&
         gtid_digest(value.canonical, &recomputed, &error) &&
         recomputed == value;
}

bool valid_deployment_shape(const StartupDeploymentIdentity &deployment) {
  return valid_text(deployment.stream_id, kMaxObjectKeyBytes) &&
         valid_uuid(deployment.server_uuid) &&
         valid_sha256(deployment.binary_fingerprint) &&
         valid_sha256(deployment.fingerprints.startup_config_sha256) &&
         valid_text(deployment.fingerprints.server_build,
                    kMaxOrdinaryIdBytes) &&
         valid_sha256(
             deployment.fingerprints.plugin_component_set_sha256) &&
         valid_sha256(deployment.fingerprints.keyring_config_sha256) &&
         valid_sha256(deployment.fingerprints.tls_config_sha256);
}

bool valid_deployment(const StreamIdentity &stream,
                      const StartupDeploymentIdentity &deployment) {
  return !stream.stream_id.empty() && deployment.stream_id == stream.stream_id &&
         valid_deployment_shape(deployment);
}

bool valid_stable_root_manifest(const StartupStableRootManifest &manifest) {
  return manifest.entry_count != 0 &&
         manifest.entry_count <= kStartupRootMaxEntries &&
         manifest.regular_file_count != 0 &&
         manifest.regular_file_count <= manifest.entry_count &&
         manifest.regular_file_bytes <= kJsonSafeIntegerMax &&
         valid_sha256(manifest.manifest_sha256);
}

bool complete_root_evidence(const StartupRootEvidence &evidence,
                            bool installed) {
  return valid_cursor(evidence.recovered_cursor) &&
         valid_gtid_digest(evidence.recovered_gtid) &&
         (!installed ||
          (evidence.marker_matches && evidence.snapshot_matches)) &&
         evidence.server_uuid_matches && evidence.configuration_matches &&
         evidence.gtid_matches && evidence.dd_matches &&
         evidence.repository_empty && evidence.extent_live_set_matches &&
         evidence.exported_extent_count <= kMaxSnapshotItems &&
         valid_sha256(evidence.exported_extent_set_sha256) &&
         evidence.internal_prepared_empty && evidence.external_xa_empty;
}

bool valid_acquired_epoch(const ExactWriterEpoch &epoch) {
  return !epoch.object.body.empty() &&
         epoch.object.body.size() <= kWriterEpochMaxBytes &&
         valid_text(epoch.object.etag, kMaxObjectKeyBytes) &&
         epoch.value.epoch != 0 &&
         epoch.value.epoch <= kJsonSafeIntegerMax &&
         valid_text(epoch.value.writer_id, kMaxOrdinaryIdBytes);
}

bool deployment_matches(const RecoveryPlan &plan,
                        const StartupDeploymentIdentity &deployment) {
  return plan.snapshot.server_identity.server_uuid == deployment.server_uuid &&
         plan.snapshot.deployment_fingerprints == deployment.fingerprints;
}

LocalInstallMarker preflight_marker(
    const StartupDeploymentIdentity &deployment) {
  LocalInstallMarker marker;
  marker.stream_id = deployment.stream_id;
  marker.server_uuid = deployment.server_uuid;
  marker.installed_head.generation = 1;
  marker.installed_head.body_sha256 = std::string(64, '0');
  marker.installed_head.snapshot_id = "preflight";
  marker.installed_head.snapshot_manifest_sha256 = std::string(64, '0');
  marker.installed_head.snapshot_cursor = {"binlog.000001", 4};
  marker.config_digest = deployment.fingerprints.startup_config_sha256;
  marker.binary_fingerprint = deployment.binary_fingerprint;
  return marker;
}

bool build_install_marker(const StreamIdentity &stream,
                          const StartupDeploymentIdentity &deployment,
                          const RecoveryPlan &published,
                          LocalInstallMarker *marker, std::string *error) {
  std::string head_sha;
  if (!sha256_hex(published.head_object.body, &head_sha, error)) return false;
  marker->stream_id = stream.stream_id;
  marker->server_uuid = published.snapshot.server_identity.server_uuid;
  marker->installed_head.generation = published.head.generation;
  marker->installed_head.body_sha256 = std::move(head_sha);
  marker->installed_head.snapshot_id = published.head.snapshot.id;
  marker->installed_head.snapshot_manifest_sha256 =
      published.head.snapshot.manifest_sha256;
  marker->installed_head.snapshot_cursor = published.head.snapshot.cursor;
  marker->config_digest = deployment.fingerprints.startup_config_sha256;
  marker->binary_fingerprint = deployment.binary_fingerprint;
  return true;
}

bool validate_sibling_paths(const fs::path &temporary, const fs::path &target,
                            std::string *error) {
  if (temporary.empty() || target.empty() || !temporary.is_absolute() ||
      !target.is_absolute() || temporary == target ||
      temporary.filename().empty() || target.filename().empty()) {
    return fail_with(error,
                     "startup target and temporary root paths are invalid");
  }
  std::error_code filesystem_error;
  const fs::path temporary_parent =
      fs::weakly_canonical(temporary.parent_path(), filesystem_error);
  if (filesystem_error)
    return fail_with(error, "cannot resolve startup temporary-root parent: " +
                                filesystem_error.message());
  const fs::path target_parent =
      fs::weakly_canonical(target.parent_path(), filesystem_error);
  if (filesystem_error || temporary_parent != target_parent ||
      !fs::is_directory(target_parent, filesystem_error) || filesystem_error) {
    return fail_with(
        error, "startup roots are not fresh siblings under one real parent");
  }
  return true;
}

bool validate_activation_options(const StreamIdentity &stream,
                                 const StartupActivationOptions &options,
                                 std::string *error) {
  if (!valid_deployment(stream, options.deployment) ||
      options.target_root.empty() || !options.target_root.is_absolute() ||
      options.target_root.filename().empty()) {
    return fail_with(
        error, "startup activation identity or target root is invalid");
  }
  std::error_code filesystem_error;
  const fs::path parent = fs::weakly_canonical(
      options.target_root.parent_path(), filesystem_error);
  if (filesystem_error || !fs::is_directory(parent, filesystem_error) ||
      filesystem_error) {
    return fail_with(error, "startup activation target parent is unreadable");
  }
  return true;
}

bool path_string(const fs::path &path, std::string *value) {
  *value = path.generic_string();
  return valid_text(*value, kMaxStartupPathBytes);
}

bool path_within_root(const fs::path &root, const fs::path &path);

bool exact_members(const rapidjson::Value &object,
                   std::initializer_list<std::string_view> required) {
  if (!object.IsObject() || object.MemberCount() != required.size())
    return false;
  std::set<std::string_view> seen;
  for (auto member = object.MemberBegin(); member != object.MemberEnd();
       ++member) {
    if (!member->name.IsString()) return false;
    const std::string_view name(member->name.GetString(),
                                member->name.GetStringLength());
    if (std::find(required.begin(), required.end(), name) == required.end() ||
        !seen.insert(name).second) {
      return false;
    }
  }
  return true;
}

bool json_string(const rapidjson::Value &object, const char *name,
                 std::string *value, size_t maximum_bytes,
                 bool allow_empty = false) {
  const auto found = object.FindMember(name);
  if (found == object.MemberEnd() || !found->value.IsString()) return false;
  value->assign(found->value.GetString(), found->value.GetStringLength());
  return valid_text(*value, maximum_bytes, allow_empty);
}

bool json_uint64(const rapidjson::Value &object, const char *name,
                 uint64_t *value) {
  const auto found = object.FindMember(name);
  if (found == object.MemberEnd() || !found->value.IsUint64() ||
      found->value.GetUint64() > kJsonSafeIntegerMax) {
    return false;
  }
  *value = found->value.GetUint64();
  return true;
}

bool json_bool(const rapidjson::Value &object, const char *name, bool *value) {
  const auto found = object.FindMember(name);
  if (found == object.MemberEnd() || !found->value.IsBool()) return false;
  *value = found->value.GetBool();
  return true;
}

bool parse_document(std::string_view json, size_t maximum_bytes,
                    rapidjson::Document *document, std::string *error) {
  if (json.empty() || json.size() > maximum_bytes)
    return fail_with(error, "startup document size is invalid");
  document->Parse<rapidjson::kParseValidateEncodingFlag |
                  rapidjson::kParseStopWhenDoneFlag>(json.data(), json.size());
  if (document->HasParseError() || !document->IsObject())
    return fail_with(error, "startup document is not strict UTF-8 JSON");
  return true;
}

bool validate_envelope(const rapidjson::Value &document,
                       std::string_view format) {
  const auto format_member = document.FindMember("format");
  const auto version_member = document.FindMember("version");
  return format_member != document.MemberEnd() &&
         format_member->value.IsString() &&
         std::string_view(format_member->value.GetString(),
                          format_member->value.GetStringLength()) == format &&
         version_member != document.MemberEnd() &&
         version_member->value.IsUint64() &&
         version_member->value.GetUint64() == kDocumentVersion;
}

void write_string(JsonWriter *writer, std::string_view value) {
  writer->String(value.data(), static_cast<rapidjson::SizeType>(value.size()));
}

void write_named_string(JsonWriter *writer, const char *name,
                        std::string_view value) {
  writer->Key(name);
  write_string(writer, value);
}

void write_cursor(JsonWriter *writer, const Cursor &cursor) {
  writer->StartObject();
  write_named_string(writer, "file", cursor.file);
  writer->Key("pos");
  writer->Uint64(cursor.pos);
  writer->EndObject();
}

bool parse_cursor(const rapidjson::Value &value, Cursor *cursor) {
  return exact_members(value, {"file", "pos"}) &&
         json_string(value, "file", &cursor->file, 255) &&
         json_uint64(value, "pos", &cursor->pos) && valid_cursor(*cursor);
}

void write_gtid(JsonWriter *writer, const GtidSetDigest &gtid) {
  writer->StartObject();
  write_named_string(writer, "canonical", gtid.canonical);
  write_named_string(writer, "sha256", gtid.sha256);
  writer->EndObject();
}

bool parse_gtid(const rapidjson::Value &value, GtidSetDigest *gtid) {
  return exact_members(value, {"canonical", "sha256"}) &&
         json_string(value, "canonical", &gtid->canonical,
                     kMaxCanonicalGtidBytes, true) &&
         json_string(value, "sha256", &gtid->sha256, 64) &&
         valid_gtid_digest(*gtid);
}

void write_fingerprints(JsonWriter *writer,
                        const DeploymentFingerprints &value) {
  writer->StartObject();
  write_named_string(writer, "startup_config_sha256",
                     value.startup_config_sha256);
  write_named_string(writer, "server_build", value.server_build);
  write_named_string(writer, "plugin_component_set_sha256",
                     value.plugin_component_set_sha256);
  write_named_string(writer, "keyring_config_sha256",
                     value.keyring_config_sha256);
  write_named_string(writer, "tls_config_sha256", value.tls_config_sha256);
  writer->EndObject();
}

bool parse_fingerprints(const rapidjson::Value &value,
                        DeploymentFingerprints *result) {
  return exact_members(value,
                       {"startup_config_sha256", "server_build",
                        "plugin_component_set_sha256",
                        "keyring_config_sha256", "tls_config_sha256"}) &&
         json_string(value, "startup_config_sha256",
                     &result->startup_config_sha256, 64) &&
         valid_sha256(result->startup_config_sha256) &&
         json_string(value, "server_build", &result->server_build,
                     kMaxOrdinaryIdBytes) &&
         json_string(value, "plugin_component_set_sha256",
                     &result->plugin_component_set_sha256, 64) &&
         valid_sha256(result->plugin_component_set_sha256) &&
         json_string(value, "keyring_config_sha256",
                     &result->keyring_config_sha256, 64) &&
         valid_sha256(result->keyring_config_sha256) &&
         json_string(value, "tls_config_sha256",
                     &result->tls_config_sha256, 64) &&
         valid_sha256(result->tls_config_sha256);
}

void write_deployment(JsonWriter *writer,
                      const StartupDeploymentIdentity &value) {
  writer->StartObject();
  write_named_string(writer, "stream_id", value.stream_id);
  write_named_string(writer, "server_uuid", value.server_uuid);
  writer->Key("fingerprints");
  write_fingerprints(writer, value.fingerprints);
  write_named_string(writer, "binary_fingerprint", value.binary_fingerprint);
  writer->EndObject();
}

bool parse_deployment(const rapidjson::Value &value,
                      StartupDeploymentIdentity *result) {
  if (!exact_members(value, {"stream_id", "server_uuid", "fingerprints",
                             "binary_fingerprint"}) ||
      !json_string(value, "stream_id", &result->stream_id,
                   kMaxObjectKeyBytes) ||
      !json_string(value, "server_uuid", &result->server_uuid, 36) ||
      !valid_uuid(result->server_uuid) ||
      !json_string(value, "binary_fingerprint", &result->binary_fingerprint,
                   64) ||
      !valid_sha256(result->binary_fingerprint)) {
    return false;
  }
  const auto fingerprints = value.FindMember("fingerprints");
  return fingerprints != value.MemberEnd() &&
         parse_fingerprints(fingerprints->value, &result->fingerprints);
}

void write_root_evidence(JsonWriter *writer,
                         const StartupRootEvidence &value) {
  writer->StartObject();
  writer->Key("recovered_cursor");
  write_cursor(writer, value.recovered_cursor);
  writer->Key("recovered_gtid");
  write_gtid(writer, value.recovered_gtid);
  writer->Key("marker_matches");
  writer->Bool(value.marker_matches);
  writer->Key("snapshot_matches");
  writer->Bool(value.snapshot_matches);
  writer->Key("server_uuid_matches");
  writer->Bool(value.server_uuid_matches);
  writer->Key("configuration_matches");
  writer->Bool(value.configuration_matches);
  writer->Key("gtid_matches");
  writer->Bool(value.gtid_matches);
  writer->Key("dd_matches");
  writer->Bool(value.dd_matches);
  writer->Key("repository_empty");
  writer->Bool(value.repository_empty);
  writer->Key("extent_live_set_matches");
  writer->Bool(value.extent_live_set_matches);
  writer->Key("exported_extent_count");
  writer->Uint64(value.exported_extent_count);
  write_named_string(writer, "exported_extent_set_sha256",
                     value.exported_extent_set_sha256);
  writer->Key("internal_prepared_empty");
  writer->Bool(value.internal_prepared_empty);
  writer->Key("external_xa_empty");
  writer->Bool(value.external_xa_empty);
  writer->EndObject();
}

bool parse_root_evidence(const rapidjson::Value &value,
                         StartupRootEvidence *result) {
  if (!exact_members(value,
                     {"recovered_cursor", "recovered_gtid", "marker_matches",
                      "snapshot_matches", "server_uuid_matches",
                      "configuration_matches", "gtid_matches", "dd_matches",
                      "repository_empty", "extent_live_set_matches",
                      "exported_extent_count", "exported_extent_set_sha256",
                      "internal_prepared_empty", "external_xa_empty"})) {
    return false;
  }
  const auto cursor = value.FindMember("recovered_cursor");
  const auto gtid = value.FindMember("recovered_gtid");
  return cursor != value.MemberEnd() && gtid != value.MemberEnd() &&
         parse_cursor(cursor->value, &result->recovered_cursor) &&
         parse_gtid(gtid->value, &result->recovered_gtid) &&
         json_bool(value, "marker_matches", &result->marker_matches) &&
         json_bool(value, "snapshot_matches", &result->snapshot_matches) &&
         json_bool(value, "server_uuid_matches",
                   &result->server_uuid_matches) &&
         json_bool(value, "configuration_matches",
                   &result->configuration_matches) &&
         json_bool(value, "gtid_matches", &result->gtid_matches) &&
         json_bool(value, "dd_matches", &result->dd_matches) &&
         json_bool(value, "repository_empty", &result->repository_empty) &&
         json_bool(value, "extent_live_set_matches",
                   &result->extent_live_set_matches) &&
         json_uint64(value, "exported_extent_count",
                     &result->exported_extent_count) &&
         result->exported_extent_count <= kMaxSnapshotItems &&
         json_string(value, "exported_extent_set_sha256",
                     &result->exported_extent_set_sha256, 64) &&
         valid_sha256(result->exported_extent_set_sha256) &&
         json_bool(value, "internal_prepared_empty",
                   &result->internal_prepared_empty) &&
         json_bool(value, "external_xa_empty", &result->external_xa_empty);
}

void write_stable_root_manifest(JsonWriter *writer,
                                const StartupStableRootManifest &value) {
  writer->StartObject();
  writer->Key("entry_count");
  writer->Uint64(value.entry_count);
  writer->Key("regular_file_count");
  writer->Uint64(value.regular_file_count);
  writer->Key("regular_file_bytes");
  writer->Uint64(value.regular_file_bytes);
  write_named_string(writer, "manifest_sha256", value.manifest_sha256);
  writer->EndObject();
}

bool parse_stable_root_manifest(const rapidjson::Value &value,
                                StartupStableRootManifest *result) {
  return exact_members(value,
                       {"entry_count", "regular_file_count",
                        "regular_file_bytes", "manifest_sha256"}) &&
         json_uint64(value, "entry_count", &result->entry_count) &&
         json_uint64(value, "regular_file_count",
                     &result->regular_file_count) &&
         json_uint64(value, "regular_file_bytes",
                     &result->regular_file_bytes) &&
         json_string(value, "manifest_sha256", &result->manifest_sha256, 64) &&
         valid_stable_root_manifest(*result);
}

std::string_view route_name(StartupCoordinatorRoute route) {
  return route == StartupCoordinatorRoute::BOOTSTRAP ? "BOOTSTRAP"
                                                     : "TAKEOVER";
}

bool parse_route(const rapidjson::Value &object, const char *name,
                 StartupCoordinatorRoute *route) {
  std::string text;
  if (!json_string(object, name, &text, 16)) return false;
  if (text == "BOOTSTRAP") {
    *route = StartupCoordinatorRoute::BOOTSTRAP;
    return true;
  }
  if (text == "TAKEOVER") {
    *route = StartupCoordinatorRoute::TAKEOVER;
    return true;
  }
  return false;
}

void write_epoch(JsonWriter *writer, const ExactWriterEpoch &epoch) {
  writer->StartObject();
  write_named_string(writer, "body", epoch.object.body);
  write_named_string(writer, "etag", epoch.object.etag);
  writer->EndObject();
}

bool parse_epoch(const rapidjson::Value &value, const StreamIdentity &stream,
                 ExactWriterEpoch *epoch, std::string *error) {
  if (!exact_members(value, {"body", "etag"}) ||
      !json_string(value, "body", &epoch->object.body,
                   kWriterEpochMaxBytes) ||
      !json_string(value, "etag", &epoch->object.etag,
                   kMaxObjectKeyBytes) ||
      !parse_writer_epoch(epoch->object.body, stream, &epoch->value, error)) {
    return fail_with(error, "startup epoch object is invalid");
  }
  return valid_acquired_epoch(*epoch);
}

void write_published_head(JsonWriter *writer,
                          const PublishedStartupHead &head) {
  writer->StartObject();
  write_named_string(writer, "body", head.object.body);
  write_named_string(writer, "etag", head.object.etag);
  writer->EndObject();
}

bool parse_published_head(const rapidjson::Value &value,
                          const StreamIdentity &stream,
                          PublishedStartupHead *head, std::string *error) {
  if (!exact_members(value, {"body", "etag"}) ||
      !json_string(value, "body", &head->object.body, kHeadMaxBytes) ||
      !json_string(value, "etag", &head->object.etag, kMaxObjectKeyBytes) ||
      !parse_head(head->object.body, stream, &head->value, error)) {
    return fail_with(error, "startup HEAD object is invalid");
  }
  return true;
}

void write_writer(JsonWriter *writer, const Writer &value) {
  writer->StartObject();
  write_named_string(writer, "id", value.id);
  writer->Key("epoch");
  writer->Uint64(value.epoch);
  writer->EndObject();
}

bool parse_writer(const rapidjson::Value &value, Writer *result) {
  return exact_members(value, {"id", "epoch"}) &&
         json_string(value, "id", &result->id, kMaxOrdinaryIdBytes) &&
         json_uint64(value, "epoch", &result->epoch) && result->epoch != 0;
}

void write_object_ref(JsonWriter *writer, const ObjectRef &value) {
  writer->StartObject();
  write_named_string(writer, "key", value.key);
  writer->Key("size");
  writer->Uint64(value.size);
  write_named_string(writer, "sha256", value.sha256);
  writer->EndObject();
}

bool parse_object_ref(const rapidjson::Value &value, ObjectRef *result) {
  return exact_members(value, {"key", "size", "sha256"}) &&
         json_string(value, "key", &result->key, kMaxObjectKeyBytes) &&
         json_uint64(value, "size", &result->size) && result->size != 0 &&
         json_string(value, "sha256", &result->sha256, 64) &&
         valid_sha256(result->sha256);
}

void write_log_anchor(JsonWriter *writer, const LogAnchor &value) {
  writer->StartObject();
  write_named_string(writer, "kind",
                     value.kind == LogAnchorKind::EMPTY_BASE
                         ? "EMPTY_BASE"
                         : "MANIFEST_BOUNDARY");
  writer->Key("generation");
  if (value.generation.has_value()) {
    writer->Uint64(*value.generation);
  } else {
    writer->Null();
  }
  writer->Key("manifest");
  if (value.manifest.has_value()) {
    write_object_ref(writer, *value.manifest);
  } else {
    writer->Null();
  }
  writer->Key("cursor");
  write_cursor(writer, value.cursor);
  writer->EndObject();
}

bool parse_log_anchor(const rapidjson::Value &value, LogAnchor *result) {
  if (!exact_members(value, {"kind", "generation", "manifest", "cursor"}))
    return false;
  std::string kind;
  if (!json_string(value, "kind", &kind, 32)) return false;
  if (kind == "EMPTY_BASE") {
    result->kind = LogAnchorKind::EMPTY_BASE;
  } else if (kind == "MANIFEST_BOUNDARY") {
    result->kind = LogAnchorKind::MANIFEST_BOUNDARY;
  } else {
    return false;
  }
  const auto generation = value.FindMember("generation");
  const auto manifest = value.FindMember("manifest");
  const auto cursor = value.FindMember("cursor");
  if (generation == value.MemberEnd() || manifest == value.MemberEnd() ||
      cursor == value.MemberEnd() ||
      !parse_cursor(cursor->value, &result->cursor)) {
    return false;
  }
  if (generation->value.IsNull()) {
    result->generation.reset();
  } else if (generation->value.IsUint64() &&
             generation->value.GetUint64() <= kJsonSafeIntegerMax) {
    result->generation = generation->value.GetUint64();
  } else {
    return false;
  }
  if (manifest->value.IsNull()) {
    result->manifest.reset();
  } else {
    ObjectRef parsed;
    if (!parse_object_ref(manifest->value, &parsed)) return false;
    result->manifest = std::move(parsed);
  }
  return (result->kind == LogAnchorKind::EMPTY_BASE &&
          !result->generation.has_value() && !result->manifest.has_value()) ||
         (result->kind == LogAnchorKind::MANIFEST_BOUNDARY &&
          result->generation.has_value() && *result->generation != 0 &&
          result->manifest.has_value());
}

std::string_view cut_source_name(FixedCutSource source) {
  switch (source) {
    case FixedCutSource::EMPTY_SOURCE:
      return "EMPTY_SOURCE";
    case FixedCutSource::RECOVERED_TAKEOVER:
      return "RECOVERED_TAKEOVER";
    case FixedCutSource::CLONE_BARRIER:
      return "CLONE_BARRIER";
  }
  return {};
}

bool parse_cut_source(const rapidjson::Value &value, const char *name,
                      FixedCutSource *source) {
  std::string text;
  if (!json_string(value, name, &text, 32)) return false;
  if (text == "EMPTY_SOURCE") {
    *source = FixedCutSource::EMPTY_SOURCE;
    return true;
  }
  if (text == "RECOVERED_TAKEOVER") {
    *source = FixedCutSource::RECOVERED_TAKEOVER;
    return true;
  }
  if (text == "CLONE_BARRIER") {
    *source = FixedCutSource::CLONE_BARRIER;
    return true;
  }
  return false;
}

void write_optional_uint64(JsonWriter *writer,
                           const std::optional<uint64_t> &value) {
  if (value.has_value()) {
    writer->Uint64(*value);
  } else {
    writer->Null();
  }
}

void write_optional_string(JsonWriter *writer,
                           const std::optional<std::string> &value) {
  if (value.has_value()) {
    write_string(writer, *value);
  } else {
    writer->Null();
  }
}

bool parse_optional_uint64(const rapidjson::Value &value,
                           std::optional<uint64_t> *result) {
  if (value.IsNull()) {
    result->reset();
    return true;
  }
  if (!value.IsUint64() || value.GetUint64() > kJsonSafeIntegerMax)
    return false;
  *result = value.GetUint64();
  return true;
}

bool parse_optional_string(const rapidjson::Value &value, size_t maximum,
                           std::optional<std::string> *result,
                           bool sha256 = false) {
  if (value.IsNull()) {
    result->reset();
    return true;
  }
  if (!value.IsString()) return false;
  std::string parsed(value.GetString(), value.GetStringLength());
  if (!valid_text(parsed, maximum) || (sha256 && !valid_sha256(parsed)))
    return false;
  *result = std::move(parsed);
  return true;
}

void write_fixed_cut_proof(JsonWriter *writer, const FixedCutProof &value) {
  writer->StartObject();
  write_named_string(writer, "source", cut_source_name(value.source));
  writer->Key("public_cursor");
  write_cursor(writer, value.public_cursor);
  writer->Key("public_gtid");
  write_gtid(writer, value.public_gtid);
  writer->Key("image_cursor");
  write_cursor(writer, value.image_cursor);
  writer->Key("image_gtid");
  write_gtid(writer, value.image_gtid);
  writer->Key("source_head_generation");
  writer->Uint64(value.source_head_generation);
  write_named_string(writer, "source_head_body_sha256",
                     value.source_head_body_sha256);
  writer->Key("clone_handle_id");
  write_optional_uint64(writer, value.clone_handle_id);
  writer->Key("redo_range_sha256");
  write_optional_string(writer, value.redo_range_sha256);
  writer->Key("empty_source_scan_stable");
  writer->Bool(value.empty_source_scan_stable);
  writer->Key("internal_prepared_empty");
  writer->Bool(value.internal_prepared_empty);
  writer->Key("external_xa_empty");
  writer->Bool(value.external_xa_empty);
  writer->Key("old_tc_authority_empty");
  writer->Bool(value.old_tc_authority_empty);
  writer->Key("user_state_empty");
  writer->Bool(value.user_state_empty);
  writer->Key("legacy_live_extents_empty");
  writer->Bool(value.legacy_live_extents_empty);
  writer->EndObject();
}

bool parse_fixed_cut_proof(const rapidjson::Value &value,
                           FixedCutProof *result) {
  if (!exact_members(
          value,
          {"source", "public_cursor", "public_gtid", "image_cursor",
           "image_gtid", "source_head_generation",
           "source_head_body_sha256", "clone_handle_id",
           "redo_range_sha256", "empty_source_scan_stable",
           "internal_prepared_empty", "external_xa_empty",
           "old_tc_authority_empty", "user_state_empty",
           "legacy_live_extents_empty"}) ||
      !parse_cut_source(value, "source", &result->source)) {
    return false;
  }
  const auto public_cursor = value.FindMember("public_cursor");
  const auto public_gtid = value.FindMember("public_gtid");
  const auto image_cursor = value.FindMember("image_cursor");
  const auto image_gtid = value.FindMember("image_gtid");
  const auto clone = value.FindMember("clone_handle_id");
  const auto redo = value.FindMember("redo_range_sha256");
  return public_cursor != value.MemberEnd() &&
         public_gtid != value.MemberEnd() && image_cursor != value.MemberEnd() &&
         image_gtid != value.MemberEnd() && clone != value.MemberEnd() &&
         redo != value.MemberEnd() &&
         parse_cursor(public_cursor->value, &result->public_cursor) &&
         parse_gtid(public_gtid->value, &result->public_gtid) &&
         parse_cursor(image_cursor->value, &result->image_cursor) &&
         parse_gtid(image_gtid->value, &result->image_gtid) &&
         json_uint64(value, "source_head_generation",
                     &result->source_head_generation) &&
         json_string(value, "source_head_body_sha256",
                     &result->source_head_body_sha256, 64, true) &&
         (result->source_head_body_sha256.empty() ||
          valid_sha256(result->source_head_body_sha256)) &&
         parse_optional_uint64(clone->value, &result->clone_handle_id) &&
         parse_optional_string(redo->value, 64, &result->redo_range_sha256,
                               true) &&
         json_bool(value, "empty_source_scan_stable",
                   &result->empty_source_scan_stable) &&
         json_bool(value, "internal_prepared_empty",
                   &result->internal_prepared_empty) &&
         json_bool(value, "external_xa_empty",
                   &result->external_xa_empty) &&
         json_bool(value, "old_tc_authority_empty",
                   &result->old_tc_authority_empty) &&
         json_bool(value, "user_state_empty", &result->user_state_empty) &&
         json_bool(value, "legacy_live_extents_empty",
                   &result->legacy_live_extents_empty);
}

void write_local_payload(JsonWriter *writer,
                         const LocalSnapshotPayload &value) {
  writer->StartObject();
  write_named_string(writer, "component", value.component);
  write_named_string(writer, "relative_path", value.relative_path);
  write_named_string(writer, "local_path", value.local_path.generic_string());
  write_named_string(writer, "format", value.format);
  writer->EndObject();
}

bool parse_local_payload(const rapidjson::Value &value,
                         LocalSnapshotPayload *result) {
  std::string local_path;
  if (!exact_members(value,
                     {"component", "relative_path", "local_path", "format"}) ||
      !json_string(value, "component", &result->component,
                   kMaxOrdinaryIdBytes) ||
      !json_string(value, "relative_path", &result->relative_path,
                   kMaxObjectKeyBytes) ||
      !json_string(value, "local_path", &local_path, kMaxStartupPathBytes) ||
      !json_string(value, "format", &result->format,
                   kMaxOrdinaryIdBytes)) {
    return false;
  }
  result->local_path = fs::path(local_path);
  return true;
}

void write_pinned_extent(JsonWriter *writer,
                         const PinnedSmartengineExtent &value) {
  writer->StartObject();
  writer->Key("writer_epoch");
  writer->Uint64(value.writer_epoch);
  write_named_string(writer, "allocation_seq", value.allocation_seq);
  write_named_string(writer, "database_name_hex", value.database_name_hex);
  write_named_string(writer, "index_id", value.index_id);
  write_named_string(writer, "object_id", value.object_id);
  write_named_string(writer, "key", value.key);
  writer->Key("size");
  writer->Uint64(value.size);
  write_named_string(writer, "sha256", value.sha256);
  writer->EndObject();
}

bool parse_pinned_extent(const rapidjson::Value &value,
                         PinnedSmartengineExtent *result) {
  return exact_members(value,
                       {"writer_epoch", "allocation_seq",
                        "database_name_hex", "index_id", "object_id", "key",
                        "size", "sha256"}) &&
         json_uint64(value, "writer_epoch", &result->writer_epoch) &&
         result->writer_epoch != 0 &&
         json_string(value, "allocation_seq", &result->allocation_seq,
                     kMaxOrdinaryIdBytes) &&
         json_string(value, "database_name_hex", &result->database_name_hex,
                     kMaxOrdinaryIdBytes) &&
         json_string(value, "index_id", &result->index_id,
                     kMaxOrdinaryIdBytes) &&
         json_string(value, "object_id", &result->object_id,
                     kMaxOrdinaryIdBytes) &&
         json_string(value, "key", &result->key, kMaxObjectKeyBytes) &&
         json_uint64(value, "size", &result->size) && result->size != 0 &&
         json_string(value, "sha256", &result->sha256, 64) &&
         valid_sha256(result->sha256);
}

void write_fixed_cut(JsonWriter *writer, const FixedSnapshotCut &value) {
  writer->StartObject();
  write_named_string(writer, "snapshot_id", value.snapshot_id);
  writer->Key("writer");
  write_writer(writer, value.writer);
  writer->Key("proof");
  write_fixed_cut_proof(writer, value.proof);
  writer->Key("log_anchor");
  write_log_anchor(writer, value.log_anchor);
  write_named_string(writer, "server_uuid", value.server_identity.server_uuid);
  writer->Key("deployment_fingerprints");
  write_fingerprints(writer, value.deployment_fingerprints);
  write_named_string(writer, "binlog_seed_path",
                     value.binlog_seed_path.generic_string());
  writer->Key("objects");
  writer->StartArray();
  for (const LocalSnapshotPayload &object : value.objects)
    write_local_payload(writer, object);
  writer->EndArray();
  writer->Key("smartengine_extents");
  writer->StartArray();
  for (const PinnedSmartengineExtent &extent : value.smartengine_extents)
    write_pinned_extent(writer, extent);
  writer->EndArray();
  writer->EndObject();
}

bool parse_fixed_cut(const rapidjson::Value &value, FixedSnapshotCut *result) {
  if (!exact_members(value,
                     {"snapshot_id", "writer", "proof", "log_anchor",
                      "server_uuid", "deployment_fingerprints",
                      "binlog_seed_path", "objects",
                      "smartengine_extents"}) ||
      !json_string(value, "snapshot_id", &result->snapshot_id,
                   kMaxOrdinaryIdBytes) ||
      !json_string(value, "server_uuid",
                   &result->server_identity.server_uuid, 36) ||
      !valid_uuid(result->server_identity.server_uuid)) {
    return false;
  }
  const auto writer = value.FindMember("writer");
  const auto proof = value.FindMember("proof");
  const auto anchor = value.FindMember("log_anchor");
  const auto fingerprints = value.FindMember("deployment_fingerprints");
  const auto objects = value.FindMember("objects");
  const auto extents = value.FindMember("smartengine_extents");
  std::string binlog_seed;
  if (writer == value.MemberEnd() || proof == value.MemberEnd() ||
      anchor == value.MemberEnd() || fingerprints == value.MemberEnd() ||
      objects == value.MemberEnd() || extents == value.MemberEnd() ||
      !parse_writer(writer->value, &result->writer) ||
      !parse_fixed_cut_proof(proof->value, &result->proof) ||
      !parse_log_anchor(anchor->value, &result->log_anchor) ||
      !parse_fingerprints(fingerprints->value,
                          &result->deployment_fingerprints) ||
      !json_string(value, "binlog_seed_path", &binlog_seed,
                   kMaxStartupPathBytes) ||
      !objects->value.IsArray() || !extents->value.IsArray() ||
      objects->value.Size() > kMaxSnapshotItems ||
      extents->value.Size() > kMaxSnapshotItems - objects->value.Size()) {
    return false;
  }
  result->binlog_seed_path = fs::path(binlog_seed);
  result->objects.clear();
  result->objects.reserve(objects->value.Size());
  for (const rapidjson::Value &entry : objects->value.GetArray()) {
    LocalSnapshotPayload parsed;
    if (!parse_local_payload(entry, &parsed)) return false;
    result->objects.push_back(std::move(parsed));
  }
  result->smartengine_extents.clear();
  result->smartengine_extents.reserve(extents->value.Size());
  for (const rapidjson::Value &entry : extents->value.GetArray()) {
    PinnedSmartengineExtent parsed;
    if (!parse_pinned_extent(entry, &parsed)) return false;
    result->smartengine_extents.push_back(std::move(parsed));
  }
  return true;
}

bool parse_decimal(std::string_view text, uint64_t *value) {
  if (text.empty() || (text.size() > 1 && text.front() == '0')) return false;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), *value);
  return parsed.ec == std::errc() && parsed.ptr == text.data() + text.size() &&
         *value <= kJsonSafeIntegerMax;
}

struct CanonicalExtent {
  PinnedSmartengineExtent value;
  uint64_t allocation{0};
  uint64_t index{0};
  uint64_t object{0};
};

bool canonical_extent_preimage(
    const std::vector<PinnedSmartengineExtent> &extents, std::string *preimage,
    std::string *error) {
  if (extents.size() > kMaxSnapshotItems)
    return fail_with(error, "startup extent set exceeds the item limit");
  std::vector<CanonicalExtent> canonical;
  canonical.reserve(extents.size());
  for (const PinnedSmartengineExtent &extent : extents) {
    CanonicalExtent parsed;
    parsed.value = extent;
    if (extent.writer_epoch == 0 ||
        extent.writer_epoch > kJsonSafeIntegerMax || extent.size == 0 ||
        extent.size > kJsonSafeIntegerMax ||
        !parse_decimal(extent.allocation_seq, &parsed.allocation) ||
        !parse_decimal(extent.index_id, &parsed.index) ||
        !parse_decimal(extent.object_id, &parsed.object) ||
        !valid_text(extent.database_name_hex, kMaxOrdinaryIdBytes) ||
        !valid_text(extent.key, kMaxObjectKeyBytes) ||
        !valid_sha256(extent.sha256)) {
      return fail_with(error,
                       "startup extent set contains an invalid identity");
    }
    canonical.push_back(std::move(parsed));
  }
  std::sort(canonical.begin(), canonical.end(),
            [](const CanonicalExtent &left, const CanonicalExtent &right) {
              return std::tie(left.value.writer_epoch, left.allocation,
                              left.value.database_name_hex, left.index,
                              left.object, left.value.key) <
                     std::tie(right.value.writer_epoch, right.allocation,
                              right.value.database_name_hex, right.index,
                              right.object, right.value.key);
            });
  for (size_t index = 1; index < canonical.size(); ++index) {
    const CanonicalExtent &left = canonical[index - 1];
    const CanonicalExtent &right = canonical[index];
    if (std::tie(left.value.writer_epoch, left.allocation,
                 left.value.database_name_hex, left.index, left.object,
                 left.value.key) ==
        std::tie(right.value.writer_epoch, right.allocation,
                 right.value.database_name_hex, right.index, right.object,
                 right.value.key)) {
      return fail_with(error, "startup extent set contains a duplicate");
    }
  }
  rapidjson::StringBuffer buffer;
  JsonWriter writer(buffer);
  writer.StartArray();
  for (const CanonicalExtent &extent : canonical)
    write_pinned_extent(&writer, extent.value);
  writer.EndArray();
  if (!writer.IsComplete() || buffer.GetSize() > kSnapshotManifestMaxBytes)
    return fail_with(error,
                     "startup extent set canonical form exceeds its bound");
  preimage->assign(buffer.GetString(), buffer.GetSize());
  return true;
}

void write_materialized(JsonWriter *writer, const MaterializedRoot &value) {
  writer->StartObject();
  writer->Key("binlog_files");
  writer->StartArray();
  for (const fs::path &path : value.binlog_files)
    write_string(writer, path.generic_string());
  writer->EndArray();
  write_named_string(writer, "binlog_index", value.binlog_index.generic_string());
  writer->Key("verified_payload_bytes");
  writer->Uint64(value.verified_payload_bytes);
  writer->EndObject();
}

bool parse_materialized(const rapidjson::Value &value,
                        MaterializedRoot *result) {
  if (!exact_members(value,
                     {"binlog_files", "binlog_index",
                      "verified_payload_bytes"})) {
    return false;
  }
  const auto files = value.FindMember("binlog_files");
  std::string index;
  if (files == value.MemberEnd() || !files->value.IsArray() ||
      files->value.Size() > kRecoverySegmentCountMax ||
      !json_string(value, "binlog_index", &index, kMaxStartupPathBytes) ||
      !json_uint64(value, "verified_payload_bytes",
                   &result->verified_payload_bytes)) {
    return false;
  }
  result->binlog_index = fs::path(index);
  result->binlog_files.clear();
  result->binlog_files.reserve(files->value.Size());
  for (const rapidjson::Value &entry : files->value.GetArray()) {
    if (!entry.IsString() || entry.GetStringLength() == 0 ||
        entry.GetStringLength() > kMaxStartupPathBytes) {
      return false;
    }
    result->binlog_files.emplace_back(
        std::string(entry.GetString(), entry.GetStringLength()));
  }
  return !result->binlog_files.empty();
}

bool parse_marker_body(const rapidjson::Value &value,
                       LocalInstallMarker *marker, std::string *error) {
  if (!value.IsString() || value.GetStringLength() == 0 ||
      value.GetStringLength() > kLocalInstallMarkerMaxBytes) {
    return fail_with(error, "startup install marker body is invalid");
  }
  const std::string_view body(value.GetString(), value.GetStringLength());
  return parse_local_install_marker(body, marker, error);
}

bool expected_bootstrap_preflight_digest(
    const StartupBootstrapPreflight &value, std::string *digest,
    std::string *error) {
  std::string root;
  if (digest == nullptr || !value.root.is_absolute() ||
      !path_string(value.root, &root) || !valid_sha256(value.request_nonce) ||
      !valid_deployment_shape(value.initialized_deployment)) {
    return fail_with(error,
                     "bootstrap preflight request identity is invalid");
  }
  std::string preimage(kBootstrapPreflightRequestDomain);
  preimage.push_back('\0');
  preimage.append(value.request_nonce);
  preimage.push_back('\0');
  preimage.append(root);
  preimage.push_back('\0');
  preimage.append(value.initialized_deployment.stream_id);
  return sha256_hex(preimage, digest, error);
}

bool serialize_preflight_impl(const StartupBootstrapPreflight &value,
                              std::string *json, std::string *error) {
  std::string root;
  std::string expected_digest;
  if (json == nullptr || !value.root.is_absolute() ||
      !path_string(value.root, &root) ||
      !valid_sha256(value.request_nonce) ||
      !valid_sha256(value.request_sha256) ||
      !valid_deployment_shape(value.initialized_deployment) ||
      !expected_bootstrap_preflight_digest(value, &expected_digest, error) ||
      value.request_sha256 != expected_digest) {
    return fail_with(error, "bootstrap preflight contains an invalid field");
  }
  rapidjson::StringBuffer buffer;
  JsonWriter writer(buffer);
  writer.StartObject();
  write_named_string(&writer, "format", kPreflightFormat);
  writer.Key("version");
  writer.Uint64(kDocumentVersion);
  write_named_string(&writer, "root", root);
  write_named_string(&writer, "request_nonce", value.request_nonce);
  write_named_string(&writer, "request_sha256", value.request_sha256);
  writer.Key("child_exited_cleanly");
  writer.Bool(value.child_exited_cleanly);
  writer.Key("initialized_deployment");
  write_deployment(&writer, value.initialized_deployment);
  writer.Key("dd_initialized");
  writer.Bool(value.dd_initialized);
  writer.Key("repository_empty");
  writer.Bool(value.repository_empty);
  writer.Key("internal_prepared_empty");
  writer.Bool(value.internal_prepared_empty);
  writer.Key("external_xa_empty");
  writer.Bool(value.external_xa_empty);
  writer.Key("empty_source_scan_stable");
  writer.Bool(value.empty_source_scan_stable);
  writer.Key("old_tc_authority_empty");
  writer.Bool(value.old_tc_authority_empty);
  writer.Key("user_state_empty");
  writer.Bool(value.user_state_empty);
  writer.Key("legacy_live_extents_empty");
  writer.Bool(value.legacy_live_extents_empty);
  writer.EndObject();
  if (!writer.IsComplete() ||
      buffer.GetSize() > kStartupBootstrapPreflightMaxBytes) {
    return fail_with(error, "bootstrap preflight exceeds its canonical bound");
  }
  json->assign(buffer.GetString(), buffer.GetSize());
  return true;
}

bool serialize_worker_request_impl(const StreamIdentity &stream,
                                   const StartupWorkerRequest &value,
                                   bool require_digest, std::string *json,
                                   std::string *error) {
  std::string root;
  WriterEpoch parsed_epoch;
  std::string parse_error;
  if (json == nullptr || !value.root.is_absolute() ||
      !path_string(value.root, &root) ||
      !valid_deployment(stream, value.deployment) ||
      !valid_sha256(value.request_nonce) ||
      (require_digest && !valid_sha256(value.request_sha256)) ||
      (!require_digest && !value.request_sha256.empty()) ||
      !valid_acquired_epoch(value.epoch) ||
      !parse_writer_epoch(value.epoch.object.body, stream, &parsed_epoch,
                          &parse_error) ||
      parsed_epoch != value.epoch.value) {
    return fail_with(error, "startup worker request contains an invalid field");
  }
  const bool bootstrap = value.route == StartupCoordinatorRoute::BOOTSTRAP;
  if (bootstrap != !value.candidate.has_value() ||
      bootstrap != !value.materialized.has_value()) {
    return fail_with(error, "startup worker request route shape is invalid");
  }
  if (!bootstrap) {
    Head parsed_head;
    const StartupCandidateBoundary &candidate = *value.candidate;
    if (candidate.head.object.body.empty() ||
        candidate.head.object.body.size() > kHeadMaxBytes ||
        !valid_text(candidate.head.object.etag, kMaxObjectKeyBytes) ||
        !parse_head(candidate.head.object.body, stream, &parsed_head,
                    &parse_error) ||
        parsed_head != candidate.head.value ||
        candidate.replay_exclusive_start != parsed_head.snapshot.cursor ||
        candidate.replay_inclusive_end != parsed_head.durable_cursor ||
        !valid_cursor(candidate.replay_exclusive_start) ||
        !valid_cursor(candidate.replay_inclusive_end) ||
        value.materialized->binlog_files.empty() ||
        !path_within_root(value.root, value.materialized->binlog_index)) {
      return fail_with(error,
                       "startup takeover worker boundary is invalid");
    }
    for (const fs::path &file : value.materialized->binlog_files) {
      if (!path_within_root(value.root, file))
        return fail_with(error,
                         "startup materialized binlog escapes worker root");
    }
  }

  rapidjson::StringBuffer buffer;
  JsonWriter writer(buffer);
  writer.StartObject();
  write_named_string(&writer, "format", kWorkerRequestFormat);
  writer.Key("version");
  writer.Uint64(kDocumentVersion);
  write_named_string(&writer, "route", route_name(value.route));
  write_named_string(&writer, "request_nonce", value.request_nonce);
  write_named_string(&writer, "request_sha256", value.request_sha256);
  write_named_string(&writer, "root", root);
  writer.Key("deployment");
  write_deployment(&writer, value.deployment);
  writer.Key("epoch");
  write_epoch(&writer, value.epoch);
  writer.Key("candidate");
  if (value.candidate.has_value()) {
    writer.StartObject();
    writer.Key("head");
    write_published_head(&writer, value.candidate->head);
    writer.Key("replay_exclusive_start");
    write_cursor(&writer, value.candidate->replay_exclusive_start);
    writer.Key("replay_inclusive_end");
    write_cursor(&writer, value.candidate->replay_inclusive_end);
    writer.EndObject();
  } else {
    writer.Null();
  }
  writer.Key("materialized");
  if (value.materialized.has_value()) {
    write_materialized(&writer, *value.materialized);
  } else {
    writer.Null();
  }
  writer.EndObject();
  if (!writer.IsComplete() || buffer.GetSize() > kStartupWorkerRequestMaxBytes)
    return fail_with(error,
                     "startup worker request exceeds its canonical bound");
  json->assign(buffer.GetString(), buffer.GetSize());
  return true;
}

bool expected_worker_request_digest(const StreamIdentity &stream,
                                    const StartupWorkerRequest &value,
                                    std::string *digest,
                                    std::string *error) {
  StartupWorkerRequest unsigned_value = value;
  unsigned_value.request_sha256.clear();
  std::string preimage;
  if (!serialize_worker_request_impl(stream, unsigned_value, false, &preimage,
                                     error)) {
    return false;
  }
  return sha256_hex(preimage, digest, error);
}

bool serialize_worker_completion_impl(const StartupWorkerCompletion &value,
                                      std::string *json,
                                      std::string *error) {
  if (json == nullptr || !valid_sha256(value.request_nonce) ||
      !valid_sha256(value.request_sha256) ||
      !complete_root_evidence(value.root_evidence, false)) {
    return fail_with(error,
                     "startup worker completion contains an invalid field");
  }
  rapidjson::StringBuffer buffer;
  JsonWriter writer(buffer);
  writer.StartObject();
  write_named_string(&writer, "format", kWorkerCompletionFormat);
  writer.Key("version");
  writer.Uint64(kDocumentVersion);
  writer.Key("child_exited_cleanly");
  writer.Bool(value.child_exited_cleanly);
  write_named_string(&writer, "request_nonce", value.request_nonce);
  write_named_string(&writer, "request_sha256", value.request_sha256);
  writer.Key("root_evidence");
  write_root_evidence(&writer, value.root_evidence);
  writer.Key("snapshot_cut");
  write_fixed_cut(&writer, value.snapshot_cut);
  writer.EndObject();
  if (!writer.IsComplete() ||
      buffer.GetSize() > kStartupWorkerEvidenceMaxBytes) {
    return fail_with(error,
                     "startup worker completion exceeds its canonical bound");
  }
  json->assign(buffer.GetString(), buffer.GetSize());
  return true;
}

bool serialize_restart_proof_impl(const StreamIdentity &stream,
                                  const StartupRestartProof &value,
                                  std::string *json, std::string *error) {
  WriterEpoch parsed_epoch;
  Head parsed_head;
  std::string parse_error;
  std::string marker_body;
  const Writer owner{value.epoch.value.writer_id, value.epoch.value.epoch};
  const bool bootstrap = value.route == StartupCoordinatorRoute::BOOTSTRAP;
  if (json == nullptr || !valid_deployment(stream, value.deployment) ||
      !valid_acquired_epoch(value.epoch) ||
      !parse_writer_epoch(value.epoch.object.body, stream, &parsed_epoch,
                          &parse_error) ||
      parsed_epoch != value.epoch.value || value.head.object.body.empty() ||
      value.head.object.body.size() > kHeadMaxBytes ||
      !valid_text(value.head.object.etag, kMaxObjectKeyBytes) ||
      !parse_head(value.head.object.body, stream, &parsed_head, &parse_error) ||
      parsed_head != value.head.value ||
      value.head.value.writer != owner ||
      (bootstrap && (value.head.value.generation != 1 ||
                     value.head.value.parent.has_value())) ||
      (!bootstrap && (value.head.value.generation <= 1 ||
                      !value.head.value.parent.has_value())) ||
      !complete_root_evidence(value.expected_root, false) ||
      value.expected_root.recovered_cursor !=
          value.head.value.durable_cursor ||
      !serialize_local_install_marker(value.marker, &marker_body, error) ||
      !valid_stable_root_manifest(value.stopped_worker_root) ||
      !valid_stable_root_manifest(value.installed_root) ||
      value.stopped_worker_root.entry_count == kJsonSafeIntegerMax ||
      value.stopped_worker_root.regular_file_count == kJsonSafeIntegerMax ||
      value.stopped_worker_root.regular_file_bytes >
          kJsonSafeIntegerMax - marker_body.size() ||
      value.installed_root.entry_count !=
          value.stopped_worker_root.entry_count + 1 ||
      value.installed_root.regular_file_count !=
          value.stopped_worker_root.regular_file_count + 1 ||
      value.installed_root.regular_file_bytes !=
          value.stopped_worker_root.regular_file_bytes + marker_body.size()) {
    return fail_with(error, "startup restart proof contains an invalid field");
  }
  rapidjson::StringBuffer buffer;
  JsonWriter writer(buffer);
  writer.StartObject();
  write_named_string(&writer, "format", kRestartProofFormat);
  writer.Key("version");
  writer.Uint64(kDocumentVersion);
  write_named_string(&writer, "route", route_name(value.route));
  writer.Key("deployment");
  write_deployment(&writer, value.deployment);
  writer.Key("head");
  write_published_head(&writer, value.head);
  writer.Key("epoch");
  write_epoch(&writer, value.epoch);
  writer.Key("marker");
  write_string(&writer, marker_body);
  writer.Key("expected_root");
  write_root_evidence(&writer, value.expected_root);
  writer.Key("stopped_worker_root");
  write_stable_root_manifest(&writer, value.stopped_worker_root);
  writer.Key("installed_root");
  write_stable_root_manifest(&writer, value.installed_root);
  writer.EndObject();
  if (!writer.IsComplete() || buffer.GetSize() > kStartupRestartProofMaxBytes)
    return fail_with(error,
                     "startup restart proof exceeds its canonical bound");
  json->assign(buffer.GetString(), buffer.GetSize());
  return true;
}

bool canonical_matches(std::string_view input, const std::string &canonical,
                       std::string *error) {
  return input == canonical ||
         fail_with(error, "startup document is not in canonical form");
}

StartupCoordinatorOutcome map_read_outcome(RecoveryReadOutcome outcome) {
  switch (outcome) {
    case RecoveryReadOutcome::BLOCKED:
      return StartupCoordinatorOutcome::BLOCKED;
    case RecoveryReadOutcome::CORRUPT:
      return StartupCoordinatorOutcome::CORRUPT;
    case RecoveryReadOutcome::READY:
    case RecoveryReadOutcome::EMPTY:
      break;
  }
  return StartupCoordinatorOutcome::CORRUPT;
}

StartupCoordinatorOutcome map_materialize_outcome(
    MaterializeOutcome outcome) {
  switch (outcome) {
    case MaterializeOutcome::BLOCKED:
      return StartupCoordinatorOutcome::BLOCKED;
    case MaterializeOutcome::CORRUPT:
      return StartupCoordinatorOutcome::CORRUPT;
    case MaterializeOutcome::LOCAL_IO_ERROR:
      return StartupCoordinatorOutcome::LOCAL_IO_ERROR;
    case MaterializeOutcome::READY:
      break;
  }
  return StartupCoordinatorOutcome::CORRUPT;
}

StartupCoordinatorOutcome map_install_outcome(InstallOutcome outcome) {
  switch (outcome) {
    case InstallOutcome::FOREIGN_OR_CORRUPT:
      return StartupCoordinatorOutcome::FOREIGN_OR_CORRUPT;
    case InstallOutcome::UNSUPPORTED:
      return StartupCoordinatorOutcome::UNSUPPORTED;
    case InstallOutcome::LOCAL_IO_ERROR:
      return StartupCoordinatorOutcome::LOCAL_IO_ERROR;
    case InstallOutcome::INSTALLED:
      break;
  }
  return StartupCoordinatorOutcome::CORRUPT;
}

bool validate_published_identity(const PublishedStartupHead &expected,
                                 const RecoveryPlan &actual,
                                 const ExactWriterEpoch &epoch,
                                 std::string *error) {
  if (expected.object.body.empty() || expected.object.etag.empty() ||
      expected.value != actual.head ||
      !same_object(expected.object, actual.head_object)) {
    return fail_with(error,
                     "published HEAD does not match its exact read-back");
  }
  if (!same_epoch(actual, epoch))
    return fail_with(
        error, "published HEAD read-back lost the exact acquired epoch");
  const Writer owner{epoch.value.writer_id, epoch.value.epoch};
  if (actual.head.writer != owner || actual.snapshot.writer != owner)
    return fail_with(
        error, "published HEAD and snapshot are not owned by the acquired epoch");
  return true;
}

bool validate_bootstrap_publication(const RecoveryPlan &published,
                                    const StartupRootEvidence &evidence,
                                    std::string *error) {
  if (published.manifests.empty() ||
      published.manifests.front().value.kind != ManifestKind::BOOTSTRAP ||
      published.head.generation != 1 || published.head.parent.has_value() ||
      published.head.base_cursor != evidence.recovered_cursor ||
      published.head.durable_cursor != evidence.recovered_cursor ||
      published.head.snapshot.cursor != evidence.recovered_cursor ||
      published.snapshot.log_anchor.kind != LogAnchorKind::EMPTY_BASE) {
    return fail_with(
        error, "published BOOTSTRAP does not bind the initialized root cut");
  }
  return true;
}

bool validate_takeover_publication(const RecoveryPlan &candidate,
                                   const RecoveryPlan &published,
                                   std::string *error) {
  if (candidate.head.generation == kJsonSafeIntegerMax)
    return fail_with(error, "candidate HEAD generation is exhausted");
  std::string candidate_sha;
  if (!sha256_hex(candidate.head_object.body, &candidate_sha, error))
    return false;
  const HeadParent expected_parent{candidate.head.generation,
                                   candidate.head_object.etag,
                                   std::move(candidate_sha)};
  if (published.manifests.empty() ||
      published.manifests.front().value.kind != ManifestKind::SNAPSHOT ||
      published.head.generation != candidate.head.generation + 1 ||
      !published.head.parent.has_value() ||
      *published.head.parent != expected_parent ||
      published.head.durable_cursor != candidate.head.durable_cursor ||
      published.head.base_cursor != candidate.head.durable_cursor ||
      published.head.snapshot.cursor != candidate.head.durable_cursor ||
      published.snapshot.log_anchor.kind !=
          LogAnchorKind::MANIFEST_BOUNDARY ||
      !published.snapshot.log_anchor.generation.has_value() ||
      *published.snapshot.log_anchor.generation != candidate.head.generation ||
      !published.snapshot.log_anchor.manifest.has_value() ||
      *published.snapshot.log_anchor.manifest != candidate.head.manifest ||
      published.snapshot.log_anchor.cursor != candidate.head.durable_cursor ||
      published.snapshot.server_identity != candidate.snapshot.server_identity ||
      published.snapshot.deployment_fingerprints !=
          candidate.snapshot.deployment_fingerprints) {
    return fail_with(
        error, "published takeover SNAPSHOT does not exactly rebase the candidate");
  }
  return true;
}

bool path_within_root(const fs::path &root, const fs::path &path) {
  if (root.empty() || path.empty()) return false;
  const fs::path normalized_root = root.lexically_normal();
  const fs::path normalized_path = path.lexically_normal();
  auto root_it = normalized_root.begin();
  auto path_it = normalized_path.begin();
  for (; root_it != normalized_root.end(); ++root_it, ++path_it) {
    if (path_it == normalized_path.end() || *root_it != *path_it) return false;
  }
  return path_it != normalized_path.end();
}

bool valid_root_snapshot(const StartupRootSnapshot &snapshot,
                         std::string *error) {
  if (!valid_stable_root_manifest(snapshot.manifest) ||
      snapshot.entries.size() != snapshot.manifest.entry_count) {
    return fail_with(error, "stopped-root manifest summary is invalid");
  }
  uint64_t regular_files = 0;
  uint64_t regular_bytes = 0;
  std::string_view previous;
  for (const StartupRootEntry &entry : snapshot.entries) {
    const fs::path relative(entry.relative_path);
    if (entry.relative_path.empty() || relative.is_absolute() ||
        relative.lexically_normal() != relative ||
        (!previous.empty() && previous >= entry.relative_path)) {
      return fail_with(error,
                       "stopped-root entries are not canonical and ordered");
    }
    previous = entry.relative_path;
    if (entry.type == StartupRootEntryType::DIRECTORY) {
      if (entry.size != 0 || !entry.sha256.empty())
        return fail_with(error, "stopped-root directory entry is invalid");
      continue;
    }
    if (entry.type != StartupRootEntryType::REGULAR_FILE ||
        !valid_sha256(entry.sha256) ||
        entry.size > kJsonSafeIntegerMax - regular_bytes) {
      return fail_with(error, "stopped-root regular-file entry is invalid");
    }
    ++regular_files;
    regular_bytes += entry.size;
  }
  if (regular_files != snapshot.manifest.regular_file_count ||
      regular_bytes != snapshot.manifest.regular_file_bytes) {
    return fail_with(error, "stopped-root summary does not match its entries");
  }
  return true;
}

bool validate_marker_only_install(const StartupRootSnapshot &stopped,
                                  const StartupRootSnapshot &installed,
                                  const LocalInstallMarker &marker,
                                  std::string *error) {
  if (!valid_root_snapshot(stopped, error) ||
      !valid_root_snapshot(installed, error)) {
    return false;
  }
  std::string marker_body;
  std::string marker_sha;
  if (!serialize_local_install_marker(marker, &marker_body, error) ||
      !sha256_hex(marker_body, &marker_sha, error)) {
    return false;
  }
  StartupRootEntry marker_entry{kLocalInstallMarkerName,
                                StartupRootEntryType::REGULAR_FILE,
                                static_cast<uint64_t>(marker_body.size()),
                                std::move(marker_sha)};
  std::vector<StartupRootEntry> expected = stopped.entries;
  if (std::any_of(expected.begin(), expected.end(),
                  [](const StartupRootEntry &entry) {
                    return entry.relative_path == kLocalInstallMarkerName;
                  })) {
    return fail_with(error,
                     "worker root already contains an install marker");
  }
  expected.push_back(std::move(marker_entry));
  std::sort(expected.begin(), expected.end(),
            [](const StartupRootEntry &left, const StartupRootEntry &right) {
              return left.relative_path < right.relative_path;
            });
  if (expected != installed.entries ||
      installed.manifest.entry_count != stopped.manifest.entry_count + 1 ||
      installed.manifest.regular_file_count !=
          stopped.manifest.regular_file_count + 1 ||
      installed.manifest.regular_file_bytes !=
          stopped.manifest.regular_file_bytes + marker_body.size()) {
    return fail_with(error,
                     "installed root differs from the stopped worker root");
  }
  return true;
}

bool validate_worker_cut(StartupCoordinatorRoute route,
                         const StartupCoordinatorOptions &options,
                         const ExactWriterEpoch &epoch,
                         const RecoveryPlan *candidate,
                         const StartupWorkerCompletion &completion,
                         std::string *error) {
  const FixedSnapshotCut &cut = completion.snapshot_cut;
  const StartupRootEvidence &evidence = completion.root_evidence;
  const Writer owner{epoch.value.writer_id, epoch.value.epoch};
  uint64_t extent_count = 0;
  std::string extent_sha;
  if (!complete_root_evidence(evidence, false) || evidence.marker_matches ||
      evidence.snapshot_matches || cut.writer != owner ||
      cut.server_identity.server_uuid != options.deployment.server_uuid ||
      cut.deployment_fingerprints != options.deployment.fingerprints ||
      cut.proof.public_cursor != evidence.recovered_cursor ||
      cut.proof.image_cursor != evidence.recovered_cursor ||
      cut.proof.public_gtid != evidence.recovered_gtid ||
      cut.proof.image_gtid != evidence.recovered_gtid ||
      !path_within_root(options.materialize.temp_root,
                        cut.binlog_seed_path) ||
      !startup_extent_set_digest(cut.smartengine_extents, &extent_count,
                                 &extent_sha, error) ||
      extent_count != evidence.exported_extent_count ||
      extent_sha != evidence.exported_extent_set_sha256) {
    return fail_with(
        error, "worker evidence does not bind one exact stable snapshot cut");
  }
  for (const LocalSnapshotPayload &object : cut.objects) {
    const fs::path relative(object.relative_path);
    if (relative.empty() || relative.is_absolute() ||
        relative.lexically_normal() != relative ||
        !path_within_root(options.materialize.temp_root, object.local_path)) {
      return fail_with(error,
                       "worker snapshot payload escapes the stable root");
    }
  }
  if (route == StartupCoordinatorRoute::BOOTSTRAP) {
    if (candidate != nullptr || cut.proof.source != FixedCutSource::EMPTY_SOURCE ||
        cut.log_anchor.kind != LogAnchorKind::EMPTY_BASE ||
        cut.proof.source_head_generation != 0 ||
        !cut.proof.source_head_body_sha256.empty() ||
        !cut.proof.empty_source_scan_stable ||
        !cut.proof.internal_prepared_empty ||
        !cut.proof.external_xa_empty || !cut.proof.old_tc_authority_empty ||
        !cut.proof.user_state_empty ||
        !cut.proof.legacy_live_extents_empty) {
      return fail_with(error,
                       "bootstrap worker cut lost its EMPTY_SOURCE preflight");
    }
  } else {
    if (candidate == nullptr ||
        cut.proof.source != FixedCutSource::RECOVERED_TAKEOVER ||
        cut.log_anchor.kind != LogAnchorKind::MANIFEST_BOUNDARY ||
        !cut.log_anchor.generation.has_value() ||
        *cut.log_anchor.generation != candidate->head.generation ||
        !cut.log_anchor.manifest.has_value() ||
        *cut.log_anchor.manifest != candidate->head.manifest ||
        cut.log_anchor.cursor != candidate->head.durable_cursor ||
        cut.proof.source_head_generation != candidate->head.generation ||
        evidence.recovered_cursor != candidate->head.durable_cursor ||
        cut.proof.empty_source_scan_stable ||
        cut.proof.internal_prepared_empty || cut.proof.external_xa_empty ||
        cut.proof.old_tc_authority_empty || cut.proof.user_state_empty ||
        cut.proof.legacy_live_extents_empty) {
      return fail_with(error,
                       "takeover worker cut lost its exact candidate boundary");
    }
    std::string candidate_sha;
    if (!sha256_hex(candidate->head_object.body, &candidate_sha, error) ||
        cut.proof.source_head_body_sha256 != candidate_sha) {
      return fail_with(error,
                       "takeover worker cut names another candidate HEAD");
    }
  }
  return true;
}

bool valid_restart_reference(const StartupRestartProofReference &reference) {
  std::string path;
  return path_string(reference.path, &path) && reference.path.is_absolute() &&
         reference.size != 0 &&
         reference.size <= kStartupRestartProofMaxBytes &&
         valid_sha256(reference.sha256);
}

}  // namespace

bool startup_extent_set_digest(
    const std::vector<PinnedSmartengineExtent> &extents, uint64_t *count,
    std::string *sha256, std::string *error) {
  if (count == nullptr || sha256 == nullptr || error == nullptr)
    return fail_with(error, "startup extent digest output is null");
  std::string preimage;
  if (!canonical_extent_preimage(extents, &preimage, error)) return false;
  if (!sha256_hex(preimage, sha256, error)) return false;
  *count = static_cast<uint64_t>(extents.size());
  return true;
}

bool finalize_startup_bootstrap_preflight(
    StartupBootstrapPreflight *value, std::string *error) {
  if (value == nullptr || error == nullptr)
    return fail_with(error, "bootstrap preflight output is null");
  value->request_sha256.clear();
  return expected_bootstrap_preflight_digest(*value, &value->request_sha256,
                                             error);
}

bool serialize_startup_bootstrap_preflight(
    const StartupBootstrapPreflight &value, std::string *json,
    std::string *error) {
  return serialize_preflight_impl(value, json, error);
}

bool parse_startup_bootstrap_preflight(
    std::string_view json, StartupBootstrapPreflight *value,
    std::string *error) {
  if (value == nullptr || error == nullptr)
    return fail_with(error, "bootstrap preflight parse output is null");
  rapidjson::Document document;
  if (!parse_document(json, kStartupBootstrapPreflightMaxBytes, &document,
                      error) ||
      !exact_members(document,
                     {"format", "version", "root", "request_nonce",
                      "request_sha256", "child_exited_cleanly",
                      "initialized_deployment", "dd_initialized",
                      "repository_empty", "internal_prepared_empty",
                      "external_xa_empty", "empty_source_scan_stable",
                      "old_tc_authority_empty", "user_state_empty",
                      "legacy_live_extents_empty"}) ||
      !validate_envelope(document, kPreflightFormat)) {
    return fail_with(error, "bootstrap preflight schema is invalid");
  }
  StartupBootstrapPreflight parsed;
  std::string root;
  const auto initialized_deployment =
      document.FindMember("initialized_deployment");
  if (!json_string(document, "root", &root, kMaxStartupPathBytes) ||
      !json_string(document, "request_nonce", &parsed.request_nonce, 64) ||
      !valid_sha256(parsed.request_nonce) ||
      !json_string(document, "request_sha256", &parsed.request_sha256, 64) ||
      !valid_sha256(parsed.request_sha256) ||
      !json_bool(document, "child_exited_cleanly",
                 &parsed.child_exited_cleanly) ||
      initialized_deployment == document.MemberEnd() ||
      !parse_deployment(initialized_deployment->value,
                        &parsed.initialized_deployment) ||
      !valid_deployment_shape(parsed.initialized_deployment) ||
      !json_bool(document, "dd_initialized", &parsed.dd_initialized) ||
      !json_bool(document, "repository_empty", &parsed.repository_empty) ||
      !json_bool(document, "internal_prepared_empty",
                 &parsed.internal_prepared_empty) ||
      !json_bool(document, "external_xa_empty", &parsed.external_xa_empty) ||
      !json_bool(document, "empty_source_scan_stable",
                 &parsed.empty_source_scan_stable) ||
      !json_bool(document, "old_tc_authority_empty",
                 &parsed.old_tc_authority_empty) ||
      !json_bool(document, "user_state_empty", &parsed.user_state_empty) ||
      !json_bool(document, "legacy_live_extents_empty",
                 &parsed.legacy_live_extents_empty)) {
    return fail_with(error, "bootstrap preflight fields are invalid");
  }
  parsed.root = fs::path(root);
  std::string canonical;
  if (!serialize_preflight_impl(parsed, &canonical, error) ||
      !canonical_matches(json, canonical, error)) {
    return false;
  }
  *value = std::move(parsed);
  return true;
}

bool finalize_startup_worker_request(const StreamIdentity &stream,
                                     StartupWorkerRequest *value,
                                     std::string *error) {
  if (value == nullptr || error == nullptr)
    return fail_with(error, "startup worker request output is null");
  value->request_sha256.clear();
  return expected_worker_request_digest(stream, *value,
                                        &value->request_sha256, error);
}

bool serialize_startup_worker_request(const StreamIdentity &stream,
                                      const StartupWorkerRequest &value,
                                      std::string *json,
                                      std::string *error) {
  std::string expected;
  if (!expected_worker_request_digest(stream, value, &expected, error) ||
      expected != value.request_sha256) {
    return fail_with(error,
                     "startup worker request digest does not bind its body");
  }
  return serialize_worker_request_impl(stream, value, true, json, error);
}

bool parse_startup_worker_request(const StreamIdentity &stream,
                                  std::string_view json,
                                  StartupWorkerRequest *value,
                                  std::string *error) {
  if (value == nullptr || error == nullptr)
    return fail_with(error, "startup worker request parse output is null");
  rapidjson::Document document;
  if (!parse_document(json, kStartupWorkerRequestMaxBytes, &document, error) ||
      !exact_members(document,
                     {"format", "version", "route", "request_nonce",
                      "request_sha256", "root", "deployment", "epoch",
                      "candidate", "materialized"}) ||
      !validate_envelope(document, kWorkerRequestFormat)) {
    return fail_with(error, "startup worker request schema is invalid");
  }
  StartupWorkerRequest parsed;
  std::string root;
  const auto deployment = document.FindMember("deployment");
  const auto epoch = document.FindMember("epoch");
  const auto candidate = document.FindMember("candidate");
  const auto materialized = document.FindMember("materialized");
  if (!parse_route(document, "route", &parsed.route) ||
      !json_string(document, "request_nonce", &parsed.request_nonce, 64) ||
      !valid_sha256(parsed.request_nonce) ||
      !json_string(document, "request_sha256", &parsed.request_sha256, 64) ||
      !valid_sha256(parsed.request_sha256) ||
      !json_string(document, "root", &root, kMaxStartupPathBytes) ||
      deployment == document.MemberEnd() || epoch == document.MemberEnd() ||
      candidate == document.MemberEnd() ||
      materialized == document.MemberEnd() ||
      !parse_deployment(deployment->value, &parsed.deployment) ||
      !valid_deployment(stream, parsed.deployment) ||
      !parse_epoch(epoch->value, stream, &parsed.epoch, error)) {
    return fail_with(error, "startup worker request fields are invalid");
  }
  parsed.root = fs::path(root);
  if (candidate->value.IsNull()) {
    parsed.candidate.reset();
  } else {
    if (!exact_members(candidate->value,
                       {"head", "replay_exclusive_start",
                        "replay_inclusive_end"})) {
      return fail_with(error, "startup worker candidate schema is invalid");
    }
    StartupCandidateBoundary boundary;
    const auto head = candidate->value.FindMember("head");
    const auto start = candidate->value.FindMember("replay_exclusive_start");
    const auto end = candidate->value.FindMember("replay_inclusive_end");
    if (head == candidate->value.MemberEnd() ||
        start == candidate->value.MemberEnd() ||
        end == candidate->value.MemberEnd() ||
        !parse_published_head(head->value, stream, &boundary.head, error) ||
        !parse_cursor(start->value, &boundary.replay_exclusive_start) ||
        !parse_cursor(end->value, &boundary.replay_inclusive_end)) {
      return fail_with(error, "startup worker candidate fields are invalid");
    }
    parsed.candidate = std::move(boundary);
  }
  if (materialized->value.IsNull()) {
    parsed.materialized.reset();
  } else {
    MaterializedRoot root_value;
    if (!parse_materialized(materialized->value, &root_value))
      return fail_with(error,
                       "startup worker materialized-root fields are invalid");
    parsed.materialized = std::move(root_value);
  }
  std::string expected;
  std::string canonical;
  if (!expected_worker_request_digest(stream, parsed, &expected, error) ||
      expected != parsed.request_sha256 ||
      !serialize_worker_request_impl(stream, parsed, true, &canonical, error) ||
      !canonical_matches(json, canonical, error)) {
    return fail_with(error,
                     "startup worker request digest or canonical form is invalid");
  }
  *value = std::move(parsed);
  return true;
}

bool serialize_startup_worker_completion(
    const StartupWorkerCompletion &value, std::string *json,
    std::string *error) {
  return serialize_worker_completion_impl(value, json, error);
}

bool parse_startup_worker_completion(std::string_view json,
                                     StartupWorkerCompletion *value,
                                     std::string *error) {
  if (value == nullptr || error == nullptr)
    return fail_with(error, "startup worker completion parse output is null");
  rapidjson::Document document;
  if (!parse_document(json, kStartupWorkerEvidenceMaxBytes, &document, error) ||
      !exact_members(document,
                     {"format", "version", "child_exited_cleanly",
                      "request_nonce", "request_sha256", "root_evidence",
                      "snapshot_cut"}) ||
      !validate_envelope(document, kWorkerCompletionFormat)) {
    return fail_with(error, "startup worker completion schema is invalid");
  }
  StartupWorkerCompletion parsed;
  const auto evidence = document.FindMember("root_evidence");
  const auto cut = document.FindMember("snapshot_cut");
  if (!json_bool(document, "child_exited_cleanly",
                 &parsed.child_exited_cleanly) ||
      !json_string(document, "request_nonce", &parsed.request_nonce, 64) ||
      !valid_sha256(parsed.request_nonce) ||
      !json_string(document, "request_sha256", &parsed.request_sha256, 64) ||
      !valid_sha256(parsed.request_sha256) ||
      evidence == document.MemberEnd() || cut == document.MemberEnd() ||
      !parse_root_evidence(evidence->value, &parsed.root_evidence) ||
      !parse_fixed_cut(cut->value, &parsed.snapshot_cut)) {
    return fail_with(error, "startup worker completion fields are invalid");
  }
  std::string canonical;
  if (!serialize_worker_completion_impl(parsed, &canonical, error) ||
      !canonical_matches(json, canonical, error)) {
    return false;
  }
  *value = std::move(parsed);
  return true;
}

bool serialize_startup_restart_proof(const StreamIdentity &stream,
                                     const StartupRestartProof &value,
                                     std::string *json,
                                     std::string *error) {
  return serialize_restart_proof_impl(stream, value, json, error);
}

bool parse_startup_restart_proof(const StreamIdentity &stream,
                                 std::string_view json,
                                 StartupRestartProof *value,
                                 std::string *error) {
  if (value == nullptr || error == nullptr)
    return fail_with(error, "startup restart proof parse output is null");
  rapidjson::Document document;
  if (!parse_document(json, kStartupRestartProofMaxBytes, &document, error) ||
      !exact_members(document,
                     {"format", "version", "route", "deployment", "head",
                      "epoch", "marker", "expected_root",
                      "stopped_worker_root", "installed_root"}) ||
      !validate_envelope(document, kRestartProofFormat)) {
    return fail_with(error, "startup restart proof schema is invalid");
  }
  StartupRestartProof parsed;
  const auto deployment = document.FindMember("deployment");
  const auto head = document.FindMember("head");
  const auto epoch = document.FindMember("epoch");
  const auto marker = document.FindMember("marker");
  const auto evidence = document.FindMember("expected_root");
  const auto stopped_worker_root =
      document.FindMember("stopped_worker_root");
  const auto installed_root = document.FindMember("installed_root");
  if (!parse_route(document, "route", &parsed.route) ||
      deployment == document.MemberEnd() || head == document.MemberEnd() ||
      epoch == document.MemberEnd() || marker == document.MemberEnd() ||
      evidence == document.MemberEnd() ||
      !parse_deployment(deployment->value, &parsed.deployment) ||
      !valid_deployment(stream, parsed.deployment) ||
      !parse_published_head(head->value, stream, &parsed.head, error) ||
      !parse_epoch(epoch->value, stream, &parsed.epoch, error) ||
      !parse_marker_body(marker->value, &parsed.marker, error) ||
      !parse_root_evidence(evidence->value, &parsed.expected_root) ||
      stopped_worker_root == document.MemberEnd() ||
      installed_root == document.MemberEnd() ||
      !parse_stable_root_manifest(stopped_worker_root->value,
                                  &parsed.stopped_worker_root) ||
      !parse_stable_root_manifest(installed_root->value,
                                  &parsed.installed_root)) {
    return fail_with(error, "startup restart proof fields are invalid");
  }
  std::string canonical;
  if (!serialize_restart_proof_impl(stream, parsed, &canonical, error) ||
      !canonical_matches(json, canonical, error)) {
    return false;
  }
  *value = std::move(parsed);
  return true;
}

namespace {

bool published_extent_digest(const SnapshotManifest &snapshot, uint64_t *count,
                             std::string *digest, std::string *error) {
  std::vector<PinnedSmartengineExtent> extents;
  extents.reserve(snapshot.smartengine_extents.size());
  for (const SmartengineExtentRef &extent : snapshot.smartengine_extents) {
    extents.push_back({extent.writer_epoch,
                       extent.allocation_seq,
                       extent.database_name_hex,
                       extent.index_id,
                       extent.object_id,
                       extent.key,
                       extent.size,
                       extent.sha256});
  }
  return startup_extent_set_digest(extents, count, digest, error);
}

bool exact_remote_proof(const RecoveryPlan &remote,
                        const StartupRestartProof &proof,
                        std::string *error) {
  if (remote.head != proof.head.value ||
      !same_object(remote.head_object, proof.head.object) ||
      !same_epoch(remote, proof.epoch)) {
    return fail_with(error,
                     "remote HEAD or epoch differs from the restart proof");
  }
  if (!deployment_matches(remote, proof.deployment) ||
      remote.head.durable_cursor != proof.expected_root.recovered_cursor ||
      remote.snapshot.gtid_executed != proof.expected_root.recovered_gtid) {
    return fail_with(error,
                     "restart proof root identity differs from remote state");
  }
  uint64_t count = 0;
  std::string digest;
  if (!published_extent_digest(remote.snapshot, &count, &digest, error) ||
      count != proof.expected_root.exported_extent_count ||
      digest != proof.expected_root.exported_extent_set_sha256) {
    return fail_with(error,
                     "restart proof extent set differs from remote snapshot");
  }
  return true;
}

bool valid_bootstrap_preflight(const StartupCoordinatorOptions &options,
                               std::string *error) {
  if (!options.bootstrap_preflight.has_value())
    return fail_with(error,
                     "bootstrap requires clean initialize preflight evidence");
  const StartupBootstrapPreflight &preflight =
      *options.bootstrap_preflight;
  std::string canonical;
  if (!serialize_startup_bootstrap_preflight(preflight, &canonical, error) ||
      !preflight.child_exited_cleanly ||
      preflight.root != options.materialize.temp_root ||
      preflight.initialized_deployment != options.deployment ||
      !preflight.dd_initialized || !preflight.repository_empty ||
      !preflight.internal_prepared_empty || !preflight.external_xa_empty ||
      !preflight.empty_source_scan_stable ||
      !preflight.old_tc_authority_empty || !preflight.user_state_empty ||
      !preflight.legacy_live_extents_empty) {
    return fail_with(
        error, "bootstrap initialize preflight is incomplete or for another root");
  }
  std::error_code filesystem_error;
  const fs::file_status status =
      fs::symlink_status(preflight.root, filesystem_error);
  if (filesystem_error || !fs::is_directory(status) || fs::is_symlink(status))
    return fail_with(error,
                     "bootstrap preflight root is not a real directory");
  return true;
}

}  // namespace

StartupCoordinatorResult StartupCoordinator::fail(
    StartupCoordinatorOutcome outcome, std::string detail) {
  state_ = outcome == StartupCoordinatorOutcome::FENCED
               ? StartupCoordinatorState::FENCED
               : StartupCoordinatorState::FAILED_CLOSED;
  return result(outcome, std::move(detail));
}

StartupCoordinatorResult StartupCoordinator::fail(
    const StartupStepResult &step, std::string_view operation) {
  StartupCoordinatorOutcome outcome = StartupCoordinatorOutcome::CORRUPT;
  switch (step.outcome) {
    case StartupStepOutcome::BLOCKED:
      outcome = StartupCoordinatorOutcome::BLOCKED;
      break;
    case StartupStepOutcome::CORRUPT:
      outcome = StartupCoordinatorOutcome::CORRUPT;
      break;
    case StartupStepOutcome::LOCAL_IO_ERROR:
      outcome = StartupCoordinatorOutcome::LOCAL_IO_ERROR;
      break;
    case StartupStepOutcome::RESTART_REQUIRED:
      outcome = StartupCoordinatorOutcome::RESTART_REQUIRED;
      break;
    case StartupStepOutcome::FENCED:
      outcome = StartupCoordinatorOutcome::FENCED;
      break;
    case StartupStepOutcome::READY:
      break;
  }
  std::string detail(operation);
  detail.append(" failed closed");
  if (!step.detail.empty()) detail.append(": ").append(step.detail);
  return fail(outcome, std::move(detail));
}

StartupCoordinatorResult StartupCoordinator::prepare_worker(
    const StartupCoordinatorOptions &options) {
  if (state_ != StartupCoordinatorState::STARTING)
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "prepare_worker may be called exactly once");
  if (storage_ == nullptr || operations_ == nullptr)
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "startup coordinator dependency is null");
  std::string error;
  if (!valid_deployment(stream_, options.deployment) ||
      !valid_sha256(options.worker_nonce) ||
      !validate_sibling_paths(options.materialize.temp_root,
                              options.target_root, &error)) {
    if (error.empty())
      error = "startup deployment identity or worker nonce is invalid";
    return fail(StartupCoordinatorOutcome::LOCAL_IO_ERROR, std::move(error));
  }
  options_ = options;
  const StartupCoordinatorOptions &startup_options = *options_;

  StartupHookProbe hook_probe;
  StartupStepResult step = operations_->startup_probe(&hook_probe);
  if (!step.ready()) return fail(step, "HEAD-first startup hook probe");
  route_ = hook_probe.route;
  const bool bootstrap = *route_ == StartupCoordinatorRoute::BOOTSTRAP;
  if ((bootstrap &&
       (!hook_probe.head_object.body.empty() ||
        !hook_probe.head_object.etag.empty() ||
        hook_probe.head_generation != 0 ||
        !hook_probe.durable_cursor.file.empty() ||
        hook_probe.durable_cursor.pos != 0)) ||
      (!bootstrap &&
       (hook_probe.head_object.body.empty() ||
        hook_probe.head_object.etag.empty() ||
        hook_probe.head_generation == 0 ||
        !valid_cursor(hook_probe.durable_cursor)))) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "startup hook returned an invalid cached HEAD probe");
  }
  hook_probe_ = std::move(hook_probe);

  RecoveryPlan initial;
  RecoveryReadResult read = storage_->read(&initial);
  if (!read.ready() && read.outcome != RecoveryReadOutcome::EMPTY)
    return fail(map_read_outcome(read.outcome),
                "HEAD-first namespace probe failed: " + read.detail);
  if ((bootstrap && read.outcome != RecoveryReadOutcome::EMPTY) ||
      (!bootstrap && !read.ready())) {
    return fail(StartupCoordinatorOutcome::FENCED,
                "HEAD-first namespace route changed before startup work");
  }
  if (!bootstrap && !deployment_matches(initial, startup_options.deployment))
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "probed snapshot deployment differs from local declaration");
  if (!bootstrap &&
      (initial.head.generation < hook_probe_->head_generation ||
       (initial.head.generation == hook_probe_->head_generation &&
        (!same_object(initial.head_object, hook_probe_->head_object) ||
         initial.head.durable_cursor != hook_probe_->durable_cursor)))) {
    return fail(StartupCoordinatorOutcome::FENCED,
                "bounded reader disagrees with the cached HEAD-first probe");
  }

  LocalInstallMarker classification_marker;
  if (bootstrap) {
    classification_marker = preflight_marker(startup_options.deployment);
  } else if (!build_install_marker(stream_, startup_options.deployment, initial,
                                   &classification_marker, &error)) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "cannot bind target classification to probed HEAD: " + error);
  }
  const TargetClassification classification = storage_->classify(
      startup_options.target_root, classification_marker, !bootstrap);
  if (classification.classification == TargetClass::FOREIGN_OR_CORRUPT ||
      (bootstrap &&
       classification.classification != TargetClass::EMPTY_TARGET)) {
    return fail(StartupCoordinatorOutcome::FOREIGN_OR_CORRUPT,
                "startup target classification failed: " +
                    classification.detail);
  }

  if (bootstrap) {
    if (!valid_bootstrap_preflight(startup_options, &error))
      return fail(StartupCoordinatorOutcome::CORRUPT, std::move(error));
    state_ = StartupCoordinatorState::INITIALIZING;
  } else {
    if (startup_options.bootstrap_preflight.has_value())
      return fail(StartupCoordinatorOutcome::CORRUPT,
                  "takeover received bootstrap-only preflight evidence");
    std::error_code filesystem_error;
    if (fs::exists(startup_options.materialize.temp_root, filesystem_error) ||
        filesystem_error) {
      return fail(StartupCoordinatorOutcome::LOCAL_IO_ERROR,
                  "takeover temporary root already exists or is unreadable");
    }
    state_ = StartupCoordinatorState::RECOVERING;
  }

  ExactWriterEpoch acquired;
  step = operations_->acquire_epoch(&acquired);
  if (!step.ready()) return fail(step, "writer epoch acquisition");
  if (!valid_acquired_epoch(acquired))
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "acquired epoch lacks exact body, ETag, or owner");
  acquired_ = std::move(acquired);

  RecoveryPlan after_epoch;
  read = storage_->read(&after_epoch);
  if (bootstrap) {
    if (read.outcome != RecoveryReadOutcome::EMPTY)
      return fail(read.outcome == RecoveryReadOutcome::BLOCKED
                      ? StartupCoordinatorOutcome::BLOCKED
                      : StartupCoordinatorOutcome::FENCED,
                  "HEAD appeared or became unreadable after bootstrap epoch "
                  "acquisition: " +
                      read.detail);
  } else {
    if (!read.ready())
      return fail(read.outcome == RecoveryReadOutcome::BLOCKED
                      ? StartupCoordinatorOutcome::BLOCKED
                      : StartupCoordinatorOutcome::FENCED,
                  "cannot freeze takeover candidate after epoch acquisition: " +
                      read.detail);
    if (!same_epoch(after_epoch, *acquired_))
      return fail(StartupCoordinatorOutcome::FENCED,
                  "takeover candidate is not bound to the acquired epoch");
    if (!deployment_matches(after_epoch, startup_options.deployment))
      return fail(
          StartupCoordinatorOutcome::CORRUPT,
          "candidate snapshot deployment differs from local declaration");
    candidate_ = std::move(after_epoch);
    MaterializedRoot materialized;
    MaterializeResult materialized_result = storage_->materialize(
        *candidate_, startup_options.materialize, &materialized);
    if (!materialized_result.ready())
      return fail(map_materialize_outcome(materialized_result.outcome),
                  "fresh-root materialization failed: " +
                      materialized_result.detail);
    materialized_ = std::move(materialized);
  }

  StartupWorkerRequest request;
  request.route = *route_;
  request.request_nonce = startup_options.worker_nonce;
  request.root = startup_options.materialize.temp_root;
  request.deployment = startup_options.deployment;
  request.epoch = *acquired_;
  if (!bootstrap) {
    request.candidate = StartupCandidateBoundary{
        {candidate_->head_object, candidate_->head},
        candidate_->snapshot.cursor,
        candidate_->head.durable_cursor};
    request.materialized = *materialized_;
  }
  if (!finalize_startup_worker_request(stream_, &request, &error))
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "cannot bind startup worker request: " + error);
  worker_nonce_ = request.request_nonce;
  worker_request_sha256_ = request.request_sha256;
  state_ = StartupCoordinatorState::WORKER_REQUIRED;
  return result(StartupCoordinatorOutcome::WORKER_REQUIRED, {},
                std::move(request));
}

StartupCoordinatorResult StartupCoordinator::finish_worker(
    const StartupWorkerCompletion &completion) {
  if (state_ != StartupCoordinatorState::WORKER_REQUIRED)
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "finish_worker requires exactly one successful prepare");
  if (storage_ == nullptr || operations_ == nullptr ||
      !options_.has_value() || !route_.has_value() ||
      !hook_probe_.has_value() || !acquired_.has_value()) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "prepared startup coordinator state is incomplete");
  }
  if (!completion.child_exited_cleanly)
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "startup worker did not exit cleanly");
  if (completion.request_nonce != worker_nonce_ ||
      completion.request_sha256 != worker_request_sha256_) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "startup worker evidence belongs to another request");
  }
  const StartupCoordinatorOptions &options = *options_;
  const bool bootstrap = *route_ == StartupCoordinatorRoute::BOOTSTRAP;
  std::string error;
  StartupRootSnapshot stopped_worker_root;
  StartupStepResult step = storage_->snapshot_stopped_root(
      options.materialize.temp_root, &stopped_worker_root);
  if (!step.ready()) return fail(step, "post-worker stopped-root snapshot");
  if (!valid_root_snapshot(stopped_worker_root, &error))
    return fail(StartupCoordinatorOutcome::CORRUPT, std::move(error));
  if (!validate_worker_cut(*route_, options, *acquired_, candidate(),
                           completion, &error)) {
    return fail(StartupCoordinatorOutcome::CORRUPT, std::move(error));
  }
  root_evidence_ = completion.root_evidence;

  const StartupSnapshotPublishRequest publish_request{
      *route_, options.materialize.temp_root, options.deployment, *acquired_,
      *root_evidence_, completion.snapshot_cut, candidate()};
  PublishedStartupHead expected_published;
  step = operations_->publish_snapshot(publish_request, &expected_published);
  if (!step.ready()) return fail(step, "startup snapshot publication");

  StartupRootSnapshot post_publication_root;
  step = storage_->snapshot_stopped_root(options.materialize.temp_root,
                                         &post_publication_root);
  if (!step.ready()) return fail(step, "post-publication stopped-root snapshot");
  if (!valid_root_snapshot(post_publication_root, &error) ||
      post_publication_root != stopped_worker_root) {
    if (error.empty())
      error = "stopped worker root changed during snapshot publication";
    return fail(StartupCoordinatorOutcome::CORRUPT, std::move(error));
  }

  RecoveryPlan published;
  RecoveryReadResult read = storage_->read(&published);
  if (!read.ready())
    return fail(read.outcome == RecoveryReadOutcome::BLOCKED
                    ? StartupCoordinatorOutcome::BLOCKED
                    : StartupCoordinatorOutcome::FENCED,
                "published HEAD cannot be read back exactly: " + read.detail);
  if (!validate_published_identity(expected_published, published, *acquired_,
                                   &error))
    return fail(StartupCoordinatorOutcome::FENCED, std::move(error));
  if (published.snapshot.gtid_executed != root_evidence_->recovered_gtid)
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "published snapshot resampled or changed recovered GTIDs");
  uint64_t published_extent_count = 0;
  std::string published_extent_sha;
  if (!published_extent_digest(published.snapshot, &published_extent_count,
                               &published_extent_sha, &error) ||
      published_extent_count != root_evidence_->exported_extent_count ||
      published_extent_sha != root_evidence_->exported_extent_set_sha256) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "published snapshot changed the worker extent set");
  }
  if (bootstrap) {
    if (!validate_bootstrap_publication(published, *root_evidence_, &error))
      return fail(StartupCoordinatorOutcome::CORRUPT, std::move(error));
  } else if (!validate_takeover_publication(*candidate_, published, &error)) {
    return fail(StartupCoordinatorOutcome::CORRUPT, std::move(error));
  }
  if (!deployment_matches(published, options.deployment))
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "published snapshot deployment differs from local declaration");
  if (!bootstrap &&
      published.head.generation <= hook_probe_->head_generation)
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "takeover publication did not advance the probed HEAD");
  published_ = std::move(published);
  state_ = StartupCoordinatorState::SNAPSHOT_PUBLISHED;

  LocalInstallMarker marker;
  if (!build_install_marker(stream_, options.deployment, *published_, &marker,
                            &error)) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "cannot bind local marker to published HEAD: " + error);
  }
  const InstallResult installed = storage_->install(
      options.materialize.temp_root, options.target_root, marker, !bootstrap);
  if (!installed.installed())
    return fail(map_install_outcome(installed.outcome),
                "atomic local-root install failed: " + installed.detail);
  marker_ = std::move(marker);
  state_ = StartupCoordinatorState::ROOT_INSTALLED;

  StartupRootSnapshot installed_root;
  step = storage_->snapshot_stopped_root(options.target_root, &installed_root);
  if (!step.ready()) return fail(step, "post-install stable-root snapshot");
  if (!validate_marker_only_install(stopped_worker_root, installed_root,
                                    *marker_, &error)) {
    return fail(StartupCoordinatorOutcome::CORRUPT, std::move(error));
  }
  stopped_worker_root_ = stopped_worker_root.manifest;
  installed_root_manifest_ = installed_root.manifest;

  RecoveryPlan final_remote;
  read = storage_->read(&final_remote);
  if (!read.ready())
    return fail(read.outcome == RecoveryReadOutcome::BLOCKED
                    ? StartupCoordinatorOutcome::BLOCKED
                    : StartupCoordinatorOutcome::FENCED,
                "final installed HEAD/epoch recheck failed: " + read.detail);
  if (final_remote.head != published_->head ||
      !same_object(final_remote.head_object, published_->head_object) ||
      !same_epoch(final_remote, *acquired_)) {
    return fail(StartupCoordinatorOutcome::FENCED,
                "HEAD or epoch changed after atomic root install");
  }

  StartupRestartProof proof{*route_,
                            options.deployment,
                            {published_->head_object, published_->head},
                            *acquired_,
                            *marker_,
                            *root_evidence_,
                            *stopped_worker_root_,
                            *installed_root_manifest_};
  std::string proof_body;
  if (!serialize_startup_restart_proof(stream_, proof, &proof_body, &error))
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "cannot build bounded restart proof: " + error);
  StartupRestartProofReference reference;
  step = operations_->persist_restart_proof(proof, &reference);
  if (!step.ready()) return fail(step, "restart proof persistence");
  if (!valid_restart_reference(reference) || reference.size != proof_body.size())
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "restart proof persistence returned an invalid reference");
  std::string proof_sha;
  if (!sha256_hex(proof_body, &proof_sha, &error) ||
      proof_sha != reference.sha256) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "restart proof reference does not bind the canonical payload");
  }
  restart_proof_ = std::move(proof);
  state_ = StartupCoordinatorState::RESTART_REQUIRED;
  return result(StartupCoordinatorOutcome::RESTART_REQUIRED, {}, std::nullopt,
                std::move(reference));
}

StartupCoordinatorResult StartupCoordinator::adopt_restart_proof(
    const StartupActivationOptions &options,
    const StartupRestartProof &proof) {
  if (state_ != StartupCoordinatorState::STARTING)
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "adopt_restart_proof may be called exactly once");
  if (storage_ == nullptr || operations_ == nullptr)
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "startup coordinator dependency is null");
  std::string error;
  std::string proof_body;
  if (!validate_activation_options(stream_, options, &error) ||
      !serialize_startup_restart_proof(stream_, proof, &proof_body, &error) ||
      proof.deployment != options.deployment) {
    if (error.empty()) error = "restart proof belongs to another deployment";
    return fail(StartupCoordinatorOutcome::CORRUPT, std::move(error));
  }
  activation_options_ = options;
  route_ = proof.route;
  acquired_ = proof.epoch;
  marker_ = proof.marker;
  root_evidence_ = proof.expected_root;

  RecoveryPlan remote;
  const RecoveryReadResult read = storage_->read(&remote);
  if (!read.ready())
    return fail(read.outcome == RecoveryReadOutcome::BLOCKED
                    ? StartupCoordinatorOutcome::BLOCKED
                    : StartupCoordinatorOutcome::FENCED,
                "cannot exact-read restart proof HEAD/epoch: " + read.detail);
  if (!exact_remote_proof(remote, proof, &error))
    return fail(StartupCoordinatorOutcome::FENCED, std::move(error));
  if ((proof.route == StartupCoordinatorRoute::BOOTSTRAP &&
       (remote.head.generation != 1 || remote.head.parent.has_value())) ||
      (proof.route == StartupCoordinatorRoute::TAKEOVER &&
       (remote.head.generation <= 1 || !remote.head.parent.has_value()))) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "restart proof route disagrees with the installed HEAD");
  }
  LocalInstallMarker expected_marker;
  if (!build_install_marker(stream_, options.deployment, remote,
                            &expected_marker, &error) ||
      expected_marker != proof.marker) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "restart proof marker does not exactly bind HEAD");
  }
  const TargetClassification classification =
      storage_->classify(options.target_root, proof.marker, true);
  if (classification.classification != TargetClass::MANAGED_REPLACE ||
      !classification.marker.has_value() ||
      *classification.marker != proof.marker) {
    return fail(StartupCoordinatorOutcome::FOREIGN_OR_CORRUPT,
                "installed target marker differs from restart proof: " +
                    classification.detail);
  }
  StartupRootSnapshot installed_root;
  const StartupStepResult snapshot_step =
      storage_->snapshot_stopped_root(options.target_root, &installed_root);
  if (!snapshot_step.ready())
    return fail(snapshot_step, "installed re-exec stable-root verification");
  if (!valid_root_snapshot(installed_root, &error) ||
      installed_root.manifest != proof.installed_root) {
    if (error.empty())
      error = "installed root manifest differs from restart proof";
    return fail(StartupCoordinatorOutcome::FOREIGN_OR_CORRUPT,
                std::move(error));
  }
  published_ = std::move(remote);
  restart_proof_ = proof;
  stopped_worker_root_ = proof.stopped_worker_root;
  installed_root_manifest_ = proof.installed_root;
  installed_root_verified_ = true;
  const StartupActivationRequest activation{proof.route, *published_,
                                             *acquired_, *marker_,
                                             *root_evidence_,
                                             *stopped_worker_root_,
                                             *installed_root_manifest_};
  const StartupStepResult step =
      operations_->activate_installed_root(activation);
  if (!step.ready()) return fail(step, "installed-root pre-recovery activation");
  state_ = StartupCoordinatorState::POST_ENGINE_VERIFICATION_REQUIRED;
  return result(
      StartupCoordinatorOutcome::POST_ENGINE_VERIFICATION_REQUIRED);
}

StartupCoordinatorResult StartupCoordinator::finish_post_engine_verification(
    const StartupRootEvidence &installed_evidence) {
  if (state_ != StartupCoordinatorState::POST_ENGINE_VERIFICATION_REQUIRED)
    return fail(
        StartupCoordinatorOutcome::CORRUPT,
        "post-engine verification requires one adopted restart proof");
  if (!activation_options_.has_value() || !restart_proof_.has_value() ||
      !published_.has_value() || !acquired_.has_value() ||
      !marker_.has_value() || !root_evidence_.has_value() ||
      !stopped_worker_root_.has_value() ||
      !installed_root_manifest_.has_value() || !installed_root_verified_) {
    return fail(StartupCoordinatorOutcome::CORRUPT,
                "adopted startup coordinator state is incomplete");
  }
  if (!complete_root_evidence(installed_evidence, true) ||
      installed_evidence.recovered_cursor !=
          root_evidence_->recovered_cursor ||
      installed_evidence.recovered_gtid != root_evidence_->recovered_gtid ||
      installed_evidence.exported_extent_count !=
          root_evidence_->exported_extent_count ||
      installed_evidence.exported_extent_set_sha256 !=
          root_evidence_->exported_extent_set_sha256) {
    return fail(
        StartupCoordinatorOutcome::CORRUPT,
        "installed root evidence differs from the published worker cut");
  }
  RecoveryPlan final_remote;
  const RecoveryReadResult read = storage_->read(&final_remote);
  if (!read.ready())
    return fail(read.outcome == RecoveryReadOutcome::BLOCKED
                    ? StartupCoordinatorOutcome::BLOCKED
                    : StartupCoordinatorOutcome::FENCED,
                "post-engine HEAD/epoch recheck failed: " + read.detail);
  std::string error;
  if (!exact_remote_proof(final_remote, *restart_proof_, &error))
    return fail(StartupCoordinatorOutcome::FENCED, std::move(error));

  state_ = StartupCoordinatorState::ACTIVATED;
  StartupCoordinatorProof proof{
      {published_->head_object, published_->head}, *acquired_, *marker_,
      installed_evidence, *installed_root_manifest_};
  return result(StartupCoordinatorOutcome::READY_FOR_ADMISSION, {},
                std::nullopt, std::nullopt, std::move(proof));
}

}  // namespace wesql::remote_commit
