/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/policy.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace wesql::remote_commit {
namespace {

void add_violation(std::vector<PolicyViolation> *violations, PolicyError code,
                   std::string detail = {}) {
  violations->push_back({code, std::move(detail)});
}

bool is_remote_commit_provider(std::string_view provider) {
  return provider == "aws" || provider == "minio" || provider == "r2";
}

}  // namespace

const char *lifecycle_state_name(LifecycleState state) {
  switch (state) {
    case LifecycleState::OFF:
      return "OFF";
    case LifecycleState::INITIALIZING:
      return "INITIALIZING";
    case LifecycleState::RECOVERING:
      return "RECOVERING";
    case LifecycleState::RUNNING:
      return "RUNNING";
    case LifecycleState::BLOCKED:
      return "BLOCKED";
    case LifecycleState::FENCED:
      return "FENCED";
  }
  return "FENCED";
}

const char *policy_error_name(PolicyError error) {
  switch (error) {
    case PolicyError::INVALID_PROVIDER:
      return "INVALID_PROVIDER";
    case PolicyError::INVALID_REPOSITORY_ID:
      return "INVALID_REPOSITORY_ID";
    case PolicyError::INVALID_BRANCH_ID:
      return "INVALID_BRANCH_ID";
    case PolicyError::INVALID_CLUSTER_PREFIX:
      return "INVALID_CLUSTER_PREFIX";
    case PolicyError::INVALID_BINLOG_BASENAME:
      return "INVALID_BINLOG_BASENAME";
    case PolicyError::CONDITIONAL_IO_UNSUPPORTED:
      return "CONDITIONAL_IO_UNSUPPORTED";
    case PolicyError::BINLOG_DISABLED:
      return "BINLOG_DISABLED";
    case PolicyError::BINLOG_FORMAT_NOT_ROW:
      return "BINLOG_FORMAT_NOT_ROW";
    case PolicyError::BINLOG_ROW_IMAGE_NOT_FULL:
      return "BINLOG_ROW_IMAGE_NOT_FULL";
    case PolicyError::BINLOG_TRANSACTION_COMPRESSION_ENABLED:
      return "BINLOG_TRANSACTION_COMPRESSION_ENABLED";
    case PolicyError::BINLOG_CHECKSUM_NOT_CRC32:
      return "BINLOG_CHECKSUM_NOT_CRC32";
    case PolicyError::BINLOG_ROW_VALUE_OPTIONS_ENABLED:
      return "BINLOG_ROW_VALUE_OPTIONS_ENABLED";
    case PolicyError::BINLOG_ENCRYPTION_ENABLED:
      return "BINLOG_ENCRYPTION_ENABLED";
    case PolicyError::BINLOG_ORDER_COMMITS_DISABLED:
      return "BINLOG_ORDER_COMMITS_DISABLED";
    case PolicyError::BINLOG_ERROR_ACTION_NOT_ABORT_SERVER:
      return "BINLOG_ERROR_ACTION_NOT_ABORT_SERVER";
    case PolicyError::GTID_MODE_NOT_ON:
      return "GTID_MODE_NOT_ON";
    case PolicyError::GTID_CONSISTENCY_DISABLED:
      return "GTID_CONSISTENCY_DISABLED";
    case PolicyError::ANONYMOUS_GTID_ALLOWED:
      return "ANONYMOUS_GTID_ALLOWED";
    case PolicyError::INNODB_WAL_NOT_DURABLE:
      return "INNODB_WAL_NOT_DURABLE";
    case PolicyError::SMARTENGINE_WAL_NOT_DURABLE:
      return "SMARTENGINE_WAL_NOT_DURABLE";
    case PolicyError::SMARTENGINE_WAL_DISABLED:
      return "SMARTENGINE_WAL_DISABLED";
    case PolicyError::SMARTENGINE_PERSISTENT_CACHE_ENABLED:
      return "SMARTENGINE_PERSISTENT_CACHE_ENABLED";
    case PolicyError::SMARTENGINE_MIDDLE_COMMIT_ENABLED:
      return "SMARTENGINE_MIDDLE_COMMIT_ENABLED";
    case PolicyError::HEURISTIC_RECOVERY_ENABLED:
      return "HEURISTIC_RECOVERY_ENABLED";
    case PolicyError::SNAPSHOT_ARCHIVE_DISABLED:
      return "SNAPSHOT_ARCHIVE_DISABLED";
    case PolicyError::SNAPSHOT_OBJECT_STORE_DISABLED:
      return "SNAPSHOT_OBJECT_STORE_DISABLED";
    case PolicyError::SMARTENGINE_IMMUTABLE_EXTENTS_UNAVAILABLE:
      return "SMARTENGINE_IMMUTABLE_EXTENTS_UNAVAILABLE";
    case PolicyError::INVALID_MAX_SEGMENT_BYTES:
      return "INVALID_MAX_SEGMENT_BYTES";
    case PolicyError::BINLOG_CACHE_TOO_LARGE:
      return "BINLOG_CACHE_TOO_LARGE";
    case PolicyError::BINLOG_STMT_CACHE_TOO_LARGE:
      return "BINLOG_STMT_CACHE_TOO_LARGE";
    case PolicyError::EXTERNAL_REPLICATION_PRESENT:
      return "EXTERNAL_REPLICATION_PRESENT";
    case PolicyError::GROUP_REPLICATION_PRESENT:
      return "GROUP_REPLICATION_PRESENT";
    case PolicyError::INTERNAL_PREPARED_PRESENT:
      return "INTERNAL_PREPARED_PRESENT";
    case PolicyError::EXTERNAL_XA_PRESENT:
      return "EXTERNAL_XA_PRESENT";
    case PolicyError::LEGACY_SOURCE_PRESENT:
      return "LEGACY_SOURCE_PRESENT";
  }
  return "UNKNOWN_POLICY_ERROR";
}

bool ConditionalIoCapabilities::complete() const {
  return exact_get_with_etag && exact_get_to_file && create_only &&
         compare_and_swap && create_from_file && distinct_404_409_412;
}

bool is_canonical_path_component(std::string_view value,
                                 size_t maximum_length) {
  if (value.empty() || value.size() > maximum_length || value == "." ||
      value == "..") {
    return false;
  }
  const auto valid_first = [](unsigned char ch) {
    return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
           (ch >= '0' && ch <= '9');
  };
  const auto valid_rest = [&](unsigned char ch) {
    return valid_first(ch) || ch == '.' || ch == '_' || ch == '-';
  };
  if (!valid_first(static_cast<unsigned char>(value.front()))) return false;
  return std::all_of(value.begin() + 1, value.end(), [&](char ch) {
    return valid_rest(static_cast<unsigned char>(ch));
  });
}

bool is_canonical_cluster_prefix(std::string_view value) {
  if (value.empty() || value.front() == '/' || value.back() == '/') {
    return false;
  }
  size_t start = 0;
  while (start < value.size()) {
    const size_t end = value.find('/', start);
    const size_t length =
        (end == std::string_view::npos ? value.size() : end) - start;
    if (!is_canonical_path_component(value.substr(start, length))) return false;
    if (end == std::string_view::npos) return true;
    start = end + 1;
  }
  return false;
}

uint64_t maximum_allowed_binlog_cache(uint64_t max_segment_bytes) {
  if (max_segment_bytes <= kSegmentEnvelopeBytes) return 0;
  return std::min(kMaxBinlogCacheBytes,
                  max_segment_bytes - kSegmentEnvelopeBytes);
}

std::vector<PolicyViolation> validate_startup_policy(
    const StartupPolicy &policy) {
  std::vector<PolicyViolation> violations;
  if (!is_remote_commit_provider(policy.provider))
    add_violation(&violations, PolicyError::INVALID_PROVIDER, policy.provider);
  if (!is_canonical_path_component(policy.repository_id))
    add_violation(&violations, PolicyError::INVALID_REPOSITORY_ID,
                  policy.repository_id);
  if (!is_canonical_path_component(policy.branch_id))
    add_violation(&violations, PolicyError::INVALID_BRANCH_ID,
                  policy.branch_id);
  if (!is_canonical_cluster_prefix(policy.cluster_object_prefix))
    add_violation(&violations, PolicyError::INVALID_CLUSTER_PREFIX,
                  policy.cluster_object_prefix);
  if (!is_canonical_path_component(policy.binlog_basename))
    add_violation(&violations, PolicyError::INVALID_BINLOG_BASENAME,
                  policy.binlog_basename);
  if (!policy.conditional_io.complete())
    add_violation(&violations, PolicyError::CONDITIONAL_IO_UNSUPPORTED);
  if (!policy.log_bin)
    add_violation(&violations, PolicyError::BINLOG_DISABLED);
  if (!policy.binlog_format_row)
    add_violation(&violations, PolicyError::BINLOG_FORMAT_NOT_ROW);
  if (!policy.binlog_row_image_full)
    add_violation(&violations, PolicyError::BINLOG_ROW_IMAGE_NOT_FULL);
  if (policy.binlog_transaction_compression)
    add_violation(&violations,
                  PolicyError::BINLOG_TRANSACTION_COMPRESSION_ENABLED);
  if (!policy.binlog_checksum_crc32)
    add_violation(&violations, PolicyError::BINLOG_CHECKSUM_NOT_CRC32);
  if (!policy.binlog_row_value_options_empty)
    add_violation(&violations, PolicyError::BINLOG_ROW_VALUE_OPTIONS_ENABLED);
  if (policy.binlog_encryption)
    add_violation(&violations, PolicyError::BINLOG_ENCRYPTION_ENABLED);
  if (!policy.binlog_order_commits)
    add_violation(&violations, PolicyError::BINLOG_ORDER_COMMITS_DISABLED);
  if (!policy.binlog_error_action_abort_server)
    add_violation(&violations,
                  PolicyError::BINLOG_ERROR_ACTION_NOT_ABORT_SERVER);
  if (!policy.gtid_mode_on)
    add_violation(&violations, PolicyError::GTID_MODE_NOT_ON);
  if (!policy.enforce_gtid_consistency)
    add_violation(&violations, PolicyError::GTID_CONSISTENCY_DISABLED);
  if (!policy.anonymous_gtid_forbidden)
    add_violation(&violations, PolicyError::ANONYMOUS_GTID_ALLOWED);
  if (policy.innodb_flush_log_at_trx_commit != 1)
    add_violation(&violations, PolicyError::INNODB_WAL_NOT_DURABLE);
  if (policy.smartengine_enabled) {
    if (policy.smartengine_flush_log_at_trx_commit != 1)
      add_violation(&violations,
                    PolicyError::SMARTENGINE_WAL_NOT_DURABLE);
    if (policy.smartengine_write_disable_wal)
      add_violation(&violations, PolicyError::SMARTENGINE_WAL_DISABLED);
    if (policy.smartengine_persistent_cache_size != 0)
      add_violation(&violations,
                    PolicyError::SMARTENGINE_PERSISTENT_CACHE_ENABLED);
    if (policy.smartengine_commit_in_the_middle)
      add_violation(&violations,
                    PolicyError::SMARTENGINE_MIDDLE_COMMIT_ENABLED);
    if (!policy.smartengine_immutable_extents)
      add_violation(
          &violations,
          PolicyError::SMARTENGINE_IMMUTABLE_EXTENTS_UNAVAILABLE);
  }
  if (!policy.heuristic_recovery_off)
    add_violation(&violations, PolicyError::HEURISTIC_RECOVERY_ENABLED);
  if (!policy.snapshot_archive)
    add_violation(&violations, PolicyError::SNAPSHOT_ARCHIVE_DISABLED);
  if (!policy.snapshot_archive_on_objectstore)
    add_violation(&violations,
                  PolicyError::SNAPSHOT_OBJECT_STORE_DISABLED);

  const uint64_t cache_limit =
      maximum_allowed_binlog_cache(policy.max_segment_bytes);
  if (cache_limit == 0 || policy.max_segment_bytes > kDefaultMaxSegmentBytes)
    add_violation(&violations, PolicyError::INVALID_MAX_SEGMENT_BYTES);
  if (policy.max_binlog_cache_size > cache_limit)
    add_violation(&violations, PolicyError::BINLOG_CACHE_TOO_LARGE);
  if (policy.max_binlog_stmt_cache_size > cache_limit)
    add_violation(&violations, PolicyError::BINLOG_STMT_CACHE_TOO_LARGE);

  if (policy.external_replication_present)
    add_violation(&violations, PolicyError::EXTERNAL_REPLICATION_PRESENT);
  if (policy.group_replication_present)
    add_violation(&violations, PolicyError::GROUP_REPLICATION_PRESENT);
  if (policy.internal_prepared_present)
    add_violation(&violations, PolicyError::INTERNAL_PREPARED_PRESENT);
  if (policy.external_xa_present)
    add_violation(&violations, PolicyError::EXTERNAL_XA_PRESENT);
  if (policy.legacy_source_present)
    add_violation(&violations, PolicyError::LEGACY_SOURCE_PRESENT);
  return violations;
}

bool CommitBinding::operator==(const CommitBinding &other) const {
  return stream_id == other.stream_id &&
         head_generation == other.head_generation &&
         head_body_sha256 == other.head_body_sha256 &&
         endpoint_file == other.endpoint_file &&
         endpoint_pos == other.endpoint_pos &&
         gtid_sha256 == other.gtid_sha256 && xid_sha256 == other.xid_sha256;
}

CommitAuthorization CommitAuthorization::group(CommitBinding binding) {
  return CommitAuthorization(AuthorizationKind::GROUP, std::move(binding));
}

CommitAuthorization CommitAuthorization::recovery(CommitBinding binding) {
  return CommitAuthorization(AuthorizationKind::RECOVERY, std::move(binding));
}

CommitAuthorization CommitAuthorization::read_only() {
  return CommitAuthorization(AuthorizationKind::READ_ONLY, {});
}

bool CommitAuthorization::consume(AuthorizationKind required_kind,
                                  const CommitBinding *expected_binding,
                                  std::string *error) {
  if (consumed_) {
    if (error != nullptr) *error = "remote commit authorization was reused";
    return false;
  }
  if (kind_ != required_kind) {
    if (error != nullptr) *error = "remote commit authorization kind mismatch";
    return false;
  }
  if (kind_ != AuthorizationKind::READ_ONLY) {
    if (expected_binding == nullptr || !(binding_ == *expected_binding)) {
      if (error != nullptr) *error = "remote commit authorization binding mismatch";
      return false;
    }
  } else if (expected_binding != nullptr) {
    if (error != nullptr)
      *error = "read-only authorization cannot carry a durable binding";
    return false;
  }
  consumed_ = true;
  return true;
}

}  // namespace wesql::remote_commit
