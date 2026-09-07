/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_POLICY_INCLUDED
#define SQL_REMOTE_COMMIT_POLICY_INCLUDED

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace wesql::remote_commit {

constexpr uint64_t kSegmentEnvelopeBytes = 16ULL * 1024 * 1024;
constexpr uint64_t kMaxBinlogCacheBytes = 1024ULL * 1024 * 1024;
constexpr uint64_t kDefaultMaxSegmentBytes = 4ULL * 1024 * 1024 * 1024;

enum class LifecycleState : uint8_t {
  OFF,
  INITIALIZING,
  RECOVERING,
  RUNNING,
  BLOCKED,
  FENCED,
};

const char *lifecycle_state_name(LifecycleState state);

enum class PolicyError : uint8_t {
  INVALID_PROVIDER,
  INVALID_REPOSITORY_ID,
  INVALID_BRANCH_ID,
  INVALID_CLUSTER_PREFIX,
  INVALID_BINLOG_BASENAME,
  CONDITIONAL_IO_UNSUPPORTED,
  BINLOG_DISABLED,
  BINLOG_FORMAT_NOT_ROW,
  BINLOG_ROW_IMAGE_NOT_FULL,
  BINLOG_TRANSACTION_COMPRESSION_ENABLED,
  BINLOG_CHECKSUM_NOT_CRC32,
  BINLOG_ROW_VALUE_OPTIONS_ENABLED,
  BINLOG_ENCRYPTION_ENABLED,
  BINLOG_ORDER_COMMITS_DISABLED,
  BINLOG_ERROR_ACTION_NOT_ABORT_SERVER,
  GTID_MODE_NOT_ON,
  GTID_CONSISTENCY_DISABLED,
  ANONYMOUS_GTID_ALLOWED,
  INNODB_WAL_NOT_DURABLE,
  SMARTENGINE_WAL_NOT_DURABLE,
  SMARTENGINE_WAL_DISABLED,
  SMARTENGINE_PERSISTENT_CACHE_ENABLED,
  SMARTENGINE_MIDDLE_COMMIT_ENABLED,
  HEURISTIC_RECOVERY_ENABLED,
  SNAPSHOT_ARCHIVE_DISABLED,
  SNAPSHOT_OBJECT_STORE_DISABLED,
  SMARTENGINE_IMMUTABLE_EXTENTS_UNAVAILABLE,
  INVALID_MAX_SEGMENT_BYTES,
  BINLOG_CACHE_TOO_LARGE,
  BINLOG_STMT_CACHE_TOO_LARGE,
  EXTERNAL_REPLICATION_PRESENT,
  GROUP_REPLICATION_PRESENT,
  INTERNAL_PREPARED_PRESENT,
  EXTERNAL_XA_PRESENT,
  LEGACY_SOURCE_PRESENT,
};

const char *policy_error_name(PolicyError error);

struct ConditionalIoCapabilities {
  bool exact_get_with_etag{false};
  bool exact_get_to_file{false};
  bool create_only{false};
  bool compare_and_swap{false};
  bool create_from_file{false};
  bool distinct_404_409_412{false};

  bool complete() const;
};

struct StartupPolicy {
  std::string provider;
  std::string repository_id;
  std::string branch_id;
  std::string cluster_object_prefix;
  std::string binlog_basename;
  ConditionalIoCapabilities conditional_io;

  bool log_bin{false};
  bool binlog_format_row{false};
  bool binlog_row_image_full{false};
  bool binlog_transaction_compression{false};
  bool binlog_checksum_crc32{false};
  bool binlog_row_value_options_empty{false};
  bool binlog_encryption{false};
  bool binlog_order_commits{false};
  bool binlog_error_action_abort_server{false};
  bool gtid_mode_on{false};
  bool enforce_gtid_consistency{false};
  bool anonymous_gtid_forbidden{false};
  uint64_t max_binlog_cache_size{0};
  uint64_t max_binlog_stmt_cache_size{0};
  uint64_t max_segment_bytes{kDefaultMaxSegmentBytes};

  uint32_t innodb_flush_log_at_trx_commit{0};
  bool smartengine_enabled{false};
  uint32_t smartengine_flush_log_at_trx_commit{0};
  bool smartengine_write_disable_wal{false};
  uint64_t smartengine_persistent_cache_size{0};
  bool smartengine_commit_in_the_middle{false};
  bool smartengine_immutable_extents{false};

  bool heuristic_recovery_off{false};
  bool snapshot_archive{false};
  bool snapshot_archive_on_objectstore{false};

  bool external_replication_present{false};
  bool group_replication_present{false};
  bool internal_prepared_present{false};
  bool external_xa_present{false};
  bool legacy_source_present{false};
};

struct PolicyViolation {
  PolicyError code;
  std::string detail;
};

bool is_canonical_path_component(std::string_view value,
                                 size_t maximum_length = 48);
bool is_canonical_cluster_prefix(std::string_view value);
uint64_t maximum_allowed_binlog_cache(uint64_t max_segment_bytes);

std::vector<PolicyViolation> validate_startup_policy(
    const StartupPolicy &policy);
std::vector<PolicyViolation> validate_startup_policy(
    const StartupPolicy &policy, bool bootstrap_preflight);

enum class AuthorizationKind : uint8_t {
  NONE,
  GROUP,
  RECOVERY,
  READ_ONLY,
};

struct CommitBinding {
  std::string stream_id;
  uint64_t head_generation{0};
  std::string head_body_sha256;
  std::string endpoint_file;
  uint64_t endpoint_pos{0};
  std::string gtid_sha256;
  std::string xid_sha256;

  bool operator==(const CommitBinding &other) const;
};

class CommitAuthorization {
 public:
  CommitAuthorization() = default;
  CommitAuthorization(const CommitAuthorization &) = delete;
  CommitAuthorization &operator=(const CommitAuthorization &) = delete;
  CommitAuthorization(CommitAuthorization &&) = default;
  CommitAuthorization &operator=(CommitAuthorization &&) = default;

  static CommitAuthorization group(CommitBinding binding);
  static CommitAuthorization recovery(CommitBinding binding);
  static CommitAuthorization read_only();

  AuthorizationKind kind() const { return kind_; }
  bool consumed() const { return consumed_; }

  bool consume(AuthorizationKind required_kind,
               const CommitBinding *expected_binding, std::string *error);

 private:
  CommitAuthorization(AuthorizationKind kind, CommitBinding binding)
      : kind_(kind), binding_(std::move(binding)) {}

  AuthorizationKind kind_{AuthorizationKind::NONE};
  CommitBinding binding_;
  bool consumed_{false};
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_POLICY_INCLUDED
