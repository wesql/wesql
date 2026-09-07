/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/policy.h"

#include <cstdlib>
#include <iostream>
#include <string>

namespace rc = wesql::remote_commit;

namespace {

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "policy test failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

rc::StartupPolicy valid_policy() {
  rc::StartupPolicy policy;
  policy.provider = "minio";
  policy.repository_id = "repo-1";
  policy.branch_id = "main";
  policy.cluster_object_prefix = "cluster-1/data";
  policy.binlog_basename = "binlog";
  policy.conditional_io = {true, true, true, true, true, true};
  policy.log_bin = true;
  policy.binlog_format_row = true;
  policy.binlog_row_image_full = true;
  policy.binlog_checksum_crc32 = true;
  policy.binlog_row_value_options_empty = true;
  policy.binlog_order_commits = true;
  policy.binlog_error_action_abort_server = true;
  policy.gtid_mode_on = true;
  policy.enforce_gtid_consistency = true;
  policy.anonymous_gtid_forbidden = true;
  policy.max_binlog_cache_size = rc::kMaxBinlogCacheBytes;
  policy.max_binlog_stmt_cache_size = rc::kMaxBinlogCacheBytes;
  policy.innodb_flush_log_at_trx_commit = 1;
  policy.smartengine_enabled = true;
  policy.smartengine_flush_log_at_trx_commit = 1;
  policy.smartengine_immutable_extents = true;
  policy.heuristic_recovery_off = true;
  policy.snapshot_archive = true;
  policy.snapshot_archive_on_objectstore = true;
  return policy;
}

bool contains(const std::vector<rc::PolicyViolation> &violations,
              rc::PolicyError code) {
  for (const auto &violation : violations) {
    if (violation.code == code) return true;
  }
  return false;
}

void test_components() {
  expect(rc::is_canonical_path_component("a"), "short component");
  expect(rc::is_canonical_path_component("A0._-"), "component alphabet");
  expect(!rc::is_canonical_path_component(""), "empty component rejected");
  expect(!rc::is_canonical_path_component("."), "dot rejected");
  expect(!rc::is_canonical_path_component(".."), "dot-dot rejected");
  expect(!rc::is_canonical_path_component("a/b"), "slash rejected");
  expect(!rc::is_canonical_path_component("a%2fb"), "percent rejected");
  expect(!rc::is_canonical_path_component("_a"), "leading underscore rejected");
  expect(!rc::is_canonical_path_component(std::string(49, 'a')),
         "long component rejected");

  expect(rc::is_canonical_cluster_prefix("cluster/a.b-c_1"),
         "cluster prefix");
  expect(!rc::is_canonical_cluster_prefix("/cluster"),
         "leading slash rejected");
  expect(!rc::is_canonical_cluster_prefix("cluster/"),
         "trailing slash rejected");
  expect(!rc::is_canonical_cluster_prefix("cluster//data"),
         "empty prefix component rejected");
  expect(!rc::is_canonical_cluster_prefix("cluster/../data"),
         "dot-dot prefix rejected");
}

void test_policy() {
  auto policy = valid_policy();
  expect(rc::validate_startup_policy(policy).empty(), "valid policy");

  policy.log_bin = false;
  expect(contains(rc::validate_startup_policy(policy),
                  rc::PolicyError::BINLOG_DISABLED),
         "serving process requires binlog");
  expect(rc::validate_startup_policy(policy, true).empty(),
         "bootstrap preflight permits initialization with binlog disabled");
  policy.gtid_mode_on = false;
  policy.smartengine_write_disable_wal = true;
  const auto preflight = rc::validate_startup_policy(policy, true);
  expect(contains(preflight, rc::PolicyError::GTID_MODE_NOT_ON),
         "bootstrap preflight still requires parsed GTID configuration");
  expect(contains(preflight, rc::PolicyError::SMARTENGINE_WAL_DISABLED),
         "bootstrap preflight does not skip other durability checks");
  policy = valid_policy();

  policy.provider = "local";
  policy.conditional_io = {};
  policy.binlog_transaction_compression = true;
  policy.smartengine_write_disable_wal = true;
  policy.max_segment_bytes = rc::kSegmentEnvelopeBytes;
  policy.external_xa_present = true;
  const auto violations = rc::validate_startup_policy(policy);
  expect(!violations.empty() &&
             violations.front().code == rc::PolicyError::INVALID_PROVIDER &&
             violations.front().detail == "local",
         "first startup violation is deterministic");
  expect(contains(violations, rc::PolicyError::INVALID_PROVIDER),
         "local provider rejected");
  expect(contains(violations, rc::PolicyError::CONDITIONAL_IO_UNSUPPORTED),
         "conditional contract required");
  expect(contains(
             violations,
             rc::PolicyError::BINLOG_TRANSACTION_COMPRESSION_ENABLED),
         "compressed transactions rejected");
  expect(contains(violations, rc::PolicyError::SMARTENGINE_WAL_DISABLED),
         "SmartEngine WAL required");
  expect(contains(violations, rc::PolicyError::INVALID_MAX_SEGMENT_BYTES),
         "segment envelope checked");
  expect(contains(violations, rc::PolicyError::EXTERNAL_XA_PRESENT),
         "external XA rejected");

  policy = valid_policy();
  policy.conditional_io.exact_get_to_file = false;
  expect(contains(rc::validate_startup_policy(policy),
                  rc::PolicyError::CONDITIONAL_IO_UNSUPPORTED),
         "exact streaming GET is required");

  policy = valid_policy();
  policy.max_segment_bytes = rc::kSegmentEnvelopeBytes + 1024;
  policy.max_binlog_cache_size = 1025;
  policy.max_binlog_stmt_cache_size = 1024;
  const auto edge = rc::validate_startup_policy(policy);
  expect(contains(edge, rc::PolicyError::BINLOG_CACHE_TOO_LARGE),
         "cache above exact edge rejected");
  expect(!contains(edge, rc::PolicyError::BINLOG_STMT_CACHE_TOO_LARGE),
         "cache at exact edge accepted");
}

void test_authorization() {
  rc::CommitBinding binding{"r=repo/b=main", 7, std::string(64, 'a'),
                            "binlog.000001", 123, std::string(64, 'b'),
                            std::string(64, 'c')};
  auto authorization = rc::CommitAuthorization::group(binding);
  std::string error;
  expect(authorization.consume(rc::AuthorizationKind::GROUP, &binding, &error),
         "matching group authorization");
  expect(authorization.consumed(), "authorization marked consumed");
  expect(!authorization.consume(rc::AuthorizationKind::GROUP, &binding, &error),
         "authorization cannot be reused");

  auto mismatched = binding;
  ++mismatched.endpoint_pos;
  auto second = rc::CommitAuthorization::group(binding);
  expect(!second.consume(rc::AuthorizationKind::GROUP, &mismatched, &error),
         "binding mismatch rejected");
  expect(!second.consumed(), "mismatch does not consume token");

  auto read_only = rc::CommitAuthorization::read_only();
  expect(read_only.consume(rc::AuthorizationKind::READ_ONLY, nullptr, &error),
         "read-only authorization");
}

}  // namespace

int main() {
  test_components();
  test_policy();
  test_authorization();
  std::cout << "remote commit policy tests passed\n";
  return EXIT_SUCCESS;
}
