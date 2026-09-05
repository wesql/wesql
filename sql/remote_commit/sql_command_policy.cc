/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/sql_command_policy.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <iterator>
#include <limits>

#include "sql/remote_commit/policy.h"

namespace wesql::remote_commit {
namespace {

using C = SqlCommandClass;
constexpr C N = C::NO_DURABLE_MUTATION;
constexpr C R = C::REMOTE_REPLAYABLE;
constexpr C L = C::LOCAL_MUTATING_REJECT;
constexpr C O = C::RECOVERING_ONLY;

// Positional by enum_sql_command. The size assertion is the upgrade guard: a
// new SQLCOM value cannot silently inherit a policy from a default branch.
constexpr C kSqlCommandClasses[] = {
    N,  // SQLCOM_SELECT
    R,  // SQLCOM_CREATE_TABLE
    R,  // SQLCOM_CREATE_INDEX
    R,  // SQLCOM_ALTER_TABLE
    R,  // SQLCOM_UPDATE
    R,  // SQLCOM_INSERT
    R,  // SQLCOM_INSERT_SELECT
    R,  // SQLCOM_DELETE
    R,  // SQLCOM_TRUNCATE
    R,  // SQLCOM_DROP_TABLE
    R,  // SQLCOM_DROP_INDEX
    N,  // SQLCOM_SHOW_DATABASES
    N,  // SQLCOM_SHOW_TABLES
    N,  // SQLCOM_SHOW_FIELDS
    N,  // SQLCOM_SHOW_KEYS
    N,  // SQLCOM_SHOW_VARIABLES
    N,  // SQLCOM_SHOW_STATUS
    N,  // SQLCOM_SHOW_ENGINE_LOGS
    N,  // SQLCOM_SHOW_ENGINE_STATUS
    N,  // SQLCOM_SHOW_ENGINE_MUTEX
    N,  // SQLCOM_SHOW_PROCESSLIST
    N,  // SQLCOM_SHOW_BINLOG_STATUS
    N,  // SQLCOM_SHOW_REPLICA_STATUS
    N,  // SQLCOM_SHOW_GRANTS
    N,  // SQLCOM_SHOW_CREATE
    N,  // SQLCOM_SHOW_CHARSETS
    N,  // SQLCOM_SHOW_COLLATIONS
    N,  // SQLCOM_SHOW_CREATE_DB
    N,  // SQLCOM_SHOW_TABLE_STATUS
    N,  // SQLCOM_SHOW_TRIGGERS
    R,  // SQLCOM_LOAD
    N,  // SQLCOM_SET_OPTION (subclassified before execution)
    N,  // SQLCOM_LOCK_TABLES
    N,  // SQLCOM_UNLOCK_TABLES
    R,  // SQLCOM_GRANT
    N,  // SQLCOM_CHANGE_DB
    R,  // SQLCOM_CREATE_DB
    R,  // SQLCOM_DROP_DB
    R,  // SQLCOM_ALTER_DB
    L,  // SQLCOM_REPAIR
    R,  // SQLCOM_REPLACE
    R,  // SQLCOM_REPLACE_SELECT
    L,  // SQLCOM_CREATE_FUNCTION (native SONAME UDF)
    L,  // SQLCOM_DROP_FUNCTION (native SONAME UDF)
    R,  // SQLCOM_REVOKE
    L,  // SQLCOM_OPTIMIZE
    N,  // SQLCOM_CHECK
    L,  // SQLCOM_ASSIGN_TO_KEYCACHE
    L,  // SQLCOM_PRELOAD_KEYS
    L,  // SQLCOM_FLUSH
    N,  // SQLCOM_KILL
    R,  // SQLCOM_ANALYZE
    N,  // SQLCOM_ROLLBACK
    N,  // SQLCOM_ROLLBACK_TO_SAVEPOINT
    R,  // SQLCOM_COMMIT
    N,  // SQLCOM_SAVEPOINT
    N,  // SQLCOM_RELEASE_SAVEPOINT
    L,  // SQLCOM_REPLICA_START
    L,  // SQLCOM_REPLICA_STOP
    L,  // SQLCOM_START_GROUP_REPLICATION
    L,  // SQLCOM_STOP_GROUP_REPLICATION
    N,  // SQLCOM_BEGIN
    L,  // SQLCOM_CHANGE_REPLICATION_SOURCE
    L,  // SQLCOM_CHANGE_REPLICATION_FILTER
    R,  // SQLCOM_RENAME_TABLE
    L,  // SQLCOM_RESET (all current subtypes are local mutations)
    L,  // SQLCOM_PURGE
    L,  // SQLCOM_PURGE_BEFORE
    N,  // SQLCOM_SHOW_BINLOGS
    N,  // SQLCOM_SHOW_OPEN_TABLES
    N,  // SQLCOM_HA_OPEN
    N,  // SQLCOM_HA_CLOSE
    N,  // SQLCOM_HA_READ
    N,  // SQLCOM_SHOW_REPLICAS
    R,  // SQLCOM_DELETE_MULTI
    R,  // SQLCOM_UPDATE_MULTI
    N,  // SQLCOM_SHOW_BINLOG_EVENTS
    R,  // SQLCOM_DO (stored functions may mutate durable state)
    N,  // SQLCOM_SHOW_WARNS
    N,  // SQLCOM_EMPTY_QUERY
    N,  // SQLCOM_SHOW_ERRORS
    N,  // SQLCOM_SHOW_STORAGE_ENGINES
    N,  // SQLCOM_SHOW_PRIVILEGES
    N,  // SQLCOM_HELP
    R,  // SQLCOM_CREATE_USER
    R,  // SQLCOM_DROP_USER
    R,  // SQLCOM_RENAME_USER
    R,  // SQLCOM_REVOKE_ALL
    N,  // SQLCOM_CHECKSUM
    R,  // SQLCOM_CREATE_PROCEDURE
    R,  // SQLCOM_CREATE_SPFUNCTION
    R,  // SQLCOM_CALL
    R,  // SQLCOM_DROP_PROCEDURE
    R,  // SQLCOM_ALTER_PROCEDURE
    R,  // SQLCOM_ALTER_FUNCTION
    N,  // SQLCOM_SHOW_CREATE_PROC
    N,  // SQLCOM_SHOW_CREATE_FUNC
    N,  // SQLCOM_SHOW_STATUS_PROC
    N,  // SQLCOM_SHOW_STATUS_FUNC
    N,  // SQLCOM_PREPARE
    R,  // SQLCOM_EXECUTE (the prepared command is checked again)
    N,  // SQLCOM_DEALLOCATE_PREPARE
    R,  // SQLCOM_CREATE_VIEW
    R,  // SQLCOM_DROP_VIEW
    R,  // SQLCOM_CREATE_TRIGGER
    R,  // SQLCOM_DROP_TRIGGER
    L,  // SQLCOM_XA_START
    L,  // SQLCOM_XA_END
    L,  // SQLCOM_XA_PREPARE
    L,  // SQLCOM_XA_COMMIT
    L,  // SQLCOM_XA_ROLLBACK
    N,  // SQLCOM_XA_RECOVER
    N,  // SQLCOM_SHOW_PROC_CODE
    N,  // SQLCOM_SHOW_FUNC_CODE
    L,  // SQLCOM_ALTER_TABLESPACE
    L,  // SQLCOM_INSTALL_PLUGIN
    L,  // SQLCOM_UNINSTALL_PLUGIN
    O,  // SQLCOM_BINLOG_BASE64_EVENT
    N,  // SQLCOM_SHOW_PLUGINS
    L,  // SQLCOM_CREATE_SERVER
    L,  // SQLCOM_DROP_SERVER
    L,  // SQLCOM_ALTER_SERVER
    R,  // SQLCOM_CREATE_EVENT
    R,  // SQLCOM_ALTER_EVENT
    R,  // SQLCOM_DROP_EVENT
    N,  // SQLCOM_SHOW_CREATE_EVENT
    N,  // SQLCOM_SHOW_EVENTS
    N,  // SQLCOM_SHOW_CREATE_TRIGGER
    N,  // SQLCOM_SHOW_PROFILE
    N,  // SQLCOM_SHOW_PROFILES
    N,  // SQLCOM_SIGNAL
    N,  // SQLCOM_RESIGNAL
    N,  // SQLCOM_SHOW_RELAYLOG_EVENTS
    N,  // SQLCOM_GET_DIAGNOSTICS
    R,  // SQLCOM_ALTER_USER
    N,  // SQLCOM_EXPLAIN_OTHER
    N,  // SQLCOM_SHOW_CREATE_USER
    N,  // SQLCOM_SHUTDOWN
    R,  // SQLCOM_SET_PASSWORD
    L,  // SQLCOM_ALTER_INSTANCE
    L,  // SQLCOM_INSTALL_COMPONENT
    L,  // SQLCOM_UNINSTALL_COMPONENT
    R,  // SQLCOM_CREATE_ROLE
    R,  // SQLCOM_DROP_ROLE
    N,  // SQLCOM_SET_ROLE
    R,  // SQLCOM_GRANT_ROLE
    R,  // SQLCOM_REVOKE_ROLE
    R,  // SQLCOM_ALTER_USER_DEFAULT_ROLE
    L,  // SQLCOM_IMPORT
    L,  // SQLCOM_CREATE_RESOURCE_GROUP
    L,  // SQLCOM_ALTER_RESOURCE_GROUP
    L,  // SQLCOM_DROP_RESOURCE_GROUP
    L,  // SQLCOM_SET_RESOURCE_GROUP
    L,  // SQLCOM_CLONE
    N,  // SQLCOM_LOCK_INSTANCE
    N,  // SQLCOM_UNLOCK_INSTANCE
    N,  // SQLCOM_RESTART_SERVER
    R,  // SQLCOM_CREATE_SRS
    R,  // SQLCOM_DROP_SRS
    N,  // SQLCOM_SHOW_PARSE_TREE
    R,  // SQLCOM_CREATE_LIBRARY
    R,  // SQLCOM_DROP_LIBRARY
    N,  // SQLCOM_SHOW_CREATE_LIBRARY
    R,  // SQLCOM_ALTER_LIBRARY
    N,  // SQLCOM_SHOW_STATUS_LIBRARY
    R,  // SQLCOM_CREATE_MASKING_POLICY
    R,  // SQLCOM_DROP_MASKING_POLICY
    N,  // SQLCOM_SHOW_CREATE_MASKING_POLICY
};

static_assert(std::size(kSqlCommandClasses) ==
                  static_cast<std::size_t>(SQLCOM_END),
              "classify every enum_sql_command before enabling remote commit");

constexpr std::array<std::string_view, 15> kProtectedSessionVariables = {
    "binlog_checksum",
    "binlog_encryption",
    "binlog_error_action",
    "binlog_format",
    "binlog_order_commits",
    "binlog_row_image",
    "binlog_row_value_options",
    "binlog_transaction_compression",
    "enforce_gtid_consistency",
    "gtid_mode",
    "innodb_flush_log_at_trx_commit",
    "smartengine_bulk_load_size",
    "smartengine_flush_log_at_trx_commit",
    "smartengine_persistent_cache_size",
    "smartengine_write_disable_wal",
};

bool ascii_equal_case_insensitive(std::string_view left,
                                  std::string_view right) {
  if (left.size() != right.size()) return false;
  return std::equal(left.begin(), left.end(), right.begin(),
                    [](unsigned char lhs, unsigned char rhs) {
                      return std::tolower(lhs) == std::tolower(rhs);
                    });
}

bool is_protected_session_variable(std::string_view variable_name) {
  if (ascii_equal_case_insensitive(variable_name, "sql_log_bin") ||
      ascii_equal_case_insensitive(variable_name, "gtid_purged"))
    return true;
  return std::any_of(kProtectedSessionVariables.begin(),
                     kProtectedSessionVariables.end(),
                     [&](std::string_view protected_name) {
                       return ascii_equal_case_insensitive(variable_name,
                                                           protected_name);
                     });
}

}  // namespace

SqlCommandClass classify_sql_command(enum_sql_command command) {
  const auto index = static_cast<std::size_t>(command);
  if (index >= std::size(kSqlCommandClasses)) return C::UNCLASSIFIED;
  return kSqlCommandClasses[index];
}

bool sql_command_classification_complete() {
  return std::none_of(std::begin(kSqlCommandClasses),
                      std::end(kSqlCommandClasses),
                      [](C value) { return value == C::UNCLASSIFIED; });
}

const char *sql_command_class_name(SqlCommandClass command_class) {
  switch (command_class) {
    case C::UNCLASSIFIED:
      return "UNCLASSIFIED";
    case C::NO_DURABLE_MUTATION:
      return "NO_DURABLE_MUTATION";
    case C::REMOTE_REPLAYABLE:
      return "REMOTE_REPLAYABLE";
    case C::LOCAL_MUTATING_REJECT:
      return "LOCAL_MUTATING_REJECT";
    case C::RECOVERING_ONLY:
      return "RECOVERING_ONLY";
  }
  return "UNCLASSIFIED";
}

SqlCommandClass classify_set_assignment(SetAssignmentScope scope,
                                        std::string_view variable_name) {
  switch (scope) {
    case SetAssignmentScope::DEFAULT_SCOPE:
    case SetAssignmentScope::SESSION:
      return is_protected_session_variable(variable_name) ? L : N;
    case SetAssignmentScope::GLOBAL:
    case SetAssignmentScope::PERSIST:
    case SetAssignmentScope::PERSIST_ONLY:
    case SetAssignmentScope::UNKNOWN:
      return L;
  }
  return L;
}

bool transaction_cache_fits_segment(uint64_t statement_cache_bytes,
                                    uint64_t transaction_cache_bytes,
                                    uint64_t maximum_segment_bytes,
                                    uint64_t *bytes_with_envelope) {
  if (statement_cache_bytes >
      std::numeric_limits<uint64_t>::max() - transaction_cache_bytes)
    return false;
  const uint64_t cache_bytes =
      statement_cache_bytes + transaction_cache_bytes;
  if (maximum_segment_bytes < kSegmentEnvelopeBytes ||
      cache_bytes > maximum_segment_bytes - kSegmentEnvelopeBytes)
    return false;
  if (bytes_with_envelope != nullptr)
    *bytes_with_envelope = cache_bytes + kSegmentEnvelopeBytes;
  return true;
}

}  // namespace wesql::remote_commit
