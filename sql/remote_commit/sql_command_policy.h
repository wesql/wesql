/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_SQL_COMMAND_POLICY_INCLUDED
#define SQL_REMOTE_COMMIT_SQL_COMMAND_POLICY_INCLUDED

#include <cstdint>
#include <string_view>

#include "my_sqlcommand.h"

namespace wesql::remote_commit {

enum class SqlCommandClass : uint8_t {
  UNCLASSIFIED,
  NO_DURABLE_MUTATION,
  REMOTE_REPLAYABLE,
  LOCAL_MUTATING_REJECT,
  RECOVERING_ONLY,
};

// Every value below SQLCOM_END has exactly one table entry. Invalid values are
// UNCLASSIFIED and must be rejected by the dispatch binding.
SqlCommandClass classify_sql_command(enum_sql_command command);
bool sql_command_classification_complete();
const char *sql_command_class_name(SqlCommandClass command_class);

enum class SetAssignmentScope : uint8_t {
  DEFAULT_SCOPE,
  SESSION,
  GLOBAL,
  PERSIST,
  PERSIST_ONLY,
  UNKNOWN,
};

// SET is composite: session/user state is allowed, but local global state,
// persisted state, and variables that can bypass the remote log contract are
// rejected before sql_set_variables() is entered.
SqlCommandClass classify_set_assignment(SetAssignmentScope scope,
                                        std::string_view variable_name);

// The cache bytes are the exact serialized bytes currently in both binlog
// caches, including a pending rows event. The fixed envelope covers final
// transaction/GTID events and segment framing that are added after prepare.
bool transaction_cache_fits_segment(uint64_t statement_cache_bytes,
                                    uint64_t transaction_cache_bytes,
                                    uint64_t maximum_segment_bytes,
                                    uint64_t *bytes_with_envelope = nullptr);

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_SQL_COMMAND_POLICY_INCLUDED
