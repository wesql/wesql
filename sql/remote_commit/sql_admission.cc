/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/sql_admission.h"

#include <string>

#include "mysql_com.h"
#include "mysqld_error.h"
#include "sql/handler.h"
#include "sql/partition_info.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/remote_commit/sql_command_policy.h"
#include "sql/set_var.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_list.h"

namespace wesql::remote_commit {
namespace {

SetAssignmentScope set_assignment_scope(enum_var_type type) {
  switch (type) {
    case OPT_DEFAULT:
      return SetAssignmentScope::DEFAULT_SCOPE;
    case OPT_SESSION:
      return SetAssignmentScope::SESSION;
    case OPT_GLOBAL:
      return SetAssignmentScope::GLOBAL;
    case OPT_PERSIST:
      return SetAssignmentScope::PERSIST;
    case OPT_PERSIST_ONLY:
      return SetAssignmentScope::PERSIST_ONLY;
  }
  return SetAssignmentScope::UNKNOWN;
}

bool reject(THD *thd, const char *detail) {
  if (thd == nullptr) return true;
  std::string feature{"remote commit mode: "};
  feature.append(detail);
  my_error(ER_NOT_SUPPORTED_YET, MYF(0), feature.c_str());
  return true;
}

bool enforce_set_subtypes(THD *thd) {
  List_iterator_fast<set_var_base> iterator(thd->lex->var_list);
  while (set_var_base *base = iterator++) {
    auto *system_variable = dynamic_cast<set_var *>(base);
    if (system_variable == nullptr) continue;
    const char *name = system_variable->m_var_tracker.get_var_name();
    const std::string_view variable_name = name == nullptr ? "" : name;
    if (classify_set_assignment(set_assignment_scope(system_variable->type),
                                variable_name) ==
        SqlCommandClass::LOCAL_MUTATING_REJECT) {
      std::string detail{"SET assignment is not remote replayable: "};
      detail.append(variable_name);
      return reject(thd, detail.c_str());
    }
  }
  return false;
}

bool reject_reset_subtype(THD *thd) {
  const ulong type = thd->lex->type;
  if ((type & REFRESH_PERSIST) != 0)
    return reject(thd, "RESET PERSIST is a local mutation");
  if ((type & REFRESH_REPLICA) != 0)
    return reject(thd, "RESET REPLICA is not accepted");
  if ((type & REFRESH_SOURCE) != 0)
    return reject(thd, "RESET BINARY LOGS AND GTIDS is not accepted");
  return reject(thd, "unclassified RESET subtype");
}

bool reject_external_table_storage(THD *thd,
                                   enum_sql_command command) {
  if (command != SQLCOM_CREATE_TABLE && command != SQLCOM_ALTER_TABLE)
    return false;
  LEX *const lex = thd->lex;
  if (lex->create_info != nullptr &&
      ((lex->create_info->used_fields &
        (HA_CREATE_USED_DATADIR | HA_CREATE_USED_INDEXDIR)) != 0 ||
       lex->create_info->data_file_name != nullptr ||
       lex->create_info->index_file_name != nullptr)) {
    return reject(thd,
                  "table DATA DIRECTORY or INDEX DIRECTORY is outside the "
                  "managed remote root");
  }
  if (lex->part_info != nullptr &&
      has_external_data_or_index_dir(*lex->part_info)) {
    return reject(thd,
                  "partition DATA DIRECTORY or INDEX DIRECTORY is outside "
                  "the managed remote root");
  }
  return false;
}

}  // namespace

bool enforce_sql_command_admission(THD *thd) {
  if (!enabled()) return false;
  if (thd == nullptr || thd->lex == nullptr)
    return reject(thd, "missing parsed SQL command");
  if (may_initialize_system_tables(thd)) return false;
  if (may_rebuild_startup_dictionary_cache(thd)) return false;

  const enum_sql_command command = thd->lex->sql_command;
  const SqlCommandClass command_class = classify_sql_command(command);
  // The parser initializes this field only for these statement families.
  // Other commands can retain an earlier value or leave it uninitialized.
  switch (command) {
    case SQLCOM_ALTER_TABLE:
    case SQLCOM_ANALYZE:
    case SQLCOM_REPAIR:
    case SQLCOM_OPTIMIZE:
    case SQLCOM_FLUSH:
      if (thd->lex->no_write_to_binlog)
        return reject(thd, "NO_WRITE_TO_BINLOG is not remote replayable");
      break;
    default:
      break;
  }
  if (reject_external_table_storage(thd, command)) return true;
  if (command == SQLCOM_SET_OPTION) return enforce_set_subtypes(thd);
  if (command == SQLCOM_RESET) return reject_reset_subtype(thd);

  switch (command_class) {
    case SqlCommandClass::NO_DURABLE_MUTATION:
    case SqlCommandClass::REMOTE_REPLAYABLE:
      return false;
    case SqlCommandClass::LOCAL_MUTATING_REJECT:
      return reject(thd, "SQL command performs a local-only mutation");
    case SqlCommandClass::RECOVERING_ONLY:
      return reject(thd, "SQL command requires internal recovery capability");
    case SqlCommandClass::UNCLASSIFIED:
      return reject(thd, "SQL command is unclassified");
  }
  return reject(thd, "SQL command is unclassified");
}

}  // namespace wesql::remote_commit
