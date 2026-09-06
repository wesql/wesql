/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_SQL_ADMISSION_INCLUDED
#define SQL_REMOTE_COMMIT_SQL_ADMISSION_INCLUDED

class THD;

namespace wesql::remote_commit {

// Runs after parsing and before mysql_execute_command performs an implicit
// commit or invokes the command implementation. Returns true after reporting a
// statement error when the command or a parsed subtype is rejected.
bool enforce_sql_command_admission(THD *thd);

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_SQL_ADMISSION_INCLUDED
