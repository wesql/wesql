#ifndef WESQL_ORM_DDL_REWRITE_H
#define WESQL_ORM_DDL_REWRITE_H

#include <string>

class THD;
class Alter_info;
struct HA_CREATE_INFO;

enum class Wesql_orm_rewrite_action {
  kNone = 0,
  kRewritten,
  kReject,
  kNoopBinlog
};

struct Wesql_orm_rewrite_result {
  Wesql_orm_rewrite_action action{Wesql_orm_rewrite_action::kNone};
  std::string binlog_sql;
  unsigned int fk_stripped{0};
  unsigned int collation_rewritten{0};
};

bool wesql_is_smartengine(const HA_CREATE_INFO *create_info);

/* Clear connection-lifetime rewrite/binlog state at each CREATE/ALTER. */
void wesql_orm_reset_statement_state(THD *thd);

/*
  CREATE: call after set_table_default_charset(), before
  mysql_prepare_create_table().
*/
Wesql_orm_rewrite_result wesql_orm_rewrite_create(THD *thd,
                                                  HA_CREATE_INFO *create_info,
                                                  Alter_info *alter_info);

/*
  ALTER step 1: after engine is known, before check_fk_parent_table_access()
  and FK MDL collection. Also snapshots this-statement keys.
*/
Wesql_orm_rewrite_result wesql_orm_rewrite_alter_fk(THD *thd,
                                                    HA_CREATE_INFO *create_info,
                                                    Alter_info *alter_info);

/*
  ALTER step 2: after mysql_prepare_alter_table(), rewrite new/changed
  columns and existing unicode_ci columns that gain an index.
*/
Wesql_orm_rewrite_result wesql_orm_rewrite_alter_collation(
    THD *thd, HA_CREATE_INFO *create_info, Alter_info *alter_info);

/*
  Build landed ALTER SQL for binlog. Uses only remapped columns plus the
  pre-prepare snapshot of this-statement keys. Returns false on success.
  If a clause cannot be printed losslessly, push 7518 and return true.
*/
bool wesql_orm_build_alter_binlog_sql(THD *thd, HA_CREATE_INFO *create_info,
                                      Alter_info *alter_info,
                                      std::string *out_sql);

const char *wesql_orm_fk_noop_sql();

#endif
