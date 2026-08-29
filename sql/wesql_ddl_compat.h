#ifndef WESQL_DDL_COMPAT_H
#define WESQL_DDL_COMPAT_H

/*
  SmartEngine CREATE/ALTER compatibility on long-lived 8.0 (MySQL 8.0.46).

  After parse, before the storage engine: remap unsupported index collations
  and strip FOREIGN KEY from SmartEngine tables. InnoDB is unchanged.
  Rewrites parsed HA_CREATE_INFO / Alter_info, not SQL text.
  Not GitHub PR #85. Not the 9.7 tree.
*/

class THD;
class Alter_info;
struct HA_CREATE_INFO;

bool wesql_is_smartengine_create(const HA_CREATE_INFO *create_info);

/* CREATE: after set_table_default_charset(), before mysql_prepare_create_table(). */
bool wesql_ddl_compat_rewrite_create(THD *thd, HA_CREATE_INFO *create_info,
                                     Alter_info *alter_info);

/*
  ALTER: after the engine is known, before check_fk_parent_table_access()
  and FK MDL collection.
*/
bool wesql_ddl_compat_rewrite_alter_fk(THD *thd, HA_CREATE_INFO *create_info,
                                       Alter_info *alter_info);

/* ALTER: after mysql_prepare_alter_table() and set_table_default_charset(). */
bool wesql_ddl_compat_rewrite_alter_collation(THD *thd,
                                              HA_CREATE_INFO *create_info,
                                              Alter_info *alter_info);

#endif
