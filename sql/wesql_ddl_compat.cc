#include "sql/wesql_ddl_compat.h"

#include "field_types.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/create_field.h"
#include "sql/derror.h"
#include "sql/handler.h"
#include "sql/key_spec.h"
#include "sql/log.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_list.h"
#include "sql/sql_table.h"

/*
  unicode_ci / unicode_520_ci live in strings/ctype-uca.cc and are not
  declared in include/mysql/strings/m_ctype.h. Same pattern as mysys/charset.cc.
*/
extern CHARSET_INFO my_charset_utf8mb4_unicode_ci;
extern CHARSET_INFO my_charset_utf8mb4_unicode_520_ci;

namespace {

const CHARSET_INFO *k_target = &my_charset_utf8mb4_0900_ai_ci;

bool collation_needs_remap(const CHARSET_INFO *cs) {
  return cs == &my_charset_utf8mb4_unicode_ci ||
         cs == &my_charset_utf8mb4_unicode_520_ci;
}

void warn_collation(THD *thd, const char *field_name, const CHARSET_INFO *from) {
  const char *old_name = (from != nullptr && from->m_coll_name != nullptr)
                             ? from->m_coll_name
                             : "unknown";
  push_warning_printf(thd, Sql_condition::SL_WARNING,
                      ER_WESQL_DDL_COLLATION_REWRITTEN,
                      ER_THD(thd, ER_WESQL_DDL_COLLATION_REWRITTEN), field_name,
                      old_name, k_target->m_coll_name);
  sql_print_warning(
      "WeSQL DDL compat: remapped collation of indexed column '%s' from %s "
      "to %s. Comparison rules changed.",
      field_name, old_name, k_target->m_coll_name);
}

void warn_fk(THD *thd, const char *name) {
  const char *shown = (name != nullptr && name[0] != '\0')
                          ? name
                          : "unnamed foreign key";
  push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WESQL_DDL_FK_STRIPPED,
                      ER_THD(thd, ER_WESQL_DDL_FK_STRIPPED), shown);
  sql_print_warning(
      "WeSQL DDL compat: removed foreign key '%s'. Parent/child checks and "
      "ON DELETE CASCADE are not enforced. The application must handle "
      "related deletes.",
      shown);
}

bool field_is_string_indexed_type(const Create_field *field) {
  if (field == nullptr) return false;
  const enum_field_types type = field->sql_type;
  return type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING ||
         type == MYSQL_TYPE_BLOB || type == MYSQL_TYPE_VAR_STRING ||
         type == MYSQL_TYPE_TINY_BLOB || type == MYSQL_TYPE_MEDIUM_BLOB ||
         type == MYSQL_TYPE_LONG_BLOB;
}

bool field_is_indexed(const Alter_info *alter_info, const char *field_name) {
  if (field_name == nullptr || alter_info == nullptr) return false;
  for (const Key_spec *key : alter_info->key_list) {
    if (key->type == KEYTYPE_FOREIGN) continue;
    for (const Key_part_spec *part : key->columns) {
      const char *n = part->get_field_name();
      if (n != nullptr &&
          my_strcasecmp(system_charset_info, n, field_name) == 0)
        return true;
    }
  }
  return false;
}

void remap_indexed_collations(THD *thd, HA_CREATE_INFO *create_info,
                              Alter_info *alter_info) {
  if (alter_info == nullptr) return;

  /*
    Table-default unicode_ci is applied to indexed columns that have no
    explicit collation. Rewrite those columns (and the table default when
    it would land on an index) to the SmartEngine allowlist.
  */
  bool inherited_index = false;
  List_iterator<Create_field> it(alter_info->create_list);
  Create_field *field;
  while ((field = it++)) {
    if (!field_is_string_indexed_type(field)) continue;
    if (!field_is_indexed(alter_info, field->field_name)) continue;
    const CHARSET_INFO *cs = get_sql_field_charset(field, create_info);
    if (!collation_needs_remap(cs)) continue;
    if (field->charset == nullptr ||
        field->charset == create_info->default_table_charset)
      inherited_index = true;
    field->charset = k_target;
    field->is_explicit_collation = true;
    warn_collation(thd, field->field_name, cs);
  }
  if (inherited_index &&
      collation_needs_remap(create_info->default_table_charset)) {
    create_info->default_table_charset = k_target;
  }
}

void strip_foreign_keys(THD *thd, Alter_info *alter_info) {
  if (alter_info == nullptr) return;

  bool had_fk = false;
  for (const Key_spec *key : alter_info->key_list) {
    if (key->type == KEYTYPE_FOREIGN) {
      had_fk = true;
      break;
    }
  }
  if (!had_fk) return;

  Mem_root_array<Key_spec *> kept(thd->mem_root);
  Mem_root_array<const char *> fk_names(thd->mem_root);
  for (Key_spec *key : alter_info->key_list) {
    if (key->type == KEYTYPE_FOREIGN) {
      fk_names.push_back(key->name.str);
      continue;
    }
    /* Parser inserts a generated KEYTYPE_MULTIPLE next to each FOREIGN KEY. */
    if (key->type == KEYTYPE_MULTIPLE && key->generated) continue;
    kept.push_back(key);
  }

  alter_info->key_list.clear();
  for (Key_spec *k : kept) alter_info->key_list.push_back(k);
  alter_info->flags &= ~Alter_info::ADD_FOREIGN_KEY;
  for (const char *name : fk_names) warn_fk(thd, name);
}

}  // namespace

bool wesql_is_smartengine_create(const HA_CREATE_INFO *create_info) {
#ifndef WITH_SMARTENGINE
  (void)create_info;
  return false;
#else
  return create_info != nullptr && create_info->db_type != nullptr &&
         create_info->db_type->db_type == DB_TYPE_SMARTENGINE;
#endif
}

bool wesql_ddl_compat_rewrite_create(THD *thd, HA_CREATE_INFO *create_info,
                                     Alter_info *alter_info) {
  if (thd == nullptr || !wesql_is_smartengine_create(create_info)) return false;
  strip_foreign_keys(thd, alter_info);
  remap_indexed_collations(thd, create_info, alter_info);
  return false;
}

bool wesql_ddl_compat_rewrite_alter_fk(THD *thd, HA_CREATE_INFO *create_info,
                                       Alter_info *alter_info) {
  if (thd == nullptr || !wesql_is_smartengine_create(create_info)) return false;
  strip_foreign_keys(thd, alter_info);
  return false;
}

bool wesql_ddl_compat_rewrite_alter_collation(THD *thd,
                                              HA_CREATE_INFO *create_info,
                                              Alter_info *alter_info) {
  if (thd == nullptr || !wesql_is_smartengine_create(create_info)) return false;
  remap_indexed_collations(thd, create_info, alter_info);
  return false;
}
