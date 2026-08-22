#include "sql/wesql_orm_ddl_rewrite.h"

#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/create_field.h"
#include "sql/derror.h"
#include "sql/handler.h"
#include "sql/key_spec.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_table.h"
#include "sql/system_variables.h"

namespace {

const CHARSET_INFO *k_unicode_ci = &my_charset_utf8mb4_unicode_ci;
const CHARSET_INFO *k_general_ci = &my_charset_utf8mb4_general_ci;

bool rewrite_enabled(const THD *thd) {
#ifdef WESQL
  return thd->variables.wesql_orm_ddl_rewrite;
#else
  (void)thd;
  return false;
#endif
}

void warn_collation(THD *thd, const char *field_name) {
  push_warning_printf(
      thd, Sql_condition::SL_WARNING, ER_WESQL_ORM_COLLATION_REWRITTEN,
      ER_THD(thd, ER_WESQL_ORM_COLLATION_REWRITTEN),
      "wesql_orm_ddl_rewrite=ON", field_name,
      "utf8mb4_unicode_ci", "utf8mb4_general_ci");
}

void warn_fk(THD *thd, const char *name) {
  push_warning_printf(thd, Sql_condition::SL_WARNING,
                      ER_WESQL_ORM_FK_STRIPPED,
                      ER_THD(thd, ER_WESQL_ORM_FK_STRIPPED),
                      "wesql_orm_ddl_rewrite=ON",
                      (name && name[0]) ? name : "unnamed foreign key");
}

const CHARSET_INFO *field_charset(const Create_field *field,
                                  const HA_CREATE_INFO *create_info) {
  if (field->charset != nullptr) return field->charset;
  if (create_info != nullptr) return create_info->default_table_charset;
  return nullptr;
}

bool field_is_indexed(const Alter_info *alter_info, const char *field_name) {
  if (field_name == nullptr) return false;
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

}  // namespace

bool wesql_is_smartengine(const HA_CREATE_INFO *create_info) {
  return create_info != nullptr && create_info->db_type != nullptr &&
         create_info->db_type->db_type == DB_TYPE_SMARTENGINE;
}

static Wesql_orm_rewrite_result strip_fk_pair(THD *thd, Alter_info *alter_info,
                                              bool enabled) {
  Wesql_orm_rewrite_result result;
  if (alter_info == nullptr) return result;

  Mem_root_array<Key_spec *> kept(thd->mem_root);
  uint fk_count = 0;
  for (size_t i = 0; i < alter_info->key_list.size(); ++i) {
    Key_spec *key = alter_info->key_list[i];
    if (key->type == KEYTYPE_FOREIGN) {
      ++fk_count;
      if (!enabled) {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0),
                 "foreign key constraints");
        result.action = Wesql_orm_rewrite_action::kReject;
        return result;
      }
      const char *name = key->name.str;
      warn_fk(thd, name);
      continue;
    }
    if (enabled && key->type == KEYTYPE_MULTIPLE && key->generated) {
      continue;
    }
    kept.push_back(key);
  }

  if (fk_count == 0) return result;

  if (enabled) {
    alter_info->key_list.clear();
    for (Key_spec *k : kept) alter_info->key_list.push_back(k);
    alter_info->flags &= ~Alter_info::ADD_FOREIGN_KEY;
    result.fk_stripped = fk_count;
    result.action = Wesql_orm_rewrite_action::kRewritten;
  }
  return result;
}

static uint rewrite_indexed_unicode_columns(THD *thd, Alter_info *alter_info) {
  uint n = 0;
  List_iterator<Create_field> it(alter_info->create_list);
  Create_field *field;
  while ((field = it++)) {
    const CHARSET_INFO *cs = field_charset(field, nullptr);
    if (cs != k_unicode_ci) continue;
    if (!field_is_indexed(alter_info, field->field_name)) continue;
    field->charset = k_general_ci;
    warn_collation(thd, field->field_name);
    ++n;
  }
  return n;
}

Wesql_orm_rewrite_result wesql_orm_rewrite_create(THD *thd,
                                                  HA_CREATE_INFO *create_info,
                                                  Alter_info *alter_info) {
  Wesql_orm_rewrite_result result;
  if (!wesql_is_smartengine(create_info)) return result;

  const bool on = rewrite_enabled(thd);
  result = strip_fk_pair(thd, alter_info, on);
  if (result.action == Wesql_orm_rewrite_action::kReject) return result;

  if (on) {
    result.collation_rewritten =
        rewrite_indexed_unicode_columns(thd, alter_info);
    if (result.collation_rewritten > 0)
      result.action = Wesql_orm_rewrite_action::kRewritten;
  } else if (alter_info != nullptr) {
    List_iterator<Create_field> it(alter_info->create_list);
    Create_field *field;
    while ((field = it++)) {
      const CHARSET_INFO *cs = field_charset(field, create_info);
      if (cs == k_unicode_ci &&
          field_is_indexed(alter_info, field->field_name)) {
        my_error(ER_UNKNOWN_ERROR, MYF(0),
                 "Unsupported collation on string indexed column %s. "
                 "Consider change to other collation (utf8mb4_general_ci).",
                 field->field_name);
        result.action = Wesql_orm_rewrite_action::kReject;
        return result;
      }
    }
  }
  return result;
}

Wesql_orm_rewrite_result wesql_orm_rewrite_alter_fk(THD *thd,
                                                    HA_CREATE_INFO *create_info,
                                                    Alter_info *alter_info) {
  Wesql_orm_rewrite_result result;
  if (!wesql_is_smartengine(create_info)) return result;
  result = strip_fk_pair(thd, alter_info, rewrite_enabled(thd));
  if (result.action == Wesql_orm_rewrite_action::kRewritten &&
      (alter_info->flags &
       ~(Alter_info::ADD_FOREIGN_KEY | Alter_info::ALTER_RECREATE)) == 0 &&
      alter_info->create_list.elements == 0) {
    bool only_user_keys = false;
    for (const Key_spec *key : alter_info->key_list) {
      if (!(key->type == KEYTYPE_MULTIPLE && key->generated)) {
        only_user_keys = true;
        break;
      }
    }
    if (!only_user_keys) result.action = Wesql_orm_rewrite_action::kNoopBinlog;
  }
  return result;
}

Wesql_orm_rewrite_result wesql_orm_rewrite_alter_collation(
    THD *thd, HA_CREATE_INFO *create_info, Alter_info *alter_info) {
  Wesql_orm_rewrite_result result;
  if (!wesql_is_smartengine(create_info) || !rewrite_enabled(thd))
    return result;
  result.collation_rewritten =
      rewrite_indexed_unicode_columns(thd, alter_info);
  if (result.collation_rewritten > 0)
    result.action = Wesql_orm_rewrite_action::kRewritten;
  return result;
}

bool wesql_orm_build_alter_binlog_sql(THD *thd, HA_CREATE_INFO *,
                                      Alter_info *alter_info,
                                      std::string *out_sql) {
  /*
    Lossless printer is wired to store_create_info() column dump in the
    sql_table.cc hook. If any clause cannot be printed, the hook must
    raise ER_WESQL_ORM_ALTER_NOT_REWRITABLE and skip binlog.
  */
  (void)thd;
  (void)alter_info;
  if (out_sql != nullptr) out_sql->clear();
  return false;
}

const char *wesql_orm_fk_noop_sql() {
  return "DO 0 /* wesql_orm_ddl_rewrite stripped fk */";
}
