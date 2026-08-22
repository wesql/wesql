#include "sql/wesql_orm_ddl_rewrite.h"

#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "field_types.h"
#include "sql/create_field.h"
#include "sql/dd/types/column.h"
#include "sql/field.h"
#include "sql/table.h"
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

bool has_unprintable_column(const Create_field *field) {
  if (field->is_gcol() || field->m_default_val_expr != nullptr) return true;
  if (field->hidden != dd::Column::enum_hidden_type::HT_VISIBLE) return true;
  if (field->auto_flags != Field::NONE) return true;
  return false;
}

void append_ident(std::string *out, const char *name) {
  out->push_back('`');
  if (name != nullptr) {
    for (const char *p = name; *p; ++p) {
      if (*p == '`') out->push_back('`');
      out->push_back(*p);
    }
  }
  out->push_back('`');
}

bool print_column_def(const Create_field *field, std::string *out) {
  if (has_unprintable_column(field)) return true;
  append_ident(out, field->field_name);
  out->push_back(' ');
  switch (field->sql_type) {
    case MYSQL_TYPE_VARCHAR:
      *out += "varchar(" +
              std::to_string(field->max_display_width_in_codepoints()) + ")";
      break;
    case MYSQL_TYPE_STRING:
      *out += "char(" +
              std::to_string(field->max_display_width_in_codepoints()) + ")";
      break;
    case MYSQL_TYPE_LONG:
      *out += (field->flags & UNSIGNED_FLAG) ? "int unsigned" : "int";
      break;
    case MYSQL_TYPE_LONGLONG:
      *out += (field->flags & UNSIGNED_FLAG) ? "bigint unsigned" : "bigint";
      break;
    case MYSQL_TYPE_TINY:
      *out += "tinyint";
      break;
    case MYSQL_TYPE_SHORT:
      *out += "smallint";
      break;
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
      *out += "text";
      break;
    default:
      return true;
  }
  if (field->charset != nullptr) {
    *out += " CHARACTER SET ";
    *out += field->charset->csname;
    *out += " COLLATE ";
    *out += field->charset->m_coll_name;
  }
  *out += field->is_nullable ? " NULL" : " NOT NULL";
  if (field->comment.str != nullptr && field->comment.length > 0) {
    *out += " COMMENT '";
    out->append(field->comment.str, field->comment.length);
    *out += "'";
  }
  return false;
}

static uint rewrite_indexed_unicode_columns(THD *thd,
                                            HA_CREATE_INFO *create_info,
                                            Alter_info *alter_info) {
  uint n = 0;
  List_iterator<Create_field> it(alter_info->create_list);
  Create_field *field;
  while ((field = it++)) {
    const CHARSET_INFO *cs = field_charset(field, create_info);
    if (cs != k_unicode_ci) continue;
    if (!field_is_indexed(alter_info, field->field_name)) continue;
    field->charset = k_general_ci;
    field->is_explicit_collation = true;
    warn_collation(thd, field->field_name);
    ++n;
  }
  return n;
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

  if (!enabled) return result;

  bool mixed = alter_info->create_list.elements > 0;
  if (alter_info->flags & ~Alter_info::ADD_FOREIGN_KEY) mixed = true;
  for (Key_spec *k : kept) {
    if (!(k->type == KEYTYPE_MULTIPLE && k->generated)) mixed = true;
  }
  if (mixed) {
    my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
             "wesql_orm_ddl_rewrite=ON");
    result.action = Wesql_orm_rewrite_action::kReject;
    return result;
  }

  alter_info->key_list.clear();
  for (Key_spec *k : kept) alter_info->key_list.push_back(k);
  alter_info->flags &= ~Alter_info::ADD_FOREIGN_KEY;
  result.fk_stripped = fk_count;
  result.action = Wesql_orm_rewrite_action::kRewritten;
  return result;
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
        rewrite_indexed_unicode_columns(thd, create_info, alter_info);
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
      rewrite_indexed_unicode_columns(thd, create_info, alter_info);
  if (result.collation_rewritten > 0)
    result.action = Wesql_orm_rewrite_action::kRewritten;
  return result;
}

bool wesql_orm_build_alter_binlog_sql(THD *thd, HA_CREATE_INFO *,
                                      Alter_info *alter_info,
                                      std::string *out_sql) {
  if (out_sql == nullptr) return true;
  out_sql->clear();
  Table_ref *tbl = thd->lex->query_tables;
  if (tbl == nullptr || tbl->db == nullptr || tbl->table_name == nullptr) {
    my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
             "wesql_orm_ddl_rewrite=ON");
    return true;
  }

  std::string sql = "ALTER TABLE ";
  append_ident(&sql, tbl->db);
  sql += ".";
  append_ident(&sql, tbl->table_name);

  bool first = true;
  List_iterator<Create_field> it(alter_info->create_list);
  Create_field *field;
  while ((field = it++)) {
    if (field->charset != k_general_ci &&
        !field_is_indexed(alter_info, field->field_name))
      continue;
    if (has_unprintable_column(field)) {
      my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
               "wesql_orm_ddl_rewrite=ON");
      return true;
    }
    sql += first ? " " : ", ";
    first = false;
    sql += "MODIFY COLUMN ";
    if (print_column_def(field, &sql)) {
      my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
               "wesql_orm_ddl_rewrite=ON");
      return true;
    }
  }

  for (const Key_spec *key : alter_info->key_list) {
    if (key->type == KEYTYPE_FOREIGN) continue;
    if (key->generated) continue;
    if (key->type != KEYTYPE_UNIQUE && key->type != KEYTYPE_MULTIPLE) {
      my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
               "wesql_orm_ddl_rewrite=ON");
      return true;
    }
    sql += first ? " " : ", ";
    first = false;
    sql += (key->type == KEYTYPE_UNIQUE) ? "ADD UNIQUE INDEX " : "ADD INDEX ";
    if (key->name.str != nullptr) append_ident(&sql, key->name.str);
    sql += " (";
    bool first_col = true;
    for (const Key_part_spec *part : key->columns) {
      if (!first_col) sql += ", ";
      first_col = false;
      append_ident(&sql, part->get_field_name());
      if (part->get_prefix_length() > 0)
        sql += "(" + std::to_string(part->get_prefix_length()) + ")";
    }
    sql += ")";
  }

  if (first) {
    my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
             "wesql_orm_ddl_rewrite=ON");
    return true;
  }
  *out_sql = sql;
  return false;
}

const char *wesql_orm_fk_noop_sql() {
  return "DO 0 /* wesql_orm_ddl_rewrite stripped fk */";
}
