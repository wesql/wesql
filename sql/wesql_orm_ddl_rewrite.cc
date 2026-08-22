#include "sql/wesql_orm_ddl_rewrite.h"

#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "field_types.h"
#include "sql/create_field.h"
#include "sql/dd/types/column.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/sql_base.h"
#include "sql/table.h"
#include "sql/derror.h"
#include "sql/handler.h"
#include "sql/key_spec.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_table.h"
#include "sql/system_variables.h"

#include <vector>

namespace {

/*
  unicode_ci is compiled in strings/ctype-uca.cc but not declared in
  include/m_ctype.h. Keep the same extern the mysys charset tables use.
*/
extern CHARSET_INFO my_charset_utf8mb4_unicode_ci;

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
  LogErr(WARNING_LEVEL, ER_WESQL_ORM_COLLATION_REWRITTEN_IN_LOG, field_name,
         "utf8mb4_unicode_ci", "utf8mb4_general_ci");
}

void warn_fk(THD *thd, const char *name) {
  const char *shown =
      (name && name[0]) ? name : "unnamed foreign key";
  push_warning_printf(thd, Sql_condition::SL_WARNING,
                      ER_WESQL_ORM_FK_STRIPPED,
                      ER_THD(thd, ER_WESQL_ORM_FK_STRIPPED),
                      "wesql_orm_ddl_rewrite=ON", shown);
  LogErr(WARNING_LEVEL, ER_WESQL_ORM_FK_STRIPPED_IN_LOG, shown);
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

bool field_is_in_new_keys(const THD *thd, const char *field_name) {
  if (field_name == nullptr) return false;
  for (const THD::Wesql_orm_key_snap &key : thd->m_wesql_orm_alter_new_keys) {
    if (key.generated) continue;
    for (const THD::Wesql_orm_key_part_snap &part : key.columns) {
      if (!part.name.empty() &&
          my_strcasecmp(system_charset_info, part.name.c_str(), field_name) ==
              0)
        return true;
    }
  }
  return false;
}

bool has_unprintable_column(const Create_field *field) {
  if (field->is_gcol() || field->m_default_val_expr != nullptr) return true;
  if (field->hidden != dd::Column::enum_hidden_type::HT_VISIBLE) return true;
  if (field->auto_flags != Field::NONE) return true;
  if (field->m_engine_attribute.length > 0) return true;
  if (field->m_secondary_engine_attribute.length > 0) return true;
  if (field->field_storage_type() != HA_SM_DEFAULT) return true;
  if (field->column_format() != COLUMN_FORMAT_TYPE_DEFAULT) return true;
  if (field->m_srid.has_value()) return true;
  if (field->is_zerofill || field->is_array) return true;
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

bool print_column_def(THD *thd, const Create_field *field, std::string *out) {
  if (has_unprintable_column(field)) return true;
  if (field->constant_default != nullptr) {
    String def_buf;
    field->constant_default->print(thd, &def_buf, QT_ORDINARY);
    if (def_buf.length() == 0) return true;
  }
  append_ident(out, field->field_name);
  out->push_back(' ');
  Field *org = nullptr;
  if (thd->lex->query_tables != nullptr &&
      thd->lex->query_tables->table != nullptr) {
    org = find_field_in_table_sef(thd->lex->query_tables->table,
                                  field->field_name);
  }
  if (org == nullptr) return true;
  String type;
  org->sql_type(type);
  out->append(type.ptr(), type.length());
  if (field->charset != nullptr) {
    *out += " CHARACTER SET ";
    *out += field->charset->csname;
    *out += " COLLATE ";
    *out += field->charset->m_coll_name;
  }
  *out += field->is_nullable ? " NULL" : " NOT NULL";
  if (field->constant_default != nullptr) {
    String def_buf;
    field->constant_default->print(thd, &def_buf, QT_ORDINARY);
    *out += " DEFAULT ";
    out->append(def_buf.ptr(), def_buf.length());
  }
  if (field->comment.str != nullptr && field->comment.length > 0) {
    *out += " COMMENT ";
    String cmt;
    append_unescaped(&cmt, field->comment.str, field->comment.length);
    out->append(cmt.ptr(), cmt.length());
  }
  return false;
}

static void collect_indexed_unicode_columns(
    THD *thd, HA_CREATE_INFO *create_info, Alter_info *alter_info,
    bool use_new_key_snapshot, std::vector<Create_field *> *out) {
  if (alter_info == nullptr || out == nullptr) return;
  List_iterator<Create_field> it(alter_info->create_list);
  Create_field *field;
  while ((field = it++)) {
    const CHARSET_INFO *cs = field_charset(field, create_info);
    if (cs != k_unicode_ci) continue;
    const bool indexed =
        use_new_key_snapshot
            ? field_is_in_new_keys(thd, field->field_name)
            : field_is_indexed(alter_info, field->field_name);
    if (!indexed) continue;
    out->push_back(field);
  }
}

static unsigned int apply_indexed_unicode_columns(
    THD *thd, const std::vector<Create_field *> &fields) {
  unsigned int n = 0;
  for (Create_field *field : fields) {
    field->charset = k_general_ci;
    field->is_explicit_collation = true;
    thd->m_wesql_orm_remapped_columns.emplace_back(field->field_name);
    warn_collation(thd, field->field_name);
    ++n;
  }
  return n;
}

void snapshot_this_statement_keys(THD *thd, Alter_info *alter_info) {
  thd->m_wesql_orm_alter_new_keys.clear();
  thd->m_wesql_orm_alter_orig_flags =
      (alter_info != nullptr) ? alter_info->flags : 0;
  if (alter_info == nullptr) return;
  for (const Key_spec *key : alter_info->key_list) {
    if (key->type == KEYTYPE_FOREIGN) continue;
    THD::Wesql_orm_key_snap snap;
    snap.type = static_cast<int>(key->type);
    if (key->name.str != nullptr)
      snap.name.assign(key->name.str, key->name.length);
    snap.generated = key->generated;
    snap.is_visible = key->key_create_info.is_visible;
    if (key->key_create_info.is_algorithm_explicit ||
        key->key_create_info.block_size != 0 ||
        (key->key_create_info.comment.str != nullptr &&
         key->key_create_info.comment.length > 0) ||
        key->key_create_info.parser_name.str != nullptr ||
        !key->key_create_info.is_visible ||
        (key->key_create_info.m_engine_attribute.str != nullptr &&
         key->key_create_info.m_engine_attribute.length > 0) ||
        (key->key_create_info.m_secondary_engine_attribute.str != nullptr &&
         key->key_create_info.m_secondary_engine_attribute.length > 0))
      snap.unprintable = true;
    for (const Key_part_spec *part : key->columns) {
      if (part->has_expression() || !part->is_ascending() ||
          part->is_explicit())
        snap.unprintable = true;
      THD::Wesql_orm_key_part_snap p;
      if (part->get_field_name() != nullptr) p.name = part->get_field_name();
      p.prefix_length = part->get_prefix_length();
      snap.columns.push_back(std::move(p));
    }
    thd->m_wesql_orm_alter_new_keys.push_back(std::move(snap));
  }
}

}  // namespace

bool wesql_is_smartengine(const HA_CREATE_INFO *create_info) {
  return create_info != nullptr && create_info->db_type != nullptr &&
         create_info->db_type->db_type == DB_TYPE_SMARTENGINE;
}

void wesql_orm_reset_statement_state(THD *thd) {
  if (thd == nullptr) return;
  thd->m_wesql_orm_binlog_sql.clear();
  thd->m_wesql_orm_use_binlog_sql = false;
  thd->m_wesql_orm_remapped_columns.clear();
  thd->m_wesql_orm_alter_new_keys.clear();
  thd->m_wesql_orm_alter_orig_flags = 0;
}

static Wesql_orm_rewrite_result strip_fk_pair(THD *thd, Alter_info *alter_info,
                                              bool enabled, bool is_alter) {
  Wesql_orm_rewrite_result result;
  if (alter_info == nullptr) return result;

  Mem_root_array<Key_spec *> kept(thd->mem_root);
  Mem_root_array<const char *> fk_names(thd->mem_root);
  for (size_t i = 0; i < alter_info->key_list.size(); ++i) {
    Key_spec *key = alter_info->key_list[i];
    if (key->type == KEYTYPE_FOREIGN) {
      if (!enabled) {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0),
                 "foreign key constraints");
        result.action = Wesql_orm_rewrite_action::kReject;
        return result;
      }
      fk_names.push_back(key->name.str);
      continue;
    }
    if (enabled && key->type == KEYTYPE_MULTIPLE && key->generated) {
      continue;
    }
    kept.push_back(key);
  }

  if (fk_names.empty()) return result;
  if (!enabled) return result;

  bool mixed = false;
  if (is_alter) {
    if (alter_info->create_list.elements > 0) mixed = true;
    if (alter_info->flags & ~Alter_info::ADD_FOREIGN_KEY) mixed = true;
    for (Key_spec *k : kept) {
      if (!(k->type == KEYTYPE_MULTIPLE && k->generated)) mixed = true;
    }
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
  for (const char *name : fk_names) warn_fk(thd, name);
  result.fk_stripped = static_cast<unsigned int>(fk_names.size());
  result.action = Wesql_orm_rewrite_action::kRewritten;
  return result;
}

Wesql_orm_rewrite_result wesql_orm_rewrite_create(THD *thd,
                                                  HA_CREATE_INFO *create_info,
                                                  Alter_info *alter_info) {
  Wesql_orm_rewrite_result result;
  /*
    create_table_impl() is also the ALTER copy path. Do not wipe the
    landed ALTER SQL that mysql_alter_table() already built.
  */
  if (thd != nullptr && thd->lex != nullptr &&
      thd->lex->sql_command != SQLCOM_ALTER_TABLE &&
      thd->lex->sql_command != SQLCOM_CREATE_INDEX &&
      thd->lex->sql_command != SQLCOM_DROP_INDEX) {
    wesql_orm_reset_statement_state(thd);
  }
  if (!wesql_is_smartengine(create_info)) return result;

  const bool on = rewrite_enabled(thd);
  thd->m_wesql_orm_remapped_columns.clear();
  result = strip_fk_pair(thd, alter_info, on, false);
  if (result.action == Wesql_orm_rewrite_action::kReject) return result;

  if (on) {
    std::vector<Create_field *> cols;
    collect_indexed_unicode_columns(thd, create_info, alter_info, false,
                                    &cols);
    result.collation_rewritten = apply_indexed_unicode_columns(thd, cols);
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
  wesql_orm_reset_statement_state(thd);
  if (!wesql_is_smartengine(create_info)) return result;
  thd->m_wesql_orm_remapped_columns.clear();
  result = strip_fk_pair(thd, alter_info, rewrite_enabled(thd), true);
  if (result.action == Wesql_orm_rewrite_action::kReject) return result;

  /*
    prepare_fields_and_keys() copies every old index into key_list.
    Snapshot this-statement keys now so landed SQL only prints new ones.
  */
  snapshot_this_statement_keys(thd, alter_info);

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
  thd->m_wesql_orm_remapped_columns.clear();
  std::vector<Create_field *> cols;
  collect_indexed_unicode_columns(thd, create_info, alter_info, true, &cols);
  if (cols.empty()) return result;

  /*
    Landed SQL only prints remapped columns + this-statement indexes.
    Any other original clause (ADD/MODIFY/DROP COLUMN, table options, …)
    would be dropped on replay. Reject unless flags are ADD INDEX only.
    Index direction / visibility / algorithm / comment cannot be printed
    losslessly — 7518 rather than silently drop them.
    Do not mutate or emit success warnings/logs until the statement is
    accepted.
  */
  const ulonglong extra =
      thd->m_wesql_orm_alter_orig_flags & ~Alter_info::ALTER_ADD_INDEX;
  bool unprintable_key = false;
  for (const THD::Wesql_orm_key_snap &key : thd->m_wesql_orm_alter_new_keys) {
    if (key.generated) continue;
    if (key.unprintable ||
        (key.type != KEYTYPE_UNIQUE && key.type != KEYTYPE_MULTIPLE)) {
      unprintable_key = true;
      break;
    }
  }
  bool unprintable_col = false;
  for (const Create_field *field : cols) {
    if (has_unprintable_column(field)) {
      unprintable_col = true;
      break;
    }
  }
  if (extra != 0 || unprintable_key || unprintable_col) {
    my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
             "wesql_orm_ddl_rewrite=ON");
    result.action = Wesql_orm_rewrite_action::kReject;
    return result;
  }

  result.collation_rewritten = apply_indexed_unicode_columns(thd, cols);
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
    bool remapped = false;
    for (const std::string &n : thd->m_wesql_orm_remapped_columns) {
      if (field->field_name != nullptr && n == field->field_name) {
        remapped = true;
        break;
      }
    }
    if (!remapped) continue;
    if (has_unprintable_column(field)) {
      my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
               "wesql_orm_ddl_rewrite=ON");
      return true;
    }
    sql += first ? " " : ", ";
    first = false;
    sql += "MODIFY COLUMN ";
    if (print_column_def(thd, field, &sql)) {
      my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
               "wesql_orm_ddl_rewrite=ON");
      return true;
    }
  }

  for (const THD::Wesql_orm_key_snap &key : thd->m_wesql_orm_alter_new_keys) {
    if (key.generated) continue;
    if (key.unprintable ||
        (key.type != KEYTYPE_UNIQUE && key.type != KEYTYPE_MULTIPLE)) {
      my_error(ER_WESQL_ORM_ALTER_NOT_REWRITABLE, MYF(0),
               "wesql_orm_ddl_rewrite=ON");
      return true;
    }
    sql += first ? " " : ", ";
    first = false;
    sql += (key.type == KEYTYPE_UNIQUE) ? "ADD UNIQUE INDEX " : "ADD INDEX ";
    if (!key.name.empty()) append_ident(&sql, key.name.c_str());
    sql += " (";
    bool first_col = true;
    for (const THD::Wesql_orm_key_part_snap &part : key.columns) {
      if (!first_col) sql += ", ";
      first_col = false;
      append_ident(&sql, part.name.c_str());
      if (part.prefix_length > 0)
        sql += "(" + std::to_string(part.prefix_length) + ")";
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
