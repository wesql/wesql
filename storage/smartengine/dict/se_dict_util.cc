/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "se_dict_util.h"
#include "dd/dd_schema.h"
#include "dd/dictionary.h"
#include "field.h"
#include "field_types.h"
#include "mysql/strings/m_ctype.h"
#include "sql_class.h"
#include "dd/cache/dictionary_client.h"
#include "dict/se_index.h"
#include "dict/se_dd_operations.h"
#include "logger/log_module.h"

extern CHARSET_INFO my_charset_gbk_bin;
extern CHARSET_INFO my_charset_gbk_chinese_ci;

namespace smartengine
{
extern const char *const se_hton_name;

/*
  This is
  - the name of the default Column Family (the CF which stores indexes which
    didn't explicitly specify which CF they are in)
  - the name used to set the default column family parameter for per-cf
    arguments.
*/
const char *const DEFAULT_CF_NAME = "default";

/*
  This is the databae_name of smartengine system table
*/
const char * DEFAULT_SYSTEM_DATABASE_NAME = "__smartengine__";

/*
  This is the table_name of smartengine system table
*/
const char * DEFAULT_SYSTEM_TABLE_NAME = "__system__";

/*
  This is the name of the Column Family used for storing the data dictionary.
*/
const char *const DEFAULT_SYSTEM_SUBTABLE_NAME = "__system__";

/*
  Now, we only use cf_id communicate with subtable in se, for system_cf, cf_id is 1,
  for other user subtable, cf_id is same with index_id, which is start from 256.
*/
const int DEFAULT_SYSTEM_SUBTABLE_ID = 1;

/*
  This is the default system table space id, use for system subtable or other subtable which table space is shared
*/
const int DEFAULT_SYSTEM_TABLE_SPACE_ID = 0;

/*
  This is the name of the hidden primary key for tables with no pk.
*/
const char *const HIDDEN_PK_NAME = "HIDDEN_PK_ID";

/*
  Name for the background thread.
*/
const char *const BG_THREAD_NAME = "se-bg";

/*
  Name for the drop index thread.
*/
const char *const INDEX_THREAD_NAME = "se-index";

/*
  Name for the renewal objstore lease lock thread.
*/
const char *const RENEW_LEASE_LOCK_THREAD_NAME = "se-renewal";

/* SE supports only the following collations for indexed columns */
static const std::set<const my_core::CHARSET_INFO *> SE_INDEX_COLLATIONS =
  {&my_charset_bin, &my_charset_latin1_bin,
   &my_charset_utf8mb3_bin, &my_charset_utf8mb3_general_ci,
   &my_charset_gbk_bin, &my_charset_gbk_chinese_ci,
   &my_charset_utf8mb4_bin, &my_charset_utf8mb4_general_ci,
   &my_charset_utf8mb4_0900_ai_ci};

std::string& gen_se_supported_collation_string()
{
  static std::string se_supported_collation_string;
  if (se_supported_collation_string.empty()) {
    // sort by name -- only do once
    std::set<std::string> names;
    for (const auto &coll : SE_INDEX_COLLATIONS) {
      names.insert(coll->m_coll_name);
    }
    bool first_coll = true;
    std::ostringstream oss;
    for (const auto &name: names) {
      if (!first_coll) {
        oss << ", ";
      } else {
        first_coll = false;
      }
      oss << name;
    }
    se_supported_collation_string = oss.str();
  }
  return se_supported_collation_string;
}

bool se_is_index_collation_supported(const my_core::Field *const field)
{
  const my_core::enum_field_types type = field->real_type();
  /* Handle [VAR](CHAR|BINARY) or TEXT|BLOB */
  if (type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING ||
      type == MYSQL_TYPE_BLOB) {
    return SE_INDEX_COLLATIONS.find(field->charset()) !=
           SE_INDEX_COLLATIONS.end();
  }
  return true;
}

/*
  Inspired by innobase_get_int_col_max_value from InnoDB. This returns the
  maximum value a type can take on.
*/
ulonglong se_get_int_col_max_value(const Field *field)
{
  ulonglong max_value = 0;
  switch (field->key_type()) {
  case HA_KEYTYPE_BINARY:
    max_value = 0xFFULL;
    break;
  case HA_KEYTYPE_INT8:
    max_value = 0x7FULL;
    break;
  case HA_KEYTYPE_USHORT_INT:
    max_value = 0xFFFFULL;
    break;
  case HA_KEYTYPE_SHORT_INT:
    max_value = 0x7FFFULL;
    break;
  case HA_KEYTYPE_UINT24:
    max_value = 0xFFFFFFULL;
    break;
  case HA_KEYTYPE_INT24:
    max_value = 0x7FFFFFULL;
    break;
  case HA_KEYTYPE_ULONG_INT:
    max_value = 0xFFFFFFFFULL;
    break;
  case HA_KEYTYPE_LONG_INT:
    max_value = 0x7FFFFFFFULL;
    break;
  case HA_KEYTYPE_ULONGLONG:
    max_value = 0xFFFFFFFFFFFFFFFFULL;
    break;
  case HA_KEYTYPE_LONGLONG:
    max_value = 0x7FFFFFFFFFFFFFFFULL;
    break;
  case HA_KEYTYPE_FLOAT:
    max_value = 0x1000000ULL;
    break;
  case HA_KEYTYPE_DOUBLE:
    max_value = 0x20000000000000ULL;
    break;
  default:
    abort();
  }

  return max_value;
}

int advice_fix_index(TABLE *table,
                     int64_t keyno,
                     int64_t hidden_pk_id,
                     uchar *data,
                     const char *op)
{
  if (nullptr == table || nullptr == table->s ||
      keyno >= table->s->keys || nullptr == data) {
    return 1;
  }

  // Get the useful columns including sk key parts and pk key parts.
  bool has_hidden_pk = MAX_INDEXES == table->s->primary_key;
  std::set<int64_t> fix_fields;
  if (!has_hidden_pk) {
    KEY *pk_info = &(table->key_info[table->s->primary_key]);
    for (int64_t i = 0; i < pk_info->actual_key_parts; ++i) {
      fix_fields.insert(pk_info->key_part[i].field->field_index());
    }
  }
  KEY *sk_info = &(table->key_info[keyno]);
  for (int64_t i = 0; i < sk_info->actual_key_parts; ++i) {
    fix_fields.insert(sk_info->key_part[i].field->field_index());
  }

  std::string fix_sql_str;
  std::string field_name_str;
  std::string target_value_str;
  fix_sql_str.reserve(101);
  field_name_str.reserve(101);
  target_value_str.reserve(101);
  fix_sql_str.append(op);
  fix_sql_str.push_back('(');
  fix_sql_str.append(sk_info->name);
  if (has_hidden_pk) {
    fix_sql_str.push_back(' ');
    fix_sql_str.append(std::to_string(hidden_pk_id));
  }
  fix_sql_str.append(")  INTO ");
  fix_sql_str.append(table->s->db.str);
  fix_sql_str.push_back('.');
  fix_sql_str.append(table->s->table_name.str);

  field_name_str.push_back('(');
  target_value_str.push_back('(');
  for (int64_t field_index = 0; field_index < table->s->fields; ++field_index) {
    Field *const field = table->field[field_index];
    enum_field_types field_type = field->real_type();
    uint field_offset = field->field_ptr() - table->record[0];
    uint null_offset = field->null_offset();
    bool maybe_null = field->is_nullable();

    // If this field is one of the indexed ones, get user data from data which
    // is from table->record[0], else, use table->s->default_values.
    uchar *buf = fix_fields.find(field_index) != fix_fields.end() ? data :
                  table->s->default_values;

    field_name_str.append(field->field_name);
    field_name_str.push_back(',');
    field->move_field(buf + field_offset,
                      maybe_null ? buf + null_offset : nullptr,
                      field->null_bit);
    if (field->is_real_null()) {
      target_value_str.append("NULL");
    } else if (MYSQL_TYPE_BLOB == field->type()) {
      // BLOB can not be index, use default empty string.
      target_value_str.append("\'\'");
    } else if (MYSQL_TYPE_VARCHAR == field->real_type() && field->binary()) {
      // For BINARY or BIT type, output using hex format.
      auto field_varstring = static_cast<Field_varstring *>(field);
      uint32_t data_len = 0;
      if (1 == field_varstring->get_length_bytes()) {
        data_len = static_cast<uint32_t>(field->field_ptr()[0]);
      } else {
        data_len = static_cast<uint32_t>(uint2korr(field->field_ptr()));
      }
      if (data_len > 0) {
        char *data_ptr =
            reinterpret_cast<char*>(field->field_ptr()) + field_varstring->get_length_bytes();
        target_value_str.append("0x");
        target_value_str.append(se_hexdump(data_ptr, data_len, data_len * 2));
      } else {
        target_value_str.append("0x0");
      }
    } else if ((MYSQL_TYPE_STRING == field->real_type() ||
                MYSQL_TYPE_BIT == field->real_type()) && field->binary()) {
      target_value_str.append("0x");
      target_value_str.append(se_hexdump(reinterpret_cast<char*>(field->field_ptr()),
                                          field->pack_length(),
                                          field->pack_length() * 2));
    } else {
      String buffer;
      String value;
      String *convert_result = field->val_str(&buffer, &value);
      if (field->str_needs_quotes()) {
        target_value_str.push_back('\'');
      }
      target_value_str.append(convert_result->c_ptr_safe());
      if (field->str_needs_quotes()) {
        target_value_str.push_back('\'');
      }
    }
    field->move_field(table->record[0] + field_offset,
                      maybe_null ? table->record[0] + null_offset : nullptr,
                      field->null_bit);
    target_value_str.push_back(',');
  }
  field_name_str.back() = ')';
  target_value_str.back() = ')';

  fix_sql_str.append(field_name_str);
  fix_sql_str.append(" VALUES ");
  fix_sql_str.append(target_value_str);
  fix_sql_str.push_back(';');

  // NO_LINT_DEBUG
  sql_print_error("CHECK TABLE %s.%s: ADVICE %s",
                  table->s->db.str, table->s->table_name.str,
                  fix_sql_str.data());
  return 0;
}

int advice_del_index(TABLE *table,
                     int64_t keyno,
                     int64_t hidden_pk_id,
                     uchar *data)
{
  return advice_fix_index(table, keyno, hidden_pk_id, data,
                          "DELETE INDEX ROW");
}

int advice_add_index(TABLE *table,
                     int64_t keyno,
                     int64_t hidden_pk_id,
                     uchar *data)
{
  return advice_fix_index(table, keyno, hidden_pk_id, data,
                          "INSERT INDEX ROW");
}

db::Range get_range(uint32_t i,
                    uchar buf[SeKeyDef::INDEX_NUMBER_SIZE * 2],
                    int offset1,
                    int offset2)
{
  uchar *buf_begin = buf;
  uchar *buf_end = buf + SeKeyDef::INDEX_NUMBER_SIZE;
  se_netbuf_store_index(buf_begin, i + offset1);
  se_netbuf_store_index(buf_end, i + offset2);

  return db::Range(
      common::Slice((const char *)buf_begin, SeKeyDef::INDEX_NUMBER_SIZE),
      common::Slice((const char *)buf_end, SeKeyDef::INDEX_NUMBER_SIZE));
}

db::Range get_range(const SeKeyDef &kd,
                    uchar buf[SeKeyDef::INDEX_NUMBER_SIZE * 2],
                    int offset1,
                    int offset2)
{
  return get_range(kd.get_index_number(), buf, offset1, offset2);
}

db::Range get_range(const SeKeyDef &kd, uchar buf[SeKeyDef::INDEX_NUMBER_SIZE * 2])
{
  if (kd.m_is_reverse_cf) {
    return smartengine::get_range(kd, buf, 1, 0);
  } else {
    return smartengine::get_range(kd, buf, 0, 1);
  }
}

bool looks_like_per_index_cf_typo(const char *const name)
{
  /**Column family name which means "put this index into its own column family".
  See SeSubtableManager::get_per_index_cf_name().*/
  const char *PER_INDEX_CF_NAME = "$per_index_cf";
  return (name && name[0] == '$' && strcmp(name, PER_INDEX_CF_NAME));
}

bool SeDdHelper::traverse_all_se_tables(
    THD *thd, bool exit_on_failure, ulong lock_timeout,
    std::function<bool(const dd::Schema *, const dd::Table *)> &&visitor) {
  auto client = thd->dd_client();
  std::vector<dd::String_type> schema_names;
  if (client->fetch_global_component_names<dd::Schema>(&schema_names)) {
    HANDLER_LOG(ERROR, "SE: failed to fetch schema names!");
    return true;
  }

  for (auto &schema_name : schema_names) {
    dd::Schema_MDL_locker schema_mdl_locker(thd);
    dd::cache::Dictionary_client::Auto_releaser schema_releaser(client);
    const dd::Schema *dd_schema = nullptr;
    std::vector<dd::String_type> table_names;
    if (schema_mdl_locker.ensure_locked(schema_name.c_str()) ||
        client->acquire(schema_name, &dd_schema)) {
      HANDLER_LOG(ERROR, "SE: failed to acquire dd::Schema object",
                  "schema_name", schema_name.c_str());
      if (!exit_on_failure) continue;
      return true;
    } else if (nullptr == dd_schema) {
      HANDLER_LOG(INFO, "SE: dd::Schema was dropped", "schema_name",
                  schema_name.c_str());
      continue;
    } else if (client->fetch_schema_table_names_by_engine(
                   dd_schema, se_hton_name, &table_names)) {
      HANDLER_LOG(ERROR,"SE: failed to fetch table names from dd::Schema",
                  "schema_name", schema_name.c_str());
      if (!exit_on_failure) continue;
      return true;
    } else {
      for (auto &table_name : table_names) {
        dd::cache::Dictionary_client::Auto_releaser tbl_releaser(client);
        MDL_ticket *tbl_ticket = nullptr;
        const dd::Table *dd_table = nullptr;
        if (!acquire_se_table(thd, lock_timeout, schema_name.c_str(),
                                   table_name.c_str(), dd_table, tbl_ticket) &&
            (nullptr != dd_table)) {
          bool error = visitor(dd_schema, dd_table);
          if (tbl_ticket) dd::release_mdl(thd, tbl_ticket);
          if (error && exit_on_failure) return true;
        }
      }
    }
  }
  return false;
}

bool SeDdHelper::get_schema_id_map(
    dd::cache::Dictionary_client* client,
    std::vector<const dd::Schema*>& all_schemas,
    std::map<dd::Object_id, const dd::Schema*>& schema_id_map)
{
  all_schemas.clear();
  schema_id_map.clear();
  if (!client->fetch_global_components(&all_schemas)) {
    for (auto& sch : all_schemas) {
      schema_id_map.insert({sch->id(), sch});
    }
    return false;
  }
  return true;
}

bool SeDdHelper::get_se_subtable_map(THD* thd,
    std::map<uint32_t, std::pair<std::string, std::string>>& subtable_map)
{
  subtable_map.clear();
  bool error = SeDdHelper::traverse_all_se_tables(
      thd, true, 0,
      [&subtable_map](const dd::Schema *dd_schema,
                      const dd::Table *dd_table) -> bool {
        std::ostringstream oss;
        oss << dd_schema->name() << '.' << dd_table->name();

        uint32_t subtable_id = 0;
        for (auto &dd_index : dd_table->indexes()) {
          const auto &p = dd_index->se_private_data();
          if (!p.exists(dd_index_key_strings[DD_INDEX_SUBTABLE_ID]) ||
              p.get(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], &subtable_id)) {
            HANDLER_LOG(ERROR, "SE: failed to get subtable_id",
                        "table_name", dd_table->name().c_str(),
                        "index_name", dd_index->name().c_str());
            return true;
          } else {
            subtable_map.emplace(
                subtable_id,
                std::make_pair(oss.str(), dd_index->name().c_str()));
          }
        }

        const auto &p = dd_table->se_private_data();
        if (p.exists(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID])) {
          if (p.get(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID],
                    &subtable_id)) {
            HANDLER_LOG(ERROR, "SE: failed to get subtable_id for hidden pk",
                        "table_name", dd_table->name().c_str());
            return true;
          } else {
            subtable_map.emplace(subtable_id,
                                 std::make_pair(oss.str(), HIDDEN_PK_NAME));
          }
        }

        return false;
      });

  return error;
}

bool SeDdHelper::acquire_se_table(THD* thd, ulong lock_timeout,
    const char* schema_name, const char* table_name,
    const dd::Table*& dd_table, MDL_ticket*& mdl_ticket)
{
  bool error = false;
  dd_table = nullptr;
  // acquire the shared MDL lock if don't own
  if (!dd::has_shared_table_mdl(thd, schema_name, table_name)) {
    if (!lock_timeout) lock_timeout = thd->variables.lock_wait_timeout;
    MDL_request mdl_request;
    MDL_REQUEST_INIT(&mdl_request, MDL_key::TABLE, schema_name, table_name,
                     MDL_SHARED, MDL_EXPLICIT);
    if (thd->mdl_context.acquire_lock(&mdl_request, lock_timeout)) {
      HANDLER_LOG(ERROR, "SE: failed to acquire shared lock on dd::Table",
                  K(schema_name), K(table_name));
      return true;
    } else {
      mdl_ticket = mdl_request.ticket;
    }
  }

  // acquire the dd::Table and verify if it is SE table
  if (thd->dd_client()->acquire(schema_name, table_name, &dd_table)) {
    HANDLER_LOG(ERROR, "SE: failed to acquire dd::Table", K(schema_name), K(table_name));
    error = true;
  } else if (!dd_table|| dd_table->engine() != se_hton_name) {
    HANDLER_LOG(WARN, "SE: no such se table", K(schema_name), K(table_name));
    dd_table = nullptr;
  }

  if (!dd_table && mdl_ticket) {
    dd::release_mdl(thd, mdl_ticket);
    mdl_ticket = nullptr;
  }

  return error;
}

bool SeDdHelper::get_se_subtable_ids(THD* thd, ulong lock_timeout,
                                             std::set<uint32_t> &subtable_ids)
{
  subtable_ids.clear();
  bool error = traverse_all_se_tables(
      thd, true, lock_timeout,
      [&subtable_ids](const dd::Schema *, const dd::Table *dd_table) -> bool {
        uint32_t subtable_id = 0;
        for (auto &dd_index : dd_table->indexes()) {
          const auto &p = dd_index->se_private_data();
          if (!p.exists(dd_index_key_strings[DD_INDEX_SUBTABLE_ID]) ||
              p.get(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], &subtable_id)) {
            HANDLER_LOG(ERROR, "SE: failed to get subtable_id",
                        "table_name", dd_table->name().c_str(),
                        "index_name", dd_index->name().c_str());
            return true;
          }
          subtable_ids.insert(subtable_id);
        }

        const auto &p = dd_table->se_private_data();
        if (p.exists(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID])) {
          if (p.get(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID],
                    &subtable_id)) {
            HANDLER_LOG(ERROR, "SE: failed to get subtable_id for hidden pk",
                        "table_name", dd_table->name().c_str());
            return true;
          } else {
            subtable_ids.insert(subtable_id);
          }
        }
        return false;
      });

  if (error) {
    HANDLER_LOG(ERROR, "SE: failed to collect subtable ids for all SE table from data dictionary");
  }

  return error;
}

bool is_smartengine_system_database(const char *database_name)
{
  return (strlen(DEFAULT_SYSTEM_DATABASE_NAME) == strlen(database_name)) &&
         (0 == strncasecmp(DEFAULT_SYSTEM_DATABASE_NAME, database_name, strlen(database_name)));
}

} // namespace smartengine
