/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012, Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include "dd/cache/dictionary_client.h"
#include "se_transaction.h"
#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#define MYSQL_SERVER 1

#include "ha_smartengine.h"
#include "sql_class.h"
#include "handler/handler_alter.h"
#include <algorithm>
#include <string>
#include <atomic>
#include "debug_sync.h"
#include "my_sys.h"
#include "sql_audit.h"
#include "sql_table.h"
#include "m_string.h"
#include "mysql/thread_pool_priv.h"
#include "debug_sync.h"
#include "json_dom.h"
#include "mysqld.h"
#include "sql_audit.h"
#include "sql_lex.h"
#include "sql_partition.h"
#include "sql_table.h"
#include "sql_thd_internal_api.h"
#include "table.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/sql_base.h"
#include "core/monitoring/query_perf_context.h"
#include "core/util/string_util.h"
#include "transactions/transaction_db_impl.h"
#include "dict/se_cf_manager.h"
#include "dict/se_log_ddl.h"
#include "dict/se_binlog_manager.h"
#include "dict/se_ddl_manager.h"
#include "dict/se_dict_manager.h"
#include "dict/se_dict_util.h"
#include "dict/se_table.h"
#include "handler/se_hton.h"
#include "index/se_index_merge.h"
#include "index/se_parallel_ddl_ctx.h"
#include "util/se_threads.h"
#include "plugin/se_system_vars.h"
#include "transaction/se_transaction_factory.h"
#include "write_batch/write_batch_with_index.h"
#include "schema/record_format.h"
#include "storage/storage_logger.h"

#ifdef HAVE_ASAN
#ifndef HAVE_purify
#define HAVE_purify
#endif
#endif

namespace smartengine {

ha_smartengine::ha_smartengine(
    my_core::handlerton *const hton,
    my_core::TABLE_SHARE *const table_arg)
    : handler(hton, table_arg),
      m_table_handler(nullptr),
      m_scan_it(nullptr),
      m_cmp_end_key(true),
      m_max_packed_sk_len(0),
      m_scan_it_skips_bloom(false),
      m_scan_it_snapshot(nullptr),
      m_tbl_def(nullptr),
      m_fields_no_needed_to_decode(0),
      m_pk_descr(nullptr),
      m_key_descr_arr(nullptr),
      m_pk_can_be_decoded(false),
      m_maybe_unpack_info(false),
      m_pk_tuple(nullptr),
      m_pk_packed_tuple(nullptr),
      m_pk_packed_tuple_old(nullptr),
      m_sk_packed_tuple(nullptr),
      m_end_key_packed_tuple(nullptr),
      m_sk_match_prefix(nullptr),
      m_sk_match_prefix_buf(nullptr),
      m_sk_packed_tuple_old(nullptr),
      m_dup_sk_packed_tuple(nullptr),
      m_dup_sk_packed_tuple_old(nullptr),
      m_pack_buffer(nullptr),
      m_new_pk_descr(nullptr),
      m_new_record(nullptr),
      m_lock_rows(SE_LOCK_NONE),
      m_keyread_only(FALSE),
      m_bulk_load_tx(nullptr),
      m_encoder_arr(nullptr),
      m_row_checksums_checked(0),
      m_in_rpl_delete_rows(false),
      m_in_rpl_update_rows(false),
      m_force_skip_unique_check(false)
{
  // TODO(alexyang): create a valid PSI_mutex_key for this mutex
  mysql_mutex_init(0, &m_bulk_load_mutex, MY_MUTEX_INIT_FAST);
}

ha_smartengine::~ha_smartengine()
{
  int err MY_ATTRIBUTE((__unused__));
  err = finalize_bulk_load();
  assert(err == 0);
  mysql_mutex_destroy(&m_bulk_load_mutex);
}

/**copy from handler::clone, and acquire table_def which store instant-added
columns information.
@param[in] name   table name
@param[in] mem_root   memory context*/
handler *ha_smartengine::clone(const char *name, MEM_ROOT *mem_root)
{
  DBUG_TRACE;

  handler *new_handler = get_new_handler(
      table->s, (table->s->m_part_info != nullptr), mem_root, ht);

  THD *thd = ha_thd();
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Table *table_def = nullptr;

  if (!new_handler) return nullptr;
  if (new_handler->set_ha_share_ref(this->get_ha_share_ref())) goto err;

  /*
    Allocate handler->ref here because otherwise ha_open will allocate it
    on this->table->mem_root and we will not be able to reclaim that memory
    when the clone handler object is destroyed.
  */
  if (!(new_handler->ref =
            (uchar *)mem_root->Alloc(ALIGN_SIZE(ref_length) * 2)))
    goto err;

  if (thd->dd_client()->acquire(table->s->db.str, table->s->table_name.str,
                                &table_def)) {
    goto err;
  }

  if (!table_def) {
    set_my_errno(ENOENT);
    goto err;
  }

  /*
    TODO: Implement a more efficient way to have more than one index open for
    the same table instance. The ha_open call is not cachable for clone.
  */
  if (new_handler->ha_open(table, name, table->db_stat,
                           HA_OPEN_IGNORE_IF_LOCKED, table_def))
    goto err;

  return new_handler;

err:
  destroy(new_handler);
  return nullptr;
}

/**
  @brief
  create() is called to create a table. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/
int ha_smartengine::create(const char *const name,
                           TABLE *const table_arg,
                           HA_CREATE_INFO *const create_info,
                           dd::Table *dd_table)
{
  DBUG_ENTER_FUNC();

  assert(table_arg != nullptr);
  assert(create_info != nullptr);

  if (create_info->data_file_name) {
    // DATA DIRECTORY is used to create tables under a specific location
    // outside the MySQL data directory. SE doesn't support this.
    // The `se_data_dir` setting should be used to configure se data
    // directory.
    DBUG_RETURN(HA_ERR_SE_TABLE_DATA_DIRECTORY_NOT_SUPPORTED);
  }

  if (create_info->index_file_name) {
    // Similar check for INDEX DIRECTORY as well.
    DBUG_RETURN(HA_ERR_SE_TABLE_INDEX_DIRECTORY_NOT_SUPPORTED);
  }

  std::string str;
  int err;
  THD *const thd = my_core::thd_get_current_thd();

  /*
    Construct dbname.tablename ourselves, because parititioning
    passes strings like "./test/t14#P#p0" for individual partitions,
    while table_arg->s->table_name has none of that.
  */
  if (se_normalize_tablename(name, &str)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  bool do_truncate = (thd->lex->sql_command == SQLCOM_TRUNCATE);
  bool from_dict = false;
  auto table_def = dd_table ? ddl_manager.find(dd_table, str, &from_dict)
                           : ddl_manager.find(thd, str, &from_dict);
  if (table_def) {
    // for truncate, delete the table first
    if (do_truncate) {
      // put it into cache for later usage in delete_table
      if (from_dict) {
        ddl_manager.put(table_def);
      }
      err = delete_table(name, dd_table);
      if (err != HA_EXIT_SUCCESS) {
        DBUG_RETURN(err);
      }

      DBUG_EXECUTE_IF("ddl_log_crash_truncate_after_delete_table", DBUG_SUICIDE(););
    } else if (is_prefix(table_def->base_tablename().c_str(), tmp_file_prefix)) {
      HANDLER_LOG(WARN, "SE: find garbage temporary table in dictionary", "table_name", str);
      assert(!from_dict);
      // remove cache entry of the table
      ddl_manager.remove_cache(table_def->full_tablename());
      se_drop_idx_thread.signal();
    } else {
      my_printf_error(ER_UNKNOWN_ERROR, "Table '%s' doesn't exist in SE, "
          "but metadata information exists. This means that data inconsistency "
          "happens. Please contact IT support to try to restore it.",
          MYF(0), str.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
  }

  if (contains_foreign_key(thd)) {
    my_printf_error(ER_NOT_SUPPORTED_YET, "SE currently doesn't support "
                                          "foreign key constraints",
                    MYF(0));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  bool write_err = false;
  //const std::unique_ptr<db::WriteBatch> wb = dict_manager.begin();
  //db::WriteBatch *const batch = wb.get();
  SeTransaction *const tx = get_or_create_tx(thd);
  se_register_tx(ht, thd, tx);
  db::WriteBatch *const batch = dynamic_cast<db::WriteBatch *>(tx->get_blind_write_batch());

  /* Create table/key descriptions and put them into the data dictionary */
  m_tbl_def = std::make_shared<SeTableDef>(str);

  uint n_keys = table_arg->s->keys;

  /*
    If no primary key found, create a hidden PK and place it inside table
    definition
  */
  if (has_hidden_pk(table_arg)) {
    n_keys += 1;
  }

  if (do_truncate && (dd::INVALID_OBJECT_ID != dd_table->se_private_id())) {
    // for truncate table, use old table_id
    // TODO (Zhao Dongsheng) : use old table space id?
    m_tbl_def->set_table_id(dd_table->se_private_id());
  } else if (m_tbl_def->init_table_id(ddl_manager)) {
    goto error;
  }

  m_key_descr_arr = new std::shared_ptr<SeKeyDef>[n_keys];
  m_tbl_def->m_key_count = n_keys;
  m_tbl_def->m_key_descr_arr = m_key_descr_arr;

  // TODO(Zhao Dongsheng): the dd table here has engine attribute?
  if (common::Status::kOk != create_key_defs(table_arg,
                                             nullptr /*old_table*/,
                                             dd_table,
                                             m_tbl_def.get(),
                                             nullptr /*old_table_def*/,
                                             create_info->engine_attribute,
                                             false /*need_rebuild*/)) {
    SE_LOG(WARN, "fail to create key defs", "table_name", str);
    goto error;
  } 

  if (m_tbl_def->write_dd_table(dd_table)) {
    goto error;
  }

  m_pk_descr = m_key_descr_arr[pk_index(table_arg, m_tbl_def.get())];

  dict_manager.lock();
  write_err = ddl_manager.put_and_write(m_tbl_def, batch, &ddl_log_manager,
                                        thd_thread_id(thd), true);
  dict_manager.unlock();

  if (write_err) {
    goto error;
  }

  if (create_info->auto_increment_value)

    m_tbl_def->m_auto_incr_val = create_info->auto_increment_value;

  /*
    We only support auto_increment at start of the PRIMARY KEY.
  */
  // Field *field;
  // if ((field= table_arg->next_number_field))
  /* TODO mdcallag: disable this for now to let UNIQUE indexes kind of work
  if ((field= table_arg->found_next_number_field))
  {
    int pk= table_arg->s->primary_key;
    Field *pk_field= table_arg->key_info[pk].key_part[0].field;
    if (field->field_index !=  pk_field->field_index)
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  */

  DBUG_EXECUTE_IF("ddl_log_crash_create_after_se_success", DBUG_SUICIDE(););

  /* mark se modified that write atomic_ddl_log, make sure hton already
     registed. if not, mark_ddl_trx_read_write won't do anything.
   */
  mark_ddl_trx_read_write();

  DBUG_RETURN(HA_EXIT_SUCCESS);

error:
  /* Delete what we have allocated so far */
  m_tbl_def.reset();
  m_pk_descr = nullptr;
  m_key_descr_arr = nullptr;

  DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
}

/**Note: the following function is called when the table is not open. That is,
  this->table==nullptr, pk_key_descr==nullptr, etc.
  tablename points to line in form "./dbname/tablename".*/
int ha_smartengine::delete_table(const char *const tablename, const dd::Table *table_def)
{
  DBUG_ENTER_FUNC();

  assert(tablename != nullptr);

  THD *const thd = table ? table->in_use : my_core::thd_get_current_thd();
  SeTransaction *const tx = get_or_create_tx(thd);
  se_register_tx(ht, thd, tx);

  db::WriteBatch *const batch =
      dynamic_cast<db::WriteBatch *>(tx->get_blind_write_batch());

  DBUG_EXECUTE_IF("ddl_log_crash_before_se_delete_success", DBUG_SUICIDE(););

#ifndef NDEBUG
  static int delete_func_enter_counts = 0;
  DBUG_EXECUTE_IF("ddl_log_crash_delete_funcs", { delete_func_enter_counts++; };);
  if (delete_func_enter_counts > 1) {
    DBUG_SUICIDE();
  }
#endif

  std::string dbname_tablename;
  if (se_normalize_tablename(tablename, &dbname_tablename)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  bool from_dict = false;
  auto tbl = table_def ? ddl_manager.find(table_def, dbname_tablename, &from_dict)
                       : ddl_manager.find(thd, dbname_tablename, &from_dict);
  if (!tbl) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Attempt to delete a table that is not present in SE-SE data "
             "dictionary");
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  for (uint i = 0; i < tbl->m_key_count; i++) {
    GL_INDEX_ID gl_index_id = tbl->m_key_descr_arr[i]->get_gl_index_id();
    assert(gl_index_id.cf_id == gl_index_id.index_id);

    db::ColumnFamilyHandle *cf_handle;
    cf_handle = cf_manager.get_cf(gl_index_id.cf_id);
    if (cf_handle == nullptr) {
      assert(false);
      sql_print_error("error find subtable: %d", gl_index_id.cf_id);
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    ddl_log_manager.write_drop_subtable_log(
        batch, gl_index_id.cf_id, thd_thread_id(thd), true);
  }

  // Remove the table entry in from table cache
  ddl_manager.remove_cache(dbname_tablename);

  DBUG_EXECUTE_IF("ddl_log_crash_after_se_delete_success", DBUG_SUICIDE(););

  /* mark se modified that write atomic_ddl_log, make sure hton already
     registed. if not, mark_ddl_trx_read_write won't do anything.
   */
  mark_ddl_trx_read_write();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::rename_table(const char *const from,
                                 const char *const to,
                                 const dd::Table *from_table_def,
                                 dd::Table *to_table_def)
{
  DBUG_ENTER_FUNC();

  assert(from != nullptr);
  assert(to != nullptr);
  assert(from_table_def != nullptr);
  assert(to_table_def != nullptr);
  assert(from_table_def->se_private_id() == to_table_def->se_private_id());
  assert(from_table_def->se_private_data().raw_string() ==
              to_table_def->se_private_data().raw_string());

  std::string from_str;
  std::string to_str;
  std::string from_db;
  std::string to_db;

  if (se_normalize_tablename(from, &from_str) ||
      se_split_normalized_tablename(from_str, &from_db)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  if (se_normalize_tablename(to, &to_str) ||
      se_split_normalized_tablename(to_str, &to_db)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

#ifndef NDEBUG
  static int rename_func_enter_counts = 0;
  DBUG_EXECUTE_IF("ddl_log_crash_rename_funcs", { rename_func_enter_counts++; };);
  if (rename_func_enter_counts > 1) {
    DBUG_SUICIDE();
  }
#endif

  // If the user changed the database part of the name then validate that the
  // 'to' database exists.
  if (from_db != to_db && !se_database_exists(to_db)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  bool from_dict = false;
  auto from_tbl_def = ddl_manager.find(from_table_def, from_str, &from_dict);
  if (nullptr == from_tbl_def) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Attempt to rename a table that is not present in SE-SE data "
             "dictionary");
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  /* Check if any index has a per-index column family */
  {
    /*
      We can't check this directly, because
      1. the object that referred to the old table has been close()d.
      2. this ha_smartengine object has no connection to any table at all, it has
         been just created with (TABLE*)nullptr.

      So, we have to find the old table in the ddl_manager (it's there because
      it has been opened by this statement), and check is_auto_cf there.
    */

    SeTableDef *tdef = from_tbl_def.get();
    assert(tdef->m_key_descr_arr != nullptr);
    for (uint i = 0; i < tdef->m_key_count; i++) {
      if (tdef->m_key_descr_arr[i]->m_is_auto_cf) {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0),
                 "ALTER TABLE on table with per-index CF");
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }
    }
  }

  DBUG_EXECUTE_IF("ddl_log_crash_before_write_rename_log", DBUG_SUICIDE(););

  THD *const thd = table ? table->in_use : my_core::thd_get_current_thd();
  SeTransaction *const tx = get_or_create_tx(thd);
  se_register_tx(ht, thd, tx);
  db::WriteBatch *const batch = dynamic_cast<db::WriteBatch *>(tx->get_blind_write_batch());

  if (ddl_log_manager.write_rename_cache_log(batch, from_str, to_str,
                                             thd_thread_id(thd))) {
    sql_print_error("write rename_cache ddl_log error, table_name(%s), "
                    "dst_table_name(%s), thread_id(%d)",
                    from_str.c_str(), to_str.c_str(),
                    thd_thread_id(thd));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  DBUG_EXECUTE_IF("ddl_log_crash_after_write_rename_log", DBUG_SUICIDE(););

  dict_manager.lock();

  if (ddl_manager.rename_cache(from_tbl_def.get(), to_str)) {
    sql_print_error("rename dictionary error, table_name(%s), dst_table_name(%s), thread_id(%d)", from_str.c_str(), to_str.c_str(), thd_thread_id(thd));
    dict_manager.unlock();

    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  dict_manager.unlock();

  DBUG_EXECUTE_IF("ddl_log_crash_after_se_rename_success", DBUG_SUICIDE(););

  /* mark se modified that write atomic_ddl_log, make sure hton already
     registed. if not, mark_ddl_trx_read_write won't do anything.
   */
  mark_ddl_trx_read_write();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

void ha_smartengine::update_create_info(HA_CREATE_INFO *const create_info)
{
  DBUG_ENTER_FUNC();

  assert(create_info != nullptr);

  if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
    create_info->auto_increment_value = m_tbl_def->m_auto_incr_val;
  }

  DBUG_VOID_RETURN;
}

int ha_smartengine::open(const char *const name,
                         int mode,
                         uint test_if_locked,
                         const dd::Table *table_def)
{
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_OPEN);

  if (close()) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  m_table_handler = se_open_tables.get_table_handler(name);

  if (m_table_handler == nullptr) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  my_core::thr_lock_data_init(&m_table_handler->m_thr_lock, &m_db_lock,
                              nullptr);

  std::string fullname;
  if (se_normalize_tablename(name, &fullname)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  // For concurrent scenario,
  //
  // stage 1 check table cache
  // T1 set rdlock on ddl_manager
  // T2 set rdlock on ddl_manager
  // T1 failed to find from table cache
  // T1 unset rdlock on ddl_manager
  // T2 failed to find from table cache
  // T2 unset rdlock on ddl_manager
  //
  // stage 2 load from dictionary
  // T1 load table_def from dictionary referenced from ha_smartengine::m_tbl_def
  // T2 load table_def from dictionary referenced from ha_smartengine::m_tbl_def
  //
  // stage 3 put into cache
  // T1 set wrlock on ddl_manager
  // T1 put table_def into table cache,
  //    the reference of T1(ha_smartengine::m_tbl_def) will be increased to 2.
  // T1 unset wrlock on ddl_manager
  // T2 set wrlock on ddl_manager
  // T2 put table_def into table cache
  //    the reference of T1(ha_smartengine::m_tbl_def) will be decreased to 1.
  //    the reference of T2(ha_smartengine::m_tbl_def) will be increased to 2.
  // T2 unset wrlock on ddl_manager
  bool from_dict = false;
  if (nullptr != table_def)
    m_tbl_def = ddl_manager.find(table_def, fullname, &from_dict);
  else
    m_tbl_def = ddl_manager.find(ha_thd(), fullname, &from_dict);
  // SE_LOG(INFO, "dongsheng debug", "table_name", m_tbl_def->base_tablename(), "space_id", m_tbl_def->space_id);
  if (m_tbl_def == nullptr) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Attempt to open a table that is not present in SE-SE data "
             "dictionary");
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  } else if (from_dict) {
    ddl_manager.put(m_tbl_def);
  }

  m_lock_rows = SE_LOCK_NONE;

  m_key_descr_arr = m_tbl_def->m_key_descr_arr;

  /*
    Full table scan actually uses primary key
    (UPDATE needs to know this, otherwise it will go into infinite loop on
    queries like "UPDATE tbl SET pk=pk+100")
  */
  key_used_on_scan = table->s->primary_key;

  // close() above has already called free_key_buffers(). No need to do it here.
  int err = alloc_key_buffers(table, m_tbl_def.get());

  if (err) {
    DBUG_RETURN(err);
  }

  /*
    init_with_fields() is used to initialize table flags based on the field
    definitions in table->field[].
    It is called by open_binary_frm(), but that function calls the method for
    a temporary ha_smartengine object which is later destroyed.

    If we are here in ::open(), then init_with_fields() has not been called
    for this object. Call it ourselves, we want all member variables to be
    properly initialized.
  */
  init_with_fields();

  setup_field_converters();

  info(HA_STATUS_NO_LOCK | HA_STATUS_VARIABLE | HA_STATUS_CONST);

  /*
    The following load_XXX code calls row decode functions, and they do
    that without having done ::external_lock() or index_init()/rnd_init().
    (Note: this also means we're doing a read when there was no
    setup_field_converters() call)

    Initialize the necessary variables for them:
  */
  m_verify_row_debug_checksums = false;

  /* TODO: move the following to where TABLE_SHARE is opened: */
  if (table->found_next_number_field)
    load_auto_incr_value();

  if (has_hidden_pk(table) && load_hidden_pk_value()) {
    free_key_buffers();
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  /* Index block size in SE: used by MySQL in query optimization */
  stats.block_size = se_tbl_options.block_size;

  /* Determine at open whether we can use Read Free Replication or not */
  //set_use_read_free_rpl(THDVAR(ha_thd(), read_free_rpl_tables));

  /* Determine at open whether we should skip unique checks for this table */
  set_skip_unique_check_tables(".*"/*THDVAR(ha_thd(), skip_unique_check_tables)*/);

  if (nullptr != table_def) {
    // sometimes null is passed here, for example, handler::clone->handler::ha_open
    get_instant_ddl_info_if_needed(table, table_def, m_instant_ddl_info);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::close()
{
  DBUG_ENTER_FUNC();

  m_tbl_def.reset();
  m_pk_descr = nullptr;
  m_key_descr_arr = nullptr;

  free_key_buffers();

  my_free(m_encoder_arr);
  m_encoder_arr = nullptr;

  if (m_table_handler != nullptr) {
    se_open_tables.release_table_handler(m_table_handler);
    m_table_handler = nullptr;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/* The following function was copied from ha_blackhole::store_lock: */
THR_LOCK_DATA **ha_smartengine::store_lock(THD *const thd,
                                           THR_LOCK_DATA **to,
                                           enum thr_lock_type lock_type)
{
  DBUG_ENTER_FUNC();

  assert(thd != nullptr);
  assert(to != nullptr);

  bool in_lock_tables = my_core::thd_in_lock_tables(thd);

  /* First, make a decision about SE's internal locking */
  if (lock_type >= TL_WRITE_ALLOW_WRITE) {
    m_lock_rows = SE_LOCK_WRITE;
  } else if (lock_type == TL_READ_WITH_SHARED_LOCKS) {
    m_lock_rows = SE_LOCK_READ;
  } else {
    m_lock_rows = SE_LOCK_NONE;
    //if (THDVAR(thd, lock_scanned_rows)) {
    if (se_thd_lock_scanned_rows(thd)) {
      /*
        The following logic was copied directly from
        ha_innobase::store_lock_with_x_type() in
        storage/innobase/handler/ha_innodb.cc and causes SE to leave
        locks in place on rows that are in a table that is not being updated.
      */
      const uint sql_command = my_core::thd_sql_command(thd);
      if ((lock_type == TL_READ && in_lock_tables) ||
          (lock_type == TL_READ_HIGH_PRIORITY && in_lock_tables) ||
          can_hold_read_locks_on_select(thd, lock_type)) {
        ulong tx_isolation = my_core::thd_tx_isolation(thd);
        if (sql_command != SQLCOM_CHECKSUM &&
            ((my_core::thd_test_options(thd, OPTION_BIN_LOG) &&
              tx_isolation > ISO_READ_COMMITTED) ||
             tx_isolation == ISO_SERIALIZABLE ||
             (lock_type != TL_READ && lock_type != TL_READ_NO_INSERT) ||
             (sql_command != SQLCOM_INSERT_SELECT &&
              sql_command != SQLCOM_REPLACE_SELECT &&
              sql_command != SQLCOM_UPDATE && sql_command != SQLCOM_DELETE &&
              sql_command != SQLCOM_CREATE_TABLE))) {
          m_lock_rows = SE_LOCK_READ;
        }
      }
    }
  }

  /* Then, tell the SQL layer what kind of locking it should use: */
  if (lock_type != TL_IGNORE && m_db_lock.type == TL_UNLOCK) {
    /*
      Here is where we get into the guts of a row level lock.
      If TL_UNLOCK is set
      If we are not doing a LOCK TABLE or DISCARD/IMPORT
      TABLESPACE, then allow multiple writers
    */

    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE) &&
        !in_lock_tables && !my_core::thd_tablespace_op(thd)) {
      lock_type = TL_WRITE_ALLOW_WRITE;
    }

    /*
      In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
      MySQL would use the lock TL_READ_NO_INSERT on t2, and that
      would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
      to t2. Convert the lock to a normal read lock to allow
      concurrent inserts to t2.
    */

    if (lock_type == TL_READ_NO_INSERT && !in_lock_tables) {
      lock_type = TL_READ;
    }

    m_db_lock.type = lock_type;
  }

  *to++ = &m_db_lock;

  DBUG_RETURN(to);
}

int ha_smartengine::external_lock(THD *const thd, int lock_type)
{
  DBUG_ENTER_FUNC();

  assert(thd != nullptr);

  //for fast path
  read_thd_vars(thd);
  if (thd->lex->sql_command == SQLCOM_SELECT &&
      !thd->in_multi_stmt_transaction_mode())
  {
    if (lock_type == F_UNLCK)
    {
      SeTransaction *tx = get_or_create_tx(table->in_use);
      tx->release_snapshot();
    }
    DBUG_RETURN(0);
  }

  int res = 0;
  int binlog_format = my_core::thd_binlog_format(thd);
  //bool unsafe_for_binlog = THDVAR(ha_thd(), unsafe_for_binlog);
  bool unsafe_for_binlog = se_thd_unsafe_for_binlog(ha_thd());
  if (lock_type == F_WRLCK && !thd->rli_slave && !unsafe_for_binlog &&
      binlog_format != BINLOG_FORMAT_ROW &&
      binlog_format != BINLOG_FORMAT_UNSPEC &&
      my_core::thd_binlog_filter_ok(thd)) {
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Can't execute updates on master with binlog_format != ROW.", MYF(0));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  if (lock_type == F_UNLCK) {
    SeTransaction *const tx = get_or_create_tx(thd);
    tx->dec_table_in_use();
    int64_t table_in_use= tx->get_table_in_use();

    if (table_in_use == 0 &&
        !my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
      /*
        Do like InnoDB: when we get here, it's time to commit a
        single-statement transaction.

        If the statement involved multiple tables, this code will be executed
        for each of them, but that's ok because non-first tx->commit() calls
        will be no-ops.
      */
      res = tx->commit_or_rollback();
    }
    DEBUG_SYNC_C("ha_se_end_statement");
  } else {
    if (my_core::thd_tx_isolation(thd) < ISO_READ_COMMITTED ||
        my_core::thd_tx_isolation(thd) > ISO_REPEATABLE_READ) {
      my_printf_error(
          ER_UNKNOWN_ERROR, "SE only supports READ COMMITTED and "
                            "REPEATABLE READ isolation levels. "
                            "Please change from current isolation level %s",
          MYF(0), tx_isolation_names[my_core::thd_tx_isolation(thd)]);
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
    /*
      It's nice to do the following on start of every statement. The problem
      is, handler->start_stmt() is not called for INSERTs.
      So, we put this code here.
    */
    SeTransaction *const tx = get_or_create_tx(thd);
    tx->add_table_in_use();

    m_update_scope_is_valid = false;

    if (skip_unique_check()) {
      if ((thd->lex->sql_command == SQLCOM_INSERT ||
           thd->lex->sql_command == SQLCOM_LOAD ||
           thd->lex->sql_command == SQLCOM_REPLACE) &&
          (thd->lex->duplicates == DUP_REPLACE ||
           thd->lex->duplicates == DUP_UPDATE)) {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "When unique checking is disabled in SE, INSERT,"
                        "UPDATE,LOAD statements with clauses that update or "
                        "replace the key (i.e. "
                        "INSERT ON DUPLICATE KEY UPDATE, REPLACE) are not "
                        "allowed. Query: %s",
                        MYF(0), thd->query().str);
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }
    }

    if (lock_type == F_WRLCK) {
      if (tx->is_tx_read_only()) {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "Can't execute updates when you started a transaction "
                        "with START TRANSACTION WITH CONSISTENT [se] "
                        "SNAPSHOT.",
                        MYF(0));
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }

      /*
        SQL layer signals us to take a write lock. It does so when starting DML
        statement. We should put locks on the rows we're reading.

        Note: sometimes, external_lock() can be called without a prior
        ::store_lock call.  That's why we need to set lock_* members here, too.
      */
      m_lock_rows = SE_LOCK_WRITE;

      if (thd->lex->sql_command == SQLCOM_CREATE_INDEX ||
          thd->lex->sql_command == SQLCOM_DROP_INDEX ||
          thd->lex->sql_command == SQLCOM_ALTER_TABLE) {
        tx->m_ddl_transaction = true;
      }
    }
    se_register_tx(se_hton, thd, tx);
  }

  DBUG_RETURN(res);
}

/**
  @note
  A quote from ha_innobase::start_stmt():
  <quote>
  MySQL calls this function at the start of each SQL statement inside LOCK
  TABLES. Inside LOCK TABLES the ::external_lock method does not work to
  mark SQL statement borders.
  </quote>
*/
int ha_smartengine::start_stmt(THD *const thd, thr_lock_type lock_type)
{
  DBUG_ENTER_FUNC();

  assert(thd != nullptr);

  SeTransaction *const tx = get_or_create_tx(thd);
  read_thd_vars(thd);
  se_register_tx(ht, thd, tx);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

void ha_smartengine::unlock_row()
{
  DBUG_ENTER_FUNC();

  if (m_lock_rows != SE_LOCK_NONE) {
    SeTransaction *const tx = get_or_create_tx(table->in_use);
    tx->release_lock(m_pk_descr->get_cf(),
                     std::string(m_last_rowkey.ptr(), m_last_rowkey.length()));
  }

  DBUG_VOID_RETURN;
}

int ha_smartengine::write_row(uchar *const buf)
{
  DBUG_ENTER_FUNC();

  assert(buf != nullptr);
  assert(buf == table->record[0]);
  assert(m_lock_rows == SE_LOCK_WRITE);

  ha_statistic_increment(&SSV::ha_write_count);
  /*
    Note: "buf == table->record[0]" is copied from innodb. I am not aware of
    any use cases where this condition is not true.
  */
  if (table->next_number_field && buf == table->record[0]) {
    int err;
    if ((err = update_auto_increment())) {
      DBUG_RETURN(err);
    }
  }

  const int rv = update_write_row(nullptr, buf, skip_unique_check());

  if (rv == 0) {
    //stats.rows_inserted++;
    update_row_stats(ROWS_INSERTED);
  }

  DBUG_RETURN(rv);
}

int ha_smartengine::update_row(const uchar *const old_data, uchar *const new_data)
{
  DBUG_ENTER_FUNC();

  assert(old_data != nullptr);
  assert(new_data != nullptr);
  assert(m_lock_rows == SE_LOCK_WRITE);
  /*
    old_data points to record we're updating. It is the same as the record
    we've just read (for multi-table UPDATE, too, because SQL layer will make
    an rnd_pos() call to re-read the record before calling update_row())
  */
  assert(new_data == table->record[0]);

  ha_statistic_increment(&SSV::ha_update_count);
  const int rv = update_write_row(old_data, new_data, false);

  if (rv == 0) {
    //stats.rows_updated++;
    update_row_stats(ROWS_UPDATED);
  }

  DBUG_RETURN(rv);
}

/*
  Delete the row we've last read. The row is also passed as parameter.

  @detail
    The caller guarantees table buf points to the row that was just read.
    The row is either table->record[0] or table->record[1].
    (Check out InnoDB: row_update_for_mysql() has "UT_NOT_USED(mysql_rec)"
*/
int ha_smartengine::delete_row(const uchar *const buf)
{
  DBUG_ENTER_FUNC();

  assert(buf != nullptr);

  ha_statistic_increment(&SSV::ha_delete_count);
  set_last_rowkey(buf);

  SeTransaction *const tx = get_or_create_tx(table->in_use);

  int ret = HA_EXIT_SUCCESS;
  if ((ret = delete_indexes(buf))) {
    __HANDLER_LOG(ERROR, "delete failed, errcode is %d", ret);
    DBUG_RETURN(ret);
  } else if (m_tbl_def->m_inplace_new_tdef != nullptr) {
    assert(!m_tbl_def->m_inplace_new_keys.empty());
    struct update_row_info del_row_info;
    del_row_info.tx = tx;
    del_row_info.old_data = buf;

    //delete the record for rebuild new table
    if ((ret = delete_row_new_table(del_row_info))) {
      __HANDLER_LOG(WARN, "SEDDL: failed to delete record for new table, errcode is %d, table_name:%s",
                    ret, table->s->table_name.str);
      DBUG_RETURN(ret);
    }
  }

  if (do_bulk_commit(tx)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  //stats.rows_deleted++;
  update_row_stats(ROWS_DELETED);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::truncate(dd::Table *table_def)
{
  /**SmartEngine supports HTON_CAN_RECREATE flag, and the truncate
   * command use create function instead of truncate function.*/
  se_assert(false);
  DBUG_ENTER_FUNC();
  
  assert(m_tbl_def != nullptr);

  m_tbl_def->m_auto_incr_val = 1;
  m_tbl_def->m_hidden_pk_val = 1;

  //remove_rows(m_tbl_def.get());

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**The name that will be used for display purposes.*/
const char *ha_smartengine::table_type() const
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(se_hton_name);
}

bool ha_smartengine::get_error_message(const int error, String *const buf)
{
  DBUG_ENTER_FUNC();

  static_assert(HA_ERR_SE_LAST > HA_ERR_FIRST,
                "HA_ERR_SE_LAST > HA_ERR_FIRST");
  static_assert(HA_ERR_SE_LAST > HA_ERR_LAST,
                "HA_ERR_SE_LAST > HA_ERR_LAST");

  assert(buf != nullptr);

  SeTransaction *const tx = get_tx_from_thd(ha_thd());
  bool temp_error = false;

  switch (error) {
  case HA_ERR_SE_PK_REQUIRED:
    buf->append("Table must have a PRIMARY KEY.");
    break;
  case HA_ERR_SE_UNIQUE_NOT_SUPPORTED:
    buf->append("Unique indexes are not supported.");
    break;
  case HA_ERR_SE_TOO_MANY_LOCKS:
    buf->append("Number of locks held reached @@se_max_row_locks.");
    break;
  case HA_ERR_LOCK_WAIT_TIMEOUT:
    assert(tx != nullptr);
    buf->append(tx->m_detailed_error);
    temp_error = true;
    break;
  case HA_ERR_SE_TABLE_DATA_DIRECTORY_NOT_SUPPORTED:
    buf->append("Specifying DATA DIRECTORY for an individual table is not "
                "supported.");
    break;
  case HA_ERR_SE_TABLE_INDEX_DIRECTORY_NOT_SUPPORTED:
    buf->append("Specifying INDEX DIRECTORY for an individual table is not "
                "supported.");
    break;
  case HA_ERR_SE_OUT_OF_SORTMEMORY:
    buf->append("Sort memory is too small.");
    break;
  default:
    // We can be called with the values which are < HA_ERR_FIRST because most
    // MySQL internal functions will just return HA_EXIT_FAILURE in case of
    // an error.
    break;
  }

  DBUG_RETURN(temp_error);
}

/**This is a list of flags that indicate what functionality the storage engine
implements. The current table flags are documented in handler.h*/
handler::Table_flags ha_smartengine::table_flags() const
{
  DBUG_ENTER_FUNC();

  /*
    HA_BINLOG_STMT_CAPABLE
      We are saying that this engine is just statement capable to have
      an engine that can only handle statement-based logging. This is
      used in testing.
    HA_REC_NOT_IN_SEQ
      If we don't set it, filesort crashes, because it assumes rowids are
      1..8 byte numbers
  */
  DBUG_RETURN(HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
              HA_CAN_INDEX_BLOBS |
              (m_pk_can_be_decoded ? HA_PRIMARY_KEY_IN_READ_INDEX : 0) |
              HA_PRIMARY_KEY_REQUIRED_FOR_POSITION | HA_NULL_IN_KEY |
              HA_PARTIAL_COLUMN_READ | HA_ATTACHABLE_TRX_COMPATIBLE);
}

/**
  @note
    The problem with this function is that SQL layer calls it, when
     - the table has not been yet opened (no ::open() call done)
     - this->table_share already exists, but it is in the process of being
       filled, so some of fields are still NULL.
     - In particular, table_share->key_info[inx].key_part[] is filled only up
       to part #part. Subsequent key parts are not yet filled.

    To complicate things further, SQL layer will call index_flags() with
    all_parts=TRUE. Essentially, we're asked to provide flags for reading
    keyparts whose datatype is not yet known.

    We walk around this problem by using check_keyread_allowed(), which uses
    table_share object and is careful not to step on unitialized data.

    When we get a call with all_parts=TRUE, we try to analyze all parts but
    ignore those that have key_part->field==nullptr (these are not initialized
    yet).
*/

ulong ha_smartengine::index_flags(uint inx, uint part, bool all_parts) const
{
  DBUG_ENTER_FUNC();

  ulong base_flags = HA_READ_NEXT | // doesn't seem to be used
                     HA_READ_ORDER | HA_READ_RANGE | HA_READ_PREV;

  if (check_keyread_allowed(inx, part, all_parts))
    base_flags |= HA_KEYREAD_ONLY;

  if (inx == table_share->primary_key) {
    /*
      Index-only reads on primary key are the same as table scan for us. Still,
      we need to explicitly "allow" them, otherwise SQL layer will miss some
      plans.
    */
    base_flags |= HA_KEYREAD_ONLY;
  } else {
    /*
      We can Index Condition Pushdown any key except the primary. With primary
      key, we get (pk, record) pair immediately, there is no place to put the
      ICP check.
    */
    base_flags |= HA_DO_INDEX_COND_PUSHDOWN;
  }

  DBUG_RETURN(base_flags);
}

uint ha_smartengine::max_supported_record_length() const
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(HA_MAX_REC_LENGTH);
}

uint ha_smartengine::max_supported_keys() const
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(MAX_INDEXES);
}

uint ha_smartengine::max_supported_key_parts() const
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(MAX_REF_PARTS);
}

uint ha_smartengine::max_supported_key_part_length(HA_CREATE_INFO *create_info) const
{
  DBUG_ENTER_FUNC();

  // Use 3072 from ha_innobase::max_supported_key_part_length
  DBUG_RETURN(MAX_KEY_LENGTH);
}

uint ha_smartengine::max_supported_key_length() const
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(16 * 1024); /* just to return something*/
}

bool ha_smartengine::primary_key_is_clustered() const
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(true);
}

void ha_smartengine::get_auto_increment(ulonglong off, ulonglong inc,
                                        ulonglong nb_desired_values,
                                        ulonglong *const first_value,
                                        ulonglong *const nb_reserved_values)
{
  /*
    MySQL has a somewhat complicated way of handling the auto-increment value.
    The first time get_auto_increment is called for a statement,
    nb_desired_values is the estimate for how many values will be needed.  The
    engine can then reserve some values, and those will be automatically used
    by MySQL, until a hard-coded value shows up in the insert statement, after
    which MySQL again calls this function to reset its starting value.
   *
    For simplicity we will just ignore nb_desired_values - we aren't going to
    reserve any extra values for a multi-insert statement.  Each row will
    simply acquire the next value as needed and we will always tell MySQL that
    we only reserved 1 value.  Since we are using an atomic value for
    m_auto_incr_val this should be safe - if we had to grab a mutex, doing
    an actual reserve of some values might be a better solution.
   */
  DEBUG_SYNC(ha_thd(), "se.autoinc_vars");

  if (off > inc) {
    off = 1;
  }

  Field *field;
  ulonglong new_val, max_val;
  field = table->key_info[table->s->next_number_index].key_part[0].field;
  max_val = se_get_int_col_max_value(field);

  // Local variable reference to simplify code below
  std::atomic<ulonglong> &auto_incr = m_tbl_def->m_auto_incr_val;

  if (inc == 1) {
    assert(off == 1);
    // Optimization for the standard case where we are always simply
    // incrementing from the last position

    new_val = auto_incr;
    while (new_val != std::numeric_limits<ulonglong>::max()) {
      if (auto_incr.compare_exchange_weak(new_val, std::min(new_val + 1, max_val))) {
        break;
      }
    }
  } else {
    // The next value can be more complicated if either 'inc' or 'off' is not 1
    ulonglong last_val = auto_incr;

    if (last_val > max_val) {
      new_val = std::numeric_limits<ulonglong>::max();
    } else {
      // Loop until we can correctly update the atomic value
      do {
        assert(last_val > 0);
        // Calculate the next value in the auto increment series: offset
        // + N * increment where N is 0, 1, 2, ...
        //
        // For further information please visit:
        // http://dev.mysql.com/doc/refman/5.7/en/replication-options-master.html
        //
        // The following is confusing so here is an explanation:
        // To get the next number in the sequence above you subtract out the
        // offset, calculate the next sequence (N * increment) and then add the
        // offset back in.
        //
        // The additions are rearranged to avoid overflow.  The following is
        // equivalent to (last_val - 1 + inc - off) / inc. This uses the fact
        // that (a+b)/c = a/c + b/c + (a%c + b%c)/c. To show why:
        //
        // (a+b)/c
        // = (a - a%c + a%c + b - b%c + b%c) / c
        // = (a - a%c) / c + (b - b%c) / c + (a%c + b%c) / c
        // = a/c + b/c + (a%c + b%c) / c
        //
        // Now, substitute a = last_val - 1, b = inc - off, c = inc to get the
        // following statement.
        ulonglong n =
            (last_val - 1) / inc + ((last_val - 1) % inc + inc - off) / inc;

        // Check if n * inc + off will overflow. This can only happen if we have
        // an UNSIGNED BIGINT field.
        if (n > (std::numeric_limits<ulonglong>::max() - off) / inc) {
          assert(max_val == std::numeric_limits<ulonglong>::max());
          // The 'last_val' value is already equal to or larger than the largest
          // value in the sequence.  Continuing would wrap around (technically
          // the behavior would be undefined).  What should we do?
          // We could:
          //   1) set the new value to the last possible number in our sequence
          //      as described above.  The problem with this is that this
          //      number could be smaller than a value in an existing row.
          //   2) set the new value to the largest possible number.  This number
          //      may not be in our sequence, but it is guaranteed to be equal
          //      to or larger than any other value already inserted.
          //
          //  For now I'm going to take option 2.
          //
          //  Returning ULLONG_MAX from get_auto_increment will cause the SQL
          //  layer to fail with ER_AUTOINC_READ_FAILED. This means that due to
          //  the SE API for get_auto_increment, inserts will fail with
          //  ER_AUTOINC_READ_FAILED if the column is UNSIGNED BIGINT, but
          //  inserts will fail with ER_DUP_ENTRY for other types (or no failure
          //  if the column is in a non-unique SK).
          new_val = std::numeric_limits<ulonglong>::max();
          auto_incr = new_val;  // Store the largest value into auto_incr
          break;
        }

        new_val = n * inc + off;

        // Attempt to store the new value (plus 1 since m_auto_incr_val contains
        // the next available value) into the atomic value.  If the current
        // value no longer matches what we have in 'last_val' this will fail and
        // we will repeat the loop (`last_val` will automatically get updated
        // with the current value).
        //
        // See above explanation for inc == 1 for why we use std::min.
      } while (!auto_incr.compare_exchange_weak(
          last_val, std::min(new_val + 1, max_val)));
    }
  }

  *first_value = new_val;
  *nb_reserved_values = 1;
}

/**
  check_if_incompatible_data() called if ALTER TABLE can't detect otherwise
  if new and old definition are compatible

  @details If there are no other explicit signs like changed number of
  fields this function will be called by compare_tables()
  (sql/sql_tables.cc) to decide should we rewrite whole table or only .frm
  file.
*/
bool ha_smartengine::check_if_incompatible_data(HA_CREATE_INFO *const info, uint table_changes)
{
  DBUG_ENTER_FUNC();

  assert(info != nullptr);

  // this function is needed only for online alter-table
  DBUG_RETURN(COMPATIBLE_DATA_NO);
}

bool ha_smartengine::same_table(const ha_smartengine &other) const
{
  return m_tbl_def->base_tablename() == other.m_tbl_def->base_tablename();
}

int ha_smartengine::finalize_bulk_load()
{
  int rc = 0;

  /* Skip if there are no possible ongoing bulk loads */
  if (m_sst_info == nullptr && m_bulk_load_tx == nullptr) {
    return rc;
  }

  SE_MUTEX_LOCK_CHECK(m_bulk_load_mutex);

  /*
    We need this check because it's possible that m_sst_info has been
    flushed and cleared by another thread by the time the mutex has been
    acquired.
  */
  if (m_sst_info != nullptr) {
    rc = m_sst_info->commit();
    if (rc != 0) {
      /*
        Log the error immediately here in case the server crashes before
        mysql prints via my_printf_error.
      */
      sql_print_error("Failed to commit bulk loaded sst file to the "
                      "data store (%s)",
                      m_sst_info->error_message().c_str());

      my_printf_error(ER_UNKNOWN_ERROR,
                      "Failed to commit bulk loaded sst file to the "
                      "data store (%s)",
                      MYF(0), m_sst_info->error_message().c_str());
      rc = HA_ERR_INTERNAL_ERROR;
    }

    m_sst_info = nullptr;
    mtables_ = nullptr;
    m_change_info.reset();
    m_bulk_load_tx->end_bulk_load(this);
    m_bulk_load_tx = nullptr;
  }

  SE_MUTEX_UNLOCK_CHECK(m_bulk_load_mutex);

  return rc;
}

/*
  Create structures needed for storing data in smartengine. This is called when the
  table is created. The structures will be shared by all TABLE* objects.

  @param
    table_arg        Table with definition
    db_table         "dbname.tablename"
    len              strlen of the above
    tbl_def_arg      tbl_def whose key_descr is being created/populated
    old_tbl_def_arg  tbl_def from which keys are being copied over from
                     (for use during inplace alter)

  @return
    0      - Ok
    other  - error, either given table ddl is not supported by se or OOM.
*/
int ha_smartengine::create_key_defs(const TABLE *new_table,
                                    const TABLE *old_table,
                                    const dd::Table *dd_table,
                                    SeTableDef *new_table_def,
                                    const SeTableDef *old_table_def,
                                    LEX_CSTRING engine_attribute,
                                    const bool need_rebuild)
{
  int ret = common::Status::kOk;
  // These need to be one greater than MAX_INDEXES since the user can create
  // MAX_INDEXES secondary keys and no primary key which would cause use to
  // generate a hidden one.
  std::array<uint32_t, MAX_INDEXES + 1> index_ids;

  if (IS_NULL(new_table) || IS_NULL(dd_table) || IS_NULL(new_table_def)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(new_table), KP(dd_table), KP(new_table_def));
  } else if (is_smartengine_system_database(new_table_def->base_dbname().c_str())) {
    ret = common::Status::kNotSupported;
    SE_LOG(WARN, "Not allowed to create user tables under the smartengine system database",
        K(ret), "database_name", new_table_def->base_dbname());
  } else {
    // The logic below involves multiple traversals of the index structure.The reason for implementing
    // it this way is that this function is only called in ddl statements, and typically, the number of
    // indexes is not large.Therefore, it won't become a performance bottleneck.The benefit is that the
    // implementation logic of the entire function becomes clearer.

    // For alter and create statements, it is necessary to check whether the collation is supported.
    // TODO(Zhao Dongsheng) : For mtr case collation_exception.
    char tablename_sys[NAME_LEN + 1] = {0};
    my_core::filename_to_tablename(new_table_def->base_tablename().c_str(), tablename_sys, sizeof(tablename_sys));
    for (uint32_t i = 0; SUCCED(ret) && i < new_table_def->m_key_count; ++i) {
      if (se_strict_collation_check && !is_hidden_pk(i, new_table, new_table_def)) {
        for (uint part = 0; SUCCED(ret) && part < new_table->key_info[i].actual_key_parts; ++part) {
          if (!se_is_index_collation_supported(new_table->key_info[i].key_part[part].field) &&
              !se_collation_exceptions->matches(tablename_sys)) {
            ret = common::Status::kNotSupported;
            SE_LOG(WARN, "Can't use collation which is not supported in smartengine",
                K(ret), "database_name", new_table_def->base_dbname(), "table_name", new_table_def->base_tablename(),
                K(i), "field_name", new_table->key_info[i].key_part[part].field->field_name);
            my_printf_error(ER_UNKNOWN_ERROR, "Unsupported collation on string indexed column"
                " %s. Consider change to other collation (%s).",
                MYF(0), new_table->key_info[i].key_part[part].field->field_name,
                gen_se_supported_collation_string().c_str());
          }
        }
      }
    }

    // Setup index ids, try to reuse old index id in some alter statements, like add index.
    if (SUCCED(ret)) {
      const char *key_name = nullptr;
      std::unordered_map<std::string, uint> old_key_pos;
      if (IS_NOTNULL(old_table_def) && !need_rebuild) {
        old_key_pos = get_old_key_positions(new_table, new_table_def, old_table, old_table_def);
      }
      for (uint32_t i = 0; SUCCED(ret) && i < new_table_def->m_key_count; ++i) {
        if (IS_NULL(key_name = get_key_name(i, new_table, new_table_def))) {
          ret = common::Status::kErrorUnexpected;
          SE_LOG(WARN, "the key name must not be nullptr", K(ret), K(i), "table_name", new_table_def->base_tablename());
        } else {
          auto iter = old_key_pos.find(key_name);
          if (old_key_pos.end() != iter) {
            index_ids[i] = old_table_def->m_key_descr_arr[iter->second]->get_gl_index_id().index_id;
            SE_LOG(INFO, "dongsheng debug, use old index id", K(key_name), K(i), "index_id", index_ids[i]);
          } else {
            index_ids[i] = ddl_manager.get_and_update_next_number(&dict_manager);
            SE_LOG(INFO, "dongsheng debug, use new index id", K(key_name), K(i), "index_id", index_ids[i]);
          }
        }
      }
    }

    // Create SeKeyDefs.
    if (SUCCED(ret)) {
      if (IS_NULL(old_table_def)) {
        // The old_table_def does not exist, it means a new table is being created.
        for (uint32_t i = 0; SUCCED(ret) && i < new_table_def->m_key_count; ++i) {
          if (create_key_def(new_table, i, new_table_def, &m_key_descr_arr[i], /*cfs[i]*/ index_ids[i])) {
            ret = common::Status::kErrorUnexpected;
            SE_LOG(WARN, "fail to create key def", K(ret), K(i));
          } else {
            m_key_descr_arr[i]->setup(new_table, new_table_def);
            SE_LOG(INFO, "create new key def", K(i), "index_id", index_ids[i]);
          }
        }
      } else if (need_rebuild) {
        // For copy-online ddl
        for (uint32_t i = 0; SUCCED(ret) && i < new_table_def->m_key_count; ++i) {
          if (create_key_def(new_table, i, new_table_def, &new_table_def->m_key_descr_arr[i], /*cfs[i],*/ index_ids[i])) {
            ret = common::Status::kErrorUnexpected;
            SE_LOG(WARN, "fail to create key def", K(ret), K(i));
          } else {
            assert(IS_NOTNULL(new_table_def->m_key_descr_arr[i]));
            new_table_def->m_key_descr_arr[i]->setup(new_table, new_table_def);
            SE_LOG(INFO, "create new key def", K(i), "index_id", index_ids[i]);
          }
        }
      } else {
        // The old_table_def exists and the need_rebuild is false, it means creating a new table def
        // as part of in-place alter table.Copy over existing keys from the old_table_def and create
        // new keys if necessary.
        if (create_inplace_key_defs(new_table, new_table_def, old_table, old_table_def, /*cfs,*/ index_ids)) {
          ret = common::Status::kErrorUnexpected;
          SE_LOG(WARN, "fail to create key defs for inplace ddl", K(ret));
        }
      }
    }

    // Create subtables.
    if (SUCCED(ret)) {
      db::ColumnFamilyHandle *handle = nullptr;
      schema::TableSchema table_schema;
      bool create_table_space = true;
      int64_t table_space_id = 0;
      
      if (IS_NOTNULL(old_table_def)) {
        create_table_space = false;
        table_space_id = old_table_def->space_id;
        //se_assert(0 != table_space_id);
      }

      if (FAILED(build_table_schema(new_table,
                                    dd_table,
                                    new_table_def,
                                    engine_attribute,
                                    table_schema))) {
        SE_LOG(WARN, "fail to build table schema", K(ret));
      } else {
        for (uint32_t i = 0; SUCCED(ret) && i < new_table_def->m_key_count; ++i) {
          THD *thd = table ? table->in_use : my_core::thd_get_current_thd();
          SeTransaction *trans = get_or_create_tx(thd);   
          se_register_tx(ht, thd, trans);
          db::WriteBatch *batch = dynamic_cast<db::WriteBatch *>(trans->get_blind_write_batch());
          table_schema.set_index_id(index_ids[i]);

          if (IS_NULL(handle = cf_manager.get_or_create_subtable(se_db,
                                                                 batch,
                                                                 thd_thread_id(thd),
                                                                 //index_ids[i],
                                                                 se_default_cf_options,
                                                                 table_schema,
                                                                 create_table_space,
                                                                 table_space_id))) {
            ret = common::Status::kErrorUnexpected;
            SE_LOG(WARN, "fail to get or create subtable");
          } else {
            if (create_table_space) {
              new_table_def->space_id = table_space_id;
            }

            // Associate the subtable with the corresponding SeKeyDef, some key
            // defs may be copied from old table def in create_inplace_key_defs.
            if (IS_NULL(new_table_def->m_key_descr_arr[i]->get_cf())) {
              se_assert(new_table_def->m_key_descr_arr[i]->get_index_number() == handle->GetID());
              new_table_def->m_key_descr_arr[i]->set_cf(handle);
#ifndef NDEBUG
              SE_LOG(INFO, "success to create new index", "full_tablename", new_table_def->full_tablename(),
                  "base_dbname", new_table_def->base_dbname(), "base_tablename", new_table_def->base_tablename(),
                  "index_name", get_key_name(i, new_table, new_table_def), "table_id", new_table_def->get_table_id(),
                  K(table_schema));
#endif
            }
          }
        }
      }
    }
  }

  return ret;
}

/*
  Create key definition needed for storing data in se.
  This can be called either during CREATE table or doing ADD index operations.

  @param in
    table_arg     Table with definition
    i             Position of index being created inside table_arg->key_info
    tbl_def_arg   Table def structure being populated
    cf_info       Struct which contains column family information

  @param out
    new_key_def  Newly created index definition.

  @return
    0      - Ok
    other  - error, either given table ddl is not supported by se or OOM.
*/
int ha_smartengine::create_key_def(const TABLE *const table_arg,
                                   const uint &i,
                                   const SeTableDef *const tbl_def_arg,
                                   std::shared_ptr<SeKeyDef> *const new_key_def,
                                   const uint32_t index_id) const
{
  DBUG_ENTER_FUNC();

  assert(new_key_def != nullptr);
  assert(*new_key_def == nullptr);

  const uint16_t index_dict_version = SeKeyDef::INDEX_INFO_VERSION_LATEST;
  uchar index_type;
  uint16_t kv_version;

  if (is_hidden_pk(i, table_arg, tbl_def_arg)) {
    index_type = SeKeyDef::INDEX_TYPE_HIDDEN_PRIMARY;
    kv_version = SeKeyDef::PRIMARY_FORMAT_VERSION_LATEST;
  } else if (i == table_arg->s->primary_key) {
    index_type = SeKeyDef::INDEX_TYPE_PRIMARY;
    uint16 pk_latest_version = SeKeyDef::PRIMARY_FORMAT_VERSION_LATEST;
    kv_version = pk_latest_version;
  } else {
    index_type = SeKeyDef::INDEX_TYPE_SECONDARY;
    uint16 sk_latest_version = SeKeyDef::SECONDARY_FORMAT_VERSION_LATEST;
    kv_version = sk_latest_version;
  }

  const char *const key_name = get_key_name(i, table_arg, tbl_def_arg);
  *new_key_def = std::make_shared<SeKeyDef>(index_id,
                                            i,
                                            index_dict_version,
                                            index_type,
                                            kv_version,
                                            key_name,
                                            SeIndexStats());

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::update_write_row(const uchar *const old_data,
                                     const uchar *const new_data,
                                     const bool skip_unique_check)
{
  DBUG_ENTER_FUNC();

  struct update_row_info row_info;
  int rc = HA_EXIT_SUCCESS;

  row_info.old_data = old_data;
  row_info.new_data = new_data;
  row_info.skip_unique_check = skip_unique_check;
  row_info.new_pk_unpack_info = nullptr;

  set_last_rowkey(old_data);

  row_info.tx = get_or_create_tx(table->in_use);

  if (old_data != nullptr) {
    row_info.old_pk_slice =
        common::Slice(m_last_rowkey.ptr(), m_last_rowkey.length());

    /* Determine which indexes need updating. */
    if ((rc = calc_updated_indexes())) {
      __HANDLER_LOG(ERROR, "calculate update indexes error, code is %d", rc);
      DBUG_RETURN(rc);
    }
  }

  /*
    Get the new row key into row_info.new_pk_slice
   */
  rc = get_pk_for_update(&row_info);
  if (rc) {
    DBUG_RETURN(rc);
  }

  bool pk_changed = false;
  if (!skip_unique_check) {
    /*
      Check to see if we are going to have failures because of unique
      keys.  Also lock the appropriate key values.
    */
    rc = check_uniqueness_and_lock(row_info, &pk_changed);
    if (rc) {
      DBUG_RETURN(rc);
    }
  }

  DEBUG_SYNC(ha_thd(), "se.update_write_row_after_unique_check");

  /*
    At this point, all locks have been obtained, and all checks for duplicate
    keys have been performed. No further errors can be allowed to occur from
    here because updates to the transaction will be made and those updates
    cannot be easily removed without rolling back the entire transaction.
  */
  rc = update_indexes(row_info, pk_changed);
  if (rc) {
    DBUG_RETURN(rc);
  }

  assert(m_tbl_def->m_added_key.empty() || m_tbl_def->m_inplace_new_keys.empty());

  if (!m_tbl_def->m_inplace_new_keys.empty()) {
    TABLE *atab = m_tbl_def->m_inplace_new_keys.begin()->second.altered_table;
    if ((rc = get_new_pk_for_update(&row_info, atab))) {
      __HANDLER_LOG(ERROR, "SEDDL: get_new_pk_for_update faield with %d, table_name: %s",
                    rc, table->s->table_name.str);
      DBUG_RETURN(rc);
    }
  }
  pk_changed = false;
  if (!skip_unique_check &&
      (rc = check_uniqueness_and_lock_rebuild(row_info, &pk_changed))) {
    DBUG_RETURN(rc);
  }
  rc = update_new_table(row_info, pk_changed);
  if (rc) {
    DBUG_RETURN(rc);
  }

  if (do_bulk_commit(row_info.tx)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::update_pk(const SeKeyDef &kd,
                              const struct update_row_info &row_info,
                              const bool &pk_changed)
{
  int rc = HA_EXIT_SUCCESS;

  const uint key_id = kd.get_keyno();
  const bool hidden_pk = is_hidden_pk(key_id, table, m_tbl_def.get());

  if (!hidden_pk && pk_changed) {
    /*
      The old key needs to be deleted.
    */
    const common::Status s = delete_or_singledelete(
        key_id, row_info.tx, kd.get_cf(), row_info.old_pk_slice);
    if (!s.ok()) {
      __HANDLER_LOG(WARN, "DML: failed to delete old record(%s) for updating pk(%u) with error %s, table_name: %s",
                    row_info.old_pk_slice.ToString(true).c_str(),
                    kd.get_index_number(), s.ToString().c_str(),
                    table->s->table_name.str);
      return row_info.tx->set_status_error(table->in_use, s, kd, m_tbl_def.get());
    }
  }

  if (table->found_next_number_field) {
    update_auto_incr_val();
  }

  common::Slice value_slice;
  if ((rc = convert_record_to_storage_format(
           row_info.new_pk_slice, row_info.new_pk_unpack_info, &value_slice))) {
    __HANDLER_LOG(ERROR, "convert record to se format error, code is %d", rc);
    return rc;
  }

  const auto cf = kd.get_cf();
  if (row_info.skip_unique_check) {
    /*
      It is responsibility of the user to make sure that the data being
      inserted doesn't violate any unique keys.
    */
    row_info.tx->get_blind_write_batch()->Put(cf, row_info.new_pk_slice,
                                              value_slice);
  } else if (row_info.tx->m_ddl_transaction) {
    /*
      DDL statement must check for unique key conflicts. For example:
      ALTER TABLE tbl DROP PRIMARY KEY, ADD PRIMARY KEY(non_unique_column)
    */
    row_info.tx->get_indexed_write_batch()->Put(cf, row_info.new_pk_slice,
                                                value_slice);
  } else {
    const auto s = row_info.tx->put(cf, row_info.new_pk_slice, value_slice);
    if (!s.ok()) {
      if (s.IsBusy()) {
        __HANDLER_LOG(WARN, "DML: duplicate entry is found for key(%s) on pk:%d, table_name:%s",
                      row_info.new_pk_slice.ToString(true).c_str(), kd.get_index_number(),
                      table->s->table_name.str);
        errkey = table->s->primary_key;
        m_dupp_errkey = errkey;
        rc = HA_ERR_FOUND_DUPP_KEY;
      } else {
        __HANDLER_LOG(WARN, "DML: failed to put record(%s:%s) for updating pk(%u) with error %s, table_name: %s",
                      row_info.new_pk_slice.ToString(true).c_str(), value_slice.ToString(true).c_str(),
                      kd.get_index_number(), s.ToString().c_str(), table->s->table_name.str);
        rc = row_info.tx->set_status_error(table->in_use, s, *m_pk_descr,
                                           m_tbl_def.get());
      }
    }
  }

  return rc;
}

int ha_smartengine::update_sk(const TABLE *const table_arg,
                              const SeKeyDef &kd,
                              const struct update_row_info &row_info,
                              const TABLE *const altered_table)
{
  uint new_packed_size;
  uint old_packed_size;

  common::Slice new_key_slice;
  common::Slice new_value_slice;
  common::Slice old_key_slice;

  const uint key_id = kd.get_keyno();
  /*
    Can skip updating this key if none of the key fields have changed.
  */
  if (row_info.old_data != nullptr && !m_update_scope.is_set(key_id)) {
    return HA_EXIT_SUCCESS;
  }

  const bool store_row_debug_checksums = should_store_row_debug_checksums();

  new_packed_size = kd.pack_record(
      table_arg, m_pack_buffer, row_info.new_data, m_sk_packed_tuple,
      &m_sk_tails, store_row_debug_checksums, row_info.hidden_pk_id,
      0, nullptr, altered_table);
  assert(new_packed_size <= m_max_packed_sk_len);
  if (row_info.old_data != nullptr) {
    // The old value
    old_packed_size = kd.pack_record(
        table_arg, m_pack_buffer, row_info.old_data, m_sk_packed_tuple_old,
        &m_sk_tails_old, store_row_debug_checksums, row_info.hidden_pk_id,
              0, nullptr, altered_table);
    assert(old_packed_size <= m_max_packed_sk_len);

    /*
      Check if we are going to write the same value. This can happen when
      one does
        UPDATE tbl SET col='foo'
      and we are looking at the row that already has col='foo'.

      We also need to compare the unpack info. Suppose, the collation is
      case-insensitive, and unpack info contains information about whether
      the letters were uppercase and lowercase.  Then, both 'foo' and 'FOO'
      will have the same key value, but different data in unpack_info.

      (note: anyone changing bytewise_compare should take this code into
      account)
    */
    if (old_packed_size == new_packed_size &&
        m_sk_tails_old.get_current_pos() == m_sk_tails.get_current_pos() &&
        memcmp(m_sk_packed_tuple_old, m_sk_packed_tuple, old_packed_size) ==
            0 &&
        memcmp(m_sk_tails_old.ptr(), m_sk_tails.ptr(),
               m_sk_tails.get_current_pos()) == 0) {
      return HA_EXIT_SUCCESS;
    }

    /*
      Deleting entries from secondary index should skip locking, but
      be visible to the transaction.
      (also note that DDL statements do not delete rows, so this is not a DDL
       statement)
    */
    old_key_slice = common::Slice(
        reinterpret_cast<const char *>(m_sk_packed_tuple_old), old_packed_size);

    row_info.tx->get_indexed_write_batch()->SingleDelete(kd.get_cf(),
                                                         old_key_slice);
  }

#if 0  /* Now we have already disable bulk load and can't skip unique check */
  /*
    We're writing a new entry for secondary key. We can skip locking; we
    should write to
    - WriteBatchWithIndex normally (so that transaction sees the new row)
    - non-indexed WriteBatch, when we don't need to see the new row:
       = when doing a DDL operation and writing to a non-unique index, or
       = when doing a bulk load
  */
  db::WriteBatchBase *write_batch;
  if ((row_info.tx->m_ddl_transaction &&
       !(table_arg->key_info[key_id].flags & HA_NOSAME)) ||
      row_info.skip_unique_check) {
    write_batch = row_info.tx->get_blind_write_batch();
  } else {
    write_batch = row_info.tx->get_indexed_write_batch();
  }
#endif

  db::WriteBatchBase *write_batch;
  write_batch = row_info.tx->get_indexed_write_batch();

  new_key_slice = common::Slice(
      reinterpret_cast<const char *>(m_sk_packed_tuple), new_packed_size);
  new_value_slice =
      common::Slice(reinterpret_cast<const char *>(m_sk_tails.ptr()),
                     m_sk_tails.get_current_pos());

  write_batch->Put(kd.get_cf(), new_key_slice, new_value_slice);

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::update_indexes(const struct update_row_info &row_info,
                                   const bool &pk_changed)
{
  int rc = 0;

  for (uint key_id = 0; key_id < m_tbl_def->m_key_count; key_id++) {
    const SeKeyDef &kd = *m_key_descr_arr[key_id];
    if (is_pk(key_id, table, m_tbl_def.get())) {
      rc = update_pk(kd, row_info, pk_changed);
    } else {
      rc = update_sk(table, kd, row_info);
    }

    if (rc != 0) {
      return rc;
    }
  }

  //update the indexes added-in-progress if necessary
  for (auto &k : m_tbl_def->m_added_key) {
    const SeKeyDef &kd = *k.first;
    const TABLE *atab = k.second.altered_table;
    if (table->s->primary_key != atab ->s->primary_key) {
      continue;
    }

    if (k.second.step >= Added_key_info::BUILDING_BASE_INDEX) {
      rc = update_sk(table, kd, row_info, atab);
    }

    assert(rc == HA_EXIT_SUCCESS);
    // it looks like update_sk() never returns error, but anyway
    //if (rc != 0) {
    //  k.second.status = HA_ERR_INTERNAL_ERROR;
    //}
  }

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::get_pk_for_update(struct update_row_info *const row_info)
{
  uint size;

  /*
    Get new row key for any insert, and any update where the pk is not hidden.
    Row key for updates with hidden pk is handled below.
  */
  if (!has_hidden_pk(table)) {
    row_info->hidden_pk_id = 0;

    row_info->new_pk_unpack_info = &m_pk_unpack_info;

    size = m_pk_descr->pack_record(table, m_pack_buffer, row_info->new_data,
                                   m_pk_packed_tuple,
                                   row_info->new_pk_unpack_info, false);
    assert(size <= m_max_packed_sk_len);
  } else if (row_info->old_data == nullptr) {
    row_info->hidden_pk_id = update_hidden_pk_val();
    size =
        m_pk_descr->pack_hidden_pk(row_info->hidden_pk_id, m_pk_packed_tuple);
  } else {
    /*
      If hidden primary key, rowkey for new record will always be the same as
      before
    */
    size = row_info->old_pk_slice.size();
    memcpy(m_pk_packed_tuple, row_info->old_pk_slice.data(), size);
    if (read_hidden_pk_id_from_rowkey(&row_info->hidden_pk_id, &m_last_rowkey)) {
      return HA_ERR_INTERNAL_ERROR;
    }
  }

  row_info->new_pk_slice =
      common::Slice((const char *)m_pk_packed_tuple, size);

  return HA_EXIT_SUCCESS;
}

/**
 @param old_record, record to be delete
 @param tx, transaction context
 @param pk_slice, pk to b delete
 @param hidden_pk_id, if table has no hidden_pk, then hidden_pk_id is 0, otherwise > 0
*/
int ha_smartengine::delete_indexes(const uchar *const old_record)
{
  DBUG_ENTER_FUNC();

  common::Slice key_slice(m_last_rowkey.ptr(), m_last_rowkey.length());
  SeTransaction *const tx = get_or_create_tx(table->in_use);

  const uint index = pk_index(table, m_tbl_def.get());
  common::Status s =
      delete_or_singledelete(index, tx, m_pk_descr->get_cf(), key_slice);
  if (!s.ok()) {
    __HANDLER_LOG(WARN, "DML: failed to delete record(%s) on pk (%u) with error %s, table_name: %s",
                  key_slice.ToString(true).c_str(), m_pk_descr->get_index_number(), s.ToString().c_str(),
                  table->s->table_name.str);
    DBUG_RETURN(tx->set_status_error(table->in_use, s, *m_pk_descr, m_tbl_def.get()));
  }

  longlong hidden_pk_id = 0;
  assert(m_tbl_def->m_key_count >= 1);
  if (has_hidden_pk(table) && read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey))
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

  // Delete the record for every secondary index
  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    if (!is_pk(i, table, m_tbl_def.get())) {
      uint packed_size;
      const SeKeyDef &kd = *m_key_descr_arr[i];
      packed_size = kd.pack_record(table, m_pack_buffer, old_record, m_sk_packed_tuple,
                                   nullptr, false, hidden_pk_id);
      assert(packed_size <= m_max_packed_sk_len);

      common::Slice secondary_key_slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), packed_size);
      /* Deleting on secondary key doesn't need any locks: */
      tx->get_indexed_write_batch()->SingleDelete(kd.get_cf(),
                                                  secondary_key_slice);
    }
  }

  // Delete the record for new added index
  for (auto &k : m_tbl_def->m_added_key) {
    const SeKeyDef &kd = *k.first;
    const TABLE *atab = k.second.altered_table;
    if (table->s->primary_key != atab->s->primary_key) {
      continue;
    }

    if (k.second.step >= Added_key_info::BUILDING_BASE_INDEX) {
      uint packed_size =
          kd.pack_record(table, m_pack_buffer, old_record, m_sk_packed_tuple, nullptr,
                         false, hidden_pk_id, 0, nullptr, atab);
      assert(packed_size <= m_max_packed_sk_len);
      common::Slice secondary_key_slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), packed_size);
      /* Deleting on secondary key doesn't need any locks: */
      tx->get_indexed_write_batch()->SingleDelete(kd.get_cf(),
                                                  secondary_key_slice);
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

bool ha_smartengine::is_using_prohibited_gap_locks(TABLE *table, bool using_full_primary_key)
{
  THD* thd = table->in_use;
  thr_lock_type lock_type = table->reginfo.lock_type;
  const char *table_name = table->s->table_name.str;
  //TODO implement this function like MySQL 8.0
  if (!using_full_primary_key
      && ht
      && (ht->db_type == DB_TYPE_INNODB || ht->db_type == DB_TYPE_SMARTENGINE)
      && !thd->rli_slave
      //&& (thd->variables.gap_lock_raise_error
      //    || thd->variables.gap_lock_write_log)
      && (thd->lex->table_count >= 2 || thd->in_multi_stmt_transaction_mode())
      //&& can_hold_locks_on_trans(thd, lock_type)
      /*&& !gap_lock_exceptions->matches(table_name)*/) //COMMENT temporarily
  {
    //gap_lock_log_write(thd, COM_QUERY, thd->query().str, thd->query().length);
    //if (thd->variables.gap_lock_raise_error)
    //{
    //  my_printf_error(ER_UNKNOWN_ERROR,
    //                  "Using Gap Lock without full unique key in multi-table "
    //                  "or multi-statement transactions is not "
    //                  "allowed. You need either 1: Execute 'SET SESSION "
    //                  "gap_lock_raise_error=0' if you are sure that "
    //                  "your application does not rely on Gap Lock. "
    //                  "2: Rewrite queries to use "
    //                  "all unique key columns in WHERE equal conditions. "
    //                  "3: Rewrite to single-table, single-statement "
    //                  "transaction.  Query: %s",
    //                  MYF(0), thd->query().length);
    //  return true;
    //}
  }
  return false;
}

bool ha_smartengine::is_using_full_key(key_part_map keypart_map, uint actual_key_parts)
{
  return (keypart_map == HA_WHOLE_KEY) ||
         (keypart_map == ((key_part_map(1) << actual_key_parts) - 1));
}

bool ha_smartengine::is_using_full_unique_key(uint index,
                                              key_part_map keypart_map,
                                              enum ha_rkey_function find_flag)
{
  return (is_using_full_key(keypart_map,
                            table->key_info[index].actual_key_parts)
          && find_flag == HA_READ_KEY_EXACT
          && (index == table->s->primary_key
              || (table->key_info[index].flags & HA_NOSAME)));
}

void ha_smartengine::load_auto_incr_value()
{
  //TODO this api should implemented like mysql 8.0
  const int save_active_index = active_index;
  active_index = table->s->next_number_index;
  //const uint8 save_table_status = table->status;

  /*
    load_auto_incr_value() may be called by statements that
    do not execute implicit commits (i.e. SHOW CREATE TABLE).
    index_last() creates a snapshot. When a snapshot is created
    here, it has to be released as well. (GitHub issue#189)
  */
  SeTransaction *const tx = get_or_create_tx(table->in_use);
  const bool is_new_snapshot = !tx->has_snapshot();

  // Do a lookup. We only need index column, so it should be index-only.
  // (another reason to make it index-only is that table->read_set is
  //  not set appropriately and non-index-only lookup will not read the value)
  const bool save_keyread_only = m_keyread_only;
  m_keyread_only = true;

  if (!index_last(table->record[0]))
    update_auto_incr_val();

  m_keyread_only = save_keyread_only;
  if (is_new_snapshot) {
    tx->release_snapshot();
  }

  //table->status = save_table_status;
  active_index = save_active_index;

  /*
    Do what ha_smartengine::index_end() does.
    (Why don't we use index_init/index_end? class handler defines index_init
     as private, for some reason).
  */
  release_scan_iterator();
}

/* Get PK value from table->record[0]. */
/*
  TODO(alexyang): No existing support for auto_increment on non-pk columns, see
  end of ha_smartengine::create. Also see opened issue here:
  https://github.com/facebook/mysql-5.6/issues/153
*/
void ha_smartengine::update_auto_incr_val()
{
  Field *field;
  ulonglong new_val, max_val;
  field = table->key_info[table->s->next_number_index].key_part[0].field;
  max_val = se_get_int_col_max_value(field);

  my_bitmap_map *const old_map =
      dbug_tmp_use_all_columns(table, table->read_set);

  new_val = field->val_int();
  if (new_val != max_val) {
    new_val++;
  }

  dbug_tmp_restore_column_map(table->read_set, old_map);

  if (new_val <= max_val) {
    ulonglong auto_incr_val = m_tbl_def->m_auto_incr_val;
    while (auto_incr_val < new_val &&
         !m_tbl_def->m_auto_incr_val.compare_exchange_weak(auto_incr_val,
                                                           new_val)) {
    // Do nothing - just loop until auto_incr_val is >= new_val or
    // we successfully set it
    }
  }
}

int ha_smartengine::load_hidden_pk_value()
{
  const int save_active_index = active_index;
  active_index = m_tbl_def->m_key_count - 1;
  //const uint8 save_table_status = table->status;

  SeTransaction *const tx = get_or_create_tx(table->in_use);
  const bool is_new_snapshot = !tx->has_snapshot();

  // Do a lookup.
  if (!index_last(table->record[0])) {
    /*
      Decode PK field from the key
    */
    longlong hidden_pk_id = 0;
    if (read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey)) {
      if (is_new_snapshot) {
        tx->release_snapshot();
      }
      return HA_ERR_INTERNAL_ERROR;
    }

    hidden_pk_id++;
    longlong old = m_tbl_def->m_hidden_pk_val;
    while (
        old < hidden_pk_id &&
        !m_tbl_def->m_hidden_pk_val.compare_exchange_weak(old, hidden_pk_id)) {
    }
  }

  if (is_new_snapshot) {
    tx->release_snapshot();
  }

  //table->status = save_table_status;
  active_index = save_active_index;

  release_scan_iterator();

  return HA_EXIT_SUCCESS;
}

/* Get PK value from m_tbl_def->m_hidden_pk_info. */
longlong ha_smartengine::update_hidden_pk_val()
{
  assert(has_hidden_pk(table));
  const longlong new_val = m_tbl_def->m_hidden_pk_val++;
  return new_val;
}

/* Get the id of the hidden pk id from m_last_rowkey */
int ha_smartengine::read_hidden_pk_id_from_rowkey(longlong *const hidden_pk_id, const String *key)
{
  assert(hidden_pk_id != nullptr);
  assert(table != nullptr);
  assert(has_hidden_pk(table));

  common::Slice rowkey_slice(key->ptr(), key->length());

  // Get hidden primary key from old key slice
  SeStringReader reader(&rowkey_slice);
  if ((!reader.read(SeKeyDef::INDEX_NUMBER_SIZE)))
    return HA_EXIT_FAILURE;

  const int length = Field_longlong::PACK_LENGTH;
  const uchar *from = reinterpret_cast<const uchar *>(reader.read(length));
  if (from == nullptr) {
    return HA_EXIT_FAILURE; /* Mem-comparable image doesn't have enough bytes */
  }

  *hidden_pk_id = se_netbuf_read_uint64(&from);
  return HA_EXIT_SUCCESS;
}

bool ha_smartengine::init_with_fields()
{
  DBUG_ENTER_FUNC();

  const uint pk = table_share->primary_key;
  if (pk != MAX_KEY) {
    const uint key_parts = table_share->key_info[pk].user_defined_key_parts;
    check_keyread_allowed(pk /*PK*/, key_parts - 1, true);
  } else {
    m_pk_can_be_decoded = false;
  }

  cached_table_flags = table_flags();

  DBUG_RETURN(false); /* Ok */
}

bool ha_smartengine::rpl_can_handle_stm_event() const noexcept
{
  return !(se_rpl_skip_tx_api_var && !super_read_only);
}

/**
  Convert record from table->record[0] form into a form that can be written
  into se.

  @param pk_packed_slice      Packed PK tuple. We need it in order to compute
                              and store its CRC.
  @param packed_rec      OUT  Data slice with record data.
*/

int ha_smartengine::convert_record_to_storage_format(
    const common::Slice &pk_packed_slice,
    SeStringWriter *const pk_unpack_info,
    common::Slice *const packed_rec)
{
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnonnull-compare"
#endif //__clang__
  DBUG_ASSERT_IMP(m_maybe_unpack_info, pk_unpack_info);
#ifndef __clang__
#pragma GCC diagnostic pop
#endif //__clang__
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_CONVERT_TO);
  m_storage_record.length(0);

  // reserve RECORD_HEADER_SIZE bytes for future use
  m_storage_record.fill(schema::RecordFormat::RECORD_HEADER_SIZE, 0);
  int field_number_len = 0;
  // store instant ddl flag and field numbers if necessary
  if (m_instant_ddl_info.have_instantly_added_columns()) {
    char *const begin_point = (char *)m_storage_record.ptr();
    begin_point[0] |= schema::RecordFormat::RECORD_INSTANT_DDL_FLAG;
    char field_number_str[2];
    char nullable_bytes_str[2];
    int2store(field_number_str, table->s->fields);
    int2store(nullable_bytes_str, m_null_bytes_in_rec);
    m_storage_record.append(field_number_str, schema::RecordFormat::RECORD_FIELD_NUMBER_SIZE);
    m_storage_record.append(nullable_bytes_str, schema::RecordFormat::RECORD_NULL_BITMAP_SIZE_BYTES);
    field_number_len = schema::RecordFormat::RECORD_FIELD_NUMBER_SIZE + schema::RecordFormat::RECORD_NULL_BITMAP_SIZE_BYTES;
  }

  /* All NULL bits are initially 0 */
  String null_bytes_str;
  null_bytes_str.length(0);
  null_bytes_str.fill(m_null_bytes_in_rec, 0);
  m_storage_record.append(null_bytes_str);

  // If a primary key may have non-empty unpack_info for certain values,
  // (m_maybe_unpack_info=TRUE), we write the unpack_info block. The block
  // itself was prepared in SeKeyDef::pack_record.
  if (m_maybe_unpack_info) {
    m_storage_record.append(reinterpret_cast<char *>(pk_unpack_info->ptr()),
                            pk_unpack_info->get_current_pos());
  }

  for (uint i = 0; i < table->s->fields; i++) {
    /* Don't pack decodable PK key parts */
    if (m_encoder_arr[i].m_storage_type != SeFieldEncoder::STORE_ALL) {
      continue;
    }

    Field *const field = table->field[i];
    if (m_encoder_arr[i].maybe_null()) {
      char *const data = (char *)m_storage_record.ptr() +
                         schema::RecordFormat::RECORD_HEADER_SIZE +
                         field_number_len; /*0 or RECORD_FIELD_NUMBER_SIZE*/
      if (field->is_null()) {
        data[m_encoder_arr[i].m_null_offset] |= m_encoder_arr[i].m_null_mask;
        /* Don't write anything for NULL values */
        continue;
      }
    }

    if (m_encoder_arr[i].m_field_type == MYSQL_TYPE_BLOB ||
        m_encoder_arr[i].m_field_type == MYSQL_TYPE_JSON) {
      append_blob_to_storage_format(m_storage_record,
                                    (my_core::Field_blob *)field);
    } else if (m_encoder_arr[i].m_field_type == MYSQL_TYPE_VARCHAR) {
      append_varchar_to_storage_format(m_storage_record,
                                       (Field_varstring *)field);
    } else if (m_encoder_arr[i].m_field_type == MYSQL_TYPE_STRING) {
      append_string_to_storage_format(m_storage_record, (Field_string *)field);
    } else {
      /* Copy the field data */
      const uint len = field->pack_length_in_rec();
      m_storage_record.append(reinterpret_cast<char *>(field->field_ptr()), len);
    }
  }

  if (should_store_row_debug_checksums()) {
    const uint32_t key_crc32 = my_core::crc32(
        0, se_slice_to_uchar_ptr(&pk_packed_slice), pk_packed_slice.size());
    const uint32_t val_crc32 =
        my_core::crc32(0, se_mysql_str_to_uchar_str(&m_storage_record),
                       m_storage_record.length());
    uchar key_crc_buf[SE_CHECKSUM_SIZE];
    uchar val_crc_buf[SE_CHECKSUM_SIZE];
    se_netbuf_store_uint32(key_crc_buf, key_crc32);
    se_netbuf_store_uint32(val_crc_buf, val_crc32);
    m_storage_record.append((const char *)&SE_CHECKSUM_DATA_TAG, 1);
    m_storage_record.append((const char *)key_crc_buf, SE_CHECKSUM_SIZE);
    m_storage_record.append((const char *)val_crc_buf, SE_CHECKSUM_SIZE);
  }

  *packed_rec =
      common::Slice(m_storage_record.ptr(), m_storage_record.length());

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::append_blob_to_storage_format(
    String &storage_record,
    my_core::Field_blob *const blob)
{
  /* Get the number of bytes needed to store length*/
  const uint length_bytes = blob->pack_length() - portable_sizeof_char_ptr;

  /* Store the length of the value */
  storage_record.append(reinterpret_cast<char *>(blob->field_ptr()), length_bytes);

  /* Store the blob value itself */
  char *data_ptr = nullptr;
  memcpy(&data_ptr, blob->field_ptr() + length_bytes, sizeof(uchar **));
  storage_record.append(data_ptr, blob->get_length());

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::append_varchar_to_storage_format(
    String &storage_record,
    my_core::Field_varstring *const field_var)
{
  uint data_len = 0;
  /* field_var->length_bytes is 1 or 2 */
  if (field_var->get_length_bytes() == 1) {
    data_len = field_var->field_ptr()[0];
  } else {
    assert(field_var->get_length_bytes() == 2);
    data_len = uint2korr(field_var->field_ptr());
  }
  storage_record.append(reinterpret_cast<char *>(field_var->field_ptr()),
                        field_var->get_length_bytes() + data_len);

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::append_string_to_storage_format(
    String &storage_record,
    my_core::Field_string *const field_str)
{
  int ret = HA_EXIT_SUCCESS;
  char length_buf[schema::RecordFormat::MYSQL_STRING_SIZE_BYTES] = {0};
  char *val = reinterpret_cast<char *>(field_str->field_ptr());
  uint16 val_length = field_str->pack_length_in_rec();
  assert(val_length >= 0);

  if (0 == val_length) {
    // For string with zero length, do not store any content.
  } else if (UNLIKELY(field_str->charset()->mbminlen > 1)) {
    // For an ASCII incompatible string, e.g. UCS-2,
    // store the data directly.
    if (UNLIKELY(storage_record.append(val, val_length))) {
      ret = HA_ERR_INTERNAL_ERROR;
      SE_LOG(ERROR, "fail to append string value", K(ret), K(val_length));
    }
  } else {
    // For an ASCII compatible string, try to stripe
    // trailing space to save storage space.
    se_assert(1 == field_str->charset()->mbminlen);
    while (val_length > 0 && 0x20 == val[val_length - 1]) {
      --val_length;
    }
    int2store(length_buf, val_length);

    if (UNLIKELY(storage_record.append(length_buf, schema::RecordFormat::MYSQL_STRING_SIZE_BYTES))) {
      ret = HA_ERR_INTERNAL_ERROR;
      SE_LOG(ERROR, "fail to append string length", K(ret), K(val_length));
    } else if (UNLIKELY(storage_record.append(val, val_length))) {
      ret = HA_ERR_INTERNAL_ERROR;
      SE_LOG(ERROR, "fail to append string value", K(ret), K(val_length));
    }
  }
  se_assert(HA_EXIT_SUCCESS == ret);

  return ret; 
}

/*
 param OUT maybe_unpack_info, true if storage record contains unpack_info
 */
void ha_smartengine::get_storage_type(SeFieldEncoder *const encoder,
                                      std::shared_ptr<SeKeyDef> pk_descr,
                                      const uint &kp,
                                      bool &maybe_unpack_info)
{
  // STORE_SOME uses unpack_info.
  if (pk_descr->has_unpack_info(kp)) {
    assert(pk_descr->can_unpack(kp));
    encoder->m_storage_type = SeFieldEncoder::STORE_SOME;
    maybe_unpack_info = true;
  } else if (pk_descr->can_unpack(kp)) {
    encoder->m_storage_type = SeFieldEncoder::STORE_NONE;
  }
}

/*
  Setup data needed to convert table->record[] to and from record storage
  format.

  @seealso
     ha_smartengine::convert_record_to_storage_format,
     ha_smartengine::convert_record_from_storage_format
*/
int ha_smartengine::setup_field_converters()
{
  assert(this->table != nullptr);
  assert(this->m_encoder_arr == nullptr);
  assert(this->m_pk_descr != nullptr);

  m_fields_no_needed_to_decode = 0;
  m_null_bytes_in_rec = 0;
  return setup_field_converters(table,
                                m_pk_descr,
                                m_encoder_arr,
                                m_fields_no_needed_to_decode,
                                m_null_bytes_in_rec,
                                m_maybe_unpack_info);

}

int ha_smartengine::setup_field_converters(const TABLE *table,
                                           std::shared_ptr<SeKeyDef> pk_descr,
                                           SeFieldEncoder* &encoder_arr,
                                           uint &fields_no_needed_to_decode,
                                           uint &null_bytes_in_rec,
                                           bool &maybe_unpack_info)
{
  uint i;
  uint null_bytes = 0;
  uchar cur_null_mask = 0x1;

  encoder_arr = static_cast<SeFieldEncoder *>(
      my_malloc(PSI_NOT_INSTRUMENTED, table->s->fields * sizeof(SeFieldEncoder), MYF(0)));
  if (encoder_arr == nullptr) {
    __HANDLER_LOG(ERROR, "allcate from memory failed");
    return HA_EXIT_FAILURE;
  }

  for (i = 0; i < table->s->fields; i++) {
    Field *const field = table->field[i];
    encoder_arr[i].m_storage_type = SeFieldEncoder::STORE_ALL;

    /*
      Check if this field is
      - a part of primary key, and
      - it can be decoded back from its key image.
      If both hold, we don't need to store this field in the value part of
      se's key-value pair.

      If hidden pk exists, we skip this check since the field will never be
      part of the hidden pk.
    */
    if (!has_hidden_pk(table) &&
        field->part_of_key.is_set(table->s->primary_key)) {
      KEY *const pk_info = &table->key_info[table->s->primary_key];
      for (uint kp = 0; kp < pk_info->user_defined_key_parts; kp++) {
        /* key_part->fieldnr is counted from 1 */
        if (field->field_index() + 1 == pk_info->key_part[kp].fieldnr) {
          get_storage_type(&encoder_arr[i], pk_descr, kp, maybe_unpack_info);
          break;
        }
      }
    }

    if (encoder_arr[i].m_storage_type != SeFieldEncoder::STORE_ALL) {
      fields_no_needed_to_decode++;
    }

    encoder_arr[i].m_field_type = field->real_type();
    encoder_arr[i].m_field_index = i;
    encoder_arr[i].m_pack_length_in_rec = field->pack_length_in_rec();

    if (field->is_nullable()) {
      encoder_arr[i].m_null_mask = cur_null_mask;
      encoder_arr[i].m_null_offset = null_bytes;
      if (cur_null_mask == 0x80) {
        cur_null_mask = 0x1;
        null_bytes++;
      } else
        cur_null_mask = cur_null_mask << 1;
    } else {
      encoder_arr[i].m_null_mask = 0;
    }
  }

  /* Count the last, unfinished NULL-bits byte */
  if (cur_null_mask != 0x1)
    null_bytes++;

  null_bytes_in_rec = null_bytes;

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::alloc_key_buffers(const TABLE *const table_arg,
                                      const SeTableDef *const tbl_def_arg,
                                      bool alloc_alter_buffers)
{
  DBUG_ENTER_FUNC();

  assert(m_pk_tuple == nullptr);
  assert(tbl_def_arg != nullptr);

  std::shared_ptr<SeKeyDef> *const kd_arr = tbl_def_arg->m_key_descr_arr;

  uint key_len = 0;
  m_max_packed_sk_len = 0;
  m_pack_key_len = 0;

  m_pk_descr = kd_arr[pk_index(table_arg, tbl_def_arg)];
  if (has_hidden_pk(table_arg)) {
    m_pk_key_parts = 1;
  } else {
    m_pk_key_parts =
        table->key_info[table->s->primary_key].user_defined_key_parts;
    key_len = table->key_info[table->s->primary_key].key_length;
  }

  // move this into get_table_handler() ??
  m_pk_descr->setup(table_arg, tbl_def_arg);

  m_pk_tuple = reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, key_len, MYF(0)));
  if (m_pk_tuple == nullptr) {
    goto error;
  }

  //packed pk length
  m_pack_key_len = m_pk_descr->max_storage_fmt_length();

  /* Sometimes, we may use m_sk_packed_tuple for storing packed PK */
  m_max_packed_sk_len = m_pack_key_len;
  for (uint i = 0; i < table_arg->s->keys; i++) {
    if (i == table_arg->s->primary_key) /* Primary key was processed above */
      continue;

    // TODO: move this into get_table_handler() ??
    kd_arr[i]->setup(table_arg, tbl_def_arg);

    const uint packed_len = kd_arr[i]->max_storage_fmt_length();
    if (packed_len > m_max_packed_sk_len) {
      m_max_packed_sk_len = packed_len;
    }
  }

  // the cached TABLE(s) were dropped at prepare_inplace_alter_table, so the
  // new SQLs will open the table again and call into this function. It makes
  // sure that enough space is allocated for the inplace index.
  for (auto &k : m_tbl_def->m_added_key) {
    const SeKeyDef &kd = *k.first;
    const uint packed_len = kd.max_storage_fmt_length();
    if (packed_len > m_max_packed_sk_len) {
      m_max_packed_sk_len = packed_len;
    }
  }

  //for online-copy-ddl, we need allocate enough space for new indexes and record
  if (m_tbl_def->m_inplace_new_tdef) {
    SeTableDef* new_tdef = m_tbl_def->m_inplace_new_tdef;
    for (uint i = 0; i < new_tdef->m_key_count; i++) {
      std::shared_ptr<SeKeyDef> kd = new_tdef->m_key_descr_arr[i];
      const uint packed_len = kd->max_storage_fmt_length();
      if (packed_len > m_max_packed_sk_len) {
        m_max_packed_sk_len = packed_len;
      }
    }

    assert(!m_tbl_def->m_inplace_new_keys.empty());
    auto iter = m_tbl_def->m_inplace_new_keys.begin();
    TABLE *altered_table = iter->second.altered_table;
    assert(altered_table != nullptr);


  //pk maybe changed, enlarge pk_buffer if necessary
    m_new_pk_descr = new_tdef->m_key_descr_arr[pk_index(altered_table, new_tdef)];
    assert(m_new_pk_descr != nullptr);

    uint new_packed_pk_len = m_new_pk_descr->max_storage_fmt_length();
    m_pack_key_len = m_pack_key_len < new_packed_pk_len ? new_packed_pk_len : m_pack_key_len;

    uint new_record_len = altered_table->s->rec_buff_length + altered_table->s->null_bytes;

    // allocate memory for old_pk in new table update
    m_pk_packed_tuple_old = reinterpret_cast<uchar *>(
        my_malloc(PSI_NOT_INSTRUMENTED, m_pack_key_len, MYF(0)));
    if (m_pk_packed_tuple_old == nullptr) {
      goto error;
    }
  }

  //allocate memory for primary key
  m_pk_packed_tuple =
      reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_pack_key_len, MYF(0)));
  if (m_pk_packed_tuple == nullptr) {
    goto error;
  }

  //allocate memory for secondary keys
  if (!(m_sk_packed_tuple =
            reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
      !(m_sk_match_prefix_buf =
            reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
      !(m_sk_packed_tuple_old =
            reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
      !(m_end_key_packed_tuple =
            reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
      !((m_pack_buffer = reinterpret_cast<uchar *>(
             my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))))) {
    goto error;
  }

  /*
    If inplace alter is happening, allocate special buffers for unique
    secondary index duplicate checking.
  */
  if (alloc_alter_buffers &&
      (!(m_dup_sk_packed_tuple =
             reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
       !(m_dup_sk_packed_tuple_old = reinterpret_cast<uchar *>(
             my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))))) {
    goto error;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);

error:
  // If we're here then this means that at some point above an allocation may
  // have failed. To avoid any resource leaks and maintain a clear contract
  // we'll clean up before returning the error code.
  free_key_buffers();

  DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
}

void ha_smartengine::free_key_buffers()
{
  my_free(m_pk_tuple);
  m_pk_tuple = nullptr;

  my_free(m_pk_packed_tuple);
  m_pk_packed_tuple = nullptr;

  my_free(m_pk_packed_tuple_old);
  m_pk_packed_tuple_old = nullptr;

  my_free(m_sk_packed_tuple);
  m_sk_packed_tuple = nullptr;

  my_free(m_sk_match_prefix_buf);
  m_sk_match_prefix_buf = nullptr;

  my_free(m_sk_packed_tuple_old);
  m_sk_packed_tuple_old = nullptr;

  my_free(m_end_key_packed_tuple);
  m_end_key_packed_tuple = nullptr;

  my_free(m_pack_buffer);
  m_pack_buffer = nullptr;

  my_free(m_dup_sk_packed_tuple);
  m_dup_sk_packed_tuple = nullptr;

  my_free(m_dup_sk_packed_tuple_old);
  m_dup_sk_packed_tuple_old = nullptr;

  my_free(m_new_record);
  m_new_record = nullptr;
}


void ha_smartengine::set_skip_unique_check_tables(const char *const whitelist)
{
  assert(whitelist != nullptr);

#if defined(HAVE_PSI_INTERFACE)
  Regex_list_handler regex_handler(key_rwlock_skip_unique_check_tables);
#else
  Regex_list_handler regex_handler;
#endif

  if (!regex_handler.set_patterns(whitelist)) {
    warn_about_bad_patterns(&regex_handler, "skip_unique_check_tables");
  }

  m_skip_unique_check = regex_handler.matches(m_tbl_def->base_tablename());
}

/*
  Check to see if the user's original statement includes foreign key
  references
*/
bool ha_smartengine::contains_foreign_key(THD *const thd)
{
  bool success;
  size_t query_length = thd->query().length + 1;
  std::shared_ptr<char> query_str(new char[query_length], std::default_delete<char []>());
  (void)thd_query_safe(thd, query_str.get(), query_length);
  const char *str = query_str.get();

  assert(str != nullptr);

  while (*str != '\0') {
    // Scan from our current pos looking for 'FOREIGN'
    str = se_find_in_string(str, "FOREIGN", &success);
    if (!success) {
      return false;
    }

    // Skip past the found "FOREIGN'
    str = se_check_next_token(&my_charset_bin, str, "FOREIGN", &success);
    assert(success);

    if (!my_isspace(&my_charset_bin, *str)) {
      return false;
    }

    // See if the next token is 'KEY'
    str = se_check_next_token(&my_charset_bin, str, "KEY", &success);
    if (!success) {
      continue;
    }

    // See if the next token is '('
    str = se_check_next_token(&my_charset_bin, str, "(", &success);
    if (!success) {
      // There is an optional index id after 'FOREIGN KEY', skip it
      str = se_skip_id(&my_charset_bin, str);

      // Now check for '(' again
      str = se_check_next_token(&my_charset_bin, str, "(", &success);
    }

    // If we have found 'FOREIGN KEY [<word>] (' we can be confident we have
    // a foreign key clause.
    return success;
  }

  // We never found a valid foreign key clause
  return false;
}


/** for ddl transaction, we write atomic-ddl log in se, so
    mark se modified.
 */
void ha_smartengine::mark_ddl_trx_read_write()
{
  Ha_trx_info *ha_info = &ha_thd()->get_ha_data(ht->slot)->ha_info[0];

  /** make sure se registed by se_register_tx */
  if (ha_info->is_started()) {
    ha_info->set_trx_read_write();
  }
}

common::Status ha_smartengine::get_for_update(SeTransaction *const tx,
                                              db::ColumnFamilyHandle *const column_family,
                                              const common::Slice &key,
                                              std::string *const value,
                                              bool lock_unique) const
{
  assert(m_lock_rows != SE_LOCK_NONE);
  const bool exclusive = m_lock_rows != SE_LOCK_READ;
  QUERY_TRACE_BEGIN(monitor::TracePoint::HA_GET_FOR_UPDATE);

  common::Status s = tx->get_for_update(column_family, key, value, exclusive, lock_unique);

  // If we have a lock conflict and we are running in READ COMMITTTED mode
  // release and reacquire the snapshot and then retry the get_for_update().
  if (s.IsBusy() && !s.IsDeadlock() &&
      my_core::thd_tx_isolation(ha_thd()) == ISO_READ_COMMITTED) {
    tx->release_snapshot();
    tx->acquire_snapshot(false);

    s = tx->get_for_update(column_family, key, value, exclusive, lock_unique);
  }

  QUERY_TRACE_END();
  return s;
}

common::Status ha_smartengine::lock_unique_key(SeTransaction *const tx,
                                               db::ColumnFamilyHandle *const column_family,
                                               const common::Slice &key,
                                               const bool total_order_seek,
                                               const bool fill_cache) const
{
  assert(m_lock_rows != SE_LOCK_NONE);
  const bool exclusive = m_lock_rows != SE_LOCK_READ;

  common::Status s = tx->lock_unique_key(column_family, key, total_order_seek, fill_cache, exclusive);

  // If we have a lock conflict and we are running in READ COMMITTED mode
  // release and reacquire the snapshot and then retry the get_for_update().
  if (s.IsBusy() && !s.IsDeadlock() && my_core::thd_tx_isolation(ha_thd()) == ISO_READ_COMMITTED) {
    tx->release_snapshot();
    tx->acquire_snapshot(false);
    s = tx->lock_unique_key(column_family, key, total_order_seek, fill_cache, exclusive);
  }
  return s;
}

/*
  Returning true if SingleDelete can be used.
  - Secondary Indexes can always use SingleDelete.
  - If the index is PRIMARY KEY, and if all of the columns of the table
    are covered by the PRIMARY KEY, SingleDelete can be used.
*/
bool ha_smartengine::can_use_single_delete(const uint &index) const
{
  return (index != pk_index(table, m_tbl_def.get()) ||
         (!has_hidden_pk(table) &&
         table->key_info[index].actual_key_parts == table->s->fields));
}

bool ha_smartengine::skip_unique_check() const
{
  /*
    We want to skip unique checks if:
      1) bulk_load is on
      2) this table is in the whitelist of tables to skip and the replication
         lag has reached a large enough value (see unique_check_lag_threshold
         and unique_check_lage_reset_threshold)
      3) the user set unique_checks option to 0, and the table does not have
         any indexes. If the table has secondary keys, then those might becomes
         inconsisted/corrupted
  */
  return /*THDVAR(table->in_use, bulk_load) ||*/
         (m_force_skip_unique_check && m_skip_unique_check) ||
         (my_core::thd_test_options(table->in_use, OPTION_RELAXED_UNIQUE_CHECKS) &&
          m_tbl_def->m_key_count == 1) ||
         (se_skip_unique_key_check_in_boost_insert &&
         !my_core::thd_test_options(table->in_use, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) ;
}

bool ha_smartengine::commit_in_the_middle()
{
  return true; /* THDVAR(table->in_use, bulk_load) ||
         THDVAR(table->in_use, commit_in_the_middle); */
}

/*
  Executing bulk commit if it should.
  @retval true if bulk commit failed
  @retval false if bulk commit was skipped or succeeded
*/
bool ha_smartengine::do_bulk_commit(SeTransaction *const tx)
{
  assert(tx != nullptr);

  bool ret = false;

  if (commit_in_the_middle() &&
      tx->get_write_count() >= se_thd_bulk_load_size(table->in_use)) {
    ret = tx->flush_batch();

    tx->invalid_writebatch_iterator();

    DBUG_EXECUTE_IF("ddl_log_crash_after_commit_in_the_middle", DBUG_SUICIDE(););
    return ret;
  }

  return false;
}

/*
  If table was created without primary key, SQL layer represents the primary
  key number as MAX_INDEXES.  Hence, this function returns true if the table
  does not contain a primary key. (In which case we generate a hidden
  'auto-incremented' pk.)
*/
bool ha_smartengine::has_hidden_pk(const TABLE *const table) const
{
  assert(table != nullptr);
  return SeKeyDef::table_has_hidden_pk(table);
}

/*
  Returns true if given index number is a hidden_pk.
  - This is used when a table is created with no primary key.
*/
bool ha_smartengine::is_hidden_pk(const uint index,
                                  const TABLE *const table_arg,
                                  const SeTableDef *const tbl_def_arg)
{
  assert(table_arg != nullptr);
  assert(table_arg->s != nullptr);
  assert(tbl_def_arg != nullptr);

  return (table_arg->s->primary_key == MAX_INDEXES &&
          index == tbl_def_arg->m_key_count - 1);
}

/* Returns index of primary key */
uint ha_smartengine::pk_index(const TABLE *const table_arg,
                              const SeTableDef *const tbl_def_arg)
{
  assert(table_arg != nullptr);
  assert(table_arg->s != nullptr);
  assert(tbl_def_arg != nullptr);

  return table_arg->s->primary_key == MAX_INDEXES ? tbl_def_arg->m_key_count - 1
      : table_arg->s->primary_key;
}

/* Returns true if given index number is a primary key */
bool ha_smartengine::is_pk(const uint index,
                           const TABLE *const table_arg,
                           const SeTableDef *const tbl_def_arg)
{
  assert(table_arg != nullptr);
  assert(table_arg->s != nullptr);
  assert(tbl_def_arg != nullptr);

  return index == table_arg->s->primary_key ||
         is_hidden_pk(index, table_arg, tbl_def_arg);
}

const char *ha_smartengine::get_key_name(const uint index,
                                         const TABLE *const table_arg,
                                         const SeTableDef *const tbl_def_arg)
{
  assert(table_arg != nullptr);

  if (is_hidden_pk(index, table_arg, tbl_def_arg)) {
    return HIDDEN_PK_NAME;
  }

  return table_arg->key_info[index].name;
}

const char *ha_smartengine::get_key_comment(const uint index,
                                            const TABLE *const table_arg,
                                            const SeTableDef *const tbl_def_arg)
{
  assert(table_arg != nullptr);

  if (is_hidden_pk(index, table_arg, tbl_def_arg)) {
    return nullptr;
  }

  return table_arg->key_info[index].comment.str;
}


/**
  Constructing m_last_rowkey (SE key expression) from
  before_update|delete image (MySQL row expression).
  m_last_rowkey is normally set during lookup phase, such as
  rnd_next_with_direction() and rnd_pos(). With Read Free Replication,
  these read functions are skipped and update_rows(), delete_rows() are
  called without setting m_last_rowkey. This function sets m_last_rowkey
  for Read Free Replication.
*/
void ha_smartengine::set_last_rowkey(const uchar *const old_data)
{
  if (old_data && use_read_free_rpl()) {
    const uint old_pk_size = m_pk_descr->pack_record(
        table, m_pack_buffer, old_data, m_pk_packed_tuple, nullptr, false);

    assert(old_pk_size <= m_max_packed_sk_len);
    m_last_rowkey.copy((const char *)m_pk_packed_tuple, old_pk_size,
                       &my_charset_bin);
  }
}

int ha_smartengine::check_and_lock_unique_pk(const uint &key_id,
                                             const struct update_row_info &row_info,
                                             bool *const found,
                                             bool *const pk_changed)
{
  DEBUG_SYNC(ha_thd(), "se.check_and_lock_unique_pk");
  assert(found != nullptr);
  assert(pk_changed != nullptr);

  *pk_changed = false;

  /*
    For UPDATEs, if the key has changed, we need to obtain a lock. INSERTs
    always require locking.
  */
  if (row_info.old_pk_slice.size() > 0) {
    /*
      If the keys are the same, then no lock is needed
    */
    if (!SePrimaryKeyComparator::bytewise_compare(row_info.new_pk_slice,
                                             row_info.old_pk_slice)) {
      *found = false;
      return HA_EXIT_SUCCESS;
    }

    *pk_changed = true;
  }

  /*
    Perform a read to determine if a duplicate entry exists. For primary
    keys, a point lookup will be sufficient.

    note: we intentionally don't set options.snapshot here. We want to read
    the latest committed data.
  */

  /*
    To prevent race conditions like below, it is necessary to
    take a lock for a target row. get_for_update() holds a gap lock if
    target key does not exist, so below conditions should never
    happen.

    1) T1 Get(empty) -> T2 Get(empty) -> T1 Put(insert) -> T1 commit
       -> T2 Put(overwrite) -> T2 commit
    2) T1 Get(empty) -> T1 Put(insert, not committed yet) -> T2 Get(empty)
       -> T2 Put(insert, blocked) -> T1 commit -> T2 commit(overwrite)
  */
  const common::Status s =
      get_for_update(row_info.tx, m_pk_descr->get_cf(), row_info.new_pk_slice,
                     &m_retrieved_record);
  if (!s.ok() && !s.IsNotFound()) {
    __HANDLER_LOG(WARN, "DML: get_for_update for key(%s) on index(%u) failed with error:%s, table_name: %s",
                  row_info.new_pk_slice.ToString(true).c_str(),
                  m_pk_descr->get_index_number(), s.ToString().c_str(),
                  table->s->table_name.str);
    return row_info.tx->set_status_error(table->in_use, s,
                                         *m_key_descr_arr[key_id], m_tbl_def.get());
  }

  *found = !s.IsNotFound();
  return HA_EXIT_SUCCESS;
}

int ha_smartengine::check_and_lock_sk(const uint &key_id,
                                      const struct update_row_info &row_info,
                                      bool *const found) const
{
  DEBUG_SYNC(ha_thd(), "se.check_and_lock_sk");
  assert(found != nullptr);
  *found = false;

  /*
    Can skip checking this key if none of the key fields have changed.
  */
  if (row_info.old_data != nullptr && !m_update_scope.is_set(key_id)) {
    return HA_EXIT_SUCCESS;
  }

  KEY *key_info = nullptr;
  uint n_null_fields = 0;
  uint user_defined_key_parts = 1;

  key_info = &table->key_info[key_id];
  user_defined_key_parts = key_info->user_defined_key_parts;
  /*
    If there are no uniqueness requirements, there's no need to obtain a
    lock for this key.
  */
  if (!(key_info->flags & HA_NOSAME)) {
    return HA_EXIT_SUCCESS;
  }

  const SeKeyDef &kd = *m_key_descr_arr[key_id];

  /*
    Calculate the new key for obtaining the lock

    For unique secondary indexes, the key used for locking does not
    include the extended fields.
  */
  uint size =
      kd.pack_record(table, m_pack_buffer, row_info.new_data, m_sk_packed_tuple,
                     nullptr, false, 0, user_defined_key_parts, &n_null_fields);
  assert(size <= m_max_packed_sk_len);

  if (n_null_fields > 0) {
    /*
      If any fields are marked as NULL this will never match another row as
      to NULL never matches anything else including another NULL.
     */
    return HA_EXIT_SUCCESS;
  }

  const common::Slice new_slice =
      common::Slice((const char *)m_sk_packed_tuple, size);

  /*
    For UPDATEs, if the key has changed, we need to obtain a lock. INSERTs
    always require locking.
  */
  if (row_info.old_data != nullptr) {
    size = kd.pack_record(table, m_pack_buffer, row_info.old_data,
                          m_sk_packed_tuple_old, nullptr, false,
                          0, user_defined_key_parts);
    assert(size <= m_max_packed_sk_len);
    const common::Slice old_slice =
        common::Slice((const char *)m_sk_packed_tuple_old, size);

    /*
      For updates, if the keys are the same, then no lock is needed

      Also check to see if the key has any fields set to NULL. If it does, then
      this key is unique since NULL is not equal to each other, so no lock is
      needed.
    */
    if (!SePrimaryKeyComparator::bytewise_compare(new_slice, old_slice)) {
      return HA_EXIT_SUCCESS;
    }
  }

  /*
    Perform a read to determine if a duplicate entry exists - since this is
    a secondary indexes a range scan is needed.

    note: we intentionally don't set options.snapshot here. We want to read
    the latest committed data.
  */

  const bool all_parts_used = (user_defined_key_parts == kd.get_key_parts());

  /*
    This iterator seems expensive since we need to allocate and free
    memory for each unique index.

    If this needs to be optimized, for keys without NULL fields, the
    extended primary key fields can be migrated to the value portion of the
    key. This enables using Get() instead of Seek() as in the primary key
    case.

    The bloom filter may need to be disabled for this lookup.
  */
  const bool total_order_seek = !can_use_bloom_filter(
      ha_thd(), kd, new_slice, all_parts_used,
      is_ascending(*m_key_descr_arr[key_id], HA_READ_KEY_EXACT));
  const bool fill_cache = true; // !THDVAR(ha_thd(), skip_fill_cache);

  /*
    psergey-todo: we just need to take lock, lookups not needed:
  */
  //std::string dummy_value;
  assert(kd.m_is_reverse_cf == false);
  const common::Status s =
      lock_unique_key(row_info.tx, kd.get_cf(), new_slice, total_order_seek, fill_cache);
  if (!s.ok() && !s.IsNotFound()) {
    __HANDLER_LOG(WARN, "DML: lock_unique_key for key(%s) on index:%d failed with error:%s, table_name: %s",
                  new_slice.ToString(true).c_str(), kd.get_index_number(), s.ToString().c_str(), table->s->table_name.str);
    return row_info.tx->set_status_error(table->in_use, s, kd, m_tbl_def.get());
  }

  IteratorUptr iter(row_info.tx->get_iterator(kd.get_cf(), total_order_seek,
      fill_cache, true /* read current data */, false /* acquire snapshot */));
  /*
    Need to scan the transaction to see if there is a duplicate key.
    Also need to scan se and verify the key has not been deleted
    in the transaction.
  */
  iter->Seek(new_slice);
  *found = (HA_EXIT_SUCCESS == read_key_exact(kd, iter.get(), all_parts_used, new_slice));

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::check_uniqueness_and_lock(
    const struct update_row_info &row_info,
    bool *const pk_changed)
{
  int rc = HA_EXIT_SUCCESS;
  /*
    Go through each index and determine if the index has uniqueness
    requirements. If it does, then try to obtain a row lock on the new values.
    Once all locks have been obtained, then perform the changes needed to
    update/insert the row.
  */
  for (uint key_id = 0; key_id < m_tbl_def->m_key_count; key_id++) {
    bool found;

    if (is_pk(key_id, table, m_tbl_def.get())) {
      rc = check_and_lock_unique_pk(key_id, row_info, &found, pk_changed);
    } else {
      rc = check_and_lock_sk(key_id, row_info, &found);
    }

    if (rc != 0) {
      return rc;
    }

    if (found) {
      /* There is a row with this key already, so error out. */
      errkey = key_id;
      m_dupp_errkey = errkey;
      return HA_ERR_FOUND_DUPP_KEY;
    }
  }

  for (auto &k : m_tbl_def->m_added_key) {
    const SeKeyDef &kd = *k.first;
    TABLE *atab = k.second.altered_table;
    if (table->s->primary_key != atab->s->primary_key) {
      continue;
    }

    bool found = false;
    rc = lock_and_check_new_index_sk(kd, k.second, row_info, &found);

    if (rc != HA_EXIT_SUCCESS){
      __HANDLER_LOG(WARN, "SEDDL: check duplicated key failed for new added key %u, errcode is %d, table_name:%s",
                    kd.get_index_number(), rc, table->s->table_name.str);
      break;
    }
  }

  return rc;
}

int ha_smartengine::bulk_load_key(SeTransaction *const tx,
                                  const SeKeyDef &kd,
                                  const common::Slice &key,
                                  const common::Slice &value,
                                  int level)
{
  db::ColumnFamilyHandle *const cf = kd.get_cf();
  assert(cf != nullptr);

  if (mtables_ == nullptr) {
    mtables_ = std::make_shared<db::MiniTables>();
    m_change_info = std::make_unique<storage::ChangeInfo>();
    mtables_->change_info_ = m_change_info.get();
    mtables_->level = level;

  }

  if (m_sst_info == nullptr) {
    m_sst_info = std::make_shared<SeSstInfo>(
        se_db, m_table_handler->m_table_name, kd.get_name(), cf,
        se_db_options, false/*THDVAR(ha_thd(), trace_sst_api)*/, mtables_.get());
    tx->start_bulk_load(this);
    m_bulk_load_tx = tx;
  }

  assert(m_sst_info != nullptr);

  int rc = m_sst_info->put(key, value);
  if (rc != 0) {
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Failed to add a key to sst file writer(%s)", MYF(0),
                    m_sst_info->error_message().c_str());
    rc = HA_ERR_INTERNAL_ERROR;
  }

  return rc;
}

common::Status ha_smartengine::delete_or_singledelete(
    uint index,
    SeTransaction *const tx,
    db::ColumnFamilyHandle *const column_family,
    const common::Slice &key)
{
  if (can_use_single_delete(index))
    return tx->single_delete(column_family, key);
  return tx->delete_key(column_family, key);
}

/*
  @brief
    Calculate (if needed) the bitmap of indexes that are modified by the
    current query

  @detail
    The calculation done by checking index definitions against the
  table->write_set
    column bitmap.
*/
int ha_smartengine::calc_updated_indexes()
{
  if (!m_update_scope_is_valid) {
    m_update_scope_is_valid = true;
    m_update_scope.clear_all();

    for (uint keynr = 0; keynr < table->s->keys; keynr++) {
      const SeKeyDef &kd = *m_key_descr_arr[keynr];
      /* Walk over all key parts, including the "extended key" suffix */
      const uint key_parts = kd.get_key_parts();
      for (uint kp = 0; kp < key_parts; kp++) {
        if (has_hidden_pk(table) && kp + 1 == key_parts)
          break;

        Field *const field = kd.get_table_field_for_part_no(table, kp);
        if (bitmap_is_set(table->write_set, field->field_index())) {
          m_update_scope.set_bit(keynr);
          break;
        }
      }
    }

    /* set update-bit for added indexes during inplace-norebuild ddl */
    for (auto &k : m_tbl_def->m_added_key) {
      const SeKeyDef &kd = *k.first;
      TABLE *atab = k.second.altered_table;
      /* Walk over all key parts, including the "extended key" suffix */
      const uint key_parts = kd.get_key_parts();
      for (uint kp = 0; kp < key_parts; kp++) {
        if (has_hidden_pk(atab) && kp + 1 == key_parts)
          break;

        Field *const field = kd.get_table_field_for_part_no(atab, kp);
        if (field == nullptr) {
          HANDLER_LOG(ERROR, "table object is mismatch with m_tbl_def");
          return HA_EXIT_FAILURE;
        } else if (bitmap_is_set(table->write_set, field->field_index())) {
          m_update_scope.set_bit(kd.get_keyno());
          break;
        }
      }
    }

    /* set update-bit for added indexes during inplace-rebuild ddl */
    for (auto &k : m_tbl_def->m_inplace_new_keys) {
      const SeKeyDef &kd = *k.first;
      TABLE *atab = k.second.altered_table;
      /* Walk over all key parts, including the "extended key" suffix */
      const uint key_parts = kd.get_key_parts();
      for (uint kp = 0; kp < key_parts; kp++) {
        if (has_hidden_pk(atab) && kp + 1 == key_parts)
          break;

        Field *const field = kd.get_table_field_for_part_no(atab, kp);
        if (field == nullptr) {
          HANDLER_LOG(ERROR, "table object is mismatch with m_tbl_def");
          return HA_EXIT_FAILURE;
        }

        /*
           field in old table be modified should also set bitmap
           if field is still exists in the altered table.
        */
        uint *col_map = m_tbl_def->m_dict_info->m_col_map;
        uint old_field_index = col_map[field->field_index()];

        if (old_field_index != uint(-1) &&
            bitmap_is_set(table->write_set, old_field_index)) {
          m_update_scope.set_bit(kd.get_keyno());
          break;
        }
      }
    }

    // it may be possible to set m_update_scope for the index to be added,
    // but it should be after calling kd.setup(). Skip it now for simplicity,
    // and suppose there is little performance drop.
  }

  return HA_EXIT_SUCCESS;
}



void ha_smartengine::read_thd_vars(THD *const thd)
{
  m_store_row_debug_checksums = false; // THDVAR(thd, store_row_debug_checksums);
  m_verify_row_debug_checksums = false; // THDVAR(thd, verify_row_debug_checksums);
  m_checksums_pct = 100; // THDVAR(thd, checksums_pct);

#ifndef NDEBUG
  m_store_row_debug_checksums = true;
  m_verify_row_debug_checksums = true;
#endif
}

const char *ha_smartengine::thd_se_tmpdir()
{
  const char *tmp_dir = nullptr; // THDVAR(ha_thd(), tmpdir);

  /*
    We want to treat an empty string as nullptr, in these cases DDL operations
    will use the default --tmpdir passed to mysql instead.
  */
  if (tmp_dir != nullptr && *tmp_dir == '\0') {
    tmp_dir = nullptr;
  }

  return (tmp_dir);
}

/**
  Checking if an index is used for ascending scan or not

  @detail
  Currently se does not support bloom filter for
  prefix lookup + descending scan, but supports bloom filter for
  prefix lookup + ascending scan. This function returns true if
  the scan pattern is absolutely ascending.
  @param kd
  @param find_flag
*/
bool ha_smartengine::is_ascending(const SeKeyDef &kd,
                                  enum ha_rkey_function find_flag) const
{
  bool is_ascending;
  switch (find_flag) {
  case HA_READ_KEY_EXACT: {
    is_ascending = !kd.m_is_reverse_cf;
    break;
  }
  case HA_READ_PREFIX: {
    is_ascending = true;
    break;
  }
  case HA_READ_KEY_OR_NEXT:
  case HA_READ_AFTER_KEY: {
    is_ascending = !kd.m_is_reverse_cf;
    break;
  }
  case HA_READ_KEY_OR_PREV:
  case HA_READ_BEFORE_KEY:
  case HA_READ_PREFIX_LAST:
  case HA_READ_PREFIX_LAST_OR_PREV: {
    is_ascending = kd.m_is_reverse_cf;
    break;
  }
  default:
    is_ascending = false;
  }
  return is_ascending;
}

void ha_smartengine::rpl_before_delete_rows()
{
  DBUG_ENTER_FUNC();

  m_in_rpl_delete_rows = true;

  DBUG_VOID_RETURN;
}

void ha_smartengine::rpl_after_delete_rows()
{
  DBUG_ENTER_FUNC();

  m_in_rpl_delete_rows = false;

  DBUG_VOID_RETURN;
}

void ha_smartengine::rpl_before_update_rows()
{
  DBUG_ENTER_FUNC();

  m_in_rpl_update_rows = true;

  DBUG_VOID_RETURN;
}

void ha_smartengine::rpl_after_update_rows()
{
  DBUG_ENTER_FUNC();

  m_in_rpl_update_rows = false;

  DBUG_VOID_RETURN;
}

/**
  @brief
  Read Free Replication can be used or not. Returning False means
  Read Free Replication can be used. Read Free Replication can be used
  on UPDATE or DELETE row events, and table must have user defined
  primary key.
*/
bool ha_smartengine::use_read_free_rpl()
{
  DBUG_ENTER_FUNC();
  DBUG_RETURN(false);
}

int ha_smartengine::build_table_schema(const TABLE *table,
                                       const dd::Table *dd_table,
                                       const SeTableDef *table_def,
                                       LEX_CSTRING engine_attribute_str,
                                       schema::TableSchema &table_schema)
{
  int ret = common::Status::kOk;
  int tmp_ret = HA_EXIT_SUCCESS;
  std::shared_ptr<SeKeyDef> primary_index;
  uint32_t null_bitmap_size = 0;
  uint32_t packed_column_count = 0;
  bool has_unpack_info = false;
  SeFieldEncoder *field_encoders = nullptr;
  std::vector<READ_FIELD> field_decoders;
  InstantDDLInfo instant_ddl_info;
  schema::ColumnSchema column_schema;
  
  if (IS_NULL(table) || IS_NULL(dd_table) || IS_NULL(table_def)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(table), KP(dd_table), KP(table_def));
  } else if (HA_EXIT_SUCCESS != (tmp_ret = get_instant_ddl_info_if_needed(table, dd_table, instant_ddl_info))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to get instant ddl info", K(ret));
  } else if (IS_NULL((primary_index = table_def->m_key_descr_arr[pk_index(table, table_def)]).get())) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "primary index must not been nullptr", K(ret),
        "primary key index", pk_index(table, table_def));
  } else if (HA_EXIT_SUCCESS != (tmp_ret = setup_field_converters(table,
                                                                  primary_index,
                                                                  field_encoders,
                                                                  packed_column_count,
                                                                  null_bitmap_size,
                                                                  has_unpack_info))) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to setup field converters", K(ret));
  } else if (HA_EXIT_SUCCESS != (tmp_ret = setup_read_decoders(table,
                                                               field_encoders,
                                                               instant_ddl_info,
                                                               true /**force_decode_all_fields*/,
                                                               field_decoders))) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to setup field decoders", K(ret));
  } else if (FAILED(table_schema.get_engine_attribute().parse(std::string(engine_attribute_str.str, engine_attribute_str.length)))) {
    SE_LOG(WARN, "fail to parse engine attribute", K(ret));
  } else {
    table_schema.set_database_name(table_def->base_dbname());
    table_schema.set_primary_index_id(primary_index->get_index_number());
    table_schema.set_packed_column_count(packed_column_count);
      
    /**primary key column*/
    //TODO:Zhao Dongsheng, we think that primary column is unfixed now,
    //and used 4 Bytes space to store data size
    column_schema.set_type(schema::ColumnType::PRIMARY_COLUMN);
    column_schema.set_data_size_bytes(4);
    table_schema.get_column_schemas().push_back(column_schema);

    /**Record header column*/
    column_schema.reset();
    column_schema.set_type(schema::ColumnType::RECORD_HEADER);
    column_schema.set_column_count(table->s->fields);
    column_schema.set_null_bitmap_size(null_bitmap_size);
    if (instant_ddl_info.have_instantly_added_columns()) {
      column_schema.set_instant();
      column_schema.set_original_column_count(instant_ddl_info.m_instant_cols);
      column_schema.set_original_null_bitmap_size(instant_ddl_info.m_null_bytes);
    }
    table_schema.get_column_schemas().push_back(column_schema);

    /**Null bitmap column*/
    if (null_bitmap_size > 0) {
      column_schema.reset();
      column_schema.set_type(schema::ColumnType::NULL_BITMAP);
      table_schema.get_column_schemas().push_back(column_schema);
    }

    /**Unpack info column*/
    if (has_unpack_info) {
      column_schema.reset();
      column_schema.set_type(schema::ColumnType::UNPACK_INFO);
      table_schema.set_unpack();
    }
      
    if (FAILED(build_column_schemas(table, field_decoders, table_schema.get_column_schemas()))) {
      SE_LOG(WARN, "fail to build column schemas", K(ret));
    } else {
      SE_LOG(INFO, "success to build table schema for table", "database_name", table_def->base_dbname(),
          "table_name", table_def->base_tablename());
    }
  }

  if (nullptr != field_encoders) {
    my_free(field_encoders);
    field_encoders = nullptr;
  }

  return ret;
}

int ha_smartengine::build_column_schemas(const TABLE *table,
                                         const std::vector<READ_FIELD> &field_decoders,
                                         schema::ColumnSchemaArray &column_schemas)
{
  int ret = common::Status::kOk;
  SeFieldEncoder *field_encoder = nullptr;
  Field *field = nullptr;
  schema::ColumnSchema column_schema;

  if (IS_NULL(table)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(table));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < field_decoders.size(); ++i) {
      column_schema.reset();
      if (IS_NULL(field_encoder = field_decoders[i].m_field_enc)) {
        ret = common::Status::kErrorUnexpected;
        SE_LOG(WARN, "field encoder must not been nullptr", K(ret), K(i));
      } else if (IS_NULL(field = table->field[field_encoder->m_field_index])) {
        ret = common::Status::kErrorUnexpected;
        SE_LOG(WARN, "field must not been nullptr", K(ret), K(i),
            "field_index", field_encoder->m_field_index);
      } else {
        if (field_encoder->maybe_null()) {
          column_schema.set_nullable();
          column_schema.set_null_offset(field_encoder->m_null_offset);
          column_schema.set_null_mask(field_encoder->m_null_mask);
        }

        if (MYSQL_TYPE_BLOB == field_encoder->m_field_type || MYSQL_TYPE_JSON == field_encoder->m_field_type) {
          column_schema.set_type(schema::ColumnType::BLOB_COLUMN);
          Field_blob *field_blob = reinterpret_cast<Field_blob *>(field);
          column_schema.unset_fixed();
          column_schema.set_data_size_bytes(field_blob->pack_length() - portable_sizeof_char_ptr);
        } else if (MYSQL_TYPE_VARCHAR == field_encoder->m_field_type) {
          column_schema.set_type(schema::ColumnType::VARCHAR_COLUMN);
          Field_varstring *field_varstr = reinterpret_cast<Field_varstring *>(field);
          column_schema.unset_fixed();
          column_schema.set_data_size_bytes(field_varstr->get_length_bytes());
        } else if (MYSQL_TYPE_STRING == field_encoder->m_field_type) {
          column_schema.set_type(schema::ColumnType::STRING_COLUMN); 
          Field_string *field_str = reinterpret_cast<Field_string *>(field);
          // The storage format for the MYSQL_TYPE_STRING type varies depending
          // on whether the character set is compatible with ASCII.For details,
          // you can refer to the function "append_string_to_storage_format".
          // Special case: String with zero length is treated as fixed-length type.
          if (field_str->charset()->mbminlen > 1 || (0 == field->pack_length_in_rec())) {
            column_schema.set_fixed();
            column_schema.set_data_size(field->pack_length_in_rec());
          } else {
            column_schema.unset_fixed();
            column_schema.set_data_size_bytes(schema::RecordFormat::MYSQL_STRING_SIZE_BYTES);
          }
        } else {
          column_schema.set_type(schema::ColumnType::FIXED_COLUMN);
          column_schema.set_fixed();
          column_schema.set_data_size(field_encoder->m_pack_length_in_rec);
        }

        column_schemas.push_back(column_schema);
      }
    }
  }

  return ret;
}

int ha_smartengine::pushdown_table_schema(const TABLE *table,
                                          const dd::Table *dd_table,
                                          const SeTableDef *table_def,
                                          LEX_CSTRING engine_attribute_str)
{
  int ret = common::Status::kOk;
  db::ColumnFamilyHandle *subtable_handle = nullptr;
  schema::TableSchema table_schema;
  int64_t dummy_commit_lsn = 0;
  const char *index_name = nullptr;

  if (IS_NULL(table) || IS_NULL(dd_table) || IS_NULL(table_def)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(table), KP(dd_table), KP(table_def));
  } else if (FAILED(build_table_schema(table,
                                       dd_table,
                                       table_def,
                                       engine_attribute_str,
                                       table_schema))) {
    SE_LOG(WARN, "fail to build table schema", K(ret));
  } else if (FAILED(storage::StorageLogger::get_instance().begin(storage::SeEvent::MODIFY_INDEX))) {
    SE_LOG(WARN, "fail to begin slog trans", K(ret));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < table_def->m_key_count; ++i) {
      if (IS_NULL(subtable_handle = table_def->m_key_descr_arr[i]->get_cf())) {
        ret = common::Status::kErrorUnexpected;
        SE_LOG(WARN, "the subtable handle must not be nullptr", K(ret), K(i));
      } else if (IS_NULL(index_name = get_key_name(i, table, table_def))) {
        ret = common::Status::kErrorUnexpected;
        SE_LOG(WARN, "the index name must not be nullptr", K(ret), K(i), K(table_schema));
      } else {
        table_schema.set_index_id(subtable_handle->GetID());
        if (FAILED(se_db->modify_table_schema(subtable_handle, table_schema))) {
          SE_LOG(WARN, "fail to modify table schema", K(ret), K(i), K(table_schema));
        } else {
          SE_LOG(INFO, "success to push down table schema", "full_tablename", table_def->full_tablename(),
              "base_dbname", table_def->base_dbname(), "base_tablename", table_def->base_tablename(),
              "index_name", index_name, "table_id", table_def->get_table_id(), K(table_schema));
        }
      }
    }

    if (SUCCED(ret)) {
      if (FAILED(storage::StorageLogger::get_instance().commit(dummy_commit_lsn))) {
        SE_LOG(WARN, "fail to commit slog transaction", K(ret));
        storage::StorageLogger::get_instance().abort();
      }
    } else {
      storage::StorageLogger::get_instance().abort();
    }
  }

  return ret;
}

} //namespace smartengine
