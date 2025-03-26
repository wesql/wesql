/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012,2013 Monty Program Ab

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
#include "dd/dictionary.h"
#include "se_dd_operations.h"
#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#include "se_ddl_manager.h"
#include <algorithm>
#include <utility>
#include "sql/field.h"
#include "sql/mysqld.h"
#include "sql/sql_table.h"
#include "sql/table.h"
#include "sql/sql_class.h"
#include "sql/thd_raii.h"      // Disable_autocommit_guard
#include "sql/transaction.h"   // trans_commit
#include "dict/se_cf_manager.h"
#include "dict//se_log_ddl.h"
#include "dict/se_dict_manager.h"
#include "dict/se_table.h"
#include "dict/se_dict_util.h"
#include "handler/se_hton.h"
#include "transactions/transaction_db_impl.h"
#include "util/se_utils.h"
#include "db/column_family.h"

namespace smartengine
{

SeDdlManager::SeDdlManager() : m_rwlock(),
                               m_dict(nullptr),
                               m_ddl_hash(),
                               m_index_num_to_keydef(),
                               m_index_num_to_uncommitted_keydef(),
                               m_stats2store(),
                               m_next_index_id(0),
                               m_next_table_id(0)
{}

bool SeDdlManager::init(THD *const thd, SeDictionaryManager *const dict_arg)
{
  m_dict = dict_arg;
  mysql_rwlock_init(0, &m_rwlock);

  uint max_index_id_in_dict = 0;
  m_dict->get_max_index_id(&max_index_id_in_dict);
  // index ids used by applications should not conflict with
  // data dictionary index ids
  if (max_index_id_in_dict < SeKeyDef::END_DICT_INDEX_ID) {
    max_index_id_in_dict = SeKeyDef::END_DICT_INDEX_ID;
  }

  m_next_table_id = DD_TABLE_ID_END + 1;

  // get value of system_cf_version
  uint16_t system_cf_version_in_dict;
  if (!m_dict->get_system_cf_version(&system_cf_version_in_dict)) {
    system_cf_version_in_dict = SeKeyDef::SYSTEM_CF_VERSION::VERSION_0;
  }
  // if we can't get value for system_cf_version from dictionary or we get older
  // version, we do upgrade on SE dictionary.
  switch (system_cf_version_in_dict) {
    // For upgrading to VERSION_1, all existing SE tables are upgraded by
    // 1) load all existing tables according to DDL_ENTRY_INDEX_START_NUMBER
    //    records in system cf
    // 2) try to aquire dd::Table object for each of them and set se_private_id
    //    and se_private_data in dd::Table/dd::Index
    // 3) persist dd::Table/dd:Index object to DDSE
    // 4) set max_table_id in system cf even there is no user defined table
    // 5) set VERSION_1 as system_cf_version in dictionary
    case SeKeyDef::SYSTEM_CF_VERSION::VERSION_0: {
      if (load_existing_tables(max_index_id_in_dict)) {
        HANDLER_LOG(ERROR, "SE: failed to load existing tables!");
        return true;
      } else if (upgrade_system_cf_version1(thd)) {
        HANDLER_LOG(ERROR, "SE: failed to upgrade system_cf to VERSION1");
        return true;
      } else if (upgrade_system_cf_version2()) {
        HANDLER_LOG(ERROR, "SE: failed to upgrade system_cf to VERSION2");
        return true;
      }
      break;
    }
    case SeKeyDef::SYSTEM_CF_VERSION::VERSION_1:
    case SeKeyDef::SYSTEM_CF_VERSION::VERSION_2: {
      uint64_t max_table_id_in_dict = 0;
      if (!m_dict->get_max_table_id(&max_table_id_in_dict)) {
        HANDLER_LOG(ERROR, "SE: failed to get max_table_id from dictionary");
        return true;
      }
      /** for version8018. there is OOM risk when populating all tables if table count is very large
      *   but this will cause duplicate key error when recovery or ha
      *   8032 has fixed this problem，avoid using too much memory: https://github.com/mysql/mysql-server/commit/ce8bcc334189fa791a1934604bcc198d08e1e5d8
      *   so populating all tables is safe for memory
      * */
      else if (!thd || populate_existing_tables(thd)) {
        HANDLER_LOG(ERROR, "SE: failed to populate existing tables!");
        return true;
      }

#ifndef NDEBUG
      HANDLER_LOG(INFO, "ddl_manager init get max table id from dictionary",
                   K(max_table_id_in_dict));
#endif
      if (max_table_id_in_dict > DD_TABLE_ID_END)
        m_next_table_id = max_table_id_in_dict + 1;

      if (SeKeyDef::SYSTEM_CF_VERSION::VERSION_1 == system_cf_version_in_dict
          && upgrade_system_cf_version2()) {
        HANDLER_LOG(ERROR, "SE: failed to upgrade system_cf to VERSION2");
        return true;
      }
      break;
    }
    default: {
      HANDLER_LOG(ERROR, "SE: unexpected value for system_cf_version",
                   K(system_cf_version_in_dict));
      return true;
    }
  }

  m_next_index_id = max_index_id_in_dict + 1;
  return false;
}

void SeDdlManager::cleanup()
{
  m_ddl_hash.clear();
  mysql_rwlock_destroy(&m_rwlock);
}

void SeDdlManager::reset()
{
  // TODO(Zhao Dongsheng): reset the table cache and repopulate it from data dictionary.
  mysql_rwlock_wrlock(&m_rwlock);
  m_ddl_hash.clear();
  mysql_rwlock_unlock(&m_rwlock);
}

/* TODO:
  This function modifies m_ddl_hash and m_index_num_to_keydef.
  However, these changes need to be reversed if dict_manager.commit fails
  See the discussion here: https://reviews.facebook.net/D35925#inline-259167
  Tracked by https://github.com/facebook/mysql-5.6/issues/33
*/
void SeDdlManager::put(const std::shared_ptr<SeTableDef>& tbl, bool lock/* = true*/)
{
  const std::string &dbname_tablename = tbl->full_tablename();

  if (lock)
    mysql_rwlock_wrlock(&m_rwlock);

  // We have to do this find because 'tbl' is not yet in the list.  We need
  // to find the one we are replacing ('rec')
  const auto &it = m_ddl_hash.find(dbname_tablename);
  if (it != m_ddl_hash.end()) {
    m_ddl_hash.erase(it);
  }
  m_ddl_hash.insert({dbname_tablename, tbl});

  for (uint keyno = 0; keyno < tbl->m_key_count; keyno++) {
    m_index_num_to_keydef[tbl->m_key_descr_arr[keyno]->get_gl_index_id()] =
        std::make_pair(dbname_tablename, keyno);
  }

  if (lock)
    mysql_rwlock_unlock(&m_rwlock);
}

/**
  Put table definition of `tbl` into the mapping, and also write it to the
  on-disk data dictionary.

  @param tbl, se_tbl_def put to cache
  @param batch, transaction buffer on current session
  @param ddl_log_manager
  @param thread_id, session thread_id
  @param write_ddl_log, for create-table, we need write remove_cache log to remove rubbish, for alter-table, we just invalid cache, make sure cache is consistent with dictionary.
*/
int SeDdlManager::put_and_write(const std::shared_ptr<SeTableDef>& tbl,
                                db::WriteBatch *const batch,
                                SeDdlLogManager *const ddl_log_manager,
                                ulong thread_id,
                                bool write_ddl_log)
{
  const std::string &dbname_tablename = tbl->full_tablename();

  if (write_ddl_log) {
    if (ddl_log_manager->write_remove_cache_log(batch,
                                                dbname_tablename,
                                                thread_id)) {
      sql_print_error(
          "write remove cache ddl_log error, table_name(%s), thread_id(%d)",
          dbname_tablename.c_str(), thread_id);
      return HA_EXIT_FAILURE;
    } else {
      put(tbl);
    }
  } else {
    remove_cache(dbname_tablename);
  }

  return HA_EXIT_SUCCESS;
}

void SeDdlManager::remove_cache(const std::string &dbname_tablename, bool lock/* = true */)
{
  if (lock) {
    mysql_rwlock_wrlock(&m_rwlock);
  }

  const auto &it = m_ddl_hash.find(dbname_tablename);
  if (it != m_ddl_hash.end()) {
    // m_index_num_to_keydef is inserted during put
    // we should remove here not other place
    if (it->second && it->second->m_key_descr_arr) {
      for (uint keyno = 0; keyno < it->second->m_key_count; keyno++) {
        auto kd = it->second->m_key_descr_arr[keyno];
        assert(kd);
        m_index_num_to_keydef.erase(kd->get_gl_index_id());
      }
    }
    m_ddl_hash.erase(it);
  }

  if (lock) {
    mysql_rwlock_unlock(&m_rwlock);
  }
}

bool SeDdlManager::rename_cache(const std::string &from, const std::string &to)
{
  std::shared_ptr<SeTableDef> tbl;
  bool from_dict = false;
  if (!(tbl = find(from, &from_dict))) {
    /** if not found, that's ok for we may executed many times */
    HANDLER_LOG(WARN, "Table doesn't exist when rename_cache", K(from), K(to));
    return false;
  }

  return rename_cache(tbl.get(), to);
}

bool SeDdlManager::rename_cache(SeTableDef* tbl, const std::string &to)
{
  assert(nullptr != tbl);
  auto new_tbl = std::make_shared<SeTableDef>(to);

  new_tbl->m_key_count = tbl->m_key_count;
  new_tbl->m_auto_incr_val =
      tbl->m_auto_incr_val.load(std::memory_order_relaxed);
  new_tbl->m_key_descr_arr = tbl->m_key_descr_arr;
  // so that it's not free'd when deleting the old rec
  tbl->m_key_descr_arr = nullptr;

  mysql_rwlock_wrlock(&m_rwlock);
  /** update dictionary cache */
  put(new_tbl, false);
  remove_cache(tbl->full_tablename(), false);

  mysql_rwlock_unlock(&m_rwlock);

  DBUG_EXECUTE_IF("ddl_log_inject_rollback_rename_process",{return true;});
  return false;
}

std::shared_ptr<SeTableDef> SeDdlManager::find(const std::string &table_name,
                                               bool* from_dict,
                                               bool lock/* = true*/)
{
  assert(!table_name.empty() && nullptr != from_dict);
  std::shared_ptr<SeTableDef> tbl = find_tbl_from_cache(table_name, lock);
  *from_dict = false;
  // for rename_cache during ha_post_recover, current_thd is nullptr
  if (nullptr == tbl && nullptr != current_thd) {
    // SeTableDef* tbl_def = find_tbl_from_dict(table_name);
    // try to acquire dd::Table object from dictionary and
    // restore SeTableDef from dd::Table
    SeTableDef* tbl_def = restore_table_from_dd(current_thd, table_name);
    if (nullptr != tbl_def) {
      *from_dict = true;
      tbl.reset(tbl_def);
    }
  }

  return tbl;
}

std::shared_ptr<SeTableDef> SeDdlManager::find(const char *table_name,
                                               int64_t name_len,
                                               bool* from_dict,
                                               bool lock/* = true*/)
{
  return find(std::string(table_name, name_len), from_dict, lock);
}

std::shared_ptr<SeTableDef> SeDdlManager::find(const dd::Table* dd_table,
                                               const std::string& table_name,
                                               bool* from_dict,
                                               bool lock/* = true*/)
{
  assert(!table_name.empty() && nullptr != from_dict);
  std::shared_ptr<SeTableDef> tbl = find_tbl_from_cache(table_name, lock);
  *from_dict = false;
  if (nullptr == tbl) {
    assert(nullptr != dd_table);
    // restore SeTableDef from dd::Table
    SeTableDef* tbl_def = restore_table_from_dd(dd_table, table_name);
    if (nullptr != tbl_def) {
      *from_dict = true;
      tbl.reset(tbl_def);
    }
  }
  return tbl;
}

std::shared_ptr<SeTableDef> SeDdlManager::find(THD* thd,
                                               const std::string& table_name,
                                               bool* from_dict,
                                               bool lock/* = true*/)
{
  assert(!table_name.empty() && nullptr != from_dict);
  std::shared_ptr<SeTableDef> tbl = find_tbl_from_cache(table_name, lock);
  *from_dict = false;
  if (nullptr == tbl) {
    assert(nullptr != thd);
    // try to acquire dd::Table object from dictionary and
    // restore SeTableDef from dd::Table
    SeTableDef* tbl_def = restore_table_from_dd(thd, table_name);
    if (nullptr != tbl_def) {
      *from_dict = true;
      tbl.reset(tbl_def);
    }
  }
  return tbl;
}

int SeDdlManager::alloc_index_id(uint64_t &index_id)
{
  assert(nullptr != m_dict);
  int ret = common::Status::kOk;

  mysql_rwlock_wrlock(&m_rwlock);
  if (FAILED(update_max_index_id(m_next_index_id))) {
    HANDLER_LOG(ERROR, "SE: failed to update max index id", K(ret), K(m_next_index_id));
  } else {
    index_id = m_next_index_id++;
  }
  mysql_rwlock_unlock(&m_rwlock);

  return ret;
}

bool SeDdlManager::get_table_id(uint64_t &table_id)
{
  bool res = true;
  mysql_rwlock_wrlock(&m_rwlock);
  if (!(res = update_max_table_id(m_next_table_id))) {
    table_id = m_next_table_id++;
  }
  mysql_rwlock_unlock(&m_rwlock);
  return res;
}

void SeDdlManager::add_uncommitted_keydefs(const std::unordered_set<std::shared_ptr<SeKeyDef>> &indexes)
{
  mysql_rwlock_wrlock(&m_rwlock);
  for (const auto &index : indexes) {
    m_index_num_to_uncommitted_keydef[index->get_gl_index_id()] = index;
  }
  mysql_rwlock_unlock(&m_rwlock);
}

void SeDdlManager::remove_uncommitted_keydefs(const std::unordered_set<std::shared_ptr<SeKeyDef>> &indexes)
{
  mysql_rwlock_wrlock(&m_rwlock);
  for (const auto &index : indexes) {
    m_index_num_to_uncommitted_keydef.erase(index->get_gl_index_id());
  }
  mysql_rwlock_unlock(&m_rwlock);
}

void SeDdlManager::adjust_stats(SeIndexStats stats)
{
  mysql_rwlock_wrlock(&m_rwlock);
  const auto &keydef = find(stats.m_gl_index_id);
  if (keydef) {
    keydef->m_stats.m_distinct_keys_per_prefix.resize(keydef->get_key_parts());
    keydef->m_stats.update(stats, keydef->max_storage_fmt_length());
    m_stats2store[keydef->m_stats.m_gl_index_id] = keydef->m_stats;
  }

  const bool should_save_stats = !m_stats2store.empty();
  mysql_rwlock_unlock(&m_rwlock);
  if (should_save_stats) {
    // Queue an async persist_stats(false) call to the background thread.
    se_queue_save_stats_request();
  }
}

void SeDdlManager::persist_stats(const bool &sync)
{
  mysql_rwlock_wrlock(&m_rwlock);
  const auto local_stats2store = std::move(m_stats2store);
  m_stats2store.clear();
  mysql_rwlock_unlock(&m_rwlock);

  // Persist stats
  const std::unique_ptr<db::WriteBatch> wb = m_dict->begin();
  std::vector<SeIndexStats> stats;
  std::transform(local_stats2store.begin(), local_stats2store.end(),
                 std::back_inserter(stats),
                 [](const std::pair<GL_INDEX_ID, SeIndexStats> &s) {
                   return s.second;
                 });
  m_dict->add_stats(wb.get(), stats);
  m_dict->commit(wb.get(), sync);
}

// Check whethter we can safely purge given subtable.
// We only can safely purge it if
//   (1) no related SeKeyDef object in uncommited hash table;
//   (2) no associated dd::Table object from global data dictionary.
bool SeDdlManager::can_purge_subtable(THD* thd, const GL_INDEX_ID& gl_index_id)
{
  if (m_index_num_to_uncommitted_keydef.find(gl_index_id) !=
      m_index_num_to_uncommitted_keydef.end())
    return false;

  uint32_t subtable_id = gl_index_id.index_id;
  auto it = m_index_num_to_keydef.find(gl_index_id);
  if (it != m_index_num_to_keydef.end()) {
    auto& tbl_name = it->second.first;
    uint keyno = it->second.second;
    // Normally, SeKeyDef object always exists with SeTableDef object together
    std::shared_ptr<SeTableDef> table_def = find_tbl_from_cache(tbl_name);
    if (!table_def) {
      /* Exception case 1:
       *   SeTableDef object with the name is removed but index cache entry of
       *   SeKeyDef in the SeTableDef is kept.
       *   The SeTableDef object may be loaded again later.
       */
      HANDLER_LOG(ERROR, "SE: unexpected error, key cache entry exists without table cache entry",
                  "table_name", tbl_name);
      return false;
    } else if (!table_def->m_key_descr_arr || keyno > table_def->m_key_count ||
               !table_def->m_key_descr_arr[keyno] ||
               subtable_id != table_def->m_key_descr_arr[keyno]->get_index_number()) {
      /* Exception case 2:
       *   old SeTableDef object with the name is removed and new
       *   SeTableDef object is added but index cache entry of SeKeyDef in
       *   old SeTableDef is kept
       */
      HANDLER_LOG(ERROR, "SE: found invalid index_id in m_index_num_to_keydef "
                  "without related SeTableDef/SeKeyDef object", K(subtable_id));
      m_index_num_to_keydef.erase(it);
      return true;
    }

    // SeKeyDef object is still present with SeTableDef object in cache
    // name string in SeTableDef is from filename format, to acquire dd object
    // we should use table name format which uses different charset
    char schema_name[FN_REFLEN+1], table_name[FN_REFLEN+1];
    filename_to_tablename(table_def->base_dbname().c_str(), schema_name, sizeof(schema_name));
    filename_to_tablename(table_def->base_tablename().c_str(), table_name, sizeof(table_name));
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    const dd::Table *dd_table = nullptr;
    MDL_ticket *tbl_ticket = nullptr;
    // try to acquire and check existence of dd::Table object
    if (SeDdHelper::acquire_se_table(
            thd, purge_acquire_lock_timeout, schema_name,
            table_name, dd_table, tbl_ticket)) {
      // if failed to acquire dd::Table object we treat as lock wait timeout
      // and related dd::Table is locked by other thread
      return false;
    } else if (nullptr != dd_table) {
      HANDLER_LOG(WARN, "SE: subtable associated table is still present in global data dictionary!",
                  K(subtable_id), K(schema_name), K(table_name));
      if (tbl_ticket) dd::release_mdl(thd, tbl_ticket);
      return false;
    } else {
      // SeTableDef is only present in cache
      // remove cache entry
      remove_cache(tbl_name);
      return true;
    }
  }

  return true;
}

int SeDdlManager::scan_for_tables(Se_tables_scanner *const tables_scanner)
{
  assert(tables_scanner != nullptr);
  int ret = 0;
  SeTableDef *rec = nullptr;

  mysql_rwlock_rdlock(&m_rwlock);
  for (const auto &it : m_ddl_hash) {
    ret = tables_scanner->add_table(it.second.get());
    if (ret)
      break;
  }
  mysql_rwlock_unlock(&m_rwlock);

  return ret;
}

bool SeDdlManager::upgrade_system_cf_version1(THD* thd)
{
  if (!m_ddl_hash.empty()) {
    if (nullptr == thd) {
      HANDLER_LOG(ERROR, "SE: unable to upgrade existing tables!");
      return true;
    } else if (upgrade_existing_tables(thd)) {
      HANDLER_LOG(ERROR, "SE: failed to upgrade existing tables!");
      return true;
    }
  }

  uint16_t target_version = SeKeyDef::SYSTEM_CF_VERSION::VERSION_1;
  if (update_max_table_id(m_next_table_id - 1)) {
    HANDLER_LOG(ERROR, "SE: failed to set max_table_id in system cf!");
    return true;
  } else if (update_system_cf_version(target_version)) {
    HANDLER_LOG(ERROR, "SE: failed to set system_cf_version as VERSION1!");
    return true;
  } else {
#ifndef NDEBUG
    HANDLER_LOG(INFO, "SE: successfully upgrade system_cf to VERSION1");
#endif
    return false;
  }
}

bool SeDdlManager::upgrade_system_cf_version2()
{
  auto wb = m_dict->begin();
  if (!wb) {
    HANDLER_LOG(ERROR, "SE: failed to begin wrtiebatch");
    return true;
  }
  auto write_batch = wb.get();

  auto dd_type = SeKeyDef::DDL_DROP_INDEX_ONGOING;
  std::unordered_set<GL_INDEX_ID> gl_index_ids;
  m_dict->get_ongoing_index_operation(&gl_index_ids, dd_type);
  for (auto& gl_index_id : gl_index_ids) {
    m_dict->delete_with_prefix(write_batch, dd_type, gl_index_id);
    m_dict->delete_index_info(write_batch, gl_index_id);
    cf_manager.drop_cf(se_db, gl_index_id.index_id);
  }
  gl_index_ids.clear();

  dd_type = SeKeyDef::DDL_CREATE_INDEX_ONGOING;
  m_dict->get_ongoing_index_operation(&gl_index_ids, dd_type);
  for (auto& gl_index_id : gl_index_ids) {
    m_dict->delete_with_prefix(write_batch, dd_type, gl_index_id);
    m_dict->delete_index_info(write_batch, gl_index_id);
    cf_manager.drop_cf(se_db, gl_index_id.index_id);
  }

  uint16_t target_version = SeKeyDef::SYSTEM_CF_VERSION::VERSION_2;
  if (m_dict->update_system_cf_version(write_batch, target_version)) {
    HANDLER_LOG(ERROR, "SE: failed to set system_cf_version as VERSION2!");
    return true;
  } else if (m_dict->commit(write_batch)) {
    HANDLER_LOG(ERROR, "SE: failed commit into dictionary!");
    return true;
  } else {
#ifndef NDEBUG
    HANDLER_LOG(INFO, "SE: successfully upgrade system_cf to VERSION2");
#endif
    return false;
  }
}

bool SeDdlManager::load_existing_tables(uint max_index_id_in_dict)
{
  /* Read the data dictionary and populate the hash */
  uchar ddl_entry[SeKeyDef::INDEX_NUMBER_SIZE];
  se_netbuf_store_index(ddl_entry, SeKeyDef::DDL_ENTRY_INDEX_START_NUMBER);
  const common::Slice ddl_entry_slice((char *)ddl_entry,
                                               SeKeyDef::INDEX_NUMBER_SIZE);

  /* Reading data dictionary should always skip bloom filter */
  db::Iterator *it = m_dict->new_iterator();
  int i = 0;
  for (it->Seek(ddl_entry_slice); it->Valid(); it->Next()) {
    const uchar *ptr;
    const uchar *ptr_end;
    const common::Slice key = it->key();
    const common::Slice val = it->value();

    if (key.size() >= SeKeyDef::INDEX_NUMBER_SIZE &&
        memcmp(key.data(), ddl_entry, SeKeyDef::INDEX_NUMBER_SIZE))
      break;

    if (key.size() <= SeKeyDef::INDEX_NUMBER_SIZE) {
      HANDLER_LOG(ERROR, "SE: unexepected key size(corruption?)", "key_size", key.size());
      return true;
    }

    auto tdef = std::make_shared<SeTableDef>(key, SeKeyDef::INDEX_NUMBER_SIZE);

    auto& table_name = tdef->full_tablename();
    // Now, read the DDLs.
    const int d_PACKED_SIZE = SeKeyDef::PACKED_SIZE * 2;
    const int real_val_size = val.size() - SeKeyDef::VERSION_SIZE;
    if (real_val_size % d_PACKED_SIZE) {
      HANDLER_LOG(ERROR, "SE: invalid keylist from dictionary", K(table_name));
      return true;
    }
    tdef->m_key_count = real_val_size / d_PACKED_SIZE;
    tdef->m_key_descr_arr = new std::shared_ptr<SeKeyDef>[tdef->m_key_count];

    ptr = reinterpret_cast<const uchar *>(val.data());
    const int version = se_netbuf_read_uint16(&ptr);
    const int exp_version = SeKeyDef::DDL_ENTRY_INDEX_VERSION;
    if (version != exp_version) {
      HANDLER_LOG(ERROR, "SE: DDL ENTRY Version mismatch", "expected", exp_version, "actual", version);
      return true;
    }
    ptr_end = ptr + real_val_size;
    for (uint keyno = 0; ptr < ptr_end; keyno++) {
      GL_INDEX_ID gl_index_id;
      se_netbuf_read_gl_index(&ptr, &gl_index_id);
      uint32_t subtable_id = gl_index_id.cf_id;
      uint16 index_dict_version = 0;
      uchar index_type = 0;
      uint16 kv_version = 0;
      uint flags = 0;
      db::ColumnFamilyHandle* cfh;

      if (get_index_dict(table_name, gl_index_id, max_index_id_in_dict,
                         index_dict_version, index_type, kv_version, flags)) {
        HANDLER_LOG(ERROR, "SE: failed to get index information from dictionary.", K(table_name));
        return true;
      } else if (nullptr == (cfh = cf_manager.get_cf(subtable_id))) {
        HANDLER_LOG(ERROR, "SE: failed to find subtable", K(table_name), K(subtable_id));
        return true;
      }

      assert(cfh != nullptr);
      tdef->space_id = (reinterpret_cast<db::ColumnFamilyHandleImpl *>(cfh))->cfd()->get_table_space_id();
      /*
        We can't fully initialize SeKeyDef object here, because full
        initialization requires that there is an open TABLE* where we could
        look at Field* objects and set max_length and other attributes
      */
      tdef->m_key_descr_arr[keyno] = std::make_shared<SeKeyDef>(
          subtable_id, keyno, cfh, index_dict_version, index_type, kv_version,
          flags & SeKeyDef::REVERSE_CF_FLAG,
          flags & SeKeyDef::AUTO_CF_FLAG, "",
          m_dict->get_stats(gl_index_id));
    }
    put(tdef);
    i++;
  }

  if (!it->status().ok()) {
    HANDLER_LOG(ERROR, "SE: failed to iterate dictioanry", "status", it->status().ToString());
    return true;
  }
  MOD_DELETE_OBJECT(Iterator, it);
  HANDLER_LOG(INFO, "SE: loaded DDL data for tables", "count", i);
  return false;
}

bool SeDdlManager::upgrade_existing_tables(THD *const thd)
{
  if (m_ddl_hash.empty()) return false;

  bool error = false;
  assert (dd::INVALID_OBJECT_ID != m_next_table_id);
  dd::cache::Dictionary_client* dc = thd->dd_client();
  dd::cache::Dictionary_client::Auto_releaser releaser(dc);
  // Disable autocommit option
  Disable_autocommit_guard autocommit_guard(thd);
  for (auto &kv: m_ddl_hash) {
    std::shared_ptr<SeTableDef> &tbl_def = kv.second;
    tbl_def->set_table_id(m_next_table_id);
    // name string in SeTableDef is from filename format, to acquire dd object
    // we should use table name format which uses different charset
    char schema_name[FN_REFLEN+1], table_name[FN_REFLEN+1];
    filename_to_tablename(tbl_def->base_dbname().c_str(), schema_name, sizeof(schema_name));
    filename_to_tablename(tbl_def->base_tablename().c_str(), table_name, sizeof(table_name));
    if (NULL != strstr(table_name, tmp_file_prefix)) {
      HANDLER_LOG(WARN, "SE: found trashy temporary table, skip it during upgrading", K(schema_name), K(table_name));
      continue;
    }

    const dd::Schema *db_sch = nullptr;
    dd::Table *dd_table = nullptr;
    MDL_request sch_mdl_request;
    MDL_REQUEST_INIT(&sch_mdl_request, MDL_key::SCHEMA, schema_name, "",
                     MDL_INTENTION_EXCLUSIVE, MDL_TRANSACTION);
    MDL_request tbl_mdl_request;
    MDL_REQUEST_INIT(&tbl_mdl_request, MDL_key::TABLE, schema_name,
                     table_name, MDL_EXCLUSIVE, MDL_TRANSACTION);
    // acquire intention_exclusive lock on dd::Schema if needed
    if (!thd->mdl_context.owns_equal_or_stronger_lock(
            MDL_key::SCHEMA, schema_name, "", MDL_INTENTION_EXCLUSIVE) &&
        thd->mdl_context.acquire_lock(&sch_mdl_request,
                                      thd->variables.lock_wait_timeout)) {
      HANDLER_LOG(ERROR, "SE: failed to acquire lock for dd::Schema", K(schema_name));
      error = true;
    // acquire dd::Schema object
    } else if (dc->acquire(schema_name, &db_sch) || (nullptr == db_sch)) {
      HANDLER_LOG(ERROR, "SE: failed to acquire dd::Schema object", K(schema_name));
      error = true;
    // acquire exclusive lock on dd::Table if needed
    } else if (!dd::has_exclusive_table_mdl(thd, schema_name, table_name) &&
               thd->mdl_context.acquire_lock(
                   &tbl_mdl_request, thd->variables.lock_wait_timeout)) {
      HANDLER_LOG(ERROR, "SE: failed to acquire exclusive lock for dd::Table",
                   K(schema_name), K(table_name));
      error = true;
    // acquire dd::Table object to modify
    } else if (dc->acquire_for_modification(schema_name, table_name, &dd_table) ||
               (nullptr == dd_table) || dd_table->engine() != se_hton_name) {
      HANDLER_LOG(ERROR, "SE: failed to acquire dd::Table object", K(schema_name), K(table_name));
      error = true;
    // set se_private_id and se_private_data
    } else if (tbl_def->write_dd_table(dd_table)) {
      HANDLER_LOG(ERROR, "SE: failed to update dd_table", K(schema_name), K(table_name));
      error = true;
    // persist se_private_id and se_private_data of dd::Table/dd::Index
    } else if (dc->update(dd_table)) {
      HANDLER_LOG(ERROR, "SE: failed to persist dd::Table", K(schema_name), K(table_name));
      error = true;
    } else {
#ifndef NDEBUG
      HANDLER_LOG(INFO, "SE: successfully upgrade dd::Table", K(schema_name), K(table_name));
#endif
      ++m_next_table_id;
      error = false;
    }
    if (error)
      break;
  }

  if (error) {
    trans_rollback_stmt(thd);
    trans_rollback(thd);
    return true;
  } else {
    return (trans_commit_stmt(thd) || trans_commit(thd));
  }
}

bool SeDdlManager::populate_existing_tables(THD *const thd)
{
  assert(nullptr != thd);
  bool error = SeDdHelper::traverse_all_se_tables(
      thd, true, 0,
      [&](const dd::Schema *dd_schema, const dd::Table *dd_table) -> bool {
        // always use filename format in SeTableDef
        char db_filename[FN_REFLEN + 1], table_filename[FN_REFLEN + 1];
        auto schema_name = dd_schema->name().c_str();
        auto table_name = dd_table->name().c_str();
        memset(db_filename, 0, sizeof(db_filename));
        memset(table_filename, 0, sizeof(table_filename));
        tablename_to_filename(schema_name, db_filename, sizeof(db_filename));
        tablename_to_filename(table_name, table_filename,
                              sizeof(table_filename));
        std::ostringstream oss;
        oss << db_filename << '.' << table_filename;
        SeTableDef *tbl_def = restore_table_from_dd(dd_table, oss.str());
        if (nullptr == tbl_def) {
          HANDLER_LOG(ERROR, "SE: failed to restore table from dd::Table", K(schema_name), K(table_name));
          return true;
        } else {
#ifndef NDEBUG
          HANDLER_LOG(INFO, "SE: successfully populate table from dd::Table", K(schema_name), K(table_name));
#endif
          put(std::shared_ptr<SeTableDef>(tbl_def));
          return false;
        }
      });

  if (error) {
    HANDLER_LOG(ERROR, "SE: failed to populate all se tables from data dictionary");
#ifndef NDEBUG
  } else {
    HANDLER_LOG(INFO, "SE: successfully populate all se tables from data dictionary");
#endif
  }
  return error;
}

SeKeyDef* SeDdlManager::restore_index_from_dd(const dd::Properties& prop,
                                              const std::string &table_name,
                                              const std::string &index_name,
                                              uint32_t subtable_id,
                                              uint keyno,
                                              int index_type)
{
  int index_version_id = 0, kv_version = 0, key_flags = 0;
  db::ColumnFamilyHandle* cfh = nullptr;
  SeKeyDef *key_def = nullptr;
  if (prop.get(dd_index_key_strings[DD_INDEX_VERSION_ID], &index_version_id) ||
      prop.get(dd_index_key_strings[DD_INDEX_KV_VERSION], &kv_version) ||
      prop.get(dd_index_key_strings[DD_INDEX_FLAGS], &key_flags)) {
    HANDLER_LOG(ERROR, "SE: failed to get index metadata from se_private_data",
                K(index_name), K(subtable_id), K(table_name));
    return nullptr;
  } else if (!SeDictionaryManager::is_valid_index_version(index_version_id)) {
    HANDLER_LOG(ERROR, "SE: get invalid index version from se_private_data",
                K(index_version_id), K(index_name), K(subtable_id), K(table_name));
    return nullptr;
  } else if (!SeDictionaryManager::is_valid_kv_version(index_type, kv_version)) {
    HANDLER_LOG(ERROR, "SE: get invalid kv_version from se_private_data",
                K(kv_version), K(index_name), K(subtable_id), K(table_name));
    return nullptr;
  } else if (nullptr == (cfh = cf_manager.get_cf(subtable_id))) {
    HANDLER_LOG(ERROR, "SE: failed to get column family handle for index",
                K(index_name), K(subtable_id), K(table_name));
    return nullptr;
  } else {
    return new SeKeyDef(subtable_id, keyno, cfh, index_version_id, index_type,
                           kv_version, key_flags & SeKeyDef::REVERSE_CF_FLAG,
                           key_flags & SeKeyDef::AUTO_CF_FLAG, index_name,
                           m_dict->get_stats({subtable_id, subtable_id}));
  }
}

SeTableDef* SeDdlManager::restore_table_from_dd(const dd::Table* dd_table, const std::string& table_name)
{
  SeTableDef* tbl_def = nullptr;
  uint32_t hidden_subtable_id = DD_SUBTABLE_ID_INVALID;
  if (nullptr != dd_table && !table_name.empty() &&
      !SeTableDef::verify_dd_table(dd_table, hidden_subtable_id)) {
    uint64_t table_id = dd_table->se_private_id();
    uint max_index_id_in_dict = 0;
    m_dict->get_max_index_id(&max_index_id_in_dict);
    if (max_index_id_in_dict < SeKeyDef::END_DICT_INDEX_ID) {
      max_index_id_in_dict = SeKeyDef::END_DICT_INDEX_ID;
    }

    tbl_def = new SeTableDef(table_name);
    tbl_def->set_table_id(table_id);
    tbl_def->m_key_count = dd_table->indexes().size();
    if (DD_SUBTABLE_ID_INVALID != hidden_subtable_id) {
      ++tbl_def->m_key_count;
    }

    bool error = false;
    // construct SeKeyDef for all user defiend indexes
    tbl_def->m_key_descr_arr = new std::shared_ptr<SeKeyDef>[tbl_def->m_key_count];
    uint keyno = 0;
    for (auto dd_index : dd_table->indexes()) {
      std::string index_name(dd_index->name().c_str());
      uint32_t subtable_id = DD_SUBTABLE_ID_INVALID;
      int index_type;
      SeKeyDef* kd = nullptr;
      const dd::Properties &p = dd_index->se_private_data();
      if (p.get(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], &subtable_id) ||
          DD_SUBTABLE_ID_INVALID == subtable_id) {
        HANDLER_LOG(ERROR, "SE: failed to get subtable_id from se_private_data of dd::Index",
                    K(index_name), K(table_name));
        error = true;
        break;
      } else if (max_index_id_in_dict < subtable_id) {
        HANDLER_LOG(ERROR, "SE: got invalid subtable id which larger than maximum id recored in dictioanry",
                    K(max_index_id_in_dict), K(subtable_id), K(index_name), K(table_name));
        error = true;
        break;
      } else if (p.get(dd_index_key_strings[DD_INDEX_TYPE], &index_type) ||
                 (index_type != SeKeyDef::INDEX_TYPE_PRIMARY &&
                  index_type != SeKeyDef::INDEX_TYPE_SECONDARY)) {
        HANDLER_LOG(ERROR, "SE: failed to get index_type from se_private_data of dd::Index",
                    K(index_type), K(subtable_id), K(index_name), K(table_name));
        error = true;
        break;
      } else if (nullptr == (kd = restore_index_from_dd(p, table_name,
            index_name, subtable_id, keyno, index_type))) {
        error = true;
        break;
      } else {
        tbl_def->m_key_descr_arr[keyno++].reset(kd);
        // TODO(Zhao Dongsheng): save table space id at se private data of dd.
        tbl_def->space_id = (reinterpret_cast<db::ColumnFamilyHandleImpl *>(kd->get_cf()))->cfd()->get_table_space_id();;
      }
    }

    if (!error && DD_SUBTABLE_ID_INVALID != hidden_subtable_id) {
      // construct SeKeyDef for hidden primary key added by SE
      int index_type = SeKeyDef::INDEX_TYPE::INDEX_TYPE_HIDDEN_PRIMARY;
      SeKeyDef* kd = nullptr;
      if (max_index_id_in_dict < hidden_subtable_id) {
        HANDLER_LOG(ERROR, "SE: got invalid hidden_subtable_id which larger than maximum id recored in dictioanry",
                    K(max_index_id_in_dict), K(hidden_subtable_id), K(table_name));
        error = true;
      } else if (nullptr == (kd = restore_index_from_dd(
           dd_table->se_private_data(), table_name, HIDDEN_PK_NAME,
           hidden_subtable_id, keyno, index_type))) {
        error = true;
      } else {
        tbl_def->m_key_descr_arr[keyno].reset(kd);
        tbl_def->space_id = (reinterpret_cast<db::ColumnFamilyHandleImpl *>(kd->get_cf()))->cfd()->get_table_space_id();;
      }
    }

    if (error) {
      delete tbl_def;
      tbl_def = nullptr;
    }
  }
  return tbl_def;
}

SeTableDef* SeDdlManager::restore_table_from_dd(THD* thd, const std::string& table_name)
{
  assert(nullptr != thd);

  std::string db_name, tbl_name, part_name;
  if (se_split_normalized_tablename(table_name, &db_name, &tbl_name, &part_name)) {
    HANDLER_LOG(ERROR, "SE: failed to parse table name", "full_table_name", table_name);
    return nullptr;
  }

  char schema_name[FN_REFLEN+1], dd_tbl_name[FN_REFLEN+1];
  filename_to_tablename(db_name.c_str(), schema_name, sizeof(schema_name));
  filename_to_tablename(tbl_name.c_str(), dd_tbl_name, sizeof(dd_tbl_name));
  MDL_ticket *tbl_ticket = nullptr;
  const dd::Table *dd_table = nullptr;
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  SeTableDef* tbl = nullptr;
  if (!SeDdHelper::acquire_se_table(thd, 0, schema_name,
          dd_tbl_name, dd_table, tbl_ticket) && nullptr != dd_table) {
    tbl = restore_table_from_dd(dd_table, table_name);
    if (tbl_ticket) dd::release_mdl(thd, tbl_ticket);
  }

  return tbl;
}

int SeDdlManager::update_max_index_id(uint64_t index_id)
{
  assert(nullptr != m_dict);
  int ret = Status::kOk;

  std::unique_ptr<db::WriteBatch> write_batch = m_dict->begin();
  if (IS_NULL(write_batch.get())) {
    ret = Status::kInvalidArgument;
    HANDLER_LOG(ERROR, "SE: failed to allocate write batch", K(ret), KP(write_batch.get()));
  } else if (FAILED(m_dict->update_max_index_id(write_batch.get(), index_id))) {
    HANDLER_LOG(ERROR, "SE: failed to update max_index_id", K(ret), K(index_id));
  } else if (FAILED(m_dict->commit(write_batch.get()))) {
    HANDLER_LOG(ERROR, "SE: failed to commit max_index_id", K(ret), K(index_id));
  } else {
#ifndef NDEBUG
    HANDLER_LOG(INFO, "SE: successfully update max_index_id", K(index_id));
#endif
  }

  return ret;
}

bool SeDdlManager::update_max_table_id(uint64_t table_id)
{
  auto write_batch = m_dict->begin();
  if (nullptr == write_batch) {
    HANDLER_LOG(ERROR, "SE: failed to begin wrtiebatch");
    return true;
  }

  bool res = false;
  m_dict->update_max_table_id(write_batch.get(), table_id);
  if (m_dict->commit(write_batch.get())) {
    HANDLER_LOG(ERROR, "SE: failed to update max_table_id", K(table_id));
    res = true;
  }
  return res;
}

bool SeDdlManager::update_system_cf_version(uint16_t system_cf_version)
{
  auto write_batch = m_dict->begin();
  if (!write_batch) {
    HANDLER_LOG(ERROR, "SE: failed to begin wrtiebatch");
    return true;
  }

  bool res = false;
  m_dict->update_system_cf_version(write_batch.get(), system_cf_version);
  if (m_dict->commit(write_batch.get())) {
    HANDLER_LOG(ERROR, "SE: failed to update system_cf_version", "version", system_cf_version);
    res = true;
  }
  return res;
}

int SeDdlManager::get_index_dict(const std::string& table_name,
                                 const GL_INDEX_ID &gl_index_id,
                                 uint max_index_id_in_dict,
                                 uint16 &index_dict_version,
                                 uchar &index_type,
                                 uint16 &kv_version,
                                 uint &flags)
{
  int ret = false;
  index_dict_version = 0;
  index_type = 0;
  kv_version = 0;
  flags = 0;

  if (!m_dict->get_index_info(gl_index_id, &index_dict_version, &index_type,
                              &kv_version)) {
    sql_print_error("SE: Could not get index information "
                    "for Index Number (%u,%u), table %s",
                    gl_index_id.cf_id, gl_index_id.index_id, table_name.c_str());
    return true;
  }

  if (max_index_id_in_dict < gl_index_id.index_id) {
    sql_print_error("SE: Found max index id %u from data dictionary "
                    "but also found larger index id %u from dictionary. "
                    "This should never happen and possibly a bug.",
                    max_index_id_in_dict, gl_index_id.index_id);
    return true;
  }

  if (!m_dict->get_cf_flags(gl_index_id.cf_id, &flags)) {
    sql_print_error("SE: Could not get Column Family Flags "
                    "for CF Number %d, table %s",
                    gl_index_id.cf_id, table_name.c_str());
    return true;
  }

  return ret;
}

std::shared_ptr<SeTableDef> SeDdlManager::find_tbl_from_cache(const std::string &table_name,
                                                              bool lock/* = true*/)
{
  std::shared_ptr<SeTableDef> tbl;
  if (lock) {
    mysql_rwlock_rdlock(&m_rwlock);
  }

  const auto &it = m_ddl_hash.find(table_name);
  if (it != m_ddl_hash.end())
    tbl = it->second;

  if (lock) {
    mysql_rwlock_unlock(&m_rwlock);
  }

  return tbl;
}

// this method assumes write lock on m_rwlock
const std::shared_ptr<SeKeyDef> &SeDdlManager::find(GL_INDEX_ID gl_index_id)
{
  auto it = m_index_num_to_keydef.find(gl_index_id);
  if (it != m_index_num_to_keydef.end()) {
    bool from_dict = false;
    auto table_def = find(it->second.first, &from_dict, false);
    if (table_def) {
      if (from_dict) put(table_def, false);

      if (it->second.second < table_def->m_key_count) {
        return table_def->m_key_descr_arr[it->second.second];
      }
    }
  } else {
    auto it = m_index_num_to_uncommitted_keydef.find(gl_index_id);
    if (it != m_index_num_to_uncommitted_keydef.end()) {
      return it->second;
    }
  }

  static std::shared_ptr<SeKeyDef> empty = nullptr;

  return empty;
}

// this is a safe version of the find() function below.  It acquires a read
// lock on m_rwlock to make sure the SeKeyDef is not discarded while we
// are finding it.  Copying it into 'ret' increments the count making sure
// that the object will not be discarded until we are finished with it.
std::shared_ptr<const SeKeyDef> SeDdlManager::safe_find(GL_INDEX_ID gl_index_id)
{
  std::shared_ptr<const SeKeyDef> ret(nullptr);

  mysql_rwlock_rdlock(&m_rwlock);

  auto it = m_index_num_to_keydef.find(gl_index_id);
  if (it != m_index_num_to_keydef.end()) {
    bool from_dict = false;
    const auto table_def = find(it->second.first, &from_dict, false);
    if (table_def && it->second.second < table_def->m_key_count) {
      const auto &kd = table_def->m_key_descr_arr[it->second.second];
      if (kd->max_storage_fmt_length() != 0) {
        ret = kd;
      }
      if (from_dict) {
        // put into cache need write lock
        mysql_rwlock_unlock(&m_rwlock);
        put(table_def);
        mysql_rwlock_rdlock(&m_rwlock);
      }
    }
  } else {
    auto it = m_index_num_to_uncommitted_keydef.find(gl_index_id);
    if (it != m_index_num_to_uncommitted_keydef.end()) {
      const auto &kd = it->second;
      if (kd->max_storage_fmt_length() != 0) {
        ret = kd;
      }
    }
  }

  mysql_rwlock_unlock(&m_rwlock);

  return ret;
}

} //namespace smartengine
