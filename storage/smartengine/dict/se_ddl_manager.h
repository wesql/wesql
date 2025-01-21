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
#pragma once

#include <map>
#include <string>
#include <unordered_set>
#include <utility>
#include <arpa/inet.h>
#include "dict/se_index.h"
#include "util/se_utils.h"

namespace smartengine {

class SeDictionaryManager;
class SeKeyDef;
class SeFieldPacking;
class SeSubtableManager;
class SeDdlManager;
class SeDdlLogManager;
struct SeCollationCodec;
struct SeInplaceDdlDictInfo;

class SeSequenceGenerator
{
  uint m_next_number = 0;
  mysql_mutex_t m_mutex;

public:
  SeSequenceGenerator(const SeSequenceGenerator &) = delete;
  SeSequenceGenerator &operator=(const SeSequenceGenerator &) = delete;
  SeSequenceGenerator() = default;

  void init(const uint &initial_number)
  {
    mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
    m_next_number = initial_number;
  }

  uint get_and_update_next_number(SeDictionaryManager *const dict);

  void cleanup() { mysql_mutex_destroy(&m_mutex); }
};

interface Se_tables_scanner
{
  virtual int add_table(SeTableDef * tdef) = 0;
};

/*
  This contains a mapping of

     dbname.table_name -> array{SeKeyDef}.

  objects are shared among all threads.
*/

class SeDdlManager
{
  SeDictionaryManager *m_dict = nullptr;
  //my_core::HASH m_ddl_hash; // Contains SeTableDef elements
  std::unordered_map<std::string, std::shared_ptr<SeTableDef>> m_ddl_hash;
  // Maps index id to <table_name, index number>
  std::map<GL_INDEX_ID, std::pair<std::string, uint>> m_index_num_to_keydef;

  // Maps index id to key definitons not yet committed to data dictionary.
  // This is mainly used to store key definitions during ALTER TABLE.
  std::map<GL_INDEX_ID, std::shared_ptr<SeKeyDef>>
    m_index_num_to_uncommitted_keydef;
  mysql_rwlock_t m_rwlock;

  SeSequenceGenerator m_sequence;
  // A queue of table stats to write into data dictionary
  // It is produced by event listener (ie compaction and flush threads)
  // and consumed by the se background thread
  std::map<GL_INDEX_ID, SeIndexStats> m_stats2store;

  const std::shared_ptr<SeKeyDef> &find(GL_INDEX_ID gl_index_id);

  // used to generate table id
  mysql_mutex_t m_next_table_id_mutex;
  uint64_t m_next_table_id;
public:
  SeDdlManager(const SeDdlManager &) = delete;
  SeDdlManager &operator=(const SeDdlManager &) = delete;
  SeDdlManager() {}

  /* Load the data dictionary from on-disk storage */
  bool init(THD *const thd, SeDictionaryManager *const dict_arg);

  void cleanup();

  void reset();

  // find is designed to find SeTableDef from table cache or load SeTableDef
  // from dictionary.
  //
  //@params
  // table_name[in] key used to search in hash_map
  // from_dict[out] tell caller whether the retured object is loaded from
  //                dictionary. If true, the returned object was loaded from
  //                dictioanry. The caller can put it into cache if needed.
  // lock[in] if true(by default), a read lock on table cache will be acquired.
  //
  //@return value
  // SeTableDef
  std::shared_ptr<SeTableDef> find(const std::string &table_name,
                                    bool* from_dict, bool lock = true);
  std::shared_ptr<SeTableDef> find(const char *table_name, int64_t name_len,
                                    bool* from_dict, bool lock = true);
  std::shared_ptr<SeTableDef> find(const dd::Table* dd_table,
                                    const std::string& table_name,
                                    bool* from_dict, bool lock = true);
  std::shared_ptr<SeTableDef> find(THD* thd, const std::string& table_name,
                                    bool* from_dict, bool lock = true);

  // find SeTableDef from table cache
  //
  //@params
  // table_name[in] key used to search in hash_map
  // lock[in] if true(by default), a read lock on table cache will be acquired.
  //
  //@return value
  // SeTableDef
  std::shared_ptr<SeTableDef> find_tbl_from_cache(
      const std::string &table_name, bool lock = true);

  // SeTableDef* find_tbl_from_dict(const std::string &table_name);
  int get_index_dict(const std::string &table_name,
                     const GL_INDEX_ID &gl_index_id,
                     uint max_index_id_in_dict,
                     uint16 &index_dict_version,
                     uchar &index_type,
                     uint16 &kv_version,
                     uint &flags);

  std::shared_ptr<const SeKeyDef> safe_find(GL_INDEX_ID gl_index_id);
  void set_stats(const std::unordered_map<GL_INDEX_ID, SeIndexStats> &stats);
  void adjust_stats(SeIndexStats stats);
  void adjust_stats2(SeIndexStats stats, const bool increment);
  void persist_stats(const bool &sync = false);

  /* Put the data into in-memory table (only) */
  void put(const std::shared_ptr<SeTableDef>& tbl_def, bool lock = true);
  /* Modify the mapping and write it to on-disk storage */
  int put_and_write(const std::shared_ptr<SeTableDef>& tbl,
                    db::WriteBatch *const batch,
                    SeDdlLogManager *const ddl_log_manager,
                    ulong thread_id, bool write_ddl_log);

  void remove_cache(const std::string &dbname_tablename, bool lock = true);
  bool rename_cache(const std::string &from, const std::string &to);
  bool rename_cache(SeTableDef* tbl, const std::string &to);

  uint get_and_update_next_number(SeDictionaryManager *const dict) {
    return m_sequence.get_and_update_next_number(dict);
  }

  bool get_table_id(uint64_t &table_id);

  /* Walk the data dictionary */
  int scan_for_tables(Se_tables_scanner *tables_scanner);

  void erase_index_num(const GL_INDEX_ID &gl_index_id);
  void add_uncommitted_keydefs(
      const std::unordered_set<std::shared_ptr<SeKeyDef>> &indexes);
  void remove_uncommitted_keydefs(
      const std::unordered_set<std::shared_ptr<SeKeyDef>> &indexes);

  // check whether background thread cna purge given subtable id
  bool can_purge_subtable(THD* thd, const GL_INDEX_ID& gl_index_id);
private:
  bool upgrade_system_cf_version1(THD* thd);
  bool upgrade_system_cf_version2();
  bool load_existing_tables(uint);
  bool upgrade_existing_tables(THD *const thd);
  bool populate_existing_tables(THD *const thd);
  bool update_max_table_id(uint64_t table_id);
  bool update_system_cf_version(uint16_t system_cf_version);

  SeKeyDef* restore_index_from_dd(const dd::Properties& prop,
                                  const std::string &table_name,
                                  const std::string &index_name,
                                  uint32_t subtable_id,
                                  uint keyno,
                                  int index_type);
  SeTableDef* restore_table_from_dd(const dd::Table* dd_table,
                                    const std::string& table_name);
  SeTableDef* restore_table_from_dd(THD* thd, const std::string& table_name);
};

} //namespace smartengine
