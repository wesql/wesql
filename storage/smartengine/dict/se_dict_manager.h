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
#ifndef SE_DICT_XE_DICT_MANAGER_H_
#define SE_DICT_XE_DICT_MANAGER_H_

#include "dict/se_index.h"

namespace smartengine
{

namespace db
{
class WriteBatchBase;
} // namespace db

/*
  SeDictionaryManager manages how MySQL on SmartEngine stores its
  internal data dictionary.

  SmartEngine stores data dictionary on dedicated system subtable
  named __system__. The system subtable is used by SmartEngine
  internally only, and not used by applications.

  Currently SmartEngine has the following data dictionary data models:

  1. Table Name => internal index id mappings
  key: SeKeyDef::DDL_ENTRY_INDEX_START_NUMBER(0x1) + dbname.tablename
  value: version, {cf_id, index_id}*n_indexes_of_the_table
  version is 2 bytes. cf_id and index_id are 4 bytes.

  2. Internal cf_id, index id => index information
  key: SeKeyDef::INDEX_INFO(0x2) + cf_id + index_id
  value: version, index_type, kv_format_version
  index_type is 1 byte, version and kv_format_version are 2 bytes.

  3. CF id => CF flags
  key: SeKeyDef::CF_DEFINITION(0x3) + cf_id
  value: version, {is_reverse_cf, is_auto_cf}
  cf_flags is 4 bytes in total.

  4. Binlog entry (updated at commit)
  key: SeKeyDef::BINLOG_INFO_INDEX_NUMBER (0x4)
  value: version, {binlog_name,binlog_pos,binlog_gtid}

  5. Ongoing drop index entry
  key: SeKeyDef::DDL_DROP_INDEX_ONGOING(0x5) + cf_id + index_id
  value: version

  6. index stats
  key: SeKeyDef::INDEX_STATISTICS(0x6) + cf_id + index_id
  value: version, {materialized PropertiesCollector::IndexStats}

  7. maximum index id
  key: SeKeyDef::MAX_INDEX_ID(0x7)
  value: index_id
  index_id is 4 bytes

  8. Ongoing create index entry
  key: SeKeyDef::DDL_CREATE_INDEX_ONGOING(0x8) + cf_id + index_id
  value: version

  Data dictionary operations are atomic inside SmartEngine. For example,
  when creating a table with two indexes, it is necessary to call Put
  three times. They have to be atomic. SeDictionaryManager has a wrapper function
  begin() and commit() to make it easier to do atomic operations.

*/
class SeDictionaryManager {
private:
  mysql_mutex_t m_mutex;
  db::DB *m_db = nullptr;
  db::ColumnFamilyHandle *m_system_cfh = nullptr;
  /* Utility to put INDEX_INFO and CF_DEFINITION */

  uchar m_key_buf_max_index_id[SeKeyDef::INDEX_NUMBER_SIZE] = {0};
  common::Slice m_key_slice_max_index_id;

  uchar m_key_buf_max_table_id[SeKeyDef::INDEX_NUMBER_SIZE] = {0};
  common::Slice m_key_slice_max_table_id;

  uchar m_key_buf_server_version[SeKeyDef::SERVER_VERSION_SIZE] = {0};
  common::Slice m_key_slice_server_version;

  std::unordered_set<dd::Object_id> dd_table_ids;

  static void dump_index_id(uchar *const netbuf,
                            SeKeyDef::DATA_DICT_TYPE dict_type,
                            const GL_INDEX_ID &gl_index_id);

public:
  SeDictionaryManager(const SeDictionaryManager &) = delete;
  SeDictionaryManager &operator=(const SeDictionaryManager &) = delete;
  SeDictionaryManager() = default;

  bool init(db::DB *const se_dict, const common::ColumnFamilyOptions &cf_options);

  inline void cleanup() { mysql_mutex_destroy(&m_mutex); }

  inline void lock() { SE_MUTEX_LOCK_CHECK(m_mutex); }

  inline void unlock() { SE_MUTEX_UNLOCK_CHECK(m_mutex); }

  /* Raw se operations */
  std::unique_ptr<db::WriteBatch> begin() const;

  int commit(db::WriteBatch *batch, const bool &sync = true) const;

  common::Status get_value(const common::Slice &key, std::string *const value) const;

  void put_key(db::WriteBatchBase *const batch,
               const common::Slice &key,
               const common::Slice &value) const;

  void delete_key(db::WriteBatchBase *batch, const common::Slice &key) const;

  db::Iterator *new_iterator() const;

  void delete_index_info(db::WriteBatch *batch, const GL_INDEX_ID &index_id) const;

  // Kept for upgrading from old version
  bool get_index_info(const GL_INDEX_ID &gl_index_id,
                      uint16_t *index_dict_version,
                      uchar *index_type,
                      uint16_t *kv_version) const;

  static bool is_valid_index_version(uint16_t index_dict_version);

  static bool is_valid_kv_version(uchar index_type, uint16_t kv_version);

  void drop_cf_flags(db::WriteBatch *const batch, uint32_t cf_id) const;

  // Kept for upgrading from old version
  bool get_cf_flags(const uint &cf_id, uint *const cf_flags) const;

  void delete_with_prefix(db::WriteBatch *const batch,
                          SeKeyDef::DATA_DICT_TYPE dict_type,
                          const GL_INDEX_ID &gl_index_id) const;

  /* Functions for fast CREATE/DROP TABLE/INDEX */
  // Kept for upgrading from old version
  void get_ongoing_index_operation(std::unordered_set<GL_INDEX_ID> *gl_index_ids, SeKeyDef::DATA_DICT_TYPE dd_type) const;

  bool get_max_index_id(uint32_t *const index_id) const;

  int update_max_index_id(db::WriteBatch *batch, uint32_t index_id) const;

  bool get_system_cf_version(uint16_t* system_cf_version) const;

  bool update_system_cf_version(db::WriteBatch *const batch, uint16_t system_cf_version) const;

  bool get_max_table_id(uint64_t *table_id) const;

  bool update_max_table_id(db::WriteBatch *const batch, uint64_t table_id) const;

  void add_stats(db::WriteBatch *const batch, const std::vector<SeIndexStats> &stats) const;

  SeIndexStats get_stats(GL_INDEX_ID gl_index_id) const;

  int set_server_version();

  int get_server_version(uint *version);

  int insert_dd_table_id(dd::Object_id table_id);
};

} //namespace smartengine

#endif
