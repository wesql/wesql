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
#include "se_dict_manager.h"
#include "dict/se_cf_manager.h"
#include "dict/se_dict_util.h"
#include "logger/log_module.h"
#include "schema/table_schema.h"
#include "storage/storage_logger.h"

namespace smartengine {

extern SeSubtableManager cf_manager;

bool SeDictionaryManager::init(db::DB *const se_dict, const common::ColumnFamilyOptions &cf_options)
{
  int ret = common::Status::kOk;
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
  m_db = se_dict;
  bool create_table_space = false;
  int64_t sys_table_space_id = DEFAULT_SYSTEM_TABLE_SPACE_ID;
  schema::TableSchema table_schema;
  table_schema.set_database_name(DEFAULT_SYSTEM_DATABASE_NAME);
  table_schema.set_primary_index_id(DEFAULT_SYSTEM_SUBTABLE_ID);
  table_schema.set_index_id(DEFAULT_SYSTEM_SUBTABLE_ID);

  if (IS_NULL(m_system_cfh = cf_manager.get_or_create_subtable(m_db,
                                                               nullptr,
                                                               -1,
                                                               cf_options,
                                                               table_schema,
                                                               false,
                                                               sys_table_space_id))) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(ERROR, "fail to get system table", K(ret), "index_id", DEFAULT_SYSTEM_SUBTABLE_ID, "index_name", DEFAULT_SYSTEM_SUBTABLE_NAME);
  } else {
      se_netbuf_store_index(m_key_buf_max_index_id, SeKeyDef::MAX_INDEX_ID);
      m_key_slice_max_index_id =common::Slice(reinterpret_cast<char *>(m_key_buf_max_index_id), SeKeyDef::INDEX_NUMBER_SIZE);
      se_netbuf_store_index(m_key_buf_max_table_id, SeKeyDef::MAX_TABLE_ID);
      m_key_slice_max_table_id = common::Slice(reinterpret_cast<char *>(m_key_buf_max_table_id), SeKeyDef::INDEX_NUMBER_SIZE);
      // TODO(Zhao Dongsheng): The function name se_netbuf_store_index is not good, it should be se_netbuf_store_uint32.
      se_netbuf_store_index(m_key_buf_server_version, SeKeyDef::SERVER_VERSION);
      m_key_slice_server_version = common::Slice(reinterpret_cast<char *>(m_key_buf_server_version), SeKeyDef::SERVER_VERSION_SIZE);
  }
  //resume_drop_indexes();
  //rollback_ongoing_index_creation();

  return FAILED(ret);
}

std::unique_ptr<db::WriteBatch> SeDictionaryManager::begin() const
{
  return std::unique_ptr<db::WriteBatch>(new db::WriteBatch);
}

int SeDictionaryManager::commit(db::WriteBatch *batch, const bool &sync) const
{
  int ret = common::Status::kOk;
  common::WriteOptions options;
  options.sync = sync;

  if (IS_NULL(batch)) {
    ret = common::Status::kInvalidArgument;
    HANDLER_LOG(ERROR, "SE: invalid argument", K(ret), KP(batch));
  } else if (FAILED(m_db->Write(options, batch).code())) {
    HANDLER_LOG(ERROR, "SE: failed to commit write batch");
    se_handle_io_error(common::Status(ret), SE_IO_ERROR_DICT_COMMIT);
  }
  if (IS_NOTNULL(batch)) {
    batch->Clear();
  }

  return ret;
}

void SeDictionaryManager::put_key(db::WriteBatchBase *const batch,
                                  const common::Slice &key,
                                  const common::Slice &value) const
{
  batch->Put(m_system_cfh, key, value);
}

common::Status SeDictionaryManager::get_value(const common::Slice &key, std::string *const value) const
{
  common::ReadOptions options;
  options.total_order_seek = true;
  return m_db->Get(options, m_system_cfh, key, value);
}

void SeDictionaryManager::delete_key(db::WriteBatchBase *batch, const common::Slice &key) const
{
  batch->Delete(m_system_cfh, key);
}

db::Iterator *SeDictionaryManager::new_iterator() const
{
  /* Reading data dictionary should always skip bloom filter */
  common::ReadOptions read_options;
  read_options.total_order_seek = true;
  return m_db->NewIterator(read_options, m_system_cfh);
}

void SeDictionaryManager::dump_index_id(uchar *const netbuf,
                                        SeKeyDef::DATA_DICT_TYPE dict_type,
                                        const GL_INDEX_ID &gl_index_id)
{
  se_netbuf_store_uint32(netbuf, dict_type);
  se_netbuf_store_uint32(netbuf + SeKeyDef::INDEX_NUMBER_SIZE,
                          gl_index_id.cf_id);
  se_netbuf_store_uint32(netbuf + 2 * SeKeyDef::INDEX_NUMBER_SIZE,
                          gl_index_id.index_id);
}

void SeDictionaryManager::delete_with_prefix(db::WriteBatch *const batch,
                                             SeKeyDef::DATA_DICT_TYPE dict_type,
                                             const GL_INDEX_ID &gl_index_id) const
{
  uchar key_buf[SeKeyDef::INDEX_NUMBER_SIZE * 3] = {0};
  dump_index_id(key_buf, dict_type, gl_index_id);
  common::Slice key = common::Slice((char *)key_buf, sizeof(key_buf));

  delete_key(batch, key);
}

void SeDictionaryManager::drop_cf_flags(db::WriteBatch *const batch, uint32_t cf_id) const
{
  uchar key_buf[SeKeyDef::INDEX_NUMBER_SIZE * 2] = {0};
  se_netbuf_store_uint32(key_buf, SeKeyDef::CF_DEFINITION);
  se_netbuf_store_uint32(key_buf + SeKeyDef::INDEX_NUMBER_SIZE, cf_id);

  delete_key(batch, common::Slice((char *)key_buf, sizeof(key_buf)));
}

void SeDictionaryManager::delete_index_info(db::WriteBatch *batch, const GL_INDEX_ID &gl_index_id) const
{
  delete_with_prefix(batch, SeKeyDef::INDEX_INFO, gl_index_id);
  delete_with_prefix(batch, SeKeyDef::INDEX_STATISTICS, gl_index_id);
  drop_cf_flags(batch, gl_index_id.index_id);
}

bool SeDictionaryManager::get_index_info(const GL_INDEX_ID &gl_index_id,
                                         uint16_t *m_index_dict_version,
                                         uchar *m_index_type,
                                         uint16_t *kv_version) const
{
  bool found = false;
  bool error = false;
  std::string value;
  uchar key_buf[SeKeyDef::INDEX_NUMBER_SIZE * 3] = {0};
  dump_index_id(key_buf, SeKeyDef::INDEX_INFO, gl_index_id);
  const common::Slice &key = common::Slice((char *)key_buf, sizeof(key_buf));

  const common::Status &status = get_value(key, &value);
  if (status.ok()) {
    const uchar *const val = (const uchar *)value.c_str();
    const uchar *ptr = val;
    *m_index_dict_version = se_netbuf_to_uint16(val);
    *kv_version = 0;
    *m_index_type = 0;
    ptr += 2;
    switch (*m_index_dict_version) {

    case SeKeyDef::INDEX_INFO_VERSION_VERIFY_KV_FORMAT:
    case SeKeyDef::INDEX_INFO_VERSION_GLOBAL_ID:
      *m_index_type = se_netbuf_to_byte(ptr);
      ptr += 1;
      *kv_version = se_netbuf_to_uint16(ptr);
      found = true;
      break;

    default:
      error = true;
      break;
    }

    switch (*m_index_type) {
    case SeKeyDef::INDEX_TYPE_PRIMARY:
    case SeKeyDef::INDEX_TYPE_HIDDEN_PRIMARY: {
      error = *kv_version > SeKeyDef::PRIMARY_FORMAT_VERSION_LATEST;
      break;
    }
    case SeKeyDef::INDEX_TYPE_SECONDARY:
      error = *kv_version > SeKeyDef::SECONDARY_FORMAT_VERSION_LATEST;
      break;
    default:
      error = true;
      break;
    }
  }

  if (error) {
    // NO_LINT_DEBUG
    sql_print_error("SE: Found invalid key version number (%u, %u, %u) "
                    "from data dictionary. This should never happen "
                    "and it may be a bug.",
                    *m_index_dict_version, *m_index_type, *kv_version);
    abort_with_stack_traces();
  }

  return found;
}

bool SeDictionaryManager::is_valid_index_version(uint16_t index_dict_version)
{
  bool error;
  switch (index_dict_version) {
    case SeKeyDef::INDEX_INFO_VERSION_VERIFY_KV_FORMAT:
    case SeKeyDef::INDEX_INFO_VERSION_GLOBAL_ID: {
      error = false;
      break;
    }
    default: {
      error = true;
      break;
    }
  }

  return !error;
}

bool SeDictionaryManager::is_valid_kv_version(uchar index_type, uint16_t kv_version)
{
  bool error = true;
  switch (index_type) {
    case SeKeyDef::INDEX_TYPE_PRIMARY:
    case SeKeyDef::INDEX_TYPE_HIDDEN_PRIMARY: {
      error = kv_version > SeKeyDef::PRIMARY_FORMAT_VERSION_LATEST;
      break;
    }
    case SeKeyDef::INDEX_TYPE_SECONDARY: {
      error = kv_version > SeKeyDef::SECONDARY_FORMAT_VERSION_LATEST;
      break;
    }
    default: {
      error = true;
      break;
    }
  }
  return !error;
}

bool SeDictionaryManager::get_cf_flags(const uint32_t &cf_id, uint32_t *const cf_flags) const
{
  bool found = false;
  std::string value;
  uchar key_buf[SeKeyDef::INDEX_NUMBER_SIZE * 2] = {0};
  se_netbuf_store_uint32(key_buf, SeKeyDef::CF_DEFINITION);
  se_netbuf_store_uint32(key_buf + SeKeyDef::INDEX_NUMBER_SIZE, cf_id);
  const common::Slice key = common::Slice((char *)key_buf, sizeof(key_buf));

  const common::Status status = get_value(key, &value);
  if (status.ok()) {
    const uchar *val = (const uchar *)value.c_str();
    uint16_t version = se_netbuf_to_uint16(val);
    if (version == SeKeyDef::CF_DEFINITION_VERSION) {
      *cf_flags = se_netbuf_to_uint32(val + SeKeyDef::VERSION_SIZE);
      found = true;
    }
  }
  return found;
}

/*
  Returning index ids that were marked as deleted (via DROP TABLE) but
  still not removed by drop_index_thread yet, or indexes that are marked as
  ongoing creation.
 */
void SeDictionaryManager::get_ongoing_index_operation(
    std::unordered_set<GL_INDEX_ID> *gl_index_ids,
    SeKeyDef::DATA_DICT_TYPE dd_type) const
{
  assert(dd_type == SeKeyDef::DDL_DROP_INDEX_ONGOING ||
              dd_type == SeKeyDef::DDL_CREATE_INDEX_ONGOING);

  uchar index_buf[SeKeyDef::INDEX_NUMBER_SIZE];
  se_netbuf_store_uint32(index_buf, dd_type);
  const common::Slice index_slice(reinterpret_cast<char *>(index_buf),
                                   SeKeyDef::INDEX_NUMBER_SIZE);

  db::Iterator *it = new_iterator();
  for (it->Seek(index_slice); it->Valid(); it->Next()) {
    common::Slice key = it->key();
    const uchar *const ptr = (const uchar *)key.data();

    /*
      Ongoing drop/create index operations require key to be of the form:
      dd_type + cf_id + index_id (== INDEX_NUMBER_SIZE * 3)

      This may need to be changed in the future if we want to process a new
      ddl_type with different format.
    */
    if (key.size() != SeKeyDef::INDEX_NUMBER_SIZE * 3 ||
        se_netbuf_to_uint32(ptr) != dd_type) {
      break;
    }

    // We don't check version right now since currently we always store only
    // SeKeyDef::DDL_DROP_INDEX_ONGOING_VERSION = 1 as a value.
    // If increasing version number, we need to add version check logic here.
    GL_INDEX_ID gl_index_id;
    gl_index_id.cf_id =
        se_netbuf_to_uint32(ptr + SeKeyDef::INDEX_NUMBER_SIZE);
    gl_index_id.index_id =
        se_netbuf_to_uint32(ptr + 2 * SeKeyDef::INDEX_NUMBER_SIZE);
    gl_index_ids->insert(gl_index_id);
  }
  MOD_DELETE_OBJECT(Iterator, it);
}

bool SeDictionaryManager::get_max_index_id(uint32_t *const index_id) const
{
  bool found = false;
  std::string value;

  const common::Status status = get_value(m_key_slice_max_index_id, &value);
  if (status.ok()) {
    const uchar *const val = (const uchar *)value.c_str();
    const uint16_t &version = se_netbuf_to_uint16(val);
    if (version == SeKeyDef::MAX_INDEX_ID_VERSION) {
      *index_id = se_netbuf_to_uint32(val + SeKeyDef::VERSION_SIZE);
      found = true;
    }
  }
  return found;
}

int SeDictionaryManager::update_max_index_id(db::WriteBatch *batch, uint32_t index_id) const
{
  int ret = common::Status::kOk;
  uint32_t old_index_id = -1;
  uchar value_buf[SeKeyDef::VERSION_SIZE + SeKeyDef::INDEX_NUMBER_SIZE] = {0};
  common::Slice value;

  if (IS_NULL(batch)) {
    ret = common::Status::kInvalidArgument;
    HANDLER_LOG(ERROR, "SE: invalid argument", K(ret), KP(batch));
  } else {
    if (get_max_index_id(&old_index_id)) {
      if (old_index_id > index_id) {
        ret = common::Status::kErrorUnexpected;
        HANDLER_LOG(ERROR, "SE: found max index id from data dictionary "
                    "but trying to update to older value. This should never "
                    "happen and possibly a bug.",
                    K(old_index_id), K(index_id));
      }
    }

    if (SUCCED(ret)) {
      se_netbuf_store_uint16(value_buf, SeKeyDef::MAX_INDEX_ID_VERSION);
      se_netbuf_store_uint32(value_buf + SeKeyDef::VERSION_SIZE, index_id);
      value.assign((char *)value_buf, sizeof(value_buf));
      if (FAILED(batch->Put(m_system_cfh, m_key_slice_max_index_id, value).code())) {
        HANDLER_LOG(ERROR, "SE: failed to put max index id to write batch", K(ret));
      }
    }
  }

  return ret;
}

bool SeDictionaryManager::get_system_cf_version(uint16_t* system_cf_version) const
{
  assert(system_cf_version != nullptr);
  uchar key_buf[SeKeyDef::INDEX_NUMBER_SIZE] = {0};
  se_netbuf_store_index(key_buf, SeKeyDef::SYSTEM_CF_VERSION_INDEX);
  common::Slice key_slice(reinterpret_cast<char *>(key_buf),
                                   SeKeyDef::INDEX_NUMBER_SIZE);
  bool found = false;
  std::string value;
  auto status = get_value(key_slice, &value);
  if (status.ok()) {
    found = true;
    *system_cf_version = se_netbuf_to_uint16((const uchar *)value.c_str());
  }
  return found;
}

bool SeDictionaryManager::update_system_cf_version(
  db::WriteBatch *const batch,
  uint16_t system_cf_version) const
{
  assert(batch != nullptr);
  uchar key_buf[SeKeyDef::INDEX_NUMBER_SIZE] = {0};
  se_netbuf_store_index(key_buf, SeKeyDef::SYSTEM_CF_VERSION_INDEX);
  common::Slice key_slice(reinterpret_cast<char *>(key_buf),
                                   SeKeyDef::INDEX_NUMBER_SIZE);

  uint16_t version_in_dict = 0;
  std::string value;
  auto status = get_value(key_slice, &value);
  if (status.ok()) {
    version_in_dict = se_netbuf_to_uint16((const uchar *)value.c_str());
    if (0 != version_in_dict && version_in_dict > system_cf_version) {
      HANDLER_LOG(ERROR, "SE: set older version is disallowed.",
                  K(version_in_dict), K(system_cf_version));
      return true;
    }
  }

  if (version_in_dict != system_cf_version) {
    uchar value_buf[SeKeyDef::VERSION_SIZE] = {0};
    se_netbuf_store_uint16(value_buf, system_cf_version);
    common::Slice value_slice((char *) value_buf, SeKeyDef::VERSION_SIZE);
    batch->Put(m_system_cfh, key_slice, value_slice);
  }

  return false;
}

bool SeDictionaryManager::get_max_table_id(uint64_t *table_id) const
{
  assert(table_id != nullptr);
  bool found = false;
  std::string value;
  auto status = get_value(m_key_slice_max_table_id, &value);
  if (status.ok()) {
    auto val = (const uchar *)value.c_str();
    const uint16_t &version = se_netbuf_to_uint16(val);
    if (version == SeKeyDef::MAX_TABLE_ID_VERSION) {
      *table_id = se_netbuf_to_uint64(val + SeKeyDef::VERSION_SIZE);
      found = true;
    }
  }
  return found;
}

bool SeDictionaryManager::update_max_table_id(
    db::WriteBatch *const batch,
    uint64_t table_id) const
{
  assert(batch != nullptr);
  uint64_t max_table_id_in_dict = dd::INVALID_OBJECT_ID;
  if (get_max_table_id(&max_table_id_in_dict)) {
    if (dd::INVALID_OBJECT_ID != max_table_id_in_dict &&
        max_table_id_in_dict > table_id) {
      HANDLER_LOG(ERROR, "SE: found max table id from data dictionary but"
                  " trying to update to older value. This should never"
                  "happen and possibly a bug.",
                  K(max_table_id_in_dict), K(table_id));
      return true;
    }
  }

  if (max_table_id_in_dict != table_id) {
    uchar value_buf[SeKeyDef::VERSION_SIZE + SeKeyDef::TABLE_ID_SIZE] = {0};
    se_netbuf_store_uint16(value_buf, SeKeyDef::MAX_TABLE_ID_VERSION);
    se_netbuf_store_uint64(value_buf + SeKeyDef::VERSION_SIZE, table_id);
    auto value = common::Slice((char *) value_buf, sizeof(value_buf));
    batch->Put(m_system_cfh, m_key_slice_max_table_id, value);
  }
  return false;
}

void SeDictionaryManager::add_stats(
    db::WriteBatch *const batch,
    const std::vector<SeIndexStats> &stats) const
{
  assert(batch != nullptr);

  for (const auto &it : stats) {
    uchar key_buf[SeKeyDef::INDEX_NUMBER_SIZE * 3] = {0};
    dump_index_id(key_buf, SeKeyDef::INDEX_STATISTICS, it.m_gl_index_id);

    // IndexStats::materialize takes complete care of serialization including
    // storing the version
    const auto value =
        SeIndexStats::materialize(std::vector<SeIndexStats>{it}, 1.);

    batch->Put(m_system_cfh, common::Slice((char *)key_buf, sizeof(key_buf)),
               value);
  }
}

SeIndexStats SeDictionaryManager::get_stats(GL_INDEX_ID gl_index_id) const
{
  uchar key_buf[SeKeyDef::INDEX_NUMBER_SIZE * 3] = {0};
  dump_index_id(key_buf, SeKeyDef::INDEX_STATISTICS, gl_index_id);

  std::string value;
  const common::Status status = get_value(
      common::Slice(reinterpret_cast<char *>(key_buf), sizeof(key_buf)),
      &value);
  if (status.ok()) {
    std::vector<SeIndexStats> v;
    // unmaterialize checks if the version matches
    if (SeIndexStats::unmaterialize(value, &v) == 0 && v.size() == 1) {
      return v[0];
    }
  }

  return SeIndexStats();
}

int SeDictionaryManager::set_server_version()
{
  std::unique_ptr<db::WriteBatch> batch;
  common::Slice server_version_value_slice;
  uint32_t server_version = MYSQL_VERSION_ID;
  uchar value_buf[SeKeyDef::VERSION_SIZE + SeKeyDef::SERVER_VERSION_SIZE] = {0};

  // Build server version value buf.
  se_netbuf_store_uint16(value_buf, SeKeyDef::SERVER_VERSION_VERSION);
  se_netbuf_store_uint32(value_buf + SeKeyDef::VERSION_SIZE, server_version);
  server_version_value_slice = common::Slice((char *)value_buf, sizeof(value_buf));

  // Put server version to data dictionary.
  batch = begin();
  batch->Put(m_system_cfh, m_key_slice_server_version, server_version_value_slice);
  commit(batch.get(), true /* sync */);

  return common::Status::kOk;
}

int SeDictionaryManager::get_server_version(uint *version)
{
  assert(nullptr != version);
  int ret = common::Status::kOk;
  std::string server_version_value;
  uint16_t server_version_version = 0;
  uint32_t server_version = 0;

  if (FAILED(get_value(m_key_slice_server_version, &server_version_value).code())) {
    HANDLER_LOG(WARN, "SE: get server version failed", K(ret));
  } else {
    // Parse server version version.
    server_version_version = se_netbuf_to_uint16((const uchar *)server_version_value.c_str());
    if (SeKeyDef::SERVER_VERSION_VERSION != server_version_version) {
      ret = common::Status::kErrorUnexpected;
      HANDLER_LOG(WARN, "SE: server version version mismatch", K(ret), K(server_version_version));
    } else {
      // Parse server version.
      server_version = se_netbuf_to_uint32((const uchar *)server_version_value.c_str() + SeKeyDef::VERSION_SIZE);
      *version = server_version;
    }
  }

  return ret;
}

int SeDictionaryManager::insert_dd_table_id(dd::Object_id table_id)
{
  int ret = common::Status::kOk;

  if (!(dd_table_ids.insert(table_id).second)) {
    ret = common::Status::kErrorUnexpected;
    HANDLER_LOG(ERROR, "SE: insert dd table id failed", K(ret), KE(table_id));
  }

  return ret;
}

} //namespace smartengine
