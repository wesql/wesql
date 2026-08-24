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
#include "se_index.h"
#include "field.h"
#include "sql_table.h"
#include "table.h"
#include <bit>
#include "dict/se_field_pack.h"
#include "dict/se_table.h"
#include "dict/se_cf_manager.h"
#include "dict/se_dd_operations.h"
#include "dict/se_dict_util.h"
#include "handler/handler_alter.h"
#include "handler/se_hton.h"
#include "schema/record_format.h"

namespace smartengine {

extern SeSubtableManager cf_manager;

SeKeyDef::SeKeyDef(uint indexnr_arg,
                   uint keyno_arg,
                   uint16_t index_dict_version_arg,
                   uchar index_type_arg,
                   uint16_t kv_format_version_arg,
                   const std::string &name,
                   const SeIndexStats &_stats)
    : m_index_number(indexnr_arg),
      m_cf_handle(nullptr),
      m_index_dict_version(index_dict_version_arg),
      m_index_type(index_type_arg),
      m_kv_format_version(kv_format_version_arg),
      m_is_reverse_cf(false),
      m_is_auto_cf(false),
      m_name(name),
      m_stats(_stats),
      m_pk_part_no(nullptr),
      m_pack_info(nullptr),
      m_keyno(keyno_arg),
      m_key_parts(0),
      m_maxlength(0) // means 'not intialized'
{
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
  se_netbuf_store_index(m_index_number_storage_form, m_index_number);
}

SeKeyDef::SeKeyDef(uint indexnr_arg,
                   uint keyno_arg,
                   db::ColumnFamilyHandle *cf_handle_arg,
                   uint16_t index_dict_version_arg,
                   uchar index_type_arg,
                   uint16_t kv_format_version_arg,
                   bool is_reverse_cf_arg,
                   bool is_auto_cf_arg,
                   const std::string &name,
                   const SeIndexStats &_stats)
    : m_index_number(indexnr_arg),
      m_cf_handle(cf_handle_arg),
      m_index_dict_version(index_dict_version_arg),
      m_index_type(index_type_arg),
      m_kv_format_version(kv_format_version_arg),
      m_is_reverse_cf(is_reverse_cf_arg),
      m_is_auto_cf(is_auto_cf_arg),
      m_name(name),
      m_stats(_stats),
      m_pk_part_no(nullptr),
      m_pack_info(nullptr),
      m_keyno(keyno_arg),
      m_key_parts(0),
      m_maxlength(0) // means 'not intialized'
{
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
  se_netbuf_store_index(m_index_number_storage_form, m_index_number);
  assert(m_cf_handle != nullptr);
}

SeKeyDef::SeKeyDef(const SeKeyDef &k)
    : m_index_number(k.m_index_number),
      m_cf_handle(k.m_cf_handle),
      m_is_reverse_cf(k.m_is_reverse_cf),
      m_is_auto_cf(k.m_is_auto_cf),
      m_name(k.m_name),
      m_stats(k.m_stats),
      m_pk_part_no(k.m_pk_part_no),
      m_pack_info(k.m_pack_info),
      m_keyno(k.m_keyno),
      m_key_parts(k.m_key_parts),
      m_maxlength(k.m_maxlength)
{
  mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
  se_netbuf_store_index(m_index_number_storage_form, m_index_number);
  if (k.m_pack_info) {
    const size_t size = sizeof(SeFieldPacking) * k.m_key_parts;
    m_pack_info =
        reinterpret_cast<SeFieldPacking *>(my_malloc(PSI_NOT_INSTRUMENTED, size, MYF(0)));
    memcpy((void *)(m_pack_info), k.m_pack_info, size);
  }

  if (k.m_pk_part_no) {
    const size_t size = sizeof(uint) * m_key_parts;
    m_pk_part_no = reinterpret_cast<uint *>(my_malloc(PSI_NOT_INSTRUMENTED, size, MYF(0)));
    memcpy(m_pk_part_no, k.m_pk_part_no, size);
  }
}

SeKeyDef::~SeKeyDef()
{
  mysql_mutex_destroy(&m_mutex);

  my_free(m_pk_part_no);
  m_pk_part_no = nullptr;

  my_free(m_pack_info);
  m_pack_info = nullptr;
}

void SeKeyDef::setup(const TABLE *const tbl, const SeTableDef *const tbl_def)
{
  assert(tbl != nullptr);
  assert(tbl_def != nullptr);

  /*
    Set max_length based on the table.  This can be called concurrently from
    multiple threads, so there is a mutex to protect this code.
  */
  const bool is_hidden_pk = (m_index_type == INDEX_TYPE_HIDDEN_PRIMARY);
  const bool hidden_pk_exists = table_has_hidden_pk(tbl);
  const bool secondary_key = (m_index_type == INDEX_TYPE_SECONDARY);
  if (!m_maxlength) {
    SE_MUTEX_LOCK_CHECK(m_mutex);
    if (m_maxlength != 0) {
      SE_MUTEX_UNLOCK_CHECK(m_mutex);
      return;
    }

    KEY *key_info = nullptr;
    KEY *pk_info = nullptr;
    if (!is_hidden_pk) {
      key_info = &tbl->key_info[m_keyno];
      if (!hidden_pk_exists)
        pk_info = &tbl->key_info[tbl->s->primary_key];
      m_name = std::string(key_info->name);
    } else {
      m_name = HIDDEN_PK_NAME;
    }

    if (secondary_key)
      m_pk_key_parts = hidden_pk_exists ? 1 : pk_info->actual_key_parts;
    else {
      pk_info = nullptr;
      m_pk_key_parts = 0;
    }

    // "unique" secondary keys support:
    m_key_parts = is_hidden_pk ? 1 : key_info->actual_key_parts;

    if (secondary_key) {
      /*
        In most cases, SQL layer puts PK columns as invisible suffix at the
        end of secondary key. There are cases where this doesn't happen:
        - unique secondary indexes.
        - partitioned tables.

        Internally, we always need PK columns as suffix (and InnoDB does,
        too, if you were wondering).

        The loop below will attempt to put all PK columns at the end of key
        definition.  Columns that are already included in the index (either
        by the user or by "extended keys" feature) are not included for the
        second time.
      */
      m_key_parts += m_pk_key_parts;
    }

    if (secondary_key)
      m_pk_part_no = reinterpret_cast<uint *>(
          my_malloc(PSI_NOT_INSTRUMENTED, sizeof(uint) * m_key_parts, MYF(0)));
    else
      m_pk_part_no = nullptr;

    const size_t size = sizeof(SeFieldPacking) * m_key_parts;
    m_pack_info =
        reinterpret_cast<SeFieldPacking *>(my_malloc(PSI_NOT_INSTRUMENTED, size, MYF(0)));

    size_t max_len = INDEX_NUMBER_SIZE;
    int unpack_len = 0;
    int max_part_len = 0;
    bool simulating_extkey = false;
    uint dst_i = 0;

    uint keyno_to_set = m_keyno;
    uint keypart_to_set = 0;
    bool can_extract_from_index = true;

    if (is_hidden_pk) {
      Field *field = nullptr;
      m_pack_info[dst_i].setup(this, field, keyno_to_set, 0, 0);
      m_pack_info[dst_i].m_unpack_data_offset = unpack_len;
      max_len += m_pack_info[dst_i].m_max_image_len;
      max_part_len = std::max(max_part_len, m_pack_info[dst_i].m_max_image_len);
      dst_i++;
    } else {
      KEY_PART_INFO *key_part = key_info->key_part;

      /* this loop also loops over the 'extended key' tail */
      for (uint src_i = 0; src_i < m_key_parts; src_i++, keypart_to_set++) {
        Field *const field = key_part ? key_part->field : nullptr;

        if (simulating_extkey && !hidden_pk_exists) {
          assert(secondary_key);
          /* Check if this field is already present in the key definition */
          bool found = false;
          for (uint j = 0; j < key_info->actual_key_parts; j++) {
            if (field->field_index() ==
                    key_info->key_part[j].field->field_index() &&
                key_part->length == key_info->key_part[j].length) {
              found = true;
              break;
            }
          }

          if (found) {
            key_part++;
            continue;
          }
        }

        if (field && field->is_nullable())
          max_len += 1; // NULL-byte

        if (!m_pack_info[dst_i].setup(this, field, keyno_to_set, keypart_to_set,
                                 key_part ? key_part->length : 0)) {
          can_extract_from_index = false;
        }

        m_pack_info[dst_i].m_unpack_data_offset = unpack_len;

        if (pk_info) {
          m_pk_part_no[dst_i] = -1;
          for (uint j = 0; j < m_pk_key_parts; j++) {
            if (field->field_index() == pk_info->key_part[j].field->field_index()) {
              m_pk_part_no[dst_i] = j;
              break;
            }
          }
        } else if (secondary_key && hidden_pk_exists) {
          /*
            The hidden pk can never be part of the sk.  So it is always
            appended to the end of the sk.
          */
          m_pk_part_no[dst_i] = -1;
          if (simulating_extkey)
            m_pk_part_no[dst_i] = 0;
        }

        max_len += m_pack_info[dst_i].m_max_image_len;

        max_part_len =
            std::max(max_part_len, m_pack_info[dst_i].m_max_image_len);

        key_part++;
        /*
          For "unique" secondary indexes, pretend they have
          "index extensions"
         */
        if (secondary_key && src_i + 1 == key_info->actual_key_parts) {
          simulating_extkey = true;
          if (!hidden_pk_exists) {
            keyno_to_set = tbl->s->primary_key;
            key_part = pk_info->key_part;
            keypart_to_set = (uint)-1;
          } else {
            keyno_to_set = tbl_def->m_key_count - 1;
            key_part = nullptr;
            keypart_to_set = 0;
          }
        }

        dst_i++;
      }
    }

    m_is_support_icp = can_extract_from_index;
    m_key_parts = dst_i;

    /* Initialize the memory needed by the stats structure */
    m_stats.m_distinct_keys_per_prefix.resize(get_key_parts());

    /*
      This should be the last member variable set before releasing the mutex
      so that other threads can't see the object partially set up.
     */
    m_maxlength = max_len;

    SE_MUTEX_UNLOCK_CHECK(m_mutex);
  }
}

/**
  Read a memcmp key part from a slice using the passed in reader.

  Returns -1 if field was null, 1 if error, 0 otherwise.
*/
int SeKeyDef::read_memcmp_key_part(const TABLE *table_arg,
                                   SeStringReader *reader,
                                   const uint part_num) const
{
  /* It is impossible to unpack the column. Skip it. */
  if (m_pack_info[part_num].m_maybe_null) {
    const char *nullp;
    if (!(nullp = reader->read(1)))
      return 1;
    if (*nullp == 0) {
      /* This is a NULL value */
      return -1;
    } else {
      /* If NULL marker is not '0', it can be only '1'  */
      if (*nullp != 1)
        return 1;
    }
  }

  SeFieldPacking *fpi = &m_pack_info[part_num];
  assert(table_arg->s != nullptr);

  bool is_hidden_pk_part = (part_num + 1 == m_key_parts) &&
                           (table_arg->s->primary_key == MAX_INDEXES);
  Field *field = nullptr;
  if (!is_hidden_pk_part)
    field = fpi->get_field_in_table(table_arg);
  if (fpi->m_skip_func(fpi, field, reader))
    return 1;

  return 0;
}

/**
  Get a mem-comparable form of Primary Key from mem-comparable form of this key

  @param
    pk_descr        Primary Key descriptor
    key             Index tuple from this key in mem-comparable form
    pk_buffer  OUT  Put here mem-comparable form of the Primary Key.

  @note
    It may or may not be possible to restore primary key columns to their
    mem-comparable form.  To handle all cases, this function copies mem-
    comparable forms directly.

    SE SE supports "Extended keys". This means that PK columns are present
    at the end of every key.  If the key already includes PK columns, then
    these columns are not present at the end of the key.

    Because of the above, we copy each primary key column.

  @todo
    If we checked crc32 checksums in this function, we would catch some CRC
    violations that we currently don't. On the other hand, there is a broader
    set of queries for which we would check the checksum twice.
*/
uint SeKeyDef::get_primary_key_tuple(const TABLE *const table,
                                     const SeKeyDef &pk_descr,
                                     const common::Slice *const key,
                                     uchar *const pk_buffer) const
{
  assert(table != nullptr);
  assert(key != nullptr);
  assert(pk_buffer);

  uint size = 0;
  uchar *buf = pk_buffer;
  assert(m_pk_key_parts);

  /* Put the PK number */
  se_netbuf_store_index(buf, pk_descr.m_index_number);
  buf += INDEX_NUMBER_SIZE;
  size += INDEX_NUMBER_SIZE;

  const char *start_offs[MAX_REF_PARTS];
  const char *end_offs[MAX_REF_PARTS];
  int pk_key_part;
  uint i;
  SeStringReader reader(key);

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE)))
    return SE_INVALID_KEY_LEN;

  for (i = 0; i < m_key_parts; i++) {
    if ((pk_key_part = m_pk_part_no[i]) != -1) {
      start_offs[pk_key_part] = reader.get_current_ptr();
    }

    if (UNLIKELY(read_memcmp_key_part(table, &reader, i) > 0)) {
      return SE_INVALID_KEY_LEN;
    }

    if (pk_key_part != -1) {
      end_offs[pk_key_part] = reader.get_current_ptr();
    }
  }

  for (i = 0; i < m_pk_key_parts; i++) {
    const uint part_size = end_offs[i] - start_offs[i];
    memcpy(buf, start_offs[i], end_offs[i] - start_offs[i]);
    buf += part_size;
    size += part_size;
  }

  return size;
}

uint SeKeyDef::get_memcmp_sk_size(const TABLE *table,
                                  const common::Slice &key,
                                  uint *n_null_fields) const
{
  assert(table != nullptr);
  assert(n_null_fields != nullptr);
  assert(m_keyno != table->s->primary_key);

  int res;
  SeStringReader reader(&key);
  const char *start = reader.get_current_ptr();

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE)))
    return SE_INVALID_KEY_LEN;

  for (uint i = 0; i < table->key_info[m_keyno].user_defined_key_parts; i++) {
    if ((res = read_memcmp_key_part(table, &reader, i)) > 0) {
      return SE_INVALID_KEY_LEN;
    } else if (res == -1) {
      (*n_null_fields)++;
    }
  }

  return (reader.get_current_ptr() - start);
}

/**
  Get a mem-comparable form of Secondary Key from mem-comparable form of this
  key, without the extended primary key tail.

  @param
    key                Index tuple from this key in mem-comparable form
    sk_buffer     OUT  Put here mem-comparable form of the Secondary Key.
    n_null_fields OUT  Put number of null fields contained within sk entry
*/
uint SeKeyDef::get_memcmp_sk_parts(const TABLE *table,
                                   const common::Slice &key,
                                   uchar *sk_buffer,
                                   uint *n_null_fields) const
{
  assert(table != nullptr);
  assert(sk_buffer != nullptr);
  assert(n_null_fields != nullptr);
  assert(m_keyno != table->s->primary_key);

  uchar *buf = sk_buffer;
  const char *start = key.data();

  uint sk_memcmp_len = get_memcmp_sk_size(table, key, n_null_fields);
  if (sk_memcmp_len == SE_INVALID_KEY_LEN) {
    return SE_INVALID_KEY_LEN;
  }

  memcpy(buf, start, sk_memcmp_len);

  return sk_memcmp_len;
}

/**
  Convert index tuple into storage (i.e. mem-comparable) format

  @detail
    Currently this is done by unpacking into table->record[0] and then
    packing index columns into storage format.

  @param pack_buffer Temporary area for packing varchar columns. Its
                     size is at least max_storage_fmt_length() bytes.
*/

uint SeKeyDef::pack_index_tuple(TABLE *tbl, uchar *pack_buffer,
                                uchar *packed_tuple,
                                const uchar *key_tuple,
                                key_part_map &keypart_map) const
{
  assert(tbl != nullptr);
  assert(pack_buffer != nullptr);
  assert(packed_tuple != nullptr);
  assert(key_tuple != nullptr);

  uchar* key_tuple_local = const_cast<uchar*>(key_tuple);

  /* We were given a record in KeyTupleFormat. First, save it to record */
  uint key_len = calculate_key_len(tbl, m_keyno, keypart_map);
  key_restore(tbl->record[0], key_tuple_local, &tbl->key_info[m_keyno], key_len);

  uint n_used_parts = std::popcount(keypart_map);
  if (keypart_map == HA_WHOLE_KEY)
    n_used_parts = 0; // Full key is used

  /* Then, convert the record into a mem-comparable form */
  return pack_record(tbl, pack_buffer, tbl->record[0], packed_tuple, nullptr,
                     false, 0, n_used_parts);
}

/**
  @brief
    Check if "unpack info" data includes checksum.

  @detail
    This is used only by CHECK TABLE to count the number of rows that have
    checksums.
*/

bool SeKeyDef::unpack_info_has_checksum(const common::Slice &unpack_info)
{
  const uchar *ptr = (const uchar *)unpack_info.data();
  size_t size = unpack_info.size();

  // Skip unpack info if present.
  if (size >= schema::RecordFormat::RECORD_UNPACK_HEADER_SIZE && ptr[0] == schema::RecordFormat::RECORD_UNPACK_DATA_FLAG) {
    const uint16 skip_len = se_netbuf_to_uint16(ptr + 1);
    SHIP_ASSERT(size >= skip_len);

    size -= skip_len;
    ptr += skip_len;
  }

  return (size == SE_CHECKSUM_CHUNK_SIZE && ptr[0] == SE_CHECKSUM_DATA_TAG);
}

/*
  @return Number of bytes that were changed
*/
int SeKeyDef::successor(uchar *const packed_tuple, const uint &len)
{
  assert(packed_tuple != nullptr);

  int changed = 0;
  uchar *p = packed_tuple + len - 1;
  for (; p > packed_tuple; p--) {
    changed++;
    if (*p != uchar(0xFF)) {
      *p = *p + 1;
      break;
    }
    *p = '\0';
  }
  return changed;
}

int SeKeyDef::pack_field(Field *const field,
                         SeFieldPacking *pack_info,
                         uchar *&tuple,
                         uchar *const packed_tuple,
                         uchar *const pack_buffer,
                         SeStringWriter *const unpack_info,
                         uint *const n_null_fields) const
{
  if (pack_info->m_maybe_null) {
    assert(is_storage_available(tuple - packed_tuple, 1));
    if (field->is_real_null()) {
      /* NULL value. store '\0' so that it sorts before non-NULL values */
      *tuple++ = 0;
      /* That's it, don't store anything else */
      if (n_null_fields)
        (*n_null_fields)++;
      return HA_EXIT_SUCCESS;
    } else {
      /* Not a NULL value. Store '1' */
      *tuple++ = 1;
    }
  } else {
    /*
     * field->real_maybe_null() may be not correct, if a nullable column
     * be a part of new table pk after rebuild ddl. In that case,
     * field in old_table is nullable, while field in new_table is not nullable.
     * When we use old_field to pack memcmp key, there will be wrong.
     */
    if (field->is_real_null()) {
      return HA_ERR_INVALID_NULL_ERROR;
    }
  }

  const bool create_unpack_info =
      (unpack_info &&  // we were requested to generate unpack_info
       pack_info->uses_unpack_info());  // and this keypart uses it
  SePackFieldContext pack_ctx(unpack_info);

  // Set the offset for methods which do not take an offset as an argument
  assert(is_storage_available(tuple - packed_tuple,
                                   pack_info->m_max_image_len));

  pack_info->m_pack_func(pack_info, field, pack_buffer, &tuple, &pack_ctx);

  /* Make "unpack info" to be stored in the value */
  if (create_unpack_info) {
    pack_info->m_make_unpack_info_func(pack_info->m_charset_codec, field,
                                       &pack_ctx);
  }

  return HA_EXIT_SUCCESS;
}

/**
  Get index columns from the record and pack them into mem-comparable form.

  @param
    tbl                   Table we're working on
    record           IN   Record buffer with fields in table->record format
    pack_buffer      IN   Temporary area for packing varchars. The size is
                          at least max_storage_fmt_length() bytes.
    packed_tuple     OUT  Key in the mem-comparable form
    unpack_info      OUT  Unpack data
    unpack_info_len  OUT  Unpack data length
    n_key_parts           Number of keyparts to process. 0 means all of them.
    n_null_fields    OUT  Number of key fields with NULL value.

  @detail
    Some callers do not need the unpack information, they can pass
    unpack_info=nullptr, unpack_info_len=nullptr.

  @return
    Length of the packed tuple
*/

uint SeKeyDef::pack_record(const TABLE *const tbl,
                           uchar *const pack_buffer,
                           const uchar *const record,
                           uchar *const packed_tuple,
                           SeStringWriter *const unpack_info,
                           const bool &should_store_row_debug_checksums,
                           const longlong &hidden_pk_id,
                           uint n_key_parts,
                           uint *const n_null_fields,
                           const TABLE *const altered_table) const
{
  assert(tbl != nullptr);
  assert(pack_buffer != nullptr);
  assert(record != nullptr);
  assert(packed_tuple != nullptr);
  // Checksums for PKs are made when record is packed.
  // We should never attempt to make checksum just from PK values
  DBUG_ASSERT_IMP(should_store_row_debug_checksums,
                  (m_index_type == INDEX_TYPE_SECONDARY));

  uchar *tuple = packed_tuple;
  size_t unpack_len_pos = size_t(-1);
  const bool hidden_pk_exists = table_has_hidden_pk(tbl);

  se_netbuf_store_index(tuple, m_index_number);
  tuple += INDEX_NUMBER_SIZE;

  // If n_key_parts is 0, it means all columns.
  // The following includes the 'extended key' tail.
  // The 'extended key' includes primary key. This is done to 'uniqify'
  // non-unique indexes
  const bool use_all_columns = n_key_parts == 0 || n_key_parts == MAX_REF_PARTS;

  // If hidden pk exists, but hidden pk wasnt passed in, we can't pack the
  // hidden key part.  So we skip it (its always 1 part).
  if (hidden_pk_exists && !hidden_pk_id && use_all_columns)
    n_key_parts = m_key_parts - 1;
  else if (use_all_columns)
    n_key_parts = m_key_parts;

  if (n_null_fields)
    *n_null_fields = 0;

  if (unpack_info) {
    unpack_info->clear();
    unpack_info->write_uint8(schema::RecordFormat::RECORD_UNPACK_DATA_FLAG);
    unpack_len_pos = unpack_info->get_current_pos();
    // we don't know the total length yet, so write a zero
    unpack_info->write_uint16(0);
  }

  int ret = HA_EXIT_SUCCESS;
  for (uint i = 0; i < n_key_parts; i++) {
    // Fill hidden pk id into the last key part for secondary keys for tables
    // with no pk
    if (hidden_pk_exists && hidden_pk_id && i + 1 == n_key_parts) {
      m_pack_info[i].fill_hidden_pk_val(&tuple, hidden_pk_id);
      break;
    }

    Field *field = nullptr;
    uint field_index = 0;
    if (altered_table != nullptr) {
      field_index = m_pack_info[i].get_field_index_in_table(altered_table);
      field = tbl->field[field_index];
    } else {
      field = m_pack_info[i].get_field_in_table(tbl);
    }

    assert(field != nullptr);

    uint field_offset = field->field_ptr() - tbl->record[0];
    uint null_offset = field->null_offset(tbl->record[0]);
    bool maybe_null = field->is_nullable();
    field->move_field(const_cast<uchar*>(record) + field_offset,
        maybe_null ? const_cast<uchar*>(record) + null_offset : nullptr,
        field->null_bit);
    // WARNING! Don't return without restoring field->ptr and field->null_ptr

    ret = pack_field(field, &m_pack_info[i], tuple, packed_tuple, pack_buffer,
                       unpack_info, n_null_fields);

    if (ret != HA_ERR_INVALID_NULL_ERROR) {
      assert(ret == HA_EXIT_SUCCESS);
    }

    // Restore field->ptr and field->null_ptr
    field->move_field(tbl->record[0] + field_offset,
                      maybe_null ? tbl->record[0] + null_offset : nullptr,
                      field->null_bit);
  }

  if (unpack_info) {
    const size_t len = unpack_info->get_current_pos();
    assert(len <= std::numeric_limits<uint16_t>::max());

    // Don't store the unpack_info if it has only the header (that is, there's
    // no meaningful content).
    // Primary Keys are special: for them, store the unpack_info even if it's
    // empty (provided m_maybe_unpack_info==true, see
    // ha_smartengine::convert_record_to_storage_format)
    if (len == schema::RecordFormat::RECORD_UNPACK_HEADER_SIZE &&
        m_index_type != SeKeyDef::INDEX_TYPE_PRIMARY) {
      unpack_info->clear();
    } else {
      unpack_info->write_uint16_at(unpack_len_pos, len);
    }

    //
    // Secondary keys have key and value checksums in the value part
    // Primary key is a special case (the value part has non-indexed columns),
    // so the checksums are computed and stored by
    // ha_smartengine::convert_record_to_storage_format
    //
    if (should_store_row_debug_checksums) {
      const uint32_t key_crc32 = crc32(0, packed_tuple, tuple - packed_tuple);
      const uint32_t val_crc32 =
          crc32(0, unpack_info->ptr(), unpack_info->get_current_pos());

      unpack_info->write_uint8(SE_CHECKSUM_DATA_TAG);
      unpack_info->write_uint32(key_crc32);
      unpack_info->write_uint32(val_crc32);
    }
  }

  assert(is_storage_available(tuple - packed_tuple, 0));

  return tuple - packed_tuple;
}

/**
  Get index columns from the old table record and added default column,
  pack them into new table se mem-comparable key form.

  @param
    old_tbl               old Table we're working on
    record           IN   old Record buffer with fields in old_table->record format
    pack_buffer      IN   Temporary area for packing varchars. The size is
                          at least max_storage_fmt_length() bytes.
    packed_tuple     OUT  Key in the mem-comparable form
    unpack_info      OUT  Unpack data
    unpack_info_len  OUT  Unpack data length
    n_key_parts           Number of keyparts to process. 0 means all of them.
    n_null_fields    OUT  Number of key fields with NULL value.
    altered_table    IN   new table
    dict_info        IN   added new columns

  @detail
    Some callers do not need the unpack information, they can pass
    unpack_info=nullptr, unpack_info_len=nullptr.

  @return
    Length of the packed tuple
*/

int SeKeyDef::pack_new_record(const TABLE *const old_tbl,
                              uchar *const pack_buffer,
                              const uchar *const record,
                              uchar *const packed_tuple,
                              SeStringWriter *const unpack_info,
                              const bool &should_store_row_debug_checksums,
                              const longlong &hidden_pk_id,
                              uint n_key_parts,
                              uint *const n_null_fields,
                              const TABLE *const altered_table,
                              const std::shared_ptr<SeInplaceDdlDictInfo> dict_info,
                              uint &size) const
{
  assert(old_tbl != nullptr);
  assert(pack_buffer != nullptr);
  assert(record != nullptr);
  assert(packed_tuple != nullptr);
  // Checksums for PKs are made when record is packed.
  // We should never attempt to make checksum just from PK values
  DBUG_ASSERT_IMP(should_store_row_debug_checksums,
                  (m_index_type == INDEX_TYPE_SECONDARY));

  int ret = HA_EXIT_SUCCESS;

  uchar *tuple = packed_tuple;
  size_t unpack_len_pos = size_t(-1);
  const bool hidden_pk_exists = table_has_hidden_pk(altered_table);

  se_netbuf_store_index(tuple, m_index_number);
  tuple += INDEX_NUMBER_SIZE;

  // If n_key_parts is 0, it means all columns.
  // The following includes the 'extended key' tail.
  // The 'extended key' includes primary key. This is done to 'uniqify'
  // non-unique indexes
  const bool use_all_columns = n_key_parts == 0 || n_key_parts == MAX_REF_PARTS;

  // If hidden pk exists, but hidden pk wasnt passed in, we can't pack the
  // hidden key part.  So we skip it (its always 1 part).
  if (hidden_pk_exists && !hidden_pk_id && use_all_columns)
    n_key_parts = m_key_parts - 1;
  else if (use_all_columns)
    n_key_parts = m_key_parts;

  if (n_null_fields)
    *n_null_fields = 0;

  if (unpack_info) {
    unpack_info->clear();
    unpack_info->write_uint8(schema::RecordFormat::RECORD_UNPACK_DATA_FLAG);
    unpack_len_pos = unpack_info->get_current_pos();
    // we don't know the total length yet, so write a zero
    unpack_info->write_uint16(0);
  }

  // check field is/not old table field
  SeFieldEncoder *encoder_arr = dict_info->m_encoder_arr;
  uint *col_map = dict_info->m_col_map;
  uint old_col_index = 0;
  uint add_col_index = 0;
  Field *new_field = nullptr;

  for (uint i = 0; i < n_key_parts; i++) {
    // Fill hidden pk id into the last key part for secondary keys for tables
    // with no pk
    if (hidden_pk_exists && hidden_pk_id && i + 1 == n_key_parts) {
      m_pack_info[i].fill_hidden_pk_val(&tuple, hidden_pk_id);
      break;
    }

    new_field = m_pack_info[i].get_field_in_table(altered_table);
    assert(new_field != nullptr);

    uint col_index = new_field->field_index();
    old_col_index = col_map[col_index];

    // field that maybe come from old table or new table
    Field *field = nullptr;
    if (old_col_index == uint(-1)) {
      /* column is from new added columns, we can use default value */
      ret = pack_field(new_field, &m_pack_info[i], tuple, packed_tuple,
                       pack_buffer, unpack_info, n_null_fields);
      if (ret && ret != HA_ERR_INVALID_NULL_ERROR) {
        __HANDLER_LOG(ERROR, "pack field failed, error code: %d", ret);
        return ret;
      }
    } else {
      field = old_tbl->field[old_col_index];
      uint field_offset = field->field_ptr() - old_tbl->record[0];
      uint null_offset = field->null_offset(old_tbl->record[0]);
      bool maybe_null = field->is_nullable();
      field->move_field(
          const_cast<uchar *>(record) + field_offset,
          maybe_null ? const_cast<uchar *>(record) + null_offset : nullptr,
          field->null_bit);
      // WARNING! Don't return without restoring field->ptr and field->null_ptr

      ret = pack_field(field, &m_pack_info[i], tuple, packed_tuple, pack_buffer,
                       unpack_info, n_null_fields);
      if (ret && ret != HA_ERR_INVALID_NULL_ERROR) {
        __HANDLER_LOG(ERROR, "pack field failed, error code: %d", ret);
        return ret;
      }

      // Restore field->ptr and field->null_ptr
      field->move_field(old_tbl->record[0] + field_offset,
                        maybe_null ? old_tbl->record[0] + null_offset : nullptr,
                        field->null_bit);
    }
  }

  if (unpack_info) {
    const size_t len = unpack_info->get_current_pos();
    assert(len <= std::numeric_limits<uint16_t>::max());

    // Don't store the unpack_info if it has only the header (that is, there's
    // no meaningful content).
    // Primary Keys are special: for them, store the unpack_info even if it's
    // empty (provided m_maybe_unpack_info==true, see
    // ha_smartengine::convert_record_to_storage_format)
    if (len == schema::RecordFormat::RECORD_UNPACK_HEADER_SIZE &&
        m_index_type != SeKeyDef::INDEX_TYPE_PRIMARY) {
      unpack_info->clear();
    } else {
      unpack_info->write_uint16_at(unpack_len_pos, len);
    }

    //
    // Secondary keys have key and value checksums in the value part
    // Primary key is a special case (the value part has non-indexed columns),
    // so the checksums are computed and stored by
    // ha_smartengine::convert_record_to_storage_format
    //
    if (should_store_row_debug_checksums) {
      const uint32_t key_crc32 = crc32(0, packed_tuple, tuple - packed_tuple);
      const uint32_t val_crc32 =
          crc32(0, unpack_info->ptr(), unpack_info->get_current_pos());

      unpack_info->write_uint8(SE_CHECKSUM_DATA_TAG);
      unpack_info->write_uint32(key_crc32);
      unpack_info->write_uint32(val_crc32);
    }
  }

  assert(is_storage_available(tuple - packed_tuple, 0));

  size = tuple - packed_tuple;

  return HA_EXIT_SUCCESS;
}


/**
  Pack the hidden primary key into mem-comparable form.

  @param
    tbl                   Table we're working on
    hidden_pk_id     IN   New value to be packed into key
    packed_tuple     OUT  Key in the mem-comparable form

  @return
    Length of the packed tuple
*/

uint SeKeyDef::pack_hidden_pk(const longlong &hidden_pk_id, uchar *const packed_tuple) const
{
  assert(packed_tuple != nullptr);

  uchar *tuple = packed_tuple;
  se_netbuf_store_index(tuple, m_index_number);
  tuple += INDEX_NUMBER_SIZE;
  assert(m_key_parts == 1);
  assert(is_storage_available(tuple - packed_tuple,
                                   m_pack_info[0].m_max_image_len));

  m_pack_info[0].fill_hidden_pk_val(&tuple, hidden_pk_id);

  assert(is_storage_available(tuple - packed_tuple, 0));
  return tuple - packed_tuple;
}

bool SeKeyDef::write_dd_index(dd::Index *dd_index, uint64_t table_id) const
{
  if (nullptr != dd_index) {
    // If there is no user-defined primary key, while there is one user-defined
    // unique key on non-nullable column(s), the key will be treated as PRIMARY
    // key, but type of coresponding dd::Index object isn't dd::Index::IT_PRIMARY.
    // To recover key type correctly, we need persist index_type as metadata.
    int index_type = m_index_type;
    dd::Properties &p = dd_index->se_private_data();
    return p.set(dd_index_key_strings[DD_INDEX_TABLE_ID], table_id) ||
           p.set(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], m_index_number) ||
           p.set(dd_index_key_strings[DD_INDEX_TYPE], index_type) ||
           write_dd_index_ext(p);
  }
  return true;
}

bool SeKeyDef::write_dd_index_ext(dd::Properties& prop) const
{
  int index_version = m_index_dict_version, kv_version = m_kv_format_version;
  int flags = 0;
  if (m_is_reverse_cf) flags |= REVERSE_CF_FLAG;
  if (m_is_auto_cf) flags |= AUTO_CF_FLAG;

  return prop.set(dd_index_key_strings[DD_INDEX_VERSION_ID], index_version) ||
         prop.set(dd_index_key_strings[DD_INDEX_KV_VERSION], kv_version) ||
         prop.set(dd_index_key_strings[DD_INDEX_FLAGS], flags);
}


bool SeKeyDef::verify_dd_index(const dd::Index *dd_index, uint64_t table_id)
{
  if (nullptr == dd_index)  return true;

  // check primary key name
  if ((dd_index->type() == dd::Index::IT_PRIMARY) !=
      !my_strcasecmp(system_charset_info,
                 dd_index->name().c_str(), primary_key_name))
    return true;

  const dd::Properties& p = dd_index->se_private_data();
  uint64_t table_id_in_dd = dd::INVALID_OBJECT_ID;
  uint32_t subtable_id=0;
  int index_type = 0;

  return !p.exists(dd_index_key_strings[DD_INDEX_TABLE_ID]) ||
         !p.exists(dd_index_key_strings[DD_INDEX_SUBTABLE_ID]) ||
         !p.exists(dd_index_key_strings[DD_INDEX_TYPE]) ||
         p.get(dd_index_key_strings[DD_INDEX_TABLE_ID], &table_id_in_dd) ||
         p.get(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], &subtable_id) ||
         p.get(dd_index_key_strings[DD_INDEX_TYPE], &index_type) ||
         (table_id_in_dd == dd::INVALID_OBJECT_ID) ||
         (table_id != table_id_in_dd) ||
         verify_dd_index_ext(p) ||
         (nullptr == cf_manager.get_cf(subtable_id));
}

bool SeKeyDef::verify_dd_index_ext(const dd::Properties& prop)
{
  int index_version_id = 0, kv_version = 0, flags = 0;
  return !prop.exists(dd_index_key_strings[DD_INDEX_VERSION_ID]) ||
         !prop.exists(dd_index_key_strings[DD_INDEX_KV_VERSION]) ||
         !prop.exists(dd_index_key_strings[DD_INDEX_FLAGS]) ||
         prop.get(dd_index_key_strings[DD_INDEX_VERSION_ID], &index_version_id) ||
         prop.get(dd_index_key_strings[DD_INDEX_KV_VERSION], &kv_version) ||
         prop.get(dd_index_key_strings[DD_INDEX_FLAGS], &flags);
}

/*
  Compares two keys without unpacking

  @detail
  @return
    0 - Ok. column_index is the index of the first column which is different.
          -1 if two kes are equal
    1 - Data format error.
*/
int SeKeyDef::compare_keys(const common::Slice *key1,
                           const common::Slice *key2,
                           std::size_t *const column_index) const
{
  assert(key1 != nullptr);
  assert(key2 != nullptr);
  assert(column_index != nullptr);

  // the caller should check the return value and
  // not rely on column_index being valid
  *column_index = 0xbadf00d;

  SeStringReader reader1(key1);
  SeStringReader reader2(key2);

  // Skip the index number
  if ((!reader1.read(INDEX_NUMBER_SIZE)))
    return HA_EXIT_FAILURE;

  if ((!reader2.read(INDEX_NUMBER_SIZE)))
    return HA_EXIT_FAILURE;

  for (uint i = 0; i < m_key_parts; i++) {
    const SeFieldPacking *const fpi = &m_pack_info[i];
    if (fpi->m_maybe_null) {
      const auto nullp1 = reader1.read(1);
      const auto nullp2 = reader2.read(1);

      if (nullp1 == nullptr || nullp2 == nullptr) {
        return HA_EXIT_FAILURE;
      }

      if (*nullp1 != *nullp2) {
        *column_index = i;
        return HA_EXIT_SUCCESS;
      }

      if (*nullp1 == 0) {
        /* This is a NULL value */
        continue;
      }
    }

    const auto before_skip1 = reader1.get_current_ptr();
    const auto before_skip2 = reader2.get_current_ptr();
    assert(fpi->m_skip_func);
    if (fpi->m_skip_func(fpi, nullptr, &reader1))
      return HA_EXIT_FAILURE;
    if (fpi->m_skip_func(fpi, nullptr, &reader2))
      return HA_EXIT_FAILURE;
    const auto size1 = reader1.get_current_ptr() - before_skip1;
    const auto size2 = reader2.get_current_ptr() - before_skip2;
    if (size1 != size2) {
      *column_index = i;
      return HA_EXIT_SUCCESS;
    }

    if (memcmp(before_skip1, before_skip2, size1) != 0) {
      *column_index = i;
      return HA_EXIT_SUCCESS;
    }
  }

  *column_index = m_key_parts;
  return HA_EXIT_SUCCESS;
}

/*
  @brief
    Given a zero-padded key, determine its real key length

  @detail
    Fixed-size skip functions just read.
*/

size_t SeKeyDef::key_length(const TABLE *const table, const common::Slice &key) const
{
  assert(table != nullptr);

  SeStringReader reader(&key);

  if ((!reader.read(INDEX_NUMBER_SIZE)))
    return size_t(-1);

  for (uint i = 0; i < m_key_parts; i++) {
    const SeFieldPacking *fpi = &m_pack_info[i];
    const Field *field = nullptr;
    if (m_index_type != INDEX_TYPE_HIDDEN_PRIMARY)
      field = fpi->get_field_in_table(table);
    if (fpi->m_skip_func(fpi, field, &reader))
      return size_t(-1);
  }
  return key.size() - reader.remaining_bytes();
}

int SeKeyDef::unpack_field(SeFieldPacking *const fpi,
                           Field *const field,
                           SeStringReader* reader,
                           const uchar *const default_value,
                           SeStringReader *unp_reader) const
{
  if (fpi->m_maybe_null) {
    const char *nullp;
    if (!(nullp = reader->read(1))) {
      return HA_EXIT_FAILURE;
    }

    if (*nullp == 0) {
      /* Set the NULL-bit of this field */
      field->set_null();
      /* Also set the field to its default value */
      memcpy(field->field_ptr(), default_value, field->pack_length());
      return HA_EXIT_SUCCESS;
    } else if (*nullp == 1) {
      field->set_notnull();
    } else {
      return HA_EXIT_FAILURE;
    }
  }

  return fpi->m_unpack_func(fpi, field, field->field_ptr(), reader, unp_reader);
}

bool SeKeyDef::table_has_unpack_info(TABLE *const table) const
{
    bool has_unpack = false;

    for (uint i = 0; i < m_key_parts; i++) {
        if (m_pack_info[i].uses_unpack_info()) {
            has_unpack = true;
            break;
        }
    }

    return has_unpack;
}


/*
  Take mem-comparable form and unpack it to Table->record
  This is a fast unpacking for record with unpack_info is null

  @detail
    not all indexes support this

  @return
    UNPACK_SUCCESS - Ok
    UNPACK_FAILURE - Data format error.
*/

int SeKeyDef::unpack_record_1(TABLE *const table,
                              uchar *const buf,
                              const common::Slice *const packed_key,
                              const common::Slice *const unpack_info,
                              const bool &verify_row_debug_checksums) const
{
  SeStringReader reader(packed_key);

  const bool is_hidden_pk = (m_index_type == INDEX_TYPE_HIDDEN_PRIMARY);
  const bool hidden_pk_exists = table_has_hidden_pk(table);
  const bool secondary_key = (m_index_type == INDEX_TYPE_SECONDARY);
  // There is no checksuming data after unpack_info for primary keys, because
  // the layout there is different. The checksum is verified in
  // ha_smartengine::convert_record_from_storage_format instead.
  DBUG_ASSERT_IMP(!secondary_key, !verify_row_debug_checksums);

  assert(!unpack_info);

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE))) {
    return HA_EXIT_FAILURE;
  }

  for (uint i = 0; i < m_key_parts; i++) {
    SeFieldPacking *const fpi = &m_pack_info[i];

    /*
      Hidden pk field is packed at the end of the secondary keys, but the SQL
      layer does not know about it. Skip retrieving field if hidden pk.
    */
    if ((secondary_key && hidden_pk_exists && i + 1 == m_key_parts) ||
        is_hidden_pk) {
      assert(fpi->m_unpack_func);
      if (fpi->m_skip_func(fpi, nullptr, &reader)) {
        return HA_EXIT_FAILURE;
      }
      continue;
    }

    Field *const field = fpi->get_field_in_table(table);

    if (fpi->m_unpack_func) {
      /* It is possible to unpack this column. Do it. */

      uint field_offset = field->field_ptr() - table->record[0];
      uint null_offset = field->null_offset();
      bool maybe_null = field->is_nullable();
      field->move_field(buf + field_offset,
                        maybe_null ? buf + null_offset : nullptr,
                        field->null_bit);
      // WARNING! Don't return without restoring field->ptr and field->null_ptr
      int res = unpack_field(fpi, field, &reader,
                             table->s->default_values + field_offset,
                             nullptr);

      // Restore field->ptr and field->null_ptr
      field->move_field(table->record[0] + field_offset,
                        maybe_null ? table->record[0] + null_offset : nullptr,
                        field->null_bit);

      if (res) {
        return res;
      }
    } else {
      /* It is impossible to unpack the column. Skip it. */
      if (fpi->m_maybe_null) {
        const char *nullp;
        if (!(nullp = reader.read(1)))
          return HA_EXIT_FAILURE;
        if (*nullp == 0) {
          /* This is a NULL value */
          continue;
        }
        /* If NULL marker is not '0', it can be only '1'  */
        if (*nullp != 1)
          return HA_EXIT_FAILURE;
      }
      if (fpi->m_skip_func(fpi, field, &reader))
        return HA_EXIT_FAILURE;
    }
  }

  if (reader.remaining_bytes())
    return HA_EXIT_FAILURE;

  return HA_EXIT_SUCCESS;
}

/** used to unpack new-pk record during onlineDDL, new_pk_record format
    is set in convert_new_record_from_old_record.
    see also unpack_record, which used to unpack secondary index
@param[in] table, new table object
@param[in/out] buf, new table record
@param[in] packed_key, se-format key
@param[in] packed_value, se-format value
@param[in] dict_info, new se_table_def
@return  success/failure
*/

int SeKeyDef::unpack_record_pk(TABLE *const table,
                               uchar *const buf,
                               const common::Slice *const packed_key,
                               const common::Slice *const packed_value,
                               const std::shared_ptr<SeInplaceDdlDictInfo> &dict_info) const
{
  SeStringReader reader(packed_key);
  SeStringReader value_reader =
      SeStringReader::read_or_empty(packed_value);

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE))) {
    __HANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
    return HA_ERR_INTERNAL_ERROR;
  }

  // Skip instant columns attribute
  value_reader.read(schema::RecordFormat::RECORD_HEADER_SIZE);

  // Skip null-bytes
  uint32_t null_bytes_in_rec = dict_info->m_null_bytes_in_rec;
  const char *null_bytes = nullptr;
  if (null_bytes_in_rec &&
      !(null_bytes = value_reader.read(null_bytes_in_rec))) {
    __HANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
    return HA_ERR_INTERNAL_ERROR;
  }

  const char *unpack_info = nullptr;
  uint16 unpack_info_len = 0;
  common::Slice unpack_slice;
  if (dict_info->m_maybe_unpack_info) {
    unpack_info = value_reader.read(schema::RecordFormat::RECORD_UNPACK_HEADER_SIZE);
    if (!unpack_info || unpack_info[0] != schema::RecordFormat::RECORD_UNPACK_DATA_FLAG) {
      __HANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
      return HA_ERR_INTERNAL_ERROR;
    }

    unpack_info_len =
        se_netbuf_to_uint16(reinterpret_cast<const uchar *>(unpack_info + 1));
    unpack_slice = common::Slice(unpack_info, unpack_info_len);

    value_reader.read(unpack_info_len - schema::RecordFormat::RECORD_UNPACK_HEADER_SIZE);
  }

  if (!unpack_info && !table_has_unpack_info(table)) {
    if (this->unpack_record_1(table, buf, packed_key, nullptr,
                              false /* verify_checksum */)) {
      __HANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
      return HA_ERR_INTERNAL_ERROR;
    }
  } else if (this->unpack_record(table, buf, packed_key,
                                 unpack_info ? &unpack_slice : nullptr,
                                 false /* verify_checksum */)) {
    __HANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
    return HA_ERR_INTERNAL_ERROR;
  }

  return HA_EXIT_SUCCESS;
}


/*
  Take mem-comparable form and unpack_info and unpack it to Table->record

  @detail
    not all indexes support this

  @return
    UNPACK_SUCCESS - Ok
    UNPACK_FAILURE - Data format error.
*/

int SeKeyDef::unpack_record(TABLE *const table,
                            uchar *const buf,
                            const common::Slice *const packed_key,
                            const common::Slice *const unpack_info,
                            const bool &verify_row_debug_checksums) const
{
  SeStringReader reader(packed_key);
  SeStringReader unp_reader = SeStringReader::read_or_empty(unpack_info);

  const bool is_hidden_pk = (m_index_type == INDEX_TYPE_HIDDEN_PRIMARY);
  const bool hidden_pk_exists = table_has_hidden_pk(table);
  const bool secondary_key = (m_index_type == INDEX_TYPE_SECONDARY);
  // There is no checksuming data after unpack_info for primary keys, because
  // the layout there is different. The checksum is verified in
  // ha_smartengine::convert_record_from_storage_format instead.
  DBUG_ASSERT_IMP(!secondary_key, !verify_row_debug_checksums);

  // Skip the index number
  if ((!reader.read(INDEX_NUMBER_SIZE))) {
    __HANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  // For secondary keys, we expect the value field to contain unpack data and
  // checksum data in that order. One or both can be missing, but they cannot
  // be reordered.
  const bool has_unpack_info =
      unp_reader.remaining_bytes() &&
      *unp_reader.get_current_ptr() == schema::RecordFormat::RECORD_UNPACK_DATA_FLAG;
  if (has_unpack_info && !unp_reader.read(schema::RecordFormat::RECORD_UNPACK_HEADER_SIZE)) {
    __HANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  for (uint i = 0; i < m_key_parts; i++) {
    SeFieldPacking *const fpi = &m_pack_info[i];

    /*
      Hidden pk field is packed at the end of the secondary keys, but the SQL
      layer does not know about it. Skip retrieving field if hidden pk.
    */
    if ((secondary_key && hidden_pk_exists && i + 1 == m_key_parts) ||
        is_hidden_pk) {
      assert(fpi->m_unpack_func);
      if (fpi->m_skip_func(fpi, nullptr, &reader)) {
        __HANDLER_LOG(ERROR, "unexpected error record,table_name:%s", table->s->table_name.str);
        return HA_EXIT_FAILURE;
      }
      continue;
    }

    Field *const field = fpi->get_field_in_table(table);

    if (fpi->m_unpack_func) {
      /* It is possible to unpack this column. Do it. */

      uint field_offset = field->field_ptr() - table->record[0];
      uint null_offset = field->null_offset();
      bool maybe_null = field->is_nullable();
      field->move_field(buf + field_offset,
                        maybe_null ? buf + null_offset : nullptr,
                        field->null_bit);
      // WARNING! Don't return without restoring field->ptr and field->null_ptr

      // If we need unpack info, but there is none, tell the unpack function
      // this by passing unp_reader as nullptr. If we never read unpack_info
      // during unpacking anyway, then there won't an error.
      const bool maybe_missing_unpack =
          !has_unpack_info && fpi->uses_unpack_info();
      int res = unpack_field(fpi, field, &reader,
                             table->s->default_values + field_offset,
                             maybe_missing_unpack ? nullptr : &unp_reader);

      // Restore field->ptr and field->null_ptr
      field->move_field(table->record[0] + field_offset,
                        maybe_null ? table->record[0] + null_offset : nullptr,
                        field->null_bit);

      if (res) {
        __HANDLER_LOG(ERROR, "unexpected error record, code:%d, table_name:%s", res, table->s->table_name.str);
        return res;
      }
    } else {
      /* It is impossible to unpack the column. Skip it. */
      if (fpi->m_maybe_null) {
        const char *nullp;
        if (!(nullp = reader.read(1))) {
          __HANDLER_LOG(ERROR, "unexpected error record, table_name:%s", table->s->table_name.str);
          return HA_EXIT_FAILURE;
        }
        if (*nullp == 0) {
          /* This is a NULL value */
          continue;
        }
        /* If NULL marker is not '0', it can be only '1'  */
        if (*nullp != 1) {
          __HANDLER_LOG(ERROR, "unexpected error record, table_name:%s", table->s->table_name.str);
          return HA_EXIT_FAILURE;
        }
      }
      if (fpi->m_skip_func(fpi, field, &reader)) {
        __HANDLER_LOG(ERROR, "unexpected error record, table_name:%s", table->s->table_name.str);
        return HA_EXIT_FAILURE;
      }
    }
  }

  /*
    Check checksum values if present
  */
  const char *ptr;
  if ((ptr = unp_reader.read(1)) && *ptr == SE_CHECKSUM_DATA_TAG) {
    if (verify_row_debug_checksums) {
      uint32_t stored_key_chksum = se_netbuf_to_uint32(
          (const uchar *)unp_reader.read(SE_CHECKSUM_SIZE));
      const uint32_t stored_val_chksum = se_netbuf_to_uint32(
          (const uchar *)unp_reader.read(SE_CHECKSUM_SIZE));

      const uint32_t computed_key_chksum =
          crc32(0, (const uchar *)packed_key->data(), packed_key->size());
      const uint32_t computed_val_chksum =
          crc32(0, (const uchar *)unpack_info->data(),
                unpack_info->size() - SE_CHECKSUM_CHUNK_SIZE);

      DBUG_EXECUTE_IF("myx_simulate_bad_key_checksum1",
                      stored_key_chksum++;);

      if (stored_key_chksum != computed_key_chksum) {
        report_checksum_mismatch(true, packed_key->data(), packed_key->size());
        return HA_EXIT_FAILURE;
      }

      if (stored_val_chksum != computed_val_chksum) {
        report_checksum_mismatch(false, unpack_info->data(),
                                 unpack_info->size() - SE_CHECKSUM_CHUNK_SIZE);
        return HA_EXIT_FAILURE;
      }
    } else {
      /* The checksums are present but we are not checking checksums */
    }
  }

  if (reader.remaining_bytes()) {
    __HANDLER_LOG(ERROR, "unexpected error record, table_name:%s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  return HA_EXIT_SUCCESS;
}

bool SeKeyDef::table_has_hidden_pk(const TABLE *const table)
{
  return table->s->primary_key == MAX_INDEXES;
}

void SeKeyDef::report_checksum_mismatch(const bool &is_key,
                                        const char *const data,
                                        const size_t data_size) const
{
  // NO_LINT_DEBUG
  sql_print_error("Checksum mismatch in %s of key-value pair for index 0x%x",
                  is_key ? "key" : "value", get_index_number());

  const std::string buf = se_hexdump(data, data_size, SE_MAX_HEXDUMP_LEN);
  // NO_LINT_DEBUG
  // sql_print_error("Data with incorrect checksum (%" PRIu64 " bytes): %s",
  sql_print_error("Data with incorrect checksum %lu bytes): %s",
                  (uint64_t)data_size, buf.c_str());

  my_error(ER_INTERNAL_ERROR, MYF(0), "Record checksum mismatch");
}

Field *SeKeyDef::get_table_field_for_part_no(TABLE *table, uint part_no) const
{
  assert(part_no < get_key_parts());
  return m_pack_info[part_no].get_field_in_table(table);
}

bool SeKeyDef::can_unpack(const uint &kp) const
{
  assert(kp < m_key_parts);
  return (m_pack_info[kp].m_unpack_func != nullptr);
}

bool SeKeyDef::has_unpack_info(const uint &kp) const
{
  assert(kp < m_key_parts);
  return m_pack_info[kp].uses_unpack_info();
}

} //namespace smartengine
