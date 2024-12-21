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
#ifndef SE_HANDLER_XE_INDEX_H_
#define SE_HANDLER_XE_INDEX_H_

#include "db.h"
#include "my_base.h"
#include "se_index_stats.h"
#include "dd/types/index.h"
#include "util/se_buff.h"

class Field;

namespace smartengine
{
namespace db
{
  class ColumnFamilyHandle;
}

class SeTableDef;
class SeFieldPacking;
struct SeInplaceDdlDictInfo;

const uint SE_INVALID_KEY_LEN = uint(-1);

/* How much one checksum occupies when stored in the record */
const size_t SE_CHECKSUM_SIZE = sizeof(uint32_t);

/*
  How much the checksum data occupies in record, in total.
  It is storing two checksums plus 1 tag-byte.
*/
const size_t SE_CHECKSUM_CHUNK_SIZE = 2 * SE_CHECKSUM_SIZE + 1;

/*
  Checksum data starts from CHECKSUM_DATA_TAG which is followed by two CRC32
  checksums.
*/
const char SE_CHECKSUM_DATA_TAG = 0x01;

/*
  An object of this class represents information about an index in an SQL
  table. It provides services to encode and decode index tuples.

  Note: a table (as in, on-disk table) has a single SeKeyDef object which
  is shared across multiple TABLE* objects and may be used simultaneously from
  different threads.

  There are several data encodings:

  === SQL LAYER ===
  SQL layer uses two encodings:

  - "Table->record format". This is the format that is used for the data in
     the record buffers, table->record[i]

  - KeyTupleFormat (see opt_range.cc) - this is used in parameters to index
    lookup functions, like handler::index_read_map().

  === Inside se ===
  Primary Key is stored as a mapping:

    index_tuple -> StoredRecord

  StoredRecord is in Table->record format, except for blobs, which are stored
  in-place. See ha_smartengine::convert_record_to_storage_format for details.

  Secondary indexes are stored as one of two variants:

    index_tuple -> unpack_info
    index_tuple -> empty_string

  index_tuple here is the form of key that can be compared with memcmp(), aka
  "mem-comparable form".

  unpack_info is extra data that allows to restore the original value from its
  mem-comparable form. It is present only if the index supports index-only
  reads.
*/

class SeKeyDef {
public:
  /* Convert a key from KeyTupleFormat to mem-comparable form */
  uint pack_index_tuple(TABLE *tbl,
                        uchar *pack_buffer,
                        uchar *packed_tuple,
                        const uchar *key_tuple,
                        key_part_map &keypart_map) const;

  int pack_field(Field *const field,
                 SeFieldPacking *pack_info,
                 uchar *&tuple,
                 uchar *const packed_tuple,
                 uchar *const pack_buffer,
                 SeStringWriter *const unpack_info,
                 uint *const n_null_fields) const;

  /* Convert a key from Table->record format to mem-comparable form */
  uint pack_record(const TABLE *const tbl,
                   uchar *const pack_buffer,
                   const uchar *const record,
                   uchar *const packed_tuple,
                   SeStringWriter *const unpack_info,
                   const bool &should_store_row_debug_checksums,
                   const longlong &hidden_pk_id = 0,
                   uint n_key_parts = 0,
                   uint *const n_null_fields = nullptr,
                   const TABLE *const altered_table = nullptr) const;

  int pack_new_record(const TABLE *const old_tbl,
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
                      uint &size) const;

  /* Pack the hidden primary key into mem-comparable form. */
  uint pack_hidden_pk(const longlong &hidden_pk_id, uchar *const packed_tuple) const;
  int unpack_field(SeFieldPacking *const fpi,
                   Field *const field,
                   SeStringReader *reader,
                   const uchar *const default_value,
                   SeStringReader *unp_reader) const;

  int unpack_record(TABLE *const table,
                    uchar *const buf,
                    const common::Slice *const packed_key,
                    const common::Slice *const unpack_info,
                    const bool &verify_row_debug_checksums) const;

  int unpack_record_pk(TABLE *const table,
                       uchar *const buf,
                       const common::Slice *const packed_key,
                       const common::Slice *const packed_value,
                       const std::shared_ptr<SeInplaceDdlDictInfo> &dict_info) const;

  bool table_has_unpack_info(TABLE *const table) const;

  int unpack_record_1(TABLE *const table,
                      uchar *const buf,
                      const common::Slice *const packed_key,
                      const common::Slice *const unpack_info,
                      const bool &verify_row_debug_checksums) const;


  static bool unpack_info_has_checksum(const common::Slice &unpack_info);

  int compare_keys(const common::Slice *key1,
                   const common::Slice *key2,
                   std::size_t *const column_index) const;

  size_t key_length(const TABLE *const table, const common::Slice &key) const;

  /* Get the key that is the "infimum" for this index */
  inline void get_infimum_key(uchar *const key, uint *const size) const
  {
    se_netbuf_store_index(key, m_index_number);
    *size = INDEX_NUMBER_SIZE;
  }

  /* Get the key that is a "supremum" for this index */
  inline void get_supremum_key(uchar *const key, uint *const size) const
  {
    se_netbuf_store_index(key, m_index_number + 1);
    *size = INDEX_NUMBER_SIZE;
  }

  /* Make a key that is right after the given key. */
  static int successor(uchar *const packed_tuple, const uint &len);

  /*
    This can be used to compare prefixes.
    if  X is a prefix of Y, then we consider that X = Y.
  */
  // b describes the lookup key, which can be a prefix of a.
  int cmp_full_keys(const common::Slice &a, const common::Slice &b) const
  {
    assert(covers_key(a));

    return memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
  }

  /* Check if given mem-comparable key belongs to this index */
  bool covers_key(const common::Slice &slice) const
  {
    if (slice.size() < INDEX_NUMBER_SIZE)
      return false;

    if (memcmp(slice.data(), m_index_number_storage_form, INDEX_NUMBER_SIZE))
      return false;

    return true;
  }

  /*
    Return true if the passed mem-comparable key
    - is from this index, and
    - it matches the passed key prefix (the prefix is also in mem-comparable
      form)
  */
  bool value_matches_prefix(const common::Slice &value,
                            const common::Slice &prefix) const
  {
    return covers_key(value) && !cmp_full_keys(value, prefix);
  }

  uint32 get_keyno() const { return m_keyno; }

  uint32 get_index_number() const { return m_index_number; }

  GL_INDEX_ID get_gl_index_id() const
  {
    const GL_INDEX_ID gl_index_id = {m_cf_handle->GetID(), m_index_number};
    se_assert(m_cf_handle->GetID() == m_index_number);
    return gl_index_id;
  }

  int read_memcmp_key_part(const TABLE *table_arg, SeStringReader *reader,
                           const uint part_num) const;

  /* Must only be called for secondary keys: */
  uint get_primary_key_tuple(const TABLE *const tbl,
                             const SeKeyDef &pk_descr,
                             const common::Slice *const key,
                             uchar *const pk_buffer) const;

  uint get_memcmp_sk_parts(const TABLE *table,
                           const common::Slice &key,
                           uchar *sk_buffer,
                           uint *n_null_fields) const;

  uint get_memcmp_sk_size(const TABLE *table,
                          const common::Slice &key,
                          uint *n_null_fields) const;

  /* Return max length of mem-comparable form */
  uint max_storage_fmt_length() const { return m_maxlength; }

  uint get_key_parts() const { return m_key_parts; }

  bool get_support_icp_flag() const { return m_is_support_icp; }
  /*
    Get a field object for key part #part_no

    @detail
      SQL layer thinks unique secondary indexes and indexes in partitioned
      tables are not "Extended" with Primary Key columns.

      Internally, we always extend all indexes with PK columns. This function
      uses our definition of how the index is Extended.
  */
  Field *get_table_field_for_part_no(TABLE *table, uint part_no) const;

  const std::string &get_name() const { return m_name; }

  SeKeyDef &operator=(const SeKeyDef &) = delete;
  SeKeyDef(const SeKeyDef &k);
  SeKeyDef(uint indexnr_arg,
           uint keyno_arg,
           db::ColumnFamilyHandle *cf_handle_arg,
           uint16_t index_dict_version_arg,
           uchar index_type_arg,
           uint16_t kv_format_version_arg,
           bool is_reverse_cf_arg,
           bool is_auto_cf_arg,
           const std::string &name,
           const SeIndexStats &stats);
  SeKeyDef(uint indexnr_arg,
           uint keyno_arg,
           uint16_t index_dict_version_arg,
           uchar index_type_arg,
           uint16_t kv_format_version_arg,
           const std::string &name,
           const SeIndexStats &stats);
  ~SeKeyDef();

  enum {
    INDEX_NUMBER_SIZE = 4,
    VERSION_SIZE = 2,
    CF_NUMBER_SIZE = 4,
    CF_FLAG_SIZE = 4,
    PACKED_SIZE = 4, // one int
    TABLE_ID_SIZE = 8,
  };

  // bit flags for combining bools when writing to disk
  enum {
    REVERSE_CF_FLAG = 1,
    AUTO_CF_FLAG = 2,
  };

  // Data dictionary types
  enum DATA_DICT_TYPE {
    DDL_ENTRY_INDEX_START_NUMBER = 1,
    INDEX_INFO = 2,
    CF_DEFINITION = 3,
    BINLOG_INFO_INDEX_NUMBER = 4,
    DDL_DROP_INDEX_ONGOING = 5,
    INDEX_STATISTICS = 6,
    MAX_INDEX_ID = 7,
    DDL_CREATE_INDEX_ONGOING = 8,
    DDL_OPERATION_LOG = 9,  //DDL-LOG
    SYSTEM_CF_VERSION_INDEX = 10,
    MAX_TABLE_ID = 11,
    END_DICT_INDEX_ID = 255
  };

  enum SYSTEM_CF_VERSION {
    VERSION_0 = 0, // old system version which doesn't have system cf version
    VERSION_1 = 1, // DDL_ENTRY_INDEX_START_NUMBER records moved to global dd
    VERSION_2 = 2, // DDL_DROP_INDEX_ONGOING/DDL_CREATE_INDEX_ONGOING removed
  };

  // Data dictionary schema version. Introduce newer versions
  // if changing schema layout
  enum {
    DDL_ENTRY_INDEX_VERSION = 1,
    CF_DEFINITION_VERSION = 1,
    BINLOG_INFO_INDEX_NUMBER_VERSION = 1,
    DDL_DROP_INDEX_ONGOING_VERSION = 1,
    MAX_INDEX_ID_VERSION = 1,
    DDL_CREATE_INDEX_ONGOING_VERSION = 1,
    DDL_OPERATION_LOG_VERSION = 1,
    MAX_TABLE_ID_VERSION = 1,
    // Version for index stats is stored in IndexStats struct
  };

  // Index info version.  Introduce newer versions when changing the
  // INDEX_INFO layout. Update INDEX_INFO_VERSION_LATEST to point to the
  // latest version number.
  enum {
    INDEX_INFO_VERSION_INITIAL = 1, // Obsolete
    INDEX_INFO_VERSION_KV_FORMAT,
    INDEX_INFO_VERSION_GLOBAL_ID,
    // There is no change to data format in this version, but this version
    // verifies KV format version, whereas previous versions do not. A version
    // bump is needed to prevent older binaries from skipping the KV version
    // check inadvertently.
    INDEX_INFO_VERSION_VERIFY_KV_FORMAT,
    // This normally point to the latest (currently it does).
    INDEX_INFO_VERSION_LATEST = INDEX_INFO_VERSION_VERIFY_KV_FORMAT,
  };

  // SE index types
  enum INDEX_TYPE {
    INDEX_TYPE_PRIMARY = 1,
    INDEX_TYPE_SECONDARY = 2,
    INDEX_TYPE_HIDDEN_PRIMARY = 3,
  };

  // Key/Value format version for each index type
  enum {
    PRIMARY_FORMAT_VERSION_INITIAL = 10,
    // This change includes:
    //  - For columns that can be unpacked with unpack_info, PK
    //    stores the unpack_info.
    //  - DECIMAL datatype is no longer stored in the row (because
    //    it can be decoded from its mem-comparable form)
    //  - VARCHAR-columns use endspace-padding.
    PRIMARY_FORMAT_VERSION_UPDATE1 = 11,
    PRIMARY_FORMAT_VERSION_LATEST = PRIMARY_FORMAT_VERSION_UPDATE1,

    SECONDARY_FORMAT_VERSION_INITIAL = 10,
    // This change the SK format to include unpack_info.
    SECONDARY_FORMAT_VERSION_UPDATE1 = 11,
    SECONDARY_FORMAT_VERSION_LATEST = SECONDARY_FORMAT_VERSION_UPDATE1,
  };

  void setup(const TABLE *const table, const SeTableDef *const tbl_def);

  bool is_primary_key() const
  {
    return m_index_type == INDEX_TYPE_PRIMARY || m_index_type == INDEX_TYPE_HIDDEN_PRIMARY;
  }

  bool is_secondary_key() const
  {
    return m_index_type == INDEX_TYPE_SECONDARY;
  }

  bool is_hidden_primary_key() const
  {
    return m_index_type == INDEX_TYPE_HIDDEN_PRIMARY;
  }

  db::ColumnFamilyHandle *get_cf() const { return m_cf_handle; }
  void set_cf(db::ColumnFamilyHandle *cf) { m_cf_handle = cf; }

  /* Check if keypart #kp can be unpacked from index tuple */
  bool can_unpack(const uint &kp) const;
  /* Check if keypart #kp needs unpack info */
  bool has_unpack_info(const uint &kp) const;

  /* Check if given table has a primary key */
  static bool table_has_hidden_pk(const TABLE *const table);

  void report_checksum_mismatch(const bool &is_key,
                                const char *const data,
                                const size_t data_size) const;

  /* Check if index is at least pk_min if it is a PK,
    or at least sk_min if SK.*/
  bool index_format_min_check(const int &pk_min, const int &sk_min) const;

  // set metadata for user-defined index
  bool write_dd_index(dd::Index *dd_index, uint64_t table_id) const;
  // set extra metadata for user-defined index and hidden primary key
  bool write_dd_index_ext(dd::Properties& prop) const;

  static bool verify_dd_index(const dd::Index *dd_index, uint64_t table_id);
  static bool verify_dd_index_ext(const dd::Properties& prop);
private:
#ifndef DBUG_OFF
  inline bool is_storage_available(const int &offset, const int &needed) const
  {
    const int storage_length = static_cast<int>(max_storage_fmt_length());
    return (storage_length - offset) >= needed;
  }
#endif // DBUG_OFF

  /* Global number of this index (used as prefix in StorageFormat) */
  const uint32 m_index_number;

  uchar m_index_number_storage_form[INDEX_NUMBER_SIZE];

  db::ColumnFamilyHandle *m_cf_handle;

public:
  uint16_t m_index_dict_version;
  uchar m_index_type;
  /* KV format version for the index id */
  uint16_t m_kv_format_version;
  /* If true, the column family stores data in the reverse order */
  bool m_is_reverse_cf;

  bool m_is_auto_cf;
  std::string m_name;
  mutable SeIndexStats m_stats;

  /* if true, DDL or DML can skip unique check */
  bool m_can_skip_unique_check = false;

private:
  friend class SeTableDef; // for m_index_number above

  /* Number of key parts in the primary key*/
  uint m_pk_key_parts;

  /*
     pk_part_no[X]=Y means that keypart #X of this key is key part #Y of the
     primary key.  Y==-1 means this column is not present in the primary key.
  */
  uint *m_pk_part_no;

  /* Array of index-part descriptors. */
  SeFieldPacking *m_pack_info;

  uint m_keyno; /* number of this index in the table */

  /*
    Number of key parts in the index (including "index extension"). This is how
    many elements are in the m_pack_info array.
  */
  uint m_key_parts;

  /* Maximum length of the mem-comparable form. */
  uint m_maxlength;

  /* if support index-push-down then set true */
  bool m_is_support_icp;

  /* mutex to protect setup */
  mysql_mutex_t m_mutex;
};

} //namespace smartengine

#endif
