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

#ifdef USE_PRAGMA_INTERFACE
#pragma interface /* gcc class implementation */
#endif

/* C++ standard header files */
#include <string>
#include <unordered_set>
#include <vector>

/* MySQL header files */
#include "my_icp.h"
#include "sql_string.h"
#include "sql/field.h"
#include "handler.h"   /* handler */
#include "sql_bitmap.h"
#include "sql_string.h"

#include "mysql/psi/mysql_thread.h"

/* se header files */
#include "table/parallel_read.h"
#include "write_batch/write_batch_with_index.h"

/* SE handler header files */
#include "se_dict_struct.h"
#include "./se_index_merge.h"
#include "./se_sst_info.h"
#include "./se_utils.h"
#include "plugin/se_status_vars.h"
#include "util/se_buff.h"

namespace smartengine
{
class SeKeyDef;
class SeTableDef;
class SeTransaction;
class SeFieldEncoder;
struct SeInplaceDdlDictInfo;
struct ParallelDDLScanCtx;

/**
  @brief
  Class definition for se storage engine plugin handler
*/
class ha_smartengine : public my_core::handler {
public:
  ha_smartengine(my_core::handlerton *const hton, my_core::TABLE_SHARE *const table_arg);

  virtual ~ha_smartengine() override;

  virtual handler *clone(const char *name, MEM_ROOT *mem_root) override;

  virtual int create(const char *const name,
                     TABLE *const form,
                     HA_CREATE_INFO *const create_info,
                     dd::Table *table_def) override;

  virtual int delete_table(const char *const from, const dd::Table *table_def) override;

  virtual int rename_table(const char *const from,
                           const char *const to,
                           const dd::Table *from_table_def,
                           dd::Table *to_table_def) override;

  virtual void update_create_info(HA_CREATE_INFO *const create_info) override;

  virtual int open(const char *const name,
                   int mode,
                   uint test_if_locked,
                   const dd::Table *table_def) override;

  virtual int close() override;

  virtual THR_LOCK_DATA **store_lock(THD *const thd,
                                     THR_LOCK_DATA **to,
                                     enum thr_lock_type lock_type) override;

  virtual int external_lock(THD *const thd, int lock_type) override;

  virtual int start_stmt(THD *const thd, thr_lock_type lock_type) override;

  virtual void unlock_row() override;

  virtual int write_row(uchar *const buf) override;

  virtual int update_row(const uchar *const old_data, uchar *const new_data) override;

  virtual int delete_row(const uchar *const buf) override;

  virtual int truncate(dd::Table *table_def) override;

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  virtual int rnd_init(bool scan) override;

  virtual int rnd_end() override;

  virtual int rnd_next(uchar *const buf) override;

  virtual int rnd_pos(uchar *const buf, uchar *const pos) override;

  virtual void position(const uchar *const record) override;

  /* At the moment, we're ok with default handler::index_init() implementation.
   */
  virtual int index_read_map(uchar *const buf,
                             const uchar *const key,
                             key_part_map keypart_map,
                             enum ha_rkey_function find_flag) override;

  virtual int index_init(uint idx, bool sorted) override;

  virtual int index_end() override;

  virtual int index_next(uchar *const buf) override;

  virtual int index_next_same(uchar *buf, const uchar *key, uint length) override;

  virtual int index_prev(uchar *const buf) override;

  virtual int index_first(uchar *const buf) override;

  virtual int index_last(uchar *const buf) override;

  virtual int index_read_last_map(uchar *const buf,
                                  const uchar *const key,
                                  key_part_map keypart_map) override;

  virtual int read_range_first(const key_range *const start_key,
                               const key_range *const end_key,
                               bool eq_range,
                               bool sorted) override;

  virtual int read_range_next() override;

  virtual int info(uint flag) override;

  /* This function will always return success, therefore no annotation related
   * to checking the return value. Can't change the signature because it's
   * required by the interface. */
  virtual int extra(enum ha_extra_function operation) override;

  virtual int reset() override;

  virtual double scan_time() override;

  virtual double read_time(uint, uint, ha_rows rows) override;

  virtual ha_rows records_in_range(uint inx,
                                   key_range *const min_key,
                                   key_range *const max_key) override;

  /**
    TODO: return actual upper bound of number of records in the table.
    (e.g. save number of records seen on full table scan and/or use file size
    as upper bound)
  */
  virtual ha_rows estimate_rows_upper_bound() override;

  virtual int records(ha_rows *num_rows) override;

  /**The name that will be used for display purposes.*/
  virtual const char *table_type() const override;

  virtual int records_from_index(ha_rows *num_rows, uint index) override;

  virtual bool get_error_message(const int error, String *const buf) override;

  /**This is a list of flags that indicate what functionality the storage engine
  implements. The current table flags are documented in handler.h*/
  virtual handler::Table_flags table_flags() const override;
  
  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

    @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  virtual ulong index_flags(uint inx, uint part, bool all_parts) const override;

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  virtual uint max_supported_record_length() const override;

  virtual uint max_supported_keys() const override;

  virtual uint max_supported_key_parts() const override;

  virtual uint max_supported_key_part_length(HA_CREATE_INFO *create_info) const override;

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  virtual uint max_supported_key_length() const override;

  virtual bool primary_key_is_clustered() const override;

  virtual void get_auto_increment(ulonglong offset,
                                  ulonglong increment,
                                  ulonglong nb_desired_values,
                                  ulonglong *const first_value,
                                  ulonglong *const nb_reserved_values) override;

  virtual bool check_if_incompatible_data(HA_CREATE_INFO *const info, uint table_changes) override;

  virtual enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table,
      my_core::Alter_inplace_info *const ha_alter_info) override;

  virtual bool prepare_inplace_alter_table(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      const dd::Table *old_table_def,
      dd::Table *new_table_def) override;

  virtual bool inplace_alter_table(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      const dd::Table *old_table_def,
      dd::Table *new_table_def) override;

  virtual bool commit_inplace_alter_table(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      bool commit,
      const dd::Table *old_dd_tab,
      dd::Table *new_dd_tab) override;

  virtual int optimize(THD *const thd, HA_CHECK_OPT *const check_opt) override;

  virtual int analyze(THD *const thd, HA_CHECK_OPT *const check_opt) override;

  virtual int check(THD *const thd, HA_CHECK_OPT *const check_opt) override;

public:
  /*
    See if this is the same base table - this should only be true for different
    partitions of the same table.
  */
  bool same_table(const ha_smartengine &other) const;

  int finalize_bulk_load();

private:
  // TODO(Zhao Dongsheng) : The structure key_def_cf_info is useless.
  struct key_def_cf_info {
    smartengine::db::ColumnFamilyHandle *cf_handle;
    bool is_reverse_cf;
    bool is_auto_cf;

    key_def_cf_info() : cf_handle(nullptr),
                        is_reverse_cf(false),
                        is_auto_cf(false) {}
  };

  // used for both update and delete operations
  struct update_row_info {
    SeTransaction *tx;
    const uchar *new_data;
    const uchar *old_data;
    common::Slice new_pk_slice;
    common::Slice old_pk_slice;

    // "unpack_info" data for the new PK value
    SeStringWriter *new_pk_unpack_info;

    longlong hidden_pk_id;
    bool skip_unique_check;
  };

  //  Used to check for duplicate entries during fast unique index creation.
  struct unique_key_buf_info {
    bool key_buf_switch = false;
    common::Slice memcmp_key;
    common::Slice memcmp_key_old;
    uchar *dup_key_buf;
    uchar *dup_key_buf_old;

    unique_key_buf_info(uchar *key_buf, uchar *key_buf_old)
        : dup_key_buf(key_buf), dup_key_buf_old(key_buf_old) {}
    /*
      This method is meant to be called back to back during inplace creation
      of unique indexes.  It will switch between two buffers, which
      will each store the memcmp form of secondary keys, which are then
      converted to slices in memcmp_key or memcmp_key_old.

      Switching buffers on each iteration allows us to retain the
      memcmp_key_old value for duplicate comparison.
    */
    inline uchar *swap_and_get_key_buf() {
      key_buf_switch = !key_buf_switch;
      return key_buf_switch ? dup_key_buf : dup_key_buf_old;
    }
  };

  // used during prepare_inplace_alter_table to determine key stat in new table
  struct prepare_inplace_key_stat {
    std::string key_name;
    /* for key in new table, stat can be ADDED, COPIED, RENAMED or REDEFINED
     * for key in old table, stat can be COPIED, RENAMED or DROPPED
     */
    enum {
      ADDED=0, COPIED, RENAMED, REDEFINED, DROPPED
    } key_stat;

    // if new key is copied or renamed from key in old table
    std::string mapped_key_name; // name of old key
    uint mapped_key_index; // index of old key in SeTableDef::m_key_descr_arr

    // for debug message
    std::string to_string() const {
      if (!key_name.empty()) {
        std::ostringstream oss;
        switch (key_stat) {
          case ADDED:
            oss << key_name << " is newly ADDED.";
            break;
          case COPIED:
            oss << key_name << " is COPIED from " << mapped_key_name << " in old table.";
            break;
          case RENAMED:
            oss << key_name << " is RENAMED from " << mapped_key_name << " in old table.";
            break;
          case REDEFINED:
            oss << key_name << " is REDEFINED.";
            break;
          case DROPPED:
            oss << key_name << " is DROPPED.";
            break;
        }
        return oss.str();
      }
      return "";
    }
  };
  std::map<std::string, prepare_inplace_key_stat> new_key_stats;

private:
  int create_key_defs(const TABLE *new_table,
                      const TABLE *old_table,
                      const dd::Table *dd_table,
                      SeTableDef *new_table_def,
                      const SeTableDef *old_table_def,
                      LEX_CSTRING engine_attribute,
                      bool need_rebuild);

  int create_key_def(const TABLE *const table_arg,
                     const uint &i,
                     const SeTableDef *const tbl_def_arg,
                     std::shared_ptr<SeKeyDef> *const new_key_def,
                     const uint32_t index_id) const;

  int update_write_row(const uchar *const old_data,
                       const uchar *const new_data,
                       const bool skip_unique_check);

  int update_pk(const SeKeyDef &kd,
                const update_row_info &row_info,
                const bool &pk_changed);

  int update_sk(const TABLE *const table_arg,
                const SeKeyDef &kd,
                const update_row_info &row_info,
                const TABLE *const altered_table = nullptr);

  int update_indexes(const update_row_info &row_info,
                     const bool &pk_changed);

  int get_pk_for_update(update_row_info *const row_info);

  int delete_indexes(const uchar *const old_record);

  int secondary_index_read(const int keyno, uchar *const buf);

  void setup_iterator_for_rnd_scan();

  void setup_scan_iterator(const SeKeyDef &kd, common::Slice *const slice)
  {
    setup_scan_iterator(kd, slice, false, false, 0);
  }

  bool is_ascending(const SeKeyDef &keydef, enum ha_rkey_function find_flag) const;

  void setup_scan_iterator(const SeKeyDef &kd,
                           common::Slice *slice,
                           const bool use_all_keys,
                           const bool is_ascending,
                           const uint eq_cond_len,
                           const common::Slice* end_key = nullptr);

  void release_scan_iterator(void);

  common::Status get_for_update(SeTransaction *const tx,
                                db::ColumnFamilyHandle *const column_family,
                                const common::Slice &key,
                                std::string *const value,
                                bool lock_unique = false) const;

  common::Status lock_unique_key(SeTransaction *const tx,
                                 db::ColumnFamilyHandle *const column_family,
                                 const common::Slice &key,
                                 const bool skip_bloom_filter,
                                 const bool fill_cache) const;

  int get_row_by_rowid(uchar *const buf,
                       const char *const rowid,
                       const uint rowid_size,
                       std::string& retrieved_record,
                       TABLE* tbl,
                       String& key,
                       const bool skip_lookup = false);

  int get_row_by_rowid(uchar *const buf,
                       const uchar *const rowid,
                       const uint rowid_size,
                       const bool skip_lookup = false);

  int get_row_by_rowid(uchar *const buf,
                       const char *const rowid,
                       const uint rowid_size,
                       const bool skip_lookup = false);

  int get_latest_row_by_rowid(uchar *const buf,
                              const char *const rowid,
                              const uint rowid_size);

  void update_auto_incr_val();

  void load_auto_incr_value();

  longlong update_hidden_pk_val();

  int load_hidden_pk_value();

  int read_hidden_pk_id_from_rowkey(longlong *const hidden_pk_id, const String *key);

  bool can_use_single_delete(const uint &index) const;

  bool is_blind_delete_enabled() { return false; }

  bool skip_unique_check() const;

  bool commit_in_the_middle();

  bool do_bulk_commit(SeTransaction *const tx);

  bool has_hidden_pk(const TABLE *const table) const;

  void update_row_stats(const operation_type &type);

  void set_last_rowkey(const uchar *const old_data);

  /* Setup field_decoders based on type of scan and table->read_set */
  int setup_read_decoders();

  int setup_read_decoders(const TABLE *table,
                          SeFieldEncoder *encoder_arr,
                          InstantDDLInfo &instant_ddl_info,
                          bool force_decode_all_fields,
                          std::vector<READ_FIELD> &decoders_vect);

  void get_storage_type(SeFieldEncoder *const encoder,
                        std::shared_ptr<SeKeyDef> pk_descr,
                        const uint &kp,
                        bool &maybe_unpack_info);

  int setup_field_converters();

  int setup_field_converters(const TABLE *table,
                             std::shared_ptr<SeKeyDef> pk_descr,
                             SeFieldEncoder* &encoder_arr,
                             uint &fields_no_needed_to_decode,
                             uint &null_bytes_in_rec,
                             bool &maybe_unpack_info);

  int alloc_key_buffers(const TABLE *const table_arg,
                        const SeTableDef *const tbl_def_arg,
                        bool alloc_alter_buffers = false);

  void free_key_buffers();

  // the buffer size should be at least 2*SeKeyDef::INDEX_NUMBER_SIZE
  smartengine::db::Range get_range(const int &i, uchar buf[]) const;

  /*
    Update stats
  */
  void update_stats(void);

  int get_instant_ddl_info_if_needed(const TABLE *curr_table, const dd::Table *curr_dd_table, InstantDDLInfo &instant_ddl_info);

  bool init_with_fields() ;
 
  bool rpl_can_handle_stm_event() const noexcept; 

  bool should_store_row_debug_checksums() const
  {
    return m_store_row_debug_checksums && (rand() % 100 < m_checksums_pct);
  }

  int convert_blob_from_storage_format(
      my_core::Field_blob *const blob,
      SeStringReader *const reader,
      bool decode);

  int convert_varchar_from_storage_format(
      my_core::Field_varstring *const field_var,
      SeStringReader *const reader,
      bool decode);

  int convert_string_from_storage_format(
      my_core::Field_string *const field_str,
      SeStringReader *const reader,
      bool decode);

  int convert_field_from_storage_format(
      my_core::Field *const field,
      SeStringReader *const reader,
      bool decode,
      uint len);

  int convert_record_from_storage_format(
      const common::Slice *key,
      const common::Slice *value,
      uchar *const buf,
      TABLE* t,
      uint32_t null_bytes_in_rec,
      bool maybe_unpack_info,
      const SeKeyDef *pk_descr,
      std::vector<READ_FIELD>& decoders_vect,
      InstantDDLInfo* instant_ddl_info,
      bool verify_row_debug_checksums,
      my_core::ha_rows& row_checksums_checked);

  int convert_record_from_storage_format(
      const common::Slice *key,
      std::string& retrieved_record,
      uchar *const buf,
      TABLE* tbl);

  int convert_record_from_storage_format(
      const common::Slice *key,
      const common::Slice *value,
      uchar *const buf,
      TABLE* tbl);

  int append_blob_to_storage_format(
      String &storage_record,
      my_core::Field_blob *const blob);

  int append_varchar_to_storage_format(
      String &storage_record,
      my_core::Field_varstring *const field_var);

  int append_string_to_storage_format(
      String &storage_record,
      my_core::Field_string *const field_str);

  int convert_record_to_storage_format(
      const common::Slice &pk_packed_slice,
      SeStringWriter *const pk_unpack_info,
      common::Slice *const packed_rec);

  int convert_new_record_from_old_record(
      const TABLE *table,
      const TABLE *altered_table,
      const std::shared_ptr<SeInplaceDdlDictInfo>& dict_info,
      const common::Slice &pk_packed_slice,
      SeStringWriter *const pk_unpack_info,
      common::Slice *const packed_rec,
      String& new_storage_record);

  static const char *get_key_name(
      const uint index,
      const TABLE *const table_arg,
      const SeTableDef *const tbl_def_arg);

  static const char *get_key_comment(
      const uint index,
      const TABLE *const table_arg,
      const SeTableDef *const tbl_def_arg);

  static bool is_hidden_pk(const uint index,
                           const TABLE *const table_arg,
                           const SeTableDef *const tbl_def_arg);

  static uint pk_index(const TABLE *const table_arg,
                       const SeTableDef *const tbl_def_arg);

  static bool is_pk(const uint index,
                    const TABLE *table_arg,
                    const SeTableDef *tbl_def_arg);

  int index_read_map_impl(uchar *const buf,
                          const uchar *const key,
                          key_part_map keypart_map,
                          enum ha_rkey_function find_flag,
                          const key_range *end_key);


  common::Status delete_or_singledelete(
      uint index,
      SeTransaction *const tx,
      db::ColumnFamilyHandle *const cf,
      const common::Slice &key);

  common::Status delete_or_singledelete_new_table(
      const TABLE *altered_table,
      SeTableDef *new_tbl_def,
      uint index,
      SeTransaction *const tx,
      db::ColumnFamilyHandle *const cf,
      const common::Slice &key);

  int index_next_with_direction(uchar *const buf, bool move_forward);

  class Item *idx_cond_push(uint keyno, class Item *const idx_cond) override;

  int create_inplace_key_defs(
      const TABLE *const table_arg,
      SeTableDef *const vtbl_def_arg,
      const TABLE *const old_table_arg,
      const SeTableDef *const old_tbl_def_arg,
      const std::array<uint32_t, MAX_INDEXES + 1> &index_ids) const;

  std::unordered_map<std::string, uint> get_old_key_positions(
      const TABLE *table_arg,
      const SeTableDef *tbl_def_arg,
      const TABLE *old_table_arg,
      const SeTableDef *old_tbl_def_arg) const;

  int compare_key_parts(const KEY *const old_key, const KEY *const new_key) const;

  int index_first_intern(uchar *buf);

  int index_last_intern(uchar *buf);

  enum icp_result check_index_cond() const;

  int find_icp_matching_index_rec(const bool &move_forward, uchar *const buf);

  int calc_updated_indexes();

  int check_and_lock_unique_pk(const uint &key_id,
                               const update_row_info &row_info,
                               bool *const found, bool *const pk_changed);

  void check_new_index_unique_inner(const SeKeyDef &kd,
                                    Added_key_info &ki,
                                    const update_row_info &row_info,
                                    const common::Slice &key,
                                    bool is_rebuild,
                                    bool *const found) const;

  int lock_and_check_new_index_pk(const SeKeyDef &kd,
                                  Added_key_info &ki,
                                  const update_row_info &row_info,
                                  bool *const pk_chnaged,
                                  bool *const found) const;

  int lock_and_check_new_index_sk(const SeKeyDef &kd, Added_key_info &ki,
                                  const update_row_info &row_info,
                                  bool *const found,
                                  bool is_rebuild = false) const;

  int check_and_lock_sk(const uint &key_id,
                        const update_row_info &row_info,
                        bool *const found) const;

  int check_uniqueness_and_lock(const update_row_info &row_info,
                                bool *const pk_changed);

  int check_uniqueness_and_lock_rebuild(const update_row_info &row_info,
                                        bool *const pk_changed);

  bool over_bulk_load_threshold(int *err);

  bool check_duplicate_in_base(const TABLE *table_arg,
                               const SeKeyDef &index,
                               const common::Slice& key,
                               unique_key_buf_info *key_buf_info);

  int fill_new_duplicate_record(const SeKeyDef *index,
                                TABLE *new_table_arg,
                                const common::Slice &dup_key,
                                const common::Slice &dup_val,
                                bool is_rebuild);

  int fill_old_table_record(TABLE *new_table,
                            TABLE *old_table,
                            const common::Slice &dup_key,
                            const SeKeyDef *dup_kd);

  int get_new_pk_for_update(update_row_info *const row_info, const TABLE *altered_table);

  int get_new_pk_for_delete(update_row_info *const row_info, const TABLE *altered_table);

  int bulk_load_key(SeTransaction *const tx,
                    const SeKeyDef &kd,
                    const common::Slice &key,
                    const common::Slice &value,
                    int level);

  int update_pk_for_new_table(const SeKeyDef &kd,
                              Added_key_info &ki,
                              const update_row_info &row_info,
                              const bool pk_changed,
                              const TABLE *altered_table);

  int update_sk_for_new_table(const SeKeyDef &kd,
                              Added_key_info &ki, 
                              const update_row_info &row_info,
                              const TABLE *const altered_table);

  int update_new_table(update_row_info &row_info, bool pk_changed);

  int delete_row_new_table(update_row_info &row_info);

  int read_key_exact(const SeKeyDef &kd,
                     db::Iterator *const iter,
                     const bool &using_full_key,
                     const common::Slice &key_slice) const;

  int read_before_key(const SeKeyDef &kd,
                      const bool &using_full_key,
                      const common::Slice &key_slice);

  int read_after_key(const SeKeyDef &kd,
                     const bool &using_full_key,
                     const common::Slice &key_slice);

  int position_to_correct_key(const SeKeyDef &kd,
                              enum ha_rkey_function &find_flag,
                              bool &full_key_match,
                              const uchar *key,
                              key_part_map &keypart_map,
                              common::Slice &key_slice,
                              bool *move_forward);

  int read_row_from_primary_key(uchar *const buf);

  int read_row_from_secondary_key(
      uchar *const buf,
      const SeKeyDef &kd,
      bool move_forward);

  int calc_eq_cond_len(const SeKeyDef &kd,
                       enum ha_rkey_function &find_flag,
                       common::Slice &slice,
                       int &bytes_changed_by_succ,
                       const key_range *end_key,
                       uint *end_key_packed_size);

  void read_thd_vars(THD *const thd);

  const char *thd_se_tmpdir();

  bool contains_foreign_key(THD *const thd);

  // avoid  write sst of creating second index when do shrink
  // extent space crash
  int inplace_populate_indexes(
      TABLE *const table_arg,
      const std::unordered_set<std::shared_ptr<SeKeyDef>> &indexes,
      const std::shared_ptr<SeKeyDef>& primary_key,
      bool is_rebuild);

  int inplace_populate_index(TABLE *const new_table_arg,
                             const std::shared_ptr<SeKeyDef>& index,
                             bool is_rebuild);

  Instant_Type check_if_support_instant_ddl(const Alter_inplace_info *ha_alter_info);

  int rnd_next_with_direction(uchar *const buf, bool move_forward);



  /* used for scan se by single thread */
  int records_from_index_single(ha_rows *num_rows, uint index);

  int scan_parallel(SeKeyDef *kd,
                    SeTransaction *const tx,
                    common::ParallelReader::F &&f);

  int check_parallel(THD *const thd, HA_CHECK_OPT *const check_opt);

  /** mark ddl transaction modified se */
  void mark_ddl_trx_read_write();

  int calculate_stats(const TABLE *const table_arg,
                      THD *const thd,
                      HA_CHECK_OPT *const check_opt);

  int prepare_inplace_alter_table_dict(
    Alter_inplace_info *ha_alter_info,
    const TABLE *altered_table,
    const TABLE *old_table,
    SeTableDef *old_tbl_def,
    const char *table_name);

  int prepare_inplace_alter_table_rebuild(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      SeTableDef *const new_tdef,
      std::shared_ptr<SeKeyDef> *const old_key_descr,
      uint old_n_keys,
      std::shared_ptr<SeKeyDef> *const new_key_descr,
      uint new_n_keys);

  int prepare_inplace_alter_table_skip_unique_check(
      TABLE *const altered_table,
      SeTableDef *const new_tbl_def) const;

  int prepare_inplace_alter_table_collect_key_stats(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info);

  int inplace_populate_new_table(TABLE *altered_table, SeTableDef *new_tdef);

  size_t get_parallel_read_threads();

  void print_dup_err(int res, std::atomic<int>& dup_ctx_id,
                     const std::shared_ptr<SeKeyDef>& index,
                     bool is_rebuild,
                     TABLE *const new_table_arg,
                     std::vector<common::Slice>& dup_key,
                     std::vector<common::Slice>& dup_val,
                     Added_key_info& added_key_info,
                     KEY* key_info);

  void print_common_err(int res);

  int build_index(common::Slice& iter_key,
                  common::Slice& iter_value,
                  ParallelDDLScanCtx & ddl_ctx,
                  common::Slice& key,
                  common::Slice& val,
                  TABLE* tbl,
                  bool hidden_pk_exists,
                  const std::shared_ptr<SeKeyDef>& index,
                  bool is_rebuild,
                  TABLE *const new_table_arg);

  int build_base_global_merge(
      std::vector<std::shared_ptr<ParallelDDLScanCtx>>& ddl_ctx_set,
      const std::shared_ptr<SeKeyDef>& index,
      bool is_rebuild,
      TABLE *const new_table_arg,
      size_t max_threads,
      bool need_unique_check,
      Added_key_info& added_key_info,
      size_t part_id,
      common::Slice& dup_key,
      common::Slice& dup_val,
      std::atomic<int>& dup_ctx_id);

  int inplace_build_base_phase_parallel(
      TABLE *const thread_id,
      const std::shared_ptr<SeKeyDef>& index,
      bool is_unique_index,
      Added_key_info& added_key_info,
      bool is_rebuild);

  int inplace_build_base_phase(TABLE *const new_table_arg,
                               const std::shared_ptr<SeKeyDef>& index,
                               bool is_unique_index,
                               Added_key_info& added_key_info,
                               bool is_rebuild);

  using UserKeySequence = std::pair<common::SequenceNumber, smartengine::db::Iterator::RecordStatus>;
  static bool check_user_key_sequence(std::vector<UserKeySequence> &user_key_sequences);

  int inplace_check_unique_phase(TABLE *new_table_arg,
                                 const std::shared_ptr<SeKeyDef>& index,
                                 Added_key_info& added_key_info,
                                 bool is_rebuild);

  int inplace_update_added_key_info_status_dup_key(
      TABLE *const new_table_arg,
      const std::shared_ptr<SeKeyDef>& index,
      Added_key_info& added_key_info,
      const common::Slice& dup_key,
      const common::Slice& dup_val,
      bool is_rebuild,
      bool use_key=true);

  int inplace_update_added_key_info_step(
      Added_key_info& added_key_info,
      Added_key_info::ADD_KEY_STEP new_step) const;

  // During build base phase, when every this number of keys merged,
  // duplicate status of DML will checked.
  static int inplace_check_dml_error(TABLE *const old_table_arg,
                                     TABLE *const new_table_arg,
                                     KEY *key,
                                     const Added_key_info& added_key_info,
                                     bool print_err = true);

  int prepare_inplace_alter_table_norebuild(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      SeTableDef *const new_tdef,
      std::shared_ptr<SeKeyDef> *const old_key_descr,
      uint old_n_keys,
      std::shared_ptr<SeKeyDef> *const new_key_descr,
      uint new_n_keys);

  int dd_commit_instant_table(const TABLE *old_table,
                              const TABLE *new_table,
                              const dd::Table *old_dd_table,
                              dd::Table *new_dd_table);

  void dd_commit_inplace_no_change(const dd::Table *old_dd_table,
                                   dd::Table *new_dd_table);

  int dd_commit_inplace_instant(Alter_inplace_info *ha_alter_info,
                                const TABLE *old_table,
                                const TABLE *new_table,
                                const dd::Table *old_dd_table,
                                dd::Table *new_dd_table);

  bool commit_inplace_alter_table_norebuild(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      bool commit,
      const dd::Table *old_dd_table,
      dd::Table *new_dd_table);

  bool commit_inplace_alter_table_rebuild(
      TABLE *const altered_table,
      my_core::Alter_inplace_info *const ha_alter_info,
      bool commit,
      const dd::Table *old_dd_table,
      dd::Table *new_dd_table);

  int commit_inplace_alter_table_common(my_core::TABLE *const altered_table,
                                        my_core::Alter_inplace_info *const ha_alters_info,
                                        const dd::Table *new_dd_table, 
                                        const std::unordered_set<std::shared_ptr<SeKeyDef>> &new_added_indexes);

  int commit_inplace_alter_get_autoinc(
      Alter_inplace_info *ha_alter_info,
      const TABLE *altered_table,
      const TABLE *old_table);

  int rollback_added_index(const std::unordered_set<std::shared_ptr<SeKeyDef>>& added_indexes);

  void set_use_read_free_rpl(const char *const whitelist);

  void set_skip_unique_check_tables(const char *const whitelist);

  virtual void rpl_before_delete_rows() ;

  virtual void rpl_after_delete_rows() ;

  virtual void rpl_before_update_rows() ;

  virtual void rpl_after_update_rows() ;

  virtual bool use_read_free_rpl();

  bool is_using_full_key(key_part_map keypart_map, uint actual_key_parts);

  bool is_using_full_unique_key(
      uint active_index,
      key_part_map keypart_map,
      enum ha_rkey_function find_flag);

  bool is_using_prohibited_gap_locks(TABLE *table, bool using_full_unique_key);

  int build_table_schema(const TABLE *table,
                         const dd::Table *dd_table,
                         const SeTableDef *table_def,
                         LEX_CSTRING engine_attribute_str,
                         schema::TableSchema &table_schema);

  int build_column_schemas(const TABLE *table,
                           const std::vector<READ_FIELD> &field_decoders,
                           schema::ColumnSchemaArray &col_schemas);

  int pushdown_table_schema(const TABLE *table,
                            const dd::Table *dd_table,
                            const SeTableDef *table_def,
                            LEX_CSTRING engine_attribute_str);

private:
  typedef struct System_status_var SSV; 

#if MAX_INDEXES <= 64
  typedef Bitmap<64>  key_map;          /* Used for finding keys */
#elif MAX_INDEXES > 255
#error "MAX_INDEXES values greater than 255 is not supported."
#else
  typedef Bitmap<((MAX_INDEXES+7)/8*8)> key_map; /* Used for finding keys */
#endif

  //combine buffer size is sort_buffer_size*16
  static const int64_t SE_MERGE_COMBINE_READ_SIZE_RATIO = 16;

  static const int64_t SE_ASSUMED_KEY_VALUE_DISK_SIZE = 100;

  static const int BUILD_BASE_CHECK_ERROR_FREQUENCY = 100;
private:

  friend struct ParallelScanCtx;

  my_core::THR_LOCK_DATA m_db_lock; ///< MySQL database lock

  SeTableHandler *m_table_handler; ///< Open table handler

  /* Iterator used for range scans and for full table/index scans */
  smartengine::db::Iterator *m_scan_it;

  bool m_cmp_end_key;

  uint m_max_packed_sk_len;
  /* length of packed pk tuple */
  uint m_pack_key_len;
  /* Whether m_scan_it was created with skip_bloom=true */
  bool m_scan_it_skips_bloom;

  const smartengine::db::Snapshot *m_scan_it_snapshot;

  // TODO(Zhao Dongsheng): use name m_table_def.
  std::shared_ptr<SeTableDef> m_tbl_def;

  /* instant ddl information used for decoding */
  InstantDDLInfo m_instant_ddl_info;

  /* number of fields not stored in a smartengine value */
  uint32_t m_fields_no_needed_to_decode;

  /* Primary Key encoder from KeyTupleFormat to StorageFormat */
  std::shared_ptr<SeKeyDef> m_pk_descr;

  /* Array of index descriptors */
  std::shared_ptr<SeKeyDef> *m_key_descr_arr;

  bool check_keyread_allowed(uint inx, uint part, bool all_parts) const;

  /*
    Number of key parts in PK. This is the same as
      table->key_info[table->s->primary_key].keyparts
  */
  uint m_pk_key_parts;

  /*
    TRUE <=> Primary Key columns can be decoded from the index
  */
  mutable bool m_pk_can_be_decoded;

  /*
   TRUE <=> Some fields in the PK may require unpack_info.
  */
  bool m_maybe_unpack_info;

  uchar *m_pk_tuple;        /* Buffer for storing PK in KeyTupleFormat */
  uchar *m_pk_packed_tuple; /* Buffer for storing PK in StorageFormat */
  // ^^ todo: change it to 'char*'? TODO: ^ can we join this with last_rowkey?

  /* Buffer for storing old-pk for new_tbl in StorageFormat during online-ddl */
  uchar *m_pk_packed_tuple_old;
  /*
    Temporary buffers for storing the key part of the Key/Value pair
    for secondary indexes.
  */
  uchar *m_sk_packed_tuple;

  /*
    Temporary buffers for storing end key part of the Key/Value pair.
    This is used for range scan only.
  */
  uchar *m_end_key_packed_tuple;

  SeStringWriter m_sk_tails;
  SeStringWriter m_pk_unpack_info;

  /*
    ha_rockdb->index_read_map(.. HA_READ_KEY_EXACT or similar) will save here
    mem-comparable form of the index lookup tuple.
  */
  uchar *m_sk_match_prefix;
  uint m_sk_match_length;

  /* Buffer space for the above */
  uchar *m_sk_match_prefix_buf;

  /* Second buffers, used by UPDATE. */
  uchar *m_sk_packed_tuple_old;
  SeStringWriter m_sk_tails_old;

  /* Buffers used for duplicate checking during unique_index_creation */
  uchar *m_dup_sk_packed_tuple;
  uchar *m_dup_sk_packed_tuple_old;

  /*
    Temporary space for packing VARCHARs (we provide it to
    pack_record()/pack_index_tuple() calls).
  */
  uchar *m_pack_buffer;

  /* new table Primary Key encoder from KeyTupleFormat to StorageFormat */
  std::shared_ptr<SeKeyDef> m_new_pk_descr;

  /* Buffers for new_record during online-copy-ddl */
  uchar *m_new_record;

  /* rowkey of the last record we've read, in StorageFormat. */
  String m_last_rowkey;

  /* Buffer used by convert_record_to_storage_format() */
  String m_storage_record;

  /* Buffer used by convert_new_record_to_storage_format() */
  String m_new_storage_record;
  bool m_new_maybe_unpack_info;

  /*
    Last retrieved record, in table->record[0] data format.

    This is used only when we get the record with se's Get() call (The
    other option is when we get a common::Slice from an iterator)
  */
  std::string m_retrieved_record;

  /* Type of locking to apply to rows */
  enum { SE_LOCK_NONE, SE_LOCK_READ, SE_LOCK_WRITE } m_lock_rows;

  /* TRUE means we're doing an index-only read. FALSE means otherwise. */
  bool m_keyread_only;

  bool m_skip_scan_it_next_call;

  /* TRUE means we are accessing the first row after a snapshot was created */
  bool m_rnd_scan_is_new_snapshot;

  /* TRUE means the replication slave will use Read Free Replication */
  bool m_use_read_free_rpl;

  /*
    TRUE means we should skip unique key checks for this table if the
    replication lag gets too large
   */
  bool m_skip_unique_check;

  /**
    @brief
    This is a bitmap of indexes (i.e. a set) whose keys (in future, values) may
    be changed by this statement. Indexes that are not in the bitmap do not need
    to be updated.
    @note Valid inside UPDATE statements, IIF(m_update_scope_is_valid == true).
  */
  key_map m_update_scope;
  bool m_update_scope_is_valid;

  /* SST information used for bulk loading the primary key */
  std::shared_ptr<SeSstInfo> m_sst_info;
  std::shared_ptr<smartengine::db::MiniTables> mtables_;
  std::unique_ptr<smartengine::storage::ChangeInfo> m_change_info;
  SeTransaction *m_bulk_load_tx;
  /* Mutex to protect finalizing bulk load */
  mysql_mutex_t m_bulk_load_mutex;

  /*
    MySQL index number for duplicate key error
  */
  int m_dupp_errkey;


  /*
    Array of table->s->fields elements telling how to store fields in the
    record.
  */
  SeFieldEncoder *m_encoder_arr;

  /*
    This tells which table fields should be decoded (or skipped) when
    decoding table row from (pk, encoded_row) pair. (Secondary keys are
    just always decoded in full currently)
  */
  std::vector<READ_FIELD> m_decoders_vect;

  /*
    Number of bytes in on-disk (storage) record format that are used for
    storing SQL NULL flags.
  */
  uint m_null_bytes_in_rec;


  /*
    A counter of how many row checksums were checked for this table. Note that
    this does not include checksums for secondary index entries.
  */
  my_core::ha_rows m_row_checksums_checked;


  /*
    Controls whether writes include checksums. This is updated from the session
    variable
    at the start of each query.
  */
  bool m_store_row_debug_checksums;

  /* Same as above but for verifying checksums when reading */
  bool m_verify_row_debug_checksums;
  int m_checksums_pct;



  /* Flags tracking if we are inside different replication operation */
  bool m_in_rpl_delete_rows;
  bool m_in_rpl_update_rows;

  bool m_force_skip_unique_check;
};

} //namespace smartengine
