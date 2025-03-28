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

#include "ha_smartengine.h"
#include "sql/debug_sync.h"
#include "sql/item.h"
#include "sql/sql_class.h"
#include "my_bit.h"
#include "dict/se_field_pack.h"
#include "dict/se_table.h"
#include "dict/se_dict_util.h"
#include "index/se_parallel_ddl_ctx.h"
#include "sql/system_variables.h"
#include "plugin/se_system_vars.h"
#include "schema/record_format.h"
#include "transaction/se_transaction_factory.h"
#include "transaction/se_transaction_impl.h"
#include "transactions/transaction_db_impl.h"

namespace smartengine
{
extern util::TransactionDB *se_db;

static uint32_t se_debug_optimizer_n_rows = 0;

int ha_smartengine::rnd_init(bool scan)
{
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_RND_INIT);

  SeTransaction *const tx = get_or_create_tx(table->in_use);
  setup_read_decoders();

  if (scan) {
    m_rnd_scan_is_new_snapshot = !tx->has_snapshot();
    setup_iterator_for_rnd_scan();
  } else {
    /* We don't need any preparations for rnd_pos() calls. */
  }

  // If m_lock_rows is on then we will be doing a get_for_update when accessing
  // the index, so don't acquire the snapshot right away.  Otherwise acquire
  // the snapshot immediately.
  tx->acquire_snapshot(m_lock_rows == SE_LOCK_NONE);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::rnd_end()
{
  DBUG_ENTER_FUNC();

  release_scan_iterator();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::rnd_next(uchar *const buf)
{
  DBUG_ENTER_FUNC();
  QUERY_TRACE_BEGIN(monitor::TracePoint::HA_RND_NEXT);

  int rc;
  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  for (;;) {
    rc = rnd_next_with_direction(buf, true);
    if (rc != HA_ERR_LOCK_DEADLOCK || !m_rnd_scan_is_new_snapshot)
      break; /* exit the loop */

    // release the snapshot and iterator and then regenerate them
    SeTransaction *tx = get_or_create_tx(table->in_use);
    tx->release_snapshot();
    release_scan_iterator();
    setup_iterator_for_rnd_scan();
  }

  m_rnd_scan_is_new_snapshot = false;

  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  QUERY_TRACE_END();
  DBUG_RETURN(rc);
}

int ha_smartengine::rnd_pos(uchar *const buf, uchar *const pos)
{
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_READ_ROW);
  int rc;
  size_t len;

  ha_statistic_increment(&SSV::ha_read_rnd_count);
  //stats.rows_requested++;
  len = m_pk_descr->key_length(table,
                               common::Slice((const char *)pos, ref_length));
  if (len == size_t(-1)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR); /* Data corruption? */
  }

  rc = get_row_by_rowid(buf, pos, len);

  if (!rc) {
    //stats.rows_read++;
    update_row_stats(ROWS_READ);
  }

  DBUG_RETURN(rc);
}

void ha_smartengine::position(const uchar *const record)
{
  DBUG_ENTER_FUNC();
  longlong hidden_pk_id = 0;
  if (has_hidden_pk(table) && read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey))
    assert(false); // should never reach here

  /*
    Get packed primary key value from the record.

    (Note: m_last_rowkey has the packed PK of last-read-row, which allows to
    handle most cases, but there is an exception to this: when slave applies
    RBR events, it fills the record and then calls position(); rnd_pos())

    Also note that we *can* rely on PK column values being available. This is
    because table_flags() includes HA_PRIMARY_KEY_REQUIRED_FOR_POSITION bit.
    When that is true, table->prepare_for_position() adds PK columns into the
    read set (this may potentially disable index-only access when PK column
    cannot be restored from its mem-comparable form in the secondary indexes).
  */
  const uint packed_size = m_pk_descr->pack_record(
      table, m_pack_buffer, record, ref, nullptr, false, hidden_pk_id);
  assert(packed_size <= m_max_packed_sk_len);

  /*
    It could be that mem-comparable form of PK occupies less than ref_length
    bytes. Fill the remainder with zeros.
  */
  if (ref_length > packed_size)
    memset(ref + packed_size, 0, ref_length - packed_size);

  DBUG_VOID_RETURN;
}

int ha_smartengine::index_read_map(uchar *const buf,
                                   const uchar *const key,
                                   key_part_map keypart_map,
                                   enum ha_rkey_function find_flag)
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(index_read_map_impl(buf, key, keypart_map, find_flag, nullptr));
}

int ha_smartengine::index_init(uint idx, bool sorted)
{
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_INDEX_INIT);

  SeTransaction *const tx = get_or_create_tx(table->in_use);
  assert(tx != nullptr);

  setup_read_decoders();

  // If m_lock_rows is not SE_LOCK_NONE then we will be doing a get_for_update
  // when accessing the index, so don't acquire the snapshot right away.
  // Otherwise acquire the snapshot immediately.
  tx->acquire_snapshot(m_lock_rows == SE_LOCK_NONE);

  active_index = idx;

  // check if se support read data from index
  if (m_keyread_only) {
    const SeKeyDef &kd = *m_key_descr_arr[idx];
    if (!kd.get_support_icp_flag()) {
      m_keyread_only = false;
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::index_end()
{
  DBUG_ENTER_FUNC();

  release_scan_iterator();

  active_index = MAX_KEY;
  in_range_check_pushed_down = FALSE;

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::index_next(uchar *const buf)
{
  DBUG_ENTER_FUNC();
  QUERY_TRACE_BEGIN(monitor::TracePoint::HA_INDEX_NEXT);

  bool moves_forward = true;
  ha_statistic_increment(&SSV::ha_read_next_count);
  if (m_key_descr_arr[active_index]->m_is_reverse_cf) {
    moves_forward = false;
  }

  int rc = index_next_with_direction(buf, moves_forward);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  QUERY_TRACE_END();
  DBUG_RETURN(rc);
}

int ha_smartengine::index_next_same(uchar *buf, const uchar *key [[maybe_unused]], uint length [[maybe_unused]])
{
  DBUG_TRACE;
  ha_statistic_increment(&SSV::ha_read_next_count);
  return index_next(buf);
}

int ha_smartengine::index_prev(uchar *const buf)
{
  DBUG_ENTER_FUNC();
  QUERY_TRACE_BEGIN(monitor::TracePoint::HA_INDEX_NEXT);

  bool moves_forward = false;
  ha_statistic_increment(&SSV::ha_read_prev_count);
  if (m_key_descr_arr[active_index]->m_is_reverse_cf) {
    moves_forward = true;
  }

  int rc = index_next_with_direction(buf, moves_forward);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  QUERY_TRACE_END();
  DBUG_RETURN(rc);
}

int ha_smartengine::index_first(uchar *const buf)
{
  DBUG_ENTER_FUNC();

  m_sk_match_prefix = nullptr;
  ha_statistic_increment(&SSV::ha_read_first_count);
  int rc = m_key_descr_arr[active_index]->m_is_reverse_cf
               ? index_last_intern(buf)
               : index_first_intern(buf);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  DBUG_RETURN(rc);
}

int ha_smartengine::index_last(uchar *const buf)
{
  DBUG_ENTER_FUNC();

  m_sk_match_prefix = nullptr;
  ha_statistic_increment(&SSV::ha_read_last_count);
  int rc = m_key_descr_arr[active_index]->m_is_reverse_cf
               ? index_first_intern(buf)
               : index_last_intern(buf);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  DBUG_RETURN(rc);
}

int ha_smartengine::index_read_last_map(uchar *const buf,
                                        const uchar *const key,
                                        key_part_map keypart_map)
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST));
}

/*
  ha_smartengine::read_range_first overrides handler::read_range_first.
  The only difference from handler::read_range_first is that
  ha_smartengine::read_range_first passes end_key to
  ha_smartengine::index_read_map_impl function.
*/
int ha_smartengine::read_range_first(const key_range *const start_key,
                                     const key_range *const end_key,
                                     bool eq_range_arg,
                                     bool sorted)
{
  DBUG_ENTER_FUNC();

  int result;

  eq_range = eq_range_arg;
  set_end_range(end_key, RANGE_SCAN_ASC);

  range_key_part = table->key_info[active_index].key_part;

  if (!start_key) // Read first record
    result = ha_index_first(table->record[0]);
  else {
    // we should implement the following logic with MySQL 8.0
    if (is_using_prohibited_gap_locks(
            table,
            is_using_full_unique_key(active_index, start_key->keypart_map,
                                     start_key->flag))) {
      DBUG_RETURN(HA_ERR_LOCK_DEADLOCK);
    }

    result = index_read_map_impl(table->record[0], start_key->key,
                                 start_key->keypart_map, start_key->flag,
                                 end_key);
  }
  if (result)
    DBUG_RETURN((result == HA_ERR_KEY_NOT_FOUND) ? HA_ERR_END_OF_FILE : result);

  if (compare_key(end_range) <= 0) {
    DBUG_RETURN(HA_EXIT_SUCCESS);
  } else {
    /*
      The last read row does not fall in the range. So request
      storage engine to release row lock if possible.
    */
    unlock_row();
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
}

int ha_smartengine::read_range_next()
{
  int result;
  DBUG_ENTER("ha_smartengine::read_range_next");

  if (eq_range)
  {
    /* We trust that index_next_same always gives a row in range */
    DBUG_RETURN(ha_index_next_same(table->record[0],
                                   end_range->key,
                                   end_range->length));
  }
  result= ha_index_next(table->record[0]);

  if (result) {
    // index_next will return HA_ERR_END_OF_FILE, when meets end key
    if (HA_ERR_END_OF_FILE == result) {
      unlock_row();
    }
    DBUG_RETURN(result);
  }

  if (!m_cmp_end_key) {
    DBUG_RETURN(0);
  } else if (compare_key(end_range) <= 0) {
    DBUG_RETURN(0);
  } else {
    /*
      The last read row does not fall in the range. So request
      storage engine to release row lock if possible.
    */
    unlock_row();
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
}

int ha_smartengine::info(uint flag)
{
  DBUG_ENTER_FUNC();

  if (!table)
    return HA_EXIT_FAILURE;

  if (flag & HA_STATUS_VARIABLE) {
    /*
      Test only to simulate corrupted stats
    */
    DBUG_EXECUTE_IF("myx_simulate_negative_stats",
                    m_pk_descr->m_stats.m_actual_disk_size =
                        -m_pk_descr->m_stats.m_actual_disk_size;);

    update_stats();

    /*
      If any stats are negative due to bad cached stats, re-run analyze table
      and re-retrieve the stats.
    */
    if (static_cast<longlong>(stats.data_file_length) < 0 ||
        static_cast<longlong>(stats.index_file_length) < 0 ||
        static_cast<longlong>(stats.records) < 0) {
      if (analyze(nullptr, nullptr)) {
        DBUG_RETURN(HA_EXIT_FAILURE);
      }

      update_stats();
    }

    // if number of records is hardcoded, we do not want to force computation
    // of memtable cardinalities
    if (stats.records == 0 || stats.records == 1 ||
        (se_debug_optimizer_n_rows == 0))
    {
      // First, compute SST files stats
      uchar buf[SeKeyDef::INDEX_NUMBER_SIZE * 2];
      auto r = get_range(pk_index(table, m_tbl_def.get()), buf);
      uint64_t sz = 0;

      uint8_t include_flags = db::DB::INCLUDE_FILES;
      // recompute SST files stats only if records count is 0
    if (stats.records == 0 || stats.records == 1) {
        se_db->GetApproximateSizes(m_pk_descr->get_cf(), &r, 1, &sz,
                                 include_flags);
        stats.records+= sz/SE_ASSUMED_KEY_VALUE_DISK_SIZE;
        stats.data_file_length+= sz;
      }

      // Second, compute memtable stats
      uint64_t memtableCount;
      uint64_t memtableSize;
      se_db->GetApproximateMemTableStats(m_pk_descr->get_cf(), r,
                                            &memtableCount, &memtableSize);
      stats.records += memtableCount;
      stats.data_file_length += memtableSize;

      if (se_debug_optimizer_n_rows > 0)
        stats.records = se_debug_optimizer_n_rows;
    }

    if (stats.records != 0)
      stats.mean_rec_length = stats.data_file_length / stats.records;
  }
  if (flag & HA_STATUS_CONST) {
    ref_length = m_pk_descr->max_storage_fmt_length();

    // TODO: Needs to reimplement after having real index statistics
    for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
      if (is_hidden_pk(i, table, m_tbl_def.get())) {
        continue;
      }
      KEY *const k = &table->key_info[i];
      for (uint j = 0; j < k->actual_key_parts; j++) {
        const SeIndexStats &k_stats = m_key_descr_arr[i]->m_stats;
        uint x = k_stats.m_distinct_keys_per_prefix.size() > j &&
                         k_stats.m_distinct_keys_per_prefix[j] > 0
                     ? k_stats.m_rows / k_stats.m_distinct_keys_per_prefix[j]
                     : 0;
        if (x > stats.records)
          x = stats.records;
        if ((x == 0/* && se_debug_optimizer_no_zero_cardinality*/) ||
            se_debug_optimizer_n_rows > 0) {
          // Fake cardinality implementation. For example, (idx1, idx2, idx3)
          // index
          // will have rec_per_key for (idx1)=4, (idx1,2)=2, and (idx1,2,3)=1.
          // rec_per_key for the whole index is 1, and multiplied by 2^n if
          // n suffix columns of the index are not used.
          x = 1 << (k->actual_key_parts - j - 1);
        }
        k->rec_per_key[j] = x;
      }
    }
  }

  if (flag & HA_STATUS_ERRKEY) {
    /*
      Currently we support only primary keys so we know which key had a
      uniqueness violation.
    */
    errkey = m_dupp_errkey;
    dup_ref = m_pk_tuple; // TODO(?): this should store packed PK.
  }

  if (flag & HA_STATUS_AUTO) {
    stats.auto_increment_value = m_tbl_def->m_auto_incr_val;
  }

  /*
    optimizer will make some decisions depending on stats.records.
    0 may lead to unexpected results, for example, semi-join strategy is
    materialized with stats.records being 0 while firstmatch with stats.records
    being 1.
  */

  if (stats.records == 0 && !(flag & HA_STATUS_TIME) &&
        table_share->table_category != TABLE_CATEGORY_TEMPORARY) {
    stats.records = 1;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::extra(enum ha_extra_function operation) 
{
  DBUG_ENTER_FUNC();

  switch (operation) {
  case HA_EXTRA_KEYREAD:
    m_keyread_only = true;
    break;
  case HA_EXTRA_NO_KEYREAD:
    m_keyread_only = false;
    break;
  case HA_EXTRA_FLUSH:
    /*
      If the table has blobs, then they are part of m_retrieved_record.
      This call invalidates them.
    */
    m_retrieved_record.clear();
    break;
  default:
    break;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_smartengine::reset()
{
  DBUG_ENTER_FUNC();

  /* Free blob data */
  m_retrieved_record.clear();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

double ha_smartengine::scan_time()
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(static_cast<double>((stats.records + stats.deleted) / 20.0 + 10));
}

double ha_smartengine::read_time(uint index, uint ranges, ha_rows rows)
{
  DBUG_ENTER_FUNC();

  if (index != table->s->primary_key) {
    /* Non covering index range scan */
    DBUG_RETURN(handler::read_time(index, ranges, rows));
  }

  DBUG_RETURN((rows / 20.0) + 1);
}

/**Given a starting key and an ending key, estimate the number of rows that
will exist between the two keys.*/
ha_rows ha_smartengine::records_in_range(uint inx,
                                         key_range *const min_key,
                                         key_range *const max_key)
{
  DBUG_ENTER_FUNC();

  ha_rows ret = 0; // THDVAR(ha_thd(), records_in_range);
  if (ret) {
    DBUG_RETURN(ret);
  }
  if (table->force_index) {
    const ha_rows force_rows = 0/*THDVAR(ha_thd(), force_index_records_in_range)*/;
    if (force_rows) {
      DBUG_RETURN(force_rows);
    }
  }

  const SeKeyDef &kd = *m_key_descr_arr[inx];

  uint size1 = 0;
  if (min_key) {
    size1 = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                min_key->key, min_key->keypart_map);
    if (min_key->flag == HA_READ_PREFIX_LAST_OR_PREV ||
        min_key->flag == HA_READ_PREFIX_LAST ||
        min_key->flag == HA_READ_AFTER_KEY) {
      kd.successor(m_sk_packed_tuple, size1);
    }
  } else {
    kd.get_infimum_key(m_sk_packed_tuple, &size1);
  }

  uint size2 = 0;
  if (max_key) {
    size2 = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple_old,
                                max_key->key, max_key->keypart_map);
    if (max_key->flag == HA_READ_PREFIX_LAST_OR_PREV ||
        max_key->flag == HA_READ_PREFIX_LAST ||
        max_key->flag == HA_READ_AFTER_KEY) {
      kd.successor(m_sk_packed_tuple_old, size2);
    }
    // pad the upper key with FFFFs to make sure it is more than the lower
    if (size1 > size2) {
      memset(m_sk_packed_tuple_old + size2, 0xff, size1 - size2);
      size2 = size1;
    }
  } else {
    kd.get_supremum_key(m_sk_packed_tuple_old, &size2);
  }

  const common::Slice slice1((const char *)m_sk_packed_tuple, size1);
  const common::Slice slice2((const char *)m_sk_packed_tuple_old, size2);

  // slice1 >= slice2 means no row will match
  if (slice1.compare(slice2) >= 0) {
    DBUG_RETURN(HA_EXIT_SUCCESS);
  }

  db::Range r(kd.m_is_reverse_cf ? slice2 : slice1,
                   kd.m_is_reverse_cf ? slice1 : slice2);

  uint64_t sz = 0;
  auto disk_size = kd.m_stats.m_actual_disk_size;
  if (disk_size == 0)
    disk_size = kd.m_stats.m_data_size;
  auto rows = kd.m_stats.m_rows;
  if (rows == 0 || disk_size == 0) {
    rows = 1;
    disk_size = SE_ASSUMED_KEY_VALUE_DISK_SIZE;
  }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  // Getting statistics, including from Memtables
  uint8_t include_flags = db::DB::INCLUDE_FILES;
  se_db->GetApproximateSizes(kd.get_cf(), &r, 1, &sz, include_flags);
  ret = rows * sz / disk_size;
  uint64_t memTableCount;
  se_db->GetApproximateMemTableStats(kd.get_cf(), r, &memTableCount, &sz);
  ret += memTableCount;

  /*
    GetApproximateSizes() gives estimates so ret might exceed stats.records.
    MySQL then decides to use full index scan rather than range scan, which
    is not efficient for most cases.
    To prevent this, changing estimated records slightly smaller than
    stats.records.
  */
  if (ret >= stats.records) {
    ret = stats.records * 0.99;
 
  }

  if (se_debug_optimizer_n_rows > 0) {
    ret = se_debug_optimizer_n_rows;
  } else if (ret == 0) {
    ret = 1;
  }

  DBUG_RETURN(ret);
}

ha_rows ha_smartengine::estimate_rows_upper_bound()
{
  DBUG_ENTER_FUNC();

  DBUG_RETURN(HA_POS_ERROR);
}

/**
  Returns the exact number of records, use primary index
*/
int ha_smartengine::records(ha_rows *num_rows)
{
  int ret = HA_EXIT_SUCCESS;

  uint index = pk_index(table, m_tbl_def.get());

  //size_t max_threads = THDVAR(ha_thd(), parallel_read_threads);
  size_t max_threads = se_thd_parallel_read_threads(ha_thd());
  if (max_threads == 1) {
    return handler::records(num_rows); 
  }

  return records_from_index(num_rows, index);
}

/**Returns the exact number of records, use chosen key*/
int ha_smartengine::records_from_index(ha_rows *num_rows, uint index)
{
  int ret = HA_EXIT_SUCCESS;

  uint size_start = 0;
  uint size_end = 0;

  std::shared_ptr<SeKeyDef> kd = m_key_descr_arr[index];
  kd->get_infimum_key(m_sk_packed_tuple, &size_start);
  kd->get_supremum_key(m_sk_packed_tuple_old, &size_end);

  const common::Slice user_key_start((const char *)m_sk_packed_tuple, size_start);
  const common::Slice user_key_end((const char *)m_sk_packed_tuple_old, size_end);

  size_t max_threads = se_thd_parallel_read_threads(ha_thd());
  if (max_threads == 1) {
    return records_from_index_single(num_rows, index); 
  }
  DBUG_EXECUTE_IF("parallel_scan_kill", table->in_use->killed = THD::KILL_QUERY;);
  common::ParallelReader preader(max_threads, table->in_use);

  //generate range with internal_key [start,end)
  storage::Range range;
  preader.build_internal_key_range(user_key_start, user_key_end, range);

  //get snapshot if necessary
  SeTransaction *const tx = get_or_create_tx(table->in_use);
  tx->acquire_snapshot(true);
  common::ReadOptions read_options = tx->get_read_opts();
  read_options.total_order_seek = true;

  db::ColumnFamilyHandle *column_family_handle = kd->get_cf();  
  common::ParallelReader::Config config(column_family_handle, range, read_options);
 
  const auto tx_impl = dynamic_cast<const SeTransactionImpl *>(tx);
  assert(tx_impl);

  Counter::Shards n_recs;
  Counter::clear(n_recs);

  util::Transaction *trx =
      const_cast<util::Transaction *>(tx_impl->get_se_trx());

  /* Check for transaction interrupted every 1000 rows. */
  size_t counter = 1000;

  THD *current_thd = my_core::thd_get_current_thd();
  ret = preader.add_scan(
      trx, config, [&](const common::ParallelReader::ExecuteCtx *ctx, db::Iterator* it) {
        int ret = common::Status::kOk;

        /* Only check the THD state for the first thread. */
        if (ctx->thread_id_ == 0) {
          --counter;
          if (counter == 0 && my_core::thd_killed(current_thd)) {
            __HANDLER_LOG(WARN, "query interrupted by user, %s", current_thd->query().str);
            return (static_cast<int>(common::Status::kCancelTask));
          }

          if (counter == 0) {
            counter = 1000;
          }
        }

        Counter::inc(n_recs, ctx->thread_id_);

        return ret;
      });

  if (ret) {
    HANDLER_LOG(WARN, "prepare for parallel scan failed", K(ret));
  } else if ((ret = preader.run())) {
    HANDLER_LOG(WARN, "do parallel scan failed", K(ret));
  } else {
    // get total count
    Counter::for_each(n_recs, [=](const Counter::Type n) {
      if (n > 0) {
        *num_rows += n;
      }
    });
  }

  //convert se err_code to mysql err_code
  if (ret == static_cast<int>(common::Status::kCancelTask)) {
    ret = HA_ERR_QUERY_INTERRUPTED; 
  }

  return ret;
}

/**
  @brief
  Doing manual compaction on OPTIMIZE TABLE in se.
  Compaction itself is executed by background thread in se, but
  CompactRange() waits until compaction completes so this function
  may take a long time.
  Since se dataset is allocated per index id, OPTIMIZE TABLE
  triggers manual compaction for all indexes of the table.
  @details
  Compaction range is from the beginning of the index id to
  the first row of the next index id. When using reverse order
  column family, the first row of the next index id should be
  the last row of the previous index id.
*/
int ha_smartengine::optimize(THD *const thd, HA_CHECK_OPT *const check_opt)
{
  DBUG_ENTER_FUNC();

  assert(thd != nullptr);
  assert(check_opt != nullptr);

  int rc = 0;
  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    se_db->Flush(common::FlushOptions(), m_key_descr_arr[i]->get_cf());
    if (!se_db->CompactRange(m_key_descr_arr[i]->get_cf(),
                             db::TaskType::MANUAL_FULL_AMOUNT_TASK).ok()) {
      rc = 1;
      break;
    }
  }

  DBUG_RETURN(rc);
}

int ha_smartengine::analyze(THD *const thd, HA_CHECK_OPT *const check_opt)
{
  DBUG_ENTER_FUNC();

  if (!table) {
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  DBUG_RETURN(calculate_stats(table, thd, check_opt));
}

int ha_smartengine::check(THD *const thd, HA_CHECK_OPT *const check_opt)
{
  //if (THDVAR(ha_thd(), parallel_read_threads) > 1)
  if (se_thd_parallel_read_threads(ha_thd()) > 1)
    return check_parallel(thd, check_opt);
  sql_print_information("--------- CHECK TABLE START ----------");
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_CHECK);
  assert(thd != nullptr);
  assert(check_opt != nullptr);

  const uint pk = pk_index(table, m_tbl_def.get());
  uint32_t record_size = table->s->reclength;
  std::unique_ptr<uchar[]> key_record {new uchar[record_size]};

  common::Slice key;
  String rowkey_copy;
  String sec_key_copy;
  const char *const table_name = table->s->table_name.str;
  const char *const db_name = table->s->db.str;
  bool error = false;

  // Only when debugging: don't use snapshot when reading
  // SeTransaction *tx= get_or_create_tx(table->in_use);
  // tx->snapshot= nullptr;

  const bool save_verify_row_debug_checksums = m_verify_row_debug_checksums;
  m_verify_row_debug_checksums = true;

  for (uint i = 0; i < table->s->fields; i++) {
    bitmap_set_bit(table->read_set, i);
  }

  /* For each secondary index, check that we can get a PK value from it */
  // NO_LINT_DEBUG
  sql_print_information(" CHECK TABLE %s.%s: Checking table %s",
                        db_name, table_name, table_name);
  ha_rows row_checksums_at_start = ha_rows(-1); // set/used iff first_index==true
  ha_rows row_checksums = ha_rows(-1);
  bool first_index = true;

  SeTransaction *const tx = get_or_create_tx(table->in_use);
  assert(tx != nullptr);
  tx->acquire_snapshot(true);

  for (uint iter_keyno = 0; iter_keyno < table->s->keys; iter_keyno++) {
    SeKeyDef *iter_keydef = m_key_descr_arr[iter_keyno].get();
    extra(HA_EXTRA_KEYREAD);
    ha_index_init(iter_keyno, true);
    ha_rows rows = 0;
    ha_rows checksums = 0;
    if (first_index)
      row_checksums_at_start = m_row_checksums_checked;
    int res = 0;
    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   Checking index %s", db_name,
                          table_name, table->key_info[iter_keyno].name);
#ifndef NDEBUG
    while (1) {
      if (!rows)
        res = index_first(table->record[0]);
      else
        res = index_next(table->record[0]);
      if (res == HA_ERR_END_OF_FILE) break;
      ++rows;
    }
#endif

    String last_key_copy; //used to check whether unique attribute is ok
    rows = 0;
    while (1) {
      if (!rows)
        res = index_first(table->record[0]);
      else
        res = index_next(table->record[0]);

      if (res == HA_ERR_END_OF_FILE)
        break;
      if (res) {
        // error
        // NO_LINT_DEBUG
        sql_print_error("CHECK TABLE %s.%s:   .. row %lld: index scan error %d",
                        db_name, table_name, rows, res);
        error = true;
        assert(0);
        goto one_row_checked;
      }
      key = m_scan_it->key();
      sec_key_copy.copy(key.data(), key.size(), &my_charset_bin);
      rowkey_copy.copy(m_last_rowkey.ptr(), m_last_rowkey.length(),
                       &my_charset_bin);

      if (!is_pk(iter_keyno, table, m_tbl_def.get())) {
        // Here, we have m_last_rowkey which should be unpack_record from
        // iterator key to table->record[0]. So the sk + pk parts in
        // table->record[0] is valid.
        memcpy(key_record.get(), table->record[0], record_size);

        if (iter_keydef->unpack_info_has_checksum(m_scan_it->value())) {
          checksums++;
        }

        // Step 1:
        // Make a se Get using rowkey_copy and put the mysql format
        // result in table->record[0]
        longlong hidden_pk_id = 0;
        if (has_hidden_pk(table) &&
            read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey)) {
          error = true;
          assert(0);
          goto one_row_checked;
        }

        if ((res = get_row_by_rowid(table->record[0], rowkey_copy.ptr(),
                                    rowkey_copy.length()))) {
          // NO_LINT_DEBUG
          sql_print_error("CHECK TABLE %s.%s:   .. row %lld: "
                          "iter sk and get from pk, get_row_by_rowid failed",
                          db_name, table_name, rows);
          advice_del_index(table, iter_keyno, hidden_pk_id, key_record.get());
          error = true;
          assert(0);
          goto one_row_checked;
        }

        // Step 2:
        // Convert the pk parts in table->record[0] to storage format then
        // compare it with rowkey_copy. table->record[0] is from pk subtable
        // get while the rowkey_copy is from sk subtable iteration.
        uint packed_size = m_pk_descr->pack_record(
            table, m_pack_buffer, table->record[0], m_pk_packed_tuple, nullptr,
            false, hidden_pk_id);
        assert(packed_size <= m_max_packed_sk_len);

        if (packed_size != rowkey_copy.length() ||
            memcmp(m_pk_packed_tuple, rowkey_copy.ptr(), packed_size)) {
          // NO_LINT_DEBUG
          sql_print_error("CHECK TABLE %s.%s:   .. row %lld: "
                          "iter sk and get from pk, "
                          "pk decoded from m_scan_it and get_row_by_rowid mismatch",
                          db_name, table_name, rows);
          print_error_row(db_name, table_name, rowkey_copy, sec_key_copy,
                          m_retrieved_record);
          advice_del_index(table, iter_keyno, hidden_pk_id, key_record.get());
          error = true;
          assert(0);
          goto one_row_checked;
        }

        // Step 3:
        // Convert the sk[iter_keyno], pk parts in table->record[0] to storage
        // format then compare it with sec_key_copy. table->record[0] is from pk
        // subtable get while the sec_key_copy is from sk subtable iteration.
        packed_size = iter_keydef->pack_record(
            table, m_pack_buffer, table->record[0], m_sk_packed_tuple,
            &m_sk_tails, false, hidden_pk_id);
        assert(packed_size <= m_max_packed_sk_len);

        if (packed_size != sec_key_copy.length() ||
            memcmp(m_sk_packed_tuple, sec_key_copy.ptr(), packed_size)) {
          // NO_LINT_DEBUG
          sql_print_error("CHECK TABLE %s.%s:   .. row %lld: "
                          "iter sk and get from pk, "
                          "sk from m_scan_it and sk decoded from get_row_by_rowid mismatch",
                          db_name, table_name, rows);
          print_error_row(db_name, table_name, rowkey_copy, sec_key_copy,
                          m_retrieved_record);
          advice_del_index(table, iter_keyno, hidden_pk_id, key_record.get());
          error = true;
          assert(0);
          goto one_row_checked;
        }

        // Step 4:
        // if index is unique index, we should check whether unique attribute is ok
        if (table->key_info[iter_keyno].flags & HA_NOSAME) {
          uint n_null_fields = 0;
          uint current_sk_size = iter_keydef->get_memcmp_sk_size(table, key, &n_null_fields);
          if (!last_key_copy.is_empty() && n_null_fields == 0) {
            if (last_key_copy.length() == current_sk_size &&
                memcmp(last_key_copy.ptr(), sec_key_copy.ptr(),
                       current_sk_size) == 0) {
              sql_print_error("CHECK TABLE %s.%s:  unique check failed",
                              db_name, table_name);
              error = true;
              assert(0);
              goto one_row_checked;
            }
          }

          //if no-null fields, there may be duplicated key error
          if (n_null_fields == 0) {
            last_key_copy.copy(sec_key_copy.ptr(), current_sk_size, &my_charset_bin);
          }
        }
      } else {
        longlong hidden_pk_id = 0;
        if (has_hidden_pk(table) &&
            read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey)) {
          error = true;
          assert(0);
          goto one_row_checked;
        }

        //for pk-index and not hidden-pk, we check every key is different or not
        if (!has_hidden_pk(table)) {
          if (!last_key_copy.is_empty()) {
            if (last_key_copy.length() == rowkey_copy.length() &&
                memcmp(last_key_copy.ptr(), rowkey_copy.ptr(),
                       last_key_copy.length()) == 0) {
              sql_print_error("CHECK TABLE %s.%s:  unique check failed",
                              db_name, table_name);
              error = true;
              assert(0);
              goto one_row_checked;
            }
          }
          last_key_copy.copy(rowkey_copy.ptr(), rowkey_copy.length(), &my_charset_bin);
        }

        for (uint keyno = 0; keyno < table->s->keys; keyno++) {
          if (is_pk(keyno, table, m_tbl_def.get())) {
            // Assume the iterator and get would be consistency. Do not do a pk
            // get again.
            continue;
          }
          SeKeyDef *keydef = m_key_descr_arr[keyno].get();
          uint packed_size = keydef->pack_record(
            table, m_pack_buffer, table->record[0], m_sk_packed_tuple,
            &m_sk_tails, false, hidden_pk_id);

          std::string sk_value_str;
          common::Status s = tx->get(keydef->get_cf(),
                common::Slice(reinterpret_cast<char*>(m_sk_packed_tuple),
                packed_size), &sk_value_str);

          // The secondary key kv does not exist.
          if (!s.ok()) {
            sql_print_error("CHECK TABLE %s.%s:   .. row %lld: "
                            "iter pk and get from sk, "
                            "sk encoded from pk value does not exist",
                            db_name, table_name, rows);
            print_error_row(db_name, table_name, rowkey_copy, sec_key_copy,
                            m_scan_it->value().ToString());
            advice_add_index(table, keyno, hidden_pk_id, table->record[0]);
            error = true;
            assert(0);
            goto one_row_checked;
          }
          // The key of a fast check uk exists, but the value is another pk.
#if 0     // TODO: need port fast uk.
          if (keydef->is_support_fast_unique_check()) {
            uint32_t pk_size = 0;
            if (get_pkvalue_from_fast_uk(sk_value_slice, pk_size)) {
              HANDLER_LOG(ERROR, "get pkvalue from fast_uk error!");
              advice_add_index(table, keyno, hidden_pk_id, table->record[0]);
              error = true;
              goto one_row_checked;
            }
            if (rowkey_copy.length() != pk_size ||
                memcmp(rowkey_copy.ptr(), m_pk_packed_tuple, pk_size)) {
              advice_add_index(table, keyno, hidden_pk_id, table->record[0]);
              error = true;
              goto one_row_checked;
            }
          }
#endif
        }
      }
one_row_checked:
      ++rows;
      DBUG_EXECUTE_IF("serial_check_kill_1", table->in_use->killed = THD::KILL_QUERY;);
      if (my_core::thd_killed(current_thd)) {
        break;
      }
    }
    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   ... %lld index entries checked "
                          "(%lld had checksums)",
                          db_name, table_name, rows, checksums);

    if (first_index) {
      row_checksums = m_row_checksums_checked - row_checksums_at_start;
      first_index = false;
    }
    ha_index_end();
    DBUG_EXECUTE_IF("serial_check_kill_2", table->in_use->killed = THD::KILL_QUERY;);
    if (my_core::thd_killed(current_thd)) {
      break;
    }
  }

  if (row_checksums != ha_rows(-1)) {
    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   %lld "
                          "table records had checksums",
                          db_name, table_name, row_checksums);
  }
  extra(HA_EXTRA_NO_KEYREAD);

  m_verify_row_debug_checksums = save_verify_row_debug_checksums;

  if (my_core::thd_killed(current_thd)) {
    sql_print_information("--------- CHECK TABLE KILLED ----------");
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  if (error) {
    m_verify_row_debug_checksums = save_verify_row_debug_checksums;
    ha_index_or_rnd_end();
    extra(HA_EXTRA_NO_KEYREAD);
    sql_print_information("--------- CHECK TABLE END ----------");
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  sql_print_information("--------- CHECK TABLE END ----------");
  DBUG_RETURN(HA_ADMIN_OK);
}

/*
  @brief
    Setup which fields will be unpacked when reading rows

  @detail
    Two special cases when we still unpack all fields:
    - When this table is being updated (m_lock_rows==SE_LOCK_WRITE).
    - When @@se_verify_row_debug_checksums is ON (In this mode, we need to
  read all
      fields to find whether there is a row checksum at the end. We could skip
      the fields instead of decoding them, but currently we do decoding.)

  @seealso
    ha_smartengine::setup_field_converters()
    ha_smartengine::convert_record_from_storage_format()
*/

int ha_smartengine::setup_read_decoders()
{
  // bitmap is cleared on index merge, but it still needs to decode columns
  bool force_decode_all_fields =
      (m_lock_rows == SE_LOCK_WRITE || m_verify_row_debug_checksums || bitmap_is_clear_all(table->read_set));

  return setup_read_decoders(this->table, m_encoder_arr, m_instant_ddl_info, force_decode_all_fields, m_decoders_vect);
}

/*
  setup information for decode se record, if force_decode_all_fields is set,
  all fields need to be decoded.
*/
int ha_smartengine::setup_read_decoders(const TABLE *table,
                                        SeFieldEncoder *encoder_arr,
                                        InstantDDLInfo &instant_ddl_info,
                                        bool force_decode_all_fields,
                                        std::vector<READ_FIELD> &decoders_vect)
{
  decoders_vect.clear();

  int last_useful = 0;
  int skip_size = 0;

  for (uint i = 0; i < table->s->fields; i++) {
    // We only need the decoder if the whole record is stored.
    if (encoder_arr[i].m_storage_type != SeFieldEncoder::STORE_ALL) {
      continue;
    }

    if (force_decode_all_fields ||
        bitmap_is_set(table->read_set, table->field[i]->field_index())) {
      // We will need to decode this field
      decoders_vect.push_back({&encoder_arr[i], true, skip_size});
      last_useful = decoders_vect.size();
      skip_size = 0;
    } else {
      if (instant_ddl_info.have_instantly_added_columns() ||
          encoder_arr[i].uses_variable_len_encoding() ||
          encoder_arr[i].maybe_null()) {
        // For variable-length field, we need to read the data and skip it
        // Or if this table hash instantly added column, we must record every field's info here
        decoders_vect.push_back({&encoder_arr[i], false, skip_size});
        skip_size = 0;
      } else {
        // Fixed-width field can be skipped without looking at it.
        // Add appropriate skip_size to the next field.
        skip_size += encoder_arr[i].m_pack_length_in_rec;
      }
    }
  }

  // It could be that the last few elements are varchars that just do
  // skipping. Remove them.
  if (!instant_ddl_info.have_instantly_added_columns()) {
    decoders_vect.erase(decoders_vect.begin() + last_useful,
                        decoders_vect.end());
  }
  return HA_EXIT_SUCCESS;
}

int ha_smartengine::convert_record_from_storage_format(
    const common::Slice *key,
    std::string& retrieved_record,
    uchar *const buf, TABLE* tbl)
{
  DBUG_EXECUTE_IF("myx_simulate_bad_row_read1",
                  dbug_append_garbage_at_end(retrieved_record););
  DBUG_EXECUTE_IF("myx_simulate_bad_row_read2",
                  dbug_truncate_record(retrieved_record););
  DBUG_EXECUTE_IF("myx_simulate_bad_row_read3",
                  dbug_modify_rec_varchar12(retrieved_record););

  const common::Slice retrieved_rec_slice(&retrieved_record.front(),
                                                   retrieved_record.size());
  return convert_record_from_storage_format(key, &retrieved_rec_slice, buf, tbl);
}

int ha_smartengine::convert_record_from_storage_format(
    const common::Slice *key,
    const common::Slice *value,
    uchar *const buf,
    TABLE* tbl)
{
  return convert_record_from_storage_format(
      key,
      value,
      buf,
      tbl,
      m_null_bytes_in_rec,
      m_maybe_unpack_info,
      m_pk_descr.get(),
      m_decoders_vect,
      &m_instant_ddl_info,
      m_verify_row_debug_checksums,
      m_row_checksums_checked);
}

int ha_smartengine::convert_blob_from_storage_format(
  my_core::Field_blob *const blob,
  SeStringReader *const reader,
  bool decode)
{
  /* Get the number of bytes needed to store length*/
  const uint length_bytes = blob->pack_length() - portable_sizeof_char_ptr;

  const char *data_len_str;
  if (!(data_len_str = reader->read(length_bytes))) {
    return HA_ERR_INTERNAL_ERROR;
  }

  memcpy(blob->field_ptr(), data_len_str, length_bytes);

  const uint32 data_len = blob->get_length(
      reinterpret_cast<const uchar*>(data_len_str), length_bytes);
      //reinterpret_cast<const uchar*>(data_len_str), length_bytes, table->s->db_low_byte_first);
  const char *blob_ptr;
  if (!(blob_ptr = reader->read(data_len))) {
    return HA_ERR_INTERNAL_ERROR;
  }

  if (decode) {
    // set 8-byte pointer to 0, like innodb does (relevant for 32-bit
    // platforms)
    memset(blob->field_ptr() + length_bytes, 0, 8);
    memcpy(blob->field_ptr() + length_bytes, &blob_ptr, sizeof(uchar **));
  }

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::convert_varchar_from_storage_format(
    my_core::Field_varstring *const field_var,
    SeStringReader *const reader,
    bool decode)
{
  const char *data_len_str;
  if (!(data_len_str = reader->read(field_var->get_length_bytes())))
    return HA_ERR_INTERNAL_ERROR;

  uint data_len;
  /* field_var->length_bytes is 1 or 2 */
  if (field_var->get_length_bytes() == 1) {
    data_len = (uchar)data_len_str[0];
  } else {
    assert(field_var->get_length_bytes() == 2);
    data_len = uint2korr(data_len_str);
  }

  if (data_len > field_var->field_length) {
    /* The data on disk is longer than table DDL allows? */
    return HA_ERR_INTERNAL_ERROR;
  }

  if (!reader->read(data_len)) {
    return HA_ERR_INTERNAL_ERROR;
  }

  if (decode) {
    memcpy(field_var->field_ptr(), data_len_str, field_var->get_length_bytes() + data_len);
  }

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::convert_string_from_storage_format(
    my_core::Field_string *const field_str,
    SeStringReader *const reader,
    bool decode)
{
  int ret = HA_EXIT_SUCCESS;
  uint16 pack_length = field_str->pack_length_in_rec();
  uint16 val_length = 0;
  const char *val_length_buf = nullptr;
  const char *val = nullptr;

  assert(pack_length >= 0);
  if (0 == pack_length) {
    // For string with zero length, nothing need to read.
  } else if (UNLIKELY(field_str->charset()->mbminlen > 1)) {
    // The storage format for the MYSQL_TYPE_STRING type varies depending
    // on whether the character set is compatible with ASCII.For details,
    // you can refer to the function "append_string_to_storage_format".
    if (IS_NULL(val = reader->read(pack_length))) {
      ret = HA_ERR_INTERNAL_ERROR;
      SE_LOG(ERROR, "fail to read string val", K(ret), K(pack_length));
    } else {
      if (decode) {
        memcpy(field_str->field_ptr(), val, pack_length);
      }
    }
  } else {
    if (IS_NULL(val_length_buf = reader->read(schema::RecordFormat::MYSQL_STRING_SIZE_BYTES))) {
      ret = HA_ERR_INTERNAL_ERROR;
      SE_LOG(ERROR, "fail to read string length buf", K(ret));
    } else if (UNLIKELY(pack_length < (val_length = uint2korr(val_length_buf)))) {
      ret = HA_ERR_INTERNAL_ERROR;
      SE_LOG(ERROR, "fail to parse value length in storage format", K(ret), K(pack_length), K(val_length));
    } else if (IS_NULL(val = reader->read(val_length))) {
      ret = HA_ERR_INTERNAL_ERROR;
      SE_LOG(ERROR, "fail to read string value", K(ret), K(val_length));
    } else {
      if (decode) {
        memcpy(field_str->field_ptr(), val, val_length);
        if (pack_length > val_length) {
          memset(field_str->field_ptr() + val_length, 0x20, pack_length - val_length);
        }
      }
    }
  }

  return ret;
}

int ha_smartengine::convert_field_from_storage_format(
    my_core::Field *const    field,
    SeStringReader *const reader,
    bool decode,
    uint len)
{
  const char *data_bytes;
  if (len > 0) {
    if ((data_bytes = reader->read(len)) == nullptr) {
      return HA_ERR_INTERNAL_ERROR;
    }

    if (decode)
      memcpy(field->field_ptr(), data_bytes, len);
  }

  return HA_EXIT_SUCCESS;
}

/*
  @brief
  Unpack the record in this->m_retrieved_record and this->m_last_rowkey from
  storage format into buf (which can be table->record[0] or table->record[1]).

  @param  key   Table record's key in mem-comparable form.
  @param  buf   Store record in table->record[0] format here

  @detail
    If the table has blobs, the unpacked data in buf may keep pointers to the
    data in this->m_retrieved_record.

    The key is only needed to check its checksum value (the checksum is in
    m_retrieved_record).

  @seealso
    ha_smartengine::setup_read_decoders()  Sets up data structures which tell which
    columns to decode.

  @return
    0      OK
    other  Error inpacking the data
*/

int ha_smartengine::convert_record_from_storage_format(
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
    my_core::ha_rows& row_checksums_checked)
{
  assert(key != nullptr);
  assert(buf != nullptr);
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_CONVERT_FROM);

  SeStringReader reader(value);

  // Used for DEBUG EXECUTION
  String debug_key;
  common::Slice debug_key_slice;
  /*
    Decode PK fields from the key
  */
  DBUG_EXECUTE_IF("myx_simulate_bad_pk_read1",{
    debug_key.copy(key->data(), key->size(), &my_charset_bin);
    dbug_modify_key_varchar8(debug_key);
    debug_key_slice = common::Slice (debug_key.ptr(), debug_key.length());
    key = &debug_key_slice;
  });

  const char *unpack_info = nullptr;
  uint16 unpack_info_len = 0;
  common::Slice unpack_slice;

  const char *header = reader.read(schema::RecordFormat::RECORD_HEADER_SIZE);
  // if column_number_need_supplement <= 0 , it means no instantly added columns should be extracted
  int column_number_need_supplement = 0;
  int offset_for_default_vect = 0;
  if (instant_ddl_info && instant_ddl_info->have_instantly_added_columns()) {
    // number of fields stored in a smartengine row
    uint16_t field_num = 0;
    if (header[0] & schema::RecordFormat::RECORD_INSTANT_DDL_FLAG) {
      const char *field_num_str = reader.read(schema::RecordFormat::RECORD_FIELD_NUMBER_SIZE);
      const char *nullable_bytes_str = reader.read(schema::RecordFormat::RECORD_NULL_BITMAP_SIZE_BYTES);
      field_num = uint2korr(field_num_str);
      column_number_need_supplement = decoders_vect.size() - (field_num - m_fields_no_needed_to_decode);
      null_bytes_in_rec = uint2korr(nullable_bytes_str);
    } else {
      // inserted before instant ddl
      field_num = instant_ddl_info->m_instant_cols;
      column_number_need_supplement = decoders_vect.size() - (field_num - m_fields_no_needed_to_decode);
      null_bytes_in_rec = instant_ddl_info->m_null_bytes;
    }
    // the first element should be get from default value array
    offset_for_default_vect = field_num - instant_ddl_info->m_instant_cols;
    assert(offset_for_default_vect >= 0 &&
           offset_for_default_vect <= (int)(instant_ddl_info->instantly_added_default_values.size()));
    assert(null_bytes_in_rec != std::numeric_limits<uint32_t>::max());
  }

  /* Other fields are decoded from the value */
  const char *null_bytes = nullptr;
  if (null_bytes_in_rec && !(null_bytes = reader.read(null_bytes_in_rec))) {
    return HA_ERR_INTERNAL_ERROR;
  }

  if (maybe_unpack_info) {
    unpack_info = reader.read(schema::RecordFormat::RECORD_UNPACK_HEADER_SIZE);

    if (!unpack_info || unpack_info[0] != schema::RecordFormat::RECORD_UNPACK_DATA_FLAG) {
      return HA_ERR_INTERNAL_ERROR;
    }

    unpack_info_len =
        se_netbuf_to_uint16(reinterpret_cast<const uchar *>(unpack_info + 1));
    unpack_slice = common::Slice(unpack_info, unpack_info_len);

    reader.read(unpack_info_len - schema::RecordFormat::RECORD_UNPACK_HEADER_SIZE);
  }

  if (!unpack_info && !pk_descr->table_has_unpack_info(t)) {
      if (pk_descr->unpack_record_1(t, buf, key, nullptr,
                                  false /* verify_checksum */)) {
          return HA_ERR_INTERNAL_ERROR;
      }
  } else if (pk_descr->unpack_record(t, buf, key,
                                unpack_info? &unpack_slice : nullptr,
                                false /* verify_checksum */)) {
      return HA_ERR_INTERNAL_ERROR;
  }

  int err = HA_EXIT_SUCCESS;
  int fields_have_been_extracted = 0;
  int extracted_normally = decoders_vect.size();
  if (column_number_need_supplement > 0) {
    extracted_normally = decoders_vect.size() - column_number_need_supplement;
  }

  for (auto it = decoders_vect.begin(); it != decoders_vect.end(); it++) {
    assert(extracted_normally >= 0);
    const SeFieldEncoder *const field_dec = it->m_field_enc;
    const bool decode = it->m_decode;

    Field *const field = t->field[field_dec->m_field_index];

    /* Skip the bytes we need to skip */
    if (it->m_skip && !reader.read(it->m_skip))
      return HA_ERR_INTERNAL_ERROR;

    uint field_offset = field->field_ptr() - t->record[0];
    uint null_offset = field->null_offset();
    bool maybe_null = field->is_nullable();
    field->move_field(buf + field_offset,
                      maybe_null ? buf + null_offset : nullptr,
                      field->null_bit);
    // WARNING! - Don't return before restoring field->ptr and field->null_ptr!

    if (fields_have_been_extracted < extracted_normally) {

      const bool isNull = field_dec->maybe_null() &&
          ((null_bytes[field_dec->m_null_offset] & field_dec->m_null_mask) != 0);

      if (isNull) {
        if (decode) {
          /* This sets the NULL-bit of this record */
          field->set_null();
          /*
            Besides that, set the field value to default value. CHECKSUM t
            depends on this.
          */
          memcpy(field->field_ptr(), t->s->default_values + field_offset,
                field->pack_length());
        }
      } else {
        if (decode) {
          field->set_notnull();
        }

        if (field_dec->m_field_type == MYSQL_TYPE_BLOB ||
            field_dec->m_field_type == MYSQL_TYPE_JSON) {
          err = convert_blob_from_storage_format(
              (my_core::Field_blob *) field, &reader, decode);
        } else if (field_dec->m_field_type == MYSQL_TYPE_VARCHAR) {
          err = convert_varchar_from_storage_format(
              (my_core::Field_varstring *) field, &reader, decode);
        } else if (field_dec->m_field_type == MYSQL_TYPE_STRING) {
          err = convert_string_from_storage_format((my_core::Field_string *)field, &reader, decode);
        } else {
          err = convert_field_from_storage_format(
              field, &reader, decode, field_dec->m_pack_length_in_rec);
        }
      }
    } else if (instant_ddl_info) {
      // instantly added columns
      const bool is_default_null = instant_ddl_info->instantly_added_default_value_null[offset_for_default_vect];
      if (decode) {
        if (is_default_null) {
          field->set_null();
        } else {
          field->set_notnull();
          const char *data_bytes = m_instant_ddl_info.instantly_added_default_values[offset_for_default_vect].c_str();
          int len = m_instant_ddl_info.instantly_added_default_values[offset_for_default_vect].size();
          memcpy(field->field_ptr(), data_bytes, len);
        }
      }
      offset_for_default_vect++;
    } else {
      __HANDLER_LOG(ERROR, "Decode value failed for unkown err");
      assert(0);
    }

    // Restore field->ptr and field->null_ptr
    field->move_field(t->record[0] + field_offset,
                      maybe_null ? t->record[0] + null_offset : nullptr,
                      field->null_bit);
    fields_have_been_extracted++;

    if (err != HA_EXIT_SUCCESS) {
      return err;
    }
  }

  // if decode ddl pk, pass checksums verification
  if (verify_row_debug_checksums) {
    if (reader.remaining_bytes() == SE_CHECKSUM_CHUNK_SIZE &&
        reader.read(1)[0] == SE_CHECKSUM_DATA_TAG) {
      uint32_t stored_key_chksum =
          se_netbuf_to_uint32((const uchar *)reader.read(SE_CHECKSUM_SIZE));
      uint32_t stored_val_chksum =
          se_netbuf_to_uint32((const uchar *)reader.read(SE_CHECKSUM_SIZE));

      const uint32_t computed_key_chksum =
          my_core::crc32(0, se_slice_to_uchar_ptr(key), key->size());
      const uint32_t computed_val_chksum =
          my_core::crc32(0, se_slice_to_uchar_ptr(value),
                         value->size() - SE_CHECKSUM_CHUNK_SIZE);

      DBUG_EXECUTE_IF("myx_simulate_bad_pk_checksum1",
                      stored_key_chksum++;);

      if (stored_key_chksum != computed_key_chksum) {
        pk_descr->report_checksum_mismatch(true, key->data(), key->size());
        return HA_ERR_INTERNAL_ERROR;
      }

      DBUG_EXECUTE_IF("myx_simulate_bad_pk_checksum2",
                      stored_val_chksum++;);
      if (stored_val_chksum != computed_val_chksum) {
        pk_descr->report_checksum_mismatch(false, value->data(),
                                             value->size());
        return HA_ERR_INTERNAL_ERROR;
      }

      row_checksums_checked++;
    }
    if (reader.remaining_bytes())
      return HA_ERR_INTERNAL_ERROR;
  }

  return HA_EXIT_SUCCESS;
}

/**
  @note
  This function is used only when the table has not yet been opened, and
  keyread_allowed bitmap doesn't have the correct values yet.

  See comment in ha_smartengine::index_flags() for details.
*/

bool ha_smartengine::check_keyread_allowed(uint inx,
                                           uint part,
                                           bool all_parts) const
{
  bool res = true;
  KEY *const key_info = &table_share->key_info[inx];

  SeFieldPacking dummy1;
  res = dummy1.setup(nullptr, key_info->key_part[part].field, inx, part,
                     key_info->key_part[part].length);

  if (res && all_parts) {
    for (uint i = 0; i < part; i++) {
      Field *field;
      if ((field = key_info->key_part[i].field)) {
        SeFieldPacking dummy;
        if (!dummy.setup(nullptr, field, inx, i,
                         key_info->key_part[i].length)) {
          /* Cannot do index-only reads for this column */
          res = false;
          break;
        }
      }
    }
  }

  const uint pk = table_share->primary_key;
  if (inx == pk && all_parts &&
      part + 1 == table_share->key_info[pk].user_defined_key_parts) {
    m_pk_can_be_decoded = res;
    // reset the cached_table_flags
    const_cast<ha_smartengine*>(this)->init();
  }

  return res;
}

int ha_smartengine::read_key_exact(const SeKeyDef &kd,
                                   db::Iterator *const iter,
                                   const bool &full_key_match,
                                   const common::Slice &key_slice) const
{
  assert(iter != nullptr);

  /*
    We are looking for the first record such that
      index_tuple= lookup_tuple.
    lookup_tuple may be a prefix of the index.
  */
  if (kd.m_is_reverse_cf) {
    if (!full_key_match) {
      if (!iter->Valid())
        iter->SeekToLast();
      else
        iter->Prev();
    }
  }

  if (!iter->Valid() || !kd.value_matches_prefix(iter->key(), key_slice)) {
    /*
      Got a record that is not equal to the lookup value, or even a record
      from another table.index.
    */
    return HA_ERR_KEY_NOT_FOUND;
  }
  return HA_EXIT_SUCCESS;
}

int ha_smartengine::read_before_key(const SeKeyDef &kd,
                                    const bool &full_key_match,
                                    const common::Slice &key_slice)
{
  /*
    We are looking for record with the biggest t.key such that
    t.key < lookup_tuple.
  */
  if (kd.m_is_reverse_cf) {
    if (m_scan_it->Valid() && full_key_match &&
        kd.value_matches_prefix(m_scan_it->key(), key_slice)) {
      /* We are using full key and we've hit an exact match */
      m_scan_it->Next();
    }
  } else {
    if (m_scan_it->Valid())
      m_scan_it->Prev();
    else
      m_scan_it->SeekToLast();
  }

  return m_scan_it->Valid() ? HA_EXIT_SUCCESS : HA_ERR_KEY_NOT_FOUND;
}

int ha_smartengine::read_after_key(const SeKeyDef &kd,
                                   const bool &full_key_match,
                                   const common::Slice &key_slice)
{
  /*
    We are looking for the first record such that

      index_tuple $GT lookup_tuple

    with HA_READ_AFTER_KEY, $GT = '>',
    with HA_READ_KEY_OR_NEXT, $GT = '>='
  */
  if (kd.m_is_reverse_cf) {
    if (!m_scan_it->Valid()) {
      m_scan_it->SeekToLast();
    } else {
      /*
        We should step back
         - when not using full extended key
         - when using full extended key and when we've got an exact match
      */
      if (!full_key_match ||
          !kd.value_matches_prefix(m_scan_it->key(), key_slice)) {
        m_scan_it->Prev();
      }
    }
  }

  return m_scan_it->Valid() ? HA_EXIT_SUCCESS : HA_ERR_KEY_NOT_FOUND;
}

int ha_smartengine::position_to_correct_key(const SeKeyDef &kd,
                                            enum ha_rkey_function &find_flag,
                                            bool &full_key_match,
                                            const uchar *key,
                                            key_part_map &keypart_map,
                                            common::Slice &key_slice,
                                            bool *move_forward)
{
  int rc = 0;

  *move_forward = true;

  switch (find_flag) {
  case HA_READ_KEY_EXACT:
    rc = read_key_exact(kd, m_scan_it, full_key_match, key_slice);
    break;
  case HA_READ_BEFORE_KEY:
    *move_forward = false;
    rc = read_before_key(kd, full_key_match, key_slice);
    if (rc == 0 && !kd.covers_key(m_scan_it->key())) {
      /* The record we've got is not from this index */
      rc = HA_ERR_KEY_NOT_FOUND;
    }
    break;
  case HA_READ_AFTER_KEY:
  case HA_READ_KEY_OR_NEXT:
    rc = read_after_key(kd, full_key_match, key_slice);
    if (rc == 0 && !kd.covers_key(m_scan_it->key())) {
      /* The record we've got is not from this index */
      rc = HA_ERR_KEY_NOT_FOUND;
    }
    break;
  case HA_READ_KEY_OR_PREV:
  case HA_READ_PREFIX:
    /* This flag is not used by the SQL layer, so we don't support it yet. */
    rc = HA_ERR_UNSUPPORTED;
    break;
  case HA_READ_PREFIX_LAST:
  case HA_READ_PREFIX_LAST_OR_PREV:
    *move_forward = false;
    /*
      Find the last record with the specified index prefix lookup.
      - HA_READ_PREFIX_LAST requires that the record has the
        prefix=lookup (if there are no such records,
        HA_ERR_KEY_NOT_FOUND should be returned).
      - HA_READ_PREFIX_LAST_OR_PREV has no such requirement. If there are no
        records with prefix=lookup, we should return the last record
        before that.
    */
    rc = read_before_key(kd, full_key_match, key_slice);
    if (rc == 0) {
      const common::Slice &rkey = m_scan_it->key();
      if (!kd.covers_key(rkey)) {
        /* The record we've got is not from this index */
        rc = HA_ERR_KEY_NOT_FOUND;
      } else if (find_flag == HA_READ_PREFIX_LAST) {
        uint size = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                        key, keypart_map);
        common::Slice lookup_tuple(reinterpret_cast<char *>(m_sk_packed_tuple),
                                    size);

        // We need to compare the key we've got with the original search prefix.
        if (!kd.value_matches_prefix(rkey, lookup_tuple)) {
          rc = HA_ERR_KEY_NOT_FOUND;
        }
      }
    }
    break;
  default:
    assert(0);
    break;
  }

  return rc;
}

int ha_smartengine::calc_eq_cond_len(const SeKeyDef &kd,
                                     enum ha_rkey_function &find_flag,
                                     common::Slice &slice,
                                     int &bytes_changed_by_succ,
                                     const key_range *end_key,
                                     uint *end_key_packed_size)
{
  if (find_flag == HA_READ_KEY_EXACT)
    return slice.size();

  if (find_flag == HA_READ_PREFIX_LAST) {
    /*
      We have made the kd.successor(m_sk_packed_tuple) call above.

      The slice is at least SeKeyDef::INDEX_NUMBER_SIZE bytes long.
    */
    return slice.size() - bytes_changed_by_succ;
  }

  if (end_key) {
    *end_key_packed_size =
        kd.pack_index_tuple(table, m_pack_buffer, m_end_key_packed_tuple,
                            end_key->key, (const_cast<key_range*>(end_key))->keypart_map);

    /*
      Calculating length of the equal conditions here. 4 byte index id is
      included.
      Example1: id1 BIGINT, id2 INT, id3 BIGINT, PRIMARY KEY (id1, id2, id3)
       WHERE id1=1 AND id2=1 AND id3>=2 => eq_cond_len= 4+8+4= 16
       WHERE id1=1 AND id2>=1 AND id3>=2 => eq_cond_len= 4+8= 12
      Example2: id1 VARCHAR(30), id2 INT, PRIMARY KEY (id1, id2)
       WHERE id1 = 'AAA' and id2 < 3; => eq_cond_len=13 (varchar used 9 bytes)
    */
    common::Slice end_slice(reinterpret_cast<char *>(m_end_key_packed_tuple),
                             *end_key_packed_size);
    return slice.difference_offset(end_slice);
  }

  /*
    On range scan without any end key condition, there is no
    eq cond, and eq cond length is the same as index_id size (4 bytes).
    Example1: id1 BIGINT, id2 INT, id3 BIGINT, PRIMARY KEY (id1, id2, id3)
     WHERE id1>=1 AND id2 >= 2 and id2 <= 5 => eq_cond_len= 4
  */
  return SeKeyDef::INDEX_NUMBER_SIZE;
}

int ha_smartengine::read_row_from_primary_key(uchar *const buf)
{
  assert(buf != nullptr);
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_READ_ROW);

  int rc;
  const common::Slice &rkey = m_scan_it->key();
  const uint pk_size = rkey.size();
  const char *pk_data = rkey.data();

  memcpy(m_pk_packed_tuple, pk_data, pk_size);
  m_last_rowkey.copy(pk_data, pk_size, &my_charset_bin);

  if (m_lock_rows != SE_LOCK_NONE) {
    /* We need to put a lock and re-read */
    rc = get_row_by_rowid(buf, m_pk_packed_tuple, pk_size);
  } else {
    /* Unpack from the row we've read */
    const common::Slice &value = m_scan_it->value();
    rc = convert_record_from_storage_format(&rkey, &value, buf, table);
  }

  return rc;
}

int ha_smartengine::read_row_from_secondary_key(uchar *const buf,
                                                const SeKeyDef &kd,
                                                bool move_forward)
{
  assert(buf != nullptr);
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_READ_ROW);
  int rc = 0;
  uint pk_size = 0;

  if (m_keyread_only && m_lock_rows == SE_LOCK_NONE && !has_hidden_pk(table)) {
    rc = find_icp_matching_index_rec(move_forward, buf);
    if (!rc) {
      /* Get the key columns and primary key value */
      const common::Slice &rkey = m_scan_it->key();
      pk_size =
          kd.get_primary_key_tuple(table, *m_pk_descr, &rkey, m_pk_packed_tuple);
      const common::Slice &value = m_scan_it->value();
      if (pk_size == SE_INVALID_KEY_LEN ||
          kd.unpack_record(table, buf, &rkey, &value,
                           m_verify_row_debug_checksums)) {
        rc = HA_ERR_INTERNAL_ERROR;
      }
    }
  } else {
    if (kd.m_is_reverse_cf)
      move_forward = !move_forward;

    rc = find_icp_matching_index_rec(move_forward, buf);
    if (!rc) {
      const common::Slice &rkey = m_scan_it->key();
      pk_size = kd.get_primary_key_tuple(table, *m_pk_descr, &rkey,
                                         m_pk_packed_tuple);
      if (pk_size == SE_INVALID_KEY_LEN) {
        rc = HA_ERR_INTERNAL_ERROR;
      } else {
        rc = get_row_by_rowid(buf, m_pk_packed_tuple, pk_size);
      }
    }
  }

  if (!rc) {
    m_last_rowkey.copy((const char *)m_pk_packed_tuple, pk_size,
                       &my_charset_bin);
  }

  return rc;
}

/**
  @brief
  Read next index tuple through the secondary index.

  @details
  m_scan_it points at the index key-value pair that we should read the (pk,row)
  pair for.
*/
int ha_smartengine::secondary_index_read(const int keyno, uchar *const buf)
{
  assert(buf != nullptr);
  assert(table != nullptr);
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_SECONDARY_INDEX_READ);
  //stats.rows_requested++;

  /* Use STATUS_NOT_FOUND when record not found or some error occurred */
  //table->status = STATUS_NOT_FOUND;

  if (m_scan_it->Valid()) {
    common::Slice key = m_scan_it->key();

    /* Check if we've ran out of records of this index */
    if (m_key_descr_arr[keyno]->covers_key(key)) {
      int rc = 0;

      // TODO: We could here check if we have ran out of range we're scanning
      const uint size = m_key_descr_arr[keyno]->get_primary_key_tuple(
          table, *m_pk_descr, &key, m_pk_packed_tuple);
      if (size == SE_INVALID_KEY_LEN) {
        return HA_ERR_INTERNAL_ERROR;
      }

      m_last_rowkey.copy((const char *)m_pk_packed_tuple, size,
                         &my_charset_bin);

      if (m_keyread_only && m_lock_rows == SE_LOCK_NONE &&
          !has_hidden_pk(table)) {
        common::Slice value = m_scan_it->value();
        if (m_key_descr_arr[keyno]->unpack_record(
                table, buf, &key, &value, m_verify_row_debug_checksums)) {
          return HA_ERR_INTERNAL_ERROR;
        }
      } else {
        rc = get_row_by_rowid(buf, m_pk_packed_tuple, size);
      }

      if (!rc) {
        //table->status = 0;
       // stats.rows_read++;
        //stats.rows_index_next++;
        update_row_stats(ROWS_READ);
      }
      return rc;
    }
  }
  return HA_ERR_END_OF_FILE;
}

/*
   See storage/smartengine/se-range-access.txt for description of how MySQL
   index navigation commands are converted into se lookup commands.

   This function takes end_key as an argument, and it is set on range scan.
   SE needs to decide whether prefix bloom filter can be used or not.
   To decide to use prefix bloom filter or not, calculating equal condition
   length
   is needed. On equal lookups (find_flag == HA_READ_KEY_EXACT), equal
   condition length is the same as common::Slice.size() of the start key.
   On range scan, equal condition length is MIN(start_key, end_key) of the
   common::Slice expression.
*/
int ha_smartengine::index_read_map_impl(uchar *const buf,
                                        const uchar *const key,
                                        key_part_map keypart_map,
                                        enum ha_rkey_function find_flag,
                                        const key_range *end_key)
{
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_INDEX_READ_MAP);
  int rc = 0;

  ha_statistic_increment(&SSV::ha_read_key_count);
  const SeKeyDef &kd = *m_key_descr_arr[active_index];
  const uint actual_key_parts = kd.get_key_parts();
  bool using_full_key = is_using_full_key(keypart_map, actual_key_parts);

  /* By default, we don't need the retrieved records to match the prefix */
  m_sk_match_prefix = nullptr;
  //stats.rows_requested++;

  if (active_index == table->s->primary_key && find_flag == HA_READ_KEY_EXACT &&
      using_full_key) {
    /*
      Equality lookup over primary key, using full tuple.
      This is a special case, use DB::Get.
    */
    const uint size = kd.pack_index_tuple(table, m_pack_buffer,
                                          m_pk_packed_tuple, key, keypart_map);
    bool skip_lookup = is_blind_delete_enabled();
    rc = get_row_by_rowid(buf, m_pk_packed_tuple, size, skip_lookup);
    if (!rc && !skip_lookup) {
      //stats.rows_read++;
      //stats.rows_index_first++;
      update_row_stats(ROWS_READ);
    }
    DBUG_RETURN(rc);
  }

  /*
    Unique secondary index performs lookups without the extended key fields
  */
  uint packed_size;
  if (active_index != table->s->primary_key &&
      table->key_info[active_index].flags & HA_NOSAME &&
      find_flag == HA_READ_KEY_EXACT && using_full_key) {
    key_part_map tmp_map = (key_part_map(1) << table->key_info[active_index]
                                                   .user_defined_key_parts) -
                           1;
    packed_size = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                      key, tmp_map);
    if (table->key_info[active_index].user_defined_key_parts !=
        kd.get_key_parts())
      using_full_key = false;
  } else {
    packed_size = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                      key, keypart_map);
  }

//  if ((pushed_idx_cond && pushed_idx_cond_keyno == active_index) &&
//      (find_flag == HA_READ_KEY_EXACT || find_flag == HA_READ_PREFIX_LAST)) 
  {
    /*
      We are doing a point index lookup, and ICP is enabled. It is possible
      that this call will be followed by ha_smartengine->index_next_same() call.

      Do what InnoDB does: save the lookup tuple now. We will need it in
      index_next_same/find_icp_matching_index_rec in order to stop scanning
      as soon as index record doesn't match the lookup tuple.

      When not using ICP, handler::index_next_same() will make sure that rows
      that don't match the lookup prefix are not returned.
      row matches the lookup prefix.
    */
    m_sk_match_prefix = m_sk_match_prefix_buf;
    m_sk_match_length = packed_size;
    memcpy(m_sk_match_prefix, m_sk_packed_tuple, packed_size);
  }

  int bytes_changed_by_succ = 0;
  if (find_flag == HA_READ_PREFIX_LAST_OR_PREV ||
      find_flag == HA_READ_PREFIX_LAST || find_flag == HA_READ_AFTER_KEY) {
    /* See below */
    bytes_changed_by_succ = kd.successor(m_sk_packed_tuple, packed_size);
  }

  common::Slice slice(reinterpret_cast<const char *>(m_sk_packed_tuple),
                       packed_size);

  uint end_key_packed_size = 0;
  const uint eq_cond_len =
      calc_eq_cond_len(kd, find_flag, slice, bytes_changed_by_succ, end_key,
                       &end_key_packed_size);

  bool use_all_keys = false;
  if (find_flag == HA_READ_KEY_EXACT &&
      my_count_bits(keypart_map) == kd.get_key_parts())
    use_all_keys = true;

  SeTransaction *const tx = get_or_create_tx(table->in_use);
  const bool is_new_snapshot = !tx->has_snapshot();
  // Loop as long as we get a deadlock error AND we end up creating the
  // snapshot here (i.e. it did not exist prior to this)
  for (;;) {
    /*
      This will open the iterator and position it at a record that's equal or
      greater than the lookup tuple.
    */
    common::Slice end_key_slice;
    common::Slice* end_key_slice_ptr = nullptr;
    const bool is_ascending_scan = is_ascending(kd, find_flag);
    // check whether can push down end_key, if false, end_key_slice_ptr would be nullptr
    // NOTE that, now we only support end_key pushdown for ascending (i.e. forward) scan
    if (is_ascending_scan && end_key && end_key_packed_size > 0) {
      if (end_key->flag == HA_READ_AFTER_KEY) {
        // including end key, thus the m_end_key_packed_tuple should be
        // the successor of m_end_key_packed_tuple (mem-comparable format)
        kd.successor(m_end_key_packed_tuple, end_key_packed_size);
      }
      end_key_slice.assign(reinterpret_cast<const char *>(m_end_key_packed_tuple), end_key_packed_size);
      end_key_slice_ptr = &end_key_slice;
    }
    setup_scan_iterator(kd, &slice, use_all_keys, is_ascending_scan,
                        eq_cond_len, end_key_slice_ptr);

    /*
      Once we are positioned on from above, move to the position we really
      want: See storage/smartengine/se-range-access.txt
    */
    bool move_forward;
    rc = position_to_correct_key(kd, find_flag, using_full_key, key,
                                 keypart_map, slice, &move_forward);

    if (rc) {
      /* This status is returned on any error */
      //table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(rc);
    }

    m_skip_scan_it_next_call = false;

    /*
      Now get the data for the row into 'buf'.  If we were using a primary key
      then we have all the rows we need.  For a secondary key we now need to
      lookup the primary key.
    */
    if (active_index == table->s->primary_key)
      rc = read_row_from_primary_key(buf);
    else
      rc = read_row_from_secondary_key(buf, kd, move_forward);

    if (rc != HA_ERR_LOCK_DEADLOCK || !is_new_snapshot)
      break; /* Exit the loop */

    // release the snapshot and iterator so they will be regenerated
    tx->release_snapshot();
    release_scan_iterator();
  }

  if (rc) {
    /* the only possible error condition is record-not-found */
    //table->status = STATUS_NOT_FOUND;
  } else {
    //table->status = 0;
    //stats.rows_read++;
    //stats.rows_index_first++;
    update_row_stats(ROWS_READ);
  }

  DBUG_RETURN(rc);
}

/*
  @brief
  Scan the secondary index until we find an index record that satisfies ICP

  @param move_forward   TRUE  <=> move m_scan_it forward
                        FALSE <=> move m_scan_it backward
  @param buf            Record buffer (must be the same buffer that
                        pushed index condition points to, in practice
                        it is table->record[0])

  @detail
  Move the current iterator m_scan_it until we get an index tuple that
  satisfies the pushed Index Condition.
  (if there is no pushed index condition, return right away)

  @return
    0     - Index tuple satisfies ICP, can do index read.
    other - error code
*/

int ha_smartengine::find_icp_matching_index_rec(
    const bool &move_forward,
    uchar *const buf)
{
  assert(buf != nullptr);


    const SeKeyDef &kd = *m_key_descr_arr[active_index];

    while (1) {
      if (!m_scan_it->Valid()) {
        //table->status = STATUS_NOT_FOUND;
        return HA_ERR_END_OF_FILE;
      }
      const common::Slice rkey = m_scan_it->key();

      if (!kd.covers_key(rkey)) {
        //table->status = STATUS_NOT_FOUND;
        return HA_ERR_END_OF_FILE;
      }

      if (m_sk_match_prefix) {
        const common::Slice prefix((const char *)m_sk_match_prefix,
                                    m_sk_match_length);
        if (!kd.value_matches_prefix(rkey, prefix)) {
          //table->status = STATUS_NOT_FOUND;
          return HA_ERR_END_OF_FILE;
        }
      }
      if (pushed_idx_cond && pushed_idx_cond_keyno == active_index) {
        const common::Slice value = m_scan_it->value();
        if (kd.unpack_record(table, buf, &rkey, &value, m_verify_row_debug_checksums)) {
          return HA_ERR_INTERNAL_ERROR;
        }

        const enum icp_result icp_status = check_index_cond();
        if (icp_status == ICP_NO_MATCH) {
          if (move_forward)
            m_scan_it->Next();
          else
            m_scan_it->Prev();
          continue; /* Get the next (or prev) index tuple */
        } else if (icp_status == ICP_OUT_OF_RANGE) {
          /* We have walked out of range we are scanning */
          //table->status = STATUS_NOT_FOUND;
          return HA_ERR_END_OF_FILE;
        } else /* icp_status == ICP_MATCH */
        {
          /* Index Condition is satisfied. We have rc==0, proceed to fetch the
           * row. */
          break;
        }
        /*
          TODO: should we have this here, or RockDB handles this internally?
          if (my_core::thd_killed(current_thd))
          {
            rc= HA_ERR_INTERNAL_ERROR; // doesn't matter
            break;
          }
        */
      } 
      else {
        break;
      }
  }

  return HA_EXIT_SUCCESS;
}

int ha_smartengine::scan_parallel(SeKeyDef *kd,
                                  SeTransaction *const tx,
                                  common::ParallelReader::F &&f)
{
  int ret = HA_EXIT_SUCCESS;
  uint size_start = 0;
  uint size_end = 0;

  kd->get_infimum_key(m_sk_packed_tuple, &size_start);
  kd->get_supremum_key(m_sk_packed_tuple_old, &size_end);

  const common::Slice user_key_start((const char *)m_sk_packed_tuple,
                                     size_start);
  const common::Slice user_key_end((const char *)m_sk_packed_tuple_old,
                                   size_end);

  size_t max_threads = se_thd_parallel_read_threads(ha_thd());
  DBUG_EXECUTE_IF("parallel_scan_kill", table->in_use->killed = THD::KILL_QUERY;);
  common::ParallelReader preader(max_threads, table->in_use);

  // generate range with internal_key [start,end)
  storage::Range range;
  preader.build_internal_key_range(user_key_start, user_key_end, range);

  common::ReadOptions read_options = tx->get_read_opts();
  read_options.total_order_seek = true;

  db::ColumnFamilyHandle *column_family_handle = kd->get_cf();
  common::ParallelReader::Config config(column_family_handle, range,
                                        read_options);

  const auto tx_impl = dynamic_cast<const SeTransactionImpl *>(tx);
  assert(tx_impl);

  util::Transaction *trx =
      const_cast<util::Transaction *>(tx_impl->get_se_trx());

  ret = preader.add_scan(trx, config, std::move(f));

  if (ret) {
    HANDLER_LOG(WARN, "prepare for parallel scan failed", K(ret));
  } else if ((ret = preader.run())) {
    HANDLER_LOG(WARN, "do parallel scan failed", K(ret));
  }

  return ret;
}

size_t ha_smartengine::get_parallel_read_threads()
{
  return se_thd_parallel_read_threads(ha_thd());
}

int ha_smartengine::check_parallel(THD *const thd, HA_CHECK_OPT *const check_opt)
{
  sql_print_information("--------- PARALLEL CHECK TABLE START ----------");
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(monitor::TracePoint::HA_CHECK);
  assert(thd != nullptr);
  assert(check_opt != nullptr);
  const char *const table_name = table->s->table_name.str;
  const char *const db_name = table->s->db.str;
  bool error = false;

  for (uint i = 0; i < table->s->fields; i++) {
    bitmap_set_bit(table->read_set, i);
  }

  SeTransaction *const tx = get_or_create_tx(table->in_use);
  assert(tx != nullptr);
  tx->acquire_snapshot(true);

  for (uint iter_keyno = 0; iter_keyno < table->s->keys; iter_keyno++) {
    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   Checking index %s", db_name,
                          table_name, table->key_info[iter_keyno].name);

    SeKeyDef *kd = m_key_descr_arr[iter_keyno].get();
    extra(HA_EXTRA_KEYREAD);
    ha_index_init(iter_keyno, true);

    size_t max_threads = se_thd_parallel_read_threads(ha_thd());

    std::vector<std::unique_ptr<ParallelScanCtx>> check_ctx_set(max_threads);
    for (int i = 0; i < static_cast<int>(check_ctx_set.size()); i++) {
      check_ctx_set[i].reset(new ParallelScanCtx(this));
      if (check_ctx_set[i]->init())
        DBUG_RETURN(HA_ADMIN_CORRUPT);
    }
    // traverse func def
    auto f = [&](const common::ParallelReader::ExecuteCtx *ctx,
                 db::Iterator *db_iter) {
      ParallelScanCtx &check_ctx = *(check_ctx_set[ctx->thread_id_].get());
      TABLE* tbl = &check_ctx.thd_table;
      common::Slice key, value;
      int ret = common::Status::kOk;

      /**Set the current_thd for parallel execute worker thread, server layer use the
      current_thd to get session variables.(Upstream commit 7b118bc)*/
      if (nullptr == current_thd) {
        current_thd = tbl->in_use;
      }

      key = db_iter->key();
      value = db_iter->value();

      if (!is_pk(iter_keyno, tbl, m_tbl_def.get())) {
        // sk
        check_ctx.secondary_key.copy(key.data(), key.size(), &my_charset_bin);
        // pk size
        const uint pk_size = kd->get_primary_key_tuple(tbl, *m_pk_descr, &key, check_ctx.pk_packed_tuple);
        // pk
        check_ctx.primary_key.copy((const char *)(check_ctx.pk_packed_tuple), pk_size,
                         &my_charset_bin);

        // TODO(qimu): checksum counter
        if (kd->unpack_info_has_checksum(value)) {
          check_ctx.checksums++;
        }

        // Step 1:
        // Make a se Get using rowkey_copy and put the mysql format
        // result in table->record[0]
        longlong hidden_pk_id = 0;
        if (has_hidden_pk(tbl) &&
            read_hidden_pk_id_from_rowkey(&hidden_pk_id, &check_ctx.primary_key)) {
          error = true;
          assert(0);
          goto one_row_checked;
        }

        if ((ret = get_row_by_rowid(tbl->record[0], check_ctx.primary_key.ptr(),
                                    check_ctx.primary_key.length(),
                                    check_ctx.record, tbl,
                                    check_ctx.primary_key, false))) {
          // NO_LINT_DEBUG
          sql_print_error(
              "CHECK TABLE %s.%s:   .. row %lld: "
              "iter sk and get from pk, get_row_by_rowid failed, thread %d",
              db_name, table_name, check_ctx.rows, ctx->thread_id_);
          advice_del_index(tbl, iter_keyno, hidden_pk_id,
                           tbl->record[0]);
          error = true;
          assert(0);
          goto one_row_checked;
        }

        // Step 2:
        // Convert the pk parts in table->record[0] to storage format then
        // compare it with rowkey_copy. table->record[0] is from pk subtable
        // get while the rowkey_copy is from sk subtable iteration.
        uint packed_size = m_pk_descr->pack_record(
            tbl, check_ctx.pack_buffer, tbl->record[0],
            check_ctx.pk_packed_tuple,
            nullptr, false, hidden_pk_id);
        assert(packed_size <= m_max_packed_sk_len);

        if (packed_size != check_ctx.primary_key.length() ||
            memcmp(check_ctx.pk_packed_tuple, check_ctx.primary_key.ptr(), packed_size)) {
          // NO_LINT_DEBUG
          sql_print_error(
              "CHECK TABLE %s.%s:   .. row %lld: "
              "iter sk and get from pk, "
              "pk decoded from m_scan_it and get_row_by_rowid mismatch",
              db_name, table_name, check_ctx.rows);
          print_error_row(db_name, table_name, check_ctx.primary_key, check_ctx.secondary_key);
          advice_del_index(tbl, iter_keyno, hidden_pk_id,
                           tbl->record[0]);
          error = true;
          assert(0);
          goto one_row_checked;
        }

        // Step 3:
        // Convert the sk[iter_keyno], pk parts in table->record[0] to storage
        // format then compare it with sec_key_copy. table->record[0] is from pk
        // subtable get while the sec_key_copy is from sk subtable iteration.
        packed_size = kd->pack_record(tbl, check_ctx.pack_buffer,
                                      tbl->record[0],
                                      check_ctx.sk_packed_tuple, &check_ctx.sk_tails, false,
                                      hidden_pk_id);
        assert(packed_size <= m_max_packed_sk_len);

        if (packed_size != check_ctx.secondary_key.length() ||
            memcmp(check_ctx.sk_packed_tuple, check_ctx.secondary_key.ptr(), packed_size)) {
          // NO_LINT_DEBUG
          sql_print_error(
              "CHECK TABLE %s.%s:   .. row %lld: "
              "iter sk and get from pk, "
              "sk from m_scan_it and sk decoded from get_row_by_rowid mismatch",
              db_name, table_name, check_ctx.rows);
          print_error_row(db_name, table_name, check_ctx.primary_key, check_ctx.secondary_key);
          advice_del_index(tbl, iter_keyno, hidden_pk_id,
                           tbl->record[0]);
          error = true;
          assert(0);
          goto one_row_checked;
        }

        // the scan thread Step 4: if index is unique index, we should check
        // whether unique attribute is ok
        if (tbl->key_info[iter_keyno].flags & HA_NOSAME) {
          uint n_null_fields = 0;
          uint current_sk_size =
              kd->get_memcmp_sk_size(tbl, key, &n_null_fields);
          if (!check_ctx.last_key.is_empty() && n_null_fields == 0) {
            if (check_ctx.last_key.length() == current_sk_size &&
                memcmp(check_ctx.last_key.ptr(), check_ctx.secondary_key.ptr(),
                       current_sk_size) == 0) {
              sql_print_error("CHECK TABLE %s.%s:  unique check failed",
                              db_name, table_name);
              error = true;
              assert(0);
              goto one_row_checked;
            }
          }

          // if no-null fields, there may be duplicated key error
          if (n_null_fields == 0) {
            check_ctx.last_key.copy(check_ctx.secondary_key.ptr(), current_sk_size,
                                   &my_charset_bin);
            if (check_ctx.first_key.is_empty()) {
              check_ctx.first_key.copy(check_ctx.secondary_key.ptr(), current_sk_size,
                                      &my_charset_bin);
            }
          }
        }
      } else {
        // pk
        check_ctx.primary_key.copy(key.data(), key.size(), &my_charset_bin);
        // record
        int rc =
            convert_record_from_storage_format(&key, &value, tbl->record[0], tbl);

        longlong hidden_pk_id = 0;
        if (has_hidden_pk(tbl) &&
            read_hidden_pk_id_from_rowkey(&hidden_pk_id, &check_ctx.primary_key)) {
          error = true;
          assert(0);
          goto one_row_checked;
        }

        // for pk-index and not hidden-pk, we check every key is different or
        // not
        if (!has_hidden_pk(tbl)) {
          if (!check_ctx.last_key.is_empty()) {
            if (check_ctx.last_key.length() == check_ctx.primary_key.length() &&
                memcmp(check_ctx.last_key.ptr(), check_ctx.primary_key.ptr(),
                       check_ctx.last_key.length()) == 0) {
              sql_print_error("CHECK TABLE %s.%s:  unique check failed",
                              db_name, table_name);
              error = true;
              assert(0);
              goto one_row_checked;
            }
          }
          check_ctx.last_key.copy(check_ctx.primary_key.ptr(), check_ctx.primary_key.length(),
                                 &my_charset_bin);
          if (check_ctx.first_key.is_empty()) {
            check_ctx.first_key.copy(check_ctx.primary_key.ptr(), check_ctx.primary_key.length(),
                                    &my_charset_bin);
          }
        }

        for (uint keyno = 0; keyno < tbl->s->keys; keyno++) {
          if (is_pk(keyno, tbl, m_tbl_def.get())) {
            // Assume the iterator and get would be consistency. Do not do a pk
            // get again.
            continue;
          }

          SeKeyDef *keydef = m_key_descr_arr[keyno].get();
          uint packed_size = keydef->pack_record(
              tbl, check_ctx.pack_buffer, tbl->record[0],
              check_ctx.sk_packed_tuple,
              &check_ctx.sk_tails, false, hidden_pk_id);

          std::string sk_value_str;
          common::Status s = tx->get(
              keydef->get_cf(),
              common::Slice(
                  reinterpret_cast<char *>(check_ctx.sk_packed_tuple), packed_size),
              &sk_value_str);

          // The secondary key kv does not exist.
          if (!s.ok()) {
            sql_print_error(
                "CHECK TABLE %s.%s:   .. row %lld: "
                "iter pk and get from sk, "
                "sk encoded from pk value does not exist",
                db_name, table_name, check_ctx.rows);
            print_error_row(db_name, table_name, check_ctx.primary_key, check_ctx.secondary_key);
            advice_add_index(tbl, keyno, hidden_pk_id,
                             tbl->record[0]);
            error = true;
            assert(0);
            goto one_row_checked;
          }
        }
      }
      one_row_checked:
      ++check_ctx.rows;
      if (my_core::thd_killed(current_thd)) {
        return 1;
      }
      return 0;
    };
    int ret = scan_parallel(kd, tx, f);

    // unique check in border
    for (int i = 0; i < static_cast<int>(check_ctx_set.size()) - 1; i++) {
      String& first = check_ctx_set[i]->last_key, &next = check_ctx_set[i + 1]->first_key;
      if (!first.is_empty() && first.length() == next.length() && memcmp(first.ptr(), next.ptr(), first.length()) == 0) {
        error = true;
        break;
      }
      // statistics
      check_ctx_set[check_ctx_set.size() - 1]->rows += check_ctx_set[i]->rows;
      check_ctx_set[check_ctx_set.size() - 1]->checksums +=
          check_ctx_set[i]->checksums;
    }

    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   ... %lld index entries checked "
                          "(%lld had checksums)",
                          db_name, table_name, check_ctx_set[max_threads - 1]->rows,
        check_ctx_set[max_threads - 1]->checksums);

    ha_index_end();
    if (my_core::thd_killed(current_thd)) {
      break;
    }
  }

  // TODO(qimu): row_checksums
//  if (row_checksums != ha_rows(-1)) {
//    // NO_LINT_DEBUG
//    sql_print_information(" CHECK TABLE %s.%s:   %lld "
//                          "table records had checksums",
//                          db_name, table_name, row_checksums);
//  }
  extra(HA_EXTRA_NO_KEYREAD);

  if (my_core::thd_killed(current_thd)) {
    sql_print_information("--------- CHECK TABLE KILLED ----------");
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  if (error) {
    // TODO(qimu): row_checksums
//    m_verify_row_debug_checksums = save_verify_row_debug_checksums;
    ha_index_or_rnd_end();
    extra(HA_EXTRA_NO_KEYREAD);
    sql_print_information("--------- CHECK TABLE END ----------");
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  sql_print_information("--------- PARALLEL CHECK TABLE END ----------");
  DBUG_RETURN(HA_ADMIN_OK);
}

/*
  Given a rowid (i.e. packed PK) as a parameter, get the record.

  @return
    0      - Ok
    other  - HA_ERR error code.
*/

int ha_smartengine::get_row_by_rowid(uchar *const buf,
                                     const char *const rowid,
                                     const uint rowid_size,
                                     std::string& retrieved_record,
                                     TABLE* tbl,
                                     String& key,
                                     const bool skip_lookup)
{
  DBUG_ENTER_FUNC();

  assert(buf != nullptr);
  assert(rowid != nullptr);
  assert(tbl != nullptr);

  int rc;

  common::Slice key_slice(rowid, rowid_size);

  SeTransaction *const tx = get_or_create_tx(tbl->in_use);
  assert(tx != nullptr);

  // only check ha_thd() with no parallelization cause current_thd is thread_local
  if (tbl == table) {
    DEBUG_SYNC(ha_thd(), "se.get_row_by_rowid");
  }

  DBUG_EXECUTE_IF("dbug.se.get_row_by_rowid", {
    THD *thd = ha_thd();
    const char act[] = "now signal Reached "
                       "wait_for signal.se.get_row_by_rowid_let_running";
    assert(opt_debug_sync_timeout > 0);
    assert(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
  };);

  bool found;
  common::Status s;

  /* Pretend row found without looking up */
  if (skip_lookup)
  {
   // stats.rows_deleted_blind++;
#if 0 // DEL-SYSSAT
    update_row_stats(ROWS_DELETED_BLIND);
#endif
    key.copy((const char *)rowid, rowid_size, &my_charset_bin);
    //table->status = 0;
    DBUG_RETURN(0);
  }

  if (m_lock_rows == SE_LOCK_NONE) {
    tx->acquire_snapshot(true);
    s = tx->get(m_pk_descr->get_cf(), key_slice, &retrieved_record);
  } else {
    s = get_for_update(tx, m_pk_descr->get_cf(), key_slice,
                       &retrieved_record);
  }

  if (!s.IsNotFound() && !s.ok()) {
    __HANDLER_LOG(WARN, "DML: lock failed for key(%s) on index %u with error %s, table_name is: %s",
                  key_slice.ToString(true).c_str(), m_pk_descr->get_index_number(),
                  s.ToString().c_str(), tbl->s->table_name.str);
    DBUG_RETURN(tx->set_status_error(tbl->in_use, s, *m_pk_descr, m_tbl_def.get()));
  }
  found = !s.IsNotFound();

  //table->status = STATUS_NOT_FOUND;
  if (found) {
    key.copy((const char *)rowid, rowid_size, &my_charset_bin);
    rc = convert_record_from_storage_format(&key_slice, retrieved_record, buf, tbl);
    //if (!rc)
    //  table->status = 0;
  } else {
    /*
      Note: we don't need to unlock the row. It is intentional that we keep
      locks on rows that don't exist.
    */
    rc = HA_ERR_KEY_NOT_FOUND;
  }

  DBUG_RETURN(rc);
}

int ha_smartengine::get_row_by_rowid(uchar *const buf,
                                     const uchar *const rowid,
                                     const uint rowid_size,
                                     const bool skip_lookup)
{
  return get_row_by_rowid(buf,
                          reinterpret_cast<const char *>(rowid),
                          rowid_size,
                          skip_lookup);
}

int ha_smartengine::get_row_by_rowid(uchar *const buf,
                                     const char *const rowid,
                                     const uint rowid_size,
                                     const bool skip_lookup)
{
  return get_row_by_rowid(buf,
                          rowid,
                          rowid_size,
                          m_retrieved_record,
                          table,
                          m_last_rowkey,
                          skip_lookup);
}

int ha_smartengine::index_next_with_direction(uchar *const buf, bool move_forward)
{
  DBUG_ENTER_FUNC();

  int rc;

  if (active_index == pk_index(table, m_tbl_def.get())) {
    rc = rnd_next_with_direction(buf, move_forward);
  } else {
    // check if writebatch iterator is still OK
    SeTransaction *const tx = get_tx_from_thd(table->in_use);
    if (tx && (!tx->is_writebatch_valid())) {
      util::BaseDeltaIterator *p_scan;
      p_scan = dynamic_cast<util::BaseDeltaIterator *>(m_scan_it);
      if (p_scan) {
        p_scan->InvalidDelta();
      }
    }

    if (m_skip_scan_it_next_call) {
      m_skip_scan_it_next_call = false;
    } else {
      if (move_forward)
        m_scan_it->Next(); /* this call cannot fail */
      else
        m_scan_it->Prev();
    }
    rc = find_icp_matching_index_rec(move_forward, buf);
    if (!rc)
      rc = secondary_index_read(active_index, buf);
  }

  DBUG_RETURN(rc);
}

/*
  Start scanning from the "first" value.

  The 'first' here means "the first from start of the key space".
  For reverse-ordered key spaces, the first value will be the biggest, 'zzzz'.

  An picture of a forward-ordered keyspace (remember, the keys have form
  'indexnr-keyval'. Suppose the index we are at has number n)

      (n-1) - ...
      ( n )          <--- 1. (n) doesn't exist in the db but it would be here.
      ( n ) - aaa       <--- 2. Seek("n") will put us here on the first index
      ( n ) - bbb               record.
      ( n ) - cc

  So, need to do: Seek(n);

  A backward-ordered keyspace:

      (n+1) - bbb
      (n+1) - aaa
      (n+1)        <--- (n+1) doesn't exist in the db but would be here.
      ( n ) - ccc       <--- 1. We need to be here.
      ( n ) - bbb
      ( n ) - aaa
      ( n )

  So, need to: Seek(n+1);

*/

int ha_smartengine::index_first_intern(uchar *const buf)
{
  DBUG_ENTER_FUNC();

  assert(buf != nullptr);

  uchar *key;
  uint key_size;
  int rc;

  if (is_pk(active_index, table, m_tbl_def.get())) {
    key = m_pk_packed_tuple;
  } else {
    key = m_sk_packed_tuple;
  }

  assert(key != nullptr);

  const SeKeyDef &kd = *m_key_descr_arr[active_index];
  if (kd.m_is_reverse_cf) {
    kd.get_supremum_key(key, &key_size);
  } else {
    kd.get_infimum_key(key, &key_size);
  }

  common::Slice index_key((const char *)key, key_size);

  SeTransaction *const tx = get_or_create_tx(table->in_use);
  assert(tx != nullptr);

  const bool is_new_snapshot = !tx->has_snapshot();
  // Loop as long as we get a deadlock error AND we end up creating the
  // snapshot here (i.e. it did not exist prior to this)
  for (;;) {
    setup_scan_iterator(kd, &index_key, false, !kd.m_is_reverse_cf,
                        SeKeyDef::INDEX_NUMBER_SIZE);
    m_skip_scan_it_next_call = true;

    rc = index_next_with_direction(buf, true);
    if (rc != HA_ERR_LOCK_DEADLOCK || !is_new_snapshot)
      break; // exit the loop

    // release the snapshot and iterator so they will be regenerated
    tx->release_snapshot();
    release_scan_iterator();
  }

  if (!rc) {
    /*
      index_next is always incremented on success, so decrement if it is
      index_first instead
     */
    //stats.rows_index_first++;
    //stats.rows_index_next--;
  }

  DBUG_RETURN(rc);
}

/**
  @details
  Start scanning from the "last" value

  The 'last' here means "the last from start of the key space".
  For reverse-ordered key spaces, we will actually read the smallest value.

  An picture of a forward-ordered keyspace (remember, the keys have form
  'indexnr-keyval'. Suppose the we are at a key that has number n)

     (n-1)-something
     ( n )-aaa
     ( n )-bbb
     ( n )-ccc            <----------- Need to seek to here.
     (n+1)      <---- Doesn't exist, but would be here.
     (n+1)-smth, or no value at all

   se's Iterator::Seek($val) seeks to "at $val or first value that's
   greater". We can't see to "(n)-ccc" directly, because we don't know what
   is the value of 'ccc' (the biggest record with prefix (n)). Instead, we seek
   to "(n+1)", which is the least possible value that's greater than any value
   in index #n. Then we step one record back.

   So, need to:  it->Seek(n+1) || it->SeekToLast(); it->Prev();

   A backward-ordered keyspace:

      (n+1)-something
      ( n ) - ccc
      ( n ) - bbb
      ( n ) - aaa       <---------------- (*) Need to seek here.
      ( n ) <--- Doesn't exist, but would be here.
      (n-1)-smth, or no value at all

   So, need to:  it->Seek(n) || it->SeekToLast(); it->Prev();
*/
int ha_smartengine::index_last_intern(uchar *const buf)
{
  DBUG_ENTER_FUNC();

  assert(buf != nullptr);

  uchar *key;
  uint key_size;
  int rc;

  if (is_pk(active_index, table, m_tbl_def.get())) {
    key = m_pk_packed_tuple;
  } else {
    key = m_sk_packed_tuple;
  }

  assert(key != nullptr);

  const SeKeyDef &kd = *m_key_descr_arr[active_index];
  if (kd.m_is_reverse_cf) {
    kd.get_infimum_key(key, &key_size);
  } else {
    kd.get_supremum_key(key, &key_size);
  }

  common::Slice index_key((const char *)key, key_size);

  SeTransaction *const tx = get_or_create_tx(table->in_use);
  assert(tx != nullptr);

  bool is_new_snapshot = !tx->has_snapshot();
  // Loop as long as we get a deadlock error AND we end up creating the
  // snapshot here (i.e. it did not exist prior to this)
  for (;;) {
    setup_scan_iterator(kd, &index_key, false, kd.m_is_reverse_cf,
                        SeKeyDef::INDEX_NUMBER_SIZE);

    if (!m_scan_it->Valid()) {
      /*
        se keyspace has no records that follow our index. It could be, our
        index is the last in the keyspace. Navigate to last record in the
        keyspace.
      */
      m_scan_it->SeekToLast();
    } else {
      /*
        Iterator is pointing at some record right after the end of the index.
        Move one record back.
      */
      m_scan_it->Prev();
    }
    m_skip_scan_it_next_call = false;

    if (is_pk(active_index, table, m_tbl_def.get())) {
      m_skip_scan_it_next_call = true;
      rc = rnd_next_with_direction(buf, false);
    } else {
      rc = find_icp_matching_index_rec(false /*move_forward*/, buf);
      if (!rc)
        rc = secondary_index_read(active_index, buf);
    }

    if (rc != HA_ERR_LOCK_DEADLOCK || !is_new_snapshot)
      break; /* exit the loop */

    // release the snapshot and iterator so they will be regenerated
    tx->release_snapshot();
    release_scan_iterator();
  }

  if (!rc) {
    /*
      index_next is always incremented on success, so decrement if it is
      index_first instead
     */
    //stats.rows_index_first++;
    //stats.rows_index_next--;
  }

  DBUG_RETURN(rc);
}

/*
  Open a cursor and position it at the passed record
*/

void ha_smartengine::setup_scan_iterator(const SeKeyDef &kd,
                                         common::Slice *const slice,
                                         const bool use_all_keys,
                                         const bool is_ascending,
                                         const uint eq_cond_len,
                                         const common::Slice* end_key)
{
  assert(slice != nullptr);
  assert(slice->size() >= eq_cond_len);

  SeTransaction *const tx = get_or_create_tx(table->in_use);

  bool skip_bloom = true;
  m_cmp_end_key = true;

  const common::Slice eq_cond(slice->data(), eq_cond_len);
  if (can_use_bloom_filter(ha_thd(), kd, eq_cond, use_all_keys, is_ascending)) {
    skip_bloom = false;
  }

  /*
    In some cases, setup_scan_iterator() is called multiple times from
    the same query but bloom filter can not always be used.
    Suppose the following query example. id2 is VARCHAR(30) and PRIMARY KEY
    (id1, id2).
     select count(*) from t2 WHERE id1=100 and id2 IN ('00000000000000000000',
    '100');
    In this case, setup_scan_iterator() is called twice, the first time is for
    (id1, id2)=(100, '00000000000000000000') and the second time is for (100,
    '100').
    If prefix bloom filter length is 24 bytes, prefix bloom filter can be used
    for the
    first condition but not for the second condition.
    If bloom filter condition is changed, currently it is necessary to destroy
    and
    re-create Iterator.
  */
  if (m_scan_it_skips_bloom != skip_bloom) {
    release_scan_iterator();
  }

  /*
    SQL layer can call rnd_init() multiple times in a row.
    In that case, re-use the iterator, but re-position it at the table start.
  */
  if (!m_scan_it) {
    const bool fill_cache = true; // !THDVAR(ha_thd(), skip_fill_cache);
    m_scan_it = tx->get_iterator(kd.get_cf(), skip_bloom, fill_cache);
    m_scan_it_skips_bloom = skip_bloom;
    // reset writebatch iterator valid for new statement
    tx->reset_writebatch_iterator();
  }

  if (nullptr != end_key) {
    m_cmp_end_key = false;
    int ret = m_scan_it->set_end_key(*end_key);
    assert(0 == ret);
  } else {
    int ret = m_scan_it->set_end_key(common::Slice() /*empty end key*/);
    assert(0 == ret);
  }
  /*
    Seek() will "Position at the first key in the source that at or past
    target".
    The operation cannot fail.
  */
  m_scan_it->Seek(*slice);
}

void ha_smartengine::release_scan_iterator()
{
  // delete m_scan_it;
  MOD_DELETE_OBJECT(Iterator, m_scan_it);
  m_scan_it = nullptr;

  if (m_scan_it_snapshot) {
    se_db->ReleaseSnapshot(m_scan_it_snapshot);
    m_scan_it_snapshot = nullptr;
  }
}

void ha_smartengine::setup_iterator_for_rnd_scan()
{
  uint key_size;

  if (m_pk_descr->m_is_reverse_cf)
    m_pk_descr->get_supremum_key(m_pk_packed_tuple, &key_size);
  else
    m_pk_descr->get_infimum_key(m_pk_packed_tuple, &key_size);

  common::Slice table_key((const char *)m_pk_packed_tuple, key_size);

  setup_scan_iterator(*m_pk_descr, &table_key);
  m_skip_scan_it_next_call = true;
}

/*
  See also secondary_index_read().
*/
int ha_smartengine::rnd_next_with_direction(uchar *const buf, bool move_forward)
{
  DBUG_ENTER_FUNC();

  int rc;

  //table->status = STATUS_NOT_FOUND;
  //stats.rows_requested++;

  if (!m_scan_it || !m_scan_it->Valid()) {
    /*
      We can get here when SQL layer has called

        h->index_init(PRIMARY);
        h->index_read_map(full index tuple, HA_READ_KEY_EXACT);

      In this case, we should return EOF.
    */
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  // check if writebatch iterator is still OK
  SeTransaction *const tx = get_tx_from_thd(table->in_use);
  if (tx && (!tx->is_writebatch_valid())) {
    util::BaseDeltaIterator *p_scan;
    p_scan = dynamic_cast<util::BaseDeltaIterator *>(m_scan_it);
    if (p_scan) {
      p_scan->InvalidDelta();
    }
  }

  for (;;) {
    if (m_skip_scan_it_next_call) {
      m_skip_scan_it_next_call = false;
    } else {
      if (move_forward)
        m_scan_it->Next(); /* this call cannot fail */
      else
        m_scan_it->Prev(); /* this call cannot fail */
    }

    if (!m_scan_it->Valid()) {
      rc = HA_ERR_END_OF_FILE;
      break;
    }

    /* check if we're out of this table */
    const common::Slice key = m_scan_it->key();
    if (!m_pk_descr->covers_key(key)) {
      rc = HA_ERR_END_OF_FILE;
      break;
    }

    if (m_sk_match_prefix) {
      const common::Slice prefix((const char *)m_sk_match_prefix, m_sk_match_length);
      if (!m_pk_descr->value_matches_prefix(key, prefix)) {
        rc = HA_ERR_END_OF_FILE;
        break;
      }
    }

    if (m_lock_rows != SE_LOCK_NONE) {
      /*
        Lock the row we've just read.

        Now we call get_for_update which will 1) Take a lock and 2) Will fail
        if the row was deleted since the snapshot was taken.
      */
      SeTransaction *const tx = get_or_create_tx(table->in_use);
      DEBUG_SYNC(ha_thd(), "se_concurrent_delete");
      const common::Status s =
          get_for_update(tx, m_pk_descr->get_cf(), key, &m_retrieved_record);
      if (s.IsNotFound() &&
          my_core::thd_tx_isolation(ha_thd()) == ISO_READ_COMMITTED) {
        // This occurs if we accessed a row, tried to lock it, failed,
        // released and reacquired the snapshot (because of READ COMMITTED
        // mode) and the row was deleted by someone else in the meantime.
        // If so, we just want to move on to the next row.
        continue;
      }

      if (!s.ok()) {
        DBUG_RETURN(
            tx->set_status_error(table->in_use, s, *m_pk_descr, m_tbl_def.get()));
      }

      // If we called get_for_update() use the value from that call not from
      // the iterator as it may be stale since we don't have a snapshot
      // when m_lock_rows is not SE_LOCK_NONE.
      m_last_rowkey.copy(key.data(), key.size(), &my_charset_bin);
      rc = convert_record_from_storage_format(&key, m_retrieved_record, buf, table);
    } else {
      // Use the value from the iterator
      common::Slice value = m_scan_it->value();
      m_last_rowkey.copy(key.data(), key.size(), &my_charset_bin);
      rc = convert_record_from_storage_format(&key, &value, buf, table);
    }

    //table->status = 0;
    break;
  }

  if (!rc) {
    //stats.rows_read++;
    //stats.rows_index_next++;
    update_row_stats(ROWS_READ);
  }

  DBUG_RETURN(rc);
}

void ha_smartengine::update_stats(void)
{
  DBUG_ENTER_FUNC();

  stats.records = 0;
  stats.index_file_length = 0ul;
  stats.data_file_length = 0ul;
  stats.mean_rec_length = 0;
  SeIndexStats index_stats;

  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    if (is_pk(i, table, m_tbl_def.get())) {
      index_stats.m_gl_index_id = m_pk_descr->get_gl_index_id();
      if (se_db->get_columnfamily_stats(m_pk_descr->get_cf(), index_stats.m_data_size,
                                      index_stats.m_rows, index_stats.m_entry_deletes, index_stats.m_actual_disk_size)) {
        ddl_manager.adjust_stats(index_stats);
        index_stats.reset();
      }
      stats.data_file_length = m_pk_descr->m_stats.m_actual_disk_size;
      stats.records = m_pk_descr->m_stats.m_rows;
    } else {
      index_stats.m_gl_index_id = m_key_descr_arr[i]->get_gl_index_id();
      if (se_db->get_columnfamily_stats(m_key_descr_arr[i]->get_cf(), index_stats.m_data_size,
                                      index_stats.m_rows, index_stats.m_entry_deletes, index_stats.m_actual_disk_size)) {
        ddl_manager.adjust_stats(index_stats);
        index_stats.reset();
      }
      stats.index_file_length += m_key_descr_arr[i]->m_stats.m_actual_disk_size;
    }
  }

  DBUG_VOID_RETURN;
}

db::Range ha_smartengine::get_range(const int &i, uchar buf[SeKeyDef::INDEX_NUMBER_SIZE * 2]) const
{
  return smartengine::get_range(*m_key_descr_arr[i], buf);
}

int ha_smartengine::records_from_index_single(ha_rows *num_rows, uint index)
{
  int error = 0;
  ha_rows rows = 0;
  uchar *buf = table->record[0];
  start_psi_batch_mode();

  /* set index_only flag for se, see more in task #25925477 */
  extra(HA_EXTRA_KEYREAD);
  if (!(error = ha_index_init(index, false))) {
    if (!(error = ha_index_first(buf))) {
      rows = 1;
      while (!table->in_use->killed) {
        DBUG_EXECUTE_IF("bug28079850",
                        table->in_use->killed = THD::KILL_QUERY;);
        if ((error = ha_index_next(buf))) {
          if (error == HA_ERR_RECORD_DELETED)
            continue;
          else
            break;
        }
        ++rows;
      }
    }
  }
  *num_rows = rows;
  end_psi_batch_mode();
  int ha_index_end_error = 0;
  if (error != HA_ERR_END_OF_FILE) *num_rows = HA_POS_ERROR;
  // Call ha_index_end() only if handler has been initialized.
  if (inited && (ha_index_end_error = ha_index_end())) *num_rows = HA_POS_ERROR;

  /* reset index_only flag */
  extra(HA_EXTRA_NO_KEYREAD);
  return (error != HA_ERR_END_OF_FILE) ? error : ha_index_end_error;
}

int ha_smartengine::calculate_stats(const TABLE *const table_arg,
                                    THD *const thd,
                                    HA_CHECK_OPT *const check_opt)
{
  DBUG_ENTER_FUNC();

  // find per column family key ranges which need to be queried
  std::unordered_map<db::ColumnFamilyHandle *, std::vector<db::Range>>
      ranges;
  std::unordered_set<GL_INDEX_ID> ids_to_check;
  std::unordered_map<GL_INDEX_ID, uint> ids_to_keyparts;
  std::vector<uchar> buf(m_tbl_def->m_key_count * 2 *
                         SeKeyDef::INDEX_NUMBER_SIZE);
  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    const auto bufp = &buf[i * 2 * SeKeyDef::INDEX_NUMBER_SIZE];
    const SeKeyDef &kd = *m_key_descr_arr[i];
    ranges[kd.get_cf()].push_back(get_range(i, bufp));
    ids_to_check.insert(kd.get_gl_index_id());
    ids_to_keyparts[kd.get_gl_index_id()] = kd.get_key_parts();
  }

  // for analyze statements, force flush on memtable to get accurate cardinality
  SeSubtableManager &cf_manager = se_get_cf_manager();
  if (thd != nullptr && /*THDVAR(thd, flush_memtable_on_analyze) &&*/
      !se_pause_background_work) {
    for (auto it : ids_to_check) {
      se_db->Flush(common::FlushOptions(), cf_manager.get_cf(it.cf_id));
    }
  }

  int num_sst = 0;
  // group stats per index id
  std::unordered_map<GL_INDEX_ID, SeIndexStats> stats;
  for (const auto &it : ids_to_check) {
    // Initialize the stats to 0. If there are no files that contain
    // this gl_index_id, then 0 should be stored for the cached stats.
    stats[it] = SeIndexStats(it);
    assert(ids_to_keyparts.count(it) > 0);
    stats[it].m_distinct_keys_per_prefix.resize(ids_to_keyparts[it]);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**
  SQL layer calls this function to push an index condition.

  @details
    The condition is for index keyno (only one condition can be pushed at a
    time).
    The caller guarantees that condition refers only to index fields; besides
    that, fields must have

      $field->part_of_key.set_bit(keyno)

    which means that

       (handler->index_flags(keyno, $keypart, 0) & HA_KEYREAD_ONLY) == 1

    which means that field value can be restored from the index tuple.

  @return
    Part of condition we couldn't check (always nullptr).
*/

class Item *ha_smartengine::idx_cond_push(uint keyno, class Item *const idx_cond)
{
  DBUG_ENTER_FUNC();

  assert(keyno != MAX_KEY);
  assert(idx_cond != nullptr);

  const SeKeyDef &kd = *m_key_descr_arr[keyno];
  if (kd.get_support_icp_flag()) {
    pushed_idx_cond = idx_cond;
    pushed_idx_cond_keyno = keyno;
    in_range_check_pushed_down = TRUE;

    /* We will check the whole condition */
    DBUG_RETURN(nullptr);
  } else {
    DBUG_RETURN(idx_cond);
  }
}

/*
  @brief
  Check the index condition.

  @detail
  Check the index condition. (The caller has unpacked all needed index
  columns into table->record[0])

  @return
    ICP_NO_MATCH - Condition not satisfied (caller should continue
                   scanning)
    OUT_OF_RANGE - We've left the range we're scanning (caller should
                   stop scanning and return HA_ERR_END_OF_FILE)

    ICP_MATCH    - Condition is satisfied (caller should fetch the record
                   and return it)
*/

enum icp_result ha_smartengine::check_index_cond() const
{
  assert(pushed_idx_cond);
  assert(pushed_idx_cond_keyno != MAX_KEY);

  if (end_range && compare_key_icp(end_range) > 0) {
    /* caller should return HA_ERR_END_OF_FILE already */
    return ICP_OUT_OF_RANGE;
  }

  return pushed_idx_cond->val_int() ? ICP_MATCH : ICP_NO_MATCH;
}

/**
  Updates row counters based on the table type and operation type.
*/
void ha_smartengine::update_row_stats(const operation_type &type)
{
  assert(type < ROWS_MAX);
  // Find if we are modifying system databases.
  if (table->s && m_tbl_def->m_is_mysql_system_table)
    global_stats.system_rows_[type].inc();
  else
    global_stats.rows_[type].inc();
}

} // namespace smartengine
