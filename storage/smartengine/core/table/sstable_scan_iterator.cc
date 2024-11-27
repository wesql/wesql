/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "db/table_cache.h"
#include "table/sstable_scan_iterator.h"

namespace smartengine
{
using namespace common;
using namespace db;
using namespace storage;
using namespace util;
using namespace monitor;

namespace table
{

int TablePrefetchHelper::init(const ScanParam &param)
{
  int ret = Status::kOk;
  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "init twice", K(ret));
  } else if (UNLIKELY(!param.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid arguement", K(ret), K(param));
  } else {
    scan_param_ = &param;
    if (FAILED(extent_layer_iter_.init(scan_param_->icomparator_,
                                       scan_param_->layer_position_,
                                       scan_param_->extent_layer_))) {
      SE_LOG(WARN, "failed to init extent layer iterator", K(ret));
    } else {
      valid_= true;
      is_inited_ = true;
    }
  }
  return ret;
}

int TablePrefetchHelper::seek(const Slice &target)
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::TABLE_PREFETCHER_SEEK);
  valid_ = true;
  extent_layer_iter_.Seek(target);
  // in seek, we can only prefetch the table reader corresponding to
  // the target, since we do not know the direction of scan
  if (!extent_layer_iter_.Valid()) {
    valid_ = false;
  } else if (FAILED(do_prefetch_index_block())) {
    SE_LOG(WARN, "do prefetch index block failed", K(ret));
  }
  QUERY_TRACE_END();
  return ret;
}

int TablePrefetchHelper::seek_to_first()
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::TABLE_PREFETCHER_SEEK);
  valid_ = true;
  extent_layer_iter_.SeekToFirst();
  // in seek_to_first, we can only prefetch the table reader corresponding to
  // the target, since we do not know the direction of scan
  if (!extent_layer_iter_.Valid()) {
    valid_ = false;
  } else if (FAILED(do_prefetch_index_block())) {
    SE_LOG(WARN, "do prefetch next failed", K(ret));
  }
  QUERY_TRACE_END();
  return ret;
}

int TablePrefetchHelper::seek_to_last()
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::TABLE_PREFETCHER_SEEK);
  valid_ = true;
  extent_layer_iter_.SeekToLast();
  // in seek_to_last, we can only prefetch the table reader corresponding to
  // the target, since we do not know the direction of scan
  if (!extent_layer_iter_.Valid()) {
    valid_ = false;
  } else if (FAILED(do_prefetch_index_block())) {
    SE_LOG(WARN, "do prefetch index block failed", K(ret));
  }
  QUERY_TRACE_END();
  return ret;
}

// prefetch table reader and index block forward
int TablePrefetchHelper::prefetch_next()
{
  int ret = Status::kOk;
  if (need_prefetch() && extent_layer_iter_.Valid()) {
    const int64_t prefetch_cnt = calc_prefetch_cnt();
    for (int64_t i = 0; SUCCED(ret) && i < prefetch_cnt; i++) {
      extent_layer_iter_.Next();
      if (!extent_layer_iter_.Valid()) {
        break;
      } else if (FAILED(do_prefetch_index_block())) {
        SE_LOG(WARN, "do prefetch index block failed", K(ret));
      }
    }
  }
  return ret;
}

// prefetch table reader and index block backward
int TablePrefetchHelper::prefetch_prev()
{
  int ret = Status::kOk;
  if (need_prefetch() && extent_layer_iter_.Valid()) {
    const int64_t prefetch_cnt = calc_prefetch_cnt();
    for (int64_t i = 0; SUCCED(ret) && i < prefetch_cnt; i++) {
      extent_layer_iter_.Prev();
      if (!extent_layer_iter_.Valid()) {
        break;
      } else if (FAILED(do_prefetch_index_block())) {
        SE_LOG(WARN, "do prefetch index block failed", K(ret));
      }
    }
  }
  return ret;
}

// prefetch a table reader and the corresponding index block
int TablePrefetchHelper::do_prefetch_index_block()
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::TABLE_PREFETCH_INDEX);
  assert(extent_layer_iter_.Valid());
  Slice meta_handle = extent_layer_iter_.value();
  TableReaderHandle &handle = get_table_reader_handle(table_reader_prefetch_pos_);
  handle.reset();
  BlockDataHandle<RowBlock> &index_handle = get_index_handle(table_reader_prefetch_pos_);
  index_handle.reset();
  index_handle.is_boundary_ = extent_layer_iter_.get_is_boundary();
  if (FAILED(load_table_reader(meta_handle, handle))) {
    SE_LOG(WARN, "failed to get table reader", K(ret), K(meta_handle),
        K(table_reader_prefetch_pos_), K(index_block_cur_pos_));
  } else if (IS_NULL(handle.extent_reader_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "table reader is nullptr", K(ret), K(table_reader_prefetch_pos_),
        K(index_block_cur_pos_));
  } else if (FAILED(handle.extent_reader_->prefetch_index_block(index_handle))) {
    SE_LOG(WARN, "failed to prefetch index block", K(ret), K(meta_handle),
        K(table_reader_prefetch_pos_), K(index_block_cur_pos_));
  } else {
    table_reader_prefetch_pos_++;
  }
  QUERY_TRACE_END();
  return ret;
}

int TablePrefetchHelper::load_table_reader(const Slice &meta_handle, TableReaderHandle &table_reader_handle)
{
  int ret = Status::kOk;
  ExtentId *eid = (ExtentId *)meta_handle.data();
  table_reader_handle.extent_id_ = *eid;
  if (UNLIKELY(nullptr != table_reader_handle.cache_handle_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "table cache handle was not released", K(ret));
  } else if (FAILED(scan_param_->table_cache_->find_extent_reader(*(scan_param_->icomparator_),
                                                                  *eid,
                                                                  scan_param_->read_options_->read_tier == kBlockCacheTier /* no_io */,
                                                                  scan_param_->skip_filters_,
                                                                  scan_param_->layer_position_.get_level(),
                                                                  true /* TODO: prefetch_index_and_filter_in_cache */,
                                                                  &table_reader_handle.cache_handle_))) {
    SE_LOG(WARN, "failed to find table from table cache", K(ret));
  } else if (IS_NULL(table_reader_handle.cache_handle_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "handle is nullptr", K(ret));
  } else {
    table_reader_handle.table_cache_ = scan_param_->table_cache_;
    table_reader_handle.extent_reader_ = scan_param_->table_cache_->get_extent_reader_from_handle(table_reader_handle.cache_handle_);
    // TODO: we can set AIORandomAccessExtent to table_reader here,
    //       but the table_reader might be used by multiple SSTableScanIterators,
    //       thus it seems we should keep AIORandomAccessExtent in SSTableScanIterator?
  }
  return ret;
}

int TablePrefetchHelper::next()
{
  int ret = Status::kOk;
  // unref table cache entry
  release_handle(get_index_handle(index_block_cur_pos_));
  index_block_cur_pos_++;
  if (index_block_cur_pos_ == table_reader_prefetch_pos_ && !extent_layer_iter_.Valid()) {
    valid_ = false;
  }
  return ret;
}

int TablePrefetchHelper::prev()
{
  return next();
}

int TablePrefetchHelper::init_index_block_iter(RowBlockIterator &index_block_iter)
{
  int ret = Status::kOk;
  ExtentReader *extent_reader = get_table_reader_handle(index_block_cur_pos_).extent_reader_;
  BlockDataHandle<RowBlock> &index_handle = get_index_handle(index_block_cur_pos_);
  index_block_iter.reset();
  if (IS_NULL(extent_reader)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "current table reader is nullptr", K(ret));
  } else if (FAILED(extent_reader->setup_index_block_iterator(&index_block_iter))) {
    SE_LOG(WARN, "failed to new index iterator", K(ret), K(index_block_cur_pos_), K(table_reader_prefetch_pos_));
  } else {
    index_block_iter.set_end_key(extent_layer_iter_.get_end_key(), index_handle.is_boundary_);
  }
  return ret;
}

int BlockPrefetchHelper::init(const ScanParam &param)
{
  int ret = Status::kOk;
  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "init twice", K(ret));
  } else if (UNLIKELY(!param.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid arguement", K(ret), K(param));
  } else if (FAILED(table_prefetch_helper_.init(param))) {
    SE_LOG(WARN, "failed to init table prefetch helper", K(ret));
  } else {
    scan_param_ = &param;
    valid_ = true;
    is_inited_ = true;
  }
  return ret;
}

int BlockPrefetchHelper::seek(const Slice &target)
{
  int ret = Status::kOk;
  valid_ = true;
  if (FAILED(table_prefetch_helper_.seek(target))) {
    SE_LOG(WARN, "table_prefetch_helper_ seek failed", K(ret));
  } else if (!table_prefetch_helper_.valid()) {
    valid_ = false;
  } else if (FAILED(table_prefetch_helper_.init_index_block_iter(index_block_iter_))) {
    SE_LOG(WARN, "failed to init index block iterator", K(ret));
  } else {
    index_block_iter_.Seek(target);
    if (!index_block_iter_.Valid()) {
      valid_ = false;
    } else if (FAILED(do_prefetch_data_block(true))) {
      SE_LOG(WARN, "do prefetch data block failed", K(ret));
    }
  }
  return ret;
}

int BlockPrefetchHelper::seek_to_first()
{
  int ret = Status::kOk;
  valid_ = true;
  if (FAILED(table_prefetch_helper_.seek_to_first())) {
    SE_LOG(WARN, "table_prefetch_helper_ seek to first failed", K(ret));
  } else if (!table_prefetch_helper_.valid()) {
    valid_ = false;
  } else if (FAILED(table_prefetch_helper_.init_index_block_iter(index_block_iter_))) {
    SE_LOG(WARN, "failed to init index block iterator", K(ret));
  } else {
    index_block_iter_.SeekToFirst();
    if (!index_block_iter_.Valid()) {
      valid_ = false;
    } else if (FAILED(do_prefetch_data_block(true))) {
      SE_LOG(WARN, "do prefetch data block failed", K(ret));
    }
  }
  return ret;
}

int BlockPrefetchHelper::seek_to_last()
{
  int ret = Status::kOk;
  valid_ = true;
  if (FAILED(table_prefetch_helper_.seek_to_last())) {
    SE_LOG(WARN, "table_prefetch_helper_ seek to last failed", K(ret));
  } else if (!table_prefetch_helper_.valid()) {
    valid_ = false;
  } else if (FAILED(table_prefetch_helper_.init_index_block_iter(index_block_iter_))) {
    SE_LOG(WARN, "failed to init index block iterator", K(ret));
  } else {
    index_block_iter_.SeekToLast();
    if (!index_block_iter_.Valid()) {
      valid_ = false;
    } else if (FAILED(do_prefetch_data_block(true))) {
      SE_LOG(WARN, "do prefetch data block failed", K(ret));
    }
  }
  return ret;
}

int BlockPrefetchHelper::do_prefetch_data_block(const bool send_req_after_add)
{
  int ret = Status::kOk;
  bool need_send_req = false;
  int64_t pos = 0;
  ExtentReader *extent_reader = nullptr;
  BlockDataHandle<RowBlock> &handle = get_block_handle(data_block_prefetch_pos_);
  handle.reset();
  handle.extent_id_ = ExtentId(index_block_iter_.get_source());
  handle.is_boundary_ = index_block_iter_.get_is_boundary();

  if (!index_block_iter_.Valid()) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the inde block iterator must be valid", K(ret));
  } else if (FAILED(handle.block_info_.deserialize_critical_info(index_block_iter_.value().data(), index_block_iter_.value().size(), pos))) {
    SE_LOG(WARN, "fail to deserialize BlockInfo", K(ret));
  } else if (IS_NULL(extent_reader = index_extent_reader())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the extent reader must not be nullptr", K(ret));
  } else if (FAILED(extent_reader->prefetch_data_block(handle))) {
    SE_LOG(WARN, "fail to prefetch data block", K(ret));
  } else {
    if (FAILED(merge_io_request(handle, need_send_req))) {
      SE_LOG(WARN, "fail to merge io request", K(ret));
    } else if (need_send_req || send_req_after_add) {
      if (FAILED(send_merged_io_request())) {
        SE_LOG(WARN, "fail to send merged io request", K(ret));
      }
    }
    ++data_block_prefetch_pos_;
  }

  return ret;
}

int BlockPrefetchHelper::merge_io_request(const BlockDataHandle<RowBlock> &handle,
                                          bool &need_send_req)
{
  int ret = Status::kOk;
  need_send_req = false;
  // merge continuous io blocks
  if (nullptr == handle.block_entry_.value_) {
    // not in block cache, merge
    if (io_merge_handle_.is_empty()) {
      io_merge_handle_.set_start_pos(data_block_prefetch_pos_);
    }
    io_merge_handle_.set_end_pos(data_block_prefetch_pos_);
  } else {
    // in block cache, send previous merged io request
    need_send_req = true;
  }
  return ret;
}


int BlockPrefetchHelper::send_merged_io_request()
{
  int ret = Status::kOk;
  if (!io_merge_handle_.is_empty()) {
    int64_t offset = 0;
    int64_t size = 0;

    BlockDataHandle<RowBlock> &start_handle = get_block_handle(io_merge_handle_.get_start_pos());
    BlockDataHandle<RowBlock> &end_handle = get_block_handle(io_merge_handle_.get_end_pos());
    // in forward scan, the offset of merged io is the start_handle's offset
    // in backward scan, the offset of merged io is the end_handle's offset
    offset = std::min(start_handle.block_info_.get_handle().get_offset(), end_handle.block_info_.get_handle().get_offset());
    // all handles share one AIOReq
    std::shared_ptr<AIOReq> aio_req(new AIOReq());

    for (int64_t i = io_merge_handle_.get_start_pos(); i <= io_merge_handle_.get_end_pos(); i++) {
      BlockDataHandle<RowBlock> &handle = get_block_handle(i);
      size += handle.block_info_.get_handle().get_size();
      handle.aio_handle_.aio_req_ = aio_req;
      handle.has_prefetched_ = true;
    }

    ExtentReader *extent_reader = index_extent_reader();
    if (extent_reader->do_io_prefetch(offset, size, &start_handle.aio_handle_)) {
      SE_LOG(WARN, "failed to do io prefetch", K(ret), K(offset), K(size));
    }
    io_merge_handle_.reset();
  }
  return ret;
}

// prefetch data blocks forward
int BlockPrefetchHelper::prefetch_next()
{
  int ret = Status::kOk;
  if (need_prefetch()) {
    const int64_t prefetch_cnt = calc_prefetch_cnt();
    for (int64_t i = 0; SUCCED(ret) && i < prefetch_cnt; i++) {
      if (index_block_iter_.Valid()) {
        // iterate current index block and do data block prefetch
        index_block_iter_.Next();
        if (!index_block_iter_.Valid()) {
          // switch extent, should send io req
          if (FAILED(send_merged_io_request())) {
            SE_LOG(WARN, "failed to send merged io request", K(ret));
          }
          i--;
        } else if (FAILED(do_prefetch_data_block(i == (prefetch_cnt - 1) /*last block in this batch*/))) {
          SE_LOG(WARN, "failed to do prefetch data block", K(ret));
        }
      } else if (FAILED(table_prefetch_helper_.prefetch_next())) { // do index block prefetch
        SE_LOG(WARN, "table_prefetch_helper_ prefetch next failed", K(ret));
      } else if (table_prefetch_helper_.is_empty()) {
        break;
      } else if (FAILED(table_prefetch_helper_.next())) { // move to next index block
        SE_LOG(WARN, "table_prefetch_helper_ get next failed", K(ret));
      } else if (FAILED(table_prefetch_helper_.init_index_block_iter(index_block_iter_))) {
        SE_LOG(WARN, "failed to init index block iterator", K(ret));
      } else {
        index_block_iter_.SeekToFirst();
        if (index_block_iter_.Valid()) {
          if (FAILED(do_prefetch_data_block(i == (prefetch_cnt - 1) /*last block in this batch*/))) {
            SE_LOG(WARN, "failed to do prefetch data block", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// prefetch data blocks backward
int BlockPrefetchHelper::prefetch_prev()
{
  int ret = Status::kOk;
  if (need_prefetch()) {
    const int64_t prefetch_cnt = calc_prefetch_cnt();
    for (int64_t i = 0; SUCCED(ret) && i < prefetch_cnt; i++) {
      if (index_block_iter_.Valid()) {
        // iterate current index block and do data block prefetch
        index_block_iter_.Prev();
        if (!index_block_iter_.Valid()) {
          // switch extent, should send io request
          if (FAILED(send_merged_io_request())) {
            SE_LOG(WARN, "failed to send merged io request", K(ret));
          }
          i--;
        } else if (FAILED(do_prefetch_data_block(i == (prefetch_cnt - 1) /*last block in this batch*/))) {
          SE_LOG(WARN, "failed to do prefetch data block", K(ret));
        }
      } else if (FAILED(table_prefetch_helper_.prefetch_prev())) { // do index block prefetch
        SE_LOG(WARN, "table_prefetch_helper_ prefetch prev failed", K(ret));
      } else if (table_prefetch_helper_.is_empty()) {
        break;
      } else if (FAILED(table_prefetch_helper_.prev())) { // move to prev index block
        SE_LOG(WARN, "table_prefetch_helper_ get prev failed", K(ret));
      } else if (FAILED(table_prefetch_helper_.init_index_block_iter(index_block_iter_))) {
        SE_LOG(WARN, "failed to init index block iterator", K(ret));
      } else {
        index_block_iter_.SeekToLast();
        if (index_block_iter_.Valid()) {
          if (FAILED(do_prefetch_data_block(i == (prefetch_cnt - 1) /*last block in this batch*/))) {
            SE_LOG(WARN, "failed to do prefetch data block", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int BlockPrefetchHelper::next()
{
  int ret = Status::kOk;
  if (FAILED(prefetch_next())) {
    SE_LOG(WARN, "do prefetch next failed", K(ret));
  } else {
    // unref data block cache entry or delete block
    release_handle(get_block_handle(data_block_cur_pos_));
    data_block_cur_pos_++;
    if (data_block_cur_pos_ == data_block_prefetch_pos_) {
      valid_ = false;
    }
  }
  return ret;
}

int BlockPrefetchHelper::prev()
{
  int ret = Status::kOk;
  if (FAILED(prefetch_prev())) {
    SE_LOG(WARN, "do prefetch prev failed", K(ret));
  } else {
    // unref data block cache entry or delete block
    release_handle(get_block_handle(data_block_cur_pos_));
    data_block_cur_pos_++;
    if (data_block_cur_pos_ == data_block_prefetch_pos_) {
      valid_ = false;
    }
  }
  return ret;
}

int BlockPrefetchHelper::init_data_block_iter(RowBlockIterator &data_block_iter)
{
  int ret = Status::kOk;
  ExtentReader *extent_reader = nullptr;
  data_block_iter.reset();
  BlockDataHandle<RowBlock> &block_handle = get_block_handle(data_block_cur_pos_);
  if (FAILED(get_extent_reader(block_handle.extent_id_, extent_reader))) {
    SE_LOG(WARN, "failed to get table reader", K(ret));
  } else if (IS_NULL(extent_reader)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "current table reader is nullptr", K(ret));
  } else if (FAILED(extent_reader->setup_data_block_iterator(scan_param_->scan_add_blocks_limit_,
                                                             add_blocks_,
                                                             block_handle,
                                                             &data_block_iter))) {
  
    SE_LOG(WARN, "failed to new data block iterator", K(ret));
  } else {
    data_block_iter.set_end_key(index_block_iter_.get_end_key(), block_handle.is_boundary_);
    data_block_iter.set_source(block_handle.extent_id_.id());
  }
  return ret;
}

int SSTableScanIterator::init(const ScanParam &param)
{
  int ret = Status::kOk;
  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "init twice", K(ret));
  } else if (UNLIKELY(!param.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid arguement", K(ret), K(param));
  } else {
    scan_param_ = param;
    read_options_ = *param.read_options_;
    scan_param_.read_options_ = &read_options_;
    if (FAILED(block_prefetch_helper_.init(scan_param_))) {
      SE_LOG(WARN, "failed to init prefetch helper", K(ret), K(param));
    } else {
      valid_ = false;
      is_inited_ = true;
    }
  }
  return ret;
}

void SSTableScanIterator::Seek(const Slice& target)
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::SST_SCAN_ITER_SEEK);
  // for reentrancy
  reset();
  if (FAILED(block_prefetch_helper_.seek(target))) {
    status_ = Status(ret);
    SE_LOG(WARN, "block_prefetch_helper_ seek failed", K(ret));
  } else if (block_prefetch_helper_.valid()) {
    data_block_iter_.reset();
    if (FAILED(block_prefetch_helper_.init_data_block_iter(data_block_iter_))) {
      status_ = Status(ret);
      SE_LOG(WARN, "block_prefetch_helper_ init data iter failed", K(ret));
    } else {
      valid_ = true;
      data_block_iter_.Seek(target);
      skip_empty_data_blocks_forward();
    }
  }
  QUERY_TRACE_END();
}

void SSTableScanIterator::SeekToFirst()
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::SST_SCAN_ITER_SEEK);
  // for reentrancy
  reset();
  if (FAILED(block_prefetch_helper_.seek_to_first())) {
    status_ = Status(ret);
    SE_LOG(WARN, "block_prefetch_helper_ seek to first failed", K(ret));
  } else if (block_prefetch_helper_.valid()) {
    data_block_iter_.reset();
    if (FAILED(block_prefetch_helper_.init_data_block_iter(data_block_iter_))) {
      status_ = Status(ret);
      SE_LOG(WARN, "block_prefetch_helper_ init data iter failed", K(ret));
    } else {
      valid_ = true;
      data_block_iter_.SeekToFirst();
      skip_empty_data_blocks_forward();
    }
  }
  QUERY_TRACE_END();
}

void SSTableScanIterator::SeekToLast()
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::SST_SCAN_ITER_SEEK);
  // for reentrancy
  reset();
  if (FAILED(block_prefetch_helper_.seek_to_last())) {
    status_ = Status(ret);
    SE_LOG(WARN, "block_prefetch_helper_ seek to last failed", K(ret));
  } else if (block_prefetch_helper_.valid()) {
    data_block_iter_.reset();
    if (FAILED(block_prefetch_helper_.init_data_block_iter(data_block_iter_))) {
      status_ = Status(ret);
      SE_LOG(WARN, "block_prefetch_helper_ init data iter failed", K(ret));
    } else {
      valid_ = true;
      data_block_iter_.SeekToLast();
      skip_empty_data_blocks_backward();
    }
  }
  QUERY_TRACE_END();
}

void SSTableScanIterator::SeekForPrev(const Slice &target)
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::SST_SCAN_ITER_SEEK);
  // for reentrancy
  reset();
  if (FAILED(block_prefetch_helper_.seek(target))) {
    status_ = Status(ret);
    SE_LOG(WARN, "block_prefetch_helper_ seek failed", K(ret));
  } else if (block_prefetch_helper_.valid()) {
    data_block_iter_.reset();
    if (FAILED(block_prefetch_helper_.init_data_block_iter(data_block_iter_))) {
      status_ = Status(ret);
      SE_LOG(WARN, "block_prefetch_helper_ init data iter failed", K(ret));
    } else {
      valid_ = true;
      data_block_iter_.SeekForPrev(target);
      skip_empty_data_blocks_backward();
    }
  }
  QUERY_TRACE_END();
}

void SSTableScanIterator::Next()
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::SST_SCAN_ITER_NEXT);
  if (first_time_prefetch_) {
    // we don't know the scan direction in Seek, thus cannot do
    // prefetch in Seek, do the first-time prefetch here
    if (FAILED(block_prefetch_helper_.prefetch_next())) {
      status_ = Status(ret);
      SE_LOG(WARN, "block_prefetch_helper_ prefetch next failed", K(ret));
    } else {
      first_time_prefetch_ = false;
    }
  }
  if (SUCCED(ret)) {
    data_block_iter_.Next();
    skip_empty_data_blocks_forward();
  }
  QUERY_TRACE_END();
}

void SSTableScanIterator::Prev()
{
  int ret = Status::kOk;
  if (first_time_prefetch_) {
    // we don't know the scan direction in Seek, thus cannot do
    // prefetch in Seek, do the first-time prefetch here
    if (FAILED(block_prefetch_helper_.prefetch_prev())) {
      status_ = Status(ret);
      SE_LOG(WARN, "block_prefetch_helper_ prefetch prev failed", K(ret));
    } else {
      first_time_prefetch_ = false;
    }
  }
  if (SUCCED(ret)) {
    data_block_iter_.Prev();
    skip_empty_data_blocks_backward();
  }
}

void SSTableScanIterator::skip_empty_data_blocks_forward()
{
  int ret = Status::kOk;
  // TODO(Zhao Dongsheng): This function will mask read errors, causing it
  // to skip certain exceptional data, ultimately returing fewer data than excepted.
  while (!data_block_iter_.Valid() && block_prefetch_helper_.valid()) {
    if (FAILED(block_prefetch_helper_.next())) {
      status_ = Status(ret);
      SE_LOG(WARN, "block_prefetch_helper_ next failed", K(ret));
    } else if (block_prefetch_helper_.valid()) {
      data_block_iter_.reset();
      if (FAILED(block_prefetch_helper_.init_data_block_iter(data_block_iter_))) {
        status_ = Status(ret);
        SE_LOG(WARN, "block_prefetch_helper_ init data iter", K(ret));
      } else {
        data_block_iter_.SeekToFirst();
        //if (!data_block_iter_.Valid()) {
          //SE_LOG(WARN, "exist empty block!");
        //}
      }
    } else {
      valid_ = false;
      break;
    }
  }
}

void SSTableScanIterator::skip_empty_data_blocks_backward()
{
  int ret = Status::kOk;
  while (!data_block_iter_.Valid() && block_prefetch_helper_.valid()) {
    if (FAILED(block_prefetch_helper_.prev())) {
      status_ = Status(ret);
      SE_LOG(WARN, "block_prefetch_helper_ prev failed", K(ret));
    } else if (block_prefetch_helper_.valid()) {
      data_block_iter_.reset();
      if (FAILED(block_prefetch_helper_.init_data_block_iter(data_block_iter_))) {
        status_ = Status(ret);
        SE_LOG(WARN, "block_prefetch_helper_ init data iter", K(ret));
      } else {
        data_block_iter_.SeekToLast();
        //if (!data_block_iter_.Valid()) {
          //SE_LOG(WARN, "exist empty block!");
        //}
      }
    } else {
      valid_ = false;
      break;
    }
  }
}

} // namespace table
} // namespace smartengine
