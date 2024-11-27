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

#include "table/extent_reader.h"

#include "memory/base_malloc.h"
#include "storage/extent_meta_manager.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "table/bloom_filter.h"
#include "table/column_block_iterator.h"
#include "table/get_context.h"
#include "table/index_block_reader.h"
#include "table/large_object.h"
#include "table/row_block_writer.h"
#include "util/crc32c.h"

namespace smartengine
{
using namespace cache;
using namespace common;
using namespace db;
using namespace memory;
using namespace monitor;
using namespace storage;
using namespace util;

namespace table
{

DEFINE_TO_STRING(ExtentReaderArgs, KV_(extent_id), KVP_(internal_key_comparator), KVP_(block_cache))

ExtentReader::ExtentReader()
    : is_inited_(false),
      internal_key_comparator_(nullptr),
      extent_(nullptr),
      extent_meta_(nullptr),
      index_block_(nullptr),
      block_cache_(nullptr),
      cache_entry_key_()
{}

ExtentReader::~ExtentReader()
{
  destroy();
}

// TODO(Zhao Dongsheng) : In the current implementation, the lifecycle of the index block
// is kept consistent with the extent reader, resulting in the index block being synchronously
// read during the initialization of the extent reader.This is unnecessary for compaction
// tasks, in a compaction task, the IO for reading the index block can be optimized because
// the data for the entire extent has already been prefetched.
int ExtentReader::init(const ExtentReaderArgs &args)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ExtentReader has been inited", K(ret));
  } else if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (FAILED(IS_NULL(extent_meta_ = ExtentMetaManager::get_instance().get_meta(args.extent_id_)))) {
    SE_LOG(WARN, "fail to get extent meta", K(ret), K(args));
  } else if (FAILED(ExtentSpaceManager::get_instance().get_readable_extent(extent_meta_->extent_id_, extent_))) {
    SE_LOG(WARN, "fail to get readable extent", K(ret), K(*extent_meta_));
  } else if (FAILED(read_index_block(extent_meta_->index_block_handle_, index_block_))) {
    SE_LOG(WARN, "fail to read index block", K(ret), K(*extent_meta_));
  } else if (FAILED(cache_entry_key_.setup(extent_))) {
    SE_LOG(WARN, "fail to setup cache key", K(ret), K(args));
  } else {
    se_assert(extent_meta_->is_normal_extent());
    internal_key_comparator_ = args.internal_key_comparator_;
    block_cache_ = args.block_cache_;
    is_inited_ = true;
  }

  // resource clean
  if (FAILED(ret)) {
    destroy();
  }

  return ret;
}

void ExtentReader::destroy()
{
  if (IS_NOTNULL(extent_)) {
    DELETE_OBJECT(ModId::kIOExtent, extent_);
  }

  if (IS_NOTNULL(index_block_)) {
    MOD_DELETE_OBJECT(RowBlock, index_block_);
  }
}

int ExtentReader::get(const Slice &key, GetContext *get_context)
{
  int ret = Status::kOk;
  bool done = false; // meaning the completion of get process, not mean that the key is found.
  bool may_exist = true;
  IndexBlockReader index_block_reader;
  BlockInfo block_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(key.empty()) || IS_NULL(get_context)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(key), KP(get_context));
  } else if (FAILED(index_block_reader.init(index_block_, internal_key_comparator_))) {
    SE_LOG(WARN, "fail to init index block reader", K(ret));
  } else {
    index_block_reader.seek(key);
    // TODO(Zhao Dongsheng): the loop can optimize.
    while (SUCCED(ret) && !done && index_block_reader.valid()) {
      block_info.reset();
      may_exist = true;
      if (FAILED(index_block_reader.get_value(true /*only_critical_info*/, block_info))) {
        SE_LOG(WARN, "fail to get block index", K(ret));
      } else if (block_info.has_bloom_filter() &&
                 FAILED(check_in_bloom_filter(get_context->get_user_key(), block_info, may_exist))) {
        SE_LOG(WARN, "fail to check in bloom filter", K(ret), K(block_info));
      } else {
        if (may_exist) {
          if (FAILED(inner_get(key, block_info, get_context, done))) {
            SE_LOG(WARN, "fail to get from data block", K(ret), K(block_info));
          }
        } else {
          QUERY_COUNT(CountPoint::BLOOM_FILTER_USEFUL);
        }

        if (SUCCED(ret)) {
          index_block_reader.next();
        }
      }
    }
  }

  return ret;
}

InternalIterator *ExtentReader::NewIterator(const common::ReadOptions &read_options,
                                            memory::SimpleAllocator *allocaor,
                                            bool skip_filters,
                                            const uint64_t scan_add_blocks_limit)
{
  abort();
  return nullptr;
}

int ExtentReader::prefetch_index_block(BlockDataHandle<RowBlock> &handle)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (IS_NULL(index_block_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "index block must not be nullptr", K(ret), KP(index_block_));
  } else {
    // The lifetimes of ExtentReader and IndexBlock are consistent.
    // The index block is released when destroy ExtentReader.
    handle.block_entry_.value_ = index_block_;
    handle.need_do_cleanup_ = false;
  }

  return ret;
}

int ExtentReader::prefetch_data_block(BlockDataHandle<RowBlock> &data_block_handle)
{
  int ret = Status::kOk;
  char cache_key_buf[CacheEntryKey::MAX_CACHE_KEY_SZIE] = {0};
  Slice cache_key;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (IS_NULL(block_cache_)) {
   // distable block cache, the prefetching does nothing.
  } else if (FAILED(cache_entry_key_.generate(data_block_handle.block_info_.get_handle().get_offset(),
                                              cache_key_buf,
                                              cache_key))) {
    SE_LOG(WARN, "fail to generate cache entry key", K(ret));
  } else if (IS_NULL(data_block_handle.block_entry_.handle_ = block_cache_->Lookup(cache_key))) {
    // Cache miss, the data block will be read by aio_handle.
    QUERY_COUNT(CountPoint::BLOCK_CACHE_MISS);
  } else {
    // Cache hit, the SSTableScanIterator is responsible for releasing the cache handle.
    data_block_handle.block_entry_.value_ = reinterpret_cast<RowBlock*>(
        block_cache_->Value(data_block_handle.block_entry_.handle_));
    data_block_handle.cache_ = block_cache_;
    data_block_handle.need_do_cleanup_ = true;
    QUERY_COUNT(CountPoint::BLOCK_CACHE_HIT);
  }

  return ret;
}

int ExtentReader::do_io_prefetch(const int64_t offset, const int64_t size, AIOHandle *aio_handle)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(offset < 0) || UNLIKELY(size <= 0) || IS_NULL(aio_handle)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(offset), K(size), KP(aio_handle));
  } else if (FAILED(extent_->prefetch(aio_handle, offset, size))) {
    SE_LOG(WARN, "fail to prefetch", K(ret), K(offset), K(size));
  } else {
    // succeed
  }

  return ret;
}

// Reading the persistent state of a block on disk returns a block consistent
// with the data stored on disk, including the block trailer, and has not
// undergone decompression. It's used by compaction when the block is reused.
int ExtentReader::get_data_block(const BlockHandle &handle, util::AIOHandle *aio_handle, char *buf, Slice &block)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(!handle.is_valid()) || IS_NULL(buf)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(handle), KP(buf));
  } else if (FAILED(read_block(handle, aio_handle, buf, block))) {
    SE_LOG(WARN, "fail to read block", K(ret), K(handle), KP(aio_handle));
  } else {
    // succeed
  }

  return ret;
}

int ExtentReader::setup_index_block_iterator(RowBlockIterator *index_block_iterator)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (IS_NULL(index_block_iterator)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(index_block_iterator));
  } else if (FAILED(index_block_iterator->setup(internal_key_comparator_, index_block_, true /*is_index_block*/))) {
    SE_LOG(WARN, "fail to setup index block iterator", K(ret));
  } else {
    index_block_iterator->set_source(extent_meta_->extent_id_.id());
  }

  return ret;
}

int ExtentReader::setup_data_block_iterator(const bool use_cache,
                                            BlockDataHandle<RowBlock> &data_block_handle,
                                            RowBlockIterator *data_block_iterator)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(!data_block_handle.block_info_.is_valid()) || IS_NULL(data_block_iterator)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(data_block_handle.block_info_), KP(data_block_iterator));
  } else {
    // get data block
    if (use_cache && IS_NOTNULL(block_cache_)) {
      if (FAILED(get_data_block_through_cache(data_block_handle))) {
        SE_LOG(WARN, "fail to get data block through block cache", K(ret));
      }
    } else {
      if (FAILED(get_data_block_bypass_cache(data_block_handle))) {
        SE_LOG(WARN, "fail to get data block bypass block cache", K(ret));
      }
    }

    // setup iterator
    if (SUCCED(ret)) {
      if (FAILED(data_block_iterator->setup(internal_key_comparator_,
                                            data_block_handle.block_entry_.value_,
                                            false /*is_index_block*/))) {
        SE_LOG(WARN, "fail to setup data block iterator", K(ret));
      }
    }
  }

  return ret;
}

int ExtentReader::setup_data_block_iterator(const int64_t scan_add_blocks_limit,
                                            int64_t &added_blocks,
                                            BlockDataHandle<RowBlock> &data_block_handle,
                                            RowBlockIterator *data_block_iterator)
{
  int ret = Status::kOk;
  RowBlock *data_block = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(scan_add_blocks_limit < 0) ||
             UNLIKELY(added_blocks < 0) ||
             UNLIKELY(!data_block_handle.block_info_.is_valid()) ||
             IS_NULL(data_block_iterator)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(scan_add_blocks_limit), K(added_blocks),
        K(data_block_handle.block_info_), KP(data_block_iterator));
  } else if (IS_NOTNULL(data_block_handle.block_entry_.handle_)) {
    // the data block has been prefetched successfully.
    if (IS_NULL(data_block = data_block_handle.block_entry_.value_)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "the data block must not be nullptr", K(ret));
    }
  } else {
    // the data block hasn't been prefetched successfully and the prefetch
    // io has submitted, so reap the aio result first.
    if (FAILED(read_data_block(data_block_handle.block_info_,
                               data_block_handle.get_aio_handle(),
                               data_block))) {
      SE_LOG(WARN, "fail to read data block", K(ret));
    } else if (IS_NOTNULL(block_cache_) && added_blocks < scan_add_blocks_limit) {
      char cache_key_buf[CacheEntryKey::MAX_CACHE_KEY_SZIE] = {0};
      Slice cache_key;

      update_mod_id(data_block->data_, ModId::kDataBlockCache);
      if (FAILED(cache_entry_key_.generate(data_block_handle.block_info_.get_handle().get_offset(), cache_key_buf, cache_key))) {
        SE_LOG(WARN, "fail to generate cache key", K(ret));
      } else if (FAILED(block_cache_->Insert(cache_key,
                                             data_block,
                                             data_block->usable_size(),
                                             &CacheEntryHelper::delete_entry<RowBlock>,
                                             &data_block_handle.block_entry_.handle_,
                                             Cache::Priority::LOW).code())) {
        QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD_FAILURES);
        SE_LOG(WARN, "fail to insert into block cache", K(ret));
      } else {
        data_block_handle.cache_ = block_cache_;
        data_block_handle.block_entry_.value_ = data_block;
        data_block_handle.need_do_cleanup_ = true;

        ++added_blocks;
        QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
        QUERY_COUNT(CountPoint::BLOCK_CACHE_DATA_ADD);
      }
    } else {
      data_block_handle.cache_ = nullptr;
      data_block_handle.block_entry_.handle_ = nullptr;
      data_block_handle.block_entry_.value_ = data_block;
      data_block_handle.need_do_cleanup_ = true;
    }
  }

  if (SUCCED(ret)) {
    if (IS_NULL(data_block_handle.block_entry_.value_)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "the data block must not be nullptr", K(ret), K(data_block_handle.block_info_));
    } else if (FAILED(data_block_iterator->setup(internal_key_comparator_,
                                                 data_block_handle.block_entry_.value_,
                                                 false /*is_index_block*/))) {
      SE_LOG(WARN, "fail to setup data block iterator", K(ret));
    }
  }

  return ret;
}

int ExtentReader::check_block_in_cache(const BlockHandle &block_handle, bool &in_cache)
{
  int ret = Status::kOk;
  char cache_key_buf[CacheEntryKey::MAX_CACHE_KEY_SZIE] = {0};
  Slice cache_key;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(!block_handle.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block_handle));
  } else if (IS_NULL(block_cache_)) {
    in_cache = false;
  } else if (FAILED(cache_entry_key_.generate(block_handle.get_offset(), cache_key_buf, cache_key))) {
    SE_LOG(WARN, "fail to generate cache entry key", K(ret), K(block_handle));
  } else {
    in_cache = block_cache_->check_in_cache(cache_key);
  }

  return ret;
}

int ExtentReader::approximate_key_offet(const Slice &key, int64_t &offset)
{
  int ret = Status::kOk;
  IndexBlockReader index_block_reader;
  BlockInfo block_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (FAILED(index_block_reader.init(index_block_, internal_key_comparator_))) {
    SE_LOG(WARN, "fail to init index block reader", K(ret));
  } else {
    index_block_reader.seek(key);
    if (index_block_reader.valid()) {
      if (FAILED(index_block_reader.get_value(true /*only_critical_info*/, block_info))) {
        SE_LOG(WARN, "fail to get block stats", K(ret));
      } else {
        offset = block_info.get_handle().get_offset();
      }
    } else {
      // key is past the last key in the extent. Approximate the offset
      // by returning the offset of the index block (which is right near
      // the end of the extent).
      offset = extent_meta_->index_block_handle_.get_offset();
    }
  }

  return ret;
}

int64_t ExtentReader::get_usable_size() const
{
  return sizeof(*this) + sizeof(*index_block_) + index_block_->size_;
}

int ExtentReader::check_in_bloom_filter(const Slice &user_key, const BlockInfo &block_info, bool &may_exist)
{
  int ret = Status::kOk;
  BloomFilterReader bloom_filter_reader;

  if (UNLIKELY(user_key.empty()) || UNLIKELY(!block_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(user_key), K(block_info));
  } else if (FAILED(bloom_filter_reader.init(block_info.get_bloom_filter(), block_info.get_probe_num()))) {
    SE_LOG(WARN, "fail to init bloom filter reader", K(ret), K(block_info));
  } else if (FAILED(bloom_filter_reader.check(user_key, may_exist))) {
    SE_LOG(WARN, "fail to check through bloom filter reader", K(ret), K(user_key));
  } else {
#ifndef NDEBUG
    SE_LOG(DEBUG, "check in bloom filter", K(may_exist));
#endif
  }

  return ret;
}

int ExtentReader::inner_get(const Slice &key,
                            const BlockInfo &block_info,
                            GetContext *get_context,
                            bool &done)
{
  int ret = Status::kOk;
  int64_t added_blocks = 0;
  RowBlockIterator data_block_iterator;
  BlockDataHandle<RowBlock> data_block_handle;
  data_block_handle.extent_id_ = extent_meta_->extent_id_;
  data_block_handle.block_info_ = block_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(!block_info.is_valid()) || IS_NULL(get_context)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block_info), KP(get_context));
  } else if (FAILED(setup_data_block_iterator(true /*use_cache*/, data_block_handle, &data_block_iterator))) {
    SE_LOG(WARN, "fail to setup data block iterator", K(ret));
  } else {
    data_block_iterator.Seek(key);
    while (SUCCED(ret) && !done && data_block_iterator.Valid()) {
      db::ParsedInternalKey parsed_key;
      if (!db::ParseInternalKey(data_block_iterator.key(), &parsed_key)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "fail to parse internal key", K(ret));
      } else if (!get_context->SaveValue(parsed_key,
                                         data_block_iterator.value(),
                                         &data_block_iterator)) {
        done = true;
        if (GetContext::kFound == get_context->State() && (kTypeValueLarge == parsed_key.type)) {
          LargeValue large_value;
          Slice large_result;
          if (FAILED(large_value.convert_to_normal_format(data_block_iterator.value(), large_result))) {
            SE_LOG(WARN, "fail to covert large value to normal format", K(ret));
          } else {
            get_context->SaveLargeValue(large_result);
          }
        }
      } else {
        data_block_iterator.Next();
      }
    }
  }

  // Release the data block cache handle.
  data_block_handle.reset();

  return ret;
}

int ExtentReader::get_data_block_through_cache(BlockDataHandle<RowBlock> &data_block_handle)
{
  int ret = Status::kOk;
  RowBlock *data_block = nullptr;
  char cache_key_buf[CacheEntryKey::MAX_CACHE_KEY_SZIE] = {0};
  Slice cache_key;

  if (UNLIKELY(!data_block_handle.block_info_.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid arguement", K(ret), "block_info", data_block_handle.block_info_);
  } else if (IS_NULL(block_cache_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "try to get data block through disabled block cache", K(ret));
  } else if (FAILED(cache_entry_key_.generate(data_block_handle.block_info_.get_handle().get_offset(), cache_key_buf, cache_key))) {
    SE_LOG(WARN, "fail to generate cache key", K(ret));
  } else if (IS_NOTNULL(data_block_handle.block_entry_.handle_ = block_cache_->Lookup(cache_key))) {
    // cache hit
    QUERY_COUNT(CountPoint::BLOCK_CACHE_DATA_HIT);
    if (IS_NULL(data_block_handle.block_entry_.value_ = reinterpret_cast<RowBlock *>(
        block_cache_->Value(data_block_handle.block_entry_.handle_)))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "the data block must not be nullptr", K(ret));
    } else {
      data_block_handle.cache_ = block_cache_;
      data_block_handle.need_do_cleanup_ = true;
    }
  } else {
    // cache miss
    QUERY_COUNT(CountPoint::BLOCK_CACHE_MISS);
    if (FAILED(read_data_block(data_block_handle.block_info_,
                               data_block_handle.get_aio_handle(),
                               data_block))) {
      SE_LOG(WARN, "fail to read row block", K(ret), K(data_block_handle.block_info_));
    } else {
      update_mod_id(data_block->data_, ModId::kDataBlockCache);
      if (FAILED(block_cache_->Insert(cache_key,
                                      data_block,
                                      data_block->usable_size(),
                                      &CacheEntryHelper::delete_entry<RowBlock>,
                                      &(data_block_handle.block_entry_.handle_),
                                      Cache::Priority::LOW).code())) {
        QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD_FAILURES);
        SE_LOG(WARN, "fail to insert into block cache", K(ret));
      } else {
        data_block_handle.cache_ = block_cache_;
        data_block_handle.block_entry_.value_ = data_block;
        data_block_handle.need_do_cleanup_ = true;

        QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
        QUERY_COUNT(CountPoint::BLOCK_CACHE_DATA_ADD);
      }
    }
  }

  // resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(data_block)) {
      MOD_DELETE_OBJECT(RowBlock, data_block);
    }
  }

  return ret;
}

int ExtentReader::get_data_block_bypass_cache(BlockDataHandle<RowBlock> &data_block_handle)
{
  int ret = Status::kOk;
  RowBlock *data_block = nullptr;

  if (UNLIKELY(!data_block_handle.block_info_.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(data_block_handle.block_info_));
  } else if (FAILED(read_data_block(data_block_handle.block_info_,
                                    data_block_handle.get_aio_handle(),
                                    data_block))) {
    SE_LOG(WARN, "fail to read row block", K(ret), K(data_block_handle.block_info_));
  } else {
    data_block_handle.cache_ = nullptr;
    data_block_handle.block_entry_.value_ = data_block;
    data_block_handle.need_do_cleanup_ = true;
  }

  return ret;
}

int ExtentReader::read_index_block(const BlockHandle &handle, RowBlock *&index_block)
{
  int ret = Status::kOk;
  char *index_block_buf = nullptr;
  Slice index_block_content;

  if (UNLIKELY(!handle.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(handle));
  } else if (IS_NULL(index_block_buf = reinterpret_cast<char *>(base_malloc(handle.get_raw_size(), ModId::kExtentReader)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for index block buf", K(ret), K(handle));
  } else if (FAILED(read_and_uncompress_block(handle, nullptr /*aio_handle*/, index_block_buf, index_block_content))) {
    SE_LOG(WARN, "fail to read and uncompress index block", K(ret), K(handle));
  } else if (IS_NULL(index_block = MOD_NEW_OBJECT(ModId::kExtentReader,
                                                  RowBlock,
                                                  index_block_content.data(),
                                                  index_block_content.size(),
                                                  kNoCompression))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for index block", K(ret), K(handle));
  } else {
    // succeed
  }

  // resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(index_block_buf)) {
      base_free(index_block_buf);
      index_block_buf = nullptr;
    }
  }

  return ret;
}

int ExtentReader::read_data_block(const BlockInfo &block_info, AIOHandle *aio_handle, RowBlock *&data_block)
{
  int ret = Status::kOk;
  char *block_buf = nullptr;
  Slice block_content;
  Slice row_block_content;

  if (UNLIKELY(!block_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block_info));
  } else if (IS_NULL(block_buf = reinterpret_cast<char *>(base_malloc(block_info.get_handle().get_raw_size(), ModId::kExtentReader)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for block buf", K(ret), K(block_info));
  } else if (FAILED(read_and_uncompress_block(block_info.get_handle(),
                                              aio_handle,
                                              block_buf,
                                              block_content))) {
    SE_LOG(WARN, "fail to read and uncompress block", K(ret), K(block_info), KP(aio_handle), K(*extent_meta_));
  } else if (block_info.is_columnar_format()) {
    if (FAILED(convert_to_row_block(block_content, block_info, row_block_content))) {
      SE_LOG(WARN, "fail to convert column block to row block", K(ret), K(block_info));
    } else {
      // free the block buf with column format.
      base_free(block_buf);
      block_buf = nullptr;
    }
  } else {
    row_block_content.assign(block_content.data(), block_content.size());
  }

  if (SUCCED(ret)) {
    if (IS_NULL(data_block = MOD_NEW_OBJECT(ModId::kExtentReader,
                                            RowBlock,
                                            row_block_content.data(),
                                            row_block_content.size(),
                                            kNoCompression))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for row block", K(ret));
    }
  }

  // resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(block_buf)) {
      base_free(block_buf);
      block_buf = nullptr;
    }
  }

  return ret;
}

int ExtentReader::read_block(const BlockHandle &handle,
                             util::AIOHandle *aio_handle,
                             char *buf,
                             Slice &block)
{
  int ret = Status::kOk;
  uint32_t actual_checksum = 0; // the checksum actually stored in the block info.
  uint32_t expect_checksum = 0; // the checksum expected based on block data.

  if (UNLIKELY(!handle.is_valid()) || IS_NULL(buf)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(handle), KP(buf));
  } else if (FAILED(extent_->read(aio_handle, handle.get_offset(), handle.get_size(), buf, block))) {
    SE_LOG(WARN, "fail to read block from extent", K(ret), KP(aio_handle), K(handle));
  } else if (UNLIKELY(buf != block.data()) || UNLIKELY(handle.get_size() != static_cast<int64_t>(block.size()))) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "the block maybe corrupt", K(ret), KP(buf), KP(block.data()), K(handle), K(block.size()));
  } else {
    // verify checksum
    actual_checksum = crc32c::Unmask(handle.get_checksum());
    expect_checksum = crc32c::Value(block.data(), block.size());
    if (UNLIKELY(actual_checksum != expect_checksum)) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "the block checksum mismatch", K(ret), K(actual_checksum), K(expect_checksum));
    }
  }

  return ret;
}

int ExtentReader::read_and_uncompress_block(const BlockHandle &handle,
                                            util::AIOHandle *aio_handle,
                                            char *buf,
                                            Slice &block)
{
  int ret = Status::kOk;

  if (UNLIKELY(!handle.is_valid()) || IS_NULL(buf)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(handle), KP(buf));
  } else {
    if (kNoCompression == handle.get_compress_type()) {
      if (FAILED(read_block(handle, aio_handle, buf, block))) {
        SE_LOG(WARN, "fail to read block", K(ret), K(handle));
      }
    } else {
      char *compressed_buf = nullptr;
      Slice compressed_block;
      CompressorHelper compressor_helper;

      if (IS_NULL(compressed_buf = reinterpret_cast<char *>(base_malloc(handle.get_size(), ModId::kExtentReader)))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for compressed buf", K(ret), K(handle));      
      } else if (FAILED(read_block(handle, aio_handle, compressed_buf, compressed_block))) {
        SE_LOG(WARN, "fail to read compressed block", K(ret), K(handle));
      } else if (FAILED(compressor_helper.uncompress(compressed_block,
                                                     handle.get_compress_type(),
                                                     buf,
                                                     handle.get_raw_size(),
                                                     block))) {
        SE_LOG(WARN, "fail to uncompress block", K(ret), K(handle), K(compressed_block));
      }

      if (IS_NOTNULL(compressed_buf)) {
        base_free(compressed_buf);
        compressed_buf = nullptr;
      }
    }
  }

  return ret;
}

int ExtentReader::convert_to_row_block(const Slice &column_block, const BlockInfo &block_info, Slice &row_block)
{
  int ret = Status::kOk;
  RowBlockWriter row_block_writer;
  ColumnBlockIterator column_block_iterator;
  Slice key;
  Slice value;
  Slice tmp_row_block;
  char *row_block_buf = nullptr;
  BlockInfo dummy_block_info;

  if (UNLIKELY(column_block.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(column_block));
  // TODO(Zhao Dongsheng): the restart_interval should use the configed value.
  } else if (FAILED(row_block_writer.init(16 /*restart_interval*/))) {
    SE_LOG(WARN, "fail to init RowBlockIterator", K(ret));
  } else if (FAILED(column_block_iterator.init(column_block, block_info, extent_meta_->table_schema_))) {
    SE_LOG(WARN, "fail to init ColumnBlockIterator", K(ret));
  } else {
    while (SUCCED(ret)) {
      key.clear();
      value.clear();
      if (FAILED(column_block_iterator.get_next_row(key, value))) {
        if (Status::kIterEnd != ret) {
          SE_LOG(WARN, "fail to get next row", K(ret));
        } else {
          /**Overwrite the ret if reach to end of iterator.*/
          ret = Status::kOk;
          break;
        }
      } else if (FAILED(row_block_writer.append(key, value))) {
        SE_LOG(WARN, "fail to append row to row block", K(ret));
      }
    }

    if (SUCCED(ret)) {
      if (FAILED(row_block_writer.build(tmp_row_block, dummy_block_info))) {
        SE_LOG(WARN, "fail to build row block", K(ret));
      } else if (IS_NULL(row_block_buf = reinterpret_cast<char *>(base_malloc(tmp_row_block.size(), ModId::kBlock)))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for row block buf", K(ret), "size", tmp_row_block.size());
      } else {
        memcpy(row_block_buf, tmp_row_block.data(), tmp_row_block.size());
        row_block.assign(row_block_buf, tmp_row_block.size());
      }
    }
  }

  // resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(row_block_buf)) {
      base_free(row_block_buf);
      row_block_buf = nullptr;
    }
  }
  
  return ret;
}

} // namespace table
} // namespace smartengine