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

#include "storage/multi_version_extent_meta_layer.h"
#include "storage/extent_meta_manager.h"
#include "storage/extent_space_manager.h"

namespace smartengine
{
using namespace util;
using namespace common;
using namespace monitor;
namespace storage
{
ExtentLayer::ExtentLayer(const util::Comparator *cmp)
    : cmp_(cmp),
      sequence_number_(0),
      extent_meta_arr_(DEFAULT_EXTENT_META_SIZE, nullptr, memory::ModId::kStorageMgr),
      lob_extent_arr_(DEFAULT_EXTENT_META_SIZE, nullptr, memory::ModId::kStorageMgr),
      extent_stats_()
{
}

ExtentLayer::~ExtentLayer()
{
  destroy();
}

void ExtentLayer::destroy()
{
  extent_meta_arr_.clear();
  lob_extent_arr_.clear();
  extent_stats_.reset();
}

ExtentLayer &ExtentLayer::operator=(const ExtentLayer &extent_layer)
{
  if (this != &extent_layer) {
    sequence_number_ = extent_layer.sequence_number_;
    extent_meta_arr_ = extent_layer.extent_meta_arr_;
    cmp_ = extent_layer.cmp_;
    lob_extent_arr_ = extent_layer.lob_extent_arr_;
    extent_stats_ = extent_layer.extent_stats_;
    for (auto iter = extent_meta_arr_.begin(); iter != extent_meta_arr_.end(); ++iter) {
      (*iter)->ref();
    }
  }

  return *this;
}
int ExtentLayer::add_extent(ExtentMeta *extent_meta, bool sorted)
{
  int ret = Status::kOk;
  int vector_ret = util::E_OK;

  if (IS_NULL(extent_meta)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument, extent meta should not nullptr", K(ret), KP(extent_meta));
  } else {
    if (sorted) {
      util::PointerSortedVector<ExtentMeta *>::iterator insert_iter;
      ExtentMetaCompare extent_meta_compare(cmp_);
      if (util::E_OK != (vector_ret = extent_meta_arr_.insert(extent_meta, insert_iter, extent_meta_compare))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "fail to insert to extent meta arr", K(ret), K(vector_ret), KP(extent_meta), K(*extent_meta));
      }
    } else if (util::E_OK != (vector_ret = extent_meta_arr_.push_back(extent_meta))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to push back to extent meta arr", K(ret), K(vector_ret), KP(extent_meta), K(*extent_meta));
    }

    if (SUCCED(ret)) {
      sequence_number_ = std::max(sequence_number_, extent_meta->largest_seqno_);
      merge_extent_stats(extent_meta, true /*add*/);
      extent_meta->ref();
    }
  }
  return ret;
}

int ExtentLayer::remove_extent(ExtentMeta *extent_meta)
{
  int ret = Status::kOk;
  int vector_ret = util::E_OK;
  ExtentMetaCompare compare_function(cmp_);
  ExtentMetaEqual equal_function;

  if (IS_NULL(extent_meta)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument, extent meta should not nullptr", K(ret), KP(extent_meta));
  } else if (util::E_OK != (vector_ret = extent_meta_arr_.remove_if(extent_meta, compare_function, equal_function))) {
    if (util::E_ENTRY_NOT_EXIST == vector_ret) {
      ret = Status::kEntryNotExist;
    } else {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to remove extent meta from extent meta arr", K(ret), K(vector_ret), KP(extent_meta), K(*extent_meta));
    }
  } else {
    merge_extent_stats(extent_meta, false /*delete*/);
    extent_meta->unref();
  }

  return ret;
}

int ExtentLayer::get_all_extent_ids(util::autovector<ExtentId> &extent_ids) const
{
  return get_extent_ids(extent_meta_arr_, extent_ids);
}

int ExtentLayer::get_all_lob_extent_ids(util::autovector<ExtentId> &extent_ids) const
{
  return get_extent_ids(lob_extent_arr_, extent_ids);
}

int ExtentLayer::get(const common::Slice &key,
                     int64_t level,
                     std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)> save_value,
                     bool &found)
{
  int ret = Status::kOk;
  QUERY_TRACE_BEGIN(TracePoint::EXTENT_LAYER_GET);
  found = false;
  ExtentMetaLargestKeyCompare comparator(cmp_);
  util::PointerSortedVector<ExtentMeta*>::iterator iter = extent_meta_arr_.lower_bound(key, comparator);
  if (extent_meta_arr_.end() != iter) {
    if (FAILED(save_value(*iter, level, found))) {
      if (Status::kNotFound != ret) {
        SE_LOG(WARN, "fail to find extent meta", K(ret));
      }
    }
  }

  QUERY_TRACE_END();
  return ret;
}

int64_t ExtentLayer::get_extent_count(storage::Range &range) const
{
  int64_t count = 0;

  ExtentMetaLargestKeyCompare comparator(cmp_);
  auto start_iter = extent_meta_arr_.lower_bound(range.start_key_, comparator);
  auto end_iter = extent_meta_arr_.lower_bound(range.end_key_, comparator);

  if (start_iter == extent_meta_arr_.end()) {
    count = 0;
  } else if (end_iter == extent_meta_arr_.end()){
    count = end_iter - start_iter;
  } else {
    count = end_iter - start_iter + 1;
  }

  return count; 
}

int ExtentLayer::add_lob_extent(ExtentMeta *extent_meta)
{
  int ret = Status::kOk;
  int vector_ret = util::E_OK;

  if (IS_NULL(extent_meta)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent_meta));
  } else if (util::E_OK != (vector_ret = lob_extent_arr_.push_back(extent_meta))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to push back to lob extent arr", K(ret), K(vector_ret), K(*extent_meta));
  } else {
    SE_LOG(DEBUG, "success to add lob extent to extent layer", K(*extent_meta));
  }

  return ret;
}

int ExtentLayer::remove_lob_extent(ExtentMeta *extent_meta)
{
  int ret = Status::kOk;
  int vector_ret = util::E_OK;
  ExtentMetaCompare compare_function(cmp_);
  ExtentMetaEqual equal_function;

  if (IS_NULL(extent_meta)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent_meta));
  } else if (util::E_OK != (vector_ret = lob_extent_arr_.remove_if(extent_meta, compare_function, equal_function))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to remove extent meta fron lob extent arr", K(ret), K(vector_ret), K(*extent_meta));
  } else {
    SE_LOG(DEBUG, "success to remove lob extent from extent layer", K(*extent_meta));
  }

  return ret;
}

int ExtentLayer::recover_reference_extents()
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  for (auto iter = extent_meta_arr_.begin(); SUCCED(ret) && extent_meta_arr_.end() != iter; ++iter) {
    if (IS_NULL(extent_meta = *iter)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
    } else if (FAILED(ExtentSpaceManager::get_instance().reference(extent_meta->table_space_id_,
                                                                   extent_meta->extent_space_type_,
                                                                   extent_meta->prefix_,
                                                                   extent_meta->extent_id_))) {
      SE_LOG(WARN, "fail to reference extent", K(ret), K(*extent_meta));
    } else {
      SE_LOG(INFO, "success to reference extent", K(*extent_meta));
    }
  }

  return ret;
}
int ExtentLayer::get_extent_ids(const ExtentMetaSortedVector &extent_meta_vector, util::autovector<ExtentId> &extent_ids) const
{
  int ret = Status::kOk;
  int vector_ret = util::E_OK;
  ExtentMeta *extent_meta = nullptr;

  for (auto iter = extent_meta_vector.begin(); SUCCED(ret) && extent_meta_vector.end() != iter; ++iter) {
    if (IS_NULL(extent_meta = *iter)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
    } else {
      extent_ids.push_back(extent_meta->extent_id_);
    }
  }

  return ret;
}

int ExtentLayer::build_extent_meta_arr(const util::autovector<ExtentId> &extent_ids)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  for (uint64_t i = 0; SUCCED(ret) && i < extent_ids.size(); ++i) {
    if (IS_NULL(extent_meta = ExtentMetaManager::get_instance().get_meta(extent_ids.at(i)))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to get extent meta", K(ret), "extent_id", extent_ids.at(i));
    } else if (FAILED(add_extent(extent_meta, true /*sorted*/))) {
      SE_LOG(WARN, "fail to add extent", K(ret), "extent_id", extent_ids.at(i));
    }
  }

  return ret;
}

int ExtentLayer::build_lob_extent_meta_arr(const util::autovector<ExtentId> &extent_ids)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  for (uint64_t i = 0; SUCCED(ret) && i < extent_ids.size(); ++i) {
    if (IS_NULL(extent_meta = ExtentMetaManager::get_instance().get_meta(extent_ids.at(i)))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to get extent meta", K(ret), "extent_id", extent_ids.at(i));
    } else if (FAILED(add_lob_extent(extent_meta))) {
      SE_LOG(WARN, "fail to add lob extent", K(ret), "extent_id", extent_ids.at(i));
    }
  }

  return ret;
}

void ExtentLayer::merge_extent_stats(const ExtentMeta *extent_meta, const bool add)
{
  if (add) {
    extent_stats_.data_size_ += extent_meta->data_size_;
    extent_stats_.num_entries_ += extent_meta->num_entries_;
    extent_stats_.num_deletes_ += extent_meta->num_deletes_;
    extent_stats_.disk_size_ += MAX_EXTENT_SIZE;
  } else {
    extent_stats_.data_size_ -= extent_meta->data_size_;
    extent_stats_.num_entries_ -= extent_meta->num_entries_;
    extent_stats_.num_deletes_ -= extent_meta->num_deletes_;
    extent_stats_.disk_size_ -= MAX_EXTENT_SIZE;
  }
  SE_LOG(DEBUG, "layer extent stats", K_(extent_stats));
}

int ExtentLayer::serialize(char *buf, int64_t buf_length, int64_t &pos) const
{
  int ret = Status::kOk;
  int64_t size = get_serialize_size();
  int64_t version = EXTENT_LAYER_VERSION;
  util::autovector<ExtentId> extent_ids;
  util::autovector<ExtentId> lob_extent_ids;

  if (IS_NULL(buf) || buf_length < 0 || pos >= buf_length) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_length), K(pos));
  } else if (FAILED(get_all_extent_ids(extent_ids))) {
    SE_LOG(WARN, "fail to get all extent ids", K(ret));
  } else if (FAILED(get_all_lob_extent_ids(lob_extent_ids))) {
    SE_LOG(WARN, "fail to get all lob extent ids", K(ret));
  } else {
    *((int64_t *)(buf + pos)) = size;
    pos += sizeof(size);
    *((int64_t *)(buf + pos)) = version;
    pos += sizeof(version);
    if (FAILED(util::serialize(buf, buf_length, pos, sequence_number_))) {
      SE_LOG(WARN, "fail to serialize sequence number", K(ret), K_(sequence_number));
    } else if (FAILED(util::serialize_v(buf, buf_length, pos, extent_ids))) {
      SE_LOG(WARN, "fail to serialize extent ids", K(ret));
    } else if (FAILED(util::serialize_v(buf, buf_length, pos, lob_extent_ids))) {
      SE_LOG(WARN, "fail to serialize lob extent ids", K(ret));
    }
  }

  return ret;
}

int ExtentLayer::deserialize(const char *buf, int64_t buf_length, int64_t &pos)
{
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t version = 0;
  util::autovector<ExtentId> extent_ids;
  util::autovector<ExtentId> lob_extent_ids;

  if (IS_NULL(buf) || buf_length < 0 || pos >= buf_length) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_length), K(pos));
  } else {
    size = *((int64_t *)(buf + pos));
    pos += sizeof(size);
    version = *((int64_t *)(buf + pos));
    pos += sizeof(version);
    if (FAILED(util::deserialize(buf, buf_length, pos, sequence_number_))) {
      SE_LOG(WARN, "fail to deserialize sequence number", K(ret));
    } else if (FAILED(util::deserialize_v(buf, buf_length, pos, extent_ids))) {
      SE_LOG(WARN, "fail to deserialize extent ids", K(ret));
    } else if (FAILED(util::deserialize_v(buf, buf_length, pos, lob_extent_ids))) {
      SE_LOG(WARN, "fail to deserialize lob extent ids", K(ret));
    } else if (FAILED(build_extent_meta_arr(extent_ids))) {
      SE_LOG(WARN, "fail to build extent meta arr", K(ret));
    } else if (FAILED(build_lob_extent_meta_arr(lob_extent_ids))) {
      SE_LOG(WARN, "fail to build lob extent meta arr", K(ret));
    }
  }

  return ret;
}

int64_t ExtentLayer::get_serialize_size() const
{
  int64_t size = 0;
  util::autovector<ExtentId> extent_ids;
  util::autovector<ExtentId> lob_extent_ids;
  get_all_extent_ids(extent_ids);
  get_all_lob_extent_ids(lob_extent_ids);

  size += 2 * sizeof(int64_t); //size and version
  size += util::get_serialize_size(sequence_number_);
  size += util::get_serialize_v_size(extent_ids);
  size += util::get_serialize_v_size(lob_extent_ids);

  return size;
}
ExtentLayerIterator::Compare::Compare(int &ret, const util::Comparator *comparator)
    : result_code_(ret),
      comparator_(comparator)
{
}

bool ExtentLayerIterator::Compare::operator()(const ExtentMeta *extent_meta, const common::Slice &key)
{
  bool bool_ret = false;

  if (nullptr == comparator_ || nullptr == extent_meta) {
    result_code_ = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K_(result_code), KP(extent_meta));
  } else {
    bool_ret = comparator_->Compare(extent_meta->largest_key_.Encode(), key) < 0 ? true : false;
  }

  return bool_ret;
}

ExtentLayerIterator::ExtentLayerIterator()
    : is_inited_(false),
      comparator_(nullptr),
      layer_position_(),
      extent_layer_(nullptr),
      iter_(),
      last_iter_()
{
}

ExtentLayerIterator::~ExtentLayerIterator()
{
}

int ExtentLayerIterator::init(const util::Comparator *comparator,
                              const LayerPosition &layer_position,
                              const ExtentLayer *extent_layer)
{
  int ret = Status::kOk;

  if (is_inited_) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ExtentLayerIterator has been inited", K(ret));
  } else if (IS_NULL(comparator) || !layer_position.is_valid() || IS_NULL(extent_layer)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(comparator), K(layer_position), KP(extent_layer));
  } else {
    comparator_ = comparator;
    layer_position_ = layer_position;
    extent_layer_ = extent_layer;
    is_inited_ = true;
  }

  return ret;
}

bool ExtentLayerIterator::Valid() const
{
  bool valid = is_inited_
               && nullptr != iter_
               && 0 != extent_layer_->extent_meta_arr_.size()
               && iter_ >= extent_layer_->extent_meta_arr_.begin()
               && iter_ != extent_layer_->extent_meta_arr_.end();

  valid &= (0 == end_ikey_.size()) ? true : (iter_ <= last_iter_);
  return valid;
}

void ExtentLayerIterator::SeekToFirst()
{
  int ret = Status::kOk;
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentLayerIterator should been inited first", K(ret));
  } else {
    iter_ = extent_layer_->extent_meta_arr_.begin();
  }
}

void ExtentLayerIterator::SeekToLast()
{
  int ret = Status::kOk;
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentLayerIterator should been inited first", K(ret));
  } else {
    iter_ = extent_layer_->extent_meta_arr_.last();
  }
}

void ExtentLayerIterator::Seek(const common::Slice &target)
{
  int ret = Status::kOk;
  Compare compare(ret, comparator_);
  iter_ = std::lower_bound(extent_layer_->extent_meta_arr_.begin(), extent_layer_->extent_meta_arr_.end(), target, compare);
  if (0 < end_ikey_.size()) {
    last_iter_ = std::lower_bound(extent_layer_->extent_meta_arr_.begin(),
                                  extent_layer_->extent_meta_arr_.end(),
                                  end_ikey_,
                                  compare);
  } else {
    last_iter_ = extent_layer_->extent_meta_arr_.last();
  }
  is_boundary_ = (iter_ == last_iter_);
}

void ExtentLayerIterator::SeekForPrev(const common::Slice &target)
{
  SE_LOG(WARN, "ExtentLayerIterator not support SeekForPrev");
}

void ExtentLayerIterator::Next()
{
  int ret = Status::kOk;
  if (!is_inited_) {
    SE_LOG(WARN, "ExtentLayerIterator should been inited first", K(ret));
  } else {
    ++iter_;
    is_boundary_ = (iter_ == last_iter_);
  }
}

void ExtentLayerIterator::Prev()
{
  int ret = Status::kOk;
  if (!is_inited_) {
    SE_LOG(WARN, "ExtentLayerIterator should been inited first", K(ret));
  } else {
    --iter_;
  }
}

//return extent meta serialize buf
common::Slice ExtentLayerIterator::key() const
{
  int ret = Status::kOk;
  common::Slice key_slice;
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentLayerIterator should been inited first", K(ret));
  } else {
    key_slice = common::Slice(reinterpret_cast<char*>(*iter_), sizeof(**iter_));
  }
  return key_slice;
}

//TODO:yuanfeng
//return extent id buf
common::Slice ExtentLayerIterator::value() const
{
  int ret = Status::kOk;
  common::Slice value_slice;
  if (!is_inited_) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentLayerIterator should been inited first", K(ret));
  } else {
     value_slice = common::Slice(reinterpret_cast<char *>(&((*iter_)->extent_id_)), sizeof((*iter_)->extent_id_));
  }
  return value_slice;
}

common::Status ExtentLayerIterator::status() const
{
  return common::Status::OK();
}

ExtentLayerVersion::ExtentLayerVersion(int32_t level, const util::Comparator *cmp)
    : refs_(0),
      level_(level),
      cmp_(cmp),
      extent_layer_arr_(DEFAULT_EXTENT_LAYER_COUNT, nullptr, memory::ModId::kStorageMgr),
      dump_extent_layer_(nullptr),
      extent_stats_()
{
}

ExtentLayerVersion::~ExtentLayerVersion()
{
  destroy();
}

void ExtentLayerVersion::destroy()
{
  for (int64_t i = 0; i < extent_layer_arr_.size(); ++i) {
    MOD_DELETE_OBJECT(ExtentLayer, extent_layer_arr_.at(i));
  }
  extent_layer_arr_.clear();
  MOD_DELETE_OBJECT(ExtentLayer, dump_extent_layer_);
  extent_stats_.reset();
}

ExtentLayer *ExtentLayerVersion::get_extent_layer(int64_t index)
{
  ExtentLayer *extent_layer = nullptr;
  if (LayerPosition::INVISIBLE_LAYER_INDEX == index) {
    extent_layer = dump_extent_layer_;
  } else if (index < 0 || index >= extent_layer_arr_.size()) {
    //out of range
  } else {
    extent_layer = extent_layer_arr_.at(index); 
  }

  return extent_layer;
}

int64_t ExtentLayerVersion::get_total_normal_extent_count() const
{
  int64_t size = 0;

  for (int64_t i = 0; i < extent_layer_arr_.size(); ++i) {
    size += extent_layer_arr_[i]->extent_meta_arr_.size();
  }

  return size;
}

int64_t ExtentLayerVersion::get_extent_count(storage::Range &range) const
{
  int64_t extent_count = 0;
  for (int64_t i = 0; i < extent_layer_arr_.size(); ++i) {
    extent_count += extent_layer_arr_[i]->get_extent_count(range);
  }

  return extent_count;
}

int64_t ExtentLayerVersion::get_total_extent_size() const
{
  int64_t size = 0;
  size += get_total_normal_extent_count();
  if (0 == level_ && nullptr != dump_extent_layer_) {
    size += dump_extent_layer_->extent_meta_arr_.size();
  }
  return size;
}

int64_t ExtentLayerVersion::get_layers_extent_size(const int64_t layer_num) const
{
  int64_t size = 0;
  int64_t len = std::min(layer_num, (int64_t)extent_layer_arr_.size());
  for (int64_t i = 0; i < len; ++i) {
    size += extent_layer_arr_[i]->extent_meta_arr_.size();
  }
  return size;
}

int ExtentLayerVersion::get_all_extent_ids(util::autovector<ExtentId> &extent_ids) const
{
  int ret = Status::kOk;
  ExtentLayer *extent_layer = nullptr;

  for (int64_t i = 0; SUCCED(ret) && i < extent_layer_arr_.size(); ++i) {
    if (IS_NULL(extent_layer = extent_layer_arr_[i])) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent layer must not nullptr", K(ret), K(i));
    } else if (FAILED(extent_layer->get_all_extent_ids(extent_ids))) {
      SE_LOG(WARN, "fail to get all extent ids from extent layer", K(ret), K(i));
    }
  }

  return ret;
}

int ExtentLayerVersion::get_extent_positions(const int64_t index_id, ExtentPositionMap &extent_positions) const
{
  int ret = Status::kOk;
  ExtentLayer *extent_layer = nullptr;
  ExtentMeta *extent_meta = nullptr;
  ExtentPosition extent_position;
  

  for (int64_t layer_index = 0; SUCCED(ret) && layer_index < extent_layer_arr_.size(); ++layer_index) {
    if (IS_NULL(extent_layer = extent_layer_arr_[layer_index])) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent layer must not nullptr", K(ret), K(layer_index));
    } else {
      for (auto iter = extent_layer->extent_meta_arr_.begin(); SUCCED(ret) && extent_layer->extent_meta_arr_.end() != iter; ++iter) {
        if (IS_NULL(extent_meta = *iter)) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
        } else {
          extent_position.set(index_id, LayerPosition(level_, layer_index), extent_meta->extent_id_);
          if (!(extent_positions.emplace(extent_meta->extent_id_.id(), extent_position).second)) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN, "fail to push back extent info", K(ret), K(extent_position));
          }
        }
      }
    }
  }

  if (0 == level_ && nullptr != dump_extent_layer_) {
    for (auto iter = dump_extent_layer_->extent_meta_arr_.begin(); SUCCED(ret) && dump_extent_layer_->extent_meta_arr_.end() != iter; ++iter) {
      if (IS_NULL(extent_meta = *iter)) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
      } else {
        extent_position.set(index_id, LayerPosition(0, LayerPosition::INVISIBLE_LAYER_INDEX), extent_meta->extent_id_);
        if (!(extent_positions.emplace(extent_meta->extent_id_.id(), extent_position).second)) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "fail to push back extent info", K(ret), K(extent_position));
        }
      }
    }
  }

  return ret;
}

int64_t ExtentLayerVersion::get_largest_sequence_number() const
{
  int64_t largest_sequence_number = 0;
  if (0 == extent_layer_arr_.size()) {
    //empty extent layer version, do nothing
  } else {
    largest_sequence_number = (*extent_layer_arr_.last())->sequence_number_;
  }
  return largest_sequence_number;
}

int ExtentLayerVersion::add_layer(ExtentLayer *extent_layer)
{
  int ret = Status::kOk;
  int vector_ret = util::E_OK;
  ExtentLayerCompare comparator;
  util::PointerSortedVector<ExtentLayer *>::iterator insert_iter;

  if (nullptr == extent_layer) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret));
  } else if (util::E_OK != (vector_ret = extent_layer_arr_.insert(extent_layer, insert_iter, comparator))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to push back extent layer", K(ret), K(vector_ret), KP(extent_layer));
  } else {
    merge_extent_stats(extent_layer->extent_stats_);
  }

  return ret;
}

int ExtentLayerVersion::get(const common::Slice &key,
                            std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)> save_value,
                            bool &found) const
{
  int ret = Status::kOk;
  ExtentLayer *extent_layer = nullptr;
  found = false;

  if (0 == extent_layer_arr_.size()) {
    //do nothing
  } else {
    for (int64_t i = extent_layer_arr_.size() - 1; SUCCED(ret) && !found && i >= 0; --i) {
      if (nullptr == (extent_layer = extent_layer_arr_.at(i))) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "unexpected error, extent layer must not nullptr", K(ret), K(i));
      } else if (FAILED(extent_layer->get(key, level_, save_value, found))) {
        if (Status::kNotFound != ret) {
          SE_LOG(WARN, "fail to get from extent layer", K(ret), K(i));
        }
      }
      SE_LOG(DEBUG, "get from extent layer", KP(this), KP(extent_layer), K(i));
    }
  }

  return ret;
}

int ExtentLayerVersion::recover_reference_extents()
{
  int ret = Status::kOk;
  ExtentLayer *extent_layer = nullptr;

  for (int64_t layer_index = 0; SUCCED(ret) && layer_index < extent_layer_arr_.size(); ++layer_index) {
    if (IS_NULL(extent_layer = extent_layer_arr_.at(layer_index))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "unexpected error, extent layer must not nullptr", K(ret), K(layer_index));
    } else if (FAILED(extent_layer->recover_reference_extents())) {
      SE_LOG(WARN, "fail to recover reference extents for ExtentLayer", K(ret), K_(level), K(layer_index));
    } else {
      SE_LOG(INFO, "success to recover reference extents for ExtentLayer", K_(level), K(layer_index));
    }
  }

  if (nullptr != dump_extent_layer_) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, dump extent layer should transform to level 0", K(ret), K_(level));
  }

  return ret;
}

void ExtentLayerVersion::merge_extent_stats(const ExtentStats &extent_stats)
{
  extent_stats_.merge(extent_stats);
}
int ExtentLayerVersion::serialize(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = Status::kOk;
  int64_t size = get_serialize_size();
  int64_t version = EXTENT_LAYER_VERSION_VERSION;
  int64_t extent_layer_count = extent_layer_arr_.size();
  int64_t dump_extent_layer_count = (nullptr == dump_extent_layer_) ? 0 : 1;
  const ExtentLayer *extent_layer = nullptr;
  util::autovector<ExtentId> extent_ids;

  if (IS_NULL(buf) || buf_len < 0 || pos + size > buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos), K(size));
  } else {
    *((int64_t *)(buf + pos)) = size;
    pos += sizeof(size);
    *((int64_t *)(buf + pos)) = version;
    pos += sizeof(version);
    if (FAILED(util::serialize(buf, buf_len, pos, level_))) {
      SE_LOG(WARN, "fail to serialize level", K(ret), K_(level));
    } else if (FAILED(util::serialize(buf, buf_len, pos, extent_layer_count))) {
      SE_LOG(WARN, "fail to serialize extent layer count", K(ret), K(extent_layer_count));
    } else {
      //serialize normal extent layer
      for (int64_t layer_index = 0; SUCCED(ret) && layer_index < extent_layer_count; ++layer_index) {
        if (IS_NULL(extent_layer = extent_layer_arr_.at(layer_index))) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, extent layer must not nullptr", K(ret), K(layer_index));
        } else if (FAILED(extent_layer->serialize(buf, buf_len, pos))) {
          SE_LOG(WARN, "fail to serialize the extent layer", K(ret));
        }
      }
    }

    if (SUCCED(ret)) {
      //serialize dump extent layer
      if (FAILED(util::serialize(buf, buf_len, pos, dump_extent_layer_count))) {
        SE_LOG(WARN, "fail to serialize dump extent layer count", K(ret), K(dump_extent_layer_count));
      } else if (nullptr != dump_extent_layer_ && FAILED(dump_extent_layer_->serialize(buf, buf_len, pos))) {
        SE_LOG(WARN, "fail to serialize dump extent layer", K(ret));
      }
    }
  }

  return ret;
}

int ExtentLayerVersion::deserialize(const char *buf, int64_t buf_length, int64_t &pos)
{
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t version = 0;
  int64_t extent_layer_count = 0;
  int64_t dump_extent_layer_count =  0;
  ExtentLayer *extent_layer = nullptr;

  if (IS_NULL(buf) || buf_length < 0 || pos >= buf_length) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_length), K(pos));
  } else {
    size = *((int64_t *)(buf + pos));
    pos += sizeof(size);
    version = *((int64_t *)(buf + pos));
    pos += sizeof(version);
    if (FAILED(util::deserialize(buf, buf_length, pos, level_))) {
      SE_LOG(WARN, "fail to deserialize level", K(ret));
    } else if (FAILED(util::deserialize(buf, buf_length, pos, extent_layer_count))) {
      SE_LOG(WARN, "fail to deserialize extent layer count", K(ret));
    } else {
      //deserialize normal extent layer
      if (extent_layer_count > 0) {
        //clear inited extent layer
        extent_layer_arr_.clear();
      }
      for (int64_t layer_index = 0; SUCCED(ret) && layer_index < extent_layer_count; ++layer_index) {
        if (IS_NULL(extent_layer = MOD_NEW_OBJECT(memory::ModId::kStorageMgr, ExtentLayer, cmp_))) {
          ret = Status::kMemoryLimit;
          SE_LOG(WARN, "fail to allocate memory for ExtentLayer", K(ret), K(layer_index), K(extent_layer_count));
        } else if (FAILED(extent_layer->deserialize(buf, buf_length, pos))) {
          SE_LOG(WARN, "fail to deserialize ExtentLayer", K(ret), K(layer_index), K(extent_layer_count));
        } else if (FAILED(add_layer(extent_layer))) {
          SE_LOG(WARN, "fail to add layer", K(ret), K(layer_index), K(extent_layer_count));
        }
      }
    }

    //deserialize dump extent layer
    if (SUCCED(ret)) {
      if (FAILED(util::deserialize(buf, buf_length, pos, dump_extent_layer_count))) {
        SE_LOG(WARN, "fail to deserialize dump extent layer count", K(ret));
      } else {
        if (1 == dump_extent_layer_count) {
          if (IS_NULL(dump_extent_layer_ = MOD_NEW_OBJECT(memory::ModId::kStorageMgr, ExtentLayer, cmp_))) {
            ret = Status::kMemoryLimit;
            SE_LOG(WARN, "fail to allocate memory for dump ExtentLayer", K(ret));
          } else if (FAILED(dump_extent_layer_->deserialize(buf, buf_length, pos))) {
            SE_LOG(WARN, "fail to deserialize dump extent layer", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int64_t ExtentLayerVersion::get_serialize_size() const
{
  int64_t size = 0;
  int64_t extent_layer_count = extent_layer_arr_.size();
  int64_t dump_extent_layer_count = (nullptr == dump_extent_layer_) ? 0 : 1;

  size += 2 * sizeof(int64_t); //size and version
  size += util::get_serialize_size(level_);
  size += util::get_serialize_size(extent_layer_count);
  for (int64_t layer_index = 0; layer_index < extent_layer_count; ++layer_index) {
    size += extent_layer_arr_.at(layer_index)->get_serialize_size();
  }
  size += util::get_serialize_size(dump_extent_layer_count);
  if (1 == dump_extent_layer_count) {
    size += dump_extent_layer_->get_serialize_size();
  }

  return size;
}

} // namespace storage
} // namespace smartengine
