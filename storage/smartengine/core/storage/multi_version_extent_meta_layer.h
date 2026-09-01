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

#ifndef SMARTENGINE_INCLUDE_EXTENT_META_H_
#define SMARTENGINE_INCLUDE_EXTENT_META_H_

#include "table/internal_iterator.h"
#include "util/autovector.h"
#include "util/pointer_vector.h"
#include "storage/storage_meta_struct.h"
#include "compact/range_iterator.h"

namespace smartengine
{
namespace storage
{
struct ExtentLayer
{
public:
  typedef util::PointerSortedVector<ExtentMeta*> ExtentMetaSortedVector;
public:
  ExtentLayer(const util::Comparator *cmp);
  ~ExtentLayer();
  void destroy();

  ExtentLayer &operator=(const ExtentLayer &extent_layer);
  bool is_empty() const { return 0 == extent_meta_arr_.size(); }
  int add_extent(ExtentMeta *extent_meta, bool sorted);
  int remove_extent(ExtentMeta *extent_meta);
  int get_all_extent_ids(util::autovector<ExtentId> &extent_ids) const;
  int get_all_lob_extent_ids(util::autovector<ExtentId> &extent_ids) const;
  int get(const common::Slice &key,
          int64_t level,
          std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)> save_value,
          bool &found);
  int64_t get_extent_count(storage::Range &range) const;
  int add_lob_extent(ExtentMeta *extent_meta);
  int remove_lob_extent(ExtentMeta *extent_meta);
  int recover_reference_extents();
  
  int deserialize_and_dump(const char *buf, int64_t buf_len, int64_t &pos,
                           char *str_buf, int64_t str_buf_len, int64_t &str_pos);
  DECLARE_SERIALIZATION()
public:
  static const int64_t DEFAULT_EXTENT_META_SIZE = 16;
  struct ExtentMetaCompare
  {
    ExtentMetaCompare(const util::Comparator *cmp) : cmp_(cmp) {}
    bool operator() (const ExtentMeta *left, const ExtentMeta *right)
    {
      return cmp_->Compare(left->largest_key_.Encode(), right->largest_key_.Encode()) < 0;
    }

    const util::Comparator *cmp_;
  };
  struct ExtentMetaEqual
  {
    ExtentMetaEqual() {}
    bool operator() (const ExtentMeta *left, const ExtentMeta *right)
    {
      return left == right && left->extent_id_.is_equal(right->extent_id_);
    }
  };
  struct ExtentMetaLargestKeyCompare
  {
    ExtentMetaLargestKeyCompare(const util::Comparator *cmp) : cmp_(cmp) {}
    bool operator() (const ExtentMeta *meta, const common::Slice &key)
    {
      return cmp_->Compare(meta->largest_key_.Encode(), key) < 0;
    }

    const util::Comparator *cmp_;
  };

  int get_extent_ids(const ExtentMetaSortedVector &extent_meta_vector, util::autovector<ExtentId> &extent_ids) const;
  int build_extent_meta_arr(const util::autovector<ExtentId> &extent_ids);
  int build_lob_extent_meta_arr(const util::autovector<ExtentId> &extent_ids);
  void merge_extent_stats(const ExtentMeta *extent_meta, const bool add);

public:
  static const int64_t EXTENT_LAYER_VERSION = 1;

  const util::Comparator *cmp_;
  common::SequenceNumber sequence_number_; //the largest data seq of kv int extent layer
  ExtentMetaSortedVector extent_meta_arr_;
  ExtentMetaSortedVector lob_extent_arr_; // just for dump extent layer
  ExtentStats extent_stats_;
};

class ExtentLayerIterator : public table::InternalIterator
{
public:
  ExtentLayerIterator();
  virtual ~ExtentLayerIterator() override;

  int init(const util::Comparator *comparator,
           const LayerPosition &layer_position,
           const ExtentLayer *extent_layer);
  virtual bool Valid() const override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Seek(const common::Slice &target) override;
  virtual void SeekForPrev(const common::Slice &target) override;
  virtual void Next() override;
  virtual void Prev() override;
  virtual common::Slice key() const override;
  virtual common::Slice value() const override;
  virtual common::Status status() const override;
  const LayerPosition &get_layer_position() { return layer_position_; }

private:
  class Compare
  {
  public:
    Compare(int &ret, const util::Comparator *comparator);
    bool operator()(const ExtentMeta *extent_meta, const common::Slice &key);
  private:
    int &result_code_;
    const util::Comparator *comparator_;
  };

  static const int64_t DEFAULT_BUF_SIZE = 1 * 1024; //1KB
private:
  bool is_inited_;
  const util::Comparator *comparator_;
  LayerPosition layer_position_;
  const ExtentLayer *extent_layer_;
  util::PointerSortedVector<ExtentMeta *>::iterator iter_;
  util::PointerSortedVector<ExtentMeta *>::iterator last_iter_;
};

class ExtentLayerVersion
{
public:
  ExtentLayerVersion(const int32_t level, const util::Comparator *cmp);
  ~ExtentLayerVersion();
  void destroy();

  void ref() { ++refs_; }
  bool unref() { return 0 == --refs_ ? true : false; }
  int32_t get_level() const { return level_; }
  int64_t get_extent_layer_size() const { return extent_layer_arr_.size(); }
  int64_t get_total_normal_extent_count() const;
  //include dump extent
  int64_t get_total_extent_size() const ;
  int64_t get_layers_extent_size(const int64_t layer_num) const ;
  int get_all_extent_ids(util::autovector<ExtentId> &extent_ids) const;
  int get_extent_positions(const int64_t index_id, ExtentPositionMap &extent_positions) const;
  int64_t get_largest_sequence_number() const;
  ExtentLayer *get_extent_layer(int64_t index);
  int get(const common::Slice &key, std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)>, bool &found) const;
  int64_t get_extent_count(storage::Range &range) const;
  int add_layer(ExtentLayer *extent_layer);
  int recover_reference_extents();
  ExtentStats get_extent_stats() const { return extent_stats_; }
  void merge_extent_stats(const ExtentStats &extent_stats);
  DECLARE_SERIALIZATION()
public:
  static const int64_t DEFAULT_EXTENT_LAYER_COUNT = 1;
  struct ExtentLayerCompare
  {
    ExtentLayerCompare() {}
    bool operator() (const ExtentLayer *left, const ExtentLayer *right)
    {
      return left->sequence_number_ < right->sequence_number_;
    }
  };
public:
  static const int64_t EXTENT_LAYER_VERSION_VERSION = 1;
  int32_t refs_;
  int32_t level_;
  const util::Comparator *cmp_;
  util::PointerSortedVector<ExtentLayer *> extent_layer_arr_;
  ExtentLayer *dump_extent_layer_;
  ExtentStats extent_stats_;
};

} // namespace storage
} // namespace smartengine

#endif
