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


#include "reuse_block_merge_iterator.h"
#include "compaction.h"
#include "db/compaction_iterator.h"
#include "db/merge_helper.h"
#include "meta_data.h"
#include "table/merging_iterator.h"
#include "util/arena.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "smartengine/options.h"
#include "smartengine/table.h"
#include "smartengine/se_constants.h"

using namespace smartengine;
using namespace common;
using namespace db;
using namespace table;
using namespace storage;
using namespace util;

class MetaDataMockIterator : public table::InternalIterator {
 public:
  static const int64_t MAX_SIZE = 4096;
  MetaDataMockIterator() {
    iter_pos_ = -1;
    data_end_ = 0;
    buf_ = (char *)::malloc(MAX_SIZE);
    size_ = MAX_SIZE;
  }
  ~MetaDataMockIterator() {
    if (nullptr != buf_) {
      ::free((void *)buf_);
    }
    buf_ = nullptr;
  }

  int add(const Slice &key_in, const ExtentMetaValue &value_in) {
    int r = util::serialize(buf_, size_, data_end_, key_in);
    if (r) return r;
    r = util::serialize(buf_, size_, data_end_, value_in);
    return r;
  }

  bool Valid() const { return iter_pos_ >= 0 && iter_pos_ <= data_end_; }

  Status status() const { return Status::OK(); }

  Slice key() const { return key_; }
  Slice value() const { return value_; }
  const ExtentMetaValue &meta_value() const { return meta_value_; }

  void Next() {
    if (iter_pos_ >= data_end_) {
      iter_pos_ = data_end_ + 1;
      return;
    }
    util::deserialize(buf_, data_end_, iter_pos_, key_);
    value_.data_ = buf_ + iter_pos_;
    util::deserialize(buf_, data_end_, iter_pos_, meta_value_);
    value_.size_ = meta_value_.get_serialize_size();
  }

  void Prev() {
    // not implements;
  }

  void Seek(const Slice &key_in) {
    const Comparator *comp = BytewiseComparator();
    while (Valid()) {
      if (comp->Compare(key_in, key_) <= 0) break;
      Next();
    }
  }

  void SeekForPrev(const Slice &key_in) {}

  void SeekToFirst() {
    iter_pos_ = 0;
    Next();
  }

  void SeekToLast() { iter_pos_ = data_end_; }

 private:
  char *buf_;
  int32_t size_;
  int64_t data_end_;
  int64_t iter_pos_;
  Slice key_;
  Slice value_;
  ExtentMetaValue meta_value_;
};

static const int64_t KEY_NUM = 7;
static const char *gsk[KEY_NUM] = {"a1",    "b1",  "comb", "deflat",
                                   "ghost", "jit", "oops"};
static const char *gek[KEY_NUM] = {"a11",       "cab",  "decide", "deserialize",
                                   "ghost_xxx", "love", "pick"};

void build_mock_iterator(MetaDataMockIterator &iter) {
  for (int64_t i = 0; i < KEY_NUM; ++i) {
    ExtentMetaValue mv;
    mv.set_start_key(Slice(gsk[i]));
    mv.set_extent_list(std::make_pair(1, 1));
    iter.add(Slice(gek[i]), mv);
  }
}

void build_mock_iterator(MetaDataMockIterator &iter,
                         const storage::Range *range, const int64_t num) {
  for (int64_t i = 0; i < num; ++i) {
    ExtentMetaValue mv;
    mv.set_start_key(range[i].start_key_);
    mv.set_extent_list(std::make_pair(1, 1));
    iter.add(range[i].end_key_, mv);
  }
}

void add_mock_iterator(MetaDataMockIterator &iter, const storage::Range &range,
                       const int64_t first, const int64_t second) {
  ExtentMetaValue mv;
  mv.set_start_key(range.start_key_);
  mv.set_extent_list(std::make_pair(first, second));
  iter.add(range.end_key_, mv);
}

TEST(RangeIteratorWrapper, scan_mock_iterator) {
  MetaDataMockIterator miter;
  build_mock_iterator(miter);
  miter.SeekToFirst();
  int64_t i = 0;
  while (miter.Valid()) {
    /*
    printf("key:%.*s, value:%.*s, extent[%ld,%ld]\n", (int)miter.key().size_,
           miter.key().data_, (int)miter.meta_value().get_start_key().size_,
           miter.meta_value().get_start_key().data_,
           miter.meta_value().get_extent_list().first,
           miter.meta_value().get_extent_list().second);
           */
    ASSERT_EQ(0, BytewiseComparator()->Compare(miter.key(), Slice(gek[i])));
    ASSERT_EQ(0, BytewiseComparator()->Compare(
                     miter.meta_value().get_start_key(), Slice(gsk[i])));
    miter.Next();
    ++i;
  }
  ASSERT_EQ(i, KEY_NUM);
}

TEST(RangeIteratorWrapper, range_scan_iterator) {
  MetaDataMockIterator miter;
  build_mock_iterator(miter);
  MetaType type;
  MetaValueRangeIterator riter(type, &miter);
  riter.seek_to_first();
  int64_t i = 0;
  while (riter.valid()) {
    printf("key:%.*s\n", (int)riter.key().size_, riter.key().data_);
    riter.next();
    ++i;
  }
}

TEST(RangeIteratorWrapper, general_two_way) {
  storage::Range range1[] = {
      {"aa", "bb"}, {"bk", "deflat"}, {"float", "jeff"}, {"kick", "qsort"}};
  storage::Range range2[] = {
      {"aac", "bz"}, {"cipher", "deflat"}, {"jeff", "kick"}, {"kick", "qsort"}};
  MetaDataMockIterator miter1;
  build_mock_iterator(miter1, range1, 4);
  MetaDataMockIterator miter2;
  build_mock_iterator(miter2, range2, 4);
  MetaValueRangeIterator *iter1 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 0, 0, 0), &miter1);
  MetaValueRangeIterator *iter2 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 1, 1), &miter2);
  RangeIterator *iters[] = {iter1, iter2};
  util::Arena arena;
  ReuseBlockMergeIterator rbiter(arena, *BytewiseComparator());
  rbiter.set_children(iters, 2);
  rbiter.seek_to_first();
  while (rbiter.valid()) {
    ASSERT_EQ(4, (int32_t)rbiter.get_output().size());
    for (int32_t i = 0; i < (int32_t)rbiter.get_output().size(); ++i) {
      const MetaDescriptor &ds = rbiter.get_output()[i];
      printf("output[%d]:%s\n", i, util::to_cstring(ds));
    }
    rbiter.next();
  }
}

TEST(RangeIteratorWrapper, disjoint_range) {
  storage::Range range1[] = {
      {"aa", "bb"}, {"bbb", "cc"}, {"dd", "ee"}, {"ff", "gg"}};
  storage::Range range2[] = {
      {"hh", "hhh"}, {"ii", "jj"}, {"kk", "ll"}, {"mm", "nn"}};
  storage::Range range3[] = {
      {"oo", "pp"}, {"qq", "rr"}, {"rrrr", "ssss"}, {"tttt", "uuuu"}};
  storage::Range range4[] = {{"vvv", "www"}, {"xxx", "zzz"}};
  MetaDataMockIterator miter1;
  build_mock_iterator(miter1, range1, 4);
  MetaDataMockIterator miter2;
  build_mock_iterator(miter2, range2, 4);
  MetaDataMockIterator miter3;
  build_mock_iterator(miter3, range3, 4);
  MetaDataMockIterator miter4;
  build_mock_iterator(miter4, range4, 2);
  MetaValueRangeIterator *iter1 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 0, 0, 10), &miter1);
  MetaValueRangeIterator *iter2 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 1, 11), &miter2);
  MetaValueRangeIterator *iter3 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 2, 12), &miter3);
  MetaValueRangeIterator *iter4 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 3, 13), &miter4);
  RangeIterator *iters[] = {iter1, iter2, iter3, iter4};
  util::Arena arena;
  ReuseBlockMergeIterator rbiter(arena, *BytewiseComparator());
  rbiter.set_children(iters, 4);
  rbiter.seek_to_first();
  while (rbiter.valid()) {
    ASSERT_EQ(1, (int32_t)rbiter.get_output().size());
    for (int32_t i = 0; i < (int32_t)rbiter.get_output().size(); ++i) {
      const MetaDescriptor &ds = rbiter.get_output()[i];
      printf("output[%d]:%s\n", i, util::to_cstring(ds));
    }
    rbiter.next();
  }
}

TEST(RangeIteratorWrapper, general_multi_way) {
  storage::Range range1[] = {
      {"aa", "bb"}, {"bk", "deflat"}, {"float", "jeff"}, {"kick", "qsort"}};
  storage::Range range2[] = {
      {"aac", "bz"}, {"cipher", "deflat"}, {"jeff", "kick"}, {"kick", "qsort"}};
  storage::Range range3[] = {{"aac", "google"},
                             {"right", "sark"},
                             {"tight", "victory"},
                             {"zebra", "zzzz"}};
  storage::Range range4[] = {{"fine", "quick"}, {"small", "window"}};
  MetaDataMockIterator miter1;
  build_mock_iterator(miter1, range1, 4);
  MetaDataMockIterator miter2;
  build_mock_iterator(miter2, range2, 4);
  MetaDataMockIterator miter3;
  build_mock_iterator(miter3, range3, 4);
  MetaDataMockIterator miter4;
  build_mock_iterator(miter4, range4, 2);
  MetaValueRangeIterator *iter1 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 0, 0, 10), &miter1);
  MetaValueRangeIterator *iter2 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 1, 11), &miter2);
  MetaValueRangeIterator *iter3 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 2, 12), &miter3);
  MetaValueRangeIterator *iter4 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 3, 13), &miter4);
  RangeIterator *iters[] = {iter1, iter2, iter3, iter4};
  util::Arena arena;
  ReuseBlockMergeIterator rbiter(arena, *BytewiseComparator());
  rbiter.set_children(iters, 4);
  rbiter.seek_to_first();
  int64_t batchs[] = {10, 1, 2, 1};
  int64_t batch = 0;
  while (rbiter.valid()) {
    ASSERT_EQ(batchs[batch], (int64_t)rbiter.get_output().size());
    for (int32_t i = 0; i < (int32_t)rbiter.get_output().size(); ++i) {
      const MetaDescriptor &ds = rbiter.get_output()[i];
      printf("output[%d]:%s\n", i, util::to_cstring(ds));
    }
    rbiter.next();
    ++batch;
  }
}

TEST(RangeIteratorWrapper, continus_range) {
  storage::Range range1[] = {
      {"aa", "bb"}, {"cipher", "deflat"}, {"float", "jeff"}, {"kick", "qsort"}};
  storage::Range range2[] = {
      {"aac", "bz"}, {"bzz", "deflat"}, {"deflat", "kick"}, {"kick", "qsort"}};
  MetaDataMockIterator miter1;
  build_mock_iterator(miter1, range1, 4);
  MetaDataMockIterator miter2;
  build_mock_iterator(miter2, range2, 4);
  MetaValueRangeIterator *iter1 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 0, 0, 0), &miter1);
  MetaValueRangeIterator *iter2 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 1, 1), &miter2);
  RangeIterator *iters[] = {iter1, iter2};
  util::Arena arena;
  ReuseBlockMergeIterator rbiter(arena, *BytewiseComparator());
  rbiter.set_children(iters, 2);
  rbiter.seek_to_first();
  int64_t batchs[] = {2, 6};
  int64_t batch = 0;
  while (rbiter.valid()) {
    ASSERT_EQ(batchs[batch], (int64_t)rbiter.get_output().size());
    for (int32_t i = 0; i < (int32_t)rbiter.get_output().size(); ++i) {
      const MetaDescriptor &ds = rbiter.get_output()[i];
      printf("output[%d]:%s\n", i, util::to_cstring(ds));
    }
    rbiter.next();
    ++batch;
  }
}

TEST(RangeIteratorWrapper, continus_range2) {
  storage::Range range1[] = {{"aa", "bb"},
                             {"cipher", "deaa"},
                             {"deaa", "deabc"},
                             {"float", "jeff"},
                             {"kick", "qsort"}};
  storage::Range range2[] = {
      {"aac", "bz"}, {"bzz", "deflat"}, {"deflat", "kick"}, {"kick", "qsort"}};
  MetaDataMockIterator miter1;
  build_mock_iterator(miter1, range1, 5);
  MetaDataMockIterator miter2;
  build_mock_iterator(miter2, range2, 4);
  MetaValueRangeIterator *iter1 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 0, 0, 0), &miter1);
  MetaValueRangeIterator *iter2 =
      new MetaValueRangeIterator(MetaType(0, 0, 1, 1, 1, 1), &miter2);
  RangeIterator *iters[] = {iter1, iter2};
  util::Arena arena;
  ReuseBlockMergeIterator rbiter(arena, *BytewiseComparator());
  rbiter.set_children(iters, 2);
  rbiter.seek_to_first();
  int64_t batchs[] = {2, 7};
  int64_t batch = 0;
  while (rbiter.valid()) {
    ASSERT_EQ(batchs[batch], (int64_t)rbiter.get_output().size());
    for (int32_t i = 0; i < (int32_t)rbiter.get_output().size(); ++i) {
      const MetaDescriptor &ds = rbiter.get_output()[i];
      printf("output[%d]:%s\n", i, util::to_cstring(ds));
    }
    rbiter.next();
    ++batch;
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
	smartengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
