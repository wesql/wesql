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

#include "table/block_struct.h"
#include "util/testharness.h"

namespace smartengine
{
using namespace common;

namespace table
{
TEST(BlockHandleTest, general_function)
{
  BlockHandle block_handle;

  ASSERT_EQ(0, block_handle.get_offset());
  ASSERT_EQ(0, block_handle.get_size());
  ASSERT_EQ(0, block_handle.get_raw_size());
  ASSERT_EQ(0, block_handle.get_checksum());
  ASSERT_EQ(kNoCompression, block_handle.get_compress_type());

  block_handle.set_offset(0);
  block_handle.set_size(1);
  block_handle.set_raw_size(2);
  block_handle.set_checksum(3);
  block_handle.set_compress_type(common::kZSTD);

  BlockHandle copy_handle(block_handle);
  ASSERT_EQ(0, block_handle.get_offset());
  ASSERT_EQ(1, block_handle.get_size());
  ASSERT_EQ(2, block_handle.get_raw_size());
  ASSERT_EQ(3, block_handle.get_checksum());
  ASSERT_EQ(kZSTD, block_handle.get_compress_type());

  BlockHandle assign_handle;
  assign_handle = block_handle;
  ASSERT_EQ(0, block_handle.get_offset());
  ASSERT_EQ(1, block_handle.get_size());
  ASSERT_EQ(2, block_handle.get_raw_size());
  ASSERT_EQ(3, block_handle.get_checksum());
  ASSERT_EQ(kZSTD, block_handle.get_compress_type());

  block_handle.reset();
  block_handle.set_offset(0);
  block_handle.set_size(1);
  block_handle.set_raw_size(2);
  block_handle.set_checksum(3);
  block_handle.set_compress_type(common::kZSTD);
}

TEST(BlockHandleTest, set_and_get)
{
  BlockHandle block_handle;

  block_handle.set_offset(1);
  ASSERT_EQ(1, block_handle.get_offset());

  block_handle.set_size(2);
  ASSERT_EQ(2, block_handle.get_size());

  block_handle.set_raw_size(3);
  ASSERT_EQ(3, block_handle.get_raw_size());

  block_handle.set_checksum(123456);
  ASSERT_EQ(123456, block_handle.get_checksum());

  block_handle.set_compress_type(common::kZlibCompression);
  ASSERT_EQ(common::kZlibCompression, block_handle.get_compress_type());
}

TEST(BlockHandleTest, is_valid)
{
  BlockHandle block_handle;

  // invalid offset
  block_handle.set_offset(-1);
  block_handle.set_size(1);
  block_handle.set_raw_size(1);
  block_handle.set_compress_type(common::kNoCompression);
  ASSERT_FALSE(block_handle.is_valid());

  // invalid size
  block_handle.set_offset(1);
  block_handle.set_size(0);
  block_handle.set_raw_size(1);
  block_handle.set_compress_type(common::kNoCompression);
  ASSERT_FALSE(block_handle.is_valid());

  // invalid raw size
  block_handle.set_offset(1);
  block_handle.set_size(1);
  block_handle.set_raw_size(0);
  block_handle.set_compress_type(kNoCompression);
  ASSERT_FALSE(block_handle.is_valid());

  // raw size not equal to size with kNoCompression
  block_handle.set_offset(1);
  block_handle.set_size(1);
  block_handle.set_raw_size(2);
  block_handle.set_compress_type(kNoCompression);
  ASSERT_FALSE(block_handle.is_valid());

  // raw size less than size with compress
  block_handle.set_offset(1);
  block_handle.set_size(2);
  block_handle.set_raw_size(1);
  block_handle.set_compress_type(common::kZSTD);
  ASSERT_FALSE(block_handle.is_valid());

  // valid block handle with kNoCompression
  block_handle.set_offset(0);
  block_handle.set_size(1);
  block_handle.set_raw_size(1);
  block_handle.set_compress_type(common::kNoCompression);
  ASSERT_TRUE(block_handle.is_valid());
  
  // valid block handle with compress
  block_handle.set_offset(1);
  block_handle.set_size(1);
  block_handle.set_raw_size(2);
  block_handle.set_compress_type(kZSTD);
  ASSERT_TRUE(block_handle.is_valid());
}
  
TEST(BlockHandleTest, serialize_and_deserialize)
{
  int ret = Status::kOk;
  BlockHandle block_handle;
  int64_t serialize_size = 0;
  char *serilize_buf = nullptr;
  int64_t pos = 0;

  block_handle.set_offset(1234);
  block_handle.set_size(5678);
  block_handle.set_raw_size(9012);
  block_handle.set_compress_type(common::kZSTD);

  serialize_size = block_handle.get_serialize_size();
  serilize_buf = new char [serialize_size];
  ASSERT_TRUE(nullptr != serilize_buf);

  ret = block_handle.serialize(serilize_buf, serialize_size, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(serialize_size, pos);

  BlockHandle des_block_handle;
  pos = 0;
  ret = des_block_handle.deserialize(serilize_buf, serialize_size, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(serialize_size, pos);
  ASSERT_EQ(1234, block_handle.get_offset());
  ASSERT_EQ(5678, block_handle.get_size());
  ASSERT_EQ(9012, block_handle.get_raw_size());
  ASSERT_EQ(kZSTD, block_handle.get_compress_type());
}

TEST(BlockInfoTest, general_function)
{
  BlockInfo block_info;

  ASSERT_EQ(0, block_info.get_handle().get_offset());
  ASSERT_EQ(0, block_info.get_handle().get_size());
  ASSERT_TRUE(block_info.get_first_key().empty());
  ASSERT_EQ(0, block_info.get_row_count());
  ASSERT_EQ(0, block_info.get_delete_row_count());
  ASSERT_EQ(0, block_info.get_single_delete_row_count());
  ASSERT_EQ(kMaxSequenceNumber, block_info.get_smallest_seq());
  ASSERT_EQ(0, block_info.get_largest_seq());
  ASSERT_FALSE(block_info.is_columnar_format());
  ASSERT_FALSE(block_info.has_bloom_filter());
  ASSERT_EQ(0, block_info.get_per_key_bits());
  ASSERT_EQ(0, block_info.get_probe_num());
  ASSERT_TRUE(block_info.get_bloom_filter().empty());
  ASSERT_TRUE(block_info.get_unit_infos().empty());

  std::string fake_string("zdsppl");
  block_info.get_handle().set_offset(1);
  block_info.get_handle().set_size(2);
  block_info.set_first_key(fake_string);
  block_info.inc_row_count();
  block_info.inc_delete_row_count();
  block_info.inc_single_delete_row_count();
  block_info.set_smallest_seq(100);
  block_info.set_largest_seq(200);
  block_info.set_columnar_format();
  block_info.set_has_bloom_filter();
  block_info.set_per_key_bits(10);
  block_info.set_probe_num(7);
  Slice bloom_filter(fake_string.data(), fake_string.size());
  block_info.set_bloom_filter(bloom_filter);
  ColumnUnitInfo unit_info;
  unit_info.column_count_ = 1;
  unit_info.column_type_ = 2;
  block_info.get_unit_infos().push_back(unit_info);

  BlockInfo copy_block_info(block_info);
  ASSERT_EQ(1, copy_block_info.get_handle().get_offset());
  ASSERT_EQ(2, copy_block_info.get_handle().get_size());
  ASSERT_EQ(Slice(fake_string), copy_block_info.get_first_key());
  ASSERT_EQ(1, copy_block_info.get_row_count());
  ASSERT_EQ(1, copy_block_info.get_delete_row_count());
  ASSERT_EQ(1, copy_block_info.get_single_delete_row_count());
  ASSERT_EQ(100, copy_block_info.get_smallest_seq());
  ASSERT_EQ(200, copy_block_info.get_largest_seq());
  ASSERT_TRUE(copy_block_info.is_columnar_format());
  ASSERT_TRUE(copy_block_info.has_bloom_filter());
  ASSERT_EQ(10, copy_block_info.get_per_key_bits());
  ASSERT_EQ(7, copy_block_info.get_probe_num());
  ASSERT_EQ(Slice(fake_string), copy_block_info.get_bloom_filter());
  ASSERT_EQ(1, copy_block_info.get_unit_infos().size());
  ASSERT_EQ(1, copy_block_info.get_unit_infos().at(0).column_count_);
  ASSERT_EQ(2, copy_block_info.get_unit_infos().at(0).column_type_);

  BlockInfo assign_block_info;
  assign_block_info = block_info;
  ASSERT_EQ(1, assign_block_info.get_handle().get_offset());
  ASSERT_EQ(2, assign_block_info.get_handle().get_size());
  ASSERT_EQ(Slice(fake_string), assign_block_info.get_first_key());
  ASSERT_EQ(1, assign_block_info.get_row_count());
  ASSERT_EQ(1, assign_block_info.get_delete_row_count());
  ASSERT_EQ(1, assign_block_info.get_single_delete_row_count());
  ASSERT_EQ(100, assign_block_info.get_smallest_seq());
  ASSERT_EQ(200, assign_block_info.get_largest_seq());
  ASSERT_TRUE(assign_block_info.is_columnar_format());
  ASSERT_TRUE(assign_block_info.has_bloom_filter());
  ASSERT_EQ(10, assign_block_info.get_per_key_bits());
  ASSERT_EQ(7, assign_block_info.get_probe_num());
  ASSERT_EQ(Slice(fake_string), assign_block_info.get_bloom_filter());
  ASSERT_EQ(1, assign_block_info.get_unit_infos().size());
  ASSERT_EQ(1, assign_block_info.get_unit_infos().at(0).column_count_);
  ASSERT_EQ(2, assign_block_info.get_unit_infos().at(0).column_type_);

  block_info.reset();
  ASSERT_EQ(0, block_info.get_handle().get_offset());
  ASSERT_EQ(0, block_info.get_handle().get_size());
  ASSERT_TRUE(block_info.get_first_key().empty());
  ASSERT_EQ(0, block_info.get_row_count());
  ASSERT_EQ(0, block_info.get_delete_row_count());
  ASSERT_EQ(0, block_info.get_single_delete_row_count());
  ASSERT_EQ(kMaxSequenceNumber, block_info.get_smallest_seq());
  ASSERT_EQ(0, block_info.get_largest_seq());
  ASSERT_FALSE(block_info.is_columnar_format());
  ASSERT_FALSE(block_info.has_bloom_filter());
  ASSERT_EQ(0, block_info.get_per_key_bits());
  ASSERT_EQ(0, block_info.get_probe_num());
  ASSERT_TRUE(block_info.get_bloom_filter().empty());
  ASSERT_TRUE(block_info.get_unit_infos().empty());
}

TEST(BlockInfoTest, set_and_get)
{
  BlockInfo block_info;

  BlockHandle handle;
  handle.set_offset(1);
  handle.set_size(2);
  handle.set_raw_size(3),
  handle.set_checksum(4);
  handle.set_compress_type(common::kZSTD);
  block_info.set_handle(handle);
  BlockHandle handle_replica = block_info.get_handle();
  ASSERT_EQ(1, handle_replica.get_offset());
  ASSERT_EQ(2, handle_replica.get_size());
  ASSERT_EQ(3, handle_replica.get_raw_size());
  ASSERT_EQ(4, handle_replica.get_checksum());
  ASSERT_EQ(kZSTD, handle_replica.get_compress_type());
  BlockHandle &handle_reference = block_info.get_handle();
  ASSERT_EQ(1, handle_reference.get_offset());
  ASSERT_EQ(2, handle_reference.get_size());
  ASSERT_EQ(3, handle_reference.get_raw_size());
  ASSERT_EQ(4, handle_reference.get_checksum());
  ASSERT_EQ(kZSTD, handle_reference.get_compress_type());
  handle_reference.set_offset(5);
  handle_reference.set_size(6);
  handle_reference.set_raw_size(7);
  handle_reference.set_checksum(8);
  handle_reference.set_compress_type(common::kZlibCompression);
  handle_replica = block_info.get_handle();
  ASSERT_EQ(5, handle_replica.get_offset());
  ASSERT_EQ(6, handle_replica.get_size());
  ASSERT_EQ(7, handle_replica.get_raw_size());
  ASSERT_EQ(8, handle_replica.get_checksum());
  ASSERT_EQ(kZlibCompression, handle_replica.get_compress_type());

  std::string first_key("zdsppl");
  block_info.set_first_key(first_key);
  ASSERT_EQ(first_key, block_info.get_first_key());

  for (int32_t i = 0; i < 10; ++i) {
    block_info.inc_row_count();
  }
  ASSERT_EQ(10, block_info.get_row_count());
  block_info.inc_delete_row_count();
  block_info.inc_delete_row_count();
  ASSERT_EQ(2, block_info.get_delete_row_count());
  block_info.inc_single_delete_row_count();
  ASSERT_EQ(1, block_info.get_single_delete_row_count());
  ASSERT_EQ(30, block_info.get_delete_percent());

  common::SequenceNumber smallest_seq = 1234;
  block_info.set_smallest_seq(smallest_seq);
  ASSERT_EQ(1234, block_info.get_smallest_seq());

  common::SequenceNumber largest_seq = 5678;
  block_info.set_largest_seq(largest_seq);
  ASSERT_EQ(largest_seq, block_info.get_largest_seq());

  ASSERT_FALSE(block_info.is_columnar_format());
  block_info.set_columnar_format();
  ASSERT_TRUE(block_info.is_columnar_format());
  block_info.set_row_format();
  ASSERT_FALSE(block_info.is_columnar_format());

  ASSERT_FALSE(block_info.has_bloom_filter());
  block_info.set_has_bloom_filter();
  ASSERT_TRUE(block_info.has_bloom_filter());
  block_info.unset_has_bloom_filter();
  ASSERT_FALSE(block_info.has_bloom_filter());

  int32_t per_key_bits = 10;
  block_info.set_per_key_bits(per_key_bits);
  ASSERT_EQ(per_key_bits, block_info.get_per_key_bits());

  int32_t probe_num = 7;
  block_info.set_probe_num(probe_num);
  ASSERT_EQ(probe_num, block_info.get_probe_num());

  std::string bloom_filter("101010");
  block_info.unset_has_bloom_filter();
  ASSERT_FALSE(block_info.has_bloom_filter());
  block_info.set_bloom_filter(Slice(bloom_filter.data(), bloom_filter.size()));
  ASSERT_EQ(bloom_filter.data(), block_info.get_bloom_filter().data());
  ASSERT_EQ(bloom_filter.size(), block_info.get_bloom_filter().size());
  ASSERT_TRUE(block_info.has_bloom_filter());

  ColumnUnitInfo unit_info;
  unit_info.column_type_ = 1;
  unit_info.compress_type_ = 2;
  unit_info.column_count_ = 3;
  ASSERT_EQ(0, block_info.get_unit_infos().size());
  block_info.get_unit_infos().push_back(unit_info);
  ASSERT_EQ(1, block_info.get_unit_infos().size());
  ASSERT_EQ(1, block_info.get_unit_infos().at(0).column_type_);
  ASSERT_EQ(2, block_info.get_unit_infos().at(0).compress_type_);
  ASSERT_EQ(3, block_info.get_unit_infos().at(0).column_count_);
}

TEST(BlockInfoTest, is_valid)
{
  BlockInfo block_info;
  std::string fake_string("zdsppl");
  ColumnUnitInfo unit_info;

  // invalid handle
  block_info.reset();
  block_info.set_first_key(fake_string);
  block_info.inc_row_count();
  ASSERT_FALSE(block_info.is_valid());

  // empty first key
  block_info.reset();
  block_info.get_handle().set_offset(0);
  block_info.get_handle().set_size(1);
  block_info.get_handle().set_raw_size(1);
  block_info.inc_row_count();
  ASSERT_FALSE(block_info.is_valid());

  // invalid row count
  block_info.reset();
  block_info.get_handle().set_offset(0);
  block_info.get_handle().set_size(1);
  block_info.get_handle().set_raw_size(1);
  block_info.set_first_key(fake_string);
  ASSERT_FALSE(block_info.is_valid());

  // row_count < (delete_row_count + single_delete_row_count)
  block_info.reset();
  block_info.get_handle().set_offset(0);
  block_info.get_handle().set_size(1);
  block_info.get_handle().set_raw_size(1);
  block_info.set_first_key(fake_string);
  block_info.inc_delete_row_count();
  ASSERT_FALSE(block_info.is_valid());

  // invalid bloom_filter
  block_info.reset();
  block_info.get_handle().set_offset(0);
  block_info.get_handle().set_size(1);
  block_info.get_handle().set_raw_size(1);
  block_info.set_first_key(fake_string);
  block_info.inc_row_count();
  block_info.set_has_bloom_filter();
  ASSERT_FALSE(block_info.is_valid());
  block_info.set_per_key_bits(10);
  ASSERT_FALSE(block_info.is_valid());
  block_info.set_probe_num(7);
  ASSERT_FALSE(block_info.is_valid());

  // invalid columnar format
  block_info.reset();
  block_info.get_handle().set_offset(0);
  block_info.get_handle().set_size(1);
  block_info.get_handle().set_raw_size(1);
  block_info.set_first_key(fake_string);
  block_info.inc_row_count();
  block_info.set_columnar_format();
  ASSERT_FALSE(block_info.is_valid());

  // valid block info
  block_info.reset();
  block_info.get_handle().set_offset(0);
  block_info.get_handle().set_size(1);
  block_info.get_handle().set_raw_size(1);
  block_info.set_first_key(fake_string);
  block_info.inc_row_count();
  ASSERT_TRUE(block_info.is_valid());
  block_info.inc_row_count();
  block_info.inc_delete_row_count();
  block_info.inc_single_delete_row_count();
  ASSERT_TRUE(block_info.is_valid());
  block_info.set_bloom_filter(fake_string);
  block_info.set_per_key_bits(10);
  block_info.set_probe_num(7);
  ASSERT_TRUE(block_info.is_valid());
  block_info.set_columnar_format();
  block_info.get_unit_infos().push_back(unit_info);
  ASSERT_TRUE(block_info.is_valid());
}

TEST(BlockInfoTest, serialize_and_deserialize)
{
  int ret = Status::kOk;
  std::string fake_string("zdsppl");
  BlockInfo block_info;
  BlockInfo des_block_info;
  char *serilize_buf = nullptr;
  int64_t serialize_size = 0;
  int64_t pos = 0;

  block_info.get_handle().set_offset(0);
  block_info.get_handle().set_size(1);
  block_info.get_handle().set_raw_size(1);
  block_info.set_first_key(fake_string);
  block_info.inc_row_count();

  // serialzie without bloom filter and with row format
  serialize_size = block_info.get_serialize_size();
  serilize_buf = new char [serialize_size];
  ASSERT_TRUE(nullptr != serilize_buf);
  ret = block_info.serialize(serilize_buf, serialize_size, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(serialize_size, pos);
  pos = 0;
  ret = des_block_info.deserialize(serilize_buf, serialize_size, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(serialize_size, pos);
  ASSERT_EQ(0, des_block_info.get_handle().get_offset());
  ASSERT_EQ(1, des_block_info.get_handle().get_size());
  ASSERT_EQ(1, des_block_info.get_handle().get_raw_size());
  ASSERT_EQ(1, des_block_info.get_row_count());
  ASSERT_EQ(0, des_block_info.get_delete_row_count());
  ASSERT_EQ(0, des_block_info.get_single_delete_row_count());
  ASSERT_FALSE(des_block_info.has_bloom_filter());
  ASSERT_FALSE(des_block_info.is_columnar_format());
  ASSERT_TRUE(des_block_info.get_bloom_filter().empty());
  ASSERT_EQ(0, des_block_info.get_per_key_bits());
  ASSERT_EQ(0, des_block_info.get_probe_num());
  delete[] serilize_buf;

  // serialize with bloom filter
  block_info.set_bloom_filter(fake_string);
  block_info.set_per_key_bits(6);
  block_info.set_probe_num(2);
  serialize_size = block_info.get_serialize_size();
  serilize_buf = new char [serialize_size];
  pos = 0;
  ret = block_info.serialize(serilize_buf, serialize_size, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(serialize_size, pos);
  des_block_info.reset();
  pos = 0;
  ret = des_block_info.deserialize(serilize_buf, serialize_size, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(serialize_size, pos);
  ASSERT_EQ(0, des_block_info.get_handle().get_offset());
  ASSERT_EQ(1, des_block_info.get_handle().get_size());
  ASSERT_EQ(1, des_block_info.get_handle().get_raw_size());
  ASSERT_EQ(1, des_block_info.get_row_count());
  ASSERT_EQ(0, des_block_info.get_delete_row_count());
  ASSERT_EQ(0, des_block_info.get_single_delete_row_count());
  ASSERT_TRUE(des_block_info.has_bloom_filter());
  ASSERT_EQ(6, des_block_info.get_per_key_bits());
  ASSERT_EQ(2, des_block_info.get_probe_num());
  ASSERT_EQ(Slice(fake_string), des_block_info.get_bloom_filter());
  delete[] serilize_buf;

  // serialize with columnar format
  ColumnUnitInfo unit_info;
  unit_info.column_count_ = 2;
  unit_info.column_type_ = 1;
  block_info.set_columnar_format();
  block_info.get_unit_infos().push_back(unit_info);
  serialize_size = block_info.get_serialize_size();
  serilize_buf = new char [serialize_size];
  pos = 0;
  ret = block_info.serialize(serilize_buf, serialize_size, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(serialize_size, pos);
  des_block_info.reset();
  pos = 0;
  des_block_info.deserialize(serilize_buf, serialize_size, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(serialize_size, pos);
  ASSERT_EQ(0, des_block_info.get_handle().get_offset());
  ASSERT_EQ(1, des_block_info.get_handle().get_size());
  ASSERT_EQ(1, des_block_info.get_handle().get_raw_size());
  ASSERT_EQ(1, des_block_info.get_row_count());
  ASSERT_EQ(0, des_block_info.get_delete_row_count());
  ASSERT_EQ(0, des_block_info.get_single_delete_row_count());
  ASSERT_TRUE(des_block_info.has_bloom_filter());
  ASSERT_EQ(6, des_block_info.get_per_key_bits());
  ASSERT_EQ(2, des_block_info.get_probe_num());
  ASSERT_EQ(Slice(fake_string), des_block_info.get_bloom_filter());
  ASSERT_TRUE(des_block_info.is_columnar_format());
  ASSERT_EQ(1, des_block_info.get_unit_infos().size());
  ASSERT_EQ(2, des_block_info.get_unit_infos().at(0).column_count_);
  ASSERT_EQ(1, des_block_info.get_unit_infos().at(0).column_type_);
  delete[] serilize_buf;
}

} // namespace table
} // namespace smartengine

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
	smartengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}