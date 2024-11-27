//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "tools/sst_dump_tool.h"
#include "table/index_block_reader.h"
#include "util/crc32c.h"
#include "util/status.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sstream>

#include "table/extent_table_factory.h"
#include "table/extent_struct.h"

namespace smartengine
{
using namespace common;
using namespace db;
using namespace logger;
using namespace memory;
using namespace storage;
using namespace table;
using namespace util;

namespace tools
{
using std::dynamic_pointer_cast;

int ExtentDumper::init(const std::string &file_path, const int32_t extent_offset)
{
  int ret = Status::kOk;
  int fd;
  struct stat file_stat;

  if (0 != ::stat(file_path.c_str(), &file_stat)) {
    ret = Status::kEntryNotExist;
    SE_LOG(WARN, "the file is not exist", K(ret), K(file_path));
  } else if (-1 == (fd = ::open(file_path.c_str(), O_RDONLY | O_DIRECT))) {
    ret = Status::kIOError;
    SE_LOG(WARN, "fail to open the data file", K(ret), K(file_path));
  } else if (FAILED(extent_.init(ExtentId(0, extent_offset), 1, fd))) {
    SE_LOG(WARN, "fail to init extent", K(ret), K(extent_offset), K(fd));
  } else {
    // succeed
  }

  return ret;
}

int ExtentDumper::dump()
{
  int ret = Status::kOk;
  Footer footer;
  RowBlock *index_block = nullptr;

  if (FAILED(read_footer(footer))) {
    SE_LOG(WARN, "fail to read footer", K(ret));
  } else if (FAILED(read_block(footer.index_block_handle_, index_block))) {
    SE_LOG(WARN, "fail to read index block", K(ret), K(footer));
  } else if (FAILED(dump_index_block(index_block))) {
    SE_LOG(WARN, "fail to dump index block", K(ret));
  } else if (FAILED(dump_all_data_block(index_block))) {
    SE_LOG(WARN, "fail to dump all data blocks", K(ret));
  } else {
    summry(footer, index_block);
  }

  MOD_DELETE_OBJECT(RowBlock, index_block);

  return ret;
}

int ExtentDumper::dump_index_block(RowBlock *index_block)
{
  int ret = Status::kOk;
  IndexBlockReader index_block_reader;
  char *block_stats_buffer = nullptr;
  int64_t index_count = 1;

  if (IS_NULL(index_block)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(index_block));
  } else if (IS_NULL(block_stats_buffer = reinterpret_cast<char *>(memory::base_malloc(storage::MAX_EXTENT_SIZE)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for block stats buffer", K(ret));
  } else if (FAILED(index_block_reader.init(index_block, &internal_comparator_))) {
    SE_LOG(WARN, "fail to init index block reader", K(ret));
  } else {
    fprintf(stderr, "INDEX BLOCK:\n");
    fprintf(stderr, "-----------------------------------------------\n");
    index_block_reader.seek_to_first();
    while (SUCCED(ret) && index_block_reader.valid()) {
      Slice internal_key;
      ParsedInternalKey parsed_internal_key;
      BlockInfo block_info;
      if (FAILED(index_block_reader.get_key(internal_key))) {
        SE_LOG(WARN, "fail to get index key", K(ret));
      } else if (FAILED(index_block_reader.get_value(false , block_info))) {
        SE_LOG(WARN, "fail to get block stats", K(ret));
      } else if (!ParseInternalKey(internal_key, &parsed_internal_key)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "fail to parse index key to internal key", K(ret));
      } else {
        memset(block_stats_buffer, 0, storage::MAX_EXTENT_SIZE);
        block_info.to_string(block_stats_buffer, storage::MAX_EXTENT_SIZE);

        fprintf(stderr, "\n");
        fprintf(stderr, "[%ld] : INTERNAL_KEY: %s : {USER_KEY: %s : SEQ: %ld : TYPE: %ld} @ %s\n",
          index_count, internal_key.ToString(true /*hex*/).c_str(), parsed_internal_key.user_key.ToString(true /*hex*/).c_str(),
          parsed_internal_key.sequence, (int64_t)parsed_internal_key.type, block_stats_buffer);

        ++index_count;
        index_block_reader.next();
      }
    }
    fprintf(stderr, "-----------------------------------------------\n");
  }

  if (IS_NOTNULL(block_stats_buffer)) {
    memory::base_free(block_stats_buffer);
    block_stats_buffer = nullptr;
  }

  return ret;
}

int ExtentDumper::dump_all_data_block(RowBlock *index_block)
{
  int ret = Status::kOk;
  IndexBlockReader index_block_reader;
  common::Slice last_key;
  BlockInfo block_info;
  int64_t block_id = 1;

  if (IS_NULL(index_block)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(index_block));
  } else if (FAILED(index_block_reader.init(index_block, &internal_comparator_))) {
    SE_LOG(WARN, "fail to init index block reader", K(ret));
  } else {
    index_block_reader.seek_to_first();
    while (SUCCED(ret) && index_block_reader.valid()) {
      if (FAILED(index_block_reader.get_key(last_key))) {
        SE_LOG(WARN, "fail to get index key", K(ret));
      } else if (FAILED(index_block_reader.get_value(false, block_info))) {
        SE_LOG(WARN, "fail to get block stats", K(ret));
      } else {
        Slice first_key(block_info.get_first_key());
        fprintf(stderr, "Data Block #%ld @ [first_key : %s], [last_key : %s]\n\n",
          block_id, first_key.ToString(true /*hex*/).c_str(), last_key.ToString(true /*hex*/).c_str());
        
        if (FAILED(dump_data_block(block_info))) {
          SE_LOG(WARN, "fail to dump data block", K(block_id));
        } else {
          ++block_id;
          index_block_reader.next();
        }
      }
      fprintf(stderr, "\n");
    }
  }

  return ret;
}

int ExtentDumper::dump_data_block(const BlockInfo &block_info)
{
  int ret = Status::kOk;
  RowBlock *data_block = nullptr;
  RowBlockIterator data_block_iterator;
  Slice internal_key;
  ParsedInternalKey parsed_internal_key;
  Slice value;
  int64_t row_count = 1;
  

  if (FAILED(read_block(block_info.get_handle(), data_block))) {
    SE_LOG(WARN, "fail to read block", K(ret), K(block_info));
  } else if (FAILED(data_block_iterator.setup(&internal_comparator_, data_block, false))) {
    fprintf(stderr, "fail to setup data block iterator: ret = %d\n", ret);
  } else {
    data_block_iterator.SeekToFirst();
    while (data_block_iterator.Valid()) {
      internal_key = data_block_iterator.key();
      value = data_block_iterator.value();

      if (!ParseInternalKey(internal_key, &parsed_internal_key)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "fail to parse internal key", K(ret));
      } else {
        fprintf(stderr, "[%ld] : INTERNAL_KEY: %s : {USER_KEY: %s : SEQ: %ld : TYPE: %ld} | VALUE: %s\n",
          row_count, internal_key.ToString(true /*hex*/).c_str(), parsed_internal_key.user_key.ToString(true /*hex*/).c_str(),
          parsed_internal_key.sequence, (int64_t)parsed_internal_key.type, value.ToString(true /*hex*/).c_str());

        ++row_count;
        data_block_iterator.Next();
      }
    }
  }

  // resource clean
  if (IS_NOTNULL(data_block)) {
    MOD_DELETE_OBJECT(RowBlock, data_block);
  }

  return ret;
}

int ExtentDumper::read_footer(Footer &footer)
{
  int ret = Status::kOk;
  Slice footer_result;
  int32_t footer_offset = storage::MAX_EXTENT_SIZE - Footer::get_max_serialize_size();
  int32_t footer_size = Footer::get_max_serialize_size();
  int64_t pos = 0;
  char footer_buf[footer_size];
  memset(footer_buf, 0, footer_size);

  if (FAILED(extent_.read(nullptr, footer_offset, footer_size, footer_buf, footer_result))) {
    fprintf(stderr, "fail to read footer. ret = %d\n", ret);
  } else if (FAILED(footer.deserialize(footer_buf, footer_size, pos))) {
    fprintf(stderr, "fail to deserialize footer: %d\n", ret);
  }

  return ret;
}

int ExtentDumper::summry(const Footer &footer, RowBlock *index_block)
{
  int ret = Status::kOk;
  common::Slice key;
  table::BlockInfo block_info;
  table::ExtentInfo extent_info;
  IndexBlockReader index_block_reader;
  char *buf = nullptr;

  if (IS_NULL(index_block)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(index_block));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for buf", K(ret));
  } else if (FAILED(index_block_reader.init(index_block, &internal_comparator_))) {
    SE_LOG(WARN, "fail to init index block reader", K(ret));
  } else {
    index_block_reader.seek_to_first();
    while (SUCCED(ret) && index_block_reader.valid()) {
      if (FAILED(index_block_reader.get_key(key))) {
        SE_LOG(WARN, "fail to get index key", K(ret));
      } else if (FAILED(index_block_reader.get_value(false, block_info))) {
        SE_LOG(WARN, "fail to get block stats", K(ret));
      } else {
        extent_info.update(key, block_info);
        index_block_reader.next();
      }
    }
    extent_info.index_block_handle_ = footer.index_block_handle_;

    fprintf(stderr, "SUMMARY:\n");
    fprintf(stderr, "-----------------------------------------------\n");
    memset(buf, 0, storage::MAX_EXTENT_SIZE);
    footer.to_string(buf, storage::MAX_EXTENT_SIZE);
    fprintf(stderr, "footer: %s\n\n", buf);
    memset(buf, 0, storage::MAX_EXTENT_SIZE);
    extent_info.to_string(buf, storage::MAX_EXTENT_SIZE);
    fprintf(stderr, "extent_stats: %s\n", buf);
    fprintf(stderr, "-----------------------------------------------\n");
  }

  return ret;
}

int ExtentDumper::read_block(const BlockHandle &handle, RowBlock *&block)
{
  int ret = Status::kOk;
  CompressorHelper compressor_helper;
  char *io_buf = nullptr;
  char *raw_buf = nullptr;
  Slice block_content;
  Slice raw_block_content;
  uint32_t actual_checksum = 0; // the checksum actually stored in the block info.
  uint32_t expect_checksum = 0; // the checksum expected based on block data.

  if (UNLIKELY(!handle.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret));
  } else if (IS_NULL(io_buf = reinterpret_cast<char *>(base_memalign(DIOHelper::DIO_ALIGN_SIZE, handle.get_size(), ModId::kDefaultMod)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for io buf", K(ret), K(handle));
  } else if (FAILED(extent_.read(nullptr, handle.get_offset(), handle.get_size(), io_buf, block_content))) {
    SE_LOG(WARN, "fail to read block", K(ret), K(handle));
  } else if (UNLIKELY(io_buf != block_content.data()) ||
             UNLIKELY(handle.get_size() != static_cast<int32_t>(block_content.size()))) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "the data may be corrupted", K(ret), K(handle), KP(io_buf),
        KP(block_content.data()), K(block_content.size()));
  } else {
    // verify checksum
    actual_checksum = crc32c::Unmask(handle.get_checksum());
    expect_checksum = crc32c::Value(block_content.data(), block_content.size());

    if (actual_checksum != expect_checksum) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "the block checksum mismatch", K(ret), K(actual_checksum), K(expect_checksum));
    } else {
      if (kNoCompression == handle.get_compress_type()) {
        raw_block_content.assign(block_content.data(), block_content.size());
      } else {
        if (IS_NULL(raw_buf = reinterpret_cast<char *>(base_memalign(
            DIOHelper::DIO_ALIGN_SIZE, handle.get_raw_size(), ModId::kDefaultMod)))) {
          ret = Status::kMemoryLimit;
          SE_LOG(WARN, "fail to allocate memory for raw buf", K(ret), K(handle));
        } else if (FAILED(compressor_helper.uncompress(block_content,
                                                       handle.get_compress_type(),
                                                       raw_buf,
                                                       handle.get_raw_size(),
                                                       raw_block_content))) {
          SE_LOG(WARN, "fail to uncompress block", K(ret));
        } else {
          base_memalign_free(io_buf);
          io_buf = nullptr; 
        }
      }

      if (SUCCED(ret)) {  
        if (IS_NULL(block = MOD_NEW_OBJECT(ModId::kDefaultMod,
                                           RowBlock,
                                           raw_block_content.data(),
                                           raw_block_content.size(),
                                           kNoCompression))) {
          ret = Status::kMemoryLimit;
          SE_LOG(WARN, "fail to allocate block", K(ret), K(raw_block_content));
        }
      }
    }
  }

  if (FAILED(ret)) {
    if (IS_NOTNULL(io_buf)) {
      base_memalign_free(io_buf);
      io_buf = nullptr;
    }

    if (IS_NOTNULL(raw_buf)) {
      base_memalign_free(raw_buf);
      raw_buf = nullptr;
    }
  }

  return ret;
}

void SSTDumpTool::print_help()
{
  fprintf(stderr,
          R"(sst_dump --file=<data_file> --extent=<extent_index> [--command=raw]
    --file=<data_file>
      Path to data file

    --extent=extent_index
      the specific extent index

    --command=raw
        raw: Dump all the contents of extent
)");
}

int SSTDumpTool::Run(int argc, char** argv) {
  const char* file_path = nullptr;
  //'raw' will genarate a txt file while 'scan' will print entries
  std::string command;
  bool set_extent_offset = false;
  std::string extent_str;
  size_t extent_offset = 0;

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--file=", 7) == 0) {
      file_path = argv[i] + 7;
    } else if (strncmp(argv[i], "--extent=", 9) == 0) {
      extent_str = argv[i] + 9;
      std::istringstream iss(extent_str);
      if (iss.fail()) {
        fprintf(stderr, "extent must be numeric");
        exit(1);
      }
      iss >> extent_offset;
      set_extent_offset = true;
    } else if (strncmp(argv[i], "--command=", 10) == 0) {
      command = argv[i] + 10;
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help();
      exit(1);
    }
  }

  if (file_path == nullptr) {
    fprintf(stderr, "file or directory must be specified.\n\n");
    print_help();
    exit(1);
  }

  if (!set_extent_offset) {
    fprintf(stderr, "extent must be specified.\n\n");
    print_help();
    exit(1);
  }

  ExtentDumper extent_dumper;
  extent_dumper.init(std::string(file_path), extent_offset);

  if (command == "raw") {
    extent_dumper.dump();
  }

  return 0;
}

}  // namespace tools
}  // namespace smartengine
