//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once

#include <algorithm>
#include <limits>
#include <string>

#include "coding.h"
#include "memory/mod_info.h"
#include "options/options.h"

#ifdef HAVE_SNAPPY
#include <snappy.h>
#endif

#ifdef HAVE_ZLIB
#include <zlib.h>
#endif

#ifdef HAVE_BZIP2
#include <bzlib.h>
#endif

#ifdef HAVE_LZ4
#include <lz4.h>
#include <lz4hc.h>
#endif

#ifdef HAVE_ZSTD
#include <zstd.h>
#endif

#ifdef HAVE_XPRESS
#include "port/xpress.h"
#endif

namespace smartengine {
namespace util {

inline bool Snappy_Supported() {
#ifdef HAVE_SNAPPY
  return true;
#endif
  return false;
}

inline bool Zlib_Supported() {
#ifdef HAVE_ZLIB
  return true;
#endif
  return false;
}

inline bool BZip2_Supported() {
#ifdef HAVE_BZIP2
  return true;
#endif
  return false;
}

inline bool LZ4_Supported() {
#ifdef HAVE_LZ4
  return true;
#endif
  return false;
}

inline bool XPRESS_Supported() {
#ifdef HAVE_XPRESS
  return true;
#endif
  return false;
}

inline bool ZSTD_Supported() {
#ifdef HAVE_ZSTD
  // ZSTD format is finalized since version 0.8.0.
  return (ZSTD_versionNumber() >= 800);
#endif
  return false;
}

inline bool ZSTDNotFinal_Supported() {
#ifdef HAVE_ZSTD
  return true;
#endif
  return false;
}

inline bool CompressionTypeSupported(common::CompressionType compression_type) {
  switch (compression_type) {
    case common::kNoCompression:
      return true;
    case common::kSnappyCompression:
      return Snappy_Supported();
    case common::kZlibCompression:
      return Zlib_Supported();
    case common::kBZip2Compression:
      return BZip2_Supported();
    case common::kLZ4Compression:
      return LZ4_Supported();
    case common::kLZ4HCCompression:
      return LZ4_Supported();
    case common::kXpressCompression:
      return XPRESS_Supported();
    case common::kZSTDNotFinalCompression:
      return ZSTDNotFinal_Supported();
    case common::kZSTD:
      return ZSTD_Supported();
    default:
      assert(false);
      return false;
  }
}

inline std::string CompressionTypeToString(
    common::CompressionType compression_type) {
  switch (compression_type) {
    case common::kNoCompression:
      return "NoCompression";
    case common::kSnappyCompression:
      return "Snappy";
    case common::kZlibCompression:
      return "Zlib";
    case common::kBZip2Compression:
      return "BZip2";
    case common::kLZ4Compression:
      return "LZ4";
    case common::kLZ4HCCompression:
      return "LZ4HC";
    case common::kXpressCompression:
      return "Xpress";
    case common::kZSTD:
    case common::kZSTDNotFinalCompression:
      return "ZSTD";
    default:
      assert(false);
      return "";
  }
}

// compress_format_version can have two values:
// 1 -- decompressed sizes for BZip2 and Zlib are not included in the compressed
// block. Also, decompressed sizes for LZ4 are encoded in platform-dependent
// way.
// 2 -- Zlib, BZip2 and LZ4 encode decompressed size as Varint32 just before the
// start of compressed block. Snappy format is the same as version 1.

template <typename T>
inline bool Snappy_Compress(const common::CompressionOptions& opts,
                            const char* input, size_t length, T* output) {
#ifdef HAVE_SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef HAVE_SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif
}

inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#ifdef HAVE_SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif
}

namespace compression {
// returns size
template <typename T>
inline size_t PutDecompressedSizeInfo(T* output, uint32_t length) {
  PutVarint32(output, length);
  return output->size();
}

inline bool GetDecompressedSizeInfo(const char** input_data,
                                    size_t* input_length,
                                    uint32_t* output_len) {
  auto new_input_data =
      GetVarint32Ptr(*input_data, *input_data + *input_length, output_len);
  if (new_input_data == nullptr) {
    return false;
  }
  *input_length -= (new_input_data - *input_data);
  *input_data = new_input_data;
  return true;
}
}  // namespace compression

// compress_format_version == 1 -- decompressed size is not included in the
// block header
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
template <typename T>
inline bool Zlib_Compress(
    const common::CompressionOptions& opts, uint32_t compress_format_version,
    const char* input, size_t length, T* output,
    const common::Slice& compression_dict = common::Slice()) {
#ifdef HAVE_ZLIB
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }

  size_t output_header_len = 0;
  if (compress_format_version == 2) {
    output_header_len = compression::PutDecompressedSizeInfo(
        output, static_cast<uint32_t>(length));
  }
  // Resize output to be the plain data length.
  // This may not be big enough if the compression actually expands data.
  output->resize(output_header_len + length);

  // The memLevel parameter specifies how much memory should be allocated for
  // the internal compression state.
  // memLevel=1 uses minimum memory but is slow and reduces compression ratio.
  // memLevel=9 uses maximum memory for optimal speed.
  // The default value is 8. See zconf.h for more details.
  static const int memLevel = 8;
  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));
  int st = deflateInit2(&_stream, opts.level, Z_DEFLATED, opts.window_bits,
                        memLevel, opts.strategy);
  if (st != Z_OK) {
    return false;
  }

  if (compression_dict.size()) {
    // Initialize the compression library's dictionary
    st = deflateSetDictionary(
        &_stream, reinterpret_cast<const Bytef*>(compression_dict.data()),
        static_cast<unsigned int>(compression_dict.size()));
    if (st != Z_OK) {
      deflateEnd(&_stream);
      return false;
    }
  }

  // Compress the input, and put compressed data in output.
  _stream.next_in = (Bytef*)input;
  _stream.avail_in = static_cast<unsigned int>(length);

  // Initialize the output size.
  _stream.avail_out = static_cast<unsigned int>(length);
  _stream.next_out = reinterpret_cast<Bytef*>(&(*output)[output_header_len]);

  bool compressed = false;
  st = deflate(&_stream, Z_FINISH);
  if (st == Z_STREAM_END) {
    compressed = true;
    output->resize(output->size() - _stream.avail_out);
  }
  // The only return value we really care about is Z_STREAM_END.
  // Z_OK means insufficient output space. This means the compression is
  // bigger than decompressed size. Just fail the compression in that case.

  deflateEnd(&_stream);
  return compressed;
#endif
  return false;
}

// compress_format_version == 1 -- decompressed size is not included in the
// block header
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline char* Zlib_Uncompress(
    const char* input_data, size_t input_length, int* decompress_size,
    uint32_t compress_format_version,
    const common::Slice& compression_dict = common::Slice(),
    int windowBits = -14) {
#ifdef HAVE_ZLIB
  uint32_t output_len = 0;
  if (compress_format_version == 2) {
    if (!compression::GetDecompressedSizeInfo(&input_data, &input_length,
                                              &output_len)) {
      return nullptr;
    }
  } else {
    // Assume the decompressed data size will 5x of compressed size, but round
    // to the page size
    size_t proposed_output_len = ((input_length * 5) & (~(4096 - 1))) + 4096;
    output_len = static_cast<uint32_t>(
        std::min(proposed_output_len,
                 static_cast<size_t>(std::numeric_limits<uint32_t>::max())));
  }

  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));

  // For raw inflate, the windowBits should be -8..-15.
  // If windowBits is bigger than zero, it will use either zlib
  // header or gzip header. Adding 32 to it will do automatic detection.
  int st =
      inflateInit2(&_stream, windowBits > 0 ? windowBits + 32 : windowBits);
  if (st != Z_OK) {
    return nullptr;
  }

  if (compression_dict.size()) {
    // Initialize the compression library's dictionary
    st = inflateSetDictionary(
        &_stream, reinterpret_cast<const Bytef*>(compression_dict.data()),
        static_cast<unsigned int>(compression_dict.size()));
    if (st != Z_OK) {
      return nullptr;
    }
  }

  _stream.next_in = (Bytef*)input_data;
  _stream.avail_in = static_cast<unsigned int>(input_length);

  char* output = static_cast<char*>(memory::base_malloc(output_len, memory::ModId::kZlibCache));
  assert(output);
  _stream.next_out = (Bytef*)output;
  _stream.avail_out = static_cast<unsigned int>(output_len);

  bool done = false;
  while (!done) {
    st = inflate(&_stream, Z_SYNC_FLUSH);
    switch (st) {
      case Z_STREAM_END:
        done = true;
        break;
      case Z_OK: {
        // No output space. Increase the output space by 20%.
        // We should never run out of output space if
        // compress_format_version == 2
        assert(compress_format_version != 2);
        size_t old_sz = output_len;
        uint32_t output_len_delta = output_len / 5;
        output_len += output_len_delta < 10 ? 10 : output_len_delta;
        char* tmp =
            static_cast<char*>(base_malloc(output_len, memory::ModId::kZlibCache));
        assert(tmp);
        memcpy(tmp, output, old_sz);

        memory::base_free(output);
        output = tmp;

        // Set more output.
        _stream.next_out = (Bytef*)(output + old_sz);
        _stream.avail_out = static_cast<unsigned int>(output_len - old_sz);
        break;
      }
      case Z_BUF_ERROR:
      default:
        memory::base_free(output);
        inflateEnd(&_stream);
        return nullptr;
    }
  }

  // If we encoded decompressed block size, we should have no bytes left
  assert(compress_format_version != 2 || _stream.avail_out == 0);
  *decompress_size = static_cast<int>(output_len - _stream.avail_out);
  inflateEnd(&_stream);
  return output;
#endif

  return nullptr;
}

// compress_format_version == 1 -- decompressed size is not included in the
// block header
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
template <typename T>
inline bool BZip2_Compress(const common::CompressionOptions& opts,
                           uint32_t compress_format_version, const char* input,
                           size_t length, T* output) {
#ifdef HAVE_BZIP2
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }
  size_t output_header_len = 0;
  if (compress_format_version == 2) {
    output_header_len = compression::PutDecompressedSizeInfo(
        output, static_cast<uint32_t>(length));
  }
  // Resize output to be the plain data length.
  // This may not be big enough if the compression actually expands data.
  output->resize(output_header_len + length);

  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  // Block size 1 is 100K.
  // 0 is for silent.
  // 30 is the default workFactor
  int st = BZ2_bzCompressInit(&_stream, 1, 0, 30);
  if (st != BZ_OK) {
    return false;
  }

  // Compress the input, and put compressed data in output.
  _stream.next_in = (char*)input;
  _stream.avail_in = static_cast<unsigned int>(length);

  // Initialize the output size.
  _stream.avail_out = static_cast<unsigned int>(length);
  _stream.next_out = reinterpret_cast<char*>(&(*output)[output_header_len]);

  bool compressed = false;
  st = BZ2_bzCompress(&_stream, BZ_FINISH);
  if (st == BZ_STREAM_END) {
    compressed = true;
    output->resize(output->size() - _stream.avail_out);
  }
  // The only return value we really care about is BZ_STREAM_END.
  // BZ_FINISH_OK means insufficient output space. This means the compression
  // is bigger than decompressed size. Just fail the compression in that case.

  BZ2_bzCompressEnd(&_stream);
  return compressed;
#endif
  return false;
}

// compress_format_version == 1 -- decompressed size is not included in the
// block header
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
inline char* BZip2_Uncompress(const char* input_data, size_t input_length,
                              int* decompress_size,
                              uint32_t compress_format_version) {
#ifdef HAVE_BZIP2
  uint32_t output_len = 0;
  if (compress_format_version == 2) {
    if (!compression::GetDecompressedSizeInfo(&input_data, &input_length,
                                              &output_len)) {
      return nullptr;
    }
  } else {
    // Assume the decompressed data size will 5x of compressed size, but round
    // to the next page size
    size_t proposed_output_len = ((input_length * 5) & (~(4096 - 1))) + 4096;
    output_len = static_cast<uint32_t>(
        std::min(proposed_output_len,
                 static_cast<size_t>(std::numeric_limits<uint32_t>::max())));
  }

  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  int st = BZ2_bzDecompressInit(&_stream, 0, 0);
  if (st != BZ_OK) {
    return nullptr;
  }

  _stream.next_in = (char*)input_data;
  _stream.avail_in = static_cast<unsigned int>(input_length);

  char* output = static_cast<char*>(base_malloc(output_len, MOdId::kBZip2));
  assert(output) _stream.next_out = (char*)output;
  _stream.avail_out = static_cast<unsigned int>(output_len);

  bool done = false;
  while (!done) {
    st = BZ2_bzDecompress(&_stream);
    switch (st) {
      case BZ_STREAM_END:
        done = true;
        break;
      case BZ_OK: {
        // No output space. Increase the output space by 20%.
        // We should never run out of output space if
        // compress_format_version == 2
        assert(compress_format_version != 2);
        uint32_t old_sz = output_len;
        output_len = output_len * 1.2;
        char* tmp = static_cast<char*>(base_malloc(output_len, MOdId::kBZip2));
        assert(tmp);
        memcpy(tmp, output, old_sz);
        base_free(output);
        output = tmp;

        // Set more output.
        _stream.next_out = (char*)(output + old_sz);
        _stream.avail_out = static_cast<unsigned int>(output_len - old_sz);
        break;
      }
      default:
        base_free(output);
        BZ2_bzDecompressEnd(&_stream);
        return nullptr;
    }
  }

  // If we encoded decompressed block size, we should have no bytes left
  assert(compress_format_version != 2 || _stream.avail_out == 0);
  *decompress_size = static_cast<int>(output_len - _stream.avail_out);
  BZ2_bzDecompressEnd(&_stream);
  return output;
#endif
  return nullptr;
}

// compress_format_version == 1 -- decompressed size is included in the
// block header using memcpy, which makes database non-portable)
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
template <typename T>
inline bool LZ4_Compress(
    const common::CompressionOptions& opts, uint32_t compress_format_version,
    const char* input, size_t length, T* output,
    const common::Slice compression_dict = common::Slice()) {
#ifdef HAVE_LZ4
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }

  size_t output_header_len = 0;
  if (compress_format_version == 2) {
    // new encoding, using varint32 to store size information
    output_header_len = compression::PutDecompressedSizeInfo(
        output, static_cast<uint32_t>(length));
  } else {
    // legacy encoding, which is not really portable (depends on big/little
    // endianness)
    output_header_len = 8;
    output->resize(output_header_len);
    char* p = const_cast<char*>(output->c_str());
    memcpy(p, &length, sizeof(length));
  }
  int compress_bound = LZ4_compressBound(static_cast<int>(length));
  output->resize(static_cast<size_t>(output_header_len + compress_bound));

  int outlen;
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  LZ4_stream_t* stream = LZ4_createStream();
  if (compression_dict.size()) {
    LZ4_loadDict(stream, compression_dict.data(),
                 static_cast<int>(compression_dict.size()));
  }
#if LZ4_VERSION_NUMBER >= 10700  // r129+
  outlen =
      LZ4_compress_fast_continue(stream, input, &(*output)[output_header_len],
                                 static_cast<int>(length), compress_bound, 1);
#else  // up to r128
  outlen = LZ4_compress_limitedOutput_continue(
      stream, input, &(*output)[output_header_len], static_cast<int>(length),
      compress_bound);
#endif
  LZ4_freeStream(stream);
#else   // up to r123
  outlen = LZ4_compress_limitedOutput(input, &(*output)[output_header_len],
                                      static_cast<int>(length), compress_bound);
#endif  // LZ4_VERSION_NUMBER >= 10400

  if (outlen == 0) {
    return false;
  }
  output->resize(static_cast<size_t>(output_header_len + outlen));
  return true;
#endif  // LZ4
  return false;
}

// compress_format_version == 1 -- decompressed size is included in the
// block header using memcpy, which makes database non-portable)
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline char* LZ4_Uncompress(
    const char* input_data, size_t input_length, int* decompress_size,
    uint32_t compress_format_version,
    const common::Slice& compression_dict = common::Slice()) {
#ifdef HAVE_LZ4
  uint32_t output_len = 0;
  if (compress_format_version == 2) {
    // new encoding, using varint32 to store size information
    if (!compression::GetDecompressedSizeInfo(&input_data, &input_length,
                                              &output_len)) {
      return nullptr;
    }
  } else {
    // legacy encoding, which is not really portable (depends on big/little
    // endianness)
    if (input_length < 8) {
      return nullptr;
    }
    memcpy(&output_len, input_data, sizeof(output_len));
    input_length -= 8;
    input_data += 8;
  }

  char* output = static_cast<char*>(memory::base_malloc(output_len, memory::ModId::kLZ4Cache));
  assert(output);
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  LZ4_streamDecode_t* stream = LZ4_createStreamDecode();
  if (compression_dict.size()) {
    LZ4_setStreamDecode(stream, compression_dict.data(),
                        static_cast<int>(compression_dict.size()));
  }
  *decompress_size = LZ4_decompress_safe_continue(
      stream, input_data, output, static_cast<int>(input_length),
      static_cast<int>(output_len));
  LZ4_freeStreamDecode(stream);
#else   // up to r123
  *decompress_size =
      LZ4_decompress_safe(input_data, output, static_cast<int>(input_length),
                          static_cast<int>(output_len));
#endif  // LZ4_VERSION_NUMBER >= 10400

  if (*decompress_size < 0) {
    memory::base_free(output);
    return nullptr;
  }
  assert(*decompress_size == static_cast<int>(output_len));
  return output;
#endif  // LZ4
  return nullptr;
}

// compress_format_version == 1 -- decompressed size is included in the
// block header using memcpy, which makes database non-portable)
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
template <typename T>
inline bool LZ4HC_Compress(
    const common::CompressionOptions& opts, uint32_t compress_format_version,
    const char* input, size_t length, T* output,
    const common::Slice& compression_dict = common::Slice()) {
#ifdef HAVE_LZ4
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }

  size_t output_header_len = 0;
  if (compress_format_version == 2) {
    // new encoding, using varint32 to store size information
    output_header_len = compression::PutDecompressedSizeInfo(
        output, static_cast<uint32_t>(length));
  } else {
    // legacy encoding, which is not really portable (depends on big/little
    // endianness)
    output_header_len = 8;
    output->resize(output_header_len);
    char* p = const_cast<char*>(output->c_str());
    memcpy(p, &length, sizeof(length));
  }
  int compress_bound = LZ4_compressBound(static_cast<int>(length));
  output->resize(static_cast<size_t>(output_header_len + compress_bound));

  int outlen;
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  LZ4_streamHC_t* stream = LZ4_createStreamHC();
  LZ4_resetStreamHC(stream, opts.level);
  const char* compression_dict_data =
      compression_dict.size() > 0 ? compression_dict.data() : nullptr;
  size_t compression_dict_size = compression_dict.size();
  LZ4_loadDictHC(stream, compression_dict_data,
                 static_cast<int>(compression_dict_size));

#if LZ4_VERSION_NUMBER >= 10700  // r129+
  outlen =
      LZ4_compress_HC_continue(stream, input, &(*output)[output_header_len],
                               static_cast<int>(length), compress_bound);
#else   // r124-r128
  outlen = LZ4_compressHC_limitedOutput_continue(
      stream, input, &(*output)[output_header_len], static_cast<int>(length),
      compress_bound);
#endif  // LZ4_VERSION_NUMBER >= 10700
  LZ4_freeStreamHC(stream);

#elif LZ4_VERSION_MAJOR  // r113-r123
  outlen = LZ4_compressHC2_limitedOutput(input, &(*output)[output_header_len],
                                         static_cast<int>(length),
                                         compress_bound, opts.level);
#else                    // up to r112
  outlen =
      LZ4_compressHC_limitedOutput(input, &(*output)[output_header_len],
                                   static_cast<int>(length), compress_bound);
#endif                   // LZ4_VERSION_NUMBER >= 10400

  if (outlen == 0) {
    return false;
  }
  output->resize(static_cast<size_t>(output_header_len + outlen));
  return true;
#endif  // LZ4
  return false;
}

template <typename T>
inline bool XPRESS_Compress(const char* input, size_t length, T* output) {
#ifdef HAVE_XPRESS
// return port::xpress::Compress(input, length, output);
#endif
  return false;
}

inline char* XPRESS_Uncompress(const char* input_data, size_t input_length,
                               int* decompress_size) {
#ifdef HAVE_XPRESS
  return port::xpress::Decompress(input_data, input_length, decompress_size);
#endif
  return nullptr;
}

// @param compression_dict Data for presetting the compression library's
//    dictionary.
template <typename T>
inline bool ZSTD_Compress(
    const common::CompressionOptions& opts, const char* input, size_t length,
    T* output, const common::Slice& compression_dict = common::Slice()) {
#ifdef HAVE_ZSTD
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }

  size_t output_header_len = compression::PutDecompressedSizeInfo(
      output, static_cast<uint32_t>(length));

  size_t compressBound = ZSTD_compressBound(length);
  output->resize(static_cast<size_t>(output_header_len + compressBound));
  size_t outlen;
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
  ZSTD_CCtx* context = ZSTD_createCCtx();
  outlen = ZSTD_compress_usingDict(
      context, &(*output)[output_header_len], compressBound, input, length,
      compression_dict.data(), compression_dict.size(), opts.level);
  ZSTD_freeCCtx(context);
#else   // up to v0.4.x
  outlen = ZSTD_compress(&(*output)[output_header_len], compressBound, input,
                         length, opts.level);
#endif  // ZSTD_VERSION_NUMBER >= 500
  if (outlen == 0) {
    return false;
  }
  output->resize(output_header_len + outlen);
  return true;
#endif
  return false;
}

// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline char* ZSTD_Uncompress(
    const char* input_data, size_t input_length, int* decompress_size,
    const common::Slice& compression_dict = common::Slice()) {
#ifdef HAVE_ZSTD
  uint32_t output_len = 0;
  if (!compression::GetDecompressedSizeInfo(&input_data, &input_length,
                                            &output_len)) {
    return nullptr;
  }

  char* output =
      static_cast<char*>(memory::base_malloc(output_len, memory::ModId::kBZSTDCache));
  assert(output);
  size_t actual_output_length;
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
  ZSTD_DCtx* context = ZSTD_createDCtx();
  actual_output_length = ZSTD_decompress_usingDict(
      context, output, output_len, input_data, input_length,
      compression_dict.data(), compression_dict.size());
  ZSTD_freeDCtx(context);
#else   // up to v0.4.x
  actual_output_length =
      ZSTD_decompress(output, output_len, input_data, input_length);
#endif  // ZSTD_VERSION_NUMBER >= 500
  assert(actual_output_length == output_len);
  *decompress_size = static_cast<int>(actual_output_length);
  return output;
#endif
  return nullptr;
}

}  // namespace util
}  // namespace smartengine
