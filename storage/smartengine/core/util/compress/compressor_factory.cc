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

#include "util/compress/compressor_factory.h"
#include "options/advanced_options.h"
#include "util/compress/compressor.h"
#include "util/status.h"

#include "logger/log_module.h"

namespace smartengine {
using namespace common;
using namespace memory;

namespace util {

CompressorFactory &CompressorFactory::get_instance()
{
  static CompressorFactory factory;
  return factory;
}

int CompressorFactory::alloc_compressor(const CompressionType compress_type, Compressor *&compressor)
{
  int ret = Status::kOk;
  compressor = nullptr;

  switch (compress_type) {
    case common::kLZ4Compression: {
#ifdef HAVE_LZ4
      if (IS_NULL(compressor = MOD_NEW_OBJECT(ModId::kCompressor, LZ4Compressor))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for LZ4Compressor", K(ret));
      }
#else
      ret = Status::kNotSupported;
      SE_LOG(WARN, "LZ4Compressor is not supported", K(ret));
#endif // HAVE_LZ4
      break;
    }
    case common::kZlibCompression: {
#ifdef HAVE_ZLIB
      if (IS_NULL(compressor = MOD_NEW_OBJECT(ModId::kCompressor, ZLIBCompressor))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for ZLIBCompressor", K(ret));
      }
#else
      ret = Status::kNotSupported;
      SE_LOG(WARN, "ZLIBCompressor is not supported", K(ret));
#endif // HAVE_ZLIB
      break;
    }
    case common::kZSTD: {
#ifdef HAVE_ZSTD
      if (IS_NULL(compressor = MOD_NEW_OBJECT(ModId::kCompressor, ZSTDCompressor))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for ZSTDCompressor", K(ret));
      }
#else
      ret = Status::kNotSupported;
      SE_LOG(WARN, "ZSTDCompressor is not supported", K(ret));
#endif // HAVE_ZSTD
      break;
    }
    default:
      ret = Status::kNotSupported;
      SE_LOG(WARN, "the compression type is not supported", KE(compress_type));
      break;
  }

  return ret;
}

int CompressorFactory::free_compressor(Compressor *&compressor)
{
  int ret = Status::kOk;

  if (IS_NULL(compressor)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(compressor));
  } else {
    switch (compressor->get_compress_type()) {
      case common::kLZ4Compression: {
#ifdef HAVE_LZ4
        LZ4Compressor *lz4_compressor = reinterpret_cast<LZ4Compressor *>(compressor);
        MOD_DELETE_OBJECT(LZ4Compressor, lz4_compressor);
        compressor = nullptr;
#else
        ret = Status::kNotSupported;
        SE_LOG(WARN, "LZ4Compressor is not supported", K(ret));
#endif // HAVE_LZ4
        break;
      }
      case common::kZlibCompression: {
#ifdef HAVE_ZLIB
        ZLIBCompressor *zlib_compressor = reinterpret_cast<ZLIBCompressor *>(compressor);
        MOD_DELETE_OBJECT(ZLIBCompressor, zlib_compressor);
        compressor = nullptr;
#else
        ret = Status::kNotSupported;
        SE_LOG(WARN, "ZLIBCompressor is not supported", K(ret));
#endif // HAVE_ZLIB
        break;
      }
      case common::kZSTD: {
#ifdef HAVE_ZSTD
        ZSTDCompressor *zstd_compressor = reinterpret_cast<ZSTDCompressor *>(compressor);
        MOD_DELETE_OBJECT(ZSTDCompressor, zstd_compressor);
        compressor = nullptr;
#else
        ret = Status::kNotSupported;
        SE_LOG(WARN, "ZSTDCompressor is not supported", K(ret));
#endif // HAVE_ZSTD
        break;
      }
      default:
        ret = Status::kNotSupported;
        SE_LOG(WARN, "the compression type is not supported", KE(compressor->get_compress_type()));
        break;
    } 
  }

  return ret;
}

} // namespace util
} // namespace smartengine
