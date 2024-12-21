#include "table/bloom_filter.h"
#include "logger/log_module.h"
#include "port/port_posix.h"
#include "util/hash.h"
#include "util/status.h"

namespace smartengine
{
using namespace common;
using namespace memory;

namespace table
{
BloomFilterWriter::BloomFilterWriter()
    : is_inited_(false),
      per_key_bits_(0),
      probe_num_(0),
      hash_values_(),
      bits_buf_(nullptr),
      bits_buf_size_(0)
{}

BloomFilterWriter::~BloomFilterWriter()
{
  destroy();
}

int BloomFilterWriter::init(int32_t per_key_bits, int32_t probe_num)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "BloomFilterWriter has been inited", K(ret));
  } else if (UNLIKELY(per_key_bits <= 0) || UNLIKELY(probe_num <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(per_key_bits), K(probe_num));
  } else {
    per_key_bits_ = per_key_bits;
    probe_num_ = probe_num;
    is_inited_ = true;
  }

  return ret;
}

void BloomFilterWriter::destroy()
{
  if (IS_NOTNULL(bits_buf_)) {
    base_free(bits_buf_);
    bits_buf_ = nullptr;
  }
}

void BloomFilterWriter::reuse()
{
  hash_values_.clear();
}

int BloomFilterWriter::add(const common::Slice &key)
{
  int ret = Status::kOk;
  uint32_t hash_value = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "BloomFilterWriter should be inited first", K(ret));
  } else if (UNLIKELY(key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else {
    hash_value = util::BloomHash(key);
    if (hash_values_.empty() || hash_value != hash_values_.back()) {
      hash_values_.push_back(hash_value);
    }
  }

  return ret;
}

int BloomFilterWriter::build(Slice &bloom_filter)
{
  int ret = Status::kOk;
  int32_t size = 0;
  int32_t cache_line_num = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "BloomFilterWriter should be inited first", K(ret));
  } else if (UNLIKELY(hash_values_.empty())) {
    // do nothing
  } else {
    calculate_space_size(hash_values_.size(), per_key_bits_, size, cache_line_num);
    if (FAILED(reserve_bits_buf(size))) {
      SE_LOG(WARN, "fail to reserve bits buf", K(ret));
    } else {
      for (auto &hash_value : hash_values_) {
        set_bits(hash_value, cache_line_num);
      }
      bloom_filter.assign(bits_buf_, size);
    }
  }

  return ret;
}

int BloomFilterWriter::reserve_bits_buf(int32_t size)
{
  int ret = Status::kOk;

  if (bits_buf_size_ >= size) {
    memset(bits_buf_, 0, bits_buf_size_);
  } else {
    if (IS_NOTNULL(bits_buf_)) {
      base_free(bits_buf_);
      bits_buf_ = nullptr;
      bits_buf_size_ = 0;
    }
    if (IS_NULL(bits_buf_ = reinterpret_cast<char *>(base_malloc(size, ModId::kBloomFilter)))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for bits buf", K(ret), K(size));
    } else {
      memset(bits_buf_, 0, size);
      bits_buf_size_ = size;
    }
  }

  return ret;
}

void BloomFilterWriter::calculate_space_size(int32_t hash_value_size,
                                             int32_t per_key_bits,
                                             int32_t &size,
                                             int32_t &cache_line_num)
{
  int32_t total_bits = hash_value_size * per_key_bits;
  cache_line_num = (total_bits + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);
  // In systems like caches, data is divided into blocks or lines,
  // and addresses are mapped to these blocks using a subset of
  // the address bits. If num_lines is a power of two (e.g., 8, 16, 32),
  // only the lower bits of the address are used for indexing. By
  // making num_lines an odd number, the indexing process might involve
  // more bits of the address, potentially resulting in a more uniform
  // distribution of data.
  if (0 == (cache_line_num % 2)) {
    ++cache_line_num;
  }
  size = cache_line_num * CACHE_LINE_SIZE;
}

void BloomFilterWriter::set_bits(uint32_t hash_value, int32_t cache_line_num)
{
  const uint32_t delta = (hash_value >> 17) | (hash_value << 15); // Rotate right 17 bits.
  uint32_t cache_line_pos = (hash_value % cache_line_num) * (CACHE_LINE_SIZE * 8);
  uint32_t bit_pos = 0;

  for (int32_t i = 0; i < probe_num_; ++i) {
    bit_pos = cache_line_pos + (hash_value % (CACHE_LINE_SIZE * 8));
    bits_buf_[bit_pos / 8] |= (1 << (bit_pos % 8));

    hash_value += delta;
  }
}

BloomFilterReader::BloomFilterReader()
    : is_inited_(false),
      bits_buf_(nullptr),
      bits_buf_size_(0),
      probe_num_(0),
      cache_line_num_(0)
{}

BloomFilterReader::~BloomFilterReader() {}

int BloomFilterReader::init(const Slice &bloom_filter, int32_t probe_num)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "BloomFilterReader has been inited", K(ret));
  } else if (UNLIKELY(bloom_filter.empty()) ||
             UNLIKELY(bloom_filter.size() < CACHE_LINE_SIZE) ||
             UNLIKELY(0 != (bloom_filter.size() % CACHE_LINE_SIZE)) ||
             UNLIKELY(probe_num <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(bloom_filter),
        "size", bloom_filter.size(), K(probe_num));
  } else {
    bits_buf_ = bloom_filter.data();
    bits_buf_size_ = bloom_filter.size();
    probe_num_ = probe_num;
    cache_line_num_ = bits_buf_size_ / CACHE_LINE_SIZE;
    is_inited_ = true;
  }

  return ret;
}

int BloomFilterReader::check(const Slice &key, bool &exist)
{
  int ret = Status::kOk;
  uint32_t hash_value = 0;
  uint32_t delta = 0;
  uint32_t cache_line_pos = 0;
  uint32_t bit_pos = 0;
  exist = true;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "BloomFilterReader should be inited", K(ret));
  } else if (UNLIKELY(key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else {
    hash_value = util::BloomHash(key);
    delta = (hash_value >> 17) | (hash_value << 15); // Rotate right 17 bits
    // calculate cache line pos for hash_value
    cache_line_pos = (hash_value % cache_line_num_) * (CACHE_LINE_SIZE * 8);

    for (int32_t i = 0; i < probe_num_; ++i) {
      bit_pos = cache_line_pos + (hash_value % (CACHE_LINE_SIZE * 8));
      if (0 == (bits_buf_[bit_pos / 8] & (1 << (bit_pos % 8)))) {
        exist = false;
        break;
      }
      hash_value += delta;
    }
  }

  return ret;
}

} // namespace table
} // namespace smartengine