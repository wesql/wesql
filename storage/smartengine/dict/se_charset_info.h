/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012,2013 Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
#ifndef SE_DICT_XE_COLLATION_H_
#define SE_DICT_XE_COLLATION_H_

#include <array>
#include <bit>
#include <memory>
#include <vector>
#include "my_inttypes.h"
#include "my_sys.h"
#include "dict/se_field_pack.h"
#include "util/se_utils.h"

extern CHARSET_INFO my_charset_gbk_chinese_ci;
extern CHARSET_INFO my_charset_gbk_bin;
extern CHARSET_INFO my_charset_utf8mb4_bin;
extern CHARSET_INFO my_charset_utf16_bin;
extern CHARSET_INFO my_charset_utf16le_bin;
extern CHARSET_INFO my_charset_utf32_bin;

namespace smartengine {

struct SeCollationCodec;

// See SeCharsetSpaceInfo::spaces_xfrm
extern const int SE_SPACE_XFRM_SIZE;

// A class holding information about how space character is represented in a
// charset.
class SeCharsetSpaceInfo {
public:
  SeCharsetSpaceInfo(const SeCharsetSpaceInfo &) = delete;
  SeCharsetSpaceInfo &operator=(const SeCharsetSpaceInfo &) = delete;
  SeCharsetSpaceInfo() = default;

  // A few strxfrm'ed space characters, at least SE_SPACE_XFRM_SIZE bytes
  std::vector<uchar> spaces_xfrm;

  // length(strxfrm(' '))
  size_t space_xfrm_len;

  // length of the space character itself
  // Typically space is just 0x20 (length=1) but in ucs2 it is 0x00 0x20
  // (length=2)
  size_t space_mb_len;
};

extern std::array<std::unique_ptr<SeCharsetSpaceInfo>, MY_ALL_CHARSETS_SIZE> se_mem_comparable_space;

/*
  @brief
  For a given charset, get
   - strxfrm('    '), a sample that is at least SE_SPACE_XFRM_SIZE bytes long.
   - length of strxfrm(charset, ' ')
   - length of the space character in the charset

  @param cs  IN    Charset to get the space for
  @param ptr OUT   A few space characters
  @param len OUT   Return length of the space (in bytes)

  @detail
    It is tempting to pre-generate mem-comparable form of space character for
    every charset on server startup.
    One can't do that: some charsets are not initialized until somebody
    attempts to use them (e.g. create or open a table that has a field that
    uses the charset).
*/

void se_get_mem_comparable_space(const CHARSET_INFO *const cs,
                                 const std::vector<uchar> **xfrm,
                                 size_t *const xfrm_len,
                                 size_t *const mb_len);

extern mysql_mutex_t se_mem_cmp_space_mutex;
extern std::array<const SeCollationCodec *, MY_ALL_CHARSETS_SIZE>
     se_collation_data;
extern mysql_mutex_t se_collation_data_mutex;

bool se_is_collation_supported(const my_core::CHARSET_INFO * cs);

static const uchar SE_MAX_XFRM_MULTIPLY = 8;

template <typename C, uint S>
C *se_compute_lookup_values(const my_core::CHARSET_INFO *cs)
{
  using T = typename C::character_type;
  auto cur= new C;

  std::map<T, std::vector<T>> rev_map;
  size_t max_conflict_size= 0;
  std::array<uchar, S> srcbuf;
  std::array<uchar, S * SE_MAX_XFRM_MULTIPLY> dstbuf;
  for (int src = 0; src < (1 << (8 * sizeof(T))); src++)
  {
    uint srclen= 0;
    if (cs == &my_charset_utf8mb3_general_ci ||
        cs == &my_charset_utf8mb4_general_ci)
    {
      srclen= cs->cset->wc_mb(cs, src, srcbuf.data(), srcbuf.data() + srcbuf.size());
      assert(srclen > 0 && srclen <= 3);
    }
    else if (cs == &my_charset_gbk_chinese_ci)
    {
      if (src <= 0xFF)
      {
        /* for 1byte src, use 0x00 makeup 2btyes */
        srcbuf[0]= 0;
        srcbuf[1]= src & 0xFF;
      }
      else
      {
        /* gbk code */
        srcbuf[0]= src >> 8;
        srcbuf[1]= src & 0xFF;
      }
      srclen= 2;
    }
    size_t xfrm_len __attribute__((__unused__))= cs->coll->strnxfrm(cs, dstbuf.data(), dstbuf.size(),
                                                 dstbuf.size(), srcbuf.data(), srclen, MY_STRXFRM_NOPAD_WITH_SPACE);
    T dst;
    if (xfrm_len > 0) {
      dst= dstbuf[0] << 8 | dstbuf[1];
      assert(xfrm_len >= sizeof(dst));
    } else {
     /*
      According to RFC 3629, UTF-8 should prohibit characters between
      U+D800 and U+DFFF, which are reserved for surrogate pairs and do
      not directly represent characters.
     */
     dst = src;
    }

    rev_map[dst].push_back(src);
    max_conflict_size= std::max(max_conflict_size, rev_map[dst].size());
  }
  cur->m_dec_idx.resize(cur->level);

  for (const auto &p : rev_map)
  {
    T dst= p.first;
    for (T idx = 0; idx < p.second.size(); idx++)
    {
      auto src= p.second[idx];
      uchar bits = static_cast<uchar>(
          std::bit_width(std::bit_ceil(p.second.size())) -
          (p.second.size() != 0));
      cur->m_enc_idx[src]= idx;
      cur->m_enc_size[src]= bits;
      cur->m_dec_size[dst]= bits;
      if (cur->level == 0 || idx < cur->level)
      {
        cur->m_dec_idx[idx][dst]= src;
      }
      else
      {
        cur->m_dec_idx_ext[dst].push_back(src);
      }
    }
  }

  cur->m_cs= cs;

  return cur;
}

const SeCollationCodec *se_init_collation_mapping(const my_core::CHARSET_INFO *cs);

int get_segment_size_from_collation(const CHARSET_INFO *const cs);


struct SeCollationCodec
{
  const my_core::CHARSET_INFO *m_cs;
  // The first element unpacks VARCHAR(n), the second one - CHAR(n).
  std::array<se_make_unpack_info_t, 2> m_make_unpack_info_func;
  std::array<se_index_field_unpack_t, 2> m_unpack_func;
  virtual ~SeCollationCodec() = default;
};

template <typename T, uint L>
struct Se_collation_codec_derived : public SeCollationCodec
{
  using character_type = T;
  static const uint level = L;
  static const uint size = 1 << (8 * sizeof(T));

  std::array<T, size> m_enc_idx;
  std::array<uchar, size> m_enc_size;

  std::array<uchar, size> m_dec_size;
  // Generally, m_dec_idx[idx][src] = original character. However, because the
  // array can grow quite large for utf8, we have m_dec_idx_ext which is more
  // space efficient.
  //
  // Essentially, if idx < level, then use m_dec_idx[idx][src], otherwise, use
  // m_dec_idx[src][idx - level]. If level is 0 then, m_dec_idx_ext is unused.
  std::vector<std::array<T, size>> m_dec_idx;
  std::array<std::vector<T>, size> m_dec_idx_ext;

  // m_dec_skip[{idx, src}] = number of bytes to skip given (idx, src)
  std::map<std::pair<T, T>, uchar> m_dec_skip;
};

// "Simple" collations (those specified in strings/ctype-simple.c) are simple
// because their strnxfrm function maps one byte to one byte. However, the
// mapping is not injective, so the inverse function will take in an extra
// index parameter containing information to disambiguate what the original
// character was.
//
// The m_enc* members are for encoding. Generally, we want encoding to be:
//      src -> (dst, idx)
//
// Since strnxfrm already gives us dst, we just need m_enc_idx[src] to give us
// idx.
//
// For the inverse, we have:
//      (dst, idx) -> src
//
// We have m_dec_idx[idx][dst] = src to get our original character back.
//

using Se_collation_codec_simple = Se_collation_codec_derived<uchar, 0>;
using Se_collation_codec_utf8 = Se_collation_codec_derived<uint16, 2>;
using Se_collation_codec_gbk = Se_collation_codec_derived<uint16, 2>;
using Se_collation_codec_utf8mb4 = Se_collation_codec_derived<uint16, 2>;

} //namespace smartengine

#endif
