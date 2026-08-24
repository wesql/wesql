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
#include "se_charset_info.h"
#include "dict/se_field_make_unpack_info.h"
#include "dict/se_field_unpack_func.h"
#include "strings/m_ctype_internals.h"

namespace smartengine {

// See SeCharsetSpaceInfo::spaces_xfrm
const int SE_SPACE_XFRM_SIZE = 32;

std::array<std::unique_ptr<SeCharsetSpaceInfo>, MY_ALL_CHARSETS_SIZE> se_mem_comparable_space;

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
                                 size_t *const mb_len)
{
  assert(cs->number < MY_ALL_CHARSETS_SIZE);
  if (!se_mem_comparable_space[cs->number].get()) {
    SE_MUTEX_LOCK_CHECK(se_mem_cmp_space_mutex);
    if (!se_mem_comparable_space[cs->number].get()) {
      // Upper bound of how many bytes can be occupied by multi-byte form of a
      // character in any charset.
      const int MAX_MULTI_BYTE_CHAR_SIZE = 4;
      assert(cs->mbmaxlen <= MAX_MULTI_BYTE_CHAR_SIZE);

      // multi-byte form of the ' ' (space) character
      uchar space_mb[MAX_MULTI_BYTE_CHAR_SIZE];

      const size_t space_mb_len = cs->cset->wc_mb(
          cs, (my_wc_t)cs->pad_char, space_mb, space_mb + sizeof(space_mb));

      uchar space[20]; // mem-comparable image of the space character

      const size_t space_len = cs->coll->strnxfrm(cs, space, sizeof(space), 1,
                                                  space_mb, space_mb_len, MY_STRXFRM_NOPAD_WITH_SPACE);
      SeCharsetSpaceInfo *const info = new SeCharsetSpaceInfo;
      info->space_xfrm_len = space_len;
      info->space_mb_len = space_mb_len;
      while (info->spaces_xfrm.size() < SE_SPACE_XFRM_SIZE) {
        info->spaces_xfrm.insert(info->spaces_xfrm.end(), space,
                                 space + space_len);
      }
      se_mem_comparable_space[cs->number].reset(info);
    }
    SE_MUTEX_UNLOCK_CHECK(se_mem_cmp_space_mutex);
  }

  *xfrm = &se_mem_comparable_space[cs->number]->spaces_xfrm;
  *xfrm_len = se_mem_comparable_space[cs->number]->space_xfrm_len;
  *mb_len = se_mem_comparable_space[cs->number]->space_mb_len;

  if (cs == &my_charset_gbk_chinese_ci || cs == &my_charset_gbk_bin)
  {
    *xfrm_len= 2;
  }
}

mysql_mutex_t se_mem_cmp_space_mutex;

std::array<const SeCollationCodec *, MY_ALL_CHARSETS_SIZE>
    se_collation_data;
mysql_mutex_t se_collation_data_mutex;

bool se_is_collation_supported(const my_core::CHARSET_INFO * cs)
{
  return (cs->coll == &my_collation_8bit_simple_ci_handler ||
          cs == &my_charset_utf8mb3_general_ci ||
          cs == &my_charset_gbk_chinese_ci ||
          cs == &my_charset_utf8mb4_general_ci);
}

const SeCollationCodec *se_init_collation_mapping(const my_core::CHARSET_INFO *cs)
{
  assert(cs && cs->state & MY_CS_AVAILABLE);
  const SeCollationCodec *codec= se_collation_data[cs->number];

  if (codec == nullptr && se_is_collation_supported(cs))
  {
    mysql_mutex_lock(&se_collation_data_mutex);
    codec= se_collation_data[cs->number];
    if (codec == nullptr)
    {
      // Compute reverse mapping for simple collations.
      if (cs->coll == &my_collation_8bit_simple_ci_handler)
      {
        Se_collation_codec_simple *cur= new Se_collation_codec_simple();
        std::map<uchar, std::vector<uchar>> rev_map;
        size_t max_conflict_size= 0;
        for (int src = 0; src < 256; src++)
        {
          uchar dst= cs->sort_order[src];
          rev_map[dst].push_back(src);
          max_conflict_size= std::max(max_conflict_size, rev_map[dst].size());
        }
        cur->m_dec_idx.resize(max_conflict_size);

        for (auto const &p : rev_map)
        {
          uchar dst= p.first;
          for (uint idx = 0; idx < p.second.size(); idx++)
          {
            uchar src= p.second[idx];
            uchar bits = static_cast<uchar>(
                std::bit_width(std::bit_ceil(p.second.size())) -
                (p.second.size() != 0));
            cur->m_enc_idx[src]= idx;
            cur->m_enc_size[src]= bits;
            cur->m_dec_size[dst]= bits;
            cur->m_dec_idx[idx][dst]= src;
          }
        }

        cur->m_make_unpack_info_func=
          {{se_make_unpack_simple_varchar, se_make_unpack_simple}};

        cur->m_unpack_func=
          {{ se_unpack_simple_varchar_space_pad, se_unpack_simple }};

        cur->m_cs= cs;
        codec= cur;
      }
      else if (cs == &my_charset_utf8mb3_general_ci) {
        Se_collation_codec_utf8 *cur;
        cur= se_compute_lookup_values<Se_collation_codec_utf8, 3>(cs);
        cur->m_make_unpack_info_func=
          {{ se_make_unpack_varchar, se_make_unpack_char }};
        cur->m_unpack_func=
          {{ se_unpack_varchar_space_pad, se_unpack_char }};
        codec= cur;
      }
      // The gbk_chinese_ci collation is similar to utf8 collations, for 1 byte weight character
      // extended to 2 bytes. for example:
      // a -> 0x61->(0x2061)
      // b -> 0x62->(0x2062)
      // after that, every character in gbk get 2bytes weight.
      else if (cs == &my_charset_gbk_chinese_ci){
        Se_collation_codec_gbk *cur;
        cur= se_compute_lookup_values<Se_collation_codec_gbk, 2>(cs);
        cur->m_make_unpack_info_func=
          {{ se_make_unpack_varchar, se_make_unpack_char }};
        cur->m_unpack_func=
          {{ se_unpack_varchar_space_pad, se_unpack_char }};
        codec= cur;
      }
      // The utf8mb4 collation is similar to utf8 collations. For 4 bytes characters, the weight generated by
      // my_strnxfrm_unicode is 2bytes and weight is 0xFFFD. So these characters,we just store character itself in
      // unpack_info,every character use 21 bits.
      else if (cs == &my_charset_utf8mb4_general_ci){
        Se_collation_codec_utf8mb4 *cur;
        cur= se_compute_lookup_values<Se_collation_codec_utf8mb4, 4>(cs);
        cur->m_make_unpack_info_func=
          {{ se_make_unpack_varchar, se_make_unpack_char }};
        cur->m_unpack_func=
          {{ se_unpack_varchar_space_pad, se_unpack_char }};
        codec= cur;
      } else {
        // utf8mb4_0900_ai_ci
        // Out of luck for now.
      }

      if (codec != nullptr)
      {
        se_collation_data[cs->number]= codec;
      }
    }
    mysql_mutex_unlock(&se_collation_data_mutex);
  }

  return codec;
}

int get_segment_size_from_collation(const CHARSET_INFO *const cs)
{
  int ret;
  if (cs == &my_charset_utf8mb4_bin || cs == &my_charset_utf16_bin ||
      cs == &my_charset_utf16le_bin || cs == &my_charset_utf32_bin) {
    /*
      In these collations, a character produces one weight, which is 3 bytes.
      Segment has 3 characters, add one byte for VARCHAR_CMP_* marker, and we
      get 3*3+1=10
    */
    ret = 10;
  } else {
    /*
      All other collations. There are two classes:
      - Unicode-based, except for collations mentioned in the if-condition.
        For these all weights are 2 bytes long, a character may produce 0..8
        weights.
        in any case, 8 bytes of payload in the segment guarantee that the last
        space character won't span across segments.

      - Collations not based on unicode. These have length(strxfrm(' '))=1,
        there nothing to worry about.

      In both cases, take 8 bytes payload + 1 byte for VARCHAR_CMP* marker.
    */
    ret = 9;
  }
  assert(ret < SE_SPACE_XFRM_SIZE);
  return ret;
}

} //namespace smartengine
