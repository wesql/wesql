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
#include "se_field_skip_func.h"
#include "field.h"
#include "dict/se_field_pack.h"
#include "util/se_buff.h"

namespace smartengine {

/*
  Function of type se_index_field_skip_t
*/

int se_skip_max_length(const SeFieldPacking *const fpi,
                       const Field *const field,
                       SeStringReader *const reader)
{
  if (UNLIKELY(!reader->read(fpi->m_max_image_len)))
    return HA_EXIT_FAILURE;
  return HA_EXIT_SUCCESS;
}

int se_skip_variable_length(const SeFieldPacking *const fpi,
                            const Field *const field,
                            SeStringReader *const reader)
{
  const uchar *ptr;
  bool finished = false;

  size_t dst_len; /* How much data can be there */
  if (field) {
    const Field_varstring *const field_var =
        reinterpret_cast<const Field_varstring *>(field);
    dst_len = field_var->pack_length() - field_var->get_length_bytes();
  } else {
    dst_len = UINT_MAX;
  }

  /* Decode the length-emitted encoding here */
  while ((ptr = (const uchar *)reader->read(SE_ESCAPE_LENGTH))) {
    /* See se_pack_with_varchar_encoding. */
    const uchar pad =
        255 - ptr[SE_ESCAPE_LENGTH - 1]; // number of padding bytes
    const uchar used_bytes = SE_ESCAPE_LENGTH - 1 - pad;

    if (used_bytes > SE_ESCAPE_LENGTH - 1 || used_bytes > dst_len) {
      return HA_EXIT_FAILURE; /* cannot store that much, invalid data */
    }

    if (used_bytes < SE_ESCAPE_LENGTH - 1) {
      finished = true;
      break;
    }
    dst_len -= used_bytes;
  }

  if (!finished) {
    return HA_EXIT_FAILURE;
  }

  return HA_EXIT_SUCCESS;
}

/*
  Skip a keypart that uses Variable-Length Space-Padded encoding
*/

int se_skip_variable_space_pad(const SeFieldPacking *const fpi,
                               const Field *const field,
                               SeStringReader *const reader)
{
  const uchar *ptr;
  bool finished = false;

  size_t dst_len = UINT_MAX; /* How much data can be there */

  if (field) {
    const Field_varstring *const field_var =
        reinterpret_cast<const Field_varstring *>(field);
    dst_len = field_var->pack_length() - field_var->get_length_bytes();
  }

  /* Decode the length-emitted encoding here */
  while ((ptr = (const uchar *)reader->read(fpi->m_segment_size))) {
    // See se_pack_with_varchar_space_pad
    const uchar c = ptr[fpi->m_segment_size - 1];
    if (c == VARCHAR_CMP_EQUAL_TO_SPACES) {
      // This is the last segment
      finished = true;
      break;
    } else if (c == VARCHAR_CMP_LESS_THAN_SPACES ||
               c == VARCHAR_CMP_GREATER_THAN_SPACES) {
      // This is not the last segment
      if ((fpi->m_segment_size - 1) > dst_len) {
        // The segment is full of data but the table field can't hold that
        // much! This must be data corruption.
        return HA_EXIT_FAILURE;
      }
      dst_len -= (fpi->m_segment_size - 1);
    } else {
      // Encountered a value that's none of the VARCHAR_CMP* constants
      // It's data corruption.
      return HA_EXIT_FAILURE;
    }
  }
  return finished ? HA_EXIT_SUCCESS : HA_EXIT_FAILURE;
}

} //namespace smartengine
