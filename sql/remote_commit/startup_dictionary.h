/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_STARTUP_DICTIONARY_INCLUDED
#define SQL_REMOTE_COMMIT_STARTUP_DICTIONARY_INCLUDED

#include <span>
#include <unordered_map>

#include "mysql/strings/m_ctype.h"
#include "sql/dd/impl/types/collation_impl.h"
#include "sql/dd/types/charset.h"
#include "sql/dd/types/collation.h"

namespace wesql::remote_commit {

// Match the complete row set and fields written by DD's charset population.
inline bool startup_character_sets_match(
    std::span<const dd::Charset *const> charsets,
    std::span<const dd::Collation *const> collations,
    std::span<const CHARSET_INFO *const> compiled) {
  std::unordered_map<dd::Object_id, const dd::Charset *> charset_rows;
  std::unordered_map<dd::Object_id, const dd::Collation *> collation_rows;
  for (const auto *row : charsets)
    if (row == nullptr || !charset_rows.emplace(row->id(), row).second)
      return false;
  for (const auto *row : collations)
    if (row == nullptr || !collation_rows.emplace(row->id(), row).second)
      return false;

  for (const CHARSET_INFO *cs : compiled) {
    if (cs == nullptr || !(cs->state & MY_CS_PRIMARY) ||
        !(cs->state & MY_CS_AVAILABLE) || (cs->state & MY_CS_HIDDEN))
      continue;
    const auto charset = charset_rows.find(cs->number);
    if (charset == charset_rows.end()) return false;
    const dd::Charset &row = *charset->second;
    if (row.name() != cs->csname ||
        row.default_collation_id() != cs->number ||
        row.mb_max_length() != cs->mbmaxlen ||
        row.comment() != (cs->comment == nullptr ? "" : cs->comment))
      return false;
    charset_rows.erase(charset);
    for (const CHARSET_INFO *cl : compiled) {
      if (cl == nullptr || !(cl->state & MY_CS_AVAILABLE) ||
          !my_charset_same(cs, cl))
        continue;
      const auto collation = collation_rows.find(cl->number);
      if (collation == collation_rows.end()) return false;
      const dd::Collation &entry = *collation->second;
      const auto *implementation =
          dynamic_cast<const dd::Collation_impl *>(&entry);
      const auto pad = cl->pad_attribute == PAD_SPACE
                           ? dd::Collation::PA_PAD_SPACE
                           : dd::Collation::PA_NO_PAD;
      if (entry.name() != cl->m_coll_name || entry.charset_id() != cs->number ||
          entry.is_compiled() != bool(cl->state & MY_CS_COMPILED) ||
          entry.sort_length() != cl->strxfrm_multiply ||
          implementation == nullptr || implementation->pad_attribute() != pad)
        return false;
      collation_rows.erase(collation);
    }
  }
  return charset_rows.empty() && collation_rows.empty();
}

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_STARTUP_DICTIONARY_INCLUDED
