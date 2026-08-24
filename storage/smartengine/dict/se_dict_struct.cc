/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "se_dict_struct.h"
#include "m_string.h"
#include "nulls.h"

namespace smartengine
{
/**
  @brief
  Example of simple lock controls. The "table_handler" it creates is a
  structure we will pass to each ha_smartengine handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/
SeTableHandler *SeOpenTablesMap::get_table_handler(const char *const table_name)
{
  assert(table_name != nullptr);

  const std::string s_table_name(table_name);

  uint length;
  length = s_table_name.length();

  // First, look up the table in the hash map.
  SE_MUTEX_LOCK_CHECK(m_mutex);
  SeTableHandler *table_handler = nullptr;
  const auto &it = m_hash.find(s_table_name);
  if (it != m_hash.end()) {
    table_handler = it->second;
  } else {
    // Since we did not find it in the hash map, attempt to create and add it
    // to the hash map.
    char *tmp_name;
    if (!(table_handler = reinterpret_cast<SeTableHandler *>(my_multi_malloc(
              PSI_NOT_INSTRUMENTED,
              MYF(MY_WME | MY_ZEROFILL), &table_handler, sizeof(*table_handler),
              &tmp_name, length + 1, NullS)))) {
      // Allocating a new SeTableHandler and a new table name failed.
      SE_MUTEX_UNLOCK_CHECK(m_mutex);
      return nullptr;
    }

    table_handler->m_ref_count = 0;
    table_handler->m_table_name_length = length;
    table_handler->m_table_name = tmp_name;
    my_stpmov(table_handler->m_table_name, table_name);

    m_hash.insert({s_table_name, table_handler});

    thr_lock_init(&table_handler->m_thr_lock);
  }
  assert(table_handler->m_ref_count >= 0);
  table_handler->m_ref_count++;

  SE_MUTEX_UNLOCK_CHECK(m_mutex);

  return table_handler;
}

/**
  @brief
  Free lock controls. We call this whenever we close a table. If the table had
  the last reference to the table_handler, then we free the memory associated
  with it.
*/
void SeOpenTablesMap::release_table_handler(SeTableHandler *const table_handler)
{
  SE_MUTEX_LOCK_CHECK(m_mutex);

  assert(table_handler != nullptr);
  assert(table_handler->m_ref_count > 0);
  if (!--table_handler->m_ref_count) {
    // Last rereference was released. Tear down the hash entry.
    const auto ret MY_ATTRIBUTE((__unused__)) =
        m_hash.erase(std::string(table_handler->m_table_name));
    assert(ret == 1); // the hash entry must actually be found and deleted
    my_core::thr_lock_delete(&table_handler->m_thr_lock);
    my_free(table_handler);
  }

  SE_MUTEX_UNLOCK_CHECK(m_mutex);
}

std::vector<std::string> SeOpenTablesMap::get_table_names() const
{
  ulong i = 0;
  const SeTableHandler *table_handler;
  std::vector<std::string> names;

  SE_MUTEX_LOCK_CHECK(m_mutex);
  for (const auto &it : m_hash) {
    table_handler = it.second;
    assert(table_handler != nullptr);
    names.push_back(table_handler->m_table_name);
    i++;
  }
  assert(i == m_hash.size());
  SE_MUTEX_UNLOCK_CHECK(m_mutex);

  return names;
}

} // namespace smartengine
