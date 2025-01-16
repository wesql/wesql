/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012, Monty Program Ab

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

#include "se_transaction_factory.h"
#include "mysql/plugin.h"
#include "se_hton.h"
#include "se_system_vars.h"
#include "se_transaction_impl.h"
#include "se_write_batch_impl.h"
#include "sql_class.h"
#include "util/se_utils.h"

namespace smartengine
{

SeTransaction *&get_tx_from_thd(THD *const thd)
{
  return *reinterpret_cast<SeTransaction **>(my_core::thd_ha_data(thd, se_hton));
}

/**TODO: maybe, call this in external_lock() and store in ha_smartengine.*/
SeTransaction *get_or_create_tx(THD *const thd)
{
  SeTransaction *&tx = get_tx_from_thd(thd);
  // TODO: this is called too many times.. O(#rows)
  if (tx == nullptr) {
    if ((se_rpl_skip_tx_api_var && thd->rli_slave) ||
        /*(THDVAR(thd, master_skip_tx_api) && !thd->rli_slave) || */
         (se_skip_unique_key_check_in_boost_insert &&
          !my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
    {
      tx = new SeWritebatchImpl(thd);
    }
    else
    {
      tx = new SeTransactionImpl(thd);
    }
    tx->set_params(thd);
    tx->start_tx();
  } else {
    tx->set_params(thd);
    if (!tx->is_tx_started()) {
      tx->start_tx();
    }
  }

  return tx;
}

void se_register_tx(handlerton *const hton, THD *const thd, SeTransaction *const tx)
{
  assert(tx != nullptr);

  trans_register_ha(thd, FALSE, se_hton, NULL);
  if (my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
    tx->start_stmt();
    trans_register_ha(thd, TRUE, se_hton, NULL);
  }
}

/*
  returns a vector of info for all non-replication threads
  for use by information_schema.se_trx
*/
std::vector<SeTrxInfo> se_get_all_trx_info()
{
  std::vector<SeTrxInfo> trx_info;
  SeTrxInfoAggregator trx_info_agg(&trx_info);
  SeTransaction::walk_tx_list(&trx_info_agg);
  return trx_info;
}

} // namespace smartengine
