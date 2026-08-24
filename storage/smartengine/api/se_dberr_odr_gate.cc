/* Combined-tree ODR gate: InnoDB dberr_t and SmartEngine se::dberr_t must
 * be distinct types with stable se_err_t numeric values. Compile+link only. */
#include "db0err.h"
#include "se_api.h"

static_assert(static_cast<int>(DB_SUCCESS) == 10, "innodb DB_SUCCESS");
static_assert(static_cast<int>(se::DB_SUCCESS) == 10, "se DB_SUCCESS");
static_assert(static_cast<int>(DB_SUCCESS_LOCKED_REC) == 9, "innodb locked rec");
static_assert(static_cast<int>(se::DB_SUCCESS_LOCKED_REC) == 9, "se locked rec");
static_assert(static_cast<int>(se::DB_QUE_THR_SUSPENDED) == 19, "se-only enumerator");
static_assert(static_cast<int>(se::DB_META_INVAL) == 3004, "se last enumerator");

int se_dberr_odr_gate(void) {
  dberr_t innodb = DB_SUCCESS;
  se_err_t se_e = se::DB_SUCCESS;
  return static_cast<int>(innodb) + static_cast<int>(se_e);
}
