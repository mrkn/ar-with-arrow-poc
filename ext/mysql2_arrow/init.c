#include "mysql2-arrow.h"

VALUE ma_mMysql2Arrow;
VALUE ma_eMysql2Error;

void Init_mysql2_arrow(void);

void
Init_mysql2_arrow(void)
{
  ma_mMysql2Arrow = rb_define_module("Mysql2Arrow");

  ma_eMysql2Error = rb_path2class("Mysql2::Error");

  Init_mysql2_result_extension();
}
