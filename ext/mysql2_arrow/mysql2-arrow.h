#ifndef MYSQL2_ARROW_H
#define MYSQL2_ARROW_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <ruby.h>
#include <ruby/encoding.h>

#ifdef HAVE_MYSQL_H
#include <mysql.h>
#include <mysql_com.h>
#include <errmsg.h>
#include <mysqld_error.h>
#else
#include <mysql/mysql.h>
#include <mysql/mysql_com.h>
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>
#endif

#if defined(__GNUC__) && (__GNUC__ >= 3)
#define RB_MYSQL_NORETURN __attribute__ ((noreturn))
#define RB_MYSQL_UNUSED __attribute__ ((unused))
#else
#define RB_MYSQL_NORETURN
#define RB_MYSQL_UNUSED
#endif

#include <mysql2/client.h>
#include <mysql2/statement.h>
#include <mysql2/result.h>

#define GET_RESULT(self) \
  mysql2_result_wrapper *wrapper; \
  Data_Get_Struct(self, mysql2_result_wrapper, wrapper);

void Init_mysql2_result_extension(void);

extern VALUE ma_mMysql2Arrow;
extern VALUE ma_eMysql2Error;

#ifdef __cplusplus
}  // extern "C"
#endif

#endif /* MYSQL2_ARROW_H */
