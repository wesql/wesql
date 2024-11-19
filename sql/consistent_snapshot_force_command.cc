/*
  Portions Copyright (c) 2024, ApeCloud Inc Holding Limited 
  Portions Copyright (c) 2009, 2023, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/consistent_snapshot_force_command.h"

#include "m_string.h"
#include "my_dbug.h"
#include "mysqld_error.h"
#include "sql/consistent_archive.h"
#include "sql/sql_class.h"

bool Consistent_archive_force_command::init() {
  DBUG_TRACE;

  Udf_data udf(m_udf_name, STRING_RESULT,
               Consistent_archive_force_command::consistent_archive_force,
               Consistent_archive_force_command::consistent_archive_force_init,
               Consistent_archive_force_command::consistent_archive_force_deinit);

  m_initialized = !register_udf(udf);
  return !m_initialized;
}

bool Consistent_archive_force_command::deinit() {
  DBUG_TRACE;

  if (m_initialized && !unregister_udf(m_udf_name)) {
    m_initialized = false;
  }

  return m_initialized;
}

char *Consistent_archive_force_command::consistent_archive_force(
    UDF_INIT *, UDF_ARGS *, char *result, unsigned long *length,
    unsigned char *, unsigned char *error) {
  DBUG_TRACE;
  Consistent_archive *consistent_archive = Consistent_archive::get_instance();
  *error = 0;
  std::string err_msg{};  // error message

  if (!consistent_archive) {
    *error = 1;
    err_msg.assign("consistent snapshot archive  is not started.");
    goto err;
  }

  consistent_archive->signal_consistent_archive();

err:
  strcpy(result, err_msg.c_str());
  *length = err_msg.length();
  return result;
}

bool Consistent_archive_force_command::consistent_archive_force_init(
    UDF_INIT *init_id, UDF_ARGS *args, char *message) {
  DBUG_TRACE;
  if (args->arg_count > 0) {
    my_stpcpy(message, "Wrong arguments: The function takes no arguments.");
    return true;
  }

  if (Udf_charset_service::set_return_value_charset(init_id) ||
      Udf_charset_service::set_args_charset(args)) {
    return true;
  }

  init_id->maybe_null = false;
  return false;
}

void Consistent_archive_force_command::consistent_archive_force_deinit(UDF_INIT *) {
  DBUG_TRACE;
}
