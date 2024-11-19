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

#ifndef BINLOG_ARCHIVE_COMMAND_INCLUDED
#define BINLOG_ARCHIVE_COMMAND_INCLUDED

#include "sql/udf_service_impl.h"
#include "sql/udf_service_util.h"

class Binlog_archive_command : public Udf_service_impl {
 public:
  Binlog_archive_command() = default;
  ~Binlog_archive_command() override = default;
  bool init() override;
  bool deinit();

 private:
  Udf_charset_service m_charset_service;
  static constexpr const char *m_udf_name = "binlog_persistent_purge";
  bool m_initialized{false};
  static char *binlog_purge(UDF_INIT *, UDF_ARGS *args, char *result,
                    unsigned long *length, unsigned char *,
                    unsigned char *error);
  static bool binlog_purge_init(UDF_INIT *init_id, UDF_ARGS *args, char *message);
  static void binlog_purge_deinit(UDF_INIT *);
};
#endif
