#!/bin/bash
# Apply WeSQL patches onto a MySQL 8.0.35 tree (after overlay copy).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
if [ ! -f sql/mysqld.cc ]; then
  echo "run from the prepared MySQL+WeSQL source root" >&2
  exit 1
fi
patch -p1 --forward --no-backup-if-mismatch < "$ROOT/patches/mysql-server-8.0.35.patch"
patch -p1 --forward --no-backup-if-mismatch < "$ROOT/patches/mysql-server-8.0.35-orm-ddl-rewrite.patch"
echo "Applied WeSQL patches including ORM DDL rewrite."
