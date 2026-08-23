#!/bin/bash
# Apply all WeSQL patches onto a MySQL 8.0.35 tree (after overlay copy).
# This is the single apply entry for local builds and CI.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
if [ ! -f sql/mysqld.cc ]; then
  echo "run from the prepared MySQL+WeSQL source root" >&2
  exit 1
fi

apply_one() {
  local p="$1"
  if [ ! -f "$p" ]; then
    echo "missing patch $p" >&2
    exit 1
  fi
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git apply "$p"
  else
    patch -p1 --forward --no-backup-if-mismatch < "$p"
  fi
}

apply_one "$ROOT/patches/mysql-server-8.0.35.patch"
apply_one "$ROOT/patches/mysql-server-8.0.35-orm-ddl-rewrite.patch"
echo "Applied WeSQL patches including ORM DDL rewrite."
