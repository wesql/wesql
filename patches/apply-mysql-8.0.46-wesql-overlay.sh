#!/usr/bin/env bash
# Apply patches/mysql-server-8.0.46.patch onto a clean Oracle MySQL 8.0.46 tree
# (tag mysql-8.0.46, commit 0a7df2e4693d8f10901a26034ae6257699356e30).
set -euo pipefail
if [ "${1:-}" = "" ]; then
  echo "usage: $0 ORACLE_8_0_46_TREE" >&2
  exit 2
fi
TREE=$1
HERE=$(cd "$(dirname "$0")" && pwd)
PATCH=$HERE/mysql-server-8.0.46.patch
if [ ! -f "$PATCH" ]; then
  echo "missing patch: $PATCH" >&2
  exit 1
fi
git -C "$TREE" apply "$PATCH"
echo "applied $PATCH onto $TREE"
