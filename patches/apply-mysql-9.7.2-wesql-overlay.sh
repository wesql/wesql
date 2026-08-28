#!/usr/bin/env bash
# Apply patches/mysql-server-9.7.2.patch onto a clean Oracle MySQL 9.7.2 tree
# (commit 008e09c2834b98143a8c067d4d225c90953050cf).
# Does not copy SmartEngine / ObjectStore / archive sources; copy those from
# branch 9.7 after the patch.
set -euo pipefail
if [ "${1:-}" = "" ]; then
  echo "usage: $0 ORACLE_9_7_2_TREE" >&2
  exit 2
fi
TREE=$1
HERE=$(cd "$(dirname "$0")" && pwd)
PATCH=$HERE/mysql-server-9.7.2.patch
if [ ! -f "$PATCH" ]; then
  echo "missing patch: $PATCH" >&2
  exit 1
fi
git -C "$TREE" apply "$PATCH"
echo "applied $PATCH onto $TREE"
