#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "usage: $0 <wesql-tree> <mysql-9.7.2-tree> [wesql-ref] [patch-path]" >&2
  exit 2
fi

wesql_tree=$1
mysql_tree=$2
wesql_ref=${3:-HEAD}
patch_path=${4:-patches/mysql-server-8.0.35.patch}

patch_stream() {
  git -C "$wesql_tree" show "$wesql_ref:$patch_path"
}

total=0
applies=0
conflicts=0

printf 'path\tapply_exit\ttext_status\n'
while IFS= read -r path; do
  total=$((total + 1))
  # Consume the whole patch stream. Exiting after the selected section would
  # send SIGPIPE to git show and make pipefail report a false conflict.
  if patch_stream |
    awk -v target="$path" '
      /^diff --git / {
        selected = ($0 == "diff --git a/" target " b/" target)
      }
      selected { print }
    ' |
    git -C "$mysql_tree" apply --check >/dev/null 2>&1; then
    apply_exit=0
    printf '%s\t%d\tapplies\n' "$path" "$apply_exit"
    applies=$((applies + 1))
  else
    apply_exit=$?
    printf '%s\t%d\tconflict\n' "$path" "$apply_exit"
    conflicts=$((conflicts + 1))
  fi
done < <(
  patch_stream |
    sed -n 's#^diff --git a/\([^ ]*\) b/.*#\1#p'
)

printf 'SUMMARY\t0\ttotal=%d applies=%d conflicts=%d\n' \
  "$total" "$applies" "$conflicts"
