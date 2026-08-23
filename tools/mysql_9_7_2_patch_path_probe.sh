#!/usr/bin/env bash

set -euo pipefail

readonly EXPECTED_WESQL_SHA=7c6d385931f2ef6c56d449bd30825b9981573cd5
readonly EXPECTED_MYSQL_SHA=008e09c2834b98143a8c067d4d225c90953050cf
readonly EXPECTED_PATHS=65

if [[ $# -ne 4 ]]; then
  echo "usage: $0 <wesql-tree> <mysql-9.7.2-tree> <wesql-commit> <patch-path>" >&2
  exit 2
fi

wesql_tree=$1
mysql_tree=$2
wesql_ref=$3
patch_path=$4

if [[ "$wesql_ref" != "$EXPECTED_WESQL_SHA" ]]; then
  echo "WeSQL ref must be the fixed 40-character SHA: $EXPECTED_WESQL_SHA" >&2
  exit 4
fi

require_clean_tree() {
  local tree=$1
  local label=$2

  if [[ -n "$(git -C "$tree" status --porcelain)" ]]; then
    echo "$label worktree is not clean: $tree" >&2
    exit 3
  fi
}

if ! resolved_wesql_sha=$(git -C "$wesql_tree" rev-parse --verify "${wesql_ref}^{commit}" 2>/dev/null); then
  echo "invalid WeSQL commit: $wesql_ref" >&2
  exit 4
fi
if [[ "$resolved_wesql_sha" != "$EXPECTED_WESQL_SHA" ]]; then
  echo "unexpected WeSQL commit: $resolved_wesql_sha" >&2
  exit 5
fi

resolved_mysql_sha=$(git -C "$mysql_tree" rev-parse --verify HEAD)
if [[ "$resolved_mysql_sha" != "$EXPECTED_MYSQL_SHA" ]]; then
  echo "unexpected MySQL commit: $resolved_mysql_sha" >&2
  exit 6
fi

if ! git -C "$wesql_tree" cat-file -e "${resolved_wesql_sha}:${patch_path}" 2>/dev/null; then
  echo "patch does not exist at fixed WeSQL commit: $patch_path" >&2
  exit 7
fi

require_clean_tree "$wesql_tree" "WeSQL"
require_clean_tree "$mysql_tree" "MySQL"

probe_tmp=$(mktemp -d "${TMPDIR:-/tmp}/wesql-972-probe.XXXXXX")
trap 'rm -rf "$probe_tmp"' EXIT

patch_file="$probe_tmp/full.patch"
path_file="$probe_tmp/paths.txt"
git -C "$wesql_tree" show "${resolved_wesql_sha}:${patch_path}" >"$patch_file"
sed -n 's#^diff --git a/\([^ ]*\) b/.*#\1#p' "$patch_file" >"$path_file"

total=$(wc -l <"$path_file" | tr -d ' ')
unique=$(sort -u "$path_file" | wc -l | tr -d ' ')
if [[ "$total" -ne "$EXPECTED_PATHS" ]]; then
  echo "unexpected patch path count: $total" >&2
  exit 8
fi
if [[ "$unique" -ne "$total" ]]; then
  echo "patch paths are not unique: total=$total unique=$unique" >&2
  exit 9
fi

applies=0
conflicts=0
section_number=0

printf 'path\tapply_exit\ttext_status\n'
while IFS= read -r path; do
  section_number=$((section_number + 1))
  printf -v section_file '%s/section-%03d.patch' \
    "$probe_tmp" "$section_number"
  awk -v target="$path" '
    /^diff --git / {
      selected = ($0 == "diff --git a/" target " b/" target)
    }
    selected { print }
  ' "$patch_file" >"$section_file"

  if [[ ! -s "$section_file" ]]; then
    echo "empty patch section: $path" >&2
    exit 10
  fi
  if ! head -n 1 "$section_file" | grep -Fqx "diff --git a/$path b/$path"; then
    echo "invalid patch section header: $path" >&2
    exit 11
  fi

  set +e
  git -C "$mysql_tree" apply --check "$section_file" >/dev/null 2>&1
  apply_exit=$?
  set -e

  if [[ "$apply_exit" -eq 0 ]]; then
    text_status=applies
    applies=$((applies + 1))
  else
    text_status=conflict
    conflicts=$((conflicts + 1))
  fi
  printf '%s\t%d\t%s\n' "$path" "$apply_exit" "$text_status"
done <"$path_file"

if [[ $((applies + conflicts)) -ne "$EXPECTED_PATHS" ]]; then
  echo "probe result count mismatch" >&2
  exit 12
fi

printf 'SUMMARY\t0\ttotal=%d applies=%d conflicts=%d\n' \
  "$total" "$applies" "$conflicts"
