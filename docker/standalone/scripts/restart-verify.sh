#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

password="${MYSQL_ROOT_PASSWORD:-passwd}"

echo "restarting wesql only; MinIO and volumes stay"
docker compose restart wesql
"$ROOT/scripts/wait-ready.sh" 180

row="$(docker compose exec -T wesql mysql -N -uroot -p"${password}" -e "SELECT name FROM wesql_trial.smoke WHERE id=1;")"
row="$(printf '%s' "$row" | tr -d '\r')"

if [[ "$row" != "after-start" ]]; then
  echo "restart verify failed: expected after-start, got '${row}'" >&2
  exit 1
fi

echo "restart verify: row still present after wesql restart"
