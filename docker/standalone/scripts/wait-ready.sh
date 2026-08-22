#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

timeout_s="${1:-180}"
password="${MYSQL_ROOT_PASSWORD:-passwd}"
echo "waiting for WeSQL health and writable leader (timeout ${timeout_s}s)"

start="$(date +%s)"
healthy=0

ready_row() {
  docker compose exec -T -e MYSQL_PWD="${password}" wesql \
    mysql -N -s -uroot -e \
    "SELECT CONCAT(ROLE, ' ', SERVER_READY_FOR_RW) FROM INFORMATION_SCHEMA.WESQL_CLUSTER_LOCAL LIMIT 1;" \
    2>/dev/null | tr -d '\r'
}

while true; do
  now="$(date +%s)"
  if (( now - start >= timeout_s )); then
    echo "timeout waiting for writable leader" >&2
    docker compose ps >&2 || true
    docker compose logs --tail=80 wesql >&2 || true
    exit 1
  fi

  if [[ "$healthy" -eq 0 ]]; then
    status="$(docker compose ps --format '{{.Service}} {{.Health}}' | awk '$1=="wesql"{print $2}')"
    if [[ "$status" == "healthy" ]]; then
      echo "wesql is healthy; waiting for ROLE=Leader and SERVER_READY_FOR_RW=Yes"
      healthy=1
    fi
  else
    row="$(ready_row || true)"
    if [[ "$row" == "Leader Yes" ]]; then
      echo "wesql is writable leader"
      exit 0
    fi
  fi
  sleep 3
done
