#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "1/5 start MinIO + WeSQL"
docker compose up -d

echo "2/5 wait until ready"
"$ROOT/scripts/wait-ready.sh" 180

echo "3/5 connect and create a table"
"$ROOT/scripts/smoke.sh"

echo "4/5 restart WeSQL and check the row"
"$ROOT/scripts/restart-verify.sh"

echo "5/5 recent logs"
"$ROOT/scripts/logs.sh" 80

echo
echo "trial loop passed."
echo "connect: docker compose exec wesql mysql -uroot -p${MYSQL_ROOT_PASSWORD:-passwd}"
echo "cleanup: ./scripts/cleanup.sh"
