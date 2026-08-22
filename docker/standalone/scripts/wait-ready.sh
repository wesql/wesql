#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

timeout_s="${1:-180}"
echo "waiting for WeSQL to become healthy (timeout ${timeout_s}s)"

start="$(date +%s)"
while true; do
  status="$(docker compose ps --format '{{.Service}} {{.Health}}' | awk '$1=="wesql"{print $2}')"
  if [[ "$status" == "healthy" ]]; then
    echo "wesql is healthy"
    exit 0
  fi
  now="$(date +%s)"
  if (( now - start >= timeout_s )); then
    echo "timeout waiting for wesql health; current=${status:-unknown}" >&2
    docker compose ps >&2 || true
    docker compose logs --tail=80 wesql >&2 || true
    exit 1
  fi
  sleep 3
done
