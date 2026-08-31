#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

docker compose down -v --remove-orphans
echo "cleaned: containers and volumes removed"
