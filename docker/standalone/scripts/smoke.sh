#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

password="${MYSQL_ROOT_PASSWORD:-passwd}"

docker compose exec -T wesql mysql -uroot -p"${password}" <<'SQL'
CREATE DATABASE IF NOT EXISTS wesql_trial;
USE wesql_trial;
DROP TABLE IF EXISTS smoke;
CREATE TABLE smoke (
  id INT NOT NULL PRIMARY KEY,
  name VARCHAR(32) NOT NULL
) ENGINE=SMARTENGINE;
INSERT INTO smoke VALUES (1, 'after-start');
SELECT id, name FROM smoke;
SQL

echo "smoke: created wesql_trial.smoke and inserted 1 row"
