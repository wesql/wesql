#!/usr/bin/env python3
"""Run each SQL statement against WeSQL and record hard fails only."""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SQL_DIR = ROOT / "sql"
OUT = ROOT / "results" / "compat-results.json"
HOST = os.environ.get("WESQL_HOST", "127.0.0.1")
PORT = os.environ.get("WESQL_PORT", "3307")
USER = os.environ.get("WESQL_USER", "root")
PASSWORD = os.environ.get("WESQL_PASSWORD", "passwd")
MYSQL = [
    "docker", "exec", "-i", os.environ.get("WESQL_CONTAINER", "wesql-compat"),
    "mysql", f"-u{USER}", f"-p{PASSWORD}",
    "--batch", "--raw",
]


def split_sql(text: str):
    buf = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("--"):
            continue
        buf.append(line)
        if s.endswith(";"):
            stmt = "\n".join(buf).strip()
            buf = []
            if stmt:
                yield stmt
    tail = "\n".join(buf).strip()
    if tail:
        yield tail


def run_sql(database: str, stmt: str):
    cmd = MYSQL + [database, "-e", stmt]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    err = p.stderr
    # mysql client prints password warning on stderr
    err_lines = [
        ln for ln in err.splitlines()
        if ln and "Using a password on the command line" not in ln
    ]
    return p.returncode, "\n".join(err_lines), p.stdout


def classify(err: str) -> str:
    e = err.lower()
    if "foreign key" in e:
        return "foreign_key"
    if "unsupported collation" in e or "collation" in e:
        return "collation"
    if "fulltext" in e:
        return "fulltext"
    if "spatial" in e or "geometry" in e:
        return "spatial_or_geometry"
    if "generated" in e or "virtual" in e:
        return "generated_column"
    if "serializable" in e or "isolation" in e:
        return "isolation"
    if "not support" in e or "doesn't support" in e or "not supported" in e:
        return "not_supported"
    return "other"


def main():
    results = []
    # create per-suite databases
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        suite = sql_file.stem
        db = "c_" + re.sub(r"[^a-z0-9_]", "_", suite)
        code, err, _ = run_sql("mysql", f"DROP DATABASE IF EXISTS `{db}`; CREATE DATABASE `{db}`;")
        if code != 0:
            results.append({
                "suite": suite, "db": db, "stmt": "CREATE DATABASE",
                "hard_fail": True, "class": "setup", "error": err,
            })
            continue
        for stmt in split_sql(sql_file.read_text()):
            code, err, out = run_sql(db, stmt)
            item = {
                "suite": suite,
                "db": db,
                "stmt": stmt,
                "hard_fail": code != 0,
                "class": classify(err) if code != 0 else "ok",
                "error": err,
            }
            results.append(item)
            status = "FAIL" if code != 0 else "ok"
            one = stmt.replace("\n", " ")[:90]
            print(f"[{suite}] {status}: {one}")
            if code != 0:
                print(f"    {err}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    fails = [r for r in results if r["hard_fail"]]
    print(f"\nTOTAL {len(results)} statements, HARD FAIL {len(fails)}")
    by = {}
    for r in fails:
        by.setdefault((r["suite"], r["class"]), 0)
        by[(r["suite"], r["class"])] += 1
    for k, n in sorted(by.items()):
        print(f"  {k[0]} / {k[1]}: {n}")


if __name__ == "__main__":
    sys.exit(main() or 0)
