#!/usr/bin/env python3
"""Exercise the release fault control itself through separate processes."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time


parser = argparse.ArgumentParser()
parser.add_argument("--driver", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
args.output.mkdir()
point = "remote_commit_crash_before_segment_put"
results = []
for scenario in ("disabled", "unarmed", "crash", "pause", "invalid", "symlink"):
    root = args.output / scenario
    root.mkdir()
    env = dict(os.environ)
    env.pop("WESQL_REMOTE_COMMIT_FAULT_DIR", None)
    if scenario != "disabled":
        env["WESQL_REMOTE_COMMIT_FAULT_DIR"] = str(root)
    if scenario in {"crash", "pause", "invalid", "disabled"}:
        (root / (point + ".arm")).write_text("crash\n" if scenario == "disabled" else scenario + "\n")
    if scenario == "symlink":
        (root / "target").write_text("crash\n")
        (root / (point + ".arm")).symlink_to(root / "target")
    child = subprocess.Popen([str(args.driver), point], env=env)
    if scenario == "pause":
        deadline = time.monotonic() + 10
        while not (root / (point + ".hit")).exists():
            if child.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError("pause did not reach its marker")
            time.sleep(0.01)
        assert child.poll() is None
        (root / (point + ".release")).touch()
    code = child.wait(timeout=10)
    expected = 86 if scenario in {"crash", "invalid", "symlink"} else 0
    assert code == expected, (scenario, code)
    if scenario in {"crash", "pause"}:
        hit = json.loads((root / (point + ".hit")).read_text())
        assert hit == {"point": point, "pid": child.pid, "action": scenario}
        assert (root / (point + ".claimed")).exists()
        assert not (root / (point + ".arm")).exists()
        assert subprocess.run([str(args.driver), point], env=env).returncode == 0
    else:
        assert not (root / (point + ".hit")).exists()
    results.append({"scenario": scenario, "exit": code})
(args.output / "RESULT.json").write_text(json.dumps(results, indent=2) + "\n")
print("PASS: release control disabled/unarmed/crash/pause/one-shot/invalid/symlink")
