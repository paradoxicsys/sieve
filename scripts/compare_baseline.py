#!/usr/bin/env python3
"""Run Sieve (1 thread and all threads) and the Python baseline on the same WET
files, then print throughput and per-rule drop-count agreement.

Usage (from the repo root, after `cargo build --release` and baseline setup):
    baseline/.venv/bin/python -I scripts/compare_baseline.py data/cc/FILE.warc.wet.gz [...]
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIEVE = os.path.join(ROOT, "target", "release", "sieve")
PY = os.path.join(ROOT, "baseline", ".venv", "bin", "python")


def run_sieve(inputs, threads, out):
    cmd = [SIEVE, "run", "-o", out, "--json", "-i", *inputs]
    if threads:
        cmd += ["--threads", str(threads)]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    r = json.loads(res.stdout)
    return {"wall": r["wall_seconds"], "docs": r["run_docs_in"], "kept": r["totals"]["kept"],
            "drops": r["totals"]["drops"]}


def run_python(inputs, out):
    res = subprocess.run([PY, "-I", os.path.join(ROOT, "baseline", "sieve_baseline.py"), "-o", out, *inputs],
                         capture_output=True, text=True, check=True)
    r = json.loads(res.stdout)
    return {"wall": r["wall_seconds"], "docs": r["docs_in"], "kept": r["kept"], "drops": r["drops"]}


def main():
    inputs = [os.path.abspath(p) for p in sys.argv[1:]]
    tmp = tempfile.mkdtemp(prefix="sieve-compare-")
    try:
        py = run_python(inputs, os.path.join(tmp, "py"))
        r1 = run_sieve(inputs, 1, os.path.join(tmp, "r1"))
        rn = run_sieve(inputs, None, os.path.join(tmp, "rn"))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"{'pipeline':<28}{'wall s':>9}{'docs/s':>10}{'speedup':>9}{'kept':>8}")
    for name, r in [("python baseline (1 core)", py), ("sieve --threads 1", r1), ("sieve (all cores)", rn)]:
        print(f"{name:<28}{r['wall']:>9.1f}{r['docs'] / r['wall']:>10.0f}{py['wall'] / r['wall']:>8.1f}x{r['kept']:>8}")

    print(f"\n{'rule':<36}{'sieve':>8}{'python':>8}{'diff':>7}")
    rules = sorted(set(rn["drops"]) | set(py["drops"]))
    exact = 0
    for rule in rules:
        a, b = rn["drops"].get(rule, 0), py["drops"].get(rule, 0)
        exact += a == b
        print(f"{rule:<36}{a:>8}{b:>8}{a - b:>+7}")
    print(f"\n{exact}/{len(rules)} rules with identical drop counts")


if __name__ == "__main__":
    main()
