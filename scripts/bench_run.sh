#!/usr/bin/env bash
# Time N full runs of the release binary over data/cc and print docs/s and peak RSS.
# Usage: scripts/bench_run.sh LABEL [N=2] [extra sieve args...]
set -euo pipefail
LABEL=$1; N=${2:-2}; shift 2 || true
for i in $(seq 1 "$N"); do
  rm -rf out/bench
  /usr/bin/time -l ./target/release/sieve run -i data/cc -o out/bench "$@" 2> out/bench.log >/dev/null
  docs=$(grep -o 'this run: [0-9]* docs in [0-9.]*s = [0-9]* docs/s' out/bench.log)
  rss=$(awk '/maximum resident set size/ {printf "%.0f MiB", $1/1048576}' out/bench.log)
  echo "$LABEL run $i: $docs, peak RSS $rss"
done
