#!/usr/bin/env bash
# Download the exact Common Crawl inputs used for the numbers in README.md:
# the first N WET files of crawl CC-MAIN-2024-10 segment 1707947473347.0 (~107 MB
# each, ~34k documents each), plus the first 100 MB of the matching WARC file
# (HTML responses, used by the extraction benchmark), plus the first 100 MB of
# each of the first 4 WARC files (data/warc, used for the multi-shard WARC run).
#
#   scripts/fetch_cc.sh [N=8]
set -euo pipefail
N="${1:-8}"
DEST="$(cd "$(dirname "$0")/.." && pwd)/data/cc"
BASE=https://data.commoncrawl.org
CRAWL=CC-MAIN-2024-10
mkdir -p "$DEST"
cd "$DEST"

curl -sf "$BASE/crawl-data/$CRAWL/wet.paths.gz" | gunzip | head -n "$N" | while read -r p; do
  f=$(basename "$p")
  [ -s "$f" ] && { echo "have $f"; continue; }
  echo "fetching $f"
  curl -sf -o "$f.tmp" "$BASE/$p" && mv "$f.tmp" "$f"
done

WARC=$(curl -sf "$BASE/crawl-data/$CRAWL/warc.paths.gz" | gunzip | head -n 1)
if [ ! -s warc-head-100MB.warc.gz.part ]; then
  echo "fetching first 100 MB of $(basename "$WARC")"
  # Byte-range request: the file is ~1 GB. The last gzip member is cut off,
  # which Sieve reports as a corrupt (truncated) shard: a useful real-world test.
  curl -sf -r 0-104857599 -o warc-head-100MB.warc.gz.part "$BASE/$WARC"
fi
# Heads (first 100 MB) of the first 4 WARC files, for the multi-shard HTML run.
mkdir -p ../warc
curl -sf "$BASE/crawl-data/$CRAWL/warc.paths.gz" | gunzip | head -n 4 | while read -r p; do
  n=$(basename "$p" .warc.gz | rev | cut -d- -f1 | rev)
  out=../warc/warc-$n-head.warc.gz
  [ -s "$out" ] || { echo "fetching head of $(basename "$p")"; curl -sf -r 0-104857599 -o "$out" "$BASE/$p"; }
done
ls -la "$DEST" ../warc
