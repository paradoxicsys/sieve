#!/usr/bin/env python3
"""Summarise a folded profile (inferno-collapse-sample output) for one thread group.

Usage: python3 scripts/profile_breakdown.py profiles/X.folded [thread-name-prefix=process-]
Prints the share of samples inside each pipeline function (inclusive) and the
top self-time leaf frames, for threads whose name starts with the prefix.
"""
import re
import sys
from collections import Counter

path = sys.argv[1]
prefix = sys.argv[2] if len(sys.argv) > 2 else "process-"
KEYS = ["detect_with_options", "GopherRepetition", "GopherQuality", "C4Filter", "BadWordsFilter",
        "LengthFilter", "Fingerprinter", "signature_of_shingles", "MinHasher::shingles", "normalize_for_dedup",
        "html_to_text", "RegexRedactor", "par_bridge", "IterBridge", "psynch_mutexwait", "swtch_pri",
        "crossbeam_channel", "process_record"]
incl, self_c, total = Counter(), Counter(), 0
for line in open(path):
    stack, n = line.rsplit(" ", 1)
    frames = stack.split(";")
    if not re.search(r": " + re.escape(prefix), frames[0]):
        continue
    n = int(n)
    total += n
    self_c[frames[-1][:100]] += n
    hit = set()
    for f in frames:
        for k in KEYS:
            if k in f and k not in hit:
                incl[k] += n
                hit.add(k)
print(f"{total} samples on threads '{prefix}*'")
for k, v in incl.most_common():
    print(f"  inclusive {100 * v / total:5.1f}%  {k}")
print("top self frames:")
for k, v in self_c.most_common(15):
    print(f"  {100 * v / total:5.1f}%  {k}")
