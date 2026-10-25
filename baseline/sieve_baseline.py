#!/usr/bin/env python3
"""Single-process Python baseline for Sieve: same stages, same filter rules.

Purpose: a fair point of comparison for Sieve's throughput, and an independent
re-implementation that cross-checks the Rust filters (their per-rule drop counts
should agree closely; see scripts/compare_baseline.py).

What "fair" means here:
* Filters are ported rule-for-rule from crates/sieve-core/src/filter/*.rs, with
  the same order, thresholds, and character-count definitions.
* Language ID calls the same Rust library (whatlang, via the `whatlang-pyo3`
  bindings) so both pipelines keep the same documents; it is native code in both.
* MinHash is vectorised with numpy the way `datasketch` does it, rather than a
  pure-Python triple loop, because that is what a competent Python pipeline uses.
* Parquet is written with pyarrow (C++), as any Python pipeline would.
Everything else (WARC parsing, filters, dedup index, PII regex) is plain Python.

Known, intentional differences from Sieve: hash functions (Python's `hash()` /
numpy instead of xxh3), so near-duplicate decisions can differ for borderline
pairs; and datasketch's wrapping 64-bit multiply instead of exact mod-p
arithmetic. Unicode predicates (`str.isalnum`, `str.isspace`) differ from Rust's
in rare non-Latin cases.

Usage: python baseline/sieve_baseline.py INPUT.warc.wet.gz [...] -o OUT_DIR
"""

import argparse
import gzip
import json
import os
import re
import sys
import time
from collections import Counter

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import whatlang

HERE = os.path.dirname(os.path.abspath(__file__))
BADWORDS = os.path.join(HERE, "..", "crates", "sieve-core", "resources", "badwords_en.txt")

# ---- config (mirrors crates/sieve-core/src/config.rs defaults) ----------------
MIN_WORDS, MAX_WORDS = 50, 100_000
LANG_ALLOWED, LANG_MIN_CONF, LANG_SAMPLE_BYTES = {"eng"}, 0.3, 4096
DUP_LINE_FRAC, DUP_PARA_FRAC, DUP_LINE_CHAR_FRAC, DUP_PARA_CHAR_FRAC = 0.3, 0.3, 0.2, 0.2
TOP_NGRAM = [(2, 0.20), (3, 0.18), (4, 0.16)]
DUP_NGRAM = [(5, 0.15), (6, 0.14), (7, 0.13), (8, 0.12), (9, 0.11), (10, 0.10)]
MIN_MEAN_WORD, MAX_MEAN_WORD = 3.0, 10.0
MAX_SYMBOL_RATIO, MAX_BULLET_FRAC, MAX_ELLIPSIS_FRAC, MIN_ALPHA_FRAC = 0.1, 0.9, 0.3, 0.8
STOP_WORDS, MIN_STOP_WORDS = {"the", "be", "to", "of", "and", "that", "have", "with"}, 2
C4_MIN_WORDS_LINE, C4_MIN_SENTENCES = 3, 5
TERMINAL = (".", "!", "?", '"', "'", "”", "’")
POLICY = ("terms of use", "privacy policy", "cookie policy", "uses cookies", "use of cookies", "use cookies")
NUM_PERM, BANDS, ROWS, SHINGLE = 128, 16, 8, 5
MERSENNE = (1 << 61) - 1
MAX_HASH = (1 << 32) - 1


class Reject(Exception):
    def __init__(self, rule, value=float("nan")):
        self.rule, self.value = rule, value


# ---- WARC reading -------------------------------------------------------------
def read_records(path):
    """Yield (headers, body) for each record. gzip.open handles multi-member files."""
    opener = gzip.open if open(path, "rb").read(2) == b"\x1f\x8b" else open
    with opener(path, "rb") as f:
        while True:
            line = f.readline()
            if not line:
                return
            if not line.strip():
                continue
            headers = {}
            while True:
                line = f.readline()
                if not line or line in (b"\r\n", b"\n"):
                    break
                k, _, v = line.decode("utf-8", "replace").partition(":")
                headers[k.strip().lower()] = v.strip()
            body = f.read(int(headers.get("content-length", 0)))
            yield headers, body


# ---- text helpers (crates/sieve-core/src/text.rs) -----------------------------
def normalize(text):
    out, pending = [], False
    for c in text:
        if c.isalnum():
            if pending and out:
                out.append(" ")
            pending = False
            out.append(c.lower())
        else:
            pending = True
    return "".join(out)


def lines(text):
    return [l.strip() for l in text.split("\n") if l.strip()]


def paragraphs(text):
    return [p.strip() for p in text.split("\n\n") if p.strip()]


def ratio(a, b):
    return 0.0 if b == 0 else a / b


# ---- filters (crates/sieve-core/src/filter/*.rs) ------------------------------
def f_length(text):
    n = len(text.split())
    if n < MIN_WORDS:
        raise Reject("length.min_words", n)
    if n > MAX_WORDS:
        raise Reject("length.max_words", n)


def f_language(text):
    sample = text.encode("utf-8")[:LANG_SAMPLE_BYTES].decode("utf-8", "ignore")
    try:
        info = whatlang.detect(sample)
    except ValueError:  # the binding raises instead of returning None
        raise Reject("lang.undetected")
    if info.lang not in LANG_ALLOWED:
        raise Reject("lang.not_allowed", info.confidence)
    if info.confidence < LANG_MIN_CONF:
        raise Reject("lang.low_confidence", info.confidence)


def dup_stats(items):
    seen, dups, dup_chars = set(), 0, 0
    for it in items:
        if it in seen:
            dups += 1
            dup_chars += len(it)
        else:
            seen.add(it)
    return ratio(dups, len(items)), dup_chars


def top_ngram_chars(words, lens, n):
    if len(words) < n:
        return 0
    counts, first = Counter(), {}
    for i in range(len(words) - n + 1):
        g = tuple(words[i:i + n])
        counts[g] += 1
        first.setdefault(g, i)
    best = max(counts.items(), key=lambda kv: (kv[1], -first[kv[0]]))
    i = first[best[0]]
    return best[1] * (sum(lens[i:i + n]) + n - 1)


def dup_ngram_chars(words, lens, n):
    seen, i, rep = set(), 0, 0
    while i + n <= len(words):
        g = tuple(words[i:i + n])
        if g in seen:
            rep += sum(lens[i:i + n])
            i += n
        else:
            seen.add(g)
            i += 1
    return rep


def f_gopher_repetition(text):
    tc = len(text)
    frac, chars = dup_stats(paragraphs(text))
    if frac > DUP_PARA_FRAC:
        raise Reject("gopher_rep.dup_para_frac", frac)
    if ratio(chars, tc) > DUP_PARA_CHAR_FRAC:
        raise Reject("gopher_rep.dup_para_char_frac", ratio(chars, tc))
    frac, chars = dup_stats(lines(text))
    if frac > DUP_LINE_FRAC:
        raise Reject("gopher_rep.dup_line_frac", frac)
    if ratio(chars, tc) > DUP_LINE_CHAR_FRAC:
        raise Reject("gopher_rep.dup_line_char_frac", ratio(chars, tc))
    words = text.split()
    lens = [len(w) for w in words]
    for n, mx in TOP_NGRAM:
        v = ratio(top_ngram_chars(words, lens, n), tc)
        if v > mx:
            raise Reject("gopher_rep.top_ngram_char_frac", v)
    for n, mx in DUP_NGRAM:
        v = ratio(dup_ngram_chars(words, lens, n), tc)
        if v > mx:
            raise Reject("gopher_rep.dup_ngram_char_frac", v)


def f_gopher_quality(text):
    words = text.split()
    n = len(words)
    if n == 0:
        raise Reject("gopher_quality.no_words", 0)
    mean = sum(len(w) for w in words) / n
    if mean < MIN_MEAN_WORD or mean > MAX_MEAN_WORD:
        raise Reject("gopher_quality.mean_word_len", mean)
    sym = ratio(max(text.count("#"), text.count("...") + text.count("…")), n)
    if sym > MAX_SYMBOL_RATIO:
        raise Reject("gopher_quality.symbol_word_ratio", sym)
    ls = lines(text)
    v = ratio(sum(1 for l in ls if l.startswith(("•", "-", "*"))), len(ls))
    if v > MAX_BULLET_FRAC:
        raise Reject("gopher_quality.bullet_lines", v)
    v = ratio(sum(1 for l in ls if l.endswith(("...", "…"))), len(ls))
    if v > MAX_ELLIPSIS_FRAC:
        raise Reject("gopher_quality.ellipsis_lines", v)
    alpha = ratio(sum(1 for w in words if any(c.isalpha() for c in w)), n)
    if alpha < MIN_ALPHA_FRAC:
        raise Reject("gopher_quality.alpha_words", alpha)
    stops = {w.lower() for w in words if len(w) <= 4} & STOP_WORDS
    if len(stops) < MIN_STOP_WORDS:
        raise Reject("gopher_quality.stop_words", len(stops))


def count_sentences(text):
    n = 0
    for i, c in enumerate(text):
        if c in ".!?" and (i + 1 == len(text) or text[i + 1].isspace()):
            n += 1
    return n


def f_c4(text):
    low = text.lower()
    if "lorem ipsum" in low:
        raise Reject("c4.lorem_ipsum")
    if "{" in text:
        raise Reject("c4.curly_brace")
    kept = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        ll = line.lower()
        ws = line.split()
        if (len(ws) >= C4_MIN_WORDS_LINE and line.endswith(TERMINAL) and "javascript" not in ll
                and not any(p in ll for p in POLICY) and not any(len(w) > 1000 for w in ws)):
            kept.append(line)
    text = "\n".join(kept)
    n = count_sentences(text)
    if n < C4_MIN_SENTENCES:
        raise Reject("c4.min_sentences", n)
    return text


def load_badwords():
    phrases, prefixes, max_words = set(), set(), 1
    for e in open(BADWORDS, encoding="utf-8"):
        e = e.strip()
        if not e:
            continue
        norm = normalize(e)
        ws = norm.split(" ")
        if not norm or any(len(w) < 2 for w in ws):
            continue
        for k in range(1, len(ws)):
            prefixes.add(" ".join(ws[:k]))
        max_words = max(max_words, len(ws))
        phrases.add(norm)
    return phrases, prefixes, max_words


BAD = load_badwords()


def f_badwords(text):
    phrases, prefixes, max_words = BAD
    words = [w for w in normalize(text).split(" ") if w]
    n = 0
    for i in range(len(words)):
        buf = ""
        for k, w in enumerate(words[i:i + max_words]):
            buf = w if k == 0 else buf + " " + w
            if buf in phrases:
                n += 1
                break
            if buf not in prefixes:
                break
    if n > 0:
        raise Reject("bad_words", n)


def apply_filters(text):
    f_length(text)
    f_language(text)
    f_gopher_repetition(text)
    f_gopher_quality(text)
    text = f_c4(text)
    f_badwords(text)
    return text


# ---- dedup (datasketch-style numpy MinHash + LSH) -----------------------------
def splitmix64(state):
    state = (state + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    z = state
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return state, z ^ (z >> 31)


def perm_params(seed=0x5EED0F51E7E5):
    a, b, st = [], [], seed
    for _ in range(NUM_PERM):
        st, x = splitmix64(st)
        a.append(x % (MERSENNE - 1) + 1)
        st, y = splitmix64(st)
        b.append(y % MERSENNE)
    return np.array(a, dtype=np.uint64)[:, None], np.array(b, dtype=np.uint64)[:, None]


PERM_A, PERM_B = perm_params()


def fingerprint(text):
    norm = normalize(text)
    exact = hash(norm)
    words = [w for w in norm.split(" ") if w]
    if not words:
        return exact, [hash(("empty", b)) for b in range(BANDS)]
    k = min(SHINGLE, len(words))
    hv = np.array([hash(tuple(words[i:i + k])) & 0xFFFFFFFFFFFFFFFF
                   for i in range(len(words) - k + 1)], dtype=np.uint64)
    # datasketch's formula: wrapping uint64 multiply, then mod p, then 32-bit mask.
    with np.errstate(over="ignore"):
        phv = np.bitwise_and((PERM_A * hv[None, :] + PERM_B) % np.uint64(MERSENNE), np.uint64(MAX_HASH))
    sig = phv.min(axis=1)
    return exact, [hash((b,) + tuple(sig[b * ROWS:(b + 1) * ROWS].tolist())) for b in range(BANDS)]


# ---- PII (crates/sieve-core/src/pii.rs, same patterns and order) ---------------
PII = re.compile("|".join([
    r"(?P<email>\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}\b)",
    r"(?P<ipv6>\b(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}\b)",
    r"(?P<ipv4>\b(?:\d{1,3}\.){3}\d{1,3}\b)",
    r"(?P<phone>(?:\+\d{1,3}[ .-]?)?(?:\(\d{2,4}\)[ .-]?|\b\d{2,4}[ .-])\d{3,4}[ .-]\d{3,4}\b)",
]))


def redact(text):
    count = 0

    def sub(m):
        nonlocal count
        if m.group("email"):
            count += 1
            return "<EMAIL_ADDRESS>"
        if m.group("ipv4"):
            if all(int(o) <= 255 for o in m.group("ipv4").split(".")):
                count += 1
                return "<IP_ADDRESS>"
            return m.group(0)
        count += 1
        return "<IP_ADDRESS>" if m.group("ipv6") else "<PHONE_NUMBER>"

    return PII.sub(sub, text), count


# ---- main ------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+")
    ap.add_argument("-o", "--output", required=True)
    args = ap.parse_args()
    os.makedirs(args.output, exist_ok=True)

    t0 = time.perf_counter()
    exact_seen, band_tables = set(), [set() for _ in range(BANDS)]
    drops, docs_in, kept_total, pii_total = Counter(), 0, 0, 0
    stage_s = Counter()
    for path in sorted(args.inputs):
        rows = {"url": [], "record_id": [], "text": [], "pii_redactions": []}
        for headers, body in read_records(path):
            if headers.get("warc-type") != "conversion":
                continue
            docs_in += 1
            url = headers.get("warc-target-uri", "")
            text = body.decode("utf-8", "replace")
            ts = time.perf_counter()
            try:
                text = apply_filters(text)
            except Reject as r:
                drops[r.rule] += 1
                stage_s["filter"] += time.perf_counter() - ts
                continue
            t1 = time.perf_counter()
            stage_s["filter"] += t1 - ts
            exact, bands = fingerprint(text)
            t2 = time.perf_counter()
            stage_s["fingerprint"] += t2 - t1
            if exact in exact_seen:
                drops["dedup.exact"] += 1
                continue
            if any(k in t for t, k in zip(band_tables, bands)):
                drops["dedup.near"] += 1
                continue
            exact_seen.add(exact)
            for t, k in zip(band_tables, bands):
                t.add(k)
            text, n = redact(text)
            stage_s["redact"] += time.perf_counter() - t2
            pii_total += n
            rows["url"].append(url)
            rows["record_id"].append(headers.get("warc-record-id", ""))
            rows["text"].append(text)
            rows["pii_redactions"].append(n)
        ts = time.perf_counter()
        name = os.path.basename(path).split(".")[0] + ".parquet"
        pq.write_table(pa.table(rows), os.path.join(args.output, name), compression="zstd", compression_level=3)
        stage_s["sink"] += time.perf_counter() - ts
        kept_total += len(rows["url"])
    wall = time.perf_counter() - t0
    report = {
        "wall_seconds": wall,
        "docs_in": docs_in,
        "docs_per_sec": docs_in / wall,
        "kept": kept_total,
        "drops": dict(sorted(drops.items())),
        "pii_redactions": pii_total,
        "stage_seconds": dict(stage_s),
    }
    json.dump(report, open(os.path.join(args.output, "_report.json"), "w"), indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    sys.exit(main())
