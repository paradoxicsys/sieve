#!/usr/bin/env python3
"""Generate the small, deterministic WARC/WET fixtures committed under tests/data.

Synthetic on purpose: every document's fate (kept, which filter drops it, which
duplicate it is) is known in advance, so the integration tests can assert exact
outcomes. Real Common Crawl pages would also drag real people's PII into the repo.

Usage: python3 scripts/make_sample.py   (stdlib only; rewrites tests/data/*)
"""

import gzip
import hashlib
import os
import random

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tests", "data")

ADJ = ["quiet", "ancient", "busy", "narrow", "golden", "remote", "famous", "modern", "gentle",
       "crowded", "wooden", "colorful", "historic", "peaceful", "rocky", "sunny", "foggy", "humble",
       "elegant", "curious", "patient", "careful", "brave", "clever", "lively", "sturdy"]
NOUN = ["harbor", "village", "library", "bridge", "garden", "market", "museum", "orchard", "lighthouse",
        "railway", "festival", "bakery", "workshop", "observatory", "vineyard", "monastery", "canal",
        "meadow", "theater", "school", "farmhouse", "glacier", "island", "forest", "castle", "river"]
NOUN_PL = ["fishermen", "students", "farmers", "painters", "engineers", "travelers", "musicians",
           "historians", "volunteers", "gardeners", "sailors", "teachers", "bakers", "climbers"]
VERB_PAST = ["visited", "restored", "photographed", "described", "explored", "measured", "repaired",
             "celebrated", "mapped", "studied", "documented", "rebuilt", "admired", "protected"]
VERB = ["return", "gather", "meet", "work", "travel", "perform", "trade", "rest", "celebrate"]
PLACE = ["northern coast", "old town square", "mountain pass", "river valley", "eastern hills",
         "city center", "southern plains", "lake shore", "western ridge", "harbor district"]
NAMES = ["Maria", "Tomas", "Aisha", "Kenji", "Elena", "Samuel", "Priya", "Lucas", "Nadia", "Oscar"]
TOPICS = ["local history", "migrating birds", "traditional boats", "old maps", "stone walls",
          "seasonal weather", "folk music", "ancient roads", "rare plants", "tidal patterns"]
TIME = ["spring", "summer", "autumn", "winter", "year", "month"]

TEMPLATES = [
    "The {adj} {noun} near the {place} was {verb_past} by a group of {noun_pl} last {time}.",
    "Many people believe that the {noun} should remain {adj} and open to everyone.",
    "In {year}, several {noun_pl} {verb_past} the {place} to learn more about {topic}.",
    "It was not easy, but {name} finally {verb_past} the {adj} {noun} with help from friends.",
    "Experts say this is one of the most {adj} places of its kind in the whole region.",
    "{name} said that the {noun_pl} would {verb} at the {noun} again next {time}.",
    "Because of this, the {place} has become known for its {adj} {noun} and friendly {noun_pl}.",
    "Visitors who come to the {noun} often stay for hours and talk with the {noun_pl}.",
    "A small sign at the entrance explains how the {noun} was {verb_past} over many years.",
    "Some of the {noun_pl} have lived in the {place} for most of their lives.",
    "There is a {adj} path that leads from the {noun} to the {place}, and it is worth the walk.",
    "According to {name}, the best time to see the {noun} is early in the morning.",
]


def sentence(rng, t):
    return t.format(adj=rng.choice(ADJ), noun=rng.choice(NOUN), noun_pl=rng.choice(NOUN_PL),
                    verb_past=rng.choice(VERB_PAST), verb=rng.choice(VERB), place=rng.choice(PLACE),
                    name=rng.choice(NAMES), topic=rng.choice(TOPICS), time=rng.choice(TIME),
                    year=rng.randint(1890, 2020))


def article(seed):
    """A deterministic English article: 3 paragraphs (one line each) of 4 sentences.

    Each template is used exactly once per article, so no long word sequences
    repeat inside a document (which the Gopher n-gram rules would penalise).
    """
    rng = random.Random(seed)
    order = TEMPLATES[:]
    rng.shuffle(order)
    sents = [sentence(rng, t) for t in order]
    return [" ".join(sents[i:i + 4]) for i in range(0, 12, 4)]


def html_page(title, paras, nav=True, extra_body=""):
    nav_html = ('<nav><a href="/">Home</a> | <a href="/news">News</a> | <a href="/about">About</a></nav>'
                if nav else "")
    body = "".join(f"<p>{p}</p>\n" for p in paras)
    return (f"<!DOCTYPE html><html><head><title>{title}</title><style>body{{margin:0}}</style>"
            f"<script>var tracking = {{id: 42}};</script></head><body>{nav_html}"
            f'<div class="sidebar">Popular stories this week</div>'
            f"<article><h1>{title}</h1>\n{body}{extra_body}</article>"
            f"<footer>Copyright 2024 Example Media. All rights reserved.</footer></body></html>")


def http(body, status="200 OK", ctype="text/html; charset=utf-8"):
    b = body.encode() if isinstance(body, str) else body
    return (f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {len(b)}\r\n\r\n").encode() + b


def record(kind, url, payload, extra_headers=None):
    payload = payload.encode() if isinstance(payload, str) else payload
    rid = hashlib.sha1((kind + str(url) + str(len(payload))).encode() + payload).hexdigest()
    h = [f"WARC/1.0", f"WARC-Type: {kind}", "WARC-Date: 2024-02-20T21:40:38Z",
         f"WARC-Record-ID: <urn:uuid:{rid[:8]}-{rid[8:12]}-{rid[12:16]}-{rid[16:20]}-{rid[20:32]}>"]
    if url is not None:
        h.append(f"WARC-Target-URI: {url}")
    for k, v in (extra_headers or {}).items():
        h.append(f"{k}: {v}")
    h.append(f"Content-Length: {len(payload)}")
    return ("\r\n".join(h) + "\r\n\r\n").encode() + payload + b"\r\n\r\n"


def write_gz_members(path, records, truncate_last=False):
    """One gzip member per record, like Common Crawl."""
    members = [gzip.compress(r, compresslevel=9, mtime=0) for r in records]
    if truncate_last:
        members[-1] = members[-1][: len(members[-1]) // 2]
    with open(path, "wb") as f:
        for m in members:
            f.write(m)


def text_doc(paras, title=None):
    return "\n".join(([title] if title else []) + paras) + "\n"


def main():
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(os.path.join(OUT, "shards"), exist_ok=True)

    good = {i: article(1000 + i) for i in range(8)}
    near = [p for p in good[2]]
    near[1] = near[1].replace(" the ", " a ", 1)  # one-word edit → near duplicate

    german = ["Der kleine Hafen im Norden der Stadt wurde im letzten Sommer von einer Gruppe "
              "von Fischern restauriert. Viele Menschen glauben, dass der alte Leuchtturm für alle "
              "offen bleiben sollte. Besucher bleiben oft stundenlang und sprechen mit den Seeleuten. "
              "Ein kleines Schild am Eingang erklärt, wie das Gebäude über viele Jahre gebaut wurde. "
              "Laut Maria ist die beste Zeit für einen Besuch früh am Morgen, wenn es noch ruhig ist."]
    spam = ["Buy cheap tickets now and save big on your next trip today."] * 12
    code = [good[5][0], "To try it yourself, run function setup() { return init(42); } in the console.",
            good[5][1], good[5][2]]
    pii = good[6][:2] + ["You can reach the editor at jane.doe@example.com or call (555) 123-4567 "
                         "during office hours. Our old server at 203.0.113.42 has been retired, and "
                         "the new contact address is news-desk@example.org for all questions."]

    warc = [
        record("warcinfo", None, "software: sieve make_sample.py\r\nformat: WARC File Format 1.0\r\n"),
        record("request", "http://example.com/a/0", "GET /a/0 HTTP/1.1\r\nHost: example.com\r\n\r\n"),
    ]
    for i in [0, 1, 2, 3, 4]:
        warc.append(record("response", f"http://example.com/a/{i}", http(html_page(f"Story {i}", good[i]))))
    # exact duplicate of story 0: different URL *and* different boilerplate; extraction
    # removes the boilerplate, so the texts become identical
    warc.append(record("response", "http://mirror.example.net/copy-of-0",
                       http(html_page("Story 0", good[0], nav=False))))
    warc.append(record("response", "http://example.com/near-2", http(html_page("Story 2", near))))
    warc.append(record("response", "http://example.com/pii", http(html_page("Contact", pii))))
    warc.append(record("response", "http://example.de/artikel", http(html_page("Hafen", german))))
    warc.append(record("response", "http://example.com/short", http(html_page("Short", ["Too short to keep."]))))
    warc.append(record("response", "http://example.com/spam", http(html_page("Deals", spam))))
    warc.append(record("response", "http://example.com/code", http(html_page("Code", code))))
    warc.append(record("response", "http://example.com/missing", http(html_page("Gone", good[7]), status="404 Not Found")))
    warc.append(record("response", "http://example.com/logo.png", http(b"\x89PNG\r\n\x1a\nfake", ctype="image/png")))
    warc.append(record("response", "http://example.com/broken-http", "this is not an http response at all"))
    warc.append(record("response", None, http(html_page("No URI", good[7]))))  # missing Target-URI
    write_gz_members(os.path.join(OUT, "sample.warc.gz"), warc)

    wet = [record("warcinfo", None, "software: sieve make_sample.py\r\n")]
    wet.append(record("conversion", "http://example.com/wet/3-copy", text_doc(good[3], "Story 3"),
                      {"WARC-Identified-Content-Language": "eng"}))  # cross-shard exact dup of story 3
    wet.append(record("conversion", "http://example.com/wet/7", text_doc(good[7], "Story 7"),
                      {"WARC-Identified-Content-Language": "eng"}))
    write_gz_members(os.path.join(OUT, "sample.wet.gz"), wet)

    trunc = [record("conversion", f"http://example.com/t/{i}", text_doc(article(3000 + i)))
             for i in range(3)]
    write_gz_members(os.path.join(OUT, "truncated.warc.gz"), trunc, truncate_last=True)

    # Shards for the crash/resume test: shard k repeats two docs from shard k-1,
    # so correct output after a crash requires the dedup state to be restored.
    prev = []
    for k in range(4):
        docs = [(f"http://shard{k}.example.com/{j}", article(5000 + 10 * k + j)) for j in range(6)]
        recs = [record("conversion", u, text_doc(p)) for u, p in docs]
        recs += [record("conversion", u.replace(f"shard{k-1}", f"dup{k}"), text_doc(p)) for u, p in prev[:2]]
        write_gz_members(os.path.join(OUT, "shards", f"shard-{k:02d}.warc.wet.gz"), recs)
        prev = docs
    print("wrote fixtures to", os.path.normpath(OUT))


if __name__ == "__main__":
    main()
