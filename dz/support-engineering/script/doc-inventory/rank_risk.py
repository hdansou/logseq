#!/usr/bin/env python3
"""Rank documents by how much verifiable-and-rottable claim they carry.

Prose does not rot; specifics do. Score counts source paths, namespaced idents,
config keys, shortcuts, version numbers and CLI invocations, then weights by age.

    rank_risk.py <ddocs-root> [--status <reviewStatus to include>] [--top N]
"""
import os, re, sys, argparse

PATTERNS = {
    "src path":   r"\b(?:src|deps|libs|packages|resources|cli)/[A-Za-z0-9_./-]+\.(?:clj[sc]?|ml|ts|js|css|edn)",
    "ident":      r":[a-z][a-z0-9.-]*/[a-z][a-zA-Z0-9?*!<>=-]*",
    "shortcut":   r"\b(?:mod|cmd|ctrl|alt|shift)\+[a-z0-9+]+",
    "version":    r"\bv?\d+\.\d+(?:\.\d+)?\b",
    "cli":        r"`logseq [a-z][a-z0-9 -]*`",
    "fn":         r"`[a-z][a-zA-Z0-9-]*[!?*]?`",
}

def score(path):
    t = open(path, encoding="utf-8").read()
    t = re.sub(r"^---.*?^---", "", t, count=1, flags=re.S | re.M)   # drop frontmatter
    counts = {k: len(set(re.findall(p, t))) for k, p in PATTERNS.items()}
    return sum(counts.values()), counts, t.count("\n") + 1

def fm(path, key):
    t = open(path, encoding="utf-8").read()
    t = re.sub(r"\A(?:<!--.*?-->\s*)+", "", t, flags=re.S)   # generated files lead with HTML comments
    if not t.startswith("---"): return None
    fmt = t[3:t.find("\n---", 3)]
    m = re.search(rf"^{key}:\s*['\"]?(.+?)['\"]?\s*$", fmt, re.M)
    return m.group(1) if m else None

ap = argparse.ArgumentParser()
ap.add_argument("root"); ap.add_argument("--status"); ap.add_argument("--top", type=int, default=25)
a = ap.parse_args()

rows = []
for dp, _, ns in os.walk(a.root):
    for n in sorted(ns):
        if not n.endswith(".md") or n == "README.md": continue
        p = os.path.join(dp, n); rel = os.path.relpath(p, a.root)
        st = fm(p, "reviewStatus") or fm(p, "review-status")
        if a.status == "unreviewed":
            if st in ("authored-from-source", "merged-not-fully-audited", "generated-gated"): continue
        elif a.status and st != a.status:
            continue
        s, c, lines = score(p)
        rows.append((s, rel, lines, fm(p, "lastVerified") or fm(p, "last-verified"), c))
rows.sort(reverse=True)
print(f"{'score':>5}  {'lines':>5}  {'verified':<11} document")
for s, rel, lines, d, c in rows[: a.top]:
    print(f"{s:>5}  {lines:>5}  {d or '—':<11} {rel}")
print(f"\n{len(rows)} documents ranked; total claim tokens {sum(r[0] for r in rows)}")
