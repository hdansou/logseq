#!/usr/bin/env python3
"""Regenerate dz/docs/TASK_LIST.md as a verification-status inventory.

Reports, per document, when it was last checked and against which revision --
derived from frontmatter, not hand-maintained, so the counts cannot drift.

    gen_task_list.py <ddocs-root> <out.md>
"""
import os, re, sys, datetime

def fm(path):
    t = open(path, encoding="utf-8").read()
    g = lambda k: (re.search(rf"^{k}:\s*['\"]?(.+?)['\"]?\s*$", t[:t.find('\n---',3)] if t.startswith('---') else '', re.M) or [None,None])[1]
    return g("lastVerified") or g("last-verified"), g("verifiedAgainst") or g("verified-against"), g("verifiedScope")

root, out = sys.argv[1], sys.argv[2]
rows = []
for dp, _, ns in os.walk(root):
    for n in sorted(ns):
        if not n.endswith(".md") or n == "README.md": continue
        p = os.path.join(dp, n)
        rel = os.path.relpath(p, root)
        d, rev, scope = fm(p)
        rows.append((rel, d, rev, scope))
rows.sort(key=lambda r: (r[1] or "0000-00-00", r[0]))

cur = [r for r in rows if r[1] and r[1] >= "2026-09-01"]
old = [r for r in rows if not (r[1] and r[1] >= "2026-09-01")]
L = ["# Documentation status", "",
     f"Generated {datetime.date.today()} by",
     "`dz/support-engineering/script/doc-inventory/gen_task_list.py`. Do not edit by hand —",
     "every field comes from each document's frontmatter.", "",
     f"| | Count |", "|---|---|",
     f"| Documents | {len(rows)} |",
     f"| Verified in the current pass (>= 2026-09-01) | {len(cur)} |",
     f"| Not verified in the current pass | {len(old)} |", ""]
L += ["## Not verified in the current pass", "",
      "These carry an older date, or none. Highest priority for the review pass.", "",
      "| Document | Last verified | Against |", "|---|---|---|"]
for rel, d, rev, _ in old:
    L.append(f"| `{rel}` | {d or '—'} | {rev or '—'} |")
L += ["", "## Verified in the current pass", "",
      "| Document | Date | Against | Scope |", "|---|---|---|---|"]
for rel, d, rev, scope in cur:
    L.append(f"| `{rel}` | {d} | `{rev or '—'}` | {scope or 'full'} |")
open(out, "w").write("\n".join(L) + "\n")
print(f"{len(rows)} documents: {len(cur)} current, {len(old)} stale -> {out}")
