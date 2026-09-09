#!/usr/bin/env python3
"""Regenerate dz/docs/TASK_LIST.md as a verification-status inventory.

Reports, per document, when it was last checked and against which revision --
derived from frontmatter, not hand-maintained, so the counts cannot drift.

    gen_task_list.py <ddocs-root> <out.md>
"""
import os, re, sys, datetime

def fm(path):
    t = open(path, encoding="utf-8").read()
    t = re.sub(r"\A(?:<!--.*?-->\s*)+", "", t, flags=re.S)   # generated files lead with HTML comments
    g = lambda k: (re.search(rf"^{k}:\s*['\"]?(.+?)['\"]?\s*$", t[:t.find('\n---',3)] if t.startswith('---') else '', re.M) or [None,None])[1]
    return (g("lastVerified") or g("last-verified"),
            g("verifiedAgainst") or g("verified-against"),
            g("verifiedScope") or g("verified-scope"),
            g("reviewStatus") or g("review-status"))

root, out = sys.argv[1], sys.argv[2]
rows = []
for dp, _, ns in os.walk(root):
    for n in sorted(ns):
        if not n.endswith(".md") or n == "README.md": continue
        p = os.path.join(dp, n)
        rel = os.path.relpath(p, root)
        d, rev, scope, st = fm(p)
        rows.append((rel, d, rev, scope, st))
rows.sort(key=lambda r: (r[1] or "0000-00-00", r[0]))

authored = [r for r in rows if r[4] == "authored-from-source"]
merged   = [r for r in rows if r[4] == "merged-not-fully-audited"]
gen      = [r for r in rows if r[4] == "generated-gated"]
old      = [r for r in rows if r not in authored and r not in merged and r not in gen]
L = ["# Documentation status", "",
     f"Generated {datetime.date.today()} by",
     "`dz/support-engineering/script/doc-inventory/gen_task_list.py`. Do not edit by hand —",
     "every field comes from each document's frontmatter.", "",
     f"| | Count |", "|---|---|",
     f"| Documents | {len(rows)} |",
     f"| Authored from source — trusted | {len(authored)} |",
     f"| Merged this pass — base inherited, needs a line-by-line audit | {len(merged)} |",
     f"| Generated, gated in CI | {len(gen)} |",
     f"| Not reviewed | {len(old)} |", "",
     "`merged-not-fully-audited` means the two copies were reconciled against source and",
     "every ported claim was verified, but content inherited from the surviving base was",
     "not re-read line by line. Two wrong claims have already been found in that bucket,",
     "so it is a review target, not a finished state.", ""]
L += ["## Not reviewed", "",
      "Older date, or none. Highest priority.", "",
      "| Document | Last verified | Against |", "|---|---|---|"]
for rel, d, rev, _, _ in old:
    L.append(f"| `{rel}` | {d or '—'} | {rev or '—'} |")
L += ["", "## Merged this pass — needs a line-by-line audit", "",
      "| Document | Date | Against | Scope |", "|---|---|---|---|"]
for rel, d, rev, scope, _ in merged:
    L.append(f"| `{rel}` | {d} | `{rev or '—'}` | {scope or 'full'} |")
L += ["", "## Authored from source", "",
      "| Document | Date | Against |", "|---|---|---|"]
for rel, d, rev, _, _ in authored:
    L.append(f"| `{rel}` | {d} | `{rev or '—'}` |")
open(out, "w").write("\n".join(L) + "\n")
print(f"{len(rows)} docs: {len(authored)} authored, {len(merged)} merged-needs-audit, {len(old)} not reviewed -> {out}")
