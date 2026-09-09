#!/usr/bin/env python3
"""Compare two documentation trees file-by-file to plan a merge.

For every path present in either tree, report which side exists, whether the
content differs, and which side carries the stronger freshness signal
(lastVerified date, then content size).

    compare_trees.py <tree-a> <tree-b> [--out report.md]
"""
import sys, os, re, hashlib, argparse

FM_DATE = re.compile(r"^lastVerified:\s*'?([0-9]{4}-[0-9]{2}-[0-9]{2})'?", re.M)
FM_REV  = re.compile(r"^verifiedAgainst:\s*'?([0-9a-f]{7,40})'?", re.M)
BODY_REV = re.compile(r"origin/master`?\s*@\s*`?([0-9a-f]{7,40})")

def md_files(root):
    out = set()
    for dirpath, _, names in os.walk(root):
        for n in names:
            if n.endswith(".md"):
                out.add(os.path.relpath(os.path.join(dirpath, n), root))
    return out

def probe(path):
    try:
        s = open(path, encoding="utf-8").read()
    except OSError:
        return None
    m = FM_DATE.search(s)
    r = FM_REV.search(s) or BODY_REV.search(s)
    return {
        "date": m.group(1) if m else None,
        "rev": r.group(1)[:10] if r else None,
        "bytes": len(s.encode()),
        "lines": s.count("\n") + 1,
        "sha": hashlib.sha256(s.encode()).hexdigest()[:12],
        "links": len(re.findall(r"\]\((?!https?:)[^)]+\.md", s)),
    }

def winner(a, b):
    """Which side looks fresher. Returns 'A', 'B', or 'tie'."""
    if a and b:
        if a["date"] != b["date"]:
            if a["date"] and b["date"]:
                return "A" if a["date"] > b["date"] else "B"
            return "A" if a["date"] else "B"
        if abs(a["bytes"] - b["bytes"]) > 200:
            return "A" if a["bytes"] > b["bytes"] else "B"
    return "tie"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tree_a"); ap.add_argument("tree_b")
    ap.add_argument("--out")
    args = ap.parse_args()
    A, B = args.tree_a.rstrip("/"), args.tree_b.rstrip("/")
    fa, fb = md_files(A), md_files(B)

    only_a, only_b, both = sorted(fa - fb), sorted(fb - fa), sorted(fa & fb)
    rows, ident, diff = [], [], []
    for rel in both:
        pa, pb = probe(os.path.join(A, rel)), probe(os.path.join(B, rel))
        if pa["sha"] == pb["sha"]:
            ident.append(rel); continue
        w = winner(pa, pb)
        diff.append(rel)
        rows.append((rel, pa, pb, w))

    L = []
    L.append(f"# Tree comparison\n")
    L.append(f"- **A** = `{A}` — {len(fa)} files")
    L.append(f"- **B** = `{B}` — {len(fb)} files\n")
    L.append("| Bucket | Count |")
    L.append("|---|---|")
    L.append(f"| Only in A | {len(only_a)} |")
    L.append(f"| Only in B | {len(only_b)} |")
    L.append(f"| In both, identical | {len(ident)} |")
    L.append(f"| In both, **differ** | {len(diff)} |")
    byw = {}
    for _, _, _, w in rows: byw[w] = byw.get(w, 0) + 1
    L.append(f"\nOf the {len(diff)} differing: "
             + ", ".join(f"**{k}** fresher in {v}" for k, v in sorted(byw.items())))

    L.append("\n## Differing files\n")
    L.append("| File | A date | A lines | B date | B lines | Fresher |")
    L.append("|---|---|---|---|---|---|")
    for rel, pa, pb, w in sorted(rows, key=lambda r: (r[3], r[0])):
        L.append(f"| `{rel}` | {pa['date'] or '—'} | {pa['lines']} | "
                 f"{pb['date'] or '—'} | {pb['lines']} | {w} |")

    for name, lst, tree in (("Only in A", only_a, A), ("Only in B", only_b, B)):
        L.append(f"\n## {name} ({len(lst)})\n")
        for rel in lst:
            p = probe(os.path.join(tree, rel))
            L.append(f"- `{rel}` — {p['date'] or 'no date'}, {p['lines']} lines, {p['links']} md links")

    text = "\n".join(L) + "\n"
    if args.out:
        open(args.out, "w").write(text)
        print(f"wrote {args.out} ({len(text.splitlines())} lines)")
    print("\n".join(L[:20]))

main()
