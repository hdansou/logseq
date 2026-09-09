#!/usr/bin/env python3
"""Regenerate the ddocs README file listings from the tree.

The prose in README.md -- quadrant descriptions, status legend, role navigation --
is hand-written and preserved. Only the file lists are generated, between
<!-- INDEX:START --> and <!-- INDEX:END --> markers, so the index cannot drift
from the tree the way a hand-maintained one does.

    gen_index.py <ddocs-root> [--check]

--check exits 1 if the committed README differs from what would be generated.
"""
import os, re, sys, argparse

QUADRANTS = [
    ("tutorials",   "Tutorials",     "_Learning-Oriented_"),
    ("how-to",      "How-To Guides", "_Task-Oriented_"),
    ("reference",   "Reference",     "_Information-Oriented_"),
    ("explanation", "Explanation",   "_Understanding-Oriented_"),
]

def frontmatter(path):
    """Return (title, status) from YAML frontmatter, falling back to the H1."""
    title = status = None
    try:
        text = open(path, encoding="utf-8").read()
    except OSError:
        return None, None
    if text.startswith("---"):
        end = text.find("\n---", 3)
        fm = text[3:end] if end > 0 else ""
        m = re.search(r"^title:\s*['\"]?(.+?)['\"]?\s*$", fm, re.M)
        if m: title = m.group(1)
        m = re.search(r"^status:\s*['\"]?(.+?)['\"]?\s*$", fm, re.M)
        if m: status = m.group(1).strip()
    if not title:
        m = re.search(r"^#\s+(.+)$", text, re.M)
        title = m.group(1).strip() if m else os.path.basename(path)[:-3]
    emoji = ""
    if status:
        for ch in status:
            if ord(ch) > 0x2000: emoji = ch; break
    return title, emoji

def humanize(seg):
    return " ".join(w.capitalize() if w.islower() else w for w in seg.split("-"))

def curated_names(readme):
    """Reuse subsection names a human already chose, e.g. content -> Content Creation."""
    out = {}
    for m in re.finditer(r"^#### \[([^\]]+)\]\(([^)]+)/\)", readme, re.M):
        out[m.group(2).rstrip("/")] = m.group(1)
    return out

def build(root, names):
    lines = []
    for qdir, qtitle, qtag in QUADRANTS:
        qpath = os.path.join(root, qdir)
        if not os.path.isdir(qpath): continue
        lines.append(f"### [{qtitle}]({qdir}/) — {qtag}\n")
        # Group every .md under the quadrant by its containing directory, at any depth.
        groups = {}
        for dp, _, ns in os.walk(qpath):
            mds = sorted(f for f in ns if f.endswith(".md"))
            if mds:
                groups[os.path.relpath(dp, root).replace(os.sep, "/")] = mds
        for f in groups.pop(qdir, []):
            t, e = frontmatter(os.path.join(qpath, f))
            lines.append(f"- {e + ' ' if e else ''}[{t}]({qdir}/{f})")
        if qdir in groups or lines[-1].startswith("- "): lines.append("")
        for rel in sorted(groups):
            depth = rel.count("/")
            heading = "#" * min(4 + depth - 1, 6)
            label = names.get(rel) or " / ".join(humanize(s) for s in rel.split("/")[1:])
            lines.append(f"{heading} [{label}]({rel}/)\n")
            for f in groups[rel]:
                t, e = frontmatter(os.path.join(root, rel, f))
                lines.append(f"- {e + ' ' if e else ''}[{t}]({rel}/{f})")
            lines.append("")
        lines.append("---\n")
    tdir = os.path.join(root, "_templates")
    if os.path.isdir(tdir):
        lines.append("### [Templates](_templates/)\n")
        for f in sorted(x for x in os.listdir(tdir) if x.endswith(".md")):
            t, e = frontmatter(os.path.join(tdir, f))
            lines.append(f"- [{t}](_templates/{f})")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root"); ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    rp = os.path.join(a.root, "README.md")
    readme = open(rp, encoding="utf-8").read()
    body = build(a.root, curated_names(readme))
    START, END = "<!-- INDEX:START -->", "<!-- INDEX:END -->"
    if START in readme and END in readme:
        new = re.sub(re.escape(START) + r".*?" + re.escape(END),
                     f"{START}\n\n{body}\n{END}", readme, flags=re.S)
    else:
        print("markers not found in README.md", file=sys.stderr); return 2
    if a.check:
        if new != readme:
            print("README.md index is stale. Run gen_index.py to refresh."); return 1
        print("README.md index is current."); return 0
    open(rp, "w").write(new)
    n = len(re.findall(r"^- ", body, re.M))
    print(f"regenerated {n} index entries")
    return 0

sys.exit(main())
