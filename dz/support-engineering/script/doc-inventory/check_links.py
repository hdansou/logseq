#!/usr/bin/env python3
"""Resolve every relative markdown link in a tree, ignoring code blocks.

    check_links.py <root> [--against <old-root>]

With --against, each broken link is also tested from the corresponding path in
<old-root>, so breakage introduced by a move is separated from pre-existing rot.

_templates/ is skipped: its links are deliberate placeholders, not rot.
"""
import sys, os, re, argparse

FENCE = re.compile(r"^\s*(```|~~~)")
INLINE = re.compile(r"`[^`\n]*`")
LINK = re.compile(r"\]\((?!https?:|#|mailto:|data:|assets:|logseq:|lsp:|file:)([^)\s]+?)(?:\s+\"[^\"]*\")?\)")

def links(path):
    """Yield (lineno, target) for links outside fenced/inline code."""
    out, fenced = [], False
    for i, line in enumerate(open(path, encoding="utf-8"), 1):
        if FENCE.match(line):
            fenced = not fenced; continue
        if fenced:
            continue
        for m in LINK.finditer(INLINE.sub("``", line)):
            t = m.group(1).split("#")[0]
            if t and not t.startswith("<"):
                out.append((i, t))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root"); ap.add_argument("--against")
    a = ap.parse_args()
    ok = introduced = preexisting = 0
    for dp, ds, ns in os.walk(a.root):
        ds[:] = [d for d in ds if d != "_templates"]
        for n in sorted(ns):
            if not n.endswith(".md"): continue
            p = os.path.join(dp, n)
            rel = os.path.relpath(p, a.root)
            for ln, t in links(p):
                if os.path.exists(os.path.normpath(os.path.join(dp, t))):
                    ok += 1; continue
                was_ok = False
                if a.against:
                    old = os.path.join(a.against, rel)
                    if os.path.exists(old):
                        was_ok = os.path.exists(
                            os.path.normpath(os.path.join(os.path.dirname(old), t)))
                if was_ok:
                    introduced += 1
                    print(f"INTRODUCED {rel}:{ln} -> {t}")
                else:
                    preexisting += 1
                    print(f"pre-existing {rel}:{ln} -> {t}")
    print(f"\nresolved {ok} | introduced-by-move {introduced} | pre-existing {preexisting}")
    return 1 if introduced else 0

sys.exit(main())
