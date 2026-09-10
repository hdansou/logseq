# Broken link triage

Snapshot taken 2026-09-09, after waves 1–3 of the review pass. Hand-written; the raw data
behind it comes from

```
dz/support-engineering/script/doc-inventory/check_links.py dz/docs/ddocs
```

The headline number started at **257 broken links** and was widely assumed to be 257 problems.
It is not. Most of it was two mechanical bugs and one category that should never have been
counted. What is left needs judgment, which is why it is written down rather than fixed.

## Where the count went

| | Then | Now | What happened |
|---|---:|---:|---|
| Source links at the wrong depth | 88 | **0** | `../../` from a page five levels deep resolves inside `ddocs/`, not at the repo root. Stage 1 of the merge re-depthed 19 and missed the rest; 47 were fixed mechanically and the audits fixed the other 41 as they went. |
| Doc links whose target moved | 30 | **6** | Rewritten only where exactly one file in the tree carries that basename. The remaining 6 sit in files a concurrent review was editing. |
| `_templates/` placeholders | 27 | 27 | **Not defects.** `relevant-guide.md`, `relevant-tutorial.md` and friends are deliberate placeholders in the Diátaxis templates. The checker should skip `_templates/` the way `rank_risk.py` already does. |
| Missing assets | 8 | 6 | Screenshots and a `LICENSE` that were never committed. |
| Links to documents that were never written | 103 | **87** | The real backlog. See below. |
| **Total** | **257** | **126** | |

## The 87, which are 49 distinct documents

These are links to pages that do not exist anywhere in the tree. They fall into two groups,
and the split matters because only one of them is a writing task.

### Group 1 — a rename the merge did not catch (~31 of 49)

The page exists under a different name. These are link fixes, not new documents, but a
basename match cannot find them and **fuzzy matching is not safe here**: string similarity
proposed `search-strategies.md` → `backup-strategies.md`, `cli-reference.md` →
`linked-references.md` and `export-graph.md` → `02-first-graph.md`, all of which are wrong.
Each needs a human to confirm the target actually covers the subject.

The unambiguous ones, offered as proposals rather than applied:

| Missing target | Refs | Almost certainly means |
|---|---:|---|
| `db-properties.md` | 6 | `reference/data/properties-reference.md` |
| `block-editing.md` | 4 | `explanation/concepts/block-editing-philosophy.md` |
| `query-dsl.md` | 4 | `reference/advanced/query-system.md` |
| `markdown.md` | 3 | `reference/formatting/markdown-syntax.md` |
| `templates.md` | 3 | `reference/interface/templates-system.md` |
| `basic-editing.md` | 2 | `tutorials/getting-started/03-basic-editing.md` |
| `graph-view.md`, `graph-navigation.md` | 4 | `explanation/concepts/graph-visualization.md` |
| `plugin-development.md` | 2 | `reference/api/plugin-api.md` |
| `daily-journaling.md` | 1 | `how-to/workflows/daily-workflows.md` |
| `performance-guide.md` | 1 | `how-to/optimization/performance-optimization.md` |

### Group 2 — genuinely unwritten (~18 of 49)

No counterpart exists. Each is a decision: write the page, or drop the link.

`built-in-classes.md` (3), `page-links.md` (2), `rtc-setup.md` (2), `rtc-onboarding.md` (2),
`sharing-graphs.md` (2), `theme-development.md` (2),
`pdf-feature-platform-compatibility-investigation.md` (2), `math-latex.md`,
`property-history.md`, `folder-structure.md`, `knowledge-graph.md`,
`building-knowledge-graph.md`, `getting-started.md`, `mobile-app.md`,
`develop-logseq-on-mobile.md`, `file-system-errors.md`, `CHANGELOG.md`, `CONTRIBUTING.md`.

`built-in-classes.md` is the most-referenced of these and the easiest to write — the 18
built-in classes are enumerated in `deps/db/src/logseq/db/frontend/class.cljs:18-105` at
`be800f1711`.

## Recommended next steps

1. Teach `check_links.py` to skip `_templates/`, removing 27 false positives from every future
   run. This is a tooling fix, not a docs fix.
2. Work Group 1 as link edits, confirming each target by reading it.
3. Take Group 2 to a decision — most of these links were written aspirationally by pages that
   were themselves never verified, so dropping the link is often the honest answer.
