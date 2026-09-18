---
title: 'Find Empty and Unused Nodes'
description: 'How to query a DB graph for empty pages, empty blocks, unused tags and unused properties, and how to tell junk apart from link stubs'
status: draft
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: authored-from-source
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  howTo:
    - '[Advanced Queries](./advanced-queries.md)'
    - '[View Functions](../content/view-types.md)'
    - '[CLI Basics](../integrations/cli-basic.md)'
  reference:
    - '[Query System](../../reference/advanced/query-system.md)'
    - '[Schema Overview](../../reference/database/schema-overview.md)'
---

# Find Empty and Unused Nodes

> **Goal**: list the pages, blocks, tags and properties in a DB graph that carry nothing, without also listing the trash, the built-ins, or the link stubs you still want.

## Overview

Graph cleanup asks five different questions, and each needs its own query:

| You want | Query |
| --- | --- |
| Pages with nothing under them | [Empty pages](#1-list-empty-pages) |
| Pages nothing links to either | [Empty pages that are safe to delete](#2-narrow-to-pages-that-are-safe-to-delete) |
| Blocks with no text | [Empty blocks](#3-list-empty-blocks) |
| Tags nothing is tagged with | [Unused tags](#4-list-unused-tags) |
| Properties never set on anything | [Unused properties](#5-list-unused-properties) |

The traps are all in the exclusions. A naive "page with no blocks" query misses half the
empty pages and silently includes pages you already deleted; a naive "unused tag" query
counts a tag as used when nothing but the tag hierarchy points at it.

## Before You Start

- You are using a DB graph. None of these queries apply to a file graph.
- You can create an **Advanced Query** block, or run `logseq query` against the graph.
- If you plan to act on the results, read [Two shapes of empty](#two-shapes-of-empty)
  first — the difference decides whether your query is correct.

## Two shapes of empty

A page that looks empty on screen is stored in one of two ways:

| How the page was made | What is stored |
| --- | --- |
| From a `[[link]]`, or with `logseq upsert page` | **Zero blocks.** Nothing has `:block/page` pointing at it. |
| By opening a new page in the UI and typing nothing | **One blank block** whose `:block/title` is `""`. |

So `(not [?b :block/page ?p])` — the clause everyone writes first — only finds the first
shape. The second shape has a block, so the page looks populated to that clause and never
appears in the results.

The fix is to negate the *conjunction*: the page has no block whose title is non-empty.

```clojure
(not [?b :block/page ?p]
     [?b :block/title ?t]
     [(!= ?t "")])
```

## Steps

### 1. List empty pages

```clojure
{:query
 [:find (pull ?p [:db/id :block/uuid :block/title])
  :where
  [?p :block/tags :logseq.class/Page]
  (not [?p :logseq.property/built-in? true])
  (not [?p :logseq.property/deleted-at _])
  (not [?b :block/page ?p]
       [?b :block/title ?t]
       [(!= ?t "")])]}
```

Every clause is load-bearing:

| Clause | Why it is there |
| --- | --- |
| `[?p :block/tags :logseq.class/Page]` | Restricts to pages. `:logseq.class/Page` is the built-in page class ([`deps/db/src/logseq/db/frontend/class.cljs:27`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)). |
| `(not [?p :logseq.property/built-in? true])` | Drops the pages Logseq ships with ([`deps/db/src/logseq/db/frontend/property.cljs:218`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)). |
| `(not [?p :logseq.property/deleted-at _])` | Drops the trash. A recycled page **keeps its `Page` tag** and only gains `:logseq.property/deleted-at` ([`deps/db/src/logseq/db/frontend/property.cljs:657-660`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)), so without this clause your "empty pages" list is partly pages you already deleted. |
| the three-clause `not` | Catches both shapes of empty, per [Two shapes of empty](#two-shapes-of-empty). |

**Journals are already excluded.** A journal page carries `:block/tags :logseq.class/Journal`,
not `:logseq.class/Page` — `Journal` *extends* `Page`
([`deps/db/src/logseq/db/frontend/class.cljs:29-32`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)),
and extension is not written into `:block/tags`. Since most journals are empty most of the
time, that is usually what you want. To include them, put an `or` in place of
the first clause:

```clojure
  (or [?p :block/tags :logseq.class/Page]
      [?p :block/tags :logseq.class/Journal])
```

On the test graph that took the result from 8 rows to 27 — the 19 extra rows are all empty
journal days.

### 2. Narrow to pages that are safe to delete

An empty page that something links to is a **link stub**, not junk — deleting it breaks the
link that created it. `:block/refs` holds those incoming links, so one clause drops them:

```clojure
  (not [?r :block/refs ?p])
```

Giving:

```clojure
{:query
 [:find (pull ?p [:db/id :block/uuid :block/title])
  :where
  [?p :block/tags :logseq.class/Page]
  (not [?p :logseq.property/built-in? true])
  (not [?p :logseq.property/deleted-at _])
  (not [?r :block/refs ?p])
  (not [?b :block/page ?p]
       [?b :block/title ?t]
       [(!= ?t "")])]}
```

On the test graph this cut 8 empty pages down to 2. The proportion is typical: most empty
pages exist *because* something links to them.

Treat the result as a shortlist rather than a verdict — `:block/refs` covers inline links but
is not guaranteed for property values, as [step 4](#4-list-unused-tags) documents.

### 3. List empty blocks

```clojure
{:query
 [:find (pull ?b [:db/id :block/uuid :block/title {:block/page [:block/title]}])
  :where
  [?b :block/page ?pg]
  [?b :block/title ""]]}
```

Expect a long list, and do not treat it as a cleanup queue. A trailing blank block is the
normal resting state of a page you just finished editing, and of nearly every journal day
you opened and left. This query is useful for auditing a specific page or for counting, not
for bulk deletion.

To scope it to one page, bind the page title:

```clojure
  [?b :block/page ?pg]
  [?pg :block/title "My Page"]
  [?b :block/title ""]
```

### 4. List unused tags

A tag is an entity like any other, tagged `:logseq.class/Tag`. "Unused" in the plain sense
means nothing carries it:

```clojure
{:query
 [:find (pull ?t [:db/id :block/uuid :block/title])
  :where
  [?t :block/tags :logseq.class/Tag]
  (not [?t :logseq.property/built-in? true])
  (not [?t :logseq.property/deleted-at _])
  (not [?b :block/tags ?t])]}
```

The built-in exclusion matters more here than anywhere else. Built-in tags outnumber user
tags in a young graph — the test graph has 18 of them (`Task`, `Page`, `Journal`, `Query`,
`Card`, `Asset`, `Quote`, `Code`, `Math`, `PDF Annotation`, …) against 7 user tags — and most
are unused in most graphs. Without that clause the result is mostly noise you cannot act on.

#### A tag can be unused and still load-bearing

Before deleting anything on this list, check whether the tag:

- **is a parent of other tags.** Every tag carries
  `:logseq.property.class/extends` — user tags point at `Root Tag` by default
  ([`deps/db/src/logseq/db/frontend/class.cljs:29-32`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)).
  A tag that nothing carries may still sit above tags that are used.
- **holds tag properties.** `:logseq.property.class/properties` is the tag's schema; deleting
  the tag takes that schema with it.
- **is a value of a node-typed property**, which does not create `:block/tags`.

To catch all three at once, replace the last clause with an incoming-reference check. An
unbound attribute in the middle position asks "does *anything at all* point at this entity?",
minus the hierarchy link every tag has and the tag page's own content:

```clojure
  (not [?e ?a ?t]
       [(!= ?a :logseq.property.class/extends)]
       [(!= ?a :block/page)]
       [(!= ?a :block/parent)])
```

> ⚠️ An unbound attribute defeats the AEV index, so this form scans. It returned in ~0.12s on
> a small test graph (about a thousand entities); it has not been measured on a large graph.
> Prefer the simple form for routine use.

> ⚠️ **Do not use `:block/refs` as the completeness check.** It is populated for inline links —
> a block created with the text `ref probe [[Book]]` gets `Book` in its `:block/refs` — but it
> is not guaranteed for property values. In the test graph, block 744 has a node-typed property
> whose value is the page `Example Page` (id 440), and that block has no `:block/refs` at all.

### 5. List unused properties

A property is both an entity and a database attribute, and that is what makes this query
work. `:db/ident` holds the attribute keyword — `:user.property/Rating-IxMP7mlf` for a
property titled `Rating`. Bind it, then use the bound variable in attribute position:

```clojure
{:query
 [:find (pull ?p [:db/id :block/uuid :block/title])
  :where
  [?p :block/tags :logseq.class/Property]
  (not [?p :logseq.property/built-in? true])
  (not [?p :logseq.property/deleted-at _])
  [?p :db/ident ?ident]
  (not [?e ?ident _])]}
```

`(not [?e ?ident _])` reads as "no entity in the graph has this attribute" — that is, the
property was defined but never set on anything. It catches values of every type, including
ref-typed values, because the test is the presence of the attribute rather than the shape of
the value.

#### Defined, declared, and used are three different states

A property can be attached to a tag's schema and still never have been filled in. Add one
clause to separate the two:

```clojure
  (not [?t :logseq.property.class/properties ?p])
```

- **With** that clause: properties that are neither used nor declared on any tag — orphans,
  the safest to delete.
- **Without** it: also includes properties that a tag promises but no node has yet filled in.
  Those are usually a schema you set up and have not used, not junk.

> ⚠️ This query also scans, for the same reason as the strict tag query above.

## Running the query

### In the app

Create the block with the **Advanced Query** slash command and paste the map form shown
above — the `#+BEGIN_QUERY` wrapper is file-graph syntax and does not work here (see
[Advanced Queries](./advanced-queries.md)).

Two constraints apply to anything you paste into a query block:

> ⚠️ **Every top-level pulled map must include `:block/uuid`.** The renderer normalizes each
> result cell to its uuid and hard-fails otherwise — `"Renderer query result map has no UUID"`,
> a 500 from the worker, and a dead query block
> ([`src/main/frontend/worker/handler/render_resource/query.cljs:15-23`](../../../../../src/main/frontend/worker/handler/render_resource/query.cljs)).
> Pull `[*]` or keep `:block/uuid` in the selector. Nested maps inside a map that already has
> a uuid need none.

> ⚠️ **Do not add `:limit`.** It is not a recognized key and produces a bare `Query Error`
> with no further detail. Bound the result with a more restrictive `:where`, or post-process
> with `(take N rows)` in a `:view`.

### From the CLI

```bash
logseq query --graph my-graph --query '[:find (pull ?p [:db/id :block/uuid :block/title])
 :where
 [?p :block/tags :logseq.class/Page]
 (not [?p :logseq.property/built-in? true])
 (not [?p :logseq.property/deleted-at _])
 (not [?b :block/page ?p]
      [?b :block/title ?t]
      [(!= ?t "")])]'
```

Note the shape difference: on the CLI you pass the bare `[:find ...]` vector, while an
in-app advanced query is the `{:query [:find ...]}` map.

> ⚠️ The CLI returns raw `d/q` output and never runs the renderer's normalizer, so a query
> that works on the CLI proves nothing about whether it will render in the app. The
> `:block/uuid` rule above is the usual way this bites.

## Verification

Run on the `cliworker` graph on 2026-09-18, CLI revision `6bf8fe7-dirty`, against source at
`5824434a74`:

- **Blank-block shape.** A probe page holding a single `:block/title ""` block was created
  with `logseq upsert page` + `logseq upsert block --blocks '[{:block/title ""}]'`. The naive
  `(not [?b :block/page ?p])` query returned 7 rows and did not include it; the three-clause
  version returned 8 and did.
- **Trash exclusion.** `ZZ Empty Probe` is an empty but recycled page in that graph
  (`:logseq.property/deleted-at 1789514179875`). It is absent from the results, and
  `logseq show --page "ZZ Empty Probe"` answers `Error (recycled-page): page is recycled`.
- **Link stubs.** Adding `(not [?r :block/refs ?p])` reduced the 8 empty pages to 2 — one
  real orphan and the probe page created for the first check.
- **Unused tags.** Of the graph's 25 tags, 18 are built-in; of the 7 user tags, 6 are in use.
  A probe tag created with `logseq upsert tag --name "ZZ Unused Tag Probe"` was the only row
  returned, by both the simple and the strict form.
- **Unused properties.** A probe property created with
  `logseq upsert property --name zz-unused-prop-probe --type default` was the only row
  returned; the graph's 15 user properties include that probe, and the other 14 are each set
  on at least one node. The unbound-attribute pattern `(not [?e ?ident _])` was confirmed to
  run in Datascript rather than throwing.
- **`:block/refs` gap.** A block created with the text `ref probe [[Book]]` gained `Book` in
  `:block/refs`, while block 744, which has a node-typed property pointing at page 440, has no
  `:block/refs` — the basis for the warning in step 4.
- **Timing.** Both scanning queries returned in ~0.12s on that graph (about a thousand
  entities). No large graph was measured.
- **Journal variant.** Swapping the first clause for the `or` form took the empty-page result
  from 8 rows to 27.

The queries were exercised on the CLI only; the in-app constraints in
[Running the query](#running-the-query) are cited from source rather than re-run in the app
for this page.

## Related Tasks

- [Advanced Queries](./advanced-queries.md) — query structure, `:view`, `:inputs`, `:rules`
- [View Functions](../content/view-types.md) — formatting result rows
- [CLI Basics](../integrations/cli-basic.md) — running queries outside the app

## See Also

- **Full reference**: [Query System](../../reference/advanced/query-system.md)
- **Schema**: [Schema Overview](../../reference/database/schema-overview.md)

---

_Last updated: 2026-09-18 | [Report an issue](https://github.com/logseq/logseq/issues)_
