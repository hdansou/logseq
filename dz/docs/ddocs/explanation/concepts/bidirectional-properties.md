---
title: 'Bidirectional properties'
description: 'Understanding how Logseq DB graphs derive reverse-reference sections from ref-valued properties'
status: stable
audience: [developer, user]
lastVerified: '2026-04-28'
logseqVersion: '0.11.x'
category: concepts
relatedDocs:
  reference:
    - '[Properties Reference](../../reference/data/properties-reference.md)'
    - '[Database Schema Overview](../../reference/database/schema-overview.md)'
  explanation:
    - '[Tag properties vs page properties](tag-properties-vs-page-properties.md)'
    - '[Linked References and Backlinks](linked-references.md)'
---

> Bidirectional properties are a DB-graph-only reverse-reference view. When a class opts in, Logseq shows nodes of that class that point at the current node through ref-valued properties. Nothing is mirrored back onto the target node, and no separate backlink record is stored.

## Executive summary

- Bidirectional properties are a derived read-time view over existing DB-graph datoms, not a second write path. The core implementation lives in `deps/db/src/logseq/db.cljs`.
- A class must opt in with `:logseq.property.class/enable-bidirectional?`. It can optionally override the section title with `:logseq.property.class/bidirectional-property-title`.
- Only ref-valued user or plugin properties that declare `:logseq.property/classes` participate. Scalar properties and unscoped properties are ignored.
- The UI loads bidirectional properties only for the root block or page properties area through an async DB-worker call. I did not find a file-graph implementation.
- Follow-up fixes matter to the current behavior: PR #12296 introduced the feature, commit `cdc1bc1d32` optimized it, and commit `96367595aa` fixed a setup regression.

## What the feature is

Think of bidirectional properties as a property-aware version of backlinks:

- Ordinary backlinks answer: "Which nodes mention or link to this page?"
- Bidirectional properties answer: "Which nodes of an opted-in class point at this node through structured ref-valued properties?"

Example:

- Class `Book` defines a ref-valued property `author`.
- Page `Dune` is tagged `#Book` and sets `author -> Frank Herbert`.
- If class `Book` enables bidirectional properties, the page `Frank Herbert` shows a `Books` section containing `Dune`.

The key point is that `Frank Herbert` does not gain a stored `books` property. Logseq computes the reverse view by scanning incoming property references.

## What exactly happens when you open a target node

The user-visible trigger is not a keyboard shortcut. It is rendering the root properties area for a page or block.

### Execution flow

```text
Open page or block
  -> properties-area renders
  -> load-bidirectional-properties effect runs for root node only
  -> frontend.db.async/<get-bidirectional-properties
  -> worker thread-api/get-bidirectional-properties
  -> logseq.db/get-bidirectional-properties
  -> reverse ref scan + filtering + grouping
  -> seq of {:title :class :entities} returns to UI
  -> bidirectional-properties-section renders below normal properties
```

### Timeline

```text
Open node -> render root properties -> async worker query -> section appears
 [0ms]          [same tick]            [no fixed delay]     [when promise resolves]

Persistence exposure window: none specific to this feature.
Display exposure window: brief async empty state until the worker returns.
```

There is no debounce, autosave timer, or feature-specific persistence batch in the inspected path.

## Technical deep dive

### 1. Schema and setup

PR #12296 added two built-in class properties in schema version `65.20`:

- `:logseq.property.class/bidirectional-property-title`
- `:logseq.property.class/enable-bidirectional?`

That migration is registered in `src/main/frontend/worker/db/migrate.cljs`.

The built-in property definitions live in `deps/db/src/logseq/db/frontend/property.cljs`:

- `enable-bidirectional?` is a public checkbox in class view context.
- `bidirectional-property-title` is a public string in class view context.

The public reference entry in `ddocs/reference/data/properties-reference.md` describes the feature accurately: enabling it on a tag shows reverse nodes that link to the current node via properties.

### 2. Worker and UI path

The frontend path is narrow and fully DB-specific:

- `src/main/frontend/components/property.cljs` calls `load-bidirectional-properties`.
- That effect only runs when the current properties area belongs to the root block or page.
- `src/main/frontend/db/async.cljs` forwards the request to the DB worker.
- `src/main/frontend/worker/db_worker.cljs` calls `ldb/get-bidirectional-properties` on the live Datascript connection.

The result is stored only in local component state:

```clojure
{:title "Books"
 :class <class-entity-map>
 :entities [<source-entity> ...]}
```

This payload is in-memory UI state only. It is recomputed from the DB whenever the loader runs.

### 3. Core DB algorithm

The owning implementation is `get-bidirectional-properties` in `deps/db/src/logseq/db.cljs`.

The algorithm is:

1. Find reverse datoms `[?e ?a target-id]` where the attribute entity `?a` has `:logseq.property/classes`.
2. For each candidate attribute, keep it only if it is:
   - a qualified keyword
   - in a user-property or plugin-property namespace
   - ref-valued in Datascript (`:db.type/ref`)
3. Load each source entity `?e` and drop it if it is:
   - the target itself
   - recycled
   - a class entity
   - a property entity
4. Read the source entity's tags from `:block/tags` and keep only non-built-in, non-recycled classes.
5. Group the source entity under each surviving class.
6. For each class group, require `:logseq.property.class/enable-bidirectional?` to be true.
7. Build the display title:
   - use `:logseq.property.class/bidirectional-property-title` if set
   - otherwise pluralize the class title
8. Sort entities by `:block/created-at`, then sort groups by the class's `:block/created-at`.

### 4. What gets grouped

The section is grouped by the source entity's class, not by property name.

That means:

- one target can show multiple bidirectional sections
- the same source entity can appear under more than one section if it has multiple non-built-in classes
- the section title describes the source class, not the property used to reach the target

This is why the feature feels closer to "reverse objects of class X" than to a generic incoming-edge inspector.

## Data structures

### Property entity

**Location:** property pages and built-in property entities in the DB schema

**Purpose:** marks whether an attribute can participate in bidirectional lookup

**Relevant fields:**

```clojure
{:db/ident :user.property/author
 :db/valueType :db.type/ref
 :logseq.property/classes #{...}}
```

**Lifecycle:** persisted as normal graph schema data; read at query time

**Why it matters:** a property without `:logseq.property/classes`, or a property that is not ref-valued, is ignored entirely

### Class entity

**Location:** tag/class pages

**Purpose:** opt-in switch and display metadata for bidirectional sections

**Relevant fields:**

```clojure
{:block/title "Book"
 :logseq.property.class/enable-bidirectional? true
 :logseq.property.class/bidirectional-property-title "Books"}
```

**Lifecycle:** persisted as normal class data; surfaced in class UI only

**Why it matters:** the section exists only if the grouping class opts in

### Source entity

**Location:** ordinary page or block entities that point at the target

**Purpose:** the nodes ultimately rendered in the reverse section

**Relevant fields:**

```clojure
{:block/title "Dune"
 :block/tags #{:Book}
 :user.property/author <target-id>}
```

**Lifecycle:** persisted content data; filtered out when recycled

**Why it matters:** only ordinary content nodes are shown. Classes, properties, and recycled entities are excluded.

### Derived result payload

**Location:** local Rum state in `properties-area`

**Purpose:** transport the grouped result from worker to UI

**Shape:**

```clojure
{:title "Books"
 :class {...}
 :entities [{...} {...}]}
```

**Lifecycle:** computed on demand, stored in-memory, not persisted

**Why it matters:** if the loader does not rerun, the UI can stay stale even though the DB is correct

## What is persisted and what is not

### Persisted

- class opt-in flag: `:logseq.property.class/enable-bidirectional?`
- optional section title override: `:logseq.property.class/bidirectional-property-title`
- property definitions and their `:logseq.property/classes`
- ordinary source-node property values that point at the target

### Not persisted

- any reverse-reference cache for bidirectional properties
- any mirrored property written onto the target node
- any separate backlink table for this feature

This keeps data-loss risk low. If the graph data persists correctly, bidirectional properties can be recomputed.

## Comparison tables

### Bidirectional properties vs linked references

| Aspect | Bidirectional properties | Linked references / backlinks |
| ------ | ------------------------ | ----------------------------- |
| Trigger data | Ref-valued property datoms | Page links, refs, and mentions |
| Opt-in | Yes, per class | No, automatic |
| Grouping | By source class | By reference context |
| Write path | No extra writes | No extra writes |
| Scope | DB graphs only | Broader Logseq linking model |
| Setup sensitivity | High | Low |

### DB graphs vs file-based graphs

| Aspect | DB graph | File-based graph |
| ------ | -------- | ---------------- |
| Built-in setup properties | Present | Not found |
| Query path | Datascript reverse scan via DB worker | No implementation found |
| UI section | Root properties area | No implementation found |
| Persistence model | Regular DB-graph entities and datoms | Not applicable for this feature |

### Platform notes

| Aspect | Finding |
| ------ | ------- |
| Electron main process | No direct implementation in inspected path |
| Shared frontend | Yes, the rendering path is in common frontend CLJS |
| Worker boundary | Yes, through the DB worker thread API |

## Scenario analysis

### Scenario A: standard working setup

**Setup:** class `Person` has `enable-bidirectional? = true`; property `friend` is ref-valued and class-scoped to `Person`; page `Alice` tagged `#Person` points `friend -> Bob`.

**Expected result:** page `Bob` shows a `People` section containing `Alice`.

**Observed in tests:** this is covered by unit and E2E tests.

**Risk level:** low

### Scenario B: property is not class-scoped

**Setup:** a ref-valued property points at the target, but the property entity does not declare `:logseq.property/classes`.

**Expected result:** users may expect a reverse section.

**Actual result:** nothing is shown.

**Why:** reverse lookup starts from attribute entities that have `:logseq.property/classes`.

**Risk level:** medium, because the failure mode is silent.

### Scenario C: class did not opt in

**Setup:** source nodes point at the target correctly, but the source class does not set `enable-bidirectional?`.

**Actual result:** the result group is dropped.

**Risk level:** low

### Scenario D: source node or class is recycled

**Setup:** the source entity or source class has been recycled/deleted.

**Actual result:** the reverse section omits it.

**Why:** current code explicitly filters recycled entities, and ADR 0009 calls out bidirectional/property-derived views as sensitive to schema deletion behavior.

**Risk level:** low

### Scenario E: page stays open while reverse references change

**Setup:** the target page is already open and some other node starts or stops referencing it.

**Likely result from current code:** the local section may remain stale until the properties area remounts or the loader reruns, because the effect depends on `root-block?` and target `:db/id`, not on reverse-reference updates.

**Risk level:** medium

**Why it matters:** this is a freshness risk, not a persistence risk.

## Risks and edge cases

### Low risk: data loss

The feature adds no special write path, queue, or cache on disk. Underlying class/property/content data persists through the normal DB-graph mechanisms, and the reverse view is recomputed.

### Medium risk: setup confusion

Several conditions must line up before anything renders:

- the property must be ref-valued
- the property must be class-scoped
- the source class must be non-built-in
- the source class must opt in
- the user must be viewing the root properties area

If any of those are wrong, the section simply does not appear.

### Medium risk: stale UI

The loader is an async effect rather than a direct query subscription. The code clearly optimizes for mount-time fetches and root-node rendering, but that also means freshness depends on when the effect reruns.

### Medium risk: performance on highly connected targets

The algorithm still scans incoming reverse datoms for the target. Commit `cdc1bc1d32` fixed the worst repeated attribute lookup pattern by caching `bidirectional-property-attr?` results per call, and the current test suite asserts bounded keyword lookups.

## Code location reference

| File | Purpose | Key functions / data | Lines |
| ---- | ------- | -------------------- | ----- |
| `deps/db/src/logseq/db.cljs` | Core reverse lookup and grouping | `bidirectional-property-attr?`, `get-ea-by-v`, `get-bidirectional-properties` | around 827-907 |
| `deps/db/src/logseq/db/frontend/property.cljs` | Built-in class property definitions | `:logseq.property.class/bidirectional-property-title`, `:logseq.property.class/enable-bidirectional?` | around 165-184 |
| `src/main/frontend/worker/db/migrate.cljs` | Schema registration | schema version `65.20` | around 74 |
| `src/main/frontend/worker/db_worker.cljs` | Worker bridge | `:thread-api/get-bidirectional-properties` | around 1065-1069 |
| `src/main/frontend/db/async.cljs` | Frontend async wrapper | `<get-bidirectional-properties` | around 48-52 |
| `src/main/frontend/components/property.cljs` | Loader and rendering | `load-bidirectional-properties`, `bidirectional-properties-section`, `properties-area` | around 357-388 and 694-848 |
| `src/main/frontend/handler/property.cljs` | Class-property picker exclusions | `class-property-excludes` | around 32-43 |
| `deps/db/test/logseq/db_test.cljs` | Unit and performance coverage | `get-bidirectional-properties*` tests | around 164-268 |
| `clj-e2e/test/logseq/e2e/bidirectional_properties_test.clj` | End-to-end behavior check | `bidirectional-properties-test` | around 14-52 |
| `docs/adr/0009-hybrid-schema-deletion.md` | Deletion semantics context | notes on bidirectional/property-derived views | around 10-68 |

## Recommendations

### For users

- Enable bidirectional properties on the class you want to appear as a reverse section.
- Make sure the property pointing at the target is a ref-valued property, not a plain string.
- Add the property to a class, not just as an ad hoc scalar field, if you want it to participate reliably.
- Look for the section on the target page or block's root properties area.

### For developers

- Preserve the current "derived view" model. Do not add mirrored writes unless there is a very strong reason.
- Keep the opt-in check tied to the displayed source class, not to every class participating in intermediate setup.
- If live freshness becomes important, consider subscribing the section to reverse-reference-relevant tx changes instead of relying only on mount-time async fetch.
- If file-graph parity is desired, treat it as a separate design problem. The current feature depends on DB schema entities and Datascript reverse scans.

### For testing

- Keep unit coverage for disabled-by-default, enabled-per-class, recycled-entity filtering, and performance bounds.
- Add or keep a regression test for the April 2026 setup bug fixed by `96367595aa`.
- Consider a UI-level test for live refresh behavior while a target page remains open.
- Consider a test where one source entity has multiple non-built-in classes and verify expected grouping.

## Appendix

### Glossary

- **Target node:** the page or block currently being viewed, whose incoming property references are being inspected.
- **Source node:** a page or block that points at the target through a ref-valued property.
- **Class:** a non-built-in tag entity stored in `:block/tags` and configured through class properties.
- **Derived view:** data computed at read time from other persisted data rather than stored separately.

### Change history

| Date | Change |
| ---- | ------ |
| 2026-01-21 | PR #12296 merged the initial feature and schema version `65.20`. |
| 2026-02-26 | Commit `cdc1bc1d32` optimized repeated attribute checks in `get-bidirectional-properties`. |
| 2026-04-22 | Commit `96367595aa` fixed a regression in bidirectional-properties setup semantics. |

### Bottom line

Bidirectional properties are not truly "two-way properties" in storage. They are a computed reverse lens over class-scoped ref properties in DB graphs, rendered as a class-grouped section on the referenced node.
