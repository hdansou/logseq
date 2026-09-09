---
title: 'Bidirectional properties'
description: 'Understanding how Logseq DB graphs derive reverse-reference sections from ref-valued properties'
status: stable
audience: [developer, user]
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
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

- Bidirectional properties are a derived read-time view over existing DB-graph datoms, not a second write path. The core implementation lives in `deps/db/src/logseq/db.cljs:858-937`.
- A class must opt in with `:logseq.property.class/enable-bidirectional?`. `:logseq.property.class/bidirectional-property-title` is meant to override the section title, but at `be800f1711` that title never reaches the UI — see "Low risk: title override is not displayed".
- Only ref-valued user or plugin properties that declare `:logseq.property/classes` participate. Scalar properties and unscoped properties are ignored.
- The UI subscribes to the section through the worker **render-resource** system (`db-hooks/use-resource [:block-bidirectional-properties <block-uuid>]`), and the worker invalidates that subscription on every transaction that could change the result. It is no longer a one-shot mount-time async fetch. There is still no file-graph implementation.
- The section renders as a **tab strip**, one tab per source class, not as a flat list.
- Follow-up work matters to the current behavior: PR #12296 introduced the feature, commit `cdc1bc1d32` optimized it, commit `96367595aa` fixed a setup regression, and the July/August 2026 reactivity refactor (`fb1047d1f8`, `d81317b38d`, `967c33092b`) moved it onto render resources.

## What the feature is

Think of bidirectional properties as a property-aware version of backlinks:

- Ordinary backlinks answer: "Which nodes mention or link to this page?"
- Bidirectional properties answer: "Which nodes of an opted-in class point at this node through structured ref-valued properties?"

Example:

- Class `Book` defines a ref-valued property `author`.
- Page `Dune` is tagged `#Book` and sets `author -> Frank Herbert`.
- If class `Book` enables bidirectional properties, the page `Frank Herbert` shows a tab labelled `Book` (the source class's `:block/title`) containing `Dune`.

The key point is that `Frank Herbert` does not gain a stored `books` property. Logseq computes the reverse view by scanning incoming property references.

The property `author` must also declare `:logseq.property/classes` (i.e. be scoped to a tag) for
any of this to happen; an unscoped ref property is invisible to the reverse scan.

## What exactly happens when you open a target node

The user-visible trigger is not a keyboard shortcut. It is rendering the root properties area for a page or block.

### Execution flow

```text
Open page or block
  -> properties-area renders (components/property.cljs:892)
  -> bidirectional-properties-area, for root block or page only (components/property.cljs:875-884)
  -> db-hooks/use-resource [:block-bidirectional-properties <block-uuid>]  (db/hooks.cljs:124-126)
  -> worker resource renderer :block-bidirectional-properties
     (worker/handler/render_resource/property.cljs:143-159)
  -> logseq.db/get-bidirectional-properties (deps/db/src/logseq/db.cljs:909-937)
  -> reverse ref scan + filtering + grouping
  -> renderer returns [#{[:bidirectional block-uuid]}          ; invalidation keys
                       [{:class-uuid ... :entity-uuids [...]}]] ; payload
  -> bidirectional-properties-section renders a tab strip below normal properties
     (components/property.cljs:440-460)

On every subsequent transaction:
  worker/render_affected_keys.cljs:455-484 recomputes the affected
  [:bidirectional <target-uuid>] keys and re-pushes the resource to the UI.
```

> **Removed:** The former path through `frontend.db.async/<get-bidirectional-properties` and a
> `:thread-api/get-bidirectional-properties` handler in `src/main/frontend/worker/db_worker.cljs`
> no longer exists. Neither file mentions bidirectional properties at `be800f1711`. The
> `def-thread-api` entry now lives in `src/main/frontend/worker/handler/property.cljs:299-303`
> and is invoked by the render-resource renderer rather than by a component effect. There is
> also no `load-bidirectional-properties` function anywhere in the tree.

### Timeline

```text
Open node -> render root properties -> resource subscription -> section appears -> stays live
 [0ms]          [same tick]            [worker render]          [on first snapshot]  [per-tx invalidation]

Persistence exposure window: none specific to this feature.
Display exposure window: brief empty state until the first resource snapshot arrives.
```

There is no debounce, autosave timer, or feature-specific persistence batch in the inspected path.

## Technical deep dive

### 1. Schema and setup

PR #12296 added two built-in class properties in schema version `65.20`:

- `:logseq.property.class/bidirectional-property-title`
- `:logseq.property.class/enable-bidirectional?`

That migration is registered at `src/main/frontend/worker/db/migrate.cljs:139`. The current
schema version is `65.33` (`deps/db/src/logseq/db/frontend/schema.cljs:33`), so `65.20` is
well behind head; graphs created today get both properties at graph-creation time rather than
through the migration.

The built-in property definitions live in `deps/db/src/logseq/db/frontend/property.cljs`:

- `bidirectional-property-title` is a public `:string` with `:view-context :class` (lines 177-180).
- `enable-bidirectional?` is a public `:checkbox` with `:view-context :class` (lines 181-186).

Both are excluded from the tag "add property" picker by `class-property-excludes`
(`src/main/frontend/handler/property.cljs:40-49`), because they are surfaced through dedicated
class UI instead.

The public reference entry in `ddocs/reference/data/properties-reference.md` describes the feature accurately: enabling it on a tag shows reverse nodes that link to the current node via properties.

### 2. Worker and UI path

The frontend path is narrow and fully DB-specific:

- `src/main/frontend/components/property.cljs:875-884` (`bidirectional-properties-area`) calls
  `db-hooks/use-resource [:block-bidirectional-properties block-uuid]`.
- It renders only when the properties area belongs to the root block or a page
  (`root-block?` or `entity/page?`), and callers can suppress it with
  `:skip-bidirectional-properties?` (`components/property.cljs:893`, `1042-1044`).
- `src/main/frontend/db/hooks.cljs:124-126` wires the resource key to the worker subscription.
- `src/main/frontend/worker/handler/render_resource/property.cljs:143-159` is the renderer. It
  calls `ldb/get-bidirectional-properties` on the live Datascript connection and returns both
  an invalidation-key set and the serialized payload.
- `src/main/frontend/worker/render_affected_keys.cljs:53-64` and `404-484` decide which
  `[:bidirectional <target-uuid>]` keys a transaction invalidates.
- `src/main/frontend/worker/handler/property.cljs:299-303` still exposes
  `:thread-api/get-bidirectional-properties` for direct worker calls.

The payload that crosses the worker boundary carries **uuids, not entity maps**, and does not
carry a title:

```clojure
[{:class-uuid #uuid "..."
  :entity-uuids [#uuid "..." #uuid "..."]}]
```

The UI resolves each `:class-uuid` back to a class through `db-hooks/use-block` and uses
`(:block/title class)` for the tab label (`components/property.cljs:426-438`). The `:title`
that `logseq.db/get-bidirectional-properties` computes — including the pluralization and the
`bidirectional-property-title` override — is currently **dropped by the renderer** and is not
what the user sees on the tab.

This payload is in-memory UI state only. It is recomputed by the worker whenever an
invalidation key fires.

### 3. Core DB algorithm

The owning implementation is `get-bidirectional-properties` in
`deps/db/src/logseq/db.cljs:909-937`, supported by three private helpers in the same file:
`bidirectional-property-attr?` (858-866), `get-bidirectional-property-attrs` (868-876),
`get-ea-by-v` (878-883) and `build-bidirectional-property-group` (892-907).

The algorithm is:

0. Bail out entirely if the target itself is a property-value block, i.e. it has
   `:logseq.property/created-from-property` (`db.cljs:915-916`). This guard is easy to miss and
   means property-value blocks never show a bidirectional section.
1. Build the candidate attribute set **once per call** with a Datalog query for every property
   that is both `:db.type/ref` and has `:logseq.property/classes` (`db.cljs:868-876`), then scan
   `:avet` datoms for `[?a target-id]` per candidate attribute (`get-ea-by-v`, `db.cljs:878-883`).
2. For each candidate attribute, keep it only if it is:
   - a qualified keyword
   - in a user-property (`namespace` contains `.property`) or plugin-property (`plugin.property.` prefix) namespace
   - ref-valued in Datascript (`:db.type/ref`)
   - non-empty `:logseq.property/classes`
3. Load each source entity `?e` and drop it if it is:
   - the target itself
   - recycled
   - a class entity
   - a property entity
4. Read the source entity's tags from `:block/tags` and keep only non-built-in, non-recycled classes.
5. Group the source entity under each surviving class.
6. For each class group, require `:logseq.property.class/enable-bidirectional?` to be true.
7. Build the display title:
   - use `:logseq.property.class/bidirectional-property-title` if set and non-blank (it may be a
     string or a ref, in which case `property-value-content` unwraps it)
   - otherwise pluralize the class title via `common-plural/plural`
   - **note:** this title does not reach the UI at `be800f1711`; see "Worker and UI path" above.
8. Sort entities by `:block/created-at`, then sort groups by the class's `:block/created-at`.

### 4. What gets grouped

The section is grouped by the source entity's class, not by property name.

That means:

- one target can show multiple bidirectional tabs
- the same source entity can appear under more than one tab if it has multiple non-built-in classes
- the tab label describes the source class, not the property used to reach the target

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

**Location:** worker render-resource output, consumed by `bidirectional-properties-area`

**Purpose:** transport the grouped result from worker to UI

**Shape returned by `logseq.db/get-bidirectional-properties` (`db.cljs:903-907`):**

```clojure
{:title "Books"
 :class {...}
 :entities [{...} {...}]}
```

**Shape actually sent to the UI (`render_resource/property.cljs:148-155`):**

```clojure
[#{[:bidirectional block-uuid]}                              ; invalidation keys
 [{:class-uuid #uuid "..." :entity-uuids [#uuid "..." ...]}]] ; payload
```

**Lifecycle:** computed on demand in the worker, stored in-memory, not persisted

**Why it matters:** the UI only receives uuids, so it re-reads the class and each source node
through its own hooks. The worker re-renders the resource whenever a matching
`[:bidirectional <target-uuid>]` key is invalidated, so the section does not go stale.

## What is persisted and what is not

### Persisted

- class opt-in flag: `:logseq.property.class/enable-bidirectional?`
- optional section title override: `:logseq.property.class/bidirectional-property-title` (persisted and readable, but not rendered at `be800f1711`)
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
| Live refresh | Yes, worker invalidation keys | Yes |

### DB graphs vs file-based graphs

| Aspect | DB graph | File-based graph |
| ------ | -------- | ---------------- |
| Built-in setup properties | Present | Not found |
| Query path | Datascript reverse scan via DB worker render resource | No implementation found |
| UI section | Tab strip in the root properties area | No implementation found |
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

**Expected result:** page `Bob` shows a tab labelled `Person` containing `Alice`.

**Observed in tests:** covered by `deps/db/test/logseq/db_test.cljs:291-397` and
`clj-e2e/test/logseq/e2e/bidirectional_properties_test.clj:15-51`.

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

**Why:** current code explicitly filters recycled entities (`db.cljs:922`, `929`), and ADR 0009
calls out bidirectional/property-derived views as sensitive to schema deletion behavior
(`docs/adr/0009-hybrid-schema-deletion.md:19`, `:68`). Regression coverage lives in
`deps/db/test/logseq/db_test.cljs:330-344`.

**Risk level:** low

### Scenario E: page stays open while reverse references change

**Setup:** the target page is already open and some other node starts or stops referencing it.

**Result at `be800f1711`:** the section updates live. `render_affected_keys.cljs:455-484`
recomputes the affected `[:bidirectional <target-uuid>]` keys from the transaction's datoms —
including edits to a property's `:db/ident`, `:db/valueType`, `:logseq.property/classes` or
`:logseq.property/deleted-at` (lines 53-57), and edits to a class's title, tags, icon-bearing
tags, `bidirectional-property-title`, `enable-bidirectional?`, `built-in?` or `deleted-at`
(lines 59-65) — and the worker re-renders the resource.

**Risk level:** low

> **Removed:** the earlier version of this page described this as a medium freshness risk on the
> grounds that the loader was a mount-time async effect keyed on `root-block?` and `:db/id`.
> That code path no longer exists; see the "Removed" note under Execution flow.

### Scenario F: target is a property-value block

**Setup:** the currently viewed node is a block created to hold a property value, i.e. it has
`:logseq.property/created-from-property`.

**Actual result:** `get-bidirectional-properties` returns `nil` before doing any scan
(`db.cljs:915-916`), so no section renders regardless of how many nodes point at it.

**Risk level:** low, but it is an undocumented hard exclusion.

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

### Resolved: stale UI

> **Removed:** this page previously listed stale UI as a medium risk. The mount-time async
> effect it described is gone. `bidirectional-properties-area` now uses a worker render-resource
> subscription (`components/property.cljs:875-884`, `db/hooks.cljs:124-126`) and
> `render_affected_keys.cljs:455-484` invalidates it per transaction.

### Low risk: title override is not displayed

`build-bidirectional-property-group` computes a pluralized or user-overridden `:title`
(`db.cljs:896-902`), but the render-resource layer serializes only `:class-uuid` and
`:entity-uuids` (`render_resource/property.cljs:148-155`), and the tab label is
`(:block/title class)` (`components/property.cljs:438`). Setting
`:logseq.property.class/bidirectional-property-title` therefore has no visible effect on the tab
strip at this commit. This is a behavioral gap, not a data risk.

### Medium risk: performance on highly connected targets

The algorithm still scans incoming reverse datoms for the target. Commit `cdc1bc1d32` fixed the
worst repeated attribute lookup pattern by hoisting the candidate-attribute query out of the
per-datom loop (`get-bidirectional-property-attrs`, `db.cljs:868-876`), and the current test
suite asserts bounded keyword lookups
(`deps/db/test/logseq/db_test.cljs:369-397`, both tests tagged `^:long`).

## Code location reference

All line numbers below are pinned to `origin/master` at `be800f1711`.

| File | Purpose | Key functions / data | Lines |
| ---- | ------- | -------------------- | ----- |
| `deps/db/src/logseq/db.cljs` | Core reverse lookup and grouping | `bidirectional-property-attr?`, `get-bidirectional-property-attrs`, `get-ea-by-v`, `build-bidirectional-property-group`, `get-bidirectional-properties` | 858-937 |
| `deps/db/src/logseq/db/frontend/property.cljs` | Built-in class property definitions | `:logseq.property.class/bidirectional-property-title`, `:logseq.property.class/enable-bidirectional?` | 177-186 |
| `src/main/frontend/worker/db/migrate.cljs` | Schema registration | schema version `65.20` entry in `schema-version->updates` | 139 |
| `src/main/frontend/worker/handler/property.cljs` | Worker thread API | `:thread-api/get-bidirectional-properties` | 299-303 |
| `src/main/frontend/worker/handler/render_resource/property.cljs` | Render-resource renderer | `block-bidirectional-properties`, `resource-renderers` | 143-159 |
| `src/main/frontend/worker/render_affected_keys.cljs` | Per-transaction invalidation | `bidirectional-property-config-attrs`, `bidirectional-class-config-attrs`, `enabled-bidirectional-class-ids`, `bidirectional-property?`, `bidirectional-target-uuids`, `bidirectional-keys` | 53-65, 404-484, 577 |
| `src/main/frontend/db/hooks.cljs` | Frontend resource subscription | `use-resource` | 124-126 |
| `src/main/frontend/components/property.cljs` | Subscription and rendering | `bidirectional-values-cp`, `bidirectional-tab-icon`, `bidirectional-property-group`, `bidirectional-properties-section`, `bidirectional-properties-area`, `properties-area` | 393-460, 875-884, 892-1044 |
| `src/main/frontend/handler/property.cljs` | Class-property picker exclusions | `class-property-excludes` | 40-49 |
| `deps/db/test/logseq/db_test.cljs` | Unit and performance coverage | `get-bidirectional-properties`, `get-bidirectional-properties-ignores-recycled-entities`, two `^:long` performance tests | 291-397 |
| `clj-e2e/test/logseq/e2e/bidirectional_properties_test.clj` | End-to-end behavior check | `bidirectional-properties-test` | 15-51 |
| `docs/adr/0009-hybrid-schema-deletion.md` | Deletion semantics context | notes on bidirectional/property-derived views | 19, 68 |

> **Removed:** `src/main/frontend/worker/db_worker.cljs` and `src/main/frontend/db/async.cljs`
> contain no bidirectional-properties code at `be800f1711` (verified with
> `git grep -n bidirectional origin/master -- <path>`, no matches). The rows citing
> `db_worker.cljs:1065-1069` and `async.cljs:48-52` have been deleted rather than re-pinned.

## Findings: New Capabilities

Discovered in `origin/master` at `be800f1711` and not described in the previous revision.

### Worker render resources replaced the mount-time async fetch

`:block-bidirectional-properties` is now a registered resource renderer
(`src/main/frontend/worker/handler/render_resource/property.cljs:143-159`) alongside
`:block-display-properties` and `:block-positioned-properties`. The renderer returns a set of
invalidation keys plus the payload, and the UI subscribes with `db-hooks/use-resource`
(`src/main/frontend/db/hooks.cljs:124-126`). This makes the section live rather than
mount-time-only.

### Per-transaction invalidation for bidirectional targets

`src/main/frontend/worker/render_affected_keys.cljs` gained a dedicated bidirectional path:

- `bidirectional-property-config-attrs` (lines 53-57) — `:db/ident`, `:db/valueType`,
  `:logseq.property/classes`, `:logseq.property/deleted-at`.
- `bidirectional-class-config-attrs` (lines 59-65) — `:block/created-at`, `:block/tags`,
  `:block/title`, `:logseq.property.class/bidirectional-property-title`,
  `:logseq.property.class/enable-bidirectional?`, `:logseq.property/built-in?`,
  `:logseq.property/deleted-at`.
- `bidirectional-keys` (lines 455-484) expands touched property and class entities back to the
  source nodes that use them, then to the `[:bidirectional <target-uuid>]` keys to invalidate,
  computing the expansion against both `db-before` and `db-after`.

Note that `render_affected_keys.cljs:404-413` additionally requires the source's class to be
tagged `:logseq.class/Tag`, which `deps/db/src/logseq/db.cljs` does not require. The two
predicates are not identical.

### The section is a tab strip, not a list

`bidirectional-properties-section` (`src/main/frontend/components/property.cljs:440-460`) builds
`shui/tabs` with one `tabs-trigger` per class. Each trigger carries a `:data-entity-count`
attribute and may show the class icon (`bidirectional-tab-icon`, lines 418-424, suppressed when
the icon id is `"hash"`). Tab content renders through the shared blocks container with
`:default-collapsed? true`, `:bidirectional? true`, `:ref? true`, `:hide-block-tags? true` and
`:hide-block-icon? true` (lines 393-410).

### Callers can suppress the section

`properties-area` accepts `:skip-bidirectional-properties?`
(`src/main/frontend/components/property.cljs:893`, `1042-1044`), a knob that did not exist when
this page was last written.

### Property-value blocks are excluded as targets

`get-bidirectional-properties` returns `nil` when the target has
`:logseq.property/created-from-property` (`deps/db/src/logseq/db.cljs:915-916`).

### The computed `:title` never reaches the UI

See "Low risk: title override is not displayed" above. This is the one finding on this page that
looks like a live bug rather than a documentation drift.

## Recommendations

### For users

- Enable bidirectional properties on the class you want to appear as a reverse section.
- Make sure the property pointing at the target is a ref-valued property, not a plain string.
- Add the property to a class, not just as an ad hoc scalar field, if you want it to participate reliably.
- Look for the tab strip below the normal properties on the target page or block's root properties area. One tab per source class.

### For developers

- Preserve the current "derived view" model. Do not add mirrored writes unless there is a very strong reason.
- Keep the opt-in check tied to the displayed source class, not to every class participating in intermediate setup.
- Live freshness is now handled by `render_affected_keys.cljs`. When adding a new attribute that
  can change the result, remember to add it to `bidirectional-property-config-attrs` or
  `bidirectional-class-config-attrs` (`render_affected_keys.cljs:53-65`), or the section will
  silently miss updates.
- Re-connect `:title` from `build-bidirectional-property-group` to the tab label, or delete the
  title-building code. Today it is computed and thrown away, and
  `:logseq.property.class/bidirectional-property-title` has no visible effect.
- If file-graph parity is desired, treat it as a separate design problem. The current feature depends on DB schema entities and Datascript reverse scans.

### For testing

- Keep unit coverage for disabled-by-default, enabled-per-class, recycled-entity filtering, and performance bounds.
- Add or keep a regression test for the April 2026 setup bug fixed by `96367595aa`.
- Add coverage for the property-value-block guard at `db.cljs:915-916`; it is currently untested
  in `deps/db/test/logseq/db_test.cljs`.
- Add coverage asserting `:logseq.property.class/bidirectional-property-title` reaches the tab
  label, which would have caught the render-resource serialization gap.
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
| 2026-01-04 | Commit `3f8c6cde92` "feat: Bi-directional property" introduced `get-bidirectional-properties` in `deps/db/src/logseq/db.cljs`. |
| 2026-01-22 | Merge commit `6c58b77392` merged PR #12296 (`feat/bi-directional-properties`) with schema version `65.20`. |
| 2026-02-26 | Commit `cdc1bc1d32` "fix: get-bidirectional-properties perf" optimized repeated attribute checks. |
| 2026-04-22 | Commit `96367595aa` "fix(regression): bidirectional properties setup" fixed setup semantics. |
| 2026-07-21 | Commit `fb1047d1f8` "refactor: data fetch && reactivity" moved the UI onto the worker render-resource system. |
| 2026-08-04 | Commits `d81317b38d` and `967c33092b` streamlined worker render subscriptions and render state. |

The prior revision of this page dated the PR #12296 merge to 2026-01-21; the merge commit
`6c58b77392` is dated 2026-01-22.

### Bottom line

Bidirectional properties are not truly "two-way properties" in storage. They are a computed reverse lens over class-scoped ref properties in DB graphs, rendered as a class-grouped section on the referenced node.
