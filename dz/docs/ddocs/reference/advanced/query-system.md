---
title: 'Query System Reference'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-22'
verifiedAgainst: '5824434a74'
verifiedScope: 'Full audit 2026-09-09 (1be80c82a1, 03ae5276c6). The 2026-09-10 date covers only the :block/properties note on :result-transform (bfcaeb03e7). The 2026-09-22 date covers only two sections, checked against 39ade9a309: "View Settings Are Block Properties" (property idents, operators and the CLI write, the latter run live on the cliworker and plugin-test graphs) and "Sorting Results" (result ordering traced through source, not observed in a running app).'
reviewStatus: audited
quadrant: 'Reference'
platform: 'Desktop + Mobile (Database graphs only)'
related-docs:
  - '[Schema System](../../explanation/architecture/schema-system.md)'
  - '[Properties Reference](../data/properties-reference.md)'
  - '[Advanced Queries How-To](../../how-to/queries/advanced-queries.md)'
tags: ['queries', 'datalog', 'advanced', 'datascript', 'database']
---

# Query System Reference

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

This reference documents Logseq's advanced query system—Datalog queries for retrieving and analyzing graph data in **database (DB) graphs**. Use queries to build custom views, generate reports, and automate workflows.

**What's Covered:**

- How a query block is stored and parsed
- The query map keys the app actually honours
- The shape of query results
- Datalog patterns for DB-graph attributes and properties
- Built-in rules
- Performance notes

**Prerequisites:**

- Familiarity with database graphs ([Schema System](../../explanation/architecture/schema-system.md))
- Basic understanding of Datalog (see [Learn Datalog Today](http://www.learndatalogtoday.org/))

> **Scope — DB graphs only.** Everything below is verified against DB-graph source. File-graph
> constructs do **not** work here: `#+BEGIN_QUERY` / `#+END_QUERY` is file-graph markup that a DB
> graph never parses, and `:block/marker`, `:block/content`, `:block/properties`, `:block/journal?`,
> `:block/scheduled`, `:block/deadline` and `:block/namespace` are not attributes of the DB-graph
> schema (`deps/db/src/logseq/db/frontend/schema.cljs:56-108`).
>
> One nuance on that list: `:block/properties` is not a DataScript attribute either, so it never
> matches in a `:where` clause — but it survives as a *computed entity lookup* returning
> `{property-ident value}` (`deps/db/src/logseq/db/common/entity_plus.cljc:138-146`, dispatched at
> `:162-163`). It therefore works on a DataScript entity object and only there — not in a `:where` clause, and not
> inside `:result-transform`, which receives plain pulled maps from `d/q`
> (`src/main/frontend/worker/handler/query.cljs:243-261`).

---

## Table of Contents

1. [Query Syntax](#query-syntax)
2. [Result Shape](#result-shape)
3. [Basic Queries](#basic-queries)
4. [Advanced Queries](#advanced-queries)
5. [Query Functions](#query-functions)
6. [Query Rules](#query-rules)
7. [Query Inputs](#query-inputs)
8. [Performance Optimization](#performance-optimization)
9. [Query Patterns](#query-patterns)
10. [Custom Views](#custom-views)

---

## Query Syntax

### How a Query Block Is Stored

Run the **`/Advanced Query`** slash command. It tags the current block with
`:logseq.class/Query`, creates the `:logseq.property/query` property on it, and renders that
property's value as a Clojure code editor
(`src/main/frontend/commands.cljs:125-132`).

The text you type into that editor becomes the `:block/title` of the query-value block. At render
time Logseq reads it with `safe-read-string`; if the result is a **map** it is treated as an
advanced (Datalog) query, otherwise it is treated as a simple DSL query string
(`src/main/frontend/components/block.cljs:4139-4152`).

So an advanced query is written as a bare EDN map — no `#+BEGIN_QUERY` wrapper, no ` ```query `
fence:

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags ?t]
         [?t :db/ident :logseq.class/Task]]}
```

### View Settings Are Block Properties, Not Query Keys

The filter chips, group-by and sort order you set through the results toolbar are **not part of the
query**. They are hidden, non-public properties written onto the query block itself — the query
block doubles as the view entity, with feature type `:query-result`. They are defined in
`deps/db/src/logseq/db/frontend/property.cljs:432-536` and written by the toolbar in
`src/main/frontend/components/views.cljs:2135-2180`.

| Toolbar control | Property | Value shape |
| --- | --- | --- |
| Filter chips | `:logseq.property.table/filters` | `{:or? false :filters [[<property-ident> <operator> <value>] ...]}` |
| Group by | `:logseq.property.view/group-by-property` | a property ident, e.g. `:logseq.property/status` |
| Sort | `:logseq.property.table/sorting` | `[{:id <property-ident> :asc? false} ...]` |
| Column visibility | `:logseq.property.table/hidden-columns` | `[<property-ident> ...]` |
| Column order | `:logseq.property.table/ordered-columns` | `[<property-ident> ...]` |
| Table / List / Gallery | `:logseq.property.view/type` | `:logseq.property.view/type.table`, `.list` or `.gallery` |

Filter operators (`src/main/frontend/components/views.cljs:1837-1853`): `:is`, `:is-not`,
`:text-contains`, `:text-not-contains`, `:date-before`, `:date-after`, `:before`, `:after`,
`:number-gt`, `:number-lt`, `:number-gte`, `:number-lte`, `:between`. `:empty` is a *value*, so an
"is not empty" chip is `[:block/tags :is-not :empty]`. A filter that matches specific nodes stores
their `:block/uuid`s, not their titles (`views.cljs:2135-2151`).

Because these live on the block, they travel with the block and not with the query text. Copying
the query text alone loses them. They can be written without the UI:

```bash
logseq upsert block -g <graph> --id <query-block-id> --update-properties '{
  :logseq.property.table/filters {:or? false :filters [[:block/tags :is-not :empty]]}
  :logseq.property.view/group-by-property :logseq.property/status
  :logseq.property.table/sorting [{:id :logseq.property/priority :asc? false}
                                  {:id :block/updated-at :asc? false}]}'
```

`--remove-properties '[:logseq.property.table/sorting ...]'` clears them again. *(Verified
2026-09-22 by writing and reading back these properties on the `cliworker` and `plugin-test`
graphs.)*

### Query Map Keys

Only these keys reach the query engine. The renderer builds the worker request in
`src/main/frontend/components/query/result.cljs:17-47`, and the worker validates it in
`src/main/frontend/worker/handler/render_resource/query.cljs:48-107`.

| Key | Honoured? | Notes |
| --- | --- | --- |
| `:query` | ✅ required | Must be a **vector** whose first element is `:find` (`render_resource/query.cljs:96-105`). A list, or a vector not starting with `:find`, is routed to the simple DSL instead. |
| `:inputs` | ✅ | Vector; resolved per [Query Inputs](#query-inputs) (`result.cljs:28-30`). |
| `:rules` | ✅ | Vector of user-defined Datalog rules (`result.cljs:31-32`). |
| `:result-transform` | ✅ | A *quoted form* (not a live fn) or a keyword naming an entry in `:query/result-transforms` in `config.edn` (`components/query.cljs:133-140`, `result.cljs:46-47`). |
| `:remove-block-children?` | ✅ | `false` keeps nested result blocks; otherwise a block whose parent is also in the result set is dropped (`result.cljs:43-44`, `render_resource/query.cljs:171-179`). |
| `:collapsed?` | ✅ | Initial collapsed state (`components/query.cljs:144`). |
| `:view` | ✅ | A `(fn [rows] …)` form, or a keyword naming an entry in `:query/views` in `config.edn` (`components/query.cljs:148-149`). |
| `:title` | ❌ ignored | The title is only rendered for *built-in* journal queries (`components/query.cljs:166-174`). Put your heading in `:view`, or in the block above the query. |
| `:breadcrumb-show?` | ❌ ignored | Only reaches the renderer on the built-in-query branch (`components/query.cljs:59-71`). |
| `:group-by-page?` | ❌ ignored | Hard-coded to `false` for DB graphs (`components/query/result.cljs:10-11`). |
| `:limit` | ❌ not a key | There is no `:limit` key, and `:limit` *inside* `:find` throws a Query Error. Bound results in `:where`, or with `(take n …)` in `:view` / `:result-transform`. |

Anything outside the accepted key set for the query kind is rejected outright with
"Invalid renderer query resource" (`render_resource/query.cljs:68-107`).

### Minimal and Full Examples

```clojure
;; minimal
{:query [:find (pull ?b [:block/uuid])
         :where [?b :block/tags :logseq.class/Task]]}
```

```clojure
;; with inputs, a transform and an explicit view
{:query [:find (pull ?b [:block/uuid]) ?deadline
         :in $ ?start
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline ?deadline]
         [(>= ?deadline ?start)]]
 :inputs [:start-of-today-ms]
 :collapsed? false
 :remove-block-children? false}
```

---

## Result Shape

The worker normalises every result row before the UI sees it
(`src/main/frontend/worker/handler/render_resource/query.cljs:15-46`, `:195-202`):

1. Each row is coerced to a tuple.
2. Any cell that is a map or entity carrying a `uuid?` `:block/uuid` is replaced by the **bare
   UUID**. A map without a UUID is a hard error ("Renderer query result map has no UUID").
3. A one-element tuple is **unwrapped** to its single value; tuples of two or more elements
   **keep their tuple shape**.

Consequences you must design around:

- **Pull, don't bind.** `[:find ?b …]` returns DataScript entity IDs — integers, not blocks. To get
  rendered blocks you must pull: `[:find (pull ?b [:block/uuid]) …]`. Any pull selector works;
  only `:block/uuid` survives normalisation.
- **Single-column block queries** arrive at `:view` as a flat sequence of UUIDs. When there is no
  `:view`, that sequence is handed to the DB table renderer as `:query-row-uuids`
  (`src/main/frontend/components/query/view.cljs:6-15`).
- **Multi-column queries** arrive as tuples, so `(for [[a b] rows] …)` destructures directly.
  `(partition n rows)` is **wrong** — rows are already tuples.
- A **numeric first `:find` column is safe.** The old page-grouping heuristic that mis-read numeric
  first columns no longer exists.
- `:result-transform` runs on the **worker side, before** normalisation
  (`render_resource/query.cljs:181-202`), so it still sees pulled maps with their full selector.
  `:view` runs after, on UUIDs/tuples.

Block results are additionally filtered: hidden blocks and the query block itself are dropped, and
unless `:remove-block-children?` is `false`, a block whose parent is **also in the result set** is
dropped — the filter is relative to the result, not to the page
(`render_resource/query.cljs:171-179`, `deps/outliner/src/logseq/outliner/tree.cljs:106-111`).

---

## Basic Queries

### Find All Blocks with a Tag

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags ?t]
         [?t :db/ident :logseq.class/Task]]}
```

`:block/tags` is a cardinality-many ref (`schema.cljs:73-74`), so you may also match the ident
directly — the built-in `between` rule does exactly this
(`deps/db/src/logseq/db/frontend/rules.cljc:94-100`):

```clojure
[?b :block/tags :logseq.class/Task]
```

Both forms match **direct** tags only. For tag inheritance use the built-in `tags` rule
(see [Query Rules](#query-rules)).

### Find Blocks by Property Value

`:logseq.property/status` and `:logseq.property/priority` are `:default`-typed properties **with
closed values** (`deps/db/src/logseq/db/frontend/property.cljs:300-338`), which makes them
**refs**. You cannot compare them to a string or keyword; you must hop to the value entity and read
its `:db/ident`:

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :logseq.property/priority ?p]
         [?p :db/ident :logseq.property/priority.high]]}
```

The valid idents are:

- Status — `:logseq.property/status.backlog`, `.todo`, `.doing`, `.in-review`, `.done`, `.canceled`
  (`property.cljs:313-318`)
- Priority — `:logseq.property/priority.low`, `.medium`, `.high`, `.urgent` (`property.cljs:334-337`)

`[(= :logseq.property/priority.high (:db/ident ?p))]` does **not** work: `:db/ident` is not callable
inside a Datalog predicate.

### Find Pages by Name Pattern

```clojure
{:query [:find (pull ?p [:block/uuid])
         :where
         [?p :block/tags :logseq.class/Page]
         [?p :block/title ?title]
         [(clojure.string/starts-with? ?title "Project")]]}
```

There is no `:block/type` attribute in DB graphs. "Is this a page?" is answered by the page-ish tags
`:logseq.class/Page`, `:logseq.class/Journal`, `:logseq.class/Tag` and `:logseq.class/Property`
(`deps/db/src/logseq/db/frontend/entity_util.cljs:24-50`).

### Find Recent Blocks

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?cutoff
         :where
         [?b :block/updated-at ?time]
         [(> ?time ?cutoff)]]
 :inputs [:-7d-ms]}
```

`:block/updated-at` is an epoch-millisecond integer (`schema.cljs:85-86`), so the input must also be
milliseconds — `:-7d-ms`, not `:-7d` (which is a `YYYYMMDD` integer). See
[Query Inputs](#query-inputs).

---

## Advanced Queries

### Multiple Conditions (AND)

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/priority ?p]
         [?p :db/ident :logseq.property/priority.high]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.todo]]}
```

### OR Conditions

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :logseq.property/priority ?p]
         (or [?p :db/ident :logseq.property/priority.high]
             [?p :db/ident :logseq.property/priority.urgent])]}
```

Equivalent, and usually easier to extend:

```clojure
[?b :logseq.property/priority ?p]
[?p :db/ident ?ident]
[(contains? #{:logseq.property/priority.high
              :logseq.property/priority.urgent} ?ident)]
```

### NOT Conditions

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         (not-join [?b]
                   [?b :logseq.property/status ?s]
                   [?s :db/ident :logseq.property/status.done])]}
```

Use `not-join` when the negated clauses introduce their own variables (`?s` here); plain `not`
would leave `?s` unbound outside the negation.

### Joins Across Relationships

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?page :block/title "Project Alpha"]
         [?b :block/page ?page]
         [?b :block/tags :logseq.class/Task]]}
```

### Aggregation (Count)

```clojure
{:query [:find ?status-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :block/title ?status-title]]}
```

This is a two-column query, so rows keep their tuple shape and a `:view` can destructure them
directly:

```clojure
 :view (fn [rows]
         [:table
          [:tbody
           (for [[status n] (sort-by second > rows)]
             [:tr [:td status] [:td n]])]])
```

### Sorting Results

DataScript's `:find`/`:where` has **no `:order-by` clause**, and in a DB graph the query map cannot
order block results at all. Sorting a block result inside `:result-transform` is discarded:

1. The transform runs in the worker, then every pulled block map is reduced to its `:block/uuid`
   (`src/main/frontend/worker/handler/render_resource/query.cljs:15-53,188-209`).
2. A block result with no `:view` renders through the standard table view
   (`src/main/frontend/components/query.cljs:52-57`).
3. The view puts those ids into a **set** and sorts them by the block's
   `:logseq.property.table/sorting`, falling back to `[{:id :block/updated-at :asc? false}]`
   (`deps/db/src/logseq/db/common/view.cljs:1113-1135`).

`remove`, `filter` and `take` in a transform still work — they change which rows exist, not their
order. Order survives only where the table view is bypassed: a query with a `:view` function
receives the transformed rows directly (`components/query.cljs:42-50`), and built-in queries render
through `->hiccup`.

To order block results, set `:logseq.property.table/sorting` on the query block (see
[View Settings Are Block Properties](#view-settings-are-block-properties-not-query-keys)). To order
inside the query text, return scalars instead of pulled blocks and sort in a `:view` function:

```clojure
{:query [:find ?title ?deadline
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline ?deadline]
         [?b :block/title ?title]]
 :view (fn [rows]
         [:ul (for [r (sort-by (fn [r] (nth r 1)) rows)]
                [:li (nth r 0)])])}
```

Note that `:result-transform` is stored as a *form*, `pr-str`-ed and evaluated with SCI in the
worker (`result.cljs:46-47`, `render_resource/query.cljs:181-192`). It must return a sequential
collection or a set, or the query fails with "Query result transform must return rows".

---

## Query Functions

Predicates and bindings are plain Clojure/ClojureScript functions evaluated by DataScript.

**String functions:**

```clojure
;; String contains
[(clojure.string/includes? ?title "meeting")]

;; String starts with
[(clojure.string/starts-with? ?title "TODO")]

;; String ends with
[(clojure.string/ends-with? ?title ".pdf")]

;; Case-insensitive match
[(clojure.string/lower-case ?title) ?lower]
[(clojure.string/includes? ?lower "important")]
```

**Comparison:**

```clojure
[(> ?date ?cutoff)]
[(>= ?date ?start)]
[(<= ?date ?end)]
[(not= ?a ?b)]
```

**Numeric:**

```clojure
[(>= ?score 70)]
[(+ ?a ?b) ?sum]
[(- ?end ?start) ?duration]
```

**Collection / existence:**

```clojure
[(contains? #{:a :b} ?ident)]
[(missing? $ ?b :logseq.property/deadline)]
[(get-else $ ?b :logseq.property/status "none") ?status]
```

`missing?` and `get-else` are DataScript built-ins; both are used by Logseq's own rules
(`rules.cljc:115-117`, `:167`).

> **There is no "today" function.** Dates come in through `:inputs` (see below), not from a
> `#inst` literal or a call to `js/Date` inside `:where`.

### Custom Query Functions

Logseq does **not** register extra Datalog functions. What it ships instead is a library of
**rules**, defined in `deps/db/src/logseq/db/frontend/rules.cljc`, listed in the next section.
`src/main/frontend/db/query_custom.cljs` is a thin dispatcher — it validates the query map and
forwards it to the worker; it defines no query functions.

---

## Query Rules

### Built-in Rules

Rules named in `:where` are detected and injected automatically, together with their dependencies —
you do **not** need to write `:in $ %` yourself
(`src/main/frontend/worker/handler/query.cljs:185-204`).

Signatures, verbatim from `deps/db/src/logseq/db/frontend/rules.cljc`:

| Rule | Signature | Line |
| --- | --- | --- |
| `parent` | `(parent ?p ?c)` — transitive; **parent first, child second** | `:7-12` |
| `class-extends` | `(class-extends ?p ?c)` — transitive tag inheritance | `:14-19` |
| `alias` | `(alias ?e2 ?e1)` — symmetric | `:21-25` |
| `self-ref` | `(self-ref ?b ?ref)` | `:27-29` |
| `has-ref` | `(has-ref ?b ?r)` — block's own refs, or an ancestor's | `:31-36` |
| `page-ref` | `(page-ref ?b ?ref)` — alias of `has-ref` | `:80-82` |
| `block-content` | `(block-content ?b ?query)` — substring of `:block/title` | `:84-87` |
| `page` | `(page ?b ?page-name)` — `?page-name` is the **lowercase** `:block/name` | `:89-92` |
| `between` | `(between ?b ?start ?end)` — `?b` is a **block**; matches its journal page's `:block/journal-day` | `:94-100` |
| `property` | `(property ?b ?prop ?val)` — self-contained but slow | `:247-264` |
| `has-property` | `(has-property ?b ?prop)` | `:178-185` |
| `ref-property` | `(ref-property ?b ?prop ?val)` | `:204-210` |
| `scalar-property` | `(scalar-property ?b ?prop ?val)` | `:188-194` |
| `tags` | `(tags ?b ?tags)` — `?tags` is a **set** of titles / idents / eids; follows `class-extends` | `:266-295` |
| `task` | `(task ?b ?statuses)` — `?statuses` is a set of status **titles** | `:297-300` |
| `priority` | `(priority ?b ?priorities)` — set of priority **titles** | `:302-305` |

Two of these are commonly mis-remembered:

- `parent` takes the **ancestor first**: `(parent ?ancestor ?descendant)`.
- `between` constrains a **block** by the journal day of its page — it is not a generic date-range
  predicate. For a date range on a property value, compare the value directly.

`task` and `priority` compare against the closed value's *title* ("Todo", "Doing", "High", …),
because they go through `ref-property-with-default` → `ref->val` → `:block/title`
(`rules.cljc:102-106`, `:137-142`, `:297-305`).

### Using a Built-in Rule

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?statuses
         :where
         (task ?b ?statuses)]
 :inputs [#{"Todo" "Doing"}]}
```

### Recursive Rules

Descendants of a block, using the built-in `parent`:

```clojure
{:query [:find (pull ?child [:block/uuid])
         :in $ ?root-uuid
         :where
         [?root :block/uuid ?root-uuid]
         (parent ?root ?child)]
 :inputs [:current-block]}
```

> `:current-block` resolves to the **`:db/id`** of the block holding the query
> (`deps/db/src/logseq/db/frontend/inputs.cljs:92-94`), so bind it directly rather than through
> `:block/uuid`:
>
> ```clojure
> :in $ ?root
> :where (parent ?root ?child)
> ```

### User-defined Rules

Supply your own in `:rules`; they are concatenated ahead of the built-ins
(`worker/handler/query.cljs:194`).

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         (open-task ?b)]
 :rules [[(open-task ?b)
          [?b :block/tags :logseq.class/Task]
          [?b :logseq.property/status ?s]
          [?s :db/ident ?ident]
          [(contains? #{:logseq.property/status.todo
                        :logseq.property/status.doing} ?ident)]]]}
```

---

## Query Inputs

`:inputs` entries are resolved by
`deps/db/src/logseq/db/frontend/inputs.cljs` (`keyword-input-dispatch`, `:69-81`). Getting the
**unit** right matters: some inputs are `YYYYMMDD` integers and some are epoch milliseconds.

| Input | Resolves to | Source |
| --- | --- | --- |
| `:today`, `:yesterday`, `:tomorrow` | `YYYYMMDD` **integer** | `:100-107` |
| `:right-now-ms` | epoch **ms** | `:109-110` |
| `:start-of-today-ms`, `:end-of-today-ms` | epoch **ms** | `:113-118` |
| `today-start`, `today-end`, `today-HHMM`… | epoch **ms** | `:78`, `:113-118` |
| `:+7d`, `:-1w`, `:+1m`, `:-2y` | `YYYYMMDD` **integer** | `:73`, `:121-125` |
| `:+7d-ms`, `:-7d-start`, `:+1w-end`, `:-1d-0930` | epoch **ms** | `:74`, `:128-133` |
| `:current-page` | lowercase page name of the current page | `:84-86` |
| `:query-page` | `:block/name` of the page containing the query block | `:88-90` |
| `:current-block` | `:db/id` of the query block | `:92-94` |
| `:parent-block` | `:db/id` of the query block's parent | `:96-98` |
| `"[[Some Page]]"` | the lowercase page name `"some page"` | `:157-159` |
| anything else | passed through unchanged | `:161-162` |

**Deprecated forms.** `:7d`, `:7d-before`, `:7d-after`, `:7d-before-ms`, `:7d-after-ms` still resolve
but are explicitly marked deprecated and rewritten to the `+`/`-` form
(`inputs.cljs:80`, `:135-138`). Prefer `:-7d` / `:+7d`. Note that **`:7d-ago` is not a recognised
input at all** — it matches no dispatch branch and is silently passed through as the raw keyword.

**Unit traps.**

- `:logseq.property/deadline` and `:logseq.property/scheduled` are `:datetime` properties
  (`property.cljs:340-353`), i.e. **epoch ms**. Comparing them to `:today` (a `YYYYMMDD` integer)
  silently matches nothing. Use `:start-of-today-ms` / `:end-of-today-ms` / `:+7d-ms`.
- `:block/journal-day` (`schema.cljs:96`) is `YYYYMMDD`, so it pairs with `:today` / `:-7d`.
- `:block/created-at` and `:block/updated-at` (`schema.cljs:85-86`) are epoch ms.

An unresolvable input is returned unchanged rather than erroring (`inputs.cljs:150-155`), so a typo
in an input keyword shows up as an empty result, not a message.

---

## Performance Optimization

### Indexed Attributes

`deps/db/src/logseq/db/frontend/schema.cljs:56-108` is the whole DB-graph schema. These attributes
carry `:db/index true`:

`:block/parent` (`:62-63`), `:block/order` (`:64`), `:block/page` (`:68`), `:block/link` (`:77`),
`:block/alias` (`:81`), `:block/created-at` (`:85`), `:block/updated-at` (`:86`),
`:block/name` (`:90`), `:block/title` (`:93`), `:block/journal-day` (`:96`).

These are `:db.unique/identity`: `:db/ident` (`:58`), `:block/uuid` (`:61`), `:file/path` (`:104`).

Everything else — including user property attributes, which are created at runtime — is not in the
static schema.

### Query Optimization Tips

**1. Start with the most restrictive clause**

```clojure
;; ❌ Slow: broad condition first
[?b :block/title ?title]
[?b :block/uuid #uuid "…"]

;; ✅ Fast: narrow condition first
[?b :block/uuid #uuid "…"]
[?b :block/title ?title]
```

**2. Match idents, not titles**

```clojure
;; ❌ String comparison against every tag entity
[?b :block/tags ?t]
[?t :block/title "Task"]

;; ✅ Unique-identity lookup
[?b :block/tags :logseq.class/Task]
```

**3. Bound the result set inside the query**

There is no `:limit` key, and `:limit` inside `:find` throws a Query Error. Constrain in `:where`
(a date floor, a tag, a page) and, if you still need a hard cap, apply `(take n …)` in
`:result-transform` or `:view`.

**4. Keep pull selectors small**

Only `:block/uuid` survives result normalisation for block queries, so `(pull ?b [*])` buys you
nothing at render time:

```clojure
;; ✅ enough for rendering
(pull ?b [:block/uuid])
```

Pull extra attributes only when `:result-transform` needs them — it runs before normalisation and
can read the full selector.

---

## Query Patterns

### Task Management

**Overdue tasks:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start-of-today
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline ?deadline]
         [(< ?deadline ?start-of-today)]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(not= ?status :logseq.property/status.done)]
         [(not= ?status :logseq.property/status.canceled)]]
 :inputs [:start-of-today-ms]}
```

**Tasks grouped by tag:**

```clojure
{:query [:find ?tag-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :block/tags ?tag]
         [?tag :block/title ?tag-title]
         [(not= ?tag-title "Task")]]}
```

### Knowledge Management

**Recently modified pages:**

```clojure
{:query [:find (pull ?p [:block/uuid]) ?time
         :in $ ?cutoff
         :where
         [?p :block/tags :logseq.class/Page]
         [?p :block/updated-at ?time]
         [(> ?time ?cutoff)]]
 :inputs [:-7d-ms]
 :view (fn [rows]
         [:div
          (for [[uuid time] (take 20 (sort-by second > rows))]
            [:div (str uuid " — " time)])])}
```

**Orphaned pages (no references):**

```clojure
{:query [:find (pull ?p [:block/uuid])
         :where
         [?p :block/tags :logseq.class/Page]
         (not-join [?p] [_ :block/refs ?p])]}
```

### Flashcards

Card scheduling uses FSRS properties, not a "next review date" property
(`deps/db/src/logseq/db/frontend/property.cljs:610-614`). `:logseq.property.fsrs/due` is a
`:datetime`, so it is compared in **epoch ms** — this is exactly how Logseq's own card-scheduling
query does it (`src/main/frontend/worker/handler/flashcard.cljs:13`, `:24-33`):

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?end-of-today
         :where
         [?b :block/tags :logseq.class/Card]
         [?b :logseq.property.fsrs/due ?due]
         [(<= ?due ?end-of-today)]]
 :inputs [:end-of-today-ms]}
```

---

## Custom Views

`:view` is evaluated with SCI (`src/main/frontend/components/query.cljs:148-149`, using
`src/main/frontend/extensions/sci.cljs:50-58`) and must return hiccup. Three constraints:

- **`:style` must be a map**, not a string. `{:style {:color "blue"}}` renders; `{:style "color:blue"}`
  silently blanks the element.
- The `:view` SCI environment silently drops `map-indexed`, `#()` reader literals and
  keyword-as-function. Use plain `(fn [x] …)`, `(for [x xs] …)` and `(get m :k)`. This restriction is
  specific to `:view`; `:result-transform` is evaluated by a separate SCI call in the worker, and
  Logseq's own test suite passes `'(partial sort-by :block/title)` there
  (`src/test/frontend/components/query/result_test.cljs:12-14`).
- `(for …)` sequences **do** render as hiccup children — no `into`/`concat` wrapper is needed.

`eval-string` catches its own errors and returns `nil` (`extensions/sci.cljs:55-58`), so a `:view`
that fails to compile does not raise — the query silently falls back to the default renderer. If a
`:view` "does nothing", suspect a compile error first.

Reusable views can live in `config.edn` under `:query/views` and be referenced by keyword
(`components/query.cljs:148`); the same applies to `:query/result-transforms`
(`components/query.cljs:133-140`).

---

## Next Steps

**Continue Learning:**

1. **[Advanced Queries How-To](../../how-to/queries/advanced-queries.md)** - Practical query recipes
2. **[Schema System](../../explanation/architecture/schema-system.md)** - Understand data structure
3. **[Learn Datalog Today](http://www.learndatalogtoday.org/)** - Interactive Datalog tutorial
4. **[DataScript Documentation](https://github.com/tonsky/datascript)** - Full Datalog reference

**For Contributors:**

- Result rendering and normalisation — `src/main/frontend/worker/handler/render_resource/query.cljs`
- Datalog execution and input resolution — `src/main/frontend/worker/handler/query.cljs`
- Built-in rules — `deps/db/src/logseq/db/frontend/rules.cljc`
- Query inputs — `deps/db/src/logseq/db/frontend/inputs.cljs`
- Simple-query DSL — `src/main/frontend/worker/query_dsl.cljs`

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
