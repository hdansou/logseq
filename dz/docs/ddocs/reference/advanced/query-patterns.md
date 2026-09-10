---
title: 'Query Patterns Reference'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
quadrant: 'Reference'
platform: 'Desktop + Mobile (Database graphs only)'
related-docs:
  - '[Query System Reference](query-system.md)'
  - '[Advanced Queries Tutorial](../../tutorials/advanced/advanced-queries-tutorial.md)'
  - '[Task Management](../../how-to/workflows/task-management-workflows.md)'
tags: ['queries', 'datalog', 'advanced', 'reference', 'patterns']
---

# Query Patterns Reference

Reference of common query patterns for Logseq **database (DB) graphs**. Copy a pattern, adapt it,
test it. Both simple (DSL) and advanced (Datalog) forms are covered.

**Quick Start:** Copy a pattern → Modify for your use case → Test → Refine

> **DB graphs only.** These patterns will not work in a file graph, and file-graph patterns will not
> work here. In particular: `#+BEGIN_QUERY … #+END_QUERY` is file-graph markup that a DB graph never
> parses, and `:block/marker`, `:block/content`, `:block/properties`, `:block/journal?`,
> `:block/priority`, `:block/scheduled`, `:block/deadline` and `:block/namespace` are absent from the
> DB-graph schema (`deps/db/src/logseq/db/frontend/schema.cljs:56-108`). Blocks carry
> `:block/title`, tasks carry `:logseq.property/*` properties, and pages are identified by their
> tags.

---

## Table of Contents

1. [Query Basics](#query-basics)
2. [Task Queries](#task-queries)
3. [Page & Block Queries](#page--block-queries)
4. [Date & Time Queries](#date--time-queries)
5. [Property Queries](#property-queries)
6. [Tag Queries](#tag-queries)
7. [Reference Queries](#reference-queries)
8. [Advanced Patterns](#advanced-patterns)
9. [Query Optimization](#query-optimization)
10. [Query Templates](#query-templates)
11. [Common Input Values](#common-input-values)

---

## Query Basics

### Writing a Query

Use the **`/Advanced Query`** slash command. It tags the block with `:logseq.class/Query` and gives
you a Clojure editor for the `:logseq.property/query` property
(`src/main/frontend/commands.cljs:125-132`). What you type is read as EDN: a **map** is an advanced
Datalog query, anything else is a simple DSL query string
(`src/main/frontend/components/block.cljs:4090-4103`).

**Advanced query template:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?input
         :where
         [?b :block/tags :logseq.class/Task]]
 :inputs [:today]
 :collapsed? false}
```

**Components that actually do something:**

```clojure
:query                   → the Datalog query (vector starting with :find) — required
:inputs                  → values bound to the :in parameters
:rules                   → your own Datalog rules
:result-transform        → a form, run in the worker on the raw rows
:view                    → a (fn [rows] …) form returning hiccup
:collapsed?              → initial collapsed state
:remove-block-children?  → false keeps nested result blocks
```

`:title`, `:breadcrumb-show?` and `:group-by-page?` are **silently ignored** for user queries in DB
graphs, and there is no `:limit` key. See
[Query System Reference → Query Map Keys](query-system.md#query-map-keys) for the citations.

### Simple (DSL) Query

A simple query is a bare expression, not a map. The operators recognised by the DB-graph DSL are
`and`, `or`, `not`, `between`, `property`, `private-property`, `task` (alias `todo`), `priority`,
`page`, `sample` and `tags`, plus bare page references, quoted full-text strings and raw Datalog
clauses (`src/main/frontend/worker/query_dsl.cljs:526-577`).

```clojure
(and (task Todo Doing) (priority High))
```

```clojure
(property Status "Done")
```

Status and priority arguments are capitalised before matching, so `(task todo doing)` and
`(task TODO DOING)` both resolve to the titles `"Todo"` and `"Doing"`
(`query_dsl.cljs:411-419`, `:421-429`).

---

## Task Queries

Tasks are blocks tagged `:logseq.class/Task`. The Task class carries exactly four properties —
`:logseq.property/status`, `:logseq.property/priority`, `:logseq.property/deadline`,
`:logseq.property/scheduled` (`deps/db/src/logseq/db/frontend/class.cljs:42`).

**Status and priority are refs.** They are `:default`-typed properties with closed values
(`deps/db/src/logseq/db/frontend/property.cljs:300-338`), so the stored value is a reference to a
closed-value entity. Match its `:db/ident` (or its `:block/title`), never a string.

| Status ident | Title |
| --- | --- |
| `:logseq.property/status.backlog` | Backlog |
| `:logseq.property/status.todo` | Todo |
| `:logseq.property/status.doing` | Doing |
| `:logseq.property/status.in-review` | In Review |
| `:logseq.property/status.done` | Done |
| `:logseq.property/status.canceled` | Canceled |

| Priority ident | Title |
| --- | --- |
| `:logseq.property/priority.low` | Low |
| `:logseq.property/priority.medium` | Medium |
| `:logseq.property/priority.high` | High |
| `:logseq.property/priority.urgent` | Urgent |

### All Tasks by Status

**All Todo:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.todo]]}
```

**All Doing:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.doing]]}
```

**Done on a journal page in the last 7 days:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.done]
         [?b :block/page ?p]
         [?p :block/tags :logseq.class/Journal]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]]
 :inputs [:-7d]}
```

`:block/journal-day` is a `YYYYMMDD` integer (`schema.cljs:96`), which is why the input is `:-7d`
(a `YYYYMMDD` integer) and not `:-7d-ms`.

### Tasks with Dates

`:logseq.property/deadline` and `:logseq.property/scheduled` are `:datetime` properties
(`property.cljs:340-353`), validated as plain numbers
(`deps/db/src/logseq/db/frontend/property/type.cljs:210-212`) — **epoch milliseconds**. Compare them
against millisecond inputs, never against `:today`.

**Due today:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(not= ?status :logseq.property/status.done)]]
 :inputs [:start-of-today-ms :end-of-today-ms]}
```

**Due this week:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:start-of-today-ms :+7d-ms]}
```

**Overdue:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?now
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline ?d]
         [(< ?d ?now)]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(not= ?status :logseq.property/status.done)]
         [(not= ?status :logseq.property/status.canceled)]]
 :inputs [:start-of-today-ms]}
```

**Scheduled today or earlier:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?end
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/scheduled ?d]
         [(<= ?d ?end)]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.todo]]
 :inputs [:end-of-today-ms]}
```

### Tasks by Priority

**High priority, still open:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/priority ?p]
         [?p :db/ident :logseq.property/priority.high]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]]}
```

**All priorities, sorted:**

Sorting happens outside the Datalog query — DataScript has no `:order-by` clause.

```clojure
{:query [:find (pull ?b [:block/uuid :logseq.property/priority])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/priority _]]
 :result-transform (fn [result]
                     (sort-by (fn [b]
                                (get-in b [:logseq.property/priority :db/id]))
                              result))}
```

`:result-transform` runs in the worker **before** rows are normalised
(`src/main/frontend/worker/handler/render_resource/query.cljs:195-202`), so it still sees the pulled
maps.

### Combined Task Views

**Today's agenda (deadline or scheduled falling today):**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]
         (or-join [?b ?start ?end]
                  (and [?b :logseq.property/scheduled ?d1]
                       [(>= ?d1 ?start)]
                       [(<= ?d1 ?end)])
                  (and [?b :logseq.property/deadline ?d2]
                       [(>= ?d2 ?start)]
                       [(<= ?d2 ?end)]))]
 :inputs [:start-of-today-ms :end-of-today-ms]
 :collapsed? false}
```

`or-join` with nested `and` branches is used by Logseq's own card-scheduling query
(`src/main/frontend/worker/handler/flashcard.cljs:24-33`).

**All active tasks:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing
                       :logseq.property/status.in-review} ?status)]]}
```

---

## Page & Block Queries

There is no `:block/type`. A node is a page when it carries one of the page-ish tags —
`:logseq.class/Page`, `:logseq.class/Journal`, `:logseq.class/Tag`, `:logseq.class/Property`
(`deps/db/src/logseq/db/frontend/entity_util.cljs:24-50`).

### Page Queries

**Pages tagged `#project`:**

```clojure
{:query [:find (pull ?p [:block/uuid])
         :in $ ?tag-title
         :where
         [?p :block/tags :logseq.class/Page]
         [?p :block/tags ?t]
         [?t :block/title ?tag-title]]
 :inputs ["project"]}
```

Tags are real refs on `:block/tags` (`schema.cljs:73-74`), so no `:block/refs` detour is needed.

**Pages whose title starts with a prefix:**

```clojure
{:query [:find (pull ?p [:block/uuid])
         :where
         [?p :block/tags :logseq.class/Page]
         [?p :block/title ?title]
         [(clojure.string/starts-with? ?title "Projects/")]]}
```

DB graphs have no `:block/namespace` attribute; class hierarchies use
`:logseq.property.class/extends` and the `class-extends` rule
(`deps/db/src/logseq/db/frontend/rules.cljc:14-19`).

**Recently modified pages:**

```clojure
{:query [:find (pull ?p [:block/uuid]) ?t
         :in $ ?since
         :where
         [?p :block/tags :logseq.class/Page]
         [?p :block/updated-at ?t]
         [(> ?t ?since)]]
 :inputs [:-7d-ms]
 :view (fn [rows]
         [:div
          (for [[uuid _t] (take 20 (sort-by second > rows))]
            [:div (str uuid)])])}
```

`:block/updated-at` is epoch ms (`schema.cljs:85-86`), so pair it with a `-ms` input.

### Block Queries

**Blocks containing a word:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/title ?c]
         [(clojure.string/includes? ?c "important")]]}
```

This is the same predicate Logseq's own `block-content` rule uses (`rules.cljc:84-87`).

**Top-level blocks of a page:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?page-title
         :where
         [?p :block/title ?page-title]
         [?p :block/tags :logseq.class/Page]
         [?b :block/parent ?p]]
 :inputs ["Project Alpha"]}
```

A page's top-level blocks have the page itself as `:block/parent`
(`deps/db/src/logseq/db/sqlite/build.cljs:249`).

**Long blocks:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/title ?c]
         [(count ?c) ?len]
         [(> ?len 500)]]}
```

---

## Date & Time Queries

Journal pages are tagged `:logseq.class/Journal` and carry `:block/journal-day`, a `YYYYMMDD`
integer (`schema.cljs:96`; the built-in `between` rule uses exactly this shape,
`rules.cljc:94-100`).

### Journal Queries

**Last 7 days:**

```clojure
{:query [:find (pull ?p [:block/uuid])
         :in $ ?start ?end
         :where
         [?p :block/tags :logseq.class/Journal]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:-7d :today]}
```

**A fixed date range:**

```clojure
{:query [:find (pull ?p [:block/uuid])
         :where
         [?p :block/tags :logseq.class/Journal]
         [?p :block/journal-day ?d]
         [(>= ?d 20260101)]
         [(<= ?d 20260131)]]}
```

**Blocks written on journal pages in a range** — this is what the built-in `between` rule is for
(`rules.cljc:94-100`). The rule is injected automatically when its name appears in `:where`
(`src/main/frontend/worker/handler/query.cljs:185-204`):

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         (between ?b ?start ?end)]
 :inputs [:-7d :today]}
```

> There are no weekday inputs. `:monday`, `:sunday` and friends match no dispatch branch in
> `deps/db/src/logseq/db/frontend/inputs.cljs:69-81` and are passed through unresolved, which
> silently yields no results. Use `:-7d` / `:+7d` offsets, or literal `YYYYMMDD` integers.

### Created/Updated Queries

`:block/created-at` and `:block/updated-at` are epoch ms (`schema.cljs:85-86`).

**Created today:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         [?b :block/created-at ?t]
         [(>= ?t ?start)]
         [(<= ?t ?end)]]
 :inputs [:start-of-today-ms :end-of-today-ms]}
```

**Updated in the last 7 days:**

```clojure
{:query [:find (pull ?b [:block/uuid]) ?t
         :in $ ?start
         :where
         [?b :block/updated-at ?t]
         [(> ?t ?start)]]
 :inputs [:-7d-ms]
 :view (fn [rows]
         [:div
          (for [[uuid _t] (sort-by second > rows)]
            [:div (str uuid)])])}
```

---

## Property Queries

In a DB graph a property is a real DataScript attribute on the block, so you match it directly
rather than digging into a map.

`:block/properties` is **not** a DataScript attribute, so `[?b :block/properties ?p]` in a
`:where` clause matches nothing. It does still exist as a *computed entity lookup*:
`lookup-kv-then-entity` intercepts the key and `get-properties` builds a `{property-ident value}`
map from the entity's own property attributes
(`deps/db/src/logseq/db/common/entity_plus.cljc:138-146`, dispatched at `:162-163`). So
`(:block/properties e)` works on an entity — inside `:result-transform`, which receives entities —
but never as a query clause.

**User property idents are not guessable.** When you create a property named "Status", Logseq mints
an ident in the `user.property` namespace **with a random 8-character suffix** — one non-digit
character followed by a 7-character nano-id — e.g. `:user.property/Status-a1b2c3d4`
(`deps/db/src/logseq/db/frontend/property.cljs:852-859`,
`deps/db/src/logseq/db/frontend/db_ident.cljc:66-90`). The source says so itself: "Using this in
read-only contexts like querying can result in db-ident conflicts." Two consequences:

1. Never hard-code a user property ident you have not read out of your own graph.
2. Resolve the property by **title** instead. This is exactly what the simple-query DSL does
   (`src/main/frontend/worker/query_dsl.cljs:343-356`).

Two exceptions, both in `create-db-ident-from-name`: properties created under a
`plugin.class.*` namespace get **no** suffix, so plugin idents are stable and safe to hard-code;
and setting `LOGSEQ_STABLE_IDENTS` suppresses the suffix, which is how the test suite gets
reproducible idents.

### Resolve a Property by Title

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?prop-title
         :where
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         [?prop :db/ident ?ident]
         [?b ?ident _]]
 :inputs ["Status"]}
```

### Property Equals a Value

The built-in `ref-property-value` rule handles the "hop to the value entity and read its text" step
for you; it reads `:block/title` for `:default` values and `:logseq.property/value` for `:number`
values (`rules.cljc:102-106`, `:131-135`).

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?prop-title ?value
         :where
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         (ref-property-value ?b ?prop ?value)]
 :inputs ["Status" "Active"]}
```

For a non-ref property (`:checkbox`, `:datetime`, and the internal scalar types) use
`scalar-property-value` instead (`rules.cljc:119-122`).

### Number Properties Are Refs

`:number` is a ref type whose value lives on the referenced entity under
`:logseq.property/value` (`deps/db/src/logseq/db/frontend/property/type.cljs:52-61`). Dereference
before comparing:

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?prop-title
         :where
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         [?prop :db/ident ?ident]
         [?b ?ident ?pv]
         [?pv :logseq.property/value ?n]
         [(>= ?n 30)]]
 :inputs ["Estimate"]}
```

Comparing `?pv` directly against a number never matches — `?pv` is an entity, not a value.

### Property Links to a Page

`:node`-typed properties (such as `:logseq.property/assignee`, `property.cljs:401-407`) store a
reference to the target node:

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?person
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/assignee ?p]
         [?p :block/title ?person]]
 :inputs ["Sarah"]}
```

### Property Aggregations

Multi-column rows keep their tuple shape, so a `:view` destructures them directly. Do **not** call
`(partition 2 rows)` — that was needed by an older renderer and is now wrong
(`render_resource/query.cljs:40-46`).

```clojure
{:query [:find ?status-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :block/title ?status-title]]
 :view (fn [rows]
         [:table
          [:thead [:tr [:th "Status"] [:th "Count"]]]
          [:tbody
           (for [[status n] (sort-by second > rows)]
             [:tr [:td status] [:td n]])]])}
```

---

## Tag Queries

`:block/tags` is a cardinality-many ref (`schema.cljs:73-74`). A built-in tag can be matched by its
ident directly; a user tag is matched through its title.

### Basic Tag Queries

**Blocks tagged with a given tag:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?tag
         :where
         [?b :block/tags ?t]
         [?t :block/title ?tag]]
 :inputs ["important"]}
```

**Two tags (AND):**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags ?t1]
         [?t1 :block/title "project"]
         [?b :block/tags ?t2]
         [?t2 :block/title "urgent"]]}
```

**Either tag (OR):**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags ?t]
         [?t :block/title ?tag]
         [(contains? #{"project" "work"} ?tag)]]}
```

### Tag Inheritance

Plain `:block/tags` clauses match **direct** tags only. To include tags that extend the one you are
asking about, use the built-in `tags` rule, which walks `:logseq.property.class/extends`
(`rules.cljc:266-295`). Its second argument is a **set** of tag titles, idents or entity ids:

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?tags
         :where
         (tags ?b ?tags)]
 :inputs [#{"project"}]}
```

### Tag Combinations

**Tagged but not done:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags ?t]
         [?t :block/title "project"]
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(not= ?status :logseq.property/status.done)]]}
```

**Exclude a tag:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         (not-join [?b]
                   [?b :block/tags ?t]
                   [?t :block/title "archived"])]}
```

Use `not-join` whenever the negated clauses introduce their own variables; a bare `not` leaves them
dangling.

---

## Reference Queries

### Backlink Queries

**Blocks referencing a page, by title:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?title
         :where
         [?p :block/title ?title]
         [?b :block/refs ?p]]
 :inputs ["Project Alpha"]}
```

`:block/name` holds the **lowercase** page name (`schema.cljs:89-90`), so match on `:block/title`
unless you are deliberately passing a lowercased value. A `"[[Project Alpha]]"` input is resolved to
the lowercase page name `"project alpha"`
(`deps/db/src/logseq/db/frontend/inputs.cljs:157-159`), which pairs with `:block/name`.

**Blocks referencing a specific block:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?target-uuid
         :where
         [?target :block/uuid ?target-uuid]
         [?b :block/refs ?target]]
 :inputs [#uuid "00000000-0000-0000-0000-000000000000"]}
```

**References made anywhere under a block** — the built-in `has-ref` rule also matches refs on an
ancestor (`rules.cljc:31-36`):

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?title
         :where
         [?ref :block/title ?title]
         (has-ref ?b ?ref)]
 :inputs ["Project Alpha"]}
```

### Connection Queries

**Mutually linked pages:**

```clojure
{:query [:find ?t1 ?t2
         :where
         [?b1 :block/page ?p1]
         [?b1 :block/refs ?p2]
         [?b2 :block/page ?p2]
         [?b2 :block/refs ?p1]
         [?p1 :block/title ?t1]
         [?p2 :block/title ?t2]]}
```

**Orphan pages (no backlinks):**

```clojure
{:query [:find (pull ?p [:block/uuid])
         :where
         [?p :block/tags :logseq.class/Page]
         (not-join [?p] [_ :block/refs ?p])]}
```

---

## Advanced Patterns

### Hierarchical Queries

Use the built-in `parent` rule — note the argument order is **ancestor first**
(`rules.cljc:7-12`):

```clojure
{:query [:find (pull ?child [:block/uuid])
         :in $ ?root
         :where
         (parent ?root ?child)]
 :inputs [:current-block]}
```

`:current-block` resolves to the `:db/id` of the block holding the query
(`deps/db/src/logseq/db/frontend/inputs.cljs:92-94`), so bind it straight into `?root`.

**Blocks that have at least one child:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [_ :block/parent ?b]]}
```

> Recursive pull selectors such as `{:block/_parent ...}` are pointless for rendering: block results
> are reduced to bare `:block/uuid` values before the UI sees them
> (`render_resource/query.cljs:15-46`). Pull extra structure only when `:result-transform` needs it.

### Text Search

**Case-insensitive:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/title ?c]
         [(clojure.string/lower-case ?c) ?lc]
         [(clojure.string/includes? ?lc "database")]]}
```

> **Unverified:** regular-expression predicates (`[(re-find #"…" ?c)]`) are DataScript built-ins,
> but no Logseq source or test exercises one inside a query `:where`, so this reference does not
> document them as supported. `clojure.string/includes?` is used by Logseq's own `block-content`
> rule (`rules.cljc:84-87`) and is the safe choice.

### Statistical Queries

**Count tasks by status:**

```clojure
{:query [:find ?status-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :block/title ?status-title]]
 :view (fn [rows]
         [:div
          [:h3 "Task count by status"]
          [:table
           [:thead [:tr [:th "Status"] [:th "Count"]]]
           [:tbody
            (for [[status n] (sort-by second > rows)]
              [:tr [:td status] [:td n]])]]])}
```

A numeric first `:find` column is safe — the old heuristic that mis-read numeric first columns as
page groupings is gone.

**Sum a number property:**

```clojure
{:query [:find (sum ?n)
         :in $ ?prop-title
         :where
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         [?prop :db/ident ?ident]
         [?b ?ident ?pv]
         [?pv :logseq.property/value ?n]]
 :inputs ["Estimate"]}
```

### Custom Sorting

```clojure
{:query [:find (pull ?b [:block/uuid :logseq.property/deadline])
         :where
         [?b :block/tags :logseq.class/Task]]
 :result-transform (fn [result]
                     (sort-by (fn [b]
                                (get b :logseq.property/deadline 99999999999999))
                              result))}
```

---

## Query Optimization

### Filter on the Narrowest Clause First

```clojure
;; ❌ Slow: scans every block that has a title
[?b :block/title ?c]
[?b :block/tags :logseq.class/Task]

;; ✅ Fast: start from the tag
[?b :block/tags :logseq.class/Task]
[?b :block/title ?c]
```

### Match Idents, Not Titles

`:db/ident` is `:db.unique/identity` (`schema.cljs:58`), so an ident lookup is a direct hit:

```clojure
;; ✅
[?b :block/tags :logseq.class/Task]

;; ❌ compares a string against every tag entity
[?b :block/tags ?t]
[?t :block/title "Task"]
```

### Bound the Result Set

There is no `:limit` key, and `:limit` inside `:find` throws a Query Error. Constrain in `:where`
(date floor, tag, page), then cap with `(take n …)` in `:result-transform` or `:view`.

```clojure
 :result-transform (fn [result] (take 50 result))
```

### Indexed Attributes

`deps/db/src/logseq/db/frontend/schema.cljs:56-108` declares `:db/index true` on:

`:block/parent`, `:block/order`, `:block/page`, `:block/link`, `:block/alias`, `:block/created-at`,
`:block/updated-at`, `:block/name`, `:block/title`, `:block/journal-day`.

`:db/ident`, `:block/uuid` and `:file/path` are `:db.unique/identity`. Substring and
lower-case predicates over `:block/title` are full scans — apply them last.

### Debugging Queries

**Inspect the raw values a clause produces.** Bind instead of pulling, and the renderer prints the
values as a plain list:

```clojure
{:query [:find ?status-title
         :where
         [?b :logseq.property/status ?s]
         [?s :block/title ?status-title]]}
```

**Confirm a clause matches anything at all:**

```clojure
{:query [:find (count ?b)
         :where [?b :block/tags :logseq.class/Task]]}
```

**Empty result with no error?** Check the usual suspects in order: an input keyword that resolves to
nothing (an unrecognised input is returned unchanged rather than erroring —
`deps/db/src/logseq/db/frontend/inputs.cljs:150-155`), a `YYYYMMDD` value compared against an
epoch-ms property, a ref-valued property compared against a string, or a `:view` that failed to
compile (SCI errors are swallowed — `src/main/frontend/extensions/sci.cljs:55-58`).

---

## Query Templates

**Page query template:**

```clojure
{:query [:find (pull ?p [:block/uuid])
         :in $ ?input
         :where
         [?p :block/tags :logseq.class/Page]
         ;; Add conditions here
         ]
 :inputs ["CHANGE_ME"]}
```

**Block query template:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?input
         :where
         [?b :block/title ?title]
         ;; Add conditions here
         ]
 :inputs ["CHANGE_ME"]
 :collapsed? false}
```

**Task query template:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.todo]
         ;; Add conditions here
         ]}
```

---

## Common Input Values

Resolved by `deps/db/src/logseq/db/frontend/inputs.cljs` (`keyword-input-dispatch`, `:69-81`).
The unit matters: some inputs are `YYYYMMDD` integers, others are epoch milliseconds.

```clojure
:today               → today as a YYYYMMDD integer          (inputs.cljs:100-101)
:yesterday           → yesterday, YYYYMMDD                  (inputs.cljs:103-104)
:tomorrow            → tomorrow, YYYYMMDD                   (inputs.cljs:106-107)
:-7d  :+7d  :-1w :+1m :-2y
                     → offset date, YYYYMMDD                (inputs.cljs:73, 121-125)
:-7d-ms :+7d-ms :-1d-start :+1d-end
                     → offset date, epoch ms                (inputs.cljs:74, 128-133)
:start-of-today-ms   → today 00:00:00.000, epoch ms         (inputs.cljs:113-118)
:end-of-today-ms     → today 23:59:59.999, epoch ms         (inputs.cljs:113-118)
:right-now-ms        → now, epoch ms                        (inputs.cljs:109-110)
:current-page        → lowercase title of the current page  (inputs.cljs:84-86)
:query-page          → :block/name of the query's page      (inputs.cljs:88-90)
:current-block       → :db/id of the query block            (inputs.cljs:92-94)
:parent-block        → :db/id of the query block's parent   (inputs.cljs:96-98)
"[[Some Page]]"      → "some page" (lowercase page name)    (inputs.cljs:157-159)
```

**Deprecated:** `:7d`, `:7d-before`, `:7d-after`, `:7d-before-ms`, `:7d-after-ms` still resolve but
are explicitly marked deprecated and rewritten internally (`inputs.cljs:80`, `:135-138`). Prefer
`:-7d` / `:+7d`.

**Not inputs at all:** `:7d-ago`, `:monday`, `:sunday` and other weekday keywords match no dispatch
branch. They are returned unchanged (`inputs.cljs:150-155`), so queries using them silently return
nothing.

**Pairing rule:** `:block/journal-day` takes `YYYYMMDD` inputs; `:block/created-at`,
`:block/updated-at`, `:logseq.property/deadline`, `:logseq.property/scheduled` and
`:logseq.property.fsrs/due` all take epoch-ms inputs.

---

## Next Steps

**Master Queries:**

1. Start with simple patterns
2. Copy and modify templates
3. Test incrementally
4. Learn Datalog basics
5. Build custom views

**Related:**

- [Query System Reference](query-system.md)
- [Advanced Queries Tutorial](../../tutorials/advanced/advanced-queries-tutorial.md)
- [Task Management](../../how-to/workflows/task-management-workflows.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
