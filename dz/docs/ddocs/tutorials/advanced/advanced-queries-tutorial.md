---
title: 'Advanced Queries Tutorial'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
quadrant: 'Tutorial'
platform: 'Desktop + Mobile (Database graphs only)'
related-docs:
  - '[Query Patterns Reference](../../reference/advanced/query-patterns.md)'
  - '[Query System Reference](../../reference/advanced/query-system.md)'
  - '[Properties Reference](../../reference/data/properties-reference.md)'
tags: ['queries', 'datalog', 'tutorial', 'advanced']
---

# Advanced Queries Tutorial

Learn to write Datalog queries in a Logseq **database (DB) graph**. This hands-on tutorial takes you
from your first query to custom dashboards, one step at a time.

**What You'll Learn:**

- Datalog fundamentals
- How a query block is stored and what shape its results come back in
- Building conditions on tags, properties and dates
- Using inputs, transforms and rules
- Aggregation and custom views

**Prerequisites:**

- Basic Logseq usage
- Understanding of blocks, tags and properties
- A DB graph (this tutorial does not apply to file graphs)

**Time to Complete:** 90-120 minutes

> **DB graphs only.** If you have seen Logseq queries elsewhere that use `#+BEGIN_QUERY`,
> `:block/marker`, `:block/content` or `:block/properties`, those are file-graph constructs. None of
> them exist in a DB graph (`deps/db/src/logseq/db/frontend/schema.cljs:56-108`), and a DB graph
> never parses `#+BEGIN_QUERY`.

---

## Table of Contents

1. [Datalog Basics](#datalog-basics)
2. [Query Structure](#query-structure)
3. [Finding Blocks](#finding-blocks)
4. [Filtering with Where](#filtering-with-where)
5. [Working with Properties](#working-with-properties)
6. [Using Inputs](#using-inputs)
7. [Transforming Results](#transforming-results)
8. [Aggregation & Grouping](#aggregation--grouping)
9. [Custom Views](#custom-views)
10. [Advanced Patterns](#advanced-patterns)

---

## Datalog Basics

### What is Datalog?

**Datalog** is a declarative query language based on logic programming. Instead of telling the
computer _how_ to find data, you describe _what_ you want to find.

**Example Comparison:**

```javascript
// Imperative (how to find)
let tasks = []
for (let block of allBlocks) {
  if (block.tags.includes("Task")) {
    tasks.push(block)
  }
}
```

```clojure
;; Declarative (what to find)
[:find (pull ?b [:block/uuid])
 :where
 [?b :block/tags :logseq.class/Task]]
```

### Key Concepts

**1. Facts.** Everything in the graph is stored as `[entity attribute value]` triples:

```text
[block-123 :block/title "Write tutorial"]
[block-123 :block/tags   tag-77]
[block-123 :block/page   page-456]
[tag-77    :db/ident     :logseq.class/Task]
```

**2. Variables** start with `?`:

```text
?b     = any block
?p     = any page
?tag   = any tag entity

Variables get bound (assigned values) while the query runs.
```

**3. Patterns** match facts:

```clojure
[?b :block/tags :logseq.class/Task]
;; matches any block tagged Task

[?b :block/title ?title]
;; matches any block with a title, and binds it to ?title
```

### Where Queries Live

Run the **`/Advanced Query`** slash command. Logseq tags the block with `:logseq.class/Query`, adds
the `:logseq.property/query` property and opens a Clojure editor for it
(`src/main/frontend/commands.cljs:125-132`). What you type is read as EDN — if it reads as a **map**
it is treated as an advanced Datalog query (`src/main/frontend/components/block.cljs:4090-4103`).

### Your First Query

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]]}
```

**What This Does:**

```text
:find (pull ?b [:block/uuid])
→ return each matching block

:where
→ subject to this condition:

[?b :block/tags :logseq.class/Task]
→ the block is tagged Task
```

> **Always `pull`.** `[:find ?b …]` returns raw DataScript entity IDs — integers, which render as a
> plain list of numbers. Only pulled entities are turned into rendered blocks
> (`src/main/frontend/worker/handler/render_resource/query.cljs:161-169`).

---

## Query Structure

### Basic Template

```clojure
{:query [:find (pull ?b [:block/uuid])  ; what to return
         :in $ ?input                    ; optional: parameters
         :where                          ; conditions
         [pattern]
         [pattern]]
 :inputs [value]                         ; optional: values for :in
 :collapsed? false}                      ; optional: start expanded
```

Only a handful of map keys do anything. `:query`, `:inputs`, `:rules`, `:result-transform`,
`:view`, `:collapsed?` and `:remove-block-children?` are honoured. `:title`, `:breadcrumb-show?` and
`:group-by-page?` are silently ignored for user queries in DB graphs, and there is no `:limit` key —
see [Query System Reference → Query Map Keys](../../reference/advanced/query-system.md#query-map-keys).

Because `:title` does nothing, put your heading in the block *above* the query, or render it
yourself from a `:view`.

### The `:find` Clause

```clojure
;; Full block, rendered by Logseq
:find (pull ?b [:block/uuid])

;; Two columns — rows come back as tuples
:find ?title ?day

;; Aggregate
:find (count ?b)
```

### The `:where` Clause

Conditions are implicitly ANDed:

```clojure
:where
[?b :block/tags :logseq.class/Task]     ; AND
[?b :logseq.property/priority ?p]       ; AND
[?p :db/ident :logseq.property/priority.high]
```

### Understanding Result Shape

Before you go further, learn what your query actually hands back. The worker normalises every row
(`render_resource/query.cljs:15-46`, `:195-202`):

- A cell that is a pulled block becomes its bare **`:block/uuid`**.
- A **one-column** row is unwrapped to that single value.
- A row with **two or more** columns keeps its tuple shape.

So a single-column `pull` query gives you a flat sequence of UUIDs, and a two-column query gives you
`[[a b] [a b] …]`. You never need `(partition 2 rows)`.

---

## Finding Blocks

### Exercise 1: Find Blocks by Tag

**Task:** Find all blocks tagged `Task` whose status is Doing.

<details>
<summary>Try it yourself first, then click to see solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.doing]]}
```

**Why the extra hop?** `:logseq.property/status` is a `:default` property with closed values
(`deps/db/src/logseq/db/frontend/property.cljs:300-321`), which makes it a **reference**. The block
points at a status entity; that entity carries the `:db/ident`.

</details>

### Exercise 2: Match Several Statuses

**Task:** Find all Todo *or* Doing tasks.

**Hint:** Bind the ident, then use `contains?` with a set.

<details>
<summary>Solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]]}
```

**Explanation:**

- `[?s :db/ident ?status]` binds the status ident
- `[(contains? #{…} ?status)]` keeps only the two you want
- The six valid idents are `:logseq.property/status.backlog`, `.todo`, `.doing`, `.in-review`,
  `.done`, `.canceled` (`property.cljs:313-318`)

</details>

### Exercise 3: Find Blocks by Text

**Task:** Find blocks whose text contains "important".

**Hint:** Block text lives in `:block/title`, and `clojure.string/includes?` works as a predicate.

<details>
<summary>Solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/title ?title]
         [(clojure.string/includes? ?title "important")]]}
```

This is the same predicate Logseq's own `block-content` rule uses
(`deps/db/src/logseq/db/frontend/rules.cljc:84-87`).

</details>

---

## Filtering with Where

### Multiple Conditions (AND)

```clojure
:where
[?b :block/tags :logseq.class/Task]
[?b :logseq.property/priority ?p]
[?p :db/ident :logseq.property/priority.high]
```

### OR Logic

```clojure
;; with contains? on a bound ident (usually clearest)
[?b :logseq.property/priority ?p]
[?p :db/ident ?priority]
[(contains? #{:logseq.property/priority.high
              :logseq.property/priority.urgent} ?priority)]

;; with an or clause
(or [?p :db/ident :logseq.property/priority.high]
    [?p :db/ident :logseq.property/priority.urgent])
```

When each branch needs several clauses, use `or-join` and list the shared variables — this is what
Logseq's own card-scheduling query does
(`src/main/frontend/worker/handler/flashcard.cljs:24-33`):

```clojure
(or-join [?b ?now]
         (and [?b :logseq.property/deadline ?d]
              [(<= ?d ?now)])
         [(missing? $ ?b :logseq.property/deadline)])
```

### Negation (NOT)

```clojure
:where
[?b :block/tags :logseq.class/Task]
(not-join [?b]
          [?b :logseq.property/status ?s]
          [?s :db/ident :logseq.property/status.done])
```

Prefer `not-join` whenever the negated clauses introduce their own variables (`?s` above). A bare
`not` leaves them unbound outside the negation.

### Exercise 4: Complex Filtering

**Task:** Find tasks that

- have priority High or Urgent, and
- do **not** contain the word "someday"

<details>
<summary>Solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/priority ?p]
         [?p :db/ident ?priority]
         [(contains? #{:logseq.property/priority.high
                       :logseq.property/priority.urgent} ?priority)]
         [?b :block/title ?title]
         [(clojure.string/includes? ?title "someday") ?has-someday]
         [(= ?has-someday false)]]
 :collapsed? false}
```

Binding the function's result (`[(f …) ?v]`) and then comparing it keeps the negation in plain
value-land, which is easier to get right than nesting a function call inside `not`.

</details>

---

## Working with Properties

In a DB graph a property is a real DataScript attribute on the block. There is no
`:block/properties` map to `get` from.

### Built-in vs User Properties

**Built-in** properties have stable, documented idents you can type directly:
`:logseq.property/status`, `:logseq.property/priority`, `:logseq.property/deadline`,
`:logseq.property/scheduled`, `:logseq.property/assignee`, and so on.

**User** properties do not. When you create a property called "Status", Logseq mints an ident in the
`user.property` namespace **with a random suffix** — something like `:user.property/Status-a1b2c3d`
(`deps/db/src/logseq/db/frontend/property.cljs:852-859`,
`deps/db/src/logseq/db/frontend/db_ident.cljc:66-90`). Never hard-code one you have not read out of
your own graph.

### Resolving a User Property by Title

Look the property up the way the simple-query DSL does
(`src/main/frontend/worker/query_dsl.cljs:343-356`):

```clojure
:where
[?prop :block/tags :logseq.class/Property]
[?prop :block/title "Estimate"]
[?prop :db/ident ?ident]
[?b ?ident ?value]
```

### Property Values Are Often Refs

Which shape a value has depends on the property's type
(`deps/db/src/logseq/db/frontend/property/type.cljs:52-66`):

| Type | Stored as | Read the value from |
| --- | --- | --- |
| `:default` (incl. closed values) | ref | `:block/title` of the referenced entity |
| `:number` | ref | `:logseq.property/value` of the referenced entity |
| `:url` | ref | `:block/title`, else `:logseq.property/value` |
| `:node`, `:date`, `:asset` | ref | the referenced node itself |
| `:datetime` | plain number (epoch ms) | the attribute directly |
| `:checkbox` | plain boolean | the attribute directly |

Logseq ships a rule that hides this distinction. `ref-property-value` walks the ref and reads either
`:block/title` or `:logseq.property/value` (`rules.cljc:102-106`, `:131-135`). Named rules are
injected automatically when they appear in `:where`
(`src/main/frontend/worker/handler/query.cljs:185-204`), so you do not have to declare `:in $ %`:

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?prop-title ?value
         :where
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         (ref-property-value ?b ?prop ?value)]
 :inputs ["Status" "Active"]}
```

### Exercise 5: Number Properties

**Task:** Find blocks whose "Estimate" number property is 30 or less.

**Hint:** `:number` is a ref type. The number itself lives under `:logseq.property/value`.

<details>
<summary>Solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?prop-title
         :where
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         [?prop :db/ident ?ident]
         [?b ?ident ?pv]
         [?pv :logseq.property/value ?n]
         [(<= ?n 30)]]
 :inputs ["Estimate"]}
```

Comparing `?pv` directly against `30` matches nothing — `?pv` is an entity, not a number.

</details>

### Property That Points at a Node

`:node`-typed properties store a reference to the target page or block. `:logseq.property/assignee`
is the built-in example (`property.cljs:401-407`):

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?person
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/assignee ?p]
         [?p :block/title ?person]]
 :inputs ["Sarah"]}
```

---

## Using Inputs

### What Are Inputs?

**Inputs** parameterise a query. Each `:in` parameter after `$` is filled from `:inputs`, in order.

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?tag-title
         :where
         [?b :block/tags ?t]
         [?t :block/title ?tag-title]]
 :inputs ["project"]}
```

### Built-in Inputs

Resolved by `deps/db/src/logseq/db/frontend/inputs.cljs` (`keyword-input-dispatch`, `:69-81`).
The **unit** is the thing to get right:

```clojure
:today :yesterday :tomorrow   ; YYYYMMDD integer      (inputs.cljs:100-107)
:-7d :+7d :-1w :+1m :-2y      ; YYYYMMDD integer      (inputs.cljs:73, 121-125)
:-7d-ms :+7d-ms :+1d-end      ; epoch milliseconds    (inputs.cljs:74, 128-133)
:start-of-today-ms            ; epoch milliseconds    (inputs.cljs:113-118)
:end-of-today-ms              ; epoch milliseconds    (inputs.cljs:113-118)
:right-now-ms                 ; epoch milliseconds    (inputs.cljs:109-110)
:current-page                 ; lowercase page title  (inputs.cljs:84-86)
:query-page                   ; :block/name of the query's page (inputs.cljs:88-90)
:current-block                ; :db/id of the query block       (inputs.cljs:92-94)
:parent-block                 ; :db/id of its parent            (inputs.cljs:96-98)
```

Three warnings that will save you an afternoon:

1. **`:today` is a `YYYYMMDD` integer, not a timestamp.** `:logseq.property/deadline` and
   `:logseq.property/scheduled` are `:datetime` properties holding **epoch milliseconds**
   (`property.cljs:340-353`). Comparing one against the other silently matches nothing. Use
   `:start-of-today-ms` / `:end-of-today-ms` / `:+7d-ms` for those, and `:today` / `:-7d` only for
   `:block/journal-day`.
2. **`:7d-before` and friends are deprecated** — they still resolve but are rewritten internally
   (`inputs.cljs:80`, `:135-138`). Prefer `:-7d` / `:+7d`.
3. **There are no weekday inputs, and `:7d-ago` is not an input.** Anything that matches no dispatch
   branch is returned unchanged (`inputs.cljs:150-155`), so the query just returns nothing with no
   error.

### Date Queries with Inputs

**Deadline falls today:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:start-of-today-ms :end-of-today-ms]}
```

**Written on a journal page in the last week:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         [?b :block/page ?p]
         [?p :block/tags :logseq.class/Journal]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:-7d :today]}
```

### Exercise 6: Overdue Tasks

**Task:** Find open tasks whose deadline is before this morning.

**Hint:** `<` plus a millisecond input.

<details>
<summary>Solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start-of-today
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline ?d]
         [(< ?d ?start-of-today)]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(not= ?status :logseq.property/status.done)]
         [(not= ?status :logseq.property/status.canceled)]]
 :inputs [:start-of-today-ms]}
```

</details>

### Multiple Inputs

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?project ?person
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :block/refs ?proj]
         [?proj :block/title ?project]
         [?b :logseq.property/assignee ?p]
         [?p :block/title ?person]]
 :inputs ["Project Alpha" "Sarah"]}
```

---

## Transforming Results

### The `:result-transform` Key

`:result-transform` is a **form**, not a live function: Logseq `pr-str`s it and evaluates it with
SCI inside the worker (`src/main/frontend/components/query/result.cljs:46-47`,
`render_resource/query.cljs:181-192`). It runs **before** rows are normalised, so it still sees the
full pulled maps.

```clojure
{:query [:find (pull ?b [:block/uuid :block/updated-at])
         :where [?b :block/tags :logseq.class/Task]]
 :result-transform (fn [result]
                     (sort-by :block/updated-at > result))}
```

It must return a sequential collection or a set, otherwise the query fails with
"Query result transform must return rows" (`render_resource/query.cljs:186-191`).

### Sorting

```clojure
;; single key
:result-transform (fn [result]
                    (sort-by :block/updated-at result))

;; descending
:result-transform (fn [result]
                    (sort-by :block/updated-at > result))

;; several keys
:result-transform (fn [result]
                    (sort-by (fn [b]
                               [(get b :logseq.property/deadline 99999999999999)
                                (get b :block/updated-at 0)])
                             result))
```

### Filtering

```clojure
:result-transform (fn [result]
                    (filter (fn [b]
                              (< (count (str (:block/title b))) 100))
                            result))
```

Note that you must pull `:block/title` for this to see anything.

### Limiting

There is no `:limit` key, and `:limit` inside `:find` throws a Query Error. Cap here instead:

```clojure
:result-transform (fn [result] (take 10 result))
```

### Reusable Transforms

A keyword `:result-transform` is looked up in `:query/result-transforms` in `config.edn`
(`src/main/frontend/components/query.cljs:133-140`), so a transform you use often can be named once
and referenced everywhere.

### Exercise 7: Sort and Limit

**Task:** Find tasks with a deadline, earliest first, and keep only the first five.

<details>
<summary>Solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid :logseq.property/deadline])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/deadline _]]
 :result-transform (fn [result]
                     (take 5 (sort-by :logseq.property/deadline result)))}
```

Pull `:logseq.property/deadline` explicitly — the transform can only sort on what you pulled.

</details>

---

## Aggregation & Grouping

### Counting

**Count everything:**

```clojure
{:query [:find (count ?b)
         :where [?b :block/tags :logseq.class/Task]]}
```

**Count by group** — the grouping column comes first, the aggregate second:

```clojure
{:query [:find ?status-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :block/title ?status-title]]}
```

Two columns means rows keep their tuple shape, so `(for [[status n] rows] …)` destructures directly.
A **numeric first column is fine** — the old renderer heuristic that mistook numeric first columns
for page groupings is gone.

**Count by tag:**

```clojure
{:query [:find ?tag-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :block/tags ?tag]
         [?tag :block/title ?tag-title]
         [(not= ?tag-title "Task")]]}
```

### Other Aggregates

```clojure
;; sum / avg / min / max over a number property
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

Swap `sum` for `avg`, `min` or `max` as needed.

### Exercise 8: Tag Statistics

**Task:** Count open tasks per tag, largest first.

<details>
<summary>Solution</summary>

```clojure
{:query [:find ?tag-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]
         [?b :block/tags ?tag]
         [?tag :block/title ?tag-title]
         [(not= ?tag-title "Task")]]
 :result-transform (fn [rows] (sort-by second > rows))}
```

Here `:result-transform` receives plain tuples, because the rows are not single pulled blocks.

</details>

---

## Custom Views

### The `:view` Key

`:view` is a `(fn [rows] …)` form that returns hiccup. Logseq `pr-str`s it and evaluates it with SCI
on the main thread (`src/main/frontend/components/query.cljs:148-149`,
`src/main/frontend/extensions/sci.cljs:50-58`).

**What `rows` contains:** whatever the normaliser produced. A single-column `pull` query gives a flat
sequence of **UUIDs** — not block maps. A two-column query gives tuples.

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
           (for [[status n] rows]
             [:tr [:td status] [:td n]])]])}
```

### Three Rules for `:view`

1. **`:style` must be a map.** `{:style {:color "blue"}}` renders; `{:style "color:blue"}` silently
   blanks the element.
2. **The `:view` SCI environment silently drops `map-indexed`, `#()` reader literals and
   keyword-as-function.** Write plain `(fn [x] …)` and `(for [x xs] …)`, and reach into a map with
   `(get row :k)` rather than `(:k row)`. (This restriction is specific to `:view`;
   `:result-transform` runs through a different SCI evaluation in the worker, and Logseq's own test
   suite passes `'(partial sort-by :block/title)` there —
   `src/test/frontend/components/query/result_test.cljs:12-14`.)
3. **`(for …)` sequences do render as hiccup children.** No `into` or `concat` wrapper is needed.

If a `:view` fails to compile, SCI's error is caught and logged and the function comes back `nil`
(`extensions/sci.cljs:55-58`), so the query silently falls back to the default renderer. A `:view`
that "does nothing" is almost always a compile error.

### Reusable Views

A keyword `:view` is looked up in `:query/views` in `config.edn`
(`components/query.cljs:148`), so you can name a view once and reference it as `:view :my-table`.

### Exercise 9: Dashboard View

**Task:** Show task counts per priority in a table with a friendly label column.

<details>
<summary>Solution</summary>

```clojure
{:query [:find ?priority-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/priority ?p]
         [?p :block/title ?priority-title]]
 :view (fn [rows]
         [:table
          [:thead
           [:tr [:th "Priority"] [:th "Count"] [:th "Urgency"]]]
          [:tbody
           (for [[priority n] (sort-by first rows)]
             [:tr
              [:td [:strong priority]]
              [:td n]
              [:td (case priority
                     "Urgent" "🔴 Urgent"
                     "High"   "🟠 High"
                     "Medium" "🟡 Medium"
                     "Low"    "🟢 Low"
                     "—")]])]])}
```

The four priority titles are Low, Medium, High and Urgent (`property.cljs:334-337`).

</details>

---

## Advanced Patterns

### Hierarchical Queries

Logseq ships a transitive `parent` rule. Note the argument order: **ancestor first, descendant
second** (`rules.cljc:7-12`).

```clojure
{:query [:find (pull ?child [:block/uuid])
         :in $ ?root
         :where
         (parent ?root ?child)]
 :inputs [:current-block]}
```

`:current-block` resolves to the `:db/id` of the block holding the query
(`inputs.cljs:92-94`), so bind it straight into `?root`.

**Blocks that have children:**

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [_ :block/parent ?b]]}
```

> Recursive pull selectors like `{:block/_parent ...}` buy nothing at render time: block results are
> reduced to bare `:block/uuid` values before the UI sees them
> (`render_resource/query.cljs:15-46`). Pull structure only if `:result-transform` needs it.

### Keeping Child Blocks in the Result

By default, when a matched block's parent is **also in the result set**, the child is dropped
(`deps/outliner/src/logseq/outliner/tree.cljs:106-111`). Set `:remove-block-children? false` to keep
those nested matches (`components/query/result.cljs:43-44`,
`render_resource/query.cljs:171-179`):

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where [?b :block/tags :logseq.class/Task]]
 :remove-block-children? false}
```

### Tag Inheritance

A plain `:block/tags` clause matches **direct** tags only. To include tags that extend the one you
are asking about, use the built-in `tags` rule, which walks
`:logseq.property.class/extends` (`rules.cljc:266-295`). Its second argument is a **set**:

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?tags
         :where
         (tags ?b ?tags)]
 :inputs [#{"project"}]}
```

### Your Own Rules

```clojure
{:query [:find (pull ?b [:block/uuid])
         :where (open-task ?b)]
 :rules [[(open-task ?b)
          [?b :block/tags :logseq.class/Task]
          [?b :logseq.property/status ?s]
          [?s :db/ident ?status]
          [(contains? #{:logseq.property/status.todo
                        :logseq.property/status.doing} ?status)]]]}
```

User rules are concatenated ahead of the built-ins
(`src/main/frontend/worker/handler/query.cljs:194`).

### Exercise 10: Blocked Tasks

**Task:** Find open tasks that reference some other block via a user property named "Depends on".

<details>
<summary>Solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?prop-title
         :where
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         [?prop :db/ident ?ident]
         [?b ?ident _]
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]]
 :inputs ["Depends on"]}
```

Because user property idents carry a random suffix, resolving by title is the only portable
approach.

</details>

---

## Practical Examples

### Example 1: Today's Agenda

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

### Example 2: Tag Dashboard

```clojure
{:query [:find ?tag-title (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]
         [?b :block/tags ?tag]
         [?tag :block/title ?tag-title]
         [(not= ?tag-title "Task")]]
 :view (fn [rows]
         [:table
          [:thead [:tr [:th "Tag"] [:th "Active tasks"]]]
          [:tbody
           (for [[tag n] (sort-by second > rows)]
             [:tr [:td tag] [:td n]])]])}
```

### Example 3: Weekly Review

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.done]
         [?b :block/page ?p]
         [?p :block/tags :logseq.class/Journal]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:-7d :today]
 :collapsed? false}
```

### Example 4: Quick Wins

High priority, small estimate. "Estimate" is a user `:number` property, so it is resolved by title
and dereferenced through `:logseq.property/value`.

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?prop-title
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/priority ?p]
         [?p :db/ident :logseq.property/priority.high]
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         [?prop :db/ident ?ident]
         [?b ?ident ?pv]
         [?pv :logseq.property/value ?n]
         [(<= ?n 30)]]
 :inputs ["Estimate"]}
```

---

## Debugging Queries

### Common Mistakes

**Comparing a ref against a string**

```clojure
;; ❌ never matches — the value is a reference
[?b :logseq.property/status "Done"]

;; ✅
[?b :logseq.property/status ?s]
[?s :db/ident :logseq.property/status.done]
```

**Calling a keyword as a function inside a predicate**

```clojure
;; ❌ :db/ident is not callable in a Datalog predicate
[(= :logseq.property/status.done (:db/ident ?s))]

;; ✅
[?s :db/ident :logseq.property/status.done]
```

**Mixing date units**

```clojure
;; ❌ :today is YYYYMMDD; :logseq.property/deadline is epoch ms
[(= ?deadline ?today)]

;; ✅
[(>= ?deadline ?start)] [(<= ?deadline ?end)]
;; with :inputs [:start-of-today-ms :end-of-today-ms]
```

**Variable not bound**

```clojure
;; ❌ nothing ever binds ?b
:where
[?p :block/title ?t]
[?b :logseq.property/priority ?pr]

;; ✅ join them
:where
[?p :block/title ?t]
[?b :block/page ?p]
[?b :logseq.property/priority ?pr]
```

### Debugging Techniques

**1. Simplify.** Start with one clause and add one at a time.

```clojure
{:query [:find (count ?b)
         :where [?b :block/tags :logseq.class/Task]]}
```

**2. Look at the raw values.** Bind instead of pulling and Logseq prints the values as a list:

```clojure
{:query [:find ?status-title
         :where
         [?b :logseq.property/status ?s]
         [?s :block/title ?status-title]]}
```

**3. Check your inputs.** An input keyword that matches no dispatch branch is passed through
unresolved (`inputs.cljs:150-155`) — the query returns nothing, with no error. Confirm the spelling
against the list in [Using Inputs](#using-inputs).

**4. Suspect the `:view` last.** SCI errors are swallowed (`extensions/sci.cljs:55-58`), so a broken
`:view` looks like "the query stopped working". Delete `:view` temporarily and see whether results
come back.

---

## Best Practices

### Performance

**✅ Do — filter on the narrowest clause first:**

```clojure
:where
[?b :block/tags :logseq.class/Task]      ; unique-identity lookup
[?b :logseq.property/deadline ?d]
[(< ?d ?now)]
[?b :block/title ?t]                     ; text last
[(clojure.string/includes? ?t "important")]
```

**❌ Avoid — full scan first:**

```clojure
:where
[?b :block/title ?t]
[(clojure.string/includes? ?t "important")]
[?b :block/tags :logseq.class/Task]
```

Attributes declared `:db/index true` in `deps/db/src/logseq/db/frontend/schema.cljs:56-108` are
`:block/parent`, `:block/order`, `:block/page`, `:block/link`, `:block/alias`, `:block/created-at`,
`:block/updated-at`, `:block/name`, `:block/title` and `:block/journal-day`. `:db/ident`,
`:block/uuid` and `:file/path` are `:db.unique/identity`.

### Maintainability

Comment your clauses — the query editor is a Clojure editor, so `;;` works:

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?start ?end
         :where
         ;; open tasks
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]

         ;; due in the window
         [?b :logseq.property/deadline ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:start-of-today-ms :+7d-ms]}
```

### Reusability

Prefer `:inputs` over hard-coded literals, and move views and transforms you use repeatedly into
`config.edn` under `:query/views` and `:query/result-transforms`
(`components/query.cljs:133-140`, `:148`).

---

## Practice Challenges

### Challenge 1: Study Time This Week

Sum a "Duration" number property across blocks tagged `#study` written on journal pages in the last
seven days.

<details>
<summary>Solution</summary>

```clojure
{:query [:find (sum ?minutes)
         :in $ ?prop-title ?tag-title ?start
         :where
         [?tag :block/title ?tag-title]
         [?b :block/tags ?tag]
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?prop-title]
         [?prop :db/ident ?ident]
         [?b ?ident ?pv]
         [?pv :logseq.property/value ?minutes]
         [?b :block/page ?p]
         [?p :block/tags :logseq.class/Journal]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]]
 :inputs ["Duration" "study" :-7d]}
```

</details>

### Challenge 2: Stale Tasks

Find open tasks not updated in the last 30 days.

<details>
<summary>Solution</summary>

```clojure
{:query [:find (pull ?b [:block/uuid])
         :in $ ?cutoff
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]
         [?b :block/updated-at ?updated]
         [(< ?updated ?cutoff)]]
 :inputs [:-30d-ms]}
```

`:block/updated-at` is epoch ms (`schema.cljs:85-86`), so the cutoff must be a `-ms` input.
`:-30d-ms` matches the relative-date-time pattern (`inputs.cljs:74`).

</details>

### Challenge 3: Team Workload

Count active tasks per assignee and show them as a table.

<details>
<summary>Solution</summary>

```clojure
{:query [:find ?person (count ?b)
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]
         [?b :logseq.property/assignee ?p]
         [?p :block/title ?person]]
 :view (fn [rows]
         [:table
          [:thead [:tr [:th "Team member"] [:th "Active tasks"]]]
          [:tbody
           (for [[person n] (sort-by second > rows)]
             [:tr [:td person] [:td n]])]])}
```

`:logseq.property/assignee` is a cardinality-many `:node` property (`property.cljs:401-407`), so a
task with two assignees contributes to both counts.

</details>

---

## Next Steps

**Continue Learning:**

1. Practice with your own queries — start simple, add one clause at a time
2. Study the [Query Patterns Reference](../../reference/advanced/query-patterns.md)
3. Read the [Query System Reference](../../reference/advanced/query-system.md) for the full contract
4. Explore [Properties Reference](../../reference/data/properties-reference.md)

**Build a personal query library:**

- Daily agenda
- Weekly review
- Tag dashboards
- Stale-task sweeps

**Advanced Topics:**

- Custom `:rules`
- Recursion with `parent` and `class-extends`
- Reusable `:query/views` and `:query/result-transforms` in `config.edn`

**Related:**

- [Query Patterns Reference](../../reference/advanced/query-patterns.md)
- [Query System Reference](../../reference/advanced/query-system.md)
- [Task Management Workflows](../../how-to/workflows/task-management-workflows.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
