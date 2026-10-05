---
title: 'Customize the Journal Queries'
description: "What the three task sections at the bottom of today's journal query in a DB graph, and how to replace them with your own, including a port of the file-graph NOW and NEXT queries"
status: draft
lastVerified: '2026-10-04'
verifiedAgainst: '22a29b30de'
reviewStatus: authored-from-source
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  howTo:
    - '[Advanced Queries](./advanced-queries.md)'
    - '[CLI Basics](../integrations/cli-basic.md)'
  reference:
    - '[config.edn Reference](../../reference/configuration/config-edn.md)'
    - '[Dynamic Variables](../../reference/syntax/dynamic-variables.md)'
---

# Customize the Journal Queries

> **Goal**: know exactly what the task sections at the bottom of today's journal show, and
> replace them with your own queries, for example the NOW and NEXT queries carried over
> from a file graph.

## Overview

Today's journal page, and only today's, ends with up to three collapsed sections
(`src/main/frontend/components/page.cljs:574-578`):

| Section | Where it comes from | Editable in `config.edn` |
| --- | --- | --- |
| **DOING** | `:default-queries :journals`, first entry | Yes, replace it |
| **TODO** | `:default-queries :journals`, second entry | Yes, replace it |
| **Scheduled and Deadline** | Hard-coded component and worker query | No. You can only change its window or turn it off |

They don't appear on other journal days, in the right sidebar, or in page previews.

## Before You Start

- You're using a DB graph. File graphs use `:block/marker` strings, which don't exist here.
- You can edit the graph's `config.edn` in the app (**Settings → Edit config.edn**). In a DB
  graph the file lives inside the database, not on disk, so edit it in the app.
- Statuses and priorities are **references** to closed values, not strings. You match them by
  `:db/ident`, for example `:logseq.property/status.doing`.

---

## 1. The built-in queries

### DOING

Defined in `src/main/frontend/state.cljs:441-452`:

```clojure
{:title-key :journal.default-query/doing     ; renders "DOING"
 :title-icon "InProgress50"
 :query [:find (pull ?b [*])
         :in $ ?start ?today
         :where
         (task ?b #{"Doing"})
         [?b :block/page ?p]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]
         [(<= ?d ?today)]]
 :inputs [:14d :today]
 :collapsed? true}
```

```
for each block b with status "Doing":
    if b is on a journal page whose day is between today − 14 days and today (inclusive):
        include b
```

`:14d` is the deprecated input syntax; it is rewritten to `:-14d`
(`deps/db/src/logseq/db/frontend/inputs.cljs:19-23`). The `task` rule matches the status
name, so `"Doing"` and `"doing"` both work (`deps/db/src/logseq/db/frontend/rules.cljc:299-306`).

### TODO

Defined in `src/main/frontend/state.cljs:453-464`:

```clojure
{:title-key :journal.default-query/todo      ; renders "TODO"
 :title-icon "Todo"
 :query [:find (pull ?b [*])
         :in $ ?start ?next
         :where
         (task ?b #{"Todo"})
         [?b :block/page ?p]
         [?p :block/journal-day ?d]
         [(> ?d ?start)]
         [(< ?d ?next)]]
 :inputs [:today :7d-after]
 :group-by-page? false
 :collapsed? true}
```

```
for each block b with status "Todo":
    if b is on a journal page whose day is after today and before today + 7:
        include b                       ; tomorrow through 6 days out
```

Both queries look **only at journal pages**, and neither sorts its results.

### Scheduled and Deadline

This section is not a `config.edn` query. The page component
(`src/main/frontend/components/scheduled_deadlines.cljs:15-45`) asks the worker for the
window, and the worker runs this query (`src/main/frontend/worker/handler/property.cljs:929-947`):

```clojure
[:find [(pull ?block [:* {:block/page [:db/id :block/title :block/uuid]}]) ...]
 :in $ ?start-time ?end-time
 :where
 (or [?block :logseq.property/scheduled ?n]
     [?block :logseq.property/deadline ?n])
 [(>= ?n ?start-time)]
 [(<= ?n ?end-time)]
 [?block :logseq.property/status ?status]
 [?status :db/ident ?status-ident]
 [(not= ?status-ident :logseq.property/status.done)]
 [(not= ?status-ident :logseq.property/status.canceled)]]
```

```
start = today at 00:00 local time
end   = (today + N days) at 00:00 local time (both inclusive)       ; N = :scheduled/future-days, default 7

for each block b on ANY page:
    if b.scheduled or b.deadline is between start and end (inclusive)
    and b has a status
    and that status is not Done or Canceled:
        include b

nest each block's children, then group the results by page
```

The window is computed in `src/main/frontend/db/async.cljs:460-466`. Unlike DOING and TODO,
this section:

- searches **every page**, and selects by date property rather than by which journal day the
  task sits on;
- includes **every open status**: Backlog, Todo, Doing and In Review. A dated block with no
  status is left out;
- **includes today**, while TODO starts tomorrow;
- compares **epoch milliseconds**, because Scheduled and Deadline store plain numbers;
- uses **local midnight** (since `829fb85cca`; earlier builds used UTC midnight). Date-only
  Scheduled and Deadline values are stored as local midnight, so the window matches the journal
  day in every timezone (`deps/common/src/logseq/common/util/date_time.cljs:119-130`, called from
  `src/main/frontend/db/async.cljs:460-466`). The window ends at 00:00 local on day N, so a task
  timed later on day N falls outside it. Verified live 2026-10-04 on Logseq DB 2.0.1 nightly 20261001 in America/Chicago (UTC−5), default 7 future days, six TODO tasks scheduled at fixed instants: 00:00 today, 12:00 today, 23:00 on day 6 and 00:00 on day 7 (local) appeared in the Scheduled and Deadline section; 23:59 yesterday and 01:00 on day 7 did not. A UTC window would have included the first excluded task and dropped the day-6 and day-7 ones.

Two `config.edn` keys control it:

```clojure
:scheduled/future-days 14                          ; default 7
:feature/disable-scheduled-and-deadline-query? true  ; hide the section
```

---

## 2. Rules for replacing the default queries

- Setting `:default-queries {:journals [...]}` **replaces both** DOING and TODO, because the
  config merge goes only one level deep. `{:journals []}` removes them.
- Each entry is an advanced-query map: `:query`, optional `:inputs`, `:result-transform`,
  `:group-by-page?`, and a header. Use a plain `:title` string; `:title-key` and `:title-icon`
  exist so the built-ins can be translated.
- `:collapsed? false` does nothing. `today-queries` sets `:collapsed? true` on every entry
  (`src/main/frontend/components/page.cljs:252`).
- Pull `[*]`, or at least `:block/uuid`. In-app query results need the uuid to render blocks.
- `:result-transform` receives the **pulled block maps** and runs before they are turned into
  uuids (`src/main/frontend/worker/handler/render_resource/query.cljs:206-227`). It runs in
  SCI, where `#(…)` and keyword-as-function calls are silently dropped. Write
  `(fn [x] …)` and use `get` and `get-in`.
- Journal-day inputs (`:today`, `:+6d`) resolve to `yyyyMMdd` integers. Millisecond inputs
  (`:+1d-start`, `:+6d-end`, `:start-of-today-ms`) resolve to **local** time
  (`inputs.cljs:113-133`).

---

## 3. Recipe: NOW and NEXT from a file graph

A common file-graph setup:

```clojure
:default-queries
{:journals
 [{:title "🔨 NOW"
   :query [:find (pull ?h [])
           :in $ ?start ?today
           :where
           [?h :block/marker ?marker]
           [(contains? #{"NOW" "DOING"} ?marker)]
           [?h :block/page ?p]
           [?p :block/journal? true]
           [?p :block/journal-day ?d]
           [(>= ?d ?start)]
           [(<= ?d ?today)]]
   :inputs [:14d :today]
   :result-transform (fn [result]
                       (sort-by (fn [h] (get h :block/priority "Z")) result))
   :group-by-page? false
   :collapsed? false}
  {:title "📅 NEXT"
   :query [:find (pull ?h [])
           :in $ ?start ?next
           :where
           [?h :block/marker ?marker]
           [(contains? #{"NOW" "LATER" "TODO"} ?marker)]
           [?h :block/page ?p]
           [?p :block/journal? true]
           [?p :block/journal-day ?d]
           [(> ?d ?start)]
           [(< ?d ?next)]]
   :inputs [:today :7d-after]
   :group-by-page? false
   :collapsed? false}]}
```

What each part becomes in a DB graph:

| File graph | DB graph |
| --- | --- |
| `:block/marker` strings | `:logseq.property/status`, a ref matched by `:db/ident` |
| `NOW`, `DOING` | `:logseq.property/status.doing` |
| `LATER`, `TODO` | `:logseq.property/status.todo` (no NOW or LATER statuses exist) |
| `[?p :block/journal? true]` | Dropped: only journal pages have `:block/journal-day` |
| `:block/priority` letter, `"Z"` fallback | `:logseq.property/priority` ref; pull its `:db/ident` and rank it |
| `(pull ?h [])` | `(pull ?b [*])` |
| `:collapsed? false` | Kept for readability, but ignored |

### The DB version

This version also extends NEXT to tasks on **other pages**, using their Scheduled or Deadline
date, since a non-journal page has no journal day:

```clojure
:default-queries
{:journals
 [{:title "🔨 NOW"
   :query [:find (pull ?b [* {:logseq.property/priority [:db/ident]}])
           :in $ ?start ?today
           :where
           [?b :logseq.property/status ?s]
           [?s :db/ident ?status]
           [(contains? #{:logseq.property/status.doing} ?status)]
           [?b :block/page ?p]
           [?p :block/journal-day ?d]
           [(>= ?d ?start)]
           [(<= ?d ?today)]]
   :inputs [:14d :today]
   :result-transform (fn [result]
                       (sort-by (fn [b]
                                  (get {:logseq.property/priority.urgent 0
                                        :logseq.property/priority.high 1
                                        :logseq.property/priority.medium 2
                                        :logseq.property/priority.low 3}
                                       (get-in b [:logseq.property/priority :db/ident])
                                       4))
                                result))
   :group-by-page? false
   :collapsed? false}
  {:title "📅 NEXT"
   :query [:find (pull ?b [*])
           :in $ ?from-day ?to-day ?from-ms ?to-ms
           :where
           [?b :logseq.property/status ?s]
           [?s :db/ident ?status]
           [(contains? #{:logseq.property/status.doing
                         :logseq.property/status.todo} ?status)]
           (or-join [?b ?from-day ?to-day ?from-ms ?to-ms]
             ;; on a journal page from tomorrow through 6 days out
             (and [?b :block/page ?p]
                  [?p :block/journal-day ?d]
                  [(>= ?d ?from-day)]
                  [(<= ?d ?to-day)])
             ;; on any page, scheduled in that range
             (and [?b :logseq.property/scheduled ?t]
                  [(>= ?t ?from-ms)]
                  [(<= ?t ?to-ms)])
             ;; on any page, due in that range
             (and [?b :logseq.property/deadline ?t]
                  [(>= ?t ?from-ms)]
                  [(<= ?t ?to-ms)]))]
   :inputs [:+1d :+6d :+1d-start :+6d-end]
   :group-by-page? false
   :collapsed? false}]}
```

Notes on the DB version:

- **Priority sort.** Pulling `{:logseq.property/priority [:db/ident]}` turns the priority ref
  into `{:db/ident :logseq.property/priority.high}`, which the transform can rank. Tasks with
  no priority sort last, as `"Z"` did.
- **Same windows.** NOW still covers today − 14 days through today. NEXT still covers tomorrow
  through 6 days out; the exclusive `?start`/`?next` pair became an inclusive, explicit range.
- **Local time.** The millisecond inputs are local midnight and 23:59:59.999, so NEXT's
  boundaries match your clock, unlike the built-in Scheduled and Deadline section.
- **No duplicates within NEXT.** A journal task that also has a Scheduled date in range
  appears once.
- **Status rule alternative.** `(task ?b #{"Doing" "Todo"})` works too, as in the built-ins.

### Avoid listing the same task twice

NEXT's Scheduled and Deadline branches overlap the built-in **Scheduled and Deadline** section:
a task on a project page due in three days appears in both. Pick one:

- drop the two branches and let the built-in section cover other pages; or
- keep them and add `:feature/disable-scheduled-and-deadline-query? true`.

To list **every** open task on non-journal pages regardless of date, replace the two branches
with `(and [?b :block/page ?p] [(missing? $ ?p :block/journal-day)])` inside the `or-join`.
Expect a long list.

---

## 4. Test the queries with sample tasks

Create tasks on specific journal days with the CLI. A missing journal page is created as a
real journal page:

```bash
logseq upsert task -g my-graph --target-page "25th Sep 2026" \
  --content 'QTEST now: Doing low, 5 days ago' --status doing --priority low
logseq upsert task -g my-graph --target-page "QTEST Project" \
  --content 'QTEST next: Todo on project page' --status todo \
  --scheduled "2026-10-03T14:00:00.000Z"
```

Use the graph's journal title format (here `25th Sep 2026`). Cover both sides of every
boundary: the first and last day in range, the day just outside, today, wrong statuses
(Done, Backlog), and a task on a non-journal page with and without a date.

The CLI doesn't resolve `:today` or other keyword inputs, so test with literal values:

```bash
logseq query -g my-graph --inputs '[20261002 20261007 1790917200000 1791435599999]' \
  --query '[:find ?title :in $ ?from-day ?to-day ?from-ms ?to-ms :where … [?b :block/title ?title]]'
```

`:result-transform` doesn't run in the CLI. Check the sort order in today's journal.

---

## Verification

**Verified 2026-10-01** against `9c18a5432b` source and graph `gtdlog` with CLI
`8ffbb22-dirty`:

- The status filters and the NOW and NEXT windows, including NEXT's other-page branches, with
  22 sample tasks covering each boundary and wrong status. Each query returned exactly the
  expected tasks.
- The every-open-task variant with `missing?`, which returned all Todo and Doing tasks on
  non-journal pages and left out the Done one.
- The pull shape `{:logseq.property/priority {:db/ident …}}` that the sort relies on.
- That `--target-page` with a missing journal title creates a page with `:block/journal-day`.

From source only: the forced collapse, the local-midnight window of Scheduled and Deadline (UTC before `829fb85cca`; the 2026-10-01 boundary samples predate that change, and the local-midnight boundaries were re-verified live 2026-10-04), and when
`:result-transform` runs. **Not yet observed in the app:** the NOW sort order.
