---
title: 'Advanced Queries'
description: 'How to write advanced Datalog queries in Logseq'
status: stable
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
  howTo:
    - '[View Functions](../content/view-types.md)'
---

# Advanced Queries

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-15 (verified against `be800f1711`; this revision
> re-checked the pull/normalization rules only)
> How to write powerful Datalog queries to search and analyze your graph.

## Query Basics

> **Removed:** The `#+BEGIN_QUERY` / `#+END_QUERY` wrapper is **file-graph syntax and does not work in a DB graph.** A DB advanced query block stores the query map as the raw text of its `:logseq.property/query` value, which is read back with `safe-read-string` and treated as an advanced query only if the result `map?` (`src/main/frontend/components/block.cljs:4139-4152`). A `#+BEGIN_QUERY` header is not valid EDN, so the read fails and the block never renders as a query. On import from a file graph the wrapper is stripped (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1452`). Every example below has been rewritten to the bare-map form.

Create one with the **Advanced Query** slash command, which tags the block
`:logseq.class/Query` and gives it a Clojure code-block query property
(`src/main/frontend/commands.cljs:125-132`, registered at line 326). Then write
the map directly:

```clojure
{:query [:find (pull ?b [*])
         :where [?b :logseq.property/status ?s]
                [?s :block/title "Todo"]]}
```

---

## Query Structure

```clojure
{:query [:find ...
         :in $ ?input
         :where ...]
 :inputs [...]
 :rules [...]
 :remove-block-children? true
 :result-transform ...
 :view ...}
```

| Key                       | Purpose                                                                     |
| ------------------------- | --------------------------------------------------------------------------- |
| `:query`                  | Datalog query. Required, must be a vector starting with `:find`             |
| `:inputs`                 | Values bound to the `:in` clause. Must be a vector                          |
| `:rules`                  | Extra user rules, merged with the auto-injected built-ins. Must be a vector  |
| `:remove-block-children?` | Keep matching child blocks as separate results when set to `false`          |
| `:result-transform`       | Modify results, in the worker, before normalization                         |
| `:view`                   | Custom display, in the UI, after normalization                              |

`:query`, `:inputs` and `:rules` are validated on every run; anything else in the
map is ignored by the worker
(`src/main/frontend/worker/handler/query.cljs:243-252`,
`src/main/frontend/worker/handler/render_resource/query.cljs:48-108`).

> **Removed:** `:title`, `:collapsed?` and `:group-by-page?` are file-graph keys and
> are not part of the DB advanced-query contract.
>
> - `:title` is only rendered for *built-in* queries; ordinary query blocks take a
>   branch with no title header (`src/main/frontend/components/query.cljs:166-177`).
>   Put the heading in the block's own text, or in `:view`.
> - `:group-by-page?` is short-circuited to `false` whenever the query is a table
>   or a DB-graph view (`src/main/frontend/components/query/result.cljs:7-15`).
>   The `(by-page false)` escape hatch it documents only applies to *simple DSL*
>   query strings — that branch tests `(string? query)`, and an advanced query's
>   `:query` is a vector.
> - `:collapsed?` is still read (`src/main/frontend/components/query.cljs:144`),
>   but collapse state in a DB graph belongs to the block (`:block/collapsed?`).
>
> All three are explicitly stripped from advanced query maps when a file graph is
> imported: `migrate-advanced-query-string` dissocs `[:title :group-by-page? :collapsed?]`
> (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1201-1209`).

### Control Child Block Results

Set `:remove-block-children?` on the query map itself, at the same level as
`:query`, `:inputs`, `:result-transform`, and `:view`. It is a per-query
display option, not a `config.edn` setting.

- Default value: `true` (an absent key is not `false`, so the top-level filter runs)
- When `true`: if both a parent block and one of its children match, Logseq only
  returns the top-level parent as a separate result
- When `false`: matching child blocks are also returned individually
- Applies **only** to pure block results — every row must be a single column
  holding a block map or entity. A tuple or scalar result skips this filter
  entirely (`src/main/frontend/worker/handler/render_resource/query.cljs:161-179,195-202`)

Use this when you want every matching task block to appear separately in an
advanced query result:

```clojure
{:query [:find (pull ?b [*])
         :in $ %
         :where (task ?b #{"Canceled" "Done" "In Review" "Doing" "Todo" "Backlog"})]
 :remove-block-children? false}
```

The six status titles above are the complete closed-value set
(`deps/db/src/logseq/db/frontend/property.cljs:313-318`). You may omit `:in $ %` —
built-in rules used in `:where` are detected and the `%` binding is appended
automatically (`src/main/frontend/worker/handler/query.cljs:185-204`).

---

## Find Patterns

### Pull All Fields

```clojure
[:find (pull ?b [*])
 :where ...]
```

### Pull Specific Fields

A partial pull pattern **must include `:block/uuid`**:

```clojure
[:find (pull ?b [:block/uuid :block/title])
 :where ...]
```

Leaving it out throws `Renderer query result map has no UUID` and the query
block dies inside its React error boundary. See
[Partial pulls must include `:block/uuid`](#partial-pulls-must-include-blockuuid)
below for why. When in doubt use `[*]`, which always carries it.

### Find Collection

```clojure
[:find [?title ...]
 :where [?p :block/title ?title]]
```

### Find Tuples

```clojure
[:find ?title ?day
 :where [?p :block/journal-day ?day]
        [?p :block/title ?title]]
```

> **Removed:** The scalar find form `[:find ?title . …]` should not be used in a
> DB advanced query. `d/q` returns a bare value for `.`, but the renderer's row
> normalizer maps over the result as a collection
> (`src/main/frontend/worker/handler/render_resource/query.cljs:151-159`). Mapping
> over a string in ClojureScript walks it character by character, so
> `"mypage"` comes back as six one-character rows rather than one value. Use the
> relation form (`[:find ?title …]`, one row) or the collection form
> (`[:find [?title ...] …]`) instead.

### What a row looks like when it reaches you

Queries run in the DB worker, and each row is normalized before it crosses back
to the UI (`render_resource/query.cljs:15-46,195-202`). This is the single most
common source of "my query works but my `:view` shows nothing":

| `:find` shape                | Row delivered to `:view`                                 |
| ---------------------------- | -------------------------------------------------------- |
| `(pull ?b [*])`              | the block's `:block/uuid` — **not** a map                 |
| `?title` (one scalar column) | the bare value, unwrapped                                 |
| `?title ?day` (two columns)  | a 2-element vector, e.g. `["Jan 1st, 2020" 20200101]`     |

Confirmed by `query-resource-preserves-scalar-tuples-test`
(`src/test/frontend/worker/handler/render_resource_test.cljs:2265-2282`) and
`query-resource-resolves-advanced-page-block-and-today-inputs-test` (line 1921),
which asserts a pulled block arrives as a UUID.

`:result-transform` is the exception: it runs in the worker on the rows *before*
normalization, so it does see pulled maps and entities
(`render_resource/query.cljs:181-202`). Filter and sort there; format in `:view`.

### Partial pulls must include `:block/uuid`

The normalizer turns a pulled map into its `:block/uuid`, so it needs one to be
there. A map without it is a hard failure, not a silently dropped column
(`render_resource/query.cljs:15-23`):

```clojure
(or (map? cell) (de/entity? cell))
(common/fail! "Renderer query result map has no UUID" {:value cell})
```

The query block then renders as a React error boundary, and the console shows a
500 from the worker invoke with `Renderer query result map has no UUID`.

```clojure
;; crashes the block — :db/id is not :block/uuid
[:find (pull ?p [:db/id :block/title]) :where ...]

;; fine
[:find (pull ?p [:block/uuid :block/title]) :where ...]
[:find (pull ?p [*]) :where ...]
```

This bites hardest when a query is drafted outside the app. `logseq query` on
the CLI returns raw `d/q` output and never runs the normalizer, so a pattern
missing `:block/uuid` prints perfectly good results there and still takes the
block down when pasted into a graph. Verify pull patterns in the app, not only
on the CLI.

Only the top-level pulled map is checked. A map carrying `:block/uuid` returns
that uuid immediately, so nested joins such as
`(pull ?b [* {:logseq.property/status [:block/title]}])` are never visited by
the normalizer and need no uuid of their own — they reach `:result-transform`
intact and collapse to the outer uuid afterwards.

---

## Common Query Patterns

### All TODO Tasks

In DB graphs, task status is stored via the `:logseq.property/status` ref property.
Status values are entities with `:block/title` such as `"Todo"`, `"Doing"`, `"Done"`,
`"Backlog"`, `"In Review"`, and `"Canceled"`.

```clojure
{:query [:find (pull ?b [*])
         :where [?b :logseq.property/status ?s]
                [?s :block/title "Todo"]]}
```

Status values are closed-value entities of `:type :default`, so their text lives
in `:block/title` (`deps/db/src/logseq/db/frontend/property/build.cljs:20-22`).
They also carry stable `:db/ident`s — `[?s :db/ident :logseq.property/status.todo]`
is an equally valid, and locale-proof, match.

### Tasks Due Today

Deadline and scheduled dates use `:logseq.property/deadline` and
`:logseq.property/scheduled`. **Both are `:datetime`, i.e. epoch milliseconds**
(`deps/db/src/logseq/db/frontend/property.cljs:340-353`).

> **Fixed (broken query).** This example previously compared `:logseq.property/deadline`
> against the `:today` input. `:today` resolves to a **YYYYMMDD integer** such as
> `20260909` (`deps/db/src/logseq/db/frontend/inputs.cljs:100-101`), which can never
> equal an epoch-millisecond value. The query matched nothing. Use the
> millisecond inputs and a range, since a `:datetime` carries a time of day:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where [?b :logseq.property/deadline ?d]
                [(>= ?d ?start)]
                [(<= ?d ?end)]]
 :inputs [:start-of-today-ms :end-of-today-ms]}
```

### Tasks in Date Range

> **Fixed (broken query).** This example previously used `:inputs [:-7d :today]`
> against `:logseq.property/scheduled`. Relative date inputs resolve to YYYYMMDD
> integers too, not milliseconds — `resolve-keyword-input :relative-date` ends in
> `date-time-util/date->int` (`inputs.cljs:121-125`) — so the comparison was
> integer-vs-epoch and matched nothing. Append a time suffix to get milliseconds:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where [?b :logseq.property/scheduled ?d]
                [(>= ?d ?start)]
                [(<= ?d ?end)]]
 :inputs [:-7d-start :end-of-today-ms]}
```

The rule of thumb: **YYYYMMDD integers compare against `:block/journal-day`;
epoch milliseconds compare against `:datetime` properties.** Mixing them is
silent — no error, just an empty result.

### Blocks with Specific Tag

`:block/refs` includes each of a block's `:block/tags`
(`deps/outliner/src/logseq/outliner/pipeline.cljs:118`), so this matches tagged
blocks — but it *also* matches plain `[[project]]` links and property refs:

```clojure
{:query [:find (pull ?b [*])
         :where [?b :block/refs ?ref]
                [?ref :block/name "project"]]}
```

For tags only, match `:block/tags` directly:

```clojure
{:query [:find (pull ?b [*])
         :where [?b :block/tags ?t]
                [?t :block/name "project"]]}
```

### Blocks on Current Page

`:current-page` resolves to the current page's title, lower-cased
(`inputs.cljs:84-86`), which is exactly what `:block/name` stores:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?current-page
         :where [?p :block/name ?current-page]
                [?b :block/page ?p]
                [?b :logseq.property/status ?s]
                [?s :block/title "Todo"]]
 :inputs [:current-page]}
```

---

## Property Queries

In DB graphs, properties are stored as direct entity attributes (not inside a
`:block/properties` map). Built-in properties use the `logseq.property/` namespace.
User-defined properties use their configured `:db/ident`.

### Blocks with Property

```clojure
{:query [:find (pull ?b [*])
         :where [?b :logseq.property/priority _]]}
```

### Property Equals Value

Priority values are ref entities: `"Low"`, `"Medium"`, `"High"`, `"Urgent"`
(`deps/db/src/logseq/db/frontend/property.cljs:334-337`).

```clojure
{:query [:find (pull ?b [*])
         :where [?b :logseq.property/priority ?p]
                [?p :block/title "High"]]}
```

Because these are refs, a predicate over the bound variable — `[(= :logseq.property/priority.high (:db/ident ?p))]` —
does **not** work: `?p` is an entity id, not an entity. Use a datom pattern,
either `[?p :block/title "High"]` or `[?p :db/ident :logseq.property/priority.high]`.

### Number Properties Need a Deref

`:number` is a value-ref type: the number lives on a separate entity under
`:logseq.property/value` (`deps/db/src/logseq/db/frontend/property/type.cljs:52-56`).
A direct bind gives you an entity id, so add the deref step:

```clojure
{:query [:find ?title ?rating
         :where [?p :block/title ?title]
                [?p :user.property/rating-a3Kf9Qz1 ?rv]
                [?rv :logseq.property/value ?rating]]}
```

Note the ident suffix. Every user-created property and class ident gets an
8-character random suffix at creation time
(`deps/db/src/logseq/db/frontend/db_ident.cljc:82-90`), so you cannot guess
`:user.property/rating`. List the real ones with:

```clojure
[:find ?ident :where [?p :db/ident ?ident]
 [(str ?ident) ?s] [(clojure.string/starts-with? ?s ":user.property/")]]
```

---

## Date Inputs

### Built-in Inputs

All resolution happens in `deps/db/src/logseq/db/frontend/inputs.cljs`.
The **Value** column is the part that decides whether your comparison can match.

| Input                | Value                                      | Source          |
| -------------------- | ------------------------------------------ | --------------- |
| `:today`             | YYYYMMDD **integer**                       | `inputs.cljs:100-101` |
| `:yesterday`         | YYYYMMDD **integer**                       | `inputs.cljs:103-104` |
| `:tomorrow`          | YYYYMMDD **integer**                       | `inputs.cljs:106-107` |
| `:current-page`      | Current page title, lower-cased            | `inputs.cljs:84-86`   |
| `:query-page`        | `:block/name` of the query block's page    | `inputs.cljs:88-90`   |
| `:current-block`     | Current block's `:db/id`                   | `inputs.cljs:92-94`   |
| `:parent-block`      | Parent of current block's `:db/id`         | `inputs.cljs:96-98`   |
| `:right-now-ms`      | Current time, epoch **milliseconds**       | `inputs.cljs:109-110` |
| `:start-of-today-ms` | Today 00:00:00.000, epoch **milliseconds** | `inputs.cljs:113-118` |
| `:end-of-today-ms`   | Today 23:59:59.999, epoch **milliseconds** | `inputs.cljs:113-118` |

> ⚠️ `:today`, `:yesterday` and `:tomorrow` are **not** timestamps. Compare them
> against `:block/journal-day`, never against `:logseq.property/deadline` or
> `:logseq.property/scheduled`.

> **Note:** `:today` additionally requires the caller to supply a `:today-day`
> context value, or the query fails with `"Query today input requires :today-day"`
> (`src/main/frontend/worker/handler/query.cljs:163-166`). The UI sets it
> automatically whenever `:today` appears in `:inputs`
> (`src/main/frontend/components/query.cljs:141-143`). `:yesterday` and
> `:tomorrow` carry no such requirement.

### Relative Date Inputs

The modern format uses `:<direction><amount><unit>` (`inputs.cljs:73`). **These
also resolve to YYYYMMDD integers** (`inputs.cljs:121-125`), not milliseconds:

| Input  | Description      |
| ------ | ---------------- |
| `:-7d` | 7 days ago       |
| `:+7d` | 7 days from now  |
| `:-1w` | 1 week ago       |
| `:+1w` | 1 week from now  |
| `:-1m` | 1 month ago      |
| `:+1m` | 1 month from now |
| `:-1y` | 1 year ago       |
| `:+1y` | 1 year from now  |

### Relative Date-Time Inputs

Append a time suffix to get **epoch milliseconds** instead
(`inputs.cljs:74,128-133`). The recognised suffixes are `ms`, `start`, `end`, or
a literal `HH`, `HHMM`, `HHMMSS` or `HHMMSSmmm` (`inputs.cljs:47-67`):

| Input          | Value                                                        |
| -------------- | ------------------------------------------------------------ |
| `:-7d-start`   | 7 days ago at 00:00:00.000                                   |
| `:-7d-end`     | 7 days ago at 23:59:59.999                                   |
| `:-7d-ms`      | 7 days ago at 00:00:00.000 (`ms` follows the sign: `+` → end of day) |
| `:+1d-1201`    | Tomorrow at 12:01                                            |
| `:-1w-083000`  | A week ago at 08:30:00                                       |

The same suffixes apply to today: `:today-start`, `:today-end`, `:today-09`,
`:today-1159`, `:today-HHMMSS`, `:today-HHMMSSmmm` (`inputs.cljs:78,113-118`).
Out-of-range components are clamped rather than rejected — `:today-9999` becomes
23:59 (`inputs.cljs:63-67`).

> **Note:** The older `:Xd-before` / `:Xd-after` format (e.g., `:7d-before`) is
> deprecated but still supported: it is rewritten to the modern form before
> resolution (`inputs.cljs:19-23,80,135-138`). Only the `d` unit is recognised in
> the deprecated form.

> **Unverified:** an input keyword that no resolver recognises is passed through
> to the query **unresolved** rather than raising an error (`inputs.cljs:149-155`
> and its own comment note this is "too subtle"). A misspelled input therefore
> looks like a query that simply returns nothing. We have not confirmed the
> end-user symptom in a running app.

### Usage

```clojure
{:query [:find ...
         :in $ ?today
         :where ...]
 :inputs [:today]}
```

## Findings: New Capabilities

- **Advanced queries execute in the DB worker behind a validated resource
  contract.** `src/main/frontend/worker/handler/render_resource/query.cljs`
  (added 2026-08-04) type-checks the query spec, runs it, applies
  `:result-transform`, and normalizes every row. The UI first projects your map
  down to the known keys (`src/main/frontend/components/query/result.cljs:17-47`) —
  so an unrecognized key is dropped silently rather than reported — and the
  worker then rejects anything outside
  `#{:kind :query :inputs :rules :current-page-title :current-block-uuid :today-day :remove-block-children? :result-transform-edn}`
  (lines 48-108).
- **`:rules` is a first-class query-map key.** Supply your own rule vector and it
  is merged with the auto-injected built-ins; the `%` binding is appended to `:in`
  for you if you did not write it (`worker/handler/query.cljs:185-204`, test
  `query-resource-injects-built-in-rules-and-merges-user-rules-test`).
- **Page-ref equality is rewritten automatically.** `(= ?x "[[Some Page]]")` in a
  `:where` clause is turned into `(contains? ?x "some page")` before the query
  runs (`worker/handler/query.cljs:128-146`).
- Query date inputs include today-time variants like `:today-start`, `:today-end`,
  and fixed-time forms such as `:today-1159` and `:+1d-1201`
  (`deps/db/src/logseq/db/frontend/inputs.cljs:74,78`).
- `:result-transform` can be supplied as a keyword and resolved through
  `[:query/result-transforms ...]` in config, not just as an inline function
  (`src/main/frontend/components/query.cljs:133-140`;
  `src/main/frontend/schema/handler/common_config.cljc:37-39`). A missing key
  throws `"Missing query result transform"`. The parallel `:query/views` lookup
  for `:view` keywords fails *silently* instead (line 148).

---

## Result Transform

`:result-transform` may be an inline function or a keyword that resolves
through `[:query/result-transforms ...]` in config.

It runs **in the worker, on pre-normalization rows**: for a `(pull ?b [*])` query
that means a sequence of pull maps, and for a tuple query a sequence of vectors
(`render_resource/query.cljs:181-202`). It must return a sequential or a set, or
the query fails with `"Query result transform must return rows"` (lines 188-191).

> **Fixed (broken examples).** The previous sort and filter examples reached into
> a pulled row with `(:block/title (:logseq.property/status r))`. A `[*]` pull
> returns card-one refs as `{:db/id N}`, so `:block/title` on that is always
> `nil` — the sorts were no-ops and the filters never excluded anything. Either
> pull the nested attribute explicitly, or bind it as its own `:find` column.

### Sort Results

Pull the nested title so the transform has something to read:

```clojure
{:query [:find (pull ?b [* {:logseq.property/priority [:block/title]}])
         :where [?b :logseq.property/priority _]]
 :result-transform
 (fn [result]
   (sort-by (fn [r] (get-in r [:logseq.property/priority :block/title])) result))}
```

### Filter Results

```clojure
{:query [:find (pull ?b [* {:logseq.property/status [:block/title]}])
         :where [?b :logseq.property/status _]]
 :result-transform
 (fn [result]
   (remove (fn [r] (= "Done" (get-in r [:logseq.property/status :block/title])))
           result))}
```

Filtering in `:where` is cheaper and usually clearer — reach for
`:result-transform` only when the predicate cannot be expressed in Datalog.

### Limit Results

```clojure
{:result-transform
 (fn [result]
   (take 10 result))}
```

### Complex Transform

```clojure
{:query [:find (pull ?b [* {:logseq.property/status [:block/title]}
                           {:logseq.property/priority [:block/title]}])
         :where [?b :logseq.property/status _]]
 :result-transform
 (fn [result]
   (->> result
        (filter (fn [r] (= "Todo" (get-in r [:logseq.property/status :block/title]))))
        (sort-by (fn [r] (get-in r [:logseq.property/priority :block/title])))
        (take 5)))}
```

---

## Combining Conditions

### AND (both conditions)

```clojure
[:where [?b :logseq.property/status ?s]
        [?s :block/title "Todo"]
        [?b :block/refs ?ref]
        [?ref :block/name "project"]]
```

### OR (either condition)

```clojure
[:where [?b :logseq.property/status ?s]
        (or [?s :block/title "Todo"]
            [?s :block/title "Doing"])]
```

### NOT (exclude)

Prefer a negated predicate over wrapping one in `not` — `not` is meant for
negating *clauses* that bind, and a bare predicate inside it has no relation to
antijoin against:

```clojure
[:where [?b :logseq.property/status ?s]
        [?s :block/title ?m]
        [(not= ?m "Done")]]
```

Use `not` when you are excluding a pattern rather than a value:

```clojure
[:where [?b :block/title _]
        (not [?b :logseq.property/status _])]
```

---

## Full Example: Project Dashboard

> **Fixed (broken query).** The previous version of this example pulled blocks and
> then read `(:block/title r)` and `(:block/title (:logseq.property/status r))`
> inside `:view`. Both are `nil` there: `:view` receives normalized rows, so a
> pulled block has already collapsed to a bare UUID
> (`render_resource/query.cljs:15-46`). The dashboard rendered a column of empty
> `<div>`s. The working form binds the fields it needs as explicit `:find`
> columns, so the row arrives as a plain tuple:

```clojure
{:query [:find ?title ?status ?priority
         :where [?b :block/refs ?ref]
                [?ref :block/name "project"]
                [?b :block/title ?title]
                [?b :logseq.property/status ?s]
                [?s :block/title ?status]
                [(contains? #{"Todo" "Doing"} ?status)]
                [?b :logseq.property/priority ?p]
                [?p :block/title ?priority]]
 :view (fn [result]
         [:div
          (for [[title status priority] (take 10 (sort-by (fn [row] (nth row 2)) result))]
            [:div {:style {:padding "8px"
                           :border-left "3px solid #4cc9f0"
                           :margin-bottom "4px"}}
             [:strong status] " " [:em priority] " " title])])}
```

Note that this form requires every listed property to be present — a block with
no priority will not match. Use `get-else` or a `(or …)` clause if you need
blocks with missing values to appear.

---

## Debugging Queries

Always start here. One `pr-str` tells you whether you have UUIDs, scalars, or
tuples, which determines everything else your `:view` can do.

### Check Query Output

```clojure
:view (fn [result]
        [:pre (pr-str (first result))])
```

### Inspect the Whole Result

```clojure
:view (fn [result]
        [:pre.code (pprint result)])
```

`pprint` is injected into the SCI sandbox's `user` namespace, along with `sum`,
`average`, `parseFloat`, `isNaN`, `log` and `call-api`
(`src/main/frontend/extensions/sci.cljs:29-37`).

> **Removed:** The "Print All Keys" recipe, `(keys (first result))`, no longer
> works. Rows reaching `:view` are UUIDs, scalars or vectors — never maps — so
> `keys` returns `nil`. To see a block's attributes, name them as `:find`
> columns, or inspect them from a `:result-transform`, which does receive maps.

### Count Results

```clojure
:view (fn [result]
        [:div (str "Found: " (count result) " results")])
```

---

## Performance Tips

1. **Be specific**: Narrow conditions reduce scan time
2. **Limit early**: Use `:result-transform` to limit
3. **Pull only needed**: Use specific pull patterns, not `[*]`
4. **Index-friendly**: Use indexed attributes when possible. The full indexed set
   in `deps/db/src/logseq/db/frontend/schema.cljs:56-108` is `:block/parent`,
   `:block/order`, `:block/page`, `:block/link`, `:block/alias`,
   `:block/created-at`, `:block/updated-at`, `:block/name`, `:block/title` and
   `:block/journal-day`. Note that `:block/refs` and `:block/tags` are
   cardinality-many refs and are **not** `:db/index true`.
5. **Avoid the `property` rule in user-facing queries**: it is documented as slow
   in the ruleset itself (`deps/db/src/logseq/db/frontend/rules.cljc:245-246`).
   Prefer a direct datom pattern on the property ident.

---

## Common Errors

### "No results" from a date comparison

By far the most common cause. `:today` and the relative inputs are YYYYMMDD
integers; `:logseq.property/deadline` and `:logseq.property/scheduled` are epoch
milliseconds. The comparison is silently false. See
[Date Inputs](#date-inputs).

Also note that `:block/scheduled` and `:block/deadline` are **not** DB-graph
attributes at all — the idents are `:logseq.property/scheduled` and
`:logseq.property/deadline` (`deps/db/src/logseq/db/frontend/property.cljs:340-353`).

### "No results" from a ref property

Status and priority values are ref entities. Query them with a datom pattern on
the value entity — `[?s :block/title "Todo"]` or `[?s :db/ident :logseq.property/status.todo]` —
not with a predicate over the bound variable, which holds an entity id.

### `:view` renders nothing, or shows blanks

Your `:view` is probably treating rows as maps. Check the shape with
`(pr-str (first result))` first; see
[What a row looks like when it reaches you](#what-a-row-looks-like-when-it-reaches-you).
A `:view` that throws while being *defined* is swallowed and logged as
`:sci-eval-failed` (`src/main/frontend/extensions/sci.cljs:55-58`); one that
throws while being *called* renders a `:query/custom-view-error` message
(`src/main/frontend/components/query.cljs:44-49`).

### "Invalid custom query" / "Invalid renderer query resource"

The query map failed validation. `:query` must be a vector beginning with
`:find`; `:inputs` and `:rules`, if present, must be vectors; and no key outside
the allowed set may appear
(`src/main/frontend/worker/handler/query.cljs:243-252`,
`src/main/frontend/worker/handler/render_resource/query.cljs:68-108`).

### "Cannot read property"

- Check attribute names (`:block/title` not `:title`)
- Verify entity has the attribute

### "Syntax error"

- Balance brackets and parentheses
- Verify Datalog syntax
- A regex or `re-pattern` input that fails to compile is isolated: the query
  reports the error and returns `{:rows []}` rather than breaking the page
  (`render_resource/query.cljs:222-245`)

---

## See Also

- **Database Schema**: [schema-overview.md](../../reference/database/schema-overview.md)
- **View Functions**: [view-types.md](../content/view-types.md)
