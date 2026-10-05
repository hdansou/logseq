# Qualifier Search in the Command Palette

> **Last revised:** 2026-09-23
> **Verified against:** worktree `eng-documentation` @ `3c0cf1323e`. Every line reference below
> indexes that revision.
> **Evidence level:** design analysis from **source reading**, three **live Datalog queries** on
> the `cliworker` graph via the CLI (`50e1849-dirty`), and — since 2026-09-23 — an **experimental
> build that confirms the core theory end to end** (section 10). Palette observations not from that
> build come from `ddocs/how-to/content/search-your-graph.md`, verified live on 2026-09-23.
> **Status:** proposal, with stage 1 prototyped and confirmed working. Nothing is shipped.
>
> **Note on process:** `AGENTS.md` directs decision documents through `spec-dev-tool`. That binary
> is not on `PATH` and is not in this repo, so this page follows the existing
> `07-investigations/NNN-*.md` convention instead. Re-home it if the tool becomes available.

## The question

Could the command palette adopt GitHub-style search syntax — free terms plus `qualifier:value`
pairs, spaces meaning AND — so that a user can ask for things the current search cannot express?
The motivating example: *list all nodes tagged `#project`*.

## Verdict

**Feasible, and cheaper than it looks, but not as palette syntax.**

Three findings drive that:

1. The motivating example **already works**, as `(tags project)` in a query block, and every tag
   page already lists its objects in a filterable table. The gap is that Cmd+K cannot reach any of
   it. This is a surface problem wearing a syntax problem's clothes.
2. The composition engine GitHub syntax would need **already ships**, as a two-stage pipeline that
   every query block in the app runs today. It does not need to be designed, only reached.
3. The palette is the wrong container: its index cannot answer qualifiers, its filter model is
   single-select, and **mobile search shares none of its code**.

Recommendation: surface the existing machinery from Cmd+K with chips; reserve literal
`qualifier:value` text for the CLI and MCP surfaces, where it costs nothing and helps most.

## 1. Three engines, not one

The single most important thing to hold in mind is that "search" in a DB graph is three unrelated
systems.

| | Full-text search | Query DSL | View filters |
|---|---|---|---|
| Lives in | `worker/search.cljs` | `worker/query_dsl.cljs` | `deps/db/.../common/view.cljs` |
| Backed by | SQLite FTS5, trigram | Datalog over the whole graph | predicates over a materialized row set |
| Job | rank text matches | **find** rows | **narrow** rows |
| Free text | trigram + fuzzy + optional vector | `block-content`, an O(n) `includes?` | `input`, a title substring over the seed |
| Reachable from the other two | no | no | yes, via a seed set |

They never meet. In particular the FTS index is **not** reachable from the DSL or from view
filters, and the DSL is not reachable from the palette.

### What the search index actually contains

```sql
CREATE TABLE blocks (id TEXT PRIMARY KEY, title TEXT NOT NULL, page TEXT)
CREATE VIRTUAL TABLE blocks_fts USING fts5(id, title, page, tokenize="trigram")
```
`worker/search.cljs:45-56`. Three columns. No tags, no properties, no dates.

One trap: the indexed `title` is not the raw title. `block-search-title` (`:553-566`) expands
inline uuid refs to their target titles, folds in the parent-page path, the class `extends` chain,
aliases, and the journal day. So a block containing `#project` **does** index the literal word
*project* — indistinguishable from prose. Text search therefore looks as though it half-answers
`tag:project`, while producing false positives on every block that merely mentions the word and
false negatives on tags applied through the property UI. It cannot be used as a shortcut.

## 2. Why the palette cannot carry the syntax as-is

**Qualifier-only queries have nothing to search.** `search-blocks` returns early on blank input
(`worker/search.cljs:936`). `tag:project` with no free text would yield zero results, because
there is no text for FTS to match. Such queries must be answered by Datascript instead — a
different engine with different result semantics (exact and complete, versus ranked and
truncated).

**Post-filtering truncates.** The existing precedent is a warning, not a template. `:code-only?`
is a qualifier in all but name, implemented as a Datascript post-filter in `include-search-block?`
(`:819-838`). To show 20 code results it must fetch 300 candidates:

```clojure
;; cmdk/state.cljs:92-97
:codes (assoc nodes-base :limit 20
              ;; larger limit for code search since most of the results will be filtered out
              :search-limit 300 :code-only? true)
```
A tag matching 1% of nodes would need a 10,000-row fetch to fill ten slots. There is **no**
Datascript-side pre-filter anywhere in the path — no way to restrict FTS to a set of uuids.

**The parser would collide with live behaviour.** `get-match-input` (`:355-377`) wraps any query
containing punctuation in a quoted FTS5 prefix phrase. A colon is punctuation, so `tag:project`
today performs a literal phrase search. Any parser must run above the worker, before
`fuzzy/search-normalize` in `frontend.search/block-search:24`. Separately, `/` already opens the
filter list and reorders groups for *any* input containing it (`cmdk/core.cljs:146`), and `#`
already means exact-title lookup (`:467-472`). Colons are legal in page titles, so `Chapter 3:
Notes` would silently become a qualifier query.

**The filter model is single-select.** `{:group :nodes}`, one at a time (`cmdk/core.cljs:57-68`),
rendered as a row above the results rather than a chip (`search-only`, `:1348-1359`). GitHub
syntax is inherently composable AND. This is the one model change genuinely worth making.

**Mobile shares none of it.** The mobile search tab is native; `mobile/components/app.cljs:276`
renders `nil` for it, and `mobile/search.cljs:78-86` is one flat block list with no groups,
filters or debounce. Anything added to cmdk is desktop and web only.

## 3. The design: a two-stage pipeline that already ships

`get-view-data` accepts **`query-entity-ids`** — a caller-supplied seed row set — when the feature
type is `:query-result`, and then applies `filters` and `input` on top
(`deps/db/src/logseq/db/common/view.cljs:1176-1230`). The production caller is
`components/query/view.cljs:6-15`. Every query block in the app already runs
**DSL → result uuids → view filters**.

| | Stage 1 — **find** | Stage 2 — **narrow** |
|---|---|---|
| Engine | DSL compiled to Datalog, whole graph | predicates over the seed set |
| Vocabulary | `tags` `task` `priority` `property` (equality) `page` `[[ref]]` `and` `or` `not` `between` (created/updated) `sample` | `is` `is-not` `text-contains` `text-not-contains` `number-gt/lt/gte/lte` `between` `date-before/after` `before/after` `:empty` |
| Operators defined at | `worker/query_dsl.cljs:502-577` | `worker/handler/view.cljs:10-26`, evaluated at `common/view.cljs:172-286` |
| Persisted as | the block's query text | `:logseq.property.table/filters` on the same block (`property.cljs:519`) |
| Rebuilt from storage by | `from-dsl` (`handler/query/builder.cljs:176-196`) | the stored filters map |

The canonical qualifier vocabulary already exists and is translated, for the query builder UI:

```clojure
;; handler/query/builder.cljs:13-22
(def db-based-block-filters
  ["tags" "page reference" "property" "task" "priority" "page" "full text search" "between" "sample"])
```

### The rule that falls out

**Every query needs at least one stage-1 clause.** A stage-2-only query ("deadline before Friday")
has no seed set, so it would scan the graph. This is the same failure mode as an all-negation
query (below), and one guard covers both.

### Consequences

- **Persistence is already defined.** "Insert as query block" writes a block with DSL content plus
  hidden filter properties — exactly the shape query blocks already have. No new storage, and the
  builder can reopen its own output.
- **Tier is a property of the operator, not a user choice.** `status: In review` is stage 1;
  `points > 3` is stage 2; `deadline before Oct 1` is stage 2 *only because* `(between deadline …)`
  is broken. Never expose tiers in the UI, but the classifier must know them, because stage-2
  clauses cannot stand alone.
- **Prefer stage 1 whenever both could express something.** Stage 2 is linear in the seed: a broad
  `tag:project` returning 10,000 rows narrowed to 12 still materializes 10,000 entities.
  `filter-eids` (`common/view.cljs:917`) and `build-fast-filter-pred` (`:298`) soften this; they do
  not remove it.
- **Free text stops being a fork.** Make bare words a stage-2 `input` whenever any clause is
  present — then they are bounded by the seed and cost nothing. With no clauses, the palette stays
  exactly what it is today: ordinary FTS. The two engines never run at once, so there is no
  ranking mismatch to reconcile. This resolves the hardest open question in the design.

Worked example — `tag:project task:Todo,Doing -status:Draft deadline<Oct 1 "review"`:

```clojure
;; stage 1
(and (tags project) (task Todo Doing) (not (property status Draft)))
;; stage 2
{:or? false :filters [[:user.property/deadline :date-before <journal-node>]]}
:input "review"
```

## 4. The operator model

**AND is free** — implicit between clauses, the default on both sides (`:or? false`; DSL `and`).

**Most OR is already free, and this is easy to miss.** `(tags a b)`, `(task Todo Doing)` and
`(priority A B)` all compile to *sets* — an intra-qualifier OR needing no operator
(`worker/query_dsl.cljs:411-450`). Adopt GitHub's convention: **a repeated qualifier ORs, distinct
qualifiers AND.** That covers most real needs with no nesting.

An asymmetry to respect: `property` and `page` are **not** set-valued. `build-property` handles
arity 2 and 3 only (`:404-409`), and `build-page` takes one name (`:460-465`). So two `status:`
clauses cannot collapse and must become an explicit `(or (property status …) (property status …))`.

**NOT should be per-clause only.** `build-and-or-not` reorders `not` clauses *after* positive
binders inside an `and` (`:259-267`), because a negation needs something to bind against. Two
consequences:

- A query of only negations is unbounded. The UI must require at least one positive clause.
- "is empty" is a negation in disguise. `(property k)` with one argument compiles to
  `has-simple-query-property` — "has this property" (`:394-402`). There is no direct "is empty"
  form; it would be `(not (property k))`, which trips the same guard. Stage 2 has `:empty` as a
  first-class value, so empty-ness belongs there.

**Cross-group nesting — do not.** `(and A (or B C))` cannot be expressed by a flat chip row, and
the view layer cannot express it either (a single `:or?` flag, not a tree). GitHub has no general
OR. Make "Edit as DSL" the escape hatch, and keep the tree flat so `from-dsl` can round-trip it.

### What else the model has to account for

- **Tagged versus mentioned.** `:block/tags` values are folded into `:block/refs`
  (`outliner/pipeline.cljs:98-128`), so `(tags project)` and `[[project]]` are different queries.
  `#project` in DSL text compiles to a *ref* query, matching tagged nodes plus plain mentions plus
  property-value refs. Users conflate these constantly; they need two clearly labelled clauses.
- **Tag inheritance.** `(tags project)` pre-expands sub-tags via `get-structured-children`
  (`:443-449`). There is no form for "this tag only".
- **Date values.** Relative forms (`-7d`, `today`, `<% today %>`) are produced by `pre-transform`
  and `resolve-dynamic-template`, so a date picker must emit those, not a literal.
- **Non-boolean, scope-shaped forms.** `(sample N)` is a post-filter, not a clause (`:745-749`); it
  does not compose with and/or/not. Sort and group are view properties — DSL `sort-by` is parsed
  into an atom nothing ever fills (`:660`, `:684`), so it is silently dropped. Chips should not
  try to express ordering.
- **Re-query cost.** `query-watch-dependencies` (`:717-738`) marks only
  `#{:between :block-content :page}` attr-watch-safe; class and property clauses stay graph-level
  opaque. Live re-running on every chip toggle may be expensive on a large graph.

## 5. The interaction

An interactive prototype of three candidate surfaces exists as a private Claude artifact:
`https://claude.ai/artifact/FQTtCtEifxbubUt4U7Hgdg` (owner-only unless shared).

- **A — builder replaces the palette body.** One surface, but the palette stops being a palette
  mid-keystroke and Esc gains a third meaning. Most new UI; rebuilds a builder that already exists.
- **B — chips in the input** *(recommended)*. Typing a known qualifier offers it; Tab converts it
  to a chip; bare words stay free text. Dodges the colon collision entirely because nothing is
  parsed from a raw string. Each chip toggles `is` / `not`; repeated qualifiers OR; the compiled
  DSL is shown under the results; negating everything trips the guard and disables insertion.
- **C — hand off to the sidebar.** One palette row closes the dialog and opens the existing
  builder pre-filled in the right sidebar. Least new code: the sidebar already hosts a live search
  panel (`right_sidebar.cljs:97-106`) and the table already filters, sorts and groups.

Showing the **compiled DSL** is load-bearing rather than decorative: it is the proof that the chips
emit an existing, tested form, and it doubles as a teaching surface for the query language.
Clauses that trigger a full scan (`text:`) should say so in the UI.

### Decision: the query block is inserted at the current block

`Insert as query block` writes into **the block being edited when the palette was opened**, not a
new page and not the end of the current page.

The mechanism already exists. `move-blocks` is the same shape of problem — a palette invocation
that has to act on where the user came from — and it solves it by capturing state *before* opening
and passing a callback through (`handler/editor.cljs:1005-1022`):

```clojure
(state/get-edit-block)                      ; captured at call time, before the palette opens
…
(route-handler/go-to-search! :nodes
  {:action :move-blocks
   :blocks blocks
   :trigger (fn [chosen] … (move-blocks! blocks (:source-block chosen) …))})
```

Those args live in `:search/args`, are read back by `(get-action)` (`cmdk/core.cljs:41`), already
join `refresh-results-key` (`:550`), and can shape the search itself — `:move-blocks` sets
`:page-only? true` (`cmdk/state.cljs:107-109`). So the insert action needs no new plumbing: capture
the edit block at open, pass a `:trigger` that writes the compiled DSL into it.

Two consequences that must be handled:

- **No current block.** Opening Cmd+K from the journals feed, or without editing, leaves nothing to
  insert into. `move-blocks` falls back to the current selection and otherwise shows a warning
  (`:1022`). Here the sensible fallback is to disable the button and leave `⏎ Open results` as the
  only action, rather than silently inventing a location.
- **Flush before writing.** Opening the palette blurs the editor, and unflushed content has caused
  a navigation bug before (`f4d37904a6`, db-test#1250). The trigger must flush the editor before it
  writes, or the insert can race the block's own save.

## 6. Build versus reuse

**Reuse:** `->dsl` / `from-dsl` (`handler/query/builder.cljs:131-196`), the DSL parser and
compiler, `get-view-data` with `query-entity-ids`, `row-matched?` / `compile-filter-clause` /
`filter-eids`, `:logseq.property.table/filters`, the property-type → operator map
(`worker/handler/view.cljs:10-26`), the pickers at `components/query/builder.cljs:253-317`, and
`frontend.ui/auto-complete` (`ui.cljs:459-524`) for the dropdown.

**Build:** the chip UI (there is no pill CSS today), a tier classifier, the positive-clause guard,
and the palette entry point. `Tab` is currently unbound in `keydown-handler`
(`cmdk/core.cljs:1104-1179`), so it is free for accepting a qualifier. Any parsed state must join
`refresh-results-key` (`:550`) or results will go stale.

## 7. Blockers and defects found along the way

These are **source-read only** and must be revalidated against current master, and reproduced blind,
before filing — see `029-upstream-defects-from-ddocs-review.md` for why. A fourth entry was
withdrawn on 2026-09-24 after investigation disproved it; the row is kept struck through so the
verdict is not rediscovered as new.

| What looks wrong | Where | Impact here |
|---|---|---|
| `(between deadline …)` / `(between scheduled …)` silently return an empty query. `get-timestamp-property` strips the namespace with `name`, so even `:logseq.property/deadline` degrades to `:deadline`, and the fallback `case` knows only `created-at` / `updated-at` | `deps/db/.../query_dsl.cljs:55-71` | Date clauses have **no working stage-1 target**. Fix first, or every date chip falls to stage 2 unnecessarily. Already proven in `029` (`:82`) |
| DSL `sort-by` is parsed into an atom no builder ever fills, so `(sort-by …)` is dropped without an error | `worker/query_dsl.cljs:660,684` | Chips cannot offer sorting; it belongs to the view |
| `handler/search.cljs` resolves a page to a `:db/id` integer and assigns `:page (str db-id)`, but the index column holds a page **uuid** string | `handler/search.cljs:13-18` vs `worker/search.cljs:598` | The plugin API's page-scoped search (`logseq/api.cljs:186-191`) should never match. The cmdk path is unaffected — it passes a uuid (`cmdk/state.cljs:106`) |
| ~~`(task …)` returns 0 against data that matches~~ — **withdrawn 2026-09-24**, see section 10, correction 3 | — | Not a defect. A test built on the realistic data shape passes every form |

Separately, a palette wart worth fixing regardless: typing `#project` offers *"Create tag called
'project'"* **above** the existing tag, because the existence check compares the raw input
including the `#` (`cmdk/core.cljs:141-147`). Any new tag affordance would land under an offer to
duplicate the thing being looked for.

## 8. Recommended sequence

1. **A tag row in the palette.** When the input matches a tag, show *"View all nodes tagged
   #project (42)"*. One `:avet` datom scan via `get-class-object-ids`
   (`deps/db/.../class.cljs:312-329`), inheritance included. Answers the motivating example, works
   on every platform, needs no new concepts. Fix the Create-row ordering first.
2. **Chips (variant B) compiling to the two-stage pipeline.** The real capability gain.
3. **Literal `qualifier:value` syntax on the CLI and MCP.** This is where GitHub-style text
   belongs: agents parse strings well, and there is no autocomplete, i18n or mobile split to pay
   for.

Explicitly rejected: porting GitHub syntax into the palette wholesale; and indexing tags into the
FTS title, which would pollute trigram and fuzzy matching and break the exact-title pass
(`worker/search.cljs:477-489`).

## 9. What was verified live

Run 2026-09-23 with `logseq query` (CLI `50e1849-dirty`, built 2026-09-21) against `cliworker`,
read-only. These confirm the stage-1 data model behind the chips; they do **not** exercise the DSL
parser, which lives in the worker and needs a `/Query` block in the app.

| Query | Result |
|---|---|
| `[?t :block/tags :logseq.class/Tag] [?b :block/tags ?t]`, counted per tag | 22 tags; `Property` 137, `Code` 42, `Page` 34, `Journal` 28, `Tag` 27, `Task` 24 |
| `[?b :block/tags :logseq.class/Task]` joined to `:logseq.property/status` | 24 nodes, each with a real status ident (`…status.todo`, `.doing`, `.done`, `.in-review`, `.backlog`, `.canceled`) |
| The same, restricted to `#{…status.todo …status.doing}` | 15 nodes — OR within a qualifier, AND across, confirmed |

Not verified: the DSL parser end to end, any palette UI behaviour, the two-stage pipeline under
load, and all three defects in section 7.

## 10. What the experimental build proved (2026-09-23)

Branch `experiment/palette-qualifier-chips` (worktree `../logseq-palette-chips`), off
`upstream/master` @ `f4d37904a6`. A vertical slice: parse `qualifier:value` from the palette input,
compile through `->dsl`, run via `frontend.db.query-dsl/query`, render as a new `:query` group.
Deliberately no chip UI and no stage-2 clauses — the point was to test the pipeline, not the
interaction. Task plan and full findings in that worktree's `TASKS.md`.

**Confirmed live** in Chrome against graph `demolocal4` on the dev build:

| Input | Compiled | Result |
|---|---|---|
| `tag:Task` | `(tags [[Task]])` | 14, Query group renders above Nodes |
| `tag:Task tag:Card` | `(tags [[Task]] [[Card]])` | multi-value OR compiles and runs |
| `tag:Task -task:Done` | `(and (tags [[Task]]) (not (task "Done")))` | negation compiles |
| `-tag:Task` | — | no query runs; the positive-clause guard holds |
| `Chapter 3: Notes`, `foo:bar` | — | no Query group; colons and unknown qualifiers stay free text |

So **the palette can execute a DSL query and render its results** — the assumption everything else
rested on. Three corrections to this document follow.

### Correction 1: `->dsl` could not emit a multi-value form

Section 3 claimed the chips could reuse `->dsl` unchanged. Not true. `->dsl*` carried a branch that
**shadowed** the general one and read only the second element:

```clojure
(and (vector? f) (= :tags (keyword (first f))))
[(symbol :tags) (->page-ref (second f))]      ; drops every value after the first
```

`[:tags "project" "infra"]` silently became `(tags [[project]])` — the repeated-qualifier OR that
the whole operator model rests on. The later `#{:page :tags}` clause is strictly more general and
identical for one value, so the fix is to delete the earlier one. No UI could reach the broken path
before, because the query builder only ever emits single-value tags, which is why it went unnoticed.
**The two-stage-pipeline claim survives; "reuse `->dsl` unchanged" does not.**

### Correction 2: the query API returns tuples, not blocks

`execute-query` ends in `query-result-tuples`, so a block query yields `[[block] [block] …]`.
Consuming it as a seq of blocks silently yields nothing — the first cut of the loader dropped all
14 results while the query itself was succeeding. Anything calling
`frontend.db.query-dsl/query` must flatten a level first.

### Correction 3: a suspected `(task …)` defect, investigated and **withdrawn**

Worth recording as a method note, because it nearly became a wrong bug report.

`(task …)` returned 0 for every value on graph `demolocal4` while `(tags [[Task]])` returned 14 and
raw Datalog found three Done tasks. Ruled out one by one: the nodes all carry real statuses;
`:logseq.property/status` is `public? true`; the closed value carries `:block/title "Done"`, exactly
what `ref->val` binds; quoting is irrelevant because `build-task` applies `(comp capitalize-all
name)`; and the rule chain and `rules-dependencies` map are complete. It looked like a solid defect.

Two things stopped the filing:

1. `query_dsl_test.cljs:440` already asserts `(task done)` and friends — and **passes**. So the form
   is not universally broken, which no "it's broken" report could survive.
2. A new test built on the shape a real graph uses — a `:logseq.class/Task` tag plus a closed-value
   `:logseq.property/status` ref, rather than the existing fixture's `DONE b1` markers — **passes
   every form**, including `(and (tags Task) (task done))` and the negated variant.

So the DSL is correct and nothing was filed. What remains is graph-specific to `demolocal4` and
does not affect this design. Test added as `task-queries-on-tagged-nodes-with-status`; it is worth
keeping because that construction had no coverage.

**Method note:** three independent checks (source reading, a live repro, ruling out four causes)
all pointed at a defect that does not exist. The thing that settled it was building the smallest
test that could fail. Cf. `029`, where 7 of 11 findings changed verdict on revalidation.

## Sources

- `src/main/frontend/worker/search.cljs:45-61,355-377,467-472,477-489,528-546,553-566,584-607,611-638,811-838,900-1006` index schema, query rewriting, retrieval legs, post-filters
- `src/main/frontend/search.cljs:21-27`, `src/main/frontend/handler/search.cljs:13-42` frontend entry points
- `src/main/frontend/components/cmdk/core.cljs:51-191,478-482,533-555,732-782,1104-1179,1234-1283,1348-1359` palette groups, ordering, filters, keys
- `src/main/frontend/components/cmdk/state.cljs:60-111` search options per filter group
- `src/main/frontend/worker/query_dsl.cljs:246-281,316-327,358-409,411-458,460-500,502-577,649-686,717-749` the DSL parser and compiler
- `deps/db/src/logseq/db/frontend/query_dsl.cljs:10-42,55-71` `pre-transform`, `get-timestamp-property`
- `deps/db/src/logseq/db/common/view.cljs:172-286,298-330,700-741,917-930,1176-1230` filter evaluation and the seeded row set
- `src/main/frontend/worker/handler/view.cljs:10-26,47-66` operators by property type
- `src/main/frontend/handler/query/builder.cljs:13-22,131-196` the qualifier vocabulary and the DSL bridge
- `src/main/frontend/handler/editor.cljs:1005-1022` `move-blocks`, the precedent for a palette action that writes back to the originating block
- `src/main/frontend/components/query/view.cljs:6-15` the production two-stage caller
- `deps/db/src/logseq/db/frontend/class.cljs:279-330` class objects by `:avet` datom scan
- `deps/db/src/logseq/db/frontend/schema.cljs:73-74`, `deps/outliner/src/logseq/outliner/pipeline.cljs:98-128` `:block/tags` and its fold into `:block/refs`
- `src/main/frontend/ui.cljs:459-524`, `src/main/frontend/components/editor.cljs:607-656` reusable autocomplete
- `src/main/mobile/search.cljs:10-24,78-86`, `src/main/mobile/components/app.cljs:276` the mobile gap
- `dz/docs/ddocs/how-to/content/search-your-graph.md` live palette behaviour, verified 2026-09-23
- `dz/docs/07-investigations/029-upstream-defects-from-ddocs-review.md:82` the `(between deadline …)` proof
