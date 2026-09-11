---
title: 'Build ASCII and SVG Charts in Logseq Queries with Hiccup'
description: 'Learn to render ASCII bar charts and full SVG line charts directly inside Logseq DB advanced queries using the :view function and Hiccup. Then make them reusable across your graph via config.edn.'
status: draft
difficulty: intermediate
timeToComplete: '45 minutes'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '0.11.x (DB graphs only)'
platforms: [desktop, web]
prerequisites:
  - '[How-To: Advanced Queries](../../how-to/queries/advanced-queries.md)'
relatedDocs:
  howTo:
    - '[../../how-to/queries/advanced-queries.md](../../how-to/queries/advanced-queries.md)'
  reference:
    - '[../../reference/configuration/config-edn-db.md](../../reference/configuration/config-edn-db.md)'
---

# Build ASCII and SVG Charts in Logseq Queries with Hiccup

> **Last revised:** 2026-09-09 (verified against `be800f1711`)
> **What you'll learn**: How to use the `:view` function in advanced queries to render aggregation results as ASCII bar charts and SVG line charts, and how to register reusable chart templates in `config.edn`.

## Introduction

Logseq DB advanced queries can return any datalog result — including aggregations like `(count ?b)` or property values across pages — but the default block renderer only displays block entities. To visualize aggregations, time series, or any non-block data, you need to supply a custom `:view` function that returns Hiccup. This tutorial walks you through three progressively richer charts and ends with a reusable template stored in `config.edn`.

This tutorial is for **DB graphs only**. File-based graphs use a different query syntax and do not support the same `:view` mechanism.

## Prerequisites

Before starting this tutorial, make sure you have:

- [ ] Logseq DB graph created ([Installation Guide](../getting-started/01-installation.md))
- [ ] Comfort writing basic advanced queries (`:find` / `:where` / `:in`)
- [ ] At least one tagged page or class with some data (we'll create one if needed)

## Background: What is `:view`?

When you write an advanced query, the result is passed to a renderer. By default, Logseq tries to render each row as a block. For anything else, you supply your own `:view`:

```clojure
{:query [:find ?status (count ?b)
         :where ...]
 :view (fn [r] [:pre.code (pr-str r)])}
```

`:view` receives the normalized query result (`r`) and must return a [Hiccup](https://github.com/weavejester/hiccup) vector — a Clojure data structure that maps to HTML/SVG. Logseq evaluates the function in an [SCI](https://github.com/babashka/sci) sandbox (`src/main/frontend/components/query.cljs:148-149`, `src/main/frontend/extensions/sci.cljs:50-58`).

> 💡 **Why `:view` is required for aggregations**: a query like `[:find ?status (count ?b)]` returns scalar tuples, not block entities. Without a `:view`, the renderer falls back to a plain `<ul>` of stringified rows (`src/main/frontend/components/query.cljs:75-85`) — readable, but not a chart.

> ⚠️ **What `r` actually contains.** Queries run in the DB worker and every row is passed through `normalize-query-row` before it reaches your `:view` (`src/main/frontend/worker/handler/render_resource/query.cljs:40-46,195-202`). Two consequences:
>
> - **Block cells become UUIDs, not entity maps.** A `(pull ?b [*])` row arrives in `:view` as a bare `:block/uuid`, so `(:block/title r)` is `nil` there. If you need block fields inside `:view`, `:find` them as explicit scalar columns instead of pulling.
> - **Multi-column rows stay as tuples.** A 2-column `:find` yields a vector of 2-element vectors. See the test `query-resource-preserves-scalar-tuples-test` (`src/test/frontend/worker/handler/render_resource_test.cljs:2021-2038`), which asserts `{:rows [["Jan 1st, 2020" 20200101]]}`. A single-column `:find` is unwrapped to the bare value.

## Step 1: Render the Raw Result

Create a new block, add the `query::` property, and paste this:

````text
- query::
  ```clojure
  {:query [:find ?status (count ?b)
           :where
           [?b :logseq.property/status ?s]
           (or [?s :db/ident :logseq.property/status.todo]
               [?s :db/ident :logseq.property/status.doing]
               [?s :db/ident :logseq.property/status.done])
           [?s :block/title ?status]]
   :view (fn [r] [:pre.code (pr-str r)])}
  ```
````

**Expected result**: a code block showing the raw result, e.g.

```text
[["Todo" 5] ["Doing" 2] ["Done" 11]]
```

> **Removed:** Earlier revisions of this page claimed that Logseq DB *flattens* aggregation results into a single alternating sequence (`("Todo" 5 "Doing" 2 …)`) and that every `:view` therefore needs `(partition N r)`. That is no longer true. Since the worker render-resource pipeline landed (`src/main/frontend/worker/handler/render_resource/query.cljs`, added 2026-08-04), each row keeps its tuple shape — `normalize-query-row` returns the tuple verbatim whenever it has more than one column (lines 40-46). Adding `(partition 2 r)` on top of that regroups *rows*, not columns, and silently produces a broken chart. Destructure the tuples directly.

Always confirm the shape with `(pr-str r)` before writing chart code — it takes ten seconds and it is the one step that catches an unwrapped-vs-tuple mistake.

## Step 2: Build an ASCII Bar Chart

Now render that count data as a horizontal text bar chart:

````text
- query::
  ```clojure
  {:query [:find ?status (count ?b)
           :where
           [?b :logseq.property/status ?s]
           (or [?s :db/ident :logseq.property/status.todo]
               [?s :db/ident :logseq.property/status.doing]
               [?s :db/ident :logseq.property/status.done])
           [?s :block/title ?status]]
   :view (fn [r]
           (let [rows    (vec r)
                 max-cnt (apply max (mapv (fn [[_ c]] c) rows))
                 bar-max 30
                 label-w (apply max (mapv (fn [[s _]] (count s)) rows))
                 pad     (fn [s n] (str s (apply str (repeat (- n (count s)) " "))))
                 bar     (fn [n] (apply str (repeat (int (* bar-max (/ n max-cnt))) "█")))]
             [:pre.code
              (apply str
                (for [[status cnt] rows]
                  (str (pad status label-w) " │ " (bar cnt) " " cnt "\n")))]))}
  ```
````

**Expected result**: a fixed-width text chart that looks like:

```text
Todo  │ █████████████ 5
Doing │ █████ 2
Done  │ ██████████████████████████████ 11
```

### How it works

| Step              | What it does                                                                             |
| ----------------- | ---------------------------------------------------------------------------------------- |
| `(vec r)`         | `r` is already a sequence of `["Todo" 5]` tuples; `vec` only makes indexing cheap. Do **not** `partition` it. |
| `max-cnt`         | Finds the largest count so we can scale bars proportionally.                             |
| `bar`             | Builds a string of `█` characters proportional to the count, capped at `bar-max` (30).   |
| `pad`             | Right-pads each label so the bar columns line up.                                        |
| `[:pre.code …]`   | Hiccup vector for a `<pre>` block with the `code` class — preserves monospace alignment. |

## Step 3: Build an SVG Line Chart

ASCII is great for terminals; for actual visualization, SVG is dramatically clearer. Logseq's `:view` accepts SVG hiccup directly.

For this step you need a tagged page with some numeric properties. If you don't have one, create a tag (class) `habit-tracker` with two number properties (`running`, `walking`) and add a few journal entries with values.

````text
- query::
  ```clojure
  {:query [:find ?title ?day ?running ?walking
           :where
           [?p :block/tags ?t]
           [?t :block/title "habit-tracker"]
           [?p :block/journal-day ?day]
           [?p :block/title ?title]
           [?p :user.property/running ?ru]
           [?ru :logseq.property/value ?running]
           [?p :user.property/walking ?wa]
           [?wa :logseq.property/value ?walking]]
   :view (fn [r]
           (let [rows   (sort-by (fn [x] (nth x 1)) < (vec r))
                 n      (count rows)
                 days   (mapv (fn [x]
                                (let [d (str (nth x 1))]
                                  (str (subs d 4 6) "-" (subs d 6 8))))
                              rows)
                 runs   (mapv (fn [x] (nth x 2)) rows)
                 walks  (mapv (fn [x] (nth x 3)) rows)
                 max-v  (apply max (concat runs walks))
                 w 720  h 400  pad-l 55  pad-b 55  pad-t 50  pad-r 130
                 plot-w (- w pad-l pad-r)
                 plot-h (- h pad-t pad-b)
                 x-of   (fn [i] (+ pad-l (* (/ i (max 1 (dec n))) plot-w)))
                 y-of   (fn [v] (+ pad-t (* (- 1 (/ v (* 1.0 max-v))) plot-h)))
                 pts    (fn [vs]
                          (apply str
                            (interpose " "
                              (for [i (range (count vs))]
                                (str (x-of i) "," (y-of (nth vs i)))))))
                 lx     (- w pad-r -10)
                 c-run  "#2563eb"
                 c-walk "#dc2626"]
             [:svg {:width w :height h}
              [:text {:x (/ w 2) :y 22 :text-anchor "middle"
                      :fill "currentColor" :font-size 16 :font-weight "bold"}
               "Habit tracker"]
              (for [i (range 5)]
                (let [frac (/ i 4)
                      v    (int (* max-v frac))
                      y    (+ pad-t (* (- 1 frac) plot-h))]
                  [:g {:key (str "y" i)}
                   [:line {:x1 pad-l :y1 y :x2 (- w pad-r) :y2 y
                           :stroke "currentColor" :stroke-opacity 0.12}]
                   [:text {:x (- pad-l 8) :y (+ y 4) :text-anchor "end"
                           :fill "currentColor" :font-size 11} (str v)]]))
              [:line {:x1 pad-l :y1 (- h pad-b) :x2 (- w pad-r) :y2 (- h pad-b) :stroke "currentColor"}]
              [:line {:x1 pad-l :y1 pad-t :x2 pad-l :y2 (- h pad-b) :stroke "currentColor"}]
              (for [i (range n)]
                [:text {:key (str "x" i) :x (x-of i) :y (+ (- h pad-b) 20)
                        :text-anchor "middle" :fill "currentColor" :font-size 12}
                 (nth days i)])
              [:polyline {:fill "none" :stroke c-run  :stroke-width 2.5 :points (pts runs)}]
              [:polyline {:fill "none" :stroke c-walk :stroke-width 2.5 :points (pts walks)}]
              (for [i (range n)]
                [:g {:key (str "pt" i)}
                 [:circle {:cx (x-of i) :cy (y-of (nth runs i))  :r 4 :fill c-run}]
                 [:circle {:cx (x-of i) :cy (y-of (nth walks i)) :r 4 :fill c-walk}]
                 [:text {:x (x-of i) :y (- (y-of (nth runs i)) 10)
                         :text-anchor "middle" :fill c-run
                         :font-size 11 :font-weight "bold"} (str (nth runs i))]
                 [:text {:x (x-of i) :y (+ (y-of (nth walks i)) 18)
                         :text-anchor "middle" :fill c-walk
                         :font-size 11 :font-weight "bold"} (str (nth walks i))]])
              [:rect {:x lx :y (+ pad-t 4)  :width 14 :height 4 :fill c-run}]
              [:text {:x (+ lx 20) :y (+ pad-t 10) :fill "currentColor" :font-size 12} "running"]
              [:rect {:x lx :y (+ pad-t 24) :width 14 :height 4 :fill c-walk}]
              [:text {:x (+ lx 20) :y (+ pad-t 30) :fill "currentColor" :font-size 12} "walking"]]))}
  ```
````

> ⚠️ **Never hardcode a user ident.** Every user-created property *and class* ident gets an 8-character random suffix when it is created — `"-"` plus one letter plus a 7-character nano-id (`deps/db/src/logseq/db/frontend/db_ident.cljc:82-90`, reached via `create-user-property-ident-from-name` at `deps/db/src/logseq/db/frontend/property.cljs:854-859` and `create-user-class-ident-from-name` at `deps/db/src/logseq/db/frontend/class.cljs:178-186`). So your habit-tracker tag is `:user.class/habit-tracker-a3Kf9Qz1`, **not** `:user.class/habit-tracker`. Matching a class by `[?t :block/title "habit-tracker"]` — as above — is portable; matching by `:db/ident` is not. Replace `:user.property/running` / `:user.property/walking` with your graph's real idents, which you can list with:
>
> ```clojure
> [:find ?ident :where [?p :db/ident ?ident]
>  [(str ?ident) ?s] [(clojure.string/starts-with? ?s ":user.property/")]]
> ```
>
> (The suffix is skipped only for `plugin.class.*` namespaces, and for test runs with `$LOGSEQ_STABLE_IDENTS` set.)

> ⚠️ **Number properties are entity refs**: `:number` is in `original-value-ref-property-types` (`deps/db/src/logseq/db/frontend/property/type.cljs:52-56`), so a direct `[?p :user.property/running ?x]` binds `?x` to a value-entity's `:db/id`, **not** the number. You must add a deref step `[?x :logseq.property/value ?n]` to get the actual number. Tip: if results show suspiciously consecutive small integers, that's the giveaway you're looking at eids.

**Expected result**: a clean two-line chart with axes, gridlines, dot markers, value labels, and a legend identifying running and walking.

### How the SVG works

| Component                               | Purpose                                                                                                                     |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `[:svg {:width :height}]`               | The SVG canvas.                                                                                                             |
| `[:text]` for title                     | Hand-positioned at `(w/2, 22)` with `text-anchor "middle"`.                                                                 |
| `(for [i (range 5)] …)`                 | Generates 5 horizontal gridlines and y-axis tick labels at evenly spaced fractions (0%, 25%, 50%, 75%, 100% of `max-v`).    |
| `[:polyline {:points "x1,y1 x2,y2 …"}]` | Connects all data points for one series with straight line segments. The `:points` string is computed via the `pts` helper. |
| `[:circle]` markers                     | One dot per data point per series — makes individual values readable.                                                       |
| Bold value labels                       | Running labels go _above_ the dot, walking _below_, so they don't collide when values cross.                                |
| Legend `[:rect]` + `[:text]`            | Hand-placed in the right margin (`pad-r 130` reserves the space).                                                           |

## Step 4: Make Charts Reusable with `:query/views`

Re-pasting that 50-line `:view` into every chart query gets old. Logseq DB lets you register named view functions in `config.edn` under `:query/views` and reference them by keyword.

### Register a named view

Open your graph's `config.edn` (Settings → Edit config.edn) and add:

```clojure
:query/views
{:pprint
 (fn [r] [:pre.code (pprint r)])

 :ascii-bars
 (fn [r]
   (let [rows    (vec r)
         max-cnt (apply max (mapv (fn [[_ c]] c) rows))
         bar-max 30
         label-w (apply max (mapv (fn [[s _]] (count s)) rows))
         pad     (fn [s n] (str s (apply str (repeat (- n (count s)) " "))))
         bar     (fn [n] (apply str (repeat (int (* bar-max (/ n max-cnt))) "█")))]
     [:pre.code
      (apply str
        (for [[status cnt] rows]
          (str (pad status label-w) " │ " (bar cnt) " " cnt "\n")))]))}
```

Save and reload the graph.

### Reference by name

In any advanced query, use the keyword instead of an inline function:

````text
- query::
  ```clojure
  {:query [:find ?status (count ?b)
           :where
           [?b :logseq.property/status ?s]
           (or [?s :db/ident :logseq.property/status.todo]
               [?s :db/ident :logseq.property/status.doing]
               [?s :db/ident :logseq.property/status.done])
           [?s :block/title ?status]]
   :view :ascii-bars}
  ```
````

**Expected result**: the same ASCII bar chart as Step 2, but the `:view` is just `:ascii-bars` — clean and reusable.

> 💡 **How it works**: when Logseq sees `:view` is a keyword, it looks up `[:query/views <keyword>]` in `config.edn`, stringifies the function with `pr-str`, then evaluates it with SCI (`src/main/frontend/components/query.cljs:148-149`). The function still runs in the same sandbox as inline `:view`s — no closure capture, no external `require`. `config.edn` validates `:query/views` as `[:map-of :keyword [:sequential any?]]` (`src/main/frontend/schema/handler/common_config.cljc:34-36`), so each value must be a *list* form such as `(fn [r] …)`.
>
> ⚠️ Unlike `:result-transform`, a missing `:query/views` entry is **not** reported. `:view` simply resolves to `nil` and the query falls back to the default renderer, so a typo in the keyword looks like "my chart disappeared". `:result-transform` by contrast throws `"Missing query result transform"` (`src/main/frontend/components/query.cljs:134-140`).
>
> 💡 `pprint` is one of the few helpers injected into the SCI `user` namespace, alongside `sum`, `average`, `parseFloat`, `isNaN`, `log` and `call-api` (`src/main/frontend/extensions/sci.cljs:29-37`) — which is why the `:pprint` view above works.

### Tip: parameterize via the result shape

Since registered views can't take config arguments (only the query result `r`), design them around a **result-shape contract**. For example, a generic ASCII bar chart assumes the result is a sequence of `[label value]` tuples from `[:find ?label (count ?b)]`. Any query producing that shape can use `:view :ascii-bars`.

For richer parameterization, design your view to accept a "sentinel" first row — for example a polymorphic chart that expects `["line" "My title"]` as row 0 and switches on it.

## Findings: New Capabilities

- **Queries now execute in the DB worker behind a render-resource contract.** `src/main/frontend/worker/handler/render_resource/query.cljs` (added 2026-08-04) validates the query spec, runs it, applies `:result-transform`, and normalizes every row before it crosses back to the UI. This is what changed the shape `:view` receives.
- **`:result-transform` and `:view` no longer see the same data.** `:result-transform` runs in the worker on the *pre-normalization* rows — pulled maps (or raw ids and scalars for a non-pull `:find`), straight from `d/q` (`render_resource/query.cljs:181-202`; `worker/handler/query.cljs:243-261`). `:view` runs in the UI on the *post-normalization* rows, where block cells have already collapsed to UUIDs. Do row-level filtering and sorting in `:result-transform`; do presentation in `:view`.
- **A `:result-transform` must return a sequential or set**, or the worker fails the resource with `"Query result transform must return rows"` (`render_resource/query.cljs:188-191`).
- `config.edn` supports both `:query/views` and `:query/result-transforms` as keyword-addressable functions (`src/main/frontend/schema/handler/common_config.cljc:34-38`), so you can keep chart rendering and post-processing reusable and centralized.
- User property **and class** idents are generated with random uniqueness suffixes in DB graphs (`deps/db/src/logseq/db/frontend/db_ident.cljc:82-90`); query snippets should always discover idents from the graph, or match on `:block/title`, instead of hardcoding guessed names.

## Verify Your Work

You should now be able to:

1. Run an aggregation query and see the raw tuple result via `:view (fn [r] [:pre.code (pr-str r)])`.
2. Render an ASCII bar chart by destructuring each `[label count]` tuple into `[:pre.code …]`.
3. Render a full SVG line chart with axes, polylines, dots, value labels, and a legend.
4. Register `:ascii-bars` in your `config.edn` under `:query/views` and reference it as `:view :ascii-bars` in any query block.

## What You've Learned

In this tutorial, you:

- ✅ Used `:view` to override Logseq's default block renderer.
- ✅ Learned that multi-column results arrive as tuples, that single-column results are unwrapped, and that block cells arrive as UUIDs.
- ✅ Built a hand-rendered ASCII bar chart with `:pre.code` and `█` characters.
- ✅ Built a full SVG line chart with axes, polylines, dots, gridlines, and a legend.
- ✅ Registered reusable views in `config.edn` under `:query/views` and called them by keyword.

## Next Steps

- **Build a reusable SVG line chart**: extract the Step 3 chart into `:query/views` under `:line-chart` and design the result-shape contract you'll standardize on.
- **Add interactivity**: SVG supports `:on-click`, hover effects via `:on-mouse-over`, and tooltips. Try adding a hover ring around each data point.
- **Use a charting library**: if you outgrow hand-rolled SVG, build a Logseq plugin that exposes a chart component using a JS library like Recharts or D3 — this is the right path once you need axes auto-rendering, tick formatting, animations, etc.

## Troubleshooting

### Query runs but shows a plain bullet list instead of a chart

**Problem**: `:view` is missing (or its keyword did not resolve), so the renderer falls back to stringifying each row into a `<ul>` (`src/main/frontend/components/query.cljs:75-85`).

**Solution**: add a `:view`. Start with `(fn [r] [:pre.code (pr-str r)])` to see the exact shape, then build up. If you used `:view :some-keyword`, check the key really exists under `:query/views` in `config.edn` — a missing key resolves to `nil` silently.

### Chart shows one giant bar, or `nth` / destructuring errors

**Problem**: the view is calling `(partition N r)` on a result that is already a sequence of tuples. `(partition 2 [["Todo" 5] ["Doing" 2] ["Done" 11]])` yields `((["Todo" 5] ["Doing" 2]) (["Done" 11]))`, so `[status cnt]` destructures a *pair of rows* rather than a label and a count.

**Solution**: remove the `partition`. Destructure each row directly. See the "What `r` actually contains" note above.

### Block renders blank with no console error

> **Removed:** The previous claim that SCI silently drops idioms like `map-indexed`, `#()` reader macros, and keyword-as-function calls is stale. `frontend.extensions.sci/eval-string` is a plain `sci/eval-string` with a small `user` namespace merged in (`src/main/frontend/extensions/sci.cljs:39-58`); standard SCI supports all three.

**Current guidance**: `eval-string` catches every exception, logs `:sci-eval-failed`, and returns `nil` (`src/main/frontend/extensions/sci.cljs:55-58`) — so a view that throws at *definition* time disappears without a visible error. Filter the browser console for `sci-eval-failed`. A view that throws at *call* time is caught separately and rendered as a `:query/custom-view-error` message (`src/main/frontend/components/query.cljs:44-49`).

> **Removed:** The troubleshooting entry for `nth not supported on this type Number()` — attributed to a pre-render `grouped-by-page-result?` check calling `first` on a numeric first column — no longer applies. No such function exists anywhere in `be800f1711`, and the current pipeline routes rows through `query-tuples` / `normalize-query-row` (`render_resource/query.cljs:151-159,40-46`), which handle numeric columns fine. You no longer need a dummy leading `?title` column.

### `:title` key is ignored

**Problem**: for regular advanced query blocks, the top-level `:title "..."` key is never rendered. The title header is drawn only on *built-in* queries (`src/main/frontend/components/query.cljs:166-177`); ordinary query blocks take the `[:div.bd …]` branch, which has no title. File-graph queries that carried a `:title` had it hoisted into the block's own `:block/title` on import, and the key stripped from the map (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1159-1167,1406-1410`).

**Solution**: put your heading inside `:view` as `[:h3 "..."]` or `[:text {:x ... :y ...} "..."]` for SVG, or as ordinary text in the query block itself.

### `Query Error` on an apparently valid query

**Problem**: `:limit N` was placed inside the `:find` vector. The query vector is handed straight to `d/q` (`src/main/frontend/worker/handler/query.cljs:261`), and Datascript's find-spec parser rejects it.

**Solution**: drop `:limit` and bound results either via `:where` filters, via `:result-transform` with `(take N rows)`, or by taking a slice inside `:view`.

### Number property values look like consecutive small integers (1095, 1093, …)

**Problem**: number properties in Logseq DB are stored as **value entities**, not inline. A direct `[?p :user.property/foo ?x]` binds `?x` to the entity's `:db/id`.

**Solution**: add a deref step:

```clojure
[?p :user.property/foo ?fv]
[?fv :logseq.property/value ?actual-number]
```

Use `?actual-number` in `:find`.

---

_Last updated: 2026-09-09 (audited against `be800f1711`) | [Report an issue](https://github.com/logseq/logseq/issues)_
