---
title: 'Build ASCII and SVG Charts in Logseq Queries with Hiccup'
description: 'Learn to render ASCII bar charts and full SVG line charts directly inside Logseq DB advanced queries using the :view function and Hiccup. Then make them reusable across your graph via config.edn.'
status: draft
difficulty: intermediate
timeToComplete: '45 minutes'
lastVerified: '2026-04-21'
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

`:view` receives the raw query result (`r`) and must return a [Hiccup](https://github.com/weavejester/hiccup) vector — a Clojure data structure that maps to HTML/SVG. Logseq evaluates the function in an [SCI](https://github.com/babashka/sci) sandbox.

> 💡 **Why `:view` is required for aggregations**: a query like `[:find ?status (count ?b)]` returns tuples, not block entities. The default renderer can't display them — it ignores them. Without `:view`, your query produces no visible output.

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
("Todo" 5 "Doing" 2 "Done" 11)
```

> ⚠️ **Important**: Logseq DB **flattens** aggregation results into a single sequence of alternating values, **not** a sequence of tuples. Your `:view` must use `(partition N r)` to re-pair them, where N is the number of `:find` variables.

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
           (let [rows    (partition 2 r)
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
| `(partition 2 r)` | Re-pairs the flat result `("Todo" 5 "Doing" 2 …)` into `(["Todo" 5] ["Doing" 2] …)`.     |
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
           [?t :db/ident :user.class/habit-tracker]
           [?p :block/journal-day ?day]
           [?p :block/title ?title]
           [?p :user.property/running ?ru]
           [?ru :logseq.property/value ?running]
           [?p :user.property/walking ?wa]
           [?wa :logseq.property/value ?walking]]
   :view (fn [r]
           (let [rows   (sort-by (fn [x] (nth x 1)) <
                                 (partition 4 r))
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

> 💡 **Property idents**: replace `:user.property/running` and `:user.property/walking` with the actual idents from your graph. They include random suffixes (e.g. `:user.property/running-jHqSz90j`). Find them by running `[:find ?ident :where [?p :db/ident ?ident] [(clojure.string/starts-with? (str ?ident) ":user.property/")]]`.
> ⚠️ **Number properties are entity refs**: a direct `[?p :user.property/running ?x]` binds `?x` to a value-entity's `:db/id`, **not** the number. You must add a deref step `[?x :logseq.property/value ?n]` to get the actual number. Tip: if results show suspiciously consecutive small integers, that's the giveaway you're looking at eids.

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
   (let [rows    (partition 2 r)
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

> 💡 **How it works**: when Logseq sees `:view` is a keyword, it looks up `[:query/views <keyword>]` in `config.edn`, stringifies the function with `pr-str`, then evaluates it with SCI. The function still runs in the same sandbox as inline `:view`s — no closure capture, no external `require`.

### Tip: parameterize via the result shape

Since registered views can't take config arguments (only the query result `r`), design them around a **result-shape contract**. For example, a generic ASCII bar chart assumes the result is `(label1 value1 label2 value2 …)` from `[:find ?label (count ?b)]`. Any query producing that shape can use `:view :ascii-bars`.

For richer parameterization, design your view to accept a "sentinel" first row. For example, a polymorphic chart could expect `(:type "line" :title "..." val1 val2 …)` and switch on the first element.

## Verify Your Work

You should now be able to:

1. Run an aggregation query and see the raw flat-seq result via `:view (fn [r] [:pre.code (pr-str r)])`.
2. Render an ASCII bar chart using `(partition 2 r)` and `[:pre.code …]`.
3. Render a full SVG line chart with axes, polylines, dots, value labels, and a legend.
4. Register `:ascii-bars` in your `config.edn` under `:query/views` and reference it as `:view :ascii-bars` in any query block.

## What You've Learned

In this tutorial, you:

- ✅ Used `:view` to override Logseq's default block renderer.
- ✅ Learned that aggregation results are flattened — `(partition N r)` re-pairs them.
- ✅ Built a hand-rendered ASCII bar chart with `:pre.code` and `█` characters.
- ✅ Built a full SVG line chart with axes, polylines, dots, gridlines, and a legend.
- ✅ Registered reusable views in `config.edn` under `:query/views` and called them by keyword.

## Next Steps

- **Build a reusable SVG line chart**: extract the Step 3 chart into `:query/views` under `:line-chart` and design the result-shape contract you'll standardize on.
- **Add interactivity**: SVG supports `:on-click`, hover effects via `:on-mouse-over`, and tooltips. Try adding a hover ring around each data point.
- **Use a charting library**: if you outgrow hand-rolled SVG, build a Logseq plugin that exposes a chart component using a JS library like Recharts or D3 — this is the right path once you need axes auto-rendering, tick formatting, animations, etc.

## Troubleshooting

### Query runs but produces no output

**Problem**: `:view` is missing on an aggregation query, so Logseq's default block renderer drops the non-block tuples.

**Solution**: aggregation queries (anything using `count`, `sum`, scalar values, or non-entity tuples in `:find`) **always** require a `:view`. Add at minimum `(fn [r] [:pre.code (pr-str r)])` to see the raw result.

### Block renders blank with no console error

**Problem**: SCI silently dropped one of the following idioms inside your `:view`:

- `map-indexed` (use `(for [i (range (count s))] …)` instead)
- `#()` reader macros (use `(fn [x] …)` instead)
- Keyword-as-function over a collection like `(map :values series)` (use `(map (fn [s] (:values s)) series)`)
- `:style` passed as a string (must be a map: `{:font-size "12px"}`, not `"font-size: 12px"`)

**Solution**: scan the let block for any of these and rewrite using the plain-`fn` form. If still blank, replace the body with `(fn [r] [:pre.code (pr-str r)])` to confirm the query is returning data.

### `nth not supported on this type Number()` error

**Problem**: the first column of your `:find` is a number (e.g. `:block/journal-day` returns an integer like `20260418`), and Logseq's pre-render check `grouped-by-page-result?` calls `first` on it before your `:view` runs.

**Solution**: ensure the **first** `:find` variable binds to a string or entity. Add `[?p :block/title ?title]` and put `?title` first — even if you don't use it in `:view`, it unblocks rendering.

### `:title` key is ignored

**Problem**: in current Logseq DB, the top-level `:title "..."` key in the query map is silently dropped.

**Solution**: put your heading inside `:view` as `[:h3 "..."]` or `[:text {:x ... :y ...} "..."]` for SVG, or as a markdown header in the block above the `query::` property.

### `Query Error` on an apparently valid query

**Problem**: `:limit N` was placed inside the `:find` vector. Logseq DB rejects it.

**Solution**: drop `:limit` and bound results either via `:where` filters or by post-processing in `:view` with `(take N rows)` after partition.

### Number property values look like consecutive small integers (1095, 1093, …)

**Problem**: number properties in Logseq DB are stored as **value entities**, not inline. A direct `[?p :user.property/foo ?x]` binds `?x` to the entity's `:db/id`.

**Solution**: add a deref step:

```clojure
[?p :user.property/foo ?fv]
[?fv :logseq.property/value ?actual-number]
```

Use `?actual-number` in `:find`.

---

_Last updated: 2026-04-21 | [Report an issue](https://github.com/logseq/logseq/issues)_
