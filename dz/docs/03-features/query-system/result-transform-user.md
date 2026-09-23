# Getting Started with :result-transform in Logseq

> **Last revised:** 2026-09-22
>
> Scope: `:result-transform` pipeline and result ordering re-verified against source at `39ade9a309`; the ordering behavior was traced in source and not observed in a running app.

**Transform your query results like a pro!**

This guide will show you how to use Logseq's `:result-transform` feature to customize your query results by filtering, limiting, and transforming data.

Sorting is the one thing `:result-transform` cannot do on its own — read [Read this first: sorting in DB graphs](#read-this-first-sorting-in-db-graphs) before you write a sort.

---

## Table of Contents

1. [What is :result-transform?](#what-is-result-transform)
2. [Quick Start](#quick-start)
3. [Common Use Cases](#common-use-cases)
4. [Step-by-Step Tutorial](#step-by-step-tutorial)
5. [Recipe Book](#recipe-book)
6. [File-Based vs DB Graphs](#file-based-vs-db-graphs)
7. [Troubleshooting](#troubleshooting)
8. [Tips & Best Practices](#tips--best-practices)
9. [FAQ](#faq)
10. [Advanced Techniques](#advanced-techniques)

---

## What is :result-transform?

`:result-transform` is a feature that lets you customize how your query results are displayed. Think of it as a post-processing step that runs after your query completes, allowing you to:

- **Filter** results to show only what matters
- **Limit** results to show just the top N items
- **Transform** data into custom formats
- **Combine** multiple operations together
- **Sort** results — but see the caveat below: in DB graphs, sorting only survives when the query also has a `:view`

### Why use it?

While Datalog queries are powerful for finding blocks, sometimes you need more control over how results are presented. `:result-transform` fills that gap.

**Example:** Your query finds all TODO tasks, but you only want the ones tagged `#work`. `:result-transform` makes this easy!

### How it works

```
Your Query → Database → Raw Results → :result-transform → Final Results → Display
```

The transformation happens automatically before results are shown to you.

### Read this first: sorting in DB graphs

**In a DB graph, sorting inside `:result-transform` is discarded for block results.** If your query returns blocks and has no `:view` function, the order you produce in the transform never reaches the screen. Filtering (`filter`, `remove`) and truncation (`take`) still work, because they change *which* rows exist, not their order.

Why: after your transform runs, each block is reduced to its bare `:block/uuid` before the rows leave the worker (`src/main/frontend/worker/handler/render_resource/query.cljs:15-21`, applied at `:209`). A result that is all uuids is then rendered by the standard table view (`src/main/frontend/components/query.cljs:52-57`), which puts the uuids into a **set** (`deps/db/src/logseq/db/common/view.cljs:1114`) and re-sorts the rows by the query block's own sorting property, defaulting to most-recently-updated-first (`deps/db/src/logseq/db/common/view.cljs:1115-1118`).

Your order *is* kept in two cases:

1. The query has a `:view` function. The view fn is called with your transformed result, in your order, before the table path is reached (`src/main/frontend/components/query.cljs:43-50`).
2. The query is one of Logseq's built-in journal queries, which render through a different code path (`src/main/frontend/components/query.cljs:60-72`).

**What to do instead**, in order of preference:

1. **Sort the table.** Click the column header in the rendered query table. That writes `:logseq.property.table/sorting` onto the query block, which is what the renderer actually reads. Same for grouping (`:logseq.property.view/group-by-property`) and filtering (`:logseq.property.table/filters`). These three are hidden properties defined in `deps/db/src/logseq/db/frontend/property.cljs:456-461,512-517,519-523`.

   You can also set them from the CLI:

   ```sh
   logseq upsert block -g <graph> --id <block-id> \
     --update-properties '{:logseq.property.view/group-by-property :logseq.property/status
                           :logseq.property.table/sorting [{:id :logseq.property/priority :asc? false}]}'
   ```

2. **Add a `:view` function.** Sort in the transform (or in the view fn itself) and render the rows yourself. Everything under [Advanced Techniques](#advanced-techniques) that depends on order needs this.

3. **Return scalars, not blocks.** If your `:find` returns values rather than pulled blocks, the table path is not taken and your order is preserved.

The code that discards the order has no file-graph vs DB-graph branch, so the same applies to any query rendered through the standard table view. The sections below still label examples "File-Based" and "DB Graphs" where the *property names* differ.

## Findings: New Capabilities

- `:result-transform` supports keyword references to reusable transforms defined in `logseq/config.edn` under `:query/result-transforms`. An unregistered keyword throws — it does not fall back to no transform (`src/main/frontend/components/query.cljs:133-140`).
- The SCI helpers `sum`, `average`, `parseFloat`, `isNaN`, `log`, `pprint` and `call-api` are available to `:view` functions, **not** to `:result-transform`. `:view` is evaluated on the main thread with those bindings injected (`src/main/frontend/extensions/sci.cljs:29-46`); `:result-transform` is evaluated in the worker with plain `sci.core/eval-string` and no bindings (`src/main/frontend/worker/handler/render_resource/query.cljs:12,191`). Inside a transform, assume `clojure.core` only.
- `:remove-block-children?` child pruning applies to block results in every graph type. The code has no file-vs-DB branch (`src/main/frontend/worker/handler/render_resource/query.cljs:177-186`).

---

## Quick Start

### Your First Transform

Let's start with something simple: keeping only the first 10 results.

**Before (no transform):**

```clojure
{:title "My Tasks"
 :query [:find (pull ?b [*])
         :where [?b :block/marker "TODO"]]}
```

**After (with transform):**

```clojure
{:title "My Tasks (First 10)"
 :query [:find (pull ?b [*])
         :where [?b :block/marker "TODO"]]
 :result-transform (fn [result] (take 10 result))}
```

That's it! Your query now shows at most 10 rows.

> **Why not `(reverse result)` as the first example?** Because it would do nothing visible. Reordering block results is dropped before rendering — see [Read this first: sorting in DB graphs](#read-this-first-sorting-in-db-graphs).

### The Basic Pattern

Every `:result-transform` follows this structure:

```clojure
:result-transform (fn [result]
                    ;; Your transformation code here
                    result)
```

- `result` is a list of your query results
- You transform it however you want
- Return the transformed result

---

## Common Use Cases

### 1. Sort by Priority

> **Needs a `:view`.** On its own, this transform has no visible effect on a block result — the table re-sorts the rows afterwards. Sort the table by clicking its Priority column header instead, or add a `:view` fn. See [Read this first](#read-this-first-sorting-in-db-graphs).

**File-Based Graphs:**

```clojure
:result-transform (fn [result]
                    (sort-by :block/priority result))
```

**DB Graphs:**

```clojure
:result-transform (fn [result]
                    (sort-by :logseq.property/priority result))
```

The equivalent that actually sticks, set on the query block:

```clojure
:logseq.property.table/sorting [{:id :logseq.property/priority :asc? false}]
```

### 2. Limit to First 10 Results

Works as written. `take` changes which rows exist, so it survives rendering.

```clojure
:result-transform (fn [result] (take 10 result))
```

Careful: which 10 you get is the pre-render order, and the table then re-sorts them. Combine with table sorting only if you are happy truncating an unsorted set.

### 3. Sort by Date (Newest First)

> **Needs a `:view`.** Same caveat as use case 1.

**By Created Date:**

```clojure
:result-transform (fn [result]
                    (reverse (sort-by :block/created-at result)))
```

**By Last Updated Date:**

```clojure
:result-transform (fn [result]
                    (sort-by :block/updated-at > result))
```

Note: Using `>` as the comparator sorts in descending order (newest first) directly without needing `reverse`.

Newest-updated-first is already the table's default (`deps/db/src/logseq/db/common/view.cljs:1115-1118`), so for this particular case you usually need no transform at all.

### 4. Filter High Priority Only

**File-Based:**

```clojure
:result-transform (fn [result]
                    (filter #(= "A" (:block/priority %)) result))
```

**DB Graphs:**

```clojure
:result-transform (fn [result]
                    (filter #(= "High" (:logseq.property/priority %)) result))
```

### 5. Combine Operations

```clojure
:result-transform (fn [result]
                    (->> result
                         (filter #(= "TODO" (:block/marker %)))
                         (sort-by :block/created-at)
                         (take 5)))
```

This filters for TODOs, sorts by date, and keeps the 5 oldest. The filter and the `take` hold; the sort determines *which* 5 you keep, but the table decides the order they are displayed in.

---

## Step-by-Step Tutorial

Let's build a practical query together: **"Show my top 10 high-priority tasks, sorted by deadline"**

### Step 1: Start with a Basic Query

```clojure
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]]}
```

This finds all TODO and DOING tasks.

### Step 2: Add Priority Filtering

First, let's identify if you're using a file-based or DB graph:

- **File-based:** Your graph folder contains markdown files (`.md`)
- **DB graph:** Your graph is database-based (newer format)

**For File-Based Graphs:**

```clojure
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]]
 :result-transform (fn [result]
                     (filter #(contains? #{"A" "B"} (:block/priority %))
                             result))}
```

**For DB Graphs:**

```clojure
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]]
 :result-transform (fn [result]
                     (filter #(contains? #{"High" "Urgent"} (:logseq.property/priority %))
                             result))}
```

### Step 3: Add Deadline Sorting

> **This step is where the transform stops being enough.** The sort below decides which tasks survive Step 4's `take 10`, but it does not decide the order they appear in. To get deadline order on screen, click the Deadline column header in the rendered table, or set `:logseq.property.table/sorting [{:id :logseq.property/deadline :asc? true}]` on the query block.

**For File-Based Graphs:**

```clojure
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]]
 :result-transform (fn [result]
                     (->> result
                          (filter #(contains? #{"A" "B"} (:block/priority %)))
                          (sort-by :block/deadline)))}
```

**For DB Graphs:**

```clojure
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]]
 :result-transform (fn [result]
                     (->> result
                          (filter #(contains? #{"High" "Urgent"} (:logseq.property/priority %)))
                          (sort-by :logseq.property/deadline)))}
```

### Step 4: Limit to Top 10

```clojure
:result-transform (fn [result]
                    (->> result
                         (filter #(contains? #{"High" "Urgent"} (:logseq.property/priority %)))
                         (sort-by :logseq.property/deadline)
                         (take 10)))
```

### Step 5: Test and Refine

Save your query and check the results. If something doesn't work, see the [Troubleshooting](#troubleshooting) section below.

---

## Recipe Book

Copy-paste these recipes and adapt them to your needs!

Recipes are tagged:

- **[order]** — changes the order only. On a block result with no `:view`, this does nothing visible; use table sorting or a `:view` fn instead. See [Read this first](#read-this-first-sorting-in-db-graphs).
- **[rows]** — changes which rows exist. Works as written.

### Recipe 1: Reverse Chronological Order — **[order]**

**Use case:** Show newest items first

```clojure
:result-transform (fn [result] (reverse result))
```

Prefer `:logseq.property.table/sorting [{:id :block/created-at :asc? false}]` on the query block.

### Recipe 2: Top 5 Results Only — **[rows]**

**Use case:** Limit overwhelming queries

```clojure
:result-transform (fn [result] (take 5 result))
```

### Recipe 3: Sort by Priority (A → B → C) — **[order]**

**File-Based:**

```clojure
:result-transform (fn [result]
                    (sort-by #(get % :block/priority "Z") result))
```

**DB Graphs:**

```clojure
:result-transform (fn [result]
                    (sort-by #(get % :logseq.property/priority "zzz") result))
```

The `"zzz"` is a fallback value for blocks without priority (sorts them last).

### Recipe 4: Advanced Priority Sort (A=1, B=2, C=3) — **[order]**

**File-Based:**

```clojure
:result-transform (fn [result]
                    (sort-by #(case (:block/priority %)
                                "A" 1
                                "B" 2
                                "C" 3
                                99)
                             result))
```

**DB Graphs:**

```clojure
:result-transform (fn [result]
                    (sort-by #(case (:logseq.property/priority %)
                                "Urgent" 1
                                "High" 2
                                "Medium" 3
                                "Low" 4
                                99)
                             result))
```

### Recipe 5: Filter Out Completed Tasks — **[rows]**

```clojure
:result-transform (fn [result]
                    (filter #(not= "DONE" (:block/marker %)) result))
```

### Recipe 6: Show Only Today's Tasks — **[rows]**

Uses `js/Date` interop, which has not been verified to work inside a worker-evaluated `:result-transform`. Test it before relying on it.

**File-Based:**

```clojure
:result-transform (fn [result]
                    (let [today (quot (.now js/Date) 86400000)]
                      (filter #(= today (quot (:block/scheduled %) 86400000))
                              result)))
```

### Recipe 7: Sort by Multiple Criteria — **[order]**

Priority first, then deadline:

**DB Graphs:**

```clojure
:result-transform (fn [result]
                    (sort-by (fn [block]
                               [(get block :logseq.property/priority "Z")
                                (get block :logseq.property/deadline 99999999)])
                             result))
```

### Recipe 8: Remove Duplicates by Content — **[rows]**

```clojure
:result-transform (fn [result]
                    (let [seen (atom #{})]
                      (filter (fn [block]
                                (let [content (:block/content block)]
                                  (if (@seen content)
                                    false
                                    (do (swap! seen conj content)
                                        true))))
                              result)))
```

### Recipe 9: Random Order (Shuffle) — **[order]**

```clojure
:result-transform (fn [result] (shuffle result))
```

### Recipe 10: Group by Tag and Flatten — **[order]**

For grouping that shows up on screen, set `:logseq.property.view/group-by-property` on the query block instead — the table renders real group headers.

**File-Based:**

```clojure
:result-transform (fn [result]
                    (->> result
                         (group-by :block/tags)
                         (sort-by first)
                         (mapcat second)))
```

### Recipe 11: Most Recently Updated Items (Top 50) — **[rows]** + **[order]**

**Use case:** Show the 50 blocks you've edited most recently

```clojure
:result-transform (fn [result]
                    (->> result
                         (sort-by :block/updated-at >)
                         (take 50)))
```

This is useful for finding blocks you're actively working on. The `sort-by` here is load-bearing — it picks *which* 50 rows survive — but the table decides their displayed order. Newest-updated-first is already the table default, so in this recipe the two agree.

### Recipe 12: Recently Updated with Priority Filter — **[rows]** + **[order]**

**Use case:** High-priority tasks sorted by recent updates

**DB Graphs:**

```clojure
:result-transform (fn [result]
                    (->> result
                         (filter #(contains? #{"High" "Urgent"}
                                   (:logseq.property/priority %)))
                         (sort-by :block/updated-at >)
                         (take 50)))
```

**File-Based:**

```clojure
:result-transform (fn [result]
                    (->> result
                         (filter #(contains? #{"A" "B"}
                                   (:block/priority %)))
                         (sort-by :block/updated-at >)
                         (take 50)))
```

---

## File-Based vs DB Graphs

The biggest difference when using `:result-transform` between graph types is **property names**.

### How to Check Your Graph Type

1. Look at your graph folder:
   - **Files ending in `.md`** → File-based graph
   - **`logseq.db` file** → DB graph

2. Or check in Logseq:
   - Open developer console (Cmd+Option+I on Mac, F12 on Windows)
   - Type: `logseq.api.get_current_graph()`
   - Look for `"type": "db"` or `"type": "file"`

### Property Name Reference

| Property  | File-Based          | DB Graph                     |
| --------- | ------------------- | ---------------------------- |
| Priority  | `:block/priority`   | `:logseq.property/priority`  |
| Deadline  | `:block/deadline`   | `:logseq.property/deadline`  |
| Scheduled | `:block/scheduled`  | `:logseq.property/scheduled` |
| Tags      | `:block/tags`       | `:logseq.property/tags`      |
| Status    | `:block/marker`     | `:logseq.property/status`    |
| Created   | `:block/created-at` | `:block/created-at` (same)   |
| Updated   | `:block/updated-at` | `:block/updated-at` (same)   |
| Content   | `:block/content`    | `:block/title`               |

### Universal Properties (Same for Both)

These properties work the same in both graph types:

- `:block/uuid`
- `:block/created-at`
- `:block/updated-at`

**Note:** `:block/marker` is file-based only. In DB graphs, task status is stored as `:logseq.property/status`.

**Note:** `:block/content` does not exist in the DB schema; block text lives in `:block/title` (`deps/db/src/logseq/db/frontend/schema.cljs:93`). Any recipe below that reads `:block/content` returns `nil` on a DB graph.

### Converting Between Graph Types

**File-Based → DB Graph:**

Replace `:block/priority` with `:logseq.property/priority`

```clojure
;; Before (file-based)
:result-transform (fn [r] (sort-by :block/priority r))

;; After (DB graph)
:result-transform (fn [r] (sort-by :logseq.property/priority r))
```

---

## Troubleshooting

### Problem: Transform Doesn't Do Anything

**Symptoms:** Query returns results, but they're not transformed

**Solutions:**

1. **Check spelling:**
   - Must be `:result-transform` (singular)
   - Not `:transform`, `:result-transforms`, or `:transform-result`

2. **Check syntax:**

   ```clojure
   ;; ✅ Correct
   :result-transform (fn [result] ...)

   ;; ❌ Wrong - missing 'fn'
   :result-transform (result ...)

   ;; ❌ Wrong - wrong parameter count
   :result-transform (fn [] ...)
   ```

3. **Check for errors in console:**
   - Open DevTools (Cmd+Option+I / F12)
   - Look for red error messages
   - Search for "sci" or "query"

### Problem: Results Are Unchanged After Sorting

**Symptoms:** Sort doesn't work as expected

**Cause 1 (most likely): your result is blocks and your query has no `:view`.** The order is discarded before rendering. This is not a bug in your transform — no property name will fix it. See [Read this first: sorting in DB graphs](#read-this-first-sorting-in-db-graphs) and sort the table instead.

Quick check: if `(fn [result] (reverse result))` changes nothing, you are hitting this. If `(fn [result] (take 1 result))` leaves one row, the transform is running fine — only the order is being dropped.

**Cause 2:** Wrong property namespace for your graph type

**Solution:**

1. Verify your graph type (see [File-Based vs DB Graphs](#file-based-vs-db-graphs))

2. Use correct property names:

   ```clojure
   ;; File-based
   (sort-by :block/priority result)

   ;; DB graph
   (sort-by :logseq.property/priority result)
   ```

3. **Debug tip:** Add this transform to see available properties:

   ```clojure
   :result-transform (fn [result]
                       (do
                         (println "First block keys:" (keys (first result)))
                         result))
   ```

   The transform runs inside the query worker, so any output lands in the worker's console context rather than the page's. The `log` and `pprint` helpers are **not** available here — they are only bound for `:view` functions (`src/main/frontend/extensions/sci.cljs:29-46`).

### Problem: Matching Child Blocks Are Missing

**Symptoms:** You know a child block matches the query, but only the parent block
shows up as a separate result.

**Cause:** Advanced query result rendering removes child matches by default when a
parent block is also in the result set.

**Solution:** Set `:remove-block-children? false` on the outer advanced query map.
This is a per-query display option, not a `config.edn` setting.

```clojure
{:title "All Matching Task Blocks"
 :query [:find (pull ?b [*])
         :in $ %
         :where (task ?b #{"Canceled" "Done" "In Review" "Doing" "Todo" "Backlog"})]
 :remove-block-children? false}
```

**Current behavior:** If omitted, the default is effectively `true` for block results in every graph type. The pruning code has no file-vs-DB branch — it applies whenever the query returned blocks and `:remove-block-children?` is not explicitly `false` (`src/main/frontend/worker/handler/render_resource/query.cljs:177-186`).

> **Corrected 2026-09-22:** an earlier revision of this guide claimed "For DB graphs, this child-pruning step is not applied." That was wrong.

### Problem: Query Shows No Results After Adding Transform

**Symptoms:** Query worked before, now shows nothing — or shows a query error box

**Cause:** Transform function returns empty, or returns something that isn't a collection

A transform must return a sequence or a set. Returning `nil`, a number, a map or a string fails the query outright with `"Query result transform must return rows"` (`src/main/frontend/worker/handler/render_resource/query.cljs:196-198`). So does supplying a form that doesn't evaluate to a function (`"Query result transform is not a function"`).

**Solutions:**

1. **Make sure you return something:**

   ```clojure
   ;; ❌ Wrong - doesn't return anything
   :result-transform (fn [result]
                       (sort-by :block/priority result)
                       nil)

   ;; ✅ Correct - returns sorted result
   :result-transform (fn [result]
                       (sort-by :block/priority result))
   ```

2. **Check your filter isn't too strict:**

   ```clojure
   ;; This might filter out everything
   (filter #(= "A" (:block/priority %)) result)

   ;; Better - handles missing priority
   (filter #(contains? #{"A" "B"} (get % :block/priority "")) result)
   ```

### Problem: Error in Console

**Symptoms:** Red error messages mentioning "sci" or "eval"

**Common Causes:**

1. **Syntax error in function:**

   ```clojure
   ;; ❌ Missing closing bracket
   :result-transform (fn [result] (sort-by :priority result)

   ;; ✅ Fixed
   :result-transform (fn [result] (sort-by :priority result))
   ```

2. **Using transform functions that fail at runtime:**

   ```clojure
   ;; ❌ Runtime error inside transform
   :result-transform (fn [result]
                       (map #(assoc % :x (/ 1 0)) result))

   ;; ✅ Safe transform
   :result-transform (fn [result]
                       (map #(assoc % :x 1) result))
   ```

   A transform that throws does **not** fall back to the untransformed result — the query renders an error box instead (`src/main/frontend/components/query.cljs:182-183`).

   Only plain `clojure.core` is in scope. `sum`, `average`, `parseFloat`, `isNaN`, `log`, `pprint` and `call-api` are bound for `:view` functions, not for `:result-transform`.

### Problem: Sort Order Is Wrong

**Symptoms:** Results are sorted, but not in the expected order

**First check:** does your query return blocks and lack a `:view`? Then what you are seeing is the table's own sorting, not yours — see [Read this first](#read-this-first-sorting-in-db-graphs). The advice below applies to sorting inside a `:view` fn, or to non-block results.

**Solutions:**

1. **Add fallback values:**

   ```clojure
   ;; Without fallback, missing values cause unexpected sorting
   (sort-by :block/priority result)

   ;; With fallback "Z", missing priorities sort last
   (sort-by #(get % :block/priority "Z") result)
   ```

2. **Reverse if needed:**

   ```clojure
   ;; For descending order
   (reverse (sort-by :block/created-at result))
   ```

3. **Use explicit case for priority:**

   ```clojure
   (sort-by #(case (:block/priority %)
               "A" 1
               "B" 2
               "C" 3
               99)
            result)
   ```

---

## Tips & Best Practices

### 1. Start Simple, Then Add Complexity

Don't try to write complex transforms right away. Build up gradually:

```clojure
;; Step 1: Just return result (verify transform works)
:result-transform (fn [result] result)

;; Step 2: Add filtering
:result-transform (fn [result] (filter #(:block/priority %) result))

;; Step 3: Add sorting (affects which rows survive step 4, not display order)
:result-transform (fn [result]
                    (->> result
                         (filter #(:block/priority %))
                         (sort-by :block/priority)))

;; Step 4: Add limit
:result-transform (fn [result]
                    (->> result
                         (filter #(:block/priority %))
                         (sort-by :block/priority)
                         (take 10)))
```

Verify each step with a `take`, not with a `reverse` — a `reverse` that appears to do nothing tells you nothing about whether the transform ran.

### 2. Use Fallback Values

Always provide fallback values for properties that might not exist:

```clojure
;; ❌ Risky - crashes if property missing
(get result :block/priority)

;; ✅ Safe - uses fallback
(get result :block/priority "Z")
```

### 3. Test with Small Queries First

Before applying transforms to queries with hundreds of results, test on small datasets:

```clojure
;; Add a limit to your query while testing
:query [:find (pull ?b [*])
        :where [?b :block/marker "TODO"]]
:result-transform (fn [result] (take 5 result))  ;; Only process 5
```

### 4. Comment Your Complex Transforms

Help your future self understand what the transform does:

```clojure
:result-transform (fn [result]
                    ;; Sort by priority (A > B > C)
                    ;; then by deadline (earliest first)
                    ;; finally limit to 10 items
                    (->> result
                         (sort-by #(case (:logseq.property/priority %)
                                     "A" 1 "B" 2 "C" 3 99))
                         (sort-by :logseq.property/deadline)
                         (take 10)))
```

### 5. Reuse Transforms in Config

For transforms you use often, define them in `config.edn`:

**In `logseq/config.edn`:**

```clojure
:query/result-transforms
{:priority-sort (fn [result]
                  (sort-by #(get % :logseq.property/priority "Z") result))

 :recent-first (fn [result]
                 (reverse (sort-by :block/created-at result)))

 :top-10 (fn [result] (take 10 result))}
```

**In your queries:**

```clojure
;; Use by keyword reference
{:title "Tasks"
 :query [...]
 :result-transform :priority-sort}
```

This makes your queries cleaner and transformations reusable!

Two things to know:

- A keyword that isn't in `:query/result-transforms` throws — the query does not silently run untransformed (`src/main/frontend/components/query.cljs:133-140`).
- `:priority-sort` and `:recent-first` above are pure reorderings, so they only do something for queries that also have a `:view`. `:top-10` works anywhere.

### 6. Chain Operations with ->>

The `->>` (thread-last) macro makes complex transformations readable:

```clojure
;; ❌ Hard to read
(take 10 (sort-by :priority (filter #(:priority %) result)))

;; ✅ Easy to read
(->> result
     (filter #(:priority %))
     (sort-by :priority)
     (take 10))
```

Read this as: "Take result, filter it, sort it, then take 10."

### 7. Keep Transforms Fast

Transforms run every time your query updates. Keep them efficient:

- Filter before sorting (process less data)
- Avoid complex calculations in sort functions
- Limit results early if possible

```clojure
;; ✅ Better: filter first (fewer items to sort)
(->> result
     (filter #(= "A" (:priority %)))
     (sort-by :deadline)
     (take 10))

;; ❌ Slower: sort everything, then filter
(->> result
     (sort-by :deadline)
     (filter #(= "A" (:priority %)))
     (take 10))
```

---

## FAQ

### Q: Can I use JavaScript in transforms?

> **Removed:** "No, JavaScript cannot be used in transforms."

**A:** Transforms are written in Clojure(Script) syntax. Bare `parseFloat` and `isNaN` are **not** bound inside a `:result-transform` — those bindings only exist for `:view` functions (`src/main/frontend/extensions/sci.cljs:29-46`). Whether raw `js/` interop such as `js/Date` is reachable from a transform depends on the worker's SCI configuration and has not been verified; treat the `js/Date` examples in this guide as untested and prefer Clojure forms. Most collection operations are easiest to write in idiomatic Clojure anyway:

| JavaScript           | Clojure               |
| -------------------- | --------------------- |
| `array.sort()`       | `(sort-by :key coll)` |
| `array.filter()`     | `(filter fn coll)`    |
| `array.map()`        | `(map fn coll)`       |
| `array.slice(0, 10)` | `(take 10 coll)`      |
| `array.reverse()`    | `(reverse coll)`      |
| `array.length`       | `(count coll)`        |

### Q: Why do my properties return nil?

**A:** Most likely you're using the wrong property namespace. Check if you're on a file-based or DB graph (see [File-Based vs DB Graphs](#file-based-vs-db-graphs)).

### Q: Can I combine multiple transforms?

**A:** Yes! Use the `->>` threading macro:

```clojure
:result-transform (fn [result]
                    (->> result
                         (filter #(:block/priority %))
                         (sort-by :block/deadline)
                         (take 10)))
```

### Q: Can I reference other transforms?

**A:** Yes, but only if they're defined in your `config.edn` registry (see [Tip #5](#5-reuse-transforms-in-config)).

### Q: Do transforms affect performance?

**A:** For normal-sized results (<100 blocks), performance impact is minimal. For large result sets (>1000 blocks), keep transforms simple.

### Q: Can I use transforms in advanced query tables?

**A:** Yes — the transform runs. But a table re-sorts the rows it is given, so only the row-changing parts of your transform (`filter`, `remove`, `take`) survive. Column sorting on the table is the supported way to order a table.

### Q: Where do I set `:remove-block-children?`?

**A:** Put it on the outer advanced query map, alongside `:query`, `:inputs`,
`:result-transform`, and `:view`.

```clojure
{:query [:find (pull ?b [*])
    :in $ %
    :where (task ?b #{"Todo"})]
 :remove-block-children? false}
```

It is not part of `:result-transform`, and it is not configured globally in
`config.edn`.

### Q: What happens if my transform throws an error?

**A:** The query fails and renders an error box. There is no fallback to untransformed results. The same happens if the form doesn't evaluate to a function, or if it returns something that is neither a sequence nor a set (`src/main/frontend/worker/handler/render_resource/query.cljs:188-199`, surfaced at `src/main/frontend/components/query.cljs:182-183`).

### Q: Can I debug my transforms?

**A:** Sort of. You can add debug output to see what's happening:

```clojure
:result-transform (fn [result]
                    (do
                      (println "Input count:" (count result))
                      (println "First item:" (first result))
                      (sort-by :priority result)))
```

Then check the browser console (DevTools).

### Q: Can transforms modify block content?

**A:** Not for block results rendered as a table. Anything you `assoc` onto a block is thrown away: each block is reduced to its bare `:block/uuid` on the way out of the worker, and the renderer then re-reads the block from the database (`src/main/frontend/worker/handler/render_resource/query.cljs:15-21`). The transform below compiles and runs, and changes nothing on screen.

```clojure
;; No visible effect on a block result
:result-transform (fn [result]
                    (map #(assoc % :block/title
                                   (str "✅ " (:block/title %)))
                         result))
```

To decorate rows, add a `:view` function and render the decorated result yourself. Nothing a transform does can change stored content either way.

### Q: Do I need to know Clojure to use this?

**A:** Not really! You can get far by copying and adapting the recipes in this guide. Over time, you'll naturally learn more.

### Q: Where should I put my transform definitions?

**A:** Two options:

1. **Inline** (in the query itself) - good for one-off transforms
2. **Config file** (`logseq/config.edn`) - good for reusable transforms

### Q: How do I limit the number of results?

**A:** Use `:result-transform` with the `take` function:

```clojure
{:query [:find (pull ?b [*])
         :where (task ?b #{"Todo"})]
 :result-transform (fn [result] (take 50 result))}
```

You can combine this with sorting and filtering:

```clojure
;; Get 50 most recently updated items
:result-transform (fn [result]
                    (->> result
                         (sort-by :block/updated-at >)
                         (take 50)))
```

### Q: How do I sort by last updated date?

**A:** Set `:logseq.property.table/sorting [{:id :block/updated-at :asc? false}]` on the query block, or click the column header in the rendered table. That is already the table's default, so for newest-first you usually need to do nothing at all.

The `:result-transform` forms below only affect the displayed order when the query also has a `:view` fn. They are still useful for deciding which rows a following `take` keeps.

```clojure
;; Most recently updated first
:result-transform (fn [result]
                    (sort-by :block/updated-at > result))

;; Oldest updates first
:result-transform (fn [result]
                    (sort-by :block/updated-at result))

;; Most recently updated, limited to 50
:result-transform (fn [result]
                    (->> result
                         (sort-by :block/updated-at >)
                         (take 50)))
```

### Q: Can I use regular expressions?

**A:** Yes! Clojure's `re-find` and `re-matches` work in transforms:

```clojure
:result-transform (fn [result]
                    (filter #(re-find #"important" (:block/content %))
                            result))
```

---

## Advanced Techniques

> **Read before copying.** Techniques 1, 2, 3, 4 and 6 depend on row order, or on fields added to the block map — both are discarded when a block result is rendered by the standard table (see [Read this first](#read-this-first-sorting-in-db-graphs)). Add a `:view` function alongside `:result-transform` and render the rows yourself. Techniques 5 and 7 only drop rows, so they work as written.
>
> On a DB graph, replace `:block/content` with `:block/title` throughout this section — `:block/content` is not in the DB schema.

### Technique 1: Multi-Level Sorting

Sort by priority, then by deadline, then by title:

**DB Graphs:**

```clojure
:result-transform (fn [result]
                    (sort-by (fn [block]
                               [(get block :logseq.property/priority "zzz")
                                (get block :logseq.property/deadline 99999999)
                                (get block :block/content "")])
                             result))
```

### Technique 2: Conditional Formatting

Add emoji prefixes based on priority:

```clojure
:result-transform (fn [result]
                    (map (fn [block]
                           (let [priority (:logseq.property/priority block)
                                 emoji (case priority
                                         "Urgent" "🔴 "
                                         "High" "🟠 "
                                         "Medium" "🟡 "
                                         "Low" "🟢 "
                                         "")]
                             (assoc block :block/content
                                    (str emoji (:block/content block)))))
                         result))
```

### Technique 3: Grouping and Sectioning

Group tasks by status:

```clojure
:result-transform (fn [result]
                    (let [groups (group-by :block/marker result)]
                      (concat
                        (get groups "DOING" [])
                        (get groups "TODO" [])
                        (get groups "WAITING" []))))
```

### Technique 4: Statistical Analysis

Count and show only top priority:

```clojure
:result-transform (fn [result]
                    (let [total (count result)
                          high-pri (filter #(= "High" (:logseq.property/priority %)) result)
                          high-count (count high-pri)]
                      ;; Return with custom header block
                      (cons {:block/content (str "Showing " high-count " of " total " tasks")}
                            high-pri)))
```

### Technique 5: Date-Based Filtering

Show only items due this week. Same `js/Date` caveat as Recipe 6.

```clojure
:result-transform (fn [result]
                    (let [now (.now js/Date)
                          week-later (+ now (* 7 24 60 60 1000))]
                      (filter (fn [block]
                                (let [deadline (:logseq.property/deadline block)]
                                  (and deadline
                                       (>= deadline now)
                                       (<= deadline week-later))))
                              result)))
```

### Technique 6: Smart Truncation

Show first N, but always include high-priority items:

```clojure
:result-transform (fn [result]
                    (let [high-pri (filter #(= "High" (:logseq.property/priority %)) result)
                          others (remove #(= "High" (:logseq.property/priority %)) result)]
                      (concat high-pri (take (- 10 (count high-pri)) others))))
```

The row *selection* holds: all High-priority items survive, and the remaining slots are filled with others. The "High-priority first" part does not — without a `:view`, the table decides the order.

### Technique 7: Deduplicate by Custom Logic

Remove blocks with duplicate content (case-insensitive):

```clojure
:result-transform (fn [result]
                    (let [seen (atom #{})]
                      (filter (fn [block]
                                (let [content (-> block
                                                  :block/content
                                                  (.toLowerCase))]
                                  (if (@seen content)
                                    false
                                    (do (swap! seen conj content)
                                        true))))
                              result)))
```

---

## What's Next?

Now that you know how to use `:result-transform`, try:

1. **Experiment** with the recipes in this guide
2. **Adapt** them to your specific needs
3. **Share** your favorite transforms with the community
4. **Combine** transforms with other query features

### Additional Resources

- [Logseq Query Documentation](https://docs.logseq.com/#/page/queries)
- [Clojure Cheat Sheet](https://clojure.org/api/cheatsheet)
- [Community Forum](https://discuss.logseq.com/)

### Need Help?

- Check the [Troubleshooting](#troubleshooting) section
- Ask in the Logseq Discord or Forum
- Share your query and we'll help you fix it!

---

**Happy querying!**

_Last updated: 2026-09-22_
_Version: 1.2_
