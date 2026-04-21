# Getting Started with :result-transform in Logseq

> **Last revised:** 2026-04-21

**Transform your query results like a pro!**

This guide will show you how to use Logseq's `:result-transform` feature to customize your query results by sorting, filtering, and transforming data in powerful ways.

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

- **Sort** results by any property (priority, date, title, etc.)
- **Filter** results to show only what matters
- **Limit** results to show just the top N items
- **Transform** data into custom formats
- **Combine** multiple operations together

### Why use it?

While Datalog queries are powerful for finding blocks, sometimes you need more control over how results are presented. `:result-transform` fills that gap.

**Example:** Your query finds all TODO tasks, but you want them sorted by priority (A > B > C). `:result-transform` makes this easy!

### How it works

```
Your Query → Database → Raw Results → :result-transform → Final Results → Display
```

The transformation happens automatically before results are shown to you.

---

## Quick Start

### Your First Transform

Let's start with something simple: reversing the order of results.

**Before (no transform):**

```clojure
{:title "My Tasks"
 :query [:find (pull ?b [*])
         :where [?b :block/marker "TODO"]]}
```

**After (with transform):**

```clojure
{:title "My Tasks (Newest First)"
 :query [:find (pull ?b [*])
         :where [?b :block/marker "TODO"]]
 :result-transform (fn [result] (reverse result))}
```

That's it! Your results now appear in reverse order.

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

### 2. Limit to First 10 Results

```clojure
:result-transform (fn [result] (take 10 result))
```

### 3. Sort by Date (Newest First)

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

This filters for TODOs, sorts by date, and shows only the 5 most recent.

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

### Recipe 1: Reverse Chronological Order

**Use case:** Show newest items first

```clojure
:result-transform (fn [result] (reverse result))
```

### Recipe 2: Top 5 Results Only

**Use case:** Limit overwhelming queries

```clojure
:result-transform (fn [result] (take 5 result))
```

### Recipe 3: Sort by Priority (A → B → C)

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

### Recipe 4: Advanced Priority Sort (A=1, B=2, C=3)

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

### Recipe 5: Filter Out Completed Tasks

```clojure
:result-transform (fn [result]
                    (filter #(not= "DONE" (:block/marker %)) result))
```

### Recipe 6: Show Only Today's Tasks

**File-Based:**

```clojure
:result-transform (fn [result]
                    (let [today (quot (.now js/Date) 86400000)]
                      (filter #(= today (quot (:block/scheduled %) 86400000))
                              result)))
```

### Recipe 7: Sort by Multiple Criteria

Priority first, then deadline:

**DB Graphs:**

```clojure
:result-transform (fn [result]
                    (sort-by (fn [block]
                               [(get block :logseq.property/priority "Z")
                                (get block :logseq.property/deadline 99999999)])
                             result))
```

### Recipe 8: Remove Duplicates by Content

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

### Recipe 9: Random Order (Shuffle)

```clojure
:result-transform (fn [result] (shuffle result))
```

### Recipe 10: Group by Tag and Flatten

**File-Based:**

```clojure
:result-transform (fn [result]
                    (->> result
                         (group-by :block/tags)
                         (sort-by first)
                         (mapcat second)))
```

### Recipe 11: Most Recently Updated Items (Top 50)

**Use case:** Show the 50 blocks you've edited most recently

```clojure
:result-transform (fn [result]
                    (->> result
                         (sort-by :block/updated-at >)
                         (take 50)))
```

This is useful for finding blocks you're actively working on.

### Recipe 12: Recently Updated with Priority Filter

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
| Content   | `:block/content`    | `:block/content` (same)      |

### Universal Properties (Same for Both)

These properties work the same in both graph types:

- `:block/uuid`
- `:block/content`
- `:block/created-at`
- `:block/updated-at`

**Note:** `:block/marker` is file-based only. In DB graphs, task status is stored as `:logseq.property/status`.

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

**Cause:** Wrong property namespace for your graph type

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

   Check the console to see what properties are actually available.

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

**Default:** If you omit the option, Logseq behaves as if `:remove-block-children?`
were set to `true`.

### Problem: Query Shows No Results After Adding Transform

**Symptoms:** Query worked before, now shows nothing

**Cause:** Transform function returns empty or nil

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

2. **Using unavailable functions:**

   ```clojure
   ;; ❌ Some functions might not be available
   :result-transform (fn [result] (println result))

   ;; ✅ Use allowed functions only
   :result-transform (fn [result] result)
   ```

### Problem: Sort Order Is Wrong

**Symptoms:** Results are sorted, but not in the expected order

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

;; Step 2: Add sorting
:result-transform (fn [result] (sort-by :block/priority result))

;; Step 3: Add filtering
:result-transform (fn [result]
                    (filter #(:block/priority %)
                            (sort-by :block/priority result)))

;; Step 4: Add limit
:result-transform (fn [result]
                    (->> result
                         (filter #(:block/priority %))
                         (sort-by :block/priority)
                         (take 10)))
```

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

**A:** No, transforms use Clojure syntax only. But don't worry—most common operations have Clojure equivalents:

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

**A:** Yes! Transforms work with both list views and table views.

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

**A:** Logseq will log the error to the console and display the original, untransformed results. Your query won't break.

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

**A:** Yes, but it only affects the display in that query, not the actual stored content.

```clojure
:result-transform (fn [result]
                    (map #(assoc % :block/content
                                   (str "✅ " (:block/content %)))
                         result))
```

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

**A:** Use `:block/updated-at` with the `>` comparator for descending order (newest first):

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

Show only items due this week:

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

This always shows all High-priority items, then fills remaining slots with others.

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

_Last updated: 2026-03-10_
_Version: 1.1_
