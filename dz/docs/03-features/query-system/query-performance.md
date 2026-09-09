# Query Performance Optimization: The Complete Guide

> **Last revised:** 2026-05-29

**Make your Logseq queries 10-1000x faster with proper clause ordering**

This guide will teach you how to write blazingly fast queries by understanding and optimizing clause ordering. Whether you're dealing with slow queries or want to build efficient ones from the start, this guide has you covered.

> **Note:** The examples in this guide use file-based graph attributes (`:block/marker`, `:block/priority`, `:block/scheduled`). In DB-based graphs, these attributes do not exist; equivalent values are queried via DB rules and `:logseq.property/*` idents (for example, task status and priority resolve through `ref-property-with-default` rules).
>
> For text matching in DB graphs, the DSL `block-content` rule reads `:block/title` (not `:block/content`).

---

## Table of Contents

1. [Why Query Performance Matters](#why-query-performance-matters)
2. [The Core Concept: Clause Ordering](#the-core-concept-clause-ordering)
3. [Understanding Query Execution](#understanding-query-execution)
4. [The Golden Rules](#the-golden-rules)
5. [Measuring Query Performance](#measuring-query-performance)
6. [Practical Examples](#practical-examples)
7. [Common Patterns](#common-patterns)
8. [Optimization Checklist](#optimization-checklist)
9. [Troubleshooting Slow Queries](#troubleshooting-slow-queries)
10. [Advanced Techniques](#advanced-techniques)
11. [Quick Reference](#quick-reference)

---

## Why Query Performance Matters

### The Problem

You've written a query that works perfectly... but it takes 5 seconds to execute. Your Logseq UI freezes while waiting for results. With a large graph (10,000+ blocks), even simple queries can become painfully slow.

### The Solution

**Good news:** Most slow queries can be made 10-1000x faster by simply reordering the clauses. No complex optimizations needed—just move lines around!

### Real-World Impact

```clojure
;; Before: Takes 3 seconds, freezes UI
{:query [:find (pull ?b [*])
         :where
         [?b :block/marker]
         [?p :block/title]
         [?b :block/refs ?p]
         [?b :block/priority "A"]]}

;; After: Takes 30ms, instant results
{:query [:find (pull ?b [*])
         :where
         [?b :block/priority "A"]      ;; ← Just reordered!
         [?b :block/marker]
         [?b :block/refs ?p]
         [?p :block/title]]}
```

**100x speedup from reordering 4 lines!**

---

## The Core Concept: Clause Ordering

### What Are Clauses?

In a Logseq query, clauses are the lines in your `:where` section:

```clojure
:where
[?b :block/priority "A"]        ;; ← Clause 1
[?b :block/marker "TODO"]       ;; ← Clause 2
[?b :block/content ?content]    ;; ← Clause 3
```

### Why Order Matters

Datalog processes clauses **in order, one at a time**, building up results:

```clojure
Step 1: [?b :block/priority "A"]
        → Finds 100 blocks

Step 2: [?b :block/marker "TODO"]
        → Keeps blocks that are also TODO (80 blocks remain)

Step 3: [?b :block/content ?content]
        → Gets content from those 80 blocks
```

**Key insight:** The first clause determines how much work all subsequent clauses have to do.

### The Cartesian Product Problem

When clauses don't share variables, the query engine creates ALL combinations:

```clojure
;; ❌ BAD: Creates 10,000 × 5,000 = 50 MILLION combinations!
:where
[?b :block/uuid]      ;; 10,000 blocks
[?p :block/name]      ;; 5,000 pages
[?b :block/parent ?p] ;; NOW it connects them (too late!)

;; ✅ GOOD: Creates only 10,000 combinations
:where
[?b :block/uuid]      ;; 10,000 blocks
[?b :block/parent ?p] ;; Immediately connects ?b to ?p
[?p :block/name]      ;; Only checks the connected pages
```

**Visualization:**

```
BAD Order:
?b        ?p         Result
[1..10k] × [1..5k] = 50 million pairs to check ❌

GOOD Order:
?b → ?p            = 10k connected pairs ✅
```

---

## Understanding Query Execution

### How Logseq Processes Your Query

Think of query execution like a filter pipeline:

```clojure
All Blocks (50,000)
  ↓
[?b :block/priority "A"]
  ↓ Filters to 100 blocks
  ↓
[?b :block/marker "TODO"]
  ↓ Filters to 80 blocks
  ↓
[?b :block/content ?content]
  ↓ Gets 80 content values
  ↓
Final Result: 80 blocks
```

**The narrower your first filter, the less work later filters have to do.**

### Selectivity: The Key Metric

**Selectivity** = How many results a clause produces

| Selectivity   | Example                        | Typical Results |
| ------------- | ------------------------------ | --------------- |
| **Very High** | `[?b :block/uuid #uuid "..."]` | 1 block         |
| **High**      | `[?b :block/priority "A"]`     | 100 blocks      |
| **Medium**    | `[?b :block/marker "TODO"]`    | 1,000 blocks    |
| **Low**       | `[?b :block/content ?c]`       | 40,000 blocks   |
| **Very Low**  | `[?b :block/uuid]`             | 50,000 blocks   |

**Rule:** High selectivity clauses should come FIRST.

---

## The Golden Rules

### Rule 1: Most Selective Clause First

Start with the clause that produces the fewest results.

```clojure
;; ✅ GOOD: Specific value first
:where
[?b :block/priority "A"]    ;; 100 results
[?b :block/marker]          ;; Check only those 100

;; ❌ BAD: General check first
:where
[?b :block/marker]          ;; 5,000 results
[?b :block/priority "A"]    ;; Check all 5,000
```

### Rule 2: Connect Variables Immediately

After binding a variable, use it in the next clause.

```clojure
;; ✅ GOOD: Connect variables sequentially
:where
[?b :block/priority "A"]    ;; Binds ?b
[?b :block/parent ?p]       ;; Uses ?b, binds ?p
[?p :block/title ?t]        ;; Uses ?p

;; ❌ BAD: Unconnected variables
:where
[?b :block/priority "A"]    ;; Binds ?b
[?p :block/title ?t]        ;; ?p unrelated to ?b!
[?b :block/parent ?p]       ;; Connects them (too late)
```

### Rule 3: Specific Values Before General Checks

Exact values are more selective than "has this attribute."

```clojure
;; ✅ GOOD: Specific priority value first
:where
[?b :block/priority "A"]    ;; Exact value: ~100 blocks
[?b :block/marker ?m]       ;; Then check marker

;; ❌ BAD: General check first
:where
[?b :block/marker ?m]       ;; Any marker: ~5,000 blocks
[?b :block/priority "A"]    ;; Then filter
```

### Rule 4: Existence Checks Last

Clauses that just check "has attribute" should come last.

```clojure
;; ✅ GOOD: Existence check last
:where
[?b :block/priority "A"]
[?b :block/marker "TODO"]
[?b :block/content]         ;; Last: just checks it exists

;; ❌ BAD: Existence check first
:where
[?b :block/content]         ;; Matches 50,000 blocks!
[?b :block/priority "A"]
```

### Rule 5: Bind Variables Before Using in Functions

Function calls need bound variables.

```clojure
;; ✅ GOOD: Bind before function
:where
[?b :block/priority ?p]              ;; Bind ?p
[(contains? #{"A" "B"} ?p)]          ;; Use ?p

;; ❌ BAD: Function before binding
:where
[(contains? #{"A" "B"} ?p)]          ;; ?p not bound yet!
[?b :block/priority ?p]
```

### Rule 6: NOT Clauses After Binding

Negation needs bound variables.

```clojure
;; ✅ GOOD: Bind first, then negate
:where
[?b :block/marker "TODO"]       ;; Bind ?b
(not [?b :block/scheduled])     ;; Negate on ?b

;; ❌ BAD: Negate before binding
:where
(not [?b :block/scheduled])     ;; ?b not bound!
[?b :block/marker "TODO"]
```

---

## Measuring Query Performance

### Method 1: Browser DevTools Console Timing

1. Open DevTools (Cmd+Option+I)
2. Go to Console tab
3. Use `console.time` / `console.timeEnd` around query execution
4. Compare timings after clause reordering

### Method 2: Add Timing to Query

**For File-Based Graphs:**

Use the browser console:

```javascript
console.time('query')
// Run your query in Logseq UI
console.timeEnd('query')
// Output: query: 234.5ms
```

### Method 3: Compare Two Versions

Test both orderings side by side:

```clojure
;; Version A
{:title "Test A"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker]
         [?b :block/priority "A"]]}

;; Version B
{:title "Test B"
 :query [:find (pull ?b [*])
         :where
         [?b :block/priority "A"]
         [?b :block/marker]]}
```

Observe which one feels faster when you reload the page.

### Method 4: Estimate Cardinality

Count intermediate results to see where the explosion happens:

```clojure
;; How many blocks have priority A?
{:query [:find (count ?b)
         :where [?b :block/priority "A"]]}
;; Result: 100

;; How many blocks have any marker?
{:query [:find (count ?b)
         :where [?b :block/marker]]}
;; Result: 5,000

;; Conclusion: Priority is 50x more selective → use it first!
```

### Performance Targets

| Query Time | User Experience   | Action                 |
| ---------- | ----------------- | ---------------------- |
| < 50ms     | Instant, smooth   | ✅ Great!              |
| 50-200ms   | Noticeable delay  | ⚠️ Could be better     |
| 200-500ms  | Slow, frustrating | ❌ Needs optimization  |
| > 500ms    | Freezes UI        | 🔴 Critical - must fix |

---

## Practical Examples

### Example 1: Finding High-Priority TODOs

#### ❌ Slow Version (3 seconds)

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/uuid]           ;; 50,000 blocks
         [?b :block/marker ?m]      ;; Still 50,000
         [?b :block/parent ?p]      ;; 50,000 parent links
         [?p :block/title ?t]       ;; 50,000 titles
         [?b :block/priority "A"]   ;; Finally filters to 100
         [(contains? #{"TODO" "DOING"} ?m)]]}
```

**Problem:** Processes 50,000 blocks for most clauses, then filters to 100 at the end.

#### ✅ Fast Version (30ms)

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/priority "A"]        ;; 100 blocks (start small!)
         [?b :block/marker ?m]           ;; Check 100
         [(contains? #{"TODO" "DOING"} ?m)]  ;; Filter 100
         [?b :block/parent ?p]           ;; 100 parent links
         [?p :block/title ?t]]}          ;; 100 titles
```

**Why it's faster:** Processes only 100 blocks through most clauses.

**Speedup: 100x**

---

### Example 2: Blocks in a Specific Project

#### ❌ Slow Version (2 seconds)

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/content ?c]          ;; 50,000 blocks
         [?p :block/title ?t]            ;; 10,000 pages
         [?b :block/refs ?p]             ;; 60,000 references
         [(clojure.string/includes? ?t "Project Alpha")]]
         :inputs ["task"]}
```

**Problem:** Creates 50,000 × 10,000 = 500 million combinations before filtering!

#### ✅ Fast Version (50ms)

```clojure
{:query [:find (pull ?b [*])
         :in $ ?project-name
         :where
         [?p :block/title ?t]                      ;; 10,000 pages
         [(clojure.string/includes? ?t ?project-name)]  ;; Filters to 1
         [?b :block/refs ?p]                       ;; ~100 blocks
         [?b :block/content ?c]]                   ;; Get 100 contents
         :inputs ["Project Alpha"]}
```

**Why it's faster:** Finds the specific page first (1 match), then finds blocks referencing it.

**Speedup: 40x**

---

### Example 3: Tasks Due This Week

#### ❌ Slow Version (5 seconds)

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/uuid]
         [?b :block/marker ?m]
         [?b :block/scheduled ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]
         [(contains? #{"TODO" "DOING"} ?m)]]
         :inputs [20250101 20250107]}
```

**Problem:** Checks all 50,000 blocks for UUIDs and markers before filtering by date.

#### ✅ Fast Version (100ms)

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/scheduled ?d]          ;; 2,000 scheduled blocks
         [(>= ?d ?start)]                  ;; Filters to 50
         [(<= ?d ?end)]                    ;; Filters to 50
         [?b :block/marker ?m]             ;; Check 50
         [(contains? #{"TODO" "DOING"} ?m)]]
         :inputs [20250101 20250107]}
```

**Why it's faster:** Starts with scheduled blocks (already more selective than all blocks).

**Speedup: 50x**

---

### Example 4: Blocks with Multiple Tags

#### ❌ Slow Version (4 seconds)

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/content]
         [?b :block/refs ?r1]
         [?r1 :block/name "project"]
         [?b :block/refs ?r2]
         [?r2 :block/name "urgent"]]}
```

**Problem:** Starts with all 50,000 blocks with content.

#### ✅ Fast Version (80ms)

```clojure
{:query [:find (pull ?b [*])
         :where
         [?r1 :block/name "project"]    ;; Find page "project" (1 match)
         [?b :block/refs ?r1]           ;; Blocks referencing it (~500)
         [?r2 :block/name "urgent"]     ;; Find page "urgent" (1 match)
         [?b :block/refs ?r2]           ;; Filter to blocks with both (~50)
         [?b :block/content]]}          ;; Get content (50)
```

**Why it's faster:** Finds specific pages first, then blocks referencing them.

**Speedup: 50x**

---

## Common Patterns

### Pattern 1: The Priority Filter Pattern

**Use case:** Filter by specific property value

```clojure
;; Template
:where
[?b :property/specific-value "exact-match"]  ;; FIRST
[?b :property/other-check]                   ;; SECOND
[?b :property/general-exists]                ;; LAST
```

**Example:**

```clojure
:where
[?b :block/priority "A"]      ;; Specific value first
[?b :block/marker "TODO"]     ;; Then check marker
[?b :block/content]           ;; Existence check last
```

---

### Pattern 2: The Reference Chain Pattern

**Use case:** Find blocks referencing a specific page

```clojure
;; Template
:where
[?page :block/name "specific-page"]  ;; Find page first
[?b :block/refs ?page]               ;; Then blocks referencing it
[?b :other/attributes]               ;; Then other checks
```

**Example:**

```clojure
:where
[?project :block/name "project-alpha"]  ;; 1 page
[?b :block/refs ?project]               ;; ~200 blocks
[?b :block/marker "TODO"]               ;; ~150 blocks
```

---

### Pattern 3: The Parent Traversal Pattern

**Use case:** Find blocks in a hierarchy

```clojure
;; Template
:where
[?root :block/title "Root Page"]  ;; Find root first
[?child :block/parent ?root]      ;; Direct children
[?b :block/parent ?child]         ;; Grandchildren
```

**Example:**

```clojure
:where
[?root :block/title "Projects"]   ;; 1 page
[?project :block/parent ?root]    ;; ~10 projects
[?task :block/parent ?project]    ;; ~100 tasks
[?task :block/marker "TODO"]      ;; ~80 tasks
```

---

### Pattern 4: The Date Range Pattern

**Use case:** Filter by date range

```clojure
;; Template
:where
[?b :date/attribute ?date]    ;; Bind date attribute first
[(>= ?date ?start)]           ;; Then filter range
[(<= ?date ?end)]
[?b :other/checks]            ;; Then other checks
```

**Example:**

```clojure
:where
[?b :block/scheduled ?date]     ;; 2,000 scheduled blocks
[(>= ?date 20250101)]           ;; ~500 blocks
[(<= ?date 20250131)]           ;; ~500 blocks
[?b :block/marker "TODO"]       ;; ~400 blocks
```

---

### Pattern 5: The Property Existence Pattern

**Use case:** Find blocks that have a specific property

```clojure
;; Template
:where
[?b :specific/value "narrow"]  ;; Narrow down first
[?b :property/check]           ;; Then property existence
```

**Example:**

```clojure
:where
[?b :block/priority "A"]       ;; 100 blocks
[?b :block/scheduled]          ;; Check if they're scheduled
```

---

### Pattern 6: The String Search Pattern

**Use case:** Full-text search with additional filters

```clojure
;; Template (if you can narrow down first)
:where
[?b :specific/filter "value"]           ;; Narrow first if possible
[?b :block/content ?c]                  ;; Then get content
[(clojure.string/includes? ?c "text")]  ;; Then search

;; OR (if full-text search is most selective)
:where
(block-content ?b "search-term")  ;; Use rule if available
[?b :other/checks]
```

**Example:**

```clojure
;; If you know the marker:
:where
[?b :block/marker "TODO"]                     ;; 5,000 blocks
[?b :block/content ?c]                        ;; Get content
[(clojure.string/includes? ?c "urgent")]      ;; ~100 blocks

;; If search term is very specific:
:where
[?b :block/content ?c]                        ;; 50,000 blocks
[(clojure.string/includes? ?c "URGENT-2025-PROJECT-ALPHA")]  ;; ~5 blocks
[?b :block/marker]                            ;; Check marker
```

---

### Pattern 7: The Negation Pattern

**Use case:** Exclude blocks with certain properties

```clojure
;; Template
:where
[?b :specific/narrow-down]     ;; Narrow down first
[?b :other/checks]             ;; More filters
(not [?b :exclude/this])       ;; Negation last
```

**Example:**

```clojure
:where
[?b :block/marker "TODO"]      ;; 5,000 tasks
[?b :block/priority "A"]       ;; 100 high-priority
(not [?b :block/scheduled])    ;; Exclude scheduled (~80)
```

---

## Optimization Checklist

Use this checklist when writing or optimizing queries:

### Step 1: Identify Selectivity

- [ ] List each clause and estimate how many results it produces
- [ ] Mark the most selective clause (fewest results)
- [ ] Mark the least selective clause (most results)

### Step 2: Order Clauses

- [ ] Move most selective clause to the top
- [ ] Connect variables sequentially (each clause uses variables from previous)
- [ ] Group related clauses together
- [ ] Move existence checks to the bottom
- [ ] Ensure function calls come after binding their variables

### Step 3: Check Connections

- [ ] Verify every clause connects to at least one previous variable
- [ ] Look for "orphan" clauses that create Cartesian products
- [ ] Fix by reordering or adding connecting clauses

### Step 4: Optimize Special Cases

- [ ] For NOT clauses: bind variables first
- [ ] For OR clauses: consider multi-clause rules instead
- [ ] For recursive rules: base case before recursive case
- [ ] For string functions: bind strings before calling functions

### Step 5: Test Performance

- [ ] Measure query execution time
- [ ] Compare against targets (< 50ms ideal)
- [ ] If slow, go back to Step 1 and re-evaluate selectivity

---

## Troubleshooting Slow Queries

### Problem 1: Query Takes 3+ Seconds

**Symptoms:**

- UI freezes when query runs
- Noticeable delay when opening a page

**Diagnosis:**

1. **Check for Cartesian products**

Look for clauses that don't share variables:

```clojure
;; ❌ Problem: ?b and ?p unconnected
:where
[?b :block/marker]     ;; No shared variable
[?p :block/title]      ;; No shared variable
[?b :block/refs ?p]    ;; Finally connects them
```

**Fix:** Reorder to connect variables immediately:

```clojure
;; ✅ Fixed
:where
[?b :block/marker]
[?b :block/refs ?p]    ;; Connects immediately
[?p :block/title]
```

2. **Check clause selectivity**

Count how many results each clause produces:

```clojure
;; Test each clause
{:query [:find (count ?b) :where [?b :block/marker]]}
;; Result: 5,000

{:query [:find (count ?b) :where [?b :block/priority "A"]]}
;; Result: 100

;; Priority is 50x more selective → use it first
```

---

### Problem 2: Query Works but Gets Slower Over Time

**Symptoms:**

- Query was fast initially
- Slows down as graph grows

**Diagnosis:**

**Issue:** Your first clause becomes less selective as you add blocks

```clojure
;; Was fast with 100 blocks, slow with 10,000
:where
[?b :block/marker]     ;; Grows from 10 to 5,000 blocks
[?b :block/priority "A"]
```

**Fix:** Use a clause that stays selective:

```clojure
;; Stays fast even with 50,000 blocks
:where
[?b :block/priority "A"]     ;; Always ~100 blocks
[?b :block/marker]
```

---

### Problem 3: Recursive Query Never Finishes

**Symptoms:**

- Query runs indefinitely
- Browser tab becomes unresponsive
- Recursive rule involved

**Diagnosis:**

**Issue:** Recursive case before base case

```clojure
;; ❌ Never terminates
:rules '[[(ancestor ?a ?c)
          [?intermediate :block/parent ?a]
          (ancestor ?intermediate ?c)]    ;; Recurses infinitely!
         [(ancestor ?a ?c)
          [?c :block/parent ?a]]]
```

**Fix:** Base case first:

```clojure
;; ✅ Terminates correctly
:rules '[[(ancestor ?a ?c)
          [?c :block/parent ?a]]          ;; Base case first
         [(ancestor ?a ?c)
          [?intermediate :block/parent ?a]
          (ancestor ?intermediate ?c)]]
```

---

### Problem 4: Query Returns Too Many Results

**Symptoms:**

- Expected 10 results, got 10,000
- Results include unrelated blocks

**Diagnosis:**

**Issue:** Missing connecting clause creates Cartesian product

```clojure
;; ❌ Returns 5,000 × 100 = 500,000 results!
:where
[?b :block/marker]     ;; 5,000 blocks
[?p :block/name "tag"] ;; 1 page
;; Missing: connection between ?b and ?p
```

**Fix:** Add connecting clause:

```clojure
;; ✅ Returns ~100 results
:where
[?p :block/name "tag"]     ;; 1 page
[?b :block/refs ?p]        ;; 100 blocks referencing it
[?b :block/marker]         ;; All 100 have markers
```

---

### Problem 5: Function Throws "Unbound Variable" Error

**Symptoms:**

- Error: "Insufficient bindings: #{?var}"
- Query fails to execute

**Diagnosis:**

**Issue:** Using variable in function before it's bound

```clojure
;; ❌ Error: ?content not bound
:where
[(clojure.string/includes? ?content "text")]
[?b :block/content ?content]
```

**Fix:** Bind variable first:

```clojure
;; ✅ Works
:where
[?b :block/content ?content]
[(clojure.string/includes? ?content "text")]
```

---

## Advanced Techniques

### Technique 1: Query Plan Visualization

Mentally trace your query execution:

```clojure
:where
[?b :block/priority "A"]       ;; 100 blocks
  ↓ (binds ?b to 100 blocks)
[?b :block/marker ?m]          ;; Check 100 blocks → 80 have markers
  ↓ (binds ?m for 80 blocks)
[(contains? #{"TODO"} ?m)]     ;; Filter 80 → 60 are TODO
  ↓ (60 blocks remain)
[?b :block/refs ?r]            ;; Get refs from 60 → 200 refs
  ↓ (binds ?r for 200 refs)
[?r :block/name ?n]            ;; Get 200 names
  ↓
Final: 60 blocks with 200 references
```

**Total operations:** ~600

Compare to bad ordering:

```clojure
:where
[?b :block/marker ?m]          ;; 5,000 blocks
  ↓
[(contains? #{"TODO"} ?m)]     ;; 3,000 are TODO
  ↓
[?b :block/refs ?r]            ;; 3,000 blocks → 30,000 refs
  ↓
[?r :block/name ?n]            ;; 30,000 names
  ↓
[?b :block/priority "A"]       ;; Filter 3,000 → 60 blocks
  ↓
Final: Same 60 blocks
```

**Total operations:** ~66,000 (110x more!)

---

### Technique 2: Benchmark-Driven Optimization

**Step-by-step benchmarking:**

```clojure
;; Benchmark each clause individually
;; 1. Count results from first clause
{:query [:find (count ?b)
         :where [?b :block/priority "A"]]}
;; Result: 100

;; 2. Count after second clause
{:query [:find (count ?b)
         :where
         [?b :block/priority "A"]
         [?b :block/marker]]}
;; Result: 80

;; 3. Count after third clause
{:query [:find (count ?b)
         :where
         [?b :block/priority "A"]
         [?b :block/marker]
         [?b :block/refs ?r]]}
;; Result: 60

;; Look for big jumps in count = bottleneck
```

---

### Technique 3: Index-Aware Query Shaping

Source-backed DB schema highlights:

- `:block/name`, `:block/title`, `:block/journal-day`, `:block/page`, `:block/parent`, `:block/created-at`, and `:block/updated-at` are indexed in DB graphs.
- New DB properties are created with `:db/index true` by default.
- `block-content` DSL matching uses `(clojure.string/includes? ...)` on `:block/title`, so text filters are still more expensive than highly selective exact matches.

> **Removed:** The older "indexed vs not indexed" list that labeled custom properties as generally unindexed and used `:block/content` as the primary text field is stale for current DB graphs.

**Strategy:** Prefer selective exact matches and graph-link constraints before text functions:

```clojure
;; ✅ Better: narrow first, then text predicate
:where
[?p :block/name "project-alpha"]
[?b :block/refs ?p]
[?b :block/title ?t]
[(clojure.string/includes? ?t "urgent")]

;; ❌ Slower: broad text scan first
:where
[?b :block/title ?t]
[(clojure.string/includes? ?t "urgent")]
[?b :block/refs ?p]
```

---

### Technique 4: Clause Grouping for Readability

Group related clauses with comments:

```clojure
:where
;; Find high-priority tasks
[?b :block/priority "A"]
[?b :block/marker "TODO"]

;; In specific project
[?project :block/name "project-alpha"]
[?b :block/refs ?project]

;; Due this week
[?b :block/scheduled ?d]
[(>= ?d 20250101)]
[(<= ?d 20250107)]

;; With specific content
[?b :block/content ?c]
[(clojure.string/includes? ?c "urgent")]
```

This helps you visualize the filtering stages.

---

### Technique 5: Two-Pass Queries for Complex Filters

When you need complex filtering that's hard to optimize:

```clojure
;; Pass 1: Get candidate blocks (fast)
{:query [:find ?uuid
         :where
         [?b :block/priority "A"]
         [?b :block/uuid ?uuid]]}
;; Results: [#uuid "..." #uuid "..." ...]

;; Pass 2: Filter in application code
;; Then fetch full blocks for matches
```

This is useful when the filtering logic is too complex for Datalog.

---

### Technique 6: Pre-compute Common Queries

For frequently-used slow queries:

**Option 1:** Use a simpler query that's "good enough"

```clojure
;; Instead of complex ancestor traversal
;; Use: just check direct parent
```

**Option 2:** Add helper properties

```clojure
;; Add a property during block creation:
;; :block/project-root → link to project

;; Then query becomes simple:
:where
[?b :block/project-root ?project]
[?project :block/name "project-alpha"]
```

---

## Findings: New Capabilities

The query runtime now includes behavior that improves performance safety and rule ergonomics:

1. **Auto rule injection for advanced queries**

- Advanced queries scan `:where` for DB DSL rules and automatically append required rule clauses.
- If query inputs already include `%` rules, runtime merges detected rules into the existing rules input.

2. **Stable rule arity when `%` is expected**

- When a query expects `%` but no rules are passed, runtime appends an empty rules vector to keep Datascript arity correct.

3. **Negation-aware DSL query normalization**

- DSL parsing now pushes `(not ...)` clauses after positive binders in `and` groups, reducing unbound-variable failures and improving execution shape.

4. **Automatic domain bindings for negative-only vars**

- DSL query construction inserts minimal binder clauses when variables appear only in negative positions, preventing invalid or overly broad plans.

---

## Quick Reference

### Selectivity Ranking (High to Low)

1. **Specific UUID:** `[?b :block/uuid #uuid "..."]` → 1 result
2. **Specific property value:** `[?b :block/priority "A"]` → 100s
3. **Rare properties:** `[?b :block/scheduled]` → 1,000s
4. **Common property values:** `[?b :block/marker "TODO"]` → 1,000s
5. **Property existence:** `[?b :block/content]` → 10,000s
6. **Any entity:** `[?b :block/uuid]` → 50,000s

### Clause Type Order

```clojure
:where
;; 1. Specific constants
[?b :block/uuid #uuid "..."]
[?p :block/name "specific-page"]

;; 2. Specific property values
[?b :block/priority "A"]

;; 3. Variable connections
[?b :block/parent ?p]

;; 4. Common properties
[?b :block/marker ?m]

;; 5. Function calls (after binding)
[(contains? #{"TODO"} ?m)]

;; 6. Existence checks
[?b :block/content]

;; 7. Negations (after binding)
(not [?b :block/scheduled])
```

### Decision Tree

```
Writing a new clause?
  ↓
Does it use a specific constant?
  YES → Put it EARLY (top 3 clauses)
  NO → Continue
  ↓
Does it connect to previous variables?
  YES → Put it next to related clause
  NO → Put it LATER
  ↓
Is it a function call?
  YES → Put it AFTER binding variables
  NO → Continue
  ↓
Is it a negation?
  YES → Put it NEAR THE END
  NO → Continue
  ↓
Is it an existence check?
  YES → Put it AT THE END
```

### Common Mistakes

| Mistake                         | Fix                                  |
| ------------------------------- | ------------------------------------ |
| `[?b :block/marker]` first      | Use `[?b :block/priority "A"]` first |
| Unconnected variables           | Add connecting clause                |
| Function before binding         | Bind variables first                 |
| Negation first                  | Bind then negate                     |
| Existence check first           | Move to end                          |
| Recursive case before base case | Base case first                      |

### Performance Expectations

| Graph Size     | Good Query | Bad Query      |
| -------------- | ---------- | -------------- |
| 1,000 blocks   | < 20ms     | < 100ms        |
| 10,000 blocks  | < 50ms     | 500ms - 2s     |
| 50,000 blocks  | < 100ms    | 5s - never     |
| 100,000 blocks | < 200ms    | Never finishes |

---

## Summary

### The Three Most Important Rules

1. **Most selective clause first** - Start with the clause that produces fewest results
2. **Connect variables immediately** - Each clause should use variables from previous clauses
3. **Measure and iterate** - Test your query and optimize based on results

### Before and After Checklist

Before optimization:

- [ ] Query takes > 200ms
- [ ] UI freezes when running
- [ ] Clauses in random order
- [ ] No thought to selectivity

After optimization:

- [x] Query takes < 50ms
- [x] UI remains responsive
- [x] Clauses ordered by selectivity
- [x] Variables connected sequentially
- [x] Tested and measured

### Remember

**A well-ordered query can be 10-1000x faster than a poorly ordered one.**

The same query, different order = massive performance difference.

**Time invested in optimization:** 5-10 minutes
**Speedup gained:** 10-1000x
**User experience improvement:** Instant instead of frozen

---

**Happy querying!**

_Last updated: 2026-05-29_
_Version: 1.2_

---

## Further Reading

- [Logseq Query Documentation](https://docs.logseq.com/#/page/queries)
- [Datalog Query Optimization](https://docs.datomic.com/on-prem/query/query-executing.html)
- [Understanding Datascript Indexes](https://github.com/tonsky/datascript/wiki/Tips-&-tricks)
