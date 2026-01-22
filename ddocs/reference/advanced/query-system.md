---
title: 'Query System Reference'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Reference'
platform: 'Desktop + Mobile'
related-docs:
  - '[Schema System](../../explanation/architecture/schema-system.md)'
  - '[Database Properties Guide](../../tutorials/development/db-properties.md)'
  - '[Advanced Queries How-To](../../how-to/advanced/advanced-queries.md)'
tags: ['queries', 'datalog', 'advanced', 'datascript', 'database']
---

# Query System Reference

This reference documents Logseq's advanced query system—powerful Datalog queries for retrieving and analyzing graph data. Master queries to build custom views, generate reports, and automate workflows.

**What's Covered:**

- Datalog query syntax
- Advanced query features
- Built-in query functions
- Query best practices
- Common query patterns

**Prerequisites:**

- Familiarity with database graphs ([Schema System](../../explanation/architecture/schema-system.md))
- Basic understanding of Datalog (see [Learn Datalog Today](http://www.learndatalogtoday.org/))

---

## Table of Contents

1. [Query Syntax](#query-syntax)
2. [Basic Queries](#basic-queries)
3. [Advanced Queries](#advanced-queries)
4. [Query Functions](#query-functions)
5. [Query Rules](#query-rules)
6. [Performance Optimization](#performance-optimization)
7. [Query Patterns](#query-patterns)

---

## Query Syntax

### Query Block Format

**Simple Query:**

````markdown
```query
{:title "My Query"
 :query [:find (pull ?b [*])
         :where [?b :block/title]]}
```
````

**Advanced Query:**

````markdown
```query
{:title "Pending Tasks Due This Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :logseq.property/status "pending"]
         [?b :logseq.property.task/due-date ?date]
         [(>= ?date ?start)]
         [(<= ?date ?end)]]
 :inputs [:today :7d-after]
 :view :table
 :collapsed? false}
```
````

### Query Map Structure

```clojure
{:title "Query Title"         ; Display name
 :query [:find ...]            ; Datalog query
 :inputs [...]                 ; Input parameters
 :view :list                   ; Display format (:list, :table, :pane)
 :collapsed? false             ; Initial collapsed state
 :breadcrumb-show? false       ; Show breadcrumb navigation
 :group-by-page? true          ; Group results by page
 :result-transform fn-name}    ; Custom result transformation
```

---

## Basic Queries

### Find All Blocks with Tag

```clojure
{:title "All Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/tags ?tag]
         [?tag :db/ident :logseq.class/Task]]}
```

### Find Blocks by Property Value

```clojure
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :logseq.property/priority "high"]]}
```

### Find Pages by Name Pattern

```clojure
{:title "Project Pages"
 :query [:find (pull ?p [*])
         :where
         [?p :block/title ?title]
         [(clojure.string/starts-with? ?title "Project")]]}
```

### Find Recent Blocks

```clojure
{:title "Recent Updates"
 :query [:find (pull ?b [*])
         :where
         [?b :block/updated-at ?time]
         [(> ?time ?cutoff)]]
 :inputs [:7d-ago]}
```

---

## Advanced Queries

### Multiple Conditions (AND)

```clojure
{:title "Urgent Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/tags ?tag]
         [?tag :db/ident :logseq.class/Task]
         [?b :logseq.property/priority "high"]
         [?b :logseq.property/status "pending"]]}
```

### OR Conditions

```clojure
{:title "High or Critical Priority"
 :query [:find (pull ?b [*])
         :where
         (or [?b :logseq.property/priority "high"]
             [?b :logseq.property/priority "critical"])]}
```

### NOT Conditions

```clojure
{:title "Incomplete Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/tags ?tag]
         [?tag :db/ident :logseq.class/Task]
         (not [?b :logseq.property/status "done"])]}
```

### Joins Across Relationships

```clojure
{:title "Tasks in Project Alpha"
 :query [:find (pull ?b [*])
         :where
         [?project :block/title "Project Alpha"]
         [?b :block/page ?project]
         [?b :block/tags ?tag]
         [?tag :db/ident :logseq.class/Task]]}
```

### Aggregation (Count)

```clojure
{:title "Task Count by Status"
 :query [:find ?status (count ?b)
         :where
         [?b :block/tags ?tag]
         [?tag :db/ident :logseq.class/Task]
         [?b :logseq.property/status ?status]]}
```

### Sorting Results

```clojure
{:title "Tasks by Due Date"
 :query [:find (pull ?b [*]) ?date
         :where
         [?b :block/tags ?tag]
         [?tag :db/ident :logseq.class/Task]
         [?b :logseq.property.task/due-date ?date]
         :order-by [[?date :asc]]]}
```

---

## Query Functions

### Built-in Functions

**String Functions:**

```clojure
;; String contains
[(clojure.string/includes? ?title "meeting")]

;; String starts with
[(clojure.string/starts-with? ?title "TODO")]

;; String ends with
[(clojure.string/ends-with? ?title ".pdf")]

;; Case-insensitive match
[(clojure.string/lower-case ?title) ?lower]
[(clojure.string/includes? ?lower "important")]
```

**Date/Time Functions:**

```clojure
;; Greater than date
[(> ?date ?cutoff)]

;; Date range
[(>= ?date ?start)]
[(<= ?date ?end)]

;; Get current date
[(identity #inst "2025-01-25") ?today]
```

**Numeric Functions:**

```clojure
;; Greater than
[(> ?count 10)]

;; Range check
[(>= ?score 70)]
[(<= ?score 100)]

;; Arithmetic
[(+ ?a ?b) ?sum]
[(- ?end ?start) ?duration]
```

**Collection Functions:**

```clojure
;; Check if entity has ref
[?b :block/tags ?tag]

;; Count refs
[(count ?refs) ?ref-count]
```

### Custom Query Functions

**Location:** `src/main/frontend/db/query_custom.cljs`

```clojure
;; Get page references
(page-ref ?b ?page-name)

;; Get block parent
(parent ?child ?parent)

;; Between dates
(between ?date ?start ?end)

;; Has namespace
(namespace ?page ?ns)
```

**Usage Example:**

```clojure
{:title "Blocks on Project Pages"
 :query [:find (pull ?b [*])
         :where
         [(namespace ?b "Project")]]}
```

---

## Query Rules

### Defining Rules

```clojure
{:title "Tasks Assigned to Me"
 :query [:find (pull ?b [*])
         :in $ % ?user
         :where
         (task-assigned-to ?b ?user)]
 :rules [[(task-assigned-to ?b ?user)
          [?b :block/tags ?tag]
          [?tag :db/ident :logseq.class/Task]
          [?b :logseq.property/assigned-to ?user]]]}
```

### Recursive Rules

**Find all descendants:**

```clojure
{:title "All Child Blocks"
 :query [:find (pull ?child [*])
         :in $ % ?root
         :where
         (descendant ?root ?child)]
 :rules [[(descendant ?parent ?child)
          [?child :block/parent ?parent]]
         [(descendant ?parent ?child)
          [?child :block/parent ?intermediate]
          (descendant ?parent ?intermediate)]]}
```

### Reusable Rule Library

**Common rules:**

```clojure
;; Task rules
[(is-task ?b)
 [?b :block/tags ?tag]
 [?tag :db/ident :logseq.class/Task]]

[(is-pending ?b)
 (is-task ?b)
 [?b :logseq.property/status "pending"]]

;; Date rules
[(due-this-week ?b)
 [?b :logseq.property.task/due-date ?date]
 [(>= ?date ?start)]
 [(<= ?date ?end)]]

;; Page rules
[(in-namespace ?page ?ns)
 [?page :block/title ?title]
 [(clojure.string/starts-with? ?title ?ns)]]
```

---

## Performance Optimization

### Indexing Strategy

**DataScript Indexes:**

```clojure
;; EAVT: Entity-Attribute-Value-Transaction
[?e :block/title ?v]  ; Fast lookup by attribute

;; AEVT: Attribute-Entity-Value-Transaction
[_ :block/title "Page Name"]  ; Fast lookup by attribute + value

;; AVET: Attribute-Value-Entity-Transaction
[_ :block/uuid #uuid "..."]  ; Fast unique lookup
```

### Query Optimization Tips

**1. Start with Most Restrictive Clauses**

```clojure
;; ❌ Slow: Broad condition first
[:where
 [?b :block/title ?title]  ; Matches thousands of blocks
 [?b :block/uuid #uuid "abc-123"]]  ; Narrow to one

;; ✅ Fast: Narrow condition first
[:where
 [?b :block/uuid #uuid "abc-123"]  ; Start with one block
 [?b :block/title ?title]]
```

**2. Use Entity IDs Instead of Lookups**

```clojure
;; ❌ Slow: String lookup each time
[?b :block/tags ?tag]
[?tag :block/title "Important"]

;; ✅ Fast: Use db/ident directly
[?b :block/tags ?tag]
[?tag :db/ident :logseq.class/Important]
```

**3. Limit Result Size**

```clojure
{:query [:find (pull ?b [*])
         :where [?b :block/title]]
 :limit 50}  ; Prevent rendering thousands of blocks
```

**4. Avoid Expensive Pull Patterns**

```clojure
;; ❌ Slow: Pull all attributes recursively
(pull ?b [*])

;; ✅ Fast: Pull only needed attributes
(pull ?b [:block/title :block/uuid :logseq.property/status])
```

---

## Query Patterns

### Task Management

**Overdue Tasks:**

```clojure
{:title "Overdue Tasks"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/tags ?tag]
         [?tag :db/ident :logseq.class/Task]
         [?b :logseq.property.task/due-date ?date]
         [(< ?date ?today)]]
 :inputs [:today]}
```

**Tasks by Project:**

```clojure
{:title "Project Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/tags ?task-tag]
         [?task-tag :db/ident :logseq.class/Task]
         [?b :block/tags ?project-tag]
         [?project-tag :block/title ?project-name]
         [(not= ?project-name "Task")]]
 :group-by-page? true}
```

### Knowledge Management

**Recently Modified Pages:**

```clojure
{:title "Recent Pages"
 :query [:find (pull ?p [*]) ?time
         :where
         [?p :block/type :page]
         [?p :block/updated-at ?time]
         [(> ?time ?cutoff)]
         :order-by [[?time :desc]]]
 :inputs [:7d-ago]
 :limit 20}
```

**Orphaned Pages (No References):**

```clojure
{:title "Orphaned Pages"
 :query [:find (pull ?p [*])
         :where
         [?p :block/type :page]
         (not [_ :block/refs ?p])]}
```

### Flashcards

**Due for Review:**

```clojure
{:title "Due Cards"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/tags ?tag]
         [?tag :db/ident :logseq.class/Card]
         [?b :logseq.property.card/next-review-date ?date]
         [(<= ?date ?today)]]
 :inputs [:today]}
```

---

## Next Steps

**Continue Learning:**

1. **[Advanced Queries How-To](../../how-to/advanced/advanced-queries.md)** - Practical query recipes
2. **[Schema System](../../explanation/architecture/schema-system.md)** - Understand data structure
3. **[Learn Datalog Today](http://www.learndatalogtoday.org/)** - Interactive Datalog tutorial
4. **[DataScript Documentation](https://github.com/tonsky/datascript)** - Full Datalog reference

**For Contributors:**

- [Query System Source Code](../../../src/main/frontend/db/query_dsl.cljs)
- [Custom Query Functions](../../../src/main/frontend/db/query_custom.cljs)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
