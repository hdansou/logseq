---
title: 'Query Patterns Reference'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Reference'
platform: 'Desktop + Mobile (Database graphs preferred)'
related-docs:
  - '[Query System Guide](../developer/query-system.md)'
  - '[Advanced Queries Tutorial](../../tutorials/advanced/advanced-queries.md)'
  - '[Task Management](../../how-to/workflows/task-management-workflows.md)'
tags: ['queries', 'datalog', 'advanced', 'reference', 'patterns']
---

# Query Patterns Reference

Complete reference of common query patterns in Logseq. Copy-paste these patterns and adapt them to your needs. Includes simple and advanced Datalog queries.

**Quick Start:** Copy a pattern → Modify for your use case → Test → Refine

---

## Table of Contents

1. [Query Basics](#query-basics)
2. [Task Queries](#task-queries)
3. [Page & Block Queries](#page--block-queries)
4. [Date & Time Queries](#date--time-queries)
5. [Property Queries](#property-queries)
6. [Tag Queries](#tag-queries)
7. [Reference Queries](#reference-queries)
8. [Advanced Patterns](#advanced-patterns)
9. [Query Optimization](#query-optimization)

---

## Query Basics

### Query Structure

**Template:**

```clojure
#+BEGIN_QUERY
{:title "Query Title"
 :query [:find (pull ?b [*])
         :in $ ?input
         :where
         [CONDITIONS]]
 :inputs [INPUT_VALUES]
 :collapsed? false
 :breadcrumb-show? true}
#+END_QUERY
```

**Components:**

```clojure
:title      → Display name (optional)
:query      → Datalog query
:inputs     → Dynamic parameters
:collapsed? → Show/hide results
:breadcrumb-show? → Show page context
:view       → Custom display (optional)
```

### Simple Query

**Basic Pattern:**

```clojure
#+BEGIN_QUERY
{:title "All TODOs"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]]}
#+END_QUERY
```

**What it does:**

- Finds all blocks (?b)
- With marker = "TODO"
- Returns full block data

---

## Task Queries

### All Tasks by Status

**All TODOs:**

```clojure
#+BEGIN_QUERY
{:title "📋 All TODO Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]]}
#+END_QUERY
```

**All DOING:**

```clojure
#+BEGIN_QUERY
{:title "🔄 In Progress"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "DOING"]]}
#+END_QUERY
```

**All DONE (Last 7 Days):**

```clojure
#+BEGIN_QUERY
{:title "✅ Completed Recently"
 :query [:find (pull ?b [*])
         :in $ ?start
         :where
         [?b :block/marker "DONE"]
         [?b :block/page ?p]
         [?p :block/journal? true]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]]
 :inputs [:7d-before]}
#+END_QUERY
```

### Tasks with Dates

**Due Today:**

```clojure
#+BEGIN_QUERY
{:title "📅 Due Today"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/deadline ?d]
         [(= ?d ?today)]
         [?b :block/marker ?m]
         [(not= ?m "DONE")]]
 :inputs [:today]}
#+END_QUERY
```

**Due This Week:**

```clojure
#+BEGIN_QUERY
{:title "📅 Due This Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/deadline ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]
         [?b :block/marker ?m]
         [(not= ?m "DONE")]]
 :inputs [:today :7d-after]}
#+END_QUERY
```

**Overdue Tasks:**

```clojure
#+BEGIN_QUERY
{:title "⚠️ Overdue"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/deadline ?d]
         [(< ?d ?today)]
         [?b :block/marker ?m]
         [(not= ?m "DONE")]
         [(not= ?m "CANCELLED")]]
 :inputs [:today]}
#+END_QUERY
```

**Scheduled Today or Earlier:**

```clojure
#+BEGIN_QUERY
{:title "⏰ Ready to Start"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/scheduled ?d]
         [(<= ?d ?today)]
         [?b :block/marker "TODO"]]
 :inputs [:today]}
#+END_QUERY
```

### Tasks by Priority

**Priority A (High):**

```clojure
#+BEGIN_QUERY
{:title "🔥 High Priority"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]
         [?b :block/priority "A"]]}
#+END_QUERY
```

**All Priorities Sorted:**

```clojure
#+BEGIN_QUERY
{:title "📊 Tasks by Priority"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/priority ?p]]
 :result-transform (fn [result]
                     (sort-by
                       #(get-in % [:block/priority])
                       result))}
#+END_QUERY
```

### Combined Task Views

**Today's Agenda:**

```clojure
#+BEGIN_QUERY
{:title "📅 Today's Agenda"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         (or
           [?b :block/scheduled ?d]
           [?b :block/deadline ?d])
         [(= ?d ?today)]
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING" "WAITING"} ?m)]]
 :inputs [:today]
 :collapsed? false}
#+END_QUERY
```

**All Active Tasks:**

```clojure
#+BEGIN_QUERY
{:title "🎯 All Active Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING" "WAITING" "NOW"} ?m)]]}
#+END_QUERY
```

---

## Page & Block Queries

### Page Queries

**All Pages with Tag:**

```clojure
#+BEGIN_QUERY
{:title "Pages Tagged #project"
 :query [:find (pull ?p [*])
         :where
         [?p :block/name]
         [?b :block/page ?p]
         [?b :block/refs ?t]
         [?t :block/name "project"]]}
#+END_QUERY
```

**Pages in Namespace:**

```clojure
#+BEGIN_QUERY
{:title "📁 All Project Pages"
 :query [:find (pull ?p [*])
         :where
         [?p :block/namespace ?ns]
         [?ns :block/name "projects"]]}
#+END_QUERY
```

**Recently Modified Pages:**

```clojure
#+BEGIN_QUERY
{:title "📝 Recently Modified"
 :query [:find (pull ?p [*])
         :in $ ?since
         :where
         [?p :block/name]
         [?b :block/page ?p]
         [?b :block/updated-at ?t]
         [(> ?t ?since)]]
 :inputs [:7d-before]
 :result-transform (fn [result]
                     (take 20
                       (sort-by #(get % :block/updated-at)
                                >
                                result)))}
#+END_QUERY
```

### Block Queries

**Blocks with Specific Content:**

```clojure
#+BEGIN_QUERY
{:title "🔍 Blocks Containing 'important'"
 :query [:find (pull ?b [*])
         :where
         [?b :block/content ?c]
         [(clojure.string/includes? ?c "important")]]}
#+END_QUERY
```

**Top-Level Blocks Only:**

```clojure
#+BEGIN_QUERY
{:title "📄 Top-Level Blocks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/parent ?p]
         [?p :block/name]  ;; parent is a page
         ]}
#+END_QUERY
```

**Blocks by Length:**

```clojure
#+BEGIN_QUERY
{:title "📏 Long Blocks (>500 chars)"
 :query [:find (pull ?b [*])
         :where
         [?b :block/content ?c]
         [(count ?c) ?len]
         [(> ?len 500)]]}
#+END_QUERY
```

---

## Date & Time Queries

### Journal Queries

**Last 7 Days:**

```clojure
#+BEGIN_QUERY
{:title "📆 Last 7 Days"
 :query [:find (pull ?p [*])
         :in $ ?start ?end
         :where
         [?p :block/journal? true]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:7d-before :today]}
#+END_QUERY
```

**This Week's Journals:**

```clojure
#+BEGIN_QUERY
{:title "📅 This Week"
 :query [:find (pull ?p [*])
         :in $ ?start ?end
         :where
         [?p :block/journal? true]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:monday :sunday]}
#+END_QUERY
```

**Specific Date Range:**

```clojure
#+BEGIN_QUERY
{:title "📆 January 2026"
 :query [:find (pull ?p [*])
         :where
         [?p :block/journal? true]
         [?p :block/journal-day ?d]
         [(>= ?d 20260101)]
         [(<= ?d 20260131)]]}
#+END_QUERY
```

### Created/Updated Queries

**Created Today:**

```clojure
#+BEGIN_QUERY
{:title "🆕 Created Today"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/created-at ?t]
         [(>= ?t ?start)]
         [(<= ?t ?end)]]
 :inputs [:start-of-today-ms :end-of-today-ms]}
#+END_QUERY
```

**Updated This Week:**

```clojure
#+BEGIN_QUERY
{:title "🔄 Updated This Week"
 :query [:find (pull ?b [*])
         :in $ ?start
         :where
         [?b :block/updated-at ?t]
         [(> ?t ?start)]]
 :inputs [:7d-before]
 :result-transform (fn [result]
                     (sort-by #(get % :block/updated-at)
                              >
                              result))}
#+END_QUERY
```

---

## Property Queries

### Simple Property Queries

**Has Specific Property:**

```clojure
#+BEGIN_QUERY
{:title "Blocks with 'status' property"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :status)]]}
#+END_QUERY
```

**Property Equals Value:**

```clojure
#+BEGIN_QUERY
{:title "Status = 'active'"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :status) ?s]
         [(= ?s "active")]]}
#+END_QUERY
```

**Multiple Property Conditions:**

```clojure
#+BEGIN_QUERY
{:title "Active High-Priority Items"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :status) "active"]
         [(get ?props :priority) "high"]]}
#+END_QUERY
```

### Property with Page References

**Property Links to Page:**

```clojure
#+BEGIN_QUERY
{:title "Tasks for Project Alpha"
 :query [:find (pull ?b [*])
         :in $ ?project
         :where
         [?b :block/marker "TODO"]
         [?b :block/refs ?p]
         [?p :block/name ?project]]
 :inputs ["projects/alpha"]}
#+END_QUERY
```

**Has Owner Property:**

```clojure
#+BEGIN_QUERY
{:title "Tasks by Owner"
 :query [:find (pull ?b [*])
         :in $ ?owner
         :where
         [?b :block/marker "TODO"]
         [?b :block/refs ?p]
         [?p :block/name ?owner]]
 :inputs ["person/sarah"]}
#+END_QUERY
```

### Property Aggregations

**Count by Property Value:**

```clojure
#+BEGIN_QUERY
{:title "Task Count by Status"
 :query [:find ?status (count ?b)
         :where
         [?b :block/marker "TODO"]
         [?b :block/properties ?props]
         [(get ?props :status) ?status]]
 :view (fn [rows]
         [:table
          [:thead [:tr [:th "Status"] [:th "Count"]]]
          [:tbody
           (for [[status count] rows]
             [:tr [:td status] [:td count]])]])}
#+END_QUERY
```

---

## Tag Queries

### Basic Tag Queries

**Blocks Tagged with #tag:**

```clojure
#+BEGIN_QUERY
{:title "#important blocks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/refs ?t]
         [?t :block/name "important"]]}
#+END_QUERY
```

**Multiple Tags (AND):**

```clojure
#+BEGIN_QUERY
{:title "#project AND #urgent"
 :query [:find (pull ?b [*])
         :where
         [?b :block/refs ?t1]
         [?t1 :block/name "project"]
         [?b :block/refs ?t2]
         [?t2 :block/name "urgent"]]}
#+END_QUERY
```

**Multiple Tags (OR):**

```clojure
#+BEGIN_QUERY
{:title "#project OR #work"
 :query [:find (pull ?b [*])
         :where
         [?b :block/refs ?t]
         [?t :block/name ?tag]
         [(contains? #{"project" "work"} ?tag)]]}
#+END_QUERY
```

### Tag Combinations

**Tagged but Not Completed:**

```clojure
#+BEGIN_QUERY
{:title "#project Tasks (Not Done)"
 :query [:find (pull ?b [*])
         :where
         [?b :block/refs ?t]
         [?t :block/name "project"]
         [?b :block/marker ?m]
         [(not= ?m "DONE")]]}
#+END_QUERY
```

**Exclude Tag:**

```clojure
#+BEGIN_QUERY
{:title "TODOs without #archived"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         (not
           [?b :block/refs ?t]
           [?t :block/name "archived"])]}
#+END_QUERY
```

---

## Reference Queries

### Backlink Queries

**Blocks Referencing Page:**

```clojure
#+BEGIN_QUERY
{:title "References to [[Project Alpha]]"
 :query [:find (pull ?b [*])
         :in $ ?page
         :where
         [?b :block/refs ?p]
         [?p :block/name ?page]]
 :inputs ["projects/alpha"]}
#+END_QUERY
```

**Block References to Specific Block:**

```clojure
#+BEGIN_QUERY
{:title "References to Block"
 :query [:find (pull ?b [*])
         :in $ ?block-uuid
         :where
         [?b :block/refs ?target]
         [?target :block/uuid ?block-uuid]]
 :inputs [#uuid "abc-123-def-456"]}
#+END_QUERY
```

### Connection Queries

**Pages Linked to Each Other:**

```clojure
#+BEGIN_QUERY
{:title "Mutual Links"
 :query [:find ?p1-name ?p2-name
         :where
         [?b1 :block/page ?p1]
         [?b1 :block/refs ?p2]
         [?b2 :block/page ?p2]
         [?b2 :block/refs ?p1]
         [?p1 :block/name ?p1-name]
         [?p2 :block/name ?p2-name]]}
#+END_QUERY
```

**Orphan Pages (No Backlinks):**

```clojure
#+BEGIN_QUERY
{:title "🔍 Orphan Pages"
 :query [:find (pull ?p [*])
         :where
         [?p :block/name]
         (not [?b :block/refs ?p])]}
#+END_QUERY
```

---

## Advanced Patterns

### Hierarchical Queries

**Parent-Child Relationship:**

```clojure
#+BEGIN_QUERY
{:title "Task with Subtasks"
 :query [:find (pull ?parent [* {:block/_parent ...}])
         :where
         [?parent :block/marker "TODO"]
         [?child :block/parent ?parent]
         [?child :block/marker]]}
#+END_QUERY
```

**Depth-Limited Hierarchy:**

```clojure
#+BEGIN_QUERY
{:title "Two Levels Deep"
 :query [:find (pull ?b [:block/content
                          {:block/_parent
                           [:block/content
                            {:block/_parent
                             [:block/content]}]}])
         :where
         [?b :block/marker "TODO"]]}
#+END_QUERY
```

### Text Search Queries

**Case-Insensitive Search:**

```clojure
#+BEGIN_QUERY
{:title "Search 'database'"
 :query [:find (pull ?b [*])
         :where
         [?b :block/content ?c]
         [(clojure.string/lower-case ?c) ?lc]
         [(clojure.string/includes? ?lc "database")]]}
#+END_QUERY
```

**Regex Pattern:**

```clojure
#+BEGIN_QUERY
{:title "Blocks with URLs"
 :query [:find (pull ?b [*])
         :where
         [?b :block/content ?c]
         [(re-find #"https?://[^\s]+" ?c)]]}
#+END_QUERY
```

### Statistical Queries

**Count Tasks by Status:**

```clojure
#+BEGIN_QUERY
{:title "📊 Task Statistics"
 :query [:find ?marker (count ?b)
         :where
         [?b :block/marker ?marker]]
 :view (fn [rows]
         [:div
          [:h3 "Task Count by Status"]
          [:table
           [:thead [:tr [:th "Status"] [:th "Count"]]]
           [:tbody
            (for [[status count] (sort-by second > rows)]
              [:tr [:td status] [:td count]])]]])}
#+END_QUERY
```

**Average Time Estimate:**

```clojure
#+BEGIN_QUERY
{:title "⏱️ Average Time Estimate"
 :query [:find (avg ?time)
         :where
         [?b :block/marker "TODO"]
         [?b :block/properties ?props]
         [(get ?props :time-estimate) ?time-str]
         [(js/parseInt ?time-str) ?time]]}
#+END_QUERY
```

### Custom Sorting

**Sort by Priority then Deadline:**

```clojure
#+BEGIN_QUERY
{:title "📋 Sorted Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]]
 :result-transform (fn [result]
                     (sort-by
                       (fn [b]
                         [(get b :block/priority "Z")
                          (get b :block/deadline 99999999)])
                       result))}
#+END_QUERY
```

---

## Query Optimization

### Performance Tips

**Use Specific Filters First:**

```clojure
❌ Slow:
[:where
 [?b :block/content ?c]
 [?b :block/marker "TODO"]]

✅ Fast:
[:where
 [?b :block/marker "TODO"]  ;; Filter early
 [?b :block/content ?c]]
```

**Limit Results:**

```clojure
:result-transform (fn [result]
                    (take 50 result))
```

**Use Indexes:**

```clojure
✅ Indexed (fast):
- :block/marker
- :block/deadline
- :block/scheduled
- :block/priority
- :block/uuid
- :block/refs

❌ Not indexed (slower):
- :block/content (full scan)
- String includes/regex
```

### Debugging Queries

**Add Intermediate Finds:**

```clojure
;; Debug: See what ?marker values exist
:query [:find ?marker
        :where
        [?b :block/marker ?marker]]

;; Then add full query
:query [:find (pull ?b [*])
        :where
        [?b :block/marker "TODO"]]
```

**Check Input Values:**

```clojure
;; Log inputs
:query [:find ?input
        :in $ ?input
        :where
        [(identity ?input)]]
:inputs [:today]
```

---

## Query Templates

### Copy-Paste Templates

**Page Query Template:**

```clojure
#+BEGIN_QUERY
{:title "CHANGE_ME"
 :query [:find (pull ?p [*])
         :in $ ?input
         :where
         [?p :block/name]
         ;; Add conditions here
         ]
 :inputs ["CHANGE_ME"]}
#+END_QUERY
```

**Block Query Template:**

```clojure
#+BEGIN_QUERY
{:title "CHANGE_ME"
 :query [:find (pull ?b [*])
         :in $ ?input
         :where
         [?b :block/content ?c]
         ;; Add conditions here
         ]
 :inputs ["CHANGE_ME"]
 :collapsed? false}
#+END_QUERY
```

**Task Query Template:**

```clojure
#+BEGIN_QUERY
{:title "CHANGE_ME"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         ;; Add conditions here
         ]}
#+END_QUERY
```

---

## Common Input Values

```clojure
:today           → Today's date (journal day)
:yesterday       → Yesterday
:tomorrow        → Tomorrow
:7d-before       → 7 days ago
:7d-after        → 7 days from now
:monday          → This Monday
:tuesday         → This Tuesday
:start-of-today-ms → Today 00:00:00 (milliseconds)
:end-of-today-ms   → Today 23:59:59 (milliseconds)
```

---

## Next Steps

**Master Queries:**

1. Start with simple patterns
2. Copy and modify templates
3. Test incrementally
4. Learn Datalog basics
5. Build custom views

**Related:**

- [Query System Guide](../developer/query-system.md)
- [Advanced Queries Tutorial](../../tutorials/advanced/advanced-queries.md)
- [Task Management](../../how-to/workflows/task-management-workflows.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
