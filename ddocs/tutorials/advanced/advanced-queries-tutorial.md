---
title: 'Advanced Queries Tutorial'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Tutorial'
platform: 'Desktop + Mobile'
related-docs:
  - '[Query Patterns Reference](../../reference/advanced/query-patterns.md)'
  - '[Properties Reference](../../reference/data/properties-reference.md)'
  - '[Schema System](../../explanation/developer/schema-system.md)'
tags: ['queries', 'datalog', 'tutorial', 'advanced']
---

# Advanced Queries Tutorial

Learn to write powerful Datalog queries in Logseq. This hands-on tutorial takes you from basic queries to advanced patterns, building your skills step-by-step.

**What You'll Learn:**

- Datalog fundamentals
- Query structure and syntax
- Building complex conditions
- Using inputs and transforms
- Aggregation and grouping
- Custom views and formatting

**Prerequisites:**

- Basic Logseq usage
- Understanding of blocks and properties
- Familiarity with basic queries (recommended)

**Time to Complete:** 90-120 minutes

---

## Table of Contents

1. [Datalog Basics](#datalog-basics)
2. [Query Structure](#query-structure)
3. [Finding Blocks](#finding-blocks)
4. [Filtering with Where](#filtering-with-where)
5. [Working with Properties](#working-with-properties)
6. [Using Inputs](#using-inputs)
7. [Transforming Results](#transforming-results)
8. [Aggregation & Grouping](#aggregation--grouping)
9. [Custom Views](#custom-views)
10. [Advanced Patterns](#advanced-patterns)

---

## Datalog Basics

### What is Datalog?

**Datalog** is a declarative query language based on logic programming. Instead of telling the computer _how_ to find data, you describe _what_ you want to find.

**Example Comparison:**

```javascript
// Imperative (how to find)
let todos = []
for (let block of allBlocks) {
  if (block.marker === "TODO") {
    todos.push(block)
  }
}

// Declarative (what to find)
[:find ?b
 :where
 [?b :block/marker "TODO"]]
```

### Key Concepts

**1. Facts:**

```markdown
In Logseq, everything is stored as facts:

[block-123 :block/marker "TODO"]
[block-123 :block/content "Write tutorial"]
[block-123 :block/page page-456]

Format: [entity attribute value]
```

**2. Variables:**

```markdown
Variables start with ?

?b = any block
?p = any page
?marker = any marker value

Variables get bound (assigned values) during query
```

**3. Patterns:**

```markdown
Patterns match facts:

[?b :block/marker "TODO"]
Matches: Any block that has marker "TODO"

[?b :block/marker ?m]
Matches: Any block with any marker
Binds the marker value to ?m
```

### Your First Query

**Find All TODOs:**

```clojure
#+BEGIN_QUERY
{:title "My first query"
 :query [:find ?b
         :where
         [?b :block/marker "TODO"]]}
#+END_QUERY
```

**What This Does:**

```markdown
:find ?b
→ Return blocks

:where
→ That match this condition:

[?b :block/marker "TODO"]
→ Block has marker "TODO"
```

---

## Query Structure

### Basic Template

**Every Query Has:**

```clojure
{:title "Query Title"          ; Optional: Display name
 :query [:find ?variable        ; What to return
         :where                 ; Conditions
         [pattern]              ; Match patterns
         [pattern]]
 :inputs [values]               ; Optional: Parameters
 :collapsed? false}             ; Optional: Show expanded
```

### The :find Clause

**What to Return:**

```clojure
; Return blocks
:find ?b

; Return specific values
:find ?marker ?content

; Return with full block data
:find (pull ?b [*])

; Return and count
:find (count ?b)
```

**Example:**

```clojure
; Just block IDs
:find ?b
:where
[?b :block/marker "TODO"]

; Block content only
:find ?content
:where
[?b :block/marker "TODO"]
[?b :block/content ?content]

; Full block data (most useful)
:find (pull ?b [*])
:where
[?b :block/marker "TODO"]
```

### The :where Clause

**Conditions Must All Match:**

```clojure
:where
[?b :block/marker "TODO"]        ; AND
[?b :block/priority "A"]         ; AND
[?b :block/deadline ?d]          ; AND
[(< ?d [:today])]                ; AND
```

**All four conditions must be true for block to match.**

---

## Finding Blocks

### Exercise 1: Find Blocks by Marker

**Task:** Find all blocks with marker "DOING"

<details>
<summary>Try it yourself first, then click to see solution</summary>

```clojure
#+BEGIN_QUERY
{:title "All DOING tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "DOING"]]}
#+END_QUERY
```

</details>

### Exercise 2: Find Multiple Markers

**Task:** Find all TODO or DOING tasks

**Hint:** Use `contains?` with a set

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "Active tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]]}
#+END_QUERY
```

**Explanation:**

- `[?b :block/marker ?m]` - Bind marker to ?m
- `[(contains? #{"TODO" "DOING"} ?m)]` - Check if ?m is in set
- `#{"TODO" "DOING"}` - Set literal in Clojure
</details>

### Exercise 3: Find Blocks with Content

**Task:** Find blocks containing the word "important"

**Hint:** Use `clojure.string/includes?`

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "Important items"
 :query [:find (pull ?b [*])
         :where
         [?b :block/content ?content]
         [(clojure.string/includes? ?content "important")]]}
#+END_QUERY
```

</details>

---

## Filtering with Where

### Multiple Conditions (AND)

**All conditions must match:**

```clojure
:where
[?b :block/marker "TODO"]
[?b :block/priority "A"]
```

**Both conditions required.**

### Using OR Logic

**Match any condition:**

```clojure
; OR with contains?
[?b :block/marker ?m]
[(contains? #{"TODO" "DOING" "WAITING"} ?m)]

; OR with or clause (less common)
(or [?b :block/marker "TODO"]
    [?b :block/marker "DOING"])
```

### Negation (NOT)

**Exclude matches:**

```clojure
:where
[?b :block/marker ?m]
[(contains? #{"TODO" "DOING"} ?m)]
(not [?b :block/priority "C"])  ; Exclude low priority
```

### Exercise 4: Complex Filtering

**Task:** Find TODO tasks that:

- Have priority A or B
- Do NOT contain "someday"

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "High priority actionable tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/priority ?p]
         [(contains? #{"A" "B"} ?p)]
         [?b :block/content ?content]
         (not [(clojure.string/includes? ?content "someday")])]
 :collapsed? false}
#+END_QUERY
```

</details>

---

## Working with Properties

### Basic Property Queries

**Has Property:**

```clojure
:where
[?b :block/properties ?props]
[(get ?props :status)]  ; Has status property
```

**Property Equals Value:**

```clojure
:where
[?b :block/properties ?props]
[(get ?props :status) ?status]
[(= ?status "active")]

; Or shorter:
[(get ?props :status) "active"]
```

### Multiple Properties

**Task with Project and Owner:**

```clojure
:where
[?b :block/marker "TODO"]
[?b :block/properties ?props]
[(get ?props :project) ?project]
[(get ?props :owner) ?owner]
```

### Exercise 5: Property Filtering

**Task:** Find blocks where:

- Has property `status`
- Status equals "active"
- Has property `priority`
- Priority equals "high"

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "Active high-priority items"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :status) "active"]
         [(get ?props :priority) "high"]]}
#+END_QUERY
```

</details>

### Property with Page Links

**When property value is page link:**

```clojure
; Property: project:: [[Projects/Alpha]]
:where
[?b :block/properties ?props]
[(get ?props :project) ?project-ref]  ; Get the reference
[?project-ref :block/name ?project-name]  ; Get page name
[(= ?project-name "projects/alpha")]  ; Match (lowercase)
```

**Or using :block/refs:**

```clojure
; Simpler approach
:where
[?b :block/marker "TODO"]
[?b :block/refs ?p]
[?p :block/name "projects/alpha"]
```

---

## Using Inputs

### What Are Inputs?

**Inputs** let you parameterize queries with dynamic values.

**Basic Syntax:**

```clojure
{:query [:find (pull ?b [*])
         :in $ ?input-value      ; Define input
         :where
         [?b :block/marker ?input-value]]  ; Use input
 :inputs ["TODO"]}               ; Provide value
```

### Built-in Inputs

**Logseq provides useful inputs:**

```clojure
:inputs [:today]           ; Current date (journal page)
:inputs [:yesterday]       ; Yesterday
:inputs [:tomorrow]        ; Tomorrow
:inputs [:7d-before]       ; 7 days ago
:inputs [:7d-after]        ; 7 days from now
:inputs [:monday]          ; This Monday
:inputs [:sunday]          ; This Sunday
:inputs [:current-page]    ; Name of current page
:inputs [:start-of-today-ms]   ; Today 00:00 (milliseconds)
:inputs [:end-of-today-ms]     ; Today 23:59 (milliseconds)
```

### Date Queries with Inputs

**Due Today:**

```clojure
#+BEGIN_QUERY
{:title "Due today"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/marker "TODO"]
         [?b :block/deadline ?d]
         [(= ?d ?today)]]
 :inputs [:today]}
#+END_QUERY
```

**Due This Week:**

```clojure
#+BEGIN_QUERY
{:title "Due this week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/marker "TODO"]
         [?b :block/deadline ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:monday :sunday]}
#+END_QUERY
```

### Exercise 6: Overdue Tasks

**Task:** Find TODO tasks with deadline before today

**Hint:** Use `<` operator and `:today` input

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "Overdue tasks"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/marker "TODO"]
         [?b :block/deadline ?d]
         [(< ?d ?today)]]
 :inputs [:today]}
#+END_QUERY
```

</details>

### Multiple Inputs

**Custom Input Values:**

```clojure
#+BEGIN_QUERY
{:title "Tasks for specific project and person"
 :query [:find (pull ?b [*])
         :in $ ?project ?person
         :where
         [?b :block/marker "TODO"]
         [?b :block/refs ?proj-page]
         [?proj-page :block/name ?project]
         [?b :block/refs ?person-page]
         [?person-page :block/name ?person]]
 :inputs ["projects/alpha" "person/sarah"]}
#+END_QUERY
```

---

## Transforming Results

### The :result-transform Function

**Transform results before display:**

```clojure
{:query [:find (pull ?b [*])
         :where ...]
 :result-transform (fn [result]
                     ; Transform result here
                     result)}
```

### Sorting Results

**Sort by Property:**

```clojure
:result-transform (fn [result]
                    (sort-by
                      (fn [b]
                        (get-in b [:block/properties :priority]))
                      result))
```

**Sort by Multiple Fields:**

```clojure
:result-transform (fn [result]
                    (sort-by
                      (fn [b]
                        [(get-in b [:block/properties :priority])
                         (get b :block/deadline)])
                      result))
```

**Reverse Sort:**

```clojure
:result-transform (fn [result]
                    (sort-by
                      (fn [b]
                        (get b :block/created-at))
                      > ; Descending order
                      result))
```

### Filtering Results

**Post-query filtering:**

```clojure
:result-transform (fn [result]
                    (filter
                      (fn [b]
                        (< (count (get b :block/content)) 100))
                      result))
```

### Limiting Results

**Take first N:**

```clojure
:result-transform (fn [result]
                    (take 10 result))
```

### Exercise 7: Sort and Limit

**Task:** Find TODO tasks, sort by priority (A, B, C), take top 5

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "Top 5 priority tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/priority ?p]]
 :result-transform (fn [result]
                     (->> result
                          (sort-by #(get % :block/priority))
                          (take 5)))}
#+END_QUERY
```

**Note:** `->>` is threading macro, passes result through functions

</details>

---

## Aggregation & Grouping

### Counting

**Count Total:**

```clojure
#+BEGIN_QUERY
{:title "Total TODO count"
 :query [:find (count ?b)
         :where
         [?b :block/marker "TODO"]]}
#+END_QUERY
```

**Count by Group:**

```clojure
#+BEGIN_QUERY
{:title "Count by marker"
 :query [:find ?marker (count ?b)
         :where
         [?b :block/marker ?marker]]}
#+END_QUERY
```

### Grouping with Aggregates

**Count by Status:**

```clojure
#+BEGIN_QUERY
{:title "Tasks by status"
 :query [:find ?status (count ?b)
         :where
         [?b :block/marker "TODO"]
         [?b :block/properties ?props]
         [(get ?props :status) ?status]]}
#+END_QUERY
```

**Count by Project:**

```clojure
#+BEGIN_QUERY
{:title "Tasks by project"
 :query [:find ?project-name (count ?b)
         :where
         [?b :block/marker "TODO"]
         [?b :block/refs ?project]
         [?project :block/name ?project-name]
         [(clojure.string/starts-with? ?project-name "projects/")]]}
#+END_QUERY
```

### Other Aggregates

**Sum, Min, Max, Average:**

```clojure
; Sum time estimates
:find (sum ?time)
:where
[?b :block/marker "TODO"]
[?b :block/properties ?props]
[(get ?props :time-estimate) ?time-str]
[(js/parseInt ?time-str) ?time]

; Average
:find (avg ?time)
; ... same where clause

; Min/Max
:find (min ?time) (max ?time)
```

### Exercise 8: Project Statistics

**Task:** Count TODO tasks by project, show project name and count

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "TODO count by project"
 :query [:find ?project-name (count ?b)
         :where
         [?b :block/marker "TODO"]
         [?b :block/refs ?p]
         [?p :block/name ?project-name]
         [(clojure.string/starts-with? ?project-name "projects/")]]
 :result-transform (fn [result]
                     (sort-by second > result))}
#+END_QUERY
```

</details>

---

## Custom Views

### The :view Function

**Custom rendering:**

```clojure
{:query [...]
 :view (fn [rows]
         [:div
          ; Your custom HTML here
          ])}
```

### Table View

**Display as Table:**

```clojure
:view (fn [rows]
        [:table
         [:thead
          [:tr
           [:th "Status"]
           [:th "Count"]]]
         [:tbody
          (for [[status count] rows]
            [:tr
             [:td status]
             [:td count]])]])
```

### Custom Formatting

**With Links and Styling:**

```clojure
:view (fn [rows]
        [:div
         (for [row rows]
           [:div.my-custom-class
            [:span {:style {:color "blue"}}
             (get row :block/content)]
            [:a {:href (str "#/page/" (get-in row [:block/page :block/name]))}
             "View Page"]])])
```

### Exercise 9: Dashboard View

**Task:** Create query showing task counts by priority with custom table view

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "Task Priority Dashboard"
 :query [:find ?priority (count ?b)
         :where
         [?b :block/marker "TODO"]
         [?b :block/priority ?priority]]
 :view (fn [rows]
         [:table
          [:thead
           [:tr
            [:th "Priority"]
            [:th "Count"]
            [:th "Urgency"]]]
          [:tbody
           (for [[priority count] (sort-by first rows)]
             [:tr
              [:td [:strong (str "[#" priority "]")]]
              [:td count]
              [:td (case priority
                     "A" "🔴 High"
                     "B" "🟡 Medium"
                     "C" "🟢 Low"
                     "Unknown")]])]])}
#+END_QUERY
```

</details>

---

## Advanced Patterns

### Hierarchical Queries

**Find Block with Children:**

```clojure
#+BEGIN_QUERY
{:title "Blocks with children"
 :query [:find (pull ?b [*
                         {:block/_parent
                          [:block/content]}])
         :where
         [?b :block/marker "TODO"]]}
#+END_QUERY
```

**Explanation:**

- `:block/_parent` - Reverse reference
- Get blocks where ?b is the parent
- Includes child block content

### Recursive Pulls

**All Descendants:**

```clojure
:find (pull ?b [*
                {:block/_parent ...}])
```

**Three dots `...` mean recursive - get children, children's children, etc.**

### Pattern Matching

**Regex Search:**

```clojure
:where
[?b :block/content ?content]
[(re-find #"https?://[^\s]+" ?content) ?url]
```

**Finds blocks with URLs.**

### Join Queries

**Tasks with Page Properties:**

```clojure
#+BEGIN_QUERY
{:title "Tasks from pages tagged 'urgent'"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/page ?p]
         [?p :block/refs ?tag]
         [?tag :block/name "urgent"]]}
#+END_QUERY
```

### Exercise 10: Advanced - Task Dependencies

**Task:** Find TODO tasks that depend on other blocks (using `depends-on::` property with block reference)

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "Tasks with dependencies"
 :query [:find (pull ?b [*
                         {:block/refs
                          [:block/content
                           :block/marker]}])
         :where
         [?b :block/marker "TODO"]
         [?b :block/properties ?props]
         [(get ?props :depends-on)]]
 :result-transform (fn [result]
                     (filter
                       (fn [b]
                         (get-in b [:block/properties :depends-on]))
                       result))}
#+END_QUERY
```

</details>

---

## Practical Examples

### Example 1: Today's Agenda

**All tasks for today:**

```clojure
#+BEGIN_QUERY
{:title "📅 Today's Agenda"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]
         (or-join [?b ?today]
           [?b :block/scheduled ?today]
           [?b :block/deadline ?today])]
 :inputs [:today]
 :result-transform (fn [result]
                     (sort-by
                       (fn [b]
                         (or (get b :block/priority) "Z"))
                       result))}
#+END_QUERY
```

### Example 2: Project Dashboard

**Tasks by project with counts:**

```clojure
#+BEGIN_QUERY
{:title "📊 Project Dashboard"
 :query [:find ?project
                (pull ?p [*])
                (count ?b)
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]
         [?b :block/refs ?p]
         [?p :block/name ?project]
         [(clojure.string/starts-with? ?project "projects/")]]
 :view (fn [rows]
         [:table
          [:thead
           [:tr
            [:th "Project"]
            [:th "Active Tasks"]
            [:th "Link"]]]
          [:tbody
           (for [[project-name project-data count] (sort-by #(nth % 2) > rows)]
             [:tr
              [:td (clojure.string/replace project-name "projects/" "")]
              [:td count]
              [:td [:a {:href (str "#/page/" project-name)} "→"]]])]])}
#+END_QUERY
```

### Example 3: Weekly Review

**Completed tasks this week:**

```clojure
#+BEGIN_QUERY
{:title "✅ Completed This Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/marker "DONE"]
         [?b :block/page ?p]
         [?p :block/journal? true]
         [?p :block/journal-day ?date]
         [(>= ?date ?start)]
         [(<= ?date ?end)]]
 :inputs [:monday :sunday]
 :result-transform (fn [result]
                     (sort-by
                       #(get-in % [:block/page :block/journal-day])
                       >
                       result))
 :collapsed? false}
#+END_QUERY
```

### Example 4: High-Value Quick Wins

**High priority, low effort tasks:**

```clojure
#+BEGIN_QUERY
{:title "⚡ Quick Wins (High Value, Low Effort)"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/priority "A"]
         [?b :block/properties ?props]
         [(get ?props :time-estimate) ?time-str]
         [(js/parseInt ?time-str) ?time]
         [(<= ?time 30)]]
 :result-transform (fn [result]
                     (sort-by
                       #(js/parseInt (get-in % [:block/properties :time-estimate]))
                       result))}
#+END_QUERY
```

---

## Debugging Queries

### Common Errors

**Error: "No protocol method IFn.-invoke"**

```clojure
; Wrong:
[(get ?props :status "active")]  ; Too many args

; Right:
[(get ?props :status) "active"]
```

**Error: Variable not bound**

```clojure
; Wrong:
:where
[?b :block/priority ?p]  ; ?b never defined!

; Right:
:where
[?b :block/marker "TODO"]  ; ?b defined
[?b :block/priority ?p]    ; Now can use ?b
```

### Debugging Techniques

**1. Simplify:**

```clojure
; Start simple
:where
[?b :block/marker "TODO"]

; Add conditions one at a time
:where
[?b :block/marker "TODO"]
[?b :block/priority ?p]  ; Add

; Then
:where
[?b :block/marker "TODO"]
[?b :block/priority ?p]
[(= ?p "A")]  ; Add
```

**2. Check Variables:**

```clojure
; See what variables contain
:find ?b ?marker ?priority
:where
[?b :block/marker ?marker]
[?b :block/priority ?priority]

; This shows actual values
```

**3. Use Intermediate Finds:**

```clojure
; Check if blocks exist
:find (count ?b)
:where
[?b :block/marker "TODO"]

; Returns number, confirms blocks found
```

---

## Best Practices

### Performance

**✅ Do:**

```clojure
; Use specific filters first
:where
[?b :block/marker "TODO"]        ; Fast (indexed)
[?b :block/deadline ?d]          ; Fast (indexed)
[(< ?d [:today])]                ; Fast (comparison)
[?b :block/content ?c]           ; Then content
[(clojure.string/includes? ?c "important")]  ; Last
```

**❌ Avoid:**

```clojure
; Content search first = slow
:where
[?b :block/content ?c]
[(clojure.string/includes? ?c "important")]
[?b :block/marker "TODO"]
```

### Maintainability

**Add Comments:**

```clojure
{:title "My Complex Query"
 :query [:find (pull ?b [*])
         :where
         ; Get active tasks
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]

         ; With high priority
         [?b :block/priority "A"]

         ; Due this week
         [?b :block/deadline ?d]
         [(>= ?d [:monday])]
         [(<= ?d [:sunday])]]}
```

### Reusability

**Use Inputs for Flexibility:**

```clojure
; Flexible
:in $ ?project
:inputs ["projects/alpha"]

; Can change input without editing query
```

---

## Practice Challenges

### Challenge 1: Study Time Tracker

Create query finding all blocks tagged `#study` from last 7 days, showing total study time.

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "📚 Study Time This Week"
 :query [:find (sum ?duration)
         :in $ ?start
         :where
         [?b :block/refs ?tag]
         [?tag :block/name "study"]
         [?b :block/properties ?props]
         [(get ?props :duration) ?dur-str]
         [(js/parseInt ?dur-str) ?duration]
         [?b :block/page ?p]
         [?p :block/journal-day ?date]
         [(>= ?date ?start)]]
 :inputs [:7d-before]}
#+END_QUERY
```

</details>

### Challenge 2: Stale Tasks

Find TODO tasks not updated in last 30 days.

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "🕸️ Stale Tasks (30+ days)"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/updated-at ?updated]
         [(- (js/Date.now) ?updated) ?age]
         [(> ?age 2592000000)]]  ; 30 days in ms
 :result-transform (fn [result]
                     (sort-by
                       #(get % :block/updated-at)
                       result))}
#+END_QUERY
```

</details>

### Challenge 3: Team Workload

Count active tasks by owner, show as table.

<details>
<summary>Solution</summary>

```clojure
#+BEGIN_QUERY
{:title "👥 Team Workload"
 :query [:find ?owner (count ?b)
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]
         [?b :block/refs ?person]
         [?person :block/name ?owner]
         [(clojure.string/starts-with? ?owner "person/")]]
 :view (fn [rows]
         [:table
          [:thead
           [:tr
            [:th "Team Member"]
            [:th "Active Tasks"]]]
          [:tbody
           (for [[owner count] (sort-by second > rows)]
             [:tr
              [:td (clojure.string/replace owner "person/" "")]
              [:td count]])]])}
#+END_QUERY
```

</details>

---

## Next Steps

**Continue Learning:**

1. Practice with your own queries
2. Start simple, add complexity gradually
3. Study the [Query Patterns Reference](../../reference/advanced/query-patterns.md)
4. Explore [Properties Reference](../../reference/data/properties-reference.md)
5. Join community to share queries

**Common Query Library:**

Build your personal query collection:

- Daily agenda
- Weekly review
- Project dashboards
- Study trackers
- Time analytics
- Habit monitors

**Advanced Topics:**

- Rules (`:rules` clause)
- Recursion patterns
- Sub-queries
- Advanced aggregations
- Performance optimization

**Related:**

- [Query Patterns Reference](../../reference/advanced/query-patterns.md)
- [Properties Reference](../../reference/data/properties-reference.md)
- [Task Management Workflows](../../how-to/workflows/task-management-workflows.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
