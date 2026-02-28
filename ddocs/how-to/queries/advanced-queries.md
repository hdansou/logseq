---
title: 'Advanced Queries'
description: 'How to write advanced Datalog queries in Logseq'
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
  howTo:
    - '[View Functions](../content/view-types.md)'
---

# Advanced Queries

> How to write powerful Datalog queries to search and analyze your graph.

## Query Basics

Advanced queries use Datalog syntax inside query blocks:

```clojure
#+BEGIN_QUERY
{:title "My Query"
 :query [:find (pull ?b [*])
         :where [?b :block/marker "TODO"]]}
#+END_QUERY
```

---

## Query Structure

```clojure
{:title "Query Title"
 :query [:find ...
         :in $ ?input
         :where ...]
 :inputs [...]
 :result-transform ...
 :view ...
 :collapsed? false}
```

| Key                 | Purpose           |
| ------------------- | ----------------- |
| `:title`            | Display title     |
| `:query`            | Datalog query     |
| `:inputs`           | Values to pass in |
| `:result-transform` | Modify results    |
| `:view`             | Custom display    |
| `:collapsed?`       | Start collapsed   |

---

## Find Patterns

### Pull All Fields

```clojure
[:find (pull ?b [*])
 :where ...]
```

### Pull Specific Fields

```clojure
[:find (pull ?b [:block/title :block/marker])
 :where ...]
```

### Find Scalar Value

```clojure
[:find ?title .
 :where [?p :block/name "mypage"]
        [?p :block/title ?title]]
```

### Find Collection

```clojure
[:find [?title ...]
 :where [?p :block/title ?title]]
```

---

## Common Query Patterns

### All TODO Tasks

```clojure
#+BEGIN_QUERY
{:title "📋 All TODOs"
 :query [:find (pull ?b [*])
         :where [?b :block/marker "TODO"]]}
#+END_QUERY
```

### Tasks Due Today

```clojure
#+BEGIN_QUERY
{:title "📅 Due Today"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where [?b :block/deadline ?d]
                [(= ?d ?today)]]
 :inputs [:today]}
#+END_QUERY
```

### Tasks in Date Range

```clojure
#+BEGIN_QUERY
{:title "📆 This Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where [?b :block/scheduled ?d]
                [(>= ?d ?start)]
                [(<= ?d ?end)]]
 :inputs [:7d-before :today]}
#+END_QUERY
```

### Blocks with Specific Tag

```clojure
#+BEGIN_QUERY
{:title "🏷️ Tagged #project"
 :query [:find (pull ?b [*])
         :where [?b :block/refs ?ref]
                [?ref :block/name "project"]]}
#+END_QUERY
```

### Blocks on Current Page

```clojure
#+BEGIN_QUERY
{:title "📄 This Page TODOs"
 :query [:find (pull ?b [*])
         :in $ ?current-page
         :where [?p :block/name ?current-page]
                [?b :block/page ?p]
                [?b :block/marker "TODO"]]
 :inputs [:current-page]}
#+END_QUERY
```

---

## Property Queries

### Blocks with Property

```clojure
#+BEGIN_QUERY
{:title "Has Priority"
 :query [:find (pull ?b [*])
         :where [?b :block/properties ?props]
                [(get ?props :priority)]]}
#+END_QUERY
```

### Property Equals Value

```clojure
#+BEGIN_QUERY
{:title "Priority A"
 :query [:find (pull ?b [*])
         :where [?b :block/properties ?props]
                [(get ?props :priority) ?p]
                [(= ?p "A")]]}
#+END_QUERY
```

### Property Contains Value

```clojure
#+BEGIN_QUERY
{:title "Tagged Important"
 :query [:find (pull ?b [*])
         :where [?b :block/properties ?props]
                [(get ?props :tags) ?tags]
                [(contains? ?tags "important")]]}
#+END_QUERY
```

---

## Date Inputs

### Built-in Inputs

| Input           | Description       |
| --------------- | ----------------- |
| `:today`        | Today's date      |
| `:yesterday`    | Yesterday         |
| `:tomorrow`     | Tomorrow          |
| `:current-page` | Current page name |
| `:7d-before`    | 7 days ago        |
| `:7d-after`     | 7 days from now   |
| `:30d-before`   | 30 days ago       |

### Usage

```clojure
{:query [:find ...
         :in $ ?today
         :where ...]
 :inputs [:today]}
```

---

## Result Transform

### Sort Results

```clojure
{:query [...]
 :result-transform
 (fn [result]
   (sort-by (fn [r] (get-in r [:block/properties :priority])) result))}
```

### Filter Results

```clojure
{:result-transform
 (fn [result]
   (filter #(not= "DONE" (:block/marker %)) result))}
```

### Limit Results

```clojure
{:result-transform
 (fn [result]
   (take 10 result))}
```

### Complex Transform

```clojure
{:result-transform
 (fn [result]
   (->> result
        (filter #(= "TODO" (:block/marker %)))
        (sort-by #(get-in % [:block/properties :priority]))
        (take 5)))}
```

---

## Combining Conditions

### AND (both conditions)

```clojure
[:where [?b :block/marker "TODO"]
        [?b :block/refs ?ref]
        [?ref :block/name "project"]]
```

### OR (either condition)

```clojure
[:where (or [?b :block/marker "TODO"]
            [?b :block/marker "DOING"])]
```

### NOT (exclude)

```clojure
[:where [?b :block/marker ?m]
        (not [(= ?m "DONE")])]
```

---

## Full Example: Project Dashboard

```clojure
#+BEGIN_QUERY
{:title "🎯 Active Projects"
 :query [:find (pull ?b [*])
         :where [?b :block/refs ?ref]
                [?ref :block/name "project"]
                [?b :block/marker ?m]
                [(contains? #{"TODO" "DOING"} ?m)]]
 :result-transform
 (fn [result]
   (->> result
        (sort-by #(get-in % [:block/properties :priority] "Z"))
        (take 10)))
 :view (fn [result]
         [:div
           (for [r result]
             [:div {:style {:padding "8px"
                            :border-left "3px solid #4cc9f0"
                            :margin-bottom "4px"}}
               [:strong (:block/marker r)] " "
               (:block/title r)])])}
#+END_QUERY
```

---

## Debugging Queries

### Check Query Output

```clojure
:view (fn [result]
        [:pre (pr-str (first result))])
```

### Print All Keys

```clojure
:view (fn [result]
        [:pre (pr-str (keys (first result)))])
```

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
4. **Index-friendly**: Use indexed attributes when possible

---

## Common Errors

### "Cannot read property"

- Check attribute names (`:block/title` not `:title`)
- Verify entity has the attribute

### "No results"

- Simplify query to debug
- Check if data exists with simpler query

### "Syntax error"

- Balance brackets and parentheses
- Verify Datalog syntax

---

## See Also

- **Database Schema**: [schema-overview.md](../../reference/database/schema-overview.md)
- **View Functions**: [view-types.md](../content/view-types.md)

---

_Last updated: 2026-01-29_
