---
title: 'Advanced Queries'
description: 'How to write advanced Datalog queries in Logseq'
status: stable
lastVerified: '2026-04-21'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
  howTo:
    - '[View Functions](../content/view-types.md)'
---

# Advanced Queries

> **Last revised:** 2026-04-21
> How to write powerful Datalog queries to search and analyze your graph.

## Query Basics

Advanced queries use Datalog syntax inside query blocks:

```clojure
#+BEGIN_QUERY
{:title "My Query"
 :query [:find (pull ?b [*])
         :where [?b :logseq.property/status ?s]
                [?s :block/title "Todo"]]}
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
 :remove-block-children? true
 :result-transform ...
 :view ...
 :collapsed? false}
```

| Key                       | Purpose                                                            |
| ------------------------- | ------------------------------------------------------------------ |
| `:title`                  | Display title                                                      |
| `:query`                  | Datalog query                                                      |
| `:inputs`                 | Values to pass in                                                  |
| `:remove-block-children?` | Keep matching child blocks as separate results when set to `false` |
| `:result-transform`       | Modify results                                                     |
| `:view`                   | Custom display                                                     |
| `:collapsed?`             | Start collapsed                                                    |

### Control Child Block Results

Set `:remove-block-children?` on the query map itself, at the same level as
`:query`, `:inputs`, `:result-transform`, and `:view`. It is a per-query
display option, not a `config.edn` setting.

- Default value: `true`
- When `true`: if both a parent block and one of its children match, Logseq only
  returns the top-level parent as a separate result
- When `false`: matching child blocks are also returned individually
- Applies to block results

Use this when you want every matching task block to appear separately in an
advanced query result:

```clojure
#+BEGIN_QUERY
{:title "All Matching Task Blocks"
 :query [:find (pull ?b [*])
         :in $ %
         :where (task ?b #{"Canceled" "Done" "In Review" "Doing" "Todo" "Backlog"})]
 :remove-block-children? false}
#+END_QUERY
```

---

## Find Patterns

### Pull All Fields

```clojure
[:find (pull ?b [*])
 :where ...]
```

### Pull Specific Fields

```clojure
[:find (pull ?b [:block/title])
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

In DB graphs, task status is stored via the `:logseq.property/status` ref property.
Status values are entities with `:block/title` such as `"Todo"`, `"Doing"`, `"Done"`,
`"Backlog"`, `"In Review"`, and `"Canceled"`.

```clojure
#+BEGIN_QUERY
{:title "All Todos"
 :query [:find (pull ?b [*])
         :where [?b :logseq.property/status ?s]
                [?s :block/title "Todo"]]}
#+END_QUERY
```

### Tasks Due Today

Deadline and scheduled dates use `:logseq.property/deadline` and `:logseq.property/scheduled`.

```clojure
#+BEGIN_QUERY
{:title "Due Today"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where [?b :logseq.property/deadline ?d]
                [(= ?d ?today)]]
 :inputs [:today]}
#+END_QUERY
```

### Tasks in Date Range

```clojure
#+BEGIN_QUERY
{:title "This Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where [?b :logseq.property/scheduled ?d]
                [(>= ?d ?start)]
                [(<= ?d ?end)]]
 :inputs [:-7d :today]}
#+END_QUERY
```

### Blocks with Specific Tag

```clojure
#+BEGIN_QUERY
{:title "Tagged #project"
 :query [:find (pull ?b [*])
         :where [?b :block/refs ?ref]
                [?ref :block/name "project"]]}
#+END_QUERY
```

### Blocks on Current Page

```clojure
#+BEGIN_QUERY
{:title "This Page Todos"
 :query [:find (pull ?b [*])
         :in $ ?current-page
         :where [?p :block/name ?current-page]
                [?b :block/page ?p]
                [?b :logseq.property/status ?s]
                [?s :block/title "Todo"]]
 :inputs [:current-page]}
#+END_QUERY
```

---

## Property Queries

In DB graphs, properties are stored as direct entity attributes (not inside a
`:block/properties` map). Built-in properties use the `logseq.property/` namespace.
User-defined properties use their configured `:db/ident`.

### Blocks with Property

```clojure
#+BEGIN_QUERY
{:title "Has Priority"
 :query [:find (pull ?b [*])
         :where [?b :logseq.property/priority _]]}
#+END_QUERY
```

### Property Equals Value

Priority values are ref entities: `"Low"`, `"Medium"`, `"High"`, `"Urgent"`.

```clojure
#+BEGIN_QUERY
{:title "High Priority"
 :query [:find (pull ?b [*])
         :where [?b :logseq.property/priority ?p]
                [?p :block/title "High"]]}
#+END_QUERY
```

---

## Date Inputs

### Built-in Inputs

| Input            | Description                        |
| ---------------- | ---------------------------------- |
| `:today`         | Today's date (YYYYMMDD int)        |
| `:yesterday`     | Yesterday                          |
| `:tomorrow`      | Tomorrow                           |
| `:current-page`  | Current page name                  |
| `:query-page`    | Page where the query block lives   |
| `:current-block` | Current block's `:db/id`           |
| `:parent-block`  | Parent of current block's `:db/id` |
| `:right-now-ms`  | Current time in epoch milliseconds |

### Relative Date Inputs

The modern format uses `:<direction><amount><unit>` (e.g., `:-7d`, `:+1m`):

| Input  | Description      |
| ------ | ---------------- |
| `:-7d` | 7 days ago       |
| `:+7d` | 7 days from now  |
| `:-1w` | 1 week ago       |
| `:+1w` | 1 week from now  |
| `:-1m` | 1 month ago      |
| `:+1m` | 1 month from now |
| `:-1y` | 1 year ago       |
| `:+1y` | 1 year from now  |

Relative date-time inputs append a time suffix (e.g., `:-7d-ms`, `:-7d-start`, `:-7d-end`).

> **Note:** The older `:Xd-before` / `:Xd-after` format (e.g., `:7d-before`) is
> deprecated but still supported internally.

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
   (sort-by (fn [r] (:block/title (:logseq.property/priority r))) result))}
```

### Filter Results

```clojure
{:result-transform
 (fn [result]
   (filter (fn [r]
             (not= "Done" (:block/title (:logseq.property/status r)))) result))}
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
        (filter (fn [r] (= "Todo" (:block/title (:logseq.property/status r)))))
        (sort-by (fn [r] (:block/title (:logseq.property/priority r))))
        (take 5)))}
```

---

## Combining Conditions

### AND (both conditions)

```clojure
[:where [?b :logseq.property/status ?s]
        [?s :block/title "Todo"]
        [?b :block/refs ?ref]
        [?ref :block/name "project"]]
```

### OR (either condition)

```clojure
[:where [?b :logseq.property/status ?s]
        (or [?s :block/title "Todo"]
            [?s :block/title "Doing"])]
```

### NOT (exclude)

```clojure
[:where [?b :logseq.property/status ?s]
        [?s :block/title ?m]
        (not [(= ?m "Done")])]
```

---

## Full Example: Project Dashboard

```clojure
#+BEGIN_QUERY
{:title "Active Projects"
 :query [:find (pull ?b [*])
         :where [?b :block/refs ?ref]
                [?ref :block/name "project"]
                [?b :logseq.property/status ?s]
                [?s :block/title ?m]
                [(contains? #{"Todo" "Doing"} ?m)]]
 :result-transform
 (fn [result]
   (->> result
        (sort-by (fn [r] (or (:block/title (:logseq.property/priority r)) "Z")))
        (take 10)))
 :view (fn [result]
         [:div
           (for [r result]
             [:div {:style {:padding "8px"
                            :border-left "3px solid #4cc9f0"
                            :margin-bottom "4px"}}
               [:strong (:block/title (:logseq.property/status r))] " "
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
4. **Index-friendly**: Use indexed attributes when possible (`:block/name`, `:block/title`, `:block/page`, `:block/parent`, `:block/created-at`, `:block/updated-at`, `:block/journal-day`)

---

## Common Errors

### "Cannot read property"

- Check attribute names (`:block/title` not `:title`)
- Verify entity has the attribute

### "No results"

- Simplify query to debug
- Check if data exists with simpler query
- Remember that status and priority values are ref entities -- query their `:block/title`, not a string directly

### "Syntax error"

- Balance brackets and parentheses
- Verify Datalog syntax

---

## See Also

- **Database Schema**: [schema-overview.md](../../reference/database/schema-overview.md)
- **View Functions**: [view-types.md](../content/view-types.md)
