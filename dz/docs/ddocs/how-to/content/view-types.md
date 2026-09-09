---
title: 'View Functions'
description: 'How to create custom view functions for advanced query results'
status: stable
lastVerified: '2026-05-30'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[Advanced Queries](../queries/advanced-queries.md)'
  reference:
    - '[Config.edn](../../reference/configuration/config-edn.md)'
---

# View Functions

> **Last revised:** 2026-05-30

> How to use `:view` functions to create custom presentations of query results.

## Findings: New Capabilities

- `:query/result-transforms` is supported in `config.edn`, and `:result-transform` is handled at query time, so you can sort or reshape results before `:view` renders them.

## What is :view?

The `:view` parameter in advanced queries lets you control exactly how results are displayed. Instead of the default table, you can create custom layouts, cards, lists, or any HTML structure.

---

## Basic Syntax

**File graph example:**

```clojure
#+BEGIN_QUERY
{:title "My Tasks"
 :query [:find (pull ?b [*])
         :where [?b :block/marker ?m]
                [(contains? #{"TODO" "DOING"} ?m)]]
 :view (fn [result]
         [:div
           (for [r result]
             [:div (:block/title r)])])}
#+END_QUERY
```

> **Note:** In DB graphs, `:block/marker` does not exist. Use `:logseq.property/status` instead. See the "Accessing Block Data" section below for details.

---

## Simple Examples

### Basic List

```clojure
:view (fn [result]
        [:ul
          (for [item result]
            [:li (get-in item [:block/title])])])
```

**Output:**

- Task 1
- Task 2
- Task 3

### Numbered List

```clojure
:view (fn [result]
        [:ol
          (for [item result]
            [:li (:block/title item)])])
```

### Comma-Separated

```clojure
:view (fn [result]
        [:span
          (interpose ", "
            (map :block/title result))])
```

---

## Card Layout

**File graph example** (uses `:block/properties`):

```clojure
:view (fn [result]
        [:div {:style {:display "grid"
                       :grid-template-columns "repeat(3, 1fr)"
                       :gap "1rem"}}
          (for [item result]
            [:div {:style {:padding "1rem"
                           :background "#f3f4f6"
                           :border-radius "8px"}}
              [:strong (:block/title item)]
              [:div {:style {:color "#666"}}
                (str "Priority: " (get-in item [:block/properties :priority] "None"))]])])
```

In a DB graph, replace `(get-in item [:block/properties :priority] "None")` with `(str (:logseq.property/priority item))` or access the property's `:block/title` for its display name.

---

## Accessing Block Data

### Common Fields

**File graphs:**

| Field      | Access              | Description       |
| ---------- | ------------------- | ----------------- |
| Title      | `:block/title`      | Block content     |
| Marker     | `:block/marker`     | TODO, DOING, DONE |
| Priority   | `:block/priority`   | A, B, C           |
| Properties | `:block/properties` | Map of properties |
| Page       | `:block/page`       | Page entity       |
| Parent     | `:block/parent`     | Parent block      |

**DB graphs (0.11.x+):** In DB graphs, `:block/marker` and `:block/priority` do not exist. Tasks use `:logseq.property/status` (with entity values like `:logseq.property/status.backlog`, `:logseq.property/status.todo`, `:logseq.property/status.doing`, `:logseq.property/status.in-review`, `:logseq.property/status.done`, and `:logseq.property/status.canceled`) and `:logseq.property/priority` (with values `:logseq.property/priority.low`, `.medium`, `.high`, and `.urgent`). Properties are stored as direct attributes on the entity rather than inside a `:block/properties` map.

| Field    | Access                     | Description                        |
| -------- | -------------------------- | ---------------------------------- |
| Title    | `:block/title`             | Block content                      |
| Status   | `:logseq.property/status`  | Entity ref (e.g., status.todo)     |
| Priority | `:logseq.property/priority`| Entity ref (e.g., priority.high)   |
| Page     | `:block/page`              | Page entity                        |
| Parent   | `:block/parent`            | Parent block                       |

### Access Patterns

**File graphs:**

```clojure
;; Direct access
(:block/title item)

;; Nested access (file graphs only)
(get-in item [:block/properties :status])

;; With default (file graphs only)
(get-in item [:block/properties :priority] "Normal")

;; Page name
(get-in item [:block/page :block/title])
```

**DB graphs:** Properties are direct attributes on the entity, not nested under `:block/properties`.

```clojure
;; Direct access
(:block/title item)

;; Properties are direct attributes (DB graphs)
(:logseq.property/status item)
(:logseq.property/priority item)

;; Page name
(get-in item [:block/page :block/title])
```

---

## Conditional Styling

**File graph example** (uses `:block/properties` with string priority values "A", "B", "C"):

```clojure
:view (fn [result]
        [:div
          (for [item result]
            (let [priority (get-in item [:block/properties :priority])
                  color (case priority
                          "A" "#ef4444"
                          "B" "#f59e0b"
                          "C" "#10b981"
                          "#6b7280")]
              [:div {:style {:border-left (str "4px solid " color)
                             :padding-left "8px"
                             :margin-bottom "4px"}}
                (:block/title item)]))])
```

> **DB graph note:** Priority values in DB graphs are entity references (`:logseq.property/priority.high`, `.medium`, `.low`) rather than strings. Adjust the `case` expression accordingly.

---

## Table with Custom Columns

**File graph example** (uses `:block/marker` and `:block/properties`):

```clojure
:view (fn [result]
        [:table {:style {:width "100%"}}
          [:thead
            [:tr
              [:th "Task"]
              [:th "Status"]
              [:th "Priority"]]]
          [:tbody
            (for [item result]
              [:tr
                [:td (:block/title item)]
                [:td (:block/marker item)]
                [:td (get-in item [:block/properties :priority] "—")]])]])
```

> **DB graph note:** Replace `(:block/marker item)` with `(:block/title (:logseq.property/status item))` and `(get-in item [:block/properties :priority])` with `(:block/title (:logseq.property/priority item))` to get display names for status and priority.

---

## Statistics View

**File graph example** (uses `:block/marker`):

```clojure
:view (fn [result]
        (let [total (count result)
              done (count (filter #(= "DONE" (:block/marker %)) result))
              todo (- total done)]
          [:div {:style {:padding "1rem" :background "#f9fafb" :border-radius "8px"}}
            [:div {:style {:font-size "2rem" :font-weight "bold"}}
              (str done "/" total " Complete")]
            [:div {:style {:background "#e5e7eb" :height "8px" :border-radius "4px" :margin-top "8px"}}
              [:div {:style {:background "#10b981"
                             :height "100%"
                             :width (str (* 100 (/ done total)) "%")
                             :border-radius "4px"}}]]]))
```

> **DB graph note:** Replace `(= "DONE" (:block/marker %))` with a check against the status entity, e.g., `(= "Done" (:block/title (:logseq.property/status %)))`.

---

## Reusable Views (Registry)

Define views in `config.edn` and reuse across queries:

### In config.edn

```clojure
{:query/views
 {:simple-list
  (fn [result]
    [:ul (for [r result] [:li (:block/title r)])])

  :cards
  (fn [result]
    [:div.grid
      (for [r result]
        [:div.card (:block/title r)])])

  :count-only
  (fn [result]
    [:div {:style {:font-size "2rem"}}
      (str (count result) " items")])}}
```

### In Queries

```clojure
#+BEGIN_QUERY
{:title "Tasks"
 :query [:find (pull ?b [*]) :where ...]
 :view :simple-list}
#+END_QUERY
```

---

## View vs Result-Transform

| Feature  | `:result-transform`   | `:view`              |
| -------- | --------------------- | -------------------- |
| Purpose  | Modify data           | Change display       |
| Returns  | Data structure        | Hiccup (HTML)        |
| Use case | Filter, sort, compute | Custom layout        |
| Chaining | Runs before :view     | Runs after transform |

### Using Both

**File graph example:**

```clojure
#+BEGIN_QUERY
{:title "Top 5 Tasks"
 :query [:find (pull ?b [*]) :where ...]
 :result-transform (fn [result]
                     (->> result
                          (sort-by #(get-in % [:block/properties :priority]))
                          (take 5)))
 :view (fn [result]
         [:ol (for [r result] [:li (:block/title r)])])}
#+END_QUERY
```

> **DB graph note:** Replace `(get-in % [:block/properties :priority])` with direct property access like `(:logseq.property/priority %)`.

---

## Hiccup Quick Reference

Hiccup is a way to write HTML using ClojureScript data structures:

```clojure
;; Element with text
[:div "Hello"]
;; → <div>Hello</div>

;; With attributes
[:div {:class "container"} "Content"]
;; → <div class="container">Content</div>

;; Inline styles
[:div {:style {:color "red" :font-size "16px"}} "Red text"]

;; Nested elements
[:div
  [:h1 "Title"]
  [:p "Paragraph"]]

;; Classes shorthand
[:div.my-class "Content"]
;; → <div class="my-class">Content</div>

;; IDs shorthand
[:div#my-id "Content"]
;; → <div id="my-id">Content</div>
```

---

## Troubleshooting

### View Not Rendering

1. Check syntax with balanced brackets
2. Ensure function returns Hiccup
3. Look for errors in Developer Console (`Cmd+Option+I`)

### Data Not Appearing

```clojure
;; Debug: Print result structure
:view (fn [result]
        [:pre (pr-str (first result))])
```

### Styles Not Applying

- Use inline `:style` maps
- Or define CSS classes in `custom.css`

---

## See Also

- **Advanced Queries**: [advanced-queries.md](../queries/advanced-queries.md)
- **Config Reference**: [config-edn.md](../../reference/configuration/config-edn.md)

---

_Last updated: 2026-03-10_
