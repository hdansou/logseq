---
title: 'View Functions'
description: 'How to create custom view functions for advanced query results'
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  howTo:
    - '[Advanced Queries](../queries/advanced-queries.md)'
  reference:
    - '[Config.edn](../../reference/configuration/config-edn.md)'
---

# View Functions

> **Last revised:** 2026-09-09 (verified against `be800f1711`)

> How to use `:view` functions to create custom presentations of query results.

## Read this first: what `:view` actually receives

**A `:view` function does not receive block maps. It receives block UUIDs.**

Query rows are normalised in the worker before they ever reach the UI. `normalize-query-cell`
replaces any map or entity that has a `:block/uuid` with that UUID
([`src/main/frontend/worker/handler/render_resource/query.cljs:15-46`](../../../../../src/main/frontend/worker/handler/render_resource/query.cljs)):

```clojure
(defn- normalize-query-cell
  [cell]
  (cond
    (and (or (map? cell) (de/entity? cell))
         (uuid? (:block/uuid cell)))
    (:block/uuid cell)
    ;; …
    ))
```

So for the usual `[:find (pull ?b [*])]` query, `result` inside your `:view` is a vector of
`#uuid` values. `(:block/title r)` on such a row returns `nil`, silently. Every example below is
written with that in mind.

Two more consequences of the same normalisation:

- **Single-column rows are unwrapped, multi-column rows stay as vectors.** `normalize-query-row`
  returns `(first tuple)` when the tuple has one element and the tuple itself otherwise
  ([`query.cljs:40-46`](../../../../../src/main/frontend/worker/handler/render_resource/query.cljs)).
  A `[:find ?a ?b]` query therefore yields two-element vectors, not maps.
- **`:result-transform` runs *before* normalisation, so it still sees full entities.** The order in
  `query-result-rows` is: build tuples → filter block results → apply `:result-transform` →
  `normalize-query-row` ([`query.cljs:195-202`](../../../../../src/main/frontend/worker/handler/render_resource/query.cljs)).
  This asymmetry is the key to the recipes in this guide.

> **Looks like a Logseq defect, not a docs defect.** `:result-transform` sees entities and `:view`
> sees UUIDs, from the same pipeline, a few lines apart. Any `:view` that wants block text has to
> get it via `:result-transform`. Worth raising upstream.

## Findings: New Capabilities

- `:query/views` and `:query/result-transforms` are both resolvable from `config.edn`: a keyword
  `:view` is looked up in `[:query/views <kw>]` and a keyword `:result-transform` in
  `[:query/result-transforms <kw>]`
  ([`src/main/frontend/components/query.cljs:133-149`](../../../../../src/main/frontend/components/query.cljs)).
  A missing `:result-transform` key throws `"Missing query result transform"`; a missing
  `:query/views` key just yields `nil` and falls back to the default rendering.

## What is :view?

The `:view` parameter in advanced queries lets you control how results are displayed. Instead of
the built-in table, you supply a function returning Hiccup.

The function is evaluated with SCI and then called on the result:

```clojure
view-f (and view-fn (sci/eval-string (pr-str view-fn)))
;; …
(sci/call-fn view-f result)
```

([`components/query.cljs:148-149`](../../../../../src/main/frontend/components/query.cljs) and
[`:43-50`](../../../../../src/main/frontend/components/query.cljs)). The returned Hiccup is passed
through `util/hiccup-keywordize`
([`src/main/frontend/util.cljc:875-882`](../../../../../src/main/frontend/util.cljc)), which
keywordises string tag names.

---

## Basic Syntax

**A DB graph cannot run `#+BEGIN_QUERY`.** That markup renders a deprecation warning instead of a
result set ([`src/main/frontend/components/block.cljs:5057-5058`](../../../../../src/main/frontend/components/block.cljs)).
Create an advanced query with the `/Advanced Query` command, which tags the block
`:logseq.class/Query` and puts your query map in the `:logseq.property/query` code block
([`src/main/frontend/commands.cljs:125-132`](../../../../../src/main/frontend/commands.cljs)).

The map you type into that code block looks like this:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/tags ?t]
         [?t :db/ident :logseq.class/Task]]
 :view (fn [result]
         [:div (str (count result) " tasks")])}
```

> **`:title` is silently ignored.** `query-title` is only rendered when `built-in-query?` is true;
> a user's advanced query takes the `[:div.bd …]` branch with no title
> ([`components/query.cljs:166-177`](../../../../../src/main/frontend/components/query.cljs)).
> Put the heading in your `:view` or in the block above the query.
>
> `:group-by-page?` is ignored too: `get-group-by-page` returns `false` unconditionally when
> `db-graph?` is set ([`src/main/frontend/components/query/result.cljs:7-15`](../../../../../src/main/frontend/components/query/result.cljs)).
>
> `:limit` and `:order-by` are not query-map keys at all. The only keys forwarded to the query
> engine are `:kind :query :current-page-title :current-block-uuid :today-day
> :remove-block-children? :result-transform-edn` (plus `:cards?` for DSL and `:inputs`/`:rules` for
> datalog) — anything else is dropped without an error
> ([`worker/handler/render_resource/query.cljs:48-61`](../../../../../src/main/frontend/worker/handler/render_resource/query.cljs)).
> Bound your result in `:where`, or with `(take n …)` inside `:view`.

---

## The core pattern: transform, then view

Because `:result-transform` sees entities and `:view` sees normalised cells, the reliable way to
render block text is to project it in the transform:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/tags ?t]
         [?t :db/ident :logseq.class/Task]]
 :result-transform (fn [result]
                     (map (fn [b] [(:block/title b)
                                   (:block/title (:logseq.property/status b))])
                          result))
 :view (fn [rows]
         [:ul
          (for [row rows]
            [:li (first row) " — " (second row)])])}
```

Each transformed row is a two-element vector, so it survives normalisation as a vector and arrives
at `:view` intact.

> **The transform's output is validated.** It must be sequential or a set, or the query fails with
> `"Query result transform must return rows"`. Every cell is then walked by
> `normalize-query-cell`: a **map without a `:block/uuid` fails** with
> `"Renderer query result map has no UUID"`, and a function value fails with
> `"Renderer query result contains a function"`
> ([`worker/handler/render_resource/query.cljs:15-26, 181-193`](../../../../../src/main/frontend/worker/handler/render_resource/query.cljs)).
> Return **vectors of scalars**, not maps.

---

## Simple Examples

These work on the raw (untransformed) result, which is a vector of UUIDs.

### Count only

```clojure
:view (fn [result]
        [:div {:style {:font-size "2rem"}}
         (str (count result) " items")])
```

### Basic List

Project the titles first, then render:

```clojure
:result-transform (fn [result] (map :block/title result))
:view (fn [titles]
        [:ul (for [t titles] [:li t])])
```

### Numbered List

```clojure
:result-transform (fn [result] (map :block/title result))
:view (fn [titles]
        [:ol (for [t titles] [:li t])])
```

### Comma-Separated

```clojure
:result-transform (fn [result] (map :block/title result))
:view (fn [titles]
        [:span (interpose ", " titles)])
```

---

## Card Layout

```clojure
:result-transform (fn [result]
                    (map (fn [b]
                           [(:block/title b)
                            (or (:block/title (:logseq.property/priority b)) "None")])
                         result))
:view (fn [rows]
        [:div {:style {:display "grid"
                       :grid-template-columns "repeat(3, 1fr)"
                       :gap "1rem"}}
         (for [row rows]
           [:div {:style {:padding "1rem"
                          :background "#f3f4f6"
                          :border-radius "8px"}}
            [:strong (first row)]
            [:div {:style {:color "#666"}}
             (str "Priority: " (second row))]])])
```

> **`:style` must be a map.** Passing a CSS string silently blanks the render rather than raising.

---

## Accessing Block Data

Everything in this section applies **inside `:result-transform`**, where rows are still entities.
By the time `:view` runs, they are UUIDs.

### Common Fields

| Field    | Access                      | Notes                                     |
| -------- | --------------------------- | ----------------------------------------- |
| Title    | `:block/title`              | Block content — **not** `:block/content`  |
| Status   | `:logseq.property/status`   | Ref to a closed-value entity              |
| Priority | `:logseq.property/priority` | Ref to a closed-value entity              |
| Deadline | `:logseq.property/deadline` | `:datetime` — epoch **milliseconds**      |
| Page     | `:block/page`               | Page entity                               |
| Parent   | `:block/parent`             | Parent block                              |

`:logseq.property/status` closed values are `:logseq.property/status.backlog`, `.todo`, `.doing`,
`.in-review`, `.done`, `.canceled`
([`deps/db/src/logseq/db/frontend/property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).
`:logseq.property/priority` closed values are `:logseq.property/priority.low`, `.medium`, `.high`,
`.urgent` ([`:334-337`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).
`:logseq.property/deadline` and `/scheduled` are `{:type :datetime}`
([`:340-353`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)) and the `:datetime`
validator is plain `number?`
([`property/type.cljs:210-212`](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs)).

> **Deleted (file-graph only, not valid here):** `:block/marker`, `:block/priority` and
> `:block/properties`. At this revision those attributes exist only inside `deps/graph-parser/`
> (the file-graph importer) and are not part of the DB schema, so the old
> `(get-in item [:block/properties :priority])` idiom has nothing to read.

### Access Patterns

```clojure
;; Inside :result-transform, rows are entities:
(:block/title b)

;; Closed-value properties are refs — take the ref's title for a display name
(:block/title (:logseq.property/status b))

;; …or compare on :db/ident for logic
(= :logseq.property/status.done (:db/ident (:logseq.property/status b)))

;; Number properties are ref entities too — deref the value
(:logseq.property/value (:user.property/Weight-a1b2c3d4 b))

;; Page name
(get-in b [:block/page :block/title])
```

> **User property idents are not guessable.** Creating a property titled "Weight" mints
> `:user.property/Weight-<8 random chars>` (`create-db-ident-from-name`,
> [`deps/db/src/logseq/db/frontend/db_ident.cljc:66-89`](../../../../../deps/db/src/logseq/db/frontend/db_ident.cljc)).
> Resolve user properties by title rather than hardcoding an ident.

---

## Conditional Styling

```clojure
:result-transform (fn [result]
                    (map (fn [b]
                           [(:block/title b)
                            (:db/ident (:logseq.property/priority b))])
                         result))
:view (fn [rows]
        [:div
         (for [row rows]
           (let [color (case (second row)
                         :logseq.property/priority.urgent "#dc2626"
                         :logseq.property/priority.high   "#ef4444"
                         :logseq.property/priority.medium "#f59e0b"
                         :logseq.property/priority.low    "#10b981"
                         "#6b7280")]
             [:div {:style {:border-left (str "4px solid " color)
                            :padding-left "8px"
                            :margin-bottom "4px"}}
              (first row)]))])
```

---

## Table with Custom Columns

```clojure
:result-transform (fn [result]
                    (map (fn [b]
                           [(:block/title b)
                            (or (:block/title (:logseq.property/status b)) "—")
                            (or (:block/title (:logseq.property/priority b)) "—")])
                         result))
:view (fn [rows]
        [:table {:style {:width "100%"}}
         [:thead
          [:tr [:th "Task"] [:th "Status"] [:th "Priority"]]]
         [:tbody
          (for [row rows]
            [:tr
             [:td (nth row 0)]
             [:td (nth row 1)]
             [:td (nth row 2)]])]])
```

---

## Statistics View

`count` works directly on the raw result, so this one needs no transform:

```clojure
:result-transform (fn [result]
                    (map (fn [b]
                           (if (= :logseq.property/status.done
                                  (:db/ident (:logseq.property/status b)))
                             "done"
                             "open"))
                         result))
:view (fn [states]
        (let [total (count states)
              done (count (filter (fn [s] (= "done" s)) states))]
          [:div {:style {:padding "1rem" :background "#f9fafb" :border-radius "8px"}}
           [:div {:style {:font-size "2rem" :font-weight "bold"}}
            (str done "/" total " Complete")]
           [:div {:style {:background "#e5e7eb" :height "8px"
                          :border-radius "4px" :margin-top "8px"}}
            [:div {:style {:background "#10b981"
                           :height "100%"
                           :width (str (if (pos? total) (* 100 (/ done total)) 0) "%")
                           :border-radius "4px"}}]]]))
```

---

## Reusable Views (Registry)

Define views in `config.edn` and reference them by keyword. A keyword `:view` is resolved through
`[:query/views <kw>]` in the repo config
([`components/query.cljs:148`](../../../../../src/main/frontend/components/query.cljs)).

### In config.edn

The shipped template registers one view as an example
([`deps/common/resources/templates/config.edn:130-133`](../../../../../deps/common/resources/templates/config.edn)):

```clojure
 ;; Pre-defined :view function to use with advanced queries.
 :query/views
 {:pprint
  (fn [r] [:pre.code (pprint r)])}
```

You can add your own alongside it:

```clojure
{:query/views
 {:count-only
  (fn [result]
    [:div {:style {:font-size "2rem"}}
     (str (count result) " items")])

  :simple-list
  (fn [titles]
    [:ul (for [t titles] [:li t])])}}
```

`:simple-list` above expects titles, so pair it with a `:result-transform` that projects them.

### In Queries

```clojure
{:query [:find (pull ?b [*]) :where ...]
 :view :simple-list}
```

If the keyword is not registered, `view-fn` is `nil` and the query falls back to the default
rendering — no error is shown.

---

## View vs Result-Transform

| Feature     | `:result-transform`              | `:view`                       |
| ----------- | -------------------------------- | ----------------------------- |
| Runs where  | Worker, before normalisation     | UI, after normalisation       |
| Sees        | Full entities                    | UUIDs and scalars             |
| Purpose     | Filter, sort, project            | Change display                |
| Returns     | Sequential/set of rows           | Hiccup                        |
| Chaining    | Runs before `:view`              | Runs after transform          |

### Using Both

```clojure
{:query [:find (pull ?b [*]) :where ...]
 :result-transform (fn [result]
                     (->> result
                          (sort-by (fn [b]
                                     (or (:block/title (:logseq.property/priority b)) "Z")))
                          (take 5)
                          (map :block/title)))
 :view (fn [titles]
         [:ol (for [t titles] [:li t])])}
```

This is also how you bound a result set, since `:limit` is not a query-map key.

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

;; Inline styles — :style must be a MAP, never a string
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

A `(for …)` sequence used as a child renders correctly — no `into`/`concat` wrapper needed.

---

## Troubleshooting

### View Not Rendering (and no error shown)

`sci/eval-string` catches every exception, logs `:sci-eval-failed` and returns `nil`
([`src/main/frontend/extensions/sci.cljs:50-58`](../../../../../src/main/frontend/extensions/sci.cljs)).
A `view-f` of `nil` fails the `(and (seq result) view-f)` guard, so the query quietly falls back to
default rendering with no visible complaint
([`components/query.cljs:43`](../../../../../src/main/frontend/components/query.cljs)).

So if your view "does nothing":

1. Check for balanced brackets.
2. Prefer plain `(fn [x] …)` and `(for […] …)` over `#(…)`, `map-indexed`, or a bare keyword used
   as a function — the SCI environment here is small and an unsupported form disappears rather than
   erroring.
3. Look for `sci-eval-failed` in the Developer Console (`Cmd+Option+I`).

A view that *runs* but throws is handled differently: that error **is** surfaced, as
`"Custom view error: …"` ([`components/query.cljs:44-49`](../../../../../src/main/frontend/components/query.cljs),
string at [`src/resources/dicts/en.edn:1605`](../../../../../src/resources/dicts/en.edn)).

### Available helpers inside a view

The SCI namespace provides `sum`, `average`, `parseFloat`, `isNaN`, `log`, `pprint` and `call-api`
([`src/main/frontend/extensions/sci.cljs:29-37`](../../../../../src/main/frontend/extensions/sci.cljs)).
`call-api` invokes a `window.logseq.api` function by name — but note that block-fetching APIs such
as `get_block` return promises
([`src/main/logseq/api/editor.cljs:314-317`](../../../../../src/main/logseq/api/editor.cljs)),
which a synchronous Hiccup-returning view cannot await. Use `:result-transform` for block data.

### Data Not Appearing

```clojure
;; Debug: print the row structure so you can see it is a UUID
:view (fn [result]
        [:pre (pr-str (first result))])
```

### Styles Not Applying

- Use inline `:style` **maps**
- Or define CSS classes in `custom.css`

---

## Not to be confused with: `:logseq.property.view/type`

This document is about query `:view` **functions**. Logseq separately has a **view type** property
that selects a built-in presentation for a block or page. It has exactly three closed values
([`deps/db/src/logseq/db/frontend/property.cljs:432-447`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)):

| `:db/ident`                          | Title          |
| ------------------------------------ | -------------- |
| `:logseq.property.view/type.table`   | Table View     |
| `:logseq.property.view/type.list`    | List View      |
| `:logseq.property.view/type.gallery` | Gallery View   |

The default is `:logseq.property.view/type.table`. The property is `:public? false` and `:hide? true`,
so it is set through the view UI rather than typed by hand.

> **Deleted (was invented):** a "calendar" view type. There is no `:logseq.property.view/type.calendar`
> anywhere in the source at this revision.

---

## See Also

- **Advanced Queries**: [advanced-queries.md](../queries/advanced-queries.md)
- **Config Reference**: [config-edn.md](../../reference/configuration/config-edn.md)

---

_Last updated: 2026-09-09 — verified against `be800f1711`._
