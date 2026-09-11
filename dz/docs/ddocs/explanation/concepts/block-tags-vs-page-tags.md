---
title: ':block/tags vs :logseq.property/page-tags'
description: 'Understanding the two tag-like attributes in Logseq DB graphs and when each applies'
status: stable
audience: [developer]
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: 'DB graph schema 65.33'
category: concepts
relatedDocs:
  reference:
    - '[Database Schema Overview](../../reference/database/schema-overview.md)'
  explanation:
    - '[File vs Database Graphs](file-vs-db.md)'
---

# `:block/tags` vs `:logseq.property/page-tags`

> In a Logseq DB graph there are two attributes that both look like "tags" but mean different things. `:block/tags` is the canonical class/instance relationship; `:logseq.property/page-tags` is a backward-compatible page-to-page association that the file-graph importer fills in.

All `file:line` citations below are at `be800f1711`.

## Overview

| Aspect              | `:block/tags`                                    | `:logseq.property/page-tags`                         |
| ------------------- | ------------------------------------------------ | ---------------------------------------------------- |
| Kind                | Core schema attribute, also a built-in property  | Built-in property                                    |
| Semantics           | *is-a* (class instance)                          | *related-to* (page ↔ page)                           |
| Value type          | `:class`, validated as "should be a Class"       | `:page`, validated as "should be a Page"             |
| Cardinality         | many (ref)                                       | many                                                 |
| Applies to          | Any block or page                                | Offered on pages only (`:view-context :page`)         |
| Created by          | `#tag` in the editor, or the Tags property       | File-graph import, or the property UI on a page      |
| Participates in     | Class system: tag properties, `extends` hierarchy | Nothing class-related; a plain many-to-many ref      |

## `:block/tags`: the real tags

`:block/tags` is a core schema attribute (`:db.type/ref`, `:db.cardinality/many`), declared next to `:block/refs` (`deps/db/src/logseq/db/frontend/schema.cljs:73-74`). It is also registered as the built-in property "Tags", with `:type :class` (`deps/db/src/logseq/db/frontend/property.cljs:75-80`). The `:class` type is validated by `class-entity?`, with the error message "should be a Class" (`deps/db/src/logseq/db/frontend/property/type.cljs:115-117`, `:177-179`). So the values are class entities, such as `:logseq.class/Task` or a user-defined tag.

Tagging a block or page with `:block/tags` makes that entity an **instance** of the class. That relationship carries:

- **Tag properties.** A class lists its "Tag Properties" in `:logseq.property.class/properties` (`property.cljs:172`). `get-block-classes-properties` collects those properties from every tag on a node and from each tag's ancestors (`deps/outliner/src/logseq/outliner/property.cljs:1020-1032`).
- **The class hierarchy.** Classes extend other classes through `:logseq.property.class/extends`. Query the hierarchy with the `class-extends` rule (`deps/db/src/logseq/db/frontend/rules.cljc:14-19`). A class's object list includes the objects of its child classes (`get-class-objects`, `deps/db/src/logseq/db/frontend/class.cljs:216-234`). The UI calls that list "Tagged nodes" (`src/resources/dicts/en.edn:195`).
- **The `#tag` syntax.** Inline `#tag`s in a block's title become `:block/tags`. When the tag is new, the outliner gives it a `:db/ident`, sets `extends` to `:logseq.class/Root`, and drops `#Page`, which turns it into a class (`add-missing-tag-idents`, `deps/outliner/src/logseq/outliner/core.cljs:264-273`). Deleting the `#tag` text from the title retracts the tag (`remove-tags-when-title-changed`, `core.cljs:254-262`).

Some classes cannot be applied inline. On non-page blocks the outliner strips `disallowed-inline-tags`: page classes, private tags, and the block-kind tags such as `#Template`, `#Query` and `#Cards`. It also strips built-in page names. The title keeps `#Name` as plain text, but no tag or ref is stored (`inline-tag-disallowed?` and `remove-disallowed-inline-classes`, `core.cljs:275-331`; `deps/db/src/logseq/db/frontend/class.cljs:130-140`).

Typical query:

```clojure
[:find ?b
 :where
 [?t :db/ident :logseq.class/Task]
 [?b :block/tags ?t]]
```

Matching `[?t :block/name "task"]` also works for built-in classes, because each one gets a lower-cased `:block/name` (`deps/db/src/logseq/db/sqlite/create_graph.cljs:160`). `:block/name` is not unique across a graph, though, so the ident is the safer handle.

## `:logseq.property/page-tags`: backward-compatible tagging

`:logseq.property/page-tags` is a built-in property (`deps/db/src/logseq/db/frontend/property.cljs:202-208`):

```clojure
:logseq.property/page-tags {:title "Page Tags"
                            :schema {:type :page
                                     :public? true
                                     :view-context :page
                                     :cardinality :many}
                            :properties
                            {:logseq.property/description "Provides a way for a page to associate to another page i.e. backward compatible tagging."}}
```

Key points:

- **The value type is `:page`, not `:class`.** `page-entity?` validates it (`type.cljs:123-125`, `:183-185`). It accepts anything `entity-util/page?` accepts: internal pages, journals, classes and property pages (`deps/db/src/logseq/db/frontend/entity_util.cljs:45-50`).
- **`:view-context :page`.** The property picker hides it on nodes that are not pages (`ui-non-suitable-property?`, `src/main/frontend/worker/handler/property.cljs:305-320`).
- **It carries no class semantics.** It creates no instance-of relationship, adds no tag properties, and plays no part in the `extends` hierarchy.
- **Import is what fills it in.** When a file graph is imported, `update-page-tags` runs on each page's file-graph tags. Tags that become classes go to `:block/tags`. `convert-tag-to-class` returns nil for the rest, and those are kept as page references in `:logseq.property/page-tags` (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:287-299`, `:344-370`).

Typical query:

```clojure
[:find ?page ?tag-page
 :where
 [?page :logseq.property/page-tags ?tag-page]]
```

> Do not confuse this property with `fix-page-tags` in `src/main/frontend/worker/pipeline.cljs:161-162`. Despite the name, that function handles the `#Page` **class** in `:block/tags` and never touches `:logseq.property/page-tags`.

## When to use which

- **New DB-graph work:** use `:block/tags`. It is the canonical tagging attribute and the only one with class semantics.
- **Data imported from a file graph:** expect some tags in `:logseq.property/page-tags`. To bring such a page into the class system, use the "Convert … to tag" action (`src/resources/dicts/en.edn:1154`). Then tag nodes with `:block/tags`.
- **Writing queries:** you almost always want `:block/tags`. Use `:logseq.property/page-tags` only when you specifically need the imported page-to-page associations.

## Common misconceptions

**"They are interchangeable."**
No. `:block/tags` must point at a class entity and participates in the class system. `:logseq.property/page-tags` accepts any page and is a plain many-to-many association.

**"`:logseq.property/page-tags` can be set on blocks from the UI."**
No. Its `:view-context` is `:page`, so the property picker only offers it on pages. Tag individual blocks with `:block/tags`.

**"The `#tag` syntax writes to `:logseq.property/page-tags`."**
No. In DB graphs, inline `#tag` writes to `:block/tags` (`core.cljs:264-273`). Only the importer or the property UI on a page populates `:logseq.property/page-tags`.

**"Every `#tag` sticks."**
No. On ordinary blocks, `#Template`, `#Query`, `#Cards`, `#Page`, `#Journal` and the other `disallowed-inline-tags` are silently stripped (`class.cljs:136-140`, `core.cljs:275-331`).

## Further reading

- Definitions: `deps/db/src/logseq/db/frontend/property.cljs:75-80` (`:block/tags`), `:202-208` (`:logseq.property/page-tags`)
- Schema: [Database Schema Overview](../../reference/database/schema-overview.md)
- Migration context: [File vs Database Graphs](file-vs-db.md)
