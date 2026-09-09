---
title: ':block/tags vs :logseq.property/page-tags'
description: 'Understanding the two tag-like attributes in Logseq DB graphs and when each applies'
status: stable
audience: [developer]
lastVerified: '2026-04-22'
logseqVersion: '0.11.x'
category: concepts
relatedDocs:
  reference:
    - '[Database Schema Overview](../../reference/database/schema-overview.md)'
  explanation:
    - '[File vs Database Graphs](file-vs-db.md)'
---

# `:block/tags` vs `:logseq.property/page-tags`

> In a Logseq DB graph there are two attributes that both look like "tags" but mean different things. `:block/tags` is the canonical class/instance relationship; `:logseq.property/page-tags` is a backward-compatible page-to-page association preserved for migration from file graphs.

## Overview

| Aspect              | `:block/tags`                        | `:logseq.property/page-tags`               |
| ------------------- | ------------------------------------ | ------------------------------------------ |
| Kind                | Core schema attribute                | Built-in property                          |
| Semantics           | *is-a* (class instance)              | *related-to* (page ↔ page)                 |
| Value type          | Tag/class entity                     | Any page                                   |
| Cardinality         | many (ref)                           | many                                       |
| Applies to          | Any block or page                    | Pages only (`:view-context :page`)         |
| Created by          | `#tag` syntax in editor              | Legacy `tags::` migration / property UI    |
| Participates in     | Class system, required properties    | Plain backlink-style grouping              |

## `:block/tags` — the real tags

`:block/tags` is a core schema attribute (`ref`, `:db.cardinality/many`) listed in the Datascript schema alongside `:block/refs`, `:block/alias`, etc. Values are class/tag entities (e.g. `:logseq.class/Task`, or a user-defined class).

Tagging a block or page with `:block/tags` makes that entity an **instance** of the class. That relationship carries:

- Inheritance of required properties defined on the class.
- Visibility in the class's "Nodes" view and class-based queries.
- The standard semantics of the `#tag` syntax in the DB-graph editor.

Typical query:

```clojure
[:find ?b
 :where
 [?b :block/tags ?t]
 [?t :block/name "task"]]
```

## `:logseq.property/page-tags` — backward-compatible tagging

`:logseq.property/page-tags` is a built-in property defined in `deps/db/src/logseq/db/frontend/property.cljs`:

```clojure
:logseq.property/page-tags
{:title "Page Tags"
 :schema {:type :page
          :public? true
          :view-context :page
          :cardinality :many}
 :properties
 {:logseq.property/description
  "Provides a way for a page to associate to another page
   i.e. backward compatible tagging."}}
```

Key points:

- **Value type is `:page`**, not a class entity. Anything that is a page can be a page-tag.
- **`:view-context :page`** — the property only surfaces on pages, not on ordinary blocks.
- It carries **no class semantics**: there is no instance-of relationship, no inheritance, no required-property machinery.
- It exists so the file-graph `tags::` style (which was really "page references treated as tags") keeps working after migration to a DB graph.

Typical query:

```clojure
[:find ?page ?tag-page
 :where
 [?page :logseq.property/page-tags ?tag-page]]
```

## When to use which

- **New DB-graph work** — use `:block/tags`. It is the canonical tagging attribute and the only one with class semantics.
- **Working with data migrated from a file graph** — expect tags expressed through `:logseq.property/page-tags`. If you want those pages to participate in the class system, convert the referenced pages into classes and re-tag with `:block/tags`.
- **Writing queries** — `:block/tags` is almost always what you want. Reach for `:logseq.property/page-tags` only when you specifically need the legacy page-to-page associations.

## Common misconceptions

**"They are interchangeable."**
No. `:block/tags` requires the value to be a tag/class entity and participates in the class system. `:logseq.property/page-tags` accepts any page and is a plain many-to-many association.

**"`:logseq.property/page-tags` works on blocks."**
No. Its `:view-context` is `:page`, so it is surfaced on pages only. Tagging individual blocks is done through `:block/tags`.

**"The `#tag` syntax writes to `:logseq.property/page-tags`."**
No. In DB graphs the `#tag` syntax writes to `:block/tags`. `:logseq.property/page-tags` is populated through migration or the property UI on a page.

## Further reading

- Definition: `deps/db/src/logseq/db/frontend/property.cljs` (`:logseq.property/page-tags`)
- Schema: [Database Schema Overview](../../reference/database/schema-overview.md)
- Migration context: [File vs Database Graphs](file-vs-db.md)
