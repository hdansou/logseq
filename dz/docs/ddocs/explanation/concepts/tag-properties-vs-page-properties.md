---
title: 'Tag properties vs page properties'
description: 'Understanding the two distinct kinds of properties in a Logseq DB graph — schema declared by a tag versus values stored on a page — and when to use each'
status: stable
audience: [developer, user]
lastVerified: '2026-04-22'
logseqVersion: '0.11.x'
category: concepts
relatedDocs:
  reference:
    - '[Database Schema Overview](../../reference/database/schema-overview.md)'
    - '[Properties Reference](../../reference/data/properties-reference.md)'
  explanation:
    - '[:block/tags vs :logseq.property/page-tags](block-tags-vs-page-tags.md)'
    - '[File vs Database Graphs](file-vs-db.md)'
---

# Tag properties vs page properties

> In a Logseq DB graph, "tag properties" and "page properties" are not two flavours of the same thing — they live in different places, play different roles, and are authored through different UI. **Tag properties declare a schema on a tag. Page properties store actual values on a page.** Understanding this distinction is essential for designing tags, writing queries, and reasoning about inheritance.

## Overview

| Aspect          | Tag properties                                              | Page properties                                            |
| --------------- | ----------------------------------------------------------- | ---------------------------------------------------------- |
| Purpose         | **Schema** — which properties instances of the tag get      | **Values** — actual data set on a specific page            |
| Lives on        | A class/tag entity                                          | A page (or block) entity                                   |
| Stored in       | `:logseq.property.class/properties` (ref, many)             | `:user.property/*` / `:logseq.property/*` datoms           |
| Inherited       | Yes — by every node tagged with the class, and by subclasses| No — values belong to the page where they are set          |
| Authored in     | The tag's "Tag properties" section                          | Inline on the page                                         |
| Canonical title | "Tag Properties" (`property.cljs:174`)                      | — (just the block's own properties)                        |

---

## Tag properties — a class's schema

A tag in the DB graph is a class entity (identified by having `:logseq.class/Tag` in its `:block/tags`; see `deps/db/src/logseq/db/frontend/entity_util.cljs:28-30`). A class can declare the properties its instances should have through `:logseq.property.class/properties`:

```clojure
;; deps/db/src/logseq/db/frontend/property.cljs:174-178
:logseq.property.class/properties {:title "Tag Properties"
                                   :schema {:type :property
                                            :cardinality :many
                                            :public? true
                                            :view-context :never}}
```

The product itself calls these **Tag Properties**. The UI message when editing a class is explicit:

> "Tag properties are inherited by all nodes using the tag. For example, each `#Task` node inherits 'Status' and 'Priority'."
> — `src/main/frontend/components/property.cljs:857`

Declaring a tag property does **not** give an instance a value. It tells the UI and queries "every node with this tag is expected to carry this property"; the instance's own datoms still hold the actual value (or absence thereof).

### Built-in examples

From `deps/db/src/logseq/db/frontend/class.cljs`:

```clojure
:logseq.class/Task
{:title "Task"
 :schema {:properties [:logseq.property/status
                       :logseq.property/priority
                       :logseq.property/deadline
                       :logseq.property/scheduled]}}

:logseq.class/Card
{:title "Card"
 :schema {:properties [:logseq.property.fsrs/state
                       :logseq.property.fsrs/due]}}

:logseq.class/Asset
{:title "Asset"
 :schema {:properties     [:logseq.property.asset/type
                           :logseq.property.asset/size
                           :logseq.property.asset/checksum]
          :required-properties [:logseq.property.asset/type
                                :logseq.property.asset/size
                                :logseq.property.asset/checksum]}}
```

Any page tagged `#Task` automatically surfaces `status`, `priority`, `deadline`, `scheduled` as expected properties. `#Asset` additionally enforces its three properties through validation.

### Inheritance across tags

Tags can extend other tags via `:logseq.property.class/extends`:

```clojure
;; deps/db/src/logseq/db/frontend/property.cljs:166-173
:logseq.property.class/extends {:title "Extends"
                                :schema {:type :class
                                         :cardinality :many
                                         :public? true
                                         :view-context :class}
                                :properties
                                {:logseq.property/description
                                 "This enables tags to inherit properties from other tags"}}
```

`:logseq.class/Journal`, for example, extends `:logseq.class/Page` (`class.cljs:30-33`), so every journal page transitively picks up the page class's tag properties. The walker that resolves the full property set for an instance lives in `deps/outliner/src/logseq/outliner/property.cljs:726-740` — it unions `:logseq.property.class/properties` across every class in `:block/tags` and their `:extends` ancestors.

---

## Page properties — values on a specific page

Page properties are property datoms attached directly to a page entity (pages are blocks with a `:block/name`). They are the actual values the user types into a page — for example `Priority: A` on a task page, or a `Status: Doing` on a project page.

Page properties live under the `:user.property/*` namespace (for user-defined properties) or under specific `:logseq.property/*` idents (for built-ins). They are plain Datascript attributes on the page's entity, not wrapped by any class machinery.

Minimal query:

```clojure
[:find ?page ?status
 :where
 [?page :block/name _]
 [?page :logseq.property/status ?status]]
```

### Page properties can exist without a tag

A page does **not** need to be tagged with a class to have properties. You can add any property to any page, and it is stored as a direct datom. Tags influence which properties the UI *suggests* and which ones show up as "expected" columns in table views — they do not gate writes.

### What happens when both a tag and the page declare the same property?

If a class declares property `X` through `:logseq.property.class/properties`, and the page sets property `X` directly, there is **one datom** — the value lives on the page entity. The class declaration just tells the UI to show `X` as part of the page's expected property set. No merging, no override logic; the page is authoritative for values. See how the UI partitions them at `src/main/frontend/components/property.cljs:749-802`:

```clojure
{:keys [all-classes classes-properties]}
   (outliner-property/get-block-classes-properties (db/get-db) (:db/id block))
 classes-properties-set (set (map :db/ident classes-properties))
 block-own-properties (->> properties
                           (remove (fn [[id _]] (classes-properties-set id))))
```

The page's properties are simply split into "came from a tag's schema" and "set directly on this page" for display purposes.

---

## How they work together — a worked example

You tag a page `Ship v2` with `#Task`:

1. `Ship v2` gains a datom `[<ship-v2> :block/tags :logseq.class/Task]`.
2. The class `:logseq.class/Task` declares, in its `:logseq.property.class/properties`, the properties `status`, `priority`, `deadline`, `scheduled`. **Nothing is copied onto `Ship v2`.**
3. The page-view UI calls `get-block-classes-properties`, walks `:block/tags` + `:extends`, and builds an "expected properties" list from every class's `:logseq.property.class/properties`. Those show up as empty-valued rows on `Ship v2`.
4. You fill in `Status: Doing`. That writes a datom `[<ship-v2> :logseq.property/status <doing-entity>]` directly on `Ship v2`. That is now a **page property** — a value on a specific page.
5. Your `#Task` query picks up `Ship v2` because of the `:block/tags` datom in step 1, and your "Status = Doing" filter matches because of the datom in step 4.

Tag properties gave the page its *shape*. Page properties gave it its *content*.

---

## When to use which

- **Declare a tag property** when multiple nodes of the same kind should carry the same field. Creating `#Book` with tag properties `author`, `read?`, and `rating` means every book page gains those three slots automatically. This is the right tool for modelling a *type*.
- **Set a page property directly** for one-off facts that are specific to that page and do not generalise. If only this one page needs a `related-ticket` field, just set it — don't create a tag for it.
- **Promote a page property to a tag property** once you notice you are setting the same property on many pages of the same kind. Add the property to the tag's schema so new instances get the slot automatically.
- **Query tag properties** when you want "all nodes of type X with field Y" — `?b :block/tags <class>` joined to `?b <property> ?v`. The class declaration tells you `?v` is expected to exist.
- **Query page properties** when the field is independent of any tag — just match the property attribute on the page.

---

## Common misconceptions

**"A tag property is a value on the tag."**
No. The tag stores a *list of property idents* — a schema. The values live on the instances.

**"Adding a tag property retroactively fills the value on existing pages."**
No. The UI starts showing the property as an expected slot on every page that has the tag, but the value is empty until each page gets its own datom written.

**"Removing a tag property deletes data from the instances."**
No. It removes the property from the class's declared schema. Any values already written to pages remain as ordinary page properties — they just stop appearing under the "inherited from tag" grouping in the UI.

**"Tag properties enforce required fields."**
Only when the class declares `:required-properties` (rare, used by `:logseq.class/Asset` and `:logseq.class/Pdf-annotation`). The general `:schema {:properties [...]}` form is a suggestion, not an enforcement. See `deps/db/src/logseq/db/frontend/class.cljs:63-64`.

**"Two pages with the same tag share property values."**
No. Each page holds its own property datoms. Sharing the tag means sharing the *shape*, not the *data*.

---

## Summary

- **Tag properties** are a schema: a list of property idents stored on a class entity in `:logseq.property.class/properties`, inherited by every instance through `:block/tags` and by every subclass through `:logseq.property.class/extends`.
- **Page properties** are data: actual property datoms attached to a specific page entity.
- The UI presents the union of "what the page's tags expect" plus "what the page has set" as a single property panel, but underneath they are two distinct concerns: *shape* (tag properties) and *content* (page properties).

## Further reading

- Tag properties attribute: `deps/db/src/logseq/db/frontend/property.cljs:174`
- Tag inheritance attribute: `deps/db/src/logseq/db/frontend/property.cljs:166`
- Built-in classes: `deps/db/src/logseq/db/frontend/class.cljs:17`
- Class/instance property resolution: `deps/outliner/src/logseq/outliner/property.cljs:726`
- UI that renders the two surfaces: `src/main/frontend/components/property.cljs:749`
- Related: [`:block/tags` vs `:logseq.property/page-tags`](block-tags-vs-page-tags.md)
