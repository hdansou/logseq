---
title: 'Tag properties vs page properties'
description: 'Understanding the two distinct kinds of properties in a Logseq DB graph — schema declared by a tag versus values stored on a page — and when to use each'
status: stable
audience: [developer, user]
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
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
| Canonical title | "Tag Properties" (`property.cljs:172`)                      | — (just the block's own properties)                        |

---

## Tag properties — a class's schema

A tag in the DB graph is a class entity (identified by having `:logseq.class/Tag` in its `:block/tags`; see `deps/db/src/logseq/db/frontend/entity_util.cljs:28-30`). A class can declare the properties its instances should have through `:logseq.property.class/properties`:

```clojure
;; deps/db/src/logseq/db/frontend/property.cljs:172-176
:logseq.property.class/properties {:title "Tag Properties"
                                   :schema {:type :property
                                            :cardinality :many
                                            :public? true
                                            :view-context :never}}
```

`:view-context :never` is why this attribute never appears as an ordinary property row on a node — it is schema plumbing, surfaced only through the tag's own editing UI.

The product itself calls these **Tag Properties**. The UI message when editing a class is explicit:

> "Tag properties are inherited by all nodes using the tag. For example, each `#Task` node inherits 'Status' and 'Priority'."
> — `src/resources/dicts/en.edn:194` (`:class/tag-properties-desc`), rendered at `src/main/frontend/components/property.cljs:1032`

Declaring a tag property does **not** give an instance a value. It tells the UI and queries "every node with this tag is expected to carry this property"; the instance's own datoms still hold the actual value (or absence thereof).

### Built-in examples

From `deps/db/src/logseq/db/frontend/class.cljs`:

```clojure
;; class.cljs:40-42
:logseq.class/Task
{:title "Task"
 :schema {:properties [:logseq.property/status
                       :logseq.property/priority
                       :logseq.property/deadline
                       :logseq.property/scheduled]}}

;; class.cljs:59-61
:logseq.class/Card
{:title "Card"
 :schema {:properties [:logseq.property.fsrs/state
                       :logseq.property.fsrs/due]}}

;; class.cljs:68-74
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

The built-in class table has grown well beyond these three. `:logseq.class/Query` declares `:logseq.property/query` (class.cljs:54-57), `:logseq.class/Code-block` declares `:logseq.property.node/display-type` and `:logseq.property.code/lang` (76-79), and `:logseq.class/Comments`, `:logseq.class/Quote-block`, `:logseq.class/Math-block`, `:logseq.class/Pdf-annotation` and `:logseq.class/Template` each declare their own (class.cljs:17-105). The same tag-property mechanism carries all of them — there is no separate "internal schema" path.

### Inheritance across tags

Tags can extend other tags via `:logseq.property.class/extends`:

```clojure
;; deps/db/src/logseq/db/frontend/property.cljs:164-171
:logseq.property.class/extends {:title "Extends"
                                :schema {:type :class
                                         :cardinality :many
                                         :public? true
                                         :view-context :class}
                                :queryable? true
                                :properties
                                {:logseq.property/description
                                 "This enables tags to inherit properties from other tags"}}
```

`:logseq.class/Journal`, for example, extends `:logseq.class/Page` (`class.cljs:30-33`), so every journal page transitively picks up the page class's tag properties. `:logseq.class/Whiteboard` does the same (36-38), and `:logseq.class/Cards` extends `:logseq.class/Query` (63-66).

Resolution happens in three layers:

1. `get-class-extends` walks one class's `:logseq.property.class/extends` chain breadth-first and returns the ancestors in outermost-first order (`deps/db/src/logseq/db/frontend/class.cljs:167-176`).
2. `get-classes-parents` runs that over a set of tags, filtering to actual classes (`deps/db/src/logseq/db/frontend/db.cljs:91-96`).
3. `get-block-classes-properties` unions `:logseq.property.class/properties` across every class in `:block/tags` plus those ancestors, dropping classes that declare none (`deps/outliner/src/logseq/outliner/property.cljs:1020-1034`).

> **Note:** this citation moved. The walker was at `outliner/property.cljs:726-740` when this page was last verified; it is now at 1020-1034.

There is also a recursive Datalog counterpart, the `class-extends` rule, for use inside queries (`deps/db/src/logseq/db/frontend/rules.cljc:14-19`).

---

## Page properties — values on a specific page

Page properties are property datoms attached directly to a page entity (pages are blocks with a `:block/name`). They are the actual values the user types into a page — for example `Priority: A` on a task page, or a `Status: Doing` on a project page.

Page properties live under the `:user.property/*` namespace (for user-defined properties) or under specific `:logseq.property/*` idents (for built-ins). They are plain Datascript attributes on the page's entity, not wrapped by any class machinery.

Minimal query:

```clojure
[:find ?page-title ?status
 :where
 [?page :block/name _]
 [?page :block/title ?page-title]
 [?page :logseq.property/status ?s]
 [?s :block/title ?status]]
```

> **Fixed (broken example).** The earlier form was `[:find ?page ?status … [?page :logseq.property/status ?status]]`. It matches, but both columns bind bare entity ids: `?page` is the page's `:db/id`, and `?status` is the *status value entity's* `:db/id`, because `:logseq.property/status` is a ref to a closed-value entity. The result was a table of meaningless integers. Both need a datom pattern to reach a readable value — `:block/title` for a page, and `:block/title` (or `:logseq.property/value` for `:number`-typed properties) for a property value entity.

### Page properties can exist without a tag

A page does **not** need to be tagged with a class to have properties. You can add any property to any page, and it is stored as a direct datom. Tags influence which properties the UI *suggests* and which ones show up as "expected" columns in table views — they do not gate writes.

### What happens when both a tag and the page declare the same property?

If a class declares property `X` through `:logseq.property.class/properties`, and the page sets property `X` directly, there is **one datom** — the value lives on the page entity. The class declaration just tells the UI to show `X` as part of the page's expected property set. No merging, no override logic; the page is authoritative for values.

The partitioning has moved out of the UI layer and into the worker. It now lives at `src/main/frontend/worker/handler/property.cljs:641-646`:

```clojure
;; src/main/frontend/worker/handler/property.cljs:641-646
{:keys [all-classes classes-properties]} (block-class-properties db block-entity)
classes-properties-set (set (map :db/ident classes-properties))
block-own-properties (->> properties
                          (remove (fn [[id _]] (contains? recycled-only-property-ids id)))
                          (remove (fn [[id _]] (classes-properties-set id))))
```

> **Note:** this citation moved. The equivalent code was at `src/main/frontend/components/property.cljs:749-802` when this page was last verified. The component now consumes an already-partitioned result rather than computing it.

The page's properties are still simply split into "came from a tag's schema" and "set directly on this page" for display purposes. The worker goes one step further and walks `all-classes` in order to build the class-supplied rows, skipping any property the page already owns (`worker/handler/property.cljs:674-694`) — so the same property never appears twice, and the page's own value always wins the slot.

A parallel, position-aware version of the same split lives in `deps/outliner/src/logseq/outliner/property.cljs:1105-1124` (`get-block-positioned-properties`), which decides which properties render beside, below, or in the properties area of a block.

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
Only when the class declares `:required-properties` — still rare, and still only `:logseq.class/Asset` (`deps/db/src/logseq/db/frontend/class.cljs:74`) and `:logseq.class/Pdf-annotation` (`class.cljs:97-98`) among the built-ins. The general `:schema {:properties [...]}` form is a suggestion, not an enforcement.

**"Two pages with the same tag share property values."**
No. Each page holds its own property datoms. Sharing the tag means sharing the *shape*, not the *data*.

---

## Summary

- **Tag properties** are a schema: a list of property idents stored on a class entity in `:logseq.property.class/properties`, inherited by every instance through `:block/tags` and by every subclass through `:logseq.property.class/extends`.
- **Page properties** are data: actual property datoms attached to a specific page entity.
- The UI presents the union of "what the page's tags expect" plus "what the page has set" as a single property panel, but underneath they are two distinct concerns: *shape* (tag properties) and *content* (page properties).

## Findings: New Capabilities

- **The `class-extends` Datalog rule** makes tag inheritance queryable directly: `(class-extends ?ancestor ?descendant)` is recursive over `:logseq.property.class/extends` (`deps/db/src/logseq/db/frontend/rules.cljc:14-19`). It underpins the `tags` rule, which matches a tag *or any of its descendants* on a block (`rules.cljc:266-295`), and `object-has-class-property?`, which asks whether a property reaches a block through any of its classes (`rules.cljc:144-151`).
- **Tag properties can carry default values that behave as if written.** `property-missing-value` plus `ref-property-value-with-default` / `scalar-property-value-with-default` let a query see a class-declared default even though no datom exists on the instance (`rules.cljc:108-142`). The comment there is worth knowing: `(missing? …)` does not work for this, because entity lookup already returns the default. `:logseq.property/status` uses exactly this, defaulting to `:logseq.property/status.todo` (`deps/db/src/logseq/db/frontend/property.cljs:319-321`).
- **`:logseq.property.class/extends` is now `:queryable? true`** (`property.cljs:169`), so tag inheritance is exposed in the query builder rather than being datalog-only.
- **Tags can declare bidirectional properties.** `:logseq.property.class/enable-bidirectional?` and `:logseq.property.class/bidirectional-property-title` (`property.cljs:177-186`) make a tag surface the reverse of property links pointing at a node — a class-level capability with no page-property equivalent.
- **`:logseq.property.class/hide-from-node`** (`property.cljs:193-196`) lets a class stay invisible on its instances; several internal classes use it.

## Further reading

- Tag properties attribute: `deps/db/src/logseq/db/frontend/property.cljs:172`
- Tag inheritance attribute: `deps/db/src/logseq/db/frontend/property.cljs:164`
- Built-in classes: `deps/db/src/logseq/db/frontend/class.cljs:17-105`
- Extends walker: `deps/db/src/logseq/db/frontend/class.cljs:167-176`, `deps/db/src/logseq/db/frontend/db.cljs:91-96`
- Class/instance property resolution: `deps/outliner/src/logseq/outliner/property.cljs:1020-1034`
- Position-aware split: `deps/outliner/src/logseq/outliner/property.cljs:1105-1124`
- Worker code that partitions the two surfaces: `src/main/frontend/worker/handler/property.cljs:641-694`
- Class detection: `deps/db/src/logseq/db/frontend/entity_util.cljs:28-30`
- Related: [`:block/tags` vs `:logseq.property/page-tags`](block-tags-vs-page-tags.md)
