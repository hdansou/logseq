---
title: 'Database Graph Schema Reference'
description: "Complete reference for Logseq's database graph schema, property types, and built-in classes"
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
sinceVersion: '0.10.0'
relatedDocs:
  explanation:
    - '[File vs DB Graphs](../../explanation/concepts/file-vs-db.md)'
    - '[Schema System](../../explanation/architecture/schema-system.md)'
    - '[Data Architecture](../../explanation/architecture/data-architecture.md)'
  reference:
    - '[Properties Reference](../data/properties-reference.md)'
    - '[Query System](../advanced/query-system.md)'
---

# Database Graph Schema Reference

> **Last revised:** 2026-09-11 (verified against `be800f1711`)

> Complete reference for the database graph schema architecture, including entity kinds, the property system, and built-in classes.

## Overview

Database graphs use a structured schema with three layers:

1. **DataScript layer**: in-memory database queried with Datalog. Its attribute schema is `schema` in `deps/db/src/logseq/db/frontend/schema.cljs:56-108`.
2. **Malli validation layer**: entity schemas in `deps/db/src/logseq/db/frontend/malli_schema.cljs`, compiled into validators by `deps/db/src/logseq/db/frontend/validate.cljs:11-14`.
3. **SQLite persistence layer**: one `db.sqlite` per graph (`deps/db/src/logseq/db/common/sqlite.cljs:29-33`). Its only table is `kvs` (`sqlite.cljs:13`), which stores the DataScript data. You query it as DataScript, not with SQL.

**Current schema version**: 65.33 (`deps/db/src/logseq/db/frontend/schema.cljs:33`)

---

## Quick Reference

| Concept             | Count | How it was counted |
| ------------------- | ----- | ------------------ |
| Built-in properties | 118   | Key/value pairs inside the `defkeywords` form of `built-in-properties` (`deps/db/src/logseq/db/frontend/property.cljs:20-704`) |
| Built-in classes    | 18    | Key/value pairs inside the `defkeywords` form of `built-in-classes` (`deps/db/src/logseq/db/frontend/class.cljs:17-105`) |
| Property types      | 19    | 11 in `internal-built-in-property-types` + 8 in `user-built-in-property-types` (`deps/db/src/logseq/db/frontend/property/type.cljs:15-21`) |
| Entity kinds        | 13    | Dispatch keys of the `Data` multi-schema (`deps/db/src/logseq/db/frontend/malli_schema.cljs:593-608`) |

The property and class counts come from reading each file with a Clojure reader and counting the pairs in the `defkeywords` form; every key was distinct. Of the 118 entries in `built-in-properties`, 105 are `:logseq.*` idents. The other 13 are DataScript attribute properties: `:block/alias`, `:block/tags`, `:block/parent`, `:block/order`, `:block/collapsed?`, `:block/page`, `:block/refs`, `:block/link`, `:block/title`, `:block/closed-value-property`, `:block/journal-day`, `:block/created-at` and `:block/updated-at`. This is the `db-attribute-properties` set (`property.cljs:711-717`), and an assertion checks that it matches the entries with an `:attribute` key (`property.cljs:719-722`). Only 26 entries have `:schema {:public? true}`, meaning users can add them to nodes and query them (see the docstring at `property.cljs:28-29`).

---

## Architecture

```
┌─────────────────────────────────────┐
│          User Layer                 │
│   Pages, Blocks, Properties, Tags   │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│       Malli Validation Schema       │
│  Entity validation, type checking   │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│        DataScript Schema            │
│  Attribute types, refs, indexing    │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│      SQLite Persistence             │
│  db.sqlite, single kvs table        │
└─────────────────────────────────────┘
```

---

## Entity Kinds

Validation assigns every entity one dispatch key. `entity-dispatch-key` tests the conditions in the order below, and the first match wins (`deps/db/src/logseq/db/frontend/malli_schema.cljs:557-591`). The schemas themselves are mapped in `Data` (`:593-608`).

| Dispatch key                  | Matches when                                                                 | Malli schema             |
| ----------------------------- | ---------------------------------------------------------------------------- | ------------------------ |
| `:reaction-entity`            | Has `:logseq.property.reaction/target`                                       | `reaction-entity`        |
| `:property`                   | Tagged `:logseq.class/Property`                                              | `property-page`          |
| `:class`                      | Tagged `:logseq.class/Tag`                                                   | `class-page`             |
| `:hidden`                     | A page with `:logseq.property/hide? true`                                    | `hidden-page`            |
| `:normal-page`                | Any other page: tagged `:logseq.class/Page` or `:logseq.class/Journal`, or the deprecated `:logseq.class/Whiteboard` | `normal-page` |
| `:asset-block`                | Has `:logseq.property.asset/type`                                            | `asset-block`            |
| `:file-block`                 | Has `:file/path`                                                             | `file-block`             |
| `:property-history-block`     | Has `:logseq.property.history/block`                                         | `property-history-block` |
| `:closed-value-block`         | Has `:block/closed-value-property`                                           | `closed-value-block`     |
| `:property-value-block`       | Has both `:logseq.property/created-from-property` and `:logseq.property/value` | `property-value-block` |
| `:property-value-placeholder` | `:db/ident` is `:logseq.property/empty-placeholder`                          | `property-value-placeholder` |
| `:block`                      | Any other entity with `:block/uuid`                                          | `block`                  |
| `:db-ident-key-value`         | Any other entity with `:db/ident`                                            | `db-ident-key-val`       |

Notes:

- The page predicates are tag checks: `class?`, `property?`, `journal?` and `internal-page?` test for the `Tag`, `Property`, `Journal` and `Page` tags. `page?` is true for any of the four (`deps/db/src/logseq/db/frontend/entity_util.cljs:24-50`). `asset?` checks for `:logseq.property.asset/type`, not a tag (`entity_util.cljs:52-56`).
- Journal pages are validated as `:normal-page` and carry `:block/journal-day`. Whiteboards were removed from the app, but the deprecated class and its dispatch branch remain (`class.cljs:35-38`, `malli_schema.cljs:571-572`).
- Normal blocks require `:block/title`, `:block/parent`, `:block/order` and `:block/page` (`malli_schema.cljs:406-414`).
- Asset blocks require `:logseq.property.asset/type`, `:logseq.property.asset/checksum` and `:logseq.property.asset/size` (`malli_schema.cljs:506-519`).
- `config.edn`, `custom.css` and `custom.js` are stored as `:file/path` entities under `logseq/` (`deps/db/src/logseq/db/sqlite/create_graph.cljs:205-217`).
- The built-in **Quick add** page is created with `:logseq.property/hide? true` (`deps/db/src/logseq/db/sqlite/util.cljs:111-112`), so it validates as `:hidden`.

---

## Property Types

Properties can store different types of values:

### User Property Types

Available when creating custom properties (`user-built-in-property-types`, `deps/db/src/logseq/db/frontend/property/type.cljs:19-21`):

| Type        | Stored value                                                          | Validation (`type.cljs:198-225`) |
| ----------- | --------------------------------------------------------------------- | -------------------------------- |
| `:default`  | Ref to a value block whose text is in `:block/title`                  | text entity                      |
| `:number`   | Ref to a value block whose number is in `:logseq.property/value`      | number entity                    |
| `:date`     | Ref to a journal page                                                 | journal date                     |
| `:datetime` | A number, stored directly                                             | `number?`                        |
| `:checkbox` | A boolean, stored directly                                            | `boolean?`                       |
| `:url`      | Ref to a value block holding the URL                                  | URL entity                       |
| `:node`     | Ref to a page or block                                                | node with a title                |
| `:asset`    | Ref to an asset node                                                  | asset node                       |

Where the value lives follows `value-ref-property-types` and `original-value-ref-property-types` (`type.cljs:52-61`). Only `:number` keeps its value in `:logseq.property/value`; `:default` and `:url` keep theirs in `:block/title`. A query therefore has to go through the ref entity, for example `[?b ?prop ?v] [?v :logseq.property/value ?n]` for a number.

### Internal Property Types

Only built-in properties may use these (`internal-built-in-property-types`, `deps/db/src/logseq/db/frontend/property/type.cljs:15-17`). The exception is `:map`, `:json` and `:string`, which user properties may also store; those can be created only through EDN or the API (`type.cljs:23-26`, and the `user-property` schema at `malli_schema.cljs:355-365`).

| Type          | Validation (`type.cljs:164-196`) | Example built-in properties                             |
| ------------- | -------------------------------- | ------------------------------------------------------- |
| `:keyword`    | `keyword?`                       | `:logseq.property/type`, `:logseq.property/view-context` |
| `:map`        | `map?`                           | `:logseq.property.table/filters`, `:logseq.property.fsrs/state` |
| `:coll`       | `coll?`                          | `:logseq.property.table/sorting`, `:logseq.property.table/ordered-columns` |
| `:any`        | `some?`                          | 4 built-in properties use it                            |
| `:entity`     | an Entity                        | `:block/parent`, `:block/page`, `:block/refs`           |
| `:class`      | a Class entity                   | `:block/tags`, `:logseq.property.class/extends`         |
| `:page`       | a Page entity                    | `:block/alias`                                          |
| `:property`   | a Property entity                | `:logseq.property.table/pinned-columns`                 |
| `:string`     | `string?`                        | `:block/title`, `:block/order`, `:logseq.property.asset/type` |
| `:json`       | `string?`                        | (none among built-ins)                                  |
| `:raw-number` | `number?`                        | `:block/journal-day`, `:logseq.property.asset/size`     |

### Type Characteristics

| Type        | Scalar/Ref | Cardinality | Closed Values |
| ----------- | ---------- | ----------- | ------------- |
| `:default`  | Reference  | One/Many    | ✅            |
| `:number`   | Reference  | One/Many    | ✅            |
| `:date`     | Reference  | One/Many    | ❌            |
| `:datetime` | Scalar     | One         | ❌            |
| `:checkbox` | Scalar     | One         | ❌            |
| `:url`      | Reference  | One/Many    | ✅            |
| `:node`     | Reference  | One/Many    | ❌            |
| `:asset`    | Reference  | One/Many    | ❌            |

Source: `closed-value-property-types` (`type.cljs:30-32`), `cardinality-property-types` (`:34-36`) and `user-ref-property-types` (`:63-66`). Closed values are themselves ref entities (closed-value blocks).

---

## Built-in Classes

Classes (tags) classify nodes and support inheritance through `:logseq.property.class/extends`. All 18 are defined in `built-in-classes` (`deps/db/src/logseq/db/frontend/class.cljs:17-105`).

### Core Classes

| Class                    | Extends | Purpose |
| ------------------------ | ------- | ------- |
| `:logseq.class/Root`     | -       | Top-level ancestor (title "Root Tag") |
| `:logseq.class/Tag`      | Root    | The class of classes: every class entity is tagged with it |
| `:logseq.class/Property` | Root    | The class of properties: every property entity is tagged with it |
| `:logseq.class/Page`     | Root    | Normal pages |

### Page Classes

| Class                      | Extends | Purpose      |
| -------------------------- | ------- | ------------ |
| `:logseq.class/Journal`    | Page    | Daily notes (`:logseq.property.journal/title-format "MMM do, yyyy"`) |
| `:logseq.class/Whiteboard` | Page    | Deprecated; still defined under a `TODO: Remove deprecated` comment (`class.cljs:35-38`) |

### Block Classes

| Class                           | Schema properties (`:schema :properties`)        | Purpose            |
| ------------------------------- | ------------------------------------------------ | ------------------ |
| `:logseq.class/Task`            | status, priority, deadline, scheduled            | Task management    |
| `:logseq.class/Query`           | query                                            | Executable queries |
| `:logseq.class/Card`            | fsrs/state, fsrs/due                             | Flashcards         |
| `:logseq.class/Cards`           | (none; extends Query)                            | Flashcard decks    |
| `:logseq.class/Asset`           | asset/type, asset/size, asset/checksum (all required) | Files/images  |
| `:logseq.class/Code-block`      | node/display-type, code/lang                     | Code blocks        |
| `:logseq.class/Quote-block`     | node/display-type                                | Block quotes       |
| `:logseq.class/Math-block`      | node/display-type                                | Math expressions   |
| `:logseq.class/Pdf-annotation`  | ls-type, pdf/hl-color, asset, pdf/hl-page, pdf/hl-value, pdf/hl-type, pdf/hl-image | PDF annotations |
| `:logseq.class/Comments`        | comments/blocks                                  | Comment container  |
| `:logseq.class/Comment`         | (none)                                           | A single comment   |
| `:logseq.class/Template`        | template-applied-to                              | Block templates    |

`Comments` and `Comment` are defined at `class.cljs:44-52` and were added by migrations 65.27 and 65.28 (`src/main/frontend/worker/db/migrate.cljs:153-156`). Both set `:logseq.property.class/hide-from-node`, as do `Asset`, `Code-block`, `Quote-block`, `Math-block` and `Pdf-annotation`.

### Class Hierarchy

This tree shows `:logseq.property.class/extends` only. Tagging is a separate relation.

```
Root
├── Tag
├── Property
├── Page
│   ├── Journal
│   └── Whiteboard (deprecated)
├── Task
├── Query
│   └── Cards (flashcard decks)
├── Card
├── Asset
├── Code-block
├── Quote-block
├── Math-block
├── Pdf-annotation
├── Comments
├── Comment
└── Template
```

`build-new-class` tags every class with `:logseq.class/Tag`, including `Root` itself. Unless a class already declares a parent, it also sets `:logseq.property.class/extends :logseq.class/Root` for every class except `Root` (`deps/db/src/logseq/db/sqlite/util.cljs:91-100`). Built-in classes are built through the same function (`deps/db/src/logseq/db/sqlite/create_graph.cljs:145-167`). User-created tags therefore extend Root and are *tagged* `Tag`; they do not extend `Tag`. Likewise, `build-new-property` tags properties with `:logseq.class/Property` (`util.cljs:76-77`).

### Class Inheritance

`:logseq.property.class/extends` is a `:class`-typed property with `:cardinality :many` (`property.cljs:164-168`), so a class can extend several parents. The outliner rejects extends cycles and changes to a built-in class's extends (`deps/outliner/src/logseq/outliner/validate.cljs:191-220`).

```clojure
;; Shape of a user-defined class entity
{:db/ident :user.class/Project-xAbc123   ; random suffix, see below
 :block/title "Project"
 :block/tags #{:logseq.class/Tag}
 :logseq.property.class/extends #{:logseq.class/Root}}
```

User class and property idents get a random suffix: a `-`, one random non-digit character and a 7-character nano-id. The exceptions are plugin classes and runs where `LOGSEQ_STABLE_IDENTS` is set (`deps/db/src/logseq/db/frontend/db_ident.cljc:66-90`). Never hard-code a user ident; look it up by title.

---

## Key Built-in Properties

Types and cardinalities below are read from each entry's `:schema` in `built-in-properties`.

### DataScript Attribute Properties

These 13 built-in properties are also DataScript attributes (`db-attribute-properties`, `property.cljs:711-717`). `:block/uuid` and `:block/name` are DataScript attributes (`schema.cljs:61`, `:90`) but are **not** built-in properties.

| Property                       | Type       | Cardinality | Public? |
| ------------------------------ | ---------- | ----------- | ------- |
| `:block/title`                 | string     | one         | no      |
| `:block/tags`                  | class      | many        | yes     |
| `:block/alias`                 | page       | many        | yes (view context `:page`) |
| `:block/parent`                | entity     | one         | no      |
| `:block/page`                  | entity     | one         | no      |
| `:block/order`                 | string     | one         | no      |
| `:block/refs`                  | entity     | many        | no      |
| `:block/link`                  | entity     | one         | no      |
| `:block/collapsed?`            | checkbox   | one         | no      |
| `:block/created-at`            | datetime   | one         | no      |
| `:block/updated-at`            | datetime   | one         | no      |
| `:block/journal-day`           | raw-number | one         | no      |
| `:block/closed-value-property` | entity     | not set in `:schema`; the DataScript attribute is many (`schema.cljs:100-101`) | no |

### Task Properties

| Property                     | Type     | UI position    |
| ---------------------------- | -------- | -------------- |
| `:logseq.property/status`    | default (closed values) | `:block-left`  |
| `:logseq.property/priority`  | default (closed values) | `:block-left`  |
| `:logseq.property/deadline`  | datetime | `:block-below` |
| `:logseq.property/scheduled` | datetime | `:block-below` |

All four are public. Status and priority values are closed-value **ref** entities, not strings.

**Status closed values** (`property.cljs:313-318`; the default is `:logseq.property/status.todo`, `:320`):

- `:logseq.property/status.backlog`: Backlog
- `:logseq.property/status.todo`: Todo
- `:logseq.property/status.doing`: Doing
- `:logseq.property/status.in-review`: In Review
- `:logseq.property/status.done`: Done
- `:logseq.property/status.canceled`: Canceled

**Priority closed values** (`property.cljs:334-337`): Low, Medium, High and Urgent (`:logseq.property/priority.low` through `:logseq.property/priority.urgent`).

### View Properties

All of these are hidden and non-public.

| Property                                  | Type     | Cardinality |
| ----------------------------------------- | -------- | ----------- |
| `:logseq.property.view/type`              | default  | one         |
| `:logseq.property.table/sorting`          | coll     | one         |
| `:logseq.property.table/filters`          | map      | one         |
| `:logseq.property.table/ordered-columns`  | coll     | one         |
| `:logseq.property.table/hidden-columns`   | keyword  | many        |
| `:logseq.property.table/sized-columns`    | map      | one         |
| `:logseq.property.table/pinned-columns`   | property | many        |

### Asset Properties

| Property                          | Type       |
| --------------------------------- | ---------- |
| `:logseq.property.asset/type`     | string     |
| `:logseq.property.asset/size`     | raw-number |
| `:logseq.property.asset/checksum` | string     |

For the full property list, see [Properties Reference](../data/properties-reference.md).

### FSRS (Flashcard) Properties

| Property                      | Type     |
| ----------------------------- | -------- |
| `:logseq.property.fsrs/state` | map      |
| `:logseq.property.fsrs/due`   | datetime |

---

## Property Configuration

Each entry in `built-in-properties` is a config map. The keys are documented in the var's docstring (`property.cljs:21-39`):

```clojure
{:title "Property Name"       ; becomes :block/title
 :attribute :block/attribute  ; only for the 13 DataScript attribute properties
 :schema {:type :default      ; property type
          :cardinality :one   ; :one (default when unset) or :many
          :public? true       ; users may add it and query it
          :hide? false        ; hide when set on a node
          :view-context :block ; where it can be seen
          :ui-position :block-left} ; where it renders
 :closed-values [...]         ; vec of maps with :value, :db-ident, :uuid, :icon
 :queryable? true             ; available in the query builder
 :properties {...}            ; other properties, e.g. :logseq.property/default-value
 :rtc {...}}                  ; RTC sync config
```

The unqualified `:schema` keys map to the properties `:logseq.property/type`, `:logseq.property/hide?`, `:logseq.property/public?`, `:logseq.property/ui-position`, `:logseq.property/view-context` and `:logseq.property/classes`, plus the DataScript attribute `:db/cardinality` (`schema-properties-map`, `property.cljs:738-748`).

### View Contexts

Valid values are defined by the `internal-property` schema (`malli_schema.cljs:349`). The key is declared only on internal properties; the `user-property` schema does not declare it (`malli_schema.cljs:355-365`). A property with no view context can be seen in any context (`property.cljs:30-32`).

| Context     | Description            |
| ----------- | ---------------------- |
| `:page`     | Only on pages          |
| `:block`    | Only on blocks         |
| `:class`    | Only on class pages    |
| `:property` | Only on property pages |
| `:never`    | Never shown            |

### UI Positions

Valid values (`property-common-schema-attrs`, `malli_schema.cljs:336-340`; also checked in `deps/outliner/src/logseq/outliner/property.cljs:1076`):

| Position       | Description              |
| -------------- | ------------------------ |
| `:properties`  | Standard properties area |
| `:block-left`  | Left of block content    |
| `:block-right` | Right of block content   |
| `:block-below` | Below block content      |

---

## DataScript Schema

`schema` in `deps/db/src/logseq/db/frontend/schema.cljs:56-108` defines these attribute constraints. Attributes with an empty map, such as `:kv/value`, `:block/collapsed?`, `:block/tx-id` and `:file/content`, are scalar and unindexed.

| Attribute                      | Constraints                    | Notes              |
| ------------------------------ | ------------------------------ | ------------------ |
| `:db/ident`                    | unique identity                | Unique keyword     |
| `:block/uuid`                  | unique identity                | Unique lookup      |
| `:block/parent`                | ref, indexed                   | Parent entity      |
| `:block/order`                 | indexed                        | Sibling order      |
| `:block/page`                  | ref, indexed                   | Containing page    |
| `:block/refs`                  | ref, cardinality many          | Outgoing links     |
| `:block/tags`                  | ref, cardinality many          | Classes            |
| `:block/link`                  | ref, indexed                   | Embed/link target  |
| `:block/alias`                 | ref, cardinality many, indexed | Page aliases       |
| `:block/created-at`            | indexed                        | Creation time      |
| `:block/updated-at`            | indexed                        | Update time        |
| `:block/name`                  | indexed (not unique)           | Lowercase page name |
| `:block/title`                 | indexed                        | Content/name       |
| `:block/journal-day`           | indexed                        | Journal date (YYYYMMDD) |
| `:block/closed-value-property` | ref, cardinality many          | Closed value owner |
| `:file/path`                   | unique identity                | File entities      |

Property idents are not listed there. Each property entity is installed as its own attribute when it is built: `build-new-property` sets `:db/index true` and `:db/cardinality`, and adds `:db/valueType :db.type/ref` for ref types (`deps/db/src/logseq/db/sqlite/util.cljs:59-89`).

### Reference vs Scalar

- **Reference types** (`:db.type/ref`): store entity IDs and enable joins.
- **Scalar types** (no `:db/valueType`): store values directly (`db-non-ref-attributes`, `schema.cljs:137-140`).

`:block/properties` is not a DataScript attribute, so it never matches in `:where`. It is computed on an Entity (`deps/db/src/logseq/db/common/entity_plus.cljc:138-146`).

---

## Querying the Schema

### Find All Properties

`build-new-property` always sets `:logseq.property/type` (`util.cljs:76-77`):

```clojure
[:find (pull ?p [*])
 :where [?p :logseq.property/type]]
```

### Find All Classes

Every class is tagged `:logseq.class/Tag`. Matching on `:logseq.property.class/extends` would miss `Root`, which has no extends.

```clojure
[:find (pull ?c [*])
 :where [?c :block/tags ?t]
        [?t :db/ident :logseq.class/Tag]]
```

### Find Entities by Class

```clojure
[:find (pull ?b [*])
 :where [?b :block/tags ?t]
        [?t :db/ident :logseq.class/Task]]
```

### Find Property Values

Status values are closed-value entities, so join to their title:

```clojure
[:find ?v
 :where [?b :logseq.property/status ?v-ent]
        [?v-ent :block/title ?v]]
```

---

## Schema Migrations

Schema versions use the `major.minor` format (e.g. 65.33), parsed by `parse-schema-version` (`schema.cljs:8-25`).

Migrations are defined in `schema-version->updates` in `src/main/frontend/worker/db/migrate.cljs:123-165`. Each entry pairs a version string with a map of `:properties`, `:classes`, `:fix` or `:delete-properties`; for example, 65.33 adds five gallery view properties (`:161-165`). At load, `db-schema/version` is compared with the newest migration version. An assertion fails if `db-schema/version` is newer, and a warning is logged if it is older (`:167-175`).

When you add a property or class to `built-in-properties` or `built-in-classes`, add a migration entry for it and bump `version` in `schema.cljs` to match.

---

## See Also

- **Properties Reference**: [properties-reference.md](../data/properties-reference.md) for the full built-in property list
- **Schema System**: [schema-system.md](../../explanation/architecture/schema-system.md) for how the layers fit together
- **Query System**: [query-system.md](../advanced/query-system.md) for querying data
- **File vs DB**: [file-vs-db.md](../../explanation/concepts/file-vs-db.md) for comparison

---

_Last updated: 2026-09-11 | [Report an issue](https://github.com/logseq/logseq/issues)_
