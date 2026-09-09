---
title: 'Database Schema System'
status: 🟢 Stable
version: 'Schema v65.33'
last-verified: '2026-09-09'
verified-against: 'be800f1711'
review-status: audited
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Database Properties Guide](../../tutorials/development/db-properties.md)'
  - '[Schema Reference](../../../../../dz/docs/03-features/database-graphs/schema-reference.md)'
  - '[Query System](../../reference/advanced/query-system.md)'
tags:
  ['schema', 'database', 'datascript', 'malli', 'architecture', 'properties']
---

# Database Schema System

This document explains Logseq's database graph schema architecture—the structured data model powering database-backed graphs. Understanding the schema system is essential for developers working with database APIs, properties, classes, and queries.

**What You'll Learn:**

- Schema architecture (DataScript + Malli)
- Property system and types
- Built-in classes and inheritance
- Entity types and relationships
- Schema validation and migrations

**Prerequisites:**

- Familiarity with database graphs (vs file-based)
- Basic understanding of Datalog queries
- ClojureScript knowledge (for contributors)

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture Layers](#architecture-layers)
3. [Property System](#property-system)
4. [Class System](#class-system)
5. [Entity Types](#entity-types)
6. [Validation & Integrity](#validation--integrity)
7. [Schema Migrations](#schema-migrations)
8. [Developer Guide](#developer-guide)
9. [Findings: New Capabilities](#findings-new-capabilities)

---

## Overview

### What is the Schema?

Logseq's **database schema** is a structured data model defining:

- **Properties**: Metadata attributes (`:logseq.property/status`, `:block/tags`, `:logseq.property/deadline`)
- **Classes**: Object types with inheritance (Task extends Root)
- **Entity types**: Pages, blocks, files, closed values
- **Relationships**: Parent-child, references, tags
- **Validation rules**: Type checking, cardinality, constraints

**Current Version:** 65.33 (major.minor) — `(def version (parse-schema-version "65.33"))` at
`deps/db/src/logseq/db/frontend/schema.cljs:33`.

> **Note:** every source citation on this page is pinned to `origin/master` at commit
> `be800f1711`.

### File-Based vs Database Graphs

| Aspect         | File-Based                           | Database                              |
| -------------- | ------------------------------------ | ------------------------------------- |
| **Storage**    | Markdown/Org files                   | SQLite database                       |
| **Properties** | Frontmatter + inline (`key:: value`) | First-class entities with schema      |
| **Schema**     | Implicit (parsed from text)          | Explicit (DataScript + Malli)         |
| **Validation** | Minimal (syntax check)               | Comprehensive (type + business logic) |
| **Queries**    | Simple text search                   | Datalog queries with joins            |
| **Sync**       | Git                                  | RTC with E2EE                         |

**Example - Same Property, Different Representations:**

```markdown
# File-Based Graph (Markdown)

- Task 1
  status:: pending
  due-date:: 2025-01-25
  tags:: [[Project Alpha]], [[High Priority]]
```

```clojure
;; Database Graph (DataScript entities)
{:block/uuid #uuid "abc-123"
 :block/title "Task 1"
 :block/tags [#:db{:id 456}  ; Project Alpha class
              #:db{:id 789}] ; High Priority class
 ;; :status is a ref to the closed value :logseq.property/status.todo,
 ;; not a string (property.cljs:300-322)
 :logseq.property/status #:db{:id 12}
 ;; :deadline is a :datetime, stored as a number (property.cljs:340-346)
 :logseq.property/deadline 20250125}
```

> **Removed:** the previous revision of this example used
> `:logseq.property.task/due-date`, which does not exist in `built-in-properties`. The
> `logseq.property.task` namespace is not in `logseq-property-namespaces`
> (`deps/db/src/logseq/db/frontend/property.cljs:756-763`) at all. Task dates are
> `:logseq.property/deadline` and `:logseq.property/scheduled`. It also showed
> `:logseq.property/status` holding the string `"pending"`; `:status` is a `:default`-typed
> property with closed values, so it holds a ref to a closed-value entity.

---

## Architecture Layers

### Multi-Layer Schema System

```
┌─────────────────────────────────────────────────────────┐
│                    User Layer                           │
│   Pages · Blocks · Properties · Tags · Queries          │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Malli Validation Layer                       │
│  • Entity type validation                               │
│  • Property value type checking                         │
│  • Cardinality enforcement (one vs many)                │
│  • Business logic rules                                 │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            DataScript Schema Layer                      │
│  • Attribute definitions (:db/ident, :db/valueType)     │
│  • References and relationships                         │
│  • Indexing configuration                               │
│  • Unique constraints                                   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│         SQLite Persistence Layer                        │
│  • Transaction log storage                              │
│  • Snapshot persistence                                 │
│  • RTC synchronization                                  │
└─────────────────────────────────────────────────────────┘
```

### DataScript Schema

**Location:** `deps/db/src/logseq/db/frontend/schema.cljs:56-108` (the `schema` var)

Defines the **structural schema** with attributes, types, and cardinality. The DB-graph schema
is small — 23 attributes total — because almost all metadata is modeled as property entities
rather than as DataScript attributes. Verbatim excerpt:

```clojure
{:db/ident        {:db/unique :db.unique/identity}
 :kv/value       {}

 :block/uuid {:db/unique :db.unique/identity}
 :block/parent {:db/valueType :db.type/ref
                :db/index true}
 :block/order {:db/index true}
 :block/collapsed? {}

 ;; belongs to which page
 :block/page {:db/valueType :db.type/ref
              :db/index true}
 ;; reference blocks
 :block/refs {:db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many}
 :block/tags {:db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many}
 ;; ...
 :block/created-at {:db/index true}
 :block/updated-at {:db/index true}}
```

Note that ref attributes here do **not** declare `:db/cardinality/one` explicitly; DataScript
defaults to one. Several attributes the previous revision showed as bare scalars
(`:block/title`, `:block/created-at`) actually carry `:db/index true`.

The file also derives five convenience sets from `schema` at lines 117-142:
`ref-type-attributes`, `card-many-attributes`, `card-many-ref-type-attributes`,
`card-one-ref-type-attributes` and `db-non-ref-attributes`.

**Attribute Types:**

| Type                 | Description                             | Example                                                |
| -------------------- | --------------------------------------- | ------------------------------------------------------ |
| **Scalar**           | No `:db/valueType`                      | `:block/title` (string), `:block/collapsed?` (boolean) |
| **Reference (one)**  | `:db.type/ref`, cardinality left default | `:block/parent`, `:block/page`, `:block/link`         |
| **Reference (many)** | `:db.type/ref` + `:db.cardinality/many` | `:block/tags`, `:block/refs`, `:block/alias`, `:block/closed-value-property` |

### Malli Schema

**Location:** `deps/db/src/logseq/db/frontend/malli_schema.cljs` (`Data` at lines 593-608,
`DB` at 610-615)

Provides **runtime validation** beyond structural constraints. Verbatim:

```clojure
(def Data
  (into
   [:multi {:dispatch (fn [d] (entity-dispatch-key *db-for-validate-fns* d))}]
   {:property property-page
    :class class-page
    :hidden hidden-page
    :normal-page normal-page
    :reaction-entity reaction-entity
    :property-history-block property-history-block
    :closed-value-block closed-value-block
    :property-value-block property-value-block
    :block block
    :asset-block asset-block
    :file-block file-block
    :db-ident-key-value db-ident-key-val
    :property-value-placeholder property-value-placeholder}))
```

`DB` is `[:sequential Data]`. The dispatch fn reads a dynamic var, `*db-for-validate-fns*`,
which callers bind before validating.

**Validation Rules:**

- Entity must have valid type
- Required fields present
- Property values match declared types
- Cardinality respected (one vs many)
- References point to existing entities

---

## Property System

### What are Properties?

**Properties** are typed metadata attributes that can be attached to blocks and pages. They define:

- **Key**: Unique identifier (e.g., `:logseq.property/status`)
- **Type**: `:schema {:type ...}` — see the 19 types below
- **Cardinality**: `:schema {:cardinality :many}`, otherwise one
- **Visibility**: `:schema {:public? ... :hide? ...}`

### Built-in Properties

**118 built-in properties**, keyed by `:db/ident`.

**Location:** `deps/db/src/logseq/db/frontend/property.cljs:20-704` (the file is `.cljs`,
not `.cljc`; entries span lines 43-704). The var is an **ordered map keyed by `:db/ident`**
built through `defkeywords`, not a vector of maps, and per-property configuration lives under a
nested `:schema` key rather than as flat `:logseq.property/*` keys. Verbatim excerpt:

```clojure
(def ^:large-vars/data-var built-in-properties
  (apply
   ordered-map
   (defkeywords
     :logseq.property/status {:title "Status"
                              :schema
                              {:type :default
                               :public? true
                               :ui-position :block-left}
                              :closed-values ; Backlog / Todo / Doing / In Review / Done / Canceled
                              (mapv ,,, [[:logseq.property/status.backlog "Backlog" "Backlog"] ,,,])
                              :properties {:logseq.property/hide-empty-value true
                                           :logseq.property/default-value :logseq.property/status.todo
                                           :logseq.property/enable-history? true}
                              :queryable? true}
     :block/tags           {:title "Tags"
                            :attribute :block/tags
                            :schema {:type :class
                                     :cardinality :many
                                     :public? true}
                            :queryable? true}
     ,,,)))
```

The recognised config keys are documented in the var's own docstring
(`property.cljs:21-39`): `:schema` (with `:type`, `:cardinality`, `:hide?`, `:public?`,
`:view-context`), `:title`, `:attribute`, `:queryable?`, `:closed-values`, `:properties` and
`:rtc`.

> **Removed:** `:logseq.property/cardinality` is not a key in these definitions — cardinality is
> `:schema {:cardinality :many}` and maps to the DataScript attribute `:db/cardinality`
> (`schema-properties-map`, `property.cljs:738-748`). `:logseq.property/tags` does not exist
> either; the tags property is the db-attribute-backed `:block/tags` (`property.cljs:75-80`).

**Property Categories** (derived by counting the 118 idents by namespace at
`property.cljs:43-704`):

| Namespace                            | Count | Examples                                                    |
| ------------------------------------ | ----- | ----------------------------------------------------------- |
| `logseq.property`                    | 39    | `status`, `priority`, `deadline`, `scheduled`, `assignee`, `icon`, `query` |
| `block` (db-attribute-backed)        | 13    | `alias`, `tags`, `parent`, `page`, `title`, `created-at`     |
| `logseq.property.asset`              | 11    | `type`, `size`, `checksum`, `external-url`, `width`          |
| `logseq.property.view`               | 10    | `type`, `group-by-property`, `gallery-card-size`             |
| `logseq.property.table`              | 6     | `sorting`, `filters`, `hidden-columns`                       |
| `logseq.property.repeat`             | 6     | `recur-frequency`, `recur-unit`, `repeat-type`               |
| `logseq.property.pdf`                | 5     | `hl-page`, `hl-value`, `hl-color`, `hl-type`, `hl-image`     |
| `logseq.property.class`              | 5     | `extends`, `properties`, `enable-bidirectional?`             |
| `logseq.property.history`            | 4     | `block`, `property`, `ref-value`, `scalar-value`             |
| `logseq.property.user`               | 3     | `name`, `email`, `avatar`                                    |
| `logseq.property.recycle`            | 3     | `original-parent`, `original-page`, `original-order`         |
| `logseq.property.reaction`           | 2     | `emoji-id`, `target`                                         |
| `logseq.property.linked-references`  | 2     | `includes`, `excludes`                                       |
| `logseq.property.fsrs`               | 2     | `due`, `state`                                               |
| 7 single-entry namespaces            | 7     | `.sync`, `.publish`, `.node`, `.journal`, `.comments`, `.code`, `.agent` |
| **Total**                            | **118** |                                                            |

> **Removed:** the previous revision's category table listed **Card (Flashcards) 15+**,
> **Whiteboard 10+** (`tldraw-page`, `tldraw-camera`) and **RTC (Sync) 5+**
> (`rtc-recent-edit-time`, `rtc-block-version`). None of those properties exist in
> `built-in-properties`. Spaced repetition is two FSRS properties; the `logseq.property.tldraw`
> namespace is declared in `logseq-property-namespaces` (`property.cljs:757`) but has zero
> built-in entries; and RTC contributes one property, `:logseq.property.sync/large-title-object`.
> `hl-stamp`, `db-ident` and `page-ref` also do not exist as built-in properties.

### Property Types

**19 property types**, split into two vars in
`deps/db/src/logseq/db/frontend/property/type.cljs`. Verbatim:

```clojure
(def internal-built-in-property-types      ; type.cljs:15-17  (11 types)
  "Valid property types only for use by internal built-in-properties"
  #{:keyword :map :coll :any :entity :class :page :property :string :json :raw-number})

(def user-built-in-property-types          ; type.cljs:19-21  (8 types)
  "Valid property types for users in order they appear in the UI"
  [:default :number :date :datetime :checkbox :url :node :asset])
```

The count is enforced by an assertion at `type.cljs:227-230`: the keys of
`built-in-validation-schemas` must equal the union of the two sets, and that map has 19 entries
(`internal-validation-schemas` at 164-196 plus the 8 user types at 201-224).

| Type         | Ref? | Validator (`type.cljs`)                | Notes                                                    |
| ------------ | ---- | -------------------------------------- | -------------------------------------------------------- |
| **default**  | ref  | `text-entity?` (134-140)               | Value block whose `:block/title` is the text              |
| **number**   | ref  | `number-entity?` (127-132)             | Value stored in the ref's `:logseq.property/value`        |
| **date**     | ref  | `date?` (157-161)                      | Must point at a journal page                              |
| **datetime** | no   | `number?` (210-212)                    | Plain epoch number, not `#inst`                           |
| **checkbox** | no   | `boolean?` (213-215)                   |                                                           |
| **url**      | ref  | `url-entity?` (100-109)                |                                                           |
| **node**     | ref  | `node-entity?` (142-145)               | Any entity with a `:block/title`                          |
| **asset**    | ref  | `asset-entity?` (150-155)              | Must be tagged `:logseq.class/Asset`                      |
| **page**     | ref  | `page-entity?` (123-125)               | Internal only                                             |
| **class**    | ref  | `class-entity?` (115-117)              | Internal only                                             |
| **property** | ref  | `property-entity?` (119-121)           | Internal only                                             |
| **entity**   | ref  | `entity?` (111-113)                    | Internal only                                             |
| **keyword / map / coll / any / string / json / raw-number** | no | `internal-validation-schemas` (164-196) | Internal only |

Cardinality is **not** a property of the type; it is declared per property. `cardinality-property-types`
(`type.cljs:34-36`) lists which types may change cardinality: `#{:default :number :url :date :node :asset}`.

> **Removed:** there is no `built-in-property-types` var, and `:icon`, `:query`,
> `:external-link` and `:other` are not property types at `be800f1711`. Icons are a `:map`-typed
> property (`:logseq.property/icon`, `property.cljs:412-413`) and queries are a `:default`-typed
> property (`:logseq.property/query`, `property.cljs:197-201`). The previous revision also
> described `node` as "Reference (many)" and `page` as "Reference (one)"; cardinality is per
> property, not per type.

---

## Class System

### What are Classes?

**Classes** (also called "tags") are object types that:

- Group related blocks/pages
- Define property schemas
- Support inheritance (`extends`)
- Enable polymorphic queries

### Built-in Classes

**18 built-in classes** form the object hierarchy.

**Location:** `deps/db/src/logseq/db/frontend/class.cljs:17-105` (the file is `.cljs`, not
`.cljc`; entries span lines 22-102). Like `built-in-properties`, this is an `ordered-map` keyed
by `:db/ident`, not a vector of maps. Verbatim excerpt:

```clojure
(def ^:large-vars/data-var built-in-classes
  (apply
   ordered-map
   (defkeywords
     :logseq.class/Root {:title "Root Tag"}

     :logseq.class/Tag {:title "Tag"}

     :logseq.class/Journal
     {:title "Journal"
      :properties {:logseq.property.class/extends :logseq.class/Page
                   :logseq.property.journal/title-format "MMM do, yyyy"}}

     :logseq.class/Task
     {:title "Task"
      :schema {:properties [:logseq.property/status :logseq.property/priority
                            :logseq.property/deadline :logseq.property/scheduled]}}
     ,,,)))
```

Note the two distinct keys: `:properties` holds property **values** set on the class entity
itself (including `:logseq.property.class/extends`), while `:schema {:properties [...]}` lists
the properties the class **gives to its objects**. `:schema` may also carry
`:required-properties` (see `:logseq.class/Asset`, `class.cljs:73-74`, and
`:logseq.class/Pdf-annotation`, `class.cljs:94-98`).

**Class Hierarchy** (all 18):

```
Root                       ; :title "Root Tag"
├── Tag
├── Property
├── Page
│   ├── Journal (extends Page)
│   └── Whiteboard (extends Page, deprecated)
├── Task
├── Comments
├── Comment
├── Query
│   └── Cards (extends Query)
├── Card
├── Asset
├── Code-block             ; :title "Code"
├── Quote-block            ; :title "Quote"
├── Math-block             ; :title "Math"
├── Pdf-annotation         ; :title "PDF Annotation"
└── Template
```

Only three classes declare `:logseq.property.class/extends` in `built-in-classes`: `Journal`
and `Whiteboard` extend `Page` (`class.cljs:32`, `:38`), and `Cards` extends `Query`
(`class.cljs:66`). Everything else lands under `Root` because `build-new-class` assigns
`:logseq.property.class/extends :logseq.class/Root` to any class that does not already declare
one, and to every class except `Root` itself
(`deps/db/src/logseq/db/sqlite/util.cljs:91-100`).

> **Removed:** the previous revision showed `Cards` nested under `Card`; `Cards` extends
> `Query`, not `Card` (`class.cljs:63-66`). It also titled Root `"Root"` (it is `"Root Tag"`),
> attached `:logseq.property/built-in? true` inline to each class definition (built-in marking
> is applied at graph creation, not declared here), and gave `Task` the non-existent
> `:logseq.property.task/due-date` and `:logseq.property.task/priority`.

### Class Inheritance

Classes inherit properties from parent classes. Real example, verbatim from `class.cljs:30-33`:

```clojure
:logseq.class/Journal
{:title "Journal"
 :properties {:logseq.property.class/extends :logseq.class/Page
              :logseq.property.journal/title-format "MMM do, yyyy"}}
```

`get-class-extends` (`class.cljs:167-176`) walks `:logseq.property.class/extends` transitively;
`get-structured-children` (`class.cljs:154-165`) walks the other direction with the
`class-extends` Datalog rule.

> **Removed:** `:logseq.property.journal/page-ref` does not exist. The `logseq.property.journal`
> namespace has exactly one built-in property, `:logseq.property.journal/title-format`
> (`property.cljs:269-272`). Journal also sets `title-format` as a value under `:properties`,
> not as an entry in a `:logseq.property.class/properties` vector.

---

## Entity Types

### Core Entity Types

**13 entity types**, one per branch of the `Data` multi-schema
(`deps/db/src/logseq/db/frontend/malli_schema.cljs:593-608`):

1. **`:property`** — property definition page
2. **`:class`** — class/tag page
3. **`:hidden`** — page with `:logseq.property/hide?` true
4. **`:normal-page`** — regular page (whiteboards dispatch here too)
5. **`:reaction-entity`** — emoji reaction, anything with `:logseq.property.reaction/target`
6. **`:property-history-block`** — anything with `:logseq.property.history/block`
7. **`:closed-value-block`** — enum-style property values
8. **`:property-value-block`** — has both `:logseq.property/created-from-property` and `:logseq.property/value`
9. **`:block`** — regular block with content
10. **`:asset-block`** — file/image attachment
11. **`:file-block`** — anything with `:file/path`
12. **`:db-ident-key-value`** — an entity carrying only a `:db/ident`
13. **`:property-value-placeholder`** — `:logseq.property/empty-placeholder`

### Entity Dispatch

**Location:** `deps/db/src/logseq/db/frontend/malli_schema.cljs:557-590`

Verbatim:

```clojure
(defn entity-dispatch-key [db ent]
  (let [d (if (:block/uuid ent) (d/entity db [:block/uuid (:block/uuid ent)]) ent)
        ;; order matters as some block types are a subset of others e.g. :whiteboard
        dispatch-key (cond
                       (:logseq.property.reaction/target d) :reaction-entity
                       (entity-util/property? d)            :property
                       (entity-util/class? d)               :class
                       (and (entity-util/page? d)
                            (true? (:logseq.property/hide? d))) :hidden
                       (whiteboard? d)                      :normal-page
                       (entity-util/page? d)                :normal-page
                       (entity-util/asset? d)               :asset-block
                       (:file/path d)                       :file-block
                       (:logseq.property.history/block d)   :property-history-block
                       (:block/closed-value-property d)     :closed-value-block
                       (and (:logseq.property/created-from-property d)
                            (:logseq.property/value d))     :property-value-block
                       (= (:db/ident d) :logseq.property/empty-placeholder)
                                                            :property-value-placeholder
                       (:block/uuid d)                      :block
                       (:db/ident d)                        :db-ident-key-value)]
    dispatch-key))
```

**Dispatch Logic:**

1. Re-resolve the argument to a live entity when it carries a `:block/uuid`.
2. Test predicates in a **fixed order**, because several entity shapes are subsets of others.
3. Fall through to `:block` (has a uuid) or `:db-ident-key-value` (has only an ident).

> **Removed:** the previous revision's dispatch fn was fictional. There is no `:type` field on
> DB-graph entities, no `:logseq.property/schema-type` and no `:logseq.property/hidden?`.
> Hiding is `:logseq.property/hide?` (`property.cljs:46-48`), and the type test is a set of
> `entity-util` predicates (`property?`, `class?`, `page?`, `asset?`) rather than a `:type`
> keyword comparison.

---

## Validation & Integrity

### Two-Stage Validation

**Stage 1: DataScript Schema**

- Type checking (ref vs scalar)
- Cardinality enforcement
- Unique constraints
- Indexing

**Stage 2: Malli Validation**

- Entity type validation
- Required field presence
- Property value types
- Business logic rules

### Validation Flow

Validation is not a separate call the caller makes. It runs inside `logseq.db/transact!` via
`transact-sync` (`deps/db/src/logseq/db.cljs:154-190`):

```clojure
;; ldb/transact! -> transact-sync
(let [tx-report* (d/with db tx-data tx-meta)          ; speculative, not committed
      tx-report  (if pipeline-f (pipeline-f tx-report*) tx-report*)
      _          (throw-if-page-has-block-parent! (:db-after tx-report) (:tx-data tx-report))
      [validate-result errors] (if (should-validate-pipeline-result? tx-meta)
                                 (db-validate/validate-tx-report tx-report nil)  ; db.cljs:166
                                 [true nil])]
  (cond
    (not validate-result) (throw-invalid-tx! tx-meta tx-data errors tx-report)  ; db.cljs:171
    ;; else compare-and-set! conn db (:db-after tx-report)
    ))
```

The write is speculative (`d/with`) until validation passes, so an invalid transaction throws
`"DB write failed with invalid data"` (`db.cljs:145-152`) rather than committing.

> **Removed:** `schema/validate-tx-data` does not exist, and neither does
> `logseq.db.frontend.malli-schema/validate-entity`. The validation API lives in a different
> namespace, `logseq.db.frontend.validate`: `validate-tx-report` (`validate.cljs:24`),
> `validate-db` (`:90`), `validate-local-db!` (`:126`), plus `get-schema-validator` /
> `get-schema-explainer` (`:16`, `:20`) and `graph-counts` (`:111`).

### Schema Version Management

**Current Version:** 65.33 (`deps/db/src/logseq/db/frontend/schema.cljs:33`)

**Version Format:** `MAJOR.MINOR`, parsed into `{:major <num> :minor <num>}` by
`parse-schema-version` (`schema.cljs:8-25`) and compared with `compare-schema-version`
(`schema.cljs:27-31`).

- **Major**: Breaking changes requiring migration
- **Minor**: Backward-compatible additions

**Version Storage:** the version is a key-value entity, not a property:

```clojure
;; Written at graph creation (deps/db/src/logseq/db/sqlite/create_graph.cljs:239)
(sqlite-util/kv :logseq.kv/schema-version db-schema/version)

;; Read back (src/main/frontend/worker/db/migrate.cljs:299)
(:kv/value (d/entity db :logseq.kv/schema-version))
```

`:logseq.kv/schema-version` is declared in `deps/db/src/logseq/db/frontend/kv_entity.cljs:19`
with the doc "Graph's current schema version".

> **Removed:** there is no `:logseq.property/schema-version` property.

---

## Schema Migrations

### When Migrations Occur

**Migration triggers:**

1. **Schema version mismatch** (graph < app version)
2. **Adding new built-in properties**
3. **Modifying property types or cardinality**
4. **Renaming or removing properties**
5. **Class hierarchy changes**

### Migration System

**Location:** `src/main/frontend/worker/db/migrate.cljs:123-164`

It is a **vector of `[version-string migration-map]` tuples**, not a map, and the versions are
strings. Verbatim tail:

```clojure
(def schema-version->updates
  "A vec of tuples defining datascript migrations. Each tuple consists of the
   schema version integer and a migration map. A migration map can have keys of :properties, :classes
   and :fix."
  [["65.7"  {:fix add-quick-add-page}]
   ,,,
   ["65.25" {:delete-properties [:block/pre-block?
                                 :logseq.property.embedding/hnsw-label
                                 :logseq.property.embedding/hnsw-label-updated-at]}]
   ["65.26" {:properties [:logseq.property.repeat/repeat-type]}]
   ["65.27" {:classes [:logseq.class/Comments]
             :properties [:logseq.property.comments/blocks]}]
   ,,,
   ["65.33" {:properties [:logseq.property.view/gallery-asset-property
                          :logseq.property.view/gallery-display-properties
                          :logseq.property.view/gallery-card-size
                          :logseq.property.view/gallery-card-width
                          :logseq.property.view/gallery-card-height]}]])
```

The migration map keys are `:properties`, `:classes`, `:fix` and — undocumented in the var's own
docstring — `:delete-properties` (`migrate.cljs:149`). `:properties` and `:classes` take idents
that already exist in `built-in-properties` / `built-in-classes`; the migration only transacts
them into graphs that predate them. `:fix` takes a function of the db.

An assertion just below the var (`migrate.cljs:167-174`) requires the highest entry to not
exceed `db-schema/version`, which is how the two files stay in step.

**Migration Process:**

```
1. Check current schema version
   ↓
2. Find applicable migrations (current → target)
   ↓
3. Apply migrations sequentially
   ↓
4. Validate migrated data
   ↓
5. Update schema version
   ↓
6. Persist to SQLite
```

### Adding New Properties

**Process:**

From the `migrate` docstring (`migrate.cljs:295-296`): *"To add a new migration, add an entry to
`schema-version->updates` and bump `db-schema/version`."*

1. Add the property to `built-in-properties` in
   `deps/db/src/logseq/db/frontend/property.cljs` (nested `:schema` map form).
2. Append a tuple to `schema-version->updates` with the next minor version as a **string**.
3. Bump `(def version (parse-schema-version "..."))` in
   `deps/db/src/logseq/db/frontend/schema.cljs:33` to match.
4. Test on sample graphs.

**Example (adding `:logseq.property.repeat/repeat-type`, schema 65.26):**

```clojure
;; 1. deps/db/src/logseq/db/frontend/property.cljs:381-393
:logseq.property.repeat/repeat-type {:title "Repeating type"
                                     :schema {:type :default
                                              :public? false}
                                     :closed-values (mapv ,,, [,,,])
                                     :properties {:logseq.property/hide-empty-value true
                                                  :logseq.property/default-value
                                                  :logseq.property.repeat/repeat-type.double-plus}
                                     :queryable? true}

;; 2. src/main/frontend/worker/db/migrate.cljs:152
["65.26" {:properties [:logseq.property.repeat/repeat-type]}]

;; 3. deps/db/src/logseq/db/frontend/schema.cljs:33  -> bump the version
```

> **Removed:** the previous revision's example migration wrote a hand-rolled `add-property-foo`
> function returning a flat map with `:logseq.property/type` and
> `:logseq.property/cardinality`. Property additions do not need a `:fix` function — listing the
> ident under `:properties` is enough — and neither of those two keys is part of a built-in
> property definition.

---

## Developer Guide

### Working with Properties

**Query Properties:**

```clojure
;; Get property entity
(d/entity db :logseq.property/status)

;; Get property value from block. For a ref-valued property this is an entity,
;; so unwrap it with db-property/property-value-content (property.cljs:836-841)
;; or db-property/lookup (property.cljs:964-968).
(:logseq.property/status block)
(db-property/lookup block :logseq.property/status)   ;; => "Todo"

;; Query blocks by property value. :status is ref-valued with closed values,
;; so match the closed value's :db/ident, not a string.
'[:find ?b
  :where
  [?b :logseq.property/status ?v]
  [?v :db/ident :logseq.property/status.todo]]
```

> **Removed:** `[?b :logseq.property/status "pending"]` cannot match. `:logseq.property/status`
> is `:type :default` with closed values (`property.cljs:300-322`), and `:default` is in
> `value-ref-property-types` (`property/type.cljs:58-61`), so the datom value is an entity id.
> `"pending"` is also not one of the six closed values (`Backlog`, `Todo`, `Doing`, `In Review`,
> `Done`, `Canceled`).

**Update Properties:**

```clojure
;; Set a ref-valued property to one of its closed values
(ldb/transact! conn
  [[:db/add block-id :logseq.property/status :logseq.property/status.done]])

;; Multi-value property (cardinality :many)
(ldb/transact! conn
  [[:db/add block-id :block/tags tag-id-1]
   [:db/add block-id :block/tags tag-id-2]])
```

Prefer `logseq.db/transact!` over raw `datascript.core/transact!` for graph writes: it is what
runs the transact pipeline and Malli validation described above
(`deps/db/src/logseq/db.cljs:192-...`). For higher-level property writes, use
`logseq.outliner.property` rather than hand-built datoms.

### Creating Custom Properties

**Via Plugin API:** verified against `libs/src/LSPlugin.ts` — `checkCurrentIsDbGraph` at line
508, `upsertProperty` at 909-912, and the `PropertySchema` type at 366-371
(`{type, cardinality, hide, public}`, all optional via `Partial<>`).

```typescript
// Check if DB graph
const isDb = await logseq.App.checkCurrentIsDbGraph()

if (isDb) {
  // Create property
  await logseq.Editor.upsertProperty(
    'my-custom-prop',
    {
      type: 'default',
      cardinality: 'one',
      hide: false,
      public: true,
    },
    {
      name: 'My Custom Property', // Display name
    }
  )
}
```

### Schema Validation

**Validate Entity:**

```clojure
(require '[logseq.db.frontend.validate :as db-validate])

;; Validate a datascript tx-report; returns [valid? errors]
(db-validate/validate-tx-report tx-report {:closed-schema? false})   ; validate.cljs:24

;; Validate a whole db; returns {:datom-count .. :entities .. :errors ..}
(db-validate/validate-db db)                                         ; validate.cljs:90

;; Validate a local (non-RTC) graph with CLI/importer defaults and logging
(db-validate/validate-local-db! db {:db-name "my-graph" :verbose true}) ; validate.cljs:126

;; Graph-wide counts used in validation output
(db-validate/graph-counts db entities)                               ; validate.cljs:111
```

---

## Findings: New Capabilities

Present in `origin/master` at `be800f1711` and undescribed by the previous revision of this page.

### `defkeywords` + `ordered-map`, not plain data literals

Both `built-in-properties` (`deps/db/src/logseq/db/frontend/property.cljs:20-704`) and
`built-in-classes` (`deps/db/src/logseq/db/frontend/class.cljs:17-105`) are built by applying
`ordered-map` to a `defkeywords` form from `logseq.common.defkeywords`. Iteration order is
therefore the declaration order, which matters for anything that renders these in sequence.

### `:rtc` per-property sync exclusion

A property definition may carry `:rtc {:rtc/ignore-attr-when-syncing true}`
(`property.cljs:14-15`). Exactly seven built-ins use it (`property.cljs:482`, `489`, `496`,
`503`, `510`, `517`, `593`): `:logseq.property.view/gallery-card-size`, `gallery-card-width`,
`gallery-card-height`, `:logseq.property.view/sort-groups-by-property`,
`:logseq.property.view/sort-groups-desc?`, `:logseq.property.table/sorting` and
`:logseq.property.asset/last-visit-page` — so per-device view geometry does not round-trip
through RTC.

### `:queryable?` gates the query builder

Independently of `:public?`/`:hide?`, a property carries `:queryable?` to say whether the query
builder may use it (`property.cljs:35`). Seven built-ins are explicitly `:queryable? false`: `:logseq.property/choice-checkbox-state`,
`choice-classes`, `choice-exclusions`, `checkbox-display-properties`,
`:logseq.property.view/feature-type`, `:logseq.property.asset/external-file-name` and
`:logseq.property.asset/align`.

### Closed values, defaults and scoped choices

`:closed-values` entries carry `:db-ident`, `:value`, `:uuid` and optionally `:icon` and nested
`:properties` (`property.cljs:36-37`, worked example at `300-322`). Defaults come in two flavors:
`:logseq.property/default-value` for ref types and `:logseq.property/scalar-default-value` for
non-ref types (`property.cljs:154-163`). Choices can additionally be scoped per tag through
`:logseq.property/choice-classes` and hidden per tag through
`:logseq.property/choice-exclusions`, resolved by `scoped-closed-values`
(`property.cljs:940-962`).

### Built-in i18n titles

`built-in-ident->i18n-key` and `built-in-display-title` (`property.cljs:977-1024`) derive an
i18n key from a `:db/ident` (e.g. `:logseq.property.code/lang` → `:property.built-in/code-lang`,
`:logseq.property/status.backlog` → `:property.status/backlog`) and fall back to
`:block/title` when no translation exists. Built-in titles shown in the UI are therefore not
necessarily the `:title` strings in `property.cljs`.

### Class tag sets beyond the hierarchy

`class.cljs:119-150` defines six overlapping sets that control class behavior independently of
`extends`: `internal-tags` (119), `private-tags` (124), `block-kind-tags` (130),
`disallowed-inline-tags` (136), `extends-hidden-tags` (142) and `hidden-tags` (148). These decide, for example, that a user cannot type
`#Asset` inline.

### `:delete-properties` migrations

`schema-version->updates` supports a fourth key that its own docstring does not mention:
`:delete-properties`. Schema `65.25` used it to remove `:block/pre-block?`,
`:logseq.property.embedding/hnsw-label` and `:logseq.property.embedding/hnsw-label-updated-at`
(`src/main/frontend/worker/db/migrate.cljs:149-151`).

## Next Steps

**Continue Learning:**

1. **[Database Properties Guide](../../tutorials/development/db-properties.md)** - Plugin API for properties
   (> **Unverified:** this target does not exist in the repo at audit time; no file named
   `db-properties.md` was found anywhere under `dz/docs/ddocs`. The same path is referenced from
   this page's `related-docs` frontmatter.)
2. **[Complete Schema Reference](../../../../../dz/docs/03-features/database-graphs/schema-reference.md)** - Full property/class list
3. **[Query System](../../reference/advanced/query-system.md)** - Datalog queries
4. **[Schema Migration Guide](../../../../../dz/docs/08-processes/schema-migration-best-practices.md)** - For contributors

**For Contributors:**

- [Schema Change PR Checklist](../../../../../dz/docs/08-processes/schema-change-pr-checklist.md)
- [Schema Integrity Reports](../../../../../dz/docs/07-investigations/schema-integrity-08-index.md)
- [RTC Sync Schema Changes](../../../../../dz/docs/08-processes/rtc-sync-schema-changes.md)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
