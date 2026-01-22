---
title: 'Database Schema System'
status: 🟢 Stable
version: 'Schema v65.12'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Database Properties Guide](../../tutorials/development/db-properties.md)'
  - '[Schema Reference](../../../dz/docs/03-features/database-graphs/schema-reference.md)'
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

---

## Overview

### What is the Schema?

Logseq's **database schema** is a structured data model defining:

- **Properties**: Metadata attributes (status, tags, due-date)
- **Classes**: Object types with inheritance (Task extends Root)
- **Entity types**: Pages, blocks, files, closed values
- **Relationships**: Parent-child, references, tags
- **Validation rules**: Type checking, cardinality, constraints

**Current Version:** 65.12 (major.minor)

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
 :logseq.property/status "pending"
 :logseq.property.task/due-date #inst "2025-01-25"}
```

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

**Location:** `deps/db/src/logseq/db/frontend/schema.cljs`

Defines the **structural schema** with attributes, types, and cardinality:

```clojure
{:block/uuid       {:db/unique :db.unique/identity}
 :block/title      {}  ; String (scalar)
 :block/parent     {:db/valueType :db.type/ref        ; Reference to parent
                    :db/cardinality :db.cardinality/one}
 :block/tags       {:db/valueType :db.type/ref        ; Multiple class references
                    :db/cardinality :db.cardinality/many}
 :block/collapsed? {}  ; Boolean (scalar)
 :block/created-at {}  ; Number/timestamp (scalar)
 :db/ident         {:db/unique :db.unique/identity}}  ; Keyword identifier
```

**Attribute Types:**

| Type                 | Description                             | Example                                                |
| -------------------- | --------------------------------------- | ------------------------------------------------------ |
| **Scalar**           | No `:db/valueType`                      | `:block/title` (string), `:block/collapsed?` (boolean) |
| **Reference (one)**  | `:db.type/ref` + `:db.cardinality/one`  | `:block/parent`, `:block/page`                         |
| **Reference (many)** | `:db.type/ref` + `:db.cardinality/many` | `:block/tags`, `:block/refs`                           |

### Malli Schema

**Location:** `deps/db/src/logseq/db/frontend/malli_schema.cljs`

Provides **runtime validation** beyond structural constraints:

```clojure
;; Entity type dispatch
(def Data
  [:multi {:dispatch entity-dispatch-key}
   [:property property-page]        ; Property definition page
   [:class class-page]             ; Class/tag page
   [:normal-page normal-page]      ; Regular page
   [:block block]                  ; Regular block
   [:property-value-block ...]     ; Property value block
   ...])
```

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
- **Type**: Data type (default, number, date, checkbox, url, node)
- **Cardinality**: Single value (`one`) or multiple values (`many`)
- **Visibility**: Public (shown in UI) or hidden (internal)

### Built-in Properties

**100+ built-in properties** organized by category:

**Location:** `deps/db/src/logseq/db/frontend/property.cljc:177-421`

```clojure
(def built-in-properties
  [{:db/ident :logseq.property/status
    :block/title "Status"
    :logseq.property/type :default
    :logseq.property/cardinality :one
    :logseq.property/hide? false
    :logseq.property/public? true}

   {:db/ident :logseq.property/tags
    :block/title "Tags"
    :logseq.property/type :node
    :logseq.property/cardinality :many
    :logseq.property/hide? false
    :logseq.property/public? true}

   {:db/ident :logseq.property.task/due-date
    :block/title "Due Date"
    :logseq.property/type :date
    :logseq.property/cardinality :one
    :logseq.property/hide? false
    :logseq.property/public? true
    :logseq.property/classes [:logseq.class/Task]}
   ...])
```

**Property Categories:**

| Category              | Count | Examples                                |
| --------------------- | ----- | --------------------------------------- |
| **Core**              | 20+   | title, tags, alias, icon, template      |
| **Task**              | 10+   | due-date, priority, status              |
| **Journal**           | 5+    | title-format, page-ref                  |
| **Card (Flashcards)** | 15+   | interval, repetitions, ease-factor      |
| **Whiteboard**        | 10+   | tldraw-page, tldraw-camera              |
| **RTC (Sync)**        | 5+    | rtc-recent-edit-time, rtc-block-version |
| **Asset**             | 5+    | size, checksum, type, external-url      |
| **PDF**               | 5+    | hl-page, hl-value, hl-stamp             |
| **System**            | 30+   | created-at, updated-at, db-ident        |

### Property Types

**14 property types** available:

```clojure
(def built-in-property-types
  [:default :number :url :date :node :page :checkbox
   :coll :icon :property :class :query :external-link :other])
```

| Type         | Storage          | UI Input     | Example                  |
| ------------ | ---------------- | ------------ | ------------------------ |
| **default**  | String           | Text input   | "pending", "in-progress" |
| **number**   | Number           | Number input | 42, 3.14                 |
| **date**     | Timestamp        | Date picker  | #inst "2025-01-25"       |
| **checkbox** | Boolean          | Checkbox     | true, false              |
| **url**      | String           | URL input    | "https://example.com"    |
| **node**     | Reference (many) | Tag picker   | [#:db{:id 123}]          |
| **page**     | Reference (one)  | Page link    | #:db{:id 456}            |

---

## Class System

### What are Classes?

**Classes** (also called "tags") are object types that:

- Group related blocks/pages
- Define property schemas
- Support inheritance (`extends`)
- Enable polymorphic queries

### Built-in Classes

**15 built-in classes** form the object hierarchy:

**Location:** `deps/db/src/logseq/db/frontend/class.cljc:44-202`

```clojure
(def built-in-classes
  [{:db/ident :logseq.class/Root
    :block/title "Root"
    :logseq.property/built-in? true}

   {:db/ident :logseq.class/Tag
    :block/title "Tag"
    :logseq.property/built-in? true
    :logseq.property.class/extends [:logseq.class/Root]}

   {:db/ident :logseq.class/Task
    :block/title "Task"
    :logseq.property/built-in? true
    :logseq.property.class/extends [:logseq.class/Root]
    :logseq.property.class/properties [
      :logseq.property.task/due-date
      :logseq.property.task/priority
      :logseq.property/status
    ]}
   ...])
```

**Class Hierarchy:**

```
Root
├── Tag
├── Property
├── Page
│   ├── Journal (extends Page)
│   └── Whiteboard (extends Page)
├── Task
├── Query
├── Card (Flashcard)
│   └── Cards (collection)
├── Asset
├── Code-block
├── Quote-block
├── Math-block
├── Pdf-annotation
└── Template
```

### Class Inheritance

Classes inherit properties from parent classes:

```clojure
;; Journal extends Page
{:db/ident :logseq.class/Journal
 :logseq.property.class/extends [:logseq.class/Page]
 :logseq.property.class/properties [
   :logseq.property.journal/title-format
   :logseq.property.journal/page-ref
   ;; ... plus all properties from Page
 ]}
```

---

## Entity Types

### Core Entity Types

**8 primary entity types** in the schema:

1. **Normal Page**: Regular pages
2. **Class Page**: Tag/class definitions
3. **Property Page**: Property definitions
4. **Hidden Page**: System pages (not shown in UI)
5. **Block**: Regular blocks with content
6. **Property Value Block**: Stores multi-value property data
7. **Closed Value Block**: Enum-style property values
8. **Asset Block**: File/image attachments

### Entity Dispatch

**Location:** `deps/db/src/logseq/db/frontend/malli_schema.cljs:130-149`

```clojure
(defn entity-dispatch-key [db entity]
  (let [type (get entity :type)
        schema-type (:logseq.property/schema-type entity)]
    (cond
      (= type :property) :property
      (= type :class) :class
      (= type :closed-value) :closed-value-block
      (and (nil? type) schema-type (contains? #{:property-value} schema-type))
        :property-value-block
      (= type :page)
        (if (:logseq.property/hidden? entity) :hidden :normal-page)
      (= type :block) :block
      ...)))
```

**Dispatch Logic:**

1. Check `:type` field
2. Check `:logseq.property/schema-type`
3. Check `:logseq.property/hidden?`
4. Fallback to default handling

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

```clojure
;; Transaction submission
(d/transact! conn tx-data)
  ↓
;; DataScript schema validation (automatic)
  ↓
;; Malli validation (explicit)
(schema/validate-tx-data db tx-data)
  ↓
;; If valid: commit
;; If invalid: throw exception with details
```

### Schema Version Management

**Current Version:** 65.12

**Version Format:** `MAJOR.MINOR`

- **Major**: Breaking changes requiring migration
- **Minor**: Backward-compatible additions

**Version Storage:**

```clojure
;; Stored in database
{:db/ident :logseq.property/schema-version
 :logseq.property/schema-version 65.12}
```

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

**Location:** `src/main/frontend/worker/db/migrate.cljs`

```clojure
(def schema-version->updates
  {65.0 [add-new-property-X
         update-property-Y-type]
   65.1 [migrate-old-format
         add-property-Z]
   65.12 [...]})
```

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

1. Add property to `built-in-properties`
2. Increment minor version (e.g., 65.12 → 65.13)
3. Create migration function
4. Test on sample graphs
5. Deploy with version check

**Example Migration:**

```clojure
(defn add-property-foo [db]
  (when-not (d/entity db :logseq.property/foo)
    [{:db/ident :logseq.property/foo
      :block/title "Foo"
      :logseq.property/type :default
      :logseq.property/cardinality :one}]))
```

---

## Developer Guide

### Working with Properties

**Query Properties:**

```clojure
;; Get property entity
(d/entity db :logseq.property/status)

;; Get property value from block
(:logseq.property/status block)

;; Query blocks by property value
'[:find ?b
  :where
  [?b :logseq.property/status "pending"]]
```

**Update Properties:**

```clojure
;; Set property value
(d/transact! conn
  [[:db/add block-id :logseq.property/status "done"]])

;; Multi-value property (cardinality :many)
(d/transact! conn
  [[:db/add block-id :block/tags tag-id-1]
   [:db/add block-id :block/tags tag-id-2]])
```

### Creating Custom Properties

**Via Plugin API:**

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
(require '[logseq.db.frontend.malli-schema :as schema])

;; Validate single entity
(schema/validate-entity db entity)

;; Validate transaction
(schema/validate-tx-data db tx-data)
```

---

## Next Steps

**Continue Learning:**

1. **[Database Properties Guide](../../tutorials/development/db-properties.md)** - Plugin API for properties
2. **[Complete Schema Reference](../../../dz/docs/03-features/database-graphs/schema-reference.md)** - Full property/class list
3. **[Query System](../../reference/advanced/query-system.md)** - Datalog queries
4. **[Schema Migration Guide](../../../dz/docs/08-processes/schema-migration-best-practices.md)** - For contributors

**For Contributors:**

- [Schema Change PR Checklist](../../../dz/docs/08-processes/schema-change-pr-checklist.md)
- [Schema Integrity Reports](../../../dz/docs/07-investigations/schema-integrity-08-index.md)
- [RTC Sync Schema Changes](../../../dz/docs/08-processes/rtc-sync-schema-changes.md)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
