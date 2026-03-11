---
title: 'Database Graph Schema Reference'
description: "Complete reference for Logseq's database graph schema, property types, and built-in classes"
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
platforms: [desktop, mobile]
graphType: [db]
sinceVersion: '0.10.0'
relatedDocs:
  explanation:
    - '[File vs DB Graphs](../../explanation/concepts/file-vs-db.md)'
  reference:
    - '[Property Types](property-types.md)'
    - '[Query DSL](../syntax/query-dsl.md)'
---

# Database Graph Schema Reference

> **Last revised:** 2026-03-10

> Complete reference for the database graph schema architecture, including entity types, property system, and built-in classes.

## Overview

Database graphs use a structured schema with three layers:

1. **DataScript Layer**: In-memory database with Datalog querying
2. **Malli Validation Layer**: Schema validation for data integrity
3. **SQLite Persistence Layer**: Long-term storage

**Current Schema Version**: 65.22

---

## Quick Reference

| Concept             | Count | Description                 |
| ------------------- | ----- | --------------------------- |
| Built-in Properties | ~87   | Core metadata system        |
| Built-in Classes    | 16    | Entity classification       |
| Property Types      | 18    | Value type definitions      |
| Entity Types        | 10+   | Pages, blocks, assets, etc. |

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
│  Transactions, snapshots, RTC sync  │
└─────────────────────────────────────┘
```

---

## Entity Types

Database graphs contain these entity types:

| Entity Type              | Description              | Key Attributes                                 |
| ------------------------ | ------------------------ | ---------------------------------------------- |
| **Normal Page**          | Standard content pages   | `:block/uuid`, `:block/name`, `:block/title`   |
| **Class Page**           | Tag/class definitions    | `:db/ident`, `:logseq.property.class/extends`  |
| **Property Page**        | Property definitions     | `:db/ident`, `:logseq.property/type`           |
| **Journal Page**         | Daily notes              | `:block/journal-day`                           |
| **Whiteboard Page**      | Canvas pages             | Uses tldraw format                             |
| **Normal Block**         | Content blocks           | `:block/parent`, `:block/order`, `:block/page` |
| **Property Value Block** | Stores property values   | `:logseq.property/value`                       |
| **Closed Value Block**   | Predefined choice values | `:block/closed-value-property`                 |
| **Asset Block**          | Files and images         | `:logseq.property.asset/type`                  |
| **Hidden Page**          | Internal pages           | Not user-visible                               |

---

## Property Types

Properties can store different types of values:

### User Property Types

Available when creating custom properties:

| Type        | Description            | Example               |
| ----------- | ---------------------- | --------------------- |
| `:default`  | Text value             | "Hello World"         |
| `:number`   | Numeric value          | 42                    |
| `:date`     | Journal date reference | [[2026-01-29]]        |
| `:datetime` | Unix timestamp         | 1706544000000         |
| `:checkbox` | Boolean                | true/false            |
| `:url`      | Web link               | "https://example.com" |
| `:node`     | Page/block reference   | [[Page Name]]         |

### Internal Property Types

Used by built-in properties only:

| Type          | Description        | Used By             |
| ------------- | ------------------ | ------------------- |
| `:keyword`    | Clojure keyword    | System properties   |
| `:map`        | Key-value map      | View configurations |
| `:coll`       | Collection         | Internal lists      |
| `:any`        | Any type           | Flexible storage    |
| `:entity`     | Entity reference   | Parent/page refs    |
| `:class`      | Class reference    | Tag assignments     |
| `:page`       | Page reference     | Page links          |
| `:property`   | Property reference | Meta-properties     |
| `:string`     | Raw string         | Internal text       |
| `:json`       | JSON data          | Structured data     |
| `:raw-number` | Raw number         | Internal counts     |

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

---

## Built-in Classes

Classes (tags) provide object-oriented classification with inheritance.

### Core Classes

| Class                    | Extends | Purpose              |
| ------------------------ | ------- | -------------------- |
| `:logseq.class/Root`     | -       | Top-level ancestor   |
| `:logseq.class/Tag`      | Root    | User-created tags    |
| `:logseq.class/Property` | Root    | Property definitions |
| `:logseq.class/Page`     | Root    | Normal pages         |

### Page Classes

| Class                      | Extends | Purpose      |
| -------------------------- | ------- | ------------ |
| `:logseq.class/Journal`    | Page    | Daily notes  |
| `:logseq.class/Whiteboard` | Page    | Canvas pages (deprecated) |

### Block Classes

| Class                           | Schema Properties                     | Purpose             |
| ------------------------------- | ------------------------------------- | ------------------- |
| `:logseq.class/Task`            | status, priority, deadline, scheduled | Task management     |
| `:logseq.class/Query`           | query                                 | Executable queries  |
| `:logseq.class/Card`            | fsrs/state, fsrs/due                  | Flashcards          |
| `:logseq.class/Cards`           | (extends Query)                       | Flashcard decks     |
| `:logseq.class/Asset`           | type, size, checksum                  | Files/images        |
| `:logseq.class/Code-block`      | display-type, code/lang               | Code blocks         |
| `:logseq.class/Quote-block`     | display-type                          | Block quotes        |
| `:logseq.class/Math-block`      | display-type                          | Math expressions    |
| `:logseq.class/Pdf-annotation`  | hl-color, hl-page, hl-value, etc.    | PDF annotations     |
| `:logseq.class/Template`        | template-applied-to                   | Block templates     |

### Class Hierarchy

```
Root
├── Tag (user-created tags inherit from here)
├── Property (custom properties inherit from here)
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
└── Template
```

### Class Inheritance

Classes support multiple inheritance via `:logseq.property.class/extends`:

```clojure
;; User-defined class extending Page
{:db/ident :user.class/Project
 :block/title "Project"
 :logseq.property.class/extends [:logseq.class/Page]}
```

---

## Key Built-in Properties

### Core Properties

| Property             | Type    | Cardinality | Purpose                 |
| -------------------- | ------- | ----------- | ----------------------- |
| `:block/uuid`        | uuid    | one         | Unique identifier       |
| `:block/title`       | string  | one         | Content/name            |
| `:block/name`        | string  | one         | Lowercase name (pages)  |
| `:block/tags`        | class   | many        | Class assignments       |
| `:block/alias`       | page    | many        | Page aliases            |
| `:block/parent`      | entity  | one         | Parent block/page       |
| `:block/page`        | entity  | one         | Containing page         |
| `:block/order`       | string  | one         | Sibling order           |
| `:block/refs`        | entity  | many        | Outgoing links          |
| `:block/collapsed?`  | boolean | one         | Collapse state          |
| `:block/created-at`  | datetime| one         | Creation timestamp      |
| `:block/updated-at`  | datetime| one         | Last update timestamp   |
| `:block/journal-day` | raw-number | one      | Journal date (YYYYMMDD) |

### Task Properties

| Property                     | Type    | Description                 |
| ---------------------------- | ------- | --------------------------- |
| `:logseq.property/status`    | default | Task status (closed values) |
| `:logseq.property/priority`  | default  | Low/Medium/High/Urgent      |
| `:logseq.property/deadline`  | datetime | Due date                    |
| `:logseq.property/scheduled` | datetime | Scheduled date              |

**Status Closed Values**:

- `Backlog` - Not yet started
- `Todo` - To be done (default)
- `Doing` - In progress
- `In Review` - Under review
- `Done` - Completed
- `Canceled` - Not doing

### View Properties

| Property                                  | Type    | Description                      |
| ----------------------------------------- | ------- | -------------------------------- |
| `:logseq.property.view/type`              | default | View type (table, list, gallery) |
| `:logseq.property.table/sorting`          | coll    | Column sort configuration        |
| `:logseq.property.table/filters`          | map     | Active filters                   |
| `:logseq.property.table/ordered-columns`  | coll    | Column ordering                  |
| `:logseq.property.table/hidden-columns`   | keyword | Hidden columns (many)            |
| `:logseq.property.table/sized-columns`    | map     | Column size settings             |
| `:logseq.property.table/pinned-columns`   | property| Pinned columns (many)            |

### Asset Properties

| Property                          | Type       | Description        |
| --------------------------------- | ---------- | ------------------ |
| `:logseq.property.asset/type`     | string     | File MIME type     |
| `:logseq.property.asset/size`     | raw-number | File size in bytes |
| `:logseq.property.asset/checksum` | string     | File hash          |

### FSRS (Flashcard) Properties

| Property                           | Type     | Description      |
| ---------------------------------- | -------- | ---------------- |
| `:logseq.property.fsrs/state`      | map      | Learning state   |
| `:logseq.property.fsrs/due`        | datetime | Next review time |

---

## Property Configuration

Each property has a configuration structure:

```clojure
{:title "Property Name"
 :attribute :block/attribute  ; DataScript attribute (optional)
 :schema {:type :default      ; Property type
          :cardinality :one   ; :one or :many
          :public? true       ; User-visible
          :hide? false        ; Hidden from UI
          :view-context :block ; Where shown
          :ui-position :properties ; UI location
          :classes #{...}}    ; Restrict to specific classes (optional)
 :closed-values [...]         ; Predefined choices (optional)
 :queryable? true             ; Available in query builder
 :properties {...}            ; Meta-properties
 :rtc {...}}                  ; RTC sync configuration (optional)
```

### View Contexts

Where a property can appear:

| Context     | Description            |
| ----------- | ---------------------- |
| `:page`     | Only on pages          |
| `:block`    | Only on blocks         |
| `:class`    | Only on class pages    |
| `:property` | Only on property pages |
| `:never`    | Never shown            |

### UI Positions

Where property is displayed:

| Position       | Description              |
| -------------- | ------------------------ |
| `:properties`  | Standard properties area |
| `:block-left`  | Left of block content    |
| `:block-right` | Right of block content   |
| `:block-below` | Below block content      |

---

## DataScript Schema

The DataScript schema defines attribute constraints:

### Attribute Types

| Attribute                    | Value Type | Cardinality | Notes           |
| ---------------------------- | ---------- | ----------- | --------------- |
| `:block/uuid`                | identity   | one         | Unique lookup   |
| `:block/name`                | indexed    | one         | Fast lookup     |
| `:block/title`               | indexed    | one         | Content         |
| `:block/parent`              | ref        | one         | Parent entity   |
| `:block/page`                | ref        | one         | Containing page |
| `:block/tags`                | ref        | many        | Classes         |
| `:block/refs`                | ref        | many        | Outgoing links  |
| `:block/link`                | ref        | one         | Tag/embed link  |
| `:block/alias`               | ref        | many        | Page aliases    |
| `:block/closed-value-property` | ref      | many        | Closed value owner |
| `:block/order`               | indexed    | one         | Sibling order   |
| `:block/created-at`          | indexed    | one         | Creation time   |
| `:block/updated-at`          | indexed    | one         | Update time     |
| `:block/journal-day`         | indexed    | one         | Journal date    |
| `:db/ident`                  | identity   | one         | Unique keyword  |

### Reference vs Scalar

- **Reference types** (`:db.type/ref`): Store entity IDs, enable joins
- **Scalar types** (no `:db/valueType`): Store values directly

---

## Querying the Schema

### Find All Properties

```clojure
[:find (pull ?p [*])
 :where [?p :logseq.property/type]]
```

### Find All Classes

```clojure
[:find (pull ?c [*])
 :where [?c :logseq.property.class/extends]]
```

### Find Entities by Class

```clojure
[:find (pull ?b [*])
 :where [?b :block/tags ?t]
        [?t :db/ident :logseq.class/Task]]
```

### Find Property Values

```clojure
[:find ?v
 :where [?b :block/uuid ?uuid]
        [?b :logseq.property/status ?v-ent]
        [?v-ent :block/title ?v]]
```

---

## Schema Migrations

Schema versions follow major.minor format (e.g., 65.22).

Migrations are defined in:

- `src/main/frontend/worker/db/migrate.cljs`

When adding properties to `built-in-properties`, add corresponding migration.

---

## See Also

- **Property Types**: [property-types.md](property-types.md) for detailed type documentation
- **Query DSL**: [query-dsl.md](../syntax/query-dsl.md) for querying data
- **File vs DB**: [file-vs-db.md](../../explanation/concepts/file-vs-db.md) for comparison

---

_Last updated: 2026-03-10 | [Report an issue](https://github.com/logseq/logseq/issues)_
