---
title: 'Data Architecture'
description: 'Understanding how Logseq stores and manages knowledge graph data'
status: stable
lastVerified: '2026-05-30'
logseqVersion: '0.11.x'
category: architecture
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
  explanation:
    - '[File vs DB Graphs](../concepts/file-vs-db.md)'
    - '[System Overview](system-overview.md)'
---

# Data Architecture

> **Last revised:** 2026-05-30

> Understanding how Logseq structures, stores, and queries your knowledge.

## Overview

Logseq's data architecture is built on a **graph-based model** where everything is an entity with attributes and relationships. This enables powerful querying and natural representation of knowledge connections.

---

## Core Concepts

### Everything is an Entity

In Logseq, all data is represented as entities:

| Entity Type  | Purpose              | Key Attributes            |
| ------------ | -------------------- | ------------------------- |
| **Page**     | Container for blocks | name, title, tags         |
| **Block**    | Unit of content      | content, parent, children |
| **Property** | Metadata field       | type, cardinality, schema |
| **Class**    | Category/tag         | extends, properties       |
| **Asset**    | Attached file        | asset/type, asset/size, asset/checksum |

### Entity-Attribute-Value (EAV)

Data is stored as EAV triples, enabling flexible queries:

```
[entity-id  :attribute      value]
[123        :block/title    "Hello World"]
[123        :block/page     456]
[123        :block/tags     [789, 890]]
```

---

## DataScript: The Query Engine

Logseq uses **DataScript**, an in-memory database that implements Datalog:

```
┌─────────────────────────────────────┐
│           DataScript                │
│  ┌─────────────────────────────┐   │
│  │     In-Memory Database      │   │
│  │  • All entities loaded      │   │
│  │  • Indexed by attributes    │   │
│  │  • Reactive subscriptions   │   │
│  └─────────────────────────────┘   │
│                                     │
│  Query Engine (Datalog)             │
│  • Pattern matching                 │
│  • Joins via references             │
│  • Aggregations                     │
└─────────────────────────────────────┘
```

### Example Query

```clojure
;; Find all tasks due this week
[:find (pull ?b [*])
 :where
 [?b :block/tags ?t]
 [?t :db/ident :logseq.class/Task]
 [?b :logseq.property/deadline ?d]
 [(<= ?d :end-of-week)]]
```

---

## Block Model

### Block Structure

Every block has:

```clojure
{:db/id 123
 :block/uuid #uuid "..."
 :block/title "Block content here"
 :block/parent {:db/id 122}      ; Parent block/page
 :block/page {:db/id 100}        ; Containing page
 :block/order "a0"               ; Sibling order
 :block/refs [{:db/id 200}]      ; Outgoing links
 :block/created-at 1706544000000
 :block/updated-at 1706544000000}
```

### Block Hierarchy

```
Page
├── Block 1
│   ├── Block 1.1
│   └── Block 1.2
├── Block 2
└── Block 3
    └── Block 3.1
```

Each block references its parent, forming a tree structure under each page.

---

## Property System

### Property Definition

Properties are first-class entities in database graphs:

```clojure
{:db/ident :user.property/priority
 :block/title "Priority"
 :block/tags [:logseq.class/Property]
 :logseq.property/type :default  ; or :number, :date, etc.
 :db/cardinality :db.cardinality/one}
```

### Property Types

| Type        | Storage          | Examples       |
| ----------- | ---------------- | -------------- |
| `:default`  | Text reference   | "High", "Low"  |
| `:number`   | Numeric value    | 42, 3.14       |
| `:date`     | Journal page ref | [[2026-01-29]] |
| `:datetime` | Timestamp        | 1706544000000  |
| `:checkbox` | Boolean          | true, false    |
| `:url`      | URL string       | "https://..."  |
| `:node`     | Page/block ref   | [[Page Name]]  |
| `:asset`    | Asset node ref   | Uploaded file block |

### Closed Values

Properties can have predefined options:

```clojure
;; :logseq.property/status has these closed values:
;; Each is a separate entity linked via :block/closed-value-property
[{:db/ident :logseq.property/status.backlog  :block/title "Backlog"}
 {:db/ident :logseq.property/status.todo     :block/title "Todo"}
 {:db/ident :logseq.property/status.doing    :block/title "Doing"}
 {:db/ident :logseq.property/status.in-review :block/title "In Review"}
 {:db/ident :logseq.property/status.done     :block/title "Done"}
 {:db/ident :logseq.property/status.canceled :block/title "Canceled"}]
```

Closed value entities reference back to their property via `:block/closed-value-property`.

---

## Class System

### Class Definition

Classes categorize entities and define schemas:

```clojure
{:db/ident :logseq.class/Task
 :block/title "Task"
 :block/tags [:logseq.class/Tag]
 ;; schema properties defined during graph creation:
 ;; :logseq.property/status, :logseq.property/priority,
 ;; :logseq.property/deadline, :logseq.property/scheduled
 }
```

Note: User-created classes automatically extend `:logseq.class/Root` via `:logseq.property.class/extends`. Built-in classes like Task do not set an explicit extends.

### Inheritance

Classes can extend other classes:

```
Built-in classes (all are tagged :logseq.class/Tag):
  Root            (top-level, user classes extend this by default)
  Tag
  Property
  Page
  ├── Journal     (extends Page)
  └── Whiteboard  (extends Page, deprecated)
  Task
  Comments
  Comment
  Query
  ├── Cards       (extends Query)
  Card
  Asset
  Template
  Code-block, Quote-block, Math-block
  Pdf-annotation
```

---

## References

### Types of References

| Reference Type  | Syntax                | Creates Backlink |
| --------------- | --------------------- | ---------------- |
| Page link       | `[[Page Name]]`       | ✅ Yes           |
| Block reference | `((block-uuid))`      | ✅ Yes           |
| Block embed     | `{{embed ((uuid))}}`  | ✅ Yes           |
| Property ref    | `property:: [[Page]]` | ✅ Yes           |
| Tag             | `#[[Tag]]`            | ✅ Yes           |

### Backlinks

Every reference creates a backlink:

```
Page A: "See [[Page B]] for details"

Page B's backlinks:
  - Page A: "See [[Page B]] for details"
```

---

## Storage Layers

### File-Based Storage

```
graph/
├── pages/
│   └── page-name.md
├── journals/
│   └── 2026_01_29.md
├── assets/
└── logseq/
    └── config.edn
```

Files are parsed on load, building the in-memory database.

### Database Storage

```
<db-graph-storage>/
├── db.sqlite        # Main database
├── db.sqlite-wal    # Write-ahead log
├── search.sqlite    # Search index database
└── assets/
```

SQLite provides ACID transactions and persistence.

For database graphs, runtime state is still held in a DataScript connection in the db worker, with SQLite as the durable backing store.

### Storage Flow

```
User Edit
    │
    ▼
DataScript Transaction
    │
    ├──► File-Based: Write to .md file
    │
    └──► Database: Write to SQLite
```

---

## Indexing

### Indexed Attributes

Schema-defined indexes and identities (from `logseq.db.frontend.schema/schema`):

| Attribute          | Index Type | Purpose               |
| ------------------ | ---------- | --------------------- |
| `:block/uuid`      | Unique     | Fast UUID lookup      |
| `:block/name`      | Index      | Page name search (lowercase, not unique) |
| `:block/title`     | Index      | Original page name    |
| `:block/parent`    | Ref+Index  | Tree traversal        |
| `:block/page`      | Ref+Index  | Page membership       |
| `:block/link`      | Ref+Index  | Tag/embed links       |
| `:block/alias`     | Ref+Index  | Alias resolution      |
| `:block/order`     | Index      | Sibling ordering      |
| `:block/journal-day` | Index    | Journal date queries  |
| `:block/created-at`| Index      | Time-based queries    |
| `:block/updated-at`| Index      | Time-based queries    |
| `:db/ident`        | Unique     | Property/class lookup |
| `:file/path`       | Unique     | File lookup (file graphs) |

> **Removed:** `:block/refs` and `:block/tags` were previously listed as indexed attributes here; they are cardinality-many refs in schema but not marked with `:db/index true`.

### Query Performance

- **Indexed lookups**: O(log n)
- **Full scans**: O(n) - avoid on large graphs
- **Reference traversal**: O(1) per hop

---

## Transactions

### Transaction Format

```clojure
;; Add or update entities
[{:block/uuid #uuid "..."
  :block/title "New content"}]

;; Retract values
[[:db/retract entity-id :attribute value]]

;; Retract entity
[[:db/retractEntity entity-id]]
```

### Transaction Metadata

```clojure
;; Transactions are submitted with metadata:
{:outliner-op :save-block}  ; or :insert-blocks, :delete-blocks, :move-blocks, etc.
```

---

## Undo/Redo

Undo/redo history is recorded as structured history ops that include db transaction metadata (`:outliner-op` and optional db-sync tx identifiers), then replayed through worker history handlers.

```
Undo Stack          Current State        Redo Stack
[tx1, tx2, tx3]  →  Database State  ←   [tx4]
```

This is implemented as operation-history replay rather than a simple blanket "invert every tx datom" rule.

---

## Data Validation

### Malli Schema

Database graphs validate entities using Malli schemas defined in `logseq.db.frontend.malli-schema`. Validation covers entity structure, property values, and `:db/ident` namespace correctness.

```clojure
;; Example: block order must be a valid fractional index string
;; Property values are validated per property type
;; :db/ident namespaces must follow naming conventions
```

### Validation Points

1. **Per transaction (default path)**: DB writes validate tx reports unless explicitly skipped via tx-meta flags (for migrations/initialization/replay paths).
2. **Manual full-graph validation**: Available through worker API (`:thread-api/validate-db`) and developer tooling.
3. **Import checks**: Import flows run explicit validation checks and report/fix issues where applicable.

> **Removed:** "Automatic full-graph validation on graph open/import/sync" is not a single always-on lifecycle hook; validation is split across tx-time checks, explicit validation APIs, and import-specific checks.

---

## Performance Considerations

### Memory Usage

- All data loaded in memory (DataScript)
- Large graphs: 100k+ blocks = 100s MB RAM
- Assets not loaded in memory

### Optimization Strategies

1. **Batch transactions**: Group related changes (`batch-transact!`, temp-conn batch paths)
2. **Indexed queries**: Prefer schema-indexed attributes for hot paths
3. **Worker offloading**: Keep heavy DB operations and sync logic in db worker
4. **Search/vector indexes**: Dedicated search DB and vector index pipeline for retrieval workflows

> **Removed:** "Lazy loading only visible content" and generic "query memoization" claims were removed from this page because they are not represented as current core architecture guarantees in the source of truth.

---

## Findings: New Capabilities

- Added built-in property type `:asset` for asset-node references in DB graphs.
- Built-in class set now includes `:logseq.class/Comments` and `:logseq.class/Comment`.
- DB runtime includes dedicated search-index progress/build flow and vector index integration in the db worker.
- Validation/fix tooling in worker DB paths includes targeted repair routines (for example schema/cardinality and invalid-entity cleanup) exposed via validation APIs.

---

## See Also

- **Schema Details**: [schema-overview.md](../../reference/database/schema-overview.md)
- **File vs DB**: [file-vs-db.md](../concepts/file-vs-db.md)
- **Query System**: [query-dsl.md](../../reference/syntax/query-dsl.md)

---

_Last updated: 2026-05-30_
