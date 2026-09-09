---
title: 'Data Architecture'
description: 'Understanding how Logseq stores and manages knowledge graph data'
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
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

> **Last revised:** 2026-09-09 (audited against `be800f1711`)

> **Graph model:** unless a section says otherwise, this page describes **DB graphs**
> (SQLite-backed). Sections that describe the legacy **file graph** model are marked.

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
| **Asset**    | Attached file        | `:logseq.property.asset/type`, `/size`, `/checksum` |

Source: `deps/db/src/logseq/db/frontend/class.cljs:68-74` (`:logseq.class/Asset` declares these three as
both `:properties` and `:required-properties`).

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

These are exactly `logseq.db.frontend.property.type/user-built-in-property-types`
(`deps/db/src/logseq/db/frontend/property/type.cljs:19-21`), listed there in UI order.
A separate `internal-built-in-property-types` set —
`#{:keyword :map :coll :any :entity :class :page :property :string :json :raw-number}` —
is reserved for built-in properties (`type.cljs:15-17`); of those, only `:map`, `:json` and
`:string` may be stored by users, and only via EDN or the API (`type.cljs:23-26`).

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

Closed value entities reference back to their property via `:block/closed-value-property`
(`deps/db/src/logseq/db/frontend/schema.cljs:99-101`, a cardinality-many ref). The six status
idents and titles above are exactly those declared at
`deps/db/src/logseq/db/frontend/property.cljs:313-318`; the property's own type is `:default`
with `:logseq.property/default-value :logseq.property/status.todo`
(`property.cljs:300-322`).

---

## Class System

### Class Definition

Classes categorize entities and define schemas:

```clojure
{:db/ident :logseq.class/Task
 :block/title "Task"
 :block/tags #{:logseq.class/Tag}
 ;; declared schema properties become :logseq.property.class/properties at graph creation
 :logseq.property.class/properties [:logseq.property/status :logseq.property/priority
                                    :logseq.property/deadline :logseq.property/scheduled]
 :logseq.property.class/extends :logseq.class/Root}
```

Source: `deps/db/src/logseq/db/frontend/class.cljs:40-42` (Task's declared schema properties) and
`deps/db/src/logseq/db/sqlite/create_graph.cljs:145-167` (`build-initial-classes*`, which turns
`:schema {:properties [...]}` into `:logseq.property.class/properties`).

Note: **every** class — user-created and built-in alike — is created through
`sqlite-util/build-new-class`, which tags it `:logseq.class/Tag` and, unless it is
`:logseq.class/Root` itself or already declares an explicit extends, sets
`:logseq.property.class/extends :logseq.class/Root`
(`deps/db/src/logseq/db/sqlite/util.cljs:91-100`).

### Inheritance

Classes can extend other classes:

```
Built-in classes (all are tagged :logseq.class/Tag; source: class.cljs:17-101):
  Root            (top-level; every other class extends this unless it declares its own extends)
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

| Reference Type  | Syntax                | Stored as                                  |
| --------------- | --------------------- | ------------------------------------------ |
| Page link       | `[[Page Name]]`       | `:block/refs` (cardinality-many ref)       |
| Block reference | `((block-uuid))`      | `:block/refs`                              |
| Tag             | `#Tag` / `#[[Tag]]`   | `:block/tags` (and `:block/refs`)          |
| Property value  | property of type `:node` / `:date` / `:asset` | a ref stored under the property's own `:db/ident` |

All ref attributes are declared in `deps/db/src/logseq/db/frontend/schema.cljs:69-83`
(`:block/refs`, `:block/tags`, `:block/link`, `:block/alias`).

> **Removed:** the previous table listed `{{embed ((uuid))}}` and `property:: [[Page]]` as
> reference *types*. `property:: value` is **file-graph markdown syntax**; DB graphs store property
> values as datoms keyed by the property's `:db/ident`, and there is no `:block/properties` attribute
> in the DB-graph datascript schema (`schema.cljs:57-106`). `{{embed ...}}` is a macro rendered at
> display time, not a distinct reference kind in the schema.
### Backlinks

Every reference creates a backlink:

```
Page A: "See [[Page B]] for details"

Page B's backlinks:
  - Page A: "See [[Page B]] for details"
```

---

## Storage Layers

### File-Based Storage (file graphs only — legacy model)

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
├── db.sqlite        # Main database (datascript.storage kvs table)
├── search-db.sqlite # Search index database
└── assets/
```

Source: `deps/db/src/logseq/db/common/sqlite.cljs:30-33` (`db.sqlite` path) and
`src/main/frontend/worker/db_core.cljs:547-551` (the worker opens `/db.sqlite` and
`/search-db.sqlite`).

The main DB is not a relational block table: `db.sqlite` holds a single
`kvs (addr INTEGER primary key, content TEXT, addresses JSON)` table used as a
`datascript.storage` backend (`deps/db/src/logseq/db/common/sqlite.cljs:11-13`).

For database graphs, runtime state is held in a DataScript connection in the db worker, with
SQLite as the durable backing store.

> **Removed:** `search.sqlite` — the search index file is `search-db.sqlite`
> (`src/main/frontend/worker/db_core.cljs:551`). `db.sqlite-wal` was also removed: no WAL sidecar
> file is named anywhere in the source; the worker's storage layer is the datascript `kvs` table.

### Storage Flow (DB graphs)

```
User Edit (UI thread)
    │
    ▼
ui-outliner-tx/transact!  collects [:op args] pairs
    │   src/main/frontend/modules/outliner/ui.cljc:9-27
    ▼
:thread-api/apply-outliner-ops  (crosses into the db worker)
    │   src/main/frontend/worker/handler/transaction.cljs:94
    ▼
logseq.outliner.op/apply-ops!   (malli-validates the ops, then dispatches)
    │   deps/outliner/src/logseq/outliner/op.cljs:427-450
    ▼
logseq.db/transact!  →  pipeline  →  malli validate  →  commit + store
        deps/db/src/logseq/db.cljs:153-180
```

The outliner core runs **in the db worker**, not on the UI thread; the UI only enqueues ops.

---

## Indexing

### Indexed Attributes

Schema-defined indexes and identities (from `logseq.db.frontend.schema/schema`,
`deps/db/src/logseq/db/frontend/schema.cljs:56-106` — every row below re-verified):

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

`:outliner-op` is attached by `logseq.outliner.core/op-transact!`
(`deps/outliner/src/logseq/outliner/core.cljs:1282-1298`). The set of legal ops is the 30-entry
malli `op-schema` in `deps/outliner/src/logseq/outliner/op.cljs:20-155`; `apply-ops!` asserts
against it before anything is transacted (`op.cljs:427-429`).

Several tx-meta flags change how a transaction is handled — notably `:skip-validate-db?`,
`:initial-db?`, `:rtc-download-graph?`, `:reset-conn!` (skip the whole pipeline) and `:fix-db?`
(run the pipeline but skip malli validation of its result)
(`deps/db/src/logseq/db.cljs:113-130`).

---

## Undo/Redo

Undo/redo history is recorded as structured history ops that include db transaction metadata
(`:outliner-op` and optional db-sync tx identifiers), then replayed through worker history handlers.
The op items are themselves malli-validated: `::db-transact` carries `{:tx-meta {:outliner-op ...}
:added-ids #{} :retracted-ids #{}}` plus optional `:db-sync/tx-id`,
`:db-sync/forward-outliner-ops` and `:db-sync/inverse-outliner-ops`; the other two item kinds are
`::record-editor-info` and `::ui-state`
(`src/main/frontend/worker/undo_redo.cljs:11-63`). Undo/redo is worker-owned
(`docs/adr/0012-worker-owned-undo-redo.md`).

```
Undo Stack          Current State        Redo Stack
[tx1, tx2, tx3]  →  Database State  ←   [tx4]
```

This is implemented as operation-history replay rather than a simple blanket "invert every tx datom" rule.

---

## Data Validation

### Malli Schema

Database graphs validate entities using Malli schemas defined in `logseq.db.frontend.malli-schema`
(`deps/db/src/logseq/db/frontend/malli_schema.cljs`). The top-level schema `DB` is
`[:sequential Data]`, and `Data` is a `:multi` dispatching on `entity-dispatch-key`, which today
routes to 13 branches: `:property`, `:class`, `:hidden`, `:normal-page`, `:reaction-entity`,
`:property-history-block`, `:closed-value-block`, `:property-value-block`, `:block`,
`:asset-block`, `:file-block`, `:db-ident-key-value`, `:property-value-placeholder`
(`malli_schema.cljs:557-613`). Validation covers entity structure, property values, and
`:db/ident` namespace correctness.

```clojure
;; Example: block order must be a valid fractional index string
;; Property values are validated per property type
;; :db/ident namespaces must follow naming conventions
```

### Validation Points

1. **Per transaction (default path)**: `logseq.db/transact-sync` runs `d/with` speculatively,
   pipes the report through the transact pipeline, then calls
   `logseq.db.frontend.validate/validate-tx-report` on the *changed* entity ids. If validation
   fails the write is thrown away (`throw-invalid-tx!`) and never reaches the conn — so the commit
   is atomic with respect to validation. Skipped for the tx-meta flags listed above.
   (`deps/db/src/logseq/db.cljs:113-186`; `deps/db/src/logseq/db/frontend/validate.cljs:24-70`.)
2. **Manual full-graph validation**: `:thread-api/validate-db` →
   `frontend.worker.db.validate/validate-db`, which by default also *repairs*: it fixes the
   `:logseq.property.class/extends` cardinality, wrong-typed icons, non-closed values and
   num-prefixed `:db/ident`s, then loops validate-and-fix over invalid blocks
   (`src/main/frontend/worker/db/validate.cljs:294-327`). Reachable from the developer command
   `:dev/validate-db` (`src/main/frontend/handler/common/developer.cljs:84-85`).
3. **Pre-export validation**: `export-repo-as-db-edn!` invokes `:thread-api/validate-db` and
   throws `"Graph validation failed"` before writing the EDN
   (`src/main/frontend/handler/db_based/export.cljs:80-88`).
4. **Import checks**: `sqlite-export/validate-import-txs` gates `:batch-import-edn`
   (`deps/outliner/src/logseq/outliner/op.cljs:214-231`), and the CLI/importer path uses
   `validate-local-db!` (`deps/db/src/logseq/db/frontend/validate.cljs:126`).

> **Removed:** "Automatic full-graph validation on graph open/import/sync" is not a single always-on lifecycle hook; validation is split across tx-time checks, explicit validation APIs, and import-specific checks.

---

## Performance Considerations

### Memory Usage

- All data loaded in memory (DataScript), in the db worker
- Assets not loaded in memory

> **Unverified:** the figure "100k+ blocks = 100s MB RAM" has no basis in source and no
> benchmark is referenced anywhere in the repo. Treat it as folklore, not a specification.

### Optimization Strategies

1. **Batch transactions**: Group related changes — `logseq.db/batch-transact-with-temp-conn!`
   (`deps/db/src/logseq/db.cljs:262`) and `logseq.db/batch-transact!` (`db.cljs:318`). The
   temp-conn path defers validation for the inner ops (`:skip-validate-db? true`) and validates
   once at the end.
2. **Indexed queries**: Prefer schema-indexed attributes for hot paths
3. **Worker offloading**: Keep heavy DB operations and sync logic in db worker
4. **Search/vector indexes**: Dedicated search DB and vector index pipeline for retrieval workflows

> **Removed:** "Lazy loading only visible content" and generic "query memoization" claims were removed from this page because they are not represented as current core architecture guarantees in the source of truth.

---

## Findings: New Capabilities

- Added built-in property type `:asset` for asset-node references in DB graphs
  (`deps/db/src/logseq/db/frontend/property/type.cljs:19-21`).
- Built-in class set now includes `:logseq.class/Comments` and `:logseq.class/Comment`
  (`deps/db/src/logseq/db/frontend/class.cljs:44-52`), plus `:logseq.class/Card`,
  `:logseq.class/Cards`, `:logseq.class/Template` and the block-kind classes
  `Code-block`, `Quote-block`, `Math-block`, `Pdf-annotation`.
- **Vector / semantic search index.** The db worker opens a per-graph vector index at
  `search/vector` alongside the FTS index, with a 384-dimension embedding
  (`src/main/frontend/worker/search.cljs:151`), a 1024-row upsert batch, and result gating at
  `max-vector-search-results 10` / `min-vector-search-score 0.5` (`search.cljs:17-19`).
  Lifecycle lives in `src/main/frontend/worker/db_core.cljs:490-583, 731-739`.
- **Reactions are first-class entities.** `:logseq.property.reaction/target` /
  `/emoji-id` back a `:toggle-reaction` outliner op
  (`deps/outliner/src/logseq/outliner/op.cljs:154-158, 176-212`) and a dedicated
  `:reaction-entity` malli branch (`malli_schema.cljs:438`).
- **Recycle bin.** `:restore-recycled` and `:recycle-delete-permanently` outliner ops back a
  soft-delete flow via `logseq.outliner.recycle` (`op.cljs:143-151`), with
  `:logseq.property/deleted-at` on the entity (`malli_schema.cljs:284`).
- **Property history.** `:logseq.property/enable-history?` on a property records value changes
  into `:property-history-block` entities (`malli_schema.cljs:450-469`;
  `property.cljs:319-322`).
- **Markdown mirror.** DB graphs can mirror to markdown on disk, toggled per graph
  (`src/main/frontend/persist_db.cljs:216-246`).
- Validation/fix tooling in worker DB paths includes targeted repair routines — extends
  cardinality, wrong-typed icons, non-closed values, num-prefixed `:db/ident`s, and a
  validate-and-fix loop over invalid blocks
  (`src/main/frontend/worker/db/validate.cljs:222-327`).

---

## See Also

- **Schema Details**: [schema-overview.md](../../reference/database/schema-overview.md)
- **File vs DB**: [file-vs-db.md](../concepts/file-vs-db.md)
- **Query System**: [query-dsl.md](../../reference/syntax/query-dsl.md)

---

_Last updated: 2026-09-09_
