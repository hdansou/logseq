---
title: 'File vs Database Graphs'
description: 'Understanding the differences between file-based and database-backed graphs in Logseq'
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
category: concepts
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
  explanation:
    - '[RTC vs File Sync](../comparisons/rtc-vs-file-sync.md)'
---

# File vs Database Graphs

> **Last revised:** 2026-03-10

> Understanding the two storage architectures in Logseq and choosing the right one for your needs.

## Overview

Logseq supports two fundamentally different ways to store your knowledge:

| Aspect               | File-Based         | Database-Based      |
| -------------------- | ------------------ | ------------------- |
| **Storage**          | Markdown/Org files | SQLite database     |
| **Introduced**       | Original (v0.0.1)  | Modern (v0.10.0)    |
| **Prefix**           | `logseq_local_`    | `logseq_db_`        |
| **External editing** | ✅ Yes             | ❌ No               |
| **Typed properties** | ❌ Text only       | ✅ Full type system |

---

## File-Based Graphs

### How It Works

Your knowledge is stored as plain text files on disk:

```
my-graph/
├── pages/
│   ├── project-ideas.md
│   └── reading-list.md
├── journals/
│   ├── 2026-01-29.md
│   └── 2026-01-28.md
├── assets/
│   └── image.png
└── logseq/
    └── config.edn
```

Each page is a separate `.md` file with human-readable content:

```markdown
- This is a block
  - This is a child block
- Another block with a [[link]]
- Block with properties
  property:: value
```

### Advantages

✅ **Portable**: Files work anywhere Markdown works  
✅ **External editors**: Use VS Code, Vim, or any text editor  
✅ **Git-friendly**: Version control with standard tools  
✅ **Human-readable**: Open files with any application  
✅ **Simple backup**: Just copy the folder

### Limitations

❌ **Text-only properties**: No dates, numbers, or references  
❌ **Limited scalability**: Thousands of files can be slow  
❌ **Conflict-prone sync**: File-level conflicts in multi-device use  
❌ **No real-time collaboration**: Only periodic file sync  
❌ **Parser dependency**: Content must be re-parsed on load

---

## Database Graphs

### How It Works

Your knowledge is stored in a SQLite database:

```
logseq_db_my-graph/
├── db.sqlite          # Main database (WAL mode enabled at runtime)
└── assets/
    └── image.png
```

Data is stored as entities with typed attributes:

```clojure
{:db/id 123
 :block/uuid #uuid "..."
 :block/title "This is a block"
 :block/parent {:db/id 122}
 :block/tags [:logseq.class/Task]
 :logseq.property/status :logseq.property/status.todo
 :logseq.property/deadline #inst "2026-02-15"}
```

### Advantages

✅ **Typed properties**: Dates, numbers, checkboxes, references  
✅ **Built-in classes**: Task, Card, Asset with behaviors  
✅ **Unlimited scale**: Handle millions of blocks  
✅ **Real-time sync**: RTC with automatic conflict resolution  
✅ **E2EE**: End-to-end encryption available  
✅ **Better queries**: Type-aware filtering and sorting

### Limitations

❌ **Logseq-only**: Cannot edit with external tools  
❌ **Less portable**: Requires export to move data  
❌ **Newer**: Less battle-tested than file-based  
❌ **Learning curve**: More concepts (classes, property types)

---

## Feature Comparison

### Properties

| Feature             | File                 | Database            |
| ------------------- | -------------------- | ------------------- |
| **Text**            | ✅                   | ✅                  |
| **Numbers**         | ❌ (text)            | ✅ (actual numbers) |
| **Dates**           | ❌ (text)            | ✅ (date picker)    |
| **Checkboxes**      | ❌                   | ✅                  |
| **Select options**  | ❌                   | ✅ (closed values)  |
| **Page references** | ✅ (text)            | ✅ (typed)          |
| **Multi-value**     | ⚠️ (comma-separated) | ✅ (native)         |

### Classes & Tags

| Feature                 | File      | Database                |
| ----------------------- | --------- | ----------------------- |
| **Tags**                | ✅ `#tag` | ✅ `#[[Class]]`         |
| **Inheritance**         | ❌        | ✅ Classes can extend   |
| **Required properties** | ❌        | ✅ Schema enforcement   |
| **Built-in classes**    | ❌        | ✅ Task, Card, Asset... |

### Queries

| Feature                 | File             | Database          |
| ----------------------- | ---------------- | ----------------- |
| **Basic queries**       | ✅               | ✅                |
| **Property filtering**  | ⚠️ Text matching | ✅ Type-aware     |
| **Date ranges**         | ⚠️ `:between`    | ✅ `:between`     |
| **Numeric comparisons** | ❌               | ✅ `>`, `<`, `>=` |
| **Class queries**       | ❌               | ✅ Tag-based      |

---

## Query Syntax Differences

### File-Based Query

```clojure
#+BEGIN_QUERY
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/content ?c]
         [(clojure.string/includes? ?c "priority:: high")]]}
#+END_QUERY
```

### Database Query

```clojure
#+BEGIN_QUERY
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/tags ?t]
         [?t :db/ident :logseq.class/Task]
         [?b :logseq.property/priority ?p]
         [?p :db/ident :logseq.property/priority.high]]}
#+END_QUERY
```

---

## When to Choose Which

### Choose File-Based When:

- You want to edit with external tools
- You use Git for version control
- You need maximum portability
- You prefer plain text simplicity
- You're migrating from another Markdown app

### Choose Database When:

- You need typed properties (dates, numbers)
- Multiple people collaborate on the graph
- You have large amounts of data
- You want built-in task management
- You need end-to-end encryption

---

## Migration

### File → Database

Logseq provides a built-in import feature (`frontend.components.imports/import-file-graph`) that handles the conversion:

1. Use the import dialog in the app (or CLI)
2. Select the file-based graph folder
3. Configure tag conversion options (e.g., convert all tags to classes)
4. Review converted properties

**Notes**:

- The exporter (`logseq.graph-parser.exporter`) handles property type inference
- Priority values are mapped (e.g., "A" → `:logseq.property/priority.high`)
- Some formatting may change
- Block UUIDs are regenerated

### Database → File

1. Export as Markdown
2. Create new file-based graph
3. Copy exported files

**Notes**:

- Typed properties become text
- Class metadata may be lost
- Property types not preserved

---

## Schema Differences

### File-Based Schema

```clojure
;; Properties stored as keyword-value maps (values are strings or sets)
{:block/properties {:priority "high"
                    :due "2026-02-15"}
 :block/properties-text-values {:priority "high"
                                :due "2026-02-15"}}

;; Tags as page entity references
{:block/tags [{:db/id 45}]}  ; references to page entities
```

### Database Schema

```clojure
;; Properties as entity references
{:logseq.property/priority {:db/ident :logseq.property/priority.high}
 :logseq.property/deadline #inst "2026-02-15T00:00:00"}

;; Tags as class references
{:block/tags [{:db/ident :logseq.class/Task}
              {:db/ident :user.class/Project}]}
```

---

## Identifying Graph Type

In code (from `logseq.db.sqlite.util`):

```clojure
(defn db-based-graph?
  [graph-name]
  (when graph-name
    (string/starts-with? graph-name db-version-prefix)))
;; where db-version-prefix = "logseq_db_"
```

File graphs use the prefix `logseq_local_` (defined as `file-version-prefix` in `logseq.common.config`).

In the UI:

- Database graphs are visually distinguished from file graphs
- Settings page shows graph type

---

## See Also

- **Database Schema**: [schema-overview.md](../../reference/database/schema-overview.md)
- **Sync Comparison**: [rtc-vs-file-sync.md](../comparisons/rtc-vs-file-sync.md)
- **Query DSL**: [query-dsl.md](../../reference/syntax/query-dsl.md)

---

_Last updated: 2026-03-10_
