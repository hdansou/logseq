# Logseq CLI: EDN Format Reference

> **Last revised:** 2026-03-10

> Comprehensive guide to the EDN (Extensible Data Notation) format used by Logseq CLI for import/export operations

**Version**: 0.4.3
**For**: Developers & Advanced Users

---

## Table of Contents

1. [Introduction](#introduction)
2. [EDN Basics](#edn-basics)
3. [Export Type Structures](#export-type-structures)
4. [Entity Schemas](#entity-schemas)
5. [Common Patterns](#common-patterns)
6. [Examples](#examples)
7. [Validation](#validation)
8. [Best Practices](#best-practices)

---

## Introduction

Logseq CLI uses EDN (Extensible Data Notation) as its primary data interchange format for graph export and import operations. EDN is a subset of Clojure's data literal format, providing a rich, extensible way to represent structured data.

### What is EDN?

EDN is a data format that supports:

- **Primitive types**: strings, numbers, booleans, nil
- **Collections**: vectors `[]`, maps `{}`, sets `#{}`
- **Symbols and keywords**: `:keyword`, `symbol`
- **Tagged literals**: `#uuid "..."`, `#inst "..."`
- **Comments**: `;; comment text`

### Why EDN for Logseq?

- **Human-readable**: Easy to inspect and debug
- **Rich data types**: Native support for UUIDs, dates, sets
- **Lossless**: Preserves all graph structure and metadata
- **Extensible**: Custom tagged literals for Logseq-specific data

---

## EDN Basics

### Primitive Values

```clojure
;; Strings
"Hello, world!"
"Multi\nline\nstring"

;; Numbers
42
3.14159
-17

;; Booleans
true
false

;; Nil (null/none)
nil
```

### Collections

```clojure
;; Vector (ordered collection)
[1 2 3 4 5]
["string" 42 true]

;; Map (key-value pairs)
{:name "Alice"
 :age 30
 :active true}

;; Set (unique values)
#{1 2 3}
#{:tag1 :tag2 :tag3}
```

### Keywords

```clojure
;; Keywords start with :
:keyword
:namespace/keyword
:db/id
:block/uuid
:my.property/priority

;; Used as map keys and identifiers
{:db/ident :logseq.class/Tag
 :block/title "My Page"}
```

### Tagged Literals

```clojure
;; UUID
#uuid "68c012ca-2aee-46a8-a589-8bf67a1d4e92"

;; Instant (timestamp)
#inst "2025-01-15T12:34:56.789Z"

;; DataScript Entity (in results)
#datascript/Entity {:db/id 123}
```

---

## Export Type Structures

### 1. Full Graph Export (`:graph`)

**Default export type** - Contains complete graph data.

```clojure
{:properties [{...} {...}]              ; Property definitions
 :classes [{...} {...}]                 ; Class/tag definitions
 :pages-and-blocks [{...} {...}]        ; All pages and blocks
 :logseq.db.sqlite.export/export-type :graph
 :logseq.db.sqlite.export/schema-version [1 2]
 :logseq.db.sqlite.export/graph-files [{:file/path "..." :file/content "..."}]
 :logseq.db.sqlite.export/kv-values {:logseq.kv/graph-created-at 1704844800000
                                     :logseq.kv/schema-version {:major 65 :minor 7}
                                     :logseq.kv/import-type :cli/create-graph}}
```

**Fields:**

- **`:properties`** - Array of property entity maps
- **`:classes`** - Array of class/tag entity maps
- **`:pages-and-blocks`** - Array of page and block entity maps
- **`:export-type`** - Always `:graph`
- **`:schema-version`** - Export format version `[major minor]`
- **`:graph-files`** - File path mappings (optional, excluded with `-F`)
- **`:kv-values`** - Graph metadata (creation time, schema version, etc.)

---

### 2. Schema-Only Export (`:graph-ontology`)

**Ontology export** - Contains only property and class definitions (no content).

```clojure
{:properties [{...} {...}]              ; User-defined properties
 :classes [{...} {...}]                 ; User-defined classes
 :logseq.db.sqlite.export/export-type :graph-ontology}
```

**Fields:**

- **`:properties`** - User-defined property definitions only (no built-ins)
- **`:classes`** - User-defined class/tag definitions only (no built-ins)
- **`:export-type`** - Always `:graph-ontology`

**Notes:**

- Built-in properties/classes excluded by default
- Use `-e logseq.class -e logseq.property` to exclude more namespaces
- Perfect for sharing graph structure without content

---

## Entity Schemas

### Property Entity

Properties define the schema for attributes used on pages and blocks.

```clojure
{:db/id 1001                                    ; Database ID
 :db/ident :my.property/priority                ; Unique identifier
 :block/uuid #uuid "a1b2c3d4-..."              ; UUID
 :block/title "priority"                        ; Display name
 :block/created-at 1704844800000               ; Creation timestamp (optional)
 :block/updated-at 1704844800000               ; Update timestamp (optional)
 :property/schema.type :number                  ; Value type
 :property/schema.cardinality :one              ; :one or :many
 :property/schema.classes #{:logseq.class/Task} ; Allowed on these classes (optional)
 :property/closed-values #{"low" "high"}        ; Enum values (optional)
 :property/schema.description "Task priority"   ; Description (optional)}
```

**Property Types** (`:property/schema.type`):

- `:string` - Text value
- `:number` - Numeric value
- `:date` - Date value
- `:checkbox` - Boolean value
- `:url` - URL value
- `:page` - Reference to another page
- `:default` - Any value type

**Cardinality** (`:property/schema.cardinality`):

- `:one` - Single value
- `:many` - Multiple values (set)

---

### Class/Tag Entity

Classes (tags) define types of pages and can have properties.

```clojure
{:db/id 2001
 :db/ident :user.class/Project                  ; Unique identifier
 :block/uuid #uuid "b2c3d4e5-..."
 :block/title "Project"                         ; Display name
 :block/created-at 1704844800000
 :block/updated-at 1704844800000
 :logseq.property.class/extends [:logseq.class/Root]            ; Parent classes
 :logseq.property.class/properties [:my.property/priority        ; Properties for this class
                    :my.property/status
                    :my.property/deadline]}
```

**Built-in Classes:**

- `:logseq.class/Root` - Base class for all classes
- `:logseq.class/Tag` - Standard tag class
- `:logseq.class/Page` - Page class
- `:logseq.class/Whiteboard` - Whiteboard page

---

### Page Entity

Pages are the top-level containers in a graph.

```clojure
{:db/id 3001
 :block/uuid #uuid "c3d4e5f6-..."
 :block/title "My Project Page"                 ; Page title
 :block/name "my project page"                  ; Lowercase normalized name
 :block/created-at 1704844800000
 :block/updated-at 1704844800000
 :block/tags #{:user.class/Project}             ; Tags/classes
 :block/collapsed? false                        ; Collapsed state (optional)

 ;; Custom properties (using defined properties)
 :my.property/priority "high"
 :my.property/status "active"
 :my.property/deadline #inst "2025-12-31T00:00:00Z"

 ;; Page icon (optional)
 :logseq.property/icon "📊"

 ;; Page description (optional)
 :logseq.property.page/description "Main project page"}
```

---

### Block Entity

Blocks are nested content units within pages.

```clojure
{:db/id 4001
 :block/uuid #uuid "d4e5f6g7-..."
 :block/title "- Task item #task"               ; Block content
 :block/page {:db/id 3001}                      ; Parent page reference
 :block/parent {:db/id 3001}                    ; Parent block/page reference
 :block/order 0                                 ; Position in parent (0-indexed)
 :block/created-at 1704844800000
 :block/updated-at 1704844800000
 :block/tags #{:logseq.class/Task}              ; Tags
 :block/collapsed? false                        ; Collapsed state (optional)

 ;; Custom properties
 :my.property/priority "high"}
```

**Block References:**

- `:block/page` - Always points to the top-level page
- `:block/parent` - Points to direct parent (can be page or block)
- References use `{:db/id N}` notation

**Block Order:**

- `:block/order` - Zero-indexed position within parent
- Must be unique within parent's children

---

## Common Patterns

### Entity References

References to other entities use maps with `:db/id`:

```clojure
;; In export
{:block/page {:db/id 3001}
 :block/parent {:db/id 3001}
 :block/tags #{:user.class/Project}}  ; Keywords for classes

;; During import, IDs are remapped
;; Keywords are resolved to actual entity IDs
```

### Property Values

```clojure
;; Single-value properties (:cardinality :one)
:my.property/status "active"
:my.property/priority 5
:my.property/due-date #inst "2025-12-31T00:00:00Z"

;; Multi-value properties (:cardinality :many)
:my.property/tags #{"tag1" "tag2" "tag3"}
:my.property/assignees #{:user/alice :user/bob}
```

### Timestamps

```clojure
;; Unix timestamp (milliseconds since epoch)
:block/created-at 1704844800000

;; EDN instant (ISO 8601)
:my.property/deadline #inst "2025-12-31T23:59:59.999Z"
```

### Optional Fields

Many fields are optional and can be omitted:

```clojure
;; Minimal page entity
{:block/uuid #uuid "..."
 :block/title "Page Title"
 :block/name "page title"}

;; Full page entity (with all optional fields)
{:block/uuid #uuid "..."
 :block/title "Page Title"
 :block/name "page title"
 :block/created-at 1704844800000
 :block/updated-at 1704844800000
 :block/tags #{:user.class/Project}
 :block/collapsed? false
 :logseq.property/icon "📊"
 :my.property/status "active"}
```

---

## Examples

### Example 1: Minimal Graph Export

```clojure
{:properties []
 :classes []
 :pages-and-blocks [{:block/uuid #uuid "68c012ca-2aee-46a8-a589-8bf67a1d4e92"
                     :block/title "My First Page"
                     :block/name "my first page"}]
 :logseq.db.sqlite.export/export-type :graph
 :logseq.db.sqlite.export/schema-version [1 2]
 :logseq.db.sqlite.export/kv-values {}}
```

### Example 2: Page with Properties

```clojure
{:properties [{:db/ident :my.property/status
               :block/title "status"
               :property/schema.type :string
               :property/schema.cardinality :one}]

 :classes [{:db/ident :my.class/Project
            :block/title "Project"
            :logseq.property.class/extends [:logseq.class/Root]
            :logseq.property.class/properties [:my.property/status]}]

 :pages-and-blocks [{:block/uuid #uuid "a1b2c3d4-..."
                     :block/title "My Project"
                     :block/name "my project"
                     :block/tags #{:my.class/Project}
                     :my.property/status "active"}]

 :logseq.db.sqlite.export/export-type :graph
 :logseq.db.sqlite.export/schema-version [1 2]}
```

### Example 3: Page with Nested Blocks

```clojure
{:properties []
 :classes []
 :pages-and-blocks
 [;; Page
  {:db/id 1
   :block/uuid #uuid "page-uuid"
   :block/title "Tasks"
   :block/name "tasks"}

  ;; Child block 1
  {:db/id 2
   :block/uuid #uuid "block1-uuid"
   :block/title "TODO First task"
   :block/page {:db/id 1}
   :block/parent {:db/id 1}
   :block/order 0
   :block/tags #{:logseq.class/Task}}

  ;; Child block 2
  {:db/id 3
   :block/uuid #uuid "block2-uuid"
   :block/title "Second task"
   :block/page {:db/id 1}
   :block/parent {:db/id 1}
   :block/order 1
   :block/tags #{:logseq.class/Task}}]

 :logseq.db.sqlite.export/export-type :graph
 :logseq.db.sqlite.export/schema-version [1 2]}
```

### Example 4: Schema-Only Export

```clojure
{:properties
 [{:db/ident :team.property/priority
   :block/title "priority"
   :property/schema.type :number
   :property/schema.cardinality :one
   :property/closed-values #{1 2 3 4 5}}

  {:db/ident :team.property/assignee
   :block/title "assignee"
   :property/schema.type :page
   :property/schema.cardinality :many}]

 :classes
 [{:db/ident :team.class/Task
   :block/title "Task"
   :logseq.property.class/extends [:logseq.class/Root]
   :logseq.property.class/properties [:team.property/priority
                      :team.property/assignee]}

  {:db/ident :team.class/Epic
   :block/title "Epic"
   :logseq.property.class/extends [:logseq.class/Root]
   :logseq.property.class/properties [:team.property/priority]}]

 :logseq.db.sqlite.export/export-type :graph-ontology}
```

---

## Validation

### Schema Validation

Logseq uses [Malli](https://github.com/metosin/malli) for schema validation.

**Validate on Export:**

```bash
# Export with validation
logseq export-edn -g my-notes -v
```

**Validate Separately:**

```bash
# Validate existing graph
logseq validate -g my-notes
```

### Common Validation Errors

**1. Missing Required Fields:**

```clojure
;; ❌ Error: Missing :block/title
{:block/uuid #uuid "..."}

;; ✅ Correct
{:block/uuid #uuid "..."
 :block/title "Page Title"
 :block/name "page title"}
```

**2. Invalid References:**

```clojure
;; ❌ Error: Referenced entity doesn't exist
{:block/page {:db/id 9999}}  ; ID 9999 doesn't exist

;; ✅ Correct: Reference valid entity
{:block/page {:db/id 1}}
```

**3. Type Mismatches:**

```clojure
;; Property defined as :number
{:db/ident :my.property/count
 :property/schema.type :number}

;; ❌ Error: String value for number property
:my.property/count "five"

;; ✅ Correct
:my.property/count 5
```

**4. Cardinality Violations:**

```clojure
;; Property defined as :cardinality :one
{:db/ident :my.property/status
 :property/schema.cardinality :one}

;; ❌ Error: Set value for single-value property
:my.property/status #{"active" "pending"}

;; ✅ Correct
:my.property/status "active"
```

---

## Best Practices

### 1. Use Meaningful Identifiers

```clojure
;; ❌ Poor naming
{:db/ident :p1}
{:db/ident :user.c/x}

;; ✅ Clear naming
{:db/ident :project.property/priority}
{:db/ident :team.class/Sprint}
```

### 2. Namespace Your Properties and Classes

```clojure
;; ✅ Good: Namespaced prevents conflicts
:my-app.property/status
:my-team.class/Task

;; ⚠️ Avoid: Generic names can conflict
:status
:Task
```

### 3. Include Timestamps for Full Backups

```bash
# Full backup with timestamps
logseq export-edn -g production -T -f backup-$(date +%Y%m%d).edn
```

### 4. Exclude Built-ins for Cleaner Exports

```bash
# Clean user-content export
logseq export-edn -g my-notes -b -F
```

### 5. Validate Before and After Import

```bash
# Validate source graph
logseq validate -g source-graph

# Export
logseq export-edn -g source-graph -f export.edn -v

# Import to new graph
logseq import-edn -g target-graph -f export.edn

# Validate target graph
logseq validate -g target-graph
```

### 6. Use Schema-Only Exports for Templates

```bash
# Create reusable schema template
logseq export-edn -g template -t :graph-ontology -f schema.edn

# Apply to new graphs
logseq import-edn -g new-project -f schema.edn
```

### 7. Handle Large Exports Efficiently

```bash
# For large graphs, use local mode (faster)
logseq export-edn -g large-graph -f export.edn

# NOT: logseq export-edn -f export.edn -a TOKEN (slower)
```

### 8. Pretty-Print for Debugging

```bash
# Export EDN
logseq export-edn -g my-graph -f graph.edn

# Pretty-print with clojure
clojure -M -e "(clojure.pprint/pprint (clojure.edn/read-string (slurp \"graph.edn\")))" > graph-pretty.edn
```

### 9. Version Control Your Schemas

```bash
# Track schema evolution
logseq export-edn -g production -t :graph-ontology -f schemas/v1.0.0.edn
git add schemas/v1.0.0.edn
git commit -m "Schema version 1.0.0"
```

### 10. Test Imports with Dry-Run (Future Feature)

```bash
# Currently not supported, but useful pattern:
# logseq import-edn -g test -f data.edn --dry-run
```

---

## Related Documentation

- [CLI User Guide](../../06-user-guides/cli-user-guide.md) - CLI command reference
- [CLI Developer Guide](developer-guide.md) - Architecture and implementation details
- [CLI Onboarding](onboarding.md) - Contributing to the CLI

---

## Appendix: Complete Entity Schema Reference

### Property Entity (Complete)

```clojure
{:db/id                           ; Integer - Database ID
 :db/ident                        ; Keyword - Unique identifier (required)
 :block/uuid                      ; UUID - Unique UUID
 :block/title                     ; String - Display name (required)
 :block/name                      ; String - Lowercase normalized name
 :block/created-at                ; Long - Unix timestamp ms (optional)
 :block/updated-at                ; Long - Unix timestamp ms (optional)
 :property/schema.type            ; Keyword - Value type (required)
 :property/schema.cardinality     ; Keyword - :one or :many (required)
 :property/schema.classes         ; Set of keywords - Allowed classes (optional)
 :property/closed-values          ; Set - Enum values (optional)
 :property/schema.description     ; String - Description (optional)
 :logseq.property/public?         ; Boolean - Public property (optional)}
```

### Class Entity (Complete)

```clojure
{:db/id                           ; Integer - Database ID
 :db/ident                        ; Keyword - Unique identifier (required)
 :block/uuid                      ; UUID - Unique UUID
 :block/title                     ; String - Display name (required)
 :block/name                      ; String - Lowercase normalized name
 :block/created-at                ; Long - Unix timestamp ms (optional)
 :block/updated-at                ; Long - Unix timestamp ms (optional)
 :logseq.property.class/extends                   ; Vector of keywords - Parent classes (optional)
 :logseq.property.class/properties                ; Vector of keywords - Properties (optional)
}
```

### Page Entity (Complete)

```clojure
{:db/id                           ; Integer - Database ID
 :block/uuid                      ; UUID - Unique UUID (required)
 :block/title                     ; String - Page title (required)
 :block/name                      ; String - Lowercase normalized name (required)
 :block/created-at                ; Long - Unix timestamp ms (optional)
 :block/updated-at                ; Long - Unix timestamp ms (optional)
 :block/tags                      ; Set of keywords - Tags/classes (optional)
 :block/collapsed?                ; Boolean - Collapsed state (optional)
 :logseq.property/icon            ; String - Page icon emoji (optional)
 :logseq.property.page/description  ; String - Page description (optional)
 ; ... custom properties based on graph schema
 }
```

### Block Entity (Complete)

```clojure
{:db/id                           ; Integer - Database ID
 :block/uuid                      ; UUID - Unique UUID (required)
 :block/title                     ; String - Block content (required)
 :block/page                      ; Map - {:db/id N} - Parent page (required)
 :block/parent                    ; Map - {:db/id N} - Direct parent (required)
 :block/order                     ; Long - Position in parent (required)
 :block/created-at                ; Long - Unix timestamp ms (optional)
 :block/updated-at                ; Long - Unix timestamp ms (optional)
 :block/tags                      ; Set of keywords - Tags (optional)
 :block/collapsed?                ; Boolean - Collapsed state (optional)
 ; ... custom properties based on graph schema
 }
```

---

**End of Reference** ✅
