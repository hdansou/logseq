# Logseq CLI: EDN Format Reference

> **Last revised:** 2026-05-29

> Comprehensive guide to the EDN (Extensible Data Notation) format used by Logseq CLI for import/export operations

**Version**: Current CLI (`graph export/import/validate`)
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

### Full Graph Export (`:graph`)

Current CLI EDN export uses this shape:

```clojure
{:properties {:my.property/status
              {:db/ident :my.property/status
               :block/title "status"
               :logseq.property/type :default
               :db/cardinality :db.cardinality/one}}
 :classes {:my.class/Project
           {:block/title "Project"
            :build/class-extends #{:logseq.class/Root}
            :build/class-properties [:my.property/status]}}
 :pages-and-blocks [{:page {:block/title "My Project"
                            :build/tags #{:my.class/Project}
                            :build/properties {:my.property/status "active"}}
                     :blocks [{:block/title "TODO First task"
                               :build/tags #{:logseq.class/Task}}]}]
 :logseq.db.sqlite.export/export-type :graph
 :logseq.db.sqlite.export/schema-version {:major 65 :minor 33}
 :logseq.db.sqlite.export/graph-files [{:file/path "pages/my-project.md"
                                        :file/content "..."}]
 :logseq.db.sqlite.export/kv-values [{:db/ident :logseq.kv/graph-created-at
                                      :kv/value 1704844800000}]
 :logseq.db.sqlite.export/property-history #{{:block/uuid #uuid "..."
                                              :block/created-at 1704844800000
                                              :logseq.property.history/block [:block/uuid #uuid "..."]
                                              :logseq.property.history/property :my.property/status
                                              :logseq.property.history/scalar-value "active"}}}
```

### Removed CLI Export Mode

> **Removed:** CLI no longer exposes a direct schema-only `:graph-ontology` export command. `logseq graph export --type edn` always exports full graph data (`:graph`).

## Entity Schemas

### Property Entity (Current keys)

```clojure
{:db/ident :my.property/priority
 :block/title "priority"
 :logseq.property/type :number
 :db/cardinality :db.cardinality/one
 :build/property-classes #{:logseq.class/Task}
 :build/closed-values [{:value 1 :uuid #uuid "..."}
                       {:value 2 :uuid #uuid "..."}]}
```

**Property Types currently exposed to users** (`:logseq.property/type`):

- `:default`
- `:number`
- `:date`
- `:datetime`
- `:checkbox`
- `:url`
- `:node`
- `:asset`

### Class Entity (Current keys)

```clojure
{:block/title "Project"
 :build/class-extends #{:logseq.class/Root}
 :build/class-properties [:my.property/priority :my.property/status]}
```

### Page + Block Container (`:pages-and-blocks`)

```clojure
{:page {:block/title "Tasks"
        :build/properties {:my.property/status "active"}
        :build/tags #{:my.class/Project}}
 :blocks [{:block/title "TODO First task"
           :build/tags #{:logseq.class/Task}}
          {:block/title "Second task"}]}
```

## Common Patterns

### References

Current export/import commonly uses graph-stable references such as:

- db idents: `:my.property/status`
- UUID refs: `[:block/uuid #uuid "..."]`
- build-page refs: `[:build/page {:block/title "Some Page"}]`

> **Removed:** `{:db/id N}`-style reference notation is not the primary EDN interchange shape for current graph EDN export/import.

### Cardinality

- `:db.cardinality/one` for single value
- `:db.cardinality/many` for set/multi value

### Timestamps

- File/block/history timestamps are Unix epoch milliseconds (when included)
- EDN supports `#inst`, but primary graph metadata timestamps in export are numeric

## Examples

### Example: Minimal Current Graph Export

```clojure
{:properties {}
 :classes {}
 :pages-and-blocks [{:page {:block/title "My First Page"}
                     :blocks []}]
 :logseq.db.sqlite.export/export-type :graph
 :logseq.db.sqlite.export/schema-version {:major 65 :minor 33}
 :logseq.db.sqlite.export/kv-values []}
```

### Example: Current CLI Command Pair

```bash
logseq graph export --graph my-graph --type edn --file graph.edn
logseq graph import --graph target-graph --type edn --input graph.edn
```

---

## Validation

### Schema Validation

Logseq uses [Malli](https://github.com/metosin/malli) for schema validation.

**Validate on Export:**

```bash
# Export EDN
logseq graph export --graph my-notes --type edn --file my-notes.edn
```

**Validate Separately:**

```bash
# Validate existing graph
logseq graph validate --graph my-notes
```

> **Removed:** `logseq export-edn ...` and `logseq validate ...` are no longer the current CLI command surface.

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
{:logseq.property.history/block [:block/uuid #uuid "00000000-0000-0000-0000-000000000000"]}

;; ✅ Correct: Reference valid entity
{:logseq.property.history/block [:block/uuid #uuid "68c012ca-2aee-46a8-a589-8bf67a1d4e92"]}
```

**3. Type Mismatches:**

```clojure
;; Property defined as :number
{:db/ident :my.property/count
 :logseq.property/type :number}

;; ❌ Error: String value for number property
:my.property/count "five"

;; ✅ Correct
:my.property/count 5
```

**4. Cardinality Violations:**

```clojure
;; Property defined as :cardinality :one
{:db/ident :my.property/status
 :db/cardinality :db.cardinality/one}

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
logseq graph export --graph production --type edn --include-timestamps --file backup-$(date +%Y%m%d).edn
```

### 4. Exclude Built-ins for Cleaner Exports

```bash
# Clean user-content export
logseq graph export --graph my-notes --type edn --exclude-built-in-pages --exclude-namespaces schema.org --file my-notes.edn
```

> **Removed:** `-F` (exclude files) is not a current CLI flag on `logseq graph export`.

### 5. Validate Before and After Import

```bash
# Validate source graph
logseq graph validate --graph source-graph

# Export
logseq graph export --graph source-graph --type edn --file export.edn

# Import to new graph
logseq graph import --graph target-graph --type edn --input export.edn

# Validate target graph
logseq graph validate --graph target-graph
```

### 6. Use Repeatable EDN Templates (Full Graph)

```bash
# Create reusable EDN template graph
logseq graph export --graph template --type edn --exclude-built-in-pages --file template.edn

# Apply to new graphs
logseq graph import --graph new-project --type edn --input template.edn
```

> **Removed:** direct CLI `:graph-ontology` export flow.

### 7. Handle Large Exports Efficiently

```bash
# For large graphs, export to local file
logseq graph export --graph large-graph --type edn --file export.edn
```

### 8. Pretty-Print for Debugging

```bash
# Export EDN
logseq graph export --graph my-graph --type edn --file graph.edn

# Pretty-print with clojure
clojure -M -e "(clojure.pprint/pprint (clojure.edn/read-string (slurp \"graph.edn\")))" > graph-pretty.edn
```

### 9. Version Control Your Schemas

```bash
# Track schema evolution
logseq graph export --graph production --type edn --exclude-built-in-pages --file schemas/v1.0.0.edn
git add schemas/v1.0.0.edn
git commit -m "Schema version 1.0.0"
```

### 10. Dry-run Imports

> **Removed:** no `--dry-run` option exists for current `logseq graph import`.

## Findings: New Capabilities

- Graph EDN export includes `:logseq.db.sqlite.export/property-history` for property change history payloads.
- Graph EDN export includes `:logseq.db.sqlite.export/kv-values` as a vector of `{:db/ident ... :kv/value ...}` entries.
- CLI supports both EDN and SQLite import/export through `logseq graph export --type edn|sqlite` and `logseq graph import --type edn|sqlite`.

---

## Related Documentation

- [CLI User Guide](../../06-user-guides/cli-user-guide.md) - CLI command reference
- [CLI Developer Guide](developer-guide.md) - Architecture and implementation details
- [CLI Onboarding](onboarding.md) - Contributing to the CLI

---

## Appendix: Complete Entity Schema Reference

### Property Entity (Complete)

```clojure
{:db/ident                        ; Keyword - Unique identifier
 :block/title                     ; String - Display name
 :logseq.property/type            ; Keyword - e.g. :default/:number/:date/:datetime/:checkbox/:url/:node/:asset
 :db/cardinality                  ; Keyword - :db.cardinality/one or :db.cardinality/many
 :build/property-classes          ; Set of class idents (optional)
 :build/closed-values             ; Vector of {:value ... :uuid ...} (optional)
 :build/properties                ; Nested build properties (optional)
 :block/uuid                      ; UUID (optional, export mode dependent)
 :block/created-at                ; Long ms (optional)
 :block/updated-at                ; Long ms (optional)}
```

### Class Entity (Complete)

```clojure
{:block/title                     ; String - Display name
 :build/class-extends             ; Set of class idents (optional)
 :build/class-properties          ; Vector of property idents (optional)
 :build/properties                ; Nested build properties (optional)
 :block/uuid                      ; UUID (optional, export mode dependent)
 :block/created-at                ; Long ms (optional)
 :block/updated-at                ; Long ms (optional)
}
```

### Page Entity (Complete)

```clojure
{:page {:block/title              ; String - Page title
  :build/journal            ; Long journal day (optional)
  :build/tags               ; Set of class/tag idents (optional)
  :build/properties         ; Property map (optional)
  :block/uuid               ; UUID (optional)
  :block/created-at         ; Long ms (optional)
  :block/updated-at         ; Long ms (optional)}
 :blocks [...]}
```

### Block Entity (Complete)

```clojure
{:block/title                     ; String - Block content
 :build/tags                      ; Set of class/tag idents (optional)
 :build/properties                ; Property map (optional)
 :build/children                  ; Nested blocks (optional)
 :block/uuid                      ; UUID (optional)
 :block/created-at                ; Long ms (optional)
 :block/updated-at                ; Long ms (optional)}
```

---

**End of Reference** ✅
