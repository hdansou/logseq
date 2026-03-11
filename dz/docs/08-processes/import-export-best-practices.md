# Import/Export Best Practices

> **Last revised:** 2026-03-10

**Document Type:** Process Guide
**Last Updated:** December 28, 2025
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

Logseq's import/export system enables graph portability, backup/restore, and cross-version compatibility. This guide covers best practices for maintaining robust import/export functionality when making schema changes.

**Key Principle:** Export formats should be forward-compatible (new versions can read old exports) and provide clear error messages when backward compatibility is not possible.

---

## Table of Contents

1. [Export Format Overview](#export-format-overview)
2. [sqlite.build EDN Format](#sqlitebuild-edn-format)
3. [Transit Serialization](#transit-serialization)
4. [Export Implementation](#export-implementation)
5. [Import Implementation](#import-implementation)
6. [Circular Reference Handling](#circular-reference-handling)
7. [UUID Consistency](#uuid-consistency)
8. [Format Versioning](#format-versioning)
9. [Cross-Version Compatibility](#cross-version-compatibility)
10. [Performance Optimization](#performance-optimization)
11. [Error Handling](#error-handling)
12. [Testing Requirements](#testing-requirements)

---

## Export Format Overview

### Available Export Formats

Logseq supports multiple export formats:

| Format               | Use Case            | Location                                                                           | Notes                    |
| -------------------- | ------------------- | ---------------------------------------------------------------------------------- | ------------------------ |
| **sqlite.build EDN** | Graph export/import | [deps/db/src/logseq/db/sqlite/build.cljs](deps/db/src/logseq/db/sqlite/build.cljs) | Graph-agnostic, portable |
| **Transit**          | Debug/backup        | [deps/db/src/logseq/db/sqlite/util.cljs](deps/db/src/logseq/db/sqlite/util.cljs)   | Full graph serialization |
| **Markdown**         | File export         | Various                                                                            | File-based graphs only   |
| **JSON**             | API/integration     | Plugin API                                                                         | Limited schema info      |

### When to Use Each Format

**Use sqlite.build EDN when:**

- Exporting for import into another Logseq instance
- Creating portable backups
- Testing schema migrations
- Sharing graphs between users

**Use Transit when:**

- Creating debug exports for bug reports
- Full graph backup with all metadata
- Testing internal data structures

**Use Markdown when:**

- Exporting file-based graphs
- Interoperability with other tools
- Human-readable output

---

## sqlite.build EDN Format

### Format Structure

The sqlite.build EDN format is a graph-agnostic representation:

```clojure
{:version 1  ; Format version
 :blocks [...] ; All blocks (pages and child blocks)
 :properties [...] ; All property definitions
 :classes [...] ; All class/tag definitions
 :closed-values [...] ; Closed value choices
 :property-values [...] ; Property value entities
 :metadata {...}} ; Graph metadata
```

### Example Export Data

```clojure
{:version 1
 :blocks
 [{:uuid #uuid "..."
   :type "page"
   :original-name "My Page"
   :created-at 1234567890
   :updated-at 1234567890
   :properties {:logseq.property/tags #{#uuid "..."}
                :logseq.property/author "Alice"}}

  {:uuid #uuid "..."
   :type "block"
   :content "Block content"
   :parent #uuid "..."  ; Reference to parent
   :left #uuid "..."    ; Reference to left sibling
   :order "a0"
   :created-at 1234567890
   :updated-at 1234567890}]

 :properties
 [{:db-ident :logseq.property/author
   :type :default
   :cardinality :one
   :schema {...}}]

 :classes
 [{:db-ident :logseq.class/Person
   :properties #{:logseq.property/name :logseq.property/email}
   :extends #{:logseq.class/Tag}}]}
```

### Key Design Principles

1. **Graph-Agnostic:** No database-specific IDs or references
2. **UUID-Based:** All entities referenced by UUID
3. **Self-Contained:** All dependencies included in export
4. **Readable:** EDN format is human-readable
5. **Extensible:** Can add new fields without breaking old parsers

### Export Location

**Primary File:** [deps/db/src/logseq/db/sqlite/build.cljs](deps/db/src/logseq/db/sqlite/build.cljs)

**Key Functions:**

- `build-blocks-vec` - Export all blocks
- `build-properties-vec` - Export property definitions
- `build-classes-vec` - Export class definitions
- `build-property-values-vec` - Export property values

---

## Transit Serialization

### What is Transit?

Transit is a data serialization format designed for Clojure/ClojureScript. It efficiently serializes Clojure data structures while preserving types.

**Location:** [deps/db/src/logseq/db/sqlite/util.cljs](deps/db/src/logseq/db/sqlite/util.cljs)

### Transit vs EDN

| Aspect          | Transit               | EDN                   |
| --------------- | --------------------- | --------------------- |
| **Size**        | Smaller (binary)      | Larger (text)         |
| **Speed**       | Faster                | Slower                |
| **Readability** | Binary (not readable) | Human readable        |
| **Use Case**    | Performance-critical  | Portability/debugging |

### Transit Functions

```clojure
;; Serialize to transit string
(defn write-transit-str [data]
  (transit/write (transit/writer :json) data))

;; Deserialize from transit string
(defn read-transit-str [s]
  (transit/read (transit/reader :json) s))
```

### Custom Transit Handlers

**Location:** [deps/db/src/logseq/db/sqlite/util.cljs](deps/db/src/logseq/db/sqlite/util.cljs)

```clojure
;; Custom write handler for DataScript entities
(def write-handlers
  {datascript.impl.entity.Entity
   (transit/write-handler
    (constantly "entity")
    (fn [e] (into {} e)))})

;; Custom write handler for ordered maps
(def write-handlers
  {flatland.ordered.map.OrderedMap
   (transit/write-handler
    (constantly "map")
    (fn [m] (into {} m)))})
```

### When to Add Custom Handlers

Add custom Transit handlers when:

- Introducing new Clojure types in schema
- Using custom data structures in properties
- Supporting new property value types
- Handling special reference types

---

## Export Implementation

### Export Process Flow

```
Graph Data (DataScript DB)
         ↓
┌────────────────────┐
│ Extract Entities   │ ← Query all blocks, properties, classes
└────────┬───────────┘
         ↓
┌────────────────────┐
│ Convert to Build   │ ← Transform to graph-agnostic format
│ Format             │   - Replace entity IDs with UUIDs
└────────┬───────────┘   - Resolve references
         ↓
┌────────────────────┐
│ Serialize to EDN   │ ← Write to file/string
│ or Transit         │
└────────────────────┘
```

### Implementing Export for New Property

When adding a new property that needs export support:

#### Step 1: Include in Build Data

```clojure
;; In deps/db/src/logseq/db/sqlite/build.cljs

(defn block->build-block
  "Convert DataScript block entity to build format"
  [db block]
  (let [base {:uuid (:block/uuid block)
              :type (:block/type block)
              :content (:block/content block)
              :created-at (:block/created-at block)
              :updated-at (:block/updated-at block)}]
    ;; Add new property if present
    (cond-> base
      (:logseq.property/my-new-property block)
      (assoc :my-new-property (:logseq.property/my-new-property block)))))
```

#### Step 2: Handle References

If property contains references to other entities:

```clojure
(defn resolve-property-references
  "Convert entity references to UUIDs for export"
  [db property-value]
  (cond
    ;; Single entity reference
    (entity? property-value)
    (:block/uuid property-value)

    ;; Collection of entity references
    (coll? property-value)
    (mapv #(if (entity? %)
             (:block/uuid %)
             %)
          property-value)

    ;; Scalar value
    :else
    property-value))
```

#### Step 3: Include in Export Options

```clojure
(defn export-db
  "Export database to build format"
  [db & [{:keys [include-timestamps?
                 property-value-uuids?
                 include-my-new-property?]  ; Add option
          :or {include-timestamps? true
               property-value-uuids? true
               include-my-new-property? true}}]]
  {:blocks (build-blocks-vec db {:include-my-new-property? include-my-new-property?})
   :properties (build-properties-vec db)
   :classes (build-classes-vec db)})
```

### Export Best Practices

1. **Don't Export Schema Version**

   ```clojure
   ;; ❌ Wrong - don't export app's schema version
   {:schema-version "65.16"}

   ;; ✅ Correct - let importing app set its own version
   {:blocks [...] :properties [...]}
   ```

2. **Preserve UUIDs**

   ```clojure
   ;; ✅ Always include UUIDs for entities
   {:uuid #uuid "123e4567-e89b-12d3-a456-426614174000"
    :content "Block content"}
   ```

3. **Handle Missing Data Gracefully**

   ```clojure
   (defn safe-export-property [block prop]
     (when-let [value (get block prop)]
       {prop value}))
   ```

4. **Validate Before Export**
   ```clojure
   (defn export-db [db]
     ;; Validate DB before export
     (when-let [errors (validate-db! db)]
       (log/warn "Exporting graph with validation errors:" errors))
     (build-export-data db))
   ```

---

## Import Implementation

### Import Process Flow

```
Build Format EDN File
         ↓
┌────────────────────┐
│ Parse EDN/Transit  │ ← Read and parse file
└────────┬───────────┘
         ↓
┌────────────────────┐
│ Validate Format    │ ← Check version, structure
└────────┬───────────┘
         ↓
┌────────────────────┐
│ Resolve References │ ← Create temp IDs for entities
│ & Dependencies     │   - Build dependency graph
└────────┬───────────┘   - Resolve circular refs
         ↓
┌────────────────────┐
│ Build Transaction  │ ← Create DataScript tx-data
│ Data               │   - Properties first
└────────┬───────────┘   - Classes second
         ↓                - Blocks last
┌────────────────────┐
│ Execute Transactions│ ← Transact to database
└────────────────────┘
```

### Implementing Import for New Property

#### Step 1: Add to Import Parser

```clojure
;; In deps/db/src/logseq/db/sqlite/build.cljs

(defn build-block->db-block
  "Convert build format block to DataScript transaction"
  [build-block uuid->tempid]
  (let [base {:block/uuid (:uuid build-block)
              :block/type (:type build-block)
              :block/content (:content build-block)
              :block/created-at (:created-at build-block)
              :block/updated-at (:updated-at build-block)}]
    ;; Import new property if present
    (cond-> base
      (:my-new-property build-block)
      (assoc :logseq.property/my-new-property
             (:my-new-property build-block)))))
```

#### Step 2: Resolve Property References

```clojure
(defn resolve-import-references
  "Convert UUIDs to tempids for import"
  [value uuid->tempid]
  (cond
    ;; UUID reference - convert to tempid
    (uuid? value)
    (uuid->tempid value)

    ;; Collection of UUIDs
    (and (coll? value) (every? uuid? value))
    (mapv uuid->tempid value)

    ;; Scalar value - use as-is
    :else
    value))
```

#### Step 3: Handle Missing Properties

```clojure
(defn import-block-with-fallback
  "Import block, using defaults for missing required properties"
  [build-block uuid->tempid schema-version]
  (let [block (build-block->db-block build-block uuid->tempid)]
    ;; Add default for new required property if missing
    (cond-> block
      (and (>= (parse-version schema-version) "65.17")
           (nil? (:logseq.property/my-new-property block)))
      (assoc :logseq.property/my-new-property default-value))))
```

### Import Best Practices

1. **Validate Input Format**

   ```clojure
   (defn validate-import-data [data]
     (when-not (:blocks data)
       (throw (ex-info "Missing :blocks in import data" {})))
     (when-not (:properties data)
       (throw (ex-info "Missing :properties in import data" {}))))
   ```

2. **Resolve Dependencies First**

   ```clojure
   ;; Import order matters!
   (defn import-graph [data]
     ;; 1. Properties (no dependencies)
     (transact! conn (build-properties-tx data))
     ;; 2. Classes (depend on properties)
     (transact! conn (build-classes-tx data))
     ;; 3. Blocks (depend on everything)
     (transact! conn (build-blocks-tx data)))
   ```

3. **Handle Format Version Differences**

   ```clojure
   (defn import-with-version-handling [data]
     (let [format-version (:version data 1)]
       (case format-version
         1 (import-format-v1 data)
         2 (import-format-v2 data)
         (throw (ex-info "Unsupported format version"
                        {:version format-version})))))
   ```

4. **Use Temp IDs for New Entities**
   ```clojure
   (defn build-uuid->tempid-map [blocks]
     (into {}
           (map (fn [block]
                  [(:uuid block) (str "tempid-" (random-uuid))]))
           blocks))
   ```

---

## Circular Reference Handling

### Common Circular References

In Logseq's data model:

1. **Parent-Child:** Parent references child, child references parent
2. **Left-Sibling:** Blocks reference each other for ordering
3. **Tags:** Page tagged with class, class has properties, properties have values on page
4. **Property Values:** Value entity references property, property referenced on entity with value

### Resolution Strategy

#### Two-Pass Import

```clojure
(defn import-with-circular-refs [blocks]
  ;; Pass 1: Create all entities with required fields only
  (let [create-tx (mapv (fn [block]
                          {:db/id (tempid)
                           :block/uuid (:uuid block)
                           :block/type (:type block)
                           :block/content (:content block)})
                        blocks)]
    (d/transact! conn create-tx))

  ;; Pass 2: Add references now that all entities exist
  (let [uuid->eid (build-uuid-eid-map @conn)
        ref-tx (mapv (fn [block]
                       {:block/uuid (:uuid block)
                        :block/parent (uuid->eid (:parent block))
                        :block/left (uuid->eid (:left block))})
                     blocks)]
    (d/transact! conn ref-tx)))
```

#### Upsert Pattern

```clojure
(defn import-with-upsert [blocks uuid->tempid]
  ;; Use :block/uuid as unique identity
  ;; DataScript resolves circular refs automatically
  (let [tx-data (mapv (fn [block]
                        {:block/uuid (:uuid block)  ; Unique identity
                         :block/content (:content block)
                         :block/parent (uuid->tempid (:parent block))
                         :block/left (uuid->tempid (:left block))})
                      blocks)]
    (d/transact! conn tx-data)))
```

### Best Practices

1. **Use Unique Identities**

   ```clojure
   ;; DataScript schema
   {:block/uuid {:db/unique :db.unique/identity}}

   ;; Import with UUID - circular refs resolved automatically
   {:block/uuid uuid-1
    :block/parent {:block/uuid uuid-2}}
   ```

2. **Explicit Dependency Ordering**

   ```clojure
   (defn sort-blocks-by-dependency [blocks]
     ;; Pages first (no parent), then child blocks
     (concat (filter #(= "page" (:type %)) blocks)
             (filter #(= "block" (:type %)) blocks)))
   ```

3. **Validate After Import**
   ```clojure
   (defn import-and-validate [data]
     (import-graph data)
     (let [[valid? errors] (validate-db! @conn)]
       (when-not valid?
         (log/error "Import resulted in invalid data:" errors)
         (throw (ex-info "Import validation failed" {:errors errors})))))
   ```

---

## UUID Consistency

### Why UUIDs Matter

UUIDs provide:

- **Stable Identity:** Entity ID that survives export/import
- **Merge Capability:** Identify same entity across graphs
- **RTC Sync:** Essential for conflict resolution
- **Debugging:** Trackable entities across systems

### UUID Requirements

1. **Every Entity Must Have UUID**

   ```clojure
   ;; ✅ Correct
   {:db/id -1
    :block/uuid (d/squuid)  ; Sequential UUID
    :block/content "..."}
   ```

2. **UUIDs Must Be Stable**

   ```clojure
   ;; ❌ Wrong - don't regenerate UUIDs on import
   (defn import-block-wrong [block]
     {:block/uuid (d/squuid)  ; NEW UUID - breaks references!
      :block/content (:content block)})

   ;; ✅ Correct - preserve original UUID
   (defn import-block-correct [block]
     {:block/uuid (:uuid block)  ; Original UUID
      :block/content (:content block)})
   ```

3. **Handle UUID Collisions**
   ```clojure
   (defn import-with-collision-check [blocks existing-db]
     (doseq [block blocks]
       (when-let [existing (d/entity existing-db [:block/uuid (:uuid block)])]
         (log/warn "UUID collision detected:" (:uuid block))
         ;; Strategy: Skip, merge, or regenerate UUID
         )))
   ```

### UUID Generation

```clojure
;; For new entities during import
(require '[datascript.core :as d])

;; Sequential UUID (better for performance)
(d/squuid)

;; Random UUID (standard)
(random-uuid)
```

### UUID in Export

```clojure
;; Always use tagged literal for UUIDs in EDN
{:uuid #uuid "123e4567-e89b-12d3-a456-426614174000"
 :content "..."}

;; Transit handles UUIDs automatically
(write-transit-str {:uuid (random-uuid)})
```

---

## Format Versioning

### Why Format Versions?

Format versions enable:

- **Backward Compatibility:** New versions can read old exports
- **Migration:** Transform old formats to new structure
- **Error Detection:** Clear errors for unsupported versions

### Version Tracking

```clojure
;; Export with format version
(defn export-db-with-version [db]
  {:version 2  ; Format version
   :app-version "0.10.9"  ; App version (optional)
   :blocks [...]
   :properties [...]})
```

### Version Compatibility Matrix

```clojure
(def format-version-compatibility
  "Map of format versions to compatible app versions"
  {1 {:min-app-version "0.9.0"
      :max-app-version "0.10.8"
      :deprecated false}
   2 {:min-app-version "0.10.9"
      :max-app-version nil  ; Current
      :deprecated false}})
```

### Handling Version Differences

```clojure
(defn import-with-version-migration [data]
  (let [format-version (:version data 1)]
    (cond
      ;; Current version - no migration needed
      (= format-version 2)
      (import-format-v2 data)

      ;; Old version - migrate first
      (= format-version 1)
      (let [migrated (migrate-format-v1->v2 data)]
        (import-format-v2 migrated))

      ;; Unknown version
      :else
      (throw (ex-info "Unsupported format version"
                     {:version format-version
                      :supported [1 2]})))))
```

### Format Migration Example

```clojure
(defn migrate-format-v1->v2
  "Migrate export format from v1 to v2"
  [v1-data]
  {:version 2
   ;; In v2, we renamed :tags to :block/tags
   :blocks (mapv (fn [block]
                   (-> block
                       (dissoc :tags)
                       (assoc :block/tags (:tags block))))
                 (:blocks v1-data))
   :properties (:properties v1-data)
   :classes (:classes v1-data)})
```

### Adding New Format Version

When making breaking changes to export format:

1. **Increment format version**

   ```clojure
   (def current-export-format-version 3)
   ```

2. **Implement new export**

   ```clojure
   (defn export-format-v3 [db]
     {:version 3
      :blocks (build-blocks-v3 db)
      :properties (build-properties-v3 db)})
   ```

3. **Add migration from previous version**

   ```clojure
   (defn migrate-format-v2->v3 [v2-data]
     ;; Transform v2 to v3 structure
     )
   ```

4. **Update import dispatcher**

   ```clojure
   (case format-version
     1 (import-v1 data)
     2 (import-v2 data)
     3 (import-v3 data)  ; New version
     (throw-unsupported-version format-version))
   ```

5. **Update documentation**

---

## Cross-Version Compatibility

### Compatibility Strategies

#### Strategy 1: Additive Changes (Preferred)

```clojure
;; Old export (v65.16)
{:blocks [{:uuid #uuid "..."
           :content "Block"}]}

;; New export (v65.17) - adds optional field
{:blocks [{:uuid #uuid "..."
           :content "Block"
           :my-new-property "value"}]}  ; Optional, old versions ignore

;; Old versions can still import new exports!
```

#### Strategy 2: Format Version with Migration

```clojure
;; Detect format version and migrate if needed
(defn import-any-version [data]
  (let [version (:version data)
        current-version 2]
    (if (< version current-version)
      ;; Migrate old format
      (import-current-version (migrate-to-current data))
      ;; Import directly
      (import-current-version data))))
```

#### Strategy 3: Explicit Compatibility Check

```clojure
(defn check-import-compatibility [export-schema-version]
  (let [app-schema-version (get-current-schema-version)]
    (cond
      ;; Same version - fully compatible
      (= export-schema-version app-schema-version)
      {:compatible? true :action :import}

      ;; Export is older - can upgrade
      (< (parse-version export-schema-version)
         (parse-version app-schema-version))
      {:compatible? true :action :import-and-migrate}

      ;; Export is newer - cannot import
      :else
      {:compatible? false
       :error "Please upgrade app to import this graph"})))
```

### Handling Schema Version Mismatches

```clojure
(defn import-with-schema-check [data]
  (let [export-version (get-schema-version-from-export data)
        app-version (get-current-schema-version)
        compat (check-import-compatibility export-version)]
    (case (:action compat)
      :import
      (import-graph data)

      :import-and-migrate
      (do (log/info "Migrating from" export-version "to" app-version)
          (import-and-migrate-graph data))

      ;; Not compatible
      (throw (ex-info "Incompatible schema version"
                     {:export-version export-version
                      :app-version app-version
                      :error (:error compat)})))))
```

### Backward Compatibility Guidelines

1. **Additive Changes Only**

   - Add new fields with defaults
   - Don't remove existing fields
   - Don't change field types

2. **Version Checks**

   - Check format version before import
   - Provide clear error messages
   - Document version requirements

3. **Migration Paths**

   - Provide migrations for at least 3 versions back
   - Test migration paths regularly
   - Document breaking changes

4. **Deprecation Strategy**
   - Mark old fields as deprecated
   - Keep supporting for 3+ versions
   - Log warnings when importing deprecated fields
   - Document timeline for removal

---

## Performance Optimization

### Export Performance

#### Large Graph Optimization

```clojure
(defn export-large-graph-optimized [db]
  ;; Process in batches to avoid memory issues
  (let [batch-size 1000
        all-blocks (get-all-block-uuids db)]
    {:blocks (->> all-blocks
                  (partition-all batch-size)
                  (mapcat (fn [batch]
                            (build-blocks-batch db batch)))
                  vec)}))
```

#### Lazy Export

```clojure
(defn export-graph-lazy [db output-stream]
  "Write export directly to stream instead of building in memory"
  (let [writer (io/writer output-stream)]
    ;; Write header
    (.write writer "{:version 1\n :blocks [")

    ;; Stream blocks
    (doseq [block (get-all-blocks-lazy db)]
      (.write writer (pr-str (block->build-block db block)))
      (.write writer "\n"))

    (.write writer "]}")
    (.flush writer)))
```

### Import Performance

#### Batch Transactions

```clojure
(defn import-in-batches [blocks batch-size]
  "Import large datasets in batches for better performance"
  (doseq [batch (partition-all batch-size blocks)]
    (let [tx-data (mapv build-block->tx batch)]
      (d/transact! conn tx-data))))
```

#### Disable Validation During Import

```clojure
(defn fast-import [data]
  "Import without validation for performance"
  (binding [validate/*validate-db?* false]
    (import-graph data))
  ;; Validate once at the end
  (validate-db! @conn))
```

#### Parallel Processing

```clojure
(require '[clojure.core.async :as async])

(defn parallel-import [blocks]
  "Process blocks in parallel (careful with transaction order!)"
  (let [pages (filter #(= "page" (:type %)) blocks)
        child-blocks (filter #(= "block" (:type %)) blocks)]
    ;; Import pages in parallel (no dependencies between pages)
    (->> pages
         (partition-all 100)
         (map #(async/thread (import-batch %)))
         (map async/<!!))
    ;; Import child blocks after pages exist
    (import-blocks-sequential child-blocks)))
```

### Benchmarks

**Typical export times:**

- Small graph (1,000 blocks): ~1s
- Medium graph (10,000 blocks): ~5s
- Large graph (100,000 blocks): ~30s

**Typical import times:**

- Small graph: ~2s
- Medium graph: ~10s
- Large graph: ~60s

### Performance Guidelines

1. **Batch Operations:** Process in chunks of 1000-5000 entities
2. **Disable Validation:** During bulk operations, validate once at end
3. **Stream Large Exports:** Don't build entire export in memory
4. **Monitor Memory:** Large graphs can exceed memory limits
5. **Progress Indicators:** Show progress for long operations

---

## Error Handling

### Common Import Errors

#### 1. Missing Dependencies

```clojure
(try
  (import-graph data)
  (catch ExceptionInfo e
    (when (= (:type (ex-data e)) :missing-dependency)
      (log/error "Import failed - missing dependency:"
                 (:missing-entity (ex-data e)))
      (show-user-error "Graph is incomplete. Missing referenced entities."))))
```

#### 2. UUID Collisions

```clojure
(defn handle-uuid-collision [existing-entity import-entity]
  (log/warn "UUID collision:" (:block/uuid existing-entity))
  ;; Strategy options:
  ;; 1. Skip import (keep existing)
  ;; 2. Merge data
  ;; 3. Regenerate UUID for import
  ;; 4. Error and abort
  )
```

#### 3. Invalid Data

```clojure
(defn import-with-validation [data]
  (try
    (import-graph data)
    (let [[valid? errors] (validate-db! @conn)]
      (if valid?
        {:success true}
        {:success false
         :errors errors
         :action :rollback}))
    (catch Exception e
      {:success false
       :error (ex-message e)
       :action :rollback})))
```

### Error Recovery Strategies

#### Rollback on Error

```clojure
(defn import-with-rollback [data]
  (let [backup (export-db @conn)]
    (try
      (import-graph data)
      (catch Exception e
        (log/error "Import failed, rolling back:" e)
        (reset-conn! conn (create-db-from-backup backup))
        (throw e)))))
```

#### Partial Import

```clojure
(defn import-with-partial-recovery [data]
  "Import as much as possible, report failures"
  (let [results (atom {:success [] :failed []})]
    (doseq [block (:blocks data)]
      (try
        (import-block block)
        (swap! results update :success conj (:uuid block))
        (catch Exception e
          (log/error "Failed to import block:" (:uuid block) e)
          (swap! results update :failed conj {:uuid (:uuid block)
                                               :error (ex-message e)}))))
    @results))
```

### User-Facing Error Messages

```clojure
(defn format-import-error [error]
  (case (:type error)
    :version-mismatch
    "This graph was exported from a newer version of Logseq. Please upgrade."

    :invalid-format
    "The import file is corrupted or in an unsupported format."

    :missing-dependencies
    "This graph is incomplete. Some referenced pages or blocks are missing."

    :validation-failed
    "The imported data contains invalid entries. The graph may not work correctly."

    "An error occurred during import. Please try again or contact support."))
```

---

## Testing Requirements

### Required Tests

#### 1. Round-Trip Test

```clojure
(deftest test-export-import-round-trip
  (testing "Export and import preserves all data"
    (let [original-db (create-test-db-with-data)
          exported (export-db original-db)
          imported-db (import-db exported)
          re-exported (export-db imported-db)]
      ;; Exported data should be identical
      (is (= exported re-exported))

      ;; All entities preserved
      (is (= (count-entities original-db)
             (count-entities imported-db)))

      ;; No validation errors
      (is (validate-db! imported-db)))))
```

#### 2. Cross-Version Test

```clojure
(deftest test-import-old-version
  (testing "Can import exports from previous versions"
    (doseq [old-version ["65.14" "65.15" "65.16"]]
      (let [old-export (load-test-export old-version)
            imported-db (import-db old-export)]
        (is (validate-db! imported-db)
            (str "Import from " old-version " should be valid"))))))
```

#### 3. Large Graph Test

```clojure
(deftest test-large-graph-export-import
  (testing "Can handle large graphs"
    (let [large-db (create-test-db-with-data {:pages 10000
                                               :blocks 100000})
          start (js/Date.now)
          exported (export-db large-db)
          export-time (- (js/Date.now) start)]
      (is (< export-time 60000) "Export should complete in < 60s")

      (let [start (js/Date.now)
            imported-db (import-db exported)
            import-time (- (js/Date.now) start)]
        (is (< import-time 120000) "Import should complete in < 120s")))))
```

#### 4. Circular Reference Test

```clojure
(deftest test-circular-references
  (testing "Handles circular references correctly"
    (let [db-with-circular-refs (create-db-with-circular-refs)
          exported (export-db db-with-circular-refs)
          imported-db (import-db exported)]
      (is (all-references-resolved? imported-db))
      (is (no-dangling-references? imported-db)))))
```

#### 5. Error Handling Test

```clojure
(deftest test-invalid-import-data
  (testing "Handles invalid import data gracefully"
    (is (thrown-with-msg? ExceptionInfo #"Missing :blocks"
                         (import-db {:properties [...]})))

    (is (thrown-with-msg? ExceptionInfo #"Unsupported format version"
                         (import-db {:version 999})))))
```

### Testing Checklist

- [ ] Round-trip test (export → import → export produces same result)
- [ ] Cross-version compatibility (import from previous 3 versions)
- [ ] Large graph performance (10K+ pages, 100K+ blocks)
- [ ] Circular reference handling
- [ ] UUID preservation and consistency
- [ ] Invalid data error handling
- [ ] Missing dependency detection
- [ ] Format version migration
- [ ] Memory usage with large graphs
- [ ] Validation after import

---

## Quick Reference

### Export Checklist

- [ ] Include all entities (blocks, properties, classes, values)
- [ ] Use UUIDs for all references
- [ ] Don't export schema version (app sets it)
- [ ] Handle circular references
- [ ] Include format version
- [ ] Validate before export
- [ ] Test round-trip import

### Import Checklist

- [ ] Validate format version
- [ ] Check schema compatibility
- [ ] Preserve original UUIDs
- [ ] Resolve references correctly
- [ ] Import in dependency order (properties → classes → blocks)
- [ ] Handle missing dependencies
- [ ] Validate after import
- [ ] Handle errors gracefully

### When Adding New Property

- [ ] Include in `block->build-block` export function
- [ ] Include in `build-block->db-block` import function
- [ ] Handle references if property contains entity refs
- [ ] Add to format version migration if breaking change
- [ ] Test export/import with new property
- [ ] Update documentation

---

## Related Documentation

- [Investigation Overview](../07-investigations/schema-evolution-and-data-migration.md)
- [Schema Migration Best Practices](./schema-migration-best-practices.md)
- [Validation Guidelines](./validation-best-practices.md)
- [RTC/Sync Considerations](./rtc-sync-schema-changes.md)
- [Testing Strategies](./schema-change-testing.md)
- [PR Review Checklist](./schema-change-pr-checklist.md)

---

## Changelog

| Date       | Changes                                         |
| ---------- | ----------------------------------------------- |
| 2025-12-28 | Initial version based on investigation findings |
