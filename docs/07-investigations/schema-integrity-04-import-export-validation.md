# Schema Changes & Data Integrity Investigation

## Part 4: Import/Export & Validation Analysis

---

## Table of Contents

1. [Export System Architecture](#export-system-architecture)
2. [Import System Architecture](#import-system-architecture)
3. [Validation System](#validation-system)
4. [EDN Format Specification](#edn-format-specification)
5. [SQLite Export/Import](#sqlite-exportimport)
6. [Schema Version Compatibility](#schema-version-compatibility)
7. [Failure Scenarios](#failure-scenarios)

---

## Export System Architecture

### Overview

The export system creates portable graph backups in EDN (Extensible Data Notation) format with Transit serialization. Exports are schema-version-tagged and validated before saving.

**Key File:** [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs)

### Export Process Flow

```
Graph (SQLite + DataScript)
        ↓
Extract all entities
        ↓
Convert to EDN format
        ↓
Validate export structure
        ↓
Serialize with Transit
        ↓
Save to .edn file
```

### Main Export Function

**Location:** [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L700-L763)

```clojure
(defn build-export
  "Build an export map from the current graph"
  [db]
  (let [all-entities (d/q '[:find [(pull ?e [*]) ...]
                           :where [?e :block/uuid]]
                         db)
        schema-version (get-schema-version db)]
    {:version 1  ;; Export format version
     :schema-version schema-version
     :entities all-entities
     :datoms (export-datoms db)
     :metadata {:exported-at (js/Date.now)
                :app-version (get-app-version)}}))
```

### Export Structure

```clojure
{:version 1                      ;; Export format version
 :schema-version {:major 65      ;; Graph schema version
                  :minor 16}
 :entities [...all-blocks...]    ;; Complete entity maps
 :datoms [...]                   ;; Raw datoms (alternative representation)
 :metadata {:exported-at 1234567890
            :app-version "0.10.5"
            :graph-name "my-graph"}}
```

### Pre-Export Validation

**Location:** [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L1100-L1113)

```clojure
(defn validate-export
  "Validate graph before export"
  [db]
  (when-let [errors (validate-db! db)]
    (throw (ex-info "Cannot export invalid graph"
                   {:errors errors}))))

(defn export-with-validation
  "Export graph with pre-validation"
  [db path]
  (validate-export db)
  (let [export-data (build-export db)]
    (spit path (pr-str export-data))))
```

**Critical Issue:** Invalid graphs cannot be exported, preventing users from creating backups when they need them most.

### Entity Filtering

```clojure
(defn export-datoms
  "Export datoms with filtering"
  [db]
  (->> (d/datoms db :eavt)
       (remove internal-attribute?)
       (remove temp-id?)
       (map datom->export-format)))

(defn internal-attribute? [datom]
  ;; Skip internal implementation details
  (#{:db/id :db/ident :db/txInstant} (.-a datom)))
```

### Export Options

```clojure
(defn export-graph
  "Export with options"
  [db {:keys [include-assets?
              include-whiteboards?
              validate?
              compress?]
       :or {include-assets? true
            include-whiteboards? true
            validate? true
            compress? false}}]
  (when validate?
    (validate-export db))

  (cond-> (build-export db)
    (not include-whiteboards?)
    (remove-whiteboards)

    (not include-assets?)
    (remove-asset-refs)

    compress?
    (compress-with-gzip)))
```

---

## Import System Architecture

### Overview

The import system loads exported graphs into the current database. It handles schema version differences, validates imported data, and runs migrations if needed.

**Key Files:**

- [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L998-L1022) - Import logic
- [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs) - UI handlers

### Import Process Flow

```
EDN file
        ↓
Parse EDN structure
        ↓
Check schema version
        ↓
Run migrations if needed
        ↓
Validate entities
        ↓
Build import transactions
        ↓
Transact to database
        ↓
Verify integrity
```

### Main Import Function

**Location:** [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L998-L1022)

```clojure
(defn build-import
  "Build import transactions from export data"
  [export-data current-db]
  (let [export-version (:schema-version export-data)
        current-version (db-schema/schema-version)
        entities (:entities export-data)]

    ;; Check version compatibility
    (when (> (:major export-version) (:major current-version))
      (throw (ex-info "Export from newer version not supported"
                     {:export-version export-version
                      :current-version current-version})))

    ;; Build transaction data
    (for [entity entities]
      (transform-entity entity export-version current-version))))
```

### Schema Version Handling

```clojure
(defn import-with-migration
  "Import and migrate if schema versions differ"
  [conn export-data]
  (let [export-version (:schema-version export-data)
        current-version (db-schema/schema-version)
        cmp (db-schema/compare-schema-version export-version current-version)]

    (cond
      ;; Export newer than current - cannot import
      (pos? cmp)
      (throw (ex-info "Cannot import from newer version"
                     {:export export-version
                      :current current-version}))

      ;; Export older than current - need migration
      (neg? cmp)
      (do
        (import-entities conn (:entities export-data))
        (migrate conn export-version current-version))

      ;; Same version - direct import
      :else
      (import-entities conn (:entities export-data)))))
```

### Entity Transformation

```clojure
(defn transform-entity
  "Transform entity during import based on schema differences"
  [entity export-version current-version]
  (cond-> entity
    ;; Fix renamed attributes
    (and (< (:major export-version) 65)
         (:block/status entity))
    (rename-attribute :block/status :block/workflow-state)

    ;; Add required new attributes
    (and (< (:minor export-version) 15)
         (nil? (:block/priority entity)))
    (assoc :block/priority 0)

    ;; Remove deprecated attributes
    (:deprecated/old-attr entity)
    (dissoc :deprecated/old-attr)))
```

### Import Validation

```clojure
(defn validate-import
  "Validate import data before importing"
  [export-data]
  (when-not (:version export-data)
    (throw (ex-info "Invalid export format" {})))

  (when-not (:schema-version export-data)
    (throw (ex-info "Missing schema version" {})))

  (when-not (seq (:entities export-data))
    (throw (ex-info "No entities in export" {})))

  ;; Validate each entity
  (doseq [entity (:entities export-data)]
    (when-let [errors (validate-entity entity)]
      (throw (ex-info "Invalid entity in export"
                     {:entity entity :errors errors})))))
```

---

## Validation System

### Three Validation Modes

1. **Transaction-Time Validation** (Mandatory)

   - Runs on every transaction
   - Fast, focused validation
   - Cannot be disabled

2. **Full-Graph Validation** (Manual)

   - User-initiated via UI
   - Scans entire database
   - Comprehensive but slow

3. **Pre-Export Validation** (Automatic)
   - Runs before export
   - Prevents exporting corrupt data
   - Can block backups

### Transaction-Time Validation

**Location:** [deps/db/src/logseq/db.cljs](../../../deps/db/src/logseq/db.cljs#L118-L143)

```clojure
(defn transact!
  "Transact with validation"
  [conn tx-data tx-meta]
  (let [tx-report (d/transact! conn tx-data tx-meta)]
    ;; Validate transaction results
    (when-let [errors (validate-tx-report @conn tx-report)]
      ;; Rollback transaction
      (throw (ex-info "Transaction validation failed"
                     {:errors errors
                      :tx-data tx-data})))
    tx-report))
```

### Validation Implementation

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs#L25-L63)

```clojure
(defn validate-tx-report
  "Validate entities modified in transaction"
  [db tx-report]
  (let [modified-eids (set (map :e (:tx-data tx-report)))
        modified-entities (map #(d/entity db %) modified-eids)]
    (keep validate-entity modified-entities)))

(defn validate-entity
  "Validate single entity against Malli schema"
  [entity]
  (when entity
    (let [entity-type (get-entity-type entity)
          schema (get-schema-for-type entity-type)
          entity-map (into {} entity)]
      (when-not (m/validate schema entity-map)
        (m/explain schema entity-map)))))
```

### Full-Graph Validation

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs#L80-L108)

```clojure
(defn validate-db!
  "Validate entire database"
  [db]
  (let [all-entities (d/q '[:find [(pull ?e [*]) ...]
                           :where [?e :block/uuid]]
                         db)
        errors (keep validate-entity all-entities)]
    (when (seq errors)
      {:errors errors
       :count (count errors)
       :sample (take 10 errors)})))
```

### Validation Error Format

```clojure
{:entity-id 12345
 :entity-type :block
 :attribute :block/priority
 :expected :number
 :actual "high"
 :message "Expected number, got string"
 :path [:block/priority]
 :schema {:type :number :cardinality :one}}
```

### Validation Strictness Levels

```clojure
(def validation-config
  {:strict-mode? true         ;; Reject any validation failure
   :warn-only-attrs #{}        ;; Attributes that only warn
   :skip-validation-for #{}    ;; Entity types to skip
   :max-errors 100})           ;; Stop after N errors

(defn validate-with-config
  [db config]
  (let [errors (validate-db! db)]
    (if (:strict-mode? config)
      (when (seq errors) (throw (ex-info "Validation failed" {:errors errors})))
      (when (> (count errors) (:max-errors config))
        (log/warn "Many validation errors found" (count errors))))))
```

---

## EDN Format Specification

### EDN Structure

```clojure
{:version 1
 :schema-version {:major 65 :minor 16}
 :entities
 [{:db/id 1
   :block/uuid #uuid "123e4567-e89b-12d3-a456-426614174000"
   :block/title "My Page"
   :block/name "my-page"
   :block/original-name "My Page"
   :block/created-at 1703779200000
   :block/updated-at 1703779200000}
  {:db/id 2
   :block/uuid #uuid "223e4567-e89b-12d3-a456-426614174001"
   :block/title "A block"
   :block/parent {:db/id 1}
   :block/left {:db/id 1}
   :block/created-at 1703779200000
   :block/updated-at 1703779200000}]
 :datoms
 [[1 :block/uuid #uuid "123e4567-e89b-12d3-a456-426614174000"]
  [1 :block/title "My Page"]
  [2 :block/uuid #uuid "223e4567-e89b-12d3-a456-426614174001"]
  [2 :block/title "A block"]
  [2 :block/parent 1]]
 :metadata
 {:exported-at 1703779200000
  :app-version "0.10.5"
  :graph-name "my-graph"
  :platform "desktop"}}
```

### Transit Serialization

```clojure
(defn serialize-export
  "Serialize export to Transit JSON format"
  [export-data]
  (let [writer (t/writer :json)]
    (t/write writer export-data)))

(defn deserialize-export
  "Deserialize export from Transit JSON"
  [transit-string]
  (let [reader (t/reader :json)]
    (t/read reader transit-string)))
```

### Custom Transit Handlers

```clojure
;; UUID handler
(def uuid-write-handler
  (t/write-handler
    (fn [_] "uuid")
    (fn [uuid] (str uuid))))

(def uuid-read-handler
  (t/read-handler
    (fn [uuid-str] (uuid uuid-str))))

;; Date handler
(def date-write-handler
  (t/write-handler
    (fn [_] "date")
    (fn [date] (.getTime date))))

(def date-read-handler
  (t/read-handler
    (fn [timestamp] (js/Date. timestamp))))
```

---

## SQLite Export/Import

### SQLite Direct Export

```clojure
(defn export-sqlite-db
  "Export SQLite database file directly"
  [source-path target-path]
  ;; Close all connections
  (close-db! source-path)

  ;; Copy SQLite file
  (fs/copy-file source-path target-path)

  ;; Reopen database
  (open-db! source-path))
```

**Advantages:**

- Fast (file copy)
- Preserves indexes
- No serialization overhead

**Disadvantages:**

- Binary format (not human-readable)
- Platform-specific (SQLite version compatibility)
- Cannot edit manually

### EDN vs SQLite Export Comparison

| Feature            | EDN Export         | SQLite Export   |
| ------------------ | ------------------ | --------------- |
| **Format**         | Text (EDN/Transit) | Binary (SQLite) |
| **Size**           | Larger             | Smaller         |
| **Speed**          | Slower             | Faster          |
| **Portability**    | High               | Medium          |
| **Human-readable** | Yes (EDN)          | No              |
| **Schema Version** | Tagged             | Embedded        |
| **Migration**      | During import      | On open         |
| **Validation**     | Pre-export         | Runtime         |

---

## Schema Version Compatibility

### Import Compatibility Matrix

| Export Version | Current Version | Action                 | Notes                  |
| -------------- | --------------- | ---------------------- | ---------------------- |
| 65.16          | 65.16           | Direct import          | Exact match            |
| 65.15          | 65.16           | Import + migrate       | Migrate forward        |
| 65.7           | 65.16           | Import + multi-migrate | Multiple migrations    |
| 65.16          | 65.15           | **REJECT**             | Cannot downgrade       |
| 66.0           | 65.16           | **REJECT**             | Major version mismatch |
| 64.x           | 65.16           | **REJECT**             | Old major version      |

### Version Check Logic

```clojure
(defn can-import?
  "Check if export can be imported"
  [export-version current-version]
  (let [cmp (db-schema/compare-schema-version export-version current-version)]
    (cond
      ;; Export newer than current
      (pos? cmp)
      {:can-import? false
       :reason "Export from newer version"
       :action "Upgrade app to import this export"}

      ;; Different major version
      (not= (:major export-version) (:major current-version))
      {:can-import? false
       :reason "Major version mismatch"
       :action "Export is incompatible"}

      ;; Export older - need migration
      (neg? cmp)
      {:can-import? true
       :reason "Export from older version"
       :action "Will migrate during import"}

      ;; Same version
      :else
      {:can-import? true
       :reason "Same version"
       :action "Direct import"})))
```

---

## Failure Scenarios

### Scenario 1: Invalid Graph Export Blocked

**Situation:** User has corrupt graph, tries to export for backup

```
User: Export graph
App: Validate graph
App: Found 5 validation errors
App: ERROR - Cannot export invalid graph
Result: No backup created
```

**Risk:** Users lose data they can't backup

**Solution:**

```clojure
(defn export-with-warnings
  "Export even if validation fails, but warn user"
  [db path {:keys [force-export?]}]
  (if-let [errors (validate-db! db)]
    (if force-export?
      (do
        (log/warn "Exporting invalid graph" errors)
        (export-without-validation db path))
      (throw (ex-info "Graph invalid" {:errors errors})))
    (export-without-validation db path)))
```

### Scenario 2: Old Export Rejected

**Situation:** User tries to import 6-month-old backup

```
Export version: 65.7
Current version: 65.16
Migrations needed: 65.8, 65.9, 65.10, 65.11, 65.12, 65.13, 65.14, 65.15, 65.16
Result: Import succeeds but takes time
```

**Risk:** Long migration path may fail midway

### Scenario 3: Import Validation Failure

**Situation:** Exported graph has entities that don't validate in newer schema

```
Import: 65.15 export into 65.16 app
Entity #123: :block/priority is "high" (string)
New schema: :block/priority must be number
Validation: FAILED
Result: Import rejected
```

**Risk:** Valid old data becomes invalid under new rules

**Solution:**

```clojure
(defn import-with-transform
  "Transform entities during import to match new schema"
  [conn export-data]
  (let [transformed-entities
        (for [entity (:entities export-data)]
          (transform-for-current-schema entity))]
    (transact! conn transformed-entities)))

(defn transform-for-current-schema
  "Fix common incompatibilities"
  [entity]
  (cond-> entity
    ;; String priority -> number
    (string? (:block/priority entity))
    (update :block/priority parse-priority)

    ;; Missing required fields
    (nil? (:block/created-at entity))
    (assoc :block/created-at (js/Date.now))))
```

---

## Key Takeaways

1. **Export = Backup:** EDN exports are the primary backup mechanism
2. **Validation Blocks Export:** Invalid graphs cannot be exported (double-edged sword)
3. **No Downgrades:** Cannot import from newer versions
4. **Migration During Import:** Old exports auto-migrate on import
5. **Three Validation Points:** Transaction, full-graph, pre-export
6. **EDN vs SQLite:** Trade-offs between portability and performance
7. **Schema Tagging:** All exports tagged with schema version
8. **Compatibility Check:** Import validates version before proceeding

---

## Next Steps

Proceed to **Part 5: RTC Synchronization Analysis** for examination of how schema versions affect real-time collaboration.
