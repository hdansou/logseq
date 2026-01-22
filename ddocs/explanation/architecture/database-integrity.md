---
title: 'Database Integrity & Validation'
status: 🟢 Stable
version: 'Schema v65.16'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Schema System](./schema-system.md)'
  - '[Block Editor Architecture](./block-editor.md)'
  - '[RTC Collaboration](../../tutorials/collaboration/rtc-setup.md)'
tags: ['integrity', 'validation', 'database', 'transactions', 'malli']
---

# Database Integrity & Validation

This document explains Logseq's database integrity system—the validation mechanisms ensuring data consistency, preventing corruption, and maintaining schema compliance. Understanding this system is essential for contributors and developers working with database graphs.

**What You'll Learn:**

- Multi-layer validation architecture
- Transaction validation process
- Schema compliance checking
- Data integrity guarantees
- Migration safety mechanisms

**Prerequisites:**

- Understanding of database graphs ([Schema System](./schema-system.md))
- Basic DataScript knowledge
- Familiarity with Malli validation

---

## Table of Contents

1. [Validation Architecture](#validation-architecture)
2. [Transaction Validation](#transaction-validation)
3. [Schema Compliance](#schema-compliance)
4. [Integrity Constraints](#integrity-constraints)
5. [Migration Safety](#migration-safety)
6. [Failure Recovery](#failure-recovery)
7. [Developer Guide](#developer-guide)

---

## Validation Architecture

### Multi-Layer Validation System

```
┌─────────────────────────────────────────────────────────┐
│            Application Layer                            │
│  • UI input validation                                  │
│  • User-facing error messages                           │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Transaction Layer                            │
│  • Pre-transaction validation                           │
│  • Transaction size limits                              │
│  • Rate limiting (planned)                              │
│  Location: frontend.handler.db_based.editor             │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Malli Validation Layer                       │
│  • Entity type dispatch                                 │
│  • Property type checking                               │
│  • Cardinality enforcement                              │
│  • Business logic rules                                 │
│  Location: logseq.db.frontend.malli_schema              │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            DataScript Schema Layer                      │
│  • Structural constraints (:db/valueType)               │
│  • Reference integrity                                  │
│  • Unique constraints (:db/unique)                      │
│  • Cardinality constraints                              │
│  Location: logseq.db.frontend.schema                    │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            SQLite Persistence Layer                     │
│  • ACID transaction guarantees                          │
│  • Foreign key constraints                              │
│  • Rollback on failure                                  │
└─────────────────────────────────────────────────────────┘
```

### Validation Modes

**Three validation modes** for different contexts:

**1. Transaction Validation** (Default)

- Validates each transaction before commit
- Fastest (only checks changed data)
- Used during normal editing

**2. Full-Graph Validation**

- Validates entire graph consistency
- Slower (checks all entities)
- Used after migrations or imports

**3. Pre-Export Validation**

- Validates before export to EDN
- Ensures exportable data
- Critical for backups

---

## Transaction Validation

### Validation Flow

**Location:** `deps/db/src/logseq/db/frontend/validate.cljs`

```
1. Prepare Transaction Data
   ↓
2. Group by Entity Type (dispatch)
   ↓
3. Validate Each Entity (Malli schema)
   ↓
4. Check DataScript Constraints
   ↓
5. Validate References (existence)
   ↓
6. Commit or Reject
```

### Pre-Transaction Validation

**Example: Insert Block**

**Location:** `src/main/frontend/handler/db_based/editor.cljs:250-280`

```clojure
(defn insert-block!
  [content]
  ;; 1. Build block entity
  (let [block {:block/uuid (random-uuid)
               :block/title content
               :block/parent parent-ref
               :block/page page-ref
               :block/order order}]

    ;; 2. Validate BEFORE transaction
    (when-let [errors (validate/validate-entity db block)]
      (throw (ex-info "Invalid block" {:errors errors})))

    ;; 3. Transact if valid
    (d/transact! conn [block])))
```

### Transaction Validation Rules

**Required Fields:**

```clojure
;; Block must have:
[:block/uuid :uuid]           ; Unique identifier
[:block/title :string]        ; Content (can be empty)
[:block/parent :ref]          ; Parent block/page
[:block/page :ref]            ; Page reference
[:block/order :string]        ; Fractional index
```

**Type Checking:**

```clojure
;; Property value types
{:logseq.property/status "pending"}       ; ✅ String (type :default)
{:logseq.property/status 123}             ; ❌ Invalid (expected string)

{:logseq.property.task/due-date #inst "2025-01-25"}  ; ✅ Date
{:logseq.property.task/due-date "2025-01-25"}        ; ❌ Invalid (expected inst)
```

**Reference Validation:**

```clojure
;; Block references must exist
[?b :block/refs [{:block/uuid #uuid "abc-123"}]]  ; ✅ If block exists
[?b :block/refs [{:block/uuid #uuid "xyz-999"}]]  ; ❌ If block missing
```

---

## Schema Compliance

### Entity Type Validation

**Location:** `deps/db/src/logseq/db/frontend/malli_schema.cljs:130-200`

```clojure
(def Data
  [:multi {:dispatch entity-dispatch-key}
   [:normal-page normal-page-schema]
   [:class class-schema]
   [:property property-schema]
   [:block block-schema]
   [:property-value-block property-value-schema]
   [:closed-value-block closed-value-schema]
   [:hidden hidden-schema]])

(defn entity-dispatch-key
  [db entity]
  ;; 1. Check :type field
  ;; 2. Check schema-type
  ;; 3. Check hidden? flag
  ;; 4. Return entity type keyword
  )
```

**Dispatch Logic:**

| Entity         | Dispatch Key            | Schema                                                                            |
| -------------- | ----------------------- | --------------------------------------------------------------------------------- |
| Regular block  | `:block`                | `[:map [:block/uuid :uuid] [:block/title :string] ...]`                           |
| Class/tag page | `:class`                | `[:map [:db/ident :keyword] [:logseq.property.class/extends [:vector :ref]] ...]` |
| Property page  | `:property`             | `[:map [:db/ident :keyword] [:logseq.property/type :keyword] ...]`                |
| Property value | `:property-value-block` | `[:map [:logseq.property/schema-type :keyword] ...]`                              |

### Property Type Enforcement

**Built-in Property Types:**

```clojure
(def property-types
  [:default :number :url :date :node :page :checkbox
   :coll :icon :property :class :query :external-link :other])
```

**Validation Examples:**

```clojure
;; Property: status (type :default)
(validate-property-value db :logseq.property/status "pending")
;; ✅ Valid (string)

(validate-property-value db :logseq.property/status 123)
;; ❌ Invalid: Expected string, got number

;; Property: due-date (type :date)
(validate-property-value db :logseq.property.task/due-date #inst "2025-01-25")
;; ✅ Valid (inst)

;; Property: tags (type :node, cardinality :many)
(validate-property-value db :logseq.property/tags [#:db{:id 123} #:db{:id 456}])
;; ✅ Valid (vector of refs)

(validate-property-value db :logseq.property/tags #:db{:id 123})
;; ❌ Invalid: Expected vector (cardinality :many)
```

---

## Integrity Constraints

### Reference Integrity

**Orphaned Reference Prevention:**

**Location:** `deps/outliner/src/logseq/outliner/core.cljs:69-95`

```clojure
(defn remove-orphaned-page-refs!
  [db block-entity txs-state old-refs new-refs]
  ;; 1. Find refs removed from block
  (let [removed-refs (set/difference (set old-refs) (set new-refs))]

    ;; 2. Check if pages now orphaned (no other refs)
    (let [orphaned-pages (filter-orphaned-pages db removed-refs)]

      ;; 3. Retract orphaned pages
      (when (seq orphaned-pages)
        (swap! txs-state into
          (mapv #(vector :db/retractEntity (:db/id %)) orphaned-pages))))))
```

**Example:**

```
Before:                   After (remove ref):
- Block A                 - Block A
  refs: [[Page X]]          refs: []
- Page X                  - Page X (orphaned → deleted)
  refs-to: [Block A]        refs-to: []
```

### Parent-Child Integrity

**Circular Reference Prevention:**

**Location:** `deps/outliner/src/logseq/outliner/validate.cljs:20-45`

```clojure
(defn validate-no-circular-refs
  [db blocks target]
  ;; Check if target is descendant of any block being moved
  (doseq [block blocks]
    (when (ancestor? db target block)
      (throw (ex-info "Circular reference detected"
                      {:block block :target target})))))

(defn ancestor?
  [db potential-ancestor block]
  ;; Recursive parent traversal
  (when-let [parent (:block/parent block)]
    (or (= (:db/id potential-ancestor) (:db/id parent))
        (ancestor? db potential-ancestor parent))))
```

**Example (Prevented):**

```
❌ Cannot move:
- Block A
  - Block B
    - Block C  (trying to move A into C → circular!)
```

### Cardinality Enforcement

**DataScript Cardinality:**

```clojure
{:block/parent {:db/cardinality :db.cardinality/one}    ; Single parent
 :block/tags {:db/cardinality :db.cardinality/many}}   ; Multiple tags
```

**Validation:**

```clojure
;; ✅ Valid: Single parent
{:block/parent #:db{:id 123}}

;; ❌ Invalid: Multiple parents
{:block/parent [#:db{:id 123} #:db{:id 456}]}

;; ✅ Valid: Multiple tags
{:block/tags [#:db{:id 789} #:db{:id 321}]}

;; ❌ Invalid: Single tag (should be vector)
{:block/tags #:db{:id 789}}
```

---

## Migration Safety

### Migration Validation Workflow

**Location:** `src/main/frontend/worker/db/migrate.cljs`

```
1. Check Schema Version
   ↓
2. Find Required Migrations (current → target)
   ↓
3. Validate Pre-Conditions
   ↓
4. Apply Migrations Sequentially
   ↓
5. Validate Post-Migration (full graph)
   ↓
6. Update Schema Version
   ↓
7. Persist to SQLite
```

### Pre-Migration Validation

```clojure
(defn validate-pre-migration
  [db from-version to-version]
  ;; 1. Check schema version format
  (assert (valid-version? from-version))

  ;; 2. Ensure migrations exist
  (assert (migration-path-exists? from-version to-version))

  ;; 3. Validate current graph state
  (when-let [errors (validate/validate-db db)]
    (throw (ex-info "Cannot migrate invalid graph" {:errors errors}))))
```

### Post-Migration Validation

```clojure
(defn validate-post-migration
  [db to-version]
  ;; 1. Validate schema version updated
  (assert (= to-version (get-schema-version db)))

  ;; 2. Validate full graph compliance
  (when-let [errors (validate/validate-db db :full-graph)]
    (throw (ex-info "Migration produced invalid graph" {:errors errors})))

  ;; 3. Check entity counts (sanity)
  (assert (pos? (count-entities db :block))))
```

### Migration Failure Recovery

**Automatic Rollback:**

**Location:** `src/main/frontend/worker/db/migrate.cljs:150-180`

```clojure
(defn apply-migrations!
  [db from-version to-version]
  ;; Create transaction scope
  (try
    ;; 1. Begin transaction
    (let [tx-id (begin-transaction! db)]

      ;; 2. Apply migrations
      (doseq [migration (get-migrations from-version to-version)]
        (apply-migration! db migration))

      ;; 3. Validate result
      (validate-post-migration db to-version)

      ;; 4. Commit if valid
      (commit-transaction! db tx-id))

    ;; Rollback on any error
    (catch Exception e
      (rollback-transaction! db)
      (throw (ex-info "Migration failed - rolled back" {:error e})))))
```

---

## Failure Recovery

### Common Failure Scenarios

**1. Invalid Graph Export Block**

**Problem:** Graph with validation errors cannot be exported (backup blocked)

**Detection:**

```clojure
(defn can-export?
  [db]
  ;; Pre-export validation
  (nil? (validate/validate-db db :pre-export)))
```

**Recovery:**

```clojure
;; Option 1: Identify invalid entities
(defn find-invalid-entities
  [db]
  (let [all-entities (d/q '[:find [?e ...] :where [?e :block/uuid]] db)]
    (filter #(not (validate/valid-entity? db %)) all-entities)))

;; Option 2: Force export with warnings (unsafe)
(defn force-export!
  [db]
  ;; Skip validation (data may be corrupt)
  (export/export-db db {:skip-validation? true}))
```

**2. Migration Cascade Failure**

**Problem:** Failed migration blocks all future schema updates

**Detection:**

```clojure
(defn migration-blocked?
  [db]
  (let [current-version (get-schema-version db)
        target-version (get-app-schema-version)]
    (and (< current-version target-version)
         (not (can-migrate? db current-version target-version)))))
```

**Recovery:**

```clojure
;; Option 1: Retry migration with fixes
(defn retry-migration!
  [db from-version to-version]
  ;; 1. Fix known issues
  (fix-common-migration-issues! db)

  ;; 2. Retry migration
  (apply-migrations! db from-version to-version))

;; Option 2: Manual version bump (emergency only)
(defn force-version-bump!
  [db version]
  ;; WARNING: May cause data corruption
  (d/transact! db [{:db/ident :logseq.property/schema-version
                    :logseq.property/schema-version version}]))
```

**3. Version Incompatibility Lock-Out**

**Problem:** Graph created with future schema version cannot be opened

**Detection:**

```clojure
(defn version-locked?
  [graph-version app-version]
  (> graph-version app-version))
```

**Recovery:**

```clojure
;; Only solution: Update Logseq to newer version
;; OR: Downgrade graph schema (risky, not recommended)
(defn downgrade-schema!
  [db target-version]
  ;; WARNING: May lose data
  (d/transact! db [{:db/ident :logseq.property/schema-version
                    :logseq.property/schema-version target-version}]))
```

---

## Developer Guide

### Implementing Validation Rules

**Custom Entity Validation:**

```clojure
(defn validate-task-block
  [db block]
  ;; Custom business logic
  (let [errors []]
    ;; Check 1: Task must have due-date or priority
    (when-not (or (:logseq.property.task/due-date block)
                  (:logseq.property/priority block))
      (conj errors "Task must have due-date or priority"))

    ;; Check 2: High priority tasks must have due-date
    (when (and (= (:logseq.property/priority block) "high")
               (not (:logseq.property.task/due-date block)))
      (conj errors "High priority task must have due-date"))

    ;; Return errors or nil
    (when (seq errors) errors)))
```

### Plugin API for Validation

**Check if Entity Valid:**

```typescript
// Via @logseq/libs (planned)
const valid = await logseq.DB.validateBlock({
  uuid: blockUuid,
  content: 'New content',
  properties: { status: 'done' },
})

if (!valid.success) {
  console.error('Validation errors:', valid.errors)
}
```

### Best Practices

**1. Validate Early**

```clojure
;; ❌ Bad: Validate after transaction
(d/transact! db tx-data)
(when-let [errors (validate/validate-tx-data db tx-data)]
  (println "Oops, invalid!"))

;; ✅ Good: Validate before transaction
(when-let [errors (validate/validate-tx-data db tx-data)]
  (throw (ex-info "Invalid transaction" {:errors errors})))
(d/transact! db tx-data)
```

**2. Provide Actionable Errors**

```clojure
;; ❌ Bad: Generic error
{:error "Invalid block"}

;; ✅ Good: Specific error with context
{:error "Block missing required field"
 :field :block/parent
 :block-uuid "abc-123"
 :suggestion "Set :block/parent to parent block or page reference"}
```

**3. Validate Incrementally**

```clojure
;; For large transactions, validate in chunks
(defn validate-large-tx
  [db tx-data]
  (doseq [chunk (partition 100 tx-data)]
    (when-let [errors (validate/validate-tx-data db chunk)]
      (throw (ex-info "Chunk validation failed" {:errors errors})))))
```

---

## Next Steps

**Continue Learning:**

1. **[Schema System](./schema-system.md)** - Data model details
2. **[Block Editor Architecture](./block-editor.md)** - Transaction flow
3. **[RTC Collaboration](../../tutorials/collaboration/rtc-setup.md)** - Multi-client validation
4. **[Complete Investigation Reports](../../../dz/docs/07-investigations/schema-integrity-08-index.md)** - 8-part deep dive

**For Contributors:**

- [Validation Source Code](../../../deps/db/src/logseq/db/frontend/validate.cljs)
- [Malli Schema Definitions](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)
- [Migration System](../../../src/main/frontend/worker/db/migrate.cljs)
- [PR Review Standards](../../../dz/docs/07-investigations/schema-integrity-07-pr-review-standards.md)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
