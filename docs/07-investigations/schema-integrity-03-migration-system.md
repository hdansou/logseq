# Schema Changes & Data Integrity Investigation

## Part 3: Migration System Deep Dive

---

## Table of Contents

1. [Migration Architecture](#migration-architecture)
2. [Migration Registry](#migration-registry)
3. [Migration Types](#migration-types)
4. [Migration Execution Flow](#migration-execution-flow)
5. [Migration Implementation Examples](#migration-implementation-examples)
6. [Error Handling](#error-handling)
7. [Testing Strategy](#testing-strategy)

---

## Migration Architecture

### Overview

The migration system upgrades graphs from older schema versions to newer ones. It runs automatically when a graph is opened with a newer client version.

**Key File:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs)

### Design Principles

1. **Sequential Execution:** Migrations run in order from current version to target version
2. **Transactional:** Each migration runs in a transaction
3. **Logged:** All migrations are logged for debugging
4. **Version-Tracked:** Graph stores its current schema version
5. **No Rollback:** Failed migrations cannot be automatically reversed

### Migration Trigger

```clojure
(defn migrate-if-needed
  "Check if migration is needed and run migrations"
  [conn]
  (let [current-version (get-schema-version conn)
        target-version db-schema/schema-version]
    (when (< (db-schema/compare-schema-version current-version target-version) 0)
      (migrate conn current-version target-version))))
```

**Trigger Points:**

- Graph open in desktop app
- Graph open in mobile app
- After RTC download
- Manual migration (dev tools)

---

## Migration Registry

### Schema Version to Updates Map

**Location:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L177-L197)

```clojure
(def schema-version->updates
  "Map of schema version to list of migration updates"
  {"65.7"  []
   "65.8"  [{:type :properties
             :properties {...}}]
   "65.9"  [{:type :rename-db-idents
             :renames {...}}]
   "65.10" [{:type :fix
             :fn fix-data-consistency}]
   "65.11" [{:type :classes
             :classes {...}}]
   "65.12" [{:type :properties
             :properties {...}}]
   "65.13" []
   "65.14" [{:type :fix
             :fn cleanup-orphaned-refs}]
   "65.15" [{:type :properties
             :properties {...}}]
   "65.16" [{:type :properties
             :properties {...}}
            {:type :classes
             :classes {...}}
            {:type :fix
             :fn update-timestamps}]})
```

### Registry Structure

Each version maps to a vector of migration operations:

```clojure
{version-string [{:type migration-type
                  ...type-specific-keys...}
                 {:type another-migration-type
                  ...}]}
```

### Version Ordering

```clojure
(defn get-migration-path
  "Get ordered list of versions to migrate through"
  [from-version to-version]
  (let [all-versions (sort-by parse-version (keys schema-version->updates))
        from-idx (.indexOf all-versions (version->str from-version))
        to-idx (.indexOf all-versions (version->str to-version))]
    (subvec all-versions (inc from-idx) (inc to-idx))))

;; Example: Migrating from 65.7 to 65.16
;; Path: ["65.8" "65.9" "65.10" "65.11" "65.12" "65.13" "65.14" "65.15" "65.16"]
```

---

## Migration Types

### Type 1: Properties Migration

**Purpose:** Add new properties to the schema

```clojure
{:type :properties
 :properties {:block/priority
              {:schema {:type :number
                        :cardinality :one
                        :default 0}}
              :block/due-date
              {:schema {:type :date
                        :cardinality :one}}}}
```

**Actions:**

1. Create property entities in database
2. Update DataScript schema
3. Update Malli schema registry
4. Optionally populate default values

**Example Implementation:**

```clojure
(defn migrate-properties
  [conn {:keys [properties]}]
  (let [property-tx-data (for [[prop-key prop-def] properties]
                          {:db/ident (keyword "db.schema" (name prop-key))
                           :db.property/schema (:schema prop-def)
                           :block/original-name (name prop-key)
                           :block/name (str/lower-case (name prop-key))})]
    (d/transact! conn property-tx-data)))
```

### Type 2: Classes Migration

**Purpose:** Add new tag/class definitions

```clojure
{:type :classes
 :classes {:task
           {:schema-properties [:block/priority :block/due-date]
            :extends [:item]}
           :urgent-task
           {:schema-properties [:block/escalation-level]
            :extends [:task]}}}
```

**Actions:**

1. Create class entities
2. Link to schema properties
3. Set up inheritance (`:class/extends`)
4. Apply to existing blocks if needed

**Example Implementation:**

```clojure
(defn migrate-classes
  [conn {:keys [classes]}]
  (let [class-tx-data (for [[class-key class-def] classes]
                       {:db/ident (keyword "db.ident" (str "block.type." (name class-key)))
                        :block/original-name (name class-key)
                        :block/name (str/lower-case (name class-key))
                        :class/schema-properties (map resolve-property (:schema-properties class-def))
                        :class/extends (map resolve-class (:extends class-def))})]
    (d/transact! conn class-tx-data)))
```

### Type 3: Rename Migration

**Purpose:** Rename attributes while preserving data

```clojure
{:type :rename-db-idents
 :renames {:block/old-name :block/new-name
           :block/status :block/workflow-state}}
```

**Actions:**

1. Create new attribute with copied values
2. Delete old attribute
3. Update references in code
4. Update Malli schemas

**Example Implementation:**

```clojure
(defn migrate-rename
  [conn {:keys [renames]}]
  (doseq [[old-attr new-attr] renames]
    (let [entities (d/q '[:find ?e ?v
                         :in $ ?attr
                         :where [?e ?attr ?v]]
                       @conn old-attr)]
      ;; Copy values to new attribute
      (d/transact! conn
                  (for [[e v] entities]
                    {:db/id e
                     new-attr v}))
      ;; Remove old attribute
      (d/transact! conn
                  (for [[e _] entities]
                    [:db/retract e old-attr])))))
```

### Type 4: Fix Migration

**Purpose:** Transform or repair data

```clojure
{:type :fix
 :fn (fn [conn]
       (let [broken-entities (find-broken-entities @conn)]
         (repair-entities conn broken-entities)))}
```

**Actions:**

1. Query for entities needing repair
2. Transform data
3. Validate results
4. Commit transaction

**Example Implementation:**

```clojure
(defn fix-orphaned-refs
  "Remove references to deleted entities"
  [conn]
  (let [all-eids (set (d/q '[:find [?e ...]
                             :where [?e :block/uuid]]
                          @conn))
        ref-attrs [:block/parent :block/left :block/tags]
        orphaned (for [attr ref-attrs
                       :let [refs (d/q '[:find ?e ?ref
                                        :in $ ?attr
                                        :where [?e ?attr ?ref]]
                                      @conn attr)]
                       [e ref] refs
                       :when (not (contains? all-eids ref))]
                   [:db/retract e attr ref])]
    (when (seq orphaned)
      (d/transact! conn orphaned))))
```

---

## Migration Execution Flow

### Main Migration Function

**Location:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L329-L366)

```clojure
(defn migrate
  "Run all migrations from current-version to target-version"
  [conn current-version target-version]
  (let [migration-path (get-migration-path current-version target-version)]
    (doseq [version migration-path]
      (println "Migrating to version:" version)
      (let [updates (get schema-version->updates version)]
        (doseq [update updates]
          (try
            (migrate-one conn update)
            (catch :default e
              (println "Migration failed:" version update)
              (throw e))))
        ;; Update schema version in graph
        (update-schema-version! conn (parse-schema-version version))))))
```

### Step-by-Step Process

1. **Version Comparison**

   ```clojure
   (when (< (compare-schema-version current target) 0)
     (migrate conn current target))
   ```

2. **Path Calculation**

   ```clojure
   ;; From 65.7 to 65.16 => ["65.8" "65.9" ... "65.16"]
   (get-migration-path current-version target-version)
   ```

3. **Sequential Execution**

   ```clojure
   (doseq [version migration-path]
     (run-migrations-for-version conn version))
   ```

4. **Type Dispatch**

   ```clojure
   (defn migrate-one [conn update]
     (case (:type update)
       :properties (migrate-properties conn update)
       :classes    (migrate-classes conn update)
       :rename-db-idents (migrate-rename conn update)
       :fix        ((:fn update) conn)))
   ```

5. **Version Update**
   ```clojure
   (defn update-schema-version! [conn new-version]
     (d/transact! conn [{:db/ident :logseq/schema-version
                        :logseq/version new-version}]))
   ```

### Transaction Boundaries

```clojure
(defn migrate-one
  "Execute one migration operation"
  [conn update]
  ;; Each migration runs in its own transaction
  (d/transact! conn
    (migration-tx-data conn update)))
```

**Critical:** Each migration is a separate transaction. If one fails, previous migrations are committed but subsequent ones don't run.

---

## Migration Implementation Examples

### Example 1: Adding Task Priority Property (65.15)

```clojure
;; Migration definition
{"65.15"
 [{:type :properties
   :properties {:block/priority
                {:schema {:type :number
                          :cardinality :one}
                 :default 0}}}]}

;; Implementation
(defn migrate-properties-65-15 [conn properties]
  ;; Create property entity
  (d/transact! conn
    [{:db/ident :db.schema/block.priority
      :block/uuid (d/squuid)
      :block/original-name "priority"
      :block/name "priority"
      :db.property/schema {:type :number
                          :cardinality :one}}])

  ;; Optionally set defaults on existing blocks
  (let [all-blocks (d/q '[:find [?e ...]
                         :where [?e :block/uuid]]
                       @conn)]
    (d/transact! conn
      (for [e all-blocks]
        {:db/id e
         :block/priority 0}))))
```

### Example 2: Renaming Status to Workflow State (65.9)

```clojure
;; Migration definition
{"65.9"
 [{:type :rename-db-idents
   :renames {:block/status :block/workflow-state}}]}

;; Implementation automatically handles:
;; 1. Finding all entities with :block/status
;; 2. Copying values to :block/workflow-state
;; 3. Removing :block/status attribute
;; 4. Updating schema definitions
```

### Example 3: Fixing Timestamp Format (65.14)

```clojure
;; Migration definition
{"65.14"
 [{:type :fix
   :fn fix-timestamp-format}]}

;; Implementation
(defn fix-timestamp-format [conn]
  (let [blocks-with-timestamps
        (d/q '[:find ?e ?created ?updated
              :where
              [?e :block/created-at ?created]
              [?e :block/updated-at ?updated]]
            @conn)]
    (d/transact! conn
      (for [[e created updated] blocks-with-timestamps
            :let [fixed-created (normalize-timestamp created)
                  fixed-updated (normalize-timestamp updated)]]
        {:db/id e
         :block/created-at fixed-created
         :block/updated-at fixed-updated}))))

(defn normalize-timestamp [ts]
  ;; Convert from various formats to standard Unix timestamp
  (cond
    (string? ts) (js/Date.parse ts)
    (> ts 1e12)  (/ ts 1000)  ; milliseconds to seconds
    :else        ts))
```

---

## Error Handling

### Migration Failures

**What Happens:**

1. Migration throws exception
2. Current transaction rolls back
3. Previous migrations remain committed
4. Schema version not updated
5. Error logged to console
6. Graph remains at partially migrated state

**Critical Issue:** No automatic rollback of previous migrations in the path.

### Failure Scenario Example

```
Starting: v65.7
Migrate to 65.8: ✅ Success (committed)
Migrate to 65.9: ✅ Success (committed)
Migrate to 65.10: ❌ FAILED (rolled back)
Migrate to 65.11: ⏭️ Skipped
Result: Graph stuck at v65.9
```

### Error Recovery

**Manual Recovery Required:**

1. Identify failed migration
2. Fix bug in migration code
3. Deploy fixed version
4. User reopens graph (retry migration)

**Alternative:**

1. Export graph (if validation passes)
2. Rollback to older app version
3. Report bug
4. Wait for fix

### Validation After Migration

```clojure
(defn migrate-with-validation [conn current target]
  (try
    (migrate conn current target)
    ;; Validate graph after all migrations
    (when-let [errors (validate-db! @conn)]
      (throw (ex-info "Migration produced invalid graph"
                     {:errors errors})))
    (catch :default e
      (log/error "Migration failed" e)
      (throw e))))
```

---

## Testing Strategy

### Unit Tests

**Location:** `src/test/frontend/worker/db/migrate_test.cljs`

```clojure
(deftest migrate-properties-test
  (let [conn (create-test-db)
        initial-version {:major 65 :minor 14}
        target-version {:major 65 :minor 15}]

    ;; Set initial version
    (set-schema-version! conn initial-version)

    ;; Run migration
    (migrate conn initial-version target-version)

    ;; Verify results
    (is (= target-version (get-schema-version conn)))
    (is (entity-exists? @conn :db.schema/block.priority))
    (is (= :number (get-property-type @conn :block/priority)))))
```

### Integration Tests

```clojure
(deftest full-migration-path-test
  (let [conn (create-test-db-v65-7)]

    ;; Load sample data
    (load-test-data! conn)

    ;; Migrate through all versions
    (migrate conn {:major 65 :minor 7} {:major 65 :minor 16})

    ;; Verify graph integrity
    (is (nil? (validate-db! @conn)))
    (is (all-refs-valid? @conn))
    (is (all-properties-exist? @conn))))
```

### Regression Tests

```clojure
(deftest migration-idempotency-test
  (let [conn (create-test-db-v65-15)]

    ;; Run migration once
    (migrate conn {:major 65 :minor 15} {:major 65 :minor 16})
    (let [state-1 (export-graph @conn)]

      ;; Run migration again
      (migrate conn {:major 65 :minor 15} {:major 65 :minor 16})
      (let [state-2 (export-graph @conn)]

        ;; Should be identical
        (is (= state-1 state-2))))))
```

---

## Migration Best Practices

### 1. Design Migrations Defensively

```clojure
;; BAD: Assumes data exists
(defn migrate-bad [conn]
  (let [blocks (d/q '[:find [?e ...] :where [?e :block/title]] @conn)]
    (d/transact! conn
      (for [e blocks]
        {:db/id e
         :block/priority 0}))))

;; GOOD: Checks for existing values
(defn migrate-good [conn]
  (let [blocks (d/q '[:find [?e ...]
                     :where
                     [?e :block/title]
                     (not [?e :block/priority])]
                   @conn)]
    (when (seq blocks)
      (d/transact! conn
        (for [e blocks]
          {:db/id e
           :block/priority 0})))))
```

### 2. Make Migrations Idempotent

```clojure
(defn migrate-idempotent [conn]
  ;; Check if migration already applied
  (when-not (property-exists? @conn :block/priority)
    (create-property! conn :block/priority {...})))
```

### 3. Log Migration Progress

```clojure
(defn migrate-with-logging [conn version]
  (log/info "Starting migration to" version)
  (let [start-time (js/Date.now)
        result (migrate-version conn version)
        duration (- (js/Date.now) start-time)]
    (log/info "Migration completed in" duration "ms")
    result))
```

### 4. Validate Before and After

```clojure
(defn migrate-safely [conn current target]
  ;; Pre-migration validation
  (when-let [errors (validate-db! @conn)]
    (throw (ex-info "Graph invalid before migration" {:errors errors})))

  ;; Run migration
  (migrate conn current target)

  ;; Post-migration validation
  (when-let [errors (validate-db! @conn)]
    (throw (ex-info "Migration produced invalid graph" {:errors errors}))))
```

### 5. Provide Rollback Instructions

```clojure
;; In migration documentation:
;;
;; Migration 65.15: Add priority property
;;
;; Rollback instructions:
;; 1. Export graph before migration
;; 2. If migration fails:
;;    - Close graph
;;    - Restore from export
;;    - Use older app version (< 65.15)
```

---

## Key Takeaways

1. **Sequential & Transactional:** Migrations run in order, one transaction per migration
2. **No Auto-Rollback:** Failed migrations leave graph partially migrated
3. **Four Migration Types:** Properties, Classes, Rename, Fix
4. **Version Registry:** All migrations defined in `schema-version->updates`
5. **Defensive Design:** Check for existing values, handle edge cases
6. **Idempotency:** Migrations should be safe to run multiple times
7. **Validation:** Test before and after migration
8. **Recovery:** Export graphs before major version upgrades

---

## Next Steps

Proceed to **Part 4: Import/Export & Validation Analysis** for examination of how schema changes affect data portability and integrity checks.
