# Schema Migration Best Practices

> **Last revised:** 2026-03-10

**Document Type:** Process Guide
**Last Updated:** December 28, 2025
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

This guide provides comprehensive best practices for managing schema migrations in Logseq. Schema migrations are necessary when:

- Adding new built-in properties
- Modifying existing property definitions
- Changing schema structure or constraints
- Updating validation rules
- Modifying bootstrap initialization

**Critical Rule:** Every schema change that affects stored data MUST include a migration entry.

---

## Table of Contents

1. [Adding New Properties](#adding-new-properties)
2. [Changing Property Types](#changing-property-types)
3. [Property Renaming](#property-renaming)
4. [Schema Version Management](#schema-version-management)
5. [Migration Patterns](#migration-patterns)
6. [Bootstrap Considerations](#bootstrap-considerations)
7. [Testing Requirements](#testing-requirements)
8. [Common Mistakes](#common-mistakes)

---

## Adding New Properties

### Required Steps

Every new built-in property requires updates in multiple locations:

#### Step 1: Define Property

**Location:** [deps/db/src/logseq/db/frontend/property.cljs](deps/db/src/logseq/db/frontend/property.cljs)

**Add to `built-in-properties` map:**

```clojure
;; Example: Adding a new asset property
{:db/ident :logseq.property.asset/my-new-property
 :title "My New Property"
 :schema {:type :default
          :cardinality :one
          :public? true
          :hide? false
          :view-context :page
          :ui-position :properties}
 :queryable? true
 :rtc {:rtc/ignore-attr-when-init-upload true
       :rtc/ignore-attr-when-init-download true
       :rtc/ignore-attr-when-syncing true}}
```

**Key Decisions:**

- **Namespace:** Use appropriate prefix (`logseq.property.*`, `logseq.property.asset.*`, etc.)
- **Type:** Choose from `:default`, `:number`, `:date`, `:datetime`, `:checkbox`, `:url`, `:node`
- **Cardinality:** `:one` (single value) or `:many` (multiple values)
- **Visibility:** Set `public?` and `hide?` appropriately
- **RTC Behavior:** Explicitly configure sync behavior (see [RTC/Sync Considerations](./rtc-sync-schema-changes.md))

#### Step 2: Add Migration Entry

**Location:** [src/main/frontend/worker/db/migrate.cljs](src/main/frontend/worker/db/migrate.cljs)

**Add to `schema-version->updates` vector:**

```clojure
(def schema-version->updates
  [;; ... existing migrations ...
   ["65.17" {:properties [:logseq.property.asset/my-new-property]}]])
```

**Migration Entry Structure:**

```clojure
["VERSION" MIGRATION-MAP]

;; MIGRATION-MAP can have keys:
;; :properties - Vector of property idents to add
;; :classes - Vector of class idents to add
;; :rename-db-idents - Map of old ident to new ident
;; :fix - Function to apply custom data transformations
```

#### Step 3: Bump Schema Version

**Location:** [deps/db/src/logseq/db/frontend/schema.cljs](deps/db/src/logseq/db/frontend/schema.cljs)

**Update version constant:**

```clojure
;; Before
(def version (parse-schema-version "65.16"))

;; After
(def version (parse-schema-version "65.17"))
```

**Version Numbering:**

- **Minor bump** (65.16 → 65.17): Adding properties, non-breaking changes
- **Major bump** (65.x → 66.0): Breaking changes, major restructuring

#### Step 4: Add Validation Rules (If Needed)

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)

**If property needs custom validation:**

```clojure
;; Add to appropriate entity schema or property value schema
(defmethod property-value-schema :logseq.property.asset/my-new-property
  [_property db property-value]
  [:and
   [:string {:error/message "Must be a string"}]
   [:re {:error/message "Must match pattern"} #"^[a-z]+$"]])
```

#### Step 5: Update Tests

**Add migration test:**

```clojure
(deftest test-migration-65-17
  (let [db (create-test-db-with-schema "65.16")
        migrated-db (migrate-db-graph db "65.17")]
    (testing "New property exists after migration"
      (is (d/entity migrated-db :logseq.property.asset/my-new-property)))
    (testing "Property has correct schema"
      (is (= :default (get-in (d/entity migrated-db :logseq.property.asset/my-new-property)
                               [:block/schema :type]))))))
```

### Property Definition Template

```clojure
{:db/ident :logseq.property.NAMESPACE/property-name
 :title "Human Readable Name"

 ;; Optional: If DataScript attribute differs from ident
 :attribute :block/attribute-name

 :schema {:type :default  ; or :number, :date, :datetime, :checkbox, :url, :node
          :cardinality :one  ; or :many
          :public? true  ; User-visible in UI
          :hide? false  ; Hidden from property lists
          :view-context :page  ; Where to show: :page, :block, :class, :property, :never
          :ui-position :properties}  ; UI placement

 :queryable? true  ; Usable in query builder

 ;; Optional: For choice properties
 :closed-values [{:uuid (d/squuid)
                  :value "Option 1"}
                 {:uuid (d/squuid)
                  :value "Option 2"}]

 ;; Optional: Meta-properties
 :properties {:logseq.property/description "Property description"
              :logseq.property/schema-classes #{:logseq.class/SomeClass}}

 ;; Required: RTC sync configuration
 :rtc {:rtc/ignore-attr-when-init-upload true
       :rtc/ignore-attr-when-init-download true
       :rtc/ignore-attr-when-syncing true}}
```

### Property Naming Conventions

**Namespace Prefixes:**

- `logseq.property.*` - Core application properties
- `logseq.property.asset.*` - Asset-related properties
- `logseq.property.embedding.*` - Embedding/AI properties
- `logseq.property.tldraw.*` - Whiteboard properties
- `user.property.*` - User-defined properties
- `plugin.property.*` - Plugin-defined properties

**Naming Style:**

- Use kebab-case: `:my-property-name`
- Be descriptive but concise
- Include namespace context
- Avoid abbreviations unless standard

**Examples:**

- ✅ `:logseq.property.asset/external-file-name`
- ✅ `:logseq.property/background-color`
- ❌ `:logseq.property/bgc` (too abbreviated)
- ❌ `:my-prop` (missing namespace)

---

## Changing Property Types

### ⚠️ Critical Warning

**DO NOT change the type of an existing property that has user data.** This will cause:

- Data loss during migration
- Validation failures
- Type errors when accessing values
- Sync conflicts in RTC

### Correct Approach: Create New Property

Instead of changing type:

1. **Create new property with new type**
2. **Add migration to transform data**
3. **Deprecate old property**
4. **Update UI/query references**

### Example: Changing Text Property to Number

```clojure
;; Step 1: Add new property with correct type
{:db/ident :logseq.property/my-property-v2
 :title "My Property"
 :schema {:type :number
          :cardinality :one
          :public? true}
 :rtc {:rtc/ignore-attr-when-syncing false}}

;; Step 2: Add migration with fix function
["65.17" {:properties [:logseq.property/my-property-v2]
          :fix migrate-my-property-to-number}]

;; Step 3: Implement migration function
(defn migrate-my-property-to-number
  [db]
  (let [old-prop-ident :logseq.property/my-property
        new-prop-ident :logseq.property/my-property-v2
        old-values (q '[:find ?e ?v
                        :where
                        [?e :logseq.property/my-property ?v]]
                      db)]
    (->> old-values
         (keep (fn [[entity-id old-value]]
                 (when-let [parsed (parse-number old-value)]
                   {:db/id entity-id
                    new-prop-ident parsed
                    old-prop-ident nil})))  ; Retract old value
         vec)))

;; Step 4: Mark old property as deprecated
{:db/ident :logseq.property/my-property
 :title "My Property (Deprecated)"
 :schema {:type :default
          :cardinality :one
          :public? false  ; Hide from UI
          :deprecated true}
 :properties {:logseq.property/description "Deprecated. Use :logseq.property/my-property-v2"}}
```

### Type Change Checklist

- [ ] Create new property with new type and versioned name
- [ ] Add migration entry with `:fix` function
- [ ] Implement data transformation function
- [ ] Handle parsing errors gracefully (log, skip, or use default)
- [ ] Mark old property as deprecated
- [ ] Update all UI components referencing property
- [ ] Update all query code
- [ ] Test migration on graphs with actual data
- [ ] Document breaking change in release notes
- [ ] Plan deprecation timeline (when to remove old property)

---

## Property Renaming

Renaming properties requires special handling to maintain data continuity.

### Using `:rename-db-idents`

```clojure
;; Example: Renaming a property
["65.15" {:rename-db-idents
          {:logseq.property.tldraw/camera :logseq.property.whiteboard/camera
           :logseq.property.tldraw/zoom :logseq.property.whiteboard/zoom}}]
```

### Renaming Process

1. **Add new property definition** with new ident
2. **Add migration entry** with `:rename-db-idents` map
3. **Remove old property definition** from `built-in-properties`
4. **Update all code references** to use new ident
5. **Bump schema version**

### Rename Migration Template

```clojure
;; Step 1: Define new property
{:db/ident :logseq.property.new-namespace/property-name
 :title "Property Name"
 :schema {:type :default
          :cardinality :one
          :public? true}
 :rtc {:rtc/ignore-attr-when-syncing false}}

;; Step 2: Add migration
["65.X" {:rename-db-idents
         {:logseq.property.old-namespace/property-name
          :logseq.property.new-namespace/property-name}}]

;; Step 3: Bump version
(def version (parse-schema-version "65.X"))
```

### Renaming Checklist

- [ ] Add new property with new ident
- [ ] Add `:rename-db-idents` migration entry
- [ ] Remove old property definition
- [ ] Search codebase for old ident references
- [ ] Update all namespaced keywords in code
- [ ] Update tests
- [ ] Update documentation
- [ ] Test migration path
- [ ] Verify export/import compatibility

---

## Schema Version Management

### Version Format

Schema versions use **major.minor** format:

- Format: `"MAJOR.MINOR"`
- Example: `"65.16"`

### When to Bump Versions

**Minor Version Bump (65.16 → 65.17):**

- Adding new properties
- Adding new validation rules
- Non-breaking schema additions
- Fix functions that don't change structure

**Major Version Bump (65.x → 66.0):**

- Removing properties (rare)
- Changing fundamental schema structure
- Breaking changes to entity model
- Incompatible with previous versions

### Version Comparison

**Location:** [deps/db/src/logseq/db/frontend/schema.cljs](deps/db/src/logseq/db/frontend/schema.cljs)

```clojure
(defn compatible-schema-version?
  "Check if graph schema version is compatible with app version"
  [graph-version app-version]
  (and graph-version
       app-version
       (= (major graph-version) (major app-version))
       (<= (minor graph-version) (minor app-version))))
```

**Compatibility Rules:**

- Same major version required
- Graph minor version ≤ app minor version
- App can read older minor versions (forward compatible)
- App cannot read newer minor versions (not backward compatible)

### Version Storage

Schema versions are stored in multiple key-value entities:

```clojure
;; Current graph schema version
:logseq.kv/schema-version "65.16"

;; Schema version when graph was first created
:logseq.kv/graph-initial-schema-version "65.10"

;; Remote schema version (for RTC)
:logseq.kv/remote-schema-version "65.16"
```

### Migration Triggers

Migrations run automatically when:

- Graph schema version < app schema version
- User opens graph in newer app version
- During graph import

**Migration Runner Location:** [src/main/frontend/worker/db/migrate.cljs](src/main/frontend/worker/db/migrate.cljs)

```clojure
(defn migrate-db-graph
  "Migrate database from its current schema version to target version"
  [conn repo target-version]
  ;; 1. Check current version
  ;; 2. Find applicable migrations
  ;; 3. Apply migrations sequentially
  ;; 4. Update schema version after each
  ;; 5. Validate after completion
  )
```

---

## Migration Patterns

### Pattern 1: Simple Property Addition

**Use when:** Adding new property that doesn't require data transformation

```clojure
;; Migration entry
["65.17" {:properties [:logseq.property/new-property]}]
```

**What it does:**

- Creates property page entity
- Adds property to `#Property` class
- Sets property as built-in
- No data transformation needed

### Pattern 2: Multiple Properties

**Use when:** Adding several related properties in one version

```clojure
["65.17" {:properties [:logseq.property/prop-one
                       :logseq.property/prop-two
                       :logseq.property/prop-three]}]
```

### Pattern 3: Property with Fix Function

**Use when:** Need to transform existing data or create initial values

```clojure
["65.17" {:properties [:logseq.property/new-property]
          :fix add-default-values-to-property}]

(defn add-default-values-to-property
  "Add default value to all pages missing the property"
  [db]
  (let [property-ident :logseq.property/new-property
        pages-without-prop (q '[:find [?e ...]
                                :where
                                [?e :block/type "page"]
                                (not [?e :logseq.property/new-property])]
                              db)]
    (mapv (fn [page-id]
            {:db/id page-id
             property-ident "default-value"})
          pages-without-prop)))
```

### Pattern 4: Pure Fix Function

**Use when:** Fixing data inconsistencies without adding properties

```clojure
["65.11" {:fix remove-block-path-refs}]

(defn remove-block-path-refs
  "Remove invalid :block/path-refs from entities"
  [db]
  (->> (d/datoms db :aevt :block/path-refs)
       (map (fn [datom]
              [:db/retract (:e datom) :block/path-refs (:v datom)]))
       vec))
```

### Pattern 5: Renaming Properties

**Use when:** Changing property ident without losing data

```clojure
["65.15" {:rename-db-idents
          {:old/property-ident :new/property-ident}}]
```

### Pattern 6: Complex Multi-Step Migration

**Use when:** Need multiple operations in sequence

```clojure
["65.17" {:properties [:logseq.property/new-property]
          :rename-db-idents {:old/ident :new/ident}
          :fix multi-step-migration}]

(defn multi-step-migration
  [db]
  (let [;; Step 1: Transform old data
        old-data-tx (transform-old-data db)
        ;; Step 2: Create new structures
        new-data-tx (create-new-structures db)
        ;; Step 3: Clean up
        cleanup-tx (cleanup-obsolete-data db)]
    ;; Combine all transaction data
    (concat old-data-tx new-data-tx cleanup-tx)))
```

### Fix Function Guidelines

**Function signature:**

```clojure
(defn migration-fix-fn
  "Description of what this migration does"
  [db]  ; db is current database value
  ;; Return transaction data (vector of maps/datoms)
  [{:db/id entity-id
    :attribute value}
   [:db/retract entity-id :attribute old-value]])
```

**Best Practices:**

- Return transaction data, don't call transact
- Handle nil/missing data gracefully
- Log warnings for problematic data
- Use query to find affected entities
- Test with realistic data volumes
- Consider performance (large graphs)
- Be idempotent (can run multiple times safely)

---

## Bootstrap Considerations

### What is Bootstrap?

Bootstrap is the initialization process that creates a new graph from scratch. It must resolve circular dependencies:

- Properties need to be tagged `#Property`
- `#Property` class needs properties
- Some properties need themselves as properties

### Bootstrap Order (Critical!)

**Location:** [deps/db/src/logseq/db/sqlite/create_graph.cljs](deps/db/src/logseq/db/sqlite/create_graph.cljs)

**Initialization sequence (DO NOT CHANGE ORDER!):**

```clojure
;; 1. Bootstrap class :db/idents only (no properties yet)
;;    - Root, Tag, Property, Template

;; 2. Key-value pairs
;;    - Schema version, DB type, metadata

;; 3. Bootstrap properties (4 properties)
;;    - :logseq.property/type
;;    - :logseq.property/hide?
;;    - :logseq.property/built-in?
;;    - :logseq.property/created-from-property

;; 4. All other properties (100+ built-in properties)

;; 5. Add bootstrap properties to themselves
;;    (Now safe because properties exist)

;; 6. Mark all properties as built-in

;; 7. Complete bootstrap classes
;;    (Add properties to classes)

;; 8. All other classes

;; 9. Initial files (config.edn, custom.css, custom.js)

;; 10. Built-in pages (Library, Contents, Quick Add)

;; 11. Hidden pages (Views, Favorites)
```

### Why Order Matters

**Problem:** Circular dependency

```
Property entity ─needs─> #Property tag ─needs─> properties ─needs─> Property entity
```

**Solution:** Multi-stage creation

1. Create Property class stub (no properties)
2. Create property entities (tag them #Property)
3. Add properties to Property class
4. Add properties to property entities themselves

### Bootstrap Classes

**The four bootstrap classes:**

```clojure
;; 1. Root - Base class for all classes
{:db/ident :logseq.class/Root
 :title "Root"}

;; 2. Tag - Base class for tags
{:db/ident :logseq.class/Tag
 :title "Tag"
 :extends #{:logseq.class/Root}}

;; 3. Property - Class for properties
{:db/ident :logseq.class/Property
 :title "Property"
 :extends #{:logseq.class/Tag}}

;; 4. Template - Class for templates
{:db/ident :logseq.class/Template
 :title "Template"
 :extends #{:logseq.class/Tag}}
```

### Bootstrap Properties

**The four properties needed for bootstrap:**

```clojure
;; 1. type - Property type
:logseq.property/type

;; 2. hide? - Hidden from UI
:logseq.property/hide?

;; 3. built-in? - Marks built-in properties
:logseq.property/built-in?

;; 4. created-from-property - Tracks property creation source
:logseq.property/created-from-property
```

### Modifying Bootstrap

**⚠️ EXTREME CAUTION REQUIRED**

Changes to bootstrap code are extremely risky:

- Can break graph creation completely
- Hard to debug (happens during initialization)
- Affects all new graphs
- Circular dependency issues are subtle

**If you must modify bootstrap:**

1. Understand current order completely
2. Diagram dependencies before changes
3. Add extensive tests
4. Test with clean graph creation
5. Test import of bootstrapped graph
6. Review with senior developer
7. Plan rollback strategy

**Common bootstrap failures:**

- "Property class not found" - Bootstrap classes not created first
- "Property not found" - Property used before creation
- "Circular reference error" - Dependency cycle not resolved

---

## Testing Requirements

Every migration must be tested before release.

### Required Tests

#### 1. Migration Path Test

```clojure
(deftest test-migration-from-previous-version
  (testing "Migrate from 65.16 to 65.17"
    (let [db (create-test-db-with-schema "65.16")
          migrated-db (migrate-db-graph db "65.17")]
      (is (= "65.17" (get-schema-version migrated-db)))
      (is (d/entity migrated-db :logseq.property/new-property))
      (validate-db! migrated-db))))  ; No validation errors
```

#### 2. Data Preservation Test

```clojure
(deftest test-migration-preserves-data
  (testing "User data preserved after migration"
    (let [db (create-test-db-with-data "65.16"
                                       {:pages 100
                                        :blocks 1000
                                        :properties 50})
          migrated-db (migrate-db-graph db "65.17")
          original-page-count (count-pages db)
          migrated-page-count (count-pages migrated-db)]
      (is (= original-page-count migrated-page-count))
      (is (no-data-loss? db migrated-db)))))
```

#### 3. Fix Function Test

```clojure
(deftest test-fix-function
  (testing "Fix function handles edge cases"
    (let [db (create-db-with-invalid-data)
          fixed-db (migrate-db-graph db "65.17")]
      (is (zero? (count-invalid-entities fixed-db)))
      (is (validate-db! fixed-db)))))
```

#### 4. Performance Test

```clojure
(deftest test-migration-performance
  (testing "Migration completes in reasonable time"
    (let [large-db (create-test-db-with-data "65.16"
                                             {:pages 10000
                                              :blocks 100000})
          start-time (js/Date.now)
          migrated-db (migrate-db-graph large-db "65.17")
          duration (- (js/Date.now) start-time)]
      (is (< duration 30000) "Migration should complete in < 30 seconds"))))
```

### Manual Testing Checklist

Before releasing schema change:

- [ ] Create new graph with new schema version
- [ ] Open existing graph with previous version (should auto-migrate)
- [ ] Verify no validation errors after migration
- [ ] Check all properties visible in UI
- [ ] Test property values can be set/read
- [ ] Export graph after migration
- [ ] Import exported graph in clean environment
- [ ] Test RTC sync between old and new versions
- [ ] Check large graph migration time
- [ ] Verify no console errors during migration

---

## Common Mistakes

### Mistake 1: Missing Migration Entry

**Symptom:** Validation errors after upgrade

**Code:**

```clojure
;; Added to built-in-properties
{:db/ident :logseq.property/new-prop ...}

;; ❌ FORGOT migration entry!
;; ❌ FORGOT schema version bump!
```

**Fix:**

```clojure
;; Add migration
["65.17" {:properties [:logseq.property/new-prop]}]

;; Bump version
(def version (parse-schema-version "65.17"))
```

### Mistake 2: Wrong Version in Migration

**Symptom:** Migration doesn't run

**Code:**

```clojure
(def version (parse-schema-version "65.17"))

;; ❌ Version mismatch!
["65.16" {:properties [:logseq.property/new-prop]}]
```

**Fix:**

```clojure
;; Match version
["65.17" {:properties [:logseq.property/new-prop]}]
```

### Mistake 3: Changing Existing Property Type

**Symptom:** Data loss, validation errors

**Code:**

```clojure
;; ❌ Changed type from :default to :number
{:db/ident :logseq.property/my-prop
 :schema {:type :number}}  ; Was :default
```

**Fix:**

```clojure
;; Create new property, migrate data
{:db/ident :logseq.property/my-prop-v2
 :schema {:type :number}}

["65.17" {:properties [:logseq.property/my-prop-v2]
          :fix migrate-my-prop-to-number}]
```

### Mistake 4: Missing RTC Configuration

**Symptom:** Property doesn't sync between devices

**Code:**

```clojure
;; ❌ No :rtc configuration
{:db/ident :logseq.property/new-prop
 :schema {:type :default}}
```

**Fix:**

```clojure
;; Always specify RTC behavior
{:db/ident :logseq.property/new-prop
 :schema {:type :default}
 :rtc {:rtc/ignore-attr-when-syncing false}}
```

### Mistake 5: Fix Function Calls Transact

**Symptom:** Migration fails or runs twice

**Code:**

```clojure
;; ❌ Fix function should not transact!
(defn bad-fix-fn [db]
  (d/transact! conn [...]))  ; WRONG!
```

**Fix:**

```clojure
;; Return transaction data
(defn good-fix-fn [db]
  [{:db/id entity-id
    :attribute value}])
```

### Mistake 6: Modifying Bootstrap Order

**Symptom:** New graphs fail to create

**Code:**

```clojure
;; ❌ Changed bootstrap order
;; Now creating properties before classes!
```

**Fix:**

```clojure
;; Restore original order (see Bootstrap Considerations)
;; Do not modify without extreme caution
```

### Mistake 7: No Tests for Migration

**Symptom:** Migration breaks in production

**Code:**

```clojure
;; ❌ No tests written
```

**Fix:**

```clojure
;; Add comprehensive tests
(deftest test-migration-65-17 ...)
```

---

## Quick Reference Checklist

Use this checklist for every schema change:

### Property Addition Checklist

- [ ] Add property to `built-in-properties` in [deps/db/src/logseq/db/frontend/property.cljs](deps/db/src/logseq/db/frontend/property.cljs)
- [ ] Add migration entry to `schema-version->updates` in [src/main/frontend/worker/db/migrate.cljs](src/main/frontend/worker/db/migrate.cljs)
- [ ] Bump `version` in [deps/db/src/logseq/db/frontend/schema.cljs](deps/db/src/logseq/db/frontend/schema.cljs)
- [ ] Version in migration matches version in schema.cljs
- [ ] Configure `:rtc` behavior explicitly
- [ ] Add validation rules if needed in [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)
- [ ] Write migration test
- [ ] Write property usage test
- [ ] Test migration on sample graph
- [ ] Test import/export compatibility
- [ ] Update documentation
- [ ] Review [PR Review Checklist](./schema-change-pr-checklist.md)

### Property Type Change Checklist

- [ ] Create new property with new type (versioned name)
- [ ] Add migration with `:fix` function
- [ ] Implement data transformation function
- [ ] Handle parsing errors gracefully
- [ ] Mark old property as deprecated
- [ ] Update UI component references
- [ ] Update query code
- [ ] Write transformation tests
- [ ] Test on graphs with real data
- [ ] Document breaking change
- [ ] Plan deprecation timeline

### Schema Version Bump Checklist

- [ ] Determine bump type (minor vs major)
- [ ] Update version in [deps/db/src/logseq/db/frontend/schema.cljs](deps/db/src/logseq/db/frontend/schema.cljs)
- [ ] Ensure all migrations have matching version
- [ ] Update CHANGELOG.md
- [ ] Test migration from previous versions
- [ ] Verify compatibility checks work correctly

---

## Related Documentation

- [Investigation Overview](../07-investigations/schema-evolution-and-data-migration.md)
- [Validation Guidelines](./validation-best-practices.md)
- [Import/Export Best Practices](./import-export-best-practices.md)
- [RTC/Sync Considerations](./rtc-sync-schema-changes.md)
- [PR Review Checklist](./schema-change-pr-checklist.md)
- [Testing Strategies](./schema-change-testing.md)
- [Database Schema Guide](../03-features/database-graphs/schema-guide.md)

---

## Changelog

| Date       | Changes                                         |
| ---------- | ----------------------------------------------- |
| 2025-12-28 | Initial version based on investigation findings |
