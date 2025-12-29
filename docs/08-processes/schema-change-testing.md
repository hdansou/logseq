# Schema Change Testing Strategies

**Document Type:** Testing Guide  
**Last Updated:** December 28, 2025  
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

Comprehensive testing is critical for schema changes to prevent data loss, validation errors, and sync conflicts. This guide provides testing strategies, patterns, and examples for all aspects of schema evolution.

**Key Principle:** Test migration paths, not just final state. Users upgrade from various versions.

---

## Table of Contents

1. [Testing Philosophy](#testing-philosophy)
2. [Test Environment Setup](#test-environment-setup)
3. [Unit Testing](#unit-testing)
4. [Migration Testing](#migration-testing)
5. [Validation Testing](#validation-testing)
6. [Import/Export Testing](#import-export-testing)
7. [RTC/Sync Testing](#rtcsync-testing)
8. [Performance Testing](#performance-testing)
9. [Integration Testing](#integration-testing)
10. [Manual Testing](#manual-testing)
11. [Test Data Generation](#test-data-generation)
12. [CI/CD Integration](#cicd-integration)

---

## Testing Philosophy

### Testing Pyramid for Schema Changes

```
                    ┌─────────────────┐
                    │  Manual Testing │  ← Few, critical paths
                    │   (UI, E2E)     │
                    └─────────────────┘
                  ┌───────────────────────┐
                  │  Integration Tests    │  ← Some, key flows
                  │  (Multi-component)    │
                  └───────────────────────┘
              ┌─────────────────────────────────┐
              │      Migration Tests            │  ← Many, all versions
              │   (Version transitions)         │
              └─────────────────────────────────┘
          ┌─────────────────────────────────────────┐
          │           Unit Tests                    │  ← Most, fast & focused
          │  (Property, validation, individual)     │
          └─────────────────────────────────────────┘
```

### Test Coverage Requirements

**Minimum coverage for schema changes:**

- ✅ **100%** migration paths tested (all version transitions)
- ✅ **100%** validation rules tested (valid + invalid cases)
- ✅ **90%+** code coverage for new property code
- ✅ **All** RTC configurations tested with multi-device scenarios
- ✅ **All** import/export format changes tested with round-trips

### Testing Priorities

**Priority 1 - Critical (Must Test):**

- Migration from previous 3 schema versions
- Validation rules on existing data
- Import/export round-trip
- RTC sync with concurrent edits

**Priority 2 - Important (Should Test):**

- Edge cases (nil, empty, special characters)
- Performance with large datasets
- Error recovery scenarios
- Cross-version compatibility

**Priority 3 - Nice to Have (Can Test):**

- UI rendering of new properties
- All possible data combinations
- Stress testing beyond typical use

---

## Test Environment Setup

### Test Database Creation

```clojure
(ns logseq.db.test-helpers
  (:require [datascript.core :as d]
            [logseq.db.schema :as db-schema]
            [logseq.db.frontend.create-graph :as create-graph]))

(defn create-test-db
  "Create empty test database with current schema"
  []
  (let [conn (d/create-conn db-schema/schema)]
    conn))

(defn create-test-db-with-schema
  "Create test database with specific schema version"
  [schema-version]
  (let [conn (d/create-conn (get-schema-for-version schema-version))]
    ;; Initialize with that version
    (create-graph/create-db-graph-from-scratch! conn schema-version)
    conn))

(defn create-test-db-with-data
  "Create test database with sample data"
  [schema-version & [{:keys [pages blocks properties]
                      :or {pages 10 blocks 100 properties 5}}]]
  (let [conn (create-test-db-with-schema schema-version)]
    (generate-test-data! conn {:pages pages
                               :blocks blocks
                               :properties properties})
    conn))
```

### Test Data Fixtures

```clojure
(def sample-block
  {:block/uuid (d/squuid)
   :block/type "block"
   :block/content "Sample block content"
   :block/parent {:db/id 2}
   :block/left {:db/id 1}
   :block/order "a0"
   :block/created-at 1234567890
   :block/updated-at 1234567890})

(def sample-page
  {:block/uuid (d/squuid)
   :block/type "page"
   :block/original-name "Sample Page"
   :block/created-at 1234567890
   :block/updated-at 1234567890})

(def sample-property
  {:db/ident :test.property/test-prop
   :block/type "property"
   :block/original-name "Test Property"
   :block/schema {:type :default
                  :cardinality :one}
   :block/created-at 1234567890
   :block/updated-at 1234567890})
```

### Mock RTC Environment

```clojure
(defn create-mock-rtc-server
  "Create mock RTC server for testing sync"
  []
  (atom {:connected-devices []
         :message-queue []}))

(defn create-mock-device
  "Create mock device for RTC testing"
  [& [{:keys [schema-version name]
       :or {schema-version "65.17"
            name "Test Device"}}]]
  {:name name
   :schema-version schema-version
   :conn (create-test-db-with-schema schema-version)
   :pending-ops (atom [])
   :received-ops (atom [])})
```

---

## Unit Testing

### Property Definition Tests

```clojure
(deftest test-property-definition
  (testing "Property definition is valid"
    (let [prop (get-property-definition :logseq.property/test-prop)]
      (is (some? prop) "Property should exist")
      (is (= :default (get-in prop [:schema :type])))
      (is (= :one (get-in prop [:schema :cardinality])))
      (is (contains? prop :rtc) "Property must have RTC config")))

  (testing "Property RTC configuration is explicit"
    (let [prop (get-property-definition :logseq.property/test-prop)
          rtc-config (:rtc prop)]
      (is (contains? rtc-config :rtc/ignore-attr-when-init-upload))
      (is (contains? rtc-config :rtc/ignore-attr-when-init-download))
      (is (contains? rtc-config :rtc/ignore-attr-when-syncing)))))
```

### Property Usage Tests

```clojure
(deftest test-property-usage
  (let [conn (create-test-db)
        page-uuid (d/squuid)]

    (testing "Can set property on page"
      (d/transact! conn [{:block/uuid page-uuid
                          :block/type "page"
                          :block/original-name "Test Page"
                          :logseq.property/test-prop "test value"}])
      (let [page (d/entity @conn [:block/uuid page-uuid])]
        (is (= "test value" (:logseq.property/test-prop page)))))

    (testing "Property value validates correctly"
      (let [page (d/entity @conn [:block/uuid page-uuid])
            [valid? errors] (validate-entity @conn page)]
        (is valid? "Page with property should be valid")
        (is (empty? errors))))

    (testing "Can update property value"
      (d/transact! conn [{:block/uuid page-uuid
                          :logseq.property/test-prop "updated value"}])
      (let [page (d/entity @conn [:block/uuid page-uuid])]
        (is (= "updated value" (:logseq.property/test-prop page)))))

    (testing "Can remove property value"
      (d/transact! conn [[:db/retract [:block/uuid page-uuid]
                          :logseq.property/test-prop]])
      (let [page (d/entity @conn [:block/uuid page-uuid])]
        (is (nil? (:logseq.property/test-prop page)))))))
```

### Property Type Tests

```clojure
(deftest test-property-type-number
  (let [conn (create-test-db)]

    (testing "Accepts valid number"
      (is (valid-property-value? @conn :logseq.property/number-prop 42)))

    (testing "Rejects invalid number"
      (is (not (valid-property-value? @conn :logseq.property/number-prop "not-a-number"))))

    (testing "Handles cardinality many"
      (is (valid-property-value? @conn :logseq.property/number-many-prop [1 2 3])))))
```

---

## Migration Testing

### Basic Migration Test

```clojure
(deftest test-migration-65-16-to-65-17
  (testing "Migration from 65.16 to 65.17 succeeds"
    (let [conn (create-test-db-with-schema "65.16")
          _ (generate-test-data! conn {:pages 100 :blocks 1000})
          pre-migration-count (count-entities @conn)]

      ;; Run migration
      (migrate-db-graph conn "65.17")

      (testing "Schema version updated"
        (is (= "65.17" (get-schema-version @conn))))

      (testing "No data loss"
        (is (= pre-migration-count (count-entities @conn))))

      (testing "New property exists"
        (is (d/entity @conn :logseq.property/new-prop-65-17)))

      (testing "Database validates"
        (let [[valid? errors] (validate-db! @conn)]
          (is valid? "Database should be valid after migration")
          (when errors
            (println "Validation errors:" errors)))))))
```

### Multi-Version Migration Path

```clojure
(deftest test-migration-path-from-older-versions
  (testing "Can migrate from multiple older versions"
    (doseq [old-version ["65.14" "65.15" "65.16"]]
      (testing (str "Migration from " old-version " to current")
        (let [conn (create-test-db-with-schema old-version)
              _ (generate-test-data! conn {:pages 50 :blocks 500})
              current-version (get-current-schema-version)]

          ;; Run migration
          (migrate-db-graph conn current-version)

          ;; Verify success
          (is (= current-version (get-schema-version @conn))
              (str "Should migrate from " old-version " to " current-version))

          (let [[valid? errors] (validate-db! @conn)]
            (is valid?
                (str "Database should be valid after migration from " old-version))))))))
```

### Migration Fix Function Test

```clojure
(deftest test-migration-fix-function
  (testing "Fix function transforms data correctly"
    (let [conn (create-test-db-with-schema "65.16")
          ;; Add data that needs fixing
          block-uuid (d/squuid)]

      (d/transact! conn [{:block/uuid block-uuid
                          :block/type "block"
                          :block/content "test"
                          :block/old-property "old-value"}])

      ;; Run migration with fix function
      (migrate-db-graph conn "65.17")

      (testing "Fix function applied transformation"
        (let [block (d/entity @conn [:block/uuid block-uuid])]
          (is (nil? (:block/old-property block))
              "Old property should be removed")
          (is (some? (:block/new-property block))
              "New property should be added")
          (is (= "transformed-old-value" (:block/new-property block))
              "Value should be transformed")))))

  (testing "Fix function handles nil values"
    (let [conn (create-test-db-with-schema "65.16")
          block-uuid (d/squuid)]

      (d/transact! conn [{:block/uuid block-uuid
                          :block/type "block"
                          :block/content "test"}])

      (migrate-db-graph conn "65.17")

      (let [block (d/entity @conn [:block/uuid block-uuid])]
        (is (some? block) "Block should still exist")))))
```

### Migration Performance Test

```clojure
(deftest test-migration-performance
  (testing "Migration completes in reasonable time"
    (let [conn (create-test-db-with-data "65.16"
                                         {:pages 1000
                                          :blocks 10000
                                          :properties 50})
          start-time (js/Date.now)]

      (migrate-db-graph conn "65.17")

      (let [duration (- (js/Date.now) start-time)]
        (is (< duration 30000)
            (str "Migration should complete in < 30s (took " duration "ms)"))))))
```

### Migration Idempotency Test

```clojure
(deftest test-migration-idempotency
  (testing "Running migration twice is safe"
    (let [conn (create-test-db-with-schema "65.16")]

      ;; First migration
      (migrate-db-graph conn "65.17")
      (let [after-first (export-db @conn)]

        ;; Second migration (should be no-op)
        (migrate-db-graph conn "65.17")
        (let [after-second (export-db @conn)]

          (is (= after-first after-second)
              "Running migration twice should not change data"))))))
```

---

## Validation Testing

### Valid Data Tests

```clojure
(deftest test-validation-valid-data
  (testing "Valid block passes validation"
    (let [conn (create-test-db)
          valid-block {:db/id -1
                       :block/uuid (d/squuid)
                       :block/type "block"
                       :block/content "Valid content"
                       :block/parent {:db/id 2}
                       :block/created-at 123456
                       :block/updated-at 123456}]
      (d/transact! conn [valid-block])
      (let [[valid? errors] (validate-tx-report @conn (d/tx-report))]
        (is valid? "Valid block should pass validation")
        (is (empty? errors)))))

  (testing "Valid page passes validation"
    (let [conn (create-test-db)
          valid-page {:db/id -1
                      :block/uuid (d/squuid)
                      :block/type "page"
                      :block/original-name "Valid Page"
                      :block/created-at 123456
                      :block/updated-at 123456}]
      (d/transact! conn [valid-page])
      (let [[valid? errors] (validate-tx-report @conn (d/tx-report))]
        (is valid?)))))
```

### Invalid Data Tests

```clojure
(deftest test-validation-invalid-data
  (testing "Missing required field fails validation"
    (let [conn (create-test-db)
          invalid-block {:db/id -1
                         :block/uuid (d/squuid)
                         :block/type "block"}]  ; Missing content
      (d/transact! conn [invalid-block])
      (let [[valid? errors] (validate-tx-report @conn (d/tx-report))]
        (is (not valid?) "Block without content should fail")
        (is (some #(str/includes? % "content") errors)
            "Error should mention missing content"))))

  (testing "Invalid type fails validation"
    (let [conn (create-test-db)
          invalid-block {:db/id -1
                         :block/uuid "not-a-uuid"  ; Should be UUID
                         :block/type "block"
                         :block/content "Content"}]
      (d/transact! conn [invalid-block])
      (let [[valid? errors] (validate-tx-report @conn (d/tx-report))]
        (is (not valid?) "Invalid UUID should fail")
        (is (some #(str/includes? % "UUID") errors)))))

  (testing "Invalid enum value fails"
    (let [conn (create-test-db)]
      (d/transact! conn [{:db/id -1
                          :block/uuid (d/squuid)
                          :logseq.property/status "invalid-status"}])
      (let [[valid? errors] (validate-tx-report @conn (d/tx-report))]
        (is (not valid?))))))
```

### Property Value Validation Tests

```clojure
(deftest test-property-value-validation
  (testing "URL property validates URL format"
    (is (valid-property-value? :url "https://example.com"))
    (is (not (valid-property-value? :url "not-a-url"))))

  (testing "Number property validates numeric values"
    (is (valid-property-value? :number 42))
    (is (valid-property-value? :number 3.14))
    (is (not (valid-property-value? :number "not-a-number"))))

  (testing "Date property validates date format"
    (is (valid-property-value? :date 20250101))
    (is (not (valid-property-value? :date "2025-01-01")))
    (is (not (valid-property-value? :date 99999999)))))
```

### Validation Fix Tests

```clojure
(deftest test-validation-fix-function
  (testing "Fix function corrects invalid data"
    (let [conn (create-test-db)]
      ;; Insert invalid data (bypassing validation)
      (binding [validate/*validate-db?* false]
        (d/transact! conn [{:db/id -1
                            :block/uuid (d/squuid)
                            :block/type "block"
                            :block/invalid-attr "should not exist"}]))

      ;; Validate (should fail)
      (let [[valid? errors] (validate-db! @conn)]
        (is (not valid?) "Should detect invalid data"))

      ;; Run fix
      (fix-invalid-blocks! conn (validate-db! @conn))

      ;; Re-validate (should pass)
      (let [[valid? errors] (validate-db! @conn)]
        (is valid? "Fix function should correct invalid data")))))
```

---

## Import/Export Testing

### Round-Trip Test

```clojure
(deftest test-export-import-round-trip
  (testing "Export and import preserves all data"
    (let [original-conn (create-test-db-with-data "65.17"
                                                   {:pages 100
                                                    :blocks 1000
                                                    :properties 20})
          ;; Export
          exported (export-db @original-conn)

          ;; Import into new database
          imported-conn (create-test-db)
          _ (import-db imported-conn exported)

          ;; Re-export
          re-exported (export-db @imported-conn)]

      (testing "Exported data structure matches"
        (is (= exported re-exported)
            "Re-export should match original export"))

      (testing "Entity count preserved"
        (is (= (count-entities @original-conn)
               (count-entities @imported-conn))))

      (testing "All UUIDs preserved"
        (is (= (get-all-uuids @original-conn)
               (get-all-uuids @imported-conn))))

      (testing "Validation passes after import"
        (let [[valid? errors] (validate-db! @imported-conn)]
          (is valid? "Imported database should be valid"))))))
```

### Cross-Version Import Test

```clojure
(deftest test-import-from-older-version
  (testing "Can import exports from previous versions"
    (doseq [old-version ["65.14" "65.15" "65.16"]]
      (testing (str "Import from version " old-version)
        (let [;; Create and export old version
              old-conn (create-test-db-with-data old-version
                                                 {:pages 50 :blocks 500})
              exported (export-db @old-conn)

              ;; Import to current version
              new-conn (create-test-db)
              _ (import-db new-conn exported)]

          (testing "Import succeeds"
            (is (pos? (count-entities @new-conn))))

          (testing "Validation passes"
            (let [[valid? errors] (validate-db! @new-conn)]
              (is valid?
                  (str "Import from " old-version " should produce valid database")))))))))
```

### Large Graph Import/Export

```clojure
(deftest test-large-graph-import-export
  (testing "Can handle large graphs efficiently"
    (let [large-conn (create-test-db-with-data "65.17"
                                               {:pages 5000
                                                :blocks 50000
                                                :properties 100})]

      (testing "Export completes in reasonable time"
        (let [start (js/Date.now)
              exported (export-db @large-conn)
              duration (- (js/Date.now) start)]
          (is (< duration 60000)
              (str "Export should complete in < 60s (took " duration "ms)"))))

      (testing "Import completes in reasonable time"
        (let [exported (export-db @large-conn)
              new-conn (create-test-db)
              start (js/Date.now)
              _ (import-db new-conn exported)
              duration (- (js/Date.now) start)]
          (is (< duration 120000)
              (str "Import should complete in < 120s (took " duration "ms)")))))))
```

### Circular Reference Import Test

```clojure
(deftest test-circular-reference-import
  (testing "Handles circular references correctly"
    (let [;; Create graph with circular references
          conn (create-test-db)
          uuid-a (d/squuid)
          uuid-b (d/squuid)]

      (d/transact! conn [{:block/uuid uuid-a
                          :block/type "block"
                          :block/content "Block A"
                          :block/parent {:block/uuid uuid-b}}
                         {:block/uuid uuid-b
                          :block/type "block"
                          :block/content "Block B"
                          :block/parent {:block/uuid uuid-a}}])

      ;; Export and import
      (let [exported (export-db @conn)
            new-conn (create-test-db)]
        (import-db new-conn exported)

        (testing "Both entities exist"
          (is (d/entity @new-conn [:block/uuid uuid-a]))
          (is (d/entity @new-conn [:block/uuid uuid-b])))

        (testing "Circular reference preserved"
          (let [block-a (d/entity @new-conn [:block/uuid uuid-a])
                block-b (d/entity @new-conn [:block/uuid uuid-b])]
            (is (= uuid-b (:block/uuid (:block/parent block-a))))
            (is (= uuid-a (:block/uuid (:block/parent block-b))))))))))
```

### Export Format Validation Test

```clojure
(deftest test-export-format-validation
  (testing "Export format is valid EDN"
    (let [conn (create-test-db-with-data "65.17")
          exported (export-db @conn)
          edn-string (pr-str exported)]

      (testing "Can parse as EDN"
        (is (some? (edn/read-string edn-string))))

      (testing "Has required keys"
        (is (contains? exported :blocks))
        (is (contains? exported :properties))
        (is (contains? exported :classes)))

      (testing "UUIDs are tagged literals"
        (is (every? uuid? (map :uuid (:blocks exported))))))))
```

---

## RTC/Sync Testing

### Basic Sync Test

```clojure
(deftest test-basic-rtc-sync
  (testing "Changes sync from device A to device B"
    (let [server (create-mock-rtc-server)
          device-a (create-mock-device {:name "Device A"})
          device-b (create-mock-device {:name "Device B"})
          block-uuid (d/squuid)]

      ;; Connect devices
      (connect-device device-a server)
      (connect-device device-b server)

      ;; Device A creates block
      (d/transact! (:conn device-a)
                   [{:block/uuid block-uuid
                     :block/type "block"
                     :block/content "Synced content"}])

      ;; Sync to server
      (sync-to-server device-a server)

      ;; Sync from server to device B
      (sync-from-server device-b server)

      ;; Verify block appears on device B
      (let [block-on-b (d/entity @(:conn device-b) [:block/uuid block-uuid])]
        (is (some? block-on-b) "Block should exist on device B")
        (is (= "Synced content" (:block/content block-on-b)))))))
```

### Concurrent Edit Conflict Test

```clojure
(deftest test-concurrent-edit-conflict
  (testing "Concurrent edits resolve correctly"
    (let [server (create-mock-rtc-server)
          device-a (create-mock-device)
          device-b (create-mock-device)
          block-uuid (d/squuid)]

      ;; Setup: both devices have same block
      (doseq [device [device-a device-b]]
        (d/transact! (:conn device)
                     [{:block/uuid block-uuid
                       :block/type "block"
                       :block/content "Original content"}]))

      ;; Concurrent edits
      (d/transact! (:conn device-a)
                   [{:block/uuid block-uuid
                     :block/content "Edit from A"
                     :block/updated-at 1000}])

      (d/transact! (:conn device-b)
                   [{:block/uuid block-uuid
                     :block/content "Edit from B"
                     :block/updated-at 1001}])

      ;; Sync both to server
      (sync-to-server device-a server)
      (sync-to-server device-b server)

      ;; Sync down to both devices
      (sync-from-server device-a server)
      (sync-from-server device-b server)

      ;; Both should converge to same content (last-write-wins)
      (let [content-a (:block/content (d/entity @(:conn device-a) [:block/uuid block-uuid]))
            content-b (:block/content (d/entity @(:conn device-b) [:block/uuid block-uuid]))]
        (is (= content-a content-b) "Both devices should have same content after sync")
        (is (or (= content-a "Edit from A")
                (= content-a "Edit from B"))
            "Content should be one of the edits")))))
```

### Property Sync Test

```clojure
(deftest test-property-rtc-sync
  (testing "Property with RTC sync enabled syncs correctly"
    (let [server (create-mock-rtc-server)
          device-a (create-mock-device)
          device-b (create-mock-device)
          page-uuid (d/squuid)]

      ;; Device A sets property
      (d/transact! (:conn device-a)
                   [{:block/uuid page-uuid
                     :block/type "page"
                     :block/original-name "Test Page"
                     :logseq.property/synced-prop "value-from-a"}])

      (sync-to-server device-a server)
      (sync-from-server device-b server)

      ;; Verify property synced to device B
      (let [page-on-b (d/entity @(:conn device-b) [:block/uuid page-uuid])]
        (is (= "value-from-a" (:logseq.property/synced-prop page-on-b))))))

  (testing "Property with RTC sync disabled does not sync"
    (let [server (create-mock-rtc-server)
          device-a (create-mock-device)
          device-b (create-mock-device)
          page-uuid (d/squuid)]

      ;; Device A sets non-synced property
      (d/transact! (:conn device-a)
                   [{:block/uuid page-uuid
                     :block/type "page"
                     :block/original-name "Test Page"
                     :logseq.property/local-only-prop "local-value"}])

      (sync-to-server device-a server)
      (sync-from-server device-b server)

      ;; Verify property NOT synced to device B
      (let [page-on-b (d/entity @(:conn device-b) [:block/uuid page-uuid])]
        (is (nil? (:logseq.property/local-only-prop page-on-b)))))))
```

### Schema Version Mismatch Test

```clojure
(deftest test-schema-version-mismatch-sync
  (testing "New property on newer version handled gracefully by older version"
    (let [server (create-mock-rtc-server)
          device-new (create-mock-device {:schema-version "65.17"})
          device-old (create-mock-device {:schema-version "65.16"})
          page-uuid (d/squuid)]

      ;; New device sets property from v65.17
      (d/transact! (:conn device-new)
                   [{:block/uuid page-uuid
                     :block/type "page"
                     :block/original-name "Test Page"
                     :logseq.property/v65-17-only "new-property-value"}])

      (sync-to-server device-new server)
      (sync-from-server device-old server)

      ;; Old device should not error
      (is (no-sync-errors? device-old))

      ;; Old device should not have the property (gracefully ignored)
      (let [page-on-old (d/entity @(:conn device-old) [:block/uuid page-uuid])]
        (is (nil? (:logseq.property/v65-17-only page-on-old)))))))
```

### Order Conflict Test

```clojure
(deftest test-order-conflict-resolution
  (testing "Concurrent block reordering resolves without data loss"
    (let [server (create-mock-rtc-server)
          device-a (create-mock-device)
          device-b (create-mock-device)
          uuids [(d/squuid) (d/squuid) (d/squuid)]
          parent-uuid (d/squuid)]

      ;; Setup: both devices have same blocks
      (doseq [device [device-a device-b]]
        (d/transact! (:conn device)
                     (cons {:block/uuid parent-uuid
                            :block/type "page"
                            :block/original-name "Parent"}
                           (map-indexed
                            (fn [idx uuid]
                              {:block/uuid uuid
                               :block/type "block"
                               :block/content (str "Block " idx)
                               :block/parent {:block/uuid parent-uuid}
                               :block/order (str "a" idx)})
                            uuids))))

      ;; Concurrent reordering
      ;; Device A: reverse order
      (d/transact! (:conn device-a)
                   (map-indexed
                    (fn [idx uuid]
                      {:block/uuid uuid
                       :block/order (str "b" idx)})
                    (reverse uuids)))

      ;; Device B: different order
      (d/transact! (:conn device-b)
                   [{:block/uuid (nth uuids 1) :block/order "c0"}
                    {:block/uuid (nth uuids 0) :block/order "c1"}
                    {:block/uuid (nth uuids 2) :block/order "c2"}])

      ;; Sync
      (sync-to-server device-a server)
      (sync-to-server device-b server)
      (sync-from-server device-a server)
      (sync-from-server device-b server)

      ;; All blocks should still exist on both devices
      (doseq [device [device-a device-b]
              uuid uuids]
        (is (d/entity @(:conn device) [:block/uuid uuid])
            "All blocks should exist after sync"))

      ;; Orders should converge
      (let [order-a (get-block-orders (:conn device-a) parent-uuid)
            order-b (get-block-orders (:conn device-b) parent-uuid)]
        (is (= order-a order-b) "Block orders should converge")))))
```

---

## Performance Testing

### Migration Performance Benchmarks

```clojure
(deftest test-migration-performance-benchmarks
  (testing "Migration performance across graph sizes"
    (doseq [[size pages blocks] [["small" 100 1000]
                                  ["medium" 1000 10000]
                                  ["large" 5000 50000]]]
      (testing (str "Migration performance on " size " graph")
        (let [conn (create-test-db-with-data "65.16"
                                             {:pages pages
                                              :blocks blocks})
              start (js/Date.now)]

          (migrate-db-graph conn "65.17")

          (let [duration (- (js/Date.now) start)
                expected-max (case size
                               "small" 5000    ; 5s
                               "medium" 15000  ; 15s
                               "large" 30000)] ; 30s
            (is (< duration expected-max)
                (str size " graph migration should complete in < "
                     expected-max "ms (took " duration "ms)"))))))))
```

### Validation Performance

```clojure
(deftest test-validation-performance
  (testing "Validation overhead is acceptable"
    (let [conn (create-test-db-with-data "65.17" {:pages 1000 :blocks 10000})]

      (testing "Full DB validation"
        (let [start (js/Date.now)]
          (validate-db! @conn)
          (let [duration (- (js/Date.now) start)]
            (is (< duration 10000)
                (str "Full DB validation should complete in < 10s (took " duration "ms)")))))

      (testing "Transaction validation overhead"
        (let [start (js/Date.now)
              _ (dotimes [_ 100]
                  (d/transact! conn [{:block/uuid (d/squuid)
                                      :block/type "block"
                                      :block/content "test"}]))
              duration (- (js/Date.now) start)
              avg-per-tx (/ duration 100)]
          (is (< avg-per-tx 50)
              (str "Average validation overhead should be < 50ms per tx (was " avg-per-tx "ms)")))))))
```

### Import/Export Performance

```clojure
(deftest test-import-export-performance
  (testing "Import/export performance scales linearly"
    (let [sizes [[1000 10000]
                 [2000 20000]
                 [4000 40000]]
          results (atom [])]

      (doseq [[pages blocks] sizes]
        (let [conn (create-test-db-with-data "65.17" {:pages pages :blocks blocks})

              ;; Measure export
              export-start (js/Date.now)
              exported (export-db @conn)
              export-time (- (js/Date.now) export-start)

              ;; Measure import
              new-conn (create-test-db)
              import-start (js/Date.now)
              _ (import-db new-conn exported)
              import-time (- (js/Date.now) import-start)]

          (swap! results conj {:pages pages
                               :blocks blocks
                               :export-time export-time
                               :import-time import-time})))

      ;; Check that time scales roughly linearly with data size
      (let [[r1 r2 r3] @results
            export-ratio-1-2 (/ (:export-time r2) (:export-time r1))
            export-ratio-2-3 (/ (:export-time r3) (:export-time r2))]
        (is (< 1.5 export-ratio-1-2 2.5)
            "Export time should scale linearly (ratio close to 2)")
        (is (< 1.5 export-ratio-2-3 2.5)
            "Export time should scale linearly (ratio close to 2)")))))
```

---

## Integration Testing

### Full Migration + Validation + Import/Export Flow

```clojure
(deftest test-full-migration-workflow
  (testing "Complete migration workflow"
    (let [;; 1. Start with old version
          old-conn (create-test-db-with-data "65.16" {:pages 100 :blocks 1000})]

      ;; 2. Export old version
      (let [exported-old (export-db @old-conn)]

        ;; 3. Migrate to new version
        (migrate-db-graph old-conn "65.17")

        (testing "Migration validation"
          (let [[valid? errors] (validate-db! @old-conn)]
            (is valid? "Migrated database should be valid")))

        ;; 4. Export new version
        (let [exported-new (export-db @old-conn)]

          ;; 5. Import to fresh database
          (let [fresh-conn (create-test-db)]
            (import-db fresh-conn exported-new)

            (testing "Import validation"
              (let [[valid? errors] (validate-db! @fresh-conn)]
                (is valid? "Imported database should be valid")))

            ;; 6. Re-export and verify consistency
            (let [re-exported (export-db @fresh-conn)]
              (is (= exported-new re-exported)
                  "Re-export should match"))))))))
```

### Multi-Device Migration Scenario

```clojure
(deftest test-multi-device-migration-scenario
  (testing "Multi-device migration and sync"
    (let [server (create-mock-rtc-server)

          ;; Device A starts on old version
          device-a (create-mock-device {:schema-version "65.16"})

          ;; Device B already on new version
          device-b (create-mock-device {:schema-version "65.17"})]

      ;; Both connected and syncing
      (connect-device device-a server)
      (connect-device device-b server)

      ;; Device B uses new property
      (d/transact! (:conn device-b)
                   [{:block/uuid (d/squuid)
                     :block/type "page"
                     :block/original-name "Test"
                     :logseq.property/v65-17-only "new-value"}])

      (sync-to-server device-b server)
      (sync-from-server device-a server)

      ;; Device A should not error (graceful degradation)
      (is (no-sync-errors? device-a))

      ;; Device A upgrades
      (migrate-db-graph (:conn device-a) "65.17")

      ;; Now resync
      (sync-from-server device-a server)

      ;; Device A should now see new property
      ;; (depends on sync strategy - may need to re-request data)
      )))
```

---

## Manual Testing

### Manual Test Checklist

**Before release, manually verify:**

#### Graph Creation

- [ ] Create new graph with new schema version
- [ ] Verify all built-in properties present
- [ ] Check no console errors
- [ ] Verify validation passes on new graph

#### Migration

- [ ] Open graph from previous version (automatic migration)
- [ ] Verify migration completes without errors
- [ ] Check all pages/blocks still accessible
- [ ] Verify properties visible and editable
- [ ] Check no validation errors after migration

#### Import/Export

- [ ] Export graph with new schema
- [ ] Import exported graph in clean environment
- [ ] Verify all data imported correctly
- [ ] Check validation passes on imported graph
- [ ] Re-export and compare (should be identical)

#### RTC Sync (if applicable)

- [ ] Connect two devices to same graph
- [ ] Edit property on device A
- [ ] Verify change appears on device B
- [ ] Perform concurrent edits
- [ ] Verify conflict resolution
- [ ] Check no sync errors in console

#### Compatibility

- [ ] Test with graph from 3 versions ago
- [ ] Verify migration path works
- [ ] Check all historical data preserved
- [ ] Test export from old version, import to new

#### Performance

- [ ] Test migration on large graph (10K+ pages)
- [ ] Monitor migration time (should complete)
- [ ] Check memory usage during migration
- [ ] Verify UI remains responsive

---

## Test Data Generation

### Generating Test Graphs

```clojure
(defn generate-test-graph
  "Generate test graph with specified characteristics"
  [conn {:keys [pages blocks properties depth branching-factor]
         :or {pages 100
              blocks 1000
              properties 20
              depth 3
              branching-factor 5}}]

  ;; Generate pages
  (let [page-uuids (repeatedly pages d/squuid)
        page-tx (map (fn [uuid]
                      {:block/uuid uuid
                       :block/type "page"
                       :block/original-name (str "Page " uuid)
                       :block/created-at (rand-int 1000000)
                       :block/updated-at (rand-int 1000000)})
                    page-uuids)]
    (d/transact! conn page-tx)

    ;; Generate hierarchical blocks
    (generate-blocks! conn page-uuids blocks depth branching-factor)

    ;; Add properties to entities
    (add-random-properties! conn properties)))

(defn generate-blocks!
  "Generate hierarchical block structure"
  [conn parent-uuids target-count depth branching-factor]
  (when (and (pos? depth) (< (count-blocks @conn) target-count))
    (let [child-uuids (repeatedly (* (count parent-uuids) branching-factor)
                                  d/squuid)
          child-tx (map-indexed
                    (fn [idx uuid]
                      (let [parent (nth parent-uuids (quot idx branching-factor))]
                        {:block/uuid uuid
                         :block/type "block"
                         :block/content (str "Block " uuid)
                         :block/parent {:block/uuid parent}
                         :block/order (str "a" (mod idx branching-factor))
                         :block/created-at (rand-int 1000000)
                         :block/updated-at (rand-int 1000000)}))
                    child-uuids)]
      (d/transact! conn child-tx)
      ;; Recurse for next level
      (generate-blocks! conn child-uuids target-count (dec depth) branching-factor))))
```

### Generating Edge Case Data

```clojure
(defn generate-edge-cases
  "Generate test data with edge cases"
  [conn]
  (d/transact! conn
               [;; Empty content
                {:block/uuid (d/squuid)
                 :block/type "block"
                 :block/content ""}

                ;; Very long content
                {:block/uuid (d/squuid)
                 :block/type "block"
                 :block/content (apply str (repeat 10000 "x"))}

                ;; Special characters
                {:block/uuid (d/squuid)
                 :block/type "block"
                 :block/content "Special: \n\t\"'<>&"}

                ;; Unicode
                {:block/uuid (d/squuid)
                 :block/type "block"
                 :block/content "Unicode: 你好 مرحبا שלום"}

                ;; Nil/missing properties (as much as schema allows)
                {:block/uuid (d/squuid)
                 :block/type "page"
                 :block/original-name "Minimal"}]))
```

---

## CI/CD Integration

### Automated Test Pipeline

```yaml
# .github/workflows/schema-tests.yml
name: Schema Change Tests

on:
  pull_request:
    paths:
      - 'deps/db/src/logseq/db/**'
      - 'src/main/frontend/worker/db_migrate.cljs'

jobs:
  schema-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Setup Clojure
        uses: DeLaGuardo/setup-clojure@master

      - name: Run unit tests
        run: bb test:unit

      - name: Run migration tests
        run: bb test:migration

      - name: Run validation tests
        run: bb test:validation

      - name: Run import/export tests
        run: bb test:import-export

      - name: Check schema version consistency
        run: bb lint:schema-version
```

### Schema Lint Checks

```clojure
;; scripts/lint_schema.clj
(defn lint-schema-changes []
  (let [changes (get-schema-changes)]

    ;; Check 1: Property added without migration?
    (when (property-added-without-migration? changes)
      (println "ERROR: Property added but no migration entry found"))

    ;; Check 2: Schema version bumped?
    (when (and (property-added? changes)
               (not (schema-version-bumped? changes)))
      (println "ERROR: Property added but schema version not bumped"))

    ;; Check 3: Migration version matches?
    (when-not (migration-version-matches-schema? changes)
      (println "ERROR: Migration version doesn't match schema version"))

    ;; Check 4: RTC config present?
    (when (property-missing-rtc-config? changes)
      (println "WARNING: Property missing RTC configuration"))))
```

---

## Quick Reference

### Test Coverage Checklist

For each schema change PR:

**Unit Tests (Required)**

- [ ] Property definition test
- [ ] Property usage test (set, get, update, remove)
- [ ] Validation rules test (valid + invalid)

**Migration Tests (Required)**

- [ ] Migration from previous version
- [ ] Migration with test data
- [ ] Migration performance (<30s for large graph)
- [ ] Migration idempotency

**Validation Tests (Required)**

- [ ] Valid data passes
- [ ] Invalid data fails with clear errors
- [ ] Fix function works (if applicable)

**Import/Export Tests (Required)**

- [ ] Round-trip test (export → import → export)
- [ ] Cross-version import (from previous 3 versions)

**RTC Tests (If Applicable)**

- [ ] Basic sync (device A → device B)
- [ ] Concurrent edit conflict resolution
- [ ] Schema version mismatch handling

**Performance Tests (For Large Changes)**

- [ ] Migration on large graph
- [ ] Validation overhead
- [ ] Import/export performance

**Manual Tests (Before Release)**

- [ ] Create new graph
- [ ] Migrate existing graph
- [ ] Export and import
- [ ] RTC sync (if applicable)

---

## Related Documentation

- [Investigation Overview](../07-investigations/schema-evolution-and-data-migration.md)
- [Schema Migration Best Practices](./schema-migration-best-practices.md)
- [Validation Guidelines](./validation-best-practices.md)
- [Import/Export Best Practices](./import-export-best-practices.md)
- [RTC/Sync Considerations](./rtc-sync-schema-changes.md)
- [PR Review Checklist](./schema-change-pr-checklist.md)

---

## Changelog

| Date       | Changes                                         |
| ---------- | ----------------------------------------------- |
| 2025-12-28 | Initial version based on investigation findings |
