# Schema Changes & Data Integrity Investigation

## Part 7: PR Review Standards & Guiding Principles

---

## Purpose

This document provides **actionable guidelines for code reviewers and developers** to ensure schema changes don't cause data migration issues or compromise user data integrity. Use these standards during:

- Pull request reviews
- Design reviews
- Pre-release testing
- Post-deployment monitoring

---

## Table of Contents

1. [General Principles](#general-principles)
2. [Schema Change Checklist](#schema-change-checklist)
3. [Migration Review Standards](#migration-review-standards)
4. [Validation Review Standards](#validation-review-standards)
5. [Import/Export Review Standards](#importexport-review-standards)
6. [RTC Sync Review Standards](#rtc-sync-review-standards)
7. [Testing Requirements](#testing-requirements)
8. [Deployment Guidelines](#deployment-guidelines)
9. [Code Review Examples](#code-review-examples)

---

## General Principles

### Principle 1: User Data is Sacred

**What it means:** Never lose user data, even during schema evolution

**In practice:**

- ✅ Always provide migration path for existing data
- ✅ Test migrations on real-world data samples
- ✅ Implement rollback mechanisms
- ❌ Never delete attributes without migration
- ❌ Never change data types without transformation
- ❌ Never assume data is in expected format

**PR Review Question:**

> "If this change breaks, how do users recover their data?"

---

### Principle 2: Backward Compatibility First

**What it means:** Prefer additive changes over breaking changes

**In practice:**

- ✅ Add new attributes (minor version)
- ✅ Make attributes optional
- ✅ Provide default values
- ⚠️ Deprecate before removing
- ❌ Remove attributes without deprecation period
- ❌ Change cardinality without migration

**PR Review Question:**

> "Can older clients still read data after this change?"

---

### Principle 3: Fail Safely

**What it means:** Failures should not corrupt data

**In practice:**

- ✅ Validate before committing
- ✅ Use transactions for atomicity
- ✅ Log failures with context
- ✅ Provide clear error messages
- ❌ Partial updates without rollback
- ❌ Silent data corruption

**PR Review Question:**

> "What happens if this operation fails halfway through?"

---

### Principle 4: Make Migrations Idempotent

**What it means:** Running migration twice should produce same result

**In practice:**

- ✅ Check if migration already applied
- ✅ Handle edge cases (missing data)
- ✅ Test multiple executions
- ❌ Assume migration runs exactly once
- ❌ Increment counters without checking

**PR Review Question:**

> "Is it safe to run this migration multiple times?"

---

### Principle 5: Test on Real Data

**What it means:** Unit tests aren't enough for migrations

**In practice:**

- ✅ Test on production data exports
- ✅ Test on old schema versions
- ✅ Test on corrupted data
- ✅ Measure performance on large graphs
- ❌ Only test on clean test data
- ❌ Skip performance testing

**PR Review Question:**

> "Has this been tested on actual user graphs?"

---

## Schema Change Checklist

Use this checklist for **any PR that modifies schema definitions**.

### Before Submission

- [ ] **Version Bump:** Schema version incremented appropriately
  - Major bump for breaking changes
  - Minor bump for additions
- [ ] **Migration Code:** Migration written if data transform needed
- [ ] **DataScript Schema:** Updated if attribute definitions change
- [ ] **Malli Schema:** Updated if validation rules change
- [ ] **Built-in Properties:** Updated if new property added
- [ ] **Documentation:** Change documented with rationale
- [ ] **Impact Analysis:** Analyzed impact on all 5 systems:
  - Migration system
  - Validation system
  - Import/Export
  - RTC Sync
  - User experience

### During Code Review

- [ ] **Breaking Change?** Major version increment required?
- [ ] **Migration Needed?** Data transformation required?
- [ ] **Validation Impact?** Will existing data fail new rules?
- [ ] **Export Compatible?** Old exports still importable?
- [ ] **RTC Compatible?** Branch creation handled correctly?
- [ ] **Error Handling:** Failures handled gracefully?
- [ ] **Testing:** Adequate test coverage?
- [ ] **Documentation:** User-facing docs updated?

### Before Merge

- [ ] **CI Passing:** All automated tests pass
- [ ] **Manual Testing:** Tested on real graph
- [ ] **Rollback Plan:** Recovery procedure documented
- [ ] **Monitoring:** Metrics/alerts in place
- [ ] **Communication:** Team notified of breaking changes

---

## Migration Review Standards

### Standard 1: Migration Registry Entry

**Requirement:** Every schema version must have migration entry

```clojure
;; ✅ GOOD: Migration defined for version
{"65.16"
 [{:type :properties
   :properties {:block/priority {...}}}]}

;; ❌ BAD: Version missing from registry
{"65.15" [...]
 "65.17" [...]}  ;; Where is 65.16?
```

**PR Review Action:**

- Check `schema-version->updates` map
- Verify no gaps in version sequence

---

### Standard 2: Migration Idempotency

**Requirement:** Migrations must be idempotent

```clojure
;; ✅ GOOD: Checks before creating
(defn add-property [conn prop-name]
  (when-not (property-exists? @conn prop-name)
    (create-property! conn prop-name)))

;; ❌ BAD: Blindly creates
(defn add-property [conn prop-name]
  (create-property! conn prop-name))  ;; Fails if already exists
```

**PR Review Action:**

- Look for existence checks
- Test running migration twice

---

### Standard 3: Migration Error Handling

**Requirement:** Migrations must handle errors gracefully

```clojure
;; ✅ GOOD: Handles missing data
(defn migrate-priorities [conn]
  (let [blocks (d/q '[:find [?e ...]
                     :where
                     [?e :block/uuid]
                     (not [?e :block/priority])]  ;; Only blocks missing it
                   @conn)]
    (when (seq blocks)
      (d/transact! conn
        (for [e blocks]
          {:db/id e
           :block/priority 0})))))

;; ❌ BAD: Assumes data exists
(defn migrate-priorities [conn]
  (let [blocks (d/q '[:find [?e ...] :where [?e :block/uuid]] @conn)]
    (d/transact! conn
      (for [e blocks]
        {:db/id e
         :block/priority 0}))))  ;; Overwrites existing values!
```

**PR Review Action:**

- Check for defensive queries
- Look for null/undefined handling

---

### Standard 4: Migration Testing

**Requirement:** Migrations must have tests for multiple scenarios

```clojure
;; ✅ GOOD: Comprehensive test coverage
(deftest migrate-priority-test
  (testing "adds priority to blocks without it"
    ...)
  (testing "preserves existing priority values"
    ...)
  (testing "handles empty graph"
    ...)
  (testing "idempotent - can run twice"
    ...))

;; ❌ BAD: Only happy path
(deftest migrate-priority-test
  (testing "adds priority"
    ...))
```

**PR Review Action:**

- Verify test coverage
- Check for edge case tests

---

### Standard 5: Migration Performance

**Requirement:** Migrations must not freeze app for minutes

```clojure
;; ✅ GOOD: Batched processing
(defn migrate-in-batches [conn]
  (loop [batch 0]
    (let [blocks (get-batch @conn batch 1000)]
      (when (seq blocks)
        (process-batch! conn blocks)
        (report-progress batch)
        (recur (inc batch))))))

;; ❌ BAD: Processes all at once
(defn migrate-all-at-once [conn]
  (let [all-blocks (d/q '[:find [(pull ?e [*]) ...]
                         :where [?e :block/uuid]]
                       @conn)]
    (process-all! conn all-blocks)))  ;; Freezes on large graphs
```

**PR Review Action:**

- Ask about performance testing
- Check for batch processing

---

## Validation Review Standards

### Standard 1: Validation Rules Don't Break Existing Data

**Requirement:** New validation must not reject existing valid data

```clojure
;; ✅ GOOD: Validation only for new data
(def priority-schema
  [:and :number [:>= 0] [:< 10]  ;; New blocks must be 0-9
   [:or [:= nil]]])                ;; But existing blocks can be nil

;; ❌ BAD: Rejects existing data
(def priority-schema
  [:and :number [:>= 0] [:< 10]])  ;; Breaks blocks with nil priority
```

**PR Review Action:**

- Check if validation tightened
- Ask: "Will existing data fail this?"

---

### Standard 2: Validation Errors Are Actionable

**Requirement:** Error messages must guide user to fix issue

```clojure
;; ✅ GOOD: Clear, actionable error
(throw (ex-info
         (str "Block priority must be a number between 0-9. "
              "Current value: " (pr-str (:block/priority entity)) ". "
              "To fix: Edit the block and set priority to a number.")
         {:entity-id (:db/id entity)
          :attribute :block/priority
          :current-value (:block/priority entity)
          :expected-type :number
          :expected-range [0 9]}))

;; ❌ BAD: Cryptic error
(throw (ex-info "Validation failed" {:entity entity}))
```

**PR Review Action:**

- Read error messages
- Check if recovery path is clear

---

### Standard 3: Validation Doesn't Block Critical Operations

**Requirement:** Users must be able to backup even invalid graphs

```clojure
;; ✅ GOOD: Allow force export
(defn export-graph [db path {:keys [validate? force?]}]
  (when (and validate? (not force?))
    (when-let [errors (validate-db! db)]
      (throw (ex-info "Graph invalid. Use :force? true to export anyway"
                     {:errors errors}))))
  (do-export db path))

;; ❌ BAD: Always blocks on invalid
(defn export-graph [db path]
  (when-let [errors (validate-db! db)]
    (throw (ex-info "Cannot export invalid graph" {:errors errors})))
  (do-export db path))
```

**PR Review Action:**

- Check for escape hatches
- Verify force options exist

---

## Import/Export Review Standards

### Standard 1: Schema Version Tagged

**Requirement:** All exports must include schema version

```clojure
;; ✅ GOOD: Version included
(defn build-export [db]
  {:version 1
   :schema-version (get-schema-version db)  ;; ← Must include
   :entities (get-all-entities db)})

;; ❌ BAD: No version info
(defn build-export [db]
  {:entities (get-all-entities db)})
```

**PR Review Action:**

- Verify `:schema-version` in export
- Check version is read on import

---

### Standard 2: Import Handles Version Mismatch

**Requirement:** Import must gracefully handle version differences

```clojure
;; ✅ GOOD: Version check and handling
(defn import-graph [conn export-data]
  (let [export-version (:schema-version export-data)
        current-version (db-schema/schema-version)
        compat (check-compatibility export-version current-version)]
    (case (:action compat)
      :direct-import (do-import conn export-data)
      :migrate (do-import-and-migrate conn export-data)
      :reject (throw (ex-info "Incompatible version" compat)))))

;; ❌ BAD: No version check
(defn import-graph [conn export-data]
  (do-import conn export-data))
```

**PR Review Action:**

- Look for version comparison
- Check error messages for mismatches

---

### Standard 3: Import Validates Data

**Requirement:** Imported data must be validated before commit

```clojure
;; ✅ GOOD: Validate before commit
(defn import-entities [conn entities]
  ;; Validate each entity
  (doseq [entity entities]
    (when-let [errors (validate-entity entity)]
      (throw (ex-info "Invalid entity in import"
                     {:entity entity :errors errors}))))
  ;; All valid - now import
  (d/transact! conn entities))

;; ❌ BAD: Blind import
(defn import-entities [conn entities]
  (d/transact! conn entities))
```

**PR Review Action:**

- Check for validation step
- Verify validation happens before transaction

---

## RTC Sync Review Standards

### Standard 1: Schema Compatibility Check

**Requirement:** Must check schema compatibility before sync

```clojure
;; ✅ GOOD: Compatibility check
(defn connect-to-graph [client-schema graph-id]
  (let [server-schema (get-server-schema graph-id)
        compat (compare-schemas client-schema server-schema)]
    (case compat
      :compatible (join-branch graph-id)
      :create-branch (create-and-join-branch graph-id client-schema)
      :download (download-graph graph-id))))

;; ❌ BAD: No check
(defn connect-to-graph [client-schema graph-id]
  (join-branch graph-id))
```

**PR Review Action:**

- Look for `compare-schemas` call
- Verify action taken for each outcome

---

### Standard 2: Migration Ops Generated

**Requirement:** Local migrations must generate RTC ops

```clojure
;; ✅ GOOD: Converts to ops
(defn run-migration-with-sync [conn migration]
  (let [result (apply-migration conn migration)]
    ;; Generate ops for RTC sync
    (when (rtc-connected?)
      (let [ops (migration-results=>client-ops result)]
        (send-to-rtc-server ops)))
    result))

;; ❌ BAD: Migration not synced
(defn run-migration [conn migration]
  (apply-migration conn migration))
```

**PR Review Action:**

- Check if migration synced
- Verify op conversion exists

---

### Standard 3: Conflict Resolution Defined

**Requirement:** Must specify how conflicts are resolved

```clojure
;; ✅ GOOD: Explicit resolution strategy
(defn handle-concurrent-property-creation [local-op remote-op]
  (if (= (:property-name local-op) (:property-name remote-op))
    ;; Same property name - use last-write-wins
    (if (> (:timestamp remote-op) (:timestamp local-op))
      {:action :accept-remote :discard :local}
      {:action :reject-remote :keep :local})
    ;; Different names - both can proceed
    {:action :accept-both}))

;; ❌ BAD: No resolution strategy
(defn handle-sync-conflict [local-op remote-op]
  ;; What happens here?
  (log/warn "Conflict detected"))
```

**PR Review Action:**

- Check for conflict handling
- Verify resolution strategy documented

---

## Testing Requirements

### Requirement 1: Unit Tests for Migrations

**Must have:**

- [ ] Test migration on empty graph
- [ ] Test migration on populated graph
- [ ] Test migration is idempotent (run twice)
- [ ] Test migration handles missing data
- [ ] Test migration handles malformed data
- [ ] Test migration performance (large dataset)

**Example:**

```clojure
(deftest migrate-priority-comprehensive-test
  (testing "empty graph"
    (let [conn (create-empty-graph)]
      (is (no-error? (migrate conn {:type :properties ...})))))

  (testing "populated graph"
    (let [conn (create-graph-with-data 1000)]
      (is (no-error? (migrate conn {:type :properties ...})))
      (is (= 1000 (count-blocks-with-priority conn)))))

  (testing "idempotent"
    (let [conn (create-graph-with-data 100)]
      (migrate conn {:type :properties ...})
      (let [state-1 (export-graph conn)]
        (migrate conn {:type :properties ...})
        (let [state-2 (export-graph conn)]
          (is (= state-1 state-2)))))))
```

---

### Requirement 2: Integration Tests for Import/Export

**Must have:**

- [ ] Export graph at version N, import at version N
- [ ] Export graph at version N, import at version N+k
- [ ] Export invalid graph, verify export blocked/warned
- [ ] Import with validation errors, verify handled
- [ ] Export/import large graph (10k+ blocks)

---

### Requirement 3: E2E Tests for RTC Sync

**Must have:**

- [ ] Two clients same version, sync operations
- [ ] Two clients different minor versions, sync operations
- [ ] Client major version mismatch, verify branching
- [ ] Migration during active sync
- [ ] Conflict resolution scenario

---

### Requirement 4: Performance Tests

**Must measure:**

- [ ] Migration time for graphs of sizes: 100, 1k, 10k, 100k blocks
- [ ] Validation time for full graph validation
- [ ] Export time for large graphs
- [ ] Import time for large graphs
- [ ] Memory usage during migration

**Acceptance criteria:**

- Migrations < 5 seconds for 10k blocks
- Validation < 10 seconds for 100k blocks
- Export < 30 seconds for 100k blocks
- Memory < 2x graph size

---

## Deployment Guidelines

### Phase 1: Pre-Deployment

1. **Code Review Approval**

   - All checklist items passed
   - At least 2 reviewers approved
   - Security review if needed

2. **Testing Verification**

   - All tests passing
   - Manual testing completed
   - Performance benchmarks acceptable

3. **Documentation Update**

   - CHANGELOG.md updated
   - API documentation updated
   - Migration guide written (if breaking)

4. **Rollback Plan**
   - Recovery procedure documented
   - Rollback tested in staging
   - Contact list for emergency

---

### Phase 2: Staged Rollout

1. **Internal Testing (Day 1-3)**

   - Deploy to internal team
   - Monitor metrics closely
   - Gather feedback

2. **Beta Release (Day 4-7)**

   - Deploy to beta testers
   - Monitor migration success rate
   - Check for unexpected errors

3. **Gradual Rollout (Week 2)**
   - 10% of users Day 1
   - 25% of users Day 2
   - 50% of users Day 3
   - 100% of users Day 4

---

### Phase 3: Post-Deployment

1. **Monitoring (Week 1)**

   - Migration success rate
   - Validation error rate
   - Export/import failures
   - User support tickets

2. **Incident Response**

   - Define incident severity
   - Establish communication channel
   - Prepare rollback procedure

3. **Retrospective (Week 2)**
   - What went well
   - What went wrong
   - Process improvements

---

## Code Review Examples

### Example 1: Property Addition (Good)

```clojure
;; PR Description:
;; Add :block/priority property to enable task prioritization
;; Schema version: 65.15 → 65.16 (minor bump - additive change)

;; 1. Schema version bump
(def schema-version {:major 65 :minor 16})  ;; ✅

;; 2. Built-in property definition
(def built-in-properties
  {:block/priority
   {:schema {:type :number
             :cardinality :one}
    :description "Task priority (0-9)"}})  ;; ✅

;; 3. Migration entry
{"65.16"
 [{:type :properties
   :properties {:block/priority
                {:schema {:type :number
                          :cardinality :one}
                 :default 5}}}]}  ;; ✅

;; 4. Migration implementation (idempotent)
(defn add-priority-property [conn]
  (when-not (property-exists? @conn :block/priority)  ;; ✅ Idempotent
    (create-property! conn :block/priority {...})))

;; 5. Validation schema
(def block-schema
  [:map
   [:block/priority {:optional true}  ;; ✅ Optional - doesn't break existing
    [:and :number [:>= 0] [:<= 9]]]])

;; 6. Tests
(deftest add-priority-test
  (testing "adds property to graph" ...)
  (testing "doesn't overwrite existing" ...)
  (testing "idempotent" ...))  ;; ✅ Comprehensive
```

**Reviewer Feedback:** ✅ LGTM - Follows all standards

---

### Example 2: Property Type Change (Bad → Fixed)

```clojure
;; ❌ INITIAL PR (BAD)

;; PR Description: Change priority from string to number

;; Schema version: 65.15 → 65.16 (minor bump)  ;; ❌ Should be major!

;; Migration
{"65.16"
 [{:type :rename-db-idents
   :renames {:block/priority-str :block/priority}}]}  ;; ❌ Doesn't transform data!

;; Validation
(def block-schema
  [:map
   [:block/priority :number]])  ;; ❌ Breaks existing string values!
```

**Reviewer Feedback:**

```
❌ This is a BREAKING change:
1. Bump major version (65 → 66)
2. Need data transformation migration, not just rename
3. Validation will reject existing data
4. Need to test on graphs with string priorities
5. Consider deprecation period instead

Please revise.
```

```clojure
;; ✅ REVISED PR (GOOD)

;; PR Description: Deprecate string priority, migrate to number
;; Schema version: 65.16 → 66.0 (major bump - breaking change)  ;; ✅

;; Migration with transformation
{"66.0"
 [{:type :fix
   :fn (fn [conn]
         (let [blocks (d/q '[:find ?e ?priority
                            :where
                            [?e :block/priority ?priority]
                            [(string? ?priority)]]  ;; ✅ Finds string values
                          @conn)]
           (when (seq blocks)
             (d/transact! conn
               (for [[e priority-str] blocks]
                 {:db/id e
                  :block/priority (parse-priority priority-str)})))))}]}  ;; ✅ Transforms

;; Helper function
(defn parse-priority [s]
  (case s
    "urgent" 9
    "high" 7
    "medium" 5
    "low" 3
    5))  ;; ✅ Handles all cases

;; Validation (allows both during transition)
(def block-schema
  [:map
   [:block/priority
    [:or :number :string]]])  ;; ✅ Backward compatible during migration

;; Tests
(deftest migrate-priority-to-number-test
  (testing "transforms string priorities"
    (let [conn (create-graph-with-string-priorities)]
      (migrate conn {:major 65 :minor 16} {:major 66 :minor 0})
      (is (every? number? (get-all-priorities conn)))))  ;; ✅ Tests transformation

  (testing "handles unknown strings"
    ...))
```

**Reviewer Feedback:** ✅ LGTM - Breaking change handled correctly

---

## Quick Reference Card

### Schema Change → Action Required

| Change Type            | Version Bump | Migration | Validation | RTC Impact |
| ---------------------- | ------------ | --------- | ---------- | ---------- |
| **Add property**       | Minor        | Optional  | Optional   | Ops        |
| **Remove property**    | Major        | Required  | Update     | Branch     |
| **Rename property**    | Major        | Required  | Update     | Ops        |
| **Change type**        | Major        | Required  | Update     | Branch     |
| **Change cardinality** | Major        | Required  | Update     | Branch     |
| **Add entity type**    | Minor        | Optional  | Add        | Ops        |
| **Tighten validation** | Major        | Optional  | Update     | Compatible |
| **Relax validation**   | Minor        | None      | Update     | Compatible |

### PR Review Quick Checks

- [ ] Version bumped?
- [ ] Migration added?
- [ ] Migration idempotent?
- [ ] Tests comprehensive?
- [ ] Error handling defensive?
- [ ] Performance acceptable?
- [ ] Documentation updated?
- [ ] Rollback plan exists?

### When to Reject PR

- ❌ Major change with minor version bump
- ❌ Breaking change without migration
- ❌ Migration not idempotent
- ❌ No tests for migration
- ❌ Validation breaks existing data
- ❌ No error handling
- ❌ Performance not tested

---

## Summary

These guiding principles and standards ensure that schema evolution in Logseq is:

1. **Safe:** User data protected at all times
2. **Predictable:** Changes follow consistent patterns
3. **Testable:** Comprehensive test coverage
4. **Recoverable:** Users can recover from failures
5. **Communicative:** Clear errors and documentation

**Remember:** When in doubt during PR review, ask:

> "If this breaks, can users recover their data?"

If the answer is no, require changes before approval.

---

## Additional Resources

- **Part 1:** Executive Summary & Overview
- **Part 2:** Schema System Technical Analysis
- **Part 3:** Migration System Deep Dive
- **Part 4:** Import/Export & Validation Analysis
- **Part 5:** RTC Synchronization Analysis
- **Part 6:** Failure Patterns & Recommendations
- **Part 8:** Investigation Index

---

## Feedback & Updates

This document is a living standard. Please:

- Submit PRs for improvements
- Add examples from real incidents
- Update based on lessons learned

Last Updated: December 2024
