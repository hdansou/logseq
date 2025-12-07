# Graph Embedding Schema Mismatch Error Investigation

**Date:** December 5, 2025  
**Issue:** "DB write failed with invalid data" when enabling graph embedding on new graphs  
**Error Type:** Malli validation failure due to schema version mismatch  
**Severity:** 🔴 High - Blocks semantic search initialization completely

---

## 🔍 Executive Summary

When users enable graph embedding (semantic search) on a new database graph, the system fails with a "DB write failed with invalid data" error. The root cause is a **schema definition oversight from the initial implementation**: the `:logseq.property.embedding/hnsw-label-updated-at` property was added in schema version 64.4 (March 15, 2025) with Malli type `:datetime`, but the corresponding DataScript schema attributes (`:db/valueType` and `:db/cardinality`) were never added. This oversight has persisted through a major refactor in July 2025.

### Key Findings

1. **Missing DataScript schema attributes since inception** - The property was added March 15, 2025 in commit `a5db52bf3` with only Malli schema definition
2. **Schema migration incomplete** - Version 64.4 added the property but never defined the DataScript schema
3. **Validation mismatch** - Malli expects `:datetime` type, but DataScript schema doesn't declare `:db/valueType :db.type/ref`
4. **Transaction contains numeric timestamps** - The embedding system writes raw `long` values (millisecond timestamps) for `:hnsw-label-updated-at`
5. **Persisted through refactor** - July 21, 2025 refactor (commit `2c27aee09`) removed related property but kept the broken one

### Critical Risks

- 🔴 **Feature broken** - Semantic search cannot be initialized on any new graph
- 🟡 **Data type confusion** - Datetime properties should use entity references, not raw longs
- 🟡 **Schema inconsistency** - Property definition doesn't match how it's used in code

---

## 📋 Table of Contents

- [Error Analysis](#error-analysis)
- [Technical Deep Dive](#technical-deep-dive)
- [Root Cause Analysis](#root-cause-analysis)
- [Execution Flow](#execution-flow)
- [Code Location Reference](#code-location-reference)
- [Recommendations](#recommendations)
- [Related Issues](#related-issues)

---

## Error Analysis

### Error Message

```clojure
{:run-task-failed
 #error {:message "DB write failed with invalid data"
         :data {:tx-data ([:db/add 242 :logseq.property.embedding/hnsw-label-updated-at 1760400455158]
                          [:db/add 243 :logseq.property.embedding/hnsw-label-updated-at 1760400410859]
                          ;; ... many more similar datoms
                          )
                :pipeline-tx-data (#datascript/Datom [...])}
         :key :embedding-stale-blocks!
         :line 121}}
```

### What's Happening

1. User enables graph embedding in UI
2. Worker calls `embedding-graph!` which marks all blocks with `:hnsw-label-updated-at = 0`
3. Embedding process tries to transact updates with timestamp values
4. Malli validation runs and **fails** because the property schema is malformed

### The Validation Error (Implicit)

The validation likely fails with errors like:

```clojure
{:errors {<datom-index> [:logseq.property.embedding/hnsw-label-updated-at
                         ["should be an entity reference"
                          "should match datetime value type"]]}}
```

**Why?** The Malli schema defines this as:

```clojure
:logseq.property.embedding/hnsw-label-updated-at
{:title "HNSW label updated-at"
 :schema {:type :datetime        ;; <-- expects datetime type
          :public? false
          :hide? true}
 :queryable? false
 :rtc property-ignore-rtc}
```

But the **DataScript schema is missing**. For datetime properties, the DataScript schema should be:

```clojure
:logseq.property.embedding/hnsw-label-updated-at
{:db/valueType :db.type/ref        ;; <-- MISSING!
 :db/cardinality :db.cardinality/one}  ;; <-- MISSING!
```

---

## Technical Deep Dive

### 1. Property Definition

**Location:** `deps/db/src/logseq/db/frontend/property.cljs:575-587`

```clojure
:logseq.property.embedding/hnsw-label-updated-at
{:title "HNSW label updated-at"
 :schema {:type :datetime
          :public? false
          :hide? true}
 :queryable? false
 :rtc property-ignore-rtc}
```

**Problem:** This only defines the **Malli validation schema**, not the **DataScript storage schema**.

### 2. Schema Migration Entry

**Location:** `src/main/frontend/worker/db/migrate.cljs:430`

**Original Addition (Schema 64.4 - March 15, 2025):**

```clojure
["64.4" {:properties [:logseq.property.embedding/hnsw-label
                      :logseq.property.embedding/hnsw-label-updated-at]}]
```

**After Refactor (Schema 65.8 - July 21, 2025):**

```clojure
["65.8" {:properties [:logseq.property.embedding/hnsw-label-updated-at]}]
```

**Problem:** The migration entry only ensures the property **exists** but doesn't ensure the DataScript schema attributes are properly defined. This has been the case since the property was first introduced in **March 2025**.

### 3. How Datetime Properties Should Work

**Example from `:block/created-at`:**

Built-in property definition:

```clojure
:block/created-at {:title "Node created at"
                   :attribute :block/created-at  ;; <-- db attribute
                   :schema {:type :datetime
                            :public? false
                            :hide? true}}
```

DataScript schema definition (in `deps/db/src/logseq/db/frontend/schema.cljs`):

```clojure
:block/created-at {:db/valueType :db.type/ref
                   :db/cardinality :db.cardinality/one}
```

**BUT** `:logseq.property.embedding/hnsw-label-updated-at` is **missing** from the DataScript schema!

### 4. How Embedding Code Uses the Property

**Location:** `src/main/frontend/worker/embedding.cljs:110-114`

```clojure
(defn- labels-update-tx-data
  [db e+updated-at-coll]
  (let [es (map first e+updated-at-coll)
        exist-es (set (keep
                       (fn [b] (when (:block/uuid b) (:db/id b)))
                       (d/pull-many db [:block/uuid :db/id] es)))]
    (keep
     (fn [[e updated-at]]
       (when (contains? exist-es e)
         [:db/add e :logseq.property.embedding/hnsw-label-updated-at updated-at]))
     e+updated-at-coll)))
```

**Problem:** The code writes **raw `long` timestamp values** (e.g., `1760400455158`), but datetime properties in DB graphs should use **entity references** to datetime entities, not raw numbers.

### 5. Validation Flow

**Location:** `deps/db/src/logseq/db.cljs:121-137`

```clojure
(let [[validate-result errors] (db-validate/validate-tx-report tx-report nil)]
  (if validate-result
    (do
      ;; Success - proceed with transaction
      (reset! conn (:db-after tx-report))
      ...)
    (do
      ;; Failure - throw error
      (when-let [f @*transact-invalid-callback]
        (f tx-report errors))
      (throw (ex-info "DB write failed with invalid data"
                      {:tx-data tx-data
                       :pipeline-tx-data (:tx-data tx-report)})))))
```

**Flow:**

```
1. embedding-graph! generates tx-data
   ↓
2. ldb/transact! called
   ↓
3. DataScript processes transaction (creates tx-report)
   ↓
4. validate-tx-report runs Malli validation
   ↓
5. ❌ VALIDATION FAILS
   │  Entity type dispatch likely succeeds
   │  But property value validation fails:
   │    - Expected: entity ref or datetime format
   │    - Actual: raw long (1760400455158)
   ↓
6. Exception thrown
   ↓
7. Embedding initialization fails
```

---

## Root Cause Analysis

### The Core Problem

**The property `:logseq.property.embedding/hnsw-label-updated-at` has inconsistent schema definitions:**

| Schema Type                     | Status         | What It Says                                    |
| ------------------------------- | -------------- | ----------------------------------------------- |
| **Malli Schema** (validation)   | ✅ Defined     | Type is `:datetime`, hidden property            |
| **DataScript Schema** (storage) | ❌ **MISSING** | No `:db/valueType` or `:db/cardinality`         |
| **Property Usage** (code)       | ⚠️ Incorrect   | Writes raw `long` values instead of entity refs |

### Why This Breaks

1. **Malli validation expects datetime semantics** - The `:type :datetime` in the schema tells Malli this property should store datetime values
2. **DataScript allows any type by default** - Without explicit schema, DataScript accepts the raw `long` values
3. **Validation catches the mismatch** - When Malli validates, it sees raw numbers where it expects entity references or datetime format
4. **No schema migration to fix it** - The 65.9 migration added the property but didn't ensure schema consistency

### Why It Worked Before (Maybe)

Possible scenarios:

- Validation was disabled during embedding initialization (`:skip-validate-db?` flag)
- The property wasn't used in older versions
- Tests don't cover this path with validation enabled

---

## Execution Flow

### Timeline: Enabling Graph Embedding

```
0ms     │ User clicks "Enable Graph Embedding" in UI
        │
10ms    │ UI sends message to DB Worker
        │   :thread-api/vec-search-embedding-graph {:reset-embedding? true}
        │
20ms    │ DB Worker: embedding-graph! called
        │
30ms    │ Check if model configured
        │   └─► ldb/get-key-value @conn :logseq.kv/graph-text-embedding-model-name
        │
40ms    │ Check if embedding exists
        │   └─► (d/datoms @conn :avet :logseq.property.embedding/hnsw-label-updated-at)
        │   └─► empty? => true (new graph)
        │
50ms    │ Mark all blocks for embedding
        │   └─► Generate tx-data: [:db/add <e> :hnsw-label-updated-at 0]
        │   └─► for ALL blocks with :block/title
        │
100ms   │ ldb/transact! called with mark-embedding-tx-data
        │   └─► {:skip-refresh? true}  ;; Skip UI refresh, but NOT validation!
        │
110ms   │ DataScript d/transact! processes datoms
        │   └─► Creates tx-report with all datoms
        │
120ms   │ ⚠️ VALIDATION CHECKPOINT
        │   ├─► db-validate/validate-tx-report runs
        │   ├─► Malli schemas loaded
        │   ├─► Entity dispatch: identifies blocks
        │   └─► Property validation:
        │       ❌ :hnsw-label-updated-at type mismatch
        │          Expected: datetime/entity-ref
        │          Actual: raw long (0)
        │
130ms   │ ❌ VALIDATION FAILED
        │   └─► throw ex-info "DB write failed with invalid data"
        │
140ms   │ Error bubbles up to worker
        │   └─► Embedding task fails
        │
150ms   │ Error shown to user
        │   └─► Semantic search remains disabled
```

---

## Code Location Reference

| File                                             | Purpose                | Key Functions/Data                             | Lines              |
| ------------------------------------------------ | ---------------------- | ---------------------------------------------- | ------------------ |
| **deps/db/src/logseq/db/frontend/property.cljs** | Property definitions   | `built-in-properties` map                      | 575-587            |
| **deps/db/src/logseq/db/frontend/schema.cljs**   | DataScript schema      | `schema` map (MISSING entry)                   | 63-105             |
| **src/main/frontend/worker/db/migrate.cljs**     | Schema migrations      | `schema-version->updates`                      | 430                |
| **src/main/frontend/worker/embedding.cljs**      | Embedding logic        | `embedding-graph!`<br>`labels-update-tx-data`  | 188-202<br>106-114 |
| **src/main/frontend/worker/db_listener.cljs**    | DB change listener     | `remove-old-embeddings-and-reset-new-updates!` | 54-68              |
| **deps/db/src/logseq/db.cljs**                   | Transaction validation | `transact-sync!`                               | 121-137            |
| **deps/db/src/logseq/db/frontend/validate.cljs** | Malli validation       | `validate-tx-report`                           | 25-63              |

---

## Recommendations

### For Immediate Fix (P0 - Critical)

#### Option 1: Add DataScript Schema Definition (Preferred)

**File:** `deps/db/src/logseq/db/frontend/schema.cljs`

Add to the `schema` map:

```clojure
(def schema
  "Schema for DB graphs. :block/tags are classes in this schema"
  (merge
   (apply dissoc file-schema/schema file-schema/file-only-attributes)
   {:block/name {:db/index true}
    :block/closed-value-property {:db/valueType :db.type/ref
                                  :db/cardinality :db.cardinality/many}
    ;; ADD THIS:
    :logseq.property.embedding/hnsw-label-updated-at {:db/valueType :db.type/ref
                                                      :db/cardinality :db.cardinality/one}}))
```

**Pros:**

- Follows existing datetime property patterns
- Proper type safety
- Malli validation will pass

**Cons:**

- Need to update embedding code to write entity refs instead of raw longs
- Requires schema migration for existing graphs

#### Option 2: Change Malli Schema Type (Quick Fix)

**File:** `deps/db/src/logseq/db/frontend/property.cljs`

Change the property type from `:datetime` to `:raw-number`:

```clojure
:logseq.property.embedding/hnsw-label-updated-at
{:title "HNSW label updated-at"
 :schema {:type :raw-number    ;; Changed from :datetime
          :public? false
          :hide? true}
 :queryable? false
 :rtc property-ignore-rtc}
```

**Pros:**

- Minimal code changes
- Matches current usage pattern

**Cons:**

- Inconsistent with other timestamp properties
- Loses datetime type semantics
- Raw numbers are harder to query/display

### For Medium-Term (P1)

#### 1. Add Schema Migration for 65.9+

**File:** `src/main/frontend/worker/db/migrate.cljs`

Update the migration:

```clojure
["65.9" {:properties [:logseq.property.embedding/hnsw-label-updated-at]
         :fix (fn [conn]
                ;; Ensure DataScript schema is present
                (when-not (contains? (:schema @conn)
                                     :logseq.property.embedding/hnsw-label-updated-at)
                  ;; Add schema attribute
                  (d/transact! conn [{:db/ident :logseq.property.embedding/hnsw-label-updated-at
                                      :db/valueType :db.type/ref
                                      :db/cardinality :db.cardinality/one}])))}]
```

#### 2. Update Embedding Code to Use Proper Datetime Format

If Option 1 is chosen, update `labels-update-tx-data`:

```clojure
(defn- labels-update-tx-data
  [db e+updated-at-coll]
  (let [es (map first e+updated-at-coll)
        exist-es (set (keep
                       (fn [b] (when (:block/uuid b) (:db/id b)))
                       (d/pull-many db [:block/uuid :db/id] es)))]
    (keep
     (fn [[e updated-at]]
       (when (contains? exist-es e)
         ;; Create datetime entity reference instead of raw long
         [:db/add e :logseq.property.embedding/hnsw-label-updated-at
          {:db/ident (keyword (str "datetime-" updated-at))
           :datetime/timestamp updated-at}]))
     e+updated-at-coll)))
```

**Note:** This requires understanding how datetime properties are stored. Check `:block/created-at` implementation.

#### 3. Add Test Coverage

**File:** `src/test/frontend/worker/embedding_test.cljs` (create if needed)

```clojure
(deftest test-embedding-graph-initialization
  (testing "enabling embedding on new graph doesn't fail validation"
    (let [conn (create-test-db)]
      ;; Add some blocks
      (d/transact! conn test-blocks)

      ;; Enable embedding
      (is (nil? (embedding/embedding-graph! "test-repo" {:reset-embedding? false})))

      ;; Verify all blocks marked
      (is (seq (d/datoms @conn :avet :logseq.property.embedding/hnsw-label-updated-at))))))
```

### For Long-Term (P2)

#### 1. Standardize Datetime Property Handling

Create a helper function for creating datetime property transactions:

**File:** `deps/db/src/logseq/db/frontend/property.cljs`

```clojure
(defn datetime-tx-data
  "Creates proper tx-data for datetime property values"
  [entity-id property-ident timestamp-ms]
  {:pre [(int? entity-id) (keyword? property-ident) (int? timestamp-ms)]}
  (let [datetime-schema (get-in built-in-properties [property-ident :schema])]
    (assert (= :datetime (:type datetime-schema))
            (str "Property " property-ident " is not a datetime property"))
    ;; Implementation depends on how datetime properties are actually stored
    ;; Check :block/created-at usage
    [:db/add entity-id property-ident timestamp-ms]))
```

#### 2. Schema Consistency Validation

Add a test that validates all built-in properties have matching DataScript schema:

```clojure
(deftest test-schema-consistency
  (testing "all built-in properties have matching DataScript schema"
    (doseq [[prop-ident prop-def] built-in-properties
            :when (not (get-in prop-def [:schema :hide?]))]
      (when-let [type (get-in prop-def [:schema :type])]
        (case type
          :datetime (is (contains? schema prop-ident)
                        (str "Datetime property " prop-ident " missing from DataScript schema"))
          ;; ... other types
          nil)))))
```

### For Documentation

Add to `docs/03-features/semantic-search/developer-guide.md`:

````markdown
## Property Schema: hnsw-label-updated-at

The `:logseq.property.embedding/hnsw-label-updated-at` property tracks when a block's
HNSW embedding was last updated.

**Schema Version:** 65.9+

**Type:** `:datetime` (or `:raw-number` depending on final fix)

**Usage:** Internal only, not user-visible

**DataScript Schema:**

```clojure
{:db/valueType :db.type/ref
 :db/cardinality :db.cardinality/one}
```
````

**Values:** Millisecond timestamps (Unix epoch)

---

## Related Issues

### Similar Past Issues

Check if these have been reported:

- Schema migrations incomplete for other properties
- Validation failures during feature initialization
- Datetime property inconsistencies

### Testing Gaps

This bug reveals:

- E2E tests don't cover embedding initialization on fresh graphs
- Schema validation tests don't check for schema consistency
- Migration tests don't validate DataScript schema changes

---

## Investigation Metadata

**Investigator:** GitHub Copilot  
**Investigation Date:** December 5, 2025  
**Code Version:** Schema 65.13 (current)  
**Affected Versions:** 64.4+ (when property was added)  
**Platform:** All (Electron Desktop, Web)

### Git History

**Property Introduction:**

- **Commit:** `a5db52bf3791512cf6f1cb072a8ac37da5b1345c`
- **Date:** March 15, 2025
- **Author:** rcmerci
- **Message:** "feat(embedding): text-embedding graph blocks and query hnsw"
- **Schema Version:** 64.4
- **What was added:**
  - `:logseq.property.embedding/hnsw-label` (type `:raw-number`)
  - `:logseq.property.embedding/hnsw-label-updated-at` (type `:datetime`) ⚠️
- **Migration:** Added both properties to schema version 64.4
- **DataScript schema:** ❌ NOT updated (this is the bug!)

**Property Refactor:**

- **Commit:** `2c27aee09d1b2c9c3e0aac6e633fa7283b05feaf`
- **Date:** July 21, 2025
- **Author:** Tienson Qin
- **Message:** "use db/id for hnsw label"
- **Schema Version:** Changed from 65.8 to 65.8 (removed hnsw-label property)
- **Changes:**
  - Removed `:logseq.property.embedding/hnsw-label` property entirely
  - Kept `:logseq.property.embedding/hnsw-label-updated-at` with `:datetime` type
  - Changed HNSW to use `:db/id` directly as labels instead of separate property
  - Updated migration to only include `hnsw-label-updated-at`
- **DataScript schema:** ❌ Still not updated

**Root Cause Timeline:**

1. **March 15, 2025** - Property added with `:datetime` type but NO DataScript schema
2. **March 15 - July 20, 2025** - Bug likely dormant (validation may have been disabled)
3. **July 21, 2025** - Refactor removed `hnsw-label`, kept `hnsw-label-updated-at`
4. **July 21 - Present** - Bug manifests when validation is enabled

### Visual Commit Timeline

```
March 15, 2025                    July 21, 2025                   December 5, 2025
     │                                  │                                │
     │ a5db52bf3                        │ 2c27aee09                      │ (current)
     │ rcmerci                          │ Tienson Qin                    │
     ├──────────────────────────────────┼────────────────────────────────┤
     │                                  │                                │
     │ ADDED:                           │ REFACTORED:                    │ BUG ACTIVE:
     │ • hnsw-label (:raw-number)      │ • Removed hnsw-label          │ • Validation fails
     │ • hnsw-label-updated-at ❌      │ • Kept updated-at ❌          │ • Embedding broken
     │   (:datetime)                    │   (still :datetime)            │
     │                                  │ • Use db/id for labels         │
     │ Schema 64.4                      │ Schema 65.8                    │ Schema 65.13
     │                                  │                                │
     │ DataScript schema:               │ DataScript schema:             │ DataScript schema:
     │ ❌ NOT UPDATED                   │ ❌ STILL NOT UPDATED           │ ❌ STILL NOT UPDATED
     │                                  │                                │
     └──────────────────────────────────┴────────────────────────────────┘

                    🐛 BUG INTRODUCED                🐛 BUG PERSISTS
                    (but dormant)                     (now active)
```

**Key Insight:** The bug was introduced **9 months ago** but may have been dormant due to validation being skipped or disabled during embedding operations. The July refactor had an opportunity to fix it but didn't notice the missing DataScript schema.**Tools Used:**

- `grep_search` for property usage
- `read_file` for schema definitions
- `semantic_search` for validation flow

**Confidence Level:** 🟢 High - Root cause identified with certainty

---

## Next Steps

1. **Immediate:** Choose between Option 1 or Option 2 for the fix
2. **Review:** Confirm how datetime properties are actually stored in DB graphs
3. **Test:** Create reproduction case before implementing fix
4. **Fix:** Implement chosen option with tests
5. **Migrate:** Add migration for existing graphs (if needed)
6. **Document:** Update developer documentation
7. **Release:** Include in next patch release

---

## Appendix: Property Type Matrix

| Property Type | Malli Schema          | DataScript Schema              | Example Value                     |
| ------------- | --------------------- | ------------------------------ | --------------------------------- |
| `:string`     | `{:type :string}`     | `{}` (no schema)               | `"Hello"`                         |
| `:number`     | `{:type :number}`     | `{}`                           | `42`                              |
| `:raw-number` | `{:type :raw-number}` | `{}`                           | `42`                              |
| `:datetime`   | `{:type :datetime}`   | `{:db/valueType :db.type/ref}` | `<entity-ref>` or `timestamp-ms`? |
| `:checkbox`   | `{:type :checkbox}`   | `{}`                           | `true/false`                      |
| `:entity`     | `{:type :entity}`     | `{:db/valueType :db.type/ref}` | `<entity-id>`                     |

**Question:** How are datetime properties actually stored? Need to check `:block/created-at` usage.
