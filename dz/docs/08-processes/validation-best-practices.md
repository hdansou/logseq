# Validation Best Practices

**Document Type:** Process Guide  
**Last Updated:** December 28, 2025  
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

Logseq uses a **dual validation system** combining DataScript schema validation (structural constraints) with Malli schema validation (business logic rules). Understanding when and how to use each system is critical for maintaining data integrity.

**Key Principle:** Validation should be strict enough to prevent invalid data but flexible enough to handle edge cases and migrations.

---

## Table of Contents

1. [Dual Validation Architecture](#dual-validation-architecture)
2. [DataScript Validation](#datascript-validation)
3. [Malli Validation](#malli-validation)
4. [Entity Type Dispatch](#entity-type-dispatch)
5. [Adding Validation Rules](#adding-validation-rules)
6. [Property Value Validation](#property-value-validation)
7. [Performance Considerations](#performance-considerations)
8. [Error Handling](#error-handling)
9. [Fixing Invalid Data](#fixing-invalid-data)
10. [Testing Validation](#testing-validation)

---

## Dual Validation Architecture

### Why Two Systems?

Logseq uses two complementary validation approaches:

| Aspect          | DataScript Schema                                                      | Malli Schema                                                                                 |
| --------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **When**        | During transaction                                                     | After transaction                                                                            |
| **What**        | Structural constraints                                                 | Business logic rules                                                                         |
| **Enforces**    | Types, cardinality, uniqueness, refs                                   | Required fields, formats, relationships                                                      |
| **Performance** | Fast (built-in)                                                        | Slower (runs on every entity)                                                                |
| **Flexibility** | Limited                                                                | Highly flexible                                                                              |
| **Location**    | [deps/db/src/logseq/db/schema.cljs](deps/db/src/logseq/db/schema.cljs) | [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs) |

### When to Use Each

**Use DataScript schema when:**

- Defining attribute types (`:db.type/ref`, `:db.type/string`)
- Setting cardinality (`:db.cardinality/one`, `:db.cardinality/many`)
- Enforcing uniqueness (`:db/unique :db.unique/identity`)
- Indexing attributes (`:db/index true`)

**Use Malli schema when:**

- Validating property values match expected format
- Enforcing required fields on entity types
- Checking relationships between properties
- Custom validation logic
- Generating helpful error messages

### Validation Flow

```
Transaction Submitted
       ↓
┌──────────────────┐
│ DataScript       │  ← Structural validation (type, cardinality, refs)
│ Schema Check     │
└─────┬────────────┘
      │ (if valid)
      ↓
┌──────────────────┐
│ Transaction      │
│ Applied to DB    │
└─────┬────────────┘
      │
      ↓
┌──────────────────┐
│ Malli            │  ← Business logic validation
│ Schema Check     │
└─────┬────────────┘
      │
      ↓
┌──────────────────┐
│ Valid or         │
│ Error Report     │
└──────────────────┘
```

---

## DataScript Validation

### Schema Definition Location

[deps/db/src/logseq/db/schema.cljs](deps/db/src/logseq/db/schema.cljs)

### Common DataScript Schema Attributes

```clojure
;; Basic attribute definition
{:db/ident :block/content
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/index true}

;; Reference attribute
{:db/ident :block/parent
 :db/valueType :db.type/ref
 :db/cardinality :db.cardinality/one
 :db/index true}

;; Many-valued attribute
{:db/ident :block/tags
 :db/valueType :db.type/ref
 :db/cardinality :db.cardinality/many
 :db/index true}

;; Unique identity
{:db/ident :block/uuid
 :db/valueType :db.type/uuid
 :db/cardinality :db.cardinality/one
 :db/unique :db.unique/identity}
```

### DataScript Value Types

```clojure
:db.type/ref        ; Reference to another entity
:db.type/keyword    ; Clojure keyword
:db.type/string     ; String value
:db.type/boolean    ; Boolean (true/false)
:db.type/long       ; Long integer
:db.type/bigint     ; Arbitrary precision integer
:db.type/float      ; Floating point number
:db.type/double     ; Double precision float
:db.type/bigdec     ; Arbitrary precision decimal
:db.type/instant    ; Timestamp
:db.type/uuid       ; UUID
:db.type/uri        ; URI
:db.type/symbol     ; Clojure symbol
```

### Cardinality

```clojure
;; Single value (default)
:db.cardinality/one

;; Multiple values (set semantics)
:db.cardinality/many
```

### Adding DataScript Attribute

When adding a property that needs DataScript validation:

```clojure
;; In deps/db/src/logseq/db/schema.cljs

;; 1. Add to schema definition
(def db-attribute-properties
  #{:block/uuid
    :block/parent
    :block/left
    ;; ... existing attributes ...
    :block/my-new-attribute})  ; Add here

;; 2. Define schema
(def schema
  {:block/my-new-attribute
   {:db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}})
```

---

## Malli Validation

### Validation Entry Point

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)

### Core Validation Functions

```clojure
;; Validate entities changed in transaction
(defn validate-tx-report
  "Validates only entities that changed in transaction"
  [db tx-report]
  ;; Returns [valid? error-data]
  )

;; Validate entire database
(defn validate-db!
  "Validates all entities in database"
  [db]
  ;; Returns validation report grouped by entity
  )
```

### When Validation Runs

**Transaction Validation:**

- Runs after every `d/transact!` call
- Only validates changed entities
- Fast, suitable for interactive use
- Controlled by `*validate-db?*` flag

**Full DB Validation:**

- Run manually via `validate-db!`
- Validates all entities
- Slower, used for integrity checks
- Used during import, migration testing

### Enabling/Disabling Validation

```clojure
;; In code
(binding [validate/*validate-db?* false]
  ;; Bulk operations without validation
  (d/transact! conn large-tx-data))

;; Normal operation (validation enabled)
(d/transact! conn tx-data)  ; Validates by default
```

**When to disable:**

- Large bulk imports (performance)
- Initial graph creation (bootstrap)
- Migration operations (fixed data)
- Testing with intentionally invalid data

**Always re-enable after bulk operation!**

---

## Entity Type Dispatch

### Multi-Method Dispatch System

Malli validation uses a multi-method to dispatch entity validation based on entity type:

```clojure
(defmulti entity-schema
  "Dispatch entity validation based on type"
  (fn [db entity] (entity-type db entity))
  :default :default)
```

### Entity Type Precedence (Critical!)

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)

```clojure
(defn entity-type
  "Determine entity type for validation dispatch.
   ORDER MATTERS! First match wins."
  [db entity]
  (cond
    (property-entity? entity) :property
    (class-entity? entity) :class
    (hidden-page? entity) :hidden
    (page? entity) :normal-page
    (asset-block? entity) :asset-block
    (file-block? entity) :file-block
    (property-history-block? entity) :property-history-block
    (closed-value-block? entity) :closed-value-block
    (property-value-block? entity) :property-value-block
    (block? entity) :block
    (kv-entity? entity) :logseq.kv
    (property-value-placeholder? entity) :property-value-placeholder
    :else :default))
```

**Why order matters:**

- Properties are also pages, but need different validation
- Classes are also pages with specific rules
- Must check specific types before general types

### Entity Type Predicates

```clojure
;; Check if entity is a property
(defn property-entity? [entity]
  (contains? (:block/type entity) "property"))

;; Check if entity is a class/tag
(defn class-entity? [entity]
  (and (page? entity)
       (or (contains? (:block/tags entity) :logseq.class/Tag)
           (= (:db/ident entity) :logseq.class/Root))))

;; Check if entity is a page
(defn page? [entity]
  (= "page" (:block/type entity)))

;; Check if entity is a block
(defn block? [entity]
  (and (some? (:block/parent entity))
       (not (page? entity))))
```

---

## Adding Validation Rules

### Step-by-Step Process

#### Step 1: Identify Entity Type

Determine which entity type needs validation:

- Property page: `:property`
- Class/tag page: `:class`
- Normal page: `:normal-page`
- Block: `:block`
- Property value: `:property-value-block`

#### Step 2: Add Entity Schema

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)

```clojure
;; Example: Add validation for property entities
(defmethod entity-schema :property
  [db entity]
  [:map {:closed true}  ; Closed schema - only listed keys allowed
   [:db/id :int]
   [:block/uuid uuid?]
   [:block/type [:= "page"]]
   [:block/original-name :string]
   [:block/created-at :int]
   [:block/updated-at :int]
   ;; Property-specific validations
   [:logseq.property/type {:optional true}
    [:enum :default :number :date :datetime :checkbox :url :node]]
   [:logseq.property/hide? {:optional true} :boolean]])
```

#### Step 3: Define Custom Validators (If Needed)

```clojure
;; Custom validator for specific format
(def email-regex #"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

(defmethod property-value-schema :logseq.property/email
  [_property db property-value]
  [:and
   [:string {:error/message "Email must be a string"}]
   [:re {:error/message "Invalid email format"} email-regex]])
```

#### Step 4: Test Validation

```clojure
(deftest test-email-validation
  (testing "Valid email passes"
    (is (valid-property-value? db :logseq.property/email "user@example.com")))

  (testing "Invalid email fails"
    (is (not (valid-property-value? db :logseq.property/email "not-an-email"))))

  (testing "Error message is clear"
    (let [[valid? errors] (validate-property-value db :logseq.property/email "bad")]
      (is (false? valid?))
      (is (str/includes? (first errors) "Invalid email format")))))
```

### Validation Schema Syntax

**Malli schema types:**

```clojure
;; Basic types
:string        ; String value
:int           ; Integer
:boolean       ; Boolean
:keyword       ; Clojure keyword
[:enum ...]    ; One of specified values

;; Composite types
[:map ...]     ; Map with specific keys
[:vector ...]  ; Vector of values
[:set ...]     ; Set of values
[:tuple ...]   ; Fixed-size sequence

;; Predicates
uuid?          ; UUID predicate
pos-int?       ; Positive integer
[:fn pred]     ; Custom predicate function

;; Combinators
[:and ...]     ; All schemas must pass
[:or ...]      ; Any schema must pass
[:not ...]     ; Schema must fail

;; Modifiers
{:optional true}     ; Key is optional
{:closed true}       ; No extra keys allowed
{:error/message ""}  ; Custom error message
```

### Common Validation Patterns

#### Required String Field

```clojure
[:block/name :string]
```

#### Optional Field

```clojure
[:logseq.property/description {:optional true} :string]
```

#### Enum Values

```clojure
[:logseq.property/status [:enum :todo :doing :done]]
```

#### Nested Map

```clojure
[:block/schema [:map
                [:type :keyword]
                [:cardinality [:enum :one :many]]]]
```

#### Custom Predicate

```clojure
[:block/order [:fn {:error/message "Order must be positive"}
               #(and (number? %) (pos? %))]]
```

#### Reference Validation

```clojure
[:block/parent [:fn {:error/message "Parent must be valid entity"}
                #(and (map? %) (contains? % :db/id))]]
```

---

## Property Value Validation

Property values have specialized validation based on property type.

### Property Value Multi-Dispatch

```clojure
(defmulti property-value-schema
  "Validate property value based on property type"
  (fn [property db property-value] (:logseq.property/type property))
  :default :default)
```

### Adding Property-Specific Validation

```clojure
;; Example: URL property validation
(defmethod property-value-schema :url
  [_property db property-value]
  [:and
   [:string {:error/message "URL must be a string"}]
   [:fn {:error/message "Must be valid URL"}
    (fn [s]
      (try
        (when (string? s)
          (let [url (js/URL. s)]
            (contains? #{"http:" "https:"} (.-protocol url))))
        (catch js/Error _ false)))]])

;; Example: Number property validation
(defmethod property-value-schema :number
  [_property db property-value]
  [:or
   [:int {:error/message "Must be an integer"}]
   [:double {:error/message "Must be a number"}]])

;; Example: Date property validation
(defmethod property-value-schema :date
  [_property db property-value]
  [:and
   [:int {:error/message "Date must be integer (journal day)"}]
   [:fn {:error/message "Must be valid journal day (YYYYMMDD)"}
    (fn [n]
      (and (>= n 19700101)
           (<= n 21000101)))]])
```

### Property Value Entity Schema

Property values are stored as separate entities:

```clojure
(defmethod entity-schema :property-value-block
  [db entity]
  [:map {:closed true}
   [:db/id :int]
   [:block/uuid uuid?]
   [:block/type [:= "property-value"]]
   [:block/schema [:map
                   [:type :keyword]
                   [:cardinality [:enum :one :many]]]]
   ;; Value stored in :block/title or :block/schema-value
   [:block/title {:optional true} :string]
   [:block/schema-value {:optional true} :any]
   [:block/created-at :int]
   [:block/updated-at :int]])
```

---

## Performance Considerations

### Validation Overhead

Malli validation adds overhead to every transaction:

- **Small transactions:** ~1-5ms overhead
- **Medium transactions:** ~10-50ms overhead
- **Large imports:** Can double transaction time

### Optimization Strategies

#### 1. Disable During Bulk Operations

```clojure
(defn import-large-graph [data]
  (binding [validate/*validate-db?* false]
    ;; Fast bulk import without validation
    (doseq [batch (partition-all 1000 data)]
      (d/transact! conn batch)))
  ;; Validate once at the end
  (when-let [errors (validate-db! @conn)]
    (log/error "Validation errors after import:" errors)))
```

#### 2. Validate Only Changed Entities

```clojure
;; validate-tx-report only checks changed entities
(let [tx-report (d/transact! conn tx-data)
      [valid? errors] (validate-tx-report @conn tx-report)]
  (when-not valid?
    (handle-validation-errors errors)))
```

#### 3. Cache Validation Schemas

```clojure
;; Malli automatically caches compiled schemas
;; But you can help by reusing schema definitions
(def common-block-schema
  [:map
   [:db/id :int]
   [:block/uuid uuid?]
   [:block/type :string]])

;; Reuse in multiple entity schemas
(defmethod entity-schema :block [db entity]
  [:merge common-block-schema
   [:map
    [:block/content :string]
    [:block/parent :any]]])
```

#### 4. Lazy Validation for Large Datasets

```clojure
(defn validate-entities-lazy [db entity-ids]
  "Validate entities lazily, reporting errors as found"
  (->> entity-ids
       (map #(d/entity db %))
       (keep (fn [entity]
               (let [[valid? errors] (validate-entity db entity)]
                 (when-not valid?
                   {:entity entity :errors errors}))))
       (take 100)))  ; Limit error count
```

### Performance Benchmarks

**Typical transaction validation times:**

- 1 entity: ~1ms
- 10 entities: ~5ms
- 100 entities: ~30ms
- 1000 entities: ~200ms

**Full DB validation times:**

- Small graph (1000 entities): ~1s
- Medium graph (10000 entities): ~10s
- Large graph (100000 entities): ~2min

---

## Error Handling

### Validation Error Structure

```clojure
{:entity-id 123
 :entity-type :block
 :errors [{:path [:block/content]
           :message "block/content is required"
           :schema [:string]
           :value nil}
          {:path [:block/uuid]
           :message "block/uuid must be a UUID"
           :schema uuid?
           :value "not-a-uuid"}]}
```

### Handling Validation Errors

#### Option 1: Fail Transaction

```clojure
(let [tx-report (d/transact! conn tx-data)
      [valid? errors] (validate-tx-report @conn tx-report)]
  (if valid?
    tx-report
    (throw (ex-info "Validation failed"
                    {:type :validation-error
                     :errors errors}))))
```

#### Option 2: Log and Continue

```clojure
(let [tx-report (d/transact! conn tx-data)
      [valid? errors] (validate-tx-report @conn tx-report)]
  (when-not valid?
    (log/warn "Validation errors (non-fatal):" errors))
  tx-report)
```

#### Option 3: Auto-Fix

```clojure
(let [tx-report (d/transact! conn tx-data)
      [valid? errors] (validate-tx-report @conn tx-report)]
  (if valid?
    tx-report
    (let [fix-tx (generate-fix-tx errors)]
      (log/info "Auto-fixing validation errors")
      (d/transact! conn fix-tx))))
```

### Custom Error Messages

```clojure
;; Add error messages to schema
[:block/order [:and
               [:int {:error/message "Order must be an integer"}]
               [:fn {:error/message "Order must be positive"}
                #(pos? %)]]]

;; Error message includes context
{:path [:block/order]
 :message "Order must be positive"
 :value -5}
```

### User-Facing Error Messages

```clojure
(defn format-validation-error-for-user [error]
  (let [{:keys [path message value]} error]
    (str "Invalid value for " (name (last path)) ": "
         message
         (when value (str " (got: " (pr-str value) ")")))))

;; Example output:
;; "Invalid value for order: Order must be positive (got: -5)"
```

---

## Fixing Invalid Data

### Fix Function Pattern

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)

```clojure
(defn fix-invalid-blocks!
  "Attempt to fix common validation errors"
  [conn validation-report]
  (let [fix-tx (->> validation-report
                    (mapcat generate-fix-tx-for-entity)
                    vec)]
    (when (seq fix-tx)
      (log/info "Applying fixes for" (count fix-tx) "issues")
      (d/transact! conn fix-tx))))
```

### Common Fix Patterns

#### 1. Retract Invalid Attribute

```clojure
(defn fix-invalid-attribute [entity-id attr]
  [:db/retract entity-id attr])
```

#### 2. Set Default Value

```clojure
(defn fix-missing-required-field [entity-id attr default]
  {:db/id entity-id
   attr default})
```

#### 3. Convert to Valid Type

```clojure
(defn fix-type-mismatch [entity-id attr value expected-type]
  (let [converted (convert-to-type value expected-type)]
    (if converted
      {:db/id entity-id attr converted}
      [:db/retract entity-id attr])))  ; Can't convert, remove it
```

#### 4. Remove Invalid Entity

```clojure
(defn fix-invalid-entity [entity-id]
  [:db/retractEntity entity-id])
```

### Fix Strategy Decision Tree

```
Validation Error
       ↓
   ┌───┴───┐
   │       │
Missing  Invalid Value
   │       │
   │       ↓
   │   ┌───┴───┐
   │   │       │
   │  Can    Cannot
   │ Convert Convert
   │   │       │
   ↓   ↓       ↓
 Set Convert Retract
Default Value  Attr
```

### Implementing Fix Function

```clojure
(defn generate-fix-tx-for-entity
  "Generate transaction data to fix validation errors on one entity"
  [{:keys [entity-id entity-type errors]}]
  (keep (fn [{:keys [path value]}]
          (let [attr (last path)]
            (cond
              ;; Missing required field - set default
              (nil? value)
              (when-let [default (default-value entity-type attr)]
                {:db/id entity-id attr default})

              ;; Invalid type - try to convert
              (type-mismatch? value attr)
              (if-let [converted (convert-type value attr)]
                {:db/id entity-id attr converted}
                [:db/retract entity-id attr])

              ;; Invalid format - retract
              :else
              [:db/retract entity-id attr])))
        errors))
```

---

## Testing Validation

### Test Structure

```clojure
(deftest test-block-validation
  (testing "Valid block passes validation"
    (let [valid-block {:db/id -1
                       :block/uuid (d/squuid)
                       :block/type "block"
                       :block/content "Valid content"
                       :block/parent {:db/id 2}
                       :block/created-at 123456
                       :block/updated-at 123456}]
      (is (valid-entity? db valid-block))))

  (testing "Missing required field fails"
    (let [invalid-block {:db/id -1
                         :block/uuid (d/squuid)
                         :block/type "block"}]
      (is (not (valid-entity? db invalid-block)))
      (is (validation-error-contains? invalid-block "content is required"))))

  (testing "Invalid type fails"
    (let [invalid-block {:db/id -1
                         :block/uuid "not-a-uuid"  ; Should be UUID
                         :block/type "block"
                         :block/content "Content"}]
      (is (not (valid-entity? db invalid-block)))
      (is (validation-error-contains? invalid-block "must be a UUID")))))
```

### Test Helpers

```clojure
(defn valid-entity? [db entity]
  (let [[valid? _] (validate-entity db entity)]
    valid?))

(defn validation-error-contains? [entity substring]
  (let [[_ errors] (validate-entity db entity)]
    (some #(str/includes? (:message %) substring) errors)))

(defn validation-errors [entity]
  (let [[_ errors] (validate-entity db entity)]
    errors))
```

### Testing Checklist

- [ ] Test valid data passes
- [ ] Test missing required fields fail
- [ ] Test invalid types fail
- [ ] Test invalid formats fail
- [ ] Test error messages are clear
- [ ] Test fix functions work correctly
- [ ] Test edge cases (nil, empty, special characters)
- [ ] Test performance with large datasets

---

## Quick Reference

### Adding New Validation Rule

1. Identify entity type in [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)
2. Add/modify `defmethod entity-schema` for that type
3. Use Malli schema syntax
4. Add custom error messages
5. Write tests (valid + invalid cases)
6. Test fix function if applicable

### Common Validation Tasks

| Task                    | Function                          | Location                                                      |
| ----------------------- | --------------------------------- | ------------------------------------------------------------- |
| Validate transaction    | `validate-tx-report`              | [validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs) |
| Validate full DB        | `validate-db!`                    | [validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs) |
| Fix invalid data        | `fix-invalid-blocks!`             | [validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs) |
| Add entity schema       | `defmethod entity-schema`         | [validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs) |
| Add property validation | `defmethod property-value-schema` | [validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs) |

---

## Related Documentation

- [Investigation Overview](../07-investigations/schema-evolution-and-data-migration.md)
- [Schema Migration Best Practices](./schema-migration-best-practices.md)
- [Import/Export Best Practices](./import-export-best-practices.md)
- [Testing Strategies](./schema-change-testing.md)
- [PR Review Checklist](./schema-change-pr-checklist.md)

---

## Changelog

| Date       | Changes                                         |
| ---------- | ----------------------------------------------- |
| 2025-12-28 | Initial version based on investigation findings |
