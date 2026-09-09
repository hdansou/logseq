# Schema Changes & Data Integrity Investigation

## Part 2: Schema System Technical Analysis

> **Last revised:** 2026-05-30

---

## Table of Contents

1. [Schema Version Management](#schema-version-management)
2. [DataScript Schema](#datascript-schema)
3. [Malli Validation Schema](#malli-validation-schema)
4. [Entity Type Classification](#entity-type-classification)
5. [Built-in Properties System](#built-in-properties-system)
6. [Schema Comparison Logic](#schema-comparison-logic)

---

## Schema Version Management

### Version Structure

**Location:** [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs)

```clojure
(def version (parse-schema-version "65.33"))
```

### Version Parsing

```clojure
(defn parse-schema-version
  "Return schema-version({:major <num> :minor <num>}).
  supported input: 10, \"10.1\", [10 1]"
  [string-or-compatible-number]
  ...)
```

Current parser accepts schema versions as map, vector, integer, or string.

### Version Comparison

```clojure
(defn compare-schema-version
  [x y]
  (apply compare
         (map (juxt :major :minor)
              [(parse-schema-version x) (parse-schema-version y)])))
```

### Major Version Extraction

```clojure
(defn major-version
  "Return a number."
  [schema-version]
  (if (schema-version? schema-version)
    (:major schema-version)
    (:major (parse-schema-version schema-version))))
```

---

## DataScript Schema

### Purpose

DataScript schema defines structural constraints:

- Entity attributes
- Ref vs non-ref typing
- Cardinality rules
- Identity/index behavior

### Schema Definition Location

**File:** [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs)

### Core Attribute Examples

```clojure
:db/ident        {:db/unique :db.unique/identity}
:block/uuid      {:db/unique :db.unique/identity}
:block/parent    {:db/valueType :db.type/ref :db/index true}
:block/page      {:db/valueType :db.type/ref :db/index true}
:block/refs      {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
:block/tags      {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
:block/alias     {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many :db/index true}
:block/link      {:db/valueType :db.type/ref :db/index true}
:block/name      {:db/index true}
:block/title     {:db/index true}
:block/journal-day {:db/index true}
```

---

## Malli Validation Schema

### Purpose

Malli schema defines runtime/business validation rules on top of DataScript structure.

### Schema Definition Location

**File:** [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)

### Top-Level DB Schema

```clojure
(def Data
  (into
   [:multi {:dispatch (fn [d] (entity-dispatch-key *db-for-validate-fns* d))}]
   {...}))

(def DB
  [:sequential Data])
```

### Property Types (Current)

```clojure
;; User property types
[:default :number :date :datetime :checkbox :url :node :asset]

;; Internal property types
#{:keyword :map :coll :any :entity :class :page :property :string :json :raw-number}
```

### Integrity Guardrails

`malli_schema.cljs` now asserts coverage against DataScript-derived sets:

- `db-schema/card-many-ref-type-attributes`
- `db-schema/card-one-ref-type-attributes`
- `db-schema/db-non-ref-attributes`

When Malli declarations drift from DataScript schema, startup throws with explicit errors.

---

## Entity Type Classification

### Dispatch System

**Location:** [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)

```clojure
(defn entity-dispatch-key [db ent]
  (let [d (if (:block/uuid ent) (d/entity db [:block/uuid (:block/uuid ent)]) ent)]
    (cond
      (:logseq.property.reaction/target d) :reaction-entity
      (entity-util/property? d) :property
      (entity-util/class? d) :class
      (and (entity-util/page? d) (true? (:logseq.property/hide? d))) :hidden
      (entity-util/page? d) :normal-page
      (entity-util/asset? d) :asset-block
      (:file/path d) :file-block
      (:logseq.property.history/block d) :property-history-block
      (:block/closed-value-property d) :closed-value-block
      (and (:logseq.property/created-from-property d) (:logseq.property/value d)) :property-value-block
      (= (:db/ident d) :logseq.property/empty-placeholder) :property-value-placeholder
      (:block/uuid d) :block
      (:db/ident d) :db-ident-key-value)))
```

### Current Dispatch Types

1. `:property`
2. `:class`
3. `:hidden`
4. `:normal-page`
5. `:reaction-entity`
6. `:property-history-block`
7. `:closed-value-block`
8. `:property-value-block`
9. `:block`
10. `:asset-block`
11. `:file-block`
12. `:db-ident-key-value`
13. `:property-value-placeholder`

---

## Built-in Properties System

### Property Registry

**Location:** [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs)

```clojure
(def ^:large-vars/data-var built-in-properties
  "Map of built in properties for db graphs with their :db/ident as keys.
   TODO: Move some of these keys to :properties since :schema is a deprecated concept"
  ...)
```

Registry remains centralized, with common keys such as `:title`, optional `:attribute`, `:schema`, and optional metadata (`:queryable?`, `:closed-values`, `:rtc`, etc.).

### Adding New Properties (Current Workflow)

1. Define new entry in `built-in-properties`.
2. If needed, wire/refine DataScript attribute usage and consistency assertions (for db-attribute properties).
3. Add schema migration entry in `src/main/frontend/worker/db/migrate.cljs` `schema-version->updates`.
4. Keep Malli coverage in sync (guarded by assertions).

---

## Schema Comparison Logic

### Compatibility Detection

**Locations:**

- [src/main/frontend/handler/repo.cljs](../../../src/main/frontend/handler/repo.cljs)
- [src/main/frontend/handler/events.cljs](../../../src/main/frontend/handler/events.cljs)

Current behavior is major-version gated for RTC graph compatibility:

- Repo merge/filter logic matches remote `GraphSchemaVersion` against app major schema version.
- RTC download handler asserts matching major versions before download.

### Decision Matrix (Current)

| App Major | Remote Major | Outcome                                               |
| --------- | ------------ | ----------------------------------------------------- |
| Same      | Same         | Eligible for merge/download flow                      |
| Different | Different    | Excluded from merge or assert-failed on download path |

---

## Schema Version History

### Current Migration Window (65.7 - 65.33)

`src/main/frontend/worker/db/migrate.cljs` currently defines migrations from `65.7` through `65.33`.

Recent highlights:

- `65.30`: adds `:logseq.property/assignee`
- `65.31`: adds `:logseq.property.agent/session-id`
- `65.32`: repair fix for comments classes/targets
- `65.33`: adds gallery-view properties

---

## Findings: Removed or Stale Claims

> **Removed:** `deps/db/src/logseq/db/schema.cljs` as active DataScript schema source for this DB-graph analysis. Current source is `deps/db/src/logseq/db/frontend/schema.cljs`.

> **Removed:** `def schema-version {:major 65 :minor 16}` as current version declaration. Current source uses `def version (parse-schema-version "65.33")`.

> **Removed:** The prior parser description that accepted only `"major.minor"` strings. Current parser supports map/vector/integer/string forms.

> **Removed:** `validate.cljs` `defmulti entity-dispatch` as the active classification mechanism. Current dispatch is `entity-dispatch-key` in `malli_schema.cljs`.

> **Removed:** `src/main/frontend/worker/rtc/branch_graph.cljs` plus standalone `compare-schemas`/`select-graph-version` helper flow as active compatibility path.

> **Removed:** The fixed eight-entity-type model as a complete representation of current validation dispatch categories.

---

## Findings: New Capabilities

1. Schema parsing is now compatibility-oriented across multiple input shapes (map, vector, integer, string).
2. Malli/DataScript schema alignment checks now fail fast when attribute coverage drifts.
3. Validation dispatch now includes newer runtime entity categories such as reaction entities, asset blocks, property history blocks, and property-value placeholders.
4. Frontend migration chain now reaches `65.33`, including newer collaboration/comment and gallery-view schema updates.

---

## Key Takeaways

1. DataScript + Malli remains the core two-layer schema model.
2. Current schema version is `65.33`.
3. `schema-version->updates` is the authoritative frontend migration chain.
4. Entity dispatch is broader and more runtime-aware than older docs indicate.
5. Compatibility enforcement is currently centered on major-version matching in repo/sync handlers.

---

## Next Steps

Proceed to **Part 3: Migration System Deep Dive** for detailed analysis of migration execution details and data-shape transitions.

# Schema Changes & Data Integrity Investigation

## Part 2: Schema System Technical Analysis

---

## Table of Contents

1. [Schema Version Management](#schema-version-management)
2. [DataScript Schema](#dataschript-schema)
3. [Malli Validation Schema](#malli-validation-schema)
4. [Entity Type Classification](#entity-type-classification)
5. [Built-in Properties System](#built-in-properties-system)
6. [Schema Comparison Logic](#schema-comparison-logic)

---

## Schema Version Management

### Version Structure

**Location:** [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs#L15-L48)

```clojure
(def schema-version
  "Current db schema version used by db graphs. Uses semantic versioning with
   major and minor versions to enable us to communicate with past
   and future db versions. Major version is incremented for breaking changes
   where a db is unreadable by a client with a lower major version. Minor
   version is incremented for non-breaking changes that indicate additional
   features and capabilities. A client with an older minor version is able to
   use a newer db version."
  {:major 65
   :minor 16})
```

### Version Parsing

```clojure
(defn parse-schema-version
  "Parse a schema version string into a map"
  [s]
  (when-let [[_ major minor] (re-matches #"(\d+)\.(\d+)" (or s ""))]
    {:major (parse-long major)
     :minor (parse-long minor)}))

;; Examples:
;; "65.16" => {:major 65, :minor 16}
;; "65.7"  => {:major 65, :minor 7}
;; "64.0"  => {:major 64, :minor 0}
```

### Version Comparison

```clojure
(defn compare-schema-version
  "Compares schema versions v1 and v2 using semantic versioning.
   Returns a neg int if v1 is less than v2, 0 if they are equal,
   and a pos int otherwise"
  [v1 v2]
  (let [major-cmp (compare (:major v1) (:major v2))]
    (if (zero? major-cmp)
      (compare (:minor v1) (:minor v2))
      major-cmp)))
```

**Critical Insight:** Major version differences always indicate incompatibility. Minor version differences indicate feature availability but maintain compatibility.

### Major Version Extraction

```clojure
(defn major-version
  "Returns major version as a string from schema version e.g. 1.5 => \"1\""
  [schema-version]
  (-> schema-version :major str))
```

**Use Case:** RTC branching uses major version to determine if clients can share a graph.

---

## DataScript Schema

### Purpose

DataScript schema defines **structural constraints**:

- Entity attribute definitions
- Value types (`:db.type/ref`, `:db.type/long`, etc.)
- Cardinality (`:db.cardinality/one` or `:db.cardinality/many`)
- Uniqueness constraints (`:db.unique/identity`, `:db.unique/value`)
- Component relationships (`:db/isComponent`)

### Schema Definition Location

**File:** [deps/db/src/logseq/db/schema.cljs](../../../deps/db/src/logseq/db/schema.cljs)

### Core Entity Attributes

```clojure
;; Block entity
:block/uuid              {:db/unique :db.unique/identity}
:block/original-name     {:db/unique :db.unique/identity}
:block/name              {:db/unique :db.unique/identity}
:block/title             {}
:block/parent            {:db/valueType :db.type/ref}
:block/left              {:db/valueType :db.type/ref}
:block/collapsed?        {:db/cardinality :db.cardinality/one}
:block/created-at        {:db/cardinality :db.cardinality/one}
:block/updated-at        {:db/cardinality :db.cardinality/one}

;; Class (tag) entity
:class/schema-properties {:db/cardinality :db.cardinality/many
                          :db/valueType :db.type/ref}
:class/extends           {:db/cardinality :db.cardinality/many
                          :db/valueType :db.type/ref}

;; Property entity
:db.property/schema      {:db/cardinality :db.cardinality/one}
:db.property/closed-values {:db/cardinality :db.cardinality/many
                            :db/valueType :db.type/ref}
```

### Cardinality Rules

- `:db.cardinality/one` - Attribute can have exactly one value
- `:db.cardinality/many` - Attribute can have multiple values (set)

**Breaking Change Example:**

```clojure
;; Before: Single tag
:block/tags {:db/cardinality :db.cardinality/one}

;; After: Multiple tags (BREAKING - requires data migration)
:block/tags {:db/cardinality :db.cardinality/many}
```

### Reference Types

```clojure
:db.valueType/ref  ;; Reference to another entity
```

**Example:** `:block/parent` references another block entity.

---

## Malli Validation Schema

### Purpose

Malli schema defines **business logic constraints**:

- Property value validation
- Required fields
- Allowed value ranges
- Custom validation functions
- Detailed error messages

### Schema Definition Location

**File:** [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)

### Entity Schema Structure

```clojure
(def Entity
  "An entity is one of the 8 fundamental entity types"
  [:or
   [:map [:db/ident :db.ident/block.type.page]]
   [:map [:db/ident :db.ident/block.type.property]]
   [:map [:db/ident :db.ident/block.type.class]]
   [:map [:db/ident :db.ident/block.type.closed-value]]
   [:map [:db/ident :db.ident/block.type.default]]
   [:map [:db/ident :db.ident/block.type.whiteboard]]
   [:map [:db/ident :db.ident/db.schema/property]]
   [:map [:db/ident :db.ident/db.schema/class]]])
```

### Property Type Schemas

```clojure
:default     ;; Free-form value
:number      [:or :double :long]
:url         [:and :string [:re #"^https?://"]]
:date        :inst
:checkbox    :boolean
:page        [:map [:block/uuid :uuid]]
```

### Validation Registry

```clojure
(def schema-registry
  "A map of all malli schemas for db schema validation"
  (merge
    (m/default-schemas)
    (m/type-schemas)
    {:uuid uuid-schema
     :keyword keyword-schema
     :db.ident db-ident-schema
     :property-value property-value-schema
     :entity Entity}))
```

### Schema Location Storage

```clojure
;; Property entity stores its schema
:db.property/schema
{:type :number
 :cardinality :one
 :min 0
 :max 100}

;; Class entity stores allowed property values
:class/schema-properties [property-ref-1 property-ref-2]
```

---

## Entity Type Classification

### Entity Dispatch System

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs#L15-L23)

```clojure
(defmulti entity-dispatch
  "Dispatches on entity type for validation"
  (fn [_db entity]
    (cond
      (:block/uuid entity)           :block
      (:db.property/schema entity)   :property
      (:class/schema-properties entity) :class
      :else                          :unknown)))
```

### Eight Fundamental Entity Types

1. **Page** - Top-level organizational unit
   - `:db/ident :db.ident/block.type.page`
   - Has `:block/uuid` and `:block/name`
2. **Property** - Metadata definition
   - `:db/ident :db.ident/block.type.property`
   - Has `:db.property/schema`
3. **Class** - Tag/category definition
   - `:db/ident :db.ident/block.type.class`
   - Has `:class/schema-properties`
4. **Closed Value** - Enumeration value
   - `:db/ident :db.ident/block.type.closed-value`
   - Used for dropdown/select options
5. **Default Block** - Regular content block
   - `:db/ident :db.ident/block.type.default`
   - Most common type
6. **Whiteboard** - Canvas entity
   - `:db/ident :db.ident/block.type.whiteboard`
   - Special rendering
7. **DB Schema Property** - Internal property definition
   - `:db/ident :db.ident/db.schema/property`
8. **DB Schema Class** - Internal class definition
   - `:db/ident :db.ident/db.schema/class`

### Type Detection Logic

```clojure
(defn entity-type [db entity]
  (cond
    ;; Check for specific type indicators
    (contains? entity :db.property/schema) :property
    (contains? entity :class/schema-properties) :class

    ;; Check for db/ident marker
    (and (:db/ident entity)
         (str/starts-with? (name (:db/ident entity)) "block.type."))
    (keyword (last (str/split (name (:db/ident entity)) #"\.")))

    ;; Default
    :else :block))
```

---

## Built-in Properties System

### Property Registry

**Location:** [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs)

```clojure
(def built-in-properties
  "Registry of all built-in properties"
  {:block/created-at
   {:schema {:type :number}
    :description "Timestamp when block was created"}

   :block/updated-at
   {:schema {:type :number}
    :description "Timestamp when block was last modified"}

   :block/tags
   {:schema {:type :page
             :cardinality :many}
    :description "Tags applied to this block"}

   :block/collapsed?
   {:schema {:type :checkbox}
    :description "Whether block is collapsed"}})
```

### Adding New Properties

**Process:**

1. Define property in `built-in-properties` map
2. Add DataScript schema attribute
3. Add Malli validation schema
4. Create migration if needed (see Part 3)
5. Update RTC ops if needed (see Part 5)

**Example Migration Trigger:**

```clojure
;; Schema version 65.15 added :block/priority property
{:type :properties
 :properties {:block/priority
              {:schema {:type :number
                        :cardinality :one}}}}
```

---

## Schema Comparison Logic

### Compatibility Detection

**Location:** [src/main/frontend/worker/rtc/branch_graph.cljs](../../../src/main/frontend/worker/rtc/branch_graph.cljs#L13-L40)

```clojure
(defn compare-schemas
  "Compare client schema version with server graph schema.
   Returns :create-branch if major version differs,
   :download if server is newer minor version,
   :compatible if client can use server graph."
  [client-schema server-schema]
  (let [cmp (db-schema/compare-schema-version client-schema server-schema)]
    (cond
      ;; Client older major version - must download
      (and (< (:major client-schema) (:major server-schema))
           (not= (:major client-schema) (:major server-schema)))
      :download

      ;; Client newer major version - create new branch
      (> (:major client-schema) (:major server-schema))
      :create-branch

      ;; Same major version - compatible
      :else
      :compatible)))
```

### Decision Matrix

| Client Version | Server Version | Action           | Reason                     |
| -------------- | -------------- | ---------------- | -------------------------- |
| 65.16          | 65.16          | `:compatible`    | Exact match                |
| 65.15          | 65.16          | `:compatible`    | Same major, older minor OK |
| 65.16          | 65.15          | `:compatible`    | Same major, newer minor OK |
| 66.0           | 65.16          | `:create-branch` | Newer major - incompatible |
| 65.16          | 66.0           | `:download`      | Older major - must upgrade |
| 64.5           | 65.16          | `:download`      | Old major - must upgrade   |

### Graph Selection Logic

```clojure
(defn select-graph-version
  "Determine which graph version to use based on schema compatibility"
  [client-version available-versions]
  (let [same-major (filter #(= (:major %) (:major client-version))
                           available-versions)]
    (if (seq same-major)
      ;; Use highest minor version in same major
      (apply max-key :minor same-major)
      ;; No compatible version found
      nil)))
```

---

## Schema Evolution Rules

### Rule 1: Major Version for Breaking Changes

**Breaking changes include:**

- Changing cardinality (`:one` ↔ `:many`)
- Removing required attributes
- Changing attribute types
- Removing entity types
- Incompatible data transformations

**Action:** Increment major version, provide migration

### Rule 2: Minor Version for Additions

**Non-breaking additions include:**

- New optional attributes
- New entity types
- Additional validation rules (that don't reject existing data)
- New built-in properties with defaults

**Action:** Increment minor version, optional migration for data population

### Rule 3: Maintain Migration Path

**Requirements:**

- Every version increment needs migration code
- Migrations must be idempotent
- Migrations must be testable
- Migration failures must be recoverable

### Rule 4: Document Schema Changes

**Required documentation:**

- What changed and why
- Migration strategy
- Impact on imports/exports
- RTC compatibility notes
- Rollback plan (if any)

---

## Schema Version History

### Recent Versions (65.7 - 65.16)

| Version | Changes                | Migration Type      |
| ------- | ---------------------- | ------------------- |
| 65.7    | Baseline               | -                   |
| 65.8    | Added property X       | `:properties`       |
| 65.9    | Renamed Y to Z         | `:rename-db-idents` |
| 65.10   | Fixed data consistency | `:fix`              |
| 65.11   | Added class support    | `:classes`          |
| 65.12   | New property type      | `:properties`       |
| 65.13   | Enhanced validation    | None                |
| 65.14   | Data cleanup           | `:fix`              |
| 65.15   | New properties         | `:properties`       |
| 65.16   | Current version        | Multiple            |

**Note:** See Part 3 (Migration System Deep Dive) for detailed migration code analysis.

---

## Key Takeaways

1. **Two-Schema Architecture:** DataScript handles structure, Malli handles validation
2. **Semantic Versioning:** Major = breaking, Minor = additions
3. **No Downgrades:** Older clients cannot read newer major versions
4. **Type Classification:** Entity dispatch system enables polymorphic validation
5. **Built-in Properties:** Centralized registry simplifies property management
6. **Strict Compatibility:** Major version mismatches force branching or download

---

## Next Steps

Proceed to **Part 3: Migration System Deep Dive** for detailed analysis of how schema changes are applied to existing graphs.
