---
title: 'Database Integrity & Validation'
status: 🟢 Stable
version: 'Schema v65.33'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Schema System](./schema-system.md)'
  - '[Block Editor Architecture](./block-editor.md)'
  - '[Real-Time Collaboration](../../tutorials/collaboration/real-time-sync.md)'
tags: ['integrity', 'validation', 'database', 'transactions', 'malli']
---

# Database Integrity & Validation

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

This document explains Logseq's database integrity system—the validation mechanisms ensuring data consistency, preventing corruption, and maintaining schema compliance. Understanding this system is essential for contributors and developers working with database graphs.

> **Scope:** this page describes **DB graphs** only. None of it applies to file graphs, which have
> no malli schema, no `:db/ident` namespacing and no schema-version migration chain.

> **Audited 2026-09-09 against `be800f1711`.** The schema version is `65.33`
> (`deps/db/src/logseq/db/frontend/schema.cljs:33`). Several code listings on this page were
> previously illustrative pseudo-code presented as real source; those have been replaced with the
> actual functions or removed. Where an earlier claim could not be found in source at all it is
> marked `> **Removed:**`.

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
8. [Findings: New Capabilities](#findings-new-capabilities)

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
│            Outliner Op Layer                            │
│  • malli `op-schema`: 30 ops, args shape-checked        │
│  • `apply-ops!` asserts before any tx is built          │
│  Location: logseq.outliner.op                           │
│            (deps/outliner/.../op.cljs:20-155, 427-429)  │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Transact Pipeline Layer                      │
│  • speculative `d/with`, then pipeline rewrites         │
│  • `throw-if-page-has-block-parent!`                    │
│  • outliner domain guards (built-in nodes, cycles)      │
│  Location: logseq.db/transact-sync                      │
│            (deps/db/src/logseq/db.cljs:153-186)         │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Malli Validation Layer                       │
│  • Entity type dispatch                                 │
│  • Property type checking                               │
│  • Cardinality enforcement                              │
│  • Closed-value membership                              │
│  Location: logseq.db.frontend.malli_schema              │
│            driven by validate/validate-tx-report        │
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
│  • datascript.storage `kvs` table, not block tables     │
│  • (addr INTEGER pk, content TEXT, addresses JSON)      │
│  Location: logseq.db.common.sqlite:11-13                │
└─────────────────────────────────────────────────────────┘

> **Removed:** "Transaction size limits", "Rate limiting (planned)", "Foreign key constraints" and
> "Rollback on failure" in the SQLite layer. There is no size cap or rate limiter in the transact
> path (`deps/db/src/logseq/db.cljs:153-186`), and the SQLite layer stores opaque datascript
> storage pages in a single `kvs` table — there are no block-level foreign keys to enforce.
> Atomicity is provided one level up instead: `transact-sync` validates the *speculative*
> `db-after` and only then does `compare-and-set!` on the conn, so an invalid transaction is
> discarded before it is ever visible (`db.cljs:161-180`).
```

### Validation Modes

**Three validation modes** for different contexts:

**1. Transaction Validation** (Default)

- `validate-tx-report` collects the ids touched by `:tx-data`, re-reads their full `:eavt` datoms
  from `db-after`, and validates only those entities. Fastest, because it only checks changed data.
- Runs on every DB-graph write unless a tx-meta escape hatch is set.
- Source: `deps/db/src/logseq/db/frontend/validate.cljs:24-70`; call site
  `deps/db/src/logseq/db.cljs:166`.

**2. Full-Graph Validation**

- `frontend.worker.db.validate/validate-db` walks all `:eavt` datoms via
  `db-validate/validate-db`, and by default **also repairs** before and during validation.
- Invoked manually through `:thread-api/validate-db` — from the `:dev/validate-db` developer
  command (`src/main/frontend/handler/common/developer.cljs:84-85`) and from EDN export.
- Source: `src/main/frontend/worker/db/validate.cljs:270-327`.

**3. Pre-Export Validation**

- `export-repo-as-db-edn!` calls `:thread-api/validate-db` first and throws
  `"Graph validation failed"` if any errors come back — the EDN is never written.
- Source: `src/main/frontend/handler/db_based/export.cljs:80-88`.

> **Removed:** "Full-graph validation is used after migrations or imports." Migrations transact with
> `:skip-validate-db? true` and run no validation pass at all
> (`src/main/frontend/worker/db/migrate.cljs:288-290`). Imports use a *different* checker,
> `sqlite-export/validate-import-txs` (`deps/outliner/src/logseq/outliner/op.cljs:221-227`), or
> `validate-local-db!` on the CLI path (`deps/db/src/logseq/db/frontend/validate.cljs:126`).

---

## Transaction Validation

### Validation Flow

**Location:** `deps/db/src/logseq/db.cljs:153-186` (the driver) and
`deps/db/src/logseq/db/frontend/validate.cljs:24-70` (the validator).

Validation is **post-speculative, pre-commit** — not "validate the map, then transact it":

```
1. d/with db tx-data tx-meta          -> speculative tx-report
   ↓
2. transact-pipeline rewrites report  (refs rebuild, extra tx-data, guards)
   ↓
3. throw-if-page-has-block-parent!    (structural guard)
   ↓
4. validate-tx-report
     a. changed-ids = distinct :e of tx-data
     b. re-read those entities' :eavt datoms from db-after
     c. update-properties-in-ents  (inject :block/properties for validation)
     d. run the malli `DB` validator over each entity
   ↓
5a. invalid -> throw-invalid-tx! ; conn untouched, ui notified
5b. valid   -> compare-and-set! conn db (:db-after tx-report), then store + callbacks
```

Two consequences worth internalising:

- The conn is only advanced by `compare-and-set!` **after** validation passes, and a lost CAS race
  simply `recur`s the whole thing. There is no partially-applied invalid state to roll back.
- Validation is over `db-after`, so it sees the *result* of the pipeline, including datoms the
  pipeline itself added.

> **Removed:** the previous "Pre-Transaction Validation / Example: Insert Block" listing, cited to
> `src/main/frontend/handler/db_based/editor.cljs:250-280`. No such function exists. That file is
> 178 lines long and contains `wrap-parse-block`, `save-file!` and `batch-set-heading!` only; there
> is no `validate/validate-entity` anywhere in the tree. Block insertion goes through the outliner
> op pipeline in the worker (`deps/outliner/src/logseq/outliner/core.cljs:1363-1370`), not a
> validate-then-`d/transact!` call in a frontend handler.

### Transaction Validation Rules

**Required Fields**, verbatim from the malli schema
(`deps/db/src/logseq/db/frontend/malli_schema.cljs:281-292, 406-414`):

```clojure
;; block-attrs — required on every normal block
[:block/title :string]
[:block/parent :int]                ; a ref, represented as :db/id in the schema
[:block/order block-order]          ; validated by db-order/validate-order-key?
[:block/page :int]
[:block/link {:optional true} :int]
[:logseq.property/created-from-property {:optional true} :int]

;; page-or-block-attrs — required on both pages and blocks
[:block/uuid :uuid]
[:block/created-at :int]
[:block/updated-at :int]
[:logseq.property/deleted-at {:optional true} :int]
[:block/properties {:optional true} block-properties]   ; injected, not stored
[:block/tags {:optional true} block-tags]
[:block/refs {:optional true} [:set :int]]
```

Note `:block/order` is not merely `:string` — it is
`[:and :string [:fn ... db-order/validate-order-key?]]`, i.e. a *base-62 fractional index key*
(`malli_schema.cljs:26-30`, `deps/db/src/logseq/db/common/order.cljs:39-42`). And `:block/properties`
is **not a stored attribute**: `update-properties-in-ents` synthesises it onto entity maps purely so
that the schema can reach property values (`malli_schema.cljs:144`, `validate.cljs:37-38`).

**Type Checking.** Property values are checked by `validate-property-value`, whose real signature is:

```clojure
(validate-property-value db validate-fn [property property-val] & validate-options)
;; malli_schema.cljs:88-118
```

It takes the *property entity* (not its ident) plus the value as a tuple, and picks a `validate-fn`
per property type. When `*closed-values-validate?*` is bound and the property has closed values, the
value must additionally be the `:db/id` of one of `(:property/closed-values property)`.

> **Removed:** every example on this page that used `:logseq.property.task/due-date`. That ident does
> not exist. The task date properties are `:logseq.property/deadline` and
> `:logseq.property/scheduled` (`deps/db/src/logseq/db/frontend/class.cljs:39-41`,
> `deps/db/src/logseq/db/frontend/property.cljs:340`).

> **Removed:** `{:logseq.property/status "pending"}` as an example of a valid `:default` string.
> `:logseq.property/status` is a **closed-value** property: its type is `:default`, but its value
> must be a ref to one of six closed-value entities — `status.backlog`, `status.todo`,
> `status.doing`, `status.in-review`, `status.done`, `status.canceled`
> (`property.cljs:300-322`). A bare string is not a legal status value.

**Reference Validation.** Refs are `:db/valueType :db.type/ref` in the datascript schema
(`deps/db/src/logseq/db/frontend/schema.cljs:69-83`), so datascript itself rejects a ref to a
non-existent entity id at transact time. The malli layer types them as `:int` / `[:set :int]`; it
checks shape, not existence.

One structural constraint *is* enforced by hand, before validation: a page may not have a block as
its parent (`throw-if-page-has-block-parent!`, `deps/db/src/logseq/db.cljs:91-101, 163`).

---

## Schema Compliance

### Entity Type Validation

**Location:** `deps/db/src/logseq/db/frontend/malli_schema.cljs:557-613`

```clojure
(def Data
  (into
   [:multi {:dispatch (fn [d] (entity-dispatch-key *db-for-validate-fns* d))}]
   {:property property-page
    :class class-page
    :hidden hidden-page
    :normal-page normal-page
    :reaction-entity reaction-entity
    :property-history-block property-history-block
    :closed-value-block closed-value-block
    :property-value-block property-value-block
    :block block
    :asset-block asset-block
    :file-block file-block
    :db-ident-key-value db-ident-key-val
    :property-value-placeholder property-value-placeholder}))

(def DB [:sequential Data])
```

`entity-dispatch-key` is a single `cond` whose **order matters**, because several entity kinds are
subsets of others. It resolves the entity from `db` by `:block/uuid` first, then tests in this
order (`malli_schema.cljs:557-591`):

| # | Test                                                          | Dispatch key                  |
|---|---------------------------------------------------------------|-------------------------------|
| 1 | `:logseq.property.reaction/target`                            | `:reaction-entity`            |
| 2 | `entity-util/property?`                                       | `:property`                   |
| 3 | `entity-util/class?`                                          | `:class`                      |
| 4 | page **and** `:logseq.property/hide?` true                     | `:hidden`                     |
| 5 | `whiteboard?` (deprecated)                                    | `:normal-page`                |
| 6 | `entity-util/page?`                                           | `:normal-page`                |
| 7 | `entity-util/asset?`                                          | `:asset-block`                |
| 8 | `:file/path`                                                  | `:file-block`                 |
| 9 | `:logseq.property.history/block`                              | `:property-history-block`     |
|10 | `:block/closed-value-property`                                | `:closed-value-block`         |
|11 | `:logseq.property/created-from-property` **and** `:logseq.property/value` | `:property-value-block` |
|12 | `:db/ident` = `:logseq.property/empty-placeholder`             | `:property-value-placeholder` |
|13 | `:block/uuid`                                                 | `:block`                      |
|14 | `:db/ident`                                                   | `:db-ident-key-value`         |

There are also two validator flavours: an *open* schema and a `mu/closed-schema` variant that
rejects undeclared keys. Tx-time validation uses the open one; full-graph `validate-db` uses the
closed one (`deps/db/src/logseq/db/frontend/validate.cljs:11-22, 90-108`). A drift guard at the
bottom of `malli_schema.cljs:617-647` throws at load time if the datascript schema gains a
cardinality-many ref, cardinality-one ref, or non-ref attribute that the malli schema does not
declare — which is why the two never silently diverge.

> **Removed:** the old dispatch table's `:property-value-block` schema, given as
> `[:map [:logseq.property/schema-type :keyword] ...]`. There is no `:logseq.property/schema-type`
> attribute. The real schema is
> `[[:logseq.property/value [:or :string :double :boolean]] [:logseq.property/created-from-property :int] ...]`
> (`malli_schema.cljs:428-436`).

> **Removed:** `:logseq.property.class/extends [:vector :ref]`. `extends` is a cardinality-many ref
> and is typed `[:set :int]` in malli terms; it is also one of `required-properties`
> (`malli_schema.cljs:121-133`).

### Property Type Enforcement

**Built-in Property Types**
(`deps/db/src/logseq/db/frontend/property/type.cljs:15-26`):

```clojure
(def internal-built-in-property-types
  "Valid property types only for use by internal built-in-properties"
  #{:keyword :map :coll :any :entity :class :page :property :string :json :raw-number})

(def user-built-in-property-types
  "Valid property types for users in order they appear in the UI"
  [:default :number :date :datetime :checkbox :url :node :asset])

(def user-allowed-internal-property-types
  #{:map :json :string})
```

Related sub-sets in the same namespace, each of which gates real behaviour:

| Var                              | Value                                              | Gates |
| -------------------------------- | -------------------------------------------------- | ----- |
| `closed-value-property-types`    | `#{:default :number :url}`                          | which properties may have closed values (`type.cljs:30-32`) |
| `cardinality-property-types`     | `#{:default :number :url :date :node :asset}`        | which may switch cardinality (`type.cljs:34-36`) |
| `default-value-ref-property-types` | `#{:default :number :checkbox}`                    | default-value support (`type.cljs:38-40`) |
| `user-ref-property-types`        | `#{:default :url :number :date :node :asset}`        | values stored as refs (`type.cljs:63-66`) |

> **Removed:** the old `property-types` vector. It named four types that do not exist anywhere in
> the tree — `:icon`, `:query`, `:external-link`, `:other` — and omitted `:datetime` and `:asset`,
> which do. There is also no single var called `property-types`; the list is split into internal vs.
> user sets as shown above.

**How cardinality is actually handled** (`malli_schema.cljs:114-118`):

```clojure
(if (db-property/many? property)
  (or (every? validate-fn'' property-val)                       ; coll expected
      (empty-placeholder-value? db property (first property-val)))
  (or (validate-fn'' property-val)                              ; scalar expected
      (empty-placeholder-value? db property property-val)))
```

Two details the old examples missed:

- A `:many` property's value is validated element-wise; a `:one` property's value is validated
  directly. The branch is chosen from the *property entity's* cardinality, not from the shape of
  the value passed in.
- Either form is **also** valid if the value is the empty placeholder
  (`:logseq.property/empty-placeholder`) — that is how "property present but unset" is represented,
  and it is why a naive "must be a ref" check would be wrong.

> **Removed:** the four `(validate-property-value db :some-ident value)` examples. The function
> takes `[db validate-fn [property property-val] & opts]` — a property *entity* inside a tuple, plus
> an explicit `validate-fn` — so none of those calls would even arity-match
> (`malli_schema.cljs:88-93`).

---

## Integrity Constraints

### Reference Integrity

**Orphaned Reference Prevention:**

**Location:** `deps/outliner/src/logseq/outliner/core.cljs:92-115` (re-pinned; it was cited as
`69-95`). The real implementation:

```clojure
(defn- remove-orphaned-page-refs!
  [db {db-id :db/id} txs-state old-refs new-refs]
  (when (not= old-refs new-refs)
    (let [new-refs   (set (map ... new-refs))          ; compared by :block/name
          old-pages  (->> (keep :db/id old-refs)
                          (d/pull-many db '[*])
                          (remove #(contains? new-refs (:block/name %)))
                          (map :block/name)
                          (remove nil?))
          orphaned-pages
          (when (seq old-pages)
            (ldb/get-orphaned-pages
             db {:pages old-pages
                 :built-in-pages-names sqlite-create-graph/built-in-pages-names
                 :empty-ref-f
                 (fn [page]
                   (let [refs (:block/_refs page)]
                     (and (or (zero? (count refs))
                              (= #{db-id} (set (map :db/id refs))))
                          (not (ldb/class? page))
                          (not (ldb/property? page)))))}))]
      (when (seq orphaned-pages)
        (swap! txs-state into
               (mapv (fn [page] [:db/retractEntity (:db/id page)]) orphaned-pages))))))
```

Three guards the old listing omitted, and each of them matters:

- Pages are matched by **`:block/name`**, not by entity id.
- Built-in pages are exempt (`built-in-pages-names`).
- A page is only orphaned if it has no refs *or* its only remaining referrer is the block being
  saved, **and** it is neither a class nor a property. Classes and properties are never garbage
  collected this way.

The caller is `remove-orphaned-refs-when-save` (`core.cljs:127-136`), which compares only
*content-derived* refs (`outliner-pipeline/block-content-refs`) — so removing a tag or a property
value does not trip page GC.

**Example:**

```
Before:                   After (remove ref):
- Block A                 - Block A
  refs: [[Page X]]          refs: []
- Page X                  - Page X (orphaned → deleted)
  refs-to: [Block A]        refs-to: []
```

### Parent-Child Integrity

**Circular Reference Prevention**

**Location:** `deps/outliner/src/logseq/outliner/core.cljs:1163-1174` — inside `move-blocks`, not
in `validate.cljs`.

```clojure
(let [parents' (->> (ldb/get-block-parents db (:block/uuid target-block) {})
                    (map :db/id)
                    (set))
      move-parents-to-child? (some parents' (map :db/id blocks))]
  (when-not move-parents-to-child?
    (transact-move-blocks! conn blocks target-block sibling? opts outliner-op top-level-blocks)
    nil))
```

Note the failure mode: a cycle-forming move is a **silent no-op**, not an exception. The same guard
clause also refuses a move whose target is one of the blocks being moved
(`(not (contains? (set (map :db/id blocks)) (:db/id target-block)))`, `core.cljs:1220`).

> **Removed:** `validate-no-circular-refs` and `ancestor?` in
> `deps/outliner/src/logseq/outliner/validate.cljs`. Neither symbol exists. That namespace is about
> **page titles, tags and class extends** — its public fns are `validate-page-title*`,
> `validate-unique-by-name-and-tags`, `validate-block-title`, `validate-property-title`,
> `validate-editing-built-in-property`, `validate-extends-property`, `validate-tags-property`,
> `built-in-entity?` and `disallow-editing-private-built-in-nodes`
> (`validate.cljs:15-378`).

**Class extends cycles**, by contrast, *are* validated by name and *do* throw:
`disallow-extends-cycle` walks `logseq.db.frontend.class/get-class-extends` and rejects a class
extending its own descendant (`deps/outliner/src/logseq/outliner/validate.cljs:200-221`).

**Built-in node protection** is the other hard guard: `save-block`, `move-blocks` and
`delete-blocks` each call `outliner-validate/built-in-entity?` and throw a `:notification`-typed
`ex-info` — "Built-in nodes can't be modified" / "can't be deleted"
(`core.cljs:548-558, 1028-1033, 1138-1144`).

**Example (Prevented):**

```
❌ Cannot move:
- Block A
  - Block B
    - Block C  (trying to move A into C → circular!)
```

### Cardinality Enforcement

**DataScript Cardinality** (`deps/db/src/logseq/db/frontend/schema.cljs:56-106`):

```clojure
{:block/parent {:db/valueType :db.type/ref :db/index true}       ; cardinality/one (the default)
 :block/tags   {:db/valueType :db.type/ref
                :db/cardinality :db.cardinality/many}}
```

The cardinality-many ref attributes in the DB-graph schema are exactly `:block/refs`,
`:block/tags`, `:block/alias` and `:block/closed-value-property`; everything else that is a ref
(`:block/parent`, `:block/page`, `:block/link`) is cardinality-one. The schema derives these sets
programmatically as `card-many-ref-type-attributes` / `card-one-ref-type-attributes`
(`schema.cljs:112-133`), and the malli drift guard asserts both sets are declared
(`malli_schema.cljs:617-647`).

For *properties* (as opposed to schema attributes), cardinality is per-property and stored on the
property entity; only `#{:default :number :url :date :node :asset}` may change cardinality
(`deps/db/src/logseq/db/frontend/property/type.cljs:34-36`).

---

## Migration Safety

**Location:** `src/main/frontend/worker/db/migrate.cljs`

### How Migration Actually Works

```
1. Read :logseq.kv/schema-version from the graph      (migrate.cljs:299)
2. compare-schema-version target vs in-db             (migrate.cljs:300)
     = 0  -> nil, nothing to do
     < 0  -> graph is NEWER than the app: notify, do not touch the graph
     > 0  -> proceed
3. Select every entry of schema-version->updates strictly greater than the
   in-db version and not greater than the target                (migrate.cljs:309-315)
4. For each, in order: upgrade-version!                          (migrate.cljs:317-319)
     - build any new built-in properties and classes
     - run the entry's :fix fn, apply :delete-properties
     - prepend (sqlite-util/kv :logseq.kv/schema-version version)
     - ldb/transact! with {:db-migrate? true :skip-validate-db? true}
5. ensure-built-in-data-exists!                                  (migrate.cljs:321-322)
```

The schema version lives in a key/value entity, `:logseq.kv/schema-version`
(`deps/db/src/logseq/db/frontend/kv_entity.cljs:19`), and is seeded at graph creation from
`db-schema/version` (`deps/db/src/logseq/db/sqlite/create_graph.cljs:239`). A load-time assertion in
`migrate.cljs:167-174` fails the build if `db-schema/version` is *behind* the highest entry in
`schema-version->updates`.

### Integrity Properties You Can Actually Rely On

- **Version bump is atomic with its migration.** `upgrade-version!` puts the new
  `:logseq.kv/schema-version` datom in the *same* transaction as the migration's data changes
  (`migrate.cljs:285-289`), so a graph cannot be left claiming a version whose changes were not
  applied.
- **Migrations run per-step, not as one big transaction.** Each entry in `schema-version->updates`
  is its own `ldb/transact!`. A failure at step *n* leaves steps `1..n-1` committed and the version
  at *n-1*; `migrate` logs and rethrows (`migrate.cljs:325-328`). The next launch resumes from
  wherever the recorded version left off, which is what makes partial failure survivable.
- **Migrations bypass validation deliberately** — `:skip-validate-db? true` — because a migration's
  intermediate shape is frequently invalid under the *new* schema until the step completes
  (`migrate.cljs:289-290`).
- **A newer-than-app graph is refused, not downgraded.** The user gets:
  "Your app is using an outdated version that is incompatible with your current graph. Please update
  your app before editing this graph." (`migrate.cljs:305-306`).

> **Removed:** `validate-pre-migration`, `validate-post-migration`, `apply-migrations!`,
> `begin-transaction!`, `commit-transaction!` and `rollback-transaction!`, all previously shown as
> real code (one cited to `migrate.cljs:150-180`). **None of these functions exist.** There is no
> pre-migration validation, no post-migration full-graph validation, and no rollback mechanism in
> the migration path — `migrate.cljs` is 330 lines and its only public fns are `delete-property`,
> `remove-block-path-refs`, `ensure-built-in-data-exists!` and `migrate`. This was the most
> consequential error on the page: it described a transactional rollback guarantee that the
> migration system does not provide.

---

## Failure Recovery

### 1. Export Blocked by an Invalid Graph

**Problem:** a graph with validation errors cannot be exported to EDN.

**Real mechanism** (`src/main/frontend/handler/db_based/export.cljs:80-88`):

```clojure
(defn export-repo-as-db-edn!
  "Run db validation before exporting EDN"
  [repo]
  (p/let [validate-result (state/<invoke-db-worker :thread-api/validate-db
                                                   (state/get-current-repo))
          {:keys [errors]} validate-result
          _ (when (seq errors)
              (throw (ex-info "Graph validation failed" validate-result)))
          result (state/<invoke-db-worker :thread-api/export-edn ...)]
    ...))
```

**Recovery:** run `:dev/validate-db` (`src/main/frontend/handler/common/developer.cljs:84-85`).
It calls the worker's `validate-db` with `:fix true` by default, which is a genuine repair pass, not
just a report — see below. It broadcasts either
`"Validation detected N invalid block(s)..."` or `"Your graph is valid! {counts}"` back to the UI
(`src/main/frontend/worker/db/validate.cljs:311-325`). The returned map carries `:errors` and
`:invalid-entity-ids`, so the offending entities can be inspected directly.

> **Removed:** `can-export?`, `find-invalid-entities`, `force-export!` and an
> `{:skip-validation? true}` export option. None exist; there is no supported way to force an
> export past validation.

### What the Repair Pass Fixes

`frontend.worker.db.validate/validate-db` (`src/main/frontend/worker/db/validate.cljs:294-327`),
with `:fix true` (the default), runs in this order:

1. `fix-extends-cardinality!` — restores `:logseq.property.class/extends` to
   `:db.cardinality/many` if it drifted (`validate.cljs:261-267`)
2. `fix-icon-wrong-type!` (`validate.cljs:251-259`)
3. `db-migrate/ensure-built-in-data-exists!`
4. `fix-non-closed-values!` (`validate.cljs:222-249`)
5. `fix-num-prefix-db-idents!` (`validate.cljs:195-209`)
6. `validate-and-fix-invalid-blocks!` — a **loop**: validate, run `fix-invalid-blocks!`, and if it
   changed anything, validate again (`validate.cljs:286-292`)

Its repairs include re-adding a missing `:block/uuid`, backfilling
`:block/updated-at` on normal pages, adding a missing `:db/ident`, and re-attaching
`:logseq.property.class/extends :logseq.class/Root` to classes that lost it
(`validate.cljs:19-193`).

### 2. Version Incompatibility

**Problem:** a graph carrying a schema version newer than the running app.

**Real behaviour:** `migrate` takes the `(neg? compare-result)` branch and posts a notification;
it makes no changes to the graph (`src/main/frontend/worker/db/migrate.cljs:303-306`).

**Recovery:** update the app. There is no downgrade path.

> **Removed:** `migration-blocked?`, `retry-migration!`, `fix-common-migration-issues!`,
> `force-version-bump!`, `version-locked?` and `downgrade-schema!`. None exist. The two
> "emergency" listings also wrote the version to `:logseq.property/schema-version`, which is not the
> attribute used — the version lives at `:logseq.kv/schema-version`
> (`deps/db/src/logseq/db/frontend/kv_entity.cljs:19`).

> **Removed:** the "Migration Cascade Failure — failed migration blocks all future schema updates"
> scenario. Because each `schema-version->updates` entry commits its own transaction together with
> its version bump, a failure blocks only the steps at and after the failure point; already-applied
> steps stay applied and are not retried.



---

## Developer Guide

### Where to Add a Validation Rule

There are four distinct places, and picking the wrong one is the usual mistake:

| You want to reject...                                  | Add it to |
| ------------------------------------------------------ | --------- |
| a malformed *outliner op* (wrong arity, wrong arg type) | `op-schema` in `deps/outliner/src/logseq/outliner/op.cljs:20-155` |
| an entity shape or property value                       | the relevant schema in `deps/db/src/logseq/db/frontend/malli_schema.cljs` |
| a domain rule about titles, tags or class extends       | `deps/outliner/src/logseq/outliner/validate.cljs` |
| a repairable inconsistency found in existing graphs     | a `fix-*!` fn in `src/main/frontend/worker/db/validate.cljs` |

The `validate.cljs` (outliner) convention is: throw an `ex-info` whose `ex-data` is
`{:type :notification :payload {:message "..." :type :error}}`. The worker's
`:thread-api/apply-outliner-ops` handler recognises `:type :notification` and broadcasts the message
to clients before rethrowing (`src/main/frontend/worker/handler/transaction.cljs:145-153`) — that is
the only supported way to surface a user-facing validation error from the worker.

Several outliner validators also carry `:i18n-key` / `:i18n-args` in the payload so the UI can
localise the message (e.g. `:node/built-in-cant-delete-error`,
`deps/outliner/src/logseq/outliner/core.cljs:1083-1087`).

> **Removed:** the `validate-task-block` example ("Task must have due-date or priority", "High
> priority tasks must have due-date"). No such business rule exists, and the code shown was also
> broken Clojure — it `conj`ed onto an immutable local and discarded the result. Task properties are
> not required: `:logseq.class/Task` declares `:properties` but no `:required-properties`
> (`deps/db/src/logseq/db/frontend/class.cljs:39-41`). Only `:logseq.class/Asset` and
> `:logseq.class/Pdf-annotation` declare `:required-properties` (`class.cljs:67-96`).

### Plugin API for Validation

> **Removed:** the `logseq.DB.validateBlock(...)` TypeScript example, labelled "(planned)".
> `validateBlock` appears nowhere in the repository, including `libs/src/LSPlugin.ts`, and no
> roadmap entry for it exists. Plugins have no validation entry point today; invalid writes surface
> as a thrown `"DB write failed with invalid data"` from the transact path
> (`deps/db/src/logseq/db.cljs:145-152`).

### Best Practices

**1. Don't hand-roll a pre-check — let the transact path reject the write**

The transaction path already validates against the speculative `db-after` and discards the write on
failure, which is strictly stronger than validating a map you are about to transact: it sees the
pipeline's own additions, and it can't drift out of sync with the schema.

```clojure
;; ❌ Redundant, and validates the wrong thing (the input, not the result)
(when-let [errors (some-hand-rolled-check db tx-data)]
  (throw (ex-info "Invalid transaction" {:errors errors})))
(ldb/transact! conn tx-data)

;; ✅ Just transact; an invalid write throws "DB write failed with invalid data"
;;    and never reaches the conn.  deps/db/src/logseq/db.cljs:145-180
(ldb/transact! conn tx-data {:outliner-op :save-block})
```

**2. Provide actionable errors**

The malli explainer already produces per-entity, per-key errors and `group-errors-by-entity`
enriches each one with its `:dispatch-key` and its page's `:block/name`
(`deps/db/src/logseq/db/frontend/validate.cljs:71-88`). When adding a domain check, match that
shape rather than returning a bare string.

**3. Batch, and let the batch validate once**

For large multi-op work, use `logseq.outliner.transaction/with-batch-tx` /
`logseq.db/batch-transact-with-temp-conn!`: inner ops run against a temp conn with
`:skip-validate-db? true` and the result is validated once on the way out
(`deps/db/src/logseq/db.cljs:262-290`). Chunking a transaction into 100-datom slices and validating
each slice, as this page previously advised, is both slower and wrong — cross-entity constraints
would be evaluated against an incomplete graph.

> **Removed:** the `validate-large-tx` chunking example and its `validate/validate-tx-data` call.
> There is no `validate-tx-data` function; the tx-time entry point is `validate-tx-report`, which
> takes a datascript **tx-report**, not tx-data (`validate.cljs:24-27`).

---

## Findings: New Capabilities

Integrity machinery that exists on `be800f1711` and was not described on this page before.

- **Outliner ops are malli-validated as a first line of defence.** `op-schema` is a
  `[:multi {:dispatch first}]` over **30** ops, each declaring its exact argument tuple, wrapped in
  an `ops-schema` registry that types `::block-id` as `uuid?`, `::property-id` as
  `qualified-keyword?`, and so on. `apply-ops!` opens with `(assert (ops-validator ops) ops)`, so a
  malformed op never reaches the tx builder
  (`deps/outliner/src/logseq/outliner/op.cljs:20-155, 157-196, 427-429`).

- **Load-time schema drift guards.** Three `let` forms at the bottom of `malli_schema.cljs` throw at
  namespace load if datascript's schema declares a cardinality-many ref, cardinality-one ref, or
  non-ref attribute that the malli schema does not cover. The malli and datascript schemas cannot
  drift apart silently (`malli_schema.cljs:617-647`).

- **Open vs. closed schema validation.** `mu/closed-schema` variants reject undeclared keys and are
  used for full-graph validation; tx-time validation uses the open variant so that in-flight UI
  attributes don't fail writes (`deps/db/src/logseq/db/frontend/validate.cljs:11-22`). Full-graph
  validation also strips `:block.temp/load-status` and `:block.temp/has-children?` before checking
  (`validate.cljs:96-99`).

- **`*closed-values-validate?*` is a separate, stricter mode.** Only `validate-local-db!` binds it
  (`validate.cljs:145-147`); when set, a closed-value property's value must be the `:db/id` of one
  of its declared choices, and a violation is logged with the full set of legal ids
  (`malli_schema.cljs:101-112`).

- **`graph-counts` as a cheap integrity signal.** `validate-db` returns entity/page/block/class/
  property/object/property-pair counts alongside errors, computed from `:avet` datoms
  (`validate.cljs:110-124`) — useful as a before/after check around a repair or migration.

- **Checksum diagnostics.** `frontend.worker.db.validate/recompute-checksum-diagnostics`
  (`src/main/frontend/worker/db/validate.cljs:330`) exists for detecting divergence in synced
  graphs.

- **Property history.** Properties marked `:logseq.property/enable-history?` record value changes
  as `:property-history-block` entities with their own malli branch requiring
  `:logseq.property.history/block` and `/property` (`malli_schema.cljs:450-469`;
  `deps/db/src/logseq/db/frontend/property.cljs:319-322`).

- **Soft delete.** `:logseq.property/deleted-at` (`malli_schema.cljs:284`) plus the
  `:restore-recycled` and `:recycle-delete-permanently` ops (`op.cljs:143-151`) mean deletion is
  recoverable, and orphan GC must account for recycled entities.

---

## Next Steps

**Continue Learning:**

1. **[Schema System](./schema-system.md)** - Data model details
2. **[Block Editor Architecture](./block-editor.md)** - Transaction flow
3. **[Real-Time Collaboration](../../tutorials/collaboration/real-time-sync.md)** - Multi-client validation
4. **[Complete Investigation Reports](../../../../../dz/docs/07-investigations/schema-integrity-08-index.md)** - 8-part deep dive

**For Contributors:**

- [Validation Source Code](../../../../../deps/db/src/logseq/db/frontend/validate.cljs)
- [Malli Schema Definitions](../../../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)
- [Transact driver / commit gate](../../../../../deps/db/src/logseq/db.cljs) (`transact-sync`)
- [Worker repair pass](../../../../../src/main/frontend/worker/db/validate.cljs)
- [Outliner op schema](../../../../../deps/outliner/src/logseq/outliner/op.cljs)
- [Migration System](../../../../../src/main/frontend/worker/db/migrate.cljs)
- [PR Review Standards](../../../../../dz/docs/07-investigations/schema-integrity-07-pr-review-standards.md)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
