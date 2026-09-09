---
title: 'Block Editor Architecture'
status: 🟢 Stable
version: 'Schema v65.33'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Schema System](./schema-system.md)'
  - '[Database Integrity](./database-integrity.md)'
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
tags: ['editor', 'outliner', 'blocks', 'architecture', 'datascript']
---

# Block Editor Architecture

This document explains Logseq's block editor—the core system enabling hierarchical note-taking with outliner-style operations. Understanding this architecture is essential for contributors and plugin developers working with block manipulation.

> **Scope:** this page describes **DB graphs**. Where behaviour differs for legacy file graphs it is
> called out explicitly.

> **Audited 2026-09-09 against `be800f1711`.** The single largest correction: the outliner core
> does **not** run on the UI thread. The UI collects ops and ships them across a worker boundary;
> `logseq.outliner.core` executes inside the db worker. Every code listing on this page was
> previously illustrative pseudo-code with invented function names and line numbers; those are
> replaced with real source below or marked `> **Removed:**`.

**What You'll Learn:**

- Outliner operation model
- Block hierarchy and ordering
- Transaction pipeline
- Auto-save mechanism
- Editor state management

**Prerequisites:**

- Familiarity with Logseq blocks
- Understanding of DataScript ([Schema System](./schema-system.md))
- Basic ClojureScript knowledge (for contributors)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Outliner Operations](#outliner-operations)
3. [Block Hierarchy](#block-hierarchy)
4. [Transaction Pipeline](#transaction-pipeline)
5. [Auto-Save System](#auto-save-system)
6. [Editor State](#editor-state)
7. [Developer Guide](#developer-guide)
8. [Findings: New Capabilities](#findings-new-capabilities)

---

## Architecture Overview

### Multi-Layer Editor Stack

```
┌─────────────────────────────────────────────────────────┐
│                    UI Layer                             │
│  • A single <textarea> for the block being edited,      │
│    over a mock-textarea used for sizing/highlighting    │
│    (components/editor.cljs:566, 773-774)                │
│  • React/Rum components                                 │
│  • Keyboard shortcuts                                   │
│  • Context menus                                        │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Editor Handler Layer                         │
│  • Edit mode management                                 │
│  • Input parsing and validation                         │
│  • Command dispatch                                     │
│  • Auto-save coordination                               │
│  Location: src/main/frontend/handler/editor.cljs        │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Op Collection (still on the UI thread)       │
│  • ui-outliner-tx/transact! macro binds *outliner-ops*  │
│  • body pushes [:op args] entries instead of tx-data    │
│  Location: src/main/frontend/modules/outliner/ui.cljc   │
└────────────────────┬────────────────────────────────────┘
                     │
 ==================  WORKER BOUNDARY (postMessage)  ======
                     │  :thread-api/apply-outliner-ops
                     │  src/main/frontend/worker/handler/
                     │      transaction.cljs:94
┌────────────────────▼────────────────────────────────────┐
│            Outliner Op Layer (db worker)                │
│  • malli op-schema: 30 ops, asserted before dispatch    │
│  • apply-ops! wraps everything in one batch tx          │
│  Location: deps/outliner/src/logseq/outliner/op.cljs    │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Outliner Core Layer (db worker)              │
│  • Block operations (insert, move, delete)              │
│  • Hierarchy + built-in-node guards                     │
│  • Transaction building                                 │
│  • Reference management                                 │
│  Location: deps/outliner/src/logseq/outliner/core.cljs  │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            DataScript Layer (db worker)                 │
│  • speculative d/with, pipeline, malli validate, commit │
│  • datascript.storage -> db.sqlite kvs table            │
│  Location: deps/db/src/logseq/db.cljs:153-186           │
└─────────────────────────────────────────────────────────┘
```

`src/main/frontend/handler/editor.cljs` is real and is the editor handler (4477 lines on
`be800f1711`), but it dispatches ops rather than building transactions itself.

> **Removed:** "Editable contentEditable elements" in the UI layer. `contentEditable` does not
> appear in `src/main/frontend/components/editor.cljs`; editing uses `ui/ls-textarea` plus a
> `mock-textarea` shadow element (`components/editor.cljs:566, 773-774`).

### Core Concepts

**Block**: Fundamental data unit with hierarchical structure

```clojure
{:db/id 123
 :block/uuid #uuid "abc-123"
 :block/title "Block content"
 :block/parent {:db/id 456}     ; Parent block reference
 :block/page {:db/id 789}       ; Page reference
 :block/order "a0"              ; Fractional indexing order (base-62)
 :block/refs #{{:db/id 321}}    ; Page/block references (cardinality-many)
 :block/tags #{{:db/id 654}}    ; Class tags (cardinality-many)
 :block/created-at 1706198400000
 :block/updated-at 1706198450000}
```

Every attribute above is in the DB-graph datascript schema
(`deps/db/src/logseq/db/frontend/schema.cljs:56-106`). Note there is **no `:block/properties`**
attribute in DB graphs — property values are datoms keyed by each property's own `:db/ident`. (A
`:block/properties` key is synthesised onto entity *maps* purely for malli validation:
`deps/db/src/logseq/db/frontend/malli_schema.cljs:144`.) Likewise `:block/marker`, `:block/content`
and `:block/left` are file-graph-era attributes that do not exist here.

**Outliner Operation**: Atomic block manipulation (insert, move, delete, indent, outdent)

**Transaction**: DataScript change set applied atomically

---

## Outliner Operations

### The 30 Outliner Ops

Everything the editor does is expressed as one of 30 ops, each with a malli-checked argument tuple
(`deps/outliner/src/logseq/outliner/op.cljs:20-155`):

| Group      | Ops |
| ---------- | --- |
| blocks     | `:save-block` `:insert-blocks` `:apply-template` `:delete-blocks` `:move-blocks` `:move-blocks-up-down` `:indent-outdent-blocks` `:collapse-expand-blocks` |
| properties | `:upsert-property` `:set-block-property` `:remove-block-property` `:delete-property-value` `:batch-delete-property-value` `:create-property-text-block` `:collapse-expand-block-property` `:batch-set-property` `:batch-remove-property` `:class-add-property` `:class-remove-property` `:upsert-closed-value` `:delete-closed-value` `:add-existing-values-to-closed-values` |
| import     | `:batch-import-edn` |
| raw        | `:transact` |
| pages      | `:create-page` `:rename-page` `:delete-page` |
| recycle    | `:restore-recycled` `:recycle-delete-permanently` |
| social     | `:toggle-reaction` |

`apply-ops!` asserts the whole sequence against the schema, then runs every op inside a single
`outliner-tx/transact!` batch, so a multi-op user action lands as one transaction
(`op.cljs:427-450`).

### Core Operations

**Location:** `deps/outliner/src/logseq/outliner/core.cljs` (1337 lines on `be800f1711`)

Each public op is a thin `op-transact!` wrapper around a pure-ish builder. The wrapper is what
attaches `:outliner-op` to the tx-meta and performs the actual `ldb/transact!`
(`core.cljs:1282-1298`):

```clojure
(defn- op-transact!
  [outliner-op f & args]
  (try
    (let [result (apply f args)]
      (when result
        (let [tx-meta (-> (outliner-tx-meta/ensure-outliner-ops
                           (:tx-meta result)
                           (direct-op-entry outliner-op args))
                          (assoc :outliner-op outliner-op))]
          (ldb/transact! (first args) (:tx-data result) tx-meta)))
      result)
    (catch :default e
      (when-not (= "not-allowed-move-block-page" (ex-message e))
        (js/console.error e)
        (throw e)))))
```

The real signatures, re-pinned:

| Public fn                  | Line | Builder             | Line |
| -------------------------- | ---- | ------------------- | ---- |
| `save-block!` `[conn block & {:as opts}]` | 1302 | `save-block` `[db block opts]` | 522 |
| `insert-blocks!` `[conn blocks target-block opts]` | 1311 | `insert-blocks` `[db blocks target-block opts]` | 795 |
| `delete-blocks!` `[conn blocks opts]` | 1320 | `delete-blocks` `[db blocks _opts]` | 1011 |
| `move-blocks!` `[conn blocks target-block opts]` | 1324 | `move-blocks` `[conn blocks target-block opts]` | 1132 |
| `move-blocks-up-down!` `[conn blocks up?]` | 1331 | `move-blocks-up-down` `[conn blocks up?]` | 1176 |
| `indent-outdent-blocks!` `[conn blocks indent? & {:as opts}]` | 1335 | `indent-outdent-blocks` `[conn blocks indent? & {:keys [parent-original logical-outdenting?]}]` | 1214 |

> **Removed:** every previous location on this page — `core.cljs:450-520` for insert,
> `600-750` for move, `850-920` for delete, `1050-1120` for indent/outdent, and
> `292-330` for a function called `outliner-insert-block!`. None of the line ranges were correct and
> `outliner-insert-block!` does not exist. The invented signatures
> (`insert-blocks! [repo db blocks target-block sibling-before?]`,
> `delete-blocks [db block-uuids & {:keys [children? page?]}]`,
> `indent-outdent-blocks [blocks indent?]`) do not match source either — note in particular that
> the mutating fns take a **`conn`**, not a `db`, and that ordering is expressed through an `opts`
> map (`:sibling?`, `:top?`, `:bottom?`, `:before?`), not a positional `sibling-before?` flag
> (`core.cljs:795-819`).

**Insert options** that actually exist, from the `insert-blocks` docstring (`core.cljs:796-813`):
`:sibling?`, `:bottom?`, `:top?`, `:keep-uuid?`, `:keep-block-order?`, `:outliner-op`,
`:replace-empty-target?`, `:update-timestamps?` (defaults to `true`).

**Delete** collects the whole subtree (`block-subtree-ids`) plus any comment areas that become
orphaned by the deletion, and refuses outright if any target is a built-in entity
(`core.cljs:1011-1033`).

**Indent/outdent** refuses non-consecutive selections, and refuses to outdent property-value blocks
(`core.cljs:1219-1225`).

### Operation Validation

What is actually checked, and how it fails:

| Check | Where | Failure mode |
| ----- | ----- | ------------ |
| op shape / arity | `op.cljs:427-429` | `assert` throws |
| target is a block map or entity | `core.cljs:819`, `1137` | op returns `nil`, no-op |
| built-in node modified / deleted | `core.cljs:526-536`, `1028-1033`, `1138-1144` | `ex-info` with `{:type :notification}`; worker broadcasts the message to the UI |
| move would make a parent a child of its own descendant | `core.cljs:1168-1174` | **silent no-op** |
| move target is itself one of the moved blocks | `core.cljs:1166` | silent no-op |
| move a page to be a child of a block, or sibling of a parentless target | `core.cljs:1062-1074` | `ex-info "not-allowed-move-block-page"`, swallowed by `op-transact!` |
| page given a block as parent | `deps/db/src/logseq/db.cljs:91-101` | `ex-info "Page can't have block as parent"` |
| resulting entities malli-valid | `deps/db/src/logseq/db.cljs:161-172` | `ex-info "DB write failed with invalid data"`, conn untouched |

> **Removed:** "Same page: blocks being moved must stay on same page (in most cases)" and the
> `validate-move` / `same-page?` listing cited to `deps/outliner/src/logseq/outliner/validate.cljs`.
> **Cross-page moves are explicitly supported**: `move-block` computes `not-same-page?`, reassigns
> `:block/page` on the moved block, and emits `children-page-tx` to reassign every descendant's
> `:block/page` too (`core.cljs:1075-1098`). Neither `validate-move` nor `same-page?` exists;
> `outliner/validate.cljs` validates page titles, tags and class extends, not block moves.

> **Removed:** "Valid order: New order must be valid fractional index" as an *operation*
> pre-condition. Orders are generated by the outliner, never supplied by the caller
> (`db-order/gen-key`, `core.cljs:1078-1082`); the fractional-index check lives in the malli
> schema and fires at validation time
> (`deps/db/src/logseq/db/frontend/malli_schema.cljs:26-30`).

---

## Block Hierarchy

### Order System (Fractional Indexing)

**Block Order:** a lexicographically-sortable **base-62** string, so a block can be inserted between
two siblings without renumbering anything.

**Location:** `deps/db/src/logseq/db/common/order.cljs` (78 lines) — a thin wrapper over the
`logseq.clj-fractional-indexing` library.

```clojure
(defn gen-key
  ([]        (gen-key @*max-key nil))
  ([end]     (gen-key @*max-key end))
  ([start end & {:keys [max-key-atom] :or {max-key-atom *max-key}}]
   (let [k (index/generate-key-between start end)]
     (reset-max-key! max-key-atom k)
     k)))

(defn gen-n-keys [n start end & {...}]
  (let [ks (index/generate-n-keys-between start end n)]
    (reset-max-key! max-key-atom (last ks))
    ks))

(defn validate-order-key? [key]
  (index/validate-order-key key index/base-62-digits)
  true)
```

Callers pass the *neighbouring* orders and get a key strictly between them — for example
`move-block` computes, for a sibling move,
`(db-order/gen-key (:block/order target-block) (:block/order (ldb/get-right-sibling target-block)))`,
and for a child move `(db-order/gen-key nil (:block/order (ldb/get-down target-block)))`
(`deps/outliner/src/logseq/outliner/core.cljs:1078-1082`). Bulk inserts use `gen-n-keys`.

`*max-key` is a module-level atom tracking the highest key seen, so appends can be generated without
querying; `get-max-order` reads it back off the db via `d/rseek-datoms db :avet :block/order`
(`order.cljs:28-30`), which works because `:block/order` is `:db/index true`
(`deps/db/src/logseq/db/frontend/schema.cljs:64`).

> **Removed:** `get-order-between` and its examples
> (`(get-order-between "a00" "b00") => "aZZ"`, `nil -> "a00"`, `"b00" nil -> "c00"`), cited to
> `order.cljs:50-120`. No such function exists, the file is only 78 lines, and the alphabet is
> base-62 (`0-9A-Za-z`), so `"aZZ"` is not a key this generator produces. The public API is
> `gen-key` / `gen-n-keys` / `get-prev-order` / `get-next-order` / `get-max-order` /
> `validate-order-key?` / `reset-max-key!`.

> **Removed:** "O(1) insertion". Generating one key is O(1) in the number of siblings — which is the
> real win versus renumbering — but the generated string grows as repeated insertions subdivide the
> same interval, so it is not O(1) in key length.

Orders are also validated: the malli schema types `:block/order` as
`[:and :string [:fn ... db-order/validate-order-key?]]`, so a bad key is rejected at transact time
(`deps/db/src/logseq/db/frontend/malli_schema.cljs:26-30`).

**Example Hierarchy** (illustrative keys; real generated keys are base-62 and often shorter):

```
Block A     order: "a0"
Block B     order: "a1"
  Child 1   order: "a0"   (children are ordered among themselves)
  Child 2   order: "a1"
Block C     order: "a2"
```

Sibling order is scoped by `:block/parent`, not globally: two blocks may share an order string as
long as they have different parents.

### Parent-Child Relationships

**Representation:**

```clojure
;; Parent → Children (query) — note ?parent-id must actually be bound in the clause
(d/q '[:find [?child ...]
       :in $ ?parent-id
       :where [?child :block/parent ?parent-id]]
     db parent-id)

;; Parent → Children (reverse lookup, preferred in practice)
(:block/_parent parent-entity)

;; Child → Parent
(:block/parent child-entity)
```

> **Removed:** the previous query left `?parent` unbound — it never unified with the `?parent-id`
> input, so it would have matched every block with any parent. `:block/parent` is
> `:db/valueType :db.type/ref` with `:db/index true`
> (`deps/db/src/logseq/db/frontend/schema.cljs:62-63`), which is what makes both the forward query
> and the `:block/_parent` reverse lookup cheap.

Children come back unordered; callers sort by `:block/order`. The outliner reads the tree through
`logseq.db` helpers rather than raw queries — `get-block-last-direct-child-id`, `get-left-sibling`,
`get-right-sibling`, `get-first-child`, `get-down`, `get-block-parents`,
`get-block-full-children-ids` (used throughout `deps/outliner/src/logseq/outliner/core.cljs`).

**Page-Level Blocks:**

```clojure
;; Top-level block (parent = page itself)
{:db/id 123
 :block/parent {:db/id 456}  ; 456 = page entity
 :block/page {:db/id 456}}   ; Same as parent

;; Nested block
{:db/id 789
 :block/parent {:db/id 123}  ; 123 = parent block
 :block/page {:db/id 456}}   ; Still references page
```

This is enforced in both directions:

- A **page** may not have a **block** as its parent — `throw-if-page-has-block-parent!` runs on
  every transaction's tx-data and throws `"Page can't have block as parent"`
  (`deps/db/src/logseq/db.cljs:91-101, 163`).
- Moving a **page** to be the child of a **block** is refused by the outliner
  (`move-page-as-block-child?` → `ex-info "not-allowed-move-block-page"`,
  `deps/outliner/src/logseq/outliner/core.cljs:1066-1074`).
- Both `:block/parent` and `:block/page` are required on a normal block by the malli schema
  (`deps/db/src/logseq/db/frontend/malli_schema.cljs:406-414`).
- A cross-page move rewrites `:block/page` on the moved block **and** on every descendant
  (`core.cljs:1092-1098`).

---

## Transaction Pipeline

### Transaction Flow

```
1. User action (keyboard, context menu)                UI thread
   ↓
2. Handler dispatch                                    src/main/frontend/handler/editor.cljs
   ↓
3. ui-outliner-tx/transact! binds *outliner-ops*       modules/outliner/ui.cljc:9-27
   body pushes [:op args] entries; nested calls reuse
   the outer binding so they collapse into one batch
   ↓
4. frontend.db.transact/apply-outliner-ops             db/transact.cljs:175-205
   - serialises mutations per repo (enqueue-outliner-mutation!)
   - first sends :thread-api/undo-redo-set-pending-editor-info
     so undo can restore the caret
   ↓
================ WORKER BOUNDARY ================
   ↓
5. :thread-api/apply-outliner-ops                      worker/handler/transaction.cljs:94
   ↓
6. logseq.outliner.op/apply-ops!                       op.cljs:427-450
   - (assert (ops-validator ops) ops)
   - builds opts': :transact-opts {:conn conn}, :local-tx? true,
     :outliner-ops semantic-ops, :db-sync/tx-id (random-uuid)
   - outliner-tx/transact! wraps the whole doseq in ONE batch tx
   ↓
7. per-op apply-op! -> outliner-core fn -> op-transact! -> ldb/transact!
   ↓
8. logseq.db/transact-sync                             deps/db/src/logseq/db.cljs:153-186
   d/with -> transact-pipeline -> guards -> malli validate -> compare-and-set! -> store
   ↓
9. Response back to the UI: {:result ... :delta ... :editor-rows ...}
   plus perf timings                                   worker/handler/transaction.cljs:96-148
```

### The UI Transaction Wrapper (real source)

**Location:** `src/main/frontend/modules/outliner/ui.cljc` — it is a **macro in a `.cljc` file**,
not a function in `ui.cljs`:

```clojure
(defmacro transact!
  [opts & body]
  `(let [test?# frontend.util/node-test?
         ops# frontend.modules.outliner.op/*outliner-ops*
         editor-info# (frontend.state/get-editor-info)]
     (reset! frontend.state/*editor-info editor-info#)
     (if ops#
       (do ~@body)                              ; nested transact! — reuse outer binding
       (binding [frontend.modules.outliner.op/*outliner-ops* (transient [])]
         ~@body
         (let [r# (persistent! frontend.modules.outliner.op/*outliner-ops*)]
           (frontend.db.transact/apply-outliner-ops nil r# ~opts))))))
```

Two behaviours worth knowing:

- **Nesting collapses.** If `*outliner-ops*` is already bound, the inner `transact!` just runs its
  body; only the outermost call ships the batch. So composing outliner helpers does not produce
  multiple transactions.
- **Editor cursor info is snapshotted** into `frontend.state/*editor-info` before the body runs —
  that is what lets undo restore the caret. `apply-outliner-ops` forwards it to the worker via
  `:thread-api/undo-redo-set-pending-editor-info` immediately before the ops
  (`src/main/frontend/db/transact.cljs:195-197`;
  `src/main/frontend/worker/undo_redo.cljs:29-36, 76-86`).

> **Removed:** the previous `(defn transact! [{:keys [outliner-op]} f] ...)` listing cited to
> `src/main/frontend/modules/outliner/ui.cljs`, along with its body
> (`state/set-editor-op!`, `refresh-affected-blocks!`, `schedule-auto-save!`,
> `state/clear-editor-op!`). The file is `ui.cljc`, `transact!` is a macro taking `[opts & body]`,
> and none of those four functions exist. In particular the wrapper does **not** trigger auto-save:
> auto-save is an independent debounce on the input element (see below).

> **Removed:** the `outliner-insert-block!` transaction-building listing cited to
> `core.cljs:292-330`. The function does not exist; block insertion is
> `insert-blocks!` → `insert-blocks` → `build-insert-blocks-tx`
> (`core.cljs:1311`, `795`, `614`). Notably, the listing had the UI wrapper calling
> `d/transact!` directly — the UI thread never transacts against the graph conn at all.

---

## Auto-Save System

### Auto-Save Flow

```
1. Block edit (user types)  -> edit-box-on-change!
   ↓
2. state/set-edit-content!  (in-memory, immediate)
   ↓
3. clear-block-auto-save-timeout!  + block-handler/mark-last-input-time!
   ↓
4. setTimeout 450ms; on fire, save only if BOTH:
     - (state/input-idle? repo :diff 450)      ; still idle after the delay
     - the value contains no "#tag"            ; tags are saved explicitly instead
   ↓
5. save-current-block! {:skip-properties? true}
   ↓
6. save-block-aux! -> save-block-if-changed! -> :save-block outliner op
   ↓
7. Worker: outliner-core/save-block! -> ldb/transact! -> datascript.storage -> db.sqlite
```

### Debounced Save (real source)

**Location:** `src/main/frontend/handler/editor.cljs:2229-2245` (re-pinned; it was cited as
`260-280`, which is inside an unrelated part of the file).

```clojure
(defn edit-box-on-change!
  [e block id]
  (when (current-edit-block? block)
    (let [value (util/evalue e)
          repo (state/get-current-repo)]
      (state/set-edit-content! id value false)
      (clear-block-auto-save-timeout!)
      (block-handler/mark-last-input-time! repo)
      (reset! *auto-save-timeout
              (js/setTimeout
               (fn []
                 (when (and (state/input-idle? repo :diff 450)
                            ;; don't auto-save block if it has tags
                            (not (re-find #"#\S+" value)))
                   ;; don't auto-save for page's properties block
                   (save-current-block! {:skip-properties? true})))
               450)))))
```

The timeout atom is `*auto-save-timeout` (`editor.cljs:1709`), and `save-current-block!` clears it
on entry so an explicit save can't race the pending one (`editor.cljs:1722-1740`).

> **Removed:** the `schedule-auto-save!` listing with its `*save-timeout` atom and
> `(or delay 450)` argument. Neither `schedule-auto-save!` nor `*save-timeout` exists; the delay is
> a hard-coded literal, not a parameter, and it is used **twice** — once as the `setTimeout` delay
> and again as the `input-idle?` threshold, which the old description missed entirely.

> **Unverified:** the "Why 450ms?" rationale (performance vs. data-loss vs. perceptibility). It is
> plausible but the number carries no comment or ADR in the source; treat it as unexplained.

Note also that `save-block-aux!` refuses to save built-in entities
(`(not (ldb/built-in? entity))`, `editor.cljs:1684-1691`), and that `save-current-block!` bails out
while an IME composition or an editor action (slash menu, autocomplete) is active
(`editor.cljs:1727-1729`).

### Persistence

**Database graphs.** There is no "export DataScript to transit and save". Persistence is a
datascript **storage backend**: after a successful `compare-and-set!`, `transact-sync` calls
`dc/store-after-transact!` (`deps/db/src/logseq/db.cljs:177`), which writes changed storage pages
into a single SQLite table:

```sql
create table if not exists kvs (addr INTEGER primary key, content TEXT, addresses JSON)
```

(`deps/db/src/logseq/db/common/sqlite.cljs:11-13`.) The file is `db.sqlite` in the graph directory
(`common/sqlite.cljs:30-33`; the worker opens `/db.sqlite` plus `/search-db.sqlite` at
`src/main/frontend/worker/db_core.cljs:547-551`).

> **Removed:** the `save-db!` listing cited to `src/main/frontend/persist_db.cljs`. That namespace
> exists (446 lines) but contains no `save-db!`; it manages the db-worker runtime, graph
> open/close/list/import/export and markdown-mirror settings. There is no `export-db` +
> `sqlite/save!` pair, and nothing in the DB-graph write path serialises the whole database to
> transit on each save.

> **Removed:** the file-graph `write-file!` listing cited to
> `src/main/frontend/worker/file.cljs`. **That file does not exist on `be800f1711`.**

---

## Editor State

### State Management

**Location:** `src/main/frontend/state.cljs` (2160 lines)

The edit-related keys, verbatim from the initial state map (`state.cljs:185-215`) and their real
shapes:

| Key | Shape | Notes |
| --- | ----- | ----- |
| `:editor/editing?` | `{editing-block-id true}` or `nil` | **a map, not a boolean.** `editing?` is `(seq (get-state :editor/editing?))` (`state.cljs:945`) |
| `:editor/block` | entity or `nil` | the block being edited (`state.cljs:941`) |
| `:editor/content` | `{block-id "text"}` | **a map keyed by block id**, not a single string (`state.cljs:970-981`) |
| `:editor/cursor-range` | range or `nil` | `state.cljs:997-1001` |
| `:editor/container-id` | int | which container the edit is in |
| `:editor/op`, `:editor/action`, `:editor/action-data` | keyword / map | current editor op and in-progress UI action (slash menu, autocomplete) |
| `:editor/last-input-time` | `{repo ms}` | drives `input-idle?` |
| `:editor/last-saved-cursor` | map | caret restoration |
| `:editor/block-refs` | set | refs accumulated during the edit |

`editing-block-id` is itself compound: `[container-id block-uuid]`, or
`[container-id property-block-uuid block-uuid]` when editing a property value
(`state.cljs:1842-1846`). That is why `:editor/editing?` is a map — the same block can be open in
more than one container.

> **Removed:** the `(defonce *state (atom {:editor/editing? false :editor/block nil
> :editor/content "" :editor/cursor-pos 0 :editor/last-saved-at nil}))` listing and its
> getters/setters (`get-edit-block`, `editing?`, `set-edit-block!`, `clear-edit!`). Three of those
> five keys are the wrong type (`:editor/editing?` is a map, `:editor/content` is a map),
> `:editor/cursor-pos` and `:editor/last-saved-at` do not exist (the real keys are
> `:editor/cursor-range` and `:editor/last-input-time`), and `set-edit-block!` / `clear-edit!`
> are not defined anywhere. State is also not a bare `defonce` atom any more — it is accessed
> through `get-state` / `set-state!` / `swap-state!` over an rfx-backed store.

### Edit Mode Lifecycle

```
1. User clicks a block or presses Enter
   ↓
2. block-handler/edit-block!            src/main/frontend/handler/block.cljs:107
   -> edit-loaded-block! (block.cljs:81) after the block is loaded
   -> edit-block-aux (block.cljs:56) -> state/set-editing! (state.cljs:1825)
   ↓
3. set-editing! swaps in, atomically:
     :editor/block-refs #{}
     :editor/block <block>
     :editor/editing? {editing-block-id true}
     :editor/container-id <container-id>
     :editor/last-key-code nil
     :editor/set-timestamp-block nil
     :editor/cursor-range <cursor-range>
     [:editor/content block-id] <content>
   then focuses the input and restores the caret          state.cljs:1850-1861
   ↓
4. User edits -> edit-box-on-change! -> debounced save    editor.cljs:2229-2245
   ↓
5. Exit -> save-current-block!, then the editing keys are
   reset to :editor/editing? nil, :editor/block nil,
   :editor/content {}   (clear-edit!)                    state.cljs:1296-1315
```

Two side effects worth knowing about, both in `set-editing!`:

- If more than one RTC user is online it publishes an `:rtc/presence-update` event carrying the
  block being edited (`state.cljs:1863-1866`).
- If the block's `:logseq.property.node/display-type` is `:code` it hands off to the code editor via
  `:editor/focus-code-editor` (`state.cljs:1867-1868`).

> **Removed:** the `edit-block!` listing cited to `src/main/frontend/handler/block.cljs:76`. The
> function is at line **107**, and its body is not
> `set-edit-block!` / `set-editor-editing!` / `focus-editor!` / `set-cursor-pos!` — none of which
> exist. The real path is `edit-block!` → `edit-loaded-block!` → `state/set-editing!`.

---

## Developer Guide

### Implementing Custom Block Operations

The pattern is: wrap your work in `ui-outliner-tx/transact!` and call **outliner op helpers** inside
it. Never build tx-data on the UI thread and never call `d/transact!` against the graph conn — the
UI thread does not own it.

```clojure
(ns my.feature
  (:require [frontend.db :as db]
            [frontend.modules.outliner.ui :as ui-outliner-tx]
            [frontend.modules.outliner.op :as outliner-op]))

(defn duplicate-block!
  [block-uuid]
  (when-let [block (db/entity [:block/uuid block-uuid])]
    (ui-outliner-tx/transact!
     {:outliner-op :insert-blocks}
     (outliner-op/insert-blocks!
      [{:block/uuid (random-uuid)
        :block/title (:block/title block)}]
      block-uuid                     ; target: insert relative to this block
      {:sibling? true}))))
```

The `frontend.modules.outliner.op` fns do not transact — they `conj!` an `[op args]` entry onto the
dynamic `*outliner-ops*` vector, and throw
`"<op> not used in (transact! ...)"` if called outside a `transact!` body
(`src/main/frontend/modules/outliner/op.cljs:12-45`). Everything the body enqueues is then validated
against `op-schema` and applied as one transaction in the worker
(`deps/outliner/src/logseq/outliner/op.cljs:427-450`). Orders, timestamps, `:block/page` and ref
rebuilding are all computed on the worker side — do not set them yourself.

> **Removed:** the previous `duplicate-block!` example. It was wrong in five ways, each of which is
> a real trap: (1) `(state/get-current-repo)` returns a repo string, not a `db`, and it was bound to
> `db` and passed to `d/transact!`; (2) `:block/properties` is not a DB-graph attribute, so
> `select-keys` on it yields nothing; (3) `calculate-order-after` does not exist — orders come from
> `db-order/gen-key` on the worker side; (4) `ui-outliner-tx/transact!` is a macro taking
> `[opts & body]`, not `[opts f]`, so passing `(fn [] ...)` would transact nothing; (5) the UI
> thread has no handle on the graph conn to `d/transact!` against.

### Plugin API for Blocks

**Available via `@logseq/libs`** (`libs/src/LSPlugin.ts:775-815, 892-900`):

```typescript
// Insert block
await logseq.Editor.insertBlock(srcBlock, 'New block content', {
  before: false,
  sibling: true,
  start: false,
  end: false,
  customUUID: undefined,
  properties: {},
})

// Insert a subtree in one call
await logseq.Editor.insertBatchBlock(srcBlock, batch, { before, sibling, keepUUID })

// Update block
await logseq.Editor.updateBlock(blockUuid, 'Updated content', { properties: {} })

// Move block  — note the options are { before, children }, NOT { sibling, before }
await logseq.Editor.moveBlock(blockUuid, targetBlockUuid, {
  before: true,
  children: false,
})

// Delete block
await logseq.Editor.removeBlock(blockUuid)

// Get block — takes the identity directly, not a {uuid} map
const block = await logseq.Editor.getBlock(blockUuid, { includeChildren: false })
```

> **Removed:** `moveBlock(..., { sibling: true, before: true })`. `moveBlock`'s options are
> `Partial<{ before: boolean; children: boolean }>` — there is no `sibling` option on move
> (`libs/src/LSPlugin.ts:894-898`). Contrast `insertBlock`, which *does* take `sibling`.

> **Removed:** `getBlock({ uuid: blockUuid })`. The first parameter is
> `BlockIdentity | EntityID`, so pass the uuid (or entity id) itself
> (`libs/src/LSPlugin.ts:807-810`).

**See:** [Plugin API Reference](../../reference/api/plugin-api.md)

---

## Findings: New Capabilities

Editor-relevant machinery on `be800f1711` that this page did not describe.

- **Worker-owned undo/redo.** History is recorded as typed ops (`::db-transact`,
  `::record-editor-info`, `::ui-state`) validated by a malli schema, with the caret position
  captured before each mutation so undo restores it
  (`src/main/frontend/worker/undo_redo.cljs:11-63`; ADRs `docs/adr/0002`, `0011`, `0012`, `0013`).

- **Mutations are serialised and instrumented.** `enqueue-outliner-mutation!` queues per repo, and
  the worker returns per-phase timings (`:apply-ms`, `:listener-ms`, `:plain-ms`, `:total-ms`) plus
  a `:delta` of changed rows and pre-fetched `:editor-rows` for the blocks under the caret
  (`src/main/frontend/db/transact.cljs:26-40, 175-205`;
  `src/main/frontend/worker/handler/transaction.cljs:80-148`).

- **Comment-aware outliner guards.** `:logseq.class/Comments` / `:logseq.class/Comment` blocks have
  their own move/delete rules — `move-source-allowed-for-comments?`,
  `move-target-allowed-for-comments?`, `protected-comment-block?` — and deleting a block also
  deletes comment areas it orphans (`deps/outliner/src/logseq/outliner/core.cljs:935-1009`).

- **Recycle bin.** `ldb/recycled?` blocks are read-only in the editor
  (`src/main/frontend/handler/block.cljs:83-84`), with `:restore-recycled` and
  `:recycle-delete-permanently` ops for the round trip (`op.cljs:143-151`).

- **Typed display blocks.** `:logseq.property.node/display-type` routes `:code`, `:quote` and
  `:math` blocks to dedicated editors; `set-editing!` hands `:code` off via
  `:editor/focus-code-editor` (`src/main/frontend/state.cljs:1867-1868`), and the classes
  `Code-block` / `Quote-block` / `Math-block` carry the schema
  (`deps/db/src/logseq/db/frontend/class.cljs:76-89`).

- **Templates as a first-class op.** `:apply-template` resolves dynamic journal references and
  creates any missing journal pages before inserting
  (`deps/outliner/src/logseq/outliner/op.cljs:239-288`).

- **RTC presence.** Entering edit mode on a block broadcasts `:rtc/presence-update` when more than
  one user is online (`src/main/frontend/state.cljs:1863-1866`).

---

## Next Steps

**Continue Learning:**

1. **[Database Integrity](./database-integrity.md)** - Transaction validation
2. **[Schema System](./schema-system.md)** - Data model details
3. **[Plugin API Reference](../../reference/api/plugin-api.md)** - Block manipulation API
4. **[Query System](../../reference/advanced/query-system.md)** - Query blocks

**For Contributors:**

- [Outliner Op Schema](../../../../../deps/outliner/src/logseq/outliner/op.cljs) (the 30 ops)
- [Outliner Core Source](../../../../../deps/outliner/src/logseq/outliner/core.cljs)
- [UI Op Collection](../../../../../src/main/frontend/modules/outliner/ui.cljc) (the `transact!` macro)
- [Worker Op Handler](../../../../../src/main/frontend/worker/handler/transaction.cljs)
- [Editor Handler Source](../../../../../src/main/frontend/handler/editor.cljs)
- [Block Order Implementation](../../../../../deps/db/src/logseq/db/common/order.cljs)
- [Transact driver / commit gate](../../../../../deps/db/src/logseq/db.cljs) (`transact-sync`)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
