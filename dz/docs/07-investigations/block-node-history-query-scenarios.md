# Logseq Investigation — Block/Node History Query Scenarios

**Investigation Date:** February 19, 2026

> **Last revised:** 2026-05-30

**Status:** Validated against current source in `src/`, `deps/`, and `packages/` for history-related debugging and feature design

---

## Executive Summary

Logseq currently has two different history mechanisms:

1. **Block content versions (RTC/server history)** — queried via worker RTC API
2. **Node property history (local graph entities)** — queried via DataScript Datalog on `:logseq.property.history/*`

These mechanisms are complementary and should not be treated as interchangeable.

---

## 1) Scenario A — Get block content versions (RTC history)

Use worker API:

```clojure
;; frontend -> db-worker
(state/<invoke-db-worker
 :thread-api/rtc-get-block-content-versions
 token graph-uuid block-uuid)
```

> **Removed:** The previously documented underlying websocket payload shape is not verifiable from the current `src/`, `deps/`, and `packages/` sources. The worker API entry point and dev-menu call site still exist, but the transport payload contract is no longer documented at this layer.

**When to use**

- Show content timeline for one block
- Compare current text vs previous server versions
- Debug remote overwrite/conflict reports

---

## 2) Scenario B — Get all property-history entries for one node

```clojure
[:find [(pull ?h [*]) ...]
 :in $ ?block-id
 :where
 [?h :logseq.property.history/block ?block-id]]
```

Recommended post-processing:

- Sort by `:block/created-at`
- Resolve referenced entities for readable values

---

## 3) Scenario C — Get history for one property on one node

```clojure
[:find ?h ?created ?ref ?scalar
 :in $ ?block-id ?prop-ident
 :where
 [?h :logseq.property.history/block ?block-id]
 [?h :logseq.property.history/property ?p]
 [?p :db/ident ?prop-ident]
 [?h :block/created-at ?created]
 [(get-else $ ?h :logseq.property.history/ref-value nil) ?ref]
 [(get-else $ ?h :logseq.property.history/scalar-value nil) ?scalar]]
```

Example `?prop-ident` values:

- `:logseq.property/status`
- `:logseq.property/priority`
- `:logseq.property/scheduled`

---

## 4) Scenario D — Task status transition history (time-in-status)

```clojure
[:find ?created ?status-ident
 :in $ ?block-id
 :where
 [?h :logseq.property.history/block ?block-id]
 [?h :logseq.property.history/property ?p]
 [?p :db/ident :logseq.property/status]
 [?h :logseq.property.history/ref-value ?status]
 [?status :db/ident ?status-ident]
 [?h :block/created-at ?created]]
```

Use this to derive:

- Time spent in `DOING`
- Transition count (`TODO -> DOING -> DONE`)
- Last status change timestamp

Current source also exposes a higher-level helper for this scenario:

```clojure
(frontend.db.async/<task-spent-time graph block-id)
```

That helper reads property history, filters `:logseq.property/status`, and derives spent time from the ordered status transitions.

---

## 5) Scenario E — Latest change timestamp per property on a node

```clojure
[:find ?prop-ident (max ?created)
 :in $ ?block-id
 :where
 [?h :logseq.property.history/block ?block-id]
 [?h :logseq.property.history/property ?p]
 [?p :db/ident ?prop-ident]
 [?h :block/created-at ?created]]
```

Useful for compact node-level audit summaries.

---

## 6) Scenario F — Graph-wide property audit in a time window

```clojure
[:find ?block-id ?created ?ref ?scalar
 :in $ ?prop-ident ?start-ms ?end-ms
 :where
 [?h :logseq.property.history/property ?p]
 [?p :db/ident ?prop-ident]
 [?h :logseq.property.history/block ?block-id]
 [?h :block/created-at ?created]
 [(>= ?created ?start-ms)]
 [(<= ?created ?end-ms)]
 [(get-else $ ?h :logseq.property.history/ref-value nil) ?ref]
 [(get-else $ ?h :logseq.property.history/scalar-value nil) ?scalar]]
```

Use for:

- What-changed-today dashboards
- Migration validation checks
- Sync regression triage

---

## 7) Operational Notes / Caveats

- Property history is generated only for properties with `:logseq.property/enable-history? = true`.
- Built-in `:logseq.property/status` and `:logseq.property/priority` currently ship with `:logseq.property/enable-history? true`.
- Undo/redo stack is separate from persisted property-history entities.
- RTC block content versions and local property history are complementary.
- Property history entries are stored as first-class `property-history-block` entities with their own Malli schema.

---

## Findings: New Capabilities

- Property history now has an explicit Malli entity shape, including required timestamps and a validation rule that one of `:logseq.property.history/ref-value` or `:logseq.property.history/scalar-value` must be present.
- Graph export/import preserves property history entries, including scalar values, referenced-node values, and built-in property values.
- Deleting entities now retracts related property-history entries, including entries attached to deleted blocks, deleted pages used as ref-values, and views associated with deleted blocks.

---

## 8) Code References

- RTC block content version worker API registration: [src/main/frontend/worker/db_worker_node.cljs](../../../src/main/frontend/worker/db_worker_node.cljs)
- Dev menu invocation example: [src/main/frontend/components/content.cljs](../../../src/main/frontend/components/content.cljs)
- Property-history creation logic: [src/main/frontend/worker/commands.cljs](../../../src/main/frontend/worker/commands.cljs)
- Property-history query helpers: [src/main/frontend/db/async.cljs](../../../src/main/frontend/db/async.cljs)
- Property-history schema/properties: [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs)
- Property-history Malli entity schema: [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)
- Property-history graph export support: [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs)
- Property-history deletion cleanup: [deps/db/src/logseq/db/common/delete_blocks.cljs](../../../deps/db/src/logseq/db/common/delete_blocks.cljs)
- Property-history export/import test coverage: [deps/db/test/logseq/db/sqlite/export_test.cljs](../../../deps/db/test/logseq/db/sqlite/export_test.cljs)
- Property-history deletion cleanup tests: [deps/db/test/logseq/db/common/delete_blocks_test.cljs](../../../deps/db/test/logseq/db/common/delete_blocks_test.cljs)
- Schema docs: [dz/docs/03-features/database-graphs/schema-reference.md](../03-features/database-graphs/schema-reference.md)
