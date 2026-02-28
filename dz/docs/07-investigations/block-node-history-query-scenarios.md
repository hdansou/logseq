# Logseq Investigation — Block/Node History Query Scenarios

**Investigation Date:** February 19, 2026  
**Status:** Query reference for history-related debugging and feature design

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

Underlying websocket payload shape:

```clojure
{:action "query-block-content-versions"
 :block-uuids [block-uuid]
 :graph-uuid graph-uuid}
```

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
- Undo/redo stack is separate from persisted property-history entities.
- RTC block content versions and local property history are complementary.
- Current schema docs indicate property history storage exists before full end-user timeline UI.

---

## 8) Code References

- RTC block content version query API: `src/main/frontend/worker/rtc/core.cljs`
- Dev menu invocation example: `src/main/frontend/components/content.cljs`
- Property-history creation logic: `src/main/frontend/worker/commands.cljs`
- Property-history query helper: `src/main/frontend/db/async.cljs`
- Property-history schema/properties: `deps/db/src/logseq/db/frontend/property.cljs`
- Schema docs: `dz/docs/03-features/database-graphs/schema-reference.md`
