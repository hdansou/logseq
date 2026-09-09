# Logseq :result-transform Developer Notes (Source-Validated)

> **Last revised:** 2026-05-29

## Scope

This document was validated against current source under `src/`, `deps/`, and `packages/` on 2026-05-29. It focuses on behavior that is implemented today.

## Verified Behavior

### 1) Where transform runs in the pipeline

`custom-query-result-transform` in `src/main/frontend/db/query_react.cljs` performs result flattening, optional current-block removal, optional page enrichment for block-shaped results, then applies `:result-transform`.

`transform-query-result` in `src/main/frontend/components/query/result.cljs` applies that transform first, then optional child filtering, then optional grouping.

Execution order:

1. query returns raw result
2. `query-react/custom-query-result-transform` runs
3. `tree/filter-top-level-blocks` may run (file graph + `:remove-block-children?`)
4. grouping may run via `db-utils/group-by-page`

### 2) Supported transform forms

`result-transform-fn` can be:

1. a keyword that resolves via config key `:query/result-transforms`
2. an inline function form

Resolution is in `src/main/frontend/db/query_react.cljs` via:

```clojure
(if (keyword? result-transform-fn)
  (get-in (state/sub-config) [:query/result-transforms result-transform-fn])
  result-transform-fn)
```

### 3) Runtime evaluation model

Transforms are evaluated by SCI:

```clojure
(if-let [f (sci/eval-string (pr-str result-transform))]
  (sci/call-fn f result)
  result)
```

On runtime exception from transform execution, Logseq logs `:sci/call-error` and returns the original result.

### 4) Config schema

`src/main/frontend/schema/handler/common_config.cljc` validates:

```clojure
[:query/result-transforms [:map-of :keyword [:sequential any?]]]
```

### 5) File-graph vs DB-graph property naming

Template examples in `deps/common/resources/templates/config.edn` and graph fixture examples in `deps/graph-parser/test/resources/exporter-test-graph/logseq/config.edn` confirm mixed usage patterns:

1. file-style examples use `:block/*` properties
2. DB-oriented examples use `:logseq.property/*` for property values

In practice, transforms must target the keys present in the query result shape they receive.

### 6) Grouping behavior with transforms

`get-group-by-page` in `src/main/frontend/components/query/result.cljs` defaults to no grouping when a transform is present, unless `:group-by-page?` is explicitly set.

Table view and db graph paths short-circuit grouping to `false`.

### 7) Current regression coverage

`src/test/frontend/components/query/result_test.cljs` covers:

1. list and table behavior
2. transformed and non-transformed results
3. grouping interaction
4. current-block removal behavior
5. `:remove-block-children?`
6. propagation to config `:query-result` atom

## Findings: New Capabilities

### SCI helper bindings available to transform code

`src/main/frontend/extensions/sci.cljs` now injects additional helpers into the default `user` namespace used by `sci/eval-string`, including:

1. `sum`
2. `average`
3. `parseFloat`
4. `isNaN`
5. `log`
6. `pprint`
7. `call-api`

This broadens what authors can do inside `:result-transform` and `:view` functions compared with older docs that only discussed basic core forms.

## Removed or Corrected Claims

> **Removed:** Performance benchmark numbers (specific milliseconds/percentages) were removed because this document had no reproducible benchmark source in repo code or tests.

> **Removed:** Statements implying a shipped `:result-transform-debug?` feature were removed. No such runtime option exists in current source.

> **Removed:** The old footer metadata (`Document Version`, `Last Updated: 2025-10-17`, investigator branding, and fixed file-count claims) was removed because it was stale and not source-backed.

> **Removed:** Guidance to use `println` as the primary debugging technique in transforms was removed. Current SCI defaults explicitly expose `log` and `pprint`; docs should recommend available bindings from `frontend.extensions.sci`.

## Practical Guidance (Current)

1. Prefer keyword transforms for reusable logic via `:query/result-transforms`.
2. For one-off behavior, inline `(fn [result] ...)` is supported.
3. Expect transform to run before grouping.
4. When transform appears to no-op, inspect key namespaces in actual results (`:block/*` vs `:logseq.property/*`).
5. On `:sci/call-error`, Logseq falls back to original results, so failures can appear as silent no-ops in UI behavior.

## Key Source Files

1. `src/main/frontend/db/query_react.cljs`
2. `src/main/frontend/components/query/result.cljs`
3. `src/main/frontend/schema/handler/common_config.cljc`
4. `src/main/frontend/extensions/sci.cljs`
5. `src/test/frontend/components/query/result_test.cljs`
6. `deps/common/resources/templates/config.edn`
7. `deps/graph-parser/test/resources/exporter-test-graph/logseq/config.edn`
