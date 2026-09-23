# Logseq :result-transform Developer Notes (Source-Validated)

> **Last revised:** 2026-09-22
>
> Scope: `:result-transform` pipeline and result ordering re-verified against source at `39ade9a309`; the ordering behavior was traced in source and not observed in a running app.

## Scope

Every claim below cites a file and line that exists at `39ade9a309`. Sections 1, 2, 3 and 8 were rewritten on 2026-09-22 because the pipeline they described no longer exists.

## Verified Behavior

### 1) Where transform runs in the pipeline

The transform no longer runs on the main thread. It is serialized into the query resource key and evaluated in the worker.

1. `frontend.components.query/custom-query*` resolves a keyword `:result-transform` against `:query/result-transforms` in the repo config, and throws `"Missing query result transform"` when the keyword is not registered — `src/main/frontend/components/query.cljs:133-140`.
2. `frontend.components.query.result/query-spec` builds the resource key and attaches `:result-transform-edn (pr-str (:result-transform query))` — `src/main/frontend/components/query/result.cljs:46-47`.
3. `frontend.components.query.result/use-query-result` subscribes to `[:query <query-spec>]` and rethrows any worker error — `src/main/frontend/components/query/result.cljs:49-61`.
4. In the worker, `require-query-spec!` whitelists `:result-transform-edn` and requires it to be a string — `src/main/frontend/worker/handler/render_resource/query.cljs:62,95-98`.
5. `query-result-rows` runs the pipeline — `src/main/frontend/worker/handler/render_resource/query.cljs:202-209`.

Execution order inside `query-result-rows`:

1. `query-tuples` normalizes raw rows to tuples (`query.cljs:158-166`)
2. `filter-block-query-result` drops hidden blocks, the current block, and — unless `:remove-block-children?` is `false` — non-top-level blocks via `otree/filter-top-level-blocks` (`query.cljs:177-186`)
3. `apply-result-transform` evaluates and applies `:result-transform-edn` (`query.cljs:188-199`)
4. `normalize-query-row` / `normalize-query-cell` reduce every pulled block to its `:block/uuid` (`query.cljs:15-52`, applied at `query.cljs:209`)

Step 2 has no file-graph vs DB-graph branch; the previously documented claim that child pruning is skipped for DB graphs is false.

### 2) Result ordering from `:result-transform` is discarded for block results

For a user query block whose result is blocks and which has no `:view`, the order produced by `:result-transform` never reaches the screen. Filtering and `take` still work, because they change *which* rows exist, not their order.

The chain:

1. `normalize-query-cell` replaces every pulled block map with its bare `:block/uuid` — `src/main/frontend/worker/handler/render_resource/query.cljs:15-21`. The rows that leave the worker are uuids, carrying no property values.
2. `custom-query-inner` computes `only-blocks? (and (seq result) (every? uuid? result))` — `src/main/frontend/components/query.cljs:40`. When the result is all uuids, the query is not a built-in query, and no `:view` fn was supplied, it renders through `query-view/query-result` — `src/main/frontend/components/query.cljs:52-57`.
3. `query-view/query-result` hands the uuids to the standard table view as `:query-row-uuids` with `:view-feature-type :query-result` — `src/main/frontend/components/query/view.cljs:6-15`.
4. The worker view resource maps those uuids back to `:db/id`s and passes them as `:query-entity-ids` — `src/main/frontend/worker/handler/render_resource/view.cljs:419-431`.
5. `get-view-data` converts them into a **set** — `deps/db/src/logseq/db/common/view.cljs:1114` — and rebuilds entities by iterating that set (`view.cljs:1129-1132`). Set iteration order is not the caller's order.
6. Rows are then sorted by the view's own `:logseq.property.table/sorting`, defaulting to `[{:id :block/updated-at :asc? false}]` — `deps/db/src/logseq/db/common/view.cljs:1115-1118`, applied by `sort-entities` at `view.cljs:1215` (definition at `view.cljs:131-140`).

Ordering from `:result-transform` is preserved in exactly two cases:

- The query supplies a `:view` fn. `custom-query-inner` calls it with the transformed result before the table branch is reached — `src/main/frontend/components/query.cljs:43-50`.
- The query is a built-in query, which renders via `->hiccup` — `src/main/frontend/components/query.cljs:60-72`.

### 3) Runtime evaluation model

The worker evaluates the transform with plain `sci.core/eval-string`, with no options map:

```clojure
;; src/main/frontend/worker/handler/render_resource/query.cljs:188-199
(defn- apply-result-transform
  [rows result-transform-edn]
  (if result-transform-edn
    (let [transform (sci/eval-string result-transform-edn)]
      (when-not (fn? transform)
        (common/fail! "Query result transform is not a function"
               {:result-transform-edn result-transform-edn}))
      (let [result (transform rows)]
        (when-not (or (sequential? result) (set? result))
          (common/fail! "Query result transform must return rows"
                 {:result result}))
        result))
    rows))
```

Two fail-fast checks: the evaluated form must be a function, and its return value must be `sequential?` or a `set?`. `common/fail!` throws `ex-info` — `src/main/frontend/worker/handler/render_resource/common.cljs:8-10`.

There is no fallback to the untransformed result. The `ex-info` propagates out of the worker, is rethrown by `use-query-result` (`src/main/frontend/components/query/result.cljs:58-60`), and is caught by the `ui/catch-error` boundary in `custom-query`, which renders a block error — `src/main/frontend/components/query.cljs:182-183`.

### 4) SCI helper bindings are not available to `:result-transform`

`frontend.extensions.sci` injects `sum`, `average`, `parseFloat`, `isNaN`, `log`, `pprint` and `call-api` into the `user` namespace — `src/main/frontend/extensions/sci.cljs:29-46`.

The worker does **not** use that namespace. `frontend.worker.handler.render-resource.query` requires `[sci.core :as sci]` directly (`query.cljs:12`) and calls `sci/eval-string` with no bindings (`query.cljs:191`). Those helpers are therefore unavailable inside `:result-transform`. Only SCI's default `clojure.core` surface is in scope.

They remain available to `:view`, which is evaluated on the main thread through `frontend.extensions.sci/eval-string` — `src/main/frontend/components/query.cljs:7,149`.

`call-api` additionally dereferences `js/window.logseq` (`src/main/frontend/extensions/sci.cljs:25-27`), which does not exist in a worker context.

### 5) Supported transform forms

`:result-transform` can be:

1. a keyword that resolves via config key `:query/result-transforms`
2. an inline function form

Resolution is in `src/main/frontend/components/query.cljs:133-140`:

```clojure
result-transform (:result-transform q)
q (if (keyword? result-transform)
    (if-let [transform (get-in repo-config
                               [:query/result-transforms result-transform])]
      (assoc q :result-transform transform)
      (throw (ex-info "Missing query result transform"
                      {:result-transform result-transform})))
    q)
```

An unregistered keyword throws; it does not degrade to an identity transform.

### 6) Config schema

`src/main/frontend/schema/handler/common_config.cljc:37-39` validates:

```clojure
[:query/result-transforms [:map-of
                           :keyword
                           [:sequential any?]]]
```

### 7) File-graph vs DB-graph property naming

Template examples in `deps/common/resources/templates/config.edn:135-138` and graph fixture examples in `deps/graph-parser/test/resources/exporter-test-graph/logseq/config.edn:193-215` confirm mixed usage patterns:

1. file-style examples use `:block/*` properties
2. DB-oriented examples use `:logseq.property/*` for property values

Transforms must target the keys present in the query result shape they receive. DB graphs store block text in `:block/title` (`deps/db/src/logseq/db/frontend/schema.cljs:93`); `:block/content` is not in the DB schema.

### 8) Ordering, grouping and filtering come from view properties on the query block

For `:query-result` views, the view entity *is* the query block: `views/view` short-circuits view lookup and uses the passed `:view-uuid` directly — `src/main/frontend/components/views.cljs:4143-4151`; that uuid is the query block's uuid, set at `src/main/frontend/components/query/view.cljs:12`.

`loaded-view-aux` reads the ordering inputs off that entity — `src/main/frontend/components/views.cljs:3895,3902-3903`:

| Property | Shape | Definition |
| --- | --- | --- |
| `:logseq.property.table/sorting` | `[{:id <property-ident> :asc? <bool>} ...]` | `deps/db/src/logseq/db/frontend/property.cljs:512-517` |
| `:logseq.property.view/group-by-property` | a property ident | `deps/db/src/logseq/db/frontend/property.cljs:456-461` |
| `:logseq.property.table/filters` | `{:or? <bool> :filters [[<prop-ident> <operator> <value>] ...]}` | `deps/db/src/logseq/db/frontend/property.cljs:519-523` |

All three are `:hide? true`/`:public? false`, so they are set from the table UI rather than typed as visible properties. They can also be written from the CLI:

```sh
logseq upsert block -g <graph> --id <block-id> \
  --update-properties '{:logseq.property.view/group-by-property :logseq.property/status
                        :logseq.property.table/sorting [{:id :logseq.property/priority :asc? false}]}'
```

(Verified live against the `cliworker` and `plugin-test` graphs on 2026-09-22 by writing the properties and reading them back.)

### 9) Grouping behavior with transforms

`get-group-by-page` in `src/main/frontend/components/query/result.cljs:7-15` defaults to no grouping when a transform is present, unless `:group-by-page?` is explicitly set. Table view and DB graph paths short-circuit grouping to `false`.

### 10) Current regression coverage

`src/test/frontend/worker/handler/render_resource_test.cljs` covers the worker side:

1. `query-resource-applies-serialized-transform-and-top-level-filter-test` (line 2309) asserts that `"(fn [rows] (reverse rows))"` is applied and that the rows returned are uuids
2. the invalid-resource-key table (line 2425) asserts that a non-string `:result-transform-edn` is rejected

`src/test/frontend/components/query/result_test.cljs` covers the component side:

1. `custom-query-subscribes-to-serialized-worker-resources-test` (line 7) — `:result-transform` reaches the resource key as EDN
2. `use-query-result-shares-rows-after-render-test` (line 63)
3. `use-query-result-throws-worker-query-errors-test` (line 85)

No test asserts the final rendered row order for a transform-sorted block result.

## Removed or Corrected Claims

> **Corrected 2026-09-22:** The pipeline description naming `custom-query-result-transform` in `src/main/frontend/db/query_react.cljs` and `transform-query-result` in `src/main/frontend/components/query/result.cljs` was removed. `query_react.cljs` does not exist at `39ade9a309` and neither function name appears anywhere in `src/` or `deps/`. See section 1 for the current chain.

> **Corrected 2026-09-22:** "On runtime exception from transform execution, Logseq logs `:sci/call-error` and returns the original result." No `:sci/call-error` key exists in the tree. Transform failures now throw; see section 3.

> **Corrected 2026-09-22:** The SCI helper bindings (`sum`, `average`, `call-api`, …) were listed as available to `:result-transform`. They are available to `:view` only; see section 4.

> **Corrected 2026-09-22:** The regression-coverage list (list/table behavior, current-block removal, `:query-result` atom propagation) did not match `result_test.cljs` at `39ade9a309`; see section 10.

> **Removed:** Performance benchmark numbers (specific milliseconds/percentages) were removed because this document had no reproducible benchmark source in repo code or tests.

> **Removed:** Statements implying a shipped `:result-transform-debug?` feature were removed. No such runtime option exists in current source.

> **Removed:** The old footer metadata (`Document Version`, `Last Updated: 2025-10-17`, investigator branding, and fixed file-count claims) was removed because it was stale and not source-backed.

## Practical Guidance (Current)

1. Use `:result-transform` for filtering and truncation. Do not use it to order block results — the order is dropped (section 2).
2. To order a block result, set `:logseq.property.table/sorting` on the query block (section 8), or return non-block rows and order them inside a `:view` fn.
3. Prefer keyword transforms for reusable logic via `:query/result-transforms`; an unregistered keyword throws.
4. Assume only plain `clojure.core` inside a transform. No `sum`, `average`, `pprint`, `log` or `call-api`.
5. A transform that throws, is not a function, or returns a non-collection surfaces as a rendered query error, not as untransformed results.
6. Transform runs before grouping and before uuid normalization.

## Key Source Files

1. `src/main/frontend/worker/handler/render_resource/query.cljs`
2. `src/main/frontend/worker/handler/render_resource/view.cljs`
3. `src/main/frontend/components/query.cljs`
4. `src/main/frontend/components/query/result.cljs`
5. `src/main/frontend/components/query/view.cljs`
6. `src/main/frontend/components/views.cljs`
7. `deps/db/src/logseq/db/common/view.cljs`
8. `deps/db/src/logseq/db/frontend/property.cljs`
9. `src/main/frontend/schema/handler/common_config.cljc`
10. `src/main/frontend/extensions/sci.cljs`
11. `src/test/frontend/worker/handler/render_resource_test.cljs`
12. `src/test/frontend/components/query/result_test.cljs`
