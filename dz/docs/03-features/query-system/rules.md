# Query Rules (Current State)

> **Last revised:** 2026-05-29

## Scope

This page validates the current `:rules` behavior for Logseq query execution against source in `src/`, `deps/`, and `packages/`.

## Current Behavior

1. There is one shared rule registry for DB query execution:
   - `deps/db/src/logseq/db/frontend/rules.cljc`
   - Includes base rules (`rules`), DSL-facing rules (`db-query-dsl-rules`), dependency map (`rules-dependencies`), and extractor (`extract-rules`).

2. Advanced query path (`frontend.db.query-custom`) auto-detects rule names used in `:where` and injects rule clauses:
   - Parses query vector via `frontend.util.datalog/query-vec->map`.
   - Finds used rules via `frontend.util.datalog/find-rules-in-where`.
   - If `%` already exists in `:in` and the last `:inputs` value is a rules vector, merges extracted rules into that last input vector.
   - Otherwise injects `%` into `:in` and appends extracted rules to `:rules`.

3. DSL query path (`frontend.db.query-dsl`) returns both query clauses and concrete rule clauses:
   - Filters produce rule names (for example `:task`, `:priority`, `:tags`, `:page-ref`, `:property` variants).
   - `parse` resolves rule dependencies using `rules/extract-rules` with `rules/rules-dependencies`.
   - `query-wrapper` always builds `:in $ %` for DSL execution.

4. Query execution path (`frontend.db.query-react`) handles rules in two ways:
   - If explicit `rules` are provided, they are appended to inputs.
   - If query expects `%` but no rules are provided, it still appends an empty rules vector so Datascript arity remains correct.

5. CLI path (`logseq.cli.command.query`) supports `%`-based rules too:
   - During execution, if `:in` ends with `%`, CLI appends `(rules/extract-rules rules/db-query-dsl-rules)` to query inputs.

## Findings: Removed or Stale Items

> **Removed:** The old split model of "DB rules vs file-based rules" is no longer represented by separate rule files. There is no current `deps/db/src/logseq/db/file_based/rules.cljc`; rule definitions are centralized in `deps/db/src/logseq/db/frontend/rules.cljc`.

> **Removed:** The claim "DB graphs have 20+ rules, file graphs have 13 rules" is stale because the separate file-based rules registry is not present in current source.

> **Removed:** The prior `query_custom.cljs` pipeline shown in older docs (with `add-rules-to-query` signature `[query-m db-rules dsl-rules]` and explicit `:rules` vs legacy `:inputs` precedence wording) does not match current implementation.

> **Removed:** The global claim "or-join and not-join are not supported" is too broad and stale. Current code contains `or-join` usage in CLI built-in queries, and parser/test limitations are scoped to specific DSL patterns, not all query execution.

## Findings: New Capabilities

1. DSL tags filter with dedicated rule path:
   - `frontend.db.query-dsl/build-tags`
   - `:tags` and `tag-spec->tag` rules in `logseq.db.frontend.rules/db-query-dsl-rules`

2. Advanced query rule merge into existing `%` input:
   - `frontend.db.query-custom/add-rules-to-query` now detects when `%` input already exists and merges extracted rule clauses into that input vector.

3. CLI `%` auto-rule injection:
   - `logseq.cli.command.query/execute-query` appends extracted DSL rules automatically when query `:in` expects `%`.

## Known Limitations (Current, Scoped)

1. Some DSL boolean compositions still have edge cases.
   - `src/test/frontend/db/query_dsl_test.cljs` contains a skipped/fixme case noting a pattern that would require `or-join`/`not-join` style handling in the DSL layer.
   - This is a DSL construction limitation, not a blanket engine limitation.

## Source Evidence

- `src/main/frontend/db/query_custom.cljs`
- `src/main/frontend/db/query_dsl.cljs`
- `src/main/frontend/db/query_react.cljs`
- `src/main/frontend/util/datalog.cljc`
- `src/main/logseq/cli/command/query.cljs`
- `deps/db/src/logseq/db/frontend/rules.cljc`
- `src/test/frontend/db/query_dsl_test.cljs`
