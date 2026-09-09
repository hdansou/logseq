# Validation Best Practices

> **Last revised:** 2026-05-30

**Document Type:** Process Guide
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

This page is validated against the current Logseq source in `src/`, `deps/`, and `packages/` as of 2026-05-30.

For DB graphs, Logseq still uses a dual validation model:

- DataScript schema enforces low-level structural constraints such as refs, cardinality, uniqueness, and indexing.
- Malli schemas validate graph entities and property values against higher-level graph rules.
- Worker-side validation can also repair known invalid states when you explicitly run graph validation.

The most important current detail is that transaction-time validation is no longer best described as a post-commit audit. For normal DB-graph writes, Logseq computes a tentative `db-after` with `d/with`, validates that result, and only then commits it.

---

## Dual Validation Architecture

| Layer                   | Purpose                                               | Current source of truth                            |
| ----------------------- | ----------------------------------------------------- | -------------------------------------------------- |
| DataScript schema       | Structural constraints for DB attributes              | `deps/db/src/logseq/db/frontend/schema.cljs`       |
| Malli entity schemas    | Entity shape and graph rule validation                | `deps/db/src/logseq/db/frontend/malli_schema.cljs` |
| Malli execution helpers | Validate changed entities, full DBs, and local DBs    | `deps/db/src/logseq/db/frontend/validate.cljs`     |
| Worker repair flow      | Validate full graphs and apply built-in fixes         | `src/main/frontend/worker/db/validate.cljs`        |
| Transaction gate        | Runs validation before accepting most DB-graph writes | `deps/db/src/logseq/db.cljs`                       |

### Current transaction flow

For normal DB-graph writes through `logseq.db/transact!`:

1. Logseq builds a tentative transaction result with `d/with`.
2. It runs `validate-tx-report` against the resulting `db-after` and changed entities.
3. If validation passes, it updates the connection and runs callbacks.
4. If validation fails, it throws `ex-info` with debug data and does not commit the write.

Validation is skipped only for specific controlled paths such as initial DB setup, graph download/reset flows, or callers that explicitly set `:skip-validate-db?` in transaction metadata or on the connection.

> **Removed:** The older "Malli validation runs after transaction" description is stale. The current DB-graph write path validates the tentative `db-after` before committing it.

---

## DataScript Validation

DataScript schema remains the first line of defense for DB graphs. The base schema lives in `deps/db/src/logseq/db/frontend/schema.cljs` and currently includes the canonical structural attributes such as:

- `:db/ident` as a unique identity
- `:block/uuid` as a unique identity
- ref attributes like `:block/parent`, `:block/page`, `:block/refs`, and `:block/tags`
- file attributes such as `:file/path`, `:file/content`, and timestamps

Use DataScript schema when the rule is inherently structural:

- attribute type
- cardinality
- uniqueness
- index presence

Do not use DataScript schema for domain rules that depend on graph semantics, entity kind, or property type. Those belong in Malli validation.

---

## Malli Validation

Malli validation is now split across two files with clearer responsibilities:

- `deps/db/src/logseq/db/frontend/malli_schema.cljs` defines the DB-wide Malli schemas, entity dispatch, and property tuple validation.
- `deps/db/src/logseq/db/frontend/validate.cljs` compiles validators and explainers, then applies them to tx reports or whole DBs.

### Current public entry points

- `validate-tx-report`
  Validates only entities touched by a transaction.
- `validate-db`
  Validates all entities in a DB and returns grouped errors and graph counts.
- `validate-local-db!`
  Validates a local non-RTC DB with options tailored for CLI, importer, and tests.

`validate-local-db!` also supports `:open-schema` and `:verbose`, which makes it the current best entry point for offline verification workflows.

> **Removed:** `validate-db!` is not a current function in `deps/db/src/logseq/db/frontend/validate.cljs`. Use `validate-db` or `validate-local-db!` instead.

### How validation is enabled or skipped now

Transaction-time validation is not controlled by a global dynamic flag. The current gates live in `deps/db/src/logseq/db.cljs` and look at transaction metadata and connection state.

Relevant skip paths include:

- `:rtc-download-graph?`
- `:reset-conn!`
- `:initial-db?`
- `:skip-validate-db?` in tx metadata
- `:skip-validate-db?` on the connection
- `:logseq.graph-parser.exporter/new-graph?`

> **Removed:** The dynamic flag `*validate-db?*` described in older docs is no longer part of the current codebase.

---

## Entity Dispatch

Current entity dispatch is data-driven, not a `defmulti` over entity schemas. `deps/db/src/logseq/db/frontend/malli_schema.cljs` uses `entity-dispatch-key` to route an entity into the DB Malli schema.

Current dispatch keys include:

- `:reaction-entity`
- `:property`
- `:class`
- `:hidden`
- `:normal-page`
- `:asset-block`
- `:file-block`
- `:property-history-block`
- `:closed-value-block`
- `:property-value-block`
- `:property-value-placeholder`
- `:block`
- `:db-ident-key-value`

Order still matters. Property and class pages are more specific than generic pages, and specialized block types are checked before generic blocks.

> **Removed:** The older `defmulti entity-schema` examples no longer match the current implementation. The current code builds a Malli `[:multi ...]` schema named `Data` and dispatches through `entity-dispatch-key`.

---

## Property Value Validation

Property value validation is now centered on built-in type schemas in `deps/db/src/logseq/db/frontend/property/type.cljs` plus helper logic in `deps/db/src/logseq/db/frontend/malli_schema.cljs`.

The current built-in validation schema map covers these shipped property types:

- `:default`
- `:number`
- `:date`
- `:datetime`
- `:checkbox`
- `:url`
- `:node`
- `:asset`
- internal validation-backed types such as `:entity`, `:class`, `:property`, `:page`, `:keyword`, `:map`, `:coll`, and `:any`

`validate-property-value` now does more than simple type checks:

- adapts validation based on whether a property type needs DB access
- respects property cardinality
- allows the empty-placeholder sentinel when appropriate
- can enforce closed values when `*closed-values-validate?*` is bound to `true`

> **Removed:** Older examples based on `defmulti property-value-schema` no longer reflect the current structure. Property-type validation is now assembled from `built-in-validation-schemas` and composed into the DB Malli schema.

---

## Error Handling And Repair

There are now two materially different validation modes:

### Transaction-time rejection

`deps/db/src/logseq/db.cljs` rejects invalid DB-graph writes by throwing `ex-info` with debug data when `validate-tx-report` fails.

Use this path to prevent new invalid data from entering the graph.

### Full-graph validation with optional repair

`src/main/frontend/worker/db/validate.cljs` runs a full-graph validation pass and, by default, attempts to repair known invalid states before reporting final errors.

Current worker-side pre-fixes include:

- `fix-extends-cardinality!`
- `fix-icon-wrong-type!`
- `db-migrate/ensure-built-in-data-exists!`
- `fix-non-closed-values!`
- `fix-num-prefix-db-idents!`

If errors remain, the worker loops through `fix-invalid-blocks!` until no more built-in fixes apply.

Tests in `src/test/frontend/worker/db_validate_test.cljs` verify current repair behavior, including:

- filling missing block UUIDs
- repairing invalid pages, properties, and classes

> **Removed:** The old guide placed `fix-invalid-blocks!` under `deps/db/src/logseq/db/frontend/validate.cljs`. Current repair logic lives in `src/main/frontend/worker/db/validate.cljs`.

---

## Performance Considerations

The current source supports these practical performance guidelines:

- Prefer transaction-time validation for normal interactive writes because it only validates changed entities.
- Use `validate-local-db!` or worker-side `validate-db` for offline verification, import validation, export validation, or test assertions.
- Skip validation only in tightly controlled flows that already set explicit transaction metadata or connection flags.
- Revalidate after migrations, imports, or repair passes instead of relying on speculative performance shortcuts.

> **Removed:** The previous millisecond and minute benchmark tables were removed. They were not backed by source or test artifacts in the current repository.

---

## Testing Validation

Current validation behavior is exercised from multiple sides of the codebase:

- `src/test/frontend/worker/db_validate_test.cljs` covers worker-side repair and validation loops.
- `deps/db/test/logseq/db/sqlite/create_graph_test.cljs` and `deps/db/test/logseq/db/sqlite/export_test.cljs` use `validate-local-db!` for DB integrity assertions.
- `deps/graph-parser/test/logseq/graph_parser/exporter_test.cljs` validates imported/exported graphs with `validate-local-db!`.
- `src/test/frontend/worker/db_sync_test.cljs` uses `validate-local-db!` in sync-heavy scenarios.

When adding or changing validation logic, prefer tests that cover both:

- transaction-time rejection for newly introduced invalid writes
- offline or worker-side full-graph validation for existing or migrated data

---

## Findings: New Capabilities

These capabilities were missing or underspecified in the previous version of this document:

1. `validate-local-db!` now provides a dedicated local/offline validation path with `:open-schema` and `:verbose` options for CLI, importer, and tests.
2. Worker-side graph validation can actively repair known invalid data, not just report it.
3. The CLI exposes that repair flow through `logseq graph validate --fix`.
4. The worker thread exposes `:thread-api/validate-db`, and frontend developer flows route through that API rather than calling low-level validation helpers directly.

---

## Best Practices Summary

- Put structural rules in DataScript schema and semantic graph rules in Malli.
- Treat `malli_schema.cljs` as the source of truth for entity and property-value validation shape.
- Treat `validate.cljs` as the execution layer for tx-report, full-DB, and local-DB checks.
- Use transaction metadata such as `:skip-validate-db?` for controlled bypasses instead of introducing new global switches.
- Use worker or CLI validation when you need repair behavior or whole-graph verification.
- Add tests for both rejection and repair when changing validation rules.

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
