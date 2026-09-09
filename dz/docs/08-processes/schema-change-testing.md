# Schema Change Testing Strategies

> **Last revised:** 2026-05-30

**Document Type:** Testing Guide
**Last Updated:** May 30, 2026
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

Schema changes in the current Logseq codebase are tested across three concrete layers:

1. Datascript schema and built-in definitions in `deps/db/`
2. Frontend migration and validation flows in `src/main/frontend/worker/db/`
3. End-to-end import/export and sync-oriented behavior in `deps/db/test/` and `src/test/`

The main rule is still correct: test upgrade paths, not just the final shape. What changed since the previous revision is the actual implementation surface. The codebase now exposes a narrower, more concrete set of migration and validation entry points than this document previously described.

---

## Current Source Of Truth

For schema change work, validate against these current anchors:

- Current DB schema version: `logseq.db.frontend.schema/version` in `deps/db/src/logseq/db/frontend/schema.cljs`.
- Current frontend migration registry: `frontend.worker.db.migrate/schema-version->updates` in `src/main/frontend/worker/db/migrate.cljs`.
- Migration entry point: `frontend.worker.db.migrate/migrate`.
- Built-in graph bootstrap data: `logseq.db.sqlite.create-graph/build-db-initial-data` in `deps/db/src/logseq/db/sqlite/create_graph.cljs`.
- Schema repair helper: `frontend.worker.db.fix/check-and-fix-schema!`.
- Transaction validation path: `logseq.db/transact!` and transact-time validation handling in `deps/db/src/logseq/db.cljs`.
- Property value validation logic: `logseq.db.frontend.malli-schema/validate-property-value` in `deps/db/src/logseq/db/frontend/malli_schema.cljs`.
- DB-sync message schemas: `deps/db-sync/src/logseq/db_sync/malli_schema.cljs`.
- Import/export round-trip tests: `deps/db/test/logseq/db/sqlite/export_test.cljs`.
- Frontend migration tests: `src/test/frontend/worker/migrate_test.cljs`.
- Frontend validation tests: `src/test/frontend/worker/db_validate_test.cljs` and `src/test/frontend/worker/db_worker_test.cljs`.

As of 2026-05-30, the current schema version is `65.33`.

---

## Findings: Removed Or Stale Claims

> **Removed:** The previous revision treated schema versions in tests as string literals like `"65.16"` and `"65.17"` as if that were the current testing interface. Current migrations store and compare schema versions as maps such as `{:major 65 :minor 33}` in the database, while `schema-version->updates` still uses string literals as migration table keys.
> **Removed:** The example helpers `get-schema-for-version`, `create-test-db-with-schema`, `create-test-db-with-data`, `generate-test-data!`, `get-current-schema-version`, and `migrate-db-graph` are not current public helpers in `src/`, `deps/`, or `packages/`.
> **Removed:** The document previously referenced `validate-db!` and `validate-tx-report` as simple standalone helpers available at the guide level. Current validation is centered around the transact pipeline in `deps/db/src/logseq/db.cljs`, worker validation entry points, and dedicated validation tests rather than the helper surface shown in the old examples.
> **Removed:** The old RTC examples assumed a generic mock server/device API and broad property RTC config keys such as `:rtc/ignore-attr-when-init-upload` and `:rtc/ignore-attr-when-init-download`. Current built-in property definitions verified in `deps/db/src/logseq/db/frontend/property.cljs` only clearly show `:rtc/ignore-attr-when-syncing` on the relevant helper path used by those properties.
> **Removed:** The previous import/export examples used generic `export-db` and `import-db` functions. Current source-of-truth coverage for schema-sensitive import/export behavior is in the SQLite export/import pipeline and its tests, especially `deps/db/test/logseq/db/sqlite/export_test.cljs`.
> **Removed:** The sample CI section referenced commands such as `bb test:unit`, `bb test:migration`, `bb test:validation`, `bb test:import-export`, and `bb lint:schema-version`. No current evidence for those repo commands was found. The validated repo-level commands are `bb dev:lint-and-test` and focused unit runs via `bb dev:test -v ...`.

---

## Findings: New Capabilities

- Frontend migrations now cover schema updates through `65.33`, including additive property/class migrations and destructive cleanup via `:delete-properties` entries in `schema-version->updates`.
- `frontend.worker.db.migrate/ensure-built-in-data-exists!` explicitly preserves graph bootstrap keys like `:logseq.kv/graph-initial-schema-version` and `:logseq.kv/graph-created-at` while backfilling missing built-in data.
- Import/export round-trip coverage is broader than this guide previously described. Current tests cover timestamps, excluded namespaces, property history preservation, class property ordering, graph file ordering, asset handling, and the rule that `:logseq.kv/schema-version` is intentionally not copied from export back into the target graph.
- Worker validation coverage now includes sync diagnostics plumbing. `thread-api/validate-db` tests verify that validation receives sync context rather than operating as a blind local-only check.
- `logseq.db/batch-transact-with-temp-conn!` explicitly defers validation during inner batch operations and validates on the final transact, which matters when testing larger schema-changing workflows.

---

## Testing Philosophy

Use a narrower testing pyramid than the old guide suggested:

1. Migration unit tests for each schema bump in `src/test/frontend/worker/migrate_test.cljs`
2. Validation tests for invalid or repaired DB state in `src/test/frontend/worker/db_validate_test.cljs` and worker thread API tests
3. Import/export round-trip tests in `deps/db/test/logseq/db/sqlite/export_test.cljs`
4. DB-sync schema and diagnostics checks when the change affects sync payloads or checksum behavior
5. Manual graph-open and cross-device checks only when the change affects user-visible migration behavior

Prefer adding one focused test per concrete behavior change instead of generic helper-heavy test scaffolding.

---

## Test Environment Setup

For current schema work, prefer these real setup patterns over bespoke helpers.

### Create a fresh current-schema conn

```clojure
(let [conn (d/create-conn db-schema/schema)]
  ...)
```

### Seed an explicit graph schema version before migration

```clojure
(d/transact! conn [{:db/ident :logseq.kv/schema-version
                    :kv/value {:major 65 :minor 32}}])

(db-migrate/migrate conn :target-version {:major 65 :minor 33})
```

### Build a fresh graph with bootstrap key-values

```clojure
(sqlite-create-graph/build-db-initial-data "")
```

This path initializes both `:logseq.kv/schema-version` and `:logseq.kv/graph-initial-schema-version` with the current schema version.

---

## Migration Testing

Current migration tests are version-specific and should stay that way.

Verified current examples already exist for:

- deleting legacy properties in the `65.25` upgrade
- adding the repeat-type property
- adding comment-related properties and classes
- tagging existing comment blocks during migration
- backfilling comment targets
- adding assignee and agent-session-id properties
- repairing comment class inheritance
- adding gallery view properties in `65.33`

### Recommended pattern

```clojure
(let [conn (d/create-conn db-schema/schema)]
  (d/transact! conn [{:db/ident :logseq.kv/schema-version
                      :kv/value {:major 65 :minor 32}}])

  (let [result (db-migrate/migrate conn :target-version {:major 65 :minor 33})]
    (is (= {:major 65 :minor 33}
           (:kv/value (d/entity @conn :logseq.kv/schema-version))))
    (is (seq (:upgrade-result-coll result)))))
```

Test these properties for each migration entry you add:

- the graph version changes to the expected target
- the new property or class exists after migration, if the migration adds one
- deleted properties are actually removed from both schema usage and property entities, if the migration removes them
- fix functions preserve unrelated user data
- `ensure-built-in-data-exists!` does not clobber graph bootstrap metadata

Add multi-hop migration coverage when a new migration depends on side effects from earlier migrations. Otherwise, a focused test for the new target version is the cheapest reliable check.

---

## Validation Testing

Current validation behavior is tied to transaction flow and worker validation entry points, not just ad hoc helper calls.

Validate these scenarios:

- invalid data should fail during transact unless the path explicitly uses `:skip-validate-db? true`
- repair paths should be covered by dedicated validation tests
- worker validation should preserve sync diagnostics context when invoked from the thread API
- schema repair helpers should only normalize schema mismatches, not mutate unrelated data

Recommended focus areas:

- `src/test/frontend/worker/db_validate_test.cljs` for DB repair and validation outcomes
- `src/test/frontend/worker/db_worker_test.cljs` for thread API validation and diagnostics behavior
- `deps/db/src/logseq/db.cljs` when the schema change alters transact-time invariants

> **Removed:** The previous guide claimed a stable `fix-invalid-blocks!` flow. The currently verified repair path is centered on worker validation tests and `frontend.worker.db.fix/check-and-fix-schema!`, not that named helper.

---

## Property And Schema Definition Testing

When adding or changing a built-in property:

1. Update the built-in property definition in `deps/db/src/logseq/db/frontend/property.cljs`.
2. If the change is schema-visible for existing graphs, add a migration entry in `src/main/frontend/worker/db/migrate.cljs`.
3. Test the property's post-migration existence and core shape.
4. If the property uses closed values, defaults, or special validation, add focused assertions around those behaviors.

Useful checks include:

- `:db/ident`
- `:logseq.property/type`
- `:db/cardinality`
- `:logseq.property/public?`
- `:logseq.property/hide?`
- default closed values and default value entities

For property value validation rules, the closest source of truth is `logseq.db.frontend.malli-schema/validate-property-value`.

---

## Import/Export Testing

The current codebase has strong evidence for schema-sensitive round-trip testing in `deps/db/test/logseq/db/sqlite/export_test.cljs`.

Use that suite as the model for schema changes that affect persisted graph shape.

Current verified round-trip coverage includes:

- full graph import/export
- timestamp-preserving import/export
- excluded namespace handling
- property history preservation
- class property ordering preservation
- graph file ordering preservation
- mixed property-value cases
- assets

Two current rules are especially important for schema work:

1. Export data includes graph content, ontology, files, key-values, and property history.
2. `:logseq.kv/schema-version` is intentionally excluded from exported KV restoration, because the importing side sets the target graph schema itself.

> **Removed:** The old guide asserted a fixed exported EDN shape with top-level keys like `:blocks`, `:properties`, and `:classes`. Current export shape is richer and is built through the SQLite export pipeline rather than that simplified example contract.

---

## RTC And Sync Testing

Schema changes only need sync-specific tests when they affect:

- checksum inputs
- transmitted datom ordering
- validation diagnostics
- DB-sync message schema
- migration behavior during sync/download flows

Current validated anchors include:

- DB-sync Malli message schemas in `deps/db-sync/src/logseq/db_sync/malli_schema.cljs`
- import ordering and schema-before-data tests in `src/test/frontend/worker/db_worker_test.cljs`
- validation diagnostics plumbing in `thread-api-validate-db-passes-sync-diagnostics-test`

Prefer these checks over generic mock device simulations:

- schema datoms are imported before dependent data datoms
- validation entry points receive sync diagnostics context
- checksum-related behavior remains stable when schema-visible attributes change

> **Removed:** The previous mock RTC server/device examples are not representative of the current tested sync surface.

---

## Performance Testing

No current repository-local performance command or benchmark harness specifically for schema changes was verified in `src/`, `deps/`, or `packages/`.

Treat performance testing as targeted follow-up work when a migration:

- touches large numbers of entities
- rewrites existing attribute values
- changes import/export payload volume
- changes sync ordering or checksum inputs

When needed, add a focused long-running test near the owning migration or export suite instead of inventing a generic benchmark layer.

---

## Integration Testing

For current code, a practical integration slice is:

1. seed a graph at an older schema version
2. run `frontend.worker.db.migrate/migrate`
3. verify the new property/class/deletion behavior
4. export and import through the SQLite export/import test path when persistence shape changed
5. validate worker diagnostics if sync code is involved

This keeps the test near the real persistence and migration code instead of depending on synthetic helpers.

---

## Manual Testing

Use manual checks only for behaviors automated tests cannot cheaply cover.

### Manual checklist for schema changes

- Open a graph created before the new schema version and verify migration completes without user-facing errors.
- Confirm the expected new property/class behavior in the UI if the schema change is user-visible.
- Re-open the graph to confirm bootstrap data and migration side effects are stable.
- If the change affects exported graph shape, run an export/import round-trip manually in addition to the automated SQLite tests.
- If the change touches sync-sensitive attributes, verify at least one real sync/download flow in a development environment.

---

## Test Data Strategy

Prefer checked-in fixtures and narrowly seeded transactions over large ad hoc generators.

Current examples already in use:

- migration fixture transit DBs in `src/test/migration/`
- hand-built transactions in `src/test/frontend/worker/migrate_test.cljs`
- round-trip graph builders in `deps/db/test/logseq/db/sqlite/export_test.cljs`

This is a better fit for Logseq's schema work than the older generic graph generator approach because it keeps each test anchored to one known migration or one known export invariant.

---

## CI And Command Guidance

Validated current commands:

- `bb dev:lint-and-test` for the main lint and unit-test pass
- `bb dev:test -v <namespace/test-name>` for a focused unit test

For schema changes, start with the narrowest relevant test namespace or test var, then run the broader main test pass if the focused check succeeds.

> **Removed:** No current source evidence was found for a dedicated `schema-tests.yml` workflow or the `bb test:*` commands shown in the previous version of this guide.

---

## Quick Reference

For each schema change PR, verify the applicable items below.

### Always required

- Add or update the built-in property/class definition if the schema surface changed.
- Add a `schema-version->updates` entry when an existing graph needs migration.
- Add a focused migration test in `src/test/frontend/worker/migrate_test.cljs` or the nearest owning test namespace.
- Verify the stored graph schema version after migration.

### Required when validation behavior changes

- Add or update validation tests.
- Verify transact-time validation or worker validation behavior.
- Verify schema repair behavior if the change introduces a repair path.

### Required when persisted graph shape changes

- Add or update SQLite export/import round-trip coverage.
- Verify `:logseq.kv/schema-version` handling is still correct on import.
- Verify files, property history, or ordering semantics if your change touches those surfaces.

### Required when sync behavior changes

- Verify import ordering, checksum, or diagnostics-related tests.
- Update DB-sync schema tests if message payload shape changes.

---

## Related Documentation

- [Investigation Overview](../07-investigations/schema-evolution-and-data-migration.md)
- [Schema Migration Best Practices](./schema-migration-best-practices.md)
- [Validation Guidelines](./validation-best-practices.md)
- [Import/Export Best Practices](./import-export-best-practices.md)
