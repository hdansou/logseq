# Schema Changes & Data Integrity Investigation

> **Update 2026-10-04:** this investigation analyzed schema **65.33**. The schema is now **65.34** (`deps/db/src/logseq/db/frontend/schema.cljs:33`), which adds a repair migration, `db-order/missing-internal-page-parent-order-tx`, that gives nested pages with a parent but no `:block/order` a deterministic order (`68a68263b7`). Statements below that call 65.33 the current version describe the analyzed snapshot; nothing else here was re-verified.

## Part 3: Migration System Deep Dive

> **Last revised:** 2026-05-30

---

## Scope and Validation Sources

This document was validated against current implementation and tests in:

- [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs)
- [src/main/frontend/worker/db_core.cljs](../../../src/main/frontend/worker/db_core.cljs)
- [src/main/frontend/worker/db/validate.cljs](../../../src/main/frontend/worker/db/validate.cljs)
- [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs)
- [src/test/frontend/worker/migrate_test.cljs](../../../src/test/frontend/worker/migrate_test.cljs)

Search across [packages](../../../packages) found no DB migration implementation path.

---

## Migration Architecture (Current)

The DB graph migration system is centralized in [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs).

- Source of truth schema version is [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs) (`db-schema/version` is `65.33`).
- Stored graph schema version is `:logseq.kv/schema-version`.
- Migrations are selected by comparing stored version to target version with `db-schema/compare-schema-version`.
- The runtime runs migrations during DB init in [src/main/frontend/worker/db_core.cljs](../../../src/main/frontend/worker/db_core.cljs), except in `sync-download-graph?` flow.

If the client is older than graph schema, migration is blocked and an error notification is posted to the user.

---

## Migration Registry Shape

Current registry is a **vector of tuples**, not a map:

```clojure
[["65.7"  {:fix add-quick-add-page}]
 ["65.8"  {:fix add-missing-page-name}]
 ...
 ["65.33" {:properties [...]}]]
```

Supported migration keys in each update map:

- `:properties`
- `:classes`
- `:fix`
- `:delete-properties`

The executor is `upgrade-version!`, which builds tx-data and transacts with `:db-migrate? true` and `:skip-validate-db? true`.

---

## Active Migration Behavior

For each selected version update:

1. Parse version string into `{:major :minor}`.
2. Build new built-in properties from `db-property/built-in-properties`.
3. Build new classes from `db-class/built-in-classes` (with base classes included).
4. Run custom `:fix` function if present.
5. Apply `:delete-properties` retractions when configured.
6. Persist new `:logseq.kv/schema-version`.

After all versioned updates, `ensure-built-in-data-exists!` runs to reconcile/repair built-in entities and keep critical built-ins present.

---

## Findings: New Capabilities

Compared with the previously documented model, current code includes these additional capabilities:

1. `:delete-properties` migration type for cleanup of retired attrs (for example in 65.25).
2. Post-migration built-in reconciliation via `ensure-built-in-data-exists!`.
3. Structured migration result reporting (`:from-version`, `:to-version`, `:upgrade-result-coll`) used by DB init flow.
4. Explicit outdated-client guardrail with user notification instead of attempting downgrade behavior.
5. New migration coverage through 65.33, including comment system repairs and new built-in properties.

---

## Removed or Stale Claims from Previous Revision

> **Removed:** Claim that migration definitions live in `deps/db/src/logseq/db/frontend/migrate_65_7_through_65_16.cljs`. That file/path is not present in current source tree.

> **Removed:** Claim that registry shape is a map of version -> vector of typed operations. Current shape is ordered tuple vector `[version updates-map]`.

> **Removed:** Claim/documentation of `:rename-db-idents` as an active migration type in this system. Current migration executor supports `:properties`, `:classes`, `:fix`, and `:delete-properties`.

> **Removed:** Claim of a `migrate-if-needed` function with explicit `(conn current-version target-version)` signature in current implementation. Current public entrypoint is `migrate` with optional `:target-version`.

> **Removed:** Claim that migration test file is `src/test/frontend/worker/db/migrate_test.cljs`. Current tests are in [src/test/frontend/worker/migrate_test.cljs](../../../src/test/frontend/worker/migrate_test.cljs).

> **Removed:** Claim that manual migration/dev-tools trigger is a current standard trigger point. Current runtime trigger in app flow is DB initialization in [src/main/frontend/worker/db_core.cljs](../../../src/main/frontend/worker/db_core.cljs).

---

## Error Handling and Recovery (Current)

- Migration execution is wrapped in `try/catch` in `migrate`.
- On failure, code logs error context and rethrows.
- No whole-path rollback exists; completed transactions before the exception remain applied.
- Validation pipeline separately calls `ensure-built-in-data-exists!` in [src/main/frontend/worker/db/validate.cljs](../../../src/main/frontend/worker/db/validate.cljs), reinforcing built-in consistency.

Operational consequence remains the same: migration failures can leave a graph partially advanced to an intermediate schema version.

---

## Test Coverage Snapshot (Current)

Current tests in [src/test/frontend/worker/migrate_test.cljs](../../../src/test/frontend/worker/migrate_test.cljs) validate:

- preservation behavior of `ensure-built-in-data-exists!`
- delete-properties migration for 65.25
- property/class additions through later versions
- comment-related fixes in 65.28-65.32
- target version advance to 65.33

This reflects active migration behavior and is a better authority than historical examples from 65.7-65.16 docs.

---

## Current Takeaways

1. Migration system is still sequential and forward-only.
2. Registry and executor evolved beyond the older 65.16-era model.
3. Built-in reconciliation is now first-class in migration/validation flows.
4. Documentation should reference [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs) and [src/test/frontend/worker/migrate_test.cljs](../../../src/test/frontend/worker/migrate_test.cljs) as primary sources.
