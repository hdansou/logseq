# RTC/Sync Schema Change Considerations

> **Last revised:** 2026-05-30

**Document Type:** Process Guide
**Last Updated:** May 30, 2026
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

This guide covers schema changes that affect Logseq DB graph sync.

As of 2026-05-30, the active sync path is centered on:

- `src/main/frontend/worker/sync/`
- `src/main/frontend/worker/db/migrate.cljs`
- `deps/db-sync/src/logseq/db_sync/`
- `deps/db/src/logseq/db/frontend/`

Schema work now affects five distinct stages:

1. Built-in property and KV definitions
2. Local DB migration on graph open
3. Initial snapshot upload/download filtering
4. Ongoing tx upload/rebase/apply
5. Post-migration sanitization for deleted or transformed attrs

**Key Principle:** treat schema changes as a combined migration + sync compatibility change. Updating only the schema definition is not sufficient.

---

## RTC Architecture Overview

### Current Owning Files

| File                                                | Current responsibility                                                                                                                                        |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/main/frontend/worker/sync/apply_txs.cljs`      | Filters ignored attrs, persists pending local txs, rebases local work, applies pulled remote txs, tracks sync conflicts, offloads and rehydrates large titles |
| `src/main/frontend/worker/sync/handle_message.cljs` | WebSocket message handling, pull/reject flow, checksum verification, presence updates                                                                         |
| `src/main/frontend/worker/sync/upload.cljs`         | Remote graph creation and upload metadata, including schema-version sent to the server                                                                        |
| `src/main/frontend/worker/sync/download.cljs`       | Snapshot import ordering, including schema datoms before regular datoms                                                                                       |
| `src/main/frontend/worker/db/migrate.cljs`          | Local schema migration plan and built-in data repair                                                                                                          |
| `deps/db-sync/src/logseq/db_sync/tx_sanitize.cljs`  | Sanitizes remote tx-data, strips deleted attrs, fixes retract cases                                                                                           |
| `deps/db/src/logseq/db/frontend/property.cljs`      | Built-in property definitions and property-level RTC ignore flag                                                                                              |
| `deps/db/src/logseq/db/frontend/kv_entity.cljs`     | KV entities including schema-version and remote-schema-version metadata                                                                                       |
| `deps/db/src/logseq/db/frontend/schema.cljs`        | Schema version parsing, comparison, and current version constant                                                                                              |

> **Removed:** The old `src/main/frontend/worker/rtc/` directory referenced by earlier versions of this doc does not exist in the current source tree. References to `core.cljs`, `op_mem_layer.cljs`, `client_op.cljs`, `local_change.cljs`, and `ws.cljs` under that path are stale.

### Current Sync Flow

```text
Local DB tx
  -> remove ignored attrs
  -> normalize tx-data
  -> persist pending local tx entry
  -> upload tx batch
  -> receive remote tx batch / reject / pull messages
  -> sanitize remote tx-data
  -> reverse and rebase pending local txs when needed
  -> transact remote txs
  -> replay canonical outliner ops
  -> broadcast rtc state and sync-conflict updates
```

The important consequence for schema work is that a new or deleted attr can surface in both snapshot import and live tx rebase paths.

---

## Property RTC Configuration

### What Exists in the Current Source

For built-in properties, the current property-level RTC control is:

```clojure
{:rtc/ignore-attr-when-syncing true}
```

This is exposed through the `property-ignore-rtc` helper in `deps/db/src/logseq/db/frontend/property.cljs`, and the sync worker derives `ignore-attrs-when-syncing` from built-in property configs in `src/main/frontend/worker/sync/const.cljs`.

For KV entities, the current initial-upload control is:

```clojure
{:rtc/ignore-entity-when-init-upload true}
```

This is used in `deps/db/src/logseq/db/frontend/kv_entity.cljs` and consumed by `src/main/frontend/worker/sync/const.cljs`.

### What the Worker Actually Ignores

`src/main/frontend/worker/sync/apply_txs.cljs` builds its ignored set from:

- `rtc-const/ignore-attrs-when-syncing`
- `rtc-const/ignore-entities-when-init-upload`
- `:block/tx-id`
- `:block/pre-block?`

That means sync filtering is partly schema-driven and partly hard-coded for known internal attrs.

> **Removed:** The older three-key property RTC matrix (`:rtc/ignore-attr-when-init-upload`, `:rtc/ignore-attr-when-init-download`, `:rtc/ignore-attr-when-syncing`) is not present in the current property source. Do not add or document per-property init-upload or init-download flags unless the codebase grows first-class support for them.

### Practical Rule

- New built-in properties sync by default unless you explicitly mark them with `:rtc/ignore-attr-when-syncing true`.
- New KV entities can opt out of initial upload with `:rtc/ignore-entity-when-init-upload true`.

---

## Schema Version Synchronization

### Current Version Facts

The current frontend schema version is:

```clojure
(def version (parse-schema-version "65.34"))
```

Source of truth: `deps/db/src/logseq/db/frontend/schema.cljs`.

The current graph metadata still includes these KV entities:

- `:logseq.kv/schema-version`
- `:logseq.kv/graph-initial-schema-version`
- `:logseq.kv/remote-schema-version`

### What Actually Gates Compatibility

Current source shows major-version gating in user-facing sync flows:

- Remote repo merge prefers remote graphs whose `GraphSchemaVersion` matches the app major version in `src/main/frontend/handler/repo.cljs`.
- Remote graph download asserts app major version equals remote graph major version in `src/main/frontend/handler/events.cljs`.
- Local DB open runs `frontend.worker.db.migrate/migrate`; if the graph schema is newer than the app, the user gets an incompatibility notification instead of silent downgrade behavior.

### What Is Sent to the Sync Service

When creating a remote graph, `src/main/frontend/worker/sync/upload.cljs` sends the graph schema version as the graph's major version string, not the full `major.minor` value.

### Snapshot Import Detail That Matters for Schema Changes

`src/main/frontend/worker/sync/download.cljs` imports schema datoms before regular datoms. This keeps `:db/ident` and schema-related rows in place before ordinary content rows are replayed.

> **Removed:** Earlier draft guidance about a generic "remote more than 3 minor versions ahead" degraded mode is not backed by current source. The concrete checks in current code are major-version compatibility gates and normal migration handling.

---

## Conflict Resolution

### Current Model

The active implementation is rebase-oriented, not a generic per-property conflict framework.

`src/main/frontend/worker/sync/apply_txs.cljs`:

- persists normalized local tx entries
- reverses pending local txs when rebasing
- sanitizes and applies remote txs
- replays canonical outliner operations against the rebased DB

### User-Visible Conflict Tracking

Current code exposes a narrow user-visible conflict surface:

```clojure
(def sync-conflict-attrs
  #{:block/title})
```

Those conflicts are stored in the client-ops SQLite store and surfaced through:

- `client-op/get-sync-conflicts`
- `client-op/clear-sync-conflicts!`
- `:sync-conflicts-updated` broadcasts

This is materially different from a blanket "all properties are last-write-wins" claim.

### Sanitization That Protects Rebase

`deps/db-sync/src/logseq/db_sync/tx_sanitize.cljs` currently:

- strips migration-deleted attrs
- drops encrypted retracts that collapse into same-value add and retract pairs after decryption
- adds missing descendant retracts for retract-entity operations when needed

That sanitization is part of schema-change safety, especially after deleting attrs or changing encrypted or title behavior.

> **Removed:** The old `op_mem_layer.cljs` examples for generic datom conflict resolution and order-conflict handling are stale. That file no longer exists in the current sync path.

---

## Remote Update Processing

### Current Message Handling

`src/main/frontend/worker/sync/handle_message.cljs` owns WebSocket message handling for DB sync. Relevant behaviors include:

- requesting a pull when the server rejects a tx as stale
- verifying checksums in dev and test when local and remote tx positions match
- updating presence and online-user state

### Current Remote Tx Application

`src/main/frontend/worker/sync/apply_txs.cljs` applies pulled remote txs by:

1. resolving temp ids against the current DB
2. sanitizing tx-data through `logseq.db-sync.tx-sanitize/sanitize-tx`
3. transacting remote txs with `:transact-remote? true`
4. replaying canonical outliner ops such as `:save-block`, `:insert-blocks`, `:move-blocks`, `:delete-blocks`, `:create-page`, `:delete-page`, `:upsert-property`, `:restore-recycled`, and `:recycle-delete-permanently`

> **Removed:** The previously documented `apply-remote-update` multimethod and `should-sync-property?` helper were not found in the current implementation.

---

## Adding Or Changing Schema-Synced Fields

### Required Update Sequence

When a schema change touches data that can be synced, use this order:

1. Add or change the built-in definition in `deps/db/src/logseq/db/frontend/property.cljs` or `deps/db/src/logseq/db/frontend/kv_entity.cljs`.
2. If the graph schema changes, add an entry to `src/main/frontend/worker/db/migrate.cljs` and bump `deps/db/src/logseq/db/frontend/schema.cljs`.
3. If attrs are deleted, update both the migration and `deps/db-sync/src/logseq/db_sync/tx_sanitize.cljs` so pulled remote txs strip the removed attrs.
4. If the attr should be skipped during live sync, set `:rtc/ignore-attr-when-syncing true` on the built-in property.
5. Test both initial download or import and ongoing tx sync or rebase.

### Examples That Match Current Code

#### New built-in property that should sync normally

```clojure
{:logseq.property/example {:title "Example"
                           :schema {:type :string
                                    :public? true}}}
```

#### New built-in property that must stay local-only during live sync

```clojure
{:logseq.property/example-local {:title "Example local"
                                 :schema {:type :string
                                          :public? false}
                                 :rtc {:rtc/ignore-attr-when-syncing true}}}
```

#### New KV entity that should not be part of initial upload

```clojure
:logseq.kv/example-local-meta {:doc "Local-only sync metadata"
                               :rtc {:rtc/ignore-entity-when-init-upload true}}
```

> **Removed:** There is no current code path that requires adding a new property to a separate `rtc-synced-properties` registry under `worker/rtc/const.cljs`.

---

## Testing RTC Behavior

Use the existing sync-focused tests as the source of truth for behavior:

- `src/test/frontend/worker/db_sync_test.cljs`
- `src/test/frontend/worker/db_core_test.cljs`
- `src/test/frontend/worker/db_worker_test.cljs`

Target the slice you changed. Current coverage areas in those tests include:

- checksum mismatch logging
- large-title offload and rehydration
- sync conflict persistence and clearing
- built-in sync repair queueing
- snapshot import ordering

Recommended focused commands:

```bash
bb dev:test -v frontend.worker.db-sync-test
bb dev:test -v frontend.worker.db-core-test
bb dev:test -v frontend.worker.db-worker-test
```

If the schema change is broader, run:

```bash
bb dev:lint-and-test
```

---

## Common RTC and Schema Pitfalls

### 1. Adding a built-in property without a migration

If the property changes persisted graph schema, update `schema-version->updates` and bump `db-schema/version`.

### 2. Deleting attrs without tx sanitization

If you delete a synced attr, also add it to `migration-deleted-attrs` in `deps/db-sync/src/logseq/db_sync/tx_sanitize.cljs` so pulled remote txs do not reintroduce stale data.

### 3. Assuming every property needs an explicit RTC block

In the current code, built-in properties sync unless you opt them out with `:rtc/ignore-attr-when-syncing true`.

### 4. Documenting or editing dead `worker/rtc` paths

The active behavior is not implemented there anymore.

### 5. Forgetting initial download behavior

A schema change must work for both:

- snapshot download and import
- live tx upload, pull, and rebase

---

## Findings: New Capabilities

The current source contains several sync capabilities that were missing from the earlier version of this guide:

1. Large block titles can be offloaded to remote object storage and rehydrated through `:logseq.property.sync/large-title-object`.
2. Sync checksum mismatches are explicitly logged in dev and test via `:rtc.log/checksum-mismatch`.
3. Title conflicts are persisted per block and exposed through `get-sync-conflicts` and `clear-sync-conflicts!` rather than only being silently resolved.
4. Remote graphs opened after migration can queue built-in sync repair txs in `src/main/frontend/worker/db_core.cljs`.

---

## Best Practices

1. Treat schema work as a three-file minimum change: definition, migration, and sync sanitization or filtering.
2. Validate both graph-open migration and live sync behavior before concluding a schema change is safe.
3. Prefer documenting actual file paths and concrete behavior over speculative helper functions or pseudocode.
4. Avoid promising generic degraded-mode compatibility unless a real code path enforces it.
5. When in doubt, read the worker sync modules first; they are the current behavior surface.

---

## Quick Reference

| Concern                           | Source of truth                                    | Required action                                                  |
| --------------------------------- | -------------------------------------------------- | ---------------------------------------------------------------- |
| Current schema version            | `deps/db/src/logseq/db/frontend/schema.cljs`       | Bump when adding schema migration                                |
| Built-in property definitions     | `deps/db/src/logseq/db/frontend/property.cljs`     | Add property and optional `:rtc/ignore-attr-when-syncing`        |
| KV sync metadata                  | `deps/db/src/logseq/db/frontend/kv_entity.cljs`    | Add KV entity and optional `:rtc/ignore-entity-when-init-upload` |
| Local graph migration             | `src/main/frontend/worker/db/migrate.cljs`         | Add migration step                                               |
| Remote tx cleanup after deletions | `deps/db-sync/src/logseq/db_sync/tx_sanitize.cljs` | Add removed attrs to sanitizer                                   |
| Snapshot import ordering          | `src/main/frontend/worker/sync/download.cljs`      | Ensure schema rows still import first                            |
| Remote graph creation metadata    | `src/main/frontend/worker/sync/upload.cljs`        | Check schema-version payload semantics                           |
| Live tx rebase and apply          | `src/main/frontend/worker/sync/apply_txs.cljs`     | Verify new attrs survive local and remote rebasing               |

---

## Related Documentation

- [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)
- [Schema Migration Best Practices](./schema-migration-best-practices.md)
- [Validation Best Practices](./validation-best-practices.md)
- [Import/Export Best Practices](./import-export-best-practices.md)
- [Schema Change Testing](./schema-change-testing.md)
- [Schema Change PR Checklist](./schema-change-pr-checklist.md)

---

## Changelog

| Date       | Changes                                                                                                                                                                        |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2026-05-30 | Revalidated against current `src/`, `deps/`, and sync worker code. Replaced stale `worker/rtc` references, corrected RTC config guidance, and added current sync capabilities. |
| 2025-12-28 | Initial version based on investigation findings                                                                                                                                |
