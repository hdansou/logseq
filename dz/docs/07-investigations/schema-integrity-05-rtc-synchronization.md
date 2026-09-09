# Schema Changes & Data Integrity Investigation

## Part 5: RTC Synchronization Analysis

> **Last revised:** 2026-05-30

---

## Scope

This document validates RTC/db-sync behavior against current source in `src/`, `deps/`, and `packages/`.

Primary implementation references:

- `src/main/frontend/handler/db_based/sync.cljs`
- `src/main/frontend/worker/sync.cljs`
- `src/main/frontend/worker/sync/apply_txs.cljs`
- `src/main/frontend/worker/sync/handle_message.cljs`
- `src/main/frontend/worker/sync/client_op.cljs`
- `src/main/frontend/worker/sync/upload.cljs`
- `src/main/frontend/worker/db/migrate.cljs`
- `deps/db-sync/src/logseq/db_sync/malli_schema.cljs`
- `deps/db-sync/src/logseq/db_sync/protocol.cljs`

`packages/` currently contains no RTC synchronization implementation in scope for this investigation.

---

## Current RTC Architecture (Validated)

RTC is implemented as DB sync over websocket + HTTP APIs (Cloudflare Durable Objects backend), not as client-managed schema branches.

High-level flow:

1. UI/handler layer starts/stops sync via thread APIs in `src/main/frontend/handler/db_based/sync.cljs`.
2. Worker entrypoints in `src/main/frontend/worker/db_core.cljs` dispatch to `frontend.worker.sync`.
3. `src/main/frontend/worker/sync.cljs` manages websocket lifecycle, reconnect, stale connection handling, and presence.
4. `src/main/frontend/worker/sync/handle_message.cljs` processes server messages (`hello`, `pull/ok`, `changed`, `tx/batch/ok`, `tx/reject`, etc.).
5. `src/main/frontend/worker/sync/apply_txs.cljs` applies remote txs, rebases local pending txs, and tracks sync conflicts.
6. `src/main/frontend/worker/sync/client_op.cljs` persists client sync state in SQLite (`client_ops`, `sync_conflicts`, `sync_meta`).

---

## Schema Version Behavior (Validated)

Schema migration is local DB migration logic (`src/main/frontend/worker/db/migrate.cljs`) driven by `schema-version->updates` as an ordered vector of `["<version>" {:migration ...}]` entries.

For remote graph creation/listing, schema version is exchanged as graph metadata:

- Graph create request includes optional `:schema-version` (`deps/db-sync/src/logseq/db_sync/malli_schema.cljs`).
- Graph list response includes optional `:schema-version` metadata (`deps/db-sync/src/logseq/db_sync/malli_schema.cljs`).

Client-side branch-per-major-version logic is not present in current RTC worker code paths.

---

## Conflict and Rebase Model (Validated)

Current synchronization conflict handling is tx-based and rebase-oriented:

1. Remote txs are sanitized and transacted (`transact-remote-txs!` in `src/main/frontend/worker/sync/apply_txs.cljs`).
2. If local pending txs exist, worker performs reverse -> apply remote -> rebase local.
3. Conflict extraction currently focuses on selected attrs (`sync-conflict-attrs` includes `:block/title`).
4. Conflicts are persisted in SQLite table `sync_conflicts` via `client-op/add-sync-conflicts!`.
5. UI receives `:sync-conflicts-updated` broadcasts with per-block conflict payload.

This is not a generic LWW/OT engine as previously documented; it is a targeted pending-tx rebase pipeline.

---

## Failure Recovery and Reliability (Validated)

### Message-level reject handling

`tx/reject` supports structured reasons including:

- `stale`
- `empty tx data`
- `invalid tx`
- `invalid t-before`
- `db transact failed`
- `snapshot upload in progress`

When rejected, client updates pending/failed tx state and may trigger pull recovery.

### Reconnect and stale websocket handling

`src/main/frontend/worker/sync.cljs` implements:

- Exponential backoff reconnect with jitter.
- WS stale watchdog (`ws-stale-kill-interval-ms`, `ws-stale-timeout-ms`) that closes stale sockets.
- Online-user + rtc-state broadcast updates on disconnect/reconnect.

### Snapshot upload/download path

`src/main/frontend/worker/sync/upload.cljs` provides snapshot upload with:

- Size-aware batching and optional gzip compression.
- Oversized datom filtering for selected attrs.
- Optional encryption flow for E2EE graphs.

Download/start flows are handled through `src/main/frontend/handler/db_based/sync.cljs` and worker thread APIs.

---

## Findings: Removed or Stale Claims

> **Removed:** References to non-existent files `src/main/frontend/worker/rtc/branch_graph.cljs` and `src/main/frontend/worker/rtc/migrate.cljs`.

> **Removed:** Branch decision matrix and helper functions such as `compare-schemas`, `create-branch`, `select-branch` in the client RTC flow.

> **Removed:** Migration-to-RTC-op conversion helpers such as `migration-results=>client-ops`, `property-migration=>ops`, `class-migration=>ops`, and related pseudo-code.

> **Removed:** Branch lifecycle and branch merge/pruning procedures in client runtime documentation.

> **Removed:** Generic LWW/OT conflict algorithm examples as authoritative RTC implementation details.

---

## Findings: New Capabilities

1. SQLite-backed client-op persistence with explicit schema initialization and indexes (`client_ops`, `sync_conflicts`, `sync_meta`) in `src/main/frontend/worker/sync/client_op.cljs`.
2. Structured sync conflict storage + retrieval + clear APIs exposed via thread APIs (`db-sync-get-block-conflicts`, `db-sync-clear-block-conflicts`) in `src/main/frontend/worker/db_core.cljs`.
3. Rebase pipeline that preserves pending local tx intent when remote txs arrive (`apply-remote-tx-with-local-changes!`) in `src/main/frontend/worker/sync/apply_txs.cljs`.
4. Protocol/schema coercion via Malli for websocket and HTTP messages in `deps/db-sync/src/logseq/db_sync/malli_schema.cljs` and `src/main/frontend/worker/sync/transport.cljs`.
5. Improved tx reject handling with per-tx metadata (`success-tx-ids`, `failed-tx-id`) and legacy-normalization support in `src/main/frontend/worker/sync/handle_message.cljs` and `src/main/frontend/worker/sync/transport.cljs`.
6. Stale websocket detection and auto-reconnect scheduling in `src/main/frontend/worker/sync.cljs`.
7. Snapshot upload hardening: payload splitting, optional gzip, oversized datom drop rules, and checksum-driven completion in `src/main/frontend/worker/sync/upload.cljs`.
8. Explicit graph readiness / operation gating in sync handler flows (`graph-ready-for-use?`, active upload/download operation checks) in `src/main/frontend/handler/db_based/sync.cljs`.

---

## Practical Takeaways for Schema/RTC Reviews

1. Treat schema migrations (`src/main/frontend/worker/db/migrate.cljs`) and RTC tx sync (`src/main/frontend/worker/sync/*.cljs`) as separate systems with integration points, not one branch-manager subsystem.
2. For RTC regressions, inspect pending tx persistence + `tx/reject` handling + rebase path first.
3. For collaboration conflicts, use sync-conflict APIs/events rather than assuming global branch divergence logic.
4. For compatibility issues, validate graph metadata (`schema-version`, `graph-ready-for-use?`, `graph-e2ee?`) and startup gating in handler + worker start flows.

---

## Evidence Notes

- Migration path: `migrate` and `upgrade-version!` in `src/main/frontend/worker/db/migrate.cljs`.
- Start/stop orchestration: `<rtc-start!`, `<rtc-stop!` in `src/main/frontend/handler/db_based/sync.cljs` and `start!`, `stop!` in `src/main/frontend/worker/sync.cljs`.
- WS protocol messages: `ws-client-message-schema`, `ws-server-message-schema` in `deps/db-sync/src/logseq/db_sync/malli_schema.cljs`.
- Conflict/rebase path: `remote-sync-conflicts`, `apply-remote-tx-with-local-changes!`, `apply-remote-txs!` in `src/main/frontend/worker/sync/apply_txs.cljs`.
