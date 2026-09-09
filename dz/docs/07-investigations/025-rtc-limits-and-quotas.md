# RTC Limits and Quotas Investigation

> **Last revised:** 2026-09-08
> **Verified against:** `origin/master` @ `d2ab772` (2026-09-02)
> **Context:** a Pro tier is planned; product wants to know which limits already exist and which would need building.

## Question

What quotas and limits apply to DB sync (RTC)? The 100 MB asset limit is known — what about graph count per user, graph size, throttles, and batching?

## Answer

Three kinds of limit exist, and they are very unevenly enforced:

1. **Hard server limits** — asset size (100 MiB, **200 MiB on E2EE**), DO SQLite row size, semantic-API rate limits (120 reads / 20 writes per minute per user-operation-graph), semantic page size (≤200).
2. **Client-side caps** — 4 KiB block titles offloaded to assets, 1 MB snapshot frames, 5,000-datom upload requests, and one silent data drop: **whiteboard pages over ~1 MB are removed from the upload.**
3. **Account-level limits (graph count, storage, free-tier "notes only") exist only as UI strings and hard-coded defaults. Nothing enforces them.** The sync server has no plan concept, no graph-count check, and no storage accounting.

The full table is in [`ddocs/reference/features/sync-limits.md`](../ddocs/reference/features/sync-limits.md).

## Findings

### The server has no notion of a plan

`deps/db-sync` consumes these JWT claims and no others: `sub` (8 sites), `email`, `cognito:username`, `username`, `preferred_username`, `name`, `exp`, `email_verified`. There is no `plan`, `tier`, or `custom:` claim. Its complete set of `403` responses is `forbidden` (non-member) and `insufficient scope`. Graph creation is an unconditional `insert into graphs …` (`index.cljs:334`).

### Where the "1 graph / 50 MB / 10 graphs" numbers actually come from

| Source | Says |
|---|---|
| `src/resources/dicts/en.edn:37` | "1 synced graph (up to 50MB, notes only)" |
| `en.edn:52` | "10 synced graphs (up to 5GB each)" |
| `en.edn:61` | "Sync assets up to 100MB per file" |
| `components/settings.cljs:940` | `count-limit (if pro-account? 10 1)` |
| `settings.cljs:951` | `default-storage-limit (if pro-account? 10 0.05)` — **10 GB**, not the 5 GB the string claims |

`pro-account?` (`settings.cljs:993`) is `LemonStatus ∈ {active, on_trial, cancelled}` or `"pro"` in `UserGroups` — a billing state, never sent to the sync server.

### The usage panel is not wired

`settings.cljs:986` binds `graph-usage []` literally. `state/get-remote-graph-usage` (`state.cljs:864`) expects PascalCase `GraphStorageUsage`/`GraphStorageLimit` — the legacy file-sync API's shape — and nothing populates it. The panel therefore always shows 0 used of the hard-coded default.

### The 403 quota handlers are legacy file-sync

`handler/user.cljs:460-476` — `graph-count-exceed-limit?`, `storage-exceed-limit?`, and the `fire-file-sync-*` events — live inside the file-sync `RemoteAPI` type. `handler/db_based/sync.cljs:447` calls `:thread-api/db-sync-upload-graph` with no plan or count check before it.

### The one silent data loss

`worker/sync/upload.cljs:25,65`: values of `:logseq.property.tldraw/page` larger than the 1 MB snapshot frame are dropped from the upload. The drop is recorded in the return value and logged; no user-facing notification exists. A large whiteboard syncs as an empty one.

### Rate limiting is REST-only

`SEMANTIC_READ_RATE_LIMITER` (120/60 s) and `SEMANTIC_WRITE_RATE_LIMITER` (20/60 s) in `wrangler.toml` gate only `handle-semantic-request` — the `/api/v1/graphs/…` and `/semantic/…` routes, keyed `sub:operation-id:graph-id`. The sync WebSocket path (`handler/ws.cljs`, `handler/sync.cljs`) has no limiter and no message-size check in code.

### E2EE doubles the asset ceiling, and the client doesn't know

Server: `max-encrypted-asset-size` 200 MiB chosen at `assets.cljs:229` when `graph-e2ee?`. Client: `worker/sync/assets.cljs:19` rejects at 100 MiB unconditionally. The extra 100 MiB is unreachable through the app.

## What a Pro tier needs that does not exist

| Requirement | State today | Hook |
|---|---|---|
| Plan/tier visible to the sync server | absent — JWT has no plan claim | add a Cognito custom claim; read it in `worker/auth.cljs` |
| Graph-count quota | absent | check before `index.cljs:334`; reject with a new 403 code |
| Storage quota | absent — no accounting anywhere | needs a meter (DO SQLite size + R2 object sizes); none exists |
| "Notes only" on free | absent | plan check at `assets.cljs:229` (already tier-aware for E2EE) |
| Usage reporting to the client | dead code expecting the file-sync API | new endpoint + replace `graph-usage []` |
| Consistent Pro storage number | 5 GB (string) vs 10 GB (code) | decide, then fix `en.edn:52` or `settings.cljs:951` |
| Whiteboard drop → user notification | silent | surface `:dropped` from `drop-oversized-upload-datoms` |

## Open questions

- Is there an account service **outside this repo** that gates graph creation before the client calls `db-sync-upload-graph`? Nothing in the client suggests one.
- The exact DO SQLite blob threshold behind `snapshot row too large` (Cloudflare docs; not in repo).
- Whether the 200 MiB E2EE allowance is intentional headroom for encryption overhead or a leftover.

## Related

- [Sync Limits, Throttles, and Batch Sizes](../ddocs/reference/features/sync-limits.md)
- Drift check: `dz/support-engineering/script/sync-limits/`
- [024 — db-worker HTTP API as a UI Backend](024-db-worker-http-api-as-ui-backend.md)
