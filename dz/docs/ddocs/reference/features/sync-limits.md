---
title: 'Sync Limits, Throttles, and Batch Sizes'
description: 'Every limit, quota, rate limit, timeout, and batch size in Logseq DB sync (RTC), with what is enforced where — and what is only claimed'
type: reference
status: active-development
lastVerified: '2026-09-09'
reviewStatus: authored-from-source
logseqVersion: '2.0.x'
platforms: [desktop, mobile]
graphType: [db]
relatedDocs:
  tutorials:
    - '[Real-Time Sync](../../tutorials/collaboration/real-time-sync.md)'
  explanation:
    - '[RTC vs File Sync](../../explanation/comparisons/rtc-vs-file-sync.md)'
  reference:
    - '[db-worker HTTP API](../api/db-worker-http-api.md)'
    - '[Assets: Link Syntax, File Types, and Sync](asset-sync.md)'
---

# Sync Limits, Throttles, and Batch Sizes

> Every numeric limit in the DB sync stack — client (`src/main/frontend/worker/sync`) and server (`deps/db-sync`) — with the file and line it comes from. Verified against `origin/master` @ `be800f1711` (2026-09-08); all 42 declared values re-checked by the drift gate on 2026-09-09. A drift check for these numbers lives at `dz/support-engineering/script/sync-limits/`.

The single most important fact on this page: **there is no server-side graph-count or storage quota.** The "1 graph / 50 MB free, 10 graphs pro" figures exist only as UI strings and hard-coded defaults; nothing in `deps/db-sync` enforces them. See [Account-level limits](#account-level-limits--claimed-not-enforced).

## Hard limits — the server rejects the request

| Limit | Value | Source | Response |
|---|---|---|---|
| Asset file size | **100 MiB** (104,857,600 B) | `deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs:9` `max-asset-size` | `413 asset too large` |
| Asset file size, **E2EE graph** | **200 MiB** (209,715,200 B) | `assets.cljs:10` `max-encrypted-asset-size`; selected at `:229` when `graph-e2ee?` | `413 asset too large` |
| Snapshot row size | Durable Object SQLite blob limit — server matches `SQLITE_TOOBIG`, `string or blob too big`, `statement too long` | `handler/sync.cljs:799` `sqlite-too-big-error?` | `413 snapshot row too large` |
| Semantic API page size | **max 200**, default 50 | `handler/semantic.cljs:25-26` | `400 invalid limit` |
| Graph not ready for use | — (snapshot upload in progress) | `handler/sync.cljs` | `409 graph not ready` |
| Semantic API on E2EE graph | disallowed | `dispatch.cljs:97` | `409 semantic-api-unavailable-for-e2ee` |
| Worker CPU per invocation | 300,000 ms | `deps/db-sync/worker/wrangler.toml [limits] cpu_ms` | platform |

These are size limits only — there is no file-type restriction anywhere in the sync path; see [Assets: Link Syntax, File Types, and Sync](asset-sync.md).

The client also checks the 100 MiB asset ceiling before uploading (`src/main/frontend/worker/sync/assets.cljs:19`) and logs `:rtc.asset.log/asset-too-large` instead of sending. Note the client does **not** know about the 200 MiB E2EE allowance — it rejects at 100 MiB regardless.

## Client-side caps — these shape what gets uploaded

| Limit | Value | Source | Behaviour when exceeded |
|---|---|---|---|
| Block title | **4,096 bytes** (UTF-8) | `worker/sync/large_title.cljs:8` `large-title-byte-limit` | Title is uploaded as a `.txt` asset and the datom carries a `:logseq.property.sync/large-title-object` reference instead |
| Snapshot upload frame | **1,000,000 bytes** | `worker/sync/upload.cljs:23` `snapshot-upload-max-bytes` (4-byte frame header, `:24`) | Rows are bin-packed into ≤1 MB frames; one row larger than a frame fails fast with `:db-sync/snapshot-row-too-large` |
| **Whiteboard page data** | **~1 MB** (frame limit minus header) | `upload.cljs:25` `ignored-oversized-upload-attrs #{:logseq.property.tldraw/page}`; `:65 drop-oversized-upload-datoms` | **The datom is dropped from the snapshot.** The whiteboard syncs as empty. No error is raised; the drop is returned in `:dropped` and logged only. |
| Upload request | **5,000 datoms** | `worker/sync/apply_txs.cljs:47` `max-upload-request-datoms` | Larger transactions are split into sequential requests |

The whiteboard rule is the only limit in the stack that discards data rather than rejecting it. It exists because tldraw stores an entire board as one property value.

## Account-level limits — claimed, not enforced

These numbers appear in the product but are **not checked by the sync server**. `deps/db-sync` reads only `sub`, `email`, `username`, `exp` from the JWT — no plan or tier claim — and its only `403`s are `forbidden` (not a member) and `insufficient scope`. Graph creation (`deps/db-sync/src/logseq/db_sync/index.cljs:334`) is an unconditional insert.

| Claim | Value | Where it lives | Enforced? |
|---|---|---|---|
| Free: synced graphs | 1 | `src/resources/dicts/en.edn:37`; `components/settings.cljs:940` `count-limit (if pro-account? 10 1)` | **No** |
| Free: storage | 50 MB | `en.edn:37` "up to 50MB"; `settings.cljs:951` `default-storage-limit … 0.05` GB | **No** |
| Free: "notes only" (no assets) | — | `en.edn:37` | **No** — no plan gate in `worker/sync/assets.cljs` |
| Pro: synced graphs | 10 | `en.edn:52`; `settings.cljs:940` | **No** |
| Pro: storage | **5 GB** per `en.edn:52`; **10 GB** per `settings.cljs:951` | contradictory | **No** |
| Assets per file | 100 MB | `en.edn:61` | **Yes** (see above — 200 MiB on E2EE) |

Two further facts about the Settings usage panel: `settings.cljs:986` binds `graph-usage []` literally, so the panel always renders zero usage against the defaults; and `state/get-remote-graph-usage` (`state.cljs:864`) expects PascalCase `GraphStorageUsage` / `GraphStorageLimit` from the **legacy file-sync API** and has no caller. The `403 graph-count-exceed-limit` / `storage-limit` handlers in `handler/user.cljs:460-476` are in the file-sync `RemoteAPI`, not the DB-sync path. The code above the defaults carries the comment *"TODO: check logic on this. What are the rules around storage limits?"*

### Where a Pro tier would need to hook in

| Limit | Enforcement point that exists today | What is missing |
|---|---|---|
| Graph count | none | a count check before the insert at `index.cljs:334`, keyed on a plan claim the JWT does not yet carry |
| Storage per graph / per user | none | any accounting at all — the DO has no size meter; R2 object sizes are not summed |
| Assets on free plan | `assets.cljs:229` size check | a plan check at the same site |
| Asset size by tier | `assets.cljs:229` | already tier-aware for E2EE; would generalise to plan |

## Throttles and rate limits

### Semantic REST API (per user, per operation, per graph)

Applies **only** to the public REST API (`/api/v1/graphs/…` and `/semantic/…` routes). Real-time sync over the WebSocket is **not** rate-limited.

| Class | Limit | Source |
|---|---|---|
| Read operations | **120 requests / 60 s** | `wrangler.toml:36-37` `SEMANTIC_READ_RATE_LIMITER` |
| Write operations | **20 requests / 60 s** | `wrangler.toml:42-43` `SEMANTIC_WRITE_RATE_LIMITER` |

Key: `<user sub>:<operation-id>:<graph-id>` (`dispatch.cljs:108`), so the budget is per operation, not global. On exceed: `429 {"error":"rate limit exceeded"}` with `retry-after: 60` (`dispatch.cljs:50`). If the limiter binding is missing: `503 rate limiter unavailable`. Which class an operation belongs to is its `:rate-class` in `routes/semantic.cljs`.

### WebSocket reconnect (client)

| Parameter | Value | Source |
|---|---|---|
| Reconnect base delay | 1,000 ms | `src/main/frontend/worker/sync.cljs:22` |
| Reconnect max delay | 30,000 ms | `sync.cljs:23` |
| Reconnect jitter | ±250 ms | `sync.cljs:24` |
| Stale-socket check interval | 60,000 ms | `sync.cljs:25` |
| Stale-socket timeout | 600,000 ms (10 min) | `sync.cljs:26` |

### Timeouts and retries (client)

| Parameter | Value | Source |
|---|---|---|
| Upload response timeout | 120,000 ms (2 min) | `worker/sync/apply_txs.cljs:46` |
| Remote snapshot apply retries | 3, 50 ms apart | `apply_txs.cljs:44-45` |

## Batch sizes

### Upload (client → server)

| Stage | Size | Source |
|---|---|---|
| Datom preparation | 100,000 datoms | `worker/sync/upload.cljs:22` |
| KV rows per snapshot batch | 500 rows | `upload.cljs:21` |
| Bytes per snapshot frame | 1,000,000 | `upload.cljs:23` |
| Datoms per tx upload request | 5,000 | `apply_txs.cljs:47` |
| Server: large-tx threshold | ≥ 500 items | `handler/sync.cljs:26` |
| Server: large-tx chunk | 500 items | `handler/sync.cljs:27` |

### Download (server → client)

| Stage | Size | Source |
|---|---|---|
| Snapshot rows per pull batch | 10,000 | `handler/sync.cljs:21` |
| Datoms per import batch | 10,000 | `worker/sync/download.cljs:160` |
| Asset download parallelism | 10 concurrent | `worker/sync/assets.cljs:24` |

### Storage

| Parameter | Value | Source | Why |
|---|---|---|---|
| SQL bind parameters per statement | 99 | `deps/db-sync/src/logseq/db_sync/batch.cljs:4` | D1 / DO SQLite cap of 100 |
| Rows per insert (3 params each) | 33 | `batch.cljs:5-11` | derived |

## Server caches and TTLs

Not user-facing, but they bound how quickly membership and identity changes take effect.

| Cache | TTL / size | Source |
|---|---|---|
| Graph access decision | 5,000 ms | `handler/index.cljs:426` |
| User upsert | 1 h TTL, 1,024 entries | `index.cljs:6-7` |
| Activity touch | 8,192 entries | `index.cljs:9` |
| Signed asset link | 300 s | `worker/asset_link.cljs:5` |

## Platform limits (Cloudflare, not Logseq)

These are not in the repo and cannot be verified from it; they are the ceiling behind several rows above.

- One Durable Object per graph; the graph's KV store lives in DO SQLite. Its per-row/blob size limit is what `snapshot row too large` surfaces.
- SQLite bind-parameter cap of 100 is why `max-sql-params` is 99.
- WebSocket message size is a platform limit; there is no client- or server-side cap in code.
- R2 holds assets; no size accounting is performed.

## Error codes a client will see

| Code / message | HTTP | Meaning |
|---|---|---|
| `asset too large` | 413 | Over 100 MiB (200 MiB E2EE) |
| `snapshot row too large` | 413 | One KV row exceeds DO SQLite's blob limit |
| `rate limit exceeded` | 429 | Semantic API budget spent; wait `retry-after` |
| `rate limiter unavailable` | 503 | Limiter binding missing (deployment issue) |
| `graph not ready` | 409 | Snapshot upload in progress |
| `semantic-api-unavailable-for-e2ee` | 409 | REST API disabled on encrypted graphs |
| `insufficient scope` | 403 | Token lacks the operation's scope |
| `forbidden` | 403 | Not a graph member/owner |
| `:db-sync/snapshot-row-too-large` | client | Row exceeds the 1 MB frame before upload |
| `:rtc.asset.log/asset-too-large` | client | Asset skipped at 100 MiB |

## Keeping this page honest

Every number here is duplicated from code and will drift. `dz/support-engineering/script/sync-limits/check_sync_limits.clj` re-reads each source and fails when a value no longer matches the declaration in `sync-limits.edn`. Run it when bumping `logseqVersion`.
