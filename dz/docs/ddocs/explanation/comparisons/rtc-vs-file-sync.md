---
title: 'RTC vs File Sync'
description: 'Why Logseq replaced file-level Markdown sync with operation-level DB sync (db-sync, still labelled RTC in parts of the code), and how the two differ'
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
category: comparisons
relatedDocs:
  tutorials:
    - '[Real-Time Sync Setup](../../tutorials/collaboration/real-time-sync.md)'
  explanation:
    - '[E2EE Encryption](../security/e2ee-encryption.md)'
  reference:
    - '[Sync Limits](../../reference/features/sync-limits.md)'
    - '[Asset Sync](../../reference/features/asset-sync.md)'
---

# RTC vs File Sync

> **Last revised:** 2026-09-11 — verified against `be800f1711`

> Why Logseq moved from syncing Markdown files to syncing database transactions, and what actually changed.

## Overview

Logseq has had two cloud sync implementations. **Only one exists today.**

- **File Sync** synced the Markdown/Org files of a file-based graph. Its runtime was deleted in commit `bcc478b5f7` ("refactor: separate og and db version", 2025-12-29). That commit removed `src/main/frontend/fs/sync.cljs` (3,322 lines), `src/main/frontend/components/file_sync.cljs`, `src/main/frontend/handler/file_sync.cljs` and `src/electron/electron/file_sync_rsapi.cljs`. File-based graphs cannot be opened any more either: the graph list keeps only DB graphs ([`repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs#L321-L323), "use `config/db-based-graph?` to avoid loading old file graphs").
- **DB sync** syncs the transactions of a database graph. In source it is called **db-sync**: the client lives in `frontend.worker.sync` ([`src/main/frontend/worker/sync.cljs`](../../../../../src/main/frontend/worker/sync.cljs) and `worker/sync/`), and the server in `logseq.db-sync` ([`deps/db-sync/`](../../../../../deps/db-sync/)). The older name **RTC** survives in UI state and component names, for example `[:rtc/state :online-users]` ([`header.cljs:124`](../../../../../src/main/frontend/components/header.cljs#L124)) and `src/main/frontend/components/rtc/`.

What remains of File Sync is not a sync engine:
- The `/file-sync/` HTTP prefix is still used, but only for the account `user_info` call ([`user.cljs:424`](../../../../../src/main/frontend/handler/user.cljs#L424), [`:511-513`](../../../../../src/main/frontend/handler/user.cljs#L511-L513), called from [`events/ui.cljs:503`](../../../../../src/main/frontend/handler/events/ui.cljs#L503)).
- Some leftover assets remain: `file_sync.css`, the `file-sync-*.png` images and a 45-line `ios/App/App/FileSync/Data+ChaChaPoly.swift` helper.

Where this page describes File Sync, it describes the code at `bcc478b5f7^` (the parent of the removal commit), cited as `fs/sync.cljs@bcc478b5f7^`.

| Method        | Graph type      | Unit synced            | Status                        |
| ------------- | --------------- | ---------------------- | ----------------------------- |
| **File Sync** | Markdown/Org files | Whole files         | **Removed** (`bcc478b5f7`)    |
| **db-sync (RTC)** | Database graphs | Transactions (datoms) | Active                  |

---

## How They Work

### File Sync (removed)

```
Device A          Logseq Sync          Device B
   │                    │                   │
   │── Upload files ───►│                   │
   │  (encrypted)       │── WS notify ─────►│
   │                    │◄── Fetch files ───│
   │                    │  3-way merge local │
```

**How it worked, at `bcc478b5f7^`:**

- It synced whole files, compared by md5 during full syncs (`fs/sync.cljs@bcc478b5f7^:53-56`).
- Local changes were flushed "every 10s" (`:63`). A local→remote full sync ran "after 20mins of idle" (`:62`).
- A WebSocket was opened per graph to learn about remote changes (`:240-241`, pinged every 5 minutes at `:215-228`).
- Uploads were batched 20 files at a time and downloads 100 at a time (`:168-169`).
- Conflicts were **merged automatically**, not written to conflict files. Incoming, base and local content were combined with `diff-merge/three-way-merge`, using copies kept under `logseq/version-files/base` and `logseq/version-files/incoming` (`:1450`, `:1498`, `:1513`).
- Because the graph was plain files on disk, external editors could change it, and file watching picked up those changes.

### db-sync / RTC

```
Device A              db-sync server             Device B
   │                        │                        │
   │══ WebSocket (wss) ═════╪════ WebSocket (wss) ═══│
   │── tx/batch (t-before) ►│                        │
   │◄── tx/batch/ok ────────│──── changed ──────────►│
   │                        │◄─── pull (since) ──────│
   │                        │──── pull/ok (txs) ────►│
```

**How it works:**

- **What syncs is transactions, not files.**
  - Every local DataScript transaction is normalized and saved as a pending row in a client-side `client_ops` SQLite table, together with its reverse datoms ([`apply_txs.cljs:1876-1889`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1876-L1889); table at [`client_op.cljs:40`](../../../../../src/main/frontend/worker/sync/client_op.cljs#L40)).
  - The client sends pending txs as a `tx/batch` of at most 50 ([`apply_txs.cljs:960`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L960), [`:994`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L994)).
- **The transport is a persistent WebSocket.**
  - The production URL is `wss://api.logseq.io/sync/%s` ([`config.cljs:41-44`](../../../../../src/main/frontend/config.cljs#L41-L44)).
  - The server types its messages as `hello`, `online-users`, `presence`, `pull/ok`, `tx/batch/ok`, `changed`, `tx/reject`, `pong` and `error`, and the client dispatches on them ([`handle_message.cljs:330-336`](../../../../../src/main/frontend/worker/sync/handle_message.cljs#L330-L336)).
  - When the server says `changed`, a client that is behind sends `pull` ([`handle_message.cljs:386-391`](../../../../../src/main/frontend/worker/sync/handle_message.cljs#L386-L391), [`:115-127`](../../../../../src/main/frontend/worker/sync/handle_message.cljs#L115-L127)).
- **The server orders transactions; it does not merge them.**
  - A `tx/batch` whose `t-before` is not the server's current `t` is rejected with reason `"stale"` ([`handler/sync.cljs:624-627`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L624-L627)).
  - On that rejection the client pulls again ([`handle_message.cljs:190-192`](../../../../../src/main/frontend/worker/sync/handle_message.cljs#L190-L192)).
- **The client reconciles.**
  - Remote txs are applied by reversing local pending txs, transacting the remote txs, and then rebasing the local txs on top ([`apply_txs.cljs:1682-1689`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1682-L1689)).
- **Presence.** Each client reports the block it is editing (`editing-block-uuid`, sent at [`sync.cljs:183`](../../../../../src/main/frontend/worker/sync.cljs#L183)). The server rebroadcasts the result as `online-users` ([`presence.cljs:38-40`](../../../../../deps/db-sync/src/logseq/db_sync/worker/presence.cljs#L38-L40), [`:47`](../../../../../deps/db-sync/src/logseq/db_sync/worker/presence.cljs#L47)).

---

## Feature Comparison

The File Sync column describes removed code at `bcc478b5f7^`.

| Feature                   | File Sync (removed)                         | db-sync (RTC)                                   |
| ------------------------- | ------------------------------------------- | ----------------------------------------------- |
| **Graph type required**   | File-based (Markdown/Org)                   | Database                                        |
| **Sync unit**             | Whole file                                  | Transaction batch                               |
| **Remote change signal**  | WebSocket notify, 10 s local flush          | WebSocket `changed` → `pull`                    |
| **Multi-user presence**   | None found                                  | ✅ `online-users` with `editing-block-uuid`     |
| **Conflict handling**     | Automatic three-way text merge              | Automatic rebase; `:block/title` conflicts recorded for review |
| **External editors**      | ✅ Files on disk                            | ❌ Graph is a single `db.sqlite`                |
| **Offline edits**         | Not re-verified                             | ✅ Queued as pending `client_ops` rows          |
| **Asset sync**            | Not re-verified                             | ✅ See [asset-sync.md](../../reference/features/asset-sync.md) |
| **Version history**       | ✅ Remote + local version files (`handler/file_sync.cljs@bcc478b5f7^:126-184`) | ❌ None found in sync code |
| **End-to-end encryption** | ✅ `age` passphrase encryption, encrypted file names (`electron/file_sync_rsapi.cljs@bcc478b5f7^:44-54`) | ✅ Per-graph, on by default; covers `:block/title`, `:block/name` and assets (see [e2ee-encryption.md](../security/e2ee-encryption.md)) |

> **Corrected:** earlier versions of this table said File Sync had no end-to-end encryption and needed manual conflict resolution. Both were wrong. The removed code encrypted file contents and names with an `age` passphrase and merged conflicts with a three-way merge.

> **Unverified:** the earlier "Bandwidth usage: higher / lower" row was removed. Nothing in source measures it.

---

## Size Limits

### File Sync Limits (removed)

| Resource       | Limit                                          | Source (`bcc478b5f7^`) |
| -------------- | ---------------------------------------------- | ---------------------- |
| Upload batch   | 20 files                                       | `fs/sync.cljs@bcc478b5f7^:169`     |
| Download batch | 100 files                                      | `fs/sync.cljs@bcc478b5f7^:168`     |
| Storage / graph count | Server answered `403 storage-limit` / `graph-count-exceed-limit` | `fs/sync.cljs@bcc478b5f7^:932-944` |

> **Unverified:** a 100 MB per-file limit for File Sync was listed here before. No such check exists in the removed client code. If there was one, it was server-side.

### db-sync Limits

| Resource          | Limit                                          | Source |
| ----------------- | ---------------------------------------------- | ------ |
| Per-asset size    | 100 MiB, or 200 MiB on the server for E2EE graphs | Server [`assets.cljs:9-10`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs#L9-L10), selected at [`:229`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs#L229); client [`worker/sync/assets.cljs:19`](../../../../../src/main/frontend/worker/sync/assets.cljs#L19) |
| Tx batch per upload | 50 pending txs                               | [`apply_txs.cljs:960`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L960) |
| Graph count / storage | Shown in the UI, **not enforced** by the server | [sync-limits.md](../../reference/features/sync-limits.md#account-level-limits--claimed-not-enforced) |

For every limit, throttle and timeout, see [sync-limits.md](../../reference/features/sync-limits.md).

## Findings: New Capabilities

db-sync has capabilities that older File Sync vs RTC writeups do not mention:

- **Presence.** Payloads carry `editing-block-uuid`, and the server broadcasts `online-users` ([`malli_schema.cljs:29`](../../../../../deps/db-sync/src/logseq/db_sync/malli_schema.cljs#L29), [`:70-73`](../../../../../deps/db-sync/src/logseq/db_sync/malli_schema.cljs#L70-L73)).
- **Partial batch outcomes.** A rejected batch reports `success-tx-ids` and `failed-tx-id` ([`malli_schema.cljs:57-58`](../../../../../deps/db-sync/src/logseq/db_sync/malli_schema.cljs#L57-L58), set at [`handler/sync.cljs:653-654`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L653-L654)).
- **Recorded sync conflicts.**
  - While remote txs are applied, the client records a conflict for a remote `:block/title` value on a block that also has pending local edits ([`apply_txs.cljs:1130-1131`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1130-L1131), [`:1190`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1190), [`:1673-1677`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1673-L1677)).
  - It stores these in a `sync_conflicts` table ([`client_op.cljs:63-72`](../../../../../src/main/frontend/worker/sync/client_op.cljs#L63-L72)) and broadcasts `:sync-conflicts-updated` ([`handler/worker.cljs:72`](../../../../../src/main/frontend/handler/worker.cljs#L72)).
- **E2EE collaboration.** Key management includes user key-pair endpoints and a `/e2ee/graphs/:id/grant-access` flow (see [e2ee-encryption.md § Server Endpoints](../security/e2ee-encryption.md#server-endpoints)).

---

## When to Use Each

### Choose File Sync When:

You can't. File Sync is removed, and file-based graphs no longer open ([`repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs#L321-L323)).

If what you actually need is **plain files for Git or an external editor**, export from the DB graph. The export screen offers "Export as standard Markdown (no block properties)" on non-native platforms ([`export.cljs:140-143`](../../../../../src/main/frontend/components/export.cljs#L140-L143), [`en.edn:647`](../../../../../src/resources/dicts/en.edn#L647)). The export is one-way: its label says block properties are dropped.

### Choose db-sync When:

✅ Several people edit the same graph (presence, server-ordered txs)
✅ You want the same graph on several devices
✅ You want E2EE for block and page titles and assets
✅ You use DB-graph features (typed properties, classes)

---

## Conflict Handling

### File Sync Conflicts (removed)

When a remote file changed and the local copy had also changed, the client merged the two instead of asking you to:

1. It downloaded the incoming version into `logseq/version-files/incoming`.
2. It three-way merged base, incoming and local content (`fs/sync.cljs@bcc478b5f7^:1498`).
3. It wrote the merged content back to the page file.

> **Corrected:** this section used to describe `conflict-2026-01-29-page.md` files and a mandatory manual merge. The removed code has no such file naming. The string `conflict` does not appear in `fs/sync.cljs@bcc478b5f7^`.

### db-sync Conflict Resolution

When two clients edit the same graph:

1. Each client uploads a `tx/batch` tagged with the `t-before` it last saw.
2. The server accepts only a batch whose `t-before` equals its current `t`. Anything else is rejected as `"stale"` ([`handler/sync.cljs:624-627`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L624-L627)).
3. The rejected client pulls, then applies the remote txs **on top of its reversed local txs**. After that it rebases its pending local txs ([`apply_txs.cljs:1682-1689`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1682-L1689)).
4. It uploads again from the new `t`.

Rebases do not always succeed:
- If a pending tx fails to rebase, or rebases to a no-op, it is marked non-pending and never uploaded ([`apply_txs.cljs:1700-1710`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1700-L1710)).
- If a remote `:block/title` overwrote a block with pending local edits, the remote value is saved as a sync conflict. The block then shows a warning button ([`block.cljs:3389`](../../../../../src/main/frontend/components/block.cljs#L3389)). The popup says: "These server versions conflicted with local edits. Copy the text you want and edit the block manually." ([`en.edn:1831`](../../../../../src/resources/dicts/en.edn#L1831)). So title conflicts are resolved automatically but surfaced for review; other attributes are not recorded.

> **Removed:** the claim "No user intervention needed". Title conflicts surface a manual review prompt, and failed rebases drop local edits.

---

## Performance Characteristics

### File Sync (removed)

| Metric                    | Value (`bcc478b5f7^`)                         |
| ------------------------- | --------------------------------------------- |
| Local flush interval      | 10 s (`fs/sync.cljs@bcc478b5f7^:63`)                      |
| Idle full sync            | after 20 min (`fs/sync.cljs@bcc478b5f7^:62`)              |
| Change detection          | File watcher + server WebSocket               |
| Network unit              | Whole file                                    |

### db-sync

| Metric        | Value                                             |
| ------------- | ------------------------------------------------- |
| Upload trigger | Every persisted local tx enqueues a flush ([`apply_txs.cljs:1887-1890`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1887-L1890)) |
| Initial sync  | Full snapshot download, imported in 10,000-datom batches ([`download.cljs:160`](../../../../../src/main/frontend/worker/sync/download.cljs#L160)) |
| Connection    | Persistent WebSocket                              |
| Network unit  | Transaction datoms                                |

> **Unverified:** "Sync latency <1 second" and "Initial sync: seconds (delta only)" were removed. Source sets no latency target. Initial sync is a full snapshot download, not a delta.

---

## Migration Between Methods

### File graph → DB graph

The supported path is an import. "File to DB graph" means "Import a file-based Logseq graph folder into a new DB graph" ([`en.edn:926`](../../../../../src/resources/dicts/en.edn#L926); handler [`imports.cljs:444`](../../../../../src/main/frontend/components/imports.cljs#L444)). The new DB graph can then be synced with db-sync.

### DB graph → File Sync

> **Removed:** there is no path back. File Sync is gone, and file graphs cannot be opened. Markdown export (above) produces files without block properties, and nothing reads them back as a graph.

---

## Security Comparison

| Aspect                | File Sync (removed)                          | db-sync (RTC)                                            |
| --------------------- | -------------------------------------------- | -------------------------------------------------------- |
| Content encryption    | `age` passphrase (`file_sync_rsapi.cljs@bcc478b5f7^:50-54`) | AES-GCM-256 for `:block/title`, `:block/name` and asset bytes, **only on E2EE graphs** |
| File/page names       | Encrypted (`encrypt-fnames`, `:44-48`)       | `:block/name` encrypted on E2EE graphs                   |
| Encryption in transit | Not re-verified                              | `wss://` ([`config.cljs:44`](../../../../../src/main/frontend/config.cljs#L44)) |
| Key management        | Passphrase                                   | Password-protected RSA private key                       |
| Server can read data  | Not re-verified                              | Only non-encrypted attributes on E2EE graphs; everything on non-E2EE graphs |

db-sync uses hybrid encryption:

- **RSA-OAEP, 4096-bit** to wrap the per-graph AES key ([`common/crypt.cljs:68`](../../../../../src/main/frontend/common/crypt.cljs#L68))
- **AES-GCM, 256-bit** for data ([`common/crypt.cljs:103`](../../../../../src/main/frontend/common/crypt.cljs#L103))
- **PBKDF2, 600,000 iterations** to derive the key that protects the RSA private key. Keys created before version `20251210` use 100,000 ([`common/crypt.cljs:100`](../../../../../src/main/frontend/common/crypt.cljs#L100), [`:139`](../../../../../src/main/frontend/common/crypt.cljs#L139)).

Only two attributes are encrypted, `:block/title` and `:block/name` ([`worker/sync/const.cljs:25`](../../../../../src/main/frontend/worker/sync/const.cljs#L25)), plus asset bytes. Entity structure, other properties and graph metadata go to the server in the clear.

> **Corrected:** this table used to say "Encryption at rest ✅ AES-256-GCM" and "Server can read data ❌ Zero-knowledge" for RTC. Encryption is per-graph and partial; see [e2ee-encryption.md § What Gets Protected](../security/e2ee-encryption.md#what-gets-protected).

---

## Troubleshooting

### File Sync Issues

> **Removed:** File Sync no longer exists, so there is nothing to troubleshoot.

### db-sync Issues

| Problem          | What source shows                                                          |
| ---------------- | -------------------------------------------------------------------------- |
| Not connecting   | The client needs a WebSocket to `wss://api.logseq.io/sync/…`. Check the network, firewall and login. |
| Sync paused      | Uploads wait until the socket is open, the worker is online and the local `t` matches the remote `t` ([`apply_txs.cljs:940-945`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L940-L945)). Reconnects use backoff (see [sync-limits.md](../../reference/features/sync-limits.md#websocket-reconnect-client)). |
| Forgot E2EE password | There is no recovery. "Force reset password" makes "all currently encrypted graph data stored on the server permanently unreadable" ([`en.edn:613`](../../../../../src/resources/dicts/en.edn#L613); flow at [`settings.cljs:1288`](../../../../../src/main/frontend/components/settings.cljs#L1288)). The local copy stays readable, and you re-upload from it. |
| Version mismatch | > **Unverified:** the server records `client-revision` only as tx metadata ([`handler/sync.cljs:336-341`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L336-L341)). No version gate was found that rejects older clients. |

---

## Recommendations

| Scenario                  | Recommendation                                     |
| ------------------------- | -------------------------------------------------- |
| Solo user, several devices | db-sync                                           |
| Team collaboration        | db-sync                                            |
| Git / plain-text workflow | db-sync plus Markdown export (one-way, drops block properties) |
| Privacy-critical data     | db-sync with E2EE on, knowing that only titles, names and assets are encrypted |
| Old file graph            | Import it into a DB graph ("File to DB graph")     |

---

## See Also

- **E2EE Security**: [e2ee-encryption.md](../security/e2ee-encryption.md)
- **Sync Limits**: [sync-limits.md](../../reference/features/sync-limits.md)
- **RTC Setup Tutorial**: [real-time-sync.md](../../tutorials/collaboration/real-time-sync.md)
- **Database Graphs**: [file-vs-db.md](../concepts/file-vs-db.md)

---

_Last updated: 2026-09-11_
