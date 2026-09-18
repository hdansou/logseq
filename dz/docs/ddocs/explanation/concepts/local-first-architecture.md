---
title: 'Local-First Architecture'
status: 🟢 Stable
version: 'Logseq 2.0.1'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
quadrant: 'Explanation'
platform: 'Desktop + Web + Mobile'
related-docs:
  - '[File vs Database Graphs](./file-vs-db.md)'
  - '[E2EE Encryption](../security/e2ee-encryption.md)'
  - '[Database Integrity](../architecture/database-integrity.md)'
  - '[RTC vs File Sync](../comparisons/rtc-vs-file-sync.md)'
tags: ['architecture', 'local-first', 'privacy', 'offline', 'sync']
---

# Local-First Architecture

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-11 — verified against `be800f1711`

This page explains Logseq's local-first design: what it means, how the current code implements it, and where the claims stop. Knowing this explains why Logseq behaves the way it does, and what it does and does not protect.

**What You'll Understand:**

- What "local-first" means
- Why local storage matters
- How Logseq implements local-first today
- Sync options and trade-offs
- Privacy and ownership, including what leaves the device
- Comparison with cloud-first apps

---

## Table of Contents

1. [Local-First Principles](#local-first-principles)
2. [Why Local-First?](#why-local-first)
3. [Logseq's Implementation](#logseqs-implementation)
4. [Storage Architecture](#storage-architecture)
5. [Sync Strategies](#sync-strategies)
6. [Privacy & Ownership](#privacy--ownership)
7. [Trade-offs](#trade-offs)
8. [Future of Local-First](#future-of-local-first)

---

## Local-First Principles

### Core Definition

**Local-First:** software that stores and processes data mainly on the user's device, with cloud sync as an optional extra.

**Key Principles, and how Logseq measures up:**

```markdown
1. No server required for core functionality
   ✅ Graphs are created, edited and queried on the device
   ✅ Sync appears in the new-graph dialog only for some users
      (see "Logseq Sync" below)

2. Data lives on your device
   ✅ One db.sqlite file per graph (desktop) or an OPFS database (web)
   ⚠️ It is a SQLite file holding DataScript storage, not plain text

3. Fast and responsive
   ✅ Queries run against a local DataScript database; there is
      no network round trip

4. Privacy by default
   ✅ Graph content is not uploaded unless you turn on sync
   ⚠️ Usage analytics and error reports are ON by default (opt-out)

5. Longevity
   ✅ Works without Logseq's servers
   ✅ SQLite is a widely readable format, and export paths exist
```

### Local-First vs Cloud-First

**Cloud-First (Traditional SaaS):**

```markdown
Architecture:
┌─────────┐   every action   ┌─────────┐
│ Browser │ ←──────────────→ │ Server  │
│ (View)  │                  │ (Truth) │
└─────────┘                  └─────────┘

Characteristics:

- Server is the source of truth
- Most operations need the network
- Data stored on company servers
- Real-time collaboration is built in
```

**Local-First (Logseq):**

```markdown
Architecture:
┌─────────────────┐  optional   ┌───────────────┐
│ Local app +     │ ←─────────→ │ db-sync       │
│ local SQLite    │  tx batches │ server        │
│ (source of edits)│            │ (orders txs)  │
└─────────────────┘             └───────────────┘

Characteristics:

- The device holds the full graph and accepts edits offline
- When sync is on, the server decides the order of transactions;
  clients rebase their local edits onto it
- Collaboration needs sync to be switched on for the graph
```

> **Corrected:** the old diagram called the sync server a passive "Mirror". With db-sync the server assigns the global transaction order. It rejects batches built on a stale `t` ([`handler/sync.cljs:624-627`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L624-L627)), and clients rebase onto that order. See [rtc-vs-file-sync.md](../comparisons/rtc-vs-file-sync.md#db-sync-conflict-resolution).

---

## Why Local-First?

### 1. Speed & Responsiveness

**Local operations skip the network:**

```markdown
Cloud-first action:
User types → Send to server → Server processes → Send back → Update UI

Local-first action:
User types → Local DB worker transacts → Update UI
```

In Logseq, edits and queries go to a DB worker on the same machine: a Web Worker on web and mobile, or a `db-worker-node` child process on desktop. They never go to a remote server ([`persist_db.cljs:422-431`](../../../../../src/main/frontend/persist_db.cljs#L422-L431)).

> **Unverified:** the earlier performance table ("Load app <1s", "Search 10k notes <100ms", "Edit block <5ms", and cloud-first figures) was removed. Source contains no benchmarks that back those numbers.

### 2. Offline Functionality

**Work Anywhere:**

- Editing, search and queries use only the local database.
- On a synced graph, offline edits are kept as pending rows in a local `client_ops` table ([`client_op.cljs:40`](../../../../../src/main/frontend/worker/sync/client_op.cljs#L40)). Upload waits until the worker is online and the socket is open ([`apply_txs.cljs:940-945`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L940-L945)).

> **Removed:** the scenario list ("Censored network: complete access") and the claims about cloud-first apps, which source cannot support.

### 3. Privacy & Control

**Your data, your device:**

```markdown
Local-first (Logseq):

- Graph content stays on the device unless the graph is synced
- Graphs are not uploaded for analytics
- Synced graphs: content goes to the sync server, partly encrypted if E2EE is on

Cloud-first:

- All content on company servers by design
```

> **Corrected:** earlier text said "No server logging your activity" and, further down, "Logseq app: Knows nothing (no telemetry)". That is wrong; see [What leaves the device](#what-leaves-the-device).

### 4. Longevity

**Future-proofing:**

```markdown
Logseq:

- Storage is a SQLite file (db.sqlite), readable by any SQLite tool,
  but its content is DataScript storage, not a table of blocks
- Export paths: SQLite DB, zip, EDN, standard Markdown (no block properties)
- The app runs without Logseq's servers

Cloud-first:

- Dependent on the company's servers and export tools
```

> **Removed:** "Readable without Logseq" and "10+ year accessibility guaranteed". Opening `db.sqlite` in another tool shows only a `kvs` table of serialized DataScript storage ([`sqlite.cljs:13`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs#L13)), not readable notes. The page also listed shut-down services, including "Evernote (struggling)", which is opinion, not source.

### 5. Cost Structure

> **Unverified:** prices ("$10-15/month", "$5/mo sync subscription") cannot be proven from source. The app has plan-limit strings only: "1 synced graph (up to 50MB, notes only)" and "10 synced graphs (up to 5GB each)" ([`en.edn:37`](../../../../../src/resources/dicts/en.edn#L37), [`:52`](../../../../../src/resources/dicts/en.edn#L52)). Those limits are not enforced by the server ([sync-limits.md](../../reference/features/sync-limits.md#account-level-limits--claimed-not-enforced)). The app is open source, and local use needs no account.

---

## Logseq's Implementation

### Graph Types

**File-Based Graphs (legacy, import only):**

File-based graphs (a folder of `journals/`, `pages/`, `assets/` and `logseq/config.edn`) **cannot be opened** at `be800f1711`. The graph list keeps only DB graphs ([`repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs#L321-L323)). A file graph gets into the app only through the "File to DB graph" import ([`imports.cljs:444`](../../../../../src/main/frontend/components/imports.cljs#L444); label at [`en.edn:926`](../../../../../src/resources/dicts/en.edn#L926)).

**Database Graphs:**

```markdown
Storage: SQLite database

Desktop layout:
~/logseq/graphs/
└── <graph>/
    ├── db.sqlite        (the graph)
    ├── assets/          (images, PDFs, …)
    └── backup/          (automatic desktop backups)

Benefits:
✅ Typed properties and classes
✅ Schema validation
✅ db-sync (RTC) support
```

Sources for the layout:
- **Graphs directory**: `~/logseq/graphs` ([`common/config.cljs:58`](../../../../../deps/common/src/logseq/common/config.cljs#L58)).
- **Database file**: `db.sqlite` ([`sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs#L29-L33)).
- **Assets directory**: `assets` ([`common/config.cljs:59`](../../../../../deps/common/src/logseq/common/config.cljs#L59)).
- **Backup directory**: `backup` ([`graph_backup.cljs:10-11`](../../../../../src/main/logseq/db_worker/graph_backup.cljs#L10-L11)).

> **Corrected:** the old layout showed `logseq.db` and `version-files/*.transit` exports. The file is `db.sqlite`. `version-files` appears at master only as a file-graph path to ignore ([`deps/common/.../graph.cljs:84`](../../../../../deps/common/src/logseq/common/graph.cljs#L84)); DB graphs never write it.

### Local Storage Technologies

Each platform runs the same worker core (`frontend.worker.db-core`) on a different SQLite binding.

**Desktop (Electron):**
- The renderer does not touch SQLite. On desktop, `<start-runtime!` asks the main process for a db-worker runtime ([`persist_db.cljs:422-427`](../../../../../src/main/frontend/persist_db.cljs#L422-L427)).
- The main process spawns `db-worker-node` as a child process ([`daemon.cljs:226`](../../../../../src/main/logseq/db_worker/daemon.cljs#L226), [`:248`](../../../../../src/main/logseq/db_worker/daemon.cljs#L248)).
- The renderer talks to it over HTTP and SSE ([`persist_db/remote.cljs:2`](../../../../../src/main/frontend/persist_db/remote.cljs#L2)).
- The worker opens the database with Node's built-in `node:sqlite` `DatabaseSync` ([`platform/node.cljs:6`](../../../../../src/main/frontend/worker/platform/node.cljs#L6), [`:21`](../../../../../src/main/frontend/worker/platform/node.cljs#L21)).

**Web and Mobile (Capacitor):**
- The app starts a Web Worker and wraps it with Comlink ([`persist_db/browser.cljs:202-208`](../../../../../src/main/frontend/persist_db/browser.cljs#L202-L208)).
- The worker loads `@sqlite.org/sqlite-wasm` and stores the database in an OPFS pool ([`platform/browser.cljs:4`](../../../../../src/main/frontend/worker/platform/browser.cljs#L4), [`:74`](../../../../../src/main/frontend/worker/platform/browser.cljs#L74)).

> **Removed:** the old snippets showed `fs.readFileSync` on `.md` files, `@capacitor/filesystem` reads of `file.md`, `window.showDirectoryPicker()`, and "IndexedDB for database graphs". None of these is how graphs are stored today. (IndexedDB is used in the browser runtime only for small key-value secrets such as the persisted E2EE password; see [e2ee-encryption.md](../security/e2ee-encryption.md).)

### In-Memory Processing

**DataScript over SQLite:**

```clojure
;; The graph is a DataScript database held in the worker.
;; Its storage is persisted to SQLite as key/value rows:
;;   create table if not exists kvs
;;     (addr INTEGER primary key, content TEXT, addresses JSON)

;; Queries are Datalog against DataScript, never SQL against a blocks table:
(d/q '[:find ?title
       :where
       [?b :block/tags ?t]
       [?t :db/ident :logseq.class/Task]
       [?b :logseq.property/status ?s]
       [?s :db/ident :logseq.property/status.todo]
       [?b :block/title ?title]]
     @conn)
```

Sources: [`sqlite.cljs:13`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs#L13) for the `kvs` table. Task status is a closed-value ref with six values: `backlog`, `todo`, `doing`, `in-review`, `done`, `canceled` ([`property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L313-L318)).

Full-text search uses a **separate** SQLite database per graph (`"search" + repo-path`, [`db_core.cljs:531`](../../../../../src/main/frontend/worker/db_core.cljs#L531)). It holds an FTS5 trigram table ([`search.cljs:57`](../../../../../src/main/frontend/worker/search.cljs#L57)).

> **Removed:** the SQL example `SELECT * FROM blocks WHERE marker = 'TODO' AND deadline < date('now')`. The graph database has no `blocks` table, and `marker` is a file-graph attribute.

---

## Storage Architecture

### File-Based Architecture (legacy)

> **Removed:** the "Read phase / Work phase / Write phase" description of parsing `.md` files into DataScript and writing them back. File graphs no longer open. The parser (`deps/graph-parser/`) is still used to import file graphs into DB graphs, and some of its text helpers are shared elsewhere; worker search, for example, requires `logseq.graph-parser.text` ([`search.cljs:15`](../../../../../src/main/frontend/worker/search.cljs#L15)).

### Database Architecture

**How It Works:**

```markdown
1. Open phase:
   - The DB worker opens db.sqlite (desktop) or the OPFS database (web/mobile)
   - DataScript storage is restored from the kvs table
   - The search database is opened alongside it
2. Work phase:
   - Outliner operations transact into DataScript
   - Changed storage is written back to SQLite
   - Queries are Datalog (DataScript), not SQL
3. Sync phase (only if the graph is synced):
   - Each local tx is saved as a pending row in client_ops
   - Pending txs are uploaded as tx/batch messages over a WebSocket
   - Remote txs are pulled, local pending txs are rebased on top
```

Sources for the sync phase: [`apply_txs.cljs:1876-1890`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1876-L1890) (pending txs), [`:960`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L960) and [`:994`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L994) (batch upload), [`:1682-1689`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1682-L1689) (rebase).

> **Removed:**
> - "Export transit file → upload → resolve conflicts". Sync uploads transaction batches, not transit exports.
> - The invented `CREATE TABLE blocks (… content TEXT, parent_id …, properties JSON …)` schema. The real on-disk schema is the single `kvs` table above; the logical schema lives in DataScript (see [schema.cljs](../../../../../deps/db/src/logseq/db/frontend/schema.cljs), version `65.33` at `:33`).

---

## Sync Strategies

### Git, iCloud, Dropbox, Syncthing (file tools)

> **Removed:** the "Git Sync (File-Based)" and "iCloud/Dropbox (File-Based)" workflows. They assumed a folder of Markdown files that Logseq reads, and file graphs no longer open.

> **Unverified:** putting a DB graph's `~/logseq/graphs/<graph>/` folder in Git or a file-sync service. Nothing in source supports it or guards against it. While the app is running, `db.sqlite` is held open by `db-worker-node`, so copying or merging the file from another device could corrupt it. For a copy you can move around, prefer the automatic backups or an export.

### Logseq Sync (db-sync / RTC)

**How It Works:**

```markdown
1. Each local transaction is queued as a pending row (client_ops)
2. If the graph is E2EE, :block/title and :block/name values are
   encrypted with the graph's AES-GCM key before upload
3. Pending txs are sent as tx/batch over a WebSocket (wss://api.logseq.io/sync/…)
4. The server appends batches in order and tells other clients "changed"
5. Other clients pull, decrypt, apply remote txs and rebase their own pending txs
6. If a remote :block/title overwrote a block with pending local edits,
   the remote value is saved as a sync conflict for you to review
```

**Benefits:**

```markdown
✅ Near-real-time sync over a persistent WebSocket
✅ Multi-user editing with presence (who is editing which block)
✅ Optional E2EE, on by default for new synced graphs
✅ Works on desktop, web and mobile
```

**Challenges:**

```markdown
⚠️ Database graphs only
⚠️ Dependent on a sync server (Logseq's, or a self-hosted one)
⚠️ E2EE covers only block titles, page names and assets;
   structure and other properties are sent in the clear
⚠️ Some conflicts are surfaced for manual review, and a local tx that
   cannot be rebased is dropped
⚠️ Access is gated: the "Use Logseq Sync?" option appears only for
   users in the "team" or "rtc_2025_07_10" groups, in dev builds,
   or when a custom sync server is set
```

Sources for the challenges and the steps above:
- **Encrypted attributes**: [`worker/sync/const.cljs:25`](../../../../../src/main/frontend/worker/sync/const.cljs#L25).
- **WebSocket URL**: [`config.cljs:41-44`](../../../../../src/main/frontend/config.cljs#L41-L44).
- **Server ordering**: [`handler/sync.cljs:624-627`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L624-L627).
- **Conflict recording**: [`apply_txs.cljs:1130-1131`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1130-L1131) and [`:1673-1677`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1673-L1677).
- **Dropped rebases**: [`apply_txs.cljs:1700-1710`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1700-L1710).
- **Access gate**: [`user.cljs:357-362`](../../../../../src/main/frontend/handler/user.cljs#L357-L362), used at [`repo.cljs:700-704`](../../../../../src/main/frontend/components/repo.cljs#L700-L704).

> **Corrected:** this section used to say "Merge changes (CRDT)", "Conflict-free (CRDT)" and "Server never sees unencrypted data". Source contains no CRDT: the server orders transactions and the client reverses, applies and rebases. The server sees everything outside the encrypted attribute set, and it sees everything on a non-E2EE graph.

> **Unverified:** "Requires subscription ($5/mo)". No price appears in source.

### Self-Hosted Sync

The db-sync server can be self-hosted:
- `deps/db-sync` ships "a Node.js adapter for self-hosting" alongside its Cloudflare Worker ([`deps/db-sync/README.md:4`](../../../../../deps/db-sync/README.md#L4), [`:104`](../../../../../deps/db-sync/README.md#L104)).
- The app has a **Sync Server URL** setting under Settings → Advanced ([`settings.cljs:939`](../../../../../src/main/frontend/components/settings.cljs#L939)). It is stored in `localStorage` as `sync-server-url` ([`config.cljs:51-56`](../../../../../src/main/frontend/config.cljs#L51-L56)) and replaces the default WebSocket URL ([`:88-93`](../../../../../src/main/frontend/config.cljs#L88-L93)).
- The setting's own warning reads: "Your Logseq authentication tokens will be sent to this server, so only use a trusted URL" ([`en.edn:1775`](../../../../../src/resources/dicts/en.edn#L1775)).
- Setting a custom URL also turns on the sync option in the new-graph dialog ([`user.cljs:361`](../../../../../src/main/frontend/handler/user.cljs#L361)).

> **Removed:** "Syncthing / rsync (file-based)" and "CRDTs (database): self-host sync server". The first two are file-graph workflows; the third named a mechanism that does not exist.

---

## Privacy & Ownership

### Data Ownership

**You Own Your Data:**

```markdown
Database graph:

- A db.sqlite file on your disk (desktop) or in browser OPFS (web)
- Automatic hourly backups on desktop, 12 kept, in <graph>/backup/
- Exports: SQLite DB, zip, EDN, standard Markdown (no block properties)

Contrast with cloud-first:

- Data primarily on the provider's servers
```

Sources:
- **Backups**: throttled to one per hour ([`electron/db.cljs:15-16`](../../../../../src/electron/electron/db.cljs#L15-L16), applied at [`:60`](../../../../../src/electron/electron/db.cljs#L60)), 12 versions kept ([`:18`](../../../../../src/electron/electron/db.cljs#L18)), written under `backup/` ([`graph_backup.cljs:10`](../../../../../src/main/logseq/db_worker/graph_backup.cljs#L10)).
- **Exports**: [`export.cljs:125-151`](../../../../../src/main/frontend/components/export.cljs#L125-L151).

> **Removed:** "Transit exports (readable)". The transit export is a debug file "to send to us for debugging" ([`en.edn:633`](../../../../../src/resources/dicts/en.edn#L633)), not a portability format.

### What Leaves the Device

**Local operation (no sync):**

```markdown
- Graph content: not uploaded
- Usage analytics (PostHog) and error reports (Sentry): ON by default,
  can be turned off in Settings → Advanced
```

The analytics are real, and they are opt-out:
- Instrumentation starts unless `:instrument/disabled?` is set ([`instrumentation/core.cljs:7-11`](../../../../../src/main/frontend/modules/instrumentation/core.cljs#L7-L11)).
- That flag is read from local storage key `instrument-disabled`, which is empty on a fresh install ([`state.cljs:114`](../../../../../src/main/frontend/state.cljs#L114)).
- The toggle reads "Send usage data and diagnostics to Logseq". Its description says: "Logseq will never collect your local graph database or sell your data." ([`en.edn:1705-1706`](../../../../../src/resources/dicts/en.edn#L1705-L1706); row at [`settings.cljs:561-567`](../../../../../src/main/frontend/components/settings.cljs#L561-L567)).
- PostHog is off in dev builds and is configured with `autocapture false`, `disable_session_recording true`, `mask_all_text true` and `mask_all_element_attributes true` ([`posthog.cljs:32-42`](../../../../../src/main/frontend/modules/instrumentation/posthog.cljs#L32-L42)).

**With Logseq Sync (db-sync):**

```markdown
- The sync server stores the graph as snapshot rows (kvs) plus a
  transaction log (tx_log)
- E2EE graph: :block/title, :block/name and asset bytes are ciphertext;
  entity structure, other properties, graph name, email and membership are not
- Non-E2EE graph: everything is readable by the server
```

The server tables are created at [`deps/db-sync/.../storage.cljs:34-43`](../../../../../deps/db-sync/src/logseq/db_sync/storage.cljs#L34-L43). What E2EE covers, and what it leaves out, is detailed in [e2ee-encryption.md § What Gets Protected](../security/e2ee-encryption.md#what-gets-protected).

> **Corrected:** the old section said "Logseq app: Knows nothing (no telemetry)", "Logseq servers: Encrypted blobs only", "No usage tracking" and "No analytics by Logseq". All four contradict source.

### Data Portability

**Exit Strategy:**

```markdown
Database graph → other tools:

1. Export as standard Markdown (drops block properties)
2. Export as EDN (full data, readable and editable; "Don't rely on this
   as a primary backup")
3. Export the SQLite DB or a zip
4. Read the EDN with your own scripts
```

- **Markdown export**: web and desktop only; it is hidden on native mobile ([`export.cljs:140-143`](../../../../../src/main/frontend/components/export.cljs#L140-L143)).
- **EDN description**: [`en.edn:635`](../../../../../src/resources/dicts/en.edn#L635).

> **Removed:**
> - "Export to JSON". No JSON export exists in the Export screen.
> - "Data loss: minimal to none" and "Vendor lock-in: none". The Markdown export itself says it drops block properties.

---

## Trade-offs

### Advantages

**✅ Speed**

```markdown
- Edits and queries hit a local DB worker, not a server
```

**✅ Privacy**

```markdown
- Graph content stays on the device unless you sync it
- Analytics can be turned off, and PostHog masks text
```

**✅ Offline Capability**

```markdown
- The full graph is local
- Synced graphs queue edits and upload when back online
```

**✅ Ownership**

```markdown
- The database file and backups are on your disk
- Multiple export formats
```

### Disadvantages

**❌ Collaboration Complexity**

```markdown
Challenge: collaboration needs a sync server

- The sync option is gated to certain groups or a custom server
- Conflicting title edits are surfaced for manual review
- Local edits that cannot be rebased are dropped

Mitigation:

- db-sync with presence (see who is editing which block)
```

**❌ Not Plain Text**

```markdown
Challenge: db.sqlite is not human-readable or diff-friendly

- External editors and Git no longer work on the live graph
- Markdown export is one-way and drops block properties
```

**❌ Mobile**

> **Unverified:** the old claims about iOS and Android file-access limits and background-sync battery drain. On mobile, the graph lives in the Web Worker's OPFS database, not in the device file system ([`persist_db.cljs:422-431`](../../../../../src/main/frontend/persist_db.cljs#L422-L431) chooses the browser worker for every non-Electron runtime). Source says nothing about battery use.

**❌ Backup Responsibility**

```markdown
Challenge: nothing is backed up off-device unless you arrange it

Built in:

- Desktop: automatic hourly backups, 12 kept, under <graph>/backup/
  (same disk as the graph)
- Export screen: "Backup now" and a scheduled backup to a chosen folder

Still needed:

- Off-device copies of those backups or exports
```

Sources: [`electron/db.cljs:15-18`](../../../../../src/electron/electron/db.cljs#L15-L18) and [`en.edn:670-677`](../../../../../src/resources/dicts/en.edn#L670-L677).

---

## Future of Local-First

### Emerging Technologies

**CRDTs (Conflict-Free Replicated Data Types):**

```markdown
What: data structures that merge concurrent edits without coordination
Status in Logseq: NOT used. db-sync is server-ordered transactions with
client-side reverse/apply/rebase.
```

> **Corrected:** "Status: Logseq RTC uses CRDTs". No CRDT library or data type appears in `src/main/frontend/worker/sync/` or `deps/db-sync/`.

**WebAssembly:**

```markdown
What: native-speed code in the browser
Status in Logseq: already shipping. The web and mobile builds run SQLite
as WASM (@sqlite.org/sqlite-wasm) on OPFS.
```

> **Corrected:** "Status: Possible future for Logseq". WASM SQLite is the current web and mobile storage engine ([`platform/browser.cljs:4`](../../../../../src/main/frontend/worker/platform/browser.cljs#L4)).

**Peer-to-Peer Sync:**

> **Unverified:** no peer-to-peer sync exists in source. db-sync always goes through a server.

### Industry Trends

> **Removed:** the "More apps going local-first" list (Linear, Figma, VS Code, …). It is industry commentary that Logseq source cannot verify.

---

## Comparison Table

| Aspect            | Local-First (Logseq DB graph)                 | Cloud-First (typical SaaS) |
| ----------------- | --------------------------------------------- | -------------------------- |
| **Latency**       | Local DB worker, no network round trip        | Network round trip         |
| **Offline**       | Full graph available; sync resumes later      | Limited                    |
| **Privacy**       | Content local unless synced; analytics opt-out | Content on servers        |
| **Storage**       | `db.sqlite` on your disk (desktop) or OPFS (web) | Provider's servers      |
| **Collaboration** | db-sync, gated, server-ordered                | Built in                   |
| **Backup**        | Automatic local hourly backups (desktop); off-device is up to you | Provider-managed |
| **Portability**   | Export SQLite / zip / EDN / Markdown          | Provider export tools      |

> **Removed:** the Speed ("50-500ms lag"), Cost ("$8-10/mo") and "Company owns data" cells, which source cannot verify.

---

## Next Steps

**Working local-first with Logseq:**

1. Remember that your graph is a local `db.sqlite` (desktop) or OPFS database (web)
2. Copy `<graph>/backup/` or regular exports somewhere off-device
3. Decide whether to sync, and whether E2EE is enough for your data
4. Check Settings → Advanced for analytics and the sync server URL

**Related:**

- [File vs Database Graphs](./file-vs-db.md)
- [E2EE Encryption](../security/e2ee-encryption.md)
- [RTC vs File Sync](../comparisons/rtc-vs-file-sync.md)
- [Backup Strategies](../../how-to/organization/backup-strategies.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
