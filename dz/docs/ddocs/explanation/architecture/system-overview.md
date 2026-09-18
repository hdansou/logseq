---
title: 'Logseq System Overview'
description: "High-level overview of Logseq's architecture, value propositions, and design principles"
status: stable
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
logseqVersion: '2.0.1'
category: architecture
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
    - '[Plugin API](../../reference/api/plugin-api.md)'
  explanation:
    - '[Data Architecture](data-architecture.md)'
    - '[File vs DB](../concepts/file-vs-db.md)'
---

# Logseq System Overview

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-11 — verified against `be800f1711`

> Understanding what Logseq is, why it exists, and how its parts fit together.

## What is Logseq?

Logseq is an **open-source, local-first knowledge management app**. It combines outline-style note-taking with a knowledge graph. Every block can be referenced, tagged, given typed properties and queried. The graph lives in a database on your device, and syncing it to a server is optional.

---

## Core Design Principles

### 🔒 Local-First

- **Local storage**: a graph is a single SQLite file, `db.sqlite`, under `~/logseq/graphs` by default ([`sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs#L29-L33), [`config.cljs:58`](../../../../../deps/common/src/logseq/common/config.cljs#L58)). On the web, the same database is kept in the browser's OPFS ([`platform/browser.cljs:74`](../../../../../src/main/frontend/worker/platform/browser.cljs#L74)).
- **Export paths**:
  - The Export screen offers a SQLite DB, a zip, an EDN file, standard Markdown (with no block properties) and a debug transit file ([`export.cljs:125-151`](../../../../../src/main/frontend/components/export.cljs#L125-L151)).
  - Desktop also writes automatic backups to `<graph>/backup/` ([`graph_backup.cljs:10`](../../../../../src/main/logseq/db_worker/graph_backup.cljs#L10)).
- **Sync is optional**: db-sync is chosen per graph with the "Use Logseq Sync?" option when the graph is created (see [e2ee-encryption.md § Is It On?](../security/e2ee-encryption.md#is-it-on)).

> **Corrected:** this section said "Open file formats (Markdown, Org-mode) ensure portability". The storage format is SQLite. Markdown is an export, and Org is not produced at all.

### 🧠 Knowledge Graph Approach

- **Bi-directional linking**: `[[Page]]` references create backlinks.
- **Block-level references**: a block ref is `[[<block-uuid>]]` ([`util/ref.cljs:5-7`](../../../../../src/main/frontend/util/ref.cljs#L5-L7)). The file-graph form `((uuid))` renders as literal text ([`block.cljs:1573-1574`](../../../../../src/main/frontend/components/block.cljs#L1573-L1574)).
- **Visual exploration**: the graph view renders with `pixi.js` ([`extensions/graph/pixi.cljs`](../../../../../src/main/frontend/extensions/graph/pixi.cljs)).

### 🔧 Extensibility

- **Plugins**: JavaScript/TypeScript plugins built with `@logseq/libs` (see [Plugin System](#3-plugin-system)).
- **Themes**: plugin-provided themes, plus a per-graph `logseq/custom.css` ([`handler/ui.cljs:116`](../../../../../src/main/frontend/handler/ui.cljs#L116)).
- **Local APIs**: the desktop app runs an HTTP server with a `POST /api` route ([`electron/server.cljs:168`](../../../../../src/electron/electron/server.cljs#L168)) and MCP routes at `/mcp` ([`:150-153`](../../../../../src/electron/electron/server.cljs#L150-L153)). There is also a separate `logseq` command-line tool (`cli/`).

---

## Graph Types

Only **database graphs** can be opened today. The graph list keeps only DB graphs ([`repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs#L321-L323): "use `config/db-based-graph?` to avoid loading old file graphs"). Old file-based (Markdown/Org folder) graphs reach the app only through the "File to DB graph" import ([`imports.cljs:444`](../../../../../src/main/frontend/components/imports.cljs#L444)).

| Aspect            | File-based graphs (legacy, import only) | Database graphs                 |
| ----------------- | --------------------------------------- | ------------------------------- |
| **Storage**       | Markdown/Org files                      | One `db.sqlite` per graph       |
| **Opens in app**  | ❌ No                                   | ✅ Yes                          |
| **Properties**    | Text in the file                        | Typed property entities         |
| **Tasks**         | `TODO`/`NOW`/`LATER` markers            | `:logseq.property/status` ref, six values ([`property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L313-L318)) |
| **Cloud sync**    | File Sync (removed, see [rtc-vs-file-sync.md](../comparisons/rtc-vs-file-sync.md)) | db-sync (RTC) |

> **Removed:** "Both types maintain the same user experience". File graphs cannot be opened, so the two types no longer share any experience.

---

## Architecture Layers

```
┌──────────────────────────────────────────────────────────┐
│                 User Interface (renderer)                │
│   React 19 via hsx components · Shui UI · editor         │
└───────────────────────────┬──────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────┐
│                 State Management                         │
│   frontend.state atoms · rfx subscriptions & events      │
└───────────────┬───────────────────────────┬──────────────┘
   Web / mobile │ Comlink                   │ Desktop (Electron)
                │                           │ HTTP + SSE
┌───────────────▼────────────┐ ┌────────────▼───────────────┐
│  DB worker (Web Worker)    │ │  db-worker-node (child     │
│  db-core · DataScript      │ │  process spawned by main)  │
│  sync · search · undo/redo │ │  db-core · DataScript      │
│                            │ │  sync · search · undo/redo │
└───────────────┬────────────┘ └────────────┬───────────────┘
                │                           │
┌───────────────▼────────────┐ ┌────────────▼───────────────┐
│ SQLite WASM on OPFS        │ │ node:sqlite  →  db.sqlite  │
│ (@sqlite.org/sqlite-wasm)  │ │ under ~/logseq/graphs      │
└────────────────────────────┘ └────────────────────────────┘
```

The two runtimes share the same core ([`db_worker.cljs:5`](../../../../../src/main/frontend/worker/db_worker.cljs#L5) and [`db_worker_node.cljs:6`](../../../../../src/main/frontend/worker/db_worker_node.cljs#L6) both require `frontend.worker.db-core`). They differ in how the UI reaches them and where SQLite lives:

- **Web and mobile**:
  - `persist-db` starts a `js/Worker` and wraps it with Comlink ([`persist_db/browser.cljs:202-208`](../../../../../src/main/frontend/persist_db/browser.cljs#L202-L208)).
  - The worker loads `@sqlite.org/sqlite-wasm` and installs an OPFS pool ([`platform/browser.cljs:4`](../../../../../src/main/frontend/worker/platform/browser.cljs#L4), [`:74`](../../../../../src/main/frontend/worker/platform/browser.cljs#L74)).
- **Desktop**:
  - `<start-runtime!` does **not** start a Web Worker. It asks the Electron main process for a db-worker runtime ([`persist_db.cljs:422-431`](../../../../../src/main/frontend/persist_db.cljs#L422-L431); handler at [`electron/handler.cljs:239`](../../../../../src/electron/electron/handler.cljs#L239)).
  - The main process spawns `db-worker-node` as a child Node process ([`db_worker/daemon.cljs:226`](../../../../../src/main/logseq/db_worker/daemon.cljs#L226), [`:248`](../../../../../src/main/logseq/db_worker/daemon.cljs#L248)).
  - The renderer talks to it over "db-worker-node HTTP and SSE" ([`persist_db/remote.cljs:2`](../../../../../src/main/frontend/persist_db/remote.cljs#L2)).
  - The Node platform opens SQLite with `node:sqlite`'s `DatabaseSync` ([`platform/node.cljs:6`](../../../../../src/main/frontend/worker/platform/node.cljs#L6), [`:21`](../../../../../src/main/frontend/worker/platform/node.cljs#L21)).

> **Corrected:** the previous diagram showed one Web Worker for every platform, a "File System (Markdown)" storage branch, and `better-sqlite3` on desktop. At `be800f1711`:
> - Desktop runs a separate Node process.
> - No Markdown storage backend exists.
> - `better-sqlite3` is required only by the self-hosted db-sync Node adapter ([`deps/db-sync/.../node/storage.cljs:2`](../../../../../deps/db-sync/src/logseq/db_sync/node/storage.cljs#L2)), not by the app's DB worker.

---

## Key Subsystems

### 1. Database Layer

- **DataScript**: an in-memory Datalog database, using Logseq's fork ([`deps.edn:10`](../../../../../deps.edn#L10)). It runs inside the DB worker or `db-worker-node`, not in the UI thread.
- **SQLite**: persistent storage. The web and mobile builds use SQLite WASM on OPFS; desktop uses `node:sqlite` (see above).
- **Schema**: Malli schemas validate entities ([`deps/db/src/logseq/db/frontend/malli_schema.cljs`](../../../../../deps/db/src/logseq/db/frontend/malli_schema.cljs), [`validate.cljs`](../../../../../deps/db/src/logseq/db/frontend/validate.cljs)). The current schema version is `65.33` ([`schema.cljs:33`](../../../../../deps/db/src/logseq/db/frontend/schema.cljs#L33)).
- **Undo/redo**: implemented in the worker ([`worker/undo_redo.cljs`](../../../../../src/main/frontend/worker/undo_redo.cljs)).

### 1b. DB Worker

- **Web Worker** on web and mobile, reached through Comlink. **`db-worker-node`** on desktop, reached through HTTP and SSE ("Node.js daemon entrypoint for db-worker", [`db_worker_node.cljs:2`](../../../../../src/main/frontend/worker/db_worker_node.cljs#L2)). The CLI shares the same daemon helpers ([`daemon.cljs:2`](../../../../../src/main/logseq/db_worker/daemon.cljs#L2)).
- **Responsibilities**:
  - DataScript transactions and queries, and SQLite persistence
  - db-sync ([`worker/sync.cljs`](../../../../../src/main/frontend/worker/sync.cljs))
  - Full-text, fuzzy and vector search ([`worker/search.cljs:2`](../../../../../src/main/frontend/worker/search.cljs#L2), [`:904`](../../../../../src/main/frontend/worker/search.cljs#L904))
  - Undo/redo

### 2. Editor System

- **Block-based editing**: every block is an entity with a UUID that can be referenced.
- **Slash commands**: typing `/` opens a command menu ([`commands.cljs`](../../../../../src/main/frontend/commands.cljs)).
- **Format**: DB graphs are Markdown-only. `get-preferred-format` always returns `:markdown` ([`state.cljs:555-557`](../../../../../src/main/frontend/state.cljs#L555-L557)).

> **Removed:** "Markdown/Org Support: Choose your preferred format". No format choice exists in DB graphs.

### 3. Plugin System

- **SDK**: `@logseq/libs` (TypeScript, `libs/src/`). Plugins talk to the host over postMessage.
  - A method declared in `LSPlugin.ts` is not necessarily implemented. A missing host method throws `Not existed method #…` ([`common.ts:328-330`](../../../../../libs/src/common.ts#L328-L330)).
- **Load modes**: `mode: 'shadow' | 'iframe'` ([`LSPlugin.core.ts:218`](../../../../../libs/src/LSPlugin.core.ts#L218)). A plugin gets `shadow` only if its package.json sets `logseq.mode` ([`:731-733`](../../../../../libs/src/LSPlugin.core.ts#L731-L733)); every other plugin uses an iframe.
  - The iframe is created with no `sandbox` attribute ([`postmate/index.ts:402-410`](../../../../../libs/src/postmate/index.ts#L402-L410)).
  - Shadow mode runs the plugin's scripts in the host document, behind a proxied global ([`LSPlugin.shadow.ts:48-70`](../../../../../libs/src/LSPlugin.shadow.ts#L48-L70)).
  - **Neither mode is a security sandbox.**
- **UI slots**: plugins can render into block and `{{renderer}}` macro slots ([`LSPlugin.ts:570-590`](../../../../../libs/src/LSPlugin.ts#L570-L590)).

> **Corrected:** "Sandbox Modes: Iframe (isolated) or Shadow (fast)". The modes exist, but "isolated" was wrong, and nothing in source supports "fast".

### 4. Sync & Collaboration

- **db-sync (RTC)** is the only cloud sync path. It syncs transaction batches over a WebSocket; see [rtc-vs-file-sync.md](../comparisons/rtc-vs-file-sync.md).
- **Collaboration**: several users can edit one graph. The server broadcasts online users and the block each one is editing.
- **E2EE**: per graph and on by default. It encrypts `:block/title`, `:block/name` and assets; see [e2ee-encryption.md](../security/e2ee-encryption.md).

> **Removed:** "File Sync: Traditional cloud sync for markdown files". Its runtime was deleted in `bcc478b5f7`.

## Findings: New Capabilities

Compared with earlier versions of this overview, source now includes:

- **CLI sync with E2EE bootstrap.**
  - The `logseq` CLI is an OCaml/Melange program under `cli/`. It reuses the ClojureScript `db-worker-node` server (`cli/AGENTS.md`).
  - `sync start`, `sync upload` and `sync download` accept `--e2ee-password` ([`cli/lib/cli_parse.ml:310-313`](../../../../../cli/lib/cli_parse.ml#L310-L313)).
- **Node-runtime secret persistence.** `db-worker-node` stores the E2EE password secret through `keytar` under the service `"Logseq E2EE"`. If the keychain call fails, it falls back to the worker's KV store ([`platform/node.cljs:607-625`](../../../../../src/main/frontend/worker/platform/node.cljs#L607-L625)). The browser runtime uses IndexedDB (see [e2ee-encryption.md](../security/e2ee-encryption.md)).
- **Sync backend.** `deps/db-sync` runs on Cloudflare Workers with a `SyncDO` Durable Object ([`wrangler.toml:21-23`](../../../../../deps/db-sync/worker/wrangler.toml#L21-L23)). It also ships a Node.js adapter "for self-hosting" ([`deps/db-sync/README.md:4`](../../../../../deps/db-sync/README.md#L4)).

> **Corrected:** "node runtime file-backed storage" became "`keytar` with KV fallback".

---

## Technology Stack

| Layer           | Technology                               | Purpose / evidence                                   |
| --------------- | ---------------------------------------- | ---------------------------------------------------- |
| **Language**    | ClojureScript (shadow-cljs)              | Main app code; `deps.edn:35`                         |
| **UI**          | React 19 + `hsx`                         | `package.json` `react 19.2.6`; `deps.edn:5`. No `rum.core` require remains under `src/main` |
| **State**       | `frontend.state` + `rfx`                 | "RFX integration for frontend application state, subscriptions, and events" ([`rfx.cljs:2`](../../../../../src/main/frontend/rfx.cljs#L2)) |
| **UI Components** | Shui                                   | `deps/shui/`                                         |
| **Database**    | DataScript (Logseq fork)                 | In-memory graph queries                              |
| **Persistence** | SQLite WASM on OPFS (web/mobile); `node:sqlite` (desktop) | `db.sqlite` per graph                  |
| **Concurrency** | Web Worker + Comlink (web/mobile); child Node process over HTTP/SSE (desktop) | Off-UI-thread DB work |
| **Desktop**     | Electron                                 | Main process, `src/electron/electron/`               |
| **Mobile**      | Capacitor                                | `package.json` `@capacitor/core`                     |
| **Styling**     | Tailwind CSS 4                           | `package.json` `tailwindcss 4.3.0`                   |
| **Sync server** | Cloudflare Workers + Durable Objects; Node adapter | `deps/db-sync/`                            |

> **Corrected:** "React + Rum" is now React + `hsx`.

---

## Who Uses Logseq?

> **Unverified:** the audience lists below (knowledge workers, development teams, personal users) are positioning, not something source can confirm. The one concrete feature they mentioned, spaced repetition, does exist: DB graphs have `Card`/`Cards` classes with FSRS state properties ([`class.cljs:57-63`](../../../../../deps/db/src/logseq/db/frontend/class.cljs#L57-L63)).

### Knowledge Workers

- Researchers, academics, students
- Writers, journalists, content creators

### Development Teams

- Documentation management
- Technical decision logs

### Personal Users

- Daily journaling
- Project planning
- Learning with flashcards (spaced repetition)

---

## What Makes Logseq Different?

> **Unverified:** competitor comparisons are marketing claims that source cannot prove. The earlier "Roam: free, self-hostable" row is supported only in part: db-sync's Node adapter can be self-hosted ([`deps/db-sync/README.md:104`](../../../../../deps/db-sync/README.md#L104)), but the app's default sync URL is compiled in ([`config.cljs:38-44`](../../../../../src/main/frontend/config.cljs#L38-L44)).

| Compared To     | Logseq Advantage                         |
| --------------- | ---------------------------------------- |
| **Notion**      | Local-first, open source                 |
| **Obsidian**    | Block-level references, open source      |
| **Roam**        | Open source, local database graphs       |
| **Apple Notes** | Knowledge graph, plugins, typed properties |

---

## Getting Started

1. **Download**: get Logseq from [logseq.com](https://logseq.com).
2. **Create a graph**: new graphs are database graphs. To bring in an old Markdown graph, use "File to DB graph".
3. **Start writing**: use daily journals or create pages.
4. **Link ideas**: use `[[double brackets]]` to connect pages; use "Copy block ref" for blocks.
5. **Explore**: try `/Query`, typed properties, tags and plugins.

---

## Further Reading

- **Data Architecture**: [data-architecture.md](data-architecture.md) — how data is stored
- **File vs DB**: [file-vs-db.md](../concepts/file-vs-db.md) — why file graphs were replaced
- **RTC vs File Sync**: [rtc-vs-file-sync.md](../comparisons/rtc-vs-file-sync.md)

> **Removed:** the link to `system-architecture.md`. That file does not exist in this doc tree.

---

_Last updated: 2026-09-11_
