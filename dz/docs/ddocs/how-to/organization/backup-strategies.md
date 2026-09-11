---
title: 'Backup and Recovery Strategies'
status: 🟢 Stable
version: 'DB graph schema 65.33'
quadrant: 'How-To'
platform: 'Desktop + Web + Mobile'
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
related-docs:
  - '[Import and Export Guide](./import-export.md)'
  - '[RTC vs File Sync](../../explanation/comparisons/rtc-vs-file-sync.md)'
  - '[File vs Database Graphs](../../explanation/concepts/file-vs-db.md)'
tags: ['backup', 'recovery', 'sync', 'export', 'data-safety']
---

# Backup and Recovery Strategies

This guide shows you how to protect a Logseq DB graph. It covers the backups Logseq already makes, the exports you can make yourself, how to restore them, and what sync does and does not protect you from.

A DB graph is a single `db.sqlite` file plus an `assets/` folder ([`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs); [`deps/common/src/logseq/common/config.cljs:56`](../../../../../deps/common/src/logseq/common/config.cljs)). It is not a folder of Markdown files, so every backup below copies that database.

**What You'll Learn:**

- Where a DB graph and its backups live
- The built-in automatic backups (desktop and browser)
- Manual exports and command-line backups
- Restoring a backup into a new graph
- Why sync is not a backup

**Time Required:** 15-30 minutes for full setup

---

## Table of Contents

1. [Understanding Backup Needs](#understanding-backup-needs)
2. [File-Based Graph Backups](#file-based-graph-backups)
3. [Database Graph Backups](#database-graph-backups)
4. [Automated Backup Workflows](#automated-backup-workflows)
5. [Recovery Procedures](#recovery-procedures)
6. [Best Practices](#best-practices)

---

## Understanding Backup Needs

### Where your graph lives

**Desktop app:** DB graphs are stored under `~/logseq/graphs/` ([`deps/common/src/logseq/common/config.cljs:55`](../../../../../deps/common/src/logseq/common/config.cljs)), one folder per graph. The home directory is expanded with Node's `os.homedir()`, so on Windows this is `%USERPROFILE%\logseq\graphs\` ([`deps/common/src/logseq/common/graph.cljs:116-126`](../../../../../deps/common/src/logseq/common/graph.cljs)). A graph's folder holds:

```
~/logseq/graphs/<graph folder>/
├── db.sqlite     ← the graph: pages, blocks, properties, settings
├── assets/       ← images, PDFs and other attachments
├── backup/       ← automatic, Mod+S and CLI snapshots
├── export/       ← output of Export SQLite DB / Export both SQLite DB and assets (desktop)
├── search/       ← search index database (search/db.sqlite)
└── client-ops-/  ← a second worker database (client-ops-/db.sqlite)
```

(`db.sqlite`: [`sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs); `backup/`: [`src/main/logseq/db_worker/graph_backup.cljs:10`, `:39-47`](../../../../../src/main/logseq/db_worker/graph_backup.cljs); `export/`: [`src/electron/electron/db.cljs:98-103`](../../../../../src/electron/electron/db.cljs), [`src/main/frontend/handler/export.cljs:78-86`](../../../../../src/main/frontend/handler/export.cljs); `search/` and `client-ops-/`: [`src/main/frontend/worker/db_core.cljs:133`, `:559-563`](../../../../../src/main/frontend/worker/db_core.cljs), joined to the graph folder by [`src/main/frontend/worker/platform/node.cljs:59-61`, `:715-716`](../../../../../src/main/frontend/worker/platform/node.cljs).)

The folder name is the graph name, URI-encoded, with `%20` turned back into a space and the remaining `%` escapes written as `~` ([`deps/common/src/logseq/common/graph_dir.cljs:6-12`](../../../../../deps/common/src/logseq/common/graph_dir.cljs)). A graph called `my/graph` therefore lives in `my~2Fgraph`.

The graphs directory can be overridden with the `LOGSEQ_GRAPHS_DIR` environment variable ([`graph.cljs:111-114`](../../../../../deps/common/src/logseq/common/graph.cljs)).

> **Unverified:** whether the whole desktop app honours `LOGSEQ_GRAPHS_DIR`. The renderer builds some graph paths from a hard-coded `~/logseq/graphs`, including the "open backups folder" link ([`src/main/frontend/config.cljs:326-335`](../../../../../src/main/frontend/config.cljs)).

**Browser (web app):** the database is stored in the browser's origin private file system ([`src/main/frontend/worker/platform/browser.cljs:74-76`](../../../../../src/main/frontend/worker/platform/browser.cljs)). You cannot reach that storage as a file, so use the backup folder or an export (below).

### Backup Strategy Matrix

| Where you use Logseq | Built-in automatic backup | Copy you control | Restore |
| -------------------- | ------------------------- | ---------------- | ------- |
| **Desktop app** | Hourly snapshot in `backup/`, 12 kept | Export SQLite DB / zip, CLI backup, then copy off the machine | Import → SQLite or SQLite + assets (.zip); CLI `graph backup restore` |
| **Browser (Chromium, not mobile)** | Hourly copy into a folder you choose, 12 older copies kept | The chosen folder | Import → SQLite |
| **Mobile app** | None found in source | See [Mobile](#mobile) | Import |
| **Logseq Sync** | Not a backup: deletions replicate (see [Sync is not a backup](#sync-is-not-a-backup)) | — | — |

### What Needs Backing Up?

- ✅ `db.sqlite`. It holds all content **and** the graph's `logseq/config.edn`, `logseq/custom.css` and `logseq/custom.js`, which are stored as file records inside the database ([`deps/db/src/logseq/db/sqlite/create_graph.cljs:205-220`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)).
- ✅ `assets/`. **Neither the automatic backups nor Export SQLite DB include assets.** A backup snapshot contains only `db.sqlite` and `metadata.edn` ([`graph_backup.cljs:11-12`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)). Only **Export both SQLite DB and assets** bundles them ([`handler/export.cljs:88-100`](../../../../../src/main/frontend/handler/export.cljs)).
- ✅ `backup/` and `export/`, when you copy the graph folder off the machine.
- ➖ `search/` and `client-ops-/`. Snapshots and exports copy only the main database connection ([`db_core.cljs:980-994`](../../../../../src/main/frontend/worker/db_core.cljs)), so these are never part of a Logseq backup.
- ❌ Plugins (`~/.logseq/plugins`, [`src/electron/electron/core.cljs:40`](../../../../../src/electron/electron/core.cljs)) and the desktop app's own `configs.edn` (Electron's user-data folder, [`src/electron/electron/configs.cljs:10-12`](../../../../../src/electron/electron/configs.cljs)) live outside the graph folder and are in no Logseq backup.
- ❌ There are no `pages/`, `journals/` or separate `logseq/` config folders in a DB graph.

> Removed claim: earlier revisions listed "SQLite database file (`<graph-name>.db`)" and a "Transit export file (`.edn` backup)". The file is `db.sqlite` inside the graph folder. EDN and debug transit are two separate exports, and the app itself says of EDN: "Don't rely on this as a primary backup." ([`src/resources/dicts/en.edn:635`](../../../../../src/resources/dicts/en.edn)).

---

## File-Based Graph Backups

> Removed section. File-based graphs can no longer be opened: the graph list keeps only DB graphs ("use `config/db-based-graph?` to avoid loading old file graphs", [`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)). File Sync was deleted in commit `bcc478b5f7` (2025-12-29). The Git setup, commit scripts, Task Scheduler batch file, "move the graph into iCloud/OneDrive/Dropbox" steps, `logseq/.recycle/` and `logseq/bak/` advice, and the "JSON / Markdown / EDN" export menu all applied to file graphs. The Export dialog has no JSON entry ([`src/main/frontend/components/export.cljs:125-153`](../../../../../src/main/frontend/components/export.cljs)), and no "Export on Schedule" plugin is part of Logseq.

**If you still have a file-graph folder:** Logseq only reads it as input, through ⋯ → **Import** → **File to DB graph**. That import creates a new DB graph ([`src/main/frontend/components/imports.cljs:597-611`](../../../../../src/main/frontend/components/imports.cljs)). Keep a plain copy of the old folder with any file-backup tool until you are happy with the imported graph. See [Import and Export Guide](./import-export.md#from-a-logseq-file-graph-file-to-db).

---

## Database Graph Backups

### Method 1: Automatic Desktop Backups (built in)

**Best For:** Everyone using the desktop app. Nothing to set up.

**How it works:**

- **No setting to turn on.** When a window opens a graph, the app registers it and starts a one-hour timer ([`src/electron/electron/handler.cljs:371-383`](../../../../../src/electron/electron/handler.cljs) → [`src/electron/electron/db.cljs:146-154`](../../../../../src/electron/electron/db.cljs), timer at [`:128-136`](../../../../../src/electron/electron/db.cljs)).
- **Hourly.** Each tick snapshots every graph that is open in a window ([`db.cljs:105-126`](../../../../../src/electron/electron/db.cljs)). A snapshot is skipped if the newest automatic backup is less than an hour old ([`db.cljs:59-61`](../../../../../src/electron/electron/db.cljs); [`graph_backup.cljs:233-240`, `:253-257`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)).
- **Keeps 12.** After each snapshot, automatic backups beyond the newest 12 are deleted ([`db.cljs:15-18`](../../../../../src/electron/electron/db.cljs); [`graph_backup.cljs:176-190`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)). The interval and the count are constants in source, not settings.
- **Only open graphs.** A graph with no open window gets no new backups, and the worker refuses with "graph not opened" ([`src/main/frontend/worker/db_core.cljs:985-987`](../../../../../src/main/frontend/worker/db_core.cljs)).

**Layout:** each backup is a folder named `<graph>-<UTC timestamp>`, for example `my-graph-20260911T143000Z`. It contains `db.sqlite` and a `metadata.edn` map with `:schema-version` (1), `:name`, `:repo`, `:source` (here `:electron-auto`), `:created-at-ms` and `:db-path` ([`graph_backup.cljs:13`, `:77-101`, `:221-231`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)). If the name is taken, `-1`, `-2`, … is appended rather than overwriting ([`:192-211`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)). If a snapshot fails, its temporary file and the empty folder are removed ([`:213-219`, `:286-288`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)).

Pruning and the one-hour check read `:source` and `:created-at-ms` from `metadata.edn` ([`:169-190`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)). A backup folder without that file is never pruned.

```
~/logseq/graphs/my-graph/backup/
├── my-graph-20260911T130000Z/
│   ├── db.sqlite
│   └── metadata.edn
└── my-graph-20260911T140000Z/
    ├── db.sqlite
    └── metadata.edn
```

**Consistency:** the snapshot is taken from the open database. The worker checkpoints the WAL, then runs SQLite's backup API on the live connection ([`db_core.cljs:980-994`](../../../../../src/main/frontend/worker/db_core.cljs); [`deps/db/src/logseq/db/sqlite/backup.cljs:35-44`](../../../../../deps/db/src/logseq/db/sqlite/backup.cljs)). It writes to a temporary file and renames it into place ([`graph_backup.cljs:258-271`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)), so a finished `backup/…/db.sqlite` is a complete file you can copy while Logseq runs.

**Find it:** ⋯ (**More**) → **Export graph**. At the bottom the desktop dialog says "Hourly backups are enabled for this graph, open backups folder for this graph."; the link opens `<graph folder>/backup` ([`components/export.cljs:155-164`](../../../../../src/main/frontend/components/export.cljs); [`en.edn:672`, `:675`](../../../../../src/resources/dicts/en.edn); path from [`frontend/config.cljs:333-335`](../../../../../src/main/frontend/config.cljs)).

> **Note:** 12 hourly snapshots cover only about the last 12 hours the graph was open. They protect against a recent mistake, not a problem you notice a week later. Copy them somewhere else as well (Methods 4-6).

---

### Method 2: Manual Snapshot (Desktop, `Mod+S`)

**Best For:** A checkpoint before a risky change (bulk edit, plugin install, import).

**Steps:**

1. In the desktop app, press `mod+s` (Cmd+S on macOS, Ctrl+S elsewhere). The command is described as "Save the current db to the disk (~/logseq/graphs/your-current-graph)" and is inactive outside the desktop app ([`src/main/frontend/modules/shortcut/config.cljs:376-378`](../../../../../src/main/frontend/modules/shortcut/config.cljs); [`en.edn:394`](../../../../../src/resources/dicts/en.edn)).
2. The notification "DB backup successfully." appears ([`src/main/frontend/persist_db.cljs:429-440`](../../../../../src/main/frontend/persist_db.cljs)).

**What it does:** it writes a snapshot into the same `backup/` folder with `:source :electron-manual` ([`handler/events.cljs:290-294`](../../../../../src/main/frontend/handler/events.cljs); [`electron/handler.cljs:256-259`](../../../../../src/electron/electron/handler.cljs); [`db.cljs:43-47`](../../../../../src/electron/electron/db.cljs)). Manual snapshots are **not throttled and never pruned**, because the one-hour throttle and the 12-copy limit apply only to automatic backups ([`db.cljs:59-61`](../../../../../src/electron/electron/db.cljs); pruning filters by source at [`graph_backup.cljs:183-186`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)). Delete old ones yourself.

---

### Method 3: Browser Backup Folder (web app)

**Best For:** Using Logseq in a desktop Chromium browser.

**Setup:**

1. ⋯ → **Export graph**. Under **Schedule backup**, click **Set backup folder first** and choose a folder ([`components/export.cljs:63-91`](../../../../../src/main/frontend/components/export.cljs); [`handler/export.cljs:214-227`](../../../../../src/main/frontend/handler/export.cljs); labels at [`en.edn:676-677`](../../../../../src/resources/dicts/en.edn)).
2. The dialog now shows **Backup folder:** with the folder name, "Backup will be created every hour.", and a **Backup now** button ([`export.cljs:68-111`](../../../../../src/main/frontend/components/export.cljs); [`en.edn:670`, `:673-674`](../../../../../src/resources/dicts/en.edn)).
3. Click **Backup now**. You get "Backup successful!" or, if nothing changed, "Graph has not been updated since last export." ([`export.cljs:100-106`](../../../../../src/main/frontend/components/export.cljs); [`en.edn:625`, `:649`](../../../../../src/resources/dicts/en.edn)).

**How it works:**

- It writes `<chosen folder>/<graph name>/db.sqlite`. Before each new copy, the previous `db.sqlite` is moved to `<graph name>/backups/<epoch ms>.db.sqlite`, and only the newest 12 of those are kept ([`handler/export.cljs:204-212`, `:247-268`](../../../../../src/main/frontend/handler/export.cljs)). Note the folder is `backups` (plural) here, unlike the desktop app's `backup`.
- It runs every hour for the current graph ([`handler/export.cljs:231`, `:287-299`](../../../../../src/main/frontend/handler/export.cljs)). The timer starts when a graph is opened ([`handler/events.cljs:280-285`](../../../../../src/main/frontend/handler/events.cljs)), and a backup also runs when you switch graphs ([`events.cljs:155-163`](../../../../../src/main/frontend/handler/events.cljs)).
- Assets are not copied.
- Click the **×** (**Cancel backup**) next to the folder name to stop ([`export.cljs:72-85`](../../../../../src/main/frontend/components/export.cljs); [`en.edn:671`](../../../../../src/resources/dicts/en.edn)).
- It needs the File System Access API. Other browsers show "Your browser doesn't support The File System Access API, please switch to a Chromium-based browser." ([`export.cljs:66`, `:112-117`](../../../../../src/main/frontend/components/export.cljs); [`en.edn:679`](../../../../../src/resources/dicts/en.edn)).
- The section is not shown on mobile, and the timer does not run in the native mobile app ([`export.cljs:165-169`](../../../../../src/main/frontend/components/export.cljs); [`handler/export.cljs:289-291`](../../../../../src/main/frontend/handler/export.cljs)).

---

### Method 4: Export SQLite DB or SQLite + Assets

**Best For:** A full copy you move to another disk or cloud storage. Use the zip when you need assets too.

**Steps:**

1. ⋯ → **Export graph** ([`components/header.cljs:227-230`](../../../../../src/main/frontend/components/header.cljs)).
2. Choose one:
   - **Export SQLite DB**: "Primary way to backup graph's content to a single .sqlite file."
   - **Export both SQLite DB and assets**: "Primary way to backup graph's content and assets to a .zip file."
     ([`export.cljs:125-133`](../../../../../src/main/frontend/components/export.cljs); [`en.edn:661`, `:666`, `:678`, `:680`](../../../../../src/resources/dicts/en.edn))
3. Move the file to your backup location.

**Where the file goes:**

- **Desktop app:** into the graph's own `export/` folder. The notification shows the path: "SQLite DB exported to {1}." or "ZIP exported to {1}." ([`handler/export.cljs:78-86`, `:118-120`, `:146-154`](../../../../../src/main/frontend/handler/export.cljs); [`electron/handler.cljs:261-263`](../../../../../src/electron/electron/handler.cljs); [`db.cljs:98-103`](../../../../../src/electron/electron/db.cljs); [`en.edn:662`, `:668`](../../../../../src/resources/dicts/en.edn)). The file names end in `_<Unix seconds>.sqlite` / `_<Unix seconds>.zip` ([`handler/export.cljs:50-54`](../../../../../src/main/frontend/handler/export.cljs)). **The export stays on the same disk as the graph**, so copy it off the machine yourself.
- **Browser:** a save dialog where the browser supports one, otherwise a download ([`handler/export.cljs:121-124`, `:155-169`](../../../../../src/main/frontend/handler/export.cljs)).

**Other entries in the dialog are not backups:**

- **Export EDN file**: "Don't rely on this as a primary backup." ([`en.edn:635`](../../../../../src/resources/dicts/en.edn))
- **Export as standard Markdown (no block properties)**: drops block properties, and cannot be imported back as a graph. The only Markdown import requires a Logseq file graph with `logseq/config.edn` ([`imports.cljs:495-498`](../../../../../src/main/frontend/components/imports.cljs)).
- **Export public pages** and **Export debug transit file**: for publishing and bug reports.

See [Import and Export Guide](./import-export.md#export-formats) for details.

> Removed claim: earlier revisions offered "Choose **EDN (Transit format)**" and a plugin script that called `logseq.api` `invoke('export-db', …)` on a `logseq.App.onScheduler('daily-backup', …)` schedule. EDN and debug transit are separate entries, and no `onScheduler` or export-db call exists in the plugin API (`libs/src/`, `src/main/logseq/api/`).

#### Mobile

The mobile app has an Export route ([`src/main/mobile/routes.cljs:19-22`](../../../../../src/main/mobile/routes.cljs)), but its only link, in the mobile header, is commented out ([`src/main/mobile/components/header.cljs:65`](../../../../../src/main/mobile/components/header.cljs)). No other link to it was found. No automatic backup runs in the native mobile app (Methods 1 and 3 are desktop and browser only).

---

### Method 5: Command-Line Backups

**Best For:** Scheduled backups and scripting.

The CLI in `cli/` is published as the npm package `@logseq/cli`, and its command is `logseq` ([`scripts/prepare-cli-package.mjs:126-133`](../../../../../scripts/prepare-cli-package.mjs); [`docs/cli/logseq-cli.md:3`](../../../../../docs/cli/logseq-cli.md)). It has four backup commands ([`cli/lib/cli_parse.ml:175-187`, `:843-871`](../../../../../cli/lib/cli_parse.ml); [`cli/lib/command_registry.ml:225-242`](../../../../../cli/lib/command_registry.ml)):

| Command | Options |
| ------- | ------- |
| `graph backup list` | `--graph` |
| `graph backup create` | `--graph`, optional `--name` (a label) |
| `graph backup restore` | `--graph`, `--src <backup name>` (required), `--dst <new graph name>` (required) |
| `graph backup remove` | `--graph`, `--src <backup name>` (required) |

- `create` snapshots the graph through the db-worker backup API. It writes into the same `<graph folder>/backup/` tree, named `<graph>-<label>-<UTC timestamp>` (or `<graph>-<UTC timestamp>` without `--name`; `/`, `\` and `:` become `-`), with `:source :cli` in `metadata.edn`. If the snapshot fails, the new backup folder is removed ([`cli/lib/graph.ml:131-155`, `:466-473`, `:796-876`](../../../../../cli/lib/graph.ml)).
- CLI backups are **not pruned** by the desktop's 12-copy limit, which only counts `:electron-auto` backups ([`graph_backup.cljs:183-186`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)). Use `graph backup remove`.
- `list` shows each backup's `name`, `created-at` (the modification time of its `db.sqlite`), `size-bytes`, and `source` when `metadata.edn` has one ([`graph.ml:702-751`](../../../../../cli/lib/graph.ml)). With `--output json` the names are at `data.backups[].name` ([`cli-e2e/spec/non_sync_cases.edn:184`](../../../../../cli-e2e/spec/non_sync_cases.edn)).
- `restore` runs a SQLite import of the backup's `db.sqlite` into a new graph named by `--dst`. An unknown name fails with "backup not found: …" ([`graph.ml:1049-1060`](../../../../../cli/lib/graph.ml)).
- `remove` deletes the folder `<graph folder>/backup/<src>` and everything in it. The name is joined to that path without further checks, so pass exactly a name printed by `graph backup list` ([`graph.ml:469-470`, `:753-758`](../../../../../cli/lib/graph.ml)).

The whole lifecycle (create → list → restore → remove) is an end-to-end CLI test case ([`non_sync_cases.edn:176-200`](../../../../../cli-e2e/spec/non_sync_cases.edn)):

```bash
logseq graph backup create --graph my-graph --name nightly
logseq --output json graph backup list --graph my-graph
logseq graph backup restore --graph my-graph --src my-graph-nightly-20260911T020000Z --dst my-graph-restored
logseq graph backup remove --graph my-graph --src my-graph-nightly-20260911T020000Z
```

---

### Method 6: Copying the Files Yourself (Time Machine, rsync, external drives)

**Best For:** Adding Logseq to an existing machine-wide backup.

**What to know about a live `db.sqlite`:** Logseq opens the database with `locking_mode=exclusive` in WAL mode, and turns automatic checkpointing off ([`db_core.cljs:585-592`, `:740-742`](../../../../../src/main/frontend/worker/db_core.cljs)). It checkpoints about 2 seconds after writes and when it closes the graph ([`db_core.cljs:74-75`, `:155-164`, `:471`, `:477-484`](../../../../../src/main/frontend/worker/db_core.cljs)). Until then, recent changes live in the `-wal` file beside `db.sqlite`. A copy of `db.sqlite` taken while Logseq is running can miss them.

**So:**

- ✅ Back up the `backup/` and `export/` folders plus `assets/`. Finished snapshots are complete files.
- ✅ To copy `db.sqlite` itself, quit Logseq first. Also stop any CLI server for the graph (`server stop --graph <name>`, [`non_sync_cases.edn:200`](../../../../../cli-e2e/spec/non_sync_cases.edn)). Closing the graph checkpoints the WAL into `db.sqlite` ([`db_core.cljs:477-484`](../../../../../src/main/frontend/worker/db_core.cljs)). Then:

  ```bash
  # macOS/Linux
  cp ~/logseq/graphs/my-graph/db.sqlite ~/backups/my-graph-$(date +%Y%m%d).sqlite
  ```

  ```powershell
  # Windows (PowerShell)
  Copy-Item "$env:USERPROFILE\logseq\graphs\my-graph\db.sqlite" "C:\backups\my-graph-$(Get-Date -Format yyyyMMdd).sqlite"
  ```

- ❌ Don't move a graph folder out of the graphs directory into a cloud-drive folder. The desktop app lists DB graphs only from the graphs directory ([`graph.cljs:123-140`](../../../../../deps/common/src/logseq/common/graph.cljs)), so a moved graph disappears from the list. Separately, cloud clients that copy a database while it is being written are a general SQLite hazard.

> Removed claim: the graph file paths `~/Library/Application Support/Logseq/graphs/<graph-name>.db`, `%APPDATA%\Logseq\graphs\…` and `~/.config/Logseq/graphs/…`. The graph is `~/logseq/graphs/<graph folder>/db.sqlite` on every desktop OS (see [Where your graph lives](#where-your-graph-lives)).

---

### Sync is not a backup

Logseq Sync keeps a graph in step across devices. It does not keep old versions.

- **Deletions replicate.** Other clients replay `:delete-blocks`, `:delete-page` and `:recycle-delete-permanently` ([`src/main/frontend/worker/sync/apply_txs.cljs:1500-1525`, `:1551-1570`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs)). Deleting something on one device deletes it everywhere. Keep Methods 1-6 running alongside sync.
- **No version history.** en.edn has no version-history or restore-from-sync strings. The "history" code in the sync worker is undo history ([`apply_txs.cljs:275-279`, `:829`](../../../../../src/main/frontend/worker/sync/apply_txs.cljs)). See also [RTC vs File Sync](../../explanation/comparisons/rtc-vs-file-sync.md).
- **Where it is turned on:** the create-graph dialog offers "Use Logseq Sync?" and then "Encrypt graph data" ([`components/repo.cljs:700-723`](../../../../../src/main/frontend/components/repo.cljs); [`en.edn:745`, `:786`](../../../../../src/resources/dicts/en.edn)). The option appears only for accounts in the `team` or `rtc_2025_07_10` group, with a custom sync server, or in dev builds ([`src/main/frontend/handler/user.cljs:357-362`](../../../../../src/main/frontend/handler/user.cljs)).
- **E2EE is partial and unrecoverable.** Only `:block/title`, `:block/name` and asset bytes are encrypted ([`src/main/frontend/worker/sync/const.cljs:25-27`](../../../../../src/main/frontend/worker/sync/const.cljs); [E2EE Encryption](../../explanation/security/e2ee-encryption.md)). A forgotten password cannot be recovered. "Force reset password" makes "all currently encrypted graph data stored on the server permanently unreadable" ([`en.edn:613`, `:618`](../../../../../src/resources/dicts/en.edn)), though "You will still be able to access the local version of your graph." ([`en.edn:600`](../../../../../src/resources/dicts/en.edn)).

> Removed claim: "Settings → Sync → Enable Sync / Choose plan (free 1GB or paid)", "Settings → Sync → Backup settings → Keep version history: 30 days", "Right-click block → Version history", "Settings → Sync → Restore from backup", "Pause sync / Resume sync / Re-initialize sync" and "Automatic conflict resolution". None of these labels exist in `en.edn`, and there is no sync retention setting. Title conflicts can need manual review (see [RTC vs File Sync](../../explanation/comparisons/rtc-vs-file-sync.md)).

---

## Automated Backup Workflows

### Workflow 1: 3-2-1 Backup Strategy

**Rule:** 3 copies, 2 different media, 1 offsite

**Implementation:**

1. **Copy 1 (hourly, same disk):** automatic desktop backups (Method 1) or the browser backup folder (Method 3).
2. **Copy 2 (daily, external drive):** copy the graph's `backup/` and `assets/` folders (script below).
3. **Copy 3 (weekly, offsite):** **Export both SQLite DB and assets** (Method 4), encrypted and uploaded to cloud storage.

> **Unverified:** this script is generic shell and was not run. Set `GRAPH_DIR` to the encoded folder name (see [Where your graph lives](#where-your-graph-lives)).

```bash
#!/bin/bash
# 3-2-1 backup of a Logseq DB graph (macOS example)
set -euo pipefail

GRAPH_DIR="$HOME/logseq/graphs/my-graph"
BACKUP_ROOT="/Volumes/ExternalDrive/Backups/Logseq"
CLOUD_DIR="$HOME/CloudStorage/Backups/Logseq"
DATE=$(date +%Y-%m-%d)
DEST="$BACKUP_ROOT/$DATE"

mkdir -p "$DEST" "$CLOUD_DIR"

# Copy 2: finished snapshots and assets (safe while Logseq runs)
rsync -a "$GRAPH_DIR/backup/" "$DEST/backup/"
rsync -a "$GRAPH_DIR/assets/" "$DEST/assets/"

# Copy 3: encrypted archive offsite
ARCHIVE="$(mktemp -d)/logseq-$DATE.tar.gz"
tar -czf "$ARCHIVE" -C "$DEST" .
gpg --encrypt --recipient your-email@example.com "$ARCHIVE"
mv "$ARCHIVE.gpg" "$CLOUD_DIR/"
rm -f "$ARCHIVE"

# Keep 30 days of dated copies
find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -mtime +30 -exec rm -rf {} +
find "$CLOUD_DIR" -type f -name 'logseq-*.tar.gz.gpg' -mtime +30 -delete

echo "3-2-1 backup completed: $DATE"
```

---

### Workflow 2: Scheduled CLI Snapshots

Automatic desktop backups need the app open with the graph loaded. A scheduled CLI backup runs without that (Method 5).

> **Unverified:** assembled from the CLI option parser and e2e spec, not run. Schedulers such as launchd and cron run with a short `PATH`, so set `LOGSEQ` to the full path of the installed `logseq` command.

```bash
#!/bin/bash
# save as: ~/scripts/logseq-cli-backup.sh
set -euo pipefail
LOGSEQ=/usr/local/bin/logseq   # output of: command -v logseq
GRAPH=my-graph

"$LOGSEQ" graph backup create --graph "$GRAPH" --name scheduled
```

**Schedule with Launchd (macOS):** create `~/Library/LaunchAgents/com.user.logseq-backup.plist`

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.user.logseq-backup</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/yourname/scripts/logseq-cli-backup.sh</string>
    </array>
    <key>StartInterval</key>
    <integer>86400</integer>
</dict>
</plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.user.logseq-backup.plist
```

**Schedule with Cron (macOS/Linux):**

```bash
# daily at 02:00
0 2 * * * /bin/bash ~/scripts/logseq-cli-backup.sh >> ~/logseq-backup.log 2>&1
```

CLI backups are never pruned automatically. Remove old ones with `graph backup remove --src <name>` (names come from `graph backup list`).

> Removed: "Workflow 3: Pre-Shutdown Backup". It committed a file graph to Git from a `trap … EXIT` in `~/.zshrc`. That trap fires whenever a shell exits, not at shutdown, and Git-versioning Markdown files does not apply to DB graphs.

---

## Recovery Procedures

### Scenario 1: Accidental Deletion

**A page:** deleting a page moves it to **Recycle** ("Node has been moved to Recycle", [`en.edn:1126`](../../../../../src/resources/dicts/en.edn)). The exceptions are tags and properties, which are removed outright, and today's journal, which is emptied rather than deleted ([`deps/outliner/src/logseq/outliner/page.cljs:105-155`](../../../../../deps/outliner/src/logseq/outliner/page.cljs)).

1. ⋯ → **Recycle**. The item appears once the Recycle page exists ([`components/header.cljs:222-225`, `:332-334`](../../../../../src/main/frontend/components/header.cljs); [`handler/page.cljs:45-49`](../../../../../src/main/frontend/handler/page.cljs)).
2. Find the page and click **Restore** ([`en.edn:1823`](../../../../../src/resources/dicts/en.edn)).

"Deleted pages and blocks stay here until restored or automatically garbage collected after 30 days." ([`en.edn:1824`](../../../../../src/resources/dicts/en.edn)). The retention is 30 days, checked at most once a day as transactions run ([`deps/outliner/src/logseq/outliner/recycle.cljs:13-14`, `:273-298`](../../../../../deps/outliner/src/logseq/outliner/recycle.cljs); [`worker/handler/transaction.cljs:28-38`](../../../../../src/main/frontend/worker/handler/transaction.cljs)).

**Blocks:** deleting blocks removes them and their children outright ([`deps/outliner/src/logseq/outliner/core.cljs:431-443`, `:1011-1051`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)). They do not go to Recycle. The function that would recycle blocks, `recycle-blocks-tx-data` ([`recycle.cljs:127`](../../../../../deps/outliner/src/logseq/outliner/recycle.cljs)), is called only from tests. To get blocks back:

1. Undo right away with `mod+z` ([`shortcut/config.cljs:235-236`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).
2. Otherwise restore a recent backup into a new graph (Scenario 2) and copy the blocks back.

> Removed claim: "Right-click nearby block → Version history → Find deleted block in timeline → Restore this version". Logseq has no block version history. The only history strings in `en.edn` are "Status history" and property-history labels.

---

### Scenario 2: Corrupted or Broken Graph

Logseq restores a backup by **importing it as a new graph**. There is no in-app "restore from backup" command. `list-backups` in [`graph_backup.cljs:161-167`](../../../../../src/main/logseq/db_worker/graph_backup.cljs) has no caller in the app.

**Steps:**

1. **Find the backup file.**
   - Desktop snapshot: ⋯ → **Export graph** → "open backups folder for this graph", then pick the newest `backup/<graph>-<timestamp>/db.sqlite`.
   - Desktop export: `<graph folder>/export/…sqlite` or `…zip`.
   - Browser backup folder: `<folder>/<graph name>/db.sqlite`, or an older `backups/<epoch ms>.db.sqlite`.
2. ⋯ → **Import** ([`components/header.cljs:232-235`](../../../../../src/main/frontend/components/header.cljs)).
3. Choose **SQLite** for a `.sqlite` file, or **SQLite + assets (.zip)** for a zip export ([`imports.cljs:572-595`](../../../../../src/main/frontend/components/imports.cljs); [`en.edn:950-951`](../../../../../src/resources/dicts/en.edn)).
4. Enter a **new** graph name. An empty name or an existing one is rejected ([`imports.cljs:74-110`](../../../../../src/main/frontend/components/imports.cljs); [`en.edn:923`, `:929`](../../../../../src/resources/dicts/en.edn)).
5. Logseq creates the graph from the file and switches to it ([`src/main/frontend/handler/db_based/import.cljs:104-121`](../../../../../src/main/frontend/handler/db_based/import.cljs)). The zip import also copies the assets and reports how many were imported or skipped ([`import.cljs:123-172`](../../../../../src/main/frontend/handler/db_based/import.cljs)).

**Command line:** `graph backup restore --graph <old> --src <backup name> --dst <new>` runs the same SQLite import from a `backup/` snapshot (Method 5).

**Assets:** a plain SQLite import brings no assets, and neither do snapshots.

> **Unverified:** that copying the old graph's `assets/` folder into the new graph's folder makes the images and files resolve. Not tested. The zip route above is the verified way to restore assets.

> **Unverified:** copying a snapshot's `db.sqlite` over a graph's own `db.sqlite`. No Logseq code restores this way and it was not tested. Use Import or `graph backup restore` instead. If you try it anyway: quit Logseq and stop any CLI server for the graph; move the current `db.sqlite`, and any `db.sqlite-wal` and `db.sqlite-shm` beside it, out of the folder instead of deleting them; then copy the snapshot in. The graph's `search/` and `client-ops-/` databases are not in the snapshot and stay as they were for the replaced file (see [Where your graph lives](#where-your-graph-lives)). How Logseq handles that mismatch was not checked.

> Removed claim: "⋯ → Import graph → EDN file → Create new graph from backup". The entry is **EDN to DB graph**, and EDN is not a primary backup.

---

### Scenario 3: Complete Data Loss (new or wiped machine)

**Recovery Steps:**

1. **Off-machine copies first.** Take the newest `db.sqlite` snapshot or zip export from your external drive or cloud storage, and import it as in Scenario 2.
2. **Machine-wide backups** (Time Machine, File History): restore `~/logseq/graphs/<graph folder>/` with Logseq quit, then check whether the graph appears in the list ([`graph.cljs:128-140`](../../../../../deps/common/src/logseq/common/graph.cljs)). If you have to choose, prefer its `backup/` snapshots over a `db.sqlite` that may have been copied while in use.
3. **Logseq Sync:** after signing in, remote graphs are listed under "Remote graphs:" on the graphs screen ([`components/repo.cljs:350-358`](../../../../../src/main/frontend/components/repo.cljs); [`en.edn:768`](../../../../../src/resources/dicts/en.edn)). Sync holds only the current state, with deletions applied.

> **Unverified:** the click-by-click steps for downloading a remote graph onto a new device were not traced.

> Removed claim: "Settings → Sync → Download graph", "git clone" of the graph and "Rebuild graph from scratch from `.edn` exports". There is no such Sync settings entry, and Git applies only to file graphs.

---

## Best Practices

### Backup Checklist

**Daily:**

- [ ] Nothing to do for automatic desktop backups. Occasionally open the backups folder and check that new timestamps appear.
- [ ] Copy `backup/` and `assets/` off the machine (Workflow 1), or run a CLI backup (Workflow 2).
- [ ] Check backup logs for errors.

**Weekly:**

- [ ] **Export both SQLite DB and assets** and move the zip offsite.
- [ ] Test a restore into a throwaway graph (below).

**Monthly:**

- [ ] Remove old `Mod+S` and CLI snapshots from `backup/`. They are never pruned automatically.
- [ ] Verify the offsite copy is accessible.
- [ ] Update backup scripts if needed.

### Testing Your Backups

**Test Restore Process:**

1. ⋯ → **Import** → **SQLite + assets (.zip)** (or **SQLite**).
2. Name the new graph `MyGraph-Test`.
3. Verify pages, properties and recent edits are present.
4. Check that assets load (zip only).
5. Delete the test graph. The app warns: "⚠️ Notice that we can't recover this graph after being deleted. Make sure you have backups before deleting it." ([`en.edn:740`](../../../../../src/resources/dicts/en.edn)). Make sure you delete the test graph, not the original.

**Why Test?**

- Corrupted backups may not be discovered until needed
- Automatic snapshots contain no assets, so a test shows what the zip adds
- Validates the entire backup pipeline

---

### Security Considerations

**Encrypt backup files:** snapshots and exports are unencrypted SQLite or zip files. Encrypt them before storing them offsite:

```bash
# Encrypt a backup with GPG
gpg --encrypt --recipient your-email@example.com my-graph_1757590000.zip

# Decrypt when needed
gpg --decrypt my-graph_1757590000.zip.gpg > my-graph_1757590000.zip
```

**Logseq Sync E2EE:** choose **Encrypt graph data** when creating a synced graph ([`repo.cljs:713-723`](../../../../../src/main/frontend/components/repo.cljs)). Keep the password safe, because it cannot be recovered ([`en.edn:618`](../../../../../src/resources/dicts/en.edn)). Only titles, page names and assets are encrypted ([`sync/const.cljs:25-27`](../../../../../src/main/frontend/worker/sync/const.cljs)).

> Removed: "Settings → Sync → Enable End-to-end encryption" (encryption is chosen per graph at creation) and "Git private repositories" (file-graph advice).

---

## Backup Strategy by Use Case

### Single Desktop

**Recommended:**

- Primary: automatic desktop backups (Method 1), plus `Mod+S` before risky changes
- Secondary: daily copy of `backup/` and `assets/` to an external drive
- Tertiary: weekly **Export both SQLite DB and assets**, encrypted, offsite

**Why:** Snapshots cover recent mistakes, and the offsite zip covers disk loss, including assets.

---

### Desktop + Sync Across Devices

**Recommended:**

- Primary: automatic desktop backups on at least one desktop
- Secondary: Logseq Sync for access across devices
- Tertiary: weekly zip export offsite

**Why:** Sync replicates deletions, so it can't replace dated copies.

---

### Browser Only

**Recommended:**

- Primary: browser backup folder in a Chromium browser (Method 3)
- Secondary: weekly **Export both SQLite DB and assets**, because the backup folder has no assets

**Why:** The browser's own storage is not reachable as a file.

---

### Mobile

**Recommended:**

- Primary: Logseq Sync to a desktop that makes automatic backups
- Secondary: periodic zip exports from that desktop

**Why:** No automatic backup runs in the native mobile app, and no link to the Export screen was found there ([Mobile](#mobile)).

---

### Automation / Power User

**Recommended:**

- Primary: scheduled `graph backup create` (Workflow 2)
- Secondary: pruning with `graph backup remove`
- Tertiary: offsite copy of `backup/` and `assets/` (Workflow 1)

**Why:** Backups run on a schedule, whether or not the app is open.

---

## Troubleshooting

**Problem: the `backup/` folder is empty or has no recent snapshot**

Automatic backups cover only graphs open in a window, and run on a one-hour timer that starts when a graph is opened ([`db.cljs:105-136`](../../../../../src/electron/electron/db.cljs)). Leave the graph open for an hour, or press `mod+s` for an immediate snapshot.

**Problem: `backup/` keeps growing**

Only automatic backups are pruned to 12. `Mod+S` (`:electron-manual`) and CLI (`:cli`) snapshots stay until you delete them ([`graph_backup.cljs:183-186`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)). Each backup's `metadata.edn` shows its source.

**Problem: "Please specify another name as another graph with this name already exists!"**

A restore always creates a new graph. Pick an unused name ([`en.edn:929`](../../../../../src/resources/dicts/en.edn)).

**Problem: "Zip missing db.sqlite. Please check the archive structure."**

The **SQLite + assets (.zip)** import needs a `db.sqlite` entry in the zip ([`en.edn:960`](../../../../../src/resources/dicts/en.edn)). Use a zip made by **Export both SQLite DB and assets**.

**Problem: "DB backup failed, please go to Export and specify a backup folder."**

The browser lost its handle to your backup folder. Open ⋯ → **Export graph** and set the folder again ([`handler/export.cljs:271-274`](../../../../../src/main/frontend/handler/export.cljs); [`en.edn:631`](../../../../../src/resources/dicts/en.edn)).

**Problem: "Your browser doesn't support The File System Access API"**

The browser backup folder needs a Chromium-based browser ([`en.edn:679`](../../../../../src/resources/dicts/en.edn)). Use **Export SQLite DB** in the meantime.

**Problem: a copied `db.sqlite` is missing recent edits**

It was copied while Logseq had changes in the `-wal` file (Method 6). Quit Logseq before copying, or use a `backup/` snapshot.

**Problem: Cannot find database file**

```bash
# macOS
open ~/logseq/graphs/

# Windows
explorer %USERPROFILE%\logseq\graphs\

# Linux
xdg-open ~/logseq/graphs/
```

> Removed: "Git push fails", "Logseq Sync stuck → Pause / Resume / Re-initialize sync" and "EDN export file too large → gzip". The first is file-graph Git advice, the sync controls do not exist, and EDN is not the backup format.

---

## Next Steps

**Implement Your Strategy:**

1. Confirm automatic backups appear in the backups folder
2. Add an off-machine copy (external drive or offsite zip)
3. Test a restore into a throwaway graph
4. Document your backup process
5. Schedule regular backup verification

**Related Guides:**

- [Import and Export Guide](./import-export.md)
- [RTC vs File Sync](../../explanation/comparisons/rtc-vs-file-sync.md)
- [File vs Database Graphs](../../explanation/concepts/file-vs-db.md)

---

**Questions or Issues?** Visit [Logseq Forum](https://discuss.logseq.com) or [Discord](https://discord.gg/KpN4eHY).
