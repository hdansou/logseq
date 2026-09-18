# The Case of the SQLite Backup System: A Detective's Investigation

> **Last revised:** 2026-05-28
> **Previously revised:** 2026-03-10
> **Change summary:** Git integration was completely removed. The system now uses a structured per-backup-directory scheme managed by the db-worker, with hourly auto-backup (Electron + Web). All git-related documentation has been replaced.

## Case File: Logseq Database Mode Backup & Versioning System

**Investigation Status:** SOLVED
**Case Complexity:** High
**Technical Domain:** Database Persistence, SQLite Backup, DB-Worker Architecture

---

## Executive Summary

Our investigation reveals that Logseq's DB-mode backup system has been substantially redesigned. **Git integration has been completely removed.** The system now uses a structured directory-per-backup scheme backed by SQLite's native backup API, executed through the db-worker process. Auto-backup fires every hour on both Electron and Web. Backups carry typed metadata (source, creation time) enabling source-scoped pruning and throttle control. This document reflects the current codebase as of 2026-05-28.

---

## The Crime Scene: System Architecture

### Location of Evidence

The backup system operates across these locations on Electron:

1. **Live Database**: `~/.logseq/graphs/<encoded-graph-dir>/`
   - Primary SQLite database used by the db-worker
   - Managed by `electron.db` namespace

2. **Backup Root**: `~/.logseq/graphs/<encoded-graph-dir>/backup/`
   - Each backup is its own subdirectory (not a single rolling file)
   - Subdirectory name is a `build-backup-name` key, URL-encoded
   - Managed by `logseq.db-worker.graph-backup` namespace

3. **Per-Backup Directory**: `backup/<backup-name>/`
   - `db.sqlite` — SQLite copy via native backup API
   - `metadata.edn` — Source, creation timestamp, schema version

4. **Emergency Write Backups**: `<graph-dir>/backups/<relative-path>/`
   - Created only when an IPC `:writeFile` operation fails
   - Rolling window of 6 files per path (hourly dedup)
   - Managed by `electron.backup-file` namespace

> **⚠️ Git integration has been removed.** There is no longer an `electron.git` namespace, no `~/.logseq/git/` directory, and no git auto-commit feature. All git-related settings (`:git/disable-auto-commit?`, `:git/auto-commit-seconds`, `:git/commit-on-close?`) have been removed from state and UI.

### Key Suspects (Components)

```
electron.db (The Backup Orchestrator)
├── backup-db-via-worker!     Backup via db-worker IPC (primary path)
├── backup-db!                Backup via direct sqlite-backup library
├── run-auto-backup!          Fires for all active window/repo pairs
├── reconcile-auto-backup-timer!  Starts/stops 1-hour interval timer
└── sync-auto-backup-repo!    Called on graph open/close to track active repos

logseq.db-worker.graph-backup (The Backup Policy Engine)
├── <create-backup!           Orchestrates throttle check + snapshot + metadata write
├── build-backup-name         Creates <graph-name>-[label-]<UTC-timestamp> keys
├── prune-backups!            Source-scoped pruning (keeps N most recent by source)
├── list-backups              Returns backup list with name/created-at/size-bytes/source
└── remove-backup!            Deletes a backup directory

electron.backup-file (The Emergency Safety Net)
├── backup-file               Creates emergency backups on write failure
├── truncate-daily-versioned-files!  Hourly + daily dedup retention strategy
└── too-soon?                 Rate-limit: skips if a backup exists within last hour

frontend.persist-db (The Manual Trigger)
└── export-current-graph!     User-triggered backup; calls IPC :db-export with force-backup?=true

frontend.handler.export (The Web Scheduler)
└── auto-db-backup!           Hourly OPFS backup for web graphs (if backup folder configured)
```

---

## The Timeline: How It All Works

### Act I: Graph Opens — Auto-backup Timer Starts

**File:** `src/electron/electron/db.cljs` — `sync-auto-backup-repo!`, `reconcile-auto-backup-timer!`

```clojure
(defn sync-auto-backup-repo!
  [window-id repo]
  (swap! *auto-backup
         (fn [state]
           (if (seq repo)
             (assoc-in state [:window->repo window-id] repo)
             (update state :window->repo dissoc window-id))))
  (reconcile-auto-backup-timer!)
  nil)

(defn- reconcile-auto-backup-timer!
  []
  (let [{:keys [interval-id]} @*auto-backup
        has-repos? (seq (active-repo-window-ids))]
    (cond
      (and has-repos? (nil? interval-id))
      (let [id (js/setInterval (fn [] (run-auto-backup!))
                               backup-interval-ms)]    ;; 3,600,000 ms = 1 hour
        (swap! *auto-backup assoc :interval-id id))
      ...)))
```

**Evidence:**

- When any window opens a repo, it registers via `sync-auto-backup-repo!`
- `reconcile-auto-backup-timer!` starts the interval **only if** at least one repo is active
- When all repos are closed, the interval is cleared
- Timer interval: **1 hour** (`backup-interval-ms = (* 60 60 1000)`)

### Act II: The Hourly Auto-backup Fires

**File:** `src/electron/electron/db.cljs` — `run-auto-backup!`, `backup-db-via-worker!`

```clojure
(defn run-auto-backup!
  []
  (p/all
   (for [[repo window-id] (active-repo-window-ids)]
     (-> (backup-db-via-worker! repo window-id {})
         (p/catch (fn [error]
                    (log/warn :electron/auto-db-backup-failed
                              {:repo repo :error error})
                    nil))))))

(defn backup-db-via-worker!
  [db-name window-id opts]
  (<create-graph-backup!
   db-name
   opts
   (fn [dst-path]
     (p/let [runtime (db-worker/ensure-runtime! db-name window-id)]
       (cli-transport/invoke runtime
                             :thread-api/backup-db-sqlite
                             [db-name dst-path])))))
```

**The Backup Chain:**

```
1. 1-hour timer fires
   ↓
2. run-auto-backup! iterates all active (repo, window-id) pairs
   ↓
3. backup-db-via-worker! calls <create-graph-backup!
   ↓
4. <create-graph-backup! calls graph-backup/<create-backup!
   with {:source :electron-auto
         :throttle-ms 3,600,000
         :keep-versions 12}
   ↓
5. graph-backup checks throttle: skip if latest :electron-auto backup < 1h old
   ↓
6. snapshot! fn: db-worker invoked via cli-transport with :thread-api/backup-db-sqlite
   ↓
7. db-worker uses SQLite native backup API → writes to backup/<name>/db.sqlite
   ↓
8. metadata.edn written: {:source :electron-auto :created-at-ms ... :repo ...}
   ↓
9. prune-backups! removes :electron-auto backups beyond keep-versions=12
```

**Key Evidence:**

- **Backup is throttled**: If a backup with source `:electron-auto` already exists within the last hour, it is skipped
- **Keeps 12 versions** for auto-backup (not 6 — that was the old design)
- **Source-scoped pruning**: Manual backups (`:electron-manual`) and auto backups (`:electron-auto`) are pruned independently
- **No change detection**: There is no `graph-has-changed?` guard; the throttle is the only guard against excessive backups

### Act III: The Manual Save (User Triggered)

**File:** `src/main/frontend/persist_db.cljs` — `export-current-graph!`  
**File:** `src/main/frontend/handler/events.cljs` — `:graph/save-db-to-disk` handler

```clojure
;; Event dispatch path (events.cljs:249)
(defmethod handle :graph/save-db-to-disk [[_ _opts]]
  (persist-db/export-current-graph! :succ-notification? true))

;; persist_db.cljs
(defn export-current-graph!
  [& {:keys [succ-notification?]}]
  (when (util/electron?)
    (when-let [repo (state/get-current-repo)]
      (->
       (p/do!
         (ipc/ipc :db-export repo true)   ;; true = force-backup?
         (when succ-notification?
           (state/pub-event!
            [:notification/show {:content "DB backup successfully."
                                 :status :success}])))
       (p/catch ...)))))
```

**IPC Handler** (`handler.cljs:262`):

```clojure
(defmethod handle :db-export [window [_ repo force-backup?]]
  (when-let [repo (canonical-repo repo)]
    (db/ensure-graph-dir! repo)
    (db/backup-db-via-worker! repo (.-id window) {:force-backup? force-backup?})))
```

**Evidence:**

- Manual save sends `force-backup? = true`, which sets `:source :electron-manual`
- `:electron-manual` backups bypass the throttle check (`force-backup?` overrides `too-soon?`)
- User sees a success notification; errors are shown as sticky error notifications
- Triggered by: keyboard shortcut → `:graph/db-save-shortcut` → `:graph/save-db-to-disk`

### Act IV: The Backup Directory Structure

**File:** `src/main/logseq/db_worker/graph_backup.cljs`

Each backup lives in its own directory:

```
~/.logseq/graphs/
└── <encoded-graph-dir>/
    ├── db.sqlite               ← live database
    └── backup/
        ├── <backup-name-1>/
        │   ├── db.sqlite       ← SQLite backup copy
        │   └── metadata.edn   ← {:schema-version 1
        │                          :name "my-graph-20260528T134500Z"
        │                          :repo "my-graph"
        │                          :source :electron-auto
        │                          :created-at-ms 1748xxx
        │                          :db-path "/path/to/db.sqlite"}
        └── <backup-name-2>/
            ├── db.sqlite
            └── metadata.edn
```

**Backup naming** (`build-backup-name`):

```clojure
;; Without label:
;; <graph-name>-20260528T134500Z

;; With label (e.g. "manual"):
;; <graph-name>-manual-20260528T134500Z
```

Timestamps are UTC ISO-like format: `YYYYMMDDTHHMMSSZ`.  
Directory names are URL-encoded via `graph-dir/graph-dir-key->encoded-dir-name`.

### Act V: Web Graph Auto-backup

**File:** `src/main/frontend/handler/export.cljs` — `auto-db-backup!`

```clojure
(defn auto-db-backup!
  [repo]
  (when (and
         util/web-platform?
         (not (util/capacitor?))
         (ldb/get-key-value (db/get-db repo) :logseq.kv/graph-backup-folder))
    (cancel-db-backup!)
    (let [interval (js/setInterval #(backup-db-graph repo)
                                   (* 1 60 60 1000))]  ;; 1 hour
      (reset! *backup-interval interval))))
```

**Evidence:**

- Web auto-backup is opt-in: requires `:logseq.kv/graph-backup-folder` to be set in the graph DB
- Fires every 1 hour (same interval as Electron)
- Web backup path is OPFS-based, not the same directory structure as Electron

### Act VI: Emergency Write Failure Backups

**File:** `src/electron/electron/handler.cljs` — `:writeFile` handler (lines 100–131)  
**File:** `src/electron/electron/backup_file.cljs`

```clojure
(defmethod handle :writeFile [window [_ repo path content]]
  (try
    (fs/writeFileSync path content)
    (catch :default e
      (let [backup-path (try
                          (backup-file/backup-file repo :backup-dir path
                            (node-path/extname path) content)
                          (catch :default backup-error ...))]
        (utils/send-to-renderer window "notification"
          {:type "error"
           :i18n-key :electron/write-file-error-with-backup
           :i18n-args [path error-message backup-path]})))))
```

**`backup-file` behavior** (`backup_file.cljs:129`):

```clojure
(defn backup-file
  [repo dir relative-path ext content
   & {:keys [keep-versions backups-dir force-backup?]
      :or {keep-versions 6 force-backup? false}}]
  ...
  (when (or force-backup? (not (too-soon? dir*)))
    (fs/writeFileSync new-path content)
    (truncate-daily-versioned-files! dir* keep-versions)))
```

**Key Evidence:**

- Emergency backup only fires on **write failure**, not on every write
- Default `keep-versions = 6`
- **Rate-limited**: `too-soon?` skips backup if a backup was made within the last hour
- **New retention strategy** (`truncate-daily-versioned-files!`):
  - No longer a simple "keep N most recent files"
  - Keeps up to 6 hourly buckets (newest backup per hour)
  - Additional slots (if `keep-versions > 6`) use daily dedup buckets
  - Old files outside both buckets are deleted
- Notification shown to user with the backup path (i18n key: `:electron/write-file-error-with-backup`)

---

## The Evidence Chain: Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                  GRAPH OPENS (Electron)                         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                   sync-auto-backup-repo!
                              ↓
              reconcile-auto-backup-timer! starts
              ⏱️ js/setInterval(..., 3,600,000ms)  ← 1 hour
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│           run-auto-backup! fires every hour                     │
│                                                                 │
│  For each active (repo, window-id):                             │
│    backup-db-via-worker!                                        │
│    └─ <create-graph-backup!                                     │
│         └─ throttle check: latest :electron-auto < 1h? → skip  │
│         └─ snapshot! via db-worker :thread-api/backup-db-sqlite │
│         └─ write metadata.edn {:source :electron-auto}         │
│         └─ prune-backups! keeps 12 :electron-auto entries       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│           BACKUP STORAGE LAYOUT                                 │
│                                                                 │
│  ~/.logseq/graphs/<encoded>/backup/                             │
│    ├── <graph-20260528T134500Z>/                                 │
│    │   ├── db.sqlite   ← native SQLite backup                   │
│    │   └── metadata.edn  ← source, timestamp, repo              │
│    └── <graph-20260528T144500Z>/                                 │
│        ├── db.sqlite                                            │
│        └── metadata.edn                                         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│               USER TRIGGERS MANUAL SAVE                         │
│                                                                 │
│  Shortcut → :graph/db-save-shortcut                             │
│    → :graph/save-db-to-disk                                     │
│    → persist-db/export-current-graph!                           │
│    → ipc :db-export repo force-backup?=true                     │
│    → backup-db-via-worker! {:force-backup? true}                │
│    → source :electron-manual, bypasses throttle                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## The Interrogation: Key Questions Answered

### Q: Are backups still change-aware (only runs if data changed)?

**A: No.** The `graph-has-changed?` function and `*last-synced-graph->tx` atom have been removed. The only guard against excessive backups is the **throttle**: if a backup with the same source was created within `throttle-ms` (1 hour for auto-backup), it is skipped.

### Q: What happened to git auto-commit?

**A: It was removed.** The entire `electron.git` namespace (`src/electron/electron/git.cljs`) no longer exists. There are no git settings, no auto-commit, no `--separate-git-dir`, and no `.gitattributes` for SQLite diff. Users who want version history can use Logseq's backup listing or external tools.

### Q: How many backup versions are kept?

**A:** Two separate policies:

- **Auto-backup** (`run-auto-backup!`): Keeps **12** versions scoped to `:electron-auto` source (`automatic-backup-keep-versions = 12`)
- **Emergency write failure backup** (`backup-file`): Keeps **6** per path (default `keep-versions = 6`), with hourly/daily dedup

### Q: Can I trigger a manual save?

**A: Yes.** The keyboard shortcut triggers `:graph/db-save-shortcut` → `:graph/save-db-to-disk` → `export-current-graph!`. This sends `force-backup? = true` to bypass the throttle and creates a `:electron-manual` sourced backup. A success notification is shown in the UI.

### Q: What happens if a write fails?

**A:** Emergency backup via `backup-file/backup-file` writes to `<graph-dir>/backups/<path>/`. The user receives an error notification that includes the backup path. The function is rate-limited (max 1 per hour per path) but `force-backup?` is available.

### Q: Is there a backup UI?

**A:** `export-current-graph!` shows a notification on success/failure. There is no dedicated backup listing UI in the renderer that was confirmed in scope. The backup directory can be accessed directly at `~/.logseq/graphs/<encoded>/backup/`. The `list-backups` function in `graph-backup` provides a structured listing API used by the CLI.

---

## The Verdict: Key Takeaways

### For Users

1. **Auto-backup every hour**: Electron backs up your database automatically every hour
2. **Manual save available**: Use the keyboard shortcut to trigger an immediate backup (bypasses throttle)
3. **12 hourly auto-backups kept**: The last 12 auto-backup snapshots are retained
4. **Emergency backups on write failure**: If a file write fails, an emergency copy is saved with a notification
5. **No git integration**: Git auto-commit was removed; use external git tools if you want git history

### For Developers

1. **Architecture**: db-worker executes the SQLite backup via `thread-api/backup-db-sqlite`
2. **Source-scoped pruning**: Use `:source` in metadata to independently manage manual vs auto backups
3. **Throttle is separate from pruning**: Throttle prevents excessive backup frequency; pruning controls retention
4. **Backup format**: Each backup is a directory with `db.sqlite` + `metadata.edn` (schema-version 1)
5. **No transaction-based change detection**: Removed — rely on the throttle mechanism

### Key Files Reference

| Component                 | File                                          | Key Functions / Lines                                                       |
| ------------------------- | --------------------------------------------- | --------------------------------------------------------------------------- |
| Auto-backup orchestration | `src/electron/electron/db.cljs`               | `run-auto-backup!`, `reconcile-auto-backup-timer!`, `backup-db-via-worker!` |
| Backup policy engine      | `src/main/logseq/db_worker/graph_backup.cljs` | `<create-backup!`, `build-backup-name`, `prune-backups!`, `list-backups`    |
| Manual save trigger       | `src/main/frontend/persist_db.cljs`           | `export-current-graph!` (line 280)                                          |
| Manual save event         | `src/main/frontend/handler/events.cljs`       | `:graph/save-db-to-disk` (line 247)                                         |
| IPC handler               | `src/electron/electron/handler.cljs`          | `:db-export` (line 253), `:writeFile` (line 101)                            |
| Emergency backups         | `src/electron/electron/backup_file.cljs`      | `backup-file`, `truncate-daily-versioned-files!`, `too-soon?`               |
| Web auto-backup           | `src/main/frontend/handler/export.cljs`       | `auto-db-backup!` (line 275)                                                |

---

## Case Closed

**Status:** ✅ SOLVED (UPDATED 2026-05-28)

The backup system has evolved from a git-backed, 30-second export model to a structured, db-worker-driven hourly backup system. Key changes:

- **Git integration removed** — `electron.git` namespace deleted entirely
- **1-hour auto-backup** replacing 30-second export + 60-second git commit
- **Per-backup directories** with `db.sqlite` + `metadata.edn` replacing rolling file writes
- **Source-scoped retention** (12 auto / 6 emergency) with throttle-based frequency control
- **Hourly/daily dedup** in emergency backup retention (new `truncate-daily-versioned-files!`)

---

**Investigation Updated**
_Detective Code Analyzer, re-opening cold case 2026-05-28_ 🔍
