# The Case of the Git-Backed SQLite Database: A Detective's Investigation

## 🔍 Case File: Logseq Database Mode Git Backup & Versioning System

**Investigation Status:** SOLVED
**Case Complexity:** High
**Technical Domain:** Database Persistence, Version Control, Backup Systems

---

## Executive Summary

Our investigation reveals a sophisticated backup and versioning system for Logseq's database mode. The system uses Git as a version control layer for SQLite databases, with periodic exports, automated commits, and a multi-tiered backup strategy. This detective-style guide documents the architecture, flow, and key evidence found throughout the codebase.

---

## The Crime Scene: System Architecture

### Location of Evidence

The backup system operates across multiple locations:

1. **Database Storage**: `~/.logseq/graphs/<db-name>/`

   - Primary SQLite database files
   - Managed by `electron.db` namespace

2. **Git Repository**: `~/.logseq/git/<graph-path>/.git`

   - Separate git directory (using `--separate-git-dir`)
   - Configured per graph
   - Managed by `electron.git` namespace

3. **Local Version Files**: `<graph-path>/logseq/version-files/local/`

   - Timestamped backup snapshots
   - Keeps latest 6 versions per file
   - Managed by `electron.backup-file` namespace

4. **Backup Directory**: `<graph-path>/logseq/bak/`
   - Emergency backups when write operations fail
   - Triggered by diff detection (deleted content)

### Key Suspects (Components)

```
frontend.persist-db (The Orchestrator)
├── Coordinates periodic exports
├── Tracks graph changes via transaction IDs
└── Triggers export every 30 seconds

electron.db (The Storage Handler)
├── Manages SQLite file I/O
├── save-db!: Writes binary data to disk
└── get-db: Reads database from disk

electron.git (The Version Control Agent)
├── Initializes git repositories
├── Configures SQLite diff handling
├── Performs auto-commits at intervals
└── Executes git operations via dugite

electron.backup-file (The Safety Net)
├── Creates timestamped version files
├── Maintains rolling window of backups
└── Truncates old versions (keeps 6)
```

---

## The Timeline: How It All Works

### Act I: Initialization (When a Graph Opens)

**File:** `electron.git.cljs:72-92`

```clojure
(defn init!
  [graph-path]
  (let [separate-git-dir (get-graph-git-dir graph-path)
        args ["init" "--separate-git-dir=" separate-git-dir]]
    ;; Configure git for SQLite files
    (run-git! graph-path #js ["config" "diff.sqlite3.binary" "true"])
    (run-git! graph-path #js ["config" "diff.sqlite3.textconv" "echo .dump | sqlite3"])
    ;; Create .gitattributes
    (fs/writeFileSync ".gitattributes" "*.sqlite diff=sqlite3")))
```

**Evidence Found:**

- Git uses a **separate directory** to keep graph directories clean
- SQLite files are configured with custom diff driver (`diff=sqlite3`)
- Text conversion command: `echo .dump | sqlite3` (converts binary to SQL for diffs)

### Act II: The Export Cycle (Every 30 Seconds)

**File:** `frontend.persist_db.cljs:55-80`

```clojure
(defn export-current-graph!
  [& {:keys [succ-notification? force-save?]}]
  (when (util/electron?)
    (when-let [repo (state/get-current-repo)]
      (when (or (and (config/db-based-graph? repo) (graph-has-changed? repo))
                force-save?)
        (<export-db repo {})
        (swap! *last-synced-graph->tx assoc repo (:max-tx (db/get-db repo)))))))

(defn run-export-periodically!
  []
  (js/setInterval export-current-graph! (* 30 1000)))  ;; 30 seconds
```

**The Export Chain:**

```
1. Timer fires (every 30s)
   ↓
2. Check if graph has changed (via max-tx comparison)
   ↓
3. Export database from OPFS/memory
   File: frontend.persist_db.browser.cljs:223-231
   ↓
4. Call electron IPC: :db-export
   File: electron.handler.cljs:282-284
   ↓
5. Write to disk: db/save-db!
   File: electron.db.cljs:20-23
   ↓
6. Update last-synced transaction ID
```

**Key Evidence:**

- Export is **change-aware**: Only exports if `max-tx` has increased
- Uses IPC (Inter-Process Communication) between renderer and main process
- Binary data passed directly (ArrayBuffer) for efficiency

### Act III: The Git Commit (Configurable Interval, Default 60s)

**File:** `electron.git.cljs:191-209`

```clojure
(defonce auto-commit-interval (atom nil))

(defn- auto-commit-tick-fn
  []
  (when (state/git-auto-commit-enabled?)
    (add-all-and-commit!)))

(defn configure-auto-commit!
  []
  (when (state/git-auto-commit-enabled?)
    (let [seconds (state/get-git-commit-seconds)  ;; Default: 60
          millis (* seconds 1000)]
      (js/setInterval auto-commit-tick-fn millis))))
```

**The Commit Chain:**

```
1. Timer fires (configurable, default 60s)
   ↓
2. Check if auto-commit enabled
   File: electron.state.cljs:24-27
   ↓
3. Add all files: git add --ignore-errors ./*
   File: electron.git.cljs:94-102
   ↓
4. Commit: git commit -m "Auto saved by Logseq"
   File: electron.git.cljs:106-128
   ↓
5. Handle errors (identity unknown, index.lock exists, etc.)
```

**Key Evidence:**

- Commits are **silent by default** (no user notification)
- Error handling for common git issues (permissions, lock files)
- Commit message: "Auto saved by Logseq" (customizable via UI)
- Commits **all graphs** currently open in all windows

### Act IV: Version Files (File Sync Integration)

**File:** `electron.backup-file.cljs:37-61`

```clojure
(defn backup-file
  [repo dir relative-path ext content]
  (let [dir* (get-version-file-dir repo relative-path)
        new-path (str (string/replace (.toISOString (js/Date.)) ":" "_")
                      ".Desktop" ext)]
    (fs/writeFileSync new-path content)
    (truncate-old-versioned-files! dir*)))  ;; Keep latest 6

(defn- truncate-old-versioned-files!
  [dir]
  (let [files (reverse (sort files))
        old-files (drop 6 files)]
    (doseq [file old-files]
      (fs-extra/removeSync file))))
```

**Evidence Found:**

- Version files created when **deletions detected** in file sync
- Format: `<ISO-timestamp>.Desktop<ext>`
- **Rolling window**: Only 6 most recent versions kept
- Used in conjunction with remote file sync system

**File:** `frontend.fs.sync.cljs:1329-1352`

```clojure
(defn- need-add-version-file?
  [^FileTxn filetxn origin-db-content]
  (when (.-updated? filetxn)
    (let [content (fs/read-file repo-dir rpath)]
      (and (seq origin-db-content)
           (or (nil? content)
               (some :removed (diff/diff origin-db-content content)))))))
```

**The Version File Trigger:**

- Created during **remote→local sync** operations
- Only for files with **deleted content** (diff contains `:removed`)
- Protects against data loss during sync conflicts

---

## Special Evidence: Git Configuration for SQLite

### The .gitattributes File

**File:** `electron.git.cljs:88-92`

```bash
*.sqlite diff=sqlite3
```

### Git Config Commands

```bash
git config diff.sqlite3.binary true
git config diff.sqlite3.textconv "echo .dump | sqlite3"
```

**What This Does:**

1. Treats `.sqlite` files as binary
2. For `git diff`, converts binary to SQL dump format
3. Enables readable diffs of database schema and data
4. Prevents git from attempting line-by-line text diffs

**Example Output:**

```bash
$ git diff HEAD^ HEAD
diff --git a/logseq.sqlite b/logseq.sqlite
--- a/logseq.sqlite
+++ b/logseq.sqlite
@@ -1,5 +1,6 @@
 CREATE TABLE blocks (id INTEGER PRIMARY KEY, ...);
 INSERT INTO blocks VALUES(1, 'My Note', ...);
+INSERT INTO blocks VALUES(2, 'New Block', ...);
```

---

## The Interrogation: Key Questions Answered

### Q: When are commits created?

**A:** Two scenarios:

1. **Periodic auto-commit**: Every N seconds (configurable, default 60s)
2. **On graph close**: Optional, controlled by `git/commit-on-close?` setting

**File:** `electron.git.cljs:211-215`

```clojure
(defn before-graph-close-hook!
  []
  (when (and (state/git-auto-commit-enabled?)
             (state/git-commit-on-close-enabled?))
    (add-all-and-commit!)))
```

### Q: What happens if a write fails?

**A:** Emergency backup is created in `logseq/bak/`

**File:** `electron.handler.cljs:119-142`

```clojure
(defmethod handle :writeFile [window [_ repo path content]]
  (try
    (fs/writeFileSync path content)
    (catch :default e
      (let [backup-path (backup-file/backup-file repo :backup-dir path ext content)]
        (notify-user (str "Write failed. Backup saved to " backup-path))))))
```

### Q: How are database changes detected?

**A:** Transaction ID tracking

**File:** `frontend.persist_db.cljs:48-53`

```clojure
(defonce *last-synced-graph->tx (atom {}))

(defn- graph-has-changed?
  [repo]
  (let [tx (@*last-synced-graph->tx repo)
        db (db/get-db repo)]
    (or (nil? tx) (> tx (:max-tx db)))))
```

**Evidence:**

- Each database has a monotonically increasing transaction ID (`max-tx`)
- System tracks last exported `max-tx` per graph
- Export only happens if current `max-tx > last-synced-tx`

### Q: Can I disable auto-commit?

**A:** Yes, through Settings

**File:** `frontend.components.settings.cljs:280-293`

```clojure
(defn switch-git-auto-commit-row []
  (let [enabled? (state/get-git-auto-commit-enabled?)]
    (toggle-switch
     :on-change (fn [_]
                  (ipc/ipc :userAppCfgs :git/disable-auto-commit? enabled?)
                  (ipc/ipc :setGitAutoCommit)))))
```

**Settings Location:**

- UI: Settings → Version Control
- Config keys:
  - `:git/disable-auto-commit?` (boolean)
  - `:git/auto-commit-seconds` (integer, default 60)
  - `:git/commit-on-close?` (boolean, default false)

---

## The Evidence Chain: Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     USER EDITS GRAPH                            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              FRONTEND (Browser/Renderer Process)                │
│                                                                 │
│  1. User Transaction → Worker Thread                            │
│  2. SQLite DB in OPFS/Memory Updated                           │
│  3. max-tx incremented                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    ⏱️  Every 30 seconds
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              EXPORT CHECK (persist_db.cljs)                     │
│                                                                 │
│  ❓ Has max-tx changed?                                         │
│      YES → Export database                                      │
│      NO  → Skip                                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓ YES
┌─────────────────────────────────────────────────────────────────┐
│              EXPORT DB (IPC to Main Process)                    │
│                                                                 │
│  1. Worker: Export SQLite to ArrayBuffer                        │
│  2. IPC: Send to main process                                   │
│  3. Main: Write to ~/.logseq/graphs/<db>/logseq.sqlite         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    ⏱️  Every 60 seconds (configurable)
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              GIT AUTO-COMMIT (electron.git.cljs)                │
│                                                                 │
│  ✅ If enabled:                                                 │
│    1. git add --ignore-errors ./*                               │
│    2. git commit -m "Auto saved by Logseq"                      │
│    3. Handle errors gracefully                                  │
│                                                                 │
│  🔴 If disabled: Skip                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    GIT REPOSITORY                               │
│                                                                 │
│  📁 ~/.logseq/git/<graph>/.git                                  │
│    ├── History of SQLite file changes                           │
│    ├── Readable diffs (via textconv)                            │
│    └── Commit messages and timestamps                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Special Operations: File Sync Integration

### The Version File Detective

**When file sync is enabled**, the system creates additional version files:

```
┌─────────────────────────────────────────────────────────────────┐
│              REMOTE → LOCAL SYNC                                │
│                                                                 │
│  Remote has updated file → Needs to apply locally               │
│                              ↓                                  │
│  🔍 Check: Does update contain deletions?                       │
│      ├── YES → Create version file first                        │
│      │         (Preserve old content)                           │
│      └── NO  → Apply update directly                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**File:** `frontend.fs.sync.cljs:908-909`

```clojure
(defn add-new-version-file
  [repo path content]
  (ipc/ipc "addVersionFile" (config/get-local-dir repo) path content))
```

**Storage:**

- Location: `<graph>/logseq/version-files/local/<path>/<timestamp>.Desktop<ext>`
- Retention: Latest 6 versions
- Purpose: Rollback capability during sync conflicts

---

## The Smoking Gun: Why This Design?

### Design Rationale

1. **Two-Speed System**

   - **Fast writes** (30s export): Ensures data reaches disk quickly
   - **Slower commits** (60s git): Reduces git overhead
   - **Result**: Balance between safety and performance

2. **Separate Git Directory**

   - Keeps graph directory clean
   - Allows multiple graphs to share git infrastructure
   - Prevents .git conflicts in synced folders

3. **Change Detection**

   - Transaction ID tracking prevents unnecessary exports
   - Only commits when data actually changed
   - Reduces I/O and disk usage

4. **Multi-Tier Backup**

   - **Primary**: SQLite file on disk
   - **Versioned**: Git history
   - **Snapshot**: Version files (latest 6)
   - **Emergency**: Backup directory (logseq/bak)

5. **SQLite Diff Configuration**
   - Binary diffs are useless for review
   - SQL dump format enables readable diffs
   - Helps with debugging and recovery

---

## Case Notes: Edge Cases and Error Handling

### Error: Git Author Identity Unknown

**File:** `electron.git.cljs:125-127`

```clojure
(if (string/starts-with? error "Author identity unknown")
  (utils/send-to-renderer "setGitUsernameAndEmail" {:type "git"})
  (notify-error))
```

**Action:** Prompts user to configure git username/email

### Error: index.lock Exists

**File:** `electron.git.cljs:98-102`

```clojure
(p/catch (fn [error]
  (if (string/includes? error "index.lock': File exists")
    (log-error error)  ;; Suppress - likely concurrent operation
    (p/rejected error))))
```

**Action:** Silently logs (prevents spurious errors)

### Error: Permission Denied

**File:** `electron.git.cljs:99-101`

```clojure
(if (string/includes? error "permission denied")
  (log-error error)
  (p/rejected error))
```

**Action:** Logs and continues (graceful degradation)

### Emergency: Write Failure

**File:** `electron.handler.cljs:131-142`

```clojure
(catch :default e
  (let [backup-path (backup-file/backup-file ...)]
    (notify-user (str "Write failed. Backup: " backup-path))))
```

**Action:** Creates timestamped backup in `logseq/bak/`

---

## The Verdict: Key Takeaways

### For Users

1. **Your data is safe**: 4 layers of backup (disk, git, version files, emergency)
2. **Auto-save works**: Database saved every 30s, committed every 60s
3. **You can customize**: Settings → Version Control
4. **You can recover**: Git history + version files provide rollback options

### For Developers

1. **Architecture**: Two-process (renderer + main) with IPC bridge
2. **Performance**: Change-aware exports minimize I/O
3. **Reliability**: Graceful error handling and degradation
4. **Integration**: Works with file sync for remote backup

### Key Files Reference

| Component            | File                                | Line Numbers       |
| -------------------- | ----------------------------------- | ------------------ |
| Export orchestration | `frontend.persist_db.cljs`          | 55-80              |
| Database I/O         | `electron.db.cljs`                  | 20-30              |
| Git operations       | `electron.git.cljs`                 | 72-215             |
| Git configuration    | `electron.git.cljs`                 | 88-92              |
| Backup files         | `electron.backup-file.cljs`         | 37-61              |
| Version files        | `frontend.fs.sync.cljs`             | 908-920, 1329-1352 |
| Settings UI          | `frontend.components.settings.cljs` | 280-329            |
| State management     | `electron.state.cljs`               | 20-31              |
| IPC handlers         | `electron.handler.cljs`             | 77-85, 282-287     |

---

## Case Closed

**Status:** ✅ SOLVED

This investigation has revealed a sophisticated, multi-layered backup and versioning system for Logseq's database mode. The system demonstrates careful attention to data integrity, performance, and user experience through:

- Automatic, periodic exports and commits
- Change-aware processing to minimize overhead
- Multiple backup tiers for maximum safety
- Graceful error handling and recovery
- Integration with file sync for remote backup
- Readable diffs for SQLite databases

The evidence is clear: Logseq's database mode is protected by a robust safety net that balances automation with user control.

---

**Investigation Complete**
_Detective Code Analyzer, signing off_ 🔍
