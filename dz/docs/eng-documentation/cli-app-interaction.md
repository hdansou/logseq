# Logseq CLI and App: Database Interaction User Experience

> **Last revised:** 2026-03-10

## Executive Summary

> **Note:** This document describes the internal database architecture and locking
> mechanism used by the Logseq CLI and app. Several CLI subcommands mentioned below
> (e.g., `server stop`, `server start`, `server list`, `server status`, `doctor`)
> are **not currently exposed** as user-facing CLI commands. The source file paths
> referenced point to compiled build artifacts; the original source has been
> reorganized. Treat the architectural descriptions as informational.

When using logseq-cli to interact with a Logseq graph, it modifies the SQLite database file directly through a managed `db-worker-node` server process. Understanding how to safely switch between CLI and app usage is critical to prevent data corruption and lock conflicts.

## Database Architecture

### File Structure

- **Main database**: `~/logseq/graphs/<graph-name>/db.sqlite`
- **Lock file**: `~/logseq/graphs/<graph-name>/db-worker.lock`
- **WAL files**: `db.sqlite-wal`, `db.sqlite-shm` (Write-Ahead Logging mode)
- **Search database**: `search/db.sqlite`
- **Client ops database**: `client-ops-db.sqlite`
- **Debug logs**: `db-worker-node-YYYYMMDD.log` (7-day retention)

### SQLite Configuration

From [db_core.cljs](../../src/main/frontend/worker/db_core.cljs#L264-L267):

```clojure
(defn- enable-sqlite-wal-mode!
  [db]
  (.exec db "PRAGMA locking_mode=exclusive")
  (.exec db "PRAGMA journal_mode=WAL")
  (.exec db "PRAGMA synchronous=NORMAL"))
```

The database uses:

- **WAL mode** (Write-Ahead Logging): Allows concurrent readers but only one writer
- **Exclusive locking**: The process that opens the database maintains exclusive locks
- **synchronous=NORMAL** (since `fff75ea17d`): no per-commit fsync. A process crash is durable; a power loss or OS crash can lose commits since the last checkpoint. The graph database checkpoints 2 s after the last store (`wal-checkpoint-idle-ms`, `src/main/frontend/worker/db_core.cljs`); the source is now at `db_core.cljs:594-603`, not the line range cited above.

## Lock File Mechanism

### Purpose

The lock file (`db-worker.lock`) prevents multiple processes from simultaneously writing to the SQLite database, which would cause corruption.

### Lock File Contents

From [db_worker_node_lock.cljs](../../src/main/frontend/worker/db_worker_node_lock.cljs#L85-L91):

```clojure
{:repo repo
 :pid (.-pid js/process)
 :lock-id (str (random-uuid))
 :host "127.0.0.1"
 :port <random-port>
 :startedAt "2026-02-10T..."}
```

### Lock Validation

The system validates locks by:

1. **Process ID (PID) check**: Verifies the process is still alive
2. **Lock ID**: Unique UUID to detect lock file staleness
3. **Repository match**: Ensures lock belongs to the correct graph

From [db_worker_node_lock.cljs](../../src/main/frontend/worker/db_worker_node_lock.cljs#L55-L62):

```clojure
(defn- pid-status
  [pid]
  (when (number? pid)
    (try
      (.kill js/process pid 0)  ; Signal 0 checks if process exists
      :alive
      (catch :default e
        (case (.-code e)
          "ESRCH" :not-found     ; Process doesn't exist
          "EPERM" :no-permission  ; Process exists but no permission
          :error)))))
```

## CLI Workflow

### Starting the CLI

From [cli/server.cljs](../../src/main/logseq/cli/server.cljs#L223-L237):

1. **CLI command executed** (e.g., `logseq add block --content "hello"`)
2. **ensure-server!** checks for existing lock file
3. **Stale lock cleanup**: If PID is `:not-found`, removes old lock
4. **Server spawn**: If no server running, spawns `db-worker-node` process
5. **Lock creation**: Server creates lock file with exclusive access
6. **Port discovery**: CLI reads port from lock file
7. **HTTP connection**: CLI connects to `http://127.0.0.1:<port>`
8. **Command execution**: CLI sends command via HTTP POST to `/v1/invoke`

### Write Operations

From [db_worker_node.cljs](../../src/main/frontend/worker/db_worker_node.cljs#L212-L214):

Before any write operation, the server validates lock ownership:

```clojure
(when (contains? write-methods method-kw)
  (let [{:keys [path lock]} @*lock-info]
    (db-lock/assert-lock-owner! path lock)))
```

Write methods include:

- `:thread-api/transact` - Database transactions
- `:thread-api/import-db` - Import operations
- `:thread-api/import-edn` - EDN imports
- `:thread-api/search-upsert-blocks` - Search index updates

## User Experience Scenarios

### Scenario 1: CLI Active, Then Opening App

**What happens:**

1. **CLI running** with `db-worker-node` server holding the lock
2. **User opens Logseq app**
3. **App attempts to open graph**
4. **Lock conflict detected**

From [db_worker_node_lock.cljs](../../src/main/frontend/worker/db_worker_node_lock.cljs#L77-L78):

```clojure
(when (and existing (contains? #{:alive :no-permission} (pid-status (:pid existing))))
  (throw (ex-info "graph already locked" {:code :repo-locked :lock existing})))
```

**User sees:**

- Error: "graph already locked" or "graph is locked"
- HTTP 409 Conflict response code

**Resolution:**

```bash
# Terminate the CLI process that holds the lock (e.g., Ctrl+C)
# Then open the app
```

### Scenario 2: App Active, Then Using CLI

**What happens:**

1. **Logseq app running** with graph open
2. **User runs CLI command**: `logseq query --name task-search`
3. **CLI attempts to start server**
4. **Lock conflict detected** (app's db-worker holds the lock)

**User sees:**

- CLI error: "graph already locked"
- Lock details showing the app's PID

**Resolution:**

```bash
# Close the graph in the app first
# OR
# If app is in another graph, CLI can work on its own graph
```

### Scenario 3: Switching Between CLI and App (Clean)

**Best practice workflow:**

```bash
# 1. Use CLI (commands that open a local graph hold a lock)
logseq query -g my-graph '[:find ?title :where [?b :block/title ?title] [?b :block/name]]'

# 2. CLI command finishes, lock is released automatically

# 3. Open app
# Now safe to open the graph in the Logseq app

# 4. Close app (releases lock on graph close)

# 5. Use CLI again
logseq show my-graph
```

### Scenario 4: Stale Lock Recovery

**What happens if process crashes:**

1. **CLI or app crashes** without cleaning up lock file
2. **Next CLI invocation** detects stale lock
3. **PID check returns** `:not-found`
4. **Automatic cleanup** - lock file removed
5. **Server starts normally**

From [db_worker_node_lock.cljs](../../src/main/frontend/worker/db_worker_node_lock.cljs#L79-L80):

```clojure
(when (and existing (= :not-found (pid-status (:pid existing))))
  (remove-lock! path))
```

**No user action required** - system self-heals

### Scenario 5: Multiple Graphs

**Concurrent usage is safe:**

```bash
# Terminal 1 - Graph A
logseq query -g graph-a '[:find ?title :where [?b :block/title ?title] [?b :block/name]]'

# Terminal 2 - Graph B (SAFE - different lock file)
logseq query -g graph-b '[:find ?title :where [?b :block/title ?title] [?b :block/name]]'

# Each graph has its own:
# - db.sqlite file
# - db-worker.lock file
# - db-worker-node process
```

## Data Consistency Guarantees

### WAL Checkpoint

From [db_core.cljs](../../src/main/frontend/worker/db_core.cljs#L582):

```clojure
(.exec db "PRAGMA wal_checkpoint(2)")  ; TRUNCATE checkpoint
```

The system performs WAL checkpoints at:

- **Graph export**: Ensures all changes written to main database file
- **DB close**: Commits pending WAL entries
- **Import operations**: Syncs before importing new data

### Transaction Safety

- **Atomic writes**: All database modifications are transactional
- **Lock validation**: Every write method checks lock ownership first
- **Error rollback**: Failed transactions don't corrupt database

## Diagnostic Commands

> **Note:** The `server` subcommands (`server status`, `server list`, `server start`,
> `server stop`, `server restart`) and the `doctor` command described below are not
> currently available as user-facing CLI commands. They represent internal or planned
> functionality. For troubleshooting, use `logseq validate -g <graph>` to check
> graph integrity, and `logseq show <graph>` to inspect graph metadata.

## Common Error Codes

| Error Code             | Meaning                          | Resolution                                  |
| ---------------------- | -------------------------------- | ------------------------------------------- |
| `:repo-locked`         | Another process holds the lock   | Stop other process or wait for timeout      |
| `:missing-repo`        | No graph specified               | Add `-g <graph-name>` flag                  |
| `:repo-mismatch`       | Command targets wrong graph      | Check `-g` matches server's graph           |
| `:server-not-found`    | Server not running               | CLI automatically starts it                 |
| `:server-start-failed` | Server failed to start           | Check logs, ensure db-worker-node.js exists |
| `:data-dir-permission` | Cannot read/write data directory | Fix permissions on `~/logseq/graphs`        |
| `:graph-not-opened`    | Database not open in worker      | Restart server                              |

## Logs and Debugging

### Server Logs

Location: `~/logseq/graphs/<graph-name>/db-worker-node-YYYYMMDD.log`

From [db_worker_node.cljs](../../src/main/frontend/worker/db_worker_node.cljs#L284-L295):

- **Rotation**: Daily log files
- **Retention**: 7 days (older logs deleted)
- **Format**: Structured log entries with timestamps

### CLI Debugging

For debugging CLI issues, use `println` or `js/console.log` in the ClojureScript source, or inspect the db-worker-node log files located at `~/logseq/graphs/<graph-name>/db-worker-node-YYYYMMDD.log`.

## Best Practices

### ✅ Safe Patterns

1. **Single access point**: Use either CLI or app at a time for a graph
2. **Wait for CLI to finish**: Ensure CLI commands complete before switching to app
3. **Check for stale locks**: If the app reports a lock conflict, verify no CLI process is running
4. **Multiple graphs**: Safe to use CLI on one graph while app uses another
5. **Automation scripts**: CLI is perfect for scripting when app is closed

### ❌ Unsafe Patterns

1. **Concurrent write**: Don't open graph in app while CLI server running
2. **Force kill**: Avoid `kill -9` on db-worker-node (use `server stop`)
3. **Manual file edits**: Never edit `db.sqlite` or lock files directly
4. **Lock file deletion**: Let system handle stale locks automatically

### 🔄 Recommended Workflow

**For humans:**

```bash
# Morning: CLI for bulk operations
logseq search -g my-graph "task"
logseq append "Daily standup notes" -a $LOGSEQ_API_SERVER_TOKEN

# Day: Use app for interactive work
# (Open Logseq app, work on graph)

# Evening: CLI for exports (app closed)
logseq export-edn -g my-graph -f backup.edn
```

**For automation:**

```bash
#!/bin/bash
# Export and validate
logseq export-edn -g my-graph -f backup.edn
logseq validate -g my-graph
```

## Technical Details

### Lock Acquisition Flow

```
CLI Command
    ↓
ensure-server! (cli/server.cljs:239)
    ↓
ensure-server-started! (cli/server.cljs:223)
    ↓
read-lock (check existing)
    ↓
cleanup-stale-lock! (if PID not found)
    ↓
spawn-server! (if needed)
    ↓
db-worker-node starts
    ↓
create-lock! (db_worker_node_lock.cljs:72)
    ↓
openSync with "wx" flag (exclusive create)
    ↓
Lock acquired ✓
```

### Server Lifecycle

```
START
    ↓
Install file logger
    ↓
Initialize platform
    ↓
Create lock file
    ↓
Open SQLite connections (db.sqlite, search, client-ops)
    ↓
Enable WAL mode
    ↓
Start HTTP server
    ↓
Update lock with port number
    ↓
READY (responds to /readyz)
    ↓
(handles /v1/invoke requests)
    ↓
SHUTDOWN (/v1/shutdown)
    ↓
Close SQLite connections
    ↓
WAL checkpoint
    ↓
Remove lock file
    ↓
EXIT
```

## Advanced Topics

### Port Assignment

- Server binds to `127.0.0.1:0` (random available port)
- Port recorded in lock file after binding
- CLI reads port from lock file to connect
- Prevents port conflicts with multiple graphs

### Lock File Permissions

- Uses Node.js exclusive file creation (`wx` flag)
- Atomic lock acquisition
- No race condition during startup

### Database Pooling (OPFS on Browser)

The browser-based app uses OPFS (Origin Private File System) with pooling:

- Not applicable to CLI (uses Node.js fs)
- CLI directly accesses `db.sqlite` file
- Both use same SQLite file format

## Conclusion

The lock file mechanism ensures **mutual exclusion** between CLI and app access to prevent SQLite database corruption. Users must:

1. **Be aware**: Only one process can write to a graph at a time
2. **Check for locks**: If the app reports a lock conflict, ensure no CLI process is running
3. **Clean shutdown**: Ensure CLI commands complete before opening app
4. **Monitor logs**: Check `db-worker-node-*.log` for issues
5. **Trust automation**: Stale lock cleanup is automatic

The architecture prioritizes **data integrity** over convenience, which is the correct trade-off for a knowledge management system.

---

**Related Files:**

- [db_worker_node_lock.cljs](../../src/main/frontend/worker/db_worker_node_lock.cljs) - Lock file management
- [db_worker_node.cljs](../../src/main/frontend/worker/db_worker_node.cljs) - Server process
- [cli/server.cljs](../../src/main/logseq/cli/server.cljs) - CLI server orchestration
- [db_core.cljs](../../src/main/frontend/worker/db_core.cljs) - SQLite operations
- [logseq-cli.md](./logseq-cli.md) - CLI documentation
