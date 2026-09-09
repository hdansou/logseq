# CMD+S (Save) in Database Mode: Complete Investigation

**Investigation Date:** November 20, 2025  
**Branch:** test/db  
**Investigator:** Code Detective Analysis

> **Last revised:** 2026-05-30

---

## Executive Summary (Validated 2026-05-30)

1. CMD+S is still registered as graph/db-save with binding mod+s in Electron.
2. The shortcut now publishes graph/db-save-shortcut, then routes to graph/save-db-to-disk.
3. Manual save triggers a backup flow, not the old direct raw db write flow.
4. WAL checkpoint behavior is still present, now using PRAGMA wal_checkpoint(TRUNCATE).
5. The old 30-second periodic export model documented here is no longer accurate.

---

## Current Flow (2026-05-30)

### 1) Shortcut and Event Chain

- src/main/frontend/modules/shortcut/config.cljs: graph/db-save publishes graph/db-save-shortcut and is inactive only when not Electron.
- src/main/frontend/handler/events.cljs: graph/db-save-shortcut delegates to graph/save-db-to-disk.
- src/main/frontend/handler/events.cljs: graph/save-db-to-disk calls persist-db/export-current-graph! with succ-notification? true.

### 2) Frontend Save Trigger

- src/main/frontend/persist_db.cljs: export-current-graph! now does ipc/ipc :db-export repo true.
- Success notification text is now DB backup successfully.
- This no longer uses force-save?/graph-has-changed? transaction tracking from the old implementation.

### 3) Electron and Worker Persistence Path

- src/electron/electron/handler.cljs: db-export calls db/backup-db-via-worker! with force-backup? flag.
- src/electron/electron/db.cljs: backup-db-via-worker! invokes thread-api/backup-db-sqlite in worker runtime.
- src/main/frontend/worker/db_core.cljs: thread-api/backup-db-sqlite performs checkpoint-db! before snapshot.
- src/main/frontend/worker/db_core.cljs: checkpoint SQL constant is PRAGMA wal_checkpoint(TRUNCATE).

### 4) Backup Storage Layout

- src/main/logseq/db_worker/graph_backup.cljs defines backup layout and metadata.
- Backups are versioned under each graph backup directory (backup root dir name is backup), with db.sqlite plus metadata.edn.
- This is different from the older direct fs/writeFileSync overwrite model documented in the previous revision.

---

## Findings: Removed or Stale Claims

> **Removed:** Claim that graph/db-save publishes graph/save-db-to-disk directly. It now publishes graph/db-save-shortcut first.

> **Removed:** Claim that export-current-graph! accepts and relies on force-save? and graph-has-changed? checks.

> **Removed:** Claim that run-export-periodically! performs 30-second periodic export from persist_db.cljs.

> **Removed:** Claim that helper <export-db! takes [repo data] and sends binary payload through db-export IPC from browser.cljs.

> **Removed:** Claim that worker API thread-api/export-db with Comlink transfer is the active manual-save path.

> **Removed:** Claim that Electron db-export handler writes raw db bytes via save-db! -> fs/writeFileSync as the primary CMD+S path.

> **Removed:** Claim that success notification message is The current db has been saved successfully to the disk.

---

## Findings: New Capabilities

1. Shortcut-specific event decoupling was introduced via graph/db-save-shortcut.
2. Desktop manual save now goes through unified graph backup infrastructure with source tagging (manual vs auto) instead of simple file overwrite semantics.
3. Worker exposes thread-api/backup-db-sqlite for checkpointed backup snapshots.
4. Graph backup subsystem provides structured backup layout and metadata tracking in src/main/logseq/db_worker/graph_backup.cljs.

---

## Notes on Auto Save and Periodic Backup

- Editor auto-save debounce around 450ms is still present in src/main/frontend/handler/editor.cljs.
- The previously documented 30-second periodic export in persist_db.cljs is gone.
- Current periodic backup behavior is hourly in the backup subsystems:
  - Desktop auto backup interval in src/electron/electron/db.cljs.
  - Web backup interval (when backup folder is configured) in src/main/frontend/handler/export.cljs.

---

## Updated Code Reference

- src/main/frontend/modules/shortcut/config.cljs
- src/main/frontend/handler/events.cljs
- src/main/frontend/persist_db.cljs
- src/electron/electron/handler.cljs
- src/electron/electron/db.cljs
- src/main/frontend/worker/db_core.cljs
- src/main/logseq/db_worker/graph_backup.cljs
- src/main/frontend/handler/editor.cljs
- src/main/frontend/handler/export.cljs

---

## Conclusion

CMD+S remains an Electron-only save shortcut, but the implementation has shifted from a direct export/write model to a checkpointed backup pipeline coordinated through Electron and db-worker runtime APIs. The previous revision's direct-write and 30-second periodic-export claims are stale and have been superseded by backup-oriented behavior.
