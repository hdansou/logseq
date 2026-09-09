# Graph Database Backup Investigation: Electron Desktop App

> **Last revised:** 2026-05-30
> **Previously revised:** 2026-01-09
> **Scope validated against:** src/, deps/, packages/

## Executive Summary

Electron DB backup behavior has changed substantially since the original January report.

1. DB backups now use a shared graph-backup engine and db-worker snapshots, not an in-place overwrite flow.
2. Backups are stored as per-backup directories under `backup/`, each containing `db.sqlite` plus `metadata.edn`.
3. Auto backup runs hourly in Electron main process, scoped to active window/repo pairs.
4. Retention is source-scoped keep-N (12 for `:electron-auto`), not hourly+daily bucket pruning for DB backups.
5. Manual backup (save shortcut path) is still supported and uses `force-backup?`.

## Current Architecture (2026-05-30)

### Trigger Paths

- Manual save path:
  - `src/main/frontend/modules/shortcut/config.cljs` (`:graph/db-save`)
  - `src/main/frontend/handler/events.cljs` (`:graph/db-save-shortcut` -> `:graph/save-db-to-disk`)
  - `src/main/frontend/persist_db.cljs` (`export-current-graph!` -> `ipc/ipc :db-export repo true`)
  - `src/electron/electron/handler.cljs` (`:db-export`)
  - `src/electron/electron/db.cljs` (`backup-db-via-worker!`)

- Automatic backup path (Electron):
  - `src/electron/electron/handler.cljs` (`:setCurrentGraph` -> `db/sync-auto-backup-repo!`)
  - `src/electron/electron/db.cljs` (`reconcile-auto-backup-timer!`, `run-auto-backup!`)
  - Interval: 1 hour (`backup-interval-ms = (* 60 60 1000)`)

### Storage Layout

- Live DB path is still under `~/.logseq/graphs/<encoded-graph-dir>/db.sqlite`.
- Backup root is now `~/.logseq/graphs/<encoded-graph-dir>/backup/`.
- Each backup is a directory with:
  - `db.sqlite`
  - `metadata.edn` (includes `:source`, `:created-at-ms`, `:repo`, `:schema-version`)

Source: `src/main/logseq/db_worker/graph_backup.cljs`, `deps/db/src/logseq/db/common/sqlite.cljs`.

### Retention and Throttling

- `src/main/logseq/db_worker/graph_backup.cljs` provides:
  - `<create-backup!`
  - `latest-backup-info`
  - `prune-backups!`
  - `throttled?`
- Electron auto backup uses:
  - `:source :electron-auto`
  - `:throttle-ms` = 1 hour
  - `:keep-versions` = 12
- Manual backup uses `:source :electron-manual` and is invoked via `force-backup?` path.

## Findings: New Capabilities

1. Unified backup policy layer (`logseq.db-worker.graph-backup`) shared by Electron DB backup paths.
2. Native SQLite snapshotting through backup API (`deps/db/src/logseq/db/sqlite/backup.cljs`) instead of copying already-exported bytes in Electron main.
3. Per-backup metadata (`metadata.edn`) enabling source-aware retention and diagnostics.
4. Source-scoped pruning (`:electron-auto` and `:electron-manual` treated independently by metadata source).
5. Safer backup target reservation (`reserve-next-backup-target!` + temp file + cleanup on failure).

## Removed or Stale Claims from Prior Version

> **Removed:** "DB backups are written as timestamped `.sqlite` files in `~/.logseq/graphs/<graph>/backups/` with 6 hourly + 6 daily retention."

Current behavior: DB backups are directory entries under `backup/` and retention is source-scoped keep-N in `graph_backup.cljs`.

> **Removed:** "Electron DB backup happens by reading old `db.sqlite`, writing a backup, then overwriting `db.sqlite` via `save-db!` on every export path."

Current behavior: Electron calls db-worker snapshot (`:thread-api/backup-db-sqlite`) through `backup-db-via-worker!`.

> **Removed:** "Automatic DB backup uses a 30-second frontend persisting loop with tx-id change detection."

Current behavior: Electron auto-backup is a 1-hour main-process timer keyed by active repos (`sync-auto-backup-repo!` + `run-auto-backup!`).

> **Removed:** "DB backup throttling/retention is handled by `electron.backup-file` hourly+daily truncation for the main DB backup flow."

Current behavior: `electron.backup-file` remains for emergency file write backups, not the primary DB graph backup pipeline.

## Notes on deps/ and packages/

- `deps/` contributes shared SQLite path/backup primitives used by the current flow:
  - `deps/db/src/logseq/db/common/sqlite.cljs`
  - `deps/db/src/logseq/db/sqlite/backup.cljs`
- No conflicting Electron DB backup implementation was found in `packages/`; primary behavior is in `src/` + `deps/`.

## Quick Risk Notes (Current)

1. Disk growth risk remains: hourly auto backups keep 12 entries per source and each contains full SQLite snapshot.
2. User deletion of `backup/` still removes recovery history.
3. Auto-backup throttle can intentionally skip additional backups within the same hour for `:electron-auto`.

## Change Log

| Date       | Version | Changes                                                                                                                                                              |
| ---------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2026-05-30 | 2.0     | Revalidated against current `src/`, `deps/`, and `packages/`; replaced stale architecture/retention claims; added removed-claim markers and new capability findings. |
| 2026-01-09 | 1.0     | Initial investigation document.                                                                                                                                      |
