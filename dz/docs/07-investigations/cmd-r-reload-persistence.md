# CMD+R/Ctrl+R Window Reload: Persistence Validation

> **Investigation date:** 2026-05-30
> **Scope validated against:** `src/`, `deps/`, `packages/` (plus plugin SDK in `libs/` where relevant)
> **Last revised:** 2026-05-30

---

## Executive Summary

`CmdOrCtrl+R` still reloads the desktop window through Electron's standard `viewMenu` role. There is still no main app-level `beforeunload` guard for unsaved editor work, so edits inside debounce/idle windows can still be lost on immediate reload.

The persistence internals described in the 2025 version are substantially outdated: the prior `*writes`/`file-writes-chan` write-queue implementation and periodic 30-second transit export flow are no longer represented by the current `src/main/frontend` code paths.

---

## Findings: Current Behavior (Validated)

1. Reload shortcut ownership

- Desktop menu uses Electron roles and includes `{:role "viewMenu"}`.
- This means standard Electron reload behavior (including `CmdOrCtrl+R`) remains active unless explicitly overridden.
- Evidence: `src/electron/electron/core.cljs` (`set-app-menu!`, menu template contains `{:role "viewMenu"}`).

2. Main-window reload guard

- No app-wide main-window `beforeunload` handler was found in current frontend runtime code.
- `beforeunload` usage exists for plugin lifecycle and PDF popup windows, not for core editor reload protection.
- Evidence:
  - No main handler in `src/main/frontend/**`.
  - PDF window-specific hook: `src/main/frontend/extensions/pdf/windows.cljs`.

3. Close and quit lifecycle (desktop)

- Window close path still persists zoom level and releases db-worker runtime for that window.
- App `before-quit` sets quitting state and stops db workers.
- Evidence:
  - `src/electron/electron/window.cljs` (`close-handler`, `persist-zoom-level`, `db-worker/release-window!`).
  - `src/electron/electron/core.cljs` (`before-quit` listener).

4. Editor autosave debounce still present

- `*auto-save-timeout` and `450ms` debounce are still present in editor change handling.
- Save trigger also checks input idleness (`state/input-idle? repo :diff 450`) and skips hashtag-tagged content in that path.
- Evidence:
  - `src/main/frontend/handler/editor.cljs` (`*auto-save-timeout`, `edit-box-on-change!`, `save-current-block!`).
  - `src/main/frontend/state.cljs` (`input-idle?`).

5. DB export path has changed

- Current explicit backup/export path in frontend uses `ipc/ipc :db-export repo true`.
- Worker export API is now `:thread-api/export-db-binary`, with WAL checkpoint via `PRAGMA wal_checkpoint(TRUNCATE)` before export.
- Evidence:
  - `src/main/frontend/persist_db.cljs` (`export-current-graph!`).
  - `src/main/frontend/worker/db_core.cljs` (`:thread-api/export-db-binary`, `wal-checkpoint-sql`).

---

## Findings: Removed or Stale Claims

> **Removed:** References to `src/main/frontend/worker/file.cljs` as the active file-write queue implementation (`*writes`, `*request-id`, `file-writes-chan`, `batch-write-interval`, and related lifecycle snippets). This path is no longer present in current source.

> **Removed:** Claim that graph-switch safety currently uses `:thread-api/file-writes-finished?` from `src/main/frontend/worker/db_worker.cljs`. No current match for this API in `src/`.

> **Removed:** Claim that DB backup persistence is primarily driven by a periodic 30-second scheduler (`run-export-periodically!` + `setInterval export-current-graph!`). Current `persist_db.cljs` no longer exposes that flow.

> **Removed:** Old worker export snippet using `:thread-api/export-db` and explicit `wal_checkpoint(2)` in `db_worker.cljs`. Current implementation route is `:thread-api/export-db-binary` in `db_core.cljs`.

> **Removed:** Fixed risk-window arithmetic tied to the old file-write queue batching path (`450ms + 1000ms queue interval` as a guaranteed architecture fact). The cited queue primitives are no longer present in current code.

> **Removed:** Statement that plugin `beforeunload` definitively does not fire on window reload without qualification. What is validated now: plugin `beforeunload` exists in plugin unload lifecycle; main-app reload guard is still absent.

---

## Findings: New Capabilities

1. Markdown mirror persistence pipeline

- DB graph changes can be mirrored to markdown via worker-side markdown mirror infrastructure.
- Flush is queued/debounced (default `1000ms`) and exposed through worker APIs.
- Evidence:
  - `src/main/frontend/worker/markdown_mirror.cljs` (`schedule-flush!`, `<flush-repo!`).
  - `src/main/frontend/worker/db_core.cljs` (`:thread-api/markdown-mirror-set-enabled`, `:thread-api/markdown-mirror-flush`, `:thread-api/markdown-mirror-regenerate`).

2. User-facing markdown mirror controls

- Settings UI now includes markdown mirror enable/disable and regenerate actions.
- Evidence: `src/main/frontend/components/settings.cljs` (`markdown-mirror-row`).

3. Runtime failover and worker lifecycle hardening

- DB worker runtime management includes failover-oriented handling and reload logic for stale worker lock scenarios on Capacitor path.
- Evidence:
  - `src/main/frontend/persist_db.cljs` (remote runtime failover paths).
  - `src/main/frontend/persist_db/browser.cljs` (`reload-app-if-old-db-worker-exists`).

---

## Updated Risk Framing (2026-05)

- Still true: immediate reload can race with unsaved in-editor state because there is no main app `beforeunload` confirmation path for pending edits.
- Still true: autosave debounce (`450ms`) creates a vulnerable window for quick reload after typing.
- Needs reinterpretation: old file-write-queue-specific timing model is stale; current persistence has different internals, especially in DB + markdown-mirror flows.

---

## Practical Verification Checklist

1. Type into an editable block, then trigger immediate `Cmd+R` in desktop.
2. Repeat with ~500ms pause after final keystroke.
3. Repeat with >2s pause.
4. Compare outcomes for:

- DB graph with markdown mirror disabled.
- DB graph with markdown mirror enabled.

5. Capture logs around:

- Editor autosave invocation (`save-current-block!` path).
- DB export/manual backup calls (`export-current-graph!` / `export-db-binary`).
- Markdown mirror queue/flush events.

---

## Source Anchors (Current)

- `src/electron/electron/core.cljs` (app menu, `viewMenu`, `before-quit`)
- `src/electron/electron/window.cljs` (window close handling, zoom persistence)
- `src/main/frontend/handler/editor.cljs` (autosave timeout/debounce and save flow)
- `src/main/frontend/state.cljs` (`input-idle?`)
- `src/main/frontend/persist_db.cljs` (`export-current-graph!`)
- `src/main/frontend/worker/db_core.cljs` (`export-db-binary`, WAL checkpoint, markdown mirror thread APIs)
- `src/main/frontend/worker/markdown_mirror.cljs` (flush queue/debounce)
- `src/main/frontend/components/settings.cljs` (markdown mirror controls)
- `libs/src/LSPlugin.core.ts`, `libs/src/LSPlugin.user.ts`, `libs/src/LSPlugin.caller.ts` (plugin `beforeunload` lifecycle)

---

## Conclusion

The core risk statement remains valid: rapid window reload can still lose edits not yet persisted by the editor save path. However, several implementation details in the original investigation were tied to now-removed internals and have been replaced with current code-backed behavior and updated capabilities.
