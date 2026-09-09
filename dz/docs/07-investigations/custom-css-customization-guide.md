# Investigation: Logseq Custom CSS Customization System

**Date:** 2026-05-30  
**Investigator:** GitHub Copilot  
**Status:** Revalidated against current source (`src/`, `deps/`, `packages/`, `libs/`)

> **Last revised:** 2026-05-30

---

## Executive Summary

- `custom.css` is still a first-class built-in file at path `logseq/custom.css`.
- Load priority is still: `:custom-css-url` in config first, then `logseq/custom.css` content.
- The old watcher-based explanation in this doc is stale; current style refresh entry points are graph setup/switch and DB editor save paths.
- Relative `../assets/...` links are expanded by `frontend.handler.assets/<expand-assets-links-for-db-graph` before style injection.

---

## Findings: Validated Current Behavior

### 1) Path + retrieval

- `frontend.config/get-custom-css-path` still resolves a custom CSS path and returns `logseq/custom.css` for DB graphs.
- `frontend.db.model/get-custom-css` still reads via `get-file repo "logseq/custom.css"`.
- DB graph initialization still creates a built-in `logseq/custom.css` row with `:file/content` in the graph DB.

### 2) CSS load pipeline

`frontend.handler.ui/add-style-if-exists!` currently does:

1. `state/get-custom-css-link` (`:custom-css-url`) first.
2. Falls back to `db-model/get-custom-css`.
3. Runs `assets-handler/<expand-assets-links-for-db-graph`.
4. Injects style with `util/add-style!`.

`frontend.handler.ui/reset-custom-css!` removes `#logseq-custom-theme-id` then re-adds style.

### 3) Triggers that are currently visible in source

- Graph restore/setup: `src/main/frontend/handler.cljs` calls `ui-handler/add-style-if-exists!`.
- Repo restore path: `src/main/frontend/handler/repo.cljs` calls `ui-handler/add-style-if-exists!`.
- Current repo effect: `src/main/frontend/components/theme.cljs` calls `ui-handler/reset-custom-css!`.
- DB editor save for `logseq/custom.css`: `src/main/frontend/handler/db_based/editor.cljs` calls `ui-handler/add-style-if-exists!`.

---

## Removed Or Stale Claims

> **Removed:** The prior claim that `src/main/frontend/fs/watcher_handler.cljs` handles `logseq/custom.css` reloads. That file/path is not present in current source.

> **Removed:** The prior claim that `frontend.config/expand-relative-assets-path` is the active custom.css expansion path. That function is not present in current source.

> **Removed:** The prior watcher timeline (`add/change/unlink` events and 200-300ms reload estimate). Current source evidence in this validation does not support that exact mechanism/timing.

> **Removed:** The prior DB graph filesystem claim `~/logseq/graphs/<graph-name>/logseq/custom.css` as a physical editable file. Current DB behavior is modeled as built-in file content stored in DB entities (`:file/path` + `:file/content`).

---

## Findings: New Capabilities

- Plugin/SDK DB API supports reading and writing built-in files including `logseq/custom.css`:
  - `logseq.DB.getFileContent("logseq/custom.css")`
  - `logseq.DB.setFileContent("logseq/custom.css", css)`
- This is implemented in `src/main/logseq/api/db.cljs` with explicit built-in path allowlist and exposed in SDK types in `libs/src/LSPlugin.ts`.

---

## Evidence Map

- Path resolution: `src/main/frontend/config.cljs`
- Load/reset logic: `src/main/frontend/handler/ui.cljs`
- CSS content retrieval: `src/main/frontend/db/model.cljs`
- DB save trigger for custom.css: `src/main/frontend/handler/db_based/editor.cljs`
- Graph/repo/theme trigger points: `src/main/frontend/handler.cljs`, `src/main/frontend/handler/repo.cljs`, `src/main/frontend/components/theme.cljs`
- DB initial built-in file creation: `deps/db/src/logseq/db/sqlite/create_graph.cljs`
- SDK/API built-in file access: `src/main/logseq/api/db.cljs`, `libs/src/LSPlugin.ts`

---

## Practical Notes (2026-05-30)

- If custom CSS appears ignored, check `:custom-css-url` first; it still has precedence over local built-in content.
- For relative asset URLs in CSS, prefer `../assets/...` paths; expansion is handled before injection.
- For programmatic customization (plugins/tools), use DB file APIs instead of assuming direct filesystem writes.
