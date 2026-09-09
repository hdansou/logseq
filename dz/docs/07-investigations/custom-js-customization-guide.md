# Investigation: Logseq Custom JavaScript (custom.js) System

**Date:** 2026-05-30
**Investigator:** GitHub Copilot
**Status:** Revalidated against current source
**Scope checked:** `src/`, `deps/`, `packages/`

> **Last revised:** 2026-05-30

---

## Executive Summary

- `custom.js` is still supported via built-in path `logseq/custom.js`.
- `:custom-js-url` still overrides local `logseq/custom.js`.
- Execution still requires user approval and is re-confirmed every 7 days.
- `custom.js` still does not hot-reload; execution remains load-time based.
- Runtime loading is now unified through DB-backed file reads (`db/get-file`) rather than split DB-vs-filesystem branches in the UI handler.

---

## Current Behavior (Validated)

### 1) Path and priority

- `get-custom-js-path` resolves built-in path logic in `src/main/frontend/config.cljs`.
- URL override remains in `state/get-custom-js-link` via `:custom-js-url` in `src/main/frontend/state.cljs`.
- Config schema still includes `:custom-js-url` in `src/main/frontend/schema/handler/common_config.cljc`.
- Config template still documents precedence of `:custom-js-url` over local `logseq/custom.js` in `deps/common/resources/templates/config.edn`.

### 2) Execution and security

`src/main/frontend/handler/ui.cljs` still implements:

- storage key pattern: `ls-js-allowed-<href>`
- approval prompt key: `:plugin/custom-js-alert`
- approval cache interval: `604800000` milliseconds (7 days)
- one-time-per-href runtime guard via `*js-execed`
- remote URL load for `http...` values
- local execution through `db/get-file` + `js/eval`

### 3) Trigger timing

`custom-js-installer` in `src/main/frontend/components/plugins.cljs` still triggers execution when:

- graph/repo context changes
- DB restore is not in progress
- runtime is not `util/nfs?`

### 4) Hot reload status

- No custom.js hot reload path was found.
- CSS hot reload is still wired (`logseq/custom.css`) in DB-file save flow (`src/main/frontend/handler/db_based/editor.cljs`), while equivalent custom.js reload wiring is absent.

---

## Findings: New Capabilities

1. Plugin DB API explicitly supports built-in file read/write for customization files.
   - Runtime API handlers: `src/main/logseq/api/db.cljs` (`set_file_content`, `get_file_content`)
   - Type/API surface: `libs/src/LSPlugin.ts` (`setFileContent`, `getFileContent`)
   - CLJS SDK wrappers: `libs/cljs-sdk/src/com/logseq/db.cljs`

2. Built-in file set now explicitly includes both customization and publishing files.
   - Supported paths include:
     - `logseq/custom.js`
     - `logseq/custom.css`
     - `logseq/publish.js`
     - `logseq/publish.css`

3. DB graph initialization explicitly seeds `logseq/custom.js` as a built-in file entry.
   - See `deps/db/src/logseq/db/sqlite/create_graph.cljs`.

---

## Removed or Stale Claims (from prior revision)

> **Removed:** Specific reference to `src/main/frontend/fs/watcher_handler.cljs` as custom.css watcher evidence. That file/path is no longer the current source of truth.

> **Removed:** Prior claim that UI execution path branches to filesystem reads (`fs/read-file`) for file-based graphs in `exec-js-if-exists-&-allowed!`. Current implementation uses `db/get-file` in the local-file branch.

> **Removed:** Old line-number anchors and historical file locations from the 2025 version (for example, line ranges in `config.cljs`, `ui.cljs`, and `plugins.cljs`) because they no longer match current source.

---

## Practical Guidance (Current)

1. Use local `logseq/custom.js` for graph-scoped customization.
2. Use `:custom-js-url` for shared remote script override.
3. Expect manual reload/re-entry behavior; do not expect hot reload for JS changes.
4. Treat all custom JS as high trust code due to `js/eval` execution and app-context privileges.

---

## Evidence Index

- `src/main/frontend/config.cljs`
- `src/main/frontend/state.cljs`
- `src/main/frontend/schema/handler/common_config.cljc`
- `src/main/frontend/handler/ui.cljs`
- `src/main/frontend/components/plugins.cljs`
- `src/main/frontend/handler/db_based/editor.cljs`
- `src/main/logseq/api/db.cljs`
- `deps/common/resources/templates/config.edn`
- `deps/db/src/logseq/db/sqlite/create_graph.cljs`
- `libs/src/LSPlugin.ts`
- `libs/cljs-sdk/src/com/logseq/db.cljs`
