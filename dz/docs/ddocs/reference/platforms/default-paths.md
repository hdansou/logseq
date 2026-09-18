---
title: 'Default Paths by Platform'
description: 'Every default file, directory, storage bucket, and keychain entry Logseq uses, grouped by platform, with the environment variables and config keys that override them'
type: reference
status: active-development
lastVerified: '2026-09-09'
reviewStatus: authored-from-source
logseqVersion: '2.0.x'
platforms: [desktop, mobile, web]
graphType: [file, db]
relatedDocs:
  reference:
    - '[Mobile Differences](mobile-differences.md)'
    - '[db-worker HTTP API](../api/db-worker-http-api.md)'
    - '[Sync Limits, Throttles, and Batch Sizes](../features/sync-limits.md)'
    - '[URI Schemes](uri-schemes.md)'
---

# Default Paths by Platform

> Where Logseq puts things when you have not told it otherwise. Verified against `origin/master` @ `be800f1711` (2026-09-08); all 74 declared constants re-checked by the drift gate on 2026-09-09; each entry cites the file that defines it. A drift check for the literal constants lives at `dz/support-engineering/script/sync-limits/` (`default-paths.edn`).

Two conventions run through everything below:

- **`<graph-dir>`** is `<graphs-root>/<encoded-graph-name>/`, where the encoding is `encodeURIComponent` with `%20`→space, `~`→`%7E`, `%`→`~` (`deps/common/src/logseq/common/graph_dir.cljs:6`). The `logseq_db_` prefix is stripped first.
- **`<userData>`** is Electron's per-user app data directory for the app name `Logseq` (`resources/package.json`). Its location is the main thing that differs between desktop OSes.

## Quick index

| I want the… | Desktop (macOS / Windows / Linux) | Mobile (Android / iOS) | Browser |
|---|---|---|---|
| **Graph** (DB) | `~/logseq/graphs/<encoded>/` | WebView OPFS `.logseq-pool-<graph>` | OPFS `.logseq-pool-<graph>` |
| **Graph** (file) | user-chosen folder | user-chosen folder (SAF / Documents / iCloud) | — |
| **config.edn** (DB graph) | *inside the graph DB* as virtual file `logseq/config.edn` | same | same |
| **config.edn** (file graph) | `<graph>/logseq/config.edn` | same | — |
| **config.edn** (global) | `~/.logseq/config/config.edn` | — | — |
| **custom.css / custom.js** (DB graph) | *inside the graph DB* as `logseq/custom.css`, `logseq/custom.js`; or a URL set in Settings | same | same |
| **custom.css / custom.js** (file graph) | `<graph>/logseq/custom.css`, `<graph>/logseq/custom.js` | same | — |
| **Assets** (DB graph) | `<graph-dir>/assets/` | lightning-fs `memory:///<graph>/assets/` (IndexedDB) | same as mobile |
| **Assets** (file graph) | `<graph>/assets/` | same | — |
| **Backups** (DB graph) | `<graph-dir>/backup/<graph>-<ts>/` | — | picked folder `/<graph>/backups/` |
| **Backups** (file graph) | `<graph>/backups/<file>/<ts>.<ext>` | — | — |
| **Preferences** (app) | `<userData>/configs.edn` + `localStorage` | `localStorage` (WebView) | `localStorage` |
| **Preferences** (plugin SDK) | `~/.logseq/preferences.json` | IndexedDB key `…/preferences.json` | IndexedDB |
| **plugins.edn** (installed manifest) | `~/.logseq/config/plugins.edn` | — | — |
| **Plugins** | `~/.logseq/plugins/<id>/` | — | — |
| **Plugin settings** | `~/.logseq/settings/<id>.json` | — | — |
| **Plugin storage** | `~/.logseq/storages/<id>/` or `<graph-dir>/assets/storages/<id>/` | — | — |
| **Markdown mirror** | `<graph-dir>/mirror/markdown/{pages,journals}/` | — | — |

Details for each row follow.

## Shared per-graph layout (DB graphs)

The same layout is used by the desktop app, the CLI's `db-worker-node`, and the self-hosted sync server. Only `<graphs-root>` moves.

| Path (relative to `<graph-dir>`) | Purpose | Source |
|---|---|---|
| `db.sqlite` | the graph | `deps/db/src/logseq/db/common/sqlite.cljs:33`; `worker/db_core.cljs:132` `repo-path` |
| `search/db.sqlite` | full-text index | `db_core.cljs:442` |
| `search/vector/` + `metadata.json` | zvec semantic index (macOS arm64 only) | `db_core.cljs:422` |
| `client-ops-/db.sqlite` | pending local ops for sync | `db_core.cljs:445` |
| `assets/` | asset files | `platform/node.cljs:527` + `common-config/local-assets-dir` |
| `backups/` | app-driven SQLite backups | `sqlite.cljs:38` `get-db-backups-path` |
| `backup/<name>/db.sqlite`, `backup/<name>/metadata.edn` | named graph backups (`logseq graph backup`) | `src/main/logseq/db_worker/graph_backup.cljs:10-12` |
| `export/` | DB export drop location | `src/electron/electron/db.cljs:100` |
| `db-worker.lock` | worker process lock | `worker/db_worker_node_lock.cljs:49` |
| `db-worker-node-YYYYMMDD.log` | worker log, 7 days retained | `worker/db_worker_node.cljs:410` |

## Shared file-graph layout (legacy file graphs)

Inside a user-chosen folder. Subfolder names are configurable in `logseq/config.edn`.

| Path | Config key | Source |
|---|---|---|
| `logseq/config.edn` | — | `frontend/config.cljs:298` |
| `logseq/custom.css`, `logseq/custom.js`, `logseq/export.css` | — | `config.cljs:299-301` |
| `logseq/publish.css`, `logseq/publish.js` | — | publishing |
| `logseq/bak/` | — | file-version backups |
| `logseq/.recycle/` | — | soft-deleted files |
| `logseq/version-files/` | — | sync version snapshots |
| `logseq/pages-metadata.edn`, `logseq/graphs-txid.edn` | — | file-sync metadata |
| `pages/` | `:pages-directory` | `deps/common/src/logseq/common/config.cljs` |
| `journals/` | `:journals-directory` | same |
| `whiteboards/` | `:whiteboards-directory` | same |
| `assets/` | — | `common-config/local-assets-dir` |

## macOS

| What | Default | Source |
|---|---|---|
| DB graphs root | `~/logseq/graphs/` | `common/config.cljs:58` `default-graphs-dir`; `common/graph.cljs:114` |
| Global dot-dir | `~/.logseq/` | `electron/configs.cljs:10` `dot-root` |
| Graph registry | `~/.logseq/graphs.edn` | `configs.cljs:16` |
| Plugins | `~/.logseq/plugins/` | `electron/core.cljs:41` `PLUGINS_ROOT` |
| Plugin settings | `~/.logseq/settings/<plugin-id>.json` | `electron/plugin.cljs:261` |
| App config (`configs.edn`) | `~/Library/Application Support/Logseq/configs.edn` | `configs.cljs:11-12` (`userData`); carries a `FIXME: move configs.edn` |
| Auth tokens | `~/logseq/auth.json` | `handler/user.cljs:106` |
| CLI config | `~/logseq/cli.edn` | `cli/lib/cli_config.ml:45` |
| Worker registry | `~/logseq/server-list` (+ `.lock`) | `logseq/db_worker/server_list.cljs:14` |
| Worker KV store | `~/logseq/kv-store.json` | `platform/node.cljs:660` |
| CLI launcher | `~/.local/bin/logseq` | `electron/cli_install.cljs:15` |
| Bundled CLI | `Logseq.app/Contents/Resources/app.asar/js/logseq-cli.js` | `core.cljs:359` |
| Embedding sidecar runtime | `~/Library/Application Support/Logseq/embedding-server/` — `.venv/`, `deps-v2.ok`, `embedding-server.log` | `electron/embedding_server.cljs:11-14` |
| Main-process log | `~/Library/Logs/Logseq/main.log` | electron-log 5.4.3 default; no override in `electron/logger.cljs` |
| E2EE keys | Keychain service **`Logseq E2EE`** | `platform/node.cljs:601` via keytar |
| Plugin download temp | `$TMPDIR/<name>.pending` | `electron/plugin.cljs:121` |

## Windows

Everything under **macOS** applies with these substitutions:

| What | Default | Source |
|---|---|---|
| `<userData>` | `%APPDATA%\Logseq\` | Electron default |
| App config | `%APPDATA%\Logseq\configs.edn` | `configs.cljs:12` |
| Main-process log | `%USERPROFILE%\AppData\Roaming\Logseq\logs\main.log` | electron-log 5 default |
| Embedding sidecar | **not started** — `embedding_server.cljs:283` returns `:skipped` off macOS | |
| CLI launcher | `%LOCALAPPDATA%\Microsoft\WindowsApps\logseq.cmd` (falls back to the first writable `PATH` dir) | `core.cljs:347-349`; `cli_install.cljs:38` |
| Dot-dir path fixing | `~/.logseq` with `\` separators | `electron/utils.cljs:118-126` `fix-win-path!` |
| E2EE keys | Windows Credential Manager, service `Logseq E2EE` | keytar |

`~` expands via `os.homedir()` on every platform (`graph.cljs:116`, `db_worker_node_lock.cljs:17`), so `~/logseq/graphs` is `C:\Users\<you>\logseq\graphs\`.

## Linux

Everything under **macOS** applies with these substitutions:

| What | Default | Source |
|---|---|---|
| `<userData>` | `~/.config/Logseq/` | Electron default |
| App config | `~/.config/Logseq/configs.edn` | `configs.cljs:12` |
| Main-process log | `~/.config/Logseq/logs/main.log` | electron-log 5 default |
| Embedding sidecar | **not started** (macOS only) | `embedding_server.cljs:283` |
| CLI launcher target | `$APPIMAGE` when running from an AppImage, else the executable | `core.cljs:373`; `cli_install.cljs:35-40` |
| E2EE keys | libsecret / GNOME Keyring, service `Logseq E2EE` | keytar |

## Android

DB graphs live in the WebView's origin-private file system, not on the shared filesystem; the native filesystem is used for file graphs, pickers, and shares.

| What | Default | Source |
|---|---|---|
| DB graph storage | OPFS inside the Capacitor WebView — see [Browser](#browser--web) | `platform/browser.cljs:196` `:capacitor` |
| App id | `com.logseq.app` | `capacitor.config.ts:8` |
| Folder-picker roots | primary storage `/storage/emulated/0/<path>`; `home` → `…/Documents/<path>`; `downloads` → `…/Download/<path>` | `android/app/src/main/java/com/logseq/app/FileUtil.java:41-45` |
| App-private external dirs | `context.getExternalFilesDirs(...)` | `FileUtil.java:119` |
| Full-storage access | requires `MANAGE_EXTERNAL_STORAGE` | `FolderPicker.java:34` |
| E2EE keys | Android Keystore via `@aparajita/capacitor-secure-storage`, key prefix `logseq.e2ee.` | `mobile/secure_storage.cljs:9` |

## iOS

| What | Default | Source |
|---|---|---|
| DB graph storage | OPFS inside the WKWebView — see [Browser](#browser--web) | `platform/browser.cljs:196` |
| Local documents | `<app sandbox>/Documents/` | `ios/App/App/FileContainer.swift:19`; `Utils.swift:63` |
| iCloud Drive | container `iCloud.com.logseq.logseq` → `…/Documents/` | `FileContainer.swift:15`; `App.entitlements:13,21` |
| Share extension | app group `group.com.logseq.logseq` | `ios/App/ShareViewController/ShareViewController.swift:19` |
| Folder picker | returns the picked URL plus `localDocumentsPath` | `FolderPicker.swift:58` |
| E2EE keys | iOS Keychain via `capacitor-secure-storage`, prefix `logseq.e2ee.` | `mobile/secure_storage.cljs:9` |

## Browser / web

Everything is inside the origin's storage; nothing touches the filesystem.

| What | Default | Source |
|---|---|---|
| Graph storage | OPFS directory `.logseq-pool-<graph>` at the OPFS root | `platform/browser.cljs:26,48` |
| SQLite VFS pool name | `logseq-pool-<graph>` (`/` and `\` → `_`) | `worker_common/util.cljc:47` |
| Files inside the pool | `/db.sqlite`, `/search/db.sqlite`, `/client-ops-/db.sqlite` | `db_core.cljs:132,442,445`; `resolve-db-path` is identity in the browser |
| Publishing mode | `/db.sqlite`, `/search-db.sqlite` | `db_core.cljs:429-433` |
| Auth tokens | `localStorage`: `id-token`, `access-token`, `refresh-token` | `handler/user.cljs:96-101` |
| Preferences | `localStorage`, EDN-encoded values | `frontend/storage.cljs` |
| Persistence | `navigator.storage.persist()` requested; warns if denied | `persist_db/browser.cljs:111` |

No vector index exists in the browser (`platform/browser.cljs` has no `:vector` key).

## CLI (`logseq`) — all desktop platforms

| What | Default | Override |
|---|---|---|
| Root dir | `$HOME/logseq` | `--root-dir`, `LOGSEQ_CLI_ROOT_DIR` |
| Config | `<root>/cli.edn` | `LOGSEQ_CLI_CONFIG` |
| Auth | `<root>/auth.json` (shared with desktop) | `config.auth_path` |
| Graphs | `<root>/graphs/` | inherited from root |
| Worker registry | `<root>/server-list` | — |
| Worker script | bundled | `LOGSEQ_DB_WORKER_NODE_SCRIPT` |

Sources: `cli/lib/cli_config.ml:43-45,123-131`; `cli/lib/auth_state.ml:38-43`; `cli/lib/server_runtime.ml:50`.

## Self-hosted sync server (Node)

| What | Default | Override |
|---|---|---|
| Data dir | `data/db-sync` (relative to cwd) | `DB_SYNC_DATA_DIR` |
| Graph index | `<data>/index.sqlite` | — |
| Per-graph DB | `<data>/graphs/<encoded>/db.sqlite` | — |
| Assets | `<data>/assets/<graph-id>/<uuid>.<type>` | `DB_SYNC_ASSETS_DRIVER` |
| Port | 8080 | `DB_SYNC_PORT` |

Sources: `deps/db-sync/src/logseq/db_sync/node/config.cljs:14-19`; `node/storage.cljs:42,47`; `node/server.cljs:115`.

## Environment variables that move paths

| Variable | Affects | Source |
|---|---|---|
| `LOGSEQ_GRAPHS_DIR` | desktop + worker graphs root (default `~/logseq/graphs`) | `common/graph.cljs:114` |
| `LOGSEQ_CLI_ROOT_DIR` | CLI root | `cli_config.ml:125` |
| `LOGSEQ_CLI_CONFIG` | CLI config file | `cli_config.ml:126` |
| `LOGSEQ_DB_WORKER_NODE_SCRIPT` | worker bundle the CLI spawns | `server_runtime.ml:50` |
| `LOGSEQ_EMBEDDINGS_URL` | embedding endpoint; required for a vector index | `platform/node.cljs:231` |
| `LOGSEQ_EMBEDDINGS_PYTHON` | interpreter used to build the sidecar venv | `embedding_server.cljs:83` |
| `LOGSEQ_EMBEDDING_MODEL` | model id (changes `search/vector` metadata) | `embedding_server.cljs:87` |
| `DB_SYNC_DATA_DIR`, `DB_SYNC_ASSETS_DRIVER`, `DB_SYNC_STORAGE_DRIVER` | self-hosted server | `node/config.cljs` |
| `APPIMAGE` | Linux CLI launcher target | `core.cljs:373` |
| `LOCALAPPDATA` | Windows CLI launcher dir | `core.cljs:347` |

## config.edn — three different things

| Kind | Where | Source |
|---|---|---|
| **DB graph config** | Not on disk. A virtual file with path `logseq/config.edn` stored **inside the graph database**, seeded from `templates/config.edn` on graph creation. Read via `<get-file-content repo "logseq/config.edn"`. | `worker/db_core.cljs:75-81`; `handler/repo_config.cljs:18` |
| **File graph config** | `<graph>/logseq/config.edn` on disk | `frontend/config.cljs:298` |
| **Global config** (desktop only) | `~/.logseq/config/config.edn` — root is `getLogseqDotDirRoot`, i.e. `~/.logseq` | `handler/global_config.cljs:22-27,76`; `config.cljs:146` `global-config-enabled? = electron?` |
| **App config** | `<userData>/configs.edn` — Electron settings, not graph config | `electron/configs.cljs:12` |

Editing a DB graph's `config.edn` in the app edits a database entity; there is no file to open in another editor. Exporting the graph (`export-edn`) includes it.

## custom.css and custom.js

Same split as `config.edn`, plus a URL escape hatch.

| Kind | Where | Source |
|---|---|---|
| DB graph | Virtual files `logseq/custom.css` and `logseq/custom.js` **inside the graph database**; loaded via `<get-file-content repo "logseq/custom.css"` | `handler/ui.cljs:114-116,132-133`; `handler/db_based/editor.cljs:164` |
| DB graph, remote override | A `custom-css-link` / `custom-js-link` URL from app state (Settings) takes precedence over the in-graph file | `handler/ui.cljs:114,132` |
| File graph | `<graph>/logseq/custom.css`, `<graph>/logseq/custom.js` | `frontend/config.cljs:299-301,374-381` (`get-custom-js-path` joins `repo-dir`/`logseq`/`custom.js`) |
| Export styling | `logseq/export.css` (file graph) / virtual (DB) | `config.cljs:300` |

There is no global `custom.css` under `~/.logseq/config/` — only `config.edn` is global (`global_config.cljs:27`). Theme plugins ship their own CSS under `~/.logseq/plugins/<id>/`.

## Assets

| Context | Location | Source |
|---|---|---|
| DB graph, desktop / CLI worker | `<graph-dir>/assets/<file>` | `platform/node.cljs:527`; `common-config/local-assets-dir` |
| DB graph, browser and mobile | lightning-fs at `memory:///<graph>/assets/` — an IndexedDB-backed virtual filesystem (`window.pfs`), **not** OPFS and not the native filesystem | `platform/browser.cljs:106-111`; `fs/memory_fs.cljs:2`; `@isomorphic-git/lightning-fs` |
| File graph | `<graph>/assets/<file>` | `handler/assets.cljs:286-294` |
| Plugin `FileStorage` with `{assets: true}` | `<graph-dir>/assets/storages/<plugin-id>/` | `api/plugin.cljs:82-90`; `electron/plugin.cljs:45` |
| Synced copy, hosted | R2 bucket `logseq-sync-assets-prod`, key `<graph-id>/<uuid>.<type>` | `deps/db-sync/worker/wrangler.toml:20`; `handler/assets.cljs:207` |
| Synced copy, self-hosted | `<data-dir>/assets/<graph-id>/<uuid>.<type>` | `node/server.cljs:115` |

Mobile is the surprise: asset files on Android/iOS live in the WebView's IndexedDB via lightning-fs, so they are invisible to the Files app and to iCloud.

## Backups

### DB graphs (desktop)

| What | Value | Source |
|---|---|---|
| Location | `<graph-dir>/backup/<name>/db.sqlite` + `metadata.edn` | `logseq/db_worker/graph_backup.cljs:10-12,39-47` |
| Name | `<graph>-<UTC timestamp>` (auto) or `<graph>-<label>-<UTC timestamp>` (named) | `graph_backup.cljs:93-101` |
| Automatic cadence | throttled to **1 per hour** | `electron/db.cljs:15-16` `backup-interval-ms` |
| Automatic retention | **12** versions — the 6 newest deduped per hour, the rest per day | `db.cljs:18`; `backup_file.cljs:34-38` |
| Manual (`force-backup?`) | same location, no throttle, tagged `:electron-manual` | `db.cljs:43-47` |
| Legacy dir | `<graph-dir>/backups/` still defined but named backups use `backup/` | `deps/db/…/sqlite.cljs:35-38` |

### File graphs (desktop)

| What | Value | Source |
|---|---|---|
| Location | `<graph>/backups/<relative-path-without-extension>/<ISO-timestamp>.<ext>` — colons in the timestamp become `_` | `electron/backup_file.cljs:7-16,23-30` |
| Retention | `keep-versions`: 6 hourly + remainder daily | `backup_file.cljs:34-70` |
| Also | `logseq/bak/` and `logseq/version-files/` from the file-sync era remain | file-graph layout above |

### Browser

Backup is an explicit export to a user-picked directory handle: `<picked>/<graph>/db.sqlite`, with previous copies rotated into `<picked>/<graph>/backups/<ms>.db.sqlite` (`handler/export.cljs:250-267`).

## Preferences — where settings actually persist

"Preferences" is four stores with different scopes. Which one a setting uses is decided per key, not per screen.

| Store | Scope | Location | What lives there | Source |
|---|---|---|---|---|
| `configs.edn` | this machine, Electron main process | `<userData>/configs.edn` | `:spell-check`, `:auto-update`, `:window/native-titlebar?`, `:feature/enable-semantic-search?`, `:feature/enable-automatic-chmod?`, `:settings/agent`, `:server/host` `:server/port` `:server/tokens` `:server/autostart` `:server/mcp-enabled?` | `electron/configs.cljs:12`; written via the `:userAppCfgs` IPC (`electron/handler.cljs:354`) |
| `localStorage` | this browser profile / WebView, all platforms | origin storage | theme (`:ui/theme`, `:ui/system-theme`, `:ui/custom-theme`, `:ui/radix-color`), `:ui/editor-font`, `:ui/wide-mode`, `:zoom-level`, `:preferred-language`, sidebar state (`:ls-left-sidebar-open`, `:ls-left-sidebar-width`), `:ls-shortcuts`, `:ui/recent-pages`, `:document/mode`, `:git/current-repo`, `:logseq-sync-enabled`, `:db-worker-client-id`, auth tokens (`id-token`, `access-token`, `refresh-token`), ~35 keys in all | `frontend/storage.cljs` (EDN-encoded values); `handler/user.cljs:95-101` |
| Graph `config.edn` | one graph | see [config.edn](#configedn--three-different-things) | everything in the in-app config editor: journals, shortcuts overrides, default templates, property visibility, … | `handler/repo_config.cljs` |
| Global `config.edn` | all graphs on this desktop | `~/.logseq/config/config.edn` | same keys as a graph config; graph config wins on conflict | `handler/global_config.cljs:27` |

Two files that are *called* preferences but are plugin-scoped:

| File | Location | Purpose | Source |
|---|---|---|---|
| `preferences.json` | desktop `~/.logseq/preferences.json`; web/mobile an IndexedDB item keyed `<dotdir>/preferences.json` | Plugin SDK `logseq.App.*Preferences` / `load_user_preferences` — a JSON bag shared by all plugins; "Open preferences" in the plugins menu opens it | `api/plugin.cljs:240-262`; `components/plugins.cljs:958-959` |
| `plugins.edn` | `~/.logseq/config/plugins.edn` | Manifest of installed plugins (id, repo, version…), auto-updated on install/uninstall; used by "Replace plugins" to reinstall a set on another machine. Desktop only, requires global config enabled | `handler/plugin_config.cljs:26-28,114,136` |

Logging out clears every `localStorage` key (`handler/user.cljs:131-141`), so browser-side preferences do not survive a sign-out.

## Plugins (desktop only)

| What | Path | Source |
|---|---|---|
| Installed plugins | `~/.logseq/plugins/<plugin-id>/` (marketplace) | `electron/core.cljs:41`; `electron/plugin.cljs:227,260` |
| Plugin settings | `~/.logseq/settings/<plugin-id>.json` | `electron/plugin.cljs:261` |
| `logseq.FileStorage` (default) | `~/.logseq/storages/<plugin-id>/<key>` | `api/plugin.cljs:82-90,123` |
| `logseq.FileStorage({assets: true})` | `<graph-dir>/assets/storages/<plugin-id>/<key>` | `api/plugin.cljs:82-90`; `write_assetsdir_file` |
| `logseq.write_dotdir_file` scratch | `~/.logseq/tmp/` (sub-root `"tmp"`) | `api/plugin.cljs:190` |
| Marketplace download | `$TMPDIR/<id>_<asset>.zip.pending`, extracted next to it, then moved into `plugins/<id>` | `electron/plugin.cljs:119-158` |
| Served as | `lsp://logseq.io/plugins/<id>/…` (bundled: `lsp://logseq.com/plugins/`) | `core.cljs:36-38` |

Writes outside `~/.logseq` or `assets/storages` are refused (`electron/plugin.cljs:41-45`).

## Markdown mirror (desktop)

The DB-graph → Markdown mirror writes under the graph directory, never into a user-chosen folder:

| Path | Source |
|---|---|
| `<graph-dir>/mirror/markdown/pages/<page>.md` | `worker/markdown_mirror.cljs:15-17,110` |
| `<graph-dir>/mirror/markdown/journals/<yyyy_MM_dd>.md` | `markdown_mirror.cljs:97-98` |

File names have `<>:"|?*\/` and control characters replaced by `_` (`markdown_mirror.cljs:19-22,60`). Enabled per graph via `:thread-api/markdown-mirror-set-enabled`; design in `docs/adr/0016-markdown-mirror.md`.

## Notes and known oddities

- **`configs.edn` lives in `<userData>` while everything else lives in `~/.logseq`.** The code says so itself: `configs.cljs:9` carries `FIXME: move configs.edn to where it should be`.
- **`auth.json` is in `~/logseq/`, not `~/.logseq/`** — alongside graphs, not alongside the registry. It is plaintext JSON containing id, access, and refresh tokens (`handler/user.cljs:109-113`).
- **The client-ops directory has a trailing hyphen**: `client-ops-/db.sqlite` (`db_core.cljs:445`), while `client-ops-repo-path` at `:133` spells it `client-ops/db.sqlite`. Both strings exist; `:851-861` handles the migration between them.
- **Mobile DB graphs are not files you can browse.** They live in the WebView's OPFS. The native Documents/iCloud paths only matter for file graphs, the folder picker, and exports.
- **Graph directory names are URL-encoded.** `My Graph` becomes `My Graph`, but `a/b` becomes `a~2Fb`.
