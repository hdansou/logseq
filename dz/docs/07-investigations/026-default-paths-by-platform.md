# Default Paths by Platform Investigation

> **Last revised:** 2026-09-08
> **Verified against:** `origin/master` @ `d2ab772` (2026-09-02)

## Question

What default paths does Logseq use, grouped by platform — Windows, Linux, macOS, Android, iOS, browser?

## Answer

Full table: [`ddocs/reference/platforms/default-paths.md`](../ddocs/reference/platforms/default-paths.md). The shape:

- **Desktop (all three OSes)** shares one layout: DB graphs under `~/logseq/graphs/<encoded>/`, the global dot-dir `~/.logseq/` (registry, plugins, plugin settings), auth at `~/logseq/auth.json`, CLI config at `~/logseq/cli.edn`. Only `<userData>` (which holds `configs.edn` and the embedding sidecar) and the log/launcher locations differ per OS.
- **Mobile (Android, iOS)** stores DB graphs in the WebView's OPFS, not on the filesystem. Native paths (Documents, iCloud container, SAF roots) serve file graphs, pickers, and shares.
- **Browser** is entirely origin storage: OPFS pool `.logseq-pool-<graph>` and `localStorage`.
- **CLI** mirrors desktop under `$HOME/logseq`, sharing `auth.json` and `graphs/` with the app.
- **Self-hosted sync server** uses `data/db-sync/{index.sqlite, graphs/, assets/}` relative to cwd.

## Findings

### One per-graph layout everywhere

`db.sqlite`, `search/db.sqlite`, `search/vector/`, `client-ops-/db.sqlite`, `assets/`, `backups/`, `backup/<name>/`, `export/`, `db-worker.lock`, `db-worker-node-YYYYMMDD.log`. Desktop, CLI worker, and the Node sync server all build it from `deps/db/src/logseq/db/common/sqlite.cljs:33` and `worker/db_core.cljs:132-445`.

### Two homes on desktop, and the code knows it is wrong

`~/.logseq/` (`configs.cljs:10 dot-root`) holds `graphs.edn`, `plugins/`, `settings/<id>.json`. `<userData>` (`configs.cljs:11 cfg-root`) holds `configs.edn` and `embedding-server/`. Line 9 of `configs.cljs`: `;; FIXME: move configs.edn to where it should be`. Meanwhile `auth.json` sits in a third place, `~/logseq/` (`handler/user.cljs:106`), next to graphs.

### Platform differences are almost all Electron defaults, not Logseq code

Logseq never calls `app.setPath`. `<userData>` and the electron-log location follow Electron / electron-log 5.4.3 defaults: `~/Library/Application Support/Logseq` · `%APPDATA%\Logseq` · `~/.config/Logseq`; logs at `~/Library/Logs/Logseq/main.log` · `%APPDATA%\Logseq\logs\main.log` · `~/.config/Logseq/logs/main.log`. The only OS-specific Logseq code is the CLI launcher (`~/.local/bin/logseq` vs `%LOCALAPPDATA%\Microsoft\WindowsApps\logseq.cmd`, `core.cljs:335-349`), `$APPIMAGE` on Linux (`core.cljs:373`), and Windows path fixing (`utils.cljs:118`).

### The embedding sidecar is macOS-only by path as well as by gate

`embedding_server.cljs:283` returns `:skipped` when `platform ≠ darwin`, so `<userData>/embedding-server/` never exists on Windows or Linux.

### Mobile is OPFS

`platform/browser.cljs:196` classifies the Capacitor WebView as `:capacitor` and uses the same OPFS adapter as the web build. There is no native-filesystem path for DB graphs on mobile. iOS exposes `Documents/` and the `iCloud.com.logseq.logseq` container (`FileContainer.swift:15-19`); Android maps SAF document ids to `/storage/emulated/0/{,Documents/,Download/}` (`FileUtil.java:41-45`). Those serve file graphs and pickers only.

### Secrets

- Desktop: OS keychain, service `Logseq E2EE` (`platform/node.cljs:601`, keytar).
- Mobile: `capacitor-secure-storage`, key prefix `logseq.e2ee.` (`mobile/secure_storage.cljs:9`).
- Browser: `localStorage` keys `id-token`, `access-token`, `refresh-token` (`handler/user.cljs:96-101`).
- Desktop/CLI shared tokens: plaintext `~/logseq/auth.json`.

### Naming quirks

- Graph dirs are URL-encoded with `%`→`~` (`graph_dir.cljs:6`): `a/b` → `a~2Fb`.
- `client-ops-/db.sqlite` (trailing hyphen, `db_core.cljs:445`) coexists with `client-ops/db.sqlite` (`:133`); `:846-856` migrates between them.
- OPFS uses two names for one graph: directory `.logseq-pool-<g>`, VFS `logseq-pool-<g>`.

### Addenda — plugins, backups, mirror, config.edn, assets

- **`config.edn` is three things.** For DB graphs it is not a file: `logseq/config.edn` is a virtual file entity inside the database (`repo_config.cljs:18`, seeded from `templates/config.edn` at `db_core.cljs:75`). File graphs keep it at `<graph>/logseq/config.edn`. The desktop-only *global* config is `~/.logseq/config/config.edn` (`global_config.cljs:22-27`). `<userData>/configs.edn` is a fourth, unrelated file (Electron settings).
- **Mobile and browser assets are in IndexedDB.** `platform/browser.cljs:106-111` routes assets to lightning-fs at `memory:///<graph>/assets/`, not OPFS and not the native filesystem — so they are invisible to the Files app and iCloud.
- **Plugin storage has two roots.** `logseq.FileStorage` writes `~/.logseq/storages/<id>/` by default and `<graph-dir>/assets/storages/<id>/` with `{assets: true}` (`api/plugin.cljs:82-90`). `write_dotdir_file` uses `~/.logseq/tmp/`. Writes elsewhere are refused (`electron/plugin.cljs:41-45`).
- **Two backup directories coexist for DB graphs.** Named/automatic backups go to `<graph-dir>/backup/<graph>-<ts>/` (`graph_backup.cljs:10`); the older `<graph-dir>/backups/` is still defined in `sqlite.cljs:35-38`. Auto-backup is throttled to hourly and keeps 12 (`db.cljs:15-18`), deduped 6-hourly-then-daily (`backup_file.cljs:34-38`).
- **`custom.css` / `custom.js` follow `config.edn`.** Virtual `logseq/custom.css` and `logseq/custom.js` entities inside a DB graph (`handler/ui.cljs:114-116,132-133`); `<graph>/logseq/custom.{css,js}` on disk for file graphs (`config.cljs:374-381`); a Settings URL overrides either. No global custom.css exists — only `config.edn` is global.
- **"Preferences" is four stores.** Electron `<userData>/configs.edn` (machine-level toggles via `:userAppCfgs`), `localStorage` (~35 UI keys plus auth tokens, cleared on logout — `user.cljs:131-141`), the graph's `config.edn`, and the global `~/.logseq/config/config.edn`. Two plugin-scoped files share the name: `~/.logseq/preferences.json` (plugin SDK JSON bag; an IndexedDB item on web/mobile — `api/plugin.cljs:240-262`) and `~/.logseq/config/plugins.edn` (installed-plugin manifest for "Replace plugins" — `plugin_config.cljs:26-28`).
- **The Markdown mirror never leaves the graph dir**: `<graph-dir>/mirror/markdown/{pages,journals}/*.md` (`markdown_mirror.cljs:15-17,97-110`).

## Not in the repo

- Electron's `<userData>` and electron-log's file locations are framework defaults, cited from their documentation for the pinned versions (`electron 42.3.0`, `electron-log 5.4.3` in `resources/package.json`).
- Snap and Flatpak confinement remap `~/.config` and `~/logseq`; the packaging repos, not this one, decide how.

## Related

- [Default Paths by Platform](../ddocs/reference/platforms/default-paths.md)
- Drift check: `dz/support-engineering/script/sync-limits/default-paths.edn` with `check_sync_limits.clj --declaration`
- [025 — RTC Limits and Quotas](025-rtc-limits-and-quotas.md)
