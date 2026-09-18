---
title: 'Troubleshooting Guide'
status: 🟢 Stable
version: 'Logseq 2.0.1'
quadrant: 'How-To'
platform: 'All Platforms'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
related-docs:
  - '[Performance Optimization Guide](../optimization/performance-optimization.md)'
  - '[Backup and Recovery Strategies](../organization/backup-strategies.md)'
  - '[Database Integrity](../../explanation/architecture/database-integrity.md)'
  - '[RTC vs File Sync](../../explanation/comparisons/rtc-vs-file-sync.md)'
  - '[File vs DB graphs](../../explanation/concepts/file-vs-db.md)'
tags: ['troubleshooting', 'debugging', 'errors', 'problems']
---

# Troubleshooting Guide

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

Diagnose and fix common Logseq problems in **DB graphs**. This guide covers startup issues, sync problems, performance, data integrity, plugins and platform quirks.

> **Scope:** checked against source at `be800f1711` (app version `2.0.1`, [`src/main/frontend/version.cljs:3`](../../../../../src/main/frontend/version.cljs)). Earlier versions of this page were written for file graphs. They described `.logseq/` folders, `logseq/bak`, markdown files, "Re-index", "Safe mode", "Clear cache" and File Sync. None of these apply to the current app. File graphs are no longer listed in the graph picker ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)), and File Sync was deleted in `bcc478b5f7` (2025-12-29).

**What This Covers:**

- Startup and loading issues
- Sync problems (db-sync)
- Performance and slowness
- Deleted, missing and invalid data
- Plugin, custom CSS and custom JS issues
- Platform-specific problems

**When to Use:**

- Logseq won't start or a graph won't open
- Sync is stuck or shows conflicts
- Pages or blocks are missing
- Plugins misbehave
- Performance has degraded

---

## Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Startup Issues](#startup-issues)
3. [Sync Problems](#sync-problems)
4. [Performance Issues](#performance-issues)
5. [Data Problems](#data-problems)
6. [Plugin Issues](#plugin-issues)
7. [Platform-Specific](#platform-specific)
8. [Advanced Recovery](#advanced-recovery)

---

## Quick Diagnostics

### First Steps

**Before diving into specific issues:**

```markdown
1. **Check Logseq version:**

   - Settings → General shows the version and a "Check for updates" button
   - Windows/Linux desktop: Help → About Logseq
   - macOS desktop: Logseq → About Logseq

2. **Reproduce the issue:**

   - Can you consistently trigger it?
   - Does it still happen with plugins disabled?
   - Is it graph-specific or global?

3. **Check logs:**
   - Desktop: View → Toggle Developer Tools → Console
   - Desktop graph worker: db-worker-node-YYYYMMDD.log in the graph folder
   - Plugins: Plugins → plugin card menu → View logs
```

Where each of these comes from:

- **Version and updates.** The General tab has a version row with the updater ([`settings.cljs:575-577`](../../../../../src/main/frontend/components/settings.cljs), used at [`:889`](../../../../../src/main/frontend/components/settings.cljs); label "Check for updates" at [`en.edn:1748`](../../../../../src/resources/dicts/en.edn)). The desktop app menu has "About Logseq" in Help on Windows/Linux ([`src/electron/electron/core.cljs:267-272`](../../../../../src/electron/electron/core.cljs), label [`en.edn:572`](../../../../../src/resources/dicts/en.edn)). On macOS it uses the standard `about` role in the app menu ([`core.cljs:221-222`](../../../../../src/electron/electron/core.cljs)). Settings → Advanced has an "Auto updater" row on macOS and Windows desktop only ([`settings.cljs:936`](../../../../../src/main/frontend/components/settings.cljs)).
- **Developer tools.** Logseq has no menu item or command of its own for DevTools. The desktop menu includes Electron's standard `viewMenu` role ([`core.cljs:246`](../../../../../src/electron/electron/core.cljs)), and Electron fills that menu with Reload, Force Reload and Toggle Developer Tools. The web app uses the browser's own DevTools.
- **Worker log.** See [Log files](#log-files) below.

> **Removed:** "Help → Developer tools". The desktop Help menu has only "Official Documentation" and, on Windows/Linux, "About Logseq" ([`core.cljs:262-272`](../../../../../src/electron/electron/core.cljs)).
>
> **Removed:** system requirements "4GB+ RAM, iOS 14+, Android 8+, 500MB free". No RAM or disk figure exists in source. The iOS app target (`com.logseq.logseq`) requires iOS 16.6 ([`ios/App/App.xcodeproj/project.pbxproj:674`](../../../../../ios/App/App.xcodeproj/project.pbxproj), [`:707`](../../../../../ios/App/App.xcodeproj/project.pbxproj)). The project-level default of 15.0 at `:596` and `:650` is overridden by that target. Android sets `minSdkVersion = 24` ([`android/variables.gradle:2`](../../../../../android/variables.gradle)).

### Isolate Plugins (there is no Safe Mode)

> **Removed:** "Settings → Advanced → Safe mode". No safe mode exists. `safe-mode` and "safe mode" match nothing in `src/` or `deps/`. The Advanced tab rows are: Auto updater, usage diagnostics, Developer mode, Sync Server URL, Publish Server URL, Network proxy, and "Automatically change file permissions" ([`settings.cljs:930-943`](../../../../../src/main/frontend/components/settings.cljs)).

**Test without plugins instead:**

```markdown
1. **Disable plugins:**

   - Open the header ⋯ menu → Plugins
   - Turn off each plugin's toggle on its card
   - Web app only: Settings → Features → Plugins turns off the whole plugin system

2. **If the problem disappears:**

   - The cause is a plugin or theme
   - Re-enable plugins one by one to find the culprit

3. **If the problem persists:**
   - Check custom.css (Settings → General → Custom theme → Edit custom.css)
   - Continue troubleshooting below
```

- **Per-plugin toggle.** The Plugins dashboard is in the header ⋯ menu ([`header.cljs:215-216`](../../../../../src/main/frontend/components/header.cljs)). Each plugin card has an on/off toggle ([`plugins.cljs:324-328`](../../../../../src/main/frontend/components/plugins.cljs)).
- **Whole plugin system (web only).** The "Plugins" switch in Settings → Features appears only when `web-platform?` is true ([`settings.cljs:1166-1167`](../../../../../src/main/frontend/components/settings.cljs)). That flag is false on desktop and on native mobile ([`src/main/frontend/util.cljc:128-130`](../../../../../src/main/frontend/util.cljc)).
- **Custom CSS.** The General tab row "Custom theme" opens "Edit custom.css" ([`settings.cljs:217-223`](../../../../../src/main/frontend/components/settings.cljs), labels [`en.edn:1753`](../../../../../src/resources/dicts/en.edn), [`:1755`](../../../../../src/resources/dicts/en.edn)).
- **Desktop check.** No Logseq setting to disable all plugins at once on desktop was found.

### Log Files

- **Desktop graph worker.** On desktop the graph database runs in a separate db-worker process. It writes `db-worker-node-YYYYMMDD.log` into the graph's own folder ([`src/main/logseq/db_worker/log.cljs:36-46`](../../../../../src/main/logseq/db_worker/log.cljs)). It keeps the newest 7 files ([`log.cljs:56-61`](../../../../../src/main/logseq/db_worker/log.cljs)). The log is installed when the worker starts ([`src/main/frontend/worker/db_worker_node.cljs:572-574`](../../../../../src/main/frontend/worker/db_worker_node.cljs)). With the default root, the graph folder is `~/logseq/graphs/<graph>/` ([`src/main/frontend/worker/db_worker_node_lock.cljs:22-26`](../../../../../src/main/frontend/worker/db_worker_node_lock.cljs)).
- **Electron main process.** `electron.logger` sends messages to `electron-log` and configures no file location ([`src/electron/electron/logger.cljs:1-4`](../../../../../src/electron/electron/logger.cljs), [`:31-40`](../../../../../src/electron/electron/logger.cljs)), so electron-log's defaults apply. The pinned version is `5.4.3` ([`resources/package.json:33`](../../../../../resources/package.json)). That version writes `main.log` under Electron's `app.getPath('logs')`.
  > **Unverified:** the exact folder. It comes from the electron-log library and Electron, not Logseq source. On macOS it is normally `~/Library/Logs/Logseq/`.
- **Plugins.** Each plugin card's menu has "View logs" ([`plugins.cljs:278`](../../../../../src/main/frontend/components/plugins.cljs), label [`en.edn:1278`](../../../../../src/resources/dicts/en.edn)).
- **Sync.** The sync indicator in the header has a "More debug info" link ([`src/main/frontend/components/rtc/indicator.cljs:221-222`](../../../../../src/main/frontend/components/rtc/indicator.cljs)).

### Diagnostic Checklist

```markdown
## Initial Diagnostic

- [ ] Logseq version (Settings → General): **\_**
- [ ] Platform: desktop / web / iOS / Android
- [ ] Operating system: **\_**
- [ ] Sync enabled: Yes / No
- [ ] Plugins installed: **\_**
- [ ] custom.css / custom.js in use: Yes / No
- [ ] Issue reproducible: Yes / No
- [ ] Affects all graphs: Yes / No
- [ ] Started after: **\_**
```

The bug report link fills in App Version, Git Revision, Platform, Language and installed Plugins for you ([`header.cljs:162-173`](../../../../../src/main/frontend/components/header.cljs)).

---

## Startup Issues

### Where Logseq Keeps Things (desktop)

Know what lives where before you delete anything:

| What                            | Where                                                                                      | Source                                                                                                                                                                                         |
| ------------------------------- | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Graphs (one folder per graph)   | `~/logseq/graphs/<graph>/db.sqlite`                                                        | [`deps/common/src/logseq/common/config.cljs:58`](../../../../../deps/common/src/logseq/common/config.cljs), [`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs) |
| Graphs dir override             | `LOGSEQ_GRAPHS_DIR` environment variable                                                   | [`deps/common/src/logseq/common/graph.cljs:111-114`](../../../../../deps/common/src/logseq/common/graph.cljs)                                                                                  |
| Automatic backups               | `~/logseq/graphs/<graph>/backup/`                                                          | [`src/main/logseq/db_worker/graph_backup.cljs:10`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)                                                                                 |
| Graph assets                    | `assets/` in the graph folder                                                              | [`config.cljs:59`](../../../../../deps/common/src/logseq/common/config.cljs)                                                                                                                   |
| Graph lock                      | `db-worker.lock` in the graph folder                                                       | [`db_worker_node_lock.cljs:47-49`](../../../../../src/main/frontend/worker/db_worker_node_lock.cljs)                                                                                          |
| Graph registry, plugins         | `~/.logseq/graphs.edn`, `~/.logseq/plugins/`                                               | [`src/electron/electron/configs.cljs:10`](../../../../../src/electron/electron/configs.cljs), [`:14-16`](../../../../../src/electron/electron/configs.cljs), [`src/electron/electron/utils.cljs:129-133`](../../../../../src/electron/electron/utils.cljs) |
| App configs                     | `configs.edn` in Electron's `userData` directory                                           | [`configs.cljs:11-12`](../../../../../src/electron/electron/configs.cljs)                                                                                                                      |
| `config.edn`, `custom.css`, `custom.js` | **Inside the database**, not on disk                                               | [`deps/db/src/logseq/db/sqlite/create_graph.cljs:205-217`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)                                                                     |

A DB graph has no `pages/`, `journals/`, `logseq/bak` or `.recycle` folders.

### Logseq Won't Start

**Symptoms:** Application doesn't launch, crashes immediately.

**Solution 1: Intel build on Apple Silicon (macOS)**

On an Apple Silicon Mac, if the Intel build is running through Rosetta, Logseq shows the warning "You are using the Intel (x64) release on Apple Silicon." ([`en.edn:598`](../../../../../src/resources/dicts/en.edn)). The detail says: "This release runs through Rosetta and can be very slow. Download the Apple Silicon (arm64) release first." ([`en.edn:597`](../../../../../src/resources/dicts/en.edn)). The check is in [`src/electron/electron/release_warning.cljs:9-13`](../../../../../src/electron/electron/release_warning.cljs) and runs at startup ([`core.cljs:301-303`](../../../../../src/electron/electron/core.cljs), [`:408`](../../../../../src/electron/electron/core.cljs)). Install the arm64 build.

**Solution 2: Reset app settings (graphs are not stored there)**

Electron's `userData` directory holds Logseq's `configs.edn` ([`configs.cljs:11-12`](../../../../../src/electron/electron/configs.cljs)). It is **not** where graphs live. Graphs are under `~/logseq/graphs` (see the table above).

> **Unverified:** everything else in the `userData` folder and what resetting it fixes. Chromium profile data such as `localStorage` is normally kept there. That includes the saved custom.js permission ([`src/main/frontend/handler/ui.cljs:134`](../../../../../src/main/frontend/handler/ui.cljs)). For an app named `Logseq` ([`resources/package.json:2-3`](../../../../../resources/package.json)) Electron normally uses `~/Library/Application Support/Logseq` (macOS), `%APPDATA%\Logseq` (Windows) and `~/.config/Logseq` (Linux). Quit Logseq and **move** (don't delete) this folder to test.
>
> **Corrected:** the old warning "This removes all local data" was wrong for DB graphs, which are stored outside `userData`. Back up `~/logseq/graphs` anyway before any reset.

**Solution 3: Reinstall Logseq**

```markdown
1. Uninstall the app (graphs in ~/logseq/graphs are separate files)
2. Download a fresh installer
3. Install and start Logseq
4. Your DB graphs in ~/logseq/graphs are listed again in the graph picker
```

> **Unverified:** the download URL. `https://logseq.com/downloads` does not appear in source.
>
> **Removed:** "Re-add graphs: Open existing graph folder". File graphs cannot be opened ([`repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)). To bring a file graph in, use Import → "File to DB graph" ([`en.edn:926-927`](../../../../../src/resources/dicts/en.edn), [`src/main/frontend/components/imports.cljs:605`](../../../../../src/main/frontend/components/imports.cljs)).

**Solution 4: Check permissions**

Logseq must be able to write to `~/logseq/graphs` and `~/.logseq`. On desktop, Settings → Advanced has "Automatically change file permissions" ([`settings.cljs:856-868`](../../../../../src/main/frontend/components/settings.cljs), [`:942`](../../../../../src/main/frontend/components/settings.cljs)). Its description is "Disable to allow editing by multiple users with permissions granted by group membership." ([`en.edn:1700-1701`](../../../../../src/resources/dicts/en.edn)).

> **Unverified:** OS commands such as `sudo chown -R $(whoami) ~/logseq ~/.logseq`. They are standard ownership fixes and were not tied to a specific Logseq error.

### White Screen / Blank Page

**Symptoms:** Logseq opens but shows a blank screen.

**Solution 1: Reload**

```markdown
- Desktop: View → Reload (or Force Reload)
- Web: reload the browser tab
```

View → Reload comes from Electron's standard View menu ([`core.cljs:246`](../../../../../src/electron/electron/core.cljs)).

> **Removed:** "Help → Reload". The Help menu has no reload item ([`core.cljs:262-272`](../../../../../src/electron/electron/core.cljs)).

**Solution 2: Rule out custom CSS and plugins**

A bad `custom.css` rule can hide the UI. Edit it inside Logseq (Settings → General → Custom theme → Edit custom.css) or disable plugins as in [Isolate Plugins](#isolate-plugins-there-is-no-safe-mode). `custom.css` is a record in the database, not a file on disk ([`create_graph.cljs:211-215`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)). See [Custom CSS](../integrations/custom-css.md#troubleshooting).

**Solution 3: GPU flag**

> **Unverified:** `--disable-gpu` is a Chromium command-line switch. Logseq does not add or read it: there is no `appendSwitch` or `disableHardwareAcceleration` in `src/electron/`.
>
> **Removed:** "Settings → Advanced → Disable GPU acceleration". There is no such row ([`settings.cljs:930-943`](../../../../../src/main/frontend/components/settings.cljs)).
>
> **Removed:** "Reset Database: delete `.logseq/` and reindex". That was file-graph advice. A DB graph has no `.logseq/` cache folder to delete.

### Stuck on Loading Screen / Graph Won't Open

**Symptoms:** The graph never finishes loading, or one graph fails while others work.

**Solution 1: Another process holds the graph**

On desktop, a graph is served by one db-worker process that holds `db-worker.lock` in the graph folder ([`db_worker_node_lock.cljs:47-49`](../../../../../src/main/frontend/worker/db_worker_node_lock.cljs)):

- **Live holder.** If the lock's process is still alive, opening fails with `graph already locked` ([`db_worker_node_lock.cljs:102-103`](../../../../../src/main/frontend/worker/db_worker_node_lock.cljs)).
- **Stale lock.** If that process no longer exists, the lock is removed automatically ([`:104-105`](../../../../../src/main/frontend/worker/db_worker_node_lock.cljs)). You do not need to delete it by hand.
- **Detached servers.** Workers not owned by the desktop app are started detached, for example by the Logseq CLI ([`src/main/logseq/db_worker/daemon.cljs:226-229`](../../../../../src/main/logseq/db_worker/daemon.cljs)), so they can outlive a window.

```markdown
1. Close other Logseq windows showing the same graph
2. Stop any Logseq CLI server for that graph
3. Check the graph folder's db-worker-node-YYYYMMDD.log for errors
4. Reopen the graph
```

**Solution 2: Invalid config.edn**

If `logseq/config.edn` is not valid EDN, Logseq shows: "The file 'logseq/config.edn' is invalid. Please reload the app to in order to see the error and fix it." ([`en.edn:683`](../../../../../src/resources/dicts/en.edn), raised in [`src/main/frontend/handler/repo_config.cljs:20-28`](../../../../../src/main/frontend/handler/repo_config.cljs)). In a DB graph, `config.edn` is stored in the database. Fix it inside Logseq with Settings → General → "Edit config.edn" ([`settings.cljs:204-205`](../../../../../src/main/frontend/components/settings.cljs), label [`en.edn:1754`](../../../../../src/resources/dicts/en.edn)). There is no file on disk to delete.

**Solution 3: Validate the graph (Developer mode)**

```markdown
1. Settings → Advanced → Developer mode (on)
2. Open the command palette (mod+shift+p)
3. Run "(Dev) Validate current graph"
4. Read the notification; run it again after it tries to fix blocks
```

- **Developer mode.** The row is in Settings → Advanced ([`settings.cljs:580-587`](../../../../../src/main/frontend/components/settings.cljs), label [`en.edn:1703`](../../../../../src/resources/dicts/en.edn)). It is hidden on native mobile ([`settings.cljs:938`](../../../../../src/main/frontend/components/settings.cljs)).
- **The command.** `:dev/validate-db` is active only in developer mode ([`src/main/frontend/modules/shortcut/config.cljs:522-525`](../../../../../src/main/frontend/modules/shortcut/config.cljs)). The command palette is `mod+shift+p` ([`config.cljs:310-311`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).
- **What it reports.** It reports "Your graph is valid!" or "Validation detected N invalid block(s). These blocks may be buggy." It also attempts a fix ([`src/main/frontend/worker/db/validate.cljs:317-325`](../../../../../src/main/frontend/worker/db/validate.cljs)).
- **More detail.** See [Database Integrity](../../explanation/architecture/database-integrity.md).

**Solution 4: Restore from a backup**

See [Restore from Backup](#restore-from-backup) below.

> **Removed:** "Settings → Advanced → Re-index graph", "Delete `.logseq/graphs/<graph>.db`", "Check `.logseq/graphs/` for a >500MB `.db` file", "Re-import from latest `.transit` export", "Consider exporting to file-based", and the incremental "copy files into a new graph" procedure. They describe file graphs or paths that do not exist:
>
> - **Search index only.** The only re-index command is "Rebuild search index" (`:search/re-index`, `mod+c mod+s`, [`config.cljs:337-338`](../../../../../src/main/frontend/modules/shortcut/config.cljs), label [`en.edn:414`](../../../../../src/resources/dicts/en.edn)). It rebuilds search indices only ([`src/main/frontend/handler/search.cljs:109-118`](../../../../../src/main/frontend/handler/search.cljs)).
> - **Debug transit is not a backup.** The "Export debug transit file" strips sensitive data ([`en.edn:633`](../../../../../src/resources/dicts/en.edn)).
> - **No file-based export target.** There is no export to a file-based graph.

---

## Sync Problems

Logseq Sync is **db-sync**, the only cloud sync. File Sync was removed in `bcc478b5f7`. See [RTC vs File Sync](../../explanation/comparisons/rtc-vs-file-sync.md).

### Sync Not Working

**Symptoms:** Changes don't reach other devices.

**Solution 1: Read the sync indicator**

The sync indicator appears in the header only when three things are true: you are logged in, your account has sync access (`rtc-group?`), and the current graph is a remote graph ([`header.cljs:99-109`](../../../../../src/main/frontend/components/header.cljs), [`:576-587`](../../../../../src/main/frontend/components/header.cljs)). If it is missing, check those three first. Its details panel ([`rtc/indicator.cljs:191-245`](../../../../../src/main/frontend/components/rtc/indicator.cljs)) shows:

```markdown
- Online / Offline
- N pending local changes
- N pending server changes
- Missing asset files, asset uploads/downloads
- Last synced time
- More debug info (local/remote tx, logs)
- A "Start sync" button when sync is not running
```

"Start sync" restarts the sync client ([`indicator.cljs:238-245`](../../../../../src/main/frontend/components/rtc/indicator.cljs)).

**Solution 2: Check connectivity**

```markdown
1. Is the indicator Offline? Check your connection, VPN and firewall
2. Desktop behind a proxy: Settings → Advanced → Network proxy
3. DevTools → Network tab: look for failed requests
```

The "Network proxy" row is desktop-only ([`settings.cljs:851-854`](../../../../../src/main/frontend/components/settings.cljs), [`:941`](../../../../../src/main/frontend/components/settings.cljs); label [`en.edn:1707`](../../../../../src/resources/dicts/en.edn)).

> **Removed:** `https://status.logseq.com`. No status page URL appears in source.

**Solution 3: Sign out and back in**

```markdown
1. Header ⋯ menu → Logout
2. Restart Logseq
3. Header ⋯ menu → Login
4. Open the graph and check the sync indicator
```

"Login" and "Logout" are in the header ⋯ menu ([`header.cljs:243-246`](../../../../../src/main/frontend/components/header.cljs), [`:260-267`](../../../../../src/main/frontend/components/header.cljs); labels [`en.edn:1885-1886`](../../../../../src/resources/dicts/en.edn)).

> **Removed:** "Settings → Sync → Sign out / Sign in / Advanced / Clear sync cache / Force sync". Settings has no Sync tab. The tabs are General, Editor, Keymap, AI (desktop), Advanced, Features, Collaboration and Encryption (when logged in), and plugin settings ([`settings.cljs:1488-1506`](../../../../../src/main/frontend/components/settings.cljs)). No "Clear sync cache" or "Force sync" action was found.

**Solution 4: Self-hosted sync server**

If you set Settings → Advanced → "Sync Server URL", make sure it is right, or use "Reset to default" ([`settings.cljs:939`](../../../../../src/main/frontend/components/settings.cljs), [`en.edn:1772-1776`](../../../../../src/resources/dicts/en.edn)). The URL must start with `https://` or `http://`. Your Logseq authentication tokens are sent to that server ([`en.edn:1775`](../../../../../src/resources/dicts/en.edn)).

**Solution 5: Limits**

- **Asset size.** The db-sync server rejects an asset larger than 100 MB with "asset too large", or 200 MB for encrypted graphs ([`deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs:9-10`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs), [`:106-107`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs), [`:230`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs)).
- **Account limits.** Hitting a limit shows "Sync graph count exceed limit" or "Sync storage exceed limit" ([`en.edn:1837`](../../../../../src/resources/dicts/en.edn), [`:1852`](../../../../../src/resources/dicts/en.edn)).

Remove or shrink very large assets.

**Solution 6: Forgotten encryption password**

For an end-to-end encrypted graph, Settings → Encryption (shown when logged in) has "Forgot password?" → "Force reset password" ([`settings.cljs:1296-1316`](../../../../../src/main/frontend/components/settings.cljs)). **This makes all currently encrypted graph data stored on the server permanently unreadable.** Afterwards you must re-upload your graphs from the client ([`en.edn:613`](../../../../../src/resources/dicts/en.edn)).

### Sync Conflicts

**Symptoms:** A small warning button appears on a block.

**How db-sync handles conflicts:**

```markdown
- Remote changes are applied automatically
- If a remote title overwrites a block you also edited locally,
  the remote version is recorded as a sync conflict
- The block shows a warning button ("Show sync conflicts")
```

**Resolve:**

```markdown
1. Click the warning button on the block
2. The "Sync conflicts" popup lists the server versions
3. Copy the text you want and edit the block manually
4. Click "Mark resolved"
```

The popup and button are in [`block.cljs:3424-3466`](../../../../../src/main/frontend/components/block.cljs). The description reads "These server versions conflicted with local edits. Copy the text you want and edit the block manually." ([`en.edn:1832`](../../../../../src/resources/dicts/en.edn)). "Mark resolved" clears the block's recorded conflicts ([`block.cljs:3457-3462`](../../../../../src/main/frontend/components/block.cljs)). Only `:block/title` conflicts are recorded ([RTC vs File Sync](../../explanation/comparisons/rtc-vs-file-sync.md)).

> **Removed:** "Search: `{{query (page "conflict-resolution")}}`". No such page exists, and `{{query}}` is deprecated. In a DB graph it renders only the warning "{{query}} is deprecated. Use '/Query' command instead." ([`block.cljs:1887-1888`](../../../../../src/main/frontend/components/block.cljs), [`en.edn:159`](../../../../../src/resources/dicts/en.edn)).
>
> **Removed:** "Conflict blocks marked with timestamp, both versions preserved" and "Remove conflict marker". db-sync writes no conflict blocks or markers.
>
> **Removed:** "Reset Sync State: Settings → Export graph → JSON/EDN, Settings → Sync → Clear sync cache". There is no JSON export, no Sync tab and no clear-cache action.

**Prevent conflicts:**

```markdown
- Let pending local changes reach 0 (sync indicator) before switching devices
- Avoid editing the same block title on two devices while one is offline
```

### Sync Slow or Stalled

**Symptoms:** Pending changes never reach zero, or assets keep uploading.

```markdown
1. Open the sync indicator: check Online, pending local/server changes,
   and missing asset files
2. Click "Start sync" if sync is not running
3. Large assets upload slowly; assets over 100 MB are rejected
4. Restart Logseq
5. Use "More debug info" when reporting the problem
```

---

## Performance Issues

See the audited [Performance Optimization Guide](../optimization/performance-optimization.md) for detail.

### Logseq is Slow

**Symptoms:** Laggy typing, slow navigation, delayed actions.

**Solution 1: Find heavy content**

```markdown
1. Split very long blocks: Logseq warns "Large block will not be editable
   or searchable to not slow down the app"
2. Split very long pages
3. Narrow or remove queries on pages you keep open
4. Replace deprecated {{embed}} macros with /Node embed
```

- **Large block warning.** The text is at [`en.edn:129`](../../../../../src/resources/dicts/en.edn).
- **`{{embed}}`.** It renders only a deprecation warning ([`block.cljs:1934-1935`](../../../../../src/main/frontend/components/block.cljs)).

> **Removed:** "Large graphs (>10,000 blocks) may slow down" and "Limit results with :limit". Source sets no block-count threshold, and `:limit` is not a supported way to cap query results.

**Solution 2: Slow plugins**

When a plugin takes too long to load, Logseq notifies you with "This plugin … takes too long to load, affecting the application startup time and potentially causing other plugins to fail to load." The notice has a "Disable now" button ([`en.edn:1283`](../../../../../src/resources/dicts/en.edn), [`plugins.cljs:1797-1822`](../../../../../src/main/frontend/components/plugins.cljs), triggered from [`src/main/frontend/handler/plugin.cljs:1313`](../../../../../src/main/frontend/handler/plugin.cljs)). Otherwise disable plugins one by one.

**Solution 3: Stale search**

Run "Rebuild search index" (`mod+c mod+s`) ([`config.cljs:337-338`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

> **Removed:**
>
> - **Invented settings.** "Settings → Auto-sync", "Settings → Real-time collaboration" and "Settings → Auto-save frequency" do not exist.
> - **No such Advanced rows.** "Settings → Advanced → Disable GPU", "Optimize database", "Re-index" and "Clear cache" are not in Settings → Advanced ([`settings.cljs:930-943`](../../../../../src/main/frontend/components/settings.cljs)). A "clear cache" row exists only as commented-out code ([`settings.cljs:569-573`](../../../../../src/main/frontend/components/settings.cljs), [`:943`](../../../../../src/main/frontend/components/settings.cljs)).
> - **"Increase RAM allocation".** This was generic and had no Logseq-specific steps.

**Solution 4: Hardware**

> **Unverified:** keeping `~/logseq/graphs` on an SSD and closing other applications. These are generic suggestions and were not measured.

### High Memory Usage

**Symptoms:** Logseq uses excessive RAM.

```markdown
1. Desktop: View → Toggle Developer Tools → Memory tab → take a heap snapshot
2. Disable plugins and compare
3. Close right-sidebar items and pages with heavy queries
4. Restart Logseq
```

> **Removed:** "Settings → Advanced → Clear cache". The row is commented out ([`settings.cljs:943`](../../../../../src/main/frontend/components/settings.cljs)).

### Slow Sync

See [Sync Slow or Stalled](#sync-slow-or-stalled) above.

---

## Data Problems

### Missing Content

**Symptoms:** Blocks or pages disappeared.

**Solution 1: Check Recycle**

Deleted pages and blocks go to the **Recycle** page. "Deleted pages and blocks stay here until restored or automatically garbage collected after 30 days." ([`en.edn:1825`](../../../../../src/resources/dicts/en.edn)).

```markdown
1. Header ⋯ menu → Recycle
2. Find the page or block ("Page deleted …" / "Block deleted …")
3. Click "Restore"
```

- **Menu entry.** "Recycle" appears in the header ⋯ menu once the Recycle page exists ([`header.cljs:223-226`](../../../../../src/main/frontend/components/header.cljs), [`:333-335`](../../../../../src/main/frontend/components/header.cljs); [`src/main/frontend/handler/page.cljs:45-49`](../../../../../src/main/frontend/handler/page.cljs)).
- **Restore.** The button is rendered in [`src/main/frontend/components/recycle.cljs:58`](../../../../../src/main/frontend/components/recycle.cljs), with labels at [`en.edn:1820-1826`](../../../../../src/resources/dicts/en.edn).

**Solution 2: Search thoroughly**

```markdown
1. Search (mod+k) with different keywords
2. If results look stale, run "Rebuild search index" (mod+c mod+s)
```

**Solution 3: Restore from backup**

See [Restore from Backup](#restore-from-backup).

> **Removed:** "Check `.logseq/bak/`" and "Check `.logseq/graphs/<graph>.transit` exports". These do not exist for DB graphs.
>
> **Removed:** file-recovery software (Recuva, TestDisk, PhotoRec) and "Trash (if plugin installed)". They are not Logseq-specific, and Recycle is built in.
>
> **Unverified:** "Sync history / contact support for recovery". No server-side version history UI was found.

### Invalid or Corrupted Data

**Symptoms:** Blocks behave strangely, imports warn about invalid blocks.

```markdown
1. Run "(Dev) Validate current graph" (see Graph Won't Open → Solution 3)
2. Run it again to confirm fixes
3. If problems remain, restore the graph from a backup
```

When an import finds invalid blocks it reports: "Import detected N invalid block(s). These blocks may be buggy when you interact with them. See the javascript console for more." ([`en.edn:935`](../../../../../src/resources/dicts/en.edn)).

> **Removed:** "Fix Encoding" with `iconv` and "Repair markdown files". A DB graph stores content in `db.sqlite`, not markdown files.

### Duplicate Content

**Symptoms:** Two pages with similar names.

Page names must be unique for a tag. For example, "Apple #Company" and "Apple #Fruit" can both exist ([`deps/outliner/src/logseq/outliner/validate.cljs:125-132`](../../../../../deps/outliner/src/logseq/outliner/validate.cljs)). The check runs when a page title changes ([`:145-149`](../../../../../deps/outliner/src/logseq/outliner/validate.cljs)). To merge them, move the content into one page, update references, and delete the other page. Deleted pages go to Recycle.

> **Removed:**
>
> - **Duplicate Finder plugin.** Not verified to exist.
> - **"Run deduplication script".** No such script exists.
> - **"Enable 'Show existing pages' in autocomplete".** No such setting exists.
> - **"Use block references" wording.** In a DB graph a block reference is `[[<uuid>]]` (Copy block ref), not `((uuid))` ([`src/main/frontend/util/ref.cljs:5-7`](../../../../../src/main/frontend/util/ref.cljs)).

---

## Plugin Issues

Plugins run without a sandbox. See [Plugin Security](../../explanation/architecture/plugin-security.md). Plugins are available on desktop and in the web app, not in the native mobile apps (`plugin-platform?`, [`util.cljc:131`](../../../../../src/main/frontend/util.cljc)).

### Plugin Won't Install

**Symptoms:** A plugin fails to install from the Marketplace or manually.

**Solution 1: Check compatibility**

```markdown
1. Check that the plugin supports DB graphs and your Logseq version
2. Read the plugin's installation instructions
```

**Solution 2: Load an unpacked plugin (desktop, Developer mode)**

```markdown
1. Settings → Advanced → Developer mode (on)
2. Header ⋯ menu → Plugins
3. Click "Load unpacked plugin" and select the plugin directory
```

The button appears only on desktop, in developer mode, outside the Marketplace view ([`plugins.cljs:829-833`](../../../../../src/main/frontend/components/plugins.cljs); labels [`en.edn:1264`](../../../../../src/resources/dicts/en.edn), [`:1302`](../../../../../src/resources/dicts/en.edn)).

> **Corrected:** "Settings → Plugins → Load unpacked plugin". Load unpacked is in the Plugins dashboard, not Settings.

**Solution 3: Remove a broken plugin folder (desktop)**

```markdown
1. Quit Logseq
2. Move the plugin's folder out of ~/.logseq/plugins/
3. Restart Logseq and reinstall from the Marketplace
```

Installed plugins live in `~/.logseq/plugins/<id>` ([`src/electron/electron/plugin.cljs:227`](../../../../../src/electron/electron/plugin.cljs), [`utils.cljs:129-133`](../../../../../src/electron/electron/utils.cljs)).

> **Corrected:** the old paths `~/Library/Application Support/Logseq/plugins`, `%APPDATA%/Logseq/plugins` and `~/.config/Logseq/plugins`. Plugins are under `~/.logseq`, not Electron's `userData`.

### Plugin Not Working

**Symptoms:** A plugin is installed but not functioning.

**Solution 1: Check the plugin card**

```markdown
1. Header ⋯ menu → Plugins → Installed
2. Check the card's toggle is on (filter by Enabled / Disabled)
3. Card menu → View logs
4. Card menu → Open settings
```

- **Enabled/Disabled filters.** [`plugins.cljs:875-880`](../../../../../src/main/frontend/components/plugins.cljs).
- **Card toggle.** [`plugins.cljs:324-328`](../../../../../src/main/frontend/components/plugins.cljs).
- **Card menu.** Open settings, View logs, Open package, Report plugin, Uninstall ([`plugins.cljs:277-292`](../../../../../src/main/frontend/components/plugins.cljs)).

**Solution 2: Review console logs**

```markdown
1. Desktop: View → Toggle Developer Tools → Console
2. Look for plugin-related errors
3. Report to the plugin developer
```

**Solution 3: Update the plugin**

```markdown
1. Header ⋯ menu → Plugins
2. Click "Check update" on the card, then "Update"
3. Use "Restart App" if prompted
```

Labels are at [`plugins.cljs:321-322`](../../../../../src/main/frontend/components/plugins.cljs) and [`en.edn:1231`](../../../../../src/resources/dicts/en.edn), [`:1292`](../../../../../src/resources/dicts/en.edn), [`:1304`](../../../../../src/resources/dicts/en.edn).

**Solution 4: Reinstall**

```markdown
1. Card menu → Uninstall
2. Restart Logseq
3. Reinstall from the Marketplace
4. Configure settings again
```

### custom.js Not Running

A **Cancel** on the "Found the custom.js file, is it allowed to execute?" prompt ([`en.edn:1235`](../../../../../src/resources/dicts/en.edn)) is stored as `false` ([`ui.cljs:137-140`](../../../../../src/main/frontend/handler/ui.cljs)). After that, Logseq never runs custom JS and never asks again ([`ui.cljs:153-154`](../../../../../src/main/frontend/handler/ui.cljs)). Custom JS is not executed in the web app ([`plugins.cljs:1790-1793`](../../../../../src/main/frontend/components/plugins.cljs)). See [Custom JavaScript](../integrations/custom-js.md) for how to reset the prompt.

### Plugin Conflicts

**Symptoms:** Several plugins interfere with each other.

```markdown
1. Turn off all plugin toggles
2. Enable one at a time and test after each
3. Identify the conflicting pair
4. Report to both plugin developers with plugin names and versions
```

> **Unverified:** "Adjust order in plugin settings". No plugin load-order setting was found.

### Plugin Performance Issues

**Symptoms:** A specific plugin causes slowness or crashes.

```markdown
1. Watch for the "takes too long to load" notice and use "Disable now"
2. Or disable the plugin manually and compare performance
3. Reduce the plugin's scope in its own settings, if it offers any
4. Report to the developer or find an alternative in the Marketplace
```

---

## Platform-Specific

### macOS Issues

**Symptom: Very slow app on Apple Silicon**

You are probably running the Intel build through Rosetta. See [Logseq Won't Start → Solution 1](#logseq-wont-start).

**Symptom: Can't open Logseq (security prompt)**

> **Unverified:** OS-level Gatekeeper workaround. Right-click Logseq.app → Open, or allow it in System Settings → Privacy & Security.

**Symptom: Graph in iCloud Drive or another file-sync folder**

DB graphs live in `~/logseq/graphs` ([`config.cljs:58`](../../../../../deps/common/src/logseq/common/config.cljs)), unless you set `LOGSEQ_GRAPHS_DIR` ([`graph.cljs:111-114`](../../../../../deps/common/src/logseq/common/graph.cljs)). Use Logseq Sync (db-sync) to share a graph between devices.

> **Unverified:** syncing a live `db.sqlite` through iCloud, Dropbox or OneDrive can corrupt it. This is general SQLite guidance and was not tested with Logseq.

**Symptom: Permission errors**

> **Unverified:** granting Full Disk Access in System Settings. First check that you own `~/logseq` and `~/.logseq`.

### Windows Issues

**Symptom: Firewall or proxy blocking sync**

```markdown
1. Allow Logseq through Windows Defender Firewall
2. If you are behind a proxy: Settings → Advanced → Network proxy
```

> **Unverified:** the firewall steps are OS-level. The proxy row is verified ([`settings.cljs:941`](../../../../../src/main/frontend/components/settings.cljs)).

**Symptom: Path Too Long**

> **Unverified:** Windows' 260-character path limit. Graph folders are created under `~/logseq/graphs` rather than a folder you pick ([`config.cljs:58`](../../../../../deps/common/src/logseq/common/config.cljs)), so "move the graph folder closer to the drive root" does not apply unless you use `LOGSEQ_GRAPHS_DIR`. Enabling Win32 long paths through Group Policy is an OS setting and was not tested.

**Symptom: OneDrive conflicts**

See the file-sync note under macOS. Use Logseq Sync instead of OneDrive for the graphs folder.

### Linux Issues

Desktop releases include an AppImage ([`.github/workflows/build-desktop-release.yml:268`](../../../../../.github/workflows/build-desktop-release.yml), [`:326`](../../../../../.github/workflows/build-desktop-release.yml)). The in-app "Auto updater" row is not shown on Linux ([`settings.cljs:936`](../../../../../src/main/frontend/components/settings.cljs)).

**Symptom: AppImage won't run**

```markdown
1. Make it executable:
   chmod +x Logseq-*.AppImage

2. Run:
   ./Logseq-*.AppImage
```

> **Unverified:** installing FUSE (`libfuse2`) is a common AppImage requirement, not something in Logseq source.
>
> **Removed:** the list of `apt install` libraries and "use Flatpak/Snap versions". No dependency list or Flatpak/Snap build was found in source.

**Symptom: Wayland issues**

> **Unverified:** `GDK_BACKEND=x11` or Ozone flags. Logseq sets no Wayland or Ozone switches in `src/electron/`.

### Mobile Issues (iOS/Android)

- **Platforms.** The iOS app requires iOS 16.6 or later, and Android requires `minSdkVersion` 24 (see [Quick Diagnostics](#first-steps)).
- **Plugins.** Plugins are not available on native mobile ([`util.cljc:131`](../../../../../src/main/frontend/util.cljc)).
- **Developer mode.** The row is hidden on native mobile ([`settings.cljs:938`](../../../../../src/main/frontend/components/settings.cljs)).
- **Backups.** Automatic hourly backups are a desktop feature ([`src/electron/electron/db.cljs:15-18`](../../../../../src/electron/electron/db.cljs)).

**Symptom: App crashes**

```markdown
1. Update to the latest version
2. Restart the device
3. Make sure the graph is synced before reinstalling
```

> **Unverified:** "Offload app" (iOS) and "Clear cache" (Android). These are OS-level steps. Whether they keep an unsynced local graph was not checked.

**Symptom: Sync not working on mobile**

```markdown
1. Check Wi-Fi / mobile data
2. Check you are logged in
3. Open the sync indicator and check Online / pending changes
```

> **Unverified:** Background App Refresh (iOS) and background data (Android) settings. Source does not show whether db-sync runs in the background on mobile.

---

## Advanced Recovery

### Restore from Backup

**Automatic backups (desktop):**

- **Schedule.** Logseq backs up each open graph every hour and keeps 12 versions ([`db.cljs:15-18`](../../../../../src/electron/electron/db.cljs)).
- **Location.** Backups go to `~/logseq/graphs/<graph>/backup/` ([`graph_backup.cljs:10`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)).
- **Contents.** Each backup is a directory holding `db.sqlite` and `metadata.edn` ([`graph_backup.cljs:11-12`](../../../../../src/main/logseq/db_worker/graph_backup.cljs), [`:61-69`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)).

**Manual export (all platforms):** open the header ⋯ menu → "Export graph" ([`header.cljs:229`](../../../../../src/main/frontend/components/header.cljs)). The Export dialog offers these options ([`src/main/frontend/components/export.cljs:119-154`](../../../../../src/main/frontend/components/export.cljs)):

| Option                                               | Notes                                                              |
| ---------------------------------------------------- | ------------------------------------------------------------------ |
| Export SQLite DB                                     | "Primary way to backup graph's content to a single .sqlite file."  |
| Export both SQLite DB and assets                     | "Primary way to backup graph's content and assets to a .zip file." |
| Export EDN file                                      | Not on mobile. "Don't rely on this as a primary backup."           |
| Export as standard Markdown (no block properties)    | Not on native mobile                                               |
| Export public pages                                  | Desktop only                                                       |
| Export debug transit file                            | For bug reports; sensitive data removed. **Not a backup.**         |

Labels are at [`en.edn:632-635`](../../../../../src/resources/dicts/en.edn), [`:647`](../../../../../src/resources/dicts/en.edn), [`:656`](../../../../../src/resources/dicts/en.edn), [`:661`](../../../../../src/resources/dicts/en.edn), [`:666`](../../../../../src/resources/dicts/en.edn), [`:678`](../../../../../src/resources/dicts/en.edn), [`:680`](../../../../../src/resources/dicts/en.edn).

**Restore into a new graph:**

```markdown
1. Header ⋯ menu → Import
2. Choose "SQLite + assets (.zip)"
3. Select a zip containing db.sqlite and an assets folder
```

The import option is at [`header.cljs:234`](../../../../../src/main/frontend/components/header.cljs) and [`imports.cljs:591-592`](../../../../../src/main/frontend/components/imports.cljs); labels at [`en.edn:950-951`](../../../../../src/resources/dicts/en.edn). A zip without `db.sqlite` fails with "Zip missing db.sqlite. Please check the archive structure." ([`en.edn:960`](../../../../../src/resources/dicts/en.edn)). For restoring an automatic backup's `db.sqlite` in place, see [Backup and Recovery Strategies](../organization/backup-strategies.md).

> **Not live-tested:** zipping an automatic backup's `db.sqlite` together with the graph's `assets/` folder and importing it this way.

### Inspecting the SQLite File

> **Unverified:** SQLite tooling on a **copy** of `db.sqlite`, with Logseq closed. This is standard SQLite practice and was not tested against Logseq graphs:
>
> ```bash
> cp ~/logseq/graphs/<graph>/db.sqlite /tmp/db-copy.sqlite
> sqlite3 /tmp/db-copy.sqlite "PRAGMA integrity_check;"
> ```
>
> **Corrected:** the old path `~/.logseq/graphs/<graph-id>.db`. The file is `~/logseq/graphs/<graph>/db.sqlite` ([`sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs)).
>
> **Removed:** "dump and rebuild, then `mv` over the original". Rebuilding from `.dump` and replacing the live file is not a documented Logseq procedure. Restore from a backup instead.

### Export and Clean Import

**Rebuild a graph from an export:**

```markdown
1. **Export current graph:**

   - Header ⋯ menu → Export graph → Export EDN file
     (or Export both SQLite DB and assets)

2. **Import into a new graph:**

   - Header ⋯ menu → Import → "EDN to DB graph"
     (or "SQLite + assets (.zip)")
   - Give the new graph a name

3. **Verify data:**

   - Check pages, queries and assets

4. **Switch to the new graph** and keep the old one until you are sure
```

"EDN to DB graph" imports a DB graph's EDN export into a new DB graph ([`en.edn:918-919`](../../../../../src/resources/dicts/en.edn)). EDN export validates the graph first and fails on invalid data ([Database Integrity](../../explanation/architecture/database-integrity.md)).

> **Removed:** "Settings → Export graph → JSON format" and "Settings → Import → Select exported JSON". Export and Import are in the header ⋯ menu, and there is no JSON graph export.

### Contact Support

**When all else fails:**

```markdown
1. **Gather information:**

   - Logseq version (Settings → General)
   - Platform and operating system
   - Error messages (Console, db-worker-node log)
   - Steps to reproduce
   - Screenshots

2. **Report:**

   - Help (?) button → Bug report → "Submit a bug report"
   - Optionally attach an "Export debug transit file" export
     (sensitive data removed)

3. **Ask the community:**
   - Help (?) button → Ask the community (Discord)
   - Help (?) button → Support forum (https://discuss.logseq.com/)
```

- **Help menu items.** [`src/main/frontend/components/container.cljs:244-256`](../../../../../src/main/frontend/components/container.cljs). The Discord item opens `https://discord.com/invite/KpN4eHY` ([`:253`](../../../../../src/main/frontend/components/container.cljs)).
- **Bug report page.** It offers a clipboard helper and "Submit a bug report" ([`src/main/frontend/components/bug_report.cljs:108-127`](../../../../../src/main/frontend/components/bug_report.cljs), [`en.edn:170-190`](../../../../../src/resources/dicts/en.edn)).
- **Where the report goes.** "Submit a bug report" opens a prefilled issue at `https://github.com/logseq/db-test/issues/new` ([`header.cljs:173`](../../../../../src/main/frontend/components/header.cljs)).

> **Removed:** "Help → Export logs" (no such command exists) and `support@logseq.com` (not in source). The old Discord link `https://discord.gg/logseq` was replaced with the invite URL the app uses.

### Data Recovery Services

> **Removed:** professional disk-recovery services and "Use version control (Git)". The first is not Logseq-specific. The second was file-graph advice: a DB graph is a single binary `db.sqlite`. Rely on automatic backups, "Export both SQLite DB and assets", and Recycle.

---

## Prevention Checklist

**Avoid future problems:**

```markdown
## Daily

- [ ] Let the sync indicator reach 0 pending local changes before closing
- [ ] Quit Logseq normally rather than force-quitting

## Weekly

- [ ] Check that ~/logseq/graphs/<graph>/backup/ has recent backups (desktop)
- [ ] Update Logseq (Settings → General → Check for updates)
- [ ] Check plugin updates (Plugins → Check update)

## Monthly

- [ ] Export graph → "Export both SQLite DB and assets" to another location
- [ ] Review Recycle before items are garbage collected (30 days)
- [ ] Review and narrow heavy queries
- [ ] Check disk space

## Best Practices

- [ ] Use Logseq Sync (db-sync) rather than file-sync folders for graphs
- [ ] Limit plugins to essential ones
- [ ] Keep backups in more than one location
- [ ] Test a restore into a new graph occasionally
- [ ] Keep a copy of your config.edn and custom.css content
```

> **Removed:** "Keep graphs reasonable size (<10k blocks)" and "Use database graphs for better performance". Source sets no size threshold, and DB graphs are the only kind you can open.

---

## Summary

**Quick Reference:**

| Problem               | First Try                                                        |
| --------------------- | ---------------------------------------------------------------- |
| Won't start           | Check the Apple Silicon warning, reinstall, check permissions     |
| White screen          | View → Reload, check custom.css, disable plugins                 |
| Graph won't open      | Close other windows/CLI servers, fix config.edn, validate graph  |
| Sync failing          | Sync indicator → Start sync, check proxy, log out and in         |
| Slow performance      | Split large blocks/pages, disable slow plugins                   |
| Missing data          | Recycle → Restore, then backups                                  |
| Plugin issues         | Card toggle, View logs, Check update, reinstall                  |

**Key Principles:**

1. **Diagnose before acting**: read the Console and db-worker log
2. **Start simple**: reload, then disable plugins
3. **Backup first**: export SQLite DB and assets before major changes
4. **Document steps**: note what you try
5. **Seek help**: use the Bug report page and community links in Help
6. **Prevent recurrence**: keep backups and test restores

**Resources:**

- [Official Documentation](https://docs.logseq.com/) ([`core.cljs:269`](../../../../../src/electron/electron/core.cljs))
- [Logseq Discord](https://discord.com/invite/KpN4eHY) ([`container.cljs:253`](../../../../../src/main/frontend/components/container.cljs))
- [Support Forum](https://discuss.logseq.com/) ([`container.cljs:254`](../../../../../src/main/frontend/components/container.cljs))
- [GitHub Issues](https://github.com/logseq/logseq/issues)

> **Removed:** the Status Page link `https://status.logseq.com`. It is not referenced in source.
