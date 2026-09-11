---
title: 'Mobile Platform Differences'
status: 🟢 Stable
version: 'DB graph schema 65.33'
quadrant: 'Reference'
platform: 'Mobile (iOS & Android)'
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
related-docs:
  - '[URI Schemes](./uri-schemes.md)'
  - '[Default Paths](./default-paths.md)'
  - '[Keyboard Shortcuts](../configuration/keyboard-shortcuts.md)'
  - '[Slash Commands](../interface/slash-commands.md)'
  - '[PDF Annotations](../features/pdf-annotations.md)'
tags: ['mobile', 'ios', 'android', 'capacitor', 'platform', 'limitations']
---

# Mobile Platform Differences

This reference covers the differences between Logseq's native mobile app (iOS and Android) and the desktop (Electron) and web builds. Each difference below was checked against the source at `be800f1711`, mostly against the platform checks in shared code (`mobile-util/native-platform?`, `util/electron?`, `util/web-platform?`).

**Platforms covered:**

- 📱 **iOS** and 🤖 **Android** — the Capacitor app
- 🖥️ **Desktop** — Electron
- 🌐 **Web** — the browser build (it differs from desktop in several rows below)

> **Scope:** everything here concerns DB graphs. File graphs cannot be opened on any platform: the graph list keeps only DB graphs (`src/main/frontend/components/repo.cljs:321-323`).

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Feature Support Matrix](#2-feature-support-matrix)
3. [UI & UX Differences](#3-ui--ux-differences)
4. [Storage](#4-storage)
5. [Plugin System, custom.js and custom.css](#5-plugin-system-customjs-and-customcss)
6. [Performance Considerations](#6-performance-considerations)
7. [Platform Detection](#7-platform-detection)
8. [Known Limitations](#8-known-limitations)
9. [For App Contributors](#9-for-app-contributors)

---

## 1. Architecture Overview

### Technology Stack

| Component | Desktop | Mobile | Source |
| --- | --- | --- | --- |
| **Runtime** | Electron | Capacitor app `com.logseq.app`, serving the `static/mobile` web bundle | `capacitor.config.ts:8-10` |
| **ClojureScript build** | `:app` (renderer) + `:electron` (main process) | `:mobile` | `shadow-cljs.edn:17`, `:132`, `:95` |
| **App shell** | `frontend.components.container` | `mobile.core` → `mobile.components.app`; startup still goes through the shared `frontend.handler/start!` | `src/main/mobile/core.cljs:89-99` |
| **Database worker** | Electron flavour of the db-worker | The browser db-worker, started with `capacitor=true` | `src/main/frontend/persist_db/browser.cljs:201-206`, `src/main/frontend/worker/platform/browser.cljs:192-197` |
| **Native APIs** | Electron IPC (`window.apis`, `ipc/ipc`) | Capacitor plugins (`App`, `Keyboard`, `Network`, `Haptics`, camera, `@capacitor/filesystem`, plus custom native plugins such as `UILocal`) | `src/main/mobile/init.cljs:73-106`, `src/main/frontend/mobile/util.cljs:27-30` |
| **Plugin system** | ✅ | ❌ | [§5](#5-plugin-system-customjs-and-customcss) |
| **HTTP API / MCP server** | ✅ | ❌ | [§2](#extended-features) |

---

## 2. Feature Support Matrix

The "Mobile" column only states what the code shows; a ✅ means no platform check excludes the feature.

### Core Features

| Feature | Desktop | iOS / Android | Evidence |
| --- | --- | --- | --- |
| **DB graphs** | ✅ | ✅ | All platforms share the graph list filter (`repo.cljs:321-323`) |
| **File-based graphs** | ❌ | ❌ | Not openable on any platform (`repo.cljs:321-323`) |
| **Sync (Logseq Sync / RTC)** | ✅ | ✅ | The mobile app lists and downloads remote graphs for users in the sync group (`src/main/mobile/components/app.cljs:205-224`). On mobile the E2EE password is saved through native secure storage (`src/main/frontend/worker/sync/crypt.cljs:43-46`, `:118-126`; `src/main/frontend/mobile/secure_storage.cljs`) |
| **Flashcards** | ✅ | ✅ | Mobile has a **Cards** tab when flashcards are enabled (`src/main/mobile/tabs.cljs:17-20`, `:36-40`) showing `fsrs/cards-view` (`app.cljs:257-260`) |
| **Whiteboards** | ❌ | ❌ | Removed on every platform: `bcc478b5f7` (2025-12-29) dropped the `tldraw:build` script from `package.json`, and `src/main/frontend/components/export.cljs:329` refers to "whiteboard removal". `:logseq.class/Whiteboard` survives only in migration and graph-view code (`src/main/frontend/worker/db/migrate.cljs:268`, `src/main/frontend/worker/graph_view.cljs:62`) |
| **Custom CSS (`logseq/custom.css`)** | ✅ | ✅ loaded | Loaded during startup on every platform, with no platform check (`src/main/frontend/handler.cljs:62`, `src/main/frontend/handler/ui.cljs:112-119`). Mobile settings have no editor for it ([§3](#settings)) |
| **Themes** | Light/dark/system + plugin themes | Light/dark/system only | Mobile theme picker: `src/main/mobile/components/settings.cljs:55-65`. Plugin themes need the plugin system, which is off on native mobile ([§5](#5-plugin-system-customjs-and-customcss)) |
| **Slash commands** | ✅ | ✅ (no plugin commands) | Same `commands-map`; see [Slash Commands](../interface/slash-commands.md#mobile) |

### Extended Features

| Feature | Desktop | iOS / Android | Evidence |
| --- | --- | --- | --- |
| **Plugin system** | ✅ | ❌ | `plugin-platform?` is true only on Electron or on non-native, non-publishing web (`src/main/frontend/util.cljc:127-131`); `config/lsp-enabled?` requires it (`src/main/frontend/config.cljs:149-152`) |
| **custom.js** | ✅ | ❌ | See [§5](#customjs) |
| **HTTP API server** | ✅ (default port `12315`) | ❌ | Settings row shown only on Electron (`src/main/frontend/components/settings.cljs:1156-1157`); default port at `src/electron/electron/server.cljs:21` |
| **MCP server** | ✅ | ❌ | Electron-only settings row (`settings.cljs:1445-1446`); served by the desktop API server (`src/electron/electron/mcp_server.cljs:2`) |
| **Markdown mirror** | ✅ | ❌ | Electron-only settings row (`settings.cljs:1158-1159`) |
| **Developer mode** | ✅ | ❌ | Row hidden on native platforms (`settings.cljs:926`) |
| **YouTube embeds** | IFrame player | Wrapper player | `use-youtube-wrapper?` is `native-platform?` (`src/main/frontend/extensions/video/youtube.cljs:31-32`) |
| **YouTube timestamps** (`/Embed YouTube timestamp`) | ✅ | ❌ | With the wrapper, `gen-youtube-ts-macro` shows "timestamp unavailable" and inserts nothing (`youtube.cljs:163-167`) |
| **PDF viewer** | ✅ | ⚠️ see [§8](#1-pdf-viewer) | |
| **Open graph in new window** | New Electron window | `window.open` (web tab behaviour) | `src/main/frontend/handler/ui.cljs:249-263` |
| **Git auto-commit** | ❌ | ❌ | No git versioning code exists at `be800f1711`. The only `git` reference in `src/electron/` is the `:shell/run` allow-list (`src/electron/electron/shell.cljs:11`) |
| **Share to Logseq** | — | ✅ | Android accepts `SEND` intents for `text/plain`, `image/*`, `application/*` and `video/*` (`android/app/src/main/AndroidManifest.xml:40-46`); iOS has a share extension that opens `logseq://shared?…` (`ios/App/ShareViewController/ShareViewController.swift:40`) |
| **Home-screen widget / Control Center controls** | — | iOS | A widget linking to `logseq://mobile`, `…/go/audio` and `…/go/quick-add` (`ios/App/shortcuts/shortcuts.swift`), plus `QuickAddButton` and `RecordAudioButton` controls (`ios/App/shortcuts/shortcutsControl.swift`) |
| **Audio recording** | — | ✅ | Toolbar **Audio** action (`src/main/mobile/components/editor_toolbar.cljs:151-160`). A recording is saved as an asset, with optional transcription through the native `UILocal` plugin (`src/main/mobile/components/recorder.cljs:50-95`) |
| **Camera** | — | ✅ | Toolbar **Photo** action (`editor_toolbar.cljs:141-149`, `src/main/frontend/mobile/camera.cljs:32`, `:61`) |
| **Sync asset size limit** | 100 MB | 100 MB | Server-side and the same for every client (`deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs:9`, `:105`) — not a mobile limit |

---

## 3. UI & UX Differences

### Navigation

The mobile app has its own shell with a native bottom tab bar instead of the desktop sidebars:

| Tab id | Label (en) | Notes | Source |
| --- | --- | --- | --- |
| `home` | Home | Required tab | `src/main/mobile/tabs.cljs:5-8`, `:29` |
| `graphs` | Graphs | | `tabs.cljs:9-12` |
| `capture` | Capture | Also opened by `logseq://mobile/go/quick-add` | `tabs.cljs:13-16`, `src/main/mobile/deeplink.cljs:80-81` |
| `flashcards` | Cards | Only when flashcards are enabled | `tabs.cljs:17-20`, `:36-40` |
| `go to` | Go To | | `tabs.cljs:21-24` |
| `search` | Search | **Android only** — added as a search-role tab | `src/main/mobile/bottom_tabs.cljs:285-292` |

- At most 4 main tabs are shown, and the set can be chosen in mobile settings (`tabs.cljs:31-34`; `src/main/mobile/components/settings.cljs:141-199`).
- English labels: `en.edn:1047-1050`, `:1078`.
- Mobile routes are only `/` (home), `/page/:name`, `/import` and `/export` (`src/main/mobile/routes.cljs:8-22`).

### Keyboard Shortcuts

- Shortcut handlers are installed at startup by `shortcut/refresh!` on every platform, with no mobile check (`src/main/frontend/handler.cljs:73`, `src/main/frontend/modules/shortcut/core.cljs:251-263`).
- These bindings are marked `:inactive` outside Electron, so they don't work on mobile (or web): `:go/electron-find-in-page`, `:go/electron-jump-to-the-next`, `:go/electron-jump-to-the-previous`, `:graph/db-save`, `:shell/run`, `:editor/copy-page-url` and `:window/close` (`src/main/frontend/modules/shortcut/config.cljs:317`, `:321`, `:325`, `:377`, `:385`, `:451`, `:455`). `:ui/install-plugin-from-github` additionally needs the plugin system (`:473`).
- For the full list see the audited [Keyboard Shortcuts](../configuration/keyboard-shortcuts.md) reference.

> **Unverified:** whether a hardware keyboard attached to a phone or tablet triggers the installed handlers was not tested.

### Touch Gestures

| Gesture | Result | Source |
| --- | --- | --- |
| **Horizontal swipe on a block** (more than 10 px and more horizontal than vertical) | Toggles the block's selection, fires a haptic tap, and shows the selection action bar while any block is selected | `src/main/frontend/handler/block.cljs:308-342` |
| **Touch-move tracking** | Registered only when `util/mobile?` or `native-platform?` is true | `src/main/frontend/components/block.cljs:4278-4282` |
| **Tap the status bar** (iOS) | Scrolls to top | `src/main/mobile/init.cljs:101-102` |
| **Android back gesture** | Routed through `MainActivity` to JS (`window.LogseqNative.onNativePop`); `@capacitor/app`'s back handler is disabled | `capacitor.config.ts:20-26` |

The page previously described swipe-to-collapse, swipe-right-to-go-back, long-press context menus and pinch zoom. None of these has a handler in the mobile code at `be800f1711`, so those rows were removed.

### Selection Action Bar

The bar is shown after a swipe selects blocks (`src/main/mobile/components/selection_toolbar.cljs:1-2`). Its actions, in order (`selection_toolbar.cljs:30-100`):

- copy, outdent, indent
- **Comment** — only when the selected blocks can take comments
- **Reaction** — only when a selection target exists
- **Delete**, **Copy ref**, **Copy url**, **Unselect**

### Editor Toolbar

The native editor toolbar is shown only on native platforms where the native toolbar plugin is present (`src/main/mobile/components/editor_toolbar.cljs:209-210`).

- **Main actions** (`editor_toolbar.cljs:180-191`): Undo, Todo, Outdent, Indent, Tag, Photo, Upload an asset, Reference, Audio, Slash, Redo. The trailing action is **Hide**, which dismisses the keyboard.
- **In a comment editor:** Reference, Photo, Upload an asset, Audio.

### Settings

Mobile has its own settings screen (`src/main/mobile/components/settings.cljs:243-307`):

- Theme, Language, Version, Tabs, Revision
- Report bug, Check log
- Log out, Discord, Forum

The desktop-only rows listed in §2 (plugins, API server, MCP, markdown mirror, developer mode, custom.css editing) do not appear there.

### Deep Links

The native app handles `logseq://` URLs (`AndroidManifest.xml:50-55`; `init.cljs:73-89`). Hosts handled in `src/main/mobile/deeplink.cljs`:

| URL | Action | Line |
| --- | --- | --- |
| `logseq://mobile/go/audio` | Start audio recording | `:77` |
| `logseq://mobile/go/quick-add` | Open the Capture tab | `:80` |
| `logseq://graph/<name>?page=…&block-id=…` | Open a graph, page or block | `:83` |
| `logseq://shared?title=&url=&type=&payload=` | Import shared content | `:124-135` |

See [URI Schemes](./uri-schemes.md).

---

## 4. Storage

| | Desktop | iOS / Android |
| --- | --- | --- |
| **Graph format** | Each DB graph is a single `db.sqlite` (`deps/db/src/logseq/db/common/sqlite.cljs:29-33`) | Same DB graph format |
| **Location** | Under `~/logseq/graphs` by default (`deps/common/src/logseq/common/config.cljs:55`); automatic backups go to `<graph>/backup/` (`src/main/logseq/db_worker/graph_backup.cljs:10`) | Inside the app, managed by the browser db-worker. Its storage layer is OPFS (`src/main/frontend/worker/platform/browser.cljs:74`), and the worker runs with owner source `:capacitor` (`:192-197`) |
| **Getting data in/out** | Filesystem, import/export | `/import` and `/export` routes (`src/main/mobile/routes.cljs:15-22`), the share sheet, and sync |

> **Unverified:** the earlier on-device paths (`/var/mobile/Containers/…`, `/data/data/com.logseq.app/files/`, Files-app visibility) and the iCloud/Google Drive/Dropbox table can't be derived from the source, so they were removed.

The earlier `frontend.mobile.fs` example namespace does not exist. `@capacitor/filesystem` is used only by the share-intent handler (`src/main/frontend/mobile/intent.cljs:4`).

---

## 5. Plugin System, custom.js and custom.css

### Plugins

Plugins are not available in the native app:

- `config/lsp-enabled?` is `(and util/plugin-platform? (not (false? feature-plugin-system-on?)) (state/lsp-enabled?-or-theme))` (`src/main/frontend/config.cljs:149-152`).
- `plugin-platform?` is `(or (and web-platform? (not PUBLISHING)) (electron?))`, where `web-platform?` means "not Electron and not a native Capacitor platform" (`src/main/frontend/util.cljc:127-131`).
- So on iOS and Android the plugin system, plugin themes, plugin slash commands and plugin UI are all off.
- In the **web** build, the plugin system can be switched on in settings (`settings.cljs:1154-1155`).

There is no plugin sandbox on any platform, and plugins are not "Electron-based". The web build can run them too.

The earlier "Why no mobile plugins?" rationale (security sandbox, app review, performance) is not recorded in the source.

> **Unverified:** those reasons may be true, but nothing at `be800f1711` documents them.

### custom.js

`custom.js` is executed by `custom-js-installer`, which runs `exec-js-if-exists-&-allowed!` only when `(not util/nfs?)` (`src/main/frontend/components/plugins.cljs:1785-1795`). The installer is mounted in `frontend.components.container` (`src/main/frontend/components/container.cljs:582`). The mobile shell does not use that container; `src/main/mobile/components/app.cljs` requires neither it nor `frontend.components.plugins`. The result:

- **Desktop:** runs, after the user confirms.
- **Web:** `nfs?` is true, so it doesn't run.
- **Native mobile:** not mounted, so it doesn't run.

### custom.css

`logseq/custom.css` is read from the graph and applied during `restore-and-setup!` (`src/main/frontend/handler.cljs:52-62`, `src/main/frontend/handler/ui.cljs:112-119`). That path runs from the shared `start!` (`handler.cljs:191`), which the mobile app calls (`src/main/mobile/core.cljs:99`), so custom CSS applies on mobile too. It can only be edited from desktop settings (`settings.cljs:217`).

---

## 6. Performance Considerations

> **Unverified:** the memory limits (2–8 GB desktop, 512 MB–4 GB mobile), block-count recommendations (100k / 50k), "SQLite with WAL mode", and asset loading speeds that used to be listed here can't be proven from source and were removed.

What the code does show:

- Both platforms run the same db-worker code over SQLite. Mobile uses the browser flavour; desktop uses the Electron flavour (`src/main/frontend/persist_db/browser.cljs:201-206`).
- The mobile app watches network status through `@capacitor/network` (`src/main/mobile/init.cljs:105-106`).

---

## 7. Platform Detection

### ClojureScript

| Predicate | Meaning | Source |
| --- | --- | --- |
| `frontend.mobile.util/native-platform?` | `Capacitor.isNativePlatform()` | `src/main/frontend/mobile/util.cljs:11-12` |
| `frontend.mobile.util/native-ios?` | Native **and** `Capacitor.getPlatform()` is `"ios"` | `mobile/util.cljs:14-16` |
| `frontend.mobile.util/native-android?` | Native **and** the platform is `"android"` | `mobile/util.cljs:18-20` |
| `frontend.mobile.util/native-iphone?` / `native-ipad?` | Device-model checks | `mobile/util.cljs:136`, `:142` |
| `frontend.util/electron?` | The user agent contains " electron" | `src/main/frontend/util.cljc:113` |
| `frontend.util/web-platform?` (= `nfs?`) | Not Electron and not native mobile | `util.cljc:127-130` |
| `frontend.util/plugin-platform?` | Electron, or non-publishing web | `util.cljc:131` |
| `frontend.util/capacitor?` | `window.isCapacitorNew` is set | `util.cljc:71` |
| `frontend.util/mobile?` | The user agent contains `Mobi`. **For UX logic only**; iPads and Android tablets don't match | `util.cljc:60-70` |

Real usage, from the YouTube extension:

```clojure
(ns frontend.extensions.video.youtube
  (:require [frontend.mobile.util :as mobile-util]))

(defn- use-youtube-wrapper? []
  (mobile-util/native-platform?))
```

The earlier examples `mobile-util/open-external-file`, `pdf-viewer/open` and a `(defn mobile? …)` wrapper in `frontend.util` do not exist. `mobile-util/alert` does exist, but it is an iOS drop-alert that takes `{:title … :message … :type …}` (`mobile/util.cljs:156-161`).

### JavaScript

The same Capacitor calls the ClojureScript predicates wrap:

```javascript
import { Capacitor } from '@capacitor/core'

const isNative = Capacitor.isNativePlatform()
const platform = Capacitor.getPlatform() // 'ios' | 'android' | 'web'
```

---

## 8. Known Limitations

### 1. PDF viewer

- `open-pdf-file` has an explicit native-platform branch: on native (and Electron) it uses the asset href directly, then sets the current PDF (`src/main/frontend/components/block.cljs:513-529`).
- The viewer component, `pdf/default-embed-playground`, is mounted in `frontend.components.theme` (`src/main/frontend/components/theme.cljs:159`).
- The mobile app's component namespace (`src/main/mobile/components/app.cljs`) does not require that namespace.

> **Unverified:** whether PDFs open in-app on iOS/Android. The code neither blocks it nor visibly mounts the viewer in the mobile shell. The earlier citation `pdf/core.cljs:1090` and the issue link `#11046` could not be tied to this behaviour.

### 2. YouTube timestamps

Native mobile uses the YouTube wrapper player, and `gen-youtube-ts-macro` refuses to generate a timestamp under the wrapper (`src/main/frontend/extensions/video/youtube.cljs:163-167`). So `/Embed YouTube timestamp` does nothing on mobile.

### 3. Local video playback

> **Unverified:** the earlier claim that local MP4/WebM/MOV assets cannot play on mobile. `src/main/frontend/components/block/video.cljs` has no platform check, and the earlier citation `youtube.cljs:212` does not relate to local video.

### 4. Plugin system

Off on native mobile (see [§5](#plugins)). Everything a plugin provides is therefore unavailable there: slash commands, commands and shortcuts, UI slots, macro renderers and themes.

### Minor limitations

| Limitation | Desktop | Mobile | Source |
| --- | --- | --- | --- |
| **HTTP API / MCP server** | ✅ | ❌ | `settings.cljs:1156-1157`, `:1445-1446` |
| **Developer mode** | ✅ | ❌ | `settings.cljs:926` |
| **Markdown mirror** | ✅ | ❌ | `settings.cljs:1158-1159` |
| **Multi-window** | ✅ Electron windows | Uses `window.open` | `src/main/frontend/handler/ui.cljs:249-263` |
| **External URLs** | Opened through `window.apis.openExternal` | Assigned to `window.location.href` (the non-Electron branch) | `src/main/frontend/util.cljc:1160-1167` |
| **Electron-only shortcuts** | ✅ | ❌ | `shortcut/config.cljs:317-473` (see [§3](#keyboard-shortcuts)) |

> **Unverified:** how the native WebView treats a `window.open` or `location.href` navigation (system browser or in-app) was not tested.

The earlier rows for "Handbooks ✅ mobile" and "DevTools" were removed. Handbooks are opened from the desktop help menu (`src/main/frontend/components/container.cljs:245`), and the mobile shell has no reference to them.

---

## 9. For App Contributors

### Gate platform-specific code

```clojure
(ns my-feature
  (:require [frontend.mobile.util :as mobile-util]
            [frontend.util :as util]))

(defn my-feature []
  (cond
    (mobile-util/native-platform?) (mobile-implementation)
    (util/electron?)               (desktop-implementation)
    :else                          (web-implementation)))
```

Use `mobile-util/native-platform?` for anything that depends on native capabilities. `util/mobile?` is a user-agent check meant for layout only (`src/main/frontend/util.cljc:62-70`).

### Run each platform

| Target | Command | Source |
| --- | --- | --- |
| Desktop / web dev watch | `pnpm watch` | `package.json:55` |
| Mobile bundle watch | `pnpm mobile-watch` | `package.json:58` |
| iOS dev environment | `bb dev:ios-app` (runs `dev:app-watch` plus `dev:cap-run-ios`) | `bb.edn:122-127` |
| Android dev environment | `bb dev:android-app` (runs `dev:app-watch` plus `dev:cap-run-android`) | `bb.edn:135-140` |

The earlier "Mobile UI guidelines" (44×44 pt tap targets and similar), the "Power users" task table and the plugin-developer checklist were removed. The first two are not derived from the codebase, and the checklist does not apply because plugins do not run on mobile.

---

## Summary

1. ✅ **DB graphs, sync, flashcards and custom.css** work on mobile; file graphs and whiteboards are gone everywhere.
2. ❌ **No plugin system and no custom.js** on native mobile (`util.cljc:127-131`, `config.cljs:149-152`).
3. ❌ **The HTTP API server, MCP server, markdown mirror and developer mode** are desktop-only.
4. 📱 **Mobile-only:**
   - native tabs and the editor toolbar;
   - swipe-to-select with an action bar;
   - camera and audio capture with transcription;
   - share targets;
   - `logseq://mobile/...` deep links;
   - iOS widget and controls.
5. ⚠️ **Unconfirmed:** the in-app PDF viewer and local video playback on mobile.

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
