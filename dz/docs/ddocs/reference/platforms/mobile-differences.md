---
title: 'Mobile Platform Differences'
status: 🟢 Stable
version: 'Since v0.5.0+'
quadrant: 'Reference'
platform: 'Mobile (iOS & Android)'
related-docs:
  - '[Desktop vs Mobile Features](../../explanation/architecture/platform-architecture.md)'
  - '[PDF Annotations](../features/pdf-annotations.md)'
  - '[Plugin Development](../../tutorials/plugins/first-plugin.md)'
tags: ['mobile', 'ios', 'android', 'capacitor', 'platform', 'limitations']
---

# Mobile Platform Differences

This reference documents key differences between Logseq's desktop and mobile platforms. Understanding these differences is essential for plugin developers, contributors, and power users who work across multiple platforms.

**Target Audience:**

- Plugin developers adapting desktop plugins for mobile
- App contributors working on mobile features
- Power users understanding feature parity
- Documentation writers covering platform-specific behavior

**Platforms Covered:**

- 📱 **iOS** (iPhone & iPad)
- 🤖 **Android** (phones & tablets)
- 🖥️ **Desktop** (Windows, macOS, Linux via Electron)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Feature Support Matrix](#2-feature-support-matrix)
3. [UI & UX Differences](#3-ui--ux-differences)
4. [File System & Storage](#4-file-system--storage)
5. [Plugin System](#5-plugin-system)
6. [Performance Considerations](#6-performance-considerations)
7. [Platform Detection](#7-platform-detection)
8. [Known Limitations](#8-known-limitations)
9. [Best Practices](#9-best-practices)

---

## 1. Architecture Overview

### Technology Stack

| Component         | Desktop                          | Mobile                             |
| ----------------- | -------------------------------- | ---------------------------------- |
| **Runtime**       | Electron (Chromium + Node.js)    | Capacitor (WebView)                |
| **WebView**       | Chromium 120+                    | iOS: WKWebView<br>Android: WebView |
| **File Access**   | Direct filesystem (Node.js `fs`) | Capacitor Filesystem API           |
| **Native APIs**   | Electron IPC                     | Capacitor Plugins                  |
| **Plugin System** | ✅ Full support                  | ❌ Not supported                   |
| **HTTP Server**   | ✅ Built-in (port 12315)         | ❌ Not available                   |
| **CLI Tools**     | ✅ Available                     | ❌ N/A                             |

### Runtime Environment

**Desktop (Electron):**

```javascript
// Full Node.js access
const fs = require('fs')
const path = require('path')

// Electron APIs
const { ipcRenderer } = require('electron')
```

**Mobile (Capacitor):**

```javascript
// Capacitor plugins only
import { Filesystem } from '@capacitor/filesystem'
import { Device } from '@capacitor/device'

// No direct Node.js or Electron APIs
```

---

## 2. Feature Support Matrix

### Core Features

| Feature               | Desktop | iOS        | Android    | Notes                         |
| --------------------- | ------- | ---------- | ---------- | ----------------------------- |
| **Database Graphs**   | ✅ Full | ✅ Full    | ✅ Full    | SQLite-based                  |
| **File-Based Graphs** | ✅ Full | ⚠️ Limited | ⚠️ Limited | No git integration            |
| **Real-Time Sync**    | ✅ Full | ✅ Full    | ✅ Full    | E2EE encryption               |
| **Journals**          | ✅ Full | ✅ Full    | ✅ Full    | All features                  |
| **Whiteboards**       | ✅ Full | ✅ Full    | ✅ Full    | tldraw-based                  |
| **Flashcards**        | ✅ Full | ✅ Full    | ✅ Full    | Spaced repetition             |
| **Query System**      | ✅ Full | ✅ Full    | ✅ Full    | Datalog & advanced            |
| **Block References**  | ✅ Full | ✅ Full    | ✅ Full    | Embed blocks                  |
| **Page References**   | ✅ Full | ✅ Full    | ✅ Full    | `[[links]]`                   |
| **Templates**         | ✅ Full | ✅ Full    | ✅ Full    | All features                  |
| **Custom CSS**        | ✅ Full | ✅ Full    | ✅ Full    | Via custom.css                |
| **Themes**            | ✅ Full | ✅ Full    | ✅ Full    | Plugin themes only on desktop |

### Extended Features

| Feature                | Desktop      | iOS         | Android     | Notes                                                                   |
| ---------------------- | ------------ | ----------- | ----------- | ----------------------------------------------------------------------- |
| **PDF Annotations**    | ✅ Full      | ❌ No       | ❌ No       | See [Known Issue #11046](https://github.com/logseq/logseq/issues/11046) |
| **PDF Viewer**         | ✅ PDF.js    | ❌ External | ❌ External | Opens in system viewer                                                  |
| **Video Embed**        | ✅ Full      | ⚠️ Limited  | ⚠️ Limited  | Local videos unsupported                                                |
| **YouTube Embed**      | ✅ IFrame    | ✅ Wrapper  | ✅ Wrapper  | Different rendering                                                     |
| **Asset Management**   | ✅ Full      | ✅ Full     | ✅ Full     | 100MB file limit                                                        |
| **Zotero Integration** | ✅ Plugin    | ❌ No       | ❌ No       | Desktop plugin only                                                     |
| **Git Sync**           | ✅ Full      | ❌ No       | ❌ No       | File-based graphs only                                                  |
| **HTTP API Server**    | ✅ Yes       | ❌ No       | ❌ No       | Port 12315                                                              |
| **MCP Server**         | ✅ Yes       | ❌ No       | ❌ No       | CLI or API mode                                                         |
| **Plugin System**      | ✅ Full      | ❌ No       | ❌ No       | Electron-based                                                          |
| **CLI Tools**          | ✅ Yes       | ❌ No       | ❌ No       | @logseq/cli                                                             |
| **Developer Mode**     | ✅ Full      | ❌ No       | ❌ No       | DevTools access                                                         |
| **Custom Shortcuts**   | ✅ Extensive | ⚠️ Limited  | ⚠️ Limited  | Hardware keyboard only                                                  |

---

## 3. UI & UX Differences

### Navigation

**Desktop:**

- Multi-window support
- Keyboard-first navigation
- Mouse hover interactions
- Right-click context menus
- Drag-and-drop everywhere

**Mobile:**

- Single-screen focus
- Touch-first gestures
- Long-press for context menus
- Swipe navigation
- Limited drag-and-drop

### Keyboard Shortcuts

**Desktop:**

```
Cmd/Ctrl + K      → Command palette
Cmd/Ctrl + O      → Quick open
Cmd/Ctrl + Shift + K → Go to page
/                 → Slash commands
[[                → Page link autocomplete
```

**Mobile:**

- ⚠️ **Requires external hardware keyboard**
- Most shortcuts work with Bluetooth keyboard
- On-screen keyboard lacks modifier keys
- Slash commands work via `/` key
- `[[` autocomplete works

### Touch Gestures

**Mobile-Specific:**

```
Long press block   → Context menu (edit, delete, move)
Swipe left        → Collapse/expand block
Swipe right       → Navigate back
Pinch zoom        → Zoom in/out (limited support)
Two-finger scroll → Scroll page
```

**Desktop Equivalent:**

- Right-click → Context menu
- Click arrow → Collapse/expand
- Browser back button → Navigate back
- Ctrl/Cmd + Mouse wheel → Zoom

### Selection Toolbar

**Mobile:**

- Floating toolbar appears on text selection
- Limited to: Bold, Italic, Link, Highlight
- Positioned above selected text

**Desktop:**

- Inline formatting buttons
- Full formatting toolbar
- Quick access to all markdown syntax

---

## 4. File System & Storage

### Graph Storage Locations

**Desktop:**

```
macOS:    ~/Documents/Logseq/
Windows:  C:\Users\<user>\Documents\Logseq\
Linux:    ~/Documents/Logseq/
```

**iOS:**

```
App Container: /var/mobile/Containers/Data/Application/<UUID>/Documents/
Access: Files app → "On My iPhone/iPad" → Logseq
```

**Android:**

```
Internal: /data/data/com.logseq.app/files/
External: /storage/emulated/0/Documents/Logseq/
Access: Via file picker or "Share to Logseq"
```

### File Access APIs

**Desktop (Node.js):**

```clojure
;; Direct filesystem access
(ns frontend.fs
  (:require ["fs" :as fs]
            ["path" :as path]))

(defn read-file [path]
  (.readFileSync fs path "utf8"))

(defn write-file [path content]
  (.writeFileSync fs path content))
```

**Mobile (Capacitor):**

```clojure
;; Via Capacitor Filesystem plugin
(ns frontend.mobile.fs
  (:require ["@capacitor/filesystem" :refer [Filesystem]]))

(defn read-file [path]
  (.readFile Filesystem #js {:path path
                             :directory "DOCUMENTS"}))

(defn write-file [path content]
  (.writeFile Filesystem #js {:path path
                              :data content
                              :directory "DOCUMENTS"}))
```

### Graph Sync

| Sync Method          | Desktop   | iOS       | Android   |
| -------------------- | --------- | --------- | --------- |
| **Logseq Sync**      | ✅ Full   | ✅ Full   | ✅ Full   |
| **Git (file-based)** | ✅ Full   | ❌ No     | ❌ No     |
| **iCloud**           | ⚠️ Manual | ⚠️ Manual | ❌ N/A    |
| **Google Drive**     | ⚠️ Manual | ❌ No     | ⚠️ Manual |
| **Dropbox**          | ⚠️ Manual | ❌ No     | ⚠️ Manual |

**⚠️ Warning:** Manual cloud sync (iCloud, Google Drive, Dropbox) can cause conflicts and data loss. Use **Logseq Sync** for reliable cross-platform syncing.

---

## 5. Plugin System

### Plugin Support

**Desktop:**

- ✅ Full plugin system (`@logseq/libs`)
- ✅ Load unpacked plugins (developer mode)
- ✅ Plugin marketplace
- ✅ Custom themes as plugins
- ✅ Slash commands, shortcuts, UI injection
- ✅ Inter-plugin communication

**Mobile:**

- ❌ No plugin support
- ⚠️ Plugin-provided themes work (if uploaded to marketplace)
- ❌ No custom plugins
- ❌ No developer mode
- ❌ No plugin API access

### Why No Mobile Plugins?

**Technical Constraints:**

1. **Security sandbox:** Mobile WebView restricts arbitrary code execution
2. **Performance:** Limited memory and CPU compared to desktop
3. **UI complexity:** Touch interface incompatible with many plugin UIs
4. **App review:** Apple/Google restrict dynamic code loading

**Workaround:**

- Use **MCP integration** (CLI mode) for AI-powered workflows
- Request features via [GitHub Discussions](https://github.com/logseq/logseq/discussions)
- Use **Logseq Sync** to leverage desktop plugins, then view on mobile

---

## 6. Performance Considerations

### Memory Limits

| Platform    | Typical Limit | Large Graph Handling                   |
| ----------- | ------------- | -------------------------------------- |
| **Desktop** | 2-8 GB        | ✅ Excellent (can handle 100k+ blocks) |
| **iOS**     | 512 MB - 2 GB | ⚠️ Good (50k blocks recommended)       |
| **Android** | 512 MB - 4 GB | ⚠️ Good (50k blocks recommended)       |

### Database Performance

**Desktop:**

- SQLite with WAL mode
- Concurrent read access
- Fast indexing
- Efficient search

**Mobile:**

- Same SQLite engine
- Limited memory affects cache
- Slower indexing on large graphs
- Search may take longer

### Asset Loading

**Desktop:**

```
Images: Instant (local filesystem)
PDFs: Fast (PDF.js in-process)
Videos: Streamed (local files)
```

**Mobile:**

```
Images: Fast (cached)
PDFs: External viewer (system-dependent)
Videos: Streamed (network-dependent for YouTube)
```

---

## 7. Platform Detection

### ClojureScript Detection

```clojure
;; Check if running on mobile
(ns frontend.util
  (:require [frontend.mobile.util :as mobile-util]))

(defn mobile? []
  (mobile-util/native-platform?))

(defn ios? []
  (mobile-util/native-ios?))

(defn android? []
  (mobile-util/native-android?))

(defn desktop? []
  (not (mobile?)))
```

### JavaScript Detection

```javascript
// Via Capacitor
import { Capacitor } from '@capacitor/core'

const isMobile = Capacitor.isNativePlatform()
const isIOS = Capacitor.getPlatform() === 'ios'
const isAndroid = Capacitor.getPlatform() === 'android'
const isWeb = Capacitor.getPlatform() === 'web'
```

### Conditional Features

```clojure
;; Example: Platform-specific behavior
(defn open-pdf [pdf-path]
  (if (mobile-util/native-platform?)
    ;; Mobile: Open in external viewer
    (mobile-util/open-external-file pdf-path)
    ;; Desktop: Open in PDF.js viewer
    (pdf-viewer/open pdf-path)))

(defn show-notification [message]
  (if (mobile-util/native-platform?)
    ;; Mobile: Native alert
    (mobile-util/alert {:title "Notification"
                        :message message})
    ;; Desktop: Toast notification
    (notification/show! message :success)))
```

---

## 8. Known Limitations

### Critical Limitations

#### 1. PDF Annotations (iOS & Android)

**Status:** ❌ **Not Supported**  
**Issue:** [#11046](https://github.com/logseq/logseq/issues/11046)

**Problem:**

- PDFs cannot be opened within Logseq mobile app
- No in-app PDF viewer available
- Cannot create PDF annotations
- Cannot view existing PDF annotations

**Workaround:**

1. Open PDFs in external viewer (system default)
2. Create annotations on desktop, view on mobile
3. Use PDF links to jump to external viewer

**Code Reference:** [src/main/frontend/extensions/pdf/core.cljs:1090](../../src/main/frontend/extensions/pdf/core.cljs#L1090)

---

#### 2. Local Video Playback

**Status:** ⚠️ **Limited Support**

**Problem:**

- Local video files (MP4, WebM, MOV) cannot be played
- Only YouTube/Vimeo embeds work reliably
- Asset videos have playback issues

**Workaround:**

1. Use YouTube/Vimeo for video embeds
2. Upload videos to video hosting service
3. Use timestamps in text instead of video blocks

**Code Reference:** [src/main/frontend/extensions/video/youtube.cljs:212](../../src/main/frontend/extensions/video/youtube.cljs#L212)

---

#### 3. Plugin System

**Status:** ❌ **Not Available**

**Reason:** Architectural limitation (Electron vs Capacitor)

**Affected Features:**

- Custom slash commands
- Keyboard shortcuts
- UI injection
- Block renderers
- Custom queries
- External API integrations

**Workaround:**

- Use MCP integration for AI workflows
- Request features as core functionality
- Use desktop for plugin-dependent workflows

---

### Minor Limitations

| Limitation               | Desktop       | Mobile      | Impact                      |
| ------------------------ | ------------- | ----------- | --------------------------- |
| **Git Sync**             | ✅ Full       | ❌ No       | Use Logseq Sync instead     |
| **HTTP API**             | ✅ Port 12315 | ❌ No       | CLI integration unavailable |
| **DevTools**             | ✅ Full       | ❌ No       | Cannot debug on mobile      |
| **Multi-window**         | ✅ Yes        | ❌ No       | Single-screen only          |
| **Drag-drop files**      | ✅ Full       | ⚠️ Limited  | Use share/import            |
| **External URLs**        | ✅ In-app     | ⚠️ External | Opens system browser        |
| **Custom CSS variables** | ✅ Full       | ✅ Full     | Same CSS support            |
| **Handbooks**            | ✅ Full       | ✅ Full     | Interactive tutorials work  |

---

## 9. Best Practices

### For Plugin Developers

**1. Check Platform Before Using Desktop-Only APIs:**

```javascript
// ❌ Bad: Assumes desktop
logseq.App.openExternalLink('https://example.com')

// ✅ Good: Check platform first
const info = await logseq.App.getInfo()
if (info.platform === 'desktop') {
  logseq.App.openExternalLink('https://example.com')
} else {
  // Mobile users won't see this plugin anyway
  console.log('Plugin not supported on mobile')
}
```

**2. Graceful Degradation:**

```clojure
;; Check capabilities before using
(defn insert-pdf-annotation [pdf-path]
  (if (and (not (mobile-util/native-platform?))
           (pdf-utils/supported?))
    (pdf-viewer/annotate pdf-path)
    (notification/show! "PDF annotations require desktop app" :warning)))
```

**3. Document Platform Support:**

```markdown
# My Awesome Plugin

**Platform Support:**

- ✅ Desktop (Windows, macOS, Linux)
- ❌ Mobile (iOS, Android) - Not supported due to PDF API requirements

## Features

- PDF highlighting (Desktop only)
- ...
```

---

### For App Contributors

**1. Use Platform-Specific Utilities:**

```clojure
(ns my-feature
  (:require [frontend.mobile.util :as mobile-util]
            [frontend.util :as util]))

(defn my-feature []
  (if (mobile-util/native-platform?)
    (mobile-implementation)
    (desktop-implementation)))
```

**2. Test on Both Platforms:**

```bash
# Desktop
yarn watch

# iOS
bb dev:ios-app

# Android
bb dev:android-app
```

**3. Follow Mobile UI Guidelines:**

- Use touch-friendly tap targets (44x44pt minimum)
- Avoid hover-dependent interactions
- Test with on-screen keyboard open
- Handle screen rotation
- Test on small screens (iPhone SE)

---

### For Power Users

**1. Choose Right Platform for Task:**

| Task                       | Recommended Platform         |
| -------------------------- | ---------------------------- |
| **Heavy editing**          | Desktop (keyboard shortcuts) |
| **Quick capture**          | Mobile (voice input, camera) |
| **PDF annotations**        | Desktop (full viewer)        |
| **Plugin workflows**       | Desktop (plugins available)  |
| **On-the-go review**       | Mobile (mobile-optimized UI) |
| **Large graph management** | Desktop (better performance) |

**2. Sync Strategy:**

```
Desktop ←──Logseq Sync──→ Mobile
   ↓                        ↓
Plugins                  Quick notes
PDF work                 Voice capture
Heavy editing            On-the-go review
```

**3. Workarounds for Limitations:**

- **PDFs:** Annotate on desktop, link to external viewer on mobile
- **Videos:** Use YouTube embeds instead of local files
- **Plugins:** Use MCP + Claude on desktop, view results on mobile
- **Large graphs:** Archive old journals to improve mobile performance

---

## Summary

**Key Takeaways:**

1. ✅ **Core features work great** on both platforms (journals, queries, sync)
2. ⚠️ **PDF annotations** unavailable on mobile ([#11046](https://github.com/logseq/logseq/issues/11046))
3. ❌ **No plugin system** on mobile (architectural limitation)
4. 🔄 **Use Logseq Sync** for reliable cross-platform workflows
5. 📱 **Mobile optimized** for quick capture, desktop for heavy editing

---

## Next Steps

**For Developers:**

- [Mobile Development Setup](../../docs/develop-logseq-on-mobile.md)
- [Capacitor Documentation](https://capacitorjs.com/docs)
- [Contributing Guide](../../CONTRIBUTING.md)

**For Users:**

- [Getting Started on Mobile](../../tutorials/getting-started/mobile-app.md)
- [Logseq Sync Setup](../../how-to/sync/real-time-sync.md)
- [Feature Requests](https://github.com/logseq/logseq/discussions)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
