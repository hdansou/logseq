---
title: 'Handbooks System: Interactive In-App Tutorials'
status: '🟢 Stable'
version: 'Since v0.8.0'
quadrant: 'Explanation'
platform: 'Desktop (macOS/Windows/Linux), Web'
related-docs:
  - '../../how-to/multimedia/video-embed.md'
  - '../../tutorials/getting-started/01-installation.md'
  - '../../reference/features/keyboard-shortcuts.md'
---

# Handbooks System: Interactive In-App Tutorials

> **Last revised:** 2026-05-30

## Overview

Logseq's Handbooks system provides interactive, in-app tutorials and help documentation accessible from within the editor. Unlike external documentation, handbooks are tightly integrated with the app, offering contextual help, demos, and keyboard shortcut references without leaving your workflow.

**Key Features**:

- 📚 Interactive tutorials with videos and images
- ⌨️ Complete keyboard shortcuts reference (organized by category)
- 🎨 Category-based organization (Getting Started, Features, Shortcuts, etc.)
- 🔍 Search within handbooks
- 🖼️ Image lightbox for screenshots
- 📹 Embedded video demonstrations (YouTube integration)
- 🌐 Hosted on CDN for fast loading (handbooks.pages.dev)

## Accessing Handbooks

### Open Handbooks Panel

**Method 1: Menu**

1. Click **Help** menu (top bar)
2. Select **Handbook**

> **Removed:** Command palette access to handbooks is not implemented in current source; opening is wired via the Help menu toggle and handbook deep links.

> **Note**: There is no dedicated keyboard shortcut for opening handbooks.

**Result**: Draggable popup panel with handbook categories

---

## Interface Overview

### Dashboard View

```txt
┌────────────────────────────────────────────────────────┐
│  📚 Logseq Handbooks                            [✕]    │
├────────────────────────────────────────────────────────┤
│                                                         │
│  Popular Topics                                        │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐    │
│  │ Quick Start │ │ Queries     │ │ PDF Tools   │    │
│  │ 5 min read  │ │ 12 topics   │ │ 8 features  │    │
│  └─────────────┘ └─────────────┘ └─────────────┘    │
│                                                         │
│  Help Categories                                       │
│  ┌─────────────────────────────────────────┐          │
│  │ 📖 Getting Started              3 topics│          │
│  │ Essential tutorials for new users       │          │
│  └─────────────────────────────────────────┘          │
│  ┌─────────────────────────────────────────┐          │
│  │ ⚙️  Features & Tools            24 topics│          │
│  │ Queries, PDF, Whiteboards, Plugins      │          │
│  └─────────────────────────────────────────┘          │
│  ┌─────────────────────────────────────────┐          │
│  │ ⌨️  Keyboard Shortcuts        150+ cmds  │          │
│  │ Complete reference by category          │          │
│  └─────────────────────────────────────────┘          │
│                                                         │
└────────────────────────────────────────────────────────┘
```

### Category View

```txt
┌────────────────────────────────────────────────────────┐
│  ⬅ Back   Features & Tools                      [✕]  │
├────────────────────────────────────────────────────────┤
│                                                         │
│  Topics in this category:                              │
│                                                         │
│  ┌──────────────────────────────────────────┐         │
│  │ 🔍 Advanced Queries                       │         │
│  │ Master datalog queries and logic         │         │
│  └──────────────────────────────────────────┘         │
│  ┌──────────────────────────────────────────┐         │
│  │ 📄 PDF Annotations                        │         │
│  │ Highlight and annotate PDFs               │         │
│  └──────────────────────────────────────────┘         │
│  ┌──────────────────────────────────────────┐         │
│  │ 🎨 Whiteboards                            │         │
│  │ Visual thinking and diagramming           │         │
│  └──────────────────────────────────────────┘         │
│                                                         │
└────────────────────────────────────────────────────────┘
```

### Topic Detail View

```txt
┌────────────────────────────────────────────────────────┐
│  ⬅ Back to Features & Tools                     [✕]  │
├────────────────────────────────────────────────────────┤
│  Advanced Queries                                      │
│                                                         │
│  Chapter: [ Getting Started ▼ ]                       │
│                                                         │
│  [=============== Demo Video ===============]          │
│  │                                           │          │
│  │      ▶️  Play Tutorial Video              │          │
│  │                                           │          │
│  [=========================================]          │
│                                                         │
│  Queries in Logseq use Datalog, a powerful           │
│  logic programming language. Here's what you         │
│  can do:                                              │
│                                                         │
│  • Find all tasks due this week                       │
│  • Search by properties                               │
│  • Create dynamic views                               │
│                                                         │
│  [Screenshot of query example]                        │
│                                                         │
│  [⬅ Prev Chapter]           [Next Chapter ➡]        │
│                                                         │
└────────────────────────────────────────────────────────┘
```

---

## Architecture

### Content Hosting

**Location**: [https://handbooks.pages.dev](https://handbooks.pages.dev)

**Structure**:

All handbook content is served from a single EDN file at the root:

```
handbooks.pages.dev/
  ├── handbooks.edn              # Single file: all categories, topics, content
  └── assets/
      ├── demo-query.mp4
      ├── screenshot-1.png
      └── ...
```

**Content Format**:

- **`handbooks.edn`**: A single EDN tree containing all categories, topics, metadata, and inline HTML content
- **Assets**: Images, videos, demos (referenced by relative paths, resolved to CDN URLs at render time)

> **Note**: Earlier documentation described separate `.edn` manifests and `.html` files per category/topic. The actual implementation loads everything from one `handbooks.edn` file and parses it into a node tree client-side.

**Code Reference**: [`handbooks/core.cljs:26-32`](../../src/main/frontend/extensions/handbooks/core.cljs#L26-L32)

```clojure
(defn get-handbooks-endpoint [resource]
  (str
   (if (storage/get :handbooks-dev-watch?)
     "http://localhost:1337"
     "https://handbooks.pages.dev")
   resource))

(defn resolve-asset-url [path]
  (if (string/starts-with? path "http")
    path
    (str (get-handbooks-endpoint "/")
         (-> path (string/replace-first "./" "")
                  (string/replace-first #"^/+" "")))))
```

### Data Loading

**On Open**:

1. Fetch `handbooks.edn` from CDN (single request)
2. Parse entire EDN tree using `edn/read-string`
3. Flatten tree into a keyed node map using `tree-seq` (stored in component state)
4. Render dashboard with categories and popular topics

**On Category Click**:

1. Look up category node from the in-memory node map
2. Render category view (no additional network request)

**On Topic Click**:

1. Look up topic node from the in-memory node map
2. Parse chapters if multi-chapter (children in the tree)
3. Render topic detail view (no additional network request)

**Code Reference**: [`handbooks/core.cljs`](../../src/main/frontend/extensions/handbooks/core.cljs) — see `load-handbooks!` (around line 502) and the `tree-seq` node flattening (around line 577).

**Caching**: All content is loaded in a single fetch and held in component state for the session. Navigation between categories and topics requires no further network requests.

### Content Structure (EDN Format)

The entire handbook content lives in a single `handbooks.edn` file as a nested tree. The root has `:popular-topics` and `:children` (categories). Each category has `:children` (topics), and each topic may have `:children` (chapters).

**Full Tree Structure** (simplified):

```clojure
{:popular-topics ["getting-started/quick-start" "features/queries"]
 :children
 [{:key "getting-started"
   :title "Getting Started"
   :description "Essential tutorials for new users"
   :color "#10B981"
   :icon "book"
   :children
   [{:key "getting-started/quick-start"
     :title "Quick Start Guide"
     :description "Get up and running in 5 minutes"
     :cover "./assets/quickstart-cover.png"
     :demos ["./assets/quickstart-demo.mp4"]
     :content "<h2>Welcome to Logseq</h2><p>This guide will...</p>"
     :children ; Multi-chapter topics (optional)
     [{:key "getting-started/quick-start/chapter-1"
       :title "Chapter 1: Installation"
       :content "<h3>Installing Logseq</h3>..."}
      {:key "getting-started/quick-start/chapter-2"
       :title "Chapter 2: First Graph"
       :content "<h3>Creating your first graph</h3>..."}]}
    {:key "getting-started/installation"
     :title "Installation"
     :description "Install Logseq on your platform"}]}]}
```

At load time, this tree is flattened into a keyed map via `tree-seq`, with the root stored under the `"__root"` key. Parent keys are computed from the `/`-delimited key paths (see `bind-parent-key` and `parse-parent-key`).

**Code Reference**: [`handbooks/core.cljs:63-74`](../../src/main/frontend/extensions/handbooks/core.cljs#L63-L74) (key parsing), tree flattening around line 577.

---

## Components

### Media Rendering

**Images**:

```html
<img src="./assets/screenshot.png" />
```

**Result**: Clickable image → Opens lightbox for full-size view

**Code**: [`handbooks/core.cljs:108-121`](../../src/main/frontend/extensions/handbooks/core.cljs#L108-L121)

```clojure
(rum/defc media-render
  [src]
  (let [src (util/trim-safe src)
        extname (some-> src (util/full-path-extname) (subs 1))
        youtube-id (and (string/includes? src "youtube.com/watch?v=")
                        (subs src (+ 2 (string/last-index-of src "v="))))]
    (cond
      (and extname (contains? config/video-formats (keyword extname)))
      [:video {:src src :controls true}]

      (string? youtube-id)
      (youtube/youtube-video youtube-id {:width "100%" :height 235})

      :else [:img {:src src}])))
```

**YouTube Videos**:

```clojure
{:demos ["https://www.youtube.com/watch?v=ABC123"]}
```

**Result**: Embedded YouTube player (using [`youtube.cljs`](../../src/main/frontend/extensions/video/youtube.cljs))

**Local Videos**:

```clojure
{:demos ["./assets/demo.mp4"]}
```

**Result**: Native HTML5 video player with controls

### Demo Gallery (Carousel)

**Multiple Demos**:

```clojure
{:demos ["./assets/demo1.mp4"
         "./assets/demo2.mp4"
         "./assets/demo3.png"]}
```

**Result**: Carousel with navigation bullets (powered by Glide.js)

**Code Reference**: [`handbooks/core.cljs:76-81`](../../src/main/frontend/extensions/handbooks/core.cljs#L76-L81)

```clojure
(defn load-glide-assets! []
  (p/let [_ (util/css-load$ (str util/JS_ROOT "/glide/glide.core.min.css"))
          _ (util/css-load$ (str util/JS_ROOT "/glide/glide.theme.min.css"))
          _ (when-not (aget js/window "Glide")
              (util/js-load$ (str util/JS_ROOT "/glide/glide.min.js")))]))
```

**Dependencies**:

- **Glide.js**: Lightweight carousel library
- **CSS**: `glide.core.min.css`, `glide.theme.min.css`
- **JS**: `glide.min.js` (loaded on-demand)

**Features**:

- Swipe on mobile
- Bullet navigation (1, 2, 3...)
- Auto-resize to container

> **Removed:** Explicit carousel keyboard navigation guarantees are not declared in current handbook code.

### Chapter Navigation

**Multi-Chapter Topics**:

```clojure
{:children
 [{:key "topic/chapter-1" :title "Chapter 1: Basics"}
  {:key "topic/chapter-2" :title "Chapter 2: Advanced"}
  {:key "topic/chapter-3" :title "Chapter 3: Pro Tips"}]}
```

**UI**:

```txt
Current chapter: [ Chapter 1: Basics ▼ ]

[Content here]

[⬅ Prev Chapter]           [Next Chapter ➡]
```

**Code Reference**: [`handbooks/core.cljs:123-152`](../../src/main/frontend/extensions/handbooks/core.cljs#L123-L152)

---

## Keyboard Shortcuts Integration

### Accessing Shortcuts

**From Handbooks Dashboard**:

1. Open handbooks via Help menu.
2. Click **"Keyboard shortcuts"** in the categories pane.
3. Handbooks closes and the shortcuts view opens in the right sidebar (`shortcut-settings`).

**Direct Access**:

1. Open Help menu.
2. Click **"Keyboard shortcuts"**.

**Code Reference**: [`handbooks/core.cljs:281-304`](../../src/main/frontend/extensions/handbooks/core.cljs#L281-L304), [`container.cljs:227-228`](../../src/main/frontend/components/container.cljs#L227-L228)

> **Removed:** Static shortcut examples/counts in this doc. The shown count is computed at runtime from `shortcut-config/*config`.

---

## Findings: New Capabilities

Validated against current source in `src/`:

1. **Deep-link routing to handbook topics** (desktop):
  - `logseq://handbook/<topic-key>` is parsed in Electron and sent to renderer.
  - Renderer opens handbooks and routes to the topic key via `:handbook/route-chan`.

2. **Copyable handbook deep links from topic view**:
  - Topic header copy action writes `logseq://handbook/<key>` to clipboard and shows a success notification.

3. **Developer watch mode for handbook content**:
  - When enabled, app polls `HEAD /handbooks.edn` and reloads content when `content-length` changes.

**Code Reference**: [`url.cljs:111-114`](../../src/electron/electron/url.cljs#L111-L114), [`listener.cljs:135-138`](../../src/main/electron/listener.cljs#L135-L138), [`state.cljs:2011-2017`](../../src/main/frontend/state.cljs#L2011-L2017), [`handbooks/core.cljs:616-623`](../../src/main/frontend/extensions/handbooks/core.cljs#L616-L623), [`handbooks/core.cljs:566-576`](../../src/main/frontend/extensions/handbooks/core.cljs#L566-L576)

---

## Troubleshooting

### Handbooks Opens But Topic Link Does Not Route

**Symptoms**: `logseq://handbook/<key>` opens app but not the expected topic.

**Checks**:

1. Confirm key exists in loaded `handbooks.edn` tree.
2. Confirm key normalization rules (lowercase + snake_case) for internal links.
3. Inspect console for `ERROR: handbook link resource not found`.

### Content Not Loading

**Symptoms**: loading state persists or error text appears in panel.

**Checks**:

1. Verify network access to `https://handbooks.pages.dev/handbooks.edn`.
2. If dev watch is enabled, verify local watch endpoint if used (`http://localhost:1337`).

---

## Removed or Out-of-Scope Items

> **Removed:** "Future Enhancements" roadmap. These items are not backed by implementation in `src/`, `deps/`, or `packages/` as of 2026-05-30.

> **Removed:** Internal handbook authoring/deploy process details and asset optimization quotas. Current repository source validates runtime consumption behavior, not publishing workflow policy.

> **Removed:** `internal-docs/handbooks-authoring.md` pointer. That path is not present in this repository.

---

## Related Documentation

- [Getting Started](../../tutorials/getting-started/01-installation.md)
- [Video Embedding](../../how-to/multimedia/video-embed.md)
- [Keyboard Shortcuts](../../reference/features/keyboard-shortcuts.md)

---

## Code Reference

**Core Implementation**: [`src/main/frontend/extensions/handbooks/core.cljs`](../../src/main/frontend/extensions/handbooks/core.cljs) (667 lines)

**Entrypoints**:

- Help menu toggle: [`src/main/frontend/components/container.cljs`](../../src/main/frontend/components/container.cljs)
- Popup wrapper and draggable container: [`src/main/frontend/components/handbooks.cljs`](../../src/main/frontend/components/handbooks.cljs)
- Desktop URL routing bridge: [`src/electron/electron/url.cljs`](../../src/electron/electron/url.cljs), [`src/main/electron/listener.cljs`](../../src/main/electron/listener.cljs)

**Dependencies**:

- YouTube player integration: [`src/main/frontend/extensions/video/youtube.cljs`](../../src/main/frontend/extensions/video/youtube.cljs)
- Lightbox integration: [`src/main/frontend/extensions/lightbox.cljs`](../../src/main/frontend/extensions/lightbox.cljs)
- Shortcut config source: [`src/main/frontend/modules/shortcut/config.cljs`](../../src/main/frontend/modules/shortcut/config.cljs)

---

**Status**: 🟢 Stable
**Feedback**: Report handbooks issues on [GitHub](https://github.com/logseq/logseq/issues) with `handbooks` label
