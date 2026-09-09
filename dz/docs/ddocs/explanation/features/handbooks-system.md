---
title: 'Handbooks System: Interactive In-App Tutorials'
status: '🟢 Stable'
version: 'Since v0.8.0'
quadrant: 'Explanation'
platform: 'Desktop (macOS/Windows/Linux), Web'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
related-docs:
  - '../../how-to/multimedia/video-embed.md'
  - '../../tutorials/getting-started/01-installation.md'
  - '../../reference/features/keyboard-shortcuts.md'
---

# Handbooks System: Interactive In-App Tutorials

> **Last revised:** 2026-09-09

## Overview

Logseq's Handbooks system provides interactive, in-app tutorials and help documentation accessible from within the editor. Unlike external documentation, handbooks are tightly integrated with the app, offering contextual help, demos, and keyboard shortcut references without leaving your workflow.

**Key Features**:

- 📚 Interactive tutorials with videos and images
- ⌨️ Entry point into the keyboard shortcuts reference
- 🎨 Category-based organization (categories, topics, chapters)
- 🔍 Fuzzy search within handbook content
- 🖼️ Image lightbox for screenshots
- 📹 Embedded video demonstrations (YouTube integration)
- 🌐 Hosted on CDN for fast loading (handbooks.pages.dev)

## Accessing Handbooks

### Open Handbooks Panel

**Method 1: Menu**

1. Click **Help** menu (top bar)
2. Select **Handbook**

The menu item is defined in `help-menu-items` and calls `handbooks/toggle-handbooks`, which toggles `:ui/handbooks-open?`.

**Code Reference**: [`container.cljs:244-246`](../../../../../src/main/frontend/components/container.cljs#L244-L246), [`handbooks.cljs:27-29`](../../../../../src/main/frontend/components/handbooks.cljs#L27-L29)

**Method 2: Deep link (desktop)**

`logseq://handbook/<topic-key>` opens the panel and routes directly to a topic. See [Deep Linking](#deep-linking-and-developer-tooling).

> **Removed:** Command palette access to handbooks is not implemented in current source; opening is wired via the Help menu toggle and handbook deep links.

> **Note**: There is no dedicated keyboard shortcut for opening handbooks. No handbook entry exists in `src/main/frontend/modules/shortcut/`. Pressing **Esc** closes the panel only while the handbook search input is focused and empty ([`handbooks/core.cljs:373-378`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L373-L378)).

**Result**: Draggable popup panel with handbook categories. The popup is a `.cp__handbooks-popup` container made draggable by `layout/setup-draggable-container!` ([`handbooks.cljs:11-25`](../../../../../src/main/frontend/components/handbooks.cljs#L11-L25)).

---

## Interface Overview

The diagrams below are illustrative. Actual titles, categories and topic counts come from the CDN-hosted content tree, not from the app. The panel header itself is labelled **Help** (`:help.handbook/title`).

### Dashboard View

```txt
┌────────────────────────────────────────────────────────┐
│  Help                                           [✕]    │
├────────────────────────────────────────────────────────┤
│  [ 🔍 Search                                     ]     │
│                                                         │
│  Popular topics                                        │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐    │
│  │ Quick Start │ │ Queries     │ │ PDF Tools   │    │
│  └─────────────┘ └─────────────┘ └─────────────┘    │
│                                                         │
│  Help categories                                       │
│  ┌─────────────────────────────────────────┐          │
│  │ 📖 Getting started              3 Topics│          │
│  │ Essential tutorials for new users       │          │
│  └─────────────────────────────────────────┘          │
│  ┌─────────────────────────────────────────┐          │
│  │ ⚙️  Features & Tools           24 Topics│          │
│  │ Queries, PDF, Whiteboards, Plugins      │          │
│  └─────────────────────────────────────────┘          │
│  ┌─────────────────────────────────────────┐          │
│  │ ⌨️  Keyboard shortcuts    <N> shortcuts  │          │
│  │ Opens the shortcuts view in the sidebar │          │
│  └─────────────────────────────────────────┘          │
│                                                         │
│  [ Discord card ]            [ Forum card ]            │
└────────────────────────────────────────────────────────┘
```

The `Keyboard shortcuts` card is appended to the category list by the app itself; every other card comes from the content tree. `<N>` is computed at runtime. The footer link cards (Discord, forum) render only on the dashboard pane ([`handbooks/core.cljs:438-481`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L438-L481), [`652-656`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L652-L656)).

### Category View

```txt
┌────────────────────────────────────────────────────────┐
│  ⬅ Features & Tools                             [✕]  │
├────────────────────────────────────────────────────────┤
│  [ 🔍 Search                                     ]     │
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
│  ⬅ Features & Tools                    [⧉] [⚙] [✕]  │
├────────────────────────────────────────────────────────┤
│  Current chapter  [ Chapter 1: Basics          ▾ ]    │
│                                                         │
│  [=============== Demo gallery ===========]           │
│  │                                           │          │
│  │      ▶️  Play tutorial video              │          │
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
│  [⬅ Prev chapter]           [Next chapter ➡]         │
│                                                         │
└────────────────────────────────────────────────────────┘
```

Note that the search bar renders only on the dashboard and category panes, not on topic detail ([`handbooks/core.cljs:636-639`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L636-L639)). The copy icon (⧉) appears only on topic detail; the settings gear (⚙) only in developer mode.

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

**Code Reference**: [`handbooks/core.cljs:24-37`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L24-L37)

```clojure
(defn get-handbooks-endpoint
  [resource]
  (str
   (if (storage/get :handbooks-dev-watch?)
     "http://localhost:1337"
     "https://handbooks.pages.dev")
   resource))

(defn resolve-asset-url
  [path]
  (if (string/starts-with? path "http")
    path (str (get-handbooks-endpoint "/")
              (-> path (string/replace-first "./" "")
                  (string/replace-first #"^/+" "")))))
```

### Asset URL Rewriting

Topic content is authored as HTML with relative asset paths. Before the HTML is injected into the DOM, every `src="..."` attribute is rewritten through `resolve-asset-url`, so relative paths become absolute CDN (or local dev-watch) URLs.

**Code Reference**: [`handbooks/core.cljs:39-48`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L39-L48)

```clojure
(defn inflate-content-assets-urls
  [content]
  (if-let [matches (and (not (string/blank? content))
                        (re-seq #"src=\"([^\"]+)\"" content))]
    (reduce
     (fn [content matched]
       (if-let [matched (second matched)]
         (string/replace content matched (resolve-asset-url matched)) content))
     content matches)
    content))
```

Paths already starting with `http` are left untouched, so external images and videos can be embedded directly.

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

**Code Reference**: `load-handbooks!` at [`handbooks/core.cljs:505-513`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L505-L513) and the `tree-seq` node flattening at [`handbooks/core.cljs:578-585`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L578-L585).

**Caching**: All content is loaded in a single fetch and held in component state for the session. Navigation between categories and topics requires no further network requests.

While the fetch is in flight the panel shows a loading state; on failure the error string is rendered in a `<code>` block instead ([`handbooks/core.cljs:628-632`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L628-L632)).

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

At load time, this tree is flattened into a keyed map via `tree-seq`, with the root stored under the `"__root"` key. Parent keys are computed from the `/`-delimited key paths (see `bind-parent-key` and `parse-parent-key`). The flattened map is also exposed on `window.handbook-nodes` for debugging.

**Code Reference**: [`handbooks/core.cljs:61-72`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L61-L72) (key parsing), tree flattening at [`578-585`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L578-L585).

Two category keys are special-cased for translation: `1.getting_started` renders as `:help/start` and `2.sync` as `:help.handbook/sync`; all other categories use their `:title` verbatim ([`handbooks/core.cljs:74-79`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L74-L79)).

---

## Components

### Media Rendering

**Images**:

```html
<img src="./assets/screenshot.png" />
```

**Result**: Clickable image → Opens lightbox for full-size view. Clicks inside rendered topic content are delegated: a click that lands on an `img` opens the PhotoSwipe lightbox with the image's natural dimensions ([`handbooks/core.cljs:239-244`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L239-L244), [`lightbox.cljs`](../../../../../src/main/frontend/extensions/lightbox.cljs)).

**Code**: [`handbooks/core.cljs:112-125`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L112-L125)

```clojure
(hsx/defc media-render
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

`media-render` is applied to `:demos` entries only. Media inside `:content` is plain HTML injected via `dangerouslySetInnerHTML` and is not routed through `media-render`.

**YouTube Videos**:

```clojure
{:demos ["https://www.youtube.com/watch?v=ABC123"]}
```

**Result**: Embedded YouTube player (using [`youtube.cljs`](../../../../../src/main/frontend/extensions/video/youtube.cljs)) at 100% width and 235px height.

**Local Videos**:

```clojure
{:demos ["./assets/demo.mp4"]}
```

**Result**: Native HTML5 video player with controls, for any extension in `config/video-formats`.

### Demo Gallery (Carousel)

**Multiple Demos**:

```clojure
{:demos ["./assets/demo1.mp4"
         "./assets/demo2.mp4"
         "./assets/demo3.png"]}
```

**Result**: Carousel with navigation bullets (powered by Glide.js). A single demo renders directly with no carousel; the Glide instance is only mounted when more than one demo is present ([`handbooks/core.cljs:211-232`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L211-L232)).

**Code Reference**: [`handbooks/core.cljs:81-86`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L81-L86)

```clojure
(defn load-glide-assets!
  []
  (p/let [_ (util/css-load$ (str util/JS_ROOT "/glide/glide.core.min.css"))
          _ (util/css-load$ (str util/JS_ROOT "/glide/glide.theme.min.css"))
          _ (when-not (aget js/window "Glide")
              (util/js-load$ (str util/JS_ROOT "/glide/glide.min.js")))]))
```

**Dependencies**:

- **Glide.js**: Lightweight carousel library
- **CSS**: `glide.core.min.css`, `glide.theme.min.css`
- **JS**: `glide.min.js` (loaded on-demand, and only once — guarded on `window.Glide`)

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
Current chapter  [ Chapter 1: Basics ▾ ]

[Content here]

[⬅ Prev chapter]           [Next chapter ➡]
```

The chapter dropdown lists sibling chapters excluding the current one and closes on any click outside `.chapters-select`. Prev/Next buttons are rendered only when the topic has more than one chapter, and each end of the range hides its respective button.

**Code Reference**: [`handbooks/core.cljs:127-156`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L127-L156) (dropdown), [`253-261`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L253-L261) (prev/next controls)

### Internal Content Links

Anchors inside topic HTML that do not start with `http` are intercepted and resolved to a handbook node key rather than navigating the browser. `parse-key-from-href` strips a trailing `.edn`, treats a leading `@` as an absolute key, otherwise joins the href against the current parent key, then lowercases and snake-cases the result. If no node matches, an error is logged to the console.

**Code Reference**: [`handbooks/core.cljs:50-59`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L50-L59), [`245-251`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L245-L251)

### Search

The search bar renders on the dashboard and category panes. It runs `search/fuzzy-search` over topic titles with a limit of 30 results. Scope depends on the pane: on a category pane the search is restricted to that category's topics and their chapters; on the dashboard it searches every node except the root.

**Code Reference**: [`handbooks/core.cljs:321-413`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L321-L413)

**Keyboard interaction inside the search input** ([`handbooks/core.cljs:373-398`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L373-L398)):

| Key       | Behaviour                                                        |
| --------- | ---------------------------------------------------------------- |
| **Esc**   | Clears the query if non-empty; otherwise closes the handbook panel |
| **↑ / ↓** | Move the selection through results (wraps at both ends)           |
| **Enter** | Open the selected topic                                           |

> **Removed:** Claims of Tab/Enter card traversal and ←/→ chapter shortcuts outside the search input. No such bindings exist in current source; prev/next chapter are mouse-activated buttons.

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

The card is synthesised by the dashboard rather than coming from `handbooks.edn`: it is appended to the category list with `:key :ls-shortcuts`, and its subtitle is the total shortcut count summed over `shortcut-config/*config` at render time.

**Code Reference**: [`handbooks/core.cljs:281-300`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L281-L300), [`container.cljs:244-246`](../../../../../src/main/frontend/components/container.cljs#L244-L246)

> **Removed:** Static shortcut examples/counts in this doc. The shown count is computed at runtime from `shortcut-config/*config`.

---

## Deep Linking and Developer Tooling

Validated against current source in `src/`:

1. **Deep-link routing to handbook topics** (desktop):
  - `logseq://handbook/<topic-key>` is parsed in Electron and sent to the renderer, along with any query-string args.
  - The renderer opens the handbooks panel and routes to the topic key via `:handbook/route-chan`; a `go-loop` in the panel consumes the channel and navigates.

2. **Copyable handbook deep links from topic view**:
  - The topic header copy action writes `logseq://handbook/<key>` to the clipboard and shows a success notification.

3. **Developer watch mode for handbook content**:
  - The settings gear is visible only when `state/developer-mode?` is true; it opens a pane with a "Writing mode" toggle backed by the `:handbooks-dev-watch?` storage key.
  - When enabled, the endpoint switches to `http://localhost:1337` and the app polls `HEAD /handbooks.edn` every 2 seconds, reloading content whenever `content-length` changes.

**Code Reference**: [`url.cljs:111-113`](../../../../../src/electron/electron/url.cljs#L111-L113), [`listener.cljs:135-138`](../../../../../src/main/electron/listener.cljs#L135-L138), [`state.cljs:2082-2087`](../../../../../src/main/frontend/state.cljs#L2082-L2087), [`handbooks/core.cljs:610-619`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L610-L619) (copy link), [`562-575`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L562-L575) (dev watch poll), [`312-319`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L312-L319) (settings pane), [`548-560`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L548-L560) (route channel)

---

## Troubleshooting

### Handbooks Opens But Topic Link Does Not Route

**Symptoms**: `logseq://handbook/<key>` opens app but not the expected topic.

**Checks**:

1. Confirm key exists in loaded `handbooks.edn` tree (inspect `window.handbook-nodes`).
2. Confirm key normalization rules (lowercase + snake_case) for internal links.
3. Inspect console for `ERROR: handbook link resource not found`.

### Content Not Loading

**Symptoms**: loading state persists or error text appears in panel.

**Checks**:

1. Verify network access to `https://handbooks.pages.dev/handbooks.edn`.
2. If dev watch is enabled, verify local watch endpoint if used (`http://localhost:1337`).

### Screenshot Detail Is Too Small

**Symptom**: a screenshot in a topic is unreadable at the panel's width.

**Fix**: Click the image. Clicks on `img` elements inside topic content open a PhotoSwipe lightbox at the image's natural size ([`handbooks/core.cljs:239-244`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L239-L244)).

---

## Removed or Out-of-Scope Items

> **Removed:** "Future Enhancements" roadmap. These items are not backed by implementation in `src/`, `deps/`, or `packages/` as of 2026-05-30.

> **Removed:** Internal handbook authoring/deploy process details and asset optimization quotas. Current repository source validates runtime consumption behavior, not publishing workflow policy.

> **Removed:** `internal-docs/handbooks-authoring.md` pointer. That path is not present in this repository.

> **Removed:** "Handbooks vs external docs" comparison table. Its access/versioning claims are editorial positioning, not verifiable against source.

---

## Related Documentation

- [Getting Started](../../tutorials/getting-started/01-installation.md)
- [Video Embedding](../../how-to/multimedia/video-embed.md)
- [Keyboard Shortcuts](../../reference/configuration/keyboard-shortcuts.md)

---

## Code Reference

**Core Implementation**: [`src/main/frontend/extensions/handbooks/core.cljs`](../../../../../src/main/frontend/extensions/handbooks/core.cljs) (663 lines)

**Key Components** (line numbers at `be800f1711`):

| Component              | Lines   | Role                                              |
| ---------------------- | ------- | ------------------------------------------------- |
| `resolve-asset-url`    | 32-37   | Relative asset path → CDN/dev URL                 |
| `inflate-content-assets-urls` | 39-48 | Rewrites `src="..."` in topic HTML           |
| `parse-key-from-href`  | 50-59   | Internal content link → node key                  |
| `topic-card`           | 88-99   | Topic card component                              |
| `pane-category-topics` | 101-110 | Category topic list                               |
| `media-render`         | 112-125 | Video / YouTube / image rendering for `:demos`    |
| `chapter-select`       | 127-156 | Chapter navigation dropdown                       |
| `pane-topic-detail`    | 158-261 | Topic content view with demos and chapters        |
| `pane-dashboard`       | 263-310 | Dashboard view with popular topics and categories |
| `pane-settings`        | 312-319 | Developer-mode writing/watch toggle               |
| `search-bar`           | 321-413 | Fuzzy search over node titles                     |
| `footer-link-cards`    | 438-481 | Discord / forum cards on the dashboard            |
| `content`              | 483-663 | Panel root: load, route, pane dispatch            |

**Entrypoints**:

- Help menu toggle: [`src/main/frontend/components/container.cljs`](../../../../../src/main/frontend/components/container.cljs)
- Popup wrapper and draggable container: [`src/main/frontend/components/handbooks.cljs`](../../../../../src/main/frontend/components/handbooks.cljs)
- Desktop URL routing bridge: [`src/electron/electron/url.cljs`](../../../../../src/electron/electron/url.cljs), [`src/main/electron/listener.cljs`](../../../../../src/main/electron/listener.cljs)

**Dependencies**:

- YouTube player integration: [`src/main/frontend/extensions/video/youtube.cljs`](../../../../../src/main/frontend/extensions/video/youtube.cljs)
- Lightbox integration (PhotoSwipe): [`src/main/frontend/extensions/lightbox.cljs`](../../../../../src/main/frontend/extensions/lightbox.cljs)
- Shortcut config source: [`src/main/frontend/modules/shortcut/config.cljs`](../../../../../src/main/frontend/modules/shortcut/config.cljs)
- Fuzzy search: [`src/main/frontend/search.cljs`](../../../../../src/main/frontend/search.cljs)
- Strings: `:help.handbook/*` in [`src/resources/dicts/en.edn`](../../../../../src/resources/dicts/en.edn)

**External Libraries**:

- Glide.js: carousel for demo galleries (loaded on demand)
- hsx / Rum-style components (ClojureScript)

---

**Status**: 🟢 Stable
**Feedback**: Report handbooks issues on [GitHub](https://github.com/logseq/logseq/issues) with `handbooks` label
