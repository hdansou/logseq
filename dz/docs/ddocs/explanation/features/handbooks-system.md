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

> **Last revised:** 2026-03-10

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

**Method 2: Command Palette**

1. Open command palette (Cmd+K / Ctrl+K)
2. Type "handbook"
3. Select the handbook option

> **Note**: There is no dedicated keyboard shortcut (e.g., `?`) for opening handbooks. Access is through the Help menu or command palette.

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

```html
<!-- In handbook HTML content -->
<video src="https://www.youtube.com/watch?v=ABC123"></video>
```

**Result**: Embedded YouTube player (using [`youtube.cljs`](../../src/main/frontend/extensions/video/youtube.cljs))

**Local Videos**:

```html
<video src="./assets/demo.mp4"></video>
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
- Keyboard navigation (← →)
- Bullet navigation (1, 2, 3...)
- Auto-resize to container

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

1. Open handbooks via Help menu
2. Click **"Keyboard shortcuts"** card at the bottom of the categories list
3. Handbooks panel closes and shortcuts panel opens in the right sidebar

**Direct Access**:

1. Open Help menu (top bar)
2. Click **"Keyboard shortcuts"** directly (separate menu item from Handbook)

**Code Reference**: [`handbooks/core.cljs:278-296`](../../src/main/frontend/extensions/handbooks/core.cljs#L278-L296)

```clojure
;; The keyboard shortcuts card is appended to the categories list in pane-dashboard:
{:key      :ls-shortcuts
 :title    [:span "Keyboard shortcuts"]
 :children [:span (->> (vals @shortcut-config/*config)
                       (map count)
                       (apply +))
            " shortcuts"]
 :color    "#2563EB"
 :icon     "command"}

;; On click, the handler checks for :ls-shortcuts key and opens the sidebar:
:on-click #(if (= key :ls-shortcuts)
             (do (state/toggle! :ui/handbooks-open?)
                 (state/open-right-sidebar!)
                 (state/sidebar-add-block! (state/get-current-repo)
                   "shortcut-settings" :shortcut-settings))
             (nav-to-pane! [:topics category title] pane-state))
```

### Shortcut Categories

Shortcuts organized by functional area:

| Category             | Examples                                                     |
| -------------------- | ------------------------------------------------------------ |
| **Editor Basics**    | Enter, Tab, Cmd+B (bold), Cmd+I (italic)                     |
| **Navigation**       | Cmd+K (search), Cmd+O (open page), Cmd+Shift+P (page search) |
| **Block Operations** | Cmd+Shift+M (move), Cmd+Shift+↑/↓ (reorder), Cmd+L (link)    |
| **Formatting**       | Cmd+B (bold), Cmd+I (italic), Cmd+U (underline)              |
| **Commands**         | / (slash command), [[ (page reference), (( (block reference) |
| **View**             | Cmd+. (right sidebar), Cmd+Shift+L (toggle sidebar)          |
| **Git**              | Cmd+Shift+U (push), Cmd+Shift+D (diff)                       |
| **Dev Tools**        | Cmd+Shift+I (developer tools)                                |

**Total**: 150+ keyboard shortcuts

**See**: [Keyboard Shortcuts Reference](../../reference/features/keyboard-shortcuts.md)

---

## Content Authoring (Internal)

### For Logseq Team

**Creating New Handbook Content**:

All content is authored in a single `handbooks.edn` file. To add a new category or topic, edit this file directly.

1. **Add a Category** (top-level child in the tree):

   ```clojure
   {:key "my-category"
    :title "My Category"
    :description "Description here"
    :color "#3B82F6"
    :icon "lightbulb"
    :children []}
   ```

2. **Add a Topic** (child of a category):

   ```clojure
   {:key "my-category/my-topic"
    :title "My Topic"
    :cover "./assets/cover.png"
    :demos ["./assets/demo.mp4"]
    :content "<h2>Topic Title</h2><p>Content in HTML format</p>"}
   ```

3. **Add Chapters** (children of a topic, optional):

   ```clojure
   {:key "my-category/my-topic/chapter-1"
    :title "Chapter 1: Basics"
    :content "<h3>Getting started</h3>..."}
   ```

4. **Add Popular Topics** (reference by key in root `:popular-topics`):

   ```clojure
   {:popular-topics ["my-category/my-topic" ...]
    :children [...]}
   ```

5. **Add Assets** (`assets/`):

   - Images: PNG, JPG, GIF, WebP, SVG
   - Videos: MP4, WebM
   - Keep assets < 5 MB each for fast loading
   - Reference with relative paths (e.g., `./assets/screenshot.png`); URLs are resolved to CDN at render time

6. **Deploy**: Push to `handbooks.pages.dev` (Cloudflare Pages)

### Asset Optimization

**Images**:

- Max width: 1200px
- Format: WebP (best compression) or PNG/JPG
- Compression: TinyPNG or ImageOptim
- Keep < 500 KB per image

**Videos**:

- Use YouTube for long videos (> 2 min)
- Local videos: MP4 H.264, < 5 MB
- Resolution: 1280x720 (720p)
- Bitrate: 1-2 Mbps

**Code Reference**: Asset URL resolution automatically prepends CDN domain:

```clojure
(defn inflate-content-assets-urls [content]
  (if-let [matches (and (not (string/blank? content))
                        (re-seq #"src=\"([^\"]+)\"" content))]
    (reduce
     (fn [content matched]
       (if-let [matched (second matched)]
         (string/replace content matched (resolve-asset-url matched)) content))
     content matches)
    content))
```

---

## User Experience

### Contextual Help

**Problem**: Users stuck, need help without leaving app

**Solution**: Open Help menu → **Handbook** → Instant access to tutorials

**Example Workflow**:

1. User confused about queries
2. Open Help → Handbook
3. Search "query"
4. Click **"Advanced Queries"** topic
5. Watch demo video
6. Read guide
7. Close handbooks (click X button)
8. Apply knowledge immediately

### Onboarding

**New User Flow**:

1. Install Logseq
2. User opens Help → Handbook
4. Follows **"Quick Start"** topic (5 min tutorial)
5. Creates first graph guided by handbook
6. Returns to handbooks for **"First Steps"** topic

**Goal**: Get users productive in < 10 minutes

### Keyboard-Friendly

**Navigation within the handbooks panel**:

- **Esc** — Close search or panel
- **Tab** — Navigate between cards
- **Enter** — Select card/topic
- **Up/Down arrows** — Navigate search results
- **←** / **→** — Previous/next chapter (in topic detail view)

**Goal**: Accessible without mouse once the panel is open

---

## Comparison with External Docs

| Feature    | Handbooks (In-App)      | External Docs                             |
| ---------- | ----------------------- | ----------------------------------------- |
| Access     | Help menu → Handbook    | Open browser, navigate to docs.logseq.com |
| Context    | No context switch       | Leaves app                                |
| Search     | Within handbooks only   | Global search                             |
| Offline    | Requires internet (CDN) | Requires internet                         |
| Multimedia | Videos, images, demos   | Text-heavy                                |
| Updates    | Automatic (CDN)         | Manual (git pull)                         |
| Versioning | Not version-specific    | Can show older versions                   |

**Best Use Cases**:

**Handbooks**:

- Quick reference while working
- Step-by-step tutorials
- Keyboard shortcut lookup
- Visual demos

**External Docs**:

- Deep dives (API, architecture)
- Code examples
- Community contributions
- Search engine indexing

---

## Troubleshooting

### Handbooks Won't Open

**Problem**: Clicking Handbook in Help menu does nothing

**Possible Causes**:

1. Panel already open (hidden behind another window or off-screen)
2. Network issue (CDN unreachable)

**Solutions**:

- Look for existing handbook panel (it is a draggable popup that may have been moved off-screen)
- Check internet connection
- Restart Logseq

### Content Not Loading

**Problem**: Blank screen or loading spinner

**Possible Causes**:

1. Network timeout
2. CDN down (rare)
3. CORS issue (dev mode)

**Solutions**:

- Wait 10 seconds (slow network)
- Check browser console: Help → Toggle Developer Tools → Console
- Try again later
- Report issue if persistent

### Videos Won't Play

**Problem**: Video player shows error

**Possible Causes**:

1. YouTube video unavailable (geographic restriction)
2. Local video codec unsupported
3. Ad blocker interfering

**Solutions**:

- Test YouTube URL in browser
- Disable ad blocker temporarily
- Try different browser (Electron vs. Chrome)

### Images Too Small

**Problem**: Can't see details in screenshots

**Solution**: Click image → Opens lightbox (full-size view)

**Zoom**:

- **+** button — Zoom in
- **-** button — Zoom out
- Click outside → Close lightbox

---

## Future Enhancements

### Planned Features (🟡 Active Development)

**Offline Handbooks**:

- Cache handbooks locally
- Access without internet
- Update on startup

**Version-Specific Handbooks**:

- Show docs for installed version
- Highlight new features in updates
- Deprecation warnings

**Interactive Demos**:

- Try features within handbook (sandbox)
- Live code examples
- Practice exercises

**Progress Tracking**:

- Mark topics as "completed"
- Track tutorial progress
- Suggest next topics

**Localization**:

- Translate handbooks to multiple languages
- Community contributions via Git

### Research Areas

- AI-powered help (answer questions based on handbook content)
- Video transcription for searchable content
- User-generated handbooks (community tutorials)

---

## Related Documentation

**Tutorials**:

- [Getting Started](../../tutorials/getting-started/01-installation.md) — Installation guide (similar to handbook content)
- [Basic Editing](../../tutorials/getting-started/03-basic-editing.md) — Editing fundamentals

**How-To Guides**:

- [Video Embedding](../multimedia/video-embed.md) — How handbook videos are embedded

**References**:

- [Keyboard Shortcuts](../../reference/features/keyboard-shortcuts.md) — Complete shortcuts list (linked from handbooks)

**Explanations**:

- This document! — Understanding the handbooks system architecture

---

## Code Reference

**Core Implementation**: [`src/main/frontend/extensions/handbooks/core.cljs`](../../src/main/frontend/extensions/handbooks/core.cljs) (660 lines)

**Key Components**:

- `topic-card` (line 83-94) — Topic card component
- `pane-category-topics` (line 96-106) — Category topic list
- `media-render` (line 108-121) — Video/image rendering
- `chapter-select` (line 123-152) — Chapter navigation dropdown
- `pane-topic-detail` (line 154-257) — Topic content view with chapters
- `pane-dashboard` (line 259-306) — Dashboard view with categories
- `content` (line 480-660) — Main component with state management and data loading

**Dependencies**:

- YouTube integration: [`youtube.cljs`](../../src/main/frontend/extensions/video/youtube.cljs)
- Lightbox: [`lightbox.cljs`](../../src/main/frontend/extensions/lightbox.cljs)
- Shortcuts config: [`shortcut/config.cljs`](../../src/main/frontend/modules/shortcut/config.cljs)

**External Libraries**:

- Glide.js: Carousel for demo galleries
- Rum: React-like components (ClojureScript)

---

**Last Updated**: March 2026
**Status**: 🟢 Stable (Handbooks mature feature)  
**Feedback**: Report handbooks issues on [GitHub](https://github.com/logseq/logseq/issues) with `handbooks` label

---

**Content Team**: To update handbook content, see internal guide: `internal-docs/handbooks-authoring.md`
