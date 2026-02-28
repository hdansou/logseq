---
title: 'Handbooks System: Interactive In-App Tutorials'
status: '🟢 Stable'
version: 'Since v0.8.0'
quadrant: 'Explanation'
platform: 'Desktop (macOS/Windows/Linux), Web'
related-docs:
  - '../../how-to/multimedia/video-embed.md'
  - '../../tutorials/getting-started/01-installation.md'
  - '../../reference/interface/keyboard-shortcuts.md'
---

# Handbooks System: Interactive In-App Tutorials

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
2. Select **Handbooks**

**Method 2: Keyboard Shortcut**

- Press **?** (question mark) — Opens handbooks dialog

**Method 3: Search**

1. Open command palette (Cmd+K / Ctrl+K)
2. Type "handbook"
3. Select "Toggle handbooks"

**Result**: Modal dialog with handbook categories

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

```
handbooks.pages.dev/
  ├── index.json                 # Root structure
  ├── getting-started.edn        # Category manifest
  ├── getting-started/
  │   ├── quick-start.edn        # Topic manifest
  │   ├── quick-start.html       # Topic content
  │   ├── installation.edn
  │   └── installation.html
  ├── features/
  │   ├── queries.edn
  │   ├── queries.html
  │   ├── pdf.edn
  │   └── pdf.html
  └── assets/
      ├── demo-query.mp4
      ├── screenshot-1.png
      └── ...
```

**Content Format**:

- **Manifests** (.edn): Metadata, structure, navigation
- **Content** (.html): Markdown-rendered HTML for topic body
- **Assets**: Images, videos, demos

**Code Reference**: [`handbooks/core.cljs:25-36`](../../../src/main/frontend/extensions/handbooks/core.cljs#L25-L36)

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

1. Fetch `index.json` (root structure)
2. Parse categories and popular topics
3. Render dashboard

**On Category Click**:

1. Fetch `{category}.edn` (e.g., `getting-started.edn`)
2. Parse topics list
3. Render category view

**On Topic Click**:

1. Fetch `{category}/{topic}.edn` (metadata)
2. Fetch `{category}/{topic}.html` (content)
3. Parse chapters if multi-chapter
4. Render topic detail view

**Code Reference**: [`handbooks/core.cljs`](../../../src/main/frontend/extensions/handbooks/core.cljs)

**Caching**: Loaded content cached in memory for session (reused on re-open).

### Content Structure (EDN Format)

**Category Manifest**:

```clojure
{:key "getting-started"
 :title "Getting Started"
 :description "Essential tutorials for new users"
 :color "#10B981"
 :icon "book"
 :children
 [{:key "getting-started/quick-start"
   :title "Quick Start Guide"
   :description "Get up and running in 5 minutes"
   :cover "./assets/quickstart-cover.png"}
  {:key "getting-started/installation"
   :title "Installation"
   :description "Install Logseq on your platform"}]}
```

**Topic Manifest**:

```clojure
{:key "getting-started/quick-start"
 :title "Quick Start Guide"
 :demos ["./assets/quickstart-demo.mp4"]
 :content "<h2>Welcome to Logseq</h2><p>This guide will...</p>"
 :children ; Multi-chapter topics
 [{:key "getting-started/quick-start/chapter-1"
   :title "Chapter 1: Installation"
   :content "<h3>Installing Logseq</h3>..."}
  {:key "getting-started/quick-start/chapter-2"
   :title "Chapter 2: First Graph"
   :content "<h3>Creating your first graph</h3>..."}]}
```

**Code Reference**: [`handbooks/core.cljs:65-80`](../../../src/main/frontend/extensions/handbooks/core.cljs#L65-L80)

---

## Components

### Media Rendering

**Images**:

```html
<img src="./assets/screenshot.png" />
```

**Result**: Clickable image → Opens lightbox for full-size view

**Code**: [`handbooks/core.cljs:130-145`](../../../src/main/frontend/extensions/handbooks/core.cljs#L130-L145)

```clojure
(rum/defc media-render [src]
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

**Result**: Embedded YouTube player (using [`youtube.cljs`](../../../src/main/frontend/extensions/video/youtube.cljs))

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

**Code Reference**: [`handbooks/core.cljs:88-102`](../../../src/main/frontend/extensions/handbooks/core.cljs#L88-L102)

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

**Code Reference**: [`handbooks/core.cljs:147-172`](../../../src/main/frontend/extensions/handbooks/core.cljs#L147-L172)

---

## Keyboard Shortcuts Integration

### Accessing Shortcuts

**From Handbooks Dashboard**:

1. Open handbooks (**?**)
2. Click **"Keyboard Shortcuts"** card
3. Opens shortcuts panel in right sidebar

**Direct Access**:

1. Open right sidebar (Cmd+Shift+L / Ctrl+Shift+L)
2. Click **Shortcuts** tab

**Code Reference**: [`handbooks/core.cljs:325-330`](../../../src/main/frontend/extensions/handbooks/core.cljs#L325-L330)

```clojure
{:key      :ls-shortcuts
 :title    [:span "Keyboard shortcuts"]
 :children [:span (->> (vals @shortcut-config/*config)
                       (map count)
                       (apply +))
            " shortcuts"]
 :icon     "command"
 :on-click #(do (state/toggle! :ui/handbooks-open?)
                (state/open-right-sidebar!)
                (state/sidebar-add-block!
                  (state/get-current-repo)
                  "shortcut-settings"
                  :shortcut-settings))}
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

**See**: [Keyboard Shortcuts Reference](../../reference/interface/keyboard-shortcuts.md)

---

## Content Authoring (Internal)

### For Logseq Team

**Creating New Handbook**:

1. **Create Category** (`categories/{name}.edn`):

   ```clojure
   {:key "my-category"
    :title "My Category"
    :description "Description here"
    :color "#3B82F6"
    :icon "lightbulb"
    :children []}
   ```

2. **Create Topic** (`categories/{name}/{topic}.edn`):

   ```clojure
   {:key "my-category/my-topic"
    :title "My Topic"
    :cover "./assets/cover.png"
    :demos ["./assets/demo.mp4"]
    :content nil}  ; Or inline HTML
   ```

3. **Create Content** (`categories/{name}/{topic}.html`):

   ```html
   <h2>Topic Title</h2>
   <p>Content in HTML format (converted from Markdown)</p>
   <img src="./assets/screenshot.png" />
   ```

4. **Add Assets** (`assets/`):

   - Images: PNG, JPG, GIF, WebP, SVG
   - Videos: MP4, WebM
   - Keep assets < 5 MB each for fast loading

5. **Update Root** (`index.json`):

   ```json
   {
     "categories": ["my-category", ...],
     "popularTopics": ["my-category/my-topic", ...]
   }
   ```

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
  (if-let [matches (re-seq #"src=\"([^\"]+)\"" content)]
    (reduce
     (fn [content matched]
       (if-let [matched (second matched)]
         (string/replace content matched (resolve-asset-url matched))
         content))
     content matches)
    content))
```

---

## User Experience

### Contextual Help

**Problem**: Users stuck, need help without leaving app

**Solution**: Press **?** → Instant access to tutorials

**Example Workflow**:

1. User confused about queries
2. Press **?**
3. Search "query"
4. Click **"Advanced Queries"** topic
5. Watch demo video
6. Read guide
7. Close handbooks (**Esc**)
8. Apply knowledge immediately

### Onboarding

**New User Flow**:

1. Install Logseq
2. App suggests: "Press **?** for Quick Start Guide"
3. User opens handbooks
4. Follows **"Quick Start"** topic (5 min tutorial)
5. Creates first graph guided by handbook
6. Returns to handbooks for **"First Steps"** topic

**Goal**: Get users productive in < 10 minutes

### Keyboard-Friendly

**Navigation**:

- **?** — Open/close handbooks
- **Esc** — Close modal
- **Tab** — Navigate between cards
- **Enter** — Select card/topic
- **←** / **→** — Previous/next chapter

**Goal**: Accessible without mouse

---

## Comparison with External Docs

| Feature    | Handbooks (In-App)      | External Docs                             |
| ---------- | ----------------------- | ----------------------------------------- |
| Access     | Press **?** (instant)   | Open browser, navigate to docs.logseq.com |
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

**Problem**: Pressing **?** does nothing

**Possible Causes**:

1. Keyboard shortcut conflict
2. Modal already open (hidden behind)
3. Network issue (CDN unreachable)

**Solutions**:

- Check keyboard shortcuts: Settings → Shortcuts → "Toggle handbooks"
- Look for existing modal (press Esc first)
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

- [Video Embedding](../../how-to/multimedia/video-embed.md) — How handbook videos are embedded

**References**:

- [Keyboard Shortcuts](../../reference/interface/keyboard-shortcuts.md) — Complete shortcuts list (linked from handbooks)

**Explanations**:

- This document! — Understanding the handbooks system architecture

---

## Code Reference

**Core Implementation**: [`src/main/frontend/extensions/handbooks/core.cljs`](../../../src/main/frontend/extensions/handbooks/core.cljs) (661 lines)

**Key Components**:

- `pane-dashboard` (line 295-345) — Dashboard view with categories
- `pane-category-topics` (line 116-128) — Category topic list
- `pane-topic-detail` (line 174-280) — Topic content view with chapters
- `media-render` (line 130-145) — Video/image rendering
- `chapter-select` (line 147-172) — Chapter navigation dropdown
- `topic-card` (line 104-114) — Topic card component

**Dependencies**:

- YouTube integration: [`youtube.cljs`](../../../src/main/frontend/extensions/video/youtube.cljs)
- Lightbox: [`lightbox.cljs`](../../../src/main/frontend/extensions/lightbox.cljs)
- Shortcuts config: [`shortcut/config.cljs`](../../../src/main/frontend/modules/shortcut/config.cljs)

**External Libraries**:

- Glide.js: Carousel for demo galleries
- Rum: React-like components (ClojureScript)

---

**Last Updated**: January 2026  
**Status**: 🟢 Stable (Handbooks mature feature)  
**Feedback**: Report handbooks issues on [GitHub](https://github.com/logseq/logseq/issues) with `handbooks` label

---

**Content Team**: To update handbook content, see internal guide: `internal-docs/handbooks-authoring.md`
