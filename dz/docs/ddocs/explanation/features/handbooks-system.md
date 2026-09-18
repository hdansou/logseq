---
title: 'Handbooks System: In-App Help Panel'
status: '🟢 Stable'
version: 'Since 0.9.20'
quadrant: 'Explanation'
platform: 'Desktop (macOS/Windows/Linux), Web; not native mobile or published sites'
graphType: [db]
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
related-docs:
  - '../../how-to/multimedia/video-embed.md'
  - '../../tutorials/getting-started/01-installation.md'
  - '../../reference/configuration/keyboard-shortcuts.md'
---

# Handbooks System: In-App Help Panel

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-11 (verified against `be800f1711`)

## Overview

The Handbooks system is an in-app help panel that you can open from inside the editor. The app itself contains only the panel. At runtime it downloads the help content (categories, topics, chapters, images and videos) from `https://handbooks.pages.dev` and renders it in a draggable popup. The panel also leads into the keyboard shortcuts reference.

The feature was introduced by commit `1389836119` ("Feat: the new handbooks (#8524)"). The earliest release tag containing it is `0.9.20`.

**Key Features**:

- 📚 Topics authored as HTML, with demo images and videos
- ⌨️ Entry point into the keyboard shortcuts reference
- 🎨 Category-based organization (categories, topics, chapters)
- 🔍 Fuzzy search over topic titles
- 🖼️ Image lightbox for screenshots
- 📹 Embedded video demos (YouTube and local video files)
- 🌐 Content fetched at runtime from `handbooks.pages.dev`

## Accessing Handbooks

### Open Handbooks Panel

**Method 1: Help menu**

1. Click the **?** help button
2. Select **Handbook**

The **?** button is rendered only when the app is not native mobile and not a published site ([`container.cljs:600-602`](../../../../../src/main/frontend/components/container.cljs#L600-L602)). Its menu entry, defined in `help-menu-items`, calls `handbooks/toggle-handbooks`, which toggles `:ui/handbooks-open?`. Opening the help menu also closes an open handbook panel ([`container.cljs:258-262`](../../../../../src/main/frontend/components/container.cljs#L258-L262)).

**Code Reference**: [`container.cljs:244-246`](../../../../../src/main/frontend/components/container.cljs#L244-L246), [`container.cljs:298-320`](../../../../../src/main/frontend/components/container.cljs#L298-L320), [`handbooks.cljs:27-29`](../../../../../src/main/frontend/components/handbooks.cljs#L27-L29)

**Method 2: Deep link (desktop)**

`logseq://handbook/<topic-key>` opens the panel and routes directly to a topic. See [Deep Linking](#deep-linking-and-developer-tooling).

> **Note**: No command palette entry or keyboard shortcut opens handbooks; `src/main/frontend/modules/shortcut/` has no handbook entry. **Esc** closes the panel only while the handbook search input is focused and empty ([`handbooks/core.cljs:373-378`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L373-L378)).

**Result**: a draggable popup panel. The popup is a `.cp__handbooks-popup` container made draggable by `layout/setup-draggable-container!` ([`handbooks.cljs:11-25`](../../../../../src/main/frontend/components/handbooks.cljs#L11-L25)). It is rendered only while `:ui/handbooks-open?` is true ([`container.cljs:319-320`](../../../../../src/main/frontend/components/container.cljs#L319-L320)).

---

## Interface Overview

The diagrams below are illustrative: the actual categories, topics and counts come from the downloaded content tree, not from the app. The strings quoted are the English UI labels from `src/resources/dicts/en.edn`.

### Dashboard View

```txt
┌────────────────────────────────────────────────────────┐
│  Help                                           [✕]    │
├────────────────────────────────────────────────────────┤
│  [ 🔍 Search                                     ]     │
│                                                         │
│  Popular topics                                        │
│  ┌─────────────────────────┐ ┌─────────────────────────┐│
│  │ [cover] Topic title     │ │ [cover] Topic title     ││
│  │         description     │ │         description     ││
│  └─────────────────────────┘ └─────────────────────────┘│
│                                                         │
│  Help categories                                       │
│  ┌─────────────────────────────────────────┐          │
│  │ [icon] Getting started                  │          │
│  │        3 topics                         │          │
│  └─────────────────────────────────────────┘          │
│  ┌─────────────────────────────────────────┐          │
│  │ [icon] Keyboard shortcuts               │          │
│  │        <N> shortcuts                    │          │
│  └─────────────────────────────────────────┘          │
│                                                         │
│  [ Discord card ]            [ Forum card ]            │
└────────────────────────────────────────────────────────┘
```

The header reads **Help** (`:help.handbook/title`). A category card shows the category's icon, its title, and "N topics", where N counts its children. The card's left border takes the category's `:color` ([`handbooks/core.cljs:290-310`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L290-L310)).

The `Keyboard shortcuts` card is appended to the category list by the app; every other card comes from the content tree. Its `<N>` is computed at runtime.

The footer link cards (Discord and forum) render only on the dashboard pane ([`handbooks/core.cljs:438-481`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L438-L481), [`652-656`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L652-L656)). The Discord card fetches its online count from `https://plugins.logseq.io/ds` and re-fetches after 10 minutes (`:436`, `:443-453`). The forum card's "800+" is hard-coded (`:480`).

### Category View

```txt
┌────────────────────────────────────────────────────────┐
│  ⬅ Category title                               [✕]  │
├────────────────────────────────────────────────────────┤
│  [ 🔍 Search                                     ]     │
│                                                         │
│  ┌──────────────────────────────────────────┐         │
│  │ [cover] Topic title                       │         │
│  │         Topic description                 │         │
│  └──────────────────────────────────────────┘         │
│  ┌──────────────────────────────────────────┐         │
│  │ [cover] Topic title                       │         │
│  │         Topic description                 │         │
│  └──────────────────────────────────────────┘         │
│                                                         │
└────────────────────────────────────────────────────────┘
```

Topic cards show the optional `:cover` image, the `:title` and the `:description` ([`handbooks/core.cljs:88-99`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L88-L99)).

### Topic Detail View

```txt
┌────────────────────────────────────────────────────────┐
│  ⬅ Category title                 [⌂] [⧉] [⚙] [✕]  │
├────────────────────────────────────────────────────────┤
│  Current chapter  [ Chapter 1: Basics          ▾ ]    │
│                                                         │
│  [=============== Demo gallery ===========]           │
│  │                                           │          │
│  │      ▶️  Demo video or image              │          │
│  │                                           │          │
│  [=========================================]          │
│                                                         │
│  Topic HTML content ...                                │
│                                                         │
│  [⬅ Prev chapter]           [Next chapter ➡]         │
│                                                         │
└────────────────────────────────────────────────────────┘
```

The search bar renders only on the dashboard and category panes, not on topic detail ([`handbooks/core.cljs:636-639`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L636-L639)). The header icons ([`handbooks/core.cljs:607-626`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L607-L626)) are:

- **Home (⌂)**: shown when the navigation history has more than one entry.
- **Copy (⧉)**: shown only on topic detail.
- **Settings gear (⚙)**: shown only in developer mode.
- **Close (✕)**: always shown.

A topic without chapters shows its title as a heading instead of the chapter selector (`:196-208`).

---

## Architecture

### Content Hosting

**Location**: [https://handbooks.pages.dev](https://handbooks.pages.dev)

All content comes from one EDN file, `handbooks.edn`, at the endpoint root ([`handbooks/core.cljs:509`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L509)). It holds a single tree with all categories, topics, metadata and inline HTML content. Asset paths inside it (`:cover`, `:demos`, and `src="..."` in `:content`) may be relative. They are resolved against the same endpoint at render time.

> **Unverified:** how assets are laid out on `handbooks.pages.dev`. The app only resolves whatever relative paths the EDN contains, so the source does not show the layout.

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

Topic content is authored as HTML with relative asset paths. Before the HTML is injected into the DOM, every `src="..."` value is rewritten through `resolve-asset-url`. Relative paths therefore become absolute URLs on the content endpoint, or on the local dev-watch endpoint.

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

**When the panel opens** (the `content` component mounts):

1. Fetch `handbooks.edn` from the endpoint (single request)
2. Parse the whole tree with `edn/read-string`
3. Flatten the tree into a keyed node map with `tree-seq`, stored in component state
4. Render the dashboard with popular topics and categories

**On Category Click**:

1. Look up the category node in the in-memory node map
2. Render the category view (no additional network request)

**On Topic Click**:

1. Look up the topic node in the in-memory node map
2. If the topic has chapters (children in the tree), show its first chapter
3. Render the topic detail view (no additional network request)

**Code Reference**: `load-handbooks!` at [`handbooks/core.cljs:505-513`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L505-L513), called on mount at [`544-546`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L544-L546); the `tree-seq` node flattening is at [`578-585`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L578-L585).

**Caching**: while the panel stays open, all content comes from that single fetch, and moving between categories and topics needs no further requests. The data lives in component-local hook state ([`handbooks/core.cljs:485-501`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L485-L501)). The popup is unmounted when the panel closes, so reopening it fetches `handbooks.edn` again. The app adds no caching of its own.

While the fetch is in flight, the panel shows a loading state. On failure it renders the error string in a `<code>` block instead ([`handbooks/core.cljs:628-632`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L628-L632)).

### Content Structure (EDN Format)

All handbook content lives in `handbooks.edn` as a nested tree. The root has `:popular-topics` and `:children` (categories). Each category has `:children` (topics), and each topic may have `:children` (chapters). The node keys the app reads are `:key`, `:title`, `:description`, `:cover`, `:demos`, `:content`, `:color`, `:icon` and `:children`.

**Tree shape** (illustrative keys and values):

```clojure
{:popular-topics ["1.getting_started/quick_start"]
 :children
 [{:key "1.getting_started"
   :title "Getting Started"
   :color "#10B981"              ; category card border
   :icon "book"                  ; tabler icon name, default "chart-bubble"
   :children
   [{:key "1.getting_started/quick_start"
     :title "Quick Start Guide"
     :description "Get up and running"
     :cover "./assets/quickstart-cover.png"
     :demos ["./assets/quickstart-demo.mp4"]
     :content "<h2>Welcome to Logseq</h2><p>...</p>"
     :children ; optional chapters
     [{:key "1.getting_started/quick_start/chapter_1"
       :title "Chapter 1"
       :content "<h3>...</h3>"}]}]}]}
```

At load time the tree is flattened into a map keyed by `:key`, with the root stored under `"__root"`. Parent keys are computed from the `/`-delimited key paths (`bind-parent-key`, `parse-parent-key`). The map is also exposed as `window.handbook-nodes` for debugging.

Keys in the map are used verbatim. Popular-topic keys and internal content links, however, are lowercased and passed through `csk/->snake_case_string` before lookup ([`handbooks/core.cljs:58-59`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L58-L59), [`272-275`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L272-L275)). Node keys therefore need to be lowercase snake_case already for those lookups to match.

**Code Reference**: [`handbooks/core.cljs:61-72`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L61-L72) (key parsing), tree flattening at [`578-585`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L578-L585).

Two category keys get translated titles: `1.getting_started` renders as `:help/start` ("Getting started") and `2.sync` as `:help.handbook/sync` ("Sync"). All other categories use their `:title` verbatim ([`handbooks/core.cljs:74-79`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L74-L79)).

---

## Components

### Media Rendering

**Images**:

```html
<img src="./assets/screenshot.png" />
```

**Result**: clicking an image opens a full-size lightbox. Clicks inside rendered topic content are delegated: a click on an `img` opens the PhotoSwipe lightbox at the image's natural dimensions ([`handbooks/core.cljs:239-244`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L239-L244), [`lightbox.cljs`](../../../../../src/main/frontend/extensions/lightbox.cljs)).

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

`media-render` handles `:demos` entries only. Media inside `:content` is plain HTML injected with `dangerouslySetInnerHTML` and does not go through `media-render` (`:222`, `:232`, `:238`).

**YouTube Videos**:

```clojure
{:demos ["https://www.youtube.com/watch?v=ABC123"]}
```

**Result**: an embedded YouTube player (from [`youtube.cljs`](../../../../../src/main/frontend/extensions/video/youtube.cljs)) at 100% width and 235px height. Only `youtube.com/watch?v=` URLs are detected.

**Local Videos**:

```clojure
{:demos ["./assets/demo.mp4"]}
```

**Result**: a native HTML5 video player with controls, for any extension in `config/video-formats`.

### Demo Gallery (Carousel)

**Multiple Demos**:

```clojure
{:demos ["./assets/demo1.mp4"
         "./assets/demo2.mp4"
         "./assets/demo3.png"]}
```

**Result**: a Glide.js carousel with numbered bullet buttons (1, 2, 3…). A single demo, or a string instead of a vector, renders directly with no carousel. The Glide instance is mounted only when its carousel element exists, which requires more than one demo ([`handbooks/core.cljs:165-173`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L165-L173), [`211-232`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L211-L232)).

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

- **Glide.js**: the `@glidejs/glide` npm package (`package.json:145`), whose dist files are copied into `js/glide` at build time (`gulpfile.js:122-125`)
- **CSS**: `glide.core.min.css`, `glide.theme.min.css`
- **JS**: `glide.min.js`, loaded on demand whenever a topic detail pane mounts, and only if `window.Glide` is not already defined

> **Removed:** "Swipe on mobile" and "Auto-resize" feature claims. They describe Glide.js library behaviour; Logseq's handbook code does not configure or declare them. Carousel keyboard navigation is likewise not declared.

### Chapter Navigation

**Multi-Chapter Topics**:

```clojure
{:children
 [{:key "topic/chapter_1" :title "Chapter 1: Basics"}
  {:key "topic/chapter_2" :title "Chapter 2: Advanced"}
  {:key "topic/chapter_3" :title "Chapter 3: Pro Tips"}]}
```

**UI**:

```txt
Current chapter  [ Chapter 1: Basics ▾ ]

[Content here]

[⬅ Prev chapter]           [Next chapter ➡]
```

The chapter dropdown lists the sibling chapters except the current one. It closes on any click inside the handbook popup that lands outside `.chapters-select`. Prev/Next buttons render only when there is more than one chapter, and each is hidden at its end of the range.

**Code Reference**: [`handbooks/core.cljs:127-156`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L127-L156) (dropdown), [`253-261`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L253-L261) (prev/next controls)

### Internal Content Links

Anchors inside topic HTML whose `href` does not start with `http` are intercepted and resolved to a handbook node key; the browser does not navigate. `parse-key-from-href` resolves the key in four steps:

1. Strip a trailing `.edn`.
2. Treat a leading `@` as an absolute key; otherwise join the href to the current parent key.
3. Lowercase the result.
4. Snake-case it.

If no node matches, an error is logged to the console.

**Code Reference**: [`handbooks/core.cljs:50-59`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L50-L59), [`245-251`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L245-L251)

### Search

The search bar renders on the dashboard and category panes. It runs `search/fuzzy-search` over node `:title`s only, with a limit of 30 results. Topic content is not searched. On a category pane the search covers that category's topics and their chapters; on the dashboard it covers every node except the root.

**Code Reference**: [`handbooks/core.cljs:321-413`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L321-L413)

**Keyboard interaction inside the search input** ([`handbooks/core.cljs:373-398`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L373-L398)):

| Key       | Behaviour                                                        |
| --------- | ---------------------------------------------------------------- |
| **Esc**   | Clears the query if non-empty; otherwise closes the handbook panel |
| **↑ / ↓** | Move the selection through results (wraps at both ends)           |
| **Enter** | Open the selected topic                                           |

> **Removed:** claims of Tab/Enter card traversal and ←/→ chapter shortcuts outside the search input. No such bindings exist in the current source; prev/next chapter are mouse-activated buttons.

---

## Keyboard Shortcuts Integration

### Accessing Shortcuts

**From Handbooks Dashboard**:

1. Open handbooks from the help menu.
2. Click **Keyboard shortcuts** in the categories list.
3. Handbooks closes, the right sidebar opens, and the shortcuts view (`shortcut-settings`) is added to it.

**Direct Access**:

1. Open the help menu.
2. Click **Keyboard shortcuts**.

The card is created by the dashboard rather than coming from `handbooks.edn`. It is appended to the category list with `:key :ls-shortcuts`, and its subtitle is the total shortcut count summed over `shortcut-config/*config` at render time.

**Code Reference**: [`handbooks/core.cljs:280-300`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L280-L300), [`container.cljs:244-246`](../../../../../src/main/frontend/components/container.cljs#L244-L246)

> **Removed:** static shortcut examples and counts. The count shown is computed at runtime from `shortcut-config/*config`.

---

## Deep Linking and Developer Tooling

Validated against current source in `src/`:

1. **Deep-link routing to handbook topics** (desktop only; the mobile deep-link handler has no `handbook` host):
   - Electron parses `logseq://handbook/<topic-key>` and sends the renderer a `:key` (the path without leading slashes) plus the query string as `:args`. The renderer uses only `:key`.
   - `state/open-handbook-pane!` opens the panel if it is closed and puts the key on `:handbook/route-chan`, a sliding buffer of 1. A `go-loop` in the panel takes keys from the channel once nodes are loaded and navigates to the matching node.
   - The key is looked up **verbatim**, with no lowercasing or snake-casing. An unknown key is ignored silently.

2. **Copyable handbook deep links from topic view**:
   - The copy icon in the topic header writes `logseq://handbook/<key>` to the clipboard and shows a success notification.

3. **Developer watch mode for handbook content**:
   - The settings gear is visible only when `state/developer-mode?` is true. It opens a pane with a "Writing mode" toggle backed by the `:handbooks-dev-watch?` storage key.
   - When enabled, the endpoint switches to `http://localhost:1337`. The app sends `HEAD /handbooks.edn` every 2 seconds and reloads the content whenever `content-length` changes.

**Code Reference**: [`url.cljs:111-114`](../../../../../src/electron/electron/url.cljs#L111-L114), [`listener.cljs:141-144`](../../../../../src/main/electron/listener.cljs#L141-L144), [`state.cljs:356`](../../../../../src/main/frontend/state.cljs#L356), [`state.cljs:2082-2087`](../../../../../src/main/frontend/state.cljs#L2082-L2087), [`handbooks/core.cljs:548-560`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L548-L560) (route channel), [`610-619`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L610-L619) (copy link), [`562-576`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L562-L576) (dev watch poll), [`312-319`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L312-L319) (settings pane)

---

## Troubleshooting

### Handbooks Opens But Topic Link Does Not Route

**Symptoms**: `logseq://handbook/<key>` opens the panel but not the expected topic.

**Checks**:

1. Confirm the key exists **exactly** in the loaded tree: inspect `window.handbook-nodes`. Deep-link keys are not normalized.
2. Remember that an unknown deep-link key fails silently; nothing is logged.
3. For links *inside* topic content, which are lowercased and snake-cased, check the console for `ERROR: handbook link resource not found`.

### Content Not Loading

**Symptoms**: the loading state persists, or error text appears in the panel.

**Checks**:

1. Verify network access to `https://handbooks.pages.dev/handbooks.edn`.
2. If developer watch mode is enabled, the app reads from `http://localhost:1337` instead. Start the local server, or turn off "Writing mode" in the settings pane.

### Screenshot Detail Is Too Small

**Symptom**: a screenshot in a topic is unreadable at the panel's width.

**Fix**: click the image. Clicks on `img` elements inside topic content open a PhotoSwipe lightbox at the image's natural size ([`handbooks/core.cljs:239-244`](../../../../../src/main/frontend/extensions/handbooks/core.cljs#L239-L244)).

---

## Removed or Out-of-Scope Items

> **Removed:** "Future Enhancements" roadmap. These items are not backed by implementation in `src/`, `deps/` or `packages/`.

> **Removed:** internal handbook authoring and deploy process details, and asset optimization quotas. The repository source shows how the app consumes the content, not how it is published.

> **Removed:** the `internal-docs/handbooks-authoring.md` pointer. That path is not present in this repository.

> **Removed:** the "Handbooks vs external docs" comparison table. Its access and versioning claims are editorial positioning, not verifiable against source.

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
| `get-handbooks-endpoint` | 24-30 | Content endpoint (production or dev watch)        |
| `resolve-asset-url`    | 32-37   | Relative asset path → endpoint URL                |
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

- Help menu and **?** button: [`src/main/frontend/components/container.cljs`](../../../../../src/main/frontend/components/container.cljs)
- Popup wrapper and draggable container: [`src/main/frontend/components/handbooks.cljs`](../../../../../src/main/frontend/components/handbooks.cljs)
- Desktop URL routing bridge: [`src/electron/electron/url.cljs`](../../../../../src/electron/electron/url.cljs), [`src/main/electron/listener.cljs`](../../../../../src/main/electron/listener.cljs)

**Dependencies**:

- YouTube player integration: [`src/main/frontend/extensions/video/youtube.cljs`](../../../../../src/main/frontend/extensions/video/youtube.cljs)
- Lightbox integration (PhotoSwipe): [`src/main/frontend/extensions/lightbox.cljs`](../../../../../src/main/frontend/extensions/lightbox.cljs)
- Shortcut config source: [`src/main/frontend/modules/shortcut/config.cljs`](../../../../../src/main/frontend/modules/shortcut/config.cljs)
- Fuzzy search: [`src/main/frontend/search.cljs:13`](../../../../../src/main/frontend/search.cljs#L13)
- Strings: `:help.handbook/*` in [`src/resources/dicts/en.edn`](../../../../../src/resources/dicts/en.edn)

**External Libraries**:

- Glide.js (`@glidejs/glide`): carousel for demo galleries, loaded on demand
- HSX components (`io.factorhouse.hsx.core`), which replaced Rum (commit `d185ea58ee`)

---

**Status**: 🟢 Stable
**Feedback**: Report handbooks issues on [GitHub](https://github.com/logseq/logseq/issues)
