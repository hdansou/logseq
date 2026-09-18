---
title: 'PDF Annotations'
status: '🟠 Desktop and web app shell only'
version: 'PDF extension since 2021-07 (82330fb3f4, first tag 0.3.2); storage as of DB schema 65.33'
quadrant: 'Reference'
graphType: [db]
platform: 'Desktop (Electron) and web app shell; not mounted by the native mobile app shell'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
related-docs:
  - '../../how-to/multimedia/asset-management.md'
  - '../../reference/database/schema-overview.md#block-classes'
  - '../../tutorials/getting-started/03-basic-editing.md'
---

> **Last revised:** 2026-09-11

# PDF Annotations Reference

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

## Overview

Logseq includes a PDF viewer built on PDF.js (`pdfjsViewer.PDFViewer`, `src/main/frontend/extensions/pdf/core.cljs:802-813`). You can highlight text, capture rectangular areas as images, search inside the PDF and browse its outline without leaving the app. Every highlight is stored as a block tagged with the built-in `PDF Annotation` class, as a child of the PDF's asset block.

**Key Features**:

- 📄 PDF viewing with a page input and previous/next buttons
- ✏️ Text highlighting (5 fixed colours)
- 📷 Area highlighting, saved as PNG assets
- 🔍 Find in PDF (Enter to search, match case)
- 🔗 Each highlight is a block with a `[[uuid]]` ref
- 📑 Outline (table of contents) and Highlights list
- 🔍 Zoom in/out, auto fit, Ctrl/Cmd+wheel zoom
- ⌨️ Four keyboard shortcuts

⚠️ **Platform Limitation**: The viewer is mounted by the shared desktop/web app container (`src/main/frontend/components/theme.cljs:159`, used from `src/main/frontend/components/container.cljs:502`). The native mobile app shell (`src/main/mobile/`) does not require that container or any `frontend.extensions.pdf` namespace, so the viewer is not mounted there. See [Platform Compatibility](#platform-compatibility).

## Quick Start

### Open a PDF

In a DB graph a PDF is an **asset block**: a block tagged `#Asset` whose file is stored as `assets/<block-uuid>.pdf` (`src/main/frontend/handler/editor.cljs:1793-1835`, file name at `:1825`).

**Method 1: Drag & Drop or Paste**

1. Start editing a block
2. Drop the PDF onto the main content area (`src/main/frontend/components/container.cljs:66-74`), or paste a copied file (`src/main/frontend/handler/paste.cljs:271-280`)
3. Logseq creates an asset block and copies the file into `assets/`
4. Click the PDF's title → the viewer opens

**Method 2: Slash Command**

1. Type `/Upload an asset` (`src/main/frontend/commands.cljs:335-338`; label `src/resources/dicts/en.edn:568`)
2. Select the PDF in the file dialog
3. Click the PDF's title to open it

Assets larger than 100 MB are rejected (`src/main/frontend/handler/editor.cljs:1818-1822`). Uploading a file whose checksum matches an existing asset shows "Asset exists already" instead of creating a duplicate (`editor.cljs:1803-1811`).

**Method 3: External Link**

```markdown
![Research Paper](https://arxiv.org/pdf/2301.12345.pdf)
```

The leading `!` is required: `show-link?` only routes a link to the media renderer when the text starts with `!` or is a bare http(s) link to an image/audio/video format (`src/main/frontend/components/block.cljs:1434-1444`, `src/main/frontend/config.cljs:186`). `pdf` is in `doc-formats` (`config.cljs:174-175`), so the link renders as a PDF link that opens the viewer (`block.cljs:603-611`).

A PDF opened from a link has no asset block. Picking a highlight colour, or clicking **Annotations page**, opens the **Create asset** dialog with the warning "Creating a local asset from an external one. PDF annotations require a local asset to work properly." (`core.cljs:184-186`, `src/main/frontend/extensions/pdf/toolbar.cljs:583-586`, `src/main/frontend/components/assets.cljs:280-300`, `en.edn:86`).

**Result**: The viewer opens in a resizable panel beside the main content (`core.cljs:87-124`, `core.cljs:1100-1113`)

---

## Viewer Interface

### Layout

```txt
┌─────────────────────────────────────────────────────────────────────────┐
│ [⚙] [▭] [🖍] [🔍-] [🔍+] [Fit] [☰] [🔍] [Annotations] [⧉] [ 3 ]/25 [↑][↓] Close │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│                          PDF Content                                     │
│                                                                          │
│  Selected text → context menu:                                           │
│  [yellow] [red] [green] [blue] [purple]                                  │
│  Copy ref · Copy text · Linked references · Delete                       │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Toolbar Controls

In toolbar order (`src/main/frontend/extensions/pdf/toolbar.cljs:527-629`):

| Button (tooltip)                    | Function                                                                                  | Shortcut |
| ----------------------------------- | ----------------------------------------------------------------------------------------- | -------- |
| More settings                       | Viewer theme (light/warm/dark), area-highlight dashed style, coloured label for highlight blocks, auto-open context menu, document metadata (`toolbar.cljs:85-113`) | —        |
| Area highlight (⌘ on macOS, Shift elsewhere) | Toggle area-selection mode                                                        | —        |
| Highlight mode                      | While on, a new text selection is highlighted immediately with the last-used colour (`core.cljs:150`, `core.cljs:206-208`) | —        |
| Zoom out / Zoom in                  | Step zoom                                                                                 | —        |
| Auto fit                            | Sets `currentScaleValue` to `"auto"` (`src/main/frontend/extensions/pdf/utils.cljs:111-112`) | —        |
| Outline                             | Popup with **Contents** and **Highlights** tabs                                           | —        |
| Search                              | Find in PDF                                                                               | Alt+F    |
| Annotations page                    | Navigates to the PDF asset block, whose children are the annotation blocks (`src/main/frontend/extensions/pdf/assets.cljs:265-269`) | —        |
| Open in external window             | Moves the viewer into a separate window (see [External Window](#external-window))         | —        |
| Page input, ↑ / ↓                   | Type a page number and press Enter; previous / next page                                  | Alt+P / Alt+N |
| Close                               | Close the viewer                                                                          | Alt+X    |

Bindings are registered as the `:pdf/*` shortcut namespace: `:pdf/previous-page` `alt+p`, `:pdf/next-page` `alt+n`, `:pdf/close` `alt+x`, `:pdf/find` `alt+f` (`src/main/frontend/modules/shortcut/config.cljs:54-64`). The handler group is enabled only while not editing a block (`config.cljs:599-601`) and only while the viewer is mounted (`core.cljs:1058-1061`). They are listed under **Others** in the shortcut settings (`config.cljs:882-887`, label `en.edn:1793`).

Tooltip labels are from `src/resources/dicts/en.edn:1185-1216`.

---

## Text Highlighting

### Create Highlight

1. Open a PDF
2. Select text
3. The context menu appears (with the default **Auto open context menu for selections** setting on; with it off, hold Alt while selecting — `core.cljs:143`, `core.cljs:151-152`, default at `src/main/frontend/state.cljs:291`):
   ```txt
   [yellow] [red] [green] [blue] [purple]
   Copy text
   ```
4. Click a colour
5. Logseq creates the annotation block and copies its `[[uuid]]` ref to the clipboard (`core.cljs:188-195`, `assets.cljs:210-216`)

**Result**:

- The text is highlighted in the PDF
- A child block is inserted under the PDF asset block (`assets.cljs:104-110`). Its title is the selected text, its UUID equals the highlight id, and it carries these properties (`assets.cljs:92-103`):
  - `:block/tags` `#{:logseq.class/Pdf-annotation}`
  - `:logseq.property/ls-type` `:annotation`
  - `:logseq.property.pdf/hl-color` (closed value), `:logseq.property/asset` (the PDF asset), `:logseq.property.pdf/hl-page`, `:logseq.property.pdf/hl-value` (the full highlight map)
- In the outline the block shows a `P<page>` prefix (`block.cljs:2491-2495`) and a `data-hl-color` attribute used to colour it (`block.cljs:3499-3502`)

### Highlight Colours

The palette is fixed to five colours (`core.cljs:226-228`). They are the closed values of `:logseq.property.pdf/hl-color` (`deps/db/src/logseq/db/frontend/property.cljs:231-242`):

| Colour    | DB Ident (closed value)         | Value      |
| --------- | ------------------------------- | ---------- |
| 🟡 Yellow | `:logseq.property/color.yellow` | `"yellow"` |
| 🔴 Red    | `:logseq.property/color.red`    | `"red"`    |
| 🟢 Green  | `:logseq.property/color.green`  | `"green"`  |
| 🔵 Blue   | `:logseq.property/color.blue`   | `"blue"`   |
| 🟣 Purple | `:logseq.property/color.purple` | `"purple"` |

The colours carry no built-in meaning. The initial last-used colour is yellow (`core.cljs:35`).

**Tip**: Pick a convention and use it consistently (e.g. red = action items, blue = quotes).

### Navigate to Highlight

**From the PDF Viewer**:

- Click a highlight → its context menu opens (`core.cljs:258-264`, `core.cljs:273-276`) with the colours, **Copy ref**, **Copy text** (text highlights only), **Linked references** and **Delete** (`core.cljs:226-236`)
- **Linked references** navigates to the annotation block (`assets.cljs:257-263`)
- **Copy ref** copies `[[<uuid>]]` (`assets.cljs:210-216`, `src/main/frontend/util/ref.cljs:5-7`)
- In **Outline → Highlights**, click an entry to scroll to it; double-click or use its link button to go to the annotation block (`toolbar.cljs:399-420`)
- Drag a highlight into the editor to drop a `[[<uuid>]]` ref (`core.cljs:266-271`, `core.cljs:303-305`)

**From the Graph**:

- Click the `P<page>` prefix of an annotation block → the PDF opens and scrolls to the highlight (`block.cljs:2475-2486`, `assets.cljs:234-251`). The prefix is rendered inside `text-block-title` (`block.cljs:2443`, `block.cljs:2475-2498`), so it also appears where the block is shown through a `[[<uuid>]]` ref.

### Update Highlight Colour

1. Click an existing highlight
2. Click a different colour
3. The overlay and the annotation block's `hl-color` update (`core.cljs:197-198`, `core.cljs:595-601`, `assets.cljs:181-189`)

> **Note:** `update-hl-block!` writes only `:logseq.property.pdf/hl-color` (`assets.cljs:181-189`). Highlights are reloaded from `:logseq.property.pdf/hl-value` (`assets.cljs:122-133`), which is written only when the block is created (`assets.cljs:99`), and the overlay colour is read from that map (`core.cljs:256`). From source, the overlay should show the original colour after the PDF is reopened; this has not been reproduced live.

### Delete Highlight

**Method 1: From the PDF**

1. Click the highlight
2. Select **Delete** — the overlay is removed and the annotation block is deleted (`core.cljs:174-178`, `assets.cljs:203-208`)

**Method 2: From the Block**

1. Delete the annotation block
2. The highlight is gone the next time the PDF is opened, because highlights are loaded from blocks whose `:logseq.property/asset` is the PDF (`assets.cljs:122-133`)

---

## Area Highlighting (Screenshots)

### Create Area Highlight

1. Open a PDF
2. Turn on area-selection mode with the toolbar button, or hold ⌘ (Meta) or Shift while dragging (`core.cljs:433-439`)
3. Drag a rectangle larger than 10×10 px on a page (`core.cljs:514-515`)
4. The context menu appears; click a colour
5. The region is captured and the annotation block is created. Area mode turns itself off after one selection (`core.cljs:534`)

**Result**:

- The region is drawn from the page canvas at `devicePixelRatio` into a PNG (`assets.cljs:149-179`) and saved through the normal asset pipeline as a file named `pdf area highlight.png` (`assets.cljs:139-142`)
- That creates a separate **Asset** block whose file is `assets/<image-asset-uuid>.png` (`editor.cljs:1825`; read back via `src/main/frontend/handler/assets.cljs:52-57`)
- The annotation block gets `:logseq.property.pdf/hl-type :area` and `:logseq.property.pdf/hl-image` pointing to that asset (`assets.cljs:101-103`). Its title is the current localised date rather than text, and it is collapsed (`assets.cljs:86-94`)
- The image asset block is moved under the annotation block (`assets.cljs:112-120`)
- The annotation block renders the image with **Asset ref block**, **Copy image** and **Maximize image** buttons (`assets.cljs:291-334`, `block.cljs:2497-2498`)

Existing area highlights can be resized by dragging their edges; the image is re-captured (`core.cljs:313-390`). As with colour changes, only `hl-color` is written back (`core.cljs:306`, `assets.cljs:181-189`), so the new position is not saved to `hl-value`.

### Use Cases

- Capture diagrams from research papers
- Screenshot tables and charts
- Save equations as images
- Archive visual content

### Edit Area Image

The capture is an ordinary image asset. Use **Asset ref block** on the image to open its asset block (`assets.cljs:308-314`).

> **Unverified:** Editing the PNG outside Logseq or replacing it with a modified file was not traced in source.

---

## PDF Navigation

### Page Navigation

**Toolbar**:

- **Page input** — type a page number and press Enter; values above the page count go to the last page (`toolbar.cljs:605-618`)
- **↑ / ↓** — previous / next page (`toolbar.cljs:621-623`)

**Keyboard** (not while editing a block):

- **Alt+P** — Previous page
- **Alt+N** — Next page

**Mouse**:

- **Scroll wheel** — Scroll through pages

The last visited page is saved per PDF in `:logseq.property.asset/last-visit-page` (`core.cljs:925-931`) and restored on open (`assets.cljs:131`, `core.cljs:958-959`). That property is excluded from sync (`deps/db/src/logseq/db/frontend/property.cljs:589-593`).

### Table of Contents

**Open Outline**:

1. Click the **Outline** toolbar button
2. A popup opens on the **Contents** tab (`toolbar.cljs:422-460`)

**Navigate**:

- Click a title → jumps to its destination (`toolbar.cljs:308-309`)
- Click the arrow to expand/collapse nested entries (`toolbar.cljs:303-307`)
- Esc closes the popup (`toolbar.cljs:351-359`)

**Example Outline**:

```txt
Contents | Highlights
  ├─ Abstract
  ├─ 1. Introduction
  ├─ 2. Related Work
  │   ├─ 2.1 Background
  │   └─ 2.2 Prior Art
  ├─ 3. Methodology
  └─ References
```

**Note**: Contents come from the PDF's embedded outline (`toolbar.cljs:339`); without one the tab shows "No outlines" (`toolbar.cljs:374`, `en.edn:1203`).

### Search Within PDF

**Open Search**:

- Click **Search** in the toolbar
- Or press **Alt+F** (`src/main/frontend/extensions/pdf/utils.cljs:188-193`)

**Search Box**:

```txt
┌─────────────────────────────────────────────┐
│  [search            ]  [Aa] [˄] [˅] [✕]     │
│  3 of 12 matches ("machine learning")       │
└─────────────────────────────────────────────┘
```

**Features**:

- Search runs on Enter, not while typing (`toolbar.cljs:229-240`)
- **Aa** toggles match case (`toolbar.cljs:258-261`)
- Chevron buttons go to the previous / next match (`toolbar.cljs:263-270`)
- All matches are highlighted as a phrase search (`toolbar.cljs:170-178`)
- Results read "{current} of {total} matches" or "Not found." (`toolbar.cljs:277-287`, `en.edn:1194`, `en.edn:1204`)

**Keyboard**:

- **Enter** — Next match
- **Shift+Enter** — Previous match
- **Esc** — Clear the query; press again on an empty box to close (`toolbar.cljs:242-247`)

---

## Zoom Controls

### Zoom Levels

**Toolbar Buttons**:

- **Zoom out / Zoom in** — multiply or divide the scale by 1.05, then round to one decimal place, clamped to 0.25–5.0 (`utils.cljs:8-10`, `utils.cljs:66-84`). In practice each click moves about 0.1.
- **Auto fit** — fit to the container width

**Mouse**:

- **Ctrl+Wheel** / **Cmd+Wheel** — zoom around the pointer by a factor of 1.05 per wheel event (`core.cljs:648-678`). This path does not apply the 0.25–5.0 clamp.

### Fit Modes

- **Auto Fit** — the default scale when a PDF opens (`core.cljs:824-827`), re-applied on resize (`core.cljs:828-830`)
- **Custom Zoom** — any scale set with the buttons or the wheel

**Persistence**: Only the page is restored. The scale is written to browser storage under `pdf-last-visit-scale/<asset-db-id>` (`core.cljs:933-937`), but nothing reads that key back and the loader returns only `:page` (`assets.cljs:131`), so PDFs reopen at Auto fit (`core.cljs:960`).

### Viewer Settings

**More settings** also holds per-device toggles kept in browser storage (`toolbar.cljs:49-66`):

- Viewer theme: light, warm, dark (`toolbar.cljs:85-90`, `toolbar.cljs:488-494`)
- Dashed style for area highlight
- Coloured label for highlight block
- Auto open context menu for selections
- Document metadata dialog with **Copy all** (`toolbar.cljs:103-131`)

---

## Annotation Storage

### File-Based Graphs (Import Only)

File graphs cannot be opened (`src/main/frontend/components/repo.cljs:321-323`). There is no `hls__` page or `assets/<name>.edn` highlights file in a DB graph.

When a file graph is **imported**, its PDF annotations are converted:

- `assets/<pdf-name>.edn` supplies the highlights and `pages/hls__<pdf-name>.md` supplies block text and child blocks (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1691-1718`)
- Each highlight becomes a `Pdf-annotation` block under the PDF asset, keeping its id, colour, page and full map in `hl-value` (`exporter.cljs:1561-1597`). Only block text is imported from the `hls__` page, not tags or properties (`exporter.cljs:1609`)
- Images in `assets/<pdf-name>/` become asset blocks titled "pdf area highlight" (`exporter.cljs:1640-1660`)

The importer reads these keys of each highlight (`exporter.cljs:1561-1596`):

```clojure
{:highlights
 [{:id #uuid "highlight-uuid"
   :page 3
   :position {:bounding {...} :rects [...] :page 3}
   :content {:text "The highlighted text"}   ; area highlights also have :image
   :properties {:color "yellow"}}]}
```

### Database Graphs (Current)

**Asset Block** (`src/main/frontend/handler/editor.cljs:1827-1835`):

```clojure
{:block/uuid #uuid "pdf-asset-uuid"
 :block/title "doc"                                    ;; file name without extension
 :block/tags #{:logseq.class/Asset}
 :logseq.property.asset/type "pdf"
 :logseq.property.asset/size 123456
 :logseq.property.asset/checksum "…"
 :logseq.property.asset/external-url "https://example.com/doc.pdf" ;; only for external assets
 :logseq.property.asset/last-visit-page 5}
;; A local PDF's file is assets/<pdf-asset-uuid>.pdf
;; and is opened as "../assets/<uuid>.pdf" (assets.cljs:239)
```

`Asset` requires `type`, `size` and `checksum` (`deps/db/src/logseq/db/frontend/class.cljs:67-73`). Property schemas: `type` (`property.cljs:554-558`), `external-url` (`:559-563`), `external-file-name` (`:565-569`), `size` (`:570-574`), `checksum` (`:585-588`), `last-visit-page` (`:589-593`).

**Annotation Block** (`src/main/frontend/extensions/pdf/assets.cljs:92-110`):

```clojure
{:block/uuid #uuid "annotation-uuid"                     ;; = highlight :id
 :block/title "highlighted text content"                 ;; area: localised date
 :block/parent [:block/uuid #uuid "pdf-asset-uuid"]
 :block/tags #{:logseq.class/Pdf-annotation}
 :logseq.property/ls-type :annotation
 :logseq.property/asset [:block/uuid #uuid "pdf-asset-uuid"]
 :logseq.property.pdf/hl-color :logseq.property/color.yellow ;; ref to the closed value
 :logseq.property.pdf/hl-page 3
 :logseq.property.pdf/hl-value {:id #uuid "..." :page 3 :position {...}
                                :content {...} :properties {:color "yellow"}}
 ;; area highlights only:
 :logseq.property.pdf/hl-type :area
 :logseq.property.pdf/hl-image [:block/uuid #uuid "image-asset-uuid"]}
```

Property schemas (`deps/db/src/logseq/db/frontend/property.cljs`):

| Property                            | Type                             | Line      |
| ----------------------------------- | -------------------------------- | --------- |
| `:logseq.property/asset`            | `:entity`                        | 221-223   |
| `:logseq.property/ls-type`          | `:keyword`                       | 226-227   |
| `:logseq.property.pdf/hl-type`      | `:keyword`                       | 229-230   |
| `:logseq.property.pdf/hl-color`     | `:default` with 5 closed values  | 231-242   |
| `:logseq.property.pdf/hl-page`      | `:raw-number`                    | 243-244   |
| `:logseq.property.pdf/hl-image`     | `:entity`                        | 245-246   |
| `:logseq.property.pdf/hl-value`     | `:map`                           | 247-248   |

All are `:hide? true`, so they do not appear in the block's property list.

**Class Definition**: [`class.cljs:90-97`](../../../../../deps/db/src/logseq/db/frontend/class.cljs#L90-L97)

```clojure
:logseq.class/Pdf-annotation
{:title "PDF Annotation"
 :properties {:logseq.property.class/hide-from-node true}
 :schema {:properties [:logseq.property/ls-type :logseq.property.pdf/hl-color :logseq.property/asset
                        :logseq.property.pdf/hl-page :logseq.property.pdf/hl-value
                        :logseq.property.pdf/hl-type :logseq.property.pdf/hl-image]
          :required-properties [:logseq.property/ls-type :logseq.property.pdf/hl-color :logseq.property/asset
                                :logseq.property.pdf/hl-page :logseq.property.pdf/hl-value]}}
```

---

## Querying Annotations

`{{query …}}` does not run in a DB graph; it renders a deprecation warning (`src/main/frontend/components/block.cljs:1887-1888`). Use the advanced queries below: type `/Advanced Query`, which creates a block tagged `#Query` with a Clojure code property (`src/main/frontend/commands.cljs:125-131`, `commands.cljs:326`), and paste the map. A query that reads as an EDN map runs as an advanced query (`block.cljs:4139-4152`).

**All PDF Annotations**:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/tags ?t]
         [?t :db/ident :logseq.class/Pdf-annotation]]}
```

**Annotations from a Specific PDF** (asset title = file name without extension, `editor.cljs:1798-1802`):

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property/asset ?a]
         [?a :block/title "document"]]}
```

**Highlights by Colour**:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property.pdf/hl-color ?c]
         [?c :db/ident :logseq.property/color.red]]}
```

**Annotations on a Specific Page** (`hl-page` is a raw number, not a ref):

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property/ls-type :annotation]
         [?b :logseq.property.pdf/hl-page 5]]}
```

**Area Highlights Only**:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property.pdf/hl-type :area]]}
```

---

## Advanced Features

### External Window

**Open PDF in Separate Window**:

1. Click **Open in external window** in the viewer toolbar (`toolbar.cljs:589-599`)
2. The viewer moves into a new window opened with `window.open("about:blank")`, 700×800 by default (`src/main/frontend/extensions/pdf/windows.cljs:75-122`)
3. In that window the button reads **Open in app window**; it and **Close** return the viewer (`toolbar.cljs:590-599`, `toolbar.cljs:625-628`)

The window's size is remembered (`windows.cljs:80-85`, `windows.cljs:105-109`). On desktop, Electron allows `about:blank` pop-ups as framed windows (`src/electron/electron/window.cljs:186-201`).

**Benefits**:

- Multi-monitor setups
- Keep the PDF visible while editing the graph

> **Unverified:** Behaviour of the external window in the web build (browser pop-up blocking) was not checked.

**Code Reference**: [`windows.cljs:75-122`](../../../../../src/main/frontend/extensions/pdf/windows.cljs#L75-L122)

### Copy Block Reference

**From a Highlight**:

1. Click the highlight
2. Select **Copy ref** (a new highlight copies its ref automatically)
3. Paste elsewhere: `[[<uuid>]]` (`src/main/frontend/util/ref.cljs:5-7`)

**Use Case**: Link to specific highlights from other notes

**Example**:

```markdown
- According to the paper [[6650c1e2-3b7a-4c2e-9f5a-1d2e3f4a5b6c]], the results show...
```

`((uuid))` is file-graph syntax and renders as literal text in a DB graph (`block.cljs:1573-1574`).

### Goto Block Reference

**From the Annotation Block**:

- Click the `P<page>` prefix → the PDF opens at the highlight

**From the PDF**:

- Click the highlight → **Linked references** → jumps to the annotation block

### Plugin Context-Menu Items

Plugins can add items to the text-highlight context menu with `registerHighlightContextMenuItem` (`libs/src/LSPlugin.user.ts:619`, type `highlight-context-menu-item` at `:301`). The host renders them for text highlights when plugins are enabled (`core.cljs:238-249`).

---

## Platform Compatibility

### ✅ Desktop (Electron)

The viewer is mounted in the app container (`theme.cljs:159`), asset links resolve the file URL with `assets-handler/<make-asset-url` (`block.cljs:569-576`), and the external window is allowed by Electron's window-open handler (`window.cljs:186-201`).

**Features**:

- ✅ PDF viewing, text and area highlights
- ✅ Find, outline, highlights list
- ✅ External window
- ✅ Local and external PDFs
- ✅ Keyboard shortcuts

### ⚠️ Web Browser

The web build uses the same container, so the viewer is mounted, and asset links use the same URL resolution (`block.cljs:569-576`). If opening fails, Logseq falls back to a `showOpenFilePicker` file picker and writes the chosen PDF into the graph (`block.cljs:541-555`).

> **Unverified:** Highlight reliability, pop-up blocking of the external window and CORS behaviour for external PDFs in browsers were not checked.

### ❌ Mobile (iOS/Android)

**Status**: The native mobile app shell does not mount the viewer.

**Code Evidence**:

- The only mount of `pdf/default-embed-playground` is `src/main/frontend/components/theme.cljs:159`, reached through `theme/container` in `src/main/frontend/components/container.cljs:502`. The portal target `#app-single-container` is also rendered there (`container.cljs:575`, `core.cljs:1113`)
- The mobile app root `src/main/mobile/components/app.cljs:1-34` requires neither `frontend.components.container` nor any `frontend.extensions.pdf` namespace, and `git grep` finds no reference to them anywhere under `src/main/mobile/`
- Shared block rendering still renders a PDF asset as a PDF link and sets `:pdf/current` when clicked (`block.cljs:603-611`, `block.cljs:531-539`), but nothing on mobile renders that state

> **Unverified:** What a tap on a PDF asset does on a device was not reproduced.

**Workaround**: Use the desktop app

**Investigation Report**: See [PDF Platform Compatibility Investigation](../../../07-investigations/pdf-feature-platform-compatibility-investigation.md) (predates this audit)

---

## Troubleshooting

### PDF Won't Open

**Problem**: Clicking a PDF does nothing or shows an error

**Errors the loader reports** (`core.cljs:996-1034`):

| Error                      | Message                                                          |
| -------------------------- | ---------------------------------------------------------------- |
| `MissingPDFException`      | "Is this the correct path?" (`en.edn:1201`)                      |
| `InvalidPDFException`      | "Is this .pdf file corrupted? Please confirm with external pdf viewer." (`en.edn:1191`) |
| `PasswordException`        | Password prompt (`core.cljs:897-917`)                            |
| anything else              | "Please confirm with pdf file resource." (`en.edn:1195`)         |

**Solutions**:

- **Mobile**: Use desktop; the viewer is not mounted on mobile
- Check the file exists at `assets/<asset-uuid>.pdf`
- Test the PDF in an external viewer
- Check the developer console (the loader logs `[PDF loader]`, `core.cljs:999`)

### Annotations Not Saving

**Problem**: Picking a colour does not create an annotation block

**Causes found in source**:

1. The PDF has no asset block (opened from a link) → the **Create asset** dialog opens instead (`core.cljs:185-186`)
2. The colour's closed value could not be resolved → no block is inserted (`assets.cljs:90-91`)

**Also note**: Colour changes and area resizes are not written to `hl-value` (see [Update Highlight Colour](#update-highlight-colour)), so the PDF overlay can differ from the block after reopening.

> **Unverified:** Other failure modes (write errors, read-only graphs, sync) were not traced.

### Search Not Finding Text

**Problem**: Search returns no results for text visible in the PDF

Find uses PDF.js's find controller over the PDF's text (`core.cljs:809-811`). A scanned PDF with no text layer has nothing to match.

**Solutions**:

- Test copy-paste from the PDF (if you can't select text, there is no text layer)
- Run the PDF through an external OCR tool, then upload the searchable copy

### Slow Performance

> **Unverified:** No PDF-specific performance handling was found beyond PDF.js rendering. General advice: prefer local assets to external URLs, and split very large PDFs.

---

## Best Practices

### Organization

**All PDFs in one place**: The built-in `Asset` class uses a gallery view (`deps/db/src/logseq/db/frontend/class.cljs:71`). Open the **Asset** tag page to browse every asset.

**Tag PDFs by Topic**: Add your own tags to the PDF's asset block, for example `#research`, then query or browse by tag.

### Annotation Workflow

**1. First Pass — Yellow Highlights**

- Highlight interesting sections
- No notes yet, just mark passages

**2. Second Pass — Add Notes**

- Open **Annotations page** from the toolbar
- Add your thoughts as child blocks under an annotation block:
  ```txt
  - "The key insight is..."          ← annotation block (P3)
    - My note: This relates to [[Other Concept]]
  ```

**3. Synthesize — Colour-Code** (your convention)

- Red: Critical points
- Green: Definitions
- Blue: Citations to follow up
- Purple: Questions/unclear

**4. Create Summary**

- Aggregate annotations on a summary page
- Link to highlights with `[[<uuid>]]` (use **Copy ref**)

### Backup

**Database graphs**: Use **Export both SQLite DB and assets** (`src/resources/dicts/en.edn:666`), which writes a `.zip` of the database and the `assets/` folder. The PDFs, the area-highlight PNGs and the annotation blocks are all included. **Export SQLite DB** (`en.edn:661`) is described as backing up "to a single .sqlite file" (`en.edn:678`), while the zip covers "content and assets" (`en.edn:680`).

---

## Code Reference

**Core Implementation**: [`src/main/frontend/extensions/pdf/`](../../../../../src/main/frontend/extensions/pdf/)

**Key Files**:

- `core.cljs` (1118 lines) — PDF viewer, highlight management
- `assets.cljs` (334 lines) — Asset inflation, annotation persistence
- `toolbar.cljs` (645 lines) — Toolbar UI, outline view, search finder
- `utils.cljs` (200 lines) — Zoom, scroll, coordinate conversion
- `utils.js` (395 lines) — PDF.js integration utilities (rect math, scroll-to-highlight)
- `finder.js` (1066 lines) — PDF.js find controller fork (text search engine)
- `windows.cljs` (122 lines) — External window management

**Key Functions**:

- `pdf-viewer` (core.cljs:791) — Main PDF viewer component
- `pdf-highlights` (core.cljs:557) — Highlight rendering and management
- `add-hl!` (core.cljs:577) — Create new highlight (local fn inside `pdf-highlights`)
- `pdf-highlights-ctx-menu` (core.cljs:126) — Colour picker / highlight context menu
- `default-embed-playground` (core.cljs:1100) — Mounts the viewer into `#app-single-container`
- `inflate-asset` (assets.cljs:34) — Convert PDF path to the `:pdf/current` map
- `db-based-ensure-ref-block!` (assets.cljs:78) — Create the annotation block
- `ensure-ref-block!` (assets.cljs:112) — Calls `db-based-ensure-ref-block!`, then moves an area image asset under the annotation block
- `db-based-load-hls-data$` (assets.cljs:122) — Load highlights from annotation blocks
- `open-block-ref!` (assets.cljs:253) — Open the PDF at a highlight from its block
- `open-pdf-file` (block.cljs:524) — Open a PDF from a rendered link or asset

---

## Related Documentation

**How-To Guides**:

- [Asset Management](../../how-to/multimedia/asset-management.md) — Managing PDF files and annotations

**References**:

- [Database Schema — Block Classes](../../reference/database/schema-overview.md#block-classes) — includes `Pdf-annotation`
- [Database Schema — Asset Properties](../../reference/database/schema-overview.md#asset-properties)

**Investigations**:

- [PDF Platform Compatibility Report](../../../07-investigations/pdf-feature-platform-compatibility-investigation.md) — earlier technical investigation, not re-audited here

---

**Last Updated**: September 2026
**Status**: 🟠 Desktop and web app shell (not mounted by the native mobile app)
**Feedback**: Report PDF issues on [GitHub](https://github.com/logseq/logseq/issues)

---

⚠️ **Mobile Users**: The PDF viewer is not mounted in the native mobile app. Use the desktop app for PDF annotation workflows.
