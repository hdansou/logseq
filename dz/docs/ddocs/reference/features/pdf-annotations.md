---
title: 'PDF Annotations'
status: '🟠 Desktop Only'
version: 'Since v0.5.0'
quadrant: 'Reference'
platform: 'Desktop (macOS/Windows/Linux) — ⚠️ Not available on mobile'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
related-docs:
  - '../../how-to/multimedia/asset-management.md'
  - '../../reference/schema/built-in-classes.md#logseqclasspdf-annotation'
  - '../../tutorials/getting-started/basic-editing.md'
---

> **Last revised:** 2026-09-09

# PDF Annotations Reference

## Overview

Logseq includes a full-featured PDF viewer with annotation capabilities built on PDF.js. You can highlight text, capture screenshots of regions, search within PDFs, and navigate pages—all without leaving the app. Annotations are stored as blocks in your graph for easy reference and linking.

**Key Features**:

- 📄 PDF viewing with pagination
- ✏️ Text highlighting (5 colors)
- 📷 Area highlighting with screenshots
- 🔍 Full-text search within PDFs
- 🔗 Annotations linked to blocks
- 📑 Table of contents navigation
- 🔍 Zoom controls
- ⌨️ Keyboard shortcuts

⚠️ **Platform Limitation**: PDF viewing currently works only on **Desktop (macOS/Windows/Linux)** and partially on **Web browsers**. **Mobile apps (iOS/Android) cannot open PDFs** (known issue).

## Quick Start

### Open a PDF

**Method 1: Drag & Drop**

1. Drag PDF file from file manager
2. Drop onto any block
3. PDF uploaded to `assets/` folder
4. Link inserted: `![Document](../assets/doc.pdf)`
5. Click link → PDF opens in viewer

**Method 2: File Picker**

1. Type `/Upload a file`
2. Select PDF from dialog
3. Click link to open

**Method 3: External Link**

```markdown
![Research Paper](https://arxiv.org/pdf/2301.12345.pdf)
```

**Result**: PDF opens in full-screen viewer with toolbar

---

## Viewer Interface

### Layout

```txt
┌────────────────────────────────────────────────────────────┐
│  [⬅] [➡] Page 3/25 [🔍-] [🔍+] [🔍Fit] [📑] [🖍Yellow] [✕]│ Toolbar
├────────────────────────────────────────────────────────────┤
│                                                             │
│                     PDF Content                             │
│                   (Page rendered)                           │
│                                                             │
│  Selected text appears here with highlight menu            │
│  [ Yellow ] [ Red ] [ Green ] [ Blue ] [ Purple ]          │
│                                                             │
└────────────────────────────────────────────────────────────┘
```

### Toolbar Controls

| Icon     | Function                    | Shortcut     |
| -------- | --------------------------- | ------------ |
| ⬅        | Previous page               | Alt+P        |
| ➡        | Next page                   | Alt+N        |
| Page X/Y | Jump to page (type number)  | —            |
| 🔍-      | Zoom out                    | —            |
| 🔍+      | Zoom in                     | —            |
| 🔍Fit    | Auto fit                    | —            |
| 📑       | Outline / Highlights panel  | —            |
| 🖍        | Highlight mode toggle       | —            |
| 🔲       | Area selection mode         | Cmd or Shift |
| 🔍       | Find / Search               | Alt+F        |
| ✕        | Close PDF viewer            | Alt+X        |

Bindings are registered as the `:pdf/*` shortcut namespace: `:pdf/previous-page` `alt+p`, `:pdf/next-page` `alt+n`, `:pdf/close` `alt+x`, `:pdf/find` `alt+f` (`src/main/frontend/modules/shortcut/config.cljs:54-64`).

---

## Text Highlighting

### Create Highlight

1. Open PDF
2. Select text (click & drag with mouse)
3. Context menu appears with color options:
   ```txt
   [ Yellow ] [ Red ] [ Green ] [ Blue ] [ Purple ]
   ```
4. Click a color
5. Annotation block created automatically

**Result**:

- Text highlighted in PDF
- Block created in graph:
  ```markdown
  - Selected text content #PDF-Annotation
    id:: block-uuid
  ```

### Highlight Colors

| Color     | Use Case                   | DB Ident (closed-value)         | Display Value |
| --------- | -------------------------- | ------------------------------- | ------------- |
| 🟡 Yellow | General highlights         | `:logseq.property/color.yellow` | `"yellow"`    |
| 🔴 Red    | Important / Critical       | `:logseq.property/color.red`    | `"red"`       |
| 🟢 Green  | Definitions / Key concepts | `:logseq.property/color.green`  | `"green"`     |
| 🔵 Blue   | References / Citations     | `:logseq.property/color.blue`   | `"blue"`      |
| 🟣 Purple | Questions / To review      | `:logseq.property/color.purple` | `"purple"`    |

**Tip**: Use color consistently for categorization (e.g., red = action items, blue = quotes).

### Navigate to Highlight

**From PDF Viewer**:

- Click highlighted text → Jumps to annotation block
- Or right-click → "Copy block reference"

**From Graph**:

- Click block reference `((block-uuid))` → PDF opens to highlight

### Update Highlight Color

1. Select existing highlight
2. Click different color in menu
3. Color updates immediately

### Delete Highlight

**Method 1: From PDF**

1. Right-click highlighted text
2. Select "Delete highlight"

**Method 2: From Block**

1. Delete annotation block
2. Highlight removed from PDF

---

## Area Highlighting (Screenshots)

### Create Area Highlight

1. Open PDF
2. Enable area selection mode (🔲 button in toolbar)
3. Click and drag to select rectangular region
4. Context menu appears with color options
5. Click a color
6. Screenshot captured and saved

**Result**:

- Screenshot saved as PNG image:
  - **File-based**: `assets/{pdf-name}/{page}_{id}_{timestamp}.png`
  - **Database**: Asset block with UUID reference
- Block created with image embed:
  ```markdown
  - ![](../assets/doc/3_abc123_1705852800.png) #PDF-Annotation
    id:: block-uuid
  ```

### Use Cases

- Capture diagrams from research papers
- Screenshot tables and charts
- Save equations without OCR errors
- Archive visual content

### Edit Area Image

Once saved, the screenshot is a standard image asset. You can:

- Open in external editor (right-click → Open in external app)
- Add annotations (use image editing tools)
- Crop/resize
- Upload modified version

---

## PDF Navigation

### Page Navigation

**Toolbar**:

- ⬅ **Previous** — Go to previous page (Alt+P)
- ➡ **Next** — Go to next page (Alt+N)
- **Page input** — Type page number, press Enter

**Keyboard**:

- **Alt+P** — Previous page
- **Alt+N** — Next page
- **Home** — First page
- **End** — Last page
- **Page Up** — Scroll up
- **Page Down** — Scroll down

**Mouse**:

- **Scroll wheel** — Scroll through pages
- **Click page thumbnail** (if outline open) — Jump to page

### Table of Contents

**Open Outline**:

1. Click 📑 button in toolbar
2. Sidebar opens with PDF structure

**Navigate**:

- Click section title → Jumps to page
- Expand/collapse nested sections

**Example Outline**:

```txt
📑 Outline
  ├─ Abstract
  ├─ 1. Introduction
  ├─ 2. Related Work
  │   ├─ 2.1 Background
  │   └─ 2.2 Prior Art
  ├─ 3. Methodology
  └─ References
```

**Note**: Outline only available if PDF has embedded table of contents (many PDFs don't).

### Search Within PDF

**Open Search**:

- Click 🔍 in toolbar
- Or press **Alt+F**

**Search Box**:

```txt
┌───────────────────────────────────────┐
│  Search: [machine learning     ]  🔍 │
│  Match case [Aa]                      │
│  3 of 12 matches ("machine learning") │
│  [ ⬆ Previous ]  [ ⬇ Next ]          │
└───────────────────────────────────────┘
```

**Features**:

- Search triggered on Enter (not incremental — type query then press Enter)
- Case-sensitive toggle
- Navigate between matches (chevron up/down buttons)
- Highlights all occurrences across pages

**Keyboard**:

- **Enter** — Next match
- **Shift+Enter** — Previous match
- **Esc** — Close search

---

## Zoom Controls

### Zoom Levels

**Toolbar Buttons**:

- **🔍-** — Zoom out
- **🔍+** — Zoom in
- **🔍Fit** — Auto fit to container width

**Mouse**:

- **Cmd+Scroll** / **Ctrl+Scroll** — Zoom in/out (handled via wheel event in `pdf-highlights`)

**Zoom Range**: Continuous zoom from 25% to 500% (scale factor 1.05x per step)

### Fit Modes

- **Auto Fit** — Automatic scaling to fit the container (the "Auto fit" toolbar button sets `currentScaleValue` to `"auto"`)
- **Custom Zoom** — Manual zoom level via +/- buttons or Ctrl/Cmd+scroll

**Persistence**: Zoom level and last-visited page saved per PDF via the `:ls-update-extra-state` event.

---

## Annotation Storage

### File-Based Graphs (Legacy)

> **Removed:** The current codebase has removed file-based graph support for PDF annotations. `ensure-ref-block!` (`src/main/frontend/extensions/pdf/assets.cljs:112-114`) now delegates unconditionally to `db-based-ensure-ref-block!` (`assets.cljs:78`). The EDN format below is retained only as historical reference for older Logseq versions.

**Highlights File**: `assets/{filename}.edn`

**Format** (EDN):

```clojure
{:highlights
 [{:id #uuid "highlight-uuid"
   :page 3
   :position {:bounding {:x1 100 :y1 200 :x2 300 :y2 250
                         :width 200 :height 50}
              :rects [{:x1 100 :y1 200 :x2 300 :y2 225}]}
   :content {:text "The highlighted text"}
   :properties {:color "yellow"}}]

 :extra
 {:page 5       ; Last visited page
  :scale 1.25}} ; Last zoom level
```

**Annotation Blocks**: Created in `hls__{filename}` page

**Area Images**: `assets/{filename}/{page}_{id}_{timestamp}.png`

### Database Graphs (Current)

**Asset Block**:

```clojure
{:block/uuid #uuid "pdf-asset-uuid"
 :block/tags [:logseq.class/Asset]
 :logseq.property.asset/type "pdf"
 :logseq.property.asset/external-url "https://example.com/doc.pdf" ;; only for remote PDFs
 :logseq.property.asset/last-visit-page 5}
;; For local PDFs, the file is stored at assets/{uuid}.pdf
;; and referenced via "../assets/{uuid}.pdf"
```

**Annotation Block**:

```clojure
{:block/uuid #uuid "annotation-uuid"
 :block/title "highlighted text content"
 :block/tags [:logseq.class/Pdf-annotation]
 :logseq.property/asset [:block/uuid #uuid "pdf-asset-uuid"]
 :logseq.property.pdf/hl-color :logseq.property/color.yellow
 :logseq.property.pdf/hl-page 3
 :logseq.property.pdf/hl-value {;; full highlight data structure
   :id #uuid "..." :page 3 :position {...} :content {...}
   }
 :logseq.property.pdf/hl-type :area ; for area highlights
 :logseq.property.pdf/hl-image [:block/uuid #uuid "image-asset-uuid"]}
```

**Class Definition**: [`class.cljs:91-98`](../../../../../deps/db/src/logseq/db/frontend/class.cljs#L91-L98)

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

### Database Graphs Only

**All PDF Annotations**:

```clojure
{{query (property :block/tags :logseq.class/Pdf-annotation)}}
```

**Annotations from Specific PDF**:

```clojure
{{query (and (property :block/tags :logseq.class/Pdf-annotation)
             (property :logseq.property/asset [[document.pdf]]))}}
```

**Highlights by Color**:

```clojure
{{query (and (property :block/tags :logseq.class/Pdf-annotation)
             (property :logseq.property.pdf/hl-color :logseq.property/color.red))}}
```

**Annotations on Specific Page**:

```clojure
{{query (and (property :block/tags :logseq.class/Pdf-annotation)
             (property :logseq.property.pdf/hl-page 5))}}
```

**Area Highlights Only**:

```clojure
{{query (and (property :block/tags :logseq.class/Pdf-annotation)
             (property :logseq.property.pdf/hl-type :area))}}
```

---

## Advanced Features

### External Window (Desktop Only)

**Open PDF in Separate Window**:

1. Right-click PDF link
2. Select "Open in new window"
3. PDF opens in system window (outside Logseq)

**Benefits**:

- Multi-monitor setups
- Keep PDF visible while editing graph
- Separate zoom/navigation state

**Code Reference**: [`windows.cljs:74-122`](../../../../../src/main/frontend/extensions/pdf/windows.cljs#L74-L122)

### Copy Block Reference

**From Highlight**:

1. Right-click highlighted text
2. Select "Copy block reference"
3. Paste elsewhere: `((block-uuid))`

**Use Case**: Link to specific highlights from other notes

**Example**:

```markdown
- According to the paper ((highlight-uuid)), the results show...
```

### Goto Block Reference

**From Annotation Block**:

- Click block reference → PDF opens to highlight location

**From PDF**:

- Click highlight → Jumps to annotation block in graph

**Bidirectional Navigation**: Seamlessly move between PDF and graph.

---

## Platform Compatibility

### ✅ Desktop (Electron) — Full Support

**Features**:

- ✅ PDF viewing with all features
- ✅ Text & area highlighting
- ✅ Search & navigation
- ✅ External window support
- ✅ Local & remote PDFs
- ✅ Keyboard shortcuts

**Best Experience**: Use desktop app for PDF work.

### ⚠️ Web Browser — Partial Support

**Features**:

- ✅ PDF viewing
- ⚠️ Text highlighting (buggy)
- ⚠️ Area highlighting (buggy)
- ✅ Search & navigation
- ⚠️ External window (may be blocked)
- ✅ Remote PDFs
- ⚠️ Local PDFs (file:// protocol restricted)

**Known Issues**:

- Annotation block creation may fail
- CORS restrictions for some remote PDFs
- Browser security policies may interfere

### ❌ Mobile (iOS/Android) — Not Supported

**Status**: PDF viewer **does not work** on mobile native apps

**Issue**: Clicking PDF link does nothing (known bug)

**Platforms Affected**:

- iOS (iPhone)
- iPadOS (iPad)
- Android phones/tablets

**Workaround**: Use desktop or web version

**Code Evidence**: No mobile-specific checks in PDF extension:

- [`core.cljs`](../../../../../src/main/frontend/extensions/pdf/core.cljs) — Assumes full browser environment
- No fallback to native PDF viewer

**Investigation Report**: See [PDF Platform Compatibility Investigation](../../dz/docs/07-investigations/pdf-feature-platform-compatibility-investigation.md)

---

## Troubleshooting

### PDF Won't Open

**Problem**: Clicking PDF link does nothing

**Possible Causes**:

1. Mobile platform (known limitation)
2. File path incorrect
3. PDF corrupted
4. PDF.js library failed to load

**Solutions**:

- **Mobile**: Use desktop instead (mobile not supported)
- **Desktop/Web**: Check file exists in `assets/` folder
- Test PDF in external viewer (Adobe Acrobat, Preview, etc.)
- Check browser console for errors (Help → Toggle Developer Tools)

### Annotations Not Saving

**Problem**: Highlights disappear after closing PDF

**Possible Causes**:

1. Database write error
2. Network error (RTC sync)
3. Graph is in read-only state

**Solutions**:

- Check for error notifications
- Verify graph is writable (not read-only)
- Check disk space
- For RTC: check internet connection

### Search Not Finding Text

**Problem**: Search returns no results for text visible in PDF

**Possible Causes**:

1. PDF is scanned image without text layer (OCR needed)
2. Text is embedded as image
3. Non-standard encoding

**Solutions**:

- Test copy-paste from PDF (if can't copy, no text layer)
- Use external OCR tool (Adobe Acrobat, ABBYY) to create searchable PDF
- For images embedded as text: search won't work

### Slow Performance

**Problem**: PDF loading/navigation is slow

**Possible Causes**:

1. Large PDF (100+ pages)
2. High-resolution images in PDF
3. Many annotations
4. Remote PDF with slow connection

**Solutions**:

- Wait for initial load (PDF.js caches pages)
- Reduce zoom level
- Use local PDFs instead of remote
- Split large PDFs into smaller documents

---

## Best Practices

### Organization

**Dedicated PDF Library Page**:

```markdown
- [[📚 PDF Library]]
  - [[Research Papers]]
    - [[Paper 1]] — ![](../assets/paper1.pdf)
    - [[Paper 2]] — ![](../assets/paper2.pdf)
  - [[Books]]
  - [[Manuals]]
```

**Tag PDFs by Topic**:

```markdown
- [[Machine Learning Paper]] #research #ML #2024
  - ![PDF](../assets/ml-paper.pdf)
```

### Annotation Workflow

**1. First Pass — Yellow Highlights**

- Highlight interesting sections
- No notes yet, just mark passages

**2. Second Pass — Add Notes**

- Review yellow highlights
- Add your own thoughts as child blocks:
  ```markdown
  - "The key insight is..." #PDF-Annotation
    - **My note**: This relates to [[Other Concept]]
  ```

**3. Synthesize — Color-Code**

- Red: Critical points
- Green: Definitions
- Blue: Citations to follow up
- Purple: Questions/unclear

**4. Create Summary**

- Aggregate annotations into summary page
- Link to highlights: `((block-uuid))`

### Backup

**File-Based**: Commit `assets/` folder to git

```bash
git add assets/document.pdf assets/document.edn assets/document/
git commit -m "Add PDF annotations for research paper"
```

**Database**: Export graph regularly (includes PDFs and annotations)

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
- `inflate-asset` (assets.cljs:34) — Convert PDF path to asset object
- `db-based-ensure-ref-block!` (assets.cljs:78) — Create the annotation block
- `ensure-ref-block!` (assets.cljs:112) — Thin wrapper that delegates to `db-based-ensure-ref-block!`

---

## Related Documentation

**How-To Guides**:

- [Asset Management](../../how-to/multimedia/asset-management.md) — Managing PDF files and annotations

**References**:

- [Built-in Classes](../../reference/schema/built-in-classes.md#logseqclasspdf-annotation) — PDF annotation class schema
- [Asset Class](../../reference/schema/built-in-classes.md#logseqclassasset) — Asset class for PDFs

**Investigations**:

- [PDF Platform Compatibility Report](../../dz/docs/07-investigations/pdf-feature-platform-compatibility-investigation.md) — Detailed technical investigation (1405 lines)

---

**Last Updated**: September 2026
**Status**: 🟠 Desktop Only (Mobile support missing)  
**Feedback**: Report PDF issues on [GitHub](https://github.com/logseq/logseq/issues) with `pdf` label

---

⚠️ **Mobile Users**: PDF viewing is not available on mobile. We recommend using the desktop app or web browser for PDF annotation workflows. Mobile support is under investigation.
