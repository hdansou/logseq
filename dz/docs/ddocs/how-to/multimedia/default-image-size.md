---
title: 'Set a Default Image Size with CSS'
description: 'How to make every image you have not resized display wider than the built-in 250px, using one rule in custom.css'
status: draft
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: authored-from-source
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  howTo:
    - '[Set Image and Asset Width](./image-width.md)'
    - '[Custom CSS](../integrations/custom-css.md)'
    - '[Asset Management](./asset-management.md)'
---

# Set a Default Image Size with CSS

> **Goal**: make every image you have not resized display at a width you choose, instead of the built-in 250px, without resizing them one by one and without changing any data.

## Overview

An image you drop into a DB graph displays **250px wide**. That number is hardcoded, and no
setting changes it ([`src/main/frontend/components/block.cljs:1178`](../../../../../src/main/frontend/components/block.cljs)):

```clojure
(let [width (or width 250 asset-width)   ; width = the value a resize drag saved
```

`config.edn` has no key for it, and the renderer reads no config while sizing an image. Two
rules in `custom.css` are the practical fix. They change only how images *display*, so
nothing is written to your graph and nothing is lost if you remove them later.

This is also the **only** way to change image size in the native mobile app, where the resize
handles are disabled ([`block.cljs:421-433`](../../../../../src/main/frontend/components/block.cljs)).

## Before You Start

- You are using a DB graph.
- You know how to open `logseq/custom.css` — Settings → General → **Edit custom.css**. See
  [Custom CSS](../integrations/custom-css.md).
- Pick a number. The default content column is 960px wide, 1440px in wide mode
  ([`src/main/frontend/shui/vars-classic.css:6-7`](../../../../../src/main/frontend/shui/vars-classic.css)),
  so something in the 500–700px range fills the column without crowding it. The examples use
  666px.

## Steps

### 1. Add the rules

Open `logseq/custom.css` and paste:

```css
/* Default display width for images that have not been resized */
.asset-container img[width='250'] {
  width: 666px;
  max-width: 100%;
  height: auto;
}

/* Keep the loading placeholder the same size, so the layout does not jump */
.img-placeholder.asset-container[style] {
  width: 666px !important;
}
```

Change `666px` in both rules to the width you want. Leave the `250` alone — it is not your
width, it is the marker that identifies an un-resized image.

### 2. Save

Saving re-injects the stylesheet immediately; no reload
([`src/main/frontend/handler/db_based/editor.cljs:161-165`](../../../../../src/main/frontend/handler/db_based/editor.cljs)).
Images that you have never resized are now 666px wide. Images you resized keep their width.

### 3. Check it

Open a page with a mix of images. Expected:

- Images you never touched: 666px wide, correct aspect ratio.
- Images you dragged: unchanged.
- A narrow container (sidebar, a split pane): the image shrinks to fit rather than overflowing.

If nothing changed, open DevTools, inspect the image, and confirm it has `width="250"` as an
**attribute**. If it has some other number, that image has been resized at some point and the
rule is correctly leaving it alone.

## Why it works

Three facts about how Logseq renders an image make this rule reliable.

**The width is an HTML attribute, not an inline style.** The computed metadata is merged into
the `<img>` element's attributes
([`src/main/frontend/components/block/image.cljs:22-29`](../../../../../src/main/frontend/components/block/image.cljs),
[`block.cljs:258-270`](../../../../../src/main/frontend/components/block.cljs)), producing
`<img width="250" height="…">`. An attribute is a *presentational hint* with no specificity,
so any CSS rule at all outranks it. No `!important` needed on the image rule.

**Only un-resized images carry exactly 250.** A resize drag saves the frame's width plus the
pointer delta, unrounded, so saved widths are fractional in practice —
`{:width 779.630493164}` is a real value from a test graph
([`block.cljs:440-469`](../../../../../src/main/frontend/components/block.cljs)). The
attribute selector `[width='250']` matches the string exactly, so a dragged image does not
match and keeps its own size.

**The placeholder is different.** While the file loads, Logseq renders a sizing placeholder
whose width is an **inline style**, not an attribute
([`block.cljs:1186-1188`](../../../../../src/main/frontend/components/block.cljs)). Inline
styles beat plain CSS rules, which is why the second rule needs `!important`. Without it the
placeholder stays 250px and the page shifts when the image appears.

`height: auto` in the first rule is doing real work, not decoration. An un-resized image also
gets a `height` attribute, computed as `250 / aspect-ratio`
([`block.cljs:1179-1185`](../../../../../src/main/frontend/components/block.cljs)). Widening
the image without releasing that height squashes it — measured at 666×63 instead of 666×167
for a 4:1 image (see [Verification](#verification)).

## What this does not change

- **Images you have resized.** They keep their saved width, and dragging still works normally,
  starting from whatever width is on screen.
- **Your data.** No asset gains `resize-metadata`. Turn the rules off and every image is 250px
  again.
- **The per-context caps.** Images in a property value are capped at 35%, plain image links at
  80px, table view at 80×200, breadcrumbs at 18px tall. Those rules are more specific and still
  win. See [Set Image and Asset Width](./image-width.md#where-your-width-will-be-ignored).
- **Videos, PDFs and published pages.** Published pages are styled by `publish.css`, not
  `custom.css`.

## Variations

### A ceiling instead of a default

To leave the 250px default alone but stop large images from dominating the page:

```css
.asset-container > img {
  max-width: 500px;
  height: auto;
}
```

This one applies to resized images too, since it is a cap rather than a default.

### Full-width images

```css
.asset-container img[width='250'] {
  width: 100%;
  max-width: 100%;
  height: auto;
}
```

### A different width in wide mode

Wide mode puts `.ls-wide-mode` on the app's main container
([`src/main/frontend/components/container.cljs:520-525`](../../../../../src/main/frontend/components/container.cljs),
[`container.css:488-492`](../../../../../src/main/frontend/components/container.css)), so it
can scope a second width:

```css
.ls-wide-mode .asset-container img[width='250'] {
  width: 1000px;
}
```

### Undo

Delete both rules and save. Nothing else to clean up.

## Edge cases

- **An image whose width really is 250.** If you drag one to exactly 250px, or set
  `{:width 250}` by hand, the rule treats it as un-resized and overrides it. Use 251 if you
  need one image pinned at about that size.
- **Gallery view and other asset layouts** were not checked against these rules.
- **Setting a width larger than the content column** was not checked in the app; the
  `max-width: 100%` in the rule is there to contain it.

## Verification

The mechanism was tested on 2026-09-18 outside Logseq, in Chromium via Playwright, against a
DOM replica of what `asset-container` renders (`<div class="asset-container"><img width="…">`
plus an inline-styled `.img-placeholder`). Source read at `5824434a74`.

| Element | Rule applied | Computed width |
| --- | --- | --- |
| `img width="250"` (un-resized) | `width: 666px` | **666px** |
| `img width="779.630493164"` (dragged) | — does not match the selector | 779.6px |
| `img width="400"` (exact width set by hand) | — does not match | 400px |
| `.img-placeholder` with `style="width:250px"` | `width: 666px !important` | **666px** |

Aspect ratio, 4:1 image with `width="250" height="62.5"`:

| Rule | 2000px container | 300px container |
| --- | --- | --- |
| with `height: auto` | 666×167 | 300×75 |
| without `height: auto` | 666×63 (squashed) | 300×63 (squashed) |

What this does **not** prove: that the rules behave the same inside the running app, where
other stylesheets are present. Confirm in DevTools after pasting them. The hardcoded 250, the
attribute rendering, the placeholder's inline style and the fractional drag widths are all
read from the source lines cited above.

## Related Tasks

- [Set Image and Asset Width](./image-width.md) — resizing one image, exact widths, and the caps
- [Asset Management](./asset-management.md) — adding, finding and deleting assets
- [Custom CSS](../integrations/custom-css.md) — where `custom.css` lives

## See Also

- **Investigation**: [Default Image Size](../../../07-investigations/028-default-image-size.md)
- **Proposal**: [Default Image Width in config.edn](../../../10-agent-guide/proposed/2026-09-10-config-default-image-width.md) — the code change that would make this a real setting

---

_Last updated: 2026-09-18 | [Report an issue](https://github.com/logseq/logseq/issues)_
