---
title: 'Set Image and Asset Width'
description: 'How to resize one image, set an exact pixel width, and change the default width for every image in a DB graph'
status: draft
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: authored-from-source
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  howTo:
    - '[Asset Management](./asset-management.md)'
    - '[Custom CSS](../integrations/custom-css.md)'
  reference:
    - '[Properties Reference](../../reference/data/properties-reference.md)'
---

# Set Image and Asset Width

> **Goal**: control how wide images display — one image, or every image in the graph.

## Overview

An image in a DB graph has no width setting. What it has is:

| Property | Holds | Written by |
| --- | --- | --- |
| `:logseq.property.asset/width`, `:logseq.property.asset/height` | the file's own pixel size | measured once on first render |
| `:logseq.property.asset/resize-metadata` | `{:width N}`, the display width | the resize drag |

Both are hidden and non-public
([`deps/db/src/logseq/db/frontend/property.cljs:575-584`](../../../../../deps/db/src/logseq/db/frontend/property.cljs),
[`:601-604`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)), so neither shows
up in the property picker. An image that has never been resized displays at a hardcoded
**250 px** ([`src/main/frontend/components/block.cljs:1174-1185`](../../../../../src/main/frontend/components/block.cljs)).

That leaves four routes, in rough order of how often you want them.

## Steps

### 1. Resize one image by dragging

Hover the image and drag the grip that appears at either edge — a 6 px bar, vertically
centred ([`src/main/frontend/components/block.css:2002-2012`](../../../../../src/main/frontend/components/block.css)).

What happens as you drag
([`src/main/frontend/components/block.cljs:440-469`](../../../../../src/main/frontend/components/block.cljs)):

- The drag starts from the frame's current width, so it begins at the size you see.
- It will not shrink below 60 px.
- On release, the width is saved unrounded — expect values like `{:width 779.630493164}`,
  since pointer deltas are fractional on HiDPI screens.

Two behaviours to know before you drag:

> ⚠️ **The width belongs to the asset, not to the place you dragged it.** The handler writes
> to the asset entity whenever there is one
> ([`src/main/frontend/handler/editor.cljs:2252-2257`](../../../../../src/main/frontend/handler/editor.cljs)),
> so resizing an image in one block changes it everywhere that asset appears.

> ⚠️ **Dragging is unavailable** in the native mobile app, in breadcrumbs, for images in a
> property position, in table view, and wherever the renderer sets `:disable-resize?`
> ([`src/main/frontend/components/block.cljs:421-433`](../../../../../src/main/frontend/components/block.cljs)).
> On mobile, use the CSS route in step 3 instead.

### 2. Set an exact pixel width

Dragging cannot hit a round number. To set one, write the property directly with the CLI:

```bash
logseq query --graph my-graph --query '[:find (pull ?a [:db/id :block/uuid :block/title :logseq.property.asset/type])
 :where [?a :block/tags :logseq.class/Asset]]'

logseq upsert block --graph my-graph --id 276 \
  --update-properties '{:logseq.property.asset/resize-metadata {:width 400}}'
```

The first command finds the asset's entity id; the second sets the display width. The write
succeeds even though the property is hidden and non-public, and the map keeps its shape —
verified by reading it back (see [Verification](#verification)).

`logseq upsert block` prints `{:result nil}` on success, which is not much of a receipt.
Query the property back if you want confirmation.

The plugin API is the other candidate for this — it passes fully qualified property names
through unchanged ([`src/main/logseq/api/block.cljs:44-57`](../../../../../src/main/logseq/api/block.cljs)) —
but a plugin write to this property has not been tested.

### 3. Change the default width for every un-resized image

There is no config key for this, and nothing in `config.edn` is read while sizing an image.
The practical route is a rule in `custom.css` that targets exactly the images which have never
been resized — they all carry the attribute `width="250"`, while dragged widths are fractional:

```css
.asset-container img[width='250'] {
  width: 666px;
  max-width: 100%;
  height: auto;
}
```

Full recipe, including the companion rule that stops the loading placeholder from shifting the
layout, and the measurements behind it: **[Set a Default Image Size with CSS](./default-image-size.md)**.

### 4. Cap every image, resized or not

```css
.asset-container > img {
  max-width: 500px;
  height: auto;
}
```

`max-width` overrides both the attribute and any dragged width, so use this when you want a
ceiling rather than a default.

## Variations

### An external image written as Markdown

For an image that is a plain link rather than an asset node, the inline metadata suffix sets
its size:

```markdown
![A diagram](https://example.com/diagram.png){:width 600}
```

The parser keeps that suffix as the link's `metadata`, which the renderer reads with
`safe-read-map-string` and merges into the `<img>` attributes
([`src/main/frontend/components/block.cljs:652-656`](../../../../../src/main/frontend/components/block.cljs)).
If the target is an asset that also has `resize-metadata`, the stored width wins — the merge
puts it last ([`src/main/frontend/components/block/image.cljs:22-29`](../../../../../src/main/frontend/components/block/image.cljs)).

### Alignment

The `⋮` button on an image offers **Align left / center / right**, stored as
`:logseq.property.asset/align` and applied as a class on the resize frame
([`src/main/frontend/components/block.cljs:342-381`](../../../../../src/main/frontend/components/block.cljs),
[`block.css:1986-2000`](../../../../../src/main/frontend/components/block.css)).

## Where your width will be ignored

Several contexts cap images regardless of what you set:

| Context | Cap |
| --- | --- |
| Image in a property value | `max-width: 35%` ([`block.css:2019-2022`](../../../../../src/main/frontend/components/block.css)) |
| Plain image link in a property value | `max-width: 80px` ([`block.css:2024-2027`](../../../../../src/main/frontend/components/block.css)) |
| Table view | `max-width: 80px`, `max-height: 200px` ([`table.css:6-8`](../../../../../src/main/frontend/components/table.css)) |
| Breadcrumb | `height: 18px; width: unset !important` ([`block.css:118-124`](../../../../../src/main/frontend/components/block.css)) |

## Verification

Run on the `cliworker` graph on 2026-09-18, CLI revision `6bf8fe7-dirty`, against source at
`5824434a74`:

- **Writing the width externally works.** `logseq upsert block --id 276 --update-properties
  '{:logseq.property.asset/resize-metadata {:width 400}}'` was accepted and read back as
  `:logseq.property.asset/resize-metadata {:width 400}`. The hidden, non-public property took
  an external write and kept the map shape.
- **Drag widths really are fractional.** Asset 230 in that graph carries
  `{:width 779.630493164}` from an earlier drag, next to its intrinsic `1484 × 798`.
- **The Markdown metadata suffix parses in a DB graph.** `![probe](https://example.com/p.png){:width 321}`
  was run through mldoc with the DB-graph options from
  [`deps/graph-parser/src/logseq/graph_parser/mldoc.cljc:344-355`](../../../../../deps/graph-parser/src/logseq/graph_parser/mldoc.cljc)
  and produced `"metadata": "{:width 321}"` on the link node. The rendering path from there is
  source-read, not observed in the app.

The CSS rules on this page have not been tried in a running app; confirm them in DevTools.

## Related Tasks

- [Asset Management](./asset-management.md) — adding, storing, finding and deleting assets
- [Custom CSS](../integrations/custom-css.md) — where `custom.css` lives

## See Also

- **Investigation**: [Default Image Size](../../../07-investigations/028-default-image-size.md)
- **Proposal**: [Default Image Width in config.edn](../../../10-agent-guide/proposed/2026-09-10-config-default-image-width.md)

---

_Last updated: 2026-09-18 | [Report an issue](https://github.com/logseq/logseq/issues)_
