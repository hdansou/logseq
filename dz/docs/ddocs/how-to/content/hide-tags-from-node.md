---
title: 'Hide Tags from a Node'
description: 'How to keep a tag attached to DB nodes without showing it in the node tag list'
status: draft
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  reference:
    - '[Properties Reference](../../reference/data/properties-reference.md)'
    - '[Schema Overview](../../reference/database/schema-overview.md)'
  explanation:
    - '[Graph Visualization](../../explanation/concepts/graph-visualization.md)'
---

> Goal: hide a tag from a block's tag chips in a DB graph without removing the tag itself.

## Overview

Use the built-in tag property `Hide from Node` when a tag should still organize content, drive queries, or provide class behavior, but should not appear as a tag chip on each tagged block.

This is a tag property, not a property you add to each individual node. Its schema limits it to tags ([`deps/db/src/logseq/db/frontend/property.cljs:193-196`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)), and the property picker only offers it on tag pages ([`src/main/frontend/worker/handler/property.cljs:315-318`](../../../../../src/main/frontend/worker/handler/property.cljs)).

Several built-in tags already have it turned on: `Comments`, `Comment`, `Asset`, `Code`, `Quote`, `Math` and `PDF Annotation` ([`deps/db/src/logseq/db/frontend/class.cljs:44-93`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)).

## Before You Start

- You are using a DB graph.
- The tag already exists, or you are about to create it.
- You can open and edit the tag page.

## Steps

### 1. Open the tag page

Open the page for the tag you want to hide from nodes.

Examples:

- `#internal`
- `#asset-library`
- `#workflow/system`

You are changing the tag itself, so make the change on the tag page, not on an individual note or block.

### 2. Enable `Hide from Node`

Add the built-in property `Hide from Node` to the tag page and check it. It is a checkbox property ([`deps/db/src/logseq/db/frontend/property.cljs:193-196`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

Add it as a property of the tag itself, not under `Tag Properties`. `Tag Properties` are passed on to tagged nodes, and `Hide from Node` is excluded from that list ([`src/main/frontend/handler/property.cljs:40-47`](../../../../../src/main/frontend/handler/property.cljs)).

In the database schema, this property is stored as `:logseq.property.class/hide-from-node`.

### 3. Use the tag normally

Keep applying the tag to pages or blocks the same way you normally would.

Once the tag has `Hide from Node` checked, blocks still carry the tag, but Logseq stops showing it as a tag chip on the right side of the block.

### 4. Confirm the result

Open a block that has the tag.

Expected result:

- The tag is still attached to the block.
- The block no longer shows that tag as a chip.
- Queries and tag-based logic can still use the tag.

## Example

Suppose you use `#internal` to mark notes for automation or internal workflows, but you do not want that tag to clutter every block.

1. Open the `#internal` tag page.
2. Enable `Hide from Node`.
3. Keep tagging blocks with `#internal`.

Result: the `#internal` tag still works as a tag, but it no longer appears as a chip on each tagged block.

## What This Does Not Do

`Hide from Node` changes one display only: the tag chips at the right of a block. The tag chip list skips tags that have it checked ([`src/main/frontend/components/block.cljs:2876-2889`](../../../../../src/main/frontend/components/block.cljs)). That list is the only UI that checks this property. The only other code that reads it is the breadcrumb data loader, which copies the value ([`src/main/frontend/worker/handler/block_breadcrumb.cljs:34`](../../../../../src/main/frontend/worker/handler/block_breadcrumb.cljs)).

It does not:

- delete the tag from existing blocks or pages
- stop queries from matching that tag
- prevent class inheritance or other tag-driven behavior
- remove tag text written inline in the block. Inline tags are never shown as chips anyway, because the chip list already skips any tag written in the block's text ([`deps/db/src/logseq/db/frontend/db.cljs:108-111`](../../../../../deps/db/src/logseq/db/frontend/db.cljs)).
- hide the tag in a page's `Tags` property. That value list hides only the built-in `Page`, `Root` and `Asset` tags ([`src/main/frontend/components/property/value.cljs:2181-2183`](../../../../../src/main/frontend/components/property/value.cljs), [`deps/db/src/logseq/db/frontend/class.cljs:148-150`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)).

Tag chips are also never shown in table views, gallery views, block references or property values ([`src/main/frontend/components/block.cljs:3677-3680`](../../../../../src/main/frontend/components/block.cljs)), so this setting makes no difference there.

If you need to remove the tag entirely, remove the tag from the node instead of using `Hide from Node`.

## When to Use It

`Hide from Node` is useful when a tag is mainly structural.

Common cases:

- internal workflow tags
- taxonomy or implementation-detail tags
- tags that add properties or behavior but should stay out of the reading UI
- utility tags that would otherwise create visual noise

## Troubleshooting

### The tag is still visible

Check that you set `Hide from Node` on the tag page itself, not on the page or block that uses the tag.

If the tag appears in a page's `Tags` property rather than as a block chip, that is expected. `Hide from Node` does not apply there.

### The tag still appears in the block text

`Hide from Node` affects the block's tag chips. It does not rewrite the block's content.

### Task and Journal chips are missing on mobile

That is separate from `Hide from Node`. On mobile, the chip list always skips the `Task` and `Journal` tags ([`src/main/frontend/components/block.cljs:2889`](../../../../../src/main/frontend/components/block.cljs)).

### I want this to affect every use of the tag

That is exactly what this property is for. Because it is a tag property, you set it once on the tag, and the chip is hidden on every block that uses the tag.

## See Also

- **Reference**: [Properties Reference](../../reference/data/properties-reference.md)
- **Concept**: [Graph Visualization](../../explanation/concepts/graph-visualization.md)
