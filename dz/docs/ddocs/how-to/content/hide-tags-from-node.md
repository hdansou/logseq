---
title: 'Hide Tags from a Node'
description: 'How to keep a tag attached to DB nodes without showing it in the node tag list'
status: draft
lastVerified: '2026-04-28'
logseqVersion: '0.11.x'
platforms: [desktop, web]
graphType: [db]
relatedDocs:
  reference:
    - '[Properties Reference](../../reference/data/properties-reference.md)'
    - '[Schema Overview](../../reference/database/schema-overview.md)'
  explanation:
    - '[Graph Visualization](../../explanation/concepts/graph-visualization.md)'
---

> Goal: hide a tag from a node's visible tag list in a DB graph without removing the tag itself.

## Overview

Use the built-in tag property `Hide from Node` when a tag should still organize content, drive queries, or provide class behavior, but should not appear as a visible tag chip on each tagged node.

This is a tag property, not a property you add to each individual node.

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

You are changing the tag itself, so make the change on the tag page or in the tag's property editor, not on an individual note or block.

### 2. Enable `Hide from Node`

Add the built-in property `Hide from Node` to that tag and turn it on.

In the database schema, this property is stored as `:logseq.property.class/hide-from-node`.

### 3. Use the tag normally

Keep applying the tag to pages or blocks the same way you normally would.

Once the tag has `Hide from Node` enabled, nodes can still carry that tag, but Logseq will stop showing that tag in the node tag list.

### 4. Confirm the result

Open a page or block that uses the tag.

Expected result:

- The tag is still attached to the node.
- The node no longer shows that tag in its visible tag chips/list.
- Queries and tag-based logic can still use the tag.

## Example

Suppose you use `#internal` to mark notes for automation or internal workflows, but you do not want that tag to clutter every node.

1. Open the `#internal` tag page.
2. Enable `Hide from Node`.
3. Keep tagging notes with `#internal`.

Result: the `#internal` tag still works as a tag, but it no longer appears in each node's visible tag list.

## What This Does Not Do

`Hide from Node` changes node display. It does not remove the tag from the underlying data.

It does not:

- delete the tag from existing blocks or pages
- stop queries from matching that tag
- prevent class inheritance or other tag-driven behavior
- remove literal tag text that is part of block content

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

### The tag still appears in the block text

`Hide from Node` affects the node's tag list. It does not rewrite the block's content.

### I want this to affect every use of the tag

That is exactly what this property is for. Because it is a tag property, you set it once on the tag, and the display behavior follows that tag wherever it is used.

## See Also

- **Reference**: [Properties Reference](../../reference/data/properties-reference.md)
- **Concept**: [Graph Visualization](../../explanation/concepts/graph-visualization.md)
