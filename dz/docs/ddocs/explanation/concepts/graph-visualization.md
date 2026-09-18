---
title: 'Graph Visualization and Navigation'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
quadrant: 'Explanation'
platform: 'Desktop'
related-docs:
  - '[Linked References](linked-references.md)'
  - '[Linking Pages Tutorial](../../tutorials/getting-started/04-linking-pages.md)'
tags: ['graph', 'visualization', 'navigation', 'connections', 'knowledge-graph']
---

# Graph Visualization and Navigation

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

This document explains Logseq's graph visualization system—how your notes form an interconnected knowledge graph, and how to navigate and understand these relationships. Master the graph view to discover insights and connections in your knowledge base.

> **Last revised:** 2026-09-11 (verified against `be800f1711`). Source links point at that revision.

**What You'll Learn:**

- Graph structure and representation
- Types of connections (links, tags, properties, hierarchy)
- Graph navigation techniques
- Page graph vs global graph views
- Graph-based discovery

**Prerequisites:**

- Basic understanding of pages and blocks
- Familiarity with page links `[[Page]]`

---

## Table of Contents

1. [Graph Concepts](#graph-concepts)
2. [Graph Structure](#graph-structure)
3. [Graph View Features](#graph-view-features)
4. [Navigation Patterns](#navigation-patterns)
5. [Graph Analysis](#graph-analysis)
6. [Best Practices](#best-practices)

---

## Graph Concepts

### What is a Knowledge Graph?

A **knowledge graph** represents your notes as:

- **Nodes** (vertices) = Pages, tags, and tagged nodes
- **Edges** (connections) = Links, tags, property values, hierarchy

```
        Page A
         / \
        /   \
    Page B—Page C
        \   /
         \ /
        Page D
```

**Example:**

```markdown
Page "Projects":

- [[Project Alpha]] #active
  - Owner: [[John]]
  - Related: [[Client X]]

Page "John":

- Role: Engineer
- Projects: [[Project Alpha]], [[Project Beta]]
```

**Graph (All pages view):**

```
        active (tag)
             │
Client X ─ Projects ─── Project Alpha
             │               │
            John ────────────┘
             │
        Project Beta
```

Every edge from a block is drawn between the block's page and the page it references ([`src/main/frontend/worker/graph_view.cljs:562-576`](../../../../../src/main/frontend/worker/graph_view.cljs)). The "Owner: [[John]]" block sits on the Projects page, so the edge runs from Projects to John, not from Project Alpha. A `#tag` on a block is one of those references, so Projects also connects to "active".

### Graph Types in Logseq

**1. Global Graph**

- Shows your whole graph, in one of two view modes: **Tags** or **All pages** ([`src/main/frontend/components/graph.cljs:313-338`](../../../../../src/main/frontend/components/graph.cljs); `en.edn:787-789`)
- Bird's-eye view of entire knowledge base
- Access: **Graph view** in the left sidebar, one of the navigation items shown by default ([`src/main/frontend/components/left_sidebar.cljs:313-320`](../../../../../src/main/frontend/components/left_sidebar.cljs), `:234-235`; `en.edn:1076`), or press `g g` ([`src/main/frontend/modules/shortcut/config.cljs:396-397`](../../../../../src/main/frontend/modules/shortcut/config.cljs)). Both open the `/graph` route, rendered by `global-graph` ([`src/main/frontend/routes.cljs:64-66`](../../../../../src/main/frontend/routes.cljs))

> **Removed:** `Cmd+Shift+G`. The graph view shortcut is `g g`; `g shift+g` is "Go to all graphs" ([`src/main/frontend/modules/shortcut/config.cljs:399`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

**2. Page Graph**

- Shows connections for one page
- Fixed at the page's direct neighbours, plus links among those neighbours
- Access: open the right sidebar and click **Page graph** in its top bar ([`src/main/frontend/components/right_sidebar.cljs:530-537`](../../../../../src/main/frontend/components/right_sidebar.cljs), `:126-128`; `en.edn:799`)

> **Removed:** "Page menu → Show graph view" and "1-3 hops away". The page graph is added from the right sidebar, and it is built from one hop around the page ([`src/main/frontend/worker/graph_view.cljs:980-1021`](../../../../../src/main/frontend/worker/graph_view.cljs)).

> **Removed:** "Namespace Graph". There is no namespace graph mode; the global graph has only the Tags and All pages modes ([`src/main/frontend/worker/graph_view.cljs:920-928`](../../../../../src/main/frontend/worker/graph_view.cljs)). Page hierarchy does appear in All pages as parent edges (see [Edge Types](#edge-types)).

---

## Graph Structure

### Node Types

**All pages view**

Every page is a node, marked with a kind ([`src/main/frontend/worker/graph_view.cljs:518-524`](../../../../../src/main/frontend/worker/graph_view.cljs)):

```markdown
Project Alpha → page
Jan 21st, 2026 → journal (hidden unless "Show journals" is on)
active → tag (a tag is a page too)
```

Some pages are left out: hidden or recycled pages, property pages, built-in pages, and pages with **Excluded from Graph view?** set ([`src/main/frontend/worker/graph_view.cljs:785-798`](../../../../../src/main/frontend/worker/graph_view.cljs), `:838-848`, `:868-871`). Pages with no connections are still drawn: the global graph never asks to hide orphans ([`src/main/frontend/components/graph.cljs:785-788`](../../../../../src/main/frontend/components/graph.cljs); [`src/main/frontend/worker/graph_view.cljs:611-613`](../../../../../src/main/frontend/worker/graph_view.cljs)).

**Tags view**

```markdown
#meeting → tag node
A block tagged #meeting → object node, linked to the tag
```

Tags view draws tags and the nodes tagged with them, blocks and pages alike ([`src/main/frontend/worker/graph_view.cljs:407-481`](../../../../../src/main/frontend/worker/graph_view.cljs)). Structural built-in tags such as Tag, Property, Page and Asset are not drawn (`:57-64`). A tag with nothing tagged is not drawn either, and if no tag has tagged nodes the view is empty (`:452-455`).

**Properties**

```markdown
A node property "project" whose value is Project Alpha
→ an edge labelled "project" to Project Alpha
```

> **Removed:** `status:: active` and `project:: [[Alpha]]`. `key:: value` typed into a block is file-graph syntax and is stored as text, not as a property. Add properties with `/Add property`.

### Edge Types

**1. Page Links (Explicit Connections)**

```markdown
A block on Project Alpha mentions [[Client X]]
```

**Graph:** `Project Alpha — Client X`

In All pages, each reference from a block becomes an edge from the block's page to the referenced page ([`src/main/frontend/worker/graph_view.cljs:562-576`](../../../../../src/main/frontend/worker/graph_view.cljs)). Only edges whose two ends are both drawn nodes are kept (`:775-783`).

**2. Block References**

```markdown
[[<block-uuid>]] → References specific block
```

**Graph (All pages):** no edge. The referenced block isn't a page node, so the reference is dropped when edges are matched to nodes ([`src/main/frontend/worker/graph_view.cljs:775-783`](../../../../../src/main/frontend/worker/graph_view.cljs)).

> **Removed:** `((block-uuid))` and "Current Block —[refs]→ Referenced Block's Page". A block reference is `[[<uuid>]]` ([`src/main/frontend/util/ref.cljs:5-7`](../../../../../src/main/frontend/util/ref.cljs)); `((uuid))` renders as literal text ([`src/main/frontend/components/block.cljs:1573-1574`](../../../../../src/main/frontend/components/block.cljs)).

> **Unverified:** How the page graph treats a block reference. It collects every `:block/refs` target of the page's blocks ([`src/main/frontend/worker/graph_view.cljs:949-961`](../../../../../src/main/frontend/worker/graph_view.cljs)), which can include blocks; whether those are drawn as nodes wasn't checked.

**3. Tags (Classification)**

```markdown
- Project meeting #meeting #important
```

**Graph:**

- Tags view: `Block —→ meeting, important` (the block is an object node) ([`src/main/frontend/worker/graph_view.cljs:431-437`](../../../../../src/main/frontend/worker/graph_view.cljs))
- All pages: `Block's page — meeting, important`, because the tags are part of the block's references ([`deps/outliner/src/logseq/outliner/pipeline.cljs:118`](../../../../../deps/outliner/src/logseq/outliner/pipeline.cljs)). A page that is itself tagged also links to its tag ([`src/main/frontend/worker/graph_view.cljs:594-599`](../../../../../src/main/frontend/worker/graph_view.cljs))

**4. Property Values**

A user property whose values are nodes draws an edge from the node that has the property to each value, labelled with the property's name ([`src/main/frontend/worker/graph_view.cljs:300-346`](../../../../../src/main/frontend/worker/graph_view.cljs)).

**5. Hierarchy**

- A page's parent page (for example the `Projects` page above `Alpha` when a `Projects/Alpha` title was split) draws an edge in All pages ([`src/main/frontend/worker/graph_view.cljs:601-609`](../../../../../src/main/frontend/worker/graph_view.cljs))
- A tag that extends another tag draws an "extends" edge in both views (`:348-372`, `:380-389`)

**6. Backlinks (Reverse Connections)**

A backlink is the same edge seen from the other end. All pages view and the page graph draw an arrowhead from the linking page to the linked one; Tags view draws none, since the global graph passes arrows off and only those two views override it ([`src/main/frontend/extensions/graph/pixi.cljs:1785-1788`](../../../../../src/main/frontend/extensions/graph/pixi.cljs), `:201-203`; [`src/main/frontend/components/graph.cljs:21`](../../../../../src/main/frontend/components/graph.cljs)).

```
Page A links to Page B
  ⇒ Page B shows "Linked references" from Page A
  ⇒ The graph draws one edge between A and B
```

---

## Graph View Features

### Global Graph View

**Access:** **Graph view** in the left sidebar, or `g g`

**Interface Elements:**

```
┌─────────────────────────────────────────────────┐
│                                                 │
│         ○ ── ○ ── ○                             │
│        /       \    \                           │
│       ○         ○ ── ○                          │
│      /          |                               │
│     ○ ────────  ○                               │
│                                                 │
│   [Your interconnected knowledge graph]         │
│                                                 │
├─────────────────────────────────────────────────┤
│  [⚙ Graph settings] [⟲ Time travel] [↻ Reset]   │
└─────────────────────────────────────────────────┘
```

The canvas has a bottom toolbar with the **Graph settings** button, the **Time travel** control, and a **Reset graph view** button that appears once something is selected or focused ([`src/main/frontend/components/graph.cljs:692-717`](../../../../../src/main/frontend/components/graph.cljs), `:682-690`; `en.edn:772`, `:777`, `:780`).

> **Removed:** "[Filter] [Orphans] [Color by] [Layout]" toolbar buttons. The toolbar holds only the three controls above.

**Controls:**

| Control | Action |
| --- | --- |
| **Mouse wheel** | Zoom in/out around the pointer |
| **Drag empty space** | Pan around |
| **Drag a node** | Move it; connected nodes follow, more weakly further away |
| **Click node** | Highlight it and its neighbours; everything else dims |
| **Click a highlighted node** | Remove its highlight |
| **Click empty space** | Clear the highlight |
| **Double-click node** | Open the page |
| **Shift-click node** | Open it in the right sidebar |
| **Cmd-click (macOS) / Ctrl-click** | Preview the node in a popup |
| **Hover** | Emphasise the node and show its label |

Sources: wheel zoom, scale clamped to 0.05–3.6 ([`src/main/frontend/extensions/graph/pixi.cljs:1593-1618`](../../../../../src/main/frontend/extensions/graph/pixi.cljs); [`src/main/frontend/extensions/graph/pixi/logic.cljs:278-300`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs)); pan and node drag (`pixi.cljs:1503-1511`); dragged neighbours follow by Depth with decaying weight (`pixi.cljs:2272-2278`); click, double-click within 320 ms and shift-click (`logic.cljs:276`, `:303-323`; `pixi.cljs:1458-1479`); Cmd/Ctrl-click preview (`pixi.cljs:1450-1457`; [`src/main/frontend/util.cljc:1072-1075`](../../../../../src/main/frontend/util.cljc)); open and open-in-sidebar ([`src/main/frontend/components/graph_actions.cljs:68-73`](../../../../../src/main/frontend/components/graph_actions.cljs)); preview popup (`graph_actions.cljs:32-66`); click on empty space clears (`pixi.cljs:1590-1592`); hover emphasis and label (`logic.cljs:263-274`, `:349-357`).

> **Removed:** "Right-click → Context menu". The renderer listens only for pointer down/move/up/leave, wheel and keydown, and its keydown handler does nothing ([`src/main/frontend/extensions/graph/pixi.cljs:1624-1629`](../../../../../src/main/frontend/extensions/graph/pixi.cljs), `:1619-1620`).

### Graph Settings

**Access:** The settings (gear) button in the bottom toolbar opens **Graph settings**. Settings are "Saved per graph" ([`src/main/frontend/components/graph.cljs:656-680`](../../../../../src/main/frontend/components/graph.cljs); `en.edn:777-778`), in the browser's local storage under a key for that graph (`graph.cljs:32-34`, `:98-114`).

**Display Options:**

```yaml
View mode:
  - Tags (default)
  - All pages

Displayed tags:        # Tags view only
  Search tags, Select all, Clear
  One checkbox per tag, with its count

Layout:                # labelled "Force graph"
  Node and link counts
  Depth: 1-5 (default 1)
    # How far a highlight spreads from the clicked node;
    # disabled until a node is selected
  Link distance: 36-180, step 6 (default 72)
  Grid layout: true/false     # Tags view only
  Show journals: true/false   # All pages view only, default off
```

Sources: defaults ([`src/main/frontend/components/graph.cljs:16-30`](../../../../../src/main/frontend/components/graph.cljs)); the three groups (`:313-338`, `:340-384`, `:427-473`); Depth disabled without a selection (`:395-397`); Depth spreading the highlight ([`src/main/frontend/extensions/graph/pixi/logic.cljs:219-251`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs)); labels `en.edn:742-753`, `:787-789`.

> **Removed:** "Show orphan pages", "Show page previews", the "Layout algorithm" choice (circular, hierarchical) and the "Color scheme" choice (by type, by tag, by creation date). None of these settings exist in the panel ([`src/main/frontend/components/graph.cljs:656-680`](../../../../../src/main/frontend/components/graph.cljs)).

**Excluding a page:** A page with the built-in **Excluded from Graph view?** checkbox property ([`deps/db/src/logseq/db/frontend/property.cljs:425-430`](../../../../../deps/db/src/logseq/db/frontend/property.cljs); `en.edn:1468`) is left out of All pages. In Tags view, the page and its blocks are left out ([`src/main/frontend/worker/graph_view.cljs:231-248`](../../../../../src/main/frontend/worker/graph_view.cljs)).

> **Unverified:** How to set **Excluded from Graph view?** from the UI. The property is defined with `:hide? true`, and no page-menu action for it was found.

### Time Travel

The **Time travel** slider hides every node created after the chosen date, so you can replay how the graph grew. Its play button ("Now") animates the slider forward to the present ([`src/main/frontend/components/graph.cljs:475-522`](../../../../../src/main/frontend/components/graph.cljs), `:580-654`; `en.edn:780-781`).

### Page Graph View

**Per-Page Graph:**

```
Current Page: [[Project Alpha]]

Nodes:
- Project Alpha (pinned in the centre)
- Pages Project Alpha's blocks link to: [[Client X]], [[Roadmap]]
- Pages whose blocks link to Project Alpha: [[Team]]
- Project Alpha's tags, and the tags they extend

Edges:
- Project Alpha to each of those nodes
- Links among the neighbour pages themselves
```

The page graph opens on the page you are viewing, or on today's journal when you aren't on a page ([`src/main/frontend/components/page.cljs:717-745`](../../../../../src/main/frontend/components/page.cljs)). It counts references to the page's aliases too, and it leaves out journal pages that mention it ([`src/main/frontend/worker/graph_view.cljs:930-961`](../../../../../src/main/frontend/worker/graph_view.cljs), `:963-1021`). Nodes are placed in rings by distance from the page, which is pinned in the centre ([`src/main/frontend/extensions/graph/pixi/logic.cljs:1083-1111`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs), `:654-656`). Labels are shown without zooming (`logic.cljs:488-490`), and double-click, shift-click and Cmd/Ctrl-click work as in the global graph ([`src/main/frontend/components/page.cljs:696-704`](../../../../../src/main/frontend/components/page.cljs)).

> **Removed:** "Adjustable Depth: 1 hop / 2 hops / 3 hops" and "Connected Pages (2 hops): John (via Team)". The page graph has no settings panel and doesn't walk past direct neighbours; a second-hop page shows up only if it is itself linked to the page ([`src/main/frontend/worker/graph_view.cljs:980-1021`](../../../../../src/main/frontend/worker/graph_view.cljs)).

**Example Visualization:**

```
        Roadmap
           |
    Team—[Project Alpha]—Client X
      |                     |
      └──── (edge only if Team links Client X)
```

---

## Navigation Patterns

### Pattern 1: Hub-and-Spoke Discovery

**Use Case:** Find all related content from central page

**Example: Project Hub**

```markdown
Page "Projects":

- [[Project Alpha]]
- [[Project Beta]]
- [[Project Gamma]]

Graph view shows:
Projects (center) ←→ All project pages (spokes)
```

**Navigation:**

1. Open the Projects page
2. Add **Page graph** in the right sidebar
3. Double-click any spoke to open it
4. Follow connections outward

### Pattern 2: Cluster Exploration

**Use Case:** Discover topic clusters

**Steps:**

1. Open the global graph in **Tags** view
2. Each tag's nodes are pulled together around it, with a shaded cluster background ([`src/main/frontend/extensions/graph/pixi/logic.cljs:1441-1453`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs), `:810-858`)
3. Zoom in near a tag: the view focuses on that tag and its nodes ([`src/main/frontend/extensions/graph/pixi.cljs:193-199`](../../../../../src/main/frontend/extensions/graph/pixi.cljs), `:2214-2234`)
4. Double-click nodes to explore content

**Example Clusters:**

```
#Project cluster:       #Book cluster:
  ○ Project Alpha         ○ Atomic Habits
  ○ Project Beta          ○ Deep Work
  ○ Website Redesign      ○ Thinking, Fast and Slow
```

In Tags view the clusters are the tags themselves. Logseq doesn't run a clustering or community-detection algorithm on links.

### Pattern 3: Path Traversal

**Use Case:** Trace connection between two pages

**Example:**

```
How is "Machine Learning" connected to "Product Design"?

Path:
Machine Learning
  → [[Data Analysis]]
    → [[User Insights]]
      → [[Product Design]]
```

**Manual Traversal:**

1. Start at page A
2. Check linked references
3. Follow promising link
4. Repeat until reaching page B

In the global graph, clicking page A and raising **Depth** highlights everything within that many hops, which can make a short path visible.

> **Unverified:** A built-in "find path between two pages" action. None was found.

### Pattern 4: Orphan Discovery

**Use Case:** Find isolated pages (potential cleanup)

**Steps:**

1. Graph view → **All pages** view mode
2. Pages with no connections are drawn apart from the rest: after the layout runs, All pages moves unlinked nodes into rings around the linked ones ([`src/main/frontend/extensions/graph/pixi/logic.cljs:1471-1472`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs), `:1037-1050`, `:953-979`). From 2,500 nodes the fast layout lays every node out in a plain grid instead, so orphans aren't set apart (`:1384-1387`, `:1003-1035`)
3. Review: Delete or connect?

> **Removed:** "Enable 'Show orphans'". There is no such toggle; All pages always includes orphans ([`src/main/frontend/worker/graph_view.cljs:611-613`](../../../../../src/main/frontend/worker/graph_view.cljs)).

**Example:**

```
Orphan Pages (no connections):
- [[Old Meeting Notes 2023]]  ← Archive?
- [[Random Idea 5]]           ← Connect or delete?
- [[Test Page]]               ← Delete
```

### Pattern 5: Backlink Navigation

**Use Case:** See who references this page

**Example:**

```markdown
Page "Product Roadmap":

Linked references:

- Q1 Planning: mentions [[Product Roadmap]]
- Team Meeting (Jan 15th, 2026): discussed [[Product Roadmap]]
- Client X Proposal: references [[Product Roadmap]]
```

**Navigation:**

1. View the "Linked references" section below the page content
2. Each referencing block is shown under its page, with its parent blocks as a breadcrumb
3. Follow trail of connections

See [Linked References](linked-references.md) for how the section is built.

---

## Graph Analysis

### Analyzing Your Graph

Logseq's graph view reports only the number of nodes and links on screen ([`src/main/frontend/components/graph.cljs:441-443`](../../../../../src/main/frontend/components/graph.cljs)). The metrics below are ways to read the picture yourself; Logseq doesn't compute them.

**Metrics to Consider:**

**1. Node Degree (Connection Count)**

```
High degree = Hub page (many connections)
Low degree = Leaf page (few connections)
Zero degree = Orphan (isolated)
```

Degree is visible: a node's radius grows with the square root of its number of links, up to a cap ([`src/main/frontend/extensions/graph/pixi/logic.cljs:593-614`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs)). In Tags view only tag nodes grow; other nodes keep a small fixed size (`:607-609`).

**Example Analysis:**

```
[[Projects]] → 47 connections (hub)
Today's journal → 312 connections (super hub)
[[Random Idea]] → 0 connections (orphan)
```

**2. Clustering (Density)**

```
Dense cluster = Related topic area
Sparse cluster = Loosely related
```

**Example:**

```
Work Cluster:
  High density = Pages frequently reference each other
  (Projects ↔ Meetings ↔ Team ↔ Clients)

Personal Cluster:
  Low density = Pages occasionally reference each other
```

**3. Path Length**

```
Short path = Closely related topics
Long path = Distantly related
No path = Unconnected domains
```

**Example:**

```
[[Code]] → [[Bug Fix]] → [[Deploy]]
  Path length: 2 (closely related)

[[Code]] → [[Team]] → [[Meeting]] → [[Lunch]]
  Path length: 3 (loosely related)
```

### Graph Health Indicators

**Healthy Graph:**

- ✅ Most pages connected (few orphans)
- ✅ Clear topic clusters
- ✅ Hub pages for major topics
- ✅ Related topics a few hops apart

**Unhealthy Graph:**

- ❌ Many orphan pages
- ❌ Single mega-cluster (no structure)
- ❌ Long paths between related topics
- ❌ Duplicate pages with similar names

These are rules of thumb, not measurements Logseq takes.

---

## Best Practices

### Building a Connected Graph

**1. Link Proactively**

```markdown
❌ Poor linking:

- Met with John about the project

✅ Rich linking:

- Met with [[John]] about [[Project Alpha]]
  - Discussed [[Roadmap]] and [[Budget]]
```

**2. Create Hub Pages**

```markdown
Page "Projects" (hub):

- [[Project Alpha]]
- [[Project Beta]]
- [[Project Gamma]]
- [[Archived Projects]]

Page "Team" (hub):

- [[John]]
- [[Sarah]]
- [[Team Meetings]]
- [[Team Processes]]
```

**3. Use Consistent Naming**

```markdown
❌ Inconsistent:

- [[Project Alpha]]
- [[Alpha Project]]
- [[Proj. Alpha]]

✅ Consistent:

- [[Project Alpha]]
- [[Project Beta]]
- [[Project Gamma]]
```

Capitalisation alone doesn't create a duplicate: page names are looked up lower-cased ([`deps/db/src/logseq/db/frontend/entity_util.cljs:107-109`](../../../../../deps/db/src/logseq/db/frontend/entity_util.cljs)).

**4. Leverage Tags**

```markdown
- Project kickoff meeting #meeting #project #q1
  - Creates connections to all three tags
```

`#tag` tags the block and creates the tag if it's new ([`deps/outliner/src/logseq/outliner/core.cljs:197-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)). In Tags view the block joins each tag's cluster; in All pages, the block's page links to each tag.

**5. Create Page Hierarchies**

```markdown
[[Projects/Work/Alpha]]
[[Projects/Work/Beta]]
[[Projects/Personal/Website]]

Creates structured graph clusters
```

Creating a `/` title with **New page** splits it into one page per segment under **Library** ([`deps/outliner/src/logseq/outliner/page.cljs:464-467`](../../../../../deps/outliner/src/logseq/outliner/page.cljs)). All pages draws an edge from each page to its parent ([`src/main/frontend/worker/graph_view.cljs:601-609`](../../../../../src/main/frontend/worker/graph_view.cljs)).

> **Unverified:** Whether a `/` title typed as a link and saved without **New page** is split as well.

### Graph Maintenance

**Weekly Cleanup:**

1. **Find orphans:**

   - Graph view → All pages
   - Link or delete

2. **Merge duplicates:**

   - Search for similar names
   - Merge: Copy content → Delete duplicate

3. **Add missing links:**

   - Review recent pages
   - Add obvious connections

4. **Create hub pages:**
   - Identify growing topic areas
   - Create index pages

**Example Cleanup Session:**

```markdown
Found:

- [[Project A]] and [[ProjectA]] (duplicate)
  → Merge into [[Project A]]

- [[Random Note 2023]] (orphan)
  → Link to [[Archive]]

- Many pages mention "React"
  → Create [[React]] hub page
```

---

## Advanced Visualization

### How the Graph Is Laid Out

**Force-directed (default):**

```
○     ○
 \   /
  ○─○
 /   \
○     ○

Pages cluster naturally based on connections
```

The layout is a force simulation from `d3-force`: links pull connected nodes to the **Link distance**, nodes repel each other, and a collision force keeps them from overlapping ([`src/main/frontend/extensions/graph/pixi/logic.cljs:3`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs), `:1420-1440`). The simulation runs a fixed number of steps before drawing, fewer for bigger graphs (`:1454-1457`, `:879-897`). The result is drawn with PixiJS ([`src/main/frontend/extensions/graph/pixi.cljs:2`](../../../../../src/main/frontend/extensions/graph/pixi.cljs); `package.json:163`, `:182`).

**Tags view clusters:**

Tags view adds a pull toward each tag's cluster position (`logic.cljs:1441-1453`), and with **Grid layout** on it arranges the clusters in a grid without the force step (`:1402-1403`, `:1218-1295`).

**Large graphs:**

At 10,000 nodes in Tags view, or 2,500 in All pages, Logseq switches to a faster layout that skips the force simulation. It also caps how many nodes and edges it draws (`logic.cljs:664-720`, `:1378-1394`).

> **Unverified:** How the graph view performs at any particular size. The thresholds above are code constants, not measured limits.

> **Removed:** "Circular" and "Hierarchical" layouts. No such layout options exist; the only layout choice is **Grid layout** in Tags view ([`src/main/frontend/components/graph.cljs:461-467`](../../../../../src/main/frontend/components/graph.cljs)).

### Color Coding

**By Node Kind:**

```
Violet = Tags
Amber  = Property pages
Sky blue = Journal pages
Blue   = The current page (page graph)
Grey   = Other pages
```

The worker colours nodes by kind ([`src/main/frontend/worker/graph_view.cljs:526-542`](../../../../../src/main/frontend/worker/graph_view.cljs)); nodes without a colour fall back to the renderer's tag, property and default colours ([`src/main/frontend/extensions/graph/pixi/logic.cljs:496-505`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs)). Exact shades differ between light and dark themes.

> **Removed:** The colour legend with yellow namespace pages, red orphan pages and white tag pages, and "By Tag" colouring (`#work` blue, `#personal` green). Colours come only from the node kinds above.

> **Unverified:** Whether large All pages graphs (10,000+ pages) keep the per-kind colours. That path gives every node the same grey ([`src/main/frontend/worker/graph_view.cljs:726-737`](../../../../../src/main/frontend/worker/graph_view.cljs)).

### Labels

Page labels appear once you zoom in far enough, on hover, and for highlighted nodes ([`src/main/frontend/extensions/graph/pixi.cljs:1903-1908`](../../../../../src/main/frontend/extensions/graph/pixi.cljs); [`src/main/frontend/extensions/graph/pixi/logic.cljs:8-19`](../../../../../src/main/frontend/extensions/graph/pixi/logic.cljs), `:349-372`). Property and "extends" edges carry labels ([`src/main/frontend/worker/graph_view.cljs:300-346`](../../../../../src/main/frontend/worker/graph_view.cljs)), drawn once you zoom in to at least 0.82× ([`src/main/frontend/extensions/graph/pixi.cljs:189`](../../../../../src/main/frontend/extensions/graph/pixi.cljs), `:1789-1793`; [`src/main/frontend/components/graph.cljs:22`](../../../../../src/main/frontend/components/graph.cljs)).

### Graph Filtering

**Show only:**

- Chosen tags (**Displayed tags**, Tags view)
- Pages other than journals (**Show journals** off, All pages view)
- Nodes created up to a date (**Time travel**)
- Everything except pages marked **Excluded from Graph view?**

Sources: [`src/main/frontend/components/graph.cljs:248-268`](../../../../../src/main/frontend/components/graph.cljs) (displayed tags), `:524-546` (journals), `:498-522` (time travel); [`src/main/frontend/worker/graph_view.cljs:231-248`](../../../../../src/main/frontend/worker/graph_view.cljs) (excluded).

> **Removed:** Filtering by namespace, by created-date range, and by connection count, and the `Show only: #project AND namespace:Work/` filter syntax. The graph view has no filter expression; its filters are the four above.

---

## Graph-Based Workflows

### Workflow 1: Daily Review

1. Open today's journal
2. Add **Page graph** in the right sidebar
3. Identify missing connections
4. Add links as appropriate

### Workflow 2: Topic Exploration

1. Start with seed page
2. Open the global graph and click the seed page
3. Raise **Depth** to 2 to highlight two hops out
4. Follow path to discovery

### Workflow 3: Content Audit

1. Global graph view, **All pages**
2. Find orphans
3. Decide: Connect, archive, or delete
4. Find duplicate names (similar nodes close together)

### Workflow 4: Idea Generation

1. Pick two random pages
2. Trace a path between them by following links
3. Use path as creative constraint
4. Generate ideas connecting concepts

---

## Mobile Graph View

No graph view was found in the mobile app. Its router defines only `/`, `/page/:name`, `/import` and `/export` ([`src/main/mobile/routes.cljs:8-19`](../../../../../src/main/mobile/routes.cljs)). Its tabs are home, graphs (the graph list), go to, search, flashcards and capture ([`src/main/mobile/components/app.cljs:272-283`](../../../../../src/main/mobile/components/app.cljs)). The graph view route belongs to the desktop app's router ([`src/main/frontend/routes.cljs:64-66`](../../../../../src/main/frontend/routes.cljs)).

> **Removed:** "Mobile Limitations" and "Mobile Best Practices" (local graph, reduced hop depth). They describe a mobile graph view that the mobile code doesn't route to.

---

## Troubleshooting

**Graph view is empty:**

- You're in **Tags** view (the default) and no tag has tagged nodes; Tags view is then empty ([`src/main/frontend/worker/graph_view.cljs:452-455`](../../../../../src/main/frontend/worker/graph_view.cljs)). Switch **View mode** to **All pages**
- In Tags view, **Displayed tags** was cleared; use **Select all**
- The **Time travel** slider is set to an early date; press its play button to return to "Now"
- "Graph failed to load." appears: use **Retry** ([`src/main/frontend/components/graph.cljs:386-393`](../../../../../src/main/frontend/components/graph.cljs); `en.edn:728`, `:773`)

> **Removed:** "Reload graph (⌘ R)". No graph-specific reload shortcut was found; a failed build offers **Retry**.

**Graph is too cluttered:**

- Turn **Show journals** off (All pages)
- Narrow **Displayed tags** (Tags view)
- Mark noisy pages **Excluded from Graph view?**
- Use the page graph instead

> **Removed:** "Hide orphan pages" and "Filter by namespace". Neither control exists (see [Graph Settings](#graph-settings)).

**Performance is slow:**

- Reduce visible nodes (hide journals, narrow displayed tags)
- Use the page graph for a single page

> **Removed:** "Close other tabs", "Increase RAM allocation" and "Consider archiving old content". No graph setting or source behind them was found. Large graphs already switch to a faster layout (see [How the Graph Is Laid Out](#how-the-graph-is-laid-out)).

**Nodes overlap/hard to see:**

- Zoom in
- Pan to area of interest
- Increase **Link distance**
- Turn on **Grid layout** (Tags view)

---

## Next Steps

**Master the Graph:**

1. Explore your current graph structure
2. Identify and clean orphan pages
3. Create hub pages for major topics
4. Practice navigation patterns
5. Review graph weekly

**Related Guides:**

- [Linked References](linked-references.md)
- [Linking Pages Tutorial](../../tutorials/getting-started/04-linking-pages.md)

> **Removed:** Links to `how-to/navigation/graph-navigation.md` and `how-to/navigation/search-strategies.md`. Neither page exists.

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
