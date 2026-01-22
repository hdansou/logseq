---
title: 'Graph Visualization and Navigation'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Linked References](../../tutorials/getting-started/linked-references.md)'
  - '[Graph Navigation How-To](../../how-to/navigation/graph-navigation.md)'
  - '[Search Strategies](../../how-to/navigation/search-strategies.md)'
tags: ['graph', 'visualization', 'navigation', 'connections', 'knowledge-graph']
---

# Graph Visualization and Navigation

This document explains Logseq's graph visualization system—how your notes form an interconnected knowledge graph, and how to navigate and understand these relationships. Master the graph view to discover insights and connections in your knowledge base.

**What You'll Learn:**

- Graph structure and representation
- Types of connections (links, references, embeds)
- Graph navigation techniques
- Local vs global graph views
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

- **Nodes** (vertices) = Pages/blocks
- **Edges** (connections) = Links, references, tags

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
# [[Projects]]

- [[Project Alpha]]
  - Status: #active
  - Owner: [[John]]
  - Related: [[Client X]]

# [[John]]

- Role: Engineer
- Projects: [[Project Alpha]], [[Project Beta]]
- Tags: #team
```

**Graph:**

```
Projects ←→ Project Alpha ←→ John
              ↓                ↓
            Client X      Project Beta
              ↑
            [#active]
```

### Graph Types in Logseq

**1. Global Graph**

- Shows all pages and connections
- Bird's-eye view of entire knowledge base
- Access: Toolbar → Graph icon or `Cmd+Shift+G`

**2. Local Graph**

- Shows connections for current page only
- Focused view (1-3 hops away)
- Access: Page menu → "Show graph view"

**3. Namespace Graph**

- Pages in same namespace hierarchy
- Example: `Projects/`, `Team/`
- Organized hierarchically

---

## Graph Structure

### Node Types

**Pages (Primary Nodes)**

```markdown
[[Project Alpha]] → Page node
[[People/John]] → Namespaced page node
[[2026-01-21]] → Journal page node
```

**Tags (Semantic Nodes)**

```markdown
#active → Tag node (implicit page)
#status/in-progress → Nested tag node
```

**Properties (Metadata Nodes)**

```markdown
status:: active → Property creates implicit connection
project:: [[Alpha]] → Explicit page reference
```

### Edge Types

**1. Page Links (Explicit Connections)**

```markdown
[[Project Alpha]] mentions [[Client X]]
```

**Graph:** `Project Alpha —[mentions]→ Client X`

**2. Block References (Granular Links)**

```markdown
((block-uuid)) → References specific block
```

**Graph:** `Current Block —[refs]→ Referenced Block's Page`

**3. Tags (Classification)**

```markdown
- Project meeting #meeting #important
```

**Graph:** `Block —[tagged]→ meeting, important`

**4. Backlinks (Reverse Connections)**

Automatically tracked:

```
Page A links to Page B
  ⇒ Page B shows "backlink" from Page A
```

---

## Graph View Features

### Global Graph View

**Access:** Click **Graph** icon in toolbar

**Interface Elements:**

```
┌─────────────────────────────────────────────────┐
│  [Filter] [Orphans] [Color by] [Layout]  [⚙️]  │
├─────────────────────────────────────────────────┤
│                                                 │
│         ○ ── ○ ── ○                            │
│        /       \    \                           │
│       ○         ○ ── ○                          │
│      /          |                               │
│     ○ ────────  ○                               │
│                                                 │
│   [Your interconnected knowledge graph]        │
│                                                 │
└─────────────────────────────────────────────────┘
```

**Controls:**

| Control               | Action           |
| --------------------- | ---------------- |
| **Mouse wheel**       | Zoom in/out      |
| **Click + drag**      | Pan around       |
| **Click node**        | Select/highlight |
| **Double-click node** | Open page        |
| **Right-click**       | Context menu     |
| **Hover**             | Show page title  |

### Graph Settings

**Access:** Settings icon in graph view

**Display Options:**

```yaml
Show orphan pages:
  true/false
  # Pages with no connections

Show journal pages:
  true/false
  # Daily notes pages

Show page previews:
  true/false
  # Hover tooltip with content

Layout algorithm:
  - Force-directed (organic clustering)
  - Circular (organized circle)
  - Hierarchical (tree structure)

Color scheme:
  - By page type (journal, namespace, orphan)
  - By tag (#project = blue, #personal = green)
  - By creation date (older = cooler colors)
```

### Local Graph View

**Per-Page Graph:**

```
Current Page: [[Project Alpha]]

Connected Pages (1 hop):
- [[Team]]
- [[Client X]]
- [[Roadmap]]
- #active

Connected Pages (2 hops):
- [[John]] (via Team)
- [[Budget]] (via Client X)
```

**Adjustable Depth:**

- 1 hop: Direct connections only
- 2 hops: Friends-of-friends
- 3 hops: Extended network

**Example Visualization:**

```
        Roadmap
           |
    Team—[Project Alpha]—Client X
      |                     |
    John                 Budget
```

---

## Navigation Patterns

### Pattern 1: Hub-and-Spoke Discovery

**Use Case:** Find all related content from central page

**Example: Project Hub**

```markdown
# [[Projects]]

- [[Project Alpha]]
- [[Project Beta]]
- [[Project Gamma]]

Graph view shows:
Projects (center) ←→ All project pages (spokes)
```

**Navigation:**

1. Open `[[Projects]]`
2. View local graph
3. Click any spoke to explore
4. Follow connections outward

### Pattern 2: Cluster Exploration

**Use Case:** Discover topic clusters

**Steps:**

1. Open global graph
2. Look for dense clusters (many connections)
3. Zoom in to identify cluster theme
4. Double-click nodes to explore content

**Example Clusters:**

```
Work Cluster:           Personal Cluster:
  ○ Projects              ○ Journal
  ○ Meetings              ○ Reflections
  ○ Team                  ○ Goals
  ○ Clients               ○ Habits
```

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

### Pattern 4: Orphan Discovery

**Use Case:** Find isolated pages (potential cleanup)

**Steps:**

1. Graph view → Enable "Show orphans"
2. Orphan pages appear disconnected
3. Review: Delete or connect?

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
# [[Product Roadmap]]

Linked References:

- [[Q1 Planning]] mentions roadmap
- [[Team Meeting 2026-01-15]] discussed roadmap
- [[Client X Proposal]] references roadmap
```

**Navigation:**

1. View "Linked References" section (bottom of page)
2. Click any reference to see context
3. Follow trail of connections

---

## Graph Analysis

### Analyzing Your Graph

**Metrics to Consider:**

**1. Node Degree (Connection Count)**

```
High degree = Hub page (many connections)
Low degree = Leaf page (few connections)
Zero degree = Orphan (isolated)
```

**Example Analysis:**

```
[[Projects]] → 47 connections (hub)
[[Daily Notes]] → 312 connections (super hub)
[[Random Idea]] → 0 connections (orphan)
```

**2. Clustering Coefficient**

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
- ✅ Average path length < 5

**Unhealthy Graph:**

- ❌ Many orphan pages
- ❌ Single mega-cluster (no structure)
- ❌ Long paths between related topics
- ❌ Duplicate pages with similar names

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
  - Action items: #todo
```

**2. Create Hub Pages**

```markdown
# [[Projects]] (Hub)

- [[Project Alpha]]
- [[Project Beta]]
- [[Project Gamma]]
- [[Archived Projects]]

# [[Team]] (Hub)

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

**4. Leverage Tags**

```markdown
- Project kickoff meeting #meeting #project #q1
  - Creates connections to all three tag pages
```

**5. Create Namespace Hierarchies**

```markdown
[[Projects/Work/Alpha]]
[[Projects/Work/Beta]]
[[Projects/Personal/Website]]

Creates structured graph clusters
```

### Graph Maintenance

**Weekly Cleanup:**

1. **Find orphans:**

   - Graph view → Show orphans
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

### Custom Graph Layouts

**Force-Directed (Default):**

```
○     ○
 \   /
  ○─○
 /   \
○     ○

Pages cluster naturally based on connections
```

**Circular:**

```
    ○
  ○   ○
○   ○   ○
  ○   ○
    ○

Pages arranged in circle (alphabetical or by date)
```

**Hierarchical:**

```
      ○
    / | \
   ○  ○  ○
  /|  |  |\
 ○ ○  ○  ○ ○

Tree structure (parent-child relationships)
```

### Color Coding

**By Page Type:**

```
🔵 Blue = Regular pages
🟢 Green = Journal pages
🟡 Yellow = Namespace pages
🔴 Red = Orphan pages
⚪ White = Tag pages
```

**By Tag:**

```
#work → Blue nodes
#personal → Green nodes
#project → Orange nodes
```

### Graph Filtering

**Show only:**

- Pages with specific tag
- Pages in namespace
- Pages created in date range
- Pages with >X connections

**Example Filter:**

```
Show only: #project AND namespace:Work/
Result: Work-related project pages only
```

---

## Graph-Based Workflows

### Workflow 1: Daily Review

1. Open today's journal
2. View local graph
3. Identify missing connections
4. Add links as appropriate

### Workflow 2: Topic Exploration

1. Start with seed page
2. View local graph (2-hop)
3. Identify interesting connections
4. Follow path to discovery

### Workflow 3: Content Audit

1. Global graph view
2. Find orphans
3. Decide: Connect, archive, or delete
4. Find duplicate names (similar nodes close together)

### Workflow 4: Idea Generation

1. Pick two random pages
2. Find shortest path between them
3. Use path as creative constraint
4. Generate ideas connecting concepts

---

## Mobile Graph View

**Mobile Limitations:**

- Smaller viewport (harder to see full graph)
- Touch gestures instead of mouse
- Limited filtering options
- May be slow on large graphs

**Mobile Best Practices:**

1. Use local graph view (not global)
2. Reduce hop depth (1-2 max)
3. Use search instead of visual navigation
4. Switch to desktop for graph analysis

---

## Troubleshooting

**Graph view is empty:**

- Check "Show journal pages" setting
- Verify pages have connections
- Reload graph (⌘ R)

**Graph is too cluttered:**

- Hide journal pages
- Hide orphan pages
- Filter by namespace or tag
- Use local graph instead

**Performance is slow:**

- Reduce visible nodes (hide journals)
- Close other tabs
- Increase RAM allocation
- Consider archiving old content

**Nodes overlap/hard to see:**

- Zoom in
- Pan to area of interest
- Switch layout algorithm
- Adjust spacing in settings

---

## Next Steps

**Master the Graph:**

1. Explore your current graph structure
2. Identify and clean orphan pages
3. Create hub pages for major topics
4. Practice navigation patterns
5. Review graph weekly

**Related Guides:**

- [Linked References Tutorial](../../tutorials/getting-started/linked-references.md)
- [Navigation Strategies](../../how-to/navigation/graph-navigation.md)
- [Search Techniques](../../how-to/navigation/search-strategies.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
