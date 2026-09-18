---
title: 'Linked References and Backlinks'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Block Editing Philosophy](./block-editing-philosophy.md)'
  - '[Graph Visualization](./graph-visualization.md)'
  - '[Linking Pages Tutorial](../../tutorials/getting-started/04-linking-pages.md)'
tags: ['links', 'backlinks', 'references', 'bidirectional', 'graph']
---

# Linked References and Backlinks

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

This document explains how linked references and backlinks work in Logseq, why bidirectional linking matters, and how to leverage this powerful feature for knowledge management.

> **Last revised:** 2026-09-11 (verified against `be800f1711`). Source links point at that revision.

**What You'll Understand:**

- Forward links vs backlinks
- Bidirectional linking concept
- Unlinked references
- How Logseq tracks connections
- Reference types and what each one stores
- Practical applications

---

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Forward Links](#forward-links)
3. [Backlinks](#backlinks)
4. [Unlinked References](#unlinked-references)
5. [Bidirectional Thinking](#bidirectional-thinking)
6. [Reference Types](#reference-types)
7. [Practical Applications](#practical-applications)
8. [Best Practices](#best-practices)

---

## Core Concepts

### What Are Links?

**Link:** A connection between two pieces of content.

```markdown
Source block: "See [[Project Alpha]] for details"
↑ This creates a link
Target page: Project Alpha
↑ This receives a backlink
```

**Visual Representation:**

```
Block A → [[Page B]]     Forward link (explicit)
              ↓
        Page B ← A       Backlink (automatic)
```

Only the forward link is stored. When you save a block, Logseq records every node the block points at in the block's `:block/refs` attribute ([`deps/db/src/logseq/db/frontend/schema.cljs:71-72`](../../../../../deps/db/src/logseq/db/frontend/schema.cljs)). A backlink is that same attribute read in reverse: a page's linked references are the blocks whose `:block/refs` contain the page ([`deps/db/src/logseq/db/common/reference.cljs:248-259`](../../../../../deps/db/src/logseq/db/common/reference.cljs)).

### Why Links Matter

**Traditional Documents:**

```markdown
File 1.docx mentions "Project Alpha"
↓
To find references:

- Manual search across all files
- Grep/text search
- Hope you remember all mentions
- No automatic tracking
```

**Logseq Approach:**

```markdown
Page [[Project Alpha]]
↓ Automatically shows:

- Linked references: blocks that link to the page or to one of its aliases
- Unlinked references: blocks whose text mentions the page title
- The page each block lives on, with its parent blocks as a breadcrumb

No manual work required
```

Both sections sit below the page content ([`src/main/frontend/components/page.cljs:521-537`](../../../../../src/main/frontend/components/page.cljs)). References to individual blocks on the page are not listed here: each referenced block carries its own count (see [Block References](#2-block-references)).

---

## Forward Links

### Creating Forward Links

**Page Links:**

```markdown
[[Page Name]] → Links to page
[Display Text]([[Page Name]]) → Links with custom text
```

The labelled form renders your text as a link to the page ([`src/main/frontend/components/block.cljs:1576-1580`](../../../../../src/main/frontend/components/block.cljs)).

> **Removed:** `[[Page Name|Display Text]]`. No handling for a `|` label inside `[[...]]` was found in the source; the labelled form above is the one the renderer handles.

Page names are matched without regard to case: `[[project alpha]]` finds the existing "Project Alpha" page, because the lookup lower-cases the name ([`deps/outliner/src/logseq/outliner/core.cljs:221`](../../../../../deps/outliner/src/logseq/outliner/core.cljs), [`deps/db/src/logseq/db/frontend/entity_util.cljs:107-109`](../../../../../deps/db/src/logseq/db/frontend/entity_util.cljs)). A link to a page that doesn't exist yet creates it when the block is saved ([`deps/outliner/src/logseq/outliner/core.cljs:197-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)).

A title with `/`, such as `Projects/Work/Alpha`, is split into one page per segment under **Library** when you create it with **New page** ([`deps/outliner/src/logseq/outliner/page.cljs:464-467`](../../../../../deps/outliner/src/logseq/outliner/page.cljs)).

> **Unverified:** What a `/` link does when the block is saved without picking **New page**. That save path creates the page without asking for the split ([`deps/outliner/src/logseq/outliner/core.cljs:205-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)).

**Example:**

```markdown
- Working on [[Project Alpha]]
  - Client: [[Acme Corp]]
  - Team: [[Sarah]], [[John]]
```

**This creates 4 forward links** from these blocks to:

1. Project Alpha
2. Acme Corp
3. Sarah
4. John

Logseq stores each link by the target's UUID, so the saved text is `[[<uuid>]]` rather than `[[Project Alpha]]` ([`deps/db/src/logseq/db/frontend/content.cljs:129-130`](../../../../../deps/db/src/logseq/db/frontend/content.cljs)). That is why renaming a page doesn't break its links: saving a new page title only changes the page's own name ([`deps/outliner/src/logseq/outliner/core.cljs:384-394`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)).

### Block References

**Reference Specific Block:**

```markdown
[[<block-uuid>]] → References exact block
```

A block reference uses the same brackets as a page link, with the block's UUID inside. **Copy block ref** on a block's context menu produces it ([`src/main/frontend/components/content.cljs:288-292`](../../../../../src/main/frontend/components/content.cljs); [`src/main/frontend/util/ref.cljs:5-7`](../../../../../src/main/frontend/util/ref.cljs)).

**Example:**

```markdown
Original block (in daily journal):

- Decision: Use PostgreSQL for database

Reference in project plan (pasted after Copy block ref):

- Database choice: [[3f2f9a5c-1a94-4ec7-a57f-31f9f13b5f9d]]
  ↑ Resolves to the original block
```

> **Removed:** `((block-uuid))`, `((block-uuid Display))` and custom `id:: db-decision` block ids. Typing `((` shows the warning "To reference a node, please use `[[]]`." ([`src/main/frontend/handler/editor.cljs:1982-1985`](../../../../../src/main/frontend/handler/editor.cljs)), and `((uuid))` text renders literally ([`src/main/frontend/components/block.cljs:1573-1574`](../../../../../src/main/frontend/components/block.cljs)). `id:: value` typed into a block is text, not a property.

### Embeds

**Embed Content:**

```markdown
/Node embed → search for a block, pick it
```

`/Node embed` inserts a new sibling block whose `:block/link` points at the chosen node ([`src/main/frontend/components/editor.cljs:289-306`](../../../../../src/main/frontend/components/editor.cljs)). The link counts as a reference: `:block/link` is added to the embedding block's `:block/refs` ([`deps/outliner/src/logseq/outliner/pipeline.cljs:118-121`](../../../../../deps/outliner/src/logseq/outliner/pipeline.cljs)).

> **Removed:** `{{embed ((block-uuid))}}` and `{{embed [[Page Name]]}}`. The macro now renders "{{embed}} is deprecated. Use '/Node embed' command instead." ([`src/main/frontend/components/block.cljs:1934-1935`](../../../../../src/main/frontend/components/block.cljs)).

> **Unverified:** Whether `/Node embed` can embed a whole page, and whether an embed shows the embedded block's children.

---

## Backlinks

### What Are Backlinks?

**Backlink:** Automatic reverse reference showing where current page is mentioned.

**Concept:**

```markdown
You create: A → [[B]] (forward link)
Logseq creates: B ← A (backlink, automatic)
```

Nothing extra is written for B. The "Linked references" list is computed from the blocks that reference B, plus the blocks that reference any of B's aliases ([`deps/db/src/logseq/db/common/reference.cljs:269-285`](../../../../../deps/db/src/logseq/db/common/reference.cljs)).

**Example:**

```markdown
Daily journal (Jan 22nd, 2026):

- Meeting about [[Project Alpha]]

Project Alpha page automatically shows:
┌─────────────────────────────────────┐
│ Linked references                   │
├─────────────────────────────────────┤
│ Jan 22nd, 2026                      │
│ - Meeting about [[Project Alpha]]   │
└─────────────────────────────────────┘
```

Journal pages are titled `MMM do, yyyy` by default ([`deps/common/src/logseq/common/util/date_time.cljs:10`](../../../../../deps/common/src/logseq/common/util/date_time.cljs)).

### Finding Backlinks

**Location:** Below the page content ([`src/main/frontend/components/page.cljs:521-529`](../../../../../src/main/frontend/components/page.cljs); label "Linked references", `en.edn:1921`). The section only appears when the page has at least one reference ([`src/main/frontend/components/reference.cljs:51-62`](../../../../../src/main/frontend/components/reference.cljs)).

```markdown
Page: [[Project Alpha]]

[Content of page]

────────────────────────────
Linked references 5          ← blocks that link here, grouped by page

- Jan 22nd, 2026
- Team Meeting Notes
- Budget Proposal

Unlinked references          ← mentions without links

- Jan 15th, 2026
- Email Archive
```

The number next to the heading counts referencing blocks, not pages ([`deps/db/src/logseq/db/common/view.cljs:1089-1091`](../../../../../deps/db/src/logseq/db/common/view.cljs); [`src/main/frontend/components/views.cljs:2695-2702`](../../../../../src/main/frontend/components/views.cljs)). The blocks are grouped under the page they live on ([`src/main/frontend/components/views.cljs:3323-3332`](../../../../../src/main/frontend/components/views.cljs); [`deps/db/src/logseq/db/common/view.cljs:1073-1074`](../../../../../deps/db/src/logseq/db/common/view.cljs)).

Linked references also show when a page is open in the right sidebar, which renders the same page component with `:sidebar? true` ([`src/main/frontend/components/right_sidebar.cljs:59-64`](../../../../../src/main/frontend/components/right_sidebar.cljs)). Unlinked references do not show in the sidebar, on the home view, or on tag and property pages ([`src/main/frontend/components/page.cljs:531-537`](../../../../../src/main/frontend/components/page.cljs)).

On a tag page, blocks tagged with that tag (or one of its child tags) are left out of Linked references ([`deps/db/src/logseq/db/common/reference.cljs:252-255`](../../../../../deps/db/src/logseq/db/common/reference.cljs), `:281-283`). They are listed on the page's **Tagged nodes** tab instead ([`src/main/frontend/components/page.cljs:393-396`](../../../../../src/main/frontend/components/page.cljs); `en.edn:195`).

### Backlink Context

**Context Shown:**

```markdown
Backlink includes:

1. Source page name (the group heading)
2. The parent blocks, as a breadcrumb
3. The referencing block itself

Example backlink display:
┌────────────────────────────────┐
│ Jan 22nd, 2026                 │ ← Source page
│ Morning review > Follow up     │ ← Breadcrumb of parents
│ - [[Project Alpha]] timeline   │ ← The referencing block
└────────────────────────────────┘
```

Referencing blocks that share a parent are shown together under one breadcrumb of that parent chain ([`deps/db/src/logseq/db/common/view.cljs:1075-1087`](../../../../../deps/db/src/logseq/db/common/view.cljs); [`src/main/frontend/components/views.cljs:2844-2853`](../../../../../src/main/frontend/components/views.cljs)). Each one is rendered with the normal block component, not as a text snippet ([`src/main/frontend/components/views.cljs:2161-2174`](../../../../../src/main/frontend/components/views.cljs)).

> **Removed:** "The link (highlighted)" and "Surrounding context" in the display. Only the Unlinked references view passes a highlight term ([`src/main/frontend/components/reference.cljs:69`](../../../../../src/main/frontend/components/reference.cljs)).

### Filtering Backlinks

The section's **Page filter** button opens a list of other pages connected to the referencing blocks, each with a count. The list draws on pages those blocks, their parents and their children link to, plus the pages the blocks sit on ([`src/main/frontend/components/reference.cljs:14-44`](../../../../../src/main/frontend/components/reference.cljs); [`deps/db/src/logseq/db/common/reference.cljs:49-64`](../../../../../deps/db/src/logseq/db/common/reference.cljs); `en.edn:1634`). "Click to include and shift-click to exclude. Click again to remove." (`en.edn:1638`). Includes must all match; any exclude removes a block ([`deps/db/src/logseq/db/common/reference.cljs:122-130`](../../../../../deps/db/src/logseq/db/common/reference.cljs)).

The choices are saved on the page as the **Included references** and **Excluded references** properties, so they persist ([`deps/db/src/logseq/db/frontend/property.cljs:253-258`](../../../../../deps/db/src/logseq/db/frontend/property.cljs); [`deps/db/src/logseq/db/common/reference.cljs:240-246`](../../../../../deps/db/src/logseq/db/common/reference.cljs)). The filter icon takes the success colour with only includes, the error colour with only excludes, and the warning colour with both ([`src/main/frontend/components/reference.cljs:28-39`](../../../../../src/main/frontend/components/reference.cljs)).

---

## Unlinked References

### What Are Unlinked References?

**Unlinked Reference:** Text mention of page name without explicit link syntax.

**Example:**

```markdown
Page name: [[Project Alpha]]

Unlinked mention:
"We discussed Project Alpha in today's meeting"
↑ No [[]] brackets

Logseq detects this and shows in "Unlinked references"
```

Nothing is stored for an unlinked mention. Logseq searches for it when the section is shown.

### How Detection Works

A node counts as an unlinked reference when its text contains the page title, ignoring case ([`deps/db/src/logseq/db/common/reference.cljs:309-318`](../../../../../deps/db/src/logseq/db/common/reference.cljs)). Nodes that already reference the page, embeds, and built-in nodes are skipped (`:319-326`). Matching is a plain substring test, not a word match.

**Matching Rules:**

```markdown
Page name: [[Customer Success]]

✅ Will match:

- "Customer Success team"
- "customer success metrics"
- "CUSTOMER SUCCESS"
- "Customer Successes" (substring)

❌ Won't match:

- "Customer" (partial)
- "Success" (partial)
- "Cust Success" (abbreviated)
- A mention of one of the page's aliases (only the title is searched)
```

### Converting Unlinked References

**Make Explicit:**

```markdown
Before:

- Project Alpha timeline is delayed

After (edit the block and wrap the mention in [[ ]]):

- [[Project Alpha]] timeline is delayed
  ↑ Now an explicit link
```

> **Removed:** The "Link" button. The Unlinked references component renders the matching blocks and has no per-item link action ([`src/main/frontend/components/reference.cljs:64-76`](../../../../../src/main/frontend/components/reference.cljs)).

**Why Convert:**

```markdown
Benefits:

- Drawn in the graph: graph edges come from stored references only
- Appears in Linked (not Unlinked) section
- Survives renaming, because the link is stored by UUID
- Query-friendly

Keep unlinked when:

- Casual mention
- Not semantically important
- Would create noise
```

The graph view builds its page-to-page edges from `:block/refs` ([`src/main/frontend/worker/graph_view.cljs:562-576`](../../../../../src/main/frontend/worker/graph_view.cljs)), so an unlinked mention draws no edge.

---

## Bidirectional Thinking

### Traditional Links (One-Way)

**Web/Wiki Model:**

```markdown
Page A links to Page B
↓
User must:

- Remember that A links to B
- Manually track backlinks
- Search to find references

Example (a static web page):

- An article about "Paris" links to "France"
- The "France" page doesn't list who links to it
- Adding a link back is a separate, manual edit
```

### Bidirectional Links (Two-Way)

**Logseq/Roam Model:**

```markdown
Page A links to Page B
↓ Automatically:

- A shows: forward link to B
- B shows: backlink from A
- Both see the connection

Example (Logseq):

- "Project Alpha" links to "Sarah"
  → Sarah's page shows Project Alpha backlink
  → No manual work
  → Always in sync
```

"Always in sync" holds because references are recalculated from a block's content and properties whenever the block is saved ([`src/main/frontend/worker/pipeline.cljs:45-70`](../../../../../src/main/frontend/worker/pipeline.cljs)).

### Mental Model Shift

**Before (Directory Thinking):**

```markdown
Information organized in hierarchy:
Projects/
├── Work/
│ └── Alpha/
└── Personal/
└── Side Project/

Problem: What if Alpha relates to Side Project?
Must duplicate or choose one location
```

**After (Network Thinking):**

```markdown
Pages exist as nodes:

- [[Project Alpha]]
- [[Side Project]]
- [[Sarah]]
- [[PostgreSQL]]

Links create structure:
[[Project Alpha]]
→ [[Sarah]] (team member)
→ [[PostgreSQL]] (tech used)
→ [[Side Project]] (related work)

Structure emerges from connections
```

---

## Reference Types

### 1. Page Links

**Direct Page Reference:**

```markdown
Syntax: [[Page Name]]
Shows on: Target page's "Linked references"
```

**Example:**

```markdown
Source:

- Discussed [[Project Alpha]] today

Target (Project Alpha page):
┌─────────────────────────────────┐
│ Linked references               │
│ Jan 22nd, 2026                  │
│ - Discussed [[Project Alpha]]   │
└─────────────────────────────────┘
```

### 2. Block References

**Reference Specific Block:**

```markdown
Syntax: [[<block-uuid>]]
Shows on: the referenced block itself, as a reference count
```

A referenced block shows a small count button beside its content ([`src/main/frontend/components/block.cljs:3551-3567`](../../../../../src/main/frontend/components/block.cljs); tooltip "Open block references", `en.edn:132`). Clicking it toggles the block's own linked references underneath it ([`src/main/frontend/components/block.cljs:3569-3575`](../../../../../src/main/frontend/components/block.cljs), `:4655-4657`); shift-click opens them in the right sidebar (`:3561-3565`). The list starts hidden unless the `:ui/auto-expand-block-refs?` config option expands it (`:4314-4317`; [`src/main/frontend/state.cljs:736-738`](../../../../../src/main/frontend/state.cljs)).

A block reference does not appear in the Linked references of the page that holds the block. That list follows references to the page itself ([`deps/db/src/logseq/db/common/reference.cljs:251`](../../../../../deps/db/src/logseq/db/common/reference.cljs)).

**Example:**

```markdown
Original block:

- Key decision: Use REST API

Reference (pasted after Copy block ref):

- Architecture follows [[6a1c0c55-2f0e-4b8e-9d0a-8a4f1f2f3b7e]]

Backlink shows on:

- The original block's reference count
- Can navigate to the referencing block from there
```

### 3. Tags

**Hashtag References:**

```markdown
Syntax: #tag
Shows on: Tag page's "Tagged nodes" tab
```

`#tag` does more than link. It tags the block, and a new tag is created as a class ([`deps/graph-parser/src/logseq/graph_parser/block.cljs:534-537`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs); [`deps/outliner/src/logseq/outliner/core.cljs:197-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)). The tag is also added to the block's references ([`deps/outliner/src/logseq/outliner/pipeline.cljs:118`](../../../../../deps/outliner/src/logseq/outliner/pipeline.cljs)), so queries and the graph see it. The tag page, however, lists tagged blocks under **Tagged nodes** and leaves them out of Linked references ([`deps/db/src/logseq/db/common/reference.cljs:252-255`](../../../../../deps/db/src/logseq/db/common/reference.cljs); [`src/main/frontend/components/page.cljs:393-396`](../../../../../src/main/frontend/components/page.cljs)).

A plain `[[Meeting]]` link to the same page doesn't tag the block, so that block does appear in Linked references.

**Example:**

```markdown
Source:

- This is an important #meeting note

Target (Meeting tag page):
┌──────────────────────────────────┐
│ Tagged nodes                     │
│ - This is an important #meeting  │
│   note                           │
└──────────────────────────────────┘
```

> **Removed:** "Syntax: `#tag` or `[[Tag]]`" as equivalents. File graphs treated them alike; file graphs can no longer be opened ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).

### 4. Property References

**Property Value Links:**

```markdown
Syntax: add a property with /Add property, type "node", value = a page
Shows on: Value page's "Linked references"
```

Add properties with `/Add property` ([`src/main/frontend/commands.cljs:354-358`](../../../../../src/main/frontend/commands.cljs); `en.edn:306`). A property of type `node` holds pages or other nodes ([`deps/db/src/logseq/db/frontend/property/type.cljs:21`](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs)). When a property value is a page or object, it is added to the block's references ([`deps/outliner/src/logseq/outliner/pipeline.cljs:105-116`](../../../../../deps/outliner/src/logseq/outliner/pipeline.cljs)). A user date-time property, and the built-in Scheduled and Deadline properties, reference the journal page for that day (`:62-82`).

**Example:**

```markdown
Source:

- Task description
  (property "project" = Project Alpha)
  (property "owner" = Sarah)

Shows backlinks on:

- Project Alpha page
- Sarah's page
```

> **Removed:** `project:: [[Project Alpha]]` property lines. `key:: value` typed into a block is file-graph syntax and is stored as text, not as a property.

### What Each Type Stores

| Type | Written to | Listed on |
| --- | --- | --- |
| Page link `[[Page]]` | `:block/refs` | Page's Linked references |
| Block reference `[[<uuid>]]` | `:block/refs` | Referenced block's count |
| Tag `#tag` | `:block/tags` and `:block/refs` | Tag page's Tagged nodes |
| Node property value | the property, and `:block/refs` | Value page's Linked references |
| Embed (`/Node embed`) | `:block/link` and `:block/refs` | Embedded block's count |
| Unlinked mention | nothing | Page's Unlinked references |

Sources: [`deps/outliner/src/logseq/outliner/pipeline.cljs:98-121`](../../../../../deps/outliner/src/logseq/outliner/pipeline.cljs) (what goes into `:block/refs`); [`deps/db/src/logseq/db/common/reference.cljs:248-259`](../../../../../deps/db/src/logseq/db/common/reference.cljs), `:309-326` (what each section lists).

> **Unverified:** That an embed shows up in the embedded block's count. `:block/link` is added to `:block/refs` (cited above), and the count reads `:block/refs` ([`deps/db/src/logseq/db/common/initial_data.cljs:255-271`](../../../../../deps/db/src/logseq/db/common/initial_data.cljs)), but this wasn't checked live.

> **Removed:** The "Reference Hierarchy / Strength of Connection" ranking (block reference strongest, unlinked weakest). Logseq doesn't weight references. Every stored reference is the same kind of `:block/refs` value ([`deps/db/src/logseq/db/frontend/schema.cljs:71-72`](../../../../../deps/db/src/logseq/db/frontend/schema.cljs)); the types differ in where they are listed, as the table shows.

---

## Practical Applications

### 1. Project Management

**Hub-and-Spoke Pattern:**

```markdown
Center: [[Project Alpha]]

Spokes (automatically linked via backlinks):

- Jan 22nd, 2026 - Daily updates
- [[Kickoff Meeting]] - Decisions
- [[Sarah]] - Team members
- [[Database Risk]] - Issues
- [[Q1 Budget]] - Resources

Navigate from any spoke → hub
Navigate from hub → any spoke
All connections visible
```

### 2. People Management

**Personal Page:**

```markdown
Page: [[Sarah]]

Linked references show:

- All meetings mentioned
- All projects involved in
- All tasks whose "owner" property is Sarah
- All decisions participated in

Automatic: No manual tracking required
```

**Example:**

```markdown
[[Sarah]]

Skills:

- PostgreSQL
- Python
- Team leadership

────────────────────────────
Linked references 23

Jan 22nd, 2026

- Meeting with [[Sarah]]
  - Discussed database optimization

Project Alpha

- Team lead: [[Sarah]]

Performance Risk

- Owner: [[Sarah]]
```

### 3. Research & Learning

**Concept Mapping:**

```markdown
Central concept: [[Machine Learning]]

Related concepts (via backlinks):

- [[Neural Networks]]
  → Links to Machine Learning
- [[Supervised Learning]]
  → Links to Machine Learning
- [[Deep Learning]]
  → Links to Machine Learning

Each page's backlinks show:

- Where concept is discussed
- Related papers/articles
- Personal notes and insights
```

### 4. Decision Logs

**Tracking Decisions:**

```markdown
Decision page: [[Use REST API]]

Properties (added with /Add property):
- date: Jan 15th, 2026 (a date property)
- context: Project Alpha (a node property)
- decided by: Sarah, John (a node property, multiple values)

Blocks:
- Simpler than GraphQL
- Team has expertise
- Easier to debug

────────────────────────────
Linked references:

Project Alpha → Shows decision context
Architecture Review → Shows discussion
```

A node property value adds a reference to the value page, so the decision shows up in the Linked references of Project Alpha, Sarah and John ([`deps/outliner/src/logseq/outliner/pipeline.cljs:105-116`](../../../../../deps/outliner/src/logseq/outliner/pipeline.cljs)). A `date` property's value is a journal page ([`deps/db/src/logseq/db/frontend/property/type.cljs:157-161`](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs)), so it lands in that day's Linked references too.

**Later references:**

```markdown
[[Implementation Plan]]

- API design follows [[<uuid of the decision block>]]
  ↑ Links to original decision
  ↑ Full context preserved
```

### 5. Literature Notes

**Zettelkasten Style:**

```markdown
Source: [[Attention Is All You Need]]

Key insights:

- Self-attention mechanism
  - Allows parallel processing
  - Better than RNNs for long sequences

Related concepts:

- [[Transformers]]
- [[BERT]]
- [[GPT Architecture]]

────────────────────────────
Linked references:

Shows everywhere this paper influenced:

- NLP System
- Deep Learning
- Jan 22nd, 2026 daily notes
```

To cite the "Self-attention mechanism" block elsewhere, use **Copy block ref** on it and paste the `[[<uuid>]]`.

---

## Best Practices

### 1. Link Liberally

**Philosophy:** When in doubt, link it.

```markdown
✅ Good (many links):

- Meeting with [[Sarah]] about [[Project Alpha]]
  discussed [[Database Performance]] and decided
  to use [[Redis]] for [[Caching Strategy]].

More links = richer context
More backlinks = better discovery
```

**Benefits:**

```markdown
- Future self thanks you
- Serendipitous discoveries
- Automatic context building
- Graph reveals patterns
```

### 2. Create Hub Pages

**Hub:** Page that serves as central reference.

```markdown
[[Project Alpha]] (hub page)

Content:

- Overview
- Key links:
  - [[Kickoff Meeting]]
  - [[Risk Register]]
  - [[Timeline]]
  - [[Team Members]]

Linked references:

- All daily mentions
- All related tasks
- All decisions
- All discussions

Result: One-stop reference
```

### 3. Use Consistent Naming

**Consistency Matters:**

```markdown
✅ Consistent:

- [[Project Alpha]] (everywhere)
- [[Sarah Smith]] (everywhere)

❌ Inconsistent:

- [[Project Alpha]]
- [[Alpha Project]]
- [[Proj. Alpha]]

Result: Split references, broken graph
```

Capitalisation alone doesn't split references: `[[project alpha]]` resolves to the same page as `[[Project Alpha]]` ([`deps/db/src/logseq/db/frontend/entity_util.cljs:107-109`](../../../../../deps/db/src/logseq/db/frontend/entity_util.cljs)).

When a page really does go by two names, give it the built-in **Alias** property ([`deps/db/src/logseq/db/frontend/property.cljs:68-74`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)). Blocks that link to an alias are included in the page's Linked references ([`deps/db/src/logseq/db/common/reference.cljs:275`](../../../../../deps/db/src/logseq/db/common/reference.cljs), `:284-285`), and the alias relation works in both directions ([`deps/db/src/logseq/db/frontend/rules.cljc:21-25`](../../../../../deps/db/src/logseq/db/frontend/rules.cljc)). Unlinked references still search only the page's own title.

### 4. Leverage Unlinked References

**Review Regularly:**

```markdown
Monthly practice:

1. Open important hub pages
2. Review "Unlinked references"
3. Convert relevant mentions to links
4. Strengthens connections
5. Discovers patterns
```

### 5. Property Links for Structure

**Structured Relationships:**

```markdown
- Task description
  (property "project" = Project Alpha, type node)
  (property "owner" = Sarah, type node)
  (built-in Status = Doing)
  (built-in Priority = High)

Benefits:

- Queryable
- Filterable
- Structured backlinks
```

Status is a built-in property with fixed choices (Backlog, Todo, Doing, In Review, Done, Canceled), not a page you link to ([`deps/db/src/logseq/db/frontend/property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

> **Unverified:** Whether picking a Status or Priority choice adds a reference to that choice. Choices are stored as entities, but whether they pass the page-or-object test in `pipeline.cljs:84-92` wasn't traced.

---

## Common Patterns

### MOC (Map of Content)

**Index Page Pattern:**

```markdown
[[Projects Index]]

Active:

- [[Project Alpha]]
- [[Project Beta]]
- [[Project Gamma]]

Completed:

- [[Project Legacy]]

One click from here to each project's page and its backlinks
```

> **Removed:** "All project backlinks visible here". An index page's Linked references list the blocks that link to the index page, not the backlinks of the pages it lists ([`deps/db/src/logseq/db/common/reference.cljs:248-259`](../../../../../deps/db/src/logseq/db/common/reference.cljs)).

### Daily Notes Aggregation

**Time-Based Hub:**

```markdown
Jan 22nd, 2026

Mentions today:

- [[Project Alpha]] → See backlink
- [[Sarah]] → See backlink
- [[Meeting Notes]] → See backlink

Each page shows today's context via backlink
```

### Concept Network

**Learning Graph:**

```markdown
[[Machine Learning]]
↓ Links to
[[Neural Networks]]
↓ Links to
[[Backpropagation]]
↓ Links to
[[Gradient Descent]]

Backlinks show:

- Learning progression
- Related papers
- Personal insights
```

---

## Troubleshooting

**Problem: Too many backlinks (noise)**

```markdown
Solution:

- Use the Page filter to include or exclude source pages
- More specific page names
- Tag blocks instead of linking the tag page: on a tag
  page, tagged blocks move to "Tagged nodes"
- Use properties for structure
```

> **Removed:** "Use namespaces: `[[Projects/Alpha]]` not `[[Alpha]]`" and "Archive old content". A `/` title is split into separate pages (see [Forward Links](#creating-forward-links)), and no archive mechanism affecting references was found.

**Problem: Missing expected backlinks**

```markdown
Check:

- Spelling of page name (capitalisation doesn't matter)
- Link syntax: [[Page]] not [Page]
- ((uuid)) is plain text, not a reference; use [[uuid]]
- A reference to one block shows on that block's count,
  not in the page's Linked references
- On a tag page, tagged blocks are under "Tagged nodes"
- A Page filter is active (the filter icon is coloured)
```

> **Removed:** "Wait for indexing (refresh)". References are recalculated in the same save that changes the block ([`src/main/frontend/worker/pipeline.cljs:45-70`](../../../../../src/main/frontend/worker/pipeline.cljs)); there is no separate reference index.

**Problem: Unlinked references not showing**

```markdown
Reasons:

- You are in the right sidebar, on the home view, or on
  a tag or property page, where the section is hidden
- The mention doesn't contain the whole title
- The block already links to the page
- The mention uses an alias, not the page title

Solutions:

- Open the page itself
- Mention the full page title
- Rebuild the search index (see below)
```

Whether the section appears at all is decided by a full-text search: Logseq takes up to 100 search hits for the page title and shows the section if one of them contains the title and doesn't already link the page ([`src/main/frontend/worker/handler/block.cljs:620-630`](../../../../../src/main/frontend/worker/handler/block.cljs)). A stale search index can therefore hide the section. **Rebuild search index** (`en.edn:414`; shortcut `mod+c mod+s`, [`src/main/frontend/modules/shortcut/config.cljs:337-338`](../../../../../src/main/frontend/modules/shortcut/config.cljs)) refreshes it.

> **Removed:** "Special characters in name", "Very short names (<3 chars)" and "Re-index graph". Matching is a plain substring test with no length rule ([`deps/db/src/logseq/db/common/reference.cljs:313-317`](../../../../../deps/db/src/logseq/db/common/reference.cljs)), and the only re-index command found rebuilds the search index.

---

## Next Steps

**Master Linked References:**

1. Practice linking in daily notes
2. Create hub pages for key topics
3. Review backlinks regularly
4. Convert unlinked references
5. Build your knowledge graph

**Related:**

- [Block Editing Philosophy](./block-editing-philosophy.md)
- [Graph Visualization](./graph-visualization.md)
- [Linking Pages Tutorial](../../tutorials/getting-started/04-linking-pages.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
