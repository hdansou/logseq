---
title: 'Block-Based Editing Philosophy'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: wip-unverified
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Basic Editing Tutorial](../../tutorials/getting-started/03-basic-editing.md)'
  - '[File Organization](./file-organization.md)'
  - '[Graph Visualization](./graph-visualization.md)'
tags: ['blocks', 'outliner', 'philosophy', 'concepts', 'editing']
---

# Block-Based Editing Philosophy

This document explains why Logseq uses block-based editing (outliner approach) instead of traditional document editing. Understanding this philosophy helps you work more effectively with Logseq's design.

**What You'll Understand:**

- Why blocks instead of documents
- The outliner mental model
- Benefits and trade-offs
- When to use blocks effectively
- Comparison with traditional editors
- Historical context

---

## Table of Contents

1. [What Are Blocks?](#what-are-blocks)
2. [The Outliner Model](#the-outliner-model)
3. [Why Blocks Matter](#why-blocks-matter)
4. [Block vs Document Thinking](#block-vs-document-thinking)
5. [Atomic Information Units](#atomic-information-units)
6. [Hierarchical Structure](#hierarchical-structure)
7. [Trade-offs](#trade-offs)
8. [Historical Context](#historical-context)

---

## What Are Blocks?

### Definition

**Block:** A single unit of content in Logseq. Every bullet point is a block.

```markdown
- This is one block ← Block 1
  - This is a child block ← Block 2
    - This is a grandchild block ← Block 3
  - This is another child block ← Block 4
- This is a sibling to the first block ← Block 5
```

**Key Properties:**

```markdown
1. UUID: Every block has a unique identifier (`:block/uuid`)
   Example: 6721a3f0-1b2c-4d5e-8f90-a1b2c3d4e5f6

2. Title: The block's text lives in `:block/title`
   Example: "Meeting notes from Jan 22"

3. Hierarchy: Parent-child relationships (`:block/parent`)
   Example: Parent → Child → Grandchild

4. Ordering: Siblings are ordered by `:block/order`
   Example: a fractional index string, not an integer position

5. Tags: `:block/tags` points at class entities
   Example: a block tagged Task, or a page tagged Project

6. References: Links to/from other blocks (`:block/refs`)
   Example: [[6721a3f0-1b2c-4d5e-8f90-a1b2c3d4e5f6]] or backlinks
```

Every one of those attributes is declared in the DB graph schema
([`schema.cljs:56-108`](../../../../../deps/db/src/logseq/db/frontend/schema.cljs)):
`:block/uuid` at `:61`, `:block/parent` at `:62`, `:block/order` at `:64`,
`:block/page` at `:68`, `:block/refs` at `:71`, `:block/tags` at `:73`,
`:block/title` at `:93`.

> **Note:** In a DB graph the block's text attribute is `:block/title`, not
> `:block/content`. `:block/content` is not in the schema at all — it survives
> only as a plugin-SDK compatibility shim
> ([`sdk/utils.cljs:56`](../../../../../src/main/logseq/sdk/utils.cljs)).

### Block Anatomy

**Visual Structure:**

```markdown
• Block content goes here ← Bullet point
  Property Name: value ← Property (rendered under the block)

- Child block ← Child (indented)
  - Grandchild ← Grandchild (double indent)
```

Properties are not typed as `key:: value` text in a DB graph — that is
file-graph Markdown syntax. You attach a property through the UI, e.g. the
`/Add property` slash command
([`commands.cljs:354-358`](../../../../../src/main/frontend/commands.cljs)),
and it is stored as its own DataScript attribute keyed by the property's
`:db/ident`.

**Technical Structure:**

```clojure
{:db/id 1234
 :block/uuid #uuid "6721a3f0-1b2c-4d5e-8f90-a1b2c3d4e5f6"
 :block/title "Block content goes here"
 :block/parent {:db/id 789}
 :block/page {:db/id 456}
 :block/order "a0"
 :block/refs [{:db/id 123}]
 :block/tags [{:db/id 42}]
 :block/created-at 1737504000000
 :user.property/severity {:db/id 999}}
```

Note the last line: a user property is a **top-level attribute** on the block,
namespaced by its `:db/ident`. There is no `:block/properties` attribute in the
schema, so a `[?b :block/properties ?p]` clause in a `:where` matches nothing.
The key is intercepted at *entity* lookup time, where it synthesises a
`{property-ident value}` map from the entity's own datoms
([`entity_plus.cljc:138-146`](../../../../../deps/db/src/logseq/db/common/entity_plus.cljc),
dispatched at `:162-163`) — so `(:block/properties some-entity)` works, but the
query clause does not.

---

## The Outliner Model

### What Is an Outliner?

**Outliner:** A tool for hierarchical note-taking where every piece of content can be nested, reordered, and referenced independently.

**Historical Outliners:**

```markdown
Classic outliner apps:

- WorkFlowy (2010) - Pure outliner
- Dynalist (2016) - WorkFlowy alternative
- Roam Research (2019) - Outliner + bidirectional links
- Logseq (2021) - Open-source Roam alternative
```

### Core Principles

**1. Everything is a Block**

````markdown
Not just text:

- Text blocks: "This is a thought"
- Code blocks: tagged `Code` (/Code block)
- Quote blocks: tagged `Quote` (/Quote)
- Math blocks: tagged `Math` (/Math block)
- Query blocks: tagged `Query` (/Query, /Advanced Query)
- Asset blocks: tagged `Asset` (images and other uploads)
````

These are not different data structures — each is an ordinary block carrying a
built-in class on `:block/tags`. `Query` (`:54`), `Asset` (`:68`), `Code-block`
(`:76`), `Quote-block` (`:81`) and `Math-block` (`:86`) are all declared in the
same `built-in-classes` map as `Task` and `Template`
([`class.cljs:17-105`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)).
The slash commands that create them live in
[`commands.cljs:246-257`](../../../../../src/main/frontend/commands.cljs)
(code, quote, math) and `:325-326` (query, advanced query).

**2. Deep Nesting**

```markdown
- Level 1
  - Level 2
    - Level 3
      - Level 4
        - Level 5
          - Level 6...
```

Nesting is just a chain of `:block/parent` refs, so the outliner imposes no
depth limit of its own — there is no maximum-depth check anywhere in
[`deps/outliner/`](../../../../../deps/outliner/src/logseq/outliner/). Practical
limits come from rendering and from your own ability to read the result.

**3. Each Block is Addressable**

```markdown
Every block can be:

- Referenced: [[6721a3f0-1b2c-4d5e-8f90-a1b2c3d4e5f6]]
- Linked to: From anywhere
- Embedded: /Node embed
- Queried: Find blocks by properties
- Moved: To any page/location
```

> **References to a block and references to a page are the same construct.**
> A DB graph stores every reference in `:block/title` as `[[<uuid>]]` — even a
> `#Tag`, which `replace-tags-with-id-refs` rewrites to the same `[[<uuid>]]`
> form
> ([`content.cljs:162-179`](../../../../../deps/db/src/logseq/db/frontend/content.cljs)).
> The renderer resolves that UUID to whatever it names, and the `block-reference`
> component simply delegates to `page-reference`
> ([`components/block.cljs:1320-1338`, `:1369-1375`](../../../../../src/main/frontend/components/block.cljs)).
> The file-graph `((uuid))` form is *not* rendered as a reference here: the
> `["Block_ref" id]` branch of `link-cp` returns the literal string
> ([`components/block.cljs:1553-1557`](../../../../../src/main/frontend/components/block.cljs)).

**4. Structure Emerges**

```markdown
Traditional: Plan structure → Write content
Outliner: Write content → Structure emerges

Example:

- Random thought 1
- Random thought 2
- Wait, these are related
  - Move under common parent
  - Structure discovered organically
```

---

## Why Blocks Matter

### 1. Granular References

**Problem with Traditional Editors:**

```markdown
Word/Google Docs approach:

- Link to entire document
- Can't reference specific paragraph
- Must manually scroll to find content

Example:
"See the risk analysis document" ← Where in the doc?
```

**Block-Based Solution:**

```markdown
Logseq approach:

- Reference specific block
- [[uuid]] shows exact content
- Click to navigate directly

Example:
"See the risk: [[6721a3f0-1b2c-4d5e-8f90-a1b2c3d4e5f6]]" ← Exact location
```

You do not type the UUID by hand. "Copy block ref" wraps the block's
`:block/uuid` in page-ref brackets and puts that on the clipboard
([`editor.cljs:3646-3657`](../../../../../src/main/frontend/handler/editor.cljs),
which calls `page-ref/->page-ref`).

### 2. Context Preservation

**Every Block Remembers:**

```markdown
Block created in: [[Daily Journal]]
But referenced from:

- [[Project Plan]]
- [[Risk Register]]
- [[Team Meeting]]

Original context: Always preserved
Current context: Wherever you use it
```

**Practical Example:**

```markdown
Monday journal:

- Met with Sarah about database refactor
  ↑ has :block/uuid 6721a3f0-…-a1b2c3d4e5f6

Tuesday project plan:

- Database work: [[6721a3f0-1b2c-4d5e-8f90-a1b2c3d4e5f6]]
  ↑ Shows full Monday context

Friday review:

- Check progress: [[6721a3f0-1b2c-4d5e-8f90-a1b2c3d4e5f6]]
  ↑ Same note, new context
```

The UUID is not something you assign. Every block gets one at creation —
`:block/uuid` is `:db.unique/identity` in the schema
([`schema.cljs:61`](../../../../../deps/db/src/logseq/db/frontend/schema.cljs)) —
and the editor inserts the bracketed form for you. The file-graph `id::`
property does not exist here.

### 3. Flexible Organization

**Reorganize Without Rewriting:**

```markdown
Initial capture (unorganized):

- Random thought A
- Random thought B
- Random thought C

Later organization (drag-and-drop):

- Topic 1
  - Random thought A
  - Random thought C
- Topic 2
  - Random thought B

Content unchanged, structure refined
```

### 4. Composability

**Build Complex from Simple:**

```markdown
Simple blocks:

- Risk: Database scalability
- Mitigation: Add read replicas
- Owner: Sarah
- Status: In progress

Composed view:

- [[Project Alpha]]
  - /Node embed → Risk: Database scalability
  - /Node embed → Mitigation: Add read replicas
  - /Node embed → Owner: Sarah
  - /Node embed → Status: In progress
```

> **`{{embed}}` is gone.** The macro still parses, but the renderer replaces it
> with a deprecation warning that points you at the slash command:
> `"{{embed}} is deprecated. Use '/Node embed' command instead."`
> ([`components/block.cljs:1917-1918`](../../../../../src/main/frontend/components/block.cljs),
> string at [`en.edn:157`](../../../../../src/resources/dicts/en.edn)).
> `/Node embed` ([`commands.cljs:105-108`, `:233-236`](../../../../../src/main/frontend/commands.cljs))
> opens a block search, and picking a result inserts a new sibling block whose
> `:block/link` points at the chosen block
> ([`components/editor.cljs:290-306`](../../../../../src/main/frontend/components/editor.cljs));
> `:block/link` is the schema attribute described as "which block this block
> links to, used for tag, embeds"
> ([`schema.cljs:76-78`](../../../../../deps/db/src/logseq/db/frontend/schema.cljs)).

---

## Block vs Document Thinking

### Document Mindset

**Traditional Approach:**

```markdown
Document structure (fixed):
├── Introduction
├── Background
│ ├── History
│ └── Context
├── Analysis
│ ├── Method
│ └── Results
└── Conclusion

Characteristics:

- Top-down planning
- Fixed structure
- Sequential reading
- Self-contained
- Publication-oriented
```

**Example Document:**

```markdown
# Project Plan Document

## Introduction

This project aims to...

## Background

Previous attempts...

## Timeline

Q1: Phase 1
Q2: Phase 2
```

### Block Mindset

**Outliner Approach:**

```markdown
Block structure (emergent):

- Thought 1
  - Supporting detail
- Thought 2
  - Evidence
  - Counterpoint
    - Rebuttal
- Synthesis of 1 & 2

Characteristics:

- Bottom-up emergence
- Flexible structure
- Non-linear discovery
- Interconnected
- Thinking-oriented
```

**Example Blocks:**

```markdown
- Project goal: Improve performance
  Deadline: Jun 30, 2026        ← property on the block
  - Why: User complaints about speed
  - Metric: Load time < 2 seconds
- Approach 1: Database optimization
  Owner: [[Sarah]]              ← property on the block
  - Cost: $10k
  - Timeline: 8 weeks
- Approach 2: Caching layer
  Owner: [[John]]               ← property on the block
  - Cost: $5k
  - Timeline: 4 weeks
```

### Key Differences

| Aspect             | Document           | Block/Outliner |
| ------------------ | ------------------ | -------------- |
| **Structure**      | Pre-planned        | Emergent       |
| **Unit**           | Page/section       | Block          |
| **Reference**      | Entire document    | Specific block |
| **Reorganization** | Copy/paste/rewrite | Drag-and-drop  |
| **Context**        | Single             | Multiple       |
| **Reading**        | Sequential         | Network        |
| **Purpose**        | Publication        | Thinking       |

---

## Atomic Information Units

### Atomic Thinking

**Concept:** Each block should contain one coherent idea.

**Good Atomicity:**

```markdown
✅ One idea per block:

- Database is slow (observation)
  - Average query time: 5 seconds (measurement)
  - Affects user experience (consequence)
  - Need to optimize (action)
```

**Poor Atomicity:**

```markdown
❌ Multiple ideas crammed together:

- Database is slow with 5 second queries which affects users
  and we need to optimize by adding indexes and caching and
  maybe also upgrading the server and...
```

### Benefits of Atomicity

**1. Reusability**

```markdown
Atomic block:

- Risk: Database performance
  Severity: High                ← property on the block

Reused in:

- [[Project Plan]] ← Risk section
- [[Team Meeting]] ← Discussion topic
- [[Budget Proposal]] ← Justification
- [[Weekly Review]] ← Tracking
```

**2. Clarity**

```markdown
Clear single idea:

- Hypothesis: Users prefer dark mode
  - Evidence: Survey shows 70% preference
  - Next step: Implement dark theme

Each block = one clear point
Easy to understand
Easy to reference
```

**3. Granular Linking**

```markdown
Link to specific idea:

- Design decision: Use REST not GraphQL
  Rationale: [[6721a3f0-1b2c-4d5e-8f90-a1b2c3d4e5f6]]

Not: "See design document section 3.2.1"
But: Direct reference to exact reasoning
```

### Finding the Right Grain

**Too Coarse:**

```markdown
❌ Everything in one block:

- Project summary: We're building a new feature that will
  improve user engagement by adding social sharing with
  analytics and optimization for mobile users while also...

Problem: Can't reference specific aspects
```

**Too Fine:**

```markdown
❌ Excessive splitting:

- The
  - database
    - is
      - slow

Problem: Meaningless fragments, no context
```

**Just Right:**

```markdown
✅ Balanced granularity:

- Database performance issue
  - Symptom: 5s average query time
  - Impact: Poor user experience
  - Solution: Add caching layer
  - Owner: Sarah

Each block: Meaningful on its own
Together: Complete picture
```

---

## Hierarchical Structure

### Natural Hierarchy

**Hierarchy Represents Relationships:**

```markdown
- Main topic
  - Supporting detail
    - Evidence
    - Example
  - Alternative view
    - Counterargument
      - Rebuttal
  - Synthesis
```

**Practical Example:**

```markdown
- Improve application performance
  Goal: Q2 2026                 ← property on the block

  - Approach 1: Frontend optimization
    - Bundle size reduction
      - Remove unused dependencies
      - Code splitting by route
    - Image optimization
      - Convert to WebP
      - Lazy loading
  - Approach 2: Backend optimization
    - Database queries
      - Add indexes
      - Query optimization
    - Caching strategy
      - Redis for sessions
      - CDN for assets
```

### Hierarchy as Context

**Parent Provides Context:**

```markdown
- [[Project Alpha]] ← Context: Project
  - Meeting 2026-01-22 ← Context: Meeting
    - Sarah proposed new approach ← Context: Discussion
      - Use microservices ← Content: Specific idea
```

**Reading with Context:**

```markdown
Without parent: "Use microservices"
↑ What for? Who said? When?

With hierarchy:
"In Project Alpha meeting on Jan 22,
Sarah proposed we use microservices"
↑ Full context from hierarchy
```

### Multiple Valid Hierarchies

**Same Blocks, Different Organizations:**

**By Time:**

```markdown
- [[2026-01-22]]
  - Project Alpha discussion
    - Technical approach
    - Budget concerns
```

**By Project:**

```markdown
- [[Project Alpha]]
  - Technical approach
    - Discussed 2026-01-22
  - Budget
    - Raised 2026-01-22
```

**By Person:**

```markdown
- [[Person/Sarah]]
  - Contributions
    - Project Alpha approach (2026-01-22)
```

> **A `/` in a page title is not part of the title.** Creating `Person/Sarah`
> creates *two* pages — `Person`, and a page whose `:block/title` is just
> `Sarah` filed under it, via `split-namespace-pages`
> ([`page.cljs:385-390`](../../../../../deps/outliner/src/logseq/outliner/page.cljs)).
> Two different parents can therefore each hold a leaf called `Sarah`
> ([`page_test.cljs:65-72`](../../../../../deps/outliner/test/logseq/outliner/page_test.cljs)).
> There is no `:block/namespace` attribute in the DB schema.

**This is the power of blocks:** Same content, multiple organizational views through references.

---

## Trade-offs

### Advantages

**✅ Flexibility**

```markdown
- Start anywhere
- Reorganize easily
- Multiple structures simultaneously
- Grow organically
```

**✅ Granularity**

```markdown
- Reference exact ideas
- Compose complex from simple
- Context-aware connections
- Atomic information units
```

**✅ Network Thinking**

```markdown
- Non-linear discovery
- Emergent patterns
- Serendipitous connections
- Knowledge graph
```

### Disadvantages

**❌ Learning Curve**

```markdown
Challenge: Different from familiar tools

- Word processors: Linear documents
- Notion: Page-based
- Evernote: Note-based

Logseq: Block-based (new mental model)
```

**❌ Long-Form Writing**

```markdown
Problem: Blocks break flow

- Each block = separate unit
- Hard to write continuous prose
- Bullet points not paragraphs

Use case mismatch:
✅ Good for: Notes, ideas, knowledge work
❌ Poor for: Essays, articles, books
```

**❌ Structure Overhead**

```markdown
Problem: Must maintain hierarchy

- Can become deeply nested
- Reorganization takes time
- Structure decisions constant

Traditional docs:

- Write linearly
- Structure once
- Done

Blocks:

- Write atomically
- Structure continuously
- Never done (evolving)
```

**❌ Export/Publishing**

```markdown
Challenge: Blocks → Traditional format

- Outliner structure ≠ Document structure
- Block references become links
- Hierarchy flattens
- Context can be lost

Publishing requires:

- Conversion/transformation
- Manual restructuring
```

Logseq ships the conversion step: `export-blocks` renders a selection as
Markdown, OPML or HTML
([`components/export.cljs:196-203`, `:275`](../../../../../src/main/frontend/components/export.cljs)),
and the whole graph can go out as a zip (`export-repo-as-zip!`,
[`handler/export.cljs:131`](../../../../../src/main/frontend/handler/export.cljs)),
a debug transit dump (`:135`), a SQLite DB (`:146`) or Roam JSON (`:191`).
What it does not do is restructure an outline into prose — that part is still
yours.

---

## When to Use Blocks

### Ideal Use Cases

**✅ Knowledge Work**

```markdown
- Research notes
- Literature reviews
- Concept mapping
- Learning new topics
```

**✅ Project Management**

```markdown
- Task tracking
- Meeting notes
- Decision logs
- Status updates
```

**✅ Personal Knowledge Management**

```markdown
- Daily journaling
- Idea capture
- Zettelkasten
- Spaced repetition
```

**✅ Brainstorming**

```markdown
- Free-form ideation
- Mind mapping
- Connecting thoughts
- Emergent structure
```

### When to Use Traditional Docs

**❌ Long-Form Writing**

```markdown
Use Word/Docs for:

- Essays
- Articles
- Reports
- Books
- Academic papers
```

**❌ Fixed-Structure Documents**

```markdown
Use templates for:

- Contracts
- Forms
- Official documents
- Formatted publications
```

**❌ Collaborative Writing**

```markdown
Use Docs/Notion for:

- Multiple simultaneous editors
- Comment threads
- Suggestion mode
- Version control (built-in)
```

### Hybrid Approach

**Best of Both:**

```markdown
1. Think in Logseq (blocks)

   - Capture ideas
   - Organize thoughts
   - Build structure
   - Make connections

2. Write in Docs (documents)

   - Export outline from Logseq
   - Write continuous prose
   - Format for publication
   - Collaborate on draft

3. Link back to Logseq
   - Published link in Logseq
   - Maintain source material
   - Future reference
```

---

## Historical Context

### Evolution of Outliners

**1980s: Early Outliners**

```markdown
ThinkTank (1983), MORE (1986):

- Hierarchical text editing
- Collapse/expand nodes
- Desktop applications
- Single-user, local files
```

**2000s: Digital Revolution**

```markdown
OmniOutliner (2001), OneNote (2003):

- Modern interfaces
- Rich content (not just text)
- Sync across devices
- Still document-based thinking
```

**2010s: Web-Based Outliners**

```markdown
WorkFlowy (2010), Dynalist (2016):

- Pure outlining (no documents)
- Web-first, real-time sync
- Everything is bullets
- Infinite nesting
- Zoom in/out

Key innovation: Document = Outline
```

**Late 2010s: Networked Thought**

```markdown
Roam Research (2019):

- Outliner + bidirectional links
- Daily notes
- Block references
- Graph database
- Knowledge network

Paradigm shift: Outliner + Wiki + Graph
```

**2020s: Open Source Era**

```markdown
Logseq (2021), Obsidian (evolved):

- Privacy-first (local-first)
- Open source options
- Plugin ecosystems

Philosophy: Own your data, build your tools
```

### The Move to a Database Graph

Logseq's own history has a second chapter. It began as an outliner over
Markdown files, where the block tree was reconstructed by parsing indentation.
Today's DB graph stores the tree directly: `:block/parent` is a DataScript ref
and sibling order is a fractional-index string on `:block/order`
([`order.cljs`](../../../../../deps/db/src/logseq/db/common/order.cljs), backed
by the `clj-fractional-indexing` dependency at
[`deps/db/deps.edn:14`](../../../../../deps/db/deps.edn)). Fractional indexing is
what lets a drag-and-drop reorder write a single datom instead of renumbering
every sibling.

Graphs live under `~/logseq/graphs`
([`config.cljs:55`](../../../../../deps/common/src/logseq/common/config.cljs)),
and at this revision the graph switcher lists only DB graphs — it filters the
repo list through `config/db-based-graph?` with the comment "use
`config/db-based-graph?` to avoid loading old file graphs"
([`components/repo.cljs:318-323`](../../../../../src/main/frontend/components/repo.cljs)).
The philosophy above is unchanged by that move; the storage format is not the
philosophy.

### Influence from Zettelkasten

**Niklas Luhmann's Method (1950s-1990s):**

```markdown
Principles:

- Atomic notes (one idea per card)
- Unique identifiers (1a, 1a1, 1b...)
- Links between cards
- Emergence over planning

Modern translation:

- Blocks = Atomic notes
- UUIDs = Unique IDs
- Block refs = Card links
- Graph = Zettelkasten network
```

**Key Insight:** Knowledge grows through connections, not hierarchies alone.

---

## Mental Model for Success

### Think in Networks

**Not:** Tree structure (rigid)

```markdown
Root
├── Branch 1
│ └── Leaf
└── Branch 2
└── Leaf
```

**Instead:** Network (flexible)

```markdown
Node 1 ←→ Node 2
↓ ↓
Node 3 ←→ Node 4
↑
Node 5
```

### Embrace Evolution

**Traditional:** Plan → Execute → Done

```markdown
1. Design structure
2. Fill in content
3. Publish
4. Rarely update
```

**Blocks:** Capture → Emerge → Refine → Repeat

```markdown
1. Capture thoughts (no structure yet)
2. Structure emerges from content
3. Refine hierarchy as understanding grows
4. Continuously evolve (living document)
```

### Start Atomic

**Practice:**

```markdown
1. One idea per block
2. Make it self-contained
3. Add context in children
4. Link to related ideas
5. Let structure emerge
```

**Over time, you'll develop intuition for:**

- What makes a good block
- When to split or merge
- How deep to nest
- Where to link

---

## Next Steps

**Develop Block Thinking:**

1. Start with daily notes (low stakes)
2. Practice atomic captures
3. Experiment with hierarchy
4. Try different organizations
5. Build your intuition

**Related:**

- [Basic Editing Tutorial](../../tutorials/getting-started/03-basic-editing.md)
- [Linked References](./linked-references.md)
- [Graph Visualization](./graph-visualization.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
