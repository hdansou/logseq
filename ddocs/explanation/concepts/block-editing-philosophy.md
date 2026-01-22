---
title: 'Block-Based Editing Philosophy'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Block Editing Tutorial](../../tutorials/getting-started/block-editing.md)'
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
1. UUID: Every block has unique identifier
   Example: abc-123-def-456-ghi

2. Content: Text, images, embeds, properties
   Example: "Meeting notes from Jan 22"

3. Hierarchy: Parent-child relationships
   Example: Parent → Child → Grandchild

4. Properties: Metadata attached to block
   Example: id:: custom-id, tags:: [[Project]]

5. References: Links to/from other blocks
   Example: ((block-ref)) or backlinks
```

### Block Anatomy

**Visual Structure:**

```markdown
• Block content goes here ← Bullet point
property:: value ← Property (indented)

- Child block ← Child (indented)
  - Grandchild ← Grandchild (double indent)
```

**Technical Structure:**

```clojure
{:block/uuid #uuid "abc-123-def-456"
 :block/content "Block content goes here"
 :block/parent {:db/id 789}
 :block/page {:db/id 456}
 :block/properties {:property "value"}
 :block/refs [{:db/id 123}]
 :block/created-at 1737504000000}
```

---

## The Outliner Model

### What Is an Outliner?

**Outliner:** A tool for hierarchical note-taking where every piece of content can be nested, reordered, and referenced independently.

**Historical Outliers:**

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
- Code blocks: `javascript ...`
- Image blocks: ![](image.png)
- Embed blocks: {{embed [[Page]]}}
- Query blocks: #+BEGIN_QUERY ...
````

**2. Infinite Nesting**

```markdown
- Level 1
  - Level 2
    - Level 3
      - Level 4
        - Level 5
          - Level 6... (unlimited depth)
```

**3. Each Block is Addressable**

```markdown
Every block can be:

- Referenced: ((block-ref))
- Linked to: From anywhere
- Embedded: {{embed ((block-ref))}}
- Queried: Find blocks by properties
- Moved: To any page/location
```

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
- ((block-ref)) shows exact content
- Click to navigate directly

Example:
"See the risk: ((abc-123))" ← Exact location
```

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
  id:: db-refactor-note

Tuesday project plan:

- Database work: ((db-refactor-note))
  ↑ Shows full Monday context

Friday review:

- Check progress: ((db-refactor-note))
  ↑ Same note, new context
```

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
  - {{embed ((risk-block))}}
  - {{embed ((mitigation-block))}}
  - {{embed ((owner-block))}}
  - {{embed ((status-block))}}
```

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
  deadline:: Q2 2026
  - Why: User complaints about speed
  - Metric: Load time < 2 seconds
- Approach 1: Database optimization
  owner:: [[Sarah]]
  - Cost: $10k
  - Timeline: 8 weeks
- Approach 2: Caching layer
  owner:: [[John]]
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
  severity:: high

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
  rationale:: ((simplicity-argument))

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
  goal:: Q2 2026

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
- Separate publishing tool
```

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
- Multiple graph modes
- Standards-based (Markdown)

Philosophy: Own your data, build your tools
```

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

- [Block Editing Tutorial](../../tutorials/getting-started/block-editing.md)
- [Linked References](./linked-references.md)
- [Graph Visualization](./graph-visualization.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
