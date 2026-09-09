---
title: 'Linked References and Backlinks'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Block Editing Philosophy](./block-editing-philosophy.md)'
  - '[Graph Visualization](./graph-visualization.md)'
  - '[Page Links Tutorial](../../tutorials/getting-started/page-links.md)'
tags: ['links', 'backlinks', 'references', 'bidirectional', 'graph']
---

# Linked References and Backlinks

This document explains how linked references and backlinks work in Logseq, why bidirectional linking matters, and how to leverage this powerful feature for knowledge management.

**What You'll Understand:**

- Forward links vs backlinks
- Bidirectional linking concept
- Unlinked references
- How Logseq tracks connections
- Reference types and hierarchy
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

- All explicit links ([[Project Alpha]])
- All block references
- All unlinked mentions
- Full context for each

No manual work required
```

---

## Forward Links

### Creating Forward Links

**Page Links:**

```markdown
[[Page Name]] → Links to page
[[Projects/Work/Alpha]] → Links to nested page
[[Page Name|Display Text]] → Links with custom text
```

**Example:**

```markdown
- Working on [[Project Alpha]]
  - Client: [[Company/Acme Corp]]
  - Team: [[Person/Sarah]], [[Person/John]]
  - Status: [[Status/In Progress]]
```

**This creates 5 forward links** from current block to:

1. Project Alpha
2. Company/Acme Corp
3. Person/Sarah
4. Person/John
5. Status/In Progress

### Block References

**Reference Specific Block:**

```markdown
((block-uuid)) → References exact block
((block-uuid Display)) → Custom display text
```

**Example:**

```markdown
Original block (in daily journal):

- Decision: Use PostgreSQL for database
  id:: db-decision

Reference in project plan:

- Database choice: ((db-decision))
  ↑ Shows full original block content
```

### Embeds

**Embed Content:**

```markdown
{{embed ((block-uuid))}} → Embeds block content
{{embed [[Page Name]]}} → Embeds entire page
```

**Example:**

```markdown
- Project requirements:
  {{embed [[Requirements/Core Features]]}}
  ↑ Shows full page content inline
```

---

## Backlinks

### What Are Backlinks?

**Backlink:** Automatic reverse reference showing where current page is mentioned.

**Concept:**

```markdown
You create: A → [[B]] (forward link)
Logseq creates: B ← A (backlink, automatic)
```

**Example:**

```markdown
Daily journal (2026-01-22):

- Meeting about [[Project Alpha]]
  - Discussed timeline
  - Reviewed budget

Project Alpha page automatically shows:
┌─────────────────────────────────────┐
│ Linked References │
├─────────────────────────────────────┤
│ • [[2026-01-22]] │
│ - Meeting about [[Project Alpha]] │
│ - Discussed timeline │
│ - Reviewed budget │
└─────────────────────────────────────┘
```

### Finding Backlinks

**Location:** Bottom of every page

```markdown
Page: [[Project Alpha]]

[Content of page]

────────────────────────────
Linked References (5) ← All pages that link here

- [[2026-01-22]]
- [[Team Meeting Notes]]
- [[Budget Proposal]]
- [[Risk Register]]
- [[Weekly Review]]

Unlinked References (2) ← Mentions without links

- [[2026-01-15]]
- [[Email Archive]]
```

### Backlink Context

**Full Context Shown:**

```markdown
Backlink includes:

1. Source page name
2. Full parent hierarchy
3. Original block content
4. Surrounding context

Example backlink display:
┌────────────────────────────────┐
│ [[2026-01-22]] │ ← Source page
│ - Morning review │ ← Parent block
│ - TODO Follow up on │ ← Grandparent
│ [[Project Alpha]] │ ← The link (highlighted)
│ - Sarah mentioned │ ← Child context
│ timeline concerns │
└────────────────────────────────┘
```

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

Logseq detects this and shows in "Unlinked References"
```

### How Detection Works

**Matching Rules:**

```markdown
Page name: [[Customer Success]]

✅ Will match:

- "Customer Success team"
- "customer success metrics"
- "CUSTOMER SUCCESS"

❌ Won't match:

- "Customer" (partial)
- "Success" (partial)
- "Cust Success" (abbreviated)
```

### Converting Unlinked References

**Make Explicit:**

```markdown
Before:

- Project Alpha timeline is delayed

After (click "Link" button):

- [[Project Alpha]] timeline is delayed
  ↑ Now an explicit link
```

**Why Convert:**

```markdown
Benefits:

- More visible in graph
- Appears in Linked (not Unlinked) section
- Stronger semantic connection
- Query-friendly

Keep unlinked when:

- Casual mention
- Not semantically important
- Would create noise
```

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

Example (Wikipedia):

- "Paris" article links to "France"
- "France" article must manually link back
- No automatic "mentioned in" section
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
- [[Technology/PostgreSQL]]

Links create structure:
[[Project Alpha]]
→ [[Sarah]] (team member)
→ [[Technology/PostgreSQL]] (tech used)
→ [[Side Project]] (related work)

Structure emerges from connections
```

---

## Reference Types

### 1. Page Links

**Direct Page Reference:**

```markdown
Syntax: [[Page Name]]
Shows on: Target page's "Linked References"
```

**Example:**

```markdown
Source:

- Discussed [[Project Alpha]] today

Target (Project Alpha page):
┌─────────────────────────────────┐
│ Linked References │
│ • From [[2026-01-22]] │
│ - Discussed [[Project Alpha]] │
└─────────────────────────────────┘
```

### 2. Block References

**Reference Specific Block:**

```markdown
Syntax: ((block-uuid))
Shows on:

- Block's page (Linked References)
- Block's own reference list
```

**Example:**

```markdown
Original block:

- Key decision: Use REST API
  id:: rest-decision

Reference:

- Architecture follows ((rest-decision))

Backlink shows on:

- Current page's Linked References
- Can navigate to original
```

### 3. Tags

**Hashtag References:**

```markdown
Syntax: #tag or [[Tag]]
Shows on: Tag page's Linked References
```

**Example:**

```markdown
Source:

- This is an important #meeting note

Target (#meeting page):
┌──────────────────────────────────┐
│ Linked References │
│ • From [[2026-01-22]] │
│ - Important #meeting note │
└──────────────────────────────────┘
```

### 4. Property References

**Property Value Links:**

```markdown
Syntax: property:: [[Value]]
Shows on: Value page's Linked References
```

**Example:**

```markdown
Source:

- Task description
  project:: [[Project Alpha]]
  owner:: [[Sarah]]

Shows backlinks on:

- Project Alpha page
- Sarah's page
```

### Reference Hierarchy

**Strength of Connection:**

```markdown
Strongest:

1. Block reference ((uuid))
   → Exact, immutable

2. Page link [[Page]]
   → Semantic, explicit

3. Property reference
   → Structured metadata

4. Tag #tag
   → Categorization

5. Unlinked reference
   → Casual mention
   Weakest
```

---

## Practical Applications

### 1. Project Management

**Hub-and-Spoke Pattern:**

```markdown
Center: [[Project Alpha]]

Spokes (automatically linked via backlinks):

- [[2026-01-22]] - Daily updates
- [[Meeting/Kickoff]] - Decisions
- [[Person/Sarah]] - Team members
- [[Risk/Database]] - Issues
- [[Budget/Q1]] - Resources

Navigate from any spoke → hub
Navigate from hub → any spoke
All connections visible
```

### 2. People Management

**Personal Page:**

```markdown
Page: [[Person/Sarah]]

Linked References show:

- All meetings mentioned
- All projects involved in
- All tasks assigned
- All decisions participated in

Automatic: No manual tracking required
```

**Example:**

```markdown
[[Person/Sarah]]

Skills:

- PostgreSQL
- Python
- Team leadership

────────────────────────────
Linked References (23):

[[2026-01-22]]

- Meeting with [[Person/Sarah]]
  - Discussed database optimization

[[Project Alpha]]

- Team lead: [[Person/Sarah]]

[[Risk/Performance]]

- Owner: [[Person/Sarah]]
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
Decision page: [[Decision/Use REST API]]

date:: [[2026-01-15]]
context:: [[Project Alpha]] redesign
decided-by:: [[Person/Sarah]], [[Person/John]]
rationale::

- Simpler than GraphQL
- Team has expertise
- Easier to debug

────────────────────────────
Linked References:

[[Project Alpha]] → Shows decision context
[[Architecture Review]] → Shows discussion
[[2026-01-15]] → Shows when decided
```

**Later references:**

```markdown
[[Implementation Plan]]

- API design follows ((rest-api-decision))
  ↑ Links to original decision
  ↑ Full context preserved
```

### 5. Literature Notes

**Zettelkasten Style:**

```markdown
Source: [[Paper/Attention Is All You Need]]

Key insights:

- Self-attention mechanism
  id:: attention-insight
  - Allows parallel processing
  - Better than RNNs for long sequences

Related concepts:

- [[Transformers]]
- [[BERT]]
- [[GPT Architecture]]

────────────────────────────
Linked References:

Shows everywhere this paper influenced:

- [[Project/NLP System]]
- [[Learning/Deep Learning]]
- [[2026-01-22]] daily notes
```

---

## Best Practices

### 1. Link Liberally

**Philosophy:** When in doubt, link it.

```markdown
✅ Good (many links):

- Meeting with [[Person/Sarah]] about [[Project Alpha]]
  discussed [[Risk/Database Performance]] and decided
  to use [[Technology/Redis]] for [[Caching Strategy]].

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
  - [[Meeting/Kickoff]]
  - [[Risk Register]]
  - [[Timeline]]
  - [[Team Members]]

Linked References:

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
- [[Person/Sarah Smith]] (everywhere)

❌ Inconsistent:

- [[Project Alpha]]
- [[Alpha Project]]
- [[project alpha]]
- [[Proj. Alpha]]

Result: Split references, broken graph
```

### 4. Leverage Unlinked References

**Review Regularly:**

```markdown
Monthly practice:

1. Open important hub pages
2. Review "Unlinked References"
3. Convert relevant mentions to links
4. Strengthens connections
5. Discovers patterns
```

### 5. Property Links for Structure

**Structured Relationships:**

```markdown
- Task description
  project:: [[Project Alpha]]
  owner:: [[Person/Sarah]]
  status:: [[Status/In Progress]]
  priority:: [[Priority/High]]

Benefits:

- Queryable
- Filterable
- Structured backlinks
```

---

## Common Patterns

### MOC (Map of Content)

**Index Page Pattern:**

```markdown
[[MOC/Projects]]

Active:

- [[Project Alpha]]
- [[Project Beta]]
- [[Project Gamma]]

Completed:

- [[Project Legacy]]

All project backlinks visible here
```

### Daily Notes Aggregation

**Time-Based Hub:**

```markdown
[[2026-01-22]]

Mentions today:

- [[Project Alpha]] → See backlink
- [[Person/Sarah]] → See backlink
- [[Meeting Notes]] → See backlink

Each page shows today's context via backlink
```

### Concept Network

**Learning Graph:**

```markdown
[[Concept/Machine Learning]]
↓ Links to
[[Concept/Neural Networks]]
↓ Links to  
[[Concept/Backpropagation]]
↓ Links to
[[Concept/Gradient Descent]]

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

- More specific page names
- Use namespaces: [[Projects/Alpha]] not [[Alpha]]
- Archive old content
- Use properties for structure
```

**Problem: Missing expected backlinks**

```markdown
Check:

- Spelling of page name
- Case sensitivity (shouldn't matter, but check)
- Link syntax: [[Page]] not [Page]
- Wait for indexing (refresh)
```

**Problem: Unlinked references not showing**

```markdown
Reasons:

- Partial word match (needs exact)
- Special characters in name
- Very short names (<3 chars)
- Indexing delay

Solutions:

- Exact name match
- Simplify page names
- Re-index graph
```

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
- [Page Links Tutorial](../../tutorials/getting-started/page-links.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
