---
title: 'Zettelkasten Method in Logseq'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Linked References](../../explanation/concepts/linked-references.md)'
  - '[Block Editing Philosophy](../../explanation/concepts/block-editing-philosophy.md)'
  - '[File Organization](../../explanation/concepts/file-organization.md)'
tags: ['zettelkasten', 'knowledge-management', 'note-taking', 'workflow']
---

# Zettelkasten Method in Logseq

Learn how to implement the Zettelkasten ("slip box") knowledge management method in Logseq. This proven system helps you build a network of interconnected ideas for research, writing, and learning.

**What You'll Learn:**

- Core Zettelkasten principles
- How to create atomic notes
- Linking strategies for building knowledge networks
- Processing workflow from capture to permanent notes
- Query patterns for discovering connections

**Time to Implement:** 30-60 minutes initial setup, ongoing practice

---

## Before You Start: how this maps onto a DB graph

The outlines below are plain text so you can read them at a glance. Here is how
the same structure is built in a DB graph:

- **Properties are structured data, not text.** Typing `confidence:: 7` into a
  block leaves a block whose text is `confidence:: 7`. Add properties with the
  `/Add property` slash command (`src/main/frontend/commands.cljs:354-358`).
  Lines such as `Confidence: 8` below are shorthand for "this note carries this
  property", not syntax to type.
- **Property idents are generated, not guessable.** A property you create called
  "Confidence" is stored as `:user.property/Confidence-<8 random chars>`
  (`deps/db/src/logseq/db/frontend/db_ident.cljc:66-90`). The queries below find
  user properties by title.
- **Note types are tags.** This guide tags every note page with one of four
  tags: `Fleeting`, `Literature`, `Permanent` or `Hub`. A page takes tags through
  its **Tags** property (`deps/db/src/logseq/db/frontend/property.cljs:75`), tags
  are matched by the built-in `tags` rule
  (`deps/db/src/logseq/db/frontend/rules.cljc:266-295`), and a tag's properties
  appear on every node that carries it
  (`deps/outliner/src/logseq/outliner/property.cljs:1020-1044`).
- **`A/B/C` page names are split into a page hierarchy.**
  `[[Zettelkasten/Permanent/Concept-Name]]` creates the pages `Zettelkasten`,
  `Permanent` and `Concept-Name`, chained by `:block/parent`; the note's title is
  `Concept-Name` (`deps/outliner/src/logseq/outliner/page.cljs:385-388`;
  `deps/outliner/test/logseq/outliner/page_test.cljs:33-49`).
- **Block refs are `[[<uuid>]]`.** "Copy block ref" in a block's context menu copies the
  block's UUID wrapped as a node reference, e.g. `[[0195b1cb-6b1f-7d70-94b8-6f1b2e4dbb31]]`
  (`src/main/frontend/components/content.cljs:289-291`, via
  `src/main/frontend/util/ref.cljs:5-7`). Typing `((` does not start a block search; it only
  shows "To reference a node, please use `[[]]`."
  (`src/main/frontend/handler/editor.cljs:1949-1952`). Named ids like `id:: key-point-1` do
  not exist in a DB graph. Below, `[[…]]` stands for a copied block ref.
- **Checkboxes are tasks.** `- [ ]` items below have become tasks: `/Todo` sets a
  block's status, which also tags it `#Task`
  (`deps/outliner/src/logseq/outliner/property.cljs:108-116, 205-206`).
- **Queries are blocks, not wrappers.** `#+BEGIN_QUERY` renders a deprecation
  warning (`src/main/frontend/components/block.cljs:5057-5058`), and so does
  `{{query …}}` (`block.cljs:1870-1871`; `src/resources/dicts/en.edn:159`). Use
  `/Query` for a simple query or `/Advanced Query` for a query map
  (`src/main/frontend/commands.cljs:110-117, 125-132, 325-326`). `/Query` turns
  the **whole block text** into the query (`src/main/frontend/handler/editor.cljs:4377-4395`),
  so put any label in a parent block and the query in a child block.
- **Embeds are a slash command.** `{{embed …}}` renders a deprecation warning
  (`block.cljs:1917-1918`; `en.edn:157`); use `/Node embed`
  (`commands.cljs:233-236`; `en.edn:549`).
- **Journal pages are titled like `Jan 22nd, 2026`** by default
  (`deps/db/src/logseq/db/frontend/class.cljs:30-33`), so a link to a day is
  `[[Jan 22nd, 2026]]`.

---

## Table of Contents

1. [Zettelkasten Principles](#zettelkasten-principles)
2. [Setup Your System](#setup-your-system)
3. [Note Types](#note-types)
4. [Capture Workflow](#capture-workflow)
5. [Creating Permanent Notes](#creating-permanent-notes)
6. [Linking Strategy](#linking-strategy)
7. [Queries & Discovery](#queries--discovery)
8. [Writing Projects](#writing-projects)
9. [Best Practices](#best-practices)

---

## Zettelkasten Principles

### Historical Background

**Original System (1950s-1990s):**

```markdown
Niklas Luhmann's Zettelkasten:

- German sociologist
- 90,000+ physical note cards
- 70 books, 400+ articles published
- Attributed success to his system

Key insight:
"I never brainstorm. I only look in my slip box."
```

### Core Principles

**1. Atomicity - One Idea Per Note:**

```markdown
❌ Wrong (too broad):

- [[Machine Learning Overview]]
  - Definition
  - History
  - Types
  - Applications
  - Algorithms

✅ Right (atomic):

- [[Supervised Learning Definition]]
  Supervised learning uses labeled training data...
- [[Supervised vs Unsupervised Learning]]
  Key difference: presence of labels...
- [[Common Supervised Algorithms]]
  Linear regression, decision trees...
```

**2. Connectivity - Link Ideas:**

```markdown
Each note should:

- Link to related concepts
- Reference sources
- Connect to existing notes
- Enable serendipitous discovery

Example:

- [[Neural Networks]]
  - Related: [[Backpropagation]], [[Activation Functions]]
  - Builds on: [[Supervised Learning]]
  - Applied in: [[Image Recognition]]
```

**3. Autonomy - Self-Contained Notes:**

```markdown
Each note should be understandable alone:

✅ Good (self-contained):

- [[Backpropagation]]
  Algorithm for training neural networks by
  computing gradients of loss with respect to
  weights using chain rule.

  Related: [[Gradient Descent]], [[Chain Rule]]

❌ Bad (incomplete):

- [[Backpropagation]]
  See [[Neural Networks]] for details.
```

**4. Emergence - Structure Develops Organically:**

```markdown
Don't plan categories upfront.
Let connections emerge naturally.

Start:

- [[Note A]]
- [[Note B]]

After linking:

- [[Note A]] → [[Common Concept]] ← [[Note B]]
- Pattern emerges
- Create hub page [[Common Concept]]
```

---

## Setup Your System

### Properties Schema

**Define the note tags and their properties once.** There is no Settings →
Properties screen. Create each tag — type `#Permanent` in a block and a new `#`
name is created as a tag (`deps/outliner/src/logseq/outliner/core.cljs:197-220`),
or turn a page into a tag (`src/main/frontend/handler/db_based/page.cljs:37-48`)
— then on the tag's page add the properties
its notes should have with `/Add property`. User property types are `:default`
(text), `:number`, `:date`, `:datetime`, `:checkbox`, `:url`, `:node` and
`:asset` (`deps/db/src/logseq/db/frontend/property/type.cljs:19-21`).

| Tag          | Property       | Type       | Notes                                                    |
| ------------ | -------------- | ---------- | -------------------------------------------------------- |
| `Fleeting`   | —              |            | Quick captures                                           |
| `Literature` | Source type    | `:node`    | Points at a page such as `[[Book]]` or `[[Article]]`     |
| `Literature` | Author         | `:node`    | Points at the author's page                              |
| `Literature` | URL            | `:url`     |                                                          |
| `Permanent`  | Confidence     | `:number`  | 1-10                                                     |
| `Permanent`  | Related topics | `:node`    | Set cardinality to multiple                              |
| `Permanent`  | Reviewed       | `:date`    | Must point at a journal page                             |
| `Hub`        | —              |            | Maps of Content                                          |

A `:date` value must be a journal page (`type.cljs:157-161`), and `:node` is one
of the types whose cardinality can be switched to multiple (`type.cljs:34-36`).

**No `created` property is needed.** Every page and block records
`:block/created-at` in epoch milliseconds when it is created
(`deps/graph-parser/src/logseq/graph_parser/block.cljs:380-383`;
`deps/outliner/src/logseq/outliner/core.cljs:122-123`), and blocks inserted from a
template get fresh timestamps rather than the template's
(`core.cljs:818, 859-862`; `deps/outliner/src/logseq/outliner/op.cljs:283-287`).

### Namespace Structure

**Organize by Type:**

```markdown
Your graph structure:
└── Zettelkasten/
├── Fleeting/ (quick captures)
├── Literature/ (source notes)
├── Permanent/ (processed notes)
└── Hubs/ (MOCs - Maps of Content)

Example pages:

- [[Zettelkasten/Fleeting/2026-01-22-idea]]
- [[Zettelkasten/Literature/Book-Title]]
- [[Zettelkasten/Permanent/Concept-Name]]
- [[Zettelkasten/Hubs/Machine-Learning]]
```

Each of these is split into a chain of pages, and the note's own title is the
last segment — `2026-01-22-idea`, `Book-Title`, `Concept-Name`,
`Machine-Learning` (`page.cljs:385-388`). The namespace is for browsing; the tag
is what the queries below use to tell note types apart.

### Templates

**How templates work in a DB graph.** A template is a **block tagged
`Template`** (`deps/db/src/logseq/db/frontend/class.cljs:100-102`). There is no
`template::` property and no `{{title}}` placeholder.

1. Write the block tree you want to reuse under one top block.
2. Tag the top block `Template` from its tag control. An inline `#Template`
   typed into the text is stripped (`class.cljs:130-140`).
3. To use it: create the note page, tag the page (`Permanent`, say), and type
   `/Template` on the page (`src/main/frontend/commands.cljs:339-343`). Only the
   blocks **under** the template block are inserted
   (`deps/outliner/src/logseq/outliner/op.cljs:239-251`).

**Template variables.** Exactly five are substituted
(`deps/outliner/src/logseq/outliner/template.cljs:104-112`): `<% today %>`,
`<% yesterday %>`, `<% tomorrow %>`, `<% time %>` and `<% current page %>`.

- `today`, `yesterday`, `tomorrow` and `current page` **already insert a page
  ref** — `[[…]]` around the page's UUID (`template.cljs:74-79`;
  `deps/common/src/logseq/common/util/page_ref.cljs:30-33`). Write them bare:
  `<% today %>`, never `[[<% today %>]]`. The journal pages they point at are
  created before insertion if missing (`op.cljs:259-263, 270`).
- Any other name loses its `<% %>` wrapper and is inserted as the bare word, so
  `<% title %>` becomes `title` (`template.cljs:8, 114-128`). No test covers
  unknown names; this follows from the code.

**Fleeting Note Template:**

```markdown
- Fleeting note   #Template
  - Captured <% today %>

    ## Quick Capture

    ## Context

    Where did this come from?

    ## Next Actions

    - Process into permanent note   #Task  (Status: Todo)
    - Connect to existing notes   #Task  (Status: Todo)
```

Use it on a page you have tagged `Fleeting`, for example
`[[Zettelkasten/Fleeting/api-caching-idea]]`.

**Literature Note Template:**

```markdown
- Literature note   #Template
  - Captured <% today %>

    ## Summary

    Brief overview of source

    ## Key Points

    - Point 1
    - Point 2
    - Point 3

    ## Quotes

    > "Important quote here"

    ## My Thoughts

    What do I think about this?

    ## Connected Ideas

    - [[Related Note 1]]
    - [[Related Note 2]]

    ## Permanent Notes to Create

    - Concept A   #Task  (Status: Todo)
    - Concept B   #Task  (Status: Todo)
```

Fill in **Source type**, **Author** and **URL** on the page itself; they are
offered because the page is tagged `Literature`. To cite a key point later, copy
its block ref.

**Permanent Note Template:**

```markdown
- Permanent note   #Template
  - Captured <% today %>

    ## Core Idea

    One clear concept in my own words

    ## Explanation

    Detailed explanation with examples

    ## Why This Matters

    Significance and applications

    ## Sources

    - Literature: [[…]]
    - Literature: [[…]]

    ## Connections

    - Builds on: [[Foundation Concept]]
    - Related to: [[Similar Concept]]
    - Leads to: [[Advanced Concept]]
    - Contrasts with: [[Alternative View]]

    ## Questions & Uncertainties

    - What about edge case X?
    - How does this relate to Y?
```

Set **Confidence** (a number such as `7`) on the page, not `7/10` text.

**Hub Page Template:**

```markdown
- Hub note   #Template
  - ## Overview

    Brief description of this topic area

  - ## Core Concepts

    - [[Permanent Note 1]]
    - [[Permanent Note 2]]
    - [[Permanent Note 3]]

  - ## Related Hubs

    - [[Related Hub 1]]
    - [[Related Hub 2]]

  - ## Recent Notes
    - (an /Advanced Query block with the query below)

  - ## Unanswered Questions

    - Question 1
    - Question 2
```

The query for **Recent Notes** — `Permanent` notes with a block that links to
this hub:

```clojure
{:query [:find (pull ?note [*])
         :in $ ?current-page
         :where
         [?hub :block/name ?current-page]
         [?b :block/refs ?hub]
         [?b :block/page ?note]
         (tags ?note #{"Permanent"})]
 :inputs [:current-page]}
```

`:current-page` resolves, when the query runs, to the lowercased title of the
page the query sits on, which is what `:block/name` holds
(`deps/db/src/logseq/db/frontend/inputs.cljs:71, 84-86`;
`deps/db/src/logseq/db/frontend/schema.cljs:89-90`). The query block travels with
the template because property blocks are copied (`op.cljs:242-243`). Built-in
rules such as `tags` are added automatically when named in `:where`
(`src/main/frontend/worker/handler/query.cljs:185-204`). The old `:title` key is
not rendered for your own queries
(`src/main/frontend/components/query.cljs:166-174`); the **Recent Notes** parent
block is the heading.

---

## Note Types

### Fleeting Notes

**Purpose:** Quick captures

```markdown
When:

- Reading and get an idea
- During conversation
- Random thoughts
- Questions that arise

Lifespan: Temporary (process within 1-2 days)

Example (page Zettelkasten/Fleeting/api-caching-idea, tagged Fleeting):

- Captured [[Jan 22nd, 2026]]

  Maybe we could cache API responses in Redis?
  Would reduce database load.

  Context: Team discussion about performance

  Next: Research Redis caching patterns
```

Processing a fleeting note means turning it into a literature or permanent note
and then deleting it or removing its `Fleeting` tag, so it leaves the queries
below.

### Literature Notes

**Purpose:** Capture source material

```markdown
When:

- Reading book/article
- Watching video
- Attending lecture
- Conversation with expert

Content:

- Summary in your words
- Key points you can cite by block ref
- Direct quotes (with page #)
- Your initial reactions

Example (page Zettelkasten/Literature/Thinking-Fast-and-Slow, tagged Literature):

  Source type: [[Book]]
  Author: [[Person/Daniel-Kahneman]]

- Captured [[Jan 15th, 2026]]

  ## Summary

  Explores two cognitive systems: fast intuitive
  thinking (System 1) and slow deliberate thinking
  (System 2).

  ## Key Points

  - System 1: Automatic, quick, emotional
  - System 2: Deliberate, slow, logical
  - Most decisions use System 1

  ## Quote

  > "System 1 operates automatically and quickly,
  > with little effort and no sense of voluntary
  > control." (p. 20)

  ## My Thoughts

  Explains why I make poor decisions when tired
  (System 2 depleted). Could apply to code reviews.

  ## Permanent Notes to Create

  - [[System 1 vs System 2]]   #Task  (Status: Todo)
  - [[Cognitive Biases from System 1]]   #Task  (Status: Todo)
  - [[Decision Fatigue]]   #Task  (Status: Todo)
```

### Permanent Notes

**Purpose:** Your knowledge in your words

```markdown
Characteristics:

- Written in your own words
- One clear concept
- Self-contained
- Well-connected
- Source references

Lifespan: Forever

Example (page Zettelkasten/Permanent/Cognitive-Load-in-Code-Review, tagged Permanent):

  Confidence: 8

- Captured [[Jan 16th, 2026]]

  ## Core Idea

  Code reviews performed when reviewer is tired
  (high cognitive load) miss more issues because
  System 2 thinking is depleted.

  ## Explanation

  Kahneman's dual-process theory suggests we have
  two thinking systems:

  - System 1: Fast, automatic (pattern matching)
  - System 2: Slow, deliberate (logical analysis)

  Code review requires System 2 for:

  - Logic verification
  - Edge case consideration
  - Security implications

  When tired, System 2 is depleted, so:

  - Reviewer relies on System 1
  - Obvious bugs caught (pattern matching)
  - Subtle logic errors missed

  ## Why This Matters

  Teams should:

  - Schedule reviews during high-energy periods
  - Avoid end-of-day reviews
  - Break large reviews into chunks
  - Consider reviewer workload

  ## Sources

  - [[…]]   ← block ref to "System 1: Automatic, quick, emotional"
  - [[…]]   ← block ref to "System 2: Deliberate, slow, logical"
  - Personal experience: a /Node embed of the incident block

  ## Connections

  - Builds on: [[System 1 vs System 2]]
  - Related to: [[Decision Fatigue]]
  - Applied in: [[Code Review Best Practices]]
  - Similar to: [[Test Writing and Cognitive Load]]

  ## Questions

  - Does pair programming mitigate this?
  - What about AI-assisted review?
```

### Hub Pages (MOCs)

**Purpose:** Maps of Content - organize topic areas

```markdown
When:

- Have 5-10+ notes on topic
- Need overview
- Starting new project
- Want to see connections

Example (page Zettelkasten/Hubs/Machine-Learning, tagged Hub):

- ## Overview

  Collection of notes on machine learning concepts,
  algorithms, and applications.

- ## Foundations

  - [[What is Machine Learning]]
  - [[Supervised vs Unsupervised Learning]]
  - [[Training Data Requirements]]

- ## Algorithms

  - [[Linear Regression]]
  - [[Decision Trees]]
  - [[Neural Networks]]
  - [[Support Vector Machines]]

- ## Advanced Topics

  - [[Deep Learning]]
  - [[Transfer Learning]]
  - [[Ensemble Methods]]

- ## Applications

  - [[Image Recognition]]
  - [[Natural Language Processing]]
  - [[Recommendation Systems]]

- ## Related Hubs

  - [[Zettelkasten/Hubs/Statistics]]
  - [[Zettelkasten/Hubs/Python-Libraries]]

- ## Current Questions

  - How to choose algorithm for specific problem?
  - When is deep learning overkill?
```

---

## Capture Workflow

### Daily Capture

**Morning Review:**

```markdown
5-minute routine:

1. Check fleeting notes from yesterday
2. Process or delete each one
3. Create literature notes for sources
4. Identify permanent notes to create
```

**Query for unprocessed fleeting notes.** Make a block such as "Fleeting notes
to process" and, as its child, a `/Query` block containing:

```clojure
(tags Fleeting)
```

To see only the ones captured recently, use `/Advanced Query` instead:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?since
         :where
         (tags ?b #{"Fleeting"})
         [?b :block/created-at ?created]
         [(>= ?created ?since)]]
 :inputs [:-2d-start]}
```

`:-2d-start` resolves to the epoch-millisecond start of the day two days ago
(`deps/db/src/logseq/db/frontend/inputs.cljs:74, 128-133`), the same unit as
`:block/created-at`. Pass the time as an input rather than computing it inside
`:where`.

### Reading Workflow

**Step-by-Step:**

```markdown
While reading:

1. Highlight/mark interesting points
2. Create fleeting notes for ideas
3. Note page numbers for quotes

After finishing section/chapter:

1. Create literature note
2. Summarize in your words
3. Put each key point in its own block, so it can be cited by block ref
4. Add direct quotes with citations
5. Write initial thoughts

Within 24-48 hours:

1. Review literature note
2. Identify concepts for permanent notes
3. Create permanent notes
4. Link to existing notes
5. Update hub pages
```

**Example Flow:**

```markdown
Reading → Fleeting note (page Zettelkasten/Fleeting/confirmation-bias-example, tagged Fleeting):

- Captured [[Jan 22nd, 2026]]

  People seek information confirming existing beliefs

  Source: Thinking Fast and Slow, p. 81

↓ Process into literature note:

- [[Zettelkasten/Literature/Thinking-Fast-and-Slow]]
  ...
  ## Key Points
  - Confirmation bias: tendency to search for,
    interpret, and recall information confirming
    pre-existing beliefs

↓ Create permanent note (page Zettelkasten/Permanent/Confirmation-Bias, tagged Permanent):

- ## Core Idea

  Confirmation bias is the tendency to favor
  information that confirms existing beliefs while
  dismissing contradictory evidence.

- ## Sources

  - [[…]]   ← block ref to the "Confirmation bias" key point

- ## Connections

  - Example of: [[System 1 Thinking]]
  - Related to: [[Cognitive Biases]]
  - Mitigated by: [[Pre-mortem Analysis]]
```

---

## Creating Permanent Notes

### Process: Literature → Permanent

**Step 1: Extract Core Idea**

```markdown
From literature note, identify one atomic concept.

❌ Too broad:
"All of Kahneman's ideas"

✅ Just right:
"Ego depletion affects decision quality"
```

**Step 2: Write in Your Words**

```markdown
Don't copy/paste from source.
Rewrite to ensure understanding.

Original (source):
"The executive function of System 2 includes
the ability to resist automatic responses."

Your words (permanent note):
"System 2 can override System 1's automatic
reactions, but this requires mental effort
and gets depleted with use."
```

**Step 3: Add Context & Examples**

```markdown
Make concept concrete:

Abstract:
"Anchoring bias affects estimates"

Concrete:
"When asked 'Is the Mississippi longer than 500
miles?', people give lower estimates than when
asked 'Is it longer than 2000 miles?' The initial
number anchors their thinking."
```

**Step 4: Connect to Existing Notes**

```markdown
Ask:

- What does this build on? [[Foundation]]
- What's similar? [[Related Concept]]
- What's opposite? [[Contrasting View]]
- What does this enable? [[Application]]
- What's an example? [[Case Study]]

Add connections section:

## Connections

- Builds on: [[System 1 Thinking]]
- Similar to: [[Priming Effect]]
- Contrasts with: [[Rational Decision Making]]
- Example: [[Amazon Pricing Strategy]]
```

**Step 5: Reference Sources**

```markdown
Link back to literature notes:

## Sources

- [[…]]   ← block ref to the key point
- Book: [[Thinking Fast and Slow]] p. 120
- Article: [[Study on Anchoring]]

Enables:

- Verify claims
- Find original context
- Trace idea development
```

---

## Linking Strategy

### Types of Links

**1. Direct Page Links:**

```markdown
For related concepts:

- [[Neural Networks]]
- [[Backpropagation]]
- [[Activation Functions]]

Creates bidirectional connections
```

The link shows up on the other page under **Linked References** without a
back-link typed by hand; linked references are built from each block's
references (`deps/db/src/logseq/db/common/reference.cljs:71-79`).

**2. Block References:**

```markdown
For specific points:

- Definition: [[…]]
- Example: [[…]]

Points at one block by its UUID
Use /Node embed to show a block and its children inline
```

**3. Relationship Links:**

```markdown
Make relationship explicit:

❌ Just linking:

- [[Concept A]]
- [[Concept B]]

✅ Explain relationship:

- Builds on: [[Foundation Concept]]
  (requires understanding this first)
- Contrasts with: [[Alternative View]]
  (opposite approach to same problem)
- Example of: [[General Principle]]
  (specific instance of broader pattern)
```

### Connection Patterns

**Hub-and-Spoke:**

```markdown
Central hub with spokes:

[[Machine Learning]] (hub)
├── [[Supervised Learning]] (spoke)
├── [[Unsupervised Learning]] (spoke)
├── [[Reinforcement Learning]] (spoke)
└── [[Deep Learning]] (spoke)

Each spoke links back to hub
Hub provides overview
```

**Sequential Chain:**

```markdown
Prerequisite progression:

[[Statistics Basics]]
↓ (requires)
[[Probability Theory]]
↓ (builds on)
[[Statistical Inference]]
↓ (enables)
[[Machine Learning]]

Each note links to previous
Shows learning path
```

**Cluster Network:**

```markdown
Interconnected concepts:

[[Neural Networks]] ←→ [[Backpropagation]]
↕ ↕
[[Loss Functions]] ←→ [[Gradient Descent]]

Each connects to multiple others
Dense connections within topic
```

### Linking Best Practices

**Link Liberally:**

```markdown
✅ Good (many links):

- [[Confirmation Bias]] relates to [[System 1]]
  and occurs when [[Ego Depletion]] is high.
  Mitigate using [[Pre-mortem Analysis]] or
  [[Devil's Advocate]] techniques.

5 links create rich network
```

**Use Different Link Types:**

```markdown
Page links: [[Concept Name]]
→ Overview, definition, main page

Block references: [[…]]
→ Specific point, quote, example

Embeds: /Node embed
→ Show content inline

Properties: Related topics (a :node property)
→ Structured relationships
```

A page named in a `:node` property counts as a reference too, so it also appears
in that page's Linked References
(`deps/outliner/src/logseq/outliner/pipeline.cljs:131-139`).

**Create Missing Links:**

```markdown
While linking, discover missing notes:

Writing: "This is similar to [[Dunning-Kruger Effect]]"
Page doesn't exist → Create it
Ensures completeness
```

---

## Queries & Discovery

Every query below is an `/Advanced Query` block unless it says `/Query`. Put a
label, if you want one, in the parent block.

### Find Related Notes

**By Tag:**

Simple query (`/Query`):

```clojure
(tags Permanent)
```

Advanced equivalent:

```clojure
{:query [:find (pull ?b [*])
         :where
         (tags ?b #{"Permanent"})]}
```

Both also match tags that extend `Permanent`: the rule follows
`:logseq.property.class/extends` (`rules.cljc:266-295`), and the simple query
expands the tag to its children first
(`src/main/frontend/worker/query_dsl.cljs:431-450`).

**By Topic:**

```clojure
{:query [:find (pull ?note [*])
         :in $ ?topic
         :where
         [?t :block/name ?topic]
         [?b :block/refs ?t]
         [?b :block/page ?note]
         (tags ?note #{"Permanent"})]
 :inputs ["[[Machine Learning]]"]}
```

A `"[[Page]]"` string input is turned into the lowercased page name
(`inputs.cljs:157-159`), which is what `:block/name` holds. This finds
`Permanent` notes with at least one block linking to the topic.

### Discover Connections

**Recently Created Notes:**

```clojure
{:query [:find (pull ?b [*])
         :in $ ?since
         :where
         (tags ?b #{"Permanent"})
         [?b :block/created-at ?created]
         [(>= ?created ?since)]]
 :inputs [:-7d-start]
 :result-transform (fn [result]
                     (sort-by (fn [b] (get b :block/created-at)) > result))}
```

`:-7d-start` is the epoch-millisecond start of the day seven days ago
(`inputs.cljs:74, 128-133`). `:result-transform` is called on the pulled block
maps before rows are reduced to UUIDs, and must return a sequence
(`src/main/frontend/worker/handler/render_resource/query.cljs:181-202`).

**Notes Needing Review:**

```clojure
{:query [:find (pull ?b [*])
         :where
         (tags ?b #{"Permanent"})
         [?rp :block/title "Reviewed"]
         [?rp :block/tags :logseq.class/Property]
         [?rp :db/ident ?reviewed]
         (not [?b ?reviewed _])]}
```

The first three `?rp` clauses look the property up by title, the same way the
query DSL does (`src/main/frontend/worker/query_dsl.cljs:343-356`), because its
ident is generated.

**Low Confidence Notes:**

```clojure
{:query [:find (pull ?b [*])
         :where
         (tags ?b #{"Permanent"})
         [?cp :block/title "Confidence"]
         [?cp :block/tags :logseq.class/Property]
         [?cp :db/ident ?confidence]
         [?b ?confidence ?v]
         [?v :logseq.property/value ?n]
         [(< ?n 6)]]}
```

A `:number` property's value is a **ref to a value entity**; the number itself is
in `:logseq.property/value` (`deps/db/src/logseq/db/frontend/property/type.cljs:52-56`;
`rules.cljc:102-106`). There is no `parseInt` step.

### Find Orphan Notes

**Isolated Notes:**

```clojure
{:query [:find (pull ?b [* {:block/_refs [:db/id]}])
         :where
         (tags ?b #{"Permanent"})]
 :result-transform (fn [result]
                     (filter (fn [b] (< (count (get b :block/_refs)) 3))
                             result))}
```

`:block/_refs` pulls the blocks that **link to** each note, so this lists
`Permanent` notes with fewer than three incoming references.

---

## Writing Projects

### From Notes to Draft

**Step 1: Create Project Page**

```markdown
Page [[Projects/Article-on-Decision-Making]]

  Project type: writing
  Status: Backlog
  Deadline: Feb 28th, 2026

- ## Goal

  Write article about improving decision making
  using cognitive science insights.

- ## Target Audience

  Software engineers and managers

- ## Key Points

  - System 1 vs System 2   #Task  (Status: Todo)
  - Ego depletion   #Task  (Status: Todo)
  - Confirmation bias   #Task  (Status: Todo)
  - Practical strategies   #Task  (Status: Todo)
```

`Status` and `Deadline` are the built-in task properties. Status is one of
Backlog, Todo, Doing, In Review, Done or Canceled — "planning" is not a value —
and Deadline is a date-time, not a page link
(`deps/db/src/logseq/db/frontend/property.cljs:313-318, 340-346`). Setting
either on the page also tags it `#Task`
(`deps/outliner/src/logseq/outliner/property.cljs:108-116`).

**Step 2: Gather Relevant Notes**

Under a "Relevant Zettelkasten Notes" block, add an `/Advanced Query` child:

```clojure
{:query [:find (pull ?note [*])
         :in $ ?topic
         :where
         [?t :block/name ?topic]
         [?b :block/refs ?t]
         [?b :block/page ?note]
         (tags ?note #{"Permanent"})]
 :inputs ["[[Decision Making]]"]}
```

```markdown
Review and select:

- [[System 1 vs System 2]]
- [[Ego Depletion]]
- [[Confirmation Bias]]
- [[Cognitive Load in Code Review]]
```

**Step 3: Create Outline**

```markdown
## Outline

I. Introduction

- Problem: Engineers make suboptimal decisions
- Promise: Cognitive science offers solutions

II. How We Think - [[…]] - [[…]]

III. Why We Fail - [[…]] - [[…]]

IV. Practical Strategies - Schedule reviews during high-energy times - Use checklists to reduce System 2 load - Seek disconfirming evidence

V. Conclusion

- Summary
- Call to action
```

**Step 4: Write Draft**

```markdown
Write in daily notes or project page:

- [[Projects/Article-on-Decision-Making]]

  ## Draft (Jan 22nd, 2026)

  ### Introduction

  Every day, software engineers make hundreds of
  decisions: Should I refactor this code? Is this
  architecture solid? Will this scale?

  Most decisions feel intuitive, but cognitive
  science reveals we're prone to systematic errors.
  The good news: understanding how we think helps
  us decide better.

  ### How We Think

  (/Node embed of the System 1 definition block)
  (/Node embed of the System 2 definition block)

  [Continue draft...]
```

**Step 5: Reference Sources**

```markdown
## Sources for Citations

- [[Zettelkasten/Literature/Thinking-Fast-and-Slow]]
- [[Zettelkasten/Literature/Decision-Fatigue-Study]]
- [[Zettelkasten/Literature/Code-Review-Research]]

All permanent notes link back to sources
Easy to add citations
```

---

## Best Practices

### Daily Habits

**Morning (5 min):**

```markdown
1. Process fleeting notes from yesterday
2. Review one old permanent note
3. Update Confidence or add connections
```

**During Reading (ongoing):**

```markdown
1. Create fleeting notes for ideas
2. Capture quotes with page numbers
3. Note connections to existing notes
```

**After Reading (30 min):**

```markdown
1. Create literature note
2. Identify 2-3 concepts for permanent notes
3. Write permanent notes
4. Link to existing notes
5. Update hub page if needed
```

**Weekly Review (30 min):**

Under a "This week's activity" block, add an `/Advanced Query` child:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?since
         :where
         (tags ?b #{"Fleeting" "Literature" "Permanent" "Hub"})
         [?b :block/created-at ?created]
         [(>= ?created ?since)]]
 :inputs [:-7d-start]}
```

```markdown
Review:

- New permanent notes
- New connections discovered
- Hubs that need updating
- Questions to investigate
```

### Quality Over Quantity

```markdown
❌ Goal: Create 10 notes per day
✅ Goal: Create 2-3 well-connected notes per day

Better to have:

- 500 high-quality connected notes
  than
- 5000 isolated unprocessed notes
```

### Progressive Summarization

**Levels of Processing:**

```markdown
Level 1: Fleeting note (original capture)
"Caching might help with API performance"

Level 2: Literature note (source context)
"Redis caching reduces database load by storing
frequently accessed data in memory. Study showed
70% reduction in database queries."

Level 3: Permanent note (your understanding)
"Cache API responses in Redis to reduce database
load. Best for read-heavy endpoints with relatively
static data. Consider TTL based on data freshness
requirements."

Level 4: Application (used in project)
Implemented Redis caching → 65% DB load reduction
Confirmed study findings
Note in [[Redis Caching Pattern]] with results
```

### Regular Maintenance

**Monthly (1 hour):**

```markdown
1. Review hub pages

   - Add new notes
   - Reorganize if structure emerged
   - Archive outdated notes

2. Check orphan notes (the "Isolated Notes" query)

   - Add missing connections
   - Merge duplicates
   - Delete truly irrelevant

3. Update Confidence ratings

   - Notes you now understand better
   - Notes requiring revision
   - Notes needing more research

4. Archive literature notes
   - Move old sources to archive
   - Keep permanent notes active
```

---

## Common Patterns

### Research Project

```markdown
1. Create hub: [[Research/Project-Name]]
2. Gather sources → literature notes
3. Extract concepts → permanent notes
4. Link heavily
5. Let patterns emerge
6. Create outline from connections
7. Write paper referencing notes
```

### Learning New Topic

```markdown
1. Start with broad overview source
2. Create literature note
3. Identify key concepts
4. Create permanent note for each
5. Link to prerequisites
6. Practice explaining in your words
7. Create hub page when have 10+ notes
8. Teach someone using your notes
```

### Idea Development

```markdown
1. Fleeting note with initial idea
2. Connect to existing notes
3. Find gaps in understanding
4. Research gaps → literature notes
5. Deepen understanding → permanent notes
6. Pattern emerges
7. Idea evolves and strengthens
```

---

## Troubleshooting

**"My notes are isolated":**

```markdown
Solution: Link liberally when writing

- While writing, ask: "What does this relate to?"
- Search graph for similar concepts
- Add connection with explanation
- Create missing intermediate notes
```

**"Too many fleeting notes piling up":**

```markdown
Solution: Daily processing habit

- Set 10-minute morning timer
- Process or delete each fleeting note
- Quick decisions, don't overthink
- Delete if not useful (that's okay!)
```

**"Notes too detailed/too brief":**

```markdown
Solution: Test understanding

- Can you explain concept to someone?
- Too brief: You won't understand later
- Too detailed: Takes too long to write
- Sweet spot: Enough for future you
```

**"Unsure what to make permanent note about":**

```markdown
Solution: Ask quality questions

- Does this change how I think?
- Will I reference this in future work?
- Does it connect to existing notes?
- Is it genuinely useful?

If no to all → skip it
```

---

## Example Workflow

**Complete Process:**

```markdown
Monday: Read article about microservices

1. While reading:

   - Create fleeting: "Service boundaries are tricky"
   - Create fleeting: "Communication overhead increases"

2. After reading:

   - Create [[Literature/Microservices-Article]], tagged Literature
   - Key points in their own blocks, citable by block ref
   - Personal thoughts

3. Next day (Tuesday):

   - Process fleeting notes
   - Create [[Permanent/Service-Boundary-Principles]], tagged Permanent
   - Create [[Permanent/Microservices-Trade-offs]], tagged Permanent
   - Link to [[System-Design]], [[Distributed-Systems]]
   - Update [[Hub/Software-Architecture]]

4. Week later:
   - Starting new project, check hub
   - Find [[Permanent/Service-Boundary-Principles]]
   - Inform design decisions
   - Add project experience back to note

Result: Knowledge captured, processed, connected, used
```

---

## Next Steps

**Get Started:**

1. Create the four note tags and their properties, then the templates (30 min)
2. Create first literature note from recent reading (30 min)
3. Extract one permanent note (15 min)
4. Link to existing notes (10 min)
5. Create daily processing habit (5 min/day)

**After 1 Week:**

- 5-10 permanent notes
- Basic connections forming
- Comfortable with workflow

**After 1 Month:**

- 30-50 permanent notes
- Clear patterns emerging
- First hub page needed
- Writing improved

**After 6 Months:**

- 200-300 permanent notes
- Multiple hub pages
- Rich network of ideas
- Original insights emerging

**Related:**

- [Linked References](../../explanation/concepts/linked-references.md)
- [Block Editing](../../explanation/concepts/block-editing-philosophy.md)
- [Research Workflow](./research-workflow.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
