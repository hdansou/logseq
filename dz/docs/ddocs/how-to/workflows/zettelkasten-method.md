---
title: 'Zettelkasten Method in Logseq'
status: 🟢 Stable
version: 'Logseq v0.10.9'
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

**Define Zettelkasten Properties:**

```markdown
Go to Settings → Properties

Add properties:

- note-type:: (fleeting, literature, permanent, hub)
- source:: (book, article, video, conversation, etc.)
- confidence:: (1-10)
- related-topics:: (page links)
- created:: (date)
- reviewed:: (date)
```

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

### Templates

**Fleeting Note Template:**

```markdown
template:: fleeting-note

- [[Zettelkasten/Fleeting/{{title}}]]
  note-type:: fleeting
  created:: [[<% today %>]]
  tags:: #fleeting

  ## Quick Capture

  ## Context

  Where did this come from?

  ## Next Actions

  - [ ] Process into permanent note
  - [ ] Connect to existing notes
```

**Literature Note Template:**

```markdown
template:: literature-note

- [[Zettelkasten/Literature/{{title}}]]
  note-type:: literature
  source-type:: [[Book]] (or Article, Video, etc.)
  author:: [[Person/]]
  created:: [[<% today %>]]
  url::
  tags:: #literature

  ## Summary

  Brief overview of source

  ## Key Points

  - Point 1 id:: key-point-1
  - Point 2 id:: key-point-2
  - Point 3 id:: key-point-3

  ## Quotes

  > "Important quote here"

  ## My Thoughts

  What do I think about this?

  ## Connected Ideas

  - [[Related Note 1]]
  - [[Related Note 2]]

  ## Permanent Notes to Create

  - [ ] Concept A
  - [ ] Concept B
```

**Permanent Note Template:**

```markdown
template:: permanent-note

- [[Zettelkasten/Permanent/{{title}}]]
  note-type:: permanent
  created:: [[<% today %>]]
  confidence:: 7/10
  tags:: #permanent

  ## Core Idea

  One clear concept in my own words

  ## Explanation

  Detailed explanation with examples

  ## Why This Matters

  Significance and applications

  ## Sources

  - Literature: ((literature-note-ref))
  - Literature: ((another-note-ref))

  ## Connections

  - Builds on: [[Foundation Concept]]
  - Related to: [[Similar Concept]]
  - Leads to: [[Advanced Concept]]
  - Contrasts with: [[Alternative View]]

  ## Questions & Uncertainties

  - What about edge case X?
  - How does this relate to Y?
```

**Hub Page Template:**

```markdown
template:: hub-note

- [[Zettelkasten/Hubs/{{title}}]]
  note-type:: hub
  created:: [[<% today %>]]
  tags:: #hub

  ## Overview

  Brief description of this topic area

  ## Core Concepts

  - [[Permanent Note 1]]
  - [[Permanent Note 2]]
  - [[Permanent Note 3]]

  ## Related Hubs

  - [[Related Hub 1]]
  - [[Related Hub 2]]

  ## Recent Notes

  #+BEGIN_QUERY
  {:title "Recent notes in this area"
  :query [:find (pull ?b [*])
  :in $ ?current-page
  :where
  [?b :block/refs ?p]
  [?p :block/name ?current-page]
  [?b :block/properties ?props]
  [(get ?props :note-type) "permanent"]]
  :inputs [:current-page]
  :collapsed? false}
  #+END_QUERY

  ## Unanswered Questions

  - Question 1
  - Question 2
```

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

Example:

- [[Zettelkasten/Fleeting/api-caching-idea]]
  note-type:: fleeting
  created:: [[2026-01-22]]

  Maybe we could cache API responses in Redis?
  Would reduce database load.

  Context: Team discussion about performance

  Next: Research Redis caching patterns
```

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
- Key points with block refs
- Direct quotes (with page #)
- Your initial reactions

Example:

- [[Zettelkasten/Literature/Thinking-Fast-and-Slow]]
  note-type:: literature
  source-type:: [[Book]]
  author:: [[Person/Daniel-Kahneman]]
  created:: [[2026-01-15]]

  ## Summary

  Explores two cognitive systems: fast intuitive
  thinking (System 1) and slow deliberate thinking
  (System 2).

  ## Key Points

  - System 1: Automatic, quick, emotional
    id:: system-1-definition
  - System 2: Deliberate, slow, logical
    id:: system-2-definition
  - Most decisions use System 1
    id:: system-1-dominant

  ## Quote

  > "System 1 operates automatically and quickly,
  > with little effort and no sense of voluntary
  > control." (p. 20)

  ## My Thoughts

  Explains why I make poor decisions when tired
  (System 2 depleted). Could apply to code reviews.

  ## Permanent Notes to Create

  - [ ] [[System 1 vs System 2]]
  - [ ] [[Cognitive Biases from System 1]]
  - [ ] [[Decision Fatigue]]
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

Example:

- [[Zettelkasten/Permanent/Cognitive-Load-in-Code-Review]]
  note-type:: permanent
  created:: [[2026-01-16]]
  confidence:: 8/10

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

  - ((system-1-definition))
  - ((system-2-definition))
  - Personal experience: {{embed ((incident-ref))}}

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

Example:

- [[Zettelkasten/Hubs/Machine-Learning]]
  note-type:: hub

  ## Overview

  Collection of notes on machine learning concepts,
  algorithms, and applications.

  ## Foundations

  - [[What is Machine Learning]]
  - [[Supervised vs Unsupervised Learning]]
  - [[Training Data Requirements]]

  ## Algorithms

  - [[Linear Regression]]
  - [[Decision Trees]]
  - [[Neural Networks]]
  - [[Support Vector Machines]]

  ## Advanced Topics

  - [[Deep Learning]]
  - [[Transfer Learning]]
  - [[Ensemble Methods]]

  ## Applications

  - [[Image Recognition]]
  - [[Natural Language Processing]]
  - [[Recommendation Systems]]

  ## Related Hubs

  - [[Zettelkasten/Hubs/Statistics]]
  - [[Zettelkasten/Hubs/Python-Libraries]]

  ## Current Questions

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

Query for unprocessed fleeting notes:
#+BEGIN_QUERY
{:title "Fleeting notes to process"
:query [:find (pull ?b [*])
:where
[?b :block/properties ?props]
[(get ?props :note-type) "fleeting"]
[?b :block/created-at ?created]
[(- (js/Date.now) ?created) ?age]
[(< ?age 172800000)]] ; 2 days in ms
:collapsed? false}
#+END_QUERY
```

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
3. Extract key points with block IDs
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
Reading → Fleeting note:

- [[Zettelkasten/Fleeting/confirmation-bias-example]]
  note-type:: fleeting
  created:: [[2026-01-22]]

  People seek information confirming existing beliefs

  Source: Thinking Fast and Slow, p. 81

↓ Process into literature note:

- [[Zettelkasten/Literature/Thinking-Fast-and-Slow]]
  ...
  ## Key Points
  - Confirmation bias: tendency to search for,
    interpret, and recall information confirming
    pre-existing beliefs id:: confirmation-bias-point

↓ Create permanent note:

- [[Zettelkasten/Permanent/Confirmation-Bias]]
  note-type:: permanent

  ## Core Idea

  Confirmation bias is the tendency to favor
  information that confirms existing beliefs while
  dismissing contradictory evidence.

  ## Sources

  - ((confirmation-bias-point))

  ## Connections

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

- ((literature-note-key-point))
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

**2. Block References:**

```markdown
For specific points:

- Definition: ((uuid-of-definition))
- Example: ((uuid-of-example))

Embeds original content
Shows updates automatically
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

8 links create rich network
```

**Use Different Link Types:**

```markdown
Page links: [[Concept Name]]
→ Overview, definition, main page

Block references: ((uuid))
→ Specific point, quote, example

Embeds: {{embed ((uuid))}}
→ Show content inline

Properties: related-to:: [[Page]]
→ Structured relationships
```

**Create Missing Links:**

```markdown
While linking, discover missing notes:

Writing: "This is similar to [[Dunning-Kruger Effect]]"
Page doesn't exist → Create it
Ensures completeness
```

---

## Queries & Discovery

### Find Related Notes

**By Tag:**

```clojure
#+BEGIN_QUERY
{:title "All permanent notes"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :note-type) "permanent"]]
 :collapsed? false}
#+END_QUERY
```

**By Topic:**

```clojure
#+BEGIN_QUERY
{:title "Notes about Machine Learning"
 :query [:find (pull ?b [*])
         :in $ ?topic
         :where
         [?b :block/refs ?p]
         [?p :block/name ?topic]
         [?b :block/properties ?props]
         [(get ?props :note-type) "permanent"]]
 :inputs ["machine-learning"]
 :collapsed? false}
#+END_QUERY
```

### Discover Connections

**Recently Created Notes:**

```clojure
#+BEGIN_QUERY
{:title "Recent permanent notes (last 7 days)"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :note-type) "permanent"]
         [?b :block/created-at ?created]
         [(- (js/Date.now) ?created) ?age]
         [(< ?age 604800000)]]  ; 7 days
 :result-transform (fn [result]
                     (sort-by
                       (fn [b]
                         (get-in b [:block/created-at]))
                       > result))
 :collapsed? false}
#+END_QUERY
```

**Notes Needing Review:**

```clojure
#+BEGIN_QUERY
{:title "Notes not reviewed recently"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :note-type) "permanent"]
         (not [(get ?props :reviewed)])]
 :collapsed? false}
#+END_QUERY
```

**Low Confidence Notes:**

```clojure
#+BEGIN_QUERY
{:title "Notes with low confidence"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :confidence) ?conf]
         [(js/parseInt ?conf) ?confNum]
         [(< ?confNum 6)]]
 :collapsed? false}
#+END_QUERY
```

### Find Orphan Notes

**Isolated Notes:**

```clojure
#+BEGIN_QUERY
{:title "Permanent notes with few connections"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :note-type) "permanent"]]
 :result-transform (fn [result]
                     (filter
                       (fn [b]
                         (< (count (get-in b [:block/refs])) 3))
                       result))
 :collapsed? false}
#+END_QUERY
```

---

## Writing Projects

### From Notes to Draft

**Step 1: Create Project Page**

```markdown
- [[Projects/Article-on-Decision-Making]]
  project-type:: writing
  status:: planning
  deadline:: [[2026-02-28]]

  ## Goal

  Write article about improving decision making
  using cognitive science insights.

  ## Target Audience

  Software engineers and managers

  ## Key Points

  - [ ] System 1 vs System 2
  - [ ] Ego depletion
  - [ ] Confirmation bias
  - [ ] Practical strategies
```

**Step 2: Gather Relevant Notes**

```markdown
## Relevant Zettelkasten Notes

#+BEGIN_QUERY
{:title "Notes for article"
:query [:find (pull ?b [*])
:where
[?b :block/refs ?p]
[?p :block/name ?topic]
[?b :block/properties ?props]
[(get ?props :note-type) "permanent"]]
:inputs ["decision-making"]
:collapsed? false}
#+END_QUERY

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

II. How We Think - ((system-1-definition)) - ((system-2-definition))

III. Why We Fail - ((ego-depletion-explanation)) - ((confirmation-bias-explanation))

IV. Practical Strategies - Schedule reviews during high-energy times - Use checklists to reduce System 2 load - Seek disconfirming evidence

V. Conclusion

- Summary
- Call to action
```

**Step 4: Write Draft**

```markdown
Write in daily notes or project page:

- [[Projects/Article-on-Decision-Making]]

  ## Draft (2026-01-22)

  ### Introduction

  Every day, software engineers make hundreds of
  decisions: Should I refactor this code? Is this
  architecture solid? Will this scale?

  Most decisions feel intuitive, but cognitive
  science reveals we're prone to systematic errors.
  The good news: understanding how we think helps
  us decide better.

  ### How We Think

  {{embed ((system-1-definition))}}
  {{embed ((system-2-definition))}}

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
3. Update confidence or add connections
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

```markdown
#+BEGIN_QUERY
{:title "This week's activity"
:query [:find (pull ?b [*])
:where
[?b :block/created-at ?created]
[(- (js/Date.now) ?created) ?age]
[(< ?age 604800000)]]}
#+END_QUERY

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

2. Check orphan notes

   - Add missing connections
   - Merge duplicates
   - Delete truly irrelevant

3. Update confidence ratings

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

   - Create [[Literature/Microservices-Article]]
   - Key points with IDs
   - Personal thoughts

3. Next day (Tuesday):

   - Process fleeting notes
   - Create [[Permanent/Service-Boundary-Principles]]
   - Create [[Permanent/Microservices-Trade-offs]]
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

1. Set up templates (30 min)
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
