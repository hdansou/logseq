---
title: 'Research Workflow'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Zettelkasten Method](./zettelkasten-method.md)'
  - '[Query Patterns](../../reference/advanced/query-patterns.md)'
  - '[PDF Annotations](../../how-to/features/pdf-annotations.md)'
tags: ['research', 'workflow', 'academic', 'literature-review']
---

# Research Workflow

Learn how to conduct academic and professional research in Logseq. This workflow takes you from literature discovery to synthesis and publication, maintaining full traceability throughout.

**What You'll Learn:**

- Research project setup
- Literature management
- Note-taking from sources
- Synthesis and analysis
- Writing and citation
- Collaboration workflows

**Time to Setup:** 30-60 minutes

---

## Table of Contents

1. [Research Project Setup](#research-project-setup)
2. [Literature Discovery](#literature-discovery)
3. [Source Management](#source-management)
4. [Reading & Annotation](#reading--annotation)
5. [Note-Taking System](#note-taking-system)
6. [Synthesis & Analysis](#synthesis--analysis)
7. [Writing Process](#writing-process)
8. [Citation Management](#citation-management)
9. [Collaboration](#collaboration)
10. [Best Practices](#best-practices)

---

## Research Project Setup

### Create Project Structure

**Project Hub Page:**

```markdown
- [[Research/Thesis-Machine-Learning-Ethics]]
  project-type:: [[Research Project]]
  status:: active
  started:: [[2026-01-22]]
  deadline:: [[2026-12-31]]
  advisor:: [[Person/Dr-Smith]]
  tags:: #research #thesis

  ## Project Overview

  ### Research Question

  How do ethical considerations impact the deployment
  of machine learning systems in healthcare?

  ### Hypotheses

  1. Transparency requirements reduce adoption speed
  2. Bias mitigation increases development costs
  3. Regulatory compliance varies by jurisdiction

  ### Objectives

  - [ ] Literature review (100+ papers)
  - [ ] Case study analysis (5 hospitals)
  - [ ] Survey design and distribution
  - [ ] Data analysis
  - [ ] Thesis writing

  ## Timeline

  - Q1 2026: Literature review
  - Q2 2026: Data collection
  - Q3 2026: Analysis
  - Q4 2026: Writing & defense

  ## Resources

  - Budget: $5000
  - Lab access: ML Lab
  - Data sources: Hospital partners

  ## Related Pages

  - [[Research/Literature-Review]]
  - [[Research/Methodology]]
  - [[Research/Data-Analysis]]
  - [[Research/Writing]]
```

### Namespace Organization

**Research Structure:**

```markdown
Your graph organization:

Research/
├── Projects/
│ └── Thesis-ML-Ethics/
│ ├── Literature-Review
│ ├── Methodology
│ ├── Data-Analysis
│ └── Writing
│
├── Literature/
│ ├── Papers/
│ │ ├── Smith-2025-Ethics-AI
│ │ └── Jones-2024-Healthcare-ML
│ ├── Books/
│ │ └── AI-Ethics-Handbook
│ └── Reports/
│ └── WHO-AI-Guidelines
│
├── Notes/
│ ├── Concepts/
│ │ ├── Algorithmic-Bias
│ │ └── Transparency-Requirement
│ └── Themes/
│ ├── Regulatory-Frameworks
│ └── Technical-Solutions
│
└── Outputs/
├── Paper-Drafts/
├── Presentations/
└── Data-Visualizations/
```

### Project Templates

**Research Project Template:**

```markdown
template:: research-project

- [[Research/Projects/{{title}}]]
  project-type:: [[Research Project]]
  status:: planning
  started:: [[<% today %>]]
  deadline::
  advisor:: [[Person/]]
  collaborators::
  tags:: #research

  ## Research Question

  What is the primary question?

  ## Background & Motivation

  Why is this important?

  ## Hypotheses

  1. Hypothesis 1
  2. Hypothesis 2

  ## Objectives

  - [ ] Objective 1
  - [ ] Objective 2

  ## Methodology

  How will we answer the question?

  ## Timeline

  - Phase 1: Literature review
  - Phase 2: Data collection
  - Phase 3: Analysis
  - Phase 4: Writing

  ## Resources Needed

  - Funding:
  - Equipment:
  - Data access:
  - Collaborators:

  ## Literature Review

  #+BEGIN_QUERY
  {:title "Related literature"
  :query [:find (pull ?b [*])
  :in $ ?project
  :where
  [?b :block/refs ?p]
  [?p :block/name ?project]
  [?b :block/properties ?props]
  [(get ?props :source-type)]]
  :inputs [:current-page]
  :collapsed? false}
  #+END_QUERY

  ## Key Findings

  - Finding 1
  - Finding 2

  ## Publications

  - [ ] Conference paper
  - [ ] Journal article
  - [ ] Thesis chapter
```

---

## Literature Discovery

### Search Strategy

**Systematic Search:**

```markdown
- [[Research/Literature-Search-Strategy]]
  project:: [[Research/Thesis-ML-Ethics]]
  date:: [[2026-01-22]]

  ## Search Terms

  Primary terms:

  - "machine learning ethics"
  - "healthcare AI"
  - "algorithmic bias"

  Secondary terms:

  - "fairness AI"
  - "transparency ML"
  - "medical AI regulation"

  Boolean combinations:

  - ("machine learning" OR "artificial intelligence")
    AND ("ethics" OR "fairness")
    AND ("healthcare" OR "medical")

  ## Databases

  - [ ] Google Scholar
  - [ ] PubMed
  - [ ] IEEE Xplore
  - [ ] ACM Digital Library
  - [ ] arXiv
  - [ ] ProQuest

  ## Inclusion Criteria

  ✅ Include:

  - Published 2020-2026
  - Peer-reviewed
  - English language
  - Healthcare focus

  ❌ Exclude:

  - Opinion pieces
  - Pre-2020 (unless seminal)
  - Non-healthcare applications

  ## Search Results

  - Google Scholar: 245 results
  - PubMed: 89 results
  - IEEE: 156 results

  Total: 490 papers
  After deduplication: 387 papers
  After title screening: 95 papers
  After abstract screening: 42 papers
  Full-text review: 42 papers
  Final included: 28 papers
```

### Capture Sources

**Quick Capture:**

```markdown
template:: source-quick-capture

- [[Literature/Papers/{{author}}-{{year}}-{{title}}]]
  source-type:: [[Paper]]
  author:: [[Person/{{author}}]]
  year:: {{year}}
  title:: {{title}}
  url::
  doi::
  status:: to-read
  project:: [[Research/]]
  tags:: #literature #to-read

  ## Quick Notes

  Why this paper matters:

  ## Next Actions

  - [ ] Download PDF
  - [ ] Read abstract
  - [ ] Full read
  - [ ] Take notes
```

---

## Source Management

### Source Properties

**Standard Metadata:**

```markdown
- [[Literature/Papers/Smith-2025-Ethics-AI]]
  source-type:: [[Paper]]

  ## Bibliographic Info

  author:: [[Person/Sarah-Smith]], [[Person/John-Doe]]
  year:: 2025
  title:: Ethical Considerations in Healthcare AI
  journal:: [[Journal/AI-Ethics]]
  volume:: 15
  issue:: 3
  pages:: 245-267
  doi:: 10.1000/ai-ethics.2025.001
  url:: https://doi.org/10.1000/ai-ethics.2025.001
  pdf:: [[../assets/smith-2025.pdf]]

  ## Classification

  publication-type:: journal-article
  research-method:: qualitative
  domain:: [[Healthcare]]
  topics:: [[Ethics]], [[Machine-Learning]], [[Transparency]]

  ## Project Context

  project:: [[Research/Thesis-ML-Ethics]]
  relevance:: high
  status:: read
  read-date:: [[2026-01-22]]

  ## Quality Assessment

  peer-reviewed:: true
  impact-factor:: 8.5
  citation-count:: 47
  quality-rating:: ⭐⭐⭐⭐⭐
```

### Reading Status Tracking

**Status Workflow:**

```markdown
Statuses:

1. to-read → Paper saved, not started
2. reading → Currently reading
3. read → Finished reading
4. annotated → Notes taken
5. synthesized → Integrated into research

Query by status:
#+BEGIN_QUERY
{:title "Papers to read"
:query [:find (pull ?b [*])
:where
[?b :block/properties ?props]
[(get ?props :status) "to-read"]
[(get ?props :source-type) "Paper"]]
:collapsed? false}
#+END_QUERY
```

---

## Reading & Annotation

### PDF Annotation Workflow

**In Logseq:**

```markdown
1. Add PDF to assets folder
   /assets/papers/smith-2025.pdf

2. Link from literature page
   pdf:: [[../assets/papers/smith-2025.pdf]]

3. Open PDF in Logseq

   - Right sidebar shows PDF
   - Left sidebar for notes

4. Annotate while reading

   - Highlight important text
   - Add comments
   - Create block references

5. Highlights appear as blocks
   - Automatic page numbers
   - Linkable with block refs
   - Searchable
```

**Example Annotations:**

```markdown
- [[Literature/Papers/Smith-2025-Ethics-AI]]

  ## Annotations

  ### Page 3 - Introduction

  > "Transparency in ML systems is not merely a
  > technical challenge but a fundamental ethical
  > requirement" id:: transparency-quote

  Note: Key argument for regulatory focus
  Related: [[Concepts/Transparency-Requirement]]

  ### Page 7 - Methodology

  > "We analyzed 50 healthcare ML deployments
  > across 10 hospitals" id:: methodology-scope

  Note: Similar scope to our research
  Could compare findings

  ### Page 15 - Key Finding

  > "78% of systems lacked adequate bias testing"
  > id:: bias-testing-stat

  Note: IMPORTANT - supports hypothesis 2
  Add to literature review section
```

### Active Reading Strategy

**Question-Driven Reading:**

```markdown
Before reading, prepare questions:

- [[Literature/Papers/Smith-2025-Ethics-AI]]

  ## Pre-Reading Questions

  1. What ethical frameworks do they use?
  2. How do they measure transparency?
  3. What are the practical recommendations?
  4. How does this relate to my hypothesis?
  5. What are the limitations?

  ## Answers (fill while reading)

  Q1: Ethical frameworks

  - Uses principlist approach (autonomy,
    beneficence, non-maleficence, justice)
  - Also references care ethics
  - ((framework-section-ref))

  Q2: Transparency measurement

  - Model interpretability metrics
  - User comprehension testing
  - Regulatory compliance checklist
  - ((transparency-metrics-ref))
```

---

## Note-Taking System

### Three-Layer Note System

**Layer 1: Source Notes (Literature Pages)**

```markdown
- [[Literature/Papers/Smith-2025-Ethics-AI]]

  ## Summary

  Paper examines ethical considerations in healthcare
  ML, focusing on transparency, bias, and regulation.
  Argues that current systems lack adequate ethical
  oversight.

  ## Key Arguments

  1. Transparency is ethical requirement, not just
     technical feature id:: arg-transparency
  2. Bias testing inadequate in most systems
     id:: arg-bias-testing
  3. Regulatory frameworks lag behind technology
     id:: arg-regulation-lag

  ## Evidence

  - Study of 50 ML systems in 10 hospitals
  - 78% lacked bias testing
  - Only 23% met transparency requirements

  ## Methodology

  - Mixed methods: surveys + interviews
  - Sample: 50 systems, 100 stakeholders
  - Limitations: Single country focus

  ## My Thoughts

  Strong empirical evidence. Methodology solid
  but limited geographic scope. Could replicate
  in our region.
```

**Layer 2: Concept Notes**

```markdown
- [[Concepts/Algorithmic-Bias]]
  note-type:: concept
  project:: [[Research/Thesis-ML-Ethics]]

  ## Definition

  Systematic and repeatable errors in ML systems
  that create unfair outcomes for certain groups.

  ## Types

  1. Data bias: Training data not representative
  2. Algorithm bias: Model design choices
  3. Interaction bias: How humans use system

  ## Sources in Literature

  - Smith (2025): 78% lack bias testing ((bias-stat))
  - Jones (2024): Racial disparities in diagnosis
  - Lee (2023): Gender bias in treatment recommendations

  ## Examples in Healthcare

  - Pulse oximeters less accurate for darker skin
  - Diagnostic AI trained mainly on white patients
  - Treatment recommendations favor insured patients

  ## Mitigation Strategies

  - Diverse training data
  - Fairness metrics in development
  - Regular bias audits
  - Stakeholder involvement

  ## Open Questions

  - How to define "fair" in medical context?
  - Trade-offs between fairness and accuracy?
  - Who should conduct bias audits?

  ## Related Concepts

  - [[Concepts/Fairness-Metrics]]
  - [[Concepts/Data-Representativeness]]
  - [[Concepts/Healthcare-Disparities]]
```

**Layer 3: Thematic Notes**

```markdown
- [[Themes/Regulatory-Frameworks-ML-Healthcare]]
  note-type:: theme
  project:: [[Research/Thesis-ML-Ethics]]

  ## Overview

  Analysis of regulatory approaches to ML in
  healthcare across different jurisdictions.

  ## Key Patterns

  1. EU: Strict, ethics-first approach
  2. US: Market-driven, post-deployment regulation
  3. Asia: Varied, innovation-focused

  ## Evidence from Literature

  ### EU Approach

  - Smith (2025): ((eu-regulation-quote))
  - Weber (2024): GDPR impact on ML development
  - Müller (2023): Medical Device Regulation updates

  Consensus: Comprehensive but may slow innovation

  ### US Approach

  - Johnson (2025): FDA guidance evolution
  - Lee (2024): State-level variation
  - Davis (2023): Industry self-regulation

  Consensus: Flexible but potentially risky gaps

  ## Implications for Research

  - Need jurisdiction-specific analysis
  - Compare outcomes across frameworks
  - Consider regulatory evolution timeline

  ## Gaps in Literature

  - Limited cross-jurisdictional comparisons
  - Few studies on regulation effectiveness
  - Lack of long-term impact data

  ## My Synthesis

  Current frameworks reflect different values:
  EU prioritizes safety/ethics, US prioritizes
  innovation/speed. Neither clearly superior.
  Need empirical comparison of patient outcomes.
```

### Progressive Note Development

**Evolution Over Time:**

```markdown
Day 1 - Initial capture:

- [[Concepts/Algorithmic-Bias]]
  Unfair outcomes from ML systems
  Source: Smith (2025)

Week 1 - After reading 5 papers:

- [[Concepts/Algorithmic-Bias]]
  Systematic errors creating unfair outcomes
  Types: data, algorithm, interaction bias
  Sources: Smith, Jones, Lee

Month 1 - After reading 20 papers:

- [[Concepts/Algorithmic-Bias]]
  [Full definition]
  [Multiple types with examples]
  [Evidence from 20+ sources]
  [Mitigation strategies]
  [Open questions]
  [Related concepts web]

Result: Rich, well-supported concept note
```

---

## Synthesis & Analysis

### Synthesis Techniques

**Compare & Contrast:**

```markdown
- [[Analysis/Transparency-Approaches-Comparison]]

  ## Research Question

  How do different papers define/measure transparency?

  ## Approaches Identified

  ### Technical Transparency (5 papers)

  - Focus: Model interpretability
  - Methods: SHAP, LIME, attention weights
  - Papers: Smith (2025), Chen (2024), Kumar (2023)
  - Strength: Measurable, technical
  - Weakness: Experts only, not patient-facing

  ### Regulatory Transparency (3 papers)

  - Focus: Disclosure requirements
  - Methods: Documentation, reporting
  - Papers: Weber (2024), Johnson (2025)
  - Strength: Standardized, enforceable
  - Weakness: May not ensure understanding

  ### User-Centered Transparency (4 papers)

  - Focus: Patient/clinician understanding
  - Methods: Plain language, visualizations
  - Papers: Lee (2024), Davis (2023), Park (2025)
  - Strength: Accessible, actionable
  - Weakness: Subjective, hard to measure

  ## Synthesis

  Three paradigms emerging, not mutually exclusive.
  Best practice likely combines all three:

  1. Technical foundation (interpretable models)
  2. Regulatory compliance (documentation)
  3. User-centered communication (explanation)

  Gap: Few papers combine all three approaches.
  Opportunity: Integrated transparency framework.
```

**Pattern Identification:**

```markdown
- [[Analysis/Patterns-Bias-Mitigation]]

  ## Pattern: Diverse Training Data

  Mentioned in: 15/28 papers
  Success rate: High (when properly implemented)
  Challenges: Data availability, privacy concerns

  ## Pattern: Fairness Metrics

  Mentioned in: 12/28 papers
  Success rate: Mixed (definition challenges)
  Challenges: Trade-offs between metrics

  ## Pattern: Stakeholder Involvement

  Mentioned in: 8/28 papers
  Success rate: High (when genuine)
  Challenges: Time, expertise, power dynamics

  ## Meta-Pattern

  Most effective approaches combine multiple
  strategies. Single interventions rarely sufficient.

  ## Recommendation for Research

  Test combined intervention package rather than
  single strategy.
```

### Evidence Mapping

**Claim → Evidence:**

```markdown
- [[Analysis/Evidence-Map-Hypothesis-2]]

  ## Hypothesis

  Bias mitigation increases ML development costs

  ## Supporting Evidence

  ### Direct Evidence

  - Lee (2024): 40% cost increase with fairness testing
    ((lee-cost-stat))
  - Kumar (2023): 6 months added to timeline
    ((kumar-timeline))
  - Park (2025): 2x data collection costs
    ((park-data-cost))

  ### Indirect Evidence

  - Smith (2025): Most systems skip bias testing
    → Implies cost barrier
  - Weber (2024): EU regulation increases costs
    → Includes bias requirements

  ### Contradicting Evidence

  - Chen (2024): Early bias detection reduces costs
    → Prevents expensive late fixes
  - Davis (2023): Automated fairness tools cheap
    → If used from start

  ## Quality Assessment

  Supporting: Strong (3 direct quantitative studies)
  Contradicting: Moderate (2 studies, different context)

  ## Synthesis

  Hypothesis supported BUT with important nuance:

  - Costs increase if added late in process
  - Costs manageable if integrated from start
  - Long-term costs may be lower (avoid failures)

  ## Refined Hypothesis

  Bias mitigation increases SHORT-TERM development
  costs, but may reduce TOTAL costs by preventing
  failures and rebuilds.
```

---

## Writing Process

### From Notes to Outline

**Step 1: Gather Relevant Notes**

```markdown
- [[Writing/Lit-Review-Section]]

  ## Related Concept Notes

  #+BEGIN_QUERY
  {:title "Concepts for lit review"
  :query [:find (pull ?b [*])
  :where
  [?b :block/properties ?props]
  [(get ?props :note-type) "concept"]
  [?b :block/refs ?p]
  [?p :block/name ?project]]
  :inputs ["research/thesis-ml-ethics"]
  :collapsed? false}
  #+END_QUERY

  Selected concepts:

  - [[Concepts/Algorithmic-Bias]]
  - [[Concepts/Transparency-Requirement]]
  - [[Concepts/Fairness-Metrics]]
  - [[Concepts/Regulatory-Frameworks]]
```

**Step 2: Create Outline**

```markdown
- [[Writing/Literature-Review-Outline]]

  ## Structure

  ### 1. Introduction (500 words)

  - Problem statement
  - Scope of review
  - Organization of review

  ### 2. Algorithmic Bias (1500 words)

  - 2.1 Definition and Types

    - Use: ((bias-definition))
    - Sources: Smith (2025), Jones (2024)

  - 2.2 Evidence in Healthcare

    - Use: ((bias-healthcare-examples))
    - Sources: Lee (2024), Park (2025)

  - 2.3 Measurement Approaches
    - Use: ((fairness-metrics))
    - Sources: Kumar (2023), Chen (2024)

  ### 3. Transparency Requirements (1200 words)

  - 3.1 Technical Transparency

    - Use: ((technical-transparency))

  - 3.2 Regulatory Requirements

    - Use: ((regulatory-transparency))

  - 3.3 User-Centered Approaches
    - Use: ((user-transparency))

  ### 4. Regulatory Frameworks (1000 words)

  - 4.1 European Approach
  - 4.2 US Approach
  - 4.3 Comparative Analysis

  ### 5. Gaps and Future Directions (500 words)

  - Identified gaps
  - Research opportunities
```

**Step 3: Write with Evidence**

```markdown
- [[Writing/Lit-Review-Draft-Section-2]]

  ## 2. Algorithmic Bias in Healthcare ML

  Algorithmic bias refers to systematic and
  repeatable errors in machine learning systems
  that create unfair outcomes for certain groups
  (Smith, 2025; Jones, 2024). {{embed ((bias-definition))}}

  In healthcare contexts, bias manifests in three
  primary forms. First, data bias emerges when
  training datasets fail to adequately represent
  the target population. For example, Lee (2024)
  found that diagnostic AI systems trained primarily
  on white patients showed significantly lower
  accuracy for patients of color. {{embed ((lee-example))}}

  Second, algorithmic bias can arise from model
  design choices. Kumar (2023) demonstrated that
  optimization for overall accuracy can mask severe
  disparities in performance across demographic groups.
  {{embed ((kumar-finding))}}

  [Continue draft...]

  ## Citations in This Section

  - Smith (2025): ((smith-ref))
  - Jones (2024): ((jones-ref))
  - Lee (2024): ((lee-ref))
  - Kumar (2023): ((kumar-ref))
```

### Draft Management

**Version Tracking:**

```markdown
- [[Writing/Thesis-Chapter-2]]
  status:: draft-2
  word-count:: 4500
  last-updated:: [[2026-01-22]]

  ## Version History

  - Draft 1 (2026-01-10): Initial complete draft
  - Draft 2 (2026-01-22): Incorporated advisor feedback
  - Draft 3 (planned): After peer review

  ## Current Status

  - [x] Complete first draft
  - [x] Advisor review
  - [ ] Incorporate feedback
  - [ ] Peer review
  - [ ] Final polish

  ## Advisor Feedback

  - DONE Expand section 2.3 on fairness metrics
  - DONE Add more EU examples in section 4.1
  - TODO Clarify methodology discussion
  - TODO Add table comparing frameworks
```

---

## Citation Management

### Citation Properties

**Standard Format:**

````markdown
- [[Literature/Papers/Smith-2025-Ethics-AI]]

  ## Citation Formats

  ### APA

  Smith, S., & Doe, J. (2025). Ethical considerations
  in healthcare AI. _AI Ethics_, _15_(3), 245-267.
  https://doi.org/10.1000/ai-ethics.2025.001

  ### Chicago

  Smith, Sarah, and John Doe. "Ethical Considerations
  in Healthcare AI." _AI Ethics_ 15, no. 3 (2025):
  245-267.

  ### BibTeX

  ```bibtex
  @article{smith2025ethics,
    author = {Smith, Sarah and Doe, John},
    title = {Ethical Considerations in Healthcare AI},
    journal = {AI Ethics},
    volume = {15},
    number = {3},
    pages = {245--267},
    year = {2025},
    doi = {10.1000/ai-ethics.2025.001}
  }
  ```
````

````

### Reference Management

**Bibliography Query:**

```clojure
#+BEGIN_QUERY
{:title "All cited sources"
 :query [:find (pull ?b [*])
         :in $ ?project
         :where
         [?b :block/refs ?p]
         [?p :block/name ?project]
         [?b :block/properties ?props]
         [(get ?props :source-type)]]
 :inputs ["research/thesis-ml-ethics"]
 :result-transform (fn [result]
                     (sort-by
                       (fn [b]
                         (str (get-in b [:block/properties :author])
                              (get-in b [:block/properties :year])))
                       result))
 :collapsed? false}
#+END_QUERY
````

### Citation Tracking

**Usage Query:**

```clojure
#+BEGIN_QUERY
{:title "Where did I cite this paper?"
 :query [:find (pull ?b [*])
         :in $ ?paper
         :where
         [?paper-page :block/name ?paper]
         [?b :block/refs ?paper-page]
         [?b :block/content ?content]
         [(clojure.string/includes? ?content "embed")]]
 :inputs ["literature/papers/smith-2025-ethics-ai"]
 :collapsed? false}
#+END_QUERY
```

---

## Collaboration

### Shared Research Graph

**Team Structure:**

```markdown
- [[Research/Team-Project-Healthcare-AI]]
  team:: [[Person/Sarah]], [[Person/John]], [[Person/Alice]]

  ## Roles & Responsibilities

  - Sarah: Literature review
  - John: Data collection
  - Alice: Analysis

  ## Shared Namespace

  Research/Team-Project/
  ├── Literature/ (Sarah's responsibility)
  ├── Data/ (John's responsibility)
  ├── Analysis/ (Alice's responsibility)
  └── Shared/ (Everyone)

  ## Collaboration Norms

  - Weekly sync meetings
  - Status updates in daily notes
  - Tag teammates with [[Person/Name]]
  - Use TODO with owner:: for tasks
```

### Review Workflow

**Request Feedback:**

```markdown
- [[Writing/Draft-Chapter-2]]

  ## Review Requests

  ### From Advisor

  - TODO Review literature synthesis
    reviewer:: [[Person/Dr-Smith]]
    requested:: [[2026-01-22]]
    due:: [[2026-01-29]]
    focus:: Conceptual framework

  ### From Peer

  - TODO Check methodology section
    reviewer:: [[Person/Sarah]]
    requested:: [[2026-01-22]]
    due:: [[2026-01-25]]
    focus:: Methods clarity
```

**Provide Feedback:**

```markdown
- [[Feedback/Sarah-Chapter-3-Review]]
  document:: [[Writing/Draft-Chapter-3]]
  reviewer:: [[Person/Me]]
  reviewed:: [[2026-01-22]]

  ## Overall Assessment

  Strong structure, needs more evidence in section 3.2

  ## Specific Comments

  ### Section 3.1 - Excellent

  Clear argument, well-supported
  ((section-3-1))

  ### Section 3.2 - Needs Work

  Claim about regulatory impact needs evidence
  ((section-3-2))

  Suggestion: Add Weber (2024) study on EU regulations

  ### Section 3.3 - Minor Issues

  Typo: "algorithim" → "algorithm"
  ((typo-location))
```

---

## Best Practices

### Daily Research Habits

**Morning (30 min):**

```markdown
1. Review yesterday's notes (10 min)
2. Plan today's reading (5 min)
3. Check research TODOs (5 min)
4. Update project dashboard (10 min)
```

**Reading Session (2-3 hours):**

```markdown
1. Read actively with questions
2. Annotate as you go
3. Create literature page immediately after
4. Extract 1-3 concept notes
5. Update relevant theme notes
```

**Evening (15 min):**

```markdown
1. Review what learned today
2. Make connections to existing notes
3. Identify gaps or questions
4. Plan tomorrow's reading
```

### Weekly Review (2 hours)

```markdown
Sunday afternoon:

1. Review week's reading (30 min)

   - Papers read: X
   - Notes created: Y
   - Connections made: Z

2. Update project dashboard (30 min)

   - Progress on objectives
   - Timeline check
   - Upcoming deadlines

3. Identify patterns (30 min)

   - Recurring themes
   - Contradictions
   - Gaps

4. Plan next week (30 min)
   - Reading list
   - Writing goals
   - Analysis tasks
```

### Quality Standards

**Source Notes:**

```markdown
✅ Good literature note:

- Complete bibliographic info
- Summary in your words
- Key arguments with block IDs
- Evidence and methodology
- Your critical thoughts
- Links to concept notes

❌ Insufficient:

- Just copy-paste abstracts
- Missing metadata
- No connections
- No critical analysis
```

**Concept Notes:**

```markdown
✅ Good concept note:

- Clear definition
- Multiple sources
- Examples
- Critical analysis
- Related concepts
- Open questions

❌ Insufficient:

- Single source
- No examples
- Isolated
- Uncritical acceptance
```

---

## Complete Research Example

**Month 1-2: Literature Review**

```markdown
Week 1-2: Broad reading

- Read 20 papers (scanning)
- Create literature pages for 10 key papers
- Identify 5 major concepts

Week 3-4: Deep reading

- Annotate 10 papers thoroughly
- Create 15 concept notes
- Start 3 theme notes
- Identify patterns
```

**Month 3-4: Data Collection**

```markdown
- Continue reading (5 papers/week)
- Collect data
- Create data analysis pages
- Link findings to literature
```

**Month 5-6: Analysis**

```markdown
- Deep analysis phase
- Create synthesis pages
- Update theme notes
- Test hypotheses against evidence
```

**Month 7-9: Writing**

```markdown
- Create outlines from notes
- Draft chapters using embedded notes
- Citations automatically tracked
- Revise based on feedback
```

**Month 10-12: Revision & Defense**

```markdown
- Final revisions
- Peer review
- Committee feedback
- Defense preparation
```

**Result:**

- 100+ literature pages
- 50+ concept notes
- 10+ theme notes
- Full traceability
- Citation management
- Rich knowledge base for future work

---

## Troubleshooting

**"Too many unprocessed papers":**

```markdown
Solution: Progressive processing

- Quick scan → Include/exclude decision
- Included → Literature page (10 min)
- Key papers → Deep annotation (1 hour)
- Not everything needs deep read
```

**"Notes feel disconnected":**

```markdown
Solution: Regular synthesis

- Weekly: Review new notes, add connections
- Monthly: Create/update theme notes
- Quarterly: Big picture synthesis
```

**"Can't find sources I need":**

```markdown
Solution: Better tagging and queries

- Use consistent properties
- Tag by method, domain, finding
- Create specialized queries
- Link from multiple angles
```

**"Writing feels overwhelming":**

```markdown
Solution: Gradual composition

- Don't start from blank page
- Start with outline from notes
- Copy relevant note sections
- Rewrite in own voice for coherence
- Polish last
```

---

## Next Steps

**Start Your Research Workflow:**

1. Create project hub (30 min)
2. Set up templates (30 min)
3. Process first 3 papers (3 hours)
4. Create initial concept notes (1 hour)
5. Establish daily habits

**After 1 Month:**

- 20-30 literature pages
- 10-15 concept notes
- Clear patterns emerging
- Comfortable with workflow

**After 6 Months:**

- 80-100 literature pages
- 40-50 concept notes
- 10+ theme notes
- Ready to write
- Deep understanding of field

**Related:**

- [Zettelkasten Method](./zettelkasten-method.md)
- [PDF Annotations](../../how-to/features/pdf-annotations.md)
- [Query Patterns](../../reference/advanced/query-patterns.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
