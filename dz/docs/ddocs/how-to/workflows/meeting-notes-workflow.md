---
title: 'Meeting Notes Workflow'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Task Management](./task-management-workflows.md)'
  - '[Daily Workflows](./daily-workflows.md)'
  - '[Templates System](../../reference/interface/templates-system.md)'
tags: ['meetings', 'workflow', 'productivity', 'collaboration']
---

# Meeting Notes Workflow

Learn how to capture, organize, and act on meeting notes in Logseq. This workflow ensures meetings lead to clear outcomes and action items never get lost.

**What You'll Learn:**

- Pre-meeting preparation
- Live note-taking strategies
- Action item tracking
- Meeting follow-up process
- Query patterns for meeting insights

**Time to Setup:** 15-30 minutes

---

## Table of Contents

1. [Pre-Meeting Preparation](#pre-meeting-preparation)
2. [Meeting Note Template](#meeting-note-template)
3. [Live Note-Taking](#live-note-taking)
4. [Action Items](#action-items)
5. [Post-Meeting Process](#post-meeting-process)
6. [Meeting Types](#meeting-types)
7. [Queries & Reports](#queries--reports)
8. [Best Practices](#best-practices)

---

## Pre-Meeting Preparation

### Create Meeting Page

**Standard Meeting Page:**

```markdown
- [[Meetings/2026-01-22-Project-Alpha-Kickoff]]
  type:: [[Meeting]]
  date:: [[2026-01-22]]
  time:: 14:00-15:00
  project:: [[Projects/Alpha]]
  attendees:: [[Person/Sarah]], [[Person/John]], [[Person/Alice]]

  ## Pre-Meeting

  ### Context

  First planning meeting for Project Alpha

  ### Goals

  - [ ] Define project scope
  - [ ] Assign initial responsibilities
  - [ ] Set next milestones

  ### Agenda

  - [ ] Introductions (5 min)
  - [ ] Project overview (10 min)
  - [ ] Technical approach discussion (25 min)
  - [ ] Next steps (10 min)
  - [ ] Q&A (10 min)

  ### Questions to Ask

  - What's the target launch date?
  - Who are the key stakeholders?
  - What are the biggest risks?

  ### Preparation Done

  - [x] Review project brief ((project-brief-ref))
  - [x] Prepare technical questions
  - [x] Share agenda with attendees
```

### Recurring Meeting Setup

**Template for Series:**

```markdown
- [[Meetings/Weekly-Team-Sync]]
  type:: [[Recurring Meeting]]
  frequency:: weekly
  day:: Wednesday
  time:: 10:00-11:00
  attendees:: [[Team/Engineering]]

  ## Standard Agenda

  1. Wins from last week
  2. Current priorities
  3. Blockers
  4. Next week plan
  5. Questions

  ## Meeting Notes

  - [[2026-01-22]] ((latest-meeting))
  - [[2026-01-15]] ((previous-meeting))
  - [[2026-01-08]]
```

---

## Meeting Note Template

### Basic Template

```markdown
template:: meeting-note

- [[Meetings/<% today %>-{{title}}]]
  type:: [[Meeting]]
  date:: [[<% today %>]]
  time::
  project:: [[Projects/]]
  attendees:: [[Person/]]
  tags:: #meeting

  ## Pre-Meeting

  ### Context

  Why are we meeting?

  ### Goals

  - [ ] Goal 1
  - [ ] Goal 2
  - [ ] Goal 3

  ### Agenda

  - [ ] Topic 1 (X min)
  - [ ] Topic 2 (X min)
  - [ ] Topic 3 (X min)

  ***

  ## Meeting Notes

  ### Discussion

  ### Key Points

  - id:: key-point-1
  - id:: key-point-2

  ### Decisions

  - id:: decision-1
  - id:: decision-2

  ### Questions & Open Items

  - id:: question-1

  ***

  ## Post-Meeting

  ### Action Items

  - TODO Action 1
    owner:: [[Person/]]
    DEADLINE: <date>
  - TODO Action 2
    owner:: [[Person/]]
    DEADLINE: <date>

  ### Follow-up Needed

  - [ ] Send summary email
  - [ ] Schedule follow-up meeting
  - [ ] Update project tracker

  ### Next Meeting

  SCHEDULED: <date>
```

### Enhanced Template

```markdown
template:: meeting-note-enhanced

- [[Meetings/<% today %>-{{title}}]]
  type:: [[Meeting]]
  date:: [[<% today %>]]
  time::
  duration::
  location::
  meeting-link::
  recording::
  project:: [[Projects/]]
  attendees:: [[Person/]]
  attendee-roles::

  - Organizer: [[Person/]]
  - Decision Maker: [[Person/]]
  - Subject Expert: [[Person/]]
    tags:: #meeting
    status:: scheduled

  ## Pre-Meeting

  ### Context & Background

  Why this meeting? What's the history?

  ### Meeting Goals

  What do we want to accomplish?

  - [ ] Goal 1
  - [ ] Goal 2

  ### Success Criteria

  How will we know the meeting was successful?

  - Criterion 1
  - Criterion 2

  ### Agenda (with time boxes)

  - [ ] Introduction & goals alignment (5 min)
  - [ ] Topic 1 (15 min)
  - [ ] Topic 2 (20 min)
  - [ ] Decisions & next steps (15 min)
  - [ ] Q&A (5 min)

  ### Pre-Reading Materials

  - ((doc-1))
  - ((doc-2))

  ### Questions to Address

  - Question 1
  - Question 2

  ### Preparation Checklist

  - [ ] Agenda shared 24h in advance
  - [ ] Materials sent to attendees
  - [ ] Room/video link confirmed
  - [ ] Previous action items reviewed

  ***

  ## During Meeting

  ### Attendance

  Present: [[Person/Sarah]], [[Person/John]]
  Absent: [[Person/Alice]] (sick)

  ### Discussion Notes

  Chronological capture:

  **14:05** - Meeting started

  **14:15** - Topic 1 discussion

  **14:30** - Key decision made

  ### Key Points

  Main takeaways:

  - id:: key-1
  - id:: key-2

  ### Decisions Made

  - Decision 1 id:: decision-1
    - Rationale: Why we chose this
    - Alternatives considered: What we didn't choose
    - Decision maker: [[Person/Sarah]]

  ### Disagreements & Debates

  - Issue: Different opinions on approach
  - Perspectives: Sarah prefers A, John prefers B
  - Resolution: Decided on A because...

  ### Parking Lot

  Off-topic items to address later:

  - Item 1
  - Item 2

  ### Questions & Open Items

  Unresolved questions:

  - Question 1 id:: open-1
  - Question 2 id:: open-2

  ***

  ## Post-Meeting

  ### Action Items

  - TODO Action 1 id:: action-1
    owner:: [[Person/Sarah]]
    DEADLINE: <date>
    priority:: [#A]
    context:: [[Context/Office]]
    depends-on::

  ### Follow-up Tasks

  - [ ] Send meeting summary to attendees
  - [ ] Share recording/transcript
  - [ ] Update project documentation ((project-doc))
  - [ ] Schedule follow-up meeting if needed
  - [ ] Close related tasks from previous meetings

  ### Next Steps

  What happens after this meeting?

  1. Step 1
  2. Step 2
  3. Step 3

  ### Next Meeting

  - Topic: Follow-up discussion
  - SCHEDULED: <date>
  - Agenda: ((follow-up-agenda))

  ### Meeting Metrics

  - Goals achieved: 2/3
  - Time used: 55/60 min
  - Attendee count: 7
  - Action items created: 5

  ### Retrospective

  What went well:

  - Clear agenda helped focus
  - Good participation

  What could improve:

  - Started 5 min late
  - Ran over on Topic 2
```

---

## Live Note-Taking

### Capture Strategy

**Block-Based Structure:**

```markdown
Use blocks for everything:

❌ Paragraph style:
Sarah mentioned we should use React. John said
TypeScript would be better. We discussed pros
and cons for 20 minutes.

✅ Block style:

- **14:15** - Framework discussion
  - Sarah: Proposes React
    - Pros: Team familiarity, ecosystem
    - Cons: Bundle size
  - John: Suggests adding TypeScript
    - Pros: Type safety, better IDE support
    - Cons: Learning curve
  - Decision: Use React + TypeScript id:: decision-framework
    - Rationale: Best of both worlds
```

### Real-Time Techniques

**Speed Shortcuts:**

```markdown
While typing:

- Use abbreviations first, expand later
- Add IDs to important blocks (id:: xyz)
- Drop timestamps for context (14:15)
- Link people [[Person/Name]]
- Link projects [[Projects/Name]]
- Use TODO for actions immediately
- Tag decisions with DECISION::
```

**Keyboard Shortcuts:**

```markdown
Essential during meetings:

- Cmd+Enter: Create TODO
- Tab: Indent (show hierarchy)
- Shift+Tab: Outdent
- /link: Quick page link
- /deadline: Set deadline
- /todo: Create task
- Cmd+Shift+A: Add to-do checkbox
```

### Capture Priorities

**What to Write:**

```markdown
✅ Essential:

- Decisions made
- Action items
- Key points
- Who said what (when relevant)
- Timestamps for decisions

⚠️ Nice to have:

- Discussion details
- Supporting arguments
- Alternatives considered

❌ Skip:

- Exact quotes (unless critical)
- Obvious/common knowledge
- Chitchat
```

---

## Action Items

### Create Actionable Tasks

**Good Action Item:**

```markdown
✅ Specific and actionable:

- TODO Review API documentation and create
  summary of authentication endpoints
  owner:: [[Person/Sarah]]
  DEADLINE: <2026-01-25 Fri>
  project:: [[Projects/Alpha]]
  context:: [[Context/Office]]
  time-estimate:: 2h
  related-decision:: ((decision-api-approach))
```

**Bad Action Item:**

```markdown
❌ Vague:

- TODO API stuff
  owner:: Sarah
```

### Action Item Template

```markdown
- TODO [Action verb] [specific task] [clear outcome]
  owner:: [[Person/Name]]
  DEADLINE: <date>
  project:: [[Projects/Name]]
  priority:: [#A]
  context:: [[Context/Office]]
  time-estimate:: 2h
  blocked-by:: ((prerequisite))
  related-discussion:: ((meeting-section))
```

### Track Meeting-Specific Actions

**Query on Meeting Page:**

```clojure
#+BEGIN_QUERY
{:title "Action items from this meeting"
 :query [:find (pull ?b [*])
         :in $ ?current-page
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]
         [?b :block/page ?p]
         [?p :block/name ?current-page]]
 :inputs [:current-page]
 :collapsed? false}
#+END_QUERY
```

---

## Post-Meeting Process

### Immediate Follow-Up (5 min)

**Right After Meeting:**

```markdown
1. Review notes for clarity

   - Expand abbreviations
   - Add missing context
   - Fix typos

2. Verify action items

   - Clear owners assigned
   - Reasonable deadlines
   - Proper priority

3. Mark meeting complete
   status:: completed

4. Update related pages
   - Add decisions to project page
   - Link from daily note
```

### Send Summary (15 min)

**Email Template:**

```markdown
## Meeting Summary Template

Subject: [Project] Meeting Summary - [Date]

Hi team,

Thanks for joining today's meeting on [Topic].

**Attendees:** Sarah, John, Alice

**Key Decisions:**

1. ((decision-1))
2. ((decision-2))

**Action Items:**

- Sarah: ((action-1)) - Due Jan 25
- John: ((action-2)) - Due Jan 27

**Open Questions:**

- ((question-1))
- ((question-2))

**Next Meeting:** [Date/Time]

Full notes: [Link to Logseq page if shared]

Thanks,
[Your name]
```

### Link to Related Pages

**Create Network:**

```markdown
From meeting page, link to:

1. Project page

   - [[Projects/Alpha]]
   - Add meeting reference
   - Update project status

2. Daily note

   - [[2026-01-22]]
   - Add ((meeting-reference))

3. People pages

   - [[Person/Sarah]]
   - Link action items

4. Topic pages
   - [[Topics/API Design]]
   - Reference decisions
```

---

## Meeting Types

### 1-on-1 Meetings

**Template:**

```markdown
- [[Meetings/1-1-Sarah-2026-01-22]]
  type:: [[1-on-1]]
  with:: [[Person/Sarah]]
  date:: [[2026-01-22]]
  frequency:: biweekly

  ## Agenda

  - [ ] How's it going? (check-in)
  - [ ] Current project status
  - [ ] Any blockers?
  - [ ] Career development
  - [ ] Feedback both ways

  ## Sarah's Topics

  - Question about architecture
  - Interest in ML project

  ## My Topics

  - Feedback on recent PR review
  - Upcoming deadline discussion

  ## Discussion

  ## Action Items

  - TODO

  ## Private Notes

  ## (Not shared)
```

### Project Meetings

**Template:**

```markdown
- [[Meetings/Project-Alpha-Weekly-2026-01-22]]
  type:: [[Project Meeting]]
  project:: [[Projects/Alpha]]
  date:: [[2026-01-22]]

  ## Status Update

  ### Progress This Week

  - Completed: API design
  - In Progress: Frontend mockups
  - Blocked: Database schema review

  ### Metrics

  - Sprint velocity: 23 points
  - Open issues: 12
  - Days to milestone: 45

  ## Discussion Topics

  ### 1. Database Schema

  - Blocker: Need DBA review
  - Timeline: This week
  - Owner: [[Person/John]]

  ### 2. Frontend Approach

  - Decision needed: React vs Vue
  - Discussion: ((discussion-notes))
  - Decision: React id:: decision-framework

  ## Risks & Issues

  - Risk: Dependency on external API
    - Mitigation: Build mock service
    - Owner: [[Person/Sarah]]

  ## Next Week Plan

  - Complete database schema
  - Start API implementation
  - Design review meeting
```

### Brainstorming Sessions

**Template:**

```markdown
- [[Meetings/Brainstorm-Feature-Ideas-2026-01-22]]
  type:: [[Brainstorming]]
  project:: [[Projects/Alpha]]
  date:: [[2026-01-22]]

  ## Challenge

  How to improve user onboarding experience?

  ## Ideas (No filtering yet)

  ### Sarah's Ideas

  - Interactive tutorial
  - Video walkthrough
  - Sample data pre-loaded

  ### John's Ideas

  - Onboarding checklist
  - Tooltips on first use
  - AI-powered guide

  ### My Ideas

  - Progressive disclosure
  - Contextual help
  - Community templates

  ## Voting

  Each person gets 3 votes:

  - Interactive tutorial: ⭐⭐⭐⭐⭐
  - Sample data: ⭐⭐⭐⭐
  - Onboarding checklist: ⭐⭐⭐

  ## Selected Ideas

  - TODO Prototype interactive tutorial
    owner:: [[Person/Sarah]]
    DEADLINE: <2026-02-05>

  - TODO Create sample data set
    owner:: [[Person/John]]
    DEADLINE: <2026-02-01>
```

### Retrospectives

**Template:**

```markdown
- [[Meetings/Sprint-Retrospective-2026-01-22]]
  type:: [[Retrospective]]
  project:: [[Projects/Alpha]]
  sprint:: Sprint 5
  date:: [[2026-01-22]]

  ## Sprint Metrics

  - Planned points: 25
  - Completed points: 23
  - Velocity: 92%

  ## What Went Well? 😊

  - Great collaboration on API design
  - Fast bug resolution
  - Good test coverage

  ## What Didn't Go Well? 😕

  - Meeting overload (12 hours)
  - Unclear requirements initially
  - Build pipeline flaky

  ## What Should We Try? 💡

  - Action: Block "no meeting" time
    owner:: [[Person/Manager]]

  - Action: Requirement template
    owner:: [[Person/Product]]

  - Action: Fix CI/CD stability
    owner:: [[Person/DevOps]]

  ## Appreciations 🙏

  - Sarah: Great pair programming
  - John: Excellent code reviews
  - Alice: Patient with questions
```

---

## Queries & Reports

### All Meetings

```clojure
#+BEGIN_QUERY
{:title "All meetings"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :type) "Meeting"]]
 :result-transform (fn [result]
                     (sort-by
                       (fn [b]
                         (get-in b [:block/properties :date]))
                       > result))
 :collapsed? false}
#+END_QUERY
```

### Meetings This Week

```clojure
#+BEGIN_QUERY
{:title "Meetings this week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/properties ?props]
         [(get ?props :type) "Meeting"]
         [?b :block/refs ?date-page]
         [?date-page :block/journal? true]
         [?date-page :block/journal-day ?date]
         [(>= ?date ?start)]
         [(<= ?date ?end)]]
 :inputs [:monday :sunday]
 :collapsed? false}
#+END_QUERY
```

### Action Items by Person

```clojure
#+BEGIN_QUERY
{:title "My meeting action items"
 :query [:find (pull ?b [*])
         :in $ ?person
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]
         [?b :block/refs ?p]
         [?p :block/name ?person]]
 :inputs ["person/sarah"]
 :collapsed? false}
#+END_QUERY
```

### Overdue Meeting Actions

```clojure
#+BEGIN_QUERY
{:title "Overdue meeting actions"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]
         [?b :block/deadline ?d]
         [(< ?d [:today])]
         [?b :block/page ?p]
         [?p :block/properties ?page-props]
         [(get ?page-props :type) "Meeting"]]
 :collapsed? false}
#+END_QUERY
```

### Meetings by Project

```clojure
#+BEGIN_QUERY
{:title "Project Alpha meetings"
 :query [:find (pull ?b [*])
         :in $ ?project
         :where
         [?b :block/properties ?props]
         [(get ?props :type) "Meeting"]
         [?b :block/refs ?p]
         [?p :block/name ?project]]
 :inputs ["projects/alpha"]
 :result-transform (fn [result]
                     (sort-by
                       (fn [b]
                         (get-in b [:block/properties :date]))
                       > result))
 :collapsed? false}
#+END_QUERY
```

### Meeting Statistics

```clojure
#+BEGIN_QUERY
{:title "Meeting stats this month"
 :query [:find (count ?b)
         :where
         [?b :block/properties ?props]
         [(get ?props :type) "Meeting"]
         [?b :block/created-at ?created]
         [(- (js/Date.now) ?created) ?age]
         [(< ?age 2592000000)]]}  ; 30 days
#+END_QUERY
```

---

## Best Practices

### Before Meeting

```markdown
✅ Do:

- Create page 24h in advance
- Share agenda with attendees
- Prepare questions
- Review previous meeting notes
- Check action item completion

❌ Avoid:

- Skipping preparation
- Vague agenda
- Not reviewing context
```

### During Meeting

```markdown
✅ Do:

- Capture decisions with IDs
- Create TODOs immediately
- Link people and projects
- Note timestamps for key moments
- Focus on outcomes, not transcription

❌ Avoid:

- Trying to capture every word
- Not linking to context
- Vague action items
- Forgetting to assign owners
```

### After Meeting

```markdown
✅ Do (within 1 hour):

- Clean up notes
- Send summary
- Create action items
- Link to related pages
- Update project status

❌ Avoid:

- Delaying follow-up
- Not sharing outcomes
- Leaving action items unassigned
```

### General Tips

**Time Management:**

```markdown
- Start on time (even if people late)
- Use agenda time boxes
- Parking lot for off-topic items
- End on time (respect calendars)
- Buffer time between meetings
```

**Participation:**

```markdown
- Round-robin for input
- Call on quiet attendees
- Limit dominant speakers
- Capture all perspectives
- Safe space for disagreement
```

**Documentation:**

```markdown
- Clear outcomes documented
- Decisions with rationale
- Action items unambiguous
- Open items tracked
- Next steps clear
```

---

## Workflow Example

**Complete Meeting Flow:**

```markdown
Monday 9 AM: Create meeting page

- Add agenda
- Share with attendees
- Prepare questions

Tuesday 2 PM: Meeting starts

- Follow agenda structure
- Capture in blocks
- Mark decisions with IDs
- Create TODOs immediately
- Note timestamps

Tuesday 2:55 PM: Wrap up

- Review action items
- Confirm owners/deadlines
- Schedule follow-up if needed

Tuesday 3:05 PM: Post-meeting

- Clean up notes (5 min)
- Send summary email (10 min)
- Link to project page
- Add to daily note

Tuesday 3:30 PM: Action items visible in:

- Personal task queries
- Project dashboard
- Team member pages
- Daily agenda

Wednesday-Friday: Work on actions

- Referenced in task queries
- Context from meeting preserved
- Easy to find related decisions

Next Tuesday: Review completion

- Check action item status
- Prepare for follow-up meeting
- Bring forward open items
```

---

## Next Steps

**Implement This Workflow:**

1. Create meeting template (10 min)
2. Set up key queries (10 min)
3. Practice with next meeting (live)
4. Refine template based on needs (10 min)
5. Share with team (optional)

**After 1 Week:**

- Comfortable with template
- Actions tracked automatically
- Notes more structured

**After 1 Month:**

- Meeting efficiency improved
- Nothing falls through cracks
- Easy to find past decisions
- Team sees the benefits

**Related:**

- [Task Management](./task-management-workflows.md)
- [Daily Workflows](./daily-workflows.md)
- [Templates System](../../reference/interface/templates-system.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
