---
title: 'Task Management Workflows'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Daily Workflows](./daily-workflows.md)'
  - '[Templates System](../../reference/interface/templates-system.md)'
  - '[Query Patterns](../../reference/advanced/query-patterns.md)'
tags: ['tasks', 'todo', 'gtd', 'productivity', 'workflow']
---

# Task Management Workflows

This guide shows you how to manage tasks effectively in Logseq. Learn different task management methodologies (GTD, PARA, custom) and build a system that works for you.

**What You'll Learn:**

- Task creation and organization
- Task properties and metadata
- GTD (Getting Things Done) workflow
- PARA method implementation
- Custom task systems
- Queries for task views
- Review workflows

**Time Required:** 30-60 minutes to set up

---

## Table of Contents

1. [Task Basics](#task-basics)
2. [Task Properties](#task-properties)
3. [GTD Workflow](#gtd-workflow)
4. [PARA Method](#para-method)
5. [Simple Task System](#simple-task-system)
6. [Task Queries](#task-queries)
7. [Review Workflows](#review-workflows)
8. [Advanced Patterns](#advanced-patterns)

---

## Task Basics

### Creating Tasks

**Basic Task:**

```markdown
- TODO Task description

Shortcut: /todo or /t
Result: Creates TODO marker
```

**Task States:**

```markdown
TODO → Not started
DOING → In progress
DONE → Completed
WAITING → Blocked/waiting on someone
LATER → Backlog
NOW → High priority/urgent
CANCELLED → Abandoned
```

**Changing States:**

```markdown
Method 1: Click checkbox
TODO → DOING → DONE

Method 2: Slash commands
/doing → Changes to DOING
/done → Changes to DONE

Method 3: Keyboard
Cmd+Enter → Toggle TODO/DONE
```

### Task Hierarchy

**Parent-Child Tasks:**

```markdown
- TODO Complete project deliverable
  - TODO Research requirements
    - DONE Review documentation
    - TODO Talk to stakeholders
  - TODO Create design
  - TODO Implementation
  - TODO Testing
```

**Benefits:**

```markdown
✅ Break down complex tasks
✅ Track subtask progress
✅ Maintain context
✅ Collapse/expand for focus
```

### Task Metadata

**Essential Properties:**

```markdown
- TODO Task description
  DEADLINE: <2026-01-30 Wed>
  SCHEDULED: <2026-01-22 Wed>
  priority:: [#A]
  project:: [[Project Alpha]]
  context:: [[Work]]
  energy:: high
  time:: 30min
```

---

## Task Properties

### Deadlines

**Add Deadline:**

```markdown
Method 1: Slash command
/deadline → Date picker

Method 2: Manual
DEADLINE: <2026-01-30 Wed>

Method 3: Relative
DEADLINE: <+7d> → 7 days from today
```

**Deadline Queries:**

```clojure
#+BEGIN_QUERY
{:title "⚠️ Due This Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/deadline ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]
         [?b :block/marker ?m]
         [(not= ?m "DONE")]]
 :inputs [:today :7d-after]
 :collapsed? false}
#+END_QUERY
```

### Scheduled Dates

**Schedule Task:**

```markdown
SCHEDULED: <2026-01-22 Wed>

Meaning: Plan to start on this date

Difference:

- SCHEDULED: When you'll work on it
- DEADLINE: When it must be done
```

**Repeating Tasks:**

```markdown
- TODO Weekly review
  SCHEDULED: <2026-01-22 Wed +1w>
  ↑ Repeats every week

- TODO Pay rent
  SCHEDULED: <2026-02-01 Sat +1m>
  ↑ Repeats monthly
```

### Priority Levels

**Set Priority:**

```markdown
/priority-a → [#A] Highest
/priority-b → [#B] Medium
/priority-c → [#C] Low

Or manual:

- TODO [#A] Critical task
- TODO [#B] Important task
- TODO [#C] Nice to have
```

**Priority Query:**

```clojure
#+BEGIN_QUERY
{:title "🔥 High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/priority "A"]]
 :collapsed? false}
#+END_QUERY
```

### Custom Properties

**Add Context:**

```markdown
- TODO Task description
  project:: [[Project Alpha]]
  context:: [[Work]], [[Client/Acme]]
  energy:: high
  time-estimate:: 2h
  depends-on:: ((other-task-ref))
  owner:: [[Person/Sarah]]
  status:: blocked
  waiting-for:: [[Person/John]] approval
```

---

## GTD Workflow

### Getting Things Done Overview

**GTD Phases:**

```markdown
1. Capture → Collect everything
2. Clarify → Process inbox
3. Organize → Categorize tasks
4. Reflect → Review regularly
5. Engage → Do the work
```

### Capture Phase

**Inbox Setup:**

```markdown
Create: [[Inbox]]

Quick capture:

- TODO [Anything that comes to mind]
  captured:: [[2026-01-22]]
  tags:: #inbox

No organizing yet, just capture
```

**Capture Template:**

```markdown
template:: quick-capture

- TODO <% cursor %>
  captured:: [[<% today %>]]
  source:: <% current page %>
  tags:: #inbox
```

### Clarify Phase

**Process Inbox Daily:**

```markdown
For each inbox item, ask:

1. Is it actionable?
   No → Delete or move to [[Reference]]
   Yes → Continue

2. What's the next action?
   → Define specific task

3. Will it take < 2 minutes?
   Yes → Do it now
   No → Continue

4. Is it a project (multiple steps)?
   Yes → Create project page
   No → Single task

5. When to do it?
   → Add SCHEDULED date
```

**Clarify Template:**

```markdown
template:: clarify-task

- TODO [[Task description]]
  next-action:: [Specific next step]
  project:: [[Project Name]] or none
  context:: [[Context]]
  time-estimate:: [minutes]
  energy:: high/medium/low
  SCHEDULED: <date>
```

### Organize Phase

**GTD Categories:**

```markdown
[[GTD/Next Actions]]

- Tasks ready to do now
- One action, no dependencies

[[GTD/Projects]]

- Multi-step outcomes
- Each has next actions

[[GTD/Waiting For]]

- Tasks blocked by others
- Regular follow-up needed

[[GTD/Someday Maybe]]

- Not now, but keep
- Review monthly

[[GTD/Reference]]

- Not actionable
- Keep for information
```

**Organization Structure:**

```markdown
- TODO Task description
  gtd-category:: [[GTD/Next Actions]]
  context:: [[Work]]
  project:: [[Project Alpha]]

Query shows all in category
```

### Context-Based Organization

**GTD Contexts:**

```markdown
[[Context/Office]] → At desk, computer
[[Context/Home]] → Personal space
[[Context/Phone]] → Calls to make
[[Context/Errands]] → Out and about
[[Context/Meeting]] → With specific person
[[Context/Computer]] → Any computer work
[[Context/Low-Energy]] → Simple tasks
```

**Context Query:**

```clojure
#+BEGIN_QUERY
{:title "📍 Tasks by Context"
 :query [:find (pull ?b [*])
         :in $ ?context
         :where
         [?b :block/marker "TODO"]
         [?b :block/refs ?p]
         [?p :block/name ?context]]
 :inputs ["context/office"]
 :collapsed? false}
#+END_QUERY
```

### Reflect Phase

**Weekly Review Template:**

```markdown
template:: gtd-weekly-review

# GTD Weekly Review - [[<% today %>]]

## 1. Get Clear

- [ ] Process all inboxes to zero
- [ ] Review [[GTD/Next Actions]]
- [ ] Review [[GTD/Projects]]
- [ ] Review [[GTD/Waiting For]]

## 2. Get Current

- [ ] Review calendar (past & future)
- [ ] Review action lists
- [ ] Update project next actions

## 3. Get Creative

- [ ] Review [[GTD/Someday Maybe]]
- [ ] New projects to add?
- [ ] Goals alignment check

## 4. Stats

- Completed this week: {{query (and [[<% week %>]] (task DONE))}}
- Active projects: [count]
- Waiting items: [count]

## 5. Next Week Planning

- [ ] High-priority projects
- [ ] Calendar commitments
- [ ] Key outcomes desired
```

### Engage Phase

**Choose Next Action:**

```markdown
Decision criteria:

1. Context: Where am I?
2. Time: How much do I have?
3. Energy: What's my level?
4. Priority: What's most important?

Query for context + time:
#+BEGIN_QUERY
{:title "✅ Doable Now"
:query [:find (pull ?b [*])
:in $ ?context
:where
[?b :block/marker "TODO"]
[?b :block/refs ?p]
[?p :block/name ?context]
[?b :block/properties ?props]
[(get ?props :time-estimate) ?time]
[(<= ?time "30min")]]
:inputs ["context/office"]}
#+END_QUERY
```

---

## PARA Method

### PARA Overview

**Four Categories:**

```markdown
Projects → Active work with deadline
Areas → Ongoing responsibilities
Resources → Topics of interest
Archives → Inactive items
```

### Projects

**Project Definition:**

```markdown
Criteria:

- Has specific outcome
- Has deadline
- Multiple tasks required
- Active (working on now)

Examples:

- ✅ "Launch marketing campaign by Q2"
- ✅ "Write research paper by March"
- ❌ "Stay healthy" (area, not project)
- ❌ "Learn about AI" (resource, not project)
```

**Project Template:**

```markdown
template:: para-project

# [[Projects/<Project Name>]]

created:: [[<% today %>]]
deadline:: <date>
status:: active

## Outcome

[Specific result to achieve]

## Next Actions

- TODO
- TODO

## Resources

- [[Resources/Topic]]

## Notes

-

## Archive Date

[When completed/cancelled]
```

**Projects Dashboard:**

```clojure
#+BEGIN_QUERY
{:title "📊 Active Projects"
 :query [:find (pull ?b [*])
         :where
         [?b :block/namespace ?ns]
         [?ns :block/name "projects"]
         [?b :block/properties ?props]
         [(get ?props :status) "active"]]
 :collapsed? false}
#+END_QUERY
```

### Areas

**Area Definition:**

```markdown
Criteria:

- Ongoing responsibility
- No end date
- Standard to maintain
- Multiple projects may relate

Examples:

- ✅ Health & Fitness
- ✅ Financial Management
- ✅ Career Development
- ✅ Home Maintenance
```

**Area Template:**

```markdown
template:: para-area

# [[Areas/<Area Name>]]

created:: [[<% today %>]]

## Responsibilities

- [What needs maintaining]

## Standards

- [Quality expectations]

## Active Projects

- [[Projects/...]]

## Routine Tasks

- TODO Weekly review
  SCHEDULED: <date +1w>

## Resources

- [[Resources/...]]
```

### Resources

**Resource Definition:**

```markdown
Criteria:

- Topic of interest
- No action required
- Reference material
- Learning/research

Examples:

- ✅ Machine Learning
- ✅ Photography Techniques
- ✅ Product Design
- ✅ Personal Finance
```

**Resource Structure:**

```markdown
[[Resources/<Topic>]]

## Overview

[What this topic covers]

## Key Concepts

- [[Concept 1]]
- [[Concept 2]]

## Sources

- [[Book/Title]]
- [[Article/Title]]

## Notes

- Learning notes
- Insights
- Examples

## Related Projects

- [[Projects/...]] (using this resource)
```

### Archives

**Archive Criteria:**

```markdown
Move to Archives when:

- Project completed ✅
- Project cancelled ❌
- No longer relevant 📦
- Keeping for reference only 📚

Examples:

- Completed projects
- Old areas no longer maintained
- Outdated resources
```

**Archive Process:**

```markdown
1. Move page to Archive namespace
   [[Projects/Old]] → [[Archives/Projects/Old]]

2. Update properties
   status:: completed
   archived:: [[2026-01-22]]

3. Remove from active queries
   (Namespace-based queries auto-exclude)

4. Keep for reference
   Still searchable
   Still linked
```

### PARA Task Flow

**Capture → Categorize:**

```markdown
1. New task captured:

   - TODO Write blog post about PARA

2. Ask: Is this a project or task?
   → Multi-step? → Project
   → Single task? → Add to project/area

3. Assign category:

   - TODO Write blog post
     project:: [[Projects/Content Marketing]]
     area:: [[Areas/Professional Development]]
     resources:: [[Resources/Writing]]

4. Add metadata:
   - deadline::
   - context::
   - next-action::
```

---

## Simple Task System

### Minimalist Approach

**Three Lists:**

```markdown
[[Tasks/Today]]

- What I'm doing today
- 3-5 items max
- Review morning & evening

[[Tasks/This Week]]

- Week's priorities
- 10-15 items max
- Review weekly

[[Tasks/Someday]]

- Ideas for future
- No limit
- Review monthly
```

**Daily Process:**

```markdown
Morning (5 min):

1. Open [[Tasks/Today]]
2. Choose 3-5 tasks from [[Tasks/This Week]]
3. Add to today
4. Focus only on today's list

Evening (3 min):

1. Check off completed
2. Move incomplete to tomorrow or this week
3. Add any new tomorrow items
```

### Personal Kanban

**Board Structure:**

```markdown
[[Kanban/Backlog]]

- All future tasks
- Rough priority order

[[Kanban/Ready]]

- Next tasks to start
- Limit: 5 items

[[Kanban/Doing]]

- Currently working
- Limit: 3 items (WIP limit)

[[Kanban/Done]]

- Completed this week
- Archive weekly
```

**Move tasks through stages:**

```markdown
- TODO Task description
  status:: backlog

Move: Update property
status:: ready → doing → done
```

**Visual Query:**

```clojure
#+BEGIN_QUERY
{:title "🎯 Kanban Board"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING" "DONE"} ?m)]]
 :group-by-page? true
 :collapsed? false}
#+END_QUERY
```

---

## Task Queries

### Essential Task Views

**Today's Tasks:**

```clojure
#+BEGIN_QUERY
{:title "📅 Today's Agenda"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         (or
           [?b :block/scheduled ?d]
           [?b :block/deadline ?d])
         [(= ?d ?today)]
         [?b :block/marker ?m]
         [(not= ?m "DONE")]]
 :inputs [:today]
 :collapsed? false}
#+END_QUERY
```

**Overdue Tasks:**

```clojure
#+BEGIN_QUERY
{:title "⚠️ Overdue"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/deadline ?d]
         [(< ?d ?today)]
         [?b :block/marker "TODO"]]
 :inputs [:today]
 :collapsed? false}
#+END_QUERY
```

**By Priority:**

```clojure
#+BEGIN_QUERY
{:title "🔥 Priority A Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/priority "A"]]
 :collapsed? false}
#+END_QUERY
```

**By Project:**

```clojure
#+BEGIN_QUERY
{:title "📋 Project Tasks"
 :query [:find (pull ?b [*])
         :in $ ?project
         :where
         [?b :block/marker ?m]
         [(contains? #{"TODO" "DOING"} ?m)]
         [?b :block/refs ?p]
         [?p :block/name ?project]]
 :inputs ["projects/alpha"]
 :collapsed? false}
#+END_QUERY
```

**Quick Wins (< 15 min):**

```clojure
#+BEGIN_QUERY
{:title "⚡ Quick Wins"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/properties ?props]
         [(get ?props :time-estimate) ?time]
         [(or
           (= ?time "5min")
           (= ?time "10min")
           (= ?time "15min"))]]
 :collapsed? false}
#+END_QUERY
```

### Advanced Queries

**Energy-Based:**

```clojure
#+BEGIN_QUERY
{:title "⚡ High Energy Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/properties ?props]
         [(get ?props :energy) "high"]]
 :collapsed? false}
#+END_QUERY
```

**Waiting Items:**

```clojure
#+BEGIN_QUERY
{:title "⏳ Waiting For"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "WAITING"]
         [?b :block/properties ?props]
         [(get ?props :waiting-for)]]
 :collapsed? false}
#+END_QUERY
```

**Completed This Week:**

```clojure
#+BEGIN_QUERY
{:title "✅ Completed This Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/marker "DONE"]
         [?b :block/page ?p]
         [?p :block/journal? true]
         [?p :block/journal-day ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:7d-before :today]
 :collapsed? false}
#+END_QUERY
```

---

## Review Workflows

### Daily Review

**Morning Review (5 min):**

```markdown
template:: daily-morning-review

## Morning Review - [[<% today %>]]

### Yesterday

- Completed: {{query [[<% yesterday %>]] (task DONE)}}
- Pending: {{query [[<% yesterday %>]] (task TODO)}}

### Today's Plan

- [ ] Review calendar
- [ ] Choose top 3 priorities
- [ ] Check deadlines

### Top 3 Today

1. TODO [#A]
2. TODO [#A]
3. TODO [#A]
```

**Evening Review (10 min):**

```markdown
template:: daily-evening-review

## Evening Review - [[<% today %>]]

### Completed

{{query [[<% today %>]] (task DONE)}}

### Incomplete

{{query [[<% today %>]] (task TODO DOING)}}

### Rollover Decisions

- [ ] Move to tomorrow: [which tasks?]
- [ ] Reschedule: [which tasks?]
- [ ] Cancel: [which tasks?]

### Reflection

went-well::
learned::
improve-tomorrow::
```

### Weekly Review

**Weekly Review Process (30 min):**

```markdown
template:: weekly-review

# Weekly Review - Week [[<% week %>]]

date:: [[<% today %>]]

## 1. Gather & Process (10 min)

- [ ] Clear inboxes
- [ ] Process loose notes
- [ ] File references
- [ ] Update task statuses

## 2. Review (15 min)

### Completed This Week

{{query (and (between <% week-start %> <% week-end %>) (task DONE))}}
Count:

### Incomplete Tasks

{{query (task TODO DOING WAITING)}}
Action: Reschedule, cancel, or keep

### Projects Review

- [ ] [[Projects/Alpha]] - Status? Next action?
- [ ] [[Projects/Beta]] - Status? Next action?

### Areas Review

- [ ] [[Areas/Health]] - Standard maintained?
- [ ] [[Areas/Career]] - Standard maintained?

## 3. Plan Next Week (5 min)

### Priorities

1.
2.
3.

### Key Events

monday::
tuesday::
wednesday::
thursday::
friday::

### Intentions

focus-area::
big-goal::
time-blocks::
```

### Monthly Review

**Monthly Review (60 min):**

```markdown
template:: monthly-review

# Monthly Review - [[<% month %>]]

date:: [[<% today %>]]

## 1. Projects (20 min)

- [ ] Review all active projects
- [ ] Archive completed projects
- [ ] Evaluate stalled projects
- [ ] Plan new projects

### Project Health

{{query (property :project)}}

### Decisions

- Archive:
- Continue:
- Start new:

## 2. Areas (20 min)

- [ ] Are standards being met?
- [ ] New areas to add?
- [ ] Areas to archive?

### Area Standards Check

- [[Areas/Health]]: ⭐⭐⭐☆☆
- [[Areas/Career]]: ⭐⭐⭐⭐☆
- [[Areas/Finance]]: ⭐⭐⭐⭐⭐

## 3. Goals Review (10 min)

- [ ] Progress on yearly goals?
- [ ] Quarterly milestones?
- [ ] Course corrections needed?

## 4. Systems (10 min)

- [ ] Is task system working?
- [ ] Queries need updating?
- [ ] Templates need revision?
- [ ] New habits to build?

## 5. Next Month (10 min)

- Key outcomes desired:
- Projects to complete:
- New initiatives:
```

---

## Advanced Patterns

### Task Dependencies

**Link Dependent Tasks:**

```markdown
- TODO Complete design
  id:: design-task
- TODO Start implementation
  depends-on:: ((design-task))
  SCHEDULED: <after design done>
```

**Query for Blocked Tasks:**

```clojure
#+BEGIN_QUERY
{:title "🚫 Blocked Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/properties ?props]
         [(get ?props :depends-on)]]
 :collapsed? false}
#+END_QUERY
```

### Time Tracking

**Track Time Spent:**

```markdown
- TODO Task description
  time-estimate:: 2h
  time-spent:: 1.5h
  time-remaining:: 0.5h

Update as you work
```

### Habit Tracking

**Recurring Habits:**

```markdown
- TODO Morning workout
  SCHEDULED: <2026-01-22 Wed +1d>
  habit:: true
  streak:: 7 days

Repeats daily (+1d)
Track streak manually or query
```

**Habit Streak Query:**

```clojure
#+BEGIN_QUERY
{:title "🔥 Habit Streaks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :habit) true]]
 :collapsed? false}
#+END_QUERY
```

### Batch Processing

**Batch Similar Tasks:**

```markdown
- Batch: Email responses
  context:: [[Context/Computer]]
  batch:: true

  - TODO Reply to Sarah
  - TODO Reply to John
  - TODO Reply to client

Do all at once (time-efficient)
```

---

## Best Practices

### Task Writing

**Good Task Format:**

```markdown
✅ Action-oriented:

- TODO Call Sarah about project timeline
- TODO Review and merge PR #123
- TODO Schedule dentist appointment

❌ Vague:

- TODO Sarah
- TODO Code review
- TODO Dentist
```

### Reasonable Limits

**Avoid Overwhelm:**

```markdown
Daily: 3-5 must-do tasks
Weekly: 10-15 priority tasks
Total active: < 50 tasks

If more:

- Archive old tasks
- Cancel unnecessary tasks
- Delegate if possible
```

### Regular Maintenance

**Weekly Tasks:**

```markdown
- [ ] Archive completed tasks
- [ ] Review stale tasks (>2 weeks)
- [ ] Update deadlines
- [ ] Clean up WAITING items
- [ ] Process inbox to zero
```

---

## Next Steps

**Build Your System:**

1. Choose methodology (GTD, PARA, Simple)
2. Set up pages and templates
3. Create essential queries
4. Start daily review habit
5. Refine system weekly

**Related:**

- [Daily Workflows](./daily-workflows.md)
- [Query Patterns](../../reference/advanced/query-patterns.md)
- [Templates System](../../reference/interface/templates-system.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
