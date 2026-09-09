---
title: 'Daily Workflow Setup Guide'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Task Management Workflows](./task-management-workflows.md)'
  - '[Templates System](../../reference/interface/templates-system.md)'
  - '[Keyboard Shortcuts](../../reference/interface/keyboard-shortcuts.md)'
tags: ['workflow', 'daily-notes', 'journal', 'routine', 'productivity']
---

# Daily Workflow Setup Guide

This guide helps you establish effective daily workflows in Logseq. Learn to structure your day, capture information efficiently, and build sustainable note-taking habits.

**What You'll Learn:**

- Morning and evening routines
- Quick capture techniques
- Daily note templates
- Review workflows
- Habit tracking
- Common workflow patterns

**Time Required:** 30 minutes to set up, 5-15 minutes daily use

---

## Table of Contents

1. [Core Daily Workflow](#core-daily-workflow)
2. [Morning Routine](#morning-routine)
3. [Throughout the Day](#throughout-the-day)
4. [Evening Routine](#evening-routine)
5. [Workflow Templates](#workflow-templates)
6. [Common Patterns](#common-patterns)
7. [Workflow Examples](#workflow-examples)
8. [Troubleshooting](#troubleshooting)

---

## Core Daily Workflow

### The Journal System

**Automatic Daily Notes:**

Every day, Logseq creates a journal page: `[[2026-01-22]]`

**Access Daily Note:**

- Shortcut: `⌘+J` (macOS) / `Ctrl+J` (Windows)
- Click: **Journals** in left sidebar
- Always opens today's page

**Why Use Daily Notes:**

```markdown
✅ Benefits:

- Automatic date organization
- Chronological context
- Easy review (yesterday, last week)
- Low friction capture
- Time-based links

📊 Perfect for:

- Meeting notes
- Daily tasks
- Quick thoughts
- Temporary notes
- Time-stamped entries
```

### Basic Daily Structure

**Minimal Setup:**

```markdown
[[2026-01-22]]

## Tasks

- TODO Review PRs
- TODO Update documentation

## Notes

- Meeting with team
  - Discussed project timeline
  - Action: Follow up with Sarah

## Captures

- Idea: Automate backup script
- Link: [[Article about productivity]]
```

**Enhanced Structure:**

```markdown
[[2026-01-22]]
template:: daily-note

## 🌅 Morning

- TODO Morning review [[[[2026-01-22]]]]
- Daily intention:: Focus on deep work

## 📋 Tasks

priority:: high

- TODO [#A] Complete feature implementation
  DEADLINE: <2026-01-22 Wed>
- TODO Review team's code

## 📝 Notes

- {{embed [[Meeting/Team Sync]]}}

## 💡 Captures

- Quick thoughts go here

## 🌙 Evening

- TODO Evening review
- Gratitude::
- Tomorrow's focus::
```

---

## Morning Routine

### 5-Minute Morning Review

**Step 1: Open Today's Journal**

```markdown
⌘+J → Opens today's journal
```

**Step 2: Review Yesterday**

```markdown
1. Click "Yesterday" link or use ⌘+[
2. Review completed tasks (DONE)
3. Roll forward incomplete tasks:
   - Select TODO blocks
   - ⌘+⇧+L → Move to right sidebar
   - Copy to today's journal
```

**Template for Morning Review:**

```markdown
## Morning Review

- Yesterday: [[[[2026-01-21]]]]
  - Completed: {{query [[[[2026-01-21]]]] (task DONE)}}
  - Pending: {{query [[[[2026-01-21]]]] (task TODO)}}
- Focus today:: [Write your main goal]
- Energy level:: 🟢 High / 🟡 Medium / 🔴 Low
```

### Daily Intention Setting

**Quick Prompt Template:**

```markdown
template:: morning-intention

## Today's Intention

created-at:: <% time %>

### Main Goal

- [What's the ONE thing that would make today successful?]

### Supporting Tasks

- TODO
- TODO
- TODO

### Time Blocks

| Time  | Activity          |
| ----- | ----------------- |
| 9-11  | Deep work: [Task] |
| 11-12 | Email/Admin       |
| 13-15 | Meetings          |
| 15-17 | Project work      |

### Energy Management

- High energy time: [When are you most focused?]
- Protect from: [Distractions to avoid]
```

**Usage:**

```markdown
1. Open today's journal: ⌘+J
2. Type: /template morning-intention
3. Fill in sections (3 minutes)
4. Reference throughout day
```

### Review Scheduled Items

**Query for Today's Tasks:**

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
         [(not= ?m "DONE")]
         [(not= ?m "CANCELLED")]]
 :inputs [:today]
 :collapsed? false}
#+END_QUERY
```

**Add to Daily Template:**

Save this query in your daily note template for automatic agenda view.

---

## Throughout the Day

### Quick Capture

**Fastest Capture Methods:**

**1. Current Journal (No Navigation)**

```markdown
⌘+J → Opens today
↵ → New block
Type → Capture thought
```

**2. Linked Reference (Contextual)**

```markdown
While viewing any page:

- Type thought
- Tag with [[Today]] or #inbox
- Automatically linked to page
```

**3. Quick Capture Template**

```markdown
template:: capture

- 💭 <% current page %> @ <% time %>
  - [Your thought here]
  - tags:: #inbox
```

**Usage:** `/template capture` from anywhere

### Inbox Processing

**Create Inbox System:**

```markdown
Page: [[Inbox]]

Purpose: Temporary holding for unprocessed items

Structure:

- [[Inbox/Thoughts]]
- [[Inbox/Tasks]]
- [[Inbox/Links]]
- [[Inbox/Follow-up]]
```

**Inbox Processing Query:**

```clojure
#+BEGIN_QUERY
{:title "📥 Inbox Items"
 :query [:find (pull ?b [*])
         :where
         [?b :block/refs ?p]
         [?p :block/name "inbox"]]
 :collapsed? false}
#+END_QUERY
```

**Process Daily (5-10 minutes):**

```markdown
1. Review inbox items
2. For each item, decide:
   - Move to project page
   - Create task (TODO)
   - Save to reference page
   - Delete if no longer relevant
3. Clear processed items from inbox
```

### Meeting Notes

**Meeting Template:**

```markdown
template:: meeting

# Meeting: <% current page %>

date:: [[[[<% today %>]]]]
time:: <% time %>
attendees::
duration::
type:: [[Meeting]]

## Agenda

-

## Discussion

-

## Decisions

-

## Action Items

- TODO [[Person]] - [Action]
  DEADLINE: <date>

## Follow-up

-

## Related

- Previous:
- Next:
```

**Usage in Daily Journal:**

```markdown
[[2026-01-22]]

## Meetings

### {{embed [[Meeting/Project Kickoff]]}}

- Created with /template meeting
- All details on dedicated page

### Quick meeting capture

- Can also capture directly in journal
- For brief/informal meetings
```

### Task Management Throughout Day

**Capture Tasks:**

```markdown
⌘+J → Open today
/todo → Create task

Example:

- TODO Email client about proposal
  context:: Mentioned in meeting
  DEADLINE: <2026-01-23 Thu>
```

**Update Task Status:**

```markdown
Click checkbox:
TODO → DOING → DONE

Or use commands:
/doing → DOING
/done → DONE
```

**Move Tasks to Projects:**

```markdown
1. Create task in journal
2. ⌘+⇧+M → Move block
3. Search for project page
4. Task maintains journal link
```

---

## Evening Routine

### 10-Minute Evening Review

**Step 1: Review Today's Journal**

```markdown
⌘+J → Today's journal

Scan:

- What got done? (DONE tasks)
- What's pending? (TODO tasks)
- What did I learn?
- What went well?
```

**Step 2: Process Inbox**

```markdown
Clear inbox items:

- Archive completed
- Move important items to projects
- Delete unnecessary captures
```

**Step 3: Plan Tomorrow**

```markdown
⌘+] → Go to tomorrow's journal
or navigate to [[[[<% tomorrow %>]]]]

Prepare:

- TODO High priority tasks
- Scheduled meetings
- Intentions for tomorrow
```

**Evening Review Template:**

```markdown
template:: evening-review

## Evening Review - [[[[<% today %>]]]]

review-time:: <% time %>

### Completed Today

{{query [[[[<% today %>]]]] (task DONE)}}

### What Went Well ✅

-
-
-

### Challenges 🤔

-
-

### Learnings 📚

-
-

### Tomorrow's Focus

- TODO [#A]
- TODO
- TODO

### Gratitude 🙏

-
-
-

### Energy/Mood

rating:: [1-10]
notes::
```

### Weekly Planning

**Friday Afternoon Routine (30 min):**

```markdown
## Week Review

week:: [[[[2026-W04]]]]

### Wins This Week 🎉

{{query (and [[2026-W04]] (task DONE))}}

### Incomplete Items

{{query (and [[2026-W04]] (task TODO))}}

### Metrics

- Tasks completed: [count]
- Pages created: [count]
- Meetings attended: [count]

### Next Week Priorities

- TODO [[Project A]] - [Milestone]
- TODO [[Project B]] - [Deliverable]
- TODO Personal: [Goal]

### Schedule Review

- Monday: [Key activities]
- Tuesday: [Key activities]
- Wednesday: [Key activities]
- Thursday: [Key activities]
- Friday: [Key activities]
```

---

## Workflow Templates

### Complete Daily Template

**Create:** `[[Templates/Daily Note]]`

```markdown
template:: daily-note

# [[[[<% today %>]]]]

created:: <% time %>
day-of-week:: <% dow %>

---

## 🌅 Morning Review (5 min)

- [ ] Review yesterday: [[[[<% yesterday %>]]]]
- [ ] Set daily intention
- [ ] Check calendar/deadlines

### Today's Focus

goal:: [One main thing to accomplish]
energy:: 🟢🟡🔴

---

## 📅 Scheduled

{{query (or (scheduled <% today %>) (deadline <% today %>))}}

---

## 📋 Tasks

### High Priority

- TODO [#A]

### Regular

- TODO

### Waiting On

- WAITING

---

## 📝 Meetings & Calls

---

## 💡 Notes & Captures

---

## 🌙 Evening Review (10 min)

- [ ] Completed: {{query (and [[[[<% today %>]]]] (task DONE))}}
- [ ] Process inbox
- [ ] Plan tomorrow

### Reflections

went-well::
learned::
improve::

---

## 🔗 Related

- Previous: [[[[<% yesterday %>]]]]
- Next: [[[[<% tomorrow %>]]]]
- Week: [[[[<% week %>]]]]
```

**Auto-Insert Template:**

```markdown
Settings → Editor → Default templates

- Journal template: Templates/Daily Note

Now: Every new journal auto-applies template
```

### Weekly Review Template

**Create:** `[[Templates/Weekly Review]]`

```markdown
template:: weekly-review

# Week [[[[<% week %>]]]] Review

date:: [[[[<% today %>]]]]
week-start:: [[[[<% week-start %>]]]]
week-end:: [[[[<% week-end %>]]]]

---

## 📊 Week at a Glance

### Completed Tasks

{{query (and (between <% week-start %> <% week-end %>) (task DONE))}}
count::

### Goals Progress

- [ ] Goal 1: [Status]
- [ ] Goal 2: [Status]
- [ ] Goal 3: [Status]

---

## 🎯 Highlights

### Wins & Achievements

1.
2.
3.

### Key Learnings

1.
2.
3.

### Challenges Faced

1.
2.

---

## 📈 Metrics

| Metric        | This Week | Last Week | Trend |
| ------------- | --------- | --------- | ----- |
| Tasks Done    |           |           |       |
| Pages Created |           |           |       |
| Meetings      |           |           |       |
| Focus Hours   |           |           |       |

---

## 🔮 Next Week Planning

### Top 3 Priorities

1. TODO [#A]
2. TODO [#A]
3. TODO [#A]

### Projects Advancing

- [[Project 1]]: [Next milestone]
- [[Project 2]]: [Next milestone]

### Time Blocks

Monday::
Tuesday::
Wednesday::
Thursday::
Friday::

---

## 🎨 Improvements

### What Went Well

-

### What to Change

-

### Experiments for Next Week

-

---

## 🔗 References

- Previous week: [[[[<% previous-week %>]]]]
- Next week: [[[[<% next-week %>]]]]
```

---

## Common Patterns

### Morning Person Workflow

```markdown
6:00 AM - Morning routine
⌘+J → Open today's journal
/template morning-intention
Review calendar, set priorities

7:00-9:00 AM - Deep work block
Close right sidebar, focus mode
Work on #A priority tasks

9:00 AM - Quick break & inbox check
Process overnight messages
Update task statuses

Continue day...
```

### Night Owl Workflow

```markdown
Evening work session:
⌘+J → Open today/tomorrow (after midnight)
Capture thoughts in journal
No pressure to organize yet

Next morning:
Review last night's captures
Organize into projects
Process quickly (fresh perspective)
```

### Minimal Workflow (5 min/day)

```markdown
Morning (2 min):
⌘+J → Today
Create 3 TODO items
Note one intention

Evening (3 min):
Check off completed
Forward incomplete to tomorrow
One reflection note
```

### Power User Workflow (30 min/day)

```markdown
Morning (15 min):

- Structured review template
- Calendar sync check
- Email → Tasks pipeline
- Set time blocks
- Review queries

Throughout day:

- Quick captures to inbox
- Task status updates
- Meeting templates
- Block references extensively

Evening (15 min):

- Full evening review template
- Inbox processing
- Tomorrow preparation
- Weekly metrics update
- Habit tracking
```

---

## Workflow Examples

### Student Workflow

```markdown
[[2026-01-22]]

## 📚 Classes Today

- [[CS101 - Intro to Programming]]
  - Lecture notes
  - TODO Assignment due Friday
- [[MATH201 - Calculus]]
  - Problem set review

## 📖 Study Sessions

- 9-11 AM: [[CS101]] - Finish lab
- 2-4 PM: [[MATH201]] - Practice problems
- 7-9 PM: [[Essay Writing]] - First draft

## 💡 Captures

- Question: How does recursion work?
  #question [[CS101]]
- Link: [[Article on study techniques]]

## ✅ Today's Tasks

- TODO Complete lab 3
- TODO Study group at 3pm
- TODO Library book return
```

### Developer Workflow

```markdown
[[2026-01-22]]

## 🚀 Sprint Tasks

sprint:: [[2026-Sprint-04]]

- TODO [#A] Fix bug #1234
  SCHEDULED: <2026-01-22 Wed>
  branch:: feature/fix-auth
- TODO Code review for PR #567
- TODO Update API documentation

## 🐛 Debugging Notes

- Issue: Login timeout
  - Tested: Chrome, Firefox
  - Stack trace: [[Debug/Login-Error]]
  - Solution: Increase timeout to 30s

## 💬 Team Sync

- Discussed: Architecture refactor
- Decisions: Use GraphQL for new API
- Action: TODO [[Alice]] - Create design doc

## 📚 Learning

- Read: [[Article/React Best Practices]]
- TODO Try: New testing framework
```

### Manager/Leader Workflow

```markdown
[[2026-01-22]]

## 👥 1-on-1s

- 10:00 [[Person/John]] - Career discussion
  - {{embed [[1-1/John/2026-01-22]]}}
- 14:00 [[Person/Sarah]] - Project check-in

## 📊 Decisions Made

- Approved: Budget for Q1
- Decided: Hire two engineers
  priority:: high
  next-step:: TODO Post job listings

## 🎯 Strategic Thinking

- Initiative: Improve team velocity
  - Ideas: Better tooling, reduce meetings
  - TODO Schedule team retro

## 📨 Important Communications

- Email: Client feedback on demo
  action:: TODO Respond by EOD
- Slack: Team question about priorities
  action:: DONE Clarified in #general
```

---

## Habit Tracking

### Daily Habits

**Habit Tracker Template:**

```markdown
template:: habit-tracker

## Habits - [[[[<% today %>]]]]

### Morning Habits

- [ ] Meditation (10 min)
- [ ] Exercise (30 min)
- [ ] Healthy breakfast
- [ ] Review goals

### Work Habits

- [ ] Deep work block (2 hrs)
- [ ] Inbox zero
- [ ] No social media before lunch

### Evening Habits

- [ ] Evening review
- [ ] Read (30 min)
- [ ] Plan tomorrow
- [ ] Gratitude journal

### Tracking

water:: [glasses]
exercise:: [minutes]
sleep:: [hours]
mood:: [1-10]
```

**Habit Tracking Query:**

```clojure
#+BEGIN_QUERY
{:title "📊 Habit Streak"
 :query [:find (pull ?b [*])
         :where
         [?b :block/content ?c]
         [(clojure.string/includes? ?c "habit-tracker")]
         [?b :block/page ?p]
         [?p :block/journal? true]
         [?p :block/journal-day ?d]
         [(> ?d 20260115)]]  ;; Last 7 days
 :collapsed? false}
#+END_QUERY
```

---

## Troubleshooting

**Problem: Forgetting to use daily notes**

**Solutions:**

```markdown
- Set reminder: Daily at 9 AM
- Make ⌘+J muscle memory
- Keep Logseq open, journal tab active
- Start small: Just 3 bullets/day
```

**Problem: Daily notes too long**

**Solutions:**

```markdown
- Move meeting notes to dedicated pages
- Use {{embed}} instead of inline
- Link to project pages, don't duplicate
- Archive old daily notes monthly
```

**Problem: Can't find old notes**

**Solutions:**

```markdown
- Use tags: #meeting #idea #question
- Link to projects: [[Project Name]]
- Use queries to surface content
- Rely on linked references
```

**Problem: Too many incomplete tasks**

**Solutions:**

```markdown
- Weekly task review: DONE or DELETE
- Use CANCELLED for abandoned tasks
- Set realistic daily limits (3-5 tasks)
- Move future tasks to project pages
```

---

## Next Steps

**Start Your Daily Workflow:**

1. Choose a workflow pattern (minimal/standard/power)
2. Create your daily template
3. Set up morning/evening routines
4. Practice for one week
5. Adjust based on what works

**Related:**

- [Task Management Workflows](./task-management-workflows.md)
- [Templates System](../../reference/interface/templates-system.md)
- [Keyboard Shortcuts](../../reference/interface/keyboard-shortcuts.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
