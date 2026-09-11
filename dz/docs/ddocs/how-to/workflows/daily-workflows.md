---
title: 'Daily Workflow Setup Guide'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
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

> **This guide is for DB graphs.** Tasks are blocks tagged `#Task` carrying a
> `Status` property — there are no `TODO`/`DOING`/`DONE` markers, no `[#A]`
> priorities and no `SCHEDULED:`/`DEADLINE:` timestamps. See
> [Task Management Workflows](./task-management-workflows.md) for the full
> mapping. The `key: value` lines in the outline examples below are shorthand for
> "this node carries this property"; add them with `/Add property`
> (`src/main/frontend/commands.cljs:354-358`) rather than typing `key:: value`,
> which a DB graph does not parse.

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

- Shortcut: `g j` — go to Journals
  (`src/main/frontend/modules/shortcut/config.cljs:328-329`)
- Shortcut: `⌘+⇧+J` (macOS) / `Alt+⇧+J` — open today in the **right sidebar**
  (`config.cljs:340-341`)
- Click: **Journals** in left sidebar

`⌘+J` is not the journal shortcut — it is `:editor/jump`, the jump-to-block
navigator (`config.cljs:447-448`). Other journal navigation:
`g t` tomorrow, `g n` next journal, `g p` previous journal
(`config.cljs:405-412`); `⌘+[` / `⌘+]` are browser-history back/forward
(`config.cljs:331-335`), not day-to-day navigation.

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

- Review PRs           #Task  Status: Todo
- Update documentation #Task  Status: Todo

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

## 🌅 Morning

- Morning review  #Task  Status: Todo
- Daily intention: Focus on deep work

## 📋 Tasks

- Complete feature implementation  #Task
  Status:   Todo
  Priority: Urgent
  Deadline: 2026-01-22
- Review team's code  #Task  Status: Todo

## 📝 Notes

- {{embed [[Team Sync]]}}

## 💡 Captures

- Quick thoughts go here

## 🌙 Evening

- Evening review  #Task  Status: Todo
- Gratitude:
- Tomorrow's focus:
```

---

## Morning Routine

### 5-Minute Morning Review

**Step 1: Open Today's Journal**

```markdown
g j → Journals (today is at the top)
```

**Step 2: Review Yesterday**

```markdown
1. g p → previous journal (config.cljs:411-412)
2. Review the Done tasks
3. Roll forward incomplete tasks:
   - Select the blocks
   - shift+enter → open selected blocks in the right sidebar
     (config.cljs:193-194)
   - Copy to today's journal
```

**Template for Morning Review:**

```markdown
## Morning Review

- Yesterday: <% yesterday %>
  - Completed
    - (and <% yesterday %> (task done))        ← /Query block
  - Pending
    - (and <% yesterday %> (task todo doing))        ← /Query block
- Focus today: [Write your main goal]
- Energy level: 🟢 High / 🟡 Medium / 🔴 Low
```

> **`{{query …}}` does not work in a DB graph.** Typed into a block it renders only a
> deprecation warning (`src/main/frontend/components/block.cljs:1870-1871`). Entered as the query
> of a `/Query` block it becomes a full-text search for that literal text, because `sanitize-q`
> quotes anything that does not start with `(`, `"`, a page ref or `[?`
> (`src/main/frontend/components/query/builder.cljs:543-552`). Every simple query in this guide is
> therefore shown as the bare form you enter after typing `/Query`.

> **Query lines in these templates.** A line marked `← /Query block` is a block of its own: type
> `/Query` on it and enter the form shown. `/Query` moves the block's whole title into the query
> (`src/main/frontend/handler/editor.cljs:4377-4394`), so a label cannot share the line and goes in
> the parent block instead. `<% today %>` and `<% yesterday %>` already insert a page reference
> (`deps/outliner/src/logseq/outliner/template.cljs:74`), so they are written bare, not inside
> `[[…]]`. Inside a template they are fixed to the day the template is inserted; in a standalone
> `/Query` they are recomputed each time the query runs
> (`src/main/frontend/worker/query_dsl.cljs:92-110`).

Two simple-query filters need an explicit `and` — `[[Page]] (task done)`
is not an implicit conjunction; `and`, `or` and `not` are the only combinators
(`src/main/frontend/worker/query_dsl.cljs:548-549`).

### Daily Intention Setting

**Quick Prompt Template:**

```markdown
- Morning intention              ← tagged Template

## Today's Intention

created-at: <% time %>

### Main Goal

- [What's the ONE thing that would make today successful?]

### Supporting Tasks

- (task)  #Task  Status: Todo
- (task)  #Task  Status: Todo
- (task)  #Task  Status: Todo

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
1. Open today's journal: g j
2. Type: /Template, then pick "Morning intention"
   (src/main/frontend/commands.cljs:339-343)
3. Fill in sections (3 minutes)
4. Reference throughout day
```

### Review Scheduled Items

**Query for Today's Tasks:**

Deadline and Scheduled are `:datetime` properties holding **epoch
milliseconds** (`deps/db/src/logseq/db/frontend/property.cljs:340-353`), so
compare them against millisecond inputs. `:today` resolves to a YYYYMMDD integer
(`deps/db/src/logseq/db/frontend/inputs.cljs:99-100`) and comparing it against a
deadline silently matches nothing.

Use `(task …)` rather than a raw `[?b :logseq.property/status ?s]` clause:
Status has a default value of Todo (`property.cljs:320`) and defaults are not
stored as datoms, so a raw clause misses every task whose status was never set
by hand. The `task` rule reads through the default
(`deps/db/src/logseq/db/frontend/rules.cljc:297-300`), and Logseq injects the
rules for you when it sees the name in `:where`
(`src/main/frontend/worker/handler/query.cljs:185-204`).

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         (or
          [?b :logseq.property/scheduled ?d]
          [?b :logseq.property/deadline ?d])
         [(>= ?d ?start)]
         [(<= ?d ?end)]
         (task ?b #{"Backlog" "Todo" "Doing" "In Review"})]
 :inputs [:start-of-today-ms :end-of-today-ms]}
```

Write it with `/Advanced Query`, which tags the block `#Query` and stores the map
in `:logseq.property/query` (`src/main/frontend/commands.cljs:125-132`). There is
no `#+BEGIN_QUERY` wrapper in a DB graph, and `:title` / `:collapsed?` /
`:breadcrumb-show?` / `:group-by-page?` are not honoured on a user query — put
the heading in the parent block
(`src/main/frontend/components/query.cljs:167-175`;
`src/main/frontend/components/query/result.cljs:10-11`).

**Add to Daily Template:**

Save this query in your daily note template for automatic agenda view.

---

## Throughout the Day

### Quick Capture

**Fastest Capture Methods:**

**1. Quick add (no navigation at all)**

```markdown
⌘+E (macOS) / Ctrl+Alt+E → Quick add
Type → Capture thought → "Add to today"
```

`:editor/quick-add` is bound to `mod+e` on macOS and `mod+alt+e` elsewhere
(`src/main/frontend/modules/shortcut/config.cljs:444-446`) and files the block on
today's journal (`src/resources/dicts/en.edn:514-515`).

**2. Linked Reference (Contextual)**

```markdown
While viewing any page:

- Type thought
- Tag with [[Today]] or #inbox
- Automatically linked to page
```

**3. Quick Capture Template**

```markdown
- Capture                        ← tagged Template
  - 💭 <% current page %> @ <% time %>
    - [Your thought here]  #inbox
```

**Usage:** `/Template` → "Capture", from anywhere. `<% current page %>` and
`<% time %>` are two of the five substituted variables; any other name loses its
`<% %>` wrapper and is left as bare text
(`deps/outliner/src/logseq/outliner/template.cljs:104-112,114-128`).

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

Type `/Query`, then enter:

```clojure
[[inbox]]
```

**Process Daily (5-10 minutes):**

```markdown
1. Review inbox items
2. For each item, decide:
   - Move to project page
   - Make it a task (/Todo)
   - Save to reference page
   - Delete if no longer relevant
3. Clear processed items from inbox
```

### Meeting Notes

**Meeting Template:**

```markdown
- Meeting                        ← tagged Template

# Meeting: <% current page %>

date:      <% today %>
time:      <% time %>
attendees:
duration:
type:      [[Meeting]]

## Agenda

-

## Discussion

-

## Decisions

-

## Action Items

- [Action]  #Task
  Status:   Todo
  Assignee: [[Person]]
  Deadline: <pick a date>

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

### {{embed [[Project Kickoff]]}}

- Created with /Template → "Meeting"
- All details on dedicated page

### Quick meeting capture

- Can also capture directly in journal
- For brief/informal meetings
```

### Task Management Throughout Day

**Capture Tasks:**

```markdown
g j  → Open today
/Todo → make the block a task

Example:

- Email client about proposal  #Task
  Status:   Todo
  Deadline: 2026-01-23
  context:  Mentioned in meeting
```

**Update Task Status:**

```markdown
Click the status control on the block and pick a value.

Or use the slash commands, which are generated from the six closed values of
:logseq.property/status (src/main/frontend/commands.cljs:150-165):
/Backlog  /Todo  /Doing  /In Review  /Done  /Canceled

Or Cmd/Ctrl+Enter, which cycles Todo → Doing → Done → (no status)
(config.cljs:167; src/main/frontend/handler/editor.cljs:819-831).
```

**Move Tasks to Projects:**

```markdown
1. Create the task in the journal
2. Select it, then ⌘+⇧+M → Move blocks
   (:editor/move-blocks, config.cljs:187-188)
3. Search for the project page
4. The block keeps its links
```

---

## Evening Routine

### 10-Minute Evening Review

**Step 1: Review Today's Journal**

```markdown
g j → Journals

Scan:

- What got done? (Done tasks)
- What's pending? (Todo / Doing / In Review tasks)
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
g t → Go to tomorrow's journal (config.cljs:405-406)
or navigate to <% tomorrow %>

Prepare:

- High-priority tasks  #Task  Status: Todo, Priority: Urgent
- Scheduled meetings
- Intentions for tomorrow
```

**Evening Review Template:**

```markdown
- Evening review                 ← tagged Template

## Evening Review - <% today %>

review-time: <% time %>

### Completed Today

(and <% today %> (task done))        ← /Query block

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

- (task)  #Task  Status: Todo, Priority: Urgent
- (task)  #Task  Status: Todo
- (task)  #Task  Status: Todo

### Gratitude 🙏

-
-
-

### Energy/Mood

rating: [1-10]
notes:
```

### Weekly Planning

**Friday Afternoon Routine (30 min):**

```markdown
## Week Review

### Wins This Week 🎉

(and (between -7d today) (task done))        ← /Query block

### Incomplete Items

(and (between -7d today) (task todo doing))        ← /Query block

`(between …)` filters on the **journal page** a block sits on: the rule requires
`[?p :block/tags :logseq.class/Journal]` and compares `:block/journal-day`, a
YYYYMMDD integer (`deps/db/src/logseq/db/frontend/rules.cljc:94-100`). Its first
argument is the block, not a date. There is no `:block/journal?` attribute and no
`[[2026-W04]]` week page — Logseq creates one journal page per day.

### Metrics

- Tasks completed: [count]
- Pages created: [count]
- Meetings attended: [count]

### Next Week Priorities

- [[Project A]] - [Milestone]   #Task  Status: Todo
- [[Project B]] - [Deliverable] #Task  Status: Todo
- Personal: [Goal]              #Task  Status: Todo

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

**Create:** a block called `Daily Note`, tagged **Template**.

```markdown
- Daily Note                     ← tagged Template

created: <% time %>

---

## 🌅 Morning Review (5 min)

- [ ] Review yesterday: <% yesterday %>
- [ ] Set daily intention
- [ ] Check calendar/deadlines

### Today's Focus

goal:   [One main thing to accomplish]
energy: 🟢🟡🔴

---

## 📅 Scheduled

(insert the "Today's Agenda" advanced query from
[Review Scheduled Items](#review-scheduled-items) here)

There is no `(scheduled …)` or `(deadline …)` simple-query operator. The whole
operator set is `and`, `or`, `not`, `[[Page]]`, a bare string (full-text),
`(between …)`, `(property …)`, `(private-property …)`, `(task …)` / `(todo …)`,
`(priority …)`, `(page …)`, `(sample …)` and `(tags …)`
(`src/main/frontend/worker/query_dsl.cljs:529-575`). `(between …)` does accept a
property name as its first argument, but the resolver strips the namespace before
checking it, so only `created-at` and `updated-at` actually resolve
(`deps/db/src/logseq/db/frontend/query_dsl.cljs:55-71`) — Deadline and Scheduled
need an advanced query.

---

## 📋 Tasks

### High Priority

- (task)  #Task  Status: Todo, Priority: Urgent

### Regular

- (task)  #Task  Status: Todo

### Waiting On

- (task)  #Task  Status: In Review

There is no `Waiting` status; the six are Backlog, Todo, Doing, In Review, Done
and Canceled (`deps/db/src/logseq/db/frontend/property.cljs:313-318`).

---

## 📝 Meetings & Calls

---

## 💡 Notes & Captures

---

## 🌙 Evening Review (10 min)

- [ ] Completed
  - (and <% today %> (task done))        ← /Query block
- [ ] Process inbox
- [ ] Plan tomorrow

### Reflections

went-well:
learned:
improve:

---

## 🔗 Related

- Previous: <% yesterday %>
- Next: <% tomorrow %>
```

`<% week %>` is not a variable — only `today`, `yesterday`, `tomorrow`, `time`
and `current page` are substituted; any other name loses its `<% %>` wrapper, so
`<% week %>` becomes the bare word `week`
(`deps/outliner/src/logseq/outliner/template.cljs:104-112,114-128`).

**Auto-Insert Template:**

The `:default-templates` key in `config.edn` is deprecated for DB graphs; it "is
replaced by #Template and the `Apply template to tags` property"
(`deps/common/src/logseq/common/config.cljs:137-138`).

```markdown
1. Tag the Daily Note block with Template.
2. On that block, set the built-in "Apply template to tags" property
   (:logseq.property/template-applied-to, a :class property, cardinality many —
   deps/db/src/logseq/db/frontend/property.cljs:696-700) to Journal.
3. Every new journal page now gets the template's children inserted
   automatically (src/main/frontend/worker/pipeline.cljs:94-150).
```

`Journal` is normally a private tag, but the selector for this property
deliberately includes private tags
(`src/main/frontend/worker/handler/property.cljs:127-131`).

### Weekly Review Template

**Create:** a block called `Weekly Review`, tagged **Template**.

```markdown
- Weekly Review                  ← tagged Template

# Weekly Review

date: <% today %>

---

## 📊 Week at a Glance

### Completed Tasks

(and (between -7d today) (task done))        ← /Query block
count:

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

1. (task)  #Task  Status: Todo, Priority: Urgent
2. (task)  #Task  Status: Todo, Priority: Urgent
3. (task)  #Task  Status: Todo, Priority: High

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

- Previous review: [[link to last week's review block]]
- Next review: [[link to next week's review block]]
```

---

## Common Patterns

### Morning Person Workflow

```markdown
6:00 AM - Morning routine
g j → Open today's journal
/Template → "Morning intention"
Review calendar, set priorities

7:00-9:00 AM - Deep work block
Close right sidebar, focus mode
Work on Urgent-priority tasks

9:00 AM - Quick break & inbox check
Process overnight messages
Update task statuses

Continue day...
```

### Night Owl Workflow

```markdown
Evening work session:
g j → Open today/tomorrow (after midnight)
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
g j → Today
Create 3 tasks (/Todo)
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
  - Assignment due Friday  #Task  Status: Todo, Deadline: Friday
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

- Complete lab 3        #Task  Status: Todo
- Study group at 3pm    #Task  Status: Todo
- Library book return   #Task  Status: Todo
```

### Developer Workflow

```markdown
[[2026-01-22]]

## 🚀 Sprint Tasks

sprint:: [[2026-Sprint-04]]

- Fix bug #1234  #Task
  Status:    Todo
  Priority:  Urgent
  Scheduled: 2026-01-22
  branch:    feature/fix-auth
- Code review for PR #567    #Task  Status: Todo
- Update API documentation   #Task  Status: Todo

## 🐛 Debugging Notes

- Issue: Login timeout
  - Tested: Chrome, Firefox
  - Stack trace: [[Debug/Login-Error]]
  - Solution: Increase timeout to 30s

## 💬 Team Sync

- Discussed: Architecture refactor
- Decisions: Use GraphQL for new API
- Action: Create design doc  #Task  Status: Todo, Assignee: [[Alice]]

## 📚 Learning

- Read: [[Article/React Best Practices]]
- Try: New testing framework  #Task  Status: Todo
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
  next-step: Post job listings  (a #Task block, Status Todo)

## 🎯 Strategic Thinking

- Initiative: Improve team velocity
  - Ideas: Better tooling, reduce meetings
  - Schedule team retro  #Task  Status: Todo

## 📨 Important Communications

- Email: Client feedback on demo
  action: Respond by EOD  (a #Task block, Status Todo)
- Slack: Team question about priorities
  action: Clarified in #general  (a #Task block, Status Done)
```

---

## Habit Tracking

### Daily Habits

**Habit Tracker Template:**

```markdown
- Habit tracker                  ← tagged Template

## Habits - <% today %>

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

water:    [glasses]     (number property)
exercise: [minutes]     (number property)
sleep:    [hours]       (number property)
mood:     [1-10]        (number property)
```

**Habit Tracking Query:**

Tag the tracker block `#Habits` and query the tag over a date range. A block's
text is `:block/title` — `:block/content` is not a DB-graph attribute and is not
queryable; it survives only as a plugin-SDK back-compat shim
(`src/main/logseq/sdk/utils.cljs:56`). Journal-ness is the
`:logseq.class/Journal` tag, not a `:block/journal?` flag, and `(between …)`
already encodes both that and the `:block/journal-day` comparison
(`deps/db/src/logseq/db/frontend/rules.cljc:94-100`):

Type `/Query`, then enter:

```clojure
(and (tags Habits) (between -7d today))
```

---

## Troubleshooting

**Problem: Forgetting to use daily notes**

**Solutions:**

```markdown
- Set reminder: Daily at 9 AM
- Make `g j` muscle memory
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
- Weekly task review: mark Done or delete
- Use the Canceled status for abandoned tasks
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
