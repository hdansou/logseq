---
title: 'Meeting Notes Workflow'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Task Management](./task-management-workflows.md)'
  - '[Daily Workflows](./daily-workflows.md)'
  - '[Templates System](../../reference/interface/templates-system.md)'
tags: ['meetings', 'workflow', 'productivity', 'collaboration']
---

# Meeting Notes Workflow

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

Learn how to capture, organize, and act on meeting notes in Logseq. This workflow ensures meetings lead to clear outcomes and action items never get lost.

**What You'll Learn:**

- Pre-meeting preparation
- Live note-taking strategies
- Action item tracking
- Meeting follow-up process
- Query patterns for meeting insights

**Time to Setup:** 15-30 minutes

---

## Before You Start: how this maps onto a DB graph

The outlines below are written as plain text so you can read them at a glance.
Four things to know about how the same structure is actually built in a DB
graph:

- **Properties are structured data, not text.** Typing `type:: Meeting` into a
  block leaves you with a block whose text is `type:: Meeting`. Add properties
  with the `/Add property` slash command
  (`src/main/frontend/commands.cljs:354-358`), which mints a property entity and
  attaches a typed value. `key:: value` lines below are shorthand for "this node
  carries this property", never literal syntax to type.
- **Property idents are generated, not guessable.** A property you create called
  "Attendees" is stored as `:user.property/Attendees-<8 random chars>`
  (`deps/db/src/logseq/db/frontend/property.cljs:852-859` →
  `deps/db/src/logseq/db/frontend/db_ident.cljc:66-90`). Resolve user properties
  by title in queries; never hard-code the ident.
- **Tasks are properties, not `TODO` markers.** `TODO`, `DOING`, `DEADLINE:` and
  `[#A]` are file-graph text markers. A DB-graph task is a node tagged `#Task`
  carrying `:logseq.property/status`, `:logseq.property/priority`,
  `:logseq.property/deadline` and `:logseq.property/scheduled`
  (`deps/db/src/logseq/db/frontend/class.cljs:39-41`).
- **`A/B/C` page names are split into a page hierarchy.** Creating
  `[[Meetings/2026-01-22-Kickoff]]` creates two pages — `Meetings` and
  `2026-01-22-Kickoff` — linked by `:block/parent`, with the top page filed under
  the built-in **Library** page. The leaf page's title is `2026-01-22-Kickoff`,
  not the full path (`deps/outliner/src/logseq/outliner/page.cljs:308-316, 385-388`;
  `deps/outliner/test/logseq/outliner/page_test.cljs:35-51`). Pages created from
  the editor are split this way by default
  (`src/main/frontend/handler/common/page.cljs:146-148`).

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

### Define a Meeting tag once

Before you create your first meeting page, create the tag that every meeting
will carry, and give it the properties every meeting needs. Do this once:

1. Create a page called `Meeting` and turn it into a tag
   (`src/main/frontend/handler/db_based/page.cljs:37-48`), or type `#Meeting` in
   a block and let Logseq create it — a new `#` name on a block is created as a
   tag (`deps/outliner/src/logseq/outliner/core.cljs:197-220`).
2. On the tag page, add the properties meetings should have. User property types
   you can choose from are `:default` (text), `:number`, `:date`, `:datetime`,
   `:checkbox`, `:url`, `:node` and `:asset`
   (`deps/db/src/logseq/db/frontend/property/type.cljs:19-21`).

A workable property set for meetings:

| Property   | Type       | Notes                                            |
| ---------- | ---------- | ------------------------------------------------ |
| Date       | `:date`    | Points at a journal page                         |
| Time       | `:default` | Free text, e.g. `14:00-15:00`                    |
| Project    | `:node`    | Points at the project page                       |
| Attendees  | `:node`    | Set cardinality to multiple to list many people  |
| Meeting URL| `:url`     | Video link                                       |

A `:date` value must be a journal page
(`deps/db/src/logseq/db/frontend/property/type.cljs:157-161`), and cardinality
can be switched to multiple for `:default`, `:number`, `:url`, `:date`, `:node`
and `:asset` (`:34-36`).

Once the tag carries these properties, any page or block tagged `#Meeting` has
them in its property list, along with the properties of any tag `Meeting`
extends (`deps/outliner/src/logseq/outliner/property.cljs:1020-1044`).

### Create Meeting Page

**Standard Meeting Page:**

```markdown
- [[Meetings/2026-01-22-Project-Alpha-Kickoff]]   #Meeting
  Date:: [[Jan 22nd, 2026]]
  Time:: 14:00-15:00
  Project:: [[Alpha]]
  Attendees:: [[Sarah]], [[John]], [[Alice]]

  ## Pre-Meeting

  ### Context

  First planning meeting for Project Alpha

  ### Goals

  - Define project scope
  - Assign initial responsibilities
  - Set next milestones

  ### Agenda

  - Introductions (5 min)
  - Project overview (10 min)
  - Technical approach discussion (25 min)
  - Next steps (10 min)
  - Q&A (10 min)

  ### Questions to Ask

  - What's the target launch date?
  - Who are the key stakeholders?
  - What are the biggest risks?

  ### Preparation Done

  - Review project brief   (link to the brief block)
  - Prepare technical questions
  - Share agenda with attendees
```

Two notes on the above:

- The property lines are what the page shows after you add the properties with
  `/Add property`. They are not text you type.
- The agenda items are plain blocks. If you want them to be checkable tasks,
  give them a status — see [Action Items](#action-items).

### Recurring Meeting Setup

**Series page:**

```markdown
- [[Weekly-Team-Sync]]   #Meeting
  Frequency:: weekly
  Time:: 10:00-11:00
  Attendees:: [[Engineering]]

  ## Standard Agenda

  1. Wins from last week
  2. Current priorities
  3. Blockers
  4. Next week plan
  5. Questions

  ## Meeting Notes

  - [[Jan 22nd, 2026]]
  - [[Jan 15th, 2026]]
  - [[Jan 8th, 2026]]
```

There is no scheduling engine behind `Frequency` — it is a property you read,
not one Logseq acts on. The recurrence Logseq does act on is the
`:logseq.property.repeat/*` family
(`deps/db/src/logseq/db/frontend/property.cljs:354-399`), and what it moves
forward is a node's `Scheduled` or `Deadline` date
(`src/main/frontend/worker/commands.cljs:224-249`), not a meeting page.

---

## Meeting Note Template

### How templates work in a DB graph

A template is a **block tagged `Template`** (`:logseq.class/Template`,
`deps/db/src/logseq/db/frontend/class.cljs:99-101`). There is no `template::`
property and no `{{title}}` placeholder.

To create one:

1. Write the block tree you want to reuse.
2. Tag the top block `Template` and give it a name. Note that an inline
   `#Template` typed into a block's text is **stripped** — `Template` is one of
   the block-kind tags removed from inline text
   (`deps/db/src/logseq/db/frontend/class.cljs:147-157`), so set the tag from the
   block's tag control rather than by typing it mid-sentence.
3. Insert it later with the `/Template` slash command
   (`src/main/frontend/commands.cljs:339-343`).

**Template variables.** Exactly five `<% %>` variables are substituted
(`deps/outliner/src/logseq/outliner/template.cljs:104-112`):

| Variable            | Expands to                              |
| ------------------- | --------------------------------------- |
| `<% today %>`       | Page ref to today's journal             |
| `<% yesterday %>`   | Page ref to yesterday's journal         |
| `<% tomorrow %>`    | Page ref to tomorrow's journal          |
| `<% time %>`        | Current time, 24-hour hour and minute   |
| `<% current page %>`| Page ref to the page you insert it on   |

`<% time %>` is formatted for your locale with a 24-hour clock (`:10-16`).
Names match case-insensitively. Any other name loses its `<% %>` wrapper and is
inserted as the bare word: `<% title %>` becomes `title`, `<% date %>` becomes
`date`, and an empty `<% %>` becomes nothing
(`deps/outliner/src/logseq/outliner/template.cljs:8, 114-128`). No test covers
unknown names; this follows from the code. `{{title}}` is not a template
variable at all.

### Basic Template

Create this tree once and tag the top block `Template`:

```markdown
- Meeting note   #Template
  - <% today %> meeting   #Meeting
    Time::
    Project::
    Attendees::

    ## Pre-Meeting

    ### Context

    Why are we meeting?

    ### Goals

    - Goal 1
    - Goal 2

    ### Agenda

    - Topic 1 (X min)
    - Topic 2 (X min)

    ## Meeting Notes

    ### Discussion

    ### Key Points

    ### Decisions

    ### Questions & Open Items

    ## Post-Meeting

    ### Action Items

    - Action 1   #Task  (Status: Todo, Assignee, Deadline)
    - Action 2   #Task  (Status: Todo, Assignee, Deadline)

    ### Follow-up Needed

    - Send summary email
    - Schedule follow-up meeting
    - Update project tracker
```

The meeting's title still has to be typed each time — no template variable
supplies it.

### Enhanced Template

The same block tree with more properties on the meeting node. Add each of these
to the `Meeting` tag first so they are offered when you fill the template in:

```markdown
- Meeting note (enhanced)   #Template
  - <% today %> meeting   #Meeting
    Time::
    Duration::
    Location::
    Meeting URL::
    Recording::
    Project::
    Attendees::
    Organizer::
    Decision maker::

    ## Pre-Meeting

    ### Context & Background
    ### Meeting Goals
    ### Success Criteria
    ### Agenda (with time boxes)
    ### Pre-Reading Materials
    ### Questions to Address
    ### Preparation Checklist

    ## During Meeting

    ### Attendance
    ### Discussion Notes
    ### Key Points
    ### Decisions Made
    ### Disagreements & Debates
    ### Parking Lot
    ### Questions & Open Items

    ## Post-Meeting

    ### Action Items
    ### Follow-up Tasks
    ### Next Steps
    ### Next Meeting
    ### Meeting Metrics
    ### Retrospective
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
  - Decision: Use React + TypeScript
    - Rationale: Best of both worlds
```

To point at a decision block later, use "Copy block ref" from the block's context menu. In a
DB graph that copies the block's UUID wrapped as a node reference —
`[[0195b1cb-6b1f-7d70-94b8-6f1b2e4dbb31]]` (`src/main/frontend/components/content.cljs:289-291`,
via `src/main/frontend/util/ref.cljs:5-7`). Typing `((` does not start a block search; it only
shows "To reference a node, please use `[[]]`."
(`src/main/frontend/handler/editor.cljs:1982-1985`).

### Real-Time Techniques

**Speed Shortcuts:**

```markdown
While typing:

- Use abbreviations first, expand later
- Drop timestamps for context (14:15)
- Link people [[Name]] and projects [[Name]]
- Tag the meeting node #Meeting
- Give an action item a status the moment it is spoken
```

**Keyboard Shortcuts:**

| Shortcut     | What it does                                                        |
| ------------ | ------------------------------------------------------------------- |
| `Cmd/Ctrl+Enter` | Cycle task status: none → Todo → Doing → Done → none            |
| `Tab`        | Indent (show hierarchy)                                             |
| `Shift+Tab`  | Outdent                                                             |
| `/Todo`, `/Doing`, `/Done` | Set a status directly                                 |
| `/Deadline`, `/Scheduled`  | Set a task date                                       |
| `/Add property`            | Attach any property                                   |
| `[[`         | Link a node                                                         |

`Cmd/Ctrl+Enter` is bound to `:editor/cycle-todo`
(`src/main/frontend/modules/shortcut/config.cljs:167-168`); the cycle it walks is
Todo → Doing → Done → cleared, and any other status (Backlog, In Review,
Canceled) or no status goes to Todo
(`src/main/frontend/handler/editor.cljs:825-838, 847-856`). `Tab` and
`Shift+Tab` are `:editor/indent` and `:editor/outdent`
(`src/main/frontend/modules/shortcut/config.cljs:220, 223`). The status slash
commands are generated from the `Status` property's closed values
(`src/main/frontend/commands.cljs:101-103, 150-165`), and `/Deadline` and
`/Scheduled` set `:logseq.property/deadline` and `:logseq.property/scheduled`
(`src/main/frontend/commands.cljs:265-276, 664-668`).

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

Setting `Status`, `Deadline` or `Scheduled` on a block automatically tags it
`#Task` unless one of its existing tags already supplies that property
(`deps/outliner/src/logseq/outliner/property.cljs:108-116`, applied at
`:205-206`). So `/Todo` on a block is all it takes to make it a task.

**Good Action Item:**

```markdown
✅ Specific and actionable:

- Review API documentation and create a summary of authentication endpoints   #Task
  Status:: Todo
  Assignee:: [[Sarah]]
  Deadline:: Jan 25th, 2026
  Priority:: High
  Project:: [[Alpha]]
```

**Bad Action Item:**

```markdown
❌ Vague:

- API stuff   #Task
  Status:: Todo
```

### The built-in task properties

| Property                      | Type        | Values                                                                 |
| ----------------------------- | ----------- | ---------------------------------------------------------------------- |
| `:logseq.property/status`     | closed set  | Backlog, Todo, Doing, In Review, Done, Canceled — default **Todo**      |
| `:logseq.property/priority`   | closed set  | Low, Medium, High, Urgent                                              |
| `:logseq.property/deadline`   | `:datetime` | Stored as epoch milliseconds                                           |
| `:logseq.property/scheduled`  | `:datetime` | Stored as epoch milliseconds                                           |
| `:logseq.property/assignee`   | `:node`     | Multiple values allowed; values must be pages                          |

Sources: `deps/db/src/logseq/db/frontend/property.cljs:300-321` (status, with the
six idents at `:313-318` and the `Todo` default at `:320`), `:323-338`
(priority), `:340-346` and `:347-353` (deadline / scheduled), `:401-408`
(assignee).

`Priority` is a closed set of the four names above — `[#A]`, `[#B]` and `[#C]`
are file-graph syntax and do not parse. `Assignee` is the built-in owner
property; there is no `owner` or `assigned-to` property.

### Track Meeting-Specific Actions

Advanced queries are written with the `/Advanced Query` slash command, which
tags the block `#Query` (`:logseq.class/Query`) and stores the query map in
`:logseq.property/query` as a Clojure code block
(`src/main/frontend/commands.cljs:125-132`). There is no `#+BEGIN_QUERY` wrapper
in a DB graph — that is file-graph syntax and renders a deprecation warning
instead of results (`src/main/frontend/components/block.cljs:5107-5108`).

**Open action items on the meeting page you are looking at:**

```clojure
{:query [:find (pull ?b [*])
         :in $ ?current-page
         :where
         [?p :block/name ?current-page]
         [?b :block/page ?p]
         (task ?b #{"Todo" "Doing"})]
 :inputs [:current-page]}
```

Why the `task` rule and not `[?b :logseq.property/status ?s]`: `Todo` is the
property's **default value**, and a default is not a datom — a direct clause
silently misses every task nobody has touched. The `task` rule reads through the
default (`deps/db/src/logseq/db/frontend/rules.cljc:297-300`). Built-in rules are
injected automatically when their name appears in `:where`
(`src/main/frontend/worker/handler/query.cljs:185-204`), so you never pass
`:rules` or bind `%` yourself.

`:current-page` resolves to the lowercased page title, which is what
`:block/name` holds (`deps/db/src/logseq/db/frontend/inputs.cljs:71, 84-86`;
`deps/db/src/logseq/db/frontend/schema.cljs:90`).

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

   - Assignee set on each
   - Reasonable deadlines
   - Priority where it matters

3. Update related pages
   - Add decisions to project page
   - Link from today's journal
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

1. ...
2. ...

**Action Items:**

- Sarah: ... - Due Jan 25
- John: ... - Due Jan 27

**Open Questions:**

- ...
- ...

**Next Meeting:** [Date/Time]

Thanks,
[Your name]
```

Copy the decision text out of the graph; a copied block ref (`[[<uuid>]]`) pasted
into an email is meaningless outside Logseq.

### Link to Related Pages

**Create Network:**

```markdown
From the meeting page, link to:

1. Project page

   - [[Alpha]] via the Project property
   - Update project status

2. Today's journal

   - Reference the meeting node from the day's entry

3. People pages

   - [[Sarah]] via Attendees / Assignee

4. Topic pages
   - [[API Design]]
```

Each of these links shows up on the other side under **Linked References** — you
do not need to add a back-link by hand. That includes pages named only in a
`:node` property such as `Project` or `Assignee`: property values are counted as
references (`deps/outliner/src/logseq/outliner/pipeline.cljs:131-139`), and
linked references are built from those
(`deps/db/src/logseq/db/common/reference.cljs:71-79`).

---

## Meeting Types

### 1-on-1 Meetings

```markdown
- [[1-1-Sarah-2026-01-22]]   #Meeting
  With:: [[Sarah]]
  Date:: [[Jan 22nd, 2026]]
  Frequency:: biweekly

  ## Agenda

  - How's it going? (check-in)
  - Current project status
  - Any blockers?
  - Career development
  - Feedback both ways

  ## Sarah's Topics

  - Question about architecture
  - Interest in ML project

  ## My Topics

  - Feedback on recent PR review
  - Upcoming deadline discussion

  ## Discussion

  ## Action Items

  ## Private Notes
```

"Private Notes" is only a heading. Nothing in the source at this revision gives
that heading a special meaning, so the blocks under it are stored and shown like
any other block, and anyone who can open the graph can read them.

### Project Meetings

```markdown
- [[Project-Alpha-Weekly-2026-01-22]]   #Meeting
  Project:: [[Alpha]]
  Date:: [[Jan 22nd, 2026]]

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
  - Owner: [[John]]

  ### 2. Frontend Approach

  - Decision needed: React vs Vue
  - Decision: React

  ## Risks & Issues

  - Risk: Dependency on external API
    - Mitigation: Build mock service
    - Owner: [[Sarah]]

  ## Next Week Plan

  - Complete database schema
  - Start API implementation
  - Design review meeting
```

If you make "Sprint velocity", "Open issues" and similar into `:number`
properties so you can chart them later, remember that a number property's value
is a **ref to a value entity** — read it through `:logseq.property/value`, not
directly (`deps/db/src/logseq/db/frontend/property/type.cljs:52-56`).

### Brainstorming Sessions

```markdown
- [[Brainstorm-Feature-Ideas-2026-01-22]]   #Meeting
  Project:: [[Alpha]]
  Date:: [[Jan 22nd, 2026]]

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

  - Prototype interactive tutorial   #Task
    Status:: Todo
    Assignee:: [[Sarah]]
    Deadline:: Feb 5th, 2026

  - Create sample data set   #Task
    Status:: Todo
    Assignee:: [[John]]
    Deadline:: Feb 1st, 2026
```

### Retrospectives

```markdown
- [[Sprint-Retrospective-2026-01-22]]   #Meeting
  Project:: [[Alpha]]
  Sprint:: Sprint 5
  Date:: [[Jan 22nd, 2026]]

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

  - Block "no meeting" time   #Task
    Assignee:: [[Manager]]

  - Requirement template   #Task
    Assignee:: [[Product]]

  - Fix CI/CD stability   #Task
    Assignee:: [[DevOps]]

  ## Appreciations 🙏

  - Sarah: Great pair programming
  - John: Excellent code reviews
  - Alice: Patient with questions
```

---

## Queries & Reports

Two ways to query, both authored from the editor:

- **`/Query`** takes a *simple query* in the DSL — `(and (tags Meeting) (task
  Todo Doing))` (`src/main/frontend/commands.cljs:110-117, 325`).
- **`/Advanced Query`** takes a Datalog query map
  (`src/main/frontend/commands.cljs:125-132, 326`).

Logseq decides which one you wrote by reading the block: a map is treated as an
advanced query, anything else as a DSL query
(`src/main/frontend/components/block.cljs:4139-4152`).

The old `{{query …}}` macro is deprecated in a DB graph and renders a warning
instead of results (`src/main/frontend/components/block.cljs:1887-1888`;
`src/resources/dicts/en.edn:159`). Use `/Query`.

The DSL filters that exist at this revision are `and`, `or`, `not`, `between`,
`property`, `private-property`, `task` (alias `todo`), `priority`, `page`,
`sample`, `tags`, a `[[page ref]]`, a bare `"string"` for full-text search, and
a raw Datalog clause (`src/main/frontend/worker/query_dsl.cljs:529-577`). There
is no `sort-by`, `page-property` or `namespace` filter.

### All Meetings

Simple query (`/Query`):

```clojure
(tags Meeting)
```

Advanced equivalent:

```clojure
{:query [:find (pull ?b [*])
         :where
         (tags ?b #{"Meeting"})]}
```

The `tags` rule matches a tag by title, `:db/ident` or entity id and follows
`class-extends` down the tag tree
(`deps/db/src/logseq/db/frontend/rules.cljc:266-295`). Both forms pick up tags
that extend `Meeting`: the simple query also expands the tag to its children
before calling the rule (`src/main/frontend/worker/query_dsl.cljs:431-450`).

### Meetings This Week

If you capture meetings as blocks on journal pages, `between` filters on the
journal day of the block's page
(`deps/db/src/logseq/db/frontend/rules.cljc:94-100`):

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         (tags ?b #{"Meeting"})
         (between ?b ?start ?end)]
 :inputs [:-7d :today]}
```

`:-7d` and `:today` both resolve to `YYYYMMDD` integers, which is what
`:block/journal-day` stores
(`deps/db/src/logseq/db/frontend/inputs.cljs:71, 73, 100-101, 121-125`).

> **Watch the units.** `:today` is `YYYYMMDD`; `Deadline` and `Scheduled` are
> epoch **milliseconds**. Comparing one against the other matches nothing and
> reports no error. For a millisecond bound use the `-ms` / `-start` / `-end`
> forms, e.g. `:+7d-end` or `:end-of-today-ms`
> (`deps/db/src/logseq/db/frontend/inputs.cljs:74, 76-77, 113-118, 128-133`).
>
> `:monday`, `:sunday` and `:7d-ago` are **not** query inputs — they match no
> dispatch branch and are handed to the query unresolved
> (`deps/db/src/logseq/db/frontend/inputs.cljs:69-80, 140-155`).

### Action Items by Person

```clojure
{:query [:find (pull ?b [*])
         :in $ ?person
         :where
         [?p :block/title ?person]
         [?b :logseq.property/assignee ?p]
         (task ?b #{"Todo" "Doing"})]
 :inputs ["Sarah"]}
```

`:logseq.property/assignee` is a `:node` property, so its value is the person's
page entity — match it on `:block/title`, not on a string
(`deps/db/src/logseq/db/frontend/property.cljs:401-408`).

### Overdue Meeting Actions

```clojure
{:query [:find (pull ?b [*])
         :in $ ?now
         :where
         (task ?b #{"Todo" "Doing"})
         [?b :logseq.property/deadline ?d]
         [(< ?d ?now)]]
 :inputs [:right-now-ms]}
```

`:right-now-ms` is the current time in milliseconds
(`deps/db/src/logseq/db/frontend/inputs.cljs:71, 109-110`), which is the same
unit `Deadline` uses. Note that tasks with no deadline are excluded — there is no
datom to match.

To narrow this to actions that sit under a meeting — a page tagged `Meeting`, or
a `Meeting` block on a journal page — require a `Meeting`-tagged ancestor:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?now
         :where
         (task ?b #{"Todo" "Doing"})
         [?b :logseq.property/deadline ?d]
         [(< ?d ?now)]
         (tags ?m #{"Meeting"})
         (parent ?m ?b)]
 :inputs [:right-now-ms]}
```

`(parent ?m ?b)` takes the ancestor first and walks `:block/parent` any number
of levels (`deps/db/src/logseq/db/frontend/rules.cljc:7-12`); a page is the
parent of its top-level blocks (`deps/db/src/logseq/db/sqlite/build.cljs:249`).
It is injected like the other built-in rules (`rules.cljc:75-78`). Matching on
`:block/page` instead would miss a meeting captured as a block on a journal.

### Meetings by Project

```clojure
{:query [:find (pull ?b [*])
         :in $ ?project
         :where
         (tags ?b #{"Meeting"})
         [?b :block/refs ?proj]
         [?proj :block/name ?project]]
 :inputs ["alpha"]}
```

`:block/name` holds the lowercased title
(`deps/db/src/logseq/db/frontend/schema.cljs:89-90`), so pass a plain string in
lower case, or pass `"[[Alpha]]"`, which is lowercased for you
(`deps/db/src/logseq/db/frontend/inputs.cljs:157-159`). Because `A/B/C` titles
are split into separate pages, name the **leaf**: `"alpha"`, not
`"projects/alpha"`. A page named only in a `:node` property such as `Project`
still counts as a reference, so this finds those meetings too
(`deps/outliner/src/logseq/outliner/pipeline.cljs:131-139`).

### Sorting and Titles

Three things people reach for that do not work on a user query:

- **`:title`** is only rendered for Logseq's own built-in queries
  (`src/main/frontend/components/query.cljs:166-174`), so it is silently ignored
  here. Put the heading in the parent block.
- **`:group-by-page?`** returns `false` immediately for every DB graph
  (`src/main/frontend/components/query/result.cljs:10-11`), and
  `:breadcrumb-show?` is read only on the built-in-query branch; a user query's
  block results take the branch above it
  (`src/main/frontend/components/query.cljs:52-71`).
- **`:limit`** and **`:order-by`** are not query-map keys at all: they are not
  among the keys passed to the query worker
  (`src/main/frontend/worker/handler/render_resource/query.cljs:48-61`). Bound
  the result in `:where`, or sort in `:result-transform`.

`:result-transform` does work. It is sent to the query worker as text
(`src/main/frontend/components/query/result.cljs:46-47`), evaluated there, and
called on the rows the query returned — for `(pull ?b [*])`, one block map per
row — before rows are reduced to UUIDs
(`src/main/frontend/worker/handler/render_resource/query.cljs:181-202`;
`src/main/frontend/worker/handler/query.cljs:243-261`). It must return a
sequence (`render_resource/query.cljs:188-191`):

```clojure
{:query [:find (pull ?b [*])
         :where (tags ?b #{"Meeting"})]
 :result-transform (fn [result]
                     (take 20 (sort-by (fn [r] (get r :block/created-at)) > result)))}
```

`:view` is different: by the time a `:view` function runs, each row is a bare
UUID, not an entity (`…/render_resource/query.cljs:15-20, 40-46, 195-202`), so
`(:block/title r)` inside a `:view` returns nil. A `:view` that fails to
evaluate is logged and dropped, and the results render as if there were no
`:view` (`src/main/frontend/extensions/sci.cljs:50-58`;
`src/main/frontend/components/query.cljs:43, 149`). An error thrown while it
runs shows `Custom view error: …` in place of the results
(`src/main/frontend/components/query.cljs:44-49`).

### Meeting Statistics

Counting meetings created in the last 30 days:

```clojure
{:query [:find (count ?b)
         :in $ ?since
         :where
         (tags ?b #{"Meeting"})
         [?b :block/created-at ?created]
         [(> ?created ?since)]]
 :inputs [:-30d-start]}
```

`:block/created-at` is epoch milliseconds
(`deps/db/src/logseq/db/frontend/schema.cljs:85`, set from
`common-util/time-ms` at `deps/outliner/src/logseq/outliner/core.cljs:122-123`),
and `:-30d-start` resolves to the millisecond timestamp at the start of that day
(`deps/db/src/logseq/db/frontend/inputs.cljs:74, 128-133`).

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

- Capture decisions in their own blocks so they can be referenced
- Set a status on an action the moment it is agreed
- Link people and projects
- Note timestamps for key moments
- Focus on outcomes, not transcription

❌ Avoid:

- Trying to capture every word
- Not linking to context
- Vague action items
- Forgetting to set Assignee
```

### After Meeting

```markdown
✅ Do (within 1 hour):

- Clean up notes
- Send summary
- Fill in Assignee and Deadline on every action
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
Monday 9 AM: Create meeting page from the Meeting note template

- Add agenda
- Share with attendees
- Prepare questions

Tuesday 2 PM: Meeting starts

- Follow agenda structure
- Capture in blocks
- Give each action a status with Cmd/Ctrl+Enter
- Note timestamps

Tuesday 2:55 PM: Wrap up

- Review action items
- Confirm Assignee and Deadline
- Schedule follow-up if needed

Tuesday 3:05 PM: Post-meeting

- Clean up notes (5 min)
- Send summary email (10 min)
- Set the Project property
- Reference the meeting from today's journal

Tuesday 3:30 PM: Action items visible in:

- Personal task queries
- Project dashboard
- Assignee's page (via Linked References)

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

1. Create the `Meeting` tag and its properties (10 min)
2. Create the meeting note template block (10 min)
3. Set up key queries (10 min)
4. Practice with next meeting (live)
5. Refine based on needs (10 min)

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
