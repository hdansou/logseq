---
title: 'Project Planning'
description: 'Learn to plan and track larger projects with Logseq'
type: tutorial
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
difficulty: intermediate
timeToComplete: '25 minutes'
prerequisites: ['todo-basics']
relatedDocs:
  tutorials:
    - '[Todo Basics](todo-basics.md)'
  howTo:
    - '[Advanced Queries](../../how-to/queries/advanced-queries.md)'
---

# Project Planning

> **Last revised:** 2026-09-09

> Learn to plan, organize, and track larger projects in Logseq.

> **Note on syntax:** The markdown snippets in Steps 1-3, 6 and 7 use file-graph syntax
> (`TODO`/`DONE` markers, `[#A]` priorities, `SCHEDULED:`/`DEADLINE:` lines, `key:: value`
> properties). In a **DB graph** none of those are text: you set the built-in **Status**,
> **Priority**, **Scheduled** and **Deadline** properties on the block instead — see
> [Todo Basics](todo-basics.md). The dashboard queries in Steps 5-7 are DB-graph queries.

## What You'll Learn

- 📁 Creating project structures
- 🔗 Linking related content
- 📊 Building project dashboards
- 📅 Timeline management
- ✅ Progress tracking

**Time:** ~25 minutes

---

## Step 1: Create a Project Page

Every project starts with a dedicated page.

### Basic Structure

Create a new page called "Project Alpha":

```markdown
# Project Alpha

status:: active
owner:: [[Your Name]]
start-date:: [[2026-02-01]]
target-date:: [[2026-03-15]]

## Overview

A brief description of what this project is about.

## Goals

- Goal 1
- Goal 2
- Goal 3

## Tasks

(We'll add these next)

## Notes

(Reference materials and discussions)
```

### Project Template

Save this as a template for reuse:

1. Create the blocks you want to reuse under a single parent block
2. Tag that parent with `#Template` to turn it into a template
3. Later, use the slash command `/Template` ("Insert a created template here") to drop a
   copy wherever you need it

> **Removed:** The file-graph "right-click -> Make template" menu item. There is no such
> command in the current source; templates are created by tagging a node with `#Template`.

---

## Step 2: Break Down Into Phases

Organize work into logical phases:

```markdown
## Phases

### Phase 1: Research

status:: complete

- DONE Gather requirements
- DONE Analyze competitors
- DONE Interview stakeholders

### Phase 2: Design

status:: in-progress

- DONE Create wireframes
- DOING Design mockups
- TODO Get feedback

### Phase 3: Implementation

status:: not-started

- TODO Build MVP
- TODO Testing
- TODO Deploy
```

---

## Step 3: Create Task Lists

### Main Task Breakdown

```markdown
## All Tasks

TODO [#A] Complete requirements document
SCHEDULED: <2026-02-05>
assignee:: [[Alice]]

TODO [#A] Design system architecture
DEADLINE: <2026-02-10>
assignee:: [[Bob]]

TODO [#B] Set up development environment
assignee:: [[Alice]]

TODO [#B] Create component library
SCHEDULED: <2026-02-15>
assignee:: [[Carol]]
depends-on:: [[Design system architecture]]
```

### Nested Tasks

```markdown
TODO Build user authentication
TODO Design login flow
TODO Implement backend API
TODO Create frontend components
TODO Write tests
TODO Document API
```

---

## Step 4: Link Related Content

### Reference Other Pages

```markdown
## Related

- See [[Meeting Notes - 2026-02-01]] for kickoff discussion
- Requirements in [[Project Alpha - Requirements]]
- Design specs in [[Project Alpha - Design]]
```

### Bidirectional Links

When you link to another page, it automatically shows as a "Linked Reference" on that page.

For example, if you have a page for [[Alice]], her name will show all projects she's assigned to.

---

## Step 5: Build a Project Dashboard

Create a query-based dashboard to track progress:

```clojure
## Dashboard

### 🔴 High Priority
#+BEGIN_QUERY
{:title "Urgent Tasks"
 :query [:find (pull ?b [*])
         :in $ ?project
         :where [?b :block/refs ?p]
                [?p :block/name ?project]
                [?b :block/tags :logseq.class/Task]
                [?b :logseq.property/status ?s]
                [?s :db/ident ?status]
                [(contains? #{:logseq.property/status.todo
                              :logseq.property/status.doing} ?status)]
                [?b :logseq.property/priority ?pr]
                [?pr :db/ident :logseq.property/priority.high]]
 :inputs ["project alpha"]}
#+END_QUERY

### 🟡 In Progress
#+BEGIN_QUERY
{:title "Currently Working"
 :query [:find (pull ?b [*])
         :in $ ?project
         :where [?b :block/refs ?p]
                [?p :block/name ?project]
                [?b :logseq.property/status ?s]
                [?s :db/ident :logseq.property/status.doing]]
 :inputs ["project alpha"]}
#+END_QUERY

### ✅ Recently Completed
#+BEGIN_QUERY
{:title "Done This Week"
 :query [:find (pull ?b [*])
         :in $ ?project ?week-ago
         :where [?b :block/refs ?p]
                [?p :block/name ?project]
                [?b :logseq.property/status ?s]
                [?s :db/ident :logseq.property/status.done]
                [?b :block/updated-at ?updated]
                [(>= ?updated ?week-ago)]]
 :inputs ["project alpha" :-7d-ms]}
#+END_QUERY
```

> **Note:** These queries use the DB graph property model. Tasks are tagged with
> `:logseq.class/Task` and use `:logseq.property/status` (closed values
> `status.backlog`, `status.todo`, `status.doing`, `status.in-review`, `status.done`,
> `status.canceled`) and `:logseq.property/priority` (`priority.low`, `priority.medium`,
> `priority.high`, `priority.urgent`) instead of the legacy `:block/marker` and
> `:block/priority` attributes.
>
> Status and priority values are **references** to closed-value entities, so a query
> binds an entity id, not a keyword. Match the identity with a datom pattern
> (`[?s :db/ident :logseq.property/status.done]`) rather than calling `(:db/ident ?s)`
> inside a predicate — a predicate receives the raw entity id, so keyword lookup on it
> returns `nil` and the clause never matches.
>
> Simpler alternative: the built-in `task` and `priority` rules are injected into
> advanced queries automatically, so `(task ?b #{"Todo" "Doing"})` and
> `(priority ?b #{"High"})` also work and are easier to read.

---

## Step 6: Track Timeline

### Milestone View

```markdown
## Milestones

TODO 🎯 Milestone 1: Requirements Complete
DEADLINE: <2026-02-10>

- Requirements doc approved
- Stakeholder sign-off

TODO 🎯 Milestone 2: Design Approved  
DEADLINE: <2026-02-20>

- All mockups reviewed
- Design system complete

TODO 🎯 Milestone 3: MVP Launch
DEADLINE: <2026-03-15>

- Core features working
- Testing complete
- Deployed to staging
```

### Timeline Query

Show upcoming deadlines:

```clojure
#+BEGIN_QUERY
{:title "📅 Upcoming Deadlines"
 :query [:find (pull ?b [*])
         :in $ ?today-ms ?project
         :where [?b :block/refs ?p]
                [?p :block/name ?project]
                [?b :logseq.property/deadline ?d]
                [(>= ?d ?today-ms)]]
 :inputs [:start-of-today-ms "project alpha"]}
#+END_QUERY
```

> **Note:** `Deadline` and `Scheduled` are `:datetime` properties, stored as epoch
> milliseconds directly on the block. Compare them against a millisecond input such as
> `:start-of-today-ms`, `:end-of-today-ms` or a relative form like `:+7d-end`.
> The plain `:today` input resolves to a `YYYYMMDD` integer (for journal-day
> comparisons), so comparing it against a datetime property silently matches everything.

---

## Step 7: Team Coordination

### Assign Tasks

Use properties to assign work:

```markdown
TODO Review design mockups
assignee:: [[Alice]]
reviewer:: [[Bob]]
status:: waiting-review
```

### Person Dashboard

Create a page for each team member showing their tasks:

```clojure
## [[Alice]]'s Tasks

#+BEGIN_QUERY
{:title "Assigned to Alice"
 :query [:find (pull ?b [*])
         :in $ ?assignee-name
         :where [?b :block/tags :logseq.class/Task]
                [?b :logseq.property/status ?s]
                [?s :db/ident ?status]
                [(contains? #{:logseq.property/status.todo
                              :logseq.property/status.doing} ?status)]
                [?b :user.property/assignee ?person]
                [?person :block/title ?assignee-name]]
 :inputs ["Alice"]}
#+END_QUERY
```

> **Note:** Custom properties like `assignee` are namespaced as `:user.property/assignee`
> in DB graphs. The property must be created in your graph before using it in queries.

---

## Step 8: Weekly Reviews

### Create Review Template

```markdown
# Weekly Review - <% today %>

## Progress This Week

- Completed:
  -
- In Progress:
  -

## Blockers

-

## Next Week

-

## Metrics

tasks-completed:: 0
tasks-added:: 0
```

### Link Reviews to Project

```markdown
## Weekly Reviews

- [[Weekly Review - 2026-02-07]]
- [[Weekly Review - 2026-02-14]]
- [[Weekly Review - 2026-02-21]]
```

---

## Practice Exercise

Create your own project:

1. Create page "Home Office Setup"
2. Add phases: Plan, Buy, Assemble, Organize
3. Add 3-4 tasks per phase with priorities
4. Set deadlines for key milestones
5. Create a simple dashboard query
6. Link to any reference pages (inspiration, shopping lists)

---

## Tips

### Keep Projects Visible

Pin important project pages to the sidebar for quick access.

### Regular Updates

Update task status daily. Stale data makes planning unreliable.

### Archive Completed Projects

When done, change status to "complete" and optionally move to an archive page.

### Use Namespaces

Organize related pages with namespaces:

- `Project Alpha/Requirements`
- `Project Alpha/Design`
- `Project Alpha/Meeting Notes`

---

## What's Next?

- **[Advanced Queries](../../how-to/queries/advanced-queries.md)** - Complex dashboard queries
- **[View Functions](../../how-to/content/view-types.md)** - Custom result displays
- **[Todo Basics](todo-basics.md)** - Review task fundamentals

---

## Summary

✅ Created project page with metadata  
✅ Organized into phases  
✅ Broke down into tasks  
✅ Linked related content  
✅ Built dashboard with queries  
✅ Tracked timeline with milestones

---

_Last updated: 2026-09-09_
