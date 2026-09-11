---
title: 'Project Planning'
description: 'Learn to plan and track larger projects with Logseq'
type: tutorial
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
difficulty: intermediate
timeToComplete: '25 minutes'
prerequisites: ['todo-basics']
relatedDocs:
  tutorials:
    - '[Todo Basics](todo-basics.md)'
  howTo:
    - '[Advanced Queries](../../how-to/queries/advanced-queries.md)'
    - '[View Functions](../../how-to/content/view-types.md)'
---

# Project Planning

> **Last revised:** 2026-09-11

> Learn to plan, organize, and track larger projects in a Logseq DB graph.

> **DB graphs only.** In a DB graph, `TODO`/`DONE` markers, `[#A]` priorities, `SCHEDULED:`/`DEADLINE:` lines and `key:: value` properties are not syntax. They are plain text. This tutorial sets the built-in **Status**, **Priority**, **Scheduled**, **Deadline** and **Assignee** properties instead, and adds other properties with `/Add property`. The outlines below show the result: a property is written as `Name: value` under its block, but you set it through the UI rather than typing that text.

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

1. Create a new page called "Project Alpha".
2. Click **Set property** under the page title to add page properties.
3. Add these properties:
   - **Stage**, with the value `Active`. Don't call it "Status": that is the built-in task property, and setting it on a page also tags the page `#Task`.
   - **Owner**, a node property pointing at `[[Your Name]]`.
   - **Start date** and **Target date**, as date properties.
4. Add top-level blocks for the sections, with child blocks under each:

```
Project Alpha                         ← page
  Stage: Active                       (page properties)
  Owner: [[Your Name]]
  Start date: Feb 1st, 2026
  Target date: Mar 15th, 2026

- Overview
  - A brief description of what this project is about.
- Goals
  - Goal 1
  - Goal 2
  - Goal 3
- Tasks
  - (We'll add these next)
- Notes
  - (Reference materials and discussions)
```

### Project Template

Save this as a template for reuse:

1. Create the blocks you want to reuse under a single parent block
2. Tag that parent with `#Template` to turn it into a template
3. Later, use the slash command `/Template` ("Insert a created template here") to insert a copy wherever you need it. The parent's child blocks are what get inserted.

> **Removed:** the file-graph "right-click → Make template" menu item. `make-template`, "make template" and "make a template" appear nowhere under `src/main` or in `en.edn`. Templates are created by tagging a node with `#Template`.

---

## Step 2: Break Down Into Phases

Organize work into logical phases. Make each phase a block with its own **Stage** property, and put its tasks underneath. Set each task's status with `/Todo`, `/Doing` or `/Done`, or with `Cmd/Ctrl+Enter`:

```
- Phases
  - Phase 1: Research
    Stage: Complete
    - Gather requirements          (Status: Done)
    - Analyze competitors          (Status: Done)
    - Interview stakeholders       (Status: Done)
  - Phase 2: Design
    Stage: In progress
    - Create wireframes            (Status: Done)
    - Design mockups               (Status: Doing)
    - Get feedback                 (Status: Todo)
  - Phase 3: Implementation
    Stage: Not started
    - Build MVP                    (Status: Todo)
    - Testing                      (Status: Todo)
    - Deploy                       (Status: Todo)
```

---

## Step 3: Create Task Lists

### Main Task Breakdown

Under **Tasks**, add one block per task, then set:

- **Status** with `/Todo`
- **Priority** with `/Priority High` or `/Priority Medium`
- **Scheduled** or **Deadline** with `/Scheduled` or `/Deadline`
- **Assignee** with `/Add property` → Assignee. Assignee is a built-in node property that can hold several people.
- **depends-on**, a node property you create with `/Add property`

```
- All Tasks
  - Complete requirements document
    Status: Todo · Priority: High · Scheduled: Feb 5th, 2026
    Assignee: [[Alice]]
  - Design system architecture
    Status: Todo · Priority: High · Deadline: Feb 10th, 2026
    Assignee: [[Bob]]
  - Set up development environment
    Status: Todo · Priority: Medium
    Assignee: [[Alice]]
  - Create component library
    Status: Todo · Priority: Medium · Scheduled: Feb 15th, 2026
    Assignee: [[Carol]]
    depends-on: Design system architecture
```

### Nested Tasks

Indent subtasks under their parent task with `Tab`:

```
- Build user authentication        (Status: Todo)
  - Design login flow              (Status: Todo)
  - Implement backend API          (Status: Todo)
  - Create frontend components     (Status: Todo)
  - Write tests                    (Status: Todo)
  - Document API                   (Status: Todo)
```

---

## Step 4: Link Related Content

### Reference Other Pages

```
- Related
  - See [[Meeting Notes - 2026-02-01]] for kickoff discussion
  - Requirements in [[Project Alpha - Requirements]]
  - Design specs in [[Project Alpha - Design]]
```

### Bidirectional Links

When you link to another page, the linking block shows up in that page's linked references.

Property values count as links too. For example, a task whose **Assignee** is `[[Alice]]` references Alice's page, so every task assigned to her appears on her page.

---

## Step 5: Build a Project Dashboard

Create a query-based dashboard to track progress. Each query lives in its own block, and `/Query` or `/Advanced Query` turns the whole block into the query. So put each heading in a parent block and the query in a child block:

- **Dashboard**
  - **🔴 High Priority**
    - `/Advanced Query` block (below: "Urgent Tasks")
  - **🟡 In Progress**
    - `/Advanced Query` block ("Currently Working")
  - **✅ Recently Completed**
    - `/Advanced Query` block ("Done This Week")

**Urgent Tasks:**

```clojure
{:query [:find (pull ?b [*])
         :in $ ?project
         :where
         [?p :block/name ?project]
         [?b :block/page ?p]
         [?b :block/tags ?t]
         [?t :db/ident :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :db/ident ?status]
         [(contains? #{:logseq.property/status.todo
                       :logseq.property/status.doing} ?status)]
         [?b :logseq.property/priority ?pr]
         [?pr :db/ident :logseq.property/priority.high]]
 :inputs ["project alpha"]}
```

**Currently Working:**

```clojure
{:query [:find (pull ?b [*])
         :in $ ?project
         :where
         [?p :block/name ?project]
         [?b :block/page ?p]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.doing]]
 :inputs ["project alpha"]}
```

**Done This Week:**

```clojure
{:query [:find (pull ?b [*])
         :in $ ?project ?week-ago
         :where
         [?p :block/name ?project]
         [?b :block/page ?p]
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.done]
         [?b :block/updated-at ?updated]
         [(>= ?updated ?week-ago)]]
 :inputs ["project alpha" :-7d-ms]}
```

`:block/name` is the page title in lower case, so the input is `"project alpha"`. `:block/page` matches every block on the page, including nested subtasks. `:block/updated-at` changes on any edit, so "Done This Week" means "done, and last edited this week".

> **Corrected:** an earlier version matched tasks with `[?b :block/refs ?p]`. That finds blocks that *link to* Project Alpha, not the tasks written on its page. If you track tasks from other pages by linking them to the project, use `:block/refs` instead.

> **Removed:** the `#+BEGIN_QUERY` … `#+END_QUERY` wrappers and the `:title` keys. In a DB graph, `#+BEGIN_QUERY` is deprecated in favour of `/Advanced Query`, and `:title` is not a query key. Enter the bare map.

> **Note:** these queries use the DB graph property model. A task is tagged with `:logseq.class/Task`. Its `:logseq.property/status` has the closed values `status.backlog`, `status.todo`, `status.doing`, `status.in-review`, `status.done` and `status.canceled`, and its `:logseq.property/priority` has `priority.low`, `priority.medium`, `priority.high` and `priority.urgent`. The legacy `:block/marker` and `:block/priority` attributes do not exist.
>
> Status and priority values are **references** to closed-value entities, so a query binds an entity id, not a keyword. Match the identity with a datom pattern such as `[?s :db/ident :logseq.property/status.done]`, rather than calling `(:db/ident ?s)` inside a predicate. A predicate receives the raw entity id, so a keyword lookup on it returns `nil` and the clause never matches.
>
> Simpler alternative: the built-in `task` and `priority` rules are added to advanced queries automatically. `(task ?b #{"Todo" "Doing"})` and `(priority ?b #{"High"})` also work, and they are easier to read.

---

## Step 6: Track Timeline

### Milestone View

Make each milestone a task with a **Deadline**, and list its criteria as child blocks:

```
- Milestones
  - 🎯 Milestone 1: Requirements Complete
    Status: Todo · Deadline: Feb 10th, 2026
    - Requirements doc approved
    - Stakeholder sign-off
  - 🎯 Milestone 2: Design Approved
    Status: Todo · Deadline: Feb 20th, 2026
    - All mockups reviewed
    - Design system complete
  - 🎯 Milestone 3: MVP Launch
    Status: Todo · Deadline: Mar 15th, 2026
    - Core features working
    - Testing complete
    - Deployed to staging
```

### Timeline Query

To show upcoming deadlines, add a parent block **📅 Upcoming Deadlines** with an `/Advanced Query` child block:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?today-ms ?project
         :where
         [?p :block/name ?project]
         [?b :block/page ?p]
         [?b :logseq.property/deadline ?d]
         [(>= ?d ?today-ms)]]
 :inputs [:start-of-today-ms "project alpha"]}
```

> **Note:** `Deadline` and `Scheduled` are `:datetime` properties, stored as epoch milliseconds directly on the block. Compare them against a millisecond input such as `:start-of-today-ms`, `:end-of-today-ms`, or a relative form like `:+7d-end`. The plain `:today` input resolves to a `YYYYMMDD` integer, meant for journal-day comparisons. Any epoch-millisecond value is far larger than that integer, so `(>= ?d ?today)` is true for every deadline.

---

## Step 7: Team Coordination

### Assign Tasks

Use properties to assign work:

- **Assignee**, the built-in property
- **reviewer**, a node property you create with `/Add property`
- **Status** set to **In Review** with `/In Review`

```
- Review design mockups
  Status: In Review
  Assignee: [[Alice]]
  reviewer: [[Bob]]
```

### Person Dashboard

On each team member's page, add a parent block such as **Alice's Tasks** with an `/Advanced Query` child block:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?assignee-name
         :where
         (task ?b #{"Todo" "Doing"})
         [?b :logseq.property/assignee ?person]
         [?person :block/title ?assignee-name]]
 :inputs ["Alice"]}
```

To reuse one query on every person's page, replace the name with `:current-page` and match `[?person :block/name ?assignee-name]`. That input resolves to the lower-cased title of the page holding the query block.

> **Corrected:** an earlier version queried `:user.property/assignee`. Assignee is the built-in `:logseq.property/assignee`. A property you create yourself gets an ident with a random suffix, such as `:user.property/reviewer-` followed by eight random characters, so `:user.property/reviewer` alone never matches.

---

## Step 8: Weekly Reviews

### Create Review Template

Create a block tagged `#Template`. Its children are what gets inserted:

```
- Weekly review template #Template
  - Weekly Review - <% today %>
    - Progress This Week
      - Completed:
      - In Progress:
    - Blockers
    - Next Week
```

When you insert it with `/Template`, `<% today %>` becomes a reference to today's journal page. It is stored as `[[<uuid>]]` and displayed as the journal's title. If you want counts such as tasks completed, add number properties with `/Add property`. `tasks-completed:: 0` text does not create a property.

### Link Reviews to Project

If you keep each review on its own page, list them on the project page:

```
- Weekly Reviews
  - [[Weekly Review - 2026-02-07]]
  - [[Weekly Review - 2026-02-14]]
  - [[Weekly Review - 2026-02-21]]
```

---

## Practice Exercise

Create your own project:

1. Create page "Home Office Setup"
2. Add phases: Plan, Buy, Assemble, Organize
3. Add 3-4 tasks per phase, and set a priority on each with `/Priority`
4. Set deadlines for key milestones with `/Deadline`
5. Create a simple dashboard query. Type `/Query`, then enter `(and (task todo doing) (page "Home Office Setup"))`
6. Link to any reference pages (inspiration, shopping lists)

---

## Tips

### Keep Projects Visible

Choose **Add to Favorites** from a project page's menu. The page then appears under **Favorites** in the left sidebar.

### Regular Updates

Update task status daily. Stale data makes planning unreliable.

### Archive Completed Projects

When a project is done, set its **Stage** to "Complete", and optionally move it to an archive page.

### Use Namespaces

Organize related pages with namespaced titles:

- `Project Alpha/Requirements`
- `Project Alpha/Design`
- `Project Alpha/Meeting Notes`

In a DB graph, a title like `Project Alpha/Requirements` creates one page per segment. `Requirements` becomes a child of `Project Alpha`, and the top-level page sits under the built-in **Library** page.

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

_Last updated: 2026-09-11_
