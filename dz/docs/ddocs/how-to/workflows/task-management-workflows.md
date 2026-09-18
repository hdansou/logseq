---
title: 'Task Management Workflows'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
verifiedScope: 'Full audit 2026-09-09 (ccc03ab0aa). The 2026-09-10 date covers only the simple-query examples and the <% %> template variables (4a37d8997b).'
reviewStatus: audited
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Daily Workflows](./daily-workflows.md)'
  - '[Templates System](../../reference/interface/templates-system.md)'
  - '[Query Patterns](../../reference/advanced/query-patterns.md)'
tags: ['tasks', 'todo', 'gtd', 'productivity', 'workflow']
---

# Task Management Workflows

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

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

> **This guide is for DB graphs.** Tasks work fundamentally differently here than
> in a file graph: there are no `TODO`/`DOING`/`DONE` markers, no `[#A]`
> priorities, no `SCHEDULED:`/`DEADLINE:` timestamps, and no `#+BEGIN_QUERY`
> blocks. Everything below uses the DB-graph equivalents, with the source
> citation for each. The `key: value` lines inside the outline examples are
> shorthand for "this node carries this property" — add them with `/Add property`
> rather than typing `key:: value` text, which a DB graph does not parse.

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

### What a task *is* in a DB graph

A task is a block tagged **`#Task`** (`:logseq.class/Task`) that carries the
built-in **Status** property. There are no `TODO`/`DOING`/`DONE` markers — those
were file-graph text. Typing `TODO` at the start of a block leaves you with a
block whose text happens to begin with the word "TODO".

The `Task` tag's schema is exactly four properties
(`deps/db/src/logseq/db/frontend/class.cljs:41`):

| Property | Ident | Type |
| --- | --- | --- |
| Status | `:logseq.property/status` | closed values (a **ref**) |
| Priority | `:logseq.property/priority` | closed values (a **ref**) |
| Deadline | `:logseq.property/deadline` | `:datetime` (epoch ms) |
| Scheduled | `:logseq.property/scheduled` | `:datetime` (epoch ms) |

### Creating Tasks

Type the block text, then run one of the status slash commands. Setting any of
the four Task properties tags the block `#Task` for you
(`deps/outliner/src/logseq/outliner/property.cljs:205-206`) — you never have to
add the tag by hand.

```markdown
- Task description        ← then press / and choose Todo
```

**Task States**

There are exactly six, and they are closed values of `:logseq.property/status`
(`deps/db/src/logseq/db/frontend/property.cljs:313-318`). The slash menu is
generated from that list (`src/main/frontend/commands.cljs:150-165`), so the
command name is the status name:

| Status | Slash command | Meaning |
| --- | --- | --- |
| Backlog | `/Backlog` | Captured, not committed to |
| Todo | `/Todo` | Not started (the default, `property.cljs:320`) |
| Doing | `/Doing` | In progress |
| In Review | `/In Review` | Waiting on review |
| Done | `/Done` | Completed |
| Canceled | `/Canceled` | Abandoned |

`WAITING`, `LATER` and `NOW` do not exist. If you need those states, model them
as a tag or a property of your own — do not expect a status called `Waiting`.

**Changing States**

```markdown
Method 1: Click the status chip on the block and pick a value.

Method 2: Slash commands — /Doing, /Done, /Canceled, ...

Method 3: Keyboard — Cmd/Ctrl+Enter cycles the current block
          Todo → Doing → Done → (no status)
          (src/main/frontend/modules/shortcut/config.cljs:167,
           src/main/frontend/handler/editor.cljs:825-837)
```

> **Note:** the cycle only walks Todo → Doing → Done → (no status). A block
> sitting on Backlog, In Review or Canceled is not part of the loop: those fall
> to the `case`'s trailing default and jump straight to Todo
> (`src/main/frontend/handler/editor.cljs:825-837`). Use the status picker if
> you want to move between the other three.

### Task Hierarchy

**Parent-Child Tasks:**

```markdown
- Complete project deliverable   #Task  Status: Todo
  - Research requirements        #Task  Status: Todo
    - Review documentation       #Task  Status: Done
    - Talk to stakeholders       #Task  Status: Todo
  - Create design                #Task  Status: Todo
  - Implementation               #Task  Status: Todo
  - Testing                      #Task  Status: Todo
```

Nesting is ordinary block nesting (`:block/parent`). Each block carries its own
Status independently.

> **Unverified:** whether a parent task's status is ever derived from its
> children was not confirmed either way from source. Do not rely on rollup.

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
- Task description   #Task
  Status:    Todo               (built-in)
  Priority:  High               (built-in)
  Deadline:  2026-01-30         (built-in, datetime)
  Scheduled: 2026-01-22         (built-in, datetime)
  project:   [[Project Alpha]]  (yours)
  context:   [[Work]]           (yours)
  energy:    high               (yours)
  time:      30min              (yours)
```

Set the built-ins from the slash menu; add the rest with `/Add property`
(`src/main/frontend/commands.cljs:354-358`).

---

## Task Properties

### Deadlines

**Add Deadline:**

```markdown
/Deadline → opens the date picker
           (src/main/frontend/commands.cljs:265-270)
```

(Or set the Deadline property directly from the block's property panel.)
`DEADLINE: <2026-01-30 Wed>` is org-mode text a DB graph does not parse, and
there is no `<+7d>` relative form in the editor.

`:logseq.property/deadline` is a **`:datetime`** property — the stored value is
**epoch milliseconds** (`deps/db/src/logseq/db/frontend/property.cljs:340-346`).
This matters for queries.

**Deadline Queries:**

The single most common mistake is comparing a deadline against `:today`.
`:today` resolves to a **YYYYMMDD integer** (`date->int`,
`deps/db/src/logseq/db/frontend/inputs.cljs:99-100`), while a deadline is epoch
milliseconds. The comparison never throws — it just silently matches nothing.
Use the millisecond inputs instead:

| Input | Resolves to |
| --- | --- |
| `:start-of-today-ms` / `:end-of-today-ms` | epoch ms (`inputs.cljs:111-118`) |
| `:+7d-end`, `:-30d-start`, `:+1m-ms` | epoch ms (`inputs.cljs:126-132`) |
| `:right-now-ms` | epoch ms (`inputs.cljs:108-109`) |
| `:today`, `:yesterday`, `:tomorrow`, `:+7d`, `:-1w` | YYYYMMDD int — for `:block/journal-day` only |

`:7d-after` and friends (`^\d+d(-before|-after|…)?$`) are **deprecated**
(`inputs.cljs:80`); `:7d-ago`, `:monday` and `:sunday` match no dispatch branch
at all and are passed through as raw keywords (`inputs.cljs:69-81`,
`inputs.cljs:143-165`) — a query using them compares against a keyword and
returns nothing.

Second gotcha: `:logseq.property/status` has a **default value** of Todo
(`property.cljs:320`), and defaults are not materialised as datoms. So
`[?b :logseq.property/status ?s]` misses every task whose status was never set
explicitly. Use the built-in `task` rule, which goes through
`ref-property-with-default` and therefore does see the default
(`deps/db/src/logseq/db/frontend/rules.cljc:297-300` and its dependency
declaration at `:315`). Logseq detects
built-in rule names in your `:where` and wires the rules into the query for you
(`src/main/frontend/worker/handler/query.cljs:185-204`) — you do not need a
`:rules` key or a `%` in `:in`.

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :logseq.property/deadline ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]
         (task ?b #{"Backlog" "Todo" "Doing" "In Review"})]
 :inputs [:start-of-today-ms :+7d-end]}
```

The `task` rule matches on the status's **title**, not its ident
(`rules.cljc:297-300` → `ref->val`, `rules.cljc:103-107`), which is why the set
holds `"In Review"` and not `:logseq.property/status.in-review`.

If you would rather match idents directly, dereference the ref — the status
value is an entity, so `[?b :logseq.property/status "Todo"]` and
`[(= :logseq.property/status.todo (:db/ident ?s))]` both never match:

```clojure
[?b :logseq.property/status ?s]
[?s :db/ident :logseq.property/status.todo]
```

### Scheduled Dates

**Schedule Task:**

```markdown
/Scheduled → opens the date picker
            (src/main/frontend/commands.cljs:271-276)

Meaning: Plan to start on this date

Difference (from the properties' own descriptions,
deps/db/src/logseq/db/frontend/property.cljs:345,353):

- Scheduled: "Use it to plan something to start at a specific date(time)."
- Deadline:  "Use it to finish something at a specific date(time)."
```

**Repeating Tasks:**

`+1w` / `+1m` suffixes inside a timestamp are file-graph syntax and do nothing.
Repetition is configured in the Deadline or Scheduled date picker, which has a
**Repeat task** toggle, an **Every _n_ _unit_** control, a **Next date** choice
and a **When _property_ is: _value_** completion trigger
(`src/main/frontend/components/property/value.cljs:347-440`;
labels at `src/resources/dicts/en.edn:1552-1558`).

It writes these properties (`property.cljs:354-397`):

| Property | Values |
| --- | --- |
| `:logseq.property.repeat/repeated?` | checkbox |
| `:logseq.property.repeat/recur-frequency` | number (default 1) |
| `:logseq.property.repeat/recur-unit` | Minute, Hour, Day, Week, Month, Year (default Day) |
| `:logseq.property.repeat/repeat-type` | "Advance from completion", "Advance from scheduled", "Advance from scheduled, skip to future" (default the last) |
| `:logseq.property.repeat/checked-property` | which property's value completes the occurrence (defaults to Status) |

```markdown
- Weekly review  #Task
  Scheduled: 2026-01-22, Repeat task ✓, Every 1 Week

- Pay rent  #Task
  Scheduled: 2026-02-01, Repeat task ✓, Every 1 Month
```

### Priority Levels

**Set Priority:**

Priorities are closed values of `:logseq.property/priority` and there are
**four**, named — not lettered
(`deps/db/src/logseq/db/frontend/property.cljs:333-337`). The slash commands are
generated from that list as `Priority <name>`, plus a `No priority` entry
(`src/main/frontend/commands.cljs:171-188`; label templates at
`src/resources/dicts/en.edn:548,558`):

```markdown
/Priority Urgent
/Priority High
/Priority Medium
/Priority Low
/No priority
```

`[#A]`, `[#B]`, `[#C]` and the `/priority-a` commands do not exist in a DB graph.

**Priority Query:**

Type `/Query`, then enter:

```clojure
(and (task todo doing) (priority high urgent))
```

> **`{{query …}}` does not work in a DB graph.** Typed into a block it renders only a
> deprecation warning (`src/main/frontend/components/block.cljs:1887-1888`). Entered as the query
> of a `/Query` block it becomes a full-text search for that literal text, because `sanitize-q`
> quotes anything that does not start with `(`, `"`, a page ref or `[?`
> (`src/main/frontend/components/query/builder.cljs:608-617`). Every simple query in this guide is
> therefore shown as the bare form you enter after typing `/Query`; in an outline, a line marked
> `← /Query block` is its own `/Query` block.

Both `(task …)` and `(priority …)` are simple-query operators that go through
the default-aware rules (`src/main/frontend/worker/query_dsl.cljs:411-429`), and
their arguments are case-insensitive: `task` capitalises every word
(`capitalize-all`, `deps/common/src/logseq/common/util.cljs:145-149`) and
`priority` capitalises the single word (`query_dsl.cljs:421-429`). Multi-word
values must therefore be quoted: `(task "in review")`.

### Custom Properties

**Add Context:**

Add each of these with `/Add property`
(`src/main/frontend/commands.cljs:354-358`). Keep the titles single-token if you
want to name them in a simple query — the DSL reads property names as symbols
(`src/main/frontend/worker/query_dsl.cljs:343-356`).

```markdown
- Task description  #Task
  Status:   Todo                 (built-in)
  Assignee: [[Sarah]]            (built-in — :logseq.property/assignee,
                                  a :node property, cardinality many;
                                  property.cljs:401-407)
  project:      [[Project Alpha]]
  context:      [[Work]], [[Client/Acme]]
  energy:       high
  time-estimate: 2h
  depends-on:   ((7b2e0c14-…-…))   ← a block-ref UUID
  waiting-for:  [[John]]
```

Do not create a property called `Status` of your own — it collides by title with
the built-in one (Logseq permits the duplicate,
`deps/outliner/src/logseq/outliner/validate.cljs:125-129`, but a simple query
naming `status` then resolves to whichever the index returns first,
`src/main/frontend/worker/query_dsl.cljs:346-356`). There is no
`:logseq.property/assigned-to`; the built-in is `:logseq.property/assignee`.

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

- [Anything that comes to mind]  #Task #inbox
  Status:    Todo
  captured:  [[2026-01-22]]

No organizing yet, just capture
```

**Capture Template:**

Make a template by adding the built-in **Template** tag to the top block of the
tree — use the block's tag control, **not** an inline `#Template`. `Template` is
one of the "block kind" tags, and inline tags in that set are stripped when the
block is saved (`deps/db/src/logseq/db/frontend/class.cljs:147-157`;
`deps/outliner/src/logseq/outliner/core.cljs:297-313`). Insert a saved template
later with `/Template` (`src/main/frontend/commands.cljs:339-343`).

```markdown
- (type here)  #Task #inbox   ← plus the Template tag
  Status:   Todo
  captured: <% today %>
  source:   <% current page %>
```

`<% cursor %>` is not a template variable — only `today`, `yesterday`,
`tomorrow`, `time` and `current page` are substituted, and anything else is
left as literal text
(`deps/outliner/src/logseq/outliner/template.cljs:104-112,127-128`).

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
   → Set the built-in Scheduled property (/Scheduled)
```

**Clarify Template:**

```markdown
- Task description  #Task     ← plus the Template tag
  Status:       Todo
  Scheduled:    <pick a date>
  next-action:  [Specific next step]
  project:      [[Project Name]]
  context:      [[Context]]
  time-estimate: [minutes]
  energy:       high/medium/low
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
- Task description  #Task
  Status:       Todo
  gtd-category: [[Next Actions]]
  context:      [[Work]]
  project:      [[Project Alpha]]

Query shows all in category
```

Note that `[[GTD/Next Actions]]` creates two pages — `GTD` and
`Next Actions` — and the reference points at the leaf, whose title is
`Next Actions` (`deps/outliner/src/logseq/outliner/page.cljs:464-467`).

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

Type `/Query`, then enter:

```clojure
(and [[Office]] (task todo))
```

Or, as an advanced query with the page name passed in — `:block/name` is the
lowercased, sanitised title, and because `A/B` titles are split into separate
pages the name to pass is the leaf:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?context
         :where
         [?b :block/refs ?p]
         [?p :block/name ?context]
         (task ?b #{"Todo"})]
 :inputs ["office"]}
```

### Reflect Phase

**Weekly Review Template:**

```markdown
- GTD weekly review           ← tagged Template

# GTD Weekly Review - <% today %>

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

- Completed this week: see the "Completed This Week" query below
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
```

Query for context + time. Make `time-estimate` a **number** property (minutes)
so it can be compared; resolve it by title so you do not have to know its
generated ident:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?context ?estimate-title ?max-minutes
         :where
         [?b :block/refs ?p]
         [?p :block/name ?context]
         (task ?b #{"Todo" "Doing"})
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?estimate-title]
         [?prop :db/ident ?estimate]
         [?b ?estimate ?v]
         [?v :logseq.property/value ?minutes]
         [(<= ?minutes ?max-minutes)]]
 :inputs ["office" "time-estimate" 30]}
```

A number property's value is stored as a **ref to a value entity**, not as a
bare number, so it has to be dereferenced through `:logseq.property/value`
before you can compare it (`deps/db/src/logseq/db/frontend/rules.cljc:103-107`
does the same thing inside `ref->val`).

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
- <Project Name>  #Project    ← plus the Template tag
  created:  <% today %>
  Deadline: <pick a date>
  para-status: active

## Outcome

[Specific result to achieve]

## Next Actions

- (next action)  #Task  Status: Todo
- (next action)  #Task  Status: Todo

## Resources

- [[Resources/Topic]]

## Notes

-

## Archive Date

[When completed/cancelled]
```

**Projects Dashboard:**

There is no `namespace` rule and no `:block/namespace` attribute. Model PARA
categories as **tags** (`#Project`, `#Area`, `#Resource`, `#Archive`) rather
than as a page-name prefix — `(tags …)` is a first-class query operator and it
also picks up tags that extend the one you name
(`deps/db/src/logseq/db/frontend/rules.cljc:266-295`):

Type `/Query`, then enter:

```clojure
(and (tags Project) (property para-status "active"))
```

If you prefer the page hierarchy, walk `:block/parent` instead, which is what
`A/B` titles actually build
(`deps/outliner/src/logseq/outliner/page.cljs:464-467`). The recursive `parent`
rule takes the **ancestor first**
(`deps/db/src/logseq/db/frontend/rules.cljc:8-12`):

```clojure
{:query [:find (pull ?b [*])
         :in $ ?category
         :where
         [?p :block/name ?category]
         (parent ?p ?b)]
 :inputs ["projects"]}
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
- <Area Name>  #Area          ← plus the Template tag
  created: <% today %>

## Responsibilities

- [What needs maintaining]

## Standards

- [Quality expectations]

## Active Projects

- [[Projects/...]]

## Routine Tasks

- Weekly review  #Task
  Status: Todo
  Scheduled: <pick a date>, Repeat task ✓, Every 1 Week

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
1. Retag the page: remove #Project, add #Archive

2. Update properties
   para-status: completed
   archived:    [[2026-01-22]]

3. Active queries filter on (tags Project), so it drops out

4. Keep for reference
   Still searchable
   Still linked
```

Retagging is the reliable move. Renaming `Projects/Old` to
`Archives/Projects/Old` reparents the leaf page, but the page's own title stays
`Old` either way, so nothing that queries by title changes
(`deps/outliner/src/logseq/outliner/page.cljs:464-467`).

### PARA Task Flow

**Capture → Categorize:**

```markdown
1. New task captured:

   - Write blog post about PARA  #Task  Status: Todo

2. Ask: Is this a project or task?
   → Multi-step? → Project
   → Single task? → Add to project/area

3. Assign category:

   - Write blog post  #Task
     Status:    Todo
     project:   [[Content Marketing]]
     area:      [[Professional Development]]
     resources: [[Writing]]

4. Add metadata:
   - Deadline (built-in, /Deadline)
   - context
   - next-action
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
- Task description  #Task
  Status: Backlog

Move: change the Status property
Backlog → Todo → Doing → In Review → Done
```

The six built-in statuses already give you Kanban columns, so you do not need a
`status` property of your own.

**Visual Query:**

Type `/Query`, then enter:

```clojure
(task backlog todo doing "in review" done)
```

`:group-by-page?` will not give you columns: it returns `false` immediately for
every DB graph (`src/main/frontend/components/query/result.cljs:10-11`). To
render one query as a board, use the view builder on the query block, or write a
`:view` function.

---

## Task Queries

### Essential Task Views

**Today's Tasks:**

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

**Overdue Tasks:**

```clojure
{:query [:find (pull ?b [*])
         :in $ ?now
         :where
         [?b :logseq.property/deadline ?d]
         [(< ?d ?now)]
         (task ?b #{"Backlog" "Todo" "Doing" "In Review"})]
 :inputs [:start-of-today-ms]}
```

**By Priority:**

Type `/Query`, then enter:

```clojure
(and (task todo) (priority urgent high))
```

**By Project:**

Type `/Query`, then enter:

```clojure
(and [[Alpha]] (task todo doing))
```

**Quick Wins (< 15 min):**

With `time-estimate` as a **number** property holding minutes:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?estimate-title ?max-minutes
         :where
         (task ?b #{"Todo"})
         [?prop :block/tags :logseq.class/Property]
         [?prop :block/title ?estimate-title]
         [?prop :db/ident ?estimate]
         [?b ?estimate ?v]
         [?v :logseq.property/value ?minutes]
         [(<= ?minutes ?max-minutes)]]
 :inputs ["time-estimate" 15]}
```

### Advanced Queries

**Energy-Based:**

Type `/Query`, then enter:

```clojure
(and (task todo) (property energy "high"))
```

**Waiting Items:**

There is no `Waiting` status. The closest built-in is **In Review**; if that does
not fit, add your own `waiting-for` property and query on its presence — the
one-argument `(property …)` form matches "has this property at all"
(`src/main/frontend/worker/query_dsl.cljs:394-401`):

Type `/Query`, then enter:

```clojure
(and (task todo doing) (property waiting-for))
```

**Completed This Week:**

Type `/Query`, then enter:

```clojure
(and (task done) (between -7d today))
```

The `between` rule filters on the **journal page** the block sits on: it
requires `[?p :block/tags :logseq.class/Journal]` and compares
`:block/journal-day`, which is a YYYYMMDD integer
(`deps/db/src/logseq/db/frontend/rules.cljc:94-100`). Its first argument is the
**block**, not a date. Note also that there is no `:block/journal?` attribute —
journal-ness is the `:logseq.class/Journal` tag.

As an advanced query:

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         (task ?b #{"Done"})
         (between ?b ?start ?end)]
 :inputs [:-7d :today]}
```

`:-7d` and `:today` are the right inputs here — both resolve to YYYYMMDD
integers, which is what `:block/journal-day` holds
(`deps/db/src/logseq/db/frontend/inputs.cljs:73,99-100,121-126`). `:7d-before`
is the deprecated spelling (`inputs.cljs:80`).

---

## Review Workflows

### Daily Review

**Morning Review (5 min):**

```markdown
- Daily morning review        ← tagged Template

## Morning Review - <% today %>

### Yesterday

- Completed
  - (and <% yesterday %> (task done))        ← /Query block
- Pending
  - (and <% yesterday %> (task todo))        ← /Query block

### Today's Plan

- [ ] Review calendar
- [ ] Choose top 3 priorities
- [ ] Check deadlines

### Top 3 Today

1. (task)  #Task  Status: Todo, Priority: Urgent
2. (task)  #Task  Status: Todo, Priority: Urgent
3. (task)  #Task  Status: Todo, Priority: High
```

> **Query lines in these templates.** A line marked `← /Query block` is a block of its own: type
> `/Query` on it and enter the form shown. `/Query` moves the block's whole title into the query
> (`src/main/frontend/handler/editor.cljs:4436-4453`), so a label cannot share the line and goes in
> the parent block instead. `<% today %>` and `<% yesterday %>` already insert a page reference
> (`deps/outliner/src/logseq/outliner/template.cljs:74`), so they are written bare, not inside
> `[[…]]`. Inside a template they are fixed to the day the template is inserted; in a standalone
> `/Query` they are recomputed each time the query runs
> (`src/main/frontend/worker/query_dsl.cljs:92-110`).

Two simple-query filters have to be joined by an explicit boolean operator —
`[[Page]] (task done)` is not an implicit AND. `and`, `or` and `not`
are the only combinators (`src/main/frontend/worker/query_dsl.cljs:548-549`).

**Evening Review (10 min):**

```markdown
- Daily evening review        ← tagged Template

## Evening Review - <% today %>

### Completed

(and <% today %> (task done))        ← /Query block

### Incomplete

(and <% today %> (task todo doing))        ← /Query block

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
- Weekly review               ← tagged Template

# Weekly Review

date: <% today %>

## 1. Gather & Process (10 min)

- [ ] Clear inboxes
- [ ] Process loose notes
- [ ] File references
- [ ] Update task statuses

## 2. Review (15 min)

### Completed This Week

(and (between -7d today) (task done))        ← /Query block
Count:

### Incomplete Tasks

(task todo doing "in review")        ← /Query block
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
- Monthly review              ← tagged Template

# Monthly Review

date: <% today %>

## 1. Projects (20 min)

- [ ] Review all active projects
- [ ] Archive completed projects
- [ ] Evaluate stalled projects
- [ ] Plan new projects

### Project Health

(property project)        ← /Query block

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
- Complete design      #Task  Status: Todo   ← Copy block ref
- Start implementation #Task  Status: Todo
  depends-on: ((7b2e0c14-…-…))
```

`id:: design-task` does not exist in a DB graph. A block reference is always a
UUID (`deps/common/src/logseq/common/util/block_ref.cljs:9`); get one with
**Copy block ref**. Logseq does not compute a scheduled date from a dependency —
`depends-on` here is just a `:node`-typed property you created, and the
`Scheduled` date is still yours to set.

**Query for Blocked Tasks:**

Type `/Query`, then enter:

```clojure
(and (task todo doing) (property depends-on))
```

### Time Tracking

**Track Time Spent:**

```markdown
- Task description  #Task
  Status:         Todo
  time-estimate:  120   (number property, minutes)
  time-spent:     90
  time-remaining: 30

Update as you work
```

Make these **number** properties if you want to compare them in queries; their
values are stored as ref entities, so a query has to read
`:logseq.property/value` off the ref.

Logseq does track one kind of time by itself: `task-spent-time` sums the periods
a task spent in the **Doing** status, using the status history recorded by
`:logseq.property/enable-history?` on the Status property
(`src/main/frontend/worker/handler/query.cljs:49-100`;
`deps/db/src/logseq/db/frontend/property.cljs:319-321`).

### Habit Tracking

**Recurring Habits:**

```markdown
- Morning workout  #Task
  Status:    Todo
  Scheduled: 2026-01-22, Repeat task ✓, Every 1 Day
  habit:     true    (a checkbox property you add)
  streak:    7       (a number property you maintain)
```

Set the repeat in the Scheduled date picker (see "Repeating Tasks" above); the
`<2026-01-22 Wed +1d>` timestamp form is file-graph syntax. Logseq does not
compute a streak — `streak` is yours to update.

**Habit Query:**

Type `/Query`, then enter:

```clojure
(property habit true)
```

### Batch Processing

**Batch Similar Tasks:**

```markdown
- Batch: Email responses
  context:: [[Context/Computer]]
  batch:: true

  - Reply to Sarah  #Task  Status: Todo
  - Reply to John   #Task  Status: Todo
  - Reply to client #Task  Status: Todo

Do all at once (time-efficient)
```

---

## Best Practices

### Task Writing

**Good Task Format:**

```markdown
✅ Action-oriented:

- Call Sarah about project timeline
- Review and merge PR #123
- Schedule dentist appointment

❌ Vague:

- Sarah
- Code review
- Dentist
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
- [ ] Clean up In Review / blocked items
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
