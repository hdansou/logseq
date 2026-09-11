---
title: 'Todo Basics'
description: "Learn to manage tasks effectively with Logseq's task management system"
type: tutorial
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
difficulty: beginner
timeToComplete: '15 minutes'
prerequisites: ['basic-editing']
relatedDocs:
  reference:
    - '[Properties Reference](../../reference/data/properties-reference.md)'
  howTo:
    - '[Repeating Tasks](../../how-to/tasks/repeating-tasks.md)'
    - '[Task Management Workflows](../../how-to/workflows/task-management-workflows.md)'
---

# Todo Basics

> **Last revised:** 2026-09-11

> Learn to create, organize, and track tasks in a Logseq DB graph.

## What You'll Learn

- ✅ Creating tasks
- ✅ Task states (Todo, Doing, Done, and more)
- ✅ Setting priorities (Urgent, High, Medium, Low)
- ✅ Scheduling and deadlines
- ✅ Querying your tasks

**Time:** ~15 minutes

---

## Step 1: Create Your First Task

### Using the Slash Command

1. In any block, type `/Todo`
2. Select it from the menu (it is in the **TASK STATUS** group) and press Enter
3. Type your task description

This sets the block's **Status** property to Todo. Setting Status, Scheduled or Deadline also tags the block `#Task`, unless one of its existing tags already provides that property (`deps/outliner/src/logseq/outliner/property.cljs:108-116`).

### Using the Keyboard Shortcut

1. Type any text in a block
2. Press `Cmd+Enter` (macOS) or `Ctrl+Enter` (Windows/Linux)
3. Each press moves the block to the next state: (none) → Todo → Doing → Done → (none)

---

## Step 2: Cycle Task States

In DB graphs, tasks use the built-in **Status** property, which has exactly these six values:

| Status      | Meaning          |
| ----------- | ---------------- |
| `Backlog`   | Not yet planned  |
| `Todo`      | Not started      |
| `Doing`     | In progress      |
| `In Review` | Under review     |
| `Done`      | Completed        |
| `Canceled`  | No longer needed |

### Change State

**Option 1: Click the status indicator**

- Click the status indicator on the left of the block. A picker opens with all six statuses. Choose the one you want.
- A tag can show Status as a checkbox instead. On the tag's page, open the Status property's menu and turn on **Show as checkbox on tagged nodes**. After that, a tagged block whose status is **Todo** or **Done** shows a checkbox: checking it sets Done and unchecking it sets Todo. A block with any other status still shows the picker, because only Todo and Done have a checkbox state.

**Option 2: Keyboard**

- `Cmd/Ctrl+Enter` cycles: Todo → Doing → Done → (none).
- Backlog, In Review and Canceled are not in the cycle. From any of them, the next press sets Todo.

**Option 3: Slash command**

- Type `/` and choose a status: `/Backlog`, `/Todo`, `/Doing`, `/In Review`, `/Done` or `/Canceled`.

**Option 4: Status shortcut**

- Select the block, so you are not editing its text, and press `p s` to open the Status property.

### Try It

Create a block, then press `Cmd/Ctrl+Enter` to set it to Todo. Press it again for Doing, and once more for Done. A fourth press clears the status.

---

## Step 3: Set Priorities

Add a priority with the slash commands `/Priority Urgent`, `/Priority High`, `/Priority Medium` or `/Priority Low`. `/No priority` clears it.

**Priority levels:**

- **Urgent** - Critical, handle immediately
- **High** - Important, do first
- **Medium** - Standard priority
- **Low** - Nice to have

### Keyboard Shortcut

Select the block (not in editing mode) and press `p p` to open the Priority property. This shortcut does not work while you are typing in the block.

---

## Step 4: Schedule Tasks

### Set a Date

Use the slash command `/Scheduled` or `/Deadline`:

1. Type `/Scheduled` or `/Deadline`
2. A calendar appears, with a time field and a box for typing a date in words
3. Select a date, and optionally a time

These set the built-in **Scheduled** or **Deadline** property. Both are datetime properties. With a block selected, `p d` also opens Deadline.

The same date editor contains the repeat settings. See [Repeating Tasks](../../how-to/tasks/repeating-tasks.md).

---

## Step 5: Create a Task List

Organize related tasks by nesting blocks, and set a status on each child block to track progress:

1. Create a parent block for your project
2. Add child blocks (indent with `Tab`)
3. Set each child block's status with `Cmd/Ctrl+Enter` or a slash command

### Track Progress

To see which tasks on the page are finished, add a query block. In a new block, type `/Query`, then enter:

```clojure
(and (task done) (page "Weekend Project"))
```

`page` matches blocks that are **on** that page.

> **Corrected:** an earlier version used `[[Weekend Project]]` here. A page reference in a query matches blocks that **reference** the page (rule at `deps/db/src/logseq/db/frontend/rules.cljc:81-82`), not the blocks on it. The `page` filter matches by the block's page (`rules.cljc:90-92`).

> **Removed:** an automatic "1/5 complete" counter and progress bar on the parent block. No such parent-task progress rollup exists in the current source. The only progress bars under `src/main/frontend/components` are for asset transfers (`block.cljs:1153`) and imports (`imports.cljs:536`).

---

## Step 6: Find Your Tasks

### Query Method

Create a simple query to find all your tasks with a particular status. Type `/Query`, then enter:

```clojure
(task todo doing)
```

This shows all tasks whose status is Todo or Doing. Status names are capitalized before matching, so `todo` and `TODO` both work. Quote a two-word status: `(task "in review")`.

You can also query by priority. Type `/Query`, then enter:

```clojure
(priority high)
```

> **`{{query …}}` does not work in a DB graph.** Typed into a block, it only shows "{{query}} is deprecated. Use '/Query' command instead." `/Query` turns the block's whole text into the query, so enter only the query and put any label in a parent block.

### Advanced Query

For more complex queries, use `/Advanced Query`. It tags the block as a query and opens a code editor for the query. Enter the query as a **map**. For example, to find all high-priority Todo tasks:

```clojure
{:query [:find (pull ?b [*])
         :where
         (task ?b #{"Todo"})
         (priority ?b #{"High"})]}
```

The `task` and `priority` rules are added for you.

> **Corrected:** an earlier version showed the bare `[:find …]` vector. Only a map is treated as an advanced query. A bare vector is not a map, and it doesn't start with `(`, a quote, a page reference or `[?`, so Logseq quotes it and runs it as a full-text search.

---

## Step 7: Task Properties

Tasks in DB graphs use built-in properties for status, priority, scheduled and deadline:

- **Status** - Built-in: Backlog, Todo, Doing, In Review, Done, Canceled
- **Priority** - Built-in: Low, Medium, High, Urgent
- **Scheduled** - Built-in: datetime property
- **Deadline** - Built-in: datetime property
- Custom properties (for example assignee or effort) can be added to any block with `/Add property`, or with `Cmd+P` on macOS and `Ctrl+Alt+P` on Windows/Linux

### Query by Properties

Type `/Query`, then enter:

```clojure
(property assignee "John")
```

The property is found by its exact, case-sensitive name. If no property is named `assignee`, the query matches nothing.

---

## Practice Exercise

Create a simple project with tasks:

1. Create a new page called "Weekend Project"
2. Add a parent block "Weekend Project Tasks" and set its priority to High
3. Add child blocks:
   - "Buy supplies from hardware store" (set status to Todo, priority to High, add a Scheduled date)
   - "Prep work area" (set status to Todo, priority to Medium)
   - "Main work" (set status to Todo, priority to Medium, add a Deadline)
   - "Clean up" (set status to Todo, priority to Low)
4. Mark "Buy supplies" as Done by pressing `Cmd/Ctrl+Enter` twice (Todo → Doing → Done)
5. Change "Main work" to Doing by pressing `Cmd/Ctrl+Enter` once
6. Add the progress query from Step 5 to the page. It lists "Buy supplies from hardware store"

---

## Tips

### Daily Task Review

Today's journal page has a collapsed **Scheduled and Deadline** section. It lists tasks whose Scheduled or Deadline date falls between today and 7 days from now, and whose status is not Done or Canceled. Tasks dated before today are not included. Only today's journal page shows this section.

You can change the 7-day window with `:scheduled/future-days` in `config.edn`. `:feature/disable-scheduled-and-deadline-query? true` hides the section.

### Use Tags

Combine tasks with inline tags for better organization:

```
Call dentist #health
Review budget #finance
```

Then set each block's status to Todo with `Cmd/Ctrl+Enter`.

### Time Blocking

Use the Scheduled property with a specific time for focused work blocks. Type `/Scheduled`, then pick a date and a time.

---

## What's Next?

- **[Repeating Tasks](../../how-to/tasks/repeating-tasks.md)** - Set up recurring tasks
- **[Task Management Workflows](../../how-to/workflows/task-management-workflows.md)** - Task properties, queries and views in depth
- **[Project Planning](project-planning.md)** - Larger project management

---

## Summary

- Created tasks using `/Todo` or `Cmd/Ctrl+Enter`
- Cycled states (Todo, Doing, Done) with `Cmd/Ctrl+Enter`
- Set priorities (Urgent, High, Medium, Low) with `/Priority`, or `p p` on a selected block
- Scheduled tasks and set deadlines with `/Scheduled` and `/Deadline`
- Queried tasks with a `/Query` block containing `(task todo doing)`

---

_Last updated: 2026-09-11_
