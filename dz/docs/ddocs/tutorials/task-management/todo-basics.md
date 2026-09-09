---
title: 'Todo Basics'
description: "Learn to manage tasks effectively with Logseq's task management system"
type: tutorial
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
difficulty: beginner
timeToComplete: '15 minutes'
prerequisites: ['basic-editing']
relatedDocs:
  reference:
    - '[Task Properties](../../reference/features/task-properties.md)'
  howTo:
    - '[Repeating Tasks](../../how-to/tasks/repeating-tasks.md)'
---

# Todo Basics

> **Last revised:** 2026-09-09

> Learn to create, organize, and track tasks in Logseq.

## What You'll Learn

- ✅ Creating TODO items
- ✅ Task states (Todo, Doing, Done, and more)
- ✅ Setting priorities (High, Medium, Low, Urgent)
- ✅ Scheduling and deadlines
- ✅ Querying your tasks

**Time:** ~15 minutes

---

## Step 1: Create Your First Task

### Using the Slash Command

1. On any page, type `/Todo`
2. Select it from the menu and press Enter
3. Type your task description

This sets the block's **Status** property to "Todo."

### Using Keyboard Shortcut

1. Type any text in a block
2. Press `Cmd+Enter` (macOS) or `Ctrl+Enter` (Windows/Linux)
3. This cycles through task states: Todo -> Doing -> Done -> (none)

---

## Step 2: Cycle Task States

In DB graphs, tasks use the built-in **Status** property with these values:

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

- Clicking the status indicator on the left of the block opens a picker listing all six
  statuses; choose the one you want.
- If Status has been configured to display as a checkbox, the indicator becomes a
  checkbox instead: checking it sets **Done**, unchecking it sets **Todo**. (Only Todo
  and Done have a checkbox state, so a checkbox cannot select Doing.)

**Option 2: Keyboard**

- `Cmd/Ctrl+Enter` cycles: Todo -> Doing -> Done -> (none)

**Option 3: Slash command**

- Type `/` and select a status (e.g., `/Todo`, `/Doing`, `/Done`)

### Try It

Create a block, then press `Cmd/Ctrl+Enter` to set it to Todo. Press it again for Doing, and once more for Done. (A fourth press clears the status.)

---

## Step 3: Set Priorities

Add priority using the slash command `/Priority High`, `/Priority Medium`, `/Priority Low`, or `/Priority Urgent`:

**Priority levels:**

- **Urgent** - Critical, handle immediately
- **High** - Important, do first
- **Medium** - Standard priority
- **Low** - Nice to have

### Keyboard Shortcut

With cursor on a block, press `p p` to open the Priority property selector.

---

## Step 4: Schedule Tasks

### Set a Date

Use the slash command `/Scheduled` or `/Deadline`:

1. Type `/Scheduled` or `/Deadline`
2. A date picker appears
3. Select a date (and optionally a time)

These set the built-in **Scheduled** or **Deadline** datetime properties on the block.

---

## Step 5: Create a Task List

Organize related tasks by nesting blocks. Set each child block's Status property to track progress:

1. Create a parent block for your project
2. Add child blocks (indent with `Tab`)
3. Set each child block's status using `Cmd/Ctrl+Enter` or the slash command

### Track Progress

To see how far along a nested task list is, query its children by status, for example:

```clojure
{{query (and (task Done) [[Weekend Project]])}}
```

> **Removed:** An automatic "1/5 complete" counter and progress bar on the parent block.
> No such parent-task progress rollup exists in the current source.

---

## Step 6: Find Your Tasks

### Query Method

Create a simple query to find all your tasks with a particular status:

```clojure
{{query (task TODO DOING)}}
```

This shows all tasks with status Todo or Doing.

You can also query by priority:

```clojure
{{query (priority high)}}
```

### Advanced Query

For more complex queries, use the slash command `/Advanced Query` which creates a tagged Query block. For example, to find all high-priority Todo tasks, set the query property to:

```clojure
[:find (pull ?b [*])
 :where
 (task ?b #{"Todo"})
 (priority ?b #{"High"})]
```

---

## Step 7: Task Properties

Tasks in DB graphs use built-in properties for status, priority, scheduled, and deadline. You can also add custom properties to blocks for further organization:

- **Status** - Built-in: Backlog, Todo, Doing, In Review, Done, Canceled
- **Priority** - Built-in: Low, Medium, High, Urgent
- **Scheduled** - Built-in: datetime property
- **Deadline** - Built-in: datetime property
- Custom properties (e.g., assignee, effort) can be added to any block

### Query by Properties

```clojure
{{query (property assignee "John")}}
```

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
4. Mark "Buy supplies" as Done using `Cmd/Ctrl+Enter` twice
5. Change "Main work" to Doing using `Cmd/Ctrl+Enter` once

---

## Tips

### Daily Task Review

Check your journal page each morning for scheduled tasks.

### Use Tags

Combine tasks with inline tags for better organization:

```
Call dentist #health
Review budget #finance
```

Then set each block's status to Todo using `Cmd/Ctrl+Enter`.

### Time Blocking

Use the Scheduled property with a specific time for focused work blocks. Type `/Scheduled` and pick a date and time from the date picker.

---

## What's Next?

- **[Repeating Tasks](../../how-to/tasks/repeating-tasks.md)** - Set up recurring tasks
- **[Task Properties](../../reference/features/task-properties.md)** - All task attributes
- **[Project Planning](project-planning.md)** - Larger project management

---

## Summary

- Created tasks using `/Todo` or `Cmd/Ctrl+Enter`
- Cycled states (Todo, Doing, Done) with `Cmd/Ctrl+Enter`
- Set priorities (High, Medium, Low, Urgent) with `/Priority` or `p p`
- Scheduled tasks and set deadlines with `/Scheduled` and `/Deadline`
- Queried tasks with `{{query (task TODO DOING)}}`

---

_Last updated: 2026-09-09_
