---
title: 'Todo Basics'
description: "Learn to manage tasks effectively with Logseq's task management system"
type: tutorial
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
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

> Learn to create, organize, and track tasks in Logseq.

## What You'll Learn

- ✅ Creating TODO items
- ✅ Task states (TODO, DOING, DONE)
- ✅ Setting priorities
- ✅ Scheduling and deadlines
- ✅ Querying your tasks

**Time:** ~15 minutes

---

## Step 1: Create Your First Task

### Using the Slash Command

1. On any page, type `/TODO`
2. Press Enter
3. Type your task description

**Result:**

```
TODO Buy groceries
```

### Using Keyboard Shortcut

1. Type any text
2. Press `Cmd+Enter` (macOS) or `Ctrl+Enter` (Windows/Linux)
3. This cycles through task states

### Manual Typing

Just type `TODO` at the start of any block:

```
TODO Finish the report
```

---

## Step 2: Cycle Task States

Tasks can be in different states:

| State       | Meaning          |
| ----------- | ---------------- |
| `TODO`      | Not started      |
| `DOING`     | In progress      |
| `DONE`      | Completed        |
| `LATER`     | Someday/Maybe    |
| `NOW`       | Do it now        |
| `WAITING`   | Blocked          |
| `CANCELLED` | No longer needed |

### Change State

**Option 1: Click the checkbox**

- Click once → DOING
- Click again → DONE

**Option 2: Keyboard**

- `Cmd/Ctrl+Enter` cycles: TODO → DOING → DONE → (none)

**Option 3: Type**

- Just change the keyword manually

### Try It

```
TODO Pick up dry cleaning
```

Click the checkbox to mark it DOING, then click again for DONE.

---

## Step 3: Set Priorities

Add priority with `/A`, `/B`, or `/C`:

```
TODO [#A] Fix urgent bug
TODO [#B] Review documentation
TODO [#C] Organize desk
```

**Priority meaning:**

- **[#A]** - High priority, do first
- **[#B]** - Medium priority
- **[#C]** - Low priority, nice to have

### Keyboard Shortcut

With cursor on task:

- `Cmd/Ctrl+Shift+1` → Priority A
- `Cmd/Ctrl+Shift+2` → Priority B
- `Cmd/Ctrl+Shift+3` → Priority C

---

## Step 4: Schedule Tasks

### Set a Date

Add `/Scheduled` or `/Deadline`:

```
TODO Submit proposal
SCHEDULED: <2026-02-15 Mon>
```

**Or:**

```
TODO Submit proposal
DEADLINE: <2026-02-15 Mon>
```

### Using Date Picker

1. Type `/Scheduled` or `/Deadline`
2. A date picker appears
3. Click a date

### Date Formats

```
<2026-02-15>              Plain date
<2026-02-15 Mon>          With day
<2026-02-15 Mon 14:00>    With time
```

---

## Step 5: Create a Task List

Organize related tasks:

```
TODO Project Alpha
  TODO Research competitors
  TODO Draft proposal
  TODO Create mockups
  DOING Write first draft
  DONE Initial brainstorm
```

Indent child tasks with `Tab`.

### Calculate Progress

Your parent task can show progress of children:

- 1/5 complete
- Visual progress bar (in some views)

---

## Step 6: Find Your Tasks

### Quick Method: Linked References

Every TODO, DONE, etc. is automatically indexed. Check the **Linked References** section at the bottom of any page.

### Query Method

Create a query to find all your TODOs:

```clojure
{{query (task TODO DOING)}}
```

This shows all tasks that are TODO or DOING.

### Advanced Query

Find all high-priority tasks:

```clojure
#+BEGIN_QUERY
{:title "🔴 High Priority"
 :query [:find (pull ?b [*])
         :where [?b :block/marker "TODO"]
                [?b :block/priority "A"]]}
#+END_QUERY
```

---

## Step 7: Task Properties (DB Graphs)

In database graphs, tasks can have properties:

```
TODO Complete report
status:: in-progress
assignee:: [[John]]
due:: [[2026-02-15]]
effort:: 2h
```

### Query by Properties

```clojure
{{query (property assignee [[John]])}}
```

---

## Practice Exercise

Create a simple project with tasks:

1. Create a new page called "Weekend Project"
2. Add these tasks:
   ```
   TODO [#A] Weekend Project Tasks
     TODO [#A] Buy supplies from hardware store
     SCHEDULED: <Saturday>
     TODO [#B] Prep work area
     TODO [#B] Main work
     DEADLINE: <Sunday>
     TODO [#C] Clean up
   ```
3. Mark "Buy supplies" as DONE
4. Change "Main work" to DOING

---

## Tips

### Daily Task Review

Check your journal page each morning for scheduled tasks.

### Use Tags

Combine tasks with tags for better organization:

```
TODO Call dentist #health
TODO Review budget #finance
```

### Time Blocking

Use scheduled times for focused work:

```
TODO Deep work session
SCHEDULED: <2026-02-15 Mon 09:00>
```

---

## What's Next?

- **[Repeating Tasks](../../how-to/tasks/repeating-tasks.md)** - Set up recurring tasks
- **[Task Properties](../../reference/features/task-properties.md)** - All task attributes
- **[Project Planning](project-planning.md)** - Larger project management

---

## Summary

✅ Created TODO items with `/TODO`  
✅ Cycled states with checkbox/keyboard  
✅ Set priorities with `[#A]`, `[#B]`, `[#C]`  
✅ Scheduled with dates  
✅ Queried tasks with `{{query}}`

---

_Last updated: 2026-01-29_
