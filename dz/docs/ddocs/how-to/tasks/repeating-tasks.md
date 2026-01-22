---
title: 'Setting Up Repeating Tasks'
status: 🟢
since-version: 'v65.0'
quadrant: 'How-To'
last-verified: '2026-01-11'
platform: 'All'
related-docs:
  - 'tutorials/task-management/todo-basics.md'
  - 'reference/features/property-history.md'
  - 'reference/database/property-types.md'
---

# Setting Up Repeating Tasks

Learn how to create tasks that automatically repeat on a schedule, perfect for recurring activities, habits, and regular reviews.

## Overview

Repeating tasks in Logseq automatically regenerate when completed, creating a new instance with the next due date. This is ideal for:

- 📅 Daily habits (exercise, journaling)
- 🗓️ Weekly reviews (team meetings, planning sessions)
- 📆 Monthly tasks (bill payments, reports)
- 🔄 Periodic maintenance (backups, updates)

## Basic Repeating Task

### Creating a Simple Repeat

Add `repeat` properties to any task:

```markdown
- TODO Water plants
  repeat/recur-frequency:: 3
  repeat/recur-unit:: day
```

**What this does:**

- Task repeats every 3 days
- When you mark it DONE, a new instance appears
- New instance has updated scheduled/deadline dates

### Quick Syntax

Use the slash command:

1. Create a task: `TODO Review notes`
2. Type `/repeat`
3. Select repeat interval
4. Configure frequency and unit

## Repeat Properties

### Core Properties

| Property                   | Type     | Values                               | Description                     |
| -------------------------- | -------- | ------------------------------------ | ------------------------------- |
| `repeat/recur-frequency`   | Number   | 1, 2, 3...                           | How often to repeat             |
| `repeat/recur-unit`        | Choice   | minute, hour, day, week, month, year | Time unit                       |
| `repeat/repeated?`         | Boolean  | true/false                           | Marks as repeating task         |
| `repeat/temporal-property` | Property | deadline or scheduled                | Which date to update            |
| `repeat/checked-property`  | Property | status                               | Which property marks completion |

### Property Details

**From code ([property.cljs:379-421](../../deps/db/src/logseq/db/frontend/property.cljs#L379-L421)):**

```clojure
:logseq.property.repeat/recur-frequency
{:title "Repeating recur frequency"
 :schema {:type :number :public? false}
 :properties {:logseq.property/default-value 1}
 :queryable? true}

:logseq.property.repeat/recur-unit
{:title "Repeating recur unit"
 :schema {:type :default :public? false}
 :closed-values [
   :logseq.property.repeat/recur-unit.minute
   :logseq.property.repeat/recur-unit.hour
   :logseq.property.repeat/recur-unit.day
   :logseq.property.repeat/recur-unit.week
   :logseq.property.repeat/recur-unit.month
   :logseq.property.repeat/recur-unit.year]
 :properties {:logseq.property/default-value
              :logseq.property.repeat/recur-unit.day}}
```

## Common Patterns

### Daily Habit

```markdown
- TODO Morning meditation
  scheduled:: [[2026-01-11]]
  repeat/recur-frequency:: 1
  repeat/recur-unit:: day
  repeat/temporal-property:: scheduled
```

**Behavior:**

- Appears every day
- When completed today, reschedules to tomorrow
- Skipping a day doesn't affect future dates

### Weekly Review

```markdown
- TODO Weekly team review
  scheduled:: [[2026-01-13]]
  repeat/recur-frequency:: 1
  repeat/recur-unit:: week
  repeat/temporal-property:: scheduled
```

**Behavior:**

- Repeats every Monday (if Jan 13 is Monday)
- Completion creates next Monday's instance
- Always same day of week

### Monthly Bill Payment

```markdown
- TODO Pay internet bill
  deadline:: [[2026-01-31]]
  repeat/recur-frequency:: 1
  repeat/recur-unit:: month
  repeat/temporal-property:: deadline
```

**Behavior:**

- Due last day of each month
- Completion sets next month's deadline
- Adjusts for month length (28-31 days)

### Biweekly Meeting

```markdown
- TODO Sprint planning meeting
  scheduled:: [[2026-01-13]]
  repeat/recur-frequency:: 2
  repeat/recur-unit:: week
  repeat/temporal-property:: scheduled
```

**Behavior:**

- Every 2 weeks
- Consistent interval from completion date

### Quarterly Review

```markdown
- TODO Quarterly goal review
  deadline:: [[2026-03-31]]
  repeat/recur-frequency:: 3
  repeat/recur-unit:: month
  repeat/temporal-property:: deadline
```

**Behavior:**

- Every 3 months
- Q1: Mar 31, Q2: Jun 30, Q3: Sep 30, Q4: Dec 31

## Advanced Configuration

### Multiple Deadlines

Create separate tasks for different aspects:

```markdown
- # Project Review (Repeating)

  - TODO Prepare review materials
    deadline:: [[2026-01-15]]
    repeat/recur-frequency:: 1
    repeat/recur-unit:: month
  - TODO Conduct review meeting
    deadline:: [[2026-01-20]]
    repeat/recur-frequency:: 1
    repeat/recur-unit:: month
```

### Conditional Repeats

Use templates for complex patterns:

```markdown
- template:: monthly-report
  TODO {{Monthly Report for [[<%currentmonth%>]]}}
  deadline:: [[<%endofmonth%>]]
  repeat/recur-frequency:: 1
  repeat/recur-unit:: month
  checklist::
  - Gather data
  - Create charts
  - Write summary
  - Submit report
```

### Chained Tasks

Create task sequences:

```markdown
- TODO Phase 1: Planning
  repeat/recur-frequency:: 6
  repeat/recur-unit:: month
  next-task:: [[Phase 2: Execution]]
- TODO Phase 2: Execution
  repeat/recur-frequency:: 6
  repeat/recur-unit:: month
  previous-task:: [[Phase 1: Planning]]
```

## Managing Repeating Tasks

### Viewing All Repeats

Query all repeating tasks:

```clojure
#+BEGIN_QUERY
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property.repeat/repeated? true]]
 :breadcrumb-show? false}
#+END_QUERY
```

### Filter by Frequency

Find daily tasks:

```clojure
#+BEGIN_QUERY
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property.repeat/repeated? true]
         [?b :logseq.property.repeat/recur-frequency 1]
         [?b :logseq.property.repeat/recur-unit
             :logseq.property.repeat/recur-unit.day]]}
#+END_QUERY
```

### Dashboard Query

Create a repeating tasks dashboard:

```markdown
- # Repeating Tasks Dashboard

  - ## Daily

    #+BEGIN_QUERY
    {:query [:find (pull ?b [:block/title :block/scheduled])
    :where
    [?b :logseq.property.repeat/repeated? true]
    [?b :logseq.property.repeat/recur-unit
    :logseq.property.repeat/recur-unit.day]]
    :order-by [[?scheduled :asc]]}
    #+END_QUERY

  - ## Weekly
    #+BEGIN_QUERY
    {:query [:find (pull ?b [:block/title :block/scheduled])
    :where
    [?b :logseq.property.repeat/repeated? true]
    [?b :logseq.property.repeat/recur-unit
    :logseq.property.repeat/recur-unit.week]]}
    #+END_QUERY
```

### Pause a Repeat

Temporarily stop repeating:

```markdown
- TODO Task to pause
  repeat/recur-frequency:: 1
  repeat/recur-unit:: week
  repeat/repeated?:: false ← Set to false
```

Or delete the repeat properties entirely.

### Modify Repeat Schedule

Change frequency:

```markdown
- TODO Task
  repeat/recur-frequency:: 2 ← Changed from 1
  repeat/recur-unit:: week
```

Existing incomplete instance remains; next instance uses new schedule.

## Completion Behavior

### What Happens When You Complete

1. **Current task marked DONE**

   - Status changes to DONE
   - Completion timestamp added

2. **New instance created**

   - Copy of original task
   - Status reset to TODO
   - Date updated based on repeat properties

3. **New instance placement**
   - Created in journal of scheduled/deadline date
   - Or same page if no temporal property

### Example Flow

**Original:**

```markdown
- TODO Water plants
  scheduled:: [[2026-01-11]]
  repeat/recur-frequency:: 3
  repeat/recur-unit:: day
```

**After completion on Jan 11:**

```markdown
- DONE Water plants
  scheduled:: [[2026-01-11]]
  completed:: [[2026-01-11]]
  repeat/recur-frequency:: 3
  repeat/recur-unit:: day
- TODO Water plants
  scheduled:: [[2026-01-14]] ← 3 days later
  repeat/recur-frequency:: 3
  repeat/recur-unit:: day
```

## Integration with Other Features

### With Scheduled & Deadline

Combine for flexible scheduling:

```markdown
- TODO Important repeating task
  scheduled:: [[2026-01-11]] ← Start working on this date
  deadline:: [[2026-01-15]] ← Must complete by this date
  repeat/recur-frequency:: 2
  repeat/recur-unit:: week
  repeat/temporal-property:: scheduled ← Which date to advance
```

### With Task Properties

Add priorities and status:

```markdown
- TODO Review security logs
  scheduled:: [[2026-01-11]]
  priority:: High
  status:: Todo
  tags:: #security #operations
  repeat/recur-frequency:: 1
  repeat/recur-unit:: day
```

### With Templates

Create complex repeating structures:

```markdown
- template:: weekly-review
  TODO Weekly Review
  scheduled:: [[<%today%>]]
  repeat/recur-frequency:: 1
  repeat/recur-unit:: week
  repeat/temporal-property:: scheduled

  - ## What went well?
  - ## What needs improvement?
  - ## Goals for next week
```

### With Property History

Track completion history:

```markdown
- TODO Habit tracker
  repeat/recur-frequency:: 1
  repeat/recur-unit:: day
  enable-history?:: true ← Track property changes
```

View completion patterns in property history panel.

## Troubleshooting

### Task Not Repeating

**Problem:** Completed task doesn't create new instance

**Check:**

1. `repeat/repeated?:: true` is set
2. `repeat/recur-frequency` has valid number
3. `repeat/recur-unit` is one of valid values
4. Task status is marked as completed (DONE)

### Wrong Repeat Date

**Problem:** New instance has incorrect date

**Solution:**

- Verify `repeat/temporal-property` is set to correct property (scheduled or deadline)
- Check frequency/unit combination
- Ensure temporal property exists on original task

### Duplicate Instances

**Problem:** Multiple repeated tasks appear

**Solution:**

- Don't manually copy repeating tasks
- Complete only one instance at a time
- Check for duplicate repeat properties

### Can't Pause Repeat

**Problem:** Task keeps regenerating

**Solution:**

- Set `repeat/repeated?:: false`
- Or remove all `repeat/*` properties
- Complete and delete unwanted future instances

## Best Practices

### 1. Use Appropriate Units

```markdown
✅ Good: Natural intervals

- Daily: Use "day"
- Weekly: Use "week"
- Monthly: Use "month"

❌ Avoid: Awkward conversions

- Weekly: Don't use "7 days"
- Monthly: Don't use "4 weeks" (inaccurate)
```

### 2. Choose Right Temporal Property

```markdown
✅ scheduled: For start-date tasks

- TODO Begin monthly review
  scheduled:: [[2026-01-15]]
  repeat/temporal-property:: scheduled

✅ deadline: For due-date tasks

- TODO Submit report
  deadline:: [[2026-01-31]]
  repeat/temporal-property:: deadline
```

### 3. Organize by Frequency

```markdown
# Daily Habits

- TODO Exercise
- TODO Journal

# Weekly Reviews

- TODO Team standup
- TODO Personal review

# Monthly Tasks

- TODO Pay bills
- TODO Review goals
```

### 4. Use Queries for Overview

Create master view of all repeats:

```markdown
- # All Repeating Tasks
  #+BEGIN_QUERY
  {:query [:find (pull ?b [*])
  :where
  [?b :logseq.property.repeat/repeated? true]]
  :collapsed? false
  :group-by-page? false}
  #+END_QUERY
```

## See Also

- [Todo System Basics](../../tutorials/task-management/todo-basics.md)
- [Property History Guide](property-history.md)
- [Property Types Reference](../../reference/database/property-types.md)
