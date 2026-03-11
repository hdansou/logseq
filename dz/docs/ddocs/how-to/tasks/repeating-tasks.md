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

> **Last revised:** 2026-03-10

Learn how to create tasks that automatically repeat on a schedule, perfect for recurring activities, habits, and regular reviews.

## Overview

Repeating tasks in Logseq automatically reschedule themselves when completed. When you mark a repeating task as DONE, Logseq advances the date to the next occurrence and resets the status back to TODO on the same task. This is ideal for:

- Daily habits (exercise, journaling)
- Weekly reviews (team meetings, planning sessions)
- Monthly tasks (bill payments, reports)
- Periodic maintenance (backups, updates)

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
- When you mark it DONE, the date advances and the status resets to TODO
- The scheduled or deadline date is updated to the next occurrence

### Enabling Repeat via the UI

To enable repeating on a task through the interface:

1. Create a task: `TODO Review notes`
2. Add a `scheduled` or `deadline` date property
3. Click the date property to open its popup
4. Check the "Repeat task" checkbox in the popup
5. Configure frequency and unit

## Repeat Properties

### Core Properties

| Property                   | Type     | Values                               | Description                     |
| -------------------------- | -------- | ------------------------------------ | ------------------------------- |
| `repeat/recur-frequency`   | Number   | 1, 2, 3...                           | How often to repeat             |
| `repeat/recur-unit`        | Choice   | Minute, Hour, Day, Week, Month, Year | Time unit                       |
| `repeat/repeated?`         | Checkbox | true/false                           | Marks as repeating task         |
| `repeat/temporal-property` | Property | deadline or scheduled                | Which date to update            |
| `repeat/checked-property`  | Property | (defaults to status)                 | Which property marks completion |

### Property Details

**From code ([property.cljs:368-405](../../deps/db/src/logseq/db/frontend/property.cljs#L368-L405)):**

```clojure
:logseq.property.repeat/recur-frequency
(let [schema {:type :number
              :public? false}]
  {:title "Repeating recur frequency"
   :schema schema
   :properties {:logseq.property/hide-empty-value true
                :logseq.property/default-value 1}
   :queryable? true})

:logseq.property.repeat/recur-unit
{:title "Repeating recur unit"
 :schema {:type :default :public? false}
 :closed-values (mapv (fn [[db-ident value]]
                        {:db-ident db-ident :value value ...})
                      [[:logseq.property.repeat/recur-unit.minute "Minute"]
                       [:logseq.property.repeat/recur-unit.hour "Hour"]
                       [:logseq.property.repeat/recur-unit.day "Day"]
                       [:logseq.property.repeat/recur-unit.week "Week"]
                       [:logseq.property.repeat/recur-unit.month "Month"]
                       [:logseq.property.repeat/recur-unit.year "Year"]])
 :properties {:logseq.property/hide-empty-value true
              :logseq.property/default-value :logseq.property.repeat/recur-unit.day}
 :queryable? true}

:logseq.property.repeat/repeated?
{:title "Node Repeats?"
 :schema {:type :checkbox :hide? true}
 :queryable? true}

:logseq.property.repeat/temporal-property
{:title "Repeating Temporal Property"
 :schema {:type :property :hide? true}}

:logseq.property.repeat/checked-property
{:title "Repeating Checked Property"
 :schema {:type :property :hide? true}}
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
- If completed late, advances to the next future date (not the missed date)

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
- Completion advances the scheduled date to next Monday
- Preserves the day of week (the code adjusts if the day-of-week drifts)

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
- Consistent interval, always advancing to a future date

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

### Both Scheduled and Deadline

When a task has both `scheduled` and `deadline` properties, completion reschedules all temporal properties, not just the one specified in `repeat/temporal-property`. The `temporal-property` determines the primary date to advance, and the other date properties are also updated accordingly.

**From code ([commands.cljs:184-198](../../src/main/frontend/worker/commands.cljs#L184-L198)):**

```clojure
(defmethod handle-command :reschedule [_ db entity _datoms]
  (let [property-ident (or (:db/ident (:logseq.property.repeat/temporal-property entity))
                           :logseq.property/scheduled)
        other-property-idents (cond
                                (and (= property-ident :logseq.property/scheduled)
                                     (:logseq.property/deadline entity))
                                [:logseq.property/deadline]
                                ...)]
    (mapcat #(compute-reschedule-property-tx db entity %)
            (distinct (cons property-ident other-property-idents)))))
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
    {:query [:find (pull ?b [*])
    :where
    [?b :logseq.property.repeat/repeated? true]
    [?b :logseq.property.repeat/recur-unit
    :logseq.property.repeat/recur-unit.day]]}
    #+END_QUERY

  - ## Weekly
    #+BEGIN_QUERY
    {:query [:find (pull ?b [*])
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
  repeat/repeated?:: false
```

Or delete the repeat properties entirely.

### Modify Repeat Schedule

Change frequency:

```markdown
- TODO Task
  repeat/recur-frequency:: 2
  repeat/recur-unit:: week
```

The next completion will use the new schedule.

## Completion Behavior

### What Happens When You Complete

When you mark a repeating task as DONE, Logseq runs a command pipeline (defined in `frontend.worker.commands`) that:

1. **Reschedules the date** -- The scheduled/deadline date advances to the next future occurrence based on the repeat frequency and unit
2. **Resets status to TODO** -- The task's status property is set back to TODO (or the unchecked state of the `checked-property`)

The task is modified in place -- no new copy is created. The same block gets its date updated and status reset.

**From code ([commands.cljs:18-25](../../src/main/frontend/worker/commands.cljs#L18-L25)):**

```clojure
[:repeated-task
 {:title "Repeated task"
  :entity-conditions [{:property :logseq.property.repeat/repeated?
                       :value true}]
  :tx-conditions [{:property :status
                   :value :done}]
  :actions [[:reschedule]
            [:set-property :status :todo]]}]
```

### Example Flow

**Original:**

```markdown
- TODO Water plants
  scheduled:: [[2026-01-11]]
  repeat/recur-frequency:: 3
  repeat/recur-unit:: day
```

**After marking DONE on Jan 11 (same task, updated in place):**

```markdown
- TODO Water plants
  scheduled:: [[2026-01-14]]
  repeat/recur-frequency:: 3
  repeat/recur-unit:: day
```

The date advances 3 days to Jan 14, and the status resets to TODO. If you completed a task that was overdue (e.g., you mark it done on Jan 15 when it was scheduled for Jan 11), the date skips forward to the next future occurrence (Jan 17), not the next sequential date (Jan 14).

## Integration with Other Features

### With Scheduled & Deadline

Combine for flexible scheduling:

```markdown
- TODO Important repeating task
  scheduled:: [[2026-01-11]]
  deadline:: [[2026-01-15]]
  repeat/recur-frequency:: 2
  repeat/recur-unit:: week
  repeat/temporal-property:: scheduled
```

When `temporal-property` is set to `scheduled`, the scheduled date is the primary date advanced, but the deadline is also rescheduled. If `temporal-property` is not set, it defaults to `scheduled`.

### With Task Properties

Add priorities and other properties:

```markdown
- TODO Review security logs
  scheduled:: [[2026-01-11]]
  priority:: High
  repeat/recur-frequency:: 1
  repeat/recur-unit:: day
```

### With Property History

Property history is enabled per-property, not per-task. To track changes to a property across all tasks that use it, enable history on the property itself via the property settings UI. The `enable-history?` setting is a property-level checkbox (`:view-context :property`), not a block-level property.

## Troubleshooting

### Task Not Repeating

**Problem:** Completed task doesn't reschedule

**Check:**

1. `repeat/repeated?` is set to true (checkbox checked)
2. `repeat/recur-frequency` has valid number
3. `repeat/recur-unit` is one of the valid values (Minute, Hour, Day, Week, Month, Year)
4. Task has a `scheduled` or `deadline` date property set
5. Task status is being changed to DONE (or the checked state of the `checked-property`)

### Wrong Repeat Date

**Problem:** Task reschedules to incorrect date

**Solution:**

- Verify `repeat/temporal-property` is set to the correct property (scheduled or deadline). If unset, it defaults to scheduled.
- Check frequency/unit combination
- Ensure the temporal property has a date value on the task
- Note: the reschedule always advances to a future date, skipping past occurrences

### Can't Pause Repeat

**Problem:** Task keeps rescheduling

**Solution:**

- Uncheck `repeat/repeated?` (set to false)
- Or remove all `repeat/*` properties

## Best Practices

### 1. Use Appropriate Units

```markdown
Good: Natural intervals

- Daily: Use "day"
- Weekly: Use "week"
- Monthly: Use "month"

Avoid: Awkward conversions

- Weekly: Don't use "7 days"
- Monthly: Don't use "4 weeks" (inaccurate)
```

### 2. Choose Right Temporal Property

```markdown
scheduled: For start-date tasks

- TODO Begin monthly review
  scheduled:: [[2026-01-15]]
  repeat/temporal-property:: scheduled

deadline: For due-date tasks

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
