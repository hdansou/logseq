---
title: 'Setting Up Repeating Tasks'
status: 🟢
since-version: 'v65.0'
quadrant: 'How-To'
last-verified: '2026-09-11'
verified-against: 'be800f1711'
reviewStatus: audited
platform: 'All'
related-docs:
  - 'tutorials/task-management/todo-basics.md'
  - 'how-to/tasks/disable-task-time-tracking.md'
  - 'reference/data/properties-reference.md'
---

# Setting Up Repeating Tasks

> **Last revised:** 2026-09-11 (verified against `be800f1711`)

This guide shows how to make a task in a Logseq **DB graph** repeat. When you mark it Done, Logseq moves its date forward and sets it back to Todo. It suits recurring activities, habits and regular reviews.

> **File-graph syntax does not apply.** Org repeater cookies such as `SCHEDULED: <2026-01-11 Sun .+1d>`, `TODO`/`DONE` text markers, and `key:: value` lines such as `repeat/recur-unit:: day` are file-graph syntax. A DB graph reads none of them. Repeating is stored in hidden built-in properties that the date picker sets for you ([property.cljs:354-399](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L354-L399)). Importing a file graph converts its `.+`, `+` and `++` repeaters into those properties ([exporter.cljs:558-585](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs#L558-L585)).

## Overview

A repeating task reschedules itself when you complete it. After each transaction, the DB worker runs its built-in commands ([pipeline.cljs:515-518](../../../../../src/main/frontend/worker/pipeline.cljs#L515-L518)). When a task has **Repeat task** turned on and its Status changes to Done, the "Repeated task" command:

1. moves the task's Scheduled and/or Deadline date to the next occurrence, and
2. sets Status back to Todo ([commands.cljs:18-25](../../../../../src/main/frontend/worker/commands.cljs#L18-L25)).

The same block is updated in place. No copy is created. The command does not run for an undo, a rebase, or changes that arrive from sync or a graph download ([pipeline.cljs:515-517](../../../../../src/main/frontend/worker/pipeline.cljs#L515-L517)).

This is ideal for:

- Daily habits (exercise, journaling)
- Weekly reviews (team meetings, planning sessions)
- Monthly tasks (bill payments, reports)
- Periodic maintenance (backups, updates)

## Findings: New Capabilities

- Repeat type is a first-class setting with three behaviors. **Advance from completion** (`dotted-plus`) counts from the moment you complete the task. **Advance from scheduled** (`plus`) counts once from the current date. **Advance from scheduled, skip to future** (`double-plus`) counts from the current date in whole steps until the result is in the future ([commands.cljs:111-155](../../../../../src/main/frontend/worker/commands.cljs#L111-L155)).
- The repeat type defaults to `double-plus`, both as the property's default value ([property.cljs:392](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L392)) and as the runtime fallback when the property is absent ([commands.cljs:201-202](../../../../../src/main/frontend/worker/commands.cljs#L201-L202)).
- The **When** setting (`repeat/checked-property`) lets a repeating task use a property other than Status as its completion toggle ([commands.cljs:33-67](../../../../../src/main/frontend/worker/commands.cljs#L33-L67)).
- Any date or datetime property can repeat, not only Scheduled and Deadline. The repeat panel appears in every date property's picker, where it is labelled "Repeat date" or "Repeat datetime" ([value.cljs:388-392](../../../../../src/main/frontend/components/property/value.cljs#L388-L392), [:539](../../../../../src/main/frontend/components/property/value.cljs#L539)). A `:date` property advances to a journal page ([commands.cljs:204-217](../../../../../src/main/frontend/worker/commands.cljs#L204-L217)).
- Importing a file graph converts its org repeaters into these properties ([exporter.cljs:566-585](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs#L566-L585)).

> **Corrected:** an earlier version said repeat properties are "preserved by the graph parser/exporter round-trip". The code converts in one direction only: file-graph repeaters become DB repeat properties on import.

## Basic Repeating Task

### Creating a Simple Repeat

1. Create a task: type `/Todo`, then `Water plants`.
2. Type `/Scheduled` (or `/Deadline`). The date editor opens for that property ([commands.cljs:264-276](../../../../../src/main/frontend/commands.cljs#L264-L276), [:664-668](../../../../../src/main/frontend/commands.cljs#L664-L668)).
3. Pick a date, and a time if you want one.
4. In the panel beside the calendar, check **Repeat task**. This turns on the task's repeat flag and records the property you opened as the date to advance ([value.cljs:379-392](../../../../../src/main/frontend/components/property/value.cljs#L379-L392)).
5. Under **Every**, enter `3` and choose **Day** ([value.cljs:393-399](../../../../../src/main/frontend/components/property/value.cljs#L393-L399)).
6. Under **Next date**, keep **Advance from scheduled, skip to future** ([value.cljs:400-403](../../../../../src/main/frontend/components/property/value.cljs#L400-L403)).
7. Leave **When** set to Status. The panel shows **is: Done** ([value.cljs:404-443](../../../../../src/main/frontend/components/property/value.cljs#L404-L443)).

**What this does:**

- The task repeats every 3 days.
- When you mark it Done, the Scheduled date advances according to the repeat type, and Status goes back to Todo.
- A repeating task shows a repeat icon next to its date ([value.cljs:672-673](../../../../../src/main/frontend/components/property/value.cljs#L672-L673)).

> **Note:** the repeat panel has the classes `hidden sm:flex` ([value.cljs:375](../../../../../src/main/frontend/components/property/value.cljs#L375)), so it is not shown on narrow screens.

> **Removed:** the `repeat/repeated?:: true`, `repeat/recur-frequency:: 3` … text blocks. In a DB graph that text is ordinary block content, not properties, and it makes nothing repeat.

## Repeat Properties

### Core Properties

These are hidden built-in properties. You set them through the panel, not by typing them.

| Property ident | Panel control | Type | Values and default |
| -------------- | ------------- | ---- | ------------------ |
| `:logseq.property.repeat/repeated?` | **Repeat task** checkbox | Checkbox | true/false |
| `:logseq.property.repeat/recur-frequency` | **Every** number | Number | Default `1` |
| `:logseq.property.repeat/recur-unit` | **Every** unit | Closed values | Minute, Hour, Day, Week, Month, Year. Default Day. Minute and Hour are not offered for a `:date` property ([value.cljs:1246-1255](../../../../../src/main/frontend/components/property/value.cljs#L1246-L1255)) |
| `:logseq.property.repeat/repeat-type` | **Next date** | Closed values | Advance from completion, Advance from scheduled, Advance from scheduled, skip to future (default) |
| `:logseq.property.repeat/temporal-property` | Set when you check **Repeat task** | Property | The date property whose picker you opened. Removed when you uncheck **Repeat task** ([value.cljs:381-387](../../../../../src/main/frontend/components/property/value.cljs#L381-L387)) |
| `:logseq.property.repeat/checked-property` | **When** | Property | Unset means Status ([commands.cljs:33-39](../../../../../src/main/frontend/worker/commands.cljs#L33-L39)). The choices are Status plus any non-built-in property on the block with at least two closed values ([value.cljs:404-409](../../../../../src/main/frontend/components/property/value.cljs#L404-L409)) |

### Property Details

The definitions are at [property.cljs:354-399](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L354-L399). An unset frequency falls back to 1 at completion time ([commands.cljs:173-195](../../../../../src/main/frontend/worker/commands.cljs#L173-L195)). Looking up an unset property on an entity returns the property's default value ([entity_plus.cljc:112-127](../../../../../deps/db/src/logseq/db/common/entity_plus.cljc#L112-L127)), so an unset unit reads as Day.

### Repeat Types

| Panel choice | Ident | Next date |
| ------------ | ----- | --------- |
| Advance from completion | `repeat-type.dotted-plus` | Now + frequency × unit. The time of day comes from the moment you complete the task ([commands.cljs:111-114](../../../../../src/main/frontend/worker/commands.cljs#L111-L114)) |
| Advance from scheduled | `repeat-type.plus` | Current date + frequency × unit, once. The result can still be in the past ([commands.cljs:116-121](../../../../../src/main/frontend/worker/commands.cljs#L116-L121)) |
| Advance from scheduled, skip to future | `repeat-type.double-plus` | Current date + whole multiples of frequency × unit, until the result is after now ([commands.cljs:123-140](../../../../../src/main/frontend/worker/commands.cljs#L123-L140)). Also used for an unknown type ([commands.cljs:154-155](../../../../../src/main/frontend/worker/commands.cljs#L154-L155)) |

## Common Patterns

Each pattern lists the panel settings. Create the task with `/Todo`, then set the date with `/Scheduled` or `/Deadline`.

### Daily Habit

- Task: **Morning meditation**
- Scheduled: 2026-01-11
- **Repeat task** checked, **Every** 1 Day, **Next date** left at the default

**Behavior:**

- Completing it on the scheduled day moves Scheduled to the next day.
- If you complete it late, Scheduled moves to the next date after now, not to the missed date.

### Weekly Review

- Task: **Weekly team review**
- Scheduled: 2026-01-12 (a Monday)
- **Repeat task** checked, **Every** 1 Week

**Behavior:**

- Completion moves Scheduled to the next Monday.
- The weekday is preserved. cljs-time adds a week as 7 days, and a test checks this ("preserves week day", [commands_test.cljs:116-117](../../../../../src/test/frontend/worker/commands_test.cljs#L116-L117)).

> **Corrected:** the earlier example used 2026-01-13 and called it a Monday. 2026-01-13 is a Tuesday.

### Monthly Bill Payment

- Task: **Pay internet bill**
- Deadline: 2026-01-28
- **Repeat task** checked, **Every** 1 Month

**Behavior:**

- Each completion moves Deadline to the same day of the next month.
- Pick a day that exists in every month (1–28). cljs-time adds months with `goog.date.Interval.MONTHS`, and Closure's `goog.date.Date.prototype.add` caps the day at the target month's length. The capped day is then carried forward, so a Jan 31 deadline goes to Feb 28, then Mar 28. This follows from library code (cljs-time `period-fn`; Closure `goog/date/date.js`) and has not been reproduced in the app.

> **Removed:** "Due last day of each month … adjusts for month length (28-31 days)". Nothing in the repeat code tracks month-end.

### Biweekly Meeting

- Task: **Sprint planning meeting**
- Scheduled: 2026-01-12
- **Repeat task** checked, **Every** 2 Week

**Behavior:**

- Every 2 weeks.
- With the default **Next date**, the result is always in the future.

### Quarterly Review

- Task: **Quarterly goal review**
- Deadline: 2026-03-31
- **Repeat task** checked, **Every** 3 Month

**Behavior:**

- Every 3 months. Because the day is capped and then carried forward (see above), the dates run Mar 31 → Jun 30 → Sep 30 → Dec 30. They do not return to the 31st.

> **Corrected:** the earlier version said the fourth date would be Dec 31.

## Advanced Configuration

### Multiple Deadlines

Create separate tasks for different aspects. Put them under a parent block, and give each child its own Deadline and repeat settings:

- Parent block: **Project Review (Repeating)**
  - Child: **Prepare review materials**. Status Todo, Deadline 2026-01-15, **Repeat task** on, **Every** 1 Month
  - Child: **Conduct review meeting**. Status Todo, Deadline 2026-01-20, **Repeat task** on, **Every** 1 Month

### Both Scheduled and Deadline

When a task has both `scheduled` and `deadline` properties, completion reschedules both, not only the one named by `repeat/temporal-property`. Two cases:

- **`temporal-property` is Scheduled or Deadline.** That property is advanced, and so is the other one if the task has it.
- **`temporal-property` is not set.** Every one of Scheduled and Deadline that is present on the task is advanced. It does *not* silently default to Scheduled.

If `temporal-property` is some other date property, only that property is advanced (the `case` returns `nil` for it).

**From code ([commands.cljs:224-249](../../../../../src/main/frontend/worker/commands.cljs#L224-L249)):**

```clojure
(def ^:private repeat-temporal-property-idents
  [:logseq.property/scheduled :logseq.property/deadline])

(defn- existing-repeat-temporal-property-idents
  [entity]
  (filterv #(some? (get entity %)) repeat-temporal-property-idents))

(defn- reschedule-property-idents
  [entity]
  (let [explicit-property-ident (:db/ident (:logseq.property.repeat/temporal-property entity))]
    (if explicit-property-ident
      (let [other-property-idents (case explicit-property-ident
                                   :logseq.property/scheduled
                                   (when (:logseq.property/deadline entity)
                                     [:logseq.property/deadline])

                                   :logseq.property/deadline
                                   (when (:logseq.property/scheduled entity)
                                     [:logseq.property/scheduled])

                                   nil)]
        (distinct (cons explicit-property-ident other-property-idents)))
      (existing-repeat-temporal-property-idents entity))))

(defmethod handle-command :reschedule [_ db entity _datoms]
  (mapcat #(compute-reschedule-property-tx db entity %) (reschedule-property-idents entity)))
```

If the task has no value for any of these dates, nothing is rescheduled, because `compute-reschedule-property-tx` returns nothing without a current date value ([commands.cljs:209](../../../../../src/main/frontend/worker/commands.cljs#L209)). The second action still runs, though ([commands.cljs:251-254](../../../../../src/main/frontend/worker/commands.cljs#L251-L254)), so Status goes straight back to Todo.

## Managing Repeating Tasks

### Viewing All Repeats

Type `/Advanced Query` and enter:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property.repeat/repeated? true]]}
```

Checkbox values are stored as plain booleans, because `:checkbox` is not a ref property type ([type.cljs:58-74](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs#L58-L74)).

### Filter by Unit

Find weekly repeats:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property.repeat/repeated? true]
         [?b :logseq.property.repeat/recur-unit ?u]
         [?u :db/ident :logseq.property.repeat/recur-unit.week]]}
```

The unit is a closed value, so match it through its `:db/ident`. The frequency is a number, and number values are stored as refs whose value lives in `:logseq.property/value` ([type.cljs:52-61](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs#L52-L61)). To filter on it, use `[?b :logseq.property.repeat/recur-frequency ?f] [?f :logseq.property/value 2]`.

> **Corrected:** the earlier `[?b :logseq.property.repeat/recur-frequency 1]` never matches, because the attribute holds a ref, not the number.

> **Unverified:** whether the panel stores a unit or frequency you leave at its default. If it does not, those tasks have no stored datom, and a `:where` pattern on that attribute will not find them. That is why this example filters on Week rather than the default Day.

### Dashboard Query

Create a repeating-tasks dashboard. Each query needs its own block, so put the headings in parent blocks:

- **Repeating Tasks Dashboard**
  - **Daily**
    - An `/Advanced Query` block:
      ```clojure
      {:query [:find (pull ?b [*])
               :where
               [?b :logseq.property.repeat/repeated? true]
               [?b :logseq.property.repeat/recur-unit ?u]
               [?u :db/ident :logseq.property.repeat/recur-unit.day]]}
      ```
  - **Weekly**
    - An `/Advanced Query` block:
      ```clojure
      {:query [:find (pull ?b [*])
               :where
               [?b :logseq.property.repeat/repeated? true]
               [?b :logseq.property.repeat/recur-unit ?u]
               [?u :db/ident :logseq.property.repeat/recur-unit.week]]}
      ```

> **Removed:** the `#+BEGIN_QUERY` … `#+END_QUERY` wrappers. In a DB graph they are deprecated in favour of `/Advanced Query` ([en.edn:120](../../../../../src/resources/dicts/en.edn#L120)). The `:breadcrumb-show?` and `:group-by-page?` keys were removed too. See the [Query System reference](../../reference/advanced/query-system.md) for the keys a DB graph honours.

### Pause a Repeat

Open the task's date picker and uncheck **Repeat task**. That sets the repeat flag to false and removes `temporal-property` ([value.cljs:381-387](../../../../../src/main/frontend/components/property/value.cljs#L381-L387)). The command only runs for tasks whose repeat flag is true ([commands.cljs:20-21](../../../../../src/main/frontend/worker/commands.cljs#L20-L21)).

### Modify Repeat Schedule

Open the date picker and change **Every** or **Next date**. The values are read at the moment of completion ([commands.cljs:199-202](../../../../../src/main/frontend/worker/commands.cljs#L199-L202)), so the next completion uses the new schedule.

## Completion Behavior

### What Happens When You Complete

When you mark a repeating task as Done, Logseq runs a command pipeline (defined in `frontend.worker.commands`) that:

1. **Reschedules the date.** The date advances according to the repeat type, frequency and unit.
2. **Resets completion state.** Status is set back to Todo, or the configured **When** property returns to its unchecked state.

The task is modified in place, and no new copy is created. The same block gets its date updated and its status reset.

**From code ([commands.cljs:18-25](../../../../../src/main/frontend/worker/commands.cljs#L18-L25)):**

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

The trigger is a transaction that sets Status to Done ([commands.cljs:98-106](../../../../../src/main/frontend/worker/commands.cljs#L98-L106)). It makes no difference how you set it: `Cmd/Ctrl+Enter`, `/Done`, the status picker or a checkbox.

With a **When** property, "done" means the choice whose checkbox state is true (or `true` for a checkbox property), and "reset" means the choice whose checkbox state is false (or `false`) ([commands.cljs:41-67](../../../../../src/main/frontend/worker/commands.cljs#L41-L67)).

> **Possible defect (from reading the source, not reproduced):** if the **When** property is a closed-value property and none of its choices has a checkbox state, `get-value` falls back to `:logseq.property/status.done`. That value can never belong to the other property, so the task never repeats. Meanwhile the panel still shows "is: Done", because it uses the same fallback ([value.cljs:417-419](../../../../../src/main/frontend/components/property/value.cljs#L417-L419)).

### Example Flow

**Original:** **Water plants**, Status Todo, Scheduled 2026-01-11, **Every** 3 Day, default **Next date**.

**After marking it Done on Jan 11:** the same block, with Scheduled 2026-01-14 and Status Todo.

The date advances 3 days to Jan 14, and the status resets to Todo. Suppose instead you complete an overdue task on Jan 15 when it was scheduled for Jan 11. The date skips forward to the next occurrence after now, Jan 17, not the next one in sequence, Jan 14 ([commands.cljs:123-140](../../../../../src/main/frontend/worker/commands.cljs#L123-L140)). With **Advance from scheduled** it would be Jan 14, which is already past. With **Advance from completion** it would be Jan 18, at the time you completed it.

## Integration with Other Features

### With Scheduled & Deadline

Combine them for flexible scheduling: give the task both a Scheduled and a Deadline date, and turn on **Repeat task** from the Scheduled picker.

When `temporal-property` is Scheduled, Scheduled is the primary date advanced, and Deadline is also rescheduled when present. If `temporal-property` is not set, every date property present on the task (Scheduled and/or Deadline) is advanced ([commands.cljs:231-246](../../../../../src/main/frontend/worker/commands.cljs#L231-L246)).

While a task is not Done or Canceled, today's journal lists it in **Scheduled and Deadline** whenever its date falls within the next 7 days ([property.cljs:859-871](../../../../../src/main/frontend/worker/handler/property.cljs#L859-L871)).

### With Task Properties

Add priorities and other properties as usual, for example `/Priority High`. Repeating only changes the date properties and the completion property.

### With Property History

Property history is enabled per property, not per task. To track changes to a property across all tasks that use it, turn on **Enable property history** on the property itself. It is a property-level checkbox (`:view-context :property`), not a block-level property ([property.cljs:635-640](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L635-L640)). Status and Priority have it on by default ([property.cljs:321](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L321), [:339](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L339)), so each Done → Todo cycle is recorded ([commands.cljs:26-31](../../../../../src/main/frontend/worker/commands.cljs#L26-L31)). See [Stop Task Time Tracking](disable-task-time-tracking.md).

## Troubleshooting

### Task Not Repeating

**Problem:** A completed task doesn't reschedule.

**Check:**

1. **Repeat task** is checked ([commands.cljs:20-21](../../../../../src/main/frontend/worker/commands.cljs#L20-L21)).
2. **Every** is a number greater than 0. Zero or less produces no next date ([commands.cljs:168-171](../../../../../src/main/frontend/worker/commands.cljs#L168-L171)).
3. The unit is one of Minute, Hour, Day, Week, Month or Year.
4. **Next date** is set to the behavior you want.
5. The task has a value for Scheduled, Deadline, or the date property you enabled repeating from. Without one, Status only flips back to Todo.
6. Status was actually changed to Done, or the **When** property to its checked value.
7. The change was not an undo, and did not arrive through sync ([pipeline.cljs:515-518](../../../../../src/main/frontend/worker/pipeline.cljs#L515-L518)).

### Wrong Repeat Date

**Problem:** The task reschedules to an unexpected date.

**Solution:**

- Check which date property repeating was enabled from (`repeat/temporal-property`). If it is unset, whichever of Scheduled and Deadline the task has is advanced.
- Check that **Next date** matches the timing rule you want.
- Check the frequency and unit combination.
- Make sure the date property has a value on the task.
- Note: `double-plus` moves to a future date, skipping past occurrences. `plus` can land in the past if the task is overdue. `dotted-plus` counts from the time you completed the task.
- Month steps cap the day at the month's length and carry the capped day forward. See [Monthly Bill Payment](#monthly-bill-payment).

### Can't Pause Repeat

**Problem:** The task keeps rescheduling.

**Solution:**

- Uncheck **Repeat task** in the date picker.

## Best Practices

### 1. Use Appropriate Units

Good: natural intervals

- Daily: use Day
- Weekly: use Week
- Monthly: use Month

Avoid awkward conversions:

- Monthly: don't use 4 Week, which drifts from calendar months

### 2. Choose the Right Temporal Property

- **Scheduled**, for tasks with a start date. Example: **Begin monthly review**, with Scheduled 2026-01-15 and repeating enabled from the Scheduled picker.
- **Deadline**, for tasks with a due date. Example: **Submit report**, with Deadline 2026-01-28 and repeating enabled from the Deadline picker.

### 3. Organize by Frequency

Group repeating tasks under parent blocks:

- **Daily Habits**
  - Exercise (Todo, repeats every 1 Day)
  - Journal (Todo, repeats every 1 Day)
- **Weekly Reviews**
  - Team standup (Todo, repeats every 1 Week)
  - Personal review (Todo, repeats every 1 Week)
- **Monthly Tasks**
  - Pay bills (Todo, repeats every 1 Month)
  - Review goals (Todo, repeats every 1 Month)

### 4. Use Queries for Overview

Create a master view of all repeats. Add a parent block **All Repeating Tasks** with an `/Advanced Query` child:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property.repeat/repeated? true]]
 :collapsed? false}
```

## See Also

- [Todo System Basics](../../tutorials/task-management/todo-basics.md)
- [Stop Task Time Tracking](disable-task-time-tracking.md)
- [Properties Reference](../../reference/data/properties-reference.md)
