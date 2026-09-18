# Investigation: Scheduled and Deadline Tasks in Logseq DB Graphs

**Purpose:** Comprehensive code investigation of how scheduled and deadline tasks work in Logseq DB-based graphs, including query syntax and examples.

**Version:** 1.1  
**Last Updated:** 2026-05-30

> **Last revised:** 2026-05-30

**Graph Mode:** Database (DB-based graphs)  
**Platform:** Cross-platform (Electron, Web)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Context](#context)
3. [Technical Deep Dive](#technical-deep-dive)
4. [Flow Diagrams](#flow-diagrams)
5. [Query Examples](#query-examples)
6. [Code Location Reference](#code-location-reference)
7. [Comparison: DB vs File-Based](#comparison-db-vs-file-based)
8. [Best Practices](#best-practices)
9. [Appendix](#appendix)

---

## Executive Summary

### Key Findings

1. **Property-Based System**: DB graphs use `:logseq.property/scheduled` and `:logseq.property/deadline` properties (not org-mode timestamps)
2. **Datetime Storage**: Values are stored as **millisecond timestamps** (epoch), not integer dates
3. **Query Syntax**: Use `(between scheduled today tomorrow)` and `(between deadline today +7d)` without quotes
4. **UI Integration**: Set via slash commands (`Scheduled`/`Deadline`), property picker, or deadline shortcut (`p d`)
5. **Task Status Format**: Use title-case (`Todo`, `Doing`, `Done`) without quotes

### Working Query Example

**Find scheduled tasks in todo status to be done during today:**

**Simple Query (DSL) - CORRECT:**

```clojure
(and
  (task Todo)
  (between scheduled today tomorrow))
```

**Why `(property scheduled today)` DOES NOT WORK:**

- The `property` operator does **exact value matching**: `[?b :logseq.property/scheduled ?val]`
- Scheduled stores: `1763186400000` (milliseconds)
- `today` resolves to: `20251122` (integer date)
- These values are **completely different** and will never match!
- You MUST use `between` which converts date integers to millisecond timestamp ranges

**Why `(between scheduled today today)` DOES NOT WORK:**

- Both `today` keywords resolve to **the same timestamp** (midnight of today)
- `between` uses: `scheduled >= start AND scheduled < end`
- When start = end, this becomes: `scheduled >= X AND scheduled < X`
- This is **mathematically impossible** - no value can satisfy this!
- **Solution:** Use `(between scheduled today tomorrow)` - captures all of today

**Advanced Query (for millisecond-precision queries):**

```clojure
{:title "Scheduled Today"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         [?s :logseq.property/value "Todo"]
         [?b :logseq.property/scheduled ?t]
         [(>= ?t ?start)]
         [(<= ?t ?end)]]
 :inputs [:start-of-today-ms :end-of-today-ms]}
```

**CRITICAL - Why `property` Doesn't Work with Datetime Properties:**

- **`property` operator**: Does exact value matching `[?b :logseq.property/scheduled ?val]`
- **Datetime properties**: Store millisecond timestamps (e.g., `1763186400000`)
- **Date keywords**: Resolve to integer dates (e.g., `today` → `20251122`)
- **Result**: `1763186400000` ≠ `20251122` - values never match!
- **Solution**: ALWAYS use `between` for scheduled/deadline queries - it converts dates to timestamp ranges
- **Keywords like `:start-of-today-ms`**: Only work in advanced queries with `:inputs`, NOT in simple DSL

### Critical Differences from File-Based Graphs

| Aspect               | File-Based                   | DB-Based                                |
| -------------------- | ---------------------------- | --------------------------------------- |
| **Property Storage** | `:block/scheduled` (integer) | `:logseq.property/scheduled` (datetime) |
| **Value Format**     | `20240315` (YYYYMMDD)        | `1710489600000` (milliseconds)          |
| **Display**          | `SCHEDULED: <2024-03-15>`    | Property UI with date picker            |
| **Task Status**      | `TODO`, `DOING`, `DONE`      | `Todo`, `Doing`, `Done`                 |

---

## Context

## Findings: New Capabilities

- `inputs.cljs` now supports `:relative-date-time` inputs like `:+7d-ms`, `:+2w-start`, and explicit time suffixes such as `:today-1400`.
- DB query DSL property matching now routes through specialized scalar/ref/default-aware rules (`:scalar-property`, `:ref-property`, and `*-with-default`) instead of a single simple property rule.
- Task filtering in DB queries uses the status property via `ref-property-with-default`, so `(task ...)` is status-driven.

> **Removed:** `task` rule no longer requires `:block/tags :logseq.class/Task` in DB query DSL rules.
> **Removed:** `simple-query-property`/`private-simple-query-property` are no longer the main path for `(property key value)` DB matching in `query_dsl.cljs`.

### Feature Overview

Scheduled and deadline properties allow users to:

- **Scheduled** (`:logseq.property/scheduled`): Plan when to **start** a task
- **Deadline** (`:logseq.property/deadline`): Set when to **finish** a task

Both are **datetime properties** supporting:

- Date-only values (stored as midnight on that date)
- Date + time values (specific hour/minute)
- Query filtering by date ranges
- Visual indicators for overdue tasks

### User Interaction

**Setting Properties:**

1. **Keyboard Shortcuts**:

- `mod+p` opens property insertion
- `p d` inserts Deadline directly (`:editor/add-property-deadline`)

2. **Slash Commands**:

- `Scheduled` - Opens date picker (`:editor/set-scheduled`)
- `Deadline` - Opens date picker (`:editor/set-deadline`)

3. **Property UI**: Click property icon → Select date/time

**Visual Display:**

- Date-only: Shows as journal page link (e.g., `[[2024-03-15]]`)
- Date+time: Shows time badge (e.g., `14:30`)
- Overdue: Highlighted in red (for deadline or scheduled tasks)

---

## Technical Deep Dive

### 1. Property Definitions

**Location:** `deps/db/src/logseq/db/frontend/property.cljs:323-338`

```clojure
:logseq.property/deadline
{:title "Deadline"
 :schema {:type :datetime
          :public? true
          :ui-position :block-below}
 :properties {:logseq.property/hide-empty-value true
              :logseq.property/description "Use it to finish something at a specific date(time)."}
 :queryable? true}

:logseq.property/scheduled
{:title "Scheduled"
 :schema {:type :datetime
          :public? true
          :ui-position :block-below}
 :properties {:logseq.property/hide-empty-value true
              :logseq.property/description "Use it to plan something to start at a specific date(time)."}
 :queryable? true}
```

**Key Attributes:**

- **Type**: `:datetime` (millisecond timestamps)
- **Public**: `true` (user-visible property)
- **Queryable**: `true` (can be used in queries)
- **UI Position**: `:block-below` (displays below block content)

---

### 2. Storage Format

**Datetime Values:**

- Stored as **epoch milliseconds** (e.g., `1710489600000`)
- Date-only values: Set to midnight (00:00:00.000) in local timezone
- Date+time values: Precise millisecond timestamp

**Example Storage:**

```clojure
;; Block entity with scheduled property
{:db/id 12345
 :block/uuid #uuid "..."
 :block/content "TODO Review project proposal"
 :logseq.property/scheduled 1710489600000  ; 2024-03-15 00:00:00
 :logseq.property/status #:db{:id 789, :ident :logseq.property/status.todo}}
```

**Conversion:**

- User selects: March 15, 2024
- System stores: `1710489600000` (midnight local time)
- Display shows: `[[2024-03-15]]` (journal page link)

---

### 3. Query System Integration

#### Query DSL: `between` Operator

**Location:** `src/main/frontend/db/query_dsl.cljs:261-275`

```clojure
(defn- db-based-build-between-three-arg
  [e]
  (when-let [k (get-timestamp-property e)]
    (let [start (->timestamp (nth e 2))
          end (->timestamp (nth e 3))]
      (when (and start end)
        (let [[start end] (sort [start end])
              sym '?v]
          {:query [['?b k sym]
                   [(list '>= sym start)]
                   [(list '< sym end)]]})))))
```

**How it works:**

1. Accepts property name + start/end timestamps
2. Converts symbolic dates (`today`, `+7d`) to millisecond timestamps
3. Generates DataScript clauses with range comparison
4. Uses `>=` for start, `<` for end (inclusive start, exclusive end)

#### Query DSL: `property` Operator

**Location:** `src/main/frontend/db/query_dsl.cljs` (`build-property-two-arg`)

```clojure
(defn- build-property-two-arg
  [e {:keys [private-property?]}]
  (let [k (->db-keyword-property (nth e 1))
        v (nth e 2)
        v' (->db-property-value k v)
        property (when (qualified-keyword? k)
                   (d/entity *current-db* k))
        ref-type? (= :db.type/ref (:db/valueType property))
        rule (if private-property?
               (if ref-type? :private-ref-property :private-scalar-property)
               (if ref-type? :ref-property :scalar-property))]
    {:query (list (symbol (name rule)) '?b k v')
     :rules [rule]}))
```

**For datetime properties (scheduled/deadline):**

- **CANNOT use `property` operator**: It does exact value matching, but datetime properties store milliseconds while date keywords resolve to integers - they never match!
- **MUST use `between` operator**: `(between scheduled today tomorrow)` for today, `(between scheduled today +7d)` for ranges
- **How `between` works**: Recognizes timestamp properties and converts date integers (like `20251122`) to millisecond ranges
- **Millisecond timestamps**: Only available in advanced queries via `:inputs` (e.g., `:start-of-today-ms`)

---

### 4. Query Execution Flow

#### Example: Get Scheduled/Deadline Tasks for Date Range

**Location:** `src/main/frontend/db/async.cljs:230-259`

```clojure
(defn <get-date-scheduled-or-deadlines
  [journal-title]
  (when-let [date (date/journal-title->int journal-title)]
    (let [future-days (state/get-scheduled-future-days)
          current-day (tf/parse yyyyMMdd-formatter (str date))
          future-date (t/plus current-day (t/days future-days))
          future-day (some->> future-date
                              (tf/unparse yyyyMMdd-formatter)
                              (parse-long))
          start-time (date/journal-day->utc-ms date)
          future-time (tc/to-long future-date)]
      (when-let [repo (and future-day (state/get-current-repo))]
        (p/let [result
                (<q repo {}
                    '[:find [(pull ?block ?block-attrs) ...]
                      :in $ ?start-time ?end-time ?block-attrs
                      :where
                      (or [?block :logseq.property/scheduled ?n]
                          [?block :logseq.property/deadline ?n])
                      [(>= ?n ?start-time)]
                      [(<= ?n ?end-time)]
                      [?block :logseq.property/status ?status]
                      [?status :db/ident ?status-ident]
                      [(not= ?status-ident :logseq.property/status.done)]
                      [(not= ?status-ident :logseq.property/status.canceled)]]
                    start-time
                    future-time
                    '[*])]
          (->> result
               db-model/sort-by-order-recursive
               db-utils/group-by-page))))))
```

**Query Logic:**

1. Find blocks with `:logseq.property/scheduled` OR `:logseq.property/deadline`
2. Filter by date range: `?n >= start-time AND ?n <= end-time`
3. Exclude done/canceled tasks (check status property)
4. Pull all block attributes
5. Sort and group results by page

---

### 5. Input Resolution (Date Keywords)

**Location:** `deps/db/src/logseq/db/frontend/inputs.cljs:103-123`

```clojure
(defmethod resolve-keyword-input :today [_ _ _]
  (date-time-util/date->int (t/today)))

(defmethod resolve-keyword-input :yesterday [_ _ _]
  (date-time-util/date->int (t/minus (t/today) (t/days 1))))

(defmethod resolve-keyword-input :tomorrow [_ _ _]
  (date-time-util/date->int (t/plus (t/today) (t/days 1))))

(defmethod resolve-keyword-input :right-now-ms [_ _ _]
  (common-util/time-ms))

(defmethod resolve-keyword-input :today-time [_db input _opts]
  (let [[hh mm ss ms] (case input
                        :start-of-today-ms [0 0 0 0]
                        :end-of-today-ms [23 59 59 999]
                        (get-ts-units nil (subs (name input) 6)))]
    (date-at-local-ms (t/today) hh mm ss ms)))

(defmethod resolve-keyword-input :relative-date [_ input _]
  (let [relative-to (get-relative-date input)
        [_ offset-direction offset offset-unit] (re-find #"^([+-])(\d+)([dwmy])$" (name input))
        offset-date (get-offset-date relative-to offset-direction offset offset-unit)]
    (date-time-util/date->int offset-date)))
```

**Supported Date Inputs:**

| Keyword              | Resolves To                     | Type           |
| -------------------- | ------------------------------- | -------------- |
| `today`              | Current date (integer YYYYMMDD) | Date           |
| `yesterday`          | Yesterday's date                | Date           |
| `tomorrow`           | Tomorrow's date                 | Date           |
| `:start-of-today-ms` | Today at 00:00:00.000           | Timestamp (ms) |
| `:end-of-today-ms`   | Today at 23:59:59.999           | Timestamp (ms) |
| `:right-now-ms`      | Current timestamp               | Timestamp (ms) |
| `+7d`                | 7 days from now                 | Date           |
| `-1w`                | 1 week ago                      | Date           |
| `:+7d-ms`            | 7 days from now (timestamp)     | Timestamp (ms) |

---

### 6. Task Status Integration

**Location:** `src/main/frontend/db/query_dsl.cljs:365-373`

```clojure
(defn- build-task
  [e]
  (let [markers (if (coll? (first (rest e)))
                  (first (rest e))
                  (rest e))]
    (when (seq markers)
      (let [markers' (set (map (comp common-util/capitalize-all name) markers))]
        {:query (list 'task '?b (set markers'))
         :rules [:task]}))))
```

**Task Status Transformation:**

- DB graphs: `common-util/capitalize-all` → `Todo`, `Doing`, `Done`
- File graphs: `string/upper-case` → `TODO`, `DOING`, `DONE`

**Query Rule:**

**Location:** `deps/db/src/logseq/db/frontend/rules.cljc` (`:task`)

```clojure
:task
'[(task ?b ?statuses)
  (ref-property-with-default ?b :logseq.property/status ?val)
  [(contains? ?statuses ?val)]]
```

**How it works:**

1. Resolve block status through `ref-property-with-default`
2. Include both explicit and default status values
3. Check if resolved status is in the requested marker set

---

## Flow Diagrams

### Setting Scheduled Property

```text
User Action: /Scheduled (or Add Property) → Select Date (March 15, 2024)
   [0ms]
    ├─→ UI: Open date picker
    │    └─→ User selects: 2024-03-15
    │
    ├─→ Command Handler (src/main/frontend/commands.cljs)
    │    └─→ :editor/set-scheduled → [:editor/new-property {:property-key "Scheduled"}]
    │
    ├─→ Transaction Builder (deps/db/src/logseq/db/frontend/property/build.cljs:68-94)
    │    ├─→ Create journal page if needed
    │    └─→ Build property value: [:block/uuid journal-uuid] or timestamp
    │
    └─→ DataScript Transact (frontend.worker.db-worker)
         └─→ [:db/add block-id :logseq.property/scheduled 1710489600000]

Result: Block now has scheduled property with datetime value
Display: [[2024-03-15]] shown in property UI
```

---

### Query Execution Timeline

```text
Query: (and (task Todo) (between scheduled today tomorrow))
  [0ms]
    │
    ├─→ Parse Query String (src/main/frontend/db/query_dsl.cljs)
    │    ├─→ Resolve timestamp bounds for `today` and `tomorrow`
    │    ├─→ Build task clause: (task ?b #{"Todo"})
    │    └─→ Build range clauses: [?b :logseq.property/scheduled ?v] [(>= ?v start)] [(< ?v end)]
    │
    ├─→ Generate DataScript Query
    │    {:query [:find [(pull ?b [*]) ...]
    │             :in $ ?start ?end
    │             :where
    │             (task ?b #{"Todo"})
    │             [?b :logseq.property/scheduled ?v]
    │             [(>= ?v ?start)]
    │             [(< ?v ?end)]
    │     :inputs [start-ms end-ms]}
    │
    ├─→ Execute Query (DataScript)
    │    └─→ Match blocks where status matches Todo and scheduled is within today's range
    │
    └─→ Return Results
         └─→ List of matching blocks

Display: Results rendered in query block
```

> **Removed:** The earlier timeline that used `(property scheduled today)` as a working path was stale.

---

## Query Examples

### Example 1: Scheduled Tasks for Today (Todo Status)

**Simple Query (CORRECT):**

```clojure
(and
  (task Todo)
  (between scheduled today tomorrow))
```

**What it does:**

- Finds blocks whose status matches `Todo`
- Filters to `Todo` status
- Uses `between scheduled today tomorrow` to match **all times during today**
- How it works: `today` = midnight start of today, `tomorrow` = midnight start of tomorrow
- Creates range: `scheduled >= today-midnight AND scheduled < tomorrow-midnight`
- This matches everything from 00:00:00 through 23:59:59.999 today

**Use Case:** Daily planning - see what you planned to start today

**❌ WRONG - These Do NOT Work:**

```clojure
(and (task Todo) (property scheduled today))
```

**Why**: `property` does exact matching. Scheduled stores milliseconds (`1763186400000`), `today` is an integer (`20251122`). They never match!

```clojure
(and (task Todo) (between scheduled today today))
```

**Why**: Both `today` keywords resolve to the same timestamp (midnight). Creates impossible condition: `value >= X AND value < X`. Nothing matches!

---

### Example 2: Deadlines in Next 7 Days

**Query:**

```clojure
(and
  (task Todo Doing)
  (between deadline today +7d))
```

**What it does:**

- Finds `Todo` or `Doing` tasks
- Deadline between today and 7 days from now
- Useful for weekly planning

**Use Case:** Weekly review - see upcoming deadlines

---

### Example 3: Overdue Tasks

**Query:**

```clojure
(and
  (task Todo Doing)
  (property deadline)
  (between deadline -365d yesterday))
```

**What it does:**

- Tasks not yet done (`Todo` or `Doing`)
- Has a deadline property
- Deadline was in the past (up to 1 year ago, before yesterday)

**Use Case:** Task cleanup - identify overdue items

---

### Example 4: Today's Scheduled Tasks (Any Status)

**Query:**

```clojure
(and
  (task Todo Doing Done)
  (between scheduled today tomorrow))
```

**What it does:**

- All tasks scheduled for today
- Includes completed tasks to review what was done

**Use Case:** End-of-day review

---

### Example 5: This Week's Schedule

**Query:**

```clojure
(and
  (task Todo Doing)
  (between scheduled today +7d))
```

**What it does:**

- Active tasks scheduled within next 7 days
- Excludes completed tasks

**Use Case:** Weekly planning board

---

### Example 6: Urgent Tasks (Deadline Today)

**Query:**

```clojure
(and
  (task Todo Doing)
  (between deadline today tomorrow))
```

**What it does:**

- Tasks due today
- Only incomplete tasks
- `between deadline today tomorrow` matches all times during today

**Use Case:** "What must I finish today?"

---

### Example 7: Advanced - Scheduled Today OR Deadline Today

**Query:**

```clojure
(or
  (and (task Todo Doing) (between scheduled today tomorrow))
  (and (task Todo Doing) (between deadline today tomorrow)))
```

**What it does:**

- Tasks scheduled to start today
- OR tasks due today
- Combines both conditions

**Use Case:** "What's on my plate today?"

---

### Example 8: Tasks with Both Scheduled AND Deadline

**Query:**

```clojure
(and
  (task Todo Doing)
  (property scheduled)
  (property deadline))
```

**What it does:**

- Tasks that have both properties set
- Useful for finding well-planned tasks
- Note: `(property name)` without a value checks for **existence**, which works fine

**Use Case:** Review task planning completeness

---

## Code Location Reference

| File                                                 | Purpose              | Key Functions/Data                                                         | Lines            |
| ---------------------------------------------------- | -------------------- | -------------------------------------------------------------------------- | ---------------- |
| `deps/db/src/logseq/db/frontend/property.cljs`       | Property definitions | Built-in properties including scheduled/deadline                           | 340-353          |
| `src/main/frontend/db/async.cljs`                    | Query functions      | `<get-date-scheduled-or-deadlines` - fetch tasks by date                   | 228-257          |
| `src/main/frontend/db/query_dsl.cljs`                | Query DSL parser     | `db-based-build-between-three-arg`, `build-property-two-arg`, `build-task` | 254-373          |
| `deps/db/src/logseq/db/frontend/rules.cljc`          | DataScript rules     | `:between`, `:task`, property helper rules                                 | 97-304           |
| `deps/db/src/logseq/db/frontend/inputs.cljs`         | Input resolution     | Date/relative-date-time keyword conversion                                 | 71-140           |
| `src/main/frontend/commands.cljs`                    | Command handlers     | Slash command actions for scheduled/deadline                               | 261-271, 654-658 |
| `src/main/frontend/modules/shortcut/config.cljs`     | Shortcut bindings    | Deadline shortcut binding                                                  | 268-271          |
| `deps/db/src/logseq/db/frontend/property/build.cljs` | Property TX builder  | Build property value transactions                                          | 68-130           |
| `src/main/frontend/components/property/value.cljs`   | Property UI          | Display datetime values, overdue highlighting                              | 401-433          |

---

## Comparison: DB vs File-Based

### Storage Differences

| Aspect               | File-Based                | DB-Based                     |
| -------------------- | ------------------------- | ---------------------------- |
| **Property Keyword** | `:block/scheduled`        | `:logseq.property/scheduled` |
| **Value Type**       | Integer (YYYYMMDD)        | Datetime (milliseconds)      |
| **Example Value**    | `20240315`                | `1710489600000`              |
| **Time Support**     | No (date only)            | Yes (hour/minute/second)     |
| **Display**          | `SCHEDULED: <2024-03-15>` | Property UI + journal link   |

### Query Syntax Differences

| Query                  | File-Based                                                 | DB-Based                                               |
| ---------------------- | ---------------------------------------------------------- | ------------------------------------------------------ |
| **Today's scheduled**  | `{{query (and (task TODO) (property scheduled today))}}`   | `(and (task Todo) (between scheduled today tomorrow))` |
| **Next week deadline** | `{{query (and (task TODO) (between deadline today +7d))}}` | `(and (task Todo) (between deadline today +7d))`       |
| **Task status format** | `TODO`, `DOING`, `DONE`                                    | `Todo`, `Doing`, `Done`                                |
| **Wrapper syntax**     | `{{query ...}}` macro                                      | `/Query` command (no wrapper)                          |

### Data Structure Comparison

**File-Based Block:**

```clojure
{:block/scheduled 20240315          ; Integer date
 :block/deadline 20240320
 :block/marker "TODO"               ; String marker
 :block/content "TODO Task"}
```

**DB-Based Block:**

```clojure
{:logseq.property/scheduled 1710489600000  ; Millisecond timestamp
 :logseq.property/deadline 1710835200000
 :logseq.property/status #:db{:id 789, :ident :logseq.property/status.todo}  ; Ref to status entity
 :block/tags #{:logseq.class/Task}
 :block/content "Task"}
```

---

## Best Practices

### For Users

1. **Use Command Entry Points**: Slash commands are the most direct for both properties; shortcut `p d` is available for Deadline.
2. **Set Both Properties**: Use scheduled (start) + deadline (finish) for better planning
3. **Check Overdue Daily**: Run overdue query in your journal template
4. **Use Timestamp Inputs**: For precise day matching, use `:start-of-today-ms :end-of-today-ms`
5. **Leverage Date Ranges**: Use `between` for weekly/monthly views

### For Query Writers

1. **Use Correct Status Format**: `Todo` not `"Todo"` or `TODO`
2. **CRITICAL - Use `between` for Datetime Properties**: The `property` operator does NOT work with scheduled/deadline because it does exact value matching (milliseconds ≠ integer dates). ALWAYS use `(between scheduled today tomorrow)` for today's tasks or `(between scheduled today +7d)` for ranges. NEVER use `(between scheduled today today)` - it creates an impossible condition!
3. **Use Timestamp Keywords in Advanced Queries**: `:start-of-today-ms` only works in advanced queries with `:inputs`, not in simple DSL
4. **Combine with Task Filter**: Always filter by status to avoid completed tasks
5. **Test Date Resolution**: Check what `today` resolves to in your timezone
6. **Watch Query Syntax**: Common mistakes:
   - ❌ `(and(task Doing) ...)` - Missing space after `and`
   - ❌ `(Task Doing)` - Uppercase `Task` (should be lowercase `task`)
   - ❌ `(between scheduled today today)` - Impossible condition (start = end)
   - ✅ `(and (task Doing) (between scheduled today tomorrow))` - Correct!

### For Developers

1. **Always Check Graph Type**: Use `(config/db-based-graph? repo)` before building queries
2. **Handle Timezones**: Datetime values are in user's local timezone
3. **Validate Property Types**: Ensure `:datetime` type for scheduled/deadline
4. **Consider Migration**: Converting file → DB requires timestamp conversion
5. **Test Edge Cases**: Midnight boundaries, timezone changes, daylight saving

---

## Troubleshooting Common Query Issues

### Issue: Query Returns Tasks Scheduled in the Past

**Symptoms:**

- Task scheduled for Nov 15th appears in query for "today" (Nov 22nd)
- Query: `(and(Task Doing) (between scheduled today tomorrow))`

**Root Causes:**

1. **Syntax Error - Missing Space:**

   ```clojure
   ❌ (and(task Doing) ...)  ; Parser may fail or misinterpret
   ✅ (and (task Doing) ...)  ; Correct - space after 'and'
   ```

2. **Wrong Operator Name:**

   ```clojure
   ❌ (and (Task Doing) ...)  ; 'Task' is not a valid operator
   ✅ (and (task Doing) ...)  ; Use lowercase 'task'
   ```

3. **Wrong Date Range:**

   ```clojure
   ❌ (between scheduled today today)     ; Impossible: start = end, nothing matches!
   ✅ (between scheduled today tomorrow)  ; Matches all times during today
   ```

**How to Debug:**

1. **Test the timestamp conversion:**

   ```bash
   node -e "
   const scheduledMs = 1763186400000; // Your task's scheduled value
   const scheduledDate = new Date(scheduledMs);
   console.log('Scheduled:', scheduledDate.toLocaleDateString());
   const today = new Date('2025-11-22');
   console.log('Today:', today.toLocaleDateString());
   console.log('Is past?', scheduledMs < today.getTime());
   "
   ```

2. **Verify query syntax:**
   - Check for spaces after operators: `(and (task ...))` not `(and(task...))`
   - Use lowercase operators: `task`, `between`, `property`
   - Use title-case status: `Todo`, `Doing`, `Done`

3. **Test with advanced query:**

   ```clojure
   {:title "Debug Scheduled Tasks"
    :query [:find (pull ?b [*])
            :in $ ?start ?end
            :where
            [?b :block/tags :logseq.class/Task]
            [?b :logseq.property/scheduled ?t]
            [(>= ?t ?start)]
            [(< ?t ?end)]]
    :inputs [:start-of-today-ms :end-of-today-ms]}
   ```

### Issue: `(property scheduled today)` Returns No Results

**Why:** The `property` operator does **exact value matching**. Scheduled stores milliseconds (`1763186400000`), but `today` resolves to an integer date (`20251122`). These never match!

**Solution:** Use `between` with tomorrow as end:

```clojure
✅ (between scheduled today tomorrow)  ; Matches all of today
```

**Why not `(between scheduled today today)`?**

- Both resolve to same timestamp (midnight)
- Creates: `value >= X AND value < X` (impossible!)
- Nothing will ever match

### Issue: Query Returns Wrong Status Tasks

**Symptoms:** Query for `Todo` tasks returns `Doing` or `Done` tasks

**Cause:** Case sensitivity or missing filter

**Solution:**

```clojure
❌ (task todo)        ; Wrong case
❌ (task "Todo")      ; Don't use quotes
✅ (task Todo)        ; Correct - title case, no quotes
✅ (task Todo Doing)  ; Multiple statuses
```

---

## Appendix

### Glossary

- **Epoch Time**: Milliseconds since January 1, 1970 00:00:00 UTC
- **DataScript**: In-memory database used by Logseq for DB graphs
- **Query DSL**: Domain-Specific Language for Logseq queries (simple syntax)
- **Datalog**: Logic programming language used for advanced queries
- **Property Entity**: In DB graphs, property values can be entity references
- **Journal Page**: Daily note page (e.g., `[[2024-03-15]]`)

### Related Documentation

- [Query Syntax: DB vs File-Based](../03-features/query-system/syntax-db-vs-file.md)
- [Query System Overview](../03-features/query-system/overview.md)
- [Query Inputs Guide](../03-features/query-system/query-inputs.md)
- [Task Management System](../03-features/task-management/system.md)
- [Dynamic Variables](../03-features/dynamic-variables/developer-guide.md)

### Date Input Quick Reference

**Common Date Keywords:**

```clojure
today          ; Current date (integer)
yesterday      ; Yesterday (integer)
tomorrow       ; Tomorrow (integer)
+7d            ; 7 days from now
-1w            ; 1 week ago
-1m            ; 1 month ago
```

**Timestamp Keywords (Milliseconds):**

```clojure
:start-of-today-ms    ; Today 00:00:00.000
:end-of-today-ms      ; Today 23:59:59.999
:right-now-ms         ; Current timestamp
:today-1400           ; Today at 14:00
:+7d-ms               ; 7 days from now (timestamp)
:-1w-start            ; Start of day 1 week ago
```

### Example Queries for Copy-Paste

**Daily Journal Template:**

```markdown
## Tasks Scheduled Today

query:: (and (task Todo Doing) (between scheduled today tomorrow))

## Tasks Due Today

query:: (and (task Todo Doing) (between deadline today tomorrow))

## Overdue Tasks

query:: (and (task Todo Doing) (property deadline) (between deadline -365d yesterday))
```

**Weekly Review Template:**

```markdown
## This Week's Schedule

query:: (and (task Todo Doing) (between scheduled today +7d))

## Upcoming Deadlines (Next 7 Days)

query:: (and (task Todo Doing) (between deadline today +7d))

## Completed This Week

query:: (and (task Done) (between scheduled -7d today))
```

---

## Change History

| Version | Date       | Changes                                                                                                | Author         |
| ------- | ---------- | ------------------------------------------------------------------------------------------------------ | -------------- |
| 1.1     | 2026-05-30 | Validated against current `src/`, `deps/`, and `packages/`; corrected stale query/task/shortcut claims | Code Detective |
| 1.0     | 2025-01-15 | Initial investigation                                                                                  | Code Detective |

---

## License

This investigation is part of the Logseq project and follows the same license (AGPL-3.0).
