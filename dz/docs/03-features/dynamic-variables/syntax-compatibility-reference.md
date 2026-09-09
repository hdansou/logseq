# Dynamic Variables & Date Syntax: Cross-System Compatibility Reference

> **Last revised:** 2026-05-28

## Overview

Logseq has **four separate contexts** where date expressions or variables can be used, each backed by a different parser. This document provides an exhaustive compatibility matrix and documents known pitfalls.

| #   | Context                   | Example Syntax          | Parser                                     | Source File                                  |
| --- | ------------------------- | ----------------------- | ------------------------------------------ | -------------------------------------------- |
| 1   | `<% %>` Dynamic Variables | `<% in 7 days %>`       | chrono-node 2.9.0 NLP                      | `src/main/frontend/template.cljs`            |
| 2   | Query DSL `between`       | `(between -7d +7d)`     | Custom `->journal-day-int` / `->timestamp` | `src/main/frontend/db/query_dsl.cljs`        |
| 3   | Advanced Query `:inputs`  | `:inputs [:today :+7d]` | `resolve-keyword-input` multimethod        | `deps/db/src/logseq/db/frontend/inputs.cljs` |
| 4   | User Macros `{{name}}`    | `{{macro $1 $2}}`       | mldoc AST parser (no date logic)           | `src/main/frontend/components/block.cljs`    |

---

## Known Pitfall: `<% today +7d %>` Returns Today (Not +7 Days)

### The Bug

`<% today +7d %>` **silently returns today's date**, not a date 7 days in the future.

### Root Cause

The `+Nd` shorthand (e.g., `+7d`, `-3d`, `+1w`) is a **Logseq-internal notation** used only in the query DSL `between` clause and advanced query `:inputs`. It is **not recognized by chrono-node**, the NLP library that powers `<% %>` variable resolution.

When chrono-node encounters `"today +7d"`, it:

1. Matches `"today +7"` as the parseable expression
2. Interprets `+7` as a **UTC+0700 timezone offset** (not date arithmetic)
3. Silently ignores the trailing `d`
4. Returns today's date adjusted to UTC+7

```
chrono.parse("today +7d")
→ matched text: "today +7"
→ knownValues: { day: 9, month: 4, year: 2026, timezoneOffset: 420 }
→ result: today's date (WRONG — user expected +7 days)
```

### Workaround

Use chrono-node's natural language syntax instead:

| Desired Offset | Broken Syntax         | Working Syntax      |
| -------------- | --------------------- | ------------------- |
| +7 days        | ~~`<% today +7d %>`~~ | `<% in 7 days %>`   |
| -3 days        | ~~`<% today -3d %>`~~ | `<% 3 days ago %>`  |
| +1 week        | ~~`<% today +1w %>`~~ | `<% in 1 week %>`   |
| +2 months      | ~~`<% today +2m %>`~~ | `<% in 2 months %>` |

---

## System 1: `<% %>` Dynamic Variables

Resolved by `frontend.template/resolve-dynamic-template!` (called only from `query_dsl.cljs` — 3 call sites).

### Built-in Keywords (exact match, case-insensitive)

| Expression           | Output             | Notes                             |
| -------------------- | ------------------ | --------------------------------- |
| `<% today %>`        | `[[journal page]]` | Today's journal page reference    |
| `<% yesterday %>`    | `[[journal page]]` | Yesterday's journal               |
| `<% tomorrow %>`     | `[[journal page]]` | Tomorrow's journal                |
| `<% time %>`         | `13:45`            | Current time (24h, locale-aware)  |
| `<% current page %>` | `[[Page Name]]`    | Current page; falls back to today |

### chrono-node NLP Expressions (fallback for non-keyword input)

#### Relative Days ✅

| Expression              | Effect   |
| ----------------------- | -------- |
| `<% in 1 day %>`        | +1 day   |
| `<% in 7 days %>`       | +7 days  |
| `<% in 30 days %>`      | +30 days |
| `<% 1 day ago %>`       | -1 day   |
| `<% 7 days ago %>`      | -7 days  |
| `<% 7 days from now %>` | +7 days  |

#### Relative Weeks ✅

| Expression               | Effect   |
| ------------------------ | -------- |
| `<% in 1 week %>`        | +7 days  |
| `<% in 2 weeks %>`       | +14 days |
| `<% a week ago %>`       | -7 days  |
| `<% a week from now %>`  | +7 days  |
| `<% 2 weeks from now %>` | +14 days |

#### Relative Months ✅

| Expression               | Effect    |
| ------------------------ | --------- |
| `<% in 1 month %>`       | +1 month  |
| `<% in 6 months %>`      | +6 months |
| `<% 2 months ago %>`     | -2 months |
| `<% a month from now %>` | +1 month  |

#### Relative Years ✅

| Expression         | Effect   |
| ------------------ | -------- |
| `<% in 1 year %>`  | +1 year  |
| `<% in 2 years %>` | +2 years |
| `<% 1 year ago %>` | -1 year  |

#### Named Days ✅

| Expression                                      | Effect                      |
| ----------------------------------------------- | --------------------------- |
| `<% next monday %>` through `<% next sunday %>` | Next occurrence of that day |
| `<% last monday %>` through `<% last sunday %>` | Previous occurrence         |
| `<% this monday %>` through `<% this sunday %>` | This week's occurrence      |

#### Named Periods ✅

| Expression                              | Effect   |
| --------------------------------------- | -------- |
| `<% next week %>` / `<% last week %>`   | ±7 days  |
| `<% next month %>` / `<% last month %>` | ±1 month |
| `<% next year %>` / `<% last year %>`   | ±1 year  |

#### Specific Dates ✅

| Expression                         | Effect                       |
| ---------------------------------- | ---------------------------- |
| `<% January 1 %>`                  | Jan 1 (current or next year) |
| `<% March 15, 2027 %>`             | Specific date                |
| `<% Jan 1st %>` / `<% Dec 25th %>` | Ordinal format               |

#### Broken Patterns ❌

These look valid but produce wrong results or no results:

| Expression                | Actual Behavior              | Reason                      |
| ------------------------- | ---------------------------- | --------------------------- |
| `<% today +7d %>`         | Returns **today**            | `+7` = timezone offset      |
| `<% today -3d %>`         | Returns **today**            | `-3` = timezone offset      |
| `<% today +1w %>`         | Returns **today**            | `+1` = timezone offset      |
| `<% +7d %>`               | Returns raw text (unchanged) | chrono can't parse at all   |
| `<% -3d %>`               | Returns raw text (unchanged) | chrono can't parse at all   |
| `<% 1 week from today %>` | Returns **today**            | chrono matches only "today" |
| `<% 3 days from today %>` | Returns **today**            | chrono matches only "today" |
| `<% next january %>`      | Ignores "next"               | chrono only sees "january"  |

---

## System 2: Query DSL `between` Clause

Used in simple queries: `{{query (between -7d +7d)}}` or `/Query` command.

### `->journal-day-int` (returns YYYYMMDD integer, for journal date ranges)

| Expression         | Meaning                         |
| ------------------ | ------------------------------- |
| `today`            | Today's journal day             |
| `yesterday`        | Yesterday's journal day         |
| `tomorrow`         | Tomorrow's journal day          |
| `[[Journal Page]]` | Specific journal page reference |
| `+Nd` / `-Nd`      | N days from/before today        |
| `+Nw` / `-Nw`      | N weeks from/before today       |
| `+Nm` / `-Nm`      | N months from/before today      |
| `+Ny` / `-Ny`      | N years from/before today       |

### `->timestamp` (returns epoch ms, for `created-at`/`updated-at` ranges)

All of the above, plus:

| Expression    | Meaning                           |
| ------------- | --------------------------------- |
| `now`         | Current timestamp in milliseconds |
| `+Nh` / `-Nh` | N hours from/before now           |
| `+Nn` / `-Nn` | N minutes from/before now         |

### Examples

```
(between -7d +7d)                    ;; journals in last/next 7 days
(between created-at -1d today)       ;; blocks created since yesterday
(between updated-at -2h now)         ;; blocks updated in last 2 hours
```

---

## System 3: Advanced Query `:inputs` Keywords

Used in `:inputs` vectors of advanced (datalog) queries.

### Basic Keywords

| Keyword          | Returns          | Type        |
| ---------------- | ---------------- | ----------- |
| `:today`         | YYYYMMDD int     | Journal day |
| `:yesterday`     | YYYYMMDD int     | Journal day |
| `:tomorrow`      | YYYYMMDD int     | Journal day |
| `:current-page`  | Page name string | Context     |
| `:query-page`    | Page name string | Context     |
| `:current-block` | db/id int        | Context     |
| `:parent-block`  | db/id int        | Context     |
| `:right-now-ms`  | Epoch ms         | Timestamp   |

### Relative Date (returns YYYYMMDD integer)

| Pattern         | Example         | Effect                   |
| --------------- | --------------- | ------------------------ |
| `:+Nd` / `:-Nd` | `:+7d` / `:-3d` | Days from/before today   |
| `:+Nw` / `:-Nw` | `:+2w` / `:-1w` | Weeks from/before today  |
| `:+Nm` / `:-Nm` | `:+1m` / `:-6m` | Months from/before today |
| `:+Ny` / `:-Ny` | `:+1y` / `:-1y` | Years from/before today  |

### Relative Date + Time (returns epoch ms)

| Pattern               | Example          | Effect                            |
| --------------------- | ---------------- | --------------------------------- |
| `:+Nd-ms` / `:-Nd-ms` | `:+7d-ms`        | End/start of day N days away      |
| `:+Nd-start`          | `:+7d-start`     | Start of day (00:00:00.000)       |
| `:+Nd-end`            | `:+7d-end`       | End of day (23:59:59.999)         |
| `:+Nd-HHMM`           | `:+7d-1430`      | Specific time (14:30) on that day |
| `:+Nd-HHMMSS`         | `:+7d-143000`    | Specific time with seconds        |
| `:+Nd-HHMMSSmmm`      | `:+7d-143000000` | Specific time with milliseconds   |

### Today Time Variants

| Keyword              | Returns                         |
| -------------------- | ------------------------------- |
| `:start-of-today-ms` | Epoch ms for today 00:00:00.000 |
| `:end-of-today-ms`   | Epoch ms for today 23:59:59.999 |
| `:today-start`       | Same as `:start-of-today-ms`    |
| `:today-end`         | Same as `:end-of-today-ms`      |
| `:today-HHMM`        | Epoch ms for today at HH:MM     |

### Deprecated Format (still functional)

| Old Syntax      | New Equivalent  |
| --------------- | --------------- |
| `:1d` / `:7d`   | `:-1d` / `:-7d` |
| `:7d-before`    | `:-7d`          |
| `:7d-after`     | `:+7d`          |
| `:7d-before-ms` | `:-7d-ms`       |
| `:7d-after-ms`  | `:+7d-ms`       |

---

## System 4: User Macros `{{name args}}`

Macros defined in `config.edn` under `:macros {}` have **no date variable support**.

| Feature                             | Supported?      |
| ----------------------------------- | --------------- |
| `$1`, `$2` positional arguments     | ✅              |
| `<% today %>` in macro body         | ❌ Not resolved |
| Date arithmetic                     | ❌              |
| Page references (rendered by mldoc) | ✅              |

---

## Cross-System Compatibility Matrix

| Feature                          |    `<% %>`    |  DSL `between`   |      Adv `:inputs`       | Macros |
| -------------------------------- | :-----------: | :--------------: | :----------------------: | :----: |
| today / yesterday / tomorrow     |      ✅       |        ✅        |            ✅            |   ❌   |
| current time                     |      ✅       |        ❌        |            ❌            |   ❌   |
| current page                     |      ✅       |        ❌        |            ✅            |   ❌   |
| query page                       |      ❌       |        ❌        |            ✅            |   ❌   |
| current / parent block           |      ❌       |        ❌        |            ✅            |   ❌   |
| now (ms)                         |      ❌       |        ✅        |            ✅            |   ❌   |
| ±N days / weeks / months / years |   ✅ (NLP)    |    ✅ (`+Nd`)    |       ✅ (`:+Nd`)        |   ❌   |
| ±N hours / minutes               |      ❌       | ✅ (`+Nh`/`+Nn`) |            ❌            |   ❌   |
| named days (next monday)         |      ✅       |        ❌        |            ❌            |   ❌   |
| specific dates (Jan 15)          |      ✅       |  ✅ (page ref)   |            ❌            |   ❌   |
| time-of-day precision            |      ❌       |        ❌        |     ✅ (`:+Nd-HHMM`)     |   ❌   |
| start/end of day                 |      ❌       |        ❌        | ✅ (`:+Nd-start`/`-end`) |   ❌   |
| `+7d` shorthand                  | ⚠️ **BROKEN** |        ✅        |            ✅            |   ❌   |

---

## Plugin API Integration

| API Method                            | Resolves `<% %>`? | Notes                                                               |
| ------------------------------------- | :---------------: | ------------------------------------------------------------------- |
| `logseq.DB.q(dslQuery)`               |        ✅         | Goes through `parse-query`                                          |
| `logseq.Editor.insertTemplate()`      |  ⚠️ **Partial**   | Resolves 5 basic keywords only (no chrono-node NLP); see note below |
| `logseq.App.onMacroRendererSlotted()` |        ❌         | Must implement own date logic                                       |
| `logseq.DB.datascriptQuery()`         |        ❌         | No variable resolution                                              |

`logseq.DB.q()` fully resolves `<% %>` (including chrono-node NLP fallback) because it passes through `query-dsl/parse-query` → `frontend.template/resolve-dynamic-template!`.

`logseq.Editor.insertTemplate()` resolves **only the five built-in keywords** (`today`, `yesterday`, `tomorrow`, `time`, `current page`) via `logseq.outliner.template/resolve-dynamic-template-blocks` (`deps/outliner/src/logseq/outliner/template.cljs`). chrono-node NLP expressions such as `<% in 7 days %>` are **not** resolved and are left as-is in the inserted blocks.

Plugins needing full chrono-node date arithmetic must call `logseq.DB.q()` or implement their own resolution.

---

## Source Code Reference

| File                                         | Purpose                               | Key Functions                                                 |
| -------------------------------------------- | ------------------------------------- | ------------------------------------------------------------- |
| `src/main/frontend/template.cljs`            | `<% %>` resolution                    | `resolve-dynamic-template!`, `variable-rules`, `template-re`  |
| `src/main/frontend/date.cljs`                | chrono-node wrapper, date utilities   | `nld-parse`, `today`, `yesterday`, `tomorrow`, `journal-name` |
| `src/main/frontend/db/query_dsl.cljs`        | DSL query parsing + `between`         | `->journal-day-int`, `->timestamp`, `parse-query`             |
| `deps/db/src/logseq/db/frontend/inputs.cljs` | Advanced query `:inputs`              | `resolve-keyword-input`, `resolve-input`                      |
| `src/main/frontend/db/query_react.cljs`      | Query execution with input resolution | `resolve-input` (wrapper), `react-query`                      |
| `src/main/frontend/components/block.cljs`    | Macro rendering                       | `macro-cp`, `macro-else-cp`, `render-macro`                   |

---

## Findings: New Capabilities

### Separate Template-Insertion Resolution Path

Template block insertion uses a **separate, lighter resolver** from the query DSL resolver:

| Path                                              | Function                          | Resolves                                        |
| ------------------------------------------------- | --------------------------------- | ----------------------------------------------- |
| `deps/outliner/src/logseq/outliner/template.cljs` | `resolve-dynamic-template-blocks` | 5 basic keywords only                           |
| `src/main/frontend/template.cljs`                 | `resolve-dynamic-template!`       | 5 basic keywords **+ chrono-node NLP fallback** |

`resolve-dynamic-template-blocks` is called from:

- `deps/outliner/src/logseq/outliner/op.cljs` (template insertion)
- `src/main/frontend/worker/pipeline.cljs` (worker-side template processing)

Unlike `resolve-dynamic-template!`, it does **not** fall back to chrono-node for unrecognised expressions — they are returned verbatim.

---

## Related Documents

- [Developer Guide](developer-guide.md) — Architecture deep-dive and code investigation
- [User Guide](user-guide.md) — End-user documentation for dynamic variables
