---
title: 'Dynamic Variables Reference'
description: "Complete reference for Logseq's dynamic template variables (< % syntax)"
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
logseqVersion: '0.11.x'
platforms: [desktop, mobile, web]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[Using Templates](../../how-to/content/templates.md)'
  explanation:
    - '[Template System](../../explanation/concepts/templates.md)'
---

# Dynamic Variables Reference

> **Last revised:** 2026-09-09

> Complete reference for Logseq's dynamic template variables, which provide runtime resolution of dates, page references, and contextual information.

## Overview

Dynamic variables use the `<% variable %>` syntax and are resolved at insertion time.

> **Important:** there are **two separate resolvers** with **different capabilities**. Which one runs depends on where the variable appears.
>
> | Where | Resolver | Built-ins | Natural-language dates |
> | ----- | -------- | --------- | ---------------------- |
> | Template application | `logseq.outliner.template/resolve-dynamic-template-blocks` (`deps/outliner/src/logseq/outliner/template.cljs:163-170`) | ✅ 5 built-ins | ❌ **not supported** |
> | Simple (DSL) query strings | `frontend.template/resolve-dynamic-template!` (`src/main/frontend/template.cljs:23-37`) | ✅ 5 built-ins | ✅ via chrono-node |
>
> `frontend.template/resolve-dynamic-template!` has exactly one caller: `frontend.db.query-dsl/pre-transform-query` (`src/main/frontend/db/query_dsl.cljs:17-20`). Natural-language expressions such as `<% next Monday %>` therefore work **only inside simple query strings** — not in templates, and not in advanced (`#+BEGIN_QUERY`) queries.

**Key Features:**

- Date-based variables (`today`, `yesterday`, `tomorrow`)
- Context variables (`current page`)
- Natural language date parsing ("next Monday", "in 3 days") — simple queries only
- The same five built-ins in both resolvers

---

## Quick Reference

| Variable             | Description            | Example Output   |
| -------------------- | ---------------------- | ---------------- |
| `<% today %>`        | Current date           | `[[2026-01-29]]` |
| `<% yesterday %>`    | Previous date          | `[[2026-01-28]]` |
| `<% tomorrow %>`     | Next date              | `[[2026-01-30]]` |
| `<% time %>`         | Current time           | `14:30`          |
| `<% current page %>` | Current page reference | `[[My Page]]`    |

---

## Built-in Variables

### today

Returns a page reference to today's journal date.

```
<% today %>
→ [[2026-01-29]]
```

### yesterday

Returns a page reference to yesterday's date.

```
<% yesterday %>
→ [[2026-01-28]]
```

### tomorrow

Returns a page reference to tomorrow's date.

```
<% tomorrow %>
→ [[2026-01-30]]
```

### time

Returns the current time (no page reference).

```
<% time %>
→ 14:30
```

**Note**: Time is always rendered in 24-hour format (`HH:MM`). Both resolvers pass `hourCycle: "h23"` to `Intl.DateTimeFormat` regardless of system locale (`deps/outliner/src/logseq/outliner/template.cljs:12-16`; `src/main/frontend/context/i18n.cljs:198-202`).

### current page

Returns a reference to the page the template is being applied into, falling back to today's journal page when there is none (`deps/outliner/src/logseq/outliner/template.cljs:104-112`; `src/main/frontend/template.cljs:14-17`).

```
<% current page %>
→ [[Project Notes]]
```

### Empty variable

`<%%>` (or `<%   %>`) resolves to an empty string rather than being left in place (`deps/outliner/src/logseq/outliner/template.cljs:120-122`; `src/main/frontend/template.cljs:29-30`).

### Complete built-in list

The five names above are the **entire** set of built-in variables. Both resolvers define them in a `variable-rules` map with exactly these keys: `today`, `yesterday`, `tomorrow`, `time`, `current page`.

---

## Natural Language Dates

> **Scope:** natural-language dates are resolved **only in simple (DSL) query strings**. In templates, an expression that is not one of the five built-ins is left as its inner text — see [Unrecognized Variables](#unrecognized-variables) below.

Inside a simple query string, an expression that is not a built-in is passed to `chrono-node`'s `parseDate` (`src/main/frontend/date.cljs:22-25`; chrono-node is pinned at `2.9.0` in `package.json:160`). If it parses, the result is emitted as a journal page reference using your configured date format; if it does not parse, the inner text is returned unchanged (`src/main/frontend/template.cljs:33-36`).

### Relative Dates

```
<% next Monday %>
→ [[2026-02-03]]

<% last Friday %>
→ [[2026-01-24]]

<% in 3 days %>
→ [[2026-02-01]]

<% 2 weeks ago %>
→ [[2026-01-15]]
```

### Specific Dates

```
<% March 15 %>
→ [[2026-03-15]]

<% next month %>
→ [[2026-02-28]]

<% end of month %>
→ [[2026-01-31]]  (depends on what chrono-node 2.9.0 recognizes)
```

### Supported Expressions

| Expression Type | Examples                                       |
| --------------- | ---------------------------------------------- |
| Relative days   | `in 3 days`, `5 days ago`                      |
| Day of week     | `next Monday`, `last Friday`, `this Wednesday` |
| Relative weeks  | `next week`, `2 weeks ago`                     |
| Relative months | `next month`, `3 months from now`              |
| Specific dates  | `March 15`, `Jan 1 2027`                       |
| Special         | `end of month`, `beginning of year` (whatever chrono-node 2.9.0 recognizes) |

---

## Usage in Templates

Templates are applied by the `:apply-template` outliner op, which resolves variables through `logseq.outliner.template/resolve-dynamic-template-blocks` before inserting the blocks (`deps/outliner/src/logseq/outliner/op.cljs:265-271`).

Three things are worth knowing about the template path:

1. **Only the five built-ins are resolved.** There is no chrono-node fallback in `deps/outliner/src/logseq/outliner/template.cljs`.
2. **Variables are resolved in property values too**, not just block text. `:block/title`, `:block/raw-title`, and every value in `:block/properties-text-values` are passed through the resolver (`deps/outliner/src/logseq/outliner/template.cljs:151-161`).
3. **Missing journal pages are created.** Before applying a template, Logseq scans it for `today`/`yesterday`/`tomorrow` and creates any journal page that does not yet exist, so the resulting reference always points at a real page (`deps/outliner/src/logseq/outliner/op.cljs:257-263`). When the journal page exists the reference is emitted as a UUID page ref; only when it is missing does it fall back to the title string (`deps/outliner/src/logseq/outliner/template.cljs:74-79`).

### Basic Template

```markdown
- Meeting Notes for <% today %>
  - Attendees::
  - ## Topics::
  - Action Items::
    - TODO
```

**When inserted, becomes:**

```markdown
- Meeting Notes for [[2026-01-29]]
  - Attendees::
  - ## Topics::
  - Action Items::
    - TODO
```

### Template with Time

```markdown
- Journal Entry
  - Date: <% today %>
  - Time: <% time %>
  - Yesterday: <% yesterday %>
  - Plan for: <% tomorrow %>
```

### Variables in Property Values

Because `:block/properties-text-values` is resolved as well, a template can seed a property:

```markdown
- Daily standup
  due:: <% tomorrow %>
```

> **Removed:** an earlier version of this page showed a "Weekly Review" template using `<% last Monday %>`, `<% last Sunday %>` and `<% next Monday %>`. Natural-language expressions do **not** resolve during template application — those would insert the bare text `last Monday`, `last Sunday`, `next Monday` into the block. Use the five built-ins in templates, and reserve natural-language dates for simple query strings.

<a id="unrecognized-variables"></a>

### Unrecognized Variables

When a template variable is not one of the five built-ins, the resolver returns the **trimmed inner text with the `<%` / `%>` delimiters stripped** — it does not leave the literal `<% ... %>` in place (`deps/outliner/src/logseq/outliner/template.cljs:127-128`). The simple-query resolver behaves the same way once chrono-node also fails to parse the text (`src/main/frontend/template.cljs:33-36`).

```
<% next Monday %>   (in a template)
→ next Monday
```

---

## Usage in Queries

Dynamic variables are resolved **before** query parsing — but only for **simple (DSL) query strings**. `pre-transform-query` calls `resolve-dynamic-template!` on the raw query string and then hands it to the parser (`src/main/frontend/db/query_dsl.cljs:17-20`).

### Simple (DSL) Queries

```
{{query (between <% yesterday %> <% tomorrow %>)}}
```

This is also the only place natural-language dates work:

```
{{query (between <% 2 weeks ago %> <% today %>)}}
```

### Advanced Queries Use `:inputs`, Not Variables

Advanced (`#+BEGIN_QUERY` / `:logseq.property/query`) queries do **not** pass through `resolve-dynamic-template!`. Use `:inputs` keywords instead — they are resolved by `logseq.db.frontend.inputs/resolve-keyword-input` (`deps/db/src/logseq/db/frontend/inputs.cljs:69-119`).

```clojure
#+BEGIN_QUERY
{:title "Scheduled Today"
 :query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :logseq.property/scheduled ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:start-of-today-ms :end-of-today-ms]}
#+END_QUERY
```

**Pick the input that matches the attribute's type:**

| Attribute | Type | Input to use |
| --------- | ---- | ------------ |
| `:block/journal-day` | `:raw-number` (YYYYMMDD) | `:today`, `:yesterday`, `:tomorrow`, `:+7d`, `:-3d` |
| `:logseq.property/deadline`, `:logseq.property/scheduled` | `:datetime` (epoch ms) | `:start-of-today-ms`, `:end-of-today-ms`, `:today-start`, `:today-end`, `:+7d-ms` |

> **Removed:** an earlier version of this page said `:today` resolves to "a date integer suitable for comparison with `:block/scheduled` and `:block/deadline`". Two things changed. `:block/scheduled` and `:block/deadline` are not DB-graph attributes — the idents are `:logseq.property/scheduled` and `:logseq.property/deadline` (`deps/db/src/logseq/db/frontend/property.cljs:340-352`). And both are typed `:datetime`, i.e. epoch milliseconds, while `:today` resolves to a `YYYYMMDD` integer (`deps/db/src/logseq/db/frontend/inputs.cljs:100-101`) — comparing the two directly never matches. `:today` is the right input for `:block/journal-day`; use the `-ms` / `-start` / `-end` inputs for the datetime properties.

---

## Case Sensitivity

Variable names are **case-insensitive**:

```
<% TODAY %>     → [[2026-01-29]]
<% Today %>     → [[2026-01-29]]
<% today %>     → [[2026-01-29]]
```

---

## Output Format

### Journal Dates

Date variables output page references using your configured journal date format:

| Setting              | Output                            |
| -------------------- | --------------------------------- |
| `yyyy-MM-dd`         | `[[2026-01-29]]`                  |
| `MMM do, yyyy`       | `[[Jan 29th, 2026]]`              |
| `EEEE, MMMM d, yyyy` | `[[Wednesday, January 29, 2026]]` |

Configure in Settings → Date format.

### Time Format

Time is always output in 24-hour format (e.g., `14:30`). The locale is used only for formatting conventions (e.g., separator characters), not for choosing between 12-hour and 24-hour display, because `hourCycle: "h23"` is passed explicitly.

---

## Limitations

1. **Single-pass resolution**: Variables are resolved once at insertion; they don't update dynamically

2. **No nested variables**: `<% <% today %> %>` is not supported

3. **Page references only**: Date variables always output `[[page reference]]` format

4. **No arithmetic**: `<% today + 1 %>` is not supported (use `<% tomorrow %>` instead)

5. **No natural language in templates**: only `today`, `yesterday`, `tomorrow`, `time` and `current page` resolve when a template is applied

6. **Natural language parsing** (simple queries only): ambiguous phrases may parse differently
   - "next Monday" may vary if today is Sunday

7. **Only five built-ins**: the variable set is hard-coded; there is no user extension point (`;; TODO: programmable` — `src/main/frontend/template.cljs:21`)

---

## Troubleshooting

### Variable Not Resolving

**Symptom A**: `<% today %>` appears literally in output.

The regex `#"<%([^%].*?)%>"` never matched, so nothing was substituted.

**Causes**:

- Extra spaces: `< % today % >` (incorrect)
- Missing closing: `<% today` (incorrect)
- A `%` immediately after `<%`: the pattern requires a non-`%` first character

**Solution**: Ensure syntax is exactly `<% variable %>`

**Symptom B**: the delimiters are gone but the text is unchanged — e.g. `<% next Monday %>` became `next Monday`.

The regex matched but the name was not recognized. In a template that means anything outside the five built-ins; in a simple query it means chrono-node could not parse it either.

### Wrong Date Format

**Symptom**: Date appears but in wrong format

**Solution**: Check Settings → Editor → Preferred date format

### Natural Language Not Parsing

**Symptom**: `<% in 3 days %>` outputs as the bare text `in 3 days`.

**Possible causes**:

- **You are in a template.** Templates have no chrono-node fallback at all — this is expected, not a bug.
- Phrase not recognized by chrono-node 2.9.0
- Locale issues

**Solution**: In a simple query, try alternative phrasing:

- Instead of "in 3 days" → "3 days from now"
- Instead of "next week" → "in 7 days"

In a template, use one of the five built-ins, or compute the date with a query input instead.

---

## Implementation Details

Both resolvers share the same regex, `#"<%([^%].*?)%>"`, and both lower-case and trim the captured name before lookup (so variable names are case-insensitive).

### Template path — `deps/outliner/src/logseq/outliner/template.cljs`

1. `dynamic-template-journal-days` scans the template for `today`/`yesterday`/`tomorrow` and the caller creates any missing journal page (`op.cljs:257-263`)
2. `variable-rules` builds the five bindings against the live DB and the target block's page (`template.cljs:104-112`)
3. `resolve-block` rewrites `:block/title`, `:block/raw-title` and `:block/properties-text-values` (`template.cljs:151-161`)
4. Blank match → `""`; unknown name → trimmed inner text (`template.cljs:120-128`)

### Simple-query path — `src/main/frontend/template.cljs`

1. **Regex Matching**: Pattern `#"<%([^%].*?)%>"` finds variables (`template.cljs:19`)
2. **Built-in Lookup**: Checks against the `variable-rules` map (`template.cljs:8-17`)
3. **NLP Fallback**: If not built-in, attempts `chrono-node` `parseDate` and emits a journal page ref (`template.cljs:34-36`)
4. **Passthrough**: Unrecognized text returned unchanged, without delimiters

## Findings: New Capabilities

Verified against `be800f1711`:

- **Dynamic variables resolve inside template property values.** `:block/properties-text-values` is rewritten alongside the block title, so `due:: <% tomorrow %>` in a template works (`deps/outliner/src/logseq/outliner/template.cljs:160-161`).
- **Journal pages are auto-created during template application.** `ensure-template-journal-pages!` creates any journal page a template's `today`/`yesterday`/`tomorrow` variables point at, so the inserted reference is never a dangling text ref (`deps/outliner/src/logseq/outliner/op.cljs:257-263`).
- **Template date refs carry the page UUID, not the title.** When the journal page exists, `page-ref-for` emits `[[<uuid>]]` rather than `[[<title>]]`, so the reference survives a later change to the journal title format; only when the page is missing does it fall back to the title string (`deps/outliner/src/logseq/outliner/template.cljs:74-79`).

---

## See Also

- **Templates**: [how-to/content/templates.md](../../how-to/content/templates.md)
- **Query Inputs**: [reference/syntax/query-dsl.md](query-dsl.md)
- **Date Handling**: [chrono-node library](https://github.com/wanasit/chrono)

---

_Last updated: 2026-09-09 | [Report an issue](https://github.com/logseq/logseq/issues)_
