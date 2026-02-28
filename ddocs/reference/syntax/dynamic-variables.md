---
title: 'Dynamic Variables Reference'
description: "Complete reference for Logseq's dynamic template variables (< % syntax)"
status: stable
lastVerified: '2026-01-29'
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

> Complete reference for Logseq's dynamic template variables, which provide runtime resolution of dates, page references, and contextual information.

## Overview

Dynamic variables use the `<% variable %>` syntax and are resolved at insertion time. They work identically in both file-based and database graphs.

**Key Features:**

- Date-based variables (`today`, `yesterday`, `tomorrow`)
- Natural language date parsing ("next Monday", "in 3 days")
- Context variables (`current page`)
- Used in templates, queries, and manual blocks

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

**Note**: Time format follows your system locale settings.

### current page

Returns a reference to the currently focused page. Falls back to today if no page is focused.

```
<% current page %>
→ [[Project Notes]]
```

---

## Natural Language Dates

Beyond built-in variables, you can use natural language date expressions. These are parsed using the chrono-node library.

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
→ [[2026-01-31]]
```

### Supported Expressions

| Expression Type | Examples                                       |
| --------------- | ---------------------------------------------- |
| Relative days   | `in 3 days`, `5 days ago`                      |
| Day of week     | `next Monday`, `last Friday`, `this Wednesday` |
| Relative weeks  | `next week`, `2 weeks ago`                     |
| Relative months | `next month`, `3 months from now`              |
| Specific dates  | `March 15`, `Jan 1 2027`                       |
| Special         | `end of month`, `beginning of year`            |

---

## Usage in Templates

Dynamic variables are most commonly used in templates:

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

### Weekly Review Template

```markdown
- Weekly Review for <% today %>
  - Last Week: <% last Monday %> to <% last Sunday %>
  - Accomplishments::
  - Challenges::
  - Next Week Focus: <% next Monday %>
```

---

## Usage in Queries

Dynamic variables are resolved **before** query parsing, making them useful for date-relative queries.

### Find Tasks Due Today

```clojure
#+BEGIN_QUERY
{:title "Due Today"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/scheduled ?d]
         [(= ?d ?today)]]
 :inputs [:today]}
#+END_QUERY
```

### Alternative with Variable in Query String

```clojure
#+BEGIN_QUERY
{:title "Tasks Scheduled for Today"
 :query [:find (pull ?b [*])
         :where
         [?b :block/page ?p]
         [?p :block/title "<% today %>"]]}
#+END_QUERY
```

**Note**: Using `:inputs [:today]` is preferred over embedding `<% today %>` in the query string for better performance and cleaner syntax.

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

Time follows your system locale:

- 24-hour: `14:30`
- 12-hour: `2:30 PM`

---

## Limitations

1. **Single-pass resolution**: Variables are resolved once at insertion; they don't update dynamically

2. **No nested variables**: `<% <% today %> %>` is not supported

3. **Page references only**: Date variables always output `[[page reference]]` format

4. **No arithmetic**: `<% today + 1 %>` is not supported (use `<% tomorrow %>` instead)

5. **Natural language parsing**: Ambiguous phrases may parse differently across locales
   - "next Monday" may vary if today is Sunday

---

## Troubleshooting

### Variable Not Resolving

**Symptom**: `<% today %>` appears literally in output

**Causes**:

- Extra spaces: `< % today % >` (incorrect)
- Missing closing: `<% today` (incorrect)
- Typo in variable name

**Solution**: Ensure syntax is exactly `<% variable %>`

### Wrong Date Format

**Symptom**: Date appears but in wrong format

**Solution**: Check Settings → Editor → Preferred date format

### Natural Language Not Parsing

**Symptom**: `<% in 3 days %>` outputs as literal text

**Possible causes**:

- Phrase not recognized by chrono-node
- Locale issues

**Solution**: Try alternative phrasing:

- Instead of "in 3 days" → "3 days from now"
- Instead of "next week" → "in 7 days"

---

## Implementation Details

Dynamic variables are processed by `frontend.template/resolve-dynamic-template!`:

1. **Regex Matching**: Pattern `#"<%([^%].*?)%>"` finds variables
2. **Built-in Lookup**: Checks against variable-rules map
3. **NLP Fallback**: If not built-in, attempts chrono-node parsing
4. **Passthrough**: Unrecognized text returned unchanged

The resolution is **storage-agnostic** - works identically on file and DB graphs.

---

## See Also

- **Templates**: [how-to/content/templates.md](../../how-to/content/templates.md)
- **Query Inputs**: [reference/syntax/query-dsl.md](query-dsl.md)
- **Date Handling**: [chrono-node library](https://github.com/wanasit/chrono)

---

_Last updated: 2026-01-29 | [Report an issue](https://github.com/logseq/logseq/issues)_
