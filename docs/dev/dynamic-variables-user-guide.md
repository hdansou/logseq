# Logseq Dynamic Variables: User Guide

**Master Templates, Queries, and Macros with Dynamic Variables**

---

## 📚 Table of Contents

1. [What Are Dynamic Variables?](#what-are-dynamic-variables)
2. [Quick Start](#quick-start)
3. [All Available Variables](#all-available-variables)
4. [Using Variables in Templates](#using-variables-in-templates)
5. [Using Variables in Queries](#using-variables-in-queries)
6. [Using Variables in Macros](#using-variables-in-macros)
7. [Real-World Use Cases](#real-world-use-cases)
8. [Tips & Tricks](#tips--tricks)
9. [Troubleshooting](#troubleshooting)
10. [FAQ](#faq)

---

## What Are Dynamic Variables?

Dynamic variables are placeholders that **automatically update** when you use them. Instead of typing today's date manually, you can use `<% today %>` and Logseq will replace it with the current date.

### Why Use Dynamic Variables?

✅ **Always Current**: Variables update automatically - no stale dates
✅ **Time Saving**: Write templates once, reuse forever
✅ **Context Aware**: Reference the current page dynamically
✅ **Natural Language**: Use phrases like "next Monday" or "last Friday"
✅ **Universal**: Work in templates, queries, and macros

### Basic Syntax

```
<% variable-name %>
```

**Examples:**
- `<% today %>` → `[[2025-10-16]]`
- `<% current page %>` → `[[My Project]]`
- `<% time %>` → `23:45`

---

## Quick Start

### Try It Now!

1. Create a new block in any page
2. Type: `Today is <% today %>`
3. Press Enter
4. You'll see: `Today is [[2025-10-16]]`

### Your First Template

1. Create a block: `Meeting Notes`
2. Add properties:
   ```
   template:: Meeting Notes
   template-including-parent:: false
   ```
3. Add child blocks:
   ```
   - 📅 Date: <% today %>
   - ⏰ Time: <% time %>
   - 📍 Project: <% current page %>
   - ## Attendees
   - ## Agenda
   - ## Action Items
   - ## Next Steps
   ```
4. Use the template anywhere with `/template` command!

---

## All Available Variables

### Date Variables

| Variable | Description | Example Output |
|----------|-------------|----------------|
| `<% today %>` | Current date | `[[2025-10-16]]` |
| `<% yesterday %>` | Previous day | `[[2025-10-15]]` |
| `<% tomorrow %>` | Next day | `[[2025-10-17]]` |

### Time Variable

| Variable | Description | Example Output |
|----------|-------------|----------------|
| `<% time %>` | Current time (24h) | `23:45` |

### Context Variable

| Variable | Description | Example Output |
|----------|-------------|----------------|
| `<% current page %>` | Active page name | `[[Project Planning]]` |

### Natural Language Dates

You can use **plain English** to reference dates!

| Expression | Example Output |
|------------|----------------|
| `<% next monday %>` | `[[2025-10-20]]` |
| `<% last friday %>` | `[[2025-10-10]]` |
| `<% in 3 days %>` | `[[2025-10-19]]` |
| `<% 2 weeks ago %>` | `[[2025-10-02]]` |
| `<% next month %>` | `[[2025-11-16]]` |
| `<% this friday %>` | `[[2025-10-17]]` |

**Supported Phrases:**
- Relative: `yesterday`, `today`, `tomorrow`
- Week references: `this week`, `next week`, `last week`
- Month references: `this month`, `next month`, `last month`
- Year references: `this year`, `next year`, `last year`
- Weekdays: `next monday`, `last friday`, etc.
- Durations: `in 3 days`, `2 weeks ago`, etc.

---

## Using Variables in Templates

Templates are reusable block structures. Dynamic variables make them powerful!

### Basic Template Structure

```
- Template Name
  template:: Template Name
  template-including-parent:: false
  - Your content with <% variables %>
  - More content...
```

### Example 1: Daily Journal Template

```
- Daily Journal
  template:: Daily Journal
  template-including-parent:: false
  - # <% today %>
  - ## 🎯 Goals for Today
  - ## 📝 Notes
  - ## ✅ Completed
  - ## 🔄 Review from <% yesterday %>
  - ## 📅 Plan for <% tomorrow %>
```

**When inserted on Oct 16, 2025:**
```
# [[2025-10-16]]
## 🎯 Goals for Today
## 📝 Notes
## ✅ Completed
## 🔄 Review from [[2025-10-15]]
## 📅 Plan for [[2025-10-17]]
```

### Example 2: Weekly Review Template

```
- Weekly Review
  template:: Weekly Review
  template-including-parent:: false
  - # Weekly Review: <% today %>
  - Week ending: <% this friday %>
  - ## 🎯 Wins This Week
  - ## 📚 Lessons Learned
  - ## 🔄 Next Week Preview
  - Preview for <% next monday %>
```

### Example 3: Project Task Template

```
- Project Task
  template:: Project Task
  template-including-parent:: false
  - TODO Task name
    project:: <% current page %>
    created:: <% today %>
    due:: <% in 7 days %>
    - ## Context
    - Working on project: <% current page %>
    - ## Subtasks
    - ## Notes
```

**Use Case**: When you're on "Website Redesign" page and insert this template, it automatically links to that project!

### Example 4: Meeting Notes Template

```
- Meeting Notes
  template:: Meeting Notes
  template-including-parent:: false
  - # Meeting: <% current page %>
  - **Date:** <% today %>
  - **Time:** <% time %>
  - **Project:** <% current page %>
  - ## Attendees
  - ## Agenda
  - ## Discussion
  - ## Decisions
  - ## Action Items
  - ## Next Meeting: <% next monday %>
```

### Example 5: Backlink to Future

```
- Backlink Future
  template:: Backlink Future
  template-including-parent:: false
  - Follow up on <% in 3 days %>
  - Review progress on <% next monday %>
  - Monthly check-in on <% next month %>
```

**Why This is Powerful**: Creates forward-looking backlinks that connect today's work to future dates!

### Template Best Practices

✅ **Use `template-including-parent:: false`** to avoid duplicating the template name
✅ **Combine multiple variables** for rich context
✅ **Use natural language dates** for flexible scheduling
✅ **Include `<% current page %>`** for automatic project linking
✅ **Add forward-looking dates** to create future connections

---

## Using Variables in Queries

Queries let you search and filter your notes. Dynamic variables make queries **update automatically**.

### ⚠️ IMPORTANT: Query Syntax Differs Between DB and File-Based Graphs!

**The way you write queries is different depending on your graph type:**

| Graph Type | Simple Query Syntax | Advanced Query Syntax |
|------------|---------------------|----------------------|
| **File-based** | `{{query (and [[page]])}}` | Code block with ````query` |
| **DB-based** | `query:: (and [[page]])` | Property with code block |

**See the complete guide**: [Query Syntax: DB vs File-Based](./query-syntax-db-vs-file.md)

---

### Basic Query Syntax (File-Based Graphs)

```clojure
{{query (and <% today %> (task NOW))}}
```

### Basic Query Syntax (DB-Based Graphs)

```markdown
query:: (and <% today %> (task "Now"))
```

**Both do the same thing**: Find all tasks tagged with today's date and marked NOW/Now.

**Important**: DB-based graphs use quoted, title-case task statuses like `"Now"`, `"Todo"`, `"Doing"`, `"Done"`. File-based graphs use uppercase keywords like `NOW`, `TODO`, `DOING`, `DONE`.

**Important**: The examples below use file-based `{{query}}` syntax. If you're using a DB-based graph, use `query::` property syntax instead.

### Example 1: Today's Tasks

**File-based**:
```clojure
{{query (and <% today %> (task TODO DOING NOW))}}
```

**DB-based**:
```markdown
query:: (and <% today %> (task "Todo" "Doing"))
```

**What it does**: Shows all tasks linked to today

**Note**: DB-based graphs use quoted, title-case task statuses (`"Todo"`, `"Doing"`, `"Done"`), while file-based graphs use uppercase keywords (`TODO`, `DOING`, `NOW`).

**Use Case**: Add to your daily note template to automatically see today's tasks

### Example 2: This Week's Tasks

**File-based**:
```clojure
{{query (and (between <% today %> <% in 7 days %>))}}
```

**DB-based**:
```markdown
query:: (and (between <% today %> <% in 7 days %>))
```

**What it does**: Shows all pages/blocks between today and 7 days from now

**Use Case**: Weekly planning view

### Example 3: Recent Activity

```clojure
{{query (and (page-property status "active") (between created-at <% 7 days ago %> <% today %>))}}
```

**What it does**: Shows active items created in the last week

**Use Case**: Track recent work

### Example 4: Upcoming Deadlines

**File-based**:
```clojure
{{query (and (property due <% today %>) (task TODO))}}
```

**DB-based**:
```markdown
query:: (and (property due <% today %>) (task "Todo"))
```

**What it does**: Shows incomplete tasks due today

**Use Case**: Daily deadline tracker

### Example 5: Yesterday's Journal Review

```clojure
{{query <% yesterday %>}}
```

**What it does**: Shows all blocks mentioning yesterday's date

**Use Case**: Quick review of yesterday's work

### Example 6: Project Progress This Week

```clojure
{{query (and <% current page %> (between <% last monday %> <% today %>))}}
```

**What it does**: Shows all activity on current project this week

**Use Case**: Place in project pages for automatic weekly summaries

### Example 7: Next Week Planning

**File-based**:
```clojure
{{query (and (task TODO) (property due <% next monday %>))}}
```

**DB-based**:
```markdown
query:: (and (task "Todo") (property due <% next monday %>))
```

**What it does**: Shows tasks scheduled for next Monday

**Use Case**: Weekend planning

### Advanced Query Example

**File-based**:
```clojure
{{query
  (and
    <% current page %>
    (or
      (property priority "high")
      (property due <% today %>)
      (property due <% tomorrow %>)
    )
    (task TODO DOING)
  )
}}
```

**DB-based**:
```markdown
query::
  (and
    <% current page %>
    (or
      (property priority "high")
      (property due <% today %>)
      (property due <% tomorrow %>)
    )
    (task "Todo" "Doing")
  )
```

**What it does**: Shows high-priority or soon-due tasks for the current project

### Query Best Practices

✅ **Use date ranges** with `between` for time-based queries
✅ **Combine `<% current page %>`** for project-specific views
✅ **Use task filters** to focus on actionable items
✅ **Add to templates** so queries auto-update in every instance
✅ **Test in a scratch page** before adding to templates
✅ **Use correct syntax** for your graph type (file-based: `{{query}}`, DB-based: `query::`)

### Query Date Formats

**Important**: Queries support TWO types of date references:

1. **Template variables** (with `<% %>`):

   File-based: `{{query (and <% today %>)}}`

   DB-based: `query:: (and <% today %>)`

   Replaced with `[[2025-10-16]]` before query runs

2. **Query keywords** (without `<% %>`):

   File-based: `{{query (between created-at yesterday today)}}`

   DB-based: `query:: (between created-at yesterday today)`

   Interpreted as date integers during query execution

**Both work!** Use template variables for consistency. See [Query Inputs Guide](./query-inputs-guide.md) for complete reference.

---

## Using Variables in Macros

Macros are reusable snippets that can include dynamic variables.

### Creating a Macro with Variables

1. Go to Settings → General → Edit config.edn
2. Add to `:macros` section:

```clojure
:macros {
  :today-ref "<% today %>"
  :current-time "Updated at <% time %> on <% today %>"
  :project-link "📁 Project: <% current page %>"
  :deadline-week "⚠️ Due: <% in 7 days %>"
}
```

### Using Macros

Type in any block:
```
{{today-ref}}
{{current-time}}
{{project-link}}
{{deadline-week}}
```

Becomes:
```
[[2025-10-16]]
Updated at 23:45 on [[2025-10-16]]
📁 Project: [[My Project]]
⚠️ Due: [[2025-10-23]]
```

### Advanced Macro Examples

#### 1. Meeting Timestamp

```clojure
:meeting-stamp "📅 <% today %> at <% time %> - <% current page %>"
```

**Usage**: `{{meeting-stamp}}`
**Output**: `📅 [[2025-10-16]] at 23:45 - [[Project Alpha]]`

#### 2. Weekly Reference

```clojure
:week-links "Week of <% this monday %> to <% this friday %>"
```

**Usage**: `{{week-links}}`
**Output**: `Week of [[2025-10-13]] to [[2025-10-17]]`

#### 3. Project Context

```clojure
:ctx "Context: <% current page %> | <% today %>"
```

**Usage**: Add to any task for automatic context
**Output**: `Context: [[Website Redesign]] | [[2025-10-16]]`

#### 4. Backlink Future

```clojure
:follow-up "Follow up on <% in 3 days %>"
:next-week-review "Review by <% next monday %>"
```

### Macro Best Practices

✅ **Keep macros short** - they expand inline
✅ **Use descriptive names** - `meeting-stamp` not `ms`
✅ **Combine variables** for rich output
✅ **Test before committing** to config.edn
✅ **Document your macros** in comments

---

## Real-World Use Cases

### Use Case 1: Daily Planning System

**Goal**: Automatic daily notes with smart queries

**Setup**:

1. Create template:
```
- Daily Plan
  template:: Daily Plan
  template-including-parent:: false
  - # <% today %>
  - ## 🎯 Priority Tasks
    - {{query (and (property due <% today %>) (task TODO))}}  # File-based
    - query:: (and (property due <% today %>) (task "Todo"))  # DB-based
  - ## 📝 Notes
  - ## ✅ Completed Today
    - {{query (and <% today %> (task DONE))}}  # File-based
    - query:: (and <% today %> (task "Done"))  # DB-based
  - ## 🔄 Review <% yesterday %>
    - {{query <% yesterday %>}}
  - ## 📅 Tomorrow Preview
    - {{query (property due <% tomorrow %>)}}
```

**Note**: This template includes both file-based (`{{query}}`) and DB-based (`query::`) syntax. Use the appropriate syntax for your graph type.

2. Use in your daily notes!

**Result**: Each day automatically shows:
- Today's tasks
- Completed items
- Yesterday's review
- Tomorrow's preview

### Use Case 2: Project Dashboard

**Goal**: Live project status on project pages

**Setup**:

Add to top of each project page:
```
- # <% current page %> Dashboard
- **Last Updated:** <% today %> at <% time %>
- ## 📊 Current Status
  - {{query (and <% current page %> (property status "active"))}}  # File-based
  - query:: (and <% current page %> (property status "active"))    # DB-based
- ## ✅ This Week's Progress
  - {{query (and <% current page %> (between <% last monday %> <% today %>))}}  # File-based
  - query:: (and <% current page %> (between <% last monday %> <% today %>))    # DB-based
- ## 🎯 Upcoming Tasks
  - {{query (and <% current page %> (task TODO) (property due <% in 7 days %>))}}         # File-based
  - query:: (and <% current page %> (task "Todo") (property due <% in 7 days %>))         # DB-based
- ## 📅 Next Review: <% next friday %>
```

**Result**: Real-time project dashboard that updates automatically

### Use Case 3: Weekly Review Workflow

**Goal**: Structured weekly reflection

**Setup**:

1. Template:
```
- Weekly Review
  template:: Weekly Review
  template-including-parent:: false
  - # Weekly Review: <% today %>
  - **Week:** <% last monday %> to <% today %>
  - ## 🎯 Wins
    - {{query (and (property status "completed") (between <% last monday %> <% today %>))}}  # File-based
    - query:: (and (property status "completed") (between <% last monday %> <% today %>))    # DB-based
  - ## 📚 Learnings
  - ## 🔄 Next Week Goals
    - Preview for <% next monday %>
  - ## 📅 Key Dates Next Week
    - {{query (and (task TODO) (between <% next monday %> <% next friday %>))}}         # File-based
    - query:: (and (task "Todo") (between <% next monday %> <% next friday %>))         # DB-based
```

2. Run every Friday

**Result**: Automatic aggregation of the week's accomplishments

### Use Case 4: Meeting Minutes System

**Goal**: Consistent meeting documentation

**Setup**:

1. Macro in config.edn:
```clojure
:meeting-header "📅 <% today %> | ⏰ <% time %> | 📁 <% current page %>"
```

2. Template:
```
- Meeting
  template:: Meeting
  template-including-parent:: false
  - # Meeting Notes
  - {{meeting-header}}
  - ## Attendees
  - ## Agenda
  - ## Discussion
  - ## Decisions
  - ## Action Items
    - TODO Follow up by <% next monday %>
  - ## Next Meeting
    - Scheduled for <% in 7 days %>
```

**Result**: Professional meeting notes with automatic dates and context

### Use Case 5: Habit Tracking

**Goal**: Track daily habits with automatic date linking

**Setup**:

1. Template:
```
- Daily Habits
  template:: Daily Habits
  template-including-parent:: false
  - # Habits: <% today %>
  - Morning Routine <% today %>
    - [ ] Exercise
    - [ ] Meditation
    - [ ] Journaling
  - Evening Routine <% today %>
    - [ ] Reading
    - [ ] Planning for <% tomorrow %>
    - [ ] Gratitude log
  - ## Streak Check
    - {{query (and [[Daily Habits]] (between <% 7 days ago %> <% today %>))}}
```

**Result**: Automatic habit tracking with visual streaks

### Use Case 6: Content Calendar

**Goal**: Blog post planning with scheduling

**Setup**:

On "Content Calendar" page:
```
- # Content Calendar
- ## This Week's Posts
  - {{query (and [[Content Calendar]] (property publish-date <% today %>))}}
- ## Next Week Pipeline
  - {{query (and [[Content Calendar]] (between <% next monday %> <% next friday %>))}}
- ## Draft Template
  - TODO <% current page %> - New Post
    created:: <% today %>
    publish-date:: <% in 7 days %>
    - ## Outline
    - ## Draft
    - ## Review on <% in 5 days %>
```

**Result**: Organized content pipeline with automatic scheduling

### Use Case 7: Learning Journal

**Goal**: Track learning progress over time

**Setup**:

```
- Learning Log
  template:: Learning Log
  template-including-parent:: false
  - # Learning: <% today %>
  - **Topic:** <% current page %>
  - **Date:** <% today %>
  - ## What I Learned
  - ## Questions
  - ## To Practice
  - ## Review on <% in 3 days %>
  - ## Related Learning
    - {{query (and <% current page %> (between <% 7 days ago %> <% yesterday %>))}}
```

**Result**: Spaced repetition for learning with automatic review dates

---

## Tips & Tricks

### Tip 1: Combine Variables for Context

Instead of:
```
Meeting on [[2025-10-16]]
```

Use:
```
Meeting for <% current page %> on <% today %> at <% time %>
```

**Why**: More context automatically captured!

### Tip 2: Create Forward-Looking Links

```
- Review this on <% in 3 days %>
- Follow up by <% next monday %>
- Quarterly review on <% next month %>
```

**Why**: Creates future backlinks that connect today's work to specific future dates

### Tip 3: Use Natural Language for Flexibility

Instead of:
```
<% tomorrow %>
```

Use:
```
<% next monday %>
```

**Why**: More flexible for weekly planning

### Tip 4: Case Doesn't Matter

All of these work:
```
<% today %>
<% TODAY %>
<% Today %>
<%today%>
```

**Why**: Variables are case-insensitive

### Tip 5: Combine with Properties

```
- TODO Task name
  due:: <% in 7 days %>
  project:: <% current page %>
  created:: <% today %>
```

**Why**: Automatic metadata for queries

### Tip 6: Use in Property Values

```
- Page title
  last-reviewed:: <% today %>
  next-review:: <% next month %>
  project:: <% current page %>
```

**Why**: Works in property values, not just content!

### Tip 7: Create Date Ranges in Queries

```
{{query (between <% last monday %> <% this friday %>)}}
```

**Why**: Automatic weekly views

### Tip 8: Debug with Simple Tests

Test variables in a scratch block:
```
Test: <% today %> | <% time %> | <% current page %>
```

**Why**: Quick validation before using in templates

### Tip 9: Mix Variables and Static Text

```
Meeting for [[Client A]] on <% today %> about <% current page %>
```

**Why**: Combine automatic and manual context

### Tip 10: Use in Template Names

```
- Weekly Report <% today %>
```

**Why**: Unique template instances with dates

---

## Troubleshooting

### Problem: Variable Shows as Literal Text

**Symptom**: You see `<% today %>` instead of `[[2025-10-16]]`

**Solutions**:

1. **Check spelling**: Must be exactly `today`, not `todya` or `todays`
   ```
   ❌ <% todya %>
   ✅ <% today %>
   ```

2. **Check syntax**: Need `<%` and `%>` with spaces
   ```
   ✅ <% today %>
   ✅ <%today%>
   ❌ < % today %>
   ❌ <%today>
   ```

3. **Check if inside code block**: Variables don't resolve in code blocks
   ```markdown
   ❌ `<% today %>` (inline code)
   ✅ <% today %> (plain text)
   ```

### Problem: Current Page Returns Wrong Value

**Symptom**: `<% current page %>` shows unexpected page

**Explanation**:
- If editing a **block**, returns the page containing that block
- If on a **page**, returns that page's name

**Example**:
- You're editing a block on "Project A" page
- `<% current page %>` → `[[Project A]]` (the page, not the block)

**This is expected!** It always returns the page context.

### Problem: Natural Language Date Not Working

**Symptom**: `<% next monday %>` shows as literal text

**Solutions**:

1. **Try variations**:
   ```
   ✅ next monday
   ✅ Next Monday
   ✅ in 1 week
   ❌ next mon (abbreviations don't work)
   ```

2. **Check supported phrases**: See [Natural Language Dates](#natural-language-dates)

3. **Test with simple dates first**:
   ```
   <% tomorrow %>  (test this first)
   <% next monday %> (then this)
   ```

### Problem: Query Not Showing Results

**Symptom**: Query with variables returns no results

**Solutions**:

1. **Test the resolved query**:
   - First write: `<% today %>` in a block
   - See what it resolves to: `[[2025-10-16]]`
   - Then use that in your query manually to test

2. **Check query syntax**:
   ```clojure
   ❌ {{query (<% today %>)}}
   ✅ {{query <% today %>}}
   ```

3. **Verify data exists**:
   - Make sure you have blocks tagged with today's date
   - Query can't find what doesn't exist!

### Problem: Double Brackets `[[<% today %>]]` Don't Work

**Symptom**: Writing `[[<% today %>]]` produces `[[[[2025-10-16]]]]` or queries don't work

**Explanation**: Variables ALREADY include the brackets!
- `<% today %>` → `[[2025-10-16]]` ✅
- `[[<% today %>]]` → `[[[[2025-10-16]]]]` ❌ (double brackets - invalid)

**Solution**:

```clojure
❌ WRONG:
{{query (and [[<% today %>]] (task TODO))}}  # File-based
query:: (and [[<% today %>]] (task "Todo"))  # DB-based
[[<% today %>]] in regular content
project:: [[<% current page %>]]

✅ CORRECT:
{{query (and <% today %> (task TODO))}}  # File-based
query:: (and <% today %> (task "Todo"))  # DB-based
<% today %> in regular content
project:: <% current page %>
```

**Why**: The template system already wraps dates with `[[` and `]]`. Adding extra brackets creates invalid syntax.

**All variables return page refs with brackets**:
- `<% today %>` → `[[2025-10-16]]`
- `<% yesterday %>` → `[[2025-10-15]]`
- `<% current page %>` → `[[My Project]]`
- `<% next monday %>` → `[[2025-10-20]]`

**Never add extra brackets around variables!**

### Problem: Template Variables Not Resolving

**Symptom**: Variables in templates remain literal after insertion

**Solutions**:

1. **Check template property**:
   ```
   ✅ template:: Template Name
   ❌ Template:: Template Name (capital T)
   ```

2. **Wait for insertion**: Variables resolve during template insertion, not before

3. **Verify Logseq version**: Very old versions might not support all variables

### Problem: Time Shows Wrong Format

**Symptom**: Time shows in 12-hour format instead of 24-hour

**Explanation**: Time format is based on your browser's locale settings

**Solution**:
- This is expected behavior
- Time follows your system's locale preferences
- Cannot be overridden currently

### Problem: Variables in Macros Not Working

**Symptom**: Macro shows `<% today %>` literally

**Solution**: Check config.edn syntax:

```clojure
✅ :macros {
     :my-date "Date: <% today %>"
   }

❌ :macros {
     :my-date Date: <% today %>  (missing quotes)
   }
```

### Problem: Date Format Not What I Expected

**Symptom**: Dates show as `[[2025-10-16]]` but you want `[[Oct 16th, 2025]]`

**Solution**: Change journal date format in Settings:
1. Go to Settings → General → Date Formatter
2. Choose your preferred format
3. All date variables will use this format

### Problem: Variable Works in One Place, Not Another

**Symptom**: `<% today %>` works in templates but not in X

**Contexts Where Variables Work**:
- ✅ Template content
- ✅ Query strings
- ✅ Macro definitions
- ✅ Regular block content
- ✅ Property values

**Contexts Where Variables DON'T Work**:
- ❌ Inside code blocks
- ❌ Page titles (manual only)
- ❌ Already-inserted content (only resolves once)

---

## FAQ

### Q1: Do variables work in both DB and file-based graphs?

**A**: Yes! Variables work identically in both DB-based and file-based graphs.

### Q2: Can I create my own custom variables?

**A**: Not currently. You can only use the built-in variables. Use macros for custom text snippets.

### Q3: Do variables update automatically in old content?

**A**: No. Variables resolve once when you insert them. Old content keeps the date it was created with.

**Example**:
- Monday: Insert `<% today %>` → Becomes `[[2025-10-13]]`
- Tuesday: That block still says `[[2025-10-13]]` (doesn't update)
- Tuesday: Insert NEW `<% today %>` → Becomes `[[2025-10-14]]`

This is by design! You want historical context to stay accurate.

### Q4: Can I use multiple variables in one line?

**A**: Yes! Use as many as you want:

```
Meeting on <% today %> at <% time %> for <% current page %>
```

### Q5: What happens if I'm not on a page (editing in sidebar)?

**A**: `<% current page %>` falls back to today's date if no page context is available.

### Q6: Can I format dates differently per variable?

**A**: No, all date variables use your global journal date format. Change it in Settings → Date Formatter.

### Q7: Do queries with variables run slower?

**A**: No, variables are replaced before the query runs, so there's no performance impact.

### Q8: Can I use variables in page properties?

**A**: Yes! Variables work in property values:

```
- Page title
  created:: <% today %>
  project:: <% current page %>
```

### Q9: What if I want to show the literal text `<% today %>`?

**A**: Workarounds:
- Use code format: `` `<% today %>` ``
- Split it: `< % today % >`
- Use escape characters (if in code block)

There's no official escape syntax currently.

### Q10: Can I use variables in advanced queries?

**A**: Yes! Variables work in both simple and advanced queries:

```clojure
{{query
  (and
    <% current page %>
    (task TODO)
    (between <% today %> <% in 7 days %>)
  )
}}
```

### Q11: Are variable names case-sensitive?

**A**: No! All these work the same:
- `<% today %>`
- `<% TODAY %>`
- `<% Today %>`

### Q12: Can I use variables in property names (not values)?

**A**: No, only in property values:

```
✅ due:: <% today %>
❌ <% current page %>::  value
```

### Q13: Do variables work in linked references?

**A**: When you insert `<% today %>`, it becomes `[[2025-10-16]]`, which creates a linked reference. So yes, indirectly!

### Q14: Can I use natural language dates in my own locale?

**A**: Partially. English phrases work best. Other languages have limited support depending on the chrono-node library.

### Q15: What's the difference between `<% today %>` in templates vs queries?

**A**: They work the same! Both resolve to the page reference for today's date.

---

## Quick Reference Card

### All Variables at a Glance

```
Date & Time:
<% today %>          Today's date
<% yesterday %>      Yesterday's date
<% tomorrow %>       Tomorrow's date
<% time %>           Current time (24h)

Context:
<% current page %>   Current page name

Natural Language:
<% next monday %>    Next Monday's date
<% last friday %>    Last Friday's date
<% in 3 days %>      3 days from now
<% 2 weeks ago %>    2 weeks ago
<% next month %>     Same day next month
```

### Common Patterns

```
Forward Links:
Follow up on <% in 3 days %>
Review by <% next monday %>

Query Today's Tasks (File-based):
{{query (and <% today %> (task TODO))}}

Query Today's Tasks (DB-based):
query:: (and <% today %> (task "Todo"))

Template Header:
# <% today %> - <% current page %>

Meeting Timestamp:
<% today %> at <% time %> - <% current page %>

Weekly Range Query:
{{query (between <% last monday %> <% today %>)}}
```

---

## Next Steps

### Level 1: Beginner
- ✅ Use `<% today %>` in daily notes
- ✅ Create your first template with date variables
- ✅ Add a simple query with `<% today %>`

### Level 2: Intermediate
- ✅ Use `<% current page %>` for project linking
- ✅ Create templates with multiple variables
- ✅ Build queries with date ranges
- ✅ Add macros with variables to config.edn

### Level 3: Advanced
- ✅ Build complex templates with nested queries
- ✅ Create project dashboards with live queries
- ✅ Use natural language dates for flexible scheduling
- ✅ Design complete workflow systems

### Level 4: Master
- ✅ Build interconnected template systems
- ✅ Create time-based automation with forward links
- ✅ Design personal productivity systems
- ✅ Share templates with the community!

---

## Resources

### Documentation
- **Developer Guide**: [dynamic-variables-developer-guide.md](./dynamic-variables-developer-guide.md) - Technical deep-dive
- **Logseq Docs**: [docs.logseq.com](https://docs.logseq.com/)

### Community
- **Forum**: Share your templates at forum.logseq.com
- **Discord**: Get help in #questions channel
- **Reddit**: r/logseq for discussions

### Example Templates Library

Check the Logseq forum for:
- Daily note templates
- Project management templates
- Meeting notes templates
- Learning journal templates
- Habit tracking templates

---

## Appendix: Complete Examples

### Complete Example 1: Student's Study System

```
- Study Session
  template:: Study Session
  template-including-parent:: false
  - # Study: <% current page %>
  - **Date:** <% today %>
  - **Time:** <% time %>
  - **Subject:** <% current page %>
  - ## 📚 Material Covered
  - ## 💡 Key Concepts
  - ## ❓ Questions
  - ## ✅ Practice Problems
  - ## 🔄 Review This
    - First review: <% in 3 days %>
    - Second review: <% in 7 days %>
    - Third review: <% next month %>
  - ## Related Material
    - {{query (and <% current page %> (between <% 7 days ago %> <% yesterday %>))}}
```

### Complete Example 2: Freelancer's Project Tracker

```
- Client Project
  template:: Client Project
  template-including-parent:: false
  - # <% current page %> - Project Dashboard
  - **Created:** <% today %>
  - **Last Updated:** <% time %>
  - ## 📊 Project Status
    - project:: <% current page %>
    - status:: active
    - started:: <% today %>
    - deadline:: <% in 30 days %>
  - ## 🎯 Milestones
    - TODO Phase 1
      due:: <% in 7 days %>
    - TODO Phase 2
      due:: <% in 14 days %>
    - TODO Final Delivery
      due:: <% in 30 days %>
  - ## 📋 Current Tasks
    - {{query (and <% current page %> (task TODO DOING))}}  # File-based
    - query:: (and <% current page %> (task "Todo" "Doing"))  # DB-based
  - ## ✅ Completed This Week
    - {{query (and <% current page %> (task DONE) (between <% last monday %> <% today %>))}}  # File-based
    - query:: (and <% current page %> (task "Done") (between <% last monday %> <% today %>))  # DB-based
  - ## 📅 Next Review: <% next friday %>
```

### Complete Example 3: Writer's Content System

```
- Blog Post
  template:: Blog Post
  template-including-parent:: false
  - TODO <% current page %> - New Post
    type:: blog-post
    status:: draft
    created:: <% today %>
    draft-due:: <% in 3 days %>
    review-due:: <% in 5 days %>
    publish-date:: <% in 7 days %>
  - ## 📝 Title Ideas
  - ## 🎯 Target Audience
  - ## 📚 Research Links
  - ## ✏️ Outline
  - ## 📄 Draft
  - ## ✅ Checklist
    - [ ] Outline complete by <% tomorrow %>
    - [ ] First draft by <% in 3 days %>
    - [ ] Self-review by <% in 5 days %>
    - [ ] Publish on <% in 7 days %>
  - ## 📊 Related Posts
    - {{query (and [[Blog Post]] <% current page %>)}}
```

### Complete Example 4: Developer's Sprint Planning

```
- Sprint
  template:: Sprint
  template-including-parent:: false
  - # Sprint: <% today %> to <% in 14 days %>
  - **Sprint Start:** <% today %>
  - **Sprint End:** <% in 14 days %>
  - **Project:** <% current page %>
  - ## 🎯 Sprint Goals
  - ## 📋 Backlog
    - {{query (and <% current page %> (property status "backlog"))}}
  - ## 🚀 In Progress
    - {{query (and <% current page %> (task DOING))}}  # File-based
    - query:: (and <% current page %> (task "Doing"))  # DB-based
  - ## ✅ Completed
    - {{query (and <% current page %> (task DONE) (between <% today %> <% in 14 days %>))}}  # File-based
    - query:: (and <% current page %> (task "Done") (between <% today %> <% in 14 days %>))  # DB-based
  - ## 📅 Daily Standups
    - <% today %> - Sprint kickoff
    - <% in 3 days %> - Check-in 1
    - <% in 7 days %> - Mid-sprint review
    - <% in 10 days %> - Check-in 2
    - <% in 14 days %> - Sprint retrospective
  - ## 🔄 Next Sprint Preview
    - Planning on <% in 14 days %>
```

---

**Last Updated:** 2025-10-16
**Version:** 1.0.0
**Compatibility:** Logseq DB and File-based graphs

---

*Master dynamic variables to supercharge your Logseq workflow! Start with simple date variables, then level up to complex automated systems. Happy note-taking!* 📝✨
