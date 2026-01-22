---
title: 'Templates System'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Reference'
platform: 'Desktop + Mobile'
related-docs:
  - '[Block Properties](./block-properties.md)'
  - '[Page Properties](./page-properties.md)'
  - '[Slash Commands](./slash-commands.md)'
tags: ['templates', 'automation', 'productivity', 'reference']
---

# Templates System

Complete reference for Logseq's template system. Templates let you create reusable block structures, automate repetitive content, and standardize your note-taking workflows.

**What You'll Learn:**

- Template syntax and structure
- Template variables and dynamic content
- Built-in vs custom templates
- Template triggers and shortcuts
- Advanced template patterns

---

## Table of Contents

1. [Template Basics](#template-basics)
2. [Creating Templates](#creating-templates)
3. [Template Variables](#template-variables)
4. [Using Templates](#using-templates)
5. [Advanced Patterns](#advanced-patterns)
6. [Built-in Templates](#built-in-templates)
7. [Troubleshooting](#troubleshooting)

---

## Template Basics

### What is a Template?

A **template** is a reusable block structure that can be inserted anywhere in your graph. Templates can include:

- Static text
- Dynamic variables (dates, times, page names)
- Nested block hierarchies
- Properties and metadata
- Block references

### Template Property

Mark any block as a template using the `template::` property:

```markdown
- Project Template
  template:: Project
  - ## Overview
  - ## Goals
  - ## Tasks
    - TODO First task
  - ## Notes
```

---

## Creating Templates

### Basic Template Structure

**Step 1: Create template page** (recommended):

```markdown
- Meeting Notes Template
  template:: meeting
  - **Date:** <% today %>
  - **Attendees:**
  - **Topics:**
    -
  - **Action Items:**
    - TODO
  - **Next Meeting:** <% tomorrow %>
```

**Step 2: Use the template:**

Type `/template` → Select "meeting"

### Inline Templates

Templates can exist anywhere, not just on dedicated pages:

```markdown
- Daily Routine
  template:: routine
  - [ ] Morning meditation
  - [ ] Review calendar
  - [ ] Check email
  - [ ] Plan top 3 tasks
```

### Template Naming

**Good Names:**

- ✅ `meeting` - Short, memorable
- ✅ `weekly-review` - Descriptive with dash
- ✅ `standup` - Single word

**Avoid:**

- ❌ `Meeting Notes Template` - Too long to type
- ❌ `template1` - Not descriptive
- ❌ `🔥 Hot Template` - Emoji may cause issues

---

## Template Variables

### Date & Time Variables

| Variable             | Output            | Example                   |
| -------------------- | ----------------- | ------------------------- |
| `<% today %>`        | Current date      | `[[2026-01-21]]`          |
| `<% tomorrow %>`     | Tomorrow's date   | `[[2026-01-22]]`          |
| `<% yesterday %>`    | Yesterday's date  | `[[2026-01-20]]`          |
| `<% time %>`         | Current time      | `14:30`                   |
| `<% current page %>` | Current page name | `[[Projects/My Project]]` |

### Date Offset Variables

```markdown
<% 3 days from now %> → [[2026-01-24]]
<% 1 week from now %> → [[2026-01-28]]
<% 2 months from now %> → [[2026-03-21]]
<% 1 year from now %> → [[2027-01-21]]

<% 3 days ago %> → [[2026-01-18]]
<% 1 week ago %> → [[2026-01-14]]
```

### Page Context Variables

| Variable             | Output                 | Usage                    |
| -------------------- | ---------------------- | ------------------------ |
| `<% current page %>` | Current page name      | Reference back to source |
| `<% page %>`         | Alias for current page | Same as above            |

### Custom Date Formats

**Configuration:** `config.edn`

```clojure
:journal/page-title-format "yyyy-MM-dd"
:template/date-format "MMM do, yyyy"
```

**Usage:**

```markdown
<% today %> → [[2026-01-21]] (journal format)
<% today :format long %> → January 21st, 2026 (long format)
```

---

## Using Templates

### Method 1: Slash Command

1. Type `/template` or `/tmpl`
2. Search for template name
3. Press Enter

**Quick insert:** `/template meeting` directly

### Method 2: Keyboard Shortcut

Set custom shortcut:

1. Settings → Keymap
2. Search "Insert template"
3. Assign shortcut (e.g., `⌘ ⇧ T`)

### Method 3: Block Properties

Manually reference template:

```markdown
- template:: meeting
```

Then use block menu (right-click) → "Apply template"

---

## Advanced Patterns

### Hierarchical Templates

**Nested structure:**

```markdown
- Project Template
  template:: project
  - ## 📋 Overview
    - **Status:** #status/planning
    - **Owner:**
    - **Due Date:** <% 2 weeks from now %>
  - ## 🎯 Goals
    -
  - ## ✅ Tasks
    - TODO [[<% current page %>]] Set up project structure
    - TODO Schedule kickoff meeting
  - ## 📝 Notes
    - ### Week of <% today %>
  - ## 🔗 Related
    - [[Projects]]
```

### Templates with Block References

**Reference other content:**

```markdown
- Standup Template
  template:: standup
  - **Yesterday:**
    - {{embed ((previous-standup))}}
  - **Today:**
    -
  - **Blockers:**
    -
  - **Links:**
    - [[Team/Standups]]
    - [[Sprint/<% current page %>]]
```

### Dynamic Task Templates

**Create recurring tasks:**

```markdown
- Weekly Review Template
  template:: weekly-review
  - ## Week of <% today %>
  - [ ] Review inbox → [[Inbox]]
  - [ ] Process notes → [[To Process]]
  - [ ] Update goals → [[Goals/<% current page %>]]
  - [ ] Schedule next week
  - **Insights:**
    -
```

### Templates with Properties

**Structured metadata:**

```markdown
- Book Notes Template
  template:: book
  - **Title:**
    title::
  - **Author:**
    author::
  - **Rating:**
    rating::
  - **Status:** #status/reading
    status:: reading
  - **Started:** <% today %>
    started:: <% today %>
  - **Finished:**
    finished::
  - ## Summary
  - ## Key Quotes
  - ## Reflections
```

---

## Built-in Templates

### Meeting Notes

```markdown
- Meeting Template
  template:: meeting
  - **Date:** <% today %>
  - **Time:** <% time %>
  - **Attendees:**
    -
  - **Agenda:**
    -
  - **Discussion:**
    -
  - **Decisions:**
    -
  - **Action Items:**
    - TODO
  - **Next Steps:**
    -
```

### Daily Note

```markdown
- Daily Note Template
  template:: daily
  - ## 🌅 Morning
    - **Intention:**
    - **Top 3 Priorities:**
      - 1.
      - 2.
      - 3.
  - ## 📝 Notes
    -
  - ## ✅ Completed
    -
  - ## 🌙 Evening Reflection
    - **Wins:**
    - **Learnings:**
    - **Tomorrow:**
```

### Research Paper

```markdown
- Research Paper Template
  template:: paper
  - **Title:**
  - **Authors:**
  - **Year:**
  - **DOI/URL:**
  - **Tags:**
  - ## Abstract
  - ## Key Findings
    -
  - ## Methodology
    -
  - ## Conclusions
    -
  - ## Notes
    -
  - ## Related Work
    -
```

### Project Brief

```markdown
- Project Brief Template
  template:: brief
  - # Project: [Name]
  - **Status:** #status/planning
  - **Owner:**
  - **Start Date:** <% today %>
  - **Target Date:** <% 4 weeks from now %>
  - ## Problem Statement
    -
  - ## Objectives
    -
  - ## Success Criteria
    -
  - ## Scope
    - ### In Scope
      -
    - ### Out of Scope
      -
  - ## Stakeholders
    -
  - ## Risks
    -
  - ## Timeline
    -
```

### Code Snippet

````markdown
- Code Snippet Template
  template:: snippet
  - **Language:**
  - **Description:**
  - **Tags:**
  - ```javascript
    // Your code here
    ```
  - **Usage:**
    -
  - **Notes:**
    -
````

---

## Template Library Organization

### Dedicated Templates Page

Create `[[Templates]]` page:

```markdown
- # Templates Library

- ## 📅 Daily & Planning

  - [[template:: daily]]
  - [[template:: weekly-review]]
  - [[template:: monthly-review]]

- ## 💼 Work & Projects

  - [[template:: meeting]]
  - [[template:: project]]
  - [[template:: standup]]
  - [[template:: retrospective]]

- ## 📚 Learning & Research

  - [[template:: book]]
  - [[template:: paper]]
  - [[template:: course]]

- ## 💻 Technical
  - [[template:: bug-report]]
  - [[template:: feature-request]]
  - [[template:: snippet]]
```

### Template Namespace

Organize with namespace:

```markdown
templates/meeting
templates/project
templates/daily
templates/book
```

**Usage:** `/template templates/meeting`

---

## Template Best Practices

### Design Principles

**1. Keep it Simple**

```markdown
❌ Too complex:

- Meeting Template (with 20 nested sections)

✅ Simple:

- Meeting Template
  - Attendees:
  - Topics:
  - Actions:
```

**2. Use Clear Headings**

```markdown
✅ Good:

- ## 🎯 Goals
- ## ✅ Tasks
- ## 📝 Notes

❌ Unclear:

- Section 1
- Section 2
- Section 3
```

**3. Include Context**

```markdown
✅ With context:

- **Project:** [[<% current page %>]]
- **Date:** <% today %>

❌ Without:

- Project:
- Date:
```

**4. Provide Examples**

```markdown
- **Rating:** (1-5 stars)
  rating::

- **Status:** (e.g., #status/active)
  status::
```

### Versioning Templates

Track template changes:

```markdown
- Project Template v2
  template:: project
  version:: 2.0
  updated:: [[2026-01-21]]
  changelog:: Added risk assessment section
  - ## Overview
    ...
```

---

## Programmatic Templates

### Template Plugin API

**Create templates via plugin:**

```typescript
// Register template
await logseq.Editor.registerTemplate('meeting', {
  content: [
    '**Date:** <% today %>',
    '**Attendees:**',
    '**Topics:**',
    '  - ',
    '**Action Items:**',
    '  - TODO ',
  ].join('\n'),
})

// Insert template
await logseq.Editor.insertAtCursor('<% template: meeting %>')
```

### Dynamic Template Generation

**Create templates from blocks:**

```typescript
async function createTemplateFromSelection() {
  const blocks = await logseq.Editor.getSelectedBlocks()
  const content = blocks.map((b) => b.content).join('\n')

  const name = await logseq.UI.showInputDialog({
    title: 'Template Name',
    placeholder: 'Enter template name',
  })

  await logseq.Editor.createPage(name, {
    properties: { template: name },
  })

  await logseq.Editor.insertBlock(name, content)
}
```

---

## Troubleshooting

### Template Not Found

**Problem:** Template doesn't appear in `/template` list

**Solutions:**

1. Check `template::` property exists
2. Reload graph (⌘ R)
3. Check spelling of template name
4. Ensure block is not nested too deeply

### Variables Not Expanding

**Problem:** `<% today %>` appears as literal text

**Solutions:**

1. Check syntax: `<% today %>` (spaces matter)
2. Ensure using template insertion (not copy-paste)
3. Update Logseq to latest version
4. Check date format in config.edn

### Template Appears Garbled

**Problem:** Block structure broken after insertion

**Solutions:**

1. Check indentation in template source
2. Avoid mixing tabs and spaces
3. Recreate template with consistent indentation
4. Use `-` for blocks, not `*` or custom markers

### Dynamic Dates Wrong

**Problem:** Dates offset incorrectly

**Solutions:**

```markdown
❌ Wrong: <% 1 week from today %>
✅ Right: <% 1 week from now %>

❌ Wrong: <% 3 day ago %>
✅ Right: <% 3 days ago %>
```

---

## Template Recipes

### 1. Daily Habit Tracker

```markdown
- Habit Tracker
  template:: habits
  - [[<% today %>]]
  - [ ] 🏃 Exercise (30 min)
  - [ ] 📖 Read (20 min)
  - [ ] 🧘 Meditate (10 min)
  - [ ] 💧 Drink 8 glasses of water
  - [ ] 😴 Sleep before 11 PM
  - **Notes:**
```

### 2. Sprint Retrospective

```markdown
- Sprint Retro
  template:: retro
  - # Sprint <% current page %> Retrospective
  - **Date:** <% today %>
  - **Sprint Period:**
  - ## 😊 What Went Well
    -
  - ## 😟 What Didn't Go Well
    -
  - ## 💡 Ideas for Improvement
    -
  - ## 🎬 Action Items
    - TODO
```

### 3. Decision Log

```markdown
- Decision Template
  template:: decision
  - # Decision: [Title]
  - **Date:** <% today %>
  - **Decision Maker:**
  - **Status:** #decision/proposed
  - ## Context
    -
  - ## Options Considered
    - ### Option 1:
      - Pros:
      - Cons:
    - ### Option 2:
      - Pros:
      - Cons:
  - ## Decision
    -
  - ## Consequences
    -
```

### 4. Contact Note

```markdown
- Contact Template
  template:: contact
  - **Name:**
  - **Role:**
  - **Company:**
  - **Email:**
  - **Phone:**
  - **LinkedIn:**
  - **Last Contact:** <% today %>
  - **Next Follow-up:** <% 1 week from now %>
  - ## Notes
    -
  - ## Interactions
    - [[<% today %>]] - First contact
```

---

## Next Steps

**Start Using Templates:**

1. Create your first template (start simple)
2. Test with `/template`
3. Refine based on usage
4. Build template library
5. Share templates with team

**Related:**

- [Slash Commands](./slash-commands.md)
- [Block Properties](./block-properties.md)
- [Productivity Tips](../../how-to/advanced/productivity-tips.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
