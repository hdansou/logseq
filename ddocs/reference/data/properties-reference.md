---
title: 'Properties Reference'
status: 🟢 Stable
version: 'Logseq v0.10.9 | Schema v65.16'
quadrant: 'Reference'
platform: 'Desktop + Mobile'
related-docs:
  - '[Schema System](../../explanation/developer/schema-system.md)'
  - '[Query Patterns](../advanced/query-patterns.md)'
  - '[Block Editing](../../tutorials/getting-started/block-editing.md)'
tags: ['properties', 'metadata', 'schema', 'reference']
---

# Properties Reference

Complete reference for properties in Logseq. Properties add structured metadata to pages and blocks, enabling powerful queries, filtering, and organization.

**Quick Reference:** `property-name:: value` or `property-name:: [[Page Link]]`

---

## Table of Contents

1. [Property Basics](#property-basics)
2. [Built-in Properties](#built-in-properties)
3. [Task Properties](#task-properties)
4. [Common Custom Properties](#common-custom-properties)
5. [Property Types](#property-types)
6. [Database Properties](#database-properties)
7. [Property Queries](#property-queries)
8. [Best Practices](#best-practices)

---

## Property Basics

### Syntax

**Basic Property:**

```markdown
property-name:: value
```

**With Page Link:**

```markdown
property-name:: [[Page Name]]
```

**Multiple Values:**

```markdown
property-name:: [[Value 1]], [[Value 2]], [[Value 3]]
```

**Multi-line Property:**

```markdown
property-name::

- Line 1
- Line 2
- Line 3
```

### Property Location

**Block Property:**

```markdown
- Block content here
  property:: value
  another-property:: [[Link]]
```

**Page Property (Frontmatter):**

```markdown
title:: Page Title
tags:: [[Tag1]], [[Tag2]]
status:: active

---

Page content starts here
```

### Naming Rules

**Valid Names:**

```markdown
✅ lowercase-with-dashes
✅ camelCase
✅ snake_case
✅ with-numbers-123

❌ Spaces not allowed
❌ Special chars (@, #, $, etc.)
```

---

## Built-in Properties

### System Properties

**Automatic (Read-Only):**

| Property     | Type      | Description     | Example         |
| ------------ | --------- | --------------- | --------------- |
| `uuid`       | UUID      | Unique block ID | `abc-123-def`   |
| `created-at` | Timestamp | Creation time   | `1737504000000` |
| `updated-at` | Timestamp | Last modified   | `1737590400000` |
| `page`       | Reference | Parent page     | Page ID         |
| `parent`     | Reference | Parent block    | Block ID        |

**Example:**

```markdown
- Block content
  uuid:: abc-123-def-456
  created-at:: 1737504000000
  updated-at:: 1737590400000
```

### Core Properties

**User-Facing:**

| Property                    | Type      | Description       | Example              |
| --------------------------- | --------- | ----------------- | -------------------- |
| `title`                     | Text      | Page title        | `My Project`         |
| `alias`                     | Text/List | Alternative names | `[[Project-A]]`      |
| `tags`                      | List      | Page tags         | `[[Tag1]], [[Tag2]]` |
| `icon`                      | Emoji     | Page icon         | `📊`                 |
| `template`                  | Text      | Template name     | `meeting-notes`      |
| `template-including-parent` | Boolean   | Include parent    | `true`               |

**Example:**

```markdown
title:: Project Alpha
alias:: [[Alpha]], [[Project-A]]
tags:: [[Project]], [[Work]], [[Client/Acme]]
icon:: 📊
```

### Display Properties

**Visual Configuration:**

| Property    | Type    | Description    | Values       |
| ----------- | ------- | -------------- | ------------ |
| `collapsed` | Boolean | Collapse block | `true/false` |
| `heading`   | Number  | Heading level  | `1-6`        |
| `public`    | Boolean | Public page    | `true/false` |

**Example:**

```markdown
- Section header
  heading:: 2
  collapsed:: true
  - Child content (collapsed by default)
```

---

## Task Properties

### Markers

**Task State:**

```markdown
- TODO Task description
  marker:: TODO

States: TODO, DOING, DONE, WAITING, LATER, NOW, CANCELLED
```

### Scheduling

**SCHEDULED:**

```markdown
- TODO Start working on task
  SCHEDULED: <2026-01-22 Wed>
```

**DEADLINE:**

```markdown
- TODO Complete by deadline
  DEADLINE: <2026-01-30 Wed>
```

**Repeating:**

```markdown
- TODO Weekly review
  SCHEDULED: <2026-01-22 Wed +1w>

Intervals:
+1d → Daily
+1w → Weekly
+1m → Monthly
+1y → Yearly
```

### Priority

**Priority Levels:**

```markdown
- TODO [#A] High priority
  priority:: A

- TODO [#B] Medium priority
  priority:: B

- TODO [#C] Low priority
  priority:: C
```

---

## Common Custom Properties

### Project Management

**Project Properties:**

```markdown
project:: [[Project Alpha]]
status:: active
priority:: high
deadline:: [[2026-03-01]]
owner:: [[Person/Sarah]]
team:: [[Person/John]], [[Person/Alice]]
budget:: $50000
progress:: 60%
phase:: implementation
risk-level:: medium
```

**Example:**

```markdown
- Complete API integration
  project:: [[Projects/Mobile App]]
  status:: in-progress
  owner:: [[Person/Sarah]]
  deadline:: [[2026-02-15]]
  priority:: high
  depends-on:: ((design-task-ref))
```

### Work Context

**Context Properties:**

```markdown
context:: [[Work]], [[Office]]
client:: [[Client/Acme Corp]]
department:: [[Engineering]]
category:: [[Development]]
type:: [[Feature]]
environment:: [[Production]]
platform:: [[iOS]], [[Android]]
```

### Time Management

**Time Properties:**

```markdown
time-estimate:: 2h
time-spent:: 1.5h
time-remaining:: 0.5h
duration:: 30min
start-time:: 09:00
end-time:: 17:00
```

### Status Tracking

**Status Properties:**

```markdown
status:: active
stage:: development
phase:: implementation
state:: in-review
progress:: 75%
completion:: [[2026-01-22]]
blocked-by:: [[Issue/123]]
waiting-for:: [[Person/Manager]] approval
```

### Metadata

**Information Properties:**

```markdown
source:: [[Book/Title]]
author:: [[Person/Name]]
date:: [[2026-01-22]]
url:: https://example.com
isbn:: 978-0-123456-78-9
doi:: 10.1000/xyz123
version:: 1.2.3
license:: MIT
```

### Learning & Research

**Research Properties:**

```markdown
topic:: [[Machine Learning]]
difficulty:: intermediate
confidence:: 8/10
resource-type:: [[Article]]
rating:: ⭐⭐⭐⭐⭐
reviewed:: [[2026-01-22]]
key-concept:: true
literature-note:: true
```

### Personal

**Personal Properties:**

```markdown
mood:: 😊
energy:: high
health:: good
sleep:: 7h
exercise:: 30min
gratitude:: [[Thing I'm grateful for]]
habit:: morning-routine
streak:: 7 days
```

---

## Property Types

### Text Properties

**Simple Text:**

```markdown
description:: Simple text value
note:: Some notes here
comment:: Additional comments
```

### Number Properties

**Numeric Values:**

```markdown
count:: 42
score:: 95
percentage:: 75%
amount:: 1000
quantity:: 5
```

### Boolean Properties

**True/False:**

```markdown
completed:: true
archived:: false
important:: true
public:: false
enabled:: true
```

### Date Properties

**Date Values:**

```markdown
date:: [[2026-01-22]]
start-date:: [[2026-01-01]]
end-date:: [[2026-12-31]]
reviewed-date:: [[2026-01-15]]
```

### Link Properties

**Page References:**

```markdown
project:: [[Project Alpha]]
person:: [[Person/Sarah]]
tag:: [[Tag Name]]
related:: [[Related Page]]
```

**Multiple Links:**

```markdown
tags:: [[Tag1]], [[Tag2]], [[Tag3]]
team-members:: [[Sarah]], [[John]], [[Alice]]
references:: [[Book/A]], [[Article/B]]
```

### List Properties

**Multiple Values:**

```markdown
keywords:: programming, tutorial, beginner
tools:: Logseq, Obsidian, Notion
languages:: JavaScript, Python, Go
```

### Block Reference Properties

**Reference Blocks:**

```markdown
decision:: ((decision-block-ref))
depends-on:: ((prerequisite-block-ref))
see-also:: ((related-block-ref))
```

---

## Database Properties

### Schema v65.16

**Built-in Database Properties:**

| Property     | Type | Cardinality | Description     |
| ------------ | ---- | ----------- | --------------- |
| `tags`       | Ref  | Many        | Page tags       |
| `icon`       | Text | One         | Page icon emoji |
| `type`       | Text | One         | Entity type     |
| `status`     | Text | One         | Status value    |
| `priority`   | Text | One         | Priority level  |
| `start-date` | Date | One         | Start date      |
| `end-date`   | Date | One         | End date        |

### Property Classes

**Typed Properties (Database Graphs):**

```markdown
Class: Task
Properties:

- assignee (Person reference)
- due-date (Date)
- priority (Text: high/medium/low)

Example:

- TODO Complete feature
  class:: [[Task]]
  assignee:: [[Person/Sarah]]
  due-date:: [[2026-01-30]]
  priority:: high
```

### Property Constraints

**Database Graph Validation:**

```markdown
Property: priority
Type: Text
Allowed values: high, medium, low
Cardinality: One (single value only)

Invalid:
priority:: high, medium ❌ (multiple values)

Valid:
priority:: high ✅
```

---

## Property Queries

### Basic Property Query

**Has Property:**

```clojure
#+BEGIN_QUERY
{:title "Blocks with 'status' property"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :status)]]}
#+END_QUERY
```

### Property Value Query

**Property Equals Value:**

```clojure
#+BEGIN_QUERY
{:title "Status = 'active'"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :status) "active"]]}
#+END_QUERY
```

### Multiple Property Query

**Multiple Conditions:**

```clojure
#+BEGIN_QUERY
{:title "High Priority Active Items"
 :query [:find (pull ?b [*])
         :where
         [?b :block/properties ?props]
         [(get ?props :status) "active"]
         [(get ?props :priority) "high"]]}
#+END_QUERY
```

### Property with Page Link

**Reference Query:**

```clojure
#+BEGIN_QUERY
{:title "Tasks for Project Alpha"
 :query [:find (pull ?b [*])
         :in $ ?project
         :where
         [?b :block/refs ?p]
         [?p :block/name ?project]]
 :inputs ["projects/alpha"]}
#+END_QUERY
```

### Property Aggregation

**Count by Property:**

```clojure
#+BEGIN_QUERY
{:title "Count by Status"
 :query [:find ?status (count ?b)
         :where
         [?b :block/properties ?props]
         [(get ?props :status) ?status]]
 :view (fn [rows]
         [:table
          [:thead [:tr [:th "Status"] [:th "Count"]]]
          [:tbody
           (for [[status count] rows]
             [:tr [:td status] [:td count]])]])}
#+END_QUERY
```

---

## Best Practices

### Naming Conventions

**Consistency:**

```markdown
✅ Good (consistent):
project:: [[Projects/Alpha]]
start-date:: [[2026-01-01]]
team-member:: [[Person/Sarah]]

❌ Bad (inconsistent):
Project:: [[Projects/Alpha]]
startDate:: [[2026-01-01]]
TeamMember:: [[Person/Sarah]]
```

**Recommendations:**

```markdown
1. Use lowercase with dashes: project-name
2. Be descriptive: owner not o
3. Use prefixes for groups: date-created, date-modified
4. Avoid abbreviations unless common: url ok, prj not ok
```

### Property Organization

**Group Related Properties:**

```markdown
- Task description

  ## Project Info

  project:: [[Project Alpha]]
  owner:: [[Person/Sarah]]
  team:: [[Person/John]], [[Person/Alice]]

  ## Scheduling

  SCHEDULED: <2026-01-22 Wed>
  DEADLINE: <2026-01-30 Wed>

  ## Metadata

  status:: in-progress
  priority:: high
  progress:: 60%
```

### Use Page Links for Values

**Structured References:**

```markdown
✅ Use page links for:

- People: owner:: [[Person/Sarah]]
- Projects: project:: [[Projects/Alpha]]
- Categories: category:: [[Development]]
- Tags: tags:: [[Important]], [[Urgent]]

Benefits:

- Queryable
- Create backlinks
- Build graph connections
```

### Avoid Over-Propertizing

**Balance:**

```markdown
❌ Too many properties:

- Simple note
  author:: [[John]]
  date:: [[2026-01-22]]
  time:: 14:30
  location:: Office
  weather:: Sunny
  mood:: Good
  coffee:: 2 cups

✅ Essential properties only:

- Simple note
  author:: [[John]]
  date:: [[2026-01-22]]
```

### Property Templates

**Standardize with Templates:**

```markdown
template:: task-template

- TODO Task description
  project:: [[Projects/]]
  owner:: [[Person/]]
  DEADLINE: <date>
  priority::
  status:: not-started
```

### Reserved Properties

**Avoid Conflicts:**

```markdown
⚠️ Reserved by Logseq:

- title
- tags
- alias
- public
- icon
- template
- collapsed
- heading

Use with original meaning or prefix custom:
my-title:: instead of title::
```

---

## Property Examples by Use Case

### Project Task

```markdown
- TODO Implement user authentication
  project:: [[Projects/Mobile App]]
  owner:: [[Person/Sarah]]
  team:: [[Person/John]], [[Person/Alice]]
  DEADLINE: <2026-02-15 Thu>
  priority:: [#A]
  status:: in-progress
  progress:: 40%
  depends-on:: ((api-design-ref))
  time-estimate:: 8h
  context:: [[Work]], [[Backend]]
  tags:: [[Feature]], [[Security]]
```

### Meeting Note

```markdown
- Meeting: Project Kickoff
  type:: [[Meeting]]
  date:: [[2026-01-22]]
  time:: 14:00-15:00
  attendees:: [[Person/Sarah]], [[Person/John]]
  project:: [[Projects/Alpha]]
  location:: Conference Room A
  agenda:: Initial planning
  decisions:: ((decision-block-ref))
  action-items:: ((action-items-ref))
  next-meeting:: [[2026-01-29]]
```

### Literature Note

```markdown
- [[Book/Thinking Fast and Slow]]
  type:: [[Literature Note]]
  author:: [[Person/Daniel Kahneman]]
  topic:: [[Psychology]], [[Decision Making]]
  rating:: ⭐⭐⭐⭐⭐
  read-date:: [[2026-01-15]]
  source-type:: Book
  isbn:: 978-0374533557
  key-concepts:: [[System 1]], [[System 2]], [[Cognitive Bias]]
  recommended-by:: [[Person/Alice]]
```

### Personal Journal

```markdown
- Daily Reflection
  date:: [[2026-01-22]]
  mood:: 😊 positive
  energy:: high
  sleep:: 7.5h
  exercise:: 30min morning run
  gratitude:: Team support on project
  learning:: New React patterns
  challenges:: Time management
  wins:: Completed feature ahead of schedule
```

---

## Common Property Sets

### GTD Properties

```markdown
gtd-category:: [[GTD/Next Actions]]
context:: [[Context/Office]]
time-estimate:: 30min
energy:: medium
```

### PARA Properties

```markdown
para-type:: [[Projects]]
area:: [[Areas/Career]]
resource:: [[Resources/Programming]]
archived:: false
```

### Zettelkasten Properties

```markdown
zettel-type:: [[Permanent Note]]
sources:: [[Literature/Book]]
related-zettels:: [[Note/123]], [[Note/456]]
confidence:: 8/10
```

---

## Property Shortcuts

### Quick Property Entry

```markdown
Slash command: /property or /prop

1. Type /property
2. Enter property name
3. Press Tab
4. Enter value
```

### Property Autocomplete

```markdown
Typing property-name:: triggers:

- Autocomplete for known properties
- Recent values for that property
- Page links if property typically uses them
```

---

## Troubleshooting

**Property not appearing in query:**

```markdown
✓ Check spelling (case-sensitive)
✓ Verify syntax: property:: value
✓ Ensure proper indentation
✓ Re-index graph if needed
```

**Multiple values not working:**

```markdown
✓ Use comma separation: [[A]], [[B]]
✓ Or list format:
tags::

- [[Tag1]]
- [[Tag2]]
```

**Property showing as plain text:**

```markdown
❌ Wrong: property: value (single colon)
✅ Right: property:: value (double colon)

❌ Wrong: property ::value (space before ::)
✅ Right: property:: value
```

---

## Next Steps

**Master Properties:**

1. Learn basic syntax
2. Use common properties consistently
3. Create property templates
4. Build queries using properties
5. Develop your property schema

**Related:**

- [Schema System](../../explanation/developer/schema-system.md)
- [Query Patterns](../advanced/query-patterns.md)
- [Task Management](../../how-to/workflows/task-management-workflows.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
