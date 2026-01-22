---
title: 'Basic Note-Taking'
status: 🟢
since-version: 'N/A'
quadrant: 'Tutorial'
last-verified: '2026-01-11'
platform: 'All'
related-docs:
  - 'tutorials/getting-started/02-first-graph.md'
  - 'tutorials/getting-started/04-linking-pages.md'
  - 'reference/syntax/markdown.md'
  - 'reference/syntax/block-editing.md'
---

# Basic Note-Taking

Learn the fundamentals of editing blocks, formatting text, and organizing your thoughts in Logseq.

**Time required:** 15-20 minutes

## Block-Based Editing

Unlike traditional note apps where you edit entire pages, Logseq works with **blocks** — individual bullet points that can be manipulated independently.

### Creating and Editing Blocks

#### Basic Block Operations

**Create a new block:**

- Press `Enter` at the end of current block
- Or click the `+` button that appears when hovering

**Edit a block:**

- Click on it to focus
- Start typing to replace selection
- Use arrow keys to navigate

**Delete a block:**

- Select all text (`Ctrl/Cmd + A`) and press `Backspace`
- Or use `Ctrl/Cmd + Shift + Backspace` to delete immediately

**Practice Exercise:**

```
- Try creating a block here
- Press Enter to make another
- Now try deleting this one
```

### Block Hierarchy

Blocks can be nested to create hierarchy:

**Indent (make child):**

- Press `Tab` at start of block
- Or click and drag the bullet point to the right

**Outdent (make parent/sibling):**

- Press `Shift + Tab`
- Or drag the bullet point to the left

**Example Structure:**

```
- Project: Website Redesign
  - Research Phase
    - Competitor analysis
    - User interviews
    - Design trends
  - Design Phase
    - Wireframes
    - Mockups
    - Prototypes
  - Development Phase
```

### Multi-Block Selection

**Select multiple blocks:**

1. `Shift + Click` to select a range
2. `Ctrl/Cmd + Click` to select non-contiguous blocks

**Bulk operations:**

- `Ctrl/Cmd + C`: Copy
- `Ctrl/Cmd + X`: Cut
- `Ctrl/Cmd + V`: Paste
- Delete: Remove all selected blocks

## Text Formatting

Logseq supports Markdown formatting syntax:

### Basic Formatting

```markdown
- **Bold text** or **bold text**
- _Italic text_ or _italic text_
- **_Bold and italic_**
- ~~Strikethrough~~
- ==Highlighted text==
- `Inline code`
```

**Renders as:**

- **Bold text** or **bold text**
- _Italic text_ or _italic text_
- **_Bold and italic_**
- ~~Strikethrough~~
- ==Highlighted text==
- `Inline code`

### Keyboard Shortcuts

| Action        | Windows/Linux      | macOS             |
| ------------- | ------------------ | ----------------- |
| Bold          | `Ctrl + B`         | `Cmd + B`         |
| Italic        | `Ctrl + I`         | `Cmd + I`         |
| Highlight     | `Ctrl + H`         | `Cmd + H`         |
| Strikethrough | `Ctrl + Shift + S` | `Cmd + Shift + S` |
| Code          | `Ctrl + Shift + C` | `Cmd + Shift + C` |

## Slash Commands

Type `/` to open the command menu for quick insertions:

### Common Commands

- `/page` — Create/link to a page
- `/block` — Embed a block reference
- `/TODO` — Create a task
- `/DOING` — Create an in-progress task
- `/DONE` — Create a completed task
- `/code` — Insert a code block
- `/query` — Insert a query
- `/image` — Embed an image
- `/drawing` — Create a drawing (whiteboard)

**Try it:**

```
- Type / and see what options appear
```

## Block References and Embeds

### Block References

Every block has a unique ID. You can reference any block from anywhere.

**Create a block reference:**

1. Type `((`
2. Start typing text from the target block
3. Select the block from the dropdown
4. The reference appears as `((block-id))`

**Example:**

```
- Original block: [[Key insight]] about productivity
- Reference it elsewhere: ((above-block))
  - The referenced block appears inline
```

**Use cases:**

- Reusing content without duplication
- Creating dynamic templates
- Building connected notes

### Block Embeds

Similar to references, but shows the full block content including children:

**Syntax:** `{{embed ((block-id))}}`

**Difference:**

- **Reference `(())`:** Shows just the text
- **Embed `{{embed}}`:** Shows text + all child blocks

## Block Properties

Add metadata to blocks using properties:

**Syntax:**

```
- Block content
  property-name:: property-value
```

**Examples:**

```
- Meeting with Client
  date:: [[Jan 15th, 2026]]
  attendees:: [[John]], [[Sarah]]
  status:: completed
```

Properties enable:

- Filtering blocks in queries
- Creating structured data
- Building databases of information

## Common Block Types

### Tasks

```
- TODO Write documentation
- DOING Review pull requests
- DONE Deploy to production
```

**Shortcuts:**

- `Ctrl/Cmd + Enter`: Toggle TODO/DONE
- `/TODO`, `/DOING`, `/DONE`: Quick create

### Headings

Logseq auto-detects headings:

```
- # Heading 1
- ## Heading 2
- ### Heading 3
```

### Lists

**Unordered (bullets):**

```
- Item 1
- Item 2
- Item 3
```

**Ordered (numbered):**

```
- 1. First item
- 2. Second item
- 3. Third item
```

### Code Blocks

**Inline code:** `` `code here` ``

**Code block:**

````
```javascript
function hello() {
  console.log("Hello, Logseq!");
}
```
````

### Quotes

```
> This is a quote
> It can span multiple lines
```

### Math (LaTeX)

**Inline:** `$E = mc^2$`

**Block:**

```
$$
\int_0^\infty e^{-x^2} dx = \frac{\sqrt{\pi}}{2}
$$
```

## Editing Best Practices

### 1. One Idea Per Block

```
✅ Good:
- Atomic Habits emphasizes small improvements
- Improving 1% daily leads to 37x better results in a year

❌ Avoid:
- Atomic Habits emphasizes small improvements and improving 1% daily leads to 37x better results in a year and this is similar to the compound effect
```

### 2. Use Hierarchy Wisely

```
✅ Good:
- Project Name
  - Goals
  - Timeline
  - Resources

❌ Avoid: (Too deep)
- Project
  - Phase 1
    - Week 1
      - Day 1
        - Morning
          - Task 1
```

### 3. Leverage Properties for Structure

```
✅ Good:
- [[Book]] [[Atomic Habits]]
  author:: [[James Clear]]
  year:: 2018
  rating:: 5

❌ Avoid:
- Book: Atomic Habits by James Clear, 2018, rating 5/5
```

## Practical Exercise

Create a project note with everything you've learned:

```
- # Project: Learn Logseq
  status:: in-progress
  start-date:: [[Jan 11th, 2026]]

  - ## Goals
    - Understand block-based editing
    - Master formatting
    - Learn linking (next tutorial)

  - ## Progress
    - DONE Complete installation
    - DONE Create first graph
    - DOING Learn basic editing
    - TODO Master linking

  - ## Key Insights
    - Blocks are the atomic unit of knowledge
    - Hierarchy creates structure
    - Properties add metadata

  - ## Resources
    - [Official Docs](https://docs.logseq.com)
    - [[Logseq]] community forum
```

## Keyboard Shortcuts Summary

| Action          | Shortcut (Win/Linux) | Shortcut (macOS) |
| --------------- | -------------------- | ---------------- |
| New block       | `Enter`              | `Enter`          |
| Indent          | `Tab`                | `Tab`            |
| Outdent         | `Shift + Tab`        | `Shift + Tab`    |
| Bold            | `Ctrl + B`           | `Cmd + B`        |
| Italic          | `Ctrl + I`           | `Cmd + I`        |
| Toggle TODO     | `Ctrl + Enter`       | `Cmd + Enter`    |
| Slash command   | `/`                  | `/`              |
| Block reference | `((`                 | `((`             |
| Page link       | `[[`                 | `[[`             |
| Search          | `Ctrl + K`           | `Cmd + K`        |

## What You've Learned

✅ Create, edit, and delete blocks  
✅ Use block hierarchy (indent/outdent)  
✅ Format text with Markdown  
✅ Use slash commands  
✅ Add block properties  
✅ Create different block types (tasks, code, quotes)

## Next Steps

Now that you can create and format content, let's learn how to connect your notes:

👉 **Next:** [Linking Pages](04-linking-pages.md)

## Further Reading

- [Markdown Reference](../../reference/syntax/markdown.md) - Complete formatting guide
- [Block Editing Reference](../../reference/syntax/block-editing.md) - Advanced editing techniques
- [Code Blocks Reference](../../reference/syntax/code-blocks.md) - Working with code
