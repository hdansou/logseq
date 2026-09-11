---
title: 'Basic Note-Taking'
status: 🟢
since-version: 'N/A'
quadrant: 'Tutorial'
last-verified: '2026-09-11'
verified-against: 'be800f1711'
review-status: audited
platform: 'All'
related-docs:
  - 'tutorials/getting-started/02-first-graph.md'
  - 'tutorials/getting-started/04-linking-pages.md'
  - 'reference/formatting/markdown-syntax.md'
  - 'explanation/concepts/block-editing-philosophy.md'
---

# Basic Note-Taking

> **Last revised:** 2026-09-11

Learn the fundamentals of editing blocks, formatting text, and organizing your thoughts in Logseq.

**Time required:** 15-20 minutes

Shortcuts below are written `Ctrl/Cmd`: use `Cmd` on macOS and `Ctrl` on Windows and Linux. They are cross-checked against the [Keyboard Shortcuts Reference](../../reference/configuration/keyboard-shortcuts.md) and [`src/main/frontend/modules/shortcut/config.cljs`](../../../../../src/main/frontend/modules/shortcut/config.cljs) (cited below as `config.cljs`).

## Block-Based Editing

Unlike traditional note apps where you edit entire pages, Logseq works with **blocks** — individual bullet points that can be manipulated independently.

### Creating and Editing Blocks

#### Basic Block Operations

**Create a new block:**

- Press `Enter` at the end of a block to create a new sibling block below (`:editor/new-block`, [`config.cljs:107`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- Click the empty row below the last block on a page to add a new block at the end ([`src/main/frontend/components/page.cljs:55-80`](../../../../../src/main/frontend/components/page.cljs))

**Edit a block:**

- Click on its text to start editing
- Use the `Up` and `Down` arrow keys to move the cursor through blocks ([`config.cljs:170`](../../../../../src/main/frontend/modules/shortcut/config.cljs), `:173`)
- Press `Esc` to stop editing ([`src/main/frontend/components/editor.cljs:754-757`](../../../../../src/main/frontend/components/editor.cljs))

**Delete a block:**

- Clear the block's text, then press `Backspace` with the cursor at the start of the empty block ([`src/main/frontend/handler/editor.cljs:3223-3226`](../../../../../src/main/frontend/handler/editor.cljs), handled by `delete-block-when-zero-pos!` at `:3163`)
- Or select the whole block and delete it: while editing, `Ctrl/Cmd + A` selects the block's text, and pressing it again selects the block itself ([`src/main/frontend/handler/editor.cljs:4234-4245`](../../../../../src/main/frontend/handler/editor.cljs), [`config.cljs:247`](../../../../../src/main/frontend/modules/shortcut/config.cljs)). Then press `Backspace` or `Delete` ([`config.cljs:208`](../../../../../src/main/frontend/modules/shortcut/config.cljs))

**Practice Exercise:**

```
- Try creating a block here
- Press Enter to make another
- Now try deleting this one
```

### Block Hierarchy

Blocks can be nested to create hierarchy:

**Indent (make child):**

- Press `Tab` ([`config.cljs:220`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- Or drag the block by its bullet and drop it well to the right of the target block. A drop more than 50px from the target's left edge nests it ([`src/main/frontend/components/block.cljs:3946-3953`](../../../../../src/main/frontend/components/block.cljs))

**Outdent (make parent/sibling):**

- Press `Shift + Tab` ([`config.cljs:223`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- Or drag the block and drop it aligned with the block it should follow, which makes it a sibling ([`src/main/frontend/components/block.cljs:3948-3956`](../../../../../src/main/frontend/components/block.cljs))

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

1. Click a block, then `Shift + Click` another block to select the range between them ([`src/main/frontend/components/block.cljs:2732-2741`](../../../../../src/main/frontend/components/block.cljs))
2. `Ctrl/Cmd + Click` to add or remove individual blocks ([`src/main/frontend/components/block.cljs:2722-2730`](../../../../../src/main/frontend/components/block.cljs); `Cmd` on macOS, `Ctrl` elsewhere, [`src/main/frontend/util.cljc:1071-1074`](../../../../../src/main/frontend/util.cljc))
3. `Ctrl/Cmd + Shift + A` selects all blocks ([`config.cljs:244`](../../../../../src/main/frontend/modules/shortcut/config.cljs))

**Bulk operations:**

- `Ctrl/Cmd + C`: Copy ([`config.cljs:226`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- `Ctrl/Cmd + X`: Cut ([`config.cljs:232`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- `Ctrl/Cmd + V`: Paste
- `Backspace` or `Delete`: Remove all selected blocks ([`config.cljs:208`](../../../../../src/main/frontend/modules/shortcut/config.cljs))

## Text Formatting

Logseq supports Markdown inline formatting syntax. Block text is parsed as Markdown ([`src/main/frontend/handler/db_based/editor.cljs:110`](../../../../../src/main/frontend/handler/db_based/editor.cljs)) and rendered by [`src/main/frontend/components/block.cljs`](../../../../../src/main/frontend/components/block.cljs) (strikethrough and highlight at `:1937-1938`, inline code at `:2016`).

### Basic Formatting

```markdown
- **Bold text**
- _Italic text_
- **_Bold and italic_**
- ~~Strikethrough~~
- ==Highlighted text==
- `Inline code`
```

**Renders as:**

- **Bold text**
- _Italic text_
- **_Bold and italic_**
- ~~Strikethrough~~
- ==Highlighted text==
- `Inline code`

### Keyboard Shortcuts

| Action        | Windows/Linux          | macOS                 | Source |
| ------------- | ---------------------- | --------------------- | ------ |
| Bold          | `Ctrl + B`             | `Cmd + B`             | `config.cljs:119` |
| Italic        | `Ctrl + I`             | `Cmd + I`             | `config.cljs:122` |
| Highlight     | `Ctrl + Shift + H`     | `Cmd + Shift + H`     | `config.cljs:125` |
| Strikethrough | `Ctrl + Shift + S`     | `Cmd + Shift + S`     | `config.cljs:128` |

## Slash Commands

Type `/` to open the command menu for quick insertions ([`src/main/frontend/handler/editor.cljs:2270-2272`](../../../../../src/main/frontend/handler/editor.cljs)):

### Common Commands

All in [`src/main/frontend/commands.cljs`](../../../../../src/main/frontend/commands.cljs):

- `/Node reference` — Insert `[[]]` and open the page search (`:227-232`)
- `/Node embed` — Search for a block. The block you pick is embedded as a new block below the current one (`:233-236`; [`src/main/frontend/components/editor.cljs:289-307`](../../../../../src/main/frontend/components/editor.cljs))
- `/Code block` — Turn the block into a code block (`:246-249`)
- `/Query` — Insert a query (`:325`)
- `/Link` — Create a HTTP link (`:239`)
- `/Image link` — Create a link to an image (`:240`)
- `/Upload an asset` — Upload a file (image, PDF, etc.) (`:335-338`)
- `/Quote` — Turn the block into a quote block (`:250-253`)
- `/Math block` — Turn the block into a LaTeX math block (`:254-257`)
- `/Heading 1` … `/Heading 6` and `/Normal text` — Set or clear the block's heading (`:198-209`)
- `/Add property` — Add a property to the block (`:354-358`)

> **Removed:** "Use `((` to create block references". Typing `((` only shows the warning "To reference a node, please use `[[]]`." ([`src/main/frontend/handler/editor.cljs:1949-1952`](../../../../../src/main/frontend/handler/editor.cljs); `src/resources/dicts/en.edn:503`). See [Block References](#block-references) below.

> **Removed:** `{{embed [[Page Name]]}}` for page embeds. Any `{{embed …}}` macro now renders the warning "{{embed}} is deprecated. Use '/Node embed' command instead." ([`src/main/frontend/components/block.cljs:1917-1918`](../../../../../src/main/frontend/components/block.cljs); `en.edn:157`).

Six task statuses are always available as slash commands under **TASK STATUS**: Backlog, Todo, Doing, In Review, Done and Canceled ([`src/main/frontend/commands.cljs:150-165`](../../../../../src/main/frontend/commands.cljs); values in [`deps/db/src/logseq/db/frontend/property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)). **TASK DATE** has Deadline and Scheduled (`commands.cljs:265-276`).

**Try it:**

```
- Type / and see what options appear
```

## Block References and Embeds

### Block References

Every block has a unique ID. You can reference any block from anywhere. A block reference uses the same brackets as a page link, with the block's UUID inside: `[[<block-uuid>]]` ([`src/main/frontend/util/ref.cljs:5-7`](../../../../../src/main/frontend/util/ref.cljs)).

**Create a block reference:**

1. Right-click the target block's bullet and choose **Copy block ref** ([`src/main/frontend/components/container.cljs:392-402`](../../../../../src/main/frontend/components/container.cljs), [`src/main/frontend/components/content.cljs:288-292`](../../../../../src/main/frontend/components/content.cljs); `en.edn:116`). Or, while editing the target block with no text selected, press `Ctrl/Cmd + C` ([`src/main/frontend/handler/editor.cljs:3662-3666`](../../../../../src/main/frontend/handler/editor.cljs), `:3680`)
2. Click into the block where you want the reference
3. Paste with `Ctrl/Cmd + V`. The text is `[[<block-uuid>]]`, and Logseq resolves it to the referenced block ([`src/main/frontend/components/block.cljs:1320-1337`](../../../../../src/main/frontend/components/block.cljs))

**Example:**

```
- Original block: [[Key insight]] about productivity
- Reference it elsewhere: [[6650e2f3-1b2c-4d5e-9f00-0a1b2c3d4e5f]]
```

> **Removed:** `((block-id))` block references. `((uuid))` text renders literally ([`src/main/frontend/components/block.cljs:1556-1557`](../../../../../src/main/frontend/components/block.cljs)).

**Use cases:**

- Reusing content without duplication
- Building connected notes

> **Removed:** "Creating dynamic templates" as a block-reference use case. No source ties block references to templates.

### Block Embeds

Embeds show a block in place, as a block of its own:

- Type `/Node embed`, search for the block, and pick it ([`src/main/frontend/components/editor.cljs:289-307`](../../../../../src/main/frontend/components/editor.cljs))
- Or, while editing the block you want to embed, press `Ctrl/Cmd + Shift + E` to copy it as an embed ([`config.cljs:158`](../../../../../src/main/frontend/modules/shortcut/config.cljs); [`src/main/frontend/handler/editor.cljs:3646-3660`](../../../../../src/main/frontend/handler/editor.cljs)). Then, while editing another block in the same graph, paste: Logseq inserts a new block below it that embeds the copied block ([`src/main/frontend/handler/paste.cljs:219-232`](../../../../../src/main/frontend/handler/paste.cljs))

> **Removed:** `{{embed ((block-id))}}` syntax ([`src/main/frontend/components/block.cljs:1917-1918`](../../../../../src/main/frontend/components/block.cljs)).

**Difference:**

- **Reference `[[<block-uuid>]]`:** Sits inline in your text
- **Embed:** Adds a separate block that renders the embedded block ([`src/main/frontend/components/block.cljs:4107-4126`](../../../../../src/main/frontend/components/block.cljs))

> **Unverified:** That an embed shows the embedded block's children. The render path swaps in the linked block, but this audit didn't trace child rendering.

## Block Properties

Properties add metadata to blocks. In a DB graph you add them through the property UI. Typing `property-name:: property-value` into a block does not create a property.

**Add a property:**

1. While editing a block, type `/Add property` ([`src/main/frontend/commands.cljs:354-358`](../../../../../src/main/frontend/commands.cljs)) or press `Cmd + P` on macOS / `Ctrl + Alt + P` on Windows and Linux ([`config.cljs:259`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
2. Pick or create a property and set its value

**Example:** For a block `Meeting with Client`, add a **date** property set to Jan 15th, 2026 and an **attendees** property pointing at the pages John and Sarah. Available property types include `date` and `node` (a link to a page or block) ([`deps/db/src/logseq/db/frontend/property/type.cljs:21`](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs)).

> **Removed:** `property-name:: property-value` syntax. It is file-graph syntax.

Properties enable:

- Filtering blocks in queries
- Creating structured data
- Building databases of information

## Common Block Types

### Tasks

A task is a block with a **Status**. The six statuses are Backlog, Todo, Doing, In Review, Done and Canceled ([`deps/db/src/logseq/db/frontend/property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)). Setting a status also tags the block `#Task` ([`deps/outliner/src/logseq/outliner/property.cljs:108-116`](../../../../../deps/outliner/src/logseq/outliner/property.cljs), `:204-206`).

**Create a task:**

- In a block, type `/Todo`, or any other status such as `/Doing` or `/Done`
- Or press `Ctrl/Cmd + Enter`

**Shortcuts:**

- `Ctrl/Cmd + Enter`: Cycle the status: no status → Todo → Doing → Done → no status. A block in Backlog, In Review or Canceled goes to Todo ([`src/main/frontend/handler/editor.cljs:819-832`](../../../../../src/main/frontend/handler/editor.cljs), [`config.cljs:167`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- Use slash commands to set a specific status directly

> **Removed:** `TODO` / `DOING` / `DONE` text markers. They are file-graph syntax. In a DB graph the status is a property value, and `src/main`, `deps/outliner` and `deps/db` have no code that turns typed `TODO` text into a status.

### Headings

Start a block with one to six `#` characters and a space. When the block is saved, Logseq removes the `#` characters and sets the block's heading level ([`src/main/frontend/handler/db_based/editor.cljs:47-52`](../../../../../src/main/frontend/handler/db_based/editor.cljs), `:105-108`, `:125-126`). The `/Heading 1` … `/Heading 6` commands do the same.

```
- # Heading 1
- ## Heading 2
- ### Heading 3
```

### Lists

**Unordered (bullets):** Every block is already a bullet.

```
- Item 1
- Item 2
- Item 3
```

**Ordered (numbered):** In an empty block, type `1. ` (one, period, space) to turn the block into a numbered list item ([`src/main/frontend/handler/editor.cljs:2265-2268`](../../../../../src/main/frontend/handler/editor.cljs)). You can also use `/Number list`, or `/Number children` to number a block's children ([`src/main/frontend/commands.cljs:306-316`](../../../../../src/main/frontend/commands.cljs)).

> **Removed:** Typing `1.`, `2.`, `3.` in front of each item. The `1. ` shortcut only fires on an empty block and switches it to numbered-list mode.

### Code Blocks

**Inline code:** `` `code here` ``

**Code block:** Type `/Code block` ([`src/main/frontend/commands.cljs:246-249`](../../../../../src/main/frontend/commands.cljs)). Fenced code is also rendered as a code block ([`src/main/frontend/components/block.cljs:5116`](../../../../../src/main/frontend/components/block.cljs)):

````
```javascript
function hello() {
  console.log("Hello, Logseq!");
}
```
````

### Quotes

Type `/Quote` to make the block a quote ([`src/main/frontend/commands.cljs:140-143`](../../../../../src/main/frontend/commands.cljs)). Markdown quote syntax is also rendered ([`src/main/frontend/components/block.cljs:5031`](../../../../../src/main/frontend/components/block.cljs)):

```
> This is a quote
> It can span multiple lines
```

### Math (LaTeX)

**Inline:** `$E = mc^2$` ([`src/main/frontend/components/block.cljs:1997`](../../../../../src/main/frontend/components/block.cljs))

**Block:** Type `/Math block` ([`src/main/frontend/commands.cljs:254-257`](../../../../../src/main/frontend/commands.cljs)). Displayed math is also rendered ([`src/main/frontend/components/block.cljs:5099`](../../../../../src/main/frontend/components/block.cljs)):

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

✅ **Good:** A block `[[Atomic Habits]] #Book` with **author** (James Clear), **year** (2018) and **rating** (5) properties added through `/Add property`.

❌ **Avoid:**

```
- Book: Atomic Habits by James Clear, 2018, rating 5/5
```

## Practical Exercise

Create a project note with everything you've learned:

1. Type this outline, using `Tab` to indent each level:

   ```
   - # Project: Learn Logseq
     - ## Goals
       - Understand block-based editing
       - Master formatting
       - Learn linking (next tutorial)
     - ## Progress
       - Complete installation
       - Create first graph
       - Learn basic editing
       - Master linking
     - ## Key Insights
       - Blocks are the atomic unit of knowledge
       - Hierarchy creates structure
       - Properties add metadata
     - ## Resources
       - [Official Docs](https://docs.logseq.com)
       - [[Logseq]] community forum
   ```

2. Click into the `Project: Learn Logseq` block and use `/Add property` to add a **start date** property
3. Under **Progress**, make each item a task: type `/Done` in "Complete installation" and "Create first graph", `/Doing` in "Learn basic editing", and `/Todo` in "Master linking"

## Keyboard Shortcuts Summary

| Action          | Shortcut (Win/Linux)   | Shortcut (macOS)      | Source (`config.cljs`) |
| --------------- | ---------------------- | --------------------- | ---------------------- |
| New block       | `Enter`                | `Enter`               | `:107` |
| Indent          | `Tab`                  | `Tab`                 | `:220` |
| Outdent         | `Shift + Tab`          | `Shift + Tab`         | `:223` |
| Bold            | `Ctrl + B`             | `Cmd + B`             | `:119` |
| Italic          | `Ctrl + I`             | `Cmd + I`             | `:122` |
| Highlight       | `Ctrl + Shift + H`     | `Cmd + Shift + H`     | `:125` |
| Strikethrough   | `Ctrl + Shift + S`     | `Cmd + Shift + S`     | `:128` |
| Cycle status    | `Ctrl + Enter`         | `Cmd + Enter`         | `:167` |
| Add property    | `Ctrl + Alt + P`       | `Cmd + P`             | `:259` |
| Copy block embed | `Ctrl + Shift + E`    | `Cmd + Shift + E`     | `:158` |
| Select all blocks | `Ctrl + Shift + A`   | `Cmd + Shift + A`     | `:244` |
| Slash command   | `/`                    | `/`                   | — |
| Page link or node reference | `[[`       | `[[`                  | — |
| Search          | `Ctrl + K`             | `Cmd + K`             | `:304` |

> **Removed:** "Block reference `((`" from this table. `((` no longer creates a reference ([`src/main/frontend/handler/editor.cljs:1949-1952`](../../../../../src/main/frontend/handler/editor.cljs)).

## Findings: New Capabilities

- `/Add property` is available in the slash menu for quickly creating a property on the current block ([`src/main/frontend/commands.cljs:354-358`](../../../../../src/main/frontend/commands.cljs)), and so is its shortcut ([`config.cljs:259`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).
- Task date commands (Deadline, Scheduled) are available directly in slash commands under **TASK DATE** ([`src/main/frontend/commands.cljs:265-276`](../../../../../src/main/frontend/commands.cljs)).
- `Ctrl/Cmd + Shift + E` copies the current block as an embed, and pasting it creates an embed block ([`src/main/frontend/handler/editor.cljs:3646-3660`](../../../../../src/main/frontend/handler/editor.cljs), [`src/main/frontend/handler/paste.cljs:219-232`](../../../../../src/main/frontend/handler/paste.cljs)).
- Typing `# ` or `1. ` at the start of a block converts it to a heading or a numbered list item.

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

- [Markdown Syntax Reference](../../reference/formatting/markdown-syntax.md) - Complete formatting guide
- [Block Editing Philosophy](../../explanation/concepts/block-editing-philosophy.md) - Why Logseq edits in blocks
- [Code Blocks Reference](../../reference/syntax/code-blocks.md) - Working with code
