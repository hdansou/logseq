---
title: 'Slash Commands Reference'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Reference'
platform: 'Desktop + Mobile'
related-docs:
  - '[Keyboard Shortcuts](./keyboard-shortcuts.md)'
  - '[Templates System](./templates-system.md)'
  - '[Block Editing](../../tutorials/getting-started/block-editing.md)'
tags: ['commands', 'slash', 'reference', 'shortcuts']
---

# Slash Commands Reference

Complete reference of slash commands (`/`) in Logseq. Slash commands provide quick access to formatting, block types, templates, and special features directly from your keyboard.

**Quick Access:** Type `/` in any block to open the command palette.

---

## Table of Contents

1. [Essential Commands](#essential-commands)
2. [Block Types](#block-types)
3. [Formatting](#formatting)
4. [Tasks & Time](#tasks--time)
5. [Templates & Embeds](#templates--embeds)
6. [Advanced Features](#advanced-features)
7. [Custom Commands](#custom-commands)

---

## Essential Commands

### Most Used Commands

| Command      | Shortcut | Creates            | Description              |
| ------------ | -------- | ------------------ | ------------------------ |
| `/template`  | `/tmpl`  | Template           | Insert reusable template |
| `/todo`      | `/t`     | TODO block         | Create task item         |
| `/doing`     | -        | DOING block        | Mark task in progress    |
| `/done`      | -        | DONE block         | Mark task complete       |
| `/deadline`  | `/dl`    | Deadline property  | Add task deadline        |
| `/scheduled` | `/sch`   | Scheduled property | Schedule task            |
| `/tag`       | `#`      | Tag                | Add inline tag           |
| `/link`      | `[[`     | Page link          | Link to page             |
| `/block-ref` | `((`     | Block reference    | Reference specific block |
| `/embed`     | -        | Block embed        | Embed block content      |

---

## Block Types

### Text Blocks

| Command       | Result          | Example                 |
| ------------- | --------------- | ----------------------- |
| `/heading 1`  | `# Heading`     | <h1>Large heading</h1>  |
| `/heading 2`  | `## Heading`    | <h2>Medium heading</h2> |
| `/heading 3`  | `### Heading`   | <h3>Small heading</h3>  |
| `/h1` - `/h6` | Heading levels  | Quick heading shortcuts |
| `/quote`      | Quote block     | > Quoted text           |
| `/line`       | Horizontal line | ---                     |
| `/divider`    | Horizontal rule | Same as `/line`         |

**Example Usage:**

```markdown
/h1 → # Main Topic
/h2 → ## Subtopic
/quote → > Important quote
/line → ---
```

### Lists

| Command           | Result  | Description            |
| ----------------- | ------- | ---------------------- |
| `/ordered-list`   | `1.`    | Numbered list          |
| `/ol`             | `1.`    | Ordered list (short)   |
| `/unordered-list` | `-`     | Bullet list (default)  |
| `/ul`             | `-`     | Unordered list (short) |
| `/checklist`      | `- [ ]` | Checkbox list          |

**Note:** Logseq uses bullets by default, so `/ul` is rarely needed.

### Code Blocks

| Command | Result          | Description             |
| ------- | --------------- | ----------------------- |
| `/code` | ` ```language ` | Syntax-highlighted code |
| `/src`  | ` ```language ` | Same as `/code`         |

**Supported Languages:**

```
javascript, typescript, python, java, clojure, clojurescript,
bash, shell, sql, json, yaml, html, css, markdown, rust, go,
ruby, php, swift, kotlin, c, cpp, csharp, and more...
```

**Example:**

````markdown
/code javascript
→

```javascript
console.log('Hello World')
```
````

````

### Special Blocks

| Command | Result | Description |
|---------|--------|-------------|
| `/query` | Query block | Advanced search queries |
| `/embed` | Embed block | Embed block/page content |
| `/quote` | Quote block | Blockquote styling |
| `/warning` | #+BEGIN_WARNING | Warning callout |
| `/tip` | #+BEGIN_TIP | Tip callout |
| `/note` | #+BEGIN_NOTE | Note callout |
| `/important` | #+BEGIN_IMPORTANT | Important callout |
| `/caution` | #+BEGIN_CAUTION | Caution callout |

**Callout Example:**

```markdown
/tip
→
#+BEGIN_TIP
Your tip content here
#+END_TIP
````

---

## Formatting

### Text Formatting

| Command          | Syntax       | Result            |
| ---------------- | ------------ | ----------------- |
| `/bold`          | `**text**`   | **bold**          |
| `/italic`        | `*text*`     | _italic_          |
| `/strikethrough` | `~~text~~`   | ~~strikethrough~~ |
| `/highlight`     | `^^text^^`   | ==highlighted==   |
| `/code`          | `` `text` `` | `inline code`     |
| `/underline`     | -            | <u>underline</u>  |
| `/mark`          | `^^text^^`   | Same as highlight |

### Links & References

| Command        | Syntax               | Creates         |
| -------------- | -------------------- | --------------- |
| `/link`        | `[[page]]`           | Page link       |
| `/page-link`   | `[[page]]`           | Same as `/link` |
| `/block-ref`   | `((uuid))`           | Block reference |
| `/block-embed` | `{{embed ((uuid))}}` | Block embed     |
| `/page-embed`  | `{{embed [[page]]}}` | Page embed      |
| `/url`         | `[text](url)`        | External link   |

**Quick Shortcuts:**

- Type `[[` → Page link
- Type `((` → Block reference
- Type `{{` → Command suggestions

### Media & Assets

| Command    | Result           | Description         |
| ---------- | ---------------- | ------------------- |
| `/image`   | `![](path)`      | Insert image        |
| `/img`     | Same             | Image (short)       |
| `/upload`  | File picker      | Upload file         |
| `/video`   | Video embed      | Embed video         |
| `/audio`   | Audio embed      | Embed audio         |
| `/youtube` | YouTube embed    | Embed YouTube video |
| `/twitter` | Tweet embed      | Embed tweet         |
| `/iframe`  | `{{iframe url}}` | Embed webpage       |

**Example:**

```markdown
/image
→ ![](../assets/image.png)

/youtube
→ {{youtube https://youtu.be/VIDEO_ID}}
```

---

## Tasks & Time

### Task Commands

| Command      | Result      | Description      |
| ------------ | ----------- | ---------------- |
| `/todo`      | `TODO`      | Create task      |
| `/doing`     | `DOING`     | Mark in progress |
| `/done`      | `DONE`      | Mark complete    |
| `/waiting`   | `WAITING`   | Blocked/waiting  |
| `/later`     | `LATER`     | Backlog          |
| `/now`       | `NOW`       | High priority    |
| `/cancelled` | `CANCELLED` | Cancelled task   |

**Task Workflow:**

```markdown
TODO Buy groceries
/doing → DOING Buy groceries
/done → DONE Buy groceries
```

### Time & Dates

| Command         | Result              | Description          |
| --------------- | ------------------- | -------------------- |
| `/deadline`     | `DEADLINE: <date>`  | Add deadline         |
| `/scheduled`    | `SCHEDULED: <date>` | Schedule task        |
| `/date`         | Date picker         | Insert date          |
| `/time`         | Time picker         | Insert time          |
| `/today`        | `[[2026-01-22]]`    | Today's journal link |
| `/tomorrow`     | `[[2026-01-23]]`    | Tomorrow's link      |
| `/yesterday`    | `[[2026-01-21]]`    | Yesterday's link     |
| `/current-time` | `14:30`             | Current time         |

**Date Picker:**

```markdown
/deadline
→ Opens calendar picker
→ DEADLINE: <2026-01-30 Wed>
```

### Priority

| Command       | Result | Description        |
| ------------- | ------ | ------------------ |
| `/priority-a` | `[#A]` | Highest priority   |
| `/priority-b` | `[#B]` | Medium priority    |
| `/priority-c` | `[#C]` | Low priority       |
| `/pa`         | `[#A]` | Priority A (short) |
| `/pb`         | `[#B]` | Priority B (short) |
| `/pc`         | `[#C]` | Priority C (short) |

---

## Templates & Embeds

### Templates

| Command     | Description             |
| ----------- | ----------------------- |
| `/template` | Insert template by name |
| `/tmpl`     | Template (short)        |

**Usage:**

```markdown
/template meeting
→ Inserts "meeting" template content
```

**See:** [Templates System](./templates-system.md) for creating templates.

### Embeds

| Command        | Embeds         |
| -------------- | -------------- |
| `/embed`       | Block or page  |
| `/embed-page`  | Entire page    |
| `/embed-block` | Specific block |

**Example:**

```markdown
/embed ((block-uuid))
→ {{embed ((abc-123-def))}}
Shows full block content inline

/embed [[Projects]]
→ {{embed [[Projects]]}}
Shows entire Projects page
```

---

## Advanced Features

### Whiteboard & Drawing

| Command       | Result         | Description            |
| ------------- | -------------- | ---------------------- |
| `/draw`       | Whiteboard     | Open TLDraw canvas     |
| `/drawing`    | Same           | Drawing (alias)        |
| `/whiteboard` | New whiteboard | Create whiteboard page |

### Queries

| Command           | Result      | Description     |
| ----------------- | ----------- | --------------- |
| `/query`          | Query block | Advanced search |
| `/advanced-query` | Same        | Query (alias)   |

**Example:**

```markdown
/query
→
#+BEGIN_QUERY
{:title "All TODOs"
:query [:find (pull ?b [*])
:where [?b :block/marker "TODO"]]}
#+END_QUERY
```

**See:** [Query System Reference](../advanced/query-system.md)

### Properties

| Command     | Result        | Description      |
| ----------- | ------------- | ---------------- |
| `/property` | `key:: value` | Add property     |
| `/prop`     | Same          | Property (short) |

**Example:**

```markdown
/property
→ status:: active
tags:: [[Project]]
```

### Plugins & Extensions

| Command          | Description                    |
| ---------------- | ------------------------------ |
| `/command`       | Plugin commands (if installed) |
| `/slash-command` | Custom slash commands          |

**Example with Plugin:**

```markdown
/pomodoro-start (from Pomodoro plugin)
/export-pdf (from PDF Export plugin)
```

---

## Workflow Commands

### Page Management

| Command            | Description          |
| ------------------ | -------------------- |
| `/page-rename`     | Rename current page  |
| `/page-delete`     | Delete current page  |
| `/page-properties` | Show page properties |
| `/favorites`       | Add to favorites     |

### Block Operations

| Command       | Description           |
| ------------- | --------------------- |
| `/copy-ref`   | Copy block reference  |
| `/copy-embed` | Copy embed code       |
| `/move-block` | Move to another page  |
| `/duplicate`  | Duplicate block       |
| `/expand`     | Expand all children   |
| `/collapse`   | Collapse all children |

### Navigation

| Command   | Description     |
| --------- | --------------- |
| `/search` | Search in graph |
| `/find`   | Find on page    |
| `/recent` | Recent pages    |
| `/graph`  | Open graph view |

---

## Mobile Commands

**Touch Shortcuts:**

Some commands have mobile-specific behavior:

| Command     | Mobile Behavior             |
| ----------- | --------------------------- |
| `/template` | Touch-friendly picker       |
| `/date`     | Native date picker          |
| `/image`    | Access camera/photo library |
| `/upload`   | Access file picker          |

**Mobile-Only Commands:**

| Command   | Description              |
| --------- | ------------------------ |
| `/camera` | Take photo (mobile only) |
| `/photo`  | Choose from library      |

---

## Custom Commands

### Creating Custom Commands

**Via Plugin:**

```typescript
// Register custom slash command
logseq.Editor.registerSlashCommand('my-command', async () => {
  await logseq.Editor.insertAtEditingCursor('Custom text')
})
```

**Usage:**

```markdown
/my-command
→ Inserts "Custom text"
```

### Popular Plugin Commands

**Common plugin slash commands:**

| Plugin         | Commands                          |
| -------------- | --------------------------------- |
| **Agenda**     | `/agenda`, `/calendar`            |
| **Tasks**      | `/task-list`, `/completed-tasks`  |
| **Export**     | `/export-pdf`, `/export-markdown` |
| **Mermaid**    | `/mermaid`, `/flowchart`          |
| **Excalidraw** | `/excalidraw`                     |
| **Charts**     | `/chart`, `/graph-chart`          |

---

## Command Categories Summary

### Quick Reference Table

| Category        | Count | Examples                            |
| --------------- | ----- | ----------------------------------- |
| **Text Blocks** | 10+   | `/h1`, `/quote`, `/code`            |
| **Formatting**  | 8+    | `/bold`, `/italic`, `/highlight`    |
| **Tasks**       | 10+   | `/todo`, `/deadline`, `/priority-a` |
| **Links**       | 6+    | `/link`, `/block-ref`, `/embed`     |
| **Media**       | 8+    | `/image`, `/video`, `/youtube`      |
| **Templates**   | 2     | `/template`, `/tmpl`                |
| **Advanced**    | 10+   | `/query`, `/draw`, `/property`      |

**Total Built-in Commands:** 50+

---

## Tips & Tricks

### Fuzzy Search

Type partial command names:

```markdown
/t → Shows: todo, template, time, tag, tip...
/dl → Shows: deadline, delete...
/h → Shows: h1, h2, h3, highlight...
```

### Quick Shortcuts

**Instead of slash commands, use:**

````markdown
[[ → Page link (instead of /link)
(( → Block ref (instead of /block-ref)

# → Tag (instead of /tag)

\*\* → Bold (instead of /bold)

-        → Italic (instead of /italic)

```language
         → Code block (instead of /code)
```
````

### Command History

Recent commands appear first in the menu. Frequently used commands are easy to access.

### Command Aliases

Many commands have short versions:

```markdown
/tmpl = /template
/dl = /deadline
/sch = /scheduled
/pa = /priority-a
/h1 = /heading 1
/ol = /ordered-list
```

---

## Troubleshooting

**Slash menu doesn't appear:**

- Ensure cursor is in block (not between blocks)
- Try typing `/` again
- Check if block is in edit mode
- Restart Logseq if persistent

**Command not found:**

- Check spelling
- Try alias (e.g., `/tmpl` vs `/template`)
- Update Logseq to latest version
- Check if plugin required

**Custom command missing:**

- Verify plugin installed and enabled
- Restart Logseq after plugin installation
- Check plugin settings

**Command doesn't work as expected:**

- Check command syntax in this guide
- Verify you're in correct context (e.g., empty block)
- Try alternative command
- Report bug if reproducible

---

## Platform Differences

### Desktop vs Mobile

| Feature                | Desktop           | Mobile                 |
| ---------------------- | ----------------- | ---------------------- |
| **Command palette**    | Full featured     | Touch-optimized        |
| **Fuzzy search**       | ✅ Yes            | ✅ Yes                 |
| **Custom commands**    | ✅ Plugin support | ⚠️ Limited plugins     |
| **Media commands**     | File picker       | Camera/photo library   |
| **Keyboard shortcuts** | Full support      | External keyboard only |

### File-Based vs Database

| Feature          | File-Based           | Database          |
| ---------------- | -------------------- | ----------------- |
| **All commands** | ✅ Supported         | ✅ Supported      |
| **Query syntax** | Datalog              | Datalog           |
| **Properties**   | Frontmatter + inline | Native properties |

---

## Command Cheat Sheet

**Print this for quick reference:**

```
ESSENTIAL
/template    Insert template
/todo        Create task
/done        Complete task
/deadline    Add deadline
/link        Link page

FORMATTING
/bold        **bold**
/italic      *italic*
/code        `code`
/highlight   ^^highlight^^
/h1-h6       Headings

BLOCKS
/quote       Quote block
/code        Code block
/query       Query block
/embed       Embed content

MEDIA
/image       Insert image
/video       Video embed
/youtube     YouTube embed

ADVANCED
/draw        Whiteboard
/property    Add property
/date        Date picker
/priority-a  High priority
```

---

## Next Steps

**Master Slash Commands:**

1. Memorize top 10 commands
2. Practice daily for muscle memory
3. Explore command categories
4. Try advanced features (queries, embeds)
5. Create custom commands via plugins

**Related:**

- [Keyboard Shortcuts](./keyboard-shortcuts.md)
- [Templates System](./templates-system.md)
- [Query System](../advanced/query-system.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
