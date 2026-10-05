---
name: logseq-db-markdown
description: Create and edit Logseq DB (Database Mode) compatible content. Use when working with Logseq database graphs. DB mode uses a UI-driven approach rather than text-based syntax for properties, tasks, queries and embeds.
---

# Logseq DB (Database Mode) Guide

This guide explains how to work with Logseq database graphs. Unlike the older file-based mode ("Logseq OG"), a DB graph stores content in SQLite and uses **UI-driven interactions** rather than text syntax for properties, tasks, queries and embeds.

> **Last revised:** 2026-10-04. **Audited in full on 2026-10-04** against source at `22a29b30de` and the installed Logseq DB 2.0.1 nightly 20261001, run on a throwaway graph. Each section below says what was run live and what was only read from source. The previous revision (2026-05-29) described a different build and had many wrong claims, corrected here.

**How to read the evidence tags**

- **Live**: run in the app on 2026-10-04 and observed.
- **Source**: read in the source tree, not run.

## Overview

- **Storage**: a SQLite database, not markdown files.
- **Properties**: structured data. Typing `key:: value` does not create a property (**Live**: typed and pasted text both stay literal).
- **Queries**: created with `/Query` or `/Advanced Query`. The `{{query}}` macro is deprecated and renders a warning (**Source**).
- **Tasks**: status is a property with closed values, not a `TODO` text marker.
- **References**: pages and blocks are both referenced as `[[...]]`. A block reference is `[[<uuid>]]`. `((uuid))` is plain text (**Live**: a typed `((` opens no picker).
- **Embeds**: `/Node embed` makes a linked block. The `{{embed}}` macro is deprecated.

**What works as plain text in a block** (**Live** unless noted)

- Markdown formatting: bold, italic, strikethrough, highlight, inline code (**Source** for the table below, except underline).
- Page links `[[Page Name]]` and tags `#tag`.
- Fenced code blocks, and `$...$` / `$$...$$` math (**Source**).
- Markdown headings: `## text` renders as an H2.
- A `---` line renders a horizontal rule.
- Video and tweet macros: `{{youtube URL}}`, `{{tweet URL}}`.

**What does not work as text**

- `property:: value`: stays literal text.
- `((uuid))`: plain text.
- `{{embed ...}}`, `{{query ...}}`, `{{today}}`, `{{current time}}`: deprecated or unsupported. The embed and query macros show a deprecation message, and `{{today}}` shows "Unsupported macro name: today".
- File-mode markers: `TODO/DOING/DONE`, `NOW/LATER/WAITING`, `[#A]`, `SCHEDULED:`, `DEADLINE:`, `template::`.
- Mermaid diagrams: a `mermaid` fence renders as a plain code block (**Live**). No renderer is built in.

## Block structure

Blocks are created by typing and pressing Enter. Tab indents and Shift+Tab outdents.

```markdown
- This is a block
  - This is a child block
- Back to parent level
```

Every block is a database entity with a UUID.

## References

### Page links and tags

```markdown
- A link to a page: [[Page Name]]
- A tag: #tag-name, or #[[multi word tag]]
```

`#tag` makes the page a **tag**, which is a class: the block becomes an object of that tag and can carry the tag's properties (**Source**). It is not just a link.

Duplicate page names are rejected, so there is no same-name UUID page link.

### Block references

A block reference is `[[<block-uuid>]]`.

**How to create (Live)**

1. Right-click a block and choose **Copy block ref**, then paste. You get `[[<uuid>]]`.
2. Or type `[[`, then search. The popup lists matching blocks (with their page) as well as pages, and picking a block stores `[[<uuid>]]`.

Typing `((` does not open a picker in this build (**Live** and **Source**: nothing sets the block-search action except the `/Node embed` flow).

### Block embeds

`/Node embed` is the DB embed. **Live**: it inserts a block with an empty title whose `:block/link` points at the chosen node, and the target's content renders in its place.

**How to embed**

1. Type `/Node embed`, then search for the block or page.
2. Or, while editing the block you want to embed, press `Cmd/Ctrl+Shift+E` and paste where you want the embed. **Live**: the clipboard holds `[[<uuid>]]`, and pasting it creates the same linked block with an empty title.

The right-click menu has no "Copy block embed" item.

`{{embed ((uuid))}}` and `{{embed [[uuid]]}}` are deprecated. **Live**: they render "{{embed}} is deprecated. Use '/Node embed' command instead."

### Namespaced pages

A `/` in a page title creates parent pages. **Live**: creating `Projects/Alpha/Tasks` made three pages, `Projects` (parent: the **Library** page), `Alpha` (parent: `Projects`) and `Tasks` (parent: `Alpha`), each stored under its leaf title. Creation fails if a parent title already exists as something other than a page (**Source**: "Cannot create this page unless all parents are pages"). The `{{namespace}}` macro is deprecated in favor of Library.

For tags, `#project/alpha` creates `Project` and `Project/alpha` and sets `Extends` so the child extends the parent. It fails if a parent is a non-tag page (**Source**).

## Properties (database-first)

Properties are structured entities, not text. **Live**: `status:: Todo` typed into a block, and the same text pasted as a multi-line block, both remain literal text with no property created.

### How to add a property

1. Type `/Add property`, choose or create a property, then set its value. For a task there are direct commands: `/Todo`, `/Doing`, `/Priority High` and so on (**Live** menu).
2. Or click **+ Add property** in the block's property area, or press `Cmd+P` (`Ctrl+Alt+P` on Windows and Linux). **Live**: `Cmd+P` opens a list of properties including Scheduled.
3. Or select a block (press Esc while editing) and use a chord. **Live**: `p` then `d` opens the Deadline date picker, and `p` then `s` opens the status list. **Source**: `p t` tags, `p p` priority, `p i` icon. There is no scheduled chord; use `/Scheduled` or `Cmd+P`.

The block right-click menu has no "Add property" item.

### Property types

User-creatable types (**Source**: `deps/db/src/logseq/db/frontend/property/type.cljs`): **Text** (default), **Number**, **Date**, **DateTime**, **Checkbox**, **URL**, **Node** and **Asset**. A Node property links to pages and blocks. A property can allow one value or many (cardinality). There is no List, Page or Reference type; the plugin API also accepts `json` and `string`.

### Page properties

Page properties appear at the top of a page. Built-in properties include icon, alias and tags. **Not checked live**: the exact placement of the "Add property" button on a page.

### Built-in properties worth knowing

- `Alias`, `Tags`, `Icon`.
- `Status`, `Priority`, `Deadline`, `Scheduled` (carried by the built-in **Task** class).
- `Publishing Public?` (the old `public::`).
- Templates are a class, not a property. See Templates.

There is no `title::`, `filters::`, `template::` or `template-including-parent::` property in DB graphs.

## Task management

### Status

Status is the built-in `:logseq.property/status` property with closed values: **Backlog, Todo, Doing, In Review, Done, Canceled** (**Source**). Set it with `/Backlog`, `/Todo`, `/Doing`, `/In Review`, `/Done`, `/Canceled`, or the `p s` chord.

### Priority

Closed values **Low, Medium, High, Urgent**. **Live**: `/Priority` filters to "Priority Low", "Priority Medium", "Priority High", "Priority Urgent" and "No priority".

### Scheduled and Deadline

Type `/Scheduled` or `/Deadline`, or select the block and press `p` then `d` for a deadline. Both are DateTime properties (**Source**). The Scheduled and Deadline section on today's journal uses a local-midnight window; see [Customize the Journal Queries](../ddocs/how-to/queries/customize-journal-queries.md).

### Repeating tasks (Source)

Click the date on a Scheduled or Deadline value, tick **Repeat task**, then set **Every N <unit>** (Minute, Hour, Day, Week, Month, Year), **Next date** (Advance from completion, Advance from scheduled, or Advance from scheduled, skip to future) and the status that triggers the repeat. The panel is hidden on narrow screens. It stores `:logseq.property.repeat/recur-frequency` (default 1), `recur-unit` (default Day), `repeated?`, `repeat-type`, `temporal-property` and `checked-property`. Not run live in this audit.

## Text formatting

| Style         | Syntax                            | Evidence |
| ------------- | --------------------------------- | -------- |
| Bold          | `**text**`                        | Source   |
| Italic        | `*text*`                          | Source   |
| Bold + italic | `***text***`                      | Source   |
| Strikethrough | `~~text~~`                        | Source   |
| Highlight     | `^^text^^`                        | Source   |
| Inline code   | `` `code` ``                      | Source   |
| Underline     | `<ins>text</ins>` or `/Underline` | Live     |

`__text__` is **bold**, not underline (**Live**).

### Headings

Typing `## Heading` at the start of a block renders an H2 (**Live**). `/Heading 1` to `/Heading 6` and `/Normal text` are in the slash menu.

## Queries

`/Query` turns the current block into a Query-tagged block and stores its text in the `:logseq.property/query` property (**Source**). Use `/Advanced Query` for datalog. The `{{query}}` macro is deprecated.

### Simple (DSL) queries — all **Live**

```clojure
(and [[Project Alpha]] (task Todo))
(task Todo Doing)
(priority High Urgent)
(property Status Done)
(property :logseq.property/status Done)
```

- DSL arguments are **case-insensitive for tasks and priorities**: `(task TODO)`, `(task Todo)` and `(task todo)` return the same blocks. `(task NOW)` and `(task Backlog)` match nothing unless such a status exists on a block.
- Statuses are Backlog, Todo, Doing, In Review, Done, Canceled. File-mode `NOW`, `LATER`, `WAITING` do not exist.
- `(property ...)` looks the property up by its **exact, case-sensitive title**: `(property Status Done)` works, and `(property status done)` and `(property Status done)` return nothing.
- `(property scheduled today)` and `(between deadline today +7d)` match nothing. The qualified `(between :logseq.property/deadline today +7d)` also matched nothing in the test. For deadline and scheduled ranges, use an advanced query as in [Customize the Journal Queries](../ddocs/how-to/queries/customize-journal-queries.md). `(between created-at -1d tomorrow)` does work.

### Template variables

`<% today %>`, `<% yesterday %>`, `<% tomorrow %>`, `<% time %>` and `<% current page %>` resolve inside a query (**Source**; `<% current page %>` **Live**). Wrap a variable in an operator, for example `(and <% today %> (task Todo))`. A query whose whole text is a bare variable becomes a text search; see [Dynamic variables](../ddocs/reference/syntax/dynamic-variables.md).

### Advanced queries (Live)

Use `/Advanced Query` and enter a map:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property/status :logseq.property/status.todo]]}
```

Status is a property whose value is a ref, so match `:logseq.property/status.todo`, not a tag. A `:title` key is accepted and ignored. Always pull with `[*]` or include `:block/uuid`.

## Embeds

See **References → Block embeds**. Page embeds use the same command: `/Node embed`, then pick the page. There is no `/Embed page` or `/Embed block` command (**Live** menu). Embed media with `/Embed Video URL`, `/Embed Twitter tweet`, `/Embed YouTube timestamp` or `/Embed HTML`.

## Code blocks

Inline code uses backticks. For a block of code, use `/Code block`, which creates a typed code block with a language setting. A fence typed in text also renders as code (**Live**).

Any CodeMirror mode name is accepted as the language; `edn`, `clj`, `cljc` and `cljs` map to `clojure` (**Source**).

## Math (LaTeX)

```markdown
- Inline: $E = mc^2$
- Block:
  $$
  \int_a^b f(x)dx = F(b) - F(a)
  $$
```

`/Math block` also inserts a block (**Live** menu entry; rendering **Source**).

## Diagrams

Mermaid is **not built in**. **Live**: a `mermaid` fence renders as a code block with a language label and a Copy button, no diagram. A plugin can register a renderer for a fenced language (**Source**).

## Lists

Bullets are blocks. A numbered list is a block property, not typed text: use `/Number list` (the block) or `/Number children` (its children), or **Toggle number list** in the block menu. Typing `1.` is plain text (**Source**).

## Block quotes and rules

```markdown
- > A quoted block
```

`/Quote` inserts one. A `---` line inside a block renders a horizontal rule (**Live**); a `## Heading` is a heading, not a rule.

## Links and media

```markdown
- [Link text](https://example.com)
- Direct URL: https://example.com
- ![Alt text](https://example.com/image.png)
```

- Use `/Upload an asset` for files and images. Uploaded files are asset blocks stored as `assets/<block-uuid>.<ext>`; resize and align them from the image's ⋮ menu.
- A hand-typed relative path such as `../assets/x.png` renders only if that file exists (**Source**; not run). An absolute path to a local image also rendered in the desktop app (**Live**), with a ⋮ menu offering Copy image and Show file in folder.
- `![alt](img.png){:width 300}` is parsed as image metadata (**Source**); an asset block's own size overrides it.
- A `.pdf` link opens in the in-app PDF viewer (**Source**).

## Macros

- `{{youtube URL}}` and `{{tweet URL}}` render an embedded player (**Live**). `vimeo`, `bilibili` and `video` are handled the same way (**Source**).
- `{{today}}`, `{{yesterday}}`, `{{tomorrow}}`, `{{current time}}` are **not** macros. **Live**: `{{today}}` shows "Unsupported macro name: today". Use the slash commands `/Today`, `/Yesterday`, `/Tomorrow`, `/Current time` and `/Date picker`.
- `{{embed}}`, `{{query}}` and `{{namespace}}` are deprecated.
- Custom macros from `:macros` in `config.edn` are still read (**Source**; not run).

## Templates

A template is a block tagged with the **Template** class, and its children are the template body.

**How to create**

1. Write a parent block and nest the template content under it.
2. Tag the parent with `#Template`.
3. Optionally set **Apply template to tags** so the template is applied automatically to blocks that get those tags (**Source**).

**How to use (Live):** type `/Template`, pick the template, and Logseq inserts the template's **children**. The template block itself is not inserted.

There is no "Convert to template" or "Make template" menu item. `template::` and `template-including-parent::` do not apply in DB graphs.

## Classes and tags

Tags can carry properties: create a tag, add properties to its definition, and every block with that tag can use them.

`#Task` is built in and already carries status, priority, deadline and scheduled. Do not recreate it as a user class with a `status` or `due-date` property. For your own structured data, create tags such as `#Book` (author, year, rating) or `#Person` (email, department, role).

## Slash commands (Live)

Type `/` in a block. The built-in entries are:

- **Structure:** Node reference, Node embed, Link, Image link, Underline, Code block, Quote, Math block, Normal text, Heading 1 to 6, Number list, Number children.
- **Tasks:** Backlog, Todo, Doing, In Review, Done, Canceled, Deadline, Scheduled, No priority, Priority Low / Medium / High / Urgent.
- **Dates:** Tomorrow, Yesterday, Today, Current time, Date picker.
- **Data:** Query, Advanced Query, Query function, Calculator, Template, Add property, Add comment, Upload an asset, Cloze.
- **Media:** Embed HTML, Embed Video URL, Embed YouTube timestamp, Embed Twitter tweet.

Plugins add more (the test profile showed AI and Callout entries). There is no `/Draw`, `/Embed page` or `/Embed block`.

## Keyboard shortcuts and menus

- `Cmd+P` (`Ctrl+Alt+P` on Windows and Linux) opens Add property (**Live**, macOS).
- With a block selected: `p d` Deadline, `p s` status, `p t` tags, `p p` priority, `p i` icon (**Live** for `d` and `s`; **Source** for the rest).
- `Cmd/Ctrl+Shift+E` while editing copies an embed of the block (**Live**).
- `[[` opens page and block search; `#` opens tag search; `/` opens the command menu.

**Right-click menu on a block bullet (Live)**: Open in sidebar, Add comment, Add reaction, Set icon, Copy block ref, Copy block URL, Copy / Export as.., Cut, Delete selected blocks, Make a Flashcard, Toggle number list, Expand all, Collapse all, plus any plugin items.

## Linked references

Linked references appear at the bottom of each page without any syntax. Filter them in the UI.

## For plugin developers

- Detect DB mode with `logseq.App.checkCurrentIsDbGraph()` (**Source**).
- Write properties with `logseq.Editor.upsertBlockProperty()`; do not generate `property:: value` text.
- See `libs/guides/db_properties_guide.md` for the API.

```typescript
await logseq.Editor.upsertProperty('rating', {
  type: 'number',
  cardinality: 'one',
  public: true
})

await logseq.Editor.createTag('Book', {
  tagProperties: [
    { name: 'author', schema: { type: 'default', cardinality: 'one' } },
    { name: 'year', schema: { type: 'number', cardinality: 'one' } }
  ]
})
```

The signatures match `libs/src/LSPlugin.ts` (**Source**; not run). The plugin API resolves a property key to a plugin-namespaced property, so `upsertBlockProperty(uuid, 'status', ...)` may not write the built-in Status. For tasks, set status through the UI or the full ident. This was not run.

## Best practices

1. **Use the UI for properties.** Do not write `property:: value`.
2. **Use `/Query` or `/Advanced Query`.** Do not use `{{query}}`.
3. **Match statuses by their real values** (Backlog, Todo, Doing, In Review, Done, Canceled). Case does not matter in `(task ...)`, but it does in `(property ...)`.
4. **Use the shortcuts that exist**: `Cmd+P`, the `p` chords on a selected block, and `/` commands.
5. **Know where data lives.** Content is in SQLite. Use **Export** (Markdown, EDN, SQLite DB, or zip) for backups. Logseq Sync is sync, not version control.
6. **Use namespaces for organization**, for example `Projects/Alpha/Tasks`. Parents are created under Library.
7. **Tag with classes** for structured data: `#Book`, `#Person`.

## DB mode vs file mode

| Feature              | File mode (Logseq OG)               | DB mode (Logseq DB)                                        |
| -------------------- | ----------------------------------- | ---------------------------------------------------------- |
| **Storage**          | Markdown/Org files                  | SQLite database                                            |
| **Page links**       | `[[Page]]`                          | `[[Page]]`                                                 |
| **Block references** | `((block-id))`                      | `[[<UUID>]]`                                               |
| **Properties**       | `property:: value` text             | UI-based, stored as database properties                    |
| **Embeds**           | `{{embed ((uuid))}}`                | `/Node embed` (a `:block/link` block)                      |
| **Tasks**            | `TODO/DOING/DONE` text markers      | Status property: Backlog, Todo, Doing, In Review, Done, Canceled |
| **Priorities**       | `[#A]` `[#B]` `[#C]`                | Priority property: Low, Medium, High, Urgent               |
| **Scheduled**        | `SCHEDULED: <date>`                 | Scheduled property (`/Scheduled`, `Cmd+P`)                 |
| **Deadline**         | `DEADLINE: <date>`                  | Deadline property (`/Deadline`, `p d`)                     |
| **Tags**             | `#tag` or `[[tag]]`                 | `#tag` makes the page a tag (class)                        |
| **Queries**          | `{{query}}` macro                   | `/Query` (a Query-tagged block)                            |
| **Query statuses**   | `TODO`, `DOING`, `DONE`             | Real values; `(task ...)` ignores case                     |
| **Templates**        | `template::` property               | `#Template` class and `Apply template to tags`             |
| **Export**           | Direct file access                  | Markdown, EDN, SQLite DB, zip                              |

**Migration:** content can be imported from file mode, but not all syntax translates directly.

## References

- [Customize the Journal Queries](../ddocs/how-to/queries/customize-journal-queries.md)
- [Dynamic variables](../ddocs/reference/syntax/dynamic-variables.md)
- [Database Graph Schema Reference](../03-features/database-graphs/schema-reference.md)
- [Query Syntax: DB vs File](../03-features/query-system/syntax-db-vs-file.md)
- `libs/guides/db_properties_guide.md` (plugin developers, in the Logseq repository)
