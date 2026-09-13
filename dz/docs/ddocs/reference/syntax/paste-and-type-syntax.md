---
title: 'Paste and Type Syntax'
description: 'Cheatsheet for getting styled, structured content into a Logseq DB graph by pasting Markdown or HTML, or by typing'
status: stable
quadrant: 'Reference'
lastVerified: '2026-09-12'
verifiedAgainst: 'be800f1711'
reviewStatus: authored-from-source
graphType: [db]
platform: 'Desktop + Web'
relatedDocs:
  - '[Markdown Mirror Syntax](./markdown-mirror-syntax.md)'
  - '[Markdown Syntax Reference](../formatting/markdown-syntax.md)'
  - '[Code Blocks Reference](./code-blocks.md)'
tags: ['markdown', 'paste', 'editor', 'syntax', 'reference']
---

# Paste and Type Syntax

The companion to [Markdown Mirror Syntax](./markdown-mirror-syntax.md), for the other direction:
what to **paste** or **type** so a Logseq DB graph ends up with real headings, blocks, code blocks
and links, not just text that looks right.

**Legend**

| Mark | Meaning |
|------|---------|
| ✅ | Becomes the real thing (a heading, a child block, a code block, a link) |
| ⚠️ | Kept as text. It may render, but it is not data (not a task, not a tag, not a property) |
| ❌ | Lost, mangled, or the paste fails |

> **How this was verified.** Source read at `origin/master` `be800f1711`. Every paste row was run
> through the real paste handler (`frontend.handler.paste/editor-on-paste!` → `extract-blocks`),
> inserted with the real outliner (`insert-blocks!`) into a test graph, and rendered with the
> Markdown Mirror. Typed rows were run through the real save parser (`wrap-parse-block`). HTML rows
> exercise the real HTML→Markdown converter with parsed HTML trees; browser HTML parsing itself
> was not run. Keystroke conversions and shortcuts are source reads. Probe:
> `dz/scripts/markdown-mirror-probe/paste_syntax_probe_test.cljs`.

---

## The short version

1. **Structure with `- ` bullets.** Indent children with two spaces or a tab.
2. **Headings:** start a bullet with `# ` to `###### `.
3. **Code and math:** give a bullet nothing but a ```` ```lang ```` fence or a `$$` block.
4. **Everything inline** (bold, links, images, tables, quotes, callouts) pastes as text and renders.
5. **Tasks, priorities, dates, properties and tags are not syntax in DB graphs.** Paste the text
   without them, then select the blocks and use shortcuts: **Cmd/Ctrl+Enter** (Todo → Doing → Done),
   `p t` (tags), `p p` (priority), `p d` (deadline), `p s` (status), `t n` (numbered list).
6. **Avoid** `#tag` (it becomes a plain page link), `[#A]` and `((uuid))`. `key:: value` lines are
   harmless but stay text.

**The Markdown Mirror format is the best paste format.** For everything marked ✅ below,
the mirror writes exactly what you would paste.

A template that pastes cleanly:

```markdown
- # Project kickoff
- ## Goals
  - Ship the **beta** by Friday
  - Owner: [[Existing Page]]
  - Spec: [design doc](https://logseq.com)
- ## Install
  - Run this once:
  - ```bash
    npm install
    ```
- ## Formula
  - $$
    E = mc^2
    $$
- ## Notes
  - > Quotes stay quotes
  - | Step | Owner |
    | --- | --- |
    | Build | Ana |
  - #+BEGIN_TIP
    Callouts work too.
    #+END_TIP
```

This gives five heading blocks, with the goals, a **bash code block**, a **math block**, a quote,
a table and a callout nested under them. The mirror writes it back identically.

---

## How paste decides

When you paste into a block being edited ([`paste.cljs:147-198`](../../../../../src/main/frontend/handler/paste.cljs)):

| Clipboard | What happens |
|-----------|--------------|
| Blocks copied from Logseq in the same graph | Pasted as copies of those blocks, not re-parsed from text |
| A file (image, PDF…) | Uploaded as an asset block |
| HTML (from a web page, Google Docs, Word…) | Converted to Markdown first, then treated as below |
| Text with a line starting `- `, `+ `, `* ` or `#`, a line that is a ```` ``` ```` fence, or a `$$` line | **Parsed into blocks** |
| Text with blank lines but none of the above | **One block per paragraph** |
| Anything else | Inserted as text at the cursor |
| A YouTube, Vimeo, Bilibili, Loom… URL | Inserted as `{{video url}}` |
| A twitter.com / x.com URL | Inserted as `{{twitter url}}` |
| A URL pasted over selected text | `[selected text](url)` |
| Anything, while editing a code or math block | Inserted as raw text |
| **Cmd/Ctrl+Shift+V** | Raw text into the current block, no parsing |

---

## Paste Markdown

### Structure

| You paste | You get | Mark |
|-----------|---------|------|
| `- Parent`<br>`  - Child`<br>`    - Grandchild` | Three nested blocks | ✅ |
| Tab indentation `- A` / `\t- B` | Nested blocks, same as spaces | ✅ |
| `- # H1` / `- ### H3` | Heading blocks (heading property, `#` removed from the text) | ✅ |
| A document with no bullets:<br>`# Title` / blank / `Intro.` / blank / `## Section` / blank / `Text.` | One block per heading. **Paragraphs join the heading above them**: block "Title⏎⏎Intro." with Heading 1 | ⚠️ |
| `First paragraph.` / blank / `Second paragraph.` | Two blocks | ✅ |
| `- first paragraph` / blank / `  second paragraph` | One block with both paragraphs | ✅ |
| `1. one` / `2. two` (no bullets) | **One block** containing the list as text. It renders as a numbered list inside the block. | ⚠️ not a numbered list of blocks |
| `- intro` / `1. one` / `2. two` | One block "intro" with the numbered lines as text | ⚠️ |
| `* star` / `+ plus` (no `- `) | **One block** containing both lines as text | ⚠️ use `- ` |

For a real numbered list, paste `- ` bullets, select them and press `t n`.

### Headings, code, math

| You paste | You get | Mark |
|-----------|---------|------|
| ```` ```python ```` / code / ```` ``` ```` on its own | A **code block**, language python | ✅ |
| `- ` bullet whose only content is a fence | A code block, with the language | ✅ |
| `- Setup:` then an indented fence in the same bullet | One text block containing "Setup:" and the fence | ⚠️ renders as code inside the text, but is not a code block |
| `$$` / formula / `$$` on its own, or as a bullet's only content | A **math block** | ✅ |
| Inline `$E=mc^2$` | Text; renders as math | ✅ |

### Text formatting

Pasted as typed and rendered ✅:

`**bold**` `*italic*` `_italic_` `~~strike~~` `==highlight==` `^^highlight^^` `` `code` ``
`[link](https://…)` `<https://…>` `![alt](https://…/a.png)` `x^{2}` `H_{2}O` `\*escaped\*`
`<u>underline</u>` `<kbd>K</kbd>`

### Links, references, tags

| You paste | You get | Mark |
|-----------|---------|------|
| `[[Existing Page]]` | A link to that page | ✅ |
| `[[Brand New Page]]` | A link, and the page is created | ✅ |
| `[label]([[Page]])` | A labelled page link | ✅ |
| `[[<block-uuid>]]` for a block in this graph | A block reference | ✅ |
| `#Project`, `#[[Multi Word]]`, `#newtag` | **Page links** `[[Project]]` `[[Multi Word]]` `[[newtag]]`. The `#` is removed and no tag is added ([`paste.cljs:59-66`](../../../../../src/main/frontend/handler/paste.cljs)). | ❌ as tags |
| `((<block-uuid>))` for a block in this graph | Text `((uuid))` with a reference recorded. It renders as literal text. | ⚠️ use `[[uuid]]` |
| `((<uuid>))` for a block **not** in this graph | **The paste fails** ("Nothing found for entity id") | ❌ |
| `![[Note]]` (Obsidian embed) | Text `![[Note]]` and a link to page "Note" | ⚠️ |

To tag pasted blocks, select them and press `p t` (Set tags for selected blocks), or type `#` in a
block and pick the tag from the list.

### Tasks, priority, dates, properties

None of these are syntax in DB graphs. Parsing of `TODO` markers and `[#A]` priorities is switched
off ([`mldoc.cljc:338-349`](../../../../../deps/graph-parser/src/logseq/graph_parser/mldoc.cljc)), and
property lines are never saved as properties.

| You paste | You get | Mark |
|-----------|---------|------|
| `- TODO buy milk`, `- DONE ship it` | Text "TODO buy milk". Not a task. | ⚠️ |
| `- [ ] checkbox`, `- [x] done` | Text "[ ] checkbox". Not a task. | ⚠️ |
| `- TODO task [#A]` | Text becomes `TODO task [[[A]]]`, and a page named `A]` is created | ❌ |
| `SCHEDULED: <2026-09-14 Mon>` | Text | ⚠️ |
| `- Book` / `  author:: Jane` | One block with "author:: Jane" as a text line | ⚠️ |
| `title:: X` / `tags:: a, b` at the top | A text block with those lines | ⚠️ |
| Mirror output `* Priority:: Urgent` | A text line `* Priority:: Urgent` | ⚠️ |
| Mirror output `* notes::` / `  - value` | Text line `* notes::`, and `value` becomes a **child block** | ⚠️ |

After pasting, select the blocks: **Cmd/Ctrl+Enter** cycles Todo → Doing → Done, `p s` sets a
status, `p p` a priority, `p d` a deadline. Add other fields with `;;` or **Cmd+P** (Mac) /
**Ctrl+Alt+P**.

### Advanced

| You paste | You get | Mark |
|-----------|---------|------|
| `- > quoted` | Text starting with `> `; renders as a quote | ✅ (not a Quote block) |
| Pipe table in a bullet | Text; renders as a table | ✅ |
| `#+BEGIN_NOTE` … `#+END_NOTE` | Text; renders as a callout | ✅ |
| Footnotes, `---`, HTML, hiccup | Text; renders | ✅ |
| `{{video url}}`, `{{cloze x}}` | Text; renders | ✅ |
| `{{embed [[Page]]}}`, `{{query …}}` | Text; renders a deprecation warning | ❌ use `/Node embed`, `/Query` |
| `> [!note]` (Obsidian callout) | A quote with literal `[!note]` | ⚠️ |
| `%%comment%%` | Visible text | ⚠️ |

---

## Paste from web pages and documents

Rich text is converted to Markdown by [`html_parser.cljs`](../../../../../src/main/frontend/extensions/html_parser.cljs), then parsed as above.

| In the source | Becomes | Mark |
|---------------|---------|------|
| Heading `<h1>`–`<h6>` | `#`–`######` heading block. **Following paragraphs join it** until the next list or heading. | ⚠️ |
| Paragraphs | Text; separate blocks only if nothing else triggers block parsing | ⚠️ |
| Bold (`<b>`, `<strong>`, or `font-weight` ≥ 600 as Google Docs uses) | `**bold**` | ✅ |
| Italic (`<i>`, `<em>`, `font-style: italic`) | `*italic*` | ✅ |
| Strikethrough (`<s>`, `<del>`, `<strike>`) | `~~strike~~` | ✅ |
| Highlight (`<mark>`, yellow background) | `==mark==` | ✅ |
| Inline `<code>` | `` `code` `` | ✅ |
| Link `<a>` | `[label](href)` | ✅ |
| Image `<img>` | `![alt](src)` | ✅ |
| Bulleted list, nested | Nested blocks | ✅ |
| **Numbered list** | `-` bullets; **numbering lost** | ⚠️ |
| Checklist checkboxes | Checkbox dropped; item text kept | ❌ |
| Underline, `<sub>`, `<sup>` | Text only; styling lost | ❌ |
| Code block `<pre><code class="language-js">` | ```` ``` ```` fence **without the language**; a code block only if it is the entire paste | ⚠️ |
| Blockquote containing a paragraph | `> ` then the text on a separate line, so the quote is lost | ❌ |
| Table | Pipe table, but the divider row always has three columns | ⚠️ |
| Horizontal rule | `---` | ✅ |
| Page containing `<figure>`, `<figcaption>`, `<aside>`, `<header>`, `<footer>`, `<center>` or `<fieldset>` | **Conversion fails and the whole paste falls back to plain text**, with all formatting lost ([`html_parser.cljs:218-219`](../../../../../src/main/frontend/extensions/html_parser.cljs)) | ❌ |

If rich paste comes out wrong, copy the source as Markdown (many editors offer "Copy as
Markdown"), or paste with **Cmd/Ctrl+Shift+V** and fix it by hand.

---

## Type as you write

### Conversions while typing

| Type | Result | Mark |
|------|--------|------|
| `# ` … `###### ` at the start of a block | Heading 1–6 when the block is saved; `#` removed ([`db_based/editor.cljs:47-56, 105-126`](../../../../../src/main/frontend/handler/db_based/editor.cljs)) | ✅ |
| ```` ``` ```` in an empty block | It autopairs to six backticks and becomes a **code block** immediately ([`editor.cljs:3559-3564`](../../../../../src/main/frontend/handler/editor.cljs)) | ✅ |
| A block whose whole text is a ```` ```lang ```` fence | Code block with that language, on save | ✅ |
| `$` | Autopairs to `$$`; a block that is only `$$…$$` becomes a **math block** on save | ✅ |
| `>` as the only character in a block | **Quote block** immediately ([`editor.cljs:3566-3571`](../../../../../src/main/frontend/handler/editor.cljs)) | ✅ |
| `> text` | Text starting with `> `; renders as a quote | ⚠️ |
| `1. ` as the only text in a block | **Numbered list** block ([`editor.cljs:2265`](../../../../../src/main/frontend/handler/editor.cljs)) | ✅ |
| `[[` | Page search; picks insert a link | ✅ |
| `#` | Tag search. **Enter** adds the tag and removes the `#text`; **Cmd/Ctrl+Enter** keeps it inline. | ✅ |
| `;;` | Add property | ✅ |
| `/` | Command menu | ✅ |
| `((` | Shows "To reference a node, please use `[[]]`" | — |
| `^^`, `==`, `**`, `~~` | Autopair | — |
| `TODO text`, `[#A]`, `SCHEDULED: <…>` | Text only | ⚠️ |
| `key:: value` on any line | Text; not a property. Use `;;` instead. | ⚠️ |

### Keyboard shortcuts

| Shortcut | Does |
|----------|------|
| Cmd/Ctrl+B / I | Bold / italic |
| Cmd/Ctrl+Shift+S | Strikethrough |
| Cmd/Ctrl+Shift+H | Highlight (`==…==`) |
| Cmd/Ctrl+L | Link |
| Cmd/Ctrl+Enter | Cycle Todo → Doing → Done → none |
| Tab / Shift+Tab | Indent / outdent |
| Shift+Enter | New line in the same block |
| Cmd/Ctrl+Shift+V | Paste as raw text into the current block |
| Cmd/Ctrl+Shift+E | Copy block embed |
| Cmd+P (Mac), Ctrl+Alt+P | Add property |

With blocks selected (click a bullet, or Esc while editing):

| Keys | Does |
|------|------|
| `p t` | Set tags |
| `p s` | Set task status |
| `p p` | Set priority |
| `p d` | Set deadline |
| `t n` | Toggle numbered list |
| Cmd/Ctrl+Enter | Cycle Todo → Doing → Done on all of them |

Source: [`shortcut/config.cljs`](../../../../../src/main/frontend/modules/shortcut/config.cljs).

### Slash commands for things with no syntax

| Want | Command |
|------|---------|
| Heading / remove heading | `/Heading 1`…`6`, `/Normal text` |
| Task status | `/Backlog` `/Todo` `/Doing` `/In Review` `/Done` `/Canceled` |
| Priority | `/Priority Urgent` `High` `Medium` `Low`, `/No priority` |
| Dates | `/Scheduled`, `/Deadline`, `/Date picker` |
| Code / quote / math | `/Code block`, `/Quote`, `/Math block` |
| Numbered list | `/Number list`, `/Number children` |
| Link to a page or block | `/Node reference` |
| Embed | `/Node embed` |
| Query | `/Query`, `/Advanced Query` |
| File or image | `/Upload an asset` |
| Web video, tweet, HTML | `/Embed Video URL`, `/Embed Twitter tweet`, `/Embed HTML` |
| Flashcard | `/Cloze` |
| Template | `/Template` |
| Underline | `/Underline` |

---

## Known issues

Found while verifying this page at `be800f1711`.

1. **Pasted `#tag` becomes a page link, not a tag** ([`paste.cljs:59-66`](../../../../../src/main/frontend/handler/paste.cljs)).
2. **Pasted `[#A]` is mangled** to `[[[A]]]` and creates a page `A]`.
3. **Pasting `((uuid))` for a block not in the graph fails the whole paste.**
4. **HTML with `<figure>`, `<aside>`, `<header>`, `<footer>`, `<center>` or `<fieldset>` falls back
   to plain text.** Code block languages, numbered lists, checkboxes, underline, sub/superscript
   and quoted paragraphs are also lost, and table divider rows get three columns.
