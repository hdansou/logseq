---
title: 'Markdown Mirror Syntax'
description: 'Cheatsheet of how Logseq DB graph content is written to Markdown Mirror files, with the parts that are lossy or ambiguous for a future two-way mirror'
status: stable
quadrant: 'Reference'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: authored-from-source
graphType: [db]
platform: 'Desktop'
relatedDocs:
  - '[Markdown Syntax Reference](../formatting/markdown-syntax.md)'
  - '[Code Blocks Reference](./code-blocks.md)'
  - '[File vs DB graphs](../../explanation/concepts/file-vs-db.md)'
  - '[Properties Reference](../data/properties-reference.md)'
tags: ['markdown', 'mirror', 'export', 'syntax', 'reference']
---

# Markdown Mirror Syntax

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

A DB graph keeps its content in a database. When **Markdown Mirror** is on, the desktop app also
writes each page to a `.md` file. This page is the cheatsheet for that translation: for each thing
you can put in a DB graph, what the database stores and the exact Markdown the mirror writes.

It is modelled on Obsidian's [Basic](https://obsidian.md/help/syntax) and
[Advanced](https://obsidian.md/help/advanced-syntax) formatting pages. There is a second purpose:
the mirror is **one-way today**. Every row that loses information or produces text you cannot tell
apart from something else is marked, so that a future two-way mirror holds no surprises.

**Legend**

| Mark | Meaning |
|------|---------|
| ✅ | Written faithfully. Reading the file back gives the same thing. |
| ⚠️ | Written, but lossy or ambiguous. The same text can come from two different things in the graph. |
| ❌ | Not written at all. |

> **How this was verified.** Source read at `origin/master` `be800f1711` (2026-09-08). Every
> example output on this page was produced by running the real mirror renderer
> (`<mirror-page!`) against a test database built for each case. Key cases were repeated end to
> end with the `logseq` CLI on a desktop 2.0.1 graph (`b09316a`, 2026-07-13) and matched.
> Sections that depend on the editor (what typing does) come from source reads of the save path.

---

## Contents

**Basic**
1. [Turning it on](#turning-it-on)
2. [Files and folders](#files-and-folders)
3. [Page file anatomy](#page-file-anatomy)
4. [Blocks and nesting](#blocks-and-nesting)
5. [Headings](#headings)
6. [Text formatting](#text-formatting)
7. [Links, references and tags](#links-references-and-tags)
8. [Embeds](#embeds)
9. [Tasks](#tasks)
10. [Properties](#properties)
11. [Special blocks](#special-blocks)

**Advanced**
12. [Tables, callouts, footnotes, math](#tables-callouts-footnotes-math)
13. [HTML, hiccup, comments and escaping](#html-hiccup-comments-and-escaping)
14. [Macros](#macros)
15. [Syntax that does not work in DB graphs](#syntax-that-does-not-work-in-db-graphs)

**Round trip**
16. [Two-way readiness: every lossy or ambiguous case](#two-way-readiness)
17. [Known bugs](#known-bugs)
18. [Reading the mirror in other tools](#reading-the-mirror-in-other-tools)
19. [Version notes](#version-notes)

---

## Turning it on

| | |
|---|---|
| Where | **Settings → Features → Markdown Mirror**, with a **Regenerate full mirror** button ([`settings.cljs:805-837`](../../../../../src/main/frontend/components/settings.cljs)) |
| Label text | "Write a derived Markdown copy of edited pages to the graph's mirror/markdown folder. Desktop only." ([`en.edn:1733`](../../../../../src/resources/dicts/en.edn)) |
| Config key | `:feature/markdown-mirror?` (off unless `true`) |
| Platforms | Desktop only. The row is rendered only under Electron ([`settings.cljs:1170-1171`](../../../../../src/main/frontend/components/settings.cljs)); the writer runs only on a Node or Electron runtime ([`markdown_mirror.cljs:200-203`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)). Web and mobile never write. |
| When files change | After an edit, changes are batched and written about **1 second** later ([`markdown_mirror.cljs:660-672`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)). A file is rewritten only if its content changed. Renaming a page writes the new file and deletes the old one; it also rewrites pages that link to it. Deleting a page deletes its file ([`markdown_mirror.cljs:674-694`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)). |
| Direction | One way. `mirror/markdown` is on the shared ignore list, so editing those files does not change the graph ([`graph.cljs:84`](../../../../../deps/common/src/logseq/common/graph.cljs)). |

---

## Files and folders

```text
~/logseq/graphs/<graph>/mirror/markdown/
├── journals/
│   └── 2026_09_12.md
└── pages/
    ├── Book.md
    ├── Projects_Alpha.md        ← page "Projects/Alpha"
    ├── Same Name.md
    └── Same Name (2).md         ← second page with the same title
```

| Page | File name | Mark |
|------|-----------|------|
| Journal | `journals/yyyy_MM_dd.md`, whatever your journal title format is ([`markdown_mirror.cljs:70-75`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)) | ✅ |
| Any other page | `pages/<title>.md`, title normalized to Unicode NFC | ✅ |
| Title with `< > : " \| ? * \ /` or control characters | Each becomes `_`. `Projects/Alpha` → `Projects_Alpha.md`, `Note: A?B*` → `Note_ A_B_.md` | ⚠️ the title cannot be recovered from the file name |
| Trailing spaces or dots | Removed | ⚠️ |
| Longer than 160 characters | Cut at 160 | ⚠️ |
| Two pages whose names normalize the same | Sorted by page UUID; the second gets ` (2)`, the third ` (3)` ([`markdown_mirror.cljs:90-110`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)) | ⚠️ which page is `(2)` depends on UUID order, not creation order |
| Title that is a Windows device name (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`9`, `LPT1`–`9`) | Not written; logged as `:invalid-file-name` | ❌ |
| Two journal pages for the same day | Not written; logged as `:duplicate-journal-day` ([`markdown_mirror.cljs:604-612`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)) | ❌ |

**Which pages get a file** ([`markdown_mirror.cljs:545-557`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)):

| Written | Not written |
|---------|-------------|
| Your pages and journals | Built-in pages (Library, Quick add, Recycle…) |
| Tag (class) pages | Property pages, including properties you created |
| The built-in **Contents** page | Hidden pages |
| | User profile pages (pages with a user email) |

---

## Page file anatomy

```markdown
id:: 7f80f1f5-29a2-4655-91a5-d01fe9e05aec
* rating:: 4.5
* notes::
  - page note

- First block
- Second block
```

| Line | What it is | Mark |
|------|------------|------|
| `id:: <uuid>` | Always the first line: the page's UUID, used to match the file back to the page ([`markdown_mirror.cljs:280-282, 527-535`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)). It is not a property. | ✅ |
| `* name:: value` at column 0 | Page properties, before the first block. See [Properties](#properties). | ✅ / ⚠️ |
| Blank line | Separates the header from the blocks. A page with no blocks has no blank line. | ✅ |
| `- …` | Blocks | see below |
| Page **tags** | Not written. `:block/tags` is an internal attribute and is skipped ([`property.cljs:711-717`](../../../../../deps/db/src/logseq/db/frontend/property.cljs), [`file.cljs:175-177`](../../../../../src/main/logseq/common/export/file.cljs)). | ❌ |
| Page **aliases** | Not written, for the same reason | ❌ |
| Page title | Only in the file name (normalized, see above) | ⚠️ |
| Page icon | Written as raw EDN: `* Icon:: {:type :emoji, :id "books"}` | ⚠️ |

---

## Blocks and nesting

| In Logseq | Mirror writes | Mark |
|-----------|---------------|------|
| A block | `- text` | ✅ |
| Child block | Two more spaces per level: `  - child`, `    - grandchild` | ✅ |
| Empty block | `-` | ✅ |
| Block with several lines | The first line after `- `; later lines indented to the block's text column | ✅ |
| Blank line inside a block | An indented empty line (two spaces for a top-level block) | ✅ |
| Collapsed block | Written with all its children; collapsed state is not written | ⚠️ collapse state is lost |
| Leading or trailing spaces or blank lines in a block | Trimmed ([`file.cljs:323`](../../../../../src/main/logseq/common/export/file.cljs)) | ⚠️ and a block that starts with a blank line triggers [bug 1](#known-bugs) |
| Numbered list (`/Number list`, or type `1. `) | `1.`, `2.` instead of `-` ([`markdown_mirror.cljs:323-330, 414-428`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)) | ⚠️ see below |
| A line of text that starts with `- ` or `1. ` inside a block | Written as is, indented: `- line` / `  - looks like a child` | ⚠️ looks like a child block |

Example: a numbered list with a plain bullet in the middle.

```markdown
1. one
  1. one.a
  2. one.b
2. two
- bullet between
3. three
```

- The counter **keeps counting across** the plain bullet: `3.`, not `1.`.
- Children of a numbered item are indented **two** spaces. CommonMark needs three under `1. `,
  so other Markdown tools may not nest them.

---

## Headings

In the editor, typing `## ` at the start of a block, `/Heading 2`, or the bullet menu all store
`:logseq.property/heading` and **remove the `#`s from the text** ([`db_based/editor.cljs:105-126`](../../../../../src/main/frontend/handler/db_based/editor.cljs)).
The mirror puts them back ([`file.cljs:228-284`](../../../../../src/main/logseq/common/export/file.cljs)).

| In Logseq | Stored | Mirror writes | Mark |
|-----------|--------|---------------|------|
| Heading 1–6 | `heading 1`…`6`, text without `#` | `- # Heading one` … `- ###### …` | ✅ |
| Auto heading (bullet menu) | `heading true` | Level = nesting depth + 1, capped at 6. A top-level block gets `##`, its child `###` | ⚠️ "auto" is lost; it reads back as a fixed level |
| Heading on a task | heading + status | `- TODO ## Heading task` | ⚠️ `##` is no longer at the start of the line, so it is not a Markdown heading |
| Text that really starts with `# ` (for example written by the CLI or API, which store text as is) | no heading property | `- # literal text` | ⚠️ identical to a Heading 1 |

---

## Text formatting

Inline Markdown is stored **as typed** in the block text and written to the file unchanged ✅.
The "Renders in DB?" column says whether Logseq itself displays it. A form that does not render
still round-trips as text.

| Syntax | Renders in DB? | Notes |
|--------|:--------------:|-------|
| `**bold**`, `__bold__` | ✅ | |
| `*italic*`, `_italic_` | ✅ | |
| `~~strikethrough~~` | ✅ | |
| `==highlight==`, `^^highlight^^` | ✅ | Both render as `<mark>`. The shortcut inserts `==`. |
| `` `inline code` `` | ✅ | |
| `$E=mc^2$`, `\( … \)` | ✅ | Inline math (KaTeX) |
| `x^{2}`, `H_{2}O` | ✅ | Superscript / subscript. Plain `x^2` stays literal. |
| `\alpha` | ✅ | Backslash entity |
| Underline | — | No Markdown syntax. Use `<u>text</u>` or `<ins>text</ins>`. |
| Line break | ✅ | A new line inside the block (Shift+Enter), or two trailing spaces |
| `&nbsp;`, `&alpha;`, `:smile:` | ❌ | Shown literally |
| `\*escaped\*` | ✅ | Backslash escapes |

Sources: [`block.cljs:1931-2068`](../../../../../src/main/frontend/components/block.cljs), parsed with mldoc 1.5.9 and the DB parser settings ([`mldoc.cljc:344-355`](../../../../../deps/graph-parser/src/logseq/graph_parser/mldoc.cljc)).

**Block background colour** (bullet menu swatch) is a hidden property and is **not written** ❌.
Only inline `==…==` / `^^…^^` survives.

---

## Links, references and tags

In the database, links point at IDs: `[[Page]]` is saved as `[[<uuid>]]` and `#tag` as
`#[[<uuid>]]` ([`content.cljs:148-172`](../../../../../deps/db/src/logseq/db/frontend/content.cljs)).
The mirror turns **page** IDs back into titles and leaves **block** IDs alone
(`:preserve-block-refs? true`, [`markdown_mirror.cljs:434-442`](../../../../../src/main/frontend/worker/markdown_mirror.cljs); [`content.cljs:199-246`](../../../../../deps/db/src/logseq/db/frontend/content.cljs)).

| In Logseq | Stored | Mirror writes | Mark |
|-----------|--------|---------------|------|
| Page link | `[[<page-uuid>]]` | `[[Foo]]` | ✅ |
| Page link to a title with spaces | `[[<uuid>]]` | `[[Multi Word Page]]` | ✅ |
| Labelled page link | `[label]([[<uuid>]])` | `[label]([[Foo]])` | ✅ |
| Page link where two pages share the title | `[[<uuid>]]` | `[[Foo]]` | ⚠️ which "Foo" is lost |
| Block reference ("Copy block ref") | `[[<block-uuid>]]` | `[[f0000000-0000-4000-8000-000000000003]]` | ⚠️ no file contains block IDs, so the target cannot be found from the mirror alone |
| Inline tag, one word | `#[[<uuid>]]` | `#Book` | ✅ |
| Inline tag with spaces | `#[[<uuid>]]` | `#[[Sci Fi]]` | ✅ |
| Tag added with the tag picker (not in the text) | `:block/tags` only | Added to the **end of the first line**, sorted: `- text #Book #[[Sci Fi]]` ([`markdown_mirror.cljs:254-319`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)) | ⚠️ you cannot tell an inline tag from a picker tag |
| Tag both inline and in the picker | both | Written once (case-insensitive check) | ✅ |
| Built-in tags: `#Task`, `#Code`, `#Quote`, `#Math`, `#Query`, `#Asset`, `#Card`, `#Template`… | `:block/tags` | Not written | ⚠️ implied by other syntax for Task/Code/Quote/Math, lost for Card, Template, Query, Asset |
| External link | `[Logseq](https://logseq.com)` | unchanged | ✅ |
| Autolink / bare URL | `<https://example.com>`, `https://example.com` | unchanged | ✅ |
| Image by URL | `![alt](https://…/a.png){:width 300}` | unchanged | ✅ |
| Legacy block ref `((uuid))` | literal text | unchanged | ✅ as text, but it **renders as literal text** in DB graphs |

Renaming a page rewrites every mirror file that links to it with the new title ✅
([`markdown_mirror.cljs:125-145`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)).

---

## Embeds

`/Node embed` does not insert any text. It creates an empty block whose `:block/link` points at
the embedded block or page. There is no `{{embed}}` macro in DB graphs.

| In Logseq | Mirror writes | Mark |
|-----------|---------------|------|
| Embedded **block** | A **copy** of the target block and its children, in place of the embed ([`markdown_mirror.cljs:332-337, 453-469`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)) | ⚠️ reads back as duplicated content, not an embed |
| Embedded **page** | `-` (an empty block) | ❌ the embed is lost |

```markdown
- Target block        ← block embed, expanded
  - target child
-                     ← page embed
```

---

## Tasks

Status is a property, not text. Typing `TODO ` does **not** create a task in a DB graph; it stays
literal text ([`mldoc.cljc:351-353`](../../../../../deps/graph-parser/src/logseq/graph_parser/mldoc.cljc)).
Use `/Todo`, `/Doing`… or Cmd/Ctrl+Enter.

The mirror writes the status as an upper-case word at the start of the line, with spaces turned
into `-` ([`markdown_mirror.cljs:240-252`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)).

| Status | Mirror writes |
|--------|---------------|
| Backlog | `- BACKLOG text` |
| Todo | `- TODO text` |
| Doing | `- DOING text` |
| In Review | `- IN-REVIEW text` |
| Done | `- DONE text` |
| Canceled | `- CANCELED text` |
| `#Task` with no status chosen (defaults to Todo) | `- TODO text` |
| Status cleared | `- text` |

Priority, scheduled and deadline are ordinary properties:

```markdown
- DOING ship the cheatsheet
  * Priority:: Urgent
  * Deadline:: Sep 15th, 2026 13:30
  * Scheduled:: Sep 14th, 2026 09:00
```

| Thing | Mark |
|-------|------|
| Status word | ✅ |
| Priority `Urgent / High / Medium / Low` | ✅ |
| Priority set to "No priority" | ⚠️ writes an empty value block: `* Priority::` then `    - ` |
| Scheduled / Deadline | ⚠️ written in the **computer's local time zone**, no zone, no seconds, format `MMM do, yyyy HH:mm` ([`file.cljs:28-40`](../../../../../src/main/logseq/common/export/file.cljs)). `2026-09-14T14:00Z` is `09:00` on a US-Central machine and `16:00` in Paris. |
| Repeating tasks | ⚠️ written as `* Repeating recur frequency::`, `* Repeating recur unit::`, `* Repeating type::` lines |
| Text that literally starts with `TODO ` but is not a task | ⚠️ `- TODO text`, identical to a real task |

---

## Properties

Properties are written as `*` list items with `name::`, so blocks (`-`) and properties (`*`) can
be told apart ([`file.cljs:151-204`](../../../../../src/main/logseq/common/export/file.cljs)).
Page properties sit at column 0 before the blocks; block properties are indented under their block.

```markdown
- all types
  * rating:: 3
  * people:: [[Foo]], [[Multi Word Page]]
  * Due Date:: Sep 12th, 2026
  * keywords::
    - alpha
    - beta
  * notes::
    - block note
  * reviewed:: true
  * color:: Red
  * meeting at:: Sep 12th, 2026 09:00
  * website:: https://example.com
```

| Property type | Mirror writes | Mark |
|---------------|---------------|------|
| Text (`:default`) | `* notes::` then the value as a child `- block note`. Many values: one child each. A value's own children and properties are written under it. | ✅ |
| Text with fixed choices (closed values) | Inline: `* color:: Red`; many: `* moods:: Joy, Calm` | ✅ |
| Number | `* rating:: 4.5` | ✅ |
| Checkbox | `* reviewed:: true` / `false` | ✅ |
| URL | `* website:: https://example.com` | ✅ |
| Node | `* people:: [[Foo]], [[Multi Word Page]]` | ⚠️ a title containing `, ` cannot be split back |
| Date | The journal's title, **without** brackets: `* Due Date:: Sep 12th, 2026` | ⚠️ no link; `, ` inside the value |
| Date and time | `* meeting at:: Sep 12th, 2026 09:00`, local time | ⚠️ time zone lost |
| Tag and property lists on tag pages | `* Extends:: Root Tag`, `* Tag Properties:: color`, without brackets | ⚠️ |
| Icon | `* Icon:: {:type :emoji, :id "books"}` (EDN) | ⚠️ |
| Hidden properties (yours or built-in) | Not written ([`file.cljs:180-181`](../../../../../src/main/logseq/common/export/file.cljs)) | ❌ |
| Task status | Not a property line; it is the status word | ✅ |
| Page tags, aliases, created/updated time | Not written | ❌ |

- **Property order** follows Logseq's internal property order, not alphabetical and not the order
  you added them. It is stable, but not obvious.
- **The label is the property's display name**, so two properties with the same name are
  written identically ⚠️.
- **Visible built-in properties that can appear** (not hidden): Description, Icon, Priority,
  Scheduled, Deadline, Assignee, Extends, Tag Properties, Hide from Node, Enable bidirectional
  properties, Bidirectional property title, Enable property history, Hide empty value, Apply template
  to tags, External URL, Published URL, the three Repeating properties, Due and State (flashcards),
  Title Format.

---

## Special blocks

| In Logseq | Stored | Mirror writes | Mark |
|-----------|--------|---------------|------|
| Code block (`/Code block`, or a block that is only a ```` ``` ```` fence) | `display-type :code`, `code/lang`, text without fences | ```` - ```clojure ```` … ```` ``` ````, indented under the bullet | ✅ |
| Code containing ```` ``` ```` | same | The fence grows to one backtick more than the longest run inside: ```` ````md ```` ([`file.cljs:254-262`](../../../../../src/main/logseq/common/export/file.cljs)) | ✅ |
| Code block with a tag | + `:block/tags` | The tag is added to the opening fence: ```` - ```js #Book ````, or ```` - ``` #Book ```` with no language | ⚠️ the tag becomes part of the fence's info string |
| Quote (`/Quote`, or type `>` alone) | `display-type :quote` | `- > line` for each line; blank lines become `>` | ⚠️ identical to a text block that starts with `> ` |
| Math block (`/Math block`, or a block that is only `$$…$$`) | `display-type :math` | `$$` / formula / `$$` | ⚠️ and see [Known bugs](#known-bugs) |
| Query (`/Query`, `/Advanced Query`) | empty text + hidden `query` property | `-` | ❌ the query is lost |
| Asset (image, PDF… pasted or uploaded) | block titled with the file name, `#Asset`, hidden asset properties; file in `<graph>/assets/<uuid>.<ext>` | `- screenshot 2026-09-12` | ❌ no link to the file |
| Reference to an asset | `[[<asset-uuid>]]` | `[[a0000000-…]]` | ⚠️ unresolvable from the mirror |
| Flashcard | `#Card` + `{{cloze answer}}` in text | `- What is {{cloze mirror}}?` | ⚠️ `#Card` dropped |
| Template | `#Template` + children | the block and children, no tag | ⚠️ |

---

## Tables, callouts, footnotes, math

All of these live in the block text, so they are written unchanged and indented under the
bullet ✅.

| Syntax | Renders in DB? | Notes |
|--------|:--------------:|-------|
| Pipe table `\| a \| b \|` with `\| --- \|` | ✅ | Alignment `:-:` is ignored |
| `#+BEGIN_NOTE` / `TIP` / `IMPORTANT` / `CAUTION` / `WARNING` / `PINNED` … `#+END_…` | ✅ | Logseq callouts |
| `#+BEGIN_CENTER`, `#+BEGIN_EXAMPLE`, `#+BEGIN_EXPORT html`, `#+BEGIN_SRC` | ✅ | |
| `---` | ✅ | Horizontal rule |
| `> quote` inside a block | ✅ | |
| Obsidian callout `> [!note]` | ❌ | Renders as a quote with literal `[!note]` |
| `* item`, `1. item`, `* [ ] task` inside a block | ✅ | `- item` and `- [ ] task` inside a block do **not** render as a list |
| Footnote `[^1]` and `[^1]: text` | ✅ | |
| `$$ … $$` inside a block, `\begin{equation}` | ✅ | |
| Mermaid / diagrams | ❌ | Plugin only |

Sources: [`block.cljs:4795-5180`](../../../../../src/main/frontend/components/block.cljs).

---

## HTML, hiccup, comments and escaping

| Syntax | Renders in DB? | Notes |
|--------|:--------------:|-------|
| `<span>`, `<kbd>`, `<u>`, block `<div>…</div>` | ✅ | Sanitized |
| `@@html: … @@` | ✅ | `/Embed HTML` inserts it |
| Hiccup `[:div [:b "x"]]` | ✅ | |
| `<!-- comment -->` | ✅ hidden | |
| `%%comment%%` (Obsidian) | ❌ | Shown literally |
| `\*`, `\#` | ✅ | |
| `\[` | ⚠️ | Starts display math, not an escaped bracket |

---

## Macros

Macros are stored and written as typed ✅.

| Macro | Renders in DB? |
|-------|:--------------:|
| `{{video url}}`, `{{youtube …}}`, `{{vimeo …}}`, `{{bilibili …}}` | ✅ |
| `{{youtube-timestamp 90}}` | ✅ |
| `{{tweet url}}`, `{{twitter url}}` | ✅ |
| `{{cloze answer}}` | ✅ |
| `{{function …}}` | ✅ inside query results |
| `{{renderer …}}` | ✅ with plugins |
| `{{img url, w, h, class}}` and your own `:macros` in config.edn | ✅ |
| `{{embed …}}`, `{{query …}}`, `{{namespace …}}` | ❌ deprecation warning |
| `{{today}}`, `{{date}}`, `{{pdf}}`, anything else | ❌ "Unsupported macro name" |

Source: [`block.cljs:1877-1946`](../../../../../src/main/frontend/components/block.cljs).

---

## Syntax that does not work in DB graphs

These are file-graph habits. In a DB graph they stay plain text, and the mirror writes them as
plain text.

| Written | In a file graph | In a DB graph, use |
|---------|-----------------|--------------------|
| `TODO`, `DOING`, `LATER`, `NOW` | Task marker | `/Todo`, `/Doing`, Cmd/Ctrl+Enter |
| `[#A]` | Priority | `/Priority Urgent` … |
| `SCHEDULED: <2026-09-12>` | Scheduled date | `/Scheduled` |
| `key:: value` | Property | `/Add property` or Cmd/Ctrl+P |
| `((uuid))` | Block reference | "Copy block ref" → `[[uuid]]` |
| `{{embed [[Page]]}}` | Embed | `/Node embed` |
| `{{query …}}`, `#+BEGIN_QUERY` | Query | `/Query`, `/Advanced Query` |

---

## Two-way readiness

A two-way mirror has to read these files back. Everything below is either **lost** (the file does
not contain it) or **ambiguous** (two different graph states write the same text).

### Lost

| What | Where |
|------|-------|
| Page tags and aliases | [Page file anatomy](#page-file-anatomy) |
| Block UUIDs (so block references and asset references cannot be resolved) | [Links](#links-references-and-tags) |
| Page embeds, queries, asset files, background colour, collapse state | [Embeds](#embeds), [Special blocks](#special-blocks) |
| Hidden properties, created/updated times | [Properties](#properties) |
| Built-in tags Card, Template, Query, Asset | [Links](#links-references-and-tags) |
| Leading and trailing spaces and blank lines in a block | [Blocks](#blocks-and-nesting) |
| Time zone of scheduled, deadline and datetime values | [Tasks](#tasks) |
| Page title characters replaced in file names | [Files](#files-and-folders) |

### Ambiguous

| The file says | It could be |
|---------------|-------------|
| `- TODO text` | a Todo task, or text starting with "TODO" |
| `- ## text` | a Heading 2, an auto heading, or text starting with "## " |
| `- > text` | a quote block, or a text block starting with "> " |
| `- text #Tag` | an inline tag, or a tag added in the picker |
| `  * note:: value` | a real property, or a second line of text that starts with `* note::` |
| `  - line` under a block | a child block, or a later line of the same block starting with `- ` |
| `- Target block` followed by its children | the block itself, or a block embed of it |
| `[[Foo]]` | one of several pages titled Foo |
| `* people:: [[A, B]]` | node titles containing `, ` |
| ```` - ```js #Book ```` | a `js` code block tagged Book, or a fence whose info string is `js #Book` |

### What the proposed importer would read

An unmerged upstream branch, `feat/two-way-sync-between-db-and-markdown` (last commit 2026-05-07,
ADR 0017 and ADR 0018, both **Proposed**), sketches the contract:

- The DB stays the source of truth. A file edit becomes normal outliner operations.
- Blocks, text and nesting are imported. **Task status is imported through the status word**.
- **All other property edits are ignored**, including `* name::` lines and their value blocks.
- Deleting, moving or renaming a mirror file does not delete, move or rename the page.
- Two-way mode is a separate opt-in and is refused for collaborative graphs.

This is a design proposal, not shipped behaviour. It does mean the safest edits in an external
editor today are block text, nesting and the status word.

---

## Known bugs

Found while building this page. Reproduced on `be800f1711` and on desktop 2.0.1.

1. **A math block, or a block that starts with a blank line, breaks the rest of the page.** After
   one, later blocks on the same page lose their task status word, their tags and their `1.`
   numbering, and block embeds after it are not expanded.

   ```markdown
   - DOING before task
   - $$
     x^2
     $$
   - after task          ← should be "- DONE after task"
   - numbered after      ← should be "1. numbered after"
   ```

   Cause: the renderer matches each written line to its block by the block's first line of text.
   Code fences have an exception. A math block's written first line is `$$`, and a block whose
   text starts with a blank line is written trimmed, so neither matches, and every block after it
   is compared against the wrong block
   ([`markdown_mirror.cljs:364-379`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)).
   The math case was reproduced on both builds; the blank-line case on `be800f1711`.

2. **A tag on a code block is written into the opening fence** (```` ```js #Book ````), so
   Markdown tools read it as part of the language.

3. **"No priority" writes an empty value block** (`* Priority::` followed by `    - `).

---

## Reading the mirror in other tools

- **Wiki links may not match file names.** `[[Projects/Alpha]]` is written in the text but the
  file is `Projects_Alpha.md`. Journal links use the journal title (`[[Sep 12th, 2026]]`) but the
  file is `2026_09_12.md`. Duplicate titles give `Name (2).md`.
- **`id::` and `* name::` lines are plain text** in other editors. `*` items render as a bullet
  list under the block.
- **Nested items under `1.`** use two spaces, which CommonMark tools may not nest.
- **Logseq-only syntax** (`#+BEGIN_NOTE`, `{{video}}`, `==highlight==` or `^^highlight^^`,
  `[[uuid]]`, `{:width 300}`) shows up as literal text.

---

## Version notes

Written against `origin/master` `be800f1711`. These mirror changes landed **after** desktop 2.0.1
(`b09316a`, 2026-07-13), so 2.0.1 output can differ:

| Commit | Change |
|--------|--------|
| `aaf189f464` (2026-08-17) and follow-ups | `#Task` blocks with the default status get `TODO` |
| `a600fcfe9c` (2026-08-26) | Children of text property values are written |
| `748c6fcf77` (2026-09-07) | The Contents page is mirrored |
| `928e093bce` (2026-09-08) | Pages that link to a renamed page are rewritten |
