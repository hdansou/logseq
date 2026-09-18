---
title: 'Markdown Syntax Reference'
status: 🟢 Stable
version: 'DB graph schema 65.33'
quadrant: 'Reference'
platform: 'Desktop + Mobile'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
verifiedScope: 'Full audit 2026-09-10 (c0e4573a15). The 2026-09-14 date covers only the highlight markup: both ==text== and ^^text^^ render as <mark>, checked in the running app.'
reviewStatus: audited
related-docs:
  - '[Slash Commands](../interface/slash-commands.md)'
  - '[Keyboard Shortcuts](../interface/keyboard-shortcuts.md)'
  - '[Basic Editing](../../tutorials/getting-started/03-basic-editing.md)'
tags: ['markdown', 'formatting', 'syntax', 'reference']
---

# Markdown Syntax Reference

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

Reference for the markup Logseq accepts inside a block. Covers text styling, links, code, tables and the Logseq-specific forms, and marks the forms that are deprecated in DB graphs.

**Quick Access:** Type `/` in any block for formatting commands.

## How Logseq parses a block

Logseq does not use a CommonMark parser. Block titles are parsed by [mldoc](https://github.com/logseq/mldoc), an OCaml Org/Markdown parser wrapped at [`deps/graph-parser/src/logseq/graph_parser/mldoc.cljc`](../../../../../deps/graph-parser/src/logseq/graph_parser/mldoc.cljc), and the resulting AST is rendered by [`src/main/frontend/components/block.cljs`](../../../../../src/main/frontend/components/block.cljs). A form only "works" if `block.cljs` has a branch for it — this page lists the branches that exist at the revision in the frontmatter. mldoc itself is an external dependency, so exactly which characters it accepts for a given form cannot be confirmed from this repository; where that matters it is flagged.

Two consequences shape everything below:

- **Some markdown is input syntax, not storage.** In a DB graph, headings, code blocks and display math are stored as node properties (`:logseq.property/heading`, `:logseq.property.node/display-type`), not as `#`, ` ``` ` or `$$` characters. Typing the markdown is a shorthand that gets normalized away (see [Headings](#headings) and [Code](#code)).
- **Several Org-style forms and macros are deprecated** and now render an in-app warning instead of content. They are listed with their warning text rather than removed from this page, because you will still meet them in imported graphs.

---

## Table of Contents

1. [Text Formatting](#text-formatting)
2. [Headings](#headings)
3. [Lists](#lists)
4. [Links](#links)
5. [Code](#code)
6. [Tables](#tables)
7. [Block Elements](#block-elements)
8. [Logseq-Specific Syntax](#logseq-specific-syntax)
9. [Special Characters](#special-characters)

---

## Text Formatting

### Basic Formatting

mldoc's inline emphasis kinds are exactly `Bold`, `Italic`, `Underline`, `Strike_through` and `Highlight` ([`schema/mldoc.cljc:72`](../../../../../deps/graph-parser/src/logseq/graph_parser/schema/mldoc.cljc)), mapped to `<b>`, `<i>`, `<ins>`, `<del>` and `<mark>` by `emphasis-cp` at [`block.cljs:1948-1956`](../../../../../src/main/frontend/components/block.cljs).

| Style         | Syntax                | Renders as | Shortcut       |
| ------------- | --------------------- | ---------- | -------------- |
| Bold          | `**text**`            | `<b>`      | `mod+b`        |
| Italic        | `*text*`              | `<i>`      | `mod+i`        |
| Strikethrough | `~~text~~`            | `<del>`    | `mod+shift+s`  |
| Highlight     | `^^text^^` or `==text==` | `<mark>` | `mod+shift+h` (inserts `==`) |
| Inline code   | `` `text` ``          | `<code>`   | –              |
| Underline     | `<ins>text</ins>`     | `<ins>`    | – (`/Underline`) |
| Superscript   | `^{text}`             | `<sup>`    | –              |
| Subscript     | `_{text}`             | `<sub>`    | –              |

- Inline code: the `Verbatim`/`Code` inline branch renders `[:code s]` ([`block.cljs:2033-2034`](../../../../../src/main/frontend/components/block.cljs)).
- Superscript/subscript: the `Superscript` and `Subscript` branches render `<sup>`/`<sub>` ([`block.cljs:1995-1998`](../../../../../src/main/frontend/components/block.cljs)). The `^{…}`/`_{…}` spelling is mldoc's and could not be confirmed in-repo.
- Underline: there is no markdown underline delimiter on the producer side — `config/get-underline` returns `""` for markdown, commented "no underline for markdown" ([`frontend/config.cljs:221-227`](../../../../../src/main/frontend/config.cljs)). `/Underline` inserts literal `<ins></ins>` and is only offered when the preferred format is markdown ([`commands.cljs:241-245`](../../../../../src/main/frontend/commands.cljs)); the tag renders through the sanitized `Inline_Html` branch ([`block.cljs:2050-2053`](../../../../../src/main/frontend/components/block.cljs)).

`mod` is `⌘` on macOS and `Ctrl` elsewhere. The four keyboard bindings are declared at [`modules/shortcut/config.cljs:119-129`](../../../../../src/main/frontend/modules/shortcut/config.cljs); the characters they wrap the selection in come from `get-bold` / `get-italic` / `get-strike-through` / `get-highlight` at [`frontend/config.cljs:206-240`](../../../../../src/main/frontend/config.cljs).

Three corrections to folklore about these shortcuts:

- There is **no `mod+e` for inline code**. On macOS `mod+e` is `:editor/quick-add` (on other platforms quick-add is `mod+alt+e`) ([`shortcut/config.cljs:444-446`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).
- There is **no `mod+k` for links**. `mod+k` is global search (`:go/search`, [`shortcut/config.cljs:304-305`](../../../../../src/main/frontend/modules/shortcut/config.cljs)). The link shortcut is `mod+l` (`:editor/insert-link`, [`:241-242`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).
- Highlight is `mod+shift+h`, not `mod+h`.

> **Verified 2026-09-14:** the highlight shortcut and the documented markup differ, but both render. Typing `^` twice auto-pairs into `^^…^^` ([`handler/editor.cljs:3457-3459`](../../../../../src/main/frontend/handler/editor.cljs)), the in-app syntax help shows `^^highlight^^` ([`components/shortcut_help.cljs:47`](../../../../../src/main/frontend/components/shortcut_help.cljs)) and the file-graph importer writes `^^…^^` ([`graph_parser/exporter.cljs:1233-1234`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs)) — but `highlight-format!` passes the block's format (defaulting to `:markdown`) to `config/get-highlight` ([`handler/editor.cljs:171-174`](../../../../../src/main/frontend/handler/editor.cljs)), which returns `"=="` for `:markdown` ([`frontend/config.cljs:236-240`](../../../../../src/main/frontend/config.cljs)), so `mod+shift+h` wraps the selection in `==`. **Both forms work:** checked in the app on 2026-09-14, `mod+shift+h` highlights the selection, and `==text==` and `^^text^^` both render as `<mark>` (see also [Markdown Mirror Syntax](../syntax/markdown-mirror-syntax.md) and [Paste and Type Syntax](../syntax/paste-and-type-syntax.md)). Nothing is broken here; only the inserted syntax differs from what the syntax help and the importer use. Either is safe to type, and `^^…^^` is the form the rest of the app presents.

**Examples:**

```markdown
This is **bold text** and this is *italic*.
You can ~~cross things out~~ or ^^highlight them^^.
Use `inline code` for commands or variables.
H_{2}O and E = mc^{2}.
```

### Combining Styles

Emphasis nodes contain further inline nodes (`[:sequential [:ref ::inline]]`, [`schema/mldoc.cljc:72-73`](../../../../../deps/graph-parser/src/logseq/graph_parser/schema/mldoc.cljc)), and `emphasis-cp` renders its children with `map-inline`, so styles nest. Inline code renders its string as-is, so markup inside backticks stays literal.

```markdown
**Bold and *italic***
~~**Struck through bold**~~
^^**Highlighted bold**^^
`**Bold code**` (bold won't render inside code)
```

---

## Headings

### Heading is a property, not markup

In a DB graph a heading is the `:logseq.property/heading` property on the block, holding the level `1`–`6`. Typing `# ` … `###### ` at the start of a block still works, but as an **input shorthand**: `wrap-parse-block` detects the leading `#{1,6}\s+` (`markdown-heading-level`, [`handler/db_based/editor.cljs:47-52`](../../../../../src/main/frontend/handler/db_based/editor.cljs)), strips it from the title ([`:105-109`](../../../../../src/main/frontend/handler/db_based/editor.cljs), via `clear-markdown-heading` at [`deps/common/src/logseq/common/util.cljs:327-332`](../../../../../deps/common/src/logseq/common/util.cljs)) and sets the property instead ([`:125-126`](../../../../../src/main/frontend/handler/db_based/editor.cljs)). The `#` characters are not stored.

Normalization is skipped for blocks whose `:logseq.property.node/display-type` is `:code` or `:math`, so a `#` inside a code block stays literal ([`db_based/editor.cljs:54-56`](../../../../../src/main/frontend/handler/db_based/editor.cljs)).

```markdown
# Heading 1
## Heading 2
### Heading 3
#### Heading 4
##### Heading 5
###### Heading 6
```

### Slash commands

The heading commands are generated from `:editor.slash/heading-label` for levels 1–6, plus a "Normal text" entry that clears the property (`headings`, [`commands.cljs:198-210`](../../../../../src/main/frontend/commands.cljs), added to the menu at [`:259`](../../../../../src/main/frontend/commands.cljs); labels at [`src/resources/dicts/en.edn:542`, `:553-554`](../../../../../src/resources/dicts/en.edn)):

```text
/Normal text   → clears heading, back to body text
/Heading 1 … /Heading 6
```

There are no `/h1`, `/h2`, `/h3` aliases — the command names are the full labels above.

**Best Practices:**

- One `#` per level, with a space after it (`#Heading` is not a heading; the pattern requires `\s+`).
- Use levels hierarchically and keep headings short.
- To remove a heading, use `/Normal text` rather than deleting `#` characters — the characters are not what is stored.

---

## Lists

In a DB graph every bullet in the outline is a separate block. The lists below are markdown lists written *inside a single block's text*, which mldoc parses and `block.cljs` renders as `<ul>`/`<ol>` (`list-item`, [`block.cljs:4852-4872`](../../../../../src/main/frontend/components/block.cljs)).

### Unordered Lists

**Syntax:**

```markdown
- Item 1
- Item 2
  - Nested item 2.1
  - Nested item 2.2
    - Deep nested 2.2.1
- Item 3
```

> **Unverified:** whether mldoc also accepts `*` as a list bullet inside a block could not be confirmed from this repository.

### Ordered Lists

**Syntax:**

```markdown
1. First item
2. Second item
   1. Nested first
   2. Nested second
3. Third item
```

Numbered items keep their number: `list-item` sets `:value` on the `<li>` when mldoc reports one ([`block.cljs:4856-4860`](../../../../../src/main/frontend/components/block.cljs)).

**Numbered blocks (not markdown):** to number a run of sibling *blocks* rather than lines inside one block, use the LIST TYPE slash commands, which toggle a property on the block:

```text
/Number list      → :editor/toggle-own-number-list
/Number children  → :editor/toggle-children-number-list
```

([`commands.cljs:306-316`](../../../../../src/main/frontend/commands.cljs); labels at [`en.edn:555-556`](../../../../../src/resources/dicts/en.edn))

### Task Lists

**Markdown checkboxes** work inside a single block. `list-item` renders an interactive checkbox whenever mldoc reports one ([`block.cljs:4865-4872`](../../../../../src/main/frontend/components/block.cljs)); clicking it calls `list-checkbox` ([`:2417-2427`](../../../../../src/main/frontend/components/block.cljs)), which rewrites `[ ]` ↔ `[x]` in the block title and saves it (`toggle-list-checkbox`, [`handler/editor.cljs:2604-2613`](../../../../../src/main/frontend/handler/editor.cljs)):

```markdown
- [ ] Unchecked task
- [x] Checked task
```

**Logseq task markers are file-graph syntax and do nothing in a DB graph.** Typing `TODO`, `DOING`, `LATER`, `NOW`, `WAITING` or `CANCELLED` at the start of a block leaves plain text: `:block/marker` is not an attribute of a DB graph, and the block parser only turns `marker`/`priority` into refs when the graph is *not* DB-based ([`deps/graph-parser/src/logseq/graph_parser/block.cljs:498`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs)). A DB-graph task is the `:logseq.property/status` property, a closed-value ref with exactly six values ([`deps/db/src/logseq/db/frontend/property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)):

| Status    | `:db/ident`                         |
| --------- | ----------------------------------- |
| Backlog   | `:logseq.property/status.backlog`   |
| Todo      | `:logseq.property/status.todo`      |
| Doing     | `:logseq.property/status.doing`     |
| In Review | `:logseq.property/status.in-review` |
| Done      | `:logseq.property/status.done`      |
| Canceled  | `:logseq.property/status.canceled`  |

Set them with the TASK STATUS slash commands, which are generated from those closed values rather than hard-coded (`get-statuses`, [`commands.cljs:150-165`](../../../../../src/main/frontend/commands.cljs), added to the menu at [`:262`](../../../../../src/main/frontend/commands.cljs)):

```text
/Backlog  /Todo  /Doing  /In Review  /Done  /Canceled
```

`mod+enter` (`:editor/cycle-todo`, [`shortcut/config.cljs:167`](../../../../../src/main/frontend/modules/shortcut/config.cljs)) cycles the status property via `db-based-cycle-todo!` ([`handler/editor.cljs:825-838`](../../../../../src/main/frontend/handler/editor.cljs)).

Priority is the same shape — four closed values (`:logseq.property/priority.low|.medium|.high|.urgent`, [`property.cljs:334-337`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)) exposed as `/Priority Low` … `/Priority Urgent` plus `/No priority` (`get-priorities`, [`commands.cljs:171-189`](../../../../../src/main/frontend/commands.cljs); labels `"Priority {1}"` and `"No priority"` at [`en.edn:558`, `:548`](../../../../../src/resources/dicts/en.edn)).

---

## Links

### Page Links (Wiki-style)

**Syntax:**

```markdown
[[Page Name]]
[[Page/Nested/Name]]
```

The delimiters are defined at [`deps/common/src/logseq/common/util/page_ref.cljs:7-9`](../../../../../deps/common/src/logseq/common/util/page_ref.cljs); `link-cp` dispatches the `Page_ref` URL type at [`block.cljs:1576-1580`](../../../../../src/main/frontend/components/block.cljs). `/Node reference` inserts `[[]]` and opens the page search ([`commands.cljs:227-231`](../../../../../src/main/frontend/commands.cljs)); typing `[[` does the same through `autopair` ([`handler/editor.cljs:1976-1980`](../../../../../src/main/frontend/handler/editor.cljs)).

**`[[Page|Display Text]]` is not Logseq syntax.** Nothing in `page_ref.cljs`, the graph-parser block/text code or `block.cljs` splits a page ref on `|`; `get-page-name` takes everything between the brackets with `page-ref-any-re`, `#"\[\[(.*)\]\]"` ([`page_ref.cljs:15`](../../../../../deps/common/src/logseq/common/util/page_ref.cljs), used at [`:42-43`](../../../../../deps/common/src/logseq/common/util/page_ref.cljs)). To give a page a second name, use the built-in **Alias** property (`:block/alias`, [`deps/db/src/logseq/db/frontend/property.cljs:68-69`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

**Nested titles create one page per segment.** `[[Projects/Work/Client A]]` does not create a single page with slashes in its name: the title is split and each segment becomes its own page under **Library**, with the leaf page titled `Client A` ([`deps/outliner/src/logseq/outliner/page.cljs:464-467`](../../../../../deps/outliner/src/logseq/outliner/page.cljs)).

**Create pages:**

Saving a block that contains `[[New Page]]` builds the referenced page: the block parser maps each ref to a page entity (`ref->map` → `page-name->map`, [`graph_parser/block.cljs:477`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs)) and `wrap-parse-block` merges those refs into the saved block ([`handler/db_based/editor.cljs:116-134`](../../../../../src/main/frontend/handler/db_based/editor.cljs)).

Page names are matched case-insensitively: the stored `:block/name` is the lower-cased title (`page-name-sanity-lc`, [`deps/common/src/logseq/common/util.cljs:135-138`](../../../../../deps/common/src/logseq/common/util.cljs)).

### External Links

**Syntax:**

```markdown
[Link Text](https://example.com)
https://example.com (auto-linked)
```

`/Link` runs `link-steps` ([`commands.cljs:51`](../../../../../src/main/frontend/commands.cljs), menu entry at [`:239`](../../../../../src/main/frontend/commands.cljs)) and `mod+l` runs `html-link-format!` ([`shortcut/config.cljs:241-242`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

> **Unverified:** earlier revisions also listed a link title (`[text](url 'title')`) and angle-bracket autolinks (`<https://…>`). Both are mldoc parsing questions that could not be confirmed from this repository.

**Examples:**

```markdown
Visit [Logseq](https://logseq.com)
Auto: https://logseq.com
```

### Block References

**Syntax:** in a DB graph a block reference uses the same brackets as a page link, with the
block's UUID inside:

```markdown
[[66d1c9f0-1f6e-4b2c-9c4f-0e2a5b8d1234]]
```

"Copy block ref" puts exactly this on the clipboard: the menu item calls `copy-block-ref!` with
`ref/->block-ref` ([`components/content.cljs:289-291`](../../../../../src/main/frontend/components/content.cljs)),
which is `page-ref/->page-ref`
([`frontend/util/ref.cljs:5-7`](../../../../../src/main/frontend/util/ref.cljs)).

> **`((uuid))` is file-graph syntax.** The mldoc schema declares `Block_ref` as a link type
> ([`schema/mldoc.cljc:52`](../../../../../deps/graph-parser/src/logseq/graph_parser/schema/mldoc.cljc)),
> and the block renderer's `link-cp` turns a `Block_ref` link back into the literal string `((id))`
> ([`components/block.cljs:1573-1574`](../../../../../src/main/frontend/components/block.cljs))
> instead of rendering a reference. Pasting `((uuid))` does not convert it either: the paste
> handler only strips the parentheses when you paste inside an existing `(( ))`
> ([`handler/paste.cljs:170-173`](../../../../../src/main/frontend/handler/paste.cljs)). The strict
> `((uuid))` regex still exists
> ([`common/util/block_ref.cljs:9`](../../../../../deps/common/src/logseq/common/util/block_ref.cljs)),
> but it no longer defines how you reference a block. mldoc itself is an external parser, so the
> `Block_ref` classification follows from the schema rather than from a run.

**Create block reference:**

```text
1. Right-click the block bullet → "Copy block ref"
2. Paste into another block
```

"Copy block ref" is an item in `block-context-menu-content` ([`components/content.cljs:288-292`](../../../../../src/main/frontend/components/content.cljs); label at [`en.edn:116`](../../../../../src/resources/dicts/en.edn)).

**Typing `((` does not open a block search.** In `autopair`, a `((` prefix shows the warning "To reference a node, please use `[[]]`." ([`handler/editor.cljs:1982-1985`](../../../../../src/main/frontend/handler/editor.cljs); text at [`en.edn:503`](../../../../../src/resources/dicts/en.edn)). Use `[[` and search for the block instead. (A second, identical `(= prefix block-ref/left-parens)` test at [`:1987-1990`](../../../../../src/main/frontend/handler/editor.cljs) would open the block search, but `cond` never reaches it.)

### Block and Page Embeds

**`{{embed ...}}` is deprecated.** In a DB graph it renders a warning rather than the embedded content:

> `{{embed}} is deprecated. Use '/Node embed' command instead.`

([`block.cljs:1934-1935`](../../../../../src/main/frontend/components/block.cljs); message at [`src/resources/dicts/en.edn:157`](../../../../../src/resources/dicts/en.edn))

Use the slash command instead:

```text
/Node embed   → opens the block/page search and embeds the chosen node
```

It runs `db-based-embed-block`, which triggers `[:editor/search-block :embed]` ([`commands.cljs:105-108`](../../../../../src/main/frontend/commands.cljs), wired in at [`:233-236`](../../../../../src/main/frontend/commands.cljs)).

---

## Code

### Inline Code

**Syntax:**

```markdown
Use `code` for inline code
Or `` code with `backticks` inside ``
```

> **Unverified:** the double-backtick form for code containing a backtick is an mldoc parsing question and could not be confirmed from this repository.

**Examples:**

```markdown
Run `npm install` to install packages.
```

### Code Blocks

**Syntax:**

````markdown
```language
code here
```
````

**Supported Languages:**

The language list is CodeMirror's, not a Logseq list. Modes are imported at [`src/main/frontend/extensions/code.cljs:7-128`](../../../../../src/main/frontend/extensions/code.cljs) and the in-app language picker enumerates `window.CodeMirror.modeInfo` directly ([`block.cljs:4932-4934`](../../../../../src/main/frontend/components/block.cljs)), so whatever appears in that picker is what is supported. The imported modes include clojure, javascript, python, ruby, go, rust, the clike family (C/C++/C#/Java/Kotlin/Scala), shell, sql, css, xml, yaml, diff, dockerfile and many more. The fence language is lower-cased before use ([`block.cljs:5167`](../../../../../src/main/frontend/components/block.cljs)).

**Examples:**

````markdown
```javascript
function hello() {
  console.log('Hello World')
}
```

```python
def hello():
    print("Hello World")
```

```bash
#!/bin/bash
echo "Hello World"
```
````

### Code blocks are nodes, not fences

`/Code block` does not insert three backticks. It runs `code-block-steps`, which upserts the block to `:logseq.property.node/display-type :code` and exits the editor ([`commands.cljs:134-138`](../../../../../src/main/frontend/commands.cljs), menu entry at [`:246-249`](../../../../../src/main/frontend/commands.cljs)); the language is stored in `:logseq.property.code/lang`, which the language picker writes ([`block.cljs:4998`](../../../../../src/main/frontend/components/block.cljs)).

Typing a fence gets normalized to the same node form **when the fence is the whole block**: `standalone-display-block` matches a title that starts and ends with ` ``` `, and turns the `Src` node into `:logseq.property.node/display-type :code`, with the fence body as the title and the fence language as `:logseq.property.code/lang` ([`frontend/format/block.cljs:18-46`](../../../../../src/main/frontend/format/block.cljs), applied at [`:60`](../../../../../src/main/frontend/format/block.cljs)); `wrap-parse-block` keeps that parsed title ([`handler/db_based/editor.cljs:116-124`](../../../../../src/main/frontend/handler/db_based/editor.cljs)). A fence mixed with other text stays in the title and is rendered in place by the `Src` branch through `src-cp` ([`block.cljs:5166-5174`](../../../../../src/main/frontend/components/block.cljs)).

### Evaluating Clojure

A `clojure` code block whose options include `:results` is evaluated with SCI and its result rendered below the block ([`block.cljs:5013-5015`](../../../../../src/main/frontend/components/block.cljs)):

````markdown
```clojure :results
(+ 1 2)
```
````

> Removed claim: earlier revisions of this page documented ` ```javascript {numberLines: true} ` and ` ```javascript {highlight: [2, 3]} `. Neither `numberLines` nor `highlight: [` appears anywhere under `src/` or `deps/` — there is no reader for either option.

---

## Tables

### Basic Table

**Syntax:**

```markdown
| Header 1 | Header 2 | Header 3 |
| -------- | -------- | -------- |
| Cell 1   | Cell 2   | Cell 3   |
| Cell 4   | Cell 5   | Cell 6   |
```

mldoc's `Table` node is rendered by `table` ([`block.cljs:5072-5073`](../../../../../src/main/frontend/components/block.cljs), function at [`:4876-4918`](../../../../../src/main/frontend/components/block.cljs)), which emits a `<thead>` from the header row and one `<tbody>` per row group.

### Alignment

**Column alignment is not rendered.** `table` reads only `header`, `groups` and `col_groups` from the table node, and gives every `<th>`, `<td>` and `<col>` the fixed class `org-left` ([`block.cljs:4877-4891`](../../../../../src/main/frontend/components/block.cljs)). Alignment markers such as `:---:` or `---:` in the separator row have no effect on how the table renders.

### Table Tips

- Columns don't need to line up in the source.
- Cells render inline markup — each cell goes through `map-inline` ([`block.cljs:4887`](../../../../../src/main/frontend/components/block.cljs)):

```markdown
| Name     | Status | Link     |
|----------|--------|----------|
| **Bold** | `code` | [[Page]] |
```

---

## Block Elements

### Blockquotes

**Markdown `>` still works** and renders a `<blockquote>` ([`block.cljs:5081-5084`](../../../../../src/main/frontend/components/block.cljs)):

```markdown
> Single line quote

> Multiple line quote
> continues here
```

**`#+BEGIN_QUOTE` is deprecated.** Any block whose raw content starts with `#+BEGIN_QUOTE` (case-insensitive) is flagged at [`block.cljs:2845-2846`](../../../../../src/main/frontend/components/block.cljs) and renders:

> `#+BEGIN_QUOTE is deprecated. Use '/Quote' command instead.`

([`src/resources/dicts/en.edn:121`](../../../../../src/resources/dicts/en.edn))

**Slash command:** `/Quote` — it does not insert `>`; it sets `:logseq.property.node/display-type :quote` on the block ([`commands.cljs:140-143`](../../../../../src/main/frontend/commands.cljs); label `:class.built-in/quote-block` at [`en.edn:208`](../../../../../src/resources/dicts/en.edn)).

### Horizontal Rules

`---` on its own parses to `Horizontal_Rule` and renders `<hr>` ([`block.cljs:5053-5054`](../../../../../src/main/frontend/components/block.cljs)); the same string is what `config/get-hr` returns for markdown ([`frontend/config.cljs:198-204`](../../../../../src/main/frontend/config.cljs)), which the HTML-paste converter emits for `<hr>` ([`extensions/html_parser.cljs:155`](../../../../../src/main/frontend/extensions/html_parser.cljs)).

```markdown
---
```

> Removed claim: earlier revisions listed `/line` and `/divider` slash commands. Neither exists in `commands-map` ([`commands.cljs:220-376`](../../../../../src/main/frontend/commands.cljs)).

### Callouts/Admonitions

Org-style admonition blocks are still rendered. `markup-element-cp` has exactly six named admonition branches — **note, tip, important, caution, warning, pinned** ([`block.cljs:5110-5126`](../../../../../src/main/frontend/components/block.cljs)) — each dispatched to `ui/admonition`, which only produces output for those six names because it looks the icon up in a `case` and returns `nil` otherwise ([`src/main/frontend/ui.cljs:629-644`](../../../../../src/main/frontend/ui.cljs)):

```markdown
#+BEGIN_TIP
This is a tip!
#+END_TIP

#+BEGIN_NOTE
#+BEGIN_IMPORTANT
#+BEGIN_CAUTION
#+BEGIN_WARNING
#+BEGIN_PINNED
```

`#+BEGIN_CENTER` is a separate branch that wraps the content in `<div class="text-center">` rather than an admonition ([`block.cljs:5128-5131`](../../../../../src/main/frontend/components/block.cljs)).

**There are no `/tip`, `/warning`, `/important`, `/note` or `/caution` slash commands** — nothing in `commands-map` inserts an admonition. These must be typed.

**Custom blocks:** any other `#+BEGIN_FOO … #+END_FOO` falls through to a generic branch that emits `<div class="FOO">` around the content ([`block.cljs:5133-5137`](../../../../../src/main/frontend/components/block.cljs)). The `_options` after `#+BEGIN_FOO` are **discarded**, so `#+BEGIN_CUSTOM Custom Title` does not render a title — style the class from CSS instead.

**Two more Org blocks are deprecated** and render warnings instead of content:

| Block | Warning | Source |
| ----- | ------- | ------ |
| `#+BEGIN_QUERY` | `#+BEGIN_QUERY is deprecated. Use '/Advanced Query' command instead.` | [`block.cljs:5107-5108`](../../../../../src/main/frontend/components/block.cljs), [`en.edn:120`](../../../../../src/resources/dicts/en.edn) |
| `#+BEGIN_EXPORT latex` | `'#+BEGIN_EXPORT latex' is deprecated. Use '/Math block' command instead.` | [`block.cljs:5102-5105`](../../../../../src/main/frontend/components/block.cljs), [`en.edn:119`](../../../../../src/resources/dicts/en.edn) |

---

## Logseq-Specific Syntax

### Properties

**`key:: value` is not a property in a DB graph.** That is file-graph syntax. In a DB graph, properties are typed entities attached to the block; add one with `/Add property` ([`commands.cljs:354-358`](../../../../../src/main/frontend/commands.cljs); label at [`en.edn:306`](../../../../../src/resources/dicts/en.edn)), or type `;;` in the editor, which opens the same property picker (`[:editor/new-property]`, [`handler/editor.cljs:3453-3455`](../../../../../src/main/frontend/handler/editor.cljs)).

> Removed claim: earlier revisions showed `key:: value`, `tags:: [[Tag1]], [[Tag2]]`, `status:: in-progress` and a multi-line `description::` form as property syntax. None of them sets a property in a DB graph. Status and priority are the closed-value properties described under [Task Lists](#task-lists).

### Tags

**Syntax:**

```markdown
#tag
#tag-with-dashes
#TagWithCaps
#[[multi word tag]]
```

A `#tag` in a block title becomes a tag, not a plain page ref: the parser collects it as a structured tag ([`graph_parser/block.cljs:513-519`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs)) and, when no page by that name exists yet, builds it as a new class with `db-class/build-new-class` ([`:473-480`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs)). Brackets around the name are stripped with `page-ref-un-brackets!` ([`:514`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs)). A tag name must not contain `#`, a tab or a newline (`tag-valid?`, [`deps/common/src/logseq/common/util.cljs:46-49`](../../../../../deps/common/src/logseq/common/util.cljs)).

**Tags with `/` are dropped in a DB graph.** Before a tag is added, `db-namespace-page?` filters out any name containing `/` that is not a journal title ([`graph_parser/block.cljs:442-447`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs), applied at [`:515-516`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs); `namespace-page?` at [`deps/common/src/logseq/common/util/namespace.cljs:12-18`](../../../../../deps/common/src/logseq/common/util/namespace.cljs)). So `#work/client-a` does not tag the block. The same rule appears in the UI: converting a page that has a parent into a tag fails with "Namespaced pages can't be tags" ([`handler/db_based/page.cljs:44-45`](../../../../../src/main/frontend/handler/db_based/page.cljs), [`en.edn:1157`](../../../../../src/resources/dicts/en.edn)).

> Removed claim: earlier revisions listed a `/tag` slash command. There is no such entry in `commands-map` or in `en.edn`'s `:editor.slash/*` labels.

**Examples:**

```markdown
This is a #project note about #[[client a]]
Tagged with multiple: #important #review
```

### Page References

See [Page Links](#page-links-wiki-style). There is no `[[Full Page Name|Short Name]]` form.

> Removed claim: earlier revisions documented `[[Full Page Name|Short Name]]` and `[[Projects/Work/Alpha|Alpha]]` as an alias syntax. No code splits a page ref on `|`; see [Page Links](#page-links-wiki-style) and use the **Alias** property instead.

### Block Properties

> Removed claim: earlier revisions showed `id::`, `created-at::`, `updated-at::`, `type::` and `collapsed::` lines under a block as "common block properties". Those are file-graph conventions; in a DB graph they are not properties (see [Properties](#properties)).

### Commands and Macros

`{{name args}}` is dispatched by `macro-cp` ([`block.cljs:1877-1946`](../../../../../src/main/frontend/components/block.cljs)). The names with a built-in branch are exactly:

| Macro | Behaviour | Source |
| ----- | --------- | ------ |
| `{{youtube URL-or-ID}}` | YouTube embed; a bare argument is accepted as an ID only when it is 11 characters | [`block.cljs:1896-1897`](../../../../../src/main/frontend/components/block.cljs), [`extensions/video.cljs:60-64`](../../../../../src/main/frontend/extensions/video.cljs) |
| `{{youtube-timestamp N}}` | Seek link for the page's YouTube player | [`block.cljs:1899-1902`](../../../../../src/main/frontend/components/block.cljs) |
| `{{vimeo …}}`, `{{bilibili …}}` | Provider embeds | [`block.cljs:1913-1918`](../../../../../src/main/frontend/components/block.cljs) |
| `{{video URL}}` | Generic video embed; needs a URL | [`block.cljs:1920-1921`](../../../../../src/main/frontend/components/block.cljs), [`:1850-1863`](../../../../../src/main/frontend/components/block.cljs) |
| `{{tweet URL}}` / `{{twitter URL}}` | Tweet embed | [`block.cljs:1923-1932`](../../../../../src/main/frontend/components/block.cljs) |
| `{{function …}}` | Query function | [`block.cljs:1890-1891`](../../../../../src/main/frontend/components/block.cljs) |
| `{{renderer …}}` | Plugin renderer slot (only when plugins are enabled) | [`block.cljs:1937-1940`](../../../../../src/main/frontend/components/block.cljs) |
| `{{zotero-imported-file …}}`, `{{zotero-linked-file …}}` | Zotero attachment links | [`block.cljs:1904-1911`](../../../../../src/main/frontend/components/block.cljs) |
| `{{embed …}}` | **Deprecated** — renders a warning (see [Embeds](#block-and-page-embeds)) | [`block.cljs:1934-1935`](../../../../../src/main/frontend/components/block.cljs) |
| `{{query …}}` | **Deprecated** — renders "{{query}} is deprecated. Use '/Query' command instead." | [`block.cljs:1887-1888`](../../../../../src/main/frontend/components/block.cljs), [`en.edn:159`](../../../../../src/resources/dicts/en.edn) |
| `{{namespace …}}` | **Deprecated** — warning pointing to Library | [`block.cljs:1893-1894`](../../../../../src/main/frontend/components/block.cljs), [`en.edn:158`](../../../../../src/resources/dicts/en.edn) |

Any other name is looked up in the component-macro registry ([`block.cljs:1942-1943`](../../../../../src/main/frontend/components/block.cljs)) — the only in-repo registration is `{{cloze …}}` for flashcards ([`extensions/fsrs.cljs:475-477`, `:484`](../../../../../src/main/frontend/extensions/fsrs.cljs)) — and then in the text macros from `state/get-macros`, which merges the built-in `img` macro with the `:macros` map from config ([`state.cljs:512-519`](../../../../../src/main/frontend/state.cljs); substitution in `macro-else-cp`, [`block.cljs:1866-1875`](../../../../../src/main/frontend/components/block.cljs)).

Slash commands that insert macros: `/Embed Video URL` → `{{video }}`, `/Embed Twitter tweet` → `{{tweet }}`, `/Query function` → `{{function }}` ([`commands.cljs:345-352`, `:327-330`](../../../../../src/main/frontend/commands.cljs)).

**Queries are not a macro.** Use `/Query` and type the bare query, not a `{{query …}}` wrapper. `/Query` is at [`commands.cljs:325`](../../../../../src/main/frontend/commands.cljs); `/Advanced Query` at [`:326`](../../../../../src/main/frontend/commands.cljs).

**Examples:**

```markdown
{{youtube https://youtu.be/VIDEO}}
{{video https://example.com/clip.mp4}}
{{tweet https://twitter.com/user/status/123456}}
```

> Removed claim: earlier revisions listed `{{pdf URL}}`. There is no `pdf` branch in `macro-cp` and no `pdf` macro registration; PDFs are assets (`:logseq.property.asset/type "pdf"`, [`block.cljs:2293`](../../../../../src/main/frontend/components/block.cljs)).

---

## Special Characters

### Escaping Characters

> **Unverified:** backslash escaping (`\*`, `\#`, `\[`, `` \` `` and so on) is handled inside mldoc. No escape handling exists in `block.cljs`, so which characters can be escaped could not be confirmed from this repository.

```markdown
\* Not emphasis
\# Not a tag
\[ Not a link start
\` Not code
```

### HTML Entities

`block.cljs` renders mldoc's `Entity` node as sanitized HTML ([`block.cljs:2010-2012`](../../../../../src/main/frontend/components/block.cljs)).

> **Unverified:** which entity spellings mldoc recognizes (`&nbsp;`, `&copy;`, Org-style `\alpha`) could not be confirmed from this repository.

### Emoji

Unicode emoji are ordinary text and need no syntax:

```markdown
😀 🎉 ✅ ❌ ⚠️ 📝 📚 💡 🚀
```

---

## Advanced Formatting

### Nested Blocks

**Syntax:**

```markdown
- Parent block
  - Child block 1
    - Grandchild block
  - Child block 2
    - Another grandchild
```

**Indent:** `Tab` (`:editor/indent`)
**Outdent:** `Shift+Tab` (`:editor/outdent`)

([`shortcut/config.cljs:220-224`](../../../../../src/main/frontend/modules/shortcut/config.cljs))

### Mixed Content

```markdown
- Block with **bold** and *italic*
  - Code: `npm install`
  - Link: [[Page]]
  - Tag: #example
```

### Math (LaTeX)

**Inline Math:**

```markdown
$E = mc^2$
$\sum_{i=1}^{n} x_i$
```

Inline math is the `Latex_Fragment` inline node ([`block.cljs:2014`](../../../../../src/main/frontend/components/block.cljs)).

**Block Math:**

```markdown
$$
\frac{-b \pm \sqrt{b^2 - 4ac}}{2a}
$$
```

`Displayed_Math` renders through `latex/latex` ([`block.cljs:5149-5152`](../../../../../src/main/frontend/components/block.cljs)). When `$$…$$` is the whole block, it is normalized to `:logseq.property.node/display-type :math` with the formula as the title ([`frontend/format/block.cljs:40-43`](../../../../../src/main/frontend/format/block.cljs)) — the same node `/Math block` creates (`math-block-steps`, [`commands.cljs:145-148`](../../../../../src/main/frontend/commands.cljs)).

### Diagrams (Mermaid)

**Logseq has no built-in Mermaid renderer.** The string `mermaid` does not appear under `src/main/` or `deps/`, and CodeMirror has no mermaid mode in the imported list ([`extensions/code.cljs:7-128`](../../../../../src/main/frontend/extensions/code.cljs)), so a ` ```mermaid ` fence renders as a plain code block. The only route to a rendered diagram is a plugin: the `Src` branch hands fenced code to `plugin-handler/hook-fenced-code-by-lang` and, when a plugin has registered that language, renders the plugin's UI next to the code ([`block.cljs:5170-5173`](../../../../../src/main/frontend/components/block.cljs)).

---

## Keyboard Shortcuts

### Quick Formatting

| Format        | Shortcut (macOS) | Shortcut (Windows/Linux) |
| ------------- | ---------------- | ------------------------ |
| Bold          | `⌘+B`            | `Ctrl+B`                 |
| Italic        | `⌘+I`            | `Ctrl+I`                 |
| Highlight     | `⌘+Shift+H`      | `Ctrl+Shift+H`           |
| Strikethrough | `⌘+Shift+S`      | `Ctrl+Shift+S`           |
| Link          | `⌘+L`            | `Ctrl+L`                 |

([`shortcut/config.cljs:119-129`, `:241-242`](../../../../../src/main/frontend/modules/shortcut/config.cljs).) There is no inline-code shortcut; `⌘+E` and `⌘+K` belong to quick add and search (see [Basic Formatting](#basic-formatting)).

**See:** [Keyboard Shortcuts Reference](../interface/keyboard-shortcuts.md)

---

## Markdown vs Logseq

### Standard Markdown

**Logseq renders:**

```text
✅ Headings (# ## ###)  → stored as the heading property
✅ Bold (**text**)
✅ Italic (*text*)
✅ Links ([text](url))
✅ Code (`code`, fenced blocks)
✅ Lists (- item, 1. item) inside a block
✅ Tables (alignment markers ignored)
✅ Blockquotes (>)
```

### Logseq Extensions

**Additional syntax in a DB graph:**

```text
🆕 Page links: [[Page]]
🆕 Block refs: [[uuid]]
🆕 Tags: #tag, #[[multi word]]
🆕 Highlight: ^^text^^
🆕 Callouts: #+BEGIN_TIP … #+END_TIP
🆕 Macros: {{youtube …}}, {{video …}}, {{tweet …}}, {{cloze …}}
```

**Not in a DB graph** (file-graph syntax or deprecated): `key:: value` properties, `TODO`/`DOING` markers, `{{embed}}`, `{{query}}`, `#+BEGIN_QUERY`, `#+BEGIN_QUOTE`. See the sections above for the replacement commands.

### Compatibility

**Export as text:** the `export-blocks` dialog ([`components/export.cljs:275`](../../../../../src/main/frontend/components/export.cljs)) shows these options when the output type is text, HTML or OPML ([`components/export.cljs:382-433`](../../../../../src/main/frontend/components/export.cljs); labels at [`en.edn:648-659`](../../../../../src/resources/dicts/en.edn)):

```text
[[text]] -> text        remove page-ref brackets
remove emphasis
remove #tags
newline after block
remove properties
```

The graph-level "Export as standard Markdown (no block properties)" entry is at [`components/export.cljs:140-143`](../../../../../src/main/frontend/components/export.cljs), hidden on native mobile ([`en.edn:647`](../../../../../src/resources/dicts/en.edn)).

> Removed claim: earlier revisions said export converts `[[Page]]` to `[Page](Page.md)` and properties to YAML frontmatter. No `frontmatter`, `yaml` or `.md)` output appears in `src/main/frontend/handler/export/`.

---

## Best Practices

### Formatting Guidelines

```text
✅ Do:

- Use semantic formatting (bold for emphasis)
- Keep formatting simple
- Be consistent with style choices
- Use headings for structure
- Link related content

❌ Avoid:

- Overusing bold/italic
- Too many highlights
- Complex nested formatting
- ALL CAPS
```

### Accessibility

```text
- Use descriptive link text: [[Database Guide]] not [[Click here]]
- Use hierarchical headings
- Avoid color-only information
- Give tables a header row
```

The text in `![text](image.png)` is not emitted as `alt`: `image-link` takes it as `title` ([`block.cljs:656`](../../../../../src/main/frontend/components/block.cljs)) and passes it through `resizable-image` ([`block.cljs:679`](../../../../../src/main/frontend/components/block.cljs)) to `asset-container` ([`:424`](../../../../../src/main/frontend/components/block.cljs)), which sets it as the `<img>` `title` attribute, a hover tooltip ([`block.cljs:261-267`](../../../../../src/main/frontend/components/block.cljs)). No `:alt` is set anywhere in `block.cljs`.

> Removed claim: an earlier "Performance Tips" list (use queries for 100+ row tables, flatten 10+ levels of nesting) had no source behind its thresholds.

---

## Common Issues

**Problem: Markdown not rendering**

```text
✓ Check: Space after # for headings
✓ Check: Correct number of backticks for code
✓ Check: Balanced ** for bold
✓ Check: Not inside a code block
```

**Problem: Links not working**

```text
✓ Check: [[Page Name]] not [Page Name]
✓ Check: No "|" inside [[ ]] (not supported)
✓ Check: Block refs contain a full UUID only
```

Page names are not case-sensitive (see [Page Links](#page-links-wiki-style)).

**Problem: Code block not formatting**

```text
✓ Check: Three backticks, not one
✓ Check: Closing backticks present
✓ Check: The language name matches an entry in the language picker
```

**Problem: A block shows "… is deprecated"**

The block uses `{{embed}}`, `{{query}}`, `{{namespace}}`, `#+BEGIN_QUERY`, `#+BEGIN_QUOTE` or `#+BEGIN_EXPORT latex`. Replace it with the command the warning names.

---

## Quick Reference Card

```text
FORMATTING
**bold**        *italic*       ~~strike~~
^^highlight^^   `code`         <ins>underline</ins>

LINKS
[[Page]]        Page link
[[uuid]]        Block ref (full UUID)
[Text](url)     External link

CODE
`inline`
```language … ``` (whole block → code node)

STRUCTURE
# H1 … ###### H6   (stored as heading property)
- Bullet list  /  1. Numbered list  (inside a block)
> Quote
---              Horizontal rule

LOGSEQ (DB graph)
#tag  #[[multi word]]    Tag
/Todo  /Doing  /Done     Task status
/Add property  or  ;;    Property
/Node embed              Embed
/Query                   Query
/                        Slash commands
```

---

## Next Steps

**Master Markdown:**

1. Practice basic formatting
2. Learn Logseq extensions
3. Use keyboard shortcuts
4. Explore advanced features
5. Develop consistent style

**Related:**
- [Slash Commands](../interface/slash-commands.md)
- [Keyboard Shortcuts](../interface/keyboard-shortcuts.md)
- [Basic Editing](../../tutorials/getting-started/03-basic-editing.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
