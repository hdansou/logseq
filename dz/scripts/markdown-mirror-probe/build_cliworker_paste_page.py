#!/usr/bin/env python3
"""Build the 'Paste and Type Cheatsheet' page in the cliworker graph via the logseq CLI.

Each case has a copyable code block (what to paste) and live 'You get' blocks that reproduce what
the paste produced in paste_syntax_probe_test.cljs at be800f1711.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_cliworker_cheatsheet_page as bc
from build_cliworker_cheatsheet_page import cli, query, add, add_edn, update, task, uuid_of, label, edn_str

PAGE = "Paste and Type Cheatsheet"
bc.PAGE = PAGE
CODE_MD = '{:logseq.property.node/display-type :code :logseq.property.code/lang "markdown"}'
H = lambda n: "{:logseq.property/heading %d}" % n
NUM = '{:logseq.property/order-list-type "number"}'


def heading(title):
    return add(PAGE, title, props=H(2))


def case(section, title, paste_md, live=None, notes=()):
    c = add(section, title)
    add(c, paste_md, props=CODE_MD)
    if live:
        live(label(c, "You get"))
    for n in notes:
        add(c, n)
    return c


def table(parent, md):
    return add(parent, md)


def main():
    existing = query(f'[:find ?b . :where [?p :block/title {edn_str(PAGE)}] [?p :block/name] [?b :block/page ?p]]')
    if existing:
        sys.exit(f"Page '{PAGE}' already has blocks; clear it first.")

    ex_page = query('[:find ?u . :where [?p :block/title "Example Page"] [?p :block/name] [?p :block/uuid ?u]]')
    tag_ex = query('[:find ?u . :where [?t :block/title "Example"] [?t :block/tags :logseq.class/Tag] [?t :block/uuid ?u]]')
    tag_ex2 = query('[:find ?u . :where [?t :block/title "Example Tag"] [?t :block/tags :logseq.class/Tag] [?t :block/uuid ?u]]')
    if not (ex_page and tag_ex and tag_ex2):
        sys.exit("Example Page / Example / Example Tag are missing; run build_cliworker_cheatsheet_page.py first.")

    cli("upsert", "page", "--page", PAGE, "--update-properties",
        '{:logseq.property/description "What to paste or type so a Logseq DB graph gets real headings, blocks, code and math, and what stays plain text." :logseq.property/icon {:type :tabler-icon :id "clipboard-text"}}')

    add(PAGE, "The companion to [[Markdown Mirror Cheatsheet]], for the other direction: what to **paste** or **type** so a Logseq DB graph ends up with real structure, not just text that looks right.")
    add(PAGE, "How to use it: copy any grey code block and paste it into an empty block. The **You get** blocks next to it show what that paste creates.")
    add(PAGE, "✅ becomes the real thing (a heading, a child block, a code block, a link)\n⚠️ stays text: it may render, but it is not data (not a task, tag or property)\n❌ lost, mangled, or the paste fails")
    add(PAGE, "Unofficial community reference. Checked on 2026-09-12 by running Logseq's own paste and save code from the development branch (be800f1711).")

    # ---------- short version ----------
    s = heading("The short version")
    holder = add(s, "Six rules")
    for rule in [
        "**Structure with `- ` bullets.** Indent children with two spaces or a tab.",
        "**Headings:** start a bullet with `# ` to `###### `.",
        "**Code and math:** give a bullet nothing but a ```` ```lang ```` fence or a `$$` block.",
        "**Inline Markdown** (bold, links, images, tables, quotes, callouts) pastes as text and renders.",
        "**Tasks, priorities, dates, properties and tags are not syntax in DB graphs.** Paste without them, select the blocks, then press `Cmd/Ctrl+Enter` for Todo, `p t` for tags, `p p` for priority, `p d` for deadline.",
        "**Avoid** `#tag` (it becomes a plain page link), `[#A]` and `((uuid))`. `key:: value` lines are harmless but stay text.",
    ]:
        add(holder, rule, props=NUM)

    # ---------- template ----------
    s = heading("A template that pastes cleanly")
    add(s, "Markdown Mirror writes this back exactly as pasted: the mirror format is the best paste format.")
    template = ("- # Project kickoff\n- ## Goals\n  - Ship the **beta** by Friday\n  - Owner: [[Example Page]]\n"
                "  - Spec: [design doc](https://logseq.com)\n- ## Install\n  - Run this once:\n  - ```bash\n    npm install\n    ```\n"
                "- ## Formula\n  - $$\n    E = mc^2\n    $$\n- ## Notes\n  - > Quotes stay quotes\n"
                "  - | Step | Owner |\n    | --- | --- |\n    | Build | Ana |\n  - #+BEGIN_TIP\n    Callouts work too.\n    #+END_TIP")
    add(s, template, props=CODE_MD)
    g = label(s, "You get")
    add(g, "Project kickoff", props=H(1))
    goals = add(g, "Goals", props=H(2))
    add(goals, "Ship the **beta** by Friday")
    add(goals, f"Owner: [[{ex_page}]]")
    add(goals, "Spec: [design doc](https://logseq.com)")
    inst = add(g, "Install", props=H(2))
    add(inst, "Run this once:")
    add(inst, "npm install", props='{:logseq.property.node/display-type :code :logseq.property.code/lang "bash"}')
    form = add(g, "Formula", props=H(2))
    add(form, "E = mc^2", props="{:logseq.property.node/display-type :math}")
    notes_b = add(g, "Notes", props=H(2))
    add(notes_b, "> Quotes stay quotes")
    add(notes_b, "| Step | Owner |\n| --- | --- |\n| Build | Ana |")
    add(notes_b, "#+BEGIN_TIP\nCallouts work too.\n#+END_TIP")

    # ---------- how paste decides ----------
    s = heading("How paste decides")
    table(s, "| Clipboard holds | What happens |\n| --- | --- |\n"
             "| Blocks copied from Logseq in the same graph | Pasted as copies of those blocks |\n"
             "| A file (image, PDF…) | Uploaded as an asset block |\n"
             "| Rich text (web page, Google Docs, Word) | Converted to Markdown, then as below |\n"
             "| Text with a line starting `- ` `+ ` `* ` `#`, a fence line, or a `$$` line | **Parsed into blocks** |\n"
             "| Text with blank lines, none of the above | One block per paragraph |\n"
             "| Anything else | Inserted at the cursor as text |\n"
             "| A YouTube / Vimeo / Loom URL | `{{video url}}` |\n"
             "| An x.com or twitter.com URL | `{{twitter url}}` |\n"
             "| A URL over selected text | `[selected text](url)` |\n"
             "| Anything, inside a code or math block, or with Cmd/Ctrl+Shift+V | Raw text, no parsing |")

    # ---------- structure ----------
    s = heading("Paste Markdown: structure")
    case(s, "✅ Bullets become nested blocks", "- Parent\n  - Child\n    - Grandchild",
         lambda g: add(add(add(g, "Parent"), "Child"), "Grandchild"),
         ["Tabs work the same as two spaces."])
    case(s, "✅ `#` on a bullet makes a heading", "- # Heading bullet\n- ### Smaller heading",
         lambda g: (add(g, "Heading bullet", props=H(1)), add(g, "Smaller heading", props=H(3))))
    case(s, "⚠️ Without bullets, paragraphs join the heading above them",
         "# Title\n\nIntro paragraph.\n\n## Section\n\nText under section.",
         lambda g: (add(g, "Title\n\nIntro paragraph.", props=H(1)), add(g, "Section\n\nText under section.", props=H(2))),
         ["Put `- ` in front of each paragraph to get separate blocks."])
    case(s, "✅ Plain paragraphs become separate blocks", "First paragraph.\n\nSecond paragraph.",
         lambda g: (add(g, "First paragraph."), add(g, "Second paragraph.")))
    case(s, "⚠️ A numbered list stays inside one block", "1. one\n2. two\n3. three",
         lambda g: add(g, "1. one\n2. two\n3. three"),
         ["For numbered blocks, paste `- ` bullets, select them and press `t n`."])
    case(s, "⚠️ `*` and `+` bullets stay inside one block", "* star bullet\n+ plus bullet",
         lambda g: add(g, "* star bullet\n+ plus bullet"),
         ["Use `- ` instead."])

    # ---------- code & math ----------
    s = heading("Paste Markdown: code and math")
    case(s, "✅ A fence on its own becomes a code block", "```python\nprint('hi')\n```",
         lambda g: add(g, "print('hi')", props='{:logseq.property.node/display-type :code :logseq.property.code/lang "python"}'))
    case(s, "⚠️ A fence after text in the same bullet stays text", "- Setup:\n  ```bash\n  npm install\n  ```",
         lambda g: add(g, "Setup:\n```bash\nnpm install\n```"),
         ["It renders as code, but it is not a code block. Give the fence its own bullet."])
    case(s, "✅ `$$` on its own becomes a math block", "$$\nE = mc^2\n$$",
         lambda g: add(g, "E = mc^2", props="{:logseq.property.node/display-type :math}"))

    # ---------- inline ----------
    s = heading("Paste Markdown: text formatting")
    sample = ("**bold** *italic* ~~strike~~ ==highlight== `code` [link](https://logseq.com) "
              "x^{2} H_{2}O $E=mc^2$ <u>underline</u> \\*escaped\\*")
    case(s, "✅ Inline Markdown pastes as typed and renders", sample, lambda g: add(g, sample))

    # ---------- links & tags ----------
    s = heading("Paste Markdown: links, references and tags")
    case(s, "✅ Page links", "- See [[Example Page]]", lambda g: add(g, f"See [[{ex_page}]]"),
         ["A link to a page that doesn't exist creates the page."])
    case(s, "❌ `#tags` become page links, not tags", "- Tags #Example #[[Example Tag]]",
         lambda g: add(g, f"Tags [[{tag_ex}]] [[{tag_ex2}]]"),
         ["To tag pasted blocks, select them and press `p t`, or type `#` in a block and pick the tag."])
    ref_target = query('[:find ?u . :where [?b :block/title "Project kickoff"] [?b :block/uuid ?u]]')
    case(s, "⚠️ `((uuid))` stays literal text", f"- (({ref_target}))",
         lambda g: add(g, f"(({ref_target}))"),
         [f"Use `[[uuid]]` for block references: [[{ref_target}]]"])
    add(s, "❌ Pasting `((uuid))` for a block that is not in this graph makes the whole paste fail.")

    # ---------- tasks & properties ----------
    s = heading("Paste Markdown: tasks, priority, dates, properties")
    add(s, "None of these are syntax in DB graphs. Paste the text, then set them on the selected blocks.")
    def todo_live(g):
        add(g, "TODO buy milk")
        add(g, "DONE ship it")
    c = case(s, "⚠️ `TODO` and `DONE` stay text", "- TODO buy milk\n- DONE ship it", todo_live)
    after = label(c, "After selecting and pressing Cmd/Ctrl+Enter on a clean bullet")
    task(add(after, "buy milk"), "--status", "todo")
    case(s, "⚠️ Checkboxes stay text", "- [ ] checkbox\n- [x] checked",
         lambda g: (add(g, "[ ] checkbox"), add(g, "[x] checked")))
    add(s, "❌ `[#A]` is mangled to `[[[A]]]` and creates a page named `A]`. Use `p p` or `/Priority High` instead.")
    add(s, "⚠️ `SCHEDULED: <2026-09-14 Mon>` stays text. Use `p d`, `/Scheduled` or `/Deadline`.")
    case(s, "⚠️ Property lines stay text", "- Book\n  author:: Jane",
         lambda g: add(g, "Book\nauthor:: Jane"),
         ["Add real properties with `;;` or Cmd+P (Mac) / Ctrl+Alt+P."])
    case(s, "⚠️ Mirror property lines pasted back stay text", "- DOING Task\n  * Priority:: Urgent",
         lambda g: add(g, "DOING Task\n* Priority:: Urgent"))

    # ---------- advanced ----------
    s = heading("Paste Markdown: tables, quotes, callouts, macros")
    case(s, "✅ Tables", "- | a | b |\n  | --- | --- |\n  | 1 | 2 |", lambda g: add(g, "| a | b |\n| --- | --- |\n| 1 | 2 |"))
    case(s, "✅ Quotes render (as text, not a Quote block)", "- > quoted text", lambda g: add(g, "> quoted text"),
         ["For a Quote block, type `>` alone in an empty block."])
    case(s, "✅ Callouts", "- #+BEGIN_NOTE\n  a note\n  #+END_NOTE", lambda g: add(g, "#+BEGIN_NOTE\na note\n#+END_NOTE"))
    case(s, "⚠️ Obsidian callouts and comments stay literal", "- > [!note] callout\n- %%comment%%",
         lambda g: (add(g, "> [!note] callout"), add(g, "%%comment%%")))
    add(s, "❌ `{{embed [[Page]]}}` and `{{query …}}` show a deprecation warning. Use `/Node embed` and `/Query`.")

    # ---------- html ----------
    s = heading("Paste from web pages and documents")
    add(s, "Rich text is converted to Markdown first, then parsed as above.")
    table(s, "| In the source | Becomes | |\n| --- | --- | --- |\n"
             "| Bold, italic, strikethrough, highlight (incl. Google Docs styled spans) | `**b**` `*i*` `~~s~~` `==m==` | ✅ |\n"
             "| Links, images, inline code, horizontal rule | `[label](href)` `![alt](src)` `` `code` `` `---` | ✅ |\n"
             "| Bulleted list, nested | Nested blocks | ✅ |\n"
             "| Heading followed by paragraphs | A heading block with the paragraphs inside it | ⚠️ |\n"
             "| Numbered list | `-` bullets, numbering lost | ⚠️ |\n"
             "| Code block with a language | Fence without the language | ⚠️ |\n"
             "| Table | Pipe table; divider row always has three columns | ⚠️ |\n"
             "| Checklist checkboxes | Checkbox dropped, text kept | ❌ |\n"
             "| Underline, subscript, superscript | Plain text | ❌ |\n"
             "| Blockquote containing a paragraph | Quote marker lost | ❌ |\n"
             "| Page containing figure, aside, header, footer, center or fieldset elements | Everything falls back to plain text | ❌ |")
    add(s, "If rich paste comes out wrong, copy the source as Markdown, or paste with Cmd/Ctrl+Shift+V and fix it by hand.")

    # ---------- typing ----------
    s = heading("Typing conversions")
    table(s, "| Type | Result | |\n| --- | --- | --- |\n"
             "| `# ` … `###### ` at the start of a block | Heading 1–6 when the block saves | ✅ |\n"
             "| ```` ``` ```` in an empty block | Code block immediately | ✅ |\n"
             "| `$$`…`$$` as the whole block | Math block when it saves | ✅ |\n"
             "| `>` alone in a block | Quote block immediately | ✅ |\n"
             "| `1. ` alone in a block | Numbered list block | ✅ |\n"
             "| `[[` | Page search; the pick inserts a link | ✅ |\n"
             "| `#` | Tag search. Enter adds the tag and removes the text; Cmd/Ctrl+Enter keeps it inline | ✅ |\n"
             "| `;;` | Add property | ✅ |\n"
             "| `/` | Command menu | ✅ |\n"
             "| `TODO text`, `[#A]`, `SCHEDULED: <…>`, `key:: value` | Plain text | ⚠️ |")

    # ---------- shortcuts ----------
    s = heading("Shortcuts")
    table(s, "| While editing | Does |\n| --- | --- |\n"
             "| Cmd/Ctrl+B, Cmd/Ctrl+I | Bold, italic |\n"
             "| Cmd/Ctrl+Shift+S | Strikethrough |\n"
             "| Cmd/Ctrl+Shift+H | Highlight |\n"
             "| Cmd/Ctrl+L | Link |\n"
             "| Cmd/Ctrl+Enter | Todo → Doing → Done |\n"
             "| Tab, Shift+Tab | Indent, outdent |\n"
             "| Shift+Enter | New line in the same block |\n"
             "| Cmd/Ctrl+Shift+V | Paste as raw text |\n"
             "| Cmd+P (Mac), Ctrl+Alt+P | Add property |")
    table(s, "| With blocks selected | Does |\n| --- | --- |\n"
             "| `p t` | Set tags |\n| `p s` | Set status |\n| `p p` | Set priority |\n"
             "| `p d` | Set deadline |\n| `t n` | Numbered list |\n| Cmd/Ctrl+Enter | Cycle status on all |")

    # ---------- slash ----------
    s = heading("Slash commands for things with no syntax")
    table(s, "| Want | Type |\n| --- | --- |\n"
             "| Heading | `/Heading 1` … `/Heading 6`, `/Normal text` |\n"
             "| Task status | `/Backlog` `/Todo` `/Doing` `/In Review` `/Done` `/Canceled` |\n"
             "| Priority | `/Priority Urgent`, `High`, `Medium`, `Low`, `/No priority` |\n"
             "| Dates | `/Scheduled` `/Deadline` `/Date picker` |\n"
             "| Special blocks | `/Code block` `/Quote` `/Math block` `/Number list` `/Number children` |\n"
             "| Links and embeds | `/Node reference` `/Node embed` |\n"
             "| Queries | `/Query` `/Advanced Query` |\n"
             "| Media | `/Upload an asset` `/Embed Video URL` `/Embed Twitter tweet` `/Embed HTML` |\n"
             "| Other | `/Cloze` `/Template` `/Underline` `/Calculator` |")

    # ---------- known issues ----------
    s = heading("Known issues")
    for line in ["Pasted `#tag` becomes a page link, not a tag.",
                 "Pasted `[#A]` becomes `[[[A]]]` and creates a page named `A]`.",
                 "Pasting `((uuid))` for a block that is not in the graph fails the whole paste.",
                 "Rich paste from a page with figure, aside, header, footer, center or fieldset elements falls back to plain text. Code block languages, numbered lists, checkboxes, underline, sub/superscript and quoted paragraphs are also lost."]:
        add(s, line, props=NUM)

    print("built", PAGE)


if __name__ == "__main__":
    main()
