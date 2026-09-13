#!/usr/bin/env python3
"""Build the 'Markdown Mirror Cheatsheet' page in the cliworker graph via the logseq CLI."""
import json, re, struct, subprocess, sys, time, zlib, os

GRAPH = "cliworker"
PAGE = "Markdown Mirror Cheatsheet"
MIRROR = f"/Users/dzu/logseq/graphs/{GRAPH}/mirror/markdown/pages/{PAGE}.md"
SCRATCH = os.path.dirname(os.path.abspath(__file__))

P_TEXT = ":user.property/Text-HuKg1E6q"
P_MTEXT = ":user.property/Multi-Text-lFGgzsgx"
P_NODE = ":user.property/Node-jJzYx4cL"
P_MNODE = ":user.property/MultiNodes-Lgygabgv"
P_DATE = ":user.property/Date-N-8XQdR6"
P_DATETIME = ":user.property/DateTime-XDVoP_zM"
P_URL = ":user.property/URL-YWadL0Hu"
P_CHECK = ":user.property/Checkbox-Oenoo9SS"
P_RATING = ":user.property/Rating-IxMP7mlf"
JOURNAL_0912 = 377


def cli(*args):
    cmd = ["logseq", *args, "--graph", GRAPH, "--output", "json"]
    out = subprocess.run(cmd, capture_output=True, text=True)
    try:
        data = json.loads(out.stdout)
    except json.JSONDecodeError:
        sys.exit(f"CLI non-JSON output for {args}:\n{out.stdout}\n{out.stderr}")
    if data.get("status") != "ok":
        sys.exit(f"CLI error for {args}: {data}")
    return data.get("data", {}).get("result")


def edn_str(s):
    return json.dumps(s, ensure_ascii=False)


def query(q):
    return cli("query", "--query", q)


def add(parent, content, props=None, tags=None):
    args = ["upsert", "block", f"--content={content}"]
    args += ["--target-page", parent] if isinstance(parent, str) else ["--target-id", str(parent), "--pos", "last-child"]
    if props:
        args += ["--update-properties", props]
    if tags:
        args += ["--update-tags", tags]
    return cli(*args)[0]


def add_edn(parent, edn):
    return cli("upsert", "block", "--target-id", str(parent), "--pos", "last-child", "--blocks", edn)


def update(bid, **kw):
    args = ["upsert", "block", "--id", str(bid)]
    if "content" in kw:
        args += [f"--content={kw['content']}"]
    if kw.get("props"):
        args += ["--update-properties", kw["props"]]
    if kw.get("tags"):
        args += ["--update-tags", kw["tags"]]
    if kw.get("remove_props"):
        args += ["--remove-properties", kw["remove_props"]]
    return cli(*args)


def task(bid, *extra):
    return cli("upsert", "task", "--id", str(bid), *extra)


def uuid_of(eid):
    r = query(f"[:find ?u . :where [(ground {eid}) ?e] [?e :block/uuid ?u]]")
    return str(r)


def heading(title):
    return add(PAGE, title, props="{:logseq.property/heading 2}")


def label(parent, text):
    return add(parent, f"**{text}**")


def notes(parent, lines):
    lab = label(parent, "Round trip")
    for line in lines:
        add(lab, line)


def png_bytes(size=16):
    rows = b""
    for y in range(size):
        row = b"\x00"
        for x in range(size):
            on = (x + y) % 4 < 2
            row += bytes((29, 107, 116) if on else (217, 236, 238))
        rows += row
    def chunk(t, d):
        return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xFFFFFFFF)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b""))


def main():
    existing = query(f'[:find ?b . :where [?p :block/title {edn_str(PAGE)}] [?p :block/name] [?b :block/page ?p]]')
    if existing:
        sys.exit(f"Page '{PAGE}' already exists (id {existing}); remove it first.")

    # ---------- supporting entities ----------
    for tag in ["Example", "Example Tag"]:
        cli("upsert", "tag", "--name", tag)
    ex_page = cli("upsert", "page", "--page", "Example Page")[0]
    if not query('[:find ?b . :where [?p :block/title "Example Page"] [?b :block/page ?p]]'):
        add("Example Page", "Content of Example Page")
    ex_page_uuid = uuid_of(ex_page)
    tag_ex = query('[:find ?u . :where [?t :block/title "Example"] [?t :block/tags :logseq.class/Tag] [?t :block/uuid ?u]]')
    tag_ex2 = query('[:find ?u . :where [?t :block/title "Example Tag"] [?t :block/tags :logseq.class/Tag] [?t :block/uuid ?u]]')

    # ---------- page ----------
    cli("upsert", "page", "--page", PAGE,
        "--update-properties", '{:logseq.property/description "What each thing in a Logseq DB graph becomes in a Markdown Mirror file, and whether it survives a round trip." :logseq.property/icon {:type :tabler-icon :id "markdown"}}',
        "--update-tags", '["Example"]')

    add(PAGE, "Logseq DB graphs can write every page to a Markdown file: **Settings → Features → Markdown Mirror** (desktop app only). Files go to `~/logseq/graphs/<graph>/mirror/markdown/pages/<title>.md` and `journals/yyyy_MM_dd.md`. The mirror is one-way today: editing the files does not change the graph.")
    add(PAGE, "Each section below has three parts: **In Logseq** (live examples), **Mirror writes** (the exact Markdown the mirror produced for those examples, copied from this page's own mirror file) and **Round trip** (what survives).")
    add(PAGE, "✅ faithful — reads back as the same thing\n⚠️ lossy or ambiguous — written, but information is lost or the text could mean something else\n❌ not written")
    add(PAGE, "Unofficial community reference. Checked on 2026-09-12 against Logseq desktop 2.0.1 and the Logseq development branch. Mirror output in the code blocks is indented to the top level; everything else is verbatim.")

    groups = {}  # section key -> "Mirror writes" label id
    mirror_labels = []

    # ---------- Page file ----------
    s = heading("The page file")
    add(s, "Every mirror file starts with `id:: <page uuid>`, then page properties as `* name:: value`, a blank line, then the blocks. This page has a description, an icon and the tag #[[%s]]." % tag_ex, tags='["Example"]')
    mirror_labels.append(("page", label(s, "Mirror writes")))
    notes(s, [
        "✅ `id::` line identifies the page; it is not a property",
        "✅ page properties appear before the first block",
        "⚠️ the icon is written as a raw EDN map",
        "❌ page tags and aliases are not written: this page's #Example tag is not in its file",
        "⚠️ the page title only survives as the file name: `/ \\ : * ? \" < > |` become `_`, duplicates get ` (2)`",
    ])

    # ---------- Blocks ----------
    s = heading("Blocks and nesting")
    g = label(s, "In Logseq"); key = "blocks"
    a_block = add(g, "A block")
    parent = add(g, "Parent")
    child = add(parent, "Child")
    add(child, "Grandchild")
    add(g, "A block with two lines\nsecond line of the same block")
    coll = add(g, "Collapsed block")
    add(coll, "Child of a collapsed block")
    update(coll, props="{:block/collapsed? true}")
    add(g, "First line\n- a later line that starts with a dash")
    add_edn(g, '[{:block/title ""}]')
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ `- ` per block, two spaces per nesting level",
        "✅ later lines of a block are indented under its first line",
        "✅ an empty block is `-`",
        "⚠️ collapsed state is lost; children are always written",
        "⚠️ a later line starting with `- ` looks exactly like a child block",
        "⚠️ leading and trailing spaces in a block are trimmed",
    ])

    # ---------- Numbered ----------
    s = heading("Numbered lists")
    g = label(s, "In Logseq"); key = "numbered"
    holder = add(g, "Steps")
    num = "{:logseq.property/order-list-type \"number\"}"
    first = add(holder, "First step", props=num)
    add(first, "Sub-step a", props=num)
    add(first, "Sub-step b", props=num)
    add(holder, "Second step", props=num)
    plain = add(holder, "A plain bullet in between")
    update(plain, remove_props="[:logseq.property/order-list-type]")
    add(holder, "Third step", props=num)
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ numbered blocks become `1.`, `2.`",
        "⚠️ numbering continues past a plain bullet (`3.`, not `1.`)",
        "⚠️ children of a numbered item are indented two spaces; CommonMark needs three, so other Markdown tools may not nest them",
    ])

    # ---------- Headings ----------
    s = heading("Headings")
    add(s, "Typing `## ` at the start of a block, `/Heading 2`, or the bullet menu stores a heading property and removes the `#`s from the text. The mirror puts them back.")
    g = label(s, "In Logseq"); key = "headings"
    add(g, "Heading 1", props="{:logseq.property/heading 1}")
    add(g, "Heading 3", props="{:logseq.property/heading 3}")
    auto = add(g, "Auto heading", props="{:logseq.property/heading true}")
    add(auto, "Auto heading child", props="{:logseq.property/heading true}")
    ht = add(g, "Heading on a task", props="{:logseq.property/heading 3}")
    task(ht, "--status", "todo")
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ fixed levels 1–6 are written as `#` … `######`",
        "⚠️ auto heading is written as a fixed level (nesting depth + 1), so it reads back as that level; these examples sit three levels deep, hence `####`",
        "⚠️ on a task the status word comes first (`TODO ## …`), so the line is no longer a Markdown heading",
        "⚠️ text that literally starts with `# ` (for example written by the CLI or API) is written the same as a real heading",
    ])

    # ---------- Text ----------
    s = heading("Text formatting")
    g = label(s, "In Logseq"); key = "text"
    add(g, "**bold** *italic* ~~strikethrough~~ ==highlight== ^^highlight^^ `inline code`")
    add(g, "Inline math $E=mc^2$, superscript x^{2}, subscript H_{2}O")
    add(g, "[External link](https://logseq.com), autolink <https://logseq.com>, bare https://logseq.com")
    add(g, "Underline needs HTML: <u>underlined</u>. Escapes: \\*not italic\\*")
    add(g, "A block with a yellow background colour", props='{:logseq.property/background-color "yellow"}')
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ inline Markdown is stored as typed and written unchanged",
        "❌ block background colour is a hidden property and is not written; only inline `==…==` / `^^…^^` highlights survive",
    ])

    # ---------- Links ----------
    a_uuid = uuid_of(a_block)
    s = heading("Links, references and tags")
    add(s, "In the database a page link is stored as `[[<uuid>]]`. The mirror turns page IDs back into titles but leaves block IDs as they are.")
    g = label(s, "In Logseq"); key = "links"
    add(g, f"Page link [[{ex_page_uuid}]]")
    add(g, f"Labelled link [see the example]([[{ex_page_uuid}]])")
    add(g, f"Block reference [[{a_uuid}]]")
    add(g, f"Inline tags #[[{tag_ex}]] and #[[{tag_ex2}]]", tags='["Example" "Example Tag"]')
    add(g, "Tags added with the tag picker", tags='["Example" "Example Tag"]')
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ page links are written with the page title",
        "⚠️ block references stay `[[uuid]]`, and no mirror file contains block IDs, so the target cannot be found from the files alone",
        "⚠️ tags from the tag picker are appended to the end of the first line, so they look the same as inline tags",
        "⚠️ if two pages share a title, `[[Title]]` no longer says which one",
        "✅ renaming a page rewrites every mirror file that links to it",
        "❌ built-in tags (Task, Code, Quote, Math, Query, Asset, Card, Template) are not written",
    ])

    # ---------- Embeds ----------
    s = heading("Embeds")
    add(s, "`/Node embed` inserts no text. It creates an empty block that points at the embedded block or page.")
    g = label(s, "In Logseq"); key = "embeds"
    add(g, "Next: an embed of the Parent block from Blocks and nesting, then an embed of Example Page")
    add_edn(g, f'[{{:block/title "" :block/link [:block/uuid #uuid "{uuid_of(parent)}"]}} {{:block/title "" :block/link [:block/uuid #uuid "{ex_page_uuid}"]}}]')
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "⚠️ a block embed is written as a copy of the block and its children; it reads back as duplicate content",
        "❌ a page embed is written as an empty `-`",
    ])

    # ---------- Tasks ----------
    s = heading("Tasks")
    add(s, "Status is a property. Typing `TODO ` does not create a task in a DB graph; use `/Todo`, `/Doing` … or Cmd/Ctrl+Enter.")
    g = label(s, "In Logseq"); key = "tasks"
    for status, text in [("backlog", "Backlog task"), ("todo", "Todo task"), ("doing", "Doing task"),
                         ("in-review", "In review task"), ("done", "Done task"), ("canceled", "Canceled task")]:
        task(add(g, text), "--status", status)
    t = add(g, "Task with priority, scheduled and deadline")
    task(t, "--status", "doing", "--priority", "urgent",
         "--scheduled", "2026-09-14T14:00:00.000Z", "--deadline", "2026-09-15T18:30:00.000Z")
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ status is an upper-case word at the start of the line; spaces become hyphens (`IN-REVIEW`)",
        "✅ priority is written as a property line",
        "⚠️ scheduled and deadline are written in the computer's local time with no time zone (these were set to 14:00 and 18:30 UTC)",
        "⚠️ text that literally starts with `TODO ` is written the same as a real task",
        "⚠️ \"No priority\" is written as an empty `* Priority::` value",
    ])

    # ---------- Properties ----------
    s = heading("Properties")
    add(s, "Properties are written as `*` list items with `name::`, so a property (`*`) is never confused with a block (`-`).")
    g = label(s, "In Logseq"); key = "props"
    props = ("{" + f'{P_TEXT} "A text value" '
             + f'{P_MTEXT} #{{"first value" "second value"}} '
             + f"{P_RATING} 4.5 {P_CHECK} true "
             + f'{P_URL} "https://logseq.com" '
             + f'{P_NODE} [:block/uuid #uuid "{ex_page_uuid}"] '
             + f"{P_DATE} {JOURNAL_0912} {P_DATETIME} 1789221600000"
             + "}")
    add(g, "A block with one property of each type", props=props)
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ text values are child bullets under `* name::`; each value of a multi-value property is its own bullet",
        "✅ numbers, checkboxes, URLs and fixed choices are written inline",
        "⚠️ node values are `[[Title]]` joined with `, `, which cannot be split back if a title contains a comma",
        "⚠️ dates are the journal title without brackets, so not a link",
        "⚠️ date-times are local time with no time zone (this one is 14:00 UTC)",
        "⚠️ properties are written in Logseq's internal order, not the order you added them",
        "❌ hidden properties, created and updated times are not written",
    ])

    # ---------- Special blocks ----------
    s = heading("Code, quotes, queries and assets")
    g = label(s, "In Logseq"); key = "special"
    add(g, "(defn greet [name]\n  (str \"Hello, \" name))",
        props='{:logseq.property.node/display-type :code :logseq.property.code/lang "clojure"}')
    add(g, "Code that contains ``` a fence",
        props='{:logseq.property.node/display-type :code :logseq.property.code/lang "markdown"}')
    add(g, "console.log(\"tagged\")",
        props='{:logseq.property.node/display-type :code :logseq.property.code/lang "js"}', tags='["Example"]')
    add(g, "A quote block\nwith a second line", props="{:logseq.property.node/display-type :quote}")
    q = add(g, "QUERY PLACEHOLDER")
    update(q, tags="[:logseq.class/Query]", props='{:logseq.property/query "(task todo doing)"}')
    update(q, content="")
    png = os.path.join(SCRATCH, "mirror-cheatsheet-asset.png")
    with open(png, "wb") as f:
        f.write(png_bytes())
    cli("upsert", "asset", "--path", png, "--target-id", str(g), "--pos", "last-child")
    add(g, "What does the mirror write for {{cloze a flashcard}}?", tags="[:logseq.class/Card]")
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ code blocks become fenced code with their language; the fence grows when the code contains backticks",
        "⚠️ a tag on a code block is written into the opening fence (```` ```js #Example ````), where Markdown tools read it as part of the language",
        "⚠️ a quote block is written as `> ` lines, the same as a text block that starts with `> `",
        "❌ a query block is written as an empty `-`; the query is lost",
        "❌ an asset is written as its title only; nothing points at the file in the graph's `assets/` folder",
        "⚠️ the flashcard text survives but the Card tag does not",
    ])

    # ---------- Advanced ----------
    s = heading("Tables, callouts, footnotes and HTML")
    add(s, "These live in a block's text, so they are written unchanged.")
    g = label(s, "In Logseq"); key = "advanced"
    add(g, "| Mirror mark | Meaning |\n| --- | --- |\n| ✅ | faithful |\n| ⚠️ | lossy or ambiguous |\n| ❌ | not written |")
    add(g, "#+BEGIN_TIP\nCallouts use #+BEGIN_NOTE, TIP, IMPORTANT, CAUTION, WARNING or PINNED.\n#+END_TIP")
    add(g, "A footnote[^1]\n\n[^1]: Footnote text lives in the same block.")
    add(g, "---")
    add(g, "Inline <kbd>HTML</kbd> and hiccup [:span {:style {:color \"teal\"}} \"hiccup\"]")
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "✅ all of this is plain block text and round-trips",
        "Not rendered in DB graphs (but still written as text): Obsidian `> [!note]` callouts, `%%comments%%`, `- [ ]` task lines inside a block, Mermaid diagrams without a plugin",
    ])

    # ---------- File-graph habits ----------
    s = heading("File-graph syntax that stays plain text")
    add(s, "| Typed | In a file graph | In a DB graph use |\n| --- | --- | --- |\n| `TODO` `DOING` `LATER` `NOW` | task marker | `/Todo`, `/Doing`, Cmd/Ctrl+Enter |\n| `[#A]` | priority | `/Priority Urgent` … |\n| `SCHEDULED: <2026-09-12>` | scheduled date | `/Scheduled` |\n| `key:: value` | property | `/Add property` or Cmd/Ctrl+P |\n| `((uuid))` | block reference | Copy block ref → `[[uuid]]` |\n| `{{embed [[Page]]}}` | embed | `/Node embed` |\n| `{{query …}}`, `#+BEGIN_QUERY` | query | `/Query`, `/Advanced Query` |")

    # ---------- Two-way ----------
    s = heading("Two-way readiness")
    add(s, "To read these files back into a graph, an importer must cope with text that can mean two things:")
    amb = add(s, "Ambiguous lines")
    for line in ["`- TODO text` — a task, or text starting with TODO",
                 "`- ## text` — a heading, an auto heading, or text starting with ##",
                 "`- > text` — a quote block, or text starting with >",
                 "`- text #Tag` — an inline tag, or a tag from the tag picker",
                 "`  * note:: value` — a property, or a second line of text typed that way",
                 "`  - line` — a child block, or a later line of the same block",
                 "a block followed by its children — the block, or a block embed of it"]:
        add(amb, line)
    lost = add(s, "Not in the files")
    for line in ["page tags and aliases", "block IDs, so block and asset references cannot be resolved",
                 "page embeds, queries, asset files, background colour, collapsed state",
                 "hidden properties, created and updated times", "time zones of dates with times"]:
        add(lost, line)
    add(s, "An unmerged Logseq branch (`feat/two-way-sync-between-db-and-markdown`, design status: proposed) would import block text, nesting and the task status word, and ignore every other property edit. Until something ships, the safest edits in an external editor are block text, nesting and status words.")

    # ---------- Known issue (math last: it disturbs everything after it in the mirror) ----------
    s = heading("Known issue: math blocks")
    add(s, "After a math block, the mirror stops writing task status words, tags and list numbers for every later block on the same page. A block whose text starts with a blank line does the same. That is why this section is last.")
    g = label(s, "In Logseq"); key = "math"
    task(add(g, "Doing task before the math block"), "--status", "doing")
    add(g, "E = mc^2", props="{:logseq.property.node/display-type :math}")
    task(add(g, "Done task after the math block"), "--status", "done")
    add(g, "Tagged block after the math block", tags='["Example"]')
    mirror_labels.append((key, label(s, "Mirror writes")))
    notes(s, [
        "⚠️ the math block itself is written correctly as `$$` lines",
        "❌ the task after it is missing `DONE` and the block after it is missing `#Example`",
    ])

    json.dump(mirror_labels, open(os.path.join(SCRATCH, "cheatsheet_groups.json"), "w"))
    print("built; mirror labels:", mirror_labels)
    time.sleep(6)
    fill_mirror_blocks(mirror_labels)


def fill_mirror_blocks(mirror_labels):
    lines = open(MIRROR, encoding="utf-8").read().split("\n")
    header = []
    for line in lines:
        if line == "":
            break
        header.append(line)
    in_idx = [i for i, l in enumerate(lines) if re.match(r"^\s*- \*\*In Logseq\*\*$", l)]
    extracts = []
    for i in in_idx:
        indent = len(lines[i]) - len(lines[i].lstrip(" "))
        out = []
        for l in lines[i + 1:]:
            if l.strip() and (len(l) - len(l.lstrip(" "))) <= indent:
                break
            out.append(l[indent + 2:] if l.startswith(" " * (indent + 2)) else l.lstrip(" "))
        while out and not out[-1].strip():
            out.pop()
        extracts.append("\n".join(out))
    keys = [k for k, _ in mirror_labels if k != "page"]
    if len(extracts) != len(keys):
        sys.exit(f"found {len(extracts)} In Logseq groups in mirror, expected {len(keys)}")
    text_by_key = dict(zip(keys, extracts))
    text_by_key["page"] = "\n".join(header)
    code = '{:logseq.property.node/display-type :code :logseq.property.code/lang "markdown"}'
    for key, lab in mirror_labels:
        add(lab, text_by_key[key], props=code)
    json.dump(text_by_key, open(os.path.join(SCRATCH, "cheatsheet_extracts.json"), "w"), indent=1, ensure_ascii=False)
    print("filled", len(mirror_labels), "mirror blocks")


if __name__ == "__main__":
    main()
