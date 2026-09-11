---
title: 'Slash Commands Reference'
status: 🟢 Stable
version: 'DB graph schema 65.33'
quadrant: 'Reference'
platform: 'Desktop + Web + Mobile'
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
related-docs:
  - '[Keyboard Shortcuts](../configuration/keyboard-shortcuts.md)'
  - '[Templates System](./templates-system.md)'
  - '[Basic Note-Taking](../../tutorials/getting-started/03-basic-editing.md)'
tags: ['commands', 'slash', 'reference']
---

# Slash Commands Reference

Complete reference of the slash (`/`) menu in Logseq DB graphs, enumerated from `commands-map` in [`src/main/frontend/commands.cljs`](../../../../../src/main/frontend/commands.cljs) at `be800f1711`. Every built-in entry is listed below with the exact English name, the menu group it appears under, and what it does.

> **Scope:** file graphs cannot be opened any more (the graph list keeps only DB graphs — `src/main/frontend/components/repo.cljs:321-323`), and the slash menu no longer has file-graph variants. The old `TODO`/`DOING`/`NOW`/`LATER` markers, `[#A]` priorities, `DEADLINE: <date>` text, `#+BEGIN_TIP` callouts, `key:: value` properties, `((uuid))` block refs, `{{embed}}`, and "Draw" whiteboard commands are gone — see [Removed commands](#removed-commands).

---

## Table of Contents

1. [How the menu works](#how-the-menu-works)
2. [Built-in commands](#built-in-commands)
3. [Commands registered by extensions](#commands-registered-by-extensions)
4. [User-defined commands (`:commands`)](#user-defined-commands-commands)
5. [Plugin slash commands](#plugin-slash-commands)
6. [Related editor triggers](#related-editor-triggers)
7. [Mobile](#mobile)
8. [Removed commands](#removed-commands)

---

## How the menu works

| Behaviour | Detail | Source |
| --- | --- | --- |
| **Opening** | Typing `/` opens the menu when the `/` is at the start of a line or starts a new word. It does not open in a comment editor. | `src/main/frontend/handler/editor.cljs:2270-2276` |
| **Filtering** | Text typed after `/` is fuzzy-matched against command names; at most 50 matches are shown. | `src/main/frontend/commands.cljs:556-574` |
| **Other UI languages** | Names are translated through the i18n dictionary. In a non-English UI the English name also matches; in `zh-CN` pinyin initials match too. | `src/main/frontend/commands.cljs:378-392`, `:562-569` |
| **Group headings** | Group headings (BASIC, FORMAT, …) are shown only while the list is unfiltered. | `src/main/frontend/components/editor.cljs:144-147` |
| **Closing** | The menu closes when you type `/` followed by whitespace, or when the query grows more than 2 characters past the last text that matched anything. Backspacing over the `/` also closes it. | `src/main/frontend/handler/editor.cljs:3573-3583`, `:3228-3233` |
| **Descriptions** | A command's description is shown as a hover title/tooltip unless `:ui/show-command-doc?` is `false` in config.edn (default `true`). | `src/main/frontend/state.cljs:720-722`, `src/main/frontend/components/editor.cljs:105` |
| **Editing a page title** | When the block being edited is a page, only the TASK STATUS, TASK DATE and PRIORITY entries and **Add property** are offered. | `src/main/frontend/components/editor.cljs:42-53` |
| **Duplicate names** | If two entries have the same name, the later one wins (built-ins, then extension, config and plugin commands, in that order). | `src/main/frontend/commands.cljs:376`, `deps/common/src/logseq/common/util.cljs:165-168` |
| **Navigation keys** | `up`/`ctrl+p`, `down`/`ctrl+n`, `enter` — the `:auto-complete/*` shortcuts. | `src/main/frontend/modules/shortcut/config.cljs:66-80`; see [Keyboard Shortcuts](../configuration/keyboard-shortcuts.md) |

There are no short aliases (`/tmpl`, `/dl`, `/pa`…) and no "recent commands first" ordering: the unfiltered list is always in the fixed order below.

---

## Built-in commands

Names are the English values from `src/resources/dicts/en.edn`; the key is given so the entry can be found in other languages. Groups appear in this order. Within a group, only the first entry carries the group heading; entries without their own heading appear under the preceding one.

### BASIC

`commands.cljs:227-237`

| Command | i18n key | What it does |
| --- | --- | --- |
| **Node reference** | `:editor.slash/node-reference` | Inserts `[[]]` and opens node search (pages and blocks). A chosen block is referenced as `[[<uuid>]]` — `src/main/frontend/util/ref.cljs:5-7`. |
| **Node embed** | `:editor.slash/node-embed` | Opens node search in embed mode (`commands.cljs:105-108`, `:687-692`). The chosen node is embedded by inserting a new sibling block whose `:block/link` points to it — no `{{embed}}` macro (`src/main/frontend/components/editor.cljs:289-306`). |

### FORMAT

`commands.cljs:239-257`

| Command | i18n key | What it does |
| --- | --- | --- |
| **Link** | `:ui/link` | Shows *Link* and *Label* inputs, then inserts `[label](link)`. Nothing is inserted if either is blank. `src/main/frontend/handler/editor.cljs:2154-2159`, `:2179-2186` |
| **Image link** | `:editor.slash/image-link` | Shows *Link* and *Label* inputs, then inserts `![label](link)`. Only the link is required. `editor.cljs:2161-2166`, `:2188-2195` |
| **Underline** | `:editor.slash/underline` | Inserts `<ins></ins>` with the cursor between the tags. It is gated on `(state/markdown?)`, which is always true because the preferred format is hard-coded to `:markdown` (`src/main/frontend/state.cljs:555-561`). |
| **Code block** | `:editor.slash/code-block` | Turns the current block into a code block (`:logseq.property.node/display-type :code`) and exits editing. The most recently used code language is applied. `commands.cljs:134-138`, `src/main/frontend/handler/events.cljs:388-402` |
| **Quote** | `:class.built-in/quote-block` | Sets `:logseq.property.node/display-type :quote` on the block. `commands.cljs:140-143` |
| **Math block** | `:editor.slash/math-block` | Sets `:logseq.property.node/display-type :math` (a LaTeX block). `commands.cljs:145-148` |

### Heading

`commands.cljs:198-213`, applied by `:editor/set-heading` at `commands.cljs:675-680`

| Command | What it does |
| --- | --- |
| **Normal text** | Removes the `:logseq.property/heading` property. |
| **Heading 1** … **Heading 6** | Sets `:logseq.property/heading` to `1`–`6`. Label key: `:editor.slash/heading-label` = `"Heading {1}"`. |

### TASK STATUS

`commands.cljs:150-165`, applied at `commands.cljs:627-633`

There is one entry per **built-in** Status choice. The labels come from `:property.status/*` (`en.edn:1571-1576`), and each choice is a closed value defined at `deps/db/src/logseq/db/frontend/property.cljs:313-318`.

| Command | Sets `:logseq.property/status` to |
| --- | --- |
| **Backlog** | `:logseq.property/status.backlog` |
| **Todo** | `:logseq.property/status.todo` |
| **Doing** | `:logseq.property/status.doing` |
| **In Review** | `:logseq.property/status.in-review` |
| **Done** | `:logseq.property/status.done` |
| **Canceled** | `:logseq.property/status.canceled` |

- Setting a status also tags the block `#Task`, unless one of the block's tags already provides the Status property (`deps/outliner/src/logseq/outliner/property.cljs:108-116`, `:206`).
- The list is read from the built-in schema, not the database (`src/main/frontend/handler/db_based/property/util.cljs:39-41` → `property.cljs:817-820`). A Status choice you add yourself is therefore **not** offered here.

### TASK DATE

`commands.cljs:265-277`

| Command | What it does |
| --- | --- |
| **Deadline** | Removes the `/` and opens the property value editor for `:logseq.property/deadline`. `commands.cljs:667-668` |
| **Scheduled** | Removes the `/` and opens the property value editor for `:logseq.property/scheduled`. `commands.cljs:664-665` |

These use the same `#Task` rule as statuses (`outliner/property.cljs:108-116`).

### PRIORITY

`commands.cljs:167-190`, applied at `commands.cljs:654-662`

| Command | Sets `:logseq.property/priority` to |
| --- | --- |
| **No priority** | the empty placeholder, which clears it |
| **Priority Low** | `:logseq.property/priority.low` |
| **Priority Medium** | `:logseq.property/priority.medium` |
| **Priority High** | `:logseq.property/priority.high` |
| **Priority Urgent** | `:logseq.property/priority.urgent` |

The values are defined at `property.cljs:334-337`. Labels use `"Priority {1}"` (`en.edn:558`) with `:property.priority/*` (`en.edn:1547-1550`).

### TIME & DATE

`commands.cljs:282-304`

| Command | Inserts |
| --- | --- |
| **Tomorrow** | A `[[…]]` reference to tomorrow's journal page, using the journal title. |
| **Yesterday** | A `[[…]]` reference to yesterday's journal page. |
| **Today** | A `[[…]]` reference to today's journal page. |
| **Current time** | The current time, formatted for the UI locale (`src/main/frontend/date.cljs:81-83`). |
| **Date picker** | Opens the editor's date picker popup (`commands.cljs:724-733`, `components/editor.cljs:637-638`). |

### LIST TYPE

`commands.cljs:306-317`

| Command | What it does |
| --- | --- |
| **Number list** | Toggles numbered-list display on the current block (`events.cljs:319-330`). |
| **Number children** | Toggles numbered-list display on the block's immediate children (`events.cljs:364-368`). |

Typing `1. ` as the entire content of a block that isn't already numbered does the same as **Number list** (`editor.cljs:2265-2267`).

### ADVANCED

`commands.cljs:319-358`

| Command | i18n key | What it does |
| --- | --- | --- |
| **Add comment** | `:block.comments/add-comment` | Adds a comment to the current block (`events.cljs:342-343`). The same action has a shortcut, `ctrl+space` (`shortcut/config.cljs:294-296`). |
| **Query** | `:property.built-in/query` | Tags the block `#Query`, moves the block's text into its query, and clears the block title (`editor.cljs:4377-4394`). Type the query bare **first** — e.g. `(task Todo)` — then run `/Query`; don't wrap it in `{{query …}}`. |
| **Advanced Query** | `:editor.slash/advanced-query` | Tags the block `#Query` and sets an empty query shown as a code block with language `clojure`, then exits editing (`commands.cljs:125-132`). |
| **Query function** | `:editor.slash/query-function` | Inserts `{{function }}`. It renders using the enclosing query's result (`src/main/frontend/components/block.cljs:1642-1649`, `:1873-1874`). |
| **Calculator** | `:editor.slash/calculator` | Turns the block into a code block with language `calc` and focuses it (`commands.cljs:119-123`). |
| **Upload an asset** | `:editor.slash/upload-asset` | Opens the file picker; the chosen files are uploaded as assets (`commands.cljs:735-737`, `components/editor.cljs:495-503`). |
| **Template** | `:class.built-in/template` | Opens template search and inserts the chosen template (`commands.cljs:693-694`). See [Templates System](./templates-system.md). |
| **Embed HTML** | `:editor.slash/embed-html` | Inserts `@@html: @@`. The HTML is rendered sanitized (`block.cljs:2022-2025`). |
| **Embed Video URL** | `:editor.slash/embed-video-url` | Inserts `{{video }}` (`block.cljs:1903-1904`). |
| **Embed YouTube timestamp** | `:editor.slash/embed-youtube-timestamp` | Inserts `{{youtube-timestamp N}}`, where N is the current second of a YouTube player in the block. If the player isn't ready it shows a warning instead (`src/main/frontend/extensions/video/youtube.cljs:163-176`). |
| **Embed Twitter tweet** | `:editor.slash/embed-twitter-tweet` | Inserts `{{tweet }}`; `{{twitter …}}` also renders (`block.cljs:1906`). |
| **Add property** | `:command.editor/add-property` | Opens the property picker for the block (`commands.cljs:744-745`, `src/main/frontend/handler/events/ui.cljs:312-324`). |

The video macros that render are exactly `video`, `youtube`, `vimeo` and `bilibili` (`src/main/frontend/components/block/video.cljs:6`). Slash commands exist only for `{{video}}` and `{{youtube-timestamp}}`.

**Total built-in entries:** 47 (2 + 6 + 7 + 6 + 2 + 5 + 5 + 2 + 12, in table order above), plus **Cloze** (below).

---

## Commands registered by extensions

Internal code can append commands through `commands/register-slash-command` (`commands.cljs:71-74`, spliced in at `:360-366`). At `be800f1711` there is exactly one caller:

| Command | i18n key | What it does | Source |
| --- | --- | --- | --- |
| **Cloze** | `:editor.slash/cloze` | Inserts `{{cloze }}`, used by the flashcard (FSRS) feature. It has no heading of its own, so it appears after the ADVANCED entries. | `src/main/frontend/extensions/fsrs.cljs:25-30` |

---

## User-defined commands (`:commands`)

config.edn accepts a `:commands` vector (`deps/common/resources/templates/config.edn:147-154`). Its schema is `[:vector [:tuple :string [:or :string [:vector :some]]]]` (`src/main/frontend/schema/handler/common_config.cljc:43-45`).

```clojure
:commands
[["js" "Javascript"]
 ["md" "Markdown"]]
```

- Each entry is `[name value]`. A string value replaces the typed `/name`; a vector value is run as command steps (`src/main/frontend/handler/common/editor.cljs:15-24`).
- User commands come after the built-in and extension commands (`commands.cljs:369`). Under the last-wins rule, an entry named like a built-in (for example `"Today"`) replaces it.

---

## Plugin slash commands

Plugins register commands with `logseq.Editor.registerSlashCommand(tag, action)`:

- **Declaration:** `libs/src/LSPlugin.ts:666-669`.
- **Implementation:** `libs/src/LSPlugin.user.ts:583-594`, which calls `registerSlashCommand` at `:177-233`.
- **Host export:** `register_plugin_slash_command` at `src/main/logseq/api.cljs:53` and `src/main/logseq/api/plugin.cljs:297-302`.

```typescript
logseq.Editor.registerSlashCommand('Insert greeting', async () => {
  await logseq.Editor.insertAtEditingCursor('Hello')
})
```

(`insertAtEditingCursor` is exported by the host: `src/main/logseq/api.cljs:126`.)

- **Callback form:** a function action is wrapped as `editor/clear-current-slash` → `editor/restore-saved-cursor` → `editor/hook` (`LSPlugin.user.ts:186-192`).
- **Step-array form:** the step names in the type are `editor/input`, `editor/hook`, `editor/clear-current-slash` and `editor/restore-saved-cursor` (`LSPlugin.ts:244-249`). Any other step name is only logged: `"No handler for step: "` (`commands.cljs:750-751`).
- **Return value:** the call returns an unregister function (`LSPlugin.user.ts:225-232`).
- **Registration check:** the host accepts the command only if the plugin id is installed, then rebuilds the menu (`src/main/frontend/handler/plugin.cljs:407-414`).
- **Placement:** plugin commands are listed last, under **PLUGINS**, with a puzzle icon; hovering one shows the plugin id (`commands.cljs:370-374`, `components/editor.cljs:104`, `:114-118`). A plugin command with the same name as a built-in replaces it (last-wins).

---

## Related editor triggers

| Type | Result | Source |
| --- | --- | --- |
| `[[` | Opens node search | `editor.cljs:1943-1947` |
| `#` at line start, after whitespace, or right after `]]` | Opens tag search | `editor.cljs:2283-2290` |
| `((` | Shows the warning *"To reference a node, please use `[[]]`."* — `((uuid))` is **not** a reference in DB graphs and renders as literal text | `editor.cljs:1949-1952`, `en.edn:503`, `block.cljs:1556-1557` |
| `{` | Auto-pairs to `{}`; there is no macro suggestion menu | `editor.cljs:1902-1904` |
| `1. ` | Toggles a numbered list | `editor.cljs:2265-2267` |

---

## Mobile

The slash menu is built by the same `commands-map` on every platform; nothing in it is gated by platform.

| Aspect | Desktop (Electron) / Web | Native iOS/Android app | Source |
| --- | --- | --- | --- |
| **Built-in commands** | All of the above | All of the above | `commands.cljs:220-376` (no platform checks) |
| **Opening the menu** | Type `/` | Type `/`, or tap **Slash** in the native editor toolbar, which inserts `/` | `src/main/mobile/components/editor_toolbar.cljs:125-131`, `:180-191`, `:209-210` |
| **Plugin commands** | Yes, when the plugin system is enabled | **No.** The plugin system is off: `plugin-platform?` is true only on Electron or on non-native, non-publishing web | `src/main/frontend/util.cljc:127-131`, `src/main/frontend/config.cljs:149-152` |
| **Upload an asset** | File picker | File picker; the toolbar also has separate *Photo* (camera), *Upload an asset* and *Audio* buttons | `editor_toolbar.cljs:133-139`, `:180-191` |

There are no mobile-only slash commands such as `/camera` or `/photo`.

---

## Removed commands

The commands this page used to document are not in `commands-map` at `be800f1711`. The commits that removed them:

| Old command(s) | Now | Evidence |
| --- | --- | --- |
| `TODO` / `DOING` / `DONE` / `LATER` / `NOW` / `WAITING` / `CANCELLED` markers | TASK STATUS entries set the Status property (6 values) | `036f1b5ab7` (2024-01-19) "feat: /todo/other-status and /urgent/other-priority to set property" removed `(->marker "TODO")`; file-graph variants removed in `bcc478b5f7` (2025-12-29) "refactor: separate og and db version" |
| `[#A]` / `[#B]` / `[#C]` priorities | PRIORITY entries (Low/Medium/High/Urgent) | `036f1b5ab7` removed `(->priority "A")` |
| `DEADLINE: <date>`, `SCHEDULED: <date>` text | TASK DATE entries set `:logseq.property/deadline` / `:scheduled` | `commands.cljs:664-668` |
| `/Draw` (whiteboard) | none | `bcc478b5f7` removed `"Draw"` |
| `Page embed` / `Block embed` with `{{embed …}}` | **Node embed** (`:block/link`) | `bcc478b5f7` removed `"Page embed"` |
| Block reference with `((uuid))` | **Node reference** → `[[<uuid>]]` | `src/main/frontend/util/ref.cljs:5-7` |
| `<`-triggered commands (`<quote`, `<src`, `<tip`, `<note`, `<warning`, … `#+BEGIN_…` blocks) | none; use **Quote**, **Code block**, **Math block** | `1401bb1dd9` (2024-08-27) "enhance: unify advanced commands with slash commands" removed `block-commands-map` |
| `{{query …}}` | **Query** / **Advanced Query**; `{{query}}` renders a deprecation warning | `block.cljs:1870-1871` |
| `key:: value` properties | **Add property** | `commands.cljs:354-358` |

Earlier versions of this page listed the following names, but none of them is a command in `commands.cljs` at `be800f1711`. Command names are sentence-case labels such as "Code block", not hyphenated slugs. The names: `/h1`–`/h6` shortcuts, `/tmpl`, `/dl`, `/sch`, `/pa`–`/pc`, `/ol`, `/ul`, `/checklist`, `/bold`, `/italic`, `/strikethrough`, `/highlight`, `/mark`, `/image`, `/img`, `/audio`, `/iframe`, `/url`, `/property`, `/prop`, `/page-rename`, `/page-delete`, `/favorites`, `/copy-ref`, `/move-block`, `/duplicate`, `/expand`, `/collapse`, `/search`, `/find`, `/recent`, `/graph`, `/camera`, `/photo`, and a "Popular plugin commands" table.

---

## Troubleshooting

**The menu doesn't open when I type `/`:**

- The `/` must be at the start of a line or begin a new word; `a/` in the middle of a word does not trigger it (`editor.cljs:2272`).
- The menu doesn't open inside a comment editor (`editor.cljs:2270`).

**A command I expected isn't listed:**

- When you're editing a page title, only task, priority and **Add property** entries are shown (`components/editor.cljs:42-53`).
- A custom Status choice isn't offered as a TASK STATUS entry (see above).
- Plugin commands don't exist in the native mobile app (see [Mobile](#mobile)).
- A `:commands` or plugin entry with the same name as a built-in replaces the built-in.

---

**Related:**

- [Keyboard Shortcuts](../configuration/keyboard-shortcuts.md)
- [Templates System](./templates-system.md)
- [Query System](../advanced/query-system.md)
