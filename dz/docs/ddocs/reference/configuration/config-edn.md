---
title: 'config.edn Reference'
description: "Reference for every config.edn key in Logseq DB graphs: type, default, effect, and the source that reads it"
status: stable
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
sinceVersion: '0.1.0'
relatedDocs:
  tutorials:
    - '[Installation Guide](../../tutorials/getting-started/01-installation.md)'
  howTo:
    - '[Custom CSS](../../how-to/integrations/custom-css.md)'
    - '[Custom Themes](../../how-to/integrations/create-theme.md)'
  reference:
    - '[Keyboard Shortcuts](keyboard-shortcuts.md)'
  explanation:
    - '[File vs DB Graphs](../../explanation/concepts/file-vs-db.md)'
---

# config.edn Reference

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-11

> Every `config.edn` key for **DB graphs**, with its type, default, what it actually does, and the
> source line that reads it. Only DB graphs can be opened (`src/main/frontend/components/repo.cljs:321-323`),
> so this page covers only them. All citations are `file:line` at `be800f1711` (app version `2.0.1`,
> `src/main/frontend/version.cljs:3`).

**Rule used throughout:** a key counts as working only if something in `src/` or `deps/` reads it.
Keys that the schema or the template mention but nothing reads are listed in
[Keys that have no effect in DB graphs](#keys-that-have-no-effect-in-db-graphs).

---

## Where config.edn lives

A DB graph has no `logseq/` folder on disk. The graph is a single `db.sqlite`
(`deps/db/src/logseq/db/common/sqlite.cljs:29-33`) under `~/logseq/graphs`
(`deps/common/src/logseq/common/config.cljs:58`). `config.edn` is a record inside that database:
`create_graph.cljs` seeds a `:file/path "logseq/config.edn"` entity, plus empty `logseq/custom.css`,
`logseq/custom.js` and `logseq/publish.css` records
(`deps/db/src/logseq/db/sqlite/create_graph.cljs:205-225`).

### Editing it

| Way                                   | Where                                                                                                    | Source                                                            |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| **Settings > General > Custom configuration > Edit config.edn** | Opens the `logseq/config.edn` record in the built-in code editor                  | `src/main/frontend/components/settings.cljs:201-207`, `:884`; path `src/main/frontend/config.cljs:345-347` |
| Settings toggles                      | Rows such as *Show brackets* or *Logical outdenting* rewrite a single key, keeping the rest of the file's text | `src/main/frontend/handler/config.cljs:19-37` (uses `borkdude.rewrite-edn`) |
| Shortcut editor                       | Writes `:shortcuts` (see [Keyboard shortcuts](#4-keyboard-shortcuts))                                     | `src/main/frontend/modules/shortcut/core.cljs:376-382`            |

### What happens when you save

`save-file!` in `src/main/frontend/handler/db_based/editor.cljs:142-165`:

1. **Deprecation check.** Keys that don't apply to DB graphs raise a *warning* notification:
   "The file {1} has the following deprecations:" (`src/resources/dicts/en.edn:691`). The check doesn't stop the save
   (`src/main/frontend/handler/common/config_edn.cljs:125-147`).
2. **Validation.** The text must parse as EDN, be a map, have no duplicate keys, and match the malli
   schema. Otherwise an *error* notification appears and **the change is not saved**
   (`config_edn.cljs:88-123`). Messages: "Failed to read file" (`en.edn:695-696`), "is not valid… wrapped
   in {}" (`en.edn:693-694`), "has duplicate keys" (`en.edn:682`), "has the following errors" (`en.edn:692`).
3. **Apply.** The record is written, the config is reloaded into app state and shortcuts are refreshed
   (`db_based/editor.cljs:150-163`). No restart is needed. Exceptions are noted per key (e.g. `:custom-css-url`).

If a stored `config.edn` can't be read when the graph opens, you get "The file 'logseq/config.edn' is
invalid. Please reload the app to in order to see the error and fix it." (`en.edn:683`,
`src/main/frontend/handler/repo_config.cljs:20-28`). The exception is then rethrown.

**The schema is open.** `Config-edn` is a plain malli `:map` with every key optional
(`src/main/frontend/schema/handler/common_config.cljc:5-66`). Unknown keys pass validation silently.
Only the types of the keys it lists are checked.

### Global config.edn (Desktop only)

If the global `config.edn` or its directory is missing, Logseq creates both from the default template (`ensure-global-config-file!`, `src/main/frontend/handler/global_config.cljs:36-45`, `6009d9b522`), including before saving a shortcut change. Read from source; not run.

On Desktop, **Settings > General > Custom global configuration > Edit global config.edn** opens a second
`config.edn` in the app's config directory. The row appears only when `global-config-enabled?` is true, and
that is `util/electron?` (`settings.cljs:209-215`, `:895`; `src/main/frontend/config.cljs:146`; path
`src/main/frontend/handler/global_config.cljs:24-27`).

Config is merged as **built-in defaults, then global, then graph**. A later value wins. Map values are
merged one level deep, so the graph's `:default-home` merges with the global one key by key
(`src/main/frontend/state.cljs:469-479`, `:493-502`).

Two keys are read from the **graph** config only, so a global value doesn't reach them:
`:publishing/all-pages-public?` at HTML export (`src/main/frontend/handler/export.cljs:22-28`) and
`:feature/markdown-mirror?` (`src/main/frontend/persist_db/browser.cljs:269-275`).

### Built-in defaults

`db-default-config` (`state.cljs:426-464`) supplies these when no config sets them:

| Key                                     | Built-in value                                             |
| --------------------------------------- | ---------------------------------------------------------- |
| `:feature/enable-search-remove-accents?` | `true`                                                     |
| `:ui/auto-expand-block-refs?`           | `true`                                                     |
| `:ui/hide-empty-properties?`            | `false`                                                    |
| `:default-queries`                      | `{:journals [<Doing, last 14 days> <Todo, next 7 days>]}` |

Other defaults live in the individual getters, and they are listed in the tables below. The shipped template
(`deps/common/resources/templates/config.edn`) sets some keys and leaves others as comments. The
**Template** column says which.

---

## 1. UI

| Key                           | Type      | Default | Template        | Effect                                                                                                                                                                                                                                       | Read at                                                               |
| ----------------------------- | --------- | ------- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `:ui/show-brackets?`          | `boolean` | `true`  | commented (`:13`) | Show `[[ ]]` around page references. Only `false` turns them off. Also toggled by Settings > Editor *Show brackets* and the `:ui/toggle-brackets` shortcut `t b`.                                                                         | `state.cljs:691-693`; `components/block.cljs:1268`, `:1399-1401`; `modules/shortcut/config.cljs:301-302` |
| `:ui/enable-tooltip?`         | `boolean` | `true`  | set (`:9`)      | Controls the **shortcut tooltips** shown by `ui/with-shortcut`. They only appear if the separate *shortcut tooltip* setting is also on. Always off on mobile. Nothing else reads it.                                                        | `src/main/frontend/ui.cljs:818-829`; `state.cljs:714-718`             |
| `:ui/show-full-blocks?`       | `boolean` | `false` | set (`:17`)     | When a block reference points at a block that has a title *and* body lines, only the title renders. Set `true` to render all of it.                                                                                                  | `components/block.cljs:3483`; `state.cljs:728-730`                    |
| `:ui/auto-expand-block-refs?` | `boolean` | `true`  | set (`:21`)     | When you zoom into a block (it becomes the current page), the list of blocks that reference it starts expanded. The references-count button toggles that list (`block.cljs:3503-3518`).                                    | `components/block.cljs:4311-4317`; `state.cljs:736-738`               |
| `:ui/show-command-doc?`       | `boolean` | `true`  | set (`:124`)    | Show each command's description in the `/` command popup.                                                                                                                                                                                    | `components/editor.cljs:105`; `state.cljs:720-722`                    |
| `:ui/show-empty-bullets?`     | `boolean` | `false` | set (`:128`)    | Render the bullet of empty blocks outside document mode. Without it, empty blocks that aren't being edited follow the hidden-bullet branch.                                                                                              | `components/block.cljs:2358-2373`                                     |
| `:ui/hide-empty-properties?`  | `boolean` | `false` | commented (`:5`) | Hide properties that have no value.                                                                                                                                                                                                         | `components/property.cljs:825`; `components/property/value.cljs:362`  |

---

## 2. Feature toggles

| Key                                              | Type      | Default | Template          | Effect                                                                                                                                                                                                                 | Read at                                                                                                    |
| ------------------------------------------------ | --------- | ------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `:feature/enable-search-remove-accents?`         | `boolean` | `true`  | set (`:26`)       | Ignore accents when matching and highlighting search text. The template says to rebuild the search index afterwards. That is the `:search/re-index` command, bound to `mod+c mod+s` (`modules/shortcut/config.cljs:337`).        | `src/main/frontend/search.cljs:24`; `handler/search.cljs:126-144`; `components/cmdk/list_item.cljs:27-31` |
| `:feature/enable-flashcards?`                    | `boolean` | `true`  | commented (`:30`) | Only `false` disables it. That hides the flashcard context-menu items, the left-sidebar *Flashcards* entry and the mobile flashcards tab. Also toggled in Settings > Features.                                                         | `state.cljs:684-688`; `components/content.cljs:134`, `:353`; `components/left_sidebar.cljs:300`; `src/main/mobile/bottom_tabs.cljs:276`; `settings.cljs:643-649` |
| `:feature/disable-scheduled-and-deadline-query?` | `boolean` | `false` | commented (`:34`) | `true` hides the *Scheduled and deadlines* section on today's journal.                                                                                                                                                  | `components/scheduled_deadlines.cljs:15-20`; `state.cljs:670-672`                                          |

---

## 3. Editor and outliner

| Key                                       | Type      | Default | Template           | Effect                                                                                                                                                                                                      | Read at                                                                                  |
| ----------------------------------------- | --------- | ------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `:block/title-max-length`                 | `int`     | `10000` | set (`:120`)       | A block whose content is longer shows a "large block" warning (code blocks excepted), and its click handler is skipped, so clicking doesn't start editing or selection. Nothing in search reads this key.                                   | `state.cljs:703-708`; `components/block.cljs:2735-2738`, `:3537-3540`                    |
| `:editor/logical-outdenting?`             | `boolean` | `false` | commented (`:207`) | By default (direct outdenting), the siblings below an outdented block become its children. With `true`, only the block moves and those siblings stay under the old parent. Settings > Editor *Logical outdenting* (`en.edn:1718`). | `handler/block.cljs:226-229`; `deps/outliner/src/logseq/outliner/core.cljs:1322-1332`; `state.cljs:724-726`; `settings.cljs:920` |
| `:editor/extra-codemirror-options`        | `map`     | `{}`    | commented (`:200-203`) | Merged over the code-block editor's defaults (`:autoCloseBrackets true`, `:lineNumbers true`, `:styleActiveLine true`, …). Any [CodeMirror 5 option](https://codemirror.net/5/doc/manual.html#config) is accepted, but `:mode` and `:extraKeys` are overridden, addon options such as `:foldGutter` do nothing, and an unknown `:keyMap` or `:scrollbarStyle` crashes the app: see [Code Block Editor Options](codemirror-options.md). `:readOnly` is dropped while you edit `config.edn` itself. | `src/main/frontend/extensions/code.cljs:370-372`, `:425-433`                             |
| `:outliner/block-title-collapse-enabled?` | `boolean` | `false` | set (`:163`)       | Let a block with a multi-line body but no children be collapsed to its first line.                                                                                                                         | `components/block.cljs:2834-2839`; `handler/editor.cljs:3944-3946`                       |
| `:shortcut/doc-mode-enter-for-new-block?` | `boolean` | `false` | set (`:115`)       | In document mode, Enter inserts a new line by default. Set `true` to make Enter create a new block.                                                                                                        | `state.cljs:740-743`; `handler/editor.cljs:2662`, `:2682`                                |

```clojure
{:block/title-max-length 10000
 :editor/logical-outdenting? false
 :editor/extra-codemirror-options {:lineWrapping true}
 :outliner/block-title-collapse-enabled? false
 :shortcut/doc-mode-enter-for-new-block? false}
```

---

## 4. Keyboard shortcuts

| Key          | Type                                               | Default | Template     | Effect                                         | Read at                                                                     |
| ------------ | -------------------------------------------------- | ------- | ------------ | ---------------------------------------------- | --------------------------------------------------------------------------- |
| `:shortcuts` | map of command keyword → `string`, `false` or vector of strings | `{}`    | set (`:110`) | Rebind or disable commands by their shortcut id. | schema `common_config.cljc:24-26`; `state.cljs:577-582`; `modules/shortcut/data_helper.cljs:53-61` |

- `+` presses keys together (`ctrl+shift+a`). A space makes a chord (`t s`). `mod` is Cmd on macOS and Ctrl elsewhere.
  `false` disables a binding, and a vector gives several bindings (template `:89-99`).
- Effective bindings are merged in this order: browser storage `:ls-shortcuts`, then the merged global and graph config (`state.cljs:577-582`).
- The in-app shortcut editor writes to the graph's `config.edn`. It also writes to the global `config.edn` on Desktop, or to browser storage on web
  (`modules/shortcut/core.cljs:376-382`, `:403-408`).
- Shortcut ids and default bindings: [Keyboard Shortcuts](keyboard-shortcuts.md) and
  `src/main/frontend/modules/shortcut/config.cljs`.

```clojure
{:shortcuts
 {:editor/new-block   "enter"
  :editor/new-line    "shift+enter"
  :editor/insert-link "mod+shift+k"      ; default is "mod+l" (shortcut/config.cljs:241)
  :editor/highlight   false              ; disable
  :ui/toggle-settings "t s"              ; chord
  :editor/up          ["ctrl+k" "up"]    ; several bindings
  :editor/down        ["ctrl+j" "down"]}}
```

---

## 5. Home page and startup sidebar

| Key             | Type  | Default | Template              | Effect                                              | Read at |
| --------------- | ----- | ------- | --------------------- | --------------------------------------------------- | ------- |
| `:default-home` | `map` | none    | commented (`:73-87`)  | Page shown at startup and pages opened in the right sidebar. Without it, the journals view loads (template `:74`). | schema `common_config.cljc:21-23`; `state.cljs:546-552` |

| Sub-key    | Type                     | Effect                                                                                                                                                                                                                                             | Read at                                                                                            |
| ---------- | ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `:page`    | `string`                 | Page name used as home (`handler/route.cljs:298`). Settings > Features *Set the default home page* writes it (`settings.cljs:529-551`). Renaming that page updates it (`handler/common/page.cljs:244-246`), and deleting it removes it (`:205-210`). | `components/header.cljs:527`; `handler/route.cljs:298`                                             |
| `:sidebar` | `string` or `[string …]` | Pages to open in the right sidebar at startup. `"Contents"` opens the Contents page.                                                                                                                                                                     | `components/container.cljs:150-169`                                                                |

```clojure
{:default-home {:page "Dashboard" :sidebar ["Contents" "Inbox"]}}
```

---

## 6. Journals and scheduling

| Key                      | Type                   | Default | Template          | Effect                                                                                                                                                                                     | Read at                                                                 |
| ------------------------ | ---------------------- | ------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| `:start-of-week`         | `0`–`6` (enum)         | `6`     | set (`:48`)       | First day of the week in date pickers. `0` = Monday … `6` = Sunday. The picker receives `(mod (inc n) 7)`. If unset, the value comes from `[:me :settings :start-of-week]` in app state, then `6`.                                            | `state.cljs:593-597`; `src/main/frontend/ui.cljs:1057`                  |
| `:scheduled/future-days` | `int`                  | `7`     | commented (`:42`) | How many days ahead the *Scheduled and deadlines* section on today's journal looks. A non-integer is ignored.                                                                                  | `state.cljs:588-591`; `src/main/frontend/db/async.cljs:401-413`         |
| `:default-queries`       | `{:journals [query …]}` | two built-in queries | commented (`:143-146`) | Queries rendered at the bottom of **today's** journal. They aren't rendered in the sidebar. See [Queries](#7-queries).                                                                                   | `components/page.cljs:234-254`                                          |

**Journal title format is not a config key in DB graphs.** `:journal/page-title-format` is ignored (see
[below](#keys-that-have-no-effect-in-db-graphs)). The format is stored as
`:logseq.property.journal/title-format` on the `Journal` tag. You change it with Settings > Editor *Preferred date format*
(`settings.cljs:460-483`), which asks for a refresh (`:480`). It is loaded when the graph opens
(`src/main/frontend/handler/repo.cljs:83-84`, `db/async.cljs:67-73`). The default is `MMM do, yyyy`
(`state.cljs:564`, `deps/common/src/logseq/common/util/date_time.cljs:10`).

---

## 7. Queries

| Key                        | Type                             | Default                                  | Template               | Effect                                                                                                                                                                                          | Read at                                          |
| -------------------------- | -------------------------------- | ---------------------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `:query/views`             | map of keyword → `(fn …)` form   | none (template sets `:pprint`)           | set (`:131-133`)       | Named `:view` functions. An advanced query with `:view :pprint` looks the name up here and evaluates it with SCI.                                                                               | `src/main/frontend/components/query.cljs:148-149` |
| `:query/result-transforms` | map of keyword → `(fn …)` form   | none                                     | commented (`:138-140`) | Named `:result-transform` functions. `:result-transform :name` looks the name up here. An unknown name throws "Missing query result transform".                                                   | `components/query.cljs:133-140`                  |
| `:default-queries`         | `{:journals [map …]}`            | Doing (last 14 days) and Todo (next 7 days) | commented (`:142-145`) | Queries on today's journal. Each entry is an advanced-query map (`:query`, optional `:inputs`, `:title` string). Entries always render collapsed (`page.cljs:241`), and a `:title` string renders as the header (`query.cljs:94-115`, `:166-169`). | `components/page.cljs:234-254`; defaults `state.cljs:436-462` |

Setting `:default-queries {:journals [...]}` **replaces** the two built-in queries, because the merge
goes only one level deep (`state.cljs:469-479`). Use `{:journals []}` to remove them.

```clojure
{:query/views
 {:pprint (fn [r] [:pre.code (pprint r)])}        ; use as :view :pprint

 :query/result-transforms
 {:sort-by-title (fn [result] (sort-by (fn [b] (get b :block/title)) result))}

 :default-queries
 {:journals
  [{:title "Doing, last 14 days"
    :query [:find (pull ?b [*])
            :in $ ?start ?today
            :where
            (task ?b #{"Doing"})
            [?b :block/page ?p]
            [?p :block/journal-day ?d]
            [(>= ?d ?start)]
            [(<= ?d ?today)]]
    :inputs [:14d :today]}]}}
```

The `:default-queries` entry above is the built-in *Doing* query (`state.cljs:438-449`), with a plain
`:title` in place of its translated `:title-key`. Transforms receive plain pulled maps. Priority is a
closed-value **ref** (`deps/db/src/logseq/db/frontend/property.cljs:323-329`), so
`(get b :logseq.property/priority)` returns an entity map, not a sortable string. Pull the ident
with `(pull ?b [* {:logseq.property/priority [:db/ident]}])` and rank it in the transform; see
[Customize the Journal Queries](../../how-to/queries/customize-journal-queries.md), which also
covers the built-in TODO and Scheduled and Deadline sections.

---

## 8. Linked references

| Key                              | Type  | Default | Template     | Effect                                                                                                                                                     | Read at                                                                                  |
| -------------------------------- | ----- | ------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `:ref/default-open-blocks-level` | `int` | `2`     | set (`:184`) | Blocks at this depth or deeper start collapsed in linked references and list views. Values above `9` count as `9`. A negative or non-integer value falls back to `2`. | `state.cljs:599-605`; `components/reference.cljs:15`; `handler/editor.cljs:4397-4400`   |

```clojure
;; a [[page]] (level 1) > b (2) > c (3) > d (4)
{:ref/default-open-blocks-level 2}  ; b collapsed
{:ref/default-open-blocks-level 3}  ; c collapsed
```

---

## 9. Custom CSS, custom JS, macros and slash commands

| Key               | Type                        | Default | Template             | Effect                                                                                                                                                                                                                                                                                                         | Read at                                                                                           |
| ----------------- | --------------------------- | ------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `:custom-css-url` | `string`                    | none    | commented (`:50-53`) | Used **instead of** the `logseq/custom.css` record. A value starting with `http` becomes a stylesheet link. Anything else is treated as CSS text, so `"@import url('…');"` works. It is applied when the graph opens, not when `config.edn` is saved: only saving `custom.css` re-applies styles (`db_based/editor.cljs:164-165`).        | `handler/ui.cljs:112-119`; `src/main/frontend/util.cljc:933-935`; `handler/repo.cljs:89-90`       |
| `:custom-js-url`  | `string`                    | none    | commented (`:55-58`) | Used instead of `logseq/custom.js`. An `http…` value is loaded as a script. Any other value is read as a file record of the graph. Before running, the app asks for confirmation (`en.edn:1235`), and it asks again after 7 days.                                                                                        | `handler/ui.cljs:129-165`; `components/plugins.cljs:1785-1795`                                    |
| `:macros`         | map of string/keyword → `string` | `{}` | set (`:220`)       | `{{name a,b}}` renders the macro text with `$1`, `$2`, … replaced, parsed as Markdown. Also expanded in property values. A built-in `img` macro is always present.                                                                                                               | `state.cljs:512-519`; `components/block.cljs:1866-1875`, `:1665-1680`; `deps/common/src/logseq/common/util/macro.cljs:13-38`; `components/property/value.cljs:1784` |
| `:commands`       | `[[name output] …]`         | `[]`    | set (`:154`)         | Extra entries in the **`/` slash-command** menu, not the command palette (despite the template comment). A string `output` is inserted as-is. A vector `output` is run as editor steps.                                                                                                        | schema `common_config.cljc:43-45`; `src/main/frontend/commands.cljs:367-376`; `handler/common/editor.cljs:15-24` |

```clojure
{:custom-css-url "@import url('https://cdn.jsdelivr.net/gh/dracula/logseq@master/custom.css');"
 :macros {"poem" "Rose is $1, violet's $2. Life's ordered: Org assists you."}   ; {{poem red,blue}}
 :commands [["js" "Javascript"]
            ["md" "Markdown"]]}                                                 ; /js inserts "Javascript"
```

---

## 10. Export and publishing

| Key                             | Type                                             | Default | Template           | Effect                                                                                                                                                                                                                                                                                                                                 | Read at                                                                                                          |
| ------------------------------- | ------------------------------------------------ | ------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `:export/bullet-indentation`    | `:tab` \| `:two-spaces` \| `:four-spaces` \| `:eight-spaces` | `:tab`  | commented (`:66`)  | Indentation used when exporting blocks as text.                                                                                                                                                                                                                                                                                         | `state.cljs:607-617`; `handler/export/common.cljs:8`, `:43`                                                      |
| `:publishing/all-pages-public?` | `boolean`                                        | `false` | commented (`:71`)  | For the HTML export of public pages (`download-repo-as-html!`, `handler/export.cljs:30-34`): `true` exports every page except those with `publishing-public?` set to `false`. `false` exports only pages with `:logseq.property/publishing-public? true`. Only the **graph** config counts at export. Settings > Editor *All pages public when publishing* writes it. | `deps/publishing/src/logseq/publishing/html.cljs:141-146`; `deps/publishing/src/logseq/publishing/db.cljs:17`, `:89`; `handler/export.cljs:22-28`; `settings.cljs:553-559` |

---

## 11. Mobile and quick capture

| Key                        | Type  | Default | Template                | Read at                                                                             |
| -------------------------- | ----- | ------- | ----------------------- | ----------------------------------------------------------------------------------- |
| `:mobile/photo`            | `map` | none    | commented (`:186-189`)  | `src/main/frontend/mobile/camera.cljs:32-41`                                        |
| `:quick-capture-templates` | `map` | none    | commented (`:213-224`)  | `src/main/frontend/quick_capture.cljs:75-83`; `src/main/frontend/mobile/intent.cljs:227-229` |
| `:quick-capture-options`   | `map` | none    | commented (`:226-235`)  | `quick_capture.cljs:29-57`                                                          |

| Sub-key                                  | Type      | Default **in code**                                   | Effect                                                                                                          |
| ---------------------------------------- | --------- | ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `:mobile/photo :allow-editing?`          | `boolean` | unset (off)                                           | Let the camera plugin crop or edit the photo before inserting it (`camera.cljs:35-37`).                                      |
| `:mobile/photo :quality`                 | `int`     | `80`                                                  | Photo quality passed to the camera plugin (`camera.cljs:38-39`).                                                |
| `:quick-capture-templates :text`         | `string`  | `"**{time}** [[quick capture]]: {text} {url}"`        | Template for captured content. `{time}` is the capture time, `{date}` today's journal title, `{text}` the shared text, `{url}` the link or asset (`quick_capture.cljs:75-83`). The mobile share intent uses it too. |
| `:quick-capture-options :insert-today?`  | `boolean` | `false`                                               | With no target page given, capture into today's journal (`quick_capture.cljs:29-31`, `:42-45`).                  |
| `:quick-capture-options :redirect-page?` | `boolean` | `false`                                               | When the capture targets a page other than the current one, open that page (passed as `:redirect?`, `quick_capture.cljs:32-34`, `:95-96`). |
| `:quick-capture-options :default-page`   | `string`  | none                                                  | Target page when neither a page nor `:insert-today?` applies. After it come the current page, then today (`quick_capture.cljs:40-57`). |

The template comments give `:insert-today?` a default of `true` and `:default-page` a default of `"quick capture"`
(template `:232-234`). The code defaults are `false` and none. For `:prettify-url?` and
`:quick-capture-templates :media`, see [Read but broken](#read-but-broken).

---

## 12. Keys outside the schema that are still read

The schema doesn't list these keys. It still accepts them, because the map is open.

| Key                            | Type      | Default | Effect                                                                                                                                                                                                          | Read at                                                                                                   |
| ------------------------------ | --------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `:feature/markdown-mirror?`    | `boolean` | `false` | Desktop only. Settings > Features *Markdown Mirror*: "Write a derived Markdown copy of edited pages to the graph's mirror/markdown folder" (`en.edn:1732-1733`). Read from graph config only.   | `settings.cljs:805-843`, `:1158-1159`; `persist_db/browser.cljs:269-275`; `persist_db.cljs:285-308`       |
| `:feature/enable-grammarly?`   | `boolean` | `false` | Its only effect: the block editor textarea starts with 2 rows instead of 1.                                                                                                                                  | `state.cljs:666-668`; `components/editor.cljs:747`                                                        |
| `:publishing/enable-editing?`  | `boolean` | none    | Only in a `PUBLISHING` build: allow editing in the published app.                                                                                                                                                  | `state.cljs:504-510`; `components/header.cljs:208`, `:232`                                                |
| `:all-pages-public?`           | `boolean` | none    | Legacy alias, read when `:publishing/all-pages-public?` is unset. Not flagged as deprecated.                                                                                                                      | `state.cljs:540-544`; `deps/publishing/src/logseq/publishing/html.cljs:141-143`                           |
| `:block/content-max-length`    | `int`     | none    | Legacy alias, read when `:block/title-max-length` is unset. Not flagged as deprecated.                                                                                                                        | `state.cljs:703-708`                                                                                      |

---

## Keys that have no effect in DB graphs

### Flagged when you save

`file-only-config` lists 23 keys (`deps/common/src/logseq/common/config.cljs:108-142`).
`config_edn.cljs:52-57` adds `:editor/command-trigger` and `:arweave/gateway`. Saving a `config.edn` that
contains any of the 25 shows the deprecation warning. The save still goes through. The app itself doesn't read any of them.
Some are read **once**, by the *File to DB graph* importer, from the *source* file graph's config
(see [Importing a file graph](#importing-a-file-graphs-configedn)).

| Key                                 | Warning text (`en.edn`)                                                  | Notes                                                                                                   |
| ----------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| `:file/name-format`                 | "is not used in DB graphs." (`:835`)                                     | Importer uses it to decode file names (`exporter.cljs:3596`).                                           |
| `:file-sync/ignore-files`           | same                                                                     | File Sync was removed (`bcc478b5f7`).                                                                   |
| `:hidden`                           | same                                                                     | Importer skips matching files (`exporter.cljs:3627`, `config.cljs:20-27`).                              |
| `:ignored-page-references-keywords` | same                                                                     | Read by the graph-parser property parsing that the importer uses (`deps/graph-parser/src/logseq/graph_parser/text.cljs:151-154`).  |
| `:journal/file-name-format`         | same                                                                     | —                                                                                                       |
| `:journal/page-title-format`        | same                                                                     | Importer copies it to the Journal tag's title format (`exporter.cljs:3429-3432`). Afterwards, use Settings > Editor *Preferred date format*. |
| `:journals-directory`               | same                                                                     | —                                                                                                       |
| `:logbook/settings`                 | same                                                                     | No sub-key is read anywhere.                                                                            |
| `:org-mode/insert-file-link?`       | same                                                                     | —                                                                                                       |
| `:pages-directory`                  | same                                                                     | —                                                                                                       |
| `:preferred-workflow`               | same                                                                     | Tasks use the Status property, a closed set of six values (`deps/db/src/logseq/db/frontend/property.cljs:313-318`). |
| `:property/separated-by-commas`     | same                                                                     | Read by the graph-parser property parsing that the importer uses (`text.cljs:122-127`).                                             |
| `:property-pages/excludelist`       | same                                                                     | Importer strips it before parsing (`exporter.cljs:3595`).                                               |
| `:srs/learning-fraction`            | same                                                                     | Flashcards use FSRS properties (`property.cljs:610-615`).                                               |
| `:srs/initial-interval`             | same                                                                     | same                                                                                                    |
| `:whiteboards-directory`            | same                                                                     | Whiteboards were removed (`bcc478b5f7`).                                                                |
| `:feature/enable-whiteboards?`      | same                                                                     | Whiteboards were removed (`bcc478b5f7`).                                                                |
| `:preferred-format`                 | "is not used in DB graphs as there is only markdown mode." (`:812`)      | `get-preferred-format` always returns `:markdown` (`state.cljs:555-557`).                               |
| `:property-pages/enabled?`          | "…as all properties have pages." (`:834`)                                | Importer strips it (`exporter.cljs:3595`).                                                              |
| `:block-hidden-properties`          | "…as hiding a property is done in its configuration." (`:807`)           | —                                                                                                       |
| `:feature/enable-block-timestamps?` | "…as it is always enabled." (`:811`)                                     | —                                                                                                       |
| `:favorites`                        | "is not stored in config for DB graphs." (`:810`)                        | Favorites live on the hidden `$$$favorites` page (`config.cljs:59`, `create_graph.cljs:195-203`). Importer converts them (`exporter.cljs:3577-3588`). |
| `:default-templates`                | "is replaced by #Template and the `Apply template to tags` property." (`:808`) | —                                                                                                  |
| `:editor/command-trigger`           | "is no longer supported. Please use '/' and report bugs on it." (`:809`) | The trigger is the constant `"/"` (`src/main/frontend/commands.cljs:28`).                               |
| `:arweave/gateway`                  | "is no longer supported." (`:806`)                                       | —                                                                                                       |

### Not read anywhere

`git grep` at `be800f1711` over `src/` and `deps/` finds no reader for these keys. The search tried the full
keyword, the bare name without namespace (`enable-journals`, `forcesettings`, `dwim`, `linkable-built-in`,
`disabled-in-block-with-tags`), and map destructuring. They aren't flagged, so setting them is silently
accepted and does nothing.

| Key                                            | What the search found                                                                                          |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `:meta/version`                                | Only the schema (`common_config.cljc:8`) and the template (`:1`).                                              |
| `:mobile` → `:gestures/disabled-in-block-with-tags` | Only the schema (`common_config.cljc:54-55`) and the template comment (`:191-195`).                        |
| `:feature/enable-journals?`                    | Nothing.                                                                                                       |
| `:graph/settings`, `:graph/forcesettings`      | Nothing. `:graph/settings` exists only as an i18n key (`components/graph.cljs:662-671`). Graph-view settings are kept per graph under `logseq.graph.settings.<repo>` (`graph.cljs:23-34`) and edited in the graph view's own panel. |
| `:dwim/settings`                               | Nothing.                                                                                                       |
| `:ref/linkable-built-in-properties`            | Nothing. Graph-parser has an unrelated var, `editable-linkable-built-in-properties` (`deps/graph-parser/src/logseq/graph_parser/property.cljs:34`). |

### Read but broken

| Key                                         | What happens                                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `:editor/preferred-pasting-file?`           | Only the Settings > Editor *Prefer pasting file* toggle reads and writes it (`settings.cljs:499-505`, `:910`; `handler/config.cljs:59-61`; `state.cljs:732-734`). The paste handler no longer reads it: the last change to it in `handler/paste.cljs` was `11600c8dc3` ("fix: paste files when presented"). The toggle does nothing. |
| `:quick-capture-options :prettify-url?`     | In the schema (`common_config.cljc:66`) and the template (`:230`, `:235`), but the reader looks up the misspelled path `[:quick-capture-option :prettify-url?]` (`quick_capture.cljs:35-37`). The value is therefore always the fallback `true`: YouTube/X links are always wrapped as `{{video}}`/`{{twitter}}`.                |
| `:quick-capture-templates :media`           | In the schema (`common_config.cljc:61`) and the template (`:224`). Only `:text` is read (`quick_capture.cljs:75-77`, `mobile/intent.cljs:227-229`).                                                                                                                                                                         |
| `:ui/mask-email?`                           | The built-in default is `true` (`state.cljs:464`). The account e-mail component passes the whole `:config` state to `mask-email?` (`components/email.cljs:25-26`). That state is keyed by repo (`state.cljs:1513-1515`), so `(:ui/mask-email? config)` is always `nil` and the address is always masked (`src/main/frontend/util/email.cljs:28-30`). Setting `false` has no effect. |

---

## Importing a file graph's config.edn

The only path from a file graph is **File to DB graph** (`en.edn:926-927`; import screen
`src/main/frontend/components/imports.cljs:605-614`), handled by `import-file-to-db-handler` (`imports.cljs:477-498`). The selected folder
must contain `logseq/config.edn`. Otherwise the import fails with "Import failed as the file
'logseq/config.edn' was not found for a Logseq graph." (`imports.cljs:495-498`, `en.edn:938`).

`export-config-file` (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:3473-3496`) then does the following:

- It saves the imported text as the new graph's `logseq/config.edn` **with every `file-only-config` key removed**
  (`:3368-3370`). You don't have to clean those keys up by hand.
- If present, it copies `:journal/page-title-format` (or the older `:date-formatter`) to the Journal tag's
  `:logseq.property.journal/title-format` (`:3372-3375`).
- It keeps the original map for the rest of the import. That map supplies `:hidden` (`:3570`), `:favorites` (`:3520-3531`),
  `:file/name-format` (`:3539`) and `:macros` (`:3544`).
- If the source `config.edn` is invalid, it reports "Import may have mistakes due to an invalid config.edn"
  and continues with a default config (`:3378-3382`).

---

## Complete example

Every key below has a reader at `be800f1711`.

```clojure
{;; UI
 :ui/show-brackets? true
 :ui/show-full-blocks? false
 :ui/auto-expand-block-refs? true
 :ui/show-command-doc? true
 :ui/show-empty-bullets? false
 :ui/hide-empty-properties? false

 ;; Features
 :feature/enable-search-remove-accents? true
 :feature/enable-flashcards? true
 :feature/disable-scheduled-and-deadline-query? false

 ;; Editor and outliner
 :block/title-max-length 10000
 :editor/logical-outdenting? false
 :editor/extra-codemirror-options {:lineWrapping true}
 :outliner/block-title-collapse-enabled? false
 :shortcut/doc-mode-enter-for-new-block? false

 ;; Shortcuts
 :shortcuts {:ui/toggle-settings "t s"}

 ;; Home, journals, references
 :default-home {:page "Dashboard" :sidebar ["Contents"]}
 :start-of-week 0
 :scheduled/future-days 14
 :ref/default-open-blocks-level 2

 ;; Queries
 :query/views {:pprint (fn [r] [:pre.code (pprint r)])}

 ;; Customization
 :macros {"poem" "Rose is $1, violet's $2."}
 :commands [["js" "Javascript"]]

 ;; Export and publishing
 :export/bullet-indentation :two-spaces
 :publishing/all-pages-public? false

 ;; Mobile
 :mobile/photo {:allow-editing? true :quality 80}
 :quick-capture-options {:insert-today? true}}
```

---

## See also

- [Keyboard Shortcuts](keyboard-shortcuts.md): shortcut ids and default bindings for `:shortcuts`
- [Custom CSS](../../how-to/integrations/custom-css.md)
- [File vs DB Graphs](../../explanation/concepts/file-vs-db.md)

## Source references

- [Shipped template](../../../../../deps/common/resources/templates/config.edn)
- [Schema: `common_config.cljc`](../../../../../src/main/frontend/schema/handler/common_config.cljc)
- [Getters, defaults and merge order: `state.cljs`](../../../../../src/main/frontend/state.cljs)
- [Deprecated keys: `file-only-config`](../../../../../deps/common/src/logseq/common/config.cljs)
- [Save-time validation and deprecation warnings: `config_edn.cljs`](../../../../../src/main/frontend/handler/common/config_edn.cljs)
- [Save flow: `db_based/editor.cljs`](../../../../../src/main/frontend/handler/db_based/editor.cljs)
- [Import: `exporter.cljs`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs)

> Links resolve against this worktree's checkout. Line numbers are for `be800f1711` (origin/master).
