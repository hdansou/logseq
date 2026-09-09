---
title: 'Keyboard Shortcuts Reference'
description: 'Complete reference for all Logseq keyboard shortcuts and command palette commands'
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
sinceVersion: '0.1.0'
relatedDocs:
  reference:
    - '[config.edn Reference](config-edn.md)'
  howTo:
    - '[Customizing Shortcuts](../../how-to/integrations/custom-shortcuts.md)'
---

# Keyboard Shortcuts Reference

> **Last revised:** 2026-09-09

> Complete reference for all built-in commands available in Logseq's command palette (`Cmd/Ctrl+Shift+P`).

## Legend

- `mod` = `Cmd` on Mac / `Ctrl` on Windows/Linux
- `---` = No keyboard shortcut assigned
- Platform-specific shortcuts indicated where different

---

## Quick Reference by Category

| Category                                              | Commands | Description                    |
| ----------------------------------------------------- | -------- | ------------------------------ |
| [Basics](#basics)                                     | 18       | Copy, paste, search, undo/redo |
| [Block Command Editing](#block-command-editing)       | 14       | Cursor movement, kill, select  |
| [Block Editing](#block-editing)                       | 16       | Create, move, indent blocks    |
| [Block Selection](#block-selection)                   | 16       | Select and manipulate blocks   |
| [Formatting](#formatting)                             | 5        | Bold, italic, highlight        |
| [Navigating](#navigating)                             | 21       | Move around, go to pages       |
| [Toggle](#toggle)                                     | 13       | Toggle UI elements             |
| [Flashcards](#flashcards)                             | 5        | Spaced repetition cards        |
| [PDF](#pdf)                                           | 4        | PDF viewer controls            |
| [Auto-complete](#auto-complete)                       | 5        | Auto-complete menu controls    |
| [Sidebar](#sidebar)                                   | 3        | Right sidebar controls         |
| [Dev Tools](#dev-tools)                               | 10       | Developer mode tools           |
| [Other Commands](#other-commands)                     | 22       | Graph, export, misc            |

---

## Basics

| Command ID                             | Shortcut                                 | Description                     |
| -------------------------------------- | ---------------------------------------- | ------------------------------- |
| `:command-palette/toggle`              | `mod+shift+p`                            | Search commands                 |
| `:editor/copy`                         | `mod+c`                                  | Copy selection or block ref     |
| `:editor/copy-text`                    | `mod+shift+c`                            | Copy selections as text         |
| `:editor/cut`                          | `mod+x`                                  | Cut                             |
| `:editor/indent`                       | `tab`                                    | Indent block                    |
| `:editor/new-block`                    | `enter`                                  | Create new block                |
| `:editor/new-line`                     | `shift+enter`                            | New line in current block       |
| `:editor/outdent`                      | `shift+tab`                              | Outdent block                   |
| `:editor/quick-add`                    | Mac: `mod+e` / Win: `mod+alt+e`         | Quick add                       |
| `:editor/redo`                         | `mod+shift+z` or `mod+y`                | Redo                            |
| `:editor/select-all-blocks`            | `mod+shift+a`                            | Select all blocks               |
| `:editor/select-parent`               | `mod+a`                                  | Select parent block             |
| `:editor/undo`                         | `mod+z`                                  | Undo                            |
| `:go/electron-find-in-page`            | `mod+f`                                  | Find text in page (desktop)     |
| `:go/electron-jump-to-the-next`        | `enter` or `mod+g`                       | Jump to next find result        |
| `:go/electron-jump-to-the-previous`    | `shift+enter` or `mod+shift+g`           | Jump to previous find result    |
| `:go/search`                           | `mod+k`                                  | Search pages and blocks         |
| `:go/search-in-page`                   | `mod+shift+k`                            | Search blocks in page           |

---

## Block Command Editing

| Command ID                                 | Shortcut                            | Description                    |
| ------------------------------------------ | ----------------------------------- | ------------------------------ |
| `:editor/backspace`                        | `backspace`                         | Delete backwards               |
| `:editor/backward-kill-word`               | Mac: --- / Win: `alt+w`            | Delete word backwards          |
| `:editor/backward-word`                    | Mac: `ctrl+shift+b` / Win: `alt+b` | Move cursor backward a word    |
| `:editor/beginning-of-block`               | Mac: --- / Win: `alt+a`            | Move cursor to block beginning |
| `:editor/clear-block`                      | Mac: `ctrl+l` / Win: `alt+l`       | Delete entire block content    |
| `:editor/copy-embed`                       | `mod+shift+e`                       | Copy block embed               |
| `:editor/end-of-block`                     | Mac: --- / Win: `alt+e`            | Move cursor to block end       |
| `:editor/forward-kill-word`                | Mac: `ctrl+w` / Win: `alt+d`       | Delete word forwards           |
| `:editor/forward-word`                     | Mac: `ctrl+shift+f` / Win: `alt+f` | Move cursor forward a word     |
| `:editor/kill-line-after`                  | Mac: --- / Win: `alt+k`            | Delete line after cursor       |
| `:editor/kill-line-before`                 | Mac: `ctrl+u` / Win: `alt+u`       | Delete line before cursor      |
| `:editor/paste-text-in-one-block-at-point` | `mod+shift+v`                       | Paste text into one block      |
| `:editor/select-down`                      | `shift+down`                        | Select content below           |
| `:editor/select-up`                        | `shift+up`                          | Select content above           |

---

## Block Editing

| Command ID                     | Shortcut                                       | Description               |
| ------------------------------ | ---------------------------------------------- | ------------------------- |
| `:editor/backspace`            | `backspace`                                    | Delete backwards          |
| `:editor/cycle-todo`           | `mod+enter`                                    | Rotate TODO state         |
| `:editor/delete`               | `delete`                                       | Delete forwards           |
| `:editor/escape-editing`       | ---                                            | Escape editing mode       |
| `:editor/follow-link`          | `mod+o`                                        | Follow link under cursor  |
| `:editor/indent`               | `tab`                                          | Indent block              |
| `:editor/move-block-down`      | Mac: `mod+shift+down` / Win: `alt+shift+down`  | Move block down           |
| `:editor/move-block-up`        | Mac: `mod+shift+up` / Win: `alt+shift+up`      | Move block up             |
| `:editor/move-blocks`          | `mod+shift+m`                                  | Move blocks to...         |
| `:editor/new-block`            | `enter`                                        | Create new block          |
| `:editor/new-line`             | `shift+enter`                                  | New line in current block |
| `:editor/open-link-in-sidebar` | `mod+shift+o`                                  | Open link in sidebar      |
| `:editor/outdent`              | `shift+tab`                                    | Outdent block             |
| `:editor/zoom-in`              | Mac: `mod+.` or `mod+shift+.` / Win: `alt+right` | Zoom in                |
| `:editor/zoom-out`             | Mac: `mod+,` / Win: `alt+left`                 | Zoom out                  |

---

## Block Selection

| Command ID                                 | Shortcut                          | Description              |
| ------------------------------------------ | --------------------------------- | ------------------------ |
| `:editor/add-property`                     | Mac: `mod+p` / Win: `ctrl+alt+p` | Add property             |
| `:editor/add-property-deadline`            | `p d`                             | Add task deadline        |
| `:editor/add-property-icon`                | `p i`                             | Add icon                 |
| `:editor/add-property-priority`            | `p p`                             | Add task priority        |
| `:editor/add-property-status`              | `p s`                             | Add task status          |
| `:editor/add-comment`                      | `ctrl+space`                      | Add comment              |
| `:editor/add-reaction`                     | `p r`                             | Add reaction             |
| `:editor/delete-selection`                 | `backspace` or `delete`           | Delete selected blocks   |
| `:editor/open-edit`                        | `enter`                           | Edit selected block      |
| `:editor/open-selected-blocks-in-sidebar`  | `shift+enter`                     | Open selected in sidebar |
| `:editor/select-all-blocks`                | `mod+shift+a`                     | Select all blocks        |
| `:editor/select-block-down`                | `alt+down`                        | Select block below       |
| `:editor/select-block-up`                  | `alt+up`                          | Select block above       |
| `:editor/select-parent`                    | `mod+a`                           | Select parent block      |
| `:editor/set-tags`                         | `p t`                             | Set tags for block(s)    |
| `:editor/toggle-display-hidden-properties` | `p a`                             | Toggle hidden properties |

---

## Formatting

| Command ID               | Shortcut      | Description   |
| ------------------------ | ------------- | ------------- |
| `:editor/bold`           | `mod+b`       | Bold          |
| `:editor/highlight`      | `mod+shift+h` | Highlight     |
| `:editor/insert-link`    | `mod+l`       | HTML Link     |
| `:editor/italics`        | `mod+i`       | Italics       |
| `:editor/strike-through` | `mod+shift+s` | Strikethrough |

---

## Navigating

| Command ID                        | Shortcut         | Description                |
| --------------------------------- | ---------------- | -------------------------- |
| `:editor/collapse-block-children` | `mod+up`         | Collapse                   |
| `:editor/down`                    | `down` or `ctrl+n` | Move cursor down        |
| `:editor/expand-block-children`   | `mod+down`       | Expand                     |
| `:editor/jump`                    | `mod+j`          | Jump to property key/value |
| `:editor/left`                    | `left`           | Move cursor left           |
| `:editor/right`                   | `right`          | Move cursor right          |
| `:editor/toggle-block-children`   | `mod+;`          | Toggle expand/collapse     |
| `:editor/toggle-open-blocks`      | `t o`            | Toggle all blocks open/closed |
| `:editor/up`                      | `up` or `ctrl+p` | Move cursor up            |
| `:go/all-graphs`                  | `g shift+g`      | Go to all graphs           |
| `:go/all-pages`                   | `g a`            | Go to all pages            |
| `:go/backward`                    | `mod+[`          | Backwards                  |
| `:go/flashcards`                  | `g f` or `t c`   | Toggle flashcards          |
| `:go/forward`                     | `mod+]`          | Forwards                   |
| `:go/graph-view`                  | `g g`            | Go to graph view           |
| `:go/home`                        | `g h`            | Go to home                 |
| `:go/journals`                    | `g j`            | Go to journals             |
| `:go/keyboard-shortcuts`          | `g s`            | Go to keyboard shortcuts   |
| `:go/next-journal`                | `g n`            | Go to next journal         |
| `:go/prev-journal`                | `g p`            | Go to previous journal     |
| `:go/tomorrow`                    | `g t`            | Go to tomorrow             |

---

## Toggle

| Command ID                    | Shortcut                              | Description                   |
| ----------------------------- | ------------------------------------- | ----------------------------- |
| `:editor/toggle-number-list`  | `t n`                                 | Toggle number list            |
| `:editor/toggle-open-blocks`  | `t o`                                 | Toggle all blocks open/closed |
| `:ui/customize-appearance`    | `c c`                                 | Customize appearance          |
| `:ui/highlight-recent-blocks` | `mod+c mod+r`                         | Highlight recent blocks       |
| `:ui/toggle-brackets`         | `t b`                                 | Toggle brackets display       |
| `:ui/toggle-contents`         | `alt+shift+c`                         | Toggle Contents in sidebar    |
| `:ui/toggle-document-mode`    | `t d`                                 | Toggle document mode          |
| `:ui/toggle-help`             | `shift+/`                             | Toggle help                   |
| `:ui/toggle-left-sidebar`     | `t l`                                 | Toggle left sidebar           |
| `:ui/toggle-right-sidebar`    | `t r`                                 | Toggle right sidebar          |
| `:ui/toggle-settings`         | Mac: `t s` or `mod+,` / Win: `t s`   | Toggle settings               |
| `:ui/toggle-theme`            | `t t`                                 | Toggle dark/light theme       |
| `:ui/toggle-wide-mode`        | `t w`                                 | Toggle wide mode              |

---

## Flashcards

| Command ID              | Shortcut | Description              |
| ----------------------- | -------- | ------------------------ |
| `:cards/again`          | `1`      | Cards: again             |
| `:cards/hard`           | `2`      | Cards: hard              |
| `:cards/good`           | `3`      | Cards: good              |
| `:cards/easy`           | `4`      | Cards: easy              |
| `:cards/toggle-answers` | `s`      | Cards: show/hide answers |

---

## PDF

| Command ID           | Shortcut | Description       |
| -------------------- | -------- | ----------------- |
| `:pdf/close`         | `alt+x`  | Close current PDF |
| `:pdf/find`          | `alt+f`  | Search in PDF     |
| `:pdf/next-page`     | `alt+n`  | Next PDF page     |
| `:pdf/previous-page` | `alt+p`  | Previous PDF page |

---

## Auto-complete

| Command ID                      | Shortcut               | Description          |
| ------------------------------- | ---------------------- | -------------------- |
| `:auto-complete/complete`       | `enter`                | Choose selected item |
| `:auto-complete/meta-complete`  | `mod+enter`            | Choose with modifier |
| `:auto-complete/next`           | `down` or `ctrl+n`     | Select next item     |
| `:auto-complete/prev`           | `up` or `ctrl+p`       | Select previous item |
| `:auto-complete/shift-complete` | `shift+enter`          | Open in sidebar      |

---

## Sidebar

| Command ID                 | Shortcut                                 | Description            |
| -------------------------- | ---------------------------------------- | ---------------------- |
| `:sidebar/clear`           | `mod+c mod+c`                            | Clear right sidebar    |
| `:sidebar/close-top`       | `c t`                                    | Close top sidebar item |
| `:sidebar/open-today-page` | Mac: `mod+shift+j` / Win: `alt+shift+j`  | Open today in sidebar  |

---

## Dev Tools

| Command ID                        | Shortcut | Description            |
| --------------------------------- | -------- | ---------------------- |
| `:dev/export-client-ops-sqlite`   | ---      | Export client ops sqlite |
| `:dev/gc-graph`                   | ---      | Garbage collect graph  |
| `:dev/recompute-checksum`         | ---      | Recompute graph checksum |
| `:dev/replace-graph-with-db-file` | ---      | Replace with db.sqlite |
| `:dev/rtc-start`                  | ---      | RTC Start              |
| `:dev/rtc-stop`                   | ---      | RTC Stop               |
| `:dev/show-block-ast`             | ---      | Show block AST         |
| `:dev/show-block-data`            | ---      | Show block data        |
| `:dev/show-page-data`             | ---      | Show page data         |
| `:dev/validate-db`                | ---      | Validate current graph |

---

## Other Commands

| Command ID                         | Shortcut       | Description                |
| ---------------------------------- | -------------- | -------------------------- |
| `:shell/run`                       | `mod+shift+1`  | Run shell command (desktop) |
| `:page/toggle-favorite`            | `mod+shift+f`  | Toggle favorite            |
| `:editor/copy-page-url`           | ---            | Copy page URL (desktop)    |
| `:editor/insert-youtube-timestamp` | `mod+shift+y`  | Insert YouTube timestamp   |
| `:go/search-themes`               | Mac: `mod+shift+i` / Win: `alt+shift+i` | Search themes |
| `:graph/add`                       | ---            | Add graph                  |
| `:graph/db-add`                    | ---            | Create new DB graph        |
| `:graph/db-save`                   | `mod+s`        | Save DB graph (desktop)    |
| `:graph/export-as-html`           | ---            | Export graph as HTML       |
| `:graph/open`                      | `alt+shift+g`  | Open graph                 |
| `:graph/remove`                    | ---            | Remove graph               |
| `:misc/copy`                       | `mod+c`        | Copy                       |
| `:misc/export-block-data`          | ---            | Export block EDN           |
| `:misc/export-graph-ontology-data` | ---            | Export tags/properties EDN |
| `:misc/export-page-data`           | ---            | Export page EDN            |
| `:misc/import-edn-data`           | ---            | Import EDN data            |
| `:publish/open-dialog`             | `mod+m`        | Open publish dialog        |
| `:search/re-index`                 | `mod+c mod+s`  | Rebuild search index       |
| `:ui/clear-all-notifications`      | ---            | Clear notifications        |
| `:ui/goto-plugins`                 | `t p`          | Go to plugins              |
| `:ui/install-plugin-from-github`   | ---            | Install from GitHub        |
| `:ui/install-plugins-from-file`    | ---            | Install from plugins.edn   |
| `:ui/select-theme-color`           | `t i`          | Select theme colors        |
| `:window/close`                    | `mod+w`        | Close window (desktop)     |

---

## Customizing Shortcuts

Add custom shortcuts in `config.edn`:

```clojure
{:shortcuts
 {:editor/new-block       "enter"
  :editor/new-line        "shift+enter"
  :editor/insert-link     "mod+shift+k"
  :editor/highlight       false           ; disable
  :ui/toggle-settings     "t s"           ; chord
  :editor/up              ["ctrl+k" "up"] ; multiple
  :editor/down            ["ctrl+j" "down"]}}
```

### Shortcut Syntax

| Syntax  | Meaning                | Example                   |
| ------- | ---------------------- | ------------------------- |
| `+`     | Simultaneous keys      | `mod+shift+a`             |
| Space   | Key chord              | `t s` (t then s)          |
| `mod`   | Cmd (Mac) / Ctrl (Win) | `mod+k`                   |
| `false` | Disable shortcut       | `:editor/highlight false` |
| `[...]` | Multiple bindings      | `["ctrl+j" "down"]`       |

---

## See Also

- **Config reference**: [config-edn.md](config-edn.md)
- **Full shortcut source**: [shortcut/config.cljs](https://github.com/logseq/logseq/blob/master/src/main/frontend/modules/shortcut/config.cljs)

---

_Last updated: 2026-09-09 | [Report an issue](https://github.com/logseq/logseq/issues)_
