# Logseq Command Palette - Complete Command Reference

**Generated automatically from source code**

This document lists all built-in commands available in Logseq's command palette (`Cmd/Ctrl+Shift+P`).

**Legend:**

- `mod` = `Cmd` on Mac / `Ctrl` on Windows/Linux
- `—` = No keyboard shortcut assigned
- Platform-specific shortcuts are indicated where different

**Total Commands:** 135

---

## All Commands

| Command ID                                 | Keyboard Shortcut                                                                         | Description                                                | Category              |
| ------------------------------------------ | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------- | --------------------- |
| `:command-palette/toggle`                  | `mod+shift+p`                                                                             | Search commands                                            | Basics                |
| `:editor/copy`                             | `mod+c`                                                                                   | Copy (copies either selection, or block reference)         | Basics                |
| `:editor/copy-text`                        | `mod+shift+c`                                                                             | Copy selections as text                                    | Basics                |
| `:editor/cut`                              | `mod+x`                                                                                   | Cut                                                        | Basics                |
| `:editor/quick-add`                        | Mac: `mod+e` / Win/Linux: `mod+alt+e`                                                     | Quick add                                                  | Basics                |
| `:editor/redo`                             | ` `                                                                                       | Redo                                                       | Basics                |
| `:editor/undo`                             | `mod+z`                                                                                   | Undo                                                       | Basics                |
| `:go/electron-find-in-page`                | `mod+f`                                                                                   | Find text in page                                          | Basics                |
| `:go/electron-jump-to-the-next`            | ` `                                                                                       | Jump to the next match to your Find bar search             | Basics                |
| `:go/electron-jump-to-the-previous`        | ` `                                                                                       | Jump to the previous match to your Find bar search         | Basics                |
| `:go/search`                               | `mod+k`                                                                                   | Search pages and blocks                                    | Basics                |
| `:go/search-in-page`                       | `mod+shift+k`                                                                             | Search blocks in page                                      | Basics                |
| `:go/search-themes`                        | Mac: `mod+shift+i` / Win/Linux: `alt+shift+i`                                             | Search themes                                              | Basics                |
| `:editor/backspace`                        | `backspace`                                                                               | Backspace / Delete backwards                               | Block Command Editing |
| `:editor/backward-kill-word`               | `alt+w`                                                                                   | Delete a word backwards                                    | Block Command Editing |
| `:editor/backward-word`                    | Mac: `ctrl+shift+b` / Win/Linux: `alt+b`                                                  | Move cursor backward a word                                | Block Command Editing |
| `:editor/beginning-of-block`               | `alt+a`                                                                                   | Move cursor to the beginning of a block                    | Block Command Editing |
| `:editor/clear-block`                      | Mac: `ctrl+l` / Win/Linux: `alt+l`                                                        | Delete entire block content                                | Block Command Editing |
| `:editor/copy-embed`                       | `mod+shift+e`                                                                             | Copy a block embed pointing to the current block           | Block Command Editing |
| `:editor/end-of-block`                     | `alt+e`                                                                                   | Move cursor to the end of a block                          | Block Command Editing |
| `:editor/forward-kill-word`                | Mac: `ctrl+w` / Win/Linux: `alt+d`                                                        | Delete a word forwards                                     | Block Command Editing |
| `:editor/forward-word`                     | Mac: `ctrl+shift+f` / Win/Linux: `alt+f`                                                  | Move cursor forward a word                                 | Block Command Editing |
| `:editor/kill-line-after`                  | `alt+k`                                                                                   | Delete line after cursor position                          | Block Command Editing |
| `:editor/kill-line-before`                 | Mac: `ctrl+u` / Win/Linux: `alt+u`                                                        | Delete line before cursor position                         | Block Command Editing |
| `:editor/paste-text-in-one-block-at-point` | `mod+shift+v`                                                                             | Paste text into one block at point                         | Block Command Editing |
| `:editor/select-down`                      | `shift+down`                                                                              | Select content below                                       | Block Command Editing |
| `:editor/select-up`                        | `shift+up`                                                                                | Select content above                                       | Block Command Editing |
| `:editor/cycle-todo`                       | `mod+enter`                                                                               | Rotate the TODO state                                      | Block Editing         |
| `:editor/delete`                           | `delete`                                                                                  | Delete / Delete forwards                                   | Block Editing         |
| `:editor/escape-editing`                   | —                                                                                         | Escape editing                                             | Block Editing         |
| `:editor/follow-link`                      | `mod+o`                                                                                   | Follow link under cursor                                   | Block Editing         |
| `:editor/indent`                           | `tab`                                                                                     | Indent block                                               | Block Editing         |
| `:editor/move-block-down`                  | Mac: `mod+shift+down` / Win/Linux: `alt+shift+down`                                       | Move block down                                            | Block Editing         |
| `:editor/move-block-up`                    | Mac: `mod+shift+up` / Win/Linux: `alt+shift+up`                                           | Move block up                                              | Block Editing         |
| `:editor/move-blocks`                      | `mod+shift+m`                                                                             | Move blocks to                                             | Block Editing         |
| `:editor/new-block`                        | `enter`                                                                                   | Create new block                                           | Block Editing         |
| `:editor/new-line`                         | `shift+enter`                                                                             | New line in current block                                  | Block Editing         |
| `:editor/open-link-in-sidebar`             | `mod+shift+o`                                                                             | Open link in sidebar                                       | Block Editing         |
| `:editor/outdent`                          | `shift+tab`                                                                               | Outdent block                                              | Block Editing         |
| `:editor/zoom-in`                          | Mac: `mod+." "mod+shift+."] "alt+right` / Win/Linux: `; FIXME: mod+. not works on Chrome` | Zoom in editing block / Forwards otherwise                 | Block Editing         |
| `:editor/zoom-out`                         | Mac: `mod+,` / Win/Linux: `alt+left`                                                      | Zoom out editing block / Backwards otherwise               | Block Editing         |
| `:editor/add-property`                     | Mac: `mod+p` / Win/Linux: `ctrl+alt+p`                                                    | Add property                                               | Block Selection       |
| `:editor/add-property-deadline`            | `p d`                                                                                     | Add task deadline to selected block                        | Block Selection       |
| `:editor/add-property-icon`                | `p i`                                                                                     | Add icon                                                   | Block Selection       |
| `:editor/add-property-priority`            | `p p`                                                                                     | Add task priority to selected block                        | Block Selection       |
| `:editor/add-property-status`              | `p s`                                                                                     | Add task status to selected block                          | Block Selection       |
| `:editor/delete-selection`                 | ` `                                                                                       | Delete selected blocks                                     | Block Selection       |
| `:editor/open-edit`                        | `enter`                                                                                   | Edit selected block                                        | Block Selection       |
| `:editor/open-selected-blocks-in-sidebar`  | `shift+enter`                                                                             | Open selected block(s) in sidebar                          | Block Selection       |
| `:editor/select-all-blocks`                | `mod+shift+a`                                                                             | Select all blocks                                          | Block Selection       |
| `:editor/select-block-down`                | `alt+down`                                                                                | Select block below                                         | Block Selection       |
| `:editor/select-block-up`                  | `alt+up`                                                                                  | Select block above                                         | Block Selection       |
| `:editor/select-parent`                    | `mod+a`                                                                                   | Select parent block                                        | Block Selection       |
| `:editor/set-tags`                         | `p t`                                                                                     | Set tags for selected block(s)                             | Block Selection       |
| `:editor/toggle-display-hidden-properties` | `p a`                                                                                     | Toggle display hidden properties                           | Block Selection       |
| `:editor/bold`                             | `mod+b`                                                                                   | Bold                                                       | Formatting            |
| `:editor/highlight`                        | `mod+shift+h`                                                                             | Highlight                                                  | Formatting            |
| `:editor/insert-link`                      | `mod+l`                                                                                   | HTML Link                                                  | Formatting            |
| `:editor/italics`                          | `mod+i`                                                                                   | Italics                                                    | Formatting            |
| `:editor/strike-through`                   | `mod+shift+s`                                                                             | Strikethrough                                              | Formatting            |
| `:editor/collapse-block-children`          | `mod+up`                                                                                  | Collapse                                                   | Navigating            |
| `:editor/down`                             | ` `                                                                                       | Move cursor down / Select down                             | Navigating            |
| `:editor/expand-block-children`            | `mod+down`                                                                                | Expand                                                     | Navigating            |
| `:editor/jump`                             | `mod+j`                                                                                   | Jump to a property key or value                            | Navigating            |
| `:editor/left`                             | `left`                                                                                    | Move cursor left / Open selected block at beginning        | Navigating            |
| `:editor/right`                            | `right`                                                                                   | Move cursor right / Open selected block at end             | Navigating            |
| `:editor/toggle-block-children`            | `mod+;`                                                                                   | Toggle expand/collapse                                     | Navigating            |
| `:editor/up`                               | ` `                                                                                       | Move cursor up / Select up                                 | Navigating            |
| `:go/all-graphs`                           | `g shift+g`                                                                               | Go to all graphs                                           | Navigating            |
| `:go/all-pages`                            | `g a`                                                                                     | Go to all pages                                            | Navigating            |
| `:go/backward`                             | `mod+open-square-bracket`                                                                 | Backwards                                                  | Navigating            |
| `:go/flashcards`                           | ` `                                                                                       | Toggle flashcards                                          | Navigating            |
| `:go/forward`                              | `mod+close-square-bracket`                                                                | Forwards                                                   | Navigating            |
| `:go/graph-view`                           | `g g`                                                                                     | Go to graph view                                           | Navigating            |
| `:go/home`                                 | `g h`                                                                                     | Go to home                                                 | Navigating            |
| `:go/journals`                             | `g j`                                                                                     | Go to journals                                             | Navigating            |
| `:go/keyboard-shortcuts`                   | `g s`                                                                                     | Go to keyboard shortcuts                                   | Navigating            |
| `:go/next-journal`                         | `g n`                                                                                     | Go to next journal                                         | Navigating            |
| `:go/prev-journal`                         | `g p`                                                                                     | Go to previous journal                                     | Navigating            |
| `:go/tomorrow`                             | `g t`                                                                                     | Go to tomorrow                                             | Navigating            |
| `:auto-complete/complete`                  | `enter`                                                                                   | Auto-complete: Choose selected item                        | Others                |
| `:auto-complete/meta-complete`             | `mod+enter`                                                                               | Auto-complete: Cmd + Enter to choose selected item         | Others                |
| `:auto-complete/next`                      | ` `                                                                                       | Auto-complete: Select next item                            | Others                |
| `:auto-complete/prev`                      | ` `                                                                                       | Auto-complete: Select previous item                        | Others                |
| `:auto-complete/shift-complete`            | `shift+enter`                                                                             | Auto-complete: Open selected item in sidebar               | Others                |
| `:cards/again`                             | `1`                                                                                       | Cards: again                                               | Others                |
| `:cards/easy`                              | `4`                                                                                       | Cards: easy                                                | Others                |
| `:cards/good`                              | `3`                                                                                       | Cards: good                                                | Others                |
| `:cards/hard`                              | `2`                                                                                       | Cards: hard                                                | Others                |
| `:cards/toggle-answers`                    | `s`                                                                                       | Cards: show/hide answers/clozes                            | Others                |
| `:command/run`                             | `mod+shift+1`                                                                             | Run git command                                            | Others                |
| `:command/toggle-favorite`                 | `mod+shift+f`                                                                             | Add to/remove from favorites                               | Others                |
| `:dev/gc-graph`                            | —                                                                                         | (Dev) Garbage collect graph (remove unused data in SQLite) | Others                |
| `:dev/replace-graph-with-db-file`          | —                                                                                         | (Dev) Replace graph with its db.sqlite file                | Others                |
| `:dev/rtc-start`                           | —                                                                                         | (Dev) RTC Start                                            | Others                |
| `:dev/rtc-stop`                            | —                                                                                         | (Dev) RTC Stop                                             | Others                |
| `:dev/show-block-ast`                      | —                                                                                         | (Dev) Show block AST                                       | Others                |
| `:dev/show-block-data`                     | —                                                                                         | (Dev) Show block data                                      | Others                |
| `:dev/show-page-data`                      | —                                                                                         | (Dev) Show page data                                       | Others                |
| `:dev/validate-db`                         | —                                                                                         | (Dev) Validate current graph                               | Others                |
| `:editor/copy-page-url`                    | —                                                                                         | Copy page url                                              | Others                |
| `:editor/insert-youtube-timestamp`         | `mod+shift+y`                                                                             | Insert youtube timestamp                                   | Others                |
| `:misc/copy`                               | `mod+c`                                                                                   | Copy                                                       | Others                |
| `:misc/export-block-data`                  | —                                                                                         | Export block EDN data                                      | Others                |
| `:misc/export-graph-ontology-data`         | —                                                                                         | Export graph's tags and properties EDN data                | Others                |
| `:misc/export-page-data`                   | —                                                                                         | Export page EDN data                                       | Others                |
| `:misc/import-edn-data`                    | —                                                                                         | Import EDN data                                            | Others                |
| `:pdf/close`                               | `alt+x`                                                                                   | Pdf: Close current pdf doc                                 | Others                |
| `:pdf/find`                                | `alt+f`                                                                                   | Pdf: Search text of current pdf doc                        | Others                |
| `:pdf/next-page`                           | `alt+n`                                                                                   | Pdf: Next page of current pdf doc                          | Others                |
| `:pdf/previous-page`                       | `alt+p`                                                                                   | Pdf: Previous page of current pdf doc                      | Others                |
| `:publish/open-dialog`                     | `mod+m`                                                                                   | Open publish dialog for current page                       | Others                |
| `:search/re-index`                         | `mod+c mod+s`                                                                             | Rebuild search index                                       | Others                |
| `:sidebar/clear`                           | `mod+c mod+c`                                                                             | Clear all in the right sidebar                             | Others                |
| `:sidebar/close-top`                       | `c t`                                                                                     | Closes the top item in the right sidebar                   | Others                |
| `:sidebar/open-today-page`                 | Mac: `mod+shift+j` / Win/Linux: `alt+shift+j`                                             | Open today's page in the right sidebar                     | Others                |
| `:ui/clear-all-notifications`              | —                                                                                         | Clear all notifications                                    | Others                |
| `:ui/goto-plugins`                         | `t p`                                                                                     | Go to plugins dashboard                                    | Others                |
| `:ui/install-plugin-from-github`           | —                                                                                         | Install plugin from GitHub release                         | Others                |
| `:ui/install-plugins-from-file`            | —                                                                                         | Install plugins from plugins.edn                           | Others                |
| `:ui/select-theme-color`                   | `t i`                                                                                     | Select available theme colors                              | Others                |
| `:ui/toggle-document-mode`                 | `t d`                                                                                     | Toggle document mode                                       | Others                |
| `:window/close`                            | `mod+w`                                                                                   | Close window                                               | Others                |
| `:editor/toggle-number-list`               | `t n`                                                                                     | Toggle number list                                         | Toggle                |
| `:editor/toggle-open-blocks`               | `t o`                                                                                     | Toggle open blocks (collapse or expand all blocks)         | Toggle                |
| `:ui/customize-appearance`                 | `c c`                                                                                     | Customize appearance                                       | Toggle                |
| `:ui/highlight-recent-blocks`              | `mod+c mod+r`                                                                             | Toggle highlight recent blocks                             | Toggle                |
| `:ui/toggle-brackets`                      | `t b`                                                                                     | Toggle whether to display brackets                         | Toggle                |
| `:ui/toggle-contents`                      | `alt+shift+c`                                                                             | Toggle Contents in sidebar                                 | Toggle                |
| `:ui/toggle-help`                          | `shift+/`                                                                                 | Toggle help                                                | Toggle                |
| `:ui/toggle-left-sidebar`                  | `t l`                                                                                     | Toggle left sidebar                                        | Toggle                |
| `:ui/toggle-right-sidebar`                 | `t r`                                                                                     | Toggle right sidebar                                       | Toggle                |
| `:ui/toggle-settings`                      | Mac: `t` / Win/Linux: `s" "mod+,"] "t s`                                                  | Toggle settings                                            | Toggle                |
| `:ui/toggle-theme`                         | `t t`                                                                                     | Toggle between dark/light theme                            | Toggle                |
| `:ui/toggle-wide-mode`                     | `t w`                                                                                     | Toggle wide mode                                           | Toggle                |

---

## Summary by Category

### Basics (13 commands)

- **`:command-palette/toggle`** - `mod+shift+p` - Search commands
- **`:editor/copy`** - `mod+c` - Copy (copies either selection, or block reference)
- **`:editor/copy-text`** - `mod+shift+c` - Copy selections as text
- **`:editor/cut`** - `mod+x` - Cut
- **`:editor/quick-add`** - Mac: `mod+e` / Win/Linux: `mod+alt+e` - Quick add
- **`:editor/redo`** - ` ` - Redo
- **`:editor/undo`** - `mod+z` - Undo
- **`:go/electron-find-in-page`** - `mod+f` - Find text in page
- **`:go/electron-jump-to-the-next`** - ` ` - Jump to the next match to your Find bar search
- **`:go/electron-jump-to-the-previous`** - ` ` - Jump to the previous match to your Find bar search
- **`:go/search`** - `mod+k` - Search pages and blocks
- **`:go/search-in-page`** - `mod+shift+k` - Search blocks in page
- **`:go/search-themes`** - Mac: `mod+shift+i` / Win/Linux: `alt+shift+i` - Search themes

### Block Command Editing (14 commands)

- **`:editor/backspace`** - `backspace` - Backspace / Delete backwards
- **`:editor/backward-kill-word`** - `alt+w` - Delete a word backwards
- **`:editor/backward-word`** - Mac: `ctrl+shift+b` / Win/Linux: `alt+b` - Move cursor backward a word
- **`:editor/beginning-of-block`** - `alt+a` - Move cursor to the beginning of a block
- **`:editor/clear-block`** - Mac: `ctrl+l` / Win/Linux: `alt+l` - Delete entire block content
- **`:editor/copy-embed`** - `mod+shift+e` - Copy a block embed pointing to the current block
- **`:editor/end-of-block`** - `alt+e` - Move cursor to the end of a block
- **`:editor/forward-kill-word`** - Mac: `ctrl+w` / Win/Linux: `alt+d` - Delete a word forwards
- **`:editor/forward-word`** - Mac: `ctrl+shift+f` / Win/Linux: `alt+f` - Move cursor forward a word
- **`:editor/kill-line-after`** - `alt+k` - Delete line after cursor position
- **`:editor/kill-line-before`** - Mac: `ctrl+u` / Win/Linux: `alt+u` - Delete line before cursor position
- **`:editor/paste-text-in-one-block-at-point`** - `mod+shift+v` - Paste text into one block at point
- **`:editor/select-down`** - `shift+down` - Select content below
- **`:editor/select-up`** - `shift+up` - Select content above

### Block Editing (14 commands)

- **`:editor/cycle-todo`** - `mod+enter` - Rotate the TODO state
- **`:editor/delete`** - `delete` - Delete / Delete forwards
- **`:editor/escape-editing`** - Escape editing
- **`:editor/follow-link`** - `mod+o` - Follow link under cursor
- **`:editor/indent`** - `tab` - Indent block
- **`:editor/move-block-down`** - Mac: `mod+shift+down` / Win/Linux: `alt+shift+down` - Move block down
- **`:editor/move-block-up`** - Mac: `mod+shift+up` / Win/Linux: `alt+shift+up` - Move block up
- **`:editor/move-blocks`** - `mod+shift+m` - Move blocks to
- **`:editor/new-block`** - `enter` - Create new block
- **`:editor/new-line`** - `shift+enter` - New line in current block
- **`:editor/open-link-in-sidebar`** - `mod+shift+o` - Open link in sidebar
- **`:editor/outdent`** - `shift+tab` - Outdent block
- **`:editor/zoom-in`** - Mac: `mod+." "mod+shift+."] "alt+right` / Win/Linux: `; FIXME: mod+. not works on Chrome` - Zoom in editing block / Forwards otherwise
- **`:editor/zoom-out`** - Mac: `mod+,` / Win/Linux: `alt+left` - Zoom out editing block / Backwards otherwise

### Block Selection (14 commands)

- **`:editor/add-property`** - Mac: `mod+p` / Win/Linux: `ctrl+alt+p` - Add property
- **`:editor/add-property-deadline`** - `p d` - Add task deadline to selected block
- **`:editor/add-property-icon`** - `p i` - Add icon
- **`:editor/add-property-priority`** - `p p` - Add task priority to selected block
- **`:editor/add-property-status`** - `p s` - Add task status to selected block
- **`:editor/delete-selection`** - ` ` - Delete selected blocks
- **`:editor/open-edit`** - `enter` - Edit selected block
- **`:editor/open-selected-blocks-in-sidebar`** - `shift+enter` - Open selected block(s) in sidebar
- **`:editor/select-all-blocks`** - `mod+shift+a` - Select all blocks
- **`:editor/select-block-down`** - `alt+down` - Select block below
- **`:editor/select-block-up`** - `alt+up` - Select block above
- **`:editor/select-parent`** - `mod+a` - Select parent block
- **`:editor/set-tags`** - `p t` - Set tags for selected block(s)
- **`:editor/toggle-display-hidden-properties`** - `p a` - Toggle display hidden properties

### Formatting (5 commands)

- **`:editor/bold`** - `mod+b` - Bold
- **`:editor/highlight`** - `mod+shift+h` - Highlight
- **`:editor/insert-link`** - `mod+l` - HTML Link
- **`:editor/italics`** - `mod+i` - Italics
- **`:editor/strike-through`** - `mod+shift+s` - Strikethrough

### Navigating (20 commands)

- **`:editor/collapse-block-children`** - `mod+up` - Collapse
- **`:editor/down`** - ` ` - Move cursor down / Select down
- **`:editor/expand-block-children`** - `mod+down` - Expand
- **`:editor/jump`** - `mod+j` - Jump to a property key or value
- **`:editor/left`** - `left` - Move cursor left / Open selected block at beginning
- **`:editor/right`** - `right` - Move cursor right / Open selected block at end
- **`:editor/toggle-block-children`** - `mod+;` - Toggle expand/collapse
- **`:editor/up`** - ` ` - Move cursor up / Select up
- **`:go/all-graphs`** - `g shift+g` - Go to all graphs
- **`:go/all-pages`** - `g a` - Go to all pages
- **`:go/backward`** - `mod+open-square-bracket` - Backwards
- **`:go/flashcards`** - ` ` - Toggle flashcards
- **`:go/forward`** - `mod+close-square-bracket` - Forwards
- **`:go/graph-view`** - `g g` - Go to graph view
- **`:go/home`** - `g h` - Go to home
- **`:go/journals`** - `g j` - Go to journals
- **`:go/keyboard-shortcuts`** - `g s` - Go to keyboard shortcuts
- **`:go/next-journal`** - `g n` - Go to next journal
- **`:go/prev-journal`** - `g p` - Go to previous journal
- **`:go/tomorrow`** - `g t` - Go to tomorrow

### Others (43 commands)

- **`:auto-complete/complete`** - `enter` - Auto-complete: Choose selected item
- **`:auto-complete/meta-complete`** - `mod+enter` - Auto-complete: Cmd + Enter to choose selected item
- **`:auto-complete/next`** - ` ` - Auto-complete: Select next item
- **`:auto-complete/prev`** - ` ` - Auto-complete: Select previous item
- **`:auto-complete/shift-complete`** - `shift+enter` - Auto-complete: Open selected item in sidebar
- **`:cards/again`** - `1` - Cards: again
- **`:cards/easy`** - `4` - Cards: easy
- **`:cards/good`** - `3` - Cards: good
- **`:cards/hard`** - `2` - Cards: hard
- **`:cards/toggle-answers`** - `s` - Cards: show/hide answers/clozes
- **`:command/run`** - `mod+shift+1` - Run git command
- **`:command/toggle-favorite`** - `mod+shift+f` - Add to/remove from favorites
- **`:dev/gc-graph`** - (Dev) Garbage collect graph (remove unused data in SQLite)
- **`:dev/replace-graph-with-db-file`** - (Dev) Replace graph with its db.sqlite file
- **`:dev/rtc-start`** - (Dev) RTC Start
- **`:dev/rtc-stop`** - (Dev) RTC Stop
- **`:dev/show-block-ast`** - (Dev) Show block AST
- **`:dev/show-block-data`** - (Dev) Show block data
- **`:dev/show-page-data`** - (Dev) Show page data
- **`:dev/validate-db`** - (Dev) Validate current graph
- **`:editor/copy-page-url`** - Copy page url
- **`:editor/insert-youtube-timestamp`** - `mod+shift+y` - Insert youtube timestamp
- **`:misc/copy`** - `mod+c` - Copy
- **`:misc/export-block-data`** - Export block EDN data
- **`:misc/export-graph-ontology-data`** - Export graph's tags and properties EDN data
- **`:misc/export-page-data`** - Export page EDN data
- **`:misc/import-edn-data`** - Import EDN data
- **`:pdf/close`** - `alt+x` - Pdf: Close current pdf doc
- **`:pdf/find`** - `alt+f` - Pdf: Search text of current pdf doc
- **`:pdf/next-page`** - `alt+n` - Pdf: Next page of current pdf doc
- **`:pdf/previous-page`** - `alt+p` - Pdf: Previous page of current pdf doc
- **`:publish/open-dialog`** - `mod+m` - Open publish dialog for current page
- **`:search/re-index`** - `mod+c mod+s` - Rebuild search index
- **`:sidebar/clear`** - `mod+c mod+c` - Clear all in the right sidebar
- **`:sidebar/close-top`** - `c t` - Closes the top item in the right sidebar
- **`:sidebar/open-today-page`** - Mac: `mod+shift+j` / Win/Linux: `alt+shift+j` - Open today's page in the right sidebar
- **`:ui/clear-all-notifications`** - Clear all notifications
- **`:ui/goto-plugins`** - `t p` - Go to plugins dashboard
- **`:ui/install-plugin-from-github`** - Install plugin from GitHub release
- **`:ui/install-plugins-from-file`** - Install plugins from plugins.edn
- **`:ui/select-theme-color`** - `t i` - Select available theme colors
- **`:ui/toggle-document-mode`** - `t d` - Toggle document mode
- **`:window/close`** - `mod+w` - Close window

### Toggle (12 commands)

- **`:editor/toggle-number-list`** - `t n` - Toggle number list
- **`:editor/toggle-open-blocks`** - `t o` - Toggle open blocks (collapse or expand all blocks)
- **`:ui/customize-appearance`** - `c c` - Customize appearance
- **`:ui/highlight-recent-blocks`** - `mod+c mod+r` - Toggle highlight recent blocks
- **`:ui/toggle-brackets`** - `t b` - Toggle whether to display brackets
- **`:ui/toggle-contents`** - `alt+shift+c` - Toggle Contents in sidebar
- **`:ui/toggle-help`** - `shift+/` - Toggle help
- **`:ui/toggle-left-sidebar`** - `t l` - Toggle left sidebar
- **`:ui/toggle-right-sidebar`** - `t r` - Toggle right sidebar
- **`:ui/toggle-settings`** - Mac: `t` / Win/Linux: `s" "mod+,"] "t s` - Toggle settings
- **`:ui/toggle-theme`** - `t t` - Toggle between dark/light theme
- **`:ui/toggle-wide-mode`** - `t w` - Toggle wide mode
