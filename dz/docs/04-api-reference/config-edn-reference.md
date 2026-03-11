# Logseq config.edn Reference Guide

> **Last revised:** 2026-03-10

A comprehensive reference for all configuration options available in Logseq's `config.edn` file. This file is located in your graph's `logseq/` directory.

> **Note:** Some options are only available for file-based graphs and are marked as such. Database graphs have different behavior for certain settings.

---

## Table of Contents

- [Logseq config.edn Reference Guide](#logseq-configedn-reference-guide)
  - [Table of Contents](#table-of-contents)
  - [1. Metadata](#1-metadata)
  - [2. UI Settings](#2-ui-settings)
  - [3. Feature Toggles](#3-feature-toggles)
  - [4. Editor Settings](#4-editor-settings)
    - [CodeMirror Options](#codemirror-options)
  - [5. Keyboard Shortcuts](#5-keyboard-shortcuts)
    - [Shortcut Syntax](#shortcut-syntax)
  - [6. Home \& Navigation](#6-home--navigation)
    - [`:default-home` Sub-keys](#default-home-sub-keys)
  - [7. Scheduling \& Calendar](#7-scheduling--calendar)
  - [8. Customization (CSS/JS/Macros)](#8-customization-cssjsmacros)
    - [Macro Placeholders](#macro-placeholders)
  - [9. Export Settings](#9-export-settings)
    - [Available Values](#available-values)
  - [10. Publishing Settings](#10-publishing-settings)
  - [11. Advanced Queries](#11-advanced-queries)
  - [12. Linked References](#12-linked-references)
  - [13. Outliner Settings](#13-outliner-settings)
  - [14. Graph View](#14-graph-view)
    - [`:graph/settings` Sub-keys](#graphsettings-sub-keys)
    - [`:graph/forcesettings` Sub-keys](#graphforcesettings-sub-keys)
  - [15. Mobile Settings](#15-mobile-settings)
    - [`:mobile/photo` Sub-keys](#mobilephoto-sub-keys)
    - [`:mobile` Sub-keys](#mobile-sub-keys)
  - [16. Quick Capture (Mobile)](#16-quick-capture-mobile)
    - [`:quick-capture-templates` Sub-keys](#quick-capture-templates-sub-keys)
    - [Template Placeholders](#template-placeholders)
    - [`:quick-capture-options` Sub-keys](#quick-capture-options-sub-keys)
  - [17. DWIM Settings (Do What I Mean)](#17-dwim-settings-do-what-i-mean)
    - [`:dwim/settings` Sub-keys](#dwimsettings-sub-keys)
  - [18. File-Graph Only Options](#18-file-graph-only-options)
    - [Logbook Settings (File-Graph Only)](#logbook-settings-file-graph-only)
      - [`:logbook/settings` Sub-keys](#logbooksettings-sub-keys)
  - [19. Spaced Repetition (SRS) - File Graphs Only](#19-spaced-repetition-srs---file-graphs-only)
  - [20. Deprecated Options](#20-deprecated-options)
  - [21. Example Configuration](#21-example-configuration)
  - [Source References](#source-references)

---

## 1. Metadata

| Key             | Type  | Default | Description                       |
| --------------- | ----- | ------- | --------------------------------- |
| `:meta/version` | `int` | `1`     | Configuration file version number |

**Example:**

```clojure
{:meta/version 1}
```

---

## 2. UI Settings

These settings control the visual appearance and behavior of the Logseq interface.

| Key                           | Type      | Default | Description                                                    |
| ----------------------------- | --------- | ------- | -------------------------------------------------------------- |
| `:ui/enable-tooltip?`         | `boolean` | `true`  | Enable tooltip preview when hovering over elements             |
| `:ui/show-brackets?`          | `boolean` | `true`  | Display brackets `[[]]` around page references                 |
| `:ui/show-full-blocks?`       | `boolean` | `false` | Display all lines of a block when referencing with `((block))` |
| `:ui/auto-expand-block-refs?` | `boolean` | `true`  | Automatically expand block references when zooming in          |
| `:ui/show-command-doc?`       | `boolean` | `true`  | Display command documentation when hovering over commands      |
| `:ui/show-empty-bullets?`     | `boolean` | `false` | Display empty bullet points                                    |
| `:ui/hide-empty-properties?`  | `boolean` | `false` | Hide empty block properties                                    |

**Example:**

```clojure
{:ui/enable-tooltip? true
 :ui/show-brackets? true
 :ui/show-full-blocks? false
 :ui/auto-expand-block-refs? true
 :ui/show-command-doc? true
 :ui/show-empty-bullets? false
 :ui/hide-empty-properties? false}
```

---

## 3. Feature Toggles

Enable or disable major Logseq features.

| Key                                              | Type      | Default | Description                                                                                                 |
| ------------------------------------------------ | --------- | ------- | ----------------------------------------------------------------------------------------------------------- |
| `:feature/enable-search-remove-accents?`         | `boolean` | `true`  | Ignore accent marks when searching. **Note:** After changing, rebuild the search index with `Ctrl+C Ctrl+S` |
| `:feature/enable-journals?`                      | `boolean` | `true`  | Enable the journals feature                                                                                 |
| `:feature/enable-flashcards?`                    | `boolean` | `true`  | Enable flashcards and spaced repetition                                                                     |
| `:feature/disable-scheduled-and-deadline-query?` | `boolean` | `false` | Disable the built-in "Scheduled tasks and deadlines" query on journal pages                                 |
| `:feature/enable-grammarly?`                     | `boolean` | `false` | Enable Grammarly integration in the editor                                                                  |

**Example:**

```clojure
{:feature/enable-search-remove-accents? true
 :feature/enable-journals? true
 :feature/enable-flashcards? true
 :feature/disable-scheduled-and-deadline-query? false}
```

---

## 4. Editor Settings

Configure editor behavior and limits.

| Key                                | Type      | Default | Description                                                                                                             |
| ---------------------------------- | --------- | ------- | ----------------------------------------------------------------------------------------------------------------------- |
| `:block/title-max-length`          | `int`     | `10000` | Maximum block content length for searchability and editing. Blocks exceeding this limit won't be searchable or editable |
| `:editor/logical-outdenting?`      | `boolean` | `false` | Enable logical outdenting behavior (outdent moves to parent level instead of root)                                      |
| `:editor/preferred-pasting-file?`  | `boolean` | `false` | When both text and a file are in the clipboard, prefer pasting the file                                                 |
| `:editor/extra-codemirror-options` | `map`     | `{}`    | Additional CodeMirror editor options. See [CodeMirror documentation](https://codemirror.net/5/doc/manual.html#config)   |

### CodeMirror Options

| Sub-key         | Type      | Default | Description                         |
| --------------- | --------- | ------- | ----------------------------------- |
| `:lineWrapping` | `boolean` | `false` | Enable line wrapping in code blocks |
| `:lineNumbers`  | `boolean` | `true`  | Show line numbers in code blocks    |
| `:readOnly`     | `boolean` | `false` | Make code blocks read-only          |

**Example:**

```clojure
{:block/title-max-length 10000
 :editor/logical-outdenting? false
 :editor/preferred-pasting-file? false
 :editor/extra-codemirror-options {:lineWrapping false
                                   :lineNumbers true
                                   :readOnly false}}
```

---

## 5. Keyboard Shortcuts

Customize keyboard shortcuts for commands.

| Key                                       | Type      | Default | Description                                                                  |
| ----------------------------------------- | --------- | ------- | ---------------------------------------------------------------------------- |
| `:shortcuts`                              | `map`     | `{}`    | Map of command keywords to key bindings                                      |
| `:shortcut/doc-mode-enter-for-new-block?` | `boolean` | `false` | In document mode, pressing Enter creates a new block (instead of a new line) |

### Shortcut Syntax

- `+` indicates simultaneous key presses: `Ctrl+Shift+a`
- Space between keys represents key chords: `t s` means pressing `t` followed by `s`
- `mod` refers to `Ctrl` on Windows/Linux and `Command` on Mac
- Use `false` to disable a specific shortcut
- Use a vector for multiple bindings: `["ctrl+j" "down"]`

**Full list of configurable shortcuts:** [shortcut/config.cljs](https://github.com/logseq/logseq/blob/master/src/main/frontend/modules/shortcut/config.cljs)

**Example:**

```clojure
{:shortcuts
 {:editor/new-block       "enter"
  :editor/new-line        "shift+enter"
  :editor/insert-link     "mod+shift+k"
  :editor/highlight       false           ; disable this shortcut
  :ui/toggle-settings     "t s"           ; key chord
  :editor/up              ["ctrl+k" "up"] ; multiple bindings
  :editor/down            ["ctrl+j" "down"]
  :editor/left            ["ctrl+h" "left"]
  :editor/right           ["ctrl+l" "right"]}
 :shortcut/doc-mode-enter-for-new-block? false}
```

---

## 6. Home & Navigation

Configure the default home page and sidebar behavior.

| Key             | Type  | Default | Description                                                                          |
| --------------- | ----- | ------- | ------------------------------------------------------------------------------------ |
| `:default-home` | `map` | `nil`   | Configure the default home page and sidebar. If unset, journal page loads on startup |

### `:default-home` Sub-keys

| Sub-key    | Type                         | Description                                        |
| ---------- | ---------------------------- | -------------------------------------------------- |
| `:page`    | `string`                     | Page name to load on startup                       |
| `:sidebar` | `string` or `vector<string>` | Page(s) to display in the right sidebar on startup |

**Example:**

```clojure
;; Set "Changelog" as home page with "Contents" in sidebar
{:default-home {:page "Changelog" :sidebar "Contents"}}

;; Set home page without sidebar
{:default-home {:page "Jun 3rd, 2021"}}

;; Multiple pages in sidebar
{:default-home {:page "home" :sidebar ["Page A" "Page B" "Contents"]}}
```

---

## 7. Scheduling & Calendar

Configure date and scheduling behavior.

| Key                      | Type  | Default | Description                                                                   |
| ------------------------ | ----- | ------- | ----------------------------------------------------------------------------- |
| `:scheduled/future-days` | `int` | `7`     | Number of future days to display in the "Scheduled tasks and deadlines" query |
| `:start-of-week`         | `int` | `6`     | First day of the week. `0` = Monday, `6` = Sunday                             |

**Example:**

```clojure
{:scheduled/future-days 14  ; Show 2 weeks of scheduled items
 :start-of-week 0}          ; Start week on Monday
```

---

## 8. Customization (CSS/JS/Macros)

Add custom styling, scripts, and text macros.

| Key               | Type     | Default | Description                                                               |
| ----------------- | -------- | ------- | ------------------------------------------------------------------------- |
| `:custom-css-url` | `string` | `nil`   | URL to import custom CSS. Takes precedence over `logseq/custom.css`       |
| `:custom-js-url`  | `string` | `nil`   | URL to import custom JavaScript. Takes precedence over `logseq/custom.js` |
| `:macros`         | `map`    | `{}`    | Text replacement macros with placeholder support                          |
| `:commands`       | `vector` | `[]`    | Custom commands for the command palette                                   |

### Macro Placeholders

- `$1`, `$2`, etc. are positional placeholders
- Use with `{{macro-name arg1,arg2}}`

**Example:**

```clojure
{:custom-css-url "@import url('https://cdn.jsdelivr.net/gh/dracula/logseq@master/custom.css');"
 :custom-js-url "https://cdn.logseq.com/custom.js"

 :macros {"poem" "Rose is $1, violet's $2. Life's ordered: Org assists you."
          "docs" "https://docs.logseq.com/#/page/$1"
          "yt" "{{video https://www.youtube.com/watch?v=$1}}"}

 :commands [["js" "Javascript"]
            ["md" "Markdown"]
            ["today" "{{date}}"]]}
```

**Usage:** `{{poem red,blue}}` expands to "Rose is red, violet's blue. Life's ordered: Org assists you."

---

## 9. Export Settings

Configure export behavior.

| Key                          | Type      | Default | Description                             |
| ---------------------------- | --------- | ------- | --------------------------------------- |
| `:export/bullet-indentation` | `keyword` | `:tab`  | Bullet indentation style when exporting |

### Available Values

| Value           | Description             |
| --------------- | ----------------------- |
| `:tab`          | Tab character (default) |
| `:two-spaces`   | Two spaces              |
| `:four-spaces`  | Four spaces             |
| `:eight-spaces` | Eight spaces            |

**Example:**

```clojure
{:export/bullet-indentation :four-spaces}
```

---

## 10. Publishing Settings

Configure Logseq publishing behavior.

| Key                             | Type      | Default | Description                                                           |
| ------------------------------- | --------- | ------- | --------------------------------------------------------------------- |
| `:publishing/all-pages-public?` | `boolean` | `false` | Publish all pages regardless of individual `public` property settings |

**Example:**

```clojure
{:publishing/all-pages-public? true}
```

---

## 11. Advanced Queries

Configure advanced query behavior and transformations.

| Key                        | Type  | Default              | Description                                               |
| -------------------------- | ----- | -------------------- | --------------------------------------------------------- |
| `:query/views`             | `map` | `{:pprint (fn ...)}` | Pre-defined `:view` functions for use in advanced queries |
| `:query/result-transforms` | `map` | `{}`                 | Transform functions for query results                     |
| `:default-queries`         | `map` | `nil`                | Queries displayed at the bottom of journal pages          |

**Example:**

```clojure
{:query/views
 {:pprint (fn [r] [:pre.code (pprint r)])
  :table (fn [r] [:table ...])}

 :query/result-transforms
 {:sort-by-priority
  (fn [result] (sort-by (fn [h] (get h :logseq.property/priority "Z")) result))}

 :default-queries
 {:journals
  [{:title "📅 Scheduled"
    :query [:find (pull ?b [*])
            :where
            [?b :block/scheduled ?d]
            [(>= ?d ?start)]
            [(<= ?d ?next)]]
    :inputs [:today :7d-after]}]}}
```

---

## 12. Linked References

Configure how linked references are displayed.

| Key                              | Type  | Default | Description                                                    |
| -------------------------------- | ----- | ------- | -------------------------------------------------------------- |
| `:ref/default-open-blocks-level` | `int` | `2`     | Default expansion level for linked references (min: 0, max: 9) |

**Example:**

```clojure
;; Block hierarchy example:
;; - a [[page]] (level 1)
;;   - b        (level 2)
;;     - c      (level 3)
;;       - d    (level 4)

{:ref/default-open-blocks-level 2}  ; Block b is collapsed
{:ref/default-open-blocks-level 3}  ; Block c is collapsed
```

---

## 13. Outliner Settings

Configure outliner behavior.

| Key                                       | Type      | Default | Description                                                                                                 |
| ----------------------------------------- | --------- | ------- | ----------------------------------------------------------------------------------------------------------- |
| `:outliner/block-title-collapse-enabled?` | `boolean` | `false` | Allow collapsing blocks with titles but no children. By default, only blocks with children can be collapsed |

**Example:**

```clojure
;; With this enabled, blocks like this can be collapsed:
;; - block title
;;   block content (continuation, not a child)

{:outliner/block-title-collapse-enabled? true}
```

---

## 14. Graph View

Configure the graph visualization.

| Key                    | Type  | Default | Description                             |
| ---------------------- | ----- | ------- | --------------------------------------- |
| `:graph/settings`      | `map` | `nil`   | Graph view display settings             |
| `:graph/forcesettings` | `map` | `nil`   | Graph physics/force simulation settings |

### `:graph/settings` Sub-keys

| Sub-key            | Type      | Default | Description                |
| ------------------ | --------- | ------- | -------------------------- |
| `:orphan-pages?`   | `boolean` | `true`  | Show pages with no links   |
| `:builtin-pages?`  | `boolean` | `false` | Show built-in system pages |
| `:excluded-pages?` | `boolean` | `false` | Show excluded pages        |
| `:journal?`        | `boolean` | `false` | Show journal pages         |

### `:graph/forcesettings` Sub-keys

| Sub-key            | Type  | Default | Description                                      |
| ------------------ | ----- | ------- | ------------------------------------------------ |
| `:link-dist`       | `int` | `180`   | Distance between linked nodes                    |
| `:charge-strength` | `int` | `-600`  | Repulsion force between nodes (negative = repel) |
| `:charge-range`    | `int` | `600`   | Maximum distance for charge force                |

**Example:**

```clojure
{:graph/settings {:orphan-pages? true
                  :builtin-pages? false
                  :excluded-pages? false
                  :journal? false}

 :graph/forcesettings {:link-dist 180
                       :charge-strength -600
                       :charge-range 600}}
```

---

## 15. Mobile Settings

Configure mobile-specific behavior.

| Key             | Type  | Default | Description                |
| --------------- | ----- | ------- | -------------------------- |
| `:mobile/photo` | `map` | `nil`   | Photo upload configuration |
| `:mobile`       | `map` | `nil`   | General mobile settings    |

### `:mobile/photo` Sub-keys

| Sub-key           | Type      | Default | Description                                 |
| ----------------- | --------- | ------- | ------------------------------------------- |
| `:allow-editing?` | `boolean` | `true`  | Allow editing/cropping photos before upload |
| `:quality`        | `int`     | `80`    | JPEG quality for uploaded photos (0-100)    |

### `:mobile` Sub-keys

| Sub-key                                 | Type             | Description                                         |
| --------------------------------------- | ---------------- | --------------------------------------------------- |
| `:gestures/disabled-in-block-with-tags` | `vector<string>` | Disable swipe gestures in blocks with specific tags |

**Example:**

```clojure
{:mobile/photo {:allow-editing? true
                :quality 80}

 :mobile {:gestures/disabled-in-block-with-tags ["kanban" "table"]}}
```

---

## 16. Quick Capture (Mobile)

Configure quick capture behavior for receiving content from other apps.

| Key                        | Type  | Default | Description                         |
| -------------------------- | ----- | ------- | ----------------------------------- |
| `:quick-capture-templates` | `map` | `nil`   | Templates for quick capture content |
| `:quick-capture-options`   | `map` | `nil`   | Quick capture behavior settings     |

### `:quick-capture-templates` Sub-keys

| Sub-key  | Type     | Description                |
| -------- | -------- | -------------------------- |
| `:text`  | `string` | Template for text content  |
| `:media` | `string` | Template for media content |

### Template Placeholders

| Placeholder | Description                                        |
| ----------- | -------------------------------------------------- |
| `{time}`    | Capture time                                       |
| `{date}`    | Capture date (use `[[{date}]]` for page reference) |
| `{text}`    | Selected text before sharing                       |
| `{url}`     | URL or asset path                                  |

### `:quick-capture-options` Sub-keys

| Sub-key           | Type      | Default           | Description                              |
| ----------------- | --------- | ----------------- | ---------------------------------------- |
| `:insert-today?`  | `boolean` | `true`            | Insert at end of today's journal         |
| `:redirect-page?` | `boolean` | `false`           | Redirect to capture page after capturing |
| `:default-page`   | `string`  | `"quick capture"` | Default page if `insert-today?` is false |

**Example:**

```clojure
{:quick-capture-templates
 {:text "[[quick capture]] **{time}**: {text} from {url}"
  :media "[[quick capture]] **{time}**: {url}"}

 :quick-capture-options
 {:insert-today? true
  :redirect-page? false
  :default-page "inbox"}}
```

---

## 17. DWIM Settings (Do What I Mean)

Configure context-aware Enter key behavior. When enabled, pressing Enter has different outcomes based on context (e.g., creating a new list item in a list, opening a block reference).

| Key              | Type  | Default | Description                 |
| ---------------- | ----- | ------- | --------------------------- |
| `:dwim/settings` | `map` | `nil`   | DWIM behavior configuration |

### `:dwim/settings` Sub-keys

| Sub-key            | Type      | Default | Description                                               |
| ------------------ | --------- | ------- | --------------------------------------------------------- |
| `:admonition&src?` | `boolean` | `true`  | Enable DWIM in admonitions and source blocks              |
| `:markup?`         | `boolean` | `false` | Enable DWIM for markup formatting                         |
| `:block-ref?`      | `boolean` | `true`  | Enable DWIM for block references (opens referenced block) |
| `:page-ref?`       | `boolean` | `true`  | Enable DWIM for page references                           |
| `:properties?`     | `boolean` | `true`  | Enable DWIM for properties                                |
| `:list?`           | `boolean` | `false` | Enable DWIM for lists (creates new list item)             |

**Example:**

```clojure
{:dwim/settings {:admonition&src? true
                 :markup? false
                 :block-ref? true
                 :page-ref? true
                 :properties? true
                 :list? false}}
```

---

## 18. File-Graph Only Options

> ⚠️ **Warning:** These options are **only for file-based graphs** and have no effect in database graphs.

| Key                                 | Type             | Default          | Description                                                                  |
| ----------------------------------- | ---------------- | ---------------- | ---------------------------------------------------------------------------- |
| `:preferred-format`                 | `keyword`        | `:markdown`      | File format: `:markdown` or `:org`                                           |
| `:preferred-workflow`               | `keyword`        | `:now`           | Task workflow: `:now` (NOW/LATER) or `:todo` (TODO/DOING)                    |
| `:pages-directory`                  | `string`         | `"pages"`        | Directory for page files                                                     |
| `:journals-directory`               | `string`         | `"journals"`     | Directory for journal files                                                  |
| `:whiteboards-directory`            | `string`         | `"whiteboards"`  | Directory for whiteboard files                                               |
| `:journal/page-title-format`        | `string`         | `"MMM do, yyyy"` | Date format for journal page titles                                          |
| `:journal/file-name-format`         | `string`         | `"yyyy_MM_dd"`   | Date format for journal filenames                                            |
| `:hidden`                           | `vector<string>` | `[]`             | Directories/files to exclude from indexing                                   |
| `:favorites`                        | `vector<string>` | `nil`            | Pages to show in left sidebar favorites                                      |
| `:default-templates`                | `map`            | `{:journals ""}` | Default templates for journals                                               |
| `:file/name-format`                 | `keyword`        | `:triple-lowbar` | Escape method for special characters in filenames                            |
| `:file-sync/ignore-files`           | `vector<string>` | `[]`             | Files to ignore during sync (regex supported)                                |
| `:org-mode/insert-file-link?`       | `boolean`        | `false`          | Convert `[[Page]]` to `[[file:./page.org][Page]]` for org-mode compatibility |
| `:property-pages/enabled?`          | `boolean`        | `true`           | Create pages for all properties                                              |
| `:property-pages/excludelist`       | `set<keyword>`   | `nil`            | Properties to exclude from having pages                                      |
| `:property/separated-by-commas`     | `set<keyword>`   | `nil`            | Properties where comma-separated values become page refs                     |
| `:ignored-page-references-keywords` | `set<keyword>`   | `nil`            | Properties ignored when parsing for references                               |
| `:block-hidden-properties`          | `set<keyword>`   | `nil`            | Block properties to hide in the UI                                           |
| `:feature/enable-whiteboards?`      | `boolean`        | `true`           | Enable whiteboards feature                                                   |
| `:feature/enable-block-timestamps?` | `boolean`        | `false`          | Enable block timestamps (always enabled in DB graphs)                        |

### Logbook Settings (File-Graph Only)

| Key                 | Type  | Default | Description                         |
| ------------------- | ----- | ------- | ----------------------------------- |
| `:logbook/settings` | `map` | `nil`   | Time tracking/logbook configuration |

#### `:logbook/settings` Sub-keys

| Sub-key                          | Type      | Default | Description                           |
| -------------------------------- | --------- | ------- | ------------------------------------- |
| `:with-second-support?`          | `boolean` | `true`  | Include seconds in logbook timestamps |
| `:enabled-in-all-blocks`         | `boolean` | `false` | Display logbook in all blocks         |
| `:enabled-in-timestamped-blocks` | `boolean` | `false` | Display logbook in timestamped blocks |

**Example (File-Graph Only):**

```clojure
{:preferred-format :markdown
 :preferred-workflow :todo

 :pages-directory "pages"
 :journals-directory "journals"
 :whiteboards-directory "whiteboards"

 :journal/page-title-format "MMM do, yyyy"
 :journal/file-name-format "yyyy_MM_dd"

 :hidden ["/archived" "/test.md" ".git"]

 :favorites ["Quick Notes" "Projects" "Reading List"]

 :default-templates {:journals "- [[Daily Review]]"}

 :file/name-format :triple-lowbar
 :file-sync/ignore-files ["*.tmp" "*.bak"]

 :property-pages/enabled? true
 :property-pages/excludelist #{:duration :rating}

 :property/separated-by-commas #{:alias :tags :authors}

 :block-hidden-properties #{:public :icon}

 :logbook/settings {:with-second-support? false
                    :enabled-in-all-blocks true}}
```

---

## 19. Spaced Repetition (SRS) - File Graphs Only

Configure the spaced repetition algorithm for flashcards.

| Key                      | Type    | Default | Description                                                                   |
| ------------------------ | ------- | ------- | ----------------------------------------------------------------------------- |
| `:srs/learning-fraction` | `float` | `0.5`   | How fast the interval changes after review (0-1). Higher = faster progression |
| `:srs/initial-interval`  | `int`   | `4`     | Initial interval (in days) after the first successful review                  |

**Example:**

```clojure
{:srs/learning-fraction 0.5
 :srs/initial-interval 4}
```

---

## 20. Deprecated Options

These options are deprecated and may be removed in future versions:

| Deprecated Key              | Replacement                     | Notes                        |
| --------------------------- | ------------------------------- | ---------------------------- |
| `:editor/command-trigger`   | N/A                             | No longer supported; use `/` |
| `:arweave/gateway`          | N/A                             | No longer supported          |
| `:all-pages-public?`        | `:publishing/all-pages-public?` | Use the new key              |
| `:block/content-max-length` | `:block/title-max-length`       | Use the new key              |

---

## 21. Example Configuration

Here's a complete example `config.edn` with commonly used settings:

```clojure
{:meta/version 1

 ;; === UI Settings ===
 :ui/enable-tooltip? true
 :ui/show-brackets? true
 :ui/show-full-blocks? false
 :ui/auto-expand-block-refs? true
 :ui/show-command-doc? true
 :ui/show-empty-bullets? false
 :ui/hide-empty-properties? false

 ;; === Features ===
 :feature/enable-search-remove-accents? true
 :feature/enable-journals? true
 :feature/enable-flashcards? true
 :feature/disable-scheduled-and-deadline-query? false

 ;; === Editor ===
 :block/title-max-length 10000
 :editor/logical-outdenting? false
 :editor/preferred-pasting-file? false
 :editor/extra-codemirror-options {:lineWrapping false
                                   :lineNumbers true}

 ;; === Shortcuts ===
 :shortcuts {:editor/new-block "enter"
             :editor/new-line "shift+enter"
             :ui/toggle-settings "t s"}
 :shortcut/doc-mode-enter-for-new-block? false

 ;; === Home & Navigation ===
 :default-home {:page "Dashboard" :sidebar "Contents"}

 ;; === Calendar ===
 :start-of-week 0  ; Monday
 :scheduled/future-days 14

 ;; === Macros ===
 :macros {"docs" "https://docs.logseq.com/#/page/$1"
          "yt" "{{video https://www.youtube.com/watch?v=$1}}"}

 ;; === Commands ===
 :commands [["js" "Javascript"]
            ["py" "Python"]]

 ;; === References ===
 :ref/default-open-blocks-level 2

 ;; === Outliner ===
 :outliner/block-title-collapse-enabled? false

 ;; === Graph View ===
 :graph/settings {:orphan-pages? true
                  :builtin-pages? false
                  :journal? false}
 :graph/forcesettings {:link-dist 180
                       :charge-strength -600
                       :charge-range 600}

 ;; === Query Views ===
 :query/views {:pprint (fn [r] [:pre.code (pprint r)])}

 ;; === Export ===
 :export/bullet-indentation :tab

 ;; === Publishing ===
 :publishing/all-pages-public? false

 ;; === Mobile ===
 :mobile/photo {:allow-editing? true
                :quality 80}
 :quick-capture-options {:insert-today? true
                         :redirect-page? false}

 ;; === DWIM ===
 :dwim/settings {:admonition&src? true
                 :block-ref? true
                 :page-ref? true
                 :properties? true
                 :list? false}}
```

---

## Source References

This documentation was compiled from:

- [templates/config.edn](../deps/common/resources/templates/config.edn) - Default configuration template
- [common_config.cljc](../src/main/frontend/schema/handler/common_config.cljc) - Malli schema validation
- [config.cljs](../deps/common/src/logseq/common/config.cljs) - Config definitions and file-only keys
- [state.cljs](../src/main/frontend/state.cljs) - Config getters and defaults

---

_Last updated: March 2026_
