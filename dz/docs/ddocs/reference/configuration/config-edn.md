---
title: 'config.edn Reference'
description: "Complete reference for Logseq's config.edn configuration file"
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
sinceVersion: '0.1.0'
relatedDocs:
  tutorials:
    - '[Installation Guide](../../tutorials/getting-started/01-installation.md)'
  howTo:
    - '[Custom CSS](../../how-to/integrations/custom-css.md)'
    - '[Custom Themes](../../how-to/integrations/create-theme.md)'
  explanation:
    - '[File vs DB Graphs](../../explanation/concepts/file-vs-db.md)'
---

# config.edn Reference

> Complete reference for all configuration options in Logseq's `config.edn` file. For DB-specific configuration, see [config-edn-db.md](config-edn-db.md).

## Overview

The `config.edn` file controls Logseq's behavior and appearance. It's located in your graph's `logseq/` directory.

**Location:**

- **File-based graphs**: `<your-graph-folder>/logseq/config.edn`
- **Database graphs**: `~/.logseq/graphs/<graph-name>/logseq/config.edn`

## Quick Reference

| Category   | Key Options                                                | Description                 |
| ---------- | ---------------------------------------------------------- | --------------------------- |
| UI         | `:ui/show-brackets?`, `:ui/enable-tooltip?`                | Visual appearance           |
| Features   | `:feature/enable-journals?`, `:feature/enable-flashcards?` | Enable/disable features     |
| Editor     | `:editor/logical-outdenting?`, `:block/title-max-length`   | Editor behavior             |
| Shortcuts  | `:shortcuts`                                               | Custom keyboard shortcuts   |
| Navigation | `:default-home`, `:start-of-week`                          | Home page and calendar      |
| Macros     | `:macros`, `:commands`                                     | Text expansion and commands |

---

## 1. Metadata

| Key             | Type  | Default | Description                       |
| --------------- | ----- | ------- | --------------------------------- |
| `:meta/version` | `int` | `1`     | Configuration file version number |

```clojure
{:meta/version 1}
```

---

## 2. UI Settings

Control the visual appearance and behavior of the Logseq interface.

| Key                           | Type      | Default | Description                                         |
| ----------------------------- | --------- | ------- | --------------------------------------------------- |
| `:ui/enable-tooltip?`         | `boolean` | `true`  | Enable tooltip preview on hover                     |
| `:ui/show-brackets?`          | `boolean` | `true`  | Display `[[]]` around page references               |
| `:ui/show-full-blocks?`       | `boolean` | `false` | Display all lines when referencing with `((block))` |
| `:ui/auto-expand-block-refs?` | `boolean` | `true`  | Auto-expand block references when zooming           |
| `:ui/show-command-doc?`       | `boolean` | `true`  | Display command documentation on hover              |
| `:ui/show-empty-bullets?`     | `boolean` | `false` | Display empty bullet points                         |
| `:ui/hide-empty-properties?`  | `boolean` | `false` | Hide empty block properties                         |

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

| Key                                              | Type      | Default | Description                                                                   |
| ------------------------------------------------ | --------- | ------- | ----------------------------------------------------------------------------- |
| `:feature/enable-search-remove-accents?`         | `boolean` | `true`  | Ignore accents when searching. Rebuild index after changing (`Ctrl+C Ctrl+S`) |
| `:feature/enable-journals?`                      | `boolean` | `true`  | Enable journals feature                                                       |
| `:feature/enable-flashcards?`                    | `boolean` | `true`  | Enable flashcards/spaced repetition                                           |
| `:feature/disable-scheduled-and-deadline-query?` | `boolean` | `false` | Disable built-in task query on journal pages                                  |
| `:feature/enable-grammarly?`                     | `boolean` | `false` | Enable Grammarly integration                                                  |

```clojure
{:feature/enable-search-remove-accents? true
 :feature/enable-journals? true
 :feature/enable-flashcards? true
 :feature/disable-scheduled-and-deadline-query? false}
```

---

## 4. Editor Settings

Configure editor behavior and limits.

| Key                                | Type      | Default | Description                                    |
| ---------------------------------- | --------- | ------- | ---------------------------------------------- |
| `:block/title-max-length`          | `int`     | `10000` | Maximum block content length for searchability |
| `:editor/logical-outdenting?`      | `boolean` | `false` | Outdent moves to parent level instead of root  |
| `:editor/preferred-pasting-file?`  | `boolean` | `false` | Prefer pasting file over text from clipboard   |
| `:editor/extra-codemirror-options` | `map`     | `{}`    | Additional CodeMirror editor options           |

### CodeMirror Options

| Sub-key         | Type      | Default | Description                         |
| --------------- | --------- | ------- | ----------------------------------- |
| `:lineWrapping` | `boolean` | `false` | Enable line wrapping in code blocks |
| `:lineNumbers`  | `boolean` | `true`  | Show line numbers                   |
| `:readOnly`     | `boolean` | `false` | Make code blocks read-only          |

```clojure
{:block/title-max-length 10000
 :editor/logical-outdenting? false
 :editor/preferred-pasting-file? false
 :editor/extra-codemirror-options {:lineWrapping false
                                   :lineNumbers true}}
```

---

## 5. Keyboard Shortcuts

Customize keyboard shortcuts for commands.

| Key                                       | Type      | Default | Description                              |
| ----------------------------------------- | --------- | ------- | ---------------------------------------- |
| `:shortcuts`                              | `map`     | `{}`    | Map of command keywords to key bindings  |
| `:shortcut/doc-mode-enter-for-new-block?` | `boolean` | `false` | Enter creates new block in document mode |

### Shortcut Syntax

- `+` = simultaneous keys: `Ctrl+Shift+a`
- Space = key chord: `t s` (press `t`, then `s`)
- `mod` = `Cmd` on Mac, `Ctrl` on Windows/Linux
- `false` = disable shortcut
- Vector = multiple bindings: `["ctrl+j" "down"]`

**Full list**: [shortcut/config.cljs](https://github.com/logseq/logseq/blob/master/src/main/frontend/modules/shortcut/config.cljs)

```clojure
{:shortcuts
 {:editor/new-block       "enter"
  :editor/new-line        "shift+enter"
  :editor/insert-link     "mod+shift+k"
  :editor/highlight       false           ; disable
  :ui/toggle-settings     "t s"           ; chord
  :editor/up              ["ctrl+k" "up"] ; multiple
  :editor/down            ["ctrl+j" "down"]}
 :shortcut/doc-mode-enter-for-new-block? false}
```

---

## 6. Home & Navigation

Configure the default home page and sidebar behavior.

| Key             | Type  | Default | Description                        |
| --------------- | ----- | ------- | ---------------------------------- |
| `:default-home` | `map` | `nil`   | Configure startup page and sidebar |

### `:default-home` Sub-keys

| Sub-key    | Type                         | Description                          |
| ---------- | ---------------------------- | ------------------------------------ |
| `:page`    | `string`                     | Page name to load on startup         |
| `:sidebar` | `string` or `vector<string>` | Page(s) for right sidebar on startup |

```clojure
;; Single page with sidebar
{:default-home {:page "Changelog" :sidebar "Contents"}}

;; Multiple sidebar pages
{:default-home {:page "home" :sidebar ["Page A" "Page B" "Contents"]}}
```

---

## 7. Scheduling & Calendar

Configure date and scheduling behavior.

| Key                      | Type  | Default | Description                               |
| ------------------------ | ----- | ------- | ----------------------------------------- |
| `:scheduled/future-days` | `int` | `7`     | Days to display in scheduled tasks query  |
| `:start-of-week`         | `int` | `6`     | First day of week. `0`=Monday, `6`=Sunday |

```clojure
{:scheduled/future-days 14  ; 2 weeks
 :start-of-week 0}          ; Monday
```

---

## 8. Customization (CSS/JS/Macros)

Add custom styling, scripts, and text macros.

| Key               | Type     | Default | Description                                        |
| ----------------- | -------- | ------- | -------------------------------------------------- |
| `:custom-css-url` | `string` | `nil`   | URL for custom CSS (overrides `logseq/custom.css`) |
| `:custom-js-url`  | `string` | `nil`   | URL for custom JavaScript                          |
| `:macros`         | `map`    | `{}`    | Text replacement macros with placeholders          |
| `:commands`       | `vector` | `[]`    | Custom commands for command palette                |

### Macro Placeholders

- `$1`, `$2`, etc. are positional placeholders
- Usage: `{{macro-name arg1,arg2}}`

```clojure
{:custom-css-url "@import url('https://example.com/theme.css');"
 :custom-js-url "https://example.com/custom.js"

 :macros {"poem" "Rose is $1, violet's $2."
          "docs" "https://docs.logseq.com/#/page/$1"
          "yt" "{{video https://www.youtube.com/watch?v=$1}}"}

 :commands [["js" "Javascript"]
            ["md" "Markdown"]
            ["today" "{{date}}"]]}
```

---

## 9. Export Settings

| Key                          | Type      | Default | Description                       |
| ---------------------------- | --------- | ------- | --------------------------------- |
| `:export/bullet-indentation` | `keyword` | `:tab`  | Bullet indentation when exporting |

**Values**: `:tab`, `:two-spaces`, `:four-spaces`, `:eight-spaces`

```clojure
{:export/bullet-indentation :four-spaces}
```

---

## 10. Publishing Settings

| Key                             | Type      | Default | Description                                       |
| ------------------------------- | --------- | ------- | ------------------------------------------------- |
| `:publishing/all-pages-public?` | `boolean` | `false` | Publish all pages regardless of `public` property |

```clojure
{:publishing/all-pages-public? true}
```

---

## 11. Advanced Queries

| Key                        | Type  | Default         | Description                               |
| -------------------------- | ----- | --------------- | ----------------------------------------- |
| `:query/views`             | `map` | `{:pprint ...}` | Pre-defined `:view` functions for queries |
| `:query/result-transforms` | `map` | `{}`            | Transform functions for query results     |
| `:default-queries`         | `map` | `nil`           | Queries at bottom of journal pages        |

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
            [?b :block/scheduled ?d]]}]}}
```

---

## 12. Linked References

| Key                              | Type  | Default | Description                   |
| -------------------------------- | ----- | ------- | ----------------------------- |
| `:ref/default-open-blocks-level` | `int` | `2`     | Default expansion level (0-9) |

```clojure
;; Hierarchy: a (1) > b (2) > c (3) > d (4)
{:ref/default-open-blocks-level 2}  ; b collapsed
{:ref/default-open-blocks-level 3}  ; c collapsed
```

---

## 13. Outliner Settings

| Key                                       | Type      | Default | Description                                         |
| ----------------------------------------- | --------- | ------- | --------------------------------------------------- |
| `:outliner/block-title-collapse-enabled?` | `boolean` | `false` | Allow collapsing blocks with titles but no children |

```clojure
{:outliner/block-title-collapse-enabled? true}
```

---

## 14. Graph View

| Key                    | Type  | Default | Description                 |
| ---------------------- | ----- | ------- | --------------------------- |
| `:graph/settings`      | `map` | `nil`   | Graph view display settings |
| `:graph/forcesettings` | `map` | `nil`   | Graph physics settings      |

### `:graph/settings` Sub-keys

| Sub-key            | Type      | Default | Description                |
| ------------------ | --------- | ------- | -------------------------- |
| `:orphan-pages?`   | `boolean` | `true`  | Show pages with no links   |
| `:builtin-pages?`  | `boolean` | `false` | Show built-in system pages |
| `:excluded-pages?` | `boolean` | `false` | Show excluded pages        |
| `:journal?`        | `boolean` | `false` | Show journal pages         |

### `:graph/forcesettings` Sub-keys

| Sub-key            | Type  | Default | Description                       |
| ------------------ | ----- | ------- | --------------------------------- |
| `:link-dist`       | `int` | `180`   | Distance between linked nodes     |
| `:charge-strength` | `int` | `-600`  | Node repulsion (negative = repel) |
| `:charge-range`    | `int` | `600`   | Max distance for charge force     |

```clojure
{:graph/settings {:orphan-pages? true
                  :builtin-pages? false
                  :journal? false}

 :graph/forcesettings {:link-dist 180
                       :charge-strength -600
                       :charge-range 600}}
```

---

## 15. Mobile Settings

| Key             | Type  | Default | Description             |
| --------------- | ----- | ------- | ----------------------- |
| `:mobile/photo` | `map` | `nil`   | Photo upload config     |
| `:mobile`       | `map` | `nil`   | General mobile settings |

### `:mobile/photo` Sub-keys

| Sub-key           | Type      | Default | Description                  |
| ----------------- | --------- | ------- | ---------------------------- |
| `:allow-editing?` | `boolean` | `true`  | Allow cropping before upload |
| `:quality`        | `int`     | `80`    | JPEG quality (0-100)         |

```clojure
{:mobile/photo {:allow-editing? true
                :quality 80}
 :mobile {:gestures/disabled-in-block-with-tags ["kanban" "table"]}}
```

---

## 16. Quick Capture (Mobile)

| Key                        | Type  | Default | Description                    |
| -------------------------- | ----- | ------- | ------------------------------ |
| `:quick-capture-templates` | `map` | `nil`   | Templates for captured content |
| `:quick-capture-options`   | `map` | `nil`   | Capture behavior               |

### Template Placeholders

| Placeholder | Description       |
| ----------- | ----------------- |
| `{time}`    | Capture time      |
| `{date}`    | Capture date      |
| `{text}`    | Selected text     |
| `{url}`     | URL or asset path |

### `:quick-capture-options` Sub-keys

| Sub-key           | Type      | Default           | Description                       |
| ----------------- | --------- | ----------------- | --------------------------------- |
| `:insert-today?`  | `boolean` | `true`            | Insert at end of today's journal  |
| `:redirect-page?` | `boolean` | `false`           | Redirect to capture page          |
| `:default-page`   | `string`  | `"quick capture"` | Default page if not using journal |

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

Context-aware Enter key behavior.

| Key              | Type  | Default | Description        |
| ---------------- | ----- | ------- | ------------------ |
| `:dwim/settings` | `map` | `nil`   | DWIM configuration |

### `:dwim/settings` Sub-keys

| Sub-key            | Type      | Default | Description                       |
| ------------------ | --------- | ------- | --------------------------------- |
| `:admonition&src?` | `boolean` | `true`  | DWIM in admonitions/source blocks |
| `:markup?`         | `boolean` | `false` | DWIM for markup formatting        |
| `:block-ref?`      | `boolean` | `true`  | Enter opens block references      |
| `:page-ref?`       | `boolean` | `true`  | Enter opens page references       |
| `:properties?`     | `boolean` | `true`  | DWIM for properties               |
| `:list?`           | `boolean` | `false` | DWIM for lists (new list item)    |

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

> ⚠️ **These options only apply to file-based graphs** and are ignored in database graphs.

| Key                                 | Type             | Default          | Description                                |
| ----------------------------------- | ---------------- | ---------------- | ------------------------------------------ |
| `:preferred-format`                 | `keyword`        | `:markdown`      | `:markdown` or `:org`                      |
| `:preferred-workflow`               | `keyword`        | `:now`           | `:now` (NOW/LATER) or `:todo` (TODO/DOING) |
| `:pages-directory`                  | `string`         | `"pages"`        | Directory for page files                   |
| `:journals-directory`               | `string`         | `"journals"`     | Directory for journal files                |
| `:whiteboards-directory`            | `string`         | `"whiteboards"`  | Directory for whiteboard files             |
| `:journal/page-title-format`        | `string`         | `"MMM do, yyyy"` | Journal page title format                  |
| `:journal/file-name-format`         | `string`         | `"yyyy_MM_dd"`   | Journal filename format                    |
| `:hidden`                           | `vector<string>` | `[]`             | Directories/files to exclude               |
| `:favorites`                        | `vector<string>` | `nil`            | Pages in left sidebar favorites            |
| `:default-templates`                | `map`            | `{:journals ""}` | Default templates                          |
| `:file/name-format`                 | `keyword`        | `:triple-lowbar` | Filename escape method                     |
| `:property-pages/enabled?`          | `boolean`        | `true`           | Create pages for properties                |
| `:property/separated-by-commas`     | `set<keyword>`   | `nil`            | Properties with comma-separated values     |
| `:feature/enable-whiteboards?`      | `boolean`        | `true`           | Enable whiteboards                         |
| `:feature/enable-block-timestamps?` | `boolean`        | `false`          | Enable block timestamps                    |

### Logbook Settings (File-Graph Only)

| Sub-key                  | Type      | Default | Description                   |
| ------------------------ | --------- | ------- | ----------------------------- |
| `:with-second-support?`  | `boolean` | `true`  | Include seconds in timestamps |
| `:enabled-in-all-blocks` | `boolean` | `false` | Show logbook in all blocks    |

---

## 19. Spaced Repetition (File Graphs Only)

| Key                      | Type    | Default | Description                |
| ------------------------ | ------- | ------- | -------------------------- |
| `:srs/learning-fraction` | `float` | `0.5`   | Interval change rate (0-1) |
| `:srs/initial-interval`  | `int`   | `4`     | Initial interval in days   |

```clojure
{:srs/learning-fraction 0.5
 :srs/initial-interval 4}
```

---

## 20. Deprecated Options

| Deprecated Key              | Replacement                     | Notes       |
| --------------------------- | ------------------------------- | ----------- |
| `:editor/command-trigger`   | N/A                             | Use `/`     |
| `:all-pages-public?`        | `:publishing/all-pages-public?` | Use new key |
| `:block/content-max-length` | `:block/title-max-length`       | Use new key |

---

## Complete Example

```clojure
{:meta/version 1

 ;; UI
 :ui/enable-tooltip? true
 :ui/show-brackets? true

 ;; Features
 :feature/enable-journals? true
 :feature/enable-flashcards? true

 ;; Editor
 :block/title-max-length 10000
 :editor/logical-outdenting? false

 ;; Shortcuts
 :shortcuts {:editor/new-block "enter"
             :ui/toggle-settings "t s"}

 ;; Navigation
 :default-home {:page "Dashboard" :sidebar "Contents"}
 :start-of-week 0

 ;; Macros
 :macros {"docs" "https://docs.logseq.com/#/page/$1"}

 ;; Graph View
 :graph/settings {:orphan-pages? true :journal? false}

 ;; Mobile
 :quick-capture-options {:insert-today? true}}
```

---

## See Also

- **DB-specific config**: [config-edn-db.md](config-edn-db.md)
- **Keyboard shortcuts**: [keyboard-shortcuts.md](keyboard-shortcuts.md)
- **How to customize**: [Custom CSS Guide](../../how-to/integrations/custom-css.md)

## Source References

- [templates/config.edn](https://github.com/logseq/logseq/blob/master/deps/common/resources/templates/config.edn)
- [common_config.cljc](https://github.com/logseq/logseq/blob/master/src/main/frontend/schema/handler/common_config.cljc)

---

_Last updated: 2026-01-29 | [Report an issue](https://github.com/logseq/logseq/issues)_
