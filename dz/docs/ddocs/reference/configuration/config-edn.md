---
title: 'config.edn Reference'
description: "Complete reference for Logseq's config.edn configuration file"
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
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

> **Last revised:** 2026-09-09

> Complete reference for all configuration options in Logseq's `config.edn` file. For DB-specific configuration, see [config-edn-db.md](config-edn-db.md).

## Overview

The `config.edn` file controls Logseq's behavior and appearance. It's located in your graph's `logseq/` directory.

**Location:**

- **File-based graphs**: `<your-graph-folder>/logseq/config.edn`
- **Database graphs**: Stored within the database itself (accessible at `logseq/config.edn` inside the graph)

## Quick Reference

| Category   | Key Options                                                | Description                 |
| ---------- | ---------------------------------------------------------- | --------------------------- |
| UI         | `:ui/show-brackets?`, `:ui/enable-tooltip?`                | Visual appearance           |
| Features   | `:feature/enable-flashcards?`, `:feature/enable-search-remove-accents?` | Enable/disable features |
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
| `:feature/enable-flashcards?`                    | `boolean` | `true`  | Enable flashcards/spaced repetition                                           |
| `:feature/disable-scheduled-and-deadline-query?` | `boolean` | `false` | Disable built-in task query on journal pages                                  |
| `:feature/enable-grammarly?`                     | `boolean` | `false` | Reserve an extra editor row so the Grammarly overlay has room                 |

```clojure
{:feature/enable-search-remove-accents? true
 :feature/enable-flashcards? true
 :feature/disable-scheduled-and-deadline-query? false}
```

> `:feature/enable-grammarly?` is read at `src/main/frontend/state.cljs:666-668` and its
> only effect is at `src/main/frontend/components/editor.cljs:747` (textarea `minRows`
> becomes `2` instead of `1`). It is not part of the validated `config.edn` schema.

> **Removed:** `:feature/enable-journals?` no longer exists in the codebase — no
> reference remains anywhere in `src/` or `deps/` at `be800f1711`. Setting it has no
> effect.

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

> **Removed:** `:graph/settings` and `:graph/forcesettings` are no longer read from
> `config.edn`, and neither key (nor the sub-keys `:link-dist`, `:charge-strength`,
> `:charge-range`) appears anywhere in `src/` or `deps/` at `be800f1711`.
>
> Graph-view state is now persisted per graph in browser local storage under the key
> `logseq.graph.settings.<repo>` (`src/main/frontend/components/graph.cljs:31-34`).
> The stored map holds `:view-mode`, `:selected-tag-ids`, `:created-at-filter`,
> `:depth`, `:link-distance`, `:grid-layout?`, `:show-journals?` and `:open-groups`
> (`src/main/frontend/components/graph.cljs:22-29`), and is edited from the graph
> view's own settings panel rather than from `config.edn`.

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

> **Removed**: `:dwim/settings` is no longer present in the current configuration schema or default template. It appeared in older versions of Logseq but has been removed from the active codebase. The key is retained here for historical reference only.

| Key              | Type  | Default | Description        |
| ---------------- | ----- | ------- | ------------------ |
| `:dwim/settings` | `map` | `nil`   | DWIM configuration (removed) |

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
| `:feature/enable-block-timestamps?` | `boolean`        | `false`          | Enable block timestamps (always enabled in DB graphs) |
| `:logbook/settings`                 | `map`            | `nil`            | Logbook configuration                      |
| `:org-mode/insert-file-link?`       | `boolean`        | `false`          | Convert page refs to org-mode file links   |
| `:file-sync/ignore-files`           | `vector<string>` | `[]`             | Files to ignore during sync (regexp supported) |
| `:ignored-page-references-keywords` | `set<keyword>`   | `nil`            | Properties to ignore when parsing page refs |
| `:property-pages/excludelist`       | `set<keyword>`   | `nil`            | Properties excluded from having property pages |
| `:block-hidden-properties`          | `set<keyword>`   | `nil`            | Hide specific block properties (not used in DB graphs) |

### `:logbook/settings` Sub-keys (File-Graph Only)

> **Unverified:** at `be800f1711` the key `:logbook/settings` survives only as an entry
> in the DB-graph deprecation list (`deps/common/src/logseq/common/config.cljs:117`).
> None of the sub-keys below are read anywhere in `src/` or `deps/`, so treat them as
> historical documentation for older file-graph builds.

| Sub-key                          | Type      | Default | Description                              |
| -------------------------------- | --------- | ------- | ---------------------------------------- |
| `:with-second-support?`          | `boolean` | `true`  | Include seconds in timestamps            |
| `:enabled-in-all-blocks`         | `boolean` | `false` | Show logbook in all blocks               |
| `:enabled-in-timestamped-blocks` | `boolean` | `true`  | Display logbook in timestamped blocks    |

---

## 19. Spaced Repetition (File Graphs Only)

> These settings are file-graph only and are not used in DB graphs. They are not part of the validated configuration schema but are read at runtime.

| Key                      | Type    | Default | Description                                              |
| ------------------------ | ------- | ------- | -------------------------------------------------------- |
| `:srs/learning-fraction` | `float` | `0.5`   | Interval change rate (0-1); higher = faster progression  |
| `:srs/initial-interval`  | `int`   | `4`     | Initial interval in days after first successful review   |

```clojure
{:srs/learning-fraction 0.5
 :srs/initial-interval 4}
```

---

## 20. Deprecated Options

| Deprecated Key              | Replacement                     | Notes                |
| --------------------------- | ------------------------------- | -------------------- |
| `:editor/command-trigger`   | N/A                             | Use `/`              |
| `:all-pages-public?`        | `:publishing/all-pages-public?` | Use new key          |
| `:block/content-max-length` | `:block/title-max-length`       | Use new key          |
| `:arweave/gateway`          | N/A                             | No longer supported  |
| `:dwim/settings`            | N/A                             | Removed from codebase |
| `:feature/enable-journals?` | N/A                             | Removed from codebase |
| `:graph/settings`           | Graph view settings panel       | No longer read from config.edn |
| `:graph/forcesettings`      | Graph view settings panel       | No longer read from config.edn |

---

## Complete Example

```clojure
{:meta/version 1

 ;; UI
 :ui/enable-tooltip? true
 :ui/show-brackets? true

 ;; Features
 :feature/enable-flashcards? true
 :feature/enable-search-remove-accents? true

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
- [file-only-config (DB-graph deprecations)](https://github.com/logseq/logseq/blob/master/deps/common/src/logseq/common/config.cljs)
- [config_edn.cljs (deprecated key validation)](https://github.com/logseq/logseq/blob/master/src/main/frontend/handler/common/config_edn.cljs)

---

_Last updated: 2026-09-09 | [Report an issue](https://github.com/logseq/logseq/issues)_
