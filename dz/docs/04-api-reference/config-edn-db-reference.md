# Logseq Database (DB) Graph config.edn Reference Guide

> **Last revised:** 2026-05-29

**Logseq Version:** 0.11.x+

A comprehensive reference for configuration options in **Database (DB) Graph** mode. This guide focuses on DB graphs and highlights differences from file-based (Markdown/Org) graphs.

---

## 📋 Table of Contents

1. [Introduction](#introduction)
2. [DB vs File-Based: Key Differences](#db-vs-file-based-key-differences)
3. [Deprecated File-Only Options](#deprecated-file-only-options)
4. [DB-Compatible Options Reference](#db-compatible-options-reference)
5. [Example DB Graph Config](#example-db-graph-config)
6. [Migration Guide](#migration-guide)
7. [Troubleshooting](#troubleshooting)

---

## Introduction

### What are Database Graphs?

**Database (DB) graphs** were introduced in Logseq 0.10.0 as a new storage backend:

- **File-based graphs** (traditional): Store data in `.md`/`.org` files on your filesystem
- **Database graphs** (new): Store data in SQLite database with improved performance and capabilities

### Configuration File Location

Both graph types use `config.edn`:

**File-Based Graphs:**

```
<your-graph-folder>/logseq/config.edn
```

**Database Graphs:**

```
macOS/Linux:  ~/.logseq/graphs/<graph-name>/logseq/config.edn
Windows:      %USERPROFILE%\.logseq\graphs\<graph-name>\logseq\config.edn
```

> **Note**: In DB graphs, `config.edn` is stored **inside the SQLite database**, not as a separate file. It's exported to the filesystem location above for compatibility.

---

## DB vs File-Based: Key Differences

### Architecture Changes

| Aspect            | File-Based                      | Database (DB)                             |
| ----------------- | ------------------------------- | ----------------------------------------- |
| Storage           | Individual `.md`/`.org` files   | Single SQLite database file               |
| Properties        | Frontmatter YAML/Org properties | Schema-validated database properties      |
| Tags/Classes      | Implicit (page links as tags)   | Explicit class hierarchy with inheritance |
| File Organization | User-controlled directories     | Database-managed, no folders              |
| Whiteboards       | Separate `.edn` files           | Stored in database                        |

### Config Implications

**23 config options are deprecated/not applicable in DB graphs** (see [Deprecated File-Only Options](#deprecated-file-only-options)).

Most UI, query, and workflow options remain available. Deprecated DB-inapplicable options are ignored after parse and surfaced as deprecation warnings.

---

## Deprecated File-Only Options

The following **23 configuration keys** are file-based only and **should NOT be used** in DB graph `config.edn`:

### File Management (6 keys)

| Deprecated Key            | Reason                             | Replacement                           |
| ------------------------- | ---------------------------------- | ------------------------------------- |
| `:file/name-format`       | DB graphs don't use files          | N/A - database manages naming         |
| `:file-sync/ignore-files` | No file sync in DB mode            | N/A                                   |
| `:hidden`                 | Hidden files concept doesn't exist | Use `:logseq.property/hide?` on pages |
| `:journals-directory`     | No directory structure             | DB stores journals internally         |
| `:pages-directory`        | No directory structure             | DB stores pages internally            |
| `:whiteboards-directory`  | No directory structure             | DB stores whiteboards internally      |

### Journal Settings (2 keys)

| Deprecated Key               | Reason                                                   | Replacement                                 |
| ---------------------------- | -------------------------------------------------------- | ------------------------------------------- |
| `:journal/file-name-format`  | Journals not stored as files                             | N/A                                         |
| `:journal/page-title-format` | Still exists for display, but filename format deprecated | Use `:logseq.property.journal/title-format` |

### Workflow (3 keys)

| Deprecated Key                | Reason                              | Replacement                            |
| ----------------------------- | ----------------------------------- | -------------------------------------- |
| `:preferred-workflow`         | No `:now` / `:later` workflow in DB | Use `:logseq.property/status` property |
| `:preferred-format`           | Only Markdown in DB mode            | N/A - Markdown only                    |
| `:org-mode/insert-file-link?` | No Org mode in DB                   | N/A                                    |

### Properties (3 keys)

| Deprecated Key                  | Reason                          | Replacement                        |
| ------------------------------- | ------------------------------- | ---------------------------------- |
| `:property/separated-by-commas` | Properties are schema-validated | DB uses structured data types      |
| `:property-pages/excludelist`   | No "property pages" in DB       | Use class/property schema directly |
| `:property-pages/enabled?`      | Property pages concept removed  | N/A                                |

### Features (3 keys)

| Deprecated Key                      | Reason                           | Replacement                |
| ----------------------------------- | -------------------------------- | -------------------------- |
| `:feature/enable-whiteboards?`      | Whiteboards always enabled in DB | N/A - always available     |
| `:feature/enable-block-timestamps?` | Block timestamps always recorded | Automatic in DB mode       |
| `:block-hidden-properties`          | DB uses `:logseq.property/hide?` | Set `:hide?` on properties |

### Flashcards/SRS (3 keys)

| Deprecated Key           | Reason                    | Replacement                      |
| ------------------------ | ------------------------- | -------------------------------- |
| `:srs/learning-fraction` | File-based SRS algorithm  | Use FSRS properties in DB        |
| `:srs/initial-interval`  | File-based SRS algorithm  | Use FSRS properties in DB        |
| `:logbook/settings`      | File-based logbook format | DB uses structured time tracking |

### UI/Organization (3 keys)

| Deprecated Key                      | Reason                                     | Replacement                      |
| ----------------------------------- | ------------------------------------------ | -------------------------------- |
| `:favorites`                        | Stored as property, not config             | Use `:logseq.property/favorites` |
| `:default-templates`                | Templates stored as pages with `#Template` | Create template pages with class |
| `:ignored-page-references-keywords` | Reference tracking is database-managed     | N/A                              |

> **Important**: Including these keys in DB graph config.edn does not hard-fail parsing, but Logseq shows deprecation warnings and treats the keys as not used in DB graphs.

> **Note**: Deprecation detection also warns for non-DB-specific deprecated keys `:editor/command-trigger` and `:arweave/gateway`.

---

## DB-Compatible Options Reference

All options from the [full config.edn reference](config-edn-reference.md) work in DB graphs **except** the 23 deprecated options above.

### 1. Metadata ✅ **Fully Supported**

```clojure
{:meta/version 1}  ;; Always set to 1
```

### 2. UI Settings ✅ **Fully Supported**

All 7 UI options work identically in DB graphs:

```clojure
{:ui/enable-tooltip? true                ;; Tooltip previews
 :ui/show-brackets? true                 ;; [[Page]] brackets
 :ui/show-full-blocks? false             ;; ((Block ref)) expansion
 :ui/auto-expand-block-refs? true        ;; Auto-expand on zoom
 :ui/show-command-doc? true              ;; Command help text
 :ui/show-empty-bullets? false           ;; Empty bullet display
 :ui/hide-empty-properties? false}       ;; Hide empty properties (DB-specific behavior)
```

**DB-Specific Note**: `:ui/hide-empty-properties?` hides properties with no value _on a specific block_, even if the property exists via tag inheritance.

### 3. Feature Toggles ✅ **Fully Supported**

```clojure
{:feature/enable-search-remove-accents? true   ;; Search without accents
 :feature/enable-journals? true                ;; Enable daily journals
 :feature/enable-flashcards? true              ;; Enable flashcard system
 :feature/disable-scheduled-and-deadline-query? false}  ;; Disable task query
```

> **DB Note**: Whiteboards are always enabled in DB mode (`:feature/enable-whiteboards?` is ignored).

### 4. Editor Settings ✅ **Fully Supported**

```clojure
{:block/title-max-length 10000           ;; Max block content length
 :editor/extra-codemirror-options        ;; CodeMirror config
 {:lineWrapping false
  :lineNumbers true
  :readOnly false}
 :editor/logical-outdenting? false       ;; Logical outdent behavior
 :editor/preferred-pasting-file? false}  ;; Paste file vs text priority
```

### 5. Shortcuts ✅ **Fully Supported**

```clojure
{:shortcuts
 {:editor/new-block "enter"
  :editor/new-line "shift+enter"
  :editor/insert-link "mod+shift+k"
  :ui/toggle-settings "t s"
  :editor/up ["ctrl+k" "up"]
  :editor/down ["ctrl+j" "down"]}

 :shortcut/doc-mode-enter-for-new-block? false}
```

### 6. Home & Navigation ✅ **Fully Supported**

```clojure
{:default-home
 {:page "Dashboard"                      ;; Startup page
  :sidebar ["Contents" "Quick Links"]}}  ;; Right sidebar pages
```

### 7. Scheduling & Calendar ✅ **Fully Supported**

```clojure
{:scheduled/future-days 7                ;; Days to show in task queries
 :start-of-week 6}                       ;; 0=Monday, 6=Sunday
```

### 8. Customization ✅ **Fully Supported**

```clojure
{:custom-css-url "@import url('https://example.com/theme.css');"
 :custom-js-url "https://example.com/custom.js"
 :macros
 {"meeting" "## Meeting Notes\nDate:: <% today %>\n- **Attendees**: $1\n- **Topic**: $2"
  "task" "TODO $1 #priority"}}
```

**DB Note**: Custom CSS/JS files are stored in the database and exported to `<graph>/logseq/custom.css|js`.

### 9. Export ✅ **Fully Supported**

```clojure
{:export/bullet-indentation :tab}        ;; :tab | :two-spaces | :four-spaces | :eight-spaces
```

### 10. Publishing ✅ **Fully Supported**

```clojure
{:publishing/all-pages-public? false}    ;; Publish all pages
```

### 11. Advanced Queries ✅ **Fully Supported**

```clojure
{:query/views
 {:pprint (fn [r] [:pre.code (pprint r)])}

 :query/result-transforms
 {:sort-by-priority
  (fn [result]
    (sort-by (fn [h] (get h :logseq.property/priority "Z")) result))}

 :default-queries
 {:journals
  [[:find (pull ?b [*])
    :where [?b :block/journal-day ?d]]]}}
```

**DB-Specific**: Queries use structured property names like `:logseq.property/priority` instead of string property keys.

### 12. Commands ✅ **Fully Supported**

```clojure
{:commands
 [["js" "Javascript"]
  ["ts" "TypeScript"]
  ["clj" "Clojure"]]}
```

### 13. Outliner ✅ **Fully Supported**

```clojure
{:outliner/block-title-collapse-enabled? false}
```

### 14. Linked References ✅ **Fully Supported**

```clojure
{:ref/default-open-blocks-level 2}       ;; 1-4, controls expansion depth
```

### 15. Graph View ✅ **Fully Supported**

```clojure
{:graph/settings
 {:orphan-pages? true                    ;; Show unlinked pages
  :builtin-pages? false                  ;; Show built-in pages
  :excluded-pages? false                 ;; Show excluded pages
  :journal? false}                       ;; Show journal pages

 :graph/forcesettings
 {:link-dist 180                         ;; Link distance
  :charge-strength -600                  ;; Node repulsion
  :charge-range 600}}                    ;; Force range
```

### 16. Mobile ✅ **Fully Supported**

```clojure
{:mobile/photo
 {:allow-editing? true
  :quality 80}

 :mobile
 {:gestures/disabled-in-block-with-tags ["kanban"]}}
```

### 17. Quick Capture ✅ **Fully Supported**

```clojure
{:quick-capture-templates
 {:text "[[quick capture]] **{time}**: {text} from {url}"
  :media "[[quick capture]] **{time}**: {url}"}

 :quick-capture-options
 {:insert-today? true                    ;; Insert at end of journal
  :redirect-page? false                  ;; Redirect after capture
  :default-page "quick capture"          ;; Default capture page
  :prettify-url? true}}                  ;; Wrap YouTube/X links in embeds
```

### 18. DWIM Settings

> **Removed:** `:dwim/settings` is not present in the current `Config-edn` schema or `deps/common/resources/templates/config.edn`. Remove it from DB graph `config.edn` guidance.

### 19. Feature Flags ⚠️ **Undocumented**

```clojure
{:feature/enable-grammarly? false}       ;; Enable Grammarly integration
```

> **Note**: Found in code but not in schema. Status unclear.

---

## Findings: New Capabilities

- `:quick-capture-options` supports `:prettify-url?` (template-documented) to auto-wrap supported URLs (for example, YouTube/X) in embed macros.
- DB config deprecation detection includes additional deprecated keys `:editor/command-trigger` and `:arweave/gateway`.

---

## Example DB Graph Config

**Complete example config.edn for a Database graph:**

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
 :shortcut/doc-mode-enter-for-new-block? false

 ;; === Navigation ===
 :default-home {:page "Dashboard", :sidebar ["Contents"]}
 :start-of-week 6
 :scheduled/future-days 7

 ;; === Customization ===
 :custom-css-url "@import url('https://cdn.example.com/my-theme.css');"
 :macros
 {"meeting" "## Meeting\nDate:: <% today %>\nAttendees: $1"}

 ;; === Shortcuts ===
 :shortcuts
 {:editor/new-block "enter"
  :editor/new-line "shift+enter"
  :editor/insert-link "mod+shift+k"
  :ui/toggle-settings "t s"}

 ;; === Queries ===
 :query/views {:pprint (fn [r] [:pre.code (pprint r)])}
 :query/result-transforms
 {:sort-by-priority
  (fn [result]
    (sort-by (fn [h] (get h :logseq.property/priority "Z")) result))}

 ;; === Advanced ===
 :commands [["js" "Javascript"] ["ts" "TypeScript"]]
 :outliner/block-title-collapse-enabled? false
 :ref/default-open-blocks-level 2
 :export/bullet-indentation :tab
 :publishing/all-pages-public? false

 ;; === Graph View ===
 :graph/settings {:orphan-pages? true :builtin-pages? false}
 :graph/forcesettings {:link-dist 180 :charge-strength -600}

 ;; === Mobile ===
 :mobile/photo {:allow-editing? true :quality 80}
 :quick-capture-templates
 {:text "[[capture]] {time}: {text}"
  :media "[[capture]] {time}: {url}"}
 :quick-capture-options
 {:insert-today? true :redirect-page? false :prettify-url? true}}
```

---

## Migration Guide

### Converting File-Based Config to DB Config

**Step 1**: Identify deprecated options

```bash
# File-based config.edn
{:preferred-format :markdown           # ❌ Remove
 :journals-directory "journals"         # ❌ Remove
 :pages-directory "pages"               # ❌ Remove
 :file/name-format :triple-lowbar       # ❌ Remove
 :ui/enable-tooltip? true               # ✅ Keep
 :shortcuts {}                          # ✅ Keep
 :macros {}}                            # ✅ Keep
```

**Step 2**: Remove 23 file-only options (see [Deprecated Options](#deprecated-file-only-options))

**Step 3**: Update query property references

```clojure
;; File-based query
[:find (pull ?b [*])
 :where [?b :block/properties ?props]
        [(get ?props "priority") ?pri]]

;; DB query
[:find (pull ?b [*])
 :where [?b :logseq.property/priority ?pri]]
```

**Step 4**: Migrate templates

- File-based: Templates are regular pages with `:template` property
- DB: Templates are pages tagged with `#Template` class

**Step 5**: Migrate favorites

```clojure
;; File-based: In config.edn
{:favorites ["Page A" "Page B"]}

;; DB: Use UI or set property
;; No config.edn entry - use :logseq.property/favorites on pages
```

---

## Troubleshooting

### Config Not Taking Effect

**Problem**: Changes to `config.edn` don't apply

**Solutions**:

1. **Restart Logseq** - Some config changes require app restart
2. **Check syntax** - EDN must be valid (use https://edn-lint.io/)
3. **Verify location** - DB config is at `~/.logseq/graphs/<name>/logseq/config.edn`
4. **Check for schema errors** - Open DevTools (Cmd+Shift+I), check console

### Schema Validation Errors

**Problem**: Config values rejected

**Cause**: DB graphs validate config against Malli schema

**Solution**: Check [schema definition](https://github.com/logseq/logseq/blob/master/src/main/frontend/schema/handler/common_config.cljc)

Example error:

```
Invalid :ui/show-brackets?: expected boolean, got "true" (string)
```

Fix:

```clojure
;; ❌ Wrong (string)
{:ui/show-brackets? "true"}

;; ✅ Correct (boolean)
{:ui/show-brackets? true}
```

### File-Only Option Warnings

**Problem**: Warning about deprecated config key

**Example**:

```
Warning: :preferred-format is not used in DB graphs
```

**Solution**: Remove the deprecated key from `config.edn`

### Query Property References

**Problem**: Query fails with "property not found"

**Cause**: DB queries use namespaced property keywords

**Solution**:

```clojure
;; ❌ File-based property access
[(get ?props "status") ?status]

;; ✅ DB property access
[?b :logseq.property/status ?status]
```

---

## Source References

- **Schema Definition**: [src/main/frontend/schema/handler/common_config.cljc](https://github.com/logseq/logseq/blob/master/src/main/frontend/schema/handler/common_config.cljc)
- **File-Only Config**: [deps/common/src/logseq/common/config.cljs](https://github.com/logseq/logseq/blob/master/deps/common/src/logseq/common/config.cljs)
- **Config Template**: [deps/common/resources/templates/config.edn](https://github.com/logseq/logseq/blob/master/deps/common/resources/templates/config.edn)
- **State Management**: [src/main/frontend/state.cljs](https://github.com/logseq/logseq/blob/master/src/main/frontend/state.cljs)

---

**Version**: Logseq DB 0.11.x

For file-based graph config reference, see [config-edn-reference.md](config-edn-reference.md).
