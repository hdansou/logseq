---
title: 'Database Graph config.edn Reference'
description: 'Configuration reference specifically for Logseq Database (DB) graphs'
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
sinceVersion: '0.10.0'
relatedDocs:
  reference:
    - '[config.edn Reference](config-edn.md)'
  explanation:
    - '[File vs DB Graphs](../../explanation/concepts/file-vs-db.md)'
---

# Database Graph config.edn Reference

> **Last revised:** 2026-09-09

> Configuration reference for **Database (DB) graphs** in Logseq. Highlights differences from file-based graphs and lists deprecated options.

## Overview

Database graphs (introduced in v0.10.0) use SQLite storage instead of individual markdown files. This changes which configuration options are applicable.

### Config Location

**Database Graphs:**

```
macOS/Linux:  ~/.logseq/graphs/<graph-name>/logseq/config.edn
Windows:      %USERPROFILE%\.logseq\graphs\<graph-name>\logseq\config.edn
```

> **Note**: In DB graphs, `config.edn` is stored inside the SQLite database and exported to the filesystem for compatibility.

---

## Quick Comparison

| Aspect            | File-Based                    | Database (DB)                             |
| ----------------- | ----------------------------- | ----------------------------------------- |
| Storage           | Individual `.md`/`.org` files | Single SQLite database                    |
| Properties        | Frontmatter YAML/Org          | Schema-validated database properties      |
| Tags/Classes      | Implicit (page links)         | Explicit class hierarchy with inheritance |
| File Organization | User-controlled directories   | Database-managed, no folders              |
| Config Options    | 50+ options                   | 23 options deprecated                     |

---

## Deprecated Options (23 Keys)

These options are **file-based only** and should NOT be used in DB graph `config.edn`:

### File Management (6 keys)

| Deprecated Key            | Reason                             | Replacement                  |
| ------------------------- | ---------------------------------- | ---------------------------- |
| `:file/name-format`       | DB graphs don't use files          | N/A                          |
| `:file-sync/ignore-files` | No file sync in DB mode            | N/A                          |
| `:hidden`                 | Hidden files concept doesn't exist | Use `:logseq.property/hide?` |
| `:journals-directory`     | No directory structure             | DB stores internally         |
| `:pages-directory`        | No directory structure             | DB stores internally         |
| `:whiteboards-directory`  | No directory structure             | DB stores internally         |

### Journal Settings (2 keys)

| Deprecated Key               | Reason              | Replacement                                 |
| ---------------------------- | ------------------- | ------------------------------------------- |
| `:journal/file-name-format`  | Not stored as files | N/A                                         |
| `:journal/page-title-format` | Display only        | Use `:logseq.property.journal/title-format` |

### Workflow (3 keys)

| Deprecated Key                | Reason                   | Replacement                   |
| ----------------------------- | ------------------------ | ----------------------------- |
| `:preferred-workflow`         | No `:now`/`:later` in DB | Use `:logseq.property/status` |
| `:preferred-format`           | Markdown only in DB      | N/A                           |
| `:org-mode/insert-file-link?` | No Org mode              | N/A                           |

### Properties (3 keys)

| Deprecated Key                  | Reason              | Replacement               |
| ------------------------------- | ------------------- | ------------------------- |
| `:property/separated-by-commas` | Schema-validated    | DB uses structured types  |
| `:property-pages/excludelist`   | No "property pages" | Use class/property schema |
| `:property-pages/enabled?`      | Concept removed     | N/A                       |

### Features (3 keys)

| Deprecated Key                      | Reason             | Replacement                  |
| ----------------------------------- | ------------------ | ---------------------------- |
| `:feature/enable-whiteboards?`      | Always enabled     | N/A                          |
| `:feature/enable-block-timestamps?` | Always recorded    | Automatic in DB              |
| `:block-hidden-properties`          | Different approach | Use `:logseq.property/hide?` |

### Flashcards/SRS (3 keys)

| Deprecated Key           | Reason               | Replacement                      |
| ------------------------ | -------------------- | -------------------------------- |
| `:srs/learning-fraction` | File-based algorithm | Use FSRS properties              |
| `:srs/initial-interval`  | File-based algorithm | Use FSRS properties              |
| `:logbook/settings`      | File-based format    | DB uses structured time tracking |

### UI/Organization (3 keys)

| Deprecated Key                      | Reason                     | Replacement                                            |
| ----------------------------------- | -------------------------- | ------------------------------------------------------ |
| `:favorites`                        | Not stored in config       | Managed via UI (internally uses `$$$favorites` page)   |
| `:default-templates`                | Templates as pages         | Use `#Template` tag and `Apply template to tags` property |
| `:ignored-page-references-keywords` | Database-managed           | N/A                                                    |

---

## Fully Supported Options

All options from [config-edn.md](config-edn.md) work in DB graphs **except** the 23 deprecated options above.

### UI Settings ✅

```clojure
{:ui/enable-tooltip? true
 :ui/show-brackets? true
 :ui/show-full-blocks? false
 :ui/auto-expand-block-refs? true
 :ui/show-command-doc? true
 :ui/show-empty-bullets? false
 :ui/hide-empty-properties? false}
```

**DB Note**: `:ui/hide-empty-properties?` hides properties with no value, even if inherited via tag.

### Feature Toggles ✅

```clojure
{:feature/enable-search-remove-accents? true
 :feature/enable-flashcards? true
 :feature/disable-scheduled-and-deadline-query? false}
```

> **Removed:** `:feature/enable-journals?` no longer exists in the codebase (no
> reference remains in `src/` or `deps/` at `be800f1711`). Setting it has no effect.

> Whiteboards are always enabled (`:feature/enable-whiteboards?` ignored).

### Editor Settings ✅

```clojure
{:block/title-max-length 10000
 :editor/logical-outdenting? false
 :editor/preferred-pasting-file? false
 :editor/extra-codemirror-options {:lineWrapping false
                                   :lineNumbers true}}
```

### Shortcuts ✅

```clojure
{:shortcuts
 {:editor/new-block "enter"
  :editor/new-line "shift+enter"
  :ui/toggle-settings "t s"}
 :shortcut/doc-mode-enter-for-new-block? false}
```

### Navigation ✅

```clojure
{:default-home {:page "Dashboard" :sidebar ["Contents"]}
 :start-of-week 6
 :scheduled/future-days 7}
```

### Customization ✅

```clojure
{:custom-css-url "@import url('https://example.com/theme.css');"
 :custom-js-url "https://example.com/custom.js"
 :macros {"meeting" "## Meeting\nDate:: <% today %>"}}
```

**DB Note**: Custom CSS/JS files are stored in database and exported to `<graph>/logseq/`.

### Advanced Queries ✅

```clojure
{:query/views
 {:pprint (fn [r] [:pre.code (pprint r)])}

 :query/result-transforms
 {:sort-by-priority
  (fn [result]
    (sort-by (fn [h] (get h :logseq.property/priority "Z")) result))}}
```

**DB-Specific**: Queries use namespaced property keywords like `:logseq.property/priority`.

### Graph View

> **Removed:** `:graph/settings` and `:graph/forcesettings` are no longer read from
> `config.edn`. Graph-view state (view mode, depth, link distance, grid layout,
> show-journals) is now stored per graph in browser local storage under the key
> `logseq.graph.settings.<repo>` — see
> `src/main/frontend/components/graph.cljs:32-95`. Adjust these settings from the
> graph view's settings panel instead.

### Export & Publishing ✅

```clojure
{:export/bullet-indentation :tab        ; :eight-spaces, :four-spaces, :two-spaces, or :tab
 :publishing/all-pages-public? false}
```

### Outliner & References ✅

```clojure
{:outliner/block-title-collapse-enabled? false
 :ref/default-open-blocks-level 2}
```

### Commands & Default Queries ✅

```clojure
{:commands [["js" "Javascript"]
            ["md" "Markdown"]]
 :default-queries {:journals []}}
```

### Mobile & Quick Capture ✅

```clojure
{:mobile/photo {:allow-editing? true :quality 80}
 :mobile {:gestures/disabled-in-block-with-tags ["kanban"]}
 :quick-capture-templates
 {:text "[[capture]] {time}: {text}"}
 :quick-capture-options
 {:insert-today? true}}
```

---

## Complete DB Graph Example

```clojure
{:meta/version 1

 ;; UI
 :ui/enable-tooltip? true
 :ui/show-brackets? true
 :ui/hide-empty-properties? false

 ;; Features
 :feature/enable-search-remove-accents? true
 :feature/enable-flashcards? true

 ;; Editor
 :block/title-max-length 10000
 :editor/logical-outdenting? false
 :outliner/block-title-collapse-enabled? false
 :ref/default-open-blocks-level 2

 ;; Navigation
 :default-home {:page "Dashboard" :sidebar ["Contents"]}
 :start-of-week 6
 :scheduled/future-days 7

 ;; Shortcuts
 :shortcuts {:editor/new-block "enter"
             :ui/toggle-settings "t s"}

 ;; Customization
 :macros {"meeting" "## Meeting\nDate:: <% today %>"}

 ;; Queries (note: namespaced properties)
 :query/result-transforms
 {:sort-by-priority
  (fn [result]
    (sort-by (fn [h] (get h :logseq.property/priority "Z")) result))}

 ;; Mobile
 :quick-capture-options {:insert-today? true}}
```

---

## Migration from File-Based

### Step 1: Identify deprecated options

```clojure
;; File-based config.edn
{:preferred-format :markdown           ; ❌ Remove
 :journals-directory "journals"         ; ❌ Remove
 :pages-directory "pages"               ; ❌ Remove
 :ui/enable-tooltip? true               ; ✅ Keep
 :shortcuts {}                          ; ✅ Keep
 :macros {}}                            ; ✅ Keep
```

### Step 2: Update query property references

```clojure
;; File-based query
[:find (pull ?b [*])
 :where [?b :block/properties ?props]
        [(get ?props "priority") ?pri]]

;; DB query
[:find (pull ?b [*])
 :where [?b :logseq.property/priority ?pri]]
```

### Step 3: Migrate templates

- **File-based**: Templates are pages with `:template` property
- **DB**: Templates are pages tagged with `#Template` class

### Step 4: Migrate favorites

```clojure
;; File-based (in config.edn)
{:favorites ["Page A" "Page B"]}

;; DB: Use the UI to favorite pages (right-click page > "Add to favorites")
;; Favorites are managed internally via a $$$favorites page
;; No config.edn entry needed
```

---

## Troubleshooting

### Config Not Taking Effect

1. **Restart Logseq** - Some changes require restart
2. **Check syntax** - EDN must be valid
3. **Verify location** - DB config at `~/.logseq/graphs/<name>/logseq/config.edn`
4. **Check console** - Open DevTools (`Cmd+Shift+I`)

### Schema Validation Errors

DB graphs validate config against Malli schema:

```clojure
;; ❌ Wrong (string)
{:ui/show-brackets? "true"}

;; ✅ Correct (boolean)
{:ui/show-brackets? true}
```

### Query Property References

```clojure
;; ❌ File-based property access
[(get ?props "status") ?status]

;; ✅ DB property access
[?b :logseq.property/status ?status]
```

---

## See Also

- **Full config reference**: [config-edn.md](config-edn.md)
- **File vs DB concepts**: [File vs DB Graphs](../../explanation/concepts/file-vs-db.md)
- **Schema reference**: [Schema Overview](../database/schema-overview.md)

## Source References

- [Schema Definition](https://github.com/logseq/logseq/blob/master/src/main/frontend/schema/handler/common_config.cljc)
- [Config Template](https://github.com/logseq/logseq/blob/master/deps/common/resources/templates/config.edn)
- [Deprecated Keys (`file-only-config`)](https://github.com/logseq/logseq/blob/master/deps/common/src/logseq/common/config.cljs)
- [Graph view settings storage](https://github.com/logseq/logseq/blob/master/src/main/frontend/components/graph.cljs)

---

_Last updated: 2026-09-09 | [Report an issue](https://github.com/logseq/logseq/issues)_
