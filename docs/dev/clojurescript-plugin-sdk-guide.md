# Logseq ClojureScript Plugin SDK Developer Guide

## Overview

The Logseq ClojureScript Plugin SDK (`@logseq/cljs-libs`) provides idiomatic ClojureScript wrappers for the JavaScript Logseq Plugin API (`@logseq/libs`). This SDK enables ClojureScript developers to build Logseq plugins with full type safety, automatic data conversion between JavaScript and ClojureScript, and functional programming patterns.

**Version:** 0.0.10
**Package:** `@logseq/cljs-libs`
**Merged:** PR #12168 (commit d8809f0b7)

## Key Features

- **Auto-generated wrappers** - All API bindings are automatically generated from the JavaScript SDK schema
- **Automatic data conversion** - Seamless conversion between JavaScript objects and ClojureScript data structures
- **Promise normalization** - Transparent handling of asynchronous operations
- **Idiomatic ClojureScript** - Functions follow ClojureScript naming conventions and patterns
- **Multiple arity support** - Optional parameters supported through multiple function arities

## Installation

### Prerequisites

- ClojureScript 1.11.60 or higher
- Node.js and npm/yarn
- Logseq desktop application

### Setup

```bash
# Install the SDK
yarn add @logseq/cljs-libs

# Or with npm
npm install @logseq/cljs-libs
```

### Project Configuration

Add to your `deps.edn`:

```clojure
{:deps {org.clojure/clojurescript {:mvn/version "1.11.60"}
        cljs-bean/cljs-bean {:mvn/version "1.5.0"}}}
```

## Architecture

### Namespace Organization

The SDK is organized into 8 main namespaces under `com.logseq`:

| Namespace | Description | Functions |
|-----------|-------------|-----------|
| `com.logseq.core` | Core plugin lifecycle and main UI | 15 functions |
| `com.logseq.app` | Application-level operations | 72+ functions |
| `com.logseq.editor` | Block and page manipulation | 70+ functions |
| `com.logseq.db` | Database queries and subscriptions | 4 functions |
| `com.logseq.ui` | User interface utilities | 6 functions |
| `com.logseq.assets` | Asset and file management | 4 functions |
| `com.logseq.git` | Git operations | 3 functions |
| `com.logseq.util` | Data conversion utilities | Internal use |

### Data Conversion

The SDK automatically handles conversion between JavaScript and ClojureScript data types:

**JavaScript → ClojureScript:**
- JS objects → ClojureScript maps
- JS arrays → ClojureScript vectors
- JS strings → ClojureScript strings
- Promises → Normalized promises that resolve to ClojureScript data

**ClojureScript → JavaScript:**
- Maps → JS objects
- Vectors → JS arrays
- Keywords → Encoded strings (with special `___kw___` prefix for round-tripping)

### Promise Handling

All asynchronous operations return JavaScript Promises that automatically convert resolved values to ClojureScript data:

```clojure
;; Promises are normalized and automatically convert results
(-> (editor/get-block "block-uuid")
    (.then (fn [block]
             ;; block is already a ClojureScript map
             (println (:content block)))))
```

## API Reference

### com.logseq.core

Core plugin lifecycle, settings management, and main UI control.

#### Lifecycle Functions

**`(ready)`**
**`(ready model)`**
**`(ready model callback)`**

Initialize the plugin. Sets SDK metadata to indicate ClojureScript runtime.

```clojure
(ns my-plugin.core
  (:require [com.logseq.core :as logseq]))

(logseq/ready {:name "my-plugin"})
```

**`(ensure-connected)`**

Ensure the plugin is connected to Logseq.

**`(beforeunload callback)`**

Register a callback to run before the plugin unloads.

```clojure
(logseq/beforeunload
  (fn []
    (println "Plugin shutting down...")))
```

#### Provider Functions

**`(provide-model model)`**

Provide a model object with custom methods accessible to the UI.

**`(provide-theme theme)`**

Register a custom theme.

**`(provide-style style)`**

Inject custom CSS styles.

**`(provide-ui ui)`**

Provide custom UI components.

#### Settings Management

**`(use-settings-schema schema)`**

Define the settings schema for your plugin.

```clojure
(logseq/use-settings-schema
  [{:key "api-key"
    :type "string"
    :title "API Key"
    :description "Your API key"}
   {:key "enabled"
    :type "boolean"
    :title "Enable Feature"
    :default true}])
```

**`(update-settings attrs)`**

Update plugin settings.

**`(on-settings-changed cb)`**

Listen for settings changes.

**`(show-settings-ui)`** / **`(hide-settings-ui)`**

Control settings UI visibility.

#### Main UI Control

**`(show-main-ui)`** / **`(show-main-ui opts)`**
**`(hide-main-ui)`** / **`(hide-main-ui opts)`**
**`(toggle-main-ui)`**

Control the main plugin UI visibility.

**`(set-main-ui-attrs attrs)`**

Set HTML attributes on the main UI container.

**`(set-main-ui-inline-style style)`**

Set inline styles on the main UI.

**`(resolve-resource-full-url file-path)`**

Resolve a plugin resource path to a full URL.

---

### com.logseq.app

Application-level operations including graph management, commands, templates, and event listeners.

#### Application Info

**`(get-info)`** / **`(get-info key)`**

Get application information. Without arguments, returns all info.

```clojure
;; Get all info
(app/get-info)

;; Get specific key
(app/get-info "version")
```

**`(get-user-info)`**

Get current user information.

**`(get-user-configs)`**

Get user configuration.

#### Commands

**`(register-command type opts action)`**

Register a custom command.

```clojure
(app/register-command
  "my-command"
  {:key "my-cmd" :label "My Command"}
  (fn [e] (println "Command executed")))
```

**`(register-command-palette opts action)`**

Register a command in the command palette.

```clojure
(app/register-command-palette
  {:key "my-cmd" :label "Do Something" :keybinding {:binding "mod+shift+x"}}
  (fn [] (println "Palette command")))
```

**`(register-command-shortcut keybinding action)`**
**`(register-command-shortcut keybinding action opts)`**

Register a keyboard shortcut.

```clojure
(app/register-command-shortcut
  "mod+shift+d"
  (fn [] (println "Shortcut pressed")))
```

**`(invoke-external-command type & args)`**

Invoke registered palette commands.

**`(invoke-external-plugin type & args)`**

Call external plugin commands or model methods.

**`(get-external-plugin pid)`**

Get an external plugin instance by ID.

#### State Management

**`(get-state-from-store path)`**

Get state from the app store. Valid paths are defined in [Logseq's state.cljs](https://github.com/logseq/logseq/blob/master/src/main/frontend/state.cljs#L27).

```clojure
(app/get-state-from-store "ui/sidebar-open?")
```

**`(set-state-from-store path value)`**

Set state in the app store.

#### Graph Operations

**`(get-current-graph)`**

Get the current graph information.

**`(check-current-is-db-graph)`**

Check if the current graph is a database graph.

**`(get-current-graph-configs & keys)`**

Get current graph configuration.

**`(set-current-graph-configs configs)`**

Set graph configuration.

**`(get-current-graph-favorites)`**

Get favorite pages in the current graph.

**`(get-current-graph-recent)`**

Get recently accessed pages.

**`(get-current-graph-templates)`**

Get all templates in the current graph.

#### Navigation

**`(push-state k)`**
**`(push-state k params)`**
**`(push-state k params query)`**

Push a new state to the navigation history.

**`(replace-state k)`**
**`(replace-state k params)`**
**`(replace-state k params query)`**

Replace the current navigation state.

#### Templates

**`(get-template name)`**

Get a template by name.

**`(exist-template name)`**

Check if a template exists.

**`(create-template target name)`**
**`(create-template target name opts)`**

Create a new template.

**`(remove-template name)`**

Delete a template.

**`(insert-template target name)`**

Insert a template at the target location.

#### UI Control

**`(set-zoom-factor factor)`**

Set the zoom level (e.g., 1.0, 1.5, 2.0).

**`(set-full-screen flag)`**

Toggle fullscreen mode.

**`(set-left-sidebar-visible flag)`** / **`(set-right-sidebar-visible flag)`**

Control sidebar visibility.

**`(clear-right-sidebar-blocks)`** / **`(clear-right-sidebar-blocks opts)`**

Clear blocks from the right sidebar.

**`(register-ui-item type opts)`**

Register a custom UI item.

**`(register-page-menu-item tag action)`**

Add an item to the page context menu.

#### Event Listeners

**`(on-current-graph-changed callback)`**

Listen for graph changes.

```clojure
(app/on-current-graph-changed
  (fn [graph]
    (println "Graph changed:" (:name graph))))
```

**`(on-graph-after-indexed callback)`**

Triggered after graph indexing completes.

**`(on-theme-mode-changed callback)`**

Listen for theme mode changes (light/dark).

**`(on-theme-changed callback)`**

Listen for theme changes.

**`(on-today-journal-created callback)`**

Triggered when today's journal page is created.

**`(on-before-command-invoked condition callback)`** / **`(on-after-command-invoked condition callback)`**

Listen for command invocation events.

**`(on-block-renderer-slotted condition callback)`**

Provide UI slot for specific block UUIDs.

**`(on-macro-renderer-slotted callback)`**

Provide UI slot for `{{renderer}}` macros.

**`(on-page-head-actions-slotted callback)`**

Add actions to page headers.

**`(on-route-changed callback)`**

Listen for route navigation changes.

**`(on-sidebar-visible-changed callback)`**

Listen for sidebar visibility changes.

#### System Operations

**`(relaunch)`** / **`(quit)`**

Restart or quit the application.

**`(open-external-link url)`**

Open a URL in the default browser.

**`(exec-git-command args)`**

Execute a git command (legacy - prefer `com.logseq.git` namespace).

**`(register-search-service s)`**

Register a custom search service.

---

### com.logseq.editor

Block and page manipulation, content editing, properties, and navigation.

#### Context Menu Registration

**`(register-slash-command tag action)`**

Register a custom slash command.

```clojure
(editor/register-slash-command
  "insert-date"
  (fn []
    (let [date (js/Date.)]
      (editor/insert-at-editing-cursor (str date)))))
```

**`(register-block-context-menu-item label action)`**

Add an item to the block context menu (right-click on block dot).

```clojure
(editor/register-block-context-menu-item
  "Copy UUID"
  (fn [block]
    (println "Block UUID:" (:uuid block))))
```

**`(register-highlight-context-menu-item label action)`**
**`(register-highlight-context-menu-item label action opts)`**

Add an item to the highlight context menu (currently PDF viewer only).

#### Editing Operations

**`(check-editing)`**

Check if a block is currently being edited. Returns the block or nil.

**`(insert-at-editing-cursor content)`**

Insert content at the current cursor position.

**`(restore-editing-cursor)`**

Restore the editing cursor to its previous position.

**`(exit-editing-mode)`** / **`(exit-editing-mode select-block)`**

Exit editing mode, optionally selecting the block.

**`(get-editing-cursor-position)`**

Get the current cursor position in the editor.

**`(get-editing-block-content)`**

Get the content of the currently editing block.

#### Current Context

**`(get-current-page)`**

Get the current page object.

**`(get-current-block)`**

Get the currently focused block.

**`(get-selected-blocks)`**

Get all currently selected blocks.

**`(clear-selected-blocks)`**

Clear block selection.

**`(get-current-page-blocks-tree)`**

Get all blocks of the current page as a tree structure.

#### Page Operations

**`(get-page src-page)`** / **`(get-page src-page opts)`**

Get a page by name, UUID, or block entity.

```clojure
;; By name
(editor/get-page "My Page")

;; By UUID
(editor/get-page "123e4567-e89b-12d3-a456-426614174000")
```

**`(get-page-blocks-tree src-page)`**

Get all blocks for a specific page.

**`(get-page-linked-references src-page)`**

Get all pages and blocks that link to this page.

**`(get-pages-from-namespace namespace)`**

Get all pages in a namespace (flattened).

**`(get-pages-tree-from-namespace namespace)`**

Get namespace pages as a tree structure.

**`(create-page page-name)`**
**`(create-page page-name properties)`**
**`(create-page page-name properties opts)`**

Create a new page.

```clojure
(editor/create-page
  "My New Page"
  {:tags ["project" "2024"]
   :status "active"})
```

**`(create-journal-page date)`**

Create a journal page for a specific date.

**`(delete-page page-name)`**

Delete a page.

**`(rename-page old-name new-name)`**

Rename a page.

**`(get-all-pages)`** / **`(get-all-pages repo)`**

Get all pages in the current graph or specific repo.

**`(is-page-block block)`**

Check if a block is a page block.

#### Block Operations

**`(get-block src-block)`** / **`(get-block src-block opts)`**

Get a block by UUID or other identifier.

```clojure
;; Simple get
(editor/get-block "block-uuid")

;; With options
(editor/get-block "block-uuid" {:includeChildren true})
```

**`(insert-block src-block content)`**
**`(insert-block src-block content opts)`**

Insert a new block relative to an existing block.

```clojure
;; Insert after
(editor/insert-block
  "parent-uuid"
  "New block content"
  {:sibling false})  ; false = child, true = sibling
```

**`(insert-batch-block src-block batch)`**
**`(insert-batch-block src-block batch opts)`**

Insert multiple blocks at once.

```clojure
(editor/insert-batch-block
  "parent-uuid"
  [{:content "First block"}
   {:content "Second block"
    :children [{:content "Nested block"}]}])
```

**`(update-block src-block content)`**
**`(update-block src-block content opts)`**

Update a block's content.

**`(remove-block src-block)`**

Delete a block.

**`(move-block src-block target-block)`**
**`(move-block src-block target-block opts)`**

Move a block to a new location.

```clojure
(editor/move-block
  "block-to-move-uuid"
  "target-uuid"
  {:before true})  ; true = before target, false = after
```

**`(new-block-uuid)`**

Generate a new unique block UUID.

**`(set-block-collapsed uuid opts)`**

Collapse or expand a block.

**`(prepend-block-in-page page content)`** / **`(prepend-block-in-page page content opts)`**

Add a block at the beginning of a page.

**`(append-block-in-page page content)`** / **`(append-block-in-page page content opts)`**

Add a block at the end of a page.

**`(get-previous-sibling-block src-block)`** / **`(get-next-sibling-block src-block)`**

Get the previous or next sibling block.

**`(edit-block src-block)`** / **`(edit-block src-block opts)`**

Put a block into editing mode.

**`(select-block src-block)`**

Select a block.

**`(scroll-to-block-in-page page-name block-id)`**
**`(scroll-to-block-in-page page-name block-id opts)`**

Scroll to a specific block on a page.

**`(open-in-right-sidebar id)`**

Open a page or block in the right sidebar.

#### Properties

**`(get-property key)`**

Get a property definition by key.

**`(upsert-property key)`**
**`(upsert-property key schema)`**
**`(upsert-property key schema opts)`**

Create or update a property definition.

```clojure
(editor/upsert-property
  "status"
  {:type "string"
   :values ["todo" "in-progress" "done"]})
```

**`(remove-property key)`**

Delete a property definition.

**`(upsert-block-property block key value)`**

Set a property on a block.

```clojure
(editor/upsert-block-property
  "block-uuid"
  "priority"
  "high")
```

**`(remove-block-property block key)`**

Remove a property from a block.

**`(get-block-property block key)`**

Get a specific property value from a block.

**`(get-block-properties block)`**

Get all properties of a block.

**`(get-page-properties page)`**

Get all properties of a page.

#### Tags

**`(get-all-tags)`**

Get all tags in the current graph.

**`(get-all-properties)`**

Get all property definitions.

**`(get-tag-objects page-identity)`**

Get tag objects by page identity (UUID, db/ident, or title).

#### Advanced Editor

**`(save-focused-code-editor-content)`**

Save the content of a focused code editor block.

**`(on-input-selection-end callback)`**

Listen for text selection end events.

---

### com.logseq.db

Database queries and transaction subscriptions.

**`(q dsl)`**

Run a DSL (simple) query.

```clojure
(db/q
  {:query '[:find ?b
            :where [?b :block/content ?c]
            [(clojure.string/includes? ?c "TODO")]]})
```

**`(datascript-query query & inputs)`**

Run a Datalog query with optional inputs.

```clojure
(db/datascript-query
  '[:find ?title
    :in $ ?tag
    :where
    [?p :block/name ?title]
    [?p :block/tags ?t]
    [?t :block/name ?tag]]
  "project")
```

**`(on-changed callback)`**

Hook all database transaction data.

```clojure
(db/on-changed
  (fn [tx-data]
    (println "Database changed:" (count (:tx-data tx-data)) "updates")))
```

**`(on-block-changed uuid callback)`**

Subscribe to changes for a specific block.

```clojure
(db/on-block-changed
  "block-uuid"
  (fn [block tx-data]
    (println "Block updated:" (:content block))))
```

---

### com.logseq.ui

User interface utilities for messages, element queries, and theming.

**`(show-msg content)`**
**`(show-msg content status)`**
**`(show-msg content status opts)`**

Show a notification message.

```clojure
;; Simple message
(ui/show-msg "Hello!")

;; With status
(ui/show-msg "Operation successful" "success")

;; With options
(ui/show-msg
  "Processing..."
  "warning"
  {:key "process-msg" :timeout 5000})
```

Status values: `"success"`, `"warning"`, `"error"`, `"info"`

**`(close-msg key)`**

Close a specific message by key.

**`(query-element-rect selector)`**

Get the bounding rectangle of a DOM element by CSS selector.

**`(query-element-by-id id)`**

Query a DOM element by ID.

**`(check-slot-valid slot)`**

Check if a UI slot is valid and available.

**`(resolve-theme-css-props-vals props)`**

Resolve theme CSS property values.

---

### com.logseq.assets

Asset file management and storage.

**`(list-files-of-current-graph)`**
**`(list-files-of-current-graph exts)`**

List all files in the current graph, optionally filtered by extensions.

```clojure
;; All files
(assets/list-files-of-current-graph)

;; PDFs only
(assets/list-files-of-current-graph ["pdf"])

;; Images
(assets/list-files-of-current-graph ["png" "jpg" "jpeg" "gif"])
```

**`(make-sandbox-storage)`**

Create a sandboxed storage instance for the plugin.

**`(make-url path)`**

Convert a relative path to an assets URL for the current graph.

**`(built-in-open path)`**

Open an asset file using Logseq's built-in viewers.

---

### com.logseq.git

Git operations for version control.

**`(exec-command args)`**

Execute a git command.

```clojure
(git/exec-command ["status"])
(git/exec-command ["add" "."])
(git/exec-command ["commit" "-m" "Update"])
```

**`(load-ignore-file)`**

Load the .gitignore file content.

**`(save-ignore-file content)`**

Save content to the .gitignore file.

---

## Usage Examples

### Example 1: Simple Plugin Setup

```clojure
(ns my-plugin.core
  (:require [com.logseq.core :as logseq]
            [com.logseq.app :as app]
            [com.logseq.editor :as editor]))

(defn main []
  ;; Initialize plugin
  (-> (logseq/ready)
      (.then (fn []
               (println "Plugin loaded!")

               ;; Register a slash command
               (editor/register-slash-command
                 "hello"
                 (fn []
                   (editor/insert-at-editing-cursor "Hello from ClojureScript!")))

               ;; Register a keyboard shortcut
               (app/register-command-shortcut
                 "mod+shift+h"
                 (fn []
                   (js/alert "Shortcut triggered!")))))))

;; Start the plugin
(main)
```

### Example 2: Working with Blocks

```clojure
(ns my-plugin.blocks
  (:require [com.logseq.editor :as editor]
            [com.logseq.ui :as ui]))

(defn create-todo-block []
  (-> (editor/get-current-block)
      (.then (fn [current-block]
               (when current-block
                 (editor/insert-block
                   (:uuid current-block)
                   "TODO New task"
                   {:sibling true}))))
      (.then (fn [new-block]
               (ui/show-msg "Task created!" "success")))
      (.catch (fn [error]
                (ui/show-msg (str "Error: " error) "error")))))

(defn update-block-priority [block-uuid priority]
  (-> (editor/upsert-block-property block-uuid "priority" priority)
      (.then (fn []
               (ui/show-msg "Priority updated" "success")))))

(defn get-all-todos []
  (-> (editor/get-all-pages)
      (.then (fn [pages]
               (->> pages
                    (filter (fn [page]
                              (re-find #"TODO" (or (:content page) ""))))
                    (take 10))))))
```

### Example 3: Database Queries

```clojure
(ns my-plugin.queries
  (:require [com.logseq.db :as db]
            [com.logseq.editor :as editor]))

(defn find-blocks-with-tag [tag]
  (db/datascript-query
    '[:find ?content ?uuid
      :in $ ?tag-name
      :where
      [?b :block/content ?content]
      [?b :block/uuid ?uuid]
      [?b :block/tags ?t]
      [?t :block/name ?tag-name]]
    tag))

(defn find-todos []
  (db/q
    '{:query [:find (pull ?b [*])
              :where
              [?b :block/marker ?marker]
              [(contains? #{"TODO" "DOING"} ?marker)]]
      :view (fn [results]
              (sort-by :block/priority results))}))

(defn watch-block-changes [block-uuid callback]
  (db/on-block-changed
    block-uuid
    (fn [block tx-data]
      (callback block))))
```

### Example 4: Settings Management

```clojure
(ns my-plugin.settings
  (:require [com.logseq.core :as logseq]
            [com.logseq.ui :as ui]))

(def settings-schema
  [{:key "api-endpoint"
    :type "string"
    :title "API Endpoint"
    :description "The backend API URL"
    :default "https://api.example.com"}
   {:key "auto-sync"
    :type "boolean"
    :title "Auto Sync"
    :description "Automatically sync data"
    :default true}
   {:key "sync-interval"
    :type "number"
    :title "Sync Interval (minutes)"
    :default 5}])

(defn init-settings []
  (logseq/use-settings-schema settings-schema)

  (logseq/on-settings-changed
    (fn [old-settings new-settings]
      (ui/show-msg "Settings updated" "info")
      (when (not= (:auto-sync old-settings)
                  (:auto-sync new-settings))
        (if (:auto-sync new-settings)
          (start-sync-timer)
          (stop-sync-timer))))))

(defn get-setting [key]
  (aget js/logseq.settings (name key)))
```

### Example 5: Event-Driven Plugin

```clojure
(ns my-plugin.events
  (:require [com.logseq.app :as app]
            [com.logseq.editor :as editor]
            [com.logseq.db :as db]
            [com.logseq.ui :as ui]))

(defn setup-event-listeners []
  ;; Listen for graph changes
  (app/on-current-graph-changed
    (fn [graph]
      (ui/show-msg (str "Switched to graph: " (:name graph)) "info")))

  ;; Listen for theme changes
  (app/on-theme-mode-changed
    (fn [mode]
      (println "Theme mode changed to:" mode)))

  ;; Listen for all database changes
  (db/on-changed
    (fn [tx-data]
      (println "Database updated with" (count (:tx-data tx-data)) "changes")))

  ;; Listen for today's journal creation
  (app/on-today-journal-created
    (fn [page]
      (editor/append-block-in-page
        (:name page)
        "## Daily Goals"
        {:properties {:type "goals"}}))))
```

### Example 6: Custom UI Integration

```clojure
(ns my-plugin.ui
  (:require [com.logseq.core :as logseq]
            [com.logseq.app :as app]
            [reagent.core :as r]))

(defn my-ui-component []
  (let [counter (r/atom 0)]
    (fn []
      [:div
       [:h2 "My Plugin UI"]
       [:p "Count: " @counter]
       [:button {:on-click #(swap! counter inc)}
        "Increment"]])))

(defn setup-ui []
  ;; Provide UI component
  (logseq/provide-ui
    {:key "my-plugin-main"
     :slot "main"
     :reset true
     :template (r/as-element [my-ui-component])})

  ;; Register toolbar icon
  (app/register-ui-item
    "toolbar"
    {:key "my-plugin-icon"
     :template [:a {:on-click #(logseq/show-main-ui)}
                [:i.ti.ti-plugin]]}))

(defn inject-custom-styles []
  (logseq/provide-style
    ".my-plugin-container {
       background: var(--ls-primary-background-color);
       padding: 1rem;
       border-radius: 8px;
     }"))
```

## Best Practices

### 1. Error Handling

Always handle promise rejections:

```clojure
(-> (editor/get-block "uuid")
    (.then (fn [block]
             (process-block block)))
    (.catch (fn [error]
              (ui/show-msg (str "Error: " (.-message error)) "error")
              (js/console.error error))))
```

### 2. Data Structure Patterns

Use idiomatic ClojureScript patterns:

```clojure
;; Good: Use destructuring
(defn process-block [{:keys [uuid content properties]}]
  (when (contains? properties :priority)
    (handle-priority uuid (:priority properties))))

;; Good: Use threading macros
(defn get-high-priority-todos []
  (->> (get-all-blocks)
       (filter #(= "high" (get-in % [:properties :priority])))
       (map :content)
       (take 10)))
```

### 3. Lifecycle Management

Properly clean up resources:

```clojure
(defonce *listeners (atom []))

(defn register-listener [listener-fn]
  (swap! *listeners conj listener-fn))

(defn cleanup []
  (logseq/beforeunload
    (fn []
      (doseq [listener @*listeners]
        (listener))
      (reset! *listeners []))))
```

### 4. Asynchronous Patterns

Use `core.async` for complex async flows:

```clojure
(require '[cljs.core.async :refer [go <!]])
(require '[cljs.core.async.interop :refer [<p!]])

(defn async-workflow []
  (go
    (try
      (let [page (<p! (editor/get-current-page))
            blocks (<p! (editor/get-page-blocks-tree (:name page)))
            processed (process-blocks blocks)]
        (<p! (save-results processed))
        (ui/show-msg "Workflow complete!" "success"))
      (catch js/Error e
        (ui/show-msg (str "Error: " (.-message e)) "error")))))
```

### 5. Performance Optimization

Batch operations when possible:

```clojure
;; Good: Batch insert
(editor/insert-batch-block
  "parent-uuid"
  (map (fn [content] {:content content})
       ["Block 1" "Block 2" "Block 3"]))

;; Avoid: Multiple single inserts
;; (doseq [content ["Block 1" "Block 2" "Block 3"]]
;;   (editor/insert-block "parent-uuid" content))
```

### 6. Type Conversion Awareness

Be aware of automatic type conversion:

```clojure
;; Keywords are preserved in ClojureScript data
{:priority "high" :tags ["urgent" "review"]}

;; But converted to strings when sent to JS
;; Use string keys when interfacing with JS APIs that expect strings
{"priority" "high" "tags" ["urgent" "review"]}
```

## Development Workflow

### Building the SDK

The ClojureScript SDK is auto-generated from the JavaScript SDK schema:

```bash
# From the libs directory
cd /path/to/logseq/libs

# Generate the SDK schema
yarn run generate:schema

# Generate ClojureScript wrappers
bb libs:generate-cljs-sdk
```

### Project Structure

```
libs/
├── cljs-sdk/
│   ├── src/com/logseq/
│   │   ├── core.cljs       # Core lifecycle & UI
│   │   ├── app.cljs        # Application APIs
│   │   ├── editor.cljs     # Editor & blocks
│   │   ├── db.cljs         # Database queries
│   │   ├── ui.cljs         # UI utilities
│   │   ├── assets.cljs     # Asset management
│   │   ├── git.cljs        # Git operations
│   │   └── util.cljs       # Data conversion
│   ├── deps.edn
│   ├── build.clj
│   └── package.json
├── scripts/
│   ├── extract-sdk-schema.js
│   └── logseq/libs/sdk_generator.clj
└── README.md
```

### Testing

```clojure
;; Example test setup
(ns my-plugin.core-test
  (:require [cljs.test :refer-macros [deftest is testing]]
            [my-plugin.core :as core]))

(deftest test-plugin-init
  (testing "Plugin initialization"
    (is (some? (core/init)))))
```

## Troubleshooting

### Common Issues

**1. Data conversion issues**

If you see unexpected data types:
- Check if keywords are being properly converted
- Use `js/console.log` to inspect data at boundaries
- Ensure you're using ClojureScript data structures internally

**2. Promise handling**

If promises aren't resolving:
- Make sure to chain `.then` calls properly
- Always include `.catch` for error handling
- Check browser console for rejection errors

**3. Event listener cleanup**

If listeners persist after plugin reload:
- Always use `beforeunload` to clean up
- Store listener references for later removal
- Test plugin reload behavior

### Debugging Tips

1. **Enable verbose logging:**
   ```clojure
   (set! js/console.debug (.-log js/console))
   ```

2. **Inspect API proxy:**
   ```clojure
   (js/console.dir js/logseq)
   ```

3. **Check data conversion:**
   ```clojure
   (require '[cljs-bean.core :as bean])

   ;; Inspect JS→CLJS conversion
   (let [js-data (js-obj "key" "value")
         clj-data (bean/->clj js-data)]
     (println clj-data))
   ```

## Additional Resources

- [Logseq Plugin API Documentation](https://logseq.github.io/plugins/)
- [Plugin Samples Repository](https://github.com/logseq/logseq-plugin-samples)
- [ClojureScript Playground Template](https://github.com/tiensonqin/logseq-cljs-playground)
- [Logseq Discord Community](https://discord.gg/KpN4eHY)

## Contributing

The SDK is auto-generated, but improvements to the generator or documentation are welcome:

1. Fork the repository
2. Make changes to the generator in `scripts/logseq/libs/sdk_generator.clj`
3. Regenerate the SDK with `bb libs:generate-cljs-sdk`
4. Test your changes
5. Submit a pull request

## License

MIT License - see the Logseq repository for full details.

---

**Last Updated:** October 2025
**SDK Version:** 0.0.10
**Generated from:** PR #12168, commit d8809f0b7
