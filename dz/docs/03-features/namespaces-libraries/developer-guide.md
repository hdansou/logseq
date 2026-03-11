# Library and Namespace: A Detective's Investigation Report

> **Last revised:** 2026-03-10

## Case Summary

**Case File**: Logseq Library and Namespace System
**Jurisdiction**: DB and File-based Graph Management
**Status**: Active, Production-Ready
**Severity**: Medium - Important for organization and hierarchy

### What are these components?

The **Library and Namespace** features are two distinct but related organizational systems in Logseq:

1. **Library Page (DB Graphs Only)**: A special built-in page that serves as a container for reusable blocks, pages, and templates. It acts as a centralized repository for content that can be shared across different parts of a graph.

2. **Namespace Pages (Primarily File Graphs)**: A hierarchical page organization system where pages can be organized using "/" separators (e.g., "Projects/Work", "Projects/Personal"), creating parent-child relationships.

### Why do they exist?

**Library Page**:

- Provides a dedicated space for reusable content like templates, code snippets, and frequently-referenced blocks
- Replaces the old `{{namespace}}` macro functionality from file-based graphs
- Allows users to organize shared content separate from their main graph structure
- Supports the "Add pages to Library" workflow for moving existing pages into the Library

**Namespace Pages**:

- Enables hierarchical organization of pages (e.g., "Programming/JavaScript" is a child of "Programming")
- Provides automatic grouping and navigation for related pages
- Supports namespace-aware search and queries
- Facilitates bulk operations (renaming a namespace renames all children)

### Key Discovery Highlights

📦 **Library Page** (DB Graphs):

- Built-in page with UUID: `00000004-1294-7765-6000-000000000000`
- Marked as `logseq.property/built-in? true`
- Pages can be added to Library via UI or by setting `:block/parent` relationship
- Filtered from page hierarchy displays to reduce noise

🌳 **Namespace Pages** (File & DB Graphs):

- Identified by "/" character in page name (e.g., "parent/child")
- File graphs: Uses `:block/namespace` attribute to track hierarchy
- DB graphs: Uses `:block/parent` for page hierarchy
- Supports nested hierarchies (e.g., "a/b/c" where "c" is child of "a/b", which is child of "a")

🔄 **Migration Path**:

- Old `:logseq.property/parent` renamed to `:logseq.property.class/extends` (for classes) or `:block/parent` (for pages)
- Top-level parent pages automatically moved into Library page during migration

---

## 📋 Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [The Crime Scene Map](#the-crime-scene-map)
3. [Evidence #1: Configuration and Constants](#evidence-1-configuration-and-constants)
4. [Evidence #2: Library Page (DB Graphs)](#evidence-2-library-page-db-graphs)
5. [Evidence #3: Namespace Pages (File Graphs)](#evidence-3-namespace-pages-file-graphs)
6. [Evidence #4: Namespace Pages (DB Graphs)](#evidence-4-namespace-pages-db-graphs)
7. [Evidence #5: User Interface Components](#evidence-5-user-interface-components)
8. [Evidence #6: Search Integration](#evidence-6-search-integration)
9. [Evidence #7: Migration and Data Transformation](#evidence-7-migration-and-data-transformation)
10. [Evidence #8: Rename Operations](#evidence-8-rename-operations)
11. [Evidence #9: Public API](#evidence-9-public-api)
12. [The Verdict: Key Insights](#the-verdict-key-insights)
13. [For Developers: Working with Library and Namespace](#for-developers-working-with-library-and-namespace)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Library & Namespace System                    │
└─────────────────────────────────────────────────────────────────┘
                                 │
                ┌────────────────┴────────────────┐
                │                                 │
        ┌───────▼────────┐              ┌────────▼─────────┐
        │  Library Page  │              │ Namespace Pages  │
        │  (DB Graphs)   │              │ (File & DB)      │
        └───────┬────────┘              └────────┬─────────┘
                │                                 │
    ┌───────────┴───────────┐        ┌───────────┴───────────┐
    │                       │        │                       │
┌───▼────┐          ┌──────▼───┐  ┌─▼──────┐        ┌──────▼────┐
│Built-in│          │Parent    │  │File    │        │DB         │
│Page    │          │Child     │  │Graph   │        │Graph      │
│Storage │          │Relations │  │:block/ │        │:block/    │
│        │          │          │  │namespace│       │parent     │
└────────┘          └──────────┘  └────────┘        └───────────┘
```

### System Components

1. **Configuration Layer** (`logseq.common.config`):

   - Defines `library-page-name` constant: "Library"
   - Shared between frontend and backend

2. **Utility Layer** (`logseq.common.util.namespace`):

   - Detects namespace pages via "/" character
   - Extracts last part of namespace (e.g., "c" from "a/b/c")
   - Validates namespace page names (excludes URLs, relative paths)

3. **Database Layer**:

   - **DB Graphs**: Library page stored with special UUID, uses `:block/parent` for hierarchy
   - **File Graphs**: Namespace stored in `:block/namespace` attribute, uses datalog rules for queries

4. **UI Layer**:

   - Library page UI for adding/managing pages
   - Hierarchy display for namespace navigation
   - Search integration with namespace awareness

5. **Migration Layer**:
   - Converts old parent system to new extends/parent system
   - Creates Library page if missing
   - Moves top-level parents into Library

---

## The Crime Scene Map

### File Structure

```
logseq/
├── deps/
│   ├── common/
│   │   └── src/logseq/common/
│   │       ├── config.cljs ..................... Library page name constant
│   │       └── util/namespace.cljs ............. Namespace detection utilities
│   └── db/
│       └── src/logseq/db/
│           ├── frontend/
│           │   ├── db.cljs ..................... Library page detection, parent hierarchy
│           │   ├── class.cljs .................. Built-in classes (including Page, Tag)
│           │   └── schema.cljs ................. DB schema definitions
│           └── sqlite/
│               ├── util.cljs ................... Page building utilities
│               └── create_graph.cljs ........... Graph creation with Library page
├── src/
│   ├── main/
│   │   ├── frontend/
│   │   │   ├── components/
│   │   │   │   ├── library.cljs ................ Library page UI components
│   │   │   │   ├── file_based/hierarchy.cljs ... Namespace hierarchy display
│   │   │   │   └── block.cljs .................. Block rendering (namespace macro deprecation)
│   │   │   ├── common/file_based/
│   │   │   │   └── db.cljs ..................... File-based namespace queries
│   │   │   ├── db/
│   │   │   │   └── file_based/model.cljs ....... File-based namespace model functions
│   │   │   ├── worker/
│   │   │   │   ├── db/migrate.cljs ............. Migration logic (parent → extends/parent)
│   │   │   │   ├── handler/page/file_based/
│   │   │   │   │   └── rename.cljs ............. Namespace rename operations
│   │   │   │   └── search.cljs ................. Search with namespace awareness
│   │   │   └── handler/
│   │   │       ├── editor.cljs ................. Block move operations to Library
│   │   │       └── page.cljs ................... Page handling
│   │   └── logseq/
│   │       └── api.cljs ........................ Public plugin API (get_pages_from_namespace)
│   └── test/
│       ├── frontend/
│       │   ├── db/file_based/model_test.cljs ... Namespace function tests
│       │   └── worker/migrate_test.cljs ........ Migration tests (Library creation)
│       └── logseq/db/
│           └── [various test files] ............ Database function tests
└── logseq.db.cljs ............................. Main DB API wrapper
```

### Data Flow

#### Library Page (DB Graphs)

```
User Action: "Add pages to Library"
     │
     ├─> frontend.components.library/select-pages
     │   ├─> Search for pages (library-page-search? true)
     │   └─> Display search results
     │
     ├─> User selects pages
     │
     ├─> frontend.handler.editor/move-blocks!
     │   └─> Set :block/parent to Library page
     │
     └─> Pages now children of Library page
```

#### Namespace Pages (File Graphs)

```
User Creates Page: "Projects/Work"
     │
     ├─> logseq.common.util.namespace/namespace-page?
     │   └─> Detects "/" character → true
     │
     ├─> Parse namespace: "Projects" (parent)
     │   └─> Last part: "Work" (child)
     │
     ├─> frontend.worker.handler.page/create!
     │   ├─> Create parent page if doesn't exist
     │   └─> Set :block/namespace to parent page ID
     │
     └─> Page stored with namespace relationship
```

#### Namespace Rename (File Graphs)

```
User Renames: "Projects" → "MyProjects"
     │
     ├─> frontend.worker.handler.page.file-based.rename/rename!
     │   └─> Detect namespace page
     │
     ├─> frontend.common.file-based.db/get-namespace-pages
     │   └─> Find all children: ["Projects/Work", "Projects/Personal"]
     │
     ├─> For each child page:
     │   ├─> Replace "Projects" with "MyProjects" in title
     │   ├─> Update :block/namespace reference
     │   └─> Update all block references in graph
     │
     └─> All pages renamed: ["MyProjects/Work", "MyProjects/Personal"]
```

---

## Evidence #1: Configuration and Constants

### Location: `deps/common/src/logseq/common/config.cljs`

**The Constant Definition**:

```clojure
(defonce library-page-name "Library")
```

**Lines**: 41

**Context**: Defined alongside other special page names like:

- `favorites-page-name` = "$$$favorites"
- `views-page-name` = "$$$views"
- `quick-add-page-name` = "Quick add"

**Key Insight**: The Library page is a first-class built-in page, on par with system pages like Favorites and Views.

### Location: `deps/common/src/logseq/common/util/namespace.cljs`

**Namespace Detection Function**:

```clojure
(defonce parent-char "/")
(defonce namespace-char "/")

(defn namespace-page?
  "Used by DB and file graphs"
  [page-name]
  (and (string? page-name)
       (string/includes? page-name namespace-char)
       (not= (string/trim page-name) namespace-char)
       (not (string/starts-with? page-name "../"))
       (not (string/starts-with? page-name "./"))
       (not (common-util/url? page-name))))

(defn get-last-part
  "Get last part of a namespace page"
  [page-name]
  (if (namespace-page? page-name)
    (last (string/split page-name parent-char))
    page-name))
```

**Lines**: 7-27

**Key Insights**:

- Namespace pages identified by "/" character
- Excludes URLs (e.g., "http://example.com")
- Excludes relative paths (e.g., "./folder", "../parent")
- Same "/" character used for both DB and file graphs
- `get-last-part` extracts the final segment (e.g., "child" from "parent/child")

**Evidence Tags**: #configuration #namespace-detection #validation

---

## Evidence #2: Library Page (DB Graphs)

### Location: `deps/db/src/logseq/db/frontend/db.cljs`

**Library Detection Function**:

```clojure
(defn library?
  [page]
  (and (entity-util/built-in? page)
       (= common-config/library-page-name (:block/title page))))
```

**Lines**: 131-134

**Title with Parents Function** (Library Filtering):

```clojure
(defn get-title-with-parents
  [entity]
  (cond
    (entity-util/class? entity)
    (get-class-title-with-extends entity)

    (entity-util/page? entity)
    (let [parents' (->> (get-page-parents entity)
                        (remove (fn [e]
                                  (and (:logseq.property/built-in? e)
                                       (= common-config/library-page-name (:block/title e))))))]
      (string/join
       ns-util/parent-char
       (map :block/title (conj (vec parents') entity))))

    :else
    (:block/title entity)))
```

**Lines**: 67-82

**Key Insight**: When displaying page titles with parent hierarchy, the Library page is **explicitly filtered out** to avoid cluttering the display. For example, if page "Templates/Meeting" has parent "Templates" which has parent "Library", the display shows only "Templates/Meeting" (not "Library/Templates/Meeting").

### Location: `logseq.db.cljs`

**Public API Functions**:

```clojure
(def library? db-db/library?)

(defn get-library-page
  [db]
  (get-built-in-page db common-config/library-page-name))

(defn page-in-library?
  "Check whether a `page` exists on the Library page"
  [db page]
  (when (page? page)
    (when-let [library-page (get-built-in-page db common-config/library-page-name)]
      (loop [parent (:block/parent page)]
        (cond
          (nil? parent)
          false
          (= (:db/id parent) (:db/id library-page))
          true
          :else
          (recur (:block/parent parent)))))))
```

**Lines**: 277-280, 585-597

**Key Insights**:

- `get-library-page`: Retrieves Library page by name (uses built-in UUID)
- `library?`: Checks if a page entity is the Library page
- `page-in-library?`: Recursively walks up parent chain to check if page is descendant of Library

**Library Page UUID**: `00000004-1294-7765-6000-000000000000`
(Defined in `common.uuid/gen-uuid` with `:builtin-block-uuid` and "Library" as inputs)

### Location: `src/main/frontend/components/library.cljs`

**UI Component for Adding Pages**:

```clojure
(rum/defc select-pages
  [library-page]
  (let [[result set-result!] (hooks/use-state nil)
        [input set-input!] (hooks/use-state "")
        [selected-choices set-selected-choices!] (hooks/use-state #{})
        items (map (fn [block]
                     {:value (:db/id block)
                      :label (:block/title block)})
                   result)]
    (hooks/use-effect!
     (fn []
       (if (string/blank? input)
         (set-result! nil)
         (p/let [result (search/block-search (state/get-current-repo) input
                                              {:enable-snippet? false
                                               :built-in? false
                                               :page-only? true
                                               :library-page-search? true})]
           (set-result! result))))
     [(hooks/use-debounced-value input 200)])
    (components-select/select
     {:items items
      :on-chosen (fn [chosen selected?]
                   (if selected?
                     (let [chosen-block (db/entity chosen)]
                       (editor-handler/move-blocks! [chosen-block] library-page {:bottom? true})
                       (set-selected-choices! (conj selected-choices chosen)))
                     (do
                       (db/transact! (state/get-current-repo)
                                     [[:db/retract chosen :block/parent]]
                                     {:outliner-op :save-block})
                       (set-selected-choices! (disj selected-choices chosen)))))})))

(rum/defc add-pages
  [library-page]
  [:div.ls-add-pages.px-1.mt-4
   (shui/button
    {:on-click (fn [e]
                 (shui/popup-show!
                  (.-target e)
                  (fn [] [:div {:style {:min-height 120}}
                          (select-pages library-page)])
                  {:align :start}))}
    (ui/icon "plus" {:size 16})
    "Add existing pages to Library")])
```

**Lines**: 15-71

**Workflow**:

1. User clicks "Add existing pages to Library" button
2. Search popup opens with `library-page-search? true` flag
3. User searches and selects pages
4. Selected pages moved to Library via `editor-handler/move-blocks!`
5. Pages now have `:block/parent` pointing to Library page
6. To remove from Library: retract `:block/parent` attribute

**Evidence Tags**: #library-page #db-graphs #built-in-page #hierarchy

---

## Evidence #3: Namespace Pages (File Graphs)

### Location: `src/main/frontend/common/file_based/db.cljs`

**Namespace Query Function**:

```clojure
(defn get-namespace-pages
  "Accepts both sanitized and unsanitized namespaces"
  [db namespace']
  (assert (string? namespace'))
  (let [namespace'' (common-util/page-name-sanity-lc namespace')
        pull-attrs [:db/id :block/name :block/original-name :block/namespace
                    {:block/file [:db/id :file/path]}]]
    (d/q
     [:find [(list 'pull '?c pull-attrs) '...]
      :in '$ '% '?namespace
      :where
      ['?p :block/name '?namespace]
      (list 'namespace '?p '?c)]
     db
     (:namespace file-rules/rules)
     namespace'')))
```

**Lines**: 8-23

**Key Insights**:

- Uses datalog query with custom `:namespace` rule
- Accepts case-insensitive namespace names
- Returns all child pages with their `:block/namespace` attribute
- Returns file path for each page

**Datalog Rule** (from `logseq.db.file-based.rules`):

```clojure
:namespace
[[(namespace ?parent ?child)
  [?child :block/namespace ?parent]]]
```

This recursive rule finds all pages where `:block/namespace` points to the parent page.

### Location: `src/main/frontend/db/file_based/model.cljs`

**Namespace Model Functions**:

```clojure
(defn get-namespace-pages
  "Accepts both sanitized and unsanitized namespaces"
  [repo namespace]
  (common-file-db/get-namespace-pages (conn/get-db repo) namespace))

(defn- tree [flat-col root]
  (let [sort-fn #(sort-by :block/name %)
        children (group-by :block/namespace flat-col)
        namespace-children (fn namespace-children [parent-id]
                             (map (fn [m]
                                    (assoc m :namespace/children
                                           (sort-fn (namespace-children {:db/id (:db/id m)}))))
                                  (sort-fn (get children parent-id))))]
    (namespace-children root)))

(defn get-namespace-hierarchy
  "Unsanitized namespaces"
  [repo namespace]
  (let [children (get-namespace-pages repo namespace)
        namespace-id (:db/id (db-utils/entity [:block/name (util/page-name-sanity-lc namespace)]))
        root {:db/id namespace-id}
        col (conj children root)]
    (tree col root)))

(defn get-page-namespace
  [repo page]
  (:block/namespace (db-utils/entity repo [:block/name (util/page-name-sanity-lc page)])))

(defn get-page-namespace-routes
  [repo page]
  (assert (string? page))
  (when-let [db (conn/get-db repo)]
    (when-not (string/blank? page)
      (let [page (util/page-name-sanity-lc (string/trim page))
            page-exist? (db-utils/entity repo [:block/name page])
            ids (if page-exist?
                  '()
                  (->> (d/datoms db :aevt :block/name)
                       (filter (fn [datom]
                                 (string/ends-with? (:v datom) (str "/" page))))
                       (map :e)))]
        (when (seq ids)
          (db-utils/pull-many repo
                              '[:db/id :block/name :block/title
                                {:block/file [:db/id :file/path]}]
                              ids))))))
```

**Lines**: 176-221

**Key Functions**:

1. **`get-namespace-pages`**: Wrapper for `common-file-db/get-namespace-pages`
2. **`get-namespace-hierarchy`**: Builds tree structure of namespace pages
   - Example: "a" → ["a/b", "a/c", "a/b/d"]
   - Returns nested structure with `:namespace/children` key
3. **`get-page-namespace`**: Gets parent namespace of a page
4. **`get-page-namespace-routes`**: Finds all namespace paths ending with page name
   - Example: If page "work" doesn't exist, finds ["projects/work", "archive/work"]

**Data Structure Example**:

```clojure
{:db/id 123
 :block/name "projects"
 :block/title "Projects"
 :namespace/children
 [{:db/id 456
   :block/name "projects/work"
   :block/title "Projects/Work"
   :block/namespace {:db/id 123}
   :namespace/children
   [{:db/id 789
     :block/name "projects/work/client-a"
     :block/title "Projects/Work/Client A"
     :block/namespace {:db/id 456}
     :namespace/children []}]}
  {:db/id 555
   :block/name "projects/personal"
   :block/title "Projects/Personal"
   :block/namespace {:db/id 123}
   :namespace/children []}]}
```

### Location: `src/main/frontend/worker/handler/page/file_based/rename.cljs`

**Namespace Update on Rename**:

```clojure
(defn rename-update-namespace!
  "update :block/namespace of the renamed block"
  [repo conn config page old-title new-name]
  (let [old-namespace? (text/namespace-page? old-title)
        new-namespace? (text/namespace-page? new-name)]
    (cond
      new-namespace?
      ;; update namespace
      (let [namespace (first (common-util/split-last "/" new-name))]
        (when namespace
          (worker-page/create! repo conn config namespace) ;; create parent page if not exist
          (let [namespace-block (d/entity @conn [:block/name (common-util/page-name-sanity-lc namespace)])
                page-txs [{:db/id (:db/id page)
                           :block/namespace (:db/id namespace-block)}]]
            (ldb/transact! conn page-txs {:persist-op? true}))))

      old-namespace?
      ;; retract namespace
      (ldb/transact! conn [[:db/retract (:db/id page) :block/namespace]] {:persist-op? true})

      :else
      nil)))
```

**Lines**: 140-161

**Key Insights**:

1. **Renaming to namespace page**: Creates parent if doesn't exist, sets `:block/namespace`
2. **Renaming from namespace page**: Retracts `:block/namespace` attribute
3. **Example**: Renaming "Work" → "Projects/Work"
   - Creates "Projects" page if it doesn't exist
   - Sets "Projects/Work" `:block/namespace` to "Projects" page ID

**Evidence Tags**: #namespace-pages #file-graphs #datalog-queries #hierarchy

---

## Evidence #4: Namespace Pages (DB Graphs)

### Location: `deps/db/src/logseq/db/frontend/db.cljs`

**Page Parents Function**:

```clojure
(defn get-page-parents
  [node]
  (when-let [parent (:block/parent node)]
    (loop [current-parent parent
           parents' []]
      (if (and current-parent
               (not (contains? parents' current-parent)))
        (recur (:block/parent current-parent)
               (conj parents' current-parent))
        (vec (reverse parents'))))))
```

**Lines**: 47-56

**Key Insight**: In DB graphs, namespace-like hierarchies are represented using `:block/parent` relationships. This function recursively walks up the parent chain to build the full hierarchy.

**Example**:

```clojure
;; Page: "Client A"
;; :block/parent → "Work"
;; :block/parent → "Projects"
;; :block/parent → "Library"

(get-page-parents client-a-page)
;; => [projects-page work-page]  ; Library filtered out
```

**Class Extends (Similar Concept for Classes)**:

```clojure
(defn get-class-title-with-extends
  [entity]
  (let [parents' (->> (db-class/get-class-extends entity)
                      (remove (fn [e] (= :logseq.class/Root (:db/ident e))))
                      vec)]
    (string/join
     ns-util/parent-char
     (map :block/title (conj parents' entity)))))
```

**Lines**: 58-65

**Key Insight**: Classes use `:logseq.property.class/extends` for inheritance, which is displayed similarly to namespace hierarchy using "/" separator.

**Example**:

```clojure
;; Class: "Meeting Note"
;; :logseq.property.class/extends → "Note"
;; :logseq.property.class/extends → "Document"

(get-class-title-with-extends meeting-note-class)
;; => "Document/Note/Meeting Note"
```

### Location: `logseq.db.cljs`

**Title with Parents Function** (Unified for Page and Class):

```clojure
(def get-title-with-parents db-db/get-title-with-parents)
```

**Line**: 533

This unified function handles both:

- **Classes**: Shows extends hierarchy (e.g., "Document/Note/Meeting Note")
- **Pages**: Shows parent hierarchy (e.g., "Projects/Work/Client A")

**Evidence Tags**: #db-graphs #hierarchy #page-parents #class-extends

---

## Evidence #5: User Interface Components

### Location: `src/main/frontend/components/library.cljs`

**Complete Library UI Implementation**:

```clojure
(ns frontend.components.library
  "Library page"
  (:require [clojure.string :as string]
            [frontend.components.select :as components-select]
            [frontend.db :as db]
            [frontend.handler.editor :as editor-handler]
            [frontend.search :as search]
            [frontend.state :as state]
            [frontend.ui :as ui]
            [logseq.shui.hooks :as hooks]
            [logseq.shui.ui :as shui]
            [promesa.core :as p]
            [rum.core :as rum]))
```

**Key Features**:

1. **`select-pages` component**: Search interface with autocomplete
2. **`add-pages` component**: Button to open page selector
3. **Search integration**: Uses `library-page-search? true` flag
4. **Move operation**: Calls `editor-handler/move-blocks!` to set parent
5. **Remove operation**: Retracts `:block/parent` to unlink from Library

**User Flow**:

```
1. User opens Library page
2. Clicks "Add existing pages to Library" button
3. Search popup appears
4. User types to search (debounced 200ms)
5. Results shown (excluding built-in pages, Library pages)
6. User clicks page to select
7. Page moved to Library (becomes child of Library page)
8. User can click again to deselect (removes from Library)
```

### Location: `src/main/frontend/components/file_based/hierarchy.cljs`

**Namespace Hierarchy Display**:

This component displays the namespace tree structure for file-based graphs, showing parent-child relationships visually.

**Features**:

- Tree view of namespace hierarchy
- Collapsible/expandable nodes
- Links to navigate to namespace pages
- Shows all descendants of a namespace

### Location: `src/main/frontend/components/block.cljs`

**Deprecation Warning** (for old `{{namespace}}` macro):

```clojure
;; Shows deprecation warning for old {{namespace}} macro
;; Suggests using Library page instead
```

**Context**: The `{{namespace}}` macro in file-based graphs has been deprecated in favor of the Library page in DB graphs.

**Evidence Tags**: #ui-components #library-ui #hierarchy-display #user-interaction

---

## Evidence #6: Search Integration

### Location: `src/main/frontend/worker/search.cljs`

**Search with Library Awareness**:

```clojure
(ns frontend.worker.search
  "Full-text and fuzzy search"
  (:require ["fuse.js" :as Fuse]
            ;; ...
            [logseq.common.util.namespace :as ns-util]
            ;; ...
            ))
```

**Lines**: 1-18

**Key Search Features**:

1. **Namespace Detection**: Uses `ns-util/namespace-page?` to identify namespace pages
2. **Library Page Filtering**: Special `library-page-search?` flag filters results
3. **Namespace-aware Matching**: Uses `text/get-namespace-last-part` for substring matching

**Search Configuration**:

```clojure
{:library-page-search? true   ; Include/exclude Library pages
 :built-in? false              ; Exclude built-in pages
 :page-only? true              ; Only search pages (not blocks)
 :enable-snippet? false}       ; Disable snippet generation
```

**Namespace Search Example**:

```clojure
;; User searches for "work"
;; Matches:
;; - "Work" (direct match)
;; - "Projects/Work" (namespace match on last part)
;; - "Archive/Old Work" (namespace match on last part)
```

### Location: `src/main/logseq/api.cljs`

**Plugin API for Namespace Search**:

```clojure
(defn ^:export get_pages_from_namespace
  [namespace]
  (when-not (db-graph?)
    (p/let [pages (file-model/get-namespace-pages (state/get-current-repo) namespace)]
      (bean/->js pages))))

(defn ^:export get_pages_tree_from_namespace
  [namespace]
  (when-not (db-graph?)
    (bean/->js (file-model/get-namespace-hierarchy (state/get-current-repo) namespace))))
```

**Context**: These functions are exposed to plugins for querying namespace hierarchies in file-based graphs.

**Evidence Tags**: #search #namespace-aware-search #library-search #plugin-api

---

## Evidence #7: Migration and Data Transformation

### Location: `src/main/frontend/worker/db/migrate.cljs`

**Critical Migration: `fix-rename-parent-to-extends`**:

```clojure
(defn fix-rename-parent-to-extends
  [db]
  (let [parent-entity (d/entity db :logseq.property/parent)]
    (when parent-entity
      (let [old-p :logseq.property/parent
            new-p :logseq.property.class/extends
            ;; ... rename property ...
            library-page (if-let [page (ldb/get-built-in-page db common-config/library-page-name)]
                           page
                           (-> (sqlite-util/build-new-page common-config/library-page-name)
                               sqlite-create-graph/mark-block-as-built-in))
            library-id (:block/uuid library-page)
            library-page-tx (when-not (de/entity? library-page)
                              [library-page])
            pages-with-parent (->> (d/datoms db :avet :logseq.property/parent)
                                   (keep (fn [d]
                                           (let [e (d/entity db (:e d))]
                                             (when-not (ldb/class? e)
                                               e)))))
            parents (->> pages-with-parent
                         (map :logseq.property/parent)
                         (common-util/distinct-by :db/id))
            top-parents (remove :logseq.property/parent parents)
            top-parent-ids (set (map :db/id top-parents))
            move-top-parents-to-library (map (fn [parent]
                                               {:db/id (:db/id parent)
                                                :block/parent [:block/uuid library-id]
                                                :block/order (db-order/gen-key)}) top-parents)
            update-children-parent-and-order (->> pages-with-parent
                                                  (remove (fn [page] (top-parent-ids (:db/id page))))
                                                  (map (fn [page]
                                                         {:db/id (:db/id page)
                                                          :block/order (db-order/gen-key)})))]
        (concat
         rename-property-tx
         library-page-tx
         move-top-parents-to-library
         update-children-parent-and-order)))))
```

**Lines**: 150-195

**Migration Steps**:

1. **Check for Old Property**: Look for `:logseq.property/parent` attribute
2. **Rename Property**:
   - For classes: `:logseq.property/parent` → `:logseq.property.class/extends`
   - For pages: `:logseq.property/parent` → `:block/parent`
3. **Create Library Page**: If doesn't exist, create with built-in UUID
4. **Identify Top Parents**: Find pages that have parent but are not children themselves
5. **Move to Library**: Set `:block/parent` of all top parents to Library page
6. **Update Orders**: Regenerate `:block/order` for all affected pages

**Example**:

```clojure
;; Before migration:
Page A
  :logseq.property/parent nil

Page B
  :logseq.property/parent → Page A

Page C
  :logseq.property/parent → Page B

;; After migration:
Library (built-in page)

Page A
  :block/parent → Library

Page B
  :block/parent → Page A

Page C
  :block/parent → Page B
```

### Schema Version Updates:

```clojure
(def schema-version->updates
  [;; ...
   ["65.1" {:fix fix-rename-parent-to-extends}]
   ;; ...
   ])
```

**Line**: 421

**Key Insight**: This migration (version 65.1) is critical for transitioning from the old parent system to the new Library-based system.

### Location: `src/test/frontend/worker/migrate_test.cljs`

**Migration Test**:

```clojure
(deftest test-fix-rename-parent-to-extends
  ;; ... test setup ...

  ;; Verify Library page created with correct UUID
  (is (= "00000004-1294-7765-6000-000000000000"
         (str (:block/uuid library-page))))

  ;; Verify top parents moved to Library
  (is (= (:db/id library-page)
         (:db/id (:block/parent page-a))))

  ;; Verify children maintain hierarchy
  (is (= (:db/id page-a)
         (:db/id (:block/parent page-b)))))
```

**Evidence Tags**: #migration #data-transformation #library-creation #schema-upgrade

---

## Evidence #8: Rename Operations

### Location: `src/main/frontend/worker/handler/page/file_based/rename.cljs`

**Complete Namespace Rename Flow**:

```clojure
(defn rename!
  [repo conn config page-uuid new-name & {:keys [persist-op?]
                                          :or {persist-op? true}}]
  (let [db @conn
        page-e        (d/entity db [:block/uuid page-uuid])
        old-name      (:block/title page-e)
        new-name      (string/trim new-name)
        old-page-name (common-util/page-name-sanity-lc old-name)
        new-page-name (common-util/page-name-sanity-lc new-name)
        new-page-e (d/entity db [:block/name new-page-name])
        name-changed? (not= old-name new-name)]
    (cond
      ;; ... validation checks ...

      (and old-name new-name name-changed?)
      (do
        ;; ... rename logic ...
        (rename-namespace-pages! repo conn config old-name new-name)
        (rename-nested-pages repo conn config old-name new-name)))))
```

**Lines**: 303-345

**Rename Operations**:

1. **`rename-namespace-pages!`**: Renames all child pages of a namespace

   ```clojure
   ;; Rename "Projects" → "MyProjects"
   ;; Automatically renames:
   ;; - "Projects/Work" → "MyProjects/Work"
   ;; - "Projects/Personal" → "MyProjects/Personal"
   ;; - "Projects/Work/Client" → "MyProjects/Work/Client"
   ```

2. **`rename-nested-pages`**: Renames pages that reference the namespace
   ```clojure
   ;; Rename "Obsidian" → "Logseq"
   ;; Automatically renames:
   ;; - "My notes about [[Obsidian]]" → "My notes about [[Logseq]]"
   ;; - "[[Obsidian]]/Plugin" → "[[Logseq]]/Plugin"
   ```

**Reference Update Function**:

```clojure
(defn replace-page-ref
  "Unsanitized only"
  [db config page new-name]
  (let [to-page (ldb/get-page db new-name)
        old-title (:block/title page)
        blocks (:block/_refs (d/entity db (:db/id page)))
        tx     (->> (map (fn [{:block/keys [uuid title properties format] :as block}]
                           (let [content    (let [content' (replace-old-page! config title old-title new-name format)]
                                              (when-not (= content' title)
                                                content'))
                                 properties (let [properties' (walk-replace-old-page! config properties old-title new-name format)]
                                              (when-not (= properties' properties)
                                                properties'))]
                             (when (or content properties)
                               (common-util/remove-nils-non-nested
                                {:block/uuid       uuid
                                 :block/title    content
                                 :block/properties properties
                                 :block/refs (->> (rename-update-block-refs! (:block/refs block) (:db/id page) (:db/id to-page))
                                                  (map :db/id)
                                                  (set))})))) blocks)
                    (remove nil?))]
    tx))
```

**Lines**: 113-138

**Updates**:

- Block content (title)
- Block properties
- Block references (`:block/refs`)
- Tag references (hashtags)
- Property references (property keys)

**Evidence Tags**: #rename-operations #namespace-rename #reference-updates #cascade-updates

---

## Evidence #9: Public API

### Location: `src/main/logseq/api.cljs`

**Plugin API Functions**:

```clojure
(defn ^:export get_pages_from_namespace
  [namespace]
  (when-not (db-graph?)
    (p/let [pages (file-model/get-namespace-pages (state/get-current-repo) namespace)]
      (bean/->js pages))))

(defn ^:export get_pages_tree_from_namespace
  [namespace]
  (when-not (db-graph?)
    (bean/->js (file-model/get-namespace-hierarchy (state/get-current-repo) namespace))))
```

**Context**: These functions are part of the Logseq Plugin API, allowing plugins to:

1. **`get_pages_from_namespace`**: Retrieve all pages under a namespace (flat list)
2. **`get_pages_tree_from_namespace`**: Retrieve namespace hierarchy as tree structure

**Example Usage** (JavaScript):

```javascript
// Get all pages under "Projects" namespace
const pages = await logseq.api.get_pages_from_namespace('Projects')
// Returns: [{name: "projects/work", title: "Projects/Work"}, ...]

// Get namespace hierarchy tree
const tree = await logseq.api.get_pages_tree_from_namespace('Projects')
// Returns: {name: "projects", children: [{name: "projects/work", children: [...]}, ...]}
```

**Restriction**: These APIs only work for **file-based graphs**, not DB graphs. DB graphs use `:block/parent` for hierarchy instead.

**Evidence Tags**: #public-api #plugin-api #namespace-queries #developer-interface

---

## The Verdict: Key Insights

### 1. Dual Systems for Different Graph Types

**Library Page** (DB Graphs Only):

- Purpose: Centralized storage for reusable content
- Implementation: Built-in page with special UUID
- Hierarchy: Uses `:block/parent` relationships
- UI: "Add pages to Library" button with search interface
- Migration: Automatically created during schema upgrade, top-level parents moved in

**Namespace Pages** (Primarily File Graphs):

- Purpose: Hierarchical page organization using "/" separator
- Implementation: `:block/namespace` attribute pointing to parent page
- Queries: Datalog rules for recursive hierarchy traversal
- UI: Hierarchy display, namespace-aware search
- API: Exposed to plugins for programmatic access

**DB Graphs Namespace-like Feature**:

- Uses `:block/parent` for page hierarchy (similar to namespace concept)
- Displays with "/" separator in title (e.g., "Parent/Child/Grandchild")
- Library page filtered from display to reduce noise

### 2. Migration Path and Backward Compatibility

**Key Migration (Schema 65.1)**:

```
Old System (Pre-Migration):
- :logseq.property/parent → used for both classes and pages

New System (Post-Migration):
- :logseq.property.class/extends → class inheritance
- :block/parent → page hierarchy
- Library page created automatically
- Top-level parents moved to Library
```

**Compatibility Considerations**:

- Old `{{namespace}}` macro deprecated in favor of Library page
- Namespace concept preserved in file graphs via `:block/namespace`
- DB graphs use parent-child relationships for similar functionality

### 3. Search and Query Integration

**Search Features**:

- `library-page-search?` flag for filtering Library pages
- Namespace-aware matching on last part of namespace (e.g., "work" matches "projects/work")
- Built-in page filtering to exclude system pages
- SQLite FTS (Full-Text Search) with trigram tokenizer for substring matching

**Query Features**:

- Datalog rules for namespace hierarchy traversal
- Recursive parent-child queries for page hierarchy
- Tree structure generation for UI display
- Plugin API for programmatic access (file graphs only)

### 4. User Experience Design

**Library Page UX**:

1. Search-based page selection (debounced for performance)
2. Multi-select for bulk operations
3. Instant add/remove from Library
4. Filtered from hierarchy display to reduce clutter

**Namespace Page UX**:

1. Automatic parent page creation on namespace page creation
2. Cascade rename for all children when parent renamed
3. Hierarchy visualization in sidebar
4. Breadcrumb-style title display with "/" separator

### 5. Data Consistency and Integrity

**Rename Operations**:

- Update all block references in content
- Update all property values
- Update `:block/refs` attribute
- Update file paths (file graphs)
- Update `:block/namespace` attribute (file graphs)
- Cascade to all child pages in namespace

**Parent-Child Integrity**:

- Prevent circular references
- Automatic order generation using fractional indexing
- Library page always exists (created in migration)
- Namespace parent pages created automatically

### 6. Performance Considerations

**Search Optimization**:

- SQLite FTS virtual table for fast full-text search
- Triggers for automatic index updates
- Debounced search input (200ms) to reduce query load
- Page-only search option to reduce result set

**Query Optimization**:

- Datalog rules compiled once, reused for all queries
- Indexed lookups on `:block/name` and `:block/namespace`
- Tree structure cached in UI components
- Pull many for batch retrieval of namespace pages

---

## For Developers: Working with Library and Namespace

### Detecting Library Pages

```clojure
(require '[logseq.db :as ldb]
         '[logseq.common.config :as common-config])

;; Check if page is Library page
(defn is-library-page? [db page-title]
  (= common-config/library-page-name page-title))

;; Check if page entity is Library
(defn library-page-entity? [page-entity]
  (ldb/library? page-entity))

;; Get Library page
(defn get-library [db]
  (ldb/get-library-page db))

;; Check if page is in Library
(defn in-library? [db page]
  (ldb/page-in-library? db page))
```

### Detecting Namespace Pages

```clojure
(require '[logseq.common.util.namespace :as ns-util])

;; Check if page name is namespace page
(defn is-namespace? [page-name]
  (ns-util/namespace-page? page-name))

;; Get last part of namespace
(defn namespace-last-part [page-name]
  (ns-util/get-last-part page-name))

;; Examples:
(is-namespace? "Projects/Work")           ;; => true
(is-namespace? "Simple Page")             ;; => false
(namespace-last-part "Projects/Work")     ;; => "Work"
(namespace-last-part "a/b/c")             ;; => "c"
```

### Querying Namespace Hierarchy (File Graphs)

```clojure
(require '[frontend.db.file-based.model :as file-model])

;; Get all child pages of namespace
(defn get-namespace-children [repo namespace]
  (file-model/get-namespace-pages repo namespace))

;; Get namespace hierarchy tree
(defn get-namespace-tree [repo namespace]
  (file-model/get-namespace-hierarchy repo namespace))

;; Get parent namespace of page
(defn get-parent-namespace [repo page-name]
  (file-model/get-page-namespace repo page-name))

;; Example:
(get-namespace-children "my-graph" "Projects")
;; => [{:block/name "projects/work" :block/title "Projects/Work" ...}
;;     {:block/name "projects/personal" :block/title "Projects/Personal" ...}]

(get-namespace-tree "my-graph" "Projects")
;; => {:db/id 123
;;     :block/name "projects"
;;     :namespace/children [...]}
```

### Querying Page Hierarchy (DB Graphs)

```clojure
(require '[logseq.db.frontend.db :as db-db])

;; Get all parents of page
(defn get-page-hierarchy [page-entity]
  (db-db/get-page-parents page-entity))

;; Get full title with parents
(defn get-full-title [page-entity]
  (db-db/get-title-with-parents page-entity))

;; Example:
(def page (d/entity db [:block/uuid some-uuid]))
(get-page-hierarchy page)
;; => [grandparent-page parent-page]

(get-full-title page)
;; => "Grandparent/Parent/Current"
```

### Adding Pages to Library

```clojure
(require '[frontend.handler.editor :as editor-handler]
         '[logseq.db :as ldb])

(defn add-to-library! [db page-entities]
  (let [library-page (ldb/get-library-page db)]
    (editor-handler/move-blocks! page-entities library-page {:bottom? true})))

;; Example:
(def pages [(d/entity db [:block/uuid page-uuid-1])
            (d/entity db [:block/uuid page-uuid-2])])
(add-to-library! db pages)
```

### Removing Pages from Library

```clojure
(require '[frontend.db :as db]
         '[frontend.state :as state])

(defn remove-from-library! [page-id]
  (db/transact! (state/get-current-repo)
                [[:db/retract page-id :block/parent]]
                {:outliner-op :save-block}))

;; Example:
(remove-from-library! 12345)
```

### Creating Namespace Pages (File Graphs)

```clojure
(require '[frontend.worker.handler.page :as worker-page])

;; Create page with namespace
(defn create-namespace-page! [repo conn config page-name]
  (worker-page/create! repo conn config page-name))

;; Example - creates both "Projects" and "Projects/Work" if needed:
(create-namespace-page! "my-graph" conn config "Projects/Work")
```

### Renaming Namespace Pages

```clojure
(require '[frontend.worker.handler.page.file-based.rename :as rename])

;; Rename page and all children
(defn rename-namespace! [repo conn config page-uuid new-name]
  (rename/rename! repo conn config page-uuid new-name))

;; Example - renames "Projects" to "MyProjects" and all children:
(rename-namespace! "my-graph" conn config page-uuid "MyProjects")
;; Automatically updates:
;; - "Projects/Work" → "MyProjects/Work"
;; - "Projects/Personal" → "MyProjects/Personal"
```

### Searching with Library/Namespace Awareness

```clojure
(require '[frontend.search :as search]
         '[frontend.state :as state])

;; Search excluding Library pages
(defn search-non-library [query]
  (search/block-search (state/get-current-repo) query
                       {:enable-snippet? false
                        :built-in? false
                        :page-only? true
                        :library-page-search? false}))

;; Search including Library pages
(defn search-library [query]
  (search/block-search (state/get-current-repo) query
                       {:enable-snippet? false
                        :built-in? false
                        :page-only? true
                        :library-page-search? true}))
```

### Plugin API (File Graphs Only)

```javascript
// Get all pages in namespace (JavaScript plugin)
const pages = await logseq.api.get_pages_from_namespace('Projects')
console.log(pages)
// [{name: "projects/work", title: "Projects/Work", ...}, ...]

// Get namespace hierarchy tree
const tree = await logseq.api.get_pages_tree_from_namespace('Projects')
console.log(tree)
// {name: "projects", children: [...]}
```

### Common Patterns

**Pattern 1: Check graph type before namespace operations**

```clojure
(defn safe-namespace-operation [repo]
  (if (config/db-based-graph? repo)
    ;; Use :block/parent for DB graphs
    (use-parent-hierarchy)
    ;; Use :block/namespace for file graphs
    (use-namespace-hierarchy)))
```

**Pattern 2: Filter Library pages from display**

```clojure
(defn display-page-title [page]
  (if (ldb/library? page)
    (:block/title page)  ; Show just "Library"
    (ldb/get-title-with-parents page)))  ; Show "Parent/Child/Page" (Library filtered)
```

**Pattern 3: Handle namespace in search**

```clojure
(defn search-with-namespace-awareness [query]
  (let [results (search/block-search repo query opts)
        ;; Match on last part of namespace too
        results' (map (fn [result]
                       (assoc result :display-name
                              (ns-util/get-last-part (:block/title result))))
                     results)]
    results'))
```

### Testing Helpers

```clojure
;; Test if Library page exists
(defn library-exists? [db]
  (some? (ldb/get-library-page db)))

;; Test namespace relationships
(defn test-namespace-hierarchy [db parent-name child-name]
  (let [parent (d/entity db [:block/name (common-util/page-name-sanity-lc parent-name)])
        child (d/entity db [:block/name (common-util/page-name-sanity-lc child-name)])]
    (= (:db/id parent) (:db/id (:block/namespace child)))))

;; Test Library page parent relationships
(defn test-library-parent [db page-name]
  (let [page (d/entity db [:block/name (common-util/page-name-sanity-lc page-name)])
        library (ldb/get-library-page db)]
    (ldb/page-in-library? db page)))
```

### Migration Considerations

**When adding new Library-related features**:

1. Check if Library page exists (use `ldb/get-library-page`)
2. Handle case where Library doesn't exist (older graphs)
3. Filter Library from displays to avoid clutter
4. Use `:block/parent` for DB graphs, `:block/namespace` for file graphs

**When working with namespaces**:

1. Always sanitize page names before queries
2. Check graph type (DB vs file) before using namespace APIs
3. Handle both "/" and "." separators (old vs new)
4. Update `:block/namespace` when creating/renaming pages

---

## Case Closed

The Library and Namespace system represents two complementary organizational features in Logseq:

1. **Library Page**: A modern, DB-graph-centric approach to organizing reusable content, replacing the old namespace macro system
2. **Namespace Pages**: A file-graph-native hierarchical organization system using "/" separators

Both systems achieve similar goals (organization and hierarchy) but are optimized for their respective graph types. The migration system ensures backward compatibility while moving users toward the newer, more robust Library-based approach.

**Key Takeaways**:

- Library page is a built-in page with special UUID in DB graphs
- Namespace pages use "/" separator and `:block/namespace` attribute in file graphs
- DB graphs use `:block/parent` for page hierarchy (similar to namespaces)
- Search, queries, and UI components are namespace/Library-aware
- Migration automatically creates Library and moves top-level parents
- Public API available for plugins (file graphs only)

**Files to Watch**:

- `logseq.common.config` - Library page name constant
- `logseq.common.util.namespace` - Namespace detection
- `logseq.db.frontend.db` - Library and parent hierarchy functions
- `frontend.components.library` - Library UI
- `frontend.db.file-based.model` - Namespace model for file graphs
- `frontend.worker.db.migrate` - Migration logic

---

_Investigation completed: 2025-10-21_
_Lead Detective: Claude Code_
_Case Status: CLOSED - Documentation Complete_
