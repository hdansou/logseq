---
title: 'Block Editor Architecture'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Schema System](./schema-system.md)'
  - '[Database Integrity](./database-integrity.md)'
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
tags: ['editor', 'outliner', 'blocks', 'architecture', 'datascript']
---

# Block Editor Architecture

This document explains Logseq's block editor—the core system enabling hierarchical note-taking with outliner-style operations. Understanding this architecture is essential for contributors and plugin developers working with block manipulation.

**What You'll Learn:**

- Outliner operation model
- Block hierarchy and ordering
- Transaction pipeline
- Auto-save mechanism
- Editor state management

**Prerequisites:**

- Familiarity with Logseq blocks
- Understanding of DataScript ([Schema System](./schema-system.md))
- Basic ClojureScript knowledge (for contributors)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Outliner Operations](#outliner-operations)
3. [Block Hierarchy](#block-hierarchy)
4. [Transaction Pipeline](#transaction-pipeline)
5. [Auto-Save System](#auto-save-system)
6. [Editor State](#editor-state)
7. [Developer Guide](#developer-guide)

---

## Architecture Overview

### Multi-Layer Editor Stack

```
┌─────────────────────────────────────────────────────────┐
│                    UI Layer                             │
│  • Editable contentEditable elements                    │
│  • React/Rum components                                 │
│  • Keyboard shortcuts                                   │
│  • Context menus                                        │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Editor Handler Layer                         │
│  • Edit mode management                                 │
│  • Input parsing and validation                         │
│  • Command dispatch                                     │
│  • Auto-save coordination                               │
│  Location: src/main/frontend/handler/editor.cljs        │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            Outliner Core Layer                          │
│  • Block operations (insert, move, delete)              │
│  • Hierarchy validation                                 │
│  • Transaction building                                 │
│  • Reference management                                 │
│  Location: deps/outliner/src/logseq/outliner/core.cljs  │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│            DataScript Layer                             │
│  • Entity storage and retrieval                         │
│  • Query execution                                      │
│  • Transaction commit                                   │
│  • Schema validation                                    │
└─────────────────────────────────────────────────────────┘
```

### Core Concepts

**Block**: Fundamental data unit with hierarchical structure

```clojure
{:db/id 123
 :block/uuid #uuid "abc-123"
 :block/title "Block content"
 :block/parent {:db/id 456}     ; Parent block reference
 :block/page {:db/id 789}       ; Page reference
 :block/order "b00"             ; Fractional indexing order
 :block/refs [{:db/id 321}]     ; Page/block references
 :block/tags [{:db/id 654}]     ; Class tags
 :block/created-at 1706198400000
 :block/updated-at 1706198450000}
```

**Outliner Operation**: Atomic block manipulation (insert, move, delete, indent, outdent)

**Transaction**: DataScript change set applied atomically

---

## Outliner Operations

### Core Operations

**Location:** `deps/outliner/src/logseq/outliner/core.cljs`

**1. Insert Block**

```clojure
(defn insert-blocks!
  [repo db blocks target-block sibling-before?]
  ;; Location: core.cljs:450-520
  ;; 1. Validate parent-child relationships
  ;; 2. Calculate new block orders
  ;; 3. Build transaction data
  ;; 4. Update references
  ;; 5. Commit transaction
  )
```

**Example:**

```
Before:                After insert (sibling):
- Parent               - Parent
  - Block A              - Block A
  - Block B              - NEW BLOCK
                         - Block B
```

**2. Move Blocks**

```clojure
(defn move-blocks
  [db blocks target-block sibling-before?]
  ;; Location: core.cljs:600-750
  ;; 1. Remove blocks from old positions
  ;; 2. Calculate new orders at target
  ;; 3. Update parent relationships
  ;; 4. Preserve children hierarchy
  )
```

**Example:**

```
Before:                After move:
- Page 1               - Page 1
  - Block A (move)       - Block C
  - Block B            - Page 2
- Page 2                 - Block A (moved here)
  - Block C              - Block B
```

**3. Delete Blocks**

```clojure
(defn delete-blocks
  [db block-uuids & {:keys [children? page?]}]
  ;; Location: core.cljs:850-920
  ;; 1. Collect blocks to delete
  ;; 2. Handle children (delete or promote)
  ;; 3. Retract entities
  ;; 4. Update references
  ;; 5. Handle orphaned pages
  )
```

**4. Indent/Outdent**

```clojure
;; Indent: Move block to become child of previous sibling
(defn indent-outdent-blocks
  [blocks indent?]
  ;; Location: core.cljs:1050-1120
  ;; Indent: target = previous sibling (as last child)
  ;; Outdent: target = parent's parent (as next sibling)
  )
```

**Example (Indent):**

```
Before:                After indent:
- Parent               - Parent
  - Block A              - Block A
  - Block B (indent)       - Block B (now child of A)
```

### Operation Validation

**Pre-conditions checked:**

1. **Target exists**: Target block must be valid entity
2. **No circular references**: Cannot move parent into its own children
3. **Same page**: Blocks being moved must stay on same page (in most cases)
4. **Valid order**: New order must be valid fractional index

**Location:** `deps/outliner/src/logseq/outliner/validate.cljs`

```clojure
(defn validate-move
  [db blocks target]
  ;; Check circular references
  (when (ancestor? db target (first blocks))
    (throw (ex-info "Circular reference detected" {})))
  ;; Check same page
  (when-not (same-page? db blocks target)
    (throw (ex-info "Blocks must be on same page" {}))))
```

---

## Block Hierarchy

### Order System (Fractional Indexing)

**Block Order:** Lexicographic string enabling O(1) insertion

**Location:** `deps/db/src/logseq/db/common/order.cljs`

```clojure
;; Generate order between two blocks
(defn get-order-between
  [left-order right-order]
  ;; Location: order.cljs:50-120
  ;; Examples:
  ;; (get-order-between nil "b00") => "a00" (before first)
  ;; (get-order-between "a00" "b00") => "aZZ" (between)
  ;; (get-order-between "b00" nil) => "c00" (after last)
  )
```

**Example Hierarchy:**

```
Block A     order: "a00"
Block B     order: "b00"
  Child 1   order: "a00" (local to Block B's children)
  Child 2   order: "b00"
Block C     order: "c00"
```

**Advantages:**

- **O(1) insertion** (no need to renumber siblings)
- **Persistent order** (survives renames, moves)
- **Lexicographic sorting** (database sorts correctly)

### Parent-Child Relationships

**Representation:**

```clojure
;; Parent → Children (query)
(d/q '[:find ?child
       :in $ ?parent-id
       :where
       [?child :block/parent ?parent]]
     db parent-id)

;; Child → Parent (attribute lookup)
(:block/parent child-entity)
```

**Page-Level Blocks:**

```clojure
;; Top-level block (parent = page itself)
{:db/id 123
 :block/parent {:db/id 456}  ; 456 = page entity
 :block/page {:db/id 456}}   ; Same as parent

;; Nested block
{:db/id 789
 :block/parent {:db/id 123}  ; 123 = parent block
 :block/page {:db/id 456}}   ; Still references page
```

---

## Transaction Pipeline

### Transaction Flow

```
1. User Action (keyboard, context menu)
   ↓
2. Handler Dispatch (editor.cljs)
   ↓
3. Outliner Operation (outliner/core.cljs)
   ↓
4. Build Transaction Data
   ↓
5. Validation (outliner/validate.cljs)
   ↓
6. UI Transaction Wrapper (ui-outliner-tx/transact!)
   ↓
7. DataScript Commit (d/transact!)
   ↓
8. Post-Commit Hooks (auto-save, indexing)
```

### Transaction Building

**Example: Insert Block**

**Location:** `deps/outliner/src/logseq/outliner/core.cljs:292-330`

```clojure
(defn outliner-insert-block!
  [db {:keys [content page sibling? before? replace-empty-target?]}]
  ;; 1. Calculate order
  (let [target-block (get-target-block db page)
        order (calculate-new-order db target-block sibling? before?)]

    ;; 2. Build block entity
    (let [new-block {:block/uuid (random-uuid)
                     :block/title content
                     :block/parent (if sibling?
                                     (:block/parent target-block)
                                     target-block)
                     :block/page page
                     :block/order order
                     :block/created-at (time-ms)
                     :block/updated-at (time-ms)}]

      ;; 3. Parse content for refs/properties
      (let [new-block' (parse-block-content db new-block)]

        ;; 4. Build transaction
        (let [tx-data [new-block']]

          ;; 5. Wrap in UI transaction
          (ui-outliner-tx/transact!
           {:outliner-op :insert-blocks}
           (fn [] (d/transact! db tx-data))))))))
```

### UI Transaction Wrapper

**Purpose:** Coordinate UI updates with database changes

**Location:** `src/main/frontend/modules/outliner/ui.cljs`

```clojure
(defn transact!
  [{:keys [outliner-op]} f]
  ;; 1. Set editing state
  (state/set-editor-op! outliner-op)

  ;; 2. Execute transaction
  (let [result (f)]

    ;; 3. Update UI
    (refresh-affected-blocks!)

    ;; 4. Trigger auto-save
    (schedule-auto-save!)

    ;; 5. Clear editing state
    (state/clear-editor-op!)

    result))
```

---

## Auto-Save System

### Auto-Save Flow

```
1. Block Edit (user types)
   ↓
2. Debounce (450ms delay)
   ↓
3. Save Block Transaction
   ↓
4. Update Timestamps
   ↓
5. Persist to Storage:
   - File Graph: Write markdown file
   - DB Graph: Export transit + save SQLite
```

### Debounced Save

**Location:** `src/main/frontend/handler/editor.cljs:260-280`

```clojure
(defonce *save-timeout (atom nil))

(defn schedule-auto-save!
  [block delay]
  ;; Cancel previous timeout
  (when-let [timeout @*save-timeout]
    (js/clearTimeout timeout))

  ;; Schedule new save
  (reset! *save-timeout
    (js/setTimeout
      #(save-block! block)
      (or delay 450))))  ; Default 450ms delay
```

**Why 450ms?**

- **Not too fast**: Avoid saving every keystroke (performance)
- **Not too slow**: Minimize data loss on crash
- **User-perceivable**: Provides feedback without lag

### Persistence Modes

**File-Based Graphs:**

```clojure
;; Location: src/main/frontend/worker/file.cljs
(defn write-file!
  [path content]
  ;; 1. Convert blocks to markdown
  ;; 2. IPC call to Electron main process
  ;; 3. Write to file system
  (ipc/ipc :writeFile path content))
```

**Database Graphs:**

```clojure
;; Location: src/main/frontend/persist_db.cljs
(defn save-db!
  [repo]
  ;; 1. Export DataScript to transit
  ;; 2. Save to SQLite database
  ;; 3. Update RTC sync queue
  (let [transit-data (export-db repo)]
    (sqlite/save! repo transit-data)))
```

---

## Editor State

### State Management

**Location:** `src/main/frontend/state.cljs`

```clojure
(defonce *state (atom {:editor/editing? false
                       :editor/block nil
                       :editor/content ""
                       :editor/cursor-pos 0
                       :editor/last-saved-at nil}))

;; Getters
(defn get-edit-block [] (:editor/block @*state))
(defn editing? [] (:editor/editing? @*state))

;; Setters
(defn set-edit-block! [block] (swap! *state assoc :editor/block block))
(defn clear-edit! [] (swap! *state dissoc :editor/block :editor/editing?))
```

### Edit Mode Lifecycle

```
1. User clicks block or presses Enter
   ↓
2. edit-block! called
   - Set :editor/block
   - Set :editor/editing? true
   - Focus contentEditable
   - Restore cursor position
   ↓
3. User edits content
   - Input events update state
   - Schedule auto-save
   ↓
4. User clicks away or presses Escape
   ↓
5. exit-edit-mode! called
   - Save block (if changed)
   - Clear :editor/block
   - Set :editor/editing? false
```

**Example:**

**Location:** `src/main/frontend/handler/block.cljs:76`

```clojure
(defn edit-block!
  [block pos]
  ;; 1. Set editing state
  (state/set-edit-block! block)
  (state/set-editor-editing! true)

  ;; 2. Focus editor
  (focus-editor! (:block/uuid block))

  ;; 3. Restore cursor
  (set-cursor-pos! pos))
```

---

## Developer Guide

### Implementing Custom Block Operations

**Example: Duplicate Block**

```clojure
(defn duplicate-block!
  [block-uuid]
  (when-let [block (db-utils/entity [:block/uuid block-uuid])]
    (let [db (state/get-current-repo)
          ;; 1. Clone block data
          new-block (-> block
                        (select-keys [:block/title :block/properties])
                        (assoc :block/uuid (random-uuid))
                        (assoc :block/created-at (time-ms))
                        (assoc :block/updated-at (time-ms)))
          ;; 2. Insert after original
          tx-data [(assoc new-block
                          :block/parent (:block/parent block)
                          :block/page (:block/page block)
                          :block/order (calculate-order-after block))]]
      ;; 3. Transact
      (ui-outliner-tx/transact!
       {:outliner-op :insert-blocks}
       (fn [] (d/transact! db tx-data))))))
```

### Plugin API for Blocks

**Available via @logseq/libs:**

```typescript
// Insert block
await logseq.Editor.insertBlock(targetBlockUuid, 'New block content', {
  sibling: true,
  before: false,
})

// Update block
await logseq.Editor.updateBlock(blockUuid, 'Updated content')

// Move block
await logseq.Editor.moveBlock(blockUuid, targetBlockUuid, {
  sibling: true,
  before: true,
})

// Delete block
await logseq.Editor.removeBlock(blockUuid)

// Get block
const block = await logseq.Editor.getBlock({ uuid: blockUuid })
```

**See:** [Plugin API Reference](../../reference/api/plugin-api.md)

---

## Next Steps

**Continue Learning:**

1. **[Database Integrity](./database-integrity.md)** - Transaction validation
2. **[Schema System](./schema-system.md)** - Data model details
3. **[Plugin API Reference](../../reference/api/plugin-api.md)** - Block manipulation API
4. **[Query System](../../reference/advanced/query-system.md)** - Query blocks

**For Contributors:**

- [Outliner Core Source](../../../../../deps/outliner/src/logseq/outliner/core.cljs)
- [Editor Handler Source](../../../../../src/main/frontend/handler/editor.cljs)
- [Block Order Implementation](../../../../../deps/db/src/logseq/db/common/order.cljs)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
