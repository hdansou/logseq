# CMD+R/Ctrl+R Window Reload: Data Persistence Deep Dive

**Investigation Date:** October 24, 2025  
**Branch:** test/db  
**Investigator:** Code Detective Analysis

---

## Executive Summary

This guide documents the behavior of window reload operations (CMD+R on macOS, Ctrl+R on Windows) in Logseq and their impact on data persistence. The investigation reveals **critical data loss risks** due to the absence of window reload safeguards for pending writes.

### Key Findings

1. ✅ **CMD+R triggers instant window reload** via Electron's default menu behavior
2. ❌ **No beforeunload handler** exists for main window reloads
3. ⚠️ **In-memory write queues are lost** during reload
4. ⚠️ **Transit files do NOT store pending writes** - they only persist committed database state
5. ⏱️ **Auto-save has 450ms delay** - edits within this window are at risk
6. ⏱️ **File writes batch with 1000ms interval** - creates additional exposure window

---

## Table of Contents

- [1. Window Reload Mechanism](#1-window-reload-mechanism)
- [2. Data Persistence Architecture](#2-data-persistence-architecture)
- [3. Write Queue System](#3-write-queue-system)
- [4. Transit File System](#4-transit-file-system)
- [5. Auto-Save Mechanism](#5-auto-save-mechanism)
- [6. Window Lifecycle Events](#6-window-lifecycle-events)
- [7. Data Loss Risk Windows](#7-data-loss-risk-windows)
- [8. Comparison: File-Based vs Database Mode](#8-comparison-file-based-vs-database-mode)
- [9. Plugin System Protection](#9-plugin-system-protection)
- [10. Recommendations](#10-recommendations)

---

## 1. Window Reload Mechanism

### 1.1 Keyboard Shortcut Binding

**Location:** Electron Default Menu (viewMenu)

The CMD+R (macOS) and Ctrl+R (Windows) shortcuts are handled by Electron's built-in menu system, not by Logseq's custom shortcuts.

```javascript
// Electron's default viewMenu includes:
{
  label: 'Reload',
  accelerator: 'CmdOrCtrl+R',
  click: (item, focusedWindow) => {
    if (focusedWindow) focusedWindow.reload()
  }
}
```

**Key Characteristics:**
- ⚡ **Instant execution** - no delay, no confirmation
- 🔄 **Full renderer process restart** - all JavaScript context destroyed
- 🧹 **Memory cleared** - all in-memory state lost
- 📦 **IndexedDB/localStorage persisted** - browser storage survives

### 1.2 What Happens During Reload

```
User presses CMD+R
    ↓
Electron Menu Handler Triggered
    ↓
BrowserWindow.reload() Called
    ↓
Renderer Process Terminated (immediate)
    ├── All JavaScript variables destroyed
    ├── Event loops cleared
    ├── Pending promises abandoned
    └── *writes atom → LOST
    ↓
New Renderer Process Started
    ├── HTML/CSS/JS reloaded
    ├── React components remounted
    └── Application state reconstructed
    ↓
Application Initialization
    ├── Database/Graph loaded
    ├── IndexedDB data retrieved
    └── UI rendered
```

**Critical Point:** The reload is **synchronous** from the user's perspective - there's no opportunity for cleanup handlers to run.

---

## 2. Data Persistence Architecture

Logseq employs a multi-layered persistence strategy that differs between file-based and database modes.

### 2.1 File-Based Graph Mode

```
User Edit in Editor
    ↓
Auto-save Timer (450ms)
    ↓
Write Request Added to Queue
    ↓
*writes atom: {request-id → page-id}
    ↓
file-writes-chan (capacity: 10,000)
    ↓
Batch Processing (1000ms interval)
    ↓
IPC to Main Process
    ↓
File System Write
    ↓
Markdown File Updated
```

**Code Reference:**
```clojure
;; src/main/frontend/worker/file.cljs:23-24
(defonce *writes (atom {}))        ;; IN-MEMORY ONLY
(defonce *request-id (atom 0))     ;; Request counter

;; src/main/frontend/worker/file.cljs:43-50
(defonce file-writes-chan
  (let [ch (async/chan 10000)]     ;; Buffer size
    ch))

(def batch-write-interval 1000)     ;; 1 second batching
```

### 2.2 Database (SQLite) Mode

```
User Edit in Editor
    ↓
Auto-save Timer (450ms)
    ↓
Transact Data to DataScript
    ↓
DataScript in-memory DB updated
    ↓
Worker Thread API Call
    ↓
SQLite Transaction
    ↓
Write-Ahead Log (WAL)
    ↓
Periodic Export (30 seconds)
    ↓
Transit File Saved to Disk
```

**Code Reference:**
```clojure
;; src/main/frontend/persist_db.cljs:63-75
(defn export-current-graph!
  [& {:keys [succ-notification? force-save?]}]
  (when (util/electron?)
    (when-let [repo (state/get-current-repo)]
      (when (or (and (config/db-based-graph? repo) 
                     (graph-has-changed? repo)) 
                force-save?)
        (println :debug :save-db-to-disk repo)
        (<export-db repo {})))))

;; src/main/frontend/persist_db.cljs:77-80
(defn run-export-periodically!
  []
  (js/setInterval export-current-graph!
                  (* 30 1000)))      ;; Every 30 seconds
```

---

## 3. Write Queue System

### 3.1 The *writes Atom (File-Based Mode)

**Location:** `src/main/frontend/worker/file.cljs`

This is the **critical in-memory structure** that tracks pending file writes.

```clojure
(defonce *writes (atom {}))
;; Structure: {request-id → page-id}
;; Example: {1 42, 2 87, 3 42}
```

**Key Characteristics:**
- 🔴 **In-memory only** - exists solely in JavaScript heap
- 🔴 **Not persisted** - lost on process termination
- 🔴 **No backup** - no redundant storage
- 🟢 **Fast lookup** - O(1) access for status checks

### 3.2 Write Request Lifecycle

```clojure
;; 1. Create write request
(defn add-write-request! [page-id]
  (let [request-id (swap! *request-id inc)]
    (swap! *writes assoc request-id page-id)
    request-id))

;; 2. Process writes (batched)
(go-loop []
  (let [writes (async/<! file-writes-chan)]
    ;; Batch processing every 1000ms
    (process-batch writes))
  (recur))

;; 3. Complete write (IPC callback)
(defn dissoc-request! [request-id]
  (swap! *writes dissoc request-id))
```

### 3.3 Checking Write Completion

**Location:** `src/main/frontend/worker/db_worker.cljs:644-660`

```clojure
(def-thread-api :thread-api/file-writes-finished?
  [repo]
  (let [conn (worker-state/get-datascript-conn repo)
        writes @*writes]
    ;; Clean pages that have been deleted
    (when conn
      (swap! *writes (fn [writes]
                       (->> writes
                            (remove (fn [[_ pid]] 
                                      (d/entity @conn pid)))
                            (into {})))))
    (if (empty? writes)
      true
      (do
        (prn "Unfinished file writes:" @*writes)
        false))))
```

**Usage Context:** Graph switching checks if writes are finished before proceeding:

```clojure
;; src/main/frontend/handler/events.cljs:121-124
(p/let [writes-finished? 
        (state/<invoke-db-worker 
          :thread-api/file-writes-finished? 
          (state/get-current-repo))]
  (log/info :graph/switch 
            {:file-writes-finished? writes-finished?}))
```

---

## 4. Transit File System

### 4.1 What Transit Files Actually Store

Transit files are **NOT** a safety mechanism for pending writes. They serve a different purpose.

**Purpose:** Serialize the complete SQLite database for backup/export/transfer

```clojure
;; src/electron/electron/db.cljs:20-23
(defn save-db!
  [db-name data]
  (let [[_db-name db-path] 
        (common-sqlite/get-db-full-path 
          (cli-common-graph/get-db-graphs-dir) 
          db-name)]
    (fs/writeFileSync db-path data)))
```

### 4.2 Transit Export Process

**Location:** `src/main/frontend/worker/db_worker.cljs:582-586`

```clojure
(def-thread-api :thread-api/export-db
  [repo]
  ;; Checkpoint Write-Ahead Log first
  (when-let [^js db (worker-state/get-sqlite-conn repo :db)]
    (.exec db "PRAGMA wal_checkpoint(2)"))
  
  ;; Export database file
  (p/let [data (<export-db-file repo)]
    (Comlink/transfer data #js [(.-buffer data)])))
```

**What Gets Exported:**
1. ✅ Committed SQLite transactions
2. ✅ DataScript database state
3. ✅ User preferences (via key-value store)
4. ✅ Graph metadata
5. ❌ **NOT** the `*writes` atom
6. ❌ **NOT** pending transactions
7. ❌ **NOT** uncommitted edits

### 4.3 Transit File Serialization Format

**Location:** `deps/db/src/logseq/db/sqlite/util.cljs:25-49`

```clojure
(def write-transit-str
  (let [write-handlers* 
        (->> (assoc dt/write-handlers
                    ;; DataScript Entity handler
                    de/Entity 
                    (transit/write-handler 
                      (constantly "datascript/Entity")
                      (fn [^de/entity entity]
                        (assoc (.-kv entity)
                               :db/id (:db/id entity))))
                    
                    ;; Error handlers
                    ExceptionInfo 
                    (transit/write-handler 
                      (constantly "error")
                      (fn [e]
                        {:message (ex-message e)
                         :data (ex-data e)}))
                    
                    js/Error 
                    (transit/write-handler 
                      (constantly "js/Error")
                      (fn [e] 
                        {:message (ex-message e)})))
             (merge write-handlers))
        writer (transit/writer :json 
                              {:handlers write-handlers*})]
    (fn write-transit-str* [o]
      (transit/write writer o))))
```

**Key Point:** Transit is for **serializing committed state**, not buffering pending changes.

### 4.4 Transit Export Triggers

Transit exports happen **independently** of window reload:

```clojure
;; src/main/frontend/persist_db.cljs:77-80
(defn run-export-periodically!
  []
  (js/setInterval export-current-graph!
                  (* 30 1000)))    ;; Every 30 seconds
```

**Triggers:**
1. ⏰ **Periodic:** Every 30 seconds (if graph changed)
2. 🖱️ **Manual:** User-initiated export
3. 🔄 **Graph switch:** Before switching graphs
4. 🚫 **NOT on window reload:** No beforeunload trigger

---

## 5. Auto-Save Mechanism

### 5.1 Auto-Save Timer

**Location:** `src/main/frontend/handler/editor.cljs`

```clojure
(defonce *auto-save-timeout (atom nil))

(defn auto-save!
  []
  (when-let [timeout @*auto-save-timeout]
    (js/clearTimeout timeout))
  (reset! *auto-save-timeout
          (js/setTimeout
            (fn []
              (save-current-block!)
              (reset! *auto-save-timeout nil))
            450)))    ;; 450ms delay
```

### 5.2 Auto-Save Flow

```
User Types Character
    ↓
auto-save! called
    ↓
Clear previous timeout
    ↓
Set new timeout (450ms)
    ↓
[User continues typing]
    ↓
Timer restarted (debounced)
    ↓
... 450ms of inactivity ...
    ↓
save-current-block! executes
    ↓
DataScript transaction
    ↓
SQLite write (DB mode)
  OR
Write queue addition (File mode)
```

### 5.3 Database Mode Special Handling

```clojure
;; Auto-save skips blocks with certain tags in DB mode
(when (config/db-based-graph? repo)
  (when-not (block-has-skip-tags? block)
    (save-block-to-db! block)))
```

**Implications:**
- 📝 Normal blocks: Auto-saved after 450ms
- 🏷️ Tagged blocks: May be skipped
- ⚡ Rapid typing: Saves batched/debounced
- 🔄 Reload within 450ms: **Changes lost**

---

## 6. Window Lifecycle Events

### 6.1 Available Lifecycle Hooks

Logseq implements different lifecycle handling for different contexts:

#### Window Close (Graceful)

**Location:** `src/electron/electron/window.cljs:90-99`

```clojure
(defn close-handler
  [^js win close-watcher-f e]
  (.preventDefault e)                    ;; Prevent immediate close
  (when-let [dir (state/get-window-graph-path win)]
    (close-watcher-f win dir))          ;; Close file watcher
  (state/close-window! win)             ;; Update state
  (let [web-contents (. win -webContents)]
    (.send web-contents "persist-zoom-level" 
           (.getZoomLevel web-contents))) ;; Save UI state
  (destroy-window! win))                ;; Finally destroy

(defn on-close-actions!
  [^js win close-watcher-f]
  (.on win "close" 
       (fn [e] (close-handler win close-watcher-f e))))
```

**Protection Provided:**
- ✅ File system watchers closed gracefully
- ✅ Zoom level persisted
- ✅ Window state saved
- ❌ **No write queue flush**
- ❌ **No forced save**

#### Application Quit

**Location:** `src/electron/electron/core.cljs:301-302`

```clojure
(.on app' "before-quit" 
     (fn [_e]
       (reset! win/*quitting? true)))
```

**Protection Provided:**
- ✅ Flag set to prevent new operations
- ❌ **No write queue processing**
- ❌ **No data persistence triggered**

#### Window Reload (CMD+R)

**Status:** ❌ **NO HANDLER EXISTS**

There is **no** beforeunload or equivalent handler for window reload events.

```bash
# Search results confirm:
$ grep -r "beforeunload" src/main/
# Only plugin-related results found
```

### 6.2 Plugin System Protection

Plugins **do** have beforeunload support (but this doesn't help core data):

**Location:** `libs/src/LSPlugin.user.ts:527-536`

```typescript
_caller.on('beforeunload', async (payload) => {
  const cb = this._beforeunloadCallback
  if (cb) {
    try {
      await cb(payload)
    } catch (e) {
      this.logger.error(`[beforeunload] `, e)
    }
  }
})
```

**Usage:**
```typescript
logseq.beforeunload(async () => {
  // Plugin cleanup code
  await savePluginData()
})
```

**Limitation:** This is for **plugin cleanup only**, not for Logseq's core data persistence.

---

## 7. Data Loss Risk Windows

### 7.1 Critical Time Windows

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA LOSS RISK TIMELINE                    │
└─────────────────────────────────────────────────────────────┘

User Types → Auto-save → Write Queue → File System
   [0ms]      [450ms]     [1450ms]      [varies]
    │           │            │             │
    ├───────────┤            │             │
    │ RISK #1:  │            │             │
    │ 450ms     │            │             │
    │ before    │            │             │
    │ auto-save │            │             │
    │           │            │             │
    │           ├────────────┤             │
    │           │ RISK #2:   │             │
    │           │ 1000ms     │             │
    │           │ batching   │             │
    │           │ window     │             │
    │           │            │             │
    │           │            ├─────────────┤
    │           │            │  RISK #3:   │
    │           │            │  IPC delay  │
    │           │            │  + FS write │
    └───────────┴────────────┴─────────────┘

Total Exposure: Up to ~1500ms after last keystroke
```

### 7.2 Scenario Analysis

#### Scenario A: Rapid Typing + Immediate Reload

```
0ms:    User types "Hello World"
50ms:   Auto-save timer reset
100ms:  User types more
150ms:  Auto-save timer reset
200ms:  User presses CMD+R
        ❌ No save triggered (< 450ms)
        ❌ Data lost
```

#### Scenario B: Type, Pause, Quick Reload

```
0ms:    User types "Important note"
450ms:  Auto-save triggered
500ms:  Save added to *writes atom
600ms:  User presses CMD+R
        ❌ Write in queue, not yet batched
        ❌ Data lost
```

#### Scenario C: Type, Wait, Reload During Batch

```
0ms:    User types "Meeting notes"
450ms:  Auto-save triggered
500ms:  Added to write queue
1450ms: Batch processing starts
1500ms: User presses CMD+R
        ❌ IPC in flight
        ❌ May or may not complete
        ⚠️ Race condition
```

#### Scenario D: Safe Reload

```
0ms:    User types "Project update"
450ms:  Auto-save triggered
500ms:  Added to write queue
1450ms: Batch sent to main process
1500ms: IPC completes
1600ms: File system write completes
2000ms: User presses CMD+R
        ✅ Data safely persisted
```

### 7.3 Database Mode vs File Mode Comparison

| Aspect | File-Based Mode | Database Mode |
|--------|----------------|---------------|
| **Auto-save delay** | 450ms | 450ms |
| **Write batching** | 1000ms | N/A (direct SQLite) |
| **Persistence layer** | Markdown files | SQLite + WAL |
| **Total risk window** | ~1450ms | ~450ms |
| **Recovery mechanism** | None | WAL may help |
| **Transit backup** | N/A | Every 30s |
| **Data loss risk** | ⚠️ High | ⚠️ Medium |

**Database Mode Advantage:**

SQLite's Write-Ahead Log (WAL) provides some protection:

```clojure
;; src/main/frontend/worker/db_worker.cljs:582-584
(def-thread-api :thread-api/export-db
  [repo]
  (when-let [^js db (worker-state/get-sqlite-conn repo :db)]
    (.exec db "PRAGMA wal_checkpoint(2)")))
```

The WAL means committed transactions are durable even without transit export, but **uncommitted edits are still lost**.

---

## 8. Comparison: File-Based vs Database Mode

### 8.1 Architectural Differences

#### File-Based Graph

```
Editor Buffer
    ↓
React State
    ↓
Auto-save (450ms)
    ↓
*writes atom ← IN-MEMORY
    ↓
file-writes-chan (10k buffer)
    ↓
Batch every 1000ms
    ↓
IPC to Main Process
    ↓
fs.writeFile()
    ↓
Markdown File
```

**Vulnerabilities:**
1. In-memory write queue
2. Async batching delay
3. IPC communication overhead
4. File system write latency

#### Database Graph

```
Editor Buffer
    ↓
React State
    ↓
Auto-save (450ms)
    ↓
DataScript Transaction
    ↓
SQLite Write
    ↓
Write-Ahead Log (WAL)
    ↓
Periodic Checkpoint
    ↓
Transit Export (30s)
    ↓
Disk File
```

**Vulnerabilities:**
1. Auto-save delay (450ms)
2. Uncommitted transactions
3. WAL not yet checkpointed

**Protection:**
- ✅ SQLite ACID guarantees
- ✅ WAL durability
- ✅ Faster than file batching

### 8.2 Risk Comparison Matrix

| Risk Factor | File Mode | DB Mode | Notes |
|-------------|-----------|---------|-------|
| **Auto-save delay** | 450ms | 450ms | Equal |
| **Write batching** | 1000ms | ~0ms | DB wins |
| **Queue in memory** | Yes (*writes) | Minimal | DB wins |
| **IPC overhead** | High | Medium | DB wins |
| **Crash recovery** | None | WAL | DB wins |
| **Transit backup** | N/A | 30s | DB advantage |
| **Total exposure** | ~1450ms | ~450ms | **DB 3x safer** |

---

## 9. Plugin System Protection

### 9.1 Plugin beforeunload API

Plugins can register cleanup handlers:

**Location:** `libs/src/LSPlugin.user.ts:596-599`

```typescript
beforeunload(callback: (e: any) => Promise<void>): void {
  if (!this.connected) return
  this._beforeunloadCallback = callback
}
```

**Usage Example:**
```typescript
logseq.beforeunload(async () => {
  console.log('Plugin unloading...')
  await savePluginState()
  await cleanupResources()
})
```

### 9.2 When Plugin beforeunload Fires

**Location:** `libs/src/LSPlugin.core.ts:947-959`

```typescript
const eventBeforeUnload = { unregister }

this.emit('beforeunload', eventBeforeUnload)

// Plugin has opportunity to:
// - Save state
// - Cancel operation (via unregister)
// - Clean up resources
```

**Trigger Conditions:**
- ✅ Plugin uninstall
- ✅ Plugin disable
- ✅ Graph switch
- ❌ **NOT on window reload**

### 9.3 Limitations

The plugin beforeunload system **does not protect core Logseq data**:

```typescript
// This ONLY runs for plugin cleanup
logseq.beforeunload(async () => {
  // Can save plugin data
  await savePluginPreferences()
  
  // CANNOT trigger Logseq auto-save
  // CANNOT flush write queue
  // CANNOT force transit export
})
```

**Why?** Because:
1. Plugins run in isolated sandboxes
2. No direct access to write queue
3. No API to force persistence
4. Window reload bypasses plugin lifecycle

---

## 10. Recommendations

### 10.1 For Users

#### Best Practices

1. **⏸️ Pause before reload**
   - Wait 2-3 seconds after last edit
   - Look for save indicator (if visible)
   - Ensure no pending operations

2. **💾 Manual save habit**
   - Use CMD+S / Ctrl+S before reload
   - Check for "saving..." indicators
   - Verify changes appear in graph

3. **🔄 Use graph refresh instead**
   - Prefer graph switching over window reload
   - Use Logseq's built-in refresh if available
   - Avoid CMD+R when possible

4. **📊 Database mode preference**
   - 3x safer than file-based mode
   - Better crash recovery
   - More reliable persistence

#### Warning Signs of Data Loss Risk

- ⚠️ Typing rapidly then reloading
- ⚠️ Reloading immediately after paste
- ⚠️ Reloading during "Saving..." indicator
- ⚠️ System lag + reload
- ⚠️ Network sync + reload

### 10.2 For Developers

#### Immediate Fixes (High Priority)

1. **🛡️ Add beforeunload handler**

```clojure
;; src/main/frontend/core.cljs
(defn setup-beforeunload-handler! []
  (.addEventListener js/window "beforeunload"
    (fn [e]
      (when-let [repo (state/get-current-repo)]
        ;; Check for pending writes
        (when (has-pending-writes? repo)
          ;; Prevent reload
          (.preventDefault e)
          ;; Show confirmation
          (set! (.-returnValue e) 
                "You have unsaved changes. Continue?"))))))
```

2. **🔄 Force flush on reload**

```clojure
(defn flush-writes-before-reload! []
  (when-let [writes @file/*writes]
    (when (seq writes)
      ;; Attempt synchronous flush
      (try
        (flush-write-queue-sync! writes)
        (catch :default e
          (log/error "Failed to flush writes" e))))))
```

3. **💾 Add visible save indicator**

```clojure
(defn show-save-status! []
  (let [pending? (has-pending-writes?)]
    (ui/show-indicator
      {:text (if pending? "Saving..." "Saved")
       :status (if pending? :pending :success)})))
```

#### Medium-Term Improvements

1. **📦 Persist write queue**

```clojure
;; Use IndexedDB to backup *writes
(defn persist-write-queue! []
  (let [writes @file/*writes]
    (idbkv/set! "pending-writes" writes)))

(defn restore-write-queue! []
  (p/let [writes (idbkv/get "pending-writes")]
    (when writes
      (reset! file/*writes writes)
      (process-pending-writes! writes))))
```

2. **⏱️ Reduce auto-save delay**

```clojure
;; Consider reducing from 450ms to 200ms
;; Or make it configurable
(def auto-save-delay 
  (or (config/get :auto-save-delay) 200))
```

3. **🚦 Block reload during writes**

```clojure
(defn prevent-reload-during-writes! []
  (when (has-pending-writes?)
    (disable-reload-shortcuts!)
    (show-warning! "Saving changes...")
    (p/let [_ (wait-for-writes-to-complete!)]
      (enable-reload-shortcuts!))))
```

#### Long-Term Architecture

1. **🏗️ Unified persistence layer**

```clojure
(defprotocol IPersistence
  (save! [this data])
  (flush! [this])
  (has-pending? [this]))

(defrecord SyncPersistence []
  IPersistence
  (save! [this data]
    ;; Immediate, blocking save
    (write-to-disk-sync! data))
  (has-pending? [this] false))

(defrecord AsyncPersistence [queue]
  IPersistence
  (save! [this data]
    ;; Queue for batch processing
    (enqueue! queue data))
  (flush! [this]
    ;; Force immediate flush
    (process-queue-sync! queue))
  (has-pending? [this]
    (seq @queue)))
```

2. **🔐 ACID guarantees for file mode**

Consider using a local SQLite database even for "file-based" mode:
- Store edits in SQLite immediately
- Export to markdown on background thread
- Provides crash recovery
- Maintains file compatibility

3. **☁️ Auto-recovery system**

```clojure
(defn setup-auto-recovery! []
  ;; Persist all edits to IndexedDB
  (add-watch state/state ::auto-backup
    (fn [_ _ old new]
      (when (editing-changed? old new)
        (backup-current-edit! new))))
  
  ;; Restore on reload
  (when-let [backup (get-backup!)]
    (show-recovery-prompt! backup)))
```

### 10.3 Testing Recommendations

#### Unit Tests

```clojure
(deftest test-write-queue-persistence
  (testing "Write queue survives reload"
    (add-write-request! 42)
    (is (= 1 (count @*writes)))
    
    ;; Simulate reload
    (persist-write-queue!)
    (reset! *writes {})
    
    ;; Restore
    (restore-write-queue!)
    (is (= 1 (count @*writes)))))

(deftest test-beforeunload-blocks-reload
  (testing "Pending writes prevent reload"
    (add-write-request! 42)
    (let [event (js/Event. "beforeunload")]
      (fire-beforeunload! event)
      (is (.-defaultPrevented event)))))
```

#### Integration Tests

```clojure
(deftest test-reload-data-persistence
  (testing "Data persists across reload"
    ;; Create test block
    (let [content "Test content"
          block-id (create-block! content)]
      
      ;; Trigger auto-save
      (wait-for-auto-save!)
      
      ;; Reload window
      (reload-window!)
      
      ;; Verify data
      (is (= content (get-block-content block-id))))))
```

#### E2E Tests

```bash
# Test CMD+R during typing
test "reload-during-typing" {
  type "Hello World"
  sleep 0.2  # Less than auto-save delay
  press "CMD+R"
  wait_for_reload
  assert_content_lost
}

# Test safe reload
test "reload-after-save" {
  type "Hello World"
  sleep 2  # More than save + batch
  press "CMD+R"
  wait_for_reload
  assert_content_preserved
}
```

---

## Appendix A: Code Locations Reference

### Critical Files

| File | Purpose | Key Functions |
|------|---------|---------------|
| `src/main/frontend/worker/file.cljs` | Write queue management | `*writes`, `file-writes-chan` |
| `src/main/frontend/handler/editor.cljs` | Auto-save logic | `auto-save!`, `*auto-save-timeout` |
| `src/main/frontend/persist_db.cljs` | Transit export | `export-current-graph!` |
| `src/main/frontend/persist_db/browser.cljs` | DB persistence | `<export-db`, `transact!` |
| `src/main/frontend/worker/db_worker.cljs` | Worker API | `:thread-api/export-db` |
| `src/electron/electron/window.cljs` | Window lifecycle | `close-handler` |
| `src/electron/electron/core.cljs` | App lifecycle | `before-quit` |
| `deps/db/src/logseq/db/sqlite/util.cljs` | Transit serialization | `write-transit-str` |

### Key Data Structures

```clojure
;; In-memory write queue (FILE MODE)
(defonce *writes (atom {}))          ;; Request ID → Page ID

;; Auto-save timer
(defonce *auto-save-timeout (atom nil))

;; Write channel
(defonce file-writes-chan 
  (async/chan 10000))

;; Last synced transaction (DB MODE)
(defonce *last-synced-graph->tx (atom {}))
```

---

## Appendix B: Glossary

| Term | Definition |
|------|------------|
| **Auto-save** | Automatic saving triggered after 450ms of inactivity |
| **Batch write** | Grouping multiple write operations for efficiency |
| **beforeunload** | Browser/Electron event fired before page unloads |
| **DataScript** | In-memory database used by Logseq |
| **IPC** | Inter-Process Communication between Electron main/renderer |
| **Transit** | Serialization format for Clojure data structures |
| **WAL** | Write-Ahead Log, SQLite's durability mechanism |
| **Write queue** | In-memory queue tracking pending file writes |

---

## Appendix C: Change History

| Date | Investigator | Changes |
|------|-------------|---------|
| 2025-10-24 | Code Detective | Initial comprehensive investigation |

---

## Conclusion

The CMD+R/Ctrl+R window reload functionality in Logseq currently **lacks safeguards against data loss**. While database mode is significantly safer than file-based mode due to SQLite's WAL, both modes expose a vulnerability window where uncommitted edits can be lost.

**Critical Findings:**
1. ❌ No beforeunload handler for main window
2. ❌ In-memory write queue not persisted
3. ❌ Transit files do not store pending writes
4. ⚠️ Auto-save delay creates 450ms risk window
5. ⚠️ File mode adds additional 1000ms batching risk

**Priority Actions:**
1. Implement beforeunload warning for pending writes
2. Add visual save status indicator
3. Consider persisting write queue to IndexedDB
4. Reduce auto-save delay or make configurable
5. Add comprehensive testing for reload scenarios

This investigation reveals a **known architectural limitation** that should be addressed to prevent user data loss.

---

**Document Version:** 1.0  
**Last Updated:** October 24, 2025  
**Status:** Comprehensive Analysis Complete ✅
