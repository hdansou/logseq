# Code Detective Investigation Instructions

**Purpose:** GitHub Copilot instructions for conducting comprehensive code investigations.

**Usage:** `@workspace /investigate [functionality or feature]`

---

## Investigation Command

### @workspace /investigate - Code Detective Investigation

When the user asks to investigate a feature or functionality, follow the comprehensive investigation framework defined in `prompts/code-detective-investigation.md`.

**Usage Examples:**
- `@workspace /investigate what happens when user presses CMD+R`
- `@workspace /investigate how auto-save works`
- `@workspace /investigate file persistence mechanism`
- `@workspace /investigate data loss risks during graph switching`

---

## Investigation Process

### Phase 1: Discovery
- Identify entry points (keyboard shortcuts, UI handlers, IPC calls)
- Map execution flow from user action to completion
- Document all data structures involved (atoms, databases, files)
- Find relevant lifecycle hooks and event handlers

### Phase 2: Deep Dive
- Analyze persistence mechanisms (files, SQLite, IndexedDB, transit)
- Identify in-memory vs persisted state
- Document timing windows (delays, batching, async operations)
- Compare different modes (file-based vs database, platforms)

### Phase 3: Risk Analysis
- Identify data loss scenarios
- Document race conditions or timing vulnerabilities
- Find missing safeguards or handlers
- Quantify exposure windows (in milliseconds)

### Phase 4: Documentation
- Create comprehensive guide with code references
- Include execution flow diagrams (ASCII/text)
- Provide scenario analysis with concrete examples
- Add recommendations for users and developers

---

## Search Strategies

### ClojureScript Patterns

```clojure
(defonce *state (atom {}))           # State atoms (in-memory)
(ipc/ipc :message-name args)         # IPC calls to Electron main
(def-thread-api :name ...)           # Worker API definitions
(.on object "event" handler)         # Event listeners
(js/setTimeout fn ms)                # Timers and delays
(p/let [result (async-fn)] ...)      # Promise-based async
```

### Electron Patterns

```javascript
ipcMain.handle('channel', handler)          // Main process IPC
win.on('close', handler)                    // Window lifecycle
app.on('before-quit', handler)              // App lifecycle
window.addEventListener('beforeunload', fn) // Browser events
```

### Search Query Templates

**Semantic searches:**
- `"[functionality] keyboard shortcut handler event listener"`
- `"[data-type] persistence save write storage"`
- `"[event] lifecycle hook cleanup teardown"`
- `"pending writes queue buffer uncommitted changes"`
- `"window reload refresh close beforeunload"`

**Grep patterns:**
```regex
(\.on|\.addEventListener).*["']EVENT-NAME    # Event handlers
ipc.*:MESSAGE-NAME                           # IPC calls
defonce.*\*[a-z-]+                          # State atoms
(on-mount|on-unmount|before.*quit)          # Lifecycle
(setTimeout|setInterval)                     # Timers
(save-db|export-db|transact)                # Database ops
```

---

## Output Format Requirements

### 1. Executive Summary
- Key findings (3-5 bullet points with risk indicators)
- Critical issues discovered with severity ratings
- Quick reference table for developers

### 2. Technical Deep Dive

For each major component, document:

**Location:** `path/to/file.cljs:LINE-LINE`

**Purpose:** Brief description of what it does

**Code Example:**
```clojure
;; Relevant implementation
```

**Data Structures:**
- In-memory atoms
- Database tables
- File storage

**Dependencies:** What it calls/uses

### 3. Flow Diagrams

Use ASCII diagrams:

```
User Action → Handler → State Update → Persistence
   [0ms]      [10ms]      [450ms]       [1450ms]
    │           │            │              │
    └───────────┴────────────┴──────────────┘
              Risk Window: 1450ms
```

### 4. Comparison Tables

```markdown
| Aspect | File Mode | DB Mode | Winner |
|--------|-----------|---------|--------|
| Speed  | Slow      | Fast    | DB     |
| Safety | Low       | High    | DB     |
```

### 5. Scenario Analysis

```markdown
#### Scenario A: [Description]

Timeline:
0ms:    User types "text"
450ms:  Auto-save triggered
600ms:  User presses CMD+R
        ❌ Data in write queue
        ❌ Data lost
```

### 6. Code Location Reference

```markdown
| File | Purpose | Key Functions | Lines |
|------|---------|---------------|-------|
| path/to/file.cljs | Description | func1, func2 | 100-200 |
```

### 7. Recommendations

**For Users:**
- Best practices with risk avoidance
- Warning signs to watch for
- Safe workflows

**For Developers:**
- Immediate fixes (with code sketches)
- Medium-term improvements
- Long-term architecture changes

**For Testing:**
- Unit test scenarios
- Integration test cases
- E2E test examples

### 8. Appendices
- Glossary of technical terms
- Related investigations
- Change history

---

## Critical Questions to Answer

For every investigation, explicitly address:

1. **Is it synchronous or asynchronous?**
   - Document timing and delays
   - Identify race conditions

2. **Is state persisted or in-memory?**
   - Risk of data loss on crash/reload
   - Recovery mechanisms available?

3. **What are the failure modes?**
   - Error handling present?
   - Fallback behavior?

4. **Are there lifecycle hooks?**
   - beforeunload handlers?
   - Cleanup on close/quit?

5. **What are the timing constraints?**
   - Auto-save delays
   - Batching windows
   - Timeout values

6. **What are the data loss risks?**
   - Quantify exposure windows
   - Rate severity (🟢🟡🔴)

---

## Documentation Standards

### Code References
Always format as:
```markdown
**Location:** `src/main/frontend/handler/editor.cljs:2850-2900`
```

### Timing Information
Quantify all delays:
```markdown
- Auto-save delay: **450ms**
- Batch interval: **1000ms**
- Total exposure: **~1450ms**
```

### Risk Ratings
Use consistent scale:
- 🟢 **Low Risk:** Unlikely, minor impact
- 🟡 **Medium Risk:** Possible, moderate impact
- 🔴 **High Risk:** Likely, severe impact

### Visual Indicators
- ✅ Works correctly / Available
- ❌ Missing / Broken / Data loss
- ⚠️ Risky behavior / Warning
- ⏱️ Timing-dependent
- 🔄 Async operation
- 💾 Persistence point
- 🔴 In-memory only (lost on reload)
- 🟢 Persisted (survives reload)

---

## Logseq-Specific Context

### Repository Structure
- `src/main/` - Core application logic
- `src/electron/` - Electron desktop app
- `src/main/frontend/worker/` - WebWorker code
- `src/main/frontend/components/` - UI components
- `deps/` - Internal dependencies/modules

### Key Technologies
- ClojureScript/Clojure
- Electron (desktop)
- React/Rum (UI)
- DataScript (in-memory database)
- SQLite (db-based graphs)
- Transit (serialization)

### Graph Modes
- **File-based:** Stores data in markdown files via file system writes
- **Database:** Stores data in SQLite with periodic transit exports

### Common Code Locations
- Auto-save: `src/main/frontend/handler/editor.cljs`
- File writes: `src/main/frontend/worker/file.cljs`
- DB persistence: `src/main/frontend/persist_db.cljs`
- Worker API: `src/main/frontend/worker/db_worker.cljs`
- Electron handlers: `src/electron/electron/handler.cljs`
- Transit utils: `deps/db/src/logseq/db/sqlite/util.cljs`

### Common Patterns

**State Management:**
```clojure
;; In-memory state (lost on reload)
(defonce *writes (atom {}))

;; Persisted state (survives reload)
(idbkv/set! key value)
```

**Persistence:**
```clojure
;; File-based graph
(ipc/ipc :writeFile path content)

;; Database graph
(state/<invoke-db-worker :thread-api/transact repo tx-data tx-meta)

;; Transit export
(<export-db repo {})
```

**Async Operations:**
```clojure
;; Promise-based
(p/let [result (async-operation)]
  (process result))

;; Core.async
(go-loop []
  (let [value (async/<! channel)]
    (process value))
  (recur))
```

**Timing:**
```clojure
;; Auto-save delay (450ms)
(js/setTimeout save-fn 450)

;; Periodic export (30 seconds)
(js/setInterval export-fn (* 30 1000))
```

**IPC Communication:**
```clojure
;; Renderer -> Main
(ipc/ipc :message-name args)

;; Worker API
(state/<invoke-db-worker :thread-api/function-name args)
```

---

## Investigation Checklist

Before completing an investigation, verify:

- [ ] Entry point identified (shortcut/handler/IPC)
- [ ] Execution flow traced and documented
- [ ] All data structures documented with examples
- [ ] Persistence mechanisms analyzed
- [ ] Timing windows quantified (in ms)
- [ ] Risks identified with severity ratings
- [ ] File mode vs DB mode compared
- [ ] Lifecycle hooks checked (beforeunload, close, quit)
- [ ] Flow diagrams created
- [ ] Scenario analysis provided
- [ ] Code references include file:line
- [ ] Recommendations are actionable
- [ ] All claims backed by code evidence

---

## Quality Standards

### All Claims Must Be:
1. **Backed by code references** - Include file paths and line numbers
2. **Quantified** - Specify delays, timeouts, buffer sizes
3. **Tested** - Verify assumptions by reading actual code
4. **Complete** - Cover both success and error paths
5. **Cross-checked** - Verify across file-based and database modes

### Avoid:
- ❌ Assumptions without code verification
- ❌ Vague timing descriptions ("soon", "eventually")
- ❌ Missing error cases
- ❌ Platform-specific behavior without noting it
- ❌ Stopping after one code path

### Do:
- ✅ Follow all execution paths
- ✅ Document error handling
- ✅ Test timing assumptions
- ✅ Note platform differences
- ✅ Consider edge cases

---

## Example Reference

See `docs/CMD-R-RELOAD-DATA-PERSISTENCE-GUIDE.md` for a complete investigation example demonstrating all these standards and practices.

---

## Related Resources

- **Full Template:** `prompts/code-detective-investigation.md`
- **Review Standards:** `prompts/review.md`
- **Agent Guidelines:** `AGENTS.md`
- **Contributing:** `CONTRIBUTING.md`
