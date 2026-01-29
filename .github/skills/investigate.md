# investigate

Conduct comprehensive code investigation following the Code Detective Investigation framework.

## Description

This skill provides a systematic approach to understanding how code works by tracing execution flows, identifying data structures, analyzing persistence mechanisms, and documenting risks. Use this for investigating user actions, features, or system behaviors.

## Usage

```
@workspace /investigate [FUNCTIONALITY_NAME]
```

**Examples:**

- `@workspace /investigate auto-save mechanism`
- `@workspace /investigate CMD-R reload behavior`
- `@workspace /investigate graph export process`
- `@workspace /investigate plugin API lifecycle`

## Instructions

s
When the user invokes this skill:

1. **Discovery Phase**

   - Identify entry points (keyboard shortcuts, UI handlers, IPC calls, API endpoints)
   - Use semantic search, grep, and file search strategically
   - Map execution flow from trigger to completion
   - Document all data structures involved

2. **Deep Dive Phase**

   - Analyze persistence mechanisms (files, SQLite, IndexedDB, transit)
   - Identify in-memory vs. persisted state
   - Document timing windows (delays, batching, async operations)
   - Compare different modes/platforms if applicable

3. **Risk Analysis Phase**

   - Identify data loss scenarios
   - Document race conditions or timing vulnerabilities
   - Find missing safeguards or handlers
   - Quantify exposure windows in milliseconds

4. **Documentation Phase**
   - Create comprehensive guide with:
     - Executive summary (key findings, critical risks)
     - Technical deep dive with code references
     - Flow diagrams (ASCII/text based)
     - Comparison tables for different modes
     - Scenario analysis with timelines
     - Code location reference table
     - Recommendations (users, developers, testing)

## Investigation Checklist

### Code Search Strategy

- [ ] Semantic search for concepts/functionality
- [ ] Grep for specific keywords and patterns
- [ ] File name patterns (e.g., `**/*handler*.cljs`)
- [ ] Follow IPC message chains
- [ ] Trace state atom usage (`*atom-name`)
- [ ] Find event listeners (`.on`, `addEventListener`)

### Critical Questions

1. Is it synchronous or asynchronous?
2. Is state persisted or in-memory?
3. What are the failure modes?
4. Are there lifecycle hooks?
5. What are the timing constraints?

### Documentation Requirements

- [ ] All claims backed by code references (file:line)
- [ ] Timing values quantified (ms)
- [ ] Data structures documented with examples
- [ ] Flow diagrams show complete path
- [ ] Risks clearly identified and explained
- [ ] Recommendations are actionable

## Output Format

Create a markdown guide with:

### 1. Executive Summary

- Key findings (3-5 bullet points)
- Critical risks discovered
- Quick reference for developers

### 2. Technical Deep Dive

For each component:

- **Purpose:** What it does
- **Location:** File path and key functions
- **Code snippets:** Relevant implementation
- **Data structures:** Atoms, databases, files
- **Dependencies:** What it calls/uses

### 3. Flow Diagrams

```
User Action → Handler → State Update → Persistence
   [0ms]      [10ms]      [450ms]       [1450ms]
```

### 4. Comparison Tables

Compare modes, platforms, or approaches

### 5. Scenario Analysis

Concrete examples with timelines and risk levels

### 6. Code Location Reference

Table of critical files with purposes and line numbers

### 7. Recommendations

- For users (best practices, warnings)
- For developers (fixes, improvements)
- For testing (test cases needed)

## Key Patterns to Look For

### ClojureScript

```clojure
(defonce *state (atom {}))              ; In-memory state
(ipc/ipc :message-name args)            ; IPC calls
(def-thread-api :thread-api/fn ...)     ; Worker API
(.on object "event" handler)            ; Event handlers
(js/setTimeout fn delay)                ; Timers
```

### Electron

```javascript
ipcMain.handle('channel', handler)      ; Main process
win.on('close', handler)                ; Window lifecycle
app.on('before-quit', handler)          ; App lifecycle
window.addEventListener('beforeunload') ; Browser events
```

## Success Criteria

- ✅ Complete execution flow documented
- ✅ All data structures identified
- ✅ Persistence mechanisms explained
- ✅ Risks quantified and rated (🟢/🟡/🔴)
- ✅ Code references include file:line
- ✅ Recommendations are actionable
- ✅ Diagrams show timing and flow
- ✅ Both file-based and DB modes considered

## Risk Rating System

- 🟢 **Low Risk:** Unlikely, minor impact
- 🟡 **Medium Risk:** Possible, moderate impact
- 🔴 **High Risk:** Likely, severe impact

## Visual Indicators

- ✅ Works correctly
- ❌ Missing or broken
- ⚠️ Risky behavior
- ⏱️ Timing-dependent
- 🔄 Async operation
- 💾 Persistence point
- 🔴 In-memory only

## Search Query Templates

### Semantic Search

```
"[functionality] keyboard shortcut handler event listener"
"[data-type] persistence save write storage"
"[event] lifecycle hook cleanup teardown"
"pending writes queue buffer uncommitted changes"
"window reload refresh close beforeunload"
```

### Grep Patterns

```
# Event handlers
(\.on|\.addEventListener).*["']EVENT-NAME

# IPC calls
ipc.*:MESSAGE-NAME

# State atoms
defonce.*\*[a-z-]+

# Lifecycle functions
(on-mount|on-unmount|before.*quit|window.*close)

# Async operations
(async/|promise|p/let|p/do!)

# Timers
(setTimeout|setInterval|js/setTimeout)

# Database operations
(save-db|export-db|import-db|transact)
```

### File Search Patterns

```
**/*[keyword]*.cljs
**/*handler*.cljs
**/*worker*.cljs
**/electron/*.cljs
**/*persist*.cljs
```

## Logseq-Specific Context

### Graph Modes

- **File-based:** Stores data in markdown files on disk
- **Database:** Stores data in SQLite with transit exports

### State Management

- In-memory atoms: `(defonce *name (atom {}))`
- Persisted: IndexedDB, SQLite, file writes
- Transit serialization for exports

### Common Code Locations

- Auto-save: `src/main/frontend/handler/editor.cljs`
- File writes: `src/main/frontend/worker/file.cljs`
- DB persistence: `src/main/frontend/persist_db.cljs`
- Worker API: `src/main/frontend/worker/db_worker.cljs`
- Electron handlers: `src/electron/electron/handler.cljs`
- Transit utils: `deps/db/src/logseq/db/sqlite/util.cljs`

### Testing Commands

- Lint & test: `bb dev:lint-and-test`
- Single test: `bb dev:test -v <namespace/testcase-name>`
- E2E basic: `bb dev:e2e-basic-test`
- E2E RTC: `bb dev:e2e-rtc-extra-test`

## Important Notes

- **MUST** use `clojure-mcp` toolkit for all .clj/.cljs/.cljc file analysis
- Never use general file editing tools for Clojure code
- Verify all code references before citing (file:line)
- Quantify all timing in milliseconds
- Consider both file-based and database modes
- Check platform differences (Electron/Web/Mobile)

## Reference

Full investigation framework: `.github/prompts/code-detective-investigation.prompt.md`
