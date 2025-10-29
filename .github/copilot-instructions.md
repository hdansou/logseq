# GitHub Copilot Instructions for Logseq

## Custom Commands

### @workspace /investigate - Code Detective Investigation

Follow the comprehensive investigation framework in `.github/code-detective-investigation.instructions.md`.

**Quick Usage:**
- `@workspace /investigate what happens when user presses CMD+R`
- `@workspace /investigate how auto-save works`
- `@workspace /investigate file persistence mechanism`

**See:** `.github/code-detective-investigation.instructions.md` for full investigation process and standards.

---

### @workspace /review - Code Review with Logseq Standards

When reviewing code, follow the guidelines in `prompts/review.md` along with these Logseq-specific checks:

**ClojureScript Specific:**
- Use `clojure-mcp` toolkit for .clj/.cljs/.cljc files (MANDATORY)
- Never use general file editing tools for Clojure code
- Run `bb dev:lint-and-test` before submitting
- Check `defkeywords` definitions for keyword usage

**Common Patterns:**
- State management: `defonce *atom-name (atom {})`
- Worker API: `def-thread-api :thread-api/name`
- IPC calls: `(ipc/ipc :message-name args)`
- Persistence: `save-db!`, `export-db`, transit files

**Testing:**
- Unit tests: `bb dev:test -i focus` for focused tests
- E2E basic: `bb dev:e2e-basic-test`
- E2E RTC: `bb dev:e2e-rtc-extra-test`

---

### @workspace /new-changelog - Generate Changelog from Git Commits

Generate a professional, AI-powered changelog from git commit history.

**Usage:**
```
@workspace /new-changelog <start-date> <end-date> [--format=<changelog|release>]
```

**Examples:**
- `@workspace /new-changelog 2025-01-05 2025-01-15` - Generate changelog
- `@workspace /new-changelog 2025-01-01 2025-01-31 --format=release` - Generate release notes

**Process:**
1. Fetch git commits from all remote branches for the date range
2. Generate raw commit log in markdown format
3. Apply AI analysis following `prompts/changelog-generation.md`
4. Create user-facing changelog with:
   - Spotlight section for major features
   - Categorized changes (New Feature, Enhancement, Bug Fix, etc.)
   - Functional area grouping
   - Contributor list

**Output Formats:**
- `changelog` (default): Detailed changelog with all commits organized by category and functional area
- `release`: Condensed release notes focusing on user-facing changes and highlights

**See:** `prompts/changelog-generation.md` for detailed formatting guidelines and examples.

---

## Repository Context

**Structure:**
- `src/main/`: Core application logic
- `src/electron/`: Electron desktop app
- `src/main/frontend/worker/`: WebWorker code
- `src/main/frontend/components/`: UI components
- `deps/`: Internal dependencies/modules
- `clj-e2e/`: End-to-end test code

**Key Technologies:**
- ClojureScript/Clojure
- Electron (desktop)
- React/Rum (UI)
- DataScript (in-memory database)
- SQLite (db-based graphs)
- Transit (serialization)

**Graph Modes:**
- **File-based:** Stores data in markdown files
- **Database:** Stores data in SQLite with transit exports

**Common Code Locations:**
- Auto-save: `src/main/frontend/handler/editor.cljs`
- File writes: `src/main/frontend/worker/file.cljs`
- DB persistence: `src/main/frontend/persist_db.cljs`
- Worker API: `src/main/frontend/worker/db_worker.cljs`
- Electron handlers: `src/electron/electron/handler.cljs`
- Transit utils: `deps/db/src/logseq/db/sqlite/util.cljs`

**Important Rules:**
1. Always use `clojure-mcp` toolkit for Clojure code editing
2. Run `clojure_inspect_project` before code analysis
3. Check both file-based and database modes
4. Consider timing/async operations
5. Look for in-memory vs persisted state
6. Follow review checklist in `prompts/review.md`

---

## Code Patterns Reference

### State Management
```clojure
;; In-memory state (lost on reload)
(defonce *writes (atom {}))

;; Persisted state (survives reload)
(defn save-to-indexeddb! [key value]
  (idbkv/set! key value))
```

### Persistence
```clojure
;; File-based graph
(defn write-file! [path content]
  (ipc/ipc :writeFile path content))

;; Database graph
(defn transact! [repo tx-data tx-meta]
  (state/<invoke-db-worker :thread-api/transact repo tx-data tx-meta))

;; Transit export
(defn export-current-graph! []
  (<export-db repo {}))
```

### Async Operations
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

### Timing
```clojure
;; Auto-save delay
(js/setTimeout save-fn 450)

;; Periodic export
(js/setInterval export-fn (* 30 1000))  ;; 30 seconds
```

### IPC Communication
```clojure
;; Renderer -> Main
(ipc/ipc :message-name args)

;; Worker API
(state/<invoke-db-worker :thread-api/function-name args)
```

---

## Best Practices

1. **Always verify code references** - Include file paths and line numbers
2. **Quantify timing** - Specify delays in milliseconds
3. **Document risks** - Use clear risk ratings
4. **Provide examples** - Show concrete scenarios
5. **Consider all modes** - File-based AND database
6. **Check lifecycle** - beforeunload, on-close, before-quit
7. **Follow the template** - Use investigation framework consistently
8. **Test your findings** - Verify claims with actual code execution

---

## Quick Investigation Checklist

- [ ] Identify entry point (shortcut/handler/IPC)
- [ ] Trace execution flow
- [ ] Find data structures (atoms, databases)
- [ ] Check persistence mechanisms
- [ ] Document timing/delays
- [ ] Identify risks (in-memory state, race conditions)
- [ ] Compare file vs database modes
- [ ] Check lifecycle hooks
- [ ] Create flow diagrams
- [ ] Provide recommendations
- [ ] Add code references (file:line)
- [ ] Include concrete scenarios
