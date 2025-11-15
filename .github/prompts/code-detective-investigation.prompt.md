# Code Detective Investigation Prompt Template

**Purpose:** A reusable prompt template for conducting comprehensive code investigations in the Logseq codebase (or similar ClojureScript/Electron applications).

**Version:** 1.0  
**Last Updated:** October 25, 2025

---

## Template Usage

Copy and customize this template for any functionality investigation. Replace `[PLACEHOLDERS]` with specific details.

---

## Investigation Prompt Template

```markdown
# Investigation Request: [FUNCTIONALITY NAME]

## Context
- **Feature/Functionality:** [What you're investigating]
- **User Action:** [Specific user interaction or keyboard shortcut]
- **Platform:** [Electron/Web/Mobile, macOS/Windows/Linux]
- **Graph Mode:** [File-based / Database mode / Both]

## Investigation Goals

Act as a code detective and conduct a comprehensive investigation to answer:

1. **What exactly happens** when [USER ACTION]?
2. **What data structures** are involved in this process?
3. **What persistence mechanisms** are used?
4. **What risks exist** for data loss or inconsistency?
5. **How does behavior differ** between [MODE A] and [MODE B]?

## Specific Questions

[List 3-5 specific questions you want answered, for example:]

1. When [ACTION] occurs, does it trigger [MECHANISM]?
2. Are pending [DATA TYPE] stored in [STORAGE LOCATION]?
3. Is there a [LIFECYCLE HOOK] that prevents [PROBLEM]?
4. What happens to [DATA STRUCTURE] during [EVENT]?
5. How long is the exposure window for [RISK]?

## Investigation Requirements

### Phase 1: Discovery
- [ ] Identify entry points (keyboard shortcuts, UI handlers, IPC calls)
- [ ] Map the execution flow from user action to final effect
- [ ] Document all data structures involved
- [ ] Find relevant lifecycle hooks and event handlers

### Phase 2: Deep Dive
- [ ] Analyze persistence mechanisms (files, SQLite, IndexedDB, transit)
- [ ] Identify in-memory vs. persisted state
- [ ] Document timing windows (delays, batching, async operations)
- [ ] Compare different modes (file-based vs. database, platforms)

### Phase 3: Risk Analysis
- [ ] Identify data loss scenarios
- [ ] Document race conditions or timing vulnerabilities
- [ ] Find missing safeguards or handlers
- [ ] Quantify exposure windows (in milliseconds)

### Phase 4: Documentation
- [ ] Create comprehensive guide with code references
- [ ] Include execution flow diagrams (ASCII/text)
- [ ] Provide scenario analysis with timelines
- [ ] Add recommendations for users and developers

## Output Format

Create a comprehensive markdown guide that includes:

### 1. Executive Summary
- Key findings (3-5 bullet points)
- Critical risks or issues discovered
- Quick reference for developers

### 2. Technical Deep Dive
For each major component:
- **Purpose:** What it does
- **Location:** File path and key functions
- **Code snippets:** Relevant implementation details
- **Data structures:** In-memory atoms, databases, files
- **Dependencies:** What it calls/uses

### 3. Flow Diagrams
Use ASCII diagrams to show:
- Execution flow from trigger to completion
- Data flow between components
- Timeline showing delays and exposure windows

Example:
```
User Action → Handler → State Update → Persistence
   [0ms]      [10ms]      [450ms]       [1450ms]
    │           │            │              │
    └───────────┴────────────┴──────────────┘
              Risk Window: 1450ms
```

### 4. Comparison Tables
Compare different modes, platforms, or approaches:

| Aspect | Mode A | Mode B | Winner |
|--------|--------|--------|--------|
| Speed  | Fast   | Slow   | Mode A |
| Safety | Low    | High   | Mode B |

### 5. Scenario Analysis
Provide concrete examples:
- **Scenario A:** [Description]
  - Timeline
  - Expected result
  - Actual result
  - Risk level

### 6. Code Location Reference
Table of all critical files:

| File | Purpose | Key Functions | Lines |
|------|---------|---------------|-------|

### 7. Recommendations

#### For Users
- Best practices
- Warning signs
- Workarounds

#### For Developers
- Immediate fixes (with code sketches)
- Medium-term improvements
- Long-term architecture changes

#### For Testing
- Unit tests needed
- Integration test scenarios
- E2E test cases

### 8. Appendices
- Glossary of technical terms
- Change history
- Related investigations

## Investigation Checklist

Use this checklist to ensure thoroughness:

### Code Search Strategy
- [ ] Semantic search for concept/functionality
- [ ] Grep for specific keywords and patterns
- [ ] File name patterns (e.g., `**/*handler*.cljs`)
- [ ] Follow IPC message chains
- [ ] Trace state atom usage (`*atom-name`)
- [ ] Find event listeners (`.on`, `addEventListener`)

### Key Patterns to Look For

#### ClojureScript Patterns
```clojure
;; State atoms (in-memory)
(defonce *state (atom {}))

;; IPC calls (Electron)
(ipc/ipc :message-name args)

;; Thread API (Worker)
(def-thread-api :thread-api/function-name ...)

;; Event handlers
(.on object "event-name" handler-fn)

;; Lifecycle hooks
(defn on-mount [] ...)
(defn on-unmount [] ...)

;; Timers and delays
(js/setTimeout fn delay)
(js/setInterval fn interval)
```

#### Electron Patterns
```javascript
// Main process handlers
ipcMain.handle('channel', handler)

// Window lifecycle
win.on('close', handler)
win.on('closed', handler)

// App lifecycle
app.on('before-quit', handler)
app.on('window-all-closed', handler)

// Browser events
window.addEventListener('beforeunload', handler)
```

### Critical Questions for Each Component

1. **Is it synchronous or asynchronous?**
   - Timing implications
   - Race condition risks

2. **Is state persisted or in-memory?**
   - Data loss risks
   - Recovery mechanisms

3. **What are the failure modes?**
   - Error handling
   - Fallback behavior

4. **Are there lifecycle hooks?**
   - Cleanup opportunities
   - Save points

5. **What are the timing constraints?**
   - Delays (debouncing, throttling)
   - Batching windows
   - Timeout values

### Data Structure Documentation Template

For each important data structure, document:

```markdown
#### [Structure Name] (e.g., *writes atom)

**Location:** `path/to/file.cljs:LINE`

**Type:** `(atom {})` / `SQLite table` / `IndexedDB store`

**Purpose:** Brief description

**Structure:**
```clojure
;; Example structure
{:key1 "value"
 :key2 {:nested "data"}}
```

**Lifecycle:**
- Created: [When/Where]
- Updated: [Triggers]
- Cleared: [Events]
- Persisted: [Yes/No/How]

**Risk Level:** [Low/Medium/High]

**Why it matters:** Explanation of importance
```

## Search Query Templates

### Semantic Search Queries
```
"[functionality] keyboard shortcut handler event listener"
"[data-type] persistence save write storage"
"[event] lifecycle hook cleanup teardown"
"pending writes queue buffer uncommitted changes"
"window reload refresh close beforeunload"
```

### Grep Patterns (Regex)
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

## Example Investigation Flow

### Step 1: Identify Entry Point
```
User presses [SHORTCUT]
  ↓
Search: "keyboard shortcut [KEY]"
Search: "accelerator [KEY]"
  ↓
Find: Electron menu or shortcut handler
```

### Step 2: Trace Execution
```
Handler identified
  ↓
Read handler implementation
  ↓
Follow function calls
  ↓
Identify IPC messages
  ↓
Find main process handler
```

### Step 3: Find State Changes
```
Look for:
- (swap! *atom ...)
- (reset! *atom ...)
- (transact! ...)
- (.send ipc ...)
```

### Step 4: Check Persistence
```
Ask:
- Is this saved to disk?
- When does save occur?
- What triggers persistence?
- Are there delays/batching?
```

### Step 5: Identify Risks
```
Look for:
- In-memory only state
- Async operations without error handling
- Missing lifecycle hooks
- No beforeunload handlers
- Long delay timers
- Unbounded queues
```

## Common Investigation Pitfalls

### Avoid
- ❌ Stopping after finding one code path
- ❌ Ignoring error cases
- ❌ Assuming sync when it's async
- ❌ Missing platform-specific behavior
- ❌ Not checking both file and DB modes

### Do
- ✅ Follow all code paths (success + error)
- ✅ Check timing diagrams
- ✅ Test assumptions with code reading
- ✅ Document ambiguities
- ✅ Note version/branch specifics

## Quality Checklist

Before finalizing your investigation:

- [ ] All claims backed by code references (file:line)
- [ ] Timing values quantified (ms)
- [ ] Data structures documented with examples
- [ ] Flow diagrams show complete path
- [ ] Risks clearly identified and explained
- [ ] Recommendations are actionable
- [ ] Code snippets are accurate
- [ ] Technical terms defined in glossary
- [ ] Cross-references between sections work

## Documentation Standards

### Code References
Always include:
```markdown
**Location:** `path/to/file.cljs:LINE-LINE`
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
```markdown
- 🟢 **Low Risk:** Unlikely, minor impact
- 🟡 **Medium Risk:** Possible, moderate impact
- 🔴 **High Risk:** Likely, severe impact
```

### Visual Indicators
```markdown
✅ Works correctly
❌ Missing or broken
⚠️ Risky behavior
⏱️ Timing-dependent
🔄 Async operation
💾 Persistence point
🔴 In-memory only
```

## Collaboration Tips

### For Reviewers
When reviewing investigation:
1. Verify code references are accurate
2. Check if timing values are realistic
3. Validate risk assessments
4. Test recommendations are feasible
5. Ensure completeness

### For Users of Investigation
Use the guide to:
1. Understand how feature works
2. Identify risks in your workflow
3. Plan fixes or workarounds
4. Write tests
5. Onboard new developers

---

## Example: Filled Template

[See CMD-R-RELOAD-DATA-PERSISTENCE-GUIDE.md as reference example]

---

## Maintenance

### When to Update This Template
- New investigation patterns discovered
- Better search strategies identified
- Common pitfalls found
- Documentation standards evolve

### Version History
| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-10-25 | Initial template |

---

## Related Resources

- [AGENTS.md](../AGENTS.md) - Agent guidelines for this repository
- [review.md](./review.md) - Code review checklist
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Contribution guidelines

---

## License

This template is part of the Logseq project and follows the same license.
```
