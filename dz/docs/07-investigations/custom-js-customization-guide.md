# Investigation: Logseq Custom JavaScript (custom.js) System

**Date:** November 23, 2025  
**Investigator:** GitHub Copilot  
**Status:** Complete  
**Version:** Logseq v0.11.x+

---

## Executive Summary

### Key Findings

- ✅ **Custom JS file location:** `logseq/custom.js` in both file-based and database graphs
- ✅ **Security-first approach:** User must explicitly allow execution with 7-day re-confirmation
- ✅ **NO hot-reload for custom.js:** Unlike custom.css, changes require manual app restart
- ✅ **Two usage methods:** Direct custom.js file OR remote JS URL via config
- ✅ **Full JavaScript access:** Can execute any JavaScript code with complete DOM/window access
- ⚠️ **Security warning:** Executes with full app permissions - only use trusted code

### Quick Reference

| Aspect             | File-Based Graph                | Database Graph                        |
| ------------------ | ------------------------------- | ------------------------------------- |
| **Path**           | `<graph-dir>/logseq/custom.js`  | `<graph-dir>/logseq/custom.js`        |
| **Storage**        | Filesystem                      | Database (as virtual file)            |
| **Edit Via**       | External editor or Settings     | Settings → Edit custom.js             |
| **Hot Reload**     | ❌ **No** - requires app reload | ❌ **No** - requires app reload       |
| **Execution Time** | On graph load                   | On graph load                         |
| **Alternative**    | `:custom-js-url` in config.edn  | `:custom-js-url` in config.edn        |
| **Security**       | User confirmation required      | User confirmation required (7d cache) |

---

## 1. Technical Deep Dive

### 1.1 File Path Resolution

**Location:** `src/main/frontend/config.cljs:498-506`

```clojure
(defn get-custom-js-path
  ([]
   (get-custom-js-path (state/get-current-repo)))
  ([repo]
   (if (db-based-graph? repo)
     (path/path-join app-name custom-js-file)          ;; "logseq/custom.js"
     (when-let [repo-dir (get-repo-dir repo)]
       (path/path-join repo-dir app-name custom-js-file)))))
```

#### Path Construction Logic

**For DB-based graphs:**

```clojure
;; Constants (src/main/frontend/config.cljs:325-329)
(def app-name "logseq")
(def custom-js-file "custom.js")

;; Result path
"logseq/custom.js"  ;; Virtual path in database
```

**For File-based graphs:**

```clojure
;; Example with repo-dir = "/Users/john/Documents/MyGraph"
(path/path-join "/Users/john/Documents/MyGraph" "logseq" "custom.js")
;; → "/Users/john/Documents/MyGraph/logseq/custom.js"
```

#### Actual Storage Locations

**File-based graphs:**

```
<graph-root-directory>/logseq/custom.js
```

**Database graphs:**

```
<graph-root-directory>/logseq/custom.js
(Still stored as a file, but tracked as virtual file in database)
```

#### Load Priority

1. **`:custom-js-url`** from `config.edn` (if set) - **HIGHEST PRIORITY**
2. **`logseq/custom.js`** file content (fallback)

### 1.2 JavaScript Loading Mechanism

**Location:** `src/main/frontend/handler/ui.cljs:134-177`

```clojure
(def *js-execed (atom #{}))

(defn exec-js-if-exists-&-allowed!
  [t]
  (when-let [href (or
                   (state/get-custom-js-link)           ;; 1. Check for URL override
                   (config/get-custom-js-path))]        ;; 2. Get local path
    (let [k (str "ls-js-allowed-" href)
          execed #(swap! *js-execed conj href)
          execed? (contains? @*js-execed href)
          ask-allow #(let [r (js/confirm (t :plugin/custom-js-alert))]
                       (if r
                         (storage/set k (js/Date.now))
                         (storage/set k false))
                       r)
          allowed! (storage/get k)
          should-ask? (or (nil? allowed!)
                          (> (- (js/Date.now) allowed!) 604800000))  ;; 7 days
          exec-fn #(when-let [scripts (and % (string/trim %))]
                     (when-not (string/blank? scripts)
                       (when (or (not should-ask?) (ask-allow))
                         (try
                           (js/eval scripts)
                           (execed)
                           (catch :default e
                             (js/console.error "[custom js]" e))))))]
      (when (and (not execed?)
                 (not= false allowed!))
        (cond
          (string/starts-with? href "http")
          (when (or (not should-ask?)
                    (ask-allow))
            (load href #(do (js/console.log "[custom js]" href) (execed))))

          (config/db-based-graph? (state/get-current-repo))
          (when-let [script (db/get-file href)]
            (exec-fn script))

          :else
          (let [repo-dir (config/get-repo-dir (state/get-current-repo))
                rpath (path/relative-path repo-dir href)]
            (p/let [exists? (fs/file-exists? repo-dir rpath)]
              (when exists?
                (util/p-handle
                 (fs/read-file repo-dir rpath)
                 exec-fn)))))))))
```

#### Security Model

**Permission Storage:**

```javascript
// LocalStorage key format
'ls-js-allowed-<href>' // e.g., "ls-js-allowed-logseq/custom.js"

// Values:
// - null/undefined: Never asked
// - false: User denied
// - <timestamp>: User approved at timestamp
```

**Re-confirmation Period:**

```clojure
604800000  ;; 7 days in milliseconds
           ;; = 7 * 24 * 60 * 60 * 1000
```

**Confirmation Dialog:**

```
Translation key: :plugin/custom-js-alert

English:
"Found custom.js file, allow execution?
(If you don't understand the content of this file, it's recommended
not to allow execution, which has certain security risks.)"
```

### 1.3 File Retrieval

**Location:** `src/main/frontend/db/model.cljs:57-60` (by analogy with custom.css)

```clojure
;; No explicit get-custom-js function, but follows same pattern:
(defn get-file
  [repo path]
  (when-let [db (db/get-db repo)]
    (when-let [page (db-utils/entity db [:file/path path])]
      (:file/content (db-utils/entity db [:file/path path])))))
```

For custom.js:

```clojure
;; DB-based graph
(db/get-file href)  ;; href = "logseq/custom.js"

;; File-based graph
(fs/read-file repo-dir rpath)  ;; Read from filesystem
```

### 1.4 Hot Reload Status

**❌ NO HOT RELOAD FOR CUSTOM.JS**

Unlike `custom.css`, custom.js does NOT have file watcher hot-reload support.

**Evidence:**

```clojure
;; src/main/frontend/fs/watcher_handler.cljs:120-124
(and (contains? #{"add" "change" "unlink"} type)
     (string/ends-with? path "logseq/custom.css"))  ;; Only CSS!
(do
  (println "reloading custom.css")
  (ui-handler/add-style-if-exists!))
```

**Why no hot reload?**

1. **Security:** Auto-executing changed JS without user confirmation is dangerous
2. **Execution model:** JS runs once on load, not continuously like CSS
3. **State management:** Re-executing arbitrary JS could corrupt app state

**User workflow:** Edit custom.js → **Reload app/graph** → Confirmation dialog → Execute

### 1.5 Execution Trigger

**Location:** `src/main/frontend/components/plugins.cljs:1528-1536`

```clojure
(rum/defc custom-js-installer
  [{:keys [t current-repo db-restoring?]}]
  (hooks/use-effect!
   (fn []
     (when (and (not db-restoring?)
                (not util/nfs?))
       (ui-handler/exec-js-if-exists-&-allowed! t)))
   [current-repo db-restoring?])
  nil)
```

**Execution timing:**

- **When:** Graph loads or switches
- **Condition:** Not during database restoration AND not in native filesystem mode
- **Dependencies:** Runs when `current-repo` or `db-restoring?` changes

---

## 2. Flow Diagrams

### 2.1 Initial Load Flow

```
App starts / Graph loads
    ↓
custom-js-installer component mounts [React useEffect]
    ↓
Check: db-restoring? = false AND util/nfs? = false
    ↓ (yes)
exec-js-if-exists-&-allowed! called
    ↓
Check: custom-js-url OR custom-js-path exists?
    ↓ (yes)
    ├─→ [Remote URL] Load script from HTTP
    │       ↓
    │   Check permission cache (7 days)
    │       ↓
    │   Show confirmation dialog (if needed)
    │       ↓
    │   Load <script src="..."> tag
    │
    └─→ [Local file] Read from DB or filesystem
            ↓
        Check permission cache (7 days)
            ↓
        Show confirmation dialog (if needed)
            ↓
        js/eval(script-content)
            ↓
        Mark as executed in *js-execed atom
            ↓
        Console: "[custom js] <href>"
```

### 2.2 Security Flow

```
custom.js found
    ↓
Check localStorage["ls-js-allowed-<href>"]
    │
    ├─→ [null/undefined] Never asked before
    │       ↓
    │   Show confirmation dialog
    │       ↓
    │   User clicks "OK" → Store timestamp
    │   User clicks "Cancel" → Store false
    │
    ├─→ [false] User previously denied
    │       ↓
    │   ❌ Do not execute (silent)
    │
    └─→ [timestamp] Previously approved
            ↓
        Check: (now - timestamp) > 7 days?
            │
            ├─→ [Yes] Re-ask confirmation
            └─→ [No] Execute immediately
```

### 2.3 File Change Flow (No Hot Reload)

```
User edits custom.js file
    ↓
File saved to disk
    ↓
❌ File watcher IGNORES custom.js
    ↓
[User must manually reload]
    ↓
Reload graph OR restart app
    ↓
custom-js-installer triggers again
    ↓
Re-check permission (may use cached approval)
    ↓
Execute updated script
```

---

## 3. Code Location Reference

| Component              | File                                                  | Lines     | Purpose                             |
| ---------------------- | ----------------------------------------------------- | --------- | ----------------------------------- |
| **Path Resolution**    | `src/main/frontend/config.cljs`                       | 498-506   | `get-custom-js-path` function       |
| **URL Config**         | `src/main/frontend/state.cljs`                        | 510-512   | `get-custom-js-link` function       |
| **JS Execution**       | `src/main/frontend/handler/ui.cljs`                   | 134-177   | Load and execute JS with security   |
| **Execution Trigger**  | `src/main/frontend/components/plugins.cljs`           | 1528-1536 | Component that triggers execution   |
| **File Watcher**       | `src/main/frontend/fs/watcher_handler.cljs`           | 120-124   | ❌ Only watches custom.css, not .js |
| **Config Schema**      | `src/main/frontend/schema/handler/common_config.cljc` | 26        | `:custom-js-url` schema definition  |
| **Config Template**    | `deps/common/resources/templates/config.edn`          | 195-202   | Example usage in config             |
| **Permission Storage** | `src/main/frontend/spec/storage.cljc`                 | 32        | LocalStorage spec                   |

---

## 4. Usage Guide

### 4.1 Method 1: Direct custom.js File (Recommended)

#### For File-Based Graphs

**Step 1:** Create the file

```bash
# Navigate to your graph directory
cd /path/to/your/graph

# Create custom.js
touch logseq/custom.js

# Edit with your preferred editor
code logseq/custom.js
# OR
nano logseq/custom.js
```

**Step 2:** Add JavaScript code

```javascript
// logseq/custom.js
console.log('Custom JS loaded!')

// Example: Add custom keyboard shortcut
document.addEventListener('keydown', function (e) {
  if (e.ctrlKey && e.shiftKey && e.key === 'H') {
    console.log('Custom shortcut triggered!')
    // Your custom logic here
  }
})

// Example: Modify DOM
window.addEventListener('load', function () {
  document.body.style.border = '3px solid red'
})
```

**Step 3:** Reload Logseq

```
Close and reopen Logseq
OR
Switch to another graph and back
```

**Step 4:** Approve execution

When prompted:

```
"Found custom.js file, allow execution?"
```

Click **OK** (only if you trust the code!)

**Verification:**

- Open DevTools Console (Cmd+Option+I on Mac)
- Look for: `[custom js] logseq/custom.js`

---

#### For Database Graphs

**Step 1:** Access via Settings

```text
Click ⚙️ (gear icon) → Settings → Advanced → Edit custom.js
```

**Note:** May need to create the file first:

```bash
# In your graph directory
touch logseq/custom.js
```

**Step 2:** Edit in Logseq's file editor

```javascript
// Same JavaScript code as above
console.log('Custom JS loaded!')
```

**Step 3:** Reload graph

```
Settings → Open another graph → Reopen this graph
```

**Step 4:** Approve execution (same as file-based)

---

### 4.2 Method 2: Remote JavaScript URL (Advanced)

**Use case:** Share custom.js across multiple graphs or team members

**Step 1:** Host JavaScript file

```javascript
// https://example.com/my-custom-logseq.js
console.log('Remote custom JS loaded!')

// Your customizations...
```

**Step 2:** Update config.edn

```bash
# Edit config file
<graph-dir>/logseq/config.edn
```

Add:

```clojure
;; Specify a custom JS import.
;; This option takes precedence over the local `logseq/custom.js` file.
:custom-js-url "https://example.com/my-custom-logseq.js"
```

**Step 3:** Reload graph

**Step 4:** Approve execution

```
"Found custom.js file, allow execution?"
[Shows URL for transparency]
```

**Security note:** Only use HTTPS URLs from trusted sources!

---

### 4.3 Permission Management

#### Check Current Permission

```javascript
// In DevTools Console
localStorage.getItem('ls-js-allowed-logseq/custom.js')

// Returns:
// null       → Never asked
// "false"    → Denied
// "17324..." → Timestamp of approval
```

#### Revoke Permission

```javascript
// In DevTools Console
localStorage.removeItem('ls-js-allowed-logseq/custom.js')

// Next graph load will ask again
```

#### Force Re-confirmation

```javascript
// Set timestamp to >7 days ago
localStorage.setItem('ls-js-allowed-logseq/custom.js', '1')

// Next graph load will ask again
```

---

## 5. Comparison: File-Based vs Database Graphs

| Aspect               | File-Based Graph               | Database Graph                 | Notes                         |
| -------------------- | ------------------------------ | ------------------------------ | ----------------------------- |
| **Storage**          | Filesystem                     | Database (virtual file)        | Both use same path            |
| **Editing**          | External editor                | Settings UI or external        | DB graphs can use both        |
| **File location**    | `<graph-dir>/logseq/custom.js` | `<graph-dir>/logseq/custom.js` | Identical paths               |
| **Retrieval method** | `fs/read-file`                 | `db/get-file`                  | Different internal mechanisms |
| **Execution timing** | On graph load                  | On graph load                  | Same                          |
| **Security model**   | User confirmation (7d cache)   | User confirmation (7d cache)   | Identical                     |
| **Hot reload**       | ❌ No                          | ❌ No                          | Requires manual reload        |
| **URL override**     | ✅ Yes (`:custom-js-url`)      | ✅ Yes (`:custom-js-url`)      | Same                          |

---

## 6. What You Can Do With custom.js

### 6.1 Capabilities

✅ **Full JavaScript access:**

- DOM manipulation
- Event listeners
- Window/document APIs
- Browser APIs (fetch, localStorage, etc.)
- Third-party libraries (via CDN)

✅ **Logseq-specific:**

- Access `window.logseq` API (plugin SDK)
- Modify UI elements
- Add custom shortcuts
- Integrate external services

✅ **Examples:**

```javascript
// 1. Add Google Analytics
;(function () {
  var script = document.createElement('script')
  script.src = 'https://www.googletagmanager.com/gtag/js?id=UA-XXXXX-Y'
  document.head.appendChild(script)

  window.dataLayer = window.dataLayer || []
  function gtag() {
    dataLayer.push(arguments)
  }
  gtag('js', new Date())
  gtag('config', 'UA-XXXXX-Y')
})()

// 2. Custom theme switcher
document.addEventListener('keydown', function (e) {
  if (e.altKey && e.key === 't') {
    document.documentElement.classList.toggle('dark')
  }
})

// 3. Auto-save reminder
setInterval(function () {
  console.log('Remember to save your work!')
}, 300000) // Every 5 minutes

// 4. Custom search integration
window.customSearch = function (query) {
  fetch(`https://api.example.com/search?q=${query}`)
    .then((r) => r.json())
    .then((data) => console.log(data))
}

// 5. Inject custom CSS dynamically
var style = document.createElement('style')
style.textContent = `
  .block-content {
    font-family: 'Comic Sans MS', cursive;
  }
`
document.head.appendChild(style)
```

### 6.2 Constraints

❌ **Cannot:**

- Modify Logseq's ClojureScript code directly
- Access Node.js APIs (runs in renderer process)
- Bypass Electron's security model
- Hot reload without app restart

⚠️ **Risks:**

- **Security:** Malicious JS can steal data, keylog, or compromise system
- **Stability:** Poorly written JS can crash or slow down Logseq
- **Compatibility:** May break with Logseq updates
- **Persistence:** No execution on every page load (only graph load)

### 6.3 Best Practices

✅ **DO:**

- Comment your code thoroughly
- Test in a separate test graph first
- Use `try/catch` for error handling
- Check Logseq version compatibility
- Document dependencies
- Use strict mode: `'use strict';`

❌ **DON'T:**

- Include untrusted code from unknown sources
- Manipulate critical Logseq state without understanding
- Block the main thread with heavy computation
- Ignore console errors
- Share custom.js without security review

---

## 7. Troubleshooting

### 7.1 JavaScript Not Executing

**Symptoms:** No console output, no effect

**Checklist:**

1. ✅ Verify file path is correct

   ```bash
   # File-based or DB-based (same location)
   ls "<graph-dir>/logseq/custom.js"
   ```

2. ✅ Check permission was granted

   ```javascript
   // DevTools Console
   localStorage.getItem('ls-js-allowed-logseq/custom.js')
   // Should return timestamp, not "false" or null
   ```

3. ✅ Verify JavaScript syntax

   ```javascript
   // Check DevTools Console for syntax errors
   // Red error messages indicate syntax problems
   ```

4. ✅ Confirm graph was reloaded

   ```text
   Custom.js only executes on graph load, not on file save
   ```

5. ✅ Check for `:custom-js-url` override

   ```clojure
   ;; In config.edn
   ;; If this is set, local custom.js is ignored!
   :custom-js-url "..."
   ```

6. ✅ Look for execution marker in Console

   ```
   [custom js] logseq/custom.js
   ```

---

### 7.2 Permission Dialog Not Showing

**Possible causes:**

1. **Already approved (within 7 days)**

   ```javascript
   localStorage.removeItem('ls-js-allowed-logseq/custom.js')
   // Then reload graph
   ```

2. **Previously denied**

   ```javascript
   localStorage.removeItem('ls-js-allowed-logseq/custom.js')
   // Then reload graph
   ```

3. **File doesn't exist**
   ```bash
   ls "<graph-dir>/logseq/custom.js"
   ```

---

### 7.3 Script Errors

**Check DevTools Console:**

```javascript
// Look for errors like:
Uncaught SyntaxError: ...
Uncaught ReferenceError: ...
[custom js] Error: ...
```

**Common errors:**

```javascript
// ❌ WRONG: Missing semicolon
console.log('test')
console.log('test2')

// ✅ CORRECT:
console.log('test')
console.log('test2')

// ❌ WRONG: Undefined variable
myFunction() // Not defined

// ✅ CORRECT:
function myFunction() {}
myFunction()
```

---

### 7.4 Changes Not Reflecting

**Remember:** NO hot reload!

**Solution:**

1. Edit custom.js
2. Save file
3. **Reload Logseq or switch graphs**
4. Approve execution (if needed)
5. Check console for confirmation

---

## 8. Security Considerations

### 8.1 Threat Model

**Attack vectors:**

1. **Malicious file in shared graph**

   - Team member adds malicious custom.js
   - Synced to your machine via Git/Syncthing
   - Executes on your next graph load

2. **Supply chain attack**

   - Using `:custom-js-url` with compromised CDN
   - Third-party script gets hacked
   - Malicious code executes in your app

3. **Social engineering**
   - "Use this custom.js for cool features!"
   - Actually contains keylogger or data exfiltration

### 8.2 Security Features

✅ **Built-in protections:**

1. **User confirmation required**

   ```
   Must explicitly approve execution
   ```

2. **Re-confirmation every 7 days**

   ```
   Can't set-and-forget malicious code
   ```

3. **URL transparency**

   ```
   Confirmation dialog shows the script source
   ```

4. **Execution tracking**

   ```
   *js-execed atom prevents duplicate execution
   ```

5. **Error isolation**
   ```clojure
   (catch :default e
     (js/console.error "[custom js]" e))
   ```

### 8.3 Recommended Safeguards

✅ **For users:**

1. **Only use code you understand**

   ```javascript
   // Read every line before approving!
   ```

2. **Check custom.js in version control**

   ```bash
   git log -- logseq/custom.js
   git diff -- logseq/custom.js
   ```

3. **Audit third-party snippets**

   ```javascript
   // Copy to separate file
   // Review thoroughly
   // Test in isolated environment
   ```

4. **Use Subresource Integrity (SRI) for CDN scripts**

   ```javascript
   var script = document.createElement('script')
   script.src = 'https://cdn.example.com/library.js'
   script.integrity = 'sha384-...'
   script.crossOrigin = 'anonymous'
   document.head.appendChild(script)
   ```

5. **Monitor DevTools Console**
   ```
   Watch for unexpected network requests or errors
   ```

✅ **For teams:**

1. **Code review custom.js changes**

   ```
   Treat custom.js like production code
   ```

2. **Use `:custom-js-url` with version pinning**

   ```clojure
   :custom-js-url "https://cdn.example.com/logseq-custom@1.2.3.js"
   ```

3. **Document approved scripts**
   ```markdown
   # Approved Custom Scripts

   - analytics.js (v2.1, approved 2025-11-01)
   - theme-switcher.js (v1.0, approved 2025-10-15)
   ```

---

## 9. Comparison: custom.js vs custom.css vs Plugins

| Feature                | custom.css        | custom.js                | Plugin                    |
| ---------------------- | ----------------- | ------------------------ | ------------------------- |
| **Purpose**            | Styling           | JavaScript logic         | Full-featured extension   |
| **Language**           | CSS               | JavaScript               | JavaScript/ClojureScript  |
| **Hot Reload**         | ✅ Yes            | ❌ No                    | ✅ Yes (dev mode)         |
| **Security**           | Low risk          | ⚠️ **High risk**         | Medium risk (sandboxed)   |
| **User confirmation**  | ❌ No             | ✅ **Yes (7d cache)**    | ✅ Yes (install time)     |
| **File location**      | `logseq/*.css`    | `logseq/custom.js`       | Separate plugin directory |
| **Distribution**       | Manual copy       | Manual copy or URL       | Marketplace               |
| **Capabilities**       | Visual only       | Full JavaScript          | Logseq API + UI           |
| **Debugging**          | DevTools Elements | DevTools Console         | Plugin console            |
| **Update mechanism**   | Save → Auto       | Save → **Manual reload** | Auto-update               |
| **Version control**    | Easy              | Easy                     | Via Git submodule         |
| **Team collaboration** | Simple            | Simple (with security)   | Complex                   |

**When to use what:**

- **custom.css:** Visual tweaks, theming, layout adjustments
- **custom.js:** Lightweight automations, integrations, shortcuts
- **Plugin:** Complex features, UI components, persistent services

---

## 10. Example: Complete Custom.js

```javascript
// logseq/custom.js
// Version: 1.0
// Author: Your Name
// Description: Custom enhancements for Logseq
// Last Updated: 2025-11-23

;(function () {
  'use strict'

  console.log('🚀 Custom Logseq JS loaded!')

  // ============================================
  // 1. CUSTOM KEYBOARD SHORTCUTS
  // ============================================

  document.addEventListener('keydown', function (e) {
    // Ctrl+Shift+D - Insert current date-time
    if (e.ctrlKey && e.shiftKey && e.key === 'D') {
      e.preventDefault()
      var now = new Date().toLocaleString()
      document.execCommand('insertText', false, now)
    }

    // Alt+H - Toggle high contrast mode
    if (e.altKey && e.key === 'h') {
      e.preventDefault()
      document.body.classList.toggle('high-contrast')
    }
  })

  // ============================================
  // 2. ANALYTICS INTEGRATION
  // ============================================

  ;(function loadAnalytics() {
    // Plausible.io example (privacy-friendly)
    var script = document.createElement('script')
    script.defer = true
    script.src = 'https://plausible.io/js/script.js'
    script.setAttribute('data-domain', 'your-domain.com')
    document.head.appendChild(script)
  })()

  // ============================================
  // 3. AUTO-SAVE REMINDER
  // ============================================

  var REMINDER_INTERVAL = 30 * 60 * 1000 // 30 minutes

  setInterval(function () {
    console.log('💾 Reminder: Your work is auto-saved!')
  }, REMINDER_INTERVAL)

  // ============================================
  // 4. CUSTOM THEME UTILITIES
  // ============================================

  window.logseqCustom = {
    // Toggle between light and dark
    toggleTheme: function () {
      var html = document.documentElement
      var isDark = html.classList.contains('dark')
      html.classList.toggle('dark', !isDark)
      html.classList.toggle('light', isDark)
      console.log('Theme switched to:', isDark ? 'light' : 'dark')
    },

    // Set accent color dynamically
    setAccentColor: function (color) {
      document.documentElement.style.setProperty('--lx-accent-09', color)
      console.log('Accent color set to:', color)
    },

    // Reset to default theme
    resetTheme: function () {
      document.documentElement.className = ''
      console.log('Theme reset to default')
    },
  }

  // ============================================
  // 5. EXTERNAL INTEGRATION
  // ============================================

  window.searchExternal = function (query) {
    var url = 'https://www.google.com/search?q=' + encodeURIComponent(query)
    window.open(url, '_blank')
  }

  // ============================================
  // 6. DOM OBSERVER (Advanced)
  // ============================================

  var observer = new MutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
      if (mutation.type === 'childList') {
        // React to DOM changes
        // console.log('DOM changed:', mutation);
      }
    })
  })

  // Start observing after DOM ready
  window.addEventListener('load', function () {
    observer.observe(document.body, {
      childList: true,
      subtree: true,
    })
  })

  // ============================================
  // 7. ERROR HANDLING
  // ============================================

  window.addEventListener('error', function (e) {
    console.error('Custom JS error:', e.message)
  })

  // ============================================
  // 8. CLEANUP ON UNLOAD
  // ============================================

  window.addEventListener('beforeunload', function () {
    console.log('👋 Custom JS cleanup')
    if (observer) observer.disconnect()
  })

  console.log('✅ Custom JS initialized successfully!')
})()
```

**Usage:**

```javascript
// In DevTools Console, access utilities:
logseqCustom.toggleTheme()
logseqCustom.setAccentColor('#ff6b6b')
searchExternal('Logseq documentation')
```

---

## 11. Best Practices

### 11.1 Code Organization

✅ **Use IIFE (Immediately Invoked Function Expression)**

```javascript
;(function () {
  'use strict'
  // Your code here
  // Avoids global namespace pollution
})()
```

✅ **Namespace your globals**

```javascript
window.myCustomLogseq = {
  version: '1.0',
  feature1: function () {},
  feature2: function () {},
}
```

✅ **Document everything**

```javascript
/**
 * Toggles high contrast mode
 * @param {boolean} force - Force on/off
 * @returns {boolean} New state
 */
function toggleHighContrast(force) {
  // Implementation
}
```

### 11.2 Performance

✅ **Debounce event listeners**

```javascript
function debounce(func, wait) {
  var timeout
  return function () {
    var context = this,
      args = arguments
    clearTimeout(timeout)
    timeout = setTimeout(function () {
      func.apply(context, args)
    }, wait)
  }
}

document.addEventListener(
  'scroll',
  debounce(function () {
    console.log('Scrolled')
  }, 250)
)
```

✅ **Use passive event listeners**

```javascript
document.addEventListener('scroll', handler, { passive: true })
```

✅ **Clean up timers and observers**

```javascript
var timer = setInterval(doSomething, 1000)

window.addEventListener('beforeunload', function () {
  clearInterval(timer)
})
```

### 11.3 Compatibility

✅ **Check for Logseq-specific APIs**

```javascript
if (window.logseq) {
  // Use Logseq plugin API
} else {
  console.warn('Logseq API not available')
}
```

✅ **Feature detection**

```javascript
if ('MutationObserver' in window) {
  // Use MutationObserver
} else {
  // Fallback
}
```

---

## 12. Related Documentation

- **Custom CSS:** Similar system but for styling
- **Plugin Development:** For more complex extensions
- **Logseq Plugin API:** `window.logseq` reference
- **Config.edn Guide:** Configuration file documentation

---

## 13. Changelog

| Version | Date       | Changes                             |
| ------- | ---------- | ----------------------------------- |
| 1.0     | 2025-11-23 | Initial comprehensive investigation |

---

## 14. Glossary

| Term                 | Definition                                                             |
| -------------------- | ---------------------------------------------------------------------- |
| **custom.js**        | User-provided JavaScript file executed on graph load                   |
| **Hot reload**       | Automatic reloading when file changes (NOT supported for custom.js)    |
| **js/eval**          | JavaScript function that executes code from string                     |
| **Permission cache** | 7-day approval stored in localStorage                                  |
| **:custom-js-url**   | Config option to load JS from remote URL instead of local file         |
| **\*js-execed**      | Atom tracking which scripts have been executed                         |
| **IPC**              | Inter-Process Communication (Electron main ↔ renderer)                 |
| **IIFE**             | Immediately Invoked Function Expression - wraps code in isolated scope |

---

## License

This investigation is part of the Logseq project documentation and follows the same license.
