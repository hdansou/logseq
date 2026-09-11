---
title: 'Custom JavaScript'
description: 'How to extend Logseq functionality with custom JavaScript'
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop]
graphType: [db]
relatedDocs:
  howTo:
    - '[Custom CSS](custom-css.md)'
  reference:
    - '[Plugin API](../../reference/api/plugin-api.md)'
---

# Custom JavaScript

> **Last revised:** 2026-09-11

> How to add custom functionality to Logseq with JavaScript without creating a full plugin.

This page covers DB graphs only. Current Logseq does not open file graphs: the graph list keeps only DB graphs ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).

## ⚠️ Important Notes

- Custom JS runs with `js/eval` in Logseq's main window ([`src/main/frontend/handler/ui.cljs:145-152`](../../../../../src/main/frontend/handler/ui.cljs)), with the same access as Logseq's own UI code. Logseq plugins are not sandboxed either, so neither route gives you isolation.
- The desktop window runs with `nodeIntegration false` and `contextIsolation true` ([`src/electron/electron/window.cljs:51-60`](../../../../../src/electron/electron/window.cljs)), so custom JS cannot `require` Node.js modules. It can still use the `window.apis` bridge that the preload script exposes to talk to the Electron main process ([`resources/js/preload.js:36`](../../../../../resources/js/preload.js)).
- Only use code you understand and trust
- Errors may break Logseq functionality
- Test in a separate graph first

---

## Where Custom JS Lives

In a DB graph, `custom.js` is not a file on disk. It is a file record stored inside the graph database with the path `logseq/custom.js`. Logseq creates it, empty, when it creates the graph ([`deps/db/src/logseq/db/sqlite/create_graph.cljs:216-220`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs), [`src/main/frontend/config.cljs:374-381`](../../../../../src/main/frontend/config.cljs)). Editing a `custom.js` file in a folder on disk has no effect on a DB graph.

When you import a file graph, the importer copies that graph's `logseq/custom.js` into the new DB graph ([`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:3336-3338`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs)).

### Open custom.js for editing

Settings has an **Edit custom.css** button but no equivalent for `custom.js` ([`src/main/frontend/components/settings.cljs:217-223`](../../../../../src/main/frontend/components/settings.cljs)). Use search instead:

1. Open search and type `custom.js`.
2. Choose `logseq/custom.js` in the **Files** group. It opens in Logseq's built-in code editor.

Search lists the graph's stored files whenever you type a query, and choosing one opens the file editor ([`src/main/frontend/components/cmdk/core.cljs:439-451`](../../../../../src/main/frontend/components/cmdk/core.cljs), [`:539`](../../../../../src/main/frontend/components/cmdk/core.cljs), [`:644-647`](../../../../../src/main/frontend/components/cmdk/core.cljs)).

Plugins can also write the file with `logseq.DB.setFileContent('logseq/custom.js', content)`. That API accepts only `logseq/custom.js`, `logseq/custom.css`, `logseq/publish.js` and `logseq/publish.css` ([`src/main/logseq/api/db.cljs:53-73`](../../../../../src/main/logseq/api/db.cljs)). `logseq.DB.getFileContent(path)` reads it back ([`:76-79`](../../../../../src/main/logseq/api/db.cljs)).

### Loading JS from config.edn instead

You can set `:custom-js-url` in the graph's `config.edn`. If it is set, Logseq ignores `logseq/custom.js` ([`src/main/frontend/handler/ui.cljs:131-133`](../../../../../src/main/frontend/handler/ui.cljs), [`src/main/frontend/state.cljs:536-538`](../../../../../src/main/frontend/state.cljs), [`deps/common/resources/templates/config.edn:55-58`](../../../../../deps/common/resources/templates/config.edn)). The value is handled in one of two ways:

- A value starting with `http` is loaded as a script from that URL ([`src/main/frontend/handler/ui.cljs:156-159`](../../../../../src/main/frontend/handler/ui.cljs)).
- Any other value is looked up as the path of a file stored in the graph, then evaluated ([`:161-165`](../../../../../src/main/frontend/handler/ui.cljs)).

---

## When Custom JS Runs

Custom JS runs only in the desktop app:

- The runner waits until the graph has finished restoring and the database worker is ready, and it skips the web build ([`src/main/frontend/components/plugins.cljs:1785-1795`](../../../../../src/main/frontend/components/plugins.cljs)). `util/nfs?` is true when Logseq is neither Electron nor a native mobile app ([`src/main/frontend/util.cljc:128-129`](../../../../../src/main/frontend/util.cljc)).
- The runner is mounted only in the desktop and web app container ([`src/main/frontend/components/container.cljs:582-585`](../../../../../src/main/frontend/components/container.cljs)). The mobile app renders its own root component ([`src/main/mobile/core.cljs:14`](../../../../../src/main/mobile/core.cljs), [`:63`](../../../../../src/main/mobile/core.cljs)), so custom JS never runs on mobile either.

### The permission prompt

Before running custom JS, Logseq asks: "Found the custom.js file, is it allowed to execute?" ([`src/resources/dicts/en.edn:1235`](../../../../../src/resources/dicts/en.edn)). The answer is stored in `localStorage` under `ls-js-allowed-<path>`, which is `ls-js-allowed-logseq/custom.js` for a DB graph ([`src/main/frontend/handler/ui.cljs:134-144`](../../../../../src/main/frontend/handler/ui.cljs), [`src/main/frontend/storage.cljs:16-29`](../../../../../src/main/frontend/storage.cljs)):

- **OK** stores the current time. Logseq asks again once 7 days (604800000 ms) have passed.
- **Cancel** stores `false`. After that, Logseq never runs custom JS and never asks again ([`src/main/frontend/handler/ui.cljs:153-154`](../../../../../src/main/frontend/handler/ui.cljs)). To get the prompt back, delete the `ls-js-allowed-logseq/custom.js` key from `localStorage` in DevTools.

### Once per session

Logseq runs each custom JS path at most once per app session ([`src/main/frontend/handler/ui.cljs:127`](../../../../../src/main/frontend/handler/ui.cljs), [`:135-136`](../../../../../src/main/frontend/handler/ui.cljs), [`:153`](../../../../../src/main/frontend/handler/ui.cljs)). Saving `logseq/custom.js` does not run it again. The save handler only reapplies `config.edn` and `custom.css` ([`src/main/frontend/handler/db_based/editor.cljs:161-165`](../../../../../src/main/frontend/handler/db_based/editor.cljs)). Reload the app to run your changes.

---

## Basic Example

```javascript
// Log when custom.js is executed
console.log('Custom JS loaded!')

// Custom JS runs via js/eval in Logseq's main window.
// Treat it as DOM/browser scripting, not as a plugin.
```

---

## Common Recipes

### Add Keyboard Shortcut

```javascript
document.addEventListener('keydown', (e) => {
  // Pick a combination Logseq does not already use.
  // Ctrl/Cmd + Shift + M is taken by "Move blocks to".
  if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'YOUR_KEY') {
    e.preventDefault()

    // Insert text at cursor
    const editor = document.activeElement
    if (editor && editor.tagName === 'TEXTAREA') {
      const text = '[[Meeting Notes]]'
      const start = editor.selectionStart
      editor.value =
        editor.value.slice(0, start) + text + editor.value.slice(start)
      editor.selectionStart = editor.selectionEnd = start + text.length
    }
  }
})
```

`mod+shift+m` is bound to `:editor/move-blocks` ([`src/main/frontend/modules/shortcut/config.cljs:187`](../../../../../src/main/frontend/modules/shortcut/config.cljs)). See [Keyboard Shortcuts](../../reference/configuration/keyboard-shortcuts.md) for the other bindings.

> **Unverified:** that Logseq saves text inserted this way. Logseq keeps the editing block's content in app state (`:editor/content`, [`src/main/frontend/state.cljs:978-981`](../../../../../src/main/frontend/state.cljs)), and assigning `textarea.value` does not fire an `input` event.

### Word Counter

```javascript
function updateWordCount() {
  const mainContent = document.querySelector('#main-content-container')
  if (!mainContent) return

  const text = mainContent.textContent || ''
  const words = text
    .trim()
    .split(/\s+/)
    .filter((w) => w.length > 0).length

  let counter = document.querySelector('#word-counter')
  if (!counter) {
    counter = document.createElement('div')
    counter.id = 'word-counter'
    counter.style.cssText =
      'position:fixed;bottom:10px;right:10px;padding:4px 8px;background:#333;color:#fff;border-radius:4px;font-size:12px;z-index:9999;'
    document.body.appendChild(counter)
  }
  counter.textContent = `${words} words`
}

setInterval(updateWordCount, 2000)
```

The main content area is `div#main-content-container` ([`src/main/frontend/components/container.cljs:90`](../../../../../src/main/frontend/components/container.cljs)).

### External Link Indicator

```javascript
document.addEventListener(
  'click',
  (e) => {
    const link = e.target.closest('a.external-link')
    if (link && !link.href.includes('logseq.com')) {
      link.setAttribute('target', '_blank')
      link.setAttribute('rel', 'noopener noreferrer')
    }
  },
  true
)
```

Logseq renders external links as `a.external-link` ([`src/main/frontend/components/block.cljs:1610`](../../../../../src/main/frontend/components/block.cljs)).

---

## Accessing Internal APIs

Custom JS runs in Logseq's main window after the graph has loaded. For most custom.js use cases, stick to standard DOM manipulation and browser APIs.

The main window also has `window.logseq.api`, the snake_case host functions that the plugin bridge calls, such as `logseq.api.get_file_content` ([`src/main/logseq/api.cljs:93-94`](../../../../../src/main/logseq/api.cljs), [`src/main/frontend/handler/plugin.cljs:44`](../../../../../src/main/frontend/handler/plugin.cljs)). These functions are internal and not documented for custom.js. If you need programmatic access to Logseq data, commands, settings UI or DB events, write a plugin instead.

```javascript
// Custom JS has access to:
// - The full DOM (document.querySelector, etc.)
// - Standard browser APIs (fetch, localStorage, MutationObserver, etc.)
// - The window.apis bridge exposed by Logseq's preload script

// Custom JS should not rely on:
// - The plugin SDK (logseq.Editor.*, logseq.DB.*, logseq.App.*), which exists
//   inside plugins, not in the main window
// - Internal window.logseq.api functions, which can change without notice
//
// For those capabilities, create a plugin instead.
// See: Plugin API Reference
```

---

## Async Functions

For operations that take time:

```javascript
async function fetchAndInsert() {
  try {
    const response = await fetch('https://api.quotable.io/random')
    const data = await response.json()

    // Insert into the active editor textarea via DOM
    const editor = document.activeElement
    if (editor && editor.tagName === 'TEXTAREA') {
      const text = `> ${data.content} — ${data.author}`
      const start = editor.selectionStart
      editor.value =
        editor.value.slice(0, start) + text + editor.value.slice(start)
      editor.selectionStart = editor.selectionEnd = start + text.length
    }
  } catch (error) {
    console.error('Failed to fetch quote:', error)
  }
}

// Usage: Call from console or bind to keyboard shortcut
```

> **Unverified:** that Logseq saves text inserted by setting `textarea.value`. See the note under [Add Keyboard Shortcut](#add-keyboard-shortcut).

---

## Storage

Persist data between sessions:

```javascript
// Save data
function saveData(key, value) {
  localStorage.setItem(`custom-js-${key}`, JSON.stringify(value))
}

// Load data
function loadData(key, defaultValue = null) {
  const stored = localStorage.getItem(`custom-js-${key}`)
  return stored ? JSON.parse(stored) : defaultValue
}

// Example: Track page visits
const visits = loadData('page-visits', {})
visits[window.location.href] = (visits[window.location.href] || 0) + 1
saveData('page-visits', visits)
```

`localStorage` is shared by every graph opened in the app, so prefix your keys.

---

## Debugging

### Open Developer Console

The desktop app uses Electron's standard **View** menu ([`src/electron/electron/core.cljs:243`](../../../../../src/electron/electron/core.cljs)). That menu includes **Toggle Developer Tools**, which is usually:

- **macOS**: `Cmd+Option+I`
- **Windows/Linux**: `Ctrl+Shift+I`

These key bindings come from Electron's default View menu, not from Logseq source.

If your script throws while it runs, Logseq catches the error and logs it to the console with the prefix `[custom js]` ([`src/main/frontend/handler/ui.cljs:148-152`](../../../../../src/main/frontend/handler/ui.cljs)).

### Add Logging

```javascript
console.log('Debug:', variable)
console.table(arrayOfObjects)
console.group('Section')
console.log('Detail 1')
console.log('Detail 2')
console.groupEnd()
```

### Handle Errors

```javascript
try {
  riskyOperation()
} catch (error) {
  console.error('Error in custom JS:', error)
  // Optionally show user notification
}
```

---

## When to Use Plugins Instead

Consider a proper plugin when you need:

- Access to the Logseq SDK (slash commands, block/page operations, DB events)
- Distribution to other users
- UI components (sidebars, modals)
- Settings UI
- Official marketplace listing

A plugin is not a security boundary. Logseq has no plugin sandbox.

See: [Plugin API Reference](../../reference/api/plugin-api.md)

---

## Troubleshooting

### JS Not Loading

1. Make sure you are using the desktop app. Custom JS does not run on web or mobile ([`src/main/frontend/components/plugins.cljs:1790-1793`](../../../../../src/main/frontend/components/plugins.cljs)).
2. Make sure you edited `logseq/custom.js` inside Logseq (search, then **Files**), not a file on disk.
3. Check that `:custom-js-url` is not set in `config.edn`. If it is set, `logseq/custom.js` is ignored.
4. If you ever clicked **Cancel** on the permission prompt, Logseq will not ask again. Delete the `ls-js-allowed-logseq/custom.js` key from `localStorage` in DevTools.
5. Reload Logseq. On desktop, use **View > Reload**. Saving the file does not run it again.

### Errors Breaking Logseq

1. Open `logseq/custom.js` from search and comment out or delete the failing code.
2. Save, then reload Logseq.

### Code Works in Console but Not in File

Custom JS runs once the graph has finished restoring and the database worker is ready, not when the page DOM loads ([`src/main/frontend/components/plugins.cljs:1790-1793`](../../../../../src/main/frontend/components/plugins.cljs)). If your code depends on specific DOM elements that render later, use a `MutationObserver` or a `setTimeout` delay:

```javascript
// Option 1: Wait for a specific element
function waitForElement(selector, callback) {
  const el = document.querySelector(selector)
  if (el) return callback(el)
  new MutationObserver((_, obs) => {
    const el = document.querySelector(selector)
    if (el) { obs.disconnect(); callback(el) }
  }).observe(document.body, { childList: true, subtree: true })
}

// Option 2: Simple delay
setTimeout(() => {
  // Your code here
}, 2000)
```

---

## See Also

- **Custom CSS**: [custom-css.md](custom-css.md)
- **Plugin Development**: [plugin-api.md](../../reference/api/plugin-api.md)

---

_Last updated: 2026-09-11_
