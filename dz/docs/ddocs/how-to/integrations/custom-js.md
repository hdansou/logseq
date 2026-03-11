---
title: 'Custom JavaScript'
description: 'How to extend Logseq functionality with custom JavaScript'
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[Custom CSS](custom-css.md)'
  reference:
    - '[Plugin API](../../reference/api/plugin-api.md)'
---

# Custom JavaScript

> **Last revised:** 2026-03-10

> How to add custom functionality to Logseq with JavaScript without creating a full plugin.

## ⚠️ Important Notes

- Custom JS has **full access** to your system (unlike sandboxed plugins)
- Only use code you understand and trust
- Errors may break Logseq functionality
- Test in a separate graph first

---

## Where to Add Custom JS

1. Open your graph folder
2. Navigate to `logseq/` subfolder
3. Create or edit `custom.js`

**Path:**

```
your-graph/
└── logseq/
    └── custom.js
```

**Alternative:** You can also set `:custom-js-url` in your `config.edn` to load a remote JS file by URL instead of using a local file.

---

## Basic Example

```javascript
// Log when custom.js is executed
console.log('Custom JS loaded!')

// Custom JS runs via js/eval in the main Electron window.
// It has full access to the DOM and standard browser APIs.
// The Logseq plugin SDK (logseq.Editor, logseq.DB, etc.) is NOT
// available in custom.js — that API is only injected into plugin sandboxes.
```

---

## Common Recipes

### Add Keyboard Shortcut

```javascript
document.addEventListener('keydown', (e) => {
  // Ctrl/Cmd + Shift + M
  if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'm') {
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

### Auto-Format Dates

```javascript
// Replace typed dates with formatted versions
document.addEventListener('input', (e) => {
  if (e.target.tagName === 'TEXTAREA') {
    const text = e.target.value
    // Convert YYYY-MM-DD to [[Month DDth, YYYY]]
    const formatted = text.replace(
      /(\d{4})-(\d{2})-(\d{2})/g,
      (match, y, m, d) => {
        const date = new Date(y, m - 1, d)
        const month = date.toLocaleString('en', { month: 'long' })
        const day = parseInt(d)
        const suffix = ['th', 'st', 'nd', 'rd'][
          day % 10 > 3 ? 0 : (((day % 100) - (day % 10) != 10) * day) % 10
        ]
        return `[[${month} ${day}${suffix}, ${y}]]`
      }
    )
    if (formatted !== text) {
      e.target.value = formatted
    }
  }
})
```

### Highlight Overdue Tasks

```javascript
function highlightOverdueTasks() {
  const today = new Date()
  today.setHours(0, 0, 0, 0)

  document.querySelectorAll('.scheduled, .deadline').forEach((el) => {
    const dateText = el.textContent
    const dateMatch = dateText.match(/(\d{4}-\d{2}-\d{2})/)
    if (dateMatch) {
      const taskDate = new Date(dateMatch[1])
      if (taskDate < today) {
        el.closest('.ls-block')?.classList.add('overdue')
      }
    }
  })
}

// Run on page changes
new MutationObserver(highlightOverdueTasks).observe(document.body, {
  childList: true,
  subtree: true,
})
```

With corresponding CSS:

```css
/* In custom.css */
.ls-block.overdue {
  border-left: 3px solid #ef4444;
  background-color: rgba(239, 68, 68, 0.1);
}
```

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

### External Link Indicator

```javascript
document.addEventListener(
  'click',
  (e) => {
    const link = e.target.closest('a[href^="http"]')
    if (link && !link.href.includes('logseq.com')) {
      // Add indicator before opening
      link.setAttribute('target', '_blank')
      link.setAttribute('rel', 'noopener noreferrer')
    }
  },
  true
)
```

---

## Accessing Internal APIs

Custom JS runs in the main Electron renderer process via `js/eval`. The `window.logseq` object exists but exposes internal host-side APIs (`window.logseq.api`, `window.logseq.sdk`) — **not** the plugin SDK proxy (i.e., `logseq.Editor.*`, `logseq.DB.*` are not available).

For most custom.js use cases, stick to standard DOM manipulation and browser APIs. If you need programmatic access to Logseq data and commands, use a proper plugin instead.

```javascript
// Custom JS has access to:
// - The full DOM (document.querySelector, etc.)
// - Standard browser APIs (fetch, localStorage, MutationObserver, etc.)
// - Electron renderer globals

// Custom JS does NOT have access to:
// - logseq.Editor.registerSlashCommand()
// - logseq.Editor.insertAtEditingCursor()
// - logseq.DB.onChanged()
// - Any other plugin SDK methods
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

---

## Debugging

### Open Developer Console

- **macOS**: `Cmd+Option+I`
- **Windows/Linux**: `Ctrl+Shift+I`

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
- Sandboxed security
- Official marketplace listing

See: [Plugin API Reference](../../reference/api/plugin-api.md)

---

## Troubleshooting

### JS Not Loading

1. Check file is named exactly `custom.js`
2. Verify it's in `logseq/` folder inside your graph
3. When Logseq detects `custom.js`, it shows a confirmation dialog asking permission to execute it — click **OK** to allow (this permission is cached for 7 days, then re-asked)
4. Reload Logseq (`Cmd/Ctrl+R`)

### Errors Breaking Logseq

1. Temporarily rename `custom.js` to `custom.js.bak`
2. Restart Logseq
3. Fix the error in your code
4. Rename back and reload

### Code Works in Console but Not in File

Custom JS executes once Logseq loads the graph (not at DOM load time). If your code depends on specific DOM elements that render later, use a `MutationObserver` or a `setTimeout` delay:

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

_Last updated: 2026-03-10_
