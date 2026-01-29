---
title: 'Custom JavaScript'
description: 'How to extend Logseq functionality with custom JavaScript'
status: stable
lastVerified: '2026-01-29'
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

---

## Basic Example

```javascript
// Log when Logseq is ready
console.log('Custom JS loaded!')

// Wait for Logseq to be fully initialized
if (typeof logseq !== 'undefined') {
  logseq.ready().then(() => {
    console.log('Logseq is ready!')
  })
}
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

## Using Logseq API

When Logseq is available, you can use its API:

```javascript
// Check if we have access to Logseq API
if (typeof logseq !== 'undefined') {
  // Register a command
  logseq.Editor.registerSlashCommand('Current Time', async () => {
    const time = new Date().toLocaleTimeString()
    await logseq.Editor.insertAtEditingCursor(time)
  })

  // Listen to events
  logseq.DB.onChanged(({ blocks, txData }) => {
    console.log('Database changed:', blocks.length, 'blocks affected')
  })

  // Get current page
  logseq.Editor.getCurrentPage().then((page) => {
    console.log('Current page:', page?.name)
  })
}
```

---

## Async Functions

For operations that take time:

```javascript
async function fetchAndInsert() {
  try {
    const response = await fetch('https://api.quotable.io/random')
    const data = await response.json()

    if (typeof logseq !== 'undefined') {
      await logseq.Editor.insertAtEditingCursor(
        `> ${data.content} — ${data.author}`
      )
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
2. Verify it's in `logseq/` folder
3. Enable in Settings → Advanced → Enable custom.js
4. Reload Logseq (`Cmd/Ctrl+R`)

### Errors Breaking Logseq

1. Temporarily rename `custom.js` to `custom.js.bak`
2. Restart Logseq
3. Fix the error in your code
4. Rename back and reload

### Code Works in Console but Not in File

Wrap code in ready check:

```javascript
document.addEventListener('DOMContentLoaded', () => {
  // Your code here
})
```

---

## See Also

- **Custom CSS**: [custom-css.md](custom-css.md)
- **Plugin Development**: [plugin-api.md](../../reference/api/plugin-api.md)

---

_Last updated: 2026-01-29_
