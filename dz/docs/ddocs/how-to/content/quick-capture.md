---
title: 'Quick Capture'
description: 'How to quickly capture content to Logseq from anywhere'
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, mobile]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[X-Callback-URL](../integrations/x-callback-url.md)'
---

# Quick Capture

> How to capture thoughts, links, and content to Logseq as quickly as possible.

## In-App Quick Capture

### Keyboard Shortcut

Press `Cmd+Shift+K` (macOS) or `Ctrl+Shift+K` (Windows/Linux) to open the quick capture modal.

Type your note and press Enter to save.

### Quick Capture Destinations

Content is captured to:

1. **Current page** (default if editing)
2. **Today's journal** (if configured)
3. **Dedicated quick capture page** (configurable)

---

## Configure Quick Capture

In `config.edn`:

```clojure
{:quick-capture-options
 {:insert-today? true      ;; Add to today's journal
  :page "Inbox"           ;; Or specific page name
  :redirect-page? false}} ;; Don't navigate after capture
```

---

## External Quick Capture

### From Browser

Use X-callback-URL:

```
logseq://x-callback-url/quickCapture?content=My quick note
```

See: [X-Callback-URL Guide](../integrations/x-callback-url.md)

### From Command Line

**macOS:**

```bash
open "logseq://x-callback-url/quickCapture?content=Quick%20thought"
```

**Windows:**

```powershell
Start-Process "logseq://x-callback-url/quickCapture?content=Quick%20thought"
```

---

## System-Wide Shortcuts

### macOS with Automator

1. Open Automator
2. Create new Quick Action
3. Add "Run Shell Script" action:
   ```bash
   open "logseq://x-callback-url/quickCapture?content=$(pbpaste | python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.stdin.read()))')"
   ```
4. Save as "Send to Logseq"
5. Assign keyboard shortcut in System Preferences → Keyboard → Shortcuts

### macOS with Alfred/Raycast

Create a workflow with keyword "lg" that opens:

```
logseq://x-callback-url/quickCapture?content={query}
```

### Windows with AutoHotkey

```autohotkey
; Ctrl+Alt+L to capture clipboard
^!l::
    clipboard := clipboard  ; Get clipboard
    EncodedClip := UriEncode(clipboard)
    Run, logseq://x-callback-url/quickCapture?content=%EncodedClip%
return

UriEncode(str) {
    return % encodeURIComponent(str)
}
```

---

## Mobile Quick Capture

### iOS/iPadOS

**Widget:**

1. Long-press home screen
2. Tap + to add widget
3. Select Logseq widget
4. Tap to capture

**Share Sheet:**

1. Select text in any app
2. Tap Share
3. Choose Logseq
4. Edit and save

**Shortcuts App:**
Create a shortcut:

1. Add "URL" action with logseq:// scheme
2. Add "Open URLs" action
3. Add to home screen or use with Siri

### Android

**Share to Logseq:**

1. Select text in any app
2. Tap Share
3. Choose Logseq

**Tasker Integration:**
See [X-Callback-URL Guide](../integrations/x-callback-url.md) for Tasker setup.

---

## Browser Extensions

### Bookmarklet

Create a bookmark with this URL:

```javascript
javascript: (function () {
  var text = window.getSelection().toString() || ''
  var url = window.location.href
  var title = document.title
  window.location =
    'logseq://x-callback-url/quickCapture?content=' +
    encodeURIComponent(text) +
    '&url=' +
    encodeURIComponent(url) +
    '&title=' +
    encodeURIComponent(title)
})()
```

Click the bookmark to send selected text + page info to Logseq.

### Chrome/Firefox Extension

Search for "Logseq" in your browser's extension store for community-built extensions.

---

## Tips for Efficient Capture

### 1. Use Tags for Organization

```
Meeting with @john about #project-x
```

Tags get auto-linked and are searchable.

### 2. Add to Specific Pages

```bash
logseq://x-callback-url/quickCapture?content=Task&page=Inbox
```

### 3. Use Templates

Set up templates for common capture types:

- Meeting notes
- Reading highlights
- Task entries

### 4. Process Later

Create an "Inbox" page for captures, then:

- Review daily
- Move to appropriate pages
- Link to projects

---

## Troubleshooting

### Capture Not Working

1. Check Logseq is installed and running
2. Verify URL encoding for special characters
3. Try simpler content first

### Wrong Page

- Verify page name in URL
- Check page exists in your graph
- Try `page=TODAY` for journal

### Content Missing Parts

- URL-encode all special characters
- Wrap content in proper quotes in shell scripts

---

## See Also

- **X-Callback-URL**: [x-callback-url.md](../integrations/x-callback-url.md)
- **HTTP API**: [http-api.md](../../reference/api/http-api.md)

---

_Last updated: 2026-01-29_
