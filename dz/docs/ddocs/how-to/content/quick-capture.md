---
title: 'Quick Capture'
description: 'How to quickly capture content to Logseq from anywhere'
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
platforms: [desktop, mobile]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[X-Callback-URL](../integrations/x-callback-url.md)'
---

# Quick Capture

> **Last revised:** 2026-03-10

> How to capture thoughts, links, and content to Logseq as quickly as possible.

## How Quick Capture Works

Quick capture is triggered externally via the `logseq://x-callback-url/quickCapture` URL scheme (desktop) or the system share sheet (mobile). There is no built-in keyboard shortcut for quick capture.

### Capture Destination Priority

When a quick capture is received, Logseq determines where to insert it using this priority order:

1. **Today's journal** -- if `:insert-today?` is `true` in config, or `page=TODAY` is passed in the URL
2. **Explicit page** -- if a `page` parameter is provided in the URL
3. **Default page** -- if `:default-page` is set in config (e.g., `"Inbox"`)
4. **Current page** -- whatever page is open in the editor
5. **Fallback** -- today's journal (if journals enabled) or a page named `"quick capture"`

---

## Configure Quick Capture

In `config.edn`:

```clojure
{:quick-capture-options
 {:insert-today? true        ;; Add to today's journal
  :default-page "Inbox"     ;; Fallback page when no page is specified
  :redirect-page? false}}   ;; Don't navigate after capture
```

You can also customize the capture template:

```clojure
{:quick-capture-templates
 {:text "**{time}** [[quick capture]]: {text} {url}"}}
```

Available template variables: `{time}`, `{date}`, `{text}`, `{url}`.

---

## External Quick Capture

### From Browser

Use X-callback-URL:

```
logseq://x-callback-url/quickCapture?content=My quick note
```

**Supported URL parameters:**

| Parameter | Type   | Description |
|-----------|--------|-------------|
| `content` | string | The text content to capture |
| `url`     | string | A URL to attach (auto-formatted as link) |
| `title`   | string | Title for the URL link |
| `page`    | string | Target page name; use `TODAY` for today's journal |
| `append`  | bool   | If `true`, append to end of page instead of current editing position |

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

_Last updated: 2026-03-10_
