---
title: 'X-Callback-URL'
description: 'How to capture content to Logseq from other apps using URL schemes'
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, mobile]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[Quick Capture](quick-capture.md)'
  reference:
    - '[HTTP API](../../reference/api/http-api.md)'
---

# X-Callback-URL

> How to send content to Logseq from other apps, scripts, and automation tools.

## What is X-Callback-URL?

X-callback-url is a way to send content to Logseq using special URLs. Works from browsers, scripts, and any app that can open URLs.

---

## Quick Start

### Basic Capture

Open this URL to send text to Logseq:

```
logseq://x-callback-url/quickCapture?content=Remember to buy milk
```

### Test It Now

**macOS Terminal:**

```bash
open "logseq://x-callback-url/quickCapture?content=Hello%20from%20Terminal"
```

**Windows PowerShell:**

```powershell
Start-Process "logseq://x-callback-url/quickCapture?content=Hello%20from%20PowerShell"
```

---

## URL Structure

```
logseq://x-callback-url/quickCapture?[parameters]
```

| Part             | Purpose            |
| ---------------- | ------------------ |
| `logseq://`      | Opens Logseq       |
| `x-callback-url` | Automation request |
| `quickCapture`   | The action         |
| `?parameters`    | What to capture    |

---

## Parameters

| Parameter | Description     | Example                      |
| --------- | --------------- | ---------------------------- |
| `content` | Text to capture | `content=My note`            |
| `url`     | Web address     | `url=https://example.com`    |
| `title`   | Link title      | `title=Example Site`         |
| `page`    | Target page     | `page=Ideas` or `page=TODAY` |
| `append`  | Add to end      | `append=true`                |

---

## Common Use Cases

### Capture a Thought

```bash
logseq://x-callback-url/quickCapture?content=Great%20project%20idea&page=TODAY
```

### Save a Webpage

```bash
logseq://x-callback-url/quickCapture?url=https://article.com&title=Interesting%20Article
```

### Add to Reading List

```bash
logseq://x-callback-url/quickCapture?url=https://blog.com/post&title=Must%20Read&page=Reading%20List&append=true
```

### Capture Quote + Source

```bash
logseq://x-callback-url/quickCapture?content=The%20best%20way%20to%20predict%20the%20future%20is%20to%20invent%20it&url=https://quote-source.com&title=Alan%20Kay
```

---

## URL Encoding

Special characters must be encoded:

| Character | Encoded |
| --------- | ------- |
| Space     | `%20`   |
| `&`       | `%26`   |
| `=`       | `%3D`   |
| `#`       | `%23`   |
| `?`       | `%3F`   |

### Encode Automatically

**macOS/Linux:**

```bash
python3 -c "import urllib.parse; print(urllib.parse.quote('Your text here'))"
```

**JavaScript:**

```javascript
encodeURIComponent('Your text here')
```

---

## Browser Bookmarklets

### Capture Current Page

```javascript
javascript: (function () {
  window.location =
    'logseq://x-callback-url/quickCapture?url=' +
    encodeURIComponent(window.location.href) +
    '&title=' +
    encodeURIComponent(document.title)
})()
```

### Capture Selection + Page

```javascript
javascript: (function () {
  var text = window.getSelection().toString()
  window.location =
    'logseq://x-callback-url/quickCapture?content=' +
    encodeURIComponent(text) +
    '&url=' +
    encodeURIComponent(window.location.href) +
    '&title=' +
    encodeURIComponent(document.title)
})()
```

**To create a bookmarklet:**

1. Create new bookmark
2. Name it "Save to Logseq"
3. Paste the JavaScript code as the URL

---

## Automation Scripts

### Daily Journal Entry (macOS)

```bash
#!/bin/bash
# daily-note.sh

NOTE="Daily standup: $1"
ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$NOTE'))")
open "logseq://x-callback-url/quickCapture?content=$ENCODED&page=TODAY"
```

Usage: `./daily-note.sh "Finished feature X"`

### Save Clipboard (macOS)

```bash
#!/bin/bash
CONTENT=$(pbpaste)
ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$CONTENT'''))")
open "logseq://x-callback-url/quickCapture?content=$ENCODED"
```

### Windows PowerShell

```powershell
function Save-ToLogseq {
    param([string]$Content, [string]$Page = "")

    $encoded = [System.Uri]::EscapeDataString($Content)
    $url = "logseq://x-callback-url/quickCapture?content=$encoded"

    if ($Page) {
        $url += "&page=$([System.Uri]::EscapeDataString($Page))"
    }

    Start-Process $url
}

# Usage
Save-ToLogseq "Meeting notes from today" "TODAY"
```

---

## Mobile Integration

### iOS Shortcuts

1. Open Shortcuts app
2. Create new shortcut
3. Add "URL" action: `logseq://x-callback-url/quickCapture`
4. Add "Open URLs" action

### Share Sheet Integration

1. Create shortcut that accepts share input
2. Extract text/URL from input
3. Build logseq:// URL
4. Open URL

### Android (Tasker)

```
Task: Save to Logseq
A1: Variable Set [Name: %content To: %CLIP]
A2: Variable Convert [Name: %content Function: URL Encode]
A3: Browse URL [URL: logseq://x-callback-url/quickCapture?content=%content]
```

---

## Alfred/Raycast Workflows

### Alfred (macOS)

1. Create new workflow
2. Add keyword trigger: "lg"
3. Add "Open URL" action:
   ```
   logseq://x-callback-url/quickCapture?content={query}&page=TODAY
   ```

### Raycast

Create a script command:

```bash
#!/bin/bash
# @raycast.title Save to Logseq
# @raycast.argument1 { "type": "text", "placeholder": "Note" }

ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$1'))")
open "logseq://x-callback-url/quickCapture?content=$ENCODED&page=TODAY"
echo "Saved to Logseq"
```

---

## Troubleshooting

### URL Not Opening Logseq

- Ensure Logseq is installed
- Check URL starts with `logseq://`
- Try closing and reopening Logseq

### Content Not Appearing

- Check URL encoding of special characters
- Verify `content` parameter is present
- Check the target page name is correct

### Page Not Found

- Page names are case-sensitive
- Use `page=TODAY` for journal (literal)
- Create the page first if it doesn't exist

---

## See Also

- **Quick Capture**: [quick-capture.md](quick-capture.md)
- **HTTP API**: [http-api.md](../../reference/api/http-api.md)

---

_Last updated: 2026-01-29_
