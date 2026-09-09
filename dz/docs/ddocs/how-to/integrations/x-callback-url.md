---
title: 'X-Callback-URL'
description: 'How to capture content to Logseq from other apps using URL schemes'
status: stable
lastVerified: '2026-05-30'
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[Quick Capture](quick-capture.md)'
  reference:
    - '[HTTP API](../../reference/api/http-api.md)'
---

# X-Callback-URL

> **Last revised:** 2026-05-30

> How to send content to Logseq from other apps, scripts, and automation tools.

## What is X-Callback-URL?

X-callback-url is a way to send content to Logseq using special URLs. Works from browsers, scripts, and any app that can open URLs.

> **Note:** X-callback-url is supported on **desktop (Electron) only**. The mobile app handles deeplinks differently and does not process `x-callback-url` actions. For mobile capture, use the share sheet or intent-based workflows instead.

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
logseq://x-callback-url/<action>?[parameters]
```

| Part             | Purpose            |
| ---------------- | ------------------ |
| `logseq://`      | Opens Logseq       |
| `x-callback-url` | Automation request |
| `<action>`       | The action to run  |
| `?parameters`    | Action parameters  |

### Supported Actions

| Action          | Description                      |
| --------------- | -------------------------------- |
| `quickCapture`  | Capture text, URLs, or notes     |
| `invokeCommand` | Invoke an editor command by name |

---

## Parameters

### `quickCapture` Parameters

| Parameter | Description                                                     | Example                      |
| --------- | --------------------------------------------------------------- | ---------------------------- |
| `content` | Text to capture                                                 | `content=My note`            |
| `url`     | Web address                                                     | `url=https://example.com`    |
| `title`   | Link title                                                      | `title=Example Site`         |
| `page`    | Target page                                                     | `page=Ideas` or `page=TODAY` |
| `append`  | When already editing, append instead of inserting at the cursor | `append=true`                |

When `page` is omitted, Logseq first checks `quick-capture-options.insert-today?`. If that is enabled, or if you pass `page=TODAY`, it targets today's journal. Otherwise it uses config `quick-capture-options.default-page`, then the currently open page, then today's journal when journals are enabled, or a "quick capture" page when journals are disabled.

The captured content is formatted using the template in your config at `quick-capture-templates.text`. The default template is:

```
**{time}** [[quick capture]]: {text} {url}
```

Available template placeholders: `{time}`, `{date}`, `{text}`, `{url}`.

## Findings: New Capabilities

- `quick-capture-options.insert-today?` can force empty `page` captures into today's journal.
- `quick-capture-options.redirect-page?` can redirect to the page that quick capture creates.
- `quick-capture-options.prettify-url?` can auto-format tweet and video URLs while capturing links.

### `invokeCommand` Parameters

| Parameter | Description              | Example               |
| --------- | ------------------------ | --------------------- |
| `action`  | Command name to invoke   | `action=toggle-theme` |
| `payload` | Optional command payload | `payload=some-value`  |

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

> **Removed:** Page names are case-sensitive.
> **Removed:** Create the page first if it doesn't exist.

Use the exact target page name. `page=TODAY` still resolves to the current journal page.

---

## See Also

- **Quick Capture**: [quick-capture.md](quick-capture.md)
- **HTTP API**: [http-api.md](../../reference/api/http-api.md)

---

_Last updated: 2026-05-30_
