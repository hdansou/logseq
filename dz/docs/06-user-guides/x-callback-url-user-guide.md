# X-Callback-URL User Guide for Logseq

**A practical guide to sending requests into Logseq from desktop URL handlers**

> **Last revised:** 2026-05-29

---

## What is X-Callback-URL?

X-callback-url is a way to send requests into Logseq from other apps, scripts, or automation tools using special URLs. In current desktop builds, it routes into Logseq's quick capture and command invocation handlers.

**What you can do:**

- 📋 Capture highlighted text from your browser
- 🔗 Save interesting links while browsing
- ⚡ Quick-add notes from anywhere on your computer
- 🤖 Automate content collection with scripts

> **Removed:** The old mobile x-callback-url walkthroughs. Mobile capture now uses separate `logseq://mobile/...` deep links and the share-intent pipeline.

---

## Quick Start

### Basic Example

The simplest way to capture something to Logseq:

```
logseq://x-callback-url/quickCapture?content=Remember to buy milk
```

Just paste this URL in your browser or terminal and hit Enter. Logseq will open the request through its current quick-capture path.

### macOS Quick Test

Open Terminal and run:

```bash
open "logseq://x-callback-url/quickCapture?content=Hello%20from%20Terminal"
```

You should see "Hello from Terminal" captured according to your current quick-capture settings.

---

## How It Works

### The URL Format

```
logseq://x-callback-url/quickCapture?[parameters]
```

**Breaking it down:**

- `logseq://` - Tells your computer to open Logseq
- `x-callback-url` - Says "this is an automation request"
- `quickCapture` - The quick-capture action
- `invokeCommand` - The command-invocation action
- `?parameters` - What to capture and where to put it

## Findings: New Capabilities

- `logseq://x-callback-url/invokeCommand` is now supported alongside `quickCapture`.
- `invokeCommand` forwards an `action` and optional `payload` into the frontend command pipeline, which currently dispatches plugin commands.
- The desktop handler still accepts the original quick-capture parameters: `url`, `title`, `content`, `page`, and `append`.

---

## Parameters

### Available Parameters

| Parameter | What It Does                 | Example                   |
| --------- | ---------------------------- | ------------------------- |
| `content` | The text you want to capture | `content=Important note`  |
| `url`     | A web address                | `url=https://example.com` |
| `title`   | A title or headline          | `title=Article Name`      |
| `page`    | Which page to save to        | `page=Reading List`       |
| `append`  | Add to end of page?          | `append=true`             |

### Parameter Details

**`content`** - Your main text

- Any text you want to save
- Can be a quick note, copied text, or longer content
- Example: `content=Meeting at 3pm tomorrow`

**`url`** - A web link

- Full URL to save as a reference
- Will be formatted as a clickable link
- Example: `url=https://docs.logseq.com`

**`title`** - A headline

- Describes the URL or content
- Used with `url` to create named links
- Example: `title=Logseq Documentation`

**`page`** - Target page name

- Where to save the content
- Use `page=TODAY` for today's journal
- Default: Goes to "quick capture" page
- Example: `page=Ideas` or `page=TODAY`

**`append`** - Position control

- `append=true` - Add to the end of the page
- `append=false` or omit - Add at current cursor position
- Useful for maintaining reading lists or logs

---

## Common Use Cases

### 1. Capture Web Pages

**Save a link with title:**

```bash
logseq://x-callback-url/quickCapture?url=https://example.com&title=Example Website
```

**Save highlighted text from a webpage:**

```bash
logseq://x-callback-url/quickCapture?url=https://blog.com/post&title=Blog Post&content=Interesting quote here
```

### 2. Quick Notes

**Capture a thought to today's journal:**

```bash
logseq://x-callback-url/quickCapture?content=Great idea for the project&page=TODAY
```

**Add to a specific page:**

```bash
logseq://x-callback-url/quickCapture?content=New task to complete&page=Tasks
```

### 3. Reading List Management

**Add to reading list (append mode):**

```bash
logseq://x-callback-url/quickCapture?url=https://article.com&title=Must Read Article&page=Reading List&append=true
```

This always adds new items to the bottom of your Reading List page.

---

## URL Encoding

### Why You Need It

Special characters in URLs must be "encoded" - converted to a format computers understand.

**Common encodings:**

| Character | Encoded | When You'll Use It    |
| --------- | ------- | --------------------- |
| Space     | `%20`   | Almost always         |
| `&`       | `%26`   | In content text       |
| `=`       | `%3D`   | In content text       |
| `#`       | `%23`   | Tags, page references |
| `?`       | `%3F`   | In content text       |

### How to Encode

**Manual (for simple cases):**

- Replace spaces with `%20`
- Example: `Hello World` → `Hello%20World`

**Automatic (recommended):**

**macOS/Linux Terminal:**

```bash
# Using Python
python3 -c "import urllib.parse; print(urllib.parse.quote('Your text here'))"

# Using jq
echo "Your text here" | jq -sRr @uri
```

**JavaScript (browser console):**

```javascript
encodeURIComponent('Your text here')
```

**Online Tools:**

- [urlencoder.org](https://www.urlencoder.org/)
- [url-encode-decode.com](https://www.url-encode-decode.com/)

---

## Platform-Specific Guides

### macOS

#### Method 1: Terminal

```bash
open "logseq://x-callback-url/quickCapture?content=Your%20note"
```

#### Method 2: Browser Bookmarklet

Create a bookmark with this code:

```javascript
javascript: (function () {
  const url = encodeURIComponent(window.location.href)
  const title = encodeURIComponent(document.title)
  const selection = encodeURIComponent(window.getSelection().toString())
  window.location.href = `logseq://x-callback-url/quickCapture?url=${url}&title=${title}&content=${selection}&page=Reading%20List`
})()
```

**How to use:**

1. Create new bookmark in your browser
2. Name it "Send to Logseq"
3. Paste the code above as the URL
4. Click the bookmark when on any webpage to capture it

#### Method 3: Alfred Workflow

**Simple Alfred Script:**

```bash
#!/bin/bash
query="$1"
encoded=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$query'))")
open "logseq://x-callback-url/quickCapture?content=${encoded}&page=TODAY"
```

#### Method 4: Keyboard Shortcut (AppleScript)

Save as an AppleScript and assign a hotkey:

```applescript
tell application "System Events"
    keystroke "c" using {command down}
    delay 0.1
end tell

set selectedText to the clipboard as text
set encodedText to do shell script "python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' " & quoted form of selectedText

do shell script "open 'logseq://x-callback-url/quickCapture?content=" & encodedText & "&page=Fleeting%20Notes'"
```

> **Removed:** The previous iOS and Android x-callback-url examples. Their current implementation lives in the mobile deep-link and share-intent code paths, not in `x-callback-url`.

---

## Customizing Templates

### Where to Configure

Edit `config.edn` in your graph directory:

```clojure
{
 :quick-capture-templates
 {:text "**{time}** [[quick capture]]: {text} {url}"
  :media "[[quick capture]] **{time}**: {url}"}

 :quick-capture-options
 {:insert-today? true
  :redirect-page? false
  :default-page "quick capture"}
}
```

### Template Variables

Use these placeholders in your templates:

| Variable | Replaced With               | Example                |
| -------- | --------------------------- | ---------------------- |
| `{time}` | Current time                | `14:35`                |
| `{date}` | Today's date (as page link) | `[[Dec 10th, 2025]]`   |
| `{text}` | Your captured content       | Whatever you sent      |
| `{url}`  | Formatted link or media     | `[Title](https://...)` |

### Template Examples

**Simple timestamp:**

```clojure
:text "{time} - {text} {url}"
```

**Structured capture:**

```clojure
:text "📝 {time}\n  Content: {text}\n  Source: {url}\n  Tags: #inbox"
```

**Reading list format:**

```clojure
:text "📚 {url}\n  Added: {time} on {date}\n  Notes: {text}"
```

**Media captures:**

```clojure
:media "📷 Captured at {time}\n{url}"
```

### Configuration Options

**`:insert-today?`**

- `true` (default): Always save to today's journal
- `false`: Use `:default-page` or `page` parameter

**`:redirect-page?`**

- `true`: Jump to the capture page after saving
- `false` (default): Stay where you are

**`:default-page`**

- Default: `"quick capture"`
- Change to your preferred inbox page
- Example: `"Inbox"`, `"Fleeting Notes"`

---

## Real-World Recipes

### Recipe 1: Browser Extension Alternative

**Goal:** Capture any webpage with one click

**Setup:** Create a browser bookmarklet

```javascript
javascript: (function () {
  const u = encodeURIComponent(location.href)
  const t = encodeURIComponent(document.title)
  const s = encodeURIComponent(window.getSelection().toString() || '')
  location.href = `logseq://x-callback-url/quickCapture?url=${u}&title=${t}&content=${s}&page=Reading%20List&append=true`
})()
```

**Usage:** Click bookmark when on any page

### Recipe 2: Terminal Quick Note

**Goal:** Capture notes from command line

**Setup:** Add to `~/.zshrc` or `~/.bashrc`

```bash
note() {
  local content="$*"
  local encoded=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(' '.join(sys.argv[1:])))" "$@")
  open "logseq://x-callback-url/quickCapture?content=${encoded}&page=TODAY"
}
```

**Usage:**

```bash
note Remember to review the quarterly report
note Meeting with @John at 2pm #meetings
```

### Recipe 3: Python Integration

**Goal:** Capture from Python scripts

**Setup:** Create `logseq_capture.py`

```python
import urllib.parse
import subprocess
import sys

def capture(content, page="TODAY", url=None, title=None, append=False):
    params = {'content': content, 'page': page}
    if url:
        params['url'] = url
    if title:
        params['title'] = title
    if append:
        params['append'] = 'true'

    query = urllib.parse.urlencode(params)
    logseq_url = f"logseq://x-callback-url/quickCapture?{query}"
    subprocess.run(['open', logseq_url])

if __name__ == '__main__':
    if len(sys.argv) > 1:
        capture(' '.join(sys.argv[1:]))
    else:
        print("Usage: python logseq_capture.py Your note here")
```

**Usage:**

```bash
python logseq_capture.py Quick note to capture
```

### Recipe 4: Raycast Extension

**Goal:** System-wide quick capture launcher

**Setup:** Create Raycast script command

```bash
#!/bin/bash

# Required parameters:
# @raycast.schemaVersion 1
# @raycast.title Quick Capture to Logseq
# @raycast.mode compact

# Optional parameters:
# @raycast.icon 📝
# @raycast.argument1 { "type": "text", "placeholder": "Content" }
# @raycast.packageName Logseq

content="$1"
encoded=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "$content")
open "logseq://x-callback-url/quickCapture?content=${encoded}&page=TODAY"

echo "Captured to Logseq"
```

### Recipe 5: Automated Web Scraping

**Goal:** Periodically capture content from RSS/API

**Setup:** Cron job with Python script

```python
#!/usr/bin/env python3
import feedparser
import urllib.parse
import subprocess
from datetime import datetime

# RSS feed to monitor
FEED_URL = "https://news.ycombinator.com/rss"

# Fetch feed
feed = feedparser.parse(FEED_URL)

# Capture top 5 items
for entry in feed.entries[:5]:
    title = entry.title
    url = entry.link

    params = urllib.parse.urlencode({
        'url': url,
        'title': title,
        'page': 'News Feed',
        'append': 'true'
    })

    subprocess.run(['open', f'logseq://x-callback-url/quickCapture?{params}'])

print(f"Captured {len(feed.entries[:5])} items at {datetime.now()}")
```

**Cron schedule** (run daily at 9am):

```
0 9 * * * /path/to/script.py
```

---

## Troubleshooting

### Issue: Nothing Happens When I Click the Link

**Possible causes:**

1. **Logseq not installed or not set as handler**
   - Solution: Open Logseq, then try again. The first launch registers the URL scheme.

2. **URL malformed**
   - Check for typos in `logseq://x-callback-url/quickCapture`
   - Ensure parameters are properly URL-encoded

3. **Another app claimed the `logseq://` scheme**
   - macOS: Reset protocol handlers
   ```bash
   /System/Library/Frameworks/CoreServices.framework/Versions/A/Frameworks/LaunchServices.framework/Versions/A/Support/lsregister -kill -r -domain local -domain system -domain user
   ```

   - Restart Logseq

### Issue: Special Characters Look Weird

**Problem:** You see `%20` or `%26` in your captured text

**Cause:** Content is double-encoded or not decoded properly

**Solution:**

- Only encode the parameter values, not the entire URL
- Use proper encoding tools (see URL Encoding section)

**Wrong:**

```bash
# Don't encode the entire URL
open "logseq%3A%2F%2Fx-callback-url%2FquickCapture..."
```

**Correct:**

```bash
# Only encode parameter values
open "logseq://x-callback-url/quickCapture?content=Hello%20World"
```

### Issue: Content Goes to Wrong Page

**Problem:** Captures appear in unexpected locations

**Checks:**

1. **Check your config.edn:**

   ```clojure
   :quick-capture-options
   {:insert-today? true  ; ← If true, always goes to TODAY
    :default-page "quick capture"}
   ```

2. **Verify `page` parameter:**
   - Use exact page name (case-sensitive)
   - Use `page=TODAY` for journal
   - Page must exist or will be created

3. **Check template redirect:**
   ```clojure
   {:redirect-page? true}  ; ← Jumps to capture page after save
   ```

### Issue: Mobile Share Not Working

**iOS:**

- Ensure Logseq app is installed
- Try force-quitting and reopening Logseq
- Check iOS Settings → Logseq → Allow Share Extension

**Android:**

- Verify app has necessary permissions
- Clear app cache and restart
- Reinstall if issues persist

---

## Best Practices

### 1. Use Consistent Page Names

Pick standard pages for different capture types:

- `page=Inbox` - Quick thoughts
- `page=Reading List` - Articles to read
- `page=TODAY` - Daily journal entries
- `page=Ideas` - Creative thoughts

### 2. Design Useful Templates

Make templates work for you:

```clojure
;; For web captures
:text "📚 {url}\n  Title: {title}\n  Captured: {date} at {time}\n  Notes: {text}\n  Tags: #to-read"

;; For quick thoughts
:text "💭 {time} - {text}"
```

### 3. Build Reusable Scripts

Don't repeat yourself - create helper functions:

```bash
# In ~/.zshrc
logseq_idea() {
  open "logseq://x-callback-url/quickCapture?content=$*&page=Ideas"
}

logseq_task() {
  open "logseq://x-callback-url/quickCapture?content=$*&page=Tasks"
}

logseq_read() {
  open "logseq://x-callback-url/quickCapture?url=$1&page=Reading%20List&append=true"
}
```

### 4. Combine with Other Tools

**With Alfred:**

- Create keyword triggers
- Use clipboard contents
- Chain multiple actions

**With Keyboard Maestro:**

- Assign global hotkeys
- Capture selected text automatically
- Build complex workflows

**With Shortcuts (iOS):**

- Add to home screen
- Create widget buttons
- Trigger from automation

### 5. Test Your URLs

Before building complex automations:

1. Test simple capture:

   ```bash
   open "logseq://x-callback-url/quickCapture?content=Test"
   ```

2. Test with special characters:

   ```bash
   open "logseq://x-callback-url/quickCapture?content=Test%20%26%20Special"
   ```

3. Test target pages:
   ```bash
   open "logseq://x-callback-url/quickCapture?content=Test&page=YOUR_PAGE"
   ```

---

## Advanced Topics

### URL Scheme Alternatives

**Graph Switching:**

```
logseq://graph/my-graph-name
```

**Open Specific Page:**

```
logseq://graph/my-graph?page=Page%20Name
```

**Open Specific Block:**

```
logseq://graph/my-graph?block-id=65a1b2c3-4d5e-6789-abcd-ef0123456789
```

### Mobile-Only URL Schemes

**iOS/Android Shortcuts:**

```
logseq://mobile/go/quick-add    # Open quick capture UI
logseq://mobile/go/audio        # Start audio recording
```

### Integration with Other PKM Tools

**From Obsidian → Logseq:**

Create Obsidian URI handler:

```javascript
// Obsidian JavaScript snippet
const text = await navigator.clipboard.readText()
const encoded = encodeURIComponent(text)
window.open(
  `logseq://x-callback-url/quickCapture?content=${encoded}&page=From%20Obsidian`
)
```

**From Notion → Logseq:**

Use Notion API + script to export and capture:

```python
# Export Notion block and send to Logseq
notion_content = fetch_notion_block(block_id)
capture(content=notion_content, page="From Notion")
```

---

## FAQ

**Q: Can I use this on Windows or Linux?**

A: The x-callback-url scheme is primarily designed for macOS. On Windows/Linux, you can use:

- Command line with similar URL format
- Direct file operations
- Logseq's built-in quick capture (Cmd/Ctrl + Shift + C)

**Q: Is there a character limit?**

A: URLs have practical limits (~2000 characters for cross-browser compatibility). For very long content, consider:

- Breaking into multiple captures
- Using file imports instead
- Storing in external files and linking

**Q: Can I capture to multiple pages at once?**

A: No, each URL captures to one page. For multiple pages, send multiple URL requests with delays between them.

**Q: Does this work with plugins?**

A: Yes! Plugins can listen for quick capture events and extend functionality. Check plugin documentation for specifics.

**Q: Can I callback to my app when capture is complete?**

A: Not currently implemented. The x-callback-url spec supports `x-success` and `x-error` callbacks, but Logseq doesn't use them yet. This could be a future enhancement.

**Q: What about privacy/security?**

A:

- URL parameters are decoded and inserted as plain text (no code execution)
- No network requests are made from captured URLs
- Files from mobile share are sandboxed to app container
- Standard app permissions apply

---

## Resources

### Documentation

- [Quick Capture Investigation](./docs/07-investigations/quick-capture-callback-url-investigation.md)
- [X-Callback-URL Comprehensive Investigation](./docs/07-investigations/x-callback-url-comprehensive-investigation.md)

### External Links

- [X-Callback-URL Specification](http://x-callback-url.com/)
- [URL Encoding Reference](https://www.w3schools.com/tags/ref_urlencode.asp)
- [iOS Shortcuts User Guide](https://support.apple.com/guide/shortcuts/welcome/ios)
- [Alfred Workflows](https://www.alfredapp.com/workflows/)

### Community Examples

- Share your automation recipes in [Logseq Discord](https://discord.gg/logseq)
- Browse community scripts on GitHub
- Check Logseq forums for integration ideas

---

## Getting Help

**Issues with x-callback-url:**

1. Check this guide's Troubleshooting section
2. Verify your URL encoding
3. Test with simple examples first
4. Ask in Logseq Discord #help channel

**Feature requests:**

- File issues on [Logseq GitHub](https://github.com/logseq/logseq)
- Discuss in community forums
- Contribute code if you're a developer

---

## Changelog

| Version | Date         | Changes                    |
| ------- | ------------ | -------------------------- |
| 1.0     | Dec 10, 2025 | Initial user guide release |

---

**Happy Capturing! 🚀**
