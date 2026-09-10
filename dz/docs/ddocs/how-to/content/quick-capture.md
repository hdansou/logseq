---
title: 'Quick Capture'
description: 'How to quickly capture content to Logseq from anywhere'
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
logseqVersion: '0.11.x'
platforms: [desktop, mobile]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[X-Callback-URL](../integrations/x-callback-url.md)'
---

# Quick Capture

> **Last revised:** 2026-09-09 (verified against `be800f1711`)

> Quick capture is an external capture pipeline that receives content from desktop callback URLs or mobile share intents and inserts it into the current graph context.

## How Quick Capture Works

Quick capture is triggered externally via the `logseq://x-callback-url/quickCapture` URL scheme on desktop or the mobile share flow on iOS/Android. Both entry points publish `[:editor/quick-capture args]` and share the same insertion logic in `src/main/frontend/quick_capture.cljs`.

### Capture Destination Priority

When a quick capture is received, Logseq determines where to insert it using this priority order:

1. **Today's journal** -- if journals are enabled and `page=TODAY` is passed, or `page` is blank while `:insert-today?` is `true`
2. **Explicit page** -- if a non-empty `page` parameter is provided in the URL
3. **Default page** -- if `:default-page` is set in config (for example, `"Inbox"`)
4. **Current page** -- whatever page is open in the editor
5. **Fallback** -- today's journal

The whole flow is gated on today's journal title resolving first: if it cannot be resolved, quick capture aborts with a date-parse error notification instead of capturing anywhere ([quick_capture.cljs:24-25, 102](../../../../../src/main/frontend/quick_capture.cljs#L24-L25)). There is no literal `"quick capture"` destination page -- `[[quick capture]]` only appears as a link inside the default text template.

### Where the Content Lands

If you are actively editing a block when the capture arrives, and `append` was not passed, and the block you are editing does not already contain `[[quick capture]]`, the content is inserted into that block. Otherwise editing is escaped, the target page is created if needed, and the content becomes a new block on it ([quick_capture.cljs:88-101](../../../../../src/main/frontend/quick_capture.cljs#L88-L101)).

---

## Configure Quick Capture

In `config.edn`:

```clojure
{:quick-capture-templates
 {:text "**{time}** [[quick capture]]: {text} {url}"
  :media "[[quick capture]] **{time}**: {url}"}

 :quick-capture-options
 {:insert-today? true
  :default-page "Inbox"
  :redirect-page? false
  :prettify-url? true}}
```

Available template variables: `{time}`, `{date}`, `{text}`, `{url}`. Both keys are accepted by the config schema ([common_config.cljc:59-66](../../../../../src/main/frontend/schema/handler/common_config.cljc#L59-L66)); `:media` is used by the mobile share flow ([intent.cljs:228](../../../../../src/main/frontend/mobile/intent.cljs#L228)).

The runtime default for `:text`, when unset, is `"**{time}** [[quick capture]]: {text} {url}"` ([quick_capture.cljs:75-77](../../../../../src/main/frontend/quick_capture.cljs#L75-L77)).

## Findings: New Capabilities

- `:prettify-url?` is now documented in the quick-capture config template and controls whether supported URLs are formatted as `{{video ...}}` or `{{twitter ...}}` ([config.edn template:230, 235](../../../../../deps/common/resources/templates/config.edn#L230)).
- The runtime still reads that option from `[:quick-capture-option :prettify-url?]` -- singular `option` -- in [quick_capture.cljs:35-37](../../../../../src/main/frontend/quick_capture.cljs#L35-L37), while the config schema and the config.edn template both spell it `:quick-capture-options`. Setting it under the documented plural key has no effect; the value falls back to `true`.
- Quick capture now honors `append` as a URL parameter, letting external callers request append-style insertion instead of editing the active block.

> **Note:** Two more comments in the bundled `config.edn` template do not match the code. The template says `:insert-today?` has "Default value: true", but the runtime default is `false` ([quick_capture.cljs:29-31](../../../../../src/main/frontend/quick_capture.cljs#L29-L31)). It also says `:default-page` has a "Default page: `quick capture`", but the runtime reads it with no default, so an unset `:default-page` simply falls through to the current page and then to today's journal.

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

> **Removed:** There is no built-in quick-capture hotkey or modal in the current source path. These examples are external wrappers around the callback URL.

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

> **Removed:** The old widget and Shortcuts App guidance is not part of the current source-backed quick-capture path. Use Share Sheet or a custom automation flow instead.

### Android

**Share to Logseq:**

1. Select text in any app
2. Tap Share
3. Choose Logseq

**Tasker Integration:**
See [X-Callback-URL Guide](../integrations/x-callback-url.md) for Tasker setup.

---

## Browser Extensions

> **Removed:** Logseq does not ship a browser extension in this quick-capture path. Use the callback URL directly or your own bookmarklet/automation.

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

_Last updated: 2026-09-09_
