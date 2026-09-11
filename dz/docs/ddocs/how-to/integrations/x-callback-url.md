---
title: 'X-Callback-URL'
description: 'How to capture content to Logseq from other apps using logseq://x-callback-url links'
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop]
graphType: [db]
relatedDocs:
  howTo:
    - '[Quick Capture](../content/quick-capture.md)'
  reference:
    - '[HTTP API](../../reference/api/http-api.md)'
    - '[URI Schemes](../../reference/platforms/uri-schemes.md)'
---

# X-Callback-URL

> **Last revised:** 2026-09-11

> How to send content to Logseq from other apps, scripts and automation tools.

## What is X-Callback-URL?

`logseq://x-callback-url/…` links let any app, script or browser that can open a URL send content to the
Logseq desktop app.

The packaged app registers itself as the handler for the `logseq` scheme
(`resources/electron-builder.yml:39-42`; `setAsDefaultProtocolClient`,
`src/electron/electron/core.cljs:66-77`). On Linux the package also declares the MIME type
`x-scheme-handler/logseq` (`electron-builder.yml:88-89`). The URL reaches the app in one of three ways:

- **macOS:** through the `open-url` event (`core.cljs:282-288`).
- **Windows and Linux, app not running:** as a command-line argument on first launch (`core.cljs:290-296`,
  `:406-407`).
- **Windows and Linux, app already running:** through the `second-instance` event (`core.cljs:483-489`).

> **Note:** X-callback-url works on **desktop (Electron) only**. The mobile router
> (`src/main/mobile/deeplink.cljs:77-134`) has no `x-callback-url` branch, so these URLs do nothing on iOS or
> Android. On mobile, use the share sheet or `logseq://mobile/go/quick-add`. The web app has no `logseq://`
> handler. The full desktop and mobile route tables are in
> [URI Schemes](../../reference/platforms/uri-schemes.md#deep-link-routes).

---

## Quick Start

### Basic Capture

Open this URL to send text to Logseq:

```
logseq://x-callback-url/quickCapture?content=Remember%20to%20buy%20milk
```

A graph must be open. Quick capture first looks up today's journal title. If it cannot, it shows an error
and captures nothing (`src/main/frontend/quick_capture.cljs:24-25`, `:102`).

### Test It Now

**macOS Terminal:**

```bash
open "logseq://x-callback-url/quickCapture?content=Hello%20from%20Terminal"
```

**Windows PowerShell:**

```powershell
Start-Process "logseq://x-callback-url/quickCapture?content=Hello%20from%20PowerShell"
```

**Linux:**

```bash
xdg-open "logseq://x-callback-url/quickCapture?content=Hello%20from%20Linux"
```

These commands were derived from the registration above and not run. Whether `xdg-open` finds Logseq
depends on how the app was installed.

---

## URL Structure

```
logseq://x-callback-url/<action>?[parameters]
```

| Part             | Purpose                                                                         |
| ---------------- | ------------------------------------------------------------------------------- |
| `logseq://`      | Opens Logseq. Other schemes are ignored (`core.cljs:63-64`)                     |
| `x-callback-url` | The URL host. It selects the x-callback-url router (`src/electron/electron/url.cljs:101-102`) |
| `<action>`       | The URL path. It is compared exactly, including case (`url.cljs:65`, `:72`, `:83`) |
| `?parameters`    | Action parameters, read with `URL.searchParams` (`url.cljs:12-22`)             |

### Supported Actions

| Action          | Description                                                  | Source             |
| --------------- | ------------------------------------------------------------ | ------------------ |
| `quickCapture`  | Capture text and/or a link into a page                       | `url.cljs:72-81`   |
| `invokeCommand` | Call a command or model function exposed by a plugin         | `url.cljs:83-86`   |

Any other action shows the error "Unimplemented x-callback-url action" (`url.cljs:88-94`).

---

## Parameters

### `quickCapture` Parameters

| Parameter | Description                                                                        | Example                      |
| --------- | ---------------------------------------------------------------------------------- | ---------------------------- |
| `content` | Text to capture. It is trimmed (`quick_capture.cljs:59`)                           | `content=My%20note`          |
| `url`     | Web address                                                                        | `url=https://example.com`    |
| `title`   | Link title                                                                         | `title=Example%20Site`       |
| `page`    | Target page name, or `TODAY` for today's journal                                   | `page=Ideas` or `page=TODAY` |
| `append`  | `true` to add a new block to the page even while you are editing a block (see below) | `append=true`              |

`append` is `true` only for the exact string `true`. Any other value counts as false (`url.cljs:78-80`).

**Which page receives the capture** (`quick_capture.cljs:42-57`), in order:

1. Today's journal, if `page=TODAY`, or if `page` is empty and `:quick-capture-options {:insert-today? true}`
   is set.
2. The `page` parameter, if it is not empty.
3. `:quick-capture-options {:default-page "…"}` from your config.
4. The page currently open.
5. Today's journal.

**Where it is inserted** (`quick_capture.cljs:84-101`):

- **You are editing a block, `append` is not `true`, and that block does not already contain `[[quick capture]]`:**
  the text is inserted at the cursor, on a new line if the block already has text. The `page` parameter is
  ignored in this case.
- **Otherwise:** editing stops. If the target page is not the page currently open, Logseq calls
  `page-handler/<create!` for it, which creates the page when none exists (`:95-96`). Then it adds the text
  as a new block on that page (`:98-100`).

**What is inserted** comes from the template at `:quick-capture-templates {:text "…"}`. The default is
(`quick_capture.cljs:75-77`):

```
**{time}** [[quick capture]]: {text} {url}
```

| Placeholder | Replaced with                                                             |
| ----------- | ------------------------------------------------------------------------- |
| `{time}`    | The current time (`date/get-current-time`, `:58`)                         |
| `{date}`    | Today's journal title (`:78`, `:81`)                                      |
| `{text}`    | The trimmed `content` (`:59`, `:83`)                                      |
| `{url}`     | A link built from `url` and `title` (`:60-74`, `:82`)                     |

How `{url}` is built (`quick_capture.cljs:60-74`):

- No `url`: just the `title`.
- A recognised video URL: `title {{video url}}`.
- A `twitter.com` or `x.com` status URL: `{{twitter url}}`.
- `title` equal to `url`: a plain link.
- Otherwise: a link with `title` as its label.

## Config Options

| Key                                            | Default | Effect                                                     |
| ---------------------------------------------- | ------- | ---------------------------------------------------------- |
| `:quick-capture-options {:insert-today? …}`    | `false` | Send captures with no `page` to today's journal (`:29-31`) |
| `:quick-capture-options {:redirect-page? …}`   | `false` | Open the target page when quick capture creates it (`:32-34`, `:96`) |
| `:quick-capture-options {:default-page …}`     | none    | Page used when `page` is empty (`:40-41`)                  |
| `:quick-capture-templates {:text …}`           | see above | Template for the inserted text (`:75-77`)                |

> **Logseq defect: `prettify-url?` is read from a misspelled key.** The video and tweet formatting is
> controlled by `(get-in config [:quick-capture-option :prettify-url?] true)`, with `option` singular
> (`quick_capture.cljs:35-37`). Setting `:quick-capture-options {:prettify-url? false}` therefore has no
> effect, and the formatting stays on. Read from source; not run.

### `invokeCommand` Parameters

| Parameter | Description                                                         | Example                                   |
| --------- | ------------------------------------------------------------------- | ----------------------------------------- |
| `action`  | `plugin.<plugin-id>.<group>.<key>`                                  | `action=plugin.my-plugin.commands.doThing` |
| `payload` | Optional string, passed to the plugin as the only argument          | `payload=some-value`                      |

The renderer splits `action` on `.` (`src/main/frontend/handler/events.cljs:306-314`):

- The first segment must be `plugin`. Any other value is only logged as a warning (`:314`).
- The second and third segments are the plugin ID and the group. The last segment is the key.
- It then calls `invoke_external_plugin_cmd` (`src/main/logseq/api/plugin.cljs:401-408`). The group
  `models` calls the plugin's model function with that key. The group `commands` calls the plugin's
  command with that key.

`invokeCommand` does **not** run built-in editor commands. A value such as `action=toggle-theme` only
logs a warning.

> **Unverified:** which key names a plugin must register for `commands` and `models` lookups to succeed.
> Also unverified: what happens with a group other than those two. The `case` in `plugin.cljs:403-408` has
> no default branch.

---

## Common Use Cases

These URLs were derived from source and not run.

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

Parameter values are decoded by the standard `URL.searchParams` API (`url.cljs:12-22`). Encode special
characters:

| Character | Encoded |
| --------- | ------- |
| Space     | `%20`   |
| `+`       | `%2B`   |
| `&`       | `%26`   |
| `=`       | `%3D`   |
| `#`       | `%23`   |
| `?`       | `%3F`   |

Under the URL standard, `URLSearchParams` decodes a literal `+` as a space. That is standard browser and
Node behaviour, not Logseq code.

### Encode Automatically

**macOS/Linux:**

```bash
python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "Your text here"
```

**JavaScript:**

```javascript
encodeURIComponent('Your text here')
```

---

## Browser Bookmarklets

Whether a browser follows `logseq://` from a bookmarklet, and whether it asks first, depends on the browser.
These were not tested.

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

1. Create a new bookmark.
2. Name it "Save to Logseq".
3. Paste the JavaScript code as the URL.

---

## Automation Scripts

The scripts pass the text to Python as an argument, so quotes and apostrophes in the text do not break them.

### Daily Journal Entry (macOS)

```bash
#!/bin/bash
# daily-note.sh

NOTE="Daily standup: $1"
ENCODED=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$NOTE")
open "logseq://x-callback-url/quickCapture?content=$ENCODED&page=TODAY"
```

Usage: `./daily-note.sh "Finished feature X"`

### Save Clipboard (macOS)

```bash
#!/bin/bash
CONTENT=$(pbpaste)
ENCODED=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$CONTENT")
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

1. Create a new workflow.
2. Add a keyword trigger: "lg".
3. Add an "Open URL" action:
   ```
   logseq://x-callback-url/quickCapture?content={query}&page=TODAY
   ```

> **Unverified:** whether Alfred URL-encodes `{query}` for you. That is an Alfred setting, not Logseq
> behaviour.

### Raycast

Create a script command:

```bash
#!/bin/bash
# @raycast.schemaVersion 1
# @raycast.mode silent
# @raycast.title Save to Logseq
# @raycast.argument1 { "type": "text", "placeholder": "Note" }

ENCODED=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1")
open "logseq://x-callback-url/quickCapture?content=$ENCODED&page=TODAY"
echo "Saved to Logseq"
```

---

## Troubleshooting

### URL Not Opening Logseq

- Make sure the desktop app is installed. Development builds and unpackaged apps may not be registered for
  the scheme.
- Check that the URL starts with `logseq://`.
- On macOS, the `open-url` handler only acts when the main window exists (`core.cljs:284-288`). If the app
  was not running, try again once it has opened.

> **Unverified:** whether a link that launches Logseq on macOS is lost before the window exists. That
> depends on event timing that was not tested.

### "Unimplemented x-callback-url action"

- The action is case-sensitive: use `quickCapture` or `invokeCommand` exactly.

### Content Not Appearing

- Check the URL encoding of special characters, including `+`.
- If you were editing a block, the text went into that block at the cursor. Add `append=true` to get a new
  block on the target page instead.
- Make sure a graph is open. Quick capture needs today's journal title.

### Wrong Page

- `page` is used exactly as given. A page that does not exist yet is created.
- `page=TODAY` targets today's journal page.
- With no `page`, the capture goes to `:default-page` or the page you have open (see
  [Which page receives the capture](#quickcapture-parameters)).

---

## See Also

- **Quick Capture**: [quick-capture.md](../content/quick-capture.md)
- **URI Schemes**: [uri-schemes.md](../../reference/platforms/uri-schemes.md)
- **HTTP API**: [http-api.md](../../reference/api/http-api.md)

---

_Last updated: 2026-09-11_
