---
title: 'Quick Capture'
description: 'How to quickly capture content to Logseq from anywhere'
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop, mobile]
graphType: [db]
relatedDocs:
  howTo:
    - '[X-Callback-URL](../integrations/x-callback-url.md)'
  reference:
    - '[config.edn Reference](../../reference/configuration/config-edn.md)'
    - '[Mobile Differences](../../reference/platforms/mobile-differences.md)'
---

# Quick Capture

> **Last revised:** 2026-09-11 (verified against `be800f1711`)

> Logseq has two capture paths. **Quick capture** takes content from outside the app (a desktop `logseq://` URL or a mobile share) and inserts it into the current graph. **Quick add** is an in-app scratch page you fill first and then move into today's journal.

## How Quick Capture Works

Quick capture starts outside the app.

- **Desktop:** the `logseq://x-callback-url/quickCapture` URL. The Electron main process parses it (`src/electron/electron/url.cljs:72-80`) and sends it to the focused window, or to the main window if none is focused (`src/electron/electron/utils.cljs:307-312`). The renderer then publishes `[:editor/quick-capture args]` (`src/main/electron/listener.cljs:90-92`).
- **Android:** text shared to Logseq publishes the same event (`src/main/frontend/mobile/intent.cljs:108-112`).

That event calls `quick-capture` in `src/main/frontend/quick_capture.cljs` (`src/main/frontend/handler/events.cljs:303-304`). Other mobile shares take separate paths; see [Mobile Quick Capture](#mobile-quick-capture).

The web build has no Electron main process, so it has no `logseq://` URL handler.

### Capture Destination Priority

When a quick capture arrives, Logseq picks the target page in this order ([quick_capture.cljs:42-57](../../../../../src/main/frontend/quick_capture.cljs#L42-L57)):

1. **Today's journal**: if `page=TODAY` is passed, or `page` is blank and `:insert-today?` is `true`
2. **Explicit page**: if a non-empty `page` parameter is provided
3. **Default page**: if `:default-page` is set in config (for example, `"Inbox"`)
4. **Current page**: whatever page is open in the editor (empty on the Journals view)
5. **Fallback**: today's journal

Because rule 1 wins, `:default-page` is used only when `:insert-today?` is not `true`.

The whole flow first resolves today's journal title. If it cannot, quick capture shows a date-parse error notification instead of capturing ([quick_capture.cljs:24-25, 102](../../../../../src/main/frontend/quick_capture.cljs#L24-L25)). There is no special `"quick capture"` destination: `[[quick capture]]` is just a link inside the default text template.

### Where the Content Lands

The content goes into the block you are editing when all three of these hold: you are editing a block, `append` was not passed, and that block does not already contain `[[quick capture]]`. It is inserted at the cursor, on a new line if the block is not empty. Otherwise editing is escaped, the target page is created if it is not the current page, and the content becomes a new block on it ([quick_capture.cljs:84-101](../../../../../src/main/frontend/quick_capture.cljs#L84-L101)). `:redirect-page?` is passed to that page creation as `:redirect?` ([quick_capture.cljs:95-96](../../../../../src/main/frontend/quick_capture.cljs#L95-L96)).

---

## Configure Quick Capture

In `config.edn`:

```clojure
{:quick-capture-templates
 {:text "**{time}** [[quick capture]]: {text} {url}"}

 :quick-capture-options
 {:insert-today? false      ; must not be true, or :default-page is never used
  :default-page "Inbox"
  :redirect-page? false}}
```

Template variables ([quick_capture.cljs:58-83](../../../../../src/main/frontend/quick_capture.cljs#L58-L83)):

| Variable | Replaced with |
|----------|---------------|
| `{time}` | The current time |
| `{date}` | Today's journal title |
| `{text}` | The trimmed `content` parameter |
| `{url}`  | The `url` rendered as a link: `[title](url)` (`src/main/frontend/config.cljs:258-262`), the bare URL when it equals the title, or just the title when there is no URL. YouTube and X/Twitter status links become `{{video ...}}` / `{{twitter ...}}` |

The config schema accepts `:text` and `:media` under `:quick-capture-templates`, and `:insert-today?`, `:redirect-page?`, `:default-page` and `:prettify-url?` under `:quick-capture-options` ([common_config.cljc:59-66](../../../../../src/main/frontend/schema/handler/common_config.cljc#L59-L66)).

Code defaults when a key is unset ([quick_capture.cljs:29-41, 75-77](../../../../../src/main/frontend/quick_capture.cljs#L29-L41)):

| Key | Default |
|-----|---------|
| `:quick-capture-templates :text` | `"**{time}** [[quick capture]]: {text} {url}"` |
| `:quick-capture-options :insert-today?` | `false` |
| `:quick-capture-options :redirect-page?` | `false` |
| `:quick-capture-options :default-page` | none |

See the [config.edn Reference](../../reference/configuration/config-edn.md) for the full key list.

## Known Issues

- **`:prettify-url?` cannot be turned off.** The reader looks up `[:quick-capture-option :prettify-url?]`, with singular `option` ([quick_capture.cljs:35-37](../../../../../src/main/frontend/quick_capture.cljs#L35-L37)). The config schema and the bundled `config.edn` template both spell the key `:quick-capture-options` ([config.edn template:230, 235](../../../../../deps/common/resources/templates/config.edn#L230)). Setting it under the documented key has no effect, and the value is always `true`.
- **`:quick-capture-templates :media` is never read.** Only `:text` is read, in `quick_capture.cljs:75-77` and `src/main/frontend/mobile/intent.cljs:227-229`.
- **Two template comments disagree with the code.** The bundled `config.edn` template says `:insert-today?` has "Default value: true", but the code default is `false` ([config.edn template:232](../../../../../deps/common/resources/templates/config.edn#L232), [quick_capture.cljs:29-31](../../../../../src/main/frontend/quick_capture.cljs#L29-L31)). It also gives `:default-page` a default of `"quick capture"` (`:234`), but the code has no default, so an unset `:default-page` falls through to the current page and then to today's journal.

> **Note:** `append` is a URL parameter. The desktop handler turns it into `true` only when the value is the string `"true"` (`url.cljs:79-80`). When set, the capture always becomes a new block on the target page instead of going into the block being edited.

---

## External Quick Capture (Desktop)

### From Browser

Use the x-callback URL:

```
logseq://x-callback-url/quickCapture?content=My%20quick%20note
```

**Supported URL parameters** (`src/electron/electron/url.cljs:66-73`):

| Parameter | Type   | Description |
|-----------|--------|-------------|
| `content` | string | The text to capture |
| `url`     | string | A URL to attach, formatted as a link |
| `title`   | string | Link text for `url` |
| `page`    | string | Target page name; use `TODAY` for today's journal |
| `append`  | bool   | `true` adds a new block on the target page instead of inserting into the block being edited |

The only other action handled under `x-callback-url` is `/invokeCommand` (`url.cljs:83-86`). Any other action shows an "Unimplemented x-callback-url action" error (`url.cljs:88-94`).

See: [X-Callback-URL Guide](../integrations/x-callback-url.md)

### From Command Line

The desktop app registers itself as the `logseq` protocol handler (`src/electron/electron/core.cljs:65-77`). Hand the URL to the OS:

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

Logseq has no global (system-wide) quick-capture hotkey. The recipes below are third-party wrappers that open the callback URL.

> **Unverified:** these recipes use macOS tools, not Logseq code, and were not tested for this page. The only Logseq part is the URL.

### macOS with Automator

1. Open Automator
2. Create a new Quick Action
3. Add a "Run Shell Script" action:
   ```bash
   open "logseq://x-callback-url/quickCapture?content=$(pbpaste | python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.stdin.read()))')"
   ```
4. Save as "Send to Logseq"
5. Assign a keyboard shortcut in the macOS keyboard shortcut settings

### macOS with Alfred

Create a workflow with the keyword "lg" that opens:

```
logseq://x-callback-url/quickCapture?content={query}
```

---

## Quick Add (In-App)

Every DB graph has a built-in hidden page named **Quick add** (`deps/common/src/logseq/common/config.cljs:62`). It is one of the built-in pages created with a new graph (`deps/db/src/logseq/db/sqlite/create_graph.cljs:130-133`) and is marked `:logseq.property/hide? true` (`deps/db/src/logseq/db/sqlite/util.cljs:111-112`). Graphs created before schema 65.7 get it from a migration (`src/main/frontend/worker/db/migrate.cljs:22-26`, `:127`).

### Desktop and Web

1. Press **Cmd+E** on macOS or **Ctrl+Alt+E** elsewhere (`:editor/quick-add`, `src/main/frontend/modules/shortcut/config.cljs:444-446`). The shortcut is inactive in published sites. A dialog opens showing the Quick add page, with an empty block ready if you have none (`src/main/frontend/handler/editor.cljs:4397-4422`).
2. Type your blocks.
3. Press the shortcut again, or click **Add to today**. All blocks on the Quick add page move to the end of today's journal, and you see "Blocks added to today!" (`editor.cljs:4424-4449`; `src/main/frontend/components/quick_add.cljs:57-65`).

Esc does not close this dialog (`src/main/frontend/handler/events/ui.cljs:533-540`).

### Mobile

The **Capture** tab renders the same Quick add view (`src/main/mobile/components/app.cljs:241-251`, `:278`). `logseq://mobile/go/quick-add` opens that tab (`src/main/mobile/deeplink.cljs:80-81`); the iOS widget and Control Center button use this link. The capture button in the native top bar moves the blocks to today's journal (`src/main/mobile/components/header.cljs:196-198`). Audio recorded while on the Capture tab is saved to the Quick add page (`src/main/mobile/components/recorder.cljs:70-72`).

---

## Mobile Quick Capture

See [Mobile Differences](../../reference/platforms/mobile-differences.md) for the full list of share targets and deep links.

### iOS/iPadOS

**Share Sheet:**

1. Select text, or open a link, image or file, in any app
2. Tap Share
3. Choose Logseq

The share extension opens `logseq://shared?...` with a JSON `payload` (`ios/App/ShareViewController/ShareViewController.swift:29-42`), which the app passes to `handle-payload` (`src/main/mobile/deeplink.cljs:124-134`). This path does **not** use `quick-capture`. It ignores `:quick-capture-options`, inserts into the current page or else today's journal, and uses only `:quick-capture-templates :text` (`src/main/frontend/mobile/intent.cljs:219-265`).

### Android

**Share to Logseq:**

1. Select text, or pick an image, video or file, in any app
2. Tap Share
3. Choose Logseq

Android accepts `SEND` intents for `text/plain`, `image/*`, `application/*` and `video/*` (`android/app/src/main/AndroidManifest.xml:40-46`). They are handled by `handle-result` (`intent.cljs:267-290`, reached from `src/main/mobile/init.cljs:109`):

- **Text** goes through `quick-capture`, so the destination rules and options above apply (`intent.cljs:108-112`).
- **Images and videos** are saved as assets (`intent.cljs:124-130`).
- **Supported documents** are saved as assets; other application types show an "unsupported" warning (`intent.cljs:132-159`).

> **Note:** On mobile, the `payload` share path falls back to a slightly different template: `"**{time}** [[quick capture]]​ {text} {url}"`, with a zero-width space and no colon (`intent.cljs:227-229`). The desktop default is `"**{time}** [[quick capture]]: {text} {url}"`. The fallback only matters when `:text` is unset.

The mobile app handles no `x-callback-url` host; its deep-link hosts are `mobile`, `graph` and `shared` (`deeplink.cljs:77-134`). The desktop callback URL therefore does not work from Android automation apps.

---

## Browser Extensions

Logseq does not ship a browser extension for capture. Use the desktop callback URL from a bookmarklet.

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

Click the bookmark to send the selected text and page link to the Logseq desktop app. Your browser may ask for permission to open the `logseq:` link.

---

## Tips for Efficient Capture

### 1. Add to Specific Pages

```bash
logseq://x-callback-url/quickCapture?content=Task&page=Inbox
```

### 2. Customize the Template

Set `:quick-capture-templates :text` to change the captured block's layout, for example to drop the `[[quick capture]]` link. There is only one text template; the `:media` template is not used.

### 3. Process Later

To capture everything to one page, set `:default-page` (and leave `:insert-today?` unset or `false`). To collect notes first and file them later, use the built-in **Quick add** page and send them to today's journal when you are ready.

---

## Troubleshooting

### Capture Not Working

1. Check that the Logseq desktop app is installed; it registers the `logseq:` protocol
2. Make sure a graph is open. The capture goes to the focused Logseq window.
3. Check URL encoding for special characters
4. Try simpler content first

### Wrong Page

- Check the `page` value in the URL. A page that does not exist is created.
- If `:insert-today?` is `true`, every capture without `page` goes to today's journal, even when `:default-page` is set.
- Use `page=TODAY` for today's journal

### Content Missing Parts

- URL-encode all special characters
- Quote the whole URL in shell scripts

---

## See Also

- **X-Callback-URL**: [x-callback-url.md](../integrations/x-callback-url.md)
- **config.edn**: [config-edn.md](../../reference/configuration/config-edn.md)
- **Mobile**: [mobile-differences.md](../../reference/platforms/mobile-differences.md)
- **HTTP API**: [http-api.md](../../reference/api/http-api.md)

---

_Last updated: 2026-09-11_
