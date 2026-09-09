---
title: 'URI Schemes'
description: 'Every URI scheme Logseq registers, rewrites internally, or consumes — with the deep-link route tables for desktop and mobile, which differ'
type: reference
status: active-development
lastVerified: '2026-09-09'
reviewStatus: authored-from-source
logseqVersion: '2.0.x'
platforms: [desktop, mobile, web]
graphType: [file, db]
relatedDocs:
  reference:
    - '[Assets: Link Syntax, File Types, and Sync](../features/asset-sync.md)'
    - '[Default Paths by Platform](default-paths.md)'
  how-to:
    - '[X-Callback-URL](../../how-to/integrations/x-callback-url.md)'
    - '[Quick Capture](../../how-to/content/quick-capture.md)'
---

# URI Schemes

> Every scheme Logseq registers with the OS or Chromium, every pseudo-scheme it rewrites in app code, and every standard scheme it consumes. Verified against `origin/master` @ `be800f1711` (2026-09-08).

The single fact most likely to surprise: **the desktop and mobile `logseq://` routers are separate implementations with almost no overlap.** Only the `graph` host works on both. See [Deep-link routes](#deep-link-routes).

| Tier | Schemes |
|---|---|
| [Registered by Logseq](#registered-by-logseq) | `logseq:` `lsp:` `assets:` |
| [Internal pseudo-schemes](#internal-pseudo-schemes) | `memory:` `zotero-link:` `zotero-path:` |
| [Consumed, not registered](#consumed-not-registered) | `file:` `http(s):` `ws(s):` `zotero:` `mailto:` `data:` `blob:` `content:` |
| [Not schemes](#not-schemes) | `direct://` `socks5://` `x-callback-url` |

---

## Registered by Logseq

| Scheme | Registration | Platforms |
|---|---|---|
| `logseq:` | `setAsDefaultProtocolClient` (`src/electron/electron/core.cljs:76`); iOS `CFBundleURLSchemes` (`ios/App/App/Info.plist:45`); Android `<data android:scheme="logseq"/>` (`android/app/src/main/AndroidManifest.xml:55`) | desktop, iOS, Android |
| `lsp:` | `registerFileProtocol` (`core.cljs:101`) | desktop only |
| `assets:` | `registerFileProtocol` (`core.cljs:80`) | desktop only |

The scheme names are `defonce` constants at `core.cljs:31-33`:

```clojure
(defonce LSP_SCHEME "logseq")
(defonce FILE_LSP_SCHEME "lsp")
(defonce FILE_ASSETS_SCHEME "assets")
```

`LSP_SCHEME` is duplicated in the renderer at `src/main/frontend/util/url.cljs:7`, with a `;; Keep same as electron/electron.core` comment — there is no shared constant.

All three are declared privileged before the app window opens (`core.cljs:466-475`):

```clojure
(let [privileges {:standard true :secure true :bypassCSP true
                  :corsEnabled true :supportFetchAPI true}]
  (.registerSchemesAsPrivileged
   protocol (bean/->js [{:scheme LSP_SCHEME        :privileges privileges}
                        {:scheme FILE_LSP_SCHEME   :privileges privileges}
                        {:scheme FILE_ASSETS_SCHEME
                         :privileges (assoc privileges :stream true)}])))
```

`:stream true` on `assets` is what lets large media be served without buffering. The two file protocols are torn down together on shutdown (`core.cljs:146-147`).

### `lsp:` — the app and its plugins

The production window loads `lsp://logseq.com/index.html` (`src/electron/electron/window.cljs:25`), so **`lsp://logseq.com` is the app's origin**. That is why `file://` subresources are cross-origin and blocked — see [Assets](../features/asset-sync.md#absolute-paths-and-file).

| Prefix | Constant | Resolves to |
|---|---|---|
| `lsp://logseq.com/…` | `STATIC_URL` | the app bundle, joined onto `__dirname` |
| `lsp://logseq.io/plugins/…` | `PLUGIN_URL` | `~/.logseq/plugins/…` |
| `lsp://logseq.com/plugins/…` | `HOST_PLUGIN_URL` | same as above |
| `lsp://logseq.io/external/…` | `EXTERNAL_PLUGIN_URL` | `<decoded root>/<relative>` — an arbitrary root, for plugins under development |
| `lsp://logseq.com/external/…` | `HOST_EXTERNAL_PLUGIN_URL` | same as above |

Constants at `core.cljs:35-39`; the handler that dispatches between them is `core.cljs:101`. `logseq.io` and `logseq.com` are interchangeable hosts here — neither is contacted over the network.

### `assets:` — local files by absolute path

Covered in depth in [Assets: Link Syntax](../features/asset-sync.md#assets-is-a-real-registered-scheme). In short, `assets://<absolute path>` serves that file, with three address forms:

| Form | Example |
|---|---|
| POSIX absolute | `assets:///Users/you/pic.png` |
| Windows drive | `assets:///C:/Users/you/pic.png` |
| Windows UNC | `assets://server/share/pic.png` |

The `"assets://"` literal is also a shared constant, `common-config/asset-protocol` (`deps/common/src/logseq/common/config.cljs:31`).

---

## Deep-link routes

`logseq://` is the only scheme reachable from outside the app. **Desktop and mobile route it independently**, and the tables below are the whole of each.

### Desktop

`logseq-url-handler` (`src/electron/electron/url.cljs:96`) dispatches on the URL **host**:

| URL | Effect | Line |
|---|---|---|
| `logseq://graph/<name>?page=&block-id=&file=` | Open or focus that graph; redirect if a target is given | `:105` |
| `logseq://new-window/<name>?page=&block-id=&file=` | Same, forced into a new window | `:108` |
| `logseq://x-callback-url/quickCapture?url=&title=&content=&page=&append=` | Quick capture into `page` (or `TODAY`) | `:101` → `:62` |
| `logseq://x-callback-url/invokeCommand?action=&payload=` | Invoke a registered command | `:101` → `:62` |
| `logseq://handbook/<key>?…` | Open a handbook pane | `:111` |

Anything else raises `:electron/link-open-failed-no-target`. An unknown `x-callback-url` action raises `:electron/unimplemented-callback` — so only these two actions exist.

`graph`/`new-window` resolve `<name>` through `handler/get-graph-name`; a name that matches no linked graph raises `:electron/link-open-failed-no-graph`, and an empty one `:electron/link-open-failed-missing-graph` (`url.cljs:24-33`).

### Mobile

`deeplink` (`src/main/mobile/deeplink.cljs:57`), reached from `handle-incoming-url!` (`src/main/mobile/init.cljs:25`):

| URL | Effect | Line |
|---|---|---|
| `logseq://graph/<name>?page=&block-id=` | Switch graph, then redirect | `:83` |
| `logseq://mobile/go/audio` | Start audio recording | `:77` |
| `logseq://mobile/go/quick-add` | Open the capture tab | `:80` |
| `logseq://shared?title=&url=&type=&payload=` | Share-sheet ingest — `payload` is URI-encoded JSON | `:124` |

Anything else falls to `:else nil` — **silently ignored, with no notification**.

Mobile also accepts the same routes over `https://logseq.com/…`: `handle-incoming-url!` admits both prefixes (`init.cljs:32-33`) and `deeplink` strips `logseq.com/` before parsing. A separate web-target parser handles `/page/<id>?graph-id=` and `/block/<id>?graph-id=` URLs, including hash-routed variants (`src/main/frontend/util/url.cljs` `parse-web-url-target`).

`logseq://mobile/go/quick-add` is additionally special-cased at cold launch (`init.cljs:87-90`), which selects the capture tab after a 2-second delay.

### What is not shared

| Host | Desktop | Mobile |
|---|---|---|
| `graph` | ✅ | ✅ |
| `new-window` | ✅ | ❌ |
| `x-callback-url` | ✅ | ❌ |
| `handbook` | ✅ | ❌ |
| `mobile` | ❌ | ✅ |
| `shared` | ❌ | ✅ |

> **`x-callback-url` does not reach the mobile router.** There is no such branch in `deeplink.cljs`, and `git log -S "x-callback-url"` over the mobile deeplink files returns nothing — it was never implemented there, not removed. A `logseq://x-callback-url/quickCapture?…` URL opened on iOS or Android passes the dedup guard, reaches `deeplink`, matches no host, and returns `nil` with no error shown. [X-Callback-URL](../../how-to/integrations/x-callback-url.md) still documents iOS Shortcuts and Tasker recipes that use it; treat that section as unverified against 2.0.x and confirm on a device before relying on it. Mobile capture goes through `logseq://shared` or the share sheet instead.

---

## Internal pseudo-schemes

These never reach Chromium's network stack — app code rewrites them first.

| Scheme | Rewritten to | Where |
|---|---|---|
| `memory://` | lightning-fs / IndexedDB | `src/main/frontend/fs.cljs:43` dispatches on the prefix; `worker/platform/browser.cljs:110` builds `memory:///<graph>/assets/` |
| `zotero-link://<rel>` | `file://<zotero-linked-attachment-base-directory>/<rel>` | `src/main/frontend/extensions/pdf/assets.cljs:225` |
| `zotero-path://<key>/<name>` | `file://<zotero-data-directory>/storage/<key>/<name>` | `assets.cljs:228` |

Both `zotero-*` forms are written during file-graph import (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1729-1730`) into `:logseq.property.asset/external-file-name`, then resolved against `:zotero/settings-v2` when a PDF is opened. Both prefixes are exactly 14 characters, which is why the resolver does a bare `(subs path 14)`.

`memory://` is a path prefix, not a protocol: `is-file-url?` (`deps/common/src/logseq/common/path.cljs:14`) groups it with `file://` and `assets://` for path-joining purposes, and the FS layer routes on it.

---

## Consumed, not registered

| Scheme | Use | Source |
|---|---|---|
| `file:` | Desktop paths throughout | `path.cljs:14`, `handler/assets.cljs` |
| `http:` `https:` | Everything remote; the web build's own origin | — |
| `ws:` `wss:` | RTC sync transport — `wss://api.logseq.io/sync/%s`, `ws://127.0.0.1:8787/sync/%s` in dev | `src/main/frontend/config.cljs:43-44` |
| `zotero:` | `zotero://select/library/items/<key>`, stored as `:logseq.property.asset/external-url` and opened via `shell.openExternal` | `components/block.cljs:516` |
| `mailto:` | Allowed target for `openExternal` | `resources/js/preload.js:9` |
| `data:` | Export download hrefs (`text/html`, `text/edn`, `text/transit`, `text/json`); also an XSS check on pasted HTML | `handler/export.cljs:36,139,197`; `handler/db_based/export.cljs:95`; `extensions/html_parser.cljs:148` |
| `blob:` | `URL.createObjectURL` for assets on browser and mobile | `handler/assets.cljs:148` |
| `content:` | Android SAF document URIs — Java layer only, never seen by ClojureScript | `android/app/src/main/java/com/logseq/app/FileUtil.java:85` |

### The `openExternal` allowlist

Following an external link is gated in the preload bridge:

```js
// resources/js/preload.js:9
const ALLOWED_EXTERNAL_PROTOCOLS = ['https:', 'http:', 'mailto:', 'zotero:', 'file:']
```

```js
// resources/js/preload.js:84
async openExternal (url, options) {
  const protocol = new URL(url).protocol
  if (!ALLOWED_EXTERNAL_PROTOCOLS.includes(protocol)) {
    throw new Error('illegal protocol')
  }
  await shell.openExternal(url, options)
},
```

Narrower than the list above. Notably `assets:` is **not** on it — which is why `[t](assets://…)` without the `!` prefix fails; see [Assets](../features/asset-sync.md#dont).

### Two different "is this a URL?" predicates

They are not interchangeable, and picking the wrong one is an easy bug:

```clojure
;; deps/common/src/logseq/common/path.cljs:14 — three specific schemes
(defn is-file-url? [s]
  (and (string? s)
       (or
        (string/starts-with? s "memory://") ;; special memory fs
        (string/starts-with? s "assets://") ;; Electron asset, urlencoded
        (string/starts-with? s "file://")   ;; Electron files
        )))

;; deps/common/src/logseq/common/path.cljs:290 — any scheme-ish prefix
(defn protocol-url? [p]
  (boolean (and (re-find #"^[a-zA-Z0-9_+\-\.]{2,}:" p)   ;; HACK: avoid matching windows drive
                (not (string/includes? p " ")))))
```

The `{2,}` in `protocol-url?` is there so `C:/Users/…` is not mistaken for a scheme.

---

## Not schemes

| Looks like | Actually |
|---|---|
| `direct://`, `socks://`, `socks5://` | Chromium **proxy-rule syntax** passed to `session.setProxy` (`src/electron/electron/utils.cljs:81`), not URIs Logseq handles |
| `x-callback-url` | A *host* under `logseq:`, not a scheme — `logseq://x-callback-url/…` (`url.cljs:101`). The Apple convention is often written as though it were one |
| `logseq://plugin/frame` | Appears only in `src/test/frontend/security_test.cljs` as a DOMPurify fixture. There is no such route; it survives sanitization because `security.cljs:35` sets `ALLOW_UNKNOWN_PROTOCOLS true` |

---

## Summary

| Question | Answer |
|---|---|
| Which schemes does Logseq own? | `logseq:` (all platforms), `lsp:` and `assets:` (desktop only) |
| Which can other apps use? | `logseq:` only |
| Does `x-callback-url` work on mobile? | No — desktop only |
| Why is `file://` blocked in the app? | The page origin is `lsp://logseq.com` and production sets `webSecurity true` |
| How do I display a local file? | `assets://<abs path>` — see [Assets](../features/asset-sync.md) |
| Can a plugin register a scheme? | No. The three registrations are hard-coded in `core.cljs` |

## Keeping this page honest

The scheme constants, the `openExternal` allowlist, and the mobile scheme registrations are declared in `dz/support-engineering/script/sync-limits/uri-schemes.edn`:

```
bb dz/support-engineering/script/sync-limits/check_sync_limits.clj \
   --root <logseq-checkout> --declaration uri-schemes.edn
```

The route tables are not machine-checkable. Re-derive them by re-reading the `cond` in `logseq-url-handler` (`electron/url.cljs:96`) and in `deeplink` (`mobile/deeplink.cljs:57`).
