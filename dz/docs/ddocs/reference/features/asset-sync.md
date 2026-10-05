---
title: 'Assets: Link Syntax, File Types, and Sync'
description: 'How to reference a file from a block (every syntax, and which ones work), which file types DB sync replicates (all of them), and what actually makes a file eligible to sync'
type: reference
status: active-development
lastVerified: '2026-09-09'
reviewStatus: authored-from-source
logseqVersion: '2.0.x'
platforms: [desktop, mobile, web]
graphType: [db]
relatedDocs:
  reference:
    - '[Sync Limits, Throttles, and Batch Sizes](sync-limits.md)'
    - '[Default Paths by Platform](../platforms/default-paths.md)'
    - '[URI Schemes](../platforms/uri-schemes.md)'
  how-to:
    - '[Asset Management](../../how-to/multimedia/asset-management.md)'
  explanation:
    - '[E2EE Encryption](../../explanation/security/e2ee-encryption.md)'
---

# Assets: Link Syntax, File Types, and Sync

> Every way to reference a file from a block and what each one actually renders, plus what DB sync (RTC) will and will not replicate. Verified against `origin/master` @ `be800f1711` (2026-09-08); link syntax confirmed by parsing each form with `mldoc@1.5.9`, the build the app ships.

Three facts govern this page:

1. **The `!` prefix decides everything about rendering.** `![x](…)` embeds; `[x](…)` links. They take different code paths and behave differently for the same target.
2. **There is no file-extension allowlist in the sync path.** Any extension replicates. The only requirement is that the file *has* an extension.
3. **Sync is driven by database entities, never by scanning the assets directory.** A file copied into `assets/` by hand, or referenced by a raw path, is invisible to sync.

| Section | Answers |
|---|---|
| [Link syntax](#link-syntax) | How do I reference a file? Which forms work? |
| [Do](#do) / [Don't](#dont) | Which form should I use, and what should I avoid? |
| [File types](#file-types) | What can I attach? |
| [Sync eligibility](#sync-eligibility) | What actually gets replicated? |
| [Limits and storage](#size-limits) | How big, where stored |

---

# Link syntax

## Quick reference

Each row was parsed with the shipped mldoc build; `url` is the AST node the renderer dispatches on.

| Markdown | mldoc `url` | Renders as | |
|---|---|---|---|
| `![t](assets/<uuid>.png)` | `Search` | inline `<img src="assets://…">` | ✅ **recommended** |
| `[t](assets/<uuid>.png)` | `Search` | file link → `shell.openPath` | ✅ |
| `![t](assets:///abs/path.png)` | `Complex {protocol "assets"}` | inline `<img src="assets://…">` | ✅ for files outside the graph |
| `[t](assets:///abs/path.png)` | `Complex` | `<a target="_blank">` | ⚠️ `assets:` is not an allowed external protocol |
| `[t](file:///abs/path.png)` | `Complex {protocol "file"}` | file link → `shell.openPath` | ✅ |
| `[t](/abs/path.png)` | `Search` | file link → `shell.openPath` | ✅ |
| `![t](file:///abs/path.png)` | `Complex {protocol "file"}` | `<img src="file://…">` | ❌ blocked in the packaged app |
| `![t](/abs/path.png)` | `Search` | `<img src="file://…">` | ❌ blocked in the packaged app |
| `![t](https://host/pic.png)` | `Complex {protocol "https"}` | inline `<img>` | ✅ |
| `https://host/pic.png` (bare) | `Complex` | inline `<img>` | ✅ auto-embeds |
| `[t](https://host/pic.png)` | `Complex` | plain external link | ✅ by design — no image |
| `https://host/clip.mp4` (bare) | `Complex` | `<video>` player | ✅ |
| `https://host/doc.pdf` (bare) | `Complex` | plain external link | ⚠️ `pdf` ∉ `media-formats` |
| `![t](https://host/doc.pdf)` | `Complex` | PDF asset viewer | ✅ |

## Do

| Goal | Use | Why |
|---|---|---|
| Reference a file already in the graph | `[[6800167b-…]]` — the asset block's uuid | The only form that is a real asset reference: it syncs, deduplicates, appears in the asset list, and is exported with the graph |
| Same, but you need markdown | `![t](assets/6800167b-….png)` | `local-relative-asset?` routes it through `<make-asset-url`, which joins the graph root at render time — survives the graph being moved, and works on desktop, browser, and mobile |
| Display an external file inline | `![t](assets:///Users/you/pic.png)` | `assets:` is a registered privileged scheme with a handler that serves absolute paths; the only protocol that renders inline in the packaged app |
| Open an external file in its native app | `[t](/Users/you/report.docx)` or `[t](file:///Users/you/report.docx)` | Both resolve to `shell.openPath`; no `!`, so no embed is attempted |
| Embed a remote image | `![t](https://host/pic.png)`, or paste the bare URL | A bare media URL auto-embeds; `!` forces it for any URL |
| Embed a remote PDF | `![t](https://host/doc.pdf)` | `pdf` is in `doc-formats`, so the `!` routes it to the PDF viewer |
| Make an external file travel with the graph | Drag, paste, or use the file picker | Only these create an asset entity with type + size + checksum |
| Ship a change that relies on an embed | Verify it in a **packaged** build | Dev sets `webSecurity false` and serves from `localhost`; some forms render there and fail once packaged |

## Don't

| Avoid | Instead | Why |
|---|---|---|
| `![t](file:///Users/you/pic.png)` | `![t](assets:///Users/you/pic.png)` | Emits `<img src="file://…">`; Chromium refuses a `file://` subresource from the app's `lsp://` origin |
| `![t](/Users/you/pic.png)` | `![t](assets:///Users/you/pic.png)` | Identical outcome — `asset-container` prefixes `file://` and nothing converts it |
| `[t](assets:///Users/you/pic.png)` | `![t](assets:///…)` to display, or `[t](/Users/…)` to open | Without `!` this is a plain `<a target="_blank">`, and `assets:` is not in `ALLOWED_EXTERNAL_PROTOCOLS` — the click throws `illegal protocol` |
| An absolute path into your own graph — `![t](/Users/you/logseq/graphs/g/assets/<uuid>.png)` | `[[<uuid>]]` or `![t](assets/<uuid>.png)` | That file is already a managed asset; the absolute path bypasses everything that makes it one, and breaks if the graph moves |
| Copying a file into `assets/` by hand | Drag, paste, or the file picker | No transaction is produced, so no sync op is emitted and nothing scans the directory |
| Expecting a path link to sync | Create a real asset | A link is text in `:block/title`; only asset entities replicate |
| Assuming `[t](https://host/pic.png)` shows the image | `![t](https://host/pic.png)` | `show-link?` needs `!` or a *bare* URL; labelled links stay links by design |
| Assuming a bare `.pdf` URL embeds | `![t](https://host/doc.pdf)` | `media-formats` covers image/audio/video only — `pdf` lives in `doc-formats` |
| A filename with no extension | Rename to add one | Ingest throws `File doesn't have a valid ext.` — see [File types](#file-types) |

## The `!` rule

One predicate gates every embed:

```clojure
;; src/main/frontend/components/block.cljs:1434
(defn- show-link?
  [s full-text]
  (let [media-formats (set (map name config/media-formats))]
    (or
     ;; markdown
     (string/starts-with? (string/triml full-text) "!")

     ;; image http link
     (and (or (string/starts-with? full-text "http://")
              (string/starts-with? full-text "https://"))
          (text-util/media-link? media-formats s)))))
```

True → `media-link` (embed). False → a link. Two consequences worth internalising:

- A **bare** `https://…/pic.png` embeds, because `full-text` *is* the URL. The same URL wrapped as `[label](…)` does not, because `full-text` then starts with `[`. This is deliberate: labelled links stay links.
- The second clause tests `media-formats` = `common-config/img-formats` ∪ audio ∪ video. **`pdf` is not in it** (it lives in `doc-formats`), so a bare PDF URL stays a plain link. Prefix it with `!` to get the viewer.

`media-link?` tolerates query strings and fragments:

```clojure
;; src/main/frontend/util/text.cljs:15
(re-pattern (str "(?i)\\." fmt "(?:\\?([^#]*))?(?:#(.*))?$"))
```

so `https://host/pic.png?w=100` still embeds.

## Graph-relative — the form to prefer

```markdown
![test](assets/6800167b-5ef9-4899-89a6-d8e933932d94.png)
```

This is the only form that is portable across desktop, browser, and mobile, and the only one that survives the graph directory being moved or renamed.

`local-relative-asset?` (`deps/common/src/logseq/common/config.cljs:68`) matches it — no `://`, and the path matches `^[./]*assets` — which routes it into `asset-link` and then `<make-asset-url`:

```clojure
;; src/main/frontend/handler/assets.cljs:179
(util/electron?)
(let [full-path (if local-asset?
                  (path/path-join repo-dir path) path)]
  ;; fullpath will be encoded
  (path/prepend-protocol "assets:" (protect-windows-drive-in-assets-path full-path)))

:else
(p/let [binary (fs/read-file-raw repo-dir path {})
        svg? (string/ends-with? path ".svg")
        type (if svg? "image/svg+xml" "image")
        blob (js/Blob. (array binary) (clj->js {:type type}))]
  (when blob (js/URL.createObjectURL blob)))
```

The graph root is joined in at render time, so:

| Platform | Resolves to |
|---|---|
| Desktop (Electron) | `assets:///Users/you/logseq/graphs/<graph>/assets/<uuid>.png` |
| Browser, mobile | a `blob:` URL read out of lightning-fs |

`../assets/…` and `./assets/…` work identically — the leading `[./]*` is stripped by `get-local-asset-absolute-path` (`config.cljs:322`).

## `assets://` is a real registered scheme

Not a convention — a privileged Electron protocol, registered before the app window opens:

```clojure
;; src/electron/electron/core.cljs:34
(defonce FILE_ASSETS_SCHEME "assets")

;; src/electron/electron/core.cljs:479
(let [privileges {:standard true :secure true :bypassCSP true
                  :corsEnabled true :supportFetchAPI true}]
  (.registerSchemesAsPrivileged
   protocol (bean/->js [{:scheme LSP_SCHEME        :privileges privileges}
                        {:scheme FILE_LSP_SCHEME   :privileges privileges}
                        {:scheme FILE_ASSETS_SCHEME
                         :privileges (assoc privileges :stream true)}])))
```

and served by a file-protocol handler:

```clojure
;; src/electron/electron/core.cljs:81
(.registerFileProtocol
 protocol FILE_ASSETS_SCHEME
 (fn [^js request callback]
   (let [url  (decode-protected-assets-schema-path (.-url request))
         path (js/decodeURIComponent (string/replace url "assets://" ""))]
     (cond (or (string/starts-with? path "/")
               (re-find #"(?i)^/[a-zA-Z]:" path))
           (callback #js {:path path})

           ;; assume windows unc path
           utils/win32?
           (callback #js {:path (str "//" path)})

           :else
           (callback #js {:path path})))))
```

Three branches, three address forms:

| Form | Example | Handled by |
|---|---|---|
| POSIX absolute | `assets:///Users/you/pic.png` | first branch (`path` starts with `/`) |
| Windows drive | `assets:///C:/Users/you/pic.png` | first branch (`^/[a-zA-Z]:`) |
| Windows UNC | `assets://server/share/pic.png` | win32 branch, re-prefixed to `//server/share/…` |

Note the **three** slashes on the absolute form: `assets:` + `//` + empty host + `/Users/…`. That is what `prepend-protocol` → `url-join` emits (`deps/common/src/logseq/common/path.cljs:172`, `:139`), and what the handler's first branch expects.

`assets://` is Electron-only. There is no such scheme in the browser or mobile builds — another reason to prefer the graph-relative form. The other two schemes Logseq registers, and every scheme it consumes, are catalogued in [URI Schemes](../platforms/uri-schemes.md).

> **Use `![…]`, not `[…]`, with `assets://`.** Without the `!`, `show-link?` is false and the renderer emits a plain `<a target="_blank">`. Following that anchor goes through `openExternal`, whose allowlist is `['https:', 'http:', 'mailto:', 'zotero:', 'file:']` (`resources/js/preload.js:9`) — `assets:` is not on it, so the click throws `illegal protocol`.

## Absolute paths and `file://`

Both `file:///Users/…` and a bare `/Users/…` reach the same place, because `string-of-url` strips the protocol off a `file:` link:

```clojure
;; src/main/frontend/components/block.cljs:138
(defn- string-of-url
  [url]
  (match url
    ["File" s]    (string/replace s "file://" "")
    ["Complex" m] (let [{:keys [link protocol]} m]
                    (if (= protocol "file") link (str protocol "://" link)))))
```

**Without `!` — works.** Both fall through to an Electron branch that renders a file icon and an `openPath` handler (`block.cljs:1611` for the `file:` protocol, `block.cljs:1561` via `search-link-cp` for the bare path). `openPath` resolves `~` and calls `shell.openPath` (`resources/js/preload.js:92`), i.e. hands the file to the OS default application. It does not display it in Logseq.

**With `!` — does not render.** Neither is graph-relative, so `image-link` (`block.cljs:652`) skips `<make-asset-url` entirely and the raw path reaches `asset-container`, which prefixes `file://` and nothing else:

```clojure
;; src/main/frontend/components/block.cljs:252
src' (if (and (seq src)
              (or (string/starts-with? src "/")
                  (string/starts-with? src "~")))
       (str "file://" src)
       src)
```

The DOM gets `<img src="file:///Users/…">`. The packaged app loads its page from `lsp://logseq.com/index.html` (`src/electron/electron/window.cljs:25`) with `:webSecurity (not dev?)` — **on** in production (`window.cljs:55`) — so Chromium refuses a `file://` subresource from an `lsp://` origin. The codebase says the same about itself:

```clojure
;; src/main/frontend/components/block.cljs:284
;; Electron renderer cannot fetch file:// URLs; read the
;; file via IPC and copy the blob directly.
```

> Dev builds set `webSecurity` to `false` and serve the app from `http://localhost:3001`, so these two forms may render in development and then fail once packaged. Verify embeds in a packaged build. (This row is traced from source, not observed at runtime.)

**Use `assets:///Users/…` instead** for an absolute path you want displayed inline.

## Remote http(s) URLs

No protocol rewriting happens: the URL passes through `image-link` (`block.cljs:662`, the `starts-with? href "http"` branch) untouched and lands in the `src` attribute. What varies is *which component* renders it, decided by extension in `media-link`:

```clojure
;; src/main/frontend/components/block.cljs:1489
(defn- media-link
  [config url s label metadata full_text]
  (let [ext (keyword (util/get-file-ext s))]
    (cond
      (contains? config/audio-formats ext)          (audio-link …)
      (contains? config/doc-formats ext)            (asset-link …)
      (not (contains? #{:mp4 :webm :mov} ext))      (image-link …)
      :else                                         (asset-reference …))))
```

| URL ends in | Component | Result |
|---|---|---|
| audio format | `audio-link` → `audio-cp` | `<audio controls>` |
| doc format (incl. `pdf`) | `asset-link` | asset viewer — PDF opens in the annotator |
| `mp4` / `webm` / `mov` | `asset-reference` → `asset-video` | `<video>` player |
| anything else | `image-link` | `<img>` |

Remote URLs are subject to the same `!` rule: a bare media URL auto-embeds, `[label](url)` stays a link. Note the asymmetry that follows from `media-formats` excluding `pdf`: a bare `.mp4` URL embeds a player, a bare `.pdf` URL does not.

Remote URLs also do not touch the asset pipeline at all — no file is downloaded, no entity is created, nothing is stored in the graph. If the host goes away, the block breaks.

## Resolution pipeline

```
mldoc "Link" node
   │
   ├─ url = ["Search" s]  ──────────────► search-link-cp   (block.cljs:1512)
   │                                        │
   │                                        ├─ show-link? ──► media-link
   │                                        └─ else       ──► openPath link
   │
   ├─ url = ["Complex" {protocol "file"}] ─► show-link? ? media-link : openPath link
   │
   └─ url = ["Complex" {other protocol}] ──► show-link? ? media-link : external <a>

media-link (block.cljs:1489)  ──► audio-link | asset-link | image-link | asset-reference
                                                              │
image-link (block.cljs:652) ── local-relative-asset? ─ yes ──► asset-link ──► <make-asset-url
                                                    └─ no ───► asset-container (raw src)
```

`<make-asset-url` (`handler/assets.cljs:148`) is the only place a path becomes an `assets://` URL — which is why forms that bypass it (absolute paths, `file://`) do not display.

## Links are not assets

**None of the syntaxes above create an asset entity.** A markdown path is text inside `:block/title` — there is no `:logseq.class/Asset` block, no type, size, or checksum. Consequences:

- **It does not sync.** The checksum-datom listener never fires; see [Sync eligibility](#sync-eligibility). On another device the path resolves to a file that isn't there.
- It does not appear in the asset list, is not deduplicated, and is not counted toward storage.
- It is not backed up with the graph and is not included in graph export.

If you want the file to travel with the graph, drag or paste it in so a real asset block is created, then reference it by `[[uuid]]`. Raw path links are for genuinely external files you deliberately want to leave on one machine.

> Pointing an absolute path at a file *inside* `<graph-dir>/assets/` — e.g. `![t](/Users/you/logseq/graphs/g/assets/<uuid>.png)` — is the worst of both: that file is already a managed asset, and the link bypasses everything that makes it one. Use `[[uuid]]`, or `![t](assets/<uuid>.png)`.

---

# File types

## Every extension is supported

The extension is derived from the filename and stored verbatim:

```clojure
;; deps/db/src/logseq/db/frontend/asset.cljs:21
(defn asset-path->type
  "Create asset type given asset path"
  [path]
  (string/lower-case (.substr (node-path/extname path) 1)))
```

That string becomes `:logseq.property.asset/type` and is never compared against a set of known types. The single check is that it is non-empty:

```clojure
;; src/main/frontend/handler/editor.cljs:1814
ext (when file-name (db-asset/asset-path->type file-name))
_ (when (string/blank? ext)
    (throw (ex-info "File doesn't have a valid ext." {:file-name file-name})))
```

Node's `path.extname` decides, so:

| Filename | Stored type | Accepted? |
|---|---|---|
| `photo.PNG` | `png` (lowercased) | Yes |
| `archive.zip` | `zip` | Yes |
| `backup.tar.gz` | `gz` — only the last segment | Yes |
| `keys.sqlite` | `sqlite` | Yes |
| `Makefile`, `LICENSE` | — | **No** — throws `File doesn't have a valid ext.` |
| `.gitignore` | — | **No** — a leading dot is part of the basename, not an extension |
| `note.` | — | **No** — trailing dot yields an empty extension |

## Every layer, checked

| Layer | Source | Filters by extension? |
|---|---|---|
| File picker | `src/main/frontend/components/editor.cljs:495` | No — the `<input type="file">` has no `accept` attribute |
| Ingest / asset block creation | `src/main/frontend/handler/editor.cljs:1793` `new-asset-block` | Non-blank extension only |
| Sync op generation | `src/main/frontend/worker/sync/asset_db_listener.cljs:20` | No — keys off the `checksum` datom, not the type |
| Client upload | `src/main/frontend/worker/sync/assets.cljs:145` `upload-remote-asset!` | Non-blank only |
| Server `PUT /assets/…` | `deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs:210` | Non-blank only (path must parse) |
| Object storage | `deps/db-sync/src/logseq/db_sync/node/assets.cljs` | None — writes bytes |
| Client download | `src/main/frontend/worker/sync/assets.cljs:351` | Non-blank only |
| Graph import (file → DB) | `deps/graph-parser/script/db_import.cljs:399` | None |

## The one extension list on the server is not a filter

`deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs:12` enumerates nine types, but only to pick a response `Content-Type` when serving a file back:

```clojure
(def ^:private asset-type->content-type
  {"png"  "image/png"   "jpg" "image/jpeg"  "jpeg" "image/jpeg"
   "gif"  "image/gif"   "webp" "image/webp" "bmp"  "image/bmp"
   "svg"  "image/svg+xml" "ico" "image/x-icon" "pdf" "application/pdf"})

(defn- response-content-type [stored-content-type asset-type]
  (or (get asset-type->content-type (string/lower-case asset-type))
      stored-content-type
      "application/octet-stream"))
```

A `.zip` uploads and downloads intact; it simply comes back as `application/octet-stream`.

## Why "images and PDFs only" is a common impression

The format sets govern **rendering**, not sync. They decide whether an asset gets an inline player or a plain file link.

| Set | Members | Source |
|---|---|---|
| `common-config/img-formats` | `gif svg jpeg ico png jpg bmp webp avif cr2` | `deps/common/src/logseq/common/config.cljs:99` |
| `config/image-formats` | `png jpg jpeg bmp gif webp svg heic avif cr2` | `src/main/frontend/config.cljs:177` |
| `config/audio-formats` | `mp3 ogg mpeg wav m4a flac wma aac` | `config.cljs:180` |
| `config/video-formats` | `mp4 webm mov flv avi mkv` | `config.cljs:183` |
| `config/doc-formats` | `doc docx xls xlsx ppt pptx one pdf epub` | `config.cljs:174` |
| `config/media-formats` | `common-config/img-formats` ∪ audio ∪ video | `config.cljs:186` |

The inline-player decision for an asset **block**:

```clojure
;; src/main/frontend/components/block.cljs:1230
(defn- img-audio-video? [block]
  (let [asset-type (some-> (:logseq.property.asset/type block) keyword)]
    (or (contains? (common-config/img-formats) asset-type)
        (contains? config/audio-formats asset-type)
        (contains? config/video-formats asset-type))))
```

Anything outside those sets renders as a file link — and syncs identically to a `.png`.

> **Two image sets, and they disagree.** `common-config/img-formats` has `ico` but not `heic`; `config/image-formats` has `heic` but not `ico`. Rendering and `media-formats` both use the *common* set, so `.heic` never gets an inline preview and is excluded from `media-formats`. `config/image-formats` has one consumer: the file-graph asset-alias extension autocomplete (`components/assets.cljs:30`).

## The genuine extension restrictions (none of them are sync)

| Restriction | Allowed set | Source | Effect |
|---|---|---|---|
| **Mobile share sheet** | `doc-formats` ∪ `media-formats` (33 extensions) | `src/main/frontend/mobile/intent.cljs:139, 193, 241` | Sharing an unlisted type into Logseq shows `:mobile.share/unsupported-import-type` and no asset is created. Note `.heic` is **not** in the set. |
| **Copy image to clipboard** (Electron) | `png jpg jpeg gif webp bmp svg ico` | `src/main/frontend/components/block.cljs:277-289` | Falls back to `:asset/copy-image-unsupported-extension` warning; the asset itself is unaffected. Also offered on inline images with no asset block (`8ffbb22093`); a missing file shows `:asset/missing-file`. Re-checked 2026-10-04 for the missing-file and copy paths, not the extension list. |
| **External link protocols** | `https: http: mailto: zotero: file:` | `resources/js/preload.js:9` | `openExternal` throws `illegal protocol` for anything else, including `assets:`. |
| **Asset alias directories** (file graphs) | user-chosen per directory | `src/main/frontend/components/assets.cljs:104-190` | Legacy file-graph feature; not part of DB sync. |

The mobile share sheet is the only place a file is refused because of its type. Use the in-app file picker or drag-and-drop instead — those have no filter.

---

# Sync eligibility

## What makes a file eligible

An asset syncs when, and only when, a **block entity** exists for it carrying all three required properties.

```clojure
;; deps/db/src/logseq/db/frontend/class.cljs:72-73
:schema {:properties          [:logseq.property.asset/type :logseq.property.asset/size :logseq.property.asset/checksum]
         :required-properties [:logseq.property.asset/type :logseq.property.asset/size :logseq.property.asset/checksum]}
```

| Requirement | Property | Set by |
|---|---|---|
| Tagged `:logseq.class/Asset` | `:block/tags` | `new-asset-block` (`handler/editor.cljs:1793`) |
| Extension | `:logseq.property.asset/type` | `asset-path->type` on the original filename |
| Byte size | `:logseq.property.asset/size` | `File.size` |
| SHA-256 | `:logseq.property.asset/checksum` | `assets-handler/get-file-checksum` |
| File on disk | `assets/<block-uuid>.<type>` | `db-based-write-asset!` |

The upload op is emitted by a DataScript listener watching for the checksum datom:

```clojure
;; src/main/frontend/worker/sync/asset_db_listener.cljs:20
(let [related-datoms (filter (fn [datom]
                               (and (asset-checksum? (:a datom)) (:added datom)))
                             tx-data)]
  (when-let [ops (not-empty (map #(datom=>op db-after %) related-datoms))]
    (client-op/add-asset-ops repo ops)))
```

`datom=>op` additionally requires `ldb/asset?` on the resulting entity (`deps/db/src/logseq/db/frontend/entity_util.cljs:52`). No transaction, no op, no upload.

## The three paths that create an eligible asset

| Path | Entry point |
|---|---|
| Drag and drop into a block | `components/block/drop.cljs:14` → `db-based-save-assets!` |
| Paste | `handler/paste.cljs:276` → `upload-asset!` |
| `/Upload an asset` command and file pickers | `commands.cljs:335`, `components/editor.cljs:495` → `upload-asset!` |

Graph import is a fourth route, but it runs once at import time and reads a file-graph tree, not a live `assets/` directory (`deps/graph-parser/script/db_import.cljs:399`).

## Copying a file into `assets/` does not sync it

It does nothing at all. Three independent reasons:

1. **No transaction is produced.** Writing to the filesystem does not touch DataScript, so `asset_db_listener` never fires.
2. **The remote key is UUID-derived, not name-derived.** Storage keys are `<graph-id>/<asset-uuid>.<type>` (`worker/sync/assets.cljs:35`, `db-sync/…/handler/assets.cljs:193` `parse-asset-path`). A file named `report.pdf` has no block UUID and therefore no addressable key.
3. **Nothing scans the directory.** The only `readdir` over the assets root is `assets-handler/<get-all-assets` (`src/main/frontend/handler/assets.cljs:300`), whose sole non-test caller is graph export to zip (`handler/export.cljs:98`).

Sync moves in the other direction: `download-remote-assets-if-missing!` (`worker/sync/assets.cljs:449`) walks asset **entities** and fetches any whose local file is absent. It never reconciles unknown local files upward.

**To make a loose file sync:** drag it into a block, paste it, or use the file picker. Deleting the graph and re-importing it as a file graph also works, but rewrites every UUID.

## Checksum deduplication

`new-asset-block` looks the checksum up before creating anything:

```clojure
;; src/main/frontend/handler/editor.cljs:1805
existing-asset (some->> checksum (db-async/<get-asset-with-checksum repo))
```

A byte-identical file already in the graph is refused with `:asset/already-exists` ("Asset exists already, title: {1}, node reference: [[{2}]]") and no second asset is created — regardless of the filename. Two different files with the same name are fine; the same file twice is not.

---

# Size limits

Type is unrestricted; size is not.

| Limit | Value | Source |
|---|---|---|
| Ingest (paste/drop/picker) | 100 MB | `handler/assets.cljs:14` `exceed-limit-size?` — shows `:asset/size-too-large`, no asset created |
| Sync op | 100 MiB | `worker/sync/assets.cljs:19,246` — op is **silently dropped**, asset stays local |
| Server `PUT`, plain graph | 100 MiB | `db-sync/…/handler/assets.cljs:9` — `413 asset too large` |
| Server `PUT`, E2EE graph | 200 MiB | `db-sync/…/handler/assets.cljs:10,229` |
| Import | 100 MB | `db_import.cljs:394` — file skipped with a console note |

The client does not know about the 200 MiB E2EE allowance; it refuses at 100 MiB regardless of encryption. Full table in [Sync Limits](sync-limits.md).

# Where assets live

| Platform | Location |
|---|---|
| Desktop, CLI | `<graph-dir>/assets/<uuid>.<ext>` |
| Browser, mobile | lightning-fs / IndexedDB at `memory:///<graph>/assets/` (`worker/platform/browser.cljs:106-111`) — not OPFS, not the native filesystem, so invisible to the Files app and iCloud |
| Server | R2 object `<graph-id>/<uuid>.<ext>`; self-hosted `data/db-sync/assets/` |

Full layout in [Default Paths by Platform](../platforms/default-paths.md).

---

# Summary

| Question | Answer |
|---|---|
| How should I reference a graph file? | `[[uuid]]`, or `![t](assets/<uuid>.ext)` |
| How should I reference an external file? | `![t](assets:///abs/path.ext)` to display it; `[t](/abs/path.ext)` to open it in the OS |
| Why doesn't my `![](file:///…)` show? | The renderer emits `<img src="file://…">`, which the packaged app blocks. Use `assets://`. |
| Why doesn't `[t](https://…/pic.png)` show the image? | `show-link?` requires `!` or a *bare* URL. Labelled links stay links. |
| Why doesn't a bare `.pdf` URL embed? | `pdf` is in `doc-formats`, not `media-formats`. Prefix with `!`. |
| Only images and PDFs sync? | No. Any extension syncs. |
| What is rejected? | Files with no extension; files over 100 MB; byte-identical duplicates. |
| Can I drop a file into `assets/`? | No. Sync reads database entities, never the directory. |
| Do path links sync? | No. A link is text; only asset entities replicate. |
| Is there a per-plan type restriction? | No. `worker/sync/assets.cljs` has no plan gate — see [Sync Limits](sync-limits.md#account-level-limits--claimed-not-enforced). |

# Keeping this page honest

The enumerable facts here — the format sets, the server's content-type map, the external-protocol allowlist — are declared in `dz/support-engineering/script/sync-limits/asset-sync.edn` and checked by:

```
bb dz/support-engineering/script/sync-limits/check_sync_limits.clj \
   --root <logseq-checkout> --declaration asset-sync.edn
```

The link-syntax table is not machine-checkable. Re-derive it by parsing each form with the pinned `mldoc` version and re-reading `show-link?`, `media-link`, and `image-link` in `components/block.cljs`.
