---
title: 'How Publishing Works'
description: 'Understanding Logseq\'s current page publishing pipeline and customization hooks'
type: explanation
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.x'
relatedDocs:
  howTo:
    - '[Publish a Page](../../how-to/integrations/publishing.md)'
    - '[Custom CSS](../../how-to/integrations/custom-css.md)'
    - '[Custom JavaScript](../../how-to/integrations/custom-js.md)'
---

> **Last revised:** 2026-09-09 (verified against `be800f1711`)

Understanding the current Logseq page publishing system, from page menu action to the public page and its custom theme hooks.

## The Short Version

Logseq's current publishing feature is a **page publishing service**, not a static-site export.

When you publish a page, Logseq:

1. Builds a snapshot of the page and the blocks it needs.
2. Uploads referenced assets.
3. Uploads optional custom publish CSS and JavaScript.
4. Sends the page snapshot to the publish service.
5. Stores the returned public URL on the page as `Published URL` (prefers the returned `short_url` when available, falling back to `/page/:graph-uuid/:page-uuid` — [handler/publish.cljs:387-397](../../../../../src/main/frontend/handler/publish.cljs#L387-L397)).

When someone opens that URL, the publish worker:

1. Loads the page snapshot and metadata.
2. Renders HTML on the server.
3. Enhances the page in the browser with search, theme switching, code highlighting, math rendering, and block toggles.

## Two Different Publishing Systems in This Repository

This repository currently contains **two different publishing-related code paths**.

### 1. Current page publishing feature

This is the workflow users see in today's UI:

- Page menu action: `Publish page`
- Keyboard shortcut: `mod+m`
- Frontend upload handler: [src/main/frontend/handler/publish.cljs](../../../../../src/main/frontend/handler/publish.cljs)
- Page snapshot builder: [src/main/frontend/worker/publish.cljs](../../../../../src/main/frontend/worker/publish.cljs)
- Public renderer: [deps/publish/src/logseq/publish/render.cljs](../../../../../deps/publish/src/logseq/publish/render.cljs)
- Publish worker routes/storage: [deps/publish/src/logseq/publish/routes.cljs](../../../../../deps/publish/src/logseq/publish/routes.cljs)

This page explains that system.

### 2. Older static export module

The repository also still has a separate static export-oriented module under [deps/publishing](../../../../../deps/publishing/README.md).

That code filters a graph for export on the page property `:logseq.property/publishing-public?` ([deps/publishing/src/logseq/publishing/db.cljs:17](../../../../../deps/publishing/src/logseq/publishing/db.cljs#L17), [:39](../../../../../deps/publishing/src/logseq/publishing/db.cljs#L39)), with the repo-config key `:publishing/all-pages-public?` deciding which way the default runs ([deps/publishing/src/logseq/publishing/html.cljs:141](../../../../../deps/publishing/src/logseq/publishing/html.cljs#L141)).

That is a different workflow from the current in-app `Publish page` feature. The two should not be mixed in user docs.

## What the User Triggers

The publish dialog is `publish-page-dialog` in [src/main/frontend/components/page_menu.cljs:27-46](../../../../../src/main/frontend/components/page_menu.cljs#L27-L46), reachable from the page menu and also through the `mod+m` shortcut defined in [src/main/frontend/modules/shortcut/config.cljs:380-382](../../../../../src/main/frontend/modules/shortcut/config.cljs#L380-L382), which publishes `:publish/open-dialog`, handled by `defevent!` in [src/main/frontend/handler/events/ui.cljs:143](../../../../../src/main/frontend/handler/events/ui.cljs#L143).

> **Removed:** The previous claim that the shortcut binding itself is handled in `src/main/frontend/handler/events/ui.cljs` is stale. That namespace handles the event, but the keybinding is defined in shortcut config.

The dialog lets the user optionally set a password before publishing. It is trimmed, dropped if blank, and sent as `:page-password` in the metadata alongside the page snapshot ([handler/publish.cljs:324-335](../../../../../src/main/frontend/handler/publish.cljs#L324-L335)). The worker never stores it — it stores a PBKDF2 hash (see [Findings](#findings-new-capabilities)).

After a successful publish, Logseq stores the returned public URL in the built-in property `:logseq.property.publish/published-url`, titled `Published URL`, defined in [deps/db/src/logseq/db/frontend/property.cljs:420-424](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L420-L424). It is a `:url` property with `:view-context :page` and `:public? true`. That property also exposes the `Unpublish` action in [src/main/frontend/components/property/value.cljs:1642-1643](../../../../../src/main/frontend/components/property/value.cljs#L1642-L1643).

## What Gets Uploaded

The page snapshot is built in [src/main/frontend/worker/publish.cljs](../../../../../src/main/frontend/worker/publish.cljs).

The payload is not just the current page title. `build-publish-page-payload` ([worker/publish.cljs:212-240](../../../../../src/main/frontend/worker/publish.cljs#L212-L240)) returns a map with these keys:

| Key | Contents |
| --- | --- |
| `:page` | the page entity as a map |
| `:page-uuid`, `:page-title` | identity of the published page |
| `:graph-uuid` | the RTC graph UUID, or the local graph UUID if there is none |
| `:block-count` | number of collected blocks |
| `:schema-version` | the DB schema version string |
| `:refs` | reference metadata used for linked references |
| `:page-tags` | tag metadata used for tag and tagged-node pages |
| `:blocks` | searchable block snippets |
| `:datoms` | the datom snapshot the public renderer rebuilds entities from |

Two attributes are stripped from the datom snapshot before upload: `:block/tx-id` and `:logseq.property.user/email` ([worker/publish.cljs:226-227](../../../../../src/main/frontend/worker/publish.cljs#L226-L227)).

The worker tests in [src/test/frontend/worker/publish_test.cljs](../../../../../src/test/frontend/worker/publish_test.cljs) verify, for example, that nested embedded blocks are included in the publish payload.

## How Assets Are Handled

The upload coordinator lives in [src/main/frontend/handler/publish.cljs](../../../../../src/main/frontend/handler/publish.cljs).

Before posting the page snapshot, Logseq uploads:

- page assets such as images, audio, video, and documents
- responsive image variants at widths `1024` and `1600`, at quality `0.9`, for `png`, `jpg`, `jpeg` and `webp` only ([handler/publish.cljs:69-76](../../../../../src/main/frontend/handler/publish.cljs#L69-L76)); a variant is stored under the asset UUID with an `@<width>` suffix ([handler/publish.cljs:94-98](../../../../../src/main/frontend/handler/publish.cljs#L94-L98))
- custom publish assets from `logseq/publish.css` and `logseq/publish.js`

Those custom publish files are declared in `custom-publish-assets` in [handler/publish.cljs:78-88](../../../../../src/main/frontend/handler/publish.cljs#L78-L88). Their SHA-256 hashes are added to publish metadata as `custom_publish_css_hash` and `custom_publish_js_hash` ([handler/publish.cljs:352-355](../../../../../src/main/frontend/handler/publish.cljs#L352-L355)), then read later by the renderer and used as the `?v=` cache-busting value on the generated `<link>` and `<script>` tags.

## Where Published Data Lives

The current publish worker splits published data across two Cloudflare bindings, declared in [deps/publish/worker/wrangler.toml](../../../../../deps/publish/worker/wrangler.toml):

- **`PUBLISH_R2`** — an R2 bucket holding the transit blob for each page under the key `publish/<graph-uuid>/<content-hash>.transit`, and each uploaded asset under `publish/assets/<graph-uuid>/<asset-uuid>.<ext>` ([routes.cljs:107](../../../../../deps/publish/src/logseq/publish/routes.cljs#L107), [:707](../../../../../deps/publish/src/logseq/publish/routes.cljs#L707)).
- **`PUBLISH_META_DO`** — a *single* Durable Object class, `PublishMetaDO`. There is only one class and one binding; the worker addresses different **instances** of it by name:
  - `idFromName "index"` — the shared index instance, which holds short-link mappings, per-graph page lists, refs, tagged-node data, search, and user pages ([routes.cljs:163](../../../../../deps/publish/src/logseq/publish/routes.cljs#L163), [:284](../../../../../deps/publish/src/logseq/publish/routes.cljs#L284), [:340](../../../../../deps/publish/src/logseq/publish/routes.cljs#L340)).
  - `idFromName "<graph-uuid>:<page-uuid>"` — one instance per published page, holding that page's metadata including the password hash ([routes.cljs:217](../../../../../deps/publish/src/logseq/publish/routes.cljs#L217), [:243](../../../../../deps/publish/src/logseq/publish/routes.cljs#L243)).

> **Corrected:** the previous revision described "Page Durable Objects" and "Index Durable Objects" as two different Durable Object types. They are two naming conventions over the same `PublishMetaDO` class.

That split makes the system flexible, but it also means publishing is not a single-file export. A published page is assembled from multiple persisted sources.

## How a Public Page Is Rendered

When a visitor opens a published URL, [deps/publish/src/logseq/publish/routes.cljs](../../../../../deps/publish/src/logseq/publish/routes.cljs) does the request-level work:

- checks password access when needed
- loads metadata
- loads refs and tagged-node data
- loads the page transit blob
- calls [deps/publish/src/logseq/publish/render.cljs](../../../../../deps/publish/src/logseq/publish/render.cljs)

If the page is not published, the worker renders a dedicated not-published page. If password-protected, it renders a password prompt page first.

The renderer then:

- rebuilds entities from datoms (`datoms->entities` in [deps/publish/src/logseq/publish/model.cljs:13](../../../../../deps/publish/src/logseq/publish/model.cljs#L13))
- resolves page refs and tags
- renders properties
- renders the nested block tree
- emits full HTML for pages, graph indexes, user pages, tags, refs, password prompts, and 404s

The core page entry point is `render-page-html` at [render.cljs:1099](../../../../../deps/publish/src/logseq/publish/render.cljs#L1099).

### The routes the worker actually serves

The dispatch table is one `cond` in `handle-fetch` ([routes.cljs:616-784](../../../../../deps/publish/src/logseq/publish/routes.cljs#L616-L784)). There is no router library and no path parameters — every route is a prefix match on `.pathname`:

| Path | Method | Result |
| --- | --- | --- |
| `/` | GET | home page |
| `/page/<graph-uuid>/<page-uuid>` | GET | the canonical published page |
| `/p/<short-id>`, `/s/<short-id>` | GET | look up the short id in the index DO, then internally rewrite to `/page/<graph-uuid>/<page-uuid>` and render it — no HTTP redirect |
| `/graph/<graph-uuid>` | GET | graph index; `/graph/<graph-uuid>/json` returns JSON instead |
| `/tag/<name>` and `/ref/<name>` | GET | tag and ref pages; both take a `/json` suffix |
| `/u/<username>` | GET | that user's published pages |
| `/asset/<graph-uuid>/<asset-uuid>.<ext>` | GET | streams the object out of R2 |
| `/search/<graph-uuid>?q=…` | GET | graph search, proxied to the index DO |
| `/static/publish.css`, `/static/publish.js`, `/static/tabler.ext.js` | GET | the built-in assets, inlined into the worker bundle |
| `/pages` | POST | publish a page |
| `/pages/…` | GET / DELETE | page metadata, transit, refs, tagged nodes; delete page or graph |
| `/assets` | POST | asset upload |
| anything else | — | the 404 page |

Note the URL shape: both segments of a page URL are **UUIDs**, not slugs. Page titles never appear in the canonical URL.

## What Runs in the Browser

The public page is server-rendered first, then enhanced in the browser by [deps/publish/src/logseq/publish/publish.js](../../../../../deps/publish/src/logseq/publish/publish.js).

That runtime adds:

- block expand/collapse interactions
- theme toggling and persistence in `localStorage` under the key `publish-theme` (the initial theme is applied by a tiny inline script in `<head>` before paint, [render.cljs:169-172](../../../../../deps/publish/src/logseq/publish/render.cljs#L169-L172))
- graph search UI, opened with `Cmd/Ctrl+K`, backed by `GET /search/<graph-uuid>?q=…` ([publish.js:282-299](../../../../../deps/publish/src/logseq/publish/publish.js#L282-L299), [:580](../../../../../deps/publish/src/logseq/publish/publish.js#L580))
- property icon rendering, on top of a minimal hand-rolled `window.React` shim injected inline by the renderer ([render.cljs:178-181](../../../../../deps/publish/src/logseq/publish/render.cljs#L178-L181)) so Tabler's React icon package can run without React
- KaTeX math rendering
- CodeMirror-based code blocks

It is an **ES module** (`<script type="module">`), and it imports KaTeX and CodeMirror from `https://esm.sh` at runtime ([publish.js:1-26](../../../../../deps/publish/src/logseq/publish/publish.js#L1-L26)). A published page therefore depends on third-party CDNs — `esm.sh`, `cdn.jsdelivr.net` for the Tabler icon webfont ([render.cljs:242-243](../../../../../deps/publish/src/logseq/publish/render.cljs#L242-L243)), `fonts.googleapis.com` for Inter and JetBrains Mono ([publish.css:2](../../../../../deps/publish/src/logseq/publish/publish.css#L2)) — and is not fully self-hosted.

The base visual theme lives in [deps/publish/src/logseq/publish/publish.css](../../../../../deps/publish/src/logseq/publish/publish.css).

## How Theme Customization Works

The customization contract is intentionally simple:

- Base CSS is linked in `<head>` from `/static/publish.css?v=<build-timestamp>` ([render.cljs:245](../../../../../deps/publish/src/logseq/publish/render.cljs#L245)).
- Your optional CSS is linked immediately afterward from `/asset/<graph-uuid>/publish.css?v=<sha256>` — and only when the page's metadata carries a `custom_publish_css_hash` ([render.cljs:217-219](../../../../../deps/publish/src/logseq/publish/render.cljs#L217-L219), emitted at [:246](../../../../../deps/publish/src/logseq/publish/render.cljs#L246)).
- Base JavaScript is loaded at the **end of `<body>`** as `<script type="module" src="/static/publish.js?v=…">` ([render.cljs:174-176](../../../../../deps/publish/src/logseq/publish/render.cljs#L174-L176), placed at [:1256](../../../../../deps/publish/src/logseq/publish/render.cljs#L1256)).
- Your optional JavaScript follows it as `<script defer src="/asset/<graph-uuid>/publish.js?v=<sha256>">`, again only when `custom_publish_js_hash` is present ([render.cljs:1257-1259](../../../../../deps/publish/src/logseq/publish/render.cljs#L1257-L1259)).

The `?v=` values differ in kind: static assets use a hard-coded build timestamp (`version`, [render.cljs:14](../../../../../deps/publish/src/logseq/publish/render.cljs#L14)), while your files use the SHA-256 of their own content, so a re-publish of a changed `publish.css` invalidates the cache automatically.

That load order matters:

- Custom CSS wins by normal cascade order because it is linked after the built-in stylesheet.
- Custom JS runs after the standard publish runtime, because a non-async module script and a `defer` classic script both execute after parsing, in document order — and the module tag comes first.

In practice this means you can:

- override the publish theme tokens in `:root` and `[data-theme="dark"]`
- restyle structural selectors such as `.wrap`, `.page-toolbar`, `.page-meta`, `.blocks`, `.block-content`, `.page-properties`, `.linked-refs`, and `.tagged-pages`
- add your own DOM behavior once the page is already rendered

## The Main Design Surface

The base stylesheet in [deps/publish/src/logseq/publish/publish.css](../../../../../deps/publish/src/logseq/publish/publish.css) exposes a set of useful custom properties, including:

- `--bg`
- `--surface`
- `--ink`
- `--muted`
- `--border`
- `--action`
- `--code-bg`
- `--math-bg`

All of these are defined twice: once on `:root` for the light theme ([publish.css:4-30](../../../../../deps/publish/src/logseq/publish/publish.css#L4-L30)) and once on `[data-theme="dark"]` ([publish.css:32-56](../../../../../deps/publish/src/logseq/publish/publish.css#L32-L56)). Dark mode is driven purely by the `data-theme` attribute on `<html>`, never by `prefers-color-scheme` in CSS — the media query is only consulted once, by the inline boot script, to pick the initial attribute value. If you override a token, override it in both places.

These are the fastest way to restyle the published theme without rewriting the whole stylesheet.

The main structural selectors worth targeting are:

- `.wrap`
- `.page-toolbar`
- `.page-meta`
- `.page-properties`
- `.properties`
- `.blocks`
- `.block-content`
- `.linked-refs`
- `.tagged-pages`

## What Custom Publish JavaScript Can Rely On

Because the built-in publish runtime runs first, your `logseq/publish.js` can assume the page HTML already exists.

But the built-in runtime is an ES module, so its top-level bindings are **not** globals. Only what it explicitly assigns to `window` is reachable from your script. At `be800f1711` that is exactly two functions:

- `window.toggleTheme()` ([publish.js:339](../../../../../deps/publish/src/logseq/publish/publish.js#L339))
- `window.toggleTopBlocks(btn)` ([publish.js:307](../../../../../deps/publish/src/logseq/publish/publish.js#L307))

Treat everything else in `publish.js` as private. There is no documented, stable extension API beyond those two functions and the DOM.

Your custom publish JavaScript should treat the public page as a normal web page:

- use DOM APIs
- use browser APIs such as `fetch`, `localStorage`, `MutationObserver`, and event listeners
- avoid assuming the Logseq plugin SDK exists on the public page

The current public renderer is intentionally thin. It is not the same environment as the main Logseq app.

## Flow Diagram

```text
User clicks Publish page
        |
        v
Publish dialog collects optional password
        |
        v
frontend.handler.publish/publish-page!
        |
        +--> build page payload in DB worker
        +--> upload assets
        +--> upload logseq/publish.css and logseq/publish.js
        +--> POST transit payload + metadata
        |
        v
Publish worker stores:
  - transit blob in R2 (PUBLISH_R2)
  - page metadata in PublishMetaDO "<graph>:<page>"
  - refs/tags/search in PublishMetaDO "index"
        |
        v
Visitor opens public URL
        |
        v
deps/publish/render.cljs renders HTML
        |
        +--> base publish.css
        +--> custom publish.css
        +--> base publish.js
        +--> custom publish.js
        |
        v
Public page with your design overrides
```

## Findings: New Capabilities

Compared with this doc's previous revision, current source shows these notable capabilities:

- Short-link entry points are supported via both `/p/<short-id>` and `/s/<short-id>`, both resolved through the index DO and then rendered as the canonical `/page/<graph-uuid>/<page-uuid>` page by an internal request rewrite ([routes.cljs:719-741](../../../../../deps/publish/src/logseq/publish/routes.cljs#L719-L741)).
- Public graph search is wired end-to-end: worker endpoint `/search/<graph-uuid>?q=...` ([routes.cljs:332-343](../../../../../deps/publish/src/logseq/publish/routes.cljs#L332-L343)) plus browser UI and the `Cmd/Ctrl+K` shortcut in `publish.js`.
- Publish payload generation explicitly excludes the comments area and comment blocks — instances of `:logseq.class/Comments` and `:logseq.class/Comment`, plus any direct child of a comments area — from exported page content ([worker/publish.cljs:37-52](../../../../../src/main/frontend/worker/publish.cljs#L37-L52)).
- Image publishing includes responsive variant generation (`@1024`, `@1600`) for `png`/`jpg`/`jpeg`/`webp`, with a SHA-256 checksum per upload ([handler/publish.cljs:69-76](../../../../../src/main/frontend/handler/publish.cljs#L69-L76), [:100-112](../../../../../src/main/frontend/handler/publish.cljs#L100-L112)).
- Page password storage on the publish worker is hash-based, not plaintext: `pbkdf2$sha256$90000$<base64url-salt>$<base64url-hash>`, with a 16-byte random salt, SHA-256, 256 derived bits, and iterations clamped to `password-kdf-max-iterations` ([common.cljs:124-125](../../../../../deps/publish/src/logseq/publish/common.cljs#L124-L125), [:135-160](../../../../../deps/publish/src/logseq/publish/common.cljs#L135-L160)). Verification rejects any stored hash claiming more than 90,000 iterations rather than honouring it ([common.cljs:175-184](../../../../../deps/publish/src/logseq/publish/common.cljs#L175-L184)).

## A Bug Worth Knowing About

Every rendered page's `<head>` links `/static/tabler-extension.css?v=<version>` ([render.cljs:244](../../../../../deps/publish/src/logseq/publish/render.cljs#L244)), but the worker has no route for that path — the resource inline is commented out ([routes.cljs:18](../../../../../deps/publish/src/logseq/publish/routes.cljs#L18)) and no branch in `handle-fetch` matches it. The request therefore falls through to the `:else` branch, which returns the 404 **HTML** page with `content-type: text/html` and, because that branch sets no `:status`, an HTTP **200**. Sibling static routes such as `/static/tabler.ext.js` are served correctly and are covered by [routes_test.cljs:155](../../../../../deps/publish/test/logseq/publish/routes_test.cljs#L155); this one is not.

This is a Logseq defect rather than a documentation defect. It is cosmetic in effect (the stylesheet was optional) but every published page makes a wasted request that looks successful.

## The Practical Mental Model

If you are changing the published design, think of the system like this:

- `frontend.handler.publish` decides what to upload.
- `frontend.worker.publish` decides what content belongs to the page snapshot.
- `deps/publish/render.cljs` decides what HTML structure exists.
- `deps/publish/publish.css` defines the default design tokens and layout.
- `logseq/publish.css` is your override layer.
- `deps/publish/publish.js` defines the default client runtime.
- `logseq/publish.js` is your enhancement layer.

If you keep those boundaries clear, the feature is straightforward to reason about.
