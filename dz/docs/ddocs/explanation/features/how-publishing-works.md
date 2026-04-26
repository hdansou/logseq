---
title: 'How Publishing Works'
description: 'Understanding Logseq\'s current page publishing pipeline and customization hooks'
type: explanation
status: stable
lastVerified: '2026-04-24'
logseqVersion: '0.11.x'
relatedDocs:
  howTo:
    - '[Publish a Page](../../how-to/integrations/publishing.md)'
    - '[Custom CSS](../../how-to/integrations/custom-css.md)'
    - '[Custom JavaScript](../../how-to/integrations/custom-js.md)'
---

> **Last revised:** 2026-04-24

Understanding the current Logseq page publishing system, from page menu action to the public page and its custom theme hooks.

## The Short Version

Logseq's current publishing feature is a **page publishing service**, not a static-site export.

When you publish a page, Logseq:

1. Builds a snapshot of the page and the blocks it needs.
2. Uploads referenced assets.
3. Uploads optional custom publish CSS and JavaScript.
4. Sends the page snapshot to the publish service.
5. Stores the returned public URL on the page as `Published URL`.

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
- Frontend upload handler: [src/main/frontend/handler/publish.cljs](../../../../src/main/frontend/handler/publish.cljs)
- Page snapshot builder: [src/main/frontend/worker/publish.cljs](../../../../src/main/frontend/worker/publish.cljs)
- Public renderer: [deps/publish/src/logseq/publish/render.cljs](../../../../deps/publish/src/logseq/publish/render.cljs)
- Publish worker routes/storage: [deps/publish/src/logseq/publish/routes.cljs](../../../../deps/publish/src/logseq/publish/routes.cljs)

This page explains that system.

### 2. Older static export module

The repository also still has a separate static export-oriented module under [deps/publishing](../../../../deps/publishing/README.md).

That code uses `publishing-public?` and `:publishing/all-pages-public?` to filter a graph for export, for example in [deps/publishing/src/logseq/publishing/db.cljs](../../../../deps/publishing/src/logseq/publishing/db.cljs).

That is a different workflow from the current in-app `Publish page` feature. The two should not be mixed in user docs.

## What the User Triggers

The publish dialog is opened from the page menu in [src/main/frontend/components/page_menu.cljs](../../../../src/main/frontend/components/page_menu.cljs) and also through the `mod+m` shortcut handled by [src/main/frontend/handler/events/ui.cljs](../../../../src/main/frontend/handler/events/ui.cljs).

The dialog lets the user optionally set a password before publishing. That password is sent along with the page snapshot in [src/main/frontend/handler/publish.cljs](../../../../src/main/frontend/handler/publish.cljs).

After a successful publish, Logseq stores the returned public URL in the built-in `Published URL` property defined in [deps/db/src/logseq/db/frontend/property.cljs](../../../../deps/db/src/logseq/db/frontend/property.cljs). That property also exposes the `Unpublish` action in [src/main/frontend/components/property/value.cljs](../../../../src/main/frontend/components/property/value.cljs).

## What Gets Uploaded

The page snapshot is built in [src/main/frontend/worker/publish.cljs](../../../../src/main/frontend/worker/publish.cljs).

The payload is not just the current page title. It includes:

- The page entity itself
- The page's blocks and child blocks
- Linked or embedded blocks that the page depends on
- Reference metadata used for linked references
- Tag metadata used for tag and tagged-node pages
- Searchable block snippets
- The graph UUID and schema version
- A datom snapshot used by the public renderer

The worker tests in [src/test/frontend/worker/publish_test.cljs](../../../../src/test/frontend/worker/publish_test.cljs) verify, for example, that nested embedded blocks are included in the publish payload.

## How Assets Are Handled

The upload coordinator lives in [src/main/frontend/handler/publish.cljs](../../../../src/main/frontend/handler/publish.cljs).

Before posting the page snapshot, Logseq uploads:

- page assets such as images, audio, video, and documents
- responsive image variants for supported image formats
- custom publish assets from `logseq/publish.css` and `logseq/publish.js`

Those custom publish files are declared in `custom-publish-assets` in [src/main/frontend/handler/publish.cljs](../../../../src/main/frontend/handler/publish.cljs). Their SHA-256 hashes are added to publish metadata as `custom_publish_css_hash` and `custom_publish_js_hash`, then read later by the renderer.

## Where Published Data Lives

The current publish worker splits published data across a few stores:

- **R2** stores the transit blob containing the page snapshot.
- **Page Durable Objects** store page metadata.
- **Index Durable Objects** store refs, tagged-node data, and search metadata.

This flow is visible in [deps/publish/src/logseq/publish/routes.cljs](../../../../deps/publish/src/logseq/publish/routes.cljs) and [deps/publish/src/logseq/publish/common.cljs](../../../../deps/publish/src/logseq/publish/common.cljs).

That split makes the system flexible, but it also means publishing is not a single-file export. A published page is assembled from multiple persisted sources.

## How a Public Page Is Rendered

When a visitor opens a published URL, [deps/publish/src/logseq/publish/routes.cljs](../../../../deps/publish/src/logseq/publish/routes.cljs) does the request-level work:

- checks password access when needed
- loads metadata
- loads refs and tagged-node data
- loads the page transit blob
- calls [deps/publish/src/logseq/publish/render.cljs](../../../../deps/publish/src/logseq/publish/render.cljs)

The renderer then:

- rebuilds entities from datoms
- resolves page refs and tags
- renders properties
- renders the nested block tree
- emits full HTML for pages, graph indexes, user pages, tags, refs, password prompts, and 404s

The core page entry point is `render-page-html` in [deps/publish/src/logseq/publish/render.cljs](../../../../deps/publish/src/logseq/publish/render.cljs).

## What Runs in the Browser

The public page is server-rendered first, then enhanced in the browser by [deps/publish/src/logseq/publish/publish.js](../../../../deps/publish/src/logseq/publish/publish.js).

That runtime adds:

- block expand/collapse interactions
- theme toggling and local theme persistence
- graph search UI
- property icon rendering
- KaTeX math rendering
- syntax highlighting and code block editors

The base visual theme lives in [deps/publish/src/logseq/publish/publish.css](../../../../deps/publish/src/logseq/publish/publish.css).

## How Theme Customization Works

The customization contract is intentionally simple:

- Base CSS is loaded from `/static/publish.css`.
- Your optional CSS is loaded afterward from `/asset/:graph/publish.css`.
- Base JavaScript is loaded from `/static/publish.js`.
- Your optional JavaScript is loaded afterward from `/asset/:graph/publish.js`.

You can see the CSS and JS injection points in [deps/publish/src/logseq/publish/render.cljs](../../../../deps/publish/src/logseq/publish/render.cljs).

That load order matters:

- Custom CSS wins by normal cascade order because it is linked after the built-in stylesheet.
- Custom JS runs after the standard publish runtime has already registered its behavior.

In practice this means you can:

- override the publish theme tokens in `:root` and `[data-theme="dark"]`
- restyle structural selectors such as `.wrap`, `.page-toolbar`, `.page-meta`, `.blocks`, `.block-content`, `.page-properties`, `.linked-refs`, and `.tagged-pages`
- add your own DOM behavior once the page is already rendered

## The Main Design Surface

The base stylesheet in [deps/publish/src/logseq/publish/publish.css](../../../../deps/publish/src/logseq/publish/publish.css) exposes a set of useful custom properties, including:

- `--bg`
- `--surface`
- `--ink`
- `--muted`
- `--border`
- `--action`
- `--code-bg`
- `--math-bg`

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

Because the built-in publish runtime runs first, your `logseq/publish.js` can safely assume that the page HTML already exists and that helper behaviors such as `window.toggleTheme()` from [deps/publish/src/logseq/publish/publish.js](../../../../deps/publish/src/logseq/publish/publish.js) have already been defined.

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
  - transit blob in R2
  - page metadata in page DO
  - refs/tags/search in index DO
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
