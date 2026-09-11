---
title: 'Publish a Page'
description: 'How to publish a page to the web and customize the published design with CSS and JavaScript'
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop, web]
graphType: [db]
relatedDocs:
  explanation:
    - '[How Publishing Works](../../explanation/features/how-publishing-works.md)'
  howTo:
    - '[Custom CSS](custom-css.md)'
    - '[Custom JavaScript](custom-js.md)'
---

> **Last revised:** 2026-09-11

How to publish a page to the web, protect it with an optional password, and customize its public design with `logseq/publish.css` and `logseq/publish.js`.

## Before You Start

There are two unrelated ways to get pages onto the web. Don't mix them up:

- **Publish page** (most of this guide) uploads **one page** to a publish server: Logseq Publish at `https://logseq.io` by default ([config.cljs:33](../../../../../src/main/frontend/config.cljs#L33)), or your own server.
- **Export public pages** (see [the last section](#alternative-export-public-pages-as-a-static-site)) writes a static HTML site to a folder. It is Electron-only and uses the older `deps/publishing` code.

You will need:

- a page in a DB graph
- to be **signed in**. The publish server rejects uploads without a valid Logseq token ([routes.cljs:64-72](../../../../../deps/publish/src/logseq/publish/routes.cljs#L64-L72)). When you're signed out the app simply omits the token ([publish.cljs:323-327](../../../../../src/main/frontend/handler/publish.cljs#L323-L327)), so you only see **Publish failed.**
- optionally, `logseq/publish.css` and `logseq/publish.js` records in the graph (see Step 4)

The graph does not need to be synced. Publishing uses the graph's sync UUID if it has one, and otherwise its local graph UUID ([graph.cljs:159-163](../../../../../src/main/frontend/worker/handler/graph.cljs#L159-L163)).

## Findings: New Capabilities

- The publish dialog is in the header's **⋯** menu on any page and also has a keyboard shortcut ([header.cljs:190-203](../../../../../src/main/frontend/components/header.cljs#L190-L203), [page_menu.cljs:143-146](../../../../../src/main/frontend/components/page_menu.cljs#L143-L146), [shortcut config.cljs:380-382](../../../../../src/main/frontend/modules/shortcut/config.cljs#L380-L382))
- You can point the app at a self-hosted publish server (see [Use a Self-Hosted Publish Server](#use-a-self-hosted-publish-server))

## Step 1: Open the Publish Dialog

You can open the publish dialog in either of these ways:

1. Open the page.
2. Open the **⋯** menu at the top right of the header.
3. Select **Publish page** ([en.edn:1593](../../../../../src/resources/dicts/en.edn#L1593)).

Or use the keyboard shortcut. It only acts when the current view is a page ([events/ui.cljs:143-150](../../../../../src/main/frontend/handler/events/ui.cljs#L143-L150)):

- macOS: `Cmd+M`
- Windows/Linux: `Ctrl+M`

The dialog is titled **Publish page** and reads "Optionally protect this page with a password. Leave empty for public access." It has an **Optional password** field and **Cancel** / **Publish** buttons ([page_menu.cljs:27-62](../../../../../src/main/frontend/components/page_menu.cljs#L27-L62); [en.edn:1591-1595](../../../../../src/resources/dicts/en.edn#L1591-L1595)).

## Step 2: Publish the Page

1. Enter a password if you want the page to require one.
2. Leave the password empty if the page should be public. A blank or whitespace-only password is ignored ([publish.cljs:328-333](../../../../../src/main/frontend/handler/publish.cljs#L328-L333)).
3. Click **Publish**. The button shows **Publishing...** while it works.

After a successful publish, Logseq ([publish.cljs:369-411](../../../../../src/main/frontend/handler/publish.cljs#L369-L411)):

- uploads the assets referenced by the page ([publish.cljs:290-298](../../../../../src/main/frontend/handler/publish.cljs#L290-L298))
- uploads `logseq/publish.css` and `logseq/publish.js` if they exist and aren't blank ([publish.cljs:300-321](../../../../../src/main/frontend/handler/publish.cljs#L300-L321))
- posts the page snapshot ([publish.cljs:323-367](../../../../../src/main/frontend/handler/publish.cljs#L323-L367))
- writes the returned URL to the page's **Published URL** property and shows **Published to: <url>** ([publish.cljs:395-398](../../../../../src/main/frontend/handler/publish.cljs#L395-L398); [en.edn:1493](../../../../../src/resources/dicts/en.edn#L1493), [:1597](../../../../../src/resources/dicts/en.edn#L1597))

## Step 3: Open or Unpublish the Result

Once the page is published, look at the page properties.

You will see the built-in **Published URL** property with ([value.cljs:1629-1643](../../../../../src/main/frontend/components/property/value.cljs#L1629-L1643)):

- the public URL as a link
- an **Unpublish** button

Clicking **Unpublish** deletes the page from the publish server, removes the property, and shows **Unpublished.** ([publish.cljs:416-440](../../../../../src/main/frontend/handler/publish.cljs#L416-L440)). The server also requires you to be signed in to unpublish ([routes.cljs:451-461](../../../../../deps/publish/src/logseq/publish/routes.cljs#L451-L461)).

## Step 4: Customize the Published Theme with CSS

`logseq/publish.css` is a file **record inside the DB graph**, not a file on disk. Creating it takes a detour:

- New graphs only contain `logseq/config.edn`, `logseq/custom.css` and `logseq/custom.js` ([create_graph.cljs:205-220](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs#L205-L220))
- Settings only has edit entries for `config.edn`, `custom.css` and (on Electron) `export.css` ([settings.cljs:884-886](../../../../../src/main/frontend/components/settings.cljs#L884-L886))
- The in-app file editor only saves files that already exist in the graph ([code.cljs:21-35](../../../../../src/main/frontend/handler/code.cljs#L21-L35))

To create or replace it, call the plugin API from a plugin:

```javascript
await logseq.DB.setFileContent('logseq/publish.css', cssText)
```

`setFileContent` is declared on `IDBProxy` ([LSPlugin.ts:1003](../../../../../libs/src/LSPlugin.ts#L1003)). Its host implementation accepts only `logseq/custom.css`, `logseq/custom.js`, `logseq/publish.css` and `logseq/publish.js` ([api/db.cljs:53-74](../../../../../src/main/logseq/api/db.cljs#L53-L74)).

When you publish, Logseq uploads this record. The page links it **after** the built-in publish stylesheet ([render.cljs:244-246](../../../../../deps/publish/src/logseq/publish/render.cljs#L244-L246)), so your rules win by normal cascade order.

### Start with theme variables

The fastest way to change the published design is to override the built-in custom properties. The built-in theme defines `--bg`, `--bg-accent`, `--surface`, `--surface-strong`, `--ink`, `--muted`, `--border`, `--link`, `--action`, `--shadow`, `--button-bg`, `--button-bg-hover`, `--code-bg`, `--code-ink`, `--code-muted`, `--math-bg`, `--quote-border`, `--card-bg`, `--input-bg`, `--image-shadow`, `--icon-day` and `--icon-night`. It redefines them under `[data-theme="dark"]` ([publish.css:6-32](../../../../../deps/publish/src/logseq/publish/publish.css#L6-L32)).

```css
:root {
  --bg: #f6f3ea;
  --surface: #fffaf0;
  --ink: #1f1d1b;
  --muted: #6d645b;
  --border: #d8cbb8;
  --action: #c04c2e;
  --code-bg: #1e1d22;
}

[data-theme='dark'] {
  --bg: #111111;
  --surface: #1a1a1a;
  --ink: #f0ede8;
  --muted: #b7aea4;
  --border: #3c3935;
  --action: #f08a5d;
}
```

### Then target page structure

Useful selectors from the built-in publish theme ([publish.css](../../../../../deps/publish/src/logseq/publish/publish.css)) include:

- `.wrap`
- `.page-toolbar`
- `.page-meta`
- `.page-properties`
- `.properties`
- `.blocks`
- `.block-content`
- `.linked-refs`
- `.tagged-pages`

Example:

```css
.wrap {
  max-width: 72rem;
}

.page-toolbar {
  position: sticky;
  top: 0;
  backdrop-filter: blur(10px);
}

.linked-refs,
.tagged-pages {
  border-top: 2px solid var(--border);
  padding-top: 2rem;
}
```

## Step 5: Add Custom Behavior with JavaScript

Create `logseq/publish.js` the same way as the CSS record:

```javascript
await logseq.DB.setFileContent('logseq/publish.js', jsText)
```

The built-in runtime is loaded as `<script type="module" src="/static/publish.js">` at the end of `<body>`. Your file follows it as a `defer` script ([render.cljs:174-176](../../../../../deps/publish/src/logseq/publish/render.cljs#L174-L176), [:1257-1259](../../../../../deps/publish/src/logseq/publish/render.cljs#L1257-L1259)), so it runs after the built-in runtime. Use it for small public-site behaviors such as banners, button actions, or layout tweaks that are easier in JavaScript than CSS.

Example:

```javascript
document.addEventListener('DOMContentLoaded', () => {
  const meta = document.querySelector('.page-meta')
  if (!meta) return

  const badge = document.createElement('span')
  badge.className = 'publish-badge'
  badge.textContent = 'Public page'
  meta.appendChild(badge)
})
```

With matching CSS in `logseq/publish.css`:

```css
.publish-badge {
  display: inline-flex;
  align-items: center;
  padding: 0.25rem 0.6rem;
  border-radius: 999px;
  background: var(--button-bg);
  color: var(--ink);
  font-size: 0.85rem;
}
```

### What your custom publish JavaScript can rely on

By the time your script runs, the published page already has:

- its HTML rendered (the server returns full HTML)
- the theme toggle: a click handler on `.theme-toggle` and a global `window.toggleTheme()` ([publish.js:264-270](../../../../../deps/publish/src/logseq/publish/publish.js#L264-L270), [:323-343](../../../../../deps/publish/src/logseq/publish/publish.js#L323-L343))
- block collapse handlers on `.block-toggle` ([publish.js:30-37](../../../../../deps/publish/src/logseq/publish/publish.js#L30-L37))
- the search UI, KaTeX rendering of `.math-block`, and read-only CodeMirror views for `.code-block` ([publish.js:652-712](../../../../../deps/publish/src/logseq/publish/publish.js#L652-L712))

The built-in runtime is an ES module, so its other functions are not globals. Use normal browser APIs. Do not assume the Logseq plugin SDK is available on the public page.

## Step 6: Republish After Design Changes

Changes to `logseq/publish.css` and `logseq/publish.js` are uploaded only when you publish.

After editing either record:

1. return to the page
2. run **Publish page** again
3. reload the public page

The custom files are linked with a content hash (`?v=<sha256>`, [render.cljs:217-219](../../../../../deps/publish/src/logseq/publish/render.cljs#L217-L219), [:1259](../../../../../deps/publish/src/logseq/publish/render.cljs#L1259)), so a changed file gets a new URL. The page HTML, however, is served with `cache-control: public, max-age=300, must-revalidate` ([routes.cljs:593](../../../../../deps/publish/src/logseq/publish/routes.cljs#L593), [:601](../../../../../deps/publish/src/logseq/publish/routes.cljs#L601)). If the old design still appears, wait up to five minutes or do a hard refresh.

To remove your custom CSS or JS, make the record blank and republish. Blank files aren't uploaded, and the page only links a custom file when a hash was recorded ([publish.cljs:307](../../../../../src/main/frontend/handler/publish.cljs#L307), [:351-355](../../../../../src/main/frontend/handler/publish.cljs#L351-L355)).

## Common Patterns

### Make the page feel more like a blog article

```css
.wrap {
  max-width: 46rem;
}

.block-text {
  font-size: 1.08rem;
}

.page-meta {
  margin-bottom: 2rem;
}
```

### Restyle the toolbar buttons

```css
.toolbar-btn,
.publish-search-toggle,
.theme-toggle {
  border-radius: 12px;
}
```

### Change dark mode independently from light mode

```css
[data-theme='light'] .wrap {
  background: rgba(255, 255, 255, 0.5);
}

[data-theme='dark'] .wrap {
  background: rgba(20, 20, 20, 0.75);
}
```

## Use a Self-Hosted Publish Server

**Settings → Advanced → Publish Server URL** lets you replace Logseq Publish ([settings.cljs:761-764](../../../../../src/main/frontend/components/settings.cljs#L761-L764), [:928](../../../../../src/main/frontend/components/settings.cljs#L928); [en.edn:1777-1782](../../../../../src/resources/dicts/en.edn#L1777-L1782)):

- The value is stored in browser `localStorage`, not in the graph ([config.cljs:102-117](../../../../../src/main/frontend/config.cljs#L102-L117))
- It must start with `http://` or `https://`, and trailing slashes are stripped ([config.cljs:119-128](../../../../../src/main/frontend/config.cljs#L119-L128))
- The app warns: "Your Logseq authentication tokens will be sent to this server, so only use a trusted URL." ([en.edn:1780](../../../../../src/resources/dicts/en.edn#L1780))
- **Reset to default** returns to Logseq Publish

## Troubleshooting

### My custom CSS or JS does not appear

Check these first:

1. The record path is exactly `logseq/publish.css` or `logseq/publish.js`
2. The record is not blank
3. You republished the page after editing it
4. You reloaded the public page (HTML can be cached for five minutes)

### The published page still shows old styling

Republish first, then wait out the five-minute HTML cache or hard-refresh the public page.

### My custom JavaScript broke the page

Replace or blank `logseq/publish.js` with `logseq.DB.setFileContent`, republish the page, and reload.

### Publish failed

- Make sure you are signed in (see [Before You Start](#before-you-start))
- If you set a **Publish Server URL**, check that it is correct, or click **Reset to default**

### The browser warns about `tabler-extension.css`

This is a known defect, not your customization. Every published page links `/static/tabler-extension.css` ([render.cljs:244](../../../../../deps/publish/src/logseq/publish/render.cljs#L244)), but the worker's inline definition of that file is commented out ([routes.cljs:18](../../../../../deps/publish/src/logseq/publish/routes.cljs#L18)). Its static routes serve only `publish.css`, `publish.js` and `tabler.ext.js` ([routes.cljs:624-645](../../../../../deps/publish/src/logseq/publish/routes.cljs#L624-L645)).

The request is not answered with a 404. It falls through to the router's last branch, which builds a response from the HTML "not found" page without setting a status ([routes.cljs:779-784](../../../../../deps/publish/src/logseq/publish/routes.cljs#L779-L784)). So the stylesheet request gets **HTTP 200** with an HTML body served as `text/html`. Browsers ignore it as a stylesheet, and the console may show a MIME-type warning rather than a 404.

### I expected "Make it public for publishing" to publish the page

That page-menu item sets `:logseq.property/publishing-public?` ([page_menu.cljs:148-150](../../../../../src/main/frontend/components/page_menu.cljs#L148-L150), [page.cljs:144](../../../../../src/main/frontend/handler/page.cljs#L144)). Only the static **Export public pages** flow reads that property ([db.cljs:17](../../../../../deps/publishing/src/logseq/publishing/db.cljs#L17)). **Publish page** ignores it.

## Alternative: Export Public Pages as a Static Site

This is the separate, **Electron-only** static export ([export.cljs:145-148](../../../../../src/main/frontend/components/export.cljs#L145-L148)).

1. Choose which pages are public. Either:
   - on each page, open the page menu and choose **Make it public for publishing** (Electron only; [page_menu.cljs:148-150](../../../../../src/main/frontend/components/page_menu.cljs#L148-L150); [en.edn:1125](../../../../../src/resources/dicts/en.edn#L1125)), or
   - turn on **Settings → Editor → All pages public when publishing**, which sets `:publishing/all-pages-public?` ([settings.cljs:541-547](../../../../../src/main/frontend/components/settings.cljs#L541-L547); [en.edn:1714](../../../../../src/resources/dicts/en.edn#L1714)). All pages are then exported except those whose property is explicitly `false` ([html.cljs:141-146](../../../../../deps/publishing/src/logseq/publishing/html.cljs#L141-L146)).
2. Open the header **⋯** menu → **Export graph** → **Export public pages** ([header.cljs:227-230](../../../../../src/main/frontend/components/header.cljs#L227-L230); [en.edn:640](../../../../../src/resources/dicts/en.edn#L640), [:656](../../../../../src/resources/dicts/en.edn#L656))
3. Pick an output folder. Logseq writes `index.html` plus a `static/` folder and the referenced assets ([core.cljs:149-161](../../../../../src/electron/electron/core.cljs#L149-L161), [export.cljs:99-118](../../../../../deps/publishing/src/logseq/publishing/export.cljs#L99-L118))

If `:publishing/enable-editing?` is true in the graph's config, the exported app allows editing ([state.cljs:504-510](../../../../../src/main/frontend/state.cljs#L504-L510)).

## Next Step

If you want the architectural view of the same feature, read [How Publishing Works](../../explanation/features/how-publishing-works.md).
