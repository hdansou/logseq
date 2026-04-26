---
title: 'Publish a Page'
description: 'How to publish a page to the web and customize the published design with CSS and JavaScript'
status: stable
lastVerified: '2026-04-24'
logseqVersion: '0.11.x'
platforms: [desktop, web]
graphType: [db]
relatedDocs:
  explanation:
    - '[How Publishing Works](../../explanation/features/how-publishing-works.md)'
  howTo:
    - '[Custom CSS](custom-css.md)'
    - '[Custom JavaScript](custom-js.md)'
---

> **Last revised:** 2026-04-24

How to publish a page to the web, protect it with an optional password, and customize its public design with `logseq/publish.css` and `logseq/publish.js`.

## Before You Start

This guide covers the current **Publish page** workflow in the Logseq app.

It does **not** describe the older static export flow under `deps/publishing`.

You will need:

- a Logseq page you want to publish
- a signed-in Logseq account
- optional custom files at `logseq/publish.css` and `logseq/publish.js`

## Step 1: Open the Publish Dialog

You can open the publish dialog in either of these ways:

1. Open the page.
2. Open the page menu.
3. Select **Publish page**.

Or use the keyboard shortcut:

- macOS: `Cmd+M`
- Windows/Linux: `Ctrl+M`

The dialog lets you enter an optional password before publishing.

## Step 2: Publish the Page

1. Enter a password if you want the page to require one.
2. Leave the password empty if the page should be public.
3. Click **Publish**.

After a successful publish, Logseq:

- uploads the page snapshot
- uploads referenced assets
- uploads non-empty `logseq/publish.css` and `logseq/publish.js`
- writes the returned URL back to the page as **Published URL**

## Step 3: Open or Unpublish the Result

Once the page is published, look at the page properties.

You will see the built-in **Published URL** property with:

- the public URL as a link
- an **Unpublish** button

Use that button when you want to remove the public page.

## Step 4: Customize the Published Theme with CSS

Create or edit this file in your graph:

```text
logseq/publish.css
```

When you publish, Logseq uploads this file and links it **after** the built-in publish stylesheet. That means your CSS overrides win by normal cascade order.

### Start with theme variables

The fastest way to change the published design is to override the built-in custom properties:

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

Useful selectors from the built-in publish theme include:

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

Create or edit this file in your graph:

```text
logseq/publish.js
```

This file is loaded **after** the built-in publish runtime. Use it to add small public-site behaviors such as banners, button actions, or layout tweaks that are easier in JavaScript than CSS.

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

- its HTML rendered
- the built-in theme toggle
- the built-in search UI
- block toggle handlers
- math and code enhancement setup

Use normal browser APIs. Do not assume the Logseq plugin SDK is available on the public page.

## Step 6: Republish After Design Changes

Changes to `logseq/publish.css` and `logseq/publish.js` are uploaded during publish.

After editing either file:

1. return to the page
2. run **Publish page** again
3. reload the public page

If the old design still appears, do a hard refresh.

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

## Troubleshooting

### My custom CSS or JS does not appear

Check these first:

1. The file path is exactly `logseq/publish.css` or `logseq/publish.js`.
2. The file is not empty.
3. You republished the page after editing it.
4. You reloaded the public page.

### The published page still shows old styling

The public page uses cached assets and HTML. Republish first, then hard-refresh the public page.

### My custom JavaScript broke the page

Remove the problematic code from `logseq/publish.js`, republish the page, and reload.

### I expected `publishing-public?` to publish the page

That property belongs to the separate static export code path. The current in-app public-page workflow uses **Publish page** from the page menu instead.

## Next Step

If you want the architectural view of the same feature, read [How Publishing Works](../../explanation/features/how-publishing-works.md).
