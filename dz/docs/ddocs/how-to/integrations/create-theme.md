---
title: 'Create a Theme'
description: 'How to create and publish custom themes for Logseq'
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  reference:
    - '[CSS Variables](../../reference/configuration/css-variables.md)'
  howTo:
    - '[Custom CSS](custom-css.md)'
---

# Create a Theme

> **Last revised:** 2026-09-11

> How to create and package a custom theme for Logseq.

Logseq themes are CSS overrides. A personal theme lives in the graph's `custom.css`. Shareable themes ship as Logseq plugins that declare theme CSS files. The selectors, variables and plugin behavior on this page were read from the source at `be800f1711`. Nothing was tested in a running app, so confirm the result in DevTools.

## Findings: New Capabilities

- Theme state is driven by `html[data-theme="light|dark"]` and `html[data-color="..."]`, with backward-compatible classes on `body` ([`src/main/frontend/components/theme.cljs:52-66`](../../../../../src/main/frontend/components/theme.cljs)).
- Plugin themes can be registered declaratively with `logseq.themes` in `package.json` or at runtime with `logseq.provideTheme(theme)` ([`libs/src/LSPlugin.core.ts:771-776`](../../../../../libs/src/LSPlugin.core.ts), [`libs/src/LSPlugin.user.ts:870-873`](../../../../../libs/src/LSPlugin.user.ts)).
- Logseq also sets `html[data-font]` and `html[data-font-global]` for font-aware styling ([`theme.cljs:68-74`](../../../../../src/main/frontend/components/theme.cljs)).
- A selected plugin theme is appended to `document.head` as a `<link rel="stylesheet">` ([`libs/src/common.ts:595-599`](../../../../../libs/src/common.ts)), and the choice is saved separately for light and dark mode ([`LSPlugin.core.ts:1886-1897`](../../../../../libs/src/LSPlugin.core.ts)).
- Most color rules read an `--lx-*` variable first and use the older `--ls-*` variable only as a fallback, so a theme that sets only `--ls-*` often changes nothing. See [How Colors Resolve](#how-colors-resolve).
- Newer shared UI (shui/Tailwind) reads HSL *channel* tokens such as `--background`, `--popover`, `--border` and `--primary` ([`tailwind.config.js:198-228`](../../../../../tailwind.config.js)), so a complete theme sets those too.
- The repository has a theme guide at `libs/guides/custom_theme_guide.md`. This page did not audit it.

## Theme Basics

This page covers DB graphs. Current Logseq does not open file graphs ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).

### Two Options

1. **Personal theme**: the graph's `custom.css`. In a DB graph it is a record in the graph database with the path `logseq/custom.css`, not a file on disk ([`deps/db/src/logseq/db/sqlite/create_graph.cljs:211-215`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)). It also loads in the mobile app.
2. **Shareable theme**: a Logseq plugin that declares one or more theme CSS files. Plugins run only in the desktop app and the web app, not in published sites or the native mobile apps ([`src/main/frontend/util.cljc:131`](../../../../../src/main/frontend/util.cljc)), and only while plugins are enabled ([`src/main/frontend/config.cljs:149-152`](../../../../../src/main/frontend/config.cljs)).

---

## Quick Start

### 1. Open custom.css

Open Settings, stay on the **General** tab, find **Custom theme** and click **Edit custom.css**. The row appears only when a graph is open ([`src/main/frontend/components/settings.cljs:217-222`](../../../../../src/main/frontend/components/settings.cljs), [`:885`](../../../../../src/main/frontend/components/settings.cljs)).

### 2. Add Your Colors

```css
/* Dark mode */
html[data-theme='dark'] body.dark-theme {
  --lx-gray-01: #1a1a2e;   /* body background */
  --lx-gray-12: #eeeeee;   /* body text, page titles */
  --lx-accent-11: #4cc9f0; /* links, page refs, tags */
}

/* Light mode */
html[data-theme='light'] body.light-theme {
  --lx-gray-01: #fafafa;
  --lx-gray-12: #333333;
  --lx-accent-11: #0077b6;
}
```

The body reads `--lx-gray-12` and `--lx-gray-01` first ([`src/main/frontend/common.css:32-34`](../../../../../src/main/frontend/common.css)). Page titles read `--lx-gray-12` first ([`common.css:334-337`](../../../../../src/main/frontend/common.css)). Links, page refs and tags read `--lx-accent-11` first ([`common.css:91`](../../../../../src/main/frontend/common.css), [`src/main/frontend/components/block.css:885-889`](../../../../../src/main/frontend/components/block.css), [`common.css:477`](../../../../../src/main/frontend/common.css)).

### 3. Save

Saving stores the content in the graph database and applies the stylesheet again right away. There is no reload step ([`src/main/frontend/handler/db_based/editor.cljs:164-165`](../../../../../src/main/frontend/handler/db_based/editor.cljs), [`src/main/frontend/util.cljc:926-944`](../../../../../src/main/frontend/util.cljc)).

---

## Theme Structure

A personal theme is a single record inside the graph database, at the path `logseq/custom.css`. There is no `logseq/` folder to create.

A shareable theme plugin is a package:

```
logseq-my-theme/
├── package.json        # logseq.themes declares each CSS file
├── index.html          # Plugin entry file
├── icon.png
├── README.md
├── screenshots/
│   ├── light.png
│   └── dark.png
└── themes/
    ├── my-theme-light.css
    └── my-theme-dark.css
```

> **Unverified:** whether a plugin that only declares themes needs `index.html`. The loader records a `theme` flag when `logseq.theme` or `logseq.themes` is present ([`libs/src/LSPlugin.core.ts:740`](../../../../../libs/src/LSPlugin.core.ts)), but this audit did not trace whether that flag skips loading the entry file.

---

## Color System

Logseq's theme contract has three layers of variables plus one set of tokens:

- Radix raw tokens: `--rx-*` ([`src/main/frontend/shui/radix.css`](../../../../../src/main/frontend/shui/radix.css), [`radix-hsl.css`](../../../../../src/main/frontend/shui/radix-hsl.css))
- Logseq scales: `--lx-gray-01`…`12` and `--lx-accent-01`…`12`, chosen by the accent color ([`src/main/frontend/shui/colors.css`](../../../../../src/main/frontend/shui/colors.css))
- Semantic variables: `--ls-*` ([`src/main/frontend/shui/vars-classic.css`](../../../../../src/main/frontend/shui/vars-classic.css), plus per-accent values in `colors.css`)
- shui tokens: `--background`, `--primary`, `--border` and friends ([`src/main/frontend/shui/index.css`](../../../../../src/main/frontend/shui/index.css), [`resources/css/shui.css`](../../../../../resources/css/shui.css))

### How Colors Resolve

Two facts decide whether a variable you set changes anything.

1. **Most rules read `--lx-*` first.** For example, the body uses `color: var(--lx-gray-12, var(--ls-primary-text-color, …))` ([`common.css:33`](../../../../../src/main/frontend/common.css)). With the default Logseq accent in light mode, and with every other accent, `--lx-gray-*` is already set, so setting only the `--ls-*` fallback changes nothing there. Set `--ls-*` only where a rule reads it first. The [CSS Variables Reference](../../reference/configuration/css-variables.md) lists what reads each variable.
2. **Where the defaults are declared.** With the default Logseq accent, the classic `--ls-*` colors are declared on `html[data-theme=light][data-color=logseq]` and `html[data-theme=dark][data-color=logseq]` ([`vars-classic.css:42`](../../../../../src/main/frontend/shui/vars-classic.css), [`:100`](../../../../../src/main/frontend/shui/vars-classic.css)), so a rule on `html[data-theme='dark']` alone is less specific and loses. With any other accent, `colors.css` declares its variables on `<body>` through `html[data-color=<accent>] body` and `html[data-color=<accent>] .dark-theme` ([`colors.css:57-58`](../../../../../src/main/frontend/shui/colors.css)), which shadows anything set on `<html>`. A rule on `html[data-theme='dark'] body.dark-theme` is more specific than both.

> **Removed:** The old three-step authoring table that implied fixed meanings for every Radix step. The current source treats these as token aliases, not a theme API you need to model in docs.

> **Removed:** The claim that `html[data-theme="light"]` alone guarantees a theme will ignore accent color. Accent color still affects many tokens through `html[data-color="..."]`.

### Current Hooks

Scope theme CSS by these hooks when needed:

| Hook | Current use |
| --- | --- |
| `html[data-theme="light"]` / `html[data-theme="dark"]` | Light and dark mode ([`theme.cljs:52`](../../../../../src/main/frontend/components/theme.cljs)) |
| `html[data-color="..."]` | Accent color; `logseq` when none was picked, `none` for the **None** accent ([`theme.cljs:62-66`](../../../../../src/main/frontend/components/theme.cljs), [`settings.cljs:399`](../../../../../src/main/frontend/components/settings.cljs)) |
| `html.dark` | Tailwind dark-mode hook ([`theme.cljs:55`](../../../../../src/main/frontend/components/theme.cljs), [`tailwind.config.js:120`](../../../../../tailwind.config.js)) |
| `body.light-theme`, `body.white-theme`, `body.dark-theme` | Backward-compatible body classes ([`theme.cljs:54-56`](../../../../../src/main/frontend/components/theme.cljs)) |
| `html[data-font="serif"|"mono"]`, `html[data-font-global="true"]` | Font preference hooks ([`theme.cljs:68-74`](../../../../../src/main/frontend/components/theme.cljs), styled in [`src/main/frontend/components/theme.css:204-218`](../../../../../src/main/frontend/components/theme.css)) |
| `html.is-mobile`, `html.is-electron`, `html.is-mac`, `html.is-native-iphone` | Platform-specific fixes ([`src/main/frontend/ui.cljs:344-359`](../../../../../src/main/frontend/ui.cljs)) |

### shui/Tailwind HSL tokens

Overriding only `--ls-*` leaves the newer shared UI (dialogs, popovers, dropdowns, buttons) on the default palette. Those components read HSL **channel** tokens, because consumers call `hsl(var(--primary))` ([`tailwind.config.js:198-228`](../../../../../tailwind.config.js)):

```css
html[data-theme='dark'] body.dark-theme {
  --background: 222 47% 11%;
  --foreground: 210 40% 96%;
  --popover: 222 47% 10%;
  --popover-foreground: 210 40% 96%;
  --border: 217 33% 24%;
  --input: 217 33% 24%;
  --primary: 199 89% 48%;
  --primary-foreground: 210 40% 98%;
  --accent: 199 89% 48%;
  --ring: 199 89% 48%;
}
```

The `body` selector matters here too: with the Logseq accent, dark-mode tokens such as `--background` and `--primary` are set on `html[data-color=logseq][data-theme=dark]` ([`colors.css:37-54`](../../../../../src/main/frontend/shui/colors.css)), which beats a plain `html[data-theme='dark']` rule.

Write `--primary: 199 89% 48%`, not `--primary: hsl(199 89% 48%)`.

---

## Personal Theme Template

```css
/* custom.css */

html[data-theme='light'] body.light-theme {
  color-scheme: light;

  /* Surfaces and text: read first by most rules */
  --lx-gray-01: #ffffff;   /* body background */
  --lx-gray-02: #f8fafc;   /* left sidebar */
  --lx-gray-03: #eef2f7;   /* the most-read gray step */
  --lx-gray-12: #1e293b;   /* body text, page titles */

  /* Links, page refs, tags */
  --lx-accent-11: #0369a1;
  --lx-accent-12: #0c4a6e; /* link hover */

  /* Read first by their rules */
  --ls-block-highlight-color: #e0f2fe;      /* selected blocks */
  --ls-selection-background-color: #dbeafe; /* text selection */
  --ls-page-blockquote-bg-color: #f1f5f9;   /* blockquotes */

  /* shui tokens: HSL channels */
  --background: 0 0% 100%;
  --foreground: 222 47% 11%;
  --primary: 199 89% 39%;
  --border: 214 32% 91%;
}

html[data-theme='dark'] body.dark-theme {
  color-scheme: dark;

  --lx-gray-01: #111827;
  --lx-gray-02: #172033;
  --lx-gray-03: #202b42;
  --lx-gray-12: #d6deeb;

  --lx-accent-11: #7dd3fc;
  --lx-accent-12: #bae6fd;

  --ls-block-highlight-color: rgb(14 165 233 / 0.22);
  --ls-selection-background-color: rgb(56 189 248 / 0.32);
  --ls-page-blockquote-bg-color: #1e293b;

  --background: 222 47% 11%;
  --foreground: 210 40% 96%;
  --primary: 199 89% 48%;
  --border: 217 33% 24%;
}

/* Global typography and layout tokens */
:root {
  --ls-font-family: 'Inter', -apple-system, sans-serif;
  --ls-page-text-size: 16px;
  --ls-page-title-size: 28px;
  --ls-main-content-max-width: 830px;
}
```

What each group feeds:

- `--lx-gray-02` is the left sidebar background ([`theme.css:200-202`](../../../../../src/main/frontend/components/theme.css)). Link hover reads `--lx-accent-12` first ([`common.css:95-97`](../../../../../src/main/frontend/common.css)).
- `--ls-block-highlight-color` colors `.block-highlight`, `.ls-block.selected` and `.ls-table-cell.selected` ([`common.css:346-351`](../../../../../src/main/frontend/common.css)). `--ls-selection-background-color` is the `::selection` background ([`common.css:158-161`](../../../../../src/main/frontend/common.css)). `--ls-page-blockquote-bg-color` is the blockquote background ([`common.css:118-122`](../../../../../src/main/frontend/common.css)).
- `--ls-font-family` is the font on `html:not(.is-native-android)` ([`common.css:17-18`](../../../../../src/main/frontend/common.css)). If the serif or mono editor font is applied globally, a more specific rule redeclares it ([`theme.css:208-218`](../../../../../src/main/frontend/components/theme.css)). `--ls-page-text-size` is the `#root` font size ([`src/main/frontend/components/container.css:8-10`](../../../../../src/main/frontend/components/container.css)), `--ls-page-title-size` the page title size ([`common.css:338`](../../../../../src/main/frontend/common.css)), and `--ls-main-content-max-width` the content width in normal mode ([`container.css:502-503`](../../../../../src/main/frontend/components/container.css)).
- `custom.css` is appended to the end of `<head>` ([`util.cljc:937-944`](../../../../../src/main/frontend/util.cljc)), so its `:root` rules win over Logseq's own `:root` defaults of equal specificity.

An earlier version of this template also set `--ls-focus-ring-color`, `--ls-block-properties-background-color`, `--ls-page-properties-background-color` and `--ls-active-secondary-color`. The first is never read. The block-properties color is read only by `.block-properties, .page-properties` ([`block.css:939-942`](../../../../../src/main/frontend/components/block.css)), which the app does not use for DB properties. The page-properties color is read only by `.open-block-ref-link` and `.query-title` ([`block.css:193`](../../../../../src/main/frontend/components/block.css), [`:1395`](../../../../../src/main/frontend/components/block.css)). `--ls-active-secondary-color` is read only by the Logseq accent's dark link-hover alias, which is declared on `<html>` ([`vars-classic.css:120`](../../../../../src/main/frontend/shui/vars-classic.css)), so setting it on `<body>` does not reach that alias.

---

## Adding Custom Fonts

### From a Web Font Service

```css
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600&display=swap');

:root {
  --ls-font-family: 'Inter', sans-serif;
}

/* Code blocks use CodeMirror 5; target its line class */
.CodeMirror pre.CodeMirror-line {
  font-family: 'JetBrains Mono', monospace;
}
```

Logseq bundles CodeMirror 5's stylesheet ([`tailwind.all.css:13`](../../../../../tailwind.all.css)).

> **Unverified:** that a font from a remote service loads. Logseq injects `custom.css` as a `data:` URL stylesheet ([`util.cljc:932-934`](../../../../../src/main/frontend/util.cljc)). Whether the app's content security policy allows the remote font was not checked.

### Local Fonts

There is no graph folder to put font files in, and a relative URL such as `./fonts/MyFont.woff2` has no folder to resolve against, because the stylesheet is a `data:` URL ([`util.cljc:932-934`](../../../../../src/main/frontend/util.cljc)). The only relative URLs Logseq rewrites are `../assets/…` ones, which become blob URLs for the graph's local assets ([`src/main/frontend/handler/assets.cljs:134-146`](../../../../../src/main/frontend/handler/assets.cljs)):

```css
@font-face {
  font-family: 'MyFont';
  src: url('../assets/MyFont.woff2') format('woff2');
  font-weight: normal;
}

:root {
  --ls-font-family: 'MyFont', sans-serif;
}
```

> **Unverified:** how to get a font file into the graph's assets, and that the resulting blob URL loads as a font. The rewrite above is derived from source only.

---

## Component Styling

### Tags

```css
a.tag {
  background-color: var(--lx-gray-03);
  padding: 2px 6px;
  border-radius: 4px;
}

a.tag[data-ref='important'] {
  background-color: #ef4444;
  color: white;
}

/* Tags attached to a block, shown in its tag list */
.block-tag a.tag {
  padding: 2px 6px;
  border-radius: 4px;
}
```

Tag links have the class `tag` ([`block.cljs:796-800`](../../../../../src/main/frontend/components/block.cljs)). That covers a `#tag` that refers to a tag page inside block text ([`block.cljs:1983-1987`](../../../../../src/main/frontend/components/block.cljs)) and the tags in a block's tag list, where each one is wrapped in `div.block-tag` ([`block.cljs:2820`](../../../../../src/main/frontend/components/block.cljs), [`:2870-2874`](../../../../../src/main/frontend/components/block.cljs)). A `#tag` whose value is not a page UUID renders as a plain `span` ([`block.cljs:1988`](../../../../../src/main/frontend/components/block.cljs)). `data-ref` is the tag title in lower case ([`block.cljs:788-789`](../../../../../src/main/frontend/components/block.cljs), [`:804`](../../../../../src/main/frontend/components/block.cljs)).

### Code Blocks

```css
/* CodeMirror editor wrapper */
.extensions__code {
  background: var(--lx-gray-02);
  border-radius: 8px;
}

.CodeMirror pre.CodeMirror-line {
  font-family: 'JetBrains Mono', monospace;
}
```

Code blocks render in `div.extensions__code` ([`src/main/frontend/extensions/code.cljs:627`](../../../../../src/main/frontend/extensions/code.cljs)). The CodeMirror theme is `lsradix light` or `lsradix dark` when the code editor's `:radix-color?` option is set, and `solarized light` or `solarized dark` otherwise ([`code.cljs:402-406`](../../../../../src/main/frontend/extensions/code.cljs), [`:411`](../../../../../src/main/frontend/extensions/code.cljs)). CodeMirror turns that into the classes `.cm-s-lsradix` plus `.cm-s-light` or `.cm-s-dark`, which `resources/css/codemirror.lsradix.css` styles mostly from `--lx-*` variables (39 of its 67 `var()` reads; the rest are `--rx-*` apart from one `--ls-*`) ([`codemirror.lsradix.css:32-55`](../../../../../resources/css/codemirror.lsradix.css)). Override syntax colors only when you need to:

```css
.cm-s-lsradix.cm-s-dark .CodeMirror-linenumber {
  color: var(--lx-gray-09);
}
```

### Page References

```css
.page-ref,
.page-ref:hover {
  color: #0369a1;
}

.page-ref:hover {
  text-decoration: underline;
}
```

Page reference links are `a.page-ref` ([`block.cljs:796-800`](../../../../../src/main/frontend/components/block.cljs)), and their color rule reads `--lx-accent-11` first ([`block.css:885-889`](../../../../../src/main/frontend/components/block.css)). An earlier version of this example styled `.page-reference`. That class exists on a wrapping `span` ([`block.cljs:1274`](../../../../../src/main/frontend/components/block.cljs)), but a color set on an ancestor does not override the link's own color rule.

### Bullets and Property Panels

```css
.bullet-container .bullet {
  width: 6px;
  height: 6px;
}

.ls-properties-area {
  border-radius: 8px;
  padding: 8px 12px;
}
```

Bullets are `span.bullet` inside `span.bullet-container` ([`block.cljs:2312`](../../../../../src/main/frontend/components/block.cljs), [`:2334`](../../../../../src/main/frontend/components/block.cljs)). DB graphs render properties in `div.ls-properties-area`, with the class `ls-page-properties` on pages and `ls-block-properties` on blocks ([`src/main/frontend/components/property.cljs:972-975`](../../../../../src/main/frontend/components/property.cljs)). The older `.block-properties` selector still has a stylesheet rule ([`block.css:939`](../../../../../src/main/frontend/components/block.css)), but the app does not render that class. It appears only in the separate publishing renderer ([`deps/publish/src/logseq/publish/render.cljs:1026`](../../../../../deps/publish/src/logseq/publish/render.cljs)).

---

## Theme Plugins

The plugin system supports two ways to register a theme:

1. Declarative `logseq.themes` entries in `package.json`, loaded with the plugin ([`LSPlugin.core.ts:771-776`](../../../../../libs/src/LSPlugin.core.ts), [`:850-866`](../../../../../libs/src/LSPlugin.core.ts))
2. Programmatic `logseq.provideTheme(theme)` ([`LSPlugin.user.ts:870-873`](../../../../../libs/src/LSPlugin.user.ts), handled in [`LSPlugin.core.ts:353-364`](../../../../../libs/src/LSPlugin.core.ts))

Both paths go through the plugin core, not a host API export. Both register the theme with the theme manager ([`LSPlugin.core.ts:1843-1853`](../../../../../libs/src/LSPlugin.core.ts)), and the app adds it to its list of installed themes ([`src/main/frontend/handler/plugin.cljs:1269-1271`](../../../../../src/main/frontend/handler/plugin.cljs)). When the plugin unloads its themes are unregistered, and a saved light or dark choice that pointed at them is cleared ([`LSPlugin.core.ts:357-360`](../../../../../libs/src/LSPlugin.core.ts), [`:1905-1929`](../../../../../libs/src/LSPlugin.core.ts)).

### `package.json`

```json
{
  "name": "logseq-theme-mytheme",
  "version": "1.0.0",
  "description": "A custom Logseq theme",
  "author": "Your Name",
  "main": "index.html",
  "logseq": {
    "id": "logseq-theme-mytheme",
    "title": "My Theme",
    "icon": "./icon.png",
    "themes": [
      {
        "name": "My Theme Light",
        "url": "./themes/my-theme-light.css",
        "mode": "light"
      },
      {
        "name": "My Theme Dark",
        "url": "./themes/my-theme-dark.css",
        "mode": "dark"
      }
    ]
  }
}
```

- `themes` may be a single object or an array ([`LSPlugin.core.ts:773-776`](../../../../../libs/src/LSPlugin.core.ts)).
- Each theme has `name` and `url`, and optionally `description` and `mode` (`light` or `dark`) ([`libs/src/LSPlugin.ts:14-26`](../../../../../libs/src/LSPlugin.ts)). An entry without `url` is skipped ([`LSPlugin.core.ts:852`](../../../../../libs/src/LSPlugin.core.ts)).
- A `url` that does not start with `http` is resolved against the plugin's folder ([`LSPlugin.core.ts:854-862`](../../../../../libs/src/LSPlugin.core.ts)).
- `logseq.title` and `logseq.icon` fall back to top-level `title` and `icon` ([`LSPlugin.core.ts:735-739`](../../../../../libs/src/LSPlugin.core.ts)).

An earlier version of this example also set `"effect": true`. That key is not needed for a theme and this audit did not confirm what it does, so it was dropped.

### Programmatic Variant

```ts
import '@logseq/libs'

async function main() {
  logseq.provideTheme({
    name: 'My Generated Dark Theme',
    url: 'https://example.com/generated-dark.css',
    mode: 'dark',
    description: 'Generated from plugin settings',
  })
}

logseq.ready(main).catch(console.error)
```

- Use an absolute URL. The folder-relative resolution runs only for `logseq.themes` ([`LSPlugin.core.ts:850-862`](../../../../../libs/src/LSPlugin.core.ts)). `provideTheme` registers the URL unchanged ([`LSPlugin.core.ts:354-355`](../../../../../libs/src/LSPlugin.core.ts)), and the stylesheet link uses it as its `href` ([`common.ts:595-599`](../../../../../libs/src/common.ts)), so a relative URL would resolve against the Logseq page. This is derived from source and was not run.
- The TypeScript `Theme` type marks `pid` as required ([`LSPlugin.ts:21`](../../../../../libs/src/LSPlugin.ts)), so a type checker may flag the call above. The core registers the theme under the calling plugin's id anyway ([`LSPlugin.core.ts:355`](../../../../../libs/src/LSPlugin.core.ts)).
- For a small style override that users do not pick as a theme, `logseq.provideStyle(css)` injects the CSS for as long as the plugin is loaded ([`LSPlugin.user.ts:875-878`](../../../../../libs/src/LSPlugin.user.ts), [`LSPlugin.core.ts:366-383`](../../../../../libs/src/LSPlugin.core.ts)).

### Choosing a Theme

- The **Themes** item in the plugins menu opens the theme list in search ([`src/main/frontend/components/plugins.cljs:1481-1482`](../../../../../src/main/frontend/components/plugins.cljs), [`src/main/frontend/handler/events/ui.cljs:128-131`](../../../../../src/main/frontend/handler/events/ui.cljs)). That menu is rendered with the toolbar's plugin items ([`plugins.cljs:1554-1555`](../../../../../src/main/frontend/components/plugins.cljs), [`src/main/frontend/components/header.cljs:598`](../../../../../src/main/frontend/components/header.cljs)).
- The shortcut `t i` (**Select available theme colors**) opens the classic theme dialog ([`src/main/frontend/modules/shortcut/config.cljs:461-462`](../../../../../src/main/frontend/modules/shortcut/config.cljs), [`plugins.cljs:1397-1400`](../../../../../src/main/frontend/components/plugins.cljs), [`src/resources/dicts/en.edn:428`](../../../../../src/resources/dicts/en.edn)).
- A theme with a `mode` is saved as the choice for that mode ([`LSPlugin.core.ts:1886-1897`](../../../../../libs/src/LSPlugin.core.ts), [`plugin.cljs:1273-1282`](../../../../../src/main/frontend/handler/plugin.cljs)). When the mode changes, Logseq selects the saved theme for the new mode, if plugins are enabled ([`theme.cljs:57`](../../../../../src/main/frontend/components/theme.cljs), [`src/main/frontend/ui.cljs:384-392`](../../../../../src/main/frontend/ui.cljs)).

---

## Testing Your Theme

### Check Both Modes

1. Toggle between light and dark mode in Settings → General ([`settings.cljs:348`](../../../../../src/main/frontend/components/settings.cljs), [`:879`](../../../../../src/main/frontend/components/settings.cljs)).
2. Switch accent colors in Settings → General → **Accent color** and confirm the theme still reads correctly ([`settings.cljs:374-434`](../../../../../src/main/frontend/components/settings.cljs), [`:882`](../../../../../src/main/frontend/components/settings.cljs)). The **None** accent leaves the classic `--ls-*` colors unset, so it is a good test for fallbacks.
3. Check font preferences if your theme changes typography ([`settings.cljs:880`](../../../../../src/main/frontend/components/settings.cljs)).

### Test Core Surfaces

- Page titles
- Block content
- Links, tags, and references
- Code blocks
- Properties
- Sidebar and search
- Settings panel

### Developer Tools

1. Open DevTools.
2. Inspect `<html>` for `data-theme` and `data-color`, and `<body>` for `dark-theme` or `light-theme`.
3. In the Computed pane, check the variable's value on the element you are styling, not just the selector that matched. A value set on `<html>` can be shadowed by one set on `<body>`.

---

## Best Practices

### Do

- ✅ Use CSS variables for colors, starting with `--lx-*`
- ✅ Support both light and dark modes
- ✅ Test with different content types
- ✅ Use semantic color names
- ✅ Include screenshots
- ✅ Verify your theme against accent color changes
- ✅ Keep layout overrides minimal

### Don't

- ❌ Hardcode colors in component selectors
- ❌ Override functional styles (layout, positioning)
- ❌ Use `!important` excessively
- ❌ Forget mobile/responsive testing

> **Removed:** The old "Submit to Marketplace" checklist. Theme distribution and release steps are covered by the plugin release workflow, not this how-to.

---

## Popular Theme Examples

> **Unverified:** these theme names do not come from Logseq's source. Check the plugin marketplace for what is currently available.

Study these for inspiration:

- **Bear Theme** - Minimalist design
- **Dracula** - Dark purple palette
- **Nord** - Arctic color scheme
- **Catppuccin** - Pastel colors

---

## See Also

- **CSS Variables**: [css-variables.md](../../reference/configuration/css-variables.md)
- **Custom CSS**: [custom-css.md](custom-css.md)
- **In-repo theme guide (not audited)**: `libs/guides/custom_theme_guide.md`

---

_Last updated: 2026-09-11_
