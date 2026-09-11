---
title: 'Custom CSS'
description: "How to customize Logseq's appearance with custom CSS"
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
    - '[Custom JavaScript](custom-js.md)'
---

# Custom CSS

> **Last revised:** 2026-09-11

> How to customize Logseq's appearance with your own CSS styles.

This page covers DB graphs only. Current Logseq does not open file graphs: the graph list keeps only DB graphs ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).

## Where Custom CSS Lives

In a DB graph, `custom.css` is not a file on disk. It is a file record stored inside the graph database with the path `logseq/custom.css`. Logseq creates it, empty, when it creates the graph ([`deps/db/src/logseq/db/sqlite/create_graph.cljs:211-215`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)). Editing a `custom.css` file in a folder on disk has no effect on a DB graph.

When you import a file graph, the importer copies that graph's `logseq/custom.css` into the new DB graph ([`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:3330-3335`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs)).

## Open custom.css for Editing

### Option 1: Settings (Recommended)

1. Open Settings.
2. Stay on the **General** tab.
3. Find **Custom theme** and click **Edit custom.css**.
4. Logseq closes Settings and opens `logseq/custom.css` in its built-in code editor.

The button is part of the General tab and appears only when a graph is open ([`src/main/frontend/components/settings.cljs:217-223`](../../../../../src/main/frontend/components/settings.cljs), [`:871-885`](../../../../../src/main/frontend/components/settings.cljs)). Labels come from [`src/resources/dicts/en.edn:1692`](../../../../../src/resources/dicts/en.edn), [`:1752`](../../../../../src/resources/dicts/en.edn) and [`:1754`](../../../../../src/resources/dicts/en.edn).

### Option 2: Search

1. Open search and type `custom.css`.
2. Choose `logseq/custom.css` in the **Files** group.

Search lists the graph's stored files whenever you type a query ([`src/main/frontend/components/cmdk/core.cljs:439-451`](../../../../../src/main/frontend/components/cmdk/core.cljs), [`:539`](../../../../../src/main/frontend/components/cmdk/core.cljs); [`src/main/frontend/search.cljs:29-43`](../../../../../src/main/frontend/search.cljs)). Choosing a file opens the same file editor as Settings ([`src/main/frontend/components/cmdk/core.cljs:644-647`](../../../../../src/main/frontend/components/cmdk/core.cljs)). Search does not offer to create a page named `custom.css` ([`:101`](../../../../../src/main/frontend/components/cmdk/core.cljs)).

### Saving

Your changes are saved into the graph database, and Logseq applies the stylesheet again right away, with no reload needed ([`src/main/frontend/handler/code.cljs:21-26`](../../../../../src/main/frontend/handler/code.cljs), [`src/main/frontend/handler/db_based/editor.cljs:142-165`](../../../../../src/main/frontend/handler/db_based/editor.cljs)).

### When custom.css is applied

Logseq injects the stylesheet as the last `<link>` in the page head ([`src/main/frontend/util.cljc:926-945`](../../../../../src/main/frontend/util.cljc)). This happens when a graph is restored at startup and again when the current graph changes ([`src/main/frontend/handler.cljs:62`](../../../../../src/main/frontend/handler.cljs), [`src/main/frontend/components/theme.cljs:18-23`](../../../../../src/main/frontend/components/theme.cljs)). The mobile app runs the same startup ([`src/main/mobile/core.cljs:99`](../../../../../src/main/mobile/core.cljs)).

> **Unverified:** whether the mobile app shows the **Edit custom.css** button. The button is in the desktop and web Settings component, and no mobile component references it.

### Loading CSS from config.edn instead

If `:custom-css-url` is set in the graph's `config.edn`, Logseq loads that and ignores `logseq/custom.css` ([`src/main/frontend/handler/ui.cljs:112-119`](../../../../../src/main/frontend/handler/ui.cljs), [`src/main/frontend/state.cljs:532-534`](../../../../../src/main/frontend/state.cljs)). A value starting with `http` is used as the stylesheet URL. Any other value is treated as CSS text, which is why the template's example is an `@import` rule ([`src/main/frontend/util.cljc:932-934`](../../../../../src/main/frontend/util.cljc), [`deps/common/resources/templates/config.edn:50-53`](../../../../../deps/common/resources/templates/config.edn)):

```clojure
:custom-css-url "@import url('https://cdn.jsdelivr.net/gh/dracula/logseq@master/custom.css');"
```

### Local images in custom.css

Before injecting the CSS, Logseq replaces relative `../assets/...` URLs with blob URLs for the graph's local assets, so `url('../assets/background.png')` works ([`src/main/frontend/handler/assets.cljs:134-146`](../../../../../src/main/frontend/handler/assets.cljs)).

### Reading or writing custom.css from a plugin

The plugin API's `logseq.DB.setFileContent(path, content)` accepts only `logseq/custom.css`, `logseq/custom.js`, `logseq/publish.css` and `logseq/publish.js`. It throws `Invalid path` for any other path ([`src/main/logseq/api/db.cljs:53-73`](../../../../../src/main/logseq/api/db.cljs)). `logseq.DB.getFileContent(path)` has no such allowlist ([`:76-79`](../../../../../src/main/logseq/api/db.cljs)). Both are declared on `IDBProxy` ([`libs/src/LSPlugin.ts:956`](../../../../../libs/src/LSPlugin.ts), [`:1003-1004`](../../../../../libs/src/LSPlugin.ts)).

Writing through `setFileContent` stores the new content but does not reapply the stylesheet. Only the file editor's save path does that ([`src/main/frontend/handler/db_based/editor.cljs:164-165`](../../../../../src/main/frontend/handler/db_based/editor.cljs)). The new CSS applies the next time the graph loads.

`logseq/publish.css` is a separate built-in file ([`deps/db/src/logseq/db/sqlite/create_graph.cljs:221-225`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)). `custom.css` does not style published pages.

---

## How Theme Colors Resolve (Read This First)

Most color rules in current Logseq read a `--lx-*` variable first and fall back to the older `--ls-*` variable only if the `--lx-*` one is unset. For example, the body uses `color: var(--lx-gray-12, var(--ls-primary-text-color, …))` and `background-color: var(--lx-gray-01, var(--ls-primary-background-color, …))` ([`src/main/frontend/common.css:32-34`](../../../../../src/main/frontend/common.css)). `--lx-gray-01`, `--lx-gray-12` and `--lx-accent-11` are defined for every accent color in [`src/main/frontend/shui/colors.css`](../../../../../src/main/frontend/shui/colors.css), so setting only the `--ls-*` fallback often changes nothing.

Specificity also matters:

- With the default Logseq accent, the classic `--ls-*` colors are set on `html[data-theme=light][data-color=logseq]` and `html[data-theme=dark][data-color=logseq]` ([`src/main/frontend/shui/vars-classic.css:42-43`](../../../../../src/main/frontend/shui/vars-classic.css), [`:100-101`](../../../../../src/main/frontend/shui/vars-classic.css)). A rule on `:root` or `html[data-theme='dark']` is less specific and loses.
- With any other accent, the variables are set on `html[data-color=<accent>] body, .dark-theme, .light-theme` ([`src/main/frontend/shui/colors.css:57-58`](../../../../../src/main/frontend/shui/colors.css)).
- Logseq sets `data-theme` on `<html>`. It also gives `<body>` the class `dark-theme` in dark mode, or `light-theme` in light mode ([`src/main/frontend/components/theme.cljs:52-56`](../../../../../src/main/frontend/components/theme.cljs)).

The color recipes below therefore set `--lx-*` variables on `html[data-theme='…'] body.…-theme`, which is more specific than both of the rules above. The selectors come from the stylesheets at `be800f1711`. They have not been tested in a running app, so confirm them in DevTools.

## Basic Customizations

### Change Font

```css
:root {
  --ls-font-family: 'Source Sans Pro', sans-serif;
}
```

`--ls-font-family` is read on `html` ([`src/main/frontend/common.css:17-18`](../../../../../src/main/frontend/common.css)), and its default is set on `:root` ([`src/main/frontend/shui/vars-classic.css:8`](../../../../../src/main/frontend/shui/vars-classic.css)). If the serif editor font is applied globally, `html[data-font='serif'][data-font-global='true']` sets the variable with higher specificity and wins ([`src/main/frontend/components/theme.css:208-210`](../../../../../src/main/frontend/components/theme.css)).

### Increase Text Size

```css
:root {
  --ls-page-text-size: 18px;
}
```

`--ls-page-text-size` sets the font size of `#root` ([`src/main/frontend/components/container.css:8-10`](../../../../../src/main/frontend/components/container.css)).

### Change Background Color

```css
/* Dark mode */
html[data-theme='dark'] body.dark-theme {
  --lx-gray-01: #1a1a2e; /* body background */
  --lx-gray-02: #16213e; /* left sidebar background */
}

/* Light mode */
html[data-theme='light'] body.light-theme {
  --lx-gray-01: #fafafa;
  --lx-gray-02: #f0f0f0;
}
```

The body background reads `--lx-gray-01` ([`src/main/frontend/common.css:34`](../../../../../src/main/frontend/common.css)), and the left sidebar reads `--lx-gray-02` ([`src/main/frontend/components/theme.css:200-202`](../../../../../src/main/frontend/components/theme.css)).

### Change Link Color

```css
.page-ref,
.page-ref:hover {
  color: #ff6b6b;
}
```

Page references take their color from `--lx-accent-11` ahead of `--ls-link-text-color` ([`src/main/frontend/components/block.css:885-889`](../../../../../src/main/frontend/components/block.css)), so setting `--ls-link-text-color` alone does not recolor them. A rule on `.page-ref` has the same specificity as Logseq's rule, and custom.css loads last, so it wins.

---

## Common Recipes

### Hide Left Sidebar

```css
#left-sidebar {
  display: none;
}
```

The left sidebar is rendered as `div#left-sidebar` ([`src/main/frontend/components/left_sidebar.cljs:556`](../../../../../src/main/frontend/components/left_sidebar.cljs)).

### Wider Content Area

```css
:root {
  --ls-main-content-max-width: 1200px;
}
```

This sets the main content width in normal mode ([`src/main/frontend/components/container.css:502-503`](../../../../../src/main/frontend/components/container.css)). In wide mode Logseq uses `--ls-main-content-max-width-wide` instead, which defaults to `1440px` ([`:488-491`](../../../../../src/main/frontend/components/container.css), [`src/main/frontend/shui/vars-classic.css:7`](../../../../../src/main/frontend/shui/vars-classic.css)).

### Rounded Corners

```css
:root {
  --ls-border-radius-low: 8px;
}
```

`--ls-border-radius-low` is read in [`src/main/frontend/common.css:50`](../../../../../src/main/frontend/common.css) and [`:597`](../../../../../src/main/frontend/common.css). An earlier version of this recipe also set `--ls-border-radius-medium`. That variable is defined ([`src/main/frontend/shui/vars-classic.css:11`](../../../../../src/main/frontend/shui/vars-classic.css)), but no stylesheet reads it, so setting it does nothing.

### Custom Block Bullets

```css
.bullet-container .bullet {
  background-color: #4ecdc4;
  width: 8px;
  height: 8px;
}
```

Bullets are `span.bullet` inside `span.bullet-container` ([`src/main/frontend/components/block.cljs:2312`](../../../../../src/main/frontend/components/block.cljs), [`:2334`](../../../../../src/main/frontend/components/block.cljs)).

### Highlight the Block Being Edited

```css
.editor-wrapper {
  background-color: rgba(100, 150, 255, 0.1);
  border-radius: 4px;
}
```

`.editor-wrapper` wraps the block editor ([`src/main/frontend/components/block.cljs:3630`](../../../../../src/main/frontend/components/block.cljs)). An earlier version of this recipe used `.editor-inner .editor-wrapper`. That matches nothing, because `.editor-inner` is rendered inside the editor, not around it ([`src/main/frontend/components/editor.cljs:771`](../../../../../src/main/frontend/components/editor.cljs)).

### Custom Tag Styles

```css
a.tag[data-ref='project'] {
  background-color: #ff6b6b;
  color: white;
  padding: 2px 6px;
  border-radius: 4px;
}

a.tag[data-ref='idea'] {
  background-color: #4ecdc4;
  color: white;
  padding: 2px 6px;
  border-radius: 4px;
}
```

Tag links have the class `tag`. Their `data-ref` is the tag title in lower case, so write `data-ref='project'` even for a tag titled `Project` ([`src/main/frontend/components/block.cljs:788-789`](../../../../../src/main/frontend/components/block.cljs), [`:800`](../../../../../src/main/frontend/components/block.cljs), [`:804`](../../../../../src/main/frontend/components/block.cljs)).

### Custom Property Area

```css
.ls-properties-area.ls-page-properties {
  background-color: var(--ls-tertiary-background-color);
  border-radius: 8px;
  padding: 8px 12px;
}
```

DB graphs render properties in `div.ls-properties-area`, with the class `ls-page-properties` on pages and `ls-block-properties` on blocks ([`src/main/frontend/components/property.cljs:972-975`](../../../../../src/main/frontend/components/property.cljs)). The older `.block-properties` selector still has a stylesheet rule ([`src/main/frontend/components/block.css:939`](../../../../../src/main/frontend/components/block.css)), but no DB graph markup uses that class.

---

## Theme-Aware Styling

Logseq sets `data-theme` on `<html>` to the current theme ([`src/main/frontend/components/theme.cljs:52`](../../../../../src/main/frontend/components/theme.cljs)):

```css
/* Dark mode only */
html[data-theme='dark'] .my-element {
  background: #333;
}

/* Light mode only */
html[data-theme='light'] .my-element {
  background: #fff;
}
```

---

## CSS Variables Reference

Variables that the recipes on this page were checked against:

| Variable                        | What reads it                                                             |
| ------------------------------- | ------------------------------------------------------------------------- |
| `--lx-gray-01`                  | Body background (`common.css:34`)                                         |
| `--lx-gray-02`                  | Left sidebar background (`theme.css:201`)                                 |
| `--lx-gray-12`                  | Body text color (`common.css:33`)                                         |
| `--lx-accent-11`                | Page reference color (`block.css:886`)                                    |
| `--ls-font-family`              | Font on `html` (`common.css:18`)                                          |
| `--ls-page-text-size`           | Font size of `#root` (`container.css:9`)                                  |
| `--ls-main-content-max-width`   | Content width, normal mode (`container.css:503`)                          |
| `--ls-main-content-max-width-wide` | Content width, wide mode (`container.css:490`)                         |
| `--ls-primary-background-color` | Fallback behind `--lx-gray-01` on the body (`common.css:34`)              |
| `--ls-link-text-color`          | Fallback behind `--lx-accent-11` on page references (`block.css:886`)     |

See full reference: [CSS Variables](../../reference/configuration/css-variables.md)

---

## Debugging CSS

### Open Developer Tools

The desktop app uses Electron's standard **View** menu ([`src/electron/electron/core.cljs:243`](../../../../../src/electron/electron/core.cljs)). That menu includes **Toggle Developer Tools**, which is usually:

- **macOS**: `Cmd+Option+I`
- **Windows/Linux**: `Ctrl+Shift+I`

These key bindings come from Electron's default View menu, not from Logseq source. In the web app, use your browser's developer tools.

### Inspect Elements

1. Open DevTools
2. Click the selector tool (top-left)
3. Click any element to inspect
4. View and modify CSS in Styles panel

### Live Edit

Edit CSS directly in DevTools to preview changes. Copy working rules to your `custom.css`.

---

## Complete Theme Example

```css
/* Catppuccin-inspired dark theme */
html[data-theme='dark'] body.dark-theme {
  --lx-gray-01: #1e1e2e; /* body background */
  --lx-gray-02: #313244; /* left sidebar */
  --lx-gray-12: #cdd6f4; /* body text */
  --lx-accent-11: #89b4fa; /* page references */
}

/* Font customization */
:root {
  --ls-font-family: 'JetBrains Mono', monospace;
  --ls-page-text-size: 15px;
}

/* Block styling */
.bullet-container .bullet {
  background-color: #cba6f7;
}

/* Tag styling */
a.tag {
  background-color: rgba(137, 180, 250, 0.2);
  border-radius: 4px;
  padding: 1px 4px;
}
```

`--lx-accent-11` is Logseq's accent shade, so changing it may also recolor other accent-colored UI.

---

## Troubleshooting

### CSS Not Loading

1. Check that `:custom-css-url` is not set in `config.edn`. If it is set, `logseq/custom.css` is ignored ([`src/main/frontend/handler/ui.cljs:114-116`](../../../../../src/main/frontend/handler/ui.cljs)).
2. Make sure you edited `logseq/custom.css` inside Logseq (Settings or search), not a file on disk.
3. Reload Logseq. On desktop, use **View > Reload**.

### Styles Not Applying

- Check selector specificity. See [How Theme Colors Resolve](#how-theme-colors-resolve-read-this-first).
- Check whether a `--ls-*` variable is only a fallback behind an `--lx-*` variable.
- Add `!important` if needed (use sparingly)
- Verify element class names in DevTools

### Broken Layout

- Avoid `display: none` on critical elements
- Test changes incrementally
- Keep a backup of working CSS

---

## See Also

- **CSS Variables**: [css-variables.md](../../reference/configuration/css-variables.md)
- **Custom JavaScript**: [custom-js.md](custom-js.md)

---

_Last updated: 2026-09-11_
