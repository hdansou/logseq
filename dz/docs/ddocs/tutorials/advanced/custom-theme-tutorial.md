---
title: 'Build a Custom Theme'
status: 🟢 Stable
version: 'Logseq DB (shui/radix token era)'
quadrant: 'Tutorial'
platform: 'Desktop + Mobile'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
related-docs:
  - '[Custom CSS Guide](../../how-to/integrations/custom-css.md)'
  - '[Create a Theme](../../how-to/integrations/create-theme.md)'
  - '[CSS Variables Reference](../../reference/configuration/css-variables.md)'
tags: ['themes', 'CSS', 'customization', 'design']
---

# Build a Custom Theme

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

Create a complete custom theme for Logseq. This hands-on tutorial guides you through designing, implementing, and publishing a theme from scratch.

**What You'll Build:**

A custom theme package including:

- Color scheme (light + dark modes)
- Typography styles
- Component customizations
- Layout adjustments
- Theme configuration

**Prerequisites:**

- Basic CSS knowledge
- Understanding of CSS variables
- Code editor (VS Code recommended)
- Logseq installed

**Time:** 2-3 hours

---

## Table of Contents

1. [Setup](#setup)
2. [Project Structure](#project-structure)
3. [Color System](#color-system)
4. [Typography](#typography)
5. [Component Styling](#component-styling)
6. [Layout Customization](#layout-customization)
7. [Testing](#testing)
8. [Publishing](#publishing)
9. [Maintenance](#maintenance)

---

## Setup

### Create Theme Directory

**Step 1: Create Project Folder**

```bash
# Create theme directory
mkdir logseq-theme-modern
cd logseq-theme-modern

# Create required files
mkdir themes
touch themes/light.css themes/dark.css
touch package.json
touch README.md
touch LICENSE
```

**Step 2: Package Configuration**

Create `package.json`:

```json
{
  "name": "logseq-theme-modern",
  "version": "1.0.0",
  "description": "A modern, clean theme for Logseq",
  "author": "Your Name",
  "license": "MIT",
  "effect": true,
  "logseq": {
    "id": "logseq-theme-modern",
    "title": "Modern Theme",
    "icon": "./icon.png",
    "themes": [
      {
        "name": "Modern Light",
        "url": "./themes/light.css",
        "description": "Light mode theme",
        "mode": "light"
      },
      {
        "name": "Modern Dark",
        "url": "./themes/dark.css",
        "description": "Dark mode theme",
        "mode": "dark"
      }
    ]
  }
}
```

Three things in that manifest are easy to get wrong:

- **`effect` belongs at the top level, not inside `logseq`.** `LSPluginPkgConfig` (`libs/src/LSPlugin.ts:69-85`) has no `effect` key. It is picked from the package root by the "Pick legal attrs" list in `libs/src/LSPlugin.core.ts:696-711`, and marketplace installs write it onto the package root (`src/electron/electron/plugin.cljs:167`). A `logseq.effect` is silently ignored. (The in-repo guide `libs/guides/custom_theme_guide.md` shows it inside `logseq`; that is a defect in that guide.)
- **`icon` is a resource path, not an emoji.** It is resolved through `_resolveResourceFullUrl` (`libs/src/LSPlugin.core.ts:739`).
- **Ship one CSS file per mode.** The theme picker filters by `mode` against the app's current light/dark state (`src/main/frontend/components/plugins.cljs:94-96`). Themes with `mode` omitted are grouped as "light and dark" instead (`:98-101`). Pointing both entries at the same file means the dark entry ships light values.

Declaring `logseq.themes` is what marks the package as a theme: `this._options.theme = Boolean(logseq.theme || !!logseq.themes)` (`libs/src/LSPlugin.core.ts:740`).

**Step 3: Development Setup**

Install in Logseq:

```markdown
1. Open Logseq Desktop
2. Settings → Advanced → enable Developer mode
3. Open the Plugins dashboard (`t p`)
4. Click "Load unpacked plugin" and select your theme directory
5. Pick your theme from the Themes list
```

Developer mode is `developer-mode-row` (`src/main/frontend/components/settings.cljs:580-587`); `t p` is bound to `:ui/goto-plugins` (`src/main/frontend/modules/shortcut/config.cljs:464-466`); the button comes from the string `:plugin/load-unpacked` (`src/resources/dicts/en.edn:1264`, rendered at `src/main/frontend/components/plugins.cljs:833-837`). The Themes list is the `installed-themes` dialog (`src/main/frontend/components/plugins.cljs:79-124`).

---

## Project Structure

### File Organization

**Complete Structure:**

```
logseq-theme-modern/
├── themes/
│   ├── light.css      # Registered as mode "light"
│   └── dark.css       # Registered as mode "dark"
├── package.json       # Theme configuration
├── icon.png           # Referenced by logseq.icon
├── README.md          # Documentation
├── LICENSE            # License file
├── screenshots/       # Preview images
│   ├── light-mode.png
│   └── dark-mode.png
├── src/               # Optional: source files you build into themes/
│   ├── _variables.css
│   ├── _typography.css
│   ├── _components.css
│   └── _layout.css
└── .github/           # GitHub metadata
    └── workflows/
        └── release.yml
```

Selecting a theme ejects the previously injected one and appends a `<link rel="stylesheet">` to `document.head` (`injectTheme`, `libs/src/common.ts:595-610`, called from `selectTheme`, `libs/src/LSPlugin.core.ts:1858-1884`). Because it lands after all app CSS, equal-specificity rules in your file win. Relative URLs inside it (fonts, images) resolve relative to the CSS file, so package your assets alongside it.

### Design System Document

Create `DESIGN.md`:

```markdown
# Modern Theme Design System

## Color Palette

**Light Mode:**

- Primary: #2B6CB0 (Blue)
- Background: #FFFFFF
- Surface: #F7FAFC
- Text: #1A202C

**Dark Mode:**

- Primary: #63B3ED (Light Blue)
- Background: #1A202C
- Surface: #2D3748
- Text: #F7FAFC

## Typography

- Font Family: 'Inter', system-ui
- Base Size: 15px
- Line Height: 1.6
- Headings: 'Inter', bold

## Spacing

- Scale: 0.25rem base
- Sizes: xs(0.25), sm(0.5), md(1), lg(1.5), xl(2)
```

---

## Color System

### CSS Variables Setup

Logseq's UI reads **three** families of custom properties, and a theme that sets only the first will look half-finished:

| Family        | Where it lives                                          | Format                          |
| ------------- | ------------------------------------------------------- | ------------------------------- |
| `--ls-*`      | Logseq's own semantic variables                          | Any CSS color                   |
| shui/Tailwind | `--background`, `--foreground`, `--border`, `--primary`… | **Space-separated HSL channels** |
| `--lx-*`      | The active neutral/accent scales (`--lx-gray-01…12`, `--lx-accent-01…12`) | Any CSS color |

`src/main/frontend/common.css:33-34` shows all three in one declaration, with the fallback chain the app relies on:

```css
body {
  color: var(--lx-gray-12, var(--ls-primary-text-color, hsl(var(--foreground))));
  background-color: var(--lx-gray-01, var(--ls-primary-background-color, hsl(var(--background))));
}
```

Note `hsl(var(--foreground))` — shui tokens are consumed *inside* `hsl()`, so you must write channels (`210 40% 96%`), never a full color. Writing `--primary: hsl(199 89% 48%)` produces `hsl(hsl(...))` and silently fails.

**Which selector to scope to.** `frontend.components.theme` sets `data-theme` on the document element and maintains compatibility classes on `body` (`src/main/frontend/components/theme.cljs:52-56`):

```clojure
(.setAttribute doc "data-theme" theme)
...
(do (.add cls "dark") (doto cls-body (.remove "white-theme" "light-theme") (.add "dark-theme")))
(do (.remove cls "dark") (doto cls-body (.remove "dark-theme") (.add "white-theme" "light-theme")))
```

So `html[data-theme='light'|'dark']` identifies the mode, but on its own it **loses**: core declares its defaults on `html[data-theme=…][data-color=logseq]` (`src/main/frontend/shui/vars-classic.css:42`, `:100`) and, for other accents, on `body` (`src/main/frontend/shui/colors.css:57-58`). Scope variables to `html[data-theme='…'][data-color], html[data-theme='…'][data-color] body` (verified live 2026-09-29 with accents logseq/tomato/blue/none; see the [DB theme walkthrough](../../../05-developer-guides/theme-development/db-theme-walkthrough.md)), which the snippets below use; `.white-theme` / `.light-theme` / `.dark-theme` are **body** classes kept for backward compatibility (the source comment points at logseq/logseq#4652). Do not write `.white-theme, html[data-theme='light'] { ... }` as one rule — they match different elements, and variables declared on `body` will not be visible to rules that resolve them on `html`.

**Step 1: Define Color Palette**

In `themes/light.css`:

```css
/* ============================
   MODERN THEME - COLOR SYSTEM
   ============================ */

/* Light Mode Variables */
html[data-theme='light'][data-color],
html[data-theme='light'][data-color] body {
  color-scheme: light;
  /* Primary Colors */
  --ls-primary-background-color: #ffffff;
  --ls-secondary-background-color: #f7fafc;
  --ls-tertiary-background-color: #edf2f7;
  --ls-quaternary-background-color: #e2e8f0;

  /* Text Colors */
  --ls-primary-text-color: #1a202c;
  --ls-secondary-text-color: #4a5568;
  --ls-title-text-color: #1a202c;
  --ls-link-text-color: #2b6cb0;
  --ls-link-text-hover-color: #2c5282;

  /* Accent Colors */
  --ls-active-primary-color: #2b6cb0;
  --ls-active-secondary-color: #63b3ed;

  /* Border Colors */
  --ls-border-color: #e2e8f0;
  --ls-secondary-border-color: #cbd5e0;

  /* Block Colors */
  --ls-block-highlight-color: #ebf8ff;
  --ls-block-ref-link-text-color: #2b6cb0;

  /* Tag Colors */
  --ls-tag-text-color: #2c5282;
  --ls-tag-text-hover-color: #1a365d;

  /* Inline code (these are the real variable names) */
  --ls-page-inline-code-bg-color: #f7fafc;
  --ls-page-inline-code-color: #2d3748;

  /* Scrollbar */
  --ls-scrollbar-background-color: #edf2f7;
  --ls-scrollbar-foreground-color: rgb(15 23 42 / 0.12);
  --ls-scrollbar-thumb-hover-color: #a0aec0;

  /* shui/Tailwind tokens — HSL channels, no hsl() wrapper */
  --background: 0 0% 100%;
  --foreground: 222 47% 11%;
  --card: 0 0% 100%;
  --card-foreground: 222 47% 11%;
  --popover: 0 0% 100%;
  --popover-foreground: 222 47% 11%;
  --primary: 211 61% 43%;
  --primary-foreground: 0 0% 100%;
  --secondary: 210 40% 96%;
  --secondary-foreground: 222 47% 11%;
  --muted: 210 40% 96%;
  --border: 214 32% 91%;
  --input: 214 32% 91%;
  --ring: 211 61% 43%;
  --accent: 211 61% 43%;
  --accent-foreground: 0 0% 100%;
}
```

In `themes/dark.css`:

```css
/* Dark Mode Variables */
html[data-theme='dark'][data-color],
html[data-theme='dark'][data-color] body {
  color-scheme: dark;

  /* Primary Colors */
  --ls-primary-background-color: #1a202c;
  --ls-secondary-background-color: #2d3748;
  --ls-tertiary-background-color: #374151;
  --ls-quaternary-background-color: #4a5568;

  /* Text Colors */
  --ls-primary-text-color: #f7fafc;
  --ls-secondary-text-color: #cbd5e0;
  --ls-title-text-color: #ffffff;
  --ls-link-text-color: #63b3ed;
  --ls-link-text-hover-color: #90cdf4;

  /* Accent Colors */
  --ls-active-primary-color: #63b3ed;
  --ls-active-secondary-color: #3182ce;

  /* Border Colors */
  --ls-border-color: #4a5568;
  --ls-secondary-border-color: #2d3748;

  /* Block Colors */
  --ls-block-highlight-color: #2c5282;
  --ls-block-ref-link-text-color: #63b3ed;

  /* Tag Colors */
  --ls-tag-text-color: #90cdf4;
  --ls-tag-text-hover-color: #bee3f8;

  /* Inline code */
  --ls-page-inline-code-bg-color: #2d3748;
  --ls-page-inline-code-color: #e2e8f0;

  /* Scrollbar */
  --ls-scrollbar-background-color: #2d3748;
  --ls-scrollbar-foreground-color: rgb(148 163 184 / 0.35);
  --ls-scrollbar-thumb-hover-color: #4a5568;

  /* shui/Tailwind tokens — HSL channels */
  --background: 222 47% 11%;
  --foreground: 210 40% 96%;
  --card: 222 47% 13%;
  --card-foreground: 210 40% 96%;
  --popover: 222 47% 10%;
  --popover-foreground: 210 40% 96%;
  --primary: 205 78% 66%;
  --primary-foreground: 222 47% 11%;
  --secondary: 217 33% 18%;
  --secondary-foreground: 210 40% 96%;
  --muted: 217 33% 16%;
  --border: 217 33% 24%;
  --input: 217 33% 24%;
  --ring: 205 78% 66%;
  --accent: 205 78% 66%;
  --accent-foreground: 222 47% 11%;
}
```

**Step 1b: Map the `--lx-*` scales**

Newer shui-heavy UI reads the active neutral and accent scales rather than `--ls-*`. If your theme is a complete palette, map them too, or menus, popovers, and the command palette will keep the stock neutrals:

```css
html[data-theme='dark'][data-color],
html[data-theme='dark'][data-color] body {
  --lx-gray-01: #1a202c;
  --lx-gray-02: #1f2733;
  --lx-gray-03: #2d3748;
  --lx-gray-04: #374151;
  --lx-gray-05: #4a5568;
  --lx-gray-06: #5a6478;
  --lx-gray-07: #718096;
  --lx-gray-08: #94a3b8;
  --lx-gray-09: #a0aec0;
  --lx-gray-10: #cbd5e0;
  --lx-gray-11: #e2e8f0;
  --lx-gray-12: #f7fafc;

  --lx-accent-09: #3182ce;
  --lx-accent-10: #2b6cb0;
  --lx-accent-11: #63b3ed;
  --lx-accent-12: #ebf8ff;
}
```

The scales themselves are defined by the accent palettes in `src/main/frontend/shui/colors.css`, selected by `html[data-color=...]`, over Radix raw palettes (`--rx-*`) in `src/main/frontend/shui/radix.css` and `radix-hsl.css`. Semantic `--ls-*` defaults and layout variables live in `src/main/frontend/shui/vars-classic.css`.

**Step 2: Semantic Color Tokens**

Add semantic naming:

```css
/* Semantic Color System */
:root {
  /* Status Colors */
  --color-success: #48bb78;
  --color-warning: #ed8936;
  --color-error: #f56565;
  --color-info: #4299e1;

  /* Task Colors */
  --color-todo: #4299e1;
  --color-doing: #ed8936;
  --color-done: #48bb78;
  --color-waiting: #a0aec0;
  --color-canceled: #718096;

  /* Priority Colors */
  --color-priority-a: #f56565;
  --color-priority-b: #ed8936;
  --color-priority-c: #ecc94b;
}
```

**Step 3: Apply to Tasks and Checkboxes**

> **DB graphs have no `TODO` / `DOING` / `DONE` markers.** Task state is the closed-value property `:logseq.property/status`, whose values are the idents `:logseq.property/status.backlog`, `.todo`, `.doing`, `.in-review`, `.done`, `.canceled` (`deps/db/src/logseq/db/frontend/property.cljs:300-318`). It is declared with `:ui-position :block-left`, so it renders through `block-positioned-properties` (`src/main/frontend/components/block.cljs:1302`), as a property value with a tabler icon — not as a text marker.
>
> `.block-marker` and `.marker-switch` still exist in `src/main/frontend/components/block.css:965-978`, but nothing in the ClojureScript renders them any more; they are file-graph leftovers. There is also no `data-ref="TODO"` attribute: `data-ref` is set to a page name on page references (`src/main/frontend/components/block.cljs:815`) and to a uuid-or-title on block references (`:1292`) — never to a marker keyword. Any theme rule keyed on `[data-ref='TODO']` is dead CSS.

Style the checkbox and the property surfaces instead:

```css
/* Markdown task checkboxes / radios */
.form-checkbox,
.form-radio {
  border-radius: 4px;
  border-width: 2px;
}
```

`.form-checkbox` and `.form-radio` are styled in `src/main/frontend/components/theme.css:56-59` and driven by `--ls-page-checkbox-color` and `--ls-page-checkbox-border-color`, so prefer setting those variables:

```css
html[data-theme='light'][data-color],
html[data-theme='light'][data-color] body {
  --ls-page-checkbox-color: #718096;
  --ls-page-checkbox-border-color: #cbd5e0;
}
```

For the status chip itself, target the positioned-property surfaces described under [Component Styling](#component-styling) — `.ls-properties-area`, `.property-pair`, `.property-value-content` — rather than inventing marker selectors.

---

## Typography

### Font System

**Step 1: Know which typography variables Logseq actually reads**

Only these exist in the app's CSS. Verify any `--ls-*` name you are about to use with `git grep -n -- '--ls-' -- '**/*.css'` before relying on it:

| Variable                                              | What it controls                                             |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| `--ls-font-family`                                     | Global app font (`src/main/frontend/common.css:17-21`)        |
| `--ls-page-text-size`                                  | Base page text size                                           |
| `--ls-page-title-size`, `--ls-page-title-line-height`  | Page title                                                    |
| `--ls-main-content-max-width`, `--ls-main-content-max-width-wide` | Content column width                              |
| `--ls-mobile-font-size`, `--ls-mobile-font-scale`      | Mobile text sizing                                            |
| `--ls-page-inline-code-color`, `--ls-page-inline-code-bg-color` | Inline code                                        |

> **There is no Logseq-provided type scale.** Names of the shape `--ls-font-size-*`, `--ls-font-weight-*`, `--ls-line-height-*` and `--ls-code-font-family` do **not** exist anywhere in the app's stylesheets — the full set of `--ls-*` variables at `be800f1711` is 114 names, and none of those are among them. Defining them under an `--ls-` prefix is harmless but misleading: nothing in Logseq consumes them, and the prefix implies otherwise. Use your own namespace for private tokens — this tutorial uses `--mt-*` ("modern theme") below.

**Step 2: Import Fonts and Declare Private Tokens**

```css
/* Import Google Fonts.
   Remote @import blocks first paint and fails offline; prefer bundling
   the .woff2 files with the theme and using @font-face. */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&display=swap');

html[data-theme='light'],
html[data-theme='dark'] {
  /* --- Logseq variables (the app reads these) --- */
  --ls-font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
    'Helvetica Neue', Arial, sans-serif;
  --ls-page-text-size: 0.9375rem; /* 15px */

  /* --- Private theme tokens (only this stylesheet reads these) --- */
  --mt-code-font-family: 'JetBrains Mono', 'Fira Code', Menlo, Monaco,
    'Courier New', monospace;

  --mt-font-size-xs: 0.75rem; /* 12px */
  --mt-font-size-sm: 0.875rem; /* 14px */
  --mt-font-size-base: 0.9375rem; /* 15px */
  --mt-font-size-md: 1rem; /* 16px */
  --mt-font-size-lg: 1.125rem; /* 18px */
  --mt-font-size-xl: 1.25rem; /* 20px */
  --mt-font-size-2xl: 1.5rem; /* 24px */
  --mt-font-size-3xl: 1.875rem; /* 30px */
  --mt-font-size-4xl: 2.25rem; /* 36px */

  --mt-line-height-tight: 1.4;
  --mt-line-height-base: 1.6;
  --mt-line-height-relaxed: 1.8;

  --mt-font-weight-light: 300;
  --mt-font-weight-normal: 400;
  --mt-font-weight-medium: 500;
  --mt-font-weight-semibold: 600;
  --mt-font-weight-bold: 700;
}
```

> The app also exposes `html[data-font='serif'|'mono']` and `html[data-font-global='true']` from the user's own font preference. Avoid fighting those unless your theme deliberately owns typography.

**Step 2: Apply Typography**

```css
/* Base Typography */
body {
  font-family: var(--ls-font-family);
  font-size: var(--mt-font-size-base);
  line-height: var(--mt-line-height-base);
  font-weight: var(--mt-font-weight-normal);
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

/* Headings */
h1.title,
h1,
.text-xl {
  font-size: var(--mt-font-size-4xl);
  font-weight: var(--mt-font-weight-bold);
  line-height: var(--mt-line-height-tight);
  margin-top: 2rem;
  margin-bottom: 1rem;
}

h2,
.text-lg {
  font-size: var(--mt-font-size-3xl);
  font-weight: var(--mt-font-weight-bold);
  line-height: var(--mt-line-height-tight);
  margin-top: 1.5rem;
  margin-bottom: 0.75rem;
}

h3 {
  font-size: var(--mt-font-size-2xl);
  font-weight: var(--mt-font-weight-semibold);
  line-height: var(--mt-line-height-tight);
  margin-top: 1.25rem;
  margin-bottom: 0.5rem;
}

h4 {
  font-size: var(--mt-font-size-xl);
  font-weight: var(--mt-font-weight-semibold);
  line-height: var(--mt-line-height-base);
}

h5 {
  font-size: var(--mt-font-size-lg);
  font-weight: var(--mt-font-weight-medium);
}

h6 {
  font-size: var(--mt-font-size-md);
  font-weight: var(--mt-font-weight-medium);
}
```

**Step 3: Content Typography**

```css
/* Paragraphs */
.block-content p {
  margin-bottom: 0.75rem;
  line-height: var(--mt-line-height-relaxed);
}

/* Links */
a {
  color: var(--ls-link-text-color);
  text-decoration: none;
  transition: color 0.15s ease;
}

a:hover {
  color: var(--ls-link-text-hover-color);
  text-decoration: underline;
}

/* Code */
code {
  font-family: var(--mt-code-font-family);
  font-size: 0.9em;
  padding: 0.2em 0.4em;
  background-color: var(--ls-page-inline-code-bg-color);
  border-radius: 4px;
}

pre code {
  padding: 0;
  background-color: transparent;
}

/* Block References */
.block-ref {
  font-weight: var(--mt-font-weight-medium);
  border-bottom: 1px dotted var(--ls-border-color);
}

/* Tags */
a.tag {
  font-size: var(--mt-font-size-sm);
  font-weight: var(--mt-font-weight-medium);
  padding: 2px 8px;
  border-radius: 6px;
  background-color: var(--ls-tertiary-background-color);
}
```

---

## Component Styling

> **Verify every selector before you rely on it.** Themes rot mostly by accumulating rules for elements that no longer exist. `git grep -nF '.your-selector' -- '*.css' '*.cljs'` at the revision you are targeting is a two-second check, and DevTools on a live block is the other half. Every selector below was checked against `be800f1711`; the ones that do **not** exist are called out explicitly so you can recognise the pattern.

### Blocks

**Step 1: Block Container**

```css
/* Block Container */
.ls-block {
  padding: 4px 0;
  border-radius: 6px;
  transition: background-color 0.15s ease;
}

/* Block Hover */
.ls-block:hover {
  background-color: var(--ls-tertiary-background-color);
}

/* Block Selected */
.ls-block.selected {
  background-color: var(--ls-block-highlight-color);
}

/* Block Content */
.block-content-wrapper {
  position: relative;
  padding-left: 28px;
}

/* Bullet */
.bullet-container {
  width: 20px;
  height: 20px;
  margin-right: 8px;
}

.bullet {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background-color: var(--ls-active-primary-color);
  transition: transform 0.15s ease;
}

.bullet:hover {
  transform: scale(1.5);
  background-color: var(--ls-link-text-color);
}
```

**Step 2: Block References**

```css
/* Block Reference Styling */
.block-ref {
  color: var(--ls-block-ref-link-text-color);
  background-color: transparent;
  border-bottom: 1px solid var(--ls-border-color);
  padding: 2px 4px;
  border-radius: 4px;
  transition: all 0.15s ease;
}

.block-ref:hover {
  background-color: var(--ls-tertiary-background-color);
  border-bottom-color: var(--ls-link-text-color);
}
```

`.block-ref` is real (`src/main/frontend/components/block.css:846`), as is `.block-ref-no-title` and `.open-block-ref-link`.

> **There is no `.block-ref-count` class.** The reference-count badge is a shui ghost button carrying Tailwind utility classes, built in `block-refs-count` (`src/main/frontend/components/block.cljs:3551-3567`). `:block-ref-count` appears in the source only as a resource key for `db-hooks/use-resource` (`src/main/frontend/components/page.cljs:560`), never as a CSS class. Style it via the button tokens (`--primary`, `--accent`) instead, and keep the hit area intact.

**Step 3: Block Properties**

The property panel is `.ls-properties-area`, with `.ls-block-properties` added for block-level (rather than page-level) panels (`src/main/frontend/components/property.cljs:998`; styled at `src/main/frontend/components/property.css:36`, `:95`, `:110`). Rows are `.property-pair`, split into `.property-key-panel` and `.property-value-panel`, and rows carry `[data-property-type=...]`:

```css
/* Block-level property panel */
.ls-block .ls-properties-area.ls-block-properties {
  margin-top: 8px;
  padding: 8px 12px;
  border-left: 3px solid var(--ls-active-primary-color);
  border-radius: 6px;
  font-size: var(--mt-font-size-sm);
}

/* Property row */
.ls-properties-area .property-pair {
  align-items: center;
}

/* Property key */
.ls-properties-area .property-key-panel .property-k {
  font-weight: var(--mt-font-weight-semibold);
  color: var(--ls-primary-text-color);
}

/* Property value */
.ls-properties-area .property-value-content {
  color: var(--ls-secondary-text-color);
}

/* Node-typed values get their own layout */
.ls-properties-area .property-pair[data-property-type='node'] .property-value-panel {
  /* ... */
}
```

Prefer the variables first: the panel background is `--ls-block-properties-background-color` for blocks and `--ls-page-properties-background-color` for pages (`src/main/frontend/components/block.css:959-962`).

> **`.block-property`, `.block-property-key` and `.block-property-value` do not exist** in the app's CSS or ClojureScript. `.block-properties` does exist (`block.css:959`) but is the outer container only.

### Sidebar

**Sidebar Layout:**

```css
/* Left Sidebar. Prefer the variables:
   --ls-left-sidebar-width, --ls-left-sidebar-sm-width,
   --ls-left-sidebar-text-color, --ls-left-sidebar-nav-btn-size,
   and --left-sidebar-bg-color (set on main.theme-container-inner). */
#left-sidebar {
  border-right: 1px solid var(--ls-border-color);
}

.left-sidebar-inner {
  color: var(--ls-left-sidebar-text-color);
}

/* Sidebar header row */
.left-sidebar-inner .sidebar-header-container {
  padding: 16px 20px;
  border-bottom: 1px solid var(--ls-border-color);
}

/* Grouped content (Favorites, Recent, …) */
.left-sidebar-inner .sidebar-content-group .sidebar-content-group-inner > .hd {
  border-radius: 8px;
}

.left-sidebar-inner .sidebar-content-group-inner > .bd ul li > a:first-of-type:hover {
  background-color: var(--ls-menu-hover-color);
}

/* Right Sidebar */
#right-sidebar {
  background-color: var(--ls-primary-background-color);
  border-left: 1px solid var(--ls-border-color);
}

/* Sidebar Section */
.sidebar-item {
  padding: 16px;
  border-bottom: 1px solid var(--ls-border-color);
}

.sidebar-item-header {
  font-weight: var(--mt-font-weight-semibold);
  font-size: var(--mt-font-size-lg);
  margin-bottom: 12px;
}
```

Verified against `src/main/frontend/components/container.css`: `#left-sidebar` (`:340`), `.left-sidebar-inner` (`:71`), `.sidebar-header-container` (`:137`), `.sidebar-contents-container` (`:141`), `.sidebar-content-group` (`:150`), `.sidebar-item` (`:620`), `.sidebar-item-header` (`:623`). `.nav-header` is a real hook, but only as a mobile-navigation click target (`src/main/frontend/components/left_sidebar_util.cljs:10`), with no styling attached.

> **`.nav-content-item` is not a left-sidebar class.** It exists only inside the onboarding tour stylesheet (`src/main/frontend/components/onboarding/index.css:468-474`). Styling it will not touch the sidebar.

### Editor

**Editor Styling:**

```css
/* Editor Container */
.editor-inner {
  padding: 12px 16px;
  background-color: var(--ls-primary-background-color);
  border-radius: 6px;
  border: 2px solid var(--ls-active-primary-color);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

/* Editor Textarea */
.editor-inner textarea {
  font-family: var(--ls-font-family);
  font-size: var(--mt-font-size-base);
  line-height: var(--mt-line-height-relaxed);
  color: var(--ls-primary-text-color);
}

/* Placeholder */
.editor-inner textarea::placeholder {
  color: var(--ls-secondary-text-color);
  opacity: 0.6;
}

/* Autocomplete / slash popup that appears while editing */
#ui__ac-inner .menu-link:hover,
.cp__commands-slash .menu-link:hover {
  background-color: var(--ls-menu-hover-color);
}
```

> **There is no `.editor-toolbar` in the desktop UI.** The only editor toolbar is the mobile one, built in `src/main/mobile/components/editor_toolbar.cljs` and rendered through a Capacitor native plugin (`mobile-util/native-editor-toolbar`, `:209`), not as themeable DOM. `.editor-toolbar-btn` does not exist anywhere.

Real editor-adjacent hooks: `.editor-inner textarea`, `.edit-input`, `.non-block-editor textarea`, `#ui__ac-inner`, `.menu-link`, `.absolute-modal[data-modal-name]`, `.cp__commands-slash`.

### Search

Search in Logseq DB is the **command palette / cmdk** surface, not a standalone search box.

```css
/* Command palette shell */
.cp__cmdk {
  border-color: var(--ls-border-color);
}

/* Search input */
.cp__cmdk-search-input,
.cp__cmdk-search-input:focus,
.cp__cmdk-search-input:focus-visible {
  font-size: var(--mt-font-size-base);
  outline: none;
}

.cp__cmdk-input-row:focus-within {
  border-color: var(--ls-active-primary-color);
}
```

The classes live in `src/main/frontend/components/cmdk/cmdk.css:1-21`, with additional rules in `resources/css/shui.css:91` and `:193`. Highlighted rows are driven by `--accent` and `--lx-gray-*`, so set those rather than writing row-level rules.

> **`#search-field`, `.search-results` and `.search-result-item` do not exist** in the app's CSS. `add-search-result-item-listener!` in `src/main/mobile/bottom_tabs.cljs:93` is a ClojureScript function name, not a class. Rules keyed on those selectors are dead.

### Modals

Dialogs are shui/Base UI components. The real selectors are `.ui__dialog-overlay` and `.ui__dialog-content`, and both take a `[label=...]` attribute naming the specific dialog:

```css
/* Overlay */
.ui__dialog-overlay {
  background-color: rgba(0, 0, 0, 0.5);
  backdrop-filter: blur(4px);
}

/* Panel */
.ui__dialog-content {
  border-radius: 12px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
}

/* Target one dialog by its label */
.ui__dialog-content[label='app-settings'] {
  max-width: 900px;
}

/* Buttons inside dialogs are shui buttons driven by tokens */
.ui__button {
  font-weight: var(--mt-font-weight-semibold);
}
```

Declared in `resources/css/shui.css:224-260`, `resources/css/ui.css:332`, and `src/main/frontend/ui.css:53`. Labelled examples in the tree include `app-settings` (`src/main/frontend/components/container.css:494`), `plugin-settings-modal`, `plugins-dashboard`, `plugin-readme` (`src/main/frontend/components/plugins.css:967-979`), `customize-appearance` (`src/main/frontend/components/settings.css:476-480`) and `user-login` (`src/main/frontend/components/user/login.css:30`).

Button colours come from the shui tokens you set in the Color System section — `--primary`, `--primary-foreground`, `--secondary`, `--accent`, `--ring` — not from per-button rules.

> **`.ui__modal-overlay`, `.ui__modal-panel-header`, `.ui__modal-panel-content`, `.ui__modal-panel-footer`, `.button-primary` and `.button-secondary` do not exist** in the app's CSS. `.ui__modal-panel` does exist, but only inside one nested block at `resources/css/shui.css:138`.

---

## Layout Customization

### Page Layout

**Main Container:**

```css
/* App Container */
#app-container {
  display: flex;
  height: 100vh;
  overflow: hidden;
}

/* Main Content Area */
#main-content-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  background-color: var(--ls-primary-background-color);
}

/* Main Content */
#main-content {
  flex: 1;
  overflow-y: auto;
  padding: 32px 48px;
  max-width: 900px;
  margin: 0 auto;
  width: 100%;
}

/* Narrow Layout (for small screens) */
@media (max-width: 768px) {
  #main-content {
    padding: 16px 20px;
  }
}
```

`#app-container` (`src/main/frontend/components/container.css:12`), `#main-content` (`:52`) and `#main-content-container` (`:57`) are all real; `#main-content-container` is also referenced from `src/main/frontend/components/block.css:1059`.

**Prefer the width variables over a hard-coded `max-width`.** The content column width is applied to `.cp__sidebar-main-content`, which uses `--ls-main-content-max-width` normally (`container.css:503`) and `--ls-main-content-max-width-wide` in wide mode (`:490`). Defaults are `960px` and `1440px` (`src/main/frontend/shui/vars-classic.css:6-7`). Setting a `max-width` on `#main-content` instead fights that switch:

```css
html[data-theme='light'],
html[data-theme='dark'] {
  --ls-main-content-max-width: 900px;
  --ls-main-content-max-width-wide: 1100px;
}
```

### Page Header

**Header Styling:**

Title sizing is a variable, not a font-size rule. `--ls-page-title-size` (default `32px`) and `--ls-page-title-line-height` drive `h1.title` and `.ls-page-title-container` (`src/main/frontend/common.css:334-341`); mobile overrides `--ls-page-title-size` on its own (`src/main/frontend/components/block.css:737`), so setting the variable keeps both platforms consistent.

```css
html[data-theme='light'],
html[data-theme='dark'] {
  --ls-page-title-size: 2.25rem;
  --ls-page-title-line-height: 1.2;
}

/* The clickable page title link */
a.page-title {
  color: var(--ls-title-text-color);
}

/* Page-level property panel (page? branch of the same component as block props) */
.ls-page-properties > .ls-properties-area > .property-block {
  border-radius: 8px;
}
```

Real selectors here: `h1.title` and `.ls-page-title-container` (`common.css:334`), `.ls-page-title` (`block.css:681`), `a.page-title` and `.page-title-sizer-wrapper` (`src/main/frontend/components/page.css:80, 88`), `.ls-page-properties` (`src/main/frontend/components/property.cljs:997`, styled at `property.css:28` and `:492`), `.journal-title`, `.breadcrumb`.

> **`.page-header`, `.page-properties-area` and `.page-property-item` do not exist as CSS classes.** `page-header` in `src/main/frontend/components/shortcut.cljs:242` is a local binding for `page-el.querySelector(":scope > header")`, and `page-properties-area?` in `src/main/frontend/components/property.cljs:957` is a boolean, not a class name. A bare `.page-title` rule *does* match, but only via `a.page-title`; the app has no standalone `.page-title` block element.

### Spacing System

**Consistent Spacing:**

```css
/* Spacing Utilities */
:root {
  --spacing-xs: 0.25rem; /* 4px */
  --spacing-sm: 0.5rem; /* 8px */
  --spacing-md: 1rem; /* 16px */
  --spacing-lg: 1.5rem; /* 24px */
  --spacing-xl: 2rem; /* 32px */
  --spacing-2xl: 3rem; /* 48px */
  --spacing-3xl: 4rem; /* 64px */
}

/* Apply Spacing to Blocks */
.ls-block {
  padding-top: var(--spacing-xs);
  padding-bottom: var(--spacing-xs);
  margin-bottom: var(--spacing-xs);
}

.ls-block h1 {
  margin-top: var(--spacing-xl);
  margin-bottom: var(--spacing-lg);
}

.ls-block h2 {
  margin-top: var(--spacing-lg);
  margin-bottom: var(--spacing-md);
}

.ls-block h3 {
  margin-top: var(--spacing-md);
  margin-bottom: var(--spacing-sm);
}

/* Section Spacing */
.page-blocks-inner {
  padding: var(--spacing-lg) 0;
}

.sidebar-item {
  padding: var(--spacing-md);
  margin-bottom: var(--spacing-sm);
}
```

### Responsive Design

**Mobile Optimizations:**

Logseq already exposes platform hooks on `html` — `is-mobile`, `is-electron`, `is-native-iphone`, `is-native-android` and friends — set by `frontend.ui/inject-document-devices-envs!` (`src/main/frontend/ui.cljs:344-358`). Prefer those over guessing at viewport widths, and prefer the mobile text variables over a font-size override; mobile body text is `calc(var(--ls-mobile-font-size) * var(--ls-mobile-font-scale, 1))` (`src/main/mobile/components/app.css:9-15`):

```css
/* Platform hook, not a width guess */
html.is-mobile {
  --ls-mobile-font-size: 16px;
  --ls-page-title-size: 1.75rem;
  --mt-font-size-base: 0.875rem;
}

/* Mobile: < 768px */
@media (max-width: 767px) {
  #main-content {
    padding: 16px;
  }

  .block-content-wrapper {
    padding-left: 20px;
  }
}

/* Tablet: 768px - 1023px */
@media (min-width: 768px) and (max-width: 1023px) {
  #main-content {
    padding: 24px 32px;
    max-width: 720px;
  }

  /* Adjust sidebar width */
  #left-sidebar {
    width: 220px;
  }
}

/* Desktop: >= 1024px */
@media (min-width: 1024px) {
  /* Default styles already optimized for desktop */
}
```

---

## Testing

### Manual Testing Checklist

**Visual Testing:**

```markdown
## Theme Testing Checklist

### Colors

- [ ] Light mode: all backgrounds visible
- [ ] Dark mode: all backgrounds visible
- [ ] Light/dark mode switching works, and each mode keeps its own theme selection
- [ ] Link colors have sufficient contrast
- [ ] Task status chips (Backlog / Todo / Doing / In Review / Done / Canceled) legible
- [ ] Tags styled appropriately
- [ ] shui tokens set: menus, popovers, dropdowns, command palette, dialogs
- [ ] `--lx-gray-*` / `--lx-accent-*` mapped, or stock neutrals verified as acceptable
- [ ] Default accent plus at least one non-default `html[data-color=...]`

### Typography

- [ ] Fonts load correctly
- [ ] Font sizes appropriate
- [ ] Line height comfortable
- [ ] Code font monospaced
- [ ] Headings hierarchy clear

### Components

- [ ] Blocks: hover, selected states
- [ ] Bullet: hover animation, hit area not shrunk
- [ ] Block references and page references styled
- [ ] Properties display correctly (`.ls-properties-area`, block and page variants)
- [ ] Tags render properly
- [ ] Code blocks / CodeMirror (`.cm-s-lsradix`), inline code, marks, blockquotes, tables
- [ ] Command palette (`.cp__cmdk`) readable, including the highlighted row
- [ ] Slash menu and autocomplete (`#ui__ac-inner`, `.cp__commands-slash`)

### Layout

- [ ] Main content centered
- [ ] Sidebars functional
- [ ] Modal dialogs centered
- [ ] Page header formatted
- [ ] Responsive on mobile
- [ ] Scrolling smooth

### Interactions

- [ ] Links clickable
- [ ] Buttons hover effects
- [ ] Editor focus visible
- [ ] Transitions smooth
- [ ] Modals open/close
```

**Test in Different Contexts:**

```markdown
1. **Create test page:**

   - Different heading levels
   - Lists (ordered, unordered, tasks)
   - Code blocks (inline and block)
   - Tables
   - Block references
   - Properties
   - Tags

2. **Test workflows:**

   - Create new page
   - Edit blocks
   - Search content
   - Open modals
   - Switch themes
   - Zoom in/out

3. **Test on different devices:**

   - Desktop (various sizes)
   - Tablet
   - Mobile

4. **Test edge cases:**
   - Very long page titles
   - Deeply nested blocks
   - Many properties
   - Large code blocks
```

### Browser DevTools

**Inspect Elements:**

```markdown
1. **Open DevTools** (F12 or Cmd+Option+I)

2. **Confirm your stylesheet is actually loaded:**

   - Look for your `<link rel="stylesheet">` at the end of `document.head`
   - If it is missing, the theme was not selected — check `logseq.themes` and the `mode` values

3. **Read the runtime attributes on `document.documentElement`:**

   - `data-theme` — `light` or `dark`; this is the selector your rules must match
   - `data-color` — the active accent (`logseq` by default)
   - `data-font`, `data-font-global` — the user's font preference
   - platform classes — `is-mobile`, `is-electron`, `is-native-iphone`, …

4. **Check CSS Variables:**

   - Inspect `html`, `body`, `.theme-container`, and the specific element you are styling
   - Read computed values, not just your declarations
   - If a variable has no effect, the component is probably reading a `--lx-*` scale or a shui HSL token instead of `--ls-*`

5. **Confirm the selector exists before writing a rule for it:**

   - Right-click the element → Inspect, and read its real classes
   - Cross-check with `git grep -nF '.the-class' -- '*.css' '*.cljs'`

6. **Test Responsive:**

   - Toggle device toolbar
   - Test various screen sizes
   - Check mobile breakpoints

7. **Check Contrast:**

   - Use Lighthouse accessibility audit
   - Verify WCAG AA compliance (4.5:1 text)
   - Check focus indicators visible (`--ls-focus-ring-color`, `--ring`)

8. **Performance:**
   - Check CSS file size
   - Look for unused selectors
   - Avoid global resets such as `* { transition: all 200ms }` — they slow editing and break CodeMirror/PDF.js interaction states
```

### User Feedback

**Beta Testing:**

```markdown
1. **Share with community:**

   - Post in Logseq Discord
   - Share GitHub repository
   - Ask for feedback

2. **Gather feedback on:**

   - Visual appeal
   - Readability
   - Usability issues
   - Missing features
   - Browser compatibility

3. **Iterate:**
   - Fix reported bugs
   - Adjust based on feedback
   - Release updated versions
```

---

## Publishing

### Prepare for Release

**Step 1: Final Checklist**

```markdown
- [ ] All CSS validated
- [ ] Screenshots created
- [ ] README.md complete
- [ ] LICENSE added
- [ ] Version number set
- [ ] package.json configured
- [ ] Test on clean Logseq install
```

**Step 2: Create Screenshots**

Take screenshots showing:

- Light mode (full page)
- Dark mode (full page)
- Detailed views (blocks, properties, tags)
- Mobile view (if supported)

Save to `screenshots/` directory.

**Step 3: Write README**

Create comprehensive `README.md`:

````markdown
# Modern Theme for Logseq

A clean, modern theme for Logseq with carefully designed colors, typography, and components.

## Features

- 🎨 Beautiful light and dark modes
- ✨ Smooth transitions and animations
- 📱 Responsive design (mobile-friendly)
- 🔤 Custom typography (Inter + JetBrains Mono)
- 🎯 Enhanced task markers
- 🔗 Styled block references
- ⚡ Optimized performance

## Screenshots

### Light Mode

![Light Mode](screenshots/light-mode.png)

### Dark Mode

![Dark Mode](screenshots/dark-mode.png)

## Installation

### From Marketplace (Recommended)

1. Open Logseq
2. Go to Settings → Plugins → Marketplace
3. Search for "Modern Theme"
4. Click Install

### Manual Installation

1. Download latest release from GitHub
2. Extract to folder
3. In Logseq: Settings → Plugins → Load unpacked plugin
4. Select extracted folder

## Usage

1. After installation, go to Settings → Themes
2. Select "Modern Light" or "Modern Dark"
3. Enjoy!

## Customization

To customize colors:

1. Open your graph's `custom.css`
2. Override CSS variables, scoped to a mode:

```css
html[data-theme='dark'][data-color],
html[data-theme='dark'][data-color] body {
  --ls-primary-background-color: #YOUR-COLOR;
  --ls-link-text-color: #YOUR-COLOR;
  /* shui tokens take HSL channels, not a color */
  --primary: 199 89% 48%;
}
```

## Compatibility

- Logseq: DB version (shui/radix token era). Themes written only against `--ls-*` will look unfinished on shui-heavy surfaces.
- Platforms: Desktop (Windows, macOS, Linux), Mobile (iOS, Android)

## Support

- 🐛 Report issues: [GitHub Issues](https://github.com/yourusername/logseq-theme-modern/issues)
- 💬 Discuss: [Logseq Discord](https://discord.gg/logseq)
- ⭐ Star the repo if you like it!

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- Inspired by [Theme Name]
- Uses [Inter](https://rsms.me/inter/) and [JetBrains Mono](https://www.jetbrains.com/lp/mono/) fonts
- Thanks to Logseq community for feedback

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

````

**Step 4: Create CHANGELOG**

Track changes in `CHANGELOG.md`:

```markdown
# Changelog

## [1.0.0] - 2026-01-22

### Added
- Initial release
- Light and dark mode support
- Custom typography system
- Enhanced component styling
- Mobile-responsive layout
- Task marker colors
- Block reference styling
- Modal improvements

### Design Decisions
- Used Inter font for readability
- Blue color scheme for professionalism
- Generous spacing for clarity
- Smooth transitions for polish
````

### Publish to GitHub

**Step 1: Create Repository**

```bash
# Initialize git
git init
git add .
git commit -m "Initial release v1.0.0"

# Create GitHub repository
# Then push:
git remote add origin https://github.com/yourusername/logseq-theme-modern.git
git branch -M main
git push -u origin main
```

**Step 2: Create Release**

```markdown
1. Go to GitHub repository
2. Click "Releases" → "Create a new release"
3. Tag version: v1.0.0
4. Release title: "Modern Theme v1.0.0"
5. Description: Paste changelog
6. Attach files: ZIP of theme
7. Click "Publish release"
```

**Step 3: Submit to Marketplace**

````markdown
1. Fork the Logseq marketplace repository
2. Add your theme entry
3. Create pull request
4. Wait for review and approval
````

Logseq reads the marketplace from a fixed raw-GitHub base (`src/main/frontend/handler/plugin.cljs:56-58`):

```clojure
(defonce central-endpoint "https://raw.githubusercontent.com/logseq/marketplace/master/")
(defonce plugins-url (str central-endpoint "plugins.json"))
```

`plugins.json` is an object whose **`packages`** key holds the entries (`load-marketplace-plugins` reads `(:packages res)`, `plugin.cljs:197-215`), and per-plugin assets such as the icon resolve under `packages/<id>/` (`pkg-asset`, `plugin.cljs:192-196`).

The keys the client actually reads are `id name title version url description author icon iir repo sponsors webPkg` (`src/main/frontend/components/plugins.cljs:358-359`), plus `effect`, `web` (`plugin.cljs:206`) and `supportsDB` (`components/plugins.cljs:409-414`).

> **Unverified:** The required shape of a marketplace submission — inline entry versus a per-package `manifest.json` — is a property of the `logseq/marketplace` repository and cannot be established from the Logseq source tree. Follow that repository's contribution instructions.
>
> Note that **`theme` is not among the keys the marketplace client reads.** Theme-ness is decided from the installed package's own `logseq.themes` / `logseq.theme` (`libs/src/LSPlugin.core.ts:740`), so a `"theme": true` in a marketplace entry has no effect on how the app classifies your package.

````

### Distribution Checklist

```markdown
- [ ] GitHub repository created
- [ ] Release published (with ZIP)
- [ ] Marketplace submission sent
- [ ] Announced in Discord
- [ ] Shared on Twitter/social media
- [ ] Added to personal website/portfolio
````

---

## Maintenance

### Version Updates

**Semantic Versioning:**

```markdown
Given version MAJOR.MINOR.PATCH:

1. MAJOR: Breaking changes

   - Major redesign
   - Incompatible with previous version
   - Example: 1.0.0 → 2.0.0

2. MINOR: New features (backward compatible)

   - Add new component styles
   - New color schemes
   - Example: 1.0.0 → 1.1.0

3. PATCH: Bug fixes
   - Fix color contrast
   - Adjust spacing
   - Example: 1.0.0 → 1.0.1
```

**Update Process:**

```bash
# 1. Make changes
# 2. Update version in package.json
# 3. Update CHANGELOG.md
# 4. Commit changes
git add .
git commit -m "Release v1.1.0: Add new features"
git tag v1.1.0
git push origin main --tags

# 5. Create GitHub release
# 6. Users auto-update via marketplace
```

### Handle Issues

**Bug Reports:**

```markdown
1. **Reproduce the issue:**
   - Ask for screenshots
   - Request Logseq version
   - Check browser/OS
2. **Debug:**
   - Test locally
   - Use DevTools
   - Identify root cause
3. **Fix:**

   - Create fix
   - Test thoroughly
   - Push update

4. **Communicate:**
   - Comment on issue
   - Explain fix
   - Close issue when resolved
```

**Feature Requests:**

```markdown
1. **Evaluate request:**

   - Aligns with theme vision?
   - Technically feasible?
   - Benefits most users?

2. **Implement:**

   - Design solution
   - Code feature
   - Test thoroughly

3. **Release:**
   - Minor version bump
   - Update changelog
   - Thank contributor
```

### Stay Updated

**Follow Logseq Development:**

```markdown
1. **Monitor Logseq releases:**

   - Check release notes
   - Test theme compatibility
   - Update if needed

2. **Join community:**

   - Logseq Discord
   - GitHub discussions
   - Twitter updates

3. **Watch for:**
   - CSS variable changes
   - New components
   - Breaking changes
   - Deprecations
```

**Re-verify against source, not against your memory.** Two commands catch most theme rot:

```bash
# Every --ls-* variable the app defines or consumes
git grep -h -oE -- '--ls-[a-z0-9-]+' -- '*.css' | sort -u

# Does this selector still exist?
git grep -nF '.your-selector' -- '*.css' '*.cljs'
```

At `be800f1711` the first command returns 114 names. Anything you use that is not in that list is your own private token — namespace it accordingly.

**Keep Dependencies Updated:**

```markdown
1. **Font updates:**

   - Check Google Fonts versions
   - Update CDN links if needed

2. **Best practices:**
   - Follow CSS standards
   - Use modern features carefully
   - Ensure cross-browser compatibility
```

---

## Summary

You've created a complete custom theme! 🎉

**What You've Built:**

✅ Complete theme package with dual modes  
✅ Custom color system with CSS variables  
✅ Professional typography setup  
✅ Styled components (blocks, sidebar, editor, search, modals)  
✅ Responsive layout for all devices  
✅ Published to GitHub and marketplace

**Next Steps:**

1. **Gather feedback** from users
2. **Iterate** based on suggestions
3. **Create variations** (different color schemes)
4. **Maintain** regular updates
5. **Help others** learn theme development

**Resources:**

- [Logseq Theme Gallery](https://github.com/logseq/awesome-logseq#themes)
- [CSS Variables Reference](../../reference/configuration/css-variables.md)
- [Logseq Discord #themes Channel](https://discord.gg/logseq)
- [Create a Theme](../../how-to/integrations/create-theme.md)

**In-repo sources of truth** (read these at the revision you are targeting):

- `libs/guides/custom_theme_guide.md` — the in-repo theme guide, including the selector map
- `src/main/frontend/shui/vars-classic.css` — `--ls-*` defaults and layout variables
- `src/main/frontend/shui/colors.css` — accent palettes keyed by `html[data-color=...]`, mapping `--lx-*` and many `--ls-*`
- `src/main/frontend/shui/radix.css`, `radix-hsl.css` — the `--rx-*` raw palettes
- `resources/css/shui.css` — shared shui/Base UI component CSS
- `resources/css/codemirror.lsradix.css` — the CodeMirror theme bridge
- `src/main/frontend/components/theme.cljs` — where `data-theme`, `data-color` and the body compat classes are set

**Tips for Success:**

- Start simple, add complexity gradually
- Test on multiple devices and browsers
- Listen to user feedback
- Keep theme maintainable
- Document your design decisions
- Share your work with the community

Happy theming! 🎨✨
