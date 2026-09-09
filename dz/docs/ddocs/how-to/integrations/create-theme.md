---
title: 'Create a Theme'
description: 'How to create and publish custom themes for Logseq'
status: stable
lastVerified: '2026-05-30'
logseqVersion: '0.11.x'
platforms: [desktop, web]
graphType: [file, db]
relatedDocs:
  reference:
    - '[CSS Variables](../../reference/configuration/css-variables.md)'
  howTo:
    - '[Custom CSS](custom-css.md)'
---

# Create a Theme

> **Last revised:** 2026-05-30

> How to create and package a custom theme for Logseq.

Logseq themes are CSS overrides. Personal themes live in `logseq/custom.css`; shareable themes ship as Logseq plugins that register theme CSS.

## Findings: New Capabilities

- Theme state is driven by `html[data-theme="light|dark"]` and `html[data-color="..."]`, with backward-compatible classes on `body`.
- Plugin themes can be registered declaratively with `logseq.themes` or imperatively with `logseq.provideTheme(theme)`.
- Logseq also exposes `html[data-font]` and `html[data-font-global]` for font-aware styling.

## Theme Basics

Themes work in both file-based and database graphs.

### Two Options

1. **Personal theme**: `logseq/custom.css` in your graph
2. **Shareable theme**: Packaged as a Logseq plugin that registers one or more theme CSS files

---

## Quick Start

### 1. Create Theme File

In your graph's `logseq/` folder, create `custom.css`:

```css
/* My Theme */
html[data-theme='dark'] {
  --ls-primary-background-color: #1a1a2e;
  --ls-primary-text-color: #eee;
  --ls-link-text-color: #4cc9f0;
}

html[data-theme='light'] {
  --ls-primary-background-color: #fafafa;
  --ls-primary-text-color: #333;
  --ls-link-text-color: #0077b6;
}
```

### 2. Reload Logseq

Press `Cmd+R` (macOS) or `Ctrl+R` (Windows/Linux).

---

## Theme Structure

```
your-theme/
├── custom.css          # Personal theme entrypoint
├── package.json        # For plugin distribution
└── README.md           # Documentation
```

---

## Color System

Logseq's current theme contract is built on three layers:

- Radix raw tokens: `--rx-*`
- Logseq aliases: `--lx-*`
- Semantic variables: `--ls-*`

Override the semantic variables first. The current source defines the defaults in `packages/ui/src/vars-classic.css` and `packages/ui/src/colors.css`.

```css
html[data-theme='dark'] {
  color-scheme: dark;

  --ls-primary-background-color: #111827;
  --ls-secondary-background-color: #172033;
  --ls-tertiary-background-color: #202b42;
  --ls-primary-text-color: #d6deeb;
  --ls-secondary-text-color: #eef2ff;
  --ls-link-text-color: #7dd3fc;
  --ls-link-text-hover-color: #bae6fd;
  --ls-selection-background-color: rgb(56 189 248 / 0.32);
}
```

> **Removed:** The old three-step authoring table that implied fixed meanings for every Radix step. The current source treats these as token aliases, not a theme API you need to model in docs.

> **Removed:** The claim that `html[data-theme="light"]` alone guarantees a theme will ignore accent color. Accent color still affects many tokens through `html[data-color="..."]`.

### Current Hooks

Scope theme CSS by these hooks when needed:

| Hook | Current use |
| --- | --- |
| `html[data-theme="light"]` / `html[data-theme="dark"]` | Light and dark mode |
| `html[data-color="..."]` | Accent color selection |
| `html.dark` | Tailwind dark-mode hook |
| `body.light-theme`, `body.white-theme`, `body.dark-theme` | Backward-compatible body classes |
| `html[data-font="serif"|"mono"]`, `html[data-font-global="true"]` | Font preference hooks |
| `html.is-mobile`, `html.is-electron`, `html.is-mac`, `html.is-native-iphone` | Platform-specific fixes |

---

## Personal Theme Template

```css
/* logseq/custom.css */

html[data-theme='light'] {
  --ls-primary-background-color: #ffffff;
  --ls-secondary-background-color: #f8fafc;
  --ls-primary-text-color: #1e293b;
  --ls-secondary-text-color: #334155;
  --ls-link-text-color: #0369a1;
  --ls-link-text-hover-color: #0c4a6e;
  --ls-focus-ring-color: rgba(14, 165, 233, 0.35);
}

html[data-theme='dark'] {
  --ls-primary-background-color: #111827;
  --ls-secondary-background-color: #172033;
  --ls-primary-text-color: #d6deeb;
  --ls-secondary-text-color: #eef2ff;
  --ls-link-text-color: #7dd3fc;
  --ls-link-text-hover-color: #bae6fd;
  --ls-selection-background-color: rgb(56 189 248 / 0.32);
}
```

---

## Adding Custom Fonts

### From Google Fonts

```css
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600&display=swap');

:root {
  --ls-font-family: 'Inter', sans-serif;
}

/* Code blocks use CodeMirror; target the editor class */
.CodeMirror pre,
.extensions__code .cm-editor .cm-content {
  font-family: 'JetBrains Mono', monospace;
}
```

### Local Fonts

Place font files in your graph's `logseq/` folder:

```css
@font-face {
  font-family: 'MyFont';
  src: url('./fonts/MyFont.woff2') format('woff2');
  font-weight: normal;
}

:root {
  --ls-font-family: 'MyFont', sans-serif;
}
```

---

## Component Styling

### Tags

```css
/* File graphs use a.tag; DB graphs use .block-tag */
a.tag {
  background-color: var(--ls-tertiary-background-color);
  padding: 2px 6px;
  border-radius: 4px;
}

a.tag[data-ref='important'] {
  background-color: #ef4444;
  color: white;
}

/* DB graph tag styling */
.block-tag a.tag {
  padding: 2px 6px;
  border-radius: 4px;
}
```

### Code Blocks

```css
/* CodeMirror editor wrapper */
.extensions__code {
  background: var(--ls-secondary-background-color);
  border-radius: 8px;
}

.CodeMirror pre,
.cm-editor .cm-content {
  font-family: 'JetBrains Mono', monospace;
}
```

### Page References

```css
.page-reference {
  color: var(--ls-link-text-color);
}

.page-reference:hover {
  text-decoration: underline;
}
```

---

## Theme Plugins

Current source supports two plugin registrations:

1. Declarative `logseq.themes` entries in `package.json`
2. Programmatic `logseq.provideTheme(theme)`

### `package.json`

```json
{
  "name": "logseq-theme-mytheme",
  "version": "1.0.0",
  "description": "A custom Logseq theme",
  "author": "Your Name",
  "logseq": {
    "id": "logseq-theme-mytheme",
    "title": "My Theme",
    "icon": "./icon.svg",
    "effect": true,
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
  },
  "main": "index.html"
}
```

### Programmatic Variant

```ts
import '@logseq/libs'

async function main() {
  logseq.provideTheme({
    name: 'My Generated Dark Theme',
    url: './themes/generated-dark.css',
    mode: 'dark',
    description: 'Generated from plugin settings',
  })
}

logseq.ready(main).catch(console.error)
```

---

## Testing Your Theme

### Check Both Modes

1. Toggle between light and dark mode in Settings
2. Switch accent colors and confirm the theme still reads correctly
3. Check font preferences if your theme changes typography

### Test Core Surfaces

- Page titles
- Block content
- Links, tags, and references
- Code blocks
- Properties
- Sidebar and search
- Settings panel

### Developer Tools

1. Open DevTools (`Cmd+Option+I`)
2. Inspect the computed values for `html[data-theme]` and `html[data-color]`
3. Live-edit CSS and verify the resulting variables, not just the selector that matched

---

## Best Practices

### Do

- ✅ Use CSS variables for colors
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

Study these for inspiration:

- **Bear Theme** - Minimalist design
- **Dracula** - Dark purple palette
- **Nord** - Arctic color scheme
- **Catppuccin** - Pastel colors

---

## See Also

- **CSS Variables**: [css-variables.md](../../reference/configuration/css-variables.md)
- **Custom CSS**: [custom-css.md](custom-css.md)

---

_Last updated: 2026-05-30_
