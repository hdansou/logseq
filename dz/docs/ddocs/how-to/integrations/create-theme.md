---
title: 'Create a Theme'
description: 'How to create and publish custom themes for Logseq'
status: stable
lastVerified: '2026-03-10'
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

> **Last revised:** 2026-03-10

> How to create a complete custom theme for Logseq.

## Theme Basics

Themes are CSS files that override Logseq's default appearance. They work in both file-based and database graphs.

### Two Options

1. **Personal theme**: `custom.css` in your graph
2. **Shareable theme**: Packaged as a plugin

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
├── custom.css          # Main CSS file
├── package.json        # For plugin distribution
└── README.md           # Documentation
```

---

## Color System

### Radix Color Scales

Logseq uses 12-step color scales:

| Step  | Purpose           |
| ----- | ----------------- |
| 01-02 | Backgrounds       |
| 03-04 | UI elements       |
| 05-06 | Borders           |
| 07-08 | Solid backgrounds |
| 09-10 | Text (secondary)  |
| 11-12 | Text (primary)    |

### Variable Layers

```css
/* Layer 1: Raw colors (Radix) */
--rx-gray-1: #fcfcfc;

/* Layer 2: Logseq extended */
--lx-gray-01: var(--rx-gray-1);

/* Layer 3: Legacy variables (most commonly overridden by themes) */
--ls-primary-background-color: var(--lx-gray-01);
```

> **Note:** The default Logseq color scheme is scoped with `html[data-theme=light][data-color=logseq]`. When overriding variables in a personal theme, using `html[data-theme='light']` (without `data-color`) is usually sufficient and will apply regardless of the selected accent color.

---

## Complete Theme Template

```css
/* =============================================================================
   Theme: [Your Theme Name]
   Author: [Your Name]
   ============================================================================= */

/* Light Mode */
html[data-theme='light'] {
  /* Backgrounds */
  --ls-primary-background-color: #ffffff;
  --ls-secondary-background-color: #f5f5f5;
  --ls-tertiary-background-color: #eeeeee;

  /* Text */
  --ls-primary-text-color: #1a1a1a;
  --ls-secondary-text-color: #666666;
  --ls-title-text-color: #000000;

  /* Links */
  --ls-link-text-color: #0077b6;
  --ls-link-text-hover-color: #023e8a;

  /* Accent */
  --ls-active-primary-color: #0077b6;
  --ls-active-secondary-color: #0096c7;

  /* Borders */
  --ls-border-color: #e0e0e0;
  --ls-secondary-border-color: #eeeeee;

  /* Blocks */
  --ls-block-bullet-color: #999999;
  --ls-block-highlight-color: rgba(0, 119, 182, 0.1);

  /* Selection */
  --ls-selection-background-color: rgba(0, 119, 182, 0.2);
}

/* Dark Mode */
html[data-theme='dark'] {
  /* Backgrounds */
  --ls-primary-background-color: #1a1a2e;
  --ls-secondary-background-color: #16213e;
  --ls-tertiary-background-color: #0f3460;

  /* Text */
  --ls-primary-text-color: #e8e8e8;
  --ls-secondary-text-color: #b0b0b0;
  --ls-title-text-color: #ffffff;

  /* Links */
  --ls-link-text-color: #4cc9f0;
  --ls-link-text-hover-color: #72efdd;

  /* Accent */
  --ls-active-primary-color: #4cc9f0;
  --ls-active-secondary-color: #72efdd;

  /* Borders */
  --ls-border-color: #2a2a4a;
  --ls-secondary-border-color: #1a1a2e;

  /* Blocks */
  --ls-block-bullet-color: #6a6a8a;
  --ls-block-highlight-color: rgba(76, 201, 240, 0.1);

  /* Selection */
  --ls-selection-background-color: rgba(76, 201, 240, 0.2);
}

/* Typography */
:root {
  --ls-font-family: 'Inter', -apple-system, sans-serif;
  --ls-page-text-size: 16px;
  --ls-page-title-size: 28px;
}

/* Component Customization */
.bullet-container .bullet {
  width: 6px;
  height: 6px;
}

.block-properties {
  border-radius: 8px;
  padding: 8px 12px;
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

## Testing Your Theme

### Check Both Modes

1. Toggle between light/dark mode (Settings → General)
2. Verify colors work in both modes

### Test All Components

- [ ] Page titles
- [ ] Block content
- [ ] Links and tags
- [ ] Code blocks
- [ ] Properties
- [ ] Sidebar
- [ ] Search
- [ ] Settings panel

### Developer Tools

1. Open DevTools (`Cmd+Option+I`)
2. Inspect elements
3. Live-edit CSS
4. Check computed styles

---

## Publishing as Plugin

### package.json

```json
{
  "name": "logseq-theme-mytheme",
  "version": "1.0.0",
  "description": "A beautiful theme for Logseq",
  "author": "Your Name",
  "logseq": {
    "id": "logseq-theme-mytheme",
    "title": "My Theme",
    "icon": "./icon.svg"
  },
  "main": "custom.css"
}
```

### Plugin Structure

```
logseq-theme-mytheme/
├── custom.css
├── package.json
├── icon.svg
├── README.md
└── screenshots/
    ├── light.png
    └── dark.png
```

### Submit to Marketplace

1. Create GitHub repo
2. Add `logseq-plugin` topic
3. Submit to Logseq plugin registry

---

## Best Practices

### Do

- ✅ Use CSS variables for colors
- ✅ Support both light and dark modes
- ✅ Test with different content types
- ✅ Use semantic color names
- ✅ Include screenshots

### Don't

- ❌ Hardcode colors in component selectors
- ❌ Override functional styles (layout, positioning)
- ❌ Use `!important` excessively
- ❌ Forget mobile/responsive testing

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

_Last updated: 2026-03-10_
