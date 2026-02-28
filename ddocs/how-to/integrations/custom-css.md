---
title: 'Custom CSS'
description: "How to customize Logseq's appearance with custom CSS"
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, web]
graphType: [file, db]
relatedDocs:
  reference:
    - '[CSS Variables](../../reference/configuration/css-variables.md)'
  howTo:
    - '[Custom JavaScript](custom-js.md)'
---

# Custom CSS

> How to customize Logseq's appearance with your own CSS styles.

## Where to Add Custom CSS

### Option 1: custom.css File (Recommended)

1. Open your graph folder
2. Navigate to `logseq/` subfolder
3. Create or edit `custom.css`

**Path:**

```
your-graph/
└── logseq/
    └── custom.css
```

### Option 2: Settings UI

1. Open Settings (gear icon)
2. Go to **Editor** tab
3. Find **Custom CSS** section
4. Paste your CSS

---

## Basic Customizations

### Change Font

```css
:root {
  --ls-font-family: 'Source Sans Pro', sans-serif;
}
```

### Increase Text Size

```css
:root {
  --ls-page-text-size: 18px;
}
```

### Change Background Color

```css
/* Dark mode */
html[data-theme='dark'] {
  --ls-primary-background-color: #1a1a2e;
  --ls-secondary-background-color: #16213e;
}

/* Light mode */
html[data-theme='light'] {
  --ls-primary-background-color: #fafafa;
  --ls-secondary-background-color: #f0f0f0;
}
```

### Change Link Color

```css
:root {
  --ls-link-text-color: #ff6b6b;
  --ls-link-text-hover-color: #ee5a5a;
}
```

---

## Common Recipes

### Hide Left Sidebar

```css
#left-sidebar {
  display: none;
}
```

### Wider Content Area

```css
:root {
  --ls-main-content-max-width: 1200px;
}
```

### Rounded Corners

```css
:root {
  --ls-border-radius-low: 8px;
  --ls-border-radius-medium: 12px;
}
```

### Custom Block Bullets

```css
.bullet-container .bullet {
  background-color: #4ecdc4;
  width: 8px;
  height: 8px;
}
```

### Highlight Current Block

```css
.editor-inner .editor-wrapper {
  background-color: rgba(100, 150, 255, 0.1);
  border-radius: 4px;
}
```

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

### Custom Page Properties

```css
.block-properties {
  background-color: var(--ls-tertiary-background-color);
  border-radius: 8px;
  padding: 8px 12px;
}
```

---

## Theme-Aware Styling

Use data attributes for theme-specific styles:

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

Common variables to customize:

| Variable                          | Purpose           |
| --------------------------------- | ----------------- |
| `--ls-primary-background-color`   | Main background   |
| `--ls-secondary-background-color` | Elevated surfaces |
| `--ls-primary-text-color`         | Body text         |
| `--ls-link-text-color`            | Links             |
| `--ls-border-color`               | Borders           |
| `--ls-font-family`                | Font              |
| `--ls-page-text-size`             | Text size         |
| `--ls-main-content-max-width`     | Content width     |

See full reference: [CSS Variables](../../reference/configuration/css-variables.md)

---

## Debugging CSS

### Open Developer Tools

- **macOS**: `Cmd+Option+I`
- **Windows/Linux**: `Ctrl+Shift+I`

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
/* Catppuccin-inspired theme */
html[data-theme='dark'] {
  /* Base colors */
  --ls-primary-background-color: #1e1e2e;
  --ls-secondary-background-color: #313244;
  --ls-tertiary-background-color: #45475a;

  /* Text colors */
  --ls-primary-text-color: #cdd6f4;
  --ls-secondary-text-color: #bac2de;
  --ls-title-text-color: #f5e0dc;

  /* Accent colors */
  --ls-link-text-color: #89b4fa;
  --ls-link-text-hover-color: #b4befe;
  --ls-active-primary-color: #cba6f7;

  /* Borders */
  --ls-border-color: #45475a;

  /* Selection */
  --ls-selection-background-color: rgba(203, 166, 247, 0.3);
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

---

## Troubleshooting

### CSS Not Loading

1. Check file is named exactly `custom.css`
2. Verify it's in `logseq/` folder
3. Reload Logseq (`Cmd/Ctrl+R`)

### Styles Not Applying

- Check selector specificity
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

_Last updated: 2026-01-29_
