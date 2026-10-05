# Example Logseq Theme: "Ocean Breeze"

> **Last revised:** 2026-09-29 (activation selectors only)

> **Correction (2026-09-29):** colour variables set on `:root` or on `html[data-theme='…']` alone lose to Logseq's own `html[data-theme][data-color=logseq]` defaults and to the per-accent declarations on `body`, so the snippets on this page now use `html[data-theme='…'][data-color], html[data-theme='…'][data-color] body`. Core also reads `--lx-gray-*`/`--lx-accent-*` before `--ls-*`, and shui tokens (`--primary`, `--background`, …) must be bare HSL channels. Verified live on Logseq 2.0.1; see the [DB theme walkthrough](./db-theme-walkthrough.md).

A sample theme demonstrating Logseq theme development best practices.

## Files in This Example

- `ocean-breeze-theme.css` - The complete theme
- `package.json` - Theme metadata for plugin distribution

## Installation

1. Copy `ocean-breeze-theme.css` to your graph's `logseq/custom.css`
2. Or install as a plugin (place entire folder in `.logseq/plugins/`)

## Features

- Calming blue-green color palette
- Excellent contrast in both light and dark modes
- Custom styled code blocks
- Rounded, modern UI components
- Smooth transitions

---

## ocean-breeze-theme.css

```css
/* =============================================================================
   Ocean Breeze Theme for Logseq
   Version: 1.0.0
   Author: Example Developer
   Description: A calming theme with ocean-inspired colors
   ============================================================================= */

/* =============================================================================
   LIGHT MODE
   ============================================================================= */

html[data-theme='light'][data-color],
html[data-theme='light'][data-color] body {
  /* Backgrounds - Light sandy beach tones */
  --lx-gray-01: #fafbfc;
  --lx-gray-02: #f5f7f9;
  --lx-gray-03: #eef2f5;
  --lx-gray-04: #e5eaef;
  --lx-gray-05: #d8dfe6;
  --lx-gray-06: #c1ccd6;
  --lx-gray-07: #a8b5c2;
  --lx-gray-08: #8796a5;
  --lx-gray-09: #6b7c8d;
  --lx-gray-10: #4f5f6f;
  --lx-gray-11: #3a4754;
  --lx-gray-12: #1f2933;

  /* Ocean blue accents */
  --lx-accent-01: #f0f9ff;
  --lx-accent-02: #e0f2fe;
  --lx-accent-03: #b9e3fc;
  --lx-accent-04: #7dd3fc;
  --lx-accent-05: #38bdf8;
  --lx-accent-06: #0ea5e9;
  --lx-accent-07: #0284c7;
  --lx-accent-08: #0369a1;
  --lx-accent-09: #075985;
  --lx-accent-10: #0c4a6e;
  --lx-accent-11: #164e63;
  --lx-accent-12: #083344;

  /* Legacy variable mapping */
  --ls-primary-background-color: var(--lx-gray-01);
  --ls-secondary-background-color: var(--lx-gray-02);
  --ls-tertiary-background-color: var(--lx-gray-03);
  --ls-quaternary-background-color: var(--lx-gray-04);

  --ls-primary-text-color: var(--lx-gray-12);
  --ls-secondary-text-color: var(--lx-gray-11);
  --ls-title-text-color: var(--lx-gray-12);

  --ls-link-text-color: var(--lx-accent-08);
  --ls-link-text-hover-color: var(--lx-accent-09);

  --ls-border-color: var(--lx-gray-05);
  --ls-secondary-border-color: var(--lx-gray-04);

  --ls-block-bullet-color: var(--lx-accent-07);
  --ls-page-checkbox-color: var(--lx-gray-06);

  --ls-scrollbar-foreground-color: var(--lx-gray-06);
  --ls-scrollbar-background-color: var(--lx-gray-02);

  --ls-selection-background-color: var(--lx-accent-03);
  --ls-block-highlight-color: var(--lx-accent-02);

  /* Code blocks */
  --ls-page-inline-code-bg-color: var(--lx-gray-04);
  --ls-page-inline-code-color: var(--lx-accent-10);

  /* Quotes */
  --ls-page-blockquote-bg-color: var(--lx-accent-01);
  --ls-page-blockquote-border-color: var(--lx-accent-06);
  --ls-page-blockquote-color: var(--lx-gray-11);
}

/* =============================================================================
   DARK MODE
   ============================================================================= */

html[data-theme='dark'][data-color],
html[data-theme='dark'][data-color] body {
  /* Backgrounds - Deep ocean tones */
  --lx-gray-01: #0a1420;
  --lx-gray-02: #111c2b;
  --lx-gray-03: #1a2637;
  --lx-gray-04: #233143;
  --lx-gray-05: #2d3d50;
  --lx-gray-06: #3d4e63;
  --lx-gray-07: #4e5f74;
  --lx-gray-08: #667a8f;
  --lx-gray-09: #8296ab;
  --lx-gray-10: #9fb0c3;
  --lx-gray-11: #c5d2e0;
  --lx-gray-12: #e5ecf3;

  /* Ocean blue accents - Brighter for dark background */
  --lx-accent-01: #0a1a2e;
  --lx-accent-02: #0d2640;
  --lx-accent-03: #133454;
  --lx-accent-04: #1a4468;
  --lx-accent-05: #22567e;
  --lx-accent-06: #2d6a95;
  --lx-accent-07: #3b80ad;
  --lx-accent-08: #4c96c5;
  --lx-accent-09: #5faddd;
  --lx-accent-10: #7dc3ed;
  --lx-accent-11: #a1d5f5;
  --lx-accent-12: #c5e5fb;

  /* Legacy variables */
  --ls-primary-background-color: var(--lx-gray-01);
  --ls-secondary-background-color: var(--lx-gray-02);
  --ls-tertiary-background-color: var(--lx-gray-03);
  --ls-quaternary-background-color: var(--lx-gray-04);

  --ls-primary-text-color: var(--lx-gray-12);
  --ls-secondary-text-color: var(--lx-gray-11);
  --ls-title-text-color: var(--lx-gray-12);

  --ls-link-text-color: var(--lx-accent-09);
  --ls-link-text-hover-color: var(--lx-accent-10);

  --ls-border-color: var(--lx-gray-05);
  --ls-secondary-border-color: var(--lx-gray-04);

  --ls-block-bullet-color: var(--lx-accent-08);
  --ls-page-checkbox-color: var(--lx-gray-06);

  --ls-scrollbar-foreground-color: var(--lx-gray-05);
  --ls-scrollbar-background-color: var(--lx-gray-02);

  --ls-selection-background-color: var(--lx-accent-03);
  --ls-block-highlight-color: var(--lx-accent-02);

  /* Code blocks */
  --ls-page-inline-code-bg-color: var(--lx-gray-04);
  --ls-page-inline-code-color: var(--lx-accent-10);

  /* Quotes */
  --ls-page-blockquote-bg-color: var(--lx-accent-02);
  --ls-page-blockquote-border-color: var(--lx-accent-06);
  --ls-page-blockquote-color: var(--lx-gray-11);
}

/* =============================================================================
   TYPOGRAPHY
   ============================================================================= */

/* Import modern, readable font */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
  --ls-font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  --ls-code-font-family: 'JetBrains Mono', 'Monaco', 'Courier New', monospace;
  --ls-page-text-size: 16px;
  --ls-page-title-size: 32px;
}

.ls-block {
  font-family: var(--ls-font-family);
  font-size: var(--ls-page-text-size);
  line-height: 1.7;
  font-weight: 400;
}

h1.title,
h1 {
  font-size: 2em;
  font-weight: 700;
  letter-spacing: -0.02em;
  margin-bottom: 0.5em;
}

h2 {
  font-size: 1.5em;
  font-weight: 600;
  letter-spacing: -0.01em;
  margin-top: 1.5em;
  margin-bottom: 0.5em;
}

h3 {
  font-size: 1.25em;
  font-weight: 600;
  margin-top: 1.25em;
  margin-bottom: 0.5em;
}

/* =============================================================================
   BLOCKS & BULLETS
   ============================================================================= */

.ls-block {
  padding: 3px 0;
}

.ls-block .bullet-container {
  width: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.ls-block .bullet {
  background-color: var(--ls-block-bullet-color);
  width: 7px;
  height: 7px;
  border-radius: 50%;
  transition: all 0.2s ease;
}

.ls-block:hover .bullet {
  transform: scale(1.3);
  box-shadow: 0 0 8px var(--lx-accent-08);
}

.block-children {
  margin-left: 28px;
  border-left: 1px solid var(--lx-gray-04);
  padding-left: 12px;
}

/* =============================================================================
   LINKS & REFERENCES
   ============================================================================= */

a {
  color: var(--ls-link-text-color);
  text-decoration: none;
  transition: all 0.2s ease;
  border-bottom: 1px solid transparent;
}

a:hover {
  color: var(--ls-link-text-hover-color);
  border-bottom-color: currentColor;
}

.block-ref {
  border-bottom: 2px solid var(--lx-accent-07);
  padding: 1px 3px;
  border-radius: 3px;
  background-color: var(--lx-accent-02);
  transition: all 0.2s ease;
}

.block-ref:hover {
  background-color: var(--lx-accent-03);
  border-bottom-color: var(--lx-accent-09);
}

.tag {
  background-color: var(--lx-accent-03);
  color: var(--lx-accent-11);
  padding: 2px 8px;
  border-radius: 6px;
  font-size: 0.875em;
  font-weight: 500;
  transition: all 0.2s ease;
}

.tag:hover {
  background-color: var(--lx-accent-04);
  transform: translateY(-1px);
}

/* =============================================================================
   CODE BLOCKS
   ============================================================================= */

code {
  background-color: var(--ls-page-inline-code-bg-color);
  color: var(--ls-page-inline-code-color);
  padding: 3px 6px;
  border-radius: 5px;
  font-size: 0.9em;
  font-family: var(--ls-code-font-family);
  font-weight: 500;
}

.extensions__code {
  background-color: var(--lx-gray-02);
  border: 1px solid var(--lx-gray-05);
  border-radius: 10px;
  padding: 1.25rem;
  margin: 1rem 0;
  overflow-x: auto;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
}

.extensions__code code {
  background: none;
  padding: 0;
  color: var(--ls-primary-text-color);
}

/* =============================================================================
   QUOTES & CALLOUTS
   ============================================================================= */

blockquote {
  background-color: var(--ls-page-blockquote-bg-color);
  border-left: 4px solid var(--ls-page-blockquote-border-color);
  color: var(--ls-page-blockquote-color);
  padding: 1rem 1.25rem;
  margin: 1rem 0;
  border-radius: 8px;
  font-style: italic;
}

/* =============================================================================
   SHADCN/UI COMPONENTS
   ============================================================================= */

/* Buttons */
.ui__button {
  border-radius: 8px;
  font-weight: 500;
  transition: all 0.2s ease;
  font-family: var(--ls-font-family);
}

.ui__button:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
}

.ui__button.as-primary {
  background-color: var(--lx-accent-08);
  color: white;
}

.ui__button.as-primary:hover {
  background-color: var(--lx-accent-09);
}

/* Cards */
.ui__card {
  background-color: var(--ls-secondary-background-color);
  border: 1px solid var(--ls-border-color);
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
  transition: all 0.2s ease;
}

.ui__card:hover {
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.08);
  transform: translateY(-2px);
}

/* Dialogs */
.ui__dialog-overlay {
  background-color: rgba(10, 20, 32, 0.6);
  backdrop-filter: blur(4px);
}

.ui__dialog-content {
  background-color: var(--ls-primary-background-color);
  border: 1px solid var(--ls-border-color);
  border-radius: 16px;
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
}

/* Dropdowns */
.ui__dropdown-menu-content {
  background-color: var(--ls-secondary-background-color);
  border: 1px solid var(--ls-border-color);
  border-radius: 10px;
  padding: 4px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.15);
}

.ui__dropdown-menu-item {
  padding: 8px 12px;
  border-radius: 6px;
  transition: all 0.15s ease;
}

.ui__dropdown-menu-item:hover {
  background-color: var(--lx-accent-03);
}

/* =============================================================================
   SIDEBAR & LAYOUT
   ============================================================================= */

.cp__header {
  background-color: var(--ls-primary-background-color);
  border-bottom: 1px solid var(--ls-border-color);
  backdrop-filter: blur(10px);
  background-color: rgba(var(--lx-gray-01-rgb), 0.95);
}

.cp__sidebar-left-layout {
  background-color: var(--ls-secondary-background-color);
  border-right: 1px solid var(--ls-border-color);
}

.cp__right-sidebar {
  background-color: var(--ls-primary-background-color);
  border-left: 1px solid var(--ls-border-color);
}

.sidebar-item {
  padding: 0.75rem 1rem;
  border-radius: 8px;
  margin: 4px 8px;
  transition: all 0.2s ease;
}

.sidebar-item:hover {
  background-color: var(--ls-tertiary-background-color);
}

/* =============================================================================
   TABLES
   ============================================================================= */

table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  margin: 1.5rem 0;
  border: 1px solid var(--ls-border-color);
  border-radius: 8px;
  overflow: hidden;
}

th {
  background-color: var(--lx-accent-03);
  color: var(--lx-accent-11);
  font-weight: 600;
  text-align: left;
  padding: 0.75rem 1rem;
  border-bottom: 2px solid var(--lx-accent-06);
}

td {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--ls-border-color);
}

tr:last-child td {
  border-bottom: none;
}

tr:nth-child(even) td {
  background-color: var(--ls-secondary-background-color);
}

/* =============================================================================
   SCROLLBARS
   ============================================================================= */

.visible-scrollbar::-webkit-scrollbar {
  width: 10px;
  height: 10px;
}

.visible-scrollbar::-webkit-scrollbar-track {
  background: var(--ls-scrollbar-background-color);
  border-radius: 5px;
}

.visible-scrollbar::-webkit-scrollbar-thumb {
  background: var(--ls-scrollbar-foreground-color);
  border-radius: 5px;
  transition: background 0.2s;
}

.visible-scrollbar::-webkit-scrollbar-thumb:hover {
  background: var(--lx-gray-08);
}

/* =============================================================================
   FOCUS STATES
   ============================================================================= */

*:focus-visible {
  outline: 2px solid var(--lx-accent-08);
  outline-offset: 2px;
  border-radius: 4px;
}

/* =============================================================================
   ANIMATIONS
   ============================================================================= */

@media (prefers-reduced-motion: no-preference) {
  * {
    transition-property: background-color, border-color, color, fill, stroke,
      opacity, box-shadow, transform;
    transition-duration: 0.2s;
    transition-timing-function: ease;
  }
}

/* Disable animations for users who prefer reduced motion */
@media (prefers-reduced-motion: reduce) {
  *,
  *::before,
  *::after {
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
  }
}

/* Page fade in */
.page {
  animation: fadeIn 0.3s ease-in;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* =============================================================================
   MOBILE OPTIMIZATIONS
   ============================================================================= */

@media (max-width: 640px) {
  :root {
    --ls-page-text-size: 15px;
  }

  .ls-block {
    padding: 2px 0;
  }

  .block-children {
    margin-left: 20px;
  }

  .ui__card {
    border-radius: 8px;
  }
}
```

---

## package.json

```json
{
  "name": "logseq-theme-ocean-breeze",
  "version": "1.0.0",
  "description": "A calming theme with ocean-inspired colors for Logseq",
  "author": "Example Developer",
  "license": "MIT",
  "keywords": ["logseq", "theme", "ocean", "blue", "calm"],
  "repository": {
    "type": "git",
    "url": "https://github.com/yourusername/logseq-theme-ocean-breeze"
  },
  "logseq": {
    "id": "ocean-breeze",
    "title": "Ocean Breeze",
    "icon": "./icon.png",
    "themes": [
      {
        "name": "Ocean Breeze Light",
        "url": "./ocean-breeze-theme.css",
        "description": "Light theme with calming ocean colors",
        "mode": "light"
      },
      {
        "name": "Ocean Breeze Dark",
        "url": "./ocean-breeze-theme.css",
        "description": "Dark theme with deep ocean tones",
        "mode": "dark"
      }
    ]
  }
}
```

---

## Customization Tips

### Change the Accent Color

To change from blue to another color, modify the accent scale:

```css
html[data-theme='light'][data-color],
html[data-theme='light'][data-color] body {
  /* Replace with your color scale */
  --lx-accent-01: #f0fdf4; /* Light green example */
  --lx-accent-09: #16a34a;
  /* ...etc */
}
```

### Adjust Font Sizes

```css
:root {
  --ls-page-text-size: 18px; /* Larger text */
  --ls-page-title-size: 36px; /* Larger titles */
}
```

### More Rounded Corners

```css
.ui__card,
.ui__button,
.ui__dropdown-menu-content {
  border-radius: 16px; /* More rounded */
}
```

### Disable Animations

```css
* {
  transition: none !important;
  animation: none !important;
}
```

## Testing

1. Test in both light and dark modes
2. Check all accent color options (Settings → Accent color)
3. Verify readability of all text
4. Test on mobile devices
5. Check with plugins installed

## License

MIT - Feel free to use and modify!
