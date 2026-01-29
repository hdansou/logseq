---
title: 'CSS Variables Reference'
description: "Complete reference for Logseq's CSS custom properties for theme customization"
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, mobile, web]
graphType: [file, db]
relatedDocs:
  howTo:
    - '[Create Custom Theme](../../how-to/plugins/create-theme.md)'
  reference:
    - '[Plugin API Reference](../api/plugin-api.md)'
---

# CSS Variables Reference

> Complete reference for Logseq's CSS custom properties (variables) used in theming and customization.

## Overview

Logseq's theming system uses CSS custom properties with three main prefixes:

| Prefix   | Purpose                      | Status                       |
| -------- | ---------------------------- | ---------------------------- |
| `--ls-*` | Legacy Logseq variables      | Maintained for compatibility |
| `--rx-*` | Radix color system (base)    | Modern, actively used        |
| `--lx-*` | Logseq extended Radix colors | Modern, actively used        |

**Total Variables**: ~150+ CSS custom properties

---

## Theme Selectors

### HTML Data Attributes

```html
<!-- Theme mode -->
<html data-theme="light">
  <!-- or "dark" -->

  <!-- Color accent -->
  <html data-color="logseq">
    <!-- or "none" for modern system -->
  </html>
</html>
```

### CSS Selectors

```css
/* Light mode */
html[data-theme='light'] {
}
.light-theme {
}

/* Dark mode */
html[data-theme='dark'] {
}
.dark-theme {
}

/* Specific color scheme */
html[data-theme='dark'][data-color='logseq'] {
}

/* Modern (no color override) */
html[data-color='none'] {
}
```

---

## Background Colors

| Variable                                 | Purpose                | Light Default | Dark Default |
| ---------------------------------------- | ---------------------- | ------------- | ------------ |
| `--ls-primary-background-color`          | Main app background    | `#ffffff`     | `#002b36`    |
| `--ls-secondary-background-color`        | Elevated surfaces      | `#f7f7f7`     | `#023643`    |
| `--ls-tertiary-background-color`         | More elevated          | `#eaeaea`     | `#08404f`    |
| `--ls-quaternary-background-color`       | Highest elevation      | `#dcdcdc`     | `#094b5a`    |
| `--ls-table-tr-even-background-color`    | Table zebra striping   | Secondary     | `#03333f`    |
| `--ls-block-properties-background-color` | Block property panels  | Secondary     | `#06323e`    |
| `--ls-page-properties-background-color`  | Page property panels   | Secondary     | `#06323e`    |
| `--ls-slide-background-color`            | Presentation mode      | `#fff`        | `#002b36`    |
| `--ls-page-blockquote-bg-color`          | Blockquote backgrounds | `#fbfaf8`     | `#023643`    |
| `--ls-page-inline-code-bg-color`         | Inline code            | Secondary     | `#01222a`    |
| `--ls-selection-background-color`        | Text selection         | `#e4f2ff`     | `#338fff`    |

---

## Text Colors

| Variable                       | Purpose          | Light Default    | Dark Default         |
| ------------------------------ | ---------------- | ---------------- | -------------------- |
| `--ls-primary-text-color`      | Main body text   | `#433f38`        | `#a4b5b6`            |
| `--ls-secondary-text-color`    | Emphasized text  | `#161e2e`        | `#dfdfdf`            |
| `--ls-title-text-color`        | Page titles      | Header button BG | `#93a1a1`            |
| `--ls-link-text-color`         | Hyperlinks       | `#106ba3`        | `rgb(138, 187, 187)` |
| `--ls-link-text-hover-color`   | Link hover       | `#1a537c`        | `#d0e8e8`            |
| `--ls-link-ref-text-color`     | Block references | Same as link     | Same as link         |
| `--ls-tag-text-color`          | Tags/hashtags    | Same as link ref | Same as link         |
| `--ls-page-inline-code-color`  | Inline code text | Primary text     | Primary text         |
| `--ls-selection-text-color`    | Selected text    | Secondary text   | `#fff`               |
| `--ls-cloze-text-color`        | Cloze deletion   | `#0000cd`        | `#8fbc8f`            |
| `--ls-icon-color`              | Icon colors      | `#646464`        | Link text            |
| `--ls-left-sidebar-text-color` | Sidebar text     | `--lx-gray-12`   | `--lx-gray-11`       |

**Opacity Variables:**

```css
--ls-tag-text-opacity: 0.8;
--ls-tag-text-hover-opacity: 1;
```

---

## Border Colors

| Variable                            | Purpose                | Light Default           | Dark Default      |
| ----------------------------------- | ---------------------- | ----------------------- | ----------------- |
| `--ls-border-color`                 | Primary borders        | `#ccc`                  | `#0e5263`         |
| `--ls-secondary-border-color`       | Secondary borders      | `#e2e2e2`               | `#126277`         |
| `--ls-tertiary-border-color`        | Subtle borders         | `rgba(200,200,200,0.3)` | `rgba(0,2,0,0.1)` |
| `--ls-guideline-color`              | Guidelines/rulers      | `rgba(46,27,5,0.08)`    | `#0b4a5a`         |
| `--ls-page-blockquote-border-color` | Blockquote left border | `#799bbc`               | Border color      |
| `--ls-block-bullet-border-color`    | Block bullet borders   | `#dedede`               | `#0f4958`         |

---

## Interactive Elements

| Variable                        | Purpose                   | Light Default                          | Dark Default          |
| ------------------------------- | ------------------------- | -------------------------------------- | --------------------- |
| `--ls-active-primary-color`     | Active/selected primary   | `rgb(0, 105, 182)`                     | `#8ec2c2`             |
| `--ls-active-secondary-color`   | Active/selected secondary | `#00477c`                              | `#d0e8e8`             |
| `--ls-menu-hover-color`         | Menu item hover           | `--ls-a-chosen-bg`                     | Secondary BG          |
| `--ls-a-chosen-bg`              | Selected item background  | Quaternary BG                          | Quaternary BG         |
| `--ls-focus-ring-color`         | Focus outline             | `rgba(66,133,244,0.5)`                 | `rgba(18,98,119,0.5)` |
| `--ls-header-button-background` | Header button             | `rgba(15,20,25,1)`                     | `#dee4ea`             |
| `--ls-button-background-hsl`    | Button HSL color          | `200 98% 35%`                          | `200 98% 35%`         |
| `--ls-button-background`        | Computed button color     | `hsl(var(--ls-button-background-hsl))` | Same                  |

---

## Block-Specific Colors

| Variable                         | Purpose              | Light Default         | Dark Default     |
| -------------------------------- | -------------------- | --------------------- | ---------------- |
| `--ls-block-bullet-color`        | Block bullet         | `rgba(67,63,56,0.25)` | `#608e91`        |
| `--ls-block-bullet-active-color` | Active bullet        | `--rx-gray-12`        | `--rx-gray-12`   |
| `--ls-block-highlight-color`     | Block selection      | `#c0e6fd`             | `#0a3d4b`        |
| `--ls-block-left-color`          | Left border on hover | `--rx-logseq-11`      | `--rx-logseq-11` |

### Highlight Colors (Text Markers)

```css
--ls-highlight-color-gray: var(--rx-gray-05);
--ls-highlight-color-red: var(--rx-red-05);
--ls-highlight-color-yellow: var(--rx-yellow-05);
--ls-highlight-color-green: var(--rx-green-05);
--ls-highlight-color-blue: var(--rx-blue-05);
--ls-highlight-color-purple: var(--rx-purple-05);
--ls-highlight-color-pink: var(--rx-pink-05);
--ls-highlight-color-default: var(--ls-secondary-background-color);
```

---

## Component Colors

| Variable                   | Purpose                 | Light Default | Dark Default |
| -------------------------- | ----------------------- | ------------- | ------------ |
| `--ls-page-checkbox-color` | Checkbox color          | `#9dbbd8`     | `#6093a0`    |
| `--ls-page-mark-bg-color`  | Marked text background  | `#fef3ac`     | `#fef3ac`    |
| `--ls-page-mark-color`     | Marked text color       | `#262626`     | `#262626`    |
| `--ls-pie-bg-color`        | Progress pie background | `#e1e1e1`     | `#01303b`    |
| `--ls-pie-fg-color`        | Progress pie foreground | `#0a4a5d`     | `#0b5869`    |

---

## Scrollbar Colors

| Variable                           | Purpose         | Light Default      | Dark Default            |
| ---------------------------------- | --------------- | ------------------ | ----------------------- |
| `--ls-scrollbar-foreground-color`  | Scrollbar thumb | `rgba(0,0,0,0.1)`  | `#11505f`               |
| `--ls-scrollbar-background-color`  | Scrollbar track | `rgba(0,0,0,0.05)` | `rgba(30,60,67,0.1)`    |
| `--ls-scrollbar-thumb-hover-color` | Thumb hover     | `rgba(0,0,0,0.2)`  | `rgba(255,255,255,0.2)` |
| `--ls-scrollbar-width`             | Scrollbar width | `6px`              | `6px`                   |

---

## Layout & Spacing

| Variable                           | Default  | Purpose                 |
| ---------------------------------- | -------- | ----------------------- |
| `--ls-page-text-size`              | `1em`    | Base page text size     |
| `--ls-page-title-size`             | `36px`   | Page title font size    |
| `--ls-main-content-max-width`      | `960px`  | Max content width       |
| `--ls-main-content-max-width-wide` | `1440px` | Wide mode max width     |
| `--ls-font-family`                 | `Inter`  | Default font family     |
| `--ls-border-radius-low`           | `4px`    | Small border radius     |
| `--ls-border-radius-medium`        | `8px`    | Medium border radius    |
| `--ls-headbar-height`              | `3rem`   | Header bar height       |
| `--ls-left-sidebar-width`          | `246px`  | Left sidebar width      |
| `--ls-left-sidebar-sm-width`       | `74vw`   | Mobile sidebar width    |
| `--ls-left-sidebar-nav-btn-size`   | `38px`   | Sidebar nav button size |

### Mobile-Specific

```css
--ls-mobile-font-scale: 1;
--ls-page-title-size: calc(26px * var(--ls-mobile-font-scale, 1));
--ls-mobile-font-size: 16px;
--ls-native-kb-height: 0px;
```

---

## Z-Index Levels

```css
--ls-z-index-level-0: 0; /* Base content */
--ls-z-index-level-1: 9; /* Tooltips */
--ls-z-index-level-2: 99; /* Dropdowns */
--ls-z-index-level-3: 999; /* Modals */
--ls-z-index-level-4: 9999; /* Top-level modals */
--ls-z-index-level-5: 99999; /* System-critical overlays */
```

---

## Color Level System

Semantic color levels for consistent theming:

```css
/* Light mode */
--color-level-1: var(--rx-gray-02);
--color-level-2: var(--rx-gray-03);
--color-level-3: var(--rx-gray-04);
--color-level-4: var(--rx-gray-05);
--color-level-5: var(--rx-gray-06);
--color-level-6: var(--rx-gray-07);

/* Dark mode */
--color-level-1: var(--ls-secondary-background-color);
--color-level-2: var(--ls-tertiary-background-color);
--color-level-3: var(--ls-quaternary-background-color);
--color-level-4: #195d6c;
--color-level-5: #266c7d;
--color-level-6: #3a7e8e;
```

---

## Status Colors

```css
/* Error */
--ls-error-text-color: var(--color-red-400);
--ls-error-background-color: var(--color-red-900);

/* Warning */
--ls-warning-text-color: var(--color-yellow-400);
--ls-warning-background-color: var(--color-yellow-900);

/* Success */
--ls-success-text-color: var(--color-green-100);
--ls-success-background-color: var(--color-green-900);

/* File Sync */
--ls-color-file-sync-error: var(--color-red-600);
--ls-color-file-sync-pending: var(--color-yellow-600);
--ls-color-file-sync-idle: var(--color-green-600);
```

---

## Radix Color System

Logseq uses the [Radix Colors](https://www.radix-ui.com/colors) system with 12-step scales.

### Naming Convention

```css
--rx-{color}-{step}        /* Base color */
--rx-{color}-{step}-hsl    /* HSL format */
--rx-{color}-{step}-alpha  /* With alpha */
```

### Available Colors

```
Gray: gray, mauve, slate, sage, olive, sand
Red: tomato, red, crimson
Pink: pink, plum, purple, violet
Blue: indigo, blue, sky, cyan
Green: teal, mint, green, grass, lime
Yellow: yellow, amber, orange
Brown: brown, bronze, gold
```

### Step Meanings

| Steps | Purpose           | Use Cases                             |
| ----- | ----------------- | ------------------------------------- |
| 01-02 | Backgrounds       | App background, subtle backgrounds    |
| 03-04 | Subtle components | Hover states, pressed states          |
| 05-06 | Borders           | Component borders, focus rings        |
| 07-08 | Solid elements    | Hovered solid backgrounds             |
| 09-10 | High contrast     | Primary buttons, links                |
| 11-12 | Text              | Low contrast text, high contrast text |

### Example Usage

```css
html[data-color='none'] {
  background-color: var(--rx-gray-01);
  color: var(--rx-gray-12);
  border-color: var(--rx-gray-06);
}
```

---

## Creating a Custom Theme

### Minimal Theme

```css
html[data-theme='dark'] {
  --ls-primary-background-color: #1e1e2e;
  --ls-secondary-background-color: #2a2a3e;
  --ls-primary-text-color: #cdd6f4;
  --ls-link-text-color: #89b4fa;
  --ls-border-color: #45475a;
}
```

### Full Theme with Radix

```css
html[data-theme='dark'][data-color='myTheme'] {
  /* Define Radix scale */
  --rx-gray-01: #0d0e14;
  --rx-gray-02: #14151c;
  --rx-gray-03: #1c1d26;
  /* ... steps 04-11 ... */
  --rx-gray-12: #e0e4f0;

  /* Map to Logseq variables */
  --ls-primary-background-color: var(--rx-gray-01);
  --ls-secondary-background-color: var(--rx-gray-02);
  --ls-primary-text-color: var(--rx-gray-12);
  --ls-border-color: var(--rx-gray-06);
}
```

---

## Source Files

CSS variables are defined in:

- `resources/css/base.css` - Core variables
- `resources/css/light.css` - Light theme
- `resources/css/dark.css` - Dark theme
- `packages/ui/src/ui.css` - UI component variables

---

## See Also

- **Theme Creation**: [how-to/plugins/create-theme.md](../../how-to/plugins/create-theme.md)
- **Plugin API**: [plugin-api.md](../api/plugin-api.md) for UI injection
- **Radix Colors**: [radix-ui.com/colors](https://www.radix-ui.com/colors)

---

_Last updated: 2026-01-29 | [Report an issue](https://github.com/logseq/logseq/issues)_
