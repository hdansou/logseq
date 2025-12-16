# Logseq Theme CSS Variable and Class Reference

**Investigation Date:** December 15, 2025  
**Version:** 1.0  
**Purpose:** Comprehensive reference of all CSS custom properties, classes, and structures that make up a Logseq theme

---

## Executive Summary

Logseq's theming system uses **CSS custom properties (variables)** with a well-defined naming convention. The system supports:

- **Light and Dark modes** via `html[data-theme='light']` and `html[data-theme='dark']`
- **Color accents** via `html[data-color='logseq']` or other color schemes
- **Radix UI color system** with `--rx-*` and `--lx-*` prefixes
- **Legacy Logseq variables** with `--ls-*` prefix (gradually being migrated)

### Key Findings

1. **~150+ CSS Custom Properties** define the complete theme
2. **Two parallel color systems:** Legacy (`--ls-*`) and Modern (`--rx-*`/`--lx-*`)
3. **Theme switching** via HTML `data-theme` and `data-color` attributes
4. **Three main CSS files** define the base theme system
5. **Semantic naming** groups variables by purpose (background, text, border, etc.)

---

## Table of Contents

1. [CSS Custom Property Structure](#css-custom-property-structure)
2. [Complete Variable Reference](#complete-variable-reference)
3. [Theme Selectors](#theme-selectors)
4. [Color System](#color-system)
5. [Creating a Custom Theme](#creating-a-custom-theme)
6. [Migration Guide](#migration-guide)

---

## CSS Custom Property Structure

### Naming Conventions

Logseq uses three main prefixes for CSS variables:

| Prefix   | Purpose                          | Status                       | Example                         |
| -------- | -------------------------------- | ---------------------------- | ------------------------------- |
| `--ls-*` | **Legacy Logseq** variables      | Maintained for compatibility | `--ls-primary-background-color` |
| `--rx-*` | **Radix** color system (base)    | Modern, actively used        | `--rx-gray-05`                  |
| `--lx-*` | **Logseq extended** Radix colors | Modern, actively used        | `--lx-gray-12`                  |

### Variable Categories

Variables are organized by semantic purpose:

1. **Background Colors** - `--ls-*-background-color`
2. **Text Colors** - `--ls-*-text-color`
3. **Border Colors** - `--ls-*-border-color`
4. **Interactive Elements** - Buttons, links, hovers
5. **Component-Specific** - Blocks, checkboxes, code, etc.
6. **Layout/Spacing** - Dimensions, padding, z-index
7. **Highlight Colors** - Text highlighting system

---

## Complete Variable Reference

### 1. Background Colors

**Purpose:** Define background colors for different layers/levels of UI

| Variable                                 | Light Mode Default | Dark Mode Default | Purpose                      |
| ---------------------------------------- | ------------------ | ----------------- | ---------------------------- |
| `--ls-primary-background-color`          | `#ffffff`          | `#002b36`         | Main app background          |
| `--ls-secondary-background-color`        | `#f7f7f7`          | `#023643`         | Slightly elevated surfaces   |
| `--ls-tertiary-background-color`         | `#eaeaea`          | `#08404f`         | More elevated surfaces       |
| `--ls-quaternary-background-color`       | `#dcdcdc`          | `#094b5a`         | Highest elevation            |
| `--ls-table-tr-even-background-color`    | Same as secondary  | `#03333f`         | Zebra striping for tables    |
| `--ls-block-properties-background-color` | Same as secondary  | `#06323e`         | Block property panels        |
| `--ls-page-properties-background-color`  | Same as secondary  | `#06323e`         | Page property panels         |
| `--ls-slide-background-color`            | `#fff`             | `#002b36`         | Presentation mode background |
| `--ls-page-blockquote-bg-color`          | `#fbfaf8`          | `#023643`         | Blockquote backgrounds       |
| `--ls-page-inline-code-bg-color`         | Secondary BG       | `#01222a`         | Inline code backgrounds      |
| `--ls-selection-background-color`        | `#e4f2ff`          | `#338fff`         | Text selection               |

**Modern Equivalents (Radix/Logseq Extended):**

```css
/* These are part of the new system */
--lx-gray-01  /* Lightest gray (backgrounds) */
--lx-gray-02
--lx-gray-03
--lx-gray-04  /* Medium gray surfaces */
/* ... up to ... */
--lx-gray-12  /* Darkest gray (text) */
```

---

### 2. Text Colors

**Purpose:** Define text colors for different content types

| Variable                         | Light Mode Default     | Dark Mode Default    | Purpose                   |
| -------------------------------- | ---------------------- | -------------------- | ------------------------- |
| `--ls-primary-text-color`        | `#433f38`              | `#a4b5b6`            | Main body text            |
| `--ls-secondary-text-color`      | `#161e2e`              | `#dfdfdf`            | Secondary/emphasized text |
| `--ls-title-text-color`          | Header button BG       | `#93a1a1`            | Page titles               |
| `--ls-link-text-color`           | `#106ba3`              | `rgb(138, 187, 187)` | Hyperlinks                |
| `--ls-link-text-hover-color`     | `#1a537c`              | `#d0e8e8`            | Hyperlinks on hover       |
| `--ls-link-ref-text-color`       | Same as link           | Same as link         | Block references          |
| `--ls-link-ref-text-hover-color` | Same as link hover     | Same as link hover   | Block ref hover           |
| `--ls-tag-text-color`            | Same as link ref       | Same as link         | Tags/hashtags             |
| `--ls-tag-text-hover-color`      | Same as link ref hover | Same as link hover   | Tag hover                 |
| `--ls-block-ref-link-text-color` | `#d8e1e8` (light)      | `#377f91`            | Block reference links     |
| `--ls-page-blockquote-color`     | Primary text           | Primary text         | Blockquote text           |
| `--ls-page-mark-color`           | `#262626`              | `#262626`            | Marked/highlighted text   |
| `--ls-page-inline-code-color`    | Primary text           | Primary text         | Inline code text          |
| `--ls-selection-text-color`      | Secondary text         | `#fff`               | Selected text             |
| `--ls-cloze-text-color`          | `#0000cd`              | `#8fbc8f`            | Cloze deletion text       |
| `--ls-icon-color`                | `#646464`              | Link text color      | Icon colors               |
| `--ls-search-icon-color`         | Primary text           | Primary text         | Search icon               |
| `--ls-search-icon-hover-color`   | Secondary text         | Secondary text       | Search icon hover         |
| `--ls-left-sidebar-text-color`   | `--lx-gray-12`         | `--lx-gray-11`       | Sidebar text              |

**Tag & Link Opacity:**

```css
--ls-tag-text-opacity: 0.8;
--ls-tag-text-hover-opacity: 1;
```

---

### 3. Border Colors

**Purpose:** Define borders, separators, and outlines

| Variable                            | Light Mode Default      | Dark Mode Default | Purpose                |
| ----------------------------------- | ----------------------- | ----------------- | ---------------------- |
| `--ls-border-color`                 | `#ccc`                  | `#0e5263`         | Primary borders        |
| `--ls-secondary-border-color`       | `#e2e2e2`               | `#126277`         | Secondary borders      |
| `--ls-tertiary-border-color`        | `rgba(200,200,200,0.3)` | `rgba(0,2,0,0.1)` | Subtle borders         |
| `--ls-guideline-color`              | `rgba(46,27,5,0.08)`    | `#0b4a5a`         | Guidelines/rulers      |
| `--ls-page-blockquote-border-color` | `#799bbc`               | Border color      | Blockquote left border |
| `--ls-block-bullet-border-color`    | `#dedede`               | `#0f4958`         | Block bullet borders   |
| `--ls-page-checkbox-border-color`   | Checkbox color          | Primary BG        | Checkbox borders       |

**Radix Border Variables:**

```css
/* Modern system uses Tailwind's border utility with: */
border-color: hsl(var(--border));
```

---

### 4. Interactive Element Colors

**Purpose:** Buttons, active states, hovers, focus rings

| Variable                        | Light Mode Default                     | Dark Mode Default     | Purpose                   |
| ------------------------------- | -------------------------------------- | --------------------- | ------------------------- |
| `--ls-active-primary-color`     | `rgb(0, 105, 182)`                     | `#8ec2c2`             | Active/selected primary   |
| `--ls-active-secondary-color`   | `#00477c`                              | `#d0e8e8`             | Active/selected secondary |
| `--ls-menu-hover-color`         | `--ls-a-chosen-bg`                     | Secondary BG          | Menu item hover           |
| `--ls-a-chosen-bg`              | Quaternary BG                          | Quaternary BG         | Selected item background  |
| `--ls-focus-ring-color`         | `rgba(66,133,244,0.5)`                 | `rgba(18,98,119,0.5)` | Focus outline             |
| `--ls-header-button-background` | `rgba(15,20,25,1)`                     | `#dee4ea`             | Header button background  |
| `--ls-button-background-hsl`    | `200 98% 35%`                          | `200 98% 35%`         | Button HSL color          |
| `--ls-button-background`        | `hsl(var(--ls-button-background-hsl))` | Same                  | Computed button color     |

---

### 5. Block-Specific Colors

**Purpose:** Block bullets, highlights, properties

| Variable                         | Light Mode Default    | Dark Mode Default | Purpose                   |
| -------------------------------- | --------------------- | ----------------- | ------------------------- |
| `--ls-block-bullet-color`        | `rgba(67,63,56,0.25)` | `#608e91`         | Block bullet color        |
| `--ls-block-bullet-active-color` | `--rx-gray-12`        | `--rx-gray-12`    | Active bullet (modern)    |
| `--ls-block-highlight-color`     | `#c0e6fd`             | `#0a3d4b`         | Block selection/highlight |
| `--ls-block-left-color`          | `--rx-logseq-11`      | `--rx-logseq-11`  | Left border on hover      |

**Highlight Colors (Text Markers):**

```css
/* Light mode highlight colors */
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

### 6. Component-Specific Colors

**Purpose:** Special-purpose component styling

| Variable                   | Light Mode Default | Dark Mode Default | Purpose                 |
| -------------------------- | ------------------ | ----------------- | ----------------------- |
| `--ls-page-checkbox-color` | `#9dbbd8`          | `#6093a0`         | Checkbox color          |
| `--ls-page-mark-bg-color`  | `#fef3ac`          | `#fef3ac`         | Marked text background  |
| `--ls-pie-bg-color`        | `#e1e1e1`          | `#01303b`         | Progress pie background |
| `--ls-pie-fg-color`        | `#0a4a5d`          | `#0b5869`         | Progress pie foreground |

**File Sync Status Colors:**

```css
--ls-color-file-sync-error: var(--color-red-600);
--ls-color-file-sync-pending: var(--color-yellow-600);
--ls-color-file-sync-idle: var(--color-green-600);
```

**Icon Preset:**

```css
--ls-color-icon-preset: 'inherit';
```

---

### 7. Scrollbar Colors

**Purpose:** Custom scrollbar styling

| Variable                           | Light Mode Default | Dark Mode Default       | Purpose         |
| ---------------------------------- | ------------------ | ----------------------- | --------------- |
| `--ls-scrollbar-foreground-color`  | `rgba(0,0,0,0.1)`  | `#11505f`               | Scrollbar thumb |
| `--ls-scrollbar-background-color`  | `rgba(0,0,0,0.05)` | `rgba(30,60,67,0.1)`    | Scrollbar track |
| `--ls-scrollbar-thumb-hover-color` | `rgba(0,0,0,0.2)`  | `rgba(255,255,255,0.2)` | Thumb hover     |
| `--ls-scrollbar-width`             | `6px`              | `6px`                   | Scrollbar width |

---

### 8. Layout & Spacing Variables

**Purpose:** Dimensions, padding, sizing

| Variable                           | Default Value | Purpose                 |
| ---------------------------------- | ------------- | ----------------------- |
| `--ls-page-text-size`              | `1em`         | Base page text size     |
| `--ls-page-title-size`             | `36px`        | Page title font size    |
| `--ls-main-content-max-width`      | `960px`       | Max content width       |
| `--ls-main-content-max-width-wide` | `1440px`      | Wide mode max width     |
| `--ls-font-family`                 | `Inter`       | Default font family     |
| `--ls-border-radius-low`           | `4px`         | Small border radius     |
| `--ls-border-radius-medium`        | `8px`         | Medium border radius    |
| `--ls-headbar-height`              | `3rem`        | Header bar height       |
| `--ls-headbar-inner-top-padding`   | `0px`         | Header top padding      |
| `--ls-left-sidebar-width`          | `246px`       | Left sidebar width      |
| `--ls-left-sidebar-sm-width`       | `74vw`        | Sidebar width on mobile |
| `--ls-left-sidebar-nav-btn-size`   | `38px`        | Sidebar nav button size |
| `--ls-draggable-handle-height`     | `30px`        | Plugin draggable handle |
| `--ls-native-kb-height`            | `0px`         | Native keyboard height  |

**Mobile-Specific:**

```css
--ls-mobile-font-scale: 1;
--ls-page-title-size: calc(26px * var(--ls-mobile-font-scale, 1));
--ls-mobile-font-size: 16px;
```

---

### 9. Z-Index Levels

**Purpose:** Layering system for overlays

```css
--ls-z-index-level-0: 0;
--ls-z-index-level-1: 9;
--ls-z-index-level-2: 99;
--ls-z-index-level-3: 999;
--ls-z-index-level-4: 9999;
--ls-z-index-level-5: 99999;
```

**Usage:**

- Level 0: Base content
- Level 1: Slightly elevated (tooltips)
- Level 2: Dropdowns
- Level 3: Modals
- Level 4: Top-level modals
- Level 5: System-critical overlays (plugin UI)

---

### 10. Color Level System

**Purpose:** Semantic color levels for consistent theming

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

### 11. Status & Notification Colors

**Purpose:** Error, warning, success states

```css
/* Dark mode defaults (light mode similar but lighter) */
--ls-error-text-color: var(--color-red-400);
--ls-error-background-color: var(--color-red-900);
--ls-warning-text-color: var(--color-yellow-400);
--ls-warning-background-color: var(--color-yellow-900);
--ls-success-text-color: var(--color-green-100);
--ls-success-background-color: var(--color-green-900);
```

---

## Theme Selectors

### HTML Attribute-Based Theming

Logseq uses HTML data attributes to control theming:

```html
<!-- Theme mode -->
<html data-theme="light">
  <!-- or "dark" -->

  <!-- Color accent -->
  <html data-color="logseq">
    <!-- or "none" for modern Radix system -->
  </html>
</html>
```

### Theme Selector Patterns

**1. Light Mode Theme:**

```css
/* All equivalent - use for backward compatibility */
.white-theme {
  /* ... */
}
.light-theme {
  /* ... */
}
html[data-theme='light'] {
  /* ... */
}
```

**2. Dark Mode Theme:**

```css
.dark-theme {
  /* ... */
}
html[data-theme='dark'] {
  /* ... */
}
```

**3. Color-Specific Light Theme:**

```css
html[data-theme='light'][data-color='logseq'] {
  /* Logseq classic light theme variables */
}
```

**4. Color-Specific Dark Theme:**

```css
html[data-theme='dark'][data-color='logseq'] {
  /* Logseq classic dark theme variables */
}
```

**5. Modern Color System (No Color):**

```css
html[data-color='none'] {
  /* Use pure Radix colors without Logseq overrides */
  --ls-block-bullet-color: var(--rx-gray-08);
  --ls-block-bullet-active-color: var(--rx-gray-12);
}
```

**6. Prefers Color Scheme (System):**

```css
@media (prefers-color-scheme: dark) {
  html {
    background-color: #002b36;
  }
}
```

---

## Color System

### Radix UI Color Scales

Logseq uses the Radix Colors system with 12-step scales:

**Naming Convention:**

```css
--rx-{color}-{step}
--rx-{color}-{step}-hsl  /* HSL format */
--rx-{color}-{step}-alpha  /* With alpha channel */
```

**Available Colors:**

```
gray, mauve, slate, sage, olive, sand
tomato, red, crimson, pink, plum, purple, violet
indigo, blue, sky, cyan, teal, mint, green, grass
lime, yellow, amber, orange, brown, bronze, gold
```

**Step Meanings:**

| Step  | Purpose           | Example Use                           |
| ----- | ----------------- | ------------------------------------- |
| 01-02 | Backgrounds       | App background, subtle backgrounds    |
| 03-04 | Subtle components | Hover states, pressed states          |
| 05-06 | Borders           | Component borders, focus rings        |
| 07-08 | Solid elements    | Hovered solid backgrounds             |
| 09-10 | High contrast     | Primary buttons, links                |
| 11-12 | Text              | Low contrast text, high contrast text |

**Example Usage:**

```css
html[data-color='none'] {
  /* Use Radix colors directly */
  background-color: var(--rx-gray-01);
  color: var(--rx-gray-12);
  border-color: var(--rx-gray-06);
}
```

### Logseq Extended Colors

**Prefix:** `--lx-*`

These extend Radix with Logseq-specific needs:

```css
--lx-gray-01  /* Extended gray scale */
--lx-gray-12
--lx-accent-11  /* Accent colors */
--lx-popover-bg  /* Component-specific */
--lx-pdf-container-dark-bg  /* Feature-specific */
```

---

## Creating a Custom Theme

### Method 1: CSS Plugin Theme

**File Structure:**

```
my-theme/
├── package.json
└── theme.css
```

**package.json:**

```json
{
  "id": "my-custom-theme",
  "name": "My Custom Theme",
  "version": "1.0.0",
  "logseq": {
    "themes": [
      {
        "name": "My Custom Theme",
        "url": "./theme.css",
        "description": "A beautiful custom theme",
        "mode": "dark"
      }
    ]
  }
}
```

**theme.css:**

```css
/* Import base if needed */
@import url('https://cdn.jsdelivr.net/npm/@radix-ui/colors@latest/dist/index.css');

/* Override Logseq variables */
.dark-theme,
html[data-theme='dark'] {
  /* Backgrounds */
  --ls-primary-background-color: #1a1b26;
  --ls-secondary-background-color: #24283b;
  --ls-tertiary-background-color: #2f3549;
  --ls-quaternary-background-color: #3b4261;

  /* Text */
  --ls-primary-text-color: #c0caf5;
  --ls-secondary-text-color: #a9b1d6;

  /* Links */
  --ls-link-text-color: #7aa2f7;
  --ls-link-text-hover-color: #bb9af7;

  /* Borders */
  --ls-border-color: #3b4261;
  --ls-secondary-border-color: #414868;

  /* Highlights */
  --ls-block-highlight-color: #2f3549;
  --ls-selection-background-color: #3d59a1;

  /* ... more variables ... */
}
```

---

### Method 2: Minimal Variable Override

Only override essential variables, let others inherit:

```css
html[data-theme='dark'] {
  /* Core colors - others will derive from these */
  --ls-primary-background-color: #1e1e2e;
  --ls-primary-text-color: #cdd6f4;
  --ls-link-text-color: #89b4fa;
  --ls-border-color: #45475a;

  /* Let these derive or use defaults */
  --ls-secondary-background-color: color-mix(
    in srgb,
    var(--ls-primary-background-color) 95%,
    white 5%
  );
}
```

---

### Method 3: Use Radix Color System

**Modern approach:**

```css
html[data-theme='dark'][data-color='myTheme'] {
  /* Use Radix color scales */
  --rx-gray-01: #0d0e14;
  --rx-gray-02: #14151c;
  --rx-gray-03: #1c1d26;
  /* ... define all 12 steps ... */
  --rx-gray-12: #e0e4f0;

  /* Map to Logseq variables */
  --ls-primary-background-color: var(--rx-gray-01);
  --ls-secondary-background-color: var(--rx-gray-02);
  --ls-tertiary-background-color: var(--rx-gray-03);
  --ls-primary-text-color: var(--rx-gray-12);
  --ls-border-color: var(--rx-gray-06);
}
```

---

### Theme Checklist

When creating a theme, ensure you define:

**Essential Colors (Required):**

- [ ] `--ls-primary-background-color`
- [ ] `--ls-secondary-background-color`
- [ ] `--ls-tertiary-background-color`
- [ ] `--ls-primary-text-color`
- [ ] `--ls-link-text-color`
- [ ] `--ls-link-text-hover-color`
- [ ] `--ls-border-color`
- [ ] `--ls-selection-background-color`

**Recommended:**

- [ ] `--ls-block-bullet-color`
- [ ] `--ls-block-highlight-color`
- [ ] `--ls-scrollbar-foreground-color`
- [ ] `--ls-scrollbar-background-color`
- [ ] `--ls-page-blockquote-bg-color`
- [ ] `--ls-page-inline-code-bg-color`

**Optional (for polish):**

- [ ] All highlight colors (`--ls-highlight-color-*`)
- [ ] Component-specific colors (checkboxes, etc.)
- [ ] Status colors (error, warning, success)

---

## Migration Guide

### From Legacy to Modern

**Legacy Approach (Still Supported):**

```css
html[data-theme='dark'] {
  --ls-primary-background-color: #002b36;
  --ls-primary-text-color: #a4b5b6;
  /* ... define all variables ... */
}
```

**Modern Approach:**

```css
html[data-theme='dark'][data-color='none'] {
  /* Define Radix scales */
  --rx-gray-01: #002b36;
  --rx-gray-12: #a4b5b6;

  /* Logseq maps to Radix automatically */
  --ls-primary-background-color: var(--lx-gray-01);
  --ls-primary-text-color: var(--lx-gray-12);
}
```

### Variable Mapping

**Common Migrations:**

| Legacy                            | Modern Equivalent                                 |
| --------------------------------- | ------------------------------------------------- |
| `--ls-primary-background-color`   | `var(--lx-gray-01)` or `hsl(var(--background))`   |
| `--ls-secondary-background-color` | `var(--lx-gray-02)`                               |
| `--ls-primary-text-color`         | `var(--lx-gray-12)` or `hsl(var(--foreground))`   |
| `--ls-link-text-color`            | `var(--lx-accent-11)` or `hsl(var(--primary)/.8)` |
| `--ls-border-color`               | `hsl(var(--border))` or `var(--lx-gray-06)`       |

---

## Testing Your Theme

### Visual Checklist

Test your theme against these components:

**1. Basic Elements:**

- [ ] Page background
- [ ] Body text
- [ ] Links (normal and hover)
- [ ] Headings

**2. Blocks:**

- [ ] Block bullets
- [ ] Block selection/highlight
- [ ] Block references
- [ ] Block properties

**3. Inputs:**

- [ ] Text inputs
- [ ] Textareas
- [ ] Checkboxes
- [ ] Radio buttons

**4. Code:**

- [ ] Inline code
- [ ] Code blocks
- [ ] Syntax highlighting

**5. Interactive:**

- [ ] Buttons (various states)
- [ ] Dropdowns
- [ ] Modals
- [ ] Tooltips

**6. Special:**

- [ ] Blockquotes
- [ ] Tables
- [ ] Highlights (all colors)
- [ ] Scrollbars

### Browser Testing

```javascript
// Test theme switching in console
document.documentElement.setAttribute('data-theme', 'dark')
document.documentElement.setAttribute('data-theme', 'light')

// Test color mode
document.documentElement.setAttribute('data-color', 'logseq')
document.documentElement.setAttribute('data-color', 'none')

// Get computed values
getComputedStyle(document.documentElement).getPropertyValue(
  '--ls-primary-background-color'
)
```

---

## Advanced Techniques

### 1. Color Mixing

```css
--ls-hover-color: color-mix(
  in srgb,
  var(--ls-primary-background-color) 90%,
  var(--ls-link-text-color) 10%
);
```

### 2. HSL Manipulation

```css
/* Define base as HSL */
--ls-primary-hsl: 200 100% 50%;

/* Create variations */
--ls-primary-light: hsl(var(--ls-primary-hsl) / 0.5);
--ls-primary-dark: hsl(calc(var(--ls-primary-hsl) - 20deg) 100% 30%);
```

### 3. Calc-Based Spacing

```css
--ls-spacing-base: 8px;
--ls-spacing-sm: calc(var(--ls-spacing-base) * 0.5);
--ls-spacing-md: var(--ls-spacing-base);
--ls-spacing-lg: calc(var(--ls-spacing-base) * 2);
```

### 4. Theme-Aware Components

```css
/* Component adapts to theme */
.my-component {
  background: var(--ls-secondary-background-color);
  color: var(--ls-primary-text-color);
  border: 1px solid var(--ls-border-color);

  &:hover {
    background: var(--ls-tertiary-background-color);
  }
}
```

### 5. Fallback Values

```css
/* Provide fallbacks for older versions */
background-color: var(--lx-gray-01, var(--ls-primary-background-color, #fff));
```

---

## Appendix A: Complete Variable List

### All `--ls-` Variables (Alphabetical)

```css
--ls-a-chosen-bg
--ls-active-primary-color
--ls-active-secondary-color
--ls-block-bullet-active-color
--ls-block-bullet-border-color
--ls-block-bullet-color
--ls-block-highlight-color
--ls-block-left-color
--ls-block-properties-background-color
--ls-block-ref-link-text-color
--ls-border-color
--ls-border-radius-low
--ls-border-radius-medium
--ls-button-background
--ls-button-background-hsl
--ls-cloze-text-color
--ls-color-file-sync-error
--ls-color-file-sync-idle
--ls-color-file-sync-pending
--ls-color-icon-preset
--ls-draggable-handle-height
--ls-error-background-color
--ls-error-text-color
--ls-focus-ring-color
--ls-font-family
--ls-guideline-color
--ls-headbar-height
--ls-headbar-inner-top-padding
--ls-header-button-background
--ls-highlight-color-blue
--ls-highlight-color-default
--ls-highlight-color-gray
--ls-highlight-color-green
--ls-highlight-color-pink
--ls-highlight-color-purple
--ls-highlight-color-red
--ls-highlight-color-yellow
--ls-icon-color
--ls-left-sidebar-nav-btn-size
--ls-left-sidebar-sm-width
--ls-left-sidebar-text-color
--ls-left-sidebar-width
--ls-link-ref-text-color
--ls-link-ref-text-hover-color
--ls-link-text-color
--ls-link-text-hover-color
--ls-main-content-max-width
--ls-main-content-max-width-wide
--ls-menu-hover-color
--ls-mobile-font-scale
--ls-mobile-font-size
--ls-native-kb-height
--ls-page-blockquote-bg-color
--ls-page-blockquote-border-color
--ls-page-blockquote-color
--ls-page-checkbox-border-color
--ls-page-checkbox-color
--ls-page-inline-code-bg-color
--ls-page-inline-code-color
--ls-page-mark-bg-color
--ls-page-mark-color
--ls-page-properties-background-color
--ls-page-text-size
--ls-page-title-size
--ls-pie-bg-color
--ls-pie-fg-color
--ls-primary-background-color
--ls-primary-text-color
--ls-quaternary-background-color
--ls-scrollbar-background-color
--ls-scrollbar-foreground-color
--ls-scrollbar-thumb-hover-color
--ls-scrollbar-width
--ls-search-icon-color
--ls-search-icon-hover-color
--ls-secondary-background-color
--ls-secondary-border-color
--ls-secondary-text-color
--ls-selection-background-color
--ls-selection-text-color
--ls-slide-background-color
--ls-success-background-color
--ls-success-text-color
--ls-table-tr-even-background-color
--ls-tag-text-color
--ls-tag-text-hover-color
--ls-tag-text-hover-opacity
--ls-tag-text-opacity
--ls-tertiary-background-color
--ls-tertiary-border-color
--ls-title-text-color
--ls-warning-background-color
--ls-warning-text-color
--ls-z-index-level-0
--ls-z-index-level-1
--ls-z-index-level-2
--ls-z-index-level-3
--ls-z-index-level-4
--ls-z-index-level-5
```

---

## Appendix B: File Locations

### Theme-Related CSS Files

| File                                                                                                                              | Purpose                  | Variables Defined           |
| --------------------------------------------------------------------------------------------------------------------------------- | ------------------------ | --------------------------- |
| [packages/ui/src/vars-classic.css](file:///Users/dzu/Projects/src/github.com/logseq/packages/ui/src/vars-classic.css)             | Classic Logseq theme     | All `--ls-*` for light/dark |
| [src/main/frontend/common.css](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/common.css)                     | Base app styles          | Legacy theme definitions    |
| [resources/css/shui.css](file:///Users/dzu/Projects/src/github.com/logseq/resources/css/shui.css)                                 | Modern Radix integration | Radix color mappings        |
| [src/main/frontend/components/theme.css](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/theme.css) | Theme utilities          | Z-index, scrollbar          |
| [packages/ui/src/radix.css](file:///Users/dzu/Projects/src/github.com/logseq/packages/ui/src/radix.css)                           | Radix UI components      | Radix color scales          |

---

## Appendix C: Related Documentation

- **Plugin UI Guide:** [PLUGIN-UI-INTEGRATION-GUIDE.md](file:///Users/dzu/Projects/src/github.com/logseq/docs/07-investigations/PLUGIN-UI-INTEGRATION-GUIDE.md)
- **Tailwind Config:** [tailwind.config.js](file:///Users/dzu/Projects/src/github.com/logseq/tailwind.config.js)
- **Theme ClojureScript:** [src/main/frontend/components/theme.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/theme.cljs)

---

## Appendix D: Quick Reference Card

### Essential Variables (Copy-Paste Template)

```css
/* === YOUR THEME NAME === */

.dark-theme,
html[data-theme='dark'] {
  /* Backgrounds (lightest to darkest) */
  --ls-primary-background-color: #____;
  --ls-secondary-background-color: #____;
  --ls-tertiary-background-color: #____;
  --ls-quaternary-background-color: #____;

  /* Text (main, emphasized) */
  --ls-primary-text-color: #____;
  --ls-secondary-text-color: #____;

  /* Links */
  --ls-link-text-color: #____;
  --ls-link-text-hover-color: #____;

  /* Borders */
  --ls-border-color: #____;

  /* Interactive */
  --ls-active-primary-color: #____;
  --ls-selection-background-color: #____;

  /* Blocks */
  --ls-block-bullet-color: #____;
  --ls-block-highlight-color: #____;

  /* Code */
  --ls-page-inline-code-bg-color: #____;

  /* Scrollbar */
  --ls-scrollbar-foreground-color: rgba(___, ___, ___, 0.3);
  --ls-scrollbar-background-color: rgba(___, ___, ___, 0.1);
}
```

---

## Version History

| Version | Date       | Changes                         |
| ------- | ---------- | ------------------------------- |
| 1.0     | 2025-12-15 | Initial comprehensive reference |

---

## License

This reference guide is part of the Logseq project and follows the same license (AGPL-3.0).
