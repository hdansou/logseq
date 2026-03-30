# Logseq Theme Development Guide

A complete guide to customizing the Logseq DB UI — from quick CSS tweaks to publishing a full theme plugin.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Quick Customization with custom.css](#2-quick-customization-with-customcss)
3. [Creating a Full Theme](#3-creating-a-full-theme)
4. [Publishing as a Plugin Theme](#4-publishing-as-a-plugin-theme)
5. [CSS Variable Reference](#5-css-variable-reference)
6. [Color Scale Design Guide](#6-color-scale-design-guide)
7. [Testing & Debugging](#7-testing--debugging)
8. [Examples](#8-examples)

---

## 1. Architecture Overview

Logseq's appearance is controlled by **HTML data attributes** on the `<html>` element. CSS selectors match these attributes to apply the right variables.

### DOM Attributes

```html
<html data-theme="dark" ← "light" | "dark" data-color="logseq" ← accent color
name | "none" data-font="default" ← "default" | "serif" | "mono"
data-font-global="false" ← "true" | "false" class="dark" ← Tailwind dark mode >
```

### CSS Variable Layers

The theme system stacks three variable namespaces, each building on the previous:

```
┌───────────────────────────────────────────────────────────┐
│ Layer 1: Raw Radix Scales  (--rx-*)                       │
│  :root { --rx-blue-09: hsl(206, 100%, 50%); }            │
│  30+ color palettes × 12 shades (01–12) + alpha variants │
│  File: packages/ui/src/radix-hsl.css                      │
├───────────────────────────────────────────────────────────┤
│ Layer 2: Semantic Accent/Gray  (--lx-*)                   │
│  html[data-color=blue] {                                  │
│    --lx-accent-01: var(--rx-blue-01);                     │
│    --lx-gray-01: var(--rx-slate-01);                      │
│  }                                                        │
│  File: packages/ui/src/colors.css                         │
├───────────────────────────────────────────────────────────┤
│ Layer 3: UI Component Variables  (--ls-*)                 │
│  html[data-theme=dark][data-color=logseq] {               │
│    --ls-primary-background-color: #002b36;                │
│    --ls-link-text-color: rgb(138, 187, 187);              │
│  }                                                        │
│  File: packages/ui/src/vars-classic.css                   │
├───────────────────────────────────────────────────────────┤
│ Layer 4: Tailwind/shadcn Tokens                           │
│  --background, --foreground, --primary, --accent, etc.    │
│  File: packages/ui/src/index.css                          │
└───────────────────────────────────────────────────────────┘
```

### CSS Priority (Cascade Order)

```
Browser defaults
  ↓ overridden by
Radix scales (radix-hsl.css)
  ↓ overridden by
Accent color mapping (colors.css, keyed by data-color)
  ↓ overridden by
Theme mode variables (vars-classic.css, keyed by data-theme × data-color)
  ↓ overridden by
Plugin theme CSS (<link> injected by plugin)
  ↓ overridden by
logseq/custom.css (<style> injected in DOM)    ← your overrides go here
  ↓ overridden by
Plugin provideStyle() CSS (<style> injected in DOM)
```

**`custom.css` wins over the built-in system** — it's the user's final say.

---

## 2. Quick Customization with custom.css

### Where to Edit

**DB graphs:** Navigate to the `logseq/custom.css` page in Logseq and edit directly.

**File graphs:** Edit `{your-graph}/logseq/custom.css` in any text editor.

### Prerequisite: Cancel the Accent Color

If you're defining your own color scheme, go to **Settings → Appearance** and click the ⊘ button (first swatch in the accent color row). This sets `data-color="none"` and prevents the built-in Radix accent system from overriding your custom colors.

If you're only making minor tweaks (e.g., changing the font or a single color), you can skip this step and your overrides will layer on top of the active accent.

### Example: Change Just the Backgrounds

```css
/* Override backgrounds while keeping the selected accent color */
html[data-theme='dark'] {
  --ls-primary-background-color: #1e1e2e;
  --ls-secondary-background-color: #181825;
  --ls-tertiary-background-color: #11111b;
}

html[data-theme='light'] {
  --ls-primary-background-color: #eff1f5;
  --ls-secondary-background-color: #e6e9ef;
  --ls-tertiary-background-color: #dce0e8;
}
```

### Example: Custom Link Colors

```css
html[data-theme='dark'] {
  --ls-link-text-color: #89b4fa;
  --ls-link-text-hover-color: #b4d0fb;
}
```

### Example: Custom Font

```css
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono&display=swap');

:root {
  --ls-font-family: 'JetBrains Mono', monospace;
}
```

### Example: Style Tags

```css
.block-tag {
  border-radius: 12px;
  padding: 3px 10px;
  margin: 0 2px;
  transition: all 0.15s ease;
}

html[data-theme='dark'] .block-tag {
  background-color: rgba(137, 180, 250, 0.15);
  border: 1px solid rgba(137, 180, 250, 0.3);
}

html[data-theme='dark'] .block-tag:hover {
  transform: translateY(-1px);
  box-shadow: 0 2px 4px rgba(137, 180, 250, 0.2);
}
```

---

## 3. Creating a Full Theme

A full theme defines its own color scales and maps them to all three variable layers. This gives you complete control over the look.

### Step 1: Choose Your Color Palette

You need:

- **An accent color** with 12 gradations (lightest → darkest for light mode; darkest → lightest for dark mode)
- **A gray scale** with 12 gradations (same pattern)
- A few standalone colors: link color, checkbox color, cloze color

### Step 2: Follow the 12-Step Scale Pattern

Radix UI's 12-step scale gives each step a clear purpose:

| Step | Purpose                   | Example Use                  |
| ---- | ------------------------- | ---------------------------- |
| 01   | App background            | Page background              |
| 02   | Subtle background         | Sidebar, secondary panels    |
| 03   | UI element background     | Buttons, inputs at rest      |
| 04   | Hovered UI element bg     | Button hover state           |
| 05   | Active/selected element   | Active button, selected item |
| 06   | Subtle borders            | Faint separators             |
| 07   | UI element borders        | Input borders, dividers      |
| 08   | Hovered borders           | Input border on hover        |
| 09   | Solid backgrounds         | Primary accent fills         |
| 10   | Hovered solid backgrounds | Accent hover state           |
| 11   | Low-contrast text         | Secondary/muted text         |
| 12   | High-contrast text        | Primary text, headings       |

### Step 3: Use the Theme Template

```css
/**
 * [THEME NAME] Theme for Logseq
 * Based on [SOURCE PALETTE]
 * [LICENSE]
 */

html {
  &[data-color='mytheme'] {
    /* ─── Define Color Scales ─── */
    body,
    .dark-theme,
    .light-theme {
      /* Light mode accent scale (12 steps) */
      --mytheme-light-accent-01: #fefefe;
      --mytheme-light-accent-02: #f8f8fb;
      --mytheme-light-accent-03: #f0f1fa;
      --mytheme-light-accent-04: #e5e6f6;
      --mytheme-light-accent-05: #d8d9f0;
      --mytheme-light-accent-06: #c7c8e6;
      --mytheme-light-accent-07: #b0b1d8;
      --mytheme-light-accent-08: #9394c8;
      --mytheme-light-accent-09: #6c63ff; /* ← signature accent */
      --mytheme-light-accent-10: #5b52e6;
      --mytheme-light-accent-11: #4a3fbf;
      --mytheme-light-accent-12: #261e6b;

      /* Dark mode accent scale */
      --mytheme-dark-accent-01: #13111f;
      --mytheme-dark-accent-02: #1c192e;
      --mytheme-dark-accent-03: #28234a;
      --mytheme-dark-accent-04: #342d5e;
      --mytheme-dark-accent-05: #403770;
      --mytheme-dark-accent-06: #4e4385;
      --mytheme-dark-accent-07: #5f5199;
      --mytheme-dark-accent-08: #7a6ec8;
      --mytheme-dark-accent-09: #6c63ff;
      --mytheme-dark-accent-10: #7f78ff;
      --mytheme-dark-accent-11: #b0abff;
      --mytheme-dark-accent-12: #e8e6ff;

      /* Light mode gray scale */
      --mytheme-light-gray-01: #ffffff;
      --mytheme-light-gray-02: #f9f9fb;
      --mytheme-light-gray-03: #f0f0f3;
      --mytheme-light-gray-04: #e8e8ec;
      --mytheme-light-gray-05: #dfdfe3;
      --mytheme-light-gray-06: #d2d2d8;
      --mytheme-light-gray-07: #bbbbc3;
      --mytheme-light-gray-08: #8d8d97;
      --mytheme-light-gray-09: #6f6f78;
      --mytheme-light-gray-10: #61616a;
      --mytheme-light-gray-11: #43434a;
      --mytheme-light-gray-12: #1e1e24;

      /* Dark mode gray scale */
      --mytheme-dark-gray-01: #111113;
      --mytheme-dark-gray-02: #19191c;
      --mytheme-dark-gray-03: #222225;
      --mytheme-dark-gray-04: #2c2c30;
      --mytheme-dark-gray-05: #37373c;
      --mytheme-dark-gray-06: #43434a;
      --mytheme-dark-gray-07: #55555e;
      --mytheme-dark-gray-08: #6f6f78;
      --mytheme-dark-gray-09: #8d8d97;
      --mytheme-dark-gray-10: #a0a0aa;
      --mytheme-dark-gray-11: #c4c4cc;
      --mytheme-dark-gray-12: #eeeeef;
    }

    /* ─── Light Mode ─── */
    &[data-theme='light'] {
      /* shadcn/tailwind tokens (HSL without hsl() wrapper) */
      --primary: 252 100% 69%;
      --primary-foreground: 0 0% 100%;
      --accent: 252 100% 69%;
      --accent-foreground: 0 0% 100%;
      --ring: 252 100% 69%;

      /* Map accent scale → --lx-accent-* */
      --lx-accent-01: var(--mytheme-light-accent-01);
      --lx-accent-02: var(--mytheme-light-accent-02);
      --lx-accent-03: var(--mytheme-light-accent-03);
      --lx-accent-04: var(--mytheme-light-accent-04);
      --lx-accent-05: var(--mytheme-light-accent-05);
      --lx-accent-06: var(--mytheme-light-accent-06);
      --lx-accent-07: var(--mytheme-light-accent-07);
      --lx-accent-08: var(--mytheme-light-accent-08);
      --lx-accent-09: var(--mytheme-light-accent-09);
      --lx-accent-10: var(--mytheme-light-accent-10);
      --lx-accent-11: var(--mytheme-light-accent-11);
      --lx-accent-12: var(--mytheme-light-accent-12);

      /* Map gray scale → --lx-gray-* */
      --lx-gray-01: var(--mytheme-light-gray-01);
      --lx-gray-02: var(--mytheme-light-gray-02);
      --lx-gray-03: var(--mytheme-light-gray-03);
      --lx-gray-04: var(--mytheme-light-gray-04);
      --lx-gray-05: var(--mytheme-light-gray-05);
      --lx-gray-06: var(--mytheme-light-gray-06);
      --lx-gray-07: var(--mytheme-light-gray-07);
      --lx-gray-08: var(--mytheme-light-gray-08);
      --lx-gray-09: var(--mytheme-light-gray-09);
      --lx-gray-10: var(--mytheme-light-gray-10);
      --lx-gray-11: var(--mytheme-light-gray-11);
      --lx-gray-12: var(--mytheme-light-gray-12);

      /* Map to legacy --ls-* variables */
      --ls-primary-background-color: var(--mytheme-light-gray-01);
      --ls-secondary-background-color: var(--mytheme-light-gray-02);
      --ls-tertiary-background-color: var(--mytheme-light-gray-03);
      --ls-quaternary-background-color: var(--mytheme-light-gray-04);
      --ls-primary-text-color: var(--mytheme-light-gray-12);
      --ls-secondary-text-color: var(--mytheme-light-gray-11);
      --ls-link-text-color: var(--mytheme-light-accent-09);
      --ls-link-text-hover-color: var(--mytheme-light-accent-10);
      --ls-block-ref-link-text-color: var(--mytheme-light-accent-09);
      --ls-border-color: var(--mytheme-light-gray-06);
      --ls-secondary-border-color: var(--mytheme-light-gray-05);
      --ls-page-checkbox-color: var(--mytheme-light-accent-08);
      --ls-selection-background-color: var(--mytheme-light-accent-04);
      --ls-block-highlight-color: var(--mytheme-light-gray-04);
      --ls-focus-ring-color: var(--mytheme-light-accent-07);
      --ls-table-tr-even-background-color: var(--mytheme-light-gray-02);
      --ls-page-properties-background-color: var(--mytheme-light-gray-02);
      --ls-block-properties-background-color: var(--mytheme-light-gray-03);
      --ls-page-inline-code-bg-color: var(--mytheme-light-gray-03);
      --ls-cloze-text-color: var(--mytheme-light-accent-10);
      --ls-a-chosen-bg: var(--mytheme-light-gray-03);
    }

    /* ─── Dark Mode ─── */
    &[data-theme='dark'] {
      /* shadcn/tailwind tokens */
      --background: 252 8% 8%;
      --foreground: 252 5% 95%;
      --primary: 252 100% 69%;
      --primary-foreground: 0 0% 100%;
      --accent: 252 20% 15%;
      --accent-foreground: 0 0% 100%;
      --ring: 252 100% 69%;
      --secondary: 252 15% 18%;
      --secondary-foreground: 0 0% 98%;
      --muted: 252 8% 12%;
      --border: 252 8% 15%;
      --card: 252 8% 7%;
      --card-foreground: 252 5% 95%;
      --popover: 252 8% 8%;
      --input: 252 15% 20%;

      /* Map accent scale */
      --lx-accent-01: var(--mytheme-dark-accent-01);
      --lx-accent-02: var(--mytheme-dark-accent-02);
      --lx-accent-03: var(--mytheme-dark-accent-03);
      --lx-accent-04: var(--mytheme-dark-accent-04);
      --lx-accent-05: var(--mytheme-dark-accent-05);
      --lx-accent-06: var(--mytheme-dark-accent-06);
      --lx-accent-07: var(--mytheme-dark-accent-07);
      --lx-accent-08: var(--mytheme-dark-accent-08);
      --lx-accent-09: var(--mytheme-dark-accent-09);
      --lx-accent-10: var(--mytheme-dark-accent-10);
      --lx-accent-11: var(--mytheme-dark-accent-11);
      --lx-accent-12: var(--mytheme-dark-accent-12);

      /* Map gray scale */
      --lx-gray-01: var(--mytheme-dark-gray-01);
      --lx-gray-02: var(--mytheme-dark-gray-02);
      --lx-gray-03: var(--mytheme-dark-gray-03);
      --lx-gray-04: var(--mytheme-dark-gray-04);
      --lx-gray-05: var(--mytheme-dark-gray-05);
      --lx-gray-06: var(--mytheme-dark-gray-06);
      --lx-gray-07: var(--mytheme-dark-gray-07);
      --lx-gray-08: var(--mytheme-dark-gray-08);
      --lx-gray-09: var(--mytheme-dark-gray-09);
      --lx-gray-10: var(--mytheme-dark-gray-10);
      --lx-gray-11: var(--mytheme-dark-gray-11);
      --lx-gray-12: var(--mytheme-dark-gray-12);

      /* Map to legacy --ls-* variables */
      --ls-primary-background-color: var(--mytheme-dark-gray-01);
      --ls-secondary-background-color: var(--mytheme-dark-gray-02);
      --ls-tertiary-background-color: var(--mytheme-dark-gray-03);
      --ls-quaternary-background-color: var(--mytheme-dark-gray-04);
      --ls-primary-text-color: var(--mytheme-dark-gray-12);
      --ls-secondary-text-color: var(--mytheme-dark-gray-11);
      --ls-link-text-color: var(--mytheme-dark-accent-11);
      --ls-link-text-hover-color: var(--mytheme-dark-accent-12);
      --ls-block-ref-link-text-color: var(--mytheme-dark-accent-09);
      --ls-border-color: var(--mytheme-dark-gray-05);
      --ls-secondary-border-color: var(--mytheme-dark-gray-04);
      --ls-page-checkbox-color: var(--mytheme-dark-accent-08);
      --ls-selection-background-color: var(--mytheme-dark-accent-04);
      --ls-block-highlight-color: var(--mytheme-dark-gray-04);
      --ls-focus-ring-color: var(--mytheme-dark-accent-07);
      --ls-table-tr-even-background-color: var(--mytheme-dark-gray-02);
      --ls-page-properties-background-color: var(--mytheme-dark-gray-02);
      --ls-block-properties-background-color: var(--mytheme-dark-gray-03);
      --ls-page-inline-code-bg-color: var(--mytheme-dark-gray-03);
      --ls-cloze-text-color: var(--mytheme-dark-accent-09);
      --ls-a-chosen-bg: var(--mytheme-dark-gray-02);
    }
  }
}
```

### Step 4: Install via custom.css

1. Paste the CSS into `logseq/custom.css`
2. Go to **Settings → Appearance → Accent color** and click ⊘ "Cancel accent color"
3. The theme takes effect immediately

> **Note:** Because the theme registers under `html[data-color=mytheme]` and Logseq won't set `data-color="mytheme"` through the UI, you can alternatively write the CSS using `:root` / `html[data-theme=dark]` / `html[data-theme=light]` selectors and skip the `data-color` selector entirely for custom.css-only themes.

### Alternative: Direct override (simpler for custom.css)

If you don't plan to distribute the theme as a plugin, you can skip the `data-color` machinery and directly override variables:

```css
/* Simpler approach for custom.css */
html[data-theme='light'] {
  --lx-accent-09: #6c63ff;
  --ls-primary-background-color: #ffffff;
  --ls-secondary-background-color: #f9f9fb;
  --ls-primary-text-color: #1e1e24;
  --ls-link-text-color: #6c63ff;
  /* ... etc */
}

html[data-theme='dark'] {
  --lx-accent-09: #6c63ff;
  --ls-primary-background-color: #111113;
  --ls-secondary-background-color: #19191c;
  --ls-primary-text-color: #eeeeef;
  --ls-link-text-color: #b0abff;
  /* ... etc */
}
```

---

## 4. Publishing as a Plugin Theme

### Scaffold the Plugin

```bash
npx @nicholasgasior/create-logseq-plugin my-theme-plugin
cd my-theme-plugin
```

### Plugin Entry Point

```typescript
// index.ts
import '@logseq/libs'

async function main() {
  // Method 1: Provide a complete theme (loads CSS + sets mode)
  logseq.provideTheme({
    name: 'My Theme Dark',
    url: './theme-dark.css',
    mode: 'dark',
  })

  // Method 2: Inject CSS directly (additive, doesn't switch mode)
  logseq.provideStyle(`
    :root {
      --ls-font-family: 'Inter', sans-serif;
    }
  `)
}

logseq.ready(main).catch(console.error)
```

### Theme CSS File (`theme-dark.css`)

Use the same CSS template from Section 3, but targeting `:root` and `html[data-theme=dark]` selectors (the plugin system handles loading/unloading).

### Plugin Theme Type

```typescript
interface Theme {
  name: string // Display name
  url: string // CSS file URL (relative to plugin dir)
  description?: string // Description shown in settings
  mode: 'light' | 'dark' // Theme mode
  pid: string // Plugin ID (set automatically)
}
```

### package.json

```json
{
  "name": "logseq-theme-mytheme",
  "version": "1.0.0",
  "logseq": {
    "id": "logseq-theme-mytheme",
    "title": "My Theme",
    "icon": "./icon.png"
  }
}
```

---

## 5. CSS Variable Reference

### Layout & Sizing

| Variable                           | Default  | Purpose              |
| ---------------------------------- | -------- | -------------------- |
| `--ls-font-family`                 | `Inter`  | Global font family   |
| `--ls-page-text-size`              | `1em`    | Body text size       |
| `--ls-page-title-size`             | `36px`   | Page title size      |
| `--ls-main-content-max-width`      | `960px`  | Content max width    |
| `--ls-main-content-max-width-wide` | `1440px` | Wide mode max width  |
| `--ls-scrollbar-width`             | `6px`    | Scrollbar width      |
| `--ls-border-radius-low`           | `4px`    | Small border radius  |
| `--ls-border-radius-medium`        | `8px`    | Medium border radius |
| `--ls-headbar-height`              | `3rem`   | Top header height    |
| `--ls-left-sidebar-width`          | `246px`  | Left sidebar width   |

### Backgrounds

| Variable                                 | Purpose                   |
| ---------------------------------------- | ------------------------- |
| `--ls-primary-background-color`          | Main page background      |
| `--ls-secondary-background-color`        | Sidebar, secondary panels |
| `--ls-tertiary-background-color`         | Nested/tertiary areas     |
| `--ls-quaternary-background-color`       | Deeply nested areas       |
| `--ls-table-tr-even-background-color`    | Table alternating rows    |
| `--ls-page-properties-background-color`  | Page property block bg    |
| `--ls-block-properties-background-color` | Block property bg         |
| `--ls-page-inline-code-bg-color`         | Inline code background    |
| `--ls-slide-background-color`            | Presentation mode bg      |
| `--ls-page-blockquote-bg-color`          | Blockquote background     |
| `--ls-a-chosen-bg`                       | Selected/chosen item bg   |
| `--ls-selection-background-color`        | Text selection bg         |

### Text Colors

| Variable                         | Purpose                  |
| -------------------------------- | ------------------------ |
| `--ls-primary-text-color`        | Main body text           |
| `--ls-secondary-text-color`      | Secondary/emphasis text  |
| `--ls-title-text-color`          | Page titles              |
| `--ls-link-text-color`           | Links                    |
| `--ls-link-text-hover-color`     | Links on hover           |
| `--ls-link-ref-text-color`       | Page reference links     |
| `--ls-link-ref-text-hover-color` | Page references on hover |
| `--ls-tag-text-color`            | Tag text                 |
| `--ls-tag-text-hover-color`      | Tags on hover            |
| `--ls-block-ref-link-text-color` | Block reference links    |
| `--ls-cloze-text-color`          | Cloze card text          |
| `--ls-page-blockquote-color`     | Blockquote text          |
| `--ls-page-mark-color`           | Highlighted text         |
| `--ls-page-inline-code-color`    | Inline code text         |
| `--ls-icon-color`                | Icon default color       |
| `--ls-left-sidebar-text-color`   | Left sidebar text        |

### Borders & Outlines

| Variable                            | Purpose                |
| ----------------------------------- | ---------------------- |
| `--ls-border-color`                 | Primary borders        |
| `--ls-secondary-border-color`       | Subtle borders         |
| `--ls-tertiary-border-color`        | Very faint borders     |
| `--ls-guideline-color`              | Indentation guides     |
| `--ls-block-bullet-border-color`    | Block bullet outlines  |
| `--ls-block-bullet-color`           | Block bullet fill      |
| `--ls-focus-ring-color`             | Focus ring/outline     |
| `--ls-page-blockquote-border-color` | Blockquote side border |
| `--ls-page-checkbox-color`          | Checkbox fill          |
| `--ls-page-checkbox-border-color`   | Checkbox border        |

### Interactive States

| Variable                        | Purpose                 |
| ------------------------------- | ----------------------- |
| `--ls-active-primary-color`     | Active states primary   |
| `--ls-active-secondary-color`   | Active states secondary |
| `--ls-block-highlight-color`    | Block highlight bg      |
| `--ls-selection-text-color`     | Selected text color     |
| `--ls-page-mark-bg-color`       | Highlight marker bg     |
| `--ls-menu-hover-color`         | Menu item hover         |
| `--ls-header-button-background` | Header button bg        |
| `--ls-button-background`        | Button background       |
| `--ls-button-background-hsl`    | Button bg in HSL format |

### Scrollbar

| Variable                           | Purpose         |
| ---------------------------------- | --------------- |
| `--ls-scrollbar-foreground-color`  | Scrollbar thumb |
| `--ls-scrollbar-background-color`  | Scrollbar track |
| `--ls-scrollbar-thumb-hover-color` | Thumb on hover  |

### Whiteboard

| Variable                           | Purpose                 |
| ---------------------------------- | ----------------------- |
| `--ls-wb-stroke-color-default`     | Whiteboard stroke color |
| `--ls-wb-background-color-default` | Whiteboard canvas bg    |
| `--ls-wb-text-color-default`       | Whiteboard text color   |

### Highlights (Block Highlight Colors)

| Variable                      | Color            |
| ----------------------------- | ---------------- |
| `--ls-highlight-color-gray`   | Gray highlight   |
| `--ls-highlight-color-red`    | Red highlight    |
| `--ls-highlight-color-yellow` | Yellow highlight |
| `--ls-highlight-color-green`  | Green highlight  |
| `--ls-highlight-color-blue`   | Blue highlight   |
| `--ls-highlight-color-purple` | Purple highlight |
| `--ls-highlight-color-pink`   | Pink highlight   |

### Accent Scale (--lx-accent-\*)

The 12-step semantic accent scale. Override these to change the accent color:

| Variable         | Usage                                      |
| ---------------- | ------------------------------------------ |
| `--lx-accent-01` | Lightest accent bg                         |
| `--lx-accent-02` | Subtle accent bg                           |
| `--lx-accent-03` | UI element bg                              |
| `--lx-accent-04` | Hovered UI bg                              |
| `--lx-accent-05` | Active/selected bg                         |
| `--lx-accent-06` | Subtle accent borders                      |
| `--lx-accent-07` | Accent borders                             |
| `--lx-accent-08` | Hovered accent borders                     |
| `--lx-accent-09` | **Primary accent** (buttons, active links) |
| `--lx-accent-10` | Hovered accent fills                       |
| `--lx-accent-11` | Low-contrast accent text                   |
| `--lx-accent-12` | High-contrast accent text                  |

Alpha variants also exist: `--lx-accent-01-alpha` through `--lx-accent-12-alpha`

### Gray Scale (--lx-gray-\*)

Same 12-step pattern for neutral grays. Used for backgrounds, text, and UI chrome.

### shadcn/Tailwind Tokens

Dark mode also relies on these HSL tokens (values without `hsl()` wrapper):

| Variable                 | Purpose              |
| ------------------------ | -------------------- |
| `--background`           | Page background      |
| `--foreground`           | Page text            |
| `--primary`              | Primary action color |
| `--primary-foreground`   | Text on primary      |
| `--accent`               | Accent surfaces      |
| `--accent-foreground`    | Text on accent       |
| `--secondary`            | Secondary surfaces   |
| `--secondary-foreground` | Text on secondary    |
| `--muted`                | Muted surfaces       |
| `--muted-foreground`     | Muted text           |
| `--destructive`          | Error/danger color   |
| `--border`               | Default borders      |
| `--input`                | Input border/bg      |
| `--ring`                 | Focus ring           |
| `--card`                 | Card background      |
| `--card-foreground`      | Card text            |
| `--popover`              | Popover background   |

### Z-Index Levels

| Variable               | Value | Purpose            |
| ---------------------- | ----- | ------------------ |
| `--ls-z-index-level-0` | 0     | Base               |
| `--ls-z-index-level-1` | 9     | Sticky elements    |
| `--ls-z-index-level-2` | 99    | Dropdowns          |
| `--ls-z-index-level-3` | 999   | Modals             |
| `--ls-z-index-level-4` | 9999  | Tooltips           |
| `--ls-z-index-level-5` | 99999 | Top-level overlays |

---

## 6. Color Scale Design Guide

### Generating a 12-Step Scale

Use these resources:

- **[Radix Colors](https://www.radix-ui.com/colors/custom)** — generate custom Radix scales with any hue
- **[Huetone](https://huetone.ardov.me/)** — design perceptually uniform color scales
- **[Leonardo](https://leonardocolor.io/)** — contrast-ratio-based color generation
- **Manual approach:** pick your base color at step 09, then interpolate lighter (01–08) and darker (10–12) steps

### Light vs Dark Mode Scale Direction

**Light mode:**

- Step 01 = near white (lightest)
- Step 12 = near black (darkest)
- Background colors come from low steps, text from high steps

**Dark mode:**

- Step 01 = near black (darkest)
- Step 12 = near white (lightest)
- Background from low steps, text from high steps (same mapping, inverted palette)

### Gray Scale Harmony

Each accent hue pairs well with a specific gray:

| Accent Family              | Harmonious Gray | Radix Name |
| -------------------------- | --------------- | ---------- |
| Red, crimson, pink, tomato | Warm gray       | `mauve`    |
| Blue                       | Cool gray       | `slate`    |
| Plum, purple, brown        | Warm neutral    | `sand`     |
| Violet, indigo             | Sage gray       | `sage`     |
| Cyan, teal                 | Olive gray      | `olive`    |
| Green, grass               | Mint gray       | `sage`     |
| Orange                     | Brown gray      | `sand`     |

### Accessibility

- **WCAG AA:** 4.5:1 contrast for normal text, 3:1 for large text
- Test `--ls-primary-text-color` against `--ls-primary-background-color`
- Test `--ls-link-text-color` against backgrounds
- Test `--ls-secondary-text-color` readability
- Use [WebAIM Contrast Checker](https://webaim.org/resources/contrastchecker/) to verify

---

## 7. Testing & Debugging

### Browser DevTools Workflow

1. Open Logseq in the browser or press `Cmd+Shift+I` in the desktop app
2. Select the `<html>` element in the Elements panel
3. View the **Computed** tab to see all resolved CSS variable values
4. Edit `data-theme` / `data-color` attributes directly to preview
5. Add CSS in the **Styles** panel to test changes live

### Quick Variable Override Test

In DevTools Console:

```javascript
// Test accent color
document.documentElement.style.setProperty('--lx-accent-09', '#ff6600')

// Test background
document.documentElement.style.setProperty(
  '--ls-primary-background-color',
  '#1a1a2e'
)

// Reset
document.documentElement.style.removeProperty('--lx-accent-09')
```

### Check Both Modes

Always test in both light and dark:

```javascript
// Toggle theme in console
document.documentElement.setAttribute('data-theme', 'dark')
document.documentElement.setAttribute('data-theme', 'light')
```

### Common Issues

| Problem               | Cause                                 | Fix                                                                    |
| --------------------- | ------------------------------------- | ---------------------------------------------------------------------- |
| Colors don't apply    | Built-in accent overrides yours       | Click ⊘ to cancel accent, or use higher-specificity selectors          |
| Dark mode looks wrong | Missing `html[data-theme=dark]` rules | Define both light and dark mode variables                              |
| Icons wrong color     | Missing `--ls-icon-color`             | Set it explicitly                                                      |
| Scrollbars unchanged  | Missing scrollbar variables           | Set `--ls-scrollbar-*` or `--lx-gray-05/02` variables                  |
| Code blocks unstyled  | Missing inline code vars              | Set `--ls-page-inline-code-bg-color` and `--ls-page-inline-code-color` |

---

## 8. Examples

### Existing Example Themes

| Theme       | File                                                                       | Style                      |
| ----------- | -------------------------------------------------------------------------- | -------------------------- |
| Gruvbox     | [dz/theme/gruvbox-logseq-theme.css](../theme/gruvbox-logseq-theme.css)     | Warm retro (orange accent) |
| Rosé Pine   | [dz/theme/rose-pine-logseq-theme.css](../theme/rose-pine-logseq-theme.css) | Soft muted (rose accent)   |
| Styled Tags | [dz/theme/styled-tags-theme.css](../theme/styled-tags-theme.css)           | Tag enhancement only       |

### Minimal Dark Override (10 lines)

```css
html[data-theme='dark'] {
  --ls-primary-background-color: #0d1117;
  --ls-secondary-background-color: #161b22;
  --ls-primary-text-color: #e6edf3;
  --ls-link-text-color: #58a6ff;
  --ls-border-color: #30363d;
  --lx-accent-09: #58a6ff;
}
```

### Complete Font Override

```css
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono&display=swap');

:root {
  --ls-font-family: 'IBM Plex Sans', sans-serif;
}

/* Also apply to editor content */
.editor-inner .ls-block {
  font-family: 'IBM Plex Sans', sans-serif;
}

/* Monospace for code blocks */
.editor-inner code,
.editor-inner pre {
  font-family: 'IBM Plex Mono', monospace;
}
```

---

## Checklist

Before shipping your theme:

- [ ] Both light and dark modes defined
- [ ] All 12 accent steps defined for both modes
- [ ] All 12 gray steps defined for both modes
- [ ] All `--lx-accent-*` variables mapped (01–12)
- [ ] All `--lx-gray-*` variables mapped (01–12)
- [ ] All `--ls-*` legacy variables mapped
- [ ] shadcn tokens set for dark mode (`--background`, `--primary`, etc.)
- [ ] Text has WCAG AA contrast (4.5:1) against backgrounds
- [ ] Links are visually distinct from body text
- [ ] Tag styling tested
- [ ] Scrollbar colors set
- [ ] Tested in both theme modes
- [ ] Tested with sidebar open/closed
- [ ] Tested with properties panel visible
- [ ] Tested cloze cards and code blocks
