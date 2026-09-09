# Logseq Theme Development Guide

> **Last revised:** 2026-05-29

**Version:** 1.0
**Target Audience:** Theme Developers

## Table of Contents

1. [Introduction](#introduction)
2. [Understanding Logseq Modes](#understanding-logseq-modes)
3. [Design System Overview](#design-system-overview)
4. [Theme Architecture](#theme-architecture)
5. [Quick Start Guide](#quick-start-guide)
6. [CSS Variables Reference](#css-variables-reference)
7. [Component Styling](#component-styling)
8. [Best Practices](#best-practices)
9. [Testing Your Theme](#testing-your-theme)
10. [Publishing Your Theme](#publishing-your-theme)
11. [Findings: New Capabilities](#findings-new-capabilities)

---

## Introduction

Logseq themes are custom CSS files that override the default appearance of the application. The theming system is built on:

- **shadcn/ui** - Modern component library based on Radix UI
- **Radix Colors** - Professional color scales for accessible design
- **TailwindCSS** - Utility-first CSS framework
- **CSS Variables** - For dynamic theming

### Two Theme Approaches

1. **Custom CSS File**: Place `custom.css` in `logseq/` directory
2. **Plugin Theme**: Distribute as a Logseq plugin with theme manifest

---

## Understanding Logseq Modes

Logseq operates in two distinct modes that affect file organization and theming:

### 1. File-Based Mode (Traditional)

- Stores notes as individual Markdown/Org files
- File structure: `pages/`, `journals/`, `logseq/`
- Custom CSS location: `<graph-root>/logseq/custom.css`
- Best for: Users who prefer file system control

### 2. Database Mode (DB-Based)

- Stores notes in a SQLite database
- Faster queries and better performance
- Custom CSS location: Still in `logseq/custom.css` (virtual file system)
- Best for: Large graphs with complex queries

**Important**: Themes work identically in both modes. The difference is primarily in how the underlying data is stored, not in how CSS is applied.

> **Removed:** `html.is-file-graph` / `set-file-graph-flag!` mode-detection hooks are no longer present in current `src/`, `deps/`, or `packages/` sources.

---

## Design System Overview

### Core Technologies

#### 1. shadcn/ui Components

Logseq uses shadcn/ui (formerly SHUI) for its component library:

```clojure
;; Component example in ClojureScript
(ns my-component
  (:require [logseq.shui.ui :as shui]))

(shui/button {:class "my-custom-button"}
  "Click Me")
```

shadcn/ui components are styled using TailwindCSS and can be customized via CSS variables.

#### 2. Radix Colors

Logseq uses Radix Colors for its color system, providing:

- **12-step scales** for each color
- **Automatic dark mode** variants
- **Alpha variants** for transparency
- **Semantic color names**

Built-in accent options in current app settings:

- `logseq` (default)
- `tomato`, `red`, `crimson`, `pink`, `plum`, `purple`, `violet`, `indigo`, `blue`, `cyan`, `teal`, `green`, `grass`, `orange`
- `none` (disable accent mapping for custom theme compatibility)

Radix base palettes (`--rx-*`) remain broader than the built-in accent picker.

#### 3. CSS Variable System

Three layers of variables:

1. **Radix Base** (`--rx-*`): Raw Radix Colors
2. **Logseq Extended** (`--lx-*`): Logseq-specific mappings
3. **Legacy Variables** (`--ls-*`): Backward compatibility

---

## Theme Architecture

### File Structure

```text
your-theme/
├── custom.css              # Main theme file
├── package.json            # Theme metadata (for plugins)
├── README.md              # Documentation
└── screenshots/           # Preview images
    ├── light-mode.png
    └── dark-mode.png
```

### Basic Theme Template

```css
/* =============================================================================
   Theme Name: My Awesome Theme
   Author: Your Name
   Version: 1.0.0
   Description: A beautiful theme for Logseq
   ============================================================================= */

/* Import additional fonts if needed */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* =============================================================================
   1. ROOT VARIABLES - LIGHT MODE
   ============================================================================= */

html[data-theme='light'] {
  /* Primary Colors */
  --lx-gray-01: #ffffff;
  --lx-gray-02: #fafafa;
  --lx-gray-03: #f5f5f5;
  --lx-gray-04: #eeeeee;
  --lx-gray-05: #e0e0e0;
  --lx-gray-06: #bdbdbd;
  --lx-gray-07: #9e9e9e;
  --lx-gray-08: #757575;
  --lx-gray-09: #616161;
  --lx-gray-10: #424242;
  --lx-gray-11: #2b2b2b;
  --lx-gray-12: #1a1a1a;

  /* Accent Colors */
  --lx-accent-09: #2563eb; /* Primary accent */
  --lx-accent-10: #1d4ed8;
  --lx-accent-11: #1e40af;

  /* Legacy Variables (for compatibility) */
  --ls-primary-background-color: var(--lx-gray-01);
  --ls-secondary-background-color: var(--lx-gray-02);
  --ls-tertiary-background-color: var(--lx-gray-03);
  --ls-quaternary-background-color: var(--lx-gray-04);

  --ls-primary-text-color: var(--lx-gray-12);
  --ls-secondary-text-color: var(--lx-gray-11);
  --ls-link-text-color: var(--lx-accent-09);
  --ls-link-text-hover-color: var(--lx-accent-10);

  /* Border & Lines */
  --ls-border-color: var(--lx-gray-05);
  --ls-secondary-border-color: var(--lx-gray-04);

  /* UI Elements */
  --ls-block-bullet-color: var(--lx-gray-08);
  --ls-page-checkbox-color: var(--lx-gray-06);
  --ls-scrollbar-foreground-color: var(--lx-gray-05);
  --ls-scrollbar-background-color: var(--lx-gray-02);
}

/* =============================================================================
   2. ROOT VARIABLES - DARK MODE
   ============================================================================= */

html[data-theme='dark'] {
  /* Primary Colors */
  --lx-gray-01: #0a0a0a;
  --lx-gray-02: #1a1a1a;
  --lx-gray-03: #2a2a2a;
  --lx-gray-04: #3a3a3a;
  --lx-gray-05: #4a4a4a;
  --lx-gray-06: #5a5a5a;
  --lx-gray-07: #6a6a6a;
  --lx-gray-08: #8a8a8a;
  --lx-gray-09: #9a9a9a;
  --lx-gray-10: #aaaaaa;
  --lx-gray-11: #cccccc;
  --lx-gray-12: #eeeeee;

  /* Accent Colors */
  --lx-accent-09: #3b82f6;
  --lx-accent-10: #60a5fa;
  --lx-accent-11: #93c5fd;

  /* Legacy Variables */
  --ls-primary-background-color: var(--lx-gray-01);
  --ls-secondary-background-color: var(--lx-gray-02);
  --ls-tertiary-background-color: var(--lx-gray-03);
  --ls-quaternary-background-color: var(--lx-gray-04);

  --ls-primary-text-color: var(--lx-gray-12);
  --ls-secondary-text-color: var(--lx-gray-11);
  --ls-link-text-color: var(--lx-accent-09);
  --ls-link-text-hover-color: var(--lx-accent-10);

  --ls-border-color: var(--lx-gray-05);
  --ls-secondary-border-color: var(--lx-gray-04);

  --ls-block-bullet-color: var(--lx-gray-08);
  --ls-page-checkbox-color: var(--lx-gray-06);
  --ls-scrollbar-foreground-color: var(--lx-gray-05);
  --ls-scrollbar-background-color: var(--lx-gray-02);
}

/* =============================================================================
   3. COMPONENT CUSTOMIZATION
   ============================================================================= */

/* Typography */
.ls-block {
  font-family:
    'Inter',
    -apple-system,
    BlinkMacSystemFont,
    sans-serif;
  font-size: 1rem;
  line-height: 1.6;
}

/* Buttons (shadcn/ui) */
.ui__button {
  border-radius: 8px;
  font-weight: 500;
}

/* Cards */
.ui__card {
  border-radius: 12px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

/* Modals/Dialogs */
.ui__dialog-content {
  border-radius: 16px;
}

/* Dropdowns */
.ui__dropdown-menu-content {
  border-radius: 8px;
  border: 1px solid var(--ls-border-color);
}

/* Code blocks */
.extensions__code {
  border-radius: 8px;
  background: var(--ls-secondary-background-color);
}

/* Block References */
.block-ref {
  border-bottom: 2px solid var(--ls-link-text-color);
  opacity: 0.8;
}

.block-ref:hover {
  opacity: 1;
  background-color: var(--lx-accent-02);
}

/* =============================================================================
   4. CUSTOM ENHANCEMENTS (OPTIONAL)
   ============================================================================= */

/* Smooth scrolling */
html {
  scroll-behavior: smooth;
}

/* Custom focus rings */
*:focus-visible {
  outline: 2px solid var(--lx-accent-09);
  outline-offset: 2px;
}

/* Animated transitions */
.ls-block,
.ui__button,
.ui__card {
  transition: all 0.2s ease-in-out;
}
```

---

## Quick Start Guide

### Method 1: Direct Custom CSS (Quickest)

1. **Open your graph** in Logseq
2. **Navigate to Settings** (click gear icon)
3. **Click "Edit custom.css"** under "General"
4. **Paste your theme CSS**
5. **Save** - Changes apply immediately!

### Method 2: Plugin Theme (For Distribution)

1. **Create theme structure:**

   ```bash
   mkdir my-logseq-theme
   cd my-logseq-theme
   ```

2. **Create `package.json`:**

   ```json
   {
     "name": "logseq-theme-my-awesome-theme",
     "version": "1.0.0",
     "description": "A beautiful theme for Logseq",
     "author": "Your Name",
     "license": "MIT",
     "main": "index.html",
     "logseq": {
       "id": "my-awesome-theme",
       "main": "index.html",
       "title": "My Awesome Theme",
       "icon": "./icon.png",
       "effect": true,
       "themes": [
         {
           "name": "My Awesome Theme",
           "url": "./custom.css",
           "description": "A beautiful theme with great typography",
           "mode": "light"
         },
         {
           "name": "My Awesome Theme Dark",
           "url": "./custom.css",
           "description": "Dark variant of My Awesome Theme",
           "mode": "dark"
         }
       ]
     }
   }
   ```

3. **Create `custom.css`** (use template above)

4. **Test locally:**
   - Copy theme folder to `<graph-root>/.logseq/plugins/`
   - Reload Logseq
   - Enable in Settings → Themes

---

## CSS Variables Reference

### Color System Hierarchy

```css
/* 1. Radix Base Colors (don't modify directly) */
--rx-gray-01 through --rx-gray-12
--rx-blue-01 through --rx-blue-12
/* ... etc for all colors */

/* 2. Logseq Extended (customize these) */
--lx-gray-01 through --lx-gray-12     /* Grayscale */
--lx-accent-01 through --lx-accent-12 /* Accent color */
--lx-bg-override                      /* Background override */

/* 3. Legacy Logseq (for compatibility) */
--ls-primary-background-color
--ls-secondary-background-color
--ls-tertiary-background-color
--ls-primary-text-color
--ls-link-text-color
/* ...over 50 legacy variables */
```

> **Removed:** `--lx-text-override` is not present in current theme CSS sources.

`--lx-bg-override` is currently used by shui/theme styling for contextual background overrides.

### Essential Variables by Category

#### Backgrounds

```css
--ls-primary-background-color    /* Main canvas */
--ls-secondary-background-color  /* Sidebars, cards */
--ls-tertiary-background-color   /* Hover states */
--ls-quaternary-background-color /* Active states */
```

#### Text

```css
--ls-primary-text-color          /* Main text */
--ls-secondary-text-color        /* Muted text */
--ls-title-text-color            /* Headings */
--ls-link-text-color             /* Links */
--ls-link-text-hover-color       /* Link hover */
```

#### Borders & Lines

```css
--ls-border-color                /* Default borders */
--ls-secondary-border-color      /* Subtle borders */
--ls-guideline-color             /* Vertical guides */
--ls-block-bullet-border-color   /* Bullet borders */
```

#### Interactive Elements

```css
--ls-block-bullet-color          /* Bullet points */
--ls-page-checkbox-color         /* Checkboxes */
--ls-selection-background-color  /* Text selection */
--ls-block-highlight-color       /* Block highlight */
```

#### Code & Special Content

```css
--ls-page-inline-code-bg-color   /* Inline code bg */
--ls-page-inline-code-color      /* Inline code text */
--ls-page-blockquote-bg-color    /* Quote background */
--ls-page-mark-bg-color          /* Highlighted text */
```

### shadcn/ui Variables

```css
--background          /* Page background */
--foreground          /* Primary text */
--card                /* Card background */
--card-foreground     /* Card text */
--popover             /* Popup background */
--popover-foreground  /* Popup text */
--primary             /* Primary color */
--primary-foreground  /* Text on primary */
--secondary           /* Secondary color */
--muted               /* Muted background */
--muted-foreground    /* Muted text */
--accent              /* Accent color */
--accent-foreground   /* Text on accent */
--destructive         /* Error/delete color */
--border              /* Border color */
--input               /* Input border */
--ring                /* Focus ring */
--radius              /* Border radius */
```

---

## Component Styling

### shadcn/ui Components

#### Buttons

```css
/* Base button styles */
.ui__button {
  /* Variants: .as-primary, .as-secondary, .as-ghost, .as-outline */
}

.ui__button.as-primary {
  background-color: var(--lx-accent-09);
  color: white;
}

.ui__button.as-ghost {
  background-color: transparent;
}

.ui__button:hover {
  transform: translateY(-1px);
}
```

#### Cards

```css
.ui__card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}

.ui__card-header {
  padding: 1.5rem;
}

.ui__card-content {
  padding: 1.5rem;
}
```

#### Dialogs/Modals

```css
.ui__dialog-overlay {
  background-color: rgba(0, 0, 0, 0.5);
  backdrop-filter: blur(4px);
}

.ui__dialog-content {
  background: var(--card);
  border-radius: 16px;
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
}
```

#### Dropdowns

```css
.ui__dropdown-menu-content {
  background: var(--popover);
  border: 1px solid var(--border);
  border-radius: 8px;
  box-shadow: 0 10px 25px rgba(0, 0, 0, 0.1);
}

.ui__dropdown-menu-item {
  padding: 0.5rem 1rem;
}

.ui__dropdown-menu-item:hover {
  background: var(--accent);
  color: var(--accent-foreground);
}
```

### Core Logseq Components

#### Blocks

```css
/* Block container */
.ls-block {
  padding: 4px 0;
}

/* Block bullet */
.ls-block .bullet-container {
  width: 20px;
}

.ls-block .bullet {
  background-color: var(--ls-block-bullet-color);
  width: 6px;
  height: 6px;
  border-radius: 50%;
}

/* Block content */
.block-content {
  flex: 1;
}

/* Nested blocks */
.block-children {
  margin-left: 2rem;
}

/* Block reference */
.block-ref {
  border-bottom: 2px solid var(--ls-link-text-color);
  padding: 0 2px;
}
```

#### Sidebar

```css
/* Left sidebar */
.cp__sidebar-left-layout {
  background: var(--ls-secondary-background-color);
  width: var(--ls-left-sidebar-width);
}

/* Right sidebar */
.cp__right-sidebar {
  background: var(--ls-primary-background-color);
  border-left: 1px solid var(--ls-border-color);
}

.sidebar-item {
  padding: 1rem;
  border-bottom: 1px solid var(--ls-border-color);
}
```

#### Header

```css
.cp__header {
  height: var(--ls-headbar-height);
  background: var(--ls-primary-background-color);
  border-bottom: 1px solid var(--ls-border-color);
  padding: 0 1rem;
}

.cp__header .logo {
  font-weight: 600;
  color: var(--ls-primary-text-color);
}
```

#### Editor

```css
/* Editor container */
.editor-wrapper {
  max-width: var(--ls-main-content-max-width);
  margin: 0 auto;
  padding: 2rem;
}

/* Editor input */
.editor-input {
  font-size: var(--ls-page-text-size);
  line-height: 1.6;
  color: var(--ls-primary-text-color);
}

/* Placeholder */
.editor-input::placeholder {
  color: var(--ls-secondary-text-color);
  opacity: 0.6;
}
```

### Content Styling

#### Headings

```css
h1.title,
h1 {
  font-size: 2em;
  font-weight: 700;
  color: var(--ls-title-text-color);
  margin-bottom: 0.5em;
}

h2 {
  font-size: 1.5em;
  font-weight: 600;
  margin-top: 1em;
  margin-bottom: 0.5em;
}

h3 {
  font-size: 1.25em;
  font-weight: 600;
}
```

#### Links & Tags

```css
a {
  color: var(--ls-link-text-color);
  text-decoration: none;
}

a:hover {
  color: var(--ls-link-text-hover-color);
  text-decoration: underline;
}

/* Tags */
.tag {
  background: var(--lx-accent-03);
  color: var(--lx-accent-11);
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.875em;
}
```

#### Code

```css
/* Inline code */
code {
  background: var(--ls-page-inline-code-bg-color);
  color: var(--ls-page-inline-code-color);
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 0.9em;
  font-family: 'Monaco', 'Menlo', monospace;
}

/* Code blocks */
.extensions__code {
  background: var(--ls-secondary-background-color);
  border: 1px solid var(--ls-border-color);
  border-radius: 8px;
  padding: 1rem;
  overflow-x: auto;
}

pre code {
  background: none;
  padding: 0;
}
```

#### Quotes

```css
blockquote {
  background: var(--ls-page-blockquote-bg-color);
  border-left: 4px solid var(--ls-page-blockquote-border-color);
  color: var(--ls-page-blockquote-color);
  padding: 1rem;
  margin: 1rem 0;
  border-radius: 4px;
}
```

#### Tables

```css
table {
  width: 100%;
  border-collapse: collapse;
  margin: 1rem 0;
}

th {
  background: var(--ls-secondary-background-color);
  font-weight: 600;
  text-align: left;
  padding: 0.75rem;
  border: 1px solid var(--ls-border-color);
}

td {
  padding: 0.75rem;
  border: 1px solid var(--ls-border-color);
}

tr:nth-child(even) {
  background: var(--ls-table-tr-even-background-color);
}
```

---

## Best Practices

### 1. Use CSS Variables

✅ **Good:**

```css
.my-component {
  color: var(--ls-primary-text-color);
  background: var(--ls-secondary-background-color);
}
```

❌ **Avoid:**

```css
.my-component {
  color: #333;
  background: #f5f5f5;
}
```

### 2. Support Both Light & Dark Modes

```css
/* Always define both */
html[data-theme='light'] {
  --my-custom-bg: #ffffff;
}

html[data-theme='dark'] {
  --my-custom-bg: #1a1a1a;
}
```

### 3. Respect Accessibility

```css
/* Ensure sufficient contrast */
:root {
  --text-on-background: #000; /* WCAG AA: 4.5:1 ratio */
}

/* Visible focus indicators */
*:focus-visible {
  outline: 2px solid var(--lx-accent-09);
  outline-offset: 2px;
}

/* Smooth motion for users who prefer it */
@media (prefers-reduced-motion: no-preference) {
  * {
    transition:
      color 0.2s,
      background-color 0.2s;
  }
}

/* Respect user preferences */
@media (prefers-reduced-motion: reduce) {
  * {
    transition: none !important;
  }
}
```

### 4. Test Color Combinations

Use color scales appropriately:

```css
/* Good contrast examples */
--text: var(--lx-gray-12); /* Darkest for text */
--background: var(--lx-gray-01); /* Lightest for background */
--border: var(--lx-gray-05); /* Mid-tone for borders */
--subtle-bg: var(--lx-gray-03); /* Subtle background */
```

### 5. Namespace Custom Classes

```css
/* Prefix with your theme name */
.mytheme-custom-button {
}
.mytheme-special-card {
}
```

### 6. Handle Both Graph Modes

> **Note:** The `is-file-graph` CSS class on `<html>` is no longer set in the current codebase. The database version is now the primary mode. Theme styles should target shared selectors that work across modes.

```css
/* Styles that work in both file-based and db-based modes */
.ls-block {
  /* Your styling */
}
```

### 7. Mobile Responsiveness

```css
/* Desktop */
.main-content {
  max-width: var(--ls-main-content-max-width);
}

/* Mobile */
@media (max-width: 640px) {
  .main-content {
    padding: 1rem;
  }

  .cp__sidebar-left-layout {
    width: var(--ls-left-sidebar-sm-width);
  }
}
```

### 8. Performance Tips

```css
/* Use CSS transforms for animations (GPU accelerated) */
.animated-element {
  transform: translateY(0);
  transition: transform 0.2s;
}

.animated-element:hover {
  transform: translateY(-2px);
}

/* Avoid expensive properties in animations */
/* ❌ Avoid: width, height, top, left */
/* ✅ Use: transform, opacity */
```

---

## Testing Your Theme

### Testing Checklist

#### Visual Testing

- [ ] Light mode displays correctly
- [ ] Dark mode displays correctly
- [ ] All accent colors work (test with different colors in Settings)
- [ ] Text is readable on all backgrounds
- [ ] Borders and separators are visible
- [ ] Focus states are clear
- [ ] Hover states work smoothly

#### Component Testing

- [ ] Buttons (all variants)
- [ ] Cards
- [ ] Modals/Dialogs
- [ ] Dropdowns
- [ ] Tooltips
- [ ] Forms (inputs, checkboxes, radios)
- [ ] Tables
- [ ] Code blocks
- [ ] Block references
- [ ] Sidebars (left & right)
- [ ] Header navigation

#### Content Testing

- [ ] Headings (H1-H6)
- [ ] Paragraphs
- [ ] Lists (ordered, unordered, task)
- [ ] Links
- [ ] Tags
- [ ] Block embeds
- [ ] Images
- [ ] Videos
- [ ] PDF annotations
- [ ] Whiteboards

#### Mode Testing

- [ ] File-based graph mode
- [ ] Database graph mode
- [ ] Desktop app
- [ ] Mobile app (iOS/Android)

#### Browser Testing

- [ ] Chrome/Edge
- [ ] Firefox
- [ ] Safari

### Testing Tools

```css
/* Add to bottom of your CSS for debugging */
/*
.ls-block {
  outline: 1px solid red !important;
}

.ls-block .block-content {
  outline: 1px solid blue !important;
}

.ls-block .block-children {
  outline: 1px solid green !important;
}
*/
```

### Common Issues & Fixes

#### Issue: Dark mode too dark

```css
/* Increase background brightness */
html[data-theme='dark'] {
  --lx-gray-01: #1a1a1a; /* Instead of #0a0a0a */
}
```

#### Issue: Links hard to see

```css
/* Increase contrast */
html[data-theme='light'] {
  --ls-link-text-color: #0066cc; /* Darker blue */
}

html[data-theme='dark'] {
  --ls-link-text-color: #60a5fa; /* Lighter blue */
}
```

#### Issue: Bullets disappearing

```css
.ls-block .bullet {
  background-color: var(--ls-block-bullet-color) !important;
  opacity: 1 !important;
}
```

---

## Publishing Your Theme

### Distribution Methods

#### 1. GitHub Repository (Recommended)

```text
your-theme-repo/
├── README.md
├── LICENSE
├── custom.css
├── package.json
├── screenshots/
│   ├── light-mode.png
│   ├── dark-mode.png
│   └── preview.gif
└── .github/
    └── FUNDING.yml (optional)
```

**README.md Template:**

````markdown
# Theme Name

Short description of your theme.

![Preview](screenshots/preview.gif)

## Installation

### Method 1: Direct CSS

1. Download `custom.css`
2. Place in `<your-graph>/logseq/custom.css`
3. Reload Logseq

### Method 2: Plugin

1. Download from Logseq Marketplace
2. Or install via Git: `<username>/<repo>`

## Features

- Feature 1
- Feature 2

## Screenshots

### Light Mode

![Light Mode](screenshots/light-mode.png)

### Dark Mode

![Dark Mode](screenshots/dark-mode.png)

## Customization

### Change accent color

```css
:root {
  --lx-accent-09: #your-color;
}
```
````

## License

MIT

```text
(End of README.md template)
```

### Marketplace Distribution

#### 2. Logseq Marketplace

Requirements:

- Public GitHub repository
- Valid `package.json` with theme metadata
- Screenshots
- Clear README

Submission process:

1. Create theme repository
2. Test thoroughly
3. Submit to Logseq Marketplace via PR
4. Follow marketplace guidelines

#### 3. Community Sharing

- Share on Logseq Discord
- Post on Logseq Reddit
- Tweet with #Logseq hashtag
- Create demo video

### Versioning

Follow semantic versioning (semver):

```json
{
  "version": "1.2.3"
}
```

- **Major (1.x.x)**: Breaking changes
- **Minor (x.2.x)**: New features, backward compatible
- **Patch (x.x.3)**: Bug fixes

### Changelog

Keep a `CHANGELOG.md`:

```markdown
# Changelog

## [1.2.0] - 2025-10-22

### Added

- New button styles
- Support for custom fonts

### Changed

- Improved dark mode contrast
- Updated card shadows

### Fixed

- Button hover state in dark mode
- Sidebar width on mobile
```

---

## Advanced Topics

### Using Radix Colors Directly

```css
/* Access any Radix color */
:root {
  /* Use HSL format */
  --my-custom-red: var(--rx-red-09);
  --my-custom-blue: var(--rx-blue-09);

  /* Use alpha variants */
  --my-overlay: var(--rx-gray-12-alpha);
}
```

### Custom Fonts

```css
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');

:root {
  --ls-font-family: 'Outfit', sans-serif;
}

.ls-block {
  font-family: var(--ls-font-family);
}

/* Code font */
code,
pre {
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}
```

### Animation Examples

```css
/* Smooth page transitions */
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

/* Pulse effect on focus */
.ls-block:focus-within {
  animation: pulse 0.5s ease-out;
}

@keyframes pulse {
  0% {
    box-shadow: 0 0 0 0 var(--lx-accent-09);
  }
  100% {
    box-shadow: 0 0 0 10px transparent;
  }
}
```

### Custom Scrollbars

```css
/* Webkit browsers */
.visible-scrollbar::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.visible-scrollbar::-webkit-scrollbar-track {
  background: var(--ls-scrollbar-background-color);
  border-radius: 4px;
}

.visible-scrollbar::-webkit-scrollbar-thumb {
  background: var(--ls-scrollbar-foreground-color);
  border-radius: 4px;
}

.visible-scrollbar::-webkit-scrollbar-thumb:hover {
  background: var(--ls-scrollbar-thumb-hover-color);
}

/* Firefox */
.visible-scrollbar {
  scrollbar-width: thin;
  scrollbar-color: var(--ls-scrollbar-foreground-color)
    var(--ls-scrollbar-background-color);
}
```

### Glass Morphism

```css
.glassmorphic-card {
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.2);
  border-radius: 16px;
}

html[data-theme='dark'] .glassmorphic-card {
  background: rgba(0, 0, 0, 0.2);
  border: 1px solid rgba(255, 255, 255, 0.1);
}
```

---

## Resources

### Official Documentation

- [Logseq Documentation](https://docs.logseq.com/)
- [shadcn/ui Docs](https://ui.shadcn.com/)
- [Radix Colors](https://www.radix-ui.com/colors)
- [TailwindCSS](https://tailwindcss.com/)

### Color Tools

- [Radix Colors Palette](https://www.radix-ui.com/colors)
- [Contrast Checker](https://webaim.org/resources/contrastchecker/)
- [Coolors](https://coolors.co/) - Color scheme generator

### Inspiration

- [Logseq Discord #themes](https://discord.gg/logseq)
- [Logseq Forum Themes](https://discuss.logseq.com/c/themes)
- [Awesome Logseq](https://github.com/logseq/awesome-logseq)

### Developer Tools

- Browser DevTools (Inspect Element)
- [VS Code](https://code.visualstudio.com/) with CSS IntelliSense
- [GitHub Desktop](https://desktop.github.com/) for version control

---

## FAQ

### Q: Do themes work in both file-based and database modes?

**A:** Yes! Themes apply identically in both modes. The CSS is the same regardless of how Logseq stores your data.

### Q: Can I use CSS preprocessors like SASS/LESS?

**A:** While Logseq only reads CSS, you can compile SASS/LESS to CSS during development and distribute the compiled CSS file.

### Q: How do I handle theme conflicts?

**A:** Use specific selectors and avoid `!important` unless necessary. Namespace your custom classes with your theme name.

### Q: Can themes modify JavaScript behavior?

**A:** No, themes are CSS-only. For JavaScript functionality, create a Logseq plugin instead.

### Q: How do I make my theme work with plugins?

**A:** Test with popular plugins installed. Many plugins use standard Logseq classes, so they'll inherit your theme automatically.

### Q: What's the difference between `--lx-*` and `--ls-*` variables?

**A:** `--lx-*` map semantic accent/gray scales (usually from Radix `--rx-*`) and are used heavily by modern UI styles. `--ls-*` are still widely consumed semantic app variables and remain important for compatibility and component coverage. In practice, robust themes often set both layers.

---

## Findings: New Capabilities

Validated against current sources in `src/`, `deps/`, and `packages/` (2026-05-29):

- Accent reset mode is implemented: selecting `none` sets `html[data-color="none"]`, allowing custom themes to avoid built-in accent remapping.
- Theme runtime now consistently sets `html` attributes for `data-theme`, `data-color`, `data-font`, and `data-font-global`.
- Plugin theming supports both declarative `logseq.themes` (from `package.json`) and programmatic registration via `logseq.provideTheme(...)`.
- Plugin-provided CSS injection via `logseq.provideStyle(...)` remains available for targeted overrides.
- Theme selection persists light/dark theme choices by mode in plugin user preferences.

### Q: Can I create themes that only affect certain pages?

**A:** Yes, use page-specific selectors:

```css
.page[data-page-name='my-page'] {
  /* Custom styles */
}
```

### Q: How do I debug my theme?

**A:** Use browser DevTools (F12), inspect elements, and check the computed styles. Add temporary outlines to see element boundaries.

---

## Conclusion

Creating a Logseq theme is a rewarding way to customize your note-taking experience. With the power of shadcn/ui, Radix Colors, and TailwindCSS, you have a robust foundation for creating beautiful, accessible themes.

### Next Steps

1. Start with the basic template
2. Customize colors to your preference
3. Test in both light and dark modes
4. Share with the community
5. Iterate based on feedback

### Get Help

- [Logseq Discord](https://discord.gg/logseq) - #themes channel
- [Logseq Forum](https://discuss.logseq.com/)
- [GitHub Discussions](https://github.com/logseq/logseq/discussions)

### Contribute

Found an issue with this guide? Have suggestions? [Open an issue](https://github.com/logseq/logseq/issues) or submit a PR!

---

## Happy Theming! 🎨
