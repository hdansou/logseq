---
title: 'Build a Custom Theme'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Tutorial'
platform: 'Desktop + Mobile'
related-docs:
  - '[Custom CSS Guide](../../how-to/customization/custom-css.md)'
  - '[Theme Development](../../developer/theme-development.md)'
  - '[CSS Variables Reference](../../reference/styling/css-variables.md)'
tags: ['themes', 'CSS', 'customization', 'design']
---

# Build a Custom Theme

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
touch custom.css
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
  "logseq": {
    "id": "logseq-theme-modern",
    "title": "Modern Theme",
    "icon": "🎨",
    "description": "A modern, clean theme for Logseq",
    "effect": true,
    "themes": [
      {
        "name": "Modern Light",
        "url": "./custom.css",
        "description": "Light mode theme",
        "mode": "light"
      },
      {
        "name": "Modern Dark",
        "url": "./custom.css",
        "description": "Dark mode theme",
        "mode": "dark"
      }
    ]
  }
}
```

**Step 3: Development Setup**

Install in Logseq:

```markdown
1. Open Logseq
2. Go to Settings → Plugins → Load unpacked plugin
3. Select your theme directory
4. Enable the theme
```

---

## Project Structure

### File Organization

**Complete Structure:**

```
logseq-theme-modern/
├── custom.css          # Main theme styles
├── package.json        # Theme configuration
├── README.md          # Documentation
├── LICENSE            # License file
├── screenshots/       # Preview images
│   ├── light-mode.png
│   └── dark-mode.png
├── src/              # Optional: source files
│   ├── _variables.css
│   ├── _typography.css
│   ├── _components.css
│   └── _layout.css
└── .github/          # GitHub metadata
    └── workflows/
        └── release.yml
```

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

**Step 1: Define Color Palette**

In `custom.css`:

```css
/* ============================
   MODERN THEME - COLOR SYSTEM
   ============================ */

/* Light Mode Variables */
.white-theme,
html[data-theme='light'] {
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

  /* Code Colors */
  --ls-codeblock-background-color: #f7fafc;
  --ls-code-text-color: #2d3748;

  /* Scrollbar */
  --ls-scrollbar-background-color: #edf2f7;
  --ls-scrollbar-thumb-hover-color: #a0aec0;
}

/* Dark Mode Variables */
.dark-theme,
html[data-theme='dark'] {
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

  /* Code Colors */
  --ls-codeblock-background-color: #2d3748;
  --ls-code-text-color: #e2e8f0;

  /* Scrollbar */
  --ls-scrollbar-background-color: #2d3748;
  --ls-scrollbar-thumb-hover-color: #4a5568;
}
```

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

**Step 3: Apply to Markers**

```css
/* Task Markers */
.form-checkbox {
  border-radius: 4px;
  border-width: 2px;
}

/* TODO Marker */
.marker-switch[data-ref='TODO'],
a.tag[data-ref='TODO'] {
  color: var(--color-todo) !important;
  background-color: transparent;
  border: 2px solid var(--color-todo);
  padding: 2px 8px;
  border-radius: 6px;
  font-weight: 600;
}

/* DOING Marker */
.marker-switch[data-ref='DOING'],
a.tag[data-ref='DOING'] {
  color: var(--color-doing) !important;
  background-color: var(--color-doing);
  padding: 2px 8px;
  border-radius: 6px;
  font-weight: 600;
}

/* DONE Marker */
.marker-switch[data-ref='DONE'],
a.tag[data-ref='DONE'] {
  color: var(--color-success) !important;
  background-color: transparent;
  border: 2px solid var(--color-success);
  padding: 2px 8px;
  border-radius: 6px;
  font-weight: 600;
}
```

---

## Typography

### Font System

**Step 1: Import Fonts**

```css
/* Import Google Fonts */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&display=swap');

/* Font Variables */
:root {
  /* Font Families */
  --ls-font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
    'Helvetica Neue', Arial, sans-serif;
  --ls-code-font-family: 'JetBrains Mono', 'Fira Code', Menlo, Monaco,
    'Courier New', monospace;

  /* Font Sizes */
  --ls-font-size-xs: 0.75rem; /* 12px */
  --ls-font-size-sm: 0.875rem; /* 14px */
  --ls-font-size-base: 0.9375rem; /* 15px */
  --ls-font-size-md: 1rem; /* 16px */
  --ls-font-size-lg: 1.125rem; /* 18px */
  --ls-font-size-xl: 1.25rem; /* 20px */
  --ls-font-size-2xl: 1.5rem; /* 24px */
  --ls-font-size-3xl: 1.875rem; /* 30px */
  --ls-font-size-4xl: 2.25rem; /* 36px */

  /* Line Heights */
  --ls-line-height-tight: 1.4;
  --ls-line-height-base: 1.6;
  --ls-line-height-relaxed: 1.8;

  /* Font Weights */
  --ls-font-weight-light: 300;
  --ls-font-weight-normal: 400;
  --ls-font-weight-medium: 500;
  --ls-font-weight-semibold: 600;
  --ls-font-weight-bold: 700;
}
```

**Step 2: Apply Typography**

```css
/* Base Typography */
body {
  font-family: var(--ls-font-family);
  font-size: var(--ls-font-size-base);
  line-height: var(--ls-line-height-base);
  font-weight: var(--ls-font-weight-normal);
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

/* Headings */
h1.title,
h1,
.text-xl {
  font-size: var(--ls-font-size-4xl);
  font-weight: var(--ls-font-weight-bold);
  line-height: var(--ls-line-height-tight);
  margin-top: 2rem;
  margin-bottom: 1rem;
}

h2,
.text-lg {
  font-size: var(--ls-font-size-3xl);
  font-weight: var(--ls-font-weight-bold);
  line-height: var(--ls-line-height-tight);
  margin-top: 1.5rem;
  margin-bottom: 0.75rem;
}

h3 {
  font-size: var(--ls-font-size-2xl);
  font-weight: var(--ls-font-weight-semibold);
  line-height: var(--ls-line-height-tight);
  margin-top: 1.25rem;
  margin-bottom: 0.5rem;
}

h4 {
  font-size: var(--ls-font-size-xl);
  font-weight: var(--ls-font-weight-semibold);
  line-height: var(--ls-line-height-base);
}

h5 {
  font-size: var(--ls-font-size-lg);
  font-weight: var(--ls-font-weight-medium);
}

h6 {
  font-size: var(--ls-font-size-md);
  font-weight: var(--ls-font-weight-medium);
}
```

**Step 3: Content Typography**

```css
/* Paragraphs */
.block-content p {
  margin-bottom: 0.75rem;
  line-height: var(--ls-line-height-relaxed);
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
  font-family: var(--ls-code-font-family);
  font-size: 0.9em;
  padding: 0.2em 0.4em;
  background-color: var(--ls-codeblock-background-color);
  border-radius: 4px;
}

pre code {
  padding: 0;
  background-color: transparent;
}

/* Block References */
.block-ref {
  font-weight: var(--ls-font-weight-medium);
  border-bottom: 1px dotted var(--ls-border-color);
}

/* Tags */
a.tag {
  font-size: var(--ls-font-size-sm);
  font-weight: var(--ls-font-weight-medium);
  padding: 2px 8px;
  border-radius: 6px;
  background-color: var(--ls-tertiary-background-color);
}
```

---

## Component Styling

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

/* Block Reference Counter */
.block-ref-count {
  font-size: 0.75em;
  color: var(--ls-secondary-text-color);
  margin-left: 4px;
}
```

**Step 3: Block Properties**

```css
/* Block Properties Container */
.block-properties {
  margin-top: 8px;
  padding: 8px 12px;
  background-color: var(--ls-secondary-background-color);
  border-left: 3px solid var(--ls-active-primary-color);
  border-radius: 6px;
  font-size: var(--ls-font-size-sm);
}

/* Property Row */
.block-property {
  display: flex;
  align-items: center;
  margin: 4px 0;
}

/* Property Key */
.block-property-key {
  font-weight: var(--ls-font-weight-semibold);
  color: var(--ls-primary-text-color);
  margin-right: 8px;
}

/* Property Value */
.block-property-value {
  color: var(--ls-secondary-text-color);
}
```

### Sidebar

**Sidebar Layout:**

```css
/* Left Sidebar */
#left-sidebar {
  background-color: var(--ls-secondary-background-color);
  border-right: 1px solid var(--ls-border-color);
}

/* Sidebar Header */
.left-sidebar-inner .nav-header {
  padding: 16px 20px;
  border-bottom: 1px solid var(--ls-border-color);
}

/* Sidebar Item */
.nav-content-item {
  padding: 8px 20px;
  border-radius: 8px;
  margin: 2px 12px;
  transition: all 0.15s ease;
}

.nav-content-item:hover {
  background-color: var(--ls-tertiary-background-color);
  transform: translateX(4px);
}

.nav-content-item.active {
  background-color: var(--ls-active-primary-color);
  color: white;
  font-weight: var(--ls-font-weight-semibold);
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
  font-weight: var(--ls-font-weight-semibold);
  font-size: var(--ls-font-size-lg);
  margin-bottom: 12px;
}
```

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
  font-size: var(--ls-font-size-base);
  line-height: var(--ls-line-height-relaxed);
  color: var(--ls-primary-text-color);
}

/* Placeholder */
.editor-inner textarea::placeholder {
  color: var(--ls-secondary-text-color);
  opacity: 0.6;
}

/* Editor Toolbar */
.editor-toolbar {
  display: flex;
  gap: 8px;
  padding: 8px 0;
  border-top: 1px solid var(--ls-border-color);
  margin-top: 8px;
}

.editor-toolbar-btn {
  padding: 6px 12px;
  border-radius: 6px;
  background-color: var(--ls-secondary-background-color);
  border: 1px solid var(--ls-border-color);
  cursor: pointer;
  transition: all 0.15s ease;
}

.editor-toolbar-btn:hover {
  background-color: var(--ls-tertiary-background-color);
  border-color: var(--ls-active-primary-color);
}
```

### Search

**Search Component:**

```css
/* Search Input */
#search-field {
  padding: 12px 16px;
  border-radius: 8px;
  background-color: var(--ls-secondary-background-color);
  border: 2px solid var(--ls-border-color);
  font-size: var(--ls-font-size-base);
  transition: all 0.2s ease;
}

#search-field:focus {
  outline: none;
  border-color: var(--ls-active-primary-color);
  box-shadow: 0 0 0 3px rgba(43, 108, 176, 0.1);
}

/* Search Results */
.search-results {
  background-color: var(--ls-primary-background-color);
  border-radius: 8px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  margin-top: 8px;
  max-height: 400px;
  overflow-y: auto;
}

/* Search Result Item */
.search-result-item {
  padding: 12px 16px;
  border-bottom: 1px solid var(--ls-border-color);
  cursor: pointer;
  transition: background-color 0.15s ease;
}

.search-result-item:hover {
  background-color: var(--ls-tertiary-background-color);
}

.search-result-item.selected {
  background-color: var(--ls-block-highlight-color);
  border-left: 3px solid var(--ls-active-primary-color);
}

/* Search Highlight */
.search-result-item .highlight {
  background-color: #fef3c7;
  color: #92400e;
  padding: 2px 4px;
  border-radius: 3px;
  font-weight: var(--ls-font-weight-semibold);
}

.dark-theme .search-result-item .highlight {
  background-color: #78350f;
  color: #fef3c7;
}
```

### Modals

**Modal Styling:**

```css
/* Modal Overlay */
.ui__modal-overlay {
  background-color: rgba(0, 0, 0, 0.5);
  backdrop-filter: blur(4px);
}

/* Modal Panel */
.ui__modal-panel {
  background-color: var(--ls-primary-background-color);
  border-radius: 12px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
  padding: 24px;
  max-width: 600px;
  width: 90%;
}

/* Modal Header */
.ui__modal-panel-header {
  font-size: var(--ls-font-size-2xl);
  font-weight: var(--ls-font-weight-bold);
  margin-bottom: 16px;
  padding-bottom: 12px;
  border-bottom: 2px solid var(--ls-border-color);
}

/* Modal Content */
.ui__modal-panel-content {
  margin: 16px 0;
  max-height: 60vh;
  overflow-y: auto;
}

/* Modal Footer */
.ui__modal-panel-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid var(--ls-border-color);
}

/* Modal Buttons */
.ui__modal-panel-footer .button {
  padding: 10px 20px;
  border-radius: 8px;
  font-weight: var(--ls-font-weight-semibold);
  cursor: pointer;
  transition: all 0.15s ease;
}

.ui__modal-panel-footer .button-primary {
  background-color: var(--ls-active-primary-color);
  color: white;
  border: none;
}

.ui__modal-panel-footer .button-primary:hover {
  background-color: var(--ls-link-text-hover-color);
  transform: translateY(-1px);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
}

.ui__modal-panel-footer .button-secondary {
  background-color: transparent;
  color: var(--ls-primary-text-color);
  border: 2px solid var(--ls-border-color);
}

.ui__modal-panel-footer .button-secondary:hover {
  background-color: var(--ls-secondary-background-color);
  border-color: var(--ls-active-primary-color);
}
```

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

/* Wide Layout (for large screens) */
@media (min-width: 1400px) {
  #main-content {
    max-width: 1100px;
  }
}

/* Narrow Layout (for small screens) */
@media (max-width: 768px) {
  #main-content {
    padding: 16px 20px;
  }
}
```

### Page Header

**Header Styling:**

```css
/* Page Header */
.page-header {
  padding: 24px 0;
  border-bottom: 2px solid var(--ls-border-color);
  margin-bottom: 32px;
}

/* Page Title */
.page-title {
  font-size: var(--ls-font-size-4xl);
  font-weight: var(--ls-font-weight-bold);
  color: var(--ls-title-text-color);
  margin-bottom: 12px;
}

/* Page Properties */
.page-properties-area {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 16px;
}

.page-property-item {
  display: flex;
  align-items: center;
  padding: 6px 12px;
  background-color: var(--ls-secondary-background-color);
  border-radius: 8px;
  font-size: var(--ls-font-size-sm);
}
```

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

```css
/* Mobile: < 768px */
@media (max-width: 767px) {
  /* Reduce font sizes */
  :root {
    --ls-font-size-base: 0.875rem;
    --ls-font-size-4xl: 1.75rem;
    --ls-font-size-3xl: 1.5rem;
  }

  /* Adjust spacing */
  #main-content {
    padding: 16px;
  }

  /* Single column layout */
  .flex-row {
    flex-direction: column;
  }

  /* Hide sidebar by default */
  #left-sidebar {
    position: fixed;
    left: -280px;
    transition: left 0.3s ease;
  }

  #left-sidebar.is-open {
    left: 0;
    z-index: 999;
  }

  /* Simplify block layout */
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
- [ ] Light/dark mode switching works
- [ ] Link colors have sufficient contrast
- [ ] Markers (TODO, DOING, DONE) colored correctly
- [ ] Tags styled appropriately

### Typography

- [ ] Fonts load correctly
- [ ] Font sizes appropriate
- [ ] Line height comfortable
- [ ] Code font monospaced
- [ ] Headings hierarchy clear

### Components

- [ ] Blocks: hover, selected states
- [ ] Bullet: hover animation
- [ ] Block references styled
- [ ] Properties display correctly
- [ ] Tags render properly
- [ ] Code blocks formatted
- [ ] Search results readable

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

2. **Check CSS Variables:**

   - Inspect root element
   - Look at computed styles
   - Verify variable values

3. **Test Responsive:**

   - Toggle device toolbar
   - Test various screen sizes
   - Check mobile breakpoints

4. **Check Contrast:**

   - Use Lighthouse accessibility audit
   - Verify WCAG AA compliance (4.5:1 text)
   - Check focus indicators visible

5. **Performance:**
   - Check CSS file size
   - Look for unused selectors
   - Optimize if needed
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

1. Go to Settings → Edit custom.css
2. Override CSS variables:

```css
:root {
  --ls-primary-background-color: #YOUR-COLOR;
  --ls-link-text-color: #YOUR-COLOR;
}
```
````

## Compatibility

- Logseq version: 0.10.0+
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
1. Fork Logseq marketplace repository
2. Add your theme to plugins.json:

```json
{
  "id": "logseq-theme-modern",
  "name": "Modern Theme",
  "author": "Your Name",
  "description": "A modern, clean theme for Logseq",
  "repo": "yourusername/logseq-theme-modern",
  "icon": "🎨",
  "theme": true
}
```
````

3. Create pull request
4. Wait for review and approval

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
- [CSS Variables Reference](../../reference/styling/css-variables.md)
- [Logseq Discord #themes Channel](https://discord.gg/logseq)
- [Theme Development Guide](../../developer/theme-development.md)

**Tips for Success:**

- Start simple, add complexity gradually
- Test on multiple devices and browsers
- Listen to user feedback
- Keep theme maintainable
- Document your design decisions
- Share your work with the community

Happy theming! 🎨✨
