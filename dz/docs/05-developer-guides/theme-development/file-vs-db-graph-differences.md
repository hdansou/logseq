# UI & Framework Differences: File-Based vs Database-Based Graphs

> **Last revised:** 2026-03-10

**Purpose:** A comprehensive guide for theme developers to understand and plan for the UI/CSS differences between file-based graphs (v0.10.x) and database-based graphs (v0.10.x+).

**Version:** 1.0
**Target Audience:** Theme developers migrating themes from file-based to database-based graphs

---

## Table of Contents

- [Overview](#overview)
- [Detection and Mode Switching](#detection-and-mode-switching)
- [Core Architectural Differences](#core-architectural-differences)
- [UI Component Differences](#ui-component-differences)
- [CSS Selector Changes](#css-selector-changes)
- [Property System Changes](#property-system-changes)
- [Migration Strategy](#migration-strategy)
- [Testing Your Theme](#testing-your-theme)
- [Common Pitfalls](#common-pitfalls)
- [Resources](#resources)

---

## Overview

### What's Changing?

Logseq 0.10.x introduces **database-based graphs** alongside traditional **file-based graphs**. While both modes share much of the UI framework, there are significant differences in:

1. **Property rendering** - DB graphs use a structured schema-based property system
2. **Page/Class/Tag system** - DB graphs introduce a formal class hierarchy
3. **Visual indicators** - New icons, badges, and type indicators for DB entities
4. **Layout structure** - Some layout adjustments for DB-specific features

### Why It Matters

Themes designed for file-based graphs may experience:

- ❌ **Layout issues** with new property UI components
- ❌ **Missing styles** for class/tag indicators
- ❌ **Visual inconsistencies** in page icons and type badges
- ❌ **Broken selectors** targeting file-specific components

---

## Detection and Mode Switching

### HTML Class Detection

Logseq adds a CSS class to the `<html>` element to indicate graph mode:

```css
/* File-based graph */
html.is-file-graph {
  /* Your file-based specific styles */
}

/* Database-based graph */
html:not(.is-file-graph) {
  /* Your database-based specific styles */
}
```

**Note:** The `is-file-graph` CSS class and the `set-file-graph-flag!` function have been removed from the current codebase. The database version is now the primary mode. The selectors above are retained for historical reference; new themes should not rely on this class for mode detection.

### Theme Implementation

```css
/* Base styles for both modes */
.ls-block {
  padding: 2px 0;
}

/* File-based specific */
html.is-file-graph {
  #main-content-container {
    padding-top: 0.5rem;
  }

  .page-inner {
    gap: 0.5rem;
  }
}

/* DB-based specific */
html:not(.is-file-graph) {
  .ls-page-properties {
    /* Enhanced property rendering */
    margin-top: 1rem;
  }

  .type-icon {
    /* Class/tag type indicators */
    opacity: 1;
  }
}
```

---

## Core Architectural Differences

### File-Based Graphs (Traditional)

**Storage:** Markdown/Org files on disk  
**Properties:** Front-matter or inline `key:: value` format  
**Pages:** Flat namespace, file-based  
**References:** `[[Page Name]]` and `#tag`

**UI Characteristics:**

- Simple property display
- Minimal visual indicators
- File-centric organization
- Properties as plain text

### Database-Based Graphs (New)

**Storage:** SQLite database with transit exports  
**Properties:** Typed, schema-validated database columns  
**Pages:** Hierarchical with classes/tags system  
**References:** Database relationships via `:db/id` and `:block/tags`

**UI Characteristics:**

- Structured property editor
- Type-specific UI controls
- Class/tag visual indicators
- Schema-driven validation

---

## UI Component Differences

### 1. Property System

#### File-Based Properties

```css
/* Simple key-value display */
.block-properties {
  margin: 4px 0;
  padding: 4px 8px;
  background-color: var(--ls-block-properties-background-color);
}

.page-properties {
  background-color: var(--ls-page-properties-background-color);
}
```

#### DB-Based Properties

```css
/* Enhanced structured property system */
.ls-properties-area {
  margin-top: 2px;
}

.ls-properties-area .property-pair {
  display: flex;
  flex-direction: row;
  flex-wrap: wrap;
  gap: 0.25rem;
}

.property-key {
  font-size: 15px;
  min-width: 160px;
  min-height: 28px;
}

.property-key-inner {
  display: flex;
  flex-direction: row;
  align-items: center;
  gap: 0.25rem;
  position: relative;
}

.property-key-inner .property-icon svg {
  width: 15px;
  height: 15px;
}

.property-value-container {
  flex: 1 1 0%;
  flex-shrink: 0;
  min-width: 100px;
  min-height: 28px;
}

.property-value-inner {
  display: flex;
  align-items: center;
}

.property-value-inner[data-type] {
  /* Type-specific styling */
  display: flex;
  align-items: center;
}

.property-value-inner[data-type='checkbox'] {
  /* Checkbox property */
}

.property-value-inner[data-type='date'] {
  /* Date property */
  white-space: nowrap;
}

.property-value-inner[data-type='datetime'] {
  /* DateTime property */
  white-space: nowrap;
}
```

**New Property UI Elements:**

- `.ls-properties-area` - Container for all properties
- `.property-pair` - Individual property key-value pair
- `.property-key` - Property name/label
- `.property-key-inner` - Property name with icon
- `.property-icon` - Icon for property type
- `.property-value-container` - Value wrapper
- `.property-value-inner` - Actual value display
- `.property-value-inner[data-type]` - Type-specific value rendering

**Key Differences:**

| Aspect        | File-Based   | DB-Based                             |
| ------------- | ------------ | ------------------------------------ |
| Layout        | Simple list  | Flex grid with icons                 |
| Min Height    | Variable     | 28px consistent                      |
| Icons         | None         | Type-specific icons                  |
| Value Types   | Plain text   | Typed (checkbox, date, select, etc.) |
| Interactivity | Text editing | Rich controls (dropdowns, pickers)   |

### 2. Page Icons & Type Indicators

#### File-Based

```css
/* Basic page icon */
.page-icon {
  margin-left: 0.75rem;
  display: flex;
  justify-content: center;
}
```

#### DB-Based

```css
/* Enhanced type icons for classes/tags */
.type-icon {
  text-align: center;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 0.25rem;
  border-width: 1px;
  margin-right: 0.5rem;
  position: relative;
  width: 24px;
  height: 24px;
  flex-shrink: 0;
  border-color: var(--ls-primary-background-color);
  overflow: hidden;
  color: var(--ls-primary-text-color);
}

.type-icon .ti,
.type-icon .tie {
  z-index: 1;
}

.type-icon:before {
  display: block;
  position: absolute;
  inset: 0;
  background: var(--ls-primary-background-color);
  content: ' ';
  opacity: 0.5;
}

/* Page icon in containers */
.page-icon {
  margin-left: 0.75rem;
  display: flex;
  justify-content: center;
}
```

**New Visual Indicators:**

- `.type-icon` - Indicates page/class/object type
- Icon backgrounds with opacity
- Bordered, rounded design
- Tabler icons (`.ti`) and extended icons (`.tie`)

### 3. Tags & Classes

#### File-Based Tags

```css
/* Simple text tags */
a.tag {
  font-size: 0.9em;
  text-align: center;
  text-decoration: none;
  display: inline-block;
  cursor: pointer;
  color: var(--ls-tag-text-color, #045591);
  opacity: var(--ls-tag-text-opacity, 0.8);
}

a.tag:hover {
  opacity: var(--ls-tag-text-hover-opacity, 1);
  color: var(--ls-tag-text-hover-color, #045591);
}

.block-tag a.tag {
  /* Tag in block context */
}
```

#### DB-Based Classes/Tags

```css
/* Structured class system */
.block-tag a.tag {
  /* Enhanced tag display */
}

.block-tag a.hash-symbol {
  /* Hash prefix for inline tags */
}

/* Class-specific UI */
.class-children {
  /* Class hierarchy display */
}

/* Object type badges */
.type-icon {
  /* Visual type indicator */
}
```

**New Class/Tag Features:**

- **Class hierarchy** - Visual tree of class relationships
- **Type badges** - Icons indicating class type
- **Hash symbols** - Distinct styling for `#tag` vs `[[Tag]]`
- **Object indicators** - Visual cues for pages with classes

### 4. Page References

#### File-Based

```css
.page-reference {
  padding: 2px 5px;
  display: block;
}

.page-reference:hover {
  color: var(--ls-link-text-hover-color);
}

.page-ref {
  color: var(--ls-link-ref-text-color);
}

.page-ref:hover {
  color: var(--ls-link-ref-text-hover-color);
}
```

#### DB-Based

```css
/* Enhanced page references with type info */
.page-reference {
  padding: 2px 5px;
  display: block;
}

/* Page references may include type icons */
.page-reference .type-icon {
  display: inline-flex;
  margin-right: 0.25rem;
}

/* Data attributes for page metadata */
[data-page-tags] {
  /* Pages with tags have this attribute */
}

/* Reference with class information */
.page-reference[data-ref] {
  /* Enhanced metadata */
}
```

---

## CSS Selector Changes

### Deprecated Selectors (File-Based Only)

These selectors may not work or have reduced functionality in DB graphs:

```css
/* File-based properties - limited in DB graphs */
.block-properties {
  /* Still works but may not capture all DB properties */
}

/* Simple property display */
.block-property-value {
  /* DB graphs use .property-value-inner instead */
}
```

### New Selectors (DB-Based)

Essential selectors for DB graph styling:

```css
/* Property system */
.ls-properties-area           /* Property container */
/* Property container */
/* Property container */
/* Property container */
.property-pair                /* Key-value pair */
.property-key                 /* Property name */
.property-key-inner           /* Property name with icon */
.property-icon                /* Property type icon */
.property-value-container     /* Value wrapper */
.property-value-inner         /* Value display */
.property-value-inner[data-type="checkbox"]  /* Specific types */
.property-value-inner[data-type="date"]
.property-value-inner[data-type="datetime"]

/* Type indicators */
.type-icon                    /* Page/class type badge */

/* Class system */
.class-children               /* Class hierarchy UI */

/* Enhanced page metadata */
[data-page-tags]              /* Pages with tags */

/* Property configuration */
.property-configure           /* Property settings UI */
.property-configure-popup-content
.ls-property-dropdown         /* Property dropdown menus */

/* Property editing */
.ls-property-add              /* Add property button */
.ls-property-name-edit-pane   /* Property name editor */
.ls-property-choices-sub-pane; /* Select options editor */
```

### Universal Selectors (Both Modes)

These work in both file-based and DB-based graphs:

```css
/* Core blocks */
.ls-block                     /* Block container */
/* Block container */
/* Block container */
/* Block container */
.block-content                /* Block content */
.block-children               /* Nested blocks */

/* Pages */
.page                         /* Page container */
.page-title                   /* Page title */
.ls-page-title                /* Page title area */

/* References */
.page-ref                     /* Page reference */
.block-ref                    /* Block reference */

/* UI components */
.ui__button                   /* Button */
.ui__icon                     /* Icon */
.ui__dropdown-menu-content    /* Dropdown */

/* Layout */
.cp__header                   /* Header */
.cp__sidebar-left-layout      /* Left sidebar */
.cp__right-sidebar; /* Right sidebar */
```

---

## Property System Changes

### Property Rendering Comparison

#### File-Based Property Display

```html
<div class="block-properties page-properties">
  <span class="property-key">tags:</span>
  <span class="property-value">[[Project]], [[Important]]</span>
</div>
```

#### DB-Based Property Display

```html
<div class="ls-properties-area">
  <div class="property-pair">
    <div class="property-key">
      <div class="property-key-inner">
        <div class="property-icon">
          <svg class="ti ti-tag">...</svg>
        </div>
        <span>tags</span>
      </div>
    </div>
    <div class="ls-block property-value-container">
      <div class="property-value">
        <div class="property-value-inner" data-type="object">
          <div class="select-item">Project</div>
          <div class="select-item">Important</div>
        </div>
      </div>
    </div>
  </div>
</div>
```

### Property Types in DB Graphs

DB graphs support typed properties with specific UI controls:

| Type         | data-type  | UI Control      | Example           |
| ------------ | ---------- | --------------- | ----------------- |
| Text         | `text`     | Input field     | Name, Description |
| Number       | `number`   | Number input    | Age, Count        |
| Date         | `date`     | Date picker     | Created date      |
| DateTime     | `datetime` | DateTime picker | Last modified     |
| Checkbox     | `checkbox` | Toggle          | Is active         |
| Select       | `object`   | Dropdown        | Status, Priority  |
| Multi-select | `object`   | Multi-select    | Tags, Categories  |
| URL          | `url`      | Link input      | Website           |

### Styling Property Types

```css
/* Text property */
.property-value-inner[data-type='text'] input {
  /* Style text inputs */
}

/* Checkbox property */
.property-value-inner[data-type='checkbox'] {
  /* Checkbox styling */
}

.property-value-inner[data-type='checkbox']
  .data-\[state\=checked\]\:bg-primary[data-state='checked'] {
  background-color: hsl(var(--primary));
}

/* Date property */
.property-value-inner[data-type='date'] {
  white-space: nowrap;
}

.property-value-inner[data-type='date'] .jtrigger-id {
  top: 4px; /* Adjust trigger position */
}

/* Select/Object property */
.property-value-inner[data-type='object'] .select-item {
  display: flex;
  align-items: center;
  flex-shrink: 0;
}

/* Priority values with colors */
.ls-icon-priorityLvlUrgent {
  color: var(--orange-rx-08);
}
```

---

## Migration Strategy

### Step 1: Audit Your Theme

Create a checklist of components your theme styles:

```markdown
## Component Audit

### Properties

- [ ] Block properties styling
- [ ] Page properties styling
- [ ] Property key styles
- [ ] Property value styles
- [ ] Property hover states

### Pages & References

- [ ] Page title styling
- [ ] Page icons
- [ ] Page references
- [ ] Block references
- [ ] Tag styling

### Layout

- [ ] Content padding
- [ ] Sidebar widths
- [ ] Block spacing
- [ ] Property margins

### Interactive Elements

- [ ] Buttons
- [ ] Dropdowns
- [ ] Input fields
- [ ] Toggle switches
```

### Step 2: Use Mode Detection

Implement mode-specific styles:

```css
/* 1. Base styles (apply to both) */
.ls-block {
  padding: 2px 0;
  min-height: 24px;
}

.page-title {
  font-size: 1.5rem;
  font-weight: 600;
}

/* 2. File-based specific adjustments */
html.is-file-graph {
  /* Reduce padding for file graphs */
  #main-content-container {
    padding-top: 0.5rem;
  }

  /* Simple property styling */
  .block-properties {
    background-color: var(--ls-block-properties-background-color);
    padding: 4px 8px;
  }
}

/* 3. DB-based specific enhancements */
html:not(.is-file-graph) {
  /* Enhanced spacing for DB graphs */
  #main-content-container {
    padding-top: 0.5rem;
  }

  .page-inner {
    gap: 0.5rem;
  }

  /* Structured property styling */
  .ls-properties-area {
    margin-top: 0.5rem;
  }

  .property-key {
    min-width: 160px;
    min-height: 28px;
  }

  /* Type icon styling */
  .type-icon {
    width: 24px;
    height: 24px;
    opacity: 1;
  }

  /* Enhanced property value display */
  .property-value-inner[data-type] {
    display: flex;
    align-items: center;
  }
}
```

### Step 3: Add DB-Specific Styles

Add new selectors for DB-only features:

```css
/* DB Graph Enhancements */

/* Property icons */
.property-key-inner .property-icon {
  display: flex;
  align-items: center;
  margin-right: 0.25rem;
}

.property-key-inner .property-icon svg {
  width: 15px;
  height: 15px;
  opacity: 0.7;
}

/* Type badges */
.type-icon {
  border: 1px solid var(--ls-border-color);
  border-radius: 0.25rem;
  background: var(--ls-tertiary-background-color);
}

.type-icon:before {
  opacity: 0.3;
}

/* Class hierarchy */
.class-children ul {
  margin-left: 1rem;
  padding-left: 0.5rem;
  border-left: 1px solid var(--ls-border-color);
}

/* Property dropdowns */
.ls-property-dropdown {
  background: var(--ls-secondary-background-color);
  border: 1px solid var(--ls-border-color);
  border-radius: 0.375rem;
}

/* Select items in properties */
.property-value-inner .select-item {
  padding: 0.125rem 0.5rem;
  border-radius: 0.25rem;
  background: var(--ls-tertiary-background-color);
  margin-right: 0.25rem;
}

/* Priority indicators */
.ls-icon-priorityLvlUrgent {
  color: var(--ls-error-text-color, #dc2626);
}

.ls-icon-priorityLvlHigh {
  color: var(--ls-warning-text-color, #ea580c);
}

.ls-icon-priorityLvlMedium {
  color: var(--ls-info-text-color, #0891b2);
}

.ls-icon-priorityLvlLow {
  color: var(--ls-secondary-text-color);
}
```

### Step 4: Test Both Modes

Create test graphs:

```markdown
## Test Checklist

### File-Based Graph Testing

1. Create a new file-based graph
2. Test all your theme's features
3. Check:
   - [ ] Block styling
   - [ ] Property display
   - [ ] Page titles
   - [ ] Tag styling
   - [ ] References
   - [ ] Sidebar layout

### DB-Based Graph Testing

1. Create a new DB-based graph
2. Add properties of different types:
   - [ ] Text property
   - [ ] Number property
   - [ ] Date property
   - [ ] Checkbox property
   - [ ] Select property (status, priority)
3. Create pages with classes
4. Test all UI components:
   - [ ] Property editor
   - [ ] Type icons
   - [ ] Class hierarchy
   - [ ] Property dropdowns
   - [ ] Select value chips

### Cross-Mode Testing

- [ ] Switch between file and DB graphs
- [ ] Check theme loads correctly
- [ ] Verify CSS variables work
- [ ] Test dark/light mode in both
```

### Step 5: Document Changes

Update your theme's README:

```markdown
## Compatibility

This theme supports both file-based and database-based graphs in Logseq v0.10.x+.

### File-Based Graphs

- ✅ Full support for traditional Logseq features
- ✅ Property styling
- ✅ Page and block references

### Database-Based Graphs

- ✅ Structured property system with type icons
- ✅ Class hierarchy visual indicators
- ✅ Enhanced property editor UI
- ✅ Type-specific property controls

### Known Differences

- DB graphs use structured property layout with icons
- Type badges appear on pages with classes
- Property dropdowns have enhanced styling
- Class hierarchy shows visual tree structure
```

---

## Testing Your Theme

### Create Test Content

#### File-Based Graph Test

Create a test page with:

```markdown
- Test Block
  tags:: [[Tag1]], [[Tag2]]
  priority:: High
  status:: In Progress
  - Child block
    due:: [[2025-01-15]]
```

#### DB-Based Graph Test

Create a page with:

- Properties of different types (text, number, date, checkbox, select)
- Multiple classes/tags assigned
- Class hierarchy (extend built-in classes)
- Property with icons

### Visual Regression Testing

1. **Screenshot Comparison**

   - Take screenshots in file-based mode
   - Take screenshots in DB-based mode
   - Compare layouts, spacing, colors

2. **Interactive Testing**

   - Test hover states
   - Test focus states
   - Test active states
   - Test dropdown interactions

3. **Responsive Testing**
   - Desktop view
   - Tablet view (if applicable)
   - Mobile view (if applicable)

### Browser DevTools Inspection

```javascript
// Check current graph mode
document.documentElement.classList.contains('is-file-graph')
// true = file-based, false = DB-based

// Find all property elements
document.querySelectorAll('.ls-properties-area .property-pair')

// Check property types
document.querySelectorAll('[data-type]')

// Find type icons
document.querySelectorAll('.type-icon')
```

---

## Common Pitfalls

### 1. **Assuming File-Based Selectors Work in DB Mode**

❌ **Wrong:**

```css
.block-properties .property-value {
  /* This doesn't target DB properties correctly */
}
```

✅ **Correct:**

```css
/* Works for both modes */
.block-properties .property-value,
.ls-properties-area .property-value-inner {
  /* Your styles */
}

/* Or use mode detection */
html.is-file-graph .block-properties .property-value {
  /* File-based specific */
}

html:not(.is-file-graph) .ls-properties-area .property-value-inner {
  /* DB-based specific */
}
```

### 2. **Hardcoded Dimensions**

❌ **Wrong:**

```css
.property-key {
  width: 120px; /* Too small for DB mode */
  height: 24px; /* May cut off icons */
}
```

✅ **Correct:**

```css
.property-key {
  min-width: 160px; /* Accommodate icons and text */
  min-height: 28px; /* Room for 24px icons + padding */
}
```

### 3. **Ignoring data-type Attributes**

❌ **Wrong:**

```css
.property-value-inner {
  /* Generic styling doesn't account for type-specific needs */
  display: block;
}
```

✅ **Correct:**

```css
.property-value-inner[data-type='checkbox'] {
  display: flex;
  align-items: center;
}

.property-value-inner[data-type='date'],
.property-value-inner[data-type='datetime'] {
  white-space: nowrap;
}

.property-value-inner[data-type='object'] .select-item {
  display: inline-flex;
  margin-right: 0.25rem;
}
```

### 4. **Not Styling Type Icons**

❌ **Wrong:**

```css
/* Type icons are invisible or clash with theme */
```

✅ **Correct:**

```css
.type-icon {
  border-color: var(--ls-border-color);
  background: var(--ls-tertiary-background-color);
  color: var(--ls-primary-text-color);
}

.type-icon:before {
  background: var(--ls-primary-background-color);
  opacity: 0.5;
}
```

### 5. **Missing Property Icon Styles**

❌ **Wrong:**

```css
/* Property icons don't match theme */
```

✅ **Correct:**

```css
.property-key-inner .property-icon svg {
  width: 15px;
  height: 15px;
  opacity: 0.7;
  color: var(--ls-secondary-text-color);
}

.property-key:hover .property-icon svg {
  opacity: 1;
}
```

### 6. **Inconsistent Spacing**

❌ **Wrong:**

```css
html.is-file-graph #main-content-container {
  padding-top: 2rem;
}

html:not(.is-file-graph) #main-content-container {
  padding-top: 0.5rem; /* Inconsistent */
}
```

✅ **Correct:**

```css
/* Consistent spacing across modes */
#main-content-container {
  padding-top: 0.5rem;
}

/* Mode-specific adjustments only when needed */
html.is-file-graph .page-inner {
  gap: 0.5rem;
}

html:not(.is-file-graph) .page-inner {
  gap: 0.5rem; /* Same spacing */
}
```

### 7. **Not Testing Dark Mode**

❌ **Wrong:**

```css
/* Only tested in light mode */
.property-value-inner {
  background: #ffffff;
  color: #000000;
}
```

✅ **Correct:**

```css
/* Use CSS variables that adapt to theme */
.property-value-inner {
  background: var(--ls-secondary-background-color);
  color: var(--ls-primary-text-color);
}

/* Or explicit dark mode styling */
html[data-theme='dark'] .property-value-inner {
  background: var(--ls-secondary-background-color);
}
```

---

## Resources

### Official Documentation

- [Theme Development Guide](./guide.md)
- [Quick Reference](./quick-reference.md)
- [Database Schema Guide](../../03-features/database-graphs/schema-guide.md)

### Code References

**Mode Detection:**

- The `is-file-graph` CSS class and `set-file-graph-flag!` function have been removed from the codebase.

**Property System:**

- [src/main/frontend/components/property.css](../../src/main/frontend/components/property.css)
- [src/main/frontend/components/property.cljs](../../src/main/frontend/components/property.cljs)

**Class System:**

- [src/main/frontend/components/class.cljs](../../src/main/frontend/components/class.cljs)
- [deps/db/src/logseq/db/frontend/class.cljs](../../deps/db/src/logseq/db/frontend/class.cljs)

**Theme CSS:**

- [src/main/frontend/components/theme.css](../../src/main/frontend/components/theme.css)
- [src/main/frontend/ui.css](../../src/main/frontend/ui.css)

**Built-in Classes:**

```clojure
;; From deps/db/src/logseq/db/frontend/class.cljs
:logseq.class/Root
:logseq.class/Tag
:logseq.class/Property
:logseq.class/Page
:logseq.class/Journal
:logseq.class/Whiteboard
:logseq.class/Task
:logseq.class/Query
:logseq.class/Card
:logseq.class/Cards
:logseq.class/Asset
:logseq.class/Code-block
:logseq.class/Quote-block
:logseq.class/Math-block
:logseq.class/Pdf-annotation
:logseq.class/Template
```

### CSS Variable Reference

**Color Variables (Both Modes):**

```css
--ls-primary-background-color
--ls-secondary-background-color
--ls-tertiary-background-color
--ls-quaternary-background-color

--ls-primary-text-color
--ls-secondary-text-color
--ls-link-text-color
--ls-link-text-hover-color

--ls-border-color
--ls-secondary-border-color

--ls-tag-text-color
--ls-tag-text-hover-color
--ls-tag-text-opacity
--ls-tag-text-hover-opacity
```

**Property Variables (DB-specific):**

```css
--ls-block-properties-background-color
--ls-page-properties-background-color
```

### Example Themes

Look at official themes that support both modes:

- Default theme (built-in)
- Dev theme (built-in)

### Community

- [Logseq Discord](https://discord.gg/logseq) - #theme-development channel
- [Logseq Forum](https://discuss.logseq.com/) - Theme category
- [GitHub Discussions](https://github.com/logseq/logseq/discussions) - Q&A

---

## Summary Checklist

When migrating your theme, ensure:

- [ ] **Mode detection** - Use `html.is-file-graph` selector
- [ ] **Property system** - Style `.ls-properties-area` and child elements
- [ ] **Type icons** - Style `.type-icon` for class/tag badges
- [ ] **Property types** - Handle `[data-type]` attributes
- [ ] **Property icons** - Style `.property-icon` elements
- [ ] **Class hierarchy** - Style `.class-children` if needed
- [ ] **Consistent spacing** - Match padding/margins across modes
- [ ] **Dark mode** - Test both themes in both modes
- [ ] **CSS variables** - Use theme variables, not hardcoded colors
- [ ] **Testing** - Create test content in both graph types
- [ ] **Documentation** - Update README with compatibility info

---

## Version History

| Version | Date       | Changes               |
| ------- | ---------- | --------------------- |
| 1.0     | 2025-12-25 | Initial guide created |
| 1.1     | 2026-03-10 | Note removal of `is-file-graph` CSS class from codebase |

---

## License

This documentation is part of the Logseq project and follows the same license.

---

**Need Help?**

If you encounter issues migrating your theme:

1. Check the [Theme Development Guide](./guide.md)
2. Review the [CSS selectors](#css-selector-changes) section
3. Test with the [Example implementations](#step-2-use-mode-detection)
4. Ask in [Discord #theme-development](https://discord.gg/logseq)

Happy theming! 🎨
