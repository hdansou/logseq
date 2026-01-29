# Tabler Icons Extension Catalog

## Overview

**Confirmed:** Logseq uses custom Tabler icon extensions with the `tie-` prefix (Tabler Icons Extension).

These custom icons extend the standard Tabler Icons library with Logseq-specific icons that are not available in the main Tabler Icons set. The icons are loaded via a custom web font.

## Font Configuration

**File:** [resources/css/tabler-extension.css](../resources/css/tabler-extension.css)

**Font Source:**

```css
@font-face {
  font-family: 'tabler-icons-extension';
  src: url('../fonts/tabler-icons-extension.woff2?mxgthk') format('woff2');
  font-style: normal;
  font-weight: 400;
}
```

**Font File:** `resources/fonts/tabler-icons-extension.woff2`

## Complete Icon List

Below is the complete catalog of all 31 custom Tabler icon extensions currently in use:

| Icon Name                | CSS Class                     | Unicode | Description/Use Case          |
| ------------------------ | ----------------------------- | ------- | ----------------------------- |
| `add-link`               | `.tie-add-link`               | `\ea01` | Adding links to content       |
| `app-feature`            | `.tie-app-feature`            | `\ea02` | Application features          |
| `block`                  | `.tie-block`                  | `\ea03` | Block/content blocks          |
| `block-search`           | `.tie-block-search`           | `\ea04` | Searching within blocks       |
| `cloud-exclamation`      | `.tie-cloud-exclamation`      | `\ea05` | Cloud sync alerts             |
| `connector`              | `.tie-connector`              | `\ea06` | Whiteboard connectors         |
| `group`                  | `.tie-group`                  | `\ea07` | Grouping elements             |
| `h-auto`                 | `.tie-h-auto`                 | `\ea08` | Auto height                   |
| `heading-off`            | `.tie-heading-off`            | `\ea09` | Heading disabled state        |
| `internal-link`          | `.tie-internal-link`          | `\ea0a` | Internal/wiki links           |
| `link-to-block`          | `.tie-link-to-block`          | `\ea0b` | Link to block reference       |
| `link-to-page`           | `.tie-link-to-page`           | `\ea0c` | Link to page                  |
| `link-to-whiteboard`     | `.tie-link-to-whiteboard`     | `\ea0d` | Link to whiteboard            |
| `move-to-sidebar-right`  | `.tie-move-to-sidebar-right`  | `\ea0e` | Move content to right sidebar |
| `new-block`              | `.tie-new-block`              | `\ea0f` | Create new block              |
| `new-page`               | `.tie-new-page`               | `\ea10` | Create new page               |
| `new-whiteboard`         | `.tie-new-whiteboard`         | `\ea11` | Create new whiteboard         |
| `new-whiteboard-element` | `.tie-new-whiteboard-element` | `\ea12` | Add new whiteboard element    |
| `object-compact`         | `.tie-object-compact`         | `\ea13` | Compact object view           |
| `object-expanded`        | `.tie-object-expanded`        | `\ea14` | Expanded object view          |
| `open-as-page`           | `.tie-open-as-page`           | `\ea15` | Open content as page          |
| `page`                   | `.tie-page`                   | `\ea16` | Page/document                 |
| `page-search`            | `.tie-page-search`            | `\ea17` | Searching pages               |
| `references-hide`        | `.tie-references-hide`        | `\ea18` | Hide references section       |
| `references-show`        | `.tie-references-show`        | `\ea19` | Show references section       |
| `select-cursor`          | `.tie-select-cursor`          | `\ea1a` | Selection cursor tool         |
| `text`                   | `.tie-text`                   | `\ea1b` | Text element                  |
| `ungroup`                | `.tie-ungroup`                | `\ea1c` | Ungroup elements              |
| `whiteboard`             | `.tie-whiteboard`             | `\ea1d` | Whiteboard/canvas             |
| `whiteboard-element`     | `.tie-whiteboard-element`     | `\ea1e` | Whiteboard element            |
| `whiteboard-search`      | `.tie-whiteboard-search`      | `\ea1f` | Searching whiteboards         |

## Usage in Code

### TypeScript/React (TLDraw/Whiteboard)

The icons are used via the `TablerIcon` component which automatically determines the correct prefix:

**File:** [packages/tldraw/apps/tldraw-logseq/src/components/icons/TablerIcon.tsx](../packages/tldraw/apps/tldraw-logseq/src/components/icons/TablerIcon.tsx)

```tsx
const extendedIcons = [
  'add-link',
  'block-search',
  'block',
  'connector',
  'group',
  'internal-link',
  'link-to-block',
  'link-to-page',
  'link-to-whiteboard',
  'move-to-sidebar-right',
  'object-compact',
  'object-expanded',
  'open-as-page',
  'page-search',
  'page',
  'references-hide',
  'references-show',
  'select-cursor',
  'text',
  'ungroup',
  'whiteboard-element',
  'whiteboard',
]

export const TablerIcon = ({
  name,
  className,
  ...props
}: { name: string } & React.HTMLAttributes<HTMLElement>) => {
  const classNamePrefix = extendedIcons.includes(name) ? `tie tie-` : `ti ti-`
  return <i className={cx(classNamePrefix + name, className)} {...props} />
}
```

**Example Usage:**

```tsx
<TablerIcon name="link-to-block" />
<TablerIcon name="new-whiteboard" />
<TablerIcon name="page-search" />
```

### ClojureScript (Main App)

Icons are used via the `icon` component with the `:extension?` flag:

**File:** [deps/shui/src/logseq/shui/icon/v2.cljs](../deps/shui/src/logseq/shui/icon/v2.cljs)

```clojure
(ui/icon "page" {:extension? true})
```

### Direct CSS Usage

Icons can also be used directly via CSS classes:

```html
<i class="tie tie-block"></i> <i class="tie tie-whiteboard"></i>
```

## Icon Categories

### Block/Page Icons (7)

- `block`, `block-search`
- `page`, `page-search`
- `whiteboard`, `whiteboard-search`, `whiteboard-element`

### Link Icons (5)

- `add-link`, `internal-link`
- `link-to-block`, `link-to-page`, `link-to-whiteboard`

### Action Icons (8)

- `new-block`, `new-page`, `new-whiteboard`, `new-whiteboard-element`
- `open-as-page`
- `group`, `ungroup`
- `move-to-sidebar-right`

### UI State Icons (6)

- `object-compact`, `object-expanded`
- `references-hide`, `references-show`
- `heading-off`
- `select-cursor`, `text`

### Misc Icons (5)

- `app-feature`
- `cloud-exclamation`
- `connector`
- `h-auto`

## Update Process

According to the comments in [resources/css/tabler-extension.css](../resources/css/tabler-extension.css):

```css
/**
 * This file is managed manually by Webfont app.
 * Steps:
 *  - download the zip from Webfont app
 *  - get the tabler-icons-extension.woff2 and replace the one in fonts/tabler-icons-extension.woff2
 *  - update the tie-xxx css rules
 */
```

### Steps to Update:

1. Download the zip from Webfont app
2. Extract and replace `resources/fonts/tabler-icons-extension.woff2`
3. Update the CSS rules in `resources/css/tabler-extension.css`
4. Update the `extendedIcons` array in `packages/tldraw/apps/tldraw-logseq/src/components/icons/TablerIcon.tsx`

## Files to Update When Adding/Removing Icons

1. **Font file:** `resources/fonts/tabler-icons-extension.woff2`
2. **CSS definitions:** `resources/css/tabler-extension.css`
3. **TypeScript icon list:** `packages/tldraw/apps/tldraw-logseq/src/components/icons/TablerIcon.tsx`

## Standard Tabler Icons

In addition to the custom extensions, Logseq also uses the standard Tabler Icons library (with `ti-` prefix). These are loaded separately via:

- `resources/css/tabler-icons.min.css`
- Standard Tabler Icons from [@tabler/icons](https://tabler.io/icons)

The `TablerIcon` component automatically uses `ti ti-` prefix for standard icons not in the `extendedIcons` list.

## Planning Font Update

When planning an update to the latest Tabler Icons version:

1. **Custom icons (tie-):** Must be manually maintained and recreated in the Webfont app
2. **Standard icons (ti-):** Can be updated by updating the `tabler-icons.min.css` file
3. **Verify compatibility:** Check that all 31 custom icons are still needed
4. **Test thoroughly:** Especially in:
   - Whiteboard/TLDraw components
   - Block references
   - Quick search
   - Page icons
   - Sidebar navigation

## Current Status

- **Total custom icons:** 31
- **Font format:** WOFF2
- **Managed via:** Manual Webfont app
- **CSS file:** resources/css/tabler-extension.css (154 lines)
- **Font file:** resources/fonts/tabler-icons-extension.woff2

---

**Last Updated:** December 15, 2025
**Total Custom Icons:** 31
