# Logseq Plugin UI Integration Guide

**Investigation Date:** December 15, 2025  
**Version:** 1.0  
**Purpose:** Comprehensive guide to Logseq's CSS and DOM structure for plugins that need to integrate into the UI

---

## Executive Summary

### Key Findings

1. **Three Main UI Integration Methods:**

   - `provideUI()` - Inject custom HTML/CSS into specific DOM locations via slots or selectors
   - `registerUIItem()` - Add buttons to toolbar or pagebar with predefined slots
   - `provideStyle()` - Inject global CSS styles into the application

2. **Four Primary Integration Points:**

   - **Toolbar** (`data-type="toolbar"`) - Top horizontal bar with plugin buttons
   - **Pagebar** (`data-type="pagebar"`) - Page-specific action bar
   - **Block Slots** - Macro renderer and block-specific UI hooks
   - **Floating UI** - Draggable/resizable modal containers

3. **CSS Architecture:**

   - CSS custom properties (CSS variables) with `--ls-` prefix for theming
   - Plugin-specific classes: `.lsp-*` (Logseq Plugin)
   - Injected UI marked with `[data-injected-ui]` attribute
   - Separate styling for light/dark themes via `html[data-theme='dark']`

4. **Critical Risks:**
   - 🟡 **Medium Risk:** Direct DOM manipulation can conflict with React rendering
   - 🟡 **Medium Risk:** Plugin CSS can override core styles if not scoped properly
   - 🟢 **Low Risk:** Well-sandboxed with DOMPurify sanitization

### Quick Reference for Developers

```typescript
// Basic UI injection
logseq.provideUI({
  key: 'my-ui',
  slot: 'toolbar', // or CSS selector via 'path'
  template: '<button data-on-click="handleClick">Click</button>',
  style: { padding: '5px' },
})

// Toolbar/pagebar button
logseq.App.registerUIItem('toolbar', {
  key: 'my-button',
  template: '<a class="button">My Button</a>',
})

// Global CSS injection
logseq.provideStyle(`
  .my-plugin-class {
    color: var(--ls-primary-text-color);
  }
`)
```

---

## Technical Deep Dive

### 1. Plugin UI Integration Entry Points

#### 1.1 `provideUI()` - Primary UI Injection Method

**Location:** [libs/src/LSPlugin.user.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/LSPlugin.user.ts#L596-L600)

**Purpose:** Inject custom HTML templates into specific locations in the Logseq UI

**Implementation:**

```typescript
provideUI (ui: UIOptions) {
  this.caller.call('provider:ui', ui)
  return this
}
```

**Data Structures:**

```typescript
// UIOptions Type Definition
type UIOptions = UIBaseOptions | UIPathOptions | UISlotOptions

type UIBaseOptions = {
  key?: string // Unique identifier
  replace?: boolean // Replace existing UI with same key
  template: string | null // HTML template (sanitized with DOMPurify)
  style?: CSS.Properties // Inline styles
  attrs?: Record<string, string> // HTML attributes
  close?: 'outside' | string // Close trigger
  reset?: boolean // Reset slot content
}

type UISlotIdentity = {
  slot: string // Slot ID (e.g., 'slot__abc123')
}

type UIPathIdentity = {
  path: string // CSS selector (e.g., '#search', '.toolbar')
}
```

**HTML Template Processing:**

- **Location:** [libs/src/helpers.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/helpers.ts#L291-L380)
- Templates are sanitized using **DOMPurify** to prevent XSS attacks
- Allowed tags include `<iframe>` with restricted attributes
- Event handlers use `data-on-{event}` attributes, not inline `onclick`

```typescript
// Sanitization (helpers.ts:330-340)
ui.template = DOMPurify.sanitize(ui.template, {
  ADD_TAGS: ['iframe'],
  ALLOW_UNKNOWN_PROTOCOLS: true,
  ADD_ATTR: [
    'allow',
    'src',
    'allowfullscreen',
    'frameborder',
    'scrolling',
    'target',
  ],
})
```

**Event Handling:**

- Events are delegated through `data-on-{event}` attributes
- Supported events: `click`, `focus`, `blur`, `keyup`, `keydown`, `change`, `input`, `contextmenu`, etc.
- Event handlers are defined in `provideModel()` and called via IPC

```typescript
// Event delegation (helpers.ts:441-459)
;['click', 'focus', 'blur', 'keyup' /* ... */].forEach((type) => {
  el.addEventListener(type, (e) => {
    const trigger = target.closest(`[data-on-${type}]`)
    if (trigger) {
      const msgType = trigger.dataset[`on${ucFirst(type)}`]
      pl.caller?.callUserModel(msgType, transformableEvent(trigger, e))
    }
  })
})
```

---

#### 1.2 `registerUIItem()` - Toolbar/Pagebar Integration

**Location:** [libs/src/LSPlugin.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/LSPlugin.ts#L1316-L1320)

**Purpose:** Add custom buttons/widgets to predefined UI areas (toolbar or pagebar)

**API Signature:**

```typescript
registerUIItem: (
  type: 'toolbar' | 'pagebar',
  opts: { key: string; template: string }
) => void
```

**DOM Structure Generated:**

```html
<!-- Toolbar item -->
<div class="ui-items-container" data-type="toolbar">
  <div class="list-wrap">
    <div
      id="injected-ui-item-toolbar-{key}-{pluginId}"
      class="injected-ui-item-toolbar"
      title="{key}"
      data-injected-ui="{pluginId}--{key}"
    >
      <!-- Your template here -->
    </div>
  </div>
</div>

<!-- Pagebar item -->
<div class="ui-items-container" data-type="pagebar">
  <div class="injected-ui-item-pagebar" data-injected-ui="{pluginId}--{key}">
    <!-- Your template here -->
  </div>
</div>
```

**Implementation Details:**

- **Renderer Location:** [src/main/frontend/components/plugins.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.cljs#L1169-L1188)
- Items are rendered dynamically when plugins are enabled
- Toolbar items can be pinned by users (stored in `:plugin/preferences`)

```clojure
;; UI Item Renderer (plugins.cljs:1169-1188)
(rum/defc ui-item-renderer
  [pid type {:keys [key template prefix]}]
  (let [*el (rum/use-ref nil)
        uni #(str prefix "injected-ui-item-" %)
        ^js pl (js/LSPluginCore.registeredPlugins.get (name pid))]

    (hooks/use-effect!
     (fn []
       (when-let [^js el (rum/deref *el)]
         (js/LSPlugin.pluginHelpers.setupInjectedUI.call
          pl #js {:slot (.-id el) :key key :template template} #js {})))
     [template])

    [:div {:id (uni (str (name key) "-" (name pid)))
           :title key
           :class (uni (name type))
           :ref *el}]))
```

---

#### 1.3 `provideStyle()` - Global CSS Injection

**Location:** [libs/src/LSPlugin.core.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/LSPlugin.core.ts#L293-L309)

**Purpose:** Inject custom CSS styles that apply globally to Logseq

**Implementation:**

```typescript
// Provider handler (LSPlugin.core.ts:293-309)
pluginLocal.on(_('style'), (style: StyleString | StyleOptions) => {
  let key: string | undefined

  if (typeof style !== 'string') {
    key = style.key
    style = style.style
  }

  if (!style || !style.trim()) return

  pluginLocal._dispose(
    setupInjectedStyle(style, {
      'data-injected-style': key ? `${key}-${pluginLocal.id}` : '',
      'data-ref': pluginLocal.id,
    })
  )
})
```

**CSS Injection Mechanism:**

- Creates a `<style>` element in document head
- Tagged with `data-ref="{pluginId}"` for cleanup
- Removed automatically when plugin is disabled/unloaded

---

### 2. Available UI Slots and Hooks

#### 2.1 Predefined Slot Locations

**Toolbar Slot**

- **Location:** Top of the application, right side
- **Component:** [src/main/frontend/components/header.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/header.cljs#L451)
- **Hook:** `hook-ui-items :toolbar`
- **CSS Container:** `.ui-items-container[data-type="toolbar"]`

**Pagebar Slot**

- **Location:** Below page title, right side
- **Component:** [src/main/frontend/components/page.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/page.cljs#L556)
- **Hook:** `hook-ui-items :pagebar`
- **CSS Container:** `.ui-items-container[data-type="pagebar"]`

**Page Head Actions Slot**

- **Hook:** `onPageHeadActionsSlotted`
- **Component:** [src/main/frontend/components/page.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/page.cljs#L555)
- **Purpose:** Add custom actions to page headers

```typescript
// Example usage
logseq.App.onPageHeadActionsSlotted(({ slot }) => {
  logseq.provideUI({
    key: 'page-action',
    slot,
    template: '<button data-on-click="handlePageAction">Action</button>',
  })
})
```

**Macro Renderer Slot**

- **Hook:** `onMacroRendererSlotted`
- **Component:** [src/main/frontend/components/block.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/block.cljs#L1955)
- **Purpose:** Render custom UI for `{{renderer ...}}` macros

```typescript
// Example: {{renderer :pomodoro, 25}}
logseq.App.onMacroRendererSlotted(({ slot, payload }) => {
  const [type, duration] = payload.arguments
  if (type !== ':pomodoro') return

  logseq.provideUI({
    key: 'pomodoro',
    slot,
    template: `<div class="pomodoro">Timer: ${duration}min</div>`,
  })
})
```

**Block Renderer Slot**

- **Hook:** `onBlockRendererSlotted(uuid, callback)`
- **Purpose:** Inject UI into specific blocks by UUID
- **Implementation:** [libs/src/LSPlugin.user.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/LSPlugin.user.ts#L221-L233)

---

#### 2.2 Slot Lifecycle

**Hook Registration Flow:**

```
User Code: logseq.App.onPageHeadActionsSlotted(callback)
    ↓
Plugin User API (LSPlugin.user.ts)
    ↓
Hook Installed: 'hook:app:page-head-actions-slotted'
    ↓
Logseq Core Renders Component
    ↓
hook-ui-slot component created (plugins.cljs:1137-1163)
    ↓
Callback invoked with {type, slot, payload}
    ↓
Plugin calls provideUI({ slot, template })
    ↓
setupInjectedUI creates DOM element (helpers.ts:296)
    ↓
UI appears in slot
```

**Cleanup on Unmount:**

```clojure
;; Cleanup effect (plugins.cljs:1149-1156)
(hooks/use-effect!
 (fn []
   (let [el (rum/deref *el-ref)]
     #(when-let [uis (seq (.querySelectorAll el "[data-injected-ui]"))]
        (doseq [^js el uis]
          (when-let [id (.-injectedUi (.-dataset el))]
            (js/LSPluginCore._forceCleanInjectedUI id))))))
 [])
```

---

### 3. CSS Architecture and Class Structure

#### 3.1 CSS Custom Properties (Theme Variables)

Logseq uses CSS custom properties for theming. Plugins should use these instead of hardcoded colors.

**Core Theme Variables:**

```css
/* Primary colors - resources/css/shui.css */
--ls-primary-background-color: hsl(var(--background));
--ls-secondary-background-color: var(--rx-gray-01);
--ls-tertiary-background-color: var(--rx-gray-03);
--ls-quaternary-background-color: var(--rx-gray-05);

--ls-primary-text-color: /* Inherited from theme */
--ls-link-text-color: /* Link colors */
--ls-link-text-hover-color: /* Link hover */
--ls-border-color: var(--lx-gray-06);

/* Block specific */
--ls-block-bullet-color: var(--rx-gray-08);
--ls-block-bullet-active-color: var(--rx-gray-12);
--ls-block-left-color: var(--rx-logseq-11);

/* Z-index layers */
--ls-z-index-level-1: /* Base level */
--ls-z-index-level-5: /* Modals/popups */

/* Draggable UI */
--ls-draggable-handle-height: 30px;
```

**Usage in Plugin CSS:**

```css
/* ✅ Good - Uses theme variables */
.my-plugin-button {
  background-color: var(--ls-secondary-background-color);
  color: var(--ls-primary-text-color);
  border: 1px solid var(--ls-border-color);
}

/* ❌ Bad - Hardcoded colors */
.my-plugin-button {
  background-color: #f0f0f0;
  color: #333;
}
```

---

#### 3.2 Plugin-Specific CSS Classes

**Namespace Convention:**

- `.lsp-*` - Logseq Plugin System classes
- `.cp__plugins-*` - Plugin management UI
- `[data-injected-ui]` - Marker for plugin-injected elements

**Key Classes:**

| Class                       | Purpose                             | Location                                                                                                          |
| --------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `.lsp-hook-ui-slot`         | Container for slot-based UI         | [plugins.css:927](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css#L927) |
| `.lsp-ui-float-container`   | Draggable floating UI               | [plugins.css:776](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css#L776) |
| `.lsp-iframe-sandbox`       | Iframe plugin container             | [plugins.css:643](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css#L643) |
| `.lsp-shadow-sandbox`       | Shadow DOM plugin container         | [plugins.css:643](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css#L643) |
| `.injected-ui-item-toolbar` | Toolbar UI item wrapper             | [plugins.css:799](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css#L799) |
| `.injected-ui-item-pagebar` | Pagebar UI item wrapper             | [plugins.css:807](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css#L807) |
| `.ui-items-container`       | Container for toolbar/pagebar items | [plugins.css:790](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css#L790) |

---

#### 3.3 CSS for Toolbar Items

```css
/* Toolbar Container - plugins.css:790-803 */
.ui-items-container[data-type='toolbar'] {
  @apply flex items-center;

  > .list-wrap {
    @apply flex items-center flex-nowrap overflow-x-hidden 
           transition-[max-width];
  }

  > .injected-ui-item-toolbar {
    @apply hover:opacity-100 transition-opacity;
  }
}
```

**Plugin Button Styling:**

```css
/* Toolbar plugin buttons - plugins.css:846-854 */
div[data-injected-ui] > a.button {
  @apply h-auto px-1.5;
}

div[data-injected-ui] :is(.ti, .tie) {
  @apply relative bottom-[-2px];
}
```

---

#### 3.4 CSS for Pagebar Items

```css
/* Pagebar Container - plugins.css:805-819 */
.ui-items-container[data-type='pagebar'] {
  @apply flex items-start pl-2;

  > .injected-ui-item-pagebar {
    @apply pr-3 opacity-30 hover:opacity-100 transition-opacity;
  }

  > .list-wrap {
    @apply flex items-center flex-nowrap overflow-x-hidden pt-[14px];
  }

  a.button {
    @apply flex items-center;
    color: var(--ls-primary-text-color);
  }
}
```

---

#### 3.5 CSS for Block Slots

```css
/* Block content slots - plugins.css:927-945 */
.block-content {
  .lsp-hook-ui-slot {
    display: inline-flex;

    [data-injected-ui] {
      display: inline-flex;
    }
  }

  .block-slotted-body {
    .lsp-hook-ui-slot {
      display: block;

      [data-injected-ui] {
        display: flex;
        flex-direction: column;
      }
    }
  }
}
```

---

#### 3.6 Dark Theme Overrides

```css
/* Dark theme adjustments - plugins.css:913-924 */
html[data-theme='dark'] {
  .lsp-frame-readme {
    background-color: #eeeeee;
  }

  .cp__plugins-page {
    .control-tabs.scrolled {
      box-shadow: 0 4px 6px -6px #666;
    }
  }
}
```

**Plugin Dark Mode Example:**

```css
/* Plugin CSS with theme support */
.my-plugin-card {
  background-color: var(--ls-secondary-background-color);
  color: var(--ls-primary-text-color);
}

/* Optional dark-mode specific overrides */
html[data-theme='dark'] .my-plugin-card {
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.4);
}
```

---

### 4. DOM Structure and Injection Points

#### 4.1 Toolbar DOM Structure

```html
<div id="head" class="cp__header">
  <div class="r">
    <!-- Other toolbar items -->

    <!-- Plugin toolbar items injected here -->
    <div class="ui-items-container" data-type="toolbar">
      <div class="list-wrap">
        <!-- Each plugin item -->
        <div
          id="injected-ui-item-toolbar-my-btn-my-plugin"
          class="injected-ui-item-toolbar"
          title="my-btn"
          data-injected-ui="my-plugin--my-btn"
        >
          <a class="button">My Button</a>
        </div>
      </div>

      <!-- Toolbar manager dropdown -->
      <div class="toolbar-plugins-manager">
        <!-- Plugin management UI -->
      </div>
    </div>
  </div>
</div>
```

**Injection Point:** [components/header.cljs:451](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/header.cljs#L451)

---

#### 4.2 Pagebar DOM Structure

```html
<div class="page">
  <div class="page-title">
    <!-- Page title content -->
  </div>

  <!-- Plugin pagebar items injected here -->
  <div class="ui-items-container" data-type="pagebar">
    <div class="list-wrap">
      <div
        class="injected-ui-item-pagebar"
        data-injected-ui="my-plugin--page-action"
      >
        <a class="button">Page Action</a>
      </div>
    </div>
  </div>
</div>
```

**Injection Point:** [components/page.cljs:556](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/page.cljs#L556)

---

#### 4.3 Floating UI DOM Structure

```html
<body>
  <!-- Main application content -->

  <!-- Floating plugin UI appended to body -->
  <div
    id="my-plugin--my-ui"
    class="lsp-ui-float-container visible"
    data-injected-ui="my-plugin--my-ui"
    draggable="true"
    resizable="true"
    style="position: fixed; top: 40%; left: 30%;"
  >
    <!-- Draggable handle -->
    <div class="draggable-handle">
      <div class="th">
        <div class="l">
          <h3>My Plugin UI</h3>
        </div>
        <div class="r">
          <a class="button close">×</a>
        </div>
      </div>
    </div>

    <!-- Content -->
    <div class="ls-ui-float-content">
      <!-- Your template content here -->
    </div>
  </div>
</body>
```

**CSS Styling:** [plugins.css:776-788](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css#L776)

---

#### 4.4 Slot-Based UI Structure

```html
<!-- Slot container created by hook-ui-slot -->
<div
  class="lsp-hook-ui-slot"
  id="slot__abc12345"
  data-type="page-head-actions-slotted"
>
  <!-- Plugin UI injected here -->
  <div
    id="my-plugin--custom-action"
    data-injected-ui="my-plugin--custom-action"
  >
    <button data-on-click="handleAction">Custom Action</button>
  </div>
</div>
```

**Component:** [plugins.cljs:1137-1163](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.cljs#L1137)

---

### 5. Event Handling and User Interactions

#### 5.1 Event Model Architecture

**Two-Step Event Handling:**

1. **Define handlers in `provideModel()`:**

```typescript
logseq.provideModel({
  handleClick(e) {
    console.log('Button clicked!', e)
  },
  handleInput(e) {
    console.log('Input changed:', e.value)
  },
})
```

2. **Reference in template via `data-on-{event}`:**

```typescript
logseq.provideUI({
  key: 'my-ui',
  slot: 'toolbar',
  template: `
    <button data-on-click="handleClick">Click Me</button>
    <input data-on-input="handleInput" />
  `,
})
```

---

#### 5.2 Supported Events

**Full Event List:** [helpers.ts:442-445](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/helpers.ts#L442)

- `click`
- `dblclick`
- `focus` / `focusin` / `focusout` / `blur`
- `keyup` / `keypress` / `keydown`
- `change`
- `input`
- `contextmenu`

---

#### 5.3 Event Object Transformation

Events are transformed to plain objects before being sent to plugins:

```typescript
// Event transformation (helpers.ts:497-526)
export function transformableEvent(target: HTMLElement, e: Event) {
  const obj: any = {}

  if (target) {
    obj.type = e.type
    const ds = target.dataset[
      // Extract useful properties
      ('value', 'id', 'className', 'dataset', 'rect')
    ].forEach((k) => {
      let v: any

      switch (k) {
        case 'rect':
          if (ds.hasOwnProperty('rect')) {
            v = target.getBoundingClientRect().toJSON()
          }
          break
        default:
          v = target[k]
      }

      if (typeof v === 'object') {
        v = { ...v }
      }

      obj[k] = v
    })
  }

  return obj
}
```

**Event Object Structure:**

```typescript
{
  type: 'click',
  value: 'input value',
  id: 'element-id',
  className: 'class1 class2',
  dataset: { customAttr: 'value' },
  rect: { x, y, width, height, top, left, right, bottom }
}
```

---

#### 5.4 Preventing Default Behavior

```html
<!-- Add data-prevent-default="true" to prevent default -->
<a href="#" data-on-click="handleLink" data-prevent-default="true"> Link </a>
```

---

### 6. Floating UI (Draggable/Resizable)

#### 6.1 Creating Floating UI

```typescript
// Floating UI is created when no 'slot' or 'path' is specified
logseq.provideUI({
  key: 'floating-window',
  template: `
    <div class="p-4">
      <h2>Floating Window</h2>
      <p>This can be dragged and resized!</p>
    </div>
  `,
  style: {
    width: '400px',
    height: '300px',
  },
  attrs: {
    title: 'My Floating Window',
  },
  close: 'outside', // Close when clicking outside
})
```

---

#### 6.2 Floating UI Features

**Draggable Handle:**

- Automatically added to top of floating UI
- Height controlled by `--ls-draggable-handle-height` (30px)
- Click and drag to move

**Resizable:**

- Enabled by default for floating UI
- User can resize from edges/corners
- Size persisted across sessions (via plugin state)

**Close Triggers:**

- `close: 'outside'` - Click outside to close
- `close: '{elementId}'` - Click specific element to close
- Close button in draggable handle

**CSS Structure:**

```css
/* Floating container - plugins.css:776-788 */
.lsp-ui-float-container {
  position: fixed;
  top: 40%;
  left: 30%;
  border: 2px solid var(--ls-border-color);
  border-radius: 6px;
  overflow: hidden;

  &.visible {
    height: unset;
  }
}

/* Draggable handle - plugins.css:723-762 */
.lsp-ui-float-container .th {
  display: flex;
  align-items: center;
  height: var(--ls-draggable-handle-height);
  background-color: var(--ls-secondary-background-color);
  color: var(--ls-primary-text-color);
  user-select: none;
}
```

---

### 7. Plugin Lifecycle and Cleanup

#### 7.1 UI Injection Lifecycle

```
Plugin Enabled
    ↓
provideUI() called
    ↓
setupInjectedUI() (helpers.ts:296)
    ↓
DOM element created and appended
    ↓
Event listeners attached
    ↓
Cleanup function stored in injectedUIEffects Map
    ↓
[UI Active State]
    ↓
Plugin Disabled OR provideUI(template: null)
    ↓
Cleanup function invoked
    ↓
Event listeners removed
    ↓
DOM element removed
```

---

#### 7.2 Cleanup Mechanisms

**Manual Cleanup (Remove UI):**

```typescript
// Remove UI by setting template to null
logseq.provideUI({
  key: 'my-ui',
  template: null, // Triggers cleanup
})
```

**Automatic Cleanup:**

1. **Plugin Disabled:** All injected UI removed
2. **Plugin Unloaded:** `cleanInjectedScripts()` called
3. **Slot Unmounted:** React useEffect cleanup in `hook-ui-slot`

**Cleanup Implementation:** [helpers.ts:495-502](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/helpers.ts#L495)

```typescript
export function cleanInjectedUI(id: string) {
  if (!injectedUIEffects.has(id)) return
  const clean = injectedUIEffects.get(id)
  try {
    clean()
  } catch (e) {
    console.warn('[CLEAN Injected UI] ', id, e)
  }
}
```

---

#### 7.3 State Persistence

**Floating UI Position:**

- Position and size stored in plugin's local state
- Restored on plugin reload
- Uses `data-dx`, `data-dy` attributes to track movement

**Toolbar Item Pinning:**

- User can pin/unpin toolbar items
- Stored in `:plugin/preferences` → `:pinnedToolbarItems`
- Persisted across sessions

---

## Flow Diagrams

### UI Injection Flow

```
Developer Code
    |
    | logseq.provideUI({ slot: 'toolbar', template: '<button>...' })
    |
    v
LSPluginUser.provideUI() (LSPlugin.user.ts)
    |
    | caller.call('provider:ui', ui)
    |
    v
Provider Handler (LSPlugin.core.ts:310-325)
    |
    | setupInjectedUI.call(pluginLocal, ui, attrs, callback)
    |
    v
setupInjectedUI() (helpers.ts:296-474)
    |
    | 1. Determine target (slot ID or CSS selector)
    | 2. Sanitize template with DOMPurify
    | 3. Create DOM element with ID: {pluginId}--{key}
    | 4. Set data-injected-ui attribute
    | 5. Apply styles and attributes
    | 6. Attach event listeners
    | 7. Append to target
    | 8. Store cleanup function
    |
    v
UI Visible in Logseq
```

---

### Event Handling Flow

```
User Clicks Button
    |
    v
Event Listener (helpers.ts:442-459)
    |
    | 1. Event bubbles to injected element
    | 2. Find closest [data-on-click] element
    | 3. Extract handler name from data-on-click
    |
    v
pl.caller.callUserModel(msgType, event)
    |
    | IPC call to plugin
    |
    v
Plugin's provideModel() handler
    |
    | handleClick(e) { ... }
    |
    v
Handler Executes
```

---

### Toolbar Item Rendering Flow

```
Logseq Header Component Renders
    |
    v
(plugins/hook-ui-items :toolbar) (header.cljs:451)
    |
    v
hook-ui-items Component (plugins.cljs:1284-1306)
    |
    | 1. Get all toolbar items from state
    | 2. Sort by key
    | 3. Filter by pinned status
    | 4. Map to ui-item-renderer
    |
    v
ui-item-renderer (plugins.cljs:1169-1188)
    |
    | 1. Create div with unique ID
    | 2. Get plugin instance from LSPluginCore
    | 3. Call setupInjectedUI with template
    |
    v
Toolbar Button Rendered
```

---

## Comparison Tables

### UI Integration Methods

| Method             | Use Case                       | Target                | Cleanup        | Complexity |
| ------------------ | ------------------------------ | --------------------- | -------------- | ---------- |
| `provideUI()`      | Custom HTML/CSS anywhere       | Slot or CSS selector  | Manual or auto | Medium     |
| `registerUIItem()` | Simple toolbar/pagebar buttons | Predefined areas only | Automatic      | Low        |
| `provideStyle()`   | Global CSS theming             | Entire app            | Automatic      | Low        |
| `provideTheme()`   | Complete theme                 | Entire app            | Manual         | High       |

---

### Slot Types Comparison

| Slot Type         | Trigger      | Lifecycle  | Multiple Instances | Example Use Case      |
| ----------------- | ------------ | ---------- | ------------------ | --------------------- |
| Toolbar           | App load     | Persistent | Yes (multi-plugin) | Global actions        |
| Pagebar           | Page load    | Per-page   | Yes                | Page-specific tools   |
| Page Head Actions | Page load    | Per-page   | Yes                | Page metadata         |
| Macro Renderer    | Block render | Per-block  | Yes                | Custom visualizations |
| Block Slot        | Block render | Per-block  | No (UUID-specific) | Block annotations     |

---

### CSS Variable Categories

| Category   | Variables                      | Purpose                | Theme-Aware |
| ---------- | ------------------------------ | ---------------------- | ----------- |
| Background | `--ls-*-background-color`      | Layered backgrounds    | ✅ Yes      |
| Text       | `--ls-*-text-color`            | Text and links         | ✅ Yes      |
| Border     | `--ls-border-color`            | Borders and separators | ✅ Yes      |
| Block      | `--ls-block-*`                 | Block-specific styling | ✅ Yes      |
| Layout     | `--ls-draggable-handle-height` | Fixed dimensions       | ❌ No       |
| Z-Index    | `--ls-z-index-level-*`         | Layering order         | ❌ No       |

---

## Scenario Analysis

### Scenario 1: Adding a Toolbar Button

**Goal:** Add a button to open a custom sidebar

**Implementation:**

```typescript
// 1. Define handler
logseq.provideModel({
  openMySidebar() {
    logseq.App.setRightSidebarVisible(true)
    // ... custom logic
  },
})

// 2. Register toolbar item
logseq.App.registerUIItem('toolbar', {
  key: 'my-sidebar-btn',
  template: `
    <a class="button" data-on-click="openMySidebar" title="Open My Sidebar">
      <i class="ti ti-layout-sidebar-right"></i>
    </a>
  `,
})
```

**Timeline:**

- `0ms`: Plugin loads
- `10ms`: `registerUIItem()` called
- `50ms`: Toolbar rendered with button
- User clicks → Handler executes immediately

**Risk Level:** 🟢 Low - Simple, well-supported pattern

---

### Scenario 2: Creating a Floating Dashboard

**Goal:** Display a draggable widget with live data

**Implementation:**

```typescript
logseq.provideModel({
  closeWidget() {
    logseq.provideUI({
      key: 'dashboard-widget',
      template: null, // Remove UI
    })
  },
  refreshData() {
    // Fetch and update data
    this.renderWidget()
  },
})

logseq.provideUI({
  key: 'dashboard-widget',
  template: `
    <div class="p-4">
      <h3>My Dashboard</h3>
      <button data-on-click="refreshData">Refresh</button>
      <div id="data-container"></div>
    </div>
  `,
  style: {
    width: '400px',
    height: '300px',
    backgroundColor: 'var(--ls-secondary-background-color)',
    color: 'var(--ls-primary-text-color)',
  },
  attrs: {
    title: 'My Dashboard',
  },
  close: 'outside',
})
```

**Timeline:**

- `0ms`: `provideUI()` called
- `10ms`: DOM element created
- `20ms`: Draggable/resizable handlers attached
- `30ms`: Widget visible and interactive

**Risk Level:** 🟡 Medium - Complex DOM manipulation, potential performance impact

---

### Scenario 3: Custom Macro Renderer

**Goal:** Render `{{renderer :chart, data.csv}}` as interactive chart

**Implementation:**

```typescript
logseq.App.onMacroRendererSlotted(({ slot, payload }) => {
  const [type, dataFile] = payload.arguments
  if (type !== ':chart') return

  // Load data and render chart
  fetch(dataFile).then((data) => {
    logseq.provideUI({
      key: `chart-${payload.uuid}`,
      slot,
      template: `
        <div class="custom-chart" data-uuid="${payload.uuid}">
          <canvas id="chart-${payload.uuid}"></canvas>
        </div>
      `,
      reset: true, // Clear previous content
    })

    // Initialize chart library
    // ...
  })
})
```

**Timeline:**

- Block renders → Macro detected → Callback fired
- `0ms`: Slot created
- `50ms`: Callback receives slot ID
- `100ms`: Data fetched
- `150ms`: Chart rendered

**Risk Level:** 🟡 Medium - Async operations, external dependencies

---

## Code Location Reference

### TypeScript/JavaScript Files

| File                                                                                                    | Purpose            | Key Functions/Classes                       |
| ------------------------------------------------------------------------------------------------------- | ------------------ | ------------------------------------------- |
| [libs/src/LSPlugin.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/LSPlugin.ts)           | Type definitions   | `UIOptions`, `IAppProxy`, `IEditorProxy`    |
| [libs/src/LSPlugin.user.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/LSPlugin.user.ts) | Plugin user API    | `LSPluginUser`, `provideUI`, `provideStyle` |
| [libs/src/LSPlugin.core.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/LSPlugin.core.ts) | Plugin core system | `LSPluginCore`, provider handlers           |
| [libs/src/helpers.ts](file:///Users/dzu/Projects/src/github.com/logseq/libs/src/helpers.ts#L253-L561)   | UI injection logic | `setupInjectedUI`, `cleanInjectedUI`        |

---

### ClojureScript Files

| File                                                                                                                                        | Purpose               | Key Components                                      |
| ------------------------------------------------------------------------------------------------------------------------------------------- | --------------------- | --------------------------------------------------- |
| [src/main/frontend/components/plugins.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.cljs)     | Plugin UI components  | `hook-ui-slot`, `hook-ui-items`, `ui-item-renderer` |
| [src/main/frontend/handler/plugin.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/handler/plugin.cljs)             | Plugin event handlers | `hook-plugin-app`, `hook-plugin-block-slot`         |
| [src/main/frontend/components/header.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/header.cljs#L451)  | Toolbar integration   | Toolbar `hook-ui-items`                             |
| [src/main/frontend/components/page.cljs](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/page.cljs#L555-L556) | Pagebar integration   | Pagebar `hook-ui-items`                             |

---

### CSS Files

| File                                                                                                                                  | Purpose          | Lines of Interest     |
| ------------------------------------------------------------------------------------------------------------------------------------- | ---------------- | --------------------- |
| [src/main/frontend/components/plugins.css](file:///Users/dzu/Projects/src/github.com/logseq/src/main/frontend/components/plugins.css) | Plugin UI styles | Full file (969 lines) |
| [resources/css/shui.css](file:///Users/dzu/Projects/src/github.com/logseq/resources/css/shui.css)                                     | Theme variables  | CSS custom properties |

---

## Recommendations

### For Users

**Best Practices:**

1. **Pin Important Toolbar Items:**

   - Right-click toolbar items → Pin to keep visible
   - Unpinned items appear in overflow menu

2. **Manage Plugin UI:**

   - Too many toolbar items? Use the toolbar manager dropdown
   - Disable plugins you don't actively use

3. **Performance:**
   - Plugins with heavy UI can slow down rendering
   - Disable complex plugins when not needed

---

### For Plugin Developers

#### Immediate Best Practices

1. **Always Use Theme Variables:**

```typescript
// ✅ Good
logseq.provideStyle(`
  .my-plugin {
    background: var(--ls-secondary-background-color);
    color: var(--ls-primary-text-color);
    border: 1px solid var(--ls-border-color);
  }
`)

// ❌ Bad
logseq.provideStyle(`
  .my-plugin {
    background: #f0f0f0;
    color: #333;
  }
`)
```

2. **Scope Your CSS:**

```typescript
// ✅ Good - Scoped with plugin-specific class
logseq.provideStyle(`
  .my-plugin-container .button {
    /* styles */
  }
`)

// ❌ Bad - Can override core styles
logseq.provideStyle(`
  .button {
    /* styles */
  }
`)
```

3. **Clean Up Resources:**

```typescript
// Store cleanup functions
let uiCleanup: (() => void) | null = null

logseq.beforeunload(async () => {
  // Clean up before plugin unloads
  if (uiCleanup) uiCleanup()
})
```

4. **Use Semantic HTML:**

```typescript
// ✅ Good
template: `
  <button data-on-click="handleAction" 
          class="ui__button" 
          aria-label="Perform action">
    Action
  </button>
`

// ❌ Bad
template: `
  <div data-on-click="handleAction">Action</div>
`
```

5. **Handle Errors Gracefully:**

```typescript
logseq.provideModel({
  async handleAction() {
    try {
      // ... action logic
    } catch (error) {
      logseq.UI.showMsg(`Error: ${error.message}`, 'error')
    }
  },
})
```

---

#### Medium-Term Improvements

1. **Optimize Rendering:**

```typescript
// Debounce frequent updates
let updateTimeout: number | null = null

function updateUI(data: any) {
  if (updateTimeout) clearTimeout(updateTimeout)

  updateTimeout = setTimeout(() => {
    logseq.provideUI({
      key: 'my-ui',
      template: generateTemplate(data),
      replace: true, // Replace existing UI
    })
  }, 100)
}
```

2. **Responsive Design:**

```typescript
logseq.provideStyle(`
  .my-plugin-panel {
    width: 100%;
    max-width: 600px;
  }
  
  @media (max-width: 768px) {
    .my-plugin-panel {
      max-width: 100%;
    }
  }
`)
```

3. **Accessibility:**

```typescript
template: `
  <div role="dialog" aria-labelledby="dialog-title">
    <h2 id="dialog-title">Dialog Title</h2>
    <button data-on-click="close" aria-label="Close dialog">
      ×
    </button>
  </div>
`
```

---

#### Long-Term Architecture

1. **Component-Based Structure:**

```typescript
// Separate concerns
class MyPluginUI {
  private container: string

  constructor() {
    this.container = 'my-plugin-ui'
  }

  render(data: any) {
    return `
      ${this.renderHeader(data)}
      ${this.renderBody(data)}
      ${this.renderFooter(data)}
    `
  }

  private renderHeader(data: any) {
    /* ... */
  }
  private renderBody(data: any) {
    /* ... */
  }
  private renderFooter(data: any) {
    /* ... */
  }
}
```

2. **State Management:**

```typescript
// Use Logseq's storage API for persistence
class PluginState {
  async save(key: string, value: any) {
    await logseq.Assets.writeFile(`state/${key}.json`, JSON.stringify(value))
  }

  async load(key: string) {
    const content = await logseq.Assets.readFile(`state/${key}.json`)
    return JSON.parse(content)
  }
}
```

3. **Testing Strategy:**

```typescript
// Mock Logseq API for unit tests
const mockLogseq = {
  provideUI: jest.fn(),
  provideModel: jest.fn(),
  // ...
}

// Test UI generation
test('renders toolbar button', () => {
  const plugin = new MyPlugin(mockLogseq)
  plugin.initialize()

  expect(mockLogseq.provideUI).toHaveBeenCalledWith(
    expect.objectContaining({
      key: 'toolbar-btn',
    })
  )
})
```

---

### For Testing

**Unit Test Scenarios:**

1. **UI Injection:**

   - ✅ Verify `provideUI` called with correct parameters
   - ✅ Check template contains expected elements
   - ✅ Validate CSS variables used (not hardcoded colors)

2. **Event Handlers:**

   - ✅ Verify `provideModel` registers handlers
   - ✅ Test handler invocation with mock events
   - ✅ Check error handling in handlers

3. **Cleanup:**
   - ✅ Verify UI removed when `template: null`
   - ✅ Check cleanup on plugin disable
   - ✅ Ensure no memory leaks (event listeners removed)

**Integration Test Scenarios:**

1. **Toolbar Integration:**

   - ✅ Plugin button appears in toolbar
   - ✅ Button click triggers correct handler
   - ✅ Button can be pinned/unpinned

2. **Slot-Based UI:**

   - ✅ Macro renderer creates UI in block
   - ✅ Multiple macros create separate UI instances
   - ✅ UI removed when block deleted

3. **Floating UI:**
   - ✅ Window appears at correct position
   - ✅ Window can be dragged
   - ✅ Window can be resized
   - ✅ Position persists across reloads

**E2E Test Scenarios:**

1. **Full User Workflow:**

   - Install plugin
   - Enable plugin
   - Interact with plugin UI
   - Disable plugin
   - Verify cleanup

2. **Theme Switching:**
   - Switch between light/dark themes
   - Verify plugin UI adapts correctly
   - Check no style breaks

---

## Appendices

### A. Glossary

| Term                | Definition                                                             |
| ------------------- | ---------------------------------------------------------------------- |
| **Slot**            | A dynamically created container for plugin UI, identified by unique ID |
| **Path**            | A CSS selector targeting existing DOM elements for UI injection        |
| **Hook**            | A callback registration point for plugin lifecycle events              |
| **Provider**        | Plugin method to supply UI, styles, or themes to Logseq                |
| **Injected UI**     | DOM elements created by plugins, marked with `[data-injected-ui]`      |
| **LSPlugin**        | Logseq Plugin - prefix for plugin system classes                       |
| **DOMPurify**       | Security library that sanitizes HTML to prevent XSS attacks            |
| **Floating UI**     | Draggable/resizable modal windows created by plugins                   |
| **Theme Variables** | CSS custom properties (e.g., `--ls-*`) for consistent theming          |

---

### B. Common UI Patterns

#### Pattern 1: Toolbar Button with Dropdown

```typescript
logseq.provideModel({
  toggleDropdown(e) {
    const menu = document.getElementById('my-dropdown')
    menu.classList.toggle('visible')
  },
})

logseq.App.registerUIItem('toolbar', {
  key: 'dropdown-btn',
  template: `
    <div class="ui__dropdown-trigger">
      <button data-on-click="toggleDropdown">
        Options <span class="ti ti-chevron-down"></span>
      </button>
      <div id="my-dropdown" class="ui__dropdown-menu" style="display:none;">
        <a data-on-click="option1">Option 1</a>
        <a data-on-click="option2">Option 2</a>
      </div>
    </div>
  `,
})
```

#### Pattern 2: Page-Specific Action

```typescript
logseq.App.onPageHeadActionsSlotted(({ slot }) => {
  logseq.provideUI({
    key: 'export-page',
    slot,
    template: `
      <button class="ui__button" data-on-click="exportCurrentPage">
        <span class="ti ti-download"></span> Export
      </button>
    `,
  })
})

logseq.provideModel({
  async exportCurrentPage() {
    const page = await logseq.Editor.getCurrentPage()
    // Export logic...
  },
})
```

#### Pattern 3: Block Annotation

```typescript
logseq.App.onBlockRendererSlotted((uuid) => {
  return ({ slot }) => {
    logseq.provideUI({
      key: `annotation-${uuid}`,
      slot,
      template: `
        <div class="block-annotation">
          <button data-on-click="addNote" data-uuid="${uuid}">
            <span class="ti ti-note"></span> Add Note
          </button>
        </div>
      `,
    })
  }
})
```

---

### C. Security Considerations

**XSS Prevention:**

1. **DOMPurify Sanitization:** All HTML templates are sanitized before injection
2. **No Inline Scripts:** Inline `onclick` attributes are not allowed
3. **Event Delegation:** Events handled through `data-on-*` attributes

**Content Security Policy:**

- Plugins run in isolated contexts
- Iframe mode provides additional sandboxing
- Shadow DOM mode prevents CSS leakage

**Best Practices:**

```typescript
// ✅ Safe - Event via data attribute
template: '<button data-on-click="handler">Click</button>'

// ❌ Unsafe - Inline handler (will be stripped)
template: '<button onclick="alert()">Click</button>'

// ✅ Safe - User content escaped
const userInput = escapeHTML(input)
template: `<div>${userInput}</div>`

// ❌ Unsafe - Direct injection
template: `<div>${input}</div>`
```

---

### D. Performance Tips

**Optimize Template Rendering:**

```typescript
// ❌ Bad - Recreates entire UI on each update
function updateCounter(count: number) {
  logseq.provideUI({
    key: 'counter',
    template: `<div>Count: ${count}</div>`,
  })
}

// ✅ Better - Update only changed content
function updateCounter(count: number) {
  const el = document.getElementById('counter-value')
  if (el) el.textContent = count.toString()
}
```

**Debounce Frequent Updates:**

```typescript
import { debounce } from 'lodash'

const updateUI = debounce((data) => {
  logseq.provideUI({
    key: 'my-ui',
    template: generateTemplate(data),
    replace: true,
  })
}, 200)
```

**Lazy Load Heavy Components:**

```typescript
logseq.App.onMacroRendererSlotted(({ slot, payload }) => {
  // Show loading state immediately
  logseq.provideUI({
    key: 'chart',
    slot,
    template: '<div>Loading chart...</div>',
  })

  // Load heavy library asynchronously
  import('./chart-library').then((ChartLib) => {
    logseq.provideUI({
      key: 'chart',
      slot,
      template: ChartLib.render(payload),
      replace: true,
    })
  })
})
```

---

### E. Debugging Tools

**Inspect Injected UI:**

```javascript
// In browser console
// Find all plugin-injected elements
document.querySelectorAll('[data-injected-ui]')

// Get plugin ID from element
const el = document.querySelector('[data-injected-ui]')
el.dataset.injectedUi // => 'my-plugin--my-ui'

// View all active UI effects
window.__injectedUIEffects
```

**Plugin State Inspection:**

```javascript
// Get all registered plugins
LSPluginCore.registeredPlugins

// Get specific plugin
const plugin = LSPluginCore.registeredPlugins.get('my-plugin')

// Check plugin settings
plugin.settings

// View installed hooks
plugin._hooks
```

**CSS Debugging:**

```css
/* Highlight all plugin UI */
[data-injected-ui] {
  outline: 2px solid red !important;
}

/* Check theme variables */
:root {
  --debug-bg: var(--ls-primary-background-color);
}
```

---

### F. Migration Guide (Legacy to New)

**Old API (Deprecated):**

```typescript
// Old way
logseq.UI.registerIcon('my-icon', '<svg>...</svg>')
logseq.UI.showMsg('Hello')
```

**New API (Current):**

```typescript
// New way
logseq.provideUI({
  key: 'my-icon',
  path: '#toolbar',
  template: '<button><svg>...</svg></button>',
})

logseq.UI.showMsg('Hello', 'success')
```

---

### G. Related Resources

**Official Documentation:**

- Plugin API: https://plugins-doc.logseq.com/
- Sample Plugins: https://github.com/logseq/logseq-plugin-samples

**Internal Documentation:**

- [AGENTS.md](file:///Users/dzu/Projects/src/github.com/logseq/AGENTS.md) - Repository guidelines
- [CONTRIBUTING.md](file:///Users/dzu/Projects/src/github.com/logseq/CONTRIBUTING.md) - Contribution guide
- [prompts/review.md](file:///Users/dzu/Projects/src/github.com/logseq/prompts/review.md) - Code review checklist

**Investigation Template:**

- [code-detective-investigation.prompt.md](file:///Users/dzu/Projects/src/github.com/logseq/.github/prompts/code-detective-investigation.prompt.md)

---

### H. Change History

| Version | Date       | Changes                     |
| ------- | ---------- | --------------------------- |
| 1.0     | 2025-12-15 | Initial comprehensive guide |

---

### I. Contributors

This guide was created through comprehensive code investigation using the Code Detective framework.

**Investigation Tools Used:**

- Semantic search for plugin-related code
- File pattern matching for CSS/ClojureScript files
- Code reading and analysis across multiple files
- Cross-reference validation

**Files Analyzed:** 40+ files across TypeScript, ClojureScript, and CSS

---

## License

This guide is part of the Logseq project and follows the same license (AGPL-3.0).
