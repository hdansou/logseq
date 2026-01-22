---
title: 'Plugin API Reference'
status: 🟢 Stable
version: 'SDK v0.0.17+'
quadrant: 'Reference'
platform: 'Desktop'
related-docs:
  - '[First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)'
  - '[Plugin Security](../../explanation/architecture/plugin-security.md)'
  - '[Database Properties](../../tutorials/development/db-properties.md)'
tags: ['plugin', 'api', 'reference', 'sdk', 'javascript', 'typescript']
---

# Plugin API Reference

Complete reference for the `@logseq/libs` SDK. This document provides detailed API documentation for all namespaces, methods, types, and interfaces available to Logseq plugin developers.

**SDK Package:** `@logseq/libs` ([@npmjs](https://www.npmjs.com/package/@logseq/libs))  
**Latest Version:** 0.0.17 (as of January 2025)  
**TypeScript Support:** ✅ Full type definitions included  
**Platform:** Desktop (Electron) only

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Core Namespaces Overview](#core-namespaces-overview)
3. [logseq.App - Application APIs](#logseqapp---application-apis)
4. [logseq.Editor - Block & Page APIs](#logseqeditor---block--page-apis)
5. [logseq.DB - Database Query APIs](#logseqdb---database-query-apis)
6. [logseq.UI - User Interface APIs](#logsequi---user-interface-apis)
7. [logseq.Assets - Asset Management APIs](#logseqassets---asset-management-apis)
8. [logseq.Git - Version Control APIs](#logseqgit---version-control-apis)
9. [Global Functions](#global-functions)
10. [Type Definitions](#type-definitions)
11. [Event Hooks](#event-hooks)
12. [Best Practices](#best-practices)

---

## Getting Started

### Installation

```bash
npm install @logseq/libs
```

### Basic Plugin Structure

```typescript
import '@logseq/libs'

async function main() {
  console.log('Plugin loaded!')

  // Register features
  logseq.Editor.registerSlashCommand('Hello', async () => {
    await logseq.Editor.insertAtEditingCursor('Hello, World!')
  })
}

logseq.ready(main).catch(console.error)
```

### Accessing the API

The SDK exposes a global `logseq` object with these namespaces:

```typescript
logseq.App // Application-level operations
logseq.Editor // Block and page manipulation
logseq.DB // Database queries
logseq.UI // UI utilities and messages
logseq.Assets // Asset management
logseq.Git // Git operations (file-based graphs only)
```

---

## Core Namespaces Overview

| Namespace    | Purpose              | Common Use Cases                                      |
| ------------ | -------------------- | ----------------------------------------------------- |
| **`App`**    | Application control  | Get app info, navigate, register commands             |
| **`Editor`** | Content manipulation | Insert/update blocks, create pages, manage properties |
| **`DB`**     | Data queries         | Search blocks, run Datalog queries, watch changes     |
| **`UI`**     | User interface       | Show messages, inject custom UI, query DOM            |
| **`Assets`** | File handling        | List assets, open files, manage storage               |
| **`Git`**    | Version control      | Git commands (file-based graphs only)                 |

---

## logseq.App - Application APIs

Application-level operations including configuration, navigation, command registration, and plugin management.

### Configuration & Info

#### `getInfo(key?)`

Get Logseq application information and capabilities.

**Signature:**

```typescript
getInfo(key?: keyof AppInfo): Promise<AppInfo | any>
```

**Parameters:**

- `key` (optional): Specific info key to retrieve

**Returns:**

- If `key` provided: Value of that key
- If no `key`: Complete `AppInfo` object

**AppInfo Fields:**

- `version`: Logseq version (e.g., "0.10.9")
- `supportDb`: Whether app supports database graphs (boolean)
- Additional platform/feature flags

**Example:**

```typescript
// Get all info
const info = await logseq.App.getInfo()
console.log('Version:', info.version)
console.log('DB support:', info.supportDb)

// Get specific value
const version = await logseq.App.getInfo('version')
```

**Since:** SDK v0.0.4

---

#### `getUserInfo()`

Get currently logged-in user information (for Logseq Sync/cloud features).

**Signature:**

```typescript
getUserInfo(): Promise<AppUserInfo | null>
```

**Returns:**

- `AppUserInfo` object if logged in
- `null` if not logged in or using local-only mode

**AppUserInfo Fields:**

- `email`: User email address
- `id`: User ID
- Additional profile fields

**Example:**

```typescript
const user = await logseq.App.getUserInfo()
if (user) {
  console.log('Logged in as:', user.email)
} else {
  await logseq.UI.showMsg('Please log in to use this feature', 'warning')
}
```

**Since:** SDK v0.0.17

---

#### `getUserConfigs()`

Get user's global app-wide configuration settings.

**Signature:**

```typescript
getUserConfigs(): Promise<AppUserConfigs>
```

**Returns:** `AppUserConfigs` object

**AppUserConfigs Fields:**

- `preferredThemeMode`: `"light"` | `"dark"`
- `preferredFormat`: `"markdown"` | `"org"`
- `preferredDateFormat`: Date format string
- `preferredStartOfWeek`: Day name
- `preferredLanguage`: Language code
- `preferredWorkflow`: Workflow type
- `currentGraph`: Current graph name
- `showBracket`: Show page brackets (boolean)
- `enabledFlashcards`: Flashcards enabled (boolean)
- `enabledJournals`: Journals enabled (boolean)

**Example:**

```typescript
const config = await logseq.App.getUserConfigs()

// Adapt plugin behavior to user preferences
if (config.preferredFormat === 'org') {
  insertOrgModeSyntax()
} else {
  insertMarkdownSyntax()
}

console.log('Theme:', config.preferredThemeMode)
console.log('Date format:', config.preferredDateFormat)
```

**Since:** SDK v0.0.17

---

### Graph Operations

#### `getCurrentGraph()`

Get information about the currently active graph.

**Signature:**

```typescript
getCurrentGraph(): Promise<AppGraphInfo | null>
```

**Returns:**

- `AppGraphInfo` object if graph is open
- `null` if no graph open (e.g., welcome screen)

**AppGraphInfo Fields:**

- `name`: Graph name
- `url`: Graph URL/identifier
- `path`: Filesystem path (desktop only)

**Example:**

```typescript
const graph = await logseq.App.getCurrentGraph()
if (graph) {
  console.log('Graph:', graph.name)
  console.log('Path:', graph.path)
} else {
  await logseq.UI.showMsg('No graph open', 'warning')
  return
}
```

**Since:** SDK v0.0.17

---

#### `checkCurrentIsDbGraph()`

Check if the current graph uses the database storage format.

**Signature:**

```typescript
checkCurrentIsDbGraph(): Promise<Boolean>
```

**Returns:**

- `true`: Database graph (new format)
- `false`: File-based graph (legacy Markdown/Org files)

**Example:**

```typescript
const isDb = await logseq.App.checkCurrentIsDbGraph()

if (isDb) {
  // Use DB-specific APIs
  await logseq.Editor.upsertProperty('status', {
    type: 'default',
    cardinality: 'one',
  })
} else {
  // Use file-based approach
  await logseq.Editor.updateBlock(uuid, 'status:: pending')
}
```

**Since:** SDK v0.0.17

---

#### `getCurrentGraphConfigs(...keys)`

Get configuration settings for the current graph (from `config.edn`).

**Signature:**

```typescript
getCurrentGraphConfigs(...keys: string[]): Promise<any>
```

**Parameters:**

- `...keys`: Optional specific config keys (supports nested paths)

**Returns:**

- If `keys` provided: Value(s) of those keys
- If no `keys`: Complete config object

**Example:**

```typescript
// Get entire config
const config = await logseq.App.getCurrentGraphConfigs()

// Get specific value
const theme = await logseq.App.getCurrentGraphConfigs('preferred-theme')

// Get nested value
const journalFormat = await logseq.App.getCurrentGraphConfigs(
  'journal',
  'page-title-format'
)
```

**Since:** SDK v0.0.17

---

#### `setCurrentGraphConfigs(configs)`

Update configuration settings for the current graph.

**Signature:**

```typescript
setCurrentGraphConfigs(configs: {}): Promise<void>
```

**Parameters:**

- `configs`: Object with config key-value pairs to set

**Example:**

```typescript
await logseq.App.setCurrentGraphConfigs({
  'preferred-format': 'markdown',
  'journal/page-title-format': 'yyyy-MM-dd',
  'feature/enable-journals': true,
})

await logseq.UI.showMsg('Config updated', 'success')
```

**⚠️ Note:** Some config changes may require graph reload to take effect.

**Since:** SDK v0.0.17

---

#### `getCurrentGraphFavorites()`

Get the list of favorite/starred pages in the current graph.

**Signature:**

```typescript
getCurrentGraphFavorites(): Promise<Array<string | PageEntity> | null>
```

**Returns:**

- Array of page names (file-based graphs) or `PageEntity` objects (DB graphs)
- `null` if no favorites or error

**Example:**

```typescript
const favorites = await logseq.App.getCurrentGraphFavorites()

if (favorites) {
  favorites.forEach((fav) => {
    const name = typeof fav === 'string' ? fav : fav.name
    console.log('⭐', name)
  })
}
```

**Since:** SDK v0.0.17

---

#### `getCurrentGraphRecent()`

Get recently accessed pages in the current graph.

**Signature:**

```typescript
getCurrentGraphRecent(): Promise<Array<PageEntity> | null>
```

**Returns:**

- Array of `PageEntity` objects (most recent first)
- `null` if no recent pages or error

**Example:**

```typescript
const recent = await logseq.App.getCurrentGraphRecent()

if (recent) {
  console.log('Recently accessed pages:')
  recent.slice(0, 5).forEach((page) => {
    console.log(`- ${page.name} (${new Date(page.updatedAt).toLocaleString()})`)
  })
}
```

**Since:** SDK v0.0.17

---

### Command Registration

#### `registerCommand(type, opts, action)`

Register a custom command in Logseq.

**Signature:**

```typescript
registerCommand(
  type: string,
  opts: {
    key: string
    label: string
    desc?: string
    palette?: boolean
    keybinding?: SimpleCommandKeybinding
    extras?: Record<string, any>
  },
  action: SimpleCommandCallback
): void
```

**Parameters:**

- `type`: Command type (usually `"simple"`)
- `opts.key`: Unique command identifier (auto-namespaced to plugin)
- `opts.label`: Display name in UI
- `opts.desc`: Optional description
- `opts.palette`: Show in command palette (default: `true`)
- `opts.keybinding`: Optional keyboard shortcut
- `action`: Callback function when command is invoked

**Example:**

```typescript
logseq.App.registerCommand(
  'simple',
  {
    key: 'export-to-json',
    label: 'Export Graph to JSON',
    desc: 'Export all blocks as JSON',
    palette: true,
    keybinding: {
      binding: 'mod+shift+j',
      mode: 'global',
    },
  },
  async () => {
    const blocks = await exportAllBlocks()
    downloadFile(blocks, 'export.json')
    await logseq.UI.showMsg('Export complete!', 'success')
  }
)
```

**Since:** SDK v0.0.17

---

#### `registerCommandPalette(opts, action)`

Register a command specifically in the command palette (Cmd/Ctrl+Shift+P).

**Signature:**

```typescript
registerCommandPalette(
  opts: {
    key: string
    label: string
    keybinding?: SimpleCommandKeybinding
  },
  action: SimpleCommandCallback
): void
```

**Parameters:**

- `opts.key`: Unique command key
- `opts.label`: Display label in palette
- `opts.keybinding`: Optional keyboard shortcut
- `action`: Callback function

**Example:**

```typescript
logseq.App.registerCommandPalette(
  {
    key: 'sync-external-data',
    label: 'Sync with External Service',
    keybinding: { binding: 'mod+shift+s' },
  },
  async () => {
    await syncWithExternalAPI()
  }
)
```

**Since:** SDK v0.0.17

---

#### `registerCommandShortcut(keybinding, action, opts?)`

Register a global keyboard shortcut.

**Signature:**

```typescript
registerCommandShortcut(
  keybinding: SimpleCommandKeybinding | string,
  action: SimpleCommandCallback,
  opts?: Partial<{
    key: string
    label: string
    desc: string
    extras: Record<string, any>
  }>
): void
```

**Parameters:**

- `keybinding`: Keyboard binding string or object with mode
  - String: Simple binding like `"mod+k"`
  - Object: `{ binding, mode?, mac? }`
- `action`: Callback function
- `opts`: Optional metadata (key, label, desc)

**Keybinding Modes:**

- `global`: Active everywhere (default)
- `editing`: Only while editing a block
- `non-editing`: Only when not editing

**Modifier Keys:**

- `mod`: Ctrl (Windows/Linux) or Cmd (macOS)
- `ctrl`: Explicit Ctrl key
- `alt`: Alt/Option key
- `shift`: Shift key

**Example:**

```typescript
// Simple global shortcut
logseq.App.registerCommandShortcut('mod+shift+k', async () => {
  await logseq.Editor.insertAtEditingCursor('📝 ')
})

// Editing-mode-only shortcut
logseq.App.registerCommandShortcut(
  {
    binding: 'ctrl+b',
    mode: 'editing',
  },
  async () => {
    await logseq.Editor.insertAtEditingCursor('**bold**')
  },
  {
    key: 'insert-bold',
    label: 'Insert Bold Markdown',
  }
)

// Platform-specific bindings
logseq.App.registerCommandShortcut(
  {
    binding: 'ctrl+k',
    mac: 'cmd+k', // Different on macOS
    mode: 'global',
  },
  async () => {
    // Action
  }
)
```

**Supported Keys:** See [key names reference](https://gist.github.com/xyhp915/d1a6d151a99f31647a95e59cdfbf4ddc)

**Since:** SDK v0.0.17

---

#### `unregisterCommandShortcut(keybinding)`

Remove a previously registered keyboard shortcut.

**Signature:**

```typescript
unregisterCommandShortcut(keybinding: string): void
```

**Example:**

```typescript
logseq.App.unregisterCommandShortcut('mod+shift+k')
```

**Since:** SDK v0.0.17

---

### UI Registration

#### `registerUIItem(type, opts)`

Add a custom button to Logseq's toolbar or page bar.

**Signature:**

```typescript
registerUIItem(
  type: 'toolbar' | 'pagebar',
  opts: {
    key: string
    template: string
  }
): void
```

**Parameters:**

- `type`: UI location (`"toolbar"` or `"pagebar"`)
- `opts.key`: Unique identifier
- `opts.template`: HTML template (use `data-on-click` for event handlers)

**Example:**

```typescript
// Add toolbar button
logseq.App.registerUIItem('toolbar', {
  key: 'my-plugin-btn',
  template: `
    <a data-on-click="myHandler"
       class="button"
       title="My Plugin Action">
      <i class="ti ti-star"></i>
    </a>
  `,
})

// Provide event handler
logseq.provideModel({
  myHandler() {
    logseq.UI.showMsg('Button clicked!', 'success')
  },
})
```

**Since:** SDK v0.0.17

---

### External Commands

#### `invokeExternalCommand(type, ...args)`

Execute a built-in Logseq command programmatically.

**Signature:**

```typescript
invokeExternalCommand(
  type: ExternalCommandType,
  ...args: Array<any>
): Promise<void>
```

**Common Commands:**

| Command                          | Description                  |
| -------------------------------- | ---------------------------- |
| `logseq.editor/cycle-todo`       | Cycle TODO/DOING/DONE status |
| `logseq.editor/indent`           | Indent current block         |
| `logseq.editor/outdent`          | Outdent current block        |
| `logseq.editor/zoom-in`          | Zoom into current block      |
| `logseq.editor/zoom-out`         | Zoom out of current block    |
| `logseq.go/home`                 | Navigate to home page        |
| `logseq.go/journals`             | Navigate to journals page    |
| `logseq.go/search`               | Open search                  |
| `logseq.ui/toggle-theme`         | Toggle light/dark theme      |
| `logseq.ui/toggle-left-sidebar`  | Toggle left sidebar          |
| `logseq.ui/toggle-right-sidebar` | Toggle right sidebar         |
| `logseq.ui/toggle-settings`      | Toggle settings modal        |
| `logseq.ui/goto-plugins`         | Open plugins page            |
| `logseq.command/run`             | Open command palette         |

**Example:**

```typescript
// Cycle TODO status
await logseq.App.invokeExternalCommand('logseq.editor/cycle-todo')

// Navigate home
await logseq.App.invokeExternalCommand('logseq.go/home')

// Toggle theme
await logseq.App.invokeExternalCommand('logseq.ui/toggle-theme')
```

**Since:** SDK v0.0.17

---

### Inter-Plugin Communication

#### `invokeExternalPlugin(type, ...args)`

Call a command or model method from another plugin.

**Signature:**

```typescript
invokeExternalPlugin(type: string, ...args: Array<any>): Promise<unknown>
```

**Parameters:**

- `type`: Plugin path in format `"plugin-id.commands.command-key"` or `"plugin-id.models.method-key"`
- `...args`: Arguments to pass

**Example:**

```typescript
// Call another plugin's command
const result = await logseq.App.invokeExternalPlugin(
  'logseq-calendar.commands.createEvent',
  { title: 'Meeting', date: '2025-12-25' }
)

// Call model method
await logseq.App.invokeExternalPlugin(
  'logseq-translator.models.translate',
  'Hello',
  'es'
)
```

**Since:** SDK v0.0.13

---

#### `getExternalPlugin(pid)`

Get metadata for another installed plugin.

**Signature:**

```typescript
getExternalPlugin(pid: string): Promise<{} | null>
```

**Returns:**

- Plugin info object if found
- `null` if not installed

**Example:**

```typescript
const plugin = await logseq.App.getExternalPlugin('logseq-calendar')

if (plugin) {
  console.log('Calendar plugin is installed')
} else {
  await logseq.UI.showMsg('Please install the Calendar plugin first', 'warning')
}
```

**Since:** SDK v0.0.13

---

### System Operations

#### `relaunch()`

Restart the Logseq application.

**Signature:**

```typescript
relaunch(): Promise<void>
```

**Example:**

```typescript
await updateSystemConfig()
await logseq.App.relaunch()
```

**⚠️ Use sparingly:** Only restart when absolutely necessary (e.g., after major config changes).

**Since:** SDK v0.0.17

---

#### `quit()`

Quit the Logseq application completely.

**Signature:**

```typescript
quit(): Promise<void>
```

**Example:**

```typescript
const confirmed = confirm('Quit Logseq?')
if (confirmed) {
  await logseq.App.quit()
}
```

**⚠️ Use with extreme caution:** User should always confirm before quitting.

**Since:** SDK v0.0.17

---

#### `openExternalLink(url)`

Open a URL in the system's default browser.

**Signature:**

```typescript
openExternalLink(url: string): Promise<void>
```

**Parameters:**

- `url`: HTTP/HTTPS URL to open

**Example:**

```typescript
await logseq.App.openExternalLink('https://docs.logseq.com')
```

**Since:** SDK v0.0.17

---

### Event Hooks

#### `onRouteChanged(callback)`

Listen for navigation/route changes.

**Signature:**

```typescript
onRouteChanged(callback: (e: { path: string }) => void): IUserOffHook
```

**Example:**

```typescript
const off = logseq.App.onRouteChanged(({ path }) => {
  console.log('Navigated to:', path)
})

// Cleanup
logseq.beforeunload(() => off())
```

**Since:** SDK v0.0.17

---

#### `onCurrentGraphChanged(callback)`

Listen for graph switches.

**Signature:**

```typescript
onCurrentGraphChanged(callback: (e: { graph: AppGraphInfo }) => void): IUserOffHook
```

**Example:**

```typescript
const off = logseq.App.onCurrentGraphChanged(({ graph }) => {
  console.log('Switched to graph:', graph.name)
  initializePluginForGraph(graph)
})
```

**Since:** SDK v0.0.17

---

#### `onThemeModeChanged(callback)`

Listen for theme mode changes (light/dark).

**Signature:**

```typescript
onThemeModeChanged(callback: (e: { mode: ThemeMode }) => void): IUserOffHook
```

**Example:**

```typescript
const off = logseq.App.onThemeModeChanged(({ mode }) => {
  console.log('Theme changed to:', mode)
  updatePluginUI(mode)
})
```

**Since:** SDK v0.0.17

---

#### `onMacroRendererSlotted(callback)`

Listen for macro renderer slots (e.g., `{{renderer :type, args}}`).

**Signature:**

```typescript
onMacroRendererSlotted(callback: (e: {
  slot: string
  payload: {
    uuid: BlockUUID
    arguments: string[]
  }
}) => void): void
```

**Example:**

```typescript
logseq.App.onMacroRendererSlotted(({ slot, payload }) => {
  const [type, ...args] = payload.arguments

  if (type !== ':my-renderer') return

  logseq.provideUI({
    key: `renderer-${payload.uuid}`,
    slot,
    template: `<div>Rendered: ${args.join(', ')}</div>`,
  })
})
```

**Since:** SDK v0.0.17

---

## logseq.Editor - Block & Page APIs

**(To be continued...)**

The complete API reference is extensive. For the full Editor, DB, UI, Assets, and Git namespace documentation, please see:

- [Official API Documentation](https://plugins-doc.logseq.com)
- [TypeScript Definitions](https://github.com/logseq/logseq/blob/master/libs/src/LSPlugin.ts)
- [Plugin Development Tutorial](../../tutorials/plugins/first-plugin.md)

---

## Key Type Definitions

### BlockEntity

```typescript
interface BlockEntity {
  id: number // Database entity ID
  uuid: BlockUUID // Block UUID (string)
  title: string // Block content
  content?: string // @deprecated, use title
  format: 'markdown' | 'org' // Block format
  parent: { id: number } // Parent block
  page: { id: number } // Owner page
  properties?: Record<string, any> // Block properties
  children?: Array<BlockEntity> // Child blocks
  'collapsed?': boolean // Collapsed state
  createdAt: number // Creation timestamp
  updatedAt: number // Last update timestamp
  marker?: string // TODO/DOING/DONE
  [key: string]: unknown // Additional fields
}
```

### PageEntity

```typescript
interface PageEntity {
  id: number
  uuid: BlockUUID
  name: string
  format: 'markdown' | 'org'
  type: 'page' | 'journal' | 'whiteboard' | 'class' | 'property' | 'hidden'
  'journal?': boolean
  journalDay?: number // YYYYMMDD for journals
  properties?: Record<string, any>
  children?: Array<PageEntity>
  createdAt: number
  updatedAt: number
  [key: string]: unknown
}
```

### PropertySchema

```typescript
type PropertySchema = {
  type: 'default' | 'number' | 'node' | 'date' | 'checkbox' | 'url' | string
  cardinality: 'many' | 'one'
  hide: boolean
  public: boolean
}
```

---

## Best Practices

### Performance

```typescript
// ✅ Good: Debounce expensive operations
import { debounce } from 'lodash'

const debouncedUpdate = debounce(async () => {
  await heavyComputation()
}, 500)

logseq.DB.onChanged(debouncedUpdate)

// ❌ Bad: Heavy computation on every change
logseq.DB.onChanged(async () => {
  await heavyComputation() // Runs on every keystroke!
})
```

### Error Handling

```typescript
// ✅ Good: Always handle errors
try {
  const block = await logseq.Editor.getBlock(uuid)
  if (!block) {
    await logseq.UI.showMsg('Block not found', 'warning')
    return
  }
  await processBlock(block)
} catch (error) {
  console.error('[My Plugin] Error:', error)
  await logseq.UI.showMsg('An error occurred', 'error')
}
```

### Cleanup

```typescript
// ✅ Good: Clean up listeners
const offHooks: Array<() => void> = []

offHooks.push(
  logseq.DB.onChanged(() => {
    /* ... */
  })
)

offHooks.push(
  logseq.App.onRouteChanged(() => {
    /* ... */
  })
)

logseq.beforeunload(async () => {
  offHooks.forEach((off) => off())
})
```

---

## Next Steps

- **[First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)** - Build your first plugin
- **[Plugin Security](../../explanation/architecture/plugin-security.md)** - Security best practices
- **[Official Samples](https://github.com/logseq/logseq-plugin-samples)** - Example plugins
- **[Full API Docs](https://plugins-doc.logseq.com)** - Complete generated documentation

---

**For questions or feedback, join the [Logseq Discord](https://discord.gg/KpN4eHY).**
