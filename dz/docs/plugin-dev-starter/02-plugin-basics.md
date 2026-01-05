# Plugin Basics

Understanding core concepts and architecture of Logseq plugins.

## Table of Contents

- [Plugin Architecture](#plugin-architecture)
- [Plugin Lifecycle](#plugin-lifecycle)
- [Core APIs](#core-apis)
- [Plugin Manifest](#plugin-manifest)
- [Plugin Types](#plugin-types)

## Plugin Architecture

### Overview

Logseq plugins run in an isolated environment and communicate with the main Logseq app through:

```
┌─────────────────────────────────────┐
│         Logseq Main App             │
│  ┌───────────────────────────────┐  │
│  │   Graph Database (DataScript) │  │
│  └───────────────────────────────┘  │
│              ↕ IPC                  │
│  ┌───────────────────────────────┐  │
│  │      Plugin Sandbox           │  │
│  │  ┌─────────────────────────┐  │  │
│  │  │   Your Plugin Code      │  │  │
│  │  │   (@logseq/libs API)    │  │  │
│  │  └─────────────────────────┘  │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

### Key Concepts

1. **Isolation**: Plugins run in a sandboxed environment for security
2. **IPC Communication**: All API calls go through Inter-Process Communication
3. **Asynchronous**: Most API methods return Promises
4. **Event-Driven**: React to user actions and app events

## Plugin Lifecycle

### Initialization Flow

```typescript
import '@logseq/libs'

// 1. Define your main function
const main = async () => {
  console.log('Plugin initializing...')

  // 2. Register commands, UI, etc.
  setupCommands()
  setupUI()
  setupEventListeners()

  console.log('Plugin ready!')
}

// 3. Bootstrap the plugin
logseq.ready(main).catch(console.error)
```

### Lifecycle Hooks

```typescript
// Plugin ready - called when Logseq is ready
logseq.ready(async () => {
  // Your initialization code
})

// Before unload - cleanup before plugin unloads
logseq.beforeunload(async () => {
  // Save state, cleanup resources
  await savePluginState()
  console.log('Plugin unloading...')
})

// App unmount - called when Logseq is closing
logseq.App.onCurrentGraphChanged(() => {
  // Handle graph switches
})
```

### Lifecycle States

```typescript
class PluginLifecycle {
  // 1. Loading
  async onLoad() {
    console.log('Loading plugin...')
    // Load settings, initialize state
  }

  // 2. Ready
  async onReady() {
    console.log('Plugin ready!')
    // Register commands, setup UI
  }

  // 3. Active
  async onActive() {
    // Handle user interactions
  }

  // 4. Unloading
  async onUnload() {
    console.log('Unloading plugin...')
    // Cleanup, save state
  }
}
```

## Core APIs

### Global `logseq` Object

All plugin functionality is accessed through the global `logseq` object:

```typescript
// Import types and initialize
import '@logseq/libs'

// Now you can use logseq.* APIs
logseq.Editor.insertBlock(...)
logseq.App.showMsg(...)
logseq.DB.datascriptQuery(...)
```

### Main API Namespaces

#### 1. **logseq.Editor** - Content Manipulation

```typescript
// Insert, update, delete blocks
await logseq.Editor.insertBlock(parentUuid, content)
await logseq.Editor.updateBlock(blockUuid, newContent)
await logseq.Editor.removeBlock(blockUuid)

// Get block data
const block = await logseq.Editor.getBlock(blockUuid)
const page = await logseq.Editor.getCurrentPage()
```

#### 2. **logseq.App** - Application Control

```typescript
// Commands and shortcuts
logseq.App.registerCommandPalette(command, callback)
logseq.App.registerUIItem('toolbar', config)

// App state
const userInfo = await logseq.App.getUserInfo()
const currentGraph = await logseq.App.getCurrentGraph()
```

#### 3. **logseq.UI** - User Interface

```typescript
// Notifications
logseq.UI.showMsg('Hello!', 'success')

// Modals
logseq.UI.showMsg('Confirm?', 'warning')
```

#### 4. **logseq.DB** - Database Queries

```typescript
// Datalog queries
const results = await logseq.DB.datascriptQuery(`
  [:find ?b
   :where
   [?b :block/content ?content]]
`)
```

#### 5. **logseq.Assets** - File Management

```typescript
// Load and save files
const content = await logseq.Assets.listFilesOfCurrentGraph()
```

## Plugin Manifest

### package.json Configuration

Every plugin needs a `logseq` field in `package.json`:

```json
{
  "name": "my-plugin",
  "version": "1.0.0",
  "main": "dist/index.js",
  "logseq": {
    "id": "my-unique-plugin-id",
    "title": "My Plugin",
    "icon": "🎨",
    "description": "A helpful plugin for Logseq"
  }
}
```

### Required Fields

| Field   | Type   | Description                    |
| ------- | ------ | ------------------------------ |
| `id`    | string | Unique identifier (kebab-case) |
| `title` | string | Display name in UI             |
| `main`  | string | Entry point file path          |

### Optional Fields

| Field         | Type     | Description           |
| ------------- | -------- | --------------------- |
| `icon`        | string   | Emoji or icon         |
| `description` | string   | Short description     |
| `author`      | string   | Plugin author         |
| `repo`        | string   | GitHub repository URL |
| `keywords`    | string[] | Search keywords       |
| `license`     | string   | License type          |

### Full Example

```json
{
  "name": "@myorg/logseq-plugin-awesome",
  "version": "1.2.0",
  "description": "An awesome Logseq plugin",
  "main": "dist/index.js",
  "author": "Your Name",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "https://github.com/username/plugin-repo.git"
  },
  "logseq": {
    "id": "awesome-plugin",
    "title": "Awesome Plugin",
    "icon": "⚡",
    "description": "Makes Logseq more awesome"
  }
}
```

## Plugin Types

### 1. Content Plugins

Manipulate blocks, pages, and content:

```typescript
// Example: Auto-formatter plugin
logseq.Editor.registerSlashCommand('Format Code', async () => {
  const block = await logseq.Editor.getCurrentBlock()
  if (block) {
    const formatted = formatCode(block.content)
    await logseq.Editor.updateBlock(block.uuid, formatted)
  }
})
```

### 2. UI Plugins

Add custom user interface elements:

```typescript
// Example: Custom sidebar
logseq.App.registerUIItem('toolbar', {
  key: 'my-sidebar-toggle',
  template: `<button>Toggle Sidebar</button>`,
})
```

### 3. Query Plugins

Provide advanced data querying:

```typescript
// Example: Advanced search plugin
const searchBlocks = async (query: string) => {
  return await logseq.DB.datascriptQuery(`
    [:find ?b ?content
     :where
     [?b :block/content ?content]
     [(re-find #"${query}" ?content)]]
  `)
}
```

### 4. Integration Plugins

Connect Logseq to external services:

```typescript
// Example: API integration
async function syncToService() {
  const blocks = await getAllModifiedBlocks()
  await fetch('https://api.example.com/sync', {
    method: 'POST',
    body: JSON.stringify(blocks),
  })
}
```

### 5. Productivity Plugins

Enhance workflow and automation:

```typescript
// Example: Template expander
logseq.Editor.registerSlashCommand('Daily Template', async () => {
  const template = `
    ## 🎯 Goals
    - 
    
    ## 📝 Notes
    - 
    
    ## ✅ Tasks
    - 
  `
  await logseq.Editor.insertAtEditingCursor(template)
})
```

## Best Practices

### 1. Error Handling

Always wrap async operations in try-catch:

```typescript
try {
  const block = await logseq.Editor.getBlock(uuid)
  // Process block
} catch (error) {
  console.error('Failed to get block:', error)
  logseq.UI.showMsg('Error: ' + error.message, 'error')
}
```

### 2. Null Checks

API methods may return `null`:

```typescript
const block = await logseq.Editor.getCurrentBlock()
if (!block) {
  logseq.UI.showMsg('No block selected', 'warning')
  return
}
// Use block safely
```

### 3. Clean Initialization

```typescript
const main = async () => {
  try {
    await initializeSettings()
    await registerCommands()
    await setupUI()
    console.log('✅ Plugin loaded successfully')
  } catch (error) {
    console.error('❌ Plugin initialization failed:', error)
  }
}

logseq.ready(main).catch(console.error)
```

### 4. Resource Cleanup

```typescript
logseq.beforeunload(async () => {
  // Clear intervals
  clearInterval(updateInterval)

  // Save state
  await savePluginState()

  // Remove event listeners
  removeAllListeners()
})
```

## Development Tips

1. **Use TypeScript** - Get type safety and autocomplete
2. **Enable source maps** - Better debugging experience
3. **Log liberally** - Help with troubleshooting
4. **Test incrementally** - Test each feature as you build
5. **Read plugin samples** - Learn from existing plugins

## Next Steps

- 📝 Learn [Blocks and Pages](05-blocks-and-pages.md) manipulation
- 🎨 Explore [UI Components](06-ui-components.md)
- ⌨️ Master [Commands & Shortcuts](07-commands-shortcuts.md)
- 🔍 Deep dive into [Database Operations](10-database.md)

---

You now understand the fundamentals of Logseq plugin development!
