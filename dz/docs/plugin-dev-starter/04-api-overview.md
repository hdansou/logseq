# API Overview

Complete overview of the Logseq Plugin SDK API.

## Table of Contents

- [Global API Object](#global-api-object)
- [Core Namespaces](#core-namespaces)
- [API Categories](#api-categories)
- [Common Patterns](#common-patterns)

## Global API Object

All plugin APIs are accessed through the global `logseq` object:

```typescript
import '@logseq/libs'

// Now logseq is available globally
logseq.Editor.*
logseq.App.*
logseq.UI.*
logseq.DB.*
logseq.Assets.*
```

## Core Namespaces

### 1. logseq.Editor

Content manipulation and editing operations.

```typescript
// Block operations
insertBlock(parentUuid, content, opts?)
updateBlock(uuid, content)
removeBlock(uuid)
getBlock(uuid, opts?)
getCurrentBlock()

// Page operations
createPage(pageName, properties?, opts?)
deletePage(pageName)
getPage(idOrName, opts?)
getCurrentPage()

// Cursor operations
insertAtEditingCursor(content)
editBlock(uuid)
exitEditingMode(selectBlock?)

// Selection
getSelectedBlocks()
selectBlock(uuid)
```

### 2. logseq.App

Application-level operations and lifecycle.

```typescript
// Commands
registerCommand(command, callback)
registerCommandPalette(command, callback)
registerSlashCommand(command, actions)

// UI Registration
registerUIItem(type, config)

// App Info
getCurrentGraph()
getUserInfo()
getStateFromStore(path)

// Navigation
pushState(state, path)
```

### 3. logseq.UI

User interface components and notifications.

```typescript
// Messages
showMsg(content, status?, opts?)

// Modals (planned)
showModal(config)
closeModal(id)
```

### 4. logseq.DB

Database queries and graph data access.

```typescript
// Datalog queries
datascriptQuery(query, ...inputs)

// Entity access
q(query)
```

### 5. logseq.Assets

File and asset management.

```typescript
// File operations
listFilesOfCurrentGraph()
```

## API Categories

### Content Management

**Creating Content:**

```typescript
// Create a new block
const block = await logseq.Editor.insertBlock(parentUuid, 'Block content', {
  before: false,
  sibling: false,
})

// Create a new page
const page = await logseq.Editor.createPage('My New Page', {
  tags: ['tag1', 'tag2'],
})
```

**Reading Content:**

```typescript
// Get a block
const block = await logseq.Editor.getBlock(uuid)
console.log(block.content)

// Get current editing block
const current = await logseq.Editor.getCurrentBlock()

// Get a page
const page = await logseq.Editor.getPage('Page Name')
```

**Updating Content:**

```typescript
// Update block content
await logseq.Editor.updateBlock(uuid, 'New content')

// Update page properties
await logseq.Editor.upsertBlockProperty(uuid, 'property-name', 'value')
```

**Deleting Content:**

```typescript
// Remove a block
await logseq.Editor.removeBlock(uuid)

// Delete a page
await logseq.Editor.deletePage('Page Name')
```

### User Interaction

**Commands:**

```typescript
// Slash command
logseq.Editor.registerSlashCommand('My Command', async () => {
  await logseq.Editor.insertAtEditingCursor('Command executed!')
})

// Keyboard shortcut
logseq.App.registerCommandPalette(
  {
    key: 'my-command',
    label: 'My Command',
    keybinding: { binding: 'mod+shift+x' },
  },
  callback
)
```

**UI Elements:**

```typescript
// Toolbar button
logseq.App.registerUIItem('toolbar', {
  key: 'my-button',
  template: '<button>Click Me</button>',
})

// Notifications
logseq.UI.showMsg('Success!', 'success')
logseq.UI.showMsg('Warning!', 'warning')
logseq.UI.showMsg('Error!', 'error')
```

### Data Queries

**Basic Queries:**

```typescript
// Find blocks with tag
const blocks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/refs ?r]
   [?r :block/name "my-tag"]]
`)
```

**Advanced Queries:**

```typescript
// Complex query with joins
const results = await logseq.DB.datascriptQuery(`
  [:find ?page-name ?block-content
   :where
   [?b :block/page ?p]
   [?p :block/name ?page-name]
   [?b :block/content ?block-content]
   [(re-find #"TODO" ?block-content)]]
`)
```

### Settings & Configuration

**Define Settings Schema:**

```typescript
logseq.useSettingsSchema([
  {
    key: 'apiKey',
    type: 'string',
    default: '',
    title: 'API Key',
    description: 'Your API key',
  },
  {
    key: 'enabled',
    type: 'boolean',
    default: true,
    title: 'Enable Feature',
  },
])
```

**Access Settings:**

```typescript
const apiKey = logseq.settings?.apiKey
const enabled = logseq.settings?.enabled
```

### Event Handling

**Listen to Events:**

```typescript
// Settings changed
logseq.on('settings:changed', (settings) => {
  console.log('Settings updated:', settings)
})

// UI visibility changed
logseq.on('ui:visible:changed', ({ visible }) => {
  console.log('UI visible:', visible)
})
```

## Common Patterns

### Safe API Calls

Always check for null and handle errors:

```typescript
try {
  const block = await logseq.Editor.getCurrentBlock()
  if (!block) {
    logseq.UI.showMsg('No block selected', 'warning')
    return
  }

  // Process block safely
  await processBlock(block)
} catch (error) {
  console.error('Operation failed:', error)
  logseq.UI.showMsg('Error: ' + error.message, 'error')
}
```

### Batch Operations

Process multiple items efficiently:

```typescript
// Get all TODO blocks
const todoBlocks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/content ?content]
   [(re-find #"TODO" ?content)]]
`)

// Process in batch
for (const [block] of todoBlocks) {
  await processBlock(block)
}
```

### Debouncing

Avoid excessive API calls:

```typescript
let timeout: NodeJS.Timeout

function debouncedSave(data: any) {
  clearTimeout(timeout)
  timeout = setTimeout(async () => {
    await saveData(data)
  }, 500)
}
```

### Model Pattern

Provide UI callbacks:

```typescript
// Register model with methods
logseq.provideModel({
  async handleClick() {
    logseq.UI.showMsg('Clicked!', 'info')
  },

  async handleSubmit(data: any) {
    await processData(data)
  },
})

// Reference in UI template
logseq.App.registerUIItem('toolbar', {
  key: 'my-ui',
  template: `
    <button data-on-click="handleClick">
      Click Me
    </button>
  `,
})
```

## Type Definitions

The SDK provides TypeScript definitions:

```typescript
import '@logseq/libs'

// Types are available
import type {
  BlockEntity,
  PageEntity,
  LSPluginUserEvents,
  IHookEvent,
} from '@logseq/libs/dist/LSPlugin.user'

// Use with API calls
const block: BlockEntity | null = await logseq.Editor.getBlock(uuid)
```

## API Reference Links

For complete API documentation:

- **[LSPluginUser Class](../classes/LSPluginUser.html)** - Main plugin class
- **[IEditorProxy Interface](../interfaces/IEditorProxy.html)** - Editor methods
- **[IAppProxy Interface](../interfaces/IAppProxy.html)** - App methods
- **[IDBProxy Interface](../interfaces/IDBProxy.html)** - Database methods
- **[BlockEntity Type](../interfaces/BlockEntity.html)** - Block structure
- **[PageEntity Type](../interfaces/PageEntity.html)** - Page structure

## Next Steps

- 📝 Master [Blocks and Pages](05-blocks-and-pages.md)
- 🎨 Build [UI Components](06-ui-components.md)
- 🔍 Query with [Database Operations](10-database.md)
- ⚙️ Configure [Settings](11-settings.md)

---

The Logseq API is powerful and flexible - explore and experiment!
