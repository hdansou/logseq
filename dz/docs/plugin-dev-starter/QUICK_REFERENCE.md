# Logseq Plugin SDK - Quick Reference

Fast lookup for common APIs and patterns.

## Core APIs

### logseq.Editor

```typescript
// Blocks
insertBlock(parent, content, opts?)
updateBlock(uuid, content)
removeBlock(uuid)
getBlock(uuid, opts?)
getCurrentBlock()
getSelectedBlocks()

// Cursor
insertAtEditingCursor(content)
editBlock(uuid)
exitEditingMode(selectBlock?)

// Pages
createPage(name, properties?, opts?)
deletePage(name)
getPage(idOrName, opts?)
getCurrentPage()
getAllPages()
getPageBlocksTree(pageName)

// Properties
upsertBlockProperty(uuid, key, value)
removeBlockProperty(uuid, key)
```

### logseq.App

```typescript
// Commands
registerCommand(command, callback)
registerCommandPalette(command, callback)
registerUIItem(type, config)

// App info
getCurrentGraph()
getUserInfo()
getStateFromStore(path)

// Events
onCurrentGraphChanged(callback)
```

### logseq.UI

```typescript
// Messages
showMsg(content, status?, opts?)
// status: 'success' | 'warning' | 'error' | 'info'
```

### logseq.DB

```typescript
// Queries
datascriptQuery(query, ...inputs)
q(query)
```

### logseq.Assets

```typescript
listFilesOfCurrentGraph()
```

## Common Patterns

### Basic Plugin Setup

```typescript
import '@logseq/libs'

const main = async () => {
  // Your code here
}

logseq.ready(main).catch(console.error)
```

### Slash Command

```typescript
logseq.Editor.registerSlashCommand('Command Name', async () => {
  await logseq.Editor.insertAtEditingCursor('Text')
})
```

### Keyboard Shortcut

```typescript
logseq.App.registerCommandPalette(
  {
    key: 'my-command',
    label: 'My Command',
    keybinding: { binding: 'mod+shift+x' },
  },
  callback
)
```

### Get Current Block

```typescript
const block = await logseq.Editor.getCurrentBlock()
if (block) {
  // Use block
}
```

### Create Block

```typescript
const block = await logseq.Editor.insertBlock(parentUuid, 'Content', {
  before: false,
  sibling: false,
})
```

### Query Database

```typescript
const results = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/content ?content]]
`)
```

### Settings Schema

```typescript
logseq.useSettingsSchema([
  {
    key: 'apiKey',
    type: 'string',
    default: '',
    title: 'API Key',
  },
])

// Access
const apiKey = logseq.settings?.apiKey
```

### Error Handling

```typescript
try {
  await logseq.Editor.updateBlock(uuid, content)
} catch (error) {
  console.error(error)
  logseq.UI.showMsg('Error: ' + error.message, 'error')
}
```

### Toolbar Button

```typescript
logseq.provideModel({
  handleClick() {
    logseq.UI.showMsg('Clicked!', 'info')
  },
})

logseq.App.registerUIItem('toolbar', {
  key: 'my-button',
  template: '<a data-on-click="handleClick">Button</a>',
})
```

## Keyboard Modifiers

- `mod` - Cmd (Mac) / Ctrl (Win/Linux)
- `shift` - Shift key
- `alt` - Alt/Option key
- `ctrl` - Control key

Examples:

- `mod+shift+n`
- `mod+alt+t`
- `ctrl+shift+p`

## Block Properties

```typescript
// Set property
await logseq.Editor.upsertBlockProperty(uuid, 'status', 'done')

// Get property
const block = await logseq.Editor.getBlock(uuid)
const status = block?.properties?.status
```

## Common Queries

### All TODOs

```typescript
const todos = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/content ?content]
   [(re-find #"TODO" ?content)]]
`)
```

### Blocks with Tag

```typescript
const blocks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/refs ?r]
   [?r :block/name "tag-name"]]
`)
```

### All Pages

```typescript
const pages = await logseq.DB.datascriptQuery(`
  [:find (pull ?p [*])
   :where
   [?p :block/name]]
`)
```

## TypeScript Types

```typescript
import '@logseq/libs'
import type {
  BlockEntity,
  PageEntity,
  LSPluginUserEvents,
} from '@logseq/libs/dist/LSPlugin.user'

interface BlockEntity {
  uuid: string
  content: string
  properties?: Record<string, any>
  page: { id: number }
  parent: { id: number }
  children?: BlockEntity[]
}

interface PageEntity {
  uuid: string
  name: string
  originalName: string
  properties?: Record<string, any>
}
```

## Status Indicators

```typescript
'success' // ✓ Green
'info' // ℹ Blue
'warning' // ⚠ Yellow
'error' // ✗ Red
```

## File Structure

```
my-plugin/
├── src/
│   └── index.ts
├── dist/
│   └── index.js
├── package.json
└── tsconfig.json
```

## package.json Template

```json
{
  "name": "my-plugin",
  "version": "0.0.1",
  "main": "dist/index.js",
  "logseq": {
    "id": "my-plugin",
    "title": "My Plugin",
    "icon": "🎉"
  }
}
```

## Useful Console Commands

```typescript
// List all plugin APIs
console.log(Object.keys(logseq))

// Get current graph info
const graph = await logseq.App.getCurrentGraph()
console.log(graph)

// Debug block structure
const block = await logseq.Editor.getCurrentBlock()
console.log(JSON.stringify(block, null, 2))
```

## Event Listeners

```typescript
// Settings changed
logseq.on('settings:changed', (settings) => {
  console.log('Settings:', settings)
})

// UI visibility changed
logseq.on('ui:visible:changed', ({ visible }) => {
  console.log('Visible:', visible)
})

// Before unload
logseq.beforeunload(async () => {
  // Cleanup
})
```

## Tips

✅ **Do:**

- Check for `null` returns
- Use try-catch for async operations
- Test incrementally
- Log errors to console

❌ **Don't:**

- Forget to import `@logseq/libs`
- Modify blocks without reading first
- Block the main thread
- Ignore user feedback

## Resources

- 📚 [Full Documentation](README.md)
- 🚀 [Quick Start](01-quick-start.md)
- 💡 [Examples](17-examples.md)
- 🔗 [API Reference](../index.html)

---

**Keep this handy for quick reference!**
