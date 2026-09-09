# Logseq Plugin Development Guide: TypeScript Edition

> **Last revised:** 2026-05-29

A comprehensive, practical guide for building Logseq plugins with TypeScript.

## 📚 Table of Contents

1. [Quick Start](#quick-start)
2. [Project Setup](#project-setup)
3. [TypeScript Configuration](#typescript-configuration)
4. [Development Workflow](#development-workflow)
5. [Plugin Architecture Patterns](#plugin-architecture-patterns)
6. [API Usage Guide](#api-usage-guide)
7. [Findings: New Capabilities](#findings-new-capabilities)
8. [UI Development](#ui-development)
9. [State Management](#state-management)
10. [Testing](#testing)
11. [Publishing](#publishing)
12. [Best Practices](#best-practices)
13. [Common Recipes](#common-recipes)
14. [Troubleshooting](#troubleshooting)
15. [Resources](#resources)

---

## Quick Start

### Prerequisites

- Node.js 18+ and npm/yarn
- Basic TypeScript knowledge
- Logseq installed (desktop or web)

### 5-Minute Plugin

Create a minimal "Hello World" plugin:

```bash
# Create project
mkdir logseq-plugin-hello
cd logseq-plugin-hello

# Initialize
npm init -y
npm install --save-dev typescript @logseq/libs @types/node
npm install --save-dev esbuild  # or webpack

# Create TypeScript config
cat > tsconfig.json << 'EOF'
{
  "compilerOptions": {
    "target": "ES2017",
    "module": "ESNext",
    "lib": ["ES2017", "DOM"],
    "moduleResolution": "node",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "outDir": "./dist"
  },
  "include": ["src/**/*"]
}
EOF

# Create source directory
mkdir src

# Create main plugin file
cat > src/index.ts << 'EOF'
import '@logseq/libs'

async function main() {
  console.log('Hello from plugin!')

  // Register a command
  logseq.App.registerCommandPalette({
    key: 'hello-world',
    label: 'Say Hello',
  }, () => {
    logseq.UI.showMsg('Hello World! 👋', 'success')
  })
}

// Entry point
logseq.ready(main).catch(console.error)
EOF

# Create package.json for plugin
cat > package.json << 'EOF'
{
  "name": "logseq-plugin-hello",
  "version": "0.0.1",
  "main": "dist/index.html",
  "logseq": {
    "id": "logseq-plugin-hello",
    "title": "Hello Plugin",
    "icon": "🎉"
  },
  "scripts": {
    "build": "tsc && esbuild src/index.ts --bundle --outfile=dist/index.js",
    "dev": "npm run build -- --watch"
  },
  "devDependencies": {
    "@logseq/libs": "latest",
    "@types/node": "latest",
    "esbuild": "latest",
    "typescript": "latest"
  }
}
EOF

# Create HTML entry point
mkdir -p dist
cat > dist/index.html << 'EOF'
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body>
  <div id="app"></div>
  <script src="https://cdn.jsdelivr.net/npm/@logseq/libs/dist/lsplugin.user.min.js"></script>
  <script src="./index.js"></script>
</body>
</html>
EOF

# Build
npm install
npm run build

# Load in Logseq
# 1. Open Logseq
# 2. Go to Settings → Plugins → Load unpacked plugin
# 3. Select this directory
```

**Test it**: Press `Cmd/Ctrl + K` to open command palette, search "Say Hello", and run it!

---

## Project Setup

### Recommended Project Structure

```
my-logseq-plugin/
├── src/
│   ├── index.ts              # Entry point
│   ├── types.ts              # Type definitions
│   ├── utils.ts              # Utility functions
│   ├── commands/             # Command handlers
│   │   ├── index.ts
│   │   └── my-command.ts
│   ├── ui/                   # UI components
│   │   ├── index.ts
│   │   └── main-ui.tsx       # If using React
│   ├── services/             # Business logic
│   │   └── data-service.ts
│   └── constants.ts          # Constants
├── dist/                     # Build output
│   ├── index.html
│   └── index.js
├── assets/                   # Static assets
│   └── icon.png
├── package.json              # Plugin manifest
├── tsconfig.json             # TypeScript config
├── .gitignore
└── README.md
```

### Complete package.json

```json
{
  "name": "logseq-plugin-my-awesome-plugin",
  "version": "1.0.0",
  "description": "An awesome Logseq plugin",
  "main": "dist/index.html",
  "author": "Your Name <your@email.com>",
  "license": "MIT",
  "logseq": {
    "id": "my-awesome-plugin",
    "title": "My Awesome Plugin",
    "icon": "./assets/icon.png",
    "main": "dist/index.html",
    "themes": []
  },
  "keywords": ["logseq", "logseq-plugin"],
  "scripts": {
    "build": "tsc && webpack --mode production",
    "dev": "tsc && webpack --mode development --watch",
    "lint": "eslint src --ext .ts,.tsx",
    "format": "prettier --write \"src/**/*.{ts,tsx}\"",
    "type-check": "tsc --noEmit"
  },
  "dependencies": {},
  "devDependencies": {
    "@logseq/libs": "^0.3.3",
    "@types/node": "^18.0.0",
    "typescript": "^4.9.0",
    "webpack": "^5.75.0",
    "webpack-cli": "^5.0.0",
    "ts-loader": "^9.4.0",
    "eslint": "^8.30.0",
    "@typescript-eslint/parser": "^5.48.0",
    "@typescript-eslint/eslint-plugin": "^5.48.0",
    "prettier": "^2.8.0"
  }
}
```

### .gitignore

```gitignore
node_modules/
dist/
*.log
.DS_Store
.vscode/
.idea/
*.swp
*.swo
*.map
coverage/
.env
.env.local
```

---

## TypeScript Configuration

### Recommended tsconfig.json

```json
{
  "compilerOptions": {
    // Target modern browsers and Node.js
    "target": "ES2017",
    "module": "ESNext",
    "lib": ["ES2017", "DOM", "DOM.Iterable"],
    "moduleResolution": "node",

    // Strict type checking
    "strict": true,
    "noImplicitAny": true,
    "strictNullChecks": true,
    "strictFunctionTypes": true,
    "noImplicitThis": true,
    "alwaysStrict": true,

    // Additional checks
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,

    // Module resolution
    "esModuleInterop": true,
    "allowSyntheticDefaultImports": true,
    "resolveJsonModule": true,
    "skipLibCheck": true,

    // Output
    "outDir": "./dist",
    "rootDir": "./src",
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,

    // JSX (if using React)
    "jsx": "react",
    "jsxFactory": "React.createElement",

    // Paths
    "baseUrl": "./",
    "paths": {
      "@/*": ["src/*"]
    }
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

### Type Definitions

The `@logseq/libs` package includes full TypeScript definitions. Key types:

```typescript
import '@logseq/libs'

// Global types available:
// - BlockEntity
// - PageEntity
// - BlockUUID
// - EntityID
// - AppUserConfigs
// - IHookEvent
// - SettingSchemaDesc
// ... and many more

// Example usage:
async function getBlock(uuid: BlockUUID): Promise<BlockEntity | null> {
  return await logseq.Editor.getBlock(uuid)
}
```

### Custom Type Definitions (types.ts)

```typescript
// src/types.ts
import { BlockEntity, PageEntity, BlockUUID } from '@logseq/libs/dist/LSPlugin'

// Extend existing types
export interface ExtendedBlockEntity extends BlockEntity {
  customProperty?: string
}

// Plugin-specific types
export interface PluginConfig {
  enabled: boolean
  apiKey?: string
  maxItems: number
}

export interface CommandContext {
  block: BlockEntity
  page: PageEntity
}

export type CommandHandler = (ctx: CommandContext) => Promise<void>

// Settings schema types
export interface PluginSettings {
  darkMode: boolean
  notifications: boolean
  customField: string
}

// UI state types
export interface UIState {
  visible: boolean
  loading: boolean
  error: string | null
}
```

---

## Development Workflow

### Build Tools

#### Option 1: esbuild (Fast, Simple)

```javascript
// build.js
const esbuild = require('esbuild')

esbuild
  .build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outfile: 'dist/index.js',
    platform: 'browser',
    target: 'es2017',
    sourcemap: process.env.NODE_ENV === 'development',
    minify: process.env.NODE_ENV === 'production',
    watch: process.argv.includes('--watch'),
  })
  .catch(() => process.exit(1))
```

**package.json scripts:**

```json
{
  "scripts": {
    "build": "tsc && node build.js",
    "dev": "NODE_ENV=development node build.js --watch"
  }
}
```

#### Option 2: webpack (Powerful, Configurable)

```javascript
// webpack.config.js
const path = require('path')

module.exports = {
  entry: './src/index.ts',
  output: {
    filename: 'index.js',
    path: path.resolve(__dirname, 'dist'),
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: ['.tsx', '.ts', '.js'],
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
  },
  devtool: 'source-map',
  mode: process.env.NODE_ENV === 'production' ? 'production' : 'development',
}
```

### Hot Reload Setup

```typescript
// src/index.ts
import '@logseq/libs'

async function main() {
  console.log('Plugin loaded!')

  // Your plugin code
}

// Development: Enable hot reload
if (process.env.NODE_ENV === 'development') {
  console.log('🔥 Development mode')

  // Auto reload on settings change
  logseq.onSettingsChanged(() => {
    console.log('Settings changed, consider reloading')
  })
}

logseq.ready(main).catch(console.error)
```

### Development in Logseq

**Load unpacked plugin:**

1. Logseq Settings → Plugins
2. Click "Load unpacked plugin"
3. Select your plugin directory

**Watch for changes:**

```bash
npm run dev
```

**Reload plugin after changes:**

- In Logseq: Right-click plugin → Reload
- Or manually: Settings → Plugins → Find your plugin → Reload button

### Debugging Setup

```typescript
// src/utils/logger.ts
import debug from 'debug'

export const log = debug('my-plugin')
export const logError = debug('my-plugin:error')
export const logAPI = debug('my-plugin:api')

// Usage:
log('Starting initialization...')
logAPI('Fetching block: %s', uuid)
logError('Failed to save: %O', error)
```

**Enable debugging in Logseq:**

```javascript
// In DevTools Console
localStorage.setItem('debug', 'my-plugin:*')
// Reload Logseq
```

---

## Plugin Architecture Patterns

### 1. Command Pattern

Organize commands as separate modules:

```typescript
// src/commands/index.ts
import { registerHelloCommand } from './hello-command'
import { registerSearchCommand } from './search-command'

export function registerAllCommands() {
  registerHelloCommand()
  registerSearchCommand()
}

// src/commands/hello-command.ts
export function registerHelloCommand() {
  logseq.App.registerCommandPalette(
    {
      key: 'hello',
      label: 'Say Hello',
      keybinding: {
        mode: 'global',
        binding: 'mod+shift+h',
      },
    },
    async () => {
      const block = await logseq.Editor.getCurrentBlock()
      if (block) {
        await logseq.Editor.insertBlock(
          block.uuid,
          `Hello from ${block.uuid}!`,
          { sibling: true }
        )
      }
    }
  )
}
```

### 2. Service Pattern

Separate business logic from UI:

```typescript
// src/services/block-service.ts
import { BlockEntity, BlockUUID } from '@logseq/libs/dist/LSPlugin'

export class BlockService {
  async getBlockWithChildren(uuid: BlockUUID): Promise<BlockEntity | null> {
    return await logseq.Editor.getBlock(uuid, { includeChildren: true })
  }

  async findBlocksByTag(tag: string): Promise<BlockEntity[]> {
    const results = await logseq.DB.q(`
      [:find (pull ?b [*])
       :where
       [?b :block/refs ?r]
       [?r :block/name "${tag.toLowerCase()}"]]
    `)
    return results?.map((r: any) => r[0]) || []
  }

  async duplicateBlock(uuid: BlockUUID): Promise<void> {
    const block = await this.getBlockWithChildren(uuid)
    if (!block) return

    const sibling = await logseq.Editor.insertBlock(uuid, block.content, {
      sibling: true,
      before: false,
    })

    if (sibling && block.children) {
      await this.duplicateChildren(block.children, sibling.uuid)
    }
  }

  private async duplicateChildren(
    children: any[],
    parentUuid: BlockUUID
  ): Promise<void> {
    for (const child of children) {
      if ('uuid' in child) continue // Skip references

      const newChild = await logseq.Editor.insertBlock(
        parentUuid,
        child.content,
        { sibling: false }
      )

      if (newChild && child.children) {
        await this.duplicateChildren(child.children, newChild.uuid)
      }
    }
  }
}

// Export singleton
export const blockService = new BlockService()
```

### 3. Hook Pattern

Centralize event handling:

```typescript
// src/hooks/index.ts
export class HookManager {
  private unsubscribers: Array<() => void> = []

  setupHooks() {
    // DB changes
    this.register(
      logseq.DB.onChanged(({ blocks, txData, txMeta }) => {
        console.log('DB changed:', blocks.length, 'blocks')
      })
    )

    // Graph changes
    this.register(
      logseq.App.onCurrentGraphChanged(async () => {
        console.log('Graph changed')
        await this.reinitialize()
      })
    )

    // Block-specific hook
    this.register(
      logseq.DB.onBlockChanged('block-uuid-here', (block) => {
        console.log('Block changed:', block)
      })
    )
  }

  private register(unsub: () => void) {
    this.unsubscribers.push(unsub)
  }

  async reinitialize() {
    // Re-fetch data, reset state, etc.
  }

  cleanup() {
    this.unsubscribers.forEach((unsub) => unsub())
    this.unsubscribers = []
  }
}
```

### 4. Settings Pattern

Type-safe settings management:

```typescript
// src/settings.ts
import { SettingSchemaDesc } from '@logseq/libs/dist/LSPlugin'

export interface PluginSettings {
  apiKey: string
  maxResults: number
  enableNotifications: boolean
  theme: 'light' | 'dark' | 'auto'
}

export const settingsSchema: SettingSchemaDesc[] = [
  {
    key: 'apiKey',
    type: 'string',
    default: '',
    title: 'API Key',
    description: 'Your API key for external service',
  },
  {
    key: 'maxResults',
    type: 'number',
    default: 10,
    title: 'Max Results',
    description: 'Maximum number of results to show',
  },
  {
    key: 'enableNotifications',
    type: 'boolean',
    default: true,
    title: 'Enable Notifications',
    description: 'Show toast notifications',
  },
  {
    key: 'theme',
    type: 'enum',
    default: 'auto',
    title: 'Theme',
    description: 'Plugin theme',
    enumChoices: ['light', 'dark', 'auto'],
    enumPicker: 'radio',
  },
]

export function getSettings(): PluginSettings {
  return logseq.settings as PluginSettings
}

export function setupSettings() {
  logseq.useSettingsSchema(settingsSchema)

  // Listen to changes
  logseq.onSettingsChanged(
    (newSettings: PluginSettings, oldSettings: PluginSettings) => {
      console.log('Settings changed from', oldSettings, 'to', newSettings)

      // React to specific changes
      if (newSettings.theme !== oldSettings.theme) {
        applyTheme(newSettings.theme)
      }
    }
  )
}

function applyTheme(theme: string) {
  // Apply theme logic
}
```

### 5. Main Plugin Class Pattern

Organize everything in a class:

```typescript
// src/plugin.ts
import { BlockEntity } from '@logseq/libs/dist/LSPlugin'
import { BlockService } from './services/block-service'
import { HookManager } from './hooks'
import { setupSettings, getSettings } from './settings'

export class MyPlugin {
  private blockService: BlockService
  private hookManager: HookManager
  private initialized = false

  constructor() {
    this.blockService = new BlockService()
    this.hookManager = new HookManager()
  }

  async initialize() {
    if (this.initialized) return

    console.log('Initializing plugin...')

    // Setup settings
    setupSettings()

    // Setup hooks
    this.hookManager.setupHooks()

    // Register commands
    this.registerCommands()

    // Setup UI
    this.setupUI()

    this.initialized = true
    console.log('Plugin initialized!')
  }

  private registerCommands() {
    logseq.App.registerCommandPalette(
      {
        key: 'duplicate-block',
        label: 'Duplicate Block',
      },
      async () => {
        const block = await logseq.Editor.getCurrentBlock()
        if (block) {
          await this.blockService.duplicateBlock(block.uuid)
          logseq.UI.showMsg('Block duplicated!', 'success')
        }
      }
    )
  }

  private setupUI() {
    // Provide model for UI interactions
    logseq.provideModel({
      handleButtonClick: async (e: any) => {
        console.log('Button clicked', e)
      },
    })

    // Provide main UI
    logseq.provideUI({
      key: 'main-ui',
      path: '#app-container',
      template: `
        <div class="my-plugin-ui">
          <button data-on-click="handleButtonClick">Click Me</button>
        </div>
      `,
    })
  }

  async cleanup() {
    console.log('Cleaning up plugin...')
    this.hookManager.cleanup()
    this.initialized = false
  }
}

// src/index.ts
import '@logseq/libs'
import { MyPlugin } from './plugin'

let plugin: MyPlugin | null = null

async function main() {
  plugin = new MyPlugin()
  await plugin.initialize()
}

// Cleanup on unload
logseq.beforeunload(async () => {
  if (plugin) {
    await plugin.cleanup()
  }
})

logseq.ready(main).catch(console.error)
```

---

## API Usage Guide

### App API

```typescript
// Get app info
const info = await logseq.App.getInfo()
console.log('Logseq version:', info.version)

// Get user configs
const configs = await logseq.App.getUserConfigs()
console.log('Preferred format:', configs.preferredFormat)

// Get current graph
const graph = await logseq.App.getCurrentGraph()
if (graph) {
  console.log('Current graph:', graph.name, graph.path)
}

// Navigation
logseq.App.pushState('page', { name: 'My Page' })

// External commands
await logseq.App.invokeExternalCommand('logseq.go/home')
```

### Editor API - Blocks

```typescript
// Get current block
const currentBlock = await logseq.Editor.getCurrentBlock()

// Get block by UUID
const block = await logseq.Editor.getBlock('block-uuid', {
  includeChildren: true,
})

// Insert block
const newBlock = await logseq.Editor.insertBlock(
  currentBlock.uuid,
  'New content',
  {
    before: false, // Insert after (true = before)
    sibling: true, // As sibling (false = as child)
    focus: true, // Focus after insert
    customUUID: 'custom-uuid',
    properties: { 'my-prop': 'value' },
  }
)

// Batch insert
const blocks = await logseq.Editor.insertBatchBlock(
  currentBlock.uuid,
  [
    { content: 'Block 1' },
    {
      content: 'Block 2',
      children: [{ content: 'Child 1' }, { content: 'Child 2' }],
    },
  ],
  { sibling: true }
)

// Update block
await logseq.Editor.updateBlock(block.uuid, 'Updated content', {
  properties: { status: 'done' },
})

// Remove block
await logseq.Editor.removeBlock(block.uuid)

// Move block
await logseq.Editor.moveBlock(sourceBlockUuid, targetBlockUuid, {
  before: true,
  children: false,
})
```

### Editor API - Pages

```typescript
// Get current page
const currentPage = await logseq.Editor.getCurrentPage()

// Get page by name
const page = await logseq.Editor.getPage('Page Name')

// Create page
const newPage = await logseq.Editor.createPage(
  'New Page',
  { tags: ['#tag1', '#tag2'] },
  {
    redirect: true, // Navigate to page
    createFirstBlock: true,
    format: 'markdown',
    journal: false,
  }
)

// Get all pages
const allPages = await logseq.Editor.getAllPages()

// Delete page
await logseq.Editor.deletePage('Page Name')

// Rename page
await logseq.Editor.renamePage('Old Name', 'New Name')

// Append/prepend blocks to page
await logseq.Editor.appendBlockInPage('Page Name', 'Content at end')

await logseq.Editor.prependBlockInPage('Page Name', 'Content at start')
```

### DB API - Queries

```typescript
// DSL Query
const todoBlocks = await logseq.DB.q(`
  [:find (pull ?b [*])
   :where
   [?b :block/marker ?m]
   [(contains? #{"TODO" "DOING"} ?m)]]
`)

// Datascript Query
const pageNames = await logseq.DB.datascriptQuery<string[][]>(`
  [:find ?name
   :where [?p :block/name ?name]]
`)

// Process results
todoBlocks?.forEach((result: any) => {
  const block = result[0] as BlockEntity
  console.log(block.content, block.marker)
})

// Query with parameters
const blocksWithTag = await logseq.DB.q(
  `
  [:find (pull ?b [*])
   :in $ ?tag
   :where
   [?b :block/refs ?r]
   [?r :block/name ?tag]]
`,
  'my-tag'
)
```

### DB API - Hooks

```typescript
// Global DB changes
const unsubscribe = logseq.DB.onChanged(({ blocks, txData, txMeta }) => {
  console.log(`${blocks.length} blocks changed`)
  console.log('Operation:', txMeta?.outlinerOp)

  blocks.forEach((block) => {
    console.log('Changed block:', block.content)
  })
})

// Block-specific changes
const unsubBlock = logseq.DB.onBlockChanged(
  'specific-block-uuid',
  (block, txData, txMeta) => {
    console.log('Block updated:', block)
  }
)

// Cleanup
unsubscribe()
unsubBlock()
```

### UI API

```typescript
// Show message
const msgKey = await logseq.UI.showMsg(
  'Operation successful!',
  'success', // 'success' | 'warning' | 'error'
  {
    key: 'unique-msg-key',
    timeout: 3000, // 0 = stay forever
  }
)

// Close message
logseq.UI.closeMsg(msgKey)

// Query elements
const rect = await logseq.UI.queryElementRect('#my-element')
console.log('Element bounds:', rect)

// Get theme colors
const colors = await logseq.UI.resolveThemeCssPropsVals([
  '--ls-primary-background-color',
  '--ls-secondary-background-color',
])
console.log('Primary bg:', colors['--ls-primary-background-color'])
```

### Assets API

```typescript
// List files in current graph
const files = await logseq.Assets.listFilesOfCurrentGraph(['pdf', 'png'])
files.forEach((file) => {
  console.log(file.path, file.size, file.modifiedTime)
})

// Create sandbox storage (IndexedDB in browser, files in desktop)
const storage = logseq.Assets.makeSandboxStorage()
await storage.setItem('my-key', 'my-value')
const value = await storage.getItem('my-key')
await storage.removeItem('my-key')

// Make asset URL
const url = await logseq.Assets.makeUrl('images/logo.png')
console.log('Asset URL:', url)

// Open an asset with Logseq's built-in handlers
await logseq.Assets.builtInOpen('assets/manual.pdf')
```

## Findings: New Capabilities

Validated against current SDK source in `libs/src/LSPlugin.ts` and `libs/src/LSPlugin.user.ts` (2026-05-29).

### 1. Unified command bus (`logseq.Commands`)

You can register and execute commands through `logseq.Commands` in addition to legacy registration APIs:

```typescript
const unregister = logseq.Commands.register(
  'duplicate-block',
  {
    label: 'Duplicate Block',
    placements: ['palette', 'shortcut'],
    keybinding: 'mod+shift+d',
  },
  async () => {
    const block = await logseq.Editor.getCurrentBlock()
    if (!block) return
    await logseq.Editor.insertBlock(block.uuid, block.title, { sibling: true })
  }
)

await logseq.Commands.execute('duplicate-block')

// Cleanup when needed
unregister?.()
```

### 2. DB graph tag/property APIs

For DB graphs, `Editor` now includes first-class tag/class and property APIs:

```typescript
if (await logseq.App.checkCurrentIsDbGraph()) {
  const allTags = await logseq.Editor.getAllTags()
  const allProperties = await logseq.Editor.getAllProperties()

  const taskTag = await logseq.Editor.getTag('logseq.class/Task')

  if (taskTag) {
    await logseq.Editor.addTagProperty(taskTag.uuid, 'priority')
  }

  const block = await logseq.Editor.getCurrentBlock()
  if (block && taskTag) {
    await logseq.Editor.addBlockTag(block.uuid, taskTag.uuid)
  }
}
```

### 3. Top-level namespaces worth using

- `logseq.Net` for network helpers.
- `logseq.Experiments` for unstable/experimental extension points.

Use `Experiments` only when no stable API exists.

---

## UI Development

### Approach 1: Vanilla JavaScript

```typescript
// src/ui/main-ui.ts
export function createMainUI() {
  logseq.provideModel({
    show() {
      logseq.showMainUI()
    },
    hide() {
      logseq.hideMainUI()
    },
    async handleSearch(e: any) {
      const query = e.dataset.query
      const results = await searchBlocks(query)
      updateResults(results)
    },
  })

  logseq.provideUI({
    key: 'main-ui',
    path: '#app',
    template: `
      <div class="plugin-container" data-on-click="show">
        <div class="search-box">
          <input
            type="text"
            placeholder="Search..."
            data-on-input="handleSearch"
            data-query=""
          />
          <div id="results"></div>
        </div>
        <button data-on-click="hide">Close</button>
      </div>
    `,
    style: {
      position: 'fixed',
      top: '50%',
      left: '50%',
      transform: 'translate(-50%, -50%)',
      width: '600px',
      backgroundColor: 'var(--ls-primary-background-color)',
      padding: '20px',
      borderRadius: '8px',
      boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
    },
  })
}

async function searchBlocks(query: string) {
  return await logseq.DB.q(`
    [:find (pull ?b [*])
     :where
     [?b :block/content ?content]
     [(clojure.string/includes? ?content "${query}")]]
  `)
}

function updateResults(results: any[]) {
  const container = parent.document.querySelector('#results')
  if (!container) return

  container.innerHTML = results
    .map((r) => {
      const block = r[0]
      return `<div class="result">${block.content}</div>`
    })
    .join('')
}
```

### Approach 2: React

```bash
npm install react react-dom @types/react @types/react-dom
```

```typescript
// src/ui/App.tsx
import React, { useState, useEffect } from 'react'
import { BlockEntity } from '@logseq/libs/dist/LSPlugin'

export const App: React.FC = () => {
  const [blocks, setBlocks] = useState<BlockEntity[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    loadBlocks()
  }, [])

  async function loadBlocks() {
    setLoading(true)
    try {
      const results = await logseq.DB.q(`
        [:find (pull ?b [*])
         :where [?b :block/marker "TODO"]]
      `)
      setBlocks(results?.map((r: any) => r[0]) || [])
    } finally {
      setLoading(false)
    }
  }

  async function handleComplete(uuid: string) {
    const block = blocks.find((b) => b.uuid === uuid)
    if (!block) return

    await logseq.Editor.updateBlock(uuid, block.content.replace('TODO', 'DONE'))
    await loadBlocks()
  }

  return (
    <div className="app">
      <h2>Todo List</h2>
      {loading ? (
        <div>Loading...</div>
      ) : (
        <ul>
          {blocks.map((block) => (
            <li key={block.uuid}>
              {block.content}
              <button onClick={() => handleComplete(block.uuid)}>
                Complete
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

// src/ui/index.tsx
import React from 'react'
import ReactDOM from 'react-dom'
import { App } from './App'

export function renderApp(container: HTMLElement) {
  ReactDOM.render(<App />, container)
}

// src/index.ts
import { renderApp } from './ui'

logseq
  .ready(() => {
    // Create container
    const container = document.createElement('div')
    container.id = 'app-root'
    document.body.appendChild(container)

    // Render React app
    renderApp(container)
  })
  .catch(console.error)
```

### CSS Styling

```typescript
// Inject global styles
logseq.provideStyle(`
  .my-plugin-container {
    font-family: var(--ls-font-family);
    color: var(--ls-primary-text-color);
    background: var(--ls-primary-background-color);
  }

  .my-plugin-button {
    padding: 8px 16px;
    background: var(--ls-active-primary-color);
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
  }

  .my-plugin-button:hover {
    opacity: 0.9;
  }
`)

// Or load from file
logseq.provideStyle({
  key: 'main-styles',
  style: await fetch(logseq.resolveResourceFullUrl('styles.css')).then((r) =>
    r.text()
  ),
})
```

---

## State Management

### Local State Pattern

```typescript
// src/state.ts
import { BlockEntity } from '@logseq/libs/dist/LSPlugin'

interface AppState {
  blocks: BlockEntity[]
  selectedBlock: BlockEntity | null
  loading: boolean
  error: string | null
}

class StateManager {
  private state: AppState = {
    blocks: [],
    selectedBlock: null,
    loading: false,
    error: null,
  }

  private listeners: Set<(state: AppState) => void> = new Set()

  getState(): Readonly<AppState> {
    return { ...this.state }
  }

  setState(partial: Partial<AppState>) {
    this.state = { ...this.state, ...partial }
    this.notify()
  }

  subscribe(listener: (state: AppState) => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  private notify() {
    this.listeners.forEach((listener) => listener(this.getState()))
  }
}

export const stateManager = new StateManager()

// Usage:
stateManager.setState({ loading: true })
const unsub = stateManager.subscribe((state) => {
  console.log('State changed:', state)
})
```

### Persistent State (Plugin Storage)

```typescript
// src/storage.ts
export class PluginStorage<T extends Record<string, any>> {
  private storage = logseq.Assets.makeSandboxStorage()
  private cache: T | null = null

  constructor(
    private key: string,
    private defaults: T
  ) {}

  async load(): Promise<T> {
    if (this.cache) return this.cache

    try {
      const json = await this.storage.getItem(this.key)
      this.cache = json ? JSON.parse(json) : this.defaults
    } catch (error) {
      console.error('Failed to load storage:', error)
      this.cache = this.defaults
    }

    return this.cache
  }

  async save(data: Partial<T>): Promise<void> {
    const current = await this.load()
    this.cache = { ...current, ...data }

    try {
      await this.storage.setItem(this.key, JSON.stringify(this.cache))
    } catch (error) {
      console.error('Failed to save storage:', error)
      throw error
    }
  }

  async get<K extends keyof T>(key: K): Promise<T[K]> {
    const data = await this.load()
    return data[key]
  }

  async set<K extends keyof T>(key: K, value: T[K]): Promise<void> {
    await this.save({ [key]: value } as Partial<T>)
  }

  async clear(): Promise<void> {
    this.cache = null
    await this.storage.removeItem(this.key)
  }
}

// Usage:
interface MyData {
  lastSync: number
  favorites: string[]
}

const storage = new PluginStorage<MyData>('my-plugin-data', {
  lastSync: 0,
  favorites: [],
})

await storage.save({ lastSync: Date.now() })
const lastSync = await storage.get('lastSync')
```

---

## Testing

### Unit Tests with Jest

```bash
npm install --save-dev jest @types/jest ts-jest @testing-library/jest-dom
```

```javascript
// jest.config.js
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'jsdom',
  roots: ['<rootDir>/src'],
  testMatch: ['**/__tests__/**/*.ts', '**/*.test.ts'],
  moduleNameMapper: {
    '^@/(.*)$': '<rootDir>/src/$1',
  },
  globals: {
    'ts-jest': {
      tsconfig: {
        jsx: 'react',
      },
    },
  },
}
```

```typescript
// src/services/__tests__/block-service.test.ts
import { BlockService } from '../block-service'

// Mock logseq API
global.logseq = {
  Editor: {
    getBlock: jest.fn(),
    insertBlock: jest.fn(),
  },
  DB: {
    q: jest.fn(),
  },
} as any

describe('BlockService', () => {
  let service: BlockService

  beforeEach(() => {
    service = new BlockService()
    jest.clearAllMocks()
  })

  describe('findBlocksByTag', () => {
    it('should query blocks with tag', async () => {
      const mockResults = [[{ uuid: '123', content: 'Test' }]]
      ;(logseq.DB.q as jest.Mock).mockResolvedValue(mockResults)

      const blocks = await service.findBlocksByTag('test')

      expect(logseq.DB.q).toHaveBeenCalledWith(expect.stringContaining('test'))
      expect(blocks).toHaveLength(1)
      expect(blocks[0].content).toBe('Test')
    })

    it('should return empty array when no results', async () => {
      ;(logseq.DB.q as jest.Mock).mockResolvedValue(null)

      const blocks = await service.findBlocksByTag('test')

      expect(blocks).toEqual([])
    })
  })
})
```

### Integration Tests

```typescript
// src/__tests__/integration.test.ts
import '@logseq/libs'

describe('Plugin Integration', () => {
  beforeAll(async () => {
    // Setup test environment
    await logseq.ready()
  })

  it('should register commands', async () => {
    // Test command registration
    const commands = await getRegisteredCommands()
    expect(commands).toContain('my-plugin.hello')
  })

  it('should handle block operations', async () => {
    const block = await logseq.Editor.getCurrentBlock()
    expect(block).toBeDefined()

    const newBlock = await logseq.Editor.insertBlock(
      block.uuid,
      'Test content',
      { sibling: true }
    )
    expect(newBlock).toBeDefined()
    expect(newBlock.content).toBe('Test content')
  })
})
```

---

## Publishing

### Pre-Publish Checklist

```typescript
// scripts/pre-publish.ts
import * as fs from 'fs'
import * as path from 'path'

function validatePlugin() {
  // Check package.json
  const pkg = JSON.parse(fs.readFileSync('package.json', 'utf-8'))

  if (!pkg.logseq?.id) {
    throw new Error('Missing logseq.id in package.json')
  }

  if (!pkg.version) {
    throw new Error('Missing version in package.json')
  }

  // Check build output
  if (!fs.existsSync('dist/index.html')) {
    throw new Error('Missing dist/index.html')
  }

  if (!fs.existsSync('dist/index.js')) {
    throw new Error('Missing dist/index.js')
  }

  // Check README
  if (!fs.existsSync('README.md')) {
    console.warn('Warning: Missing README.md')
  }

  console.log('✅ Plugin validation passed!')
}

validatePlugin()
```

### GitHub Release Workflow

```yaml
# .github/workflows/release.yml
name: Release Plugin

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18'

      - name: Install dependencies
        run: npm ci

      - name: Build
        run: npm run build

      - name: Create Release Archive
        run: |
          cd dist
          zip -r ../plugin.zip .

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v1
        with:
          files: plugin.zip
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### Marketplace Submission

1. **Create Release on GitHub**
   - Tag: `v1.0.0`
   - Attach: `plugin.zip` containing dist files

2. **Submit to Logseq Marketplace**
   - Fork: https://github.com/logseq/marketplace
   - Edit: `plugins.json`
   - Add your plugin:

```json
{
  "id": "my-awesome-plugin",
  "title": "My Awesome Plugin",
  "description": "Does awesome things",
  "author": "Your Name",
  "repo": "username/logseq-plugin-my-awesome",
  "icon": "./icon.png",
  "theme": false,
  "effect": false
}
```

3. **Create Pull Request**
   - Title: "Add: My Awesome Plugin"
   - Wait for review and merge

---

## Best Practices

### 1. Error Handling

```typescript
// Always wrap async operations
async function safeOperation() {
  try {
    const block = await logseq.Editor.getCurrentBlock()
    if (!block) {
      logseq.UI.showMsg('No block selected', 'warning')
      return
    }

    // Do something
  } catch (error) {
    console.error('Operation failed:', error)
    logseq.UI.showMsg(
      `Error: ${error instanceof Error ? error.message : 'Unknown error'}`,
      'error'
    )
  }
}

// Global error handler
window.addEventListener('unhandledrejection', (event) => {
  console.error('Unhandled promise rejection:', event.reason)
  logseq.UI.showMsg('An unexpected error occurred', 'error')
})
```

### 2. Performance

```typescript
// Batch operations
async function updateMultipleBlocks(
  updates: Array<{ uuid: string; content: string }>
) {
  // Good: Parallel execution
  await Promise.all(
    updates.map((u) => logseq.Editor.updateBlock(u.uuid, u.content))
  )

  // Bad: Sequential execution
  // for (const u of updates) {
  //   await logseq.Editor.updateBlock(u.uuid, u.content)
  // }
}

// Cache expensive queries
const cache = new Map<string, any>()

async function getPageBlocksCached(pageName: string) {
  if (cache.has(pageName)) {
    return cache.get(pageName)
  }

  const blocks = await logseq.Editor.getPageBlocksTree(pageName)
  cache.set(pageName, blocks)

  // Invalidate cache on changes
  logseq.DB.onChanged(() => cache.clear())

  return blocks
}

// Debounce frequent operations
function debounce<T extends (...args: any[]) => any>(
  fn: T,
  delay: number
): (...args: Parameters<T>) => void {
  let timeoutId: NodeJS.Timeout
  return (...args) => {
    clearTimeout(timeoutId)
    timeoutId = setTimeout(() => fn(...args), delay)
  }
}

const handleInput = debounce(async (query: string) => {
  const results = await searchBlocks(query)
  updateUI(results)
}, 300)
```

### 3. User Experience

```typescript
// Show progress for long operations
async function longOperation() {
  const msgKey = await logseq.UI.showMsg('Processing...', 'info', {
    timeout: 0,
  })

  try {
    await doWork()
    logseq.UI.closeMsg(msgKey)
    logseq.UI.showMsg('Done!', 'success')
  } catch (error) {
    logseq.UI.closeMsg(msgKey)
    logseq.UI.showMsg('Failed!', 'error')
  }
}

// Keyboard shortcuts
logseq.App.registerCommandShortcut(
  { mode: 'global', binding: 'mod+shift+p' },
  () => {
    // Your action
  }
)

// Graceful degradation
async function getBlockSafely(uuid: string) {
  try {
    return await logseq.Editor.getBlock(uuid)
  } catch (error) {
    console.warn('Block not found, using fallback')
    return null
  }
}
```

### 4. Security

```typescript
// Sanitize user input
import DOMPurify from 'dompurify'

function renderUserContent(html: string) {
  return DOMPurify.sanitize(html)
}

// Validate block UUIDs
function isValidUUID(uuid: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
    uuid
  )
}

// Don't expose secrets in settings
const settingsSchema: SettingSchemaDesc[] = [
  {
    key: 'apiKey',
    type: 'string',
    default: '',
    title: 'API Key',
    description: '⚠️ This will be stored in plain text',
    inputAs: 'password', // Use password input (still not encrypted!)
  },
]
```

### 5. Documentation

````typescript
/**
 * Duplicates a block and all its children
 * @param uuid - The UUID of the block to duplicate
 * @returns The UUID of the duplicated block
 * @throws {Error} If the block doesn't exist
 * @example
 * ```typescript
 * const newUuid = await duplicateBlock('existing-uuid')
 * ```
 */
async function duplicateBlock(uuid: BlockUUID): Promise<BlockUUID> {
  // Implementation
}
````

---

## Common Recipes

### 1. Todo List Manager

```typescript
interface Todo {
  uuid: BlockUUID
  content: string
  marker: string
  priority: number
}

class TodoManager {
  async getAllTodos(): Promise<Todo[]> {
    const results = await logseq.DB.q(`
      [:find (pull ?b [*])
       :where
       [?b :block/marker ?m]
       [(contains? #{"TODO" "DOING" "LATER" "NOW"} ?m)]]
    `)

    return (
      results?.map((r: any) => ({
        uuid: r[0].uuid,
        content: r[0].content,
        marker: r[0].marker,
        priority: this.getPriority(r[0].content),
      })) || []
    )
  }

  async completeTodo(uuid: BlockUUID) {
    const block = await logseq.Editor.getBlock(uuid)
    if (!block) return

    const content = block.content.replace(/^(TODO|DOING|NOW|LATER)/, 'DONE')
    await logseq.Editor.updateBlock(uuid, content)
  }

  private getPriority(content: string): number {
    if (content.includes('[#A]')) return 1
    if (content.includes('[#B]')) return 2
    if (content.includes('[#C]')) return 3
    return 4
  }
}
```

### 2. Block Template System

```typescript
interface Template {
  name: string
  content: string
  children?: Template[]
}

class TemplateManager {
  private templates: Map<string, Template> = new Map()

  registerTemplate(template: Template) {
    this.templates.set(template.name, template)
  }

  async applyTemplate(templateName: string, targetUuid: BlockUUID) {
    const template = this.templates.get(templateName)
    if (!template) {
      throw new Error(`Template not found: ${templateName}`)
    }

    return await this.insertTemplate(template, targetUuid)
  }

  private async insertTemplate(
    template: Template,
    parentUuid: BlockUUID
  ): Promise<BlockUUID> {
    // Insert main block
    const block = await logseq.Editor.insertBlock(
      parentUuid,
      this.processContent(template.content),
      { sibling: false }
    )

    if (!block) throw new Error('Failed to insert block')

    // Insert children
    if (template.children) {
      for (const child of template.children) {
        await this.insertTemplate(child, block.uuid)
      }
    }

    return block.uuid
  }

  private processContent(content: string): string {
    // Replace variables
    return content
      .replace(/\{\{date\}\}/g, new Date().toLocaleDateString())
      .replace(/\{\{time\}\}/g, new Date().toLocaleTimeString())
  }
}

// Usage:
const templates = new TemplateManager()

templates.registerTemplate({
  name: 'meeting-notes',
  content: '## Meeting Notes - {{date}}',
  children: [
    { name: 'attendees', content: '**Attendees:**' },
    { name: 'agenda', content: '**Agenda:**' },
    { name: 'notes', content: '**Notes:**' },
    { name: 'action-items', content: '**Action Items:**' },
  ],
})

// Apply template
logseq.Editor.registerSlashCommand('Meeting Template', async () => {
  const block = await logseq.Editor.getCurrentBlock()
  if (block) {
    await templates.applyTemplate('meeting-notes', block.uuid)
  }
})
```

### 3. Block Link Graph

```typescript
class BlockLinkGraph {
  async getLinkedBlocks(uuid: BlockUUID): Promise<BlockEntity[]> {
    const results = await logseq.DB.q(
      `
      [:find (pull ?b [*])
       :in $ ?uuid
       :where
       [?target :block/uuid ?uuid]
       [?b :block/refs ?target]]
    `,
      uuid
    )

    return results?.map((r: any) => r[0]) || []
  }

  async getBacklinks(uuid: BlockUUID): Promise<Map<string, BlockEntity[]>> {
    const linked = await this.getLinkedBlocks(uuid)

    const byPage = new Map<string, BlockEntity[]>()

    for (const block of linked) {
      const page = await logseq.Editor.getPage(block.page.id)
      if (!page) continue

      const pageName = page.name || page.originalName
      if (!byPage.has(pageName)) {
        byPage.set(pageName, [])
      }
      byPage.get(pageName)!.push(block)
    }

    return byPage
  }

  async visualizeGraph(uuid: BlockUUID) {
    const backlinks = await this.getBacklinks(uuid)

    logseq.provideUI({
      key: 'link-graph',
      template: this.renderGraph(uuid, backlinks),
    })
  }

  private renderGraph(
    uuid: string,
    backlinks: Map<string, BlockEntity[]>
  ): string {
    let html = '<div class="link-graph">'
    html += `<h3>Links to ${uuid}</h3>`

    for (const [page, blocks] of backlinks) {
      html += `<div class="page-group">`
      html += `<h4>📄 ${page}</h4>`
      html += '<ul>'
      for (const block of blocks) {
        html += `<li>${block.content}</li>`
      }
      html += '</ul></div>'
    }

    html += '</div>'
    return html
  }
}
```

### 4. Export to External Format

```typescript
interface ExportOptions {
  format: 'json' | 'markdown' | 'html'
  includeProperties: boolean
  includeChildren: boolean
}

class BlockExporter {
  async exportPage(pageName: string, options: ExportOptions): Promise<string> {
    const blocks = await logseq.Editor.getPageBlocksTree(pageName)

    switch (options.format) {
      case 'json':
        return JSON.stringify(blocks, null, 2)
      case 'markdown':
        return this.toMarkdown(blocks, options)
      case 'html':
        return this.toHTML(blocks, options)
    }
  }

  private toMarkdown(blocks: BlockEntity[], options: ExportOptions): string {
    return blocks
      .map((block) => this.blockToMarkdown(block, 0, options))
      .join('\n')
  }

  private blockToMarkdown(
    block: BlockEntity,
    level: number,
    options: ExportOptions
  ): string {
    const indent = '  '.repeat(level)
    let md = `${indent}- ${block.content}\n`

    if (options.includeProperties && block.properties) {
      for (const [key, value] of Object.entries(block.properties)) {
        md += `${indent}  ${key}:: ${value}\n`
      }
    }

    if (options.includeChildren && block.children) {
      for (const child of block.children) {
        if ('uuid' in child) {
          md += this.blockToMarkdown(child as BlockEntity, level + 1, options)
        }
      }
    }

    return md
  }

  private toHTML(blocks: BlockEntity[], options: ExportOptions): string {
    let html = '<ul>'
    for (const block of blocks) {
      html += this.blockToHTML(block, options)
    }
    html += '</ul>'
    return html
  }

  private blockToHTML(block: BlockEntity, options: ExportOptions): string {
    let html = `<li>${this.escapeHTML(block.content)}`

    if (options.includeChildren && block.children) {
      html += '<ul>'
      for (const child of block.children) {
        if ('uuid' in child) {
          html += this.blockToHTML(child as BlockEntity, options)
        }
      }
      html += '</ul>'
    }

    html += '</li>'
    return html
  }

  private escapeHTML(text: string): string {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
  }
}
```

---

## Troubleshooting

### Common Issues

#### 1. Plugin Not Loading

**Symptoms**: Plugin doesn't appear in plugin list

**Solutions**:

```typescript
// Check package.json has required fields
{
  "name": "logseq-plugin-...",
  "version": "1.0.0",
  "main": "dist/index.html",
  "logseq": {
    "id": "unique-id",
    "title": "Plugin Title"
  }
}

// Ensure dist/index.html exists and loads SDK
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body>
  <div id="app"></div>
  <script src="https://cdn.jsdelivr.net/npm/@logseq/libs/dist/lsplugin.user.min.js"></script>
  <script src="./index.js"></script>
</body>
</html>

// Check for JavaScript errors in DevTools
```

#### 2. API Calls Timing Out

**Symptoms**: Operations take > 10s and fail

**Solutions**:

```typescript
// Break into smaller chunks
async function processLargeDataset(items: string[]) {
  const BATCH_SIZE = 50

  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    const batch = items.slice(i, i + BATCH_SIZE)
    await Promise.all(batch.map((item) => processItem(item)))

    // Give UI time to breathe
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

// Use streaming approach
async function* streamBlocks(pageUuids: string[]) {
  for (const uuid of pageUuids) {
    const blocks = await logseq.Editor.getPageBlocksTree(uuid)
    yield blocks
  }
}

for await (const blocks of streamBlocks(pages)) {
  processBlocks(blocks)
}
```

#### 3. Settings Not Persisting

**Symptoms**: Settings reset on reload

**Solutions**:

```typescript
// Ensure settings schema is registered before ready()
logseq.useSettingsSchema(settingsSchema)
logseq.ready(main).catch(console.error)

// Not after:
logseq.ready(() => {
  logseq.useSettingsSchema(settingsSchema)  // ❌ Too late
})

// Use correct types in schema
{
  key: 'count',
  type: 'number',  // Not 'int' or 'integer'
  default: 0
}
```

#### 4. UI Not Updating

**Symptoms**: UI doesn't reflect state changes

**Solutions**:

```typescript
// Re-render UI after state changes
function updateUI(data: any) {
  const container = parent.document.querySelector('#my-container')
  if (!container) return

  container.innerHTML = renderData(data)

  // Re-attach event listeners
  container.querySelectorAll('[data-on-click]').forEach((el) => {
    const handler = el.getAttribute('data-on-click')
    el.addEventListener('click', () => logseq.call(handler, el))
  })
}

// Or use framework reactivity (React, Vue, etc.)
```

#### 5. Memory Leaks

**Symptoms**: Logseq slows down over time

**Solutions**:

```typescript
// Clean up hooks on unload
const unsubscribers: Array<() => void> = []

logseq.ready(() => {
  unsubscribers.push(logseq.DB.onChanged(handler))
})

logseq.beforeunload(async () => {
  unsubscribers.forEach((unsub) => unsub())
})

// Clear intervals/timeouts
const intervals: NodeJS.Timeout[] = []

function setupPolling() {
  const id = setInterval(poll, 5000)
  intervals.push(id)
}

logseq.beforeunload(async () => {
  intervals.forEach(clearInterval)
})
```

---

## Resources

### Official Documentation

- [Plugin API Docs](https://plugins-doc.logseq.com/)
- [GitHub Repo](https://github.com/logseq/logseq)
- [Marketplace](https://github.com/logseq/marketplace)

### Example Plugins

- [Official Samples](https://github.com/logseq/logseq-plugin-samples)
- [Community Plugins](https://github.com/topics/logseq-plugin)

### Community

- [Discord](https://discord.gg/logseq)
- [Forum](https://discuss.logseq.com/)
- [Reddit](https://reddit.com/r/logseq)

### Type Definitions

- `@logseq/libs` package includes full TypeScript definitions
- Primary API types are in `node_modules/@logseq/libs/dist/LSPlugin.d.ts`

### Useful Libraries

- **UI Frameworks**: React, Vue, Svelte
- **State Management**: Zustand, Jotai, Valtio
- **Utilities**: Lodash, date-fns, DOMPurify
- **Build Tools**: esbuild, webpack, Vite

---

**Happy Plugin Development! 🚀**

_Questions? Join the Logseq Discord and ask in #plugin-development_

---

> **Removed:** Fixed SDK pin guidance (`@logseq/libs ^0.2.1`) from this guide footer. The in-repo SDK is now `@logseq/libs 0.3.3`; prefer `latest` or a tested minimum version for your plugin.

**Last Updated**: 2026-05-29
**SDK Version (repo source)**: @logseq/libs 0.3.3
**Logseq Version**: Check at runtime via `await logseq.App.getInfo()`
