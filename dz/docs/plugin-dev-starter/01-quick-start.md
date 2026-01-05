# Quick Start Guide

Get your first Logseq plugin running in minutes!

## Prerequisites

- Node.js 14+ installed
- Basic TypeScript/JavaScript knowledge
- Logseq desktop app installed

## Installation

```bash
# Create a new plugin directory
mkdir my-logseq-plugin
cd my-logseq-plugin

# Initialize npm project
npm init -y

# Install Logseq SDK
npm install @logseq/libs

# Install development dependencies
npm install -D typescript @types/node
```

## Project Setup

### 1. Create `tsconfig.json`

```json
{
  "compilerOptions": {
    "target": "ES2017",
    "module": "ESNext",
    "moduleResolution": "node",
    "esModuleInterop": true,
    "strict": true,
    "outDir": "./dist",
    "skipLibCheck": true
  },
  "include": ["src/**/*"]
}
```

### 2. Create `package.json` (Logseq manifest)

```json
{
  "name": "my-logseq-plugin",
  "version": "0.0.1",
  "description": "My first Logseq plugin",
  "main": "dist/index.js",
  "logseq": {
    "id": "my-plugin-id",
    "title": "My Plugin",
    "icon": "🎉"
  },
  "scripts": {
    "build": "tsc",
    "dev": "tsc --watch"
  }
}
```

### 3. Create `src/index.ts`

```typescript
import '@logseq/libs'

const main = async () => {
  console.log('🎉 My plugin loaded!')

  // Show a welcome message
  logseq.UI.showMsg('Welcome to my plugin!', 'success')

  // Register a slash command
  logseq.Editor.registerSlashCommand('Insert Greeting', async () => {
    await logseq.Editor.insertAtEditingCursor('Hello from my plugin! 👋')
  })

  // Register a toolbar button
  logseq.App.registerUIItem('toolbar', {
    key: 'my-plugin-toolbar',
    template: `
      <a data-on-click="showMessage" class="button">
        <i class="ti ti-heart"></i>
      </a>
    `,
  })

  // Handle toolbar button click
  logseq.provideModel({
    showMessage() {
      logseq.UI.showMsg('You clicked the toolbar button!', 'info')
    },
  })
}

// Bootstrap the plugin
logseq.ready(main).catch(console.error)
```

## Build and Run

### 1. Build Your Plugin

```bash
npm run build
```

### 2. Load in Logseq

1. Open Logseq
2. Go to **Settings → Advanced → Developer mode** (enable it)
3. Go to **Settings → Plugins → Load unpacked plugin**
4. Select your plugin directory (`my-logseq-plugin`)

### 3. Test Your Plugin

- Type `/Insert Greeting` in any block
- Click the heart icon in the toolbar
- Check the console for log messages (View → Toggle Developer Tools)

## Development Workflow

### Watch Mode

Run TypeScript in watch mode for automatic rebuilds:

```bash
npm run dev
```

### Hot Reload

When you make changes:

1. Save your files
2. Rebuild (automatic with watch mode)
3. Reload the plugin in Logseq:
   - Settings → Plugins → Click the reload icon next to your plugin

## Your First Features

### Add a Keyboard Shortcut

```typescript
logseq.App.registerCommandPalette(
  {
    key: 'my-quick-note',
    label: 'My Plugin: Quick Note',
    keybinding: {
      binding: 'mod+shift+n',
    },
  },
  async () => {
    const block = await logseq.Editor.insertBlock(
      (await logseq.Editor.getCurrentPage())!.uuid,
      '📝 Quick note: ',
      { before: false }
    )
    if (block) {
      await logseq.Editor.editBlock(block.uuid)
    }
  }
)
```

### Add Plugin Settings

```typescript
logseq.useSettingsSchema([
  {
    key: 'greeting',
    type: 'string',
    default: 'Hello',
    title: 'Greeting Text',
    description: 'Customize the greeting message',
  },
  {
    key: 'showNotifications',
    type: 'boolean',
    default: true,
    title: 'Show Notifications',
    description: 'Display notifications when using the plugin',
  },
])

// Access settings
const greeting = logseq.settings?.greeting || 'Hello'
```

### Query the Database

```typescript
// Get all blocks with a specific tag
const blocks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/refs ?r]
   [?r :block/name "important"]]
`)

console.log('Important blocks:', blocks)
```

## Common Issues

### Plugin not loading?

- Check the console for errors (View → Toggle Developer Tools)
- Ensure `package.json` has the `logseq` field configured
- Verify your plugin ID is unique

### Changes not reflecting?

- Make sure to rebuild (`npm run build`)
- Reload the plugin in Settings → Plugins
- Try restarting Logseq

### TypeScript errors?

- Run `npm install` to ensure dependencies are installed
- Check your `tsconfig.json` configuration
- Make sure you're importing `@logseq/libs`

## Next Steps

Now that you have a working plugin:

1. 📖 Read [Plugin Basics](02-plugin-basics.md) to understand core concepts
2. 🎨 Explore [UI Components](06-ui-components.md) for building interfaces
3. 📝 Learn [Blocks and Pages](05-blocks-and-pages.md) for content manipulation
4. 💡 Check [Examples](17-examples.md) for inspiration

## Example Plugins to Study

- **logseq-plugin-samples** - Official sample plugins
- **logseq-plugin-tabs** - Tab management
- **logseq-plugin-todo-master** - Task management
- **logseq-plugin-heatmap** - Data visualization

---

**Congratulations! 🎉** You've created your first Logseq plugin. Keep building!
