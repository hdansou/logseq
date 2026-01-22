---
title: 'Building Your First Logseq Plugin'
status: 🟢 Stable
version: 'Since v0.8.0'
quadrant: 'Tutorial'
platform: 'Desktop'
related-docs:
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
  - '[Plugin Security](../../explanation/architecture/plugin-security.md)'
  - '[Database Properties](../development/db-properties.md)'
tags: ['plugin', 'development', 'javascript', 'typescript', 'tutorial']
---

# Building Your First Logseq Plugin

This tutorial walks you through creating a functional Logseq plugin from scratch. You'll learn the plugin architecture, core APIs, and development workflow by building a simple "Quick Note" plugin that adds a slash command to insert timestamped notes.

**What You'll Learn:**

- Plugin project structure and configuration
- Using the `@logseq/libs` SDK
- Registering slash commands
- Manipulating blocks and pages
- Testing and debugging plugins

**Prerequisites:**

- Node.js 16+ and npm/yarn installed
- Basic JavaScript/TypeScript knowledge
- Logseq Desktop installed (v0.8.0+)
- Text editor (VS Code recommended)

**Time Required:** ~30 minutes

---

## Table of Contents

1. [Understanding Plugin Architecture](#1-understanding-plugin-architecture)
2. [Project Setup](#2-project-setup)
3. [Writing Your First Plugin](#3-writing-your-first-plugin)
4. [Testing the Plugin](#4-testing-the-plugin)
5. [Adding User Settings](#5-adding-user-settings)
6. [Advanced Features](#6-advanced-features)
7. [Publishing Your Plugin](#7-publishing-your-plugin)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Understanding Plugin Architecture

### Sandbox Environment

Logseq plugins run in an **isolated iframe sandbox** for security:

```
┌─────────────────────────────────────────┐
│  Logseq Main Application                │
│  ┌────────────────────────────────────┐ │
│  │  Plugin Iframe (Sandboxed)         │ │
│  │  ┌──────────────────────────────┐  │ │
│  │  │  Your Plugin Code            │  │ │
│  │  │  (index.html + JS bundle)    │  │ │
│  │  └──────────────────────────────┘  │ │
│  │           ↕                         │ │
│  │  PostMessage RPC Communication      │ │
│  └────────────────────────────────────┘ │
│                 ↕                        │
│  Logseq Core APIs (@logseq/libs SDK)    │
└─────────────────────────────────────────┘
```

**Key Points:**

- Plugins cannot directly access DOM or Logseq internals
- Communication happens via `postMessage` RPC (Remote Procedure Call)
- The `@logseq/libs` SDK wraps this communication with a clean API

### Plugin Lifecycle

```typescript
// Entry point pattern (every plugin follows this)
import '@logseq/libs'

async function main() {
  // 1. Plugin initialization
  console.log('✓ Plugin loaded!')

  // 2. Register features (commands, UI, hooks)
  logseq.Editor.registerSlashCommand('My Command', async () => {
    await logseq.Editor.insertAtEditingCursor('Hello!')
  })

  // 3. Cleanup (optional)
  logseq.beforeunload(async () => {
    console.log('Plugin unloading...')
  })
}

// Bootstrap (required)
logseq.ready(main).catch(console.error)
```

### Core API Namespaces

The SDK exposes these primary namespaces:

| Namespace       | Purpose                      | Example Use Case                             |
| --------------- | ---------------------------- | -------------------------------------------- |
| `logseq.App`    | Application-level operations | Get app version, navigate, register commands |
| `logseq.Editor` | Block/page manipulation      | Insert blocks, update content, query pages   |
| `logseq.DB`     | Database queries             | Datalog queries, watch for changes           |
| `logseq.UI`     | UI utilities                 | Show messages, inject custom UI elements     |
| `logseq.Assets` | Asset management             | List files, open assets                      |
| `logseq.Git`    | Git operations               | Commit, push (desktop file-based only)       |

---

## 2. Project Setup

### Step 1: Create Project Directory

```bash
mkdir logseq-quick-note
cd logseq-quick-note
npm init -y
```

### Step 2: Install Dependencies

```bash
# Install Logseq SDK
npm install @logseq/libs

# Install build tools (we'll use Parcel for simplicity)
npm install --save-dev parcel

# Optional: TypeScript support
npm install --save-dev typescript @types/node
```

### Step 3: Configure package.json

Edit `package.json`:

```json
{
  "name": "logseq-quick-note",
  "version": "0.1.0",
  "description": "Quick timestamped notes with a slash command",
  "author": "Your Name",
  "license": "MIT",
  "main": "dist/index.html",
  "scripts": {
    "dev": "parcel ./index.html --public-url ./",
    "build": "parcel build --public-url . --no-source-maps index.html"
  },
  "logseq": {
    "id": "logseq-quick-note",
    "main": "dist/index.html",
    "icon": "./icon.png",
    "title": "Quick Note"
  }
}
```

**Logseq Configuration Fields:**

| Field    | Required | Description                                    |
| -------- | -------- | ---------------------------------------------- |
| `id`     | ✅ Yes   | Unique identifier (lowercase-with-hyphens)     |
| `main`   | ✅ Yes   | Entry point HTML file (relative path)          |
| `title`  | ❌ No    | Display name (falls back to package `name`)    |
| `icon`   | ❌ No    | Plugin icon shown in marketplace               |
| `effect` | ❌ No    | Set `true` for background-only plugins (no UI) |

### Step 4: Create Entry Point HTML

Create `index.html`:

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Quick Note Plugin</title>
  </head>
  <body>
    <!-- Plugins don't render visible HTML in their iframe by default -->
    <!-- This file is just the entry point for your JavaScript -->
    <script src="./src/main.ts"></script>
  </body>
</html>
```

### Step 5: Create Plugin Source File

Create `src/main.ts`:

```typescript
import '@logseq/libs'

async function main() {
  console.log('🚀 Quick Note plugin loaded!')
}

logseq.ready(main).catch(console.error)
```

### Step 6: Project Structure

Your project should now look like this:

```
logseq-quick-note/
├── package.json
├── index.html
├── icon.png (optional, 128x128px)
├── node_modules/
└── src/
    └── main.ts
```

---

## 3. Writing Your First Plugin

### Goal: Add a Slash Command

We'll create `/quick note` that inserts a timestamped block like this:

```markdown
- **[2025-01-15 14:32]** Quick note here
```

### Step 1: Register Slash Command

Update `src/main.ts`:

```typescript
import '@logseq/libs'

async function main() {
  console.log('🚀 Quick Note plugin loaded!')

  // Register slash command
  logseq.Editor.registerSlashCommand('Quick Note', async () => {
    // Get current timestamp
    const now = new Date()
    const timestamp = now
      .toLocaleString('en-CA', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      })
      .replace(',', '')

    // Insert formatted block at cursor
    await logseq.Editor.insertAtEditingCursor(`**[${timestamp}]** `)

    // Show success message
    await logseq.UI.showMsg('Quick note inserted!', 'success')
  })
}

logseq.ready(main).catch(console.error)
```

**Code Breakdown:**

1. **`registerSlashCommand(label, callback)`**: Adds command to slash menu (`/`)
2. **`new Date()`**: JavaScript date object
3. **`toLocaleString()`**: Formats date/time (YYYY-MM-DD HH:mm)
4. **`insertAtEditingCursor(content)`**: Inserts text at cursor position
5. **`showMsg(message, type)`**: Shows toast notification

### Step 2: Build the Plugin

```bash
npm run dev
```

This starts Parcel in development mode with hot reloading. Output will be in `dist/` folder.

---

## 4. Testing the Plugin

### Step 1: Enable Developer Mode

1. Open **Logseq Desktop**
2. Click **Settings** (⚙️ icon) → **Advanced**
3. Enable **Developer mode**

![Developer Mode Setting](./assets/dev-mode-setting.png)

### Step 2: Load Unpacked Plugin

1. Press `t p` (or **... menu** → **Plugins**)
2. Click **Load unpacked plugin**
3. Select your `logseq-quick-note` folder

![Load Unpacked Plugin](./assets/load-unpacked.png)

### Step 3: Test the Command

1. Create a new block or edit an existing one
2. Type `/` to open the slash command menu
3. Type `quick note` and press Enter
4. You should see a timestamped block inserted!

![Quick Note Command](./assets/quick-note-demo.gif)

### Step 4: Debug with DevTools

**Open Browser DevTools:**

- **Windows/Linux**: `Ctrl + Shift + I`
- **macOS**: `Cmd + Option + I`

**View Plugin Console:**

- Go to **Console** tab
- Look for logs prefixed with your plugin name
- Check for errors or warnings

```javascript
// Add debug logging to your plugin
console.log('[Quick Note] Command triggered')
console.log('[Quick Note] Current block:', block)
```

---

## 5. Adding User Settings

Let's make the timestamp format configurable.

### Step 1: Define Settings Schema

Update `src/main.ts`:

```typescript
import '@logseq/libs'

// Define settings schema
logseq.useSettingsSchema([
  {
    key: 'timestampFormat',
    type: 'enum',
    title: 'Timestamp Format',
    description: 'Choose how timestamps appear',
    enumChoices: ['12-hour', '24-hour', 'ISO-8601'],
    enumPicker: 'select',
    default: '24-hour',
  },
  {
    key: 'includeSeconds',
    type: 'boolean',
    title: 'Include Seconds',
    description: 'Show seconds in timestamp',
    default: false,
  },
  {
    key: 'boldTimestamp',
    type: 'boolean',
    title: 'Bold Timestamp',
    description: 'Wrap timestamp in bold markdown',
    default: true,
  },
])

async function main() {
  console.log('🚀 Quick Note plugin loaded!')

  logseq.Editor.registerSlashCommand('Quick Note', async () => {
    const settings = logseq.settings
    const now = new Date()

    // Format timestamp based on settings
    let timestamp: string
    const options: Intl.DateTimeFormatOptions = {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    }

    if (settings?.includeSeconds) {
      options.second = '2-digit'
    }

    if (settings?.timestampFormat === '12-hour') {
      options.hour12 = true
    } else if (settings?.timestampFormat === 'ISO-8601') {
      timestamp = now.toISOString().slice(0, 19).replace('T', ' ')
    } else {
      options.hour12 = false
    }

    if (!timestamp) {
      timestamp = now.toLocaleString('en-CA', options).replace(',', '')
    }

    // Apply bold formatting if enabled
    const prefix = settings?.boldTimestamp ? '**' : ''
    const suffix = settings?.boldTimestamp ? '**' : ''

    await logseq.Editor.insertAtEditingCursor(
      `${prefix}[${timestamp}]${suffix} `
    )

    await logseq.UI.showMsg('Quick note inserted!', 'success')
  })
}

logseq.ready(main).catch(console.error)
```

### Step 2: Access Settings UI

Reload your plugin and:

1. Go to **Settings** → **Plugins**
2. Find **Quick Note** in the plugin list
3. Click **Settings** icon (⚙️)
4. Configure your preferences

![Plugin Settings](./assets/plugin-settings.png)

### Step 3: Listen for Settings Changes

```typescript
// React to settings changes in real-time
logseq.onSettingsChanged((newSettings, oldSettings) => {
  console.log('[Quick Note] Settings updated:', {
    old: oldSettings,
    new: newSettings,
  })

  logseq.UI.showMsg('Settings updated!', 'success', { timeout: 2000 })
})
```

---

## 6. Advanced Features

### Add Keyboard Shortcut

```typescript
// Register global keyboard shortcut
logseq.App.registerCommandShortcut(
  { binding: 'mod+shift+n', mode: 'global' },
  async () => {
    const settings = logseq.settings
    const now = new Date()
    const timestamp = formatTimestamp(now, settings) // extract to function

    await logseq.Editor.insertAtEditingCursor(`**[${timestamp}]** `)
  },
  {
    key: 'quick-note-shortcut',
    label: 'Insert Quick Note',
    desc: 'Insert timestamped note at cursor',
  }
)
```

**Shortcut Modifiers:**

- `mod`: Ctrl (Windows/Linux) or Cmd (macOS)
- `alt`: Alt key
- `shift`: Shift key
- `ctrl`: Explicit Ctrl (not cross-platform)

### Add Command Palette Entry

```typescript
// Register command in command palette (Cmd+K / Ctrl+K)
logseq.App.registerCommandPalette(
  {
    key: 'quick-note-palette',
    label: 'Quick Note: Insert Timestamp',
    keybinding: { binding: 'mod+shift+n' },
  },
  async () => {
    // Same logic as slash command
    await insertQuickNote()
  }
)
```

### Add Block Context Menu

```typescript
// Right-click menu on blocks
logseq.Editor.registerBlockContextMenuItem(
  'Convert to Quick Note',
  async ({ uuid }) => {
    const block = await logseq.Editor.getBlock(uuid)
    if (!block) return

    const now = new Date()
    const timestamp = formatTimestamp(now, logseq.settings)
    const newContent = `**[${timestamp}]** ${block.content}`

    await logseq.Editor.updateBlock(uuid, newContent)
    await logseq.UI.showMsg('Block converted!', 'success')
  }
)
```

### Add Toolbar Button

```typescript
// Add button to top toolbar
logseq.App.registerUIItem('toolbar', {
  key: 'quick-note-toolbar',
  template: `
    <a data-on-click="quickNoteHandler" 
       class="button" 
       title="Insert Quick Note">
      <i class="ti ti-clock"></i>
    </a>
  `,
})

// Provide event handler
logseq.provideModel({
  async quickNoteHandler() {
    await insertQuickNote()
  },
})
```

### Extract Logic to Reusable Function

```typescript
// Refactor for DRY (Don't Repeat Yourself)
async function insertQuickNote() {
  const settings = logseq.settings
  const now = new Date()
  const timestamp = formatTimestamp(now, settings)
  const prefix = settings?.boldTimestamp ? '**' : ''
  const suffix = settings?.boldTimestamp ? '**' : ''

  await logseq.Editor.insertAtEditingCursor(`${prefix}[${timestamp}]${suffix} `)

  await logseq.UI.showMsg('Quick note inserted!', 'success')
}

function formatTimestamp(date: Date, settings: any): string {
  const options: Intl.DateTimeFormatOptions = {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }

  if (settings?.includeSeconds) {
    options.second = '2-digit'
  }

  if (settings?.timestampFormat === '12-hour') {
    options.hour12 = true
  } else if (settings?.timestampFormat === 'ISO-8601') {
    return date.toISOString().slice(0, 19).replace('T', ' ')
  } else {
    options.hour12 = false
  }

  return date.toLocaleString('en-CA', options).replace(',', '')
}
```

---

## 7. Publishing Your Plugin

### Step 1: Prepare for Production

```bash
# Build optimized bundle
npm run build

# Verify dist/ folder contents
ls dist/
# Should show: index.html, main.[hash].js, icon.png (if included)
```

### Step 2: Update package.json

```json
{
  "name": "logseq-quick-note",
  "version": "1.0.0",
  "description": "Quick timestamped notes with a slash command",
  "author": "Your Name <your.email@example.com>",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "https://github.com/yourusername/logseq-quick-note"
  },
  "keywords": ["logseq", "plugin", "timestamp", "notes"],
  "logseq": {
    "id": "logseq-quick-note",
    "main": "dist/index.html",
    "icon": "./icon.png",
    "title": "Quick Note"
  }
}
```

### Step 3: Create README.md

```markdown
# Logseq Quick Note Plugin

Insert timestamped notes quickly with a slash command, keyboard shortcut, or toolbar button.

## Features

- `/quick note` slash command
- Configurable timestamp formats (12-hour, 24-hour, ISO-8601)
- Keyboard shortcut: `Cmd+Shift+N` (Mac) / `Ctrl+Shift+N` (Windows/Linux)
- Right-click block menu: "Convert to Quick Note"

## Installation

### From Marketplace

1. Open Logseq → Plugins (`t p`)
2. Search for "Quick Note"
3. Click Install

### Manual Installation

1. Download the latest release
2. Unzip to a folder
3. Load unpacked plugin in Logseq

## Usage

Type `/quick note` in any block to insert a timestamped note.

## Settings

- **Timestamp Format**: 12-hour, 24-hour, or ISO-8601
- **Include Seconds**: Show seconds in timestamp
- **Bold Timestamp**: Wrap timestamp in bold markdown

## License

MIT
```

### Step 4: Submit to Marketplace

1. Create GitHub repository with your plugin code
2. Create a release with semantic versioning (e.g., `v1.0.0`)
3. Submit to **Logseq Marketplace**:
   - Fork https://github.com/logseq/marketplace
   - Add your plugin entry to `plugins.json`
   - Create pull request

**plugins.json Entry Example:**

```json
{
  "id": "logseq-quick-note",
  "title": "Quick Note",
  "description": "Insert timestamped notes quickly",
  "author": "Your Name",
  "repo": "yourusername/logseq-quick-note",
  "icon": "./icon.png",
  "theme": false
}
```

---

## 8. Troubleshooting

### Plugin Not Loading

**Problem:** Plugin doesn't appear after loading

**Solutions:**

1. Check `package.json` has valid `logseq` config
2. Verify `main` field points to correct HTML file
3. Check console for JavaScript errors (`Ctrl/Cmd + Shift + I`)
4. Ensure `logseq.ready()` is called

### Slash Command Not Appearing

**Problem:** `/quick note` doesn't show in menu

**Solutions:**

1. Verify `registerSlashCommand` is called in `main()` function
2. Check command name doesn't conflict with built-in commands
3. Reload plugin after code changes (`t p` → click reload icon)

### Settings Not Saving

**Problem:** Settings reset after restart

**Solutions:**

1. Check `useSettingsSchema` is called before `logseq.ready()`
2. Verify settings schema has valid `key` and `default` values
3. Access settings via `logseq.settings` (not `logseq.settings.get()`)

### Build Errors

**Problem:** Parcel build fails

**Solutions:**

```bash
# Clear cache and rebuild
rm -rf .parcel-cache dist
npm run build

# Check for TypeScript errors
npx tsc --noEmit

# Update dependencies
npm update
```

### "Cannot read property 'Editor' of undefined"

**Problem:** `logseq` is undefined

**Solutions:**

1. Import SDK: `import '@logseq/libs'` at top of file
2. Wrap API calls in `logseq.ready(main)` callback
3. Check `index.html` loads your script file correctly

### Performance Issues

**Problem:** Plugin slows down Logseq

**Solutions:**

```typescript
// Bad: Runs on every keystroke
logseq.DB.onChanged(() => {
  // Heavy computation
  processAllBlocks()
})

// Good: Debounce expensive operations
import { debounce } from 'lodash'

const debouncedProcess = debounce(() => {
  processAllBlocks()
}, 500)

logseq.DB.onChanged(debouncedProcess)
```

---

## Next Steps

**Congratulations!** You've built a functional Logseq plugin. 🎉

### Continue Learning:

1. **[Plugin API Reference](../../reference/api/plugin-api.md)** - Complete API documentation
2. **[Plugin Security](../../explanation/architecture/plugin-security.md)** - Security best practices
3. **[Database Properties](../development/db-properties.md)** - Working with DB graphs
4. **[Official Plugin Samples](https://github.com/logseq/logseq-plugin-samples)** - Example plugins

### Explore Advanced Topics:

- **Custom UI Components**: Inject React/Vue components into Logseq
- **Database Queries**: Use Datalog to query your graph
- **Theme Development**: Create custom themes as plugins
- **External APIs**: Integrate with third-party services (OpenAI, Notion, etc.)
- **Macro Renderers**: Build custom block renderers like `{{renderer :my-type}}`

### Join the Community:

- **Discord**: https://discord.gg/KpN4eHY
- **Forum**: https://discuss.logseq.com
- **GitHub Discussions**: https://github.com/logseq/logseq/discussions

---

## Complete Code Reference

### Final src/main.ts

```typescript
import '@logseq/libs'

// ========== Settings Schema ==========
logseq.useSettingsSchema([
  {
    key: 'timestampFormat',
    type: 'enum',
    title: 'Timestamp Format',
    description: 'Choose how timestamps appear',
    enumChoices: ['12-hour', '24-hour', 'ISO-8601'],
    enumPicker: 'select',
    default: '24-hour',
  },
  {
    key: 'includeSeconds',
    type: 'boolean',
    title: 'Include Seconds',
    description: 'Show seconds in timestamp',
    default: false,
  },
  {
    key: 'boldTimestamp',
    type: 'boolean',
    title: 'Bold Timestamp',
    description: 'Wrap timestamp in bold markdown',
    default: true,
  },
])

// ========== Helper Functions ==========
function formatTimestamp(date: Date, settings: any): string {
  const options: Intl.DateTimeFormatOptions = {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }

  if (settings?.includeSeconds) {
    options.second = '2-digit'
  }

  if (settings?.timestampFormat === '12-hour') {
    options.hour12 = true
  } else if (settings?.timestampFormat === 'ISO-8601') {
    return date.toISOString().slice(0, 19).replace('T', ' ')
  } else {
    options.hour12 = false
  }

  return date.toLocaleString('en-CA', options).replace(',', '')
}

async function insertQuickNote() {
  const settings = logseq.settings
  const now = new Date()
  const timestamp = formatTimestamp(now, settings)
  const prefix = settings?.boldTimestamp ? '**' : ''
  const suffix = settings?.boldTimestamp ? '**' : ''

  await logseq.Editor.insertAtEditingCursor(`${prefix}[${timestamp}]${suffix} `)

  await logseq.UI.showMsg('Quick note inserted!', 'success')
}

// ========== Main Plugin Logic ==========
async function main() {
  console.log('🚀 Quick Note plugin loaded!')

  // Slash command
  logseq.Editor.registerSlashCommand('Quick Note', insertQuickNote)

  // Keyboard shortcut
  logseq.App.registerCommandShortcut(
    { binding: 'mod+shift+n', mode: 'global' },
    insertQuickNote,
    {
      key: 'quick-note-shortcut',
      label: 'Insert Quick Note',
      desc: 'Insert timestamped note at cursor',
    }
  )

  // Command palette entry
  logseq.App.registerCommandPalette(
    {
      key: 'quick-note-palette',
      label: 'Quick Note: Insert Timestamp',
      keybinding: { binding: 'mod+shift+n' },
    },
    insertQuickNote
  )

  // Block context menu
  logseq.Editor.registerBlockContextMenuItem(
    'Convert to Quick Note',
    async ({ uuid }) => {
      const block = await logseq.Editor.getBlock(uuid)
      if (!block) return

      const settings = logseq.settings
      const now = new Date()
      const timestamp = formatTimestamp(now, settings)
      const prefix = settings?.boldTimestamp ? '**' : ''
      const suffix = settings?.boldTimestamp ? '**' : ''
      const newContent = `${prefix}[${timestamp}]${suffix} ${block.content}`

      await logseq.Editor.updateBlock(uuid, newContent)
      await logseq.UI.showMsg('Block converted!', 'success')
    }
  )

  // Toolbar button
  logseq.App.registerUIItem('toolbar', {
    key: 'quick-note-toolbar',
    template: `
      <a data-on-click="quickNoteHandler" 
         class="button" 
         title="Insert Quick Note (Mod+Shift+N)"
         style="opacity: 0.7;">
        <i class="ti ti-clock"></i>
      </a>
    `,
  })

  // Provide event handler for toolbar button
  logseq.provideModel({
    quickNoteHandler: insertQuickNote,
  })

  // Listen for settings changes
  logseq.onSettingsChanged((newSettings, oldSettings) => {
    console.log('[Quick Note] Settings updated:', {
      old: oldSettings,
      new: newSettings,
    })
    logseq.UI.showMsg('Settings updated!', 'success', { timeout: 2000 })
  })
}

// ========== Bootstrap ==========
logseq.ready(main).catch(console.error)
```

### package.json

```json
{
  "name": "logseq-quick-note",
  "version": "1.0.0",
  "description": "Quick timestamped notes with a slash command",
  "author": "Your Name <your.email@example.com>",
  "license": "MIT",
  "main": "dist/index.html",
  "repository": {
    "type": "git",
    "url": "https://github.com/yourusername/logseq-quick-note"
  },
  "keywords": ["logseq", "plugin", "timestamp", "notes"],
  "scripts": {
    "dev": "parcel ./index.html --public-url ./",
    "build": "parcel build --public-url . --no-source-maps index.html"
  },
  "dependencies": {
    "@logseq/libs": "^0.0.17"
  },
  "devDependencies": {
    "parcel": "^2.12.0",
    "typescript": "^5.3.0",
    "@types/node": "^20.10.0"
  },
  "logseq": {
    "id": "logseq-quick-note",
    "main": "dist/index.html",
    "icon": "./icon.png",
    "title": "Quick Note"
  }
}
```

---

**Happy Plugin Development! 🎉**

For questions or feedback, join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
