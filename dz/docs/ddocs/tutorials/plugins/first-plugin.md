---
title: 'Building Your First Logseq Plugin'
status: 🟢 Stable
version: '@logseq/libs 0.3.4'
quadrant: 'Tutorial'
platform: 'Desktop'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
related-docs:
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
  - '[Plugin Security](../../explanation/architecture/plugin-security.md)'
  - '[Properties Reference](../../reference/data/properties-reference.md)'
tags: ['plugin', 'development', 'javascript', 'typescript', 'tutorial']
---

# Building Your First Logseq Plugin

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

This tutorial walks you through creating a functional Logseq plugin from scratch. You'll learn the plugin architecture, core APIs, and development workflow by building a simple "Quick Note" plugin that adds a slash command to insert timestamped notes.

**What You'll Learn:**

- Plugin project structure and configuration
- Using the `@logseq/libs` SDK
- Registering slash commands
- Manipulating blocks and pages
- Testing and debugging plugins

**Prerequisites:**

- Node.js and npm/yarn installed (a recent LTS; your bundler, not Logseq, sets the floor)
- Basic JavaScript/TypeScript knowledge
- **Logseq Desktop.** Plugin development and testing require the desktop app, because Logseq has to read your code from a local directory (`libs/guides/starter_guide.md`)
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

- A plugin's own code runs in its own iframe document; it cannot reach into the host DOM directly. To put UI in the host it goes through SDK calls such as `provideUI`, `provideStyle`, and `App.registerUIItem`.
- Communication happens via `postMessage` RPC (Remote Procedure Call)
- The `@logseq/libs` SDK wraps this communication with a clean API

> There is a second runtime mode. `package.json > logseq.mode` accepts `'shadow' | 'iframe'` (`libs/src/LSPlugin.ts:74`), and `libs/src/LSPlugin.shadow.ts` implements the shadow variant, in which the plugin's script is evaluated inside the host document instead of an iframe. This tutorial uses the default iframe mode throughout.

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

`ILSPluginUser` declares exactly these namespaces (`libs/src/LSPlugin.ts:1255-1266`):

| Namespace             | Interface / class       | Purpose                                                     |
| --------------------- | ----------------------- | ----------------------------------------------------------- |
| `logseq.App`          | `IAppProxy`             | App-level operations: version, navigation, UI items, hooks   |
| `logseq.Commands`     | `ICommandsProxy`        | Unified `register` / `execute` for commands (see §6)         |
| `logseq.Editor`       | `IEditorProxy`          | Block/page manipulation, slash and context-menu commands     |
| `logseq.DB`           | `IDBProxy`              | `datascriptQuery`, `onChanged`, `onBlockChanged`             |
| `logseq.Git`          | `IGitProxy`             | `execCommand`, `loadIgnoreFile`, `saveIgnoreFile`            |
| `logseq.UI`           | `IUIProxy`              | `showMsg`, `closeMsg`, element queries, theme CSS props      |
| `logseq.Assets`       | `IAssetsProxy`          | `listFilesOfCurrentGraph`, asset helpers                     |
| `logseq.Net`          | `LSPluginNet`           | Network helpers (`libs/src/modules/LSPlugin.Net.ts`)         |
| `logseq.FileStorage`  | `LSPluginFileStorage`   | Per-plugin key/value storage (`modules/LSPlugin.Storage.ts`) |
| `logseq.Experiments`  | `LSPluginExperiments`   | Unstable APIs (`modules/LSPlugin.Experiments.ts`)            |

There is no `logseq.Search` namespace. `libs/src/modules/LSPlugin.Search.ts` backs `logseq.App.registerSearchService(hooks)` (declared at `libs/src/LSPlugin.ts:415`, constructed at `libs/src/LSPlugin.user.ts:430-438`), which lets a plugin *provide* a search backend rather than query one.

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

**Logseq Configuration Fields**

The `logseq` object is typed as `LSPluginPkgConfig` (`libs/src/LSPlugin.ts:69-85`) and parsed in `libs/src/LSPlugin.core.ts:716-740`:

| Field      | Required | Description                                                                                   |
| ---------- | -------- | --------------------------------------------------------------------------------------------- |
| `id`       | ❌ No    | Identity key. Auto-generated from the directory name when loading an unpacked plugin.           |
| `main`     | ❌ No    | Entry file. `entry` is an alias, and `pkg.main` is the fallback: `logseq.entry \|\| logseq.main \|\| pkg.main`. A theme-only plugin needs no entry. |
| `entry`    | ❌ No    | Alias of `main`.                                                                                |
| `devEntry` | ❌ No    | Alternative entry used in development; takes precedence over `entry`/`main`.                    |
| `title`    | ❌ No    | Display name. Falls back to `pkg.title`, then `pkg.name`.                                       |
| `icon`     | ❌ No    | Plugin icon. Falls back to `pkg.icon`. Resolved relative to the plugin root.                    |
| `mode`     | ❌ No    | `'shadow'` or `'iframe'`.                                                                       |
| `themes`   | ❌ No    | Array of `Theme` objects. Declaring it marks the package as a theme.                            |
| `theme`    | ❌ No    | Legacy single-theme flag. `LSPlugin.ts:82-84` says "For legacy themes, do not use."              |

> **`effect` is not a `logseq.*` field.** `LSPluginPkgConfig` has no `effect` key. The flag is read from the **top level** of `package.json`, in the "Pick legal attrs" list at `libs/src/LSPlugin.core.ts:696-711`, which assigns `this._options[k] = pkg[k]` for `name`, `author`, `repository`, `version`, `description`, `repo`, `title`, `effect`, `sponsors`. Marketplace installs confirm this: `src/electron/electron/plugin.cljs:167` writes `(set! (.-effect pkg) (boolean effect))` onto the package root. Writing `"effect": true` inside `logseq` does nothing.

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

The toggle is `developer-mode-row` (`src/main/frontend/components/settings.cljs:580-587`), hidden on native mobile (`:926`). Its description string reads "Developer mode helps contributors and extension developers test their integrations with Logseq more efficiently." (`src/resources/dicts/en.edn:1703-1704`).

### Step 2: Load Unpacked Plugin

1. Press `t p` (or **... menu** → **Plugins**)
2. Click **Load unpacked plugin**
3. Select your `logseq-quick-note` folder

`t p` is the binding for `:ui/goto-plugins` (`src/main/frontend/modules/shortcut/config.cljs:464-466`). The **Load unpacked plugin** button is rendered at `src/main/frontend/components/plugins.cljs:833-837` from the string `:plugin/load-unpacked` (`src/resources/dicts/en.edn:1264`).

### Step 3: Test the Command

1. Create a new block or edit an existing one
2. Type `/` to open the slash command menu
3. Type `quick note` and press Enter
4. You should see a timestamped block inserted!

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
    let timestamp: string | undefined
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

1. Open the **Plugins** dashboard (`t p`)
2. Find **Quick Note** in the plugin list
3. Open its **Settings** (⚙️)
4. Configure your preferences

The panel is the `plugin-settings-modal` dialog (`src/main/frontend/components/plugins.css:967`), rendered from the schema you registered. A plugin can also open it itself with `logseq.showSettingsUI()` (`libs/src/LSPlugin.ts:1217`).

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

### The `logseq.Commands` alternative

The per-surface registration calls used below all still exist and still work. Alongside them the SDK now ships a unified command API, `ICommandsProxy` (`libs/src/LSPlugin.ts:615-637`), which registers one logical command against one or more UI surfaces:

```typescript
const unregister = logseq.Commands.register(
  'insert-quick-note',
  {
    title: 'Insert quick note',
    placements: ['slash', 'palette', 'shortcut'],
    keybinding: { mode: 'global', binding: 'mod+shift+n' },
  },
  insertQuickNote
)

logseq.beforeunload(async () => {
  unregister && unregister()
})
```

Valid placements are `'palette' | 'shortcut' | 'slash' | 'block-context-menu' | 'highlight-context-menu' | 'page-menu' | 'simple'` (`CommandPlacement`, `libs/src/LSPlugin.ts:254-261`). `Commands.execute(id, ...args)` runs a built-in (`logseq.*`) or plugin command. See `libs/guides/commands_api_guide.md` for the full option list.

Under the hood, `Commands.register` maps each placement onto the same primitives shown below (`libs/src/LSPlugin.user.ts:275-300`), so the two styles are interchangeable.

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

**Shortcut Modifiers** — aliases are resolved in `src/main/frontend/modules/shortcut/utils.cljs:10-17`:

- `mod`: `meta` on macOS, `ctrl` elsewhere
- `cmd` / `command`: `meta`
- `opt` / `option`: `alt`
- `shift`, `ctrl`, `alt`, `meta`: literal modifiers

`SimpleCommandKeybinding` is `{ mode?: 'global' | 'non-editing' | 'editing'; binding: Keybinding; mac?: string }` (`libs/src/LSPlugin.ts:289-293`); `mac` overrides the binding on macOS. The signature is `registerCommandShortcut(keybinding, action, opts?)`, where `opts` is `Partial<{ key, label, desc, extras }>` (`libs/src/LSPlugin.ts:445-454`).

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
    // `block.content` is deprecated in the type surface (LSPlugin.ts:195);
    // DB graphs carry the text on `block.title`.
    const newContent = `**[${timestamp}]** ${block.title}`

    await logseq.Editor.updateBlock(uuid, newContent)
    await logseq.UI.showMsg('Block converted!', 'success')
  }
)
```

> **Read block text from `title`, not `content`.** `BlockEntity` (`libs/src/LSPlugin.ts:187-214`) declares `title: string` and `fullTitle: string`, and marks `content?: string` with `// @deprecated. use :title instead!` (`:195`). `fullTitle` is the same text with block-reference uuids replaced by their title text.
>
> The callback signature is `BlockCommandCallback = (e: IHookEvent & { uuid: BlockUUID }) => Promise<void>` (`libs/src/LSPlugin.ts:251-253`), so `uuid` is the only guaranteed field.
>
> `updateBlock` is `(srcBlock: BlockIdentity | EntityID, content: string, opts?: Partial<{ properties: {} }>)` (`libs/src/LSPlugin.ts:799-803`).

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

The `logseq` object is **required**: the installer throws `Illegal logseq package` when
`package.json` has no `logseq` key (`src/main/frontend/handler/plugin.cljs:1171-1172`). For a
non-GitHub install the plugin id falls back to `logseq.id`, then to the package `name`
(`:1173-1175`); a plugin is treated as a theme when `logseq.theme` or `logseq.themes` is present
(`:1177`).

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

1. Open Logseq → Plugins (`t p`, bound to `:ui/goto-plugins` at
   `src/main/frontend/modules/shortcut/config.cljs:464`)
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

1. Create a GitHub repository with your plugin code
2. Create a release with semantic versioning (e.g., `v1.0.0`)
3. Submit to the **Logseq Marketplace**: fork https://github.com/logseq/marketplace, add your entry, open a pull request

**What Logseq fetches.** The app reads the marketplace from a fixed raw-GitHub base (`src/main/frontend/handler/plugin.cljs:56-58`):

```clojure
(defonce central-endpoint "https://raw.githubusercontent.com/logseq/marketplace/master/")
(defonce plugins-url (str central-endpoint "plugins.json"))
(defonce stats-url   (str central-endpoint "stats.json"))
```

`plugins.json` is an object whose **`packages`** key holds the entries — `load-marketplace-plugins` reads `(:packages res)` (`plugin.cljs:197-215`). Per-plugin assets such as the icon are resolved under `packages/<id>/` by `pkg-asset` (`plugin.cljs:192-196`).

**Fields the app actually consumes.** The plugin card destructures `id name title version url description author icon iir repo sponsors webPkg` (`src/main/frontend/components/plugins.cljs:358-360`), and separately reads:

- `effect` — filters out effect plugins on non-Electron builds (`plugin.cljs:206`) and is written onto the installed `package.json` root (`src/electron/electron/plugin.cljs:167`)
- `supportsDB` — drives the database-support badge; `false` shows "does not support DB", `true` shows "supports DB" (`components/plugins.cljs:409-414`)
- `web` — allows a package on non-Electron builds (`plugin.cljs:206`)

> **Unverified:** The exact required shape of a marketplace submission, and whether an entry lives inline in `plugins.json` or in a per-package `manifest.json`, are properties of the `logseq/marketplace` repository and cannot be determined from the Logseq source tree. Follow that repository's own contribution instructions. The list above is only what the client reads back.
>
> Note also that `theme` is **not** among the keys the marketplace client reads; theme-ness is decided from the installed package's `logseq.themes` / `logseq.theme` (`libs/src/LSPlugin.core.ts:740`).

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

1. Check `useSettingsSchema` is called **before** `logseq.ready()`. This is load-bearing: `ready()` merges the schema's defaults into `baseInfo.settings` and then re-sends the schema to the host (`libs/src/LSPlugin.user.ts:829-837`). `useSettingsSchema` itself only forwards to the host when already connected, otherwise it just stores the schema for `ready()` to pick up (`:886-896`).
2. Verify every schema entry has `key`, `type`, `title`, `description` and `default` — all five are non-optional in `SettingSchemaDesc` (`libs/src/LSPlugin.ts:295-313`).
3. Access settings via the `logseq.settings` property (`libs/src/LSPlugin.ts:1123`), not a getter method. To write, use `logseq.updateSettings(attrs)` (`:1210`).

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
3. **[Properties Reference](../../reference/data/properties-reference.md)** - Built-in and user properties in DB graphs
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
      const newContent = `${prefix}[${timestamp}]${suffix} ${block.title}`

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
    "@logseq/libs": "^0.3.4"
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
