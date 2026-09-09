---
title: 'Plugin API Reference'
description: "Complete reference for Logseq's Plugin SDK APIs for extension development"
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [file, db]
sinceVersion: '0.4.0'
relatedDocs:
  tutorials:
    - '[Plugin Development Tutorial](../../tutorials/plugins/getting-started.md)'
  howTo:
    - '[Publish Plugin](../../how-to/plugins/publish-plugin.md)'
  reference:
    - '[HTTP API Reference](http-api.md)'
---

# Plugin API Reference

> **Last revised:** 2026-09-09

> Complete reference for the Logseq Plugin SDK, enabling developers to extend Logseq with custom functionality, commands, and UI elements.

## Overview

The Plugin API provides programmatic access to Logseq through the global `logseq` object. Key capabilities:

- **Content manipulation**: Create, read, update, delete blocks and pages
- **UI integration**: Commands, toolbars, sidebars, modals
- **Database queries**: Run Datalog queries on your graph
- **Event hooks**: React to changes in blocks, pages, themes
- **Asset management**: Work with files and attachments

---

## Quick Reference

| Namespace         | Purpose              | Key Methods                                       | Interface (`libs/src/LSPlugin.ts`) |
| ----------------- | -------------------- | ------------------------------------------------- | ---------------------------------- |
| `logseq.App`      | App-level operations | `registerCommand`, `pushState`, `getCurrentGraph` | `IAppProxy` (404)                  |
| `logseq.Commands` | Unified command bus  | `register`, `execute`                             | `ICommandsProxy` (615)             |
| `logseq.Editor`   | Content manipulation | `insertBlock`, `getPage`, `createPage`            | `IEditorProxy` (642)               |
| `logseq.DB`       | Database queries     | `q`, `datascriptQuery`, `onChanged`               | `IDBProxy` (956)                   |
| `logseq.Git`      | Version control      | `execCommand`                                     | `IGitProxy` (1010)                 |
| `logseq.UI`       | User interface       | `showMsg`, `closeMsg`                             | `IUIProxy` (1032)                  |
| `logseq.Utils`    | SDK helpers          | `toJs`                                            | `IUtilsProxy` (1045)               |
| `logseq.Assets`   | File management      | `listFilesOfCurrentGraph`                         | `IAssetsProxy` (1052)              |

---

## Getting Started

### Plugin Entry Point

Every plugin starts with `logseq.ready()`:

```javascript
import '@logseq/libs'

logseq.ready(() => {
  console.log('Plugin loaded!')

  // Register commands, hooks, etc.
  logseq.App.registerCommandPalette({ key: 'hello', label: 'Say Hello' }, () =>
    logseq.UI.showMsg('Hello from plugin!')
  )
})
```

### Plugin Model

Pass a model object for UI callbacks:

```javascript
logseq.ready(
  {
    onSettingsChanged: (newSettings) => {
      console.log('Settings updated:', newSettings)
    },
  },
  () => {
    console.log('Plugin ready with model')
  }
)
```

---

## logseq.App

Application-level operations and registrations.
Interface: `libs/src/LSPlugin.ts:404-610` (`IAppProxy`).

### App Info

#### getInfo

Get Logseq application information. Cached after the first call
(`libs/src/LSPlugin.user.ts:402-407`).

```typescript
getInfo(key?: keyof AppInfo): Promise<AppInfo | any>

// AppInfo: { version: string; supportDb: boolean }
// -- src/main/logseq/api/app.cljs:46-52
```

> `getInfo` is implemented in the JS SDK, not as a host export. It is therefore
> **not** reachable over the HTTP API; use `logseq.App.getAppInfo` there.

#### getUserInfo

Get the signed-in user (Logseq Sync / cloud). Resolves `null` when not signed in.

```typescript
getUserInfo(): Promise<AppUserInfo | null>
```

#### checkCurrentIsDbGraph

Whether the active graph is a DB graph.

```typescript
checkCurrentIsDbGraph(): Promise<Boolean>
```

#### getStateFromStore / setStateFromStore

Read and write paths of the frontend state atom.

```typescript
getStateFromStore<T = any>(path: string | Array<string>): Promise<T>
setStateFromStore(path: string | Array<string>, value: any): Promise<void>
```

### Command Registration

#### registerCommandPalette

Add command to command palette (`Cmd/Ctrl+Shift+P`).

```typescript
registerCommandPalette(
  opts: { key: string; label: string; keybinding?: SimpleCommandKeybinding },
  action: SimpleCommandCallback
): void
```

**Example**:

```javascript
logseq.App.registerCommandPalette(
  {
    key: 'my-action',
    label: 'Do Something',
    keybinding: { binding: 'mod+shift+d' },
  },
  () => console.log('Action triggered!')
)
```

#### registerCommand

Register a named command with full options.

```typescript
registerCommand(
  type: string,
  opts: {
    key: string;
    label: string;
    desc?: string;
    palette?: boolean;
    keybinding?: SimpleCommandKeybinding;
  },
  action: SimpleCommandCallback
): void
```

#### registerCommandShortcut

Bind keyboard shortcut to action.

```typescript
registerCommandShortcut(
  keybinding: SimpleCommandKeybinding | string,
  action: SimpleCommandCallback,
  opts?: { key?: string; label?: string; desc?: string; extras?: Record<string, any> }
): void
```

**Keybinding format**:
| Modifier | Meaning |
|----------|---------|
| `mod` | Cmd (Mac) / Ctrl (Windows/Linux) |
| `ctrl` | Control |
| `shift` | Shift |
| `alt` | Alt/Option |
| `meta` | Command (Mac) |

**Examples**: `"mod+shift+a"`, `"ctrl+alt+p"`, `"mod+enter"`

### UI Item Registration

#### registerUIItem

Add items to toolbar or other UI containers.

```typescript
registerUIItem(
  type: "toolbar" | "pagebar",
  opts: {
    key: string;
    template: string;
  }
): void
```

**Example**:

```javascript
logseq.App.registerUIItem('toolbar', {
  key: 'my-button',
  template: `<a class="button" data-on-click="onButtonClick">Click Me</a>`,
})
```

#### registerPageMenuItem

Add item to page context menu.

```typescript
registerPageMenuItem(
  tag: string,
  action: (e: IHookEvent & { page: string }) => void
): void
```

**Example**:

```javascript
logseq.App.registerPageMenuItem('Export Page', async (e) => {
  const page = await logseq.Editor.getPage(e.page)
  console.log('Exporting page:', page.name)
})
```

### Navigation

#### pushState

Navigate to a route.

```typescript
pushState(
  route: string,  // e.g., "page", "home", "all-pages", "graph"
  params?: Record<string, any>,
  query?: Record<string, any>
): void
```

**Example**:

```javascript
// Navigate to a page
logseq.App.pushState('page', { name: 'My Page' })

// Go home
logseq.App.pushState('home')
```

#### replaceState

Replace current route without adding to history.

```typescript
replaceState(
  route: string,
  params?: Record<string, any>,
  query?: Record<string, string>
): void
```

### Graph Operations

#### getCurrentGraph

Get information about the current graph.

```typescript
getCurrentGraph(): Promise<AppGraphInfo | null>

// AppGraphInfo:
// {
//   name: string;
//   url: string;
//   path: string;
// }
```

#### getCurrentGraphConfigs

Get graph configuration values.

```typescript
getCurrentGraphConfigs(...keys: string[]): Promise<Record<string, any>>
```

#### setCurrentGraphConfigs

Update graph configuration.

```typescript
setCurrentGraphConfigs(configs: Record<string, any>): Promise<void>
```

#### getCurrentGraphFavorites / getCurrentGraphRecent / getCurrentGraphTemplates

```typescript
getCurrentGraphFavorites(): Promise<Array<string | PageEntity> | null>
getCurrentGraphRecent(): Promise<Array<string | PageEntity> | null>
getCurrentGraphTemplates(): Promise<Record<string, BlockEntity> | null>
```

### External Commands & Plugins

#### invokeExternalCommand

Invoke any registered palette command by id.

```typescript
invokeExternalCommand(type: ExternalCommandType, ...args: Array<any>): Promise<void>
```

#### invokeExternalPlugin

Call a command or model exposed by another plugin.

```typescript
// type: `xx-plugin-id.commands.xx-key` or `xx-plugin-id.models.xx-key`
invokeExternalPlugin(type: string, ...args: Array<any>): Promise<unknown>
```

#### getExternalPlugin

Look up another installed plugin by id; resolves `null` if absent.

```typescript
getExternalPlugin(pid: string): Promise<{} | null>
```

### System Operations

```typescript
relaunch(): Promise<void>
quit(): Promise<void>
openExternalLink(url: string): Promise<void>

setZoomFactor(factor: number): void
setFullScreen(flag: boolean | 'toggle'): void
setLeftSidebarVisible(flag: boolean | 'toggle'): void
setRightSidebarVisible(flag: boolean | 'toggle'): void
clearRightSidebarBlocks(opts?: { close: boolean }): void
```

> `openExternalLink` silently ignores anything that is not an `http`/`https`
> URL (`src/main/logseq/api/app.cljs:116-119`).

### User Preferences

#### getUserConfigs

Get user settings.

```typescript
getUserConfigs(): Promise<AppUserConfigs>

// AppUserConfigs includes:
// {
//   preferredThemeMode: "dark" | "light";
//   preferredFormat: "markdown" | "org";
//   preferredDateFormat: string;
//   preferredStartOfWeek: string;
//   preferredLanguage: string;
//   preferredWorkflow: string;
//   currentGraph: string;
//   showBracket: boolean;
//   enabledFlashcards: boolean;
//   enabledJournals: boolean;
// }
```

### Hooks

#### onCurrentGraphChanged

Triggered when user switches graphs.

```javascript
logseq.App.onCurrentGraphChanged(() => {
  console.log('Graph changed!')
})
```

#### onThemeModeChanged

Triggered when theme mode changes.

```javascript
logseq.App.onThemeModeChanged(({ mode }) => {
  console.log('Theme mode:', mode) // "dark" or "light"
})
```

#### onBlockRendererSlotted

Provide a UI slot for a specific block by UUID. This is a conditional slot hook: you pass a block UUID and a callback that fires when that block's slot is available.

```typescript
onBlockRendererSlotted(
  uuid: BlockUUID,
  callback: (e: IHookEvent & { slot: string } & BlockEntity) => void
): IUserOffHook
```

**Example**:

```javascript
logseq.App.onBlockRendererSlotted(blockUuid, ({ slot }) => {
  logseq.provideUI({
    key: `renderer-${blockUuid}`,
    slot,
    template: `<div>Custom renderer content</div>`,
  })
})
```

#### onMacroRendererSlotted

Register custom macro renderer for `{{renderer arg1, arg2}}` macros.

```javascript
// e.g. {{renderer :h1, hello world, green}}
logseq.App.onMacroRendererSlotted(({ slot, payload }) => {
  const { arguments: args, uuid } = payload
  let [type, text, color] = args
  if (type !== ':h1') return
  logseq.provideUI({
    key: 'h1-playground',
    slot,
    template: `<h2 style="color: ${color || 'red'}">${text}</h2>`,
  })
})
```

#### onRouteChanged

Triggered on navigation.

```javascript
logseq.App.onRouteChanged(({ path, template }) => {
  console.log('Route:', path)
})
```

---

## logseq.Editor

Content manipulation API for blocks and pages.

### Block Operations

#### getCurrentBlock

Get the block currently being edited.

```typescript
getCurrentBlock(): Promise<BlockEntity | null>
```

#### getBlock

Get a block by UUID or ID.

```typescript
getBlock(
  srcBlock: BlockUUID | BlockId,
  opts?: { includeChildren?: boolean }
): Promise<BlockEntity | null>
```

**BlockEntity structure**:

```typescript
interface BlockEntity {
  id: number           // database entity id
  uuid: string
  title: string        // block text content
  fullTitle: string    // title with block refs resolved
  content?: string     // @deprecated - use title instead
  format: 'markdown' | 'org'
  order: string
  page: IEntityID      // { id: number }
  parent: IEntityID    // { id: number }
  createdAt: number
  updatedAt: number
  properties?: Record<string, any>
  'collapsed?': boolean
  marker?: string
  children?: Array<BlockEntity | BlockUUIDTuple>
}
```

#### insertBlock

Insert a new block.

```typescript
insertBlock(
  srcBlock: BlockUUID | BlockId,
  content: string,
  opts?: {
    before?: boolean;      // Insert before target
    sibling?: boolean;     // Insert as sibling (vs child)
    isPageBlock?: boolean; // Target is page UUID
    focus?: boolean;       // Focus after insert
    customUUID?: string;   // Custom UUID
    properties?: Record<string, any>;
  }
): Promise<BlockEntity>
```

**Examples**:

```javascript
// Insert as child of current block
const newBlock = await logseq.Editor.insertBlock(
  currentBlock.uuid,
  'New child block'
)

// Insert as sibling before
await logseq.Editor.insertBlock(currentBlock.uuid, 'Before sibling', {
  sibling: true,
  before: true,
})

// Insert to page
await logseq.Editor.insertBlock(pageUUID, 'First block on page', {
  isPageBlock: true,
})
```

#### updateBlock

Update block content.

```typescript
updateBlock(
  srcBlock: BlockUUID | BlockId,
  content: string,
  opts?: { properties?: Record<string, any> }
): Promise<void>
```

#### removeBlock

Delete a block.

```typescript
removeBlock(srcBlock: BlockUUID | BlockId): Promise<void>
```

#### moveBlock

Move a block to new location.

```typescript
moveBlock(
  srcBlock: BlockUUID | BlockId,
  targetBlock: BlockUUID | BlockId,
  opts?: {
    before?: boolean;
    children?: boolean;
  }
): Promise<void>
```

#### insertBatchBlock

Insert multiple blocks at once.

```typescript
insertBatchBlock(
  srcBlock: BlockUUID | BlockId,
  blocks: Array<{
    content: string;
    properties?: Record<string, any>;
    children?: BlockEntity[];
  }>,
  opts?: { before?: boolean; sibling?: boolean }
): Promise<BlockEntity[]>
```

**Example**:

```javascript
await logseq.Editor.insertBatchBlock(
  pageUUID,
  [
    { content: 'Block 1' },
    {
      content: 'Block 2 with children',
      children: [{ content: 'Child 1' }, { content: 'Child 2' }],
    },
    { content: 'Block 3' },
  ],
  { isPageBlock: true }
)
```

### Block Properties

Signatures below are from `libs/src/LSPlugin.ts:918-930`.

#### getBlockProperty

Get a block property value.

```typescript
getBlockProperty(
  block: BlockIdentity | EntityID,
  key: string
): Promise<BlockEntity | null>
```

#### upsertBlockProperty

Insert or update a block property. There is no `setBlockProperty`; the host
export is `upsert_block_property` (`src/main/logseq/api.cljs:141`).

```typescript
upsertBlockProperty(
  block: BlockIdentity | EntityID,
  key: string,
  value: any,
  options?: Partial<{ reset: boolean }>
): Promise<void>
```

#### removeBlockProperty

Remove a block property.

```typescript
removeBlockProperty(
  block: BlockIdentity | EntityID,
  key: string
): Promise<void>
```

#### getBlockProperties

Get all properties of a block.

```typescript
getBlockProperties(
  block: BlockIdentity | EntityID
): Promise<Record<string, any> | null>
```

#### getPageProperties

```typescript
getPageProperties(
  page: PageIdentity | EntityID
): Promise<Record<string, any> | null>
```

### Page Operations

#### getCurrentPage

Get the currently focused page.

```typescript
getCurrentPage(): Promise<PageEntity | null>
```

**PageEntity structure**:

```typescript
interface PageEntity {
  id: number
  uuid: string
  name: string
  format: 'markdown' | 'org'
  type: 'page' | 'journal' | 'whiteboard' | 'class' | 'property' | 'hidden'
  createdAt: number
  updatedAt: number
  'journal?': boolean
  title?: string
  originalName?: string
  file?: IEntityID          // { id: number }
  namespace?: IEntityID
  properties?: Record<string, any>
  journalDay?: number
  children?: Array<PageEntity>
}
```

#### getPage

Get a page by name or UUID.

```typescript
getPage(
  srcPage: string | PageUUID,
  opts?: { includeChildren?: boolean }
): Promise<PageEntity | null>
```

#### getAllPages

Get all pages in the graph.

```typescript
getAllPages(repo?: string): Promise<PageEntity[]>
```

#### createPage

Create a new page.

```typescript
createPage(
  pageName: string,
  properties?: Record<string, any>,
  opts?: {
    redirect?: boolean;      // Navigate to page
    createFirstBlock?: boolean;
    format?: "markdown" | "org";
    journal?: boolean;
  }
): Promise<PageEntity>
```

**Example**:

```javascript
const page = await logseq.Editor.createPage(
  'New Page',
  { status: 'active', priority: 'high' },
  { redirect: true }
)
```

#### deletePage

Delete a page by name.

```typescript
deletePage(pageName: string): Promise<void>
```

#### renamePage

Rename a page.

```typescript
renamePage(
  oldName: string,
  newName: string
): Promise<void>
```

#### getPageBlocksTree

Get all blocks of a page as a tree.

```typescript
getPageBlocksTree(
  srcPage: string | PageUUID
): Promise<BlockEntity[]>
```

### Queries

#### getPageLinkedReferences

Get blocks that link to a page.

```typescript
getPageLinkedReferences(
  pageName: string
): Promise<[PageEntity, BlockEntity[]][]>
```

#### getPagesFromNamespace

Get pages in a namespace.

```typescript
getPagesFromNamespace(
  namespace: string
): Promise<PageEntity[]>
```

#### getPagesTreeFromNamespace

Get namespace pages as tree.

```typescript
getPagesTreeFromNamespace(
  namespace: string
): Promise<PageEntity[]>
```

### Commands

#### registerSlashCommand

Add custom slash command.

```typescript
registerSlashCommand(
  tag: string,
  action: BlockCommandCallback | Array<[string, BlockCommandCallback]>
): void
```

**Example**:

```javascript
logseq.Editor.registerSlashCommand('Insert Date', async () => {
  const block = await logseq.Editor.getCurrentBlock()
  const date = new Date().toISOString().split('T')[0]
  await logseq.Editor.insertBlock(block.uuid, `Date: ${date}`, {
    sibling: true,
  })
})
```

#### registerBlockContextMenuItem

Add item to block context menu (right-click).

```typescript
registerBlockContextMenuItem(
  label: string,
  action: BlockCommandCallback  // (e: IHookEvent & { uuid: BlockUUID }) => Promise<void>
): void
```

**Example**:

```javascript
logseq.Editor.registerBlockContextMenuItem('Copy Block UUID', async ({ uuid }) => {
  await navigator.clipboard.writeText(uuid)
  logseq.UI.showMsg('UUID copied!')
})
```

### Editing

#### editBlock

Enter edit mode on a block.

```typescript
editBlock(
  srcBlock: BlockUUID | BlockId,
  opts?: { pos?: number }  // Cursor position
): Promise<void>
```

#### exitEditingMode

Exit current edit mode.

```typescript
exitEditingMode(selectBlock?: boolean): Promise<void>
```

#### scrollToBlockInPage

Scroll to a block in a page.

```typescript
scrollToBlockInPage(
  pageName: string,
  blockId: BlockUUID | BlockId,
  opts?: { replaceState?: boolean }
): void
```

### Selection

#### getSelectedBlocks

Get currently selected blocks.

```typescript
getSelectedBlocks(): Promise<BlockEntity[] | null>
```

---

## logseq.DB

Database query API.

### q (DSL Query)

Run a DSL query (see [Logseq query docs](https://docs.logseq.com/#/page/queries)).

```typescript
q<T = any>(dsl: string): Promise<T>
```

**Example**:

```javascript
// Find all TODO blocks
const todos = await logseq.DB.q(`
  [:find (pull ?b [*])
   :where
   [?b :block/marker ?m]
   [(contains? #{"TODO" "DOING"} ?m)]]
`)

// Find pages with specific property
const projectPages = await logseq.DB.q(`
  [:find (pull ?p [*])
   :where
   [?p :block/properties ?props]
   [(get ?props :type) ?type]
   [(= ?type "project")]]
`)
```

### datascriptQuery

Run a raw Datascript query with inputs.

```typescript
datascriptQuery<T = any>(
  query: string,
  ...inputs: any[]
): Promise<T>
```

**Example**:

```javascript
const results = await logseq.DB.datascriptQuery(
  `
  [:find ?title
   :in $ ?marker
   :where
   [?b :block/marker ?marker]
   [?b :block/page ?p]
   [?p :block/name ?title]]
`,
  'TODO'
)
```

### Hooks

#### onChanged

Listen to any database changes.

```typescript
onChanged(callback: (event: {
  blocks: BlockEntity[];
  txData: IDatom[];
  txMeta?: { outlinerOp: string };
}) => void): void
```

**Example**:

```javascript
logseq.DB.onChanged(({ blocks, txData, txMeta }) => {
  console.log('Changed blocks:', blocks.length)
  console.log('Transaction type:', txMeta?.outlinerOp)
})
```

#### onBlockChanged

Listen to specific block changes.

```typescript
onBlockChanged(
  uuid: BlockUUID,
  callback: (block: BlockEntity, txData: IDatom[], txMeta?: any) => void
): () => void  // Returns unsubscribe function
```

---

## logseq.UI

User interface operations.

### showMsg

Display a toast notification.

```typescript
showMsg(
  content: string,
  status?: "success" | "warning" | "error" | string,
  opts?: {
    key?: string;
    timeout?: number;  // ms, 0 = no auto-dismiss
  }
): Promise<string>  // Returns message key
```

**Examples**:

```javascript
// Simple message
logseq.UI.showMsg('Hello!')

// Success with timeout
logseq.UI.showMsg('Saved!', 'success', { timeout: 3000 })

// Persistent error
const key = await logseq.UI.showMsg('Error!', 'error', { timeout: 0 })
// Later: logseq.UI.closeMsg(key);
```

### closeMsg

Close a message by key.

```typescript
closeMsg(key: string): void
```

### queryElementRect

Get dimensions of an element.

```typescript
queryElementRect(selector: string): Promise<DOMRectReadOnly | null>
```

### resolveThemeCssPropsVals

Get computed CSS variable values.

```typescript
resolveThemeCssPropsVals(
  props: string | string[]
): Promise<Record<string, string | undefined> | null>
```

**Example**:

```javascript
const colors = await logseq.UI.resolveThemeCssPropsVals([
  '--ls-primary-background-color',
  '--ls-primary-text-color',
])
```

---

## logseq.Assets

Asset and file management.

### listFilesOfCurrentGraph

List all asset files.

```typescript
listFilesOfCurrentGraph(
  exts?: string | string[]  // e.g., ".pdf" or [".pdf", ".png"]
): Promise<Array<{
  path: string;
  size: number;
  accessTime: number;
  modifiedTime: number;
  changeTime: number;
  birthTime: number;
}>>
```

### builtInOpen

Open an asset with default handler.

```typescript
builtInOpen(path: string): Promise<boolean | undefined>
```

### makeSandboxStorage

Create sandboxed storage for plugin.

```typescript
makeSandboxStorage(): IAsyncStorage
```

**Example**:

```javascript
const storage = logseq.Assets.makeSandboxStorage()
await storage.setItem('key', 'value')
const value = await storage.getItem('key')
```

---

## logseq.Git

Git integration (file-based graphs only).

### execCommand

Execute a git command.

```typescript
execCommand(args: string[]): Promise<IGitResult>

// IGitResult:
// {
//   stdout: string;
//   stderr: string;
//   exitCode: number;
// }
```

**Example**:

```javascript
const result = await logseq.Git.execCommand(['status', '--porcelain'])
console.log(result.stdout)
console.log('Exit code:', result.exitCode)
```

### loadIgnoreFile

Load `.gitignore` contents.

```typescript
loadIgnoreFile(): Promise<string>
```

### saveIgnoreFile

Save `.gitignore` contents.

```typescript
saveIgnoreFile(content: string): Promise<void>
```

---

## Plugin Settings

### useSettingsSchema

Define plugin settings schema.

```typescript
logseq.useSettingsSchema([
  {
    key: 'option1',
    type: 'string',
    default: 'default value',
    title: 'Option 1',
    description: 'Description of option 1',
  },
  {
    key: 'enabled',
    type: 'boolean',
    default: true,
    title: 'Enable Feature',
  },
  {
    key: 'count',
    type: 'number',
    default: 10,
    title: 'Count',
    description: 'Number of items',
  },
  {
    key: 'choice',
    type: 'enum',
    default: 'option1',
    enumChoices: ['option1', 'option2', 'option3'],
    title: 'Choose Option',
  },
])
```

### Access Settings

```javascript
// Get all settings
const settings = logseq.settings

// Get specific setting
const enabled = logseq.settings?.enabled

// Update settings
logseq.updateSettings({ enabled: false })
```

---

## UI Injection

### provideUI

Inject custom UI into slots.

```typescript
logseq.provideUI({
  key: string;
  slot: string;
  template: string;
  style?: Record<string, string>;
  attrs?: Record<string, string>;
  reset?: boolean;
});
```

### provideStyle

Inject CSS styles.

```typescript
logseq.provideStyle(`
  .my-plugin-class {
    color: var(--ls-primary-text-color);
  }
`)
```

### provideModel

Register callback handlers.

```typescript
logseq.provideModel({
  onButtonClick() {
    console.log('Button clicked!')
  },
})
```

---

## Sandbox Modes

### iframe Mode (Default)

- Strong isolation (separate window context)
- Cross-origin security
- Higher IPC overhead
- Set in `package.json`: `"logseq": { "mode": "iframe" }`

### shadow Mode

- Same window context
- Faster execution
- Limited isolation
- Set in `package.json`: `"logseq": { "mode": "shadow" }`

**package.json configuration**:

```json
{
  "logseq": {
    "id": "my-plugin",
    "main": "dist/index.js",
    "mode": "iframe"
  }
}
```

---

## Error Handling

### Try-Catch Pattern

```javascript
try {
  const page = await logseq.Editor.getPage('nonexistent')
  if (!page) {
    logseq.UI.showMsg('Page not found', 'warning')
  }
} catch (error) {
  logseq.UI.showMsg(`Error: ${error.message}`, 'error')
  console.error(error)
}
```

### Timeout Behavior

- API calls timeout after **10 seconds**
- Hooks are not rate-limited

---

## Key Type Definitions

Transcribed from `libs/src/LSPlugin.ts`.

### BlockEntity

`libs/src/LSPlugin.ts:187-214`

```typescript
export interface BlockEntity {
  id: EntityID // db id
  uuid: BlockUUID
  order: string
  format: 'markdown' | 'org'
  parent: IEntityID
  title: string
  fullTitle: string // block refs replaced with their title text
  content?: string // @deprecated -- use title
  page: IEntityID // owner page
  createdAt: number
  updatedAt: number
  ident?: string // ident for a property block
  properties?: Record<string, any>
  'collapsed?': boolean

  // optional fields on a dummy page
  anchor?: string
  body?: any
  children?: Array<BlockEntity | BlockUUIDTuple>
  container?: string
  file?: IEntityID
  level?: number
  meta?: { timestamps: any; properties: any; startPos: number; endPos: number }
  marker?: string

  [key: string]: unknown
}
```

### PageEntity

`libs/src/LSPlugin.ts:219-239`. A page is a block with some specific fields.

```typescript
export interface PageEntity {
  id: EntityID
  uuid: BlockUUID
  name: string
  format: 'markdown' | 'org'
  type: 'page' | 'journal' | 'whiteboard' | 'class' | 'property' | 'hidden'
  updatedAt: number
  createdAt: number
  'journal?': boolean

  title?: string
  file?: IEntityID
  originalName?: string
  namespace?: IEntityID
  children?: Array<PageEntity>
  properties?: Record<string, any>
  journalDay?: number // YYYYMMDD
  ident?: string

  [key: string]: unknown
}
```

### PropertySchema

`libs/src/LSPlugin.ts:366-371`

```typescript
export type PropertySchema = {
  type: 'default' | 'number' | 'node' | 'date' | 'checkbox' | 'url' | string
  cardinality: 'many' | 'one'
  hide: boolean
  public: boolean
}
```

---

## Findings: New Capabilities

Present in the SDK at `be800f1711` but not yet given full sections above.

### logseq.Commands -- unified command bus

`libs/src/LSPlugin.ts:615-637`. Wraps palette commands, shortcuts, slash
commands and context-menu entries behind one registration call. `when` is
stored as command metadata for future host-side evaluation.

```typescript
register(
  id: string,
  options: CommandRegisterOptions,
  action?: CommandCallback | BlockCommandCallback | Array<SlashCommandAction>
): CommandUnregister | false

// Built-in commands use `logseq.*` ids; plugin commands can be addressed as
// `plugin-id/key`, `plugin-id.commands.key`, or a local command key.
execute(id: string, ...args: Array<any>): Promise<unknown>
```

### DB-graph property and tag entity APIs

On `logseq.Editor` (`libs/src/LSPlugin.ts:849-915`), backed by
`src/main/logseq/api.cljs:206-224`. These operate on property and tag
*entities*, which only exist in DB graphs.

```typescript
// properties
getProperty(key: string): Promise<BlockEntity | null>
upsertProperty(key: string, schema?: Partial<PropertySchema>, opts?: { name?: string }): Promise<IEntityID>
removeProperty(key: string): Promise<void>
getAllProperties(): Promise<PageEntity[] | null>
addPropertyValueChoices(propertyId: BlockIdentity, choices: Array<BlockIdentity>): Promise<void>

// tags (classes)
getAllTags(): Promise<PageEntity[] | null>
getTag(nameOrIdent: string | EntityID): Promise<PageEntity | null>
getTagsByName(tagName: string): Promise<Array<PageEntity> | null>
getTagObjects(nameOrIdent: string): Promise<BlockEntity[] | null>
createTag(...): Promise<PageEntity | null>
addTagProperty(tagId: BlockIdentity, propertyIdOrName: BlockIdentity): Promise<void>
removeTagProperty(tagId: BlockIdentity, propertyIdOrName: BlockIdentity): Promise<void>
addTagExtends(tagId: BlockIdentity, parentTagIdOrName: BlockIdentity): Promise<void>
removeTagExtends(tagId: BlockIdentity, parentTagIdOrName: BlockIdentity): Promise<void>
addBlockTag(blockId: BlockIdentity, tagId: BlockIdentity): Promise<void>
removeBlockTag(blockId: BlockIdentity, tagId: BlockIdentity): Promise<void>
setBlockIcon(blockId: BlockIdentity, iconType: 'tabler-icon' | 'emoji', iconName: string): Promise<void>
```

### App hooks not documented above

`libs/src/LSPlugin.ts:561-605`: `onGraphAfterIndexed`, `onThemeChanged`,
`onTodayJournalCreated`, `onBeforeCommandInvoked`, `onAfterCommandInvoked`,
`onPageHeadActionsSlotted`, `onSidebarVisibleChanged`.

### Template APIs

`libs/src/LSPlugin.ts:532-541`: `getTemplate`, `existTemplate`,
`createTemplate`, `removeTemplate`, `insertTemplate`.

---

## Best Practices

1. **Always check return values** - Methods may return `null`
2. **Use batch operations** when modifying multiple blocks
3. **Clean up hooks** in `beforeunload`
4. **Handle async properly** - All API calls are async
5. **Use settings schema** for user preferences
6. **Test in both graph types** - File and DB graphs

---

## See Also

- **HTTP API**: [http-api.md](http-api.md) for external integration
- **MCP Server**: [mcp-server.md](mcp-server.md) for AI integration
- **Plugin SDK**: [@logseq/libs](https://github.com/logseq/logseq-plugin-samples)

---

_Last updated: 2026-09-09 | [Report an issue](https://github.com/logseq/logseq/issues)_
