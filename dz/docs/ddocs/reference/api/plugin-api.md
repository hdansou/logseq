---
title: 'Plugin API Reference'
description: "Reference for Logseq's Plugin SDK (@logseq/libs) as implemented by the host for DB graphs"
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop]
graphType: [db]
sinceVersion: '0.4.0'
relatedDocs:
  tutorials:
    - '[Your First Plugin](../../tutorials/plugins/first-plugin.md)'
  explanation:
    - '[Plugin Architecture](../../explanation/concepts/plugin-architecture.md)'
    - '[Plugin Security](../../explanation/architecture/plugin-security.md)'
  reference:
    - '[HTTP API Reference](http-api.md)'
---

# Plugin API Reference

> **Last revised:** 2026-09-11

> Reference for the Logseq Plugin SDK: the methods a plugin can call, what the host actually does with
> them, and which declared methods have no implementation.

## Overview

The Plugin API provides programmatic access to Logseq through the global `logseq` object. Key capabilities:

- **Content manipulation**: Create, read, update, delete blocks and pages
- **UI integration**: Commands, toolbars, sidebars, modals
- **Database queries**: Run simple (DSL) and Datalog queries on the graph
- **Event hooks**: React to changes in blocks, pages, themes
- **Asset management**: Work with files and attachments

Everything here was checked against `be800f1711` (app version `2.0.1`, `src/main/frontend/version.cljs:3`;
SDK `@logseq/libs` `0.3.4`, `libs/package.json:4`). Only DB graphs can be opened in this version
(`src/main/frontend/components/repo.cljs:321-323`). Plugins load in Electron and in non-publishing web
builds, not on native mobile (`plugin-platform?`, `src/main/frontend/util.cljc:131`); APIs that go through
Electron IPC or `window.apis` work on desktop only.

### How a call reaches the host

A method being declared in `libs/src/LSPlugin.ts` does not mean it works. Each call resolves like this:

1. **SDK-local handler.** `_makeUserProxy` (`libs/src/LSPlugin.user.ts:1002-1077`) first runs a handler
   implemented inside the SDK, if one exists (the `app`, `editor`, `db`, `assets` objects at `:401-717`).
2. **Hooks.** Names starting with `on`, `off` or `once` become hook subscriptions (`:1017-1059`).
3. **Host call.** Anything else is sent to the host as `api:call` (`:1069`). Only the `git`, `ui`, `assets`
   and `utils` namespaces prefix the method name (`:1064-1066`).
4. **Host lookup.** `invokeHostExportedApi` (`libs/src/common.ts:301-331`) snake-cases the name. If its
   first segment is a `window.logseq.sdk` namespace (the `^:export` fns in
   `src/main/logseq/sdk/{assets,core,debug,experiments,ui,utils}.cljs`), it looks there. Otherwise it looks
   in `window.logseq.api` (the `^:export` defs in `src/main/logseq/api.cljs`), then in Electron's preload
   `window.apis` (`resources/js/preload.js:36`), trying the camelCase name last (`common.ts:322-326`). A
   miss throws `Not existed method #…` (`:328-330`).

A declared method with no SDK-local handler, no host export and no preload key throws. Such methods are
marked **Not implemented** below.

---

## Quick Reference

| Namespace         | Purpose              | Key Methods                                       | Interface (`libs/src/LSPlugin.ts`) |
| ----------------- | -------------------- | ------------------------------------------------- | ---------------------------------- |
| `logseq.App`      | App-level operations | `registerCommand`, `pushState`, `getCurrentGraph` | `IAppProxy` (404)                  |
| `logseq.Commands` | Unified command bus  | `register`, `execute`                             | `ICommandsProxy` (615)             |
| `logseq.Editor`   | Content manipulation | `insertBlock`, `getPage`, `createPage`            | `IEditorProxy` (642)               |
| `logseq.DB`       | Database queries     | `q`, `datascriptQuery`, `onChanged`               | `IDBProxy` (956)                   |
| `logseq.Git`      | **Not implemented**  | `execCommand` (throws)                            | `IGitProxy` (1010)                 |
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

`ready(model?, callback?)` (`libs/src/LSPlugin.user.ts:802-852`) takes an optional model object. Its
methods are the handlers that injected UI calls, for example through `data-on-click`. To react to
settings changes, use `logseq.onSettingsChanged` (`LSPlugin.user.ts:903`). A model method named
`onSettingsChanged` is not a settings hook.

```javascript
logseq.ready(
  {
    onButtonClick() {
      logseq.UI.showMsg('Button clicked')
    },
  },
  () => {
    console.log('Plugin ready with model')
  }
)

logseq.onSettingsChanged((newSettings, oldSettings) => {
  console.log('Settings updated:', newSettings)
})
```

---

## logseq.App

Application-level operations and registrations.
Interface: `libs/src/LSPlugin.ts:404-610` (`IAppProxy`).

### App Info

#### getInfo

Get Logseq application information. It is SDK-local and cached after the first call
(`libs/src/LSPlugin.user.ts:402-407`).

```typescript
getInfo(key?: keyof AppInfo): Promise<AppInfo | any>

// AppInfo: { version: string; supportDb: boolean }
// -- src/main/logseq/api/app.cljs:46-52 (version is the app version; supportDb is always true)
```

> `getInfo` is implemented in the JS SDK, not as a host export, so the HTTP API cannot reach it. The server
> maps `logseq.App.getInfo` to `app@get_info` (`src/electron/electron/server.cljs:73-81`), and the renderer
> looks that up on `window.logseq.api` (`src/main/electron/listener.cljs:113-122`), which has no `get_info`.
> Use `logseq.App.getAppInfo` there (`get_app_info`, `src/main/logseq/api.cljs:69`).

#### getUserInfo

> **Not implemented.** Declared at `LSPlugin.ts:411`. Nothing implements it: there is no SDK-local handler
> (`LSPlugin.user.ts:401-561`), no `get_user_info` export in `src/main/logseq/api.cljs`, and no `getUserInfo`
> key in `resources/js/preload.js`. The call throws `Not existed method` (`libs/src/common.ts:328-330`).

```typescript
getUserInfo(): Promise<AppUserInfo | null>
```

#### checkCurrentIsDbGraph

Whether the active graph is a DB graph. Host: `config/db-based-graph?` (`src/main/logseq/api.cljs:66`;
the zero-arity form reads the current repo, `src/main/frontend/config.cljs:314-316`).

```typescript
checkCurrentIsDbGraph(): Promise<Boolean>
```

#### getStateFromStore / setStateFromStore

Read and write paths of the frontend state atom (`src/main/logseq/api/app.cljs:24-44`). Path segments are
keywordized; a segment starting with `@` is used as a plain string. Atom values are dereferenced on read.

```typescript
getStateFromStore<T = any>(path: string | Array<string>): Promise<T>
setStateFromStore(path: string | Array<string>, value: any): Promise<void>
```

### Command Registration

#### registerCommandPalette

Add a command to the command palette (built-in binding `mod+shift+p`,
`src/main/frontend/modules/shortcut/config.cljs:310`). SDK-local; returns an unregister function
(`LSPlugin.user.ts:441-451`).

```typescript
registerCommandPalette(
  opts: { key: string; label: string; keybinding?: SimpleCommandKeybinding },
  action: SimpleCommandCallback
): CommandUnregister | false
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

Register a named command with full options. `type` becomes the command group. SDK-local
(`LSPlugin.user.ts:409-428`); `opts` also accepts `extras`.

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
): CommandUnregister | false
```

#### registerCommandShortcut

Bind a keyboard shortcut to an action. SDK-local (`LSPlugin.user.ts:453-475`). A string keybinding is
normalized to `{ mode: 'global', binding }` (`:144-155`).

```typescript
registerCommandShortcut(
  keybinding: SimpleCommandKeybinding | string,
  action: SimpleCommandCallback,
  opts?: { key?: string; label?: string; desc?: string; extras?: Record<string, any> }
): CommandUnregister | false

// SimpleCommandKeybinding (LSPlugin.ts:288-293):
// { mode?: 'global' | 'non-editing' | 'editing'; binding: string | string[]; mac?: string }
```

**Keybinding format**: bindings use the same string form as Logseq's built-in shortcuts, such as
`mod+shift+p` (`shortcut/config.cljs:310`). The key names Logseq supports are listed in the gist linked
from `LSPlugin.ts:440-441`.

**Examples**: `"mod+shift+a"`, `"mod+enter"`

### UI Item Registration

#### registerUIItem

Add items to the toolbar or page bar. Host: `register_plugin_ui_item` (`src/main/logseq/api.cljs:59`,
`src/main/logseq/api/plugin.cljs:390-394`).

```typescript
registerUIItem(
  type: "toolbar" | "pagebar",
  opts: {
    key: string;
    template: string;
  }
): void
```

**Example** (`onButtonClick` is a method provided with `provideModel` or `ready(model)`):

```javascript
logseq.App.registerUIItem('toolbar', {
  key: 'my-button',
  template: `<a class="button" data-on-click="onButtonClick">Click Me</a>`,
})
```

#### registerPageMenuItem

Add an item to the page menu. SDK-local (`LSPlugin.user.ts:490-512`). The callback receives `page`, the
page name (`src/main/frontend/components/page_menu.cljs:153-156`).

> Declared to return `CommandUnregister | false` (`LSPlugin.ts:554-557`), but the handler does not return
> the result of `registerCommand.call` (`LSPlugin.user.ts:502`), so it returns `undefined`. You get no
> unregister function.

```typescript
registerPageMenuItem(
  tag: string,
  action: (e: IHookEvent & { page: string }) => void
): CommandUnregister | false
```

**Example**:

```javascript
logseq.App.registerPageMenuItem('Export Page', async (e) => {
  const page = await logseq.Editor.getPage(e.page)
  console.log('Exporting page:', page?.name)
})
```

### Navigation

#### pushState

Navigate to a route (`src/main/logseq/api/app.cljs:153-162`). For `page`, the host redirects to
`params.name` and honours `query.anchor`. Other route names go to the router as keywords.

```typescript
pushState(
  route: string,  // router name, e.g. "page", "home", "all-pages", "graph" (src/main/frontend/routes.cljs:30-50)
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

Replace the current route without adding to history (`app.cljs:164-172`).

```typescript
replaceState(
  route: string,
  params?: Record<string, any>,
  query?: Record<string, any>
): void
```

### Graph Operations

#### getCurrentGraph

Get information about the current graph (`src/main/logseq/api/app.cljs:92-98`). Resolves `null` when no
graph is open or the demo graph is active.

```typescript
getCurrentGraph(): Promise<AppGraphInfo | null>

// { url: <repo id>, name: <basename of repo id>, path: <config/get-repo-dir of repo> }
```

#### getCurrentGraphConfigs

Get graph configuration values (`app.cljs:68-72`). With no keys it returns the whole config. With keys it
returns the value at that path, with each key keywordized.

```typescript
getCurrentGraphConfigs(...keys: string[]): Promise<any>
```

#### setCurrentGraphConfigs

Update graph configuration. Each top-level key is written with `config-handler/set-config!`
(`app.cljs:74-79`).

```typescript
setCurrentGraphConfigs(configs: Record<string, any>): Promise<void>
```

#### getCurrentGraphFavorites / getCurrentGraphRecent / getCurrentGraphTemplates

```typescript
getCurrentGraphFavorites(): Promise<Array<string | PageEntity> | null>
getCurrentGraphRecent(): Promise<Array<string | PageEntity> | null>
getCurrentGraphTemplates(): Promise<Record<string, BlockEntity> | null>
```

- `getCurrentGraphFavorites` (`api.cljs:73`, `app.cljs:81-83`) resolves page **objects**, not strings. They
  are the pages linked from the favorites page, in order, skipping recycled pages
  (`src/main/frontend/worker/handler/graph.cljs:34-44`).
- `getCurrentGraphRecent` (`api.cljs:74`, `app.cljs:85-90`) resolves page objects too, at most 20, pages
  only, hidden pages removed (`worker/handler/graph.cljs:51-61`).
- `getCurrentGraphTemplates` is **not implemented**; see [Template APIs](#template-apis).

### External Commands & Plugins

#### invokeExternalCommand

Invoke a built-in palette command by id (`src/main/logseq/api/app.cljs:121-128`). The host does nothing
unless `type` starts with `logseq.`; the rest is lower-cased and looked up among palette commands. Known ids
are listed in `ExternalCommandType` (`LSPlugin.ts:315-352`).

```typescript
invokeExternalCommand(type: ExternalCommandType, ...args: Array<any>): Promise<void>
```

#### invokeExternalPlugin

Call a command or model exposed by another plugin. SDK-local (`LSPlugin.user.ts:529-548`); it throws unless
the group is `models` or `commands`. Host: `invoke_external_plugin_cmd`
(`src/main/logseq/api/plugin.cljs:401-408`).

```typescript
// type: `xx-plugin-id.commands.xx-key` or `xx-plugin-id.models.xx-key`
invokeExternalPlugin(type: string, ...args: Array<any>): Promise<unknown>
```

#### getExternalPlugin

Look up another installed plugin by id. Resolves the plugin instance's `toJSON()`, or `null` if absent
(`plugin.cljs:396-399`).

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

- `relaunch`/`quit` go through Electron IPC (`src/main/logseq/api/app.cljs:108-114`).
- `openExternalLink` does nothing unless the string contains `http://` or `https://` (unanchored
  `re-find`, `app.cljs:116-119`). It then calls `window.apis.openExternal`, which checks the protocol
  against `ALLOWED_EXTERNAL_PROTOCOLS` (`resources/js/preload.js:9`).
- `setZoomFactor` has no host export. It resolves through the preload fallback to
  `window.apis.setZoomFactor` (`libs/src/common.ts:326`, `resources/js/preload.js:154-156`), so it works
  on desktop only.
- `setFullScreen` is SDK-local (`LSPlugin.user.ts:550-560`). It calls `_callMainWin`
  (`resources/js/preload.js:136-138`, IPC channel `call-main-win` in
  `src/electron/electron/core.cljs:167`), so it is desktop only.
- The sidebar methods are in `app.cljs:131-151`.

> **`execGitCommand` is not implemented.** Declared as deprecated (`LSPlugin.ts:499-504`). The
> `exec_git_command` export was removed from `api.cljs` in `bcc478b5f7`, and nothing replaces it.

### User Preferences

#### getUserConfigs

Get user settings. The declared `AppUserConfigs` (`LSPlugin.ts:157-171`) does not match what the host
returns. The host object (`src/main/logseq/api/app.cljs:54-66`) has exactly these keys:

```typescript
getUserConfigs(): Promise<AppUserConfigs>

// Returned by the host:
// {
//   preferredLanguage: string;
//   preferredThemeMode: "dark" | "light";
//   preferredFormat: string;
//   preferredDateFormat: string;
//   preferredStartOfWeek: ...;
//   currentGraph: string;
//   showBrackets: boolean;      // the type declares `showBracket`
//   enabledFlashcards: boolean;
//   me: ...;
// }
```

The declared `preferredWorkflow`, `showBracket` and `enabledJournals` are never returned.

### Hooks

Every `on*` hook returns an unregister function (`LSPlugin.user.ts:1043-1057`).

#### onCurrentGraphChanged

Triggered when the user switches graphs (emitted at `src/main/frontend/components/theme.cljs:23`).

```javascript
logseq.App.onCurrentGraphChanged(() => {
  console.log('Graph changed!')
})
```

#### onThemeModeChanged

Triggered when the theme mode changes (`theme.cljs:58`, payload `{ mode }`).

```javascript
logseq.App.onThemeModeChanged(({ mode }) => {
  console.log('Theme mode:', mode) // "dark" or "light"
})
```

#### onBlockRendererSlotted

Provide a UI slot for a specific block by UUID. It is SDK-local (`LSPlugin.user.ts:514-527`) and installs
the hook `slot:<uuid>`. The block renders a slot only while such a hook exists
(`src/main/frontend/components/block.cljs:3432`, `:2787-2792`). The callback gets `{ type, slot, payload }`
merged with the block's fields (`src/main/frontend/components/plugins.cljs:1429-1431`,
`src/main/frontend/handler/plugin.cljs:975-978`).

```typescript
onBlockRendererSlotted(
  uuid: BlockUUID,
  callback: (e: IHookEvent & { slot: string } & Omit<BlockEntity, 'children' | 'page'>) => void
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

Register a custom renderer for `{{renderer arg1, arg2}}` macros. In DB graphs the host still renders the
slot for `renderer` macros (`src/main/frontend/components/block.cljs:1920-1923`). The payload is the macro
options, which include `arguments` (`block.cljs:1860-1862`), plus the block `uuid`.

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

Triggered on navigation, with payload `{ template, path, parameters }` (`src/main/frontend/core.cljs:34-35`).

```javascript
logseq.App.onRouteChanged(({ path, template }) => {
  console.log('Route:', path)
})
```

---

## logseq.Editor

Content manipulation API for blocks and pages.
Interface: `libs/src/LSPlugin.ts:642-951`. Host implementations: `src/main/logseq/api/editor.cljs`,
`src/main/logseq/api/block.cljs` and `src/main/logseq/api/db_based.cljs`.

### Block Operations

#### getCurrentBlock

Get the block being edited. If nothing is being edited, the host falls back to the first selected block or
the editor's block container (`src/main/logseq/api/editor.cljs:319-328`). `opts` is passed on to `getBlock`.

```typescript
getCurrentBlock(opts?: Partial<{ includeChildren: boolean }>): Promise<BlockEntity | null>
```

#### getBlock

Get a block by UUID or entity id (`editor.cljs:314-317`, `src/main/logseq/api/block.cljs:225-253`).

```typescript
getBlock(
  srcBlock: BlockIdentity | EntityID,
  opts?: Partial<{ includeChildren: boolean }>
): Promise<BlockEntity | null>
```

- **Pages.** With no `opts`, the host defaults to `{ includePage: true }` (`editor.cljs:317`) and returns
  pages. If you pass `opts` without `includePage: true` (an undeclared option), a page resolves `null`
  (`block.cljs:243-245`).
- **Children.** Without `includeChildren`, `children` holds `["uuid", "<uuid>"]` tuples for the immediate
  children (`block.cljs:249-251`). With it, `children` is the nested tree (`:246-248`).
- **Refs.** `refs` are compacted to `{ id }` (`block.cljs:216-223`).

The result's shape is described under [Key Type Definitions](#key-type-definitions).

#### insertBlock

Insert a new block (`src/main/logseq/api/editor.cljs:233-268`).

```typescript
insertBlock(
  srcBlock: BlockIdentity | EntityID,   // a block, or a page by name or UUID
  content: string,
  opts?: Partial<{
    before: boolean;       // with sibling: before the target; without: as first child
    sibling: boolean;      // insert as sibling (default: child)
    start: boolean;
    end: boolean;
    customUUID: string;    // throws if a block with this UUID exists
    properties: {};        // see Block Properties for key resolution
  }>
): Promise<BlockEntity | null>
```

The host reads only `before`, `start`, `end`, `sibling`, `customUUID`, `properties`, `autoOrderedList` and
`schema` (`editor.cljs:238`).

- **Ignored options.** `isPageBlock` and `focus` are not read. The block is never opened for editing
  (`:265`).
- **Default position.** With neither `sibling` nor `start`, the new block goes after the target's last
  child, which appends it as the last child (`src/main/frontend/handler/editor.cljs:748-749`, `:778-779`).
- **`before` without `sibling`.** The block becomes the target's first child (`editor.cljs:245-256`).
- **Custom UUID.** `customUUID` (or `properties.id`) sets the block UUID; an existing UUID throws
  (`:239-244`).

**Examples**:

```javascript
// Append as the last child of a block
const newBlock = await logseq.Editor.insertBlock(
  currentBlock.uuid,
  'New child block'
)

// Insert as sibling before
await logseq.Editor.insertBlock(currentBlock.uuid, 'Before sibling', {
  sibling: true,
  before: true,
})

// Append to a page (target may be a page name or UUID)
await logseq.Editor.insertBlock('My Page', 'Last block on page')
```

#### updateBlock

Update block content (`editor.cljs:289-296`, `src/main/logseq/api/db_based.cljs:93-109`). `properties`
are saved first, with the same key resolution as [Block Properties](#block-properties); then the content
is saved.

```typescript
updateBlock(
  srcBlock: BlockIdentity | EntityID,
  content: string,
  opts?: Partial<{ properties: {} }>
): Promise<void>
```

#### removeBlock

Delete a block (`editor.cljs:281-287`).

```typescript
removeBlock(srcBlock: BlockIdentity | EntityID): Promise<void>
```

#### moveBlock

Move a block to a new location (`editor.cljs:298-312`). `before` maps to the `:top` drop position and
`children` to `:nested`; `before` wins if both are set.

```typescript
moveBlock(
  srcBlock: BlockIdentity,
  targetBlock: BlockIdentity,
  opts?: Partial<{
    before: boolean;
    children: boolean;
  }>
): Promise<void>
```

#### insertBatchBlock

Insert multiple blocks at once (`editor.cljs:270-279`, `src/main/logseq/api/db_based.cljs:48-81`).

```typescript
insertBatchBlock(
  srcBlock: BlockIdentity,              // a block, or a page by name or UUID
  batch: IBatchBlock | Array<IBatchBlock>,
  opts?: Partial<{ before: boolean; sibling: boolean; keepUUID: boolean }>
): Promise<Array<BlockEntity> | null>

// IBatchBlock (LSPlugin.ts:129-138):
// { content: string; properties?: Record<string, any>; children?: Array<IBatchBlock> }
```

- **Options read.** Only `sibling`, `before` and `schema` (`db_based.cljs:56`); `keepUUID` is ignored.
- **Page targets.** When the target is a page, `sibling` is forced to `false` (`:66`).
- **Properties.** `properties` are applied in DB graphs (`:73-79`), despite the `@NOTE: not supported
  for DB graph` comment on `IBatchBlock` (`LSPlugin.ts:132-135`).
- **Return.** A flat list of the inserted blocks (`:80-81`).

**Example**:

```javascript
await logseq.Editor.insertBatchBlock(
  'My Page',
  [
    { content: 'Block 1' },
    {
      content: 'Block 2 with children',
      children: [{ content: 'Child 1' }, { content: 'Child 2' }],
    },
    { content: 'Block 3' },
  ]
)
```

### Block Properties

Signatures below are from `libs/src/LSPlugin.ts:918-930`. In DB graphs a property is an entity with a
`:db/ident`, and the key you pass is resolved to that ident (`src/main/logseq/api/block.cljs:20-57`):

- **Sanitizing.** The key is trimmed, spaces are removed, and leading `:`/`_` are stripped (`:20-26`).
- **Qualified keys** are used as the ident unchanged, for example `"user.property/rating-AbCd1234"` or
  `":logseq.property/status"` (`:52-54`).
- **Bare keys** resolve to the calling plugin's namespace (`:17`, `:35-42`, `:55-57`): `"rating"` becomes
  `:plugin.property.<plugin-id>/rating`. It does **not** mean the user's `rating` property or a built-in
  one. To read or write a user or built-in property, pass its full ident.
- **Creating properties.** Writing a key that does not exist creates the property, but only inside the
  plugin's own namespace. Any other ident throws "Plugins can only upsert its own properties"
  (`:92-99`, `:169-171`). Existing properties in any namespace can be set.
- **Inferred type.** A new property's type comes from the value: boolean → `checkbox`, number(s) →
  `number`, URL(s) → `url`, object → `json`, anything else → `default` (`:113-122`). An array value makes
  it cardinality `many` (`:138-143`).

#### getBlockProperty

Get one property value of a block (`src/main/logseq/api/editor.cljs:496-516`).

```typescript
getBlockProperty(
  block: BlockIdentity | EntityID,
  key: string
): Promise<BlockEntity | null>
```

- **Where it looks.** The block's own properties merged with default-valued properties from its tags
  (`:476-489`). The lookup is by resolved ident, so a bare key only finds the plugin's own property
  (`:501-505`).
- **Ref values.** An entity value comes back as an object with extra `value` and `ident` fields
  (`:506-511`). The declared `BlockEntity` return type only fits this case.
- **Many-valued properties** come back as arrays (`:491-494`).
- **JSON.** A string value of a plugin `json` property is JSON-parsed (`block.cljs:79-90`).

#### upsertBlockProperty

Insert or update a block property (`editor.cljs:454-465`, `src/main/logseq/api/db_based.cljs:189-195`).
There is no `setBlockProperty`; the host export is `upsert_block_property` (`src/main/logseq/api.cljs:141`).
`reset: true` replaces the existing values of a many-valued property instead of adding to them
(`block.cljs:173-181`).

```typescript
upsertBlockProperty(
  block: BlockIdentity | EntityID,
  key: string,
  value: any,
  options?: Partial<{ reset: boolean }>
): Promise<void>
```

> The host also reads an undeclared `schema` option, but it never takes effect. `upsert-block-property`
> builds `{key schema}` with `key` unbound (the parameter is `key'`), so the map is keyed by
> `cljs.core/key` (`db_based.cljs:190-193`). The lookup `(get schema k)` by property name
> (`block.cljs:188`) therefore finds nothing. To control a new property's type or cardinality, create it
> first with `upsertProperty`.

#### removeBlockProperty

Remove a block property (`editor.cljs:467-474`). The key resolves as described above.

```typescript
removeBlockProperty(
  block: BlockIdentity | EntityID,
  key: string
): Promise<void>
```

#### getBlockProperties

Get all properties of a block (`editor.cljs:518-522`): its own plus tag defaults, as for `getBlockProperty`.

```typescript
getBlockProperties(
  block: BlockIdentity | EntityID
): Promise<Record<string, any> | null>
```

- **Keys** are ident strings with a leading colon, such as `":user.property/rating-AbCd1234"`
  (`block.cljs:73-77`).
- **Values** are dereferenced to readable content (`src/main/frontend/handler/db_based/property/util.cljs:12-37`).
- **Many-valued properties** are sets on the host, and `bean/->js` passes them to `clj->js` (cljs-bean
  1.9.0), which turns them into arrays.

#### getPageProperties

Same as `getBlockProperties`, applied to the page entity (`editor.cljs:524-528`).

```typescript
getPageProperties(
  page: PageIdentity | EntityID
): Promise<Record<string, any> | null>
```

### Page Operations

#### getCurrentPage

Get the page for the current route (`src/main/logseq/api/editor.cljs:105-110`). Resolves `null` when the
route is not a page; on a zoomed-in block route it resolves that block.

```typescript
getCurrentPage(): Promise<PageEntity | BlockEntity | null>
```

The result's shape is described under [Key Type Definitions](#key-type-definitions).

#### getPage

Get a page by name or UUID (`editor.cljs:112-116`).

```typescript
getPage(
  srcPage: PageIdentity | EntityID,
  opts?: Partial<{ includeChildren: boolean }>
): Promise<PageEntity | null>
```

> The host function takes one argument, so `opts.includeChildren` is ignored. It also does not check that
> the entity found is a page.

#### getAllPages

Get all pages in the graph (`editor.cljs:124-136`).

```typescript
getAllPages(repo?: string): Promise<PageEntity[] | null>
```

The host takes no arguments, so `repo` is ignored. It returns every non-hidden entity with a
`:block/name`, sorted by title, with only `id`, `uuid`, `name`, `title`, `createdAt` and `updatedAt`.

#### createPage

Create a page, or return the existing one (`src/main/logseq/api/editor.cljs:138-161`).

```typescript
createPage(
  pageName: BlockPageName,
  properties?: {},
  opts?: Partial<{
    redirect: boolean;         // defaults to TRUE
    createFirstBlock: boolean; // ignored by the host
    customUUID: string;
    format: 'markdown' | 'org';
    journal: boolean;
  }>
): Promise<PageEntity | null>
```

The host reads `redirect`, `format`, `journal`, `schema`, `class` and `customUUID`:

- **`redirect`** defaults to `true` (`:149`); pass `redirect: false` to stay where you are.
- **Existing page.** If a page with that name already exists, it is returned and `properties` are **not**
  applied: they are saved only onto a newly created page (`:144-158`).
- **Property keys** resolve as in [Block Properties](#block-properties). `{ status: 'active' }` creates
  `:plugin.property.<plugin-id>/status`, not the built-in Status.
- **`class: true`** (undeclared) creates the page as a tag (`:151`).

**Example**:

```javascript
const page = await logseq.Editor.createPage(
  'New Page',
  { rating: 5 }, // -> :plugin.property.<plugin-id>/rating, type inferred as number
  { redirect: false }
)
```

#### deletePage

Delete a page by name or UUID (`editor.cljs:171-173`, `src/main/frontend/handler/common/page.cljs:190-196`).
`restorePage` and `deleteRecycledPagePermanently` are also available; the second only acts on a recycled
page (`editor.cljs:175-185`).

```typescript
deletePage(pageName: BlockPageName): Promise<void>
```

#### renamePage

Rename a page.

```typescript
renamePage(oldName: string, newName: string): Promise<void>
```

> Despite the declared `oldName` (`LSPlugin.ts:846`), the host is `page-handler/rename!`
> (`editor.cljs:187-188`), whose first argument must be the **page UUID string**. A page name throws
> "Invalid page uuid" (`src/main/frontend/handler/page.cljs:124-129`).

#### getPageBlocksTree

Get all blocks of a page as a tree, by name or UUID (`editor.cljs:376-382`).

```typescript
getPageBlocksTree(
  srcPage: PageIdentity
): Promise<Array<BlockEntity> | null>
```

### Queries

#### getPageLinkedReferences

Get blocks that reference a page or block (`editor.cljs:384-391`).

```typescript
getPageLinkedReferences(
  srcPage: PageIdentity
): Promise<Array<[page: PageEntity, blocks: Array<BlockEntity>]> | null>
```

> The host groups the referencing blocks with `group-by :block/page` (`editor.cljs:38-43`) and converts
> that map with `bean/->js`. The result is an object keyed by page, not the declared array of
> `[page, blocks]` tuples. If the blocks carry no `:block/page`, it is the plain block array.
> **Unverified:** the exact key format of the object; it was read from source, not run.

#### getPagesFromNamespace / getPagesTreeFromNamespace

> **Not implemented.** Declared at `LSPlugin.ts:748-758`. Both were defined in
> `src/main/logseq/api/file_based.cljs` (`:69`, `:75` at `eaa5a57337^`), which commit `eaa5a57337`
> ("remove file based api") deleted. No export replaces them, so a call throws `Not existed method`.

```typescript
getPagesFromNamespace(namespace: BlockPageName): Promise<Array<PageEntity> | null>
getPagesTreeFromNamespace(namespace: BlockPageName): Promise<Array<PageEntity> | null>
```

### Commands

#### registerSlashCommand

Add a custom slash command. It is SDK-local (`LSPlugin.user.ts:583-594`, `:177-233`) and registers through
the host's `register_plugin_slash_command` (`src/main/logseq/api.cljs:53`,
`src/main/logseq/api/plugin.cljs:297-302`).

A function callback is wrapped as the steps `editor/clear-current-slash`, `editor/restore-saved-cursor`
and `editor/hook` (`LSPlugin.user.ts:185-191`). The hook event carries `uuid` and `format`
(`src/main/frontend/commands.cljs:578-579`). Slash commands need plugins to be loaded, which does not
happen on native mobile (`src/main/frontend/util.cljc:131`).

```typescript
registerSlashCommand(
  tag: string,
  action: BlockCommandCallback | Array<SlashCommandAction>
): CommandUnregister | false

// SlashCommandAction (LSPlugin.ts:244-249):
// [cmd: 'editor/input' | 'editor/hook' | 'editor/clear-current-slash' | 'editor/restore-saved-cursor', ...args]
```

**Example**:

```javascript
logseq.Editor.registerSlashCommand('Insert Date', async ({ uuid }) => {
  const date = new Date().toISOString().split('T')[0]
  await logseq.Editor.insertBlock(uuid, `Date: ${date}`, {
    sibling: true,
  })
})
```

#### registerBlockContextMenuItem

Add an item to the block context menu (right-click the bullet). SDK-local (`LSPlugin.user.ts:596-617`).
The event carries `uuid` (`src/main/frontend/components/content.cljs:371-376`).

> Declared to return `CommandUnregister | false` (`LSPlugin.ts:676-679`). The handler does not return the
> result of `registerCommand.call` (`LSPlugin.user.ts:607`), so it returns `undefined`.

```typescript
registerBlockContextMenuItem(
  label: string,
  action: BlockCommandCallback  // (e: IHookEvent & { uuid: BlockUUID }) => Promise<void>
): CommandUnregister | false
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

Enter edit mode on a block (`editor.cljs:214-220`). `srcBlock` must be a UUID; anything else throws.
`pos` defaults to the end of the content.

```typescript
editBlock(
  srcBlock: BlockIdentity,
  opts?: { pos: number }  // cursor position
): Promise<void>
```

#### exitEditingMode

Exit edit mode, optionally selecting the block (`editor.cljs:64-67`).

```typescript
exitEditingMode(selectBlock?: boolean): Promise<void>
```

#### scrollToBlockInPage

Navigate to a block in a page. SDK-local (`LSPlugin.user.ts:644-656`): it calls
`pushState('page', { name: pageName }, { anchor: 'block-content-' + blockId })`, or `replaceState` when
`opts.replaceState` is set.

```typescript
scrollToBlockInPage(
  pageName: BlockPageName,
  blockId: BlockIdentity,
  opts?: { replaceState: boolean }
): void
```

### Selection

#### getSelectedBlocks

Get the currently selected blocks (`editor.cljs:92-99`).

```typescript
getSelectedBlocks(): Promise<Array<BlockEntity> | null>
```

---

## logseq.DB

Database query API. Interface: `libs/src/LSPlugin.ts:956-1005`; host: `src/main/logseq/api/db.cljs`.

### q (DSL Query)

Run a **simple query** (DSL) string (`src/main/logseq/api/db.cljs:15-19`). The DB worker executes it
(`src/main/frontend/db/query_dsl.cljs:40-50`) and the result is flattened.

```typescript
q<T = any>(dsl: string): Promise<T>
```

**Example**:

```javascript
// Blocks whose Status is Todo or Doing; names are capitalized to match the
// status values (src/main/frontend/worker/query_dsl.cljs:411-419)
const tasks = await logseq.DB.q('(task todo doing)')

// Blocks whose "rating" property is 5 (form tested in src/test/frontend/db/query_dsl_test.cljs:217)
const rated = await logseq.DB.q('(property rating 5)')
```

### datascriptQuery

Run a raw Datascript query with inputs.

- **SDK-local step.** The handler (`LSPlugin.user.ts:690-704`) drops the proxy's namespace tag. If any
  input is a function, it calls the host's `datascript_query` directly through
  `this.Experiments.ensureHostScope()`.
- **Host.** Otherwise the host (`src/main/logseq/api/db.cljs:21-45`) resolves inputs the way advanced
  queries resolve `:inputs` (`src/main/frontend/worker/handler/query.cljs:284-297`). A `[[Page]]` string
  becomes a lower-cased page name; other strings pass through unchanged
  (`deps/db/src/logseq/db/frontend/inputs.cljs:143-162`).
- **Result keys** are not camel-cased (`db.cljs:45` passes `false`).

Use DB attributes only. `:block/marker`, `:block/content` and `:block/properties` do not match in `:where`.

```typescript
datascriptQuery<T = any>(
  query: string,
  ...inputs: any[]
): Promise<T>
```

**Example**:

```javascript
// Titles of Todo blocks on one page
const results = await logseq.DB.datascriptQuery(
  `
  [:find ?title
   :in $ ?page-title
   :where
   [?p :block/title ?page-title]
   [?b :block/page ?p]
   [?b :logseq.property/status ?s]
   [?s :db/ident :logseq.property/status.todo]
   [?b :block/title ?title]]
`,
  'My Page'
)
```

The six status values are closed-value idents `:logseq.property/status.backlog`, `.todo`, `.doing`,
`.in-review`, `.done`, `.canceled` (`deps/db/src/logseq/db/frontend/property.cljs:313-318`).

### setFileContent / getFileContent

```typescript
setFileContent(path: string, content: string): Promise<void>
getFileContent(path: string): Promise<string | null>
```

- **setFileContent** (`src/main/logseq/api/db.cljs:53-74`) accepts only `logseq/custom.js`,
  `logseq/custom.css`, `logseq/publish.js` and `logseq/publish.css`. Any other path throws
  "Invalid path", and non-string content throws. The content is transacted into the graph database as
  `:file/path`/`:file/content`.
- **getFileContent** (`db.cljs:76-79`) has no allowlist.

### Hooks

#### onChanged

Listen to database changes. Emitted only for transactions that touch blocks
(`src/main/frontend/handler/events.cljs:264-268`).

```typescript
onChanged(callback: (event: {
  blocks: BlockEntity[];
  txData: IDatom[];
  txMeta?: { outlinerOp: string; [key: string]: any };
}) => void): IUserOffHook
```

**Example**:

```javascript
logseq.DB.onChanged(({ blocks, txData, txMeta }) => {
  console.log('Changed blocks:', blocks.length)
  console.log('Transaction type:', txMeta?.outlinerOp)
})
```

#### onBlockChanged

Listen to changes to one block. SDK-local (`LSPlugin.user.ts:660-688`). The host fires `block:<uuid>`
only for blocks with an installed hook (`src/main/frontend/handler/plugin.cljs:966-973`).

```typescript
onBlockChanged(
  uuid: BlockUUID,
  callback: (block: BlockEntity, txData: IDatom[], txMeta?: { outlinerOp: string; [key: string]: any }) => void
): IUserOffHook  // unsubscribe function
```

---

## logseq.UI

User interface operations. Calls resolve to `window.logseq.sdk.ui` (`src/main/logseq/sdk/ui.cljs`).

### showMsg

Display a toast notification (`src/main/logseq/sdk/ui.cljs:18-28`). `status` defaults to `success`,
`timeout: 0` keeps the message open, and content starting with `[:` is parsed as hiccup (`:23-24`).

```typescript
showMsg(
  content: string,
  status?: "success" | "warning" | "error" | string,
  opts?: {
    key?: string;
    timeout?: number;  // ms, 0 = no auto-dismiss
  }
): Promise<string>  // message key
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

Close a message by key (`ui.cljs:34-37`).

```typescript
closeMsg(key: string): void
```

### queryElementRect

Get the bounding rect of the first element matching a selector (`ui.cljs:39-42`).

```typescript
queryElementRect(selector: string): Promise<DOMRectReadOnly | null>
```

### resolveThemeCssPropsVals

Get computed CSS custom property values from `document.body` (`ui.cljs:54-63`).

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

Asset and file management. Calls resolve to `window.logseq.sdk.assets` (`src/main/logseq/sdk/assets.cljs`),
except `makeSandboxStorage`, which is SDK-local.

### listFilesOfCurrentGraph

List files under the graph's `assets` directory (`src/main/logseq/sdk/assets.cljs:12-15`). Desktop only: it
calls the Electron IPC handler `getAssetsFiles` (`src/electron/electron/handler.cljs:359-364`), which
resolves nothing when there is no `assets` directory.

```typescript
listFilesOfCurrentGraph(
  exts?: string | string[]
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

Open an asset in Logseq (`assets.cljs:17-24`). Only `pdf` is handled, by opening the PDF viewer; any other
extension returns `false`.

```typescript
builtInOpen(path: string): Promise<boolean | undefined>
```

### makeSandboxStorage

Create key-value file storage for the plugin. SDK-local (`LSPlugin.user.ts:713-717`): it returns an
`LSPluginFileStorage` with `assets: true`, which has `getItem`, `setItem`, `removeItem`, `hasItem`,
`allKeys` and `clear` (`libs/src/modules/LSPlugin.Storage.ts:4-14`).

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

> **Not implemented.** The interface is still declared (`libs/src/LSPlugin.ts:1010-1020`), and the proxy
> prefixes these calls to `git_exec_command`, `git_load_ignore_file` and `git_save_ignore_file`
> (`LSPlugin.user.ts:1064-1066`). Nothing handles them. There is no `src/main/logseq/sdk/git.cljs`, so
> `git` is not a `window.logseq.sdk` namespace. `src/main/logseq/api.cljs` has no such exports either:
> commit `036df25a17` ("Enhance: plugin APIs (#10399)") removed all three. Every call throws
> `Not existed method` (`libs/src/common.ts:328-330`). A DB graph is a single `db.sqlite` database
> (`deps/db/src/logseq/db/common/sqlite.cljs:29-33`), not a git working tree.

```typescript
// Declared only -- all three throw
execCommand(args: string[]): Promise<IGitResult>
loadIgnoreFile(): Promise<string>
saveIgnoreFile(content: string): Promise<void>
```

---

## Plugin Settings

### useSettingsSchema

Define the plugin settings schema. Each entry is a `SettingSchemaDesc` (`libs/src/LSPlugin.ts:295-313`):

- `type` is `string`, `number`, `boolean`, `enum`, `object`, `heading` or `button`.
- `key`, `default`, `title` and `description` are required. `description` supports markdown.
- Optional fields: `inputAs`, `enumChoices`, `enumPicker`, and `buttonText`/`buttonAction` (both required
  for `button`).

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
    description: 'Turn the feature on or off',
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
    description: 'Pick one',
  },
])
```

### Access Settings

```javascript
// Get all settings
const settings = logseq.settings

// Get specific setting
const enabled = logseq.settings?.enabled

// Update settings (LSPlugin.user.ts:898)
logseq.updateSettings({ enabled: false })

// React to changes; returns an unsubscribe function (LSPlugin.user.ts:903)
logseq.onSettingsChanged((next, prev) => {})
```

---

## UI Injection

### provideUI

Inject custom UI into a slot or into a DOM path. `UIOptions` is defined at `libs/src/LSPlugin.ts:39-67`.

```typescript
logseq.provideUI({
  key?: string;
  template: string | null;
  slot: string;            // or `path: string` (a DOM selector)
  style?: CSS.Properties;
  attrs?: Record<string, string>;
  replace?: boolean;
  close?: 'outside' | string;
  reset?: boolean;         // reset slot content
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

A plugin runs in one of two modes. **Neither is a sandbox or a security boundary.** See
[Plugin Security](../../explanation/architecture/plugin-security.md).

The mode comes from `logseq.mode` in the plugin's `package.json` (`libs/src/LSPlugin.core.ts:731-733`).
Shadow mode applies only when `mode === 'shadow'` (`:1289`); anything else runs as iframe.

### iframe Mode (Default)

- The plugin runs in an iframe created **without** a `sandbox` attribute (`libs/src/postmate/index.ts:402-410`).
- API calls cross the frame boundary as messages (postmate).
- Set in `package.json`: `"logseq": { "mode": "iframe" }`, or omit it.

### shadow Mode

- The plugin runs in the host document behind a proxied global (`libs/src/LSPlugin.shadow.ts:48-70`).
- Set in `package.json`: `"logseq": { "mode": "shadow" }`.

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

Host exceptions are caught and sent back tagged with `LSPMSG_ERROR_TAG`
(`libs/src/LSPlugin.core.ts:414-429`). That includes the `Not existed method` error for unimplemented APIs.

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

- Proxied API calls go through `callAsync`, which uses a 10-second deferred
  (`libs/src/LSPlugin.caller.ts:207-210`). The deferred rejects with `[deferred timeout]` when it expires
  (`libs/src/common.ts:265-285`).
- Postmate requests also default to a 10-second timeout (`libs/src/postmate/index.ts:4`, `:149-175`).

---

## Key Type Definitions

Transcribed from `libs/src/LSPlugin.ts`. These are **declared** types; what you get back is shaped by the
host's conversion (`src/main/logseq/sdk/utils.cljs`):

- **`:block/*`, `:db/*`, `:file/*` attributes** lose their namespace and are camel-cased:
  `:block/created-at` becomes `createdAt` (`utils.cljs:11-15`, `:45-50`). `datascriptQuery` skips the
  camel-casing (`src/main/logseq/api/db.cljs:45`).
- **Property attributes** keep the full ident as a string key with a leading colon, such as
  `":logseq.property/status"` or `":user.property/rating-AbCd1234"` (`utils.cljs:45-47`).
- **Titled entities** get `content` set to the title and a computed `fullTitle` (`utils.cljs:54-59`).
- **Ref values.** Results built with `result->js` (for example `getPage`, `insertBlock`, the tag and
  property APIs) replace ref-valued properties with entity ids (`utils.cljs:63-87`, `:113-118`).
- **Leftover fields.** `marker`, `journal?`, `type`, `format`, `file`, `namespace` and `originalName` do
  not appear in the DB schema (`deps/db/src/logseq/db/frontend/schema.cljs`). They are file-graph
  leftovers in the type.

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

The host validates `type` against `default`, `number`, `date`, `datetime`, `checkbox`, `url`, `node`,
`asset`, `json` and `string` (`src/main/logseq/api/db_based.cljs:130-134`). `cardinality` must be
`one`/`many`. `hide` and `public` are mapped to `:logseq.property/hide?` and `:public?` (`:141-158`).

---

## Findings: New Capabilities

Present in the SDK at `be800f1711` but not given full sections above.

### logseq.Commands -- unified command bus

`libs/src/LSPlugin.ts:615-637`; SDK-local (`libs/src/LSPlugin.user.ts:235-383`, `:563-567`). One
registration call covers palette commands, shortcuts, slash commands and context-menu entries. `when` is
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

On `logseq.Editor` (`libs/src/LSPlugin.ts:849-915`), backed by `src/main/logseq/api.cljs:206-224` and
`src/main/logseq/api/db_based.cljs`. These operate on property and tag *entities*. Bare names resolve into
the plugin's own namespaces, `plugin.property.<plugin-id>` and `plugin.class.<plugin-id>`
(`src/main/logseq/api/block.cljs:17-18`, `:35-66`).

```typescript
// properties
getProperty(key: string): Promise<BlockEntity | null>
upsertProperty(key: string, schema?: Partial<PropertySchema>, opts?: { name?: string }): Promise<IEntityID>
removeProperty(key: string): Promise<void>
getAllProperties(): Promise<PageEntity[] | null>
addPropertyValueChoices(propertyId: BlockIdentity, choices: Array<BlockIdentity>): Promise<void>
setPropertyNodeTags(propertyId: BlockIdentity, tagIds: Array<EntityID>): Promise<void>

// tags (classes)
getAllTags(): Promise<PageEntity[] | null>
getTag(nameOrIdent: string | EntityID): Promise<PageEntity | null>
getTagsByName(tagName: string): Promise<Array<PageEntity> | null>
getTagObjects(nameOrIdent: string): Promise<BlockEntity[] | null>
createTag(tagName: string, opts?: Partial<{ uuid: string; tagProperties: Array<{ name: string; schema?: Partial<PropertySchema>; properties?: {} }> }>): Promise<PageEntity | null>
addTagProperty(tagId: BlockIdentity, propertyIdOrName: BlockIdentity): Promise<void>
removeTagProperty(tagId: BlockIdentity, propertyIdOrName: BlockIdentity): Promise<void>
addTagExtends(tagId: BlockIdentity, parentTagIdOrName: BlockIdentity): Promise<void>
removeTagExtends(tagId: BlockIdentity, parentTagIdOrName: BlockIdentity): Promise<void>
addBlockTag(blockId: BlockIdentity, tagId: BlockIdentity): Promise<void>
removeBlockTag(blockId: BlockIdentity, tagId: BlockIdentity): Promise<void>
setBlockIcon(blockId: BlockIdentity, iconType: 'tabler-icon' | 'emoji', iconName: string): Promise<void>
removeBlockIcon(blockId: BlockIdentity): Promise<void>
```

Behaviour worth knowing (all in `db_based.cljs`):

- **`upsertProperty`** only works inside the plugin's own namespace; other idents throw
  (`:136-140`, `block.cljs:92-99`). It validates `schema` (`:130-158`). The type says it resolves
  `IEntityID`, but the host resolves the full property entity (`:161`, `:177-178`).
- **`removeProperty`** deletes only `plugin.property.*` properties and silently does nothing for any other
  key (`:180-187`).
- **`getAllTags`** excludes the root tag (`:197-201`).
- **`getTag`** tries `:plugin.class.<plugin-id>/<title>` first, then falls back to a tag lookup by name
  (`:308-326`).
- **`getTagObjects`** throws if the argument does not resolve to a tag (`:208-231`).
- **`createTag`** creates the tag in the `plugin.class.<plugin-id>` namespace and rejects titles containing
  `/`. `tagProperties` reuses existing properties or creates plugin properties, then attaches them to the
  tag (`:233-264`).
- **`addTagProperty`** rejects private built-in properties (`:332-345`). **`addTagExtends`** and
  **`removeTagExtends`** refuse to modify built-in tags (`:272-292`).
- **`setBlockIcon`** with `emoji` requires a name that exists in emoji-mart's data (`:385-402`).
- **`setPropertyNodeTags`** requires numeric entity ids (`:416-428`).

### App hooks not documented above

Declared at `libs/src/LSPlugin.ts:561-605`:

- `onThemeChanged` — emitted at `src/main/frontend/handler/plugin.cljs:1282`.
- `onTodayJournalCreated` — payload `{ title }`, `src/main/frontend/handler/page.cljs:337`.
- `onBeforeCommandInvoked` / `onAfterCommandInvoked` — `plugin.cljs:1147`, `:1150`.
- `onPageHeadActionsSlotted` — `src/main/frontend/components/page.cljs:356`.
- `onSidebarVisibleChanged` — payload `{ visible }`, `src/main/frontend/components/theme.cljs:89`.
- `onGraphAfterIndexed` — declared at `:561`, but **never fires**: nothing under `src/` emits
  `graph-after-indexed`.

On `logseq.Editor`, `onInputSelectionEnd` is emitted at `src/main/frontend/ui.cljs:200`.

### Also available (declared and implemented)

Not documented in detail here; host lines are in `src/main/logseq/api/editor.cljs` unless noted.

- **Editor**:
  - Editing: `checkEditing`, `insertAtEditingCursor`, `restoreEditingCursor`, `getEditingCursorPosition`,
    `getEditingBlockContent` (`:59-90`); `saveFocusedCodeEditorContent` (`:55-57`).
  - Pages: `getTodayPage` (`:118-122`); `getCurrentPageBlocksTree` (`:366-374`); `createJournalPage`
    (`:163-169`); `restorePage`, `deleteRecycledPagePermanently` (`:175-185`).
  - Blocks: `prependBlockInPage`, `appendBlockInPage`, which create the page if it is missing (`:393-441`);
    `getPreviousSiblingBlock`, `getNextSiblingBlock` (`:330-348`); `setBlockCollapsed` (`:350-364`).
  - Selection and navigation: `clearSelectedBlocks` (`:101-103`); `selectBlock` (`:207-212`);
    `openInRightSidebar` (`:190-202`); `openPDFViewer` (`:530-542`).
  - SDK-local: `newBlockUUID` and `isPageBlock` (`LSPlugin.user.ts:572-581`).
  - `registerHighlightContextMenuItem` is SDK-local (`LSPlugin.user.ts:619-642`) and PDF viewer only
    (`LSPlugin.ts:681-686`). Like the other menu registrations, it returns `undefined`.
- **App**: `registerSearchService` (SDK-local, `LSPlugin.user.ts:430-439`); `getCurrentRoute`
  (`src/main/logseq/api/app.cljs:174-178`).
- **DB**: `customQuery` (`src/main/logseq/api/db.cljs:47-51`).
- **UI**: `queryElementById`, `checkSlotValid` (`src/main/logseq/sdk/ui.cljs:44-52`).
- **Assets**: `makeUrl` (`src/main/logseq/sdk/assets.cljs:10`).
- **Host exports not declared in `LSPlugin.ts`** are still reachable through the proxy, because any name
  is forwarded. Examples: `logseq.App.getAppInfo`, `showThemes`, `setThemeMode` and `search`
  (`src/main/logseq/api.cljs:69`, `:76-77`, `:187-192`); `logseq.Editor.downloadGraphDb` and
  `downloadGraphPages` (`:105-106`).

### Template APIs

`libs/src/LSPlugin.ts:533-541`: `getTemplate`, `existTemplate`,
`createTemplate`, `removeTemplate`, `insertTemplate`; plus `getCurrentGraphTemplates` (`:513`).

> **Declared, not implemented.** None of these six has a host implementation at `be800f1711`. They
> lived in `src/main/logseq/api/file_based.cljs`, which commit `eaa5a57337` ("remove file based
> api") deleted, and nothing under `src/main/logseq/` replaces them. A call is snake-cased to a host
> name such as `get_template` (`libs/src/common.ts`, `invokeHostExportedApi`), finds no match, and
> reaches `throw new Error('Not existed method #…')` (`libs/src/common.ts:328-330`). That code path
> was read, not run. Users insert templates with `/Template`, and templates can apply themselves to
> tagged blocks through the "Apply template to tags" property — see
> [Templates System](../interface/templates-system.md).

---

## Best Practices

1. **Always check return values**: many methods resolve `null`.
2. **Use batch operations** when inserting several blocks.
3. **Clean up hooks**: every `on*` hook returns an unsubscribe function; call it in `beforeunload`.
4. **Handle async properly**: all API calls are async and time out after 10 seconds.
5. **Use a settings schema** for user preferences.
6. **Target DB graphs**: file graphs cannot be opened. Pass full property idents
   (`user.property/…`, `logseq.property/…`) when you mean a user or built-in property, because bare names
   resolve into the plugin's own namespace.
7. **Feature-detect** declared methods marked **Not implemented** above; they throw.

---

## See Also

- **HTTP API**: [http-api.md](http-api.md) for external integration
- **MCP Server**: [mcp-server.md](mcp-server.md) for AI integration
- **Plugin architecture**: [plugin-architecture.md](../../explanation/concepts/plugin-architecture.md)
- **Plugin security**: [plugin-security.md](../../explanation/architecture/plugin-security.md)
- **Plugin SDK samples**: [logseq-plugin-samples](https://github.com/logseq/logseq-plugin-samples)

---

_Last updated: 2026-09-11 | [Report an issue](https://github.com/logseq/logseq/issues)_
