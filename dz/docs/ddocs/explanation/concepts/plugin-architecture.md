---
title: 'Plugin Architecture'
status: 🟡 Active Development
version: '@logseq/libs v0.3.4'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
quadrant: 'Explanation'
platform: '🟠 Desktop Only'
related-docs:
  - '[Plugin Security Architecture](../architecture/plugin-security.md)'
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
  - '[First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)'
tags: ['plugins', 'architecture', 'extensibility', 'SDK']
---

# Plugin Architecture

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

This page explains how Logseq's plugin system works: where plugin code runs, how it reaches the
host, what the SDK exposes, and which extension points exist.

> **Read this first.** Logseq does **not** sandbox plugins:
>
> - The plugin `<iframe>` has no `sandbox` attribute.
> - There is no permission system.
> - API dispatch resolves by name, with no allowlist.
>
> [Plugin Security Architecture](../architecture/plugin-security.md) covers the security side in
> detail; this page covers mechanism. Installing a plugin means running arbitrary code with your
> account's privileges.

**What you'll understand:**

- Plugin system architecture and where each piece runs
- Lifecycle states and load sequence
- The API surface exposed by `@logseq/libs`
- Extension points
- Communication patterns between plugin and host
- Performance considerations

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Plugin Lifecycle](#plugin-lifecycle)
3. [API Layers](#api-layers)
4. [Extension Points](#extension-points)
5. [Security Model](#security-model)
6. [Communication Patterns](#communication-patterns)
7. [Performance Considerations](#performance-considerations)
8. [Design Principles](#design-principles)

---

## Architecture Overview

### System Components

There are three code locations involved in every plugin call:

```text
┌────────────────────────────────────────────────────────────┐
│ Electron renderer (one window, one set of webPreferences)  │
│  src/electron/electron/window.cljs:50-60                   │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Host realm (the Logseq UI; lsp://logseq.com in prod) │  │
│  │  window.logseq.api   src/main/logseq/api.cljs        │  │
│  │  window.logseq.sdk   src/main/logseq/sdk/            │  │
│  │  window.apis         resources/js/preload.js:36      │  │
│  │  LSPluginCore        libs/src/LSPlugin.core.ts       │  │
│  └───────────────────────┬──────────────────────────────┘  │
│                          │ Postmate RPC                    │
│                          │ libs/src/postmate/index.ts      │
│  ┌───────────────────────▼──────────────────────────────┐  │
│  │ Plugin frame(s) — plain <iframe>, class              │  │
│  │ .lsp-iframe-sandbox                                  │  │
│  │  LSPluginUser / logseq.*  libs/src/LSPlugin.user.ts  │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

The production window loads `lsp://logseq.com/index.html`; development builds load
`http://localhost:3001`
([`src/electron/electron/window.cljs:19-25`](../../../../../src/electron/electron/window.cljs)).

On the host side, `setupPluginCore()` creates `LSPluginCore` and assigns it to
`window.LSPluginCore`
([`libs/src/LSPlugin.core.ts:1934-1941`](../../../../../libs/src/LSPlugin.core.ts)). It ships as
the prebuilt bundle `resources/js/lsplugin.core.js` (output path at
[`libs/webpack.config.core.js:29-30`](../../../../../libs/webpack.config.core.js)).

The plugin side is `@logseq/libs` at version `0.3.4`. Its `main` entry is `dist/lsplugin.user.js`
([`libs/package.json:4-6`](../../../../../libs/package.json)).

### Plugin as Extension

On desktop, installed plugins live in directories under `~/.logseq/plugins/`. Directory names
beginning with `_` or `.` are skipped
([`src/electron/electron/utils.cljs:129-139`](../../../../../src/electron/electron/utils.cljs)).

> **Unverified:** the SDK also has a web-plugin path. `PluginLocal.isWebPlugin` is true on the web
> platform or for a `webPkg` option (`libs/src/LSPlugin.core.ts:1221-1223`). Web plugins load the
> SDK from jsdelivr (`:817-818`), and
> [`src/main/frontend/handler/common/plugin.cljs`](../../../../../src/main/frontend/handler/common/plugin.cljs)
> fetches web-plugin entry info. Whether the shipped web app exposes plugins to users was not
> checked, so the `Desktop Only` label above is not confirmed.

The `package.json` **must** carry a `logseq` object; the installer throws `Illegal logseq package`
without it
([`src/main/frontend/handler/plugin.cljs:1171-1172`](../../../../../src/main/frontend/handler/plugin.cljs)).
The installer derives two facts from the package:

- **Plugin id.** For a GitHub install the id is the repo name with `/` replaced by `_`. Otherwise it
  is `logseq.id`, falling back to the package `name` (`:1173-1175`).
- **Theme flag.** The package is treated as a theme when `logseq.theme` or `logseq.themes` is
  present (`:1177`).

The recognized `logseq` keys are typed as `LSPluginPkgConfig`
([`libs/src/LSPlugin.ts:69-85`](../../../../../libs/src/LSPlugin.ts)): `id`, `main`, `entry`
(an alias of `main`), `title`, `mode` (`'shadow' | 'iframe'`), `themes`, `icon`, `devEntry`,
`theme`.

A theme-only package needs no entry: `load()` logs `load:skip (no entry - theme-only package)` and
returns
([`libs/src/LSPlugin.core.ts:1037-1040`](../../../../../libs/src/LSPlugin.core.ts)).

### Isolation Model

The separation between plugin code and host code is **structural, not enforced**:

- A plugin in the default `iframe` mode runs in a child frame and talks to the host over one
  Postmate channel.
- A plugin declaring `mode: 'shadow'` runs in a shadow root **in the host realm**, with no message
  boundary at all (`libs/src/LSPlugin.caller.ts:352-398`). `PluginLocal.shadow` is simply
  `options.mode === 'shadow'` (`libs/src/LSPlugin.core.ts:1288-1290`).
- Even in `iframe` mode, `logseq.Experiments.ensureHostScope()` returns `window.top`
  ([`libs/src/modules/LSPlugin.Experiments.ts:274-282`](../../../../../libs/src/modules/LSPlugin.Experiments.ts)).
  That is an escape hatch into the host realm, and it is part of the SDK.
- The iframe is created with only `id`, `name`, `allow` and classes set
  ([`libs/src/postmate/index.ts:402-410`](../../../../../libs/src/postmate/index.ts)). **No
  `sandbox` attribute is ever set.**

So the frame gives stability and a clear API boundary, not containment.

---

## Plugin Lifecycle

### Lifecycle States

`PluginLocal` tracks exactly five states
([`libs/src/LSPlugin.core.ts:250-256`](../../../../../libs/src/LSPlugin.core.ts)):

```ts
enum PluginLocalLoadStatus {
  LOADING   = 'loading',
  UNLOADING = 'unloading',
  LOADED    = 'loaded',
  UNLOADED  = 'unload',
  ERROR     = 'error',
}
```

Transitions go through `_transitionStatus(next, from?)` (`:1198-1209`), which throws
`Invalid plugin status transition` when the current state is not in `from`. `load()` may start only
from `UNLOADED` or `ERROR` (`:1015-1018`), and the unload transition requires `LOADED`
(`:1138-1140`).

### Lifecycle Hooks

```javascript
// index.js — plugin entry point
function main() {
  console.log('Plugin loaded!')

  logseq.Editor.registerSlashCommand('Say Hi', async () => console.log('Hi!'))
  logseq.App.onRouteChanged(({ path, template }) => { /* ... */ })
  logseq.DB.onChanged(({ blocks, txData, txMeta }) => { /* ... */ })
}

logseq.ready(main).catch(console.error)
```

`ready()` is overloaded: it accepts a model object, a callback, or both
([`libs/src/LSPlugin.ts:1130-1140`](../../../../../libs/src/LSPlugin.ts)). Per the JSDoc, the model
argument is the same as the model in `provideModel` (`:1128`).

`beforeunload` takes a callback that returns `Promise<void>` (`LSPlugin.ts:1142`), so it cannot veto
the unload. The host awaits the before-unload message, then catches and logs any failure, and
unloading continues (`libs/src/LSPlugin.core.ts:1142-1152`). That await is a Postmate request with a
ten-second timeout; see [Request timeouts](#request-timeouts).

```javascript
logseq.beforeunload(async () => {
  await saveState()
})
```

Host-side events a plugin can subscribe to include these (`IAppProxy`, `LSPlugin.ts:560-605`):

```javascript
logseq.App.onCurrentGraphChanged(() => {})
logseq.App.onGraphAfterIndexed(({ repo }) => {})
logseq.App.onThemeModeChanged(({ mode }) => {})       // 'dark' | 'light'
logseq.App.onThemeChanged(({ name, mode, pid, url }) => {})
logseq.App.onTodayJournalCreated(({ title }) => {})
logseq.App.onRouteChanged(({ path, template }) => {})
logseq.App.onSidebarVisibleChanged(({ visible }) => {})
logseq.App.onBeforeCommandInvoked(condition, (e) => {})
logseq.App.onAfterCommandInvoked(condition, (e) => {})
```

### Load Sequence

`LSPluginCore` loads registered plugins one at a time, awaiting each `load()` in a `for` loop
(`libs/src/LSPlugin.core.ts:1546-1557`).

`PluginLocal.load()` ([`libs/src/LSPlugin.core.ts:1000-1086`](../../../../../libs/src/LSPlugin.core.ts))
runs, in order:

```text
1.  Return early if the plugin is already pending or loaded.        (:1006-1010)
2.  Transition UNLOADED|ERROR -> LOADING.                           (:1015-1018)
3.  _preparePackageConfigs()   read package config                  (:1025, def :658)
4.  _setupUserSettings(reload) restore settings                     (:1027, def :558)
5.  install package themes, unless the plugin is disabled           (:1029-1031)
6.  return early if disabled, or if there is no entry (theme-only)  (:1033-1040)
7.  emit 'beforeload'                                               (:1042)
8.  _tryToNormalizeEntry()     normalize the entry URL              (:1044, def :785)
9.  new LSPluginCaller(this) + connectToChild()                     (:1047-1048)
10. call the plugin's ready handler (LSPMSG_READY), deferred behind
    the load indicator when one is passed                           (:1051-1059)
11. register disposers (caller.destroy, cleanInjectedScripts)       (:1061-1065)
12. emit 'loadeded'                                                 (:1067)
13. finally: LOADING -> LOADED, or -> UNLOADED when disabled        (:1076-1085)
```

Step 8, `_tryToNormalizeEntry()`, takes one of three paths:

- A `devEntry` resolves to a full resource URL (`:789-795`).
- An entry not ending in `.js` is left unchanged (`:797`).
- A `.js` entry gets a generated wrapper page, `<id>_index.html`, that loads the SDK and the entry
  script (`:808-831`). The wrapper's path is then converted to an `lsp://` resource (`:833-845`).

Any throw inside the `try` block is caught at `:1070-1075`. The error is logged as `load:failed`,
the runtime is disposed, and the status is set to `ERROR`.

`reload()` (`:1088-1104`) skips while the plugin is pending. Otherwise it emits `beforereload`,
unloads if loaded, calls `load({ reload: true })`, then emits `reloaded`.

> The Plugins dashboard is reachable with the `t p` shortcut, bound to `:ui/goto-plugins`
> ([`src/main/frontend/modules/shortcut/config.cljs:464`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

---

## API Layers

### SDK Structure

`ILSPluginUser` declares exactly ten namespaces
([`libs/src/LSPlugin.ts:1256-1266`](../../../../../libs/src/LSPlugin.ts)):

```text
logseq.
├── App          IAppProxy          LSPlugin.ts:404
├── Commands     ICommandsProxy     LSPlugin.ts:615
├── Editor       IEditorProxy       LSPlugin.ts:642
├── DB           IDBProxy           LSPlugin.ts:956
├── Git          IGitProxy          LSPlugin.ts:1010   ← see note below
├── UI           IUIProxy           LSPlugin.ts:1032
├── Assets       IAssetsProxy       LSPlugin.ts:1052
├── Net          LSPluginNet        modules/LSPlugin.Net.ts:282
├── FileStorage  LSPluginFileStorage modules/LSPlugin.Storage.ts:20
└── Experiments  LSPluginExperiments modules/LSPlugin.Experiments.ts:47
```

A `logseq.Utils` proxy also exists at runtime (`libs/src/LSPlugin.user.ts:1130-1133`), typed as
`IUtilsProxy` (`LSPlugin.ts:1045-1047`) with a single `toJs` method, even though it is not listed
on `ILSPluginUser`.

> **`logseq.Git` is dead API.** `IGitProxy` declares `execCommand`, `loadIgnoreFile` and
> `saveIgnoreFile` (`LSPlugin.ts:1010-1020`), but the SDK-side `git` object is empty
> (`libs/src/LSPlugin.user.ts:707`). Every call is therefore forwarded to the host as `git_<method>`
> (`:1064-1065`). No host export, preload bridge method or SDK callable matches, so each call ends
> in `Not existed method #...`, thrown by `invokeHostExportedApi`
> ([`libs/src/common.ts:328-330`](../../../../../libs/src/common.ts)).

### Core APIs

**Application API** (`IAppProxy`, `LSPlugin.ts:404-610`):

```javascript
// Graph info — { name, url, path }  (AppGraphInfo, LSPlugin.ts:176-182)
const graph = await logseq.App.getCurrentGraph()          // :507

// Is this a DB graph?
const isDb = await logseq.App.checkCurrentIsDbGraph()     // :508

// User configuration (AppUserConfigs, LSPlugin.ts:157-171)
const configs = await logseq.App.getUserConfigs()         // :412

// Register a command. registerCommand(type, opts, action) — LSPlugin.ts:418-428
logseq.App.registerCommand(
  'my-plugin-action',
  {
    key: 'my-plugin-action',
    label: 'Do something cool',
    palette: true,
    keybinding: { binding: 'mod+shift+c' },
  },
  async () => {
    logseq.UI.showMsg('Command executed!')
  }
)

// Shorthand for a palette command — LSPlugin.ts:430-437
logseq.App.registerCommandPalette({ key: 'k', label: 'L' }, () => {})
```

`IAppProxy` has **no** `showMsg`, `getUserInput`, `setUserConfigs` or `onPageCreated`. Use
`logseq.UI.showMsg` for notifications.

**Editor API** (`IEditorProxy`, `LSPlugin.ts:642-951`):

```javascript
// BlockEntity — LSPlugin.ts:187-214. `title` is the content field (:193);
// `content` survives only as a deprecated alias (:195).
const block = await logseq.Editor.getCurrentBlock()         // :712

await logseq.Editor.updateBlock(block.uuid, 'Updated content')  // :799

// insertBlock(srcBlock, content, opts?) — LSPlugin.ts:775-786
await logseq.Editor.insertBlock(block.uuid, 'New block content', {
  before: false,
  sibling: true,
})

// The cursor-insert method is `insertAtEditingCursor` — LSPlugin.ts:699
logseq.Editor.registerSlashCommand('Insert Timestamp', async () => {
  await logseq.Editor.insertAtEditingCursor(`⏰ ${new Date().toISOString()}`)
})
```

There is no `logseq.Editor.insertAtCursor` and no `logseq.Editor.getAllBlocks`. For whole-graph
reads, use `getAllPages()` (`:848`) or a query.

**Database API** (`IDBProxy`, `LSPlugin.ts:956-1005`):

```javascript
// DSL query
const r1 = await logseq.DB.q('...')                       // :960

// Datalog with parameters
const r2 = await logseq.DB.datascriptQuery(query, ...inputs)  // :971

// All transactions
logseq.DB.onChanged(({ blocks, txData, txMeta }) => {})   // :978-982

// One block
logseq.DB.onBlockChanged(uuid, (block, txData, txMeta) => {})  // :989-996
```

In a **DB graph**, task status is a closed-value **ref**, not a string marker. `:block/marker`,
`:block/content`, `:block/journal?`, `:block/priority`, `:block/scheduled` and `:block/deadline` are
not DB-graph attributes:

```javascript
const todos = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :logseq.property/status ?s]
   [?s :db/ident :logseq.property/status.todo]]
`)
```

The six status idents are `:logseq.property/status.backlog`, `.todo`, `.doing`, `.in-review`,
`.done`, `.canceled`. The four priority idents are `:logseq.property/priority.low`, `.medium`,
`.high`, `.urgent`
([`deps/db/src/logseq/db/frontend/property.cljs:313-318`, `:334-337`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

> `:block/properties` is a special case. It is **not** a DataScript attribute, so it never matches
> in a `:where` clause. `lookup-kv-then-entity` intercepts the key and builds a
> `{property-ident value}` map from the entity
> ([`deps/db/src/logseq/db/common/entity_plus.cljc:138-146`, dispatched `:162-163`](../../../../../deps/db/src/logseq/db/common/entity_plus.cljc)).
> It works only on an *entity*, not in a query clause.

> **Do not use `libs/guides/db_query_guide.md` as a reference.** It uses `:logseq.task/status`,
> `:logseq.class/Todo`, `:block/type` and `:block/journal?`. None of those occur anywhere in
> `deps/db/src` at this revision.

**UI API** (`IUIProxy`, `LSPlugin.ts:1032-1043`) has exactly six members:

```javascript
logseq.UI.showMsg(content, status?, opts?)   // :1033-1037
logseq.UI.closeMsg(key)                      // :1038
logseq.UI.queryElementRect(selector)         // :1039
logseq.UI.queryElementById(id)               // :1040
logseq.UI.checkSlotValid(slot)               // :1041
logseq.UI.resolveThemeCssPropsVals(props)    // :1042
```

There is no `logseq.UI.prompt`, `logseq.UI.select` or `logseq.UI.setMainUIInlineStyle`. The main-UI
helpers live on the root object (`LSPlugin.ts:1221-1252`), and `setMainUIInlineStyle` takes a
**`CSS.Properties` object**, not a CSS string (`:1235`):

```javascript
logseq.setMainUIInlineStyle({ zIndex: 11, color: 'blue' })
logseq.showMainUI()
logseq.hideMainUI({ restoreEditingCursor: true })
logseq.toggleMainUI()
```

**Assets and file storage:**

```javascript
// IAssetsProxy — LSPlugin.ts:1052-1087
await logseq.Assets.listFilesOfCurrentGraph(['png', 'pdf'])
await logseq.Assets.makeUrl(path)          // NOT makeAssetUrl
await logseq.Assets.builtInOpen(path)
const store = logseq.Assets.makeSandboxStorage()

// logseq.FileStorage implements IAsyncStorage — modules/LSPlugin.Storage.ts:3-15
await logseq.FileStorage.setItem('state.json', JSON.stringify(state))
await logseq.FileStorage.getItem('state.json')
await logseq.FileStorage.hasItem('state.json')
await logseq.FileStorage.removeItem('state.json')
await logseq.FileStorage.allKeys()
await logseq.FileStorage.clear()
```

`IAsyncStorage` declares exactly the six methods above and **no** `readFile` / `writeFile`.

### Advanced APIs

**Macro renderer** (`onMacroRendererSlotted`, `LSPlugin.ts:599-601`). The payload field is
`arguments`, not `args`, and the payload also carries the block `uuid`:

```javascript
// {{renderer :h1, hello world, green}}
logseq.App.onMacroRendererSlotted(({ slot, payload: { arguments: args, uuid } }) => {
  const [type, text, color] = args
  if (type !== ':h1') return

  logseq.provideUI({
    key: `h1-${slot}`,
    slot,
    template: `<h2 style="color: ${color || 'red'}">${text}</h2>`,
  })
})
```

**Settings** (`SettingSchemaDesc`, `LSPlugin.ts:295-313`). Valid `type` values are exactly
`string | number | boolean | enum | object | heading | button`:

```javascript
logseq.useSettingsSchema([
  { key: 'apiKey', type: 'string', default: '', title: 'API Key', description: 'Your service API key' },
  { key: 'enableFeature', type: 'boolean', default: true, title: 'Enable Feature X', description: 'Turn on advanced feature' },
  {
    key: 'theme',
    type: 'enum',
    default: 'light',
    enumChoices: ['light', 'dark', 'auto'],
    enumPicker: 'select',       // 'select' | 'radio' | 'checkbox'
    title: 'Theme',
    description: 'UI theme preference',
  },
])

const settings = logseq.settings                       // LSPlugin.ts:1123
logseq.updateSettings({ apiKey: 'x' })                 // :1210
logseq.onSettingsChanged((next, prev) => { /* ... */ }) // :1215
```

Three more schema fields:

- `inputAs` (`'color' | 'date' | 'datetime-local' | 'range' | 'textarea'`) refines the input.
- `buttonText` and `buttonAction` are required when `type` is `'button'` (`:311-312`).
- `buttonAction` names a method on the object passed to `provideModel`. The host calls it as an
  awaited model call (`src/main/frontend/components/plugins_settings.cljs:106-110`).

---

## Extension Points

**1. Palette commands.** Use `logseq.App.registerCommand(type, opts, action)` (`LSPlugin.ts:418-428`)
or `registerCommandPalette(opts, action)` (`:430-437`). A keybinding without a palette entry is
`registerCommandShortcut(keybinding, action, opts?)` (`:445-454`).

**2. Slash commands.** `logseq.Editor.registerSlashCommand(tag, action)` (`LSPlugin.ts:666-669`)
takes either a `BlockCommandCallback` or an array of `SlashCommandAction` tuples. Each tuple's first
element is one of `editor/input`, `editor/hook`, `editor/clear-current-slash` or
`editor/restore-saved-cursor` (`:244-249`):

```javascript
logseq.Editor.registerSlashCommand('Generate UUID', async () => {
  await logseq.Editor.insertAtEditingCursor(await logseq.Editor.newBlockUUID())
})

logseq.Editor.registerSlashCommand('💥 Big Bang', [
  ['editor/hook', 'customCallback'],
  ['editor/clear-current-slash'],
])
```

**3. Block context menu.** Use `registerBlockContextMenuItem(label, action)` (`LSPlugin.ts:676-679`).
The action is a `BlockCommandCallback`, which receives an event carrying `uuid` (`:251-253`):

```javascript
logseq.Editor.registerBlockContextMenuItem('Export as JSON', async ({ uuid }) => {
  const block = await logseq.Editor.getBlock(uuid, { includeChildren: true })
  await navigator.clipboard.writeText(JSON.stringify(block, null, 2))
  logseq.UI.showMsg('Copied to clipboard!')
})
```

Two related menu APIs:

- `registerHighlightContextMenuItem(label, action, opts?)` (`:687-693`) is, per its JSDoc, currently
  available only in the PDF viewer.
- `logseq.App.registerPageMenuItem(tag, action)` (`:554-557`) adds a page-level menu entry; the
  action receives `page`.

**4. Renderers.**

- `onMacroRendererSlotted` handles `{{renderer …}}` macros (`:599-601`).
- `onBlockRendererSlotted(uuid, cb)` targets a specific block (`:574-577`).
- `onPageHeadActionsSlotted` targets page head actions (`:603`).

**5. UI slots.** Use `registerUIItem(type, opts)`, where `type` is `'toolbar' | 'pagebar'` and `opts`
is `{ key, template }` (`LSPlugin.ts:549-552`). The template cannot carry a function, so handlers
are referenced by name through `data-on-<event>` attributes. The host finds the nearest
`[data-on-<type>]` element and calls that name on the plugin's model (`libs/src/common.ts:521-527`):

```javascript
logseq.App.registerUIItem('toolbar', {
  key: 'my-plugin-button',
  template: `<a class="button" data-on-click="handleClick"><i class="ti ti-star"></i></a>`,
})

logseq.provideModel({
  handleClick() {
    logseq.UI.showMsg('Toolbar button clicked!')
  },
})
```

`logseq.provideUI(ui)` (`:1194`) injects arbitrary DOM. It accepts either a `path` (a DOM selector)
or a `slot`, plus `template`, `style`, `attrs`, `key`, `close`, `replace` and `reset` (`UIOptions`,
`LSPlugin.ts:39-67`).

**6. Styles and themes.** Use `logseq.provideStyle(styleOrOptions)` (`:1174`) and
`logseq.provideTheme(theme)` (`:1161`).

**7. Experimental renderers.** `logseq.Experiments` adds `registerFencedCodeRenderer`,
`registerDaemonRenderer`, `registerHostedRenderer`, `registerSidebarRenderer`,
`registerRouteRenderer`, `registerBlockPropertiesRenderer`, `registerBlockRenderer` and
`registerExtensionsEnhancer`
([`libs/src/modules/LSPlugin.Experiments.ts:105-272`](../../../../../libs/src/modules/LSPlugin.Experiments.ts)).
The SDK's own header comment warns that these may be adjusted at any time and may not be supported on
the Marketplace (`:42-46`).

> **The `subs` option is inert.** Seven of these methods declare `subs?: Array<string>`
> (`Experiments.ts:110`, `:126`, `:142`, `:159`, `:173`, `:205`, `:239`). The JSDoc calls it a
> "Reserved subscription list for future reactive updates" (`:196`, `:230`).
>
> The host copies `subs` into each renderer registration: `src/main/logseq/sdk/experiments.cljs:22`,
> `:34`, `:41`, `:62`, `:69`, `:89`, and `src/main/frontend/handler/plugin.cljs:559-563`. No code
> under `src/main/` reads it back, so it does not drive re-renders.

---

## Security Model

This section is a summary. [Plugin Security Architecture](../architecture/plugin-security.md) gives
the full treatment, with the code behind each claim.

### There is no sandbox

- The plugin `<iframe>` is created with only `id`, `name`, `allow` and CSS classes
  (`libs/src/postmate/index.ts:402-410`). The string `sandbox=` appears nowhere in the repository
  outside a vendored icon bundle.
- The SDK passes `PluginLocalOptions.allow` through as the iframe's `allow` (Permissions Policy)
  attribute. It is declared (`libs/src/LSPlugin.core.ts:224`) and read
  (`libs/src/LSPlugin.caller.ts:289`), but **never assigned**, so no `allow` value is ever applied.
- Electron `webPreferences` are window-wide, not per-plugin
  (`src/electron/electron/window.cljs:50-60`): `nodeIntegration false`,
  `nodeIntegrationInWorker false`, `sandbox false`, `webSecurity (not dev?)`,
  `contextIsolation true`. Same-origin policy is therefore off in development builds.
- The `lsp://` scheme is registered with `:bypassCSP true` (`src/electron/electron/core.cljs:474-485`).
  `disableXFrameOptions` deletes `X-Frame-Options` **and `Content-Security-Policy`** response
  headers in the window's session (`src/electron/electron/utils.js:9-29`).

### There is no permission model

All plugins have identical reach. API dispatch resolves **by name**, with no allowlist and no schema
validation:

1. `initApiProxyHandlers` (`libs/src/LSPlugin.core.ts:411-422`) passes each `api:call` message to
   `invokeHostExportedApi` (`libs/src/common.ts:301-332`).
2. That function looks the name up on `window.logseq.api`, the matching `window.logseq.sdk.<ns>`,
   the SDK's callables, and `window.apis`, the Electron preload bridge (`resources/js/preload.js:36`).

A method the SDK never advertised is reachable if it exists on any of those objects.

The only stated security posture is the install-time warning on the Plugins dashboard
([`src/resources/dicts/en.edn:1294`](../../../../../src/resources/dicts/en.edn)):

> "Plugins can access your graph and your local files, issue network requests. They can also cause
> data corruption or loss. We're working on proper access rules for your graphs. Meanwhile, make
> sure you have regular backups of your graphs and only install the plugins when you can read and
> understand the source code."

### What the code does mitigate

| Threat                                          | Mitigation in code                                                                         |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Markup injected through `provideUI`             | `template` goes through `DOMPurify.sanitize` before insertion. The config explicitly adds `iframe` tags, iframe attributes, and `ALLOW_UNKNOWN_PROTOCOLS: true` (`libs/src/common.ts:400-413`) |
| Arbitrary protocol from `openExternal`          | Preload allowlists `https:`, `http:`, `mailto:`, `zotero:`, `file:` (`resources/js/preload.js:9`, `:84-90`) |
| `custom.js` in a graph                          | Explicit `confirm()` consent, stored with a timestamp and re-asked after 604800000 ms (7 days) (`src/main/frontend/handler/ui.cljs:129-147`) |

### Practices that still help

```javascript
// Validate anything you received from outside the plugin
if (!/^https?:\/\//.test(url)) {
  logseq.UI.showMsg('Invalid URL', 'error')
  return
}

// Never log secrets
console.log('Authenticating...')   // ok
console.log('API Key:', apiKey)    // not ok

// Contain your own failures — a throw in a hook should not kill the plugin
try {
  await riskyOperation()
} catch (error) {
  logseq.UI.showMsg(`Error: ${error.message}`, 'error')
}
```

For users: read the source before installing, and keep graph backups. The warning above is the
entire enforcement story.

---

## Communication Patterns

### Plugin → Logseq

`_makeUserProxy` ([`libs/src/LSPlugin.user.ts:1002-1076`](../../../../../libs/src/LSPlugin.user.ts))
wraps each namespace and handles a method call in three steps:

1. **Local handler.** If the SDK defines the method locally, that runs first. Its return value is
   used unless it returns `PROXY_CONTINUE` (`:1011-1015`).
2. **Hook subscription.** Names starting with `on`, `once` or `off` become hook subscriptions (see
   below).
3. **Host call.** Anything else is sent as one `api:call` message carrying `{ tag, method, args }`
   via `caller.callAsync` (`:1069-1073`).

For the `git`, `ui`, `assets` and `utils` namespaces, the method name is prefixed with the namespace
(`:1064-1065`). `app`, `editor`, `db` and `commands` calls are not prefixed.

`callAsync` returns a Promise backed by a ten-second deferred
([`libs/src/LSPlugin.caller.ts:207-209`](../../../../../libs/src/LSPlugin.caller.ts)). On the host,
`invokeHostExportedApi` snake-cases the name and resolves it against `window.logseq.api`, the
matching `window.logseq.sdk.<ns>`, the SDK's callables, and `window.apis`. It throws
`Not existed method #<name>` if nothing matches (`libs/src/common.ts:301-332`).

### Logseq → Plugin

Events are delivered over the same channel. In a namespaced proxy, a method name matching
`/^(once|off|on)/i` becomes a `hook:<ns>:<snake_case_type>` subscription. The proxy calls
`App._installPluginHook` or `_uninstallPluginHook` on the host
(`libs/src/LSPlugin.user.ts:1017-1058`). The call returns an unsubscribe function synchronously, not
a Promise (`:1057`; `IUserOffHook`, `LSPlugin.ts:111`):

```javascript
const off = logseq.DB.onChanged(({ blocks }) => { /* ... */ })
// later
off()
```

### The transport

The handshake always goes over `window.postMessage`. The host sets `enableMessageChannel: true` for
every plugin frame
([`libs/src/LSPlugin.caller.ts:291`](../../../../../libs/src/LSPlugin.caller.ts)). If the child
accepts, the parent creates a `MessageChannel` and transfers a port in a `setup-channel` message
(`libs/src/postmate/index.ts:451-469`). The child then moves its listener from the window to the port
(`:318-345`). See
[Postmate transport: MessageChannel optimization](../../reference/plugin-apis/pr-12395/01-postmate-message-channel.md).

### Plugin → Plugin

There is a supported path, and it does not go through the graph. `IAppProxy` exposes these methods
(`LSPlugin.ts:466-478`):

```javascript
// call another plugin's registered command or provideModel method
await logseq.App.invokeExternalPlugin('other-plugin-id.commands.some-key', ...args)

// inspect another loaded plugin
const p = await logseq.App.getExternalPlugin('other-plugin-id')
```

`invokeExternalPlugin` takes `xx-plugin-id.commands.xx-key` or `xx-plugin-id.models.xx-key`. It
couples the two plugins by id, so use it deliberately.

---

## Performance Considerations

### Plugin Load Time

Plugins load sequentially: each `load()` is awaited before the next starts
(`libs/src/LSPlugin.core.ts:1546-1557`). The handshake has an 8-second budget, after which the frame
is destroyed (`libs/src/LSPlugin.caller.ts:296-304`). Keep `main()` to registration only:

```javascript
// Slow: everything happens at load
function main() {
  loadEntireDatabase()
  processAllBlocks()
}

// Better: register cheaply, do work on demand
function main() {
  logseq.App.registerCommand('heavy', { key: 'heavy', label: 'Heavy Operation' }, async () => {
    process(await loadData())
  })
}
```

`load()` logs its own duration as `load:done in <n>ms` (`libs/src/LSPlugin.core.ts:1068-1069`).
That log is the quickest way to find a slow plugin.

### Memory Usage

`DB.onChanged` fires for every transaction. An unbounded cache keyed on block uuid grows for the
lifetime of the session:

```javascript
const cache = new Map()
const MAX_SIZE = 1000

logseq.DB.onChanged(({ blocks }) => {
  for (const b of blocks) {
    cache.set(b.uuid, b)
    if (cache.size > MAX_SIZE) cache.delete(cache.keys().next().value)
  }
})
```

Also unsubscribe: every `on*` call returns an off-function, and `beforeunload` is the place to call
it.

### Debouncing Events

```javascript
let timer
logseq.DB.onChanged(({ blocks }) => {
  clearTimeout(timer)
  timer = setTimeout(() => heavyProcessing(blocks), 1000)
})
```

If you only care about one block, use `logseq.DB.onBlockChanged(uuid, cb)` (`LSPlugin.ts:989-996`)
instead of filtering every transaction yourself.

### Request timeouts

There are two separate ten-second timeouts, one in each direction:

- **Plugin → host.** Every proxied API call waits on `deferred(1000 * 10)`
  (`libs/src/LSPlugin.caller.ts:207-209`). A call that takes longer rejects with
  `[deferred timeout] async call #N` (`libs/src/common.ts:280-285`), even if the host later
  completes the work.
- **Host → plugin.** When the host awaits a plugin model function, it uses Postmate
  `ParentAPI.get()`. That rejects after `Postmate.requestTimeout`, default `10_000`, with
  `Postmate: request timeout (10000ms)` (`libs/src/postmate/index.ts:4`, `:149-175`). Awaited calls
  include settings button actions, `invokeExternalPlugin` on `models`, and the before-unload
  message.

Neither timeout is configurable from plugin code.

---

## Design Principles

### 1. One channel, name-resolved

There is a single RPC path and a single resolution rule: snake-case the method name and look it up
on the host's exported objects (`libs/src/common.ts:301-332`). That keeps the SDK thin. It is also
why the surface cannot be constrained without a registry.

### 2. Async by default

Every proxied host call returns a Promise, because every call is a message round trip. Hook
subscriptions are the exception: they return an off-function.

### 3. Event-driven

Hooks are installed on the host and events are pushed to the plugin, not polled
(`libs/src/LSPlugin.user.ts:1017-1058`).

### 4. Declarative UI

Plugins describe markup (`provideUI`, `provideStyle`) and name their handlers (`provideModel`),
because functions cannot be embedded in a template. The host passes templates through DOMPurify
before injecting them, with a config that permits iframes (`libs/src/common.ts:400-413`).

### 5. Structural, not capability, separation

A plugin can do whatever the host exports. The frame is a boundary for *organization*; see
[Security Model](#security-model).

---

## Known Gaps

These come from what the code does today, not from a roadmap:

- **No access rules.** The dashboard warning states outright that they are still being worked on
  (`src/resources/dicts/en.edn:1294`).
- **`allow` is dead.** Declared and read, never assigned
  (`libs/src/LSPlugin.core.ts:224`, `libs/src/LSPlugin.caller.ts:289`).
- **`logseq.Git` is dead.** Declared and exposed, with no host implementation
  (`libs/src/LSPlugin.user.ts:707`, `libs/src/common.ts:328-330`).
- **`subs` is inert.** Seven `Experiments` registration methods accept it; the host stores it and
  never reads it under `src/main/`.

---

## Summary

1. **One renderer, one channel.** Plugins run in a plain `<iframe>`, or directly in the host realm
   in `shadow` mode. They reach the host through Postmate, upgraded to a `MessageChannel` after the
   handshake.
2. **Five lifecycle states** with guarded transitions, driven by `PluginLocal.load()`, `unload()` and
   `reload()`.
3. **Ten SDK namespaces**, one of which (`Git`) has no host implementation.
4. **Extension points** are commands, slash commands, context-menu items, renderers, UI slots,
   styles and themes, and the experimental renderer family.
5. **No sandbox, no permissions.** The separation is structural. Installing a plugin is running
   arbitrary code.
6. **Performance** is your responsibility: register cheaply, debounce `DB.onChanged`, unsubscribe in
   `beforeunload`, and remember the ten-second timeouts in both directions.

---

## Next Steps

- [First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)
- [Plugin API Reference](../../reference/api/plugin-api.md)
- [Plugin Security Architecture](../architecture/plugin-security.md)
- [PR 12395 API notes](../../reference/plugin-apis/pr-12395/README.md)

The authoritative surface is the type file itself:
[`libs/src/LSPlugin.ts`](../../../../../libs/src/LSPlugin.ts).
