---
title: 'Plugin Architecture'
status: 🟡 Active Development
version: '@logseq/libs v0.3.4'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: wip-unverified
quadrant: 'Explanation'
platform: '🟠 Desktop Only'
related-docs:
  - '[Plugin Security Architecture](../architecture/plugin-security.md)'
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
  - '[First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)'
tags: ['plugins', 'architecture', 'extensibility', 'SDK']
---

# Plugin Architecture

Understand how Logseq's plugin system actually works: where plugin code runs, how it reaches the
host, what the SDK exposes, and which extension points exist.

> **Read this first.** Logseq does **not** sandbox plugins. The plugin `<iframe>` carries no
> `sandbox` attribute, there is no permission system, and API dispatch resolves by name with no
> allowlist. The [Plugin Security Architecture](../architecture/plugin-security.md) document covers
> that in detail; this document covers mechanism, not protection. Installing a plugin means running
> arbitrary code with your account's privileges.

**What You'll Understand:**

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
│  │ Host realm (the Logseq UI, lsp://logseq.com)         │  │
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

`LSPluginCore` is set up on the host side by `setupPluginCore()`
([`libs/src/LSPlugin.core.ts:1937`](../../../../../libs/src/LSPlugin.core.ts)) and ships as the
prebuilt bundle `resources/js/lsplugin.core.js`. The plugin side is `@logseq/libs`, whose `main`
entry is `dist/lsplugin.user.js` ([`libs/package.json`](../../../../../libs/package.json), version
`0.3.4`).

### Plugin as Extension

A Logseq plugin is a directory under `~/.logseq/plugins/<id>` containing a `package.json`. Directory
names beginning with `_` or `.` are skipped
([`src/electron/electron/utils.cljs:122-139`](../../../../../src/electron/electron/utils.cljs)).

The `package.json` **must** carry a `logseq` object — the installer throws
`Illegal logseq package` without it
([`src/main/frontend/handler/plugin.cljs:1171-1172`](../../../../../src/main/frontend/handler/plugin.cljs)).
The plugin id falls back to `logseq.id`, then the package `name` (`:1173-1175`), and the package is
treated as a theme when `logseq.theme` or `logseq.themes` is present (`:1177`).

The recognised `logseq` keys are typed as `LSPluginPkgConfig`
([`libs/src/LSPlugin.ts:69-85`](../../../../../libs/src/LSPlugin.ts)): `id`, `main`, `entry`
(an alias of `main`), `title`, `mode` (`'shadow' | 'iframe'`), `themes`, `icon`, `devEntry`,
`theme`.

A theme-only package needs no entry at all — `load()` logs
`load:skip (no entry - theme-only package)` and returns
([`libs/src/LSPlugin.core.ts:1037-1040`](../../../../../libs/src/LSPlugin.core.ts)).

### Isolation Model

The separation between plugin code and host code is **structural, not enforced**:

- A plugin in the default `iframe` mode runs in a child frame and talks to the host over one
  Postmate channel.
- A plugin declaring `mode: 'shadow'` runs in a shadow root **in the host realm** with no message
  boundary at all (`libs/src/LSPlugin.caller.ts:352-397`; `PluginLocal.shadow` is just
  `options.mode === 'shadow'`, `libs/src/LSPlugin.core.ts:1288-1290`).
- Even in `iframe` mode, `logseq.Experiments.ensureHostScope()` returns `window.top`
  ([`libs/src/modules/LSPlugin.Experiments.ts:274-282`](../../../../../libs/src/modules/LSPlugin.Experiments.ts)),
  a documented escape hatch into the host realm.
- The iframe is created with only `id`, `name`, `allow` and classes set
  ([`libs/src/postmate/index.ts:402-410`](../../../../../libs/src/postmate/index.ts)). **No
  `sandbox` attribute is ever set.**

So the practical benefits of the frame are stability and a clear API boundary — not containment.

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

Transitions are guarded by `_transitionStatus(next, from?)` (`:1198-1209`), which throws on an
illegal transition. `load()` may only start from `UNLOADED` or `ERROR` (`:1015-1018`); `unload()`
may only start from `LOADED` (`:1138-1140`).

### Lifecycle Hooks

```javascript
// index.js — plugin entry point
function main() {
  console.log('Plugin loaded!')

  logseq.Editor.registerSlashCommand('Say Hi', () => console.log('Hi!'))
  logseq.App.onRouteChanged(({ path, template }) => { /* ... */ })
  logseq.DB.onChanged(({ blocks, txData, txMeta }) => { /* ... */ })
}

// `ready` resolves once the host has sent the ready message
logseq.ready(main).catch(console.error)
```

`ready()` is overloaded — it accepts a model object, a callback, or both
([`libs/src/LSPlugin.ts:1130-1140`](../../../../../libs/src/LSPlugin.ts)). The model argument is the
same shape as `provideModel`.

`beforeunload` takes a callback that returns a **`Promise<void>`** (`LSPlugin.ts:1142`); the
returned value is not a veto:

```javascript
logseq.beforeunload(async () => {
  await saveState()
})
```

Host-side events a plugin can subscribe to (all on `IAppProxy`, `LSPlugin.ts:560-605`):

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

`PluginLocal.load()` ([`libs/src/LSPlugin.core.ts:1000-1086`](../../../../../libs/src/LSPlugin.core.ts))
runs, in order:

```text
1.  Bail out early if the plugin is already pending or loaded.
2.  Transition UNLOADED|ERROR -> LOADING.
3.  _preparePackageConfigs()   read and validate package.json      (:658)
4.  _setupUserSettings(reload) restore settings + logger           (:558)
5.  install package themes (skipped when the plugin is disabled)
6.  return early if disabled, or if there is no entry (theme-only)
7.  emit 'beforeload'
8.  _tryToNormalizeEntry()     resolve the entry to an lsp:// URL  (:785)
9.  new LSPluginCaller(...) + connectToChild()                     (caller :58)
10. call the plugin's ready handler (LSPMSG_READY), optionally
    deferred behind the load indicator
11. register disposers (caller.destroy, cleanInjectedScripts)
12. emit 'loadeded'; transition LOADING -> LOADED (or UNLOADED when disabled)
```

Any throw inside that block is caught at `:1070-1075`: the runtime is disposed and the status is set
to `ERROR`.

`reload()` (`:1088-1104`) emits `beforereload`, unloads if loaded, calls `load({ reload: true })`,
then emits `reloaded`.

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
└── Experiments  LSPluginExperiments modules/LSPlugin.Experiments.ts
```

A `logseq.Utils` proxy also exists at runtime (`libs/src/LSPlugin.user.ts:1130`, `IUtilsProxy` at
`LSPlugin.ts:1045-1047`, a single `toJs` method) even though it is not listed on `ILSPluginUser`.

> **`logseq.Git` is dead API.** `IGitProxy` is declared and `logseq.Git` is exposed, but no `git_*`
> method is exported anywhere under `src/`, so every call ends in
> `Not existed method #...` thrown by `invokeHostExportedApi`
> ([`libs/src/common.ts:331-333`](../../../../../libs/src/common.ts)).

### Core APIs

**Application API** (`IAppProxy`, `LSPlugin.ts:404-610`):

```javascript
// Graph info — { name, url, path }  (AppGraphInfo, LSPlugin.ts:176-180)
const graph = await logseq.App.getCurrentGraph()

// Is this a DB graph?
const isDb = await logseq.App.checkCurrentIsDbGraph()   // :508

// User configuration (AppUserConfigs, LSPlugin.ts:157-174)
const configs = await logseq.App.getUserConfigs()

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

// Shorthand for a palette-only command — LSPlugin.ts:430-438
logseq.App.registerCommandPalette({ key: 'k', label: 'L' }, () => {})
```

There is **no** `logseq.App.showMsg`, `logseq.App.getUserInput`, `logseq.App.setUserConfigs` or
`logseq.App.onPageCreated` in `IAppProxy`. Use `logseq.UI.showMsg` for notifications.

**Editor API** (`IEditorProxy`, `LSPlugin.ts:642-954`):

```javascript
// BlockEntity — LSPlugin.ts:187-217. `title` is the content field;
// `content` survives only as a deprecated alias (:195).
const block = await logseq.Editor.getCurrentBlock()

await logseq.Editor.updateBlock(block.uuid, 'Updated content')

// insertBlock(srcBlock, content, opts?) — LSPlugin.ts:775-791
await logseq.Editor.insertBlock(block.uuid, 'New block content', {
  before: false,
  sibling: true,
})

// The cursor-insert method is `insertAtEditingCursor` — LSPlugin.ts:699
logseq.Editor.registerSlashCommand('Insert Timestamp', async () => {
  await logseq.Editor.insertAtEditingCursor(`⏰ ${new Date().toISOString()}`)
})
```

There is no `logseq.Editor.insertAtCursor` and no `logseq.Editor.getAllBlocks`. Whole-graph reads go
through `getAllPages()` (`:848`) or a query.

**Database API** (`IDBProxy`, `LSPlugin.ts:956-1005`):

```javascript
// DSL query
const r1 = await logseq.DB.q('...')                       // :960

// Datalog with parameters
const r2 = await logseq.DB.datascriptQuery(query, ...inputs)  // :971

// All transactions
logseq.DB.onChanged(({ blocks, txData, txMeta }) => {})   // :978-983

// One block
logseq.DB.onBlockChanged(uuid, (block, txData, txMeta) => {})  // :989-997
```

In a **DB graph**, task state is a closed-value **ref**, not a string marker. `:block/marker`,
`:block/content`, `:block/journal?`, `:block/priority`, `:block/scheduled` and `:block/deadline` are
file-graph attributes and do not exist in a DB graph:

```javascript
const todos = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :logseq.property/status ?s]
   [?s :db/ident :logseq.property/status.todo]]
`)
```

The six status idents are `:logseq.property/status.backlog`, `.todo`, `.doing`, `.in-review`,
`.done`, `.canceled`; the four priority idents are `:logseq.property/priority.low`, `.medium`,
`.high`, `.urgent`
([`deps/db/src/logseq/db/frontend/property.cljs:313-318`, `:334-337`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

> `:block/properties` is a special case. It is **not** a DataScript attribute, so it never matches in
> a `:where` clause; `lookup-kv-then-entity` intercepts the key and builds a
> `{property-ident value}` map from the entity
> ([`deps/db/src/logseq/db/common/entity_plus.cljc:138-146`, dispatched `:162-163`](../../../../../deps/db/src/logseq/db/common/entity_plus.cljc)).
> It therefore works on an *entity* — for example in a `:result-transform` — and only there.

> **Do not use `libs/guides/db_query_guide.md` as a reference.** It documents
> `:logseq.task/status`, `:logseq.class/Todo`, `:block/type` and `:block/journal?`, none of which
> exist at this revision.

**UI API** (`IUIProxy`, `LSPlugin.ts:1032-1043`) has exactly six members:

```javascript
logseq.UI.showMsg(content, status?, opts?)   // :1033-1037
logseq.UI.closeMsg(key)                      // :1038
logseq.UI.queryElementRect(selector)         // :1039
logseq.UI.queryElementById(id)               // :1040
logseq.UI.checkSlotValid(slot)               // :1041
logseq.UI.resolveThemeCssPropsVals(props)    // :1042
```

There is no `logseq.UI.prompt`, no `logseq.UI.select`, and no `logseq.UI.setMainUIInlineStyle` — the
main-UI helpers live on the root object (`LSPlugin.ts:1221-1252`) and
`setMainUIInlineStyle` takes a **`CSS.Properties` object**, not a CSS string:

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

`FileStorage` has **no** `readFile` / `writeFile`; the interface is the six `IAsyncStorage` methods
above.

### Advanced APIs

**Macro renderer** (`onMacroRendererSlotted`, `LSPlugin.ts:599-602`). The payload field is
`arguments` — not `args` — and carries the macro's `uuid`:

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

`inputAs` (`'color' | 'date' | 'datetime-local' | 'range' | 'textarea'`) refines a `string` input;
`buttonText` and `buttonAction` are required when `type` is `'button'`, and `buttonAction` names a
method on the object passed to `provideModel`.

---

## Extension Points

**1. Palette commands** — `logseq.App.registerCommand(type, opts, action)`
(`LSPlugin.ts:418-428`) or `registerCommandPalette(opts, action)` (`:430-438`). A bare keybinding
without a palette entry is `registerCommandShortcut(keybinding, action, opts?)` (`:445-459`).

**2. Slash commands** — `logseq.Editor.registerSlashCommand(tag, action)`
(`LSPlugin.ts:666-669`). The action is a callback **or** an array of fixed
`SlashCommandAction` tuples:

```javascript
logseq.Editor.registerSlashCommand('Generate UUID', async () => {
  await logseq.Editor.insertAtEditingCursor(await logseq.Editor.newBlockUUID())
})

logseq.Editor.registerSlashCommand('💥 Big Bang', [
  ['editor/hook', 'customCallback'],
  ['editor/clear-current-slash'],
])
```

**3. Block context menu** — `registerBlockContextMenuItem(label, action)`
(`LSPlugin.ts:676-679`). The callback receives `{ uuid }`:

```javascript
logseq.Editor.registerBlockContextMenuItem('Export as JSON', async ({ uuid }) => {
  const block = await logseq.Editor.getBlock(uuid, { includeChildren: true })
  await navigator.clipboard.writeText(JSON.stringify(block, null, 2))
  logseq.UI.showMsg('Copied to clipboard!')
})
```

`registerHighlightContextMenuItem(label, action, opts?)` (`:687-694`) is the PDF-viewer equivalent,
and `logseq.App.registerPageMenuItem(tag, action)` (`:554-557`) adds a page-level menu entry.

**4. Renderers** — `onMacroRendererSlotted` for `{{renderer …}}` (`:599-602`),
`onBlockRendererSlotted(uuid, cb)` for a specific block (`:574-577`), and
`onPageHeadActionsSlotted` (`:603`).

**5. UI slots** — `registerUIItem(type, opts)` where `type` is `'toolbar' | 'pagebar'` and `opts` is
`{ key, template }` (`LSPlugin.ts:549-552`). Handlers are referenced by name from `provideModel`,
because event handlers cannot cross the frame boundary as strings:

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

Arbitrary DOM injection is `logseq.provideUI(ui)` (`:1194`), which accepts either a `path`
(CSS selector) or a `slot` identity, plus `template`, `style`, `attrs`, `key`, `close`, `replace`
and `reset` (`UIOptions`, `LSPlugin.ts:39-67`).

**6. Styles and themes** — `logseq.provideStyle(styleOrOptions)` (`:1174`) and
`logseq.provideTheme(theme)` (`:1161`).

**7. Experimental renderers** — `logseq.Experiments` adds
`registerFencedCodeRenderer`, `registerDaemonRenderer`, `registerHostedRenderer`,
`registerSidebarRenderer`, `registerRouteRenderer`, `registerBlockPropertiesRenderer`,
`registerBlockRenderer` and `registerExtensionsEnhancer`
([`libs/src/modules/LSPlugin.Experiments.ts:105-272`](../../../../../libs/src/modules/LSPlugin.Experiments.ts)).
The SDK's own header warns these may be adjusted at any time and may not be supported on the
Marketplace (`:42-46`).

> Four of these registration APIs accept a `subs` option. It is stored and **never read** anywhere
> under `src/main/` — the SDK's own comment calls it reserved for future use. It does not drive
> re-renders.

---

## Security Model

This section is a summary. The full treatment, with the code that backs each claim, is
[Plugin Security Architecture](../architecture/plugin-security.md).

### There is no sandbox

- The plugin `<iframe>` is created with only `id`, `name`, `allow` and CSS classes
  (`libs/src/postmate/index.ts:402-410`). The string `sandbox=` appears nowhere in the repository
  outside a vendored icon bundle.
- `PluginLocalOptions.allow` — the one attribute that could restrict anything — is declared
  (`libs/src/LSPlugin.core.ts:224`) and read (`libs/src/LSPlugin.caller.ts:289`) but **never
  assigned**, so the Permissions-Policy attribute is never applied either.
- Electron `webPreferences` are window-wide, not per-plugin
  (`src/electron/electron/window.cljs:50-60`): `nodeIntegration false`,
  `nodeIntegrationInWorker false`, `sandbox false`, `webSecurity (not dev?)`,
  `contextIsolation true`. Same-origin policy is off in development builds.
- The `lsp://` scheme is registered `:bypassCSP true`
  (`src/electron/electron/core.cljs:465-476`), and `disableXFrameOptions` strips
  `X-Frame-Options` **and `Content-Security-Policy`** from every response
  (`src/electron/electron/utils.js:9-29`).

### There is no permission model

All plugins have identical reach. API dispatch resolves **by name**, with no allowlist and no schema
validation: `initApiProxyHandlers` (`libs/src/LSPlugin.core.ts:411-422`) hands the message straight
to `invokeHostExportedApi` (`libs/src/common.ts:301-331`), which looks the name up on
`window.logseq.api`, `window.logseq.sdk.<ns>`, and `window.apis` — the Electron preload bridge
(`resources/js/preload.js:36`). A method the SDK never advertised is reachable if it exists.

The only stated security posture is the install-time warning on the Plugins dashboard
([`src/resources/dicts/en.edn:1294`](../../../../../src/resources/dicts/en.edn)):

> "Plugins can access your graph and your local files, issue network requests. They can also cause
> data corruption or loss. We're working on proper access rules for your graphs. Meanwhile, make
> sure you have regular backups of your graphs and only install the plugins when you can read and
> understand the source code."

### What the code does mitigate

| Threat                                          | Mitigation in code                                                                         |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Markup injected through `provideUI`             | `template` is passed through DOMPurify before insertion (`libs/src/common.ts:400-413`)      |
| Arbitrary protocol from `openExternal`          | Preload allowlists `https:`, `http:`, `mailto:`, `zotero:`, `file:` (`resources/js/preload.js:9`, `:84-90`) |
| `custom.js` in a graph                          | Explicit `confirm()` consent, re-asked weekly (`src/main/frontend/handler/ui.cljs:129-165`) |

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

For users: read the source before installing, prefer plugins whose repository you can inspect, and
keep graph backups — the warning above is the whole of the enforcement story.

---

## Communication Patterns

### Plugin → Logseq

Every `logseq.*` call becomes one `api:call` message carrying `{ tag, method, args }`. The `Proxy`
built by `_makeUserProxy` ([`libs/src/LSPlugin.user.ts:1002-1076`](../../../../../libs/src/LSPlugin.user.ts))
sends it at `:1069-1073`. For the `git`, `ui`, `assets` and `utils` namespaces the method name is
prefixed with the namespace (`:1064-1065`); `app`, `editor`, `db` and `commands` are not prefixed.

Host-side, `invokeHostExportedApi` snake-cases the name and resolves it against
`window.logseq.api`, the matching `window.logseq.sdk.<ns>` namespace, the SDK's `callables`, and
`window.apis` — throwing `Not existed method #<name>` if nothing matches
(`libs/src/common.ts:301-333`).

All proxied calls return Promises.

### Logseq → Plugin

Events are delivered over the same channel. `_makeUserProxy` recognises any method name starting
with `on`, `once` or `off` and turns it into a `hook:<ns>:<snake_case_type>` subscription, calling
`App._installPluginHook` / `_uninstallPluginHook` on the host
(`libs/src/LSPlugin.user.ts:1017-1058`). The returned value is an unsubscribe function
(`IUserOffHook`, `LSPlugin.ts:111`):

```javascript
const off = logseq.DB.onChanged(({ blocks }) => { /* ... */ })
// later
off()
```

### The transport

The handshake starts as `window.postMessage`. If both sides agree — `enableMessageChannel: true` is
set for every plugin frame at
[`libs/src/LSPlugin.caller.ts:291`](../../../../../libs/src/LSPlugin.caller.ts) — the parent creates
a `MessageChannel` and transfers a port to the child
(`libs/src/postmate/index.ts:451-468`), and the child swaps its window listener for a port listener
(`:318-341`). See
[Postmate transport: MessageChannel optimization](../../reference/plugin-apis/pr-12395/01-postmate-message-channel.md).

### Plugin → Plugin

There is a supported path, and it is not the graph. `IAppProxy` exposes
(`LSPlugin.ts:472-478`):

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

`load()` is awaited per plugin and the handshake has an 8-second budget before the frame is
destroyed (`libs/src/LSPlugin.caller.ts:296-304`). Keep `main()` to registration only:

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

`load()` logs its own duration (`load:done in <n>ms`, `libs/src/LSPlugin.core.ts:1067-1069`), which
is the quickest way to see whether a plugin is the slow one.

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

Also unsubscribe: every `on*` call returns an off-function, and `beforeunload` is where to call it.

### Debouncing Events

```javascript
let timer
logseq.DB.onChanged(({ blocks }) => {
  clearTimeout(timer)
  timer = setTimeout(() => heavyProcessing(blocks), 1000)
})
```

If you only care about one block, prefer `logseq.DB.onBlockChanged(uuid, cb)`
(`LSPlugin.ts:989-997`) over filtering every transaction yourself.

### Request timeouts

Each Postmate request rejects after `Postmate.requestTimeout` milliseconds — default `10_000`
(`libs/src/postmate/index.ts:4`, applied at `:148-175`). A host API call that takes longer than that
rejects with `Postmate: request timeout (10000ms)` even if the host eventually completes it.

---

## Design Principles

### 1. One channel, name-resolved

There is a single RPC path and a single resolution rule: snake-case the method name and look it up
on the host's exported objects (`libs/src/common.ts:301-331`). That keeps the SDK thin, and it is
also why the surface is impossible to constrain without a registry.

### 2. Async by default

Every proxied call returns a Promise, because every call is a message round trip.

### 3. Event-driven

Hooks are installed on the host and pushed to the plugin rather than polled
(`libs/src/LSPlugin.user.ts:1017-1058`).

### 4. Declarative UI

Plugins describe markup (`provideUI`, `provideStyle`) and name their handlers (`provideModel`),
because functions cannot cross the frame boundary. The host sanitizes the markup with DOMPurify
before injecting it (`libs/src/common.ts:400-413`).

### 5. Structural, not capability, separation

A plugin can do whatever the host exports. The frame is a boundary for *organisation* — see
[Security Model](#security-model).

---

## Known Gaps

Grounded in what the code says today, not a roadmap:

- **No access rules.** The dashboard warning states outright that they are still being worked on
  (`src/resources/dicts/en.edn:1294`).
- **`allow` is dead.** Declared and read, never assigned
  (`libs/src/LSPlugin.core.ts:224`, `libs/src/LSPlugin.caller.ts:289`).
- **`logseq.Git` is dead.** Declared and exposed, with no host implementation
  (`libs/src/common.ts:331-333`).
- **`subs` is inert.** Accepted by four `Experiments` registration APIs, stored, never read under
  `src/main/`.

---

## Summary

1. **One renderer, one channel.** Plugins run in a plain `<iframe>` (or, in `shadow` mode, directly
   in the host realm) and reach the host through Postmate, upgraded to a `MessageChannel` after the
   handshake.
2. **Five lifecycle states** with guarded transitions, driven by `PluginLocal.load()` / `unload()` /
   `reload()`.
3. **Ten SDK namespaces**, one of which (`Git`) has no host implementation.
4. **Extension points** are commands, slash commands, context-menu items, renderers, UI slots,
   styles/themes, and the experimental renderer family.
5. **No sandbox, no permissions.** The separation is structural. Installing a plugin is running
   arbitrary code.
6. **Performance** is your responsibility: register cheaply, debounce `DB.onChanged`, unsubscribe in
   `beforeunload`, and remember the 10-second RPC timeout.

---

## Next Steps

- [First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)
- [Plugin API Reference](../../reference/api/plugin-api.md)
- [Plugin Security Architecture](../architecture/plugin-security.md)
- [PR 12395 API notes](../../reference/plugin-apis/pr-12395/README.md)

The authoritative surface is the type file itself:
[`libs/src/LSPlugin.ts`](../../../../../libs/src/LSPlugin.ts).
