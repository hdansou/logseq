# Postmate transport: MessageChannel optimization

## Summary

The Postmate handshake between the host page and a plugin iframe now establishes a dedicated `MessageChannel` for all RPC traffic after the initial handshake. Previously every call went through `window.postMessage` on the global window, which forced both sides to filter every cross-frame message.

The legacy `postMessage` transport is preserved as a fallback so older hosts (pre-0.3.1) still work with new plugins, and old plugins still work in the new host.

## Why it matters for plugin authors

Nothing to import. Just rebuild against `@logseq/libs@0.3.1` and your plugin automatically uses the faster path when running on a Logseq build that includes this PR.

Concrete benefits:

- **Lower per-call overhead** — no broadcast filtering, no `event.source` checks per message.
- **Better isolation** — host messages from other iframes can't be observed by your plugin.
- **Improved GC** — channels and listeners are torn down deterministically when the plugin unloads (`refactor(libs): enhance transport listener and improve garbage collection handling`).

High-frequency call sites benefit the most:

- streaming UI updates from `registerDaemonRenderer` / `registerHostedRenderer`
- chained `DB.datascriptQuery` calls
- cursor / selection sync handlers
- progress callbacks from long-running ops

## Verifying the transport is active

In the plugin iframe DevTools console:

```js
// inspect the active caller — when a MessagePort is bound, transport is upgraded
window.__LSP__?.caller?._port // truthy = MessageChannel path active
```

If `_port` is undefined the host hasn't been upgraded; the plugin still works via the legacy listener.

## Source

- `libs/src/postmate/index.ts` — handshake + channel setup (+312 / -148).
- `libs/src/LSPlugin.caller.ts` — caller-side listener refactor (+122 / -99).
- `libs/src/LSPlugin.core.ts` — host-side listener refactor (+196 / -84).
- Commits of interest:
  - `refactor(libs): optimize message transport using MessageChannel`
  - `enhance(libs): improve MessageChannel handling and backward compatibility`
  - `refactor(libs): streamline handshake process and improve error handling`
