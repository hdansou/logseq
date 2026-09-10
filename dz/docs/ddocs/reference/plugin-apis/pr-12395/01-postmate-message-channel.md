---
title: 'Postmate MessageChannel transport'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: wip-unverified
---

# Postmate transport: MessageChannel optimization

> **Shipped.** PR [#12395](https://github.com/logseq/logseq/pull/12395) is merged
> (`a95483655b refactor: plugin libs (#12395)`); this transport is present at `be800f1711`.

## Summary

The Postmate handshake between the host page and a plugin iframe now establishes a dedicated
`MessageChannel` for all RPC traffic after the initial handshake. Previously every call went through
`window.postMessage` on the global window, which forced both sides to filter every cross-frame
message.

The handshake itself still goes over `window.postMessage` — deliberately, "to maintain backward
compatibility"
([`libs/src/postmate/index.ts:494-503`](../../../../../../libs/src/postmate/index.ts)). Each side
advertises support and only upgrades if the other agrees:

- Parent sends `enableMessageChannel: 1` in the handshake (`:500`).
- Child records it and replies with `acceptsMessageChannel` (`:569`, `:582-590`).
- Parent then creates the channel and transfers `port2` in a `setup-channel` message (`:451-468`),
  or adopts a port the child returned (`:440-450`).
- Child removes its window `message` listener and re-attaches the same listener to the port
  (`:318-341`).

If either side does not advertise support, both stay on `window.postMessage` — so an old plugin
works in the new host and a new plugin works in an old host.

## Why it matters for plugin authors

Nothing to import. Rebuild against `@logseq/libs` 0.3.1 or later (the PR bumped `libs/package.json`
from `0.2.12` to `0.3.1`; the current version at this revision is **0.3.4**) and your plugin
automatically uses the faster path when running on a Logseq build that includes this PR.

Concrete benefits, each visible in the transport code:

- **Lower per-call overhead.** On the port path both sides skip `sanitize()` and its
  origin/`event.source` check, validating only the message shape (`:109-118` parent, `:264-273`
  child).
- **Better isolation.** After the upgrade the child's `window` listener is explicitly removed
  (`:328`), so messages the host page exchanges with other frames are no longer delivered to your
  plugin's handler at all.
- **Deterministic teardown.** `ParentAPI.destroy()` removes the transport listener, calls
  `messagePort.close()`, and removes the iframe (`:205-216`). Per-request listeners are also removed
  on reply or timeout (`:155-175`).

High-frequency call sites benefit the most:

- streaming UI updates from `logseq.Experiments.registerDaemonRenderer` /
  `registerHostedRenderer`
  ([`libs/src/modules/LSPlugin.Experiments.ts:122`, `:138`](../../../../../../libs/src/modules/LSPlugin.Experiments.ts))
- chained `logseq.DB.datascriptQuery` calls
- cursor / selection sync handlers
- progress callbacks from long-running ops

## Behavior change: requests now time out

The same PR introduced a per-request timeout. Every `ParentAPI.get()` call rejects with
`Postmate: request timeout (10000ms)` if no reply arrives within `Postmate.requestTimeout`
milliseconds, defaulting to `defaultRequestTimeout = 10_000`
(`libs/src/postmate/index.ts:4`, `:384`, applied at `:148-175`).

There is no plugin-facing knob for this — `Postmate` is internal to the SDK. If a host API call of
yours used to take longer than ten seconds, it now rejects even when the host eventually completes
the work. Break long operations into smaller calls, or drive them with a hook instead of one
blocking request.

The handshake has its own separate budgets: five attempts at 500 ms intervals
(`maxHandshakeRequests = 5`, `:5`, `:505`), inside an 8-second ceiling enforced by the caller which
destroys the frame on expiry (`HANDSHAKE_TIMEOUT = 8_000`,
[`libs/src/LSPlugin.caller.ts:296-304`](../../../../../../libs/src/LSPlugin.caller.ts)).

## Verifying the transport is active

The SDK does not expose the port to plugin code, so there is nothing to inspect from inside your
plugin. The observable signal is on the **host** side. `ParentAPI.addTransportListener` logs
unconditionally (it is not gated on `NODE_ENV`), so the host DevTools console shows one line per
plugin frame (`libs/src/postmate/index.ts:67-79`):

```text
[DEBUG] Using MessagePort for communication: lsp://logseq.io/plugins/...
```

versus, on the legacy path:

```text
[DEBUG] Using postMessage for communication: lsp://logseq.io/plugins/...
```

The richer `log('Child: Successfully switched to MessagePort...')` traces (`:338`) exist only in
development builds — they are guarded by `process.env.NODE_ENV !== 'production'` and are stripped
from the shipped `dist/lsplugin.user.js`.

> **Unverified:** the earlier revision of this page suggested checking
> `window.__LSP__?.caller?._port` from the plugin console. No such global and no such field exist at
> this revision — the only `__LSP__*` global in the SDK is `window.__LSP__HOST__`
> (`libs/src/LSPlugin.user.ts:59`, `:1175`), and the port lives in private fields of `ParentAPI` /
> `ChildAPI`. That check has been removed.

## Source

At `be800f1711`, with the diff sizes from `a95483655b`:

- [`libs/src/postmate/index.ts`](../../../../../../libs/src/postmate/index.ts) — handshake + channel
  setup (+312 / -148).
- [`libs/src/LSPlugin.caller.ts`](../../../../../../libs/src/LSPlugin.caller.ts) — caller-side
  listener refactor (+122 / -99); `enableMessageChannel: true` is set for every plugin frame at
  `:291`.
- [`libs/src/LSPlugin.core.ts`](../../../../../../libs/src/LSPlugin.core.ts) — host-side listener
  refactor (+196 / -84).
- Commits on the PR branch (squashed into `a95483655b` on master):
  - `a9f1df22ae refactor(libs): optimize message transport using MessageChannel`
  - `925a65fb43 enhance(libs): improve MessageChannel handling and backward compatibility`
  - `677c8c2791 refactor(libs): streamline handshake process and improve error handling`
  - `b280431210 refactor(libs): enhance transport listener and improve garbage collection handling`
