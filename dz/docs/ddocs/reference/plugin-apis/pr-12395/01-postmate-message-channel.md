---
title: 'Postmate MessageChannel transport'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
---

# Postmate transport: MessageChannel optimization

> **Shipped.** PR [#12395](https://github.com/logseq/logseq/pull/12395) is merged
> (`a95483655b refactor: plugin libs (#12395)`); this transport is present at `be800f1711`.

All line references below are to
[`libs/src/postmate/index.ts`](../../../../../../libs/src/postmate/index.ts) at `be800f1711`
unless another file is named.

## Summary

After the Postmate handshake between the host page and a plugin iframe, both sides can move their
RPC traffic onto a dedicated `MessageChannel` port. Without the port, the host's `ParentAPI`
listens on the host `window` (`:67-79`) and the plugin's `ChildAPI` listens on the plugin `window`
(`:228-236`). Every `message` event on those windows then has to pass `sanitize()` (`:34-43`).

The handshake itself always goes over `window.postMessage`, per the source comment "to maintain
backward compatibility" (`:494-503`). Each side advertises support and upgrades only if the other
agrees:

1. The host creates every plugin frame with `enableMessageChannel: true`
   ([`libs/src/LSPlugin.caller.ts:283-292`](../../../../../../libs/src/LSPlugin.caller.ts)).
   The parent sends `enableMessageChannel: 1` in the handshake when `MessageChannel` is available
   (`:422-427`, `:500`).
2. The child records the flag (`:569`) and replies with `acceptsMessageChannel: 1` or `0`
   (`:582-590`).
3. On an accepting reply, the parent creates a `MessageChannel`, keeps `port1`, and transfers
   `port2` in a `setup-channel` message (`:451-469`). The parent also has a branch that adopts a
   port transferred with the reply (`:442-450`), but the child at this revision sends its reply
   without a transfer list (`:582-590`), so that branch is not exercised by this SDK.
4. The child accepts `setup-channel` only from the recorded parent origin (`:604-610`). It then
   dispatches a `postmate:channel-ready` event on its window (`:621-623`).
5. `ChildAPI` handles that event: it removes its window `message` listener and attaches the same
   listener to the port (`:318-345`).

If either side does not advertise support, both stay on `window.postMessage`. An old plugin never
sends `acceptsMessageChannel`, so the parent never creates a channel. A new plugin in an old host
never receives `enableMessageChannel`, so it replies with `acceptsMessageChannel: 0` (`:587`).

## Why it matters for plugin authors

Nothing to import. The PR bumped `libs/package.json` from `0.2.12` to `0.3.1`; at `be800f1711` the
version is **0.3.4** ([`libs/package.json:4`](../../../../../../libs/package.json)). A plugin built
against 0.3.1 or later uses the port whenever the host also supports it.

What changes, as visible in the transport code:

- **No origin check per message on the port path.** Both listeners skip `sanitize()` and validate
  only the message shape: `postmate` key, `type`, and a known message kind (`:109-118` parent,
  `:264-273` child). `sanitize()` adds a comparison of `event.origin` to the allowed origin
  (`:35-36`).
- **The window listener is removed.** After the upgrade the child's `window` listener is detached
  (`:328`). Other `message` events posted to the plugin's window no longer reach the Postmate
  handler.
- **Teardown closes the port.** `ParentAPI.destroy()` removes the transport listener, calls
  `messagePort.close()`, and removes the iframe (`:205-216`). `Postmate.destroy()`, used when the
  handshake times out, also closes the port (`:524-534`). Per-request reply listeners are removed
  on reply or on timeout (`:155-175`).

Once the port is active, every later request, reply, `call` and `emit` uses it: `postToChild`
(`:89-95`) and `postToParent` (`:238-250`) both prefer the port.

> **Unverified:** earlier revisions of this page claimed specific call sites benefit most (hosted
> and daemon renderers, chained `datascriptQuery` calls, selection sync, progress callbacks). No
> measurement or code in the repository supports a performance ranking, so that list was removed.

## Behavior change: host-to-plugin requests now time out

The PR added a per-request timeout to `ParentAPI.get()`. The previous revision of this file has no
`requestTimeout` (`a95483655b~1:libs/src/postmate/index.ts`). If no reply arrives within
`Postmate.requestTimeout` ms, `get()` rejects with `Postmate: request timeout (10000ms)`. The
default is `defaultRequestTimeout = 10_000` (`:4`, `:384`); the timer is armed at `:149-175`.

This applies to the **host awaiting your plugin**, not to your plugin awaiting the host:

- **Host → plugin (new timeout).** `ParentAPI.get()` is used only when the host calls a plugin
  model function with the await flag
  ([`libs/src/LSPlugin.caller.ts:333-338`](../../../../../../libs/src/LSPlugin.caller.ts)). The
  flag is added by `callUserModelAsync` (`LSPlugin.caller.ts:216-219`). Callers at this revision:
  - `call-plugin-user-model!`
    ([`src/main/frontend/handler/plugin.cljs:269-273`](../../../../../../src/main/frontend/handler/plugin.cljs)),
    used for plugin settings button actions
    ([`src/main/frontend/components/plugins_settings.cljs:106-110`](../../../../../../src/main/frontend/components/plugins_settings.cljs))
    and for the `:models` group of `invoke_external_plugin_cmd`
    ([`src/main/logseq/api/plugin.cljs:401-405`](../../../../../../src/main/logseq/api/plugin.cljs)).
  - The before-unload message on plugin unload
    ([`libs/src/LSPlugin.core.ts:1143-1146`](../../../../../../libs/src/LSPlugin.core.ts)). A
    rejection there is caught and logged as `beforeunload hook failed`, and unloading continues
    (`LSPlugin.core.ts:1148-1152`).

  A model function that takes longer than ten seconds now makes the host's promise reject, even if
  the function later completes.
- **Plugin → host (unchanged).** SDK API calls such as `logseq.Editor.*` go through `callAsync`.
  It sends the call with `ChildAPI.emit` (`LSPlugin.caller.ts:159-172`) and waits on a
  `deferred(1000 * 10)` actor (`LSPlugin.caller.ts:207-209`). That actor rejects with
  `[deferred timeout] async call #N`
  ([`libs/src/common.ts:280-285`](../../../../../../libs/src/common.ts); tag set at
  `LSPlugin.caller.ts:165`). The same ten-second actor existed before the PR
  (`a95483655b~1:libs/src/LSPlugin.caller.ts:205`), so this PR did not change it.

There is no plugin-facing setting for either timeout.

The handshake has its own separate budgets:

- The parent sends up to five handshake attempts, 500 ms apart (`maxHandshakeRequests = 5`, `:5`,
  `:505-507`, `:512`).
- The caller races the handshake against `HANDSHAKE_TIMEOUT = 8_000`. On expiry it calls
  `pt.destroy()` and rejects with `handshake Timeout`
  ([`libs/src/LSPlugin.caller.ts:296-310`](../../../../../../libs/src/LSPlugin.caller.ts)).

## Verifying the transport is active

The SDK does not expose the port to plugin code, so there is nothing to inspect from inside your
plugin. The observable signal is on the **host** side. `ParentAPI.addTransportListener` logs with
`console.debug`, which is not gated on `NODE_ENV` or `Postmate.debug` (`:67-79`):

```text
[DEBUG] Using MessagePort for communication: <plugin frame src>
```

or, on the legacy path:

```text
[DEBUG] Using postMessage for communication: <plugin frame src>
```

Two details:

- **Log level.** `console.debug` messages appear only when the DevTools console shows the Verbose
  level.
- **Frequency.** `addTransportListener` runs at `ParentAPI` construction (`:139`) and again for
  every `get()` request (`:168`). Expect one line at connect plus one per awaited host → plugin
  call, not one line per frame.

The frame `src` is the plugin entry URL with a `__v__` query parameter added
(`LSPlugin.caller.ts:260-265`).

The detailed `log(...)` traces, such as `Child: Successfully switched to MessagePort...` (`:338`),
are doubly gated:

- They are wrapped in `process.env.NODE_ENV !== 'production'`.
- `log` prints only when `Postmate.debug` is true (`:10`); it defaults to `false` (`:383`).

The string `Successfully switched` does not occur in the committed `resources/js/lsplugin.user.js`
or `resources/js/lsplugin.core.js` at `be800f1711`. `Using MessagePort` does occur in both.

> **Note:** an earlier revision of this page suggested checking `window.__LSP__?.caller?._port`
> from the plugin console. No such global or field exists at `be800f1711`. The only `__LSP__*`
> global in the SDK is `window.__LSP__HOST__`
> ([`libs/src/LSPlugin.user.ts:59`](../../../../../../libs/src/LSPlugin.user.ts), `:1175`). The port
> is held in `messagePort` fields on `ParentAPI` / `ChildAPI` (`:65`, `:225`). That check has been
> removed.

## Source

At `be800f1711`, with the diff sizes from `a95483655b` (`git show --numstat`):

- [`libs/src/postmate/index.ts`](../../../../../../libs/src/postmate/index.ts): handshake and
  channel setup (+312 / -148).
- [`libs/src/LSPlugin.caller.ts`](../../../../../../libs/src/LSPlugin.caller.ts): caller-side
  listener refactor (+122 / -99). `enableMessageChannel: true` is set for every plugin frame at
  `:291`.
- [`libs/src/LSPlugin.core.ts`](../../../../../../libs/src/LSPlugin.core.ts): host-side refactor
  (+196 / -84).
- Commits on the PR branch, squashed into `a95483655b`. None of them is an ancestor of master:
  - `a9f1df22ae refactor(libs): optimize message transport using MessageChannel`
  - `925a65fb43 enhance(libs): improve MessageChannel handling and backward compatibility`
  - `677c8c2791 refactor(libs): streamline handshake process and improve error handling`
  - `b280431210 refactor(libs): enhance transport listener and improve garbage collection handling`
