# Investigation: X-Callback-URL and URL Schemes in Logseq

> **Last revised:** 2026-05-30

---

## Scope

This document validates Logseq URL handling against the current source in `src/`, `deps/`, and `packages/`.

Primary implementation references:

- `src/electron/electron/core.cljs`
- `src/electron/electron/url.cljs`
- `src/main/electron/listener.cljs`
- `src/main/frontend/quick_capture.cljs`
- `src/main/frontend/handler/events.cljs`
- `src/main/frontend/mobile/intent.cljs`
- `src/main/mobile/init.cljs`
- `src/main/mobile/deeplink.cljs`
- `src/main/frontend/schema/handler/common_config.cljc`

`deps/` and `packages/` currently contain no x-callback-url implementation in scope for this investigation.

---

## Executive Summary

Current Logseq URL handling is split into two separate runtime paths:

1. Desktop x-callback-url handling lives in Electron and currently supports `quickCapture` and `invokeCommand`.
2. Mobile uses separate deeplink hosts such as `logseq://mobile/...`, `logseq://graph/...`, and `logseq://shared?...`; it does not route `x-callback-url` actions in `src/`.
3. Quick capture formatting and page selection are shared frontend logic in `src/main/frontend/quick_capture.cljs`.
4. Windows and Linux deeplinks are handled from process arguments in addition to the macOS `open-url` event.

---

## Current Architecture (Validated)

### Desktop x-callback-url flow

Desktop URL handling is implemented in Electron:

1. `src/electron/electron/core.cljs` registers `logseq://` as the application protocol.
2. macOS deeplinks arrive through the Electron `open-url` event.
3. Windows and Linux deeplinks are extracted from process arguments on first launch and on `second-instance`.
4. `src/electron/electron/url.cljs` routes URLs by host.
5. For host `x-callback-url`, Electron forwards the action to the focused renderer.
6. `src/main/electron/listener.cljs` converts the IPC message into a frontend event.
7. `src/main/frontend/quick_capture.cljs` or `src/main/frontend/handler/events.cljs` performs the actual behavior.

### Mobile deeplink flow

Mobile URL handling is separate from x-callback-url:

1. `src/main/mobile/init.cljs` listens for Capacitor `appUrlOpen` and initial launch URLs.
2. `src/main/mobile/deeplink.cljs` handles `logseq://mobile/...`, `logseq://graph/...`, and `logseq://shared?...` routes.
3. `src/main/frontend/mobile/intent.cljs` processes shared payloads and legacy Android share results.
4. Mobile share/deeplink handling reuses quick-capture events where appropriate.

This is a shared capture pipeline with different entrypoints, not one cross-platform x-callback-url implementation.

---

## X-Callback-URL Behavior (Validated)

Current x-callback-url handling is defined by `x-callback-url-handler` in `src/electron/electron/url.cljs`.

### Supported actions

1. `/quickCapture`
2. `/invokeCommand`

### `quickCapture`

Accepted parameters:

- `url`
- `title`
- `content`
- `page`
- `append`

Electron forwards those values to the renderer IPC channel `quickCapture`, which publishes `[:editor/quick-capture args]`.

`src/main/frontend/quick_capture.cljs` then:

1. Resolves the destination page.
2. Formats links and embeds.
3. Applies the configured text template.
4. Inserts into the current edit session or creates a new block.

### `invokeCommand`

Accepted parameters:

- `action`
- `payload`

Electron forwards those values to the renderer IPC channel `invokeCommand`, which publishes `[:editor/invoke-command args]`.

`src/main/frontend/handler/events.cljs` currently expects `action` in dotted form, splits it into `[type id group ...]`, and dispatches only the `:plugin` case through `plugin-api/invoke_external_plugin_cmd`.

### Unsupported callback semantics

No code in `src/`, `deps/`, or `packages/` currently handles classic x-callback-url callback parameters such as:

- `x-success`
- `x-error`
- `x-cancel`

Current behavior is one-way dispatch into Logseq; unsupported actions trigger an in-app error notification.

---

## Other URL Hosts and Deeplinks (Validated)

### Electron URL hosts

`src/electron/electron/url.cljs` also routes these non-x-callback hosts:

- `graph`
- `new-window`
- `handbook`

`graph` and `new-window` open a local graph and optionally redirect to `page`, `block-id`, or `file`.

### Mobile deeplink hosts

`src/main/mobile/deeplink.cljs` currently handles:

- `mobile` with `/go/quick-add`
- `mobile` with `/go/audio`
- `graph`
- `shared`

It also first checks for parsed Logseq web targets through `frontend.util.url/parse-web-url-target`, so mobile deeplinks are broader than raw `logseq://` host routing.

---

## Quick Capture Configuration (Validated)

The config schema in `src/main/frontend/schema/handler/common_config.cljc` allows:

- `:quick-capture-templates`
  - `:text`
  - `:media`

- `:quick-capture-options`
  - `:insert-today?`
  - `:redirect-page?`
  - `:default-page`
  - `:prettify-url?`

Current runtime behavior in `src/main/frontend/quick_capture.cljs` is:

1. `insert-today?` defaults to `false` in the getter used by quick capture.
2. `redirect-page?` defaults to `false`.
3. `prettify-url?` defaults to `true`.
4. When no explicit page is supplied, quick capture falls back through today, explicit default page, current page, then a final fallback page depending on whether journals are enabled.

For x-callback-url captures, the text template is the relevant template path. Mobile shared media processing additionally uses the media template path in `src/main/frontend/mobile/intent.cljs`.

---

## Findings: Removed or Stale Claims

> **Removed:** The claim that x-callback-url is implemented as one cross-platform protocol layer across desktop and mobile. Current `src/` code only routes `x-callback-url` in Electron.
> **Removed:** Claims that mobile quick capture is performed through `logseq://x-callback-url/...`. Mobile currently routes through `logseq://mobile/...`, `logseq://graph/...`, and `logseq://shared?...`.
> **Removed:** Claims that `/quickCapture` is the only supported x-callback-url action. Current Electron code also supports `/invokeCommand`.
> **Removed:** Claims that Logseq implements x-callback success or error callback URLs. Current code only dispatches the action internally and shows a notification for unsupported actions.
> **Removed:** Previous default-value guidance that treated `:insert-today?` as defaulting to `true` in the runtime quick-capture path. The current getter in `src/main/frontend/quick_capture.cljs` uses `false` as its default.
> **Removed:** Native iOS and Android registration details as source-of-truth evidence for this validation pass. The requested validation scope was `src/`, `deps/`, and `packages/`, and the deciding behavior documented here is fully observable from the current `src/` code paths.

---

## Findings: New Capabilities

1. Desktop x-callback-url now supports `logseq://x-callback-url/invokeCommand` in addition to `quickCapture`.
2. Desktop deeplink handling now includes Windows and Linux argument-based startup and subsequent-instance routing in `src/electron/electron/core.cljs`.
3. Electron URL routing includes additional non-x-callback hosts for `graph`, `new-window`, and `handbook`.
4. Quick capture now exposes `:prettify-url?` in the validated config schema and runtime.
5. Mobile startup suppresses duplicate incoming URLs by tracking the last shared URL and timestamp in `src/main/mobile/init.cljs`.
6. Mobile deeplink handling supports both direct `logseq://...` routes and parsed Logseq web targets before falling back to host-based routing.

---

## Practical Takeaways for Reviews

1. Treat desktop x-callback-url support and mobile deeplink/share handling as separate systems that converge only at the frontend quick-capture layer.
2. For desktop automation regressions, inspect `src/electron/electron/url.cljs`, `src/main/electron/listener.cljs`, and `src/main/frontend/quick_capture.cljs` first.
3. For command invocation regressions, inspect `src/main/frontend/handler/events.cljs` and verify the `action` string matches the expected dotted plugin command format.
4. For mobile share or deeplink issues, inspect `src/main/mobile/init.cljs`, `src/main/mobile/deeplink.cljs`, and `src/main/frontend/mobile/intent.cljs` rather than assuming any x-callback-url path.

---

## Evidence Notes

- Protocol registration and deeplink entrypoints: `register-default-protocol-client!`, `setup-deeplink!`, `handle-initial-deeplink!`, and the `second-instance` handler in `src/electron/electron/core.cljs`.
- Desktop x-callback-url routing: `x-callback-url-handler` and `logseq-url-handler` in `src/electron/electron/url.cljs`.
- Renderer IPC bridge: `safe-api-call "quickCapture"` and `safe-api-call "invokeCommand"` in `src/main/electron/listener.cljs`.
- Quick capture runtime behavior: `quick-capture` in `src/main/frontend/quick_capture.cljs`.
- External command dispatch: `handle :editor/invoke-command` in `src/main/frontend/handler/events.cljs`.
- Mobile URL intake: `handle-incoming-url!` and launch handling in `src/main/mobile/init.cljs`.
- Mobile host routing: `deeplink` in `src/main/mobile/deeplink.cljs`.
- Mobile share handling: `handle-payload`, `handle-result`, and `handle-received` in `src/main/frontend/mobile/intent.cljs`.
- Config schema: `Config-edn` in `src/main/frontend/schema/handler/common_config.cljc`.
