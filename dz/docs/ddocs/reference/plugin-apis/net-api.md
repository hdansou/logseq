---
title: 'logseq.Net: HTTP client for plugins'
description: 'What logseq.Net does, which @logseq/libs version has it, how it differs on desktop and web, and how to verify it with a probe plugin'
type: reference
status: active-development
lastVerified: '2026-10-05'
verifiedAgainst: '1d1dfd680c'
reviewStatus: verified-live
verifiedScope: 'Verified live on 2026-10-05: the probe plugin (8 probes) on app.logseq.com, and on the installed Logseq-DB 2.0.1 desktop app. Everything about retry options and POST/PUT/PATCH/DELETE comes from reading the source and was not exercised.'
logseqVersion: '2.0.1'
platforms: [desktop, web]
graphType: [db]
---

# logseq.Net: HTTP client for plugins

`logseq.Net` is the plugin SDK's HTTP client. On desktop it sends requests from the Electron main
process, so a plugin can call sites that send no CORS headers. On the web it is a thin wrapper
around `fetch` and is bound by CORS.

Source: [`libs/src/modules/LSPlugin.Net.ts`](../../../../../libs/src/modules/LSPlugin.Net.ts).
Added by `3c2359a79e` (PR #12588, 2026-05-07). That commit is in the `2.0.1` tag.

## Get the right SDK version

`npm install @logseq/libs` installs **0.0.17**, which has no `Net`. That is the `latest` dist-tag.
The newer releases are published but tagged `next`.

| dist-tag | version | has `Net` |
|---|---|---|
| `latest` | 0.0.17 | no |
| `next` | 0.3.4 | yes |

Install an explicit range:

```sh
npm install @logseq/libs@^0.3.4
```

Checked against the npm registry on 2026-10-05. Versions 0.3.1 to 0.3.4 exist.

The two samples that make HTTP calls (`logseq-imdb-top250-importer`, `logseq-reddit-hot-news`) use
plain `fetch()`, and the typedoc site at logseq.github.io/plugins does not list `Net`. This page
did not check why. The source file above is the reference.

## API

`logseq.Net` has these public methods: `request`, `get`, `head`, `post`, `put`, `patch`, `delete`.

| Method | Returns | Notes |
|---|---|---|
| `request(options)` | `Promise<LSPluginNetResponse>` | Takes `{url, method, headers, body, responseType, timeout, retry, cache, signal}`. Gives you `status`, `statusText`, `ok`, `url`, `headers`. |
| `get(url, options?)`, `head`, `post(url, body?, options?)`, `put`, `patch`, `delete` | `Promise<T>` | Resolve to the **response body**, not a response object. Reject with `LSPluginNetError` on a non-2xx status. |

Use `request()` when you need the status or headers. Use `get()` when you only need the body.

Defaults, from the source:

- `responseType` is `json`. Other values: `text`, `base64`, `arraybuffer`.
- Caching is **off**. Pass `cache: true` or `{key, ttl, maxEntries}`. When on, it applies to GET
  and HEAD only, with a 5-minute TTL and 100 entries.
- Retries are **off**. Pass `retry: n` or `{retries, delay, factor, retryOn}`. The defaults are a
  300 ms delay with a factor of 2, retrying on 429 and 5xx.

## Desktop versus web

| | Desktop | Web |
|---|---|---|
| Transport | Electron main process (`httpRequest` handler, `src/electron/electron/handler.cljs`) | `fetch` in the plugin iframe |
| Site without CORS headers | works | fails with `Failed to fetch` |
| Selection | the SDK calls the host function `exper_request`, then reads the result from a callback | `baseInfo.webMode` is true, so it calls `fetch` directly |

If the proxy call returns nothing, the SDK falls back to `fetch` (`shouldFallbackAfterProxyError`).

## Observed behavior

These are results from the probe plugin, not source reading.

| Probe | Desktop 2.0.1 | Web (app.logseq.com) |
|---|---|---|
| `get()` on a CORS-enabled URL | body as a string | same |
| `get()` on a site with no CORS headers | works | `Failed to fetch` |
| plain `fetch()` on the same site | `Failed to fetch` | `Failed to fetch` |
| `request()` | `status=200 ok=true`, 22 headers | `status=200 ok=true`, 8 headers |
| `timeout: 1` | rejects with `Error invoking remote method 'main': AbortError: The operation was aborted.` | rejects with `signal is aborted without reason` |
| HTTP 404 | rejects with `HTTP request failed with status 404 Not Found` | rejects with `HTTP request failed with status 404` |
| `cache: true`, second identical call | about 51 ms, then 0 ms | about 39 ms, then 0 ms |

Two consequences:

- A timeout surfaces as a different message on each platform. On desktop it is an Electron IPC
  error string. Match on the text or the `LSPluginNetError` class, not on `AbortError` alone.
- `get()` returns the body. Reading `.status` on its result gives `undefined`.

## Verify it yourself

The probe plugin lives in its own repository, `logseq-net-probe`. It vendors `@logseq/libs` 0.3.4,
so it needs no build.

1. Turn on Developer mode: Settings, Advanced.
2. Plugins, **Load unpacked plugin**, and pick the folder.
3. A panel opens and runs the probes. Each row shows PASS or FAIL against the expected outcome for
   the platform. On desktop, the `get()` row for a site without CORS headers must PASS.

All requests are read-only GETs to `api.github.com` and `example.com`. Nothing is written to the
graph.

### Driving it without a UI

On web, serve the folder with `npx http-server -p 8299 --cors` and register it from the page
console with `LSPluginCore.register({key, url, id, webPkg})`. Stock Chromium blocks pages on
`https://app.logseq.com` from reaching `http://localhost`, which shows up as `handshake Timeout`.
Launch the browser with the local-network-access checks disabled.

On desktop, do not call `LSPluginCore.register` in a second instance: it shares `~/.logseq` with
your own app. Load the plugin in your own app, then start the second instance and read
`window.__netProbeResults` from the plugin frame over CDP.

## Not verified

- `post`, `put`, `patch`, `delete`, `retry`, and `abortable` request behavior. Only GET was run.
- `Net` on mobile builds.
- Whether the web fallback or proxy selection changes with future builds. The checks above are
  dated 2026-10-05.
