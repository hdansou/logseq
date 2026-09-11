---
title: 'HTTP API Reference'
description: "Reference for the Logseq desktop app's HTTP API server for external application integration"
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop]
graphType: [db]
sinceVersion: '0.10.0'
relatedDocs:
  reference:
    - '[MCP Server Reference](mcp-server.md)'
    - '[Plugin API Reference](plugin-api.md)'
    - '[db-worker HTTP API](db-worker-http-api.md)'
  howTo:
    - '[CLI Integration](../../how-to/integrations/cli-basic.md)'
---

# HTTP API Reference

> **Last revised:** 2026-09-11

> Reference for the desktop app's HTTP API server, which lets external programs call Logseq's plugin API methods over HTTP.

## Overview

The HTTP API server is a [Fastify](https://fastify.dev/) server that runs in the Electron main process
(`src/electron/electron/server.cljs`). It forwards each request to the renderer, which calls the matching
plugin API method and sends the result back. You can use it to:

- **Automate**: create, read, update and delete blocks and pages
- **Integrate**: connect Logseq with external tools and services
- **Control the app remotely**: trigger Logseq actions from scripts
- **Connect AI clients**: through the `/mcp` endpoint (see [MCP Server](mcp-server.md))

It exists only in the desktop app. The settings row is rendered only `(when (util/electron?) ...)`
(`src/main/frontend/components/settings.cljs:1156-1157`).

> This is not the HTTP API of the db worker. The desktop renderer also talks to its database worker
> process over HTTP/SSE. That internal interface is documented separately in
> [db-worker HTTP API](db-worker-http-api.md).

**Base URL (default)**: `http://127.0.0.1:12315` (`src/electron/electron/server.cljs:20-21`)

---

## Quick Reference

| Endpoint      | Purpose                          | Auth checked |
| ------------- | -------------------------------- | ------------ |
| `GET /`       | HTML documentation page          | No           |
| `POST /api`   | Main API endpoint                | Yes          |
| `POST /mcp`   | MCP Streamable HTTP requests     | Yes          |
| `GET /mcp`    | MCP SSE stream for a session     | Yes          |
| `DELETE /mcp` | Close an MCP session             | Yes          |

The routes are registered in `start!` (`src/electron/electron/server.cljs:166-179`). `GET /` serves
`docs/api_server.html` (`resources/docs/api_server.html`) with the host and port filled in (`:169-177`).
The `/mcp` routes exist only if the MCP Server setting was on **when the server started** (`:178-179`).

Other server settings, from `start!` (`server.cljs:160-164`):

- CORS allows every origin (`{:origin "*"}`).
- Requests time out after 42 seconds (`:requestTimeout (* 1000 42)`).

---

## Getting Started

### Enable the Server

1. Open **Settings → Features** and turn on **HTTP API server** (`settings.cljs:783-786`, `:1129`,
   `:1156-1157`; labels `src/resources/dicts/en.edn:1663`, `:1691`). The switch saves a local-storage flag and
   shows a **Restart** button that relaunches the app (`settings.cljs:617-630`).
2. After the restart, an API icon appears in the header (`src/main/frontend/components/header.cljs:601-602`).
3. Click the icon, then **Start server** (`src/main/frontend/components/server.cljs:171-174`). While the server
   runs, the menu shows its URL (`:189-195`).

> **The MCP Server setting does not start the server.** Turning on **Settings → AI → MCP Server** sets the
> same "HTTP API server" flag if it was off (`settings.cljs:1426-1428`). It restarts the server only if the
> server is already running (`:1429-1433`). You still have to start the server from the header icon.

### Configuration

Open it from the API icon → **Server configurations** (`src/main/frontend/components/server.cljs:182-186`;
label `en.edn:1669`).

| Setting    | Config key          | Default     | Description                                                   |
| ---------- | ------------------- | ----------- | ------------------------------------------------------------- |
| Host       | `:server/host`      | `127.0.0.1` | Address the server listens on (`src/electron/electron/server.cljs:20`, `:181`) |
| Port       | `:server/port`      | `12315`     | Port the server listens on (`src/electron/electron/server.cljs:21`, `:181`) |
| Auto start | `:server/autostart` | unset (off) | Restart the server when the app window loads (`src/main/frontend/components/server.cljs:138-145`) |

The settings are saved as `:server/<key>` items (`src/electron/electron/server.cljs:53-62`) in
`configs.edn`, which lives in Electron's `userData` directory (`src/electron/electron/configs.cljs:11-12`).
**Save and apply** restarts the server only when the host or port changed
(`src/main/frontend/components/server.cljs:113-121`).

> **Auto start is off by default, but the checkbox shows it as on.** An unset `:server/autostart` is `nil`,
> so the auto-start check at `src/main/frontend/components/server.cljs:142` does nothing. The checkbox is
> rendered `:checked (not (false? autostart))` (same file, `:106`), which shows `nil` as checked. To turn auto
> start on, untick the box, tick it again, and save.

If you change the host to something other than a loopback address, the API is reachable from other
machines. Because CORS allows every origin, also read [Authentication](#authentication) before you do that.

---

## Authentication

A Fastify `preHandler` hook checks the `Authorization` header on every route except `/`
(`src/electron/electron/server.cljs:92-103`, `:167`). The check is `validate-auth-token` (`:83-90`):

1. It removes the text `Bearer ` from the header value (`:85`). A raw token without the `Bearer ` prefix is
   accepted too.
2. If no token list has **ever been saved** (`:server/tokens` is unset), no further check is made (`:86`).
3. Otherwise the token must equal a saved token's value (`:87-90`). If it does not, the server responds
   `401` with the error `Access Denied!` (`:90`, `:100-103`).

A request **without** an `Authorization` header always gets `401`. The header value is `undefined`, and
`clojure.string/replace` on it throws. The `catch` at `:100-103` then turns that error into a 401. This
was read from source, not run.

> **Warning:** until you save at least one token, any request that has *some* `Authorization` header
> (for example `Authorization: x`) is accepted. Every origin is allowed by CORS (`server.cljs:164`), so
> save a token before you start the server.

### Managing Tokens

1. Click the API icon → **Authorization tokens** (`src/main/frontend/components/server.cljs:176-180`).
2. Click **+ Add new token** (`:54-56`; label `en.edn:1678`).
3. Enter a name and a token value. The refresh button next to a row fills in a generated value
   (`util/unique-id`, `:41-47`).
4. Click **Save** (`:57-62`).

A saved list is stored as `:server/tokens`. It is a vector of `{:name … :value …}` maps
(`src/electron/electron/server.cljs:26-30`, `:56-61`; entry shape from
`src/main/frontend/components/server.cljs:26`). If you delete every token and save, the saved list is empty. From then on every request is
rejected, because step 3 above runs against an empty list.

### Generate Secure Token

Any hard-to-guess string works. For example:

```bash
# macOS/Linux
openssl rand -hex 32

# Node.js
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"

# Python
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### Include in Requests

```bash
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer YOUR_TOKEN_HERE" \
  -H "Content-Type: application/json" \
  -d '{"method": "logseq.App.getCurrentGraph"}'
```

---

## Request Format

All API calls use `POST /api` with a JSON body (`server.cljs:115-130`):

```json
{
  "method": "logseq.Namespace.methodName",
  "args": [arg1, arg2]
}
```

- `method` is required. Without it the server responds `400` with `:method of body is missing!` (`:127-129`).
- `args` is optional. The renderer spreads it as positional arguments (`apply js-invoke`,
  `src/main/electron/listener.cljs:122`).

### Example

```bash
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"method": "logseq.Editor.insertBlock", "args": ["My Page", "Hello World"]}'
```

---

## Response Format

### Success

The response body is the method's return value itself, with **no** wrapper object. The server sends
whatever the renderer returns (`server.cljs:119-125`), and the renderer returns the method's resolved value
(`listener.cljs:122-123`).

```json
{ "uuid": "…", "title": "My Page", "…": "…" }
```

> **Unverified:** how Fastify serializes values that are not objects (a string, `true`, or no return value).
> This was not run.

### Error

If the method throws or its promise rejects, the renderer returns an object with an `error` key
(`listener.cljs:124-129`). The server then sets status `500` (`server.cljs:121-124`):

```json
{ "error": "Error description" }
```

---

## How a Method Name Is Resolved

`resolve-real-api-method` (`src/electron/electron/server.cljs:73-81`) rewrites a name that starts with
`logseq.`:

1. It splits the name on `.`. The **second** segment, lower-cased, is the namespace. The **last** segment
   is the method (`:77-79`).
2. It joins them as `<namespace>@<method>` and snake-cases the result with `csk/->snake_case` (`:80`). So
   `logseq.Editor.getBlock` becomes `editor@get_block`.

The renderer then looks the method up (`src/main/electron/listener.cljs:104-129`):

- Namespaces `app`, `editor`, `db` and `cli` all resolve against the **flat** `window.logseq.api` object,
  which holds the `^:export` defs in `src/main/logseq/api.cljs` (`listener.cljs:113-119`). The namespace is
  not used for the lookup, so `logseq.App.getBlock` finds the same function as `logseq.Editor.getBlock`.
- Any other namespace resolves against `window.logseq.sdk.<namespace>` (`listener.cljs:115`, `:119`). That is
  where the `^:export` fns of `src/main/logseq/sdk/ui.cljs` and `src/main/logseq/sdk/assets.cljs` live. If no
  such namespace object exists, the error is `MethodNotExist: <namespace>@<method>` (`:120-121`).

This lookup is narrower than the one plugins use. It has no fallback to Electron's preload `window.apis`
and no camelCase retry (compare [Plugin API → How a call reaches the host](plugin-api.md#how-a-call-reaches-the-host)).

### What is not reachable

The HTTP API can only call functions the host exports. It cannot call:

- **SDK-local methods.** These are implemented inside `@logseq/libs` itself, for example `logseq.App.getInfo`
  (there is no `get_info` export). Use `logseq.App.getAppInfo` instead (`get_app_info`,
  `src/main/logseq/api.cljs:69`).
- **Methods that take a JavaScript callback**, such as `App.registerCommand` or `DB.onChanged`. A callback
  cannot be sent as JSON.
- **Methods the plugin API declares but never implemented.** Examples: the six template methods, `getUserInfo`
  and `getPagesFromNamespace`. The [Plugin API Reference](plugin-api.md) marks each one **Not implemented**.

What you get back for an unreachable method depends on the namespace:

- `logseq.Git.*` has no `window.logseq.sdk.git` object, so it returns
  `{"error": "MethodNotExist: git@…"}` with status 500 (`listener.cljs:120-121`).
- A missing method in the `app`, `editor`, `db` or `cli` namespaces makes `js-invoke` throw. That also
  returns an `error` object with status 500 (`listener.cljs:122`, `:127-129`), but the message is the
  JavaScript engine's own `TypeError` text.

---

## Available API Methods

Each method below was checked against its host export at `be800f1711`. Argument names follow the host
function. The [Plugin API Reference](plugin-api.md) documents behaviour and caveats. It is the reference for
every other exported method.

### logseq.App

Exports: `src/main/logseq/api.cljs:69-87`. Implementations: `src/main/logseq/api/app.cljs`.

| Method                   | Description                     | Args                                   |
| ------------------------ | ------------------------------- | -------------------------------------- |
| `getCurrentGraph`        | Get current graph info          | -                                      |
| `getUserConfigs`         | Get user configuration          | -                                      |
| `getAppInfo`             | Get app info                    | -                                      |
| `getCurrentRoute`        | Get current route               | -                                      |
| `getCurrentGraphConfigs` | Get graph config                | `...keys` (key path, `app.cljs:68-69`) |
| `pushState`              | Navigate to route               | `route`, `params?`, `query?` (`:153-154`) |
| `replaceState`           | Replace current route           | `route`, `params?`, `query?` (`:164-165`) |
| `setLeftSidebarVisible`  | Show/hide left sidebar          | `flag` (bool or `"toggle"`, `:131-133`) |
| `setRightSidebarVisible` | Show/hide right sidebar         | `flag` (bool or `"toggle"`, `:139-141`) |
| `setThemeMode`           | Set light/dark mode             | `mode` (`:104-106`)                    |
| `openExternalLink`       | Open URL externally             | `url` (only `http(s)://`, `:116-118`)  |

### logseq.Editor

Exports: `src/main/logseq/api.cljs:97-141`. Implementations: `src/main/logseq/api/editor.cljs`.

| Method                | Description              | Args                                                  |
| --------------------- | ------------------------ | ----------------------------------------------------- |
| `getCurrentBlock`     | Get block being edited   | `opts?` (`editor.cljs:319-320`)                       |
| `getCurrentPage`      | Get current page         | - (`:105-106`)                                        |
| `getBlock`            | Get block by ID          | `id`, `opts?` (`:314-315`)                            |
| `getPage`             | Get page by name/UUID    | `idOrPageName` (`:112-113`)                           |
| `getAllPages`         | Get all pages            | - (`:124-125`)                                        |
| `createPage`          | Create new page          | `name`, `properties?`, `opts?` (`:138-139`)           |
| `deletePage`          | Delete page              | `name` (`:171-172`)                                   |
| `renamePage`          | Rename page              | `pageUuid`, `newName` (`:187-188`; see note)          |
| `insertBlock`         | Insert block             | `id`, `content`, `opts?` (`:233-234`)                 |
| `updateBlock`         | Update block content     | `id`, `content`, `opts?` (`:289-290`)                 |
| `removeBlock`         | Delete block             | `id` (`:281-282`)                                     |
| `moveBlock`           | Move block               | `srcBlockUuid`, `targetBlockUuid`, `opts?` (`:298-299`) |
| `insertBatchBlock`    | Insert multiple blocks   | `blockUuid`, `batchBlocks`, `opts?` (`:270-271`)      |
| `getBlockProperty`    | Get block property       | `id`, `key` (`:496-497`)                              |
| `upsertBlockProperty` | Set block property       | `id`, `key`, `value`, `opts?` (`:454-455`)            |
| `removeBlockProperty` | Remove block property    | `id`, `key` (`:467-468`)                              |
| `getBlockProperties`  | Get all block properties | `id` (`:518-519`)                                     |
| `openInRightSidebar`  | Open block in sidebar    | `blockIdOrUuidOrKey` (`:190-191`)                     |
| `editBlock`           | Enter edit mode          | `blockUuid`, `opts?` (`:214-215`)                     |
| `exitEditingMode`     | Exit edit mode           | `select?` (`:64-65`)                                  |

> **`renamePage` needs the page UUID.** Its host is `page-handler/rename!`, whose first argument must be the
> page UUID string. A page name throws "Invalid page uuid" (see
> [Plugin API → renamePage](plugin-api.md#renamepage)).
>
> **Property names.** The property methods pass the key through `get-db-ident-from-property-name`
> (`src/main/logseq/api/block.cljs:44-57`, called at `editor.cljs:473`):
>
> - A qualified key such as `logseq.property/status` is used as it is (`block.cljs:53-54`).
> - An unqualified key gets a plugin-specific namespace (`:55-57`). It does not resolve to your graph's user
>   property of that name.
>
> Pass qualified property idents over HTTP.
>
> **Unverified:** which namespace an unqualified key gets over HTTP, where no plugin is calling. The body of
> `resolve-property-prefix-for-db` was not read.

### logseq.DB

Exports: `src/main/logseq/api.cljs:90-92`. Implementations: `src/main/logseq/api/db.cljs`.

| Method            | Description                                      | Args                              |
| ----------------- | ------------------------------------------------ | --------------------------------- |
| `q`               | Run a **query DSL** string (`query-dsl/query`)   | `queryString` (`db.cljs:15-18`)   |
| `datascriptQuery` | Run a Datalog query string                       | `query`, `...inputs` (`:21-22`)   |
| `customQuery`     | Read an EDN query string and run it as a custom query | `queryString` (`:47-50`)     |

### logseq.UI

Resolved against `window.logseq.sdk.ui` (`src/main/logseq/sdk/ui.cljs`).

| Method             | Description            | Args                                       |
| ------------------ | ---------------------- | ------------------------------------------ |
| `showMsg`          | Show notification      | `content`, `status?`, `opts?` (`ui.cljs:30`) |
| `closeMsg`         | Close notification     | `key` (`:34`)                              |
| `queryElementRect` | Get element dimensions | `selector` (`:39`)                         |

### logseq.Assets

Resolved against `window.logseq.sdk.assets` (`src/main/logseq/sdk/assets.cljs`).

| Method                    | Description                         | Args                   |
| ------------------------- | ----------------------------------- | ---------------------- |
| `listFilesOfCurrentGraph` | List files in the graph's `assets` directory | `exts?` (`assets.cljs:12`) |
| `builtInOpen`             | Open an asset (PDF only)            | `path` (`:17`)         |

### logseq.CLI

These exports back the MCP tools (`src/main/logseq/api.cljs:235-239`): `listPages`, `listTags`,
`listProperties`, `getPageData` and `upsertNodes`. For their arguments and results, see
[MCP Server](mcp-server.md#mcp-tools-reference).

---

## Code Examples

The examples below were derived from source. They were not run against a live server.

### Python

```python
import requests

BASE_URL = "http://127.0.0.1:12315/api"
TOKEN = "your-token-here"

def logseq_api(method, args=None):
    response = requests.post(
        BASE_URL,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        },
        json={"method": method, "args": args or []}
    )
    body = response.json()
    if response.status_code != 200:
        raise RuntimeError(body)
    return body  # the method's return value, not wrapped

# Get all pages
pages = logseq_api("logseq.Editor.getAllPages")
print(pages)

# Append a block to a page (the target may be a page name or UUID)
result = logseq_api("logseq.Editor.insertBlock", [
    "My Page",
    "New block content"
])
```

### JavaScript

```javascript
const BASE_URL = 'http://127.0.0.1:12315/api'
const TOKEN = 'your-token-here'

async function logseqApi(method, args = []) {
  const response = await fetch(BASE_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ method, args }),
  })
  return response.json()
}

// Get current page
const page = await logseqApi('logseq.Editor.getCurrentPage')
console.log(page)

// Find TODO and DOING tasks. Status is a ref to a closed value, not a string.
const tasks = await logseqApi('logseq.DB.datascriptQuery', [
  `[:find (pull ?b [:block/uuid :block/title])
    :where [?b :logseq.property/status ?s]
           [?s :db/ident ?ident]
           [(contains? #{:logseq.property/status.todo :logseq.property/status.doing} ?ident)]]`,
])
```

### curl

```bash
# Get all pages
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"method": "logseq.Editor.getAllPages"}'

# Append a block to a page
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "logseq.Editor.insertBlock",
    "args": ["My Page", "Hello from API"]
  }'

# Run a Datalog query
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "logseq.DB.datascriptQuery",
    "args": ["[:find (pull ?p [:block/uuid :block/title]) :where [?p :block/name]]"]
  }'
```

---

## Error Handling

| HTTP Status | Meaning                                                        | Source                         |
| ----------- | -------------------------------------------------------------- | ------------------------------ |
| 200         | The method returned                                            | `server.cljs:119-125`          |
| 400         | The body has no `method`                                       | `server.cljs:127-129`          |
| 401         | No `Authorization` header, or the token is not in the saved list | `server.cljs:83-103`         |
| 500         | The method threw or rejected, or the namespace does not exist  | `listener.cljs:120-129`, `server.cljs:121-124` |

A request with no JSON body at all makes the handler throw (`server.cljs:130`).

> **Unverified:** the status code and body Fastify sends for that throw, and the exact JSON form of the
> 401 error. Fastify serializes both, not Logseq code.

### Common Errors

```json
{"error": "MethodNotExist: git@exec_command"}
{"error": "Invalid page uuid"}
```

The first comes from an unknown namespace (`listener.cljs:120-121`). The second is a method's own error,
here from `renamePage` called with a page name (`src/main/frontend/handler/page.cljs:124-129`).

---

## Best Practices

1. **Save at least one token before starting the server.** With no saved token list, any `Authorization`
   header is accepted.
2. **Use one token per integration**, so you can revoke one without breaking the others.
3. **Handle errors**: check the status code, and a non-200 body for an `error` key.
4. **Keep tokens secret.** Never commit them to version control.
5. **Batch writes** where possible (`insertBatchBlock`).

---

## Troubleshooting

### Server Not Starting

- Check whether port 12315 is already in use. A failed start sets the status to `:error` and shows an error
  notification (`server.cljs:185-187`; `src/main/frontend/components/server.cljs:152-155`).
- Try a different port in **Server configurations**.
- Restart Logseq.

### No API Icon in the Header

- Turn on **Settings → Features → HTTP API server** and restart the app (`settings.cljs:617-630`,
  `header.cljs:601-602`).

### 401 Unauthorized

- Send an `Authorization` header. A request without one is always rejected.
- Check that the token matches a saved token's value exactly.
- Make sure you clicked **Save** in the Authorization tokens panel.

### Method Errors

- The namespace segment is case-insensitive, because it is lower-cased (`server.cljs:78`). The method name
  goes through `csk/->snake_case` (`:80`). By reading the code, both `getAllPages` and `get_all_pages`
  resolve to `get_all_pages`, while `getallpages` does not. This was not run.
- Check that the method has a host export (see [What is not reachable](#what-is-not-reachable)).

---

## See Also

- **MCP Server**: [mcp-server.md](mcp-server.md) for AI assistant integration
- **Plugin API**: [plugin-api.md](plugin-api.md) for method behaviour and which methods are not implemented
- **db-worker HTTP API**: [db-worker-http-api.md](db-worker-http-api.md), the internal renderer-to-worker interface
- **Source**: [`src/electron/electron/server.cljs`](../../../../../src/electron/electron/server.cljs),
  [`src/main/electron/listener.cljs`](../../../../../src/main/electron/listener.cljs)

---

_Last updated: 2026-09-11 | [Report an issue](https://github.com/logseq/logseq/issues)_
