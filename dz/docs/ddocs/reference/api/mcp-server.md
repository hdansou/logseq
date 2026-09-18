---
title: 'MCP Server Reference'
description: "Reference for the Logseq desktop app's Model Context Protocol (MCP) server for AI assistant integration"
status: stable
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop]
graphType: [db]
sinceVersion: '0.10.5'
relatedDocs:
  tutorials:
    - '[MCP Integration Tutorial](../../tutorials/integrations/mcp-integration.md)'
  reference:
    - '[HTTP API Reference](http-api.md)'
---

# MCP Server Reference

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-11

> Reference for the desktop app's Model Context Protocol (MCP) server, which lets AI assistants read and write your graph.

## Overview

The **MCP Server** implements the [Model Context Protocol](https://modelcontextprotocol.io/) with the
official TypeScript SDK (`@modelcontextprotocol/sdk`, `resources/package.json:28`). It advertises itself as
`Logseq MCP Server` version `0.1.0` (`src/electron/electron/mcp_server.cljs:220-222`). An MCP client can use
it to:

- **Read** pages, blocks, tags and properties from your graph
- **Write** new pages, blocks, tags and properties, and edit blocks (via `upsertNodes`)
- **Search** your graph for matching blocks

**Endpoint**: `http://127.0.0.1:12315/mcp` (Streamable HTTP). It is served by the desktop app's
[HTTP API server](http-api.md) (`src/electron/electron/server.cljs:144-154`).

Tool calls go to the renderer of the app's main window and act on that window's current graph. They use
`state/get-current-repo` (`src/main/logseq/api/db_based/cli.cljs:15-37`), and the server sends every call to
the window it was set up with (`server.cljs:112`, `:198-200`).

> **Removed:** The standalone `logseq mcp-server` CLI command and its stdio transport were removed with the
> old `deps/cli` package in commit `52398ac7aa` ("chore: remove old cli (#12739)"). At `be800f1711` the MCP
> server exists **only** as the desktop app's `/mcp` routes. The current OCaml CLI under `cli/` has no MCP
> command (`cli/lib/command_id.ml:1-63`).

---

## Quick Reference

| Tool             | Description                                   | Key Input                  | Calls                      |
| ---------------- | --------------------------------------------- | -------------------------- | -------------------------- |
| `listPages`      | List all pages                                | `expand?`                  | `logseq.cli.listPages`     |
| `getPage`        | Get page content and blocks                   | `pageName`                 | `logseq.cli.getPageData`   |
| `searchBlocks`   | Search blocks by text                         | `searchTerm`               | `logseq.app.search`        |
| `upsertNodes`    | Create pages/blocks/tags/properties, edit blocks | `operations`, `dry-run?` | `logseq.cli.upsertNodes`   |
| `listTags`       | List all tags                                 | `expand?`                  | `logseq.cli.listTags`      |
| `listProperties` | List all properties                           | `expand?`                  | `logseq.cli.listProperties` |

The tools are defined in `api-tools` (`mcp_server.cljs:122-213`) and registered in `create-mcp-api-server`
(`:224-231`). Each one calls a host API method, which is resolved the same way as an
[HTTP API](http-api.md#how-a-method-name-is-resolved) request (`server.cljs:144-148`). The `logseq.cli.*`
methods are exported at `src/main/logseq/api.cljs:235-239`. `logseq.app.search` is exported at `:187-192`.

---

## Getting Started

### Enable in Desktop App

1. **Open Settings → AI** (`src/main/frontend/components/settings.cljs:1454-1458`, `:1495`; label
   `src/resources/dicts/en.edn:1688`).
2. **Turn on "MCP Server"** (`settings.cljs:1424-1452`; label `en.edn:1709`).
3. **Start the HTTP API server** from the API icon in the header, if it is not running. The toggle turns on
   the "HTTP API server" feature flag (`settings.cljs:1438-1440`). It restarts the server only if the server
   is already running (`:1429-1433`); it never starts a stopped server.
4. **Add a token**: API icon → **Authorization tokens** → **+ Add new token** → **Save** (see
   [HTTP API → Authentication](http-api.md#authentication)).
5. **Verify**: open the API icon menu. While the server is running with MCP enabled, the menu shows the
   `/mcp` URL with a copy button (`src/main/frontend/components/server.cljs:196-202`).

Turning MCP Server on saves `:server/mcp-enabled?` in the Electron config (`settings.cljs:1441`,
`server.cljs:53-62`). The `/mcp` routes are registered only when the server starts with that flag set
(`server.cljs:178-179`). If you change the flag while the server is stopped, the change applies the next
time you start it.

Opening `http://127.0.0.1:12315/mcp` in a browser does **not** verify anything. A plain browser request has
no `Authorization` header, so it gets `401` (`server.cljs:83-103`). With a header but no session ID, a `GET`
gets `400` (`mcp_server.cljs:54-60`).

### Enable via CLI

> **Removed:** There is no CLI entry point for the MCP server at `be800f1711`.
> See the note above.

---

## Configuration

### Desktop Settings

The MCP Server uses the HTTP API server's host, port and tokens.

| Setting    | Config key             | Default     | Description                                    |
| ---------- | ---------------------- | ----------- | ---------------------------------------------- |
| MCP Server | `:server/mcp-enabled?` | unset (off) | Settings → AI (`server.cljs:40`)               |
| Host       | `:server/host`         | `127.0.0.1` | HTTP API server host (`server.cljs:20`)        |
| Port       | `:server/port`         | `12315`     | HTTP API server port (`server.cljs:21`)        |

### Authentication

`/mcp` goes through the same `preHandler` token check as `/api` (`server.cljs:92-103`, `:167`):

- A request with no `Authorization` header gets `401`.
- If no token list has ever been saved, any `Authorization` header is accepted.
- Once tokens are saved, send `Authorization: Bearer <token>`.

Details are in [HTTP API → Authentication](http-api.md#authentication).

### Transport

| Request       | Behaviour                                                                                   | Source                   |
| ------------- | ------------------------------------------------------------------------------------------- | ------------------------ |
| `POST /mcp`   | No `mcp-session-id` header and an `initialize` body: create a session. A known session ID: dispatch to that session. Anything else: `400` with JSON-RPC error `-32000` "Bad Request: No valid session ID provided" | `mcp_server.cljs:19-52` |
| `GET /mcp`    | SSE stream for an existing session. Needs the `mcp-session-id` header, otherwise `400`       | `mcp_server.cljs:54-60`  |
| `DELETE /mcp` | Close a session and answer `{"ok": true}`. Needs the `mcp-session-id` header, otherwise `400` | `mcp_server.cljs:62-70` |

Session IDs are random UUIDs (`mcp_server.cljs:30`). Sessions live in memory and are removed when their
transport closes (`:12-13`, `:34-37`).

DNS-rebinding protection is on. The only allowed host is exactly `<host>:<port>` from the server settings
(`mcp_server.cljs:31-32`). With the defaults, connect to `http://127.0.0.1:12315/mcp` exactly; a
`localhost` URL sends a different `Host` header.

---

## MCP Tools Reference

### Result format

A successful tool call returns one text content item. Its text is `JSON.stringify` of the method's result
(`mcp-success-response`, `mcp_server.cljs:77-80`).

If the method returns an object with an `error` key, the text is `API Error: <message>`
(`mcp_server.cljs:93-94`). An unexpected exception gives `Unexpected API error: <message>` (`:84-87`,
`:96`). Neither sets the MCP `isError` flag (`:72-75`, `:84-87`). A client therefore receives errors as
ordinary text results.

**Key names.** The `logseq.cli.*` results are converted with `clj->js` in the renderer (`cli.cljs:18`, `:25`,
`:32`, `:39`). In ClojureScript 1.12.134 (pinned at `deps.edn:4`), `clj->js` converts keyword keys with
`name` by default, which drops the namespace. So `:block/title` should come out as `"title"` and
`:block/uuid` as `"uuid"`. The examples below use those names. This was derived from source; the tools were
not run.

---

### listPages

List all pages in the graph.

**Input** (`mcp_server.cljs:124-129`):
| Parameter | Type    | Required | Description                             |
|-----------|---------|----------|-----------------------------------------|
| `expand`  | boolean | No       | Provide additional detail on each page  |

**Result** (`src/main/logseq/api/db_based/tools.cljs:87-101`): every entity with a `:block/name`, except
hidden ones. There is no paging or limit argument.

Without `expand`, each page has only its title and UUID:

```json
[
  { "title": "Page Name", "uuid": "page-uuid" }
]
```

With `expand: true`, `created-at` and `updated-at` are added. They come from `:block/created-at` and
`:block/updated-at` (`tools.cljs:95-99`):

```json
[
  {
    "uuid": "page-uuid",
    "title": "Page Name",
    "created-at": 1640000000000,
    "updated-at": 1640000000000
  }
]
```

---

### getPage

Get a page and its blocks. Properties and tags are pages too, so `getPage` also works on them
(`mcp_server.cljs:130-134`).

**Input**:
| Parameter  | Type   | Required | Description          |
|------------|--------|----------|----------------------|
| `pageName` | string | Yes      | The page's name or UUID |

**Result** (`tools.cljs:74-85`): an object with two keys.

- `entity`: the page, with hidden properties removed, `:block/tags` and `:block/refs` dropped, and the UUID
  as a string.
- `blocks`: the page's blocks in tree order (`otree/blocks->vec-tree`). Each block has hidden properties
  removed, `:block/children` and `:block/page` dropped, and UUID references in its title replaced
  (`:63-70`).

```json
{
  "entity": { "uuid": "page-uuid", "title": "Page Name" },
  "blocks": [
    { "uuid": "block-uuid", "title": "Block content" }
  ]
}
```

The objects contain more attributes than shown. If no page matches, the result is
`API Error: Page "<name>" not found` (`cli.cljs:38-40`).

---

### searchBlocks

Search for blocks containing text. It calls `logseq.app.search` with `{:enable-snippet? false}`
(`mcp_server.cljs:114-116`). That runs the app's search handler and converts the result keys for JSON
(`src/main/logseq/api.cljs:187-192`).

**Input**:
| Parameter    | Type   | Required | Description            |
|--------------|--------|----------|------------------------|
| `searchTerm` | string | Yes      | Text to search for     |

> **Unverified:** the shape of the search result. It was not traced beyond `search-handler/search`.

---

### upsertNodes

Create pages, blocks, tags and properties, or edit blocks, in one batch.

**Input** (`mcp_server.cljs:188-196`):
| Parameter    | Type    | Required | Description                                                  |
|--------------|---------|----------|--------------------------------------------------------------|
| `operations` | array   | Yes      | Array of operation objects (see below)                       |
| `dry-run`    | boolean | No       | Build and validate the change, but do not commit it (`cli.cljs:45-51`) |

Each operation object has:
| Field        | Type   | Description                                                        |
|--------------|--------|--------------------------------------------------------------------|
| `operation`  | string | `"add"` or `"edit"`                                                |
| `entityType` | string | `"block"`, `"page"`, `"tag"` or `"property"`                       |
| `id`         | string or null | For `edit`, the entity's UUID. For `add`, an optional temporary ID that later operations can reference |
| `data`       | object | Fields to set (see below)                                          |

The MCP input schema allows a number for `id` (`mcp_server.cljs:194`). The server-side schema accepts only
a string or null (`tools.cljs:272`), so a numeric `id` is rejected.

The `data` object can contain (`tools.cljs:273-281`):
| Field                    | Description                                           |
|--------------------------|-------------------------------------------------------|
| `title`                  | Page/tag/property name or block content               |
| `page-id`                | Page UUID, or the temporary `id` of a page added earlier in the batch. Required when adding a block |
| `tags`                   | Array of tag UUID strings                             |
| `property-type`          | Property type                                         |
| `property-cardinality`   | `"one"` or `"many"`                                   |
| `property-classes`       | Array of allowed tag UUIDs or names                   |
| `class-extends`          | Array of parent tag UUIDs or names                    |
| `class-properties`       | Array of property UUIDs or names                      |

Rules checked by the schema (`tools.cljs:282-298`):

- **Adding a block** requires `title` and `page-id`. Only `tags` may be added besides those.
- **Editing a block** requires a UUID `id`, and `data` may contain only `title`. Tags cannot be edited yet
  (`:294`).
- **Editing a page, tag or property** is not supported. The call fails with "Editing a page, tag or property
  isn't supported yet" (`tools.cljs:333-334`).

A payload that fails validation returns `Tool arguments are invalid:` followed by the details (`:344`).

**Result**: a summary string, such as `Added: {:block 1}.`. It is prefixed with `Dry run: ` for a dry run
(`src/main/logseq/api/db_based/util.cljs:12-24`), and it arrives JSON-encoded in the text content.

**Example**: add a block to an existing page.

```json
{
  "operations": [
    {
      "operation": "add",
      "entityType": "block",
      "id": null,
      "data": {
        "page-id": "119268a6-704f-4e9e-8c34-36dfc6133729",
        "title": "New block text"
      }
    }
  ]
}
```

**Example**: create a new page with a block.

```json
{
  "operations": [
    {
      "operation": "add",
      "entityType": "page",
      "id": "temp-Inbox",
      "data": { "title": "Inbox" }
    },
    {
      "operation": "add",
      "entityType": "block",
      "data": {
        "page-id": "temp-Inbox",
        "title": "My first block"
      }
    }
  ]
}
```

These payloads match the schema. They were not run.

---

### listTags

List all tags in the graph.

**Input** (`mcp_server.cljs:202-207`):
| Parameter | Type    | Required | Description                                              |
|-----------|---------|----------|----------------------------------------------------------|
| `expand`  | boolean | No       | Include additional detail (parents, tag properties, etc) |

**Result** (`tools.cljs:40-61`): every entity tagged `:logseq.class/Tag`.

- Without `expand`: title and UUID only.
- With `expand`: the whole entity except `:block/tags`, `:block/order`, `:block/refs` and `:block/name`.
  Parent tags (`:logseq.property.class/extends`) and tag properties (`:logseq.property.class/properties`) are
  reduced to their `:db/ident`s.

```json
[
  { "title": "Tag Name", "uuid": "tag-uuid" }
]
```

---

### listProperties

List all properties defined in the graph.

**Input** (`mcp_server.cljs:208-213`):
| Parameter | Type    | Required | Description                                                |
|-----------|---------|----------|------------------------------------------------------------|
| `expand`  | boolean | No       | Include additional detail (type, cardinality, etc)         |

**Result** (`tools.cljs:19-38`): every entity tagged `:logseq.class/Property`.

- Without `expand`: title and UUID only.
- With `expand`: the whole entity except `:block/tags`, `:block/order`, `:block/refs`, `:block/name`,
  `:db/index` and `:logseq.property/default-value`. Allowed tags (`:logseq.property/classes`) are reduced to
  their idents.

```json
[
  { "title": "property-name", "uuid": "property-uuid" }
]
```

---

## Client Configuration

Logseq exposes only a Streamable HTTP endpoint; there is no stdio transport. Every client needs:

```
URL:     http://127.0.0.1:12315/mcp
Header:  Authorization: Bearer YOUR_TOKEN
```

> **Unverified:** the configuration file, key names and schema for a particular client (Claude Desktop,
> Cline, and so on). Those belong to the client, not to Logseq, and cannot be checked in the Logseq source.
> Clients that can only launch stdio subprocesses need a generic remote-MCP bridge in front of the URL.
> Logseq does not ship one.

### Custom Client

```javascript
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js'

const transport = new StreamableHTTPClientTransport(
  new URL('http://127.0.0.1:12315/mcp'),
  { requestInit: { headers: { Authorization: 'Bearer YOUR_TOKEN' } } }
)

const client = new Client({
  name: 'my-client',
  version: '1.0.0',
})

await client.connect(transport)

// List pages
const pages = await client.callTool({
  name: 'listPages',
  arguments: {},
})
```

`requestInit` is how the MCP TypeScript SDK client sends extra headers (`StreamableHTTPClientTransportOptions`).
It was checked in SDK 1.30.0's type definitions, not in Logseq. The example was not run.

---

## Troubleshooting

### MCP Not Available

1. Make sure MCP Server is on in Settings → AI.
2. Make sure the HTTP API server is running. The API icon menu should list a URL ending in `/mcp`.
3. If you turned MCP on while the server was stopped, start the server so the routes get registered.

### 401 Unauthorized

- The client must send an `Authorization` header. A request without one is always rejected.
- The token must match a saved token.

### 400 Bad Request

- The first request must be an MCP `initialize` without an `mcp-session-id` header. Later requests must send
  the session ID the server returned.
- Connect with the configured host and port exactly (DNS-rebinding allow-list).

### Connection Refused

1. Check the host and port settings.
2. Make sure Logseq desktop is running and the server is started.

### Tool Errors

- Check that the page name is correct and the page exists in the current graph.
- Use the `dry-run` option with `upsertNodes` to validate operations before committing.
- Watch the Electron main-process log: every `/mcp` request is logged (`src/electron/electron/mcp_server.cljs:21`, `:57`, `:65`).

---

## See Also

- **HTTP API**: [http-api.md](http-api.md) for general API access and authentication
- **MCP Specification**: [modelcontextprotocol.io](https://modelcontextprotocol.io/)
- **Tutorial**: [MCP Integration Tutorial](../../tutorials/integrations/mcp-integration.md)

---

_Last updated: 2026-09-11 | [Report an issue](https://github.com/logseq/logseq/issues)_
