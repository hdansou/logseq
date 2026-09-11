---
title: 'Model Context Protocol (MCP) Integration'
status: 🟡 Active Development
version: 'Desktop MCP server added 2025-09-17 (a16c89b986)'
quadrant: 'Tutorial'
platform: 'Desktop (Electron)'
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
verifiedScope: 'Full audit 2026-09-09. The 2026-09-11 date covers only the no-tokens authentication note (Host / authentication section).'
reviewStatus: audited
related-docs:
  - '[CLI Development Guide](../../reference/tools/cli-reference.md)'
  - '[Database Properties](../development/db-properties.md)'
  - '[Plugin Development](../plugins/first-plugin.md)'
tags: ['mcp', 'ai', 'claude', 'integration', 'api']
---

# Model Context Protocol (MCP) Integration

This tutorial explains how to connect an AI assistant to Logseq using the Model Context Protocol (MCP). Logseq Desktop ships an MCP server as an optional set of routes on its built-in HTTP API server, so any MCP client that speaks the **Streamable HTTP** transport can read, search, and modify the graph you currently have open.

**What You'll Learn:**

- What Logseq's MCP server actually is, and what it is not
- Turning it on and finding its URL
- The six tools it exposes, with their real input schemas
- Exercising the endpoint by hand with `curl`
- Troubleshooting sessions, auth, and host checks

**Prerequisites:**

- **Logseq Desktop (Electron).** The MCP server lives in the Electron main process (`src/electron/electron/mcp_server.cljs`) and is registered only `(when (util/electron?) ...)` in the settings UI (`src/main/frontend/components/settings.cljs:1442-1446`). There is no web or mobile equivalent.
- A **DB graph** open in the app. Every tool is routed to the db-worker thread API (`src/main/frontend/worker/handler/cli.cljs:33-56`).
- An MCP client that supports the Streamable HTTP transport.

**Time Required:** ~15 minutes

---

## Table of Contents

1. [Understanding MCP](#1-understanding-mcp)
2. [MCP Architecture in Logseq](#2-mcp-architecture-in-logseq)
3. [Enabling the MCP Server](#3-enabling-the-mcp-server)
4. [Connecting an MCP Client](#4-connecting-an-mcp-client)
5. [Available MCP Tools](#5-available-mcp-tools)
6. [Exercising the Endpoint by Hand](#6-exercising-the-endpoint-by-hand)
7. [Advanced Usage](#7-advanced-usage)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Understanding MCP

### What is MCP?

The **Model Context Protocol** (MCP) is an open standard for connecting AI assistants to external data sources and tools. It provides a structured way for LLMs to:

- **Read data** from applications (pages, blocks, properties)
- **Write data** to applications (create/update content)
- **Search** across datasets
- **Execute operations** with proper validation

Logseq's server implements the spec revision **2025-03-26**, which the source explicitly links to (`src/electron/electron/mcp_server.cljs:17`).

### What Logseq's MCP server is *not*

Three clarifications, because they are easy to assume otherwise:

- **It is not a CLI subcommand.** The old ClojureScript CLI under `deps/cli` was deleted in `52398ac7aa`. The current Logseq CLI is written in OCaml under `cli/lib/`, and its command registry (`cli/lib/command_registry.ml`) contains no MCP command.
- **It is not an npm package you install.** There is nothing to `npm install`; the server is compiled into the desktop app.
- **It is not a stdio server.** The only transport is `StreamableHTTPServerTransport` from `@modelcontextprotocol/sdk` (`src/electron/electron/mcp_server.cljs:4`).

> **Not the same thing:** Logseq's cloud sync worker also exposes an MCP endpoint (`deps/db-sync/worker/entry.mjs:41-186`), built with `openApiMcpServer` from the Logseq server API's OpenAPI spec and served at `/mcp` on the worker origin. That is a hosted service for synced graphs, with a different tool set and different auth. This tutorial covers only the desktop server.

---

## 2. MCP Architecture in Logseq

### Architecture Overview

```
┌────────────────────────────────────────────────────────┐
│  MCP Client (Streamable HTTP)                          │
└─────────────┬──────────────────────────────────────────┘
              │ JSON-RPC over HTTP
              │ POST/GET/DELETE http://<host>:<port>/mcp
              │ Session pinned by the `mcp-session-id` header
              ▼
┌────────────────────────────────────────────────────────┐
│  Logseq Desktop HTTP API server (Fastify)              │
│  src/electron/electron/server.cljs                     │
│    preHandler: bearer-token check (:83-101)            │
│    /api   → generic API bridge                         │
│    /mcp   → MCP routes, only when :mcp-enabled? (:178) │
│  ┌──────────────────────────────────────────────────┐  │
│  │  McpServer "Logseq MCP Server" v0.1.0            │  │
│  │  6 registered tools (mcp_server.cljs:124-215)    │  │
│  │  listPages   getPage      upsertNodes            │  │
│  │  searchBlocks listTags    listProperties         │  │
│  └──────────┬───────────────────────────────────────┘  │
└─────────────┼──────────────────────────────────────────┘
              │ each tool calls a `logseq.*` API method
              │ (server.cljs:105-113 → renderer)
              ▼
┌────────────────────────────────────────────────────────┐
│  Renderer: window.logseq.api  (src/main/logseq/api.cljs)│
│  → db-worker thread API (worker/handler/cli.cljs)      │
│  → the graph currently open in the app                 │
└────────────────────────────────────────────────────────┘
```

### One mode, not two

There is a single operating mode. The MCP routes are mounted on the app's existing HTTP API server, on the same host and port — not on a separate port and not against the SQLite file directly:

```clojure
;; src/electron/electron/server.cljs:144-154
(defn- initialize-mcp-routes [^js server]
  ...
  (.post server "/mcp" ...)
  (.get server "/mcp" desktop-mcp-server/handle-get-request)
  (.delete server "/mcp" desktop-mcp-server/handle-delete-request))
```

Consequences worth internalising:

- **The app must be running.** Tools reach the graph through the renderer (`server.cljs:105-113` sends `invokeLogseqAPI` to the window; `src/main/electron/listener.cljs:104-128` dispatches it onto `window.logseq.api`).
- **The graph is whatever is open.** No tool takes a graph path or name; every call resolves against `(state/get-current-repo)` (`src/main/logseq/api/db_based/cli.cljs:27-32`).
- **Writes land live.** `upsertNodes` transacts through the outliner and calls `ui-handler/re-render-root!` (`src/main/logseq/api/db_based/cli.cljs:42-54`), so changes appear in the UI immediately.

### Sessions

`StreamableHTTPServerTransport` is created with a random session id generator, and transports are kept in an atom keyed by session id (`mcp_server.cljs:12`, `:19-42`):

- A POST with **no** `mcp-session-id` header and an `initialize` body creates a new transport and returns a session id.
- Every later POST/GET/DELETE must carry `mcp-session-id`, or the server replies `400` with `"Bad Request: No valid session ID provided"` (POST) or `"Invalid or missing session ID"` (GET/DELETE).

---

## 3. Enabling the MCP Server

### Turn on the toggle

MCP is off by default. It is a UI toggle, not a config-file edit:

1. Open **Settings → AI**.
2. Enable **MCP Server**.

The setting's own description reads: *"Enable MCP Server to allow commmunication with local AI applications. MCP Server depends on HTTP API server."* (`src/resources/dicts/en.edn:1708-1709` — the typo is in the source string).

Toggling it on also flips the HTTP API server on for you if it was off, then restarts the server if it was already running (`src/main/frontend/components/settings.cljs:1421-1440`). The flag is persisted as the config item `:server/mcp-enabled?` (`src/electron/electron/server.cljs:40`).

### Find the URL

Defaults come from `src/electron/electron/server.cljs:20-21`:

```clojure
(defn get-host [] (or (cfgs/get-item :server/host) "127.0.0.1"))
(defn get-port [] (or (cfgs/get-item :server/port) 12315))
```

So the default endpoint is:

```
http://127.0.0.1:12315/mcp
```

When MCP is enabled and the server is running, the app shows that exact URL in the API-server popup with a click-to-copy affordance (`src/main/frontend/components/server.cljs:196-201`). Use the copied string verbatim — see the host check below.

### Auth tokens

The Fastify `preHandler` hook applies to every route except `/`, and validates the `Authorization` header against the configured token list (`src/electron/electron/server.cljs:83-101, 167`):

```clojure
(defn- validate-auth-token [token]
  (let [token (string/replace token "Bearer " "")]
    (when-let [valid-tokens (cfgs/get-item :server/tokens)]
      (when (or (string/blank? token)
                (not (some #(or (= % token) (= (:value %) token)) valid-tokens)))
        (throw (js/Error. "Access Denied!"))))))
```

Two behaviours follow:

- If you have configured **no** tokens, `when-let` short-circuits and **the token value is not checked**. The header must still be present: `validate-auth-token` first calls `string/replace` on it, which throws on a missing header, and the pre-handler turns that into a 401 (`src/electron/electron/server.cljs:85`, `:96-103`). Any `Authorization` value therefore passes.
- If you have configured tokens, every MCP request needs `Authorization: Bearer <token>`. Manage tokens from the API-server popup's token panel.

### Host / DNS-rebinding check

The transport is created with DNS rebinding protection and an allow-list containing exactly the configured host and port (`mcp_server.cljs:31-32`):

```clojure
{:sessionIdGenerator (comp str random-uuid)
 :enableDnsRebindingProtection true
 :allowedHosts #js [(str host ":" port)]}
```

With the defaults, the only accepted `Host` header is `127.0.0.1:12315`. Pointing a client at `http://localhost:12315/mcp` sends `Host: localhost:12315`, which is not on the allow-list. Use the host string the app shows you.

---

## 4. Connecting an MCP Client

Any client that speaks Streamable HTTP can connect. Point it at the URL from the previous section and, if you configured tokens, give it an `Authorization: Bearer <token>` header.

> **Unverified:** The exact configuration file, key names, and schema for a given MCP client (Claude Desktop, Cline, etc.) are properties of that client, not of Logseq, and cannot be verified from the Logseq source tree. Consult your client's own documentation for how it declares a remote Streamable HTTP server. What Logseq guarantees is only the contract described here: `POST`/`GET`/`DELETE` on `http://<host>:<port>/mcp`, Streamable HTTP, optional bearer token.

Once connected, the client should list six tools. The server identifies itself as (`mcp_server.cljs:220-222`):

```clojure
(McpServer. #js {:name "Logseq MCP Server"
                 :version "0.1.0"})
```

---

## 5. Available MCP Tools

Exactly six tools are registered, from the `api-tools` map (`src/electron/electron/mcp_server.cljs:124-215`), each wired up by `create-mcp-api-server` via `.registerTool` (`:224-231`). Input schemas are declared with **zod v3** (`zod/v3` is imported at `:6` with a comment noting zod 4 is incompatible with the MCP SDK).

Each tool forwards to a `logseq.*` API method, which the server resolves to a renderer call:

| Tool             | Underlying API method        | Required args | Optional args |
| ---------------- | ---------------------------- | ------------- | ------------- |
| `listPages`      | `logseq.cli.listPages`       | —             | `expand`      |
| `getPage`        | `logseq.cli.getPageData`     | `pageName`    | —             |
| `searchBlocks`   | `logseq.app.search`          | `searchTerm`  | —             |
| `listTags`       | `logseq.cli.listTags`        | —             | `expand`      |
| `listProperties` | `logseq.cli.listProperties`  | —             | `expand`      |
| `upsertNodes`    | `logseq.cli.upsertNodes`     | `operations`  | `dry-run`     |

> **Unverified:** The exact JSON *key spelling* in tool results. Results are ClojureScript maps keyed by namespaced attributes such as `:block/title` and `:block/uuid`, converted with `clj->js` and then `JSON.stringify`d by `mcp-success-response` (`mcp_server.cljs:77-81`). The attribute names below are what the source produces; confirm the serialized key form by calling the tool once against your own graph.

---

### Tool 1: `listPages`

**Title:** "List Pages" · **Description:** "List all pages in a graph"

**Input Schema:**

```typescript
{
  expand?: boolean // "Provide additional detail on each page"
}
```

**Result** — every non-hidden page entity, from `logseq.api.db-based.tools/list-pages` (`src/main/logseq/api/db_based/tools.cljs:87`):

- Without `expand`: `:block/title` and `:block/uuid` (stringified) only.
- With `expand: true`: `:block/uuid`, `:block/title`, `:block/created-at`, `:block/updated-at`.

The source comments why the expanded form is still narrow: *"Until there are options to limit pages, return minimal info to avoid exceeding max payload size"*. There is no paging or limit argument.

---

### Tool 2: `getPage`

**Title:** "Get Page" · **Description:** "Get a page's content including its blocks. A property and a tag are pages."

**Input Schema:**

```typescript
{
  pageName: string // "The page's name or uuid"
}
```

**Result** — from `get-page-data` (`tools.cljs:74`), a map with two keys:

- `:entity` — the page entity with hidden properties removed, `:block/tags` and `:block/refs` dropped, `:block/uuid` stringified.
- `:blocks` — the page's block tree (`otree/blocks->vec-tree`), each block with hidden properties removed and `:block/children` / `:block/page` dropped.

If the page is not found, the API returns `{error: "Page \"...\" not found"}` (`src/main/logseq/api/db_based/cli.cljs:34-40`), which the MCP layer surfaces as `"API Error: ..."` text content (`mcp_server.cljs:89-97`).

**Note:** Properties and tags are themselves pages, so `getPage` works on them too.

---

### Tool 3: `searchBlocks`

**Title:** "Search Blocks" · **Description:** "Search graph for blocks containing search term"

**Input Schema:**

```typescript
{
  searchTerm: string
}
```

This is the only tool that does not go through the `logseq.cli.*` family. It calls `logseq.app.search` with snippets disabled (`mcp_server.cljs:114-116`):

```clojure
(call-api-fn "logseq.app.search" [(aget args "searchTerm") #js {:enable-snippet? false}])
```

which is `logseq.api/search` (`src/main/logseq/api.cljs:187-193`), the same search handler the app's own search uses.

---

### Tool 4: `listTags`

**Title:** "List Tags" · **Description:** "List all tags in a graph"

**Input Schema:**

```typescript
{
  expand?: boolean // "Provide additional detail on each tag e.g. their parents (extends) and tag properties"
}
```

**Result** — from `list-tags` (`tools.cljs:40`), every entity tagged `:logseq.class/Tag`:

- Without `expand`: `:block/title` and `:block/uuid`.
- With `expand: true`: the whole entity minus `:block/tags`, `:block/order`, `:block/refs`, `:block/name`, with `:logseq.property.class/extends` and `:logseq.property.class/properties` mapped down to their `:db/ident`s.

Note that parents come back as **`:db/ident` keywords** (for example `:logseq.class/Task`), not as UUIDs.

---

### Tool 5: `listProperties`

**Title:** "List Properties" · **Description:** "List all properties in a graph"

**Input Schema:**

```typescript
{
  expand?: boolean // "Provide additional detail on each property e.g. property type, cardinality"
}
```

**Result** — from `list-properties` (`tools.cljs:19`), every entity tagged `:logseq.class/Property`:

- Without `expand`: `:block/title` and `:block/uuid`.
- With `expand: true`: the whole entity minus `:block/tags`, `:block/order`, `:block/refs`, `:block/name`, `:db/index`, `:logseq.property/default-value`, with `:logseq.property/classes` mapped to `:db/ident`s.

---

### Tool 6: `upsertNodes`

**Title:** "Upsert Nodes"

The tool description opens with a hard constraint that is part of the registered schema, not editorial advice: *"This tool must be called at most once per user request. Never re-call it unless explicitly asked."*

**Input Schema** (`mcp_server.cljs:186-196`):

```typescript
{
  operations: Array<{
    operation: 'add' | 'edit'
    entityType: 'block' | 'page' | 'tag' | 'property'
    id?: string | number | null   // uuid string for `edit`; a temp string for `add` when later ops reference it
    data: {                        // passthrough object; validated server-side
      title?: string               // page/tag/property name, or block content
      'page-id'?: string           // required when adding a block
      tags?: string[]              // tag uuids
      'property-type'?: string
      'property-cardinality'?: 'one' | 'many'
      'property-classes'?: string[]  // uuid strings or tag names
      'class-extends'?: string[]     // uuid strings or tag names
      'class-properties'?: string[]  // uuid strings or property names
    }
  }>
  'dry-run'?: boolean // "Pretend to do batch update. Does everything except actually commit change to db e.g. validation."
}
```

**Server-side validation** is a malli schema in `logseq.api.db-based.tools` (`tools.cljs:265-301`), and it is stricter than the zod input schema:

- The operation map is `{:closed true}` — unknown top-level keys are rejected.
- `add` + `block` requires both `:title` and `:page-id`, and allows only `:tags` besides.
- `edit` + `block` requires `:id` to be a uuid string and allows **only** `:title` in `:data`. The source comments `;; :tags not supported yet`.
- `add` for `page` / `tag` / `property` requires `:data {:title <string>}`.
- Every other `edit` requires `:id` to be a uuid string.

**Not supported yet** — `build-upsert-nodes-edn` throws before doing anything else (`tools.cljs:333-334`):

```clojure
(when (seq (filter #(and (#{"page" "tag" "property"} (:entityType %)) (= "edit" (:operation %))) operations*))
  (throw (ex-info "Editing a page, tag or property isn't supported yet" {})))
```

So `edit` works on **blocks only**.

**Example call:**

```json
{
  "operations": [
    {
      "operation": "add",
      "entityType": "page",
      "id": "temp-meeting-notes",
      "data": { "title": "Meeting Notes" }
    },
    {
      "operation": "add",
      "entityType": "block",
      "data": {
        "page-id": "temp-meeting-notes",
        "title": "Discuss Q1 goals"
      }
    }
  ],
  "dry-run": true
}
```

**Result** — a one-line summary string, not the created nodes (`src/main/logseq/api/db_based/util.cljs:12-24`):

```
Dry run: Added: {:page 1, :block 1}.
```

**Best practices** (the first three come from the tool's own description text):

1. Check existence with `getPage` before creating a page, tag, or property.
2. Use a temporary string `id` for a new node that later operations reference.
3. Call the tool at most once per user request.
4. Run with `"dry-run": true` first — validation still runs, but nothing is committed.

---

## 6. Exercising the Endpoint by Hand

Everything below assumes the defaults (`127.0.0.1:12315`) and no configured tokens. Add `-H "Authorization: Bearer <token>"` to every request if you have tokens configured.

### Step 1: Initialize and capture the session id

The Streamable HTTP transport requires the client to accept both JSON and SSE, so send both media types in `Accept`. Use `-D -` (or `-i`) so you can read the `mcp-session-id` response header:

```bash
curl -sS -D - http://127.0.0.1:12315/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-03-26",
      "capabilities": {},
      "clientInfo": { "name": "curl", "version": "1.0.0" }
    }
  }'
```

The response headers carry `mcp-session-id`. The Electron main-process console also logs `Initialize sessionId <id>` (`mcp_server.cljs:40`).

### Step 2: List the tools

```bash
SID=<paste the mcp-session-id>

curl -sS http://127.0.0.1:12315/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
```

You should see the six tools from section 5.

### Step 3: Call a tool

```bash
curl -sS http://127.0.0.1:12315/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SID" \
  -d '{
    "jsonrpc":"2.0","id":3,"method":"tools/call",
    "params":{"name":"listPages","arguments":{"expand":true}}
  }'
```

### Step 4: Close the session

```bash
curl -sS -X DELETE http://127.0.0.1:12315/mcp \
  -H "mcp-session-id: $SID"
```

The server closes the transport and replies `{"ok":true}` (`mcp_server.cljs:62-70`).

### Dry-running a write

`dry-run` runs the full build-and-validate path and skips only the transaction (`src/main/logseq/api/db_based/cli.cljs:42-54`):

```bash
curl -sS http://127.0.0.1:12315/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SID" \
  -d '{
    "jsonrpc":"2.0","id":4,"method":"tools/call",
    "params":{"name":"upsertNodes","arguments":{
      "operations":[{"operation":"add","entityType":"page","id":"temp-test",
                     "data":{"title":"Test Page"}}],
      "dry-run":true}}
  }'
```

---

## 7. Advanced Usage

### Use Case 1: Summarise a set of pages

**Prompt:**

```
Summarize the pages tagged Project in my Logseq graph
```

**What the assistant can do with the available tools:**

1. `listTags(expand: true)` to find the `Project` tag and its uuid.
2. `getPage("Project")` — a tag *is* a page, so this returns the tag's own page.
3. `getPage(...)` per page of interest.

Note the limitation honestly: there is **no** "list objects of a tag" tool, so a client cannot enumerate a tag's members in one call. It has to work from `listPages` plus per-page fetches, or from `searchBlocks`.

### Use Case 2: Create a page with blocks

**Prompt:**

```
Create a page called "Q1 Goals" with three blocks
```

1. `getPage("Q1 Goals")` to check existence.
2. `upsertNodes` with `dry-run: true` — one `add`/`page` operation carrying a temp `id`, then three `add`/`block` operations whose `data.page-id` is that temp id.
3. Re-run with `dry-run` omitted.

To tag the blocks, pass `data.tags` as an array of **tag uuid strings** (the malli schema requires uuid strings, `tools.cljs:276`). The built-in Task class uuid used in the tool's own worked example is `00000002-1282-1814-5700-000000000000`.

### Use Case 3: Graph inventory

1. `listPages` and count.
2. `listTags(expand: true)` and `listProperties(expand: true)` for the schema shape.
3. `searchBlocks(...)` for text-level questions.

> **Careful with task queries.** In a DB graph there are no `TODO`/`DOING` markers to search for. Task state is the closed-value property `:logseq.property/status`, whose values are the idents `:logseq.property/status.backlog`, `.todo`, `.doing`, `.in-review`, `.done`, `.canceled` (`deps/db/src/logseq/db/frontend/property.cljs:300-318`). `searchBlocks("TODO")` will not find open tasks.

---

## 8. Troubleshooting

### The client sees no tools, or cannot connect

1. **Is the toggle on?** Settings → AI → MCP Server. The routes are only registered when `:mcp-enabled?` is true at server start (`server.cljs:178-179`).
2. **Is the HTTP server running?** MCP depends on it. Toggling MCP on enables it, but if you had already started/stopped the server you may need to restart it from the API-server popup.
3. **Did you restart the server after toggling?** `initialize-mcp-routes` runs inside `start!`, so the routes only appear on a (re)start (`server.cljs:156-183`).
4. **Check the main-process console.** Successful wiring logs `[server] MCP routes initialized` (`server.cljs:149`).

### `400 Bad Request: No valid session ID provided`

You sent a non-`initialize` POST without an `mcp-session-id` header, or with a session id the server no longer holds. Re-run `initialize` and use the fresh id. Transports are dropped from the atom when they close (`mcp_server.cljs:34-37`).

### `400 Invalid or missing session ID` on GET/DELETE

Same cause, on the GET (SSE stream) and DELETE (close) routes (`mcp_server.cljs:54-70`).

### `401` on every request

Tokens are configured and your `Authorization` header is missing or wrong (`server.cljs:83-101`). Copy a token from the API-server popup's token panel and send `Authorization: Bearer <token>`.

### Requests rejected on host grounds

DNS rebinding protection allows exactly `"<host>:<port>"` (`mcp_server.cljs:31-32`). If you configured the host as `127.0.0.1`, a client using `localhost` will be rejected. Either use the URL the app copies for you, or change `:server/host` to match what your client sends.

### `"This endpoint must be called on a DB graph"` or empty results

The tools resolve against the graph currently open in the desktop app. Open the DB graph you mean to query before calling.

### An `upsertNodes` call fails validation

Read the message: the malli explainer is humanized into the error text (`tools.cljs:343-345`), so it names the offending key. Common causes:

- Editing a page, tag, or property — not supported yet (`tools.cljs:333-334`).
- Passing `:tags` to an `edit` + `block` operation — the `:data` map is closed to `:title` only.
- Passing a tag by name where a uuid string is required.
- Adding a block without `data.page-id`.

---

## Next Steps

You now have an AI client talking to your live Logseq graph.

### Continue Learning:

1. **[CLI Development Guide](../../reference/tools/cli-reference.md)** - The OCaml CLI (separate from MCP)
2. **[Database Properties](../development/db-properties.md)** - DB schema behind `upsertNodes`
3. **[Plugin Development](../plugins/first-plugin.md)** - Build in-app integrations
4. **[MCP Specification](https://modelcontextprotocol.io)** - Official MCP docs

### Where to look in the source:

- `src/electron/electron/mcp_server.cljs` — transport, tool registry, tool descriptions
- `src/electron/electron/server.cljs` — route mounting, auth, host/port
- `src/main/logseq/api/db_based/cli.cljs` — the `logseq.cli.*` API methods
- `src/main/logseq/api/db_based/tools.cljs` — result shapes and `upsertNodes` validation
- `src/main/frontend/components/settings.cljs` — the settings toggle

---

## Additional Resources

- **MCP Specification:** https://modelcontextprotocol.io
- **Logseq source:** https://github.com/logseq/logseq
- **Cline (MCP Client):** https://github.com/cline/cline

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
