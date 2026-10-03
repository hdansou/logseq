---
title: 'Logseq Server API Reference (REST + MCP)'
description: 'Reference for the cloud REST API and ChatGPT MCP endpoint served by the db-sync worker at api.logseq.io: who can use it today, endpoints, auth, limits, E2EE restriction, how to test it, and where the code and the published spec disagree'
type: reference
status: active-development
lastVerified: '2026-10-03'
verifiedAgainst: '9c18a5432b'
reviewStatus: authored-from-source
logseqVersion: '2.0.x'
platforms: [cloud]
graphType: [db]
sinceVersion: 'PR #12896 (merged 2026-07-13)'
relatedDocs:
  reference:
    - '[MCP Server Reference (desktop)](mcp-server.md)'
    - '[HTTP API Reference (desktop)](http-api.md)'
    - '[db-worker HTTP API](db-worker-http-api.md)'
    - '[Sync Limits, Throttles, and Batch Sizes](../features/sync-limits.md)'
---

# Logseq Server API Reference (REST + MCP)

> **Last revised:** 2026-10-03

> Reference for the **cloud** API that the db-sync Cloudflare Worker serves. It lets outside
> programs, ChatGPT in particular, read and edit a synced DB graph with no Logseq client
> running. Introduced by [logseq/logseq#12896](https://github.com/logseq/logseq/pull/12896)
> ("Add db-sync semantic REST and MCP APIs", Tienson Qin, merged 2026-07-13). Upstream renamed
> it "Logseq Server API" in `998207ffd5`.

**Verification.** Every claim on this page was read from source at `9c18a5432b`, which matches
`upstream/master` @ `22263c3620` for `deps/db-sync/`. On 2026-10-03 the public endpoints were
also probed on `https://api.logseq.io`. `/openapi.json`, `/api-docs`, `/health` and the OAuth
metadata answer `200`, the live spec lists the same 36 operations as source, and
`/api/v1/graphs` answers `401` without a token. The MCP endpoint was also driven anonymously:
`initialize`, `tools/list` and the `search` tool succeed, and `execute` fails with the REST
layer's `401` (see [Testing it](#testing-it)). The three local test suites pass at `9c18a5432b`.
Authenticated calls were **not** exercised. Behaviour marked *source-only* has not been
reproduced live, and *inferred* marks conclusions drawn from configuration rather than observed.

## Who can use it today

Status on **2026-10-03**: the API is deployed, but no ordinary Logseq user can reach it yet.

| Route | Works? | Why |
|---|---|---|
| ChatGPT app directory | ❌ | Logseq's app is **not listed** in ChatGPT's app directory (checked by hand on 2026-10-03). Prod serves an OpenAI app-domain challenge (`5aedf959ce`), so the app has likely been submitted for review |
| Official plugin [logseq/chatgpt-logseq-plugin](https://github.com/logseq/chatgpt-logseq-plugin) | ❌ for now | Public since 2026-07-13. It bundles the **production** app ID `plugin_asdk_app_6a54a479aba4819180fb254d0bd78be3` plus a DB-graph skill, for ChatGPT and Codex. An unapproved app works only in its owner's developer workspace (*inferred*) |
| Your own ChatGPT developer-mode connector to `https://api.logseq.io/mcp` | ❌ | The only extra OAuth client prod accepts (`COGNITO_CLIENT_IDS = 5nrhvuvjn9aagbpff45rjpfmm9`) is registered to Logseq's own connector callback. A new connector gets its own callback URL, which Cognito rejects (*inferred*) |
| Claude, Cursor or any other MCP client | ❌ | They can connect and list tools without a token, but `execute` needs a scoped token. Cognito offers no dynamic client registration, so these clients cannot obtain one |
| `curl` with the desktop app's login token | ❌ | The app sends its Cognito **ID token** (`frontend/handler/db_based/sync.cljs:33-35`). ID tokens carry no `scope` claim, so every operation fails check 2 with `403 insufficient scope` (*source-only*) |
| Logseq team members with the staging app | ✅ | Staging app `plugin_asdk_app_6a523b64b8d481919e4d3ee457ab54b7` was verified end to end against the non-E2EE `test-mcp` graph (PR #12896 report) |

When the app is approved, it will still need a **synced, non-E2EE DB graph**, and encryption is
the default. In **New graph**, ticking **Use Logseq Sync?** reveals **Encrypt graph data**, which
starts checked (`components/repo.cljs:646`). It must be unticked when the graph is created. The
graph list shows a cloud icon for an unencrypted synced graph and a lock for an encrypted one
(`graph-sync-icon-name`). This page does not cover changing encryption on an existing graph.

### Why direct access is closed

ADR 079 and its research report make these choices deliberately. Read both with
`git show 3d10d64562:docs/agent-guide/079-db-sync-rest-mcp-chatgpt.md` and
`git show 3d10d64562:docs/agent-guide/079-db-sync-rest-mcp-chatgpt_report.md`.

- **The target client is a hosted AI app.** "The first Logseq ChatGPT app consists of the
  remote `/mcp` endpoint, OAuth metadata, OpenAPI-backed tools, and deployment documentation."
  A general developer API is not a stated goal.
- **The server must not hold plaintext.** Decrypting E2EE graphs in the worker was "Rejected
  because the server must not possess plaintext graph keys or content". The API therefore only
  reaches graphs whose owners turned encryption off.
- **Access is granted per OAuth client, not per user.** Scopes come only from Cognito app
  clients that Logseq registers. There are no personal access tokens. The PR report notes that
  Cognito "does not advertise CIMD, dynamic client registration, PKCE methods", so each new
  client is a manual Cognito change.
- **Generated code never sees credentials.** Giving Code Mode the token was rejected because
  "generated code must not receive credentials or arbitrary network access". That suits a
  hosted agent better than a script holding a token.

Opening it to users would take server-side work: either Cognito clients for more MCP clients,
or personal access tokens that carry the two scopes. A clearer graph-encryption choice in the
UI would also help. Neither exists in the code at `9c18a5432b`.

### Using it with Claude or another agent

Options as of 2026-10-03, best first:

| # | Route | Status | Trade-offs |
|---|---|---|---|
| 1 | **Local access instead of the server API**: the desktop [MCP server](mcp-server.md), the [logseq CLI](../../how-to/integrations/cli-basic.md), or a headless MCP server such as `~/Projects/src/github.com/logseq-headless-mcp` | ✅ Works today | Works on **E2EE** and local-only graphs. Needs a machine running Logseq or the CLI |
| 2 | **Ask Logseq** for a Cognito client per agent (Claude.ai connectors and `claude mcp add --client-id …` both take a pre-registered client) or for personal access tokens with the two scopes | ⏳ Needs upstream work | The only route to `api.logseq.io` itself. Still non-E2EE graphs only |
| 3 | **Self-host the Node adapter** (`pnpm build:node-adapter`, see `deps/db-sync/README.md`) and point the app at it with **Settings → Sync Server URL** (stored as `localStorage` `sync-server-url`, `frontend/config.cljs:51-66`). Then call `/sync/<graph-id>/semantic/...` with the app's ordinary ID token | ❓ *Source-only, not tried* | No `/api/v1`, `/openapi.json` or `/mcp` routes on Node, so the agent uses curl or a thin wrapper. The graph moves to your server, and must be non-E2EE: an E2EE graph returns ciphertext titles instead of `409`. The ID token expires, so a script must refresh it. Relies on the unsealed path described under [Authentication and authorization](#authentication-and-authorization), which upstream would likely close |
| 4 | **Deploy the whole worker to your own Cloudflare account** (`wrangler deploy` with D1, R2, Durable Objects, rate-limit and Worker Loader bindings) | ❓ Not tried | The only way to get `/mcp` yourself. The app still signs in through Logseq's Cognito, so tokens still lack scopes, and you would have to patch the scope check. The most work for the least benefit |

Internal paths on the Node adapter mirror the public operations: the `:internal-path` of each
entry in `worker/routes/semantic.cljs`, for example `GET /semantic/pages` for `listPages`.

## Overview

The API has three layers. All of them live in `deps/db-sync/`.

| Layer | Path | What it is |
|---|---|---|
| REST API | `/api/v1/graphs/...` | 36 versioned operations over pages, blocks, properties, tags, tasks, assets and search |
| Spec and docs | `/openapi.json`, `/api-docs` | OpenAPI 3.1 document generated from the operation registry, plus a Redocly HTML render. Both are public |
| MCP | `/mcp` | Model Context Protocol server built on Cloudflare **Code Mode**, aimed at ChatGPT apps |

How a request flows:

```text
ChatGPT ──MCP──▶ /mcp (entry.mjs)
                   │  model-written JS runs in a Dynamic Worker sandbox
                   │  codemode.request() → only /api/v1/* allowed
                   ▼
client ──HTTP──▶ /api/v1/graphs/:graph-id/... (dispatch.cljs)
                   │  1. JWT valid?  2. scope?  3. graph member?  4. not E2EE?  5. rate limit
                   ▼  rewritten to internal /semantic/... path
                 SyncDO (per-graph Durable Object)
                   │  handler/semantic.cljs → deps/outliner ops on the graph's DataScript conn
                   ▼
                 tx_log + broadcast {"type":"changed"} → connected clients pull via normal sync
```

**Key design rule:** mutations go through `deps/outliner` (`insert-blocks!`, `save-block!`,
`move-blocks!`, `page/create!`, `page/delete!`, `property/set-block-property!`, ...). Handlers
never write raw datoms. An API edit therefore lands in the same `tx_log` as an edit from the
app, and connected clients pick it up through the existing sync protocol.

### When to use it, and when not to

| Use the server API when | Use something else when |
|---|---|
| The graph is **synced and not E2EE** | The graph is E2EE: every call returns `409`. Use the desktop [MCP server](mcp-server.md) or [CLI](../../how-to/integrations/cli-basic.md) |
| No Logseq client is running (server-side automation, ChatGPT) | You need Datalog queries: the API has no query endpoint |
| You need simple CRUD on pages, blocks, properties and tasks | You need full-text or semantic search: `searchGraph` is a title substring match |

## Source map

| Concern | File |
|---|---|
| Operation registry, routing, OpenAPI generation | `deps/db-sync/src/logseq/db_sync/worker/routes/semantic.cljs` |
| Auth, scope, E2EE gate, rate limits, forwarding | `deps/db-sync/src/logseq/db_sync/worker/dispatch.cljs` (`handle-semantic-request`) |
| Read/write handlers inside the Durable Object | `deps/db-sync/src/logseq/db_sync/worker/handler/semantic.cljs` |
| Graph listing from D1 | `deps/db-sync/src/logseq/db_sync/index.cljs` (`<semantic-graphs-list`) |
| Signed asset URLs | `deps/db-sync/src/logseq/db_sync/worker/asset_link.cljs` |
| Accepted OAuth client IDs | `deps/common/src/logseq/common/authorization.cljs` (`client-id-allowed?`) |
| Worker entry: `/mcp`, `/api-docs`, OAuth metadata | `deps/db-sync/worker/entry.mjs` |
| MCP tool descriptors shown to ChatGPT | `deps/db-sync/worker/chatgpt_app.mjs` |
| ChatGPT attachment upload | `deps/db-sync/worker/chatgpt_asset_upload.mjs` |
| Base64 size check for Code Mode uploads | `deps/db-sync/worker/semantic_request.mjs` |
| Bindings, rate limits, client IDs per env | `deps/db-sync/worker/wrangler.toml` |

The original design record, ADR 079, was deleted from the repo in `cbe33b9bc0`. Read it with
`git show 3d10d64562:docs/agent-guide/079-db-sync-rest-mcp-chatgpt.md`. Some of its claims no
longer match the code (see [Spec and design drift](#spec-and-design-drift)).

## Authentication and authorization

A graph-scoped request must pass five checks, in this order (`handle-semantic-request`):

| # | Check | Failure |
|---|---|---|
| 1 | Valid Cognito JWT. `iss` matches `COGNITO_ISSUER`; `aud` or `client_id` is in `COGNITO_CLIENT_ID` ∪ `COGNITO_CLIENT_IDS` | `401` |
| 2 | Space-separated `scope` claim contains the operation's scope | `403 insufficient scope` |
| 3 | User owns the graph or is a member of it | the graph-access response (`403`/`404`) |
| 4 | Graph exists in D1 and `graph_e2ee = 0` | `404`, or `409 semantic-api-unavailable-for-e2ee` |
| 5 | Rate limiter allows the call | `429` with `retry-after: 60`; `503 rate limiter unavailable` if the binding is missing |

`listGraphs` is not graph-scoped, so it skips checks 3 and 4.

**Scopes**

| Scope | Grants |
|---|---|
| `logseq/read` | Every `GET` operation |
| `logseq/write` | Every `POST`, `PATCH`, `PUT` and `DELETE` operation |

The OpenAPI document declares an OAuth2 authorization-code flow against the Cognito issuer
(`<issuer>/oauth2/authorize`, `<issuer>/oauth2/token`). `GET /.well-known/oauth-protected-resource`
(and `/.well-known/oauth-protected-resource/mcp`) returns the RFC 9728 metadata that MCP clients
use to discover it:

```json
{"resource":"https://api.logseq.io/mcp",
 "authorization_servers":["https://cognito-idp.us-east-1.amazonaws.com/us-east-1_dtagLnju8"],
 "scopes_supported":["logseq/read","logseq/write"],
 "bearer_methods_supported":["header"]}
```

**Accepted clients.** PR #12896 changed the JWT check from one client ID to a set: the primary
`COGNITO_CLIENT_ID` plus the comma-separated `COGNITO_CLIENT_IDS`. In `wrangler.toml`, prod adds
one extra client and staging adds two. These are the OAuth clients for the ChatGPT app.

**Internal paths are sealed on Cloudflare only.** On the worker, the rewritten `/semantic/...`
path is reachable only from inside; a direct `/sync/<graph-id>/semantic...` request returns `404`
(`worker/dispatch.cljs:207-209`). The self-hosted **Node adapter has no such seal**. It forwards
`/sync/<graph-id>/semantic/...` to the same handlers after checking only the JWT and graph
membership (`node/dispatch.cljs:54-75`). Scope, E2EE and rate-limit checks all live in the
worker's `handle-semantic-request`, and `handler/semantic.cljs` has no E2EE check of its own.
On a shared self-hosted server, any member can therefore write to a graph without
`logseq/write`. *Source-only.*

## Rate limits

These are Cloudflare Rate Limiting bindings from `wrangler.toml`. The numbers are the same in
every environment.

| Binding | Applies to | Limit |
|---|---|---|
| `SEMANTIC_READ_RATE_LIMITER` | operations with `:rate-class :read` | 120 requests / 60 s |
| `SEMANTIC_WRITE_RATE_LIMITER` | operations with `:rate-class :write` | 20 requests / 60 s |

The limiter key is `<sub>:<operationId>:<graph-id>`, so the budget is **per user, per operation,
per graph**, not a global budget. `listGraphs` uses `<sub>:listGraphs`. The sync WebSocket is
not rate-limited. See [Sync Limits](../features/sync-limits.md#semantic-rest-api-per-user-per-operation-per-graph).

## Conventions

### Identifiers

- **Path IDs** (`:page-id`, `:block-id`, `:tag-id`, `:asset-block-id`) are block **UUIDs**.
  Pages, tags, properties and assets are all block entities and share one UUID space.
- **`:property-id`** accepts a UUID, a qualified ident (`logseq.property/status`,
  `user.property/foo-xyz`) or an exact title (`resolve-property-ident`).
- **Entity selectors**, used for ref-typed property values, task status and task priority,
  accept a UUID, an ident or an exact title.

### Response shape: `BlockResponse`

```json
{"uuid":"…","kind":"block","title":"…","order":"a0","parent-id":"…","page-id":"…","children":[…]}
```

`kind` is one of `page`, `block`, `tag`, `property` or `asset`, in that order of precedence
(`block-kind`). For example, an asset block is `asset`, not `block`. Block titles are returned
as stored. A reference appears as `[[<uuid>]]`, not as the page title.

### Pagination

Every list operation is cursor-paginated:

| Query param | Meaning |
|---|---|
| `limit` | 1–200, default 50. Larger values are clamped to 200; `0`, negative or non-numeric gives `400 invalid limit` |
| `cursor` | Opaque value from the previous response's `next-cursor`. A malformed cursor gives `400 invalid cursor`; it never restarts at page one |

The response is `{"<collection>": [...], "next-cursor": "..."}`. `next-cursor` is present only
when another page exists.

### Time filters

`listPages`, `listTasks`, `listTags`, `listTagObjects`, `listProperties`, `listAssets` and
`searchGraph` accept `created-after` and `updated-after`, in Unix **milliseconds**. Both
comparisons are strict (`>`). If both are given, an entity must pass both. Entities with no
timestamp are excluded. Filtering happens before pagination. A non-digit value gives
`400 invalid time filter`.

### How lists are computed (cost)

The ADR says lists never load all graph entities. That holds for only some of them:

| Operation(s) | Strategy | Cost |
|---|---|---|
| `listPageReferences`, `listTagObjects`, `listTasks` | Walk the AVET index for the one target entity, stop at `limit + 1`; cursor = entity id | Bounded |
| `listPageBlocks` | Collect top-level blocks of one page, sort, paginate; each root then includes its **full** descendant tree | Bounded by page size, not by `limit` |
| `listPages`, `listTags`, `listProperties`, `listAssets` | Collect **all** matching entities, sort in memory by `(order or lower-case title, uuid)` | Whole collection per call |
| `searchGraph` | Scan **every** `:block/title` datom for a case-insensitive substring | Whole graph per call |
| `listGraphs` | One D1 query, `ORDER BY updated_at DESC, graph_id`, `LIMIT n+1` | Bounded |

## Endpoint reference

`graph-id` is the graph UUID. Resolve it from a name with `listGraphs?name=`.

### Graphs

| Method | Path | operationId | Notes |
|---|---|---|---|
| GET | `/api/v1/graphs` | `listGraphs` | Graphs you own or belong to that are non-E2EE and `graph_ready_for_use`. `name=` is an exact, case-insensitive match. Rows carry `graph-id`, `graph-name`, `schema-version`, `role`, `invited-by`, `created-at`, `updated-at` |

### Pages

| Method | Path | operationId | Body | Notes |
|---|---|---|---|---|
| GET | `/pages` | `listPages` | — | Entities tagged `#Page` or `#Journal`. **Response key is `blocks`**, not `pages` (see drift) |
| POST | `/pages` | `createPage` | `{"title"}` | `outliner.page/create!`. `201 {"uuid","kind":"page","title"}` |
| GET | `/pages/:page-id` | `getPage` | — | One page, no tree |
| PATCH | `/pages/:page-id` | `updatePage` | `{"title"}` | Rename via `save-block!`. The title is stored verbatim; `[[refs]]` are not processed |
| DELETE | `/pages/:page-id` | `deletePage` | — | `outliner.page/delete!` (page and its tree). `204` |
| GET | `/pages/:page-id/blocks` | `listPageBlocks` | — | Paginates **top-level** blocks in outliner order. Each includes its full subtree, so a page never splits a parent from its children |
| GET | `/pages/:page-id/references` | `listPageReferences` | — | Flat list of entities whose `:block/refs` contains the page. Not expanded |

Paths in the tables below are relative to `/api/v1/graphs/:graph-id`.

### Blocks

| Method | Path | operationId | Body | Notes |
|---|---|---|---|---|
| GET | `/blocks/:block-id` | `getBlock` | — | Only the addressed block (no `children` loaded) |
| PATCH | `/blocks/:block-id` | `updateBlock` | `{"title"}` | Replaces the title. `[[Name]]` refs are resolved, and **a missing page is created** (see below) |
| DELETE | `/blocks/:block-id` | `deleteBlock` | — | Block and its descendants. Deletes pages via `page/delete!` and assets the same way. `204` |
| POST | `/block-moves` | `moveBlocks` | `{"block-ids":[…],"target-id","position"}` | `position`: `before`, `after`, `first-child`, `last-child`. All sources are validated (unique, existing, not the target) before `move-blocks!` |
| POST | `/blocks/:block-id/children` | `insertBlockChildren` | `{"position":"append"\|"prepend","blocks":[BlockTree]}` | `201 {"blocks":[{uuid,title,children?}]}` |
| POST | `/block-trees` | `insertBlockTree` | `{"target-id","position"?,"blocks":[BlockTree]}` | Same as above with the target in the body. `position` defaults to `append`. Both insert under the target as children, not as siblings |

`BlockTree` is `{"title": "...", "children": [BlockTree]}`.

**References in titles.** On insert, capture, createTask and `updateBlock`, every `[[x]]` in a
title is resolved (`prepare-block-title!`):

- If `x` is a UUID or numeric entity id that exists, it becomes a ref to that entity.
- Otherwise `x` is treated as a page **title**. The page is looked up, and **created if
  missing**. The title is then rewritten to `[[<uuid>]]` and `:block/refs` is set.

Tags (`#x`) in titles are not converted (`:replace-tag? false`). Use the `block/tags` property instead.

### Block properties

| Method | Path | operationId | Body | Notes |
|---|---|---|---|---|
| PUT | `/blocks/:block-id/properties/:property-id` | `setBlockProperty` | `{"value": PropertyValue}` | Idempotent. A cardinality-many array **replaces** the existing values. `{"updated":true}` |
| DELETE | `/blocks/:block-id/properties/:property-id` | `deleteBlockProperty` | — | Removes the value. The property definition stays. `204` |
| POST | `/block-properties/batch-set` | `batchSetBlockProperty` | `{"entries":[{block-id,property-id,value}],"isResetExistingValues"?}` | Validates every entry first, then applies all of them in one transaction. Many-values **append** unless `isResetExistingValues: true`. `{"updated":n}` |
| POST | `/block-properties/batch-delete` | `batchDeleteBlockProperty` | `{"entries":[{block-id,property-id}]}` | One transaction. `{"deleted":n}` |

`PropertyValue`:

- **Scalar properties** take a JSON string, number or boolean, passed through unchanged.
- **Ref properties** (`node`, `default` with refs, closed values, `block/tags`) take an entity
  selector or an array of them. Every selector must resolve, or the call fails with
  `400 property value must resolve to an existing entity...`.
- **`block/tags`** values must resolve to **classes**.
- **Status and Priority** also accept case-insensitive aliases:
  `todo doing in-review done canceled backlog` / `low medium high urgent`.

### Tasks

| Method | Path | operationId | Notes |
|---|---|---|---|
| GET | `/tasks` | `listTasks` | Walks the `#Task` class index. Filters: `status`, `priority` (alias or choice selector; an unknown value gives `400`), `content` (case-insensitive title substring). Rows add `status`, `priority` (as `{uuid,ident,title,value}`), `scheduled`, `deadline` |
| POST | `/tasks` | `createTask` | Body `{"title", "page-id"?, "status"? (default todo), "priority"?, "scheduled"?, "deadline"?}`. Inserts a normal block (into `page-id`, or today's journal), tags it `#Task`, sets typed properties, all in one transaction. `201 TaskResponse` |

The tool descriptions tell agents never to write `TODO` text markers and never to list tasks
through search.

`scheduled` and `deadline` are documented as `YYYYMMDD` integers, and the handler passes them
straight to `set-block-property!`. Those built-in properties are `:datetime` (ms) in the DB
schema. A `YYYYMMDD` integer is therefore stored as a millisecond timestamp in January 1970.
*Source-only; not reproduced live.*

### Tags

| Method | Path | operationId | Notes |
|---|---|---|---|
| GET | `/tags` | `listTags` | Entities tagged `#Tag` (classes). `{uuid, ident, title}` |
| POST | `/tags` | `createTag` | `page/create! … {:class? true}`. `201` |
| GET | `/tags/:tag-id` | `getTag` | |
| PATCH | `/tags/:tag-id` | `updateTag` | Rename |
| DELETE | `/tags/:tag-id` | `deleteTag` | `page/delete!`. `204` |
| GET | `/tags/:tag-id/objects` | `listTagObjects` | Flat list of entities whose `:block/tags` contains the tag |

### Property definitions

| Method | Path | operationId | Notes |
|---|---|---|---|
| GET | `/properties` | `listProperties` | All non-hidden properties, including built-ins. `{uuid, ident, title, type, cardinality, choices?}` |
| POST | `/properties` | `createProperty` | `{"title","type"?,"cardinality"?}`. `type` is one of `default number date datetime checkbox url node asset map json string` (default `default`). `cardinality` is `db.cardinality/one` or `db.cardinality/many`. Uses `upsert-property!` |
| GET | `/properties/:property-id` | `getProperty` | `choices` lists closed values, so agents can discover user-added choices |
| PATCH | `/properties/:property-id` | `updateProperty` | Any of `title`, `type`, `cardinality` |
| DELETE | `/properties/:property-id` | `deleteProperty` | `page/delete!`. Built-ins refuse with `400 property cannot be deleted` |

### Capture

| Method | Path | operationId | Notes |
|---|---|---|---|
| POST | `/capture` | `captureToToday` | `{"blocks":[BlockTree]}`. Creates today's journal if missing, then appends. `201 {"page-id","blocks"}` |

"Today" is computed with the Worker's clock (`today-journal-day` uses `js/Date` local getters).
Cloudflare Workers run in **UTC**, so a capture made late in the evening in the Americas, or
early in the morning in Asia, lands on the neighbouring day's journal. *Source-only.*

### Assets

| Method | Path | operationId | Notes |
|---|---|---|---|
| GET | `/assets` | `listAssets` | Blocks with `:logseq.property.asset/type`. `{uuid,title,type,size,checksum}` |
| POST | `/assets` | `createAsset` | Raw body (`application/octet-stream`). Query: `file-name`*, `size`* (exact decoded bytes), `checksum`* (SHA-256 hex), `title`, `page-id` (UUID; default today's journal), `encoding=base64` |
| GET | `/assets/:asset-block-id` | `getAsset` | Metadata plus `url` and `expires-at`: a **5-minute** HMAC-signed `/assets/<graph>/<uuid>.<ext>?expires=&signature=` URL that needs no bearer token |

Upload behaviour (`handle-assets`):

- **Validation:** checks run in this order: file type from the extension, size ≤ 100 MiB
  (otherwise `413`), checksum format, page. An existing identical checksum gives
  `409 asset checksum already exists`.
- **Storage:** the body is streamed to R2 at `<graph-id>/<uuid>.<ext>` and never buffered whole.
  `encoding=base64` is decoded as a stream. `size` and `checksum` always describe the decoded file.
- **Block creation:** the asset block (`#Asset`, type, size, checksum, `remote-metadata`) is then
  inserted through `insert-blocks!`. If that fails, the R2 object is deleted.
- Delete an asset with `DELETE /blocks/:id`.

Signed URLs need the `ASSET_LINK_SECRET` worker secret. A signature is accepted only while
`now < expires ≤ now + 300 s`.

### Search

| Method | Path | operationId | Notes |
|---|---|---|---|
| GET | `/search?q=&types=` | `searchGraph` | Case-insensitive **title substring** over blocks, tags, properties and assets. `types` is a comma list of `blocks,tags,properties,assets` (default all). Each row has `resource`. Not semantic search and not full-text over property values |

## MCP endpoint (`/mcp`)

`entry.mjs` wraps the REST API in a Streamable-HTTP MCP server named `logseq`. A missing
`Accept: application/json, text/event-stream` header is added, because some clients omit it.
`tools/list` returns the descriptors from `chatgpt_app.mjs`:

| Tool | Scopes declared | What it does |
|---|---|---|
| `search` | read + write | Runs model-written JS against the OpenAPI document (`codemode.spec()`), so the full spec stays out of model context |
| `execute` | read + write | Runs model-written JS in a **Dynamic Worker** sandbox (`env.LOADER`). `codemode.request(opts)` is the only exit. The host rejects any path not starting `/api/v1/`, attaches the caller's `Authorization` header, and calls the worker in-process |
| `upload_asset` | read + write | Takes a ChatGPT attachment (`openai/fileParams`). The worker downloads it over HTTPS, requires `content-length` ≤ 100 MiB, hashes while streaming to a temporary R2 key, then posts it to `createAsset`. Use this instead of base64 in `execute` |
| `get_asset_image` | read | Fetches an image asset (≤ 10 MiB, `image/*`) through its signed URL and returns MCP image content plus a UI resource (`ui://…`, `text/html;profile=mcp-app`) so ChatGPT renders it inline |

Credentials never reach the sandbox or the model. The sandbox sees only `codemode.request`, and
the token stays in the host closure. When `execute` code passes `encoding=base64`, the host
recalculates the decoded size itself (`semantic_request.mjs`) instead of trusting the
model-supplied `size`.

Other public routes served by `entry.mjs`:

| Path | Purpose |
|---|---|
| `/api-docs`, `/api-docs/` | Redocly HTML built at release time (`pnpm build:api-docs`), embedded in the bundle, `cache-control: max-age=300` |
| `/.well-known/openai-apps-challenge` | Returns `OPENAI_APPS_CHALLENGE` for OpenAI app-domain verification (added in `5aedf959ce`) |
| `/mcp/icon.png` | Logseq icon for the MCP server info |

### Connecting a client

**The MCP transport has no auth gate.** `handleMcp` never returns `401`; it only forwards
whatever `Authorization` header arrived to the REST calls that `execute` makes. Verified live on
2026-10-03: without a token, `initialize`, `tools/list` and `search` succeed, and `execute`
returns `isError: true` with `Logseq API request failed: 401 {"error":"unauthorized"}`.

ChatGPT learns that it must sign in from the per-tool `securitySchemes` (and the
`_meta.securitySchemes` mirror) in `tools/list`, not from an HTTP challenge. It then reads
`/.well-known/oauth-protected-resource/mcp`, runs the authorization-code flow against Cognito,
and asks for `logseq/read` and `logseq/write`. The MCP authorization spec starts OAuth from a
`401` with `WWW-Authenticate`, which this server never sends, so a client that follows only
the spec connects anonymously.

A token works only when all of these are true:

1. It was issued to `COGNITO_CLIENT_ID` or to one of the `COGNITO_CLIENT_IDS`. Any other client's
   token fails the `aud`/`client_id` check with `401` (`client-id-allowed?`).
2. Its `scope` claim includes the operation's scope. Otherwise the call fails with `403`.
3. The caller is a member of a non-E2EE graph.

In practice only the OAuth clients Logseq registered for ChatGPT can mint such a token (see
[Who can use it today](#who-can-use-it-today)). A client that lets you set a static header, such
as `claude mcp add --transport http logseq https://api.logseq.io/mcp --header "Authorization: Bearer <token>"`,
would work with a valid scoped token, but there is no supported way to get one.

## Testing it

### Local test suites (no account needed)

These run against fake JWT claims and in-memory graphs. They never contact Cognito or a real
graph. Run them from `deps/db-sync` in this worktree.

```sh
cd deps/db-sync
pnpm install --ignore-workspace   # also installs redocly, which a root workspace install skips
pnpm test:chatgpt-app             # node --test: /mcp entry, OAuth metadata, asset-image widget
pnpm test                         # shadow-cljs compile + full db-sync suite
pnpm test:api-docs                # builds the OpenAPI spec and Redoc HTML, then checks them
```

Results on 2026-10-03 at `9c18a5432b` (Node 22.20.0):

| Script | Result | Covers |
|---|---|---|
| `test:chatgpt-app` | 25 / 25 pass | Tool descriptors and annotations, OAuth metadata, `Accept` upgrade, asset-image widget |
| `test` | 191 tests, 4,107 assertions, 0 failures, 0 errors | Route registry, scope, E2EE and rate-limit gates, outliner-backed handlers, plus the rest of db-sync |
| `test:api-docs` | 2 / 2 pass | `/api-docs` routing. Fails with `redocly: command not found` after a root-only install |

Notes:

- `pnpm test` prints many `transact-failed … Nothing found for entity id` lines. These come
  from tests that send bad transactions on purpose; they are not failures.
- `test:api-docs` still names `worker/api_docs_build_test.mjs`, which `862ea7b7d0` (#12942)
  deleted. `node --test` ignores the missing file, so only `api_docs_test.mjs` runs.
- PR #12896 reported 167 tests and 3,915 assertions. The suite has grown since.

### Live probes without a token

Read-only checks against production:

```sh
curl -s https://api.logseq.io/openapi.json | jq '.paths | length'          # 22 paths, 36 operations
curl -s https://api.logseq.io/.well-known/oauth-protected-resource/mcp     # OAuth metadata
curl -s -o /dev/null -w '%{http_code}\n' https://api.logseq.io/api/v1/graphs  # 401

H='content-type: application/json'; A='accept: application/json, text/event-stream'
curl -s https://api.logseq.io/mcp -H "$H" -H "$A" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'                       # four tools
curl -s https://api.logseq.io/mcp -H "$H" -H "$A" -d '{"jsonrpc":"2.0","id":2,
  "method":"tools/call","params":{"name":"search","arguments":{"code":
  "async () => { const s = await codemode.spec(); return Object.keys(s.paths).length }"}}}'
```

`codemode.spec()` returns a promise, so `search` code must `await` it. Without `await`, the call
fails with `Cannot convert undefined or null to object`.

### End to end

An end-to-end test needs a token with the Logseq scopes, which today means the Logseq team's
ChatGPT developer app. The checklist the team used is in the PR #12896 report
(`git show 3d10d64562:docs/agent-guide/079-db-sync-rest-mcp-chatgpt_report.md`, section
"Developer-mode verification"). Run it against a throwaway synced graph created **with
encryption unticked**, because `execute` can delete pages. Once the app is approved, the same
checklist applies to the public app.

## Spec and design drift

Places where the published OpenAPI document, the tool descriptions or ADR 079 disagree with the
code at `9c18a5432b`:

| Claim | Where | What the code does |
|---|---|---|
| `listPages` returns `{"pages":[…]}` (`required: ["pages"]`) | Live `/openapi.json` | Returns `{"blocks":[…]}` (`(paginated-response url :blocks …)`). A client generated from the spec finds no `pages` key |
| "Every collection … never loads complete graph entity sets" | ADR 079, PR body | True for references, tag objects and tasks only. `listPages/Tags/Properties/Assets` sort the whole collection in memory, and `searchGraph` scans every title datom |
| `setBlockProperty` property ID "may be its UUID or ident" | Operation description | Also accepts an exact title (`resolve-property-ident`) |
| `upload_asset.pageId` accepts "UUID, ident, or title" | `chatgpt_app.mjs` | Forwarded as `page-id`, which `createAsset` resolves by UUID only. Anything else gives `400 invalid asset page-id` |
| Block `kind` is `"page"` or `"block"` | ADR 079 | Also `tag`, `property` and `asset` |
| Task `scheduled`/`deadline` are `YYYYMMDD` | `createTask` schema | Stored unchanged in `:datetime` (ms) properties (see Tasks) |

## Compatibility notes

| Graph | Supported |
|---|---|
| DB graph, synced, not E2EE | ✅ |
| DB graph, synced, **E2EE** (the common case) | ❌ `409 semantic-api-unavailable-for-e2ee`. The server cannot decrypt titles |
| Local-only DB graph | ❌ not on the server |
| File graph | ❌ |

| Environment | Worker | D1 | Accepted extra OAuth clients |
|---|---|---|---|
| prod (`--env prod`) | `logseq-sync-prod` | `logseq-sync-graphs-prod` | 1 |
| staging (`--env staging`) | `logseq-sync-staging` | `logseq-sync-graph-meta-staging` | 2 |

## See Also

- [Desktop MCP Server](mcp-server.md): the local server, which works on E2EE graphs
- [db-worker HTTP API](db-worker-http-api.md): the local worker's HTTP surface
- [Sync Limits](../features/sync-limits.md): every server limit in one table
- [Sync protocol](../../../10-agent-guide/db-sync/protocol.md): how `changed` broadcasts reach clients

## Changelog

| Date / commit | Change |
|---|---|
| `3d10d64562` (2026-07-13) | PR #12896: REST API, OpenAPI, `/mcp` with `search`/`execute`, scopes, rate limits, signed asset links |
| 2026-07-13 | [logseq/chatgpt-logseq-plugin](https://github.com/logseq/chatgpt-logseq-plugin) published with the production app ID |
| `5aedf959ce` | `/.well-known/openai-apps-challenge` |
| `91ea251a86`, `e2b7814f64` | Uploads marked as remote assets; original upload bytes preserved (`upload_asset` tool) |
| `998207ffd5` | Renamed to "Logseq Server API" |
| `67d8c2fc0f` | Semantic pagination fix |
