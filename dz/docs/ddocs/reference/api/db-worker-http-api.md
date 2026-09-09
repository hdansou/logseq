---
title: 'db-worker HTTP API'
description: 'Reference for the db-worker-node HTTP server: endpoints, transit encoding, repo binding, lock ownership, and error codes'
type: reference
status: active-development
lastVerified: '2026-09-06'
logseqVersion: '2.0.x'
platforms: [desktop]
graphType: [db]
relatedDocs:
  reference:
    - '[Semantic Search API](semantic-search-api.md)'
    - '[HTTP API Reference](http-api.md)'
---

# db-worker HTTP API

> The local HTTP surface of `db-worker-node`, the Node process that owns a DB graph. It exposes the worker's entire internal RPC surface, so treat it as a privileged local interface, not a public API.

Verified against `origin/master` at `d2ab772` (2026-09-02).

**This is not the same as the [HTTP API](http-api.md)**, which is the app's plugin-facing server. The db-worker API is lower level: it speaks directly to the process holding the SQLite database and the Datascript connection for one graph.

## Process model

One db-worker-node process is bound to exactly **one graph** for its lifetime. Requests naming any other graph are rejected. Multiple workers can run concurrently on different ports, one per graph.

The process holds an exclusive **file lock** on the graph directory. Every repo-bound request re-asserts lock ownership before executing, so a worker that has lost its lock fails requests rather than corrupting the database.

### Starting a worker

```bash
db-worker-node --root-dir ~/logseq --repo logseq_db_mygraph
```

| Flag | Required | Description |
|---|---|---|
| `--root-dir <path>` | yes | Logseq root directory |
| `--repo <name>` | yes | Graph to bind to |
| `--create-empty-db` | no | Start with empty initial datoms |
| `--embedding-endpoint <url>` | no | Embedding service URL. Required for a vector index to be built. |
| `--embedding-model-id <id>` | no | Overrides the embedding model |
| `--log-level <level>` | no | Default `info` |
| `--version` | no | Print build metadata and exit |

In normal desktop use the Electron main process spawns and supervises workers; you rarely start one by hand.

### Discovering a running worker

Running workers append a line to `<root-dir>/server-list`:

```
<pid> <port>
```

Ports are allocated dynamically, so read this file rather than assuming one. Confirm a given port with [`GET /healthz`](#get-healthz), which reports the bound repo.

## Transport

- **Host:** loopback only.
- **Content type:** `application/json` on every endpoint except `/v1/events`.
- **Authentication:** none. Access control is loopback binding plus file-lock ownership.
- **CORS:** `Access-Control-Allow-Origin` is `lsp://logseq.com` only. Browsers on other origins are blocked; non-browser clients are unaffected. `OPTIONS` returns `204`.

## Endpoints

### `GET /healthz`

Liveness plus identity. Returns `200` when ready, `503` while starting.

```json
{
  "repo": "logseq_db_mygraph",
  "status": "ready",
  "host": "127.0.0.1",
  "port": 3000,
  "pid": 41288,
  "owner-source": "cli",
  "root-dir": "/Users/you/logseq",
  "revision": "d2ab772"
}
```

| Field | Type | Notes |
|---|---|---|
| `repo` | string | The bound graph. Use this to match a port to a graph. |
| `status` | string | `"ready"` or `"starting"` |
| `owner-source` | string | What spawned the worker |
| `revision` | string | Build revision |

### `POST /v1/invoke`

The general RPC endpoint. Dispatches any `thread-api` method.

**There is no method allowlist.** Every internal worker method is reachable, including destructive ones such as `:thread-api/unsafe-unlink-db`. Access is constrained only by repo binding and lock ownership.

Request:

```json
{
  "method": "thread-api/search-blocks",
  "argsTransit": "[\"logseq_db_mygraph\",\"query\",[\"^ \",\"~:limit\",20]]"
}
```

| Field | Type | Notes |
|---|---|---|
| `method` | string | Method name without the leading colon. `thread-api/` prefix included. |
| `argsTransit` | string | Transit-JSON encoded argument vector. **Preferred.** |
| `args` | array | Plain JSON fallback. See the warning below. |

Success:

```json
{"ok": true, "resultTransit": "[\"^ \",\"~:id\",\"...\"]"}
```

Failure:

```json
{"ok": false, "error": {"code": "repo-mismatch", "message": "repo does not match bound repo"}}
```

Results are **always** transit-encoded. Decode `resultTransit` with a transit reader; the response is not plain JSON data.

> **Use `argsTransit`, not `args`.**
> The endpoint accepts `args`, but it re-encodes the array verbatim before dispatch. A JSON object's keys stay strings and never become Clojure keywords, so an option map sent as `args` arrives with every key inert. This fails silently — the call succeeds and the options are simply ignored. For example, `:feature/enable-semantic-search?` passed via `args` will never enable semantic search.

#### Transit encoding

Transit-JSON writes maps as `["^ ", k1, v1, k2, v2, ...]` and keywords as `~:name`:

```json
["logseq_db_mygraph",
 "concurrent editing",
 ["^ ", "~:limit", 20, "~:feature/enable-semantic-search?", true]]
```

Responses may use the transit cache — repeated keys appear as `"^0"`, `"^1"` back-references — so a hand-rolled decoder will misread them. Use a real transit library.

The worker registers read handlers for the tags `datascript/Entity`, `error`, and `js/Error`.

#### Slow-call logging

Calls exceeding 10 seconds log a warning. This is **not** a timeout — the call continues. Set your own client timeout.

### `POST /v1/import-db-binary?repo=<repo>`

Imports a raw SQLite database. The body is the binary payload, not JSON. `repo` is a query parameter and is subject to the same repo binding and lock checks. Equivalent to `:thread-api/import-db-binary`.

### `GET /v1/events`

Server-Sent Events stream of worker events. Each message is:

```
data: {"type": "<event-type>", "payload": "<transit-encoded>"}

```

The `payload` field is a transit string requiring a second decode after parsing the JSON envelope. A decoded payload shaped `[keyword, value]` should be read as an event type and body, overriding the envelope's `type`.

The stream sends no heartbeat; clients should handle silent connections and reconnect on close.

### `POST /v1/shutdown`

Requests graceful shutdown. Responds `200` with `{"ok": true}` and stops roughly 10 ms later, so the response arrives before the process exits.

All endpoints return `405` for the wrong HTTP method and `404` for unknown paths.

## Repo binding

Every method receives the repo as its **first argument** and it must match the bound graph. Comparison uses graph-directory identity, so `mygraph` and `logseq_db_mygraph` are treated as the same graph.

A set of methods is exempt because they are process-scoped rather than graph-scoped:

`:thread-api/init`, `:thread-api/get-version`, `:thread-api/list-db`, `:thread-api/set-context`, `:thread-api/sync-app-state`, `:thread-api/update-thread-atom`, `:thread-api/mobile-logs`, `:thread-api/set-db-sync-config`, `:thread-api/get-db-sync-config`, `:thread-api/db-sync-stop`, `:thread-api/db-sync-list-remote-graphs`, `:thread-api/db-sync-update-presence`, `:thread-api/db-sync-ensure-user-rsa-keys`, `:thread-api/get-user-rsa-key-pair`, `:thread-api/init-user-rsa-key-pair`, `:thread-api/reset-user-rsa-key-pair`, `:thread-api/change-e2ee-password`, `:thread-api/get-e2ee-password`, `:thread-api/save-e2ee-password`, `:thread-api/verify-and-save-e2ee-password`, `:thread-api/resolve-ui-request`, `:thread-api/reject-ui-request`, `:thread-api/cancel-ui-requests`

Every method **not** in that set also asserts lock ownership before executing.

## Versioning

**The thread-api is not versioned.** There is no API version number, no per-method version, and no negotiation. What exists:

| Signal | Where | What it tells you |
|---|---|---|
| `/v1/` path prefix | every endpoint | Transport shape only. Has never been bumped; there is no `/v2`. |
| `revision` in `/healthz` | `logseq.common.version/REVISION` | The build the worker was compiled from — a `goog-define` stamped at build time (`"dev"` in dev builds). Changes on every build, whether or not any method changed. |
| `:thread-api/get-version` | `non-repo-methods` | **Listed but never defined.** Invoking it fails with `No method found`. Do not rely on it. |

`def-thread-api` (`src/main/frontend/common/thread_api.cljc`) registers a keyword → function in a map. It carries no docstring, no version, no deprecation marker. Method names, argument shapes, and event payloads change in ordinary commits without notice.

### What the CLI does about it

The only compatibility check in the codebase is **exact revision match, enforced by the CLI**: `cli/server.cljs:106 revision-mismatch?` and `cli/lib/server_runtime.ml:670 revision_matches` compare the worker's `revision` against the CLI's own build and, on mismatch, stop the worker and start one from the CLI's bundled runtime. The Electron app does not currently enforce this (see `dz/docs/10-agent-guide/db-worker/001-db-worker-node-restart-on-version-mismatch.md`, which is CLI-only as of `d2ab772`).

### What a third-party client should do

1. **Pin a revision.** Record the `revision` you developed against; check `/healthz` on connect and warn or refuse on drift. The [client adapter](../../../../../dz/support-engineering/script/db-worker-client/client.mjs) does this via `connect({ expectedRevision })`.
2. **Regenerate the method reference** for the revision you target. The [generated thread-api reference](thread-api.generated.md) is produced from the `def-thread-api` declarations by `dz/support-engineering/script/thread-api-docs/gen_thread_api_docs.clj`; running it with `--check` fails when the committed reference no longer matches source.
3. **Prefer methods the renderer's own remote transport uses** (`persist_db/remote.cljs`) — those change least, because the app depends on them the same way you do.

## Error codes

| Code | HTTP | Cause |
|---|---|---|
| `missing-repo` | `400` | First argument is absent or not a string |
| `repo-mismatch` | `409` | Repo does not match the bound graph. Response includes `repo` and `bound-repo`. |
| `repo-locked` | `409` | Lock is held by another process |
| `invalid-query` | `400` | Datascript query failed to parse |
| `validation-error` | `400` | Method-level validation failure |
| `exception` | `500` | Unclassified error |

A method raising `ex-info` with a numeric `:status` in its data propagates that status verbatim.

## Examples

### Find the worker for a graph

```bash
while read -r pid port; do
  curl -s "http://127.0.0.1:$port/healthz" | jq -r '"\(.port) \(.repo) \(.status)"'
done < ~/logseq/server-list
```

### Invoke a method

```bash
curl -s http://127.0.0.1:3000/v1/invoke \
  -H 'Content-Type: application/json' \
  -d '{"method":"thread-api/search-blocks",
       "argsTransit":"[\"logseq_db_mygraph\",\"outliner\",[\"^ \",\"~:limit\",5]]"}'
```

### Encode and decode transit

Babashka bundles `cognitect.transit`, so no dependencies are needed:

```clojure
(require '[cognitect.transit :as transit])
(import '[java.io ByteArrayInputStream ByteArrayOutputStream])

(defn write-transit [x]
  (let [out (ByteArrayOutputStream.)]
    (transit/write (transit/writer out :json) x)
    (str out)))

(defn read-transit [s]
  (transit/read
   (transit/reader (ByteArrayInputStream. (.getBytes s "UTF-8")) :json
                   {:handlers {"datascript/Entity" (transit/read-handler identity)}})))
```

A complete worked client — server discovery, transit round-trip, and result diffing — is at `dz/support-engineering/script/semantic-search/test_semantic_search.clj`.

## Safety notes

- **No authentication.** Anything that can reach loopback and hold the lock can read and modify the graph.
- **No allowlist.** Destructive methods are reachable over the same endpoint as reads.
- **Do not write concurrently with the app.** The lock is the only guard; bypassing it risks corruption.
- **The RPC surface is internal.** Method names, argument shapes, and semantics change without notice. Pin to a revision, or re-verify against source after upgrading.

## Related

- [Semantic Search API](semantic-search-api.md) — `search-blocks` options
- [HTTP API Reference](http-api.md) — the app's plugin-facing server
