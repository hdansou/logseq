---
title: 'Semantic Search API'
description: "Reference for Logseq's hybrid keyword + vector search: search options, ranking constants, the platform vector contract, and the on-disk zvec collection"
type: reference
status: active-development
lastVerified: '2026-09-06'
logseqVersion: '2.0.x'
platforms: [desktop]
graphType: [db]
relatedDocs:
  tutorials:
    - '[Turn On Semantic Search](../../tutorials/getting-started/semantic-search.md)'
  explanation:
    - '[How Semantic Search Works](../../explanation/features/how-embeddings-work.md)'
  reference:
    - '[db-worker HTTP API](db-worker-http-api.md)'
---

# Semantic Search API

> Reference for the vector-search layer inside `search-blocks`. There is no standalone semantic-search endpoint; every entry point below is an option on an ordinary search call.

Verified against `origin/master` at `d2ab772` (2026-09-02).

## Entry points

### `:thread-api/search-blocks`

The single search RPC. Reachable from the renderer via the worker bridge, and from outside the app over [`POST /v1/invoke`](db-worker-http-api.md).

```
[repo q option] -> seq of block results
                   | {:items [...] :matched-count n}   when :include-matched-count? is true
```

| Argument | Type | Description |
|---|---|---|
| `repo` | string | Graph identifier, e.g. `logseq_db_mygraph`. Must match the worker's bound repo. |
| `q` | string | Query text. A blank query returns `nil`. |
| `option` | map | See [Search options](#search-options). |

*Source:* `src/main/frontend/worker/handler/search.cljs`

### `logseq.App.search`

The plugin-facing wrapper.

```js
const results = await logseq.App.search("concurrent editing", { limit: 20 })
```

Semantic ranking is folded in automatically, but **a plugin cannot enable it**: `frontend/search.cljs` overwrites `:feature/enable-semantic-search?` from application state before the call reaches the worker. Passing the flag yourself has no effect.

### Not available

- **CLI.** `logseq search` issues a Datascript substring query and never reaches `search-blocks`. Enabling the setting does not change CLI results.
- **Vector-only queries.** There is no API that returns vector hits without keyword hits.

## Search options

Options relevant to the vector path. All are keys in the `option` map.

**`:feature/enable-semantic-search?`** *(boolean, default `false`)*
Master switch for the vector path. When false or absent, no vector query runs and no embedding is requested. In-app this is stamped from the user's setting; over the HTTP API you must pass it yourself.
*Example:* `{:feature/enable-semantic-search? true}`

**`:query-embedding`** *(vector of floats, default: computed)*
The query vector. Must have exactly `384` elements for the default model. When supplied, the worker uses it directly and **skips the embedding call and its timeout entirely**. When absent, the worker requests one from the platform.
*Example:* `{:query-embedding [0.021 -0.114 ...]}`

**`:page-only?`** *(boolean, default `false`)*
Restricts the search to page results. **Disables the vector path** — vector search never runs when this is true.

**`:page`** *(string, optional)*
Page UUID to scope results to. Applied to vector hits via the collection's inverted `page` index, with progressively widened candidate sets (see [Page-scoped queries](#page-scoped-queries)).

**`:limit`** *(integer, default `100`)*
Maximum results returned overall. Vector hits are separately capped at `10` regardless of this value.

**`:search-limit`** *(integer, default: `:limit`)*
Per-source candidate limit used before fusion.

**`:include-matched-count?`** *(boolean, default `false`)*
Changes the return shape to `{:items [...] :matched-count n}`.

**`:enable-snippet?`** *(boolean, default `true`)*
Whether to build highlighted snippets.

**`:built-in?`**, **`:code-only?`**, **`:dev?`** *(boolean, default `false`)*
Visibility filters applied after fusion.

## Result shape

A vector-derived result carries fields a keyword-only result does not:

| Key | Type | Notes |
|---|---|---|
| `:id` | string | Block UUID. Stable across sources. |
| `:page` | string | Page UUID. |
| `:block/title` | string | Display title. |
| `:vector-score` | float | **Present only on vector hits.** `1 / (1 + distance)`, always `> 0.5`. Not a raw cosine — see below. |
| `:vector-title` | string | The text that was embedded, when it differs from the display title. |

The presence of `:vector-score` is the only reliable signal that the vector path actually ran — checking result counts is not sufficient, because fusion interleaves both sources.

## Ranking constants

Fusion is Reciprocal Rank Fusion over each source's positions; raw scores are discarded. Each source contributes `weight / (rrf-k + rank + 1)`.

**RRF is applied only when vector results are present.** A keyword-only search — because the flag is off, the platform has no vector index, or the embedding timed out — keeps the original keyword scoring untouched. Enabling semantic search therefore cannot reorder a result set it contributed nothing to.

| Constant | Value | Meaning |
|---|---|---|
| `rrf-k` | `60` | RRF damping term |
| `keyword-rrf-weight` | `1.25` | Keyword source weight |
| `vector-rrf-weight` | `1.0` | Vector source weight |
| `source-score-tie-break-weight` | `0.000001` | Tie-break nudge from raw source score |
| `min-vector-search-score` | `0.5` | Floor on `:vector-score`; hits at or below are discarded |
| `max-vector-search-results` | `10` | Hard cap on vector hits per query |
| `query-embedding-timeout-ms` | `50` | Budget for embedding the query before falling back |

Term-match boosts, applied to vector hits so lexical evidence still counts:

| Constant | Value | Applies when |
|---|---|---|
| `primary-title-term-match-boost` | `0.004` | First query term matches the title |
| `title-term-match-boost` | `0.002` | Any later query term matches the title |
| `context-term-match-boost` | `0.0005` | Term matches only the embedded context |
| `max-vector-term-match-boost` | `0.005` | Ceiling on the summed boost |

*Source:* `src/main/frontend/worker/search.cljs`

## Indexing

### What gets embedded

`block->index` emits one search document per block. When a vector index is present it adds `:vector-title` — the **unsanitized** title, as opposed to the sanitized `:title` the FTS table receives.

For pages and classes, `:vector-title` includes namespace parents (`Projects/Infra/Runbook`); journals append their journal day; pages append their aliases. For an ordinary block it is the block's own title.

The embedding input is `:vector-title` when present, else `:title`, truncated to `2048` characters.

> **No cross-block context.** A block's embedding contains only that block's own text. Sibling, child, and parent-block context was implemented as context version `2` and removed in `d53153c` (PR #12818, 2026-06-24) because the incremental indexing path recomputed sibling ordering per changed block. Do not assume `:vector-title` carries neighbouring content — see [How Semantic Search Works](../../explanation/features/how-embeddings-work.md#the-context-chunk-that-was-tried-and-withdrawn).

### Batching

| Constant | Value |
|---|---|
| `vector-embedding-batch-size` | `32` blocks per embedding request |
| `vector-embedding-max-batch-chars` | `65536` (`32 × 2048`) |
| `vector-embedding-max-title-length` | `2048` characters |
| `vector-embedding-parallelism` | `2` concurrent batches |
| `vector-upsert-batch-size` | `1024` documents per zvec upsert |

A batch closes when either the block count or the character budget is reached.

### Index invalidation

`metadata.json` in the collection directory records:

```json
{"embedding-model-id": "all-MiniLM-L6-v2", "embedding-dimension": 384, "context-version": 3}
```

On startup this is compared against what the running platform would produce. **Any mismatch triggers a full rebuild.** The index is derived data and is never migrated.

`vector-context-version` is `3`. Bump it whenever the embedded text changes shape, or stale vectors will be silently retained.

| Version | Shipped | Embedded text |
|---|---|---|
| `2` | `fd1906a`, 2026-05-28 | Labelled context chunk: page, parent path (≤3), previous sibling, own title, children (≤2), next sibling — segments capped at 320 chars |
| `3` | `d53153c`, 2026-06-24 | Unsanitized title only |

### Related thread-api methods

| Method | Effect on the vector index |
|---|---|
| `:thread-api/search-upsert-blocks` | Schedules an async embed + upsert for the given blocks |
| `:thread-api/search-delete-blocks` | Deletes the ids from the collection, then from FTS |
| `:thread-api/search-truncate-tables` | Destroys and reopens the collection |
| `:thread-api/search-build-blocks-indice-in-worker` | Full rebuild; takes an optional `force?` argument |

## Platform vector contract

The db-worker obtains its vector index from the platform adapter at `[:vector :open-index]`. An implementation receives `{:path :dimension}` and returns a map of functions:

| Key | Signature | Notes |
|---|---|---|
| `:query` | `(embedding limit page) -> [result]` | `page` may be `nil` |
| `:upsert!` | `(docs) -> nil` | Docs are `{:id :page :embedding :vector-title}` |
| `:delete!` | `(ids) -> nil` | |
| `:truncate!` | `() -> nil` | Destroys and reopens |
| `:metadata` | `() -> map or nil` | Reads `metadata.json` |
| `:set-metadata!` | `(map) -> nil` | Writes `metadata.json` |
| `:close!` | `() -> nil` | |

Embedding is a separate platform key, `[:embedding :embed-texts]`, with `[:embedding :model-id]` and `[:embedding :dimension]` alongside.

Only the Node adapter implements these. `src/main/frontend/worker/platform/browser.cljs` has no `:vector` key, so the code path is unreachable in browser and mobile builds.

*Source:* `src/main/frontend/worker/platform/node.cljs`

## Availability gates

All of these must hold, and they are enforced independently:

| Gate | Where | Effect if unmet |
|---|---|---|
| macOS **arm64** | `platform/node.cljs` | No vector index is created |
| Embedding endpoint configured | `platform/node.cljs` | No vector index is created |
| macOS | `electron/embedding_server.cljs` | Sidecar `start!` returns `:skipped` |
| macOS arm64 | `components/settings.cljs` | Settings row is hidden |
| `python3` on PATH | `electron/handler.cljs` | Toggle rejects with `:missing-python3` |
| `:feature/enable-semantic-search?` in user config | `electron/configs.cljs` | Sidecar not started at boot |

There is **no default embedding endpoint**. A db-worker started without `--embedding-endpoint` or `LOGSEQ_EMBEDDINGS_URL` builds no vector index, regardless of platform.

## On-disk layout

```
<root-dir>/graphs/<encoded-graph-name>/search/vector/
  metadata.json
  <zvec collection files>
```

`<root-dir>` defaults to `~/logseq`. `<encoded-graph-name>` is `encodeURIComponent` of the graph key with `%20` restored to a space, `~` escaped to `%7E`, and `%` replaced by `~`.

### zvec collection schema

Library: `@zvec/zvec` `0.4.0`, with per-platform native bindings.

| Field | Type | Index |
|---|---|---|
| `embedding` | `VECTOR_FP32` | HNSW, `COSINE` metric |
| `page` | `STRING` | `INVERT` |
| `title` | `STRING` | none |

Opened with `enableMMAP: true`. A missing collection is created on open.

Queries request `outputFields` of `page` and `title` only, with HNSW `ef: 300`. **The stored vector is never returned** — there is no path through the app that reads an embedding back out of the collection.

### How `:vector-score` is derived

zvec returns a `score` that Logseq treats as a **distance**, and converts:

```
:vector-score = 1 / (1 + distance)
```

This lands in `(0, 1]` and is monotonically decreasing in distance. The `0.5` floor is therefore a floor on *distance*, not on cosine similarity: `1/(1+d) > 0.5` holds exactly when `d < 1`. If zvec's cosine distance is `1 - cos`, the effective filter is `cos > 0` — it removes anti-correlated vectors, and is much weaker than a `0.5` similarity cutoff would be. Do not read `:vector-score` as a cosine value or compare it against similarity thresholds from other systems.

### Page-scoped queries

zvec has no combined vector + filter query, so page scoping is done by over-fetching and filtering. Candidate sets widen through multipliers `[4, 16, 64]` of the requested limit until enough same-page results are found or the multipliers are exhausted.

## Embedding service

The default implementation is `sidecar/embedding_server.py` — a stdlib `ThreadingHTTPServer` wrapping `sentence-transformers`. Any OpenAI-compatible embeddings server can replace it.

### `GET /healthz`

```json
{"status": "ok"}
```

### `POST /v1/embeddings`

Request:

```json
{"input": ["some text"], "model": "all-MiniLM-L6-v2"}
```

`input` accepts a string or an array of strings. `model` is optional and defaults to the server's `--model`.

Response:

```json
{"object": "list",
 "model": "all-MiniLM-L6-v2",
 "data": [{"object": "embedding", "index": 0, "embedding": [0.021, -0.114]}]}
```

Vectors are L2-normalized, so cosine similarity equals the dot product. Errors return HTTP 400 with `{"error": "..."}`.

### Running it standalone

```bash
python3 sidecar/embedding_server.py --host 127.0.0.1 --port 8765 --model all-MiniLM-L6-v2
```

| Flag | Default |
|---|---|
| `--host` | `127.0.0.1` |
| `--port` | `8765` |
| `--model` | `all-MiniLM-L6-v2` |
| `--log-file` | none |

### Electron-managed lifecycle

| Item | Value |
|---|---|
| Runtime directory | `<userData>/embedding-server/` |
| Virtualenv | `<userData>/embedding-server/.venv` |
| Dependencies | `sentence-transformers`, `httpx[socks]` |
| Dependency stamp | `deps-v2.ok` |
| Log | `<userData>/embedding-server/embedding-server.log` |
| Readiness timeout | `120000` ms |
| Port | allocated dynamically |
| Published as | `LOGSEQ_EMBEDDINGS_URL` |

Started at app boot when the setting is on, and lazily via `ensure-endpoint!` when a graph's db-worker runtime is created. Stopped immediately when the setting is turned off.

### Environment variables

| Variable | Purpose |
|---|---|
| `LOGSEQ_EMBEDDINGS_URL` | Embedding endpoint. Required for the Node platform to build a vector index. |
| `LOGSEQ_EMBEDDING_MODEL` | Overrides the model id. |
| `LOGSEQ_EMBEDDINGS_PYTHON` | Python interpreter used to build the virtualenv. Defaults to `python3`. |

### Supported model dimensions

| Model | Dimension |
|---|---|
| `all-MiniLM-L6-v2` | `384` |

An unrecognised model id raises `Unsupported embedding model dimension` at platform init.

## Worked example

Querying over the HTTP API with an injected embedding — the path that bypasses the 50 ms budget:

```clojure
;; args, before transit encoding
["logseq_db_mygraph"
 "concurrent editing"
 {:limit 20
  :feature/enable-semantic-search? true
  :query-embedding [0.021 -0.114 ...]}]  ; 384 floats
```

Arguments **must** be transit-encoded; see [db-worker HTTP API](db-worker-http-api.md#post-v1invoke) for why plain JSON silently disables the flag.

A ready-made harness lives at `dz/support-engineering/script/semantic-search/test_semantic_search.clj`. It runs keyword-only, worker-embedded, and injected-embedding probes and diffs them.

## Related

- [How Semantic Search Works](../../explanation/features/how-embeddings-work.md) — design rationale
- [db-worker HTTP API](db-worker-http-api.md) — transport, repo binding, error codes
- [Turn On Semantic Search](../../tutorials/getting-started/semantic-search.md) — end-user setup
