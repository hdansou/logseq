# Graph Vector Embeddings for Agentic Work Investigation

> **Last revised:** 2026-09-06
> **Verified against:** `origin/master` @ `d2ab772` (2026-09-02)

## Question

If I want a vector embedding of a Logseq graph — for agentic work or a plugin — what are my options? Can I read the vectors Logseq already computes?

## Answer

**No, you cannot read Logseq's vectors.** They are written into the zvec collection and never read back out. The query path requests `outputFields` of `page` and `title` only; no code path in the app returns a stored embedding.

You have four options, and only one of them yields actual vectors:

| # | Option | Gives you | Works off macOS arm64 |
|---|---|---|---|
| 1 | Re-embed via the sidecar | **Vectors** | Yes |
| 2 | `POST /api` (port 12315) | Ranked results | Keyword-only |
| 3 | MCP `searchBlocks` | Ranked results | Keyword-only |
| 4 | `POST /v1/invoke` with `:query-embedding` | Ranked results, your vector | Keyword-only |

For agentic work over a whole graph, option 1 for indexing plus option 2 for live lookups is the practical combination. See [How to Build an External Vector Index](../../../ddocs/how-to/integrations/external-vector-index.md).

## Findings

### The stored vectors are write-only

`query-zvec` in `src/main/frontend/worker/platform/node.cljs` requests:

```clojure
:outputFields #js [zvec-page-field zvec-title-field]   ; "page" and "title"
```

The `embedding` field is never listed, and `zvec-result->map` returns only `:id`, `:page`, `:vector-title`, `:vector-score`. The platform contract (`:query`, `:upsert!`, `:delete!`, `:truncate!`, `:metadata`, `:set-metadata!`, `:close!`) has no read-vector operation.

Opening the collection yourself with `@zvec/zvec` and requesting `embedding` as an output field may be possible — that depends on the library, which is not vendored in this repo — but it means reading a live collection out from under the worker that holds the graph lock. Re-embedding is cheaper and safer.

### `:vector-score` is not a cosine similarity

This corrects a plausible-looking misreading. `zvec-result->map` treats zvec's returned `score` as a **distance** and converts it:

```clojure
distance (gobj/get doc "score")
:vector-score (if (number? distance) (/ 1 (+ 1 distance)) 0.0)
```

So `:vector-score = 1 / (1 + distance)`, landing in `(0, 1]`.

The consequence matters for anyone tuning against it. `min-vector-search-score` is `0.5`, which looks like a similarity cutoff but is not:

```
1/(1+d) > 0.5   ⟺   d < 1
```

If zvec's cosine distance is `1 - cos`, the filter admits everything with `cos > 0` — it discards only anti-correlated vectors. **The `max-vector-search-results` cap of 10 does far more filtering than the score floor does.** Do not compare `:vector-score` against similarity thresholds borrowed from other vector stores.

### The four options in detail

**1. Re-embed via the sidecar.** `sidecar/embedding_server.py` is a stdlib HTTP server with no Logseq coupling: `POST /v1/embeddings` with `{"input": [...]}`, OpenAI-shaped. Run it standalone on any port, on any platform. Extract block text via the CLI or `thread-api/q`, embed it, keep your own index. You choose the model, dimension, and chunking, and you are not bound by the 384-dim contract or the platform gate.

**2. `POST /api` on port 12315.** The Electron plugin-facing server (`src/electron/electron/server.cljs`) exposes the whole `logseq.api` surface behind token auth:

```
POST http://127.0.0.1:12315/api
{"method": "logseq.App.search", "args": ["query text"]}
```

Enabled via Settings → Features → HTTP server (`:server/autostart`, `:server/tokens`, default port from `:server/port`).

**3. MCP.** `src/electron/electron/mcp_server.cljs` mounts at `/mcp` on the same port when `:server/mcp-enabled?` is set. Its `searchBlocks` tool calls `logseq.app.search` with `{:enable-snippet? false}`. Input schema is `searchTerm` only — no page scoping, no limit.

**4. `POST /v1/invoke` on the db-worker.** `thread-api/search-blocks` accepting an injected `:query-embedding`. The only path where you supply the vector, so it is the one to use for reranking experiments or for querying Logseq's index with a different model. Also skips the 50 ms embedding budget. Costs: transit encoding required, no authentication, internal API subject to change.

### Constraints that shape the choice

**Platform gating applies to options 2–4.** All three ride the same fused pipeline, which produces vector hits only on macOS arm64 with an embedding endpoint configured and the feature enabled. Everywhere else they silently return keyword-only results. Option 1 is the only one unaffected, because it bypasses Logseq's vector layer entirely.

**Plugins may not reach options 2 and 4 directly.** The db-worker's CORS `Access-Control-Allow-Origin` is exactly `lsp://logseq.com`, which is the main window origin (`src/electron/electron/window.cljs:25`). Whether a plugin iframe inherits that origin was **not verified** — test a `fetch` from the plugin before designing around it. `logseq.App.search` through the SDK always works and needs no network access.

**Plugins cannot force semantic ranking on.** `frontend/search.cljs` overwrites `:feature/enable-semantic-search?` from application state before the call reaches the worker, so a plugin gets it only when the user has already enabled it.

## Not viable

- **Reading `metadata.json`** tells you the model, dimension, and context version, but contains no vectors.
- **`logseq search`** never reaches `search-blocks` — it is a Datascript substring query. See [022](022-logseq-cli-semantic-search.md).
- **A vector-only query API** does not exist. Vector hits are only ever returned fused with keyword hits.

## Open questions

- Does `@zvec/zvec` support returning a vector field via `outputFields`? Untested; the package is not vendored here.
- Does a plugin iframe share the `lsp://logseq.com` origin, and can it therefore reach the db-worker HTTP API?
- Is zvec's returned `score` under `COSINE` actually `1 - cos`? The `1/(1+d)` transform implies a distance, but the exact range was not confirmed against the library.

## Related

- [How to Build an External Vector Index](../../../ddocs/how-to/integrations/external-vector-index.md)
- [Semantic Search API](../../../ddocs/reference/api/semantic-search-api.md)
- [db-worker HTTP API](../../../ddocs/reference/api/db-worker-http-api.md)
- [How Semantic Search Works](../../../ddocs/explanation/features/how-embeddings-work.md)
- [022 — Logseq CLI Semantic Search](022-logseq-cli-semantic-search.md)
