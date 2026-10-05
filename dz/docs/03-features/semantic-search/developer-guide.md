# Semantic Search Developer Guide (Current)

> **Last revised:** 2026-05-29

This guide validates Logseq semantic search behavior against current source in `src/`, `deps/`, and `packages/`.

## Architecture Overview

Current semantic search is implemented as a hybrid keyword + vector pipeline inside the db worker core, not as a dedicated inference worker pair.

- Main entrypoint: `src/main/frontend/worker/db_core.cljs`
- Search logic: `src/main/frontend/worker/search.cljs`
- Browser search engine bridge: `src/main/frontend/search/browser.cljs`
- Runtime vector backend (Node/Electron): `src/main/frontend/worker/platform/node.cljs`

### Current flow

1. Main thread calls `:thread-api/search-blocks`.
2. `db_core` optionally generates query embeddings through `platform/embed-texts`.
3. `search/search-blocks` runs keyword retrieval (FTS/fuzzy/exact) and optional vector retrieval.
4. Results are fused and reranked (`RRF`) with lightweight term-match boosts.

## Verified Thread APIs (Current)

Defined in `src/main/frontend/worker/db_core.cljs`:

- `:thread-api/search-blocks`
- `:thread-api/search-upsert-blocks`
- `:thread-api/search-delete-blocks`
- `:thread-api/search-truncate-tables`
- `:thread-api/search-build-blocks-indice`
- `:thread-api/search-build-blocks-indice-in-worker`
- `:thread-api/search-index-build-progress`

## Indexing Model (Current)

### Keyword index

- Uses SQLite FTS5 trigram virtual table `blocks_fts` with triggers on `blocks`.
- Built and maintained in `src/main/frontend/worker/search.cljs`.

### Vector index

- Optional per runtime/platform.
- On supported Node/Electron paths, vector index is opened by platform adapter and attached to worker state.
- Metadata is persisted and validated:
  - `:embedding-model-id`
  - `:embedding-dimension`
  - `:context-version` (`search/vector-context-version`, currently `3`)

### Build pipeline details

`src/main/frontend/worker/db_core.cljs` now builds indices in cooperative batches with progress reporting and idle-aware pauses.

- Search DB version gate: `search-db-version` is `6` (`src/main/frontend/worker/handler/search.cljs:15`). `blocks_fts` rows share the rowid of their `blocks` row and the triggers delete and update by rowid (`792f86328b`); an index at version 4 migrates in place in batches, other old versions rebuild.
- Batch constants include:
  - `search-index-build-batch-size = 200`
  - `vector-embedding-batch-size = 32`
  - `vector-embedding-parallelism = 2`
- Vector titles are context-expanded (page/path/neighbor/children) before embedding.

## Search Scoring (Current)

In `src/main/frontend/worker/search.cljs`:

- Vector candidate cap: `max-vector-search-results = 10`
- Minimum vector score: `min-vector-search-score = 0.5`
- Keyword + vector fusion uses RRF:
  - `rrf-k = 60`
  - `keyword-rrf-weight = 1.25`
  - `vector-rrf-weight = 1.0`

Result ranking combines:

- Fused RRF score
- Small term-match boosts for title/context
- Stable tie-break by source scores

## Runtime and Model Support (Current)

### Browser runtime

- Browser platform adapter (`platform/browser.cljs`) does not define a local embedding runtime.
- No browser-side `transformers.js` model loading path is present in current worker platform code.

### Node/Electron runtime

- Embeddings are fetched via HTTP endpoint (OpenAI-style `/v1/embeddings`) in `platform/node.cljs`.
- Default endpoint: `http://127.0.0.1:8765/v1/embeddings`.
- Default model id: `all-MiniLM-L6-v2`.
- Supported dimensions map:
  - `all-MiniLM-L6-v2` -> `384`
  - `BAAI/bge-m3` -> `1024`
  - `Qwen/Qwen3-Embedding-4B` -> `1024`

### Electron embedding server

`src/electron/electron/embedding_server.cljs`:

- Manages sidecar lifecycle and health checks.
- Publishes endpoint via `LOGSEQ_EMBEDDINGS_URL`.
- Starts only on macOS (`darwin`) and otherwise returns `:skipped`.

## Removed or Stale Claims from Previous Version

> **Removed:** The dedicated three-context architecture (Main Thread + DB SharedWorker + Inference SharedWorker) is no longer the canonical implementation path in current source.

> **Removed:** `:thread-api/vec-search-*` APIs are not present in current `db_core` thread APIs.

> **Removed:** Worker-to-worker `set-infer-worker-proxy` Comlink bridge is not present in current db worker architecture.

> **Removed:** Browser `transformers.js` WebGPU embedding pipeline (`Xenova/all-MiniLM-L6-v2` loaded directly in browser inference worker) is not present in current `platform/browser.cljs` / db worker search path.

> **Removed:** Hardcoded vector distance threshold filtering at `distance < 0.6` in the old inference/HNSW path is not how current scoring is implemented; current vector filtering uses normalized score threshold (`> 0.5`) and hybrid reranking.

## Findings: New Capabilities

1. Hybrid reranking is explicit and tunable via RRF constants in `search.cljs`.
2. Vector context enrichment is first-class (`Page`, `Path`, `Previous`, `Block`, `Children`, `Next`) and versioned (`vector-context-version`).
3. Index build is cooperative and UI-friendly: time-budgeted batches, idle checks, and progress callbacks (`:thread-api/search-index-build-progress`).
4. Embedding model support expanded beyond MiniLM to include BGE and Qwen embeddings on Node/Electron runtime.
5. Vector index metadata validation prevents stale index/model/context mismatches before reuse.

## Known Constraints (Current)

- Browser runtime path in current platform code does not expose a local embedding backend.
- Electron sidecar embedding server startup is currently macOS-gated in `embedding_server.cljs`.
- Vector result count and threshold are conservative by default (`10` results, minimum score `0.5`).
