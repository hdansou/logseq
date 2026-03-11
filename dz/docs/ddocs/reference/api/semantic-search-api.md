---
title: 'Semantic Search API'
description: "API reference for Logseq's semantic search functionality"
type: reference
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [db]
relatedDocs:
  tutorials:
    - '[Semantic Search Tutorial](../../tutorials/getting-started/semantic-search.md)'
  explanation:
    - '[How Embeddings Work](../../explanation/features/how-embeddings-work.md)'
---

# Semantic Search API

> **Last revised:** 2026-03-10

> Complete API reference for Logseq's semantic search system.

## Overview

The semantic search API is available through the DB Worker thread API. The CLI provides basic text search but does not currently use semantic/vector search.

---

## Thread API

### `vec-search-search`

Execute a semantic search query.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-search repo query nums-neighbors)
```

**Parameters:**

| Name             | Type   | Description                          |
| ---------------- | ------ | ------------------------------------ |
| `repo`           | string | Graph identifier                     |
| `query`          | string | Search query text                    |
| `nums-neighbors` | number | Number of nearest neighbors to return |

**Returns:**

```clojure
[{:block <block-entity>
  :distance <float>}
 ...]
```

**Example:**

```clojure
(m/sp
  (let [results (m/? (state/<invoke-db-worker
                       :thread-api/vec-search-search
                       "my-graph"
                       "machine learning concepts"
                       20))]
    (doseq [{:keys [block distance]} results]
      (println (:block/title block) "- Distance:" distance))))
```

---

### `vec-search-embedding-model-info`

Get information about available and currently loaded embedding models.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-embedding-model-info repo)
```

**Returns:**

```clojure
{:available-model-names ["Xenova/all-MiniLM-L6-v2"]
 :graph-text-embedding-model-name "Xenova/all-MiniLM-L6-v2"}
```

---

### `vec-search-embedding-graph`

Index (or re-index) the graph for semantic search.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-embedding-graph repo opts)
```

**Parameters:**

| Name   | Type   | Description                                                |
| ------ | ------ | ---------------------------------------------------------- |
| `repo` | string | Graph identifier                                           |
| `opts` | map    | Options map. `{:reset-embedding? true}` to force re-index. |

**Returns:** `nil`

---

### `vec-search-cancel-indexing`

Cancel an in-progress indexing operation.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-cancel-indexing repo)
```

**Returns:** `nil`

---

### `vec-search-update-index-info`

Refresh the vector search index state (current count, dimensions, etc.).

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-update-index-info repo)
```

**Returns:** `nil`

---

### `vec-search-load-model`

Load a specific embedding model for the graph.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-load-model repo model-name)
```

**Parameters:**

| Name         | Type   | Description                             |
| ------------ | ------ | --------------------------------------- |
| `repo`       | string | Graph identifier                        |
| `model-name` | string | Model name (e.g. "Xenova/all-MiniLM-L6-v2") |

---

### `vec-search-init-embedding-model`

Initialize the embedding model previously configured for the graph.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-init-embedding-model repo)
```

---

## Embedding Module API

Located in `frontend.worker.embedding`.

### `task--search`

Internal search function called by the worker.

```clojure
(task--search repo query-string nums-neighbors)
```

**Parameters:**

| Name             | Type   | Description                 |
| ---------------- | ------ | --------------------------- |
| `repo`           | string | Graph repository            |
| `query-string`   | string | Search query                |
| `nums-neighbors` | number | Number of nearest neighbors |

**Returns:**

```clojure
[{:block <entity>
  :distance <float>}
 ...]
```

**Behavior:**

- Returns empty if indexing is in progress
- Excludes results with distance >= 0.6 (lower distance = better match)
- Skips invalid labels (NaN, overflow past `2147483647`)

---

### `embedding-graph!`

Entry point for indexing blocks for semantic search.

```clojure
(embedding-graph! repo {:keys [reset-embedding?]})
```

**Behavior:**

- Checks that a model name has been set (via `logseq.kv/graph-text-embedding-model-name`)
- Marks blocks with `hnsw-label-updated-at` = 0 if no prior embedding exists or if resetting
- Delegates to `task--embedding-stale-blocks!` which:
  - Finds stale blocks (where `block/updated-at` > `hnsw-label-updated-at`)
  - Partitions blocks by text size (500 characters per partition)
  - Generates embeddings via inference worker (`text-embedding+store!`)
  - Writes HNSW index to storage
  - Updates `hnsw-label-updated-at` timestamps

---

## Inference Worker API

Located in `frontend.inference-worker.inference-worker` (class `InferenceWorker`), backed by `frontend.inference-worker.text-embedding`.

### `search`

Execute vector similarity search (k-nearest-neighbor).

```javascript
inferWorker.search(repo, queryString, numsNeighbors)
```

**Parameters:**

| Name             | Type   | Description         |
| ---------------- | ------ | ------------------- |
| `repo`           | string | Graph identifier    |
| `queryString`    | string | Query text          |
| `numsNeighbors`  | number | K nearest neighbors |

**Returns:**

```javascript
{
  distances: Float32Array,
  neighbors: Int32Array
}
```

---

### `text-embedding+store!`

Generate embeddings for text and add them to the HNSW index.

```javascript
inferWorker["text-embedding+store!"](repo, textArray, labels, replaceDeleted)
```

**Parameters:**

| Name              | Type       | Description                              |
| ----------------- | ---------- | ---------------------------------------- |
| `repo`            | string     | Graph identifier                         |
| `textArray`       | string[]   | Array of block text strings              |
| `labels`          | number[]   | Array of `db/id` values (HNSW labels)    |
| `replaceDeleted`  | boolean    | Whether to replace previously deleted items |

---

### `delete-labels`

Remove items from the HNSW index by label.

```javascript
inferWorker["delete-labels"](repo, labels)
```

---

### `force-reset-index!`

Remove all data from the HNSW index and reinitialize it.

```javascript
inferWorker["force-reset-index!"](repo)
```

---

### `write-index!`

Persist the HNSW index to storage (OPFS).

```javascript
inferWorker["write-index!"](repo)
```

---

### `index-info`

Get current index statistics.

```javascript
inferWorker["index-info"](repo)
```

**Returns:**

```javascript
{
  currentCount: number,   // Number of items in index
  maxElements: number,    // Current capacity
  efSearch: number,       // Query-time ef parameter
  numDims: number         // Embedding dimensions
}
```

---

## CLI Commands

### Search Command

```bash
logseq search [options] <query>
```

The CLI search command performs basic text matching on `:block/title`. It does not use semantic/vector search. It supports two modes: local graph search (`-g`) and in-app API search (`-a`).

**Options:**

| Flag                              | Description                              |
| --------------------------------- | ---------------------------------------- |
| `-g, --graph <name>`              | Local graph to search                    |
| `-l, --limit <n>`                 | Max results (default: 100)               |
| `-r, --raw`                       | Print raw response                       |
| `-a, --api-server-token <token>`  | API server token to search in-app graph  |

**Examples:**

```bash
# Search a local graph
logseq search -g my-notes "project ideas"

# Search with API (in-app graph)
logseq search -a <token> "machine learning"

# Limit results
logseq search -g my-notes -l 5 "important tasks"
```

---

## Search Result Structure

When semantic search is active, the in-app combined search produces results with the following fields:

### Combined Result

```clojure
{:id "block-uuid-string"
 :title "Block content"
 :combined-score 0.85        ; Final ranking score
 :keyword-score 0.92         ; FTS/fuzzy match score
 :semantic-score 0.78        ; Vector similarity (1 / (1 + distance))
 :page "page-uuid-string"}
```

### Scoring Formula

```
combined_score = (0.9 x normalized_keyword_score)
               + (0.1 x semantic_score)
               + bonus
```

Where `semantic_score = 1.0 / (1.0 + cosine_distance)`.

**Normalization:** Keyword scores are min-max normalized across the result set. Page blocks receive a +2 boost to the raw keyword score before normalization.

**Bonuses:**

- Page match: +0.02
- Tagged block: +0.01

---

## Configuration

### Model Selection

Semantic search is configured per-graph through the Logseq UI (Settings > Features). The selected model name is stored in the database as `logseq.kv/graph-text-embedding-model-name`.

There are no `config.edn` settings for semantic search. Model loading and index management are handled through the thread API calls described above.

### Model Options

| Model                    | Dimensions | Notes             |
| ------------------------ | ---------- | ----------------- |
| `Xenova/all-MiniLM-L6-v2` | 384        | Default, balanced |

---

## Data Structures

### HNSW Index

- **Algorithm**: Hierarchical Navigable Small World
- **Library**: hnswlib-wasm
- **Distance metric**: cosine
- **Storage**: OPFS (Origin Private File System in browser)

### Index Parameters

| Parameter      | Value | Description               |
| -------------- | ----- | ------------------------- |
| M              | 16    | Max connections per layer |
| efConstruction | 200   | Build-time search depth   |
| efSearch       | 32    | Query-time search depth   |

---

## Error Handling

### Common Errors

| Error                  | Cause                        | Solution                |
| ---------------------- | ---------------------------- | ----------------------- |
| "Model not loaded"     | First-time startup           | Wait for model download |
| "Index not available"  | Indexing in progress         | Wait or check status    |
| "Worker not connected" | Worker initialization failed | Restart Logseq          |

### Error Recovery

```clojure
(try
  (m/? (state/<invoke-db-worker :thread-api/vec-search-search ...))
  (catch js/Error e
    (log/error :semantic-search-error {:error e})
    ;; Fallback to keyword search
    (keyword-search-fallback query)))
```

---

## Performance

### Latencies

| Operation         | Typical Time |
| ----------------- | ------------ |
| Query embedding   | ~50ms        |
| HNSW search       | ~10ms        |
| Result processing | ~20ms        |
| Total             | ~100ms       |

### Scaling

| Graph Size     | Index Memory | Search Time |
| -------------- | ------------ | ----------- |
| 1,000 blocks   | ~5MB         | ~50ms       |
| 10,000 blocks  | ~50MB        | ~80ms       |
| 100,000 blocks | ~500MB       | ~150ms      |

---

## See Also

- **[Semantic Search Tutorial](../../tutorials/getting-started/semantic-search.md)**
- **[How Embeddings Work](../../explanation/features/how-embeddings-work.md)**
- **[CLI Reference](../api/http-api.md)**

---

_Last updated: 2026-03-10_
