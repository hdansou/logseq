---
title: 'Semantic Search API'
description: "API reference for Logseq's semantic search functionality"
type: reference
status: stable
lastVerified: '2026-01-29'
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

> Complete API reference for Logseq's semantic search system.

## Overview

The semantic search API is available through the DB Worker thread API and the CLI.

---

## Thread API

### `vec-search-search`

Execute a semantic search query.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-search repo query limit)
```

**Parameters:**

| Name    | Type   | Description                   |
| ------- | ------ | ----------------------------- |
| `repo`  | string | Graph identifier              |
| `query` | string | Search query text             |
| `limit` | number | Maximum results (default: 10) |

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

### `vec-search-enabled?`

Check if semantic search is available for a graph.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-enabled? repo)
```

**Returns:** `boolean`

---

### `vec-search-reindex`

Force re-indexing of the semantic search index.

**Call:**

```clojure
(state/<invoke-db-worker :thread-api/vec-search-reindex repo)
```

**Returns:** `nil`

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
- Filters results with distance ≥ 0.6
- Skips invalid labels (NaN, overflow)

---

### `task--index-embedding`

Index blocks for semantic search.

```clojure
(task--index-embedding repo)
```

**Behavior:**

- Finds stale blocks (updated since last embedding)
- Batches blocks (500 chars per batch)
- Generates embeddings via inference worker
- Stores in HNSW index
- Updates `hnsw-label-updated-at` timestamps

---

## Inference Worker API

Located in `frontend.inference-worker.text-embedding`.

### `search`

Execute vector similarity search.

```javascript
inferWorker.search(repo, queryString, numNeighbors)
```

**Parameters:**

| Name           | Type   | Description         |
| -------------- | ------ | ------------------- |
| `repo`         | string | Graph identifier    |
| `queryString`  | string | Query text          |
| `numNeighbors` | number | K nearest neighbors |

**Returns:**

```javascript
{
  distances: Float32Array,
  neighbors: Int32Array
}
```

---

### `addBlocks`

Add block embeddings to the index.

```javascript
inferWorker.addBlocks(repo, blocks)
```

**Parameters:**

| Name     | Type   | Description                         |
| -------- | ------ | ----------------------------------- |
| `repo`   | string | Graph identifier                    |
| `blocks` | array  | `[{id: number, text: string}, ...]` |

---

### `removeBlocks`

Remove blocks from the index.

```javascript
inferWorker.removeBlocks(repo, blockIds)
```

---

## CLI Commands

### Search Command

```bash
logseq search [options] <query>
```

**Options:**

| Flag                 | Description               |
| -------------------- | ------------------------- |
| `-g, --graph <name>` | Graph to search           |
| `-l, --limit <n>`    | Max results (default: 20) |
| `-p, --pages-only`   | Only search page titles   |

**Examples:**

```bash
# Basic search (uses semantic if enabled)
logseq search "machine learning"

# Search specific graph
logseq search -g my-notes "project ideas"

# Limit results
logseq search -l 5 "important tasks"
```

**Output:**

```
Results for "machine learning":

1. [Page: AI Research]
   Block: Introduction to neural networks...
   Score: 0.92 (keyword: 0.95, semantic: 0.78)

2. [Page: Study Notes]
   Block: Deep learning fundamentals...
   Score: 0.85 (keyword: 0.0, semantic: 0.85)
```

---

## Search Result Structure

### Combined Result

```clojure
{:id "block-uuid"
 :db/id 12345
 :block/uuid #uuid "..."
 :block/title "Block content"
 :combined-score 0.85        ; Final ranking
 :keyword-score 0.92         ; FTS match score
 :semantic-score 0.78        ; Vector similarity
 :page {:block/name "page-name"}}
```

### Scoring Formula

```
combined_score = (0.9 × normalized_keyword)
               + (0.1 × semantic_score)
               + page_bonus
               + tag_bonus
```

**Bonuses:**

- Page match: +0.02
- Tagged block: +0.01

---

## Configuration

### Graph Settings

In `config.edn`:

```clojure
{:feature/semantic-search? true
 :semantic-search/model "Xenova/all-MiniLM-L6-v2"
 :semantic-search/batch-size 100}
```

### Model Options

| Model              | Size | Dimensions | Notes             |
| ------------------ | ---- | ---------- | ----------------- |
| `all-MiniLM-L6-v2` | 80MB | 384        | Default, balanced |

---

## Data Structures

### HNSW Index

- **Algorithm**: Hierarchical Navigable Small World
- **Library**: hnswlib-wasm
- **Storage**: OPFS (browser) / file (CLI)

### Index Parameters

| Parameter      | Value | Description               |
| -------------- | ----- | ------------------------- |
| M              | 16    | Max connections per layer |
| efConstruction | 200   | Build-time neighbors      |
| efSearch       | 50    | Query-time neighbors      |

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

_Last updated: 2026-01-29_
