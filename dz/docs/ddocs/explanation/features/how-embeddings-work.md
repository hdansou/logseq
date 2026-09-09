---
title: 'How Text Embeddings Work'
description: "Understanding the technology behind Logseq's semantic search"
type: explanation
status: stable
lastVerified: '2026-05-30'
logseqVersion: '0.11.x'
relatedDocs:
  tutorials:
    - '[Semantic Search Tutorial](../../tutorials/getting-started/semantic-search.md)'
  reference:
    - '[Semantic Search API](../../reference/api/semantic-search-api.md)'
---

# How Text Embeddings Work

> **Last revised:** 2026-05-30

> Understanding the AI technology that powers Logseq's semantic search.

## The Core Idea

Text embeddings convert words into numbers that capture meaning. Similar concepts get similar numbers.

```
"dog" → [0.12, 0.85, -0.33, 0.67, ...]  (384 numbers)
"puppy" → [0.15, 0.82, -0.31, 0.69, ...]  (similar!)
"car" → [-0.45, 0.11, 0.72, -0.23, ...]  (different)
```

When you search for "dog", the system finds blocks whose numbers are close to the query's numbers.

---

## Why This Matters

### Traditional Search Limitations

Keyword search only finds exact matches:

- "dog" finds "dog"
- Misses: "puppy", "canine", "Golden Retriever"

### Semantic Search Advantages

Vector similarity finds meaning:

- "dog" finds related concepts
- Works with synonyms
- Handles paraphrasing
- Understands context

---

## Logseq's Architecture

### Current Runtime Flow (Desktop)

For desktop DB graphs, embeddings are served by a local Python process and consumed by the Node db-worker runtime:

```
┌─────────────────┐
│   Main Thread   │ ← React UI
└────────┬────────┘
     │ IPC
     ▼
┌─────────────────┐
│ Electron Main   │
│ embedding_server│
└───────┬─────────┘
    │ HTTP /v1/embeddings
    ▼
┌─────────────────┐      ┌───────────────────────┐
│ Python sidecar  │      │ db-worker-node        │
│ sentence-       │◄────►│ @zvec/zvec HNSW index │
│ transformers    │      │ + SQLite FTS index    │
└─────────────────┘      └───────────────────────┘
```

> **Removed:** The older Inference Worker + `@huggingface/transformers` + `hnswlib-wasm` browser path described here is no longer the active implementation for desktop DB graph semantic search.

### Components

| Component        | Purpose                          |
| ---------------- | -------------------------------- |
| Electron embedding server | Starts/stops local embedding endpoint and publishes `LOGSEQ_EMBEDDINGS_URL` |
| Python sidecar (`sidecar/embedding_server.py`) | Serves `/v1/embeddings` and `/healthz` using `sentence-transformers` |
| db-worker-node platform | Calls embedding endpoint, builds vectors, queries vector index |
| ZVEC index (`@zvec/zvec`) | HNSW + cosine vector retrieval with persisted metadata |
| SQLite FTS index | Keyword retrieval (trigram FTS5) combined with vector results |

---

## The Embedding Model

### MiniLM-L6-v2

Current default model:

- **Name**: `all-MiniLM-L6-v2`
- **Output**: 384-dimensional vectors

Configured alternatives in source:

- `BAAI/bge-m3` (1024)
- `Qwen/Qwen3-Embedding-4B` (1024)

### Why This Model?

- Good default quality/speed tradeoff for note search
- Matches current vector dimension defaults and metadata checks
- Can be overridden via `LOGSEQ_EMBEDDING_MODEL`

---

## The Search Process

### 1. Indexing (Background)

When blocks are indexed/reindexed:

```
Block + local context → Embedding endpoint → ZVEC upsert
```

1. Build a `vector-title` string from page/path/siblings/children context
2. Batch and embed via HTTP endpoint (`/v1/embeddings`)
3. Validate embedding count and upsert to ZVEC collection
4. Persist vector metadata (`embedding-model-id`, `embedding-dimension`, `context-version`)

### 2. Searching (Real-time)

When you search:

```
Query → FTS candidates + query embedding → Vector query → Re-rank
```

1. **Embed query**: Same process as indexing
2. **Vector query**: HNSW cosine search against ZVEC
3. **Combine results**: Reciprocal Rank Fusion (RRF) + match/tie-break scoring

---

## Vector Similarity

### Distance Metrics

Logseq uses **cosine similarity** measured as distance:

- **0.0**: Identical meaning
- **0.3**: Very similar
- **0.6**: Somewhat related (threshold)
- **1.0**: Unrelated

### Filtering

Vector candidates with `vector-score <= 0.5` are filtered out.

### Score Conversion

```
semantic_score = 1 / (1 + distance)
```

- Distance 0.0 → Score 1.0
- Distance 0.3 → Score 0.77
- Distance 0.5 → Score 0.67

---

## HNSW Index

### What is HNSW?

Hierarchical Navigable Small World is a data structure for fast approximate nearest neighbor search.

### Why HNSW?

- **Fast**: O(log n) search time
- **Scalable**: Handles millions of vectors
- **Accurate**: >95% recall of true nearest neighbors
- **Memory efficient**: Supports mmap-backed local collections

### Configuration

Current exposed configuration in code:

- **Index type**: HNSW
- **Metric**: cosine
- **Query `ef`**: 300
- **Vector field**: fp32 with model-specific dimension

> **Removed:** Legacy docs about fixed hnswlib params (`M`, `ef_construction`, `ef_search`) no longer map to the current ZVEC-backed implementation.

### How It Works

Imagine a network of connected vectors:

1. Start at a random high-level entry point
2. Greedily move toward query vector
3. Descend to finer layers
4. Return k nearest neighbors

---

## Hybrid Search

### Combining Methods

When vector results are available, Logseq uses Reciprocal Rank Fusion (RRF):

```
rrf_score(id) = Σ(weight_i / (k + rank_i + 1)),  k = 60
```

Current weights:

- keyword list: `1.25`
- vector list: `1.0`

Then small boosts/tie-breakers are added (for term matches in title/context and source scores).

### Why This Balance?

- Exact FTS matches remain strong
- Vector results improve semantic recall
- If query embedding fails, search falls back to keyword pipeline

### Bonuses

- Non-RRF fallback path still includes small bonuses (page/tag)

---

## What Gets Indexed

### Included

- Page titles
- Block titles with UUID references resolved
- Vector context payload that includes:
  - Page title
  - Parent path
  - Previous sibling title
  - Current block text
  - First child titles
  - Next sibling title

### Excluded

- Hidden entities
- Blank titles
- Closed values and oversized titles

Note: Vector context is capped/truncated to reduce oversized payloads (segment and title length limits, batched char budget).

### Staleness Detection

Reindexing is trigger-driven from changed datoms and tree/context-affecting attributes.

- Title/content/alias/property changes
- Parent/page/order changes (because vector context depends on structure)

> **Removed:** The old DB properties `:logseq.property.embedding/hnsw-label` and `:logseq.property.embedding/hnsw-label-updated-at` were deleted in migration 65.25 and are no longer used for staleness tracking.

## Findings: New Capabilities

- Local embedding service lifecycle is managed by Electron (`setup!`, `ensure-endpoint!`, `stop!`) and injected into db-worker runtime.
- Model dimension compatibility is validated using persisted vector metadata; index rebuilds occur when model/context metadata changes.
- Context-aware vector text is now first-class (`vector-context-version`), improving retrieval beyond plain block text.
- Search ranking moved to RRF-based fusion with explicit keyword/vector weights and deterministic tie-breaks.

---

## Performance Considerations

### Initial Indexing

Search indexing is batched and progress-reported from db-worker:

- FTS indexing batch size: `200`
- Vector embedding batch size: `32`
- Vector embedding parallelism: `2`
- Build progress is staged (FTS then vector) and emitted to clients

### Incremental Updates

After initial index:

- Only changed blocks re-indexed
- Background processing
- Minimal impact on UI

### Memory Usage

> **Removed:** The old fixed memory table was not source-backed for the current ZVEC implementation and has been removed.

---

## Storage

### Search DB + Vector Index Files

Current persistence uses:

- SQLite search DB (`search/db.sqlite` logical path via platform storage)
- ZVEC vector collection (`search/vector`)
- Vector metadata file (`metadata.json`) storing model/dimension/context version
- mmap-enabled index open/create in node platform

---

## Limitations

### Context Window

- Embedding payload text is truncated and batched by character limits in db-worker
- Effective token handling is delegated to the embedding model/tokenizer

### Semantic Gaps

Embedding quality can still vary for:

- Domain-specific jargon
- Personal abbreviations
- Very recent concepts

### Platform Scope

- Vector embeddings are enabled in node runtime only when running on macOS arm64
- Non-vector environments continue to use keyword search

---

## Future Improvements

### Planned Enhancements

- Optional larger embedding models are already wired by model-id/dimension map
- Further quality work can build on context-versioned vector metadata and rebuild logic

### Research Areas

- Cross-modal search (images)
- Query expansion
- Personalized embeddings

---

## See Also

- **[Semantic Search Tutorial](../../tutorials/getting-started/semantic-search.md)**
- **[Semantic Search API](../../reference/api/semantic-search-api.md)**
- **[System Overview](../architecture/system-overview.md)**

---

_Last updated: 2026-05-30_
