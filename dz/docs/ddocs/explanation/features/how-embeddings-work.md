---
title: 'How Text Embeddings Work'
description: "Understanding the technology behind Logseq's semantic search"
type: explanation
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
relatedDocs:
  tutorials:
    - '[Semantic Search Tutorial](../../tutorials/getting-started/semantic-search.md)'
  reference:
    - '[Semantic Search API](../../reference/api/semantic-search-api.md)'
---

# How Text Embeddings Work

> **Last revised:** 2026-03-10

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

### Three-Worker System

Logseq uses separate threads to keep the UI responsive:

```
┌─────────────────┐
│   Main Thread   │ ← React UI
│   (Browser UI)  │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌───────┐ ┌──────────┐
│  DB   │ │Inference │
│Worker │→│ Worker   │ ← ML Operations
└───────┘ └──────────┘
    │         │
    ▼         ▼
DataScript   HNSW Index
```

### Components

| Component        | Purpose                          |
| ---------------- | -------------------------------- |
| DB Worker        | Database operations, orchestrates embedding tasks |
| Inference Worker | SharedWorker for ML model execution (via Comlink) |
| HNSW Index       | Fast vector search (hnswlib-wasm)  |
| Transformers.js  | Model framework (`@huggingface/transformers`) |

---

## The Embedding Model

### MiniLM-L6-v2

Logseq uses a compact, efficient model:

- **Name**: Xenova/all-MiniLM-L6-v2
- **Size**: ~23MB (fp32)
- **Output**: 384-dimensional vectors
- **Speed**: ~50ms per query
- **Quality**: Excellent for semantic similarity

### Why This Model?

- Small enough for browsers
- Fast enough for real-time search
- Accurate enough for note-taking
- WebGPU acceleration when available

---

## The Search Process

### 1. Indexing (Background)

When you write content:

```
Your text → Tokenize → Model → Embedding → HNSW Index
```

1. **Tokenize**: Split text into model-friendly pieces
2. **Encode**: Convert tokens to numbers
3. **Embed**: Generate 384-dimension vector
4. **Store**: Add to HNSW index

### 2. Searching (Real-time)

When you search:

```
Query → Embedding → HNSW Search → Results
```

1. **Embed query**: Same process as indexing
2. **HNSW search**: Find nearest neighbors
3. **Rank results**: Combine with keyword scores

---

## Vector Similarity

### Distance Metrics

Logseq uses **cosine similarity** measured as distance:

- **0.0**: Identical meaning
- **0.3**: Very similar
- **0.6**: Somewhat related (threshold)
- **1.0**: Unrelated

### Filtering

Results with distance ≥ 0.6 are excluded as not relevant enough.

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
- **Memory efficient**: Fits in browser memory

### Configuration

Logseq initializes HNSW with these parameters (see `init-index!` in `text_embedding.cljs`):

- **M** (max connections per node): 16
- **ef_construction** (index build quality): 200
- **ef_search** (search quality): 32
- **Initial max elements**: 100 (auto-resized as needed)

### How It Works

Imagine a network of connected vectors:

1. Start at a random high-level entry point
2. Greedily move toward query vector
3. Descend to finer layers
4. Return k nearest neighbors

---

## Hybrid Search

### Combining Methods

Logseq uses weighted combination:

```
final_score = (0.9 × keyword_score) + (0.1 × semantic_score) + bonuses
```

### Why This Balance?

- **Exact matches are important**: Users often know what they want
- **Semantic helps discovery**: Surfaces related content
- **Graceful degradation**: Works even if semantic fails

### Bonuses

- Page match: +0.02
- Tagged block: +0.01

---

## What Gets Indexed

### Included

- Page titles
- Block content (title text with UUID references resolved)

### Excluded

- Hidden entities
- View-for blocks (internal property views)
- Empty blocks (blank titles)
- Blocks currently being indexed

Note: Tags and property descriptions are not currently included in the embedding text, though support for them is planned (code exists but is commented out).

### Staleness Detection

Blocks are re-indexed when:

- Content changes
- Title updates
- Last update > last embedding time

---

## Performance Considerations

### Initial Indexing

First-time indexing processes all blocks:

- ~100 blocks/second
- 10,000 blocks ≈ 2 minutes
- Progress shown in UI

### Incremental Updates

After initial index:

- Only changed blocks re-indexed
- Background processing
- Minimal impact on UI

### Memory Usage

| Graph Size     | Approximate Memory |
| -------------- | ------------------ |
| 1,000 blocks   | ~5MB               |
| 10,000 blocks  | ~50MB              |
| 100,000 blocks | ~500MB             |

---

## Storage

### Emscripten Virtual Filesystem (via hnswlib-wasm)

The HNSW index is persisted using hnswlib-wasm's built-in Emscripten filesystem manager:

- Uses `writeIndex`/`readIndex` for serialization
- Synced via `EmscriptenFileSystemManager.isSynced()`
- Per-graph storage (keyed by repo name)
- Survives browser restarts

---

## Limitations

### Language Support

- Best performance: English
- Good: European languages
- Limited: CJK, RTL languages

### Context Window

- MiniLM-L6-v2 supports up to 256 tokens (model limit)
- Logseq does not explicitly truncate; the model's tokenizer handles overflow
- Very long blocks may lose tail content during tokenization

### Semantic Gaps

Model may not understand:

- Domain-specific jargon
- Personal abbreviations
- Very recent concepts

---

## Future Improvements

### Planned Enhancements

- Larger models for better accuracy
- Local fine-tuning
- Multi-lingual models
- Longer context windows

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

_Last updated: 2026-03-10_
