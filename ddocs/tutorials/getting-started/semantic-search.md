---
title: 'Semantic Search'
description: 'Learn to use AI-powered semantic search to find content by meaning'
type: tutorial
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [db]
difficulty: intermediate
timeToComplete: '20 minutes'
prerequisites: ['first-graph']
relatedDocs:
  reference:
    - '[Semantic Search API](../../reference/api/semantic-search-api.md)'
  explanation:
    - '[How Embeddings Work](../../explanation/features/how-embeddings-work.md)'
  howTo:
    - '[Semantic Search Queries](../../how-to/queries/semantic-search.md)'
---

# Semantic Search

> Learn to find content by meaning, not just keywords.

## What You'll Learn

- 🔍 What semantic search is
- ⚙️ How to enable it
- 🎯 How to search effectively
- 📊 Understanding results
- 🔧 Troubleshooting

**Time:** ~20 minutes

---

## What is Semantic Search?

Traditional search finds exact matches:

- Search for "dog" → finds blocks containing "dog"

Semantic search finds meaning:

- Search for "dog" → also finds "puppy", "canine", "pet", "German Shepherd"

### How It Works

1. **Embedding Model**: Converts text to numbers (vectors)
2. **Vector Index**: Stores and organizes these vectors
3. **Similarity Search**: Finds content with similar meaning

---

## Step 1: Enable Semantic Search

### Requirements

- Database graph (not file-based)
- Logseq Desktop (not web)
- ~500MB disk space for model

### Enable Feature

1. Open **Settings** (gear icon)
2. Go to **Features**
3. Toggle **Semantic Search** ON
4. Wait for model download (~100MB)

### First-Time Setup

After enabling:

1. Model downloads automatically
2. Your graph is indexed (may take minutes for large graphs)
3. Status shows in bottom-right

---

## Step 2: Use Semantic Search

### Quick Search

Press `Cmd+K` (macOS) or `Ctrl+K` (Windows/Linux) and type naturally:

Instead of:

```
"meeting notes project alpha"
```

Try:

```
"discussions about the new product launch"
```

### Search Command

Use `/Semantic Search` on any page for dedicated search.

### CLI Search

```bash
logseq search "concepts related to learning"
```

---

## Step 3: Understand Results

### Hybrid Scoring

Search results combine:

- **90%**: Keyword match score
- **10%**: Semantic similarity score

### Result Display

Each result shows:

- Block content
- Match type indicator
- Source page

### Example Results

Query: "machine learning"

| Block                     | Match Type              |
| ------------------------- | ----------------------- |
| "Machine learning basics" | Keyword + Semantic      |
| "Neural network tutorial" | Semantic only           |
| "Training AI models"      | Semantic only           |
| "ML algorithm overview"   | Keyword (ML) + Semantic |

---

## Step 4: Search Effectively

### Good Semantic Queries

✅ **Natural language**

```
"ideas for improving productivity"
```

✅ **Concepts and topics**

```
"health and wellness goals"
```

✅ **Questions**

```
"how to set up development environment"
```

### Less Effective Queries

❌ **Single keywords**

```
"python"  # Too broad
```

❌ **Very specific terms**

```
"UUID-12345"  # Use keyword search
```

❌ **Abbreviations**

```
"API"  # Try "application programming interface"
```

---

## Step 5: Combine Search Methods

### Hybrid Search (Default)

Both keyword and semantic search run together:

- Exact matches rank highest
- Similar concepts appear below
- Best of both worlds

### Force Keyword Only

For exact matches, use quotes:

```
"exact phrase match"
```

### Tag/Property Search

Combine with structured queries:

```
#project related to machine learning
```

---

## How Indexing Works

### Automatic Updates

When you edit content:

1. Block marked as "stale"
2. Re-indexed within minutes
3. New embedding stored

### Manual Re-index

If search seems off:

1. Settings → Features
2. Click "Re-index semantic search"
3. Wait for completion

### What Gets Indexed

✅ Indexed:

- Page titles
- Block content
- Property values

❌ Not indexed:

- System pages (logseq/\*)
- Hidden blocks
- Empty blocks

---

## Performance

### Index Size

- ~384 dimensions per block
- ~1.5KB per block average
- 10,000 blocks ≈ 15MB index

### Search Speed

- Query embedding: ~50ms
- Vector search: ~10ms
- Total: ~100ms for most queries

### Large Graphs

For graphs with 50,000+ blocks:

- Initial indexing: 5-15 minutes
- Use "pause indexing" during intensive work
- Results may be slightly delayed after major edits

---

## Troubleshooting

### "No semantic results"

1. Check semantic search is enabled
2. Wait for initial indexing to complete
3. Try re-indexing in settings

### "Results don't seem relevant"

1. Rephrase query as natural language
2. Be more specific about topic
3. Check indexed content is meaningful (not just links)

### "Indexing stuck"

1. Check status in bottom-right
2. Restart Logseq
3. If persists, try re-indexing

### "High memory usage"

Normal during:

- Initial indexing
- Model loading
- Large search operations

Reduce by:

- Closing other apps
- Waiting for indexing to complete

---

## Technical Details

### Embedding Model

- **Model**: Xenova/all-MiniLM-L6-v2
- **Dimensions**: 384
- **Technology**: HuggingFace Transformers.js
- **Acceleration**: WebGPU when available

### Vector Index

- **Algorithm**: HNSW (Hierarchical Navigable Small World)
- **Library**: hnswlib-wasm
- **Storage**: OPFS (Origin Private File System)

### Distance Threshold

- **Max distance**: 0.6
- **Score formula**: `1 / (1 + distance)`
- **Higher score** = more relevant

---

## Practice Exercise

1. Enable semantic search on your graph
2. Wait for indexing to complete
3. Try these searches:

```
"things I want to accomplish"
"interesting articles to read"
"notes from meetings"
"ideas for projects"
```

4. Compare results with keyword-only search (use quotes)
5. Notice how semantic finds related concepts

---

## What's Next?

- **[Semantic Search Queries](../../how-to/queries/semantic-search.md)** - Advanced usage
- **[How Embeddings Work](../../explanation/features/how-embeddings-work.md)** - Technical deep-dive
- **[Semantic Search API](../../reference/api/semantic-search-api.md)** - For developers

---

## Summary

✅ Enabled semantic search in settings  
✅ Used natural language queries  
✅ Understood hybrid scoring  
✅ Learned indexing process  
✅ Troubleshot common issues

---

_Last updated: 2026-01-29_
