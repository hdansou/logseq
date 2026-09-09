---
title: 'Build an External Vector Index of Your Graph'
description: 'Extract blocks from a Logseq DB graph, embed them yourself, and query them from an agent or script — without depending on Logseq internal vector index'
type: how-to
status: active-development
lastVerified: '2026-09-06'
logseqVersion: '2.0.x'
platforms: [desktop]
graphType: [db]
relatedDocs:
  reference:
    - '[db-worker HTTP API](../../reference/api/db-worker-http-api.md)'
    - '[Semantic Search API](../../reference/api/semantic-search-api.md)'
  explanation:
    - '[How Semantic Search Works](../../explanation/features/how-embeddings-work.md)'
---

# Build an External Vector Index of Your Graph

This guide shows how to extract every block from a DB graph, embed the blocks yourself, and keep the resulting index outside Logseq — the practical option when you need vectors for agentic work, retrieval-augmented generation, or a plugin.

**Use this approach when** you need the vectors themselves, need to run on Windows or Linux, need a model other than `all-MiniLM-L6-v2`, or need chunking that Logseq does not do. **Do not use it** if you only need ranked search results — call `logseq.App.search` over [`POST /api`](#appendix-querying-logseqs-own-index-instead) instead and let Logseq do the work.

Logseq's own vectors are not readable. The query path never returns a stored embedding, so re-embedding is the supported way to get them. See [investigation 023](../../../dz/docs/07-investigations/023-graph-vector-embeddings-for-agents.md).

## Assumptions

- A DB graph and a working `logseq` CLI, **or** a running `db-worker-node` you can reach over HTTP.
- Python 3 with `sentence-transformers`, or any embeddings service you prefer.
- You are comfortable with the index being a point-in-time snapshot unless you add the incremental step at the end.

## Pipeline shape

```
  extract          chunk           embed            store            query
 ┌────────┐      ┌────────┐     ┌────────┐      ┌────────┐      ┌────────┐
 │ blocks │─────►│  text  │────►│vectors │─────►│  index │◄─────│  agent │
 │ + uuid │      │ + meta │     │        │      │        │      │        │
 └────────┘      └────────┘     └────────┘      └────────┘      └────────┘
  Datascript      your rules     sidecar or      sqlite/         top-k +
  query           (Logseq does   any provider    faiss/etc       uuid back
                  none)                                          to Logseq
```

The block UUID is the join key throughout. Keep it on every record — it is how you get from a retrieval hit back to something a user can open.

## Step 1 — Extract blocks

Query the graph for every block with a title, pulling the fields you need.

```bash
logseq query --graph my-graph --output edn --query '
[:find (pull ?b [:block/uuid
                 :block/title
                 :block/created-at
                 :block/updated-at
                 {:block/page [:block/uuid :block/title]}
                 {:block/parent [:block/uuid]}])
 :where
 [?b :block/uuid]
 [?b :block/title]]' > blocks.edn
```

Use `--output json` if your downstream tooling prefers it.

> **Verify the shape before you build on it.** Run the query once and inspect the output. Pull results nest differently depending on cardinality, and reference attributes come back as maps rather than scalars. Confirm what you actually get rather than assuming the shape above.

If you would rather not depend on CLI flags, the same query goes through `thread-api/q` over [`POST /v1/invoke`](../../reference/api/db-worker-http-api.md#post-v1invoke). That path needs transit encoding; there is a working client at `dz/support-engineering/script/semantic-search/test_semantic_search.clj`.

### What to exclude

Not every block is worth embedding. Filter out at minimum:

- **Blank and near-blank titles** — no semantic content, and they dilute results.
- **Closed values and property-value nodes** — these are structural entities, not prose.
- **Recycled blocks** — soft-deleted content the user expects to be gone.
- **Very long blocks** — Logseq skips titles over 10,000 characters entirely; you will want a chunking rule instead (step 2).

Logseq's own `block->index` also drops closed values, blank titles, and over-long titles. Matching those exclusions keeps your index comparable to the built-in one.

## Step 2 — Chunk with context

This is where an external index can beat the built-in one, and it is the step worth spending effort on.

Logseq embeds **one block's own title, alone**. It has no parent, sibling, or child context — that design was implemented and then reverted for performance reasons ([details](../../explanation/features/how-embeddings-work.md#the-context-chunk-that-was-tried-and-withdrawn)). The reason it was withdrawn was incremental invalidation cost inside a live editor, which is a constraint a batch pipeline does not have.

So build the context Logseq cannot afford to:

```python
def chunk(block, by_uuid, children_of):
    """One embedding text per block, with bounded local context."""
    parts = []
    page = block.get("page", {}).get("title")
    if page:
        parts.append(f"Page: {page}")

    # ancestor path, nearest first, bounded
    path, cur, depth = [], block.get("parent"), 0
    while cur and depth < 3:
        node = by_uuid.get(cur["uuid"])
        if not node:
            break
        path.append(node["title"])
        cur, depth = node.get("parent"), depth + 1
    if path:
        parts.append("Path: " + " > ".join(reversed(path)))

    parts.append(f"Block: {block['title'][:2000]}")

    kids = [by_uuid[c]["title"] for c in children_of.get(block["uuid"], [])[:3]]
    if kids:
        parts.append("Children: " + "; ".join(kids))

    return "\n".join(parts)
```

Build `children_of` **once** as a single pass over all blocks, not per block. The reverted in-app implementation failed precisely because the incremental path re-derived sibling ordering for every changed block; in a batch job you have every block in hand, so there is no excuse for the quadratic version.

Cap each segment. Long child lists and deep paths crowd out the block's own text, and most embedding models have a token budget well below what an unbounded subtree produces.

## Step 3 — Embed

Run the sidecar standalone. It is stdlib Python plus `sentence-transformers`, with no Logseq dependency:

```bash
python3 sidecar/embedding_server.py --host 127.0.0.1 --port 8765 \
  --model all-MiniLM-L6-v2
```

Then batch your texts through it:

```python
import json, urllib.request

def embed(texts, url="http://127.0.0.1:8765/v1/embeddings", batch=32):
    out = []
    for i in range(0, len(texts), batch):
        payload = json.dumps({"input": texts[i:i + batch]}).encode()
        req = urllib.request.Request(
            url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            body = json.load(r)
        out.extend(d["embedding"] for d in body["data"])
    return out
```

Batch size 32 matches what Logseq uses and is a reasonable default. Vectors come back L2-normalized, so cosine similarity is a plain dot product.

You are not tied to this server or this model. Any OpenAI-compatible embeddings endpoint works with the same code — swap the URL. Larger models will improve retrieval noticeably over `all-MiniLM-L6-v2`, and unlike the in-app path you are not constrained to 384 dimensions.

## Step 4 — Store

For most graphs, do not reach for a vector database. A graph of 50,000 blocks at 384 dimensions is about 75 MB as float32 — brute-force cosine over that is a single matrix multiply taking milliseconds, and it is exact rather than approximate.

```python
import numpy as np

vectors = np.array(embeddings, dtype=np.float32)   # (n, dim), already normalized
np.save("vectors.npy", vectors)
with open("meta.json", "w") as f:                   # row i -> block metadata
    json.dump(metadata, f)
```

Reach for FAISS, hnswlib, or a hosted store when you outgrow that — typically past a few hundred thousand blocks, or when you need the index resident in a long-lived service rather than reloaded per query.

Persist the model id and dimension alongside the vectors, and refuse to query when they disagree with the current model. Logseq does exactly this in its `metadata.json`, and it is the cheapest possible guard against silently comparing vectors from two different models.

## Step 5 — Query

```python
def search(query, vectors, metadata, k=10):
    q = np.array(embed([query])[0], dtype=np.float32)
    scores = vectors @ q                      # cosine, both sides normalized
    top = np.argsort(-scores)[:k]
    return [{**metadata[i], "score": float(scores[i])} for i in top]
```

Because both sides are normalized, `scores` are true cosine similarities in `[-1, 1]` — unlike Logseq's `:vector-score`, which is `1/(1+distance)` and should not be compared against similarity thresholds. Pick your own cutoff empirically; `0.3`–`0.4` is a common starting range for MiniLM.

### Getting back to Logseq

Every hit carries a block UUID, so you can hand the user a link:

```
logseq://graph/<graph-name>?block-id=<uuid>
```

For an agent, returning the UUID alongside the text lets a follow-up tool call fetch the live block through `POST /api` — worth doing, since your snapshot may be stale.

## Step 6 — Keep it fresh

The simplest correct approach is to re-run the whole pipeline on a schedule. Embedding 50,000 blocks with MiniLM takes minutes, and a full rebuild has no invalidation bugs.

If that is too slow, drive updates from `:block/updated-at`: extract only blocks changed since your last run, re-embed those, and replace their rows. **If you chunk with context (step 2), also re-embed the neighbours of every changed block** — its parent, its children, and its siblings — because their chunks contain the changed text. This is exactly the bookkeeping that made the in-app version too expensive; in a batch job it is a manageable extra query, but it is easy to forget and the failure is silent.

For live updates, `GET /v1/events` on the db-worker streams transactions over SSE. Treat that as a trigger to re-extract rather than a source of truth about which blocks to touch.

## Appendix: querying Logseq's own index instead

If you decided partway through that you only need ranked results, stop and use this instead:

```bash
curl -s http://127.0.0.1:12315/api \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{"method": "logseq.App.search", "args": ["concurrent editing"]}'
```

Enable the server in Settings → Features → HTTP server and set a token. Results are keyword and semantic fused, ranked by Logseq. Note that the semantic half only contributes on macOS arm64 with the feature enabled — elsewhere you get keyword results, silently.

## Related

- [Investigation 023 — Graph Vector Embeddings for Agentic Work](../../../dz/docs/07-investigations/023-graph-vector-embeddings-for-agents.md)
- [Semantic Search API](../../reference/api/semantic-search-api.md) — the built-in pipeline's constants and contracts
- [db-worker HTTP API](../../reference/api/db-worker-http-api.md) — extraction over `thread-api/q`
