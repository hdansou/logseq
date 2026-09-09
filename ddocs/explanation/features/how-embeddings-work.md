---
title: 'How Semantic Search Works'
description: "The architecture behind Logseq's hybrid keyword + vector search: the embedding sidecar, the zvec index, and how the two result sets are fused"
type: explanation
status: active-development
lastVerified: '2026-09-06'
logseqVersion: '2.0.x'
platforms: [desktop]
graphType: [db]
relatedDocs:
  tutorials:
    - '[Semantic Search Tutorial](../../tutorials/getting-started/semantic-search.md)'
  reference:
    - '[Semantic Search API](../../reference/api/semantic-search-api.md)'
    - '[db-worker HTTP API](../../reference/api/db-worker-http-api.md)'
---

# How Semantic Search Works

> Why Logseq blends vector similarity into keyword search instead of replacing it, and how the pieces fit together.

## The problem

Full-text search finds blocks that contain the words you typed. It cannot find the block that says "the outliner drops the caret when two clients edit the same parent" when you search for *concurrent editing bugs* — there is no lexical overlap at all.

Vector search solves that, but it fails in the opposite direction: ask it for an exact page title and it will happily return four things that are *about* the same topic and rank the exact match fifth. Neither approach alone produces a search box people trust.

Logseq therefore runs both and fuses the results. **Semantic search is not a separate feature or a separate mode** — there is no "semantic search" endpoint. It is an additional ranking signal folded into the existing `search-blocks` call, and it can be silently absent without breaking anything.

That last property drives most of the design. Every stage below is allowed to fail, and every failure degrades to plain keyword search rather than to an error.

## The shape of the system

```
                      ┌──────────────────────────────┐
   your query ───────►│ frontend/search.cljs         │
                      │ stamps the user's setting     │
                      └──────────────┬───────────────┘
                                     │ :thread-api/search-blocks
                                     ▼
                      ┌──────────────────────────────┐
                      │ worker/handler/search.cljs   │
                      │ embeds the query (≤50 ms)     │──────► embedding sidecar
                      └──────────────┬───────────────┘        (Python, HTTP)
                                     │ + :query-embedding
                                     ▼
                      ┌──────────────────────────────┐
                      │ worker/search.cljs           │
                      │                              │
                      │   SQLite FTS  ──┐            │
                      │   fuzzy match ──┼─► RRF ─────┼──► ranked results
                      │   zvec ANN    ──┘            │
                      └──────────────────────────────┘
                                     │
                                     ▼
                            zvec HNSW collection
                            (on disk, per graph)
```

Four processes are involved on the desktop: the renderer, the db-worker (a Node process), the Python embedding sidecar, and — inside the db-worker — the native zvec library. The renderer never talks to the sidecar or to zvec directly.

## Why a Python sidecar

Embedding text requires a transformer model. Logseq could have bundled an ONNX runtime into the Electron binary, but that would add tens of megabytes to every download for a feature most users never enable, on every platform, whether or not it is supported there.

Instead the app shells out to a small stdlib HTTP server (`sidecar/embedding_server.py`) backed by `sentence-transformers`, and installs it into a virtualenv under the user's data directory **the first time semantic search is switched on**. Users who never enable it pay nothing.

The sidecar speaks the OpenAI embeddings shape — `POST /v1/embeddings` with `{"input": [...]}`. This is deliberate: it is a well-known contract, it makes the sidecar trivially replaceable, and it means the endpoint can be pointed at any compatible server by setting `LOGSEQ_EMBEDDINGS_URL`. Nothing in the app assumes the sidecar is the one that ships with it.

The default model is `all-MiniLM-L6-v2` at 384 dimensions, and vectors come back L2-normalized, so cosine similarity is just a dot product.

## Why zvec, and what the index actually holds

Vector search needs an approximate-nearest-neighbour index. Logseq uses `@zvec/zvec`, a native library with per-platform bindings, storing one collection per graph at `<root>/graphs/<graph>/search/vector`.

The collection holds one document per indexed block:

| Field | Type | Purpose |
|---|---|---|
| `embedding` | `VECTOR_FP32`, HNSW, cosine | the vector, dimension from the model |
| `page` | `STRING`, inverted index | lets a page-scoped search filter |
| `title` | `STRING` | carried through to ranking, not indexed |

An important asymmetry: **the text that gets embedded is not the text that gets full-text indexed.** The FTS table stores a sanitized title; the vector document stores the raw one. For pages and classes that raw title includes its namespace parents, so a page nested under `Projects/Infra` embeds with that lineage attached and matches queries about infrastructure even when the page's own name doesn't mention it.

### The context chunk that was tried and withdrawn

Today's chunk is deliberately thin, and it is worth knowing that this is a retreat rather than a starting position.

[ADR 077](../../../docs/agent-guide/077-vector-embedding-context.md) proposed embedding a bounded *context chunk* per block, so that meaning split across neighbouring blocks would still be findable. That design shipped with the original zvec work as context version 2. Each block embedded a labelled six-part text:

```
Page: Infrastructure
Path: Runbooks > Incident response
Previous: Page the on-call engineer
Block: Escalate if unacknowledged after 15 minutes
Children: Use the escalation rota, Do not skip tiers
Next: File a postmortem within 48 hours
```

Segments were capped at 320 characters, parents walked up to three levels, and children were limited to two.

It was removed four weeks later, and the reason is instructive. The ADR anticipated the danger and wrote a constraint against it: *"Do not compute sibling or child context by sorting the same parent children for every block."* Full rebuilds honoured that — they precomputed a children-and-neighbours cache once and reused it across batches. But the **incremental** path, which runs on every transaction, had no cache to hand and fell back to walking each block's parent's children and sorting them, once per changed block. Editing became measurably slower as graphs grew.

The fix collapsed the whole mechanism: `:vector-title` became the plain unsanitized title, and it is now computed only when a vector index actually exists. Bumping the context version to `3` invalidated every index built under the old scheme.

So the trade-off the ADR was weighing is still open, not settled. Richer context genuinely does raise recall. What sank it was not the embedding cost but the *invalidation* cost — a block's vector depending on its neighbours means neighbour edits dirty it, and there was no cheap incremental way to discover that. Anyone revisiting this should start from the incremental path, not the rebuild path.

### Why the index can be thrown away

The collection stores a `metadata.json` recording the model id, the dimension, and the context version. On startup those are compared against what the running platform would produce. Any mismatch — a model swap, a dimension change, a bump to the chunking strategy — invalidates the whole index and triggers a rebuild.

This is cheap to reason about precisely because the index is disposable. It is derived data: it can always be rebuilt from the graph, so the system never has to migrate it.

## Fusing two rankings

The interesting problem is combining a keyword ranking and a vector ranking whose scores mean completely different things. A BM25-ish rank and a cosine similarity are not comparable numbers.

Logseq uses **Reciprocal Rank Fusion**, which throws the raw scores away and uses only each result's *position* in its own list. Each source contributes `weight / (k + rank + 1)`, with `k = 60`. Keyword results carry a weight of `1.25` against the vector side's `1.0`.

Crucially, fusion only engages when there are vector results at all. A search that produced none — flag off, no index, embedding timed out — keeps its original keyword ranking untouched rather than being re-scored through a one-sided RRF.

That asymmetry is the design's core opinion: **keyword search wins ties.** Vector hits are there to surface things keyword search missed entirely, not to reorder things it already found correctly. Two further guards push the same way:

- Vector hits are capped at 10 per query, and anything scoring at or below `0.5` is discarded. That number reads like a similarity cutoff but is not one: the score is `1/(1+distance)`, so the threshold only excludes results further than distance `1` — anti-correlated vectors, essentially. The cap of 10 is doing far more filtering work than the floor is.
- A small term-match boost re-injects lexical evidence into vector-only hits, so a vector result that happens to contain your actual search terms outranks one that merely feels related. Matching the first query term in the title is worth the most; matching only in the surrounding context is worth an order of magnitude less.

The net effect is that turning semantic search on should never make a query that already worked worse. It adds a tail of results, it does not reshuffle the head.

## Failure is the normal case

The query-embedding call has a **50 millisecond budget**. If the sidecar has not answered by then, the worker abandons the embedding, logs a warning, and reruns the search keyword-only.

Fifty milliseconds is far less than a cold sentence-transformer needs, which is intentional: search must stay responsive while you type, and a warm sidecar answers in single-digit milliseconds. But it has a consequence worth internalising — **the first search after the sidecar starts will almost always come back keyword-only**, and so will every search if the sidecar is wedged. From the outside, "semantic search is broken" and "semantic search is slow" look identical.

This is why anything diagnosing the feature should supply its own `:query-embedding` rather than relying on the worker to produce one; that path skips the timeout entirely and tests the index rather than the clock. The [Semantic Search API reference](../../reference/api/semantic-search-api.md) covers how.

The same tolerance runs through the rest of the stack. No vector index on the platform? The option is ignored. Sidecar unreachable? Keyword-only. Index empty or stale? Zero vector hits, keyword results unaffected. There is no configuration in which the search box stops working because the semantic layer is unhappy.

## Why it only runs on Apple silicon

The feature is gated to macOS on arm64, and the gate is enforced independently in three places — the settings row is only offered there, the Electron sidecar refuses to start elsewhere, and the db-worker's Node platform only builds a vector index when it is both on macOS arm64 *and* given an embedding endpoint.

The last condition is worth calling out because it changed: there is no default endpoint any more. A db-worker started without `--embedding-endpoint` (or `LOGSEQ_EMBEDDINGS_URL`) builds no vector index at all, regardless of platform. Semantic search is opt-in at the process level, not just in the UI.

Browser and mobile builds have no vector support whatsoever — their platform adapter simply has no vector implementation, so the code path is unreachable rather than disabled.

## What this rules out

Some consequences follow directly from the architecture and are worth knowing before you build on it:

- **The CLI has no semantic search.** `logseq search` does not call `search-blocks` at all; it runs a Datascript substring query. Enabling the setting changes nothing about CLI results.
- **Plugins cannot force it on.** `logseq.App.search` reaches the same fused pipeline, but the frontend overwrites the flag from application state, so a plugin gets semantic ranking only when the user has already enabled it.
- **There is no "search by vector" API.** You cannot ask for vector hits alone. The only way in is a normal search that happens to carry an embedding.

## Related

- [Semantic Search Tutorial](../../tutorials/getting-started/semantic-search.md) — turning it on and confirming it works
- [Semantic Search API](../../reference/api/semantic-search-api.md) — options, constants, and the platform contract
- [db-worker HTTP API](../../reference/api/db-worker-http-api.md) — invoking `search-blocks` from outside the app
