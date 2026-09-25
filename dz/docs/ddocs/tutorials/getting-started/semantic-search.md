---
title: 'Turn On Semantic Search'
description: 'Enable meaning-based search in your Logseq DB graph and confirm it is actually working'
type: tutorial
status: active-development
lastVerified: '2026-09-24'
reviewStatus: authored-from-source
verifiedScope: 'The 2026-09-24 date covers only the Troubleshooting entry "The toggle is on, but the sidecar log does not exist", observed on Logseq DB 2.0.1 (50e1849-dirty). Everything else dates from 2026-09-06.'
logseqVersion: '2.0.x'
platforms: [desktop]
graphType: [db]
difficulty: beginner
timeToComplete: '15 minutes, plus indexing time'
prerequisites: ['A DB graph', 'macOS on Apple silicon', 'Python 3 installed']
relatedDocs:
  reference:
    - '[Semantic Search API](../../reference/api/semantic-search-api.md)'
  explanation:
    - '[How Semantic Search Works](../../explanation/features/how-embeddings-work.md)'
---

# Turn On Semantic Search

In this tutorial you will switch on semantic search, wait for your graph to be indexed, and run a search that finds a block **by meaning rather than by wording**. By the end you will be able to tell — from what you see on screen — whether semantic search is genuinely running or whether Logseq has quietly fallen back to ordinary keyword search.

No prior setup is needed beyond a DB graph you already use.

## Before you start

Semantic search has hard requirements. Check these first, because if any of them is missing the setting will not appear at all:

- **macOS on Apple silicon** (M1 or later). Intel Macs, Windows, Linux, mobile, and the web app do not support it.
- **A DB graph.** File-based graphs are not supported.
- **Python 3 on your system.** Run `python3 --version` in a terminal. If that fails, install Python 3 before continuing.

You will also need disk space and patience for the first run: Logseq downloads a language model of roughly 90 MB and builds a virtual environment the first time you enable the feature.

## Step 1 — Enable the setting

1. Open **Settings** (<kbd>⌘</kbd> + <kbd>,</kbd>).
2. Go to the **Features** section.
3. Turn on **Semantic search**.

**What you should see:** the toggle moves to the on position and stays there.

**If instead you see a warning** reading *"Install Python 3 to enable semantic search"*, Logseq could not find `python3` on your PATH. Install Python 3, restart Logseq, and repeat this step.

**If you cannot find the toggle at all**, your machine does not meet the requirements above — most commonly because it is an Intel Mac. The row is hidden rather than disabled.

## Step 2 — Let the model install

The first time you enable semantic search, Logseq sets up a Python environment in the background and downloads the embedding model. This is a one-time cost, and it takes a few minutes on a normal connection.

Nothing in the interface reports this directly. To watch it happen, open a terminal and tail the sidecar's log:

```bash
tail -f ~/Library/Application\ Support/Logseq/embedding-server/embedding-server.log
```

**What you should see:** lines showing pip installing `sentence-transformers`, then eventually:

```
Embedding server listening on http://127.0.0.1:<port>/v1/embeddings
```

Once that line appears, the model is downloaded and warmed up.

## Step 3 — Restart Logseq and reopen your graph

Close Logseq completely and reopen it, then open the graph you want to search.

This step matters. Enabling the setting checks that Python exists, but the embedding service and the vector index are wired up when a graph's worker starts. Until you reopen the graph, searches will keep returning keyword-only results.

## Step 4 — Wait for indexing

Logseq now embeds every block in your graph. On a small graph this is quick; on a large one it can take a while, and it runs in the background while you work.

There is deliberately **no progress bar for this stage** — the embedding pass is hidden from the search-index progress indicator so it cannot block or alarm you while you work. So rather than watching the interface, confirm the index landed on disk:

```bash
ls ~/logseq/graphs/*/search/vector/
```

**What you should see:** a `metadata.json` file alongside the collection's data files. Check its contents:

```bash
cat ~/logseq/graphs/*/search/vector/metadata.json
```

```json
{"embedding-model-id":"all-MiniLM-L6-v2","embedding-dimension":384,"context-version":3}
```

If that file is missing, indexing has not completed yet. To force a rebuild rather than wait, run the **Rebuild search index** command from the command palette (<kbd>⌘</kbd> + <kbd>⇧</kbd> + <kbd>P</kbd>) — that path *does* show progress and ends with a *"Search indices rebuilt"* notification.

## Step 5 — Search by meaning

Now the payoff. You want a query that shares **no words** with the block you expect to find — otherwise keyword search would have found it anyway and you have learned nothing.

Pick a block you know exists, then search for a paraphrase of it. For example, if your graph contains:

> *Weekly sync moved to Thursdays because half the team is out on Fridays*

then search for:

```
schedule change for the recurring meeting
```

**What you should see:** the block appears in your results, despite sharing only the word *"the"* with your query.

Try two or three such paraphrases. Semantic matches tend to appear below exact keyword matches — that is by design, and the next step explains how to tell them apart.

## Step 6 — Confirm it is really semantic

This is the step that separates "it works" from "it looks like it works". Search for something that **only** a semantic match could satisfy, and compare against a nonsense control:

1. Search a paraphrase, as in step 5, and note the results.
2. Now search a string that appears nowhere in your graph and means nothing — `qqzzx plover` — and note that you get nothing.
3. Search a paraphrase of a block again, but deliberately misspell every significant word: `skedule chnge recuring meting`.

**What you should see:** step 3 still finds the block. Keyword and fuzzy matching cannot bridge that many misspellings at once, so a hit here is strong evidence the vector index is doing the work.

If step 3 returns nothing while step 5 worked, you were probably seeing fuzzy keyword matching all along.

## Troubleshooting

**Searches feel identical to before.**
The most likely cause is the 50 millisecond budget Logseq gives the embedding service. If the service is slow to wake, the search quietly falls back to keyword-only. Run a few searches in a row — once the service is warm, later searches should start showing semantic hits. If they never do, continue below.

**Nothing works and you want a definitive answer.**
Run the diagnostic script, which tests the index directly and bypasses the timeout:

```bash
dz/support-engineering/script/semantic-search/test_semantic_search.clj "your query"
```

It reports whether the sidecar is reachable, whether the index exists, and whether vector hits come back — and tells you which of those is at fault.

**The toggle is on, but the sidecar log does not exist.**
The app never managed to start the sidecar. Open the main log and look for the failure:

```bash
grep -n 'embedding-server/start-failed' ~/Library/Logs/Logseq/main.log | tail -3
```

Each hit is followed by `Error: spawn …/embedding-server/.venv/bin/python ENOENT`. Do
not take that path at face value: Node reports a missing *working directory* with the
same message, and the sidecar's working directory is the app bundle's own
`Contents/Resources/sidecar`. Check both:

```bash
ls -la ~/Library/Application\ Support/Logseq/embedding-server/.venv/bin/python
ls /Applications/Logseq-DB.app/Contents/Resources/sidecar/embedding_server.py
```

Adjust the bundle name if yours is installed as `Logseq.app`. If the first exists and
the second does not, your build shipped without the sidecar,
and no setting, restart, or reinstall of the Python environment will help. That was the
case on 2026-09-24 with Logseq DB 2.0.1 built 2026-09-21: the venv imported
`sentence_transformers` fine, the setting was on, and every search had been silently
keyword-only since the day the feature was enabled. You need a build that includes the
`sidecar` folder (`resources/electron-builder.yml:52-53`).

**You changed your mind.**
Turn the toggle off in Settings. The embedding service stops immediately. The index stays on disk; delete `~/logseq/graphs/<graph>/search/vector/` if you want the space back.

## What you learned

You enabled semantic search, watched the model install, confirmed the index on disk, and — most usefully — established a repeatable test that distinguishes real semantic matching from fuzzy keyword matching.

## Next steps

- [How Semantic Search Works](../../explanation/features/how-embeddings-work.md) — why results are ordered the way they are, and why keyword matches still win
- [Semantic Search API](../../reference/api/semantic-search-api.md) — querying it from a plugin or script
