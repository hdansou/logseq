---
title: 'Build a UI on the db-worker API'
description: 'Connect a new frontend to a running Logseq graph over the db-worker HTTP + SSE transport: discovery, live reads, a change stream, and a validated write'
type: tutorial
status: active-development
lastVerified: '2026-09-08'
logseqVersion: '2.0.x'
platforms: [desktop]
graphType: [db]
difficulty: intermediate
timeToComplete: '45 minutes'
prerequisites: ['A DB graph open in Logseq desktop', 'Node.js 18+', 'babashka (for the reference generator)']
relatedDocs:
  reference:
    - '[db-worker HTTP API](../../reference/api/db-worker-http-api.md)'
    - '[Generated thread-api reference](../../reference/api/thread-api.generated.md)'
  explanation:
    - '[How Semantic Search Works](../../explanation/features/how-embeddings-work.md)'
---

# Build a UI on the db-worker API

In this tutorial you will connect a second frontend to a graph that Logseq desktop already has open, read live data from it, watch changes arrive as the app edits, and make one validated write of your own. By the end you will have a working client you can grow into a UI, and a generated reference of every method it could call.

You will use the same transport Logseq's own renderer uses — `POST /v1/invoke` and `GET /v1/events` on `db-worker-node` — so nothing here is a workaround. The thing to internalise before you start is the one hard constraint: **this API has no versioning and no authentication.** Everything below is built to cope with that.

## Before you start

- Logseq desktop is running with a DB graph open. That is what spawns the worker you will talk to.
- `node --version` is 18 or newer (for global `fetch`).
- `bb --version` works — babashka runs the reference generator in step 6.
- You are on the machine running Logseq. The worker binds to loopback only, and its CORS policy allows only the app's own origin, so a browser tab elsewhere cannot reach it. Your eventual UI must live in a desktop shell (Electron, Tauri) or behind a local proxy.

## Step 1 — Find the worker

Every running worker writes `<pid> <port>` to `~/logseq/server-list`. The client adapter reads that and probes each port:

```bash
cd dz/support-engineering/script/db-worker-client
npm install
node -e '
import("./client.mjs").then(async ({ discover }) => {
  const d = await discover();
  for (const w of d.workers) console.log(w.baseUrl, w.status, w.repo, w.revision);
  console.log("chosen:", d.chosen?.baseUrl);
});'
```

**What you should see:** one line per worker, and `chosen:` pointing at the one whose `repo` is your open graph:

```
http://127.0.0.1:52417 ready logseq_db_mygraph 2a9c1f0
chosen: http://127.0.0.1:52417
```

**If `chosen:` is `undefined`,** no worker is ready. Open a graph in Logseq and re-run. If `server-list` lists a port that is unreachable, that worker has exited without cleaning up — `logseq server cleanup` removes stale entries.

Write down the `revision`. You will pin to it in the next step.

## Step 2 — Connect, and pin the revision

```js
import { connect } from "./client.mjs";

const client = await connect({
  baseUrl: "http://127.0.0.1:52417",
  expectedRevision: "2a9c1f0",     // from step 1
});
console.log("connected to", client.repo, "at", client.revision);
```

**What you should see:** `connected to logseq_db_mygraph at 2a9c1f0`.

Now deliberately break it — change `expectedRevision` to `"nope"` and run again.

**What you should see:** a thrown `WorkerError` with `code: "revision-mismatch"`. This is the check that will save you after a Logseq update: the thread-api has no version of its own, so an exact build-revision match is the only compatibility signal available. Keep it in your connect path; never remove it to make an error go away.

Put the real revision back before continuing.

## Step 3 — Read something

Start with a Datascript query written as EDN. The adapter turns it into transit for you:

```js
const rows = await client.q(`
  [:find (pull ?p [:block/uuid :block/title])
   :where [?p :block/name] [?p :block/journal-day]]`);
console.log(rows.length, "journal pages");
console.log(rows[0]);
```

**What you should see:** a count and one row shaped like `[{ "block/uuid": "...", "block/title": "Sep 8th, 2026" }]`. Keyword keys arrive as `"ns/name"` strings; UUIDs as strings; sets as arrays.

Now pull one entity by lookup ref:

```js
const [{ "block/uuid": uuid }] = rows[0];
const page = await client.pull("[*]", ["block/uuid", uuid]);
console.log(Object.keys(page));
```

**What you should see:** the page's full attribute list — `db/id`, `block/title`, `block/created-at`, and so on.

**If you get `WorkerError: repo does not match bound repo`,** you constructed the client with a `repo` the worker isn't bound to. Use `connect()` without a `repo` argument and it binds to whatever the worker reports.

## Step 4 — Watch the app edit

This is the step that proves a second client is a first-class citizen. Subscribe to the change stream, then go edit a block in Logseq:

```js
const sub = client.subscribe((type, payload) => {
  if (type === "sync-db-changes") {
    console.log("tx", payload["tx-id"], "touched", payload["tx-data"]?.length, "datoms");
  }
});
// ...type something into any block in Logseq, then:
setTimeout(() => sub.close(), 30_000);
```

**What you should see:** within a second of each keystroke, a `tx` line. The worker broadcasts every committed transaction to **every** connected SSE client, not just the app.

The full list of event types the renderer itself handles is in the [generated reference](../../reference/api/thread-api.generated.md#sse-events-get-v1events). For a UI, `sync-db-changes` is the one that matters: treat it as "something changed, re-read what you're showing."

## Step 5 — Make one validated write

Writes go through `apply-outliner-ops`, which validates each op against the same schema the app uses. Rename the page you pulled in step 3 by saving it back with a new title:

```js
import { kw } from "./client.mjs";

const result = await client.applyOutlinerOps([
  [kw("save-block"), { "block/uuid": uuid, "block/title": page["block/title"] + " ✎" }, {}],
]);
console.log(result);
```

**What you should see:** the write succeeds, the page title in Logseq changes immediately, and — if your subscription from step 4 is still open — a `sync-db-changes` event for your own transaction arrives too.

Now send something the schema rejects:

```js
await client.applyOutlinerOps([[kw("save-block"), "not a block", {}]]);
```

**What you should see:** a `WorkerError` with `code: "validation-error"`. The worker refused it before touching the database. This is why your UI must never call `thread-api/transact` directly — that path skips validation and semantic undo entirely.

Undo your rename:

```js
await client.undo();
```

## Step 6 — Generate the reference for your revision

You have used five methods. The worker has 163. Do not discover the rest by reading source — generate the reference:

```bash
bb dz/support-engineering/script/thread-api-docs/gen_thread_api_docs.clj \
   --root /path/to/logseq/checkout \
   --out ddocs/reference/api/thread-api.generated.md
```

**What you should see:** a one-line summary on stderr —

```
thread-api: 163 methods, 148 undocumented, 1 drift finding(s), 17 events, 30 outliner ops
```

— and a Markdown file with every method, its parameter vector, whether it is repo-bound, how many places the renderer and CLI call it, the SSE event catalogue, and the outliner op table.

Read the **Drift findings** section first. On master it lists `:thread-api/get-version`: allow-listed but never defined, so calling it fails. That is exactly the kind of thing you want a generator to tell you rather than discover at runtime.

Then run it in check mode:

```bash
bb dz/support-engineering/script/thread-api-docs/gen_thread_api_docs.clj \
   --root /path/to/logseq/checkout --out ddocs/reference/api/thread-api.generated.md --check
```

**What you should see:** `thread-api docs are current`, exit 0. When you later move to a new revision and the API has changed, this exits 1 and tells you to regenerate. Wire it into whatever CI you have.

## Step 7 — Write a note for what you verified

The generator cannot say what a method is *for*. Open `dz/support-engineering/script/thread-api-docs/thread-api-notes.edn` and add an entry for one method you used today:

```clojure
:thread-api/get-journal-page-by-day
{:doc "Look up the journal page entity for a day: [repo yyyymmdd]."
 :stability :internal}
```

Regenerate. **What you should see:** your note in the method's row, and "With hand-written notes" incremented. If you ever document a method that is later removed, `--check` fails with `notes-for-undefined-method` — notes cannot rot silently either.

## What you learned

You discovered a worker, pinned its revision, read with `q` and `pull`, received the app's own edits over SSE, made a validated write and an invalid one, undid it, and generated a complete method reference gated against drift. Those seven things are the whole foundation of a UI on this API; everything else is rendering.

## Next steps

- [db-worker HTTP API](../../reference/api/db-worker-http-api.md) — transport, error codes, and the versioning situation in full
- [Generated thread-api reference](../../reference/api/thread-api.generated.md) — start with `get-render-snapshots` for page-level view data
- Investigation 024 (`dz/docs/07-investigations/`) — the pros and cons that led here, with file-level evidence
