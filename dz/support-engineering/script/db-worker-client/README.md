# db-worker-client

Minimal client adapter for the Logseq `db-worker-node` HTTP + SSE API — the same transport the desktop renderer uses (`src/main/frontend/persist_db/remote.cljs`).

```
npm install
node smoke.mjs                  # discover a worker via ~/logseq/server-list, run read-only checks
SMOKE_WRITE=1 node smoke.mjs    # also perform one write and wait for the :sync-db-changes event
node smoke.mjs http://127.0.0.1:PORT
```

| File | Purpose |
|---|---|
| `client.mjs` | `discover`, `health`, `connect` (with revision check), `createClient` (`q`, `pull`, `searchBlocks`, `renderSnapshots`, `applyOutlinerOps`, `undo`, `redo`, `subscribe`), transit encode/decode |
| `edn.mjs` | Minimal EDN reader so queries and ops can be written as literals |
| `smoke.mjs` | End-to-end check; exits 0/1/2 |

Rules the adapter enforces so callers don't have to remember them:

- arguments always go as `argsTransit` (plain-JSON `args` silently turns keyword keys into inert strings)
- results are decoded to plain JS: keyword keys → `"ns/name"` strings, UUIDs → strings, sets/lists → arrays
- `connect({ expectedRevision })` refuses a worker built from a different revision — the thread-api is not versioned, so this is the only compatibility signal

Tutorial: `ddocs/tutorials/integrations/build-a-db-worker-ui.md`. Reference: `ddocs/reference/api/db-worker-http-api.md`.
