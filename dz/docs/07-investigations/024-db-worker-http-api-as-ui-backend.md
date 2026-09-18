# db-worker HTTP API as a UI Backend Investigation

> **Last revised:** 2026-09-08
> **Verified against:** `origin/master` @ `d2ab772` (2026-09-02)

## Question

Can a new UI use `POST /v1/invoke` on db-worker-node as its backend? What are the pros and cons, is the thread-api versioned, and can the API surface be documented mechanically instead of by hand?

## Answer

**Yes, and it is less exotic than it sounds:** `/v1/invoke` + `/v1/events` is the Electron renderer's own production transport (`src/main/frontend/persist_db/remote.cljs`). A second frontend does what Logseq's frontend does, over the same wire, and can share the app's live worker for the same graph.

**The thread-api is not versioned.** The only compatibility signal is the build `revision` from `/healthz`, and the only enforcement is CLI-side exact-match. Pin a revision.

**The surface can be generated.** All 163 methods are declared by one macro; a babashka script now extracts them, joins hand-written notes, and fails `--check` on drift. It found a phantom method on its first run.

## Findings

### It is the production transport

`frontend.persist-db.remote` is a `PersistentDB` implementation over HTTP + SSE against db-worker-node. `persist_db.cljs:247` starts it; `frontend/handler/worker.cljs` dispatches its events. The renderer calls **165 distinct** thread-api keywords (`:thread-api/pull` alone from 49 sites).

### Reactivity works for a second client

`db_listener.cljs:136` broadcasts every committed transaction as `:sync-db-changes` via `shared-service/broadcast-to-clients!`. On the node runtime that becomes `platform/post-message!` → `db_worker_node.cljs handle-event!` → a write to **every** connected SSE response (`*sse-clients` is a set). Confirmed end-to-end with the client adapter against a stub that mimics the worker's `[type payload]` transit envelope.

The renderer-facing event contract is the `defmethod handle` set in `frontend/handler/worker.cljs` — 17 event types on master (`:sync-db-changes`, `:notification`, `:capture-error`, `:rtc-sync-state`, `:sync-conflicts-updated`, `:remote-graph-gone`, `:db-worker/ui-request`, `:thread-api/search-index-build-progress`, …).

### The app's live worker can be shared

`cli/lib/server_runtime.ml start_server_unprofiled`: if `find_repo_server` returns a `Ready` server for the repo it is used regardless of `owner_source`; electron-owned servers are marked `owned = false` and `owner_manageable` returns false for them, so the CLI never kills them. A UI following the same policy edits the same graph through the same process, with the worker serializing writes.

### Writes go through a validated op vocabulary

`:thread-api/apply-outliner-ops [repo ops opts]` validates against the malli `op-schema` in `deps/outliner/src/logseq/outliner/op.cljs` — 30 ops (`:save-block`, `:insert-blocks`, `:move-blocks`, `:indent-outdent-blocks`, `:create-page`, `:upsert-property`, …), 13 of them "semantic" for undo/sync. Undo/redo are worker methods too (`handler/undo_redo.cljs`). A UI should never call `:thread-api/transact` directly.

### There is a view-model read API

`:thread-api/get-render-snapshots [repo {:blocks :children :resources}]` → `{:basis-rev :slots :groups}`. Resource kinds in `handler/render_resource/basic.cljs`: `:journals`, `:favorites`, `:recent-pages`, `:page-identity`, `:block-breadcrumb`, `:block-ref-count`, `:block-comment-threads`, `:block-task-time`, `:route-block`, … `:basis-rev` is usable for cache invalidation against `:sync-db-changes`.

### Cons, each verified

| Con | Evidence |
|---|---|
| No authentication | `db_worker_node.cljs` never reads `Authorization`; it appears only in CORS allow-headers |
| No method allowlist | `/v1/invoke` dispatches any registered keyword, including `:thread-api/unsafe-unlink-db` |
| Browser tabs are blocked | `Access-Control-Allow-Origin: lsp://logseq.com` (`db_worker_node.cljs:33`) — the main window origin |
| Plain-JSON `args` silently breaks options | `<invoke!` re-encodes `args` verbatim; keyword keys never materialise. Use `argsTransit`. |
| The worker does not render | `:block/title` is markdown-ish text with `[[uuid]]` refs; parsing and the editor are renderer-side |
| Large, internal surface | 163 methods; `db_core.cljs` had a 1,313-line diff between June and September; handlers moved into `worker/handler/*` |
| One graph per worker | Multi-graph UIs need one connection per worker, discovered via `server-list` + `/healthz` |

### Versioning

- `def-thread-api` (`common/thread_api.cljc`) registers `keyword → fn` in a volatile map. No docstring slot, no version, no deprecation.
- `/v1/` is the only protocol marker. No `/v2`, no negotiation (`git grep` for `protocol-version`/`api-version` is empty).
- `:thread-api/get-version` is in `non-repo-methods` but **has no definition** anywhere. Invoking it returns `No method found`.
- `/healthz.revision` = `logseq.common.version/REVISION`, a `goog-define` set at build time; `"dev"` in dev builds.
- Enforcement exists only in the CLI: `cli/server.cljs:106` and `cli/lib/server_runtime.ml:370` compare revisions exactly and restart mismatched workers. Electron does not (`dz/docs/10-agent-guide/db-worker/001-*.md` is a plan, CLI-only so far).

### Generating the reference instead of writing it

Every method is declared by the same macro with a uniform shape, so the reference is extractable:

```
(def-thread-api :thread-api/NAME [params] body...)
```

`dz/support-engineering/script/thread-api-docs/gen_thread_api_docs.clj` (babashka; edamame parses the sources) produces `ddocs/reference/api/thread-api.generated.md` with: every method, params, file, repo-bound flag (from `non-repo-methods`), renderer and CLI call-site counts (cljs keywords and OCaml `"thread-api/…"` strings), the SSE event catalogue (emit sites × renderer handlers), and the outliner op table (from the malli schema). Hand-written prose lives in `thread-api-notes.edn`, joined by method name.

Why not clj-kondo's analysis export: the repo's kondo hook expands `def-thread-api` to an anonymous `fn`, so kondo never sees a named var to report.

`--check` regenerates and diffs, ignoring only the revision stamp, and exits 1 with the regenerate command. Verified by breaking it three ways: appending a fake row (STALE, exit 1), a notes entry for a nonexistent method (drift finding), and a revision-only change (passes).

**First-run findings**, which is the argument for the gate:

- `:thread-api/get-version` — allow-listed, never defined.
- 38 methods with zero renderer and zero CLI call sites — candidates for dead code or driven by paths the scan does not cover.
- The earlier hand-count of "165 methods" was two too high: one is the macro's own docstring example, one a keyword truncated by a naive regex.

## Recommendation

Build the UI in a desktop shell (CORS rules out browser tabs), behind a single adapter module that owns transit, method names, and event decoding. Start read-only (`pull`, `q`, `get-render-snapshots`, SSE), add `apply-outliner-ops` once the adapter is solid, never `transact`. Pin a revision and regenerate the reference when you move it.

Upstream, two small changes would make this materially safer for third parties, and neither is invasive:

1. Give `def-thread-api` an optional docstring slot. The generator already reads one if present.
2. Adopt the `--check` gate in CI so the reference cannot rot, and delete `:thread-api/get-version` from `non-repo-methods` or define it.

## Open questions

- Does a plugin iframe share the `lsp://logseq.com` origin? Untested; determines whether plugins can reach the worker directly.
- Which of the 38 never-called methods are genuinely dead versus driven by RTC/sync internals the scan does not attribute?

## Related

- [db-worker HTTP API](../ddocs/reference/api/db-worker-http-api.md)
- [Generated thread-api reference](../ddocs/reference/api/thread-api.generated.md)
- [Build a UI on the db-worker API](../ddocs/tutorials/integrations/build-a-db-worker-ui.md)
- Client adapter: `dz/support-engineering/script/db-worker-client/`
- Generator: `dz/support-engineering/script/thread-api-docs/`
- [023 — Graph Vector Embeddings for Agentic Work](023-graph-vector-embeddings-for-agents.md)
