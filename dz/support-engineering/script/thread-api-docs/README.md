# thread-api-docs

Generates `ddocs/reference/api/thread-api.generated.md` from the `def-thread-api` declarations in a Logseq checkout, and gates it against drift.

```
bb gen_thread_api_docs.clj --root <logseq-checkout> --out ddocs/reference/api/thread-api.generated.md
bb gen_thread_api_docs.clj --root <logseq-checkout> --out <same> --check    # exit 1 if stale
bb gen_thread_api_docs.clj --root <logseq-checkout> --edn                    # machine-readable model
```

What is extracted, and from where:

| Section | Source of truth |
|---|---|
| Methods, params, file, line | every `(def-thread-api :thread-api/NAME [params] ...)` under `src/main` and `deps` |
| Repo-bound flag | `non-repo-methods` in `src/main/frontend/worker/db_worker_node.cljs` |
| Renderer / CLI call-site counts | `:thread-api/NAME` in `src/main/frontend` (excluding the worker); `"thread-api/NAME"` in `cli/lib/*.ml` and `src/main/logseq/cli` |
| SSE event catalogue | `broadcast-to-clients!` / `post-message!` emit sites in the worker × `defmethod handle` in `frontend/handler/worker.cljs` |
| Outliner ops | the malli `op-schema` in `deps/outliner/src/logseq/outliner/op.cljs`; "semantic" from `op/construct.cljc` |
| Prose | `thread-api-notes.edn` (hand-written, joined by method name) |

Drift findings are printed and embedded in the document: methods allow-listed but undefined, methods called but undefined, notes for methods that no longer exist.

`--check` ignores only the revision stamp, so a rebuild at a new commit with no API change passes. Verified by breaking it: a fake row → STALE (exit 1); a note for a nonexistent method → drift finding; a revision-only change → pass.

Requires babashka (edamame is bundled). Not clj-kondo: the repo's kondo hook expands `def-thread-api` to an anonymous `fn`, so kondo's analysis never sees a named var.
