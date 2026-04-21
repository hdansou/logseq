# ADR 0016: Hybrid Asset Reconciliation for DB Sync

Date: 2026-04-16
Status: Proposed

## Context

DB sync uploads assets via an op queue (`client_ops` rows with `kind='asset'`)
drained by `process-asset-ops!`. Each successful PUT stamps the asset entity
with `:logseq.property.asset/remote-metadata {:checksum ... :type ...}`, which
is the client's source of truth for "this asset is on the server".

This leaves two divergence cases that current sync cannot self-heal:

1. **Missed upload.** A sync session caught up block txs but died before the
   asset queue drained (network loss, tab close, worker crash). The asset
   entity exists, `remote-metadata` is nil, and no `:update-asset` op remains
   in `client_ops` — so the next connect has nothing to retry. Result: file on
   disk, nothing on the server, block references a 404 URL.
2. **Server-side loss.** `remote-metadata` claims the asset was uploaded, but
   the object is actually gone from the remote (bucket lifecycle mishap,
   partial restore, cross-region copy miss). The client will never retry
   because its local bookkeeping says "done".

Today there is no HEAD/exists check anywhere in the worker sync code, and the
only bulk-requeue path (`client-op/add-all-exists-asset-as-ops`, used by
`upload-graph!`) unconditionally re-uploads every asset — wasteful when run
just to cover the two cases above.

## Decision

1. Introduce an explicit reconcile pass that walks DB-tracked assets, confirms
   their remote presence, and feeds only the missing ones back into the
   existing upload queue. Reuse `process-asset-ops!` as the uploader; do not
   add a second upload path.
2. Use a **hybrid policy** that avoids HEAD requests when the local DB already
   proves an asset is unsynced:
   - Asset entities with no `:logseq.property.asset/remote-metadata` are
     enqueued for upload directly. No HEAD request.
   - Asset entities with `remote-metadata` get a HEAD against
     `{http-base}/assets/{graph-id}/{asset-uuid}.{asset-type}`. On 404, the
     client clears `remote-metadata` (`:persist-op? true`) and enqueues a
     `:update-asset` op. On 2xx, no action. On any other status or network
     error, skip the entity and log — do not treat ambiguous failures as
     remote loss.
3. Reconcile scope is limited to asset entities present in DataScript
   (datoms on `:avet :logseq.property.asset/type`). Orphan files on disk
   with no DB entity are out of scope — they are unreferenced and
   reintroducing them would be speculative.
4. Reconcile is a user-triggered action from sync settings
   ("Verify & upload missing assets"), not an automatic step of every
   WS connect. Auto-run is a follow-up (see Out of Scope).
5. Implementation location:
   - New ns `frontend.worker.sync.asset-reconcile` owns the walk and HEAD
     policy, depending on `frontend.worker.sync.auth`,
     `frontend.worker.sync.large-title` (URL builder),
     `frontend.worker.sync.assets` (to trigger `enqueue-asset-sync!` after
     enqueuing), and `frontend.worker.sync.client-op` (to write ops).
   - New thread-api endpoint `:thread-api/rtc-reconcile-assets` exposes the
     reconcile pass to the main thread; the UI button invokes it via
     `state/<invoke-db-worker`.
   - HEAD requests issue from the worker, reusing
     `sync-auth/auth-headers` and `sync-auth/http-base-url` for parity with
     the existing upload/download paths.
6. HEAD concurrency is bounded to the same parallelism used by
   `process-asset-ops!` (currently 10) to avoid a separate tuning knob and
   keep server load comparable to a normal asset drain.
7. Progress is reported through the existing
   `:rtc/asset-upload-download-progress` state so the sync UI can render a
   single progress surface for both reconcile-driven re-uploads and regular
   uploads.
8. Cancellation: if reconcile is interrupted mid-walk, any
   `remote-metadata` clears already committed stand (they are correct), and
   the op queue drains normally on the next connect.

## Consequences

### Positive

- Fixes missed-upload without touching the hot-path sync flow — the queue and
  uploader remain unchanged.
- Fixes server-side loss, which no current code path catches.
- HEAD cost scales only with the number of assets that _claim_ to be uploaded,
  not with every asset every pass. Graphs that have never synced an asset
  issue zero HEADs.
- Reuses the existing upload pipeline, progress channel, encryption path, and
  auth — no new surface area for asset encryption or credentials.

### Tradeoffs

- For each asset with `remote-metadata`, one extra HEAD round-trip per
  reconcile run. Batched and bounded, but not free.
- Clearing `remote-metadata` on a 404 commits a persisted tx, which costs one
  DataScript write per confirmed-missing asset. Acceptable because these cases
  are expected to be rare.
- HEAD against the assets host relies on the server returning 404 (not 403)
  for missing objects. If the bucket policy ever returns 403 for absent keys,
  the reconcile pass will silently skip those assets. Policy assumption is
  documented here and must be preserved server-side.
- Ambiguous statuses (5xx, network errors) are intentionally treated as "don't
  know" rather than "missing" — this avoids spurious re-uploads at the cost of
  requiring another reconcile run to catch real misses once the server
  recovers.

## Safety and Integrity Rules

1. Reconcile never deletes local files and never deletes remote objects. It
   only adds PUT work.
2. `remote-metadata` is cleared only on an explicit 404 from the HEAD request.
   Any other status or error leaves the entity untouched.
3. Reconcile must not run while `upload-graph!` is in flight for the same
   repo — full-graph upload already enqueues all assets via
   `add-all-exists-asset-as-ops`, so a concurrent reconcile is pure waste.
   Guard with the existing upload inflight state.
4. HEAD requests use the same bearer token flow as upload/download
   (`sync-auth/<resolve-ws-token` semantics) and must refresh on expiry rather
   than failing the whole pass.
5. Reconcile respects `max-asset-size` (100 MB). Oversized entities are
   skipped the same way `process-asset-op!` skips them today — reconcile must
   not resurrect an op that the uploader would immediately drop.
6. E2EE graphs: reconcile does not decrypt or re-encrypt anything. It only
   probes object existence and enqueues ops; the existing upload path handles
   encryption on the re-upload.

## Out of Scope

- Orphan files on disk with no DataScript entity. Handling these requires
  deciding whether to drop them or construct new asset entities, which is a
  separate policy decision.
- Automatic reconcile on WS connect. A heuristic like "run once if there are
  local assets without `remote-metadata` and zero pending asset ops" is
  attractive but warrants its own ADR — it changes startup-time network
  behavior for every user.
- Server-side reconciliation (e.g., a server endpoint that returns the set of
  present asset keys in one call). That would obsolete per-asset HEADs but
  requires a server change and is out of scope for this client-only ADR.

## Verification

New worker tests covering the three branches of the walk:

```bash
bb dev:test -v frontend.worker.asset-reconcile-test/enqueues-assets-missing-remote-metadata-test
bb dev:test -v frontend.worker.asset-reconcile-test/head-404-clears-remote-metadata-and-enqueues-test
bb dev:test -v frontend.worker.asset-reconcile-test/head-200-is-noop-test
bb dev:test -v frontend.worker.asset-reconcile-test/head-5xx-does-not-clear-remote-metadata-test
bb dev:test -v frontend.worker.asset-reconcile-test/oversized-asset-is-skipped-test
```

Additional checks:

- After a reconcile run with mixed state (some missing remote-metadata, some
  present-and-live, some present-and-404), the resulting `client_ops` rows
  match exactly the subset that should be re-uploaded.
- HEAD request count equals the count of assets with `remote-metadata`, not
  the total asset count.
- Running reconcile twice back-to-back on the same graph issues uploads only
  on the first pass; the second pass is a no-op once the queue drains.
