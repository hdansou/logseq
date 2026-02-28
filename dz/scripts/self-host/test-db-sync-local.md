# Testing DB Sync with Node.js Adapter (Self-Host)

This guide covers local self-host testing of Logseq db-sync using the **Node.js adapter** (not Cloudflare wrangler).

## Quick Summary

**Q: How to load a graph from `http://localhost:8787/graphs?token=dev-token` in the Logseq app at `http://localhost:3001/#/graphs`?**

**A:** The Logseq app automatically connects to the local server when `ENABLE_DB_SYNC_LOCAL=true` is set. The `?token=dev-token` parameter is for backend server authentication (not client app). The app uses its standard Cognito/JWT authentication flow and sends tokens via `Authorization: Bearer <token>` headers.

**Q: How to verify if `ENABLE_DB_SYNC_LOCAL` is enabled?**

**A:** Check in browser console:

```javascript
frontend.config.db_sync_local_QMARK_ // Should return: true
frontend.config.db_sync_ws_url // Should return: "ws://127.0.0.1:8787/sync/%s"
frontend.config.db_sync_http_base // Should return: "http://127.0.0.1:8787"
```

Or check the Network tab for WebSocket/HTTP requests to `127.0.0.1:8787` instead of production URLs.

## Prerequisites

- Node.js installed
- Clojure (for shadow-cljs builds)
- Dependencies installed (`yarn install` at repo root, `yarn install` in `deps/db-sync`)

## Step 1 — Build and start the Node.js sync server

```bash
cd deps/db-sync
npm run build:node-adapter
./start-server.sh          # auth=none, accepts all requests
```

### Auth driver options

| Script            | Auth                         | Use case                                    |
| ----------------- | ---------------------------- | ------------------------------------------- |
| `start-server.sh` | `DB_SYNC_AUTH_DRIVER=none`   | Easiest for local dev — no token validation |
| `start-local.sh`  | `DB_SYNC_AUTH_DRIVER=static` | Validates against a fixed `dev-token`       |
| `start.sh`        | Cognito (default)            | Validates real Cognito JWTs                 |

For local testing, **`start-server.sh`** (auth=none) is recommended. The server runs on port 8787 by default.

Verify it's running: <http://localhost:8787/graphs>

## Step 2 — Start the Logseq app

```bash
ENABLE_DB_SYNC_LOCAL=true yarn watch
```

Or use the convenience script:

```bash
./start-app-local-sync.sh
```

The `ENABLE_DB_SYNC_LOCAL=true` env var switches the app's sync endpoints from production (`wss://logseq-sync-prod.logseq.workers.dev`) to local (`ws://127.0.0.1:8787`). This is wired via `shadow-cljs.edn` → `frontend.config/ENABLE-DB-SYNC-LOCAL`.

Open the app at: <http://localhost:3001>

## Step 3 — Log in to Logseq

**You must be logged in** with a Logseq account in the app. The graphs page UI only fetches remote graphs when `login?` is true (`(:auth/id-token @state/state)`). Every HTTP call to the sync server goes through `task--ensure-id&access-token`, which requires a valid auth session.

With `DB_SYNC_AUTH_DRIVER=none` on the server, the Cognito token sent by the app is accepted without validation.

## Step 4 — Upload / download graphs

1. Navigate to <http://localhost:3001/#/graphs>
2. Local graphs appear; remote graphs are fetched from `http://127.0.0.1:8787/graphs`
3. To upload a local graph: use the graph menu → "Use Logseq sync"
4. To download a remote graph: click on a remote-only graph in the list

## Troubleshooting

### Schema version mismatch

```txt
Assert failed: {:app {:major 65, :minor 22}, :remote-graph "1"}
```

This error occurs when the `schema_version` stored on the server doesn't match the app's current major version. It happens when graphs were created with an older app version or when the schema version wasn't sent properly.

**Inspect stored versions:**

```bash
sqlite3 deps/db-sync/data/db-sync/index.sqlite \
  "SELECT graph_id, graph_name, schema_version FROM graphs;"
```

**Fix options:**

1. **Update the version** (quick fix for testing):

   ```bash
   sqlite3 deps/db-sync/data/db-sync/index.sqlite \
     "UPDATE graphs SET schema_version = '65' WHERE schema_version = '1';"
   ```

2. **Delete stale graphs** and re-upload:

   ```bash
   sqlite3 deps/db-sync/data/db-sync/index.sqlite \
     "DELETE FROM graph_members WHERE graph_id IN ('<graph-uuid>');"
   sqlite3 deps/db-sync/data/db-sync/index.sqlite \
     "DELETE FROM graphs WHERE schema_version != '65';"
   ```

3. **Wipe all server data** and start fresh:

   ```bash
   rm -rf deps/db-sync/data/db-sync/
   ```

   Then restart the node adapter — it will re-initialize.

### Remote graphs not showing in the app

- Confirm you are **logged in** (Logseq account required)
- Confirm `ENABLE_DB_SYNC_LOCAL=true` was set when `yarn watch` started
- Check that `http://localhost:8787/graphs` returns data in the browser
- Check the browser console for auth or network errors

### Auth token mismatch with `static` driver

If using `start-local.sh` (static auth), the server expects `Authorization: Bearer dev-token`. The app sends a Cognito JWT token instead, so static auth will reject it. Use `start-server.sh` (auth=none) to avoid this.

## Key files

| File                                               | Purpose                                                 |
| -------------------------------------------------- | ------------------------------------------------------- |
| `deps/db-sync/start-server.sh`                     | Start node adapter with auth=none                       |
| `deps/db-sync/start-local.sh`                      | Start node adapter with static token auth               |
| `deps/db-sync/start.sh`                            | Start node adapter with Cognito auth                    |
| `start-app-local-sync.sh`                          | Start app with `ENABLE_DB_SYNC_LOCAL=true`              |
| `src/main/frontend/config.cljs`                    | `db-sync-local?`, `db-sync-ws-url`, `db-sync-http-base` |
| `src/main/frontend/handler/db_based/sync.cljs`     | Client sync HTTP calls, `<get-remote-graphs`            |
| `deps/db-sync/src/logseq/db_sync/worker/auth.cljs` | Server auth drivers (cognito/static/none)               |
| `deps/db-sync/src/logseq/db_sync/index.cljs`       | Server graph index SQLite schema + queries              |
| `deps/db-sync/data/db-sync/`                       | Server data directory (SQLite DBs, assets)              |
