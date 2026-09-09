# db-worker-node: Comprehensive Guide

## Executive Summary

db-worker-node is the Node.js daemon that hosts Logseq's DB worker runtime outside the UI process. It gives Desktop and CLI a shared, local process boundary for graph DB operations, exposed via a small HTTP/SSE API.

At a high level, it provides:

- A local daemon per graph/repo with explicit lifecycle
- HTTP invoke endpoints for thread-api calls
- Server-sent events for worker-emitted events
- Lock ownership and write-guard enforcement for graph safety
- Health metadata for discovery, ownership, and revision compatibility
- Shared orchestration for both Logseq CLI and Electron Desktop

## What It Is

db-worker-node is not the cloud sync backend. It is a local runtime process that runs worker thread APIs against a local graph database for Desktop/CLI flows.

Two layers work together:

1. Runtime daemon

- Process entrypoint starts worker core and opens/binds the graph DB
- Exposes local HTTP endpoints (`/healthz`, `/v1/invoke`, `/v1/import-db-binary`, `/v1/events`, `/v1/shutdown`)
- Streams worker events over SSE

2. Lifecycle orchestration

- Discovers and reuses per-graph daemons when safe
- Verifies ownership and revision compatibility
- Restarts mismatched runtimes when needed
- Publishes process info into a server list and graph lock files

## Why It Exists

Desktop and CLI need reliable local DB execution without putting heavy DB work in the renderer process. db-worker-node centralizes that execution with:

- process isolation
- predictable API surface
- graph-level lock/ownership control
- consistent behavior across Desktop and CLI

## Core Runtime Architecture

## Daemon entrypoint

Primary runtime implementation:

- `src/main/frontend/worker/db_worker_node.cljs`

Key startup flow:

1. Parse args (`--root-dir`, `--repo`, `--owner-source`, `--create-empty-db`, etc.)
2. Install file logger under graph dir with retention
3. Build Node platform and init worker core
4. Call `:thread-api/init`
5. Acquire graph lock (owner source tagged)
6. Open/create DB with `:thread-api/create-or-open-db`
7. Start local HTTP server and publish health/list entry

## Exposed local API

- `GET /healthz`
  - returns repo, status, host, port, pid, owner-source, root-dir, revision

- `GET /v1/events`
  - SSE stream of worker events

- `POST /v1/invoke`
  - generic thread-api invocation via transit payload

- `POST /v1/import-db-binary`
  - binary import endpoint for DB binary payloads

- `POST /v1/shutdown`
  - graceful daemon shutdown

## Invocation model

Client sends `method` + transit encoded args. Runtime:

- normalizes method name
- validates repo binding for repo-scoped methods
- enforces lock ownership for write methods
- invokes worker proxy (`remoteInvoke` or `remoteInvokeBinary`)
- returns transit-encoded result or structured error

## Locking and Safety

Graph safety relies on lock metadata and owner checks.

Key points:

- Daemon acquires graph lock on startup
- Writes require lock-owner validation
- Repo mismatch returns explicit conflict errors
- Lock can be recreated/updated to maintain ownership freshness

Write-guarded operations include transaction and import/export style methods (for example `:thread-api/transact`, `:thread-api/import-db-binary`, `:thread-api/backup-db-sqlite`).

## Lifecycle Orchestration (CLI + Desktop)

Shared orchestration lives in:

- `src/main/logseq/cli/server.cljs`

What it does:

- Resolve graph root and per-repo directories
- Discover active daemons from server-list + `/healthz`
- Ensure server started for target graph
- Validate owner-source compatibility
- Detect revision mismatch and restart server
- Provide base URL to callers

This orchestration is used by:

- CLI command execution paths
- Electron DB worker manager via `ensure-server!`

## Desktop Integration

Desktop integration manager:

- `src/electron/electron/db_worker.cljs`

Behavior:

- Starts managed runtime through shared CLI server orchestration with `:owner-source :electron`
- Tracks runtime per window/repo
- Reuses healthy runtime when possible
- Stops runtime when last window detaches or repo is released

## CLI Integration

CLI transport and execution paths:

- `src/main/logseq/cli/transport.cljs`
- `src/main/logseq/cli/main.cljs`
- `src/main/logseq/cli/commands.cljs`

Behavior:

- CLI command builder resolves action
- Graph-scoped actions ensure daemon via shared server orchestrator
- Transport invokes worker methods over `/v1/invoke`
- CLI may subscribe to `/v1/events` for streamed updates

## What It Can Be Used For

1. Local graph data operations for CLI commands

- list/show/query/upsert/remove/graph workflows using DB thread APIs

2. Desktop DB graph runtime hosting

- offloads DB-heavy worker operations from renderer process

3. Shared backup/import/export operations

- binary import and snapshot/backup thread APIs in one local runtime

4. Runtime health and diagnostics

- health endpoint, structured invoke errors, per-graph logs

5. Safe multi-consumer orchestration

- owner-source semantics plus lock guards to avoid unsafe cross-owner writes

## What It Integrates With

## Internal runtime integrations

- Worker core and thread-api registry
- Node platform abstraction for file/system operations
- Graph lock system (`db-worker-node-lock`)
- Server list registry (`db-worker.server-list`)

## Product integrations

- Logseq CLI v2 command system
- Electron Desktop DB runtime manager
- Renderer remote persistent DB flows through IPC -> runtime ensure

## Storage and filesystem integrations

- Graph root directory (`~/logseq` default)
- Per-graph repo directories
- Log files (`db-worker-node-YYYYMMDD.log`, retention 7)

## Operational integrations

- Health-based runtime discovery
- Revision/version compatibility checks and restart policy
- Graceful shutdown hooks for SIGINT/SIGTERM

## Security and Trust Boundary

db-worker-node is a local trusted-process API surface intended for same-machine product components. Core safeguards include:

- repo-bound method validation
- lock-owner enforcement for writes
- owner-source checks in orchestrator
- explicit refusal on repo mismatch / missing repo

It is not designed as a public network service.

## Operational Behavior Notes

- Daemon binds host `127.0.0.1` with dynamic port by default.
- Health status reports `starting` vs `ready`.
- Invoke timeout logging exists for slow calls.
- Server list entry is appended on ready and removed on shutdown.
- Lock is cleaned up during shutdown/error paths.

## Known Boundaries

- API is intentionally narrow and local-process oriented.
- Correct behavior depends on lock + server-list consistency.
- Compatibility is revision-sensitive; mismatches are treated as restart/fail-fast conditions.

## Practical Integration Checklist

1. Ensure runtime bundle exists (`dist/db-worker-node.js`).
2. Ensure root dir and graph repo dir are readable/writable.
3. Start via orchestrator (`ensure-server!`) rather than manual ad hoc process management.
4. Verify `/healthz` and revision alignment.
5. Invoke thread APIs through CLI transport contract.
6. For write operations, ensure lock ownership is intact.
7. Verify logs under graph directory for runtime diagnostics.

## Source Map

Primary code references:

- `src/main/frontend/worker/db_worker_node.cljs`
- `src/main/logseq/cli/server.cljs`
- `src/main/logseq/cli/transport.cljs`
- `src/main/logseq/cli/main.cljs`
- `src/main/logseq/cli/commands.cljs`
- `src/electron/electron/db_worker.cljs`

Related docs:

- `docs/cli/logseq-cli-v2.md`
- `docs/agent-guide/db-worker/001-db-worker-node-restart-on-version-mismatch.md`
- `docs/agent-guide/db-worker/002-desktop-db-worker-request-cap-switch-graph.md`
- `docs/agent-guide/db-worker/005-unified-graph-backup.md`
