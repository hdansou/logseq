# DB-Sync Worker: Comprehensive Guide

## Executive Summary

The DB-sync worker is Logseq's synchronization backend for DB graphs. It provides:

- Authentication and graph access control
- Graph lifecycle APIs (create, list, members, delete)
- Real-time sync over WebSocket
- HTTP sync fallback and bootstrap flows
- Snapshot upload/download for initial graph hydration
- Graph asset storage and retrieval
- End-to-end encryption key management endpoints

In production, this service runs as a Cloudflare Worker with Durable Objects and D1. In self-hosted mode, the same protocol is served by a Node adapter that reuses the same core handler logic.

## Case File: What It Is

Think of the DB-sync worker as two layers:

1. Edge router and identity gate
   - Authenticates requests and enforces graph access
   - Routes requests to index handlers, asset handlers, or per-graph sync runtimes

2. Per-graph sync runtime
   - Maintains graph state, tx logs, and checksums
   - Accepts tx batches, resolves stale writes, and serves pull streams
   - Handles snapshot import/export and live collaboration presence

The Cloudflare version uses Durable Objects for per-graph isolation. The Node adapter uses an in-process graph runtime registry to provide equivalent behavior.

## Why It Exists

DB graphs need more than a plain REST API:

- Real-time multi-user sync requires push plus pull semantics
- Concurrency needs monotonic ordering and stale-write rejection
- Graph bootstrap must move large snapshots efficiently
- Collaboration features require presence and membership controls
- Encryption keys need graph-scoped, role-aware management

The worker consolidates these concerns into a single protocol boundary for desktop/web/mobile DB graph clients.

## Architecture

## 1) Cloudflare Runtime

- Entry worker exports a fetch handler and a Durable Object class
- Global routes handle health, graphs, e2ee, assets, and sync path forwarding
- Sync paths are forwarded to graph-scoped Durable Object instances

Key behavior:

- `GET /health` returns worker health
- `/graphs*` and `/e2ee*` are handled against index storage
- `/sync/:graph-id/*` is routed to graph Durable Object runtime
- `/assets/*` is validated and served through configured asset bucket

## 2) Node Adapter Runtime

- Exposes the same route surface (`/health`, `/graphs`, `/e2ee`, `/sync`, `/assets`)
- Reuses worker handlers for consistency
- Uses local sqlite/filesystem drivers by default
- Manages graph runtimes in-process instead of Durable Objects

This allows local development, self-hosting, and operational workflows without Cloudflare.

## Protocol Surfaces

## HTTP APIs

Primary endpoint families:

- Worker health: `/health`
- Graph index and membership: `/graphs`, `/graphs/:graph-id/*`
- E2EE key endpoints: `/e2ee/*`
- Sync HTTP APIs: `/sync/:graph-id/pull`, `/sync/:graph-id/tx/batch`, snapshot endpoints
- Asset APIs: `/assets/:graph-id/:asset-uuid.:ext`

The canonical protocol reference is:

- `docs/agent-guide/db-sync/protocol.md`

## WebSocket APIs

WebSocket sync endpoint:

- `/sync/:graph-id`

Client messages include:

- `hello`
- `pull`
- `tx/batch`
- `presence`
- `ping`

Server messages include:

- `hello`
- `pull/ok`
- `tx/batch/ok`
- `tx/reject`
- `changed`
- `online-users`
- `pong`

## Data and Consistency Model

The sync runtime maintains:

- Graph kv rows
- Transaction log
- Monotonic sync position (`t`)
- Graph checksum
- Sync metadata flags (including snapshot upload state)

Notable consistency controls:

- `t-before` validation for tx batches
- Reject path for stale or invalid txs
- Checksum propagation in pull and tx acknowledgements
- Readiness gating when snapshot upload/import is in progress

## Security Model

Authentication and authorization are enforced at route boundaries.

- JWT-based identity claims for user resolution
- Access checks before sync and asset operations
- Manager role checks for member management and key grant operations
- Optional admin token for operator-only flows (graph deletion and bypass operations)

This gives three practical control planes:

- Normal user access
- Graph manager actions
- Admin/operator override

## What It Can Be Used For

## 1) Real-time collaborative graph editing

- Multi-client live sync with server-side ordering and conflict detection
- Presence broadcasts for online users and editing focus

## 2) DB graph lifecycle management

- Create/list/delete graphs
- Manage graph members and roles
- Check graph access and readiness

## 3) Initial data bootstrap and migration

- Upload initial snapshot stream
- Download snapshots for restore and transfer workflows
- Gate sync until graph is marked ready

## 4) End-to-end encryption key coordination

- Store per-user RSA key pair metadata
- Store and distribute per-graph encrypted AES keys
- Grant key access to members (manager-only)

## 5) Asset pipeline for DB graphs

- Upload and serve binary assets with metadata
- Enforce per-graph access checks
- Keep graph assets colocated with sync backend policy

## 6) Operational support and observability

- Health checks and route-level diagnostics
- Sentry integration in worker runtime
- Usage reporting scripts and admin scripts in `deps/db-sync`

## What It Integrates With

## Client integrations

- Logseq DB sync client runtime in frontend worker stack
- Logseq desktop/mobile/web DB graph flows
- Any client that speaks the documented JSON + Transit protocol

## Infrastructure integrations

- Cloudflare Worker runtime
- Cloudflare Durable Objects for graph isolation
- Cloudflare D1 for graph/user/index metadata
- R2-compatible asset bucket binding

## Identity integrations

- Cognito issuer/client/jwks configuration
- JWT claim extraction for user identity and access checks

## Self-hosted integrations

- Node.js HTTP + WebSocket server
- Local sqlite index and graph storage
- Filesystem asset storage
- Environment-driven deployment config

## Operator tooling integrations

- Admin token protected routes
- CLI scripts for usage stats, graph lookup, and cleanup
- Local and CI migration flows for D1 schema

## Deployment Patterns

## Cloudflare (managed edge)

Use when you need:

- Durable Object graph isolation
- Tight integration with D1 and Worker infrastructure
- Global edge entrypoint behavior

## Node adapter (self-hosted)

Use when you need:

- Local dev without Cloudflare dependencies
- Self-managed hosting footprint
- Reuse of existing Node infrastructure

Both keep protocol compatibility by sharing core handler logic.

## Operational Notes

- Snapshot stream gzip can be enabled/disabled by runtime env; Node defaults it off for adapter/client compatibility in this repository.
- Graph readiness is enforced to avoid serving partially imported bootstrap state.
- Access checks happen before sync and asset actions, except explicit admin-token override paths.

## Known Boundaries

- Current Node adapter config supports sqlite storage and filesystem assets.
- Protocol changes require synchronized updates across client, server handlers, and protocol documentation.
- Admin token grants broad powers and should be rotated and tightly scoped operationally.

## Practical Integration Checklist

1. Pick runtime: Cloudflare Worker or Node adapter.
2. Configure identity inputs (issuer, client id, jwks URL).
3. Configure storage and asset backends.
4. Apply schema migrations before traffic.
5. Validate `/health`, `/graphs`, and one `/sync/:graph-id/*` path.
6. Validate asset upload/download and access control behavior.
7. Validate snapshot upload and readiness transitions.
8. Add monitoring and alerting for route failures and sync rejects.

## Source Map

Primary implementation references:

- `deps/db-sync/src/logseq/db_sync/worker.cljs`
- `deps/db-sync/src/logseq/db_sync/worker/dispatch.cljs`
- `deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs`
- `deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs`
- `deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs`
- `deps/db-sync/src/logseq/db_sync/node/server.cljs`
- `deps/db-sync/src/logseq/db_sync/node/dispatch.cljs`
- `deps/db-sync/src/logseq/db_sync/node/config.cljs`

Reference docs:

- `docs/agent-guide/db-sync/protocol.md`
- `docs/agent-guide/db-sync/db-sync-guide.md`
- `deps/db-sync/README.md`
