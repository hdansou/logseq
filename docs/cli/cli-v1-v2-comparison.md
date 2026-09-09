# CLI v1 vs CLI v2: Source-Backed Investigation

This document is the comprehensive comparison between Logseq CLI v1 (legacy `deps/cli`) and CLI v2 (new `src/main/logseq/cli`).

Scope note:

- This comparison is based on source code paths in this repository.
- When docs conflict, source code is treated as truth.

## Case Summary

The two CLIs are not just version bumps of the same runtime path.

- CLI v1 is an nbb-bootstrapped command app whose data access can be local SQLite or Desktop HTTP API depending on mode.
- CLI v2 is a compiled Node CLI that routes graph operations through db-worker-node lifecycle orchestration and invoke transport.

In practical terms, CLI v2 introduces a daemon-mediated architecture, while CLI v1 often executes directly against local graph storage.

## Evidence Index

CLI v1 source anchors:

- `deps/cli/src/logseq/cli.cljs`
- `deps/cli/src/logseq/cli/util.cljs`
- `deps/cli/src/logseq/cli/commands/query.cljs`
- `deps/cli/src/logseq/cli/commands/search.cljs`
- `deps/cli/package.json`

CLI v2 source anchors:

- `src/main/logseq/cli/main.cljs`
- `src/main/logseq/cli/commands.cljs`
- `src/main/logseq/cli/server.cljs`
- `src/main/logseq/cli/transport.cljs`
- `src/main/logseq/cli/command/*.cljs`
- root `package.json`
- `shadow-cljs.edn`

## Architecture Comparison

### CLI v1 Architecture

Execution characteristics:

- Entry dispatch is command-table based (`logseq.cli` in `deps/cli/src/logseq/cli.cljs`).
- Uses lazy namespace loading to keep startup lighter.
- Supports two main data paths:
  - Local path: direct SQLite open/query/write flow for many commands.
  - API path: POST requests to Desktop HTTP API at `http://127.0.0.1:12315/api`.

Mode switching:

- `api-command?` in `deps/cli/src/logseq/cli/util.cljs` chooses API mode based on explicit option or env context.

Operational profile:

- Works well as a direct local graph utility.
- Can bridge to the running app through HTTP API for in-app graph operations.

### CLI v2 Architecture

Execution characteristics:

- Uses a parse -> build-action -> execute -> format pipeline (`src/main/logseq/cli/main.cljs`, `src/main/logseq/cli/commands.cljs`).
- Graph operations are worker-mediated:
  - Ensure server (`logseq.cli.server/ensure-server!`).
  - Invoke worker method over HTTP `/v1/invoke` (`logseq.cli.transport/invoke`).

Server lifecycle profile:

- Server ownership and revision semantics are explicit.
- Lock-file and process-state checks are part of standard flow.
- Server controls are first-class (`server list/start/stop/restart/cleanup`).

Operational profile:

- More deterministic around process ownership and graph access boundaries.
- Better aligned with shared daemon runtime behavior used by newer app flows.

## Command Surface Differences

### v1 Command Model

Representative top-level commands from `deps/cli/src/logseq/cli.cljs`:

- `list`
- `show`
- `search`
- `query`
- `export`
- `export-edn`
- `import-edn`
- `append`
- `mcp-server`
- `validate`

### v2 Command Model

Representative action families from `src/main/logseq/cli/commands.cljs`:

- Graph and backup lifecycle (`graph list/create/switch/remove/validate/info/export/import/backup *`)
- Server lifecycle (`server list/cleanup/start/stop/restart`)
- List/search/query/show/debug/doctor
- Upsert/remove/move for nodes/pages/tags/properties/assets/tasks
- Sync and auth command families
- Completion/skill/example utilities

Interpretation:

- v2 is significantly broader in command taxonomy and runtime integration.
- v1 remains leaner and closer to direct utility-style CLI usage.

## Data Access and Transport

### v1 Data Access

- Local data access by direct DB open paths (`sqlite-cli/open-db!` usage in command implementations).
- Optional API fetch through `api-fetch` against Desktop HTTP API.

### v2 Data Access

- Worker method invocation through transport payloads encoded as transit.
- Invoke endpoint: `/v1/invoke`.
- Event stream support via `/v1/events` for long-running/progress flows.

## Packaging and Build Signals

### v1 Packaging

- Package manifest: `deps/cli/package.json`.
- Package name: `@logseq/cli`.
- Bin entry: `cli.mjs`.

### v2 Packaging

- Root package bin points to `dist/logseq.js` in root `package.json`.
- Shadow target `:logseq-cli` in `shadow-cljs.edn` points to `logseq.cli.main/main`.
- db-worker runtime artifacts are expected in dist/static release workflows.

## Compatibility and Migration Considerations

1. Invocation assumptions:

Scripts written for v1 command names/options may not map cleanly to v2 command families.

1. Runtime assumptions:

- v1 scripts may assume direct local DB access semantics.
- v2 scripts should assume worker lifecycle, lock/ownership, and transport semantics.

1. Environment assumptions:

- v1 API-mode workflows depend on Desktop HTTP API state.
- v2 workflows depend on db-worker availability and CLI-managed lifecycle behavior.

1. MCP usage:

- Be explicit which CLI generation a client configuration targets.
- Avoid ambiguous binary paths in automation templates.

## Recommended Documentation Structure

Keep three layers, each with clear audience:

1. Version-specific references:

- `docs/cli/logseq-cli-v1.md`
- `docs/cli/logseq-cli-v2.md`

1. Comparative investigation (this page):

- `docs/cli/cli-v1-v2-comparison.md`

1. Router entry point:

- `docs/cli/logseq-cli.md`

Keep short legacy/new labels in user-facing integration docs so readers pick the right command set immediately.

## Related Docs

- `docs/cli/logseq-cli.md`
- `docs/cli/logseq-cli-v1.md`
- `docs/cli/logseq-cli-v2.md`
- `docs/cli/cli-generations.md`
- `ddocs/reference/api/mcp-server.md`
