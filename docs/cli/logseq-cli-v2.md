# Logseq CLI v2 (db-worker-node)

The Logseq CLI is a Node.js program compiled from ClojureScript that connects to a db-worker-node server managed by the CLI. When installed, the CLI binary name is `logseq`.

Source-of-truth note:
If any docs conflict, prefer behavior implemented in `src/main/logseq/cli/*` and `src/test/logseq/cli/*`.

Related docs:

- CLI v1 reference: `docs/cli/logseq-cli-v1.md`
- Comparison: `docs/cli/cli-generations.md`
- Landing page: `docs/cli/logseq-cli.md`
- db-worker-node architecture and integrations: `docs/agent-guide/db-worker/db-worker-node-comprehensive-guide.md`

## Primary source paths

- `src/main/logseq/cli/main.cljs`
- `src/main/logseq/cli/commands.cljs`
- `src/main/logseq/cli/server.cljs`
- `src/main/logseq/cli/transport.cljs`
- root `package.json`
- `shadow-cljs.edn`

## Runtime model

CLI v2 is db-worker-node centric.

- Command parsing and action building happen in `src/main/logseq/cli/*`.
- Graph-scoped commands ensure a db-worker-node process through `logseq.cli.server/ensure-server!`.
- The CLI invokes worker methods over HTTP `/v1/invoke` through `logseq.cli.transport/invoke`.
- Ownership, revision checks, restart behavior, and lock handling live in `src/main/logseq/cli/server.cljs`.

## Packaging and entrypoint

- Root package bin: `dist/logseq.js`
- Build target: `:logseq-cli` in `shadow-cljs.edn`
- Bundled worker runtime: `dist/db-worker-node.js`

## Operational characteristics

- db-worker-node mediated command execution
- explicit server lifecycle commands
- ownership and revision compatibility checks
- different command surface and operational semantics from legacy deps/cli

## Common workflows

Build the CLI:

```bash
clojure -M:cljs compile logseq-cli
pnpm db-worker-node:release:bundle
```

Run the CLI locally:

```bash
node ./dist/logseq.js graph list
```

## Key behavioral notes

- The CLI manages `db-worker-node` automatically and should not require a manual daemon startup path for ordinary commands.
- Server lifecycle, owner-source checks, and revision mismatch handling are part of the normal runtime model.
- The root CLI config defaults to `<root-dir>/cli.edn`, with `root-dir` defaulting to `~/logseq`.
- Auth state is stored separately in `~/logseq/auth.json`.
