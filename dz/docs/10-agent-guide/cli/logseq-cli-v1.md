# Logseq CLI v1 (Legacy deps/cli)

This page documents the legacy CLI implemented under `deps/cli`.

Source-of-truth note:
If any docs conflict, prefer behavior implemented in `deps/cli/src/logseq/*`.

## Primary source paths

- `deps/cli/src/logseq/cli.cljs`
- `deps/cli/src/logseq/cli/util.cljs`
- `deps/cli/src/logseq/cli/commands/*.cljs`
- `deps/cli/package.json`

## Runtime model

CLI v1 has two execution modes.

### Local graph mode

- Commands commonly access local DB graphs directly.
- For example, query opens SQLite directly through `sqlite-cli/open-db!` in `deps/cli/src/logseq/cli/commands/query.cljs`.

### HTTP API mode

- CLI v1 can call the Desktop local HTTP API through `logseq.cli.util/api-fetch` in `deps/cli/src/logseq/cli/util.cljs`.
- Mode selection is handled by `api-command?` in `deps/cli/src/logseq/cli/util.cljs`.

## Packaging and entrypoint

- npm package: `@logseq/cli`
- Package manifest: `deps/cli/package.json`
- Bin entry: `cli.mjs`

## Characteristics

- nbb-based bootstrap flow
- local SQLite access is a first-class execution path
- optional Desktop HTTP API integration
- command surface differs from the newer db-worker-based CLI

## Related docs

- Comparison: `docs/cli/cli-generations.md`
- CLI v2: `docs/cli/logseq-cli-v2.md`
- Legacy developer onboarding: `dz/docs/05-developer-guides/cli-development/onboarding.md`
- Legacy usage guide: `ddocs/how-to/integrations/cli-basic.md`
