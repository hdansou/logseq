# Logseq CLI Generations (Source-Of-Truth)

This page clarifies the two CLI implementations that coexist in the repository.

Use source code as the source of truth when documentation differs.

## At a glance

| Track                 | Primary source path             | Runtime model                                                  | Packaging signal                                                           |
| --------------------- | ------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Legacy CLI (v1-style) | `deps/cli/src/logseq/cli.cljs`  | Local SQLite access by default, optional Desktop HTTP API mode | `deps/cli/package.json` (`@logseq/cli`, `cli.mjs`)                         |
| New CLI (v2-style)    | `src/main/logseq/cli/main.cljs` | db-worker-node orchestration + invoke transport                | root `package.json` (`dist/logseq.js`) + `shadow-cljs.edn` (`:logseq-cli`) |

## Source-backed behavior differences

### Legacy CLI (v1-style)

- HTTP API path exists through `logseq.cli.util/api-fetch` in `deps/cli/src/logseq/cli/util.cljs`.
- Local mode exists through direct DB open/read (for example, query command uses `sqlite-cli/open-db!`) in `deps/cli/src/logseq/cli/commands/query.cljs`.
- Mode selection is handled by `api-command?` in `deps/cli/src/logseq/cli/util.cljs`.

### New CLI (v2-style)

- Core commands ensure a db-worker-node server via `logseq.cli.server/ensure-server!`.
- Command execution invokes worker methods through HTTP `/v1/invoke` using `logseq.cli.transport/invoke`.
- Lifecycle and ownership semantics are centralized in `src/main/logseq/cli/server.cljs`.

## Version-specific docs

- CLI v1 reference: `docs/cli/logseq-cli-v1.md`
- CLI v2 reference: `docs/cli/logseq-cli-v2.md`
- Comprehensive comparison: `docs/cli/cli-v1-v2-comparison.md`
- CLI landing page: `docs/cli/logseq-cli.md`
- Legacy-focused companion docs:
  - `dz/docs/05-developer-guides/cli-development/onboarding.md`
  - `ddocs/how-to/integrations/cli-basic.md`

When editing docs, keep this split explicit to avoid mixing command surfaces and runtime assumptions.
