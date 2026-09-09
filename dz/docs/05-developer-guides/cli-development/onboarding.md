# Logseq CLI Developer Onboarding

> **Last revised:** 2026-05-29
> **Scope note:** This guide now targets the current db-worker-node-based CLI under `src/main/logseq/cli`.

Audience: Developers contributing to the current Logseq CLI implementation.

Scope: Local setup, architecture, run/debug/test workflow, command extension points, conventions, and migration pitfalls.

---

## Quick start

1. Prerequisites

- Node.js >= 22.20.0 (`package.json` engines)
- pnpm
- Clojure CLI (for shadow-cljs build targets)
- Babashka (recommended for local tasks and CLI e2e)

1. Install dependencies

- Open a terminal at repository root
- Run: `pnpm install`

1. Try the CLI locally

- Build CLI/runtime artifacts:
  - `clojure -M:cljs compile logseq-cli`
  - `pnpm db-worker-node:compile:bundle`
- Run help:
  - `node static/logseq-cli.js --help`

> **Removed:** The legacy source bootstrap flow `cd deps/cli && yarn install && node cli.mjs -h` is no longer valid in this repo state.
>
> **Removed:** `yarn link` workflow for `deps/cli` is no longer applicable.

1. Sanity check commands

- `node static/logseq-cli.js graph list`
- `node static/logseq-cli.js graph create --graph demo`
- `node static/logseq-cli.js list page --graph demo --limit 5`
- `node static/logseq-cli.js show --graph demo --page Home`
- `node static/logseq-cli.js doctor --graph demo`

> **Removed:** Top-level `list`, `show`, `validate`, `export-edn`, `import-edn`, and `mcp-server` command surface from legacy CLI v1 is no longer the active onboarding target.

---

## Repository layout for the CLI

- `src/main/logseq/cli/main.cljs` — CLI entrypoint (`run!`, `main`), parse/execute/format pipeline.
- `src/main/logseq/cli/commands.cljs` — Command table assembly, parse + validation, action execution.
- `src/main/logseq/cli/command/*.cljs` — Command family modules (`graph`, `list`, `upsert`, `remove`, `query`, `search`, `show`, `server`, `sync`, `doctor`, `debug`, `auth`, `completion`, `skill`, `agent`, `example`).
- `src/main/logseq/cli/config.cljs` — Config/env/flag resolution.
- `src/main/logseq/cli/root_dir.cljs` — Root-dir normalization and permission checks.
- `src/main/logseq/cli/server.cljs` — db-worker-node lifecycle + revision checks.
- `src/test/logseq/cli/` — Unit tests for command parsing/execution and formatting.
- `cli-e2e/` — Shell-first e2e harness (`bb -f cli-e2e/bb.edn ...`).
- `deps/db-sync/` — Sync backend adapter used by sync/e2e preflight.

---

## How the CLI works

- Parsing and dispatch are implemented by `logseq.cli.commands` + `logseq.cli.command.core` using `babashka.cli`.
- `logseq.cli.main/run!` resolves config, ensures root-dir, builds an action, executes it, formats output, and returns exit code + output.
- Graph operations are db-worker-node mediated (HTTP invoke), not direct local SQLite manipulations from a standalone `deps/cli` package.
- Default CLI root is `~/logseq`; graph storage is `<root-dir>/graphs`.
- Output modes include human, json, and edn (`--output`/`-o`).
- Debug and perf flags are built in:
  - `--verbose` emits structured debug logs to stderr.
  - `--profile` prints stage timing lines to stderr.

Key runtime details:

- Root-dir permission failures surface as `:root-dir-permission`.
- Command/build/execute/format stages are measured when profiling is enabled.
- Global options include `--graph`, `--root-dir`, `--timeout-ms`, `--output`, `--verbose`, `--profile`, `--config`, `--version`, and `--help`.

---

## Running and debugging

Run from repository root:

- Build CLI and worker artifacts:
  - `clojure -M:cljs compile logseq-cli`
  - `pnpm db-worker-node:compile:bundle`
- Help: `node static/logseq-cli.js --help`
- Graph list: `node static/logseq-cli.js graph list`
- Graph info: `node static/logseq-cli.js graph info --graph <graph>`
- Structured output: `node static/logseq-cli.js graph list --output json`

Debug tips:

- Use `--verbose` and `--profile` first.
- Use command-level tests in `src/test/logseq/cli/command/` to isolate parser and action issues.
- For server lifecycle issues, inspect `src/main/logseq/cli/server.cljs` and run `node static/logseq-cli.js server list --output json`.

---

## Testing and linting

From repository root:

- Unit tests (full): `bb dev:lint-and-test`
- Unit tests (focused): `bb dev:test -v <namespace/testcase-name>`
- CLI e2e non-sync: `bb -f cli-e2e/bb.edn test`
- CLI e2e sync: `bb -f cli-e2e/bb.edn test-sync`
- CLI e2e cleanup: `bb -f cli-e2e/bb.edn cleanup`

`cli-e2e` preflight builds these artifacts when `--skip-build` is not passed:

- `clojure -M:cljs compile logseq-cli`
- `pnpm db-worker-node:compile:bundle`
- `pnpm --dir deps/db-sync build:node-adapter`

---

## Adding a new command

1. Create the command namespace

- File: `src/main/logseq/cli/command/<family>.cljs`
- Follow the family pattern used by existing modules (`entries`, option spec, validation, and action construction).

1. Add CLI options (if needed)

- Define command/family spec in the command module and merge with global options from `logseq.cli.command.core/global-spec`.

1. Register it in the command table

- Edit `src/main/logseq/cli/commands.cljs` by wiring your command family `entries` into `base-table`.

1. Implement the logic

- Keep parsing/validation in command modules and execution in action handlers.
- Use consistent result maps for formatter compatibility (`:status`, `:data`, `:error`, optional `:exit-code`).

1. Add tests

- Unit tests in `src/test/logseq/cli/command/<family>_test.cljs` and/or `src/test/logseq/cli/commands_test.cljs`.
- Add/extend shell e2e cases in `cli-e2e/` when behavior is operator-facing.

1. Try it manually

- `node static/logseq-cli.js --help`
- `node static/logseq-cli.js <command> --help`
- `node static/logseq-cli.js <command> ... --output json`

---

## Conventions and style

- Follow the main project’s dev practices (`docs/dev-practices.md`).
- Namespace docstrings: every public namespace should have a short docstring where applicable.
- Keep public API surface minimal; prefer private fns where possible.
- Favor small, composable functions; avoid giant forms (see `bb lint:large-vars`).
- Use `babashka.cli` for option parsing and keep command help aligned with generated summaries.
- Error messages should be actionable and exit with non-zero when appropriate.

---

## Working with graphs and the DB

- Graphs live under `<root-dir>/graphs` (`~/logseq/graphs` by default).
- Current inspect/edit surface is grouped under `list`, `upsert`, `remove`, `query`, `search`, and `show` families.
- Graph lifecycle operations are under `graph` and `server` commands.
- Worker orchestration and revision checks are handled by CLI/server code, not a Desktop Local HTTP API token flow.

---

## Authentication and sync

- Cloud auth is handled by `login`/`logout` and stored in `~/logseq/auth.json`.
- Sync workflow uses `sync` subcommands (`status`, `start`, `stop`, `upload`, `download`, `asset download`, `remote-graphs`, `ensure-keys`, `grant-access`, `config set|get|unset`).

> **Removed:** Legacy `search <term> -a <token>` onboarding guidance tied to Desktop Local HTTP API token usage.

---

## Common pitfalls

- Wrong Node version: ensure Node >= 22.20.0.
- Running old deps/cli commands from legacy docs (`import-edn`, `export-edn`, `mcp-server`, top-level `validate`) instead of current `graph`/`sync` surfaces.
- Forgetting build artifacts before local runs (`static/logseq-cli.js`, db-worker bundle, db-sync adapter for sync e2e).
- Ignoring stderr when using `--verbose`/`--profile` (stdout stays formatter-controlled).
- Mixing CLI v1 and v2 assumptions when editing docs or tests.

---

## PR checklist (before you open a PR)

- Added/updated unit tests in `src/test/logseq/cli/**`.
- Added/updated `cli-e2e` cases when command behavior changed.
- Ran `bb dev:lint-and-test` (or explained scope-limited alternative).
- Verified manual runs for key scenarios.
- Updated CLI docs (`docs/cli/logseq-cli.md`, `docs/cli/logseq-cli-v2.md`, and comparison docs) for user-facing changes.

---

## Findings: New Capabilities

Compared with this document's previous revision, the current CLI in `src/main/logseq/cli` adds or formalizes these capabilities:

- db-worker-node-first runtime with explicit server lifecycle (`server list|start|stop|restart|cleanup`) and revision checks.
- Structured output modes (`--output json|edn`) across command families.
- Expanded command families: `graph`, `list`, `upsert`, `remove`, `query`, `search`, `show`, `sync`, `doctor`, `debug`, `completion`, `skill`, and `agent`.
- Built-in CLI profiling (`--profile`) and structured debug logging (`--verbose`).
- Cloud auth lifecycle (`login`, `logout`) with persisted auth file (`~/logseq/auth.json`).
- Shell-first CLI e2e harness (`cli-e2e`) with suite-level preflight and case-level parallelism.

---

## Reference snippets

- Show help for all commands: `node static/logseq-cli.js --help`
- Show command help: `node static/logseq-cli.js <command> --help`
- List graphs: `node static/logseq-cli.js graph list`
- Show graph metadata: `node static/logseq-cli.js graph info --graph <graph>`
- List pages: `node static/logseq-cli.js list page --graph <graph> --limit 10`
- Query local graph: `node static/logseq-cli.js query --graph <graph> --query '[:find (pull ?b [*]) :where [?b :block/title]]'`
- Search blocks: `node static/logseq-cli.js search block --graph <graph> --content note`
- Export graph: `node static/logseq-cli.js graph export --graph <graph> --type edn --file out.edn`
- Import graph: `node static/logseq-cli.js graph import --graph <new-graph> --type edn --input out.edn`
- Validate graph: `node static/logseq-cli.js graph validate --graph <graph>`
- Server status view: `node static/logseq-cli.js server list --output json`
- Sync status: `node static/logseq-cli.js sync status --graph <graph>`

If you get stuck, start with `src/main/logseq/cli/main.cljs`, `src/main/logseq/cli/commands.cljs`, and the relevant module in `src/main/logseq/cli/command/`.

---

Happy hacking!
