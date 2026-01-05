# Logseq CLI Developer Onboarding

Audience: Junior developers contributing to the Logseq CLI (the `deps/cli` package).

Scope: Local setup, how it works, how to run/debug/test, how to add a new command, conventions, and common pitfalls.

---

## Quick start

1. Prerequisites

- Node.js >= 22.17.0 and Yarn
- Babashka (bb) installed
- The Logseq Desktop “database version” app installed (needed for DB graphs)

2. Install dependencies

- Open a terminal in `deps/cli`
- Run: `yarn install`

3. Try the CLI locally

- From `deps/cli`, run: `node cli.mjs -h`
- Optionally, create a global symlink to run `logseq` directly: `yarn link`

4. Sanity check commands

- List graphs (requires the Desktop “database version” app): `logseq list`
- Show graph info: `logseq show <graph-name>`
- Export EDN: `logseq export-edn -g <graph-name> -f out.edn`
- Import EDN: `logseq import-edn -g <graph-name> -f out.edn`
- Validate graph: `logseq validate -g <graph-name>`
- MCP server: `logseq mcp-server -g <graph-name>`
- Search current graph through HTTP API: start Desktop app with Local HTTP API enabled, then: `logseq search <term> -a <api-server-token>`

If you didn’t `yarn link`, substitute `node cli.mjs` for `logseq`.

---

## Repository layout for the CLI

- `deps/cli/cli.mjs` — Node entrypoint that boots an nbb runtime and loads ClojureScript.
- `deps/cli/src/logseq/cli.cljs` — Main namespace. Defines the command table, help, and dispatch via `babashka.cli`.
- `deps/cli/src/logseq/cli/commands/` — Individual CLI commands:
  - `graph.cljs` — list, show commands
  - `search.cljs` — search command
  - `query.cljs` — query command
  - `export.cljs` — export (markdown) command
  - `export_edn.cljs` — export-edn command
  - `import_edn.cljs` — import-edn command (v0.4.0+)
  - `validate.cljs` — validate command (v0.4.0+)
  - `mcp_server.cljs` — mcp-server command (v0.3.0+)
  - `append.cljs` — append command
- `deps/cli/src/logseq/cli/spec.cljs` — Command option specifications for `babashka.cli`.
- `deps/cli/src/logseq/cli/util.cljs` — Shared helpers (paths, API calls, error handling).
- `deps/cli/src/logseq/cli/common/` — Shared infrastructure:
  - `graph.cljs` — DB graphs directory logic
  - `file.cljs` — File operations
  - `util.cljs` — Common utilities
  - `export/` — Export utilities (text, common)
  - `mcp/` — MCP server and tools (v0.3.0+)
- `deps/cli/README.md` — User-facing CLI usage docs.
- `deps/cli/CHANGELOG.md` — Version history and changes.
- `deps/cli/package.json` — Package metadata, test script, Node engine.
- `deps/cli/bb.edn` — Babashka tasks for linting and vendorizing.
- `deps/cli/nbb.edn` — nbb test runner deps (during development).

Related code you may interact with:

- `deps/db` — Database utilities and export/import logic
- `src/electron/electron/*` — DB and Electron helpers referenced by the CLI

---

## How the CLI works

- Node bootstrap: `cli.mjs` uses `@logseq/nbb-logseq` to load the main CLJS namespace (`src/logseq/cli.cljs`). It adds classpaths for `src` and `vendor/src`.
- Main dispatch: `logseq.cli` builds a `table` of commands and dispatches via `babashka.cli/dispatch`.
- Lazy loading: Commands are loaded with `lazy-load-fn` to keep startup fast. Your command’s function is resolved on first use.
- Offline by default: Most commands operate directly on local DB graphs under `~/logseq/graphs`.
- HTTP API when needed: Some commands (e.g., `search` with `-a/--api-query-token`) hit the Desktop app’s Local HTTP API if enabled.

Key runtime details:

- Graphs directory: `~/logseq/graphs` (from `logseq.cli.common.graph/get-db-graphs-dir`).
- Before running most commands, the CLI checks the DB Desktop app is installed (the directory exists).
- Error reporting: SCI errors use `nbb.error/print-error-report`; other errors are printed to stderr and exit with code 1.

---

## Running and debugging

Run from `deps/cli`:

- Help: `node cli.mjs -h` or `logseq -h`
- List: `logseq list`
- Show: `logseq show <graph>`
- Search via API: `logseq search <term> -a <token>` (requires HTTP API enabled in Desktop app)

Debug tips:

- Use `println` or `js/console.log` inside CLJS.
- Wrap async flows with `promesa.core` and handle errors using `(p/catch cli-util/command-catch-handler)`.
- If you see SCI errors, they’ll be formatted by `nbb.error/print-error-report` via the lazy loader.

---

## Testing and linting

From `deps/cli`:

- Run tests: `yarn test`
  - Uses `@logseq/nbb-logseq` and `nextjournal/nbb-test-runner`.
  - Options: `yarn test -H` (help), `yarn test -i focus` (run focused tests).
- Lint and hygiene tasks (Babashka):
  - `bb lint:carve` — dead code and unused vars
  - `bb lint:ns-docstrings` — ensure namespaces have docstrings
  - `bb lint:minimize-public-vars` — reduce public API surface
  - `bb lint:large-vars` — flag overly large forms

Vendorizing nbb deps (when packaging):

- `bb build:vendor-nbb-deps` — copies cached nbb deps into `vendor/src` and removes `nbb.edn`, making the package self-contained.

---

## Adding a new command

1. Create the command namespace

- File: `deps/cli/src/logseq/cli/commands/<your_cmd>.cljs`
- Namespace: `(ns logseq.cli.commands.<your-cmd> (:require [promesa.core :as p] [logseq.cli.util :as cli-util] ...))`
- Export a function (e.g., `run` or a name matching how you’ll reference it in `lazy-load-fn`). Prefer returning a promise with `promesa` for async flows.

2. Add CLI options (if needed)

- Edit `deps/cli/src/logseq/cli/spec.cljs` to define a spec map for your command’s options (descriptions, coercions, aliases, etc.).

3. Register it in the command table

- Edit `deps/cli/src/logseq/cli.cljs` and add an entry in `table`:
  - `:cmds ["your-cmd"]`
  - `:desc` brief description
  - `:fn (lazy-load-fn 'logseq.cli.commands.<your-cmd>/<your-fn>)`
  - `:args->opts`, `:coerce`, `:require`, `:no-keyword-opts` as appropriate
  - `:spec` pointing to the spec you added

4. Implement the logic

- Reuse helpers in `cli.util.cljs` (e.g., graph path resolution, DB open args, API fetch helpers, error handling).
- For local DB reads, open a connection via `(apply sqlite-cli/open-db! (cli-util/->open-db-args graph))` and ensure you close or scope appropriately.
- Prefer small, pure helpers and minimal stdout prints for stable outputs.

5. Add tests

- Create tests in `deps/cli/test` using `nextjournal/nbb-test-runner`.
- Keep tests hermetic when possible; for DB reads, use a small sample graph or mock layers where practical.

6. Try it manually

- `logseq your-cmd ...` (or `node cli.mjs your-cmd ...`)
- `logseq help your-cmd` should show your options if you wired `:spec`.

---

## Conventions and style

- Follow the main project’s dev practices (`docs/dev-practices.md`).
- Namespace docstrings: every public namespace should have a short docstring.
- Keep public API surface minimal; prefer private fns where possible.
- Favor small, composable functions; avoid giant forms (see `bb lint:large-vars`).
- Use `babashka.cli` for option parsing and `cli/format-opts` for help output.
- Error messages should be actionable and exit with non-zero when appropriate.

---

## Working with graphs and the DB

- Graphs live under `~/logseq/graphs`.
- `list` and `show` rely on `logseq.cli.common.graph` to resolve paths and names.
- Local DB access uses `logseq.db.common.sqlite-cli` under the hood; you’ll typically create open args via `cli.util/->open-db-args`.
- The Desktop app must be the “database version” for DB graphs to exist; the CLI shows a helpful error if not installed.

---

## Using the HTTP API (optional)

- Enable “Local HTTP Server” in the Desktop app settings.
- Some commands accept an `-a/--api-query-token` option; provide a valid token to run via API.
- Handle API responses with `cli-util/api-handle-error-response` and error flows via `cli-util/command-catch-handler`.

---

## Common pitfalls

- Wrong Node version: ensure Node >= 22.17.0 (CLI enforces this via `package.json`).
- Missing Desktop “database version”: local DB commands fail if `~/logseq/graphs` is absent.
- Not using `lazy-load-fn`: register commands through the lazy loader to prevent slow startup.
- Unhandled async errors: always add `(p/catch ...)` in command pipelines.
- CLI vs. API: API-based features require the Desktop app running with HTTP API enabled and a token.- **Command syntax change (v0.4.0)**: Graph name moved from positional arg to `-g` option for most commands.

---

## PR checklist (before you open a PR)

- Added/updated tests (`yarn test`).
- Linted with bb tasks where applicable.
- Updated `spec.cljs` and help text for any new options.
- Verified manual runs for key scenarios.
- Updated docs (`docs/06-user-guides/cli-user-guide.md`) if user-facing behavior changed.

---

## Reference snippets

- Show help for all commands: `logseq -h`
- Show help for a command: `logseq help <command>`
- List graphs: `logseq list`
- Show graph details: `logseq show <graph>`
- Search current graph via API: `logseq search <term> -a <token>`
- Query local graph: `logseq query '[:find (pull ?b [*]) :where [?b :kv/value]]' -g <graph>`
- Export EDN: `logseq export-edn -g <graph> -f out.edn`
- Import EDN: `logseq import-edn -g <graph> -f in.edn`
- Validate graph: `logseq validate -g <graph>`
- MCP server: `logseq mcp-server -g <graph>`

If you get stuck, start by reading `deps/cli/src/logseq/cli.cljs` and the relevant command in `deps/cli/src/logseq/cli/commands/`, then check `deps/cli/README.md`.

---

Happy hacking!
