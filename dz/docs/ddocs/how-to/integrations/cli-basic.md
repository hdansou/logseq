---
title: 'CLI Basics'
description: 'How to use the Logseq command-line interface for automation and scripting'
status: stable
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
logseqVersion: '2.0.1'
cliVersion: 'source-backed'
platforms: [desktop]
graphType: [db]
relatedDocs:
  reference:
    - '[HTTP API](../../reference/api/http-api.md)'
    - '[MCP Server](../../reference/api/mcp-server.md)'
---

# CLI Basics

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-11

> Source-backed Logseq CLI basics for working with DB graphs from the terminal.

## Scope

This page matches the command surface at `be800f1711`. The CLI is written in OCaml and compiled to a
Node.js program with Melange. Its code is under `cli/lib/`:

| What                   | Where                                                           |
| ---------------------- | --------------------------------------------------------------- |
| Command list           | `cli/lib/command_id.ml:1-63` (IDs) and `:65-…` (command paths)  |
| Options per command    | `allowed_options_for_path`, `cli/lib/cli_parse.ml:176-330`      |
| Global options         | `global_options`, `cli/lib/command_registry.ml:161-183`         |

Any option not in a command's list is rejected with `Unknown option: :<name>` (`cli_parse.ml:333-339`).

It is packaged as the npm package `@logseq/cli`, whose command is `logseq`
(`scripts/prepare-cli-package.mjs:127-132`). CI publishes that package with the dist-tag given as a
workflow input, or `alpha` by default (`.github/workflows/deps-cli.yml:195-202`). So
`npm install -g @logseq/cli@alpha` should install it. This was derived from the workflow, not run.

> **Removed:** The old flat command model (`logseq list`, `logseq show my-graph`, `logseq export`, `logseq import-edn`, `logseq validate`, `logseq append`, `logseq mcp-server`) is not in the current command table (`command_id.ml`). There is no MCP command. The MCP server exists only in the desktop app ([MCP Server](../../reference/api/mcp-server.md)).
> **Removed:** Positional graph syntax such as `logseq search my-graph "term"` and `logseq query my-graph '[:find ...]'` is no longer current; use `--graph`.
> **Removed:** The older `-a/--api-server-token` workflow does not exist in the current CLI. No command accepts that option.

## Capabilities Worth Knowing

- `graph create --enable-sync` creates the graph, uploads it, and starts sync in one command
  (`cli/lib/graph.ml:1024-1042`). `--e2ee-password` is only accepted together with `--enable-sync` (`:378-380`).
- `graph backup list/create/restore/remove` manage backups (`command_id.ml`, `cli_parse.ml:180-187`).
- `graph export --type sqlite` is supported alongside EDN export (`cli_parse.ml:881-887`).
- `query list` lists built-in queries: `list-priority`, `list-status`, `recent-updated` and `task-search`
  (`cli/lib/query.ml:74`, `:124`, `:174`, `:263`). It also lists custom queries (`query.ml:7`).
- `show` accepts `--page-hierarchy`, `--linked-references` and `--ref-id-footer` (`cli_parse.ml:291-301`).
- `sync download` accepts `--progress`, which is off by default (`cli_parse.ml:312-313`, `cli/lib/sync.ml:193`).
- `agent bridge`, `completion`, `debug pull`, `doctor`, `skill show`, `skill install` and `example` are
  utility commands (`command_id.ml`).
- `doctor` runs three checks: `db-worker-script`, `root-dir` and `running-servers` (`cli/lib/doctor.ml:61-190`).
- `graph switch` and `graph remove` are part of the graph commands.
- `graph validate` accepts `--fix` (`cli_parse.ml:183`, `graph.ml:475`).
- `upsert` covers `block`, `page`, `task`, `asset`, `tag` and `property`. `remove` covers `block`, `page`,
  `tag` and `property`.

## Prerequisites

- Node.js 22.20.0 or higher. The package copies the repository's `engines` field (`package.json:15-17`,
  `prepare-cli-package.mjs:134`).
- A DB graph under the CLI root directory. Graphs live in `<root-dir>/graphs` (`graph.ml:573`), and the root
  directory defaults to `~/logseq` (`cli/lib/cli_config.ml:42-43`). That is the same folder the desktop app
  uses by default.

You do not need the desktop app running. The CLI finds and starts its own `db-worker-node.js` process
(`cli/lib/server_runtime.ml:64-77`, `:203-213`, `:274`).

> The CLI does not read `LOGSEQ_GRAPHS_DIR`; that name does not appear in `cli/lib`. If the desktop app uses
> a different graphs folder, point the CLI at the matching root with `--root-dir` or `LOGSEQ_CLI_ROOT_DIR`
> (`cli_config.ml:125`).

## Choosing a Graph

Most commands need a graph. The CLI takes it from, in order of what you would normally use:

- `-g/--graph <name>`
- the `graph` key in `cli.edn`
- the `LOGSEQ_CLI_GRAPH` environment variable

These sources are read at `cli_config.ml:124` and `:309-316`. When none gives a graph, the CLI stops with
"graph name is required" and the hint `Use --graph <name>` (`cli/lib/error.ml:298`).

These commands do not need a graph (`command_id.ml:166-170`): `version`, `graph list`, `server list`,
`server cleanup`, `completion`, `login`, `logout`, `skill show`, `skill install`, `example` and
`sync remote-graphs`.

## Quick Start

```bash
logseq --help
logseq --version
logseq graph list
logseq graph info --graph my-graph
logseq list page --graph my-graph
logseq search page --graph my-graph --content "meeting notes"
logseq query --graph my-graph --name recent-updated --inputs '[30]'
logseq show --graph my-graph --page Home
logseq sync status --graph my-graph   # requires `logseq login` first
```

The commands on this page were checked against the parser, not run.

## Core Commands

### Listing

List subcommands (`cli_parse.ml:190-216`):

- `list page`
- `list tag`
- `list property`
- `list task`
- `list node`
- `list asset`

All of them accept `--fields`, `--limit`, `--offset`, `--sort` and `--order` (`cli_parse.ml:164-165`).

### Search

Search is scoped by entity type and takes `-c/--content` (`cli_parse.ml:285-288`,
`cli/lib/search.ml:50`):

- `search block --graph my-graph --content "task"`
- `search page --graph my-graph --content "home"`
- `search property --graph my-graph --content "owner"`
- `search tag --graph my-graph --content "quote"`

### Query

`query` accepts `--query`, `--name` and `--inputs` (`cli_parse.ml:290`):

- `query --graph my-graph --query '[:find ?title :where [?p :block/title ?title]]'`
- `query --graph my-graph --name recent-updated --inputs '[30]'` (input `recent-days`, `query.ml:176`)
- `query list --graph my-graph`

### Show

`show` is a tree inspector, not a graph metadata command. It accepts `--id`, `--uuid`, `--page`,
`--page-hierarchy`, `--linked-references`, `--ref-id-footer` and `--level` (`cli_parse.ml:291-301`):

- `show --graph my-graph --page Home`
- `show --graph my-graph --page Home --page-hierarchy true`
- `show --graph my-graph --id 123 --level 2`
- `show --graph my-graph --uuid 11111111-1111-1111-1111-111111111111`

A boolean option on its own means `true`. It also takes an explicit `true` or `false` as the next argument
(`cli_parse.ml:33-40`, `:60-67`). `--level` must be an integer (`:519`).

### Upsert and remove

- `upsert page --graph my-graph --page Home`
- `upsert block --graph my-graph --content "New block"`. With no `--target-id`, `--target-uuid` or
  `--target-page`, the block goes to today's journal (`cli/lib/upsert.ml:1046-1051`).
- `upsert task --graph my-graph --content "Write report" --target-page Home --status todo`. This creates a
  task block on `Home` (`upsert.ml:976-992`).
- `upsert task --graph my-graph --page Home --status doing`. This turns the page `Home` itself into a task
  (`mode = Page`, `upsert.ml:921-935`). It does not add a block to the page.
- `upsert asset --graph my-graph --path /tmp/diagram.png --target-page Home`. `--path` is required when
  creating an asset (`upsert.ml:303`).
- `upsert tag --graph my-graph --name Project`
- `upsert property --graph my-graph --name owner --type default --cardinality one`. `--type default`
  (`cli/lib/property.ml:58`) and `--cardinality one|many` (`property.ml:87-88`) are both accepted.
- `remove block --graph my-graph --id 123`
- `remove page --graph my-graph --page Home`
- `remove tag --graph my-graph --name Project`
- `remove property --graph my-graph --name owner`

`upsert block` and `upsert page` are for generic tag and property changes: `--update-tags`,
`--update-properties`, `--remove-tags` and `--remove-properties` (`cli_parse.ml:221-248`). `upsert task` is
for task fields: `--status`, `--priority`, `--scheduled` and `--deadline`, plus their `--no-*` forms
(`cli_parse.ml:259-279`). `upsert tag` and `upsert property` edit the ontology itself (`cli_parse.ml:281-284`).

> **Unverified:** exactly how `--status` text is matched. The CLI queries the Status property's closed values
> and resolves the text against them, and an unknown value returns an error that lists the available
> statuses (`upsert.ml:2519-2536`). The matching rule inside `Task_status.resolve_status_ident` was not read.
> Run `logseq query --graph my-graph --name list-status` to see the values.

## Graph Management

### Graph create, info, export, import

- `graph create --graph my-graph`
- `graph create --graph my-graph --enable-sync --e2ee-password "my-secret"`
- `graph switch --graph my-graph`
- `graph remove --graph my-graph`
- `graph info --graph my-graph`
- `graph export --graph my-graph --type edn --file /tmp/my-graph.edn --edn-options '{:export-type :graph :include-timestamps? true}' --pretty-print`
- `graph export --graph my-graph --type sqlite --file /tmp/my-graph.sqlite`
- `graph import --graph my-graph --type edn --input /tmp/my-graph.edn`
- `graph validate --graph my-graph`
- `graph validate --graph my-graph --fix`

`graph export` accepts `--type`, `--file`, `-e/--edn-options` and `-p/--pretty-print` (`cli_parse.ml:188-189`).
`--edn-options` must be an EDN map (`:452-466`). With `--type sqlite`, `--edn-options` and `--pretty-print`
are rejected (`graph.ml:381-386`).

The older flat flags `--include-timestamps`, `--exclude-built-in-pages` and `--exclude-namespaces` are not
in the option list, so they fail with `Unknown option` (`cli_parse.ml:333-339`). Put those settings in
`--edn-options` instead.

`graph import` requires `--type edn` or `--type sqlite`, and `--input` (`cli_parse.ml:890-898`).

### Backups

- `graph backup list --graph my-graph`
- `graph backup create --graph my-graph --name nightly`
- `graph backup restore --graph my-graph --src <backup-name> --dst my-graph-restore`
- `graph backup remove --graph my-graph --src <backup-name>`

A backup is named `<graph>-<name>-<UTC timestamp>`, or `<graph>-<UTC timestamp>` without `--name`
(`graph.ml:407-416`). Copy the exact name from `graph backup list`. Backups are stored in
`<graph dir>/backup/<backup-name>` (`graph.ml:629-633`). `restore` requires both `--src` and `--dst`
(`cli_parse.ml:847-858`). `remove` requires `--src` (`:860-867`).

> **Warning:** `graph backup remove` joins `--src` onto the backup directory without checking the path, then
> deletes that directory tree (`graph.ml:632-633`, `:893-898`). Pass only a name copied from
> `graph backup list`, never a path.

### Server and sync

- `server list`
- `server cleanup`
- `server start --graph my-graph`
- `server stop --graph my-graph`
- `server restart --graph my-graph`
- `sync status --graph my-graph`
- `sync start --graph my-graph --e2ee-password "my-secret"`
- `sync stop --graph my-graph`
- `sync upload --graph my-graph`
- `sync download --graph my-graph --progress`
- `sync asset download --graph my-graph --id 123`
- `sync remote-graphs`
- `sync ensure-keys --graph my-graph`
- `sync grant-access --graph my-graph --graph-id <uuid> --email teammate@example.com`
- `sync config set ws-url wss://sync.example`
- `sync config get ws-url`
- `sync config unset ws-url`

Sync options are listed at `cli_parse.ml:306-319`. `sync start` and `sync upload` take `--e2ee-password`.
`sync download` takes `--progress` and `--e2ee-password`. `sync ensure-keys` takes `--e2ee-password` and
`--upload-keys`.

`sync config` keys are `ws-url` and `http-base` (`sync.ml:92-95`). The key and value are positional
arguments (`cli_parse.ml:1042-1081`). The environment variables `LOGSEQ_CLI_WS_URL` and
`LOGSEQ_CLI_HTTP_BASE` also set them (`cli_config.ml:131-132`).

Every `sync` command requires login, `sync config` included (`command_id.ml:172-177`).
`login` and `logout` manage the saved cloud login in `~/logseq/auth.json` (`cli/lib/auth_state.ml:37-46`).
That default is under your home directory, not `--root-dir`.

## Utilities

- `agent bridge --graph my-graph` (`cli_parse.ml:1099-1100`)
- `completion zsh`, or `completion --shell bash`. Only `zsh` and `bash` are supported (`cli_parse.ml:1136-1160`).
- `debug pull --uuid 11111111-1111-1111-1111-111111111111 --graph my-graph`. Also accepts `--id` or
  `--ident` (`cli_parse.ml:321-322`).
- `doctor`, optionally with `--dev-script` (`cli_parse.ml:324`, `:1101-1105`)
- `skill show`
- `skill install --global` (`cli_parse.ml:329`)
- `example upsert page`. Everything after `example` selects which examples to show (`cli_parse.ml:813-818`).

## Working Modes

### Local Mode

Graph commands work on the DB graph under `<root-dir>/graphs` through a `db-worker-node` process that the CLI
manages. `server start/stop/restart/list/cleanup` control those processes explicitly.

### Connected Mode

Sync commands use the configured `ws-url` and `http-base` and require a saved login (`~/logseq/auth.json`).

## Notes

- `graph info` is the graph metadata command.
- `graph export` supports EDN and SQLite output.
- `query list` includes built-in and custom queries.
- `show` is for entity trees, not graph metadata.
- `doctor` is the quickest check when a graph command hangs or the worker will not start.
- Global options apply to every command (`command_registry.ml:161-183`):
  - `--config` (default `<root-dir>/cli.edn`)
  - `-g/--graph`
  - `--root-dir` (default `~/logseq`)
  - `--timeout-ms` (default `10000`)
  - `-o/--output`: `human`, `json` or `edn`, default `human` (`cli_parse.ml:445-450`)
  - `-v/--verbose`, `--profile`, `-h/--help`, `--version`
- Environment variables (`cli_config.ml:121-132`): `LOGSEQ_CLI_GRAPH`, `LOGSEQ_CLI_ROOT_DIR`,
  `LOGSEQ_CLI_CONFIG`, `LOGSEQ_CLI_TIMEOUT_MS`, `LOGSEQ_CLI_OUTPUT`, `LOGSEQ_CLI_WS_URL`,
  `LOGSEQ_CLI_HTTP_BASE`, plus login and logout timeout variables.

## Troubleshooting

### "Graph not found"

```bash
logseq graph list
logseq graph info --graph exact-graph-name
```

If the graph exists in the desktop app but not in `graph list`, check `--root-dir` (see
[Prerequisites](#prerequisites)).

### "graph name is required"

Pass `--graph`, or set `graph` in `cli.edn` or `LOGSEQ_CLI_GRAPH`.

### "Cannot connect to Logseq"

- Run `logseq doctor` first. It checks the db-worker-node script, the root directory and running servers.
- Make sure the graph exists and the CLI root directory is correct.
- For sync, run `logseq login`, then check the endpoints with `logseq sync config get`.

### "Invalid query syntax"

```bash
logseq query --graph my-graph --query '[:find ?t :where [?b :block/title ?t]]'
logseq query --graph my-graph --name task-search --inputs '["todo"]'
```

For `task-search`, a string status such as `"todo"` is trimmed, lower-cased and turned into
`:logseq.property/status.todo` (`query.ml:677-689`). A keyword input is passed through unchanged.
