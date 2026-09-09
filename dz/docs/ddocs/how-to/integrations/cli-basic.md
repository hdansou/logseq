---
title: 'CLI Basics'
description: 'How to use the Logseq command-line interface for automation and scripting'
status: stable
lastVerified: '2026-05-30'
logseqVersion: '0.11.x'
cliVersion: 'source-backed'
platforms: [desktop]
graphType: [db]
relatedDocs:
  reference:
    - '[HTTP API](../../reference/api/http-api.md)'
    - '[MCP Server](../../reference/api/mcp-server.md)'
---

# CLI Basics

> **Last revised:** 2026-05-30

> Source-backed Logseq CLI basics for working with DB graphs from the terminal.

## Scope

This page matches the current command surface implemented in `src/main/logseq/cli/`.

> **Removed:** The old `@logseq/cli` npm install/update/uninstall story is not current for this source-backed CLI.
> **Removed:** The old flat command model (`logseq list`, `logseq show my-graph`, `logseq export`, `logseq import-edn`, `logseq validate`, `logseq append`, `logseq mcp-server`) does not match the current command table.
> **Removed:** Positional graph syntax such as `logseq search my-graph "term"` and `logseq query my-graph '[:find ...]'` is no longer current; use `--graph`.
> **Removed:** The older `-a/--api-server-token` workflow for graph operations is not part of the current source-backed CLI surface.

## Findings: New Capabilities

- `graph create --enable-sync` can create a graph, upload it, and start sync in one command.
- `graph backup list/create/restore/remove` is part of the current graph management surface.
- `graph export --type sqlite` is supported alongside EDN export.
- `query list` exposes built-in queries such as `task-search`, `recent-updated`, `list-status`, and `list-priority`.
- `show` now supports `--page-hierarchy`, `--linked-references`, and `--ref-id-footer`.
- `sync download --progress` streams realtime download progress when requested.
- `agent bridge`, `completion`, `debug pull`, `skill show`, `skill install`, and `example` are current utility commands.

## Prerequisites

- Node.js 22.20.0 or higher
- A local DB graph under the CLI root directory

> **Removed:** Logseq Desktop installed is not required for local graph commands; the CLI starts and manages `db-worker-node` itself.

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
logseq sync status --graph my-graph
```

## Core Commands

### Listing

Current list subcommands:

- `list page`
- `list tag`
- `list property`
- `list task`
- `list node`
- `list asset`

### Search

Search is scoped by entity type and requires `--content`:

- `search block --graph my-graph --content "task"`
- `search page --graph my-graph --content "home"`
- `search property --graph my-graph --content "owner"`
- `search tag --graph my-graph --content "quote"`

### Query

Current query shapes:

- `query --graph my-graph --query '[:find ?title :where [?p :block/title ?title]]'`
- `query --graph my-graph --name recent-updated --inputs '[30]'`
- `query list --graph my-graph`

### Show

`show` is a tree inspector, not a graph metadata command:

- `show --graph my-graph --page Home`
- `show --graph my-graph --page Home --page-hierarchy true`
- `show --graph my-graph --id 123 --level 2`
- `show --graph my-graph --uuid 11111111-1111-1111-1111-111111111111`

### Upsert and remove

- `upsert page --graph my-graph --page Home`
- `upsert block --graph my-graph --content "New block"`
- `upsert task --graph my-graph --page Home --status doing`
- `remove page --graph my-graph --page Home`

Use `upsert block` for generic tag/property mutation. Use `upsert task` for task-specific status and priority fields.

## Graph Management

### Graph create, info, export, import

- `graph create --graph my-graph`
- `graph create --graph my-graph --enable-sync --e2ee-password "my-secret"`
- `graph info --graph my-graph`
- `graph export --graph my-graph --type edn --file /tmp/my-graph.edn --include-timestamps --exclude-built-in-pages --exclude-namespaces user,project`
- `graph export --graph my-graph --type sqlite --file /tmp/my-graph.sqlite`
- `graph import --graph my-graph --type edn --input /tmp/my-graph.edn`
- `graph validate --graph my-graph`

### Backups

- `graph backup list --graph my-graph`
- `graph backup create --graph my-graph --name nightly`
- `graph backup restore --src my-graph-nightly --dst my-graph-restore`
- `graph backup remove --src my-graph-nightly`

### Server and sync

- `server list`
- `server cleanup`
- `server start --graph my-graph`
- `server stop --graph my-graph`
- `server restart --graph my-graph`
- `sync status --graph my-graph`
- `sync start --graph my-graph --e2ee-password "my-secret"`
- `sync upload --graph my-graph`
- `sync download --graph my-graph --progress`
- `sync asset download --graph my-graph --id 123`
- `sync remote-graphs`
- `sync ensure-keys`
- `sync grant-access --graph my-graph --graph-id <uuid> --email teammate@example.com`
- `sync config set ws-url wss://sync.example`
- `sync config get ws-url`
- `sync config unset ws-url`

`login` and `logout` manage persisted cloud auth in `~/logseq/auth.json`.

## Utilities

- `agent bridge --graph my-graph`
- `completion zsh`
- `debug pull --uuid 11111111-1111-1111-1111-111111111111 --graph my-graph`
- `skill show`
- `skill install --global`
- `example upsert page`

## Working Modes

### Local Mode

Local graph commands work directly against the DB graph on disk and start `db-worker-node` as needed.

### Connected Mode

Sync and login flows use the configured cloud and sync settings, with `~/logseq/auth.json` as the persisted auth file.

## Notes

- `graph info` is the current graph metadata command.
- `graph export` supports EDN and SQLite output.
- `query list` includes built-in and custom queries.
- `sync download` can stream progress when `--progress` is enabled.
- `show` is for entity trees, not graph metadata.

## Troubleshooting

### "Graph not found"

```bash
logseq graph list
logseq graph info --graph exact-graph-name
```

### "Cannot connect to Logseq"

- Ensure the graph exists and the CLI root directory is correct.
- Check the relevant sync config with `logseq sync config get`.
- Use `logseq login` when a cloud-authenticated sync flow is required.

### "Invalid query syntax"

```bash
logseq query --graph my-graph --query '[:find ?t :where [?b :block/title ?t]]'
logseq query --graph my-graph --name task-search --inputs '[:logseq.property/status.todo ""]'
```
