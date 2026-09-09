# Logseq CLI - Current User Guide

> Command-line interface for Logseq database graphs
> **Last revised:** 2026-05-29

## Scope

This guide matches the current source-backed CLI in `src/main/logseq/cli/`.

> **Removed:** The old `@logseq/cli` npm install/update/uninstall story is not part of the current source-backed CLI in this repository.
> **Removed:** Positional graph syntax such as `logseq search my-graph "term"` and `logseq query my-graph '[:find ...]'` is no longer current; use `--graph`.
> **Removed:** Top-level `export-edn`, `import-edn`, and `validate` commands do not exist in the current command table.
> **Removed:** `logseq list` and `logseq show GRAPH` as graph-level commands do not match the current parser; use `graph list` and `show --graph ...`.

## Findings: New Capabilities

- `graph backup list/create/restore/remove`
- `graph export --type sqlite`
- `graph create --enable-sync` with `--e2ee-password`
- `query list` plus built-in queries (`task-search`, `recent-updated`, `list-status`, `list-priority`)
- `show --page-hierarchy`, `--linked-references`, and `--ref-id-footer`
- `sync download --progress`
- `agent bridge`, `completion`, `example`, and `skill show/install`

## Quick Start

```bash
logseq --help
logseq graph list
logseq graph info --graph my-graph
logseq list page --graph my-graph
logseq search page --graph my-graph --content "meeting notes"
logseq query --graph my-graph --name recent-updated --inputs '[30]'
logseq show --graph my-graph --page Home
logseq sync status --graph my-graph
```

## Global Flags

The current global flags are:

| Flag               | Meaning                                | Default              |
| ------------------ | -------------------------------------- | -------------------- |
| `--help` / `-h`    | Show help                              | off                  |
| `--version`        | Show version                           | off                  |
| `--config`         | Path to `cli.edn`                      | `<root-dir>/cli.edn` |
| `--graph` / `-g`   | Target graph                           | none                 |
| `--root-dir`       | CLI root directory                     | `~/logseq`           |
| `--timeout-ms`     | Request timeout in ms                  | `10000`              |
| `--output` / `-o`  | Output format (`human`, `json`, `edn`) | `human`              |
| `--verbose` / `-v` | Enable debug logging to stderr         | off                  |
| `--profile`        | Print stage timing to stderr           | off                  |

Environment variables recognized by the current source include `LOGSEQ_CLI_GRAPH`, `LOGSEQ_CLI_ROOT_DIR`, `LOGSEQ_CLI_TIMEOUT_MS`, `LOGSEQ_CLI_OUTPUT`, `LOGSEQ_CLI_CONFIG`, `LOGSEQ_CLI_LOGIN_TIMEOUT_MS`, and `LOGSEQ_CLI_LOGOUT_TIMEOUT_MS`.

## Defaults

| Resource           | Default                  |
| ------------------ | ------------------------ |
| CLI root directory | `~/logseq`               |
| Graph storage root | `<root-dir>/graphs`      |
| CLI config file    | `<root-dir>/cli.edn`     |
| Server list file   | `<root-dir>/server-list` |
| Auth file          | `~/logseq/auth.json`     |

## Command Reference

### Graph inspection and editing

- `list page`
- `list tag`
- `list property`
- `list task`
- `list node`
- `list asset`
- `upsert block`
- `upsert page`
- `upsert task`
- `upsert asset`
- `upsert tag`
- `upsert property`
- `remove block`
- `remove page`
- `remove tag`
- `remove property`
- `query`
- `query list`
- `search block`
- `search page`
- `search property`
- `search tag`
- `show`

### Graph management

- `graph list`
- `graph create`
- `graph switch`
- `graph remove`
- `graph validate`
- `graph info`
- `graph export`
- `graph import`
- `graph backup list`
- `graph backup create`
- `graph backup restore`
- `graph backup remove`
- `server list`
- `server cleanup`
- `server start`
- `server stop`
- `server restart`
- `doctor`
- `sync status`
- `sync start`
- `sync stop`
- `sync upload`
- `sync download`
- `sync asset download`
- `sync remote-graphs`
- `sync ensure-keys`
- `sync grant-access`
- `sync config set`
- `sync config get`
- `sync config unset`

### Authentication and utilities

- `login`
- `logout`
- `agent bridge`
- `completion`
- `debug pull`
- `skill show`
- `skill install`
- `example <selector...>`

## Common Syntax

```bash
# Search
logseq search page --graph my-graph --content "home"

# Query built-in and custom queries
logseq query --graph my-graph --name task-search --inputs '[:logseq.property/status.done "daily"]'
logseq query list --graph my-graph

# Show a page or entity tree
logseq show --graph my-graph --page Home
logseq show --graph my-graph --id 123 --level 2
logseq show --graph my-graph --uuid 11111111-1111-1111-1111-111111111111

# Graph export and import
logseq graph export --graph my-graph --type edn --file /tmp/my-graph.edn --include-timestamps
logseq graph export --graph my-graph --type sqlite --file /tmp/my-graph.sqlite
logseq graph import --graph my-graph --type edn --input /tmp/my-graph.edn

# Backups
logseq graph backup list --graph my-graph
logseq graph backup create --graph my-graph --name nightly

# Sync
logseq sync status --graph my-graph
logseq sync download --graph my-graph --progress
```

## Notes

- `search` requires `--content` and the scope must be one of `block`, `page`, `property`, or `tag`.
- `show` is a tree inspector, not a graph-metadata command; use `graph info` for graph metadata.
- `query --name` resolves built-ins and `:custom-queries` from `cli.edn`.
- `graph export` supports both EDN and SQLite output, with EDN-only flags rejected for SQLite exports.
- `sync download` supports realtime progress streaming when `--progress` is enabled.
