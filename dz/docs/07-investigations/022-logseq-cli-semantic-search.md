# Logseq CLI Semantic Search Investigation

> **Last revised:** 2026-05-29

## Question

Does `logseq search "todo"` use semantic search when semantic search is enabled on a graph?

## Answer

No. The current CLI search path does not use the semantic/vector search stack. It requires an explicit search subcommand plus `--content`, then runs a Datascript substring query over block, page, property, or tag titles.

## Current Behavior

The CLI entry point is [src/main/logseq/cli/command/search.cljs](../../../src/main/logseq/cli/command/search.cljs). It defines four subcommands only: `search block`, `search page`, `search property`, and `search tag`. Each one builds a query from `--content`, sends it through `:thread-api/q`, and normalizes UUID labels in returned titles.

Recycled block and page entries are filtered out in the CLI path. Property and tag search do not apply that recycle filter.

## Findings: New Capabilities

- The CLI now exposes explicit `search block`, `search page`, `search property`, and `search tag` subcommands.
- CLI search normalizes UUID references in titles before printing results.
- Block and page search suppress recycled entries so soft-deleted content does not surface.

## Removed

> **Removed:** `logseq search "todo"` automatically uses semantic search when semantic search is enabled on a graph.

> **Removed:** The CLI search command uses a hybrid keyword/semantic ranking model.

> **Removed:** The semantic search flow described in the old draft belongs to the frontend search stack, not the CLI command path.

## Code References

- [src/main/logseq/cli/command/search.cljs](../../../src/main/logseq/cli/command/search.cljs)
- [src/main/logseq/cli/commands.cljs](../../../src/main/logseq/cli/commands.cljs)
- [src/main/logseq/cli/format.cljs](../../../src/main/logseq/cli/format.cljs)
- [src/main/frontend/search.cljs](../../../src/main/frontend/search.cljs)
- [src/main/frontend/search/agency.cljs](../../../src/main/frontend/search/agency.cljs)
- [src/main/frontend/worker/search.cljs](../../../src/main/frontend/worker/search.cljs)

## Summary

CLI search is title/name substring search with explicit subcommands. Semantic search remains available in the frontend search stack, but it is not part of the `logseq search` CLI path.
