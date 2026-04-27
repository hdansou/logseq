# Discord post — April 26, 2026

Logseq update for March 23 to April 26 is out.

This release focuses on three things:

- Sync is more reliable, including better recovery for encrypted graphs and support for custom self-hosted sync server URLs.
- The CLI keeps growing into a real graph-management tool, with task, node, asset, backup, search, browser-open, and sync-debug workflows.
- Large graphs feel faster, with better indexing feedback, cheaper view rendering, and safer large-page loading.

A few user-facing highlights:

- You can now open an asset directly from the block context menu.
- There is a new `yyyy-MM-dd EEE` journal date format.
- Dark theme is preserved when installing non-theme plugins.
- A long list of fixes landed for page controls, template insertion, drag-and-drop assets, desktop image copying, and recycle flows.

If you use sync heavily, this release also hardens checksum, replay, download, and graph-switching behavior, especially around encrypted and self-hosted setups.

If you use the CLI, a lot of rough edges were removed too: output is easier to read, completions go deeper, and several bugs around partial updates, built-in nodes, property handling, and sync errors are fixed.

Full changelog: [changelog_2026-03-23_to_2026-04-26.md](changelog_2026-03-23_to_2026-04-26.md)