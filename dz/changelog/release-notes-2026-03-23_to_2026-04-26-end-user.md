# What's new — April 26, 2026 (End-user focus)

This version is written for people who primarily use the app itself rather than the CLI. The headline is simple: everyday editing is steadier, synced graphs are safer, and large graphs feel less heavy.

---

## Editing gets out of your way more often

This release fixes a dense set of frustrating editing issues: page-title controls now behave more consistently across platforms, journal titles keep the right non-editable behavior while still showing the caret correctly, task status no longer flickers, and template insertion handles dynamic variables properly again.

Large pages are more stable too. Linked references load more safely, deferred rendering is less likely to break the layout, and block indentation and macro rendering issues were cleaned up.

Recycled and deleted content also behaves better. Restoring pages is more reliable, recycle-related wording is clearer, and page cleanup is less likely to leave the UI in a confusing state.

---

## Sync is easier to trust

If you use sync, this release hardens a lot of the failure paths that used to be hard to reason about: duplicate uploads, stale history operations, checksum drift, replay after undo/redo, and graph download cleanup are all substantially better handled now.

If you use encrypted graphs, recovery is much safer when keys are missing, stale, or invalid. Logseq now bootstraps encryption state more carefully and handles decrypt failures more gracefully instead of spiraling into a broken state.

If you self-host sync, you can now configure a custom sync server URL directly.

---

## Larger graphs feel more responsive

Search indexing is faster and now shows progress while it runs. Big pages render in a safer order, linked references can wait until the main content is ready, and heavy graph views do less work before showing useful results.

The result is not just speed in benchmarks. The app feels more understandable while it is busy, especially during first index or when opening a page with a lot of content.

---

## Small improvements you will notice

- You can open an asset directly from the block context menu
- A new `yyyy-MM-dd EEE` journal format is available
- Dark theme is preserved when installing non-theme plugins
- Tooltips, sidebars, and error screens were polished
- Drag-and-drop assets and desktop image copying work correctly again
- Slash commands and other UI text respect localization more consistently

---

## What this release is not about

There is a lot of CLI work in this release too, but this version intentionally keeps the focus on the in-app experience. If you want the terminal and sync-heavy angle, review the CLI-and-sync variant alongside this one.

_Full technical changelog: [changelog_2026-03-23_to_2026-04-26.md](changelog_2026-03-23_to_2026-04-26.md)_