---
title: 'Keyboard Shortcuts (moved)'
description: 'This page has merged into the canonical keyboard shortcuts reference'
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.x'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  reference:
    - '[Keyboard Shortcuts Reference](../configuration/keyboard-shortcuts.md)'
---

# Keyboard Shortcuts (moved)

**This page has moved to [Keyboard Shortcuts Reference](../configuration/keyboard-shortcuts.md).**

Two pages carried the title "Keyboard Shortcuts Reference". They have been merged into the
`reference/configuration/` copy, which is keyed on command ids and is verified row by row against
`src/main/frontend/modules/shortcut/config.cljs`. This path is kept so existing links resolve.

## Why this copy was not the one kept

It listed shortcuts as key glyphs with no command ids, which made it unverifiable, and several of
its entries were wrong at `be800f1711` — including three of the ten in its own "Learn These First"
table:

| It claimed | Actually |
|---|---|
| `⌘ S` — "Save current block" | `:graph/db-save`, which saves the graph, not a block (`config.cljs:376`) |
| `⌘ J` — "Open today's journal" | `:editor/jump`, the jump-to-block navigation mode (`config.cljs:447`) |
| `⌘ /` — "Toggle between editing and view mode" | No such binding exists |

Its "Task Markers" section documented `TODO`, `DOING`, `DONE`, `LATER`, `NOW`, `WAITING` and
`CANCELLED` with `/todo`-style slash commands. Those are file-graph constructs; none of those
slash commands exists anywhere in `src/main/frontend/` at this revision. In a DB graph a task is a
block tagged `:logseq.class/Task` whose `:logseq.property/status` is a **ref** to one of six
closed values — `:logseq.property/status.backlog`, `.todo`, `.doing`, `.in-review`, `.done`,
`.canceled` (`deps/db/src/logseq/db/frontend/property.cljs:313-318`).

To open the shortcuts UI, press `t s` (`:ui/toggle-settings`, `config.cljs:423`) and choose Keymap.
