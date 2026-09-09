---
title: 'Stop Task Time Tracking in DB Graphs'
status: 🟢
since-version: 'v65.0'
quadrant: 'How-To'
last-verified: '2026-04-28'
platform: 'All'
related-docs:
  - 'tutorials/task-management/todo-basics.md'
  - 'reference/data/properties-reference.md'
  - 'explanation/concepts/file-vs-db.md'
---

This guide shows how to stop Logseq from recording new task status history in a DB graph, which also stops new task time from accumulating.

## Before you start

This guide assumes:

1. You are using a DB graph.
2. You can edit built-in properties in that graph.
3. You want to stop future task time tracking, not remove history that was already recorded.

## Turn off status property history

1. Open the built-in `Status` property in your graph.
2. Open the property settings or edit view for `Status`.
3. Find `Enable property history`.
4. Turn it off or set it to `false`.
5. Save the property change if Logseq prompts you to confirm it.

## Check the result

After you disable `Enable property history` on `Status`:

1. New task status changes are no longer recorded in property history.
2. New task time stops accumulating from those status changes.
3. Existing history is kept, so older tracked time may still appear on tasks that already have history.

## Notes

- This is a property-level setting in DB graphs, not a graph-wide timetracking setting.
- If you still need status history for auditing or workflow review, leave this setting enabled.
- This guide does not remove existing status history. It only prevents new history from being recorded.
