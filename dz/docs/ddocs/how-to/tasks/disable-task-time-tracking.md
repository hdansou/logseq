---
title: 'Stop Task Time Tracking in DB Graphs'
status: 🟢
since-version: 'v65.0'
quadrant: 'How-To'
last-verified: '2026-09-11'
verified-against: 'be800f1711'
review-status: audited
graph-type: [db]
platform: 'All'
related-docs:
  - 'tutorials/task-management/todo-basics.md'
  - 'reference/data/properties-reference.md'
  - 'explanation/concepts/file-vs-db.md'
---

This guide shows how to stop Logseq from recording new task status history in a DB graph, which also stops new status changes from feeding the task time shown next to a task.

DB graphs have no graph-wide time-tracking setting. The time shown on a task is calculated from the task's recorded `Status` history ([`src/main/frontend/worker/handler/query.cljs:49-95`](../../../../../src/main/frontend/worker/handler/query.cljs)). That history is written only for properties that have `Enable property history` turned on ([`src/main/frontend/worker/commands.cljs:26-31`](../../../../../src/main/frontend/worker/commands.cljs), [`:256-273`](../../../../../src/main/frontend/worker/commands.cljs)). The built-in `Status` and `Priority` properties have it turned on by default ([`deps/db/src/logseq/db/frontend/property.cljs:319-321`](../../../../../deps/db/src/logseq/db/frontend/property.cljs), [`:338-339`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

## Before you start

This guide assumes:

1. You are using a DB graph.
2. You want to stop future task time tracking, not remove history that was already recorded.

## Turn off status property history

1. Open the `Status` property page. Either search for `Status`, or click the `Status` property on any task and choose `Go to this property` ([`src/main/frontend/components/property/config.cljs:969-977`](../../../../../src/main/frontend/components/property/config.cljs)).
2. In the property page's properties, find `Enable property history`. It is a checkbox property that applies only to properties ([`deps/db/src/logseq/db/frontend/property.cljs:635-640`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).
3. Uncheck it.

`Enable property history` is not in the `Configure property` menu. That menu has no item for it and hides the default-value item for it ([`src/main/frontend/components/property/config.cljs:890-895`](../../../../../src/main/frontend/components/property/config.cljs)).

## Check the result

After you uncheck `Enable property history` on `Status`:

1. New status changes are no longer recorded. The history command skips any property whose `Enable property history` value is not `true` ([`src/main/frontend/worker/commands.cljs:259`](../../../../../src/main/frontend/worker/commands.cljs)).
2. Existing history is kept. Tasks that already have history still show a time next to the title, and you can click it to open `Status history` ([`src/main/frontend/components/block.cljs:3329-3344`](../../../../../src/main/frontend/components/block.cljs)).
3. A task whose last recorded change was to `Doing` keeps gaining time. The calculation adds the time from that `Doing` entry until now ([`src/main/frontend/worker/handler/query.cljs:79-82`](../../../../../src/main/frontend/worker/handler/query.cljs)), and later changes are no longer recorded to stop it. Move those tasks out of `Doing` before you turn history off.

## Notes

- This is a setting on one property, not a graph-wide setting. It does not affect `Priority`, which has its own `Enable property history` value.
- If you still need status history for auditing or workflow review, leave this setting on.
- This guide does not remove existing status history. It only prevents new history from being recorded.
- Logseq also skips recording history for undo, rebase and sync or graph-download transactions ([`src/main/frontend/worker/pipeline.cljs:515-518`](../../../../../src/main/frontend/worker/pipeline.cljs)).
