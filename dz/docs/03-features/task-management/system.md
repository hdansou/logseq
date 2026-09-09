# Task Management System

> **Last revised:** 2026-05-29

## Overview

Logseq task management is currently DB-native in active runtime paths, with markdown/task-marker compatibility preserved for import/export and markdown mirroring.

Core task model is defined by built-in class/property schema:

- Task class: `:logseq.class/Task`
- Status property: `:logseq.property/status`
- Priority property: `:logseq.property/priority`
- Date properties: `:logseq.property/scheduled`, `:logseq.property/deadline`

Source of truth:

- `deps/db/src/logseq/db/frontend/class.cljs`
- `deps/db/src/logseq/db/frontend/property.cljs`

## Current Task Model

### Status Closed Values

Built-in status closed values are currently:

- Backlog (`:logseq.property/status.backlog`)
- Todo (`:logseq.property/status.todo`)
- Doing (`:logseq.property/status.doing`)
- In Review (`:logseq.property/status.in-review`)
- Done (`:logseq.property/status.done`)
- Canceled (`:logseq.property/status.canceled`)

These are defined in:

- `deps/db/src/logseq/db/frontend/property.cljs`

### Priority Closed Values

Priority closed values are DB closed values (not legacy A/B/C markers):

- Low
- Medium
- High
- Urgent

Defined in:

- `deps/db/src/logseq/db/frontend/property.cljs`

## State Transitions and Interaction

### Keyboard Transition

Task cycling is implemented via `:editor/cycle-todo` (`mod+enter`), and currently cycles:

- Todo -> Doing
- Doing -> Done
- Done -> Todo
- Unset -> Todo

Implemented in:

- `src/main/frontend/modules/shortcut/config.cljs`
- `src/main/frontend/handler/editor.cljs`

### Slash and Property Commands

Task-related slash commands are generated from current closed values and property actions, not a hardcoded legacy marker list.

Validated paths:

- Status slash actions: `src/main/frontend/commands.cljs`
- Priority slash actions: `src/main/frontend/commands.cljs`
- Deadline/Scheduled slash actions: `src/main/frontend/commands.cljs`
- Property keyboard bindings:
  - status: `p s`
  - priority: `p p`
  - deadline: `p d`
  - defined in `src/main/frontend/modules/shortcut/config.cljs`

## Query Integration

### Query DSL Task and Priority Filters

Task and priority query operators are implemented through Query DSL builders and DB rules:

- `(task ...)` uses rule `task`
- `(priority ...)` uses rule `priority`

Validated in:

- `src/main/frontend/db/query_dsl.cljs`
- `deps/db/src/logseq/db/frontend/rules.cljc`

### Date Filtering

For scheduled/deadline range semantics, `between` remains the reliable range mechanism in Query DSL.

Validated in:

- `src/main/frontend/db/query_dsl.cljs`

## Recurring Tasks

Recurring tasks are actively represented in DB properties and worker commands.

Validated behavior:

- Repeat metadata is preserved through import/export paths.
- Worker command `:repeated-task` can reschedule and reset status when done.

Validated in:

- `src/main/frontend/worker/commands.cljs`
- `deps/graph-parser/test/logseq/graph_parser/exporter_test.cljs`

## Compatibility and Legacy Markers

Legacy markers (for example NOW/LATER/WAITING variants) are still handled in compatibility paths (not as primary DB closed values).

Examples of compatibility evidence:

- Marker-to-status aliasing in CLI add/update flows.
- Legacy task marker preservation/import tests.

Validated in:

- `src/main/logseq/cli/command/add.cljs`
- `deps/graph-parser/test/logseq/graph_parser/exporter_test.cljs`

> **Removed:** Runtime documentation claiming a dedicated file-graph marker engine with workflow cycling implemented in `deps/common/src/logseq/common/marker.cljs`.
>
> That source path does not exist in current code. Current active cycling logic is DB-status based in editor handlers.

> **Removed:** Documentation claim that `Cmd/Ctrl+Shift+Enter` creates a new task and `t t` is quick task creation.
>
> Current bindings are `mod+enter` for task status cycling, while `t t` is theme toggle.

> **Removed:** Documentation claim that `:preferred-workflow :todo|:now` drives active task workflow in DB graphs.
>
> `:preferred-workflow` is marked file-only/deprecated for DB graphs in config metadata.

## Findings: New Capabilities

The following capabilities are present in current source and were missing from the previous version of this document:

1. Logseq CLI task listing with normalized status/priority filters and content search:
   - `logseq list task --status ... --priority ... --content ...`
   - `src/main/logseq/cli/command/list.cljs`
2. Built-in CLI query templates for task/status workflows:
   - `task-search`, `list-status`, `list-priority`
   - `src/main/logseq/cli/command/query.cljs`
3. Task listing in db-worker common path based on `:logseq.class/Task` plus status/priority/deadline/scheduled payload fields:
   - `src/main/logseq/cli/common/db_worker.cljs`

## Notes

- Keep this doc focused on validated runtime behavior and avoid speculative internal snippets.
- When updating task states or closed values, re-validate against `deps/db/src/logseq/db/frontend/property.cljs` and `src/main/frontend/handler/editor.cljs`.
