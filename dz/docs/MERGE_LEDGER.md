# ddocs merge ledger

Merging root `ddocs/` (A) into `dz/docs/ddocs/` (B). Destination is B: it is named in
`progress.json`, carries the audit `lastVerified` stamps, and keeps every personal doc under `dz/`.

Content is merged by reading both versions, not by picking a winner on date or length.
Regenerate the underlying comparison with
`dz/support-engineering/script/doc-inventory/compare_trees.py ddocs dz/docs/ddocs`.

Legend: `[ ]` pending · `[~]` in progress · `[x]` merged · `[S]` skipped

## Stage 1 — move A-only files (46)

Done — 46 moved, 19 source links re-depthed, 0 links broken by the move.

No content decisions; `git mv` plus a source-link depth fix (`../../../` → `../../../../../`).

- [x] `explanation/architecture/block-editor.md`
- [x] `explanation/architecture/database-integrity.md`
- [x] `explanation/architecture/plugin-security.md`
- [x] `explanation/architecture/schema-system.md`
- [x] `explanation/concepts/bidirectional-properties.md`
- [x] `explanation/concepts/block-editing-philosophy.md`
- [x] `explanation/concepts/block-tags-vs-page-tags.md`
- [x] `explanation/concepts/file-organization.md`
- [x] `explanation/concepts/file-vs-database.md`
- [x] `explanation/concepts/graph-visualization.md`
- [x] `explanation/concepts/linked-references.md`
- [x] `explanation/concepts/local-first-architecture.md`
- [x] `explanation/concepts/plugin-architecture.md`
- [x] `explanation/concepts/tag-properties-vs-page-properties.md`
- [x] `how-to/content/hide-tags-from-node.md`
- [x] `how-to/integrations/external-vector-index.md`
- [x] `how-to/optimization/performance-optimization.md`
- [x] `how-to/organization/backup-strategies.md`
- [x] `how-to/organization/import-export.md`
- [x] `how-to/tasks/disable-task-time-tracking.md`
- [x] `how-to/troubleshooting/troubleshooting-guide.md`
- [x] `how-to/workflows/daily-workflows.md`
- [x] `how-to/workflows/meeting-notes-workflow.md`
- [x] `how-to/workflows/research-workflow.md`
- [x] `how-to/workflows/task-management-workflows.md`
- [x] `how-to/workflows/zettelkasten-method.md`
- [x] `reference/advanced/query-patterns.md`
- [x] `reference/advanced/query-system.md`
- [x] `reference/api/db-worker-http-api.md`
- [x] `reference/api/thread-api.generated.md`
- [x] `reference/data/properties-reference.md`
- [x] `reference/features/asset-sync.md`
- [x] `reference/features/sync-limits.md`
- [x] `reference/formatting/markdown-syntax.md`
- [x] `reference/interface/keyboard-shortcuts.md`
- [x] `reference/interface/slash-commands.md`
- [x] `reference/interface/templates-system.md`
- [x] `reference/platforms/default-paths.md`
- [x] `reference/platforms/mobile-differences.md`
- [x] `reference/platforms/uri-schemes.md`
- [x] `tutorials/advanced/advanced-queries-tutorial.md`
- [x] `tutorials/advanced/custom-theme-tutorial.md`
- [x] `tutorials/advanced/workflow-automation-tutorial.md`
- [x] `tutorials/integrations/build-a-db-worker-ui.md`
- [x] `tutorials/integrations/mcp-integration.md`
- [x] `tutorials/plugins/first-plugin.md`

## Stage 2 — content merges (47), smallest diff first

| ✔ | Δ lines | A lines | A date | B lines | B date | File |
|---|---|---|---|---|---|---|
| [ ] | 39 | 301 | 2026-01-29 | 313 | 2026-05-30 | `how-to/integrations/custom-css.md` |
| [ ] | 40 | 170 | — | 183 | — | `tutorials/getting-started/01-installation.md` |
| [ ] | 54 | 330 | 2026-01-29 | 325 | 2026-03-10 | `reference/syntax/dynamic-variables.md` |
| [ ] | 56 | 384 | 2026-01-29 | 395 | 2026-03-10 | `reference/configuration/css-variables.md` |
| [ ] | 59 | 372 | 2026-01-29 | 370 | 2026-03-10 | `reference/api/http-api.md` |
| [ ] | 74 | 354 | 2026-01-29 | 383 | 2026-03-10 | `reference/configuration/config-edn-db.md` |
| [ ] | 79 | 189 | 2026-01-29 | 209 | 2026-05-30 | `explanation/architecture/system-overview.md` |
| [ ] | 80 | 393 | 2026-01-29 | 414 | 2026-03-10 | `tutorials/task-management/project-planning.md` |
| [ ] | 84 | 464 | — | 478 | — | `tutorials/getting-started/04-linking-pages.md` |
| [ ] | 92 | 313 | 2026-01-29 | 371 | 2026-05-30 | `how-to/content/view-types.md` |
| [ ] | 99 | 549 | 2026-01-29 | 541 | 2026-03-10 | `reference/configuration/config-edn.md` |
| [ ] | 100 | 401 | — | 416 | — | `tutorials/getting-started/03-basic-editing.md` |
| [ ] | 107 | 258 | 2026-01-29 | 291 | 2026-05-30 | `explanation/comparisons/rtc-vs-file-sync.md` |
| [ ] | 108 | 245 | 2026-01-29 | 255 | 2026-05-30 | `how-to/content/quick-capture.md` |
| [ ] | 116 | 291 | 2026-01-29 | 316 | 2026-05-30 | `explanation/concepts/file-vs-db.md` |
| [ ] | 125 | 227 | — | 229 | — | `tutorials/getting-started/02-first-graph.md` |
| [ ] | 130 | 310 | 2026-09-09 | 312 | 2026-05-30 | `how-to/integrations/x-callback-url.md` |
| [ ] | 133 | 350 | 2026-01-29 | 377 | 2026-05-30 | `how-to/integrations/custom-js.md` |
| [ ] | 156 | 395 | 2026-01-29 | 410 | 2026-03-10 | `reference/database/schema-overview.md` |
| [ ] | 179 | 368 | 2026-01-29 | 414 | 2026-05-30 | `explanation/architecture/data-architecture.md` |
| [ ] | 179 | 478 | — | 481 | — | `reference/syntax/code-blocks.md` |
| [ ] | 196 | 642 | — | 658 | — | `how-to/multimedia/video-embed.md` |
| [ ] | 210 | 722 | — | 714 | — | `reference/features/pdf-annotations.md` |
| [ ] | 281 | 453 | — | 404 | — | `how-to/content/favorites-recent.md` |
| [ ] | 283 | 376 | 2026-01-29 | 451 | 2026-05-30 | `how-to/queries/advanced-queries.md` |
| [ ] | 291 | 305 | 2026-01-29 | 231 | 2026-05-30 | `how-to/backup/backup-restore.md` |
| [ ] | 299 | 760 | 2026-09-09 | 889 | — | `how-to/multimedia/asset-management.md` |
| [ ] | 313 | 305 | 2026-01-29 | 245 | 2026-03-10 | `tutorials/task-management/todo-basics.md` |
| [ ] | 314 | 364 | 2026-01-29 | 302 | 2026-05-30 | `tutorials/knowledge-management/flashcards-basics.md` |
| [ ] | 342 | 121 | — | 229 | — | `README.md` |
| [ ] | 346 | 533 | — | 530 | — | `tutorials/collaboration/real-time-sync.md` |
| [ ] | 353 | 301 | 2026-01-29 | 327 | 2026-03-10 | `reference/configuration/keyboard-shortcuts.md` |
| [ ] | 376 | 523 | — | 510 | — | `how-to/tasks/repeating-tasks.md` |
| [ ] | 380 | 370 | 2026-01-29 | 364 | 2026-05-30 | `how-to/integrations/create-theme.md` |
| [ ] | 384 | 319 | 2026-01-29 | 240 | 2026-05-30 | `how-to/content/create-flashcard-deck.md` |
| [ ] | 400 | 749 | — | 793 | — | `explanation/security/e2ee-encryption.md` |
| [ ] | 408 | 434 | 2026-01-29 | 289 | 2026-03-10 | `reference/features/flashcards.md` |
| [ ] | 425 | 533 | — | 596 | — | `how-to/integrations/zotero.md` |
| [ ] | 462 | 314 | 2026-01-29 | 198 | 2026-05-30 | `how-to/integrations/cli-basic.md` |
| [ ] | 497 | 433 | 2026-01-29 | 404 | 2026-03-10 | `reference/api/mcp-server.md` |
| [ ] | 502 | 161 | 2026-09-06 | 360 | 2026-05-30 | `explanation/features/how-embeddings-work.md` |
| [ ] | 512 | 149 | 2026-09-06 | 382 | 2026-05-30 | `tutorials/getting-started/semantic-search.md` |
| [ ] | 593 | 620 | — | 267 | — | `reference/features/user-profiles.md` |
| [ ] | 643 | 741 | — | 453 | — | `explanation/features/handbooks-system.md` |
| [ ] | 803 | 352 | 2026-09-06 | 485 | 2026-03-10 | `reference/api/semantic-search-api.md` |
| [ ] | 822 | 579 | — | 273 | 2026-04-24 | `how-to/integrations/publishing.md` |
| [ ] | 2147 | 1138 | 2026-01-29 | 1229 | 2026-03-10 | `reference/api/plugin-api.md` |

Total changed lines: 14671

## Stage 3 — B-only files (10): no action, they stay

- [S] `explanation/features/how-publishing-works.md`
- [S] `reference/features/calculator.md`
- [S] `reference/plugin-apis/pr-12395/01-postmate-message-channel.md`
- [S] `reference/plugin-apis/pr-12395/02-hosted-renderer.md`
- [S] `reference/plugin-apis/pr-12395/03-sidebar-renderer.md`
- [S] `reference/plugin-apis/pr-12395/04-app-get-current-route.md`
- [S] `reference/plugin-apis/pr-12395/05-block-properties-class-defaults.md`
- [S] `reference/plugin-apis/pr-12395/06-helpers-to-common-rename.md`
- [S] `reference/plugin-apis/pr-12395/README.md`
- [S] `tutorials/queries/charts-with-hiccup.md`

## Stage 4 — finish

- [ ] Merge the two `README.md` indexes (A 121 lines, B 229)
- [ ] Retire root `ddocs/`
- [ ] Rewrite inbound `../../../ddocs/` references under `dz/docs/`
- [ ] Link check: zero broken links across the merged tree
- [ ] Triage the **258 pre-existing broken links** found by `check_links.py` (none caused by the merge)
- [ ] Regenerate `TASK_LIST.md` from the merged inventory
