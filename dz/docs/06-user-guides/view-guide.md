---
title: ':view Feature Investigation'
description: 'Archived investigation notes for custom query views'
status: archived
lastVerified: '2026-05-29'
relatedDocs:
  howTo:
    - '[View Functions](../../ddocs/how-to/content/view-types.md)'
    - '[Advanced Queries](../../ddocs/how-to/queries/advanced-queries.md)'
---

## Archived: `:view Feature Investigation`

> **Last revised:** 2026-05-29

This page has been archived.

The maintained docs for this topic are:

- [View Functions](../../ddocs/how-to/content/view-types.md)
- [Advanced Queries](../../ddocs/how-to/queries/advanced-queries.md)

Why this page was archived:

- It mixed correct behavior with outdated or unsupported claims.
- The canonical guidance already lives in the Diátaxis docs above.
- Keeping this page active would duplicate and confuse the source of truth.

> **Removed:** The old walkthrough-style claims about a single custom `:view` surface are stale. The current implementation routes query results through the shared view stack and supports table, list, and gallery view types.

## Findings: New Capabilities

- Query result views are rendered through `frontend.components.query.view/query-result`, which keeps entity IDs available for live updates.
- The shared view resolver in `deps/db/src/logseq/db/common/view.cljs` now recognizes `:all-pages`, `:class-objects`, `:property-objects`, `:linked-references`, `:unlinked-references`, and `:query-result` feature types.
- Table views support row selection, row numbers, sortable columns, pinned columns, per-view filters, and ordering metadata.
- Gallery views support asset-property selection, configurable display properties, and custom card sizing through preset or explicit width and height values.
- Advanced query inputs still resolve current-page, current-block, page-ref, and relative date or time keywords.

If you are looking for current behavior, use the linked docs and the code paths in `src/main/frontend/components/query.cljs`, `src/main/frontend/db/query_react.cljs`, and `src/main/frontend/components/query/view.cljs`.
