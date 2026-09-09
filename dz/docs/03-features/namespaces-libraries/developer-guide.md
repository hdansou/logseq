# Library and Namespace Developer Guide (Validated)

> **Last revised:** 2026-05-29

## Scope

This guide documents how Library and slash-based namespace behavior currently works in the codebase.

Validation sources for this revision:

- `src/main/`
- `deps/`
- `packages/`

## Current Architecture (Verified)

### Library page foundation

- Library name constant is defined as `"Library"` in `deps/common/src/logseq/common/config.cljs`.
- Namespace delimiter and detection helpers are in `deps/common/src/logseq/common/util/namespace.cljs` (`namespace-page?`, `get-last-part`, `parent-char` = `"/"`).
- Library page detection for DB entities is in `deps/db/src/logseq/db/frontend/db.cljs` (`library?`).
- Library page retrieval and membership checks are in `deps/db/src/logseq/db.cljs` (`get-library-page`, `page-in-library?`).
- Parent-chain title rendering excludes the built-in Library ancestor in `deps/db/src/logseq/db/frontend/db.cljs` (`get-title-with-parents`).

### UI and search

- Library UI is implemented in `src/main/frontend/components/library.cljs`.
- The add-pages workflow uses search with `:library-page-search? true` and `editor-handler/move-blocks!`.
- Search implementation supports `:library-page-search?` filtering in `src/main/frontend/worker/search.cljs`.
- FTS5 trigram indexing is used in `src/main/frontend/worker/search.cljs`.

### Namespace creation model (current)

- Slash page splitting is implemented in `deps/outliner/src/logseq/outliner/page.cljs` with `:split-namespace?` and `split-namespace-pages`.
- Namespace parents for normal pages are modeled with `:block/parent` (not `:block/namespace`) in current runtime creation flows.
- Namespace parent creation attaches the top parent under Library when no explicit parent exists (`page-with-parent-and-order`).
- Guardrails for invalid namespace parent types are implemented in `split-namespace-pages` and covered by tests.

## Claim Audit

Status legend: `ACCURATE`, `STALE`, `REMOVED`, `NEW`.

### Core claims from prior revision

1. `Library page is a built-in page named "Library".`  
   Status: `ACCURATE`

2. `Library page UUID is derived from built-in UUID generation and resolves to 00000004-1294-7765-6000-000000000000.`  
   Status: `ACCURATE`

3. `namespace-page?` identifies slash pages and rejects relative paths/URLs.  
   Status: `ACCURATE`

4. `DB page hierarchy uses :block/parent and get-page-parents.`  
   Status: `ACCURATE`

5. `Title rendering omits Library from displayed parent chain.`  
   Status: `ACCURATE`

6. `Library UI supports adding existing pages via search and move operation.`  
   Status: `ACCURATE`

7. `Search has a :library-page-search? mode.`  
   Status: `ACCURATE`

8. `SQLite FTS trigram support is used for search.`  
   Status: `ACCURATE`

9. `Namespace pages are primarily represented by :block/namespace in frontend runtime model.`  
   Status: `STALE`  
   Updated: current runtime namespace hierarchy creation is parent-based (`:block/parent`) via outliner page creation.

10. `Namespace query APIs are implemented in src/main/frontend/common/file_based/db.cljs and src/main/frontend/db/file_based/model.cljs.`  
    Status: `REMOVED`

11. `Namespace rename logic is implemented in src/main/frontend/worker/handler/page/file_based/rename.cljs.`  
    Status: `REMOVED`

12. `Plugin API exports get_pages_from_namespace and get_pages_tree_from_namespace.`  
    Status: `REMOVED`

13. `Migration fix-rename-parent-to-extends exists and is registered at schema 65.1.`  
    Status: `REMOVED`

14. `Namespace hierarchy UI component is at src/main/frontend/components/file_based/hierarchy.cljs.`  
    Status: `REMOVED`

15. `Top-level migration path details in prior revision (property rename + parent move logic in fix-rename-parent-to-extends).`  
    Status: `STALE`  
    Updated: current `src/main/frontend/worker/db/migrate.cljs` migration list starts at newer schema updates and does not include that function.

16. `Deprecated {{namespace}} macro warning exists in UI.`  
    Status: `ACCURATE`

### Removed references (explicit)

> **Removed:** `logseq.db.cljs` at repository root. The active file is `deps/db/src/logseq/db.cljs`.

> **Removed:** `src/main/frontend/common/file_based/db.cljs`.

> **Removed:** `src/main/frontend/db/file_based/model.cljs`.

> **Removed:** `src/main/frontend/worker/handler/page/file_based/rename.cljs`.

> **Removed:** `src/main/frontend/components/file_based/hierarchy.cljs`.

## Findings: New Capabilities

The following capabilities were not captured in the prior revision and are now part of the implementation:

1. Slash namespace creation is consolidated in outliner page creation (`deps/outliner/src/logseq/outliner/page.cljs`) via `:split-namespace?`, including parent materialization and validation.
2. Namespace creation enforces parent-type constraints:
   - page namespaces require page parents,
   - class namespaces require class parents,
   - invalid parent type combinations throw user-facing validation errors.
3. Namespace creation behavior is covered with dedicated tests in `deps/outliner/test/logseq/outliner/page_test.cljs`, including:
   - multi-level creation,
   - shared child names under different parents,
   - class/page incompatibility checks,
   - slash-formatted journal titles not creating namespace parents.
4. Library add-page UI now uses i18n keys (`:library/add-pages`, `:library/add-existing-pages`) instead of hardcoded English strings.
5. API surface in `src/main/logseq/api.cljs` is now focused on current editor/db/db-based/CLI APIs and no longer exports legacy namespace list/tree plugin helpers.

## Practical Developer Notes (Current)

### Detect library and ancestry

Use:

- `logseq.db/library?`
- `logseq.db/get-library-page`
- `logseq.db/page-in-library?`
- `logseq.db/get-title-with-parents`

### Create slash hierarchy pages

Use page creation with `:split-namespace? true` through the outliner page flow (`frontend.worker.handler.page/create!` -> `logseq.outliner.page/create!`).

### Search behavior for Library workflows

Use search options including:

- `:library-page-search? true`
- `:page-only? true`
- `:built-in? false`

for add-existing-pages-to-Library selectors.

## Summary

- Library fundamentals remain intact and accurate.
- Prior file-based namespace model references and namespace plugin API references are obsolete.
- Namespace creation logic has shifted to consolidated outliner flows with stronger validation and tests.
