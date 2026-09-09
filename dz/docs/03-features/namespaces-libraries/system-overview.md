# Namespaces and Library System Overview (Validated)

> **Last revised:** 2026-05-29

## Scope

This overview documents how namespace and Library behavior currently works in the repository.

Validation sources for this revision:

- `src/`
- `deps/`
- `packages/`

## Current Architecture (Verified)

### Library foundation

- Built-in Library name is defined as `"Library"` in `deps/common/src/logseq/common/config.cljs`.
- Library page detection is implemented in `deps/db/src/logseq/db/frontend/db.cljs` (`library?`).
- Library retrieval and membership checks are implemented in `deps/db/src/logseq/db.cljs` (`get-library-page`, `page-in-library?`).
- Parent-chain title rendering hides the built-in Library ancestor in `deps/db/src/logseq/db/frontend/db.cljs` (`get-title-with-parents`).

### Namespace fundamentals

- Namespace delimiter helpers are implemented in `deps/common/src/logseq/common/util/namespace.cljs`:
  - `namespace-char` = `"/"`
  - `parent-char` = `"/"`
  - `namespace-page?` rejects `../`, `./`, URL-like values, and bare `/`.
- Slash namespace creation is handled in `deps/outliner/src/logseq/outliner/page.cljs` via `:split-namespace?` and `split-namespace-pages`.
- Current runtime hierarchy creation for slash pages is parent-based (`:block/parent`) through outliner page creation flows.

### UI and search behavior

- Library UI exists in `src/main/frontend/components/library.cljs`.
- Add-existing-pages flow uses search with `:library-page-search? true`, then `editor-handler/move-blocks!` to move selected pages under Library.
- Search supports `:library-page-search?` filtering in `src/main/frontend/worker/search.cljs`.
- Search index uses SQLite FTS5 trigram tokenizer in `src/main/frontend/worker/search.cljs` (`blocks_fts` with `tokenize="trigram"`).

### Related runtime behavior

- Page creation API path is `frontend.worker.handler.page/create! -> logseq.outliner.page/create!` (`src/main/frontend/worker/handler/page.cljs`).
- Pipeline contains Library-related normalization in `src/main/frontend/worker/pipeline.cljs` (`toggle-page-and-block`), including page/block conversion rules when moving under Library.

## Claim Audit

Status legend: `ACCURATE`, `STALE`, `REMOVED`, `NEW`.

### Core technical claims from prior revision

1. `Namespace delimiter is "/" and namespace-page? validates slash-based names.`  
   Status: `ACCURATE`

2. `Library is a built-in page named "Library".`  
   Status: `ACCURATE`

3. `Library membership can be determined from parent ancestry (page-in-library?).`  
   Status: `ACCURATE`

4. `Library UI supports adding existing pages through search and move operations.`  
   Status: `ACCURATE`

5. `Search includes a :library-page-search? mode.`  
   Status: `ACCURATE`

6. `Search uses SQLite FTS5 trigram indexing.`  
   Status: `ACCURATE`

7. `Namespace page hierarchy is represented primarily via :block/namespace relationships.`  
   Status: `STALE`  
   Updated: active namespace page creation flow is parent-based (`:block/parent`) in outliner page creation.

8. `Namespace query rules are implemented at deps/db/src/logseq/db/file_based/rules.cljc with :namespace/:namespace-pages/:namespace-hierarchy.`  
   Status: `REMOVED`

9. `Namespace hierarchy UI lives at src/main/frontend/components/file_based/hierarchy.cljs.`  
   Status: `REMOVED`

10. `Namespace route helpers (for example get-page-namespace-routes in file-based model flow) are part of current runtime implementation.`  
    Status: `REMOVED`

11. `Namespace query DSL operator and builder (for example build-namespace and (namespace "...")) are currently supported in the shown form.`  
    Status: `REMOVED`

12. `Migration function migrate-orphaned-pages-to-library exists in src/main/frontend/worker/db/migrate.cljs.`  
    Status: `REMOVED`

13. `Pipeline function auto-organize-new-pages exists in src/main/frontend/worker/pipeline.cljs.`  
    Status: `REMOVED`

14. `setup-namespace-hierarchy / add-to-library-templates are active pipeline functions.`  
    Status: `REMOVED`

15. `Plugin API exports get_pages_from_namespace and get_pages_tree_from_namespace.`  
    Status: `REMOVED`

16. `fix-rename-parent-to-extends migration is present in current schema migration flow.`  
    Status: `REMOVED`

17. `Library dashboard/drag-and-drop components shown in this document reflect current code.`  
    Status: `REMOVED`

18. `Custom namespace-config / library-config maps shown in this document are current runtime configuration.`  
    Status: `REMOVED`

19. `Examples using namespace query operator ((namespace "...")) are current guaranteed behavior.`  
    Status: `REMOVED`

20. `Library title path display excludes built-in Library parent.`  
    Status: `ACCURATE`

### Removed references (explicit)

> **Removed:** `deps/db/src/logseq/db/file_based/rules.cljc`.

> **Removed:** `src/main/frontend/components/file_based/hierarchy.cljs`.

> **Removed:** `src/main/frontend/common/file_based/db.cljs`.

> **Removed:** `src/main/frontend/db/file_based/model.cljs`.

> **Removed:** `src/main/frontend/worker/handler/page/file_based/rename.cljs`.

> **Removed:** Namespace plugin API exports `get_pages_from_namespace` and `get_pages_tree_from_namespace` in `src/main/logseq/api.cljs`.

> **Removed:** `migrate-orphaned-pages-to-library` in `src/main/frontend/worker/db/migrate.cljs`.

> **Removed:** `auto-organize-new-pages` and `setup-namespace-hierarchy` in `src/main/frontend/worker/pipeline.cljs`.

## Findings: New Capabilities

The following capabilities were not captured in the prior revision and are now part of the implementation:

1. Slash namespace materialization is consolidated in outliner page creation (`deps/outliner/src/logseq/outliner/page.cljs`) via `:split-namespace?`.
2. Namespace creation enforces parent-type guardrails:
   - non-class pages require page parents,
   - class pages require class parents,
   - invalid combinations throw user-facing validation errors.
3. Slash namespace creation behavior is covered by dedicated tests in `deps/outliner/test/logseq/outliner/page_test.cljs`, including:
   - multi-level parent creation,
   - same child titles under different parents,
   - class/page/property incompatibility checks,
   - slash journal format not creating namespace parent pages.
4. Library add-pages UI uses i18n keys (`:library/add-pages`, `:library/add-existing-pages`) in `src/main/frontend/components/library.cljs`.
5. Pipeline logic in `src/main/frontend/worker/pipeline.cljs` includes explicit page/block conversion behavior when moving content under Library (`toggle-page-and-block`).

## Summary

- Library foundations and search integration remain valid.
- Most legacy file-based namespace model references and namespace query/operator examples in the previous overview are obsolete.
- Current slash namespace creation lives in outliner flows with stronger validation and test coverage.
