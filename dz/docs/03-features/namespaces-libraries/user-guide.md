# Library and Namespace User Guide (Validated)

> **Last revised:** 2026-05-29

## Scope

This guide documents user-facing Library and slash namespace behavior that is currently implemented.

Validation sources for this revision:

- `src/main/`
- `deps/`
- `packages/`

## Current Behavior (Verified)

### Library page (DB graphs)

- The built-in Library page name is `Library`.
- Library detection and membership are DB-level behaviors, not just UI labels.
- The Library add-pages UI is implemented and uses search constrained for library workflows.
- Search supports a dedicated `:library-page-search?` option to avoid adding invalid candidates.

### Slash namespace pages

- Slash detection is centralized (`namespace-page?`) and excludes URL and relative-path patterns.
- DB page hierarchy uses parent links (`:block/parent`) for page namespace structure.
- Slash namespace parent creation is handled in outliner page creation when `:split-namespace? true`.
- Namespace parent creation validates page/class parent compatibility and throws user-facing validation errors when invalid.

### Search behavior

- SQLite FTS5 trigram indexing is used for block/page search.
- Namespace queries can match by last path segment for slash pages.

## Claim Audit

Status legend: `ACCURATE`, `STALE`, `REMOVED`, `NEW`.

1. `Library is a built-in page available in DB graphs.`
   Status: `ACCURATE`

2. `You can add existing pages to Library from Library UI search and they become Library children.`
   Status: `ACCURATE`

3. `Slash titles create hierarchical namespace relationships.`
   Status: `ACCURATE`

4. `Creating slash pages auto-materializes missing parent pages in create flows.`
   Status: `ACCURATE`

5. `DB slash hierarchies are parent-based and rendered from ancestor chain, not from legacy file-only namespace model assumptions.`
   Status: `ACCURATE`

6. `In DB graphs, slash pages may require manually setting parent to create hierarchy.`
   Status: `STALE`
   Updated: DB creation path enables namespace splitting through `:split-namespace? true` and builds parent chain automatically for slash page creation flows.

7. `File graphs provide a dedicated namespace hierarchy sidebar component at src/main/frontend/components/file_based/hierarchy.cljs.`
   Status: `REMOVED`

8. `Renaming a namespace parent always cascades by renaming all child page titles.`
   Status: `STALE`
   Updated: current rename op is a page title save operation. Hierarchy display is driven by parent chain and title-with-parents rendering. Do not assume a legacy bulk child-title rewrite flow.

9. `Search supports namespace last-part matching (for slash pages).`
   Status: `ACCURATE`

10. `Search uses FTS5 trigram tokenizer.`
    Status: `ACCURATE`

11. `Most DB search flows exclude built-in pages unless explicitly requested.`
    Status: `ACCURATE`

12. `Developer reference is library-and-namespace-developer-guide.md.`
    Status: `STALE`
    Updated: repository guide is `dz/docs/03-features/namespaces-libraries/developer-guide.md`.

## Removed References (Explicit)

> **Removed:** `src/main/frontend/components/file_based/hierarchy.cljs`.

> **Removed:** Legacy assumptions that namespace hierarchy UI is implemented through a file-based hierarchy component path.

## Findings: New Capabilities

The following capabilities were not captured clearly in the prior user guide and are now part of the implementation:

1. Slash namespace creation is consolidated in outliner page creation (`deps/outliner/src/logseq/outliner/page.cljs`) via `:split-namespace?`, including parent materialization and validation.
2. Namespace creation enforces parent-type constraints:
   - page namespaces require page parents,
   - class namespaces require class parents,
   - invalid parent type combinations raise user-facing validation errors.
3. Namespace creation behavior is covered with dedicated tests in `deps/outliner/test/logseq/outliner/page_test.cljs`, including multi-level creation, shared child names under different parents, class/page incompatibility checks, and slash-formatted journal behavior.
4. Library add-page UI uses i18n keys (`:library/add-pages`, `:library/add-existing-pages`) in the current implementation.
5. Library search mode filters candidates using `:library-page-search?` with built-in/page constraints for safer add-to-library flows.

## Practical User Guide (Current)

### Library workflows (DB graphs)

1. Open search (`Cmd/Ctrl + K`) and navigate to `Library`.
2. Use the add-pages action on the Library page.
3. Search and select existing pages to move them under Library.
4. Remove a page from Library by clearing/changing its parent relationship; the page itself is not deleted.

### Namespace workflows

1. Create a slash page such as `Projects/Work/Client-A`.
2. In DB create flows, namespace parents are created and linked as parent chain.
3. Use page breadcrumb/path rendering to navigate ancestor hierarchy.
4. Keep hierarchies shallow (usually 2-3 levels) for readability.

### Search tips

1. For slash pages, searching by the last segment can still return namespace pages.
2. Use full path terms for precision.
3. Built-in pages are commonly filtered out in generic search unless explicitly included.

## Troubleshooting (Validated)

### I cannot find the Library page

- Confirm you are using a DB graph.
- Search for `Library` directly.

### Slash page did not become a hierarchy

- Ensure the page title actually contains `/` with no stray formatting.
- In DB graphs, create flows with namespace splitting enabled will build parent chain; if a page was created outside that flow, parent links may not match expected hierarchy.

### Rename produced unexpected namespace results

- Treat rename as page-title update behavior.
- Verify parent links and current breadcrumb path rather than assuming a legacy child-title rewrite model.

## Summary

- Library fundamentals and add-page workflow remain accurate.
- Namespace behavior is parent-chain based in current DB runtime creation.
- File-based hierarchy component references from older docs are removed.
- User guidance has been updated to match current implementation and tests.
