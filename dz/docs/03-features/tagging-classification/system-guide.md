# Tagging and Classification System

> **Last revised:** 2026-05-29

## Scope

This guide documents current behavior verified in source under src, deps, and packages.

- Database graphs: tags are class entities, with inheritance and class-defined properties.
- File-based graphs: this document does not redefine legacy hashtag semantics.
- Core implementation is in deps and src. No core tagging/classification engine logic was found in packages.

## Verified Architecture

### Built-in class model

Built-in classes are defined in `logseq.db.frontend.class/built-in-classes` and include:

- :logseq.class/Root
- :logseq.class/Tag
- :logseq.class/Property
- :logseq.class/Page
- :logseq.class/Journal
- :logseq.class/Whiteboard (marked deprecated in code comment)
- :logseq.class/Task
- :logseq.class/Comments
- :logseq.class/Comment
- :logseq.class/Query
- :logseq.class/Card
- :logseq.class/Cards
- :logseq.class/Asset
- :logseq.class/Code-block
- :logseq.class/Quote-block
- :logseq.class/Math-block
- :logseq.class/Pdf-annotation
- :logseq.class/Template

Related class sets are also defined there: `page-classes`, `internal-tags`, `private-tags`, `block-kind-tags`, `disallowed-inline-tags`, `extends-hidden-tags`, and `hidden-tags`.

### Inheritance

Inheritance is represented by `:logseq.property.class/extends` and resolved recursively via rule `class-extends` in `logseq.db.frontend.rules`.

- Downward traversal helper: `logseq.db.frontend.class/get-structured-children`
- Upward traversal helper: `logseq.db.frontend.class/get-class-extends`
- Class object retrieval including descendants: `logseq.db.frontend.class/get-class-objects`

### API surface (DB based)

Public DB API functions in `src/main/logseq/api/db_based.cljs` include:

- `get-all-tags`, `get-tag`, `get-tags-by-name`, `get-tag-objects`
- `create-tag` (supports optional `tagProperties` upsert/attach)
- `add-tag-extends`, `remove-tag-extends` (built-in tags protected)
- `tag-add-property`, `tag-remove-property`
- `add-block-tag`, `remove-block-tag`

Input constraints are enforced (for example, non-blank title and no slash for `create-tag`).

### Validation and normalization pipeline

Tag/class integrity is actively repaired/normalized in worker code:

- `src/main/frontend/worker/pipeline.cljs`
  - `fix-page-tags`: auto-add missing `:db/ident` and root extends for new class tags; retract incompatible `:logseq.class/Page` tagging combinations.
  - `fix-inline-built-in-page-classes`: strips inline page-class tags from title and retracts those tags.
  - `ensure-query-property-on-tag-additions`: ensures blocks tagged as `:logseq.class/Query` have a query property value block.
  - `ensure-comments-blocks-property-on-tag-additions`: initializes comments blocks linkage when tagging with `:logseq.class/Comments`.
- `src/main/frontend/worker/db/validate.cljs`
  - Repairs invalid class/tag states (missing class extends, non-class entries in `:block/tags`, class/property misuse patterns).

### Property typing relevant to classification

`deps/db/src/logseq/db/frontend/property/type.cljs` defines user property types as:

- `:default :number :date :datetime :checkbox :url :node :asset`

`malli_schema.cljs` validates tags and properties with class-aware constraints.

### Schema version

Current schema version in source is `65.33` (`deps/db/src/logseq/db/frontend/schema.cljs`), with migrations up to `65.33` in `src/main/frontend/worker/db/migrate.cljs`.

## Findings: New Capabilities

Compared to the previous revision of this guide, the following capabilities are now explicitly present in current code and should be documented:

1. Built-in comment classes `:logseq.class/Comments` and `:logseq.class/Comment` with migration support.
2. Automatic query property bootstrapping when a block is tagged as `:logseq.class/Query`.
3. Automatic comments linkage initialization when a block is tagged as `:logseq.class/Comments`.
4. User property type list includes `:asset`.
5. Schema/migration head is `65.33` (not `65.22`).

## Findings: Removed or Stale Claims

> **Removed:** The prior document included large hypothetical implementations (for example `create-class-from-usage`, visual query builder, cache/gc subsystems, hybrid migration toggles, and portability import/export workflows) that are not present as concrete production code in current src/deps/packages.

> **Removed:** Prior “comprehensive” query examples and pseudo-rules were mixed with non-authoritative snippets and should not be treated as implementation documentation.

> **Removed:** The previously stated schema head `65.22` is stale; source now declares `65.33`.

## Practical Notes

1. In DB graphs, class identity is grounded on entity/class predicates and `:db/ident` namespace conventions (user class idents are generated via `create-user-class-ident-from-name`).
2. Built-in/private class constraints are enforced both in API calls and in validation/repair passes.
3. Query/property behavior depends on class-aware rules (not only direct block property presence), especially through `object-has-class-property?` and `class-extends`.
