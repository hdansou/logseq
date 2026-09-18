---
title: 'Editor.getBlockProperties class defaults'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
---

# `Editor.getBlockProperties`: class default values are now merged in

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

## Summary

PR [#12395](https://github.com/logseq/logseq/pull/12395) (`a95483655b`) changed three Editor APIs
to return the **effective** properties of a block. The effective properties are the block's own
properties merged with the default values its classes (tags) define:

- `logseq.Editor.getBlockProperties`
- `logseq.Editor.getPageProperties`, which delegates to it
- `logseq.Editor.getBlockProperty`

Before the PR, `get_block_properties` read only the block's own `:block/properties` (see the
removed lines in `git show a95483655b -- src/main/logseq/api/editor.cljs`).

This is DB-graph behavior. The class and default-value attributes are DB-graph schema.

Declarations:
[`libs/src/LSPlugin.ts:928-930`](../../../../../../libs/src/LSPlugin.ts).

## Behavior

At `be800f1711`, all three APIs go through `get-all-block-properties`
([`src/main/logseq/api/editor.cljs:480-489`](../../../../../../src/main/logseq/api/editor.cljs)):

1. Fetch the block from the DB worker and read its `:block/properties` (`editor.cljs:482-483`).
   The worker builds that map from the block's own property datoms, starting from `{}`
   ([`src/main/frontend/worker/handler/property.cljs:559-577`](../../../../../../src/main/frontend/worker/handler/property.cljs),
   assoc'd at
   [`src/main/frontend/worker/handler/block.cljs:399-404`](../../../../../../src/main/frontend/worker/handler/block.cljs)).
   A block with no own properties therefore yields an empty map, not `nil`, and still gets class
   defaults.
2. Ask the worker for class defaults. The API calls `db-async/<get-block-class-default-properties`
   (`editor.cljs:476-478`;
   [`src/main/frontend/db/async.cljs:136-138`](../../../../../../src/main/frontend/db/async.cljs)),
   which runs `:thread-api/get-block-class-default-properties` (worker `property.cljs:179-192`).
3. The worker collects properties from every class on the block **and every class those classes
   extend**:
   - The walk is `outliner-property/get-block-classes-properties`
     ([`deps/outliner/src/logseq/outliner/property.cljs:1020-1034`](../../../../../../deps/outliner/src/logseq/outliner/property.cljs)).
   - Parent classes come from `ldb/get-classes-parents` via `get-class-extends`
     ([`deps/db/src/logseq/db/frontend/db.cljs:91-96`](../../../../../../deps/db/src/logseq/db/frontend/db.cljs)).
4. It keeps only properties that have `:logseq.property/default-value`, keyed by the property's
   `:db/ident`. Ref defaults are converted with `entity-util/entity->map` (worker
   `property.cljs:186-191`). A property that has only `:logseq.property/scalar-default-value` is
   **not** included, because that attribute is never read here.
5. Merge **class defaults first; own properties override**:
   `(merge classes-properties own-properties)` (`editor.cljs:486-488`).

### Output shape of `getBlockProperties`

The merged map goes through `api-block/into-readable-db-properties`
([`src/main/logseq/api/block.cljs:73-77`](../../../../../../src/main/logseq/api/block.cljs)). That
calls `readable-properties` with `{:original-key? true :key-fn str}`
([`src/main/frontend/handler/db_based/property/util.cljs:12-37`](../../../../../../src/main/frontend/handler/db_based/property/util.cljs)),
and the result goes to `sdk-utils/result->js` (`editor.cljs:518-522`). The resulting JS object has
these properties:

- **Keys are full idents as strings, with the leading colon.** `key-fn str` turns
  `:logseq.property/status` into `":logseq.property/status"`. The keys are not short names like
  `status`.
- **Map values are reduced to display content.** `property-value-content` returns `:block/title`,
  or else `:logseq.property/value`
  ([`deps/db/src/logseq/db/frontend/property.cljs:836-841`](../../../../../../deps/db/src/logseq/db/frontend/property.cljs)).
  Set values become sets of that content, and other values pass through unchanged.

`getPageProperties` resolves the page and returns `get_block_properties` for its UUID
(`editor.cljs:524-528`).

## Example

The built-in `Task` class lists `:logseq.property/status` in its schema
([`deps/db/src/logseq/db/frontend/class.cljs:39-41`](../../../../../../deps/db/src/logseq/db/frontend/class.cljs)).
`:logseq.property/status` has `:logseq.property/default-value :logseq.property/status.todo`
(`property.cljs:319-321`). That closed value's title is `"Todo"` (`property.cljs:315`).

For a block tagged `#Task` with no status set:

```ts
const props = await logseq.Editor.getBlockProperties(blockUuid)
// { ":logseq.property/status": "Todo", ... }
```

> **Unverified:** the output above is derived by reading the code path, not by running it against a
> live graph. Two points were not confirmed:
>
> - That the `Task` class schema at `class.cljs:39-41` materializes as
>   `:logseq.property.class/properties` on the class entity, which is the attribute the walk reads.
> - The exact plain form `worker-plain/attribute-value->plain` gives a block's *own* ref-valued
>   properties before `readable-properties` runs (worker `property.cljs:481-483`).

If a block sets a property itself, that value replaces the class default for the same ident.

## `getBlockProperty` also changed

`getBlockProperty(block, key)` now reads the same merged map (`editor.cljs:496-516`). It looks up
the key by sanitized name, by keyword, or by resolved ident (`editor.cljs:501-505`). A property that
is unset on the block but has a class default therefore returns the default.

Map values are returned with `:block/value` and `:db/ident` added (`editor.cljs:506-511`). Sets are
converted to vectors before serialization (`editor.cljs:491-494`, `:512-513`).

> **`getBlockProperties` does not do that conversion.** Its merged map goes through
> `readable-properties`, which keeps many-valued properties as sets
> (`src/main/frontend/handler/db_based/property/util.cljs:12-37`), and then straight into
> `result->js` (`editor.cljs:518-522`; `src/main/logseq/sdk/utils.cljs:113-118`), which has no set
> handling of its own — `property-refs->ids` keeps sets as sets (`sdk/utils.cljs:69`). The comment
> beside `getBlockProperty`'s conversion says cljs-bean does not turn sets into plain JavaScript
> values (`editor.cljs:493`). So a many-valued property may not arrive as an array from
> `getBlockProperties` even though the same property does from `getBlockProperty`. The code path is
> verified; the JavaScript value was not reproduced at runtime. Read many-valued properties with
> `getBlockProperty` until that is confirmed.

## What hasn't changed

- Writing and removing properties still act on the block, not the class. For example,
  `remove_block_property` calls `property-handler/remove-block-property!` on the block's UUID
  (`editor.cljs:470-474`).
- The merge only fills in **unset** properties. It never replaces a value the block has.

> **Unverified:** earlier revisions claimed that on file-based graphs these calls return only the
> block's own properties. Whether file graphs still reach this code at `be800f1711` was not
> confirmed. The API layer adds no graph-type branch here (`editor.cljs:480-528`).

## Possible Logseq defect: set values in `getBlockProperties`

`get_block_property` converts set values to vectors before `result->js`, citing
"cljs-bean does not convert ClojureScript sets to plain JavaScript values" (`editor.cljs:491-494`).
`get_block_properties` does not. `readable-properties` turns set values into sets (`util.cljs:26-27`),
and `result->js` passes them to `bean/->js` without conversion
([`src/main/logseq/sdk/utils.cljs:113-118`](../../../../../../src/main/logseq/sdk/utils.cljs)).
Many-valued properties may therefore not serialize to plain arrays through `getBlockProperties`.
Not reproduced.

## Source

- `get-block-classes-properties-has-default-value`: `src/main/logseq/api/editor.cljs:476-478`.
- `get-all-block-properties`: `editor.cljs:480-489`.
- `get_block_property`: `editor.cljs:496-516`; `get_block_properties`: `editor.cljs:518-522`;
  `get_page_properties`: `editor.cljs:524-528`.
- Worker default collection: `src/main/frontend/worker/handler/property.cljs:179-192`.
- Class and property walk: `deps/outliner/src/logseq/outliner/property.cljs:1020-1034`.
- History:
  - The PR's version called `outliner-property/get-block-classes-properties` directly in
    `editor.cljs`.
  - The call moved behind the DB worker in `fe65e36f13 refactor: remove ui db` (2026-07-08).
  - PR-branch commit, squashed into `a95483655b` and not an ancestor of master:
    `10106c1b8a fix(apis): simplify block property retrieval and enhance default value handling`.
