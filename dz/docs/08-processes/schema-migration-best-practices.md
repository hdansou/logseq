# Schema Migration Best Practices

> **Last revised:** 2026-05-30

**Document Type:** Process Guide
**Validated Against:** `src/`, `deps/`, and `packages/` on 2026-05-30
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

This guide reflects the current DB-graph migration path implemented in:

- [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs)
- [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs)
- [deps/db/src/logseq/db/sqlite/create_graph.cljs](../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)
- [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs)
- [src/test/frontend/worker/migrate_test.cljs](../../../src/test/frontend/worker/migrate_test.cljs)

As of this revision, the current frontend DB schema version is `65.34`.

**Critical rule:** if a change affects persisted DB-graph data, update the built-in definition, add or adjust the migration entry, and bump `db-schema/version` together.

---

## Current Workflow

### 1. Define the schema entity

Add new built-in properties in [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs) and new built-in classes in [deps/db/src/logseq/db/frontend/class.cljs](../../../deps/db/src/logseq/db/frontend/class.cljs).

For a new property, the live source still centers the definition on:

```clojure
{:db/ident :logseq.property.example/my-property
 :title "My Property"
 :schema {:type :default
          :cardinality :one
          :public? true
          :hide? false
          :view-context :page}
 :queryable? true}
```

Use the current user-facing property types from `logseq.db.frontend.property.type/user-built-in-property-types`:

- `:default`
- `:number`
- `:date`
- `:datetime`
- `:checkbox`
- `:url`
- `:node`
- `:asset`

Internal-only types still exist, but they are not the normal path for user-visible schema additions.

### 2. Add a migration entry

Add an entry to `schema-version->updates` in [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs).

The current handwritten source supports these migration map keys:

```clojure
["65.X" {:properties [:logseq.property.example/my-property]
         :classes [:logseq.class/Example]
         :fix some-fix-fn
         :delete-properties [:legacy/property]}]
```

What each key does today:

- `:properties`: creates built-in property pages from `built-in-properties`
- `:classes`: creates built-in classes from `built-in-classes`
- `:fix`: runs a function of `[db]` that returns tx-data
- `:delete-properties`: removes obsolete properties and their stored values

### 3. Bump the schema version

Update `version` in [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs):

```clojure
(def version (parse-schema-version "65.34"))
```

The migration entry and `db-schema/version` must advance together.

### 4. Update validation only where the live code validates it

Graph-wide validation still lives in [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs), but property-value validation for edits is currently built through:

- [deps/outliner/src/logseq/outliner/property.cljs](../../../deps/outliner/src/logseq/outliner/property.cljs)
- [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)
- [deps/db/src/logseq/db/frontend/property/type.cljs](../../../deps/db/src/logseq/db/frontend/property/type.cljs)

If a new property needs custom value semantics, confirm the change in the outliner/property validation path, not only in `frontend/validate.cljs`.

### 5. Add focused tests

Target the migration slice you changed. Current repo examples live in [src/test/frontend/worker/migrate_test.cljs](../../../src/test/frontend/worker/migrate_test.cljs).

Typical checks:

- migrated schema version updated correctly
- new property or class exists after migration
- deleted properties are actually removed when using `:delete-properties`
- fix-function data changes are preserved

---

## Version Management

Schema versions are parsed and stored as maps such as `{:major 65 :minor 33}`, not as plain strings in the DB.

Relevant kv entities currently include:

- `:logseq.kv/schema-version`
- `:logseq.kv/graph-initial-schema-version`
- `:logseq.kv/remote-schema-version`

`migrate` in [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs) applies every migration newer than the graph version and not newer than the requested target version, then runs `ensure-built-in-data-exists!` as a final repair pass.

Compatibility rules that are directly visible in source today:

- newer app versions can migrate older graphs forward
- an older client is blocked from editing a graph with a newer schema version
- RTC tracks `:logseq.kv/remote-schema-version`, and its kv doc states RTC will not start when major schema versions differ

---

## Migration Patterns In Current Source

### Simple property addition

```clojure
["65.X" {:properties [:logseq.property.example/my-property]}]
```

Use this when the entity can be created entirely from the built-in definition.

### Class and property addition together

```clojure
["65.X" {:classes [:logseq.class/Example]
         :properties [:logseq.property.example/items]}]
```

This matches the current migration mechanism, which can build both in one version step.

### Fix-only migration

```clojure
["65.X" {:fix repair-existing-data}]

(defn repair-existing-data [db]
  [[:db/retract entity-id :bad/attr bad-value]])
```

Best practices for `:fix` functions in the current codebase:

- take `[db]`
- return tx-data
- do not transact directly
- tolerate partial or legacy data
- stay idempotent when practical

### Property deletion

```clojure
["65.X" {:delete-properties [:legacy/property]}]
```

This is now a first-class migration capability. The live example is `65.25` in [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs).

---

## Property Changes

### Adding a new property

Use this checklist:

- add the definition to `built-in-properties`
- add a `schema-version->updates` entry
- bump `db-schema/version`
- add focused tests for the touched migration step
- verify any edit-time validation path if the property has special value rules

### Changing a property type

The current outliner update path still rejects type changes when existing data is present. Treat type changes as migrations to a new property rather than in-place mutation.

Preferred pattern:

1. Add a new property with the new type.
2. Use a `:fix` function to copy or transform old data.
3. Update call sites to the new ident.
4. Remove the old property later with `:delete-properties` once the graph no longer depends on it.

### RTC notes

Do not assume every new property needs a three-key RTC config. In current handwritten source, the shared helper in [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs) only sets `:rtc/ignore-attr-when-syncing`, and source-level sync filtering in [src/main/frontend/worker/sync/const.cljs](../../../src/main/frontend/worker/sync/const.cljs) also keys off `:rtc/ignore-attr-when-syncing`.

Match nearby built-ins and the specific sync path you are changing instead of cargo-culting a larger RTC map.

---

## Bootstrap Considerations

Bootstrap still matters because migrations and new-graph creation must agree on the same built-in shape.

The live order in [deps/db/src/logseq/db/sqlite/create_graph.cljs](../../../deps/db/src/logseq/db/sqlite/create_graph.cljs) is:

1. bootstrap class ids for `Root`, `Property`, `Tag`, `Page`, and `Template`
2. initial kv data, including schema-version markers and `:logseq.property/empty-placeholder`
3. initial properties, with four bootstrap properties handled specially
4. remaining built-in classes
5. initial files
6. default built-in pages
7. hidden pages

The current bootstrap properties are:

- `:logseq.property/type`
- `:logseq.property/hide?`
- `:logseq.property/built-in?`
- `:logseq.property/created-from-property`

New graphs currently also include these built-in files:

- `logseq/config.edn`
- `logseq/custom.css`
- `logseq/custom.js`
- `logseq/publish.css`
- `logseq/publish.js`

And these hidden/built-in pages are created in bootstrap:

- `Views`
- `Favorites`
- `Recycle`

If you change bootstrap order or the built-in sets, validate both new-graph creation and forward migration behavior.

---

## Testing Guidance

Use the existing migration tests in [src/test/frontend/worker/migrate_test.cljs](../../../src/test/frontend/worker/migrate_test.cljs) as the first pattern, then validate graph integrity with the DB validator when needed.

Useful current validation surfaces:

- migration-specific assertions in `src/test/frontend/worker/migrate_test.cljs`
- create-graph and export/import validation tests under `deps/db/test/`
- graph-wide validation via `validate-local-db!` in [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs)

Minimum expectation for a schema migration change:

- test the exact version boundary you changed
- test the newly created or deleted entities
- test preservation of existing user data affected by the migration

---

## Findings: New Capabilities

Compared with the older process document, the current source adds or clearly exposes these capabilities:

- `:delete-properties` is now a supported migration primitive in `schema-version->updates`
- `ensure-built-in-data-exists!` runs after normal version upgrades to repair missing built-in entities without overwriting preserved kv data such as graph creation metadata
- bootstrap now creates `publish.css` and `publish.js` for new DB graphs
- bootstrap now explicitly creates `Recycle` alongside `Views` and `Favorites`
- migration coverage in [src/test/frontend/worker/migrate_test.cljs](../../../src/test/frontend/worker/migrate_test.cljs) includes deletion and repair scenarios, not only additive upgrades

---

## Removed Or Stale Guidance

> **Removed:** `:rename-db-idents` is not part of the current handwritten migration implementation in [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs). The supported path today is add new property or class, migrate data with `:fix`, then optionally remove old properties later with `:delete-properties`.
> **Removed:** the example migration runner named `migrate-db-graph` does not exist in current source. The live entry point is `migrate` in [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs).
> **Removed:** the old `compatible-schema-version?` example is not present in current handwritten source. Current compatibility behavior is enforced through `parse-schema-version`, `compare-schema-version`, and the guard logic inside `migrate`.
> **Removed:** the previous document’s claim that migrations end with a validation pass is too strong for the current implementation. `migrate` applies version updates and then `ensure-built-in-data-exists!`; it does not call a final graph validator itself.
> **Removed:** the earlier "always add explicit three-key RTC config" rule is not an accurate description of the current source path. Confirm the actual sync behavior you need instead of assuming all three RTC keys are required in handwritten source.
> **Removed:** property-specific validation is not primarily extended through `defmethod property-value-schema` in [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs). Current property-value validation is assembled through the outliner and malli-schema path described above.

---

## Quick Reference Checklist

- add or adjust the built-in definition
- add the migration entry using only currently supported keys
- bump `db-schema/version`
- update validation only on the live validation path you touched
- write a focused migration test
- confirm new-graph bootstrap still matches the migrated built-in shape when relevant

---

## Changelog

| Date       | Changes                                                                                                                                                                |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2026-05-30 | Revalidated against current `src/`, `deps/`, and `packages/`; removed stale rename and validation guidance; added current deletion, repair, and bootstrap capabilities; updated user property-type list to include `:asset` |
| 2025-12-28 | Initial version based on investigation findings                                                                                                                        |
