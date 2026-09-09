# Logseq Plugin Developer Guide: Database Graphs

> **Last revised:** 2026-05-29

**Validation scope:** `src/`, `deps/`, and `packages/` in this repository (checked 2026-05-29).

## Overview

Database (DB) graphs use structured entities and schema-aware properties instead of inline `key:: value` parsing. For plugin developers, this enables:

- schema-driven property definitions
- class/tag entities with relations
- richer query options (`q`, `customQuery`, `datascriptQuery`)
- reactive subscriptions for DB changes

## DB vs File-Based (Plugin-Relevant)

| Capability       | File-based graph    | DB graph                            |
| ---------------- | ------------------- | ----------------------------------- |
| Storage model    | Markdown/Org files  | SQLite-backed entity graph          |
| Properties       | Inline text parsing | Typed property entities + schema    |
| Tags/classes     | Text references     | Class entities and class APIs       |
| Query APIs       | Mostly DSL usage    | DSL + customQuery + Datascript      |
| Reactive updates | Limited             | `DB.onChanged`, `DB.onBlockChanged` |

## Getting Started

- Runtime graph check:
  - JavaScript/TypeScript: `await logseq.App.checkCurrentIsDbGraph()`
  - ClojureScript: `(app/check-current-is-db-graph)`
- Current SDK versions in this repo:
  - `@logseq/libs`: `0.3.3` (`libs/package.json`)
  - `@logseq/cljs-libs`: `0.0.12` (`libs/cljs-sdk/package.json`)

```ts
import '@logseq/libs'

async function main() {
  const isDb = await logseq.App.checkCurrentIsDbGraph()
  if (!isDb) {
    logseq.UI.showMsg('This feature requires a DB graph', 'warning')
    return
  }

  await logseq.Editor.upsertProperty('task-priority', { type: 'number' })
}

logseq.ready(main).catch(console.error)
```

## Verified Property APIs

### Property schema types

Current valid `type` values accepted by runtime validation:

- `default`
- `number`
- `date`
- `datetime`
- `checkbox`
- `url`
- `node`
- `asset`
- `json`
- `string`

`cardinality` supports `one` and `many`.

### Core property methods

- `Editor.getProperty(key)`
- `Editor.upsertProperty(key, schema?, opts?)`
- `Editor.removeProperty(key)`
- `Editor.getAllProperties()`
- `Editor.upsertBlockProperty(block, key, value, options?)`
- `Editor.removeBlockProperty(block, key)`
- `Editor.getBlockProperty(block, key)`
- `Editor.getBlockProperties(block)`
- `Editor.getPageProperties(page)`

### Namespacing and control

Property identifiers are normalized and plugin-scoped for upsert control. Runtime enforces that plugins upsert only their own property namespace.

## Verified Tag/Class APIs

The following Editor methods are currently exported and wrapped in both JS and CLJS SDKs:

- `Editor.getAllTags()`
- `Editor.getTagObjects(nameOrIdent)`
- `Editor.createTag(tagName, opts?)`
- `Editor.getTag(nameOrIdentOrId)`
- `Editor.getTagsByName(tagName)`
- `Editor.addTagProperty(tagId, propertyIdOrName)`
- `Editor.removeTagProperty(tagId, propertyIdOrName)`
- `Editor.addTagExtends(tagId, parentTagIdOrName)`
- `Editor.removeTagExtends(tagId, parentTagIdOrName)`
- `Editor.addBlockTag(blockId, tagId)`
- `Editor.removeBlockTag(blockId, tagId)`
- `Editor.setPropertyNodeTags(propertyId, tagIds)`

## Verified Query & Reactivity APIs

- `DB.q(dsl)`
- `DB.customQuery(query)`
- `DB.datascriptQuery(query, ...inputs)`
- `DB.onChanged(callback)`
- `DB.onBlockChanged(uuid, callback)`

```ts
const off = logseq.DB.onChanged(({ blocks, txData, txMeta }) => {
  console.log('changed blocks:', blocks.length)
  console.log('outliner op:', txMeta?.outlinerOp)
})

// later
off()
```

## Removed or Corrected Claims

> **Removed:** "For ClojureScript plugins: @logseq/cljs-libs 0.0.11+" as the current repo version is `0.0.12`.

> **Removed:** "Plugin API doesn't directly support setting closed values yet." Current API includes `Editor.addPropertyValueChoices(propertyId, choices)`.

> **Removed:** Example implying class constraints for `node` properties are configured via extra `upsertProperty(..., ..., opts)` class options. Current supported path is `Editor.setPropertyNodeTags(propertyId, tagIds)`.

> **Removed:** Claim that DB properties automatically coerce arbitrary strings to schema types (for example `'5'` to number) as a guaranteed behavior. Runtime conversion is explicit for some types (`json`, `string`) and otherwise validation/type handling is schema-driven.

## Findings: New Capabilities

1. Property type support explicitly includes `asset` in runtime schema checks.
2. Tag/class lifecycle and relation APIs are first-class in `Editor` (`createTag`, `getTag`, `getTagsByName`, tag-property and extends management).
3. `DB.customQuery` is part of the public DB API alongside `q` and `datascriptQuery`.
4. Experimental block rendering APIs are available via `logseq.Experiments`:
   - `registerBlockPropertiesRenderer`
   - `registerBlockRenderer`

For `registerBlockRenderer`, `when` must be a synchronous predicate function.

## Practical Patterns

### 1) Safe DB-only initialization

```ts
async function initDbOnlyFeature() {
  if (!(await logseq.App.checkCurrentIsDbGraph())) return

  await logseq.Editor.upsertProperty('review-state', {
    type: 'default',
    cardinality: 'one',
  })
}
```

### 2) Create a tag and attach it to a block

```ts
const tag = await logseq.Editor.createTag('Project')
const block = await logseq.Editor.getCurrentBlock()

if (tag?.uuid && block?.uuid) {
  await logseq.Editor.addBlockTag(block.uuid, tag.uuid)
}
```

### 3) Keep node property references constrained by tags

```ts
const ownerProp = await logseq.Editor.upsertProperty('owner', {
  type: 'node',
  cardinality: 'one',
})

const peopleTag = await logseq.Editor.createTag('Person')
if (ownerProp?.id && peopleTag?.id) {
  await logseq.Editor.setPropertyNodeTags(ownerProp.id, [peopleTag.id])
}
```

## Migration Notes (File-Based -> DB)

- Keep graph-type branching explicit with `checkCurrentIsDbGraph()`.
- Prefer `Editor.getBlockProperty` / `Editor.upsertBlockProperty` for DB graphs.
- For dual-mode plugins, isolate property read/write logic behind an adapter.
- Do not assume legacy inline-property parsing in DB mode.

## Source Anchors (Validated 2026-05-29)

- API typing: `libs/src/LSPlugin.ts`
- JS experiments API: `libs/src/modules/LSPlugin.Experiments.ts`
- CLJS SDK wrappers:
  - `libs/cljs-sdk/src/com/logseq/editor.cljs`
  - `libs/cljs-sdk/src/com/logseq/db.cljs`
  - `libs/cljs-sdk/src/com/logseq/app.cljs`
- Runtime API exports: `src/main/logseq/api.cljs`
- DB editor/property runtime:
  - `src/main/logseq/api/editor.cljs`
  - `src/main/logseq/api/db_based.cljs`
  - `src/main/logseq/api/block.cljs`
- DB API runtime: `src/main/logseq/api/db.cljs`
