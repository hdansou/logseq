# `helpers.ts` → `common.ts` rename

## Summary

The internal utilities module `libs/src/helpers.ts` was renamed to `libs/src/common.ts` and trimmed (-24 lines, +1 line — most exports were left intact, a few unused helpers removed).

This is an **internal** module — it isn't part of the public `@logseq/libs` surface. The rename only affects consumers who imported from the deep path (which they shouldn't have been doing).

## Why it matters

For the vast majority of plugins: **nothing changes.** Public exports continue to come from `@logseq/libs`.

You only need to act if either is true:

1. Your plugin's TypeScript imports from `@logseq/libs/dist/helpers` or a similarly deep path.
2. You vendored / forked `@logseq/libs` and depend on the internal module layout.

## Migration

Find:

```ts
import { safeSnakeCase, deepMerge } from '@logseq/libs/dist/helpers'
```

Replace with:

```ts
import { safeSnakeCase, deepMerge } from '@logseq/libs/dist/common'
```

Or — preferred — stop importing from internal paths and rely on what's re-exported from the package root.

## Symbols re-exported from `common.ts`

Per the PR diff, these helpers are still available (now from `common.ts`):

- `isValidUUID`
- `deepMerge`
- `mergeSettingsWithSchema`
- `PluginLogger`
- `safeSnakeCase`
- `safetyPathJoin`
- `normalizeKeyStr`

These are imported by `LSPlugin.user.ts`, `LSPlugin.caller.ts`, `LSPlugin.core.ts`, and `modules/LSPlugin.Experiments.ts` after the rename.

## Source

- New file: `libs/src/common.ts`
- All updated import sites in the same PR (`libs/src/LSPlugin.*.ts`, `libs/src/modules/*.ts`).
- Commit: `refactor(libs): rename helpers.ts to common.ts and update imports`
