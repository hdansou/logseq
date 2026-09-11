---
title: 'helpers.ts to common.ts rename'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
---

# `helpers.ts` → `common.ts` rename

## Summary

PR [#12395](https://github.com/logseq/logseq/pull/12395) (`a95483655b`) renamed the internal
utilities module `libs/src/helpers.ts` to `libs/src/common.ts`. Git records it as a rename at 96%
similarity with +1 / -24 lines. The changes were:

- **Removed:** `setupIframeSandbox(props, target)`, the only export dropped. Nothing called it: the
  caller has its own `_setupIframeSandbox()` method
  (`a95483655b~1:libs/src/helpers.ts:245`, `a95483655b~1:libs/src/LSPlugin.caller.ts:219`).
- **Removed:** a JSDoc comment block (`@param timeout` / `@param tag`) above `deferred`.
- **Changed (+1):** `let _appPathRoot` became `let _appPathRoot: string`
  ([`libs/src/common.ts:27`](../../../../../../libs/src/common.ts)).

Every other export was kept.

This is an **internal** module. The package root does not re-export it. `LSPlugin.user.ts`, the
webpack entry, re-exports only `./LSPlugin`, which holds the type surface
([`libs/src/LSPlugin.user.ts:1162`](../../../../../../libs/src/LSPlugin.user.ts);
entry at [`libs/webpack.config.js:11`](../../../../../../libs/webpack.config.js)).

## Why it matters

For most plugins, **nothing changes**. The `logseq` global and the types in `LSPlugin.ts` are
unaffected.

You only need to act if either is true:

1. Your plugin's TypeScript imports from `@logseq/libs/dist/helpers` or a similar deep path.
2. You vendored or forked `@logseq/libs` and depend on the internal module layout.

## Migration

For type-only imports, change the path:

```ts
import type { PluginLogger } from '@logseq/libs/dist/helpers'
```

becomes

```ts
import type { PluginLogger } from '@logseq/libs/dist/common'
```

Do **not** rely on a deep path for runtime values. The package build is
`tsc && rm dist/*.js && pnpm build:user`
([`libs/package.json:15`](../../../../../../libs/package.json)). It deletes the per-module `.js`
files `tsc` emits in `dist/` and keeps only the webpack bundle `dist/lsplugin.user.js`
(`libs/package.json:6`, `libs/webpack.config.js:46-50`). So `dist/common` resolves to a `.d.ts`
file with no JavaScript module behind it. The script had the same shape before the PR (`npm run`
instead of `pnpm`; `a95483655b~1:libs/package.json:14`).

The root does not re-export these helpers, so a plugin that needs one at runtime must supply its
own. Two are thin wrappers over declared dependencies of `@logseq/libs`:

- `safeSnakeCase` is `snakeCase` from `change-case` (`libs/src/common.ts:6`, `:30`).
- `deepMerge(a, b)` is `deepmerge` with arrays overwritten rather than concatenated
  (`libs/src/common.ts:5`, `:58-61`).

## Helpers the public SDK class imports from `common.ts`

`LSPlugin.user.ts` imports exactly these seven names from `./common`
([`libs/src/LSPlugin.user.ts:1-9`](../../../../../../libs/src/LSPlugin.user.ts)). All are still
exported at `be800f1711`:

| Symbol | `common.ts` line |
| --- | --- |
| `safeSnakeCase` | `:30` |
| `deepMerge` | `:58` |
| `PluginLogger` | `:108` |
| `isValidUUID` | `:215` |
| `safetyPathJoin` | `:247` |
| `mergeSettingsWithSchema` | `:612` |
| `normalizeKeyStr` | `:627` |

The other import sites import different subsets:

- `LSPlugin.caller.ts` imports only `deferred` and `IS_DEV` (`libs/src/LSPlugin.caller.ts:5`).
- `LSPlugin.core.ts` imports a longer list (`libs/src/LSPlugin.core.ts:2-27`) and the whole module
  as `pluginHelpers` (`:28`).
- `modules/LSPlugin.Experiments.ts` imports only `safeSnakeCase`
  (`libs/src/modules/LSPlugin.Experiments.ts:3`).

## Source

- [`libs/src/common.ts`](../../../../../../libs/src/common.ts), renamed from `libs/src/helpers.ts`
  in `a95483655b`.
- Import sites at `be800f1711`: `libs/src/LSPlugin.user.ts:1-9`, `libs/src/LSPlugin.caller.ts:5`,
  `libs/src/LSPlugin.core.ts:10-28`, `libs/src/modules/LSPlugin.Experiments.ts:3`.
- PR-branch commit (squashed into `a95483655b`; not itself an ancestor of master):
  `28a4474810 refactor(libs): rename helpers.ts to common.ts and update imports`.
