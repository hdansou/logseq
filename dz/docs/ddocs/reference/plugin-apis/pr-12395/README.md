# PR #12395 — Plugin libs refactor

Reference docs for the plugin SDK changes shipped in [logseq/logseq#12395](https://github.com/logseq/logseq/pull/12395) (merged 2026-04-14, `@logseq/libs` 0.2.12 → 0.3.1).

## What changed

| # | Area | Doc |
| --- | --- | --- |
| 1 | Postmate transport rewrite using `MessageChannel` | [01-postmate-message-channel.md](./01-postmate-message-channel.md) |
| 2 | New `registerHostedRenderer` experimental API | [02-hosted-renderer.md](./02-hosted-renderer.md) |
| 3 | New `registerSidebarRenderer` experimental API | [03-sidebar-renderer.md](./03-sidebar-renderer.md) |
| 4 | New `App.getCurrentRoute()` API | [04-app-get-current-route.md](./04-app-get-current-route.md) |
| 5 | `Editor.getBlockProperties` now merges class default values | [05-block-properties-class-defaults.md](./05-block-properties-class-defaults.md) |
| 6 | `helpers.ts` renamed to `common.ts` | [06-helpers-to-common-rename.md](./06-helpers-to-common-rename.md) |

## Other items in the PR (not separately documented here)

- `App.exportDebugLogDb` was added then removed before merge — do not rely on it.
- `clj-e2e/test/logseq/e2e/plugins_marketplace_test.clj` adds an end-to-end marketplace install test.
- `libs/development-notes/experiments_api_guide.md` and `starter_guide.md` are upstream docs added by the PR — see them for the full Experiments surface.

## Compatibility

- Minimum SDK version for plugins using these APIs: **0.3.1**.
- The Postmate change is transparent to existing plugins; older hosts fall back to the legacy transport.
- `registerHostedRenderer`, `registerSidebarRenderer`, `getCurrentRoute`, and the class-default behavior require a Logseq desktop build that includes this PR (after 2026-04-14).
