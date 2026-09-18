---
title: 'Experiments.registerHostedRenderer'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
---

# `Experiments.registerHostedRenderer`

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Experimental.** The SDK's own header warns that plugins relying on `logseq.Experiments.*`
> "temporarily may not be supported on the Marketplace" and that the API "might be adjusted at any
> time" (`libs/src/modules/LSPlugin.Experiments.ts:41-45`).

> **Shipped.** PR [#12395](https://github.com/logseq/logseq/pull/12395) is merged
> (`a95483655b refactor: plugin libs (#12395)`), and this API is present at `be800f1711`.

## Summary

Register a plugin-supplied render function that the **host** mounts, rather than rendering inside
the plugin's own iframe. The host owns the DOM node and calls your `render` when it mounts the
container.

`registerSidebarRenderer` (see [03-sidebar-renderer.md](./03-sidebar-renderer.md)) is a thin wrapper
over this.

## API

```ts
logseq.Experiments.registerHostedRenderer(
  key: string,
  opts: {
    title?: string
    subs?: Array<string>
    type?: string
    render: (props: {}) => any
  }
): void
```

Exactly as declared at `libs/src/modules/LSPlugin.Experiments.ts:138-153`.

- **`key`** — unique within the plugin. It is keywordized host-side
  (`src/main/frontend/handler/plugin.cljs:560`) and stored under the plugin's resources.
- **`type`** is **not** a free-form label. It is the **router key**: `register-hosted-renderer`
  dispatches on it (`src/main/frontend/handler/plugin.cljs:678-688`) —
  `"block"` routes to block-renderer storage, `"block-properties"` to block-properties storage, and
  **anything else** (including omitting it) to hosted-renderer storage. Consumers then filter on
  the exact string: the right sidebar looks for `(= (:type %) "sidebar")`
  (`src/main/frontend/components/right_sidebar.cljs:510-523`). Pick a value the consumer you want
  actually matches.
- **`render`** — must return something the host renderer can render. The host calls it as
  `(render opts)` inside `renderer-container`
  (`src/main/frontend/components/plugins.cljs:1856-1861`); if `render` is not a function the
  container falls back to printing the options. Build React elements with
  `logseq.Experiments.React.createElement` so they share the host's React instance
  (`LSPlugin.Experiments.ts:50-52`).
- **`title`** — used as the label in the right-sidebar menu
  (`right_sidebar.cljs:487-499`) and written into the pane header element
  (`plugins.cljs:1874-1877`).

> **`subs` does nothing today.** The host stores it (`sdk/experiments.cljs:57-62`,
> `handler/plugin.cljs:559-563`) but **nothing reads it** anywhere under `src/main/` at this
> revision — `renderer-container` simply calls `(render opts)`. The SDK's own doc comment for the
> sibling `registerBlockPropertiesRenderer` calls `subs` a "Reserved subscription list for future
> reactive updates" (`LSPlugin.Experiments.ts`). Do not expect `subs` to trigger re-renders; a
> previous revision of this page described it as driving host-state subscriptions, which it does
> not. Drive your own updates from `logseq.DB.onChanged` or `logseq.App.onRouteChanged`.

The host also accepts a `:mode` key that the TS type does not declare
(`src/main/logseq/sdk/experiments.cljs:62` extracts `[:title :type :mode :subs :render]`).

## Example

```ts
import '@logseq/libs'

logseq.ready(() => {
  const React = logseq.Experiments.React as any

  logseq.Experiments.registerHostedRenderer('review-card', {
    title: 'Review queue',
    type: 'card',
    render: () => {
      const [count, setCount] = React.useState(0)

      React.useEffect(() => {
        const refresh = async () => {
          // DB graph: status is a closed-value ref, not a :block/marker string.
          const rows = await logseq.DB.datascriptQuery(
            `[:find (count ?b)
              :where
              [?b :block/tags :logseq.class/Task]
              [?b :logseq.property/status ?s]
              [?s :db/ident :logseq.property/status.todo]]`
          )
          setCount(rows?.[0]?.[0] ?? 0)
        }
        refresh()
        // subs is inert — subscribe yourself.
        const off = logseq.App.onRouteChanged(refresh)
        return () => off?.()
      }, [])

      return React.createElement(
        'div',
        {
          className: 'review-card',
          style: { padding: 12, border: '1px solid var(--ls-border-color)' },
        },
        `Open tasks: ${count}`
      )
    },
  })
}).catch(console.error)
```

`--ls-border-color` is a real Logseq CSS variable. Note that there is no Logseq-provided type
scale — `--ls-font-size-*`, `--ls-font-weight-*` and `--ls-line-height-*` do not exist.

## When to use

- Persistent UI you want the host to own, rather than an iframe you own.
- Components that should share the host's React instance.
- The building block for `registerSidebarRenderer`.

For UI that lives inside your own iframe, prefer `provideUI` (`libs/src/LSPlugin.ts:1194`) or
`registerUIItem` (`:549-552`).

## Source

- TS API: [`libs/src/modules/LSPlugin.Experiments.ts`](../../../../../../libs/src/modules/LSPlugin.Experiments.ts):138-153
- Host SDK entry: [`src/main/logseq/sdk/experiments.cljs`](../../../../../../src/main/logseq/sdk/experiments.cljs):57-62 (`register_hosted_renderer`)
- Registry atom: [`src/main/frontend/handler/plugin.cljs`](../../../../../../src/main/frontend/handler/plugin.cljs):662 (`*hosted-renderer-providers` — a set of plugin ids; the resources themselves live in app state, `:557-568`)
- Type routing: `src/main/frontend/handler/plugin.cljs:678-688`
- Lookup: `src/main/frontend/handler/plugin.cljs:690-701` (`get-hosted-renderers`, `resolve-hosted-render`)
- Host container: `src/main/frontend/components/plugins.cljs:1856-1861`
- Merge commit: `a95483655b refactor: plugin libs (#12395)`
