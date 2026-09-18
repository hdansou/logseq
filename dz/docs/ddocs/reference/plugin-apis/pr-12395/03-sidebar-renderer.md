---
title: 'Experiments.registerSidebarRenderer'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
---

# `Experiments.registerSidebarRenderer`

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Experimental.** See the warning in
> [02-hosted-renderer.md](./02-hosted-renderer.md) — `libs/src/modules/LSPlugin.Experiments.ts:41-45`.

> **Shipped.** Present at `be800f1711`, merged as `a95483655b refactor: plugin libs (#12395)`.

## Summary

A convenience wrapper over `registerHostedRenderer` that makes the renderer available as a
**right-sidebar** pane. The user opens it from the sidebar's plugin-renderer menu; the host then
mounts your `render` output into a sidebar item, using `title` as the header.

## API

```ts
logseq.Experiments.registerSidebarRenderer(
  key: string,
  opts: {
    title?: string
    subs?: Array<string>
    render: (props: {}) => any
    [k: string]: any // forwarded to registerHostedRenderer
  }
): void
```

The whole implementation is three lines (`libs/src/modules/LSPlugin.Experiments.ts:155-167`):

```ts
key = `_sidebar.${key}`
opts.type = 'sidebar'
return this.registerHostedRenderer(key, opts)
```

So `key` is namespaced under `_sidebar.*` and `type` is forced to `'sidebar'`. Setting `type`
yourself is pointless — it is overwritten.

> **`subs` does nothing.** Nothing under `src/main/` reads it at this revision; see
> [02-hosted-renderer.md](./02-hosted-renderer.md#api). A previous revision of this page advised
> "keep `subs` minimal — every key in the list re-runs `render`". That is not what happens: no key
> re-runs anything. Subscribe yourself with `logseq.DB.onChanged` or `logseq.App.onRouteChanged`.

## How the host wires it up

1. The right sidebar collects every hosted renderer whose `:type` is exactly `"sidebar"` and, if
   any exist, shows a puzzle-piece button (`src/main/frontend/components/right_sidebar.cljs:510-523`).
   It filters on `:type`, **not** on the `_sidebar.` key prefix.
2. Each menu entry shows your `title` and adds a sidebar block keyed
   `(keyword pid key)` with type `:plugin`
   (`src/main/frontend/components/right_sidebar.cljs:487-499`).
3. `renderer-resolver` looks the entry back up with
   `resolve-hosted-render pid key :sidebar` and mounts it
   (`src/main/frontend/components/plugins.cljs:1863-1882`); it also writes `title` into the pane
   header element.

The renderer is only mounted once the user opens it from that menu — registering it does not put
anything on screen.

## Example — open-task panel

```ts
import '@logseq/libs'

logseq.ready(() => {
  const React = logseq.Experiments.React as any

  logseq.Experiments.registerSidebarRenderer('open-tasks', {
    title: 'Open tasks',
    render: () => {
      const [items, setItems] = React.useState<any[]>([])

      React.useEffect(() => {
        const refresh = () =>
          logseq.DB.datascriptQuery(
            `[:find (pull ?b [:block/uuid :block/title])
              :where
              [?b :block/tags :logseq.class/Task]
              [?b :logseq.property/status ?s]
              [?s :db/ident :logseq.property/status.todo]]`
          ).then((rows: any[][]) => setItems(rows.map((r) => r[0])))

        refresh()
        const off = logseq.DB.onChanged(refresh)
        return () => off?.()
      }, [])

      return React.createElement(
        'ul',
        { style: { paddingLeft: 16 } },
        items.slice(0, 20).map((b: any) =>
          React.createElement(
            'li',
            {
              key: b.uuid,
              onClick: () => logseq.Editor.openInRightSidebar(b.uuid),
            },
            b.title
          )
        )
      )
    },
  })
}).catch(console.error)
```

Two corrections against the previous revision of this example:

- The query used `[?b :block/marker "TODO"]`. `:block/marker` does not exist in a DB graph — it
  survives only in `deps/graph-parser/`. Status is a closed-value ref matched by `:db/ident`.
- The click handler called `logseq.Editor.scrollToBlockInPage(b.uuid)` with one argument. Its
  signature is `(pageName: BlockPageName, blockId: BlockIdentity, opts?)`
  (`libs/src/LSPlugin.ts:932-936`), so a single UUID is wrong. `openInRightSidebar(idOrKey)`
  (`:938`) takes one argument and is the better fit here; use `scrollToBlockInPage` only when you
  have both the page name and the block.

## Patterns

- Use `logseq.App.getCurrentRoute()` inside `render` to make the panel page-aware — see
  [04-app-get-current-route.md](./04-app-get-current-route.md) — and re-fetch it from an
  `onRouteChanged` subscription, since nothing re-renders on its own.
- For several panels per plugin, register several distinct keys (`'today-stats'`, `'inbox'`); each
  becomes its own entry in the sidebar menu.
- Return a cleanup function from your effects — the host mounts and unmounts the container as the
  user opens and closes the pane.

## Source

- TS API: [`libs/src/modules/LSPlugin.Experiments.ts`](../../../../../../libs/src/modules/LSPlugin.Experiments.ts):155-167
- Sidebar menu: [`src/main/frontend/components/right_sidebar.cljs`](../../../../../../src/main/frontend/components/right_sidebar.cljs):487-499, `:510-523`
- Resolution and mount: [`src/main/frontend/components/plugins.cljs`](../../../../../../src/main/frontend/components/plugins.cljs):1856-1882
- Registry: `src/main/frontend/handler/plugin.cljs:662`, `:678-701`
- Merge commit: `a95483655b refactor: plugin libs (#12395)`
