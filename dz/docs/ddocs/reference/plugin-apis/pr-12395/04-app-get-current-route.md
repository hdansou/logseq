---
title: 'App.getCurrentRoute'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
---

# `App.getCurrentRoute`

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Shipped.** PR [#12395](https://github.com/logseq/logseq/pull/12395) is merged
> (`a95483655b refactor: plugin libs (#12395)`); this API is present at `be800f1711`.

## Summary

Returns the active reitit route match — the path and the route template — so a plugin can make
page-aware decisions without subscribing to navigation events. Unlike the rest of `Experiments`,
this is a stable `IAppProxy` member.

## API

```ts
logseq.App.getCurrentRoute(): Promise<{
  path: string
  parameters: Record<string, any>
  template: string
}>
```

As declared at `libs/src/LSPlugin.ts:526-530`.

The implementation is three lines (`src/main/logseq/api/app.cljs:174-178`):

```clojure
(def get_current_route
  (fn []
    (some-> (state/get-route-match)
            (dissoc :data)
            (bean/->js))))
```

So the object you get back is **the whole reitit route match minus `:data`**, not a hand-built
three-key record. `state/get-route-match` reads `:route-match` from app state
(`src/main/frontend/state.cljs:839-841`). Expect `path` and `template` to be present, and expect
additional keys the TypeScript type does not mention (reitit also carries `path-params`,
`query-params` and `result`). `some->` means the whole call resolves to `undefined` before the
router has matched anything — always null-check.

> **Unverified:** the exact contents and key casing of `parameters` could not be determined from
> source. Logseq's routes are declared without a `:coercion`
> (`src/main/frontend/routes.cljs`, router built at `src/main/frontend/core.cljs:27-29`), and the
> host reads path parameters from **`:path-params`**, not `:parameters` — for example
> `state/get-current-page` uses `[:path-params :name]`
> (`src/main/frontend/state.cljs:851-855`). Log the returned object once in the devtools console
> before depending on a particular key.

## Route templates

The templates you can match against are the route table in
`src/main/frontend/routes.cljs:44-75` — including `/`, `/graphs`, `/page/:name`,
`/page/:name/block/:block-route-name`, `/all-pages`, `/graph`, `/settings` and `/import` — plus any
route a plugin has added with `logseq.Experiments.registerRouteRenderer`
(`src/main/frontend/components/plugins.cljs:1884`).

## Examples

### One-shot probe

```ts
const r = await logseq.App.getCurrentRoute()
console.log(r) // inspect the real shape before relying on any key
// r.template === '/page/:name'
// r.path     === '/page/2026-04-15'
```

### Conditional behaviour based on the current view

```ts
const r = await logseq.App.getCurrentRoute()

switch (r?.template) {
  case '/page/:name':
    await logseq.UI.showMsg(`On page ${r.path}`)
    break
  case '/graph':
    enableGraphOverlay()
    break
  case '/all-pages':
    augmentAllPagesView()
    break
}
```

### Combine with `onRouteChanged`

`logseq.App.onRouteChanged` (`libs/src/LSPlugin.ts:604`) fires with `{ path, template }`, so for
those two fields you do not need a second call. The host emits
`(select-keys route [:template :path :parameters])` on navigation
(`src/main/frontend/core.cljs:31-35`).

```ts
const off = logseq.App.onRouteChanged(async ({ path, template }) => {
  if (template === '/page/:name') {
    const r = await logseq.App.getCurrentRoute()
    refreshContextSidebar(r)
  }
})
// off() to unsubscribe — IUserHook returns IUserOffHook
```

### Use inside a sidebar renderer

```ts
logseq.Experiments.registerSidebarRenderer('context', {
  title: 'Context',
  render: () => {
    const React = logseq.Experiments.React as any
    const [path, setPath] = React.useState<string | null>(null)

    React.useEffect(() => {
      const refresh = () =>
        logseq.App.getCurrentRoute().then((r) => setPath(r?.path ?? null))
      refresh()
      // `subs` is inert — subscribe explicitly.
      const off = logseq.App.onRouteChanged(refresh)
      return () => off?.()
    }, [])

    return React.createElement('div', null, path ? `Viewing: ${path}` : 'No page open')
  },
})
```

See [03-sidebar-renderer.md](./03-sidebar-renderer.md) for why the `subs` option cannot drive that
refresh.

## Console testing

```js
logseq.api.get_current_route()
```

Exported on the global host API at `src/main/logseq/api.cljs:87`.

## Source

- TS type: [`libs/src/LSPlugin.ts`](../../../../../../libs/src/LSPlugin.ts):526-530
- Implementation: [`src/main/logseq/api/app.cljs`](../../../../../../src/main/logseq/api/app.cljs):174-178
- Exported on the global API: [`src/main/logseq/api.cljs`](../../../../../../src/main/logseq/api.cljs):87
- Route table: [`src/main/frontend/routes.cljs`](../../../../../../src/main/frontend/routes.cljs):44-75
- Navigation hook: [`src/main/frontend/core.cljs`](../../../../../../src/main/frontend/core.cljs):31-35
- Merge commit: `a95483655b refactor: plugin libs (#12395)`
