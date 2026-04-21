# `App.getCurrentRoute`

## Summary

Returns the active reitit route the user is on — the path, parsed parameters, and the original template. Lets a plugin make page-aware decisions without subscribing to history events.

## API

```ts
logseq.App.getCurrentRoute(): Promise<{
  path: string
  parameters: Record<string, any>
  template: string
}>
```

Backed by Clojure `logseq.api.app/get_current_route`, which serializes `(state/get-route-match)` minus `:data`.

## Examples

### One-shot probe

```ts
const r = await logseq.App.getCurrentRoute()
// {
//   path: '/page/2026-04-15',
//   parameters: { name: '2026-04-15' },
//   template: '/page/:name'
// }
```

### Conditional behavior based on the current view

```ts
const r = await logseq.App.getCurrentRoute()

switch (r?.template) {
  case '/page/:name':
    logseq.UI.showMsg(`On page ${r.parameters.name}`)
    break
  case '/graph':
    enableGraphOverlay()
    break
  case '/all-pages':
    augmentAllPagesView()
    break
}
```

### Combine with `onRouteChanged` for reactive plugins

```ts
logseq.App.onRouteChanged(async () => {
  const r = await logseq.App.getCurrentRoute()
  if (r?.template === '/page/:name') {
    refreshContextSidebar(r.parameters.name)
  }
})
```

### Use inside a sidebar renderer

```ts
logseq.Experiments.registerSidebarRenderer('context', {
  title: 'Context',
  subs: ['route/current'],
  render: () => {
    const React = logseq.Experiments.React as any
    const [page, setPage] = React.useState<string | null>(null)
    React.useEffect(() => {
      logseq.App.getCurrentRoute().then(r => setPage(r?.parameters?.name ?? null))
    })
    return React.createElement('div', null, page ? `Viewing: ${page}` : 'No page open')
  }
})
```

## Console testing

```js
logseq.api.get_current_route()
```

## Source

- TS type: `libs/src/LSPlugin.ts:1079`
- Implementation: `src/main/logseq/api/app.cljs:176`
- Exported on global API: `src/main/logseq/api.cljs:86`
- Commit: `enhance(apis): add get_current_route function for route retrieval`
