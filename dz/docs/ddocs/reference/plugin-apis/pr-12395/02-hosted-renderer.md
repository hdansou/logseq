# `Experiments.registerHostedRenderer`

> **Experimental.** Plugins relying on `logseq.Experiments.*` may not be accepted to the official Marketplace and the API can change between versions.

## Summary

Register a plugin-supplied React component that the **host** mounts itself, rather than rendering inside the plugin's own iframe. The host owns the DOM node, lifecycle, and re-renders driven by host state subscriptions (`subs`).

This is the foundation that `registerSidebarRenderer` (see [03-sidebar-renderer.md](./03-sidebar-renderer.md)) is built on top of.

## API

```ts
logseq.Experiments.registerHostedRenderer(
  key: string,
  opts: {
    title?: string,
    type?: string,        // free-form classification, e.g. 'sidebar', 'card'
    subs?: string[],      // host state keys to re-render on (e.g. 'ui/theme', 'route/current')
    render: (props: {}) => any
  }
): void
```

- `key` — unique within the plugin. Hosted renderers are tracked in `*hosted-renderer-providers` keyed by `[plugin-id, key]`.
- `render` — must return something the host can render. In practice: a React element built with `logseq.Experiments.React.createElement` so it shares the host's React instance.

## Example

```ts
import '@logseq/libs'

logseq.ready(() => {
  const React = logseq.Experiments.React as any

  logseq.Experiments.registerHostedRenderer('review-card', {
    title: 'Review queue',
    type: 'card',
    subs: ['route/current'],
    render: () => {
      const [count, setCount] = React.useState(0)
      React.useEffect(() => {
        // refresh whenever route changes
        logseq.App.getCurrentRoute().then(async () => {
          const blocks = await logseq.DB.datascriptQuery(
            `[:find (count ?b) :where [?b :block/marker "TODO"]]`
          )
          setCount(blocks?.[0]?.[0] ?? 0)
        })
      })
      return React.createElement(
        'div',
        { className: 'review-card', style: { padding: 12, border: '1px solid var(--ls-border-color)' } },
        `Open TODOs: ${count}`
      )
    }
  })
}).catch(console.error)
```

## When to use

- Persistent UI you don't want re-mounted on every plugin reload.
- Components that need to respond to host state (route, theme, sidebar visibility).
- Building blocks for vertically integrated plugins (dashboards, side panels, status bars).

For ad-hoc UI that lives inside your own iframe, prefer `provideUI` / `registerUIItem`.

## Source

- TS API: `libs/src/modules/LSPlugin.Experiments.ts:98`
- Host SDK entry: `src/main/logseq/sdk/experiments.cljs:42` (`register_hosted_renderer`)
- Registry: `src/main/frontend/handler/plugin.cljs:591` (`*hosted-renderer-providers`)
- Commit: `enhance(plugins): add hosted renderer registration and resolution`
