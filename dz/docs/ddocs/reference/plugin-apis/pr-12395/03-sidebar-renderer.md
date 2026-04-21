# `Experiments.registerSidebarRenderer`

> **Experimental.**

## Summary

A convenience wrapper around `registerHostedRenderer` that mounts the rendered output into Logseq's **right sidebar**. The host renders a sidebar pane whose content is your render function's output, with `title` shown as the pane header.

## API

```ts
logseq.Experiments.registerSidebarRenderer(
  key: string,
  opts: {
    title?: string,
    subs?: string[],
    render: (props: {}) => any,
    [k: string]: any           // forwarded to registerHostedRenderer
  }
): void
```

Internally:

```ts
key = `_sidebar.${key}`
opts.type = 'sidebar'
return this.registerHostedRenderer(key, opts)
```

So `key` is automatically namespaced under `_sidebar.*` and `type` is forced to `'sidebar'`.

## Example — quick TODO panel

```ts
import '@logseq/libs'

logseq.ready(() => {
  const React = logseq.Experiments.React as any

  logseq.Experiments.registerSidebarRenderer('open-todos', {
    title: 'Open TODOs',
    subs: ['ui/theme', 'route/current'],
    render: () => {
      const [items, setItems] = React.useState<any[]>([])

      React.useEffect(() => {
        logseq.DB.datascriptQuery(
          `[:find (pull ?b [:block/uuid :block/title])
            :where [?b :block/marker "TODO"]]`
        ).then((rows: any[][]) => setItems(rows.map(r => r[0])))
      }, [])

      return React.createElement(
        'ul',
        { style: { paddingLeft: 16 } },
        items.slice(0, 20).map((b: any) =>
          React.createElement(
            'li',
            { key: b.uuid, onClick: () => logseq.Editor.scrollToBlockInPage(b.uuid) },
            b.title
          )
        )
      )
    }
  })
}).catch(console.error)
```

The user can open it via the right sidebar menu like any other built-in pane.

## Patterns

- Keep `subs` minimal — every key in the list re-runs `render` when its value changes.
- Use `logseq.App.getCurrentRoute()` inside `render` to make the panel page-aware (see [04-app-get-current-route.md](./04-app-get-current-route.md)).
- For multiple panels per plugin, use distinct `key` values (`'today-stats'`, `'inbox'`, etc.) — each becomes its own sidebar entry.

## Source

- TS API: `libs/src/modules/LSPlugin.Experiments.ts:115`
- Mount point in host: `src/main/frontend/components/right_sidebar.cljs` (renders entries from `*hosted-renderer-providers` whose key starts with `_sidebar.`)
- Commit: `enhance(plugin): WIP add support for user registered host renderers in the right sidebar`
