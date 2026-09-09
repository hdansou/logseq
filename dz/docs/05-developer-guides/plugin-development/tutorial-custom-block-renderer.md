# Tutorial: Build a Drag-and-Drop Kanban with `registerBlockRenderer`

> **Last revised:** 2026-05-11
> **Applies to:** Logseq desktop builds dated **2026-04-28** or later (when [PR #12533](https://github.com/logseq/logseq/pull/12533) merged)
> **Companion POC:** [`logseq-block-views`](https://github.com/logseq-dev/plugin-dev/tree/master/logseq-block-views) — every code snippet in this guide compiles in that project (lives in the plugin-dev workspace at `~/Projects/src/github.com/logseq-dev/plugin-dev/logseq-block-views/`).

**What you'll build**

A plugin that turns any DB-graph block into a kanban board:

- Tag a block with `view:: kanban` and it renders as a kanban.
- Configure which property to group by with `view-by:: status` (default).
- Optionally restrict columns to a subset with `view-columns:: todo, doing, done`.
- Child blocks become cards, bucketed into columns by their status property.
- Drag a card between columns — the status property updates and the card snaps to its new column immediately.

The plugin works with **both** the built-in `:logseq.property/status` (set automatically when you tag a block with `#Task`) and user-defined `status::` text properties.

**Time required**: ~45 minutes if you read carefully, plus the time you'd otherwise have spent hitting the same dead ends I did. There are several non-obvious traps documented inline.

**Prerequisites**

- Logseq desktop, **built after 2026-04-28** (any nightly or release from then on)
- Developer Mode enabled (`Settings → Advanced → Developer mode`)
- Node.js (any recent LTS)
- A DB graph to test against
- Familiarity with TypeScript and React (we use `logseq.Experiments.React` from the host)

> ⚠️ **Experimental API warning.** `logseq.Experiments.registerBlockRenderer` lives under `Experiments`. Signatures and behaviors may change without a deprecation cycle. Plugins relying on it may be temporarily unsupported on the Marketplace.

> 📌 **`@logseq/libs` version trap.** On npm the `latest` tag is pinned to the very old `0.0.17`. The current SDK is published under the `next` tag as **`0.3.3`**, which is the minimum version that ships `registerBlockRenderer` types.
>
> - `npm install @logseq/libs` → installs `0.0.17`, **no `registerBlockRenderer`** ❌
> - `npm install @logseq/libs@^0.3.3` → installs `0.3.3` ✓
> - `npm install @logseq/libs@next` → installs `0.3.3` ✓

---

## Table of contents

1. [Scaffold the plugin](#1-scaffold-the-plugin)
2. [Hello, renderer](#2-hello-renderer)
3. [Understand the data the host sends you](#3-understand-the-data-the-host-sends-you)
4. [Read the board config from the parent block](#4-read-the-board-config-from-the-parent-block)
5. [Discover the property's actual ident](#5-discover-the-propertys-actual-ident)
6. [Async-resolve built-in entity references](#6-async-resolve-built-in-entity-references)
7. [Bucket cards into columns](#7-bucket-cards-into-columns)
8. [Render the kanban](#8-render-the-kanban)
9. [Add drag-and-drop](#9-add-drag-and-drop)
10. [Persist the move with optimistic UI](#10-persist-the-move-with-optimistic-ui)
11. [Gotchas and host-side gaps](#11-gotchas-and-host-side-gaps)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Scaffold the plugin

```bash
mkdir logseq-kanban-renderer
cd logseq-kanban-renderer
npm init -y
npm install @logseq/libs@^0.3.3
npm install --save-dev typescript esbuild
mkdir src
```

Create `package.json` with the Logseq manifest section:

```json
{
  "name": "logseq-kanban-renderer",
  "version": "0.0.1",
  "main": "dist/index.html",
  "scripts": {
    "build": "esbuild src/index.ts --bundle --outfile=dist/index.js --platform=browser --format=iife && cp index.html dist/index.html",
    "watch": "esbuild src/index.ts --bundle --outfile=dist/index.js --platform=browser --format=iife --watch"
  },
  "dependencies": {
    "@logseq/libs": "^0.3.3"
  },
  "devDependencies": {
    "esbuild": "^0.24.0",
    "typescript": "^5.6.0"
  },
  "logseq": {
    "id": "kanban-renderer",
    "title": "Kanban Renderer",
    "icon": "",
    "main": "dist/index.html"
  }
}
```

> 🚨 **Don't set `"mode": "shadow"`.** The shadow-mode loader in current Logseq builds calls into `window.QSandbox.importHTML`, but `QSandbox` is referenced only by `lsplugin.core.js` and **never defined anywhere in the shipped app bundle**. Setting `mode: "shadow"` makes the plugin fail to load with `TypeError: es is not a function`. The default **iframe mode is correct** for renderer-only plugins despite the `Can not access host scope!` cosmetic warning you'll see in the console (more on that below).

Create `index.html`:

```html
<!doctype html>
<html>
  <head><meta charset="utf-8" /></head>
  <body><script src="./index.js"></script></body>
</html>
```

Create `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "lib": ["ES2020", "DOM"],
    "noEmit": true
  },
  "include": ["src/**/*"]
}
```

Create `.gitignore`:

```
node_modules/
dist/
```

---

## 2. Hello, renderer

Start with the smallest possible thing that registers as a block renderer. `src/index.ts`:

```typescript
import '@logseq/libs'

async function main(): Promise<void> {
  const React: any = (logseq as any).Experiments.React

  ;(logseq as any).Experiments.registerBlockRenderer('hello-kanban', {
    when: (props: { properties?: Record<string, any> }) => {
      // Match blocks with `view:: kanban` set.
      const view = Object.values(props.properties ?? {}).find(
        (v) => typeof v === 'string' && v === 'kanban',
      )
      return Boolean(view)
    },
    render: () =>
      React.createElement(
        'div',
        { style: { padding: 12, border: '1px solid red' } },
        'Hello from the kanban renderer',
      ),
  })
}

logseq.ready(main).catch(console.error)
```

Build and load:

```bash
npm run build
```

In Logseq: **Plugins → Load unpacked plugin → pick the folder.** Then create a block, set its `view::` property to `kanban`, and you should see the red-bordered placeholder replace the outline.

> ⚠️ **You'll see `Can not access host scope!` in the console.** Ignore it. The SDK's `ensureHostScope()` does a defensive probe that throws under file-origin sandboxing but still returns `window.top` and the `Experiments` calls still work. This log line is cosmetic noise, not an error.

If the placeholder shows up, the API plumbing is fine and we can build the real thing.

---

## 3. Understand the data the host sends you

This is the most non-obvious part of the API. The renderer receives an object roughly shaped like:

```typescript
{
  blockId: string                   // parent's :block/uuid (string)
  uuid: string                      // same as blockId
  page: string                      // parent's page title
  content: string                   // parent block title text
  format: 'markdown' | 'org' | ...
  properties: Record<string, any>   // parent's properties, see below
  children: Child[]                 // recursive child tree, when includeChildren: true
}
```

But **`properties` (parent) and `children` (descendants) are normalized through different code paths** and end up with different shapes. This caught me out for hours and isn't documented anywhere. Here's the cheat sheet:

### Parent's `properties`

Built by `build-block-renderer-match-context` (`src/main/frontend/components/block.cljs`). Each property:

- **Key** = the `:db/ident` keyword with the **leading `:` stripped** — e.g. `"user.property/view-XCZND0jX"` for a user-defined `view::` property in a DB graph.
- **Value** = serialized via `serialize-property-value-for-plugin` (`src/main/frontend/handler/plugin.cljs:777`):
  - **string** → string
  - **entity ref** → `{ uuid: string, title: string }` ← title is included!
  - **keyword** → `"ns/name"` string
  - **set/sequential of refs** → array of the above

### Children's properties

Built by `build-block-renderer-children-props` → `db/pull` → `normalize-keyword-for-json` (`src/main/logseq/sdk/utils.cljs`). Critically different:

- **No `.properties` sub-object** — properties live at the **top level** of each child object.
- **Keys** for non-`block/*` and non-`db/*` namespaces keep the leading `:`. So you see `:logseq.property/status` and `:user.property/foo-XYZ` as object keys on the child.
- **Values** of entity refs come through as bare `{ dbId: number }` — **no title attached**. `db/pull` returns refs as `{:db/id N}` only, and the normalizer just camelCases that.

A typical card object:

```javascript
{
  dbId: 1819,
  uuid: "6a014a22-...",
  title: "card 0",
  tags: [{ dbId: 142 }],
  ":logseq.property/status": { dbId: 1825 },   // ← no title!
  children: [...]
}
```

This means **for built-in ref-typed properties on cards, you can't read the value synchronously** in the renderer — you have to async-fetch via `logseq.Editor.getBlockProperty`. We'll do that in step 6.

### DB-graph property ident pattern

User-defined properties in a DB graph get a **random discriminator suffix**: a property named `view` becomes `:user.property/view-XCZND0jX`. You can't guess the suffix — you have to scan keys with a regex. Built-in properties (`:logseq.property/...`) don't have this.

---

## 4. Read the board config from the parent block

We want three optional board-level properties:

| Property | Purpose | Default |
|---|---|---|
| `view::` | Activates the renderer (must equal `kanban`) | required |
| `view-by::` | Which child property to group cards by | `status` |
| `view-columns::` | Comma-separated subset/ordering of columns | full option list |

A regex factory accepts both shapes (parent has no leading `:`, children do):

```typescript
const identReFor = (name: string) =>
  new RegExp(
    `^:?(user\\.property\\/${name}-[A-Za-z0-9]+|logseq\\.property\\/${name})$`,
  )

function resolveValue(raw: any): string {
  if (typeof raw === 'string') return raw
  if (raw && typeof raw === 'object' && typeof raw.title === 'string')
    return raw.title
  return ''
}

function readParentProp(
  props: Record<string, any> | undefined,
  shortName: string,
): string {
  if (!props) return ''
  const re = identReFor(shortName)
  const entry = Object.entries(props).find(([k]) => re.test(k))
  return resolveValue(entry?.[1])
}

function readParentPropList(
  props: Record<string, any> | undefined,
  shortName: string,
): string[] {
  if (!props) return []
  const re = identReFor(shortName)
  const entry = Object.entries(props).find(([k]) => re.test(k))
  const raw = entry?.[1]
  if (Array.isArray(raw)) return raw.map(resolveValue).filter(Boolean)
  if (typeof raw === 'string')
    return raw.split(/[,\n]/).map((s) => s.trim()).filter(Boolean)
  return []
}
```

Now the `when` predicate stays cheap, and we'll use `readParentProp` / `readParentPropList` from inside the render to get config.

---

## 5. Discover the property's actual ident

The user says `view-by:: status`. We need to find which ident on the cards corresponds to that short name. We scan child keys with the same regex factory:

```typescript
type Child = Record<string, any>

function findChildProp(
  card: Child,
  shortName: string,
): { key: string; value: any } | null {
  const re = identReFor(shortName)
  for (const k of Object.keys(card)) {
    if (re.test(k)) return { key: k, value: card[k] }
  }
  return null
}
```

The first card on the board that has the property set tells us the full ident (including the random suffix for user-defined properties). We store the colon-stripped form for the API write later:

```typescript
const stripColon = (k: string) => (k.startsWith(':') ? k.slice(1) : k)

let propIdent: string | null = null
for (const c of children) {
  const found = findChildProp(c, viewByName)
  if (found) {
    propIdent = stripColon(found.key)
    break
  }
}
```

---

## 6. Async-resolve built-in entity references

For each card whose status came through as `{ dbId: N }` (no title), we need to fetch the full target entity to get its `title`. The SDK exposes `logseq.Editor.getBlockProperty(uuid, key)` which returns the resolved entity for ref-typed properties.

Because we need to fetch asynchronously and update the view when the data arrives, the renderer must be a real React function component using `useState` + `useEffect`. Logseq's `Experiments.React` is the host's React instance, so hooks work.

```typescript
function KanbanBoard(props: {
  content?: string
  children: Child[]
  properties?: Record<string, any>
}) {
  const React: any = (logseq as any).Experiments.React
  // uuid → resolved status title, cached across renders
  const [resolved, setResolved] = React.useState({}) as [
    Record<string, string>,
    (fn: (prev: Record<string, string>) => Record<string, string>) => void,
  ]

  const viewByName = readParentProp(props.properties, 'view-by') || 'status'

  // discover propIdent + collect synchronously resolvable values
  let propIdent: string | null = null
  const observed: string[] = []
  for (const c of props.children) {
    const found = findChildProp(c, viewByName)
    if (!found) continue
    if (!propIdent) propIdent = stripColon(found.key)
    const v = resolveValue(found.value)
    if (v) observed.push(v)
  }

  // For each card that needs async resolution, fetch and cache.
  React.useEffect(() => {
    if (!propIdent) return
    const toResolve: string[] = []
    for (const c of props.children) {
      if (!c.uuid || resolved[c.uuid]) continue
      const found = findChildProp(c, viewByName)
      if (!found || resolveValue(found.value)) continue
      toResolve.push(c.uuid)
    }
    if (toResolve.length === 0) return
    Promise.all(
      toResolve.map(async (uuid) => {
        try {
          const r: any = await (logseq.Editor as any).getBlockProperty(
            uuid,
            propIdent,
          )
          return { uuid, title: typeof r?.title === 'string' ? r.title : '' }
        } catch {
          return { uuid, title: '' }
        }
      }),
    ).then((rs) =>
      setResolved((prev) => {
        const next = { ...prev }
        for (const r of rs) if (r.title) next[r.uuid] = r.title
        return next
      }),
    )
  }, [props.children, propIdent])

  const statusOf = (card: Child): string => {
    const sync = resolveValue(findChildProp(card, viewByName)?.value)
    return sync || (card.uuid && resolved[card.uuid]) || ''
  }

  // …continued in next steps
}
```

This gives us first-paint with sync data, then a second paint a moment later with the async-resolved titles. Good UX.

---

## 7. Bucket cards into columns

Columns come from one of three sources, in order:

1. The user's explicit `view-columns::` setting
2. A hardcoded option list for known built-in properties (status)
3. Distinct values observed on the cards (derived)

```typescript
const BUILTIN_OPTIONS: Record<string, string[]> = {
  status: ['Backlog', 'Todo', 'Doing', 'In Review', 'Done', 'Canceled'],
}

const PALETTE = [
  '#94a3b8', '#60a5fa', '#34d399', '#f59e0b', '#a78bfa', '#fb7185', '#22d3ee',
]

type Column = { id: string; title: string; accent: string }
const colId = (t: string) => t.toLowerCase().trim()

function decideColumnTitles(
  viewByName: string,
  configured: string[],
  observed: string[],
): string[] {
  if (configured.length > 0) return canonicalizeTitles(viewByName, configured)
  if (BUILTIN_OPTIONS[viewByName]) return BUILTIN_OPTIONS[viewByName]
  const seen = new Map<string, string>()
  for (const s of observed) {
    const id = colId(s)
    if (id && !seen.has(id)) seen.set(id, s)
  }
  return [...seen.values()]
}

function canonicalizeTitles(viewByName: string, titles: string[]): string[] {
  const opts = BUILTIN_OPTIONS[viewByName]
  if (!opts) return titles
  return titles.map((t) => opts.find((o) => colId(o) === colId(t)) ?? t)
}

function buildColumns(titles: string[]): Column[] {
  if (titles.length === 0)
    return [{ id: '__empty__', title: 'Cards', accent: PALETTE[0] }]
  return titles.map((title, i) => ({
    id: colId(title),
    title,
    accent: PALETTE[i % PALETTE.length],
  }))
}
```

The `canonicalizeTitles` step is important: if the user types `view-columns:: todo, doing, done` with lowercase, we snap them to `Todo / Doing / Done` so the writeback's title-resolution finds the right built-in entity.

Then in your component:

```typescript
const configured = readParentPropList(props.properties, 'view-columns')
const columns = buildColumns(
  decideColumnTitles(viewByName, configured, [
    ...observed,
    ...(Object.values(resolved) as string[]),
  ]),
)
const colIdsInSet = new Set(columns.map((c) => c.id))

const byCol = new Map<string, Child[]>(columns.map((c) => [c.id, []]))
for (const card of props.children) {
  const id = colId(statusOf(card))
  if (id && colIdsInSet.has(id)) byCol.get(id)!.push(card)
  // else: hidden (status outside subset, or no status set)
}
```

Cards with no status, or with a status not in the column set, are hidden. Toggle back to outline view from the block's action menu to see/edit them.

---

## 8. Render the kanban

The grid layout plus per-column rendering. Inline styles are fine for a POC; theme-aware via Logseq CSS variables.

```typescript
const renderColumn = (col: Column) => {
  const cards = byCol.get(col.id) ?? []
  return React.createElement('div', {
    key: col.id,
    style: {
      background: 'var(--ls-secondary-background-color)',
      border: '1px solid var(--ls-border-color)',
      borderRadius: 8,
      padding: 10,
      display: 'flex',
      flexDirection: 'column',
      minWidth: 220,
    },
  }, [
    // header with title + count
    React.createElement('div', {
      key: 'header',
      style: {
        display: 'flex',
        justifyContent: 'space-between',
        paddingBottom: 8,
        marginBottom: 8,
        borderBottom: `2px solid ${col.accent}`,
        fontSize: 12,
        fontWeight: 600,
        textTransform: 'uppercase',
      },
    }, [
      React.createElement('span', { key: 't', style: { color: col.accent } }, col.title),
      React.createElement('span', { key: 'c' }, String(cards.length)),
    ]),
    // cards
    cards.length === 0
      ? React.createElement('div', { key: 'empty', style: { opacity: 0.4 } }, 'Drop cards here')
      : React.createElement('div', { key: 'cards' }, cards.map(renderCard)),
  ])
}

return React.createElement('section', {
  style: {
    display: 'flex',
    flexDirection: 'column',
    gap: 12,
    padding: 12,
    border: '1px solid var(--ls-border-color)',
    borderRadius: 10,
  },
}, [
  React.createElement('div', { key: 'title', style: { fontWeight: 700 } },
    `🗂️ ${props.content || 'Kanban'} — by ${viewByName} · ${props.children.length} cards`,
  ),
  React.createElement('div', {
    key: 'cols',
    style: {
      display: 'grid',
      gridTemplateColumns: `repeat(${columns.length}, 1fr)`,
      gap: 12,
    },
  }, columns.map(renderColumn)),
])
```

Register the component:

```typescript
;(logseq as any).Experiments.registerBlockRenderer('kanban-board', {
  when: (props) => readParentProp(props.properties, 'view').toLowerCase() === 'kanban',
  includeChildren: true,
  priority: 50,
  render: (rprops) =>
    React.createElement(KanbanBoard, {
      content: rprops.content,
      children: rprops.children ?? [],
      properties: rprops.properties,
    }),
})
```

At this point you have a read-only kanban that displays correctly. Rebuild, reload, and verify before moving on to drag-and-drop.

---

## 9. Add drag-and-drop

Three things conspire against you here:

1. **Logseq's outliner attaches its own block-level `drag` handlers**, some of them natively (not via React). React's `stopPropagation` doesn't stop them — you also need `nativeEvent.stopImmediatePropagation()`.
2. **If your drag payload uses `text/plain`**, Logseq's drop handler accepts it as a new text block and inserts ghost cards. Use a custom MIME type.
3. **The block-level `:on-drag-end` handler removes a `#dragging-ghost-element`** that *Logseq's* drag-start would have created. If our drag-end runs without that element existing, it throws `removeChild on Node`. Stop propagation on every handler.

Here's the card with full DnD defenses:

```typescript
const KANBAN_MIME = 'application/x-logseq-kanban-card'

const renderCard = (card: Child) =>
  React.createElement('div', {
    key: card.uuid ?? cardText(card),
    draggable: true,
    onDragStart: (e: any) => {
      e.stopPropagation()
      e.nativeEvent?.stopImmediatePropagation?.()
      if (card.uuid) e.dataTransfer.setData(KANBAN_MIME, card.uuid)
      e.dataTransfer.effectAllowed = 'move'
      e.currentTarget.style.opacity = '0.4'
    },
    onDragEnd: (e: any) => {
      e.stopPropagation()
      e.nativeEvent?.stopImmediatePropagation?.()
      e.currentTarget.style.opacity = '1'
    },
    style: {
      cursor: 'grab',
      userSelect: 'none',
      // …rest of card style
    },
  }, cardText(card) || '(empty)')
```

And the column as a drop target:

```typescript
onDragOver: (e: any) => {
  // Only react to our own drag payloads.
  if (!Array.from(e.dataTransfer.types).includes(KANBAN_MIME)) return
  e.preventDefault()
  e.stopPropagation()
  e.nativeEvent?.stopImmediatePropagation?.()
  e.dataTransfer.dropEffect = 'move'
  e.currentTarget.style.background = 'var(--ls-tertiary-background-color)'
},
onDragLeave: (e: any) => {
  e.currentTarget.style.background = 'var(--ls-secondary-background-color)'
},
onDrop: (e: any) => {
  e.preventDefault()
  e.stopPropagation()
  e.nativeEvent?.stopImmediatePropagation?.()
  e.currentTarget.style.background = 'var(--ls-secondary-background-color)'
  const uuid = e.dataTransfer.getData(KANBAN_MIME)
  if (!uuid || !propIdent) return
  // …moves persisted in next step
},
```

At this point dragging fades the card, the columns highlight on hover, and dropping does nothing yet. No ghost blocks, no errors.

---

## 10. Persist the move with optimistic UI

When the user drops a card on a column, two things happen:

1. **Optimistic update** — snap the card to the new column locally so the UI is instant.
2. **DB write** — call `logseq.Editor.upsertBlockProperty(uuid, propIdent, column.title)` in the background.

### The destructive footgun

> 🚨 **Do NOT pass an ident keyword string as the value for a ref-typed property.** I learned this the destructive way. Passing `'logseq.property/status.doing'` to `upsertBlockProperty` silently **retracts** the property (and may strip associated tags). The correct shape per the SDK docs (`libs/src/LSPlugin.ts:3116`) is the entity's **title** — `'Doing'` — which the host then resolves to the ref.

```typescript
async function moveCard(
  uuid: string,
  propIdent: string,
  column: Column,
): Promise<void> {
  if (column.id.startsWith('__')) return  // synthetic columns (__empty__) are not real status values
  try {
    await (logseq.Editor as any).upsertBlockProperty(
      uuid,
      propIdent,
      column.title, // ← title, not ident
    )
  } catch (err) {
    console.error('[kanban] failed to update status:', err)
  }
}
```

Hook it up in the drop handler with an optimistic local update so the card moves immediately:

```typescript
onDrop: (e: any) => {
  // …prop/event handling above
  const uuid = e.dataTransfer.getData(KANBAN_MIME)
  if (!uuid || !propIdent) return
  // Optimistic: update the local cache so statusOf() returns the new value
  // on next render. Logseq doesn't re-render us on its own when a child's
  // property changes.
  setResolved((prev) => ({ ...prev, [uuid]: col.title }))
  moveCard(uuid, propIdent, col)
}
```

Build, reload, drag a card, and watch it land in the new column instantly.

> **First time? Test on a disposable card.** Built-in `:logseq.property/status` writeback via title-resolution is not officially documented for plugins. We verified it works on the May 2026 build, but corner cases (typo in title, multi-value properties) may still retract or no-op.

---

## 11. Gotchas and host-side gaps

These are real footguns nothing in the docs warns you about. Worth pinning to your wall.

| Gotcha | Symptom | Fix |
|---|---|---|
| `mode: "shadow"` doesn't load | `TypeError: es is not a function` on plugin load | Use the default iframe mode for renderer plugins. Shadow loader needs `window.QSandbox` which isn't bundled in shipped builds. |
| `Can not access host scope!` warning | Logged every drag in iframe mode | Cosmetic. The SDK's `ensureHostScope()` probe throws but still returns `window.top`. Ignore. |
| `npm install @logseq/libs` installs `0.0.17` | Plugin has no `registerBlockRenderer` typing | Pin `@logseq/libs@^0.3.3` (npm `next` tag). |
| Parent props vs child props are shaped differently | Predicate works, render lookups don't | Parent's `properties` is a sub-object with bare ident keys; child props live at the top level with `:` prefix. |
| User properties have a random suffix | `properties.status` is always `undefined` | Scan keys with `^:?user\.property\/<name>-[A-Za-z0-9]+$` instead of dot-access. |
| Built-in entity refs on children come through as `{dbId: N}` | Cards never bucket correctly | Async-fetch via `logseq.Editor.getBlockProperty(uuid, ident)` and cache. |
| Ident-string value retracts a ref property | Card loses its status (and possibly tag) after drag | Always pass the entity's **title** as the value, never the ident keyword. |
| Logseq outliner intercepts drags | Ghost blocks created on drop, or `removeChild` errors | Stop propagation on every drag handler, including `stopImmediatePropagation`. Use a custom MIME type. |
| No auto-refresh after property update | Card data updates but stays in old column until Cmd+R | Optimistically update local state on drop. |
| `subs: ['logseq.property/status']` does nothing | View doesn't react to status changes from outside the kanban | The `subs` field is documented as reserved; not yet wired. Don't rely on it. |

---

## 12. Troubleshooting

**Plugin won't load**
- Open Logseq's main DevTools (`Cmd+Option+I`) and look for errors at plugin load time.
- Check the plugin card on the Plugins page for an error badge.
- Confirm `package.json` has `"main": "dist/index.html"` and a `dist/index.html` exists.

**Renderer doesn't activate**
- Confirm `view::` (not `view:`) on the block; the `::` is significant.
- Confirm the property value is exactly `kanban` (case-insensitive matched, but no extra whitespace).
- Add `console.info` inside your `when` predicate and check whether it's being called.

**Cards don't appear**
- Add `console.info('[debug]', children[0])` inside the render and inspect the first card's shape.
- For ref-typed properties: confirm `getBlockProperty(uuid, ident)` is being called and returns an entity with a `title`.
- Check whether your column subset includes the status values your cards have.

**Drag does nothing**
- Add a log at the top of `onDrop`. If it doesn't fire, the event isn't reaching you — check `onDragOver`'s `preventDefault()`.
- If `onDrop` fires but `moveCard` silently succeeds without changing data: you may be hitting the destructive-value path. Verify you're passing the column title, not the ident.

**Card moves visually but reverts on Cmd+R**
- The DB write failed silently. Check console for `[kanban] failed to update status:` errors.
- Confirm `propIdent` is the colon-stripped form (`'logseq.property/status'`, not `':logseq.property/status'`).

---

## Where to go next

- **Add card metadata badges** — assignee, due date, priority — using the same `getBlockProperty` pattern.
- **Drag rollback on write failure** — revert the optimistic update if `upsertBlockProperty` throws.
- **Multi-property grouping** — let the user pick `view-by:: priority` and group by priority instead.
- **Settings panel** — `logseq.useSettingsSchema(...)` for plugin-global defaults.
- **Filter by tag** — show only `#Task` children, hide non-task siblings.
- **Marketplace packaging** — see `libs/guides/starter_guide.md` for icon, screenshots, and manifest fields.

The complete working POC lives in the separate `logseq-block-views` plugin repo (`~/Projects/src/github.com/logseq-dev/plugin-dev/logseq-block-views/`). Every snippet in this guide is reproducible from that codebase.

---

## Bonus: a much simpler example

If the kanban feels like a lot, the same POC also includes a small **callout** renderer at `src/callout.ts` — a single `registerBlockRenderer` call that turns any block with `type:: info` into a styled info-callout box. No async, no drag-and-drop, ~70 lines. Useful as a contrast and as proof that one plugin can host multiple renderers.

---

## Beyond this tutorial — what the POC also does

The tutorial walks through the minimum viable kanban (read, group, drag). The shipping POC builds on that with several quality-of-life additions you can study in `src/index.ts`:

- **Inline edit on double-click** — each card is a small function component (`Card`) with `useState` for edit mode. The display swaps to a `<textarea>` (Enter saves via `logseq.Editor.updateBlock`, Shift+Enter newlines, Escape cancels). Optimistic local title prevents the empty-state flicker.
- **Shift+Click → open in right sidebar** — `logseq.Editor.openInRightSidebar(uuid)` opens the full property editor. Gated on Shift so the two synthetic single-clicks of a double-click don't race.
- **+ Add card per column** — `AddCardForm` component renders a dashed-border button that expands into a textarea. On submit it calls `insertBlock(boardUuid, title)` followed by `upsertBlockProperty(newUuid, propIdent, column.title)`. Synthetic cards in `extraCards` state show immediately; props.children eventually catches up.
- **Card badges** — `priority`, `deadline`, `scheduled` are async-fetched per card via `getBlockProperty`. Date-typed properties arrive as raw Unix ms timestamps (not entity refs), formatted via `formatDate` as `Today` / `Tomorrow` / `Yesterday` / `May 13`. Overdue deadlines flip to red automatically.

Each of these took a couple of additional gotchas to land — see the POC's `REQUIREMENTS.md` (`logseq-block-views/REQUIREMENTS.md`, in the `logseq-dev/plugin-dev` workspace) for the full list. Most notable: `upsertBlockProperty` for ref-typed properties needs the entity **title** as the value, not the ident keyword — passing the ident silently retracts the property.

