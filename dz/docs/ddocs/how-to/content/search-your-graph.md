---
title: 'Search Your Graph: Which Tool for Which Job'
description: 'Cmd+K, Cmd+Shift+K, Cmd+F, queries, table search, the CLI and the MCP server: what each one actually searches, how matching works, and when to reach for which'
type: how-to
status: stable
lastVerified: '2026-09-23'
verifiedAgainst: 'a00871e20c'
reviewStatus: authored-from-source
logseqVersion: '2.0.x'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  tutorials:
    - '[../../tutorials/getting-started/semantic-search.md](../../tutorials/getting-started/semantic-search.md)'
  how-to:
    - '[../queries/advanced-queries.md](../queries/advanced-queries.md)'
    - '[../integrations/cli-basic.md](../integrations/cli-basic.md)'
  reference:
    - '[../../reference/configuration/keyboard-shortcuts.md](../../reference/configuration/keyboard-shortcuts.md)'
    - '[../../reference/api/mcp-server.md](../../reference/api/mcp-server.md)'
  explanation:
    - '[../../explanation/features/how-embeddings-work.md](../../explanation/features/how-embeddings-work.md)'
---

# Search Your Graph: Which Tool for Which Job

> **Goal**: Pick the right search surface for the question you have, and know why a result did or did not show up.

> Written from source at `a00871e20c` (2026-09-23). Line references point at that revision.
> The command palette, current-page search and find-in-page were traced in code; the CLI
> subcommand list was taken from `logseq search --help` on revision `50e1849-dirty`.
> Nothing on this page was observed in a running desktop app.

## The short version

Logseq DB has one search *engine* and several *doors* into it, plus one door that is not
a Logseq search at all.

| You want to… | Use | Why |
|---|---|---|
| Find a page, tag, property or block anywhere in the graph | **Cmd+K** (Ctrl+K) | Full-text search over every node title, with fuzzy fallback and semantic matches if enabled |
| Find a block on the page you are reading | **Cmd+Shift+K** | Same engine, scoped to the current page, so results are not crowded out by other pages |
| Jump to a word you can already see on screen | **Cmd+F** (desktop only) | Chromium's find bar; it highlights rendered text, including tables, code and property values |
| Run a command by name | **Cmd+Shift+P** | The same palette, filtered to commands |
| Keep a search open while you work | **Cmd+K**, then **Cmd+Enter** | Opens the results in the right sidebar as a persistent panel |
| Link to something while typing | `[[` or `#` in the editor | Node and tag autocomplete, backed by the same engine |
| Answer a structured question ("open tasks tagged X due this week") | A **query** block | Search matches text; queries match properties, tags, dates and relationships |
| Narrow a table or a tag's rows | The table's **search box** and **filter** chips | Filters the rows already in the view, no index involved |
| Search from a script, an agent or another machine | `logseq search …` (CLI), the **MCP server** | Same worker-side search functions, no UI |

The one rule that matters most: **Cmd+F searches pixels, everything else searches the
database.** Cmd+F cannot find anything that is collapsed, on another page, or not yet
rendered. Cmd+K cannot see a word that is only on screen because a query rendered it.

## 1. Cmd+K: the command palette

`:go/search` is bound to `mod+k` (`shortcut/config.cljs:306`). It lives in the
`editor-global` group (`:687`), so it works whether or not you are editing a block.
On mobile the search button in the footer fires the same event
(`mobile/footer.cljs:32`).

### What it searches

The palette runs several independent searches and shows them as groups
(`cmdk/core.cljs:84-94`, ordering at `:150-186`):

| Group | Source | Shown by default |
|---|---|---|
| **Create** | Your input, when no page with exactly that name exists | 1 item |
| **Current page** | Node results that live on the page you are viewing | up to 10 |
| **Nodes** | Pages, tags, properties, objects and blocks, all as one list | up to 10 |
| **Recently updated** | Pages by last edit, no search needed | 5 |
| **Commands** | Every keyboard command, by its label | 5 |
| **Files** | Graph files such as `config.edn` and `custom.css` | 5 |
| **Filters** | The "Search only …" switches described below | 5 |

"Show more" on the Nodes or Current page group re-runs the search with a limit of 100
instead of 10 (`cmdk/state.cljs:83`, `cmdk/core.cljs:921-925`). Other groups only ever
return what they already fetched.

A **node** means anything with a title: a page, a tag, a property, an object of a tag,
or a plain block. There is no separate "pages" list. Pages are ranked and shown in the
same Nodes group as blocks, so a page called *Meeting* and a block that mentions
*meeting* compete for the same ten slots.

### How matching works

The search index is a SQLite table of node titles with an FTS5 trigram index
(`worker/search.cljs:45-61`). For each block it stores the title with the parent
chain folded in, plus aliases for pages, plus the journal day for journal pages
(`:553-566`). Titles are case-folded and accent-stripped before indexing
(`:584-606`), so `cafe` finds *Café*. The property value text of a block is **not**
in the index; only the title is.

For a query, `search-blocks` (`:924-1006`) runs up to four strategies and fuses them:

1. **Exact title.** A single word is first matched against titles exactly, case
   insensitive (`:474-489`). If that already fills the limit, nothing else runs.
2. **Full text.** The trigram index matches your terms as substrings anywhere in the
   title, in any order. The words `and`, `or`, `not` (and `&`, `|`) between terms become
   FTS operators (`:355-377`). Punctuation in the query turns it into a quoted phrase
   with prefix matching, so `foo/bar` or `v2.1` search for that literal run of characters.
   Wrap terms in double quotes yourself to force a phrase.
3. **Fuzzy.** A `LIKE` pattern with wildcards between characters, scored by the
   fuzzy matcher (`:491-526`). It runs only when the exact and full-text passes did not
   already return a multi-word match, and it prefers page titles over block titles.
4. **Semantic.** If semantic search is on, the query embedding is compared with the
   vector index and the two lists are fused by reciprocal rank (`:907-922`, `:757-809`).
   See the tutorial *Turn On Semantic Search* for setup and the explanation *How Semantic
   Search Works* for the fusion. The Settings toggle appears only on macOS with Apple
   silicon and requires Python 3 (`settings.cljs:589-611`).

Two special forms:

- **`#tag`**: a query that starts with `#` and has no spaces is an exact tag-title lookup
  and skips full-text and fuzzy entirely (`:467-472`, `:928-929`). Type `#proj` and you
  get nothing unless a tag is literally called *proj*. Drop the `#` to search loosely.
- **One or two characters**: too short for trigrams, so a plain `LIKE` substring pass is
  added (`:938-940`). Expect noise.

### What is hidden from search

The engine drops results that are not for you to see (`:528-546`, `:819-838`):

- Hidden pages and blocks on hidden pages, except the quick-capture page.
- Private built-in pages. Public built-ins (tags such as `Task`, properties such as
  `Status`) do appear, because the palette passes `:built-in? true` (`cmdk/state.cljs:87`).
- Closed property values, blank blocks, and blocks whose title exceeds 10,000 characters
  are never indexed at all (`:590-593`).
- Deleted properties.

If a page you know exists is missing, check whether it is a *hidden* page first, then
rebuild the index (section 8).

### Filters: "Search only …"

Type `/` as the first character, or pick the Filters group, to restrict the palette to one
source (`cmdk/core.cljs:57-68`, dictionary at `en.edn:243-250`):

| Filter | What you get |
|---|---|
| Search only current page | Nodes whose page is the one you are on |
| Search only nodes | Pages, tags, properties, blocks, without commands or files |
| Search only codes | Only code blocks (display type `code` or tagged `Code-block`), fetched with a wider net of 300 candidates because most are filtered out (`cmdk/state.cljs:92-97`) |
| Search only commands | Same as Cmd+Shift+P |
| Search only files | Graph files by path |
| Search only themes | Installed themes, same as Cmd+Shift+I |

The active filter shows above the input. **Esc** clears it (`cmdk/core.cljs:1280`).
The last input and filter are remembered per graph, so reopening the palette resumes
where you left off (`:750-756`).

### Keys inside the palette

| Key | Action |
|---|---|
| Enter | Open the highlighted node, run the command, or create the page |
| Cmd+Enter | Open the highlighted result in the right sidebar, or open the whole search there (`:1271`) |
| `/` | Show the filters |
| Esc | Clear the filter, then close |

### Creating from the palette

When your input does not exactly match an existing page title, the first group is
**Create**. Plain text offers *Create page called '…'*; input starting with `#` offers
*Create tag called '…'* (`cmdk/core.cljs:100-118`). The names `config.edn`, `custom.js`
and `custom.css` never offer creation.

## 2. Cmd+Shift+K: only this page

`:go/search-in-page` (`shortcut/config.cljs:315`) opens the same palette with the
*current page* filter pre-applied. The engine receives the page uuid and every SQL
pass adds `page = ?` (`worker/search.cljs:951-955`, `cmdk/state.cljs:99-106`).

Use it when the page is long and a global search would bury the local hits under
better-ranked pages elsewhere. It finds blocks by their **title text**. It does not
find a page-level property value, a rendered query result, or the contents of an
embedded page, because those are not blocks of this page.

If the current view is not a page (for example the journals feed), the filter is not
offered at all (`cmdk/core.cljs:61-62`).

## 3. Cmd+F: find in the rendered page (desktop only)

`:go/electron-find-in-page` (`shortcut/config.cljs:318-320`) exists only in the
Electron build. The binding is removed at startup on web and mobile
(`:587`, `remove (comp :inactive val)`), so in a browser Cmd+F is the browser's own
find bar.

It is not a Logseq search. The find bar (`components/find_in_page.cljs`) sends your
text over IPC to the main process, which calls Chromium's `webContents.findInPage`
(`electron/find_in_page.cljs:5-13`). Chromium highlights matches in the current DOM
and reports `N/total` back. So:

- It finds any **visible text**: block content, property values, table cells, code,
  query results, the sidebar. That is the one thing Cmd+K cannot do.
- It does **not** find collapsed children, blocks scrolled far enough to be unmounted,
  or anything on another page.
- **Enter** or **Cmd+G** jumps to the next match, **Shift+Enter** or **Cmd+Shift+G** to
  the previous one (`shortcut/config.cljs:322-328`). These only act while the bar is open
  and you are not editing a block, otherwise Enter keeps its editor meaning
  (`handler/search.cljs:81-90`).
- The **Aa** button toggles match case. **Esc**, the X button, or clicking outside closes
  it and clears the highlights (`find_in_page.cljs:49-68`).

Reach for it when you are already on the right page and want the eye to land on a word.
Reach for Cmd+Shift+K when you want to *navigate to* a block.

## 4. Cmd+Shift+P and Cmd+Shift+I

Both open the same palette with a filter pre-applied: commands (`:command-palette/toggle`,
`mod+shift+p`) or themes (`:go/search-themes`, `mod+shift+i` on Mac, `alt+shift+i`
elsewhere) (`shortcut/config.cljs:309-313`). Commands are matched by their translated
label, so search in the language your UI is set to.

## 5. A persistent search in the sidebar

From the palette, **Cmd+Enter** on the input opens the search as a right-sidebar item
(`right_sidebar.cljs:97-106`). It is a full palette instance: you can keep editing the
query, apply filters, and the item's title follows your input. Blur the input and the
sidebar item is saved under the new text, so it survives navigation. Use it to keep a
"things to file" list beside the page you are working on.

## 6. Search while typing

The editor's autocompletes use the same worker search with a 150 ms debounce
(`components/editor.cljs:224-262`):

- `[[` searches **nodes** and offers to create the page if nothing matches exactly.
- `#` searches **tags** only and offers *create tag*.
- `/` lists slash commands, matched by label.

These are the fastest way to find a node when the goal is to link to it rather than
open it.

## 7. Structured search: queries and views

Text search answers "where does this word appear". It cannot answer "which tasks are
overdue" or "which pages have no tag". For those, write a query:

- Type `/Query` in a block for a DSL query, or use the advanced Datalog form. See
  *Advanced Queries* and the *Query Patterns* reference.
- Every query block and every tag page renders a **table view** with its own search box
  (`components/views.cljs:1661-1690`, mounted at `:3569-3571`). Typing there filters the
  rows already in the table by their property values. It is local to the view, ignores
  the search index, and Esc clears it. The **filter** chips beside it are persisted on
  the block as properties; the DSL cannot express them (see *Advanced Queries*, section
  "Filters, Group-by and Sort Without the UI").
- A page's **linked references** have their own include/exclude page filter behind the
  filter icon (`components/reference.cljs:14-33`).
- The **graph view** has a tag search that limits which tags are drawn
  (`components/graph.cljs:355-358`).

## 8. Search outside the app

The CLI and the MCP server call the same worker functions as the palette, so they see
the same index and obey the same hidden-node rules.

```bash
logseq search block    --graph my-graph --content "meeting notes"
logseq search page     --graph my-graph --content "home"
logseq search property --graph my-graph --content "due"
logseq search tag      --graph my-graph --content "project"
```

`--content` is required and the scope must be one of the four. Add `-o json` for
machine-readable output. For structured questions use `logseq query` with a Datalog
form or one of the built-in named queries. See *CLI Basics*.

The MCP server exposes a search tool over HTTP for agents; see the *MCP Server*
reference. The plugin API exports a `search` function for plugins
(`logseq/api.cljs:187`).

## 9. When search misses

- **Rebuild the index.** Run the *Rebuild search index* command from Cmd+Shift+P, or
  press `mod+c mod+s` (`:search/re-index`). It drops and recreates the SQLite tables
  from the database (`handler/search.cljs:109-118`, `worker/search.cljs:63-89`). Do
  this after a sync that replaced many blocks or when a page you can open never shows
  up in Cmd+K.
- **Accents.** Indexing always strips accents, and the query side strips them when
  `:feature/enable-search-remove-accents?` is true, which is the default
  (`state.cljs:431`). Set it to `false` in `config.edn` only if you need `resume` and
  `résumé` to differ.
- **Short queries.** One or two characters fall back to substring matching and rank
  poorly. Type a third character.
- **`#` prefix.** Remember that `#word` is an exact tag lookup. Remove the `#` for a
  loose search.
- **Property values.** Cmd+K does not index them. Search for the node the value points
  to instead, or use a query, or Cmd+F on the page that shows it.
- **Semantic results look wrong.** Semantic matches are fused with keyword matches; a
  low-scoring vector hit can outrank a weak keyword hit. Turn the toggle off in
  Settings to compare.

## Comparison at a glance

| | Cmd+K | Cmd+Shift+K | Cmd+F | Table search | Query | CLI / MCP |
|---|---|---|---|---|---|---|
| Scope | Whole graph | Current page | Rendered text on screen | Rows in one view | Whole graph | Whole graph |
| Matches | Node titles | Node titles | Any visible text | Property values in the view | Properties, tags, refs, dates, text | Node titles |
| Fuzzy / semantic | Yes / optional | Yes / optional | No | No | No | No |
| Finds collapsed content | Yes | Yes | No | n/a | Yes | Yes |
| Result | Navigate, create, run | Navigate | Highlight | Filtered rows | Live block | Text or JSON |
| Platforms | All | All | Desktop only | All | All | Terminal |

## Sources

- `src/main/frontend/modules/shortcut/config.cljs:306-328,587,687-691` shortcut bindings and the Electron-only gate
- `src/main/frontend/components/cmdk/core.cljs:51-118,120-191,281-470,750-780,921-925,1271-1280` palette groups, ordering, loaders, filters, tips
- `src/main/frontend/components/cmdk/state.cljs:81-111` search options per filter group
- `src/main/frontend/worker/search.cljs:21-89,347-395,463-546,553-626,811-838,907-1006` index schema, query parsing, strategies, hidden-node rules, result fusion
- `src/main/frontend/handler/search.cljs:44-118` find-in-page state machine and index rebuild
- `src/main/frontend/components/find_in_page.cljs`, `src/electron/electron/find_in_page.cljs`, `src/electron/electron/handler.cljs:584-588`, `src/main/electron/listener.cljs:84-90` the Cmd+F round trip
- `src/main/frontend/components/right_sidebar.cljs:91-106` sidebar search
- `src/main/frontend/components/editor.cljs:224-286` autocomplete search
- `src/main/frontend/components/views.cljs:1661-1690,3569-3571` table view search box
- `src/main/frontend/components/settings.cljs:589-611` semantic search toggle
- `src/resources/dicts/en.edn:227-272,1645-1655` palette and find-bar strings
