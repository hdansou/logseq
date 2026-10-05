---
title: 'Create a Logseq DB theme, end to end'
description: 'From a design system to a released theme: map tokens, beat the core selectors, verify every selector against source, validate live, test with a purpose-built graph, package and ship'
status: stable
quadrant: 'How-to'
lastVerified: '2026-09-29'
verifiedAgainst: 'Logseq 2.0.1 web (app.logseq.com) + Logseq-DB.app, master 9c18a5432b'
reviewStatus: audited
graphType: [db]
relatedDocs:
  - '[Create a Theme (ddocs)](../../ddocs/how-to/integrations/create-theme.md)'
  - '[Build a Custom Theme tutorial (ddocs)](../../ddocs/tutorials/advanced/custom-theme-tutorial.md)'
tags: ['themes', 'CSS', 'custom.css', 'plugin', 'testing']
---

# Create a Logseq DB theme, end to end

This is the process used to build the **Inkwell** theme (Slate variant) (deep slate `#223B3B` + `#9C2529` on Apple system greys). Each step lists what to do and the traps we actually hit. The worked example lives in:

- `~/Projects/src/github.com/logseq-inkwell-theme/themes/inkwell-slate.css`: the theme (Slate variant), one file for light and dark, generated from `src/base.css` + `src/palettes/slate.css`
- `~/Projects/src/github.com/logseq-inkwell-theme/tools/theme-lab/build_theme_lab.py`: generator for the human-testing graph (`npm run lab`)
- `~/Projects/src/github.com/logseq-inkwell-theme`: the release repo (plugin package + custom.css)

**Verified scope (2026-09-29):** selectors and variables were read in source at master `9c18a5432b`, then checked live on app.logseq.com (Logseq 2.0.1, DB graph) by injecting the CSS and reading computed styles in light and dark, with the accent set to `logseq`, `tomato`, `blue` and `none`. The Theme-Lab graph was opened in Logseq-DB.app and checked with one screen capture. The date picker, the sidebar's active item and the properties area were checked in source only.

The six steps:

1. [Write your tokens](#1-write-your-tokens)
2. [Map them onto Logseq's three variable layers](#2-map-tokens-onto-logseqs-three-variable-layers)
3. [Use a selector that beats core](#3-use-a-selector-that-beats-core)
4. [Verify every component selector against source](#4-verify-every-component-selector-against-source)
5. [Validate live in the browser](#5-validate-live-on-applogseqcom)
6. [Test with people using the Theme-Lab graph](#6-human-testing-with-the-theme-lab-graph)

Then [package and release](#7-package-and-release). The [pitfalls table](#pitfalls-we-hit) collects every trap in one place.

---

## 1. Write your tokens

Keep your design system in its own namespace (`--ink-*` here) so it never collides with Logseq's variables. Define light values on `:root` and dark overrides on `html[data-theme="dark"]`. Core sets `data-theme` to `light` or `dark` on `<html>`, and never to anything else ([`src/main/frontend/components/theme.cljs:52`](../../../../src/main/frontend/components/theme.cljs)). `html.dark` is added alongside it, so you don't need it.

You need **three forms** of each colour, because Logseq consumes colours three ways:

| Form | Example | Used by |
|---|---|---|
| Hex/rgba | `--ink-tint: #223b3b` | `--ls-*`, `--lx-*` and your own rules |
| Bare HSL channels | `--ink-c-tint: 180 27% 18%` | shui/Tailwind tokens (`--primary`, …) |
| 12-step scales | `--ink-g01…12`, `--ink-a01…12` | `--lx-gray-*`, `--lx-accent-*` |

Convert hex to channels with any HSL converter (we used Python `colorsys`). Build the scales from your greys and your tint, from 01 (app background) to 12 (strongest text).

## 2. Map tokens onto Logseq's three variable layers

Core CSS reads variables in a fixed priority. Setting only the legacy `--ls-*` layer changes little, because most rules check `--lx-*` first. For example, page links use `var(--lx-accent-11, var(--ls-link-text-color, …))` ([`src/main/frontend/components/block.css:915-921`](../../../../src/main/frontend/components/block.css)), and the scales are always set by the accent colour ([`src/main/frontend/shui/colors.css:1-35`](../../../../src/main/frontend/shui/colors.css)).

| Layer | Variables | Format | What reads it |
|---|---|---|---|
| **Scales** (read first) | `--lx-gray-01…12`, `--lx-accent-01…12`, plus `-alpha` variants | any colour | page refs and tags (`accent-11`), bullets (`gray-08`), collapsed halo and guide lines (`gray-04-alpha`), properties (`gray-03`), sidebar (`gray-02`), title (`gray-12`), Tailwind `gray-*`/`accent-*` utilities ([`tailwind.config.js:4-42`](../../../../tailwind.config.js)) |
| **shui tokens** | `--background --foreground --card --popover --primary --secondary --muted --accent --destructive --border --input --ring` (+ `-foreground`) | **bare `H S% L%` channels** | every `bg-primary`, `border-border`, `ring-ring`… as `hsl(var(--x))` ([`tailwind.config.js:198-228`](../../../../tailwind.config.js)): buttons, dialogs, popovers, inputs, calendar |
| **Classic `--ls-*`** | `--ls-primary-background-color`, `--ls-link-text-color`, `--ls-page-mark-bg-color`, `--ls-page-blockquote-*`, `--ls-block-highlight-color`, `--ls-highlight-color-<name>` … | any colour | fallbacks after `--lx-*`, and the only source for highlights, blockquotes, selection, and the DB background-colour property |

Inkwell's mapping (abridged; the full block is in the theme file):

| Design token | Logseq variables |
|---|---|
| surface `#fff` / `#1c1c1e` | `--ink-g01`→`--lx-gray-01`, `--background`, `--card`, `--ls-primary-background-color` |
| bg `#f5f5f7` | `--lx-gray-02`, `--ls-secondary-background-color` |
| sidebar `#f2f2f5` | `--left-sidebar-bg-color` (see [pitfalls](#pitfalls-we-hit)) |
| float (popovers) | `--popover` |
| label / label-2 / label-3 | `--lx-gray-12` / `--lx-gray-11` / `--lx-gray-08`; `--foreground`, `--muted-foreground`, `--ls-block-bullet-color` |
| line / line-soft | `--input` / `--border`, `--ls-border-color`, `--lx-gray-06` / `--lx-gray-04` |
| fill / fill-strong (alpha) | `--lx-gray-03-alpha` / `--lx-gray-04-alpha`, `--ls-page-inline-code-bg-color` |
| tint `#223b3b` | `--primary`, `--ring`, `--lx-accent-09`, `--lx-accent-11` |
| tint-soft | `--accent`, `--lx-accent-07-alpha` (palette highlight), `--ls-block-highlight-color`, `--ls-selection-background-color` |
| accent `#9c2529` | `--ls-cloze-text-color`, today's date in the calendar |
| highlight palette | `--ls-highlight-color-{yellow,red,pink,green,blue,purple,gray}`, `--ls-page-mark-bg-color` |

## 3. Use a selector that beats core

Core declares its defaults in two places:

- For the default accent, on `html[data-theme=light|dark][data-color=logseq]`, specificity 0,2,1 ([`src/main/frontend/shui/vars-classic.css:42`](../../../../src/main/frontend/shui/vars-classic.css), [`:100`](../../../../src/main/frontend/shui/vars-classic.css); [`colors.css:1-54`](../../../../src/main/frontend/shui/colors.css)).
- For every other accent, on `body`/`.light-theme`/`.dark-theme` ([`colors.css:57-58`](../../../../src/main/frontend/shui/colors.css)). A declaration on `body` shadows anything inherited from `<html>`.

`data-color` itself is set from the Accent color setting ([`theme.cljs:62-66`](../../../../src/main/frontend/components/theme.cljs)).

So `:root { … }` and `html[data-theme="light"] { … }` **lose**. Put the whole mapping block on:

```css
html[data-theme][data-color], html[data-theme][data-color] body { /* --lx-*, shui tokens, --ls-* */ }
```

This ties with core on `<html>` and wins because custom.css and plugin themes load later. Both are appended to `<head>`: custom.css through `util/add-style!` from [`src/main/frontend/handler/ui.cljs:139-146`](../../../../src/main/frontend/handler/ui.cljs), plugin themes as a `<link>` in [`libs/src/common.ts:595-599`](../../../../libs/src/common.ts). On `body` it beats `html[data-color=X] body` (0,1,2). `data-color="none"` matches no core rule, and `[data-color]` still matches it.

Your `--ink-*` tokens can stay on `:root` / `html[data-theme="dark"]`, because nothing in core competes for those names.

**Verified live:** with the accent set to logseq, tomato, blue and none, `--primary`, `--lx-gray-02`, `--lx-accent-11`, the link colour and the sidebar background all stayed on Inkwell values.

## 4. Verify every component selector against source

Class names drift between releases, and many selectors from older themes no longer exist. For each selector in your draft:

```bash
# count source files that mention the class (cljs + css + shui)
grep -rlE '(^|[^A-Za-z0-9_-])page-ref([^A-Za-z0-9_-]|$)' src/main deps/shui/src resources/css | wc -l
```

We ran this as a script over every class in the draft, then read the render code for anything with zero or surprising hits. Watch out for zsh here: `--include=*.cljs` globs fail with "no matches found", so use Python or quote the pattern.

### Selectors that exist (DB graphs, master 9c18a5432b)

| Surface | Selector | Source |
|---|---|---|
| Page title | `.ls-page-title-container` (it is a block); size from `--ls-page-title-size` | [`block.cljs:3606`](../../../../src/main/frontend/components/block.cljs), [`common.css:334-337`](../../../../src/main/frontend/common.css) |
| Headings (rendered) | `.ls-block h1…h6` (`h1.block-title-wrap.as-heading`) | [`block.cljs:2602`](../../../../src/main/frontend/components/block.cljs) |
| Headings (editing) | `.editor-inner .h1…h6` (the class is on the textarea) | [`editor.cljs:550-566`](../../../../src/main/frontend/components/editor.cljs) |
| Bullets | `.bullet-container:not(.typed-list) .bullet`; hover `.bullet-link-wrap:hover > .bullet-container…` (core uses `!important`) | [`block.css:1291-1365`](../../../../src/main/frontend/components/block.css) |
| Collapsed bullet | `.bullet-container.bullet-closed` | [`block.cljs:2463`](../../../../src/main/frontend/components/block.cljs) |
| Guide line | `.block-children` border-left (core `!important`); `.block-children-left-border` is only the click strip | [`block.css:648-675`](../../../../src/main/frontend/components/block.css) |
| Page link | `a.page-ref` inside `span.page-reference`; brackets `.page-reference .bracket` | [`block.cljs:827`](../../../../src/main/frontend/components/block.cljs), [`:1308-1311`](../../../../src/main/frontend/components/block.cljs) |
| Inline tag / tag chips | `a.tag` / `.block-tag a.tag` | [`block.cljs:827`](../../../../src/main/frontend/components/block.cljs), [`:2993-3000`](../../../../src/main/frontend/components/block.cljs) |
| External link | `a.external-link` | [`block.cljs:1643`](../../../../src/main/frontend/components/block.cljs) |
| Block ref `[[uuid]]` | renders as `.page-reference .page-ref`; `.block-ref` only on legacy/embed paths | live check |
| Properties | `.ls-properties-area`, `.property-key-inner`, `a.property-k` | [`property.cljs:357-387`](../../../../src/main/frontend/components/property.cljs) |
| Background colour | inline `var(--ls-highlight-color-<name>)`, so set the variables | [`block.cljs:3602-3613`](../../../../src/main/frontend/components/block.cljs) |
| Callouts | `.admonitionblock.note|tip|important|warning|caution|pinned`, `.admonition-icon` (has `border-r`) | [`ui.cljs:650-662`](../../../../src/main/frontend/ui.cljs), [`block.cljs:4851-4866`](../../../../src/main/frontend/components/block.cljs) |
| Quote | `blockquote`, `.block-content-or-editor-wrap[data-node-type='quote']`; colours from `--ls-page-blockquote-*` | [`common.css:118-133`](../../../../src/main/frontend/common.css) |
| Task status / priority | `.ls-icon-Backlog|Todo|InProgress50|InReview|Done|Cancelled`, `.ls-icon-priorityLvlLow|Medium|High|Urgent` | [`deps/db/.../property.cljs:313-337`](../../../../deps/db/src/logseq/db/frontend/property.cljs), [`shui/icon/v2.cljs:34`](../../../../deps/shui/src/logseq/shui/icon/v2.cljs) |
| Left sidebar | `.left-sidebar-inner.as-container`, `a.item`, `a.item.active`, group header `.sidebar-content-group-inner > .hd`; bg `--left-sidebar-bg-color` | [`container.css:71-175`](../../../../src/main/frontend/components/container.css), [`theme.css:201`](../../../../src/main/frontend/components/theme.css) |
| Popovers / menus | `.ui__popover-content`, `.ui__dropdown-menu-content`, `.ui__context-menu-content`, `.ls-preview-popup` | [`deps/shui/.../components.cljs:740`](../../../../deps/shui/src/logseq/shui/components.cljs), [`:819`](../../../../deps/shui/src/logseq/shui/components.cljs) |
| Dialogs | `.ui__dialog-overlay`, `.ui__dialog-content` (Base UI) | [`components.cljs:824-842`](../../../../deps/shui/src/logseq/shui/components.cljs) |
| Command palette | `.cp__cmdk`, `.cp__cmdk-search-input`, `[data-cmdk-item][data-highlighted]` | [`cmdk/list_item.cljs:84-89`](../../../../src/main/frontend/components/cmdk/list_item.cljs) |
| Keycaps | `.shui-shortcut-combo > kbd.shui-shortcut-key` | live check; [`shortcut.css:138`](../../../../src/main/frontend/components/shortcut.css) |
| Buttons | `.ui__button.as-solid|as-secondary|as-outline|as-ghost` (variant is a class, not `data-variant`) | [`components.cljs:470-495`](../../../../deps/shui/src/logseq/shui/components.cljs) |
| Inputs | `.ui__input`, `.form-input` | [`components.cljs:535`](../../../../deps/shui/src/logseq/shui/components.cljs) |
| Calendar | `.ui__calendar [data-selected]`, `[data-today]` (react-day-picker 9.14, custom classNames) | [`components.cljs:558-590`](../../../../deps/shui/src/logseq/shui/components.cljs) |
| Code | `.CodeMirror` (CodeMirror 5.65), `.CodeMirror-gutters`, inline `:not(pre) > code` | `package.json` `codemirror 5.65.18`, [`common.css:594`](../../../../src/main/frontend/common.css) |

### Selectors that do not exist (remove them)

| Dead selector | Why | Use instead |
|---|---|---|
| `[data-radix-popper-content-wrapper]`, `[data-radix-dialog-overlay]` | UI moved to Base UI (`@base-ui/react`) | `.ui__popover-content`, `.ui__dialog-overlay` |
| `.tippy-box`, `.preview-ref-wrap`, `.ui__modal-overlay`, `.ui__modal-panel` | removed | `.ls-preview-popup`, `.ui__dialog-*` |
| `.cp__palette*`, `.cp__cmdk-item`, `.cp__cmdk-group-header` | old palette | `[data-cmdk-item]` (group headers have no class) |
| `button[data-variant=…]` | variant is a class | `.ui__button.as-*` |
| `.rdp-day_selected`, `.rdp-day_today` | shui replaces the classNames | `[data-selected]`, `[data-today]` |
| `[data-bg-color]`, `.bg-green` … | the DB background colour is an inline var | `--ls-highlight-color-*` |
| `.marker-switch`, `.doing/.done` markers, `span.priority` | DB tasks are icons | `.ls-icon-<Name>` |
| `.block-properties`, `.page-properties` | file graphs only | `.ls-properties-area` |
| `.nav-header`, `[data-active]`, `--ls-left-sidebar-bg-color` | not in the sidebar | see the sidebar row above |
| `.editor-inner.h1` (compound) | class is on the textarea inside | `.editor-inner .h1` |
| `.block-refs-count`, `.references-count` | count is a plain shui button | no hook |
| bare `.tag` | also matches KaTeX equation numbers | `a.tag` |

## 5. Validate live on app.logseq.com

The web app runs a real DB graph (an in-memory "Demo", thrown away on reload), so you can test without touching your own graphs.

1. Open app.logseq.com in Chrome and read the version: `(await logseq.api.get_app_info()).version` (it was 2.0.1). Confirm the graph is a DB graph with `logseq.api.get_current_graph()` (`logseq_db_Demo`).
2. Create a test page with the plugin API (`create_page`, `append_block_in_page`, `insert_block`, `upsert_block_property`, `add_block_tag`) covering every surface. Statuses take titles: `upsert_block_property(uuid, 'logseq.property/status', 'Doing')`.
3. Inject the theme as a temporary `<style id="hs-test-style">` appended to `<head>`. Put the CSS in a `String.raw` template literal, because a `\2197` escape is a syntax error in a normal one. Check `sheet.cssRules.length` equals your rule count; a parse drop shows up here.
4. Run a checker that:
   - lists selectors with no matching element (either not on screen or dead), and
   - compares `getComputedStyle` against the resolved token for about 25 key properties: link colour, tag background, bullet, guide line, sidebar, H1/H2 size, title size, background colour, mark, code, callouts, blockquote, status icons.
   Normalise colours through a throwaway element's `style.color` before comparing.
5. Repeat after `logseq.api.set_theme_mode('dark')` and after `document.documentElement.setAttribute('data-color', 'tomato')` (and `blue`, `none`).
6. Open overlays and screenshot them: ⌘K (palette), a right-click on a bullet (context menu).
7. Remove the style tag and close the tab.

What this caught that reading the source didn't: the page-title shrink, the `svg.note` colour, the keycap markup and the CodeMirror gutter. A second pass (the Inkwell build, same day) caught the callout SVG `fill` attribute, the keycap specificity and a transition-timing false alarm (see [pitfalls](#pitfalls-we-hit)). The screenshot found the `fill` problem after the computed-style check had passed.

Serving the built file beats pasting it into the console: run a tiny local server that sends `Access-Control-Allow-Origin: https://app.logseq.com` and `Access-Control-Allow-Private-Network: true`, then `fetch('http://127.0.0.1:<port>/<file>.css')` from the page and set a `<style>`'s text. Every reload then tests exactly what `npm run build` produced.

## 6. Human testing with the Theme-Lab graph

Automated checks prove selectors match. They don't prove the theme looks right. For that, build a persistent graph that exercises every surface and has a feedback checklist in it.

**Build.** `tools/theme-lab/build_theme_lab.py` in the theme repo writes a sqlite.build EDN with:
- 12 test pages: typography, outliner, links and references, properties and tags (`#Book`, `#Person`), all task statuses and priorities, all callouts, quotes, 7 background colours, code and math, tables, a flashcard
- three journal days
- a **Feedback** page with 22 Todo checklist items, each with hints and an empty `Notes:` block
- the theme embedded as the graph file:

```clojure
:logseq.db.sqlite.export/graph-files [{:file/path "logseq/custom.css" :file/content "…"}]
```

(`::graph-files` is appended to `:misc-tx` for `:graph-human` imports: [`deps/db/src/logseq/db/sqlite/export.cljs:1285-1326`](../../../../deps/db/src/logseq/db/sqlite/export.cljs).)

```bash
python3 build_theme_lab.py
logseq graph create --graph Theme-Lab
logseq graph import --graph Theme-Lab --type edn --input theme-lab.edn --timeout-ms 60000
logseq graph validate --graph Theme-Lab          # expect :errors nil
logseq query --graph Theme-Lab --query '[:find ?t :where [?p :block/name] [?p :block/title ?t] [(missing? $ ?p :db/ident)]]'
```

**Import traps:**

| Trap | What happens | Fix |
|---|---|---|
| Import "succeeds" and writes nothing | the CLI prints `Imported edn…` and exits 0 on a rejected import. The error is only in `~/logseq/graphs/<graph>/db-worker-node-<date>.log`, for example `Lookup ref attribute should be marked as :db/unique: [:build/page {:build/journal 20260915}]` | always verify with queries; grep the log for `console.error]` |
| `[:build/page …]` property values | only translated with `:translate-property-values?`, which the import path doesn't set ([`build.cljs:78-79`](../../../../deps/db/src/logseq/db/sqlite/build.cljs)) | give target pages a fixed `:block/uuid` + `:build/keep-uuid? true` and use `[:block/uuid <uuid>]` |
| `[[Title]]` links in block text | stored as plain text: no ref, no backlink | give every page and tag a deterministic uuid (uuid5 of the title) and rewrite links to `[[uuid]]`; inline tags become `#[[uuid]]` |
| Class page names | built from the EDN key, including any random suffix | set `:block/name` on each class |
| Re-running into the same graph | duplicates or conflicts | `logseq graph remove` + `create` before each import (this also deletes collected feedback, so read it first) |

**Open it for the tester.** The desktop app handles `logseq://graph/<name>?page=<title>` and opens the graph in a new window ([`src/electron/electron/url.cljs:36-60`](../../../../src/electron/electron/url.cljs)). With two Logseq builds installed, target the right one explicitly:

```bash
open -a /Applications/Logseq-DB.app "logseq://graph/Theme-Lab?page=Theme%20Lab%20%E2%80%94%20Start%20Here"
```

custom.css is applied when the graph loads. Nothing needs to be enabled.

**Collect feedback.** The tester ticks each checklist item and writes under its `Notes:` block, which carries an `area` property. Read it back with the CLI:

```bash
logseq query --graph Theme-Lab --query '[:find ?area ?note :where [?n :user.property/area ?a] [?a :block/title ?area] [?c :block/parent ?n] [?c :block/title ?note]]'
```

## 7. Package and release

- **custom.css (personal):** paste the theme file into Settings › General › Custom theme › Edit custom.css. In a DB graph it is stored in the graph database ([`deps/db/src/logseq/db/sqlite/create_graph.cljs:211-215`](../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)) and reapplied on save.
- **Plugin theme (shareable):** `package.json` `logseq.themes` entries with `mode: "light"` and `mode: "dark"`. `selectTheme` ejects the previous theme and appends a `<link>` ([`libs/src/LSPlugin.core.ts:1858-1877`](../../../../libs/src/LSPlugin.core.ts)). Because the Inkwell file carries both modes behind `html[data-theme]`, both entries can point at the same CSS file. The same file doubles as custom.css, so there is one source of truth.
- **Release checklist:**
  - structural tests pass: the activation selector is present, shui tokens are bare channels, both 12-step scales are defined, and no dead selectors from the table above appear
  - a live validation run (step 5) against the current app version
  - one Theme-Lab feedback round with every checklist item ticked
  - screenshots in light and dark
  - CHANGELOG and version bump
  - marketplace `manifest.json` PR

---

## Pitfalls we hit

| # | Symptom | Cause | Fix |
|---|---|---|---|
| 1 | Dialogs, menus, buttons and inputs lose all colour | shui tokens set to hex: `--background: var(--ink-surface)` makes `hsl(#fff)`, which is invalid | channel tokens `--ink-c-*: 180 27% 18%` ([`tailwind.config.js:198-228`](../../../../tailwind.config.js)) |
| 2 | Theme partly applies, and only with some accents | mapping on `:root`/`html[data-theme]` loses to `[data-theme][data-color=logseq]` and to `body` declarations | `html[data-theme][data-color], … body` |
| 3 | `--ls-link-text-color` etc. have no effect | core reads `--lx-*` first | define both 12-step scales |
| 4 | Sidebar colour ignored | `--left-sidebar-bg-color` is defined on `main.theme-container-inner` ([`theme.css:200-201`](../../../../src/main/frontend/components/theme.css)) | override it on that selector |
| 5 | Page title renders at body size | the DB title is a block, so `.ls-block .block-content { font-size }` applies | `.ls-page-title-container .block-content { font-size: inherit }` |
| 6 | NOTE callout icon stays yellow | core colours `svg.note` directly ([`common.css:500`](../../../../src/main/frontend/common.css)) | `.admonitionblock .admonition-icon svg { color: inherit }` |
| 7 | Keycap style never applies | keycaps are `.shui-shortcut-combo > kbd.shui-shortcut-key`, not `kbd`/`.ui__button-shortcut-key` | target the shui classes |
| 8 | Beige (light) / teal (dark) strip beside code | CodeMirror 5 theme gutter | `.CodeMirror-gutters { background: transparent !important }` |
| 9 | Hover bullet colour ignored | core hover rule uses `!important` | match with `!important`; a blanket "no `!important`" rule doesn't survive contact with core |
| 10 | Guide-line colour ignored | the visible line is `.block-children`'s border, with `!important` in core ([`block.css:674`](../../../../src/main/frontend/components/block.css)) | `.block-children { border-left-color: … !important }` |
| 11 | Theme-Lab import silently empty | rejected EDN still exits 0 | query after import; read the worker log |
| 12 | Links in the test graph have no backlinks | `[[Title]]` is text in DB graphs; only `[[uuid]]` is a ref | deterministic uuids + rewrite |
| 13 | A second `[[` on a line double-underlines | border on both `span.page-reference` and `a.page-ref` | style `a.page-ref` only |
| 14 | IMPORTANT/WARNING/CAUTION callout icons keep core colours though `color` is right | those SVGs carry a `fill="var(--color-…)"` attribute | add `fill: currentColor`; check computed `fill`, not just `color` |
| 15 | Keycap border and text size ignored | core `.shui-shortcut-combo.shui-shortcut-glow` (0,2,0) sets `border: none`; `kbd.shui-shortcut-key` (0,1,1) sets size and colour | match those selectors: `.shui-shortcut-combo.shui-shortcut-glow`, `.shui-shortcut-combo kbd.shui-shortcut-key` |
| 16 | A computed-style check reports the old colour | bullets animate `background-color` (0.12s); the check ran mid-transition | wait ~1s after injecting before reading styles |
| 17 | Computed checks pass but the screenshot is wrong | a check reads the wrong property (e.g. `color` on an SVG painted by `fill`) | always pair computed checks with a screenshot |
| 18 | Hovered row in the tag-table filter is invisible in dark mode, text dims | core `#ui__ac-inner .menu-link.chosen` (1,2,0) paints it `--lx-gray-04` (= popover colour in dark), and `.cp__select-main .menu-link.chosen` sets secondary text; a token-contrast test passed because the token never reached the row | match those selectors; give floating-list rows their own `--ink-hover` token lighter than the popover in dark mode; verify the hovered element's computed background live |
