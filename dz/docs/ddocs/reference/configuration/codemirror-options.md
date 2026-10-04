---
title: 'Code Block Editor Options (:editor/extra-codemirror-options)'
description: 'Which CodeMirror 5 options work in Logseq code blocks through :editor/extra-codemirror-options: tested values, keys Logseq overrides, options that do nothing, and two that crash the app'
type: reference
status: stable
lastVerified: '2026-10-03'
verifiedAgainst: 'c96e47732b'
reviewStatus: verified-live
logseqVersion: '2.0.1'
platforms: [desktop]
graphType: [db]
relatedDocs:
  reference:
    - '[config.edn Reference](config-edn.md)'
    - '[CSS Variables Reference](css-variables.md)'
  howTo:
    - '[Custom CSS](../../how-to/integrations/custom-css.md)'
---

# Code Block Editor Options (`:editor/extra-codemirror-options`)

> **Last revised:** 2026-10-03

> Reference for the options you can pass to the code block editor from `config.edn`.
> Logseq's code blocks are [CodeMirror 5](https://codemirror.net/5/doc/manual.html#config)
> editors (version 5.65.18, `package.json:162`).

**How this was verified.** Each value in the tables below was set on a throwaway DB graph in
Logseq desktop 2.0.1 with `logseq.api.set_current_graph_configs`, the page with a JavaScript
code block was reopened, and the option was read back from the live editor with
`cm.getOption(...)`. Unless a row says "seen", the check is that the editor holds the new
value, not that its visible effect was observed. Web and mobile were not tested.

## Example

```clojure
:editor/extra-codemirror-options {:lineWrapping true
                                  :lineNumbers  false
                                  :tabSize      2}
```

The setting is per graph. An open code block picks it up the next time it is rendered
(reopen the page).

## How Logseq builds the options

`render!` in `src/main/frontend/extensions/code.cljs:425-450` merges, later entries winning:

1. Logseq's defaults: `:theme`, `:autoCloseBrackets true`, `:lineNumbers true`,
   `:matchBrackets` (true only for Clojure, EDN, Scheme and Lisp blocks), `:styleActiveLine true`.
2. Your `:editor/extra-codemirror-options` map.
3. Logseq's fixed keys: `:mode`, `:tabIndex`, `:extraKeys`.
4. In published graphs: `:readOnly true` and `:cursorBlinkRate -1`.

Consequences:

- **Any key is accepted.** The config schema only requires a map
  (`src/main/frontend/schema/handler/common_config.cljc:56`). The map goes to CodeMirror as is,
  and keys CodeMirror does not know are ignored.
- **`:mode` cannot be set.** It always comes from the block's language. Tested: `:mode "python"`
  on a `js` block left the mode at `text/javascript`.
- **`:extraKeys` cannot be set.** Logseq replaces it with its own Esc binding. Tested: a
  `Ctrl-Q` binding never reached the editor.
- **`:tabIndex` does nothing, `:tabindex` works.** Logseq passes `:tabIndex -1`
  (`code.cljs:435`), but CodeMirror's option is spelled `tabindex`, so the live value stays
  `null`. Setting `:tabindex 3` in the config reached the editor.
- **`:readOnly` is dropped while you edit `config.edn` itself** (`code.cljs:431-433`), so a
  read-only setting cannot lock you out of the config file.
- **Only four addons are bundled** (`code.cljs:3-6`): `closebrackets`, `matchbrackets`,
  `show-hint` and `active-line`. Options that belong to any other addon have nothing behind them.

## Options that work

| Option | Logseq default | Tested value | Result |
| --- | --- | --- | --- |
| `:lineWrapping` | `false` | `true` | seen: long lines wrap (`CodeMirror-wrap` class) |
| `:lineNumbers` | `true` | `false` | seen: gutter numbers gone |
| `:readOnly` | `false` | `true`, `"nocursor"` | held |
| `:cursorBlinkRate` | `530` | `-1` | seen: cursor hidden |
| `:showCursorWhenSelecting` | `false` | `false` | held (this is the default) |
| `:theme` | `"solarized light"` / `"solarized dark"` | `"monokai"` | seen: class `cm-s-monokai` applied; see [Themes](#themes) |
| `:tabSize` | `4` | `8` | held |
| `:indentUnit` | `2` | `4` | held |
| `:indentWithTabs` | `false` | `true` | held |
| `:smartIndent` | `true` | `false` | held |
| `:matchBrackets` | `false` (`true` for Lisp-like languages) | `true` | held |
| `:autoCloseBrackets` | `true` | `false` | held |
| `:styleActiveLine` | `true` | `false` | held |
| `:firstLineNumber` | `1` | `10` | held |
| `:viewportMargin` | `10` | `50` | held |
| `:direction` | `"ltr"` | `"rtl"` | held |
| `:spellcheck` | `false` | `true` | held |
| `:dragDrop` | `true` | `false` | held |
| `:undoDepth` | `200` | `10` | held |
| `:tabindex` | `null` | `3` | held |
| `:hintOptions` | `null` | `{:completeSingle false}` | held |
| `:inputStyle` | `"textarea"` | `"contenteditable"` | held |
| `:scrollbarStyle` | `"native"` | `"null"` | held (hides scrollbars; other values crash, see below) |

A commonly shared snippet, confirmed valid as a whole:

```clojure
:editor/extra-codemirror-options {:lineWrapping true
                                  :readOnly true
                                  :cursorBlinkRate -1
                                  :showCursorWhenSelecting false}
```

It wraps long lines, makes code blocks read-only and hides the cursor. The last line restates
the default.

### Other core options

The bundled CodeMirror defines 60 core options (`Object.keys(CodeMirror.defaults)` on the live
editor). The ones not in the table above take the same path and were not tested one by one:

`addModeClass`, `allowDropFileTypes`, `autocapitalize`, `autocorrect`, `autofocus`,
`configureMouse`, `coverGutterNextToScrollbar`, `cursorHeight`, `cursorScrollMargin`,
`disableInput`, `electricChars`, `fixedGutter`, `flattenSpans`, `gutters`, `historyEventDelay`,
`keyMap`, `lineNumberFormatter`, `lineSeparator`, `lineWiseCopyCut`, `maxHighlightLength`,
`moveInputWithCursor`, `pasteLinesPerSelection`, `phrases`, `pollInterval`,
`resetSelectionOnContextMenu`, `rtlMoveVisually`, `screenReaderLabel`, `selectionsMayTouch`,
`singleCursorHeightPerLine`, `specialCharPlaceholder`, `specialChars`, `value`,
`wholeLineUpdateBefore`, `workDelay`, `workTime`.

Options whose value is a function (`lineNumberFormatter`, `specialCharPlaceholder`,
`configureMouse`) cannot be expressed in EDN.

## Themes

`:theme` sets the editor's `cm-s-<name>` class. Logseq ships styles for two themes only:
`solarized` (`src/main/frontend/extensions/code.css`) and its own `lsradix`
(`resources/css/codemirror.lsradix.css`), each in `light` and `dark`. For any other
[CodeMirror 5 theme](https://codemirror.net/5/demo/theme.html), load its stylesheet yourself,
at the top of `custom.css`:

```css
@import "https://codemirror.net/5/theme/monokai.css";
```

Two things to know:

- Logseq's default is `"solarized <mode>"`, or `"lsradix <mode>"` when an accent colour is set
  (`code.cljs:402-406`). A `:theme` in the config replaces both words, so the editor no longer
  follows light and dark mode.
- When the app theme or accent colour changes, Logseq sets the editor's theme back to its own
  default (`code.cljs:617-625`), and the configured `:theme` returns the next time the block is
  rendered. Read in source, not observed.

## Options that are accepted but do nothing

| Option | Why |
| --- | --- |
| `:foldGutter true`, and `"CodeMirror-foldgutter"` in `:gutters` | The fold addons (`fold/foldcode`, `fold/foldgutter`, `fold/brace-fold`, …) are not bundled. Code folding is not available in Logseq code blocks; it would need those addons added to `code.cljs` or injected by a plugin. |
| `:mode`, `:extraKeys`, `:tabIndex` | Overridden by Logseq, see above. |
| Any unknown key | CodeMirror stores it and ignores it. |

## Options that crash the app

These values name something that is not bundled. CodeMirror throws while the code block is
being created, and Logseq shows its **"Something went wrong"** screen. In testing, the screen
stayed after the option was removed from the config; the app had to be relaunched.

| Option | Tested value | Also expected to crash |
| --- | --- | --- |
| `:keyMap` | `"emacs"` | `"vim"`, `"sublime"` (no keymap files are bundled; not tested separately) |
| `:scrollbarStyle` | `"overlay"` | `"simple"` (same missing addon; not tested) |

**Recovery.** Remove the option from `config.edn` and relaunch Logseq. If the config page can
no longer be opened in the app, fix the config from outside it, for example with the Logseq CLI.
This recovery path was not exercised with a hand-edited `config.edn`: in the test the option was
removed through the plugin API before relaunching.

## Source references

| What | Where |
| --- | --- |
| Config key read | `src/main/frontend/extensions/code.cljs:370-372` |
| Bundled addons | `src/main/frontend/extensions/code.cljs:3-6` |
| Default theme name | `src/main/frontend/extensions/code.cljs:402-406` |
| Option merge | `src/main/frontend/extensions/code.cljs:425-450` |
| Theme reset on app theme change | `src/main/frontend/extensions/code.cljs:617-625` |
| Schema (`:map`) | `src/main/frontend/schema/handler/common_config.cljc:56` |
| CodeMirror version | `package.json:162` |
