---
title: 'Use a Custom Font'
description: 'How to load and apply your own font in a DB graph, including fonts shipped in the graph assets and icon fonts'
status: draft
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: authored-from-source
logseqVersion: '2.0.1'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  howTo:
    - '[Custom CSS](./custom-css.md)'
    - '[Create a Theme](./create-theme.md)'
  reference:
    - '[CSS Variables](../../reference/configuration/css-variables.md)'
---

# Use a Custom Font

> **Goal**: display a font of your choosing in Logseq — one installed on the machine, one shipped inside the graph, or one fetched from a CDN.

## Overview

Logseq has a built-in font control with three fixed choices, and a variable that custom
CSS can point anywhere. This guide covers the second: making the app use a font that is
not on that list.

There are two halves to the job, and both have to be right:

1. **Make the font available to the page** — nothing at all if it is already installed on
   the machine, an `@font-face` rule if it is not.
2. **Point Logseq at it** — normally by redefining `--ls-font-family`, not by writing a
   `font-family` rule.

## Before You Start

- You are using a DB graph.
- You know how to open `logseq/custom.css` — Settings → General → **Edit custom.css**. See
  [Custom CSS](./custom-css.md) for where that file lives and how saving behaves.
- Decide how far the font should reach:

| Scope | What to change |
| --- | --- |
| The whole app, including the sidebar and menus | `--ls-font-family` |
| Block content only | `font-family` on `.ls-block` |
| Inline code and code blocks | `font-family` on `:not(pre) > code` and `pre code` |

## The built-in setting first

Settings → General → **Font** offers **Default**, **Serif** and **Mono**, with a
**Set as global font family** checkbox
([`src/main/frontend/components/settings.cljs:263-287`](../../../../../src/main/frontend/components/settings.cljs);
strings at [`src/resources/dicts/en.edn:1758-1759`](../../../../../src/resources/dicts/en.edn)).

The choice is stored in local storage and written onto `<html>` as `data-font` and
`data-font-global`
([`src/main/frontend/state.cljs:2068-2072`](../../../../../src/main/frontend/state.cljs),
[`src/main/frontend/components/theme.cljs:70-74`](../../../../../src/main/frontend/components/theme.cljs)).

This matters for your CSS, because with **Set as global font family** ticked, the theme
redefines the same variable you are about to set, from a more specific selector:

```css
html[data-font='serif'][data-font-global='true'] { --ls-font-family: Lyon-Text, Georgia, ui-serif, serif; }
```

([`src/main/frontend/components/theme.css:204-218`](../../../../../src/main/frontend/components/theme.css)).
`html[data-font][data-font-global]` outranks `:root`, so **leave Font on Default**, or match
that specificity in your own rule.

## Steps

### 1. Make the font available

Pick whichever of these fits the font you have.

**a. The font is installed on the machine.** Nothing to load — skip to step 2 and name it.

**b. The font file lives in the graph.** Put the file in the graph folder's `assets/`
directory (`~/logseq/graphs/<graph>/assets/` by default) and reference it with a relative
`../assets/` path:

```css
@font-face {
  font-family: 'MyFont';
  src: url('../assets/MyFont.woff2') format('woff2');
  font-weight: 400;
  font-display: swap;
}
```

Before injecting your stylesheet, Logseq rewrites every `url('../assets/…')` it finds into a
blob URL for that file
([`src/main/frontend/handler/assets.cljs:165-177`](../../../../../src/main/frontend/handler/assets.cljs)).
Three consequences worth knowing:

- The path must start with `../assets/` and sit inside `url(…)`. That exact pattern is what
  the rewrite looks for; `/assets/…` or a bare filename is left alone and will not resolve,
  because the stylesheet is injected as a `data:` URL with no base to resolve against
  ([`src/main/frontend/util.cljc:927-946`](../../../../../src/main/frontend/util.cljc)).
- A file you dropped into Logseq is stored under its asset UUID, so the name to use is
  `../assets/<uuid>.woff2`, not the original filename. A file you copy into `assets/`
  yourself keeps its name.
- The blob is created with the MIME type `image` whatever the file is
  ([`:158-163`](../../../../../src/main/frontend/handler/assets.cljs)). That turns out not to
  matter — see [Verification](#verification).

**c. The font is on a CDN.** Use the URL directly. Nothing rewrites it:

```css
@font-face {
  font-family: 'MyFont';
  src: url('https://cdn.jsdelivr.net/npm/@fontsource/inter@5/files/inter-latin-400-normal.woff2') format('woff2');
  font-display: swap;
}
```

A remote `@font-face` is the option confirmed to work on the iOS app.

**d. Nothing else works.** Embed the font as a base64 `data:` URI in the `src`. It bypasses
the rewrite entirely, at the cost of a large `custom.css`.

### 2. Point Logseq at the font

```css
:root {
  --ls-font-family: 'MyFont';
}
```

Use the variable rather than a `font-family` rule. The app sets the base font itself, with
`!important`, reading that variable
([`src/main/frontend/common.css:16-20`](../../../../../src/main/frontend/common.css)):

```css
html:not(.is-native-android) {
  font-family: var(--ls-font-family), sans-serif, system-ui, …, !important;
}
```

A plain `html { font-family: MyFont }` in custom.css loses to that `!important`. Setting the
variable wins without a fight, and keeps the whole fallback chain intact. The default value
is `Inter`, set on `:root`
([`src/main/frontend/shui/vars-classic.css:8`](../../../../../src/main/frontend/shui/vars-classic.css)).

### 3. Save and check

Saving `logseq/custom.css` re-injects the stylesheet immediately — no restart on desktop or
web ([`src/main/frontend/handler/db_based/editor.cljs:161-165`](../../../../../src/main/frontend/handler/db_based/editor.cljs)).

If the font does not change, work down this list:

1. Is **Settings → General → Font** still on Default? See [above](#the-built-in-setting-first).
2. Is the font name spelled exactly as the `@font-face` declares it?
3. For an `../assets/` font, is the file really in the graph's `assets/` folder, under the
   name you wrote?
4. Open DevTools and check the `logseq-custom-theme-id` stylesheet and the Network/Fonts
   panel for a failed font load.

## Variations

### Content only, leaving the UI alone

```css
.ls-block {
  font-family: 'MyFont', sans-serif;
}
```

This is the selector the built-in Serif and Mono choices use for their non-global mode
([`src/main/frontend/components/theme.css:204-218`](../../../../../src/main/frontend/components/theme.css)).

### Code font

Inline code has its own stack
([`src/main/frontend/common.css:581-593`](../../../../../src/main/frontend/common.css)):

```css
:not(pre) > code,
pre code {
  font-family: 'MyMonoFont', MonoLisa, 'Fira Code', Menlo, monospace;
}
```

### An icon font, for private-use glyphs

Icon characters in the Private Use Area (tabler icons in a property choice, for example)
render on desktop but not in the iOS app, which ships only the SVG icons. A `@font-face`
alias fixes it, but it **must** carry a `unicode-range`: icon fonts also map `a-z`, `0-9` and
`_` for ligatures, and without the range the icon font hijacks ordinary text.

```css
@font-face {
  font-family: 'tabler-icons-fallback';
  src: url('https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@2.47.0/fonts/tabler-icons.woff2') format('woff2');
  unicode-range: U+E000-F8FF;
}

html.is-native-ios {
  --ls-font-family: 'tabler-icons-fallback', Inter !important;
}
```

Put the alias **first** in the family list so it is consulted before the text font, and match
the tabler version Logseq bundles. This exact recipe was confirmed by a user on iPhone and
iPad on 2026-09-12.

## Platform Notes

- **Android (native app):** the base-font rule is scoped to `html:not(.is-native-android)`
  ([`src/main/frontend/common.css:16-20`](../../../../../src/main/frontend/common.css)), so
  `--ls-font-family` does not drive the base font there at all. Style `.ls-block` instead.
- **iOS (native app):** the variable is redefined on `html.is-native-ios`
  ([`src/main/mobile/components/app.css:12-15`](../../../../../src/main/mobile/components/app.css)),
  which outranks `:root`. Write your rule against `html.is-native-ios`, or add `!important`.
- **iOS, after editing:** custom.css does sync, but the app can keep serving the old CSS.
  Force-quit and reopen the app after changing it (observed on iPad, 2026-09-12).
- **Mobile editing:** whether the mobile Settings screen offers **Edit custom.css** is
  unverified — the button lives in the desktop/web Settings component. Edit on desktop and
  let it sync.

## Verification

Read against source at `5824434a74` on 2026-09-18. One claim was tested, the rest are
source-read:

- **Blob MIME type.** `<make-data-url` hardcodes `{:type "image"}`, which raised the question
  of whether an `../assets/` font would load at all. Tested outside Logseq, in Chromium via
  Playwright: the same font file served from a blob URL typed `image`, `font/ttf` and `""`
  loaded in all three cases (`document.fonts.load` → `status: "loaded"`). The hardcoded type
  is not an obstacle. Not re-tested inside the Logseq app.
- **The iOS icon-font recipe** is field-confirmed by a user on iPhone and iPad (2026-09-12),
  not by this pass.
- Everything else on this page — the variable, the `!important` base rule, the `data-font`
  specificity, the Android exclusion, the asset-URL rewrite — is read from the source lines
  cited inline and was not exercised in a running app.

## Related Tasks

- [Custom CSS](./custom-css.md) — where `custom.css` lives, how it is applied, other recipes
- [Create a Theme](./create-theme.md) — packaging styles as a theme instead

## See Also

- **Reference**: [CSS Variables](../../reference/configuration/css-variables.md)

---

_Last updated: 2026-09-18 | [Report an issue](https://github.com/logseq/logseq/issues)_
