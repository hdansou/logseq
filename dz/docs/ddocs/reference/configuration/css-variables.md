---
title: 'CSS Variables Reference'
description: "Reference for Logseq's CSS custom properties for theme customization"
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '2.0.1'
platforms: [desktop, mobile, web]
graphType: [db]
relatedDocs:
  howTo:
    - '[Create Custom Theme](../../how-to/integrations/create-theme.md)'
    - '[Custom CSS](../../how-to/integrations/custom-css.md)'
  reference:
    - '[Plugin API Reference](../api/plugin-api.md)'
---

# CSS Variables Reference

> **Last revised:** 2026-09-11

> Reference for Logseq's CSS custom properties (variables) used in theming and customization.

Every variable, default and selector on this page was read from the stylesheets at `be800f1711`. Paths are relative to the Logseq repository root. Nothing here was tested in a running app, so confirm computed values in DevTools.

## Overview

Logseq's stylesheets use four families of custom properties:

| Family | Purpose | Where the values come from |
| --- | --- | --- |
| `--ls-*` | Logseq's semantic and layout variables | Mainly [`src/main/frontend/shui/vars-classic.css`](../../../../../src/main/frontend/shui/vars-classic.css), plus per-accent values in [`src/main/frontend/shui/colors.css`](../../../../../src/main/frontend/shui/colors.css) |
| `--lx-*` | The active accent and neutral scales (`--lx-accent-01`…`12`, `--lx-gray-01`…`12`) | Per accent color in [`src/main/frontend/shui/colors.css`](../../../../../src/main/frontend/shui/colors.css); `--lx-gray-02` also in dark mode for every accent ([`src/main/frontend/shui/index.css:62`](../../../../../src/main/frontend/shui/index.css)) |
| `--rx-*` | Raw [Radix Colors](https://www.radix-ui.com/colors) scales | [`src/main/frontend/shui/radix.css`](../../../../../src/main/frontend/shui/radix.css), [`src/main/frontend/shui/radix-hsl.css`](../../../../../src/main/frontend/shui/radix-hsl.css) |
| shui tokens | `--background`, `--foreground`, `--primary`, `--border` and friends, written as HSL channels | [`src/main/frontend/shui/index.css:2-32`](../../../../../src/main/frontend/shui/index.css) (light), [`:34-63`](../../../../../src/main/frontend/shui/index.css) (dark), overridden in [`resources/css/shui.css:5-31`](../../../../../resources/css/shui.css) and `colors.css` |

This page lists the variables a theme is most likely to set. It is not an exhaustive list of every token.

---

## How Variables Resolve (Read This First)

Four facts decide whether setting a variable changes anything.

1. **Most color rules read `--lx-*` first.** The `--ls-*` variable is often only a fallback. For example, the body uses `color: var(--lx-gray-12, var(--ls-primary-text-color, hsl(var(--foreground))))` ([`src/main/frontend/common.css:33-34`](../../../../../src/main/frontend/common.css)), and links use `var(--lx-accent-11, var(--ls-link-text-color, …))` ([`common.css:91`](../../../../../src/main/frontend/common.css)). The tables below say, for each variable, what reads it and whether an `--lx-*` variable is read first.
2. **Which `--lx-*` variables are set depends on the accent color.**
   - With the default Logseq accent, `--lx-accent-01`…`12` are set in both modes ([`colors.css:3-14`](../../../../../src/main/frontend/shui/colors.css)). `--lx-gray-01`…`12` are set in light mode ([`:23-34`](../../../../../src/main/frontend/shui/colors.css)). In dark mode `colors.css` sets only `--lx-gray-01`, to `var(--ls-primary-background-color, …)` ([`:38`](../../../../../src/main/frontend/shui/colors.css)), and `index.css` sets `--lx-gray-02` to `var(--ls-secondary-background-color, …)` for every accent ([`src/main/frontend/shui/index.css:62`](../../../../../src/main/frontend/shui/index.css)). So with the default accent in dark mode the `--ls-*` fallbacks behind `--lx-gray-03`…`12` are live.
   - With any other accent, `colors.css` sets `--lx-accent-*`, `--lx-gray-*` and many `--ls-*` colors (for example [`colors.css:57-141`](../../../../../src/main/frontend/shui/colors.css) for `tomato`).
   - With the **None** accent (`data-color="none"`), `colors.css` has no block and the classic `--ls-*` colors are not set. No `--lx-*` variable is set except `--lx-gray-02` in dark mode (from `index.css:62`, which then falls back to the `--secondary` token) and `--lx-gray-07` inside the search dialog, `.cp__cmdk` ([`resources/css/shui.css:80`](../../../../../resources/css/shui.css), [`:91-93`](../../../../../resources/css/shui.css)). Colors then come mostly from the shui tokens.
3. **Specificity and element.** With the Logseq accent the classic `--ls-*` colors are declared on `html[data-theme=light][data-color=logseq]` and `html[data-theme=dark][data-color=logseq]` ([`vars-classic.css:42`](../../../../../src/main/frontend/shui/vars-classic.css), [`:100`](../../../../../src/main/frontend/shui/vars-classic.css)). A rule on `:root` or `html[data-theme='dark']` is less specific and loses. Other accents nest `body, .dark-theme, .light-theme { … }` inside `html { &[data-color=<accent>] { … } }` ([`colors.css:1-2`](../../../../../src/main/frontend/shui/colors.css), [`:57-58`](../../../../../src/main/frontend/shui/colors.css)). The build runs `postcss-nested` ([`postcss.config.js:9`](../../../../../postcss.config.js)), which turns that into `html[data-color=<accent>] body, html[data-color=<accent>] .dark-theme, html[data-color=<accent>] .light-theme`. Those match `<body>`, so anything you set on `<html>` is shadowed there. A rule on `html[data-theme='dark'] body.dark-theme` (or `…'light'] body.light-theme`) is more specific than both.
4. **Aliases resolve where they are declared.** A variable whose value is `var(--other)`, such as `--ls-tag-text-color: var(--ls-link-ref-text-color)` ([`vars-classic.css:65`](../../../../../src/main/frontend/shui/vars-classic.css)), is computed on the element that declares it and inherited as a finished value. Overriding `--other` on `<body>` does not update an alias declared on `<html>`. Set the alias itself.

The shui token defaults in `index.css`, and its dark-mode `--lx-gray-02`, sit inside `@layer base` ([`src/main/frontend/shui/index.css:1`](../../../../../src/main/frontend/shui/index.css)). Under CSS cascade layers an unlayered rule beats a layered one whatever its specificity, so any ordinary rule overrides them. This follows from the stylesheet; it was not checked against the compiled CSS. The per-accent shui values in `colors.css` are unlayered and follow rule 3.

---

## Theme Selectors

### Attributes and Classes Logseq Sets

| Hook | Set by | Values |
| --- | --- | --- |
| `html[data-theme]` | [`src/main/frontend/components/theme.cljs:52`](../../../../../src/main/frontend/components/theme.cljs) | `light` or `dark`. **System** mode still writes one of these: it reads `prefers-color-scheme` and stores `light` or `dark` in `:ui/theme` ([`src/main/frontend/state.cljs:1346`](../../../../../src/main/frontend/state.cljs), [`:1349-1355`](../../../../../src/main/frontend/state.cljs)). |
| `html.dark` | [`theme.cljs:53-56`](../../../../../src/main/frontend/components/theme.cljs) | Present in dark mode, for Tailwind's dark variant |
| `body.dark-theme` / `body.light-theme` / `body.white-theme` | [`theme.cljs:55-56`](../../../../../src/main/frontend/components/theme.cljs) | `dark-theme` in dark mode; `white-theme light-theme` in light mode. Kept for backward compatibility. |
| `html[data-color]` | [`theme.cljs:62-66`](../../../../../src/main/frontend/components/theme.cljs) | The accent color, `logseq` when none was ever picked |
| `html[data-font]`, `html[data-font-global]` | [`theme.cljs:68-74`](../../../../../src/main/frontend/components/theme.cljs) | Editor font type (styled for `serif` and `mono` in [`src/main/frontend/components/theme.css:204-218`](../../../../../src/main/frontend/components/theme.css)); `true`/`false` |
| `html.is-mac`, `.is-win32`, `.is-linux`, `.is-electron`, `.is-ios`, `.is-mobile`, `.is-native-ios`, `.is-native-android`, `.is-native-iphone`, `.is-native-ipad`, `.is-publish-mode`, … | [`src/main/frontend/ui.cljs:344-359`](../../../../../src/main/frontend/ui.cljs) | Platform classes |

The accent picker is **Settings → General → Accent color** ([`src/main/frontend/components/settings.cljs:374-434`](../../../../../src/main/frontend/components/settings.cljs), [`:882`](../../../../../src/main/frontend/components/settings.cljs)). It offers `none`, `logseq`, and `tomato red crimson pink plum purple violet indigo blue cyan teal green grass orange` ([`settings.cljs:399`](../../../../../src/main/frontend/components/settings.cljs), [`src/main/frontend/colors.cljs:4`](../../../../../src/main/frontend/colors.cljs)).

### CSS Selectors

```css
/* Light mode (any accent) */
html[data-theme='light'] body.light-theme {
}

/* Dark mode (any accent) */
html[data-theme='dark'] body.dark-theme {
}

/* Dark mode with the default Logseq accent */
html[data-theme='dark'][data-color='logseq'] body.dark-theme {
}

/* The "None" accent */
html[data-color='none'] body {
}
```

`.light-theme` and `.dark-theme` match `<body>`, not `<html>`. A combined rule such as `.light-theme, html[data-theme='light'] { … }` declares variables on two different elements.

---

## Background Colors

Defaults are for the Logseq accent ([`vars-classic.css:42-98`](../../../../../src/main/frontend/shui/vars-classic.css) light, [`:100-162`](../../../../../src/main/frontend/shui/vars-classic.css) dark). Other accents replace many of them in `colors.css`.

| Variable | Light default | Dark default | What reads it |
| --- | --- | --- | --- |
| `--ls-primary-background-color` | `#ffffff` | `#002b36` | Many rules. Examples: body background, behind `--lx-gray-01` (`common.css:34`); `textarea` (`common.css:59`); Tailwind `gray-01` fallback (`tailwind.config.js:36`) |
| `--ls-secondary-background-color` | `#f7f7f7` | `#023643` | Many rules. Examples: `pre` (`common.css:83`), `pre.code` (`:105`), `.bg-base-2` (`:193`); left sidebar, behind `--lx-gray-02` (`theme.css:201`); dark-mode `--lx-gray-02` (`shui/index.css:62`); Tailwind `gray-02` fallback (`tailwind.config.js:37`) |
| `--ls-tertiary-background-color` | `#eaeaea` | `#08404f` | Many rules. Examples: `.bg-base-4` (`common.css:201`); `resources/css/shui.css:152`; Tailwind `gray-03` fallback (`tailwind.config.js:38`) |
| `--ls-quaternary-background-color` | `#dcdcdc` | `#094b5a` | Many rules. Examples: `.keyboard-shortcut > code` (`common.css:630`); Tailwind `gray-04`…`07` fallback (`tailwind.config.js:39-42`) |
| `--ls-block-properties-background-color` | secondary background | `#06323e` | `.block-properties, .page-properties`, behind `--lx-gray-03` (`block.css:939-942`). The app renders DB properties as `div.ls-properties-area` instead (`property.cljs:972-975`). `page-properties` is only a class on the block editor when the text is `---` front matter (`editor.cljs:524`, `:549`, `:716`); both classes also appear in the separate publishing renderer (`deps/publish/src/logseq/publish/render.cljs:1026`, `:1250`). |
| `--ls-page-properties-background-color` | secondary background | `#06323e` | `.open-block-ref-link` (`block.css:193`), `.query-title` (`block.css:1395`) |
| `--ls-page-blockquote-bg-color` | `var(--lx-gray-03, #fbfaf8)` | secondary background | Blockquotes, read first (`common.css:122`). In light mode the right sidebar redeclares it as quaternary background (`right_sidebar.css:9`). |
| `--ls-page-inline-code-bg-color` | secondary background | `#01222a` | Inline code, behind `--lx-gray-06` (`common.css:589`). In light mode the right sidebar redeclares it (`right_sidebar.css:8`). |
| `--ls-selection-background-color` | `#e4f2ff` | `#338fff` | `::selection` (`common.css:159`, `:164`); `block.css:1424`; `command_palette.css:20`, `:24` |
| `--ls-table-tr-even-background-color` | secondary background | `#03333f` | **Nothing.** Defined but never read. |
| `--ls-slide-background-color` | `#fff` | primary background | **Nothing.** Defined but never read. |

Bare stylesheet names in the "What reads it" columns are under `src/main/frontend/components/` (`block.css`, `theme.css`, `container.css`, `command_palette.css`, `right_sidebar.css`, …), except `common.css` and `ui.css`, which are under `src/main/frontend/`. Other paths are relative to `src/main/frontend/` (`shui/…`, `extensions/…`, `components/…`) unless they start with `resources/`, `src/` or are `tailwind.config.js` (repository root). "Many rules" means more than about ten `var()` reads; the examples are a sample.

---

## Text Colors

| Variable | Light default | Dark default | What reads it |
| --- | --- | --- | --- |
| `--ls-primary-text-color` | `#433f38` | `#a4b5b6` | Many rules. Examples: body text, behind `--lx-gray-12` (`common.css:33`); `::selection` text (`common.css:160`, `:165`); dark code blocks, read first (`.cm-s-lsradix.cm-s-dark`, `resources/css/codemirror.lsradix.css:37-39`) |
| `--ls-secondary-text-color` | `#161e2e` | `#dfdfdf` | Many rules. Examples: `.heading-bg` hover ring, behind `--lx-gray-09` (`common.css:319`); `.keyboard-shortcut > code` (`common.css:633`); `command_palette.css:37` |
| `--ls-title-text-color` | `var(--ls-header-button-background)` | `#93a1a1` | Page titles, behind `--lx-gray-12` (`common.css:334-337`) |
| `--ls-link-text-color` | `#106ba3` | `rgb(138, 187, 187)` | Many rules. Examples: links and page refs, behind `--lx-accent-11` (`common.css:91`, `block.css:886`); `button.menu` (`common.css:378`); `a.login` (`common.css:410`) |
| `--ls-link-text-hover-color` | `#1a537c` | `var(--ls-active-secondary-color)` | Link hover, behind `--lx-accent-12` (`common.css:96`); `a.login:hover` (`common.css:414`); about a dozen more rules |
| `--ls-link-ref-text-color` | `var(--ls-link-text-color)` | `var(--ls-link-text-color)` | `components/plugins.css:883`; `extensions/pdf/pdf.css:799`; the light `--ls-tag-text-color` alias (`shui/vars-classic.css:65`) |
| `--ls-block-ref-link-text-color` | `var(--ls-link-text-color)` | `#377f91` | `.block-ref-no-title` underline (`block.css:855`); `.block-ref` underline through the Tailwind arbitrary value `border-b-[--ls-block-ref-link-text-color]` (`block.css:827`) |
| `--ls-tag-text-color` | `var(--ls-link-ref-text-color)` | `var(--ls-link-text-color)` | `a.tag`, behind `--lx-accent-11` (`common.css:477`) |
| `--ls-page-inline-code-color` | primary text | primary text | Inline code, behind `--lx-gray-11` (`common.css:590`) |
| `--ls-page-blockquote-color` | primary text | primary text | Blockquote text (`common.css:123`) |
| `--ls-selection-text-color` | secondary text | `#fff` | Command palette highlighted icon (`command_palette.css:19`) only |
| `--ls-cloze-text-color` | `#0000cd` | `#8fbc8f` | Cloze (`block.css:1407`, `:1412`) |
| `--ls-icon-color` | `#646464` | link text | Plugin log view, `DEBUG` level, only (`components/plugin_logs.cljs:153`) |
| `--ls-search-icon-color`, `--ls-search-icon-hover-color` | primary / secondary text | primary / secondary text | `command_palette.css:16`, `:31` |
| `--ls-left-sidebar-text-color` | `var(--lx-gray-12)` | `var(--lx-gray-11)` | **Nothing.** Defined but never read. |
| `--ls-tag-text-hover-color`, `--ls-link-ref-text-hover-color` | alias of link hover | alias of link hover | **Nothing with an effect.** `--ls-link-ref-text-hover-color` is read only by the light `--ls-tag-text-hover-color` declaration (`shui/vars-classic.css:66`), and no rule reads `--ls-tag-text-hover-color`. |

**Opacity variables** (on `:root`, [`vars-classic.css:2-3`](../../../../../src/main/frontend/shui/vars-classic.css)):

```css
--ls-tag-text-opacity: 0.8;
--ls-tag-text-hover-opacity: 1;
```

Neither is read. The `a.tag:hover` rule reads a misspelled `--ls-tag-text-hover-opasxcity` instead ([`common.css:480`](../../../../../src/main/frontend/common.css)), so setting `--ls-tag-text-hover-opacity` does nothing.

---

## Border Colors

| Variable | Light default | Dark default | What reads it |
| --- | --- | --- | --- |
| `--ls-border-color` | `#ccc` | `#0e5263` | Many rules. Examples: `hr`, behind `--lx-gray-05` (`common.css:524`); `resources/css/shui.css:196`; `resources/css/ui.css:1043`, `:1071`; `extensions/graph.css:94` |
| `--ls-secondary-border-color` | `#e2e2e2` | `#126277` | `settings.css:312`, `:329`, `:421`; `block.css:36`; `file_sync.css:344`; `extensions/handbooks/handbooks.css:40`, `:108`, `:187`, `:373` |
| `--ls-tertiary-border-color` | `rgba(200, 200, 200, 0.3)` | `rgba(0, 2, 0, 0.1)` | `container.css:146`; `file_sync.css:265`, `:289`, `:295`; `extensions/handbooks/handbooks.css:122`, `:125` |
| `--ls-guideline-color` | `rgba(46, 27, 5, 0.08)` | `#0b4a5a` | Child-block guide line, behind `--lx-gray-04-alpha` (`block.css:654`) |
| `--ls-page-blockquote-border-color` | `var(--lx-gray-08, #799bbc)` | border color | Blockquote left border, read first (`common.css:133`) |
| `--ls-block-bullet-border-color` | `#dedede` | `#0f4958` | Collapsed bullet, behind `--lx-gray-04-alpha` (`block.css:1290`); `block.css:1320` |

Every border also gets `border-color: hsl(var(--border))` from `html *` ([`resources/css/shui.css:1-3`](../../../../../resources/css/shui.css)).

---

## Interactive Elements

| Variable | Light default | Dark default | What reads it |
| --- | --- | --- | --- |
| `--ls-active-primary-color` | `rgb(0, 105, 182)` | `#8ec2c2` | Drag-over indicator (`container.css:615`); `onboarding/index.css:28`; plugin log `INFO` level (`plugin_logs.cljs:152`) |
| `--ls-active-secondary-color` | `#00477c` | `#d0e8e8` | Only the dark `--ls-link-text-hover-color` alias (`shui/vars-classic.css:120`) |
| `--ls-menu-hover-color` | `var(--ls-a-chosen-bg)` | secondary background | Menu hover, behind `--lx-gray-05` (`common.css:384`); behind `--lx-gray-04` in `ui.css:25` |
| `--ls-a-chosen-bg` | quaternary background | quaternary background | `a.chosen` (`common.css:429`), `common.css:625`; command palette, behind `--lx-gray-03` / `--lx-gray-02` (`command_palette.css:36`, `:42`); `cmdk/cmdk.css:69`, `:75`, `:81` |
| `--ls-header-button-background` | `rgba(15, 20, 25, 1)` | `#dee4ea` | Only the light `--ls-title-text-color` alias (`shui/vars-classic.css:60`) |
| `--ls-button-background-hsl` | `200 98% 35%` | `200 98% 35%` | Only `--ls-button-background` (`shui/vars-classic.css:97`, `:155`) |
| `--ls-button-background` | `hsl(var(--ls-button-background-hsl))` | same | Onboarding only: behind `--lx-accent-09` (`onboarding/index.css:407`); `:425` |
| `--ls-focus-ring-color` | `rgba(66, 133, 244, 0.5)` | `rgba(18, 98, 119, 0.5)` | **Nothing.** Defined for every accent but never read. Tailwind's `ring` color is the shui `--ring` token (`tailwind.config.js:200`). |

---

## Block-Specific Colors

| Variable | Defined | What reads it |
| --- | --- | --- |
| `--ls-block-bullet-color` | `:root`: `var(--lx-gray-08, var(--rx-gray-08))` ([`vars-classic.css:28`](../../../../../src/main/frontend/shui/vars-classic.css)); Logseq accent: `rgba(67, 63, 56, 0.25)` light, `#608e91` dark; None accent: `var(--rx-gray-08)` ([`shui.css:81`](../../../../../resources/css/shui.css)) | Bullet, behind `--lx-gray-08` (`block.css:1294`); heading bullet (`block.css:1267`); `block.css:1324` |
| `--ls-block-highlight-color` | `#c0e6fd` light, `#0a3d4b` dark; also set per accent in `colors.css` | `.block-highlight`, `.ls-block.selected` and `.ls-table-cell.selected`, read first (`common.css:346-351`) |
| `--ls-block-bullet-active-color` | Only under `html[data-color=none]`: `var(--rx-gray-12)` ([`shui.css:82`](../../../../../resources/css/shui.css)) | **Nothing.** Defined but never read. |
| `--ls-block-left-color` | Only on `.block-children-left-border:hover` under `html[data-color=logseq]`: `var(--rx-logseq-11)` ([`shui.css:75-77`](../../../../../resources/css/shui.css)) | **Nothing.** Defined but never read. |

### Highlight Colors (Block Background Colors)

Defined on `:root` ([`vars-classic.css:19-25`](../../../../../src/main/frontend/shui/vars-classic.css)):

```css
--ls-highlight-color-gray: var(--rx-gray-05);
--ls-highlight-color-red: var(--rx-red-05);
--ls-highlight-color-yellow: var(--rx-yellow-05);
--ls-highlight-color-green: var(--rx-green-05);
--ls-highlight-color-blue: var(--rx-blue-05);
--ls-highlight-color-purple: var(--rx-purple-05);
--ls-highlight-color-pink: var(--rx-pink-05);
```

No stylesheet reads them directly. The ClojureScript builds `var(--ls-highlight-color-<name>)` as an inline style when a block's background color is one of these built-in names ([`src/main/frontend/ui.cljs:123-136`](../../../../../src/main/frontend/ui.cljs), [`src/main/frontend/components/block.cljs:3588-3592`](../../../../../src/main/frontend/components/block.cljs)).

---

## Component Colors

| Variable | Light default | Dark default | What reads it |
| --- | --- | --- | --- |
| `--ls-page-checkbox-color` | `#9dbbd8` | `#6093a0` | `.form-checkbox`, `.form-radio`, read first (`theme.css:57-58`) |
| `--ls-page-checkbox-border-color` | `var(--ls-page-checkbox-color)` | primary background | `theme.css:59`, `:71` |
| `--ls-page-mark-bg-color` | `#fef3ac` | `#fef3ac` | `mark` (`common.css:575`) |
| `--ls-page-mark-color` | `#262626` | `#262626` | `mark` (`common.css:576`) |
| `--ls-pie-bg-color` | `#e1e1e1` | `#01303b` | RTC sync progress pie, set as an inline style (`src/main/frontend/ui.cljs:1128-1131`) |
| `--ls-pie-fg-color` | `#0a4a5d` | `#0b5869` | Same (`src/main/frontend/ui.cljs:1129`) |

---

## Scrollbar Colors

| Variable | Light default | Dark default | What reads it |
| --- | --- | --- | --- |
| `--ls-scrollbar-foreground-color` | `rgba(0, 0, 0, 0.1)` | `#11505f` | Behind `--lx-gray-05` (`theme.css:3`, `:21`, `:40`) |
| `--ls-scrollbar-background-color` | `rgba(0, 0, 0, 0.05)` | `rgba(30, 60, 67, 0.1)` | Behind `--lx-gray-02` (`theme.css:3`, `:25`) |
| `--ls-scrollbar-thumb-hover-color` | `rgba(0, 0, 0, 0.2)` | `rgba(255, 255, 255, 0.2)` | Active thumb, behind `--lx-gray-06` (`theme.css:31`) |
| `--ls-scrollbar-width` | `6px` (`:root`) | `6px` | `theme.css:26` |

The `::-webkit-scrollbar` rules apply only inside `.visible-scrollbar` or `html.custom-scrollbar` ([`theme.css:19-37`](../../../../../src/main/frontend/components/theme.css)). Logseq adds `custom-scrollbar` when it is not on macOS or when scrollbars take up width ([`theme.cljs:25-39`](../../../../../src/main/frontend/components/theme.cljs)).

---

## Layout & Spacing

Defined on `:root` in [`vars-classic.css:1-17`](../../../../../src/main/frontend/shui/vars-classic.css).

| Variable | Default | What reads it |
| --- | --- | --- |
| `--ls-page-text-size` | `1em` | `#root` font size (`container.css:8-10`) |
| `--ls-page-title-size` | `36px` | Page title (`common.css:338`, with a `32px` fallback); `block.css:721` |
| `--ls-main-content-max-width` | `960px` | Content width in normal mode (`container.css:503`); `common.css:264`; `onboarding.css:2` |
| `--ls-main-content-max-width-wide` | `1440px` | Content width in wide mode (`container.css:490`) |
| `--ls-font-family` | `Inter` | Font on `html:not(.is-native-android)` (`common.css:17-18`). Redeclared by `html[data-font='serif'\|'mono'][data-font-global='true']` (`theme.css:208-218`) and on iOS (`src/main/mobile/components/app.css:12-14`). |
| `--ls-border-radius-low` | `4px` | `common.css:50`, `:597-598`; `block.css:1379` |
| `--ls-headbar-height` | `3rem` | `container.css:54`, `:763`; `header.css:8`, `:205`. `header.css:227-294` redeclares it on native iPad, iPhone and zoomed-iOS layouts. |
| `--ls-headbar-inner-top-padding` | `0px` | `container.css:84`, `:341`; `header.css:6`, `:8`, `:205` |
| `--ls-left-sidebar-width` | `246px` | `container.css:47`, `:324`, `:444`; `header.css:11` |
| `--ls-left-sidebar-sm-width` | `74vw` | `container.css:76` |
| `--ls-native-kb-height` | `0px` | `command_palette.css:107`; `src/main/mobile/components/app.css:64` |
| `--ls-border-radius-medium` | `8px` | **Nothing.** Defined but never read. |
| `--ls-left-sidebar-nav-btn-size` | `38px` | **Nothing.** Defined but never read. |

The left sidebar background is a separate variable, `--left-sidebar-bg-color`, declared on `main.theme-container-inner` as `var(--lx-gray-02, var(--ls-secondary-background-color, …))` ([`theme.css:200-202`](../../../../../src/main/frontend/components/theme.css)) and read in [`container.css:77`](../../../../../src/main/frontend/components/container.css) and [`:166`](../../../../../src/main/frontend/components/container.css).

### Mobile-Specific

Declared on `:root` in [`src/main/mobile/components/app.css:6-10`](../../../../../src/main/mobile/components/app.css). That stylesheet is part of the mobile build only ([`tailwind.mobile.css:43`](../../../../../tailwind.mobile.css)); `tailwind.all.css` does not import it.

```css
--ls-mobile-font-scale: 1;
--ls-page-title-size: calc(26px * var(--ls-mobile-font-scale, 1));
--ls-mobile-font-size: 16px;
```

On iOS, `html` font size is `calc(var(--ls-mobile-font-size) * var(--ls-mobile-font-scale, 1))` ([`app.css:12-16`](../../../../../src/main/mobile/components/app.css)). The app sets `--ls-mobile-font-scale` at runtime ([`src/main/frontend/mobile/util.cljs:50`](../../../../../src/main/frontend/mobile/util.cljs)).

---

## Z-Index Levels

Defined on `:root` in [`src/main/frontend/components/theme.css:5-10`](../../../../../src/main/frontend/components/theme.css). The comments are not in the source; they name what reads each level.

```css
--ls-z-index-level-0: 0;     /* not read */
--ls-z-index-level-1: 9;     /* see below */
--ls-z-index-level-2: 99;    /* inside .cp__plugins (plugins.css:687) */
--ls-z-index-level-3: 999;   /* help button and menu (container.css:530, :541) */
--ls-z-index-level-4: 9999;  /* .cp__sidebar-left-layout (container.css:334) */
--ls-z-index-level-5: 99999; /* popper that wraps .repos-list (resources/css/shui.css:236-237) */
```

Level 1 is read by `.menu-backdrop` (`common.css:397`), inside `.cp__sidebar-left-layout` at widths of 640px and up (`container.css:437`), `.cp__right-sidebar` (`container.css:564`), `#custom-context-menu` as level 1 + 1 (`content.css:17`) and inside `.cp__plugins` (`plugins.css:289`). Several readers go through Tailwind arbitrary values such as `z-[var(--ls-z-index-level-4)]`.

---

## Color Level System

```css
/* .white-theme, .light-theme, html[data-theme='light'] — any accent */
--color-level-1: var(--rx-gray-02);
--color-level-2: var(--rx-gray-03);
--color-level-3: var(--rx-gray-04);
--color-level-4: var(--rx-gray-05);
--color-level-5: var(--rx-gray-06);
--color-level-6: var(--rx-gray-07);

/* html[data-theme=dark][data-color=logseq] only */
--color-level-1: var(--ls-secondary-background-color);
--color-level-2: var(--ls-tertiary-background-color);
--color-level-3: var(--ls-quaternary-background-color);
--color-level-4: #195d6c;
--color-level-5: #266c7d;
--color-level-6: #3a7e8e;
```

Light values are at [`vars-classic.css:31-40`](../../../../../src/main/frontend/shui/vars-classic.css), dark at [`:156-161`](../../../../../src/main/frontend/shui/vars-classic.css). No rule sets them in dark mode with a non-Logseq accent. They are read in `block.css:1182-1250` (the `.color-level` rule reads `--lx-gray-02` first, `block.css:1182`; the `.color-level-5` and `.color-level-6` rules read them directly, `:1246`, `:1250`), `shortcut.css:67` (behind `--lx-gray-09`), `shortcut.css:601` (read first) and `resources/css/shui.css:459`, `:463` (behind `--lx-accent-06` / `--lx-accent-08-alpha`).

---

## Logseq Scales (`--lx-*`)

`colors.css` maps each accent color to two 12-step scales:

- `--lx-accent-01`…`12` from the accent's Radix scale (the Logseq accent uses `--rx-logseq-*`).
- `--lx-gray-01`…`12` from a matching neutral scale (for example `--rx-mauve-*` for `tomato`, [`colors.css:90-101`](../../../../../src/main/frontend/shui/colors.css)).

Non-Logseq accents also set `-alpha` variants (`--lx-accent-01-alpha`, `--lx-gray-01-alpha`, …), 24 per accent. The Logseq accent block, which starts at [`colors.css:2`](../../../../../src/main/frontend/shui/colors.css) and ends before the `tomato` block at [`:57`](../../../../../src/main/frontend/shui/colors.css), sets none. See [How Variables Resolve](#how-variables-resolve-read-this-first) for which of these the Logseq and None accents leave unset.

Counting `var(--lx-…)` reads in the stylesheets at `be800f1711`, the most-read ones are `--lx-gray-03` (29), `--lx-gray-12` (21), `--lx-gray-02` (17), `--lx-gray-06` (15), `--lx-gray-09` (13), `--lx-gray-11` (12), `--lx-gray-04` (11), `--lx-gray-10` (10) and `--lx-gray-01` (9). The most-read accent step is `--lx-accent-11` (6). Tailwind's `gray-01`…`12` and `accent-01`…`12` utilities also resolve through them ([`tailwind.config.js:4-47`](../../../../../tailwind.config.js)). `gray-01`…`07` fall back to the `--ls-*` backgrounds and then `--rx-gray-*`; `gray-08`…`12` fall back to `--rx-gray-*`; `accent-02`…`11` fall back to the `--accent` token.

---

## shui Tokens

Newer shared UI (dialogs, popovers, menus, buttons) uses shadcn-style tokens. They hold **HSL channels**, not colors, because consumers wrap them: `border: 'hsl(var(--border))'`, `primary: 'hsl(var(--primary))'`, `popover: 'hsl(var(--popover))'` ([`tailwind.config.js:198-228`](../../../../../tailwind.config.js)).

| Token | Default light ([`index.css:2-32`](../../../../../src/main/frontend/shui/index.css)) |
| --- | --- |
| `--background` / `--foreground` | `0 0% 100%` / `222.2 84% 4.9%` |
| `--card`, `--popover` | `0 0% 100%` (their `-foreground`: `222.2 84% 4.9%`) |
| `--primary` / `--primary-foreground` | `222.2 47.4% 11.2%` / `210 40% 98%` |
| `--secondary`, `--muted`, `--accent` | `210 40% 96.1%` (`--muted-foreground`: `0 0% 52.3%`; the other two `-foreground`: `222.2 47.4% 11.2%`) |
| `--destructive` / `--destructive-foreground` | `0 84.2% 60.2%` / `210 40% 98%` |
| `--border` | `300 1% 92%` |
| `--input` | `214.3 31.8% 91.4%` |
| `--ring` | `222.2 84% 4.9%` |
| `--radius` | `0.5rem` |

Dark defaults are on `html[data-theme=dark]` at [`index.css:34-61`](../../../../../src/main/frontend/shui/index.css). `resources/css/shui.css:5-31` overrides several of them per mode, and each accent overrides `--primary`, `--accent`, `--ring` and more (for the Logseq accent, [`colors.css:16-54`](../../../../../src/main/frontend/shui/colors.css)).

Write `--primary: 199 89% 48%`, not `--primary: hsl(199 89% 48%)`.

---

## Status Colors

Status styling uses Tailwind utility classes rather than dedicated custom properties. They are defined in [`src/main/frontend/common.css:437-468`](../../../../../src/main/frontend/common.css):

```css
/* Error */
.text-error   → @apply text-red-rx-09;
.bg-error     → @apply bg-red-rx-04-alpha;

/* Warning */
.text-warning → @apply text-yellow-600 dark:text-yellow-500/70;
.bg-warning   → @apply bg-yellow-300 dark:bg-yellow-500/50;

/* Success */
.text-success → @apply text-green-rx-09;
.bg-success   → @apply bg-green-rx-04-alpha;
```

`.text-error` and `.bg-error` are each defined twice with the same body (`common.css:445-451` and `:454-460`).

### Sync Indicator Colors

Defined on `:root` in [`src/main/frontend/components/file_sync.css:1-5`](../../../../../src/main/frontend/components/file_sync.css):

```css
--ls-color-file-sync-error: var(--color-red-600);
--ls-color-file-sync-pending: var(--color-yellow-600);
--ls-color-file-sync-idle: var(--color-green-600);
```

Despite the file name, these style the RTC sync indicator, `.cp__rtc-sync` ([`file_sync.css:7`](../../../../../src/main/frontend/components/file_sync.css), rendered in [`src/main/frontend/components/rtc/indicator.cljs:257-259`](../../../../../src/main/frontend/components/rtc/indicator.cljs)). `pending` and `idle` are read (`file_sync.css:18-35`, `:191`, `:197`). `--ls-color-file-sync-error` is never read.

---

## Radix Color System

Logseq ships the [Radix Colors](https://www.radix-ui.com/colors) 12-step scales.

### Naming Convention

```css
--rx-{color}-{step}        /* color value, radix.css */
--rx-{color}-{step}-alpha  /* translucent variant, radix.css */
--rx-{color}-{step}-hsl    /* HSL channels, radix-hsl.css */
```

Steps are two digits, `01` to `12`. Light values are on `:root` ([`radix.css:1`](../../../../../src/main/frontend/shui/radix.css), [`:353`](../../../../../src/main/frontend/shui/radix.css)). Dark values are on `html.dark, html[data-theme=dark]` ([`radix.css:716`](../../../../../src/main/frontend/shui/radix.css), [`:1068`](../../../../../src/main/frontend/shui/radix.css); [`radix-hsl.css:340`](../../../../../src/main/frontend/shui/radix-hsl.css)).

### Available Colors

```
Logseq:  logseq
Gray:    gray, mauve, slate, sage, olive, sand
Red:     tomato, red, crimson
Pink:    pink, plum, purple, violet
Blue:    indigo, blue, sky, cyan
Green:   teal, mint, green, grass, lime
Yellow:  yellow, amber, orange
Brown:   brown, bronze, gold
```

For what each step is meant for, see the Radix documentation. The step meanings are Radix's convention, not something Logseq's stylesheets enforce.

---

## Creating a Custom Theme

Put this in `custom.css` (Settings → General → **Custom theme** → **Edit custom.css**, shown only when a graph is open: [`src/main/frontend/components/settings.cljs:217-222`](../../../../../src/main/frontend/components/settings.cljs), [`:885`](../../../../../src/main/frontend/components/settings.cljs)) or in a theme plugin's CSS file. The selectors follow [How Variables Resolve](#how-variables-resolve-read-this-first). They are derived from the stylesheets and were not tested in a running app.

### Minimal Dark Theme

```css
html[data-theme='dark'] body.dark-theme {
  /* Read first by most rules */
  --lx-gray-01: #1e1e2e;  /* body background (common.css:34) */
  --lx-gray-02: #181825;  /* left sidebar (theme.css:201) */
  --lx-gray-12: #cdd6f4;  /* body text, page titles (common.css:33, :337) */
  --lx-accent-11: #89b4fa; /* links, page refs, tags */

  /* Read directly */
  --ls-page-blockquote-bg-color: #313244;
  --ls-block-highlight-color: #313244;
  --ls-selection-background-color: #45475a;

  /* shui tokens: HSL channels */
  --background: 240 21% 15%;
  --foreground: 226 64% 88%;
  --popover: 240 21% 12%;
  --border: 237 16% 23%;
  --primary: 217 92% 76%;
}
```

With the default Logseq accent in dark mode, `--lx-gray-03`…`12` start out unset (rule 2). Setting any of them switches the rules that read them away from their `--ls-*` fallbacks.

### Remapping a Whole Scale

Set `--lx-gray-01`…`12` and `--lx-accent-01`…`12` directly, on the same `body` selector. Overriding `--rx-*` is less reliable: with the Logseq accent in light mode, `--lx-gray-*` is declared on `<html>` as `var(--rx-gray-NN)` ([`colors.css:23-34`](../../../../../src/main/frontend/shui/colors.css)), so a `--rx-*` value set on `<body>` never reaches it (rule 4 above).

---

## Other Variables

Variables a theme may meet that the sections above do not cover. Paths are under `src/main/frontend/` unless they start with `resources/`.

| Variable | Defined | What reads it |
| --- | --- | --- |
| `--lx-pdf-container-dark-bg` | `var(--lx-gray-03)` on `.extensions__pdf-container` (`extensions/pdf/pdf.css:713-714`); redeclared in `resources/css/shui.css:128` | PDF viewer backgrounds in dark mode, read first (`extensions/pdf/pdf.css:302`, `:727`, `:739`) |
| `--lx-bg-override` | Per element in `resources/css/shui.css:72`, `:123`; also set by a Tailwind utility generated in `tailwind.config.js:86` | **Nothing.** No stylesheet, script or Tailwind color reads it through `var()`. |
| `--ls-gallery-card-width`, `--ls-gallery-card-height` | `220px`, `320px` on `.ls-cards` (`components/block/views.css:18-20`); also referenced in `components/views.cljs:2364-2365` | Gallery-view card size (`components/block/views.css:26`, `:70-71`) |
| `--ls-draggable-handle-height` | `30px` on `:root` (`components/plugins.css:1-2`) | Plugin UI drag handles (`components/plugins.css:699`, `:711`, `:747`, `:749`, `:758`) |
| `--ls-color-icon-preset` | `"inherit"` on `.cp__emoji-icon-picker` (`components/icon.css:1-2`); also referenced in `components/icon.cljs:368` | Icon picker preset color (`components/icon.css:58`) |
| `--ls-page-title-line-height`, `--ls-block-control-size` | `1.38`, `24px`, local to one rule (`components/block.css:718-719`) | The same rule (`components/block.css:721`) |
| `--kbd-glow-top`, `--kbd-glow-bottom` | Per mode (`resources/css/shui.css:10-11`, `:29-30`) | Keyboard-shortcut key glow (`resources/css/shui.css:298`, `:304`, `:399`, `:409`) |

---

## Source Files

- [`src/main/frontend/shui/vars-classic.css`](../../../../../src/main/frontend/shui/vars-classic.css): `:root` layout variables, highlight colors, `--color-level-*`, and the Logseq accent's light/dark `--ls-*` colors
- [`src/main/frontend/shui/colors.css`](../../../../../src/main/frontend/shui/colors.css): per-accent `--lx-*` scales, shui tokens and `--ls-*` colors, keyed by `html[data-color=…]`
- [`src/main/frontend/shui/radix.css`](../../../../../src/main/frontend/shui/radix.css): Radix scales (`--rx-*`, `--rx-*-alpha`)
- [`src/main/frontend/shui/radix-hsl.css`](../../../../../src/main/frontend/shui/radix-hsl.css): Radix HSL channel variants (`--rx-*-hsl`)
- [`src/main/frontend/shui/index.css`](../../../../../src/main/frontend/shui/index.css): shui token defaults (inside `@layer base`)
- [`resources/css/shui.css`](../../../../../resources/css/shui.css): shui token overrides per mode, `data-color=logseq` and `data-color=none` rules
- [`src/main/frontend/components/theme.css`](../../../../../src/main/frontend/components/theme.css): z-index levels, scrollbar, checkbox, left sidebar background and font-preference hooks
- [`src/main/frontend/common.css`](../../../../../src/main/frontend/common.css): base element colors and status utility classes
- [`src/main/mobile/components/app.css`](../../../../../src/main/mobile/components/app.css): mobile text-size variables (mobile build only)
- [`src/main/frontend/components/file_sync.css`](../../../../../src/main/frontend/components/file_sync.css): sync indicator colors
- [`tailwind.config.js`](../../../../../tailwind.config.js): maps Tailwind color names onto `--lx-*`, `--rx-*` and shui tokens

---

## Logseq Defects Noticed

- Defined but never read: `--ls-focus-ring-color`, `--ls-table-tr-even-background-color`, `--ls-slide-background-color`, `--ls-left-sidebar-text-color`, `--ls-tag-text-opacity`, `--ls-tag-text-hover-opacity`, `--ls-tag-text-hover-color`, `--ls-border-radius-medium`, `--ls-left-sidebar-nav-btn-size`, `--ls-block-bullet-active-color`, `--ls-block-left-color`, `--ls-color-file-sync-error`, `--ls-z-index-level-0`, `--lx-bg-override`.
- `a.tag:hover` reads `--ls-tag-text-hover-opasxcity`, a typo for `--ls-tag-text-hover-opacity` ([`common.css:480`](../../../../../src/main/frontend/common.css)).
- `colors.css` has a `brown` accent block ([`colors.css:1243`](../../../../../src/main/frontend/shui/colors.css)) that the accent picker does not offer ([`src/main/frontend/colors.cljs:4`](../../../../../src/main/frontend/colors.cljs)).
- Each non-Logseq accent still sets whiteboard variables `--ls-wb-stroke-color-default`, `--ls-wb-background-color-default` and `--ls-wb-text-color-default` (for example [`colors.css:136-138`](../../../../../src/main/frontend/shui/colors.css)). Whiteboards were removed, so nothing reads them. The None-accent block also still sets `--ls-*` backgrounds on the tldraw `.tl-container` ([`resources/css/shui.css:111-116`](../../../../../resources/css/shui.css)), a class no ClojureScript under `src/main` renders.
- Read but defined nowhere in the repository, so the fallback (or nothing) always applies: `--ls-error-text-color` ([`src/main/frontend/components/plugin_logs.cljs:150`](../../../../../src/main/frontend/components/plugin_logs.cljs)), `--ls-bg-quaternary` ([`src/main/frontend/ui.css:143`](../../../../../src/main/frontend/ui.css)), `--ls-quinary-background-color` and `--ls-senary-background-color` ([`resources/css/ui.css:1082-1083`](../../../../../resources/css/ui.css), [`:1090-1091`](../../../../../resources/css/ui.css)), `--ls-right-sidebar-topbar-color` ([`src/main/frontend/components/container.css:694`](../../../../../src/main/frontend/components/container.css)), `--lx-red-11` ([`src/main/frontend/components/block.css:480`](../../../../../src/main/frontend/components/block.css)), `--lx-popover-bg` ([`resources/css/shui.css:66`](../../../../../resources/css/shui.css)) and `--ls-win32-title-bar-height` ([`src/main/frontend/components/header.css:7`](../../../../../src/main/frontend/components/header.css)).
- Tailwind's `accent-01` and `accent-12` colors are `var(--lx-accent-01, --rx-gray-01)` and `var(--lx-accent-12, --rx-gray-12)` ([`tailwind.config.js:8`](../../../../../tailwind.config.js), [`:19`](../../../../../tailwind.config.js)). The fallback is missing its `var()`, so when the `--lx-accent-*` variable is unset (for example with the None accent) the fallback is the literal text `--rx-gray-01`, not a color. Derived from source; not run.

---

## See Also

- **Create a Theme**: [create-theme.md](../../how-to/integrations/create-theme.md)
- **Custom CSS**: [custom-css.md](../../how-to/integrations/custom-css.md)
- **Plugin API**: [plugin-api.md](../api/plugin-api.md)
- **Radix Colors**: [radix-ui.com/colors](https://www.radix-ui.com/colors)

---

_Last updated: 2026-09-11 | [Report an issue](https://github.com/logseq/logseq/issues)_
