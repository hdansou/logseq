# Logseq Command Palette Reference (Validated)

> **Last revised:** 2026-05-29

This reference is validated against current source in `src/`, `deps/`, and `packages/`.

## Validation Scope

Primary command definitions and categorization:

- `src/main/frontend/modules/shortcut/config.cljs`
- `src/main/frontend/handler/command_palette.cljs`
- `src/main/logseq/api/plugin.cljs`

Related command-palette extension points:

- `deps/common/resources/templates/config.edn` (`:commands []`)

No additional command definition maps were found under `packages/`.

## Current Snapshot

- Built-in shortcut commands in `all-built-in-keyboard-shortcuts`: **145**
- Categories in `*category`: **9** (`basics`, `formatting`, `navigating`, `block-editing`, `block-command-editing`, `block-selection`, `toggle`, `others`, `plugins`)
- Command palette command set is composed of:
  - Built-in shortcut-derived commands
  - Plugin-registered commands (runtime)
  - User custom commands from config (`:commands`)

## Findings: Stale Claims Fixed

1. `Total Commands: 135` was stale. Current built-in count is **145**.
2. Two legacy IDs listed in this doc are no longer present in current source.
3. Several shortcut renderings in the previous version were parser artifacts (for example malformed multi-binding/macOS condition output). Source truth is the Clojure map values in `config.cljs`.

> **Removed:** `:command/run`
>
> Replaced by `:shell/run` (`mod+shift+1`, Electron-only).

> **Removed:** `:command/toggle-favorite`
>
> Replaced by `:page/toggle-favorite` (`mod+shift+f`).

## Findings: New Capabilities

The following built-in commands exist in current source but were missing from the previous reference:

- `:editor/add-reaction` (`p r`)
- `:editor/add-comment` (`ctrl+space`)
- `:graph/export-as-html` (no default binding)
- `:graph/open` (`alt+shift+g`)
- `:graph/remove` (no default binding)
- `:graph/add` (no default binding)
- `:graph/db-add` (disabled by default binding)
- `:graph/db-save` (`mod+s`, Electron-only)
- `:dev/recompute-checksum` (dev, no default binding)
- `:dev/export-client-ops-sqlite` (dev, no default binding)
- `:shell/run` (`mod+shift+1`, Electron-only)
- `:page/toggle-favorite` (`mod+shift+f`)

Additional command-palette capabilities verified:

- Runtime plugin shortcut registration routes plugin IDs into `:shortcut.category/plugins`.
- Command palette supports persisted invoke history and top-command ranking.
- User config supports custom command entries via `:commands []` in config templates.

## Notes for Future Regeneration

When updating this reference, parse command bindings directly from `all-built-in-keyboard-shortcuts` and treat:

- `[]` and `false` as no default keybinding
- vector bindings as multiple bindings
- `(if mac? ...)` as platform-specific bindings
