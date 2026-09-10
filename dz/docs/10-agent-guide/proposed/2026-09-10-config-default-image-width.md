# Default Image Width in config.edn

> **Status:** Proposed. Scope only; nothing implemented.
> **Date:** 2026-09-10
> **Scoped against:** `master` @ `be800f1` (2026-09-08)
> **Background:** [028 — Default Image Size Investigation](../../07-investigations/028-default-image-size.md)

## Goal

Let a user choose the width that un-resized images display at, through `config.edn`, instead of the hard-coded 250px.

## Problem

`asset-cp` sizes every un-resized image as `(or width 250 asset-width)`, where `width` comes from `:logseq.property.asset/resize-metadata` (`src/main/frontend/components/block.cljs:1157-1168`). There is no config key, so users resize images one at a time or override the size with a `custom.css` rule that saves nothing and leaves a 250px loading placeholder behind. On native mobile, where resize handles are disabled (`block.cljs:410-421`), there is no way to change the size at all.

## Users

- **Primary:** desktop, web and mobile users who insert many images and want a size that suits their graph.
- **Secondary:** users on native mobile, for whom this is the only way to change image size.

## Constraints

- **Must:** leave images that have `resize-metadata` exactly as they are.
- **Must:** keep today's 250px when the key is missing or invalid.
- **Must:** re-render images when `config.edn` changes, without reloading the app.
- **Must not:** write to the graph. This is a display default, not data.
- **Must not:** change videos, published pages, PDFs, breadcrumbs, table view or property-value image caps.
- **Performance:** no measurable change. The width is derived from config already in app state.
- **Security:** none. Config is user-authored and read locally.

## Design Decisions

1. **Display default, not write-on-insert** — nothing is persisted and the setting applies at once to every un-resized image. Alternative considered: save `resize-metadata {:width N}` on each new asset, which only affects new images and needs a write hook in the insert path.
2. **Key: `:ui/default-image-width`** — this is a display-only setting like the other `:ui/` keys, and nothing is written to asset properties. Alternative considered: `:asset/default-image-width`, which reads as asset data. Confirm the name with maintainers (see [Open Questions](#open-questions)).
3. **Whole pixels, at least 60** — the schema type is `:int`, matching `:block/title-max-length` and `:ref/default-open-blocks-level` (`src/main/frontend/schema/handler/common_config.cljc:28`, `:50`). 60 is the resize handle's own floor (`block.cljs:449`). Anything else falls back to 250.
4. **A pure sizing function in `frontend.components.block.asset`** — lift the inline logic from `asset-cp` into a testable function next to `show-image-placeholder?` (`src/main/frontend/components/block/asset.cljs:55`), whose tests already live in `asset_test.cljs`. Alternative considered: a `state.cljs` accessor like `get-ref-open-blocks-level` (`state.cljs:598-604`). That reads non-reactive config and adds a second function to test.
5. **Read config reactively** — follow `nested-link`: `(state/config-for-repo (rfx/use-sub [:config]) repo)` (`block.cljs:1399-1401`). `state/get-config` alone is a snapshot and would not re-render on edit.
6. **One key in the shared schema** — adding it to `common_config.cljc` makes it valid in both global and graph `config.edn`. The existing merge order (built-in defaults, then global, then graph, `state.cljs:493-502`) lets a graph override the global value with no extra code.

## Data Model

No graph data changes. One new optional config key:

```clojure
;; config.edn (global or graph)
:ui/default-image-width 666
```

The render-time rule, in priority order:

| Source | Width |
|---|---|
| `:logseq.property.asset/resize-metadata :width` on the asset | that value |
| `:ui/default-image-width`, if an integer ≥ 60 | that value |
| otherwise | 250 |

Height stays `width / (asset-width / asset-height)` when both original dimensions are known, and is omitted otherwise (unchanged from `block.cljs:1162-1168`).

## Acceptance Criteria

1. With no key set, every image renders exactly as today.
2. With `:ui/default-image-width 666`, an un-resized image renders 666px wide, keeping its aspect ratio.
3. The loading placeholder uses the same width, so the layout doesn't jump.
4. A resized image keeps its saved width.
5. Dragging to resize still starts from the displayed size and saves as before.
6. Editing `config.edn` updates open pages without a reload.
7. A graph `config.edn` value overrides a global one.
8. A string, float, zero, negative or sub-60 value renders at 250, and the config editor flags a non-integer as a type error.
9. On native mobile, images honour the key.
10. Table view, breadcrumbs, property values, videos and published pages look the same as before.

## Out of Scope (v1)

- Videos (separate sizing: `asset-video-style`, `block.cljs:471-485`).
- Published pages. The publish renderer ignores `resize-metadata` and uses a responsive `srcset` (`deps/publish/src/logseq/publish/render.cljs:868-906`), and config is not part of the publish payload.
- PDF assets, gallery-card layout, and markdown image-link metadata (`image-link`, `block.cljs:641-668`).
- Writing `resize-metadata` to assets, and bulk-resetting resized images.
- A Settings UI control. Without one, no UI text is added and no i18n work is needed.
- A default height, or per-format defaults.

## Open Questions

| Question | Owner | Resolve by |
|---|---|---|
| Key name `:ui/default-image-width`, or `:asset/…`? | Maintainers | Before the PR |
| Does a graph's `logseq/config.edn` sync to collaborators through RTC? If yes, a graph-level value applies to everyone on that graph. | Implementer | Task 1 |
| Does a very large value (e.g. `5000`) overflow the block, or do existing styles cap it? If it overflows, clamp to the container. | Implementer | Task 3 manual check |
| Does gallery view read the same width? If so, keep gallery cards on the old sizing. | Implementer | Task 3 manual check |

## Task List

Branch off `master` (not `eng-documentation`). Unit tests: `bb dev:test -v <ns>`. Full gate: `bb dev:lint-and-test`.

- [ ] **0. Answer the RTC config question** — read how `logseq/config.edn` is stored and synced in DB graphs.
      Done when: the sync answer is recorded under Open Questions.
      Est: ~15 min

- [ ] **1. Sizing function (RED → GREEN)** — `src/main/frontend/components/block/asset.cljs`, `src/test/frontend/components/block/asset_test.cljs`
      RED: tests for resize width winning, a valid config width, a missing key, invalid values (string, float, 0, negative, 59) falling back to 250, and height from aspect ratio with and without original dimensions.
      GREEN: add `image-display-metadata [asset-block config-width]` returning `{:width … :height …}`.
      Done when: `bb dev:test -v frontend.components.block.asset-test` passes.
      Est: ~20 min

- [ ] **2. Config key, validation and template** — `src/main/frontend/schema/handler/common_config.cljc`, `src/test/frontend/handler/common/config_edn_test.cljs`, `deps/common/resources/templates/config.edn`
      Add `[:ui/default-image-width :int]`. Add a validation case showing `"666"` reports a type error while `666` is valid. Add a commented template entry in the existing style (`config.edn:117-120`) with "Default value: 250", left commented so a future default change still reaches new graphs.
      Done when: `bb dev:test -v frontend.handler.common.config-edn-test` passes.
      Est: ~15 min

- [ ] **3. Wire `asset-cp`** — `src/main/frontend/components/block.cljs:1157-1168`
      Read `:ui/default-image-width` reactively (decision 5) and replace the inline `img-metadata` with `image-display-metadata`.
      Done when: all unit tests pass, and in a dev build acceptance criteria 1-7, 9 and 10 hold, and the oversize and gallery questions are answered.
      Est: ~30 min

- [ ] **4. Gate and review** — whole changeset.
      Done when: `bb dev:lint-and-test` is green and a code review finds nothing blocking. The PR describes the new key, the fallback rules and the out-of-scope surfaces.
      Est: ~15 min

- [ ] **5. Docs (after merge)** — in `eng-documentation`: `dz/docs/ddocs/reference/configuration/config-edn-db.md` and [028](../../07-investigations/028-default-image-size.md).
      Document the key. Note that the `custom.css` `img[width="250"]` workaround stops matching once the key is set to another value, and should be removed.
      Done when: both pages are updated and their review dates bumped.
      Est: ~15 min

### Progress Log

<!-- Updated during implementation -->
